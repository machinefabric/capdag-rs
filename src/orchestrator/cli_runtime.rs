//! [`CliRuntime`] — the capdag CLI's implementation of [`EngineRuntime`].
//!
//! It hosts cartridges in-process (via [`CartridgeHostRuntime`]) and, exactly like the
//! engine's daemon-hosted runtime, reuses ONE long-lived [`RelaySwitch`] across every
//! segment — including every ForEach body. A cap's cartridge process is therefore
//! spawned once and each body multiplexes onto it, so the cartridge's own declared
//! concurrency pools (e.g. a capacity-1 "gpu" pool) serialize model loads instead of
//! N bodies each loading a fresh copy of the model into GPU memory.
//!
//! Where the cartridges come from — dev binaries, installed cartridges, or bundled
//! cartridges — is orthogonal to how they are hosted and called; this runtime resolves
//! them lazily on first need and hosts each exactly once (deduped by binary path).
//!
//! Reference-regime specifics vs the engine: terminal sinks are persisted through
//! [`CliDiskWriter`] part-files in the emit target directory (so UNBOUNDED terminals
//! stream to disk per L16, and `emit_terminals` renames the parts to their contract
//! names), the flow observer tracks ONLY feed-bearing rids (for the Ctrl-C
//! stop-input control), and any ForEach body failure fails the whole plan
//! (failures are exposed, not tolerated). Everything else — the segment
//! orchestration — is the shared `EngineRuntime::run_segment` default.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::bifaci::cartridge_repo::CartridgeChannel;
use crate::bifaci::protocol_trace::ProtocolTraceSink;
use crate::bifaci::relay_switch::RelaySwitch;
use crate::cap::registry::FabricRegistry;
use crate::orchestrator::execute_plan::EngineRuntime;
use crate::orchestrator::executor::{
    discover_bundled_cartridges, segment_activity_timeout, CartridgeManager, ExecutionContext,
};
use crate::orchestrator::types::ResolvedGraph;
use crate::ExecutionError;

/// The capdag CLI's cartridge-hosting runtime. Build with [`CliRuntime::new`]; the
/// shared cartridge host is constructed lazily on first segment and torn down when the
/// runtime is dropped.
pub struct CliRuntime {
    cartridge_dir: PathBuf,
    registry_url: Option<String>,
    channel: CartridgeChannel,
    fabric_manifest_version: u32,
    dev_binaries: Vec<PathBuf>,
    bundled_cartridges_dir: Option<PathBuf>,
    fabric_registry: Arc<FabricRegistry>,
    /// Optional per-segment protocol trace. When set, the shared `run_segment` samples
    /// the switch's L8 snapshot live (250ms) and once at teardown, appending one JSONL
    /// line per sample — the CLI's `--trace` and the scenario harness's
    /// `CAPDAG_SCENARIO_TRACE` wire this. `None` disables tracing.
    trace_sink: Option<Arc<ProtocolTraceSink>>,
    /// Where terminal-sink part-files are streamed — the resolved emit directory
    /// (`--output`, or the current directory), so the final rename to contract
    /// names never crosses a filesystem.
    persist_dir: PathBuf,
    /// Uniquifies part-file names across sinks, runs, and ForEach bodies.
    writer_seq: Arc<std::sync::atomic::AtomicU64>,
    /// The long-lived in-process cartridge host, built on demand. Behind a mutex for
    /// interior mutability under the `&self` trait hooks; the lock is held only while
    /// registering newly-needed cartridges, never across segment execution.
    host: Mutex<CliHost>,
    /// Live-input state for the stop-input control (15.2 §Runs Stop): taps of
    /// HOST-opened feeds and the rids of FEED-BEARING requests (cartridge-side
    /// taps). First Ctrl-C in `capdag run` closes the taps and the machine
    /// drains to complete outputs; a second Ctrl-C aborts.
    live_inputs: CliLiveInputs,
}

/// Tracks everything a stop-input must close, and doubles as the CLI's
/// [`FlowObserver`] (rid→step correlation is a no-op here; only feed-bearing
/// rids matter to the CLI).
#[derive(Default)]
struct CliLiveInputs {
    host_taps: std::sync::Mutex<Vec<crate::bifaci::live_feed::LiveFeedHandle>>,
    feed_bearing: std::sync::Mutex<Vec<crate::bifaci::frame::MessageId>>,
}

impl crate::orchestrator::stream_io::FlowObserver for CliLiveInputs {
    fn record(&self, _rid: &crate::bifaci::frame::MessageId, _token_id: &str) {}

    fn record_feed_bearing(&self, rid: &crate::bifaci::frame::MessageId) {
        self.feed_bearing
            .lock()
            .expect("CLI feed-bearing registry mutex poisoned")
            .push(rid.clone());
    }
}

/// Lazily-initialised shared host state. `ctx` owns the switch, the host tasks, and
/// their cleanup handles for the runtime's whole lifetime; per-segment execution builds
/// cheap `ExecutionContext::from_switch` clones that share this switch and are dropped
/// (without tearing the host down) at the end of each segment.
struct CliHost {
    ctx: Option<ExecutionContext>,
    manager: Option<CartridgeManager>,
    /// Cap URNs already hosted — the fast path for repeated ForEach bodies of the same
    /// cap, which must NOT re-resolve or re-host anything.
    registered_caps: HashSet<String>,
    /// Cartridge binaries already hosted — a binary serving several caps is hosted once.
    registered_paths: HashSet<PathBuf>,
    /// Bundled cartridges are discovered and registered exactly once.
    bundled_registered: bool,
}

impl CliRuntime {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cartridge_dir: PathBuf,
        registry_url: Option<String>,
        channel: CartridgeChannel,
        fabric_manifest_version: u32,
        dev_binaries: Vec<PathBuf>,
        bundled_cartridges_dir: Option<PathBuf>,
        fabric_registry: Arc<FabricRegistry>,
        trace_sink: Option<Arc<ProtocolTraceSink>>,
        persist_dir: PathBuf,
    ) -> Self {
        Self {
            cartridge_dir,
            registry_url,
            channel,
            fabric_manifest_version,
            dev_binaries,
            bundled_cartridges_dir,
            fabric_registry,
            trace_sink,
            persist_dir,
            writer_seq: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            host: Mutex::new(CliHost {
                ctx: None,
                manager: None,
                registered_caps: HashSet::new(),
                registered_paths: HashSet::new(),
                bundled_registered: false,
            }),
            live_inputs: CliLiveInputs::default(),
        }
    }

    /// STOP INPUT (15.2 §Runs Stop): close every open live tap — host-opened
    /// feeds directly, cartridge-resolved feeds via a non-force Cancel on
    /// their feed-bearing requests — and let the machine DRAIN: in-flight
    /// items flow through, terminals end, writers finalize, and the run
    /// completes with its outputs. This is the tap-off control, distinct
    /// from aborting the machine.
    pub async fn stop_live_inputs(&self) {
        {
            let mut taps = self
                .live_inputs
                .host_taps
                .lock()
                .expect("CLI host tap registry mutex poisoned");
            for tap in taps.iter() {
                tap.close();
            }
            taps.clear();
        }
        let rids: Vec<crate::bifaci::frame::MessageId> = {
            let mut rids = self
                .live_inputs
                .feed_bearing
                .lock()
                .expect("CLI feed-bearing registry mutex poisoned");
            std::mem::take(&mut *rids)
        };
        if rids.is_empty() {
            return;
        }
        let switch = {
            let host = self.host.lock().await;
            host.ctx.as_ref().map(|ctx| ctx.switch().clone())
        };
        let Some(switch) = switch else {
            // No host was ever built — nothing is running, nothing to stop.
            return;
        };
        for rid in rids {
            // Frame-only stop: the cartridge runtime closes the request's
            // feed taps and the request ends NATURALLY after the drain —
            // host-side request state stays live so every downstream cap
            // still receives the drained stream.
            switch.stop_request_feeds(&rid).await;
        }
    }
}

impl CliHost {
    /// Ensure every cartridge this segment's caps need is hosted on the shared switch,
    /// building the switch on first use. Idempotent per binary: a cartridge already
    /// hosted (by an earlier segment or another cap of the same binary) is not hosted
    /// again. Fails hard if a cap cannot be resolved to a cartridge — a missing plugin
    /// is exposed, never silently skipped.
    async fn ensure_hosted(
        &mut self,
        graph: &ResolvedGraph,
        rt: &CliRuntime,
    ) -> Result<(), ExecutionError> {
        let cap_urns: Vec<&str> = graph.edges.iter().map(|e| e.cap_urn.as_str()).collect();

        // Fast path: every cap this segment needs is already hosted. This is the hot
        // path for a ForEach — bodies 2..N of the same cap must not re-resolve or
        // re-host; they just reuse the already-spawned cartridge process.
        if self.ctx.is_some() && cap_urns.iter().all(|c| self.registered_caps.contains(*c)) {
            return Ok(());
        }

        if self.manager.is_none() {
            let mut manager = CartridgeManager::new(
                rt.cartridge_dir.clone(),
                rt.registry_url.clone(),
                rt.channel,
                rt.fabric_manifest_version,
                rt.dev_binaries.clone(),
                crate::bifaci::release_cert::RegistryTrust::from_build_constants(),
                rt.fabric_registry.config().registry_base_url.clone(),
            );
            manager.init().await?;
            self.manager = Some(manager);
        }

        // Resolve this segment's caps to cartridges (immutable borrow scoped to here).
        let mut resolved = {
            let manager = self
                .manager
                .as_ref()
                .expect("initialised immediately above");
            manager.resolve_cartridges(&cap_urns).await?
        };

        // Bundled cartridges are hosted once, on the first segment, beside the resolved
        // dev/registry cartridges.
        if !self.bundled_registered {
            if let Some(dir) = &rt.bundled_cartridges_dir {
                resolved.extend(
                    discover_bundled_cartridges(
                        dir,
                        rt.channel,
                        rt.registry_url.as_deref(),
                        rt.fabric_manifest_version,
                    )
                    .await?,
                );
            }
            self.bundled_registered = true;
        }

        // Host only cartridges not already hosted (dedup by binary path).
        resolved.retain(|(path, _, _)| !self.registered_paths.contains(path));

        // Build the bootstrap context on first use. `ExecutionContext::new` creates the
        // switch and starts its background frame pump, so RelayNotify capability updates
        // keep flowing between segments — the reuse invariant depends on it.
        if self.ctx.is_none() {
            self.ctx = Some(ExecutionContext::new(rt.fabric_registry.clone()).await?);
        }

        if !resolved.is_empty() {
            for (path, _, _) in &resolved {
                self.registered_paths.insert(path.clone());
            }
            self.ctx
                .as_mut()
                .expect("bootstrap context ensured above")
                .add_cartridge_host(resolved)
                .await?;
        }

        // Every cap this segment needs is now hosted — record them so the next body of
        // this cap takes the fast path above.
        for c in &cap_urns {
            self.registered_caps.insert((*c).to_string());
        }
        Ok(())
    }
}

#[async_trait]
impl EngineRuntime for CliRuntime {
    async fn segment_switch(
        &self,
        graph: &ResolvedGraph,
    ) -> Result<Arc<RelaySwitch>, ExecutionError> {
        let mut host = self.host.lock().await;
        host.ensure_hosted(graph, self).await?;
        Ok(host
            .ctx
            .as_ref()
            .expect("ensure_hosted guarantees a bootstrap context")
            .switch()
            .clone())
    }

    async fn activity_timeout_secs(&self, graph: &ResolvedGraph) -> Result<u64, ExecutionError> {
        Ok(segment_activity_timeout(graph))
    }

    fn trace_sink(&self) -> Option<Arc<ProtocolTraceSink>> {
        self.trace_sink.clone()
    }

    fn flow_observer(&self) -> Option<&dyn crate::orchestrator::stream_io::FlowObserver> {
        // The CLI observes ONLY feed-bearing rids (for the stop-input
        // control); step correlation is engine-side machinery.
        Some(&self.live_inputs)
    }

    fn on_host_feed_open(&self, handle: &crate::bifaci::live_feed::LiveFeedHandle) {
        let mut taps = self
            .live_inputs
            .host_taps
            .lock()
            .expect("CLI host tap registry mutex poisoned");
        taps.retain(|t| !t.is_closed());
        taps.push(handle.clone());
    }

    /// Persist every terminal sink through a [`CliDiskWriter`] part-file in the
    /// emit directory. This is what lets an UNBOUNDED terminal (a live capture)
    /// stream to disk instead of hitting the L16 buffering refusal; bounded
    /// terminals take the identical path — one code path, no special cases.
    fn writer_factory(
        &self,
    ) -> Option<Box<crate::orchestrator::stream_io::SegmentWriterFactory>> {
        let dir = self.persist_dir.clone();
        let seq = self.writer_seq.clone();
        Some(Box::new(move |sink: &str, coordinate| {
            let n = seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let tag = match &coordinate {
                Some(c) => format!("{n}.{sink}.b{}", c.body_index),
                None => format!("{n}.{sink}"),
            };
            Box::new(crate::orchestrator::cli_writer::CliDiskWriter::new(
                dir.clone(),
                tag,
            ))
        }))
    }

    fn fabric_registry(&self) -> Arc<FabricRegistry> {
        self.fabric_registry.clone()
    }

    async fn foreach_partial_failure_policy(&self) -> String {
        // Reference regime: expose failures — any ForEach body failure fails the plan.
        "fail".to_string()
    }
}

impl Drop for CliRuntime {
    /// Tear down the long-lived in-process cartridge host. `ExecutionContext` has no
    /// `Drop`, so aborting its host tasks — which kills the cartridge child processes —
    /// must be explicit here, or every `CliRuntime` (one per CLI run / per scenario
    /// test) would leak its cartridge processes. `get_mut` needs no lock (we hold
    /// `&mut self`), and `shutdown()` is synchronous (it only aborts task handles), so
    /// it is safe to call from `drop`.
    fn drop(&mut self) {
        if let Some(ctx) = self.host.get_mut().ctx.take() {
            let _ = ctx.shutdown();
        }
    }
}
