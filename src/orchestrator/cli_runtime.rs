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
//! names), and a run of a map's body that fails fails the whole plan unless the
//! machine itself says otherwise (failures are exposed; the CLI adds no
//! tolerance of its own). Everything else — the segment orchestration — is the
//! shared `EngineRuntime::run_segment` default.
//!
//! Every invocation's request id is correlated to its step in one
//! [`RunFlowRegistry`] the switch's terminations fold into, exactly as in the
//! engine: a run started through [`CliRuntime::scope`] is recorded under its
//! own scope, so its protocol flow can be read step by step while it runs and
//! summarised after. The same registry holds the FEED-BEARING requests the
//! stop-input control closes.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::bifaci::cartridge_repo::CartridgeChannel;
use crate::bifaci::protocol_trace::ProtocolTraceSink;
use crate::bifaci::relay_switch::RelaySwitch;
use crate::cap::registry::FabricRegistry;
use crate::orchestrator::execute_plan::{EngineRuntime, MapFailurePolicy};
use crate::orchestrator::executor::{
    discover_bundled_cartridges, segment_activity_timeout, CartridgeManager, ExecutionContext,
};
use crate::orchestrator::observe::RunKey;
use crate::orchestrator::run_flow::{FlowRecorder, FlowScopeGuard, RunFlowRegistry};
use crate::orchestrator::transient::TransientArtifact;
use crate::orchestrator::types::ResolvedGraph;
use crate::ExecutionError;

/// The flow scope a [`CliRuntime`] used directly — not through
/// [`CliRuntime::scope`] — records its invocations under.
pub const CLI_UNSCOPED_FLOW: &str = "cli";

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
    /// Every part-file this runtime's writers created (see
    /// [`CliRuntime::discard_parts`]).
    parts: crate::orchestrator::cli_writer::PartFiles,
    /// The long-lived in-process cartridge host, built on demand. Behind a mutex for
    /// interior mutability under the `&self` trait hooks; the lock is held only while
    /// registering newly-needed cartridges, never across segment execution.
    host: Mutex<CliHost>,
    /// Taps of HOST-opened live feeds, for the stop-input control (15.2 §Runs
    /// Stop). The rids of FEED-BEARING requests (cartridge-side taps) are in
    /// `run_flows`. First Ctrl-C in `capdag run` closes both and the machine
    /// drains to complete outputs; a second Ctrl-C aborts.
    host_taps: std::sync::Mutex<Vec<crate::bifaci::live_feed::LiveFeedHandle>>,
    /// rid → step correlation of every invocation, by run scope; fed by the
    /// switch's terminations once the switch exists.
    run_flows: Arc<RunFlowRegistry>,
    /// The scope this runtime's own invocations are recorded under when it is
    /// used directly rather than through [`CliRuntime::scope`]. Held for the
    /// runtime's lifetime.
    _unscoped_flow: FlowScopeGuard,
    unscoped_recorder: FlowRecorder,
    /// Where a scoped run captures its intermediate values, when it does
    /// ([`CliRuntime::with_transient_root`]).
    transient_root: Option<PathBuf>,
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
    /// Every cap the bundled cartridges declare, once they are discovered.
    bundled_caps: Vec<crate::urn::cap_urn::CapUrn>,
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
        let run_flows = Arc::new(RunFlowRegistry::new());
        let unscoped_flow = FlowScopeGuard::new(CLI_UNSCOPED_FLOW.to_string(), run_flows.clone());
        let unscoped_recorder = unscoped_flow.recorder();
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
            parts: Default::default(),
            host: Mutex::new(CliHost {
                ctx: None,
                manager: None,
                registered_caps: HashSet::new(),
                registered_paths: HashSet::new(),
                bundled_registered: false,
                bundled_caps: Vec::new(),
            }),
            host_taps: std::sync::Mutex::new(Vec::new()),
            run_flows,
            _unscoped_flow: unscoped_flow,
            unscoped_recorder,
            transient_root: None,
        }
    }

    /// Capture every intermediate value of a scoped run under `root` — one
    /// directory per run scope — so it can be inspected while the run goes
    /// on, as the engine captures a machine run's. The caller owns `root`
    /// and removes it when what it holds is no longer wanted.
    pub fn with_transient_root(mut self, root: PathBuf) -> Self {
        self.transient_root = Some(root);
        self
    }

    /// The run-flow registry every invocation of this runtime is correlated
    /// in. Read a scope's live flow with
    /// [`join_flow_states`](crate::orchestrator::join_flow_states) against
    /// [`protocol_stats`](RelaySwitch::protocol_stats), and its summary with
    /// [`RunFlowRegistry::take_flow_summary`].
    pub fn run_flows(&self) -> &Arc<RunFlowRegistry> {
        &self.run_flows
    }

    /// The shared relay switch, once the first segment has built it. `None`
    /// before anything has run.
    pub async fn switch(&self) -> Option<Arc<RelaySwitch>> {
        let host = self.host.lock().await;
        host.ctx.as_ref().map(|ctx| ctx.switch().clone())
    }

    /// A view of this runtime that records the invocations of run `key`
    /// under its own flow scope ([`RunKey::flow_scope`]) and, when a
    /// transient root is set, captures its intermediates in a directory of
    /// their own. `on_transient` hears of every capture. Everything else —
    /// the shared switch, the hosted cartridges, the writers — is this
    /// runtime's.
    pub fn scope(
        self: &Arc<Self>,
        key: RunKey,
        on_transient: Option<TransientSink>,
    ) -> CliRunScope {
        let scope = key.flow_scope();
        let guard = FlowScopeGuard::new(scope.clone(), self.run_flows.clone());
        let recorder = guard.recorder();
        CliRunScope {
            runtime: self.clone(),
            transient_root: self.transient_root.as_ref().map(|root| root.join(&scope)),
            on_transient,
            recorder,
            _guard: guard,
        }
    }

    /// Remove every part-file that was created for a sink of `plan` and was
    /// not given its name: what a run that has ended left behind.
    ///
    /// A run's parts become outputs when it ends and they are named
    /// ([`emit_terminals`](super::cli_output::emit_terminals)). After that —
    /// or after a run that failed, which names nothing — whatever of its
    /// parts is still where it was created belongs to no output, and a
    /// directory of the user's is no place to leave it. Call it once a
    /// plan's run is over, however it ended. Returns how many were removed.
    pub fn discard_parts(&self, plan: &crate::planner::MachinePlan) -> usize {
        self.parts.discard(|sink| plan.nodes.contains_key(sink))
    }

    /// STOP INPUT (15.2 §Runs Stop): close every open live tap — host-opened
    /// feeds directly, cartridge-resolved feeds via a CloseStream frame to
    /// their feed-bearing requests — then AWAIT the evidence that each
    /// stopped request drained and ENDED, and return the verdict. The
    /// machine drains: in-flight items flow through, terminals end, writers
    /// finalize, and the run completes with its outputs. This is the tap-off
    /// control, distinct from aborting the machine. Nothing open to stop is
    /// an error, never a silent success.
    pub async fn stop_live_inputs(
        &self,
    ) -> Result<
        crate::bifaci::live_feed::StopInputsOutcome,
        crate::bifaci::live_feed::StopInputsError,
    > {
        let host_taps_closed = {
            let mut taps = self
                .host_taps
                .lock()
                .expect("CLI host tap registry mutex poisoned");
            for tap in taps.iter() {
                tap.close();
            }
            let n = taps.len() as u64;
            taps.clear();
            n
        };
        let rids: Vec<crate::bifaci::frame::MessageId> = self.run_flows.take_feed_bearing();
        if rids.is_empty() && host_taps_closed == 0 {
            return Err(crate::bifaci::live_feed::StopInputsError::NothingToStop);
        }
        let switch = {
            let host = self.host.lock().await;
            host.ctx.as_ref().map(|ctx| ctx.switch().clone())
        };
        let Some(switch) = switch else {
            if rids.is_empty() {
                return Ok(crate::bifaci::live_feed::StopInputsOutcome {
                    host_taps_closed,
                    feed_requests_stopped: 0,
                    feeds_ended: 0,
                });
            }
            return Err(crate::bifaci::live_feed::StopInputsError::NoHost);
        };
        crate::bifaci::live_feed::stop_feed_requests(
            &switch,
            &rids,
            host_taps_closed,
            crate::bifaci::live_feed::STOP_INPUT_DRAIN_TIMEOUT,
        )
        .await
    }
}

impl CliRuntime {
    fn register_host_tap(&self, handle: &crate::bifaci::live_feed::LiveFeedHandle) {
        let mut taps = self
            .host_taps
            .lock()
            .expect("CLI host tap registry mutex poisoned");
        taps.retain(|t| !t.is_closed());
        taps.push(handle.clone());
    }
}

/// Hears of each intermediate value a scoped run captures. A failure is a
/// hard execution error: a run whose inspection surface diverges from what
/// it captured is in an illegal state.
pub type TransientSink = Arc<dyn Fn(&TransientArtifact) -> Result<(), ExecutionError> + Send + Sync>;

/// One run's view of a [`CliRuntime`] ([`CliRuntime::scope`]): the shared
/// switch and cartridges, with the run's own flow scope and transient
/// directory. Hold it for as long as the run executes; dropping it ends the
/// scope (its flow summary stays readable from the registry).
pub struct CliRunScope {
    runtime: Arc<CliRuntime>,
    transient_root: Option<PathBuf>,
    on_transient: Option<TransientSink>,
    recorder: FlowRecorder,
    _guard: FlowScopeGuard,
}

#[async_trait]
impl EngineRuntime for CliRunScope {
    async fn segment_switch(
        &self,
        graph: &ResolvedGraph,
    ) -> Result<Arc<RelaySwitch>, ExecutionError> {
        self.runtime.segment_switch(graph).await
    }

    async fn activity_timeout_secs(&self, graph: &ResolvedGraph) -> Result<u64, ExecutionError> {
        self.runtime.activity_timeout_secs(graph).await
    }

    fn trace_sink(&self) -> Option<Arc<ProtocolTraceSink>> {
        self.runtime.trace_sink()
    }

    fn flow_observer(&self) -> Option<&dyn crate::orchestrator::stream_io::FlowObserver> {
        Some(&self.recorder)
    }

    fn on_host_feed_open(&self, handle: &crate::bifaci::live_feed::LiveFeedHandle) {
        self.runtime.register_host_tap(handle);
    }

    fn writer_factory(
        &self,
    ) -> Option<Box<crate::orchestrator::stream_io::SegmentWriterFactory>> {
        self.runtime.writer_factory()
    }

    fn transient_artifact_root(&self) -> Option<PathBuf> {
        self.transient_root.clone()
    }

    fn on_transient_artifact(&self, artifact: &TransientArtifact) -> Result<(), ExecutionError> {
        match &self.on_transient {
            Some(sink) => sink(artifact),
            None => Ok(()),
        }
    }

    fn fabric_registry(&self) -> Arc<FabricRegistry> {
        self.runtime.fabric_registry()
    }

    async fn map_failure_policy(&self) -> Result<MapFailurePolicy, ExecutionError> {
        self.runtime.map_failure_policy().await
    }
}

impl CliHost {
    /// Whether a bundled cartridge declares `cap_urn` itself. A cap URN that does
    /// not parse is not the bundle's; the registry resolution names it.
    fn bundle_provides(&self, cap_urn: &str) -> bool {
        crate::urn::cap_urn::CapUrn::from_string(cap_urn)
            .map(|cap| self.bundled_caps.iter().any(|bundled| bundled.is_equivalent(&cap)))
            .unwrap_or(false)
    }

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
        let cap_urns: Vec<&str> = graph.steps.iter().map(|step| step.cap_urn.as_str()).collect();

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

        // Bundled cartridges are discovered once, on the first segment, and hosted
        // beside the dev/registry cartridges this segment resolves.
        let mut resolved = Vec::new();
        if !self.bundled_registered {
            if let Some(dir) = &rt.bundled_cartridges_dir {
                let bundled = discover_bundled_cartridges(
                    dir,
                    rt.channel,
                    rt.registry_url.as_deref(),
                    rt.fabric_manifest_version,
                )
                .await?;
                for (_, _, groups) in &bundled {
                    self.bundled_caps
                        .extend(groups.iter().flat_map(|g| g.caps.iter().map(|c| c.urn.clone())));
                }
                resolved.extend(bundled);
            }
            self.bundled_registered = true;
        }

        // A cap the bundle provides is the bundle's: it is never looked up in a
        // registry. A bundled cartridge is never published to one, and the same
        // identity fetched from a registry would be a second artifact of it, which
        // the host rightly refuses as ambiguous. Resolution's relation, as for dev
        // and registry cartridges: the cap itself (`is_equivalent`), not anything
        // that could serve it.
        let unbundled: Vec<&str> = cap_urns
            .iter()
            .copied()
            .filter(|cap| !self.bundle_provides(cap))
            .collect();
        {
            let manager = self
                .manager
                .as_ref()
                .expect("initialised immediately above");
            resolved.extend(manager.resolve_cartridges(&unbundled).await?);
        }

        // Host only cartridges not already hosted (dedup by binary path).
        resolved.retain(|(path, _, _)| !self.registered_paths.contains(path));

        // Build the bootstrap context on first use. `ExecutionContext::new` creates the
        // switch and starts its background frame pump, so RelayNotify capability updates
        // keep flowing between segments — the reuse invariant depends on it.
        if self.ctx.is_none() {
            // The command line is the operator's own tool: the person running it
            // decides for themselves, so its cartridges are under `full`.
            let ctx = ExecutionContext::new(
                rt.fabric_registry.clone(),
                crate::bifaci::policy::HostPolicy::new(crate::bifaci::policy::NetworkPolicy::Full),
            )
            .await?;
            // Every termination folds into its run's flow summary the moment
            // it happens (the switch's terminated ring is bounded — reading it
            // later would miss some). The fold is one map update, no I/O: it
            // runs under the request-table guard.
            let flows = rt.run_flows.clone();
            ctx.switch()
                .set_terminate_observer(Box::new(move |summary| {
                    flows.accumulate_termination(summary)
                }))
                .await;
            self.ctx = Some(ctx);
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
        Some(&self.unscoped_recorder)
    }

    fn on_host_feed_open(&self, handle: &crate::bifaci::live_feed::LiveFeedHandle) {
        self.register_host_tap(handle);
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
        let parts = self.parts.clone();
        Some(Box::new(move |sink: &str, coordinate| {
            let n = seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // `n` alone makes the name unique; the run it belongs to makes it
            // readable — every enclosing run's index, outermost first.
            let tag = match &coordinate {
                Some(c) => {
                    let run: Vec<String> = c
                        .enclosing
                        .iter()
                        .map(|outer| outer.body_index)
                        .chain(std::iter::once(c.body_index))
                        .map(|index| index.to_string())
                        .collect();
                    format!("{n}.{sink}.b{}", run.join("-"))
                }
                None => format!("{n}.{sink}"),
            };
            Box::new(crate::orchestrator::cli_writer::CliDiskWriter::new(
                dir.clone(),
                sink,
                tag,
                parts.clone(),
            ))
        }))
    }

    fn fabric_registry(&self) -> Arc<FabricRegistry> {
        self.fabric_registry.clone()
    }

    async fn map_failure_policy(&self) -> Result<MapFailurePolicy, ExecutionError> {
        // Reference regime: expose failures. A machine that wants a failed
        // item dropped says so on the step (`on_error=absent`).
        Ok(MapFailurePolicy::Fail)
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
