//! `execute_plan` — runs a [`MachinePlan`].
//!
//! A plan is steps, the values they read and produce, and the maps they run
//! in. This module decides what runs when; how one pipelined chain of steps is
//! run — cartridges, frames, credit — is the segment executor's, behind the
//! pluggable [`EngineRuntime`].
//!
//! ## Scopes and units
//!
//! A plan is run scope by scope: the machine itself, and the body of each map.
//! A scope's work is its **units**:
//!
//! - a **chain**: steps whose responses stream one into the next
//!   ([`decompose_step_chains`](super::executor::decompose_step_chains)). It
//!   is handed whole to [`EngineRuntime::run_segment`];
//! - a **map**: a `ForEach`. Its body — itself a scope — runs once per item of
//!   the sequences it zips, and what the body produces is collected, in item
//!   order, into sequences the enclosing scope reads.
//!
//! A unit starts as soon as every value it reads has been produced or is known
//! to be absent; units with no path between them run at the same time
//! (15.2 §Scheduling). Nothing here limits how much runs at once: a started
//! request waits for its cap's pools like any dispatch.
//!
//! ## What a step is given
//!
//! A value is **absent** when the output that would have produced it did not
//! fire, its step was skipped, or its step failed under `on_error=absent`
//! (15.2 §Absent values and skipped steps). A step with a required arg wired
//! to an absent value is skipped; an arg that is not required is not sent; a
//! gather sends the members that are present.
//!
//! ## Failure
//!
//! A step that fails is attempted again when its policy says so and the
//! failure's class allows it, and leaves its outputs absent when its policy
//! tolerates the failure. Otherwise the failure ends the run: every other
//! live request is cancelled as its collateral ([`RunCancel`]) before anything
//! is joined, and the failure itself is what `execute_plan` returns.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::cap::registry::FabricRegistry;
use crate::failure::AttributionClass;
use crate::machine::OnError;
use crate::orchestrator::executor::{
    decompose_step_chains, topological_sort_steps, CollateralCause, RunCancel,
};
use crate::orchestrator::plan_converter::steps_to_resolved_graph;
use crate::orchestrator::run_arguments::RunArgumentLedger;
use crate::orchestrator::stream_io::{PipelineLogRecord, PipelineProgressTracker};
use crate::orchestrator::types::{ResolvedGraph, ResolvedStep, StepSource};
use crate::planner::plan::MapRun;
use crate::planner::plan_analysis::derive_output_media_urn;
use crate::planner::{ExecutionNodeType, MachinePlan, StepToken};
use crate::{
    BodyOutcome, CapProgressFn, CapStepProgressFn, ExecutionError, PipelineLogFn, StreamMeta,
    PIPELINE_STALL_TIMEOUT_SECS,
};

/// How much of an item's bytes to keep as a preview snippet.
const ITEM_PREVIEW_CAP: usize = 2048;

// =============================================================================
// Result + callback types
// =============================================================================

/// Result of executing a plan to its end.
#[derive(Debug, Clone)]
pub struct PipelineResult {
    /// One entry per plan `Output` node, in node-id order. A machine with no
    /// output has none.
    pub terminals: Vec<PlanTerminal>,
    /// Per-run outcome of every map's body (and, for a plan with no map, one
    /// entry for the whole run).
    pub body_outcomes: Vec<BodyOutcome>,
    /// What became of every step that did not simply run once and complete:
    /// skipped, attempted again, or failed and tolerated. In the order it
    /// happened.
    pub steps: Vec<StepReport>,
    /// How the run ended. A run that failed or was cancelled has no result:
    /// it is the error `execute_plan` returns.
    pub verdict: RunVerdict,
}

impl PipelineResult {
    /// The terminal for a given `Output` node id.
    pub fn terminal(&self, output_node_id: &str) -> Option<&PlanTerminal> {
        self.terminals
            .iter()
            .find(|t| t.output_node_id() == output_node_id)
    }
}

/// How a run that reached its end ended (15.2 §Error handling). The other two
/// ways a run ends — failed, cancelled — are errors, not results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunVerdict {
    /// Every step ran or was skipped; nothing failed.
    Completed,
    /// Every output that could be produced was, and at least one step — or
    /// one run of a map — failed and was tolerated. Never reported as success.
    CompletedWithToleratedFailures,
}

/// A failure a step had: the identity its source declared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepFailure {
    pub class: AttributionClass,
    /// The machine-readable code, when the source declared one.
    pub code: Option<String>,
    pub message: String,
}

impl StepFailure {
    fn of(error: &ExecutionError) -> Self {
        Self {
            class: error.attribution_class(),
            code: error.failure_code().map(str::to_string),
            message: error.failure_reason(),
        }
    }
}

/// What became of a step, other than running once and completing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum StepOutcome {
    /// It completed after failing: each failed attempt, in order.
    Completed { failed_attempts: Vec<StepFailure> },
    /// It did not run: a required arg of it was wired to an absent value.
    /// `absent` names the nodes that were. Every output of it is absent.
    Skipped { absent: Vec<String> },
    /// It failed for good and its policy tolerates that: every output of it
    /// is absent and the run went on. `failure` is the last attempt's;
    /// `failed_attempts` the ones before it.
    FailedTolerated {
        failure: StepFailure,
        failed_attempts: Vec<StepFailure>,
    },
}

/// One step's entry in a run's report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepReport {
    pub step_token_id: StepToken,
    pub cap_urn: String,
    /// The run of the map the step was in, when it is in one.
    pub body: Option<ForEachBodyCoordinate>,
    #[serde(flatten)]
    pub outcome: StepOutcome,
}

/// One plan terminal (`Output` node): what was produced at it, or that
/// nothing was.
#[derive(Debug, Clone)]
pub enum PlanTerminal {
    Produced(ProducedTerminal),
    /// The node is absent at the end of the run: the output that would have
    /// produced it did not fire, or its step was skipped or failed and was
    /// tolerated. Not an empty value, and not an error.
    NotProduced {
        output_node_id: String,
        media_urn: String,
    },
}

impl PlanTerminal {
    /// The plan `Output` node id this terminal corresponds to.
    pub fn output_node_id(&self) -> &str {
        match self {
            PlanTerminal::Produced(produced) => &produced.output_node_id,
            PlanTerminal::NotProduced { output_node_id, .. } => output_node_id,
        }
    }

    /// The media URN the terminal holds — or would have.
    pub fn media_urn(&self) -> &str {
        match self {
            PlanTerminal::Produced(produced) => &produced.media_urn,
            PlanTerminal::NotProduced { media_urn, .. } => media_urn,
        }
    }

    /// What was produced, when something was.
    pub fn produced(&self) -> Option<&ProducedTerminal> {
        match self {
            PlanTerminal::Produced(produced) => Some(produced),
            PlanTerminal::NotProduced { .. } => None,
        }
    }
}

/// The data produced at a plan terminal.
///
/// When the runtime persists terminal output, `writer_results` carries the file paths
/// and `items` is empty; otherwise `items` holds the in-memory data.
#[derive(Debug, Clone)]
pub struct ProducedTerminal {
    /// The plan `Output` node id this terminal corresponds to (e.g. `output_5`).
    pub output_node_id: String,
    /// Unwrapped output blobs. Empty when `writer_results` is populated (on disk).
    pub items: Vec<OutputItem>,
    /// Whether the output is a sequence — an output declared one, or what a
    /// map collected (structurally a sequence whatever the item count).
    pub is_sequence: bool,
    /// The media URN the terminal holds in this run.
    pub media_urn: String,
    /// Results from any incremental disk writes. Empty when the runtime does not persist.
    pub writer_results: Vec<WriterResult>,
}

/// A single unwrapped output item.
#[derive(Debug, Clone)]
pub struct OutputItem {
    /// Raw bytes (CBOR transport stripped).
    pub data: Vec<u8>,
    /// Item index (0 for scalar, 0..N for sequence items).
    pub index: usize,
}

/// Result of a runtime persisting one output of a segment to disk.
#[derive(Debug, Clone)]
pub struct WriterResult {
    pub is_sequence: bool,
    pub media_urn: String,
    /// Blob: single path. Sequence: one path per item.
    pub saved_paths: Vec<String>,
    pub total_bytes: usize,
    /// Blob-mode stream meta (STREAM_START); `None` in sequence mode.
    pub stream_meta: Option<StreamMeta>,
    /// Sequence-mode per-item meta; empty in blob mode.
    pub item_metas: Vec<Option<StreamMeta>>,
}

/// One value a plan execution — or one segment of it — is given.
///
/// `Bytes` is materialized content: one value's raw bytes, or, for a
/// sequence, an RFC 8742 CBOR sequence of byte-string items. A `Spool` is an
/// ended unbounded stream on disk in that same byte form, sent from the file
/// and never loaded. A `LiveReference` is a live-capture SOURCE (13.2
/// §Reference Media, live family): the selector bytes travel to the first
/// consuming cap labeled with the REFERENCE urn, and that cap's cartridge
/// resolves capture — the engine never touches a device. The machine then
/// stops per 15.2 §Runs Stop (stop condition, operator stop → drain, or
/// abort).
#[derive(Debug, Clone)]
pub enum PlanInput {
    Bytes(Vec<u8>),
    Spool { path: std::path::PathBuf },
    LiveReference {
        reference_urn: String,
        selector: Vec<u8>,
    },
}

impl PlanInput {
    pub fn live_reference_urn(&self) -> Option<&str> {
        match self {
            PlanInput::LiveReference { reference_urn, .. } => Some(reference_urn),
            _ => None,
        }
    }
}

/// Stable address of one run of a map's body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForEachBodyCoordinate {
    pub foreach_token_id: StepToken,
    pub body_index: usize,
    /// The runs this one is inside, when its map is opened inside another's
    /// body: outermost first. Empty for a map of the machine itself. A body
    /// index alone does not name a run of a nested map — the same index
    /// recurs under every run of the map around it.
    #[serde(default)]
    pub enclosing: Vec<MapRun>,
}

impl ForEachBodyCoordinate {
    /// This run and the runs it is inside, outermost first.
    fn path(&self) -> Vec<MapRun> {
        let mut path = self.enclosing.clone();
        path.push(MapRun {
            foreach_token_id: self.foreach_token_id.clone(),
            body_index: self.body_index,
        });
        path
    }
}

/// Transient description of one item a map's body is about to be run on. The
/// bytes remain on the pipeline wire; only this bounded UI snapshot leaves the
/// executor. A map that zips several sequences is described by the item of
/// its first source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForEachItemSnapshot {
    pub foreach_token_id: StepToken,
    pub body_index: usize,
    /// The runs this one is inside (see [`ForEachBodyCoordinate::enclosing`]).
    #[serde(default)]
    pub enclosing: Vec<MapRun>,
    pub item_preview_text: Option<String>,
    pub item_byte_count: u64,
}

/// Per-body outcome callback — delivers the running set of body outcomes.
pub type BodyOutcomeFn = Arc<dyn Fn(&[BodyOutcome]) -> Result<(), ExecutionError> + Send + Sync>;

/// Delivers ForEach input-item snapshots APPEND-ONLY: each call carries the
/// next item(s) in body-index order, published before the item's body
/// spawns. A bounded region delivers its items one by one as they dispatch;
/// a live-fed region cannot do otherwise — the total is unknown until the
/// feed ends. Consumers accumulate; a publication that does not continue
/// the stored run is a contract violation on their side.
pub type ForEachItemsFn =
    Arc<dyn Fn(&str, &[ForEachItemSnapshot]) -> Result<(), ExecutionError> + Send + Sync>;

/// Delivers each step's report the moment its outcome is known.
pub type StepReportFn = Arc<dyn Fn(&StepReport) + Send + Sync>;

/// What a host is told while a plan runs. Every one is optional.
#[derive(Clone, Default)]
pub struct RunCallbacks {
    /// The run's overall progress, `0.0..=1.0`: the sum of what every step has
    /// contributed (15.2 §Progress subdivision). Never goes down.
    pub progress: Option<CapProgressFn>,
    /// A single step's own progress, under its step token; a map's aggregate
    /// over its runs, under the map's token.
    pub step_progress: Option<CapStepProgressFn>,
    pub log: Option<PipelineLogFn>,
    pub body_outcome: Option<BodyOutcomeFn>,
    pub foreach_items: Option<ForEachItemsFn>,
    pub step_report: Option<StepReportFn>,
}

/// Output of running one resolved segment through an [`EngineRuntime`]: every
/// materialised node's items, each node's cardinality, per-persisted-sink
/// writer results, spooled nodes, and the nodes that are absent.
pub use crate::orchestrator::executor::DagOutput as SegmentOutput;

// =============================================================================
// EngineRuntime — the pluggable cartridge-execution backend
// =============================================================================

/// What a host does with a run of a map's body that fails and whose step
/// does not tolerate the failure itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MapFailurePolicy {
    /// It fails the run, like any step's failure.
    Fail,
    /// The item is dropped: it contributes nothing to what the map collects,
    /// its failure is recorded on its body outcome, and the run goes on and
    /// ends *completed with tolerated failures* — unless every run of the
    /// map failed, which fails the run.
    DropItem,
}

/// The cartridge-execution backend `execute_plan` streams segments through.
///
/// Implementations differ only in how cartridges are hosted and whether terminal
/// output is persisted — scheduling, maps and result assembly are backend
/// independent and live in [`execute_plan`].
#[async_trait]
pub trait EngineRuntime: Send + Sync {
    // ── Backend hooks: the ONLY things that differ between the engine and the CLI ──
    //
    // Every backend reuses ONE long-lived `RelaySwitch` across all segments (including
    // every ForEach body), so a cap's cartridge process is spawned once and each body
    // multiplexes onto it. That single process's declared concurrency pools (e.g. a
    // capacity-1 "gpu" pool) then serialize the model loads — which is what stops a
    // ForEach fan-out from loading N model copies into GPU memory at once. This contract is identical for the engine
    // (`EngineHostRuntime`, daemon-hosted cartridges) and the CLI (`CliRuntime`,
    // in-process dev-bin cartridges); only the hooks below differ.

    /// The long-lived relay switch to run this segment's caps on. `graph` lets a
    /// backend lazily ensure the segment's cartridges are hosted before returning the
    /// switch (the CLI registers dev-bin cartridge hosts on demand; the engine ignores
    /// `graph` and returns its already-populated switch). Called once per segment —
    /// segments run at the same time, so it is called concurrently; the returned
    /// switch is shared, never torn down here.
    async fn segment_switch(
        &self,
        graph: &ResolvedGraph,
    ) -> Result<Arc<crate::bifaci::relay_switch::RelaySwitch>, ExecutionError>;

    /// Per-item activity timeout (seconds) for this segment. The engine reads it from
    /// its config service (a config-service failure is propagated — fail hard, never
    /// silently defaulted); the CLI reads it from the terminal cap's metadata. A value
    /// the source does not specify uses the documented default, but an *error* fetching
    /// it aborts the segment.
    async fn activity_timeout_secs(&self, graph: &ResolvedGraph) -> Result<u64, ExecutionError>;

    /// Disk writer factory for persisted terminal sinks. `Some` for the engine (each
    /// persisted sink gets a writer bound to the run artifact dir); `None` for a
    /// runtime that keeps everything in memory. Default `None`.
    fn writer_factory(&self) -> Option<Box<crate::orchestrator::stream_io::SegmentWriterFactory>> {
        None
    }

    /// Flow observer correlating request ids to strand steps for run flow snapshots.
    /// `Some` for the engine; `None` for the CLI. Default `None`.
    fn flow_observer(&self) -> Option<&dyn crate::orchestrator::stream_io::FlowObserver> {
        None
    }

    /// A live feed was opened BY THE HOST (13.2 §Reference Media, host
    /// resolution — a live source driving engine-side per-item dispatch).
    /// The engine registers the tap so a run stop closes it (close-tap →
    /// the feed ends → in-flight bodies drain → the run completes as a
    /// valid stopped run, 15.2 §Runs Stop). The CLI's contract is the
    /// default no-op: its runs end with the process, and teardown releases
    /// the devices.
    fn on_host_feed_open(&self, _handle: &crate::bifaci::live_feed::LiveFeedHandle) {}

    /// Root directory for TRANSIENT run artifacts (the engine: the run's
    /// `run_artifacts/{id}/transient`). When `Some`, every INTERMEDIATE
    /// chain sink — memory-materialized or spooled — is captured there the
    /// moment it materializes (data + `provenance.json` sidecar), making
    /// mid-strand media inspectable with an eagerly-reaped disk lifetime.
    /// `None` (the CLI, reference runtimes): intermediates are discarded as
    /// before — no inspection surface, no capture.
    fn transient_artifact_root(&self) -> Option<std::path::PathBuf> {
        None
    }

    /// A transient artifact was captured — the node just materialized,
    /// possibly while later steps still run. The engine publishes it on the
    /// run-media channel so the loom can enable the node immediately. A
    /// publication failure is a hard execution error: a run whose inspection
    /// surface silently diverges from its disk state is an illegal state.
    /// Default no-op.
    fn on_transient_artifact(
        &self,
        _artifact: &crate::orchestrator::transient::TransientArtifact,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    /// Per-segment protocol trace sink. `Some` when the CLI's `--trace` /
    /// the scenario harness's `CAPDAG_SCENARIO_TRACE` is active — the segment is then
    /// sampled live (250ms) and snapshotted at teardown. `None` otherwise (the engine
    /// runs its own switch-level dev trace instead). Default `None`.
    fn trace_sink(&self) -> Option<Arc<crate::bifaci::protocol_trace::ProtocolTraceSink>> {
        None
    }

    /// The fabric registry: the cap definitions the plan was built on.
    fn fabric_registry(&self) -> Arc<FabricRegistry>;

    /// What becomes of a run of a map's body that fails. A host that cannot
    /// say (its setting is unreadable, or says something else) fails the run
    /// with that error — it is never guessed.
    async fn map_failure_policy(&self) -> Result<MapFailurePolicy, ExecutionError>;

    // ── Provided orchestration: identical for every backend ──

    /// Run one resolved segment: a graph of steps, pipelined where it can be
    /// (`execute_plan` hands it one pipelined chain at a time, and several at
    /// once).
    ///
    /// This is the abstraction's job and is the SAME for every backend: get the shared
    /// switch, build a per-segment [`ExecutionContext`] over it, set inputs, wire the
    /// optional protocol trace, and drive `run_dag_on_context`. Backends customise only
    /// via the hooks above — they do not override this.
    ///
    /// - `body_coordinate`: the run of a map's body this segment is part of.
    /// - `persist_sinks`: the nodes whose value is a plan terminal. A persisting
    ///   runtime writes each to disk (one writer per sink) and returns its
    ///   `writer_results`; every other node is kept in memory, or spooled when
    ///   it turns out unbounded.
    /// - `cancel`: the run's failure signal. Once it is raised the segment's
    ///   requests are cancelled as the collateral of that failure and it ends
    ///   with [`ExecutionError::Collateral`].
    #[allow(clippy::too_many_arguments)]
    async fn run_segment(
        &self,
        graph: &ResolvedGraph,
        initial_inputs: HashMap<String, PlanInput>,
        initial_is_sequence: HashMap<String, bool>,
        cap_arguments: &HashMap<String, Vec<(String, Vec<u8>)>>,
        progress_fn: Option<&CapProgressFn>,
        step_progress_fn: Option<&CapStepProgressFn>,
        log_fn: Option<&PipelineLogFn>,
        body_coordinate: Option<ForEachBodyCoordinate>,
        stall_tracker: Option<Arc<PipelineProgressTracker>>,
        persist_sinks: &HashSet<String>,
        cancel: &RunCancel,
    ) -> Result<SegmentOutput, ExecutionError> {
        use crate::orchestrator::executor::{run_dag_on_context, ExecutionContext};

        let activity_timeout_secs = self.activity_timeout_secs(graph).await?;

        // Shared, long-lived switch (cartridges hosted + reused). The per-segment
        // context is lightweight: its own node_data, no cleanup handles — so dropping
        // it at the end of the segment does NOT tear down the shared cartridge host.
        let switch = self.segment_switch(graph).await?;
        let mut ctx = ExecutionContext::from_switch(switch).await?;

        // Strict 1:1 between initial_inputs and initial_is_sequence — every input node
        // carries an explicit scalar/sequence flag (the invariant run_dag_on_context
        // relies on; a missing flag would silently send a sequence as a scalar blob).
        let input_keys: HashSet<&str> = initial_inputs.keys().map(|s| s.as_str()).collect();
        let flag_keys: HashSet<&str> = initial_is_sequence.keys().map(|s| s.as_str()).collect();
        let missing: Vec<&str> = input_keys.difference(&flag_keys).copied().collect();
        if !missing.is_empty() {
            return Err(ExecutionError::HostError(format!(
                "run_segment: initial_is_sequence is missing flags for input node(s) {missing:?}"
            )));
        }
        let extra: Vec<&str> = flag_keys.difference(&input_keys).copied().collect();
        if !extra.is_empty() {
            return Err(ExecutionError::HostError(format!(
                "run_segment: initial_is_sequence has stale flags for node(s) {extra:?}"
            )));
        }
        for (node, input) in initial_inputs {
            let is_seq = *initial_is_sequence
                .get(&node)
                .expect("key set verified above");
            ctx.set_node_is_sequence(node.clone(), is_seq);
            match input {
                PlanInput::Bytes(bytes) => ctx.set_node_data(node, bytes),
                // On disk: sent from the file, never loaded.
                PlanInput::Spool { path } => ctx.set_node_spool(node, path),
                // The selector is what is sent, labeled with the reference URN.
                PlanInput::LiveReference {
                    reference_urn,
                    selector,
                } => {
                    ctx.set_node_live_reference(node.clone(), reference_urn);
                    ctx.set_node_data(node, selector);
                }
            }
        }

        // Optional per-segment protocol trace (CLI). When a sink is supplied, sample the
        // switch's L8 snapshot live every 250ms so a HANGING segment still leaves a stall
        // line, and snapshot once more at teardown on both the Ok and Err paths. A live
        // sample write failure is logged and swallowed; the final snapshot is fail-hard.
        let trace_sink = self.trace_sink();
        let trace_label = trace_sink.as_ref().map(|_| {
            graph
                .steps
                .last()
                .map(|step| step.cap_urn.clone())
                .unwrap_or_else(|| "empty-graph".to_string())
        });
        let trace_sampler = match (&trace_sink, &trace_label) {
            (Some(sink), Some(label)) => {
                let switch = ctx.switch().clone();
                let sink = sink.clone();
                let label = label.clone();
                Some(tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(250));
                    loop {
                        ticker.tick().await;
                        let stats = switch.protocol_stats().await;
                        if let Err(e) = sink.record_deduped(&stats, &label).await {
                            tracing::debug!(
                                error = %e, segment = %label,
                                "protocol trace live sample failed (continuing)"
                            );
                        }
                    }
                }))
            }
            _ => None,
        };

        let writer = self.writer_factory();
        let observer = self.flow_observer();
        let transient_root = self.transient_artifact_root();
        let on_transient = |artifact: &crate::orchestrator::transient::TransientArtifact| {
            self.on_transient_artifact(artifact)
        };
        let out = run_dag_on_context(
            &mut ctx,
            graph,
            cap_arguments,
            progress_fn,
            step_progress_fn,
            log_fn,
            stall_tracker,
            writer.as_deref(),
            body_coordinate,
            persist_sinks,
            activity_timeout_secs,
            observer,
            transient_root.as_deref(),
            Some(&on_transient),
            Some(cancel),
        )
        .await;

        // Stop the live sampler before the final snapshot so they cannot race on the file.
        if let Some(handle) = trace_sampler {
            handle.abort();
            let _ = handle.await;
        }
        if let (Some(sink), Some(label)) = (&trace_sink, &trace_label) {
            let stats = ctx.switch().protocol_stats().await;
            if let Err(e) = sink.record_deduped(&stats, label).await {
                match &out {
                    Ok(_) => {
                        return Err(ExecutionError::HostError(format!(
                            "protocol trace write failed for segment '{label}': {e}"
                        )))
                    }
                    Err(_) => tracing::error!(
                        error = %e, segment = %label,
                        "protocol trace write failed on the segment error path"
                    ),
                }
            }
        }
        out
    }
}

// =============================================================================
// What a node holds in a run
// =============================================================================

/// What a node holds in a run.
#[derive(Debug, Clone)]
enum Held {
    /// Decoded items in memory: exactly one for a single value, any number
    /// for a sequence.
    Memory {
        items: Arc<Vec<Vec<u8>>>,
        is_sequence: bool,
    },
    /// An ended unbounded stream on disk, in the `node_data` byte form.
    Spooled {
        path: std::path::PathBuf,
        is_sequence: bool,
    },
    /// A live reference the run was given at an input of the machine.
    Live {
        reference_urn: String,
        selector: Vec<u8>,
        is_sequence: bool,
    },
    /// A plan terminal the runtime persisted: its data is where the writers
    /// put it. Nothing reads it but the run's result.
    Persisted {
        writers: Vec<WriterResult>,
        is_sequence: bool,
    },
    /// Not produced (15.2 §Absent values and skipped steps). Not empty.
    Absent,
}

/// The values of a scope, by node.
type Values = HashMap<String, Held>;

impl Held {
    fn memory(items: Vec<Vec<u8>>, is_sequence: bool) -> Self {
        Held::Memory {
            items: Arc::new(items),
            is_sequence,
        }
    }

    /// What a segment is given of this value, with its cardinality.
    fn as_plan_input(&self, node: &str) -> Result<(PlanInput, bool), ExecutionError> {
        match self {
            Held::Memory { items, is_sequence } => {
                let bytes = if *is_sequence {
                    crate::orchestrator::cbor_util::wrap_raw_items_as_cbor_sequence(items)
                        .map_err(|e| {
                            ExecutionError::HostError(format!(
                                "node '{node}': its items cannot be sent as a sequence: {e}"
                            ))
                        })?
                } else {
                    match items.as_slice() {
                        [one] => one.clone(),
                        other => {
                            return Err(ExecutionError::HostError(format!(
                                "node '{node}' holds {} items for a single value",
                                other.len()
                            )));
                        }
                    }
                };
                Ok((PlanInput::Bytes(bytes), *is_sequence))
            }
            Held::Spooled { path, is_sequence } => {
                Ok((PlanInput::Spool { path: path.clone() }, *is_sequence))
            }
            Held::Live {
                reference_urn,
                selector,
                is_sequence,
            } => Ok((
                PlanInput::LiveReference {
                    reference_urn: reference_urn.clone(),
                    selector: selector.clone(),
                },
                *is_sequence,
            )),
            Held::Persisted { .. } => Err(ExecutionError::HostError(format!(
                "node '{node}' is a plan terminal persisted by the runtime; a step cannot read it"
            ))),
            Held::Absent => Err(ExecutionError::HostError(format!(
                "node '{node}' is absent; an absent value is never sent"
            ))),
        }
    }
}

/// What a plan was given at one of its inputs, as a value of the run.
fn held_from_input(node: &str, input: PlanInput, is_sequence: bool) -> Result<Held, ExecutionError> {
    Ok(match input {
        PlanInput::Bytes(bytes) if is_sequence => Held::memory(
            crate::orchestrator::stream_io::decode_terminal_output(&bytes, Some(true)).map_err(
                |e| {
                    ExecutionError::HostError(format!(
                        "input '{node}' is given as a sequence, and its bytes are not a CBOR \
                         sequence of byte strings: {e}"
                    ))
                },
            )?,
            true,
        ),
        PlanInput::Bytes(bytes) => Held::memory(vec![bytes], false),
        PlanInput::Spool { path } => Held::Spooled { path, is_sequence },
        PlanInput::LiveReference {
            reference_urn,
            selector,
        } => Held::Live {
            reference_urn,
            selector,
            is_sequence,
        },
    })
}

/// What a scope can read that it does not produce: what the scopes around it
/// hold (`shared`, the same for every run of a map's body) and what this run
/// was given (`given`: a body's items; the machine's inputs).
#[derive(Clone)]
struct Env {
    shared: Arc<Values>,
    given: Arc<Values>,
}

impl Env {
    fn get(&self, node: &str) -> Option<&Held> {
        self.given.get(node).or_else(|| self.shared.get(node))
    }
}

// =============================================================================
// The shape of a plan: scopes, units, what crosses a map's boundary
// =============================================================================

/// A scope of a plan: the id of the map whose body it is, or `None` for the
/// machine itself.
type ScopeKey = Option<String>;

enum UnitKind {
    /// A pipelined chain: indices into its scope's graph, in order.
    Chain(Vec<usize>),
    /// A map opened in the scope.
    Map(String),
}

/// One thing a scope runs.
struct Unit {
    kind: UnitKind,
    /// The nodes whose values it must be given before it starts.
    reads: Vec<String>,
    /// The nodes whose values it gives the scope when it ends.
    produces: Vec<String>,
    /// How many steps it is: its share of the scope's progress.
    steps: usize,
}

struct ScopeUnits {
    /// Every step of the scope.
    graph: ResolvedGraph,
    units: Vec<Unit>,
    /// `waits_on[i]`: the units that produce something unit `i` reads.
    waits_on: Vec<Vec<usize>>,
}

struct PlanShape {
    scopes: HashMap<ScopeKey, Arc<ScopeUnits>>,
    /// map → every step inside it at any depth, by node id, sorted.
    map_steps: HashMap<String, Vec<String>>,
    /// map → the cap URNs of those steps, in the same order.
    map_cap_urns: HashMap<String, Vec<String>>,
    /// map → the nodes inside it that something outside it reads, sorted.
    exports: HashMap<String, Vec<String>>,
}

impl PlanShape {
    fn of(plan: &MachinePlan, registry: &FabricRegistry) -> Result<Self, ExecutionError> {
        let invalid = |message: String| ExecutionError::HostError(format!("plan '{}': {message}", plan.name));
        let enclosing = plan.enclosing_maps();
        let scope_of = |node: &str| -> ScopeKey { enclosing.get(node).map(|map| map.to_string()) };
        // The maps around a node, innermost first.
        let around = |node: &str| -> Vec<&str> {
            let mut maps = Vec::new();
            let mut at = node;
            while let Some(map) = enclosing.get(at) {
                maps.push(*map);
                at = map;
            }
            maps
        };

        let mut ids: Vec<&String> = plan.nodes.keys().collect();
        ids.sort();
        let mut steps_in: HashMap<ScopeKey, Vec<&str>> = HashMap::new();
        let mut maps_in: HashMap<ScopeKey, Vec<&str>> = HashMap::new();
        steps_in.entry(None).or_default();
        for id in &ids {
            match &plan.nodes[*id].node_type {
                ExecutionNodeType::Cap { .. } => {
                    steps_in.entry(scope_of(id)).or_default().push(id.as_str())
                }
                ExecutionNodeType::ForEach { .. } => {
                    maps_in.entry(scope_of(id)).or_default().push(id.as_str());
                    steps_in.entry(Some(id.to_string())).or_default();
                }
                _ => {}
            }
        }

        // Who reads each value, and from where. A value read from outside a
        // map it is in is one of that map's exports; a value read inside a
        // map it is not in is one of that map's imports; a value of a scope's
        // step read by anything but a step or a gather of that same scope must
        // be materialised — it cannot be pipelined past its reader.
        let mut exports: HashMap<String, std::collections::BTreeSet<String>> = HashMap::new();
        let mut imports: HashMap<String, std::collections::BTreeSet<String>> = HashMap::new();
        let mut keep: HashMap<ScopeKey, HashSet<String>> = HashMap::new();
        for edge in &plan.edges {
            let (Some(from), Some(to)) = (plan.nodes.get(&edge.from_node), plan.nodes.get(&edge.to_node))
            else {
                return Err(invalid(format!(
                    "an edge names '{}' -> '{}', and one of them is not in the plan",
                    edge.from_node, edge.to_node
                )));
            };
            let holds_value = matches!(
                from.node_type,
                ExecutionNodeType::InputSlot { .. }
                    | ExecutionNodeType::Cap { .. }
                    | ExecutionNodeType::CapOutput { .. }
                    | ExecutionNodeType::Item { .. }
            );
            if !holds_value {
                continue; // a map into its item, a gather into its step
            }
            let reader_around: Vec<&str> = match &to.node_type {
                // A step's own output node does not read it.
                ExecutionNodeType::CapOutput { cap_node, .. } if *cap_node == edge.from_node => {
                    continue;
                }
                ExecutionNodeType::Cap { .. }
                | ExecutionNodeType::Collect { .. }
                | ExecutionNodeType::ForEach { .. } => around(&edge.to_node),
                // A plan output is read by the machine.
                ExecutionNodeType::Output { .. } => Vec::new(),
                other => {
                    return Err(invalid(format!(
                        "'{}' is wired into '{}', which reads nothing ({other:?})",
                        edge.from_node, edge.to_node
                    )));
                }
            };
            let from_around = around(&edge.from_node);
            for map in &from_around {
                if !reader_around.contains(map) {
                    exports
                        .entry(map.to_string())
                        .or_default()
                        .insert(edge.from_node.clone());
                }
            }
            for map in &reader_around {
                if !from_around.contains(map) {
                    imports
                        .entry(map.to_string())
                        .or_default()
                        .insert(edge.from_node.clone());
                }
            }
            let read_in_place = matches!(
                to.node_type,
                ExecutionNodeType::Cap { .. } | ExecutionNodeType::Collect { .. }
            ) && scope_of(&edge.to_node) == scope_of(&edge.from_node);
            if !read_in_place {
                keep.entry(scope_of(&edge.from_node))
                    .or_default()
                    .insert(edge.from_node.clone());
            }
        }

        let mut map_steps: HashMap<String, Vec<String>> = HashMap::new();
        let mut map_cap_urns: HashMap<String, Vec<String>> = HashMap::new();
        for id in &ids {
            if !matches!(plan.nodes[*id].node_type, ExecutionNodeType::ForEach { .. }) {
                continue;
            }
            let mut inside: Vec<String> = plan
                .map_members(id)
                .into_iter()
                .filter(|member| plan.nodes[member].is_cap())
                .collect();
            inside.sort();
            map_cap_urns.insert(
                id.to_string(),
                inside
                    .iter()
                    .filter_map(|step| plan.nodes[step].cap_urn().map(str::to_string))
                    .collect(),
            );
            map_steps.insert(id.to_string(), inside);
        }

        let no_keep = HashSet::new();
        let mut scopes: HashMap<ScopeKey, Arc<ScopeUnits>> = HashMap::new();
        for (scope, step_ids) in &steps_in {
            let graph = steps_to_resolved_graph(plan, step_ids, registry)
                .map_err(|e| invalid(e.to_string()))?;
            let order = topological_sort_steps(&graph.steps)?;
            let chains =
                decompose_step_chains(&graph.steps, &order, keep.get(scope).unwrap_or(&no_keep));

            let mut units: Vec<Unit> = Vec::new();
            for chain in chains {
                let head = &graph.steps[chain[0]];
                let last = &graph.steps[*chain.last().expect("a chain has a step")];
                let mut reads: Vec<String> = Vec::new();
                for node in head.source_nodes() {
                    if !reads.contains(node) {
                        reads.push(node.clone());
                    }
                }
                units.push(Unit {
                    reads,
                    produces: last.produced_nodes().cloned().collect(),
                    steps: chain.len(),
                    kind: UnitKind::Chain(chain),
                });
            }
            for map in maps_in.get(scope).map(Vec::as_slice).unwrap_or(&[]) {
                let ExecutionNodeType::ForEach { sources, .. } = &plan.nodes[*map].node_type else {
                    unreachable!("maps_in holds ForEach nodes");
                };
                let mut reads: Vec<String> = Vec::new();
                for node in sources
                    .iter()
                    .map(|source| &source.node)
                    .chain(imports.get(*map).into_iter().flatten())
                {
                    if !reads.contains(node) {
                        reads.push(node.clone());
                    }
                }
                units.push(Unit {
                    reads,
                    produces: exports
                        .get(*map)
                        .map(|nodes| nodes.iter().cloned().collect())
                        .unwrap_or_default(),
                    steps: map_steps[*map].len(),
                    kind: UnitKind::Map(map.to_string()),
                });
            }

            let mut produced_by: HashMap<&str, usize> = HashMap::new();
            for (index, unit) in units.iter().enumerate() {
                for node in &unit.produces {
                    produced_by.insert(node.as_str(), index);
                }
            }
            let waits_on: Vec<Vec<usize>> = units
                .iter()
                .enumerate()
                .map(|(index, unit)| {
                    let mut waits: Vec<usize> = unit
                        .reads
                        .iter()
                        .filter_map(|node| produced_by.get(node.as_str()).copied())
                        .filter(|producer| *producer != index)
                        .collect();
                    waits.sort_unstable();
                    waits.dedup();
                    waits
                })
                .collect();
            scopes.insert(
                scope.clone(),
                Arc::new(ScopeUnits {
                    graph,
                    units,
                    waits_on,
                }),
            );
        }

        Ok(Self {
            scopes,
            map_steps,
            map_cap_urns,
            exports: exports
                .into_iter()
                .map(|(map, nodes)| (map, nodes.into_iter().collect()))
                .collect(),
        })
    }
}

// =============================================================================
// Progress: the sum of what each part has contributed
// =============================================================================

/// The progress of something made of parts with fixed shares: the sum of
/// what each part has contributed (15.2 §Progress subdivision). Parts move
/// independently — several at once, in any order — and the total never goes
/// down.
struct SharedProgress {
    shares: Vec<f32>,
    parts: Mutex<Vec<f32>>,
    /// The highest total reported, as `f32` bits (non-negative floats order
    /// like their bits).
    reported: AtomicU32,
    sink: CapProgressFn,
}

impl SharedProgress {
    /// `weights[i]` is how much of the whole part `i` is; they need not sum
    /// to anything in particular.
    fn new(weights: &[usize], sink: CapProgressFn) -> Arc<Self> {
        let total: usize = weights.iter().sum::<usize>().max(1);
        Arc::new(Self {
            shares: weights.iter().map(|w| *w as f32 / total as f32).collect(),
            parts: Mutex::new(vec![0.0; weights.len()]),
            reported: AtomicU32::new(0f32.to_bits()),
            sink,
        })
    }

    /// Part `index` is at `progress` of its own `0.0..=1.0`.
    fn set(&self, index: usize, progress: f32, cap_urn: &str, message: &str) {
        let total = {
            let mut parts = self.parts.lock().expect("progress parts");
            let progress = progress.clamp(0.0, 1.0);
            if progress > parts[index] {
                parts[index] = progress;
            }
            parts
                .iter()
                .zip(&self.shares)
                .map(|(part, share)| part * share)
                .sum::<f32>()
                .clamp(0.0, 1.0)
        };
        let before = self.reported.fetch_max(total.to_bits(), Ordering::Relaxed);
        (self.sink)(f32::from_bits(before.max(total.to_bits())), cap_urn, message);
    }

    /// The callback a part reports its own progress through.
    fn part(self: &Arc<Self>, index: usize) -> CapProgressFn {
        let this = Arc::clone(self);
        Arc::new(move |progress: f32, cap_urn: &str, message: &str| {
            this.set(index, progress, cap_urn, message);
        })
    }
}

// =============================================================================
// execute_plan
// =============================================================================

type Boxed<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// The run of a map's body a piece of work is part of.
#[derive(Clone)]
struct BodyCtx {
    coordinate: ForEachBodyCoordinate,
    /// The outermost map's stall tracker: any frame of any run touches it.
    stall_tracker: Arc<PipelineProgressTracker>,
}

/// Everything a run shares.
struct Run {
    plan: MachinePlan,
    shape: PlanShape,
    runtime: Arc<dyn EngineRuntime>,
    arguments: Arc<RunArgumentLedger>,
    callbacks: RunCallbacks,
    /// The nodes whose values are plan terminals.
    persist_sinks: HashSet<String>,
    map_failure_policy: MapFailurePolicy,
    reports: Mutex<Vec<StepReport>>,
    body_outcomes: Mutex<Vec<BodyOutcome>>,
    /// A step — or a run of a map — failed and was tolerated.
    tolerated: AtomicBool,
    /// Spool files the run owns: removed when it ends, however it ends.
    spools: Mutex<Vec<std::path::PathBuf>>,
}

impl Drop for Run {
    fn drop(&mut self) {
        for path in self.spools.lock().expect("run spools").drain(..) {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Execute a `MachinePlan`, returning what was produced at each plan `Output`
/// and how the run ended.
///
/// `initial_inputs` gives every `InputSlot` of the plan its value, with its
/// cardinality in `initial_is_sequence`; both are keyed by slot node id and
/// must name exactly the plan's slots. `arguments` holds the values of the
/// args the machine leaves unbound, and is where a host changes them while
/// the plan runs.
///
/// A failure that ends the run is returned as the error it is, attributed to
/// its step; every other live request was cancelled as its collateral first.
pub async fn execute_plan(
    plan: &MachinePlan,
    runtime: Arc<dyn EngineRuntime>,
    initial_inputs: HashMap<String, PlanInput>,
    initial_is_sequence: HashMap<String, bool>,
    arguments: Arc<RunArgumentLedger>,
    callbacks: RunCallbacks,
) -> Result<PipelineResult, ExecutionError> {
    plan.validate()
        .map_err(|e| ExecutionError::HostError(format!("plan '{}' is not valid: {e}", plan.name)))?;
    let registry = runtime.fabric_registry();
    let shape = PlanShape::of(plan, &registry)?;

    // The plan's inputs: every slot is given exactly one value.
    let mut slots: Vec<&String> = plan
        .nodes
        .iter()
        .filter(|(_, node)| matches!(node.node_type, ExecutionNodeType::InputSlot { .. }))
        .map(|(id, _)| id)
        .collect();
    slots.sort();
    let mut given: Values = HashMap::new();
    let mut initial_inputs = initial_inputs;
    for slot in &slots {
        let input = initial_inputs.remove(*slot).ok_or_else(|| {
            ExecutionError::HostError(format!("input '{slot}' of the plan was given no value"))
        })?;
        let is_sequence = *initial_is_sequence.get(*slot).ok_or_else(|| {
            ExecutionError::HostError(format!("initial input '{slot}' has no sequence flag"))
        })?;
        given.insert((*slot).clone(), held_from_input(slot, input, is_sequence)?);
    }
    if !initial_inputs.is_empty() || initial_is_sequence.len() != slots.len() {
        let mut stray: Vec<&String> = initial_inputs
            .keys()
            .chain(initial_is_sequence.keys().filter(|key| !slots.contains(key)))
            .collect();
        stray.sort();
        stray.dedup();
        return Err(ExecutionError::HostError(format!(
            "a value or a sequence flag was given for {stray:?}, which the plan has no input for"
        )));
    }

    // Terminals: (Output node id, the node it is of), deterministically ordered.
    let mut outputs: Vec<(String, String)> = plan
        .nodes
        .iter()
        .filter_map(|(id, n)| match &n.node_type {
            ExecutionNodeType::Output { source_node, .. } => {
                Some((id.clone(), source_node.clone()))
            }
            _ => None,
        })
        .collect();
    outputs.sort();

    let map_failure_policy = runtime.map_failure_policy().await?;
    let has_map = !shape.map_steps.is_empty();
    let run = Arc::new(Run {
        plan: plan.clone(),
        shape,
        runtime: runtime.clone(),
        arguments,
        persist_sinks: outputs.iter().map(|(_, source)| source.clone()).collect(),
        map_failure_policy,
        reports: Mutex::new(Vec::new()),
        body_outcomes: Mutex::new(Vec::new()),
        tolerated: AtomicBool::new(false),
        spools: Mutex::new(Vec::new()),
        callbacks,
    });

    let started = Instant::now();
    let env = Env {
        shared: Arc::new(HashMap::new()),
        given: Arc::new(given),
    };
    let values = run
        .clone()
        .run_scope(None, env.clone(), None, run.callbacks.progress.clone(), RunCancel::new())
        .await?;

    // ── Assemble terminals ──
    let mut terminals = Vec::with_capacity(outputs.len());
    let mut saved_paths: Vec<String> = Vec::new();
    let mut total_bytes = 0usize;
    for (out_id, source) in &outputs {
        let media_urn = derive_output_media_urn(plan, out_id).map_err(|e| {
            ExecutionError::HostError(format!("terminal '{out_id}': its media cannot be derived: {e}"))
        })?;
        let held = values.get(source).or_else(|| env.get(source)).ok_or_else(|| {
            ExecutionError::HostError(format!(
                "terminal '{out_id}' is of '{source}', which produced no value"
            ))
        })?;
        terminals.push(match held {
            Held::Absent => PlanTerminal::NotProduced {
                output_node_id: out_id.clone(),
                media_urn,
            },
            Held::Persisted {
                writers,
                is_sequence,
            } => {
                for writer in writers {
                    saved_paths.extend(writer.saved_paths.iter().cloned());
                    total_bytes += writer.total_bytes;
                }
                PlanTerminal::Produced(ProducedTerminal {
                    output_node_id: out_id.clone(),
                    items: Vec::new(),
                    is_sequence: *is_sequence,
                    media_urn,
                    writer_results: writers.clone(),
                })
            }
            Held::Memory { items, is_sequence } => PlanTerminal::Produced(ProducedTerminal {
                output_node_id: out_id.clone(),
                items: items
                    .iter()
                    .enumerate()
                    .map(|(index, data)| OutputItem {
                        data: data.clone(),
                        index,
                    })
                    .collect(),
                is_sequence: *is_sequence,
                media_urn,
                writer_results: Vec::new(),
            }),
            Held::Spooled { .. } | Held::Live { .. } => {
                return Err(ExecutionError::HostError(format!(
                    "terminal '{out_id}' is of '{source}', which holds an unbounded stream that \
                     was not persisted — an unbounded machine output needs a runtime that \
                     persists terminals (L16)"
                )));
            }
        });
    }

    // A plan with no map is one body: one outcome for the whole run. When
    // maps exist the outcomes are their runs only — the other steps surface
    // as their own run-graph nodes, so a whole-run outcome beside them would
    // render as a phantom extra item.
    let mut body_outcomes = std::mem::take(&mut *run.body_outcomes.lock().expect("body outcomes"));
    if !has_map {
        let mut cap_urns: Vec<(&String, String)> = plan
            .nodes
            .iter()
            .filter_map(|(id, node)| node.cap_urn().map(|urn| (id, urn.to_string())))
            .collect();
        cap_urns.sort();
        body_outcomes.push(BodyOutcome {
            foreach_token_id: None,
            body_index: 0,
            enclosing: Vec::new(),
            success: true,
            cap_urns: cap_urns.into_iter().map(|(_, urn)| urn).collect(),
            failed_token_id: None,
            error: None,
            failed_arg_urn: None,
            title: None,
            saved_paths,
            total_bytes,
            duration_ms: started.elapsed().as_millis() as u64,
            item_preview_text: None,
            item_byte_count: 0,
        });
    }

    if let Some(progress) = &run.callbacks.progress {
        progress(1.0, "", "Completed");
    }
    let steps = std::mem::take(&mut *run.reports.lock().expect("step reports"));
    let verdict = if run.tolerated.load(Ordering::Relaxed) {
        RunVerdict::CompletedWithToleratedFailures
    } else {
        RunVerdict::Completed
    };
    Ok(PipelineResult {
        terminals,
        body_outcomes,
        steps,
        verdict,
    })
}

impl Run {
    fn report(&self, report: StepReport) {
        if let Some(callback) = &self.callbacks.step_report {
            callback(&report);
        }
        self.reports.lock().expect("step reports").push(report);
    }

    fn log(&self, record: PipelineLogRecord) {
        if let Some(log) = &self.callbacks.log {
            log(record);
        }
    }

    /// Run one scope: start every unit as soon as what it reads is there,
    /// units with no path between them at the same time. Returns the values
    /// the scope's units produced.
    ///
    /// The first unit to fail for good ends the scope: the failure is raised
    /// on `cancel` — the scope's own signal — nothing more is started, and
    /// what is still running in it ends as its collateral before this returns
    /// the failure. Whether that ends the run is for whoever started the
    /// scope to say.
    fn run_scope(
        self: Arc<Self>,
        scope: ScopeKey,
        env: Env,
        body: Option<BodyCtx>,
        progress: Option<CapProgressFn>,
        cancel: RunCancel,
    ) -> Boxed<Result<Values, ExecutionError>> {
        Box::pin(async move {
            let units = self
                .shape
                .scopes
                .get(&scope)
                .cloned()
                .ok_or_else(|| ExecutionError::HostError(format!("no scope {scope:?} in the plan")))?;
            let weights: Vec<usize> = units.units.iter().map(|unit| unit.steps).collect();
            let shared = progress.map(|sink| SharedProgress::new(&weights, sink));

            let mut values: Values = HashMap::new();
            let mut done = vec![false; units.units.len()];
            let mut started = vec![false; units.units.len()];
            let mut running: tokio::task::JoinSet<(usize, Result<Values, ExecutionError>)> =
                tokio::task::JoinSet::new();
            let mut failure: Option<ExecutionError> = None;

            loop {
                if failure.is_none() && cancel.cause().is_none() {
                    for index in 0..units.units.len() {
                        if started[index] || !units.waits_on[index].iter().all(|dep| done[*dep]) {
                            continue;
                        }
                        let unit = &units.units[index];
                        // What the unit reads, as it stands now.
                        let mut inputs: Values = HashMap::with_capacity(unit.reads.len());
                        let mut missing: Option<&String> = None;
                        for node in &unit.reads {
                            match values.get(node).or_else(|| env.get(node)) {
                                Some(held) => {
                                    inputs.insert(node.clone(), held.clone());
                                }
                                None => {
                                    missing = Some(node);
                                    break;
                                }
                            }
                        }
                        if let Some(node) = missing {
                            let error = ExecutionError::HostError(format!(
                                "node '{node}' is read and nothing produced a value for it"
                            ));
                            cancel.raise(CollateralCause::of(&error));
                            failure = Some(error);
                            break;
                        }
                        started[index] = true;
                        let part = shared.as_ref().map(|shared| shared.part(index));
                        let run = self.clone();
                        let units = units.clone();
                        let body = body.clone();
                        let cancel = cancel.clone();
                        running.spawn(async move {
                            let result = match &units.units[index].kind {
                                UnitKind::Chain(chain) => {
                                    run.run_chain(units.clone(), chain.clone(), inputs, body, part, cancel)
                                        .await
                                }
                                UnitKind::Map(map) => {
                                    run.run_map(map.clone(), inputs, body, part, cancel).await
                                }
                            };
                            (index, result)
                        });
                    }
                }

                let Some(joined) = running.join_next().await else {
                    break;
                };
                let (index, result) = match joined {
                    Ok(finished) => finished,
                    Err(join_error) => {
                        let error = ExecutionError::HostError(format!(
                            "a unit of the run panicked: {join_error}"
                        ));
                        cancel.raise(CollateralCause::of(&error));
                        if failure.as_ref().map_or(true, ExecutionError::is_collateral) {
                            failure = Some(error);
                        }
                        continue;
                    }
                };
                match result {
                    Ok(produced) => {
                        values.extend(produced);
                        done[index] = true;
                        if let Some(shared) = &shared {
                            shared.set(index, 1.0, "", "");
                        }
                    }
                    Err(error) => {
                        // The first failure of its own is the cause; what the
                        // signal stopped is a consequence of it.
                        if !error.is_collateral() {
                            cancel.raise(CollateralCause::of(&error));
                        }
                        if failure.as_ref().map_or(true, ExecutionError::is_collateral)
                            && (failure.is_none() || !error.is_collateral())
                        {
                            failure = Some(error);
                        }
                    }
                }
            }

            if let Some(error) = failure {
                return Err(error);
            }
            if done.iter().any(|done| !done) {
                // Nothing here failed: something around it did, before every
                // unit of this scope could start.
                let cause = cancel.cause().ok_or_else(|| {
                    ExecutionError::HostError(format!(
                        "scope {scope:?} ended with units that never became ready"
                    ))
                })?;
                return Err(ExecutionError::Collateral {
                    class: cause.class,
                    detail: cause.detail,
                });
            }
            Ok(values)
        })
    }

    /// Whether `step` can be given what it is wired to, as things stand in
    /// `seen` — and, when it can, what it is sent. An input wired to an
    /// absent value is not sent when its arg is not required, and stops the
    /// step when it is; a gather keeps the members that are present.
    fn settle_inputs(step: &mut ResolvedStep, seen: &Values) -> Result<Vec<String>, ExecutionError> {
        let unseen = |node: &str| {
            ExecutionError::HostError(format!(
                "step '{}' reads '{node}', which has no value",
                step.node
            ))
        };
        let mut absent_required: Vec<String> = Vec::new();
        let mut kept = Vec::with_capacity(step.inputs.len());
        for mut input in std::mem::take(&mut step.inputs) {
            match &mut input.source {
                StepSource::Gather(nodes) => {
                    let mut present = Vec::with_capacity(nodes.len());
                    for node in nodes.drain(..) {
                        match seen.get(&node) {
                            None => return Err(unseen(&node)),
                            Some(Held::Absent) => {}
                            Some(_) => present.push(node),
                        }
                    }
                    *nodes = present;
                    kept.push(input);
                }
                StepSource::Node(node) => match seen.get(node.as_str()) {
                    None => return Err(unseen(node)),
                    Some(Held::Absent) => {
                        let stream = crate::MediaUrn::from_string(&input.stream_urn).map_err(|e| {
                            ExecutionError::HostError(format!(
                                "step '{}': input stream URN '{}' is not a media URN: {e}",
                                step.node, input.stream_urn
                            ))
                        })?;
                        let arg = step
                            .cap
                            .args
                            .iter()
                            .find(|arg| {
                                crate::MediaUrn::from_string(arg.stream_urn())
                                    .map(|urn| urn.is_equivalent(&stream).unwrap_or(false))
                                    .unwrap_or(false)
                            })
                            .ok_or_else(|| {
                                ExecutionError::HostError(format!(
                                    "step '{}' is wired on stream '{}', which no arg of cap '{}' \
                                     is delivered under",
                                    step.node, input.stream_urn, step.cap_urn
                                ))
                            })?;
                        if arg.required {
                            absent_required.push(node.clone());
                        }
                        // Not required: not sent — the arg takes its default,
                        // or is omitted when it has none.
                    }
                    Some(_) => kept.push(input),
                },
            }
        }
        step.inputs = kept;
        Ok(absent_required)
    }

    /// Run one pipelined chain, given the values its head reads.
    ///
    /// Returns the value of every node its last step produces. A step that
    /// cannot be given a required value is skipped, and the chain goes on
    /// from the next; a step with a policy (always a chain of its own) is
    /// attempted again and tolerated as that policy says.
    async fn run_chain(
        self: Arc<Self>,
        units: Arc<ScopeUnits>,
        chain: Vec<usize>,
        inputs: Values,
        body: Option<BodyCtx>,
        progress: Option<CapProgressFn>,
        cancel: RunCancel,
    ) -> Result<Values, ExecutionError> {
        let mut steps: Vec<ResolvedStep> =
            chain.iter().map(|&index| units.graph.steps[index].clone()).collect();
        let total = steps.len();
        let coordinate = body.as_ref().map(|body| body.coordinate.clone());
        let mut seen = inputs;

        // Skipped steps: each leaves its outputs absent, which is what the
        // next step of the chain then sees.
        let mut first = 0;
        loop {
            let absent = Self::settle_inputs(&mut steps[first], &seen)?;
            if absent.is_empty() {
                break;
            }
            let step = &steps[first];
            self.report(StepReport {
                step_token_id: step.token_id.clone(),
                cap_urn: step.cap_urn.clone(),
                body: coordinate.clone(),
                outcome: StepOutcome::Skipped { absent },
            });
            for node in step.produced_nodes() {
                seen.insert(node.clone(), Held::Absent);
            }
            // Outside a map it will never be dispatched: an argument update to
            // it from here on is honest about that.
            if body.is_none() {
                self.arguments.exhaust_steps(&[step.node.as_str()]);
            }
            first += 1;
            if first == total {
                return Ok(steps[total - 1]
                    .produced_nodes()
                    .map(|node| (node.clone(), Held::Absent))
                    .collect());
            }
        }
        let steps = steps.split_off(first);
        let head = &steps[0];
        let last = steps.last().expect("a chain has a step");

        // What the head is sent.
        let mut initial_inputs: HashMap<String, PlanInput> = HashMap::new();
        let mut initial_is_sequence: HashMap<String, bool> = HashMap::new();
        for node in head.source_nodes() {
            if initial_inputs.contains_key(node) {
                continue;
            }
            let held = seen.get(node).ok_or_else(|| {
                ExecutionError::HostError(format!(
                    "step '{}' reads '{node}', which has no value",
                    head.node
                ))
            })?;
            let (input, is_sequence) = held.as_plan_input(node)?;
            initial_inputs.insert(node.clone(), input);
            initial_is_sequence.insert(node.clone(), is_sequence);
        }
        let graph = ResolvedGraph {
            nodes: units.graph.nodes.clone(),
            steps: steps.clone(),
            graph_name: units.graph.graph_name.clone(),
        };
        let persist: HashSet<String> = last
            .produced_nodes()
            .filter(|node| self.persist_sinks.contains(*node))
            .cloned()
            .collect();

        // The chain's steps dispatch NOW: journal them and read their values
        // in one atomic ledger step, so a mid-run argument update lands
        // entirely before or entirely after — never between its steps. Every
        // attempt of a step reads what its first one read.
        let step_ids: Vec<&str> = steps.iter().map(|step| step.node.as_str()).collect();
        let cap_arguments = if body.is_some() {
            self.arguments.dispatch_steps_in_map(&step_ids)
        } else {
            self.arguments.dispatch_steps(&step_ids)
        };

        // The steps this chain did not skip report over the rest of its share.
        let skipped_share = first as f32 / total as f32;
        let run_share = 1.0 - skipped_share;
        let segment_progress: Option<CapProgressFn> = progress.map(|part| {
            Arc::new(move |p: f32, cap_urn: &str, message: &str| {
                part(skipped_share + run_share * p.clamp(0.0, 1.0), cap_urn, message);
            }) as CapProgressFn
        });
        // Inside a map a step's own progress is the map's to aggregate.
        let step_progress = if body.is_some() {
            None
        } else {
            self.callbacks.step_progress.as_ref()
        };

        let policy = head.policy;
        let backoff = crate::net_retry::RetryPolicy::default();
        let mut failed_attempts: Vec<StepFailure> = Vec::new();
        let segment = loop {
            let attempt = self
                .runtime
                .run_segment(
                    &graph,
                    initial_inputs.clone(),
                    initial_is_sequence.clone(),
                    &cap_arguments,
                    segment_progress.as_ref(),
                    step_progress,
                    self.callbacks.log.as_ref(),
                    coordinate.clone(),
                    body.as_ref().map(|body| body.stall_tracker.clone()),
                    &persist,
                    &cancel,
                )
                .await;
            let error = match attempt {
                Ok(segment) => break segment,
                Err(error) => error,
            };
            let class = error.attribution_class();
            // Stopped because something else failed, or cancelled: neither is
            // this step's failure to attempt again or to tolerate.
            if error.is_collateral() || class == AttributionClass::User {
                return Err(error);
            }
            let retryable = matches!(
                class,
                AttributionClass::Resource | AttributionClass::Environment | AttributionClass::Internal
            );
            if retryable && (failed_attempts.len() as u32) < policy.retry {
                let wait = backoff.delay_before_retry(failed_attempts.len() as u32);
                self.log(PipelineLogRecord::attributed(
                    &head.token_id,
                    head.cap_urn.clone(),
                    "warn",
                    class,
                    format!(
                        "attempt {} of {} failed: {} — trying again in {}ms",
                        failed_attempts.len() + 1,
                        policy.retry + 1,
                        error.failure_reason(),
                        wait.as_millis()
                    ),
                ));
                failed_attempts.push(StepFailure::of(&error));
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {}
                    cause = cancel.raised() => {
                        return Err(ExecutionError::Collateral {
                            class: cause.class,
                            detail: cause.detail,
                        });
                    }
                }
                continue;
            }
            if policy.on_error == OnError::Absent {
                self.tolerated.store(true, Ordering::Relaxed);
                self.log(PipelineLogRecord::attributed(
                    &head.token_id,
                    head.cap_urn.clone(),
                    "warn",
                    class,
                    format!(
                        "failed and tolerated (on_error=absent): {} — its outputs are absent",
                        error.failure_reason()
                    ),
                ));
                self.report(StepReport {
                    step_token_id: head.token_id.clone(),
                    cap_urn: head.cap_urn.clone(),
                    body: coordinate.clone(),
                    outcome: StepOutcome::FailedTolerated {
                        failure: StepFailure::of(&error),
                        failed_attempts,
                    },
                });
                return Ok(head
                    .produced_nodes()
                    .map(|node| (node.clone(), Held::Absent))
                    .collect());
            }
            return Err(error);
        };
        if !failed_attempts.is_empty() {
            self.report(StepReport {
                step_token_id: head.token_id.clone(),
                cap_urn: head.cap_urn.clone(),
                body: coordinate.clone(),
                outcome: StepOutcome::Completed { failed_attempts },
            });
        }

        // What the last step produced, node by node.
        let SegmentOutput {
            mut node_data,
            node_is_sequence,
            mut writer_results,
            mut node_spool,
            absent,
            ..
        } = segment;
        // A spooled value is the run's to remove — unless the runtime keeps
        // transient artifacts, which then owns it.
        if self.runtime.transient_artifact_root().is_none() {
            self.spools
                .lock()
                .expect("run spools")
                .extend(node_spool.values().cloned());
        }
        let mut produced: Values = HashMap::new();
        for node in last.produced_nodes() {
            // One node, one truth: a value is in memory, or on disk, or
            // absent. A segment that says two of these has broken its
            // contract, and reading either would be a guess — the shape that
            // once ran a whole capture as zero runs of the map over it.
            let in_memory = node_data.get(node).is_some_and(|items| !items.is_empty());
            let on_disk = node_spool.contains_key(node);
            if on_disk && (node_data.contains_key(node) || writer_results.contains_key(node)) {
                return Err(ExecutionError::HostError(format!(
                    "node '{node}' is recorded both on disk and in memory by the segment that \
                     produced it — the executor must record exactly one"
                ))
                .at_step(&last.token_id));
            }
            if absent.contains(node) && (in_memory || on_disk || writer_results.contains_key(node)) {
                return Err(ExecutionError::HostError(format!(
                    "node '{node}' is recorded both absent and produced by the segment that \
                     produced it"
                ))
                .at_step(&last.token_id));
            }
            if absent.contains(node) {
                produced.insert(node.clone(), Held::Absent);
                continue;
            }
            let is_sequence = node_is_sequence.get(node).copied();
            let held = if let Some(writers) = writer_results.remove(node) {
                Held::Persisted {
                    writers,
                    is_sequence: is_sequence.unwrap_or(false),
                }
            } else if let Some(path) = node_spool.remove(node) {
                Held::Spooled {
                    path,
                    is_sequence: is_sequence.unwrap_or(false),
                }
            } else if let Some(items) = node_data.remove(node) {
                let is_sequence = is_sequence.ok_or_else(|| {
                    ExecutionError::HostError(format!(
                        "node '{node}' completed without recording its cardinality"
                    ))
                })?;
                Held::memory(items, is_sequence)
            } else {
                // A step that declares no main output produces nothing there,
                // and nothing reads it.
                continue;
            };
            produced.insert(node.clone(), held);
        }
        Ok(produced)
    }

    /// Run one map: its body once per item of the sequences it zips, as many
    /// at once as admission lets through, and collect what the body produces
    /// for the scope around it — each exported node a sequence, in item
    /// order, flat.
    async fn run_map(
        self: Arc<Self>,
        map: String,
        inputs: Values,
        outer: Option<BodyCtx>,
        progress: Option<CapProgressFn>,
        cancel: RunCancel,
    ) -> Result<Values, ExecutionError> {
        let ExecutionNodeType::ForEach {
            token_id, sources, ..
        } = &self.plan.nodes[&map].node_type
        else {
            return Err(ExecutionError::HostError(format!("'{map}' is not a map of the plan")));
        };
        let token_id = token_id.clone();
        let sources = sources.clone();
        let exports: Vec<String> = self.shape.exports.get(&map).cloned().unwrap_or_default();
        let steps_inside: Vec<String> = self.shape.map_steps.get(&map).cloned().unwrap_or_default();
        let cap_urns: Vec<String> = self.shape.map_cap_urns.get(&map).cloned().unwrap_or_default();
        let enclosing: Vec<MapRun> = outer
            .as_ref()
            .map(|outer| outer.coordinate.path())
            .unwrap_or_default();
        // A failure of the map itself — its sources, its bookkeeping — is
        // attributed to the map's own step.
        let at_step = |error: ExecutionError| error.at_step(&token_id);

        // The sequences it zips. A source that is absent leaves the map with
        // nothing to run: every step inside it is skipped, and everything it
        // would have produced is absent.
        let mut item_sources: Vec<RegionItemSource> = Vec::with_capacity(sources.len());
        for source in &sources {
            let held = inputs.get(&source.node).ok_or_else(|| {
                at_step(ExecutionError::HostError(format!(
                    "map '{map}' iterates '{}', which has no value",
                    source.node
                )))
            })?;
            let not_a_sequence = || {
                at_step(ExecutionError::HostError(format!(
                    "map '{map}' iterates '{}', which holds a single value in this run — the \
                     plan was built for a sequence there",
                    source.node
                )))
            };
            item_sources.push(match held {
                Held::Absent => {
                    let absent = vec![source.node.clone()];
                    for step in &steps_inside {
                        if let Some(ExecutionNodeType::Cap {
                            token_id: step_token,
                            cap_urn,
                            ..
                        }) = self.plan.nodes.get(step).map(|n| &n.node_type)
                        {
                            self.report(StepReport {
                                step_token_id: step_token.clone(),
                                cap_urn: cap_urn.clone(),
                                body: outer.as_ref().map(|outer| outer.coordinate.clone()),
                                outcome: StepOutcome::Skipped {
                                    absent: absent.clone(),
                                },
                            });
                        }
                    }
                    return Ok(exports
                        .into_iter()
                        .map(|node| (node, Held::Absent))
                        .collect());
                }
                Held::Memory { is_sequence: false, .. } | Held::Spooled { is_sequence: false, .. } => {
                    return Err(not_a_sequence());
                }
                Held::Memory { items, .. } => region_source_from_memory(items.as_ref().clone()),
                Held::Spooled { path, .. } => {
                    let index = scan_spooled_sequence(path).await.map_err(&at_step)?;
                    region_source_from_spool(path.clone(), index)
                }
                // A LIVE reference feeding a map directly is resolved by the
                // HOST (13.2 §Reference Media, host resolution): this is the
                // runtime that iterates the items, so it opens the device
                // itself and dispatches one body per delivered item WHILE the
                // capture runs.
                Held::Live {
                    reference_urn,
                    selector,
                    ..
                } => {
                    let selector = crate::bifaci::live_feed::LiveFeedSelector::parse(selector)
                        .map_err(|e| {
                            at_step(ExecutionError::HostError(format!(
                                "live source '{reference_urn}' feeding map '{map}': {e}"
                            )))
                        })?;
                    let opened = crate::capture::open(
                        reference_urn,
                        selector,
                        Arc::new(std::sync::atomic::AtomicU64::new(0)),
                    )
                    .map_err(|e| {
                        at_step(ExecutionError::HostError(format!(
                            "host capture for map '{map}': {e}"
                        )))
                    })?;
                    // Register the tap so a run stop closes it (drain
                    // semantics, 15.2 §Runs Stop).
                    self.runtime.on_host_feed_open(&opened.handle);
                    region_source_from_live(opened)
                }
                Held::Persisted { .. } => {
                    return Err(at_step(ExecutionError::HostError(format!(
                        "map '{map}' iterates '{}', a plan terminal the runtime persisted",
                        source.node
                    ))));
                }
            });
        }
        let source_nodes: Vec<String> = sources.iter().map(|source| source.node.clone()).collect();
        let mut zip = ZippedSources::new(token_id.clone(), source_nodes, item_sources)
            .map_err(&at_step)?;
        let known_total = zip.known_total();

        // Per-run progress slots, grown as items are dispatched (a live
        // feed's total is unknown until it ends). The map's own progress is
        // the slot sum over the known total when there is one, else over the
        // dispatched count, through a monotone clamp so it never runs
        // backwards while the denominator grows.
        let run_slots: Arc<Mutex<Vec<f32>>> = Arc::new(Mutex::new(Vec::new()));
        let dispatched_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let reported_bits = Arc::new(AtomicU32::new(0f32.to_bits()));
        let aggregate = {
            let slots = run_slots.clone();
            let dispatched = dispatched_counter.clone();
            let reported = reported_bits.clone();
            Arc::new(move || -> f32 {
                let sum: f32 = slots.lock().expect("progress slots").iter().sum();
                let denominator = known_total
                    .unwrap_or_else(|| dispatched.load(Ordering::Relaxed))
                    .max(1) as f32;
                let raw = (sum / denominator).clamp(0.0, 1.0);
                let before = reported.fetch_max(raw.to_bits(), Ordering::Relaxed);
                f32::from_bits(before.max(raw.to_bits()))
            })
        };
        // A map of the machine itself reports its aggregate under its own
        // token; one inside another's body is that body's to account for.
        let step_sink = if outer.is_none() {
            self.callbacks.step_progress.clone()
        } else {
            None
        };
        let stall_tracker = outer
            .as_ref()
            .map(|outer| outer.stall_tracker.clone())
            .unwrap_or_else(|| Arc::new(PipelineProgressTracker::new()));
        let mut stall_warning_logged = false;

        // What every run of the body reads from outside the map: the same
        // values each time.
        let shared: Arc<Values> = Arc::new(inputs);
        let permits = Arc::new(tokio::sync::Semaphore::new(REGION_BODY_MAX_IN_FLIGHT));
        let mut bodies: tokio::task::JoinSet<(usize, Result<Values, ExecutionError>, u64)> =
            tokio::task::JoinSet::new();

        let mut source_done = false;
        let mut dispatched = 0usize;
        let mut completed = 0usize;
        let mut succeeded = 0usize;
        let mut failed = 0usize;
        // Per run, in item order: what it exported, when it completed.
        let mut run_values: Vec<Option<Values>> = Vec::new();
        let mut item_facts: Vec<(Option<String>, u64)> = Vec::new();
        let mut failure: Option<ExecutionError> = None;

        while !(source_done && completed >= dispatched) {
            let stopping = failure.is_some() || cancel.cause().is_some();
            if stopping && completed >= dispatched {
                break;
            }
            tokio::select! {
                biased;
                joined = bodies.join_next(), if completed < dispatched => {
                    let Some(joined) = joined else { break };
                    completed += 1;
                    stall_tracker.touch();
                    stall_warning_logged = false;
                    let (index, result, duration_ms) = match joined {
                        Ok(finished) => finished,
                        Err(join_error) => {
                            let error = at_step(ExecutionError::HostError(format!(
                                "a run of map '{map}' panicked: {join_error}"
                            )));
                            cancel.raise(CollateralCause::of(&error));
                            if failure.as_ref().map_or(true, ExecutionError::is_collateral) {
                                failure = Some(error);
                            }
                            continue;
                        }
                    };
                    let outcome = |success: bool, error: Option<&ExecutionError>, values: Option<&Values>| {
                        let mut saved_paths: Vec<String> = Vec::new();
                        let mut total_bytes = 0usize;
                        for held in values.into_iter().flat_map(|values| values.values()) {
                            match held {
                                Held::Persisted { writers, .. } => {
                                    for writer in writers {
                                        saved_paths.extend(writer.saved_paths.iter().cloned());
                                        total_bytes += writer.total_bytes;
                                    }
                                }
                                Held::Memory { items, .. } => {
                                    total_bytes += items.iter().map(Vec::len).sum::<usize>();
                                }
                                _ => {}
                            }
                        }
                        BodyOutcome {
                            foreach_token_id: Some(token_id.clone()),
                            body_index: index,
                            enclosing: enclosing.clone(),
                            success,
                            cap_urns: cap_urns.clone(),
                            failed_token_id: error.and_then(|e| e.step_token_id().cloned()),
                            error: error.map(|e| e.to_string()),
                            failed_arg_urn: error.and_then(|e| e.failure_arg_urn().map(str::to_string)),
                            title: None,
                            saved_paths,
                            total_bytes,
                            duration_ms,
                            item_preview_text: item_facts.get(index).and_then(|f| f.0.clone()),
                            item_byte_count: item_facts.get(index).map(|f| f.1).unwrap_or(0),
                        }
                    };
                    let record = |outcome: BodyOutcome| -> Result<(), ExecutionError> {
                        let mut outcomes = self.body_outcomes.lock().expect("body outcomes");
                        outcomes.push(outcome);
                        match &self.callbacks.body_outcome {
                            Some(callback) => callback(&outcomes),
                            None => Ok(()),
                        }
                    };
                    match result {
                        Ok(values) => {
                            // Only what the map exports is kept of a run.
                            let mut kept: Values = HashMap::with_capacity(exports.len());
                            for node in &exports {
                                match values.get(node) {
                                    Some(held) => {
                                        kept.insert(node.clone(), held.clone());
                                    }
                                    None => {
                                        let error = at_step(ExecutionError::HostError(format!(
                                            "run {index} of map '{map}' produced no value for \
                                             '{node}', which is read outside the map"
                                        )));
                                        cancel.raise(CollateralCause::of(&error));
                                        failure.get_or_insert(error);
                                    }
                                }
                            }
                            if let Err(error) = record(outcome(true, None, Some(&kept))) {
                                cancel.raise(CollateralCause::of(&error));
                                failure.get_or_insert(error);
                            }
                            run_values[index] = Some(kept);
                            succeeded += 1;
                        }
                        Err(error) => {
                            let class = error.attribution_class();
                            let ends_the_run = error.is_collateral()
                                || class == AttributionClass::User
                                || self.map_failure_policy == MapFailurePolicy::Fail;
                            if !error.is_collateral() {
                                if let Err(error) = record(outcome(false, Some(&error), None)) {
                                    cancel.raise(CollateralCause::of(&error));
                                    failure.get_or_insert(error);
                                }
                                tracing::error!("[execute_plan] map '{map}' run {index} failed: {error}");
                            }
                            if ends_the_run {
                                if !error.is_collateral() {
                                    cancel.raise(CollateralCause::of(&error));
                                }
                                if failure.as_ref().map_or(true, ExecutionError::is_collateral)
                                    && (failure.is_none() || !error.is_collateral())
                                {
                                    failure = Some(error);
                                }
                            } else {
                                // The host drops the item: the failure is on
                                // its body outcome, and the run goes on.
                                self.tolerated.store(true, Ordering::Relaxed);
                                failed += 1;
                            }
                        }
                    }
                    if let Some(part) = &progress {
                        let total = known_total.unwrap_or(dispatched);
                        part(aggregate(), "", &format!("Completed {completed}/{total}"));
                    }
                }
                next = async {
                    let permit = permits
                        .clone()
                        .acquire_owned()
                        .await
                        .expect("map body semaphore is never closed");
                    (permit, zip.next().await)
                }, if !source_done && !stopping => {
                    let (permit, next) = next;
                    let items = match next {
                        // The SOURCE failed (device died, spool corrupt), or
                        // the zipped sequences are of different lengths: the
                        // map's input is wrong — not a run's failure, and no
                        // item policy applies.
                        Err(error) => {
                            drop(permit);
                            let error = at_step(error);
                            cancel.raise(CollateralCause::of(&error));
                            failure.get_or_insert(error);
                            source_done = true;
                            continue;
                        }
                        Ok(None) => {
                            drop(permit);
                            source_done = true;
                            // Overruns at a host-held capture edge are counted
                            // loss (12.5 §Overrun) — surface them, never silently.
                            let overruns = zip.overruns();
                            if overruns > 0 {
                                self.log(PipelineLogRecord {
                                    step_token_id: Some(token_id.clone()),
                                    cap_urn: None,
                                    level: "warn".to_string(),
                                    attribution_class: AttributionClass::Resource,
                                    message: format!(
                                        "live feed dropped {overruns} item(s) at the capture \
                                         edge (drop-oldest overrun policy) — bodies ran on \
                                         the delivered items only"
                                    ),
                                    meta: None,
                                    body_index: None,
                                    arg_urn: None,
                                });
                            }
                            continue;
                        }
                        Ok(Some(items)) => items,
                    };
                    let index = dispatched;
                    item_facts.push((items[0].preview.clone(), items[0].byte_count));
                    run_values.push(None);
                    run_slots.lock().expect("progress slots").push(0.0);
                    dispatched_counter.store(index + 1, Ordering::Relaxed);

                    // The item's snapshot is published BEFORE its run spawns —
                    // append-only deltas, one per item (the total is unknowable
                    // for a live feed until it ends).
                    if let Some(callback) = &self.callbacks.foreach_items {
                        let published = callback(&token_id, &[ForEachItemSnapshot {
                            foreach_token_id: token_id.clone(),
                            body_index: index,
                            enclosing: enclosing.clone(),
                            item_preview_text: items[0].preview.clone(),
                            item_byte_count: items[0].byte_count,
                        }]);
                        if let Err(error) = published {
                            // The run it would have had is not started.
                            drop(permit);
                            run_values.pop();
                            item_facts.pop();
                            run_slots.lock().expect("progress slots").pop();
                            dispatched_counter.store(index, Ordering::Relaxed);
                            cancel.raise(CollateralCause::of(&error));
                            failure.get_or_insert(error);
                            source_done = true;
                            continue;
                        }
                    }

                    // What this run is given: one item of each source.
                    let given: Values = sources
                        .iter()
                        .zip(items)
                        .map(|(source, item)| (source.item.clone(), Held::memory(vec![item.bytes], false)))
                        .collect();
                    let env = Env {
                        shared: shared.clone(),
                        given: Arc::new(given),
                    };
                    let body = BodyCtx {
                        coordinate: ForEachBodyCoordinate {
                            foreach_token_id: token_id.clone(),
                            body_index: index,
                            enclosing: enclosing.clone(),
                        },
                        stall_tracker: stall_tracker.clone(),
                    };
                    // The run's own progress fills its slot; the map reports
                    // the aggregate — to the run's overall progress, and
                    // under its own token to whoever follows single steps.
                    let run_progress: Option<CapProgressFn> = (progress.is_some()
                        || step_sink.is_some())
                    .then(|| {
                        let part = progress.clone();
                        let slots = run_slots.clone();
                        let tracker = stall_tracker.clone();
                        let aggregate = aggregate.clone();
                        let step_sink = step_sink.clone();
                        let token_id = token_id.clone();
                        Arc::new(move |p: f32, cap_urn: &str, message: &str| {
                            if let Some(slot) = slots.lock().expect("progress slots").get_mut(index) {
                                *slot = p.clamp(0.0, 1.0);
                            }
                            tracker.touch();
                            let step = aggregate();
                            if let Some(sink) = &step_sink {
                                sink(step, cap_urn, &token_id);
                            }
                            if let Some(part) = &part {
                                part(step, cap_urn, message);
                            }
                        }) as CapProgressFn
                    });
                    let run = self.clone();
                    let body_scope = Some(map.clone());
                    // The run's own signal: a failure in it ends what it is
                    // running, and no more — whether it ends the map is
                    // decided when the run's failure is received, above.
                    let run_cancel = cancel.child();
                    bodies.spawn(async move {
                        let _permit = permit;
                        let started = Instant::now();
                        let result = run
                            .run_scope(body_scope, env, Some(body), run_progress, run_cancel)
                            .await;
                        (index, result, started.elapsed().as_millis() as u64)
                    });
                    dispatched += 1;
                }
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                    if stall_tracker.is_stalled() && !stall_warning_logged {
                        self.log(PipelineLogRecord {
                            step_token_id: Some(token_id.clone()),
                            cap_urn: None,
                            level: "warn".to_string(),
                            attribution_class: AttributionClass::Internal,
                            message: format!(
                                "This ForEach step has had no progress for \
                                 {PIPELINE_STALL_TIMEOUT_SECS}s; continuing to wait. \
                                 Use Cancel to abort."
                            ),
                            meta: None,
                            body_index: None,
                            arg_urn: None,
                        });
                        stall_warning_logged = true;
                    }
                }
            }
        }

        // No further run of anything inside the map will be dispatched once
        // the outermost map ends: from here a mid-run argument update to its
        // steps is honestly "already dispatched".
        if outer.is_none() {
            let ids: Vec<&str> = steps_inside.iter().map(String::as_str).collect();
            self.arguments.exhaust_steps(&ids);
        }

        if let Some(error) = failure {
            return Err(error);
        }
        if let Some(cause) = cancel.cause() {
            return Err(ExecutionError::Collateral {
                class: cause.class,
                detail: cause.detail,
            });
        }
        // Every run failed and the host dropped each: nothing was mapped.
        if failed > 0 && succeeded == 0 {
            return Err(at_step(ExecutionError::HostError(format!(
                "every run of the map failed ({failed} of {dispatched})"
            ))));
        }

        // Collect: each exported node the sequence of it over the runs, in
        // item order — flat, when a run's value is itself a sequence. A run
        // that failed and was dropped, and a run in which the node is absent,
        // contribute nothing.
        let mut collected: Values = HashMap::with_capacity(exports.len());
        for node in &exports {
            let mut items: Vec<Vec<u8>> = Vec::new();
            let mut writers: Vec<WriterResult> = Vec::new();
            for values in run_values.iter().flatten() {
                match values.get(node) {
                    Some(Held::Memory { items: run_items, .. }) => {
                        items.extend(run_items.iter().cloned());
                    }
                    Some(Held::Persisted { writers: run_writers, .. }) => {
                        writers.extend(run_writers.iter().cloned());
                    }
                    Some(Held::Absent) | None => {}
                    Some(Held::Spooled { .. }) | Some(Held::Live { .. }) => {
                        return Err(at_step(ExecutionError::HostError(format!(
                            "'{node}' is an unbounded stream inside map '{map}' and is read \
                             outside it — an unbounded value cannot be collected over a map's \
                             runs; consume it inside the map"
                        ))));
                    }
                }
            }
            collected.insert(
                node.clone(),
                if writers.is_empty() {
                    Held::memory(items, true)
                } else {
                    Held::Persisted {
                        writers,
                        is_sequence: true,
                    }
                },
            );
        }
        Ok(collected)
    }
}

// =============================================================================
// A map's items
// =============================================================================

/// The sequences a map zips, read together: run `i` is given item `i` of
/// each. Reading is cancel-safe — an item taken from one source while another
/// is awaited is held, not lost.
struct ZippedSources {
    foreach_token_id: StepToken,
    nodes: Vec<String>,
    sources: Vec<RegionItemSource>,
    /// The next item of each source, once it has arrived.
    pending: Vec<Option<RegionItemDelivery>>,
    /// Whether each source has ended.
    ended: Vec<bool>,
    /// How many items each source has given.
    given: Vec<usize>,
}

impl ZippedSources {
    /// Fails at once when the sources' lengths are all known and differ:
    /// nothing is run on sequences that cannot be zipped.
    fn new(
        foreach_token_id: StepToken,
        nodes: Vec<String>,
        sources: Vec<RegionItemSource>,
    ) -> Result<Self, ExecutionError> {
        let known: Vec<Option<usize>> = sources.iter().map(|source| source.known_total).collect();
        if let Some(lengths) = known.iter().copied().collect::<Option<Vec<usize>>>() {
            if lengths.windows(2).any(|pair| pair[0] != pair[1]) {
                return Err(ExecutionError::ZipLengthMismatch {
                    foreach_token_id,
                    sources: nodes.into_iter().zip(lengths).collect(),
                });
            }
        }
        let count = sources.len();
        Ok(Self {
            foreach_token_id,
            nodes,
            sources,
            pending: (0..count).map(|_| None).collect(),
            ended: vec![false; count],
            given: vec![0; count],
        })
    }

    /// How many runs there will be, when every source knows its length.
    fn known_total(&self) -> Option<usize> {
        self.sources
            .iter()
            .map(|source| source.known_total)
            .collect::<Option<Vec<usize>>>()
            .and_then(|lengths| lengths.first().copied())
    }

    /// Items a host-held capture dropped at its edge, over every source.
    fn overruns(&self) -> u64 {
        self.sources
            .iter()
            .filter_map(|source| source.feed_handle.as_ref())
            .map(|handle| handle.overruns())
            .sum()
    }

    /// The next item of every source, or `None` when all have ended together.
    /// One ending while another still has an item is a length mismatch.
    async fn next(&mut self) -> Result<Option<Vec<RegionItemDelivery>>, ExecutionError> {
        for index in 0..self.sources.len() {
            if self.pending[index].is_some() || self.ended[index] {
                continue;
            }
            match self.sources[index].rx.recv().await {
                None => self.ended[index] = true,
                Some(Err(error)) => return Err(error),
                Some(Ok(delivery)) => {
                    self.given[index] += 1;
                    self.pending[index] = Some(delivery);
                }
            }
        }
        if self.ended.iter().all(|ended| *ended) {
            return Ok(None);
        }
        if self.ended.iter().any(|ended| *ended) {
            return Err(ExecutionError::ZipLengthMismatch {
                foreach_token_id: self.foreach_token_id.clone(),
                sources: self
                    .nodes
                    .iter()
                    .cloned()
                    .zip(self.sources.iter().zip(&self.given).map(|(source, given)| {
                        source.known_total.unwrap_or(*given)
                    }))
                    .collect(),
            });
        }
        Ok(Some(
            self.pending
                .iter_mut()
                .map(|slot| slot.take().expect("every source has an item pending"))
                .collect(),
        ))
    }
}

/// One indexed item of a spooled region input: where its self-delimiting
/// CBOR value sits in the file, plus the UI snapshot facts gathered during
/// the single indexing pass (so snapshots never require a second decode).
struct SpoolItemEntry {
    offset: u64,
    len: u64,
    raw_len: u64,
    preview: Option<String>,
}
type SpoolItemIndex = Vec<SpoolItemEntry>;

/// Index a spooled CBOR sequence: one pass, one item resident at a time.
async fn scan_spooled_sequence(
    path: &std::path::Path,
) -> Result<SpoolItemIndex, ExecutionError> {
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(path).await.map_err(|e| {
        ExecutionError::HostError(format!(
            "failed to open region input spool '{}': {e}",
            path.display()
        ))
    })?;
    let mut index = SpoolItemIndex::new();
    let mut buf: Vec<u8> = Vec::new();
    let mut read_buf = vec![0u8; 256 * 1024];
    let mut offset: u64 = 0;
    loop {
        let n = file.read(&mut read_buf).await.map_err(|e| {
            ExecutionError::HostError(format!(
                "failed to read region input spool '{}': {e}",
                path.display()
            ))
        })?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&read_buf[..n]);
        loop {
            if buf.is_empty() {
                break;
            }
            let mut cursor = std::io::Cursor::new(buf.as_slice());
            let value: ciborium::Value = match ciborium::de::from_reader(&mut cursor) {
                Ok(v) => v,
                Err(_) => break, // incomplete — read more
            };
            let consumed = cursor.position() as u64;
            let raw = crate::orchestrator::stream_io::unwrap_cbor_value(value, index.len())
                .map_err(|e| {
                    ExecutionError::HostError(format!(
                        "region input spool '{}' item {}: {e}",
                        path.display(),
                        index.len()
                    ))
                })?;
            index.push(SpoolItemEntry {
                offset,
                len: consumed,
                raw_len: raw.len() as u64,
                preview: item_preview_snippet(&raw),
            });
            offset += consumed;
            buf.drain(..consumed as usize);
        }
    }
    if !buf.is_empty() {
        return Err(ExecutionError::HostError(format!(
            "{} bytes of an incomplete CBOR item at the end of region input spool              '{}' — truncated intermediate",
            buf.len(),
            path.display()
        )));
    }
    Ok(index)
}

/// Read + decode ONE spooled item for a body dispatch.
async fn read_spool_item(
    path: &std::path::Path,
    offset: u64,
    len: u64,
    item_index: usize,
) -> Result<Vec<u8>, ExecutionError> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let mut file = tokio::fs::File::open(path).await.map_err(|e| {
        ExecutionError::HostError(format!(
            "failed to open region input spool '{}': {e}",
            path.display()
        ))
    })?;
    file.seek(std::io::SeekFrom::Start(offset)).await.map_err(|e| {
        ExecutionError::HostError(format!(
            "failed to seek region input spool '{}': {e}",
            path.display()
        ))
    })?;
    let mut bytes = vec![0u8; len as usize];
    file.read_exact(&mut bytes).await.map_err(|e| {
        ExecutionError::HostError(format!(
            "failed to read item {item_index} from region input spool '{}': {e}",
            path.display()
        ))
    })?;
    let value: ciborium::Value = ciborium::de::from_reader(bytes.as_slice()).map_err(|e| {
        ExecutionError::HostError(format!(
            "region input spool '{}' item {item_index} does not decode: {e}",
            path.display()
        ))
    })?;
    crate::orchestrator::stream_io::unwrap_cbor_value(value, item_index)
        .map_err(|e| ExecutionError::HostError(e.to_string()))
}

/// One item delivered to the region driver: the RAW body-input bytes plus
/// the UI facts computed where the bytes were last resident.
struct RegionItemDelivery {
    bytes: Vec<u8>,
    preview: Option<String>,
    byte_count: u64,
}

/// A region's item source, unified across the three input kinds: bounded
/// in-memory items, an ended unbounded stream spooled to disk, and a LIVE
/// host-opened feed (13.2 §Reference Media, host resolution). A per-source
/// task feeds a small bounded channel; the driver dispatches one body per
/// delivery as it arrives — for a live feed that is per-item dispatch WHILE
/// the capture runs.
struct RegionItemSource {
    rx: tokio::sync::mpsc::Receiver<Result<RegionItemDelivery, ExecutionError>>,
    /// `Some(n)` when the item total is known up front (memory / spool);
    /// `None` for a live feed — the total exists only once the feed ends.
    known_total: Option<usize>,
    /// The host-held tap of a live source (stop/drain + overrun accounting).
    feed_handle: Option<crate::bifaci::live_feed::LiveFeedHandle>,
}

/// Source-task channel capacity: small on purpose — dispatch (bounded by
/// the in-flight semaphore) is the pacing stage, not this buffer.
const REGION_SOURCE_CHANNEL_CAP: usize = 4;

fn region_source_from_memory(items: Vec<Vec<u8>>) -> RegionItemSource {
    let known = items.len();
    let (tx, rx) = tokio::sync::mpsc::channel(REGION_SOURCE_CHANNEL_CAP);
    tokio::spawn(async move {
        for bytes in items {
            let delivery = RegionItemDelivery {
                preview: item_preview_snippet(&bytes),
                byte_count: bytes.len() as u64,
                bytes,
            };
            if tx.send(Ok(delivery)).await.is_err() {
                return; // driver gone (region failed) — nothing to do
            }
        }
    });
    RegionItemSource {
        rx,
        known_total: Some(known),
        feed_handle: None,
    }
}

fn region_source_from_spool(path: std::path::PathBuf, index: SpoolItemIndex) -> RegionItemSource {
    let known = index.len();
    let (tx, rx) = tokio::sync::mpsc::channel(REGION_SOURCE_CHANNEL_CAP);
    tokio::spawn(async move {
        for (i, entry) in index.iter().enumerate() {
            let delivery = match read_spool_item(&path, entry.offset, entry.len, i).await {
                Ok(bytes) => Ok(RegionItemDelivery {
                    preview: entry.preview.clone(),
                    byte_count: entry.raw_len,
                    bytes,
                }),
                Err(e) => Err(e),
            };
            let failed = delivery.is_err();
            if tx.send(delivery).await.is_err() || failed {
                return;
            }
        }
    });
    RegionItemSource {
        rx,
        known_total: Some(known),
        feed_handle: None,
    }
}

/// Bridge a HOST-opened live feed into the driver: unwrap each delivered
/// CBOR item to its raw bytes and compute its UI facts while it is
/// resident. A stream error (device failure, declared overrun failure) is
/// delivered as the source's terminal error — the region fails, it never
/// ends silently short.
fn region_source_from_live(opened: crate::bifaci::live_feed::OpenedFeed) -> RegionItemSource {
    let handle = opened.handle.clone();
    let mut feed_rx = opened.rx;
    let (tx, rx) = tokio::sync::mpsc::channel(REGION_SOURCE_CHANNEL_CAP);
    tokio::spawn(async move {
        while let Some(delivered) = feed_rx.recv().await {
            let delivery = match delivered {
                Ok((value, _meta)) => match value {
                    ciborium::Value::Bytes(bytes) => Ok(RegionItemDelivery {
                        preview: item_preview_snippet(&bytes),
                        byte_count: bytes.len() as u64,
                        bytes,
                    }),
                    other => Err(ExecutionError::HostError(format!(
                        "live feed delivered a non-bytes item ({other:?}) — the \
                         capture contract delivers raw payload bytes"
                    ))),
                },
                Err(e) => Err(ExecutionError::HostError(format!(
                    "live feed failed while driving the ForEach region: {e}"
                ))),
            };
            let failed = delivery.is_err();
            if tx.send(delivery).await.is_err() || failed {
                return;
            }
        }
    });
    RegionItemSource {
        rx,
        known_total: None,
        feed_handle: Some(handle),
    }
}

/// Bound on a map's runs IN FLIGHT at once. Cartridge capacity gates the
/// actual work; this bounds host-side memory (each in-flight run holds its
/// item bytes) so a long feed never loads all its items at once.
const REGION_BODY_MAX_IN_FLIGHT: usize = 32;


/// A bounded UTF-8 preview of an item's bytes, or `None` if binary/empty.
fn item_preview_snippet(bytes: &[u8]) -> Option<String> {
    if bytes.is_empty() {
        return None;
    }
    let mut end = bytes.len().min(ITEM_PREVIEW_CAP);
    for _ in 0..3 {
        match std::str::from_utf8(&bytes[..end]) {
            Ok(text) => return Some(text.to_string()),
            Err(e) if e.valid_up_to() > 0 && end == bytes.len().min(ITEM_PREVIEW_CAP) => {
                end = e.valid_up_to();
            }
            Err(_) => return None,
        }
    }
    match std::str::from_utf8(&bytes[..end]) {
        Ok(text) if !text.is_empty() => Some(text.to_string()),
        _ => None,
    }
}


#[cfg(test)]
mod tests;
