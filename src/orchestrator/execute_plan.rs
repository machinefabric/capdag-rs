//! `execute_plan` — the single ForEach-aware, fan-out-aware plan executor.
//!
//! This is the intelligent execution path, shared by the reference/CLI runtime and
//! the engine. It runs a [`MachinePlan`] (a branching DAG) by partitioning it into:
//!
//! - a **trunk**: the maximal ForEach-free subgraph (input slots + caps not inside any
//!   ForEach body). The trunk is run as a single segment through the pluggable
//!   [`EngineRuntime`], which streams cap-to-cap and handles **fan-out** natively.
//! - one **ForEach region** per ForEach node: the per-item body subgraph. Each region
//!   maps its input sequence, running the body (itself possibly a fan-out subgraph)
//!   once per item and collecting every region node's per-item output back into a
//!   sequence.
//!
//! A plan has one terminal per `Output` node — a linear machine has one, a fan-out
//! machine has several — so the result is a [`PipelineResult`] of [`TerminalOutput`]s.
//!
//! Backend differences (how cartridges are hosted, whether terminal output is
//! persisted) live behind [`EngineRuntime`]; the partition, per-item fan-out, and
//! result assembly live here once.
//!
//! Nested ForEach (a sequence produced inside a body, re-mapped) is a hard error.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::cap::registry::FabricRegistry;
use crate::orchestrator::plan_converter::plan_to_resolved_graph;
use crate::orchestrator::stream_io::{PipelineLogRecord, PipelineProgressTracker};
use crate::orchestrator::types::ResolvedGraph;
use crate::orchestrator::ParseOrchestrationError;
use crate::planner::plan_analysis::derive_foreach_media_urns;
use crate::planner::{
    ExecutionNodeType, InputCardinality, MachineNode, MachinePlan, MachinePlanEdge, StepToken,
};
use crate::{
    BodyOutcome, CapProgressFn, CapStepProgressFn, ExecutionError, PipelineLogFn, StreamMeta,
    PIPELINE_STALL_TIMEOUT_SECS,
};

/// How much of an item's bytes to keep as a preview snippet.
const ITEM_PREVIEW_CAP: usize = 2048;

// =============================================================================
// Result + callback types
// =============================================================================

/// Result of executing a plan. A plan has one [`TerminalOutput`] per `Output` node —
/// a linear machine has one, a fan-out machine has several (one per terminal anchor).
#[derive(Debug, Clone)]
pub struct PipelineResult {
    /// One entry per plan `Output` node, keyed by its node id.
    pub terminals: Vec<TerminalOutput>,
    /// Per-body outcome tracking across every ForEach region (and one entry per
    /// ForEach-free trunk execution).
    pub body_outcomes: Vec<BodyOutcome>,
}

impl PipelineResult {
    /// The terminal for a given `Output` node id.
    pub fn terminal(&self, output_node_id: &str) -> Option<&TerminalOutput> {
        self.terminals
            .iter()
            .find(|t| t.output_node_id == output_node_id)
    }
}

/// One plan terminal (`Output` node) and the data produced at it.
///
/// When the runtime persists terminal output, `writer_results` carries the file paths
/// and `items` is empty; otherwise `items` holds the in-memory data.
#[derive(Debug, Clone)]
pub struct TerminalOutput {
    /// The plan `Output` node id this terminal corresponds to (e.g. `output_5`).
    pub output_node_id: String,
    /// Unwrapped output blobs. Empty when `writer_results` is populated (on disk).
    pub items: Vec<OutputItem>,
    /// Whether the output is a sequence — a cap with sequence output, or the per-item
    /// results of a ForEach region (structurally a sequence regardless of item count).
    pub is_sequence: bool,
    /// Terminal output media URN.
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

/// Result of a runtime persisting a terminal segment's output to disk.
///
/// A segment run through `run_segment` is always linear with a single sink, so one
/// `WriterResult` corresponds to one terminal — `execute_plan` tracks which sink node
/// that is from the segment it ran.
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

/// One initial input of a plan execution.
///
/// `Bytes` is materialized content (a read file, a typed value). A
/// `LiveReference` is a live-capture SOURCE (13.2 §Reference Media, live
/// family): the selector bytes travel to the first consuming cap labeled
/// with the REFERENCE urn, and that cap's cartridge resolves capture — the
/// engine never touches a device. The machine then stops per 15.2 §Runs
/// Stop (stop condition, operator stop → drain, or abort).
#[derive(Debug, Clone)]
pub enum PlanInput {
    Bytes(Vec<u8>),
    LiveReference {
        reference_urn: String,
        selector: Vec<u8>,
    },
}

impl PlanInput {
    /// The bytes placed at the input node: content bytes, or the selector.
    pub fn into_node_bytes(self) -> Vec<u8> {
        match self {
            PlanInput::Bytes(b) => b,
            PlanInput::LiveReference { selector, .. } => selector,
        }
    }

    pub fn live_reference_urn(&self) -> Option<&str> {
        match self {
            PlanInput::Bytes(_) => None,
            PlanInput::LiveReference { reference_urn, .. } => Some(reference_urn),
        }
    }
}

/// Stable address of one body within a particular ForEach boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForEachBodyCoordinate {
    pub foreach_token_id: StepToken,
    pub body_index: usize,
}

/// Transient description of one materialized ForEach input item. The bytes remain
/// on the pipeline wire; only this bounded UI snapshot leaves the executor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForEachItemSnapshot {
    pub foreach_token_id: StepToken,
    pub body_index: usize,
    pub item_preview_text: Option<String>,
    pub item_byte_count: u64,
}

/// Per-item callback — delivers each output item as it is produced.
pub type PipelineItemFn = Arc<dyn Fn(&OutputItem, usize) + Send + Sync>;

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

/// Output of running one resolved (ForEach-free) segment through an [`EngineRuntime`].
///
/// A segment is now an arbitrary ForEach-free DAG (fan-out and convergence included),
/// so its output is the multi-sink [`DagOutput`] the shared executor produces: every
/// node's items, each node's cardinality, and per-persisted-sink writer results.
pub use crate::orchestrator::executor::DagOutput as SegmentOutput;

// =============================================================================
// EngineRuntime — the pluggable cartridge-execution backend
// =============================================================================

/// The cartridge-execution backend `execute_plan` streams segments through.
///
/// Implementations differ only in how cartridges are hosted and whether terminal
/// output is persisted — the trunk/region partition and result assembly are backend
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
    /// `graph` and returns its already-populated switch). Called once per segment; the
    /// returned switch is shared, never torn down here.
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
    /// persisted sink gets a writer bound to the run artifact dir); `None` for the CLI
    /// (everything stays in memory). Default `None`.
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

    /// The fabric registry, for plan→resolved-graph conversion.
    fn fabric_registry(&self) -> Arc<FabricRegistry>;

    /// ForEach partial-failure policy: `"fail"` (any body failure fails the plan) or
    /// `"allow"` (fail only when every body failed).
    async fn foreach_partial_failure_policy(&self) -> String;

    // ── Provided orchestration: identical for every backend ──

    /// Run one resolved segment — an arbitrary ForEach-free DAG (fan-out and
    /// convergence included). The shared executor decomposes it into pipelined chains,
    /// materialising producers at every non-linear junction, so both backends handle
    /// identical input.
    ///
    /// This is the abstraction's job and is the SAME for every backend: get the shared
    /// switch, build a per-segment [`ExecutionContext`] over it, set inputs, wire the
    /// optional protocol trace, and drive `run_dag_on_context`. Backends customise only
    /// via the hooks above — they do not override this.
    ///
    /// - `body_coordinate`: stable ForEach token + local index for a body segment.
    /// - `persist_sinks`: the sink node ids whose output is plan terminal. A persisting
    ///   runtime writes each to disk (one writer per sink) and returns its
    ///   `writer_results`; every other node is kept in memory.
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
            if let Some(reference_urn) = input.live_reference_urn() {
                ctx.set_node_live_reference(node.clone(), reference_urn.to_string());
            }
            ctx.set_node_data(node, input.into_node_bytes());
        }

        // Optional per-segment protocol trace (CLI). When a sink is supplied, sample the
        // switch's L8 snapshot live every 250ms so a HANGING segment still leaves a stall
        // line, and snapshot once more at teardown on both the Ok and Err paths. A live
        // sample write failure is logged and swallowed; the final snapshot is fail-hard.
        let trace_sink = self.trace_sink();
        let trace_label = trace_sink.as_ref().map(|_| {
            graph
                .edges
                .last()
                .map(|e| e.cap_urn.clone())
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
// Region model
// =============================================================================

/// One ForEach region: the per-item body subgraph mapped over `input_node`'s sequence.
struct Region {
    fe_id: String,
    /// Stable identity of the originating ForEach strand step. `fe_id` is only
    /// the planner-local node key and must never cross the run-state boundary.
    step_token_id: StepToken,
    /// The producer node whose sequence output this region iterates.
    input_node: String,
    /// The body's per-item entry cap (fed the single item).
    body_entry: String,
    /// Synthetic input-slot id for the body sub-plan.
    body_input_id: String,
    /// The item's media URN (the element type of the input sequence).
    item_media: String,
    /// Every cap node inside the body (in-body fan-out included).
    body_nodes: Vec<String>,
    /// Cap URNs of the body, for outcome reporting.
    body_cap_urns: Vec<String>,
}

/// Compute the ForEach regions of a plan. Each ForEach node defines a region whose body
/// is every cap reachable from `body_entry` via `Direct` edges (in-body fan-out
/// included), stopping at `Output` nodes. Regions are disjoint (one producer per node).
async fn compute_regions(
    plan: &MachinePlan,
    registry: &FabricRegistry,
) -> Result<Vec<Region>, ExecutionError> {
    // Forward adjacency over Direct edges only (Iteration/Collection wire ForEach nodes).
    let mut direct_adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for edge in &plan.edges {
        if matches!(edge.edge_type, crate::planner::EdgeType::Direct) {
            direct_adj
                .entry(edge.from_node.as_str())
                .or_default()
                .push(edge.to_node.as_str());
        }
    }

    let mut regions = Vec::new();
    for (fe_id, node) in &plan.nodes {
        let ExecutionNodeType::ForEach {
            token_id,
            input_node,
            body_entry,
            ..
        } = &node.node_type
        else {
            continue;
        };

        // BFS the body: caps reachable from body_entry via Direct edges, excluding
        // Output nodes and any other ForEach node. A cap whose input is a
        // SEQUENCE closes the region: it consumes the region's collected
        // per-item output as one sequence (the fold), so it belongs to the
        // post-region trunk, never to the body.
        let mut body_nodes: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(body_entry.clone());
        seen.insert(body_entry.clone());
        while let Some(nid) = queue.pop_front() {
            match plan.nodes.get(&nid).map(|n| &n.node_type) {
                Some(ExecutionNodeType::Cap { cap_urn, .. }) => {
                    if nid != *body_entry {
                        let cap = registry.get_cached_cap(cap_urn).ok_or_else(|| {
                            ExecutionError::HostError(format!(
                                "cap '{cap_urn}' at '{nid}' is not in the registry cache"
                            ))
                        })?;
                        let (input_is_sequence, _) = cap.sequence_shape();
                        if input_is_sequence {
                            // Fold consumer — closes the region; runs post-region.
                            continue;
                        }
                    }
                    body_nodes.push(nid.clone());
                }
                Some(ExecutionNodeType::ForEach { .. }) => {
                    return Err(ExecutionError::HostError(format!(
                        "nested ForEach reached from body of '{fe_id}' at '{nid}' — unsupported"
                    )));
                }
                _ => continue, // Output / InputSlot / Collect: not body caps.
            }
            if let Some(children) = direct_adj.get(nid.as_str()) {
                for &child in children {
                    if seen.insert(child.to_string()) {
                        queue.push_back(child.to_string());
                    }
                }
            }
        }
        body_nodes.sort();

        let body_cap_urns: Vec<String> = body_nodes
            .iter()
            .filter_map(|nid| match &plan.nodes.get(nid)?.node_type {
                ExecutionNodeType::Cap { cap_urn, .. } => Some(cap_urn.clone()),
                _ => None,
            })
            .collect();

        let (_list_media, item_media) =
            derive_foreach_media_urns(plan, input_node).map_err(|e| {
                ExecutionError::HostError(format!("derive foreach item media for '{fe_id}': {e}"))
            })?;
        regions.push(Region {
            fe_id: fe_id.clone(),
            step_token_id: token_id.clone(),
            input_node: input_node.clone(),
            body_entry: body_entry.clone(),
            body_input_id: format!("{fe_id}_body_input"),
            item_media,
            body_nodes,
            body_cap_urns,
        });
    }
    // Deterministic order.
    regions.sort_by(|a, b| a.fe_id.cmp(&b.fe_id));
    Ok(regions)
}

/// The trunk caps that depend — transitively, over data-flow edges — on a
/// ForEach region's output: they can only run AFTER the regions (a fold cap
/// consuming the collected per-item sequence, and everything downstream of
/// it). Includes gather `Collect` nodes on the post side.
fn compute_post_region_caps(plan: &MachinePlan, region_nodes: &HashSet<String>) -> HashSet<String> {
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for edge in &plan.edges {
        if matches!(
            edge.edge_type,
            crate::planner::EdgeType::Direct
                | crate::planner::EdgeType::Arg { .. }
                | crate::planner::EdgeType::Collection
        ) {
            adj.entry(edge.from_node.as_str())
                .or_default()
                .push(edge.to_node.as_str());
        }
    }
    let mut post: HashSet<String> = HashSet::new();
    let mut queue: std::collections::VecDeque<&str> =
        region_nodes.iter().map(|s| s.as_str()).collect();
    let mut seen: HashSet<&str> = queue.iter().copied().collect();
    while let Some(nid) = queue.pop_front() {
        if let Some(children) = adj.get(nid) {
            for &child in children {
                if !seen.insert(child) {
                    continue;
                }
                queue.push_back(child);
                if region_nodes.contains(child) {
                    continue;
                }
                match plan.nodes.get(child).map(|n| &n.node_type) {
                    Some(ExecutionNodeType::Cap { .. })
                    | Some(ExecutionNodeType::Collect { .. }) => {
                        post.insert(child.to_string());
                    }
                    _ => {}
                }
            }
        }
    }
    post
}

/// The post-region sub-plan: the post caps/Collects with every data-flow edge
/// INTO them (external producers stay as root references — their materialized
/// data seeds the segment's node table).
fn build_post_subplan(plan: &MachinePlan, post_ids: &HashSet<String>) -> MachinePlan {
    let mut post = MachinePlan::new(&format!("{} [post-region]", plan.name));
    for id in post_ids {
        if let Some(node) = plan.nodes.get(id) {
            post.add_node(node.clone());
        }
    }
    for edge in &plan.edges {
        if matches!(
            edge.edge_type,
            crate::planner::EdgeType::Direct
                | crate::planner::EdgeType::Arg { .. }
                | crate::planner::EdgeType::Collection
        ) && post_ids.contains(&edge.to_node)
        {
            post.add_edge(edge.clone());
        }
    }
    post
}

/// The trunk sub-plan: input slots + every cap NOT inside a ForEach body, with the
/// `Direct` edges among them. ForEach nodes, region caps, and `Output` nodes are
/// dropped — the resulting graph is ForEach-free and fan-out-capable.
fn build_trunk_subplan(plan: &MachinePlan, region_nodes: &HashSet<String>) -> MachinePlan {
    let mut trunk = MachinePlan::new(&format!("{} [trunk]", plan.name));
    let mut trunk_ids: HashSet<String> = HashSet::new();
    for (id, node) in &plan.nodes {
        match &node.node_type {
            ExecutionNodeType::InputSlot { .. } => {
                trunk.add_node(node.clone());
                trunk_ids.insert(id.clone());
            }
            ExecutionNodeType::Cap { .. } if !region_nodes.contains(id) => {
                trunk.add_node(node.clone());
                trunk_ids.insert(id.clone());
            }
            // Standalone / gather Collects (output_media_urn set) are trunk
            // structure: plan_converter resolves them through into per-producer
            // edges and the segment executor gathers at the consuming arg.
            ExecutionNodeType::Collect {
                output_media_urn: Some(_),
                ..
            } => {
                trunk.add_node(node.clone());
                trunk_ids.insert(id.clone());
            }
            _ => {}
        }
    }
    for edge in &plan.edges {
        // Data-flow edges among trunk nodes: the main input (`Direct`), any
        // convergence input (`Arg`), and gather wiring into a Collect
        // (`Collection`). All must survive into the subgraph the executor runs.
        if matches!(
            edge.edge_type,
            crate::planner::EdgeType::Direct
                | crate::planner::EdgeType::Arg { .. }
                | crate::planner::EdgeType::Collection
        ) && trunk_ids.contains(&edge.from_node)
            && trunk_ids.contains(&edge.to_node)
        {
            trunk.add_edge(edge.clone());
        }
    }
    trunk
}

/// The body sub-plan for a region: a synthetic input slot feeding `body_entry`, plus
/// every body cap and the `Direct` edges among them. Run once per item; the executor
/// reads each body cap's output from the returned `node_data` (so in-body fan-out is
/// handled by the segment runtime, which does fan-out).
fn build_body_subplan(plan: &MachinePlan, region: &Region) -> MachinePlan {
    let mut body = MachinePlan::new(&format!("{} [foreach body {}]", plan.name, region.fe_id));
    body.add_node(MachineNode::input_slot(
        &region.body_input_id,
        "item_input",
        &region.item_media,
        InputCardinality::Single,
    ));
    let body_set: HashSet<&str> = region.body_nodes.iter().map(|s| s.as_str()).collect();
    for nid in &region.body_nodes {
        if let Some(node) = plan.nodes.get(nid) {
            body.add_node(node.clone());
        }
    }
    body.add_edge(MachinePlanEdge::direct(
        &region.body_input_id,
        &region.body_entry,
    ));
    for edge in &plan.edges {
        // Data-flow edges among body nodes: the main input (`Direct`) and any in-body
        // convergence input (`Arg`).
        if matches!(
            edge.edge_type,
            crate::planner::EdgeType::Direct | crate::planner::EdgeType::Arg { .. }
        ) && body_set.contains(edge.from_node.as_str())
            && body_set.contains(edge.to_node.as_str())
        {
            body.add_edge(edge.clone());
        }
    }
    body
}

// =============================================================================
// Sub-plan execution
// =============================================================================

/// Run one ForEach-free `MachinePlan` (the trunk, or a ForEach body) as a single DAG
/// through the engine runtime. The sub-plan may fan out and converge freely; the
/// shared executor (`run_dag_on_context`, inside `run_segment`) decomposes it into
/// pipelined chains, materialising producers at every non-linear junction — so the old
/// per-chain decomposition is gone and convergence (`Arg`) edges survive to execution.
/// `roots` supplies each input slot's materialised bytes + sequence flag (keyed by the
/// slot node id); `persist_sinks` names the cap nodes whose output is a plan terminal.
/// Returns the runtime's multi-sink [`SegmentOutput`]. Progress `p` is mapped into
/// `[base, base + weight]`.
#[allow(clippy::too_many_arguments)]
async fn run_subplan(
    runtime: &Arc<dyn EngineRuntime>,
    registry: &Arc<FabricRegistry>,
    subplan: &MachinePlan,
    roots: HashMap<String, (PlanInput, bool)>,
    persist_sinks: &HashSet<String>,
    cap_arguments: &HashMap<String, Vec<(String, Vec<u8>)>>,
    progress_fn: Option<&CapProgressFn>,
    step_progress_fn: Option<&CapStepProgressFn>,
    log_fn: Option<&PipelineLogFn>,
    body_coordinate: Option<ForEachBodyCoordinate>,
    stall_tracker: Option<Arc<PipelineProgressTracker>>,
    progress_base: f32,
    progress_weight: f32,
) -> Result<SegmentOutput, ExecutionError> {
    let graph = to_graph(subplan, registry).await?;

    let mut inputs: HashMap<String, PlanInput> = HashMap::new();
    let mut is_seq: HashMap<String, bool> = HashMap::new();
    for (id, (input, seq)) in roots {
        inputs.insert(id.clone(), input);
        is_seq.insert(id, seq);
    }

    // Scale the segment's own [0,1] progress into this sub-plan's slice of the run.
    let seg_pfn: Option<CapProgressFn> = progress_fn.map(|parent| {
        let parent = parent.clone();
        Arc::new(move |p: f32, cap_urn: &str, msg: &str| {
            parent(progress_base + progress_weight * p, cap_urn, msg);
        }) as CapProgressFn
    });

    runtime
        .run_segment(
            &graph,
            inputs,
            is_seq,
            cap_arguments,
            seg_pfn.as_ref(),
            step_progress_fn,
            log_fn,
            body_coordinate,
            stall_tracker,
            persist_sinks,
        )
        .await
}

// =============================================================================
// execute_plan
// =============================================================================

/// Execute a `MachinePlan` (a branching DAG), returning one [`TerminalOutput`] per
/// plan `Output` node.
#[allow(clippy::too_many_arguments)]
pub async fn execute_plan(
    plan: &MachinePlan,
    runtime: Arc<dyn EngineRuntime>,
    initial_inputs: HashMap<String, PlanInput>,
    initial_is_sequence: HashMap<String, bool>,
    arguments: &crate::orchestrator::run_arguments::RunArgumentLedger,
    progress_fn: Option<&CapProgressFn>,
    step_progress_fn: Option<&CapStepProgressFn>,
    log_fn: Option<&PipelineLogFn>,
    item_fn: Option<&PipelineItemFn>,
    body_outcome_fn: Option<&BodyOutcomeFn>,
    foreach_items_fn: Option<&ForEachItemsFn>,
) -> Result<PipelineResult, ExecutionError> {
    let registry = runtime.fabric_registry();

    // Terminals: (Output node id, source producer node id), deterministically ordered.
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
    if outputs.is_empty() {
        return Err(ExecutionError::HostError(
            "plan has no Output node".to_string(),
        ));
    }

    let regions = compute_regions(plan, &registry).await?;
    let region_nodes: HashSet<String> = regions
        .iter()
        .flat_map(|r| r.body_nodes.iter().cloned())
        .collect();
    // Caps that consume region output (the fold and everything after it) run
    // AFTER the regions, in their own segment.
    let post_ids = compute_post_region_caps(plan, &region_nodes);

    // Accumulated producer output items (node id → items) across trunk + regions.
    let mut node_data: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
    let mut node_seq: HashMap<String, bool> = HashMap::new();
    let mut node_writers: HashMap<String, Vec<WriterResult>> = HashMap::new();
    let mut body_outcomes: Vec<BodyOutcome> = Vec::new();
    // Spooled UNBOUNDED intermediates (chain-split boundaries, L16): node id
    // → spool path. Ownership arrives with each segment's DagOutput; the
    // guard removes every file when the plan completes — success or error.
    struct PlanSpoolCleanup(Vec<std::path::PathBuf>);
    impl Drop for PlanSpoolCleanup {
        fn drop(&mut self) {
            for path in &self.0 {
                let _ = std::fs::remove_file(path);
            }
        }
    }
    let mut spool_cleanup = PlanSpoolCleanup(Vec::new());
    let mut node_spools: HashMap<String, std::path::PathBuf> = HashMap::new();

    // Cap nodes whose output is a plan terminal — persisted when the runtime persists.
    let persist_sinks: HashSet<String> = outputs.iter().map(|(_, src)| src.clone()).collect();

    // ── Trunk (ForEach-free, pre-region) — decomposed into linear chains,
    // materialized at fan-out. Region caps AND post-region caps are excluded. ──
    let mut trunk_excluded = region_nodes.clone();
    trunk_excluded.extend(post_ids.iter().cloned());
    let trunk = build_trunk_subplan(plan, &trunk_excluded);
    // A live source feeding a ForEach region DIRECTLY is resolved by the
    // HOST (13.2 §Reference Media, host resolution): the engine is the
    // runtime that iterates the items, so the engine opens the device
    // itself and dispatches one body per delivered item. Collect those
    // references here — the region loop below builds a live item source
    // from each. (A live source feeding LINEAR caps keeps the cartridge-side
    // resolution: the first consuming cap's runtime opens the device.)
    let mut live_region_inputs: HashMap<String, (String, Vec<u8>)> = HashMap::new();
    for (k, v) in &initial_inputs {
        if let PlanInput::LiveReference {
            reference_urn,
            selector,
        } = v
        {
            if regions.iter().any(|r| &r.input_node == k) {
                live_region_inputs
                    .insert(k.clone(), (reference_urn.clone(), selector.clone()));
            }
        }
    }
    let mut trunk_roots: HashMap<String, (PlanInput, bool)> = HashMap::new();
    for (k, v) in initial_inputs {
        let seq = *initial_is_sequence.get(&k).ok_or_else(|| {
            ExecutionError::HostError(format!("initial input '{k}' has no sequence flag"))
        })?;
        trunk_roots.insert(k, (v, seq));
    }

    // Trunk gets the first slice of the progress band; regions share the rest.
    let trunk_weight = if regions.is_empty() { 1.0 } else { 0.15 };
    let trunk_start = Instant::now();
    // The trunk's caps dispatch NOW: journal them and read their values in one
    // atomic ledger step, so a mid-run argument update lands entirely before
    // or entirely after this segment — never between its caps.
    let trunk_arguments = arguments.snapshot_for_segment(&trunk);
    let trunk_seg = run_subplan(
        &runtime,
        &registry,
        &trunk,
        trunk_roots,
        &persist_sinks,
        &trunk_arguments,
        progress_fn,
        step_progress_fn,
        log_fn,
        None,
        None,
        0.0,
        trunk_weight,
    )
    .await?;
    let trunk_ms = trunk_start.elapsed().as_millis() as u64;
    let trunk_writers = trunk_seg.writer_results;
    // Cap URNs run in the trunk, for the trunk BodyOutcome (linear/no-ForEach case).
    let trunk_cap_urns: Vec<String> = trunk
        .nodes
        .values()
        .filter_map(|n| match &n.node_type {
            ExecutionNodeType::Cap { cap_urn, .. } => Some(cap_urn.clone()),
            _ => None,
        })
        .collect();
    for (nid, items) in trunk_seg.node_data {
        node_data.insert(nid, items);
    }
    for (nid, seq) in &trunk_seg.node_is_sequence {
        node_seq.insert(nid.clone(), *seq);
    }
    let transients_on = runtime.transient_artifact_root().is_some();
    for (nid, path) in trunk_seg.node_spool {
        // With transient capture ON, spooled intermediates were ADOPTED under
        // the run's transient root — the TTL reaper owns them, never this
        // guard.
        if !transients_on {
            spool_cleanup.0.push(path.clone());
        }
        node_spools.insert(nid, path);
    }
    // Record a trunk BodyOutcome ONLY for the linear/no-ForEach case, where the trunk
    // is the whole pipeline (one outcome, like a linear run). When ForEach regions
    // exist, `body_outcomes` must be the per-item bodies only — the trunk caps surface
    // as their own run-graph nodes, so a trunk outcome would render as a phantom extra
    // media item (an empty "Item 1") in the ForEach group.
    if regions.is_empty() {
        let trunk_saved: Vec<String> = trunk_writers
            .values()
            .flatten()
            .flat_map(|w| w.saved_paths.clone())
            .collect();
        let trunk_bytes: usize = trunk_writers
            .values()
            .flatten()
            .map(|w| w.total_bytes)
            .sum();
        body_outcomes.push(BodyOutcome {
            foreach_token_id: None,
            body_index: 0,
            success: true,
            cap_urns: trunk_cap_urns,
            failed_token_id: None,
            error: None,
            failed_arg_urn: None,
            title: None,
            saved_paths: trunk_saved,
            total_bytes: trunk_bytes,
            duration_ms: trunk_ms,
            item_preview_text: None,
            item_byte_count: 0,
        });
    }
    for (sink, ws) in trunk_writers {
        node_writers.entry(sink).or_default().extend(ws);
    }

    // ── ForEach regions ──
    let post_weight = if post_ids.is_empty() { 0.0 } else { 0.2 };
    let region_band = 1.0 - trunk_weight - post_weight;
    let region_slice = if regions.is_empty() {
        0.0
    } else {
        region_band / regions.len() as f32
    };
    for (ri, region) in regions.iter().enumerate() {
        // The region's item source, by input kind:
        // - a LIVE reference feeding the region directly → the HOST opens the
        //   device (13.2 §Reference Media, host resolution) and bodies
        //   dispatch per item WHILE the capture runs;
        // - bounded in-memory trunk output;
        // - a SPOOL FILE (an unbounded trunk stream that ended when its feed
        //   stopped) — indexed once, items streamed from the file so the
        //   whole capture is never memory-resident.
        let source: RegionItemSource =
            if let Some((reference_urn, selector_bytes)) = live_region_inputs.get(&region.input_node)
            {
                let selector = crate::bifaci::live_feed::LiveFeedSelector::parse(selector_bytes)
                    .map_err(|e| {
                        ExecutionError::HostError(format!(
                            "live source '{}' feeding ForEach region '{}': {e}",
                            reference_urn, region.fe_id
                        ))
                    })?;
                let opened = crate::capture::open(
                    reference_urn,
                    selector,
                    Arc::new(std::sync::atomic::AtomicU64::new(0)),
                )
                .map_err(|e| {
                    ExecutionError::HostError(format!(
                        "host capture for ForEach region '{}': {e}",
                        region.fe_id
                    ))
                })?;
                // Register the tap so a run stop closes it (drain semantics,
                // 15.2 §Runs Stop).
                runtime.on_host_feed_open(&opened.handle);
                region_source_from_live(opened)
            } else if let Some(items) = node_data.get(&region.input_node) {
                // One node, one truth: a region input present in BOTH the
                // in-memory map and the spool map is an executor contract
                // breach (the exact shape that once ran a 682-frame capture
                // as zero bodies) — refused, never guessed at.
                if node_spools.contains_key(&region.input_node) {
                    return Err(ExecutionError::HostError(format!(
                        "ForEach region '{}' input '{}' is recorded BOTH in memory                          and as a spool file — the executor must record exactly one",
                        region.fe_id, region.input_node
                    )));
                }
                region_source_from_memory(items.clone())
            } else if let Some(path) = node_spools.get(&region.input_node) {
                let index = scan_spooled_sequence(path).await?;
                region_source_from_spool(path.clone(), index)
            } else {
                return Err(ExecutionError::HostError(format!(
                    "ForEach region '{}' input '{}' produced no data (have: {:?})",
                    region.fe_id,
                    region.input_node,
                    node_data.keys().collect::<Vec<_>>()
                )));
            };
        let body_plan = build_body_subplan(plan, region);
        let base = trunk_weight + region_slice * ri as f32;

        let per_item = run_region_bodies(
            &runtime,
            &registry,
            &body_plan,
            &persist_sinks,
            region,
            source,
            arguments,
            progress_fn,
            step_progress_fn,
            log_fn,
            item_fn,
            foreach_items_fn,
            body_outcome_fn,
            base,
            region_slice,
            &mut body_outcomes,
        )
        .await?;

        // Accumulate each body node's per-item output into a sequence.
        for nid in &region.body_nodes {
            let mut seq: Vec<Vec<u8>> = Vec::new();
            for (item_map, _) in &per_item {
                if let Some(items) = item_map.get(nid) {
                    seq.extend(items.iter().cloned());
                }
            }
            node_data.insert(nid.clone(), seq);
            // A region node's accumulated output is structurally a sequence.
            node_seq.insert(nid.clone(), true);
        }
        // Region writer results, keyed by sink node.
        for (_, writers_by_sink) in &per_item {
            for (sink, ws) in writers_by_sink {
                node_writers
                    .entry(sink.clone())
                    .or_default()
                    .extend(ws.clone());
            }
        }
    }

    // ── Post-region segment: the fold consumer(s) and everything after them.
    // Their external producers (region body nodes, pre-trunk caps, input slots)
    // are materialized from the accumulated node_data as segment roots — a
    // region node's accumulated per-item output enters as ONE sequence. ──
    if !post_ids.is_empty() {
        let post_plan = build_post_subplan(plan, &post_ids);
        let mut post_roots: HashMap<String, (PlanInput, bool)> = HashMap::new();
        for edge in &plan.edges {
            if !post_ids.contains(&edge.to_node) || post_ids.contains(&edge.from_node) {
                continue;
            }
            if post_roots.contains_key(&edge.from_node) {
                continue;
            }
            let items = node_data.get(&edge.from_node).ok_or_else(|| {
                if node_spools.contains_key(&edge.from_node) {
                    // Strict-state refusal, precisely named: the producer's
                    // unbounded stream was spooled at a chain-split boundary,
                    // and spools do not cross the region/post segment
                    // boundary — the post segment materializes its roots
                    // from memory.
                    ExecutionError::HostError(format!(
                        "post-region segment needs '{}', but that node is an \
                         UNBOUNDED intermediate spooled to disk — a spooled stream \
                         cannot cross into the post-region segment; consume it \
                         within its own segment or restructure the machine",
                        edge.from_node
                    ))
                } else {
                    ExecutionError::HostError(format!(
                        "post-region segment needs '{}' but it produced no data",
                        edge.from_node
                    ))
                }
            })?;
            let is_seq = *node_seq.get(&edge.from_node).ok_or_else(|| {
                ExecutionError::HostError(format!(
                    "post-region segment root '{}' has no sequence flag — a producer \
                     completed without recording its cardinality",
                    edge.from_node
                ))
            })?;
            let bytes = if is_seq {
                crate::orchestrator::cbor_util::wrap_raw_items_as_cbor_sequence(items).map_err(
                    |e| {
                        ExecutionError::HostError(format!(
                            "materialise post-region root '{}': {e}",
                            edge.from_node
                        ))
                    },
                )?
            } else {
                items.first().cloned().ok_or_else(|| {
                    ExecutionError::HostError(format!(
                        "post-region root '{}' holds no items",
                        edge.from_node
                    ))
                })?
            };
            post_roots.insert(edge.from_node.clone(), (PlanInput::Bytes(bytes), is_seq));
        }

        // Post-region caps dispatch now — same atomic journal-and-read as the
        // trunk. Everything before this point was editable "unreached DAG".
        let post_arguments = arguments.snapshot_for_segment(&post_plan);
        let post_seg = run_subplan(
            &runtime,
            &registry,
            &post_plan,
            post_roots,
            &persist_sinks,
            &post_arguments,
            progress_fn,
            step_progress_fn,
            log_fn,
            None,
            None,
            1.0 - post_weight,
            post_weight,
        )
        .await?;
        for (nid, items) in post_seg.node_data {
            node_data.insert(nid, items);
        }
        for (nid, seq) in &post_seg.node_is_sequence {
            node_seq.insert(nid.clone(), *seq);
        }
        for (sink, ws) in post_seg.writer_results {
            node_writers.entry(sink).or_default().extend(ws);
        }
        if !transients_on {
            for path in post_seg.node_spool.values() {
                // Consumed within the post segment; nothing after it reads
                // them. (With transient capture ON these are ADOPTED
                // artifacts under the run's transient root — reaper-owned.)
                let _ = std::fs::remove_file(path);
            }
        }
    }

    // ── Assemble terminals ──
    let mut terminals = Vec::with_capacity(outputs.len());
    for (out_id, src) in &outputs {
        let in_region = region_nodes.contains(src);
        let media_urn = terminal_media(plan, &registry, src).await?;
        let writers = node_writers.remove(src).unwrap_or_default();
        if !writers.is_empty() {
            let is_sequence = in_region || writers.iter().any(|w| w.is_sequence);
            terminals.push(TerminalOutput {
                output_node_id: out_id.clone(),
                items: Vec::new(),
                is_sequence,
                media_urn,
                writer_results: writers,
            });
            continue;
        }
        let data = node_data.get(src).cloned().ok_or_else(|| {
            ExecutionError::HostError(format!(
                "terminal '{out_id}' source '{src}' produced no data"
            ))
        })?;
        let is_sequence = if in_region {
            true
        } else {
            producer_is_sequence(plan, &registry, src).await
        };
        let items: Vec<OutputItem> = data
            .into_iter()
            .enumerate()
            .map(|(i, d)| OutputItem { data: d, index: i })
            .collect();
        if let Some(ifn) = item_fn {
            let n = items.len();
            for it in &items {
                ifn(it, n);
            }
        }
        terminals.push(TerminalOutput {
            output_node_id: out_id.clone(),
            items,
            is_sequence,
            media_urn,
            writer_results: Vec::new(),
        });
    }

    if let Some(pfn) = progress_fn {
        pfn(1.0, "", "Completed");
    }
    Ok(PipelineResult {
        terminals,
        body_outcomes,
    })
}

// =============================================================================
// Region body execution
// =============================================================================

/// Run a ForEach region's body once per input item, concurrently, collecting each
/// item's full body `node_data` (so every body node — in-body fan-out included — is
/// captured) and honoring the runtime's partial-failure policy.
#[allow(clippy::too_many_arguments)]
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

/// Bound on region bodies IN FLIGHT at once. Cartridge capacity gates the
/// actual work; this bounds host-side memory (each in-flight body holds its
/// item bytes) so a long feed never loads all its items at once.
const REGION_BODY_MAX_IN_FLIGHT: usize = 32;

#[allow(clippy::too_many_arguments)]
async fn run_region_bodies(
    runtime: &Arc<dyn EngineRuntime>,
    registry: &Arc<FabricRegistry>,
    body_subplan: &MachinePlan,
    persist_sinks: &HashSet<String>,
    region: &Region,
    mut source: RegionItemSource,
    arguments: &crate::orchestrator::run_arguments::RunArgumentLedger,
    progress_fn: Option<&CapProgressFn>,
    step_progress_fn: Option<&CapStepProgressFn>,
    log_fn: Option<&PipelineLogFn>,
    item_fn: Option<&PipelineItemFn>,
    foreach_items_fn: Option<&ForEachItemsFn>,
    body_outcome_fn: Option<&BodyOutcomeFn>,
    progress_base: f32,
    progress_weight: f32,
    body_outcomes: &mut Vec<BodyOutcome>,
) -> Result<
    Vec<(
        HashMap<String, Vec<Vec<u8>>>,
        HashMap<String, Vec<WriterResult>>,
    )>,
    ExecutionError,
> {
    let fe_token_id = region.step_token_id.clone();
    let known_total = source.known_total;
    // Hoisted so the select handlers never touch `source` while an arm
    // future borrows its receiver.
    let feed_handle = source.feed_handle.take();

    // Per-body progress slots, grown as items are dispatched (a live feed's
    // total is unknown until it ends). Aggregated step progress divides by
    // the known total when there is one, else by the dispatched count, and
    // passes through a monotone clamp so the bar never runs backwards while
    // the denominator grows.
    let body_progress_slots: Arc<std::sync::Mutex<Vec<f32>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let dispatched_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    // Non-negative f32 bit patterns order like the floats — fetch_max works.
    let reported_step_bits = Arc::new(AtomicU32::new(0f32.to_bits()));
    let stall_tracker = Arc::new(PipelineProgressTracker::new());
    let mut stall_warning_logged = false;

    let body_permits = Arc::new(tokio::sync::Semaphore::new(REGION_BODY_MAX_IN_FLIGHT));

    type BodyOk = (
        usize,
        HashMap<String, Vec<Vec<u8>>>,
        HashMap<String, Vec<WriterResult>>,
        u64,
    );
    type BodyErr = (usize, ExecutionError, u64);

    let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<Result<BodyOk, BodyErr>>();

    let mut source_done = false;
    let mut dispatched = 0usize;
    let mut completed = 0usize;
    let mut succeeded = 0usize;
    let mut failed = 0usize;
    let mut item_results: Vec<
        Option<(
            HashMap<String, Vec<Vec<u8>>>,
            HashMap<String, Vec<WriterResult>>,
        )>,
    > = Vec::new();
    let mut item_facts: Vec<(Option<String>, u64)> = Vec::new();

    // Shared step aggregation: slot sum over the denominator, monotone.
    let aggregate_step = {
        let slots = body_progress_slots.clone();
        let dispatched = dispatched_counter.clone();
        let reported = reported_step_bits.clone();
        move || -> f32 {
            let sum: f32 = slots.lock().expect("progress slots").iter().sum();
            let denom = known_total
                .unwrap_or_else(|| dispatched.load(Ordering::Relaxed))
                .max(1) as f32;
            let raw = (sum / denom).clamp(0.0, 1.0);
            let bits = reported.fetch_max(raw.to_bits(), Ordering::Relaxed);
            f32::from_bits(bits.max(raw.to_bits()))
        }
    };

    while !(source_done && completed >= dispatched) {
        tokio::select! {
            biased;
            recv = done_rx.recv(), if completed < dispatched => {
                let Some(result) = recv else { break };
                match result {
                    Ok((i, node_data, writers, ms)) => {
                        stall_tracker.touch();
                        stall_warning_logged = false;
                        let item_bytes: usize = writers
                            .values()
                            .flatten()
                            .map(|w| w.total_bytes)
                            .sum::<usize>()
                            .max(node_data.values().flatten().map(|d| d.len()).sum());
                        let saved_paths: Vec<String> =
                            writers.values().flatten().flat_map(|w| w.saved_paths.clone()).collect();
                        body_outcomes.push(BodyOutcome {
                            foreach_token_id: Some(region.step_token_id.clone()),
                            body_index: i,
                            success: true,
                            cap_urns: region.body_cap_urns.clone(),
                            failed_token_id: None,
                            error: None,
                            failed_arg_urn: None,
                            title: None,
                            saved_paths,
                            total_bytes: item_bytes,
                            duration_ms: ms,
                            item_preview_text: item_facts.get(i).and_then(|f| f.0.clone()),
                            item_byte_count: item_facts.get(i).map(|f| f.1).unwrap_or(0),
                        });
                        if let Some(bofn) = body_outcome_fn { bofn(body_outcomes)?; }
                        if let Some(ifn) = item_fn {
                            // Surface the region's body-entry per-item output as it lands.
                            if let Some(items) = node_data.get(&region.body_entry) {
                                for d in items {
                                    ifn(&OutputItem { data: d.clone(), index: i }, known_total.unwrap_or(dispatched));
                                }
                            }
                        }
                        item_results[i] = Some((node_data, writers));
                        completed += 1;
                        succeeded += 1;
                    }
                    Err((i, e, ms)) => {
                        stall_tracker.touch();
                        stall_warning_logged = false;
                        let error_str = format!("{e}");
                        body_outcomes.push(BodyOutcome {
                            foreach_token_id: Some(region.step_token_id.clone()),
                            body_index: i,
                            success: false,
                            cap_urns: region.body_cap_urns.clone(),
                            failed_token_id: e.step_token_id().cloned(),
                            error: Some(error_str),
                            failed_arg_urn: e.failure_arg_urn().map(str::to_string),
                            title: None,
                            saved_paths: vec![],
                            total_bytes: 0,
                            duration_ms: ms,
                            item_preview_text: item_facts.get(i).and_then(|f| f.0.clone()),
                            item_byte_count: item_facts.get(i).map(|f| f.1).unwrap_or(0),
                        });
                        if let Some(bofn) = body_outcome_fn { bofn(body_outcomes)?; }
                        tracing::error!("[execute_plan] region '{}' body {i} failed: {e}", region.fe_id);
                        completed += 1;
                        failed += 1;
                    }
                }
                if let Some(pfn) = progress_fn {
                    let step = aggregate_step();
                    let total_display = known_total.unwrap_or(dispatched);
                    pfn(progress_base + progress_weight * step, "", &format!("Completed {completed}/{total_display}"));
                }
            }
            delivered = async {
                let permit = body_permits
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("region body semaphore is never closed");
                let item = source.rx.recv().await;
                (permit, item)
            }, if !source_done => {
                let (permit, item) = delivered;
                match item {
                    None => {
                        drop(permit);
                        source_done = true;
                        // No further body will dispatch: from here a mid-run
                        // argument update to these steps is honestly
                        // "already dispatched", not "applied to nothing".
                        arguments.exhaust_bodies(body_subplan);
                        // Overruns at a host-held capture edge are counted loss
                        // (12.5 §Overrun) — surface them, never silently.
                        if let Some(handle) = &feed_handle {
                            let overruns = handle.overruns();
                            if overruns > 0 {
                                if let Some(lfn) = log_fn {
                                    lfn(PipelineLogRecord {
                                        step_token_id: Some(region.step_token_id.clone()),
                                        cap_urn: None,
                                        level: "warn".to_string(),
                                        attribution_class: crate::AttributionClass::Resource,
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
                            }
                        }
                    }
                    Some(Err(e)) => {
                        drop(permit);
                        // The SOURCE failed (device died, spool corrupt): the
                        // region's input is incomplete — this is an input
                        // failure of the whole region, not a body failure,
                        // and no partial-failure policy applies.
                        return Err(ExecutionError::StepFailed {
                            step_token_id: region.step_token_id.clone(),
                            source: Box::new(e),
                        });
                    }
                    Some(Ok(delivery)) => {
                        let i = dispatched;
                        item_facts.push((delivery.preview.clone(), delivery.byte_count));
                        item_results.push(None);
                        body_progress_slots.lock().expect("progress slots").push(0.0);
                        dispatched_counter.store(i + 1, Ordering::Relaxed);

                        // The item's snapshot is published BEFORE its body
                        // spawns — append-only deltas, one per item (the total
                        // is unknowable for a live feed until it ends).
                        if let Some(callback) = foreach_items_fn {
                            callback(&fe_token_id, &[ForEachItemSnapshot {
                                foreach_token_id: fe_token_id.clone(),
                                body_index: i,
                                item_preview_text: delivery.preview.clone(),
                                item_byte_count: delivery.byte_count,
                            }])?;
                        }

                        let runtime = runtime.clone();
                        let registry = registry.clone();
                        let body_subplan = body_subplan.clone();
                        let persist_sinks = persist_sinks.clone();
                        let body_input_id = region.body_input_id.clone();
                        // This body dispatches NOW: journal it and read its
                        // values in one atomic ledger step — the dispatch/
                        // update ordering rule for ForEach bodies.
                        let cap_arguments = arguments.snapshot_for_body(&body_subplan, i);
                        let body_log_fn = log_fn.cloned();
                        let body_stall_tracker = stall_tracker.clone();
                        let done_tx = done_tx.clone();
                        let body_coordinate = ForEachBodyCoordinate {
                            foreach_token_id: fe_token_id.clone(),
                            body_index: i,
                        };

                        let item_pfn: Option<CapProgressFn> = progress_fn.map(|parent| {
                            let parent = parent.clone();
                            let slots = body_progress_slots.clone();
                            let tracker = stall_tracker.clone();
                            let dispatched_now = dispatched_counter.clone();
                            let reported = reported_step_bits.clone();
                            let step_sink = step_progress_fn.cloned();
                            let fe_token_id = fe_token_id.clone();
                            Arc::new(move |p: f32, cap_urn: &str, msg: &str| {
                                {
                                    let mut slots = slots.lock().expect("progress slots");
                                    if let Some(slot) = slots.get_mut(i) {
                                        *slot = p;
                                    }
                                }
                                tracker.touch();
                                let sum: f32 = slots.lock().expect("progress slots").iter().sum();
                                let denom = known_total
                                    .unwrap_or_else(|| dispatched_now.load(Ordering::Relaxed))
                                    .max(1) as f32;
                                let raw = (sum / denom).clamp(0.0, 1.0);
                                let bits = reported.fetch_max(raw.to_bits(), Ordering::Relaxed);
                                let step = f32::from_bits(bits.max(raw.to_bits()));
                                if let Some(sink) = &step_sink {
                                    sink(step, cap_urn, &fe_token_id);
                                }
                                parent(progress_base + progress_weight * step, cap_urn, msg);
                            }) as CapProgressFn
                        });

                        let raw_item_bytes = delivery.bytes;
                        tokio::spawn(async move {
                            let _permit = permit;
                            let started = Instant::now();
                            let mut roots: HashMap<String, (PlanInput, bool)> = HashMap::new();
                            roots.insert(body_input_id, (PlanInput::Bytes(raw_item_bytes), false));
                            // The item's local progress is [0,1]; item_pfn aggregates it across items.
                            let res = run_subplan(
                                &runtime,
                                &registry,
                                &body_subplan,
                                roots,
                                &persist_sinks,
                                &cap_arguments,
                                item_pfn.as_ref(),
                                None,
                                body_log_fn.as_ref(),
                                Some(body_coordinate),
                                Some(body_stall_tracker),
                                0.0,
                                1.0,
                            )
                            .await;
                            let ms = started.elapsed().as_millis() as u64;
                            let _ = match res {
                                Ok(seg) => {
                                    // A body-internal spool was consumed within the body's
                                    // own segment; nothing after the body reads it.
                                    for path in seg.node_spool.values() {
                                        let _ = std::fs::remove_file(path);
                                    }
                                    done_tx.send(Ok((i, seg.node_data, seg.writer_results, ms)))
                                }
                                Err(e) => done_tx.send(Err((i, e, ms))),
                            };
                        });
                        dispatched += 1;
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                if stall_tracker.is_stalled() && !stall_warning_logged {
                    if let Some(lfn) = log_fn {
                        lfn(PipelineLogRecord {
                            step_token_id: Some(region.step_token_id.clone()),
                            cap_urn: None,
                            level: "warn".to_string(),
                            attribution_class: crate::AttributionClass::Internal,
                            message: format!(
                                "This ForEach step has had no progress for \
                                 {PIPELINE_STALL_TIMEOUT_SECS}s; continuing to wait. \
                                 Use Cancel to abort."
                            ),
                            meta: None,
                            body_index: None,
                            arg_urn: None,
                        });
                    }
                    stall_warning_logged = true;
                }
            }
        }
    }
    drop(done_tx);

    let item_count = dispatched;
    if failed > 0 {
        let policy = runtime.foreach_partial_failure_policy().await;
        let should_fail = match policy.as_str() {
            "fail" => true,
            _ => succeeded == 0,
        };
        if should_fail {
            return Err(ExecutionError::StepFailed {
                step_token_id: region.step_token_id.clone(),
                source: Box::new(ExecutionError::HostError(format!(
                    "ForEach step failed: {failed}/{item_count} bodies failed (policy={policy})"
                ))),
            });
        }
    }

    // Ordered by item index; failed items (tolerated by policy) contribute nothing.
    Ok(item_results.into_iter().flatten().collect())
}

// =============================================================================
// Helpers
// =============================================================================

async fn to_graph(
    plan: &MachinePlan,
    registry: &FabricRegistry,
) -> Result<ResolvedGraph, ExecutionError> {
    plan_to_resolved_graph(plan, registry)
        .await
        .map_err(|e: ParseOrchestrationError| {
            ExecutionError::HostError(format!("plan → resolved graph: {e}"))
        })
}

/// The media URN a producer node emits: a cap's output spec, or an input slot's media.
async fn terminal_media(
    plan: &MachinePlan,
    registry: &FabricRegistry,
    source_node: &str,
) -> Result<String, ExecutionError> {
    let node = plan.nodes.get(source_node).ok_or_else(|| {
        ExecutionError::HostError(format!("terminal source '{source_node}' not in plan"))
    })?;
    match &node.node_type {
        ExecutionNodeType::Cap { cap_urn, .. } => {
            let cap = registry.get_cap(cap_urn).await.map_err(|e| {
                ExecutionError::HostError(format!(
                    "resolve cap '{cap_urn}' for terminal media: {e}"
                ))
            })?;
            Ok(cap.urn.out_spec().to_string())
        }
        ExecutionNodeType::InputSlot {
            expected_media_urn, ..
        } => Ok(expected_media_urn.clone()),
        other => Err(ExecutionError::HostError(format!(
            "terminal source '{source_node}' is not a data producer: {other:?}"
        ))),
    }
}

/// Whether a (trunk) producer emits a sequence: a cap by its output cardinality, an
/// input slot by its declared cardinality.
async fn producer_is_sequence(plan: &MachinePlan, registry: &FabricRegistry, node: &str) -> bool {
    match plan.nodes.get(node).map(|n| &n.node_type) {
        Some(ExecutionNodeType::Cap { cap_urn, .. }) => registry
            .get_cached_cap(cap_urn)
            .and_then(|c| c.output.as_ref().map(|o| o.is_sequence))
            .unwrap_or(false),
        Some(ExecutionNodeType::InputSlot { cardinality, .. }) => {
            matches!(cardinality, InputCardinality::Sequence)
        }
        _ => false,
    }
}

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
mod tests {
    use super::*;
    use crate::cap::definition::{ArgSource, Cap, CapArg, CapOutput};
    use crate::planner::MachineNode;
    use crate::CapUrn;

    fn test_cap(urn: &str, output_is_sequence: bool) -> Cap {
        let cap_urn = CapUrn::from_string(urn).expect("valid test cap URN");
        let input = cap_urn.in_spec().to_string();
        let mut output = CapOutput::new(cap_urn.out_spec().to_string(), "output".to_string());
        output.is_sequence = output_is_sequence;
        Cap {
            urn: cap_urn,
            version: 1,
            title: "Test capability".to_string(),
            cap_description: None,
            documentation: None,
            metadata: HashMap::new(),
            aliases: vec!["test".to_string()],
            is_abstract: false,
            args: vec![CapArg::new(
                input.clone(),
                true,
                vec![ArgSource::Stdin { stdin: input }],
            )],
            output: Some(output),
            metadata_json: None,
            registered_by: None,
            supported_model_types: Vec::new(),
            default_model_spec: None,
        }
    }

    struct RecordingRuntime {
        registry: Arc<FabricRegistry>,
    }

    #[async_trait]
    impl EngineRuntime for RecordingRuntime {
        async fn segment_switch(
            &self,
            _graph: &ResolvedGraph,
        ) -> Result<Arc<crate::bifaci::relay_switch::RelaySwitch>, ExecutionError> {
            panic!("recording runtime overrides run_segment")
        }

        async fn activity_timeout_secs(
            &self,
            _graph: &ResolvedGraph,
        ) -> Result<u64, ExecutionError> {
            panic!("recording runtime overrides run_segment")
        }

        fn fabric_registry(&self) -> Arc<FabricRegistry> {
            self.registry.clone()
        }

        async fn foreach_partial_failure_policy(&self) -> String {
            "fail".to_string()
        }

        async fn run_segment(
            &self,
            graph: &ResolvedGraph,
            _initial_inputs: HashMap<String, PlanInput>,
            _initial_is_sequence: HashMap<String, bool>,
            _cap_arguments: &HashMap<String, Vec<(String, Vec<u8>)>>,
            progress_fn: Option<&CapProgressFn>,
            _step_progress_fn: Option<&CapStepProgressFn>,
            _log_fn: Option<&PipelineLogFn>,
            _body_coordinate: Option<ForEachBodyCoordinate>,
            _stall_tracker: Option<Arc<PipelineProgressTracker>>,
            _persist_sinks: &HashSet<String>,
        ) -> Result<SegmentOutput, ExecutionError> {
            let edge = graph
                .edges
                .first()
                .expect("body graph has a capability edge");
            if let Some(progress) = progress_fn {
                progress(1.0, &edge.cap_urn, "complete");
            }
            Ok(SegmentOutput {
                node_data: HashMap::from([(edge.to.clone(), vec![b"result".to_vec()])]),
                node_is_sequence: HashMap::from([(edge.to.clone(), false)]),
                writer_results: HashMap::new(),
                terminal_meta: HashMap::new(),
                node_spool: HashMap::new(),
            })
        }
    }

    /// A runtime that records the PlanInput map its segment received —
    /// pinning what the ENGINE hands the executor for a live source.
    struct InputCapturingRuntime {
        registry: Arc<FabricRegistry>,
        seen_inputs: Arc<std::sync::Mutex<Vec<HashMap<String, PlanInput>>>>,
        seen_flags: Arc<std::sync::Mutex<Vec<HashMap<String, bool>>>>,
    }

    #[async_trait]
    impl EngineRuntime for InputCapturingRuntime {
        async fn segment_switch(
            &self,
            _graph: &ResolvedGraph,
        ) -> Result<Arc<crate::bifaci::relay_switch::RelaySwitch>, ExecutionError> {
            panic!("capturing runtime overrides run_segment")
        }

        async fn activity_timeout_secs(
            &self,
            _graph: &ResolvedGraph,
        ) -> Result<u64, ExecutionError> {
            panic!("capturing runtime overrides run_segment")
        }

        fn fabric_registry(&self) -> Arc<FabricRegistry> {
            self.registry.clone()
        }

        async fn foreach_partial_failure_policy(&self) -> String {
            "fail".to_string()
        }

        async fn run_segment(
            &self,
            graph: &ResolvedGraph,
            initial_inputs: HashMap<String, PlanInput>,
            initial_is_sequence: HashMap<String, bool>,
            _cap_arguments: &HashMap<String, Vec<(String, Vec<u8>)>>,
            _progress_fn: Option<&CapProgressFn>,
            _step_progress_fn: Option<&CapStepProgressFn>,
            _log_fn: Option<&PipelineLogFn>,
            _body_coordinate: Option<ForEachBodyCoordinate>,
            _stall_tracker: Option<Arc<PipelineProgressTracker>>,
            _persist_sinks: &HashSet<String>,
        ) -> Result<SegmentOutput, ExecutionError> {
            self.seen_inputs
                .lock()
                .expect("input capture lock")
                .push(initial_inputs);
            self.seen_flags
                .lock()
                .expect("flag capture lock")
                .push(initial_is_sequence);
            let edge = graph.edges.first().expect("segment has an edge");
            Ok(SegmentOutput {
                node_data: HashMap::from([(edge.to.clone(), vec![b"result".to_vec()])]),
                node_is_sequence: HashMap::from([(edge.to.clone(), false)]),
                writer_results: HashMap::new(),
                terminal_meta: HashMap::new(),
                node_spool: HashMap::new(),
            })
        }
    }

    // TEST1449: a live source enters the executor as PlanInput::LiveReference
    // and reaches run_segment INTACT — reference urn and selector bytes
    // unchanged, sequence flag true. This is the engine half of
    // reference-forwarding (13.2 §Reference Media): losing the reference (or
    // flattening it to bytes-only) would silently run the machine on the
    // SELECTOR TEXT as content.
    #[tokio::test]
    async fn test1449_live_reference_reaches_segment_intact() {
        let mut plan = MachinePlan::new("live-linear");
        plan.add_node(MachineNode::input_slot(
            "input",
            "input",
            "media:audio-frames;pcm",
            crate::planner::InputCardinality::Sequence,
        ));
        plan.add_node(MachineNode::cap(
            "encode",
            "cap:encode-audio-frames;in=\"media:audio-frames;pcm\";out=\"media:audio;ext=wav\"",
        ));
        plan.add_node(MachineNode::output("out", "result", "encode"));
        plan.add_edge(MachinePlanEdge::direct("input", "encode"));
        plan.add_edge(MachinePlanEdge::direct("encode", "out"));

        let registry = FabricRegistry::new_for_test();
        registry.add_caps_to_cache(vec![test_cap(
            "cap:encode-audio-frames;in=\"media:audio-frames;pcm\";out=\"media:audio;ext=wav\"",
            false,
        )]);
        let registry = Arc::new(registry);
        let seen_inputs = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_flags = Arc::new(std::sync::Mutex::new(Vec::new()));
        let runtime: Arc<dyn EngineRuntime> = Arc::new(InputCapturingRuntime {
            registry,
            seen_inputs: seen_inputs.clone(),
            seen_flags: seen_flags.clone(),
        });

        let inputs = HashMap::from([(
            "input".to_string(),
            PlanInput::LiveReference {
                reference_urn: "media:audio;live;microphone".to_string(),
                selector: br#"{"stop":{"duration_ms":5000}}"#.to_vec(),
            },
        )]);
        let flags = HashMap::from([("input".to_string(), true)]);
        execute_plan(
            &plan,
            runtime,
            inputs,
            flags,
            &crate::orchestrator::run_arguments::RunArgumentLedger::new(&plan, HashMap::new())
                .expect("empty ledger"),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("live linear plan executes");

        let seen = seen_inputs.lock().expect("captured inputs");
        assert_eq!(seen.len(), 1, "one trunk segment");
        match seen[0].get("input").expect("live input delivered") {
            PlanInput::LiveReference {
                reference_urn,
                selector,
            } => {
                assert_eq!(reference_urn, "media:audio;live;microphone");
                assert_eq!(selector, br#"{"stop":{"duration_ms":5000}}"#);
            }
            other => panic!("live input flattened to {other:?}"),
        }
        let flags = seen_flags.lock().expect("captured flags");
        assert_eq!(
            flags[0].get("input"),
            Some(&true),
            "a live feed is a sequence at its anchor"
        );
    }

    /// Handles a capless trunk (a live source feeding a region directly has
    /// no trunk caps), records every body's input, and counts host-feed tap
    /// registrations — proving the engine wires stop for host-opened feeds.
    struct LiveRegionRuntime {
        registry: Arc<FabricRegistry>,
        body_inputs: Arc<std::sync::Mutex<Vec<HashMap<String, PlanInput>>>>,
        taps_registered: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl EngineRuntime for LiveRegionRuntime {
        async fn segment_switch(
            &self,
            _graph: &ResolvedGraph,
        ) -> Result<Arc<crate::bifaci::relay_switch::RelaySwitch>, ExecutionError> {
            panic!("live region runtime overrides run_segment")
        }

        async fn activity_timeout_secs(
            &self,
            _graph: &ResolvedGraph,
        ) -> Result<u64, ExecutionError> {
            panic!("live region runtime overrides run_segment")
        }

        fn fabric_registry(&self) -> Arc<FabricRegistry> {
            self.registry.clone()
        }

        async fn foreach_partial_failure_policy(&self) -> String {
            "fail".to_string()
        }

        fn on_host_feed_open(&self, _handle: &crate::bifaci::live_feed::LiveFeedHandle) {
            self.taps_registered
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }

        async fn run_segment(
            &self,
            graph: &ResolvedGraph,
            initial_inputs: HashMap<String, PlanInput>,
            _initial_is_sequence: HashMap<String, bool>,
            _cap_arguments: &HashMap<String, Vec<(String, Vec<u8>)>>,
            _progress_fn: Option<&CapProgressFn>,
            _step_progress_fn: Option<&CapStepProgressFn>,
            _log_fn: Option<&PipelineLogFn>,
            body_coordinate: Option<ForEachBodyCoordinate>,
            _stall_tracker: Option<Arc<PipelineProgressTracker>>,
            _persist_sinks: &HashSet<String>,
        ) -> Result<SegmentOutput, ExecutionError> {
            if body_coordinate.is_some() {
                let edge = graph.edges.first().expect("body graph has an edge");
                self.body_inputs
                    .lock()
                    .expect("body input capture lock")
                    .push(initial_inputs);
                return Ok(SegmentOutput {
                    node_data: HashMap::from([(edge.to.clone(), vec![b"mapped".to_vec()])]),
                    node_is_sequence: HashMap::from([(edge.to.clone(), false)]),
                    writer_results: HashMap::new(),
                    terminal_meta: HashMap::new(),
                    node_spool: HashMap::new(),
                });
            }
            // The trunk of a direct live→region plan has NO caps: nothing to
            // run, nothing produced.
            Ok(SegmentOutput {
                node_data: HashMap::new(),
                node_is_sequence: HashMap::new(),
                writer_results: HashMap::new(),
                terminal_meta: HashMap::new(),
                node_spool: HashMap::new(),
            })
        }
    }

    // TEST1454: a live source mapped DIRECTLY into a ForEach region is
    // resolved BY THE HOST (13.2 §Reference Media, host resolution): the
    // engine opens the feed through the built-in capture dispatch — here the
    // real `media:live;synthetic` backend, no mocks of the seam — and
    // dispatches one body per delivered item while the feed runs. The tap is
    // registered with the runtime so a run stop can close it.
    #[tokio::test]
    async fn test1454_live_source_drives_foreach_region_host_side() {
        let mut plan = MachinePlan::new("live-foreach");
        plan.add_node(MachineNode::input_slot(
            "input",
            "input",
            "media:feed-frames",
            crate::planner::InputCardinality::Sequence,
        ));
        plan.add_node(MachineNode::cap(
            "mapper",
            "cap:in=\"media:feed-frames\";map;out=\"media:fmt=json;record\"",
        ));
        plan.add_node(MachineNode::for_each_token(
            "fe",
            "input",
            "mapper",
            "mapper",
            "live-foreach-token".parse().unwrap(),
        ));
        plan.add_node(MachineNode::output("out", "result", "mapper"));
        plan.add_edge(MachinePlanEdge::direct("input", "fe"));
        plan.add_edge(MachinePlanEdge::iteration("fe", "mapper"));
        plan.add_edge(MachinePlanEdge::direct("mapper", "out"));

        let registry = FabricRegistry::new_for_test();
        registry.add_caps_to_cache(vec![test_cap(
            "cap:in=\"media:feed-frames\";map;out=\"media:fmt=json;record\"",
            false,
        )]);
        let registry = Arc::new(registry);
        let body_inputs: Arc<std::sync::Mutex<Vec<HashMap<String, PlanInput>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let taps_registered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let runtime: Arc<dyn EngineRuntime> = Arc::new(LiveRegionRuntime {
            registry,
            body_inputs: body_inputs.clone(),
            taps_registered: taps_registered.clone(),
        });

        let snapshots: Arc<std::sync::Mutex<Vec<ForEachItemSnapshot>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let snapshots_sink = snapshots.clone();
        let items_fn: ForEachItemsFn = Arc::new(move |_token, items| {
            snapshots_sink
                .lock()
                .expect("snapshot lock")
                .extend(items.iter().cloned());
            Ok(())
        });

        let inputs = HashMap::from([(
            "input".to_string(),
            PlanInput::LiveReference {
                reference_urn: "media:live;synthetic".to_string(),
                selector: br#"{"params":{"items":3,"interval_ms":0,"item_bytes":8}}"#.to_vec(),
            },
        )]);
        let flags = HashMap::from([("input".to_string(), true)]);
        let result = execute_plan(
            &plan,
            runtime,
            inputs,
            flags,
            &crate::orchestrator::run_arguments::RunArgumentLedger::new(&plan, HashMap::new())
                .expect("empty ledger"),
            None,
            None,
            None,
            None,
            None,
            Some(&items_fn),
        )
        .await
        .expect("a live source drives the region host-side");

        // One body per delivered item, with the feed's actual payload bytes
        // (the synthetic backend emits [i % 256; item_bytes]).
        let seen = body_inputs.lock().expect("captured body inputs");
        assert_eq!(seen.len(), 3, "one body per feed item");
        let mut got: Vec<Vec<u8>> = seen
            .iter()
            .map(|m| {
                let (_, input) = m.iter().next().expect("one body root");
                match input {
                    PlanInput::Bytes(b) => b.clone(),
                    other => panic!("body root is raw item bytes, got {other:?}"),
                }
            })
            .collect();
        got.sort();
        assert_eq!(got, vec![vec![0u8; 8], vec![1u8; 8], vec![2u8; 8]]);

        // Snapshots arrived append-only, one delta per item, in index order.
        let snaps = snapshots.lock().expect("snapshots");
        assert_eq!(snaps.len(), 3);
        assert!(snaps
            .iter()
            .enumerate()
            .all(|(i, s)| s.body_index == i && s.item_byte_count == 8));

        // The host-opened tap was registered for stop wiring.
        assert_eq!(
            taps_registered.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the engine must be able to close the feed on a run stop"
        );

        // The region terminal assembled one item per body.
        let terminal = result.terminal("out").expect("region terminal");
        assert!(terminal.is_sequence);
        assert_eq!(terminal.items.len(), 3);
    }

    /// Records the ARGUMENTS each body's dispatch delivered, keyed by the
    /// body index — the observable the mid-run update test splits on.
    struct ArgRecordingRuntime {
        registry: Arc<FabricRegistry>,
        body_args: Arc<std::sync::Mutex<Vec<(usize, Vec<(String, Vec<u8>)>)>>>,
    }

    #[async_trait]
    impl EngineRuntime for ArgRecordingRuntime {
        async fn segment_switch(
            &self,
            _graph: &ResolvedGraph,
        ) -> Result<Arc<crate::bifaci::relay_switch::RelaySwitch>, ExecutionError> {
            panic!("arg recording runtime overrides run_segment")
        }

        async fn activity_timeout_secs(
            &self,
            _graph: &ResolvedGraph,
        ) -> Result<u64, ExecutionError> {
            panic!("arg recording runtime overrides run_segment")
        }

        fn fabric_registry(&self) -> Arc<FabricRegistry> {
            self.registry.clone()
        }

        async fn foreach_partial_failure_policy(&self) -> String {
            "fail".to_string()
        }

        async fn run_segment(
            &self,
            graph: &ResolvedGraph,
            _initial_inputs: HashMap<String, PlanInput>,
            _initial_is_sequence: HashMap<String, bool>,
            cap_arguments: &HashMap<String, Vec<(String, Vec<u8>)>>,
            _progress_fn: Option<&CapProgressFn>,
            _step_progress_fn: Option<&CapStepProgressFn>,
            _log_fn: Option<&PipelineLogFn>,
            body_coordinate: Option<ForEachBodyCoordinate>,
            _stall_tracker: Option<Arc<PipelineProgressTracker>>,
            _persist_sinks: &HashSet<String>,
        ) -> Result<SegmentOutput, ExecutionError> {
            if let Some(coordinate) = body_coordinate {
                self.body_args.lock().expect("body arg capture lock").push((
                    coordinate.body_index,
                    cap_arguments.get("mapper").cloned().unwrap_or_default(),
                ));
                let edge = graph.edges.first().expect("body graph has an edge");
                return Ok(SegmentOutput {
                    node_data: HashMap::from([(edge.to.clone(), vec![b"mapped".to_vec()])]),
                    node_is_sequence: HashMap::from([(edge.to.clone(), false)]),
                    writer_results: HashMap::new(),
                    terminal_meta: HashMap::new(),
                    node_spool: HashMap::new(),
                });
            }
            // The trunk of a direct live→region plan has NO caps: nothing to
            // run, nothing produced.
            Ok(SegmentOutput {
                node_data: HashMap::new(),
                node_is_sequence: HashMap::new(),
                writer_results: HashMap::new(),
                terminal_meta: HashMap::new(),
                node_spool: HashMap::new(),
            })
        }
    }

    // TEST1476: a mid-run argument update through the REAL executor. The
    // honesty contract under test is the SPLIT: with the engine reporting
    // `bodies_dispatched_before = k`, every body the executor dispatched with
    // index < k must have received the OLD value and every body >= k the NEW
    // one — the report and the deliveries can never disagree, whatever the
    // race timing. Items arrive over time (synthetic live feed) so the update
    // genuinely lands mid-run.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test1476_mid_run_update_splits_bodies_exactly_as_reported() {
        let mut plan = MachinePlan::new("mid-run-update");
        plan.add_node(MachineNode::input_slot(
            "input",
            "input",
            "media:feed-frames",
            crate::planner::InputCardinality::Sequence,
        ));
        plan.add_node(MachineNode::cap(
            "mapper",
            "cap:in=\"media:feed-frames\";map;out=\"media:fmt=json;record\"",
        ));
        plan.add_node(MachineNode::for_each_token(
            "fe",
            "input",
            "mapper",
            "mapper",
            "mid-run-update-token".parse().unwrap(),
        ));
        plan.add_node(MachineNode::output("out", "result", "mapper"));
        plan.add_edge(MachinePlanEdge::direct("input", "fe"));
        plan.add_edge(MachinePlanEdge::iteration("fe", "mapper"));
        plan.add_edge(MachinePlanEdge::direct("mapper", "out"));

        let registry = FabricRegistry::new_for_test();
        registry.add_caps_to_cache(vec![test_cap(
            "cap:in=\"media:feed-frames\";map;out=\"media:fmt=json;record\"",
            false,
        )]);
        let body_args: Arc<std::sync::Mutex<Vec<(usize, Vec<(String, Vec<u8>)>)>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let runtime: Arc<dyn EngineRuntime> = Arc::new(ArgRecordingRuntime {
            registry: Arc::new(registry),
            body_args: body_args.clone(),
        });

        const ARG_URN: &str = "media:enc=utf-8;question";
        let ledger = Arc::new(
            crate::orchestrator::run_arguments::RunArgumentLedger::new(
                &plan,
                HashMap::from([(
                    "mapper".to_string(),
                    vec![(ARG_URN.to_string(), b"old".to_vec())],
                )]),
            )
            .expect("initial values name the plan's step"),
        );

        // The updater: as soon as the first body has dispatched, apply the new
        // value — squarely mid-run, while the feed is still delivering.
        let update_ledger = ledger.clone();
        let observed = body_args.clone();
        let updater = tokio::spawn(async move {
            loop {
                if !observed.lock().expect("body arg capture lock").is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            update_ledger
                .apply(&[crate::orchestrator::run_arguments::ArgumentUpdate {
                    token_id: "mapper".to_string(),
                    media_urn: ARG_URN.to_string(),
                    value: b"new".to_vec(),
                }])
                .expect("the update batch is valid")
        });

        let inputs = HashMap::from([(
            "input".to_string(),
            PlanInput::LiveReference {
                reference_urn: "media:live;synthetic".to_string(),
                selector: br#"{"params":{"items":6,"interval_ms":120,"item_bytes":4}}"#.to_vec(),
            },
        )]);
        let flags = HashMap::from([("input".to_string(), true)]);
        execute_plan(
            &plan,
            runtime,
            inputs,
            flags,
            ledger.as_ref(),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("the run completes with the update applied mid-flight");

        let applied = updater.await.expect("updater task");
        assert_eq!(applied.outcomes.len(), 1);
        use crate::orchestrator::run_arguments::ArgumentUpdateDisposition;
        let reported_before = match &applied.outcomes[0].disposition {
            ArgumentUpdateDisposition::Applied => 0u64,
            ArgumentUpdateDisposition::AppliedToRemainingBodies { bodies_dispatched } => {
                *bodies_dispatched
            }
            ArgumentUpdateDisposition::AlreadyDispatched => panic!(
                "the update landed while the feed was still delivering — it cannot have \
                 missed every body"
            ),
        };

        let mut deliveries = body_args.lock().expect("body arg capture lock").clone();
        deliveries.sort_by_key(|(index, _)| *index);
        assert_eq!(deliveries.len(), 6, "one dispatch per feed item");
        for (index, args) in &deliveries {
            let value = args
                .iter()
                .find(|(urn, _)| urn == ARG_URN)
                .map(|(_, bytes)| bytes.clone())
                .expect("every body carries the argument");
            let expected: &[u8] = if (*index as u64) < reported_before { b"old" } else { b"new" };
            assert_eq!(
                value, expected,
                "body {index} must match the engine's report (bodies_dispatched_before = {reported_before})"
            );
        }
        // The update demonstrably reached remaining work: at least one body ran
        // with the new value, and the first body (which triggered the update)
        // ran with the old one.
        assert!(
            deliveries.iter().any(|(_, args)| args
                .iter()
                .any(|(urn, bytes)| urn == ARG_URN && bytes == b"new")),
            "no body ever received the new value — the executor is not reading the ledger"
        );
        assert_eq!(
            deliveries[0].1.iter().find(|(urn, _)| urn == ARG_URN).map(|(_, b)| b.as_slice()),
            Some(&b"old"[..]),
            "the first body dispatched before the update and must keep the old value"
        );
    }

    /// Trunk returns its region-input node SPOOLED (the unbounded-feed
    /// shape); body calls are recorded so per-item dispatch is provable.
    struct SpooledTrunkRuntime {
        registry: Arc<FabricRegistry>,
        spool_path: std::path::PathBuf,
        body_inputs: Arc<std::sync::Mutex<Vec<HashMap<String, PlanInput>>>>,
        /// TEST1460: reproduce the executor contract breach that once ran a
        /// whole capture as zero bodies — an (empty) in-memory entry recorded
        /// ALONGSIDE the spool for the same sink.
        duplicate_node_data: bool,
    }

    #[async_trait]
    impl EngineRuntime for SpooledTrunkRuntime {
        async fn segment_switch(
            &self,
            _graph: &ResolvedGraph,
        ) -> Result<Arc<crate::bifaci::relay_switch::RelaySwitch>, ExecutionError> {
            panic!("spooled trunk runtime overrides run_segment")
        }

        async fn activity_timeout_secs(
            &self,
            _graph: &ResolvedGraph,
        ) -> Result<u64, ExecutionError> {
            panic!("spooled trunk runtime overrides run_segment")
        }

        fn fabric_registry(&self) -> Arc<FabricRegistry> {
            self.registry.clone()
        }

        async fn foreach_partial_failure_policy(&self) -> String {
            "fail".to_string()
        }

        async fn run_segment(
            &self,
            graph: &ResolvedGraph,
            initial_inputs: HashMap<String, PlanInput>,
            _initial_is_sequence: HashMap<String, bool>,
            _cap_arguments: &HashMap<String, Vec<(String, Vec<u8>)>>,
            _progress_fn: Option<&CapProgressFn>,
            _step_progress_fn: Option<&CapStepProgressFn>,
            _log_fn: Option<&PipelineLogFn>,
            body_coordinate: Option<ForEachBodyCoordinate>,
            _stall_tracker: Option<Arc<PipelineProgressTracker>>,
            _persist_sinks: &HashSet<String>,
        ) -> Result<SegmentOutput, ExecutionError> {
            let edge = graph.edges.first().expect("segment has an edge");
            if body_coordinate.is_some() {
                // A region body: record the item it was dispatched with and
                // produce a per-item result.
                self.body_inputs
                    .lock()
                    .expect("body input capture lock")
                    .push(initial_inputs);
                return Ok(SegmentOutput {
                    node_data: HashMap::from([(edge.to.clone(), vec![b"upscaled".to_vec()])]),
                    node_is_sequence: HashMap::from([(edge.to.clone(), false)]),
                    writer_results: HashMap::new(),
                    terminal_meta: HashMap::new(),
                    node_spool: HashMap::new(),
                });
            }
            // The trunk: its sink was an UNBOUNDED stream that ended — the
            // executor spooled it. Write the spool (3 CBOR Bytes items) and
            // return it WITHOUT in-memory node_data, exactly as
            // run_dag_on_context does for an engaged spool.
            let mut spool_bytes = Vec::new();
            for i in 0..3u8 {
                ciborium::ser::into_writer(
                    &ciborium::Value::Bytes(format!("frame-{i}").into_bytes()),
                    &mut spool_bytes,
                )
                .expect("encode spool item");
            }
            std::fs::write(&self.spool_path, &spool_bytes).expect("write spool file");
            let node_data = if self.duplicate_node_data {
                HashMap::from([(edge.to.clone(), Vec::new())])
            } else {
                HashMap::new()
            };
            Ok(SegmentOutput {
                node_data,
                node_is_sequence: HashMap::from([(edge.to.clone(), true)]),
                writer_results: HashMap::new(),
                terminal_meta: HashMap::new(),
                node_spool: HashMap::from([(edge.to.clone(), self.spool_path.clone())]),
            })
        }
    }

    // TEST1456: an UNBOUNDED trunk stream (a live capture's content) that
    // ended and was SPOOLED drives a ForEach region — per-item dispatch
    // streams items from the spool file: every item gets its own body with
    // the right bytes, the snapshots carry real per-item facts, the region
    // output assembles in item order, and the spool file is removed when the
    // plan completes. This is the `cam → bridge → foreach(filter)` machine
    // shape; refusing it was scaffolding, not design.
    #[tokio::test]
    async fn test1456_spooled_unbounded_input_drives_foreach_region() {
        let mut plan = MachinePlan::new("spooled-foreach");
        plan.add_node(MachineNode::input_slot(
            "input",
            "input",
            "media:ext=pdf",
            crate::planner::InputCardinality::Single,
        ));
        plan.add_node(MachineNode::cap(
            "bridge",
            "cap:frames;in=\"media:ext=pdf\";out=\"media:ext=png;image\"",
        ));
        plan.add_node(MachineNode::cap(
            "mapper",
            "cap:in=\"media:ext=png;image\";upscale;out=\"media:ext=png;image;up\"",
        ));
        plan.add_node(MachineNode::for_each_token(
            "fe",
            "bridge",
            "mapper",
            "mapper",
            "spooled-foreach-token".parse().unwrap(),
        ));
        plan.add_node(MachineNode::output("out", "result", "mapper"));
        plan.add_edge(MachinePlanEdge::direct("input", "bridge"));
        plan.add_edge(MachinePlanEdge::direct("bridge", "fe"));
        plan.add_edge(MachinePlanEdge::iteration("fe", "mapper"));
        plan.add_edge(MachinePlanEdge::direct("mapper", "out"));

        let registry = FabricRegistry::new_for_test();
        registry.add_caps_to_cache(vec![
            test_cap("cap:frames;in=\"media:ext=pdf\";out=\"media:ext=png;image\"", true),
            test_cap(
                "cap:in=\"media:ext=png;image\";upscale;out=\"media:ext=png;image;up\"",
                false,
            ),
        ]);
        let registry = Arc::new(registry);

        let dir = tempfile::tempdir().unwrap();
        let spool_path = dir.path().join("bridge.spool");
        let body_inputs: Arc<std::sync::Mutex<Vec<HashMap<String, PlanInput>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let runtime: Arc<dyn EngineRuntime> = Arc::new(SpooledTrunkRuntime {
            registry,
            spool_path: spool_path.clone(),
            body_inputs: body_inputs.clone(),
            duplicate_node_data: false,
        });

        let snapshots: Arc<std::sync::Mutex<Vec<ForEachItemSnapshot>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let snapshots_sink = snapshots.clone();
        let items_fn: ForEachItemsFn = Arc::new(move |_token, items| {
            snapshots_sink
                .lock()
                .expect("snapshot lock")
                .extend(items.iter().cloned());
            Ok(())
        });

        let inputs = HashMap::from([("input".to_string(), PlanInput::Bytes(b"doc".to_vec()))]);
        let flags = HashMap::from([("input".to_string(), false)]);
        let result = execute_plan(
            &plan,
            runtime,
            inputs,
            flags,
            &crate::orchestrator::run_arguments::RunArgumentLedger::new(&plan, HashMap::new())
                .expect("empty ledger"),
            None,
            None,
            None,
            None,
            None,
            Some(&items_fn),
        )
        .await
        .expect("a spooled unbounded region input executes");

        // Every spooled item got its own body, with the decoded raw bytes.
        let seen = body_inputs.lock().expect("captured body inputs");
        assert_eq!(seen.len(), 3, "one body per spooled item");
        let mut got: Vec<Vec<u8>> = seen
            .iter()
            .map(|m| {
                let (_, input) = m.iter().next().expect("one body root");
                match input {
                    PlanInput::Bytes(b) => b.clone(),
                    other => panic!("body root is raw bytes, got {other:?}"),
                }
            })
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![b"frame-0".to_vec(), b"frame-1".to_vec(), b"frame-2".to_vec()]
        );

        // Snapshots carried real per-item facts from the indexing pass.
        let snaps = snapshots.lock().expect("snapshots");
        assert_eq!(snaps.len(), 3);
        assert!(snaps.iter().all(|s| s.item_byte_count == 7));

        // The region output assembled as a sequence, one item per body.
        let terminal = result.terminal("out").expect("region terminal");
        assert!(terminal.is_sequence);
        assert_eq!(terminal.items.len(), 3);

        // The plan-level cleanup removed the spool file.
        assert!(
            !spool_path.exists(),
            "spool files are removed when the plan completes"
        );
    }

    // TEST1460: a segment recording the SAME sink both in memory (empty) and
    // as a spool file is refused loudly. This is the exact executor-contract
    // breach behind the silent zero-output webcam run: the region driver
    // preferred the phantom empty memory entry over 682 spooled frames.
    #[tokio::test]
    async fn test1460_region_input_recorded_twice_is_refused() {
        let mut plan = MachinePlan::new("spooled-foreach-conflict");
        plan.add_node(MachineNode::input_slot(
            "input",
            "input",
            "media:ext=pdf",
            crate::planner::InputCardinality::Single,
        ));
        plan.add_node(MachineNode::cap(
            "bridge",
            "cap:frames;in=\"media:ext=pdf\";out=\"media:ext=png;image\"",
        ));
        plan.add_node(MachineNode::cap(
            "mapper",
            "cap:in=\"media:ext=png;image\";upscale;out=\"media:ext=png;image;up\"",
        ));
        plan.add_node(MachineNode::for_each_token(
            "fe",
            "bridge",
            "mapper",
            "mapper",
            "spooled-foreach-conflict-token".parse().unwrap(),
        ));
        plan.add_node(MachineNode::output("out", "result", "mapper"));
        plan.add_edge(MachinePlanEdge::direct("input", "bridge"));
        plan.add_edge(MachinePlanEdge::direct("bridge", "fe"));
        plan.add_edge(MachinePlanEdge::iteration("fe", "mapper"));
        plan.add_edge(MachinePlanEdge::direct("mapper", "out"));

        let registry = FabricRegistry::new_for_test();
        registry.add_caps_to_cache(vec![
            test_cap("cap:frames;in=\"media:ext=pdf\";out=\"media:ext=png;image\"", true),
            test_cap(
                "cap:in=\"media:ext=png;image\";upscale;out=\"media:ext=png;image;up\"",
                false,
            ),
        ]);

        let dir = tempfile::tempdir().unwrap();
        let runtime: Arc<dyn EngineRuntime> = Arc::new(SpooledTrunkRuntime {
            registry: Arc::new(registry),
            spool_path: dir.path().join("bridge.spool"),
            body_inputs: Arc::new(std::sync::Mutex::new(Vec::new())),
            duplicate_node_data: true,
        });

        let inputs = HashMap::from([("input".to_string(), PlanInput::Bytes(b"doc".to_vec()))]);
        let flags = HashMap::from([("input".to_string(), false)]);
        let error = execute_plan(
            &plan,
            runtime,
            inputs,
            flags,
            &crate::orchestrator::run_arguments::RunArgumentLedger::new(&plan, HashMap::new())
                .expect("empty ledger"),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect_err("a doubly-recorded region input must never run silently");
        assert!(
            error.to_string().contains("recorded BOTH in memory"),
            "the refusal names the contract breach, got: {error}"
        );
    }

    /// TEST1456's runtime with transient capture switched ON.
    struct TransientTrunkRuntime {
        inner: SpooledTrunkRuntime,
        transient_root: std::path::PathBuf,
    }

    #[async_trait]
    impl EngineRuntime for TransientTrunkRuntime {
        async fn segment_switch(
            &self,
            graph: &ResolvedGraph,
        ) -> Result<Arc<crate::bifaci::relay_switch::RelaySwitch>, ExecutionError> {
            self.inner.segment_switch(graph).await
        }

        async fn activity_timeout_secs(
            &self,
            graph: &ResolvedGraph,
        ) -> Result<u64, ExecutionError> {
            self.inner.activity_timeout_secs(graph).await
        }

        fn fabric_registry(&self) -> Arc<FabricRegistry> {
            self.inner.fabric_registry()
        }

        async fn foreach_partial_failure_policy(&self) -> String {
            self.inner.foreach_partial_failure_policy().await
        }

        fn transient_artifact_root(&self) -> Option<std::path::PathBuf> {
            Some(self.transient_root.clone())
        }

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
        ) -> Result<SegmentOutput, ExecutionError> {
            self.inner
                .run_segment(
                    graph,
                    initial_inputs,
                    initial_is_sequence,
                    cap_arguments,
                    progress_fn,
                    step_progress_fn,
                    log_fn,
                    body_coordinate,
                    stall_tracker,
                    persist_sinks,
                )
                .await
        }
    }

    // TEST1459: with transient capture ON, a spooled intermediate is
    // reaper-owned — the plan-level cleanup that TEST1456 proves for the
    // capture-off runtime must NOT delete it, or the inspection surface would
    // vanish the moment the run completes. Same plan, same spool, opposite
    // ownership.
    #[tokio::test]
    async fn test1459_transient_capture_owns_spooled_intermediates() {
        let mut plan = MachinePlan::new("spooled-foreach-transient");
        plan.add_node(MachineNode::input_slot(
            "input",
            "input",
            "media:ext=pdf",
            crate::planner::InputCardinality::Single,
        ));
        plan.add_node(MachineNode::cap(
            "bridge",
            "cap:frames;in=\"media:ext=pdf\";out=\"media:ext=png;image\"",
        ));
        plan.add_node(MachineNode::cap(
            "mapper",
            "cap:in=\"media:ext=png;image\";upscale;out=\"media:ext=png;image;up\"",
        ));
        plan.add_node(MachineNode::for_each_token(
            "fe",
            "bridge",
            "mapper",
            "mapper",
            "spooled-foreach-transient-token".parse().unwrap(),
        ));
        plan.add_node(MachineNode::output("out", "result", "mapper"));
        plan.add_edge(MachinePlanEdge::direct("input", "bridge"));
        plan.add_edge(MachinePlanEdge::direct("bridge", "fe"));
        plan.add_edge(MachinePlanEdge::iteration("fe", "mapper"));
        plan.add_edge(MachinePlanEdge::direct("mapper", "out"));

        let registry = FabricRegistry::new_for_test();
        registry.add_caps_to_cache(vec![
            test_cap("cap:frames;in=\"media:ext=pdf\";out=\"media:ext=png;image\"", true),
            test_cap(
                "cap:in=\"media:ext=png;image\";upscale;out=\"media:ext=png;image;up\"",
                false,
            ),
        ]);
        let registry = Arc::new(registry);

        let dir = tempfile::tempdir().unwrap();
        let spool_path = dir.path().join("bridge.spool");
        let transient_root = dir.path().join("transient");
        let runtime: Arc<dyn EngineRuntime> = Arc::new(TransientTrunkRuntime {
            inner: SpooledTrunkRuntime {
                registry,
                spool_path: spool_path.clone(),
                body_inputs: Arc::new(std::sync::Mutex::new(Vec::new())),
                duplicate_node_data: false,
            },
            transient_root,
        });

        let inputs = HashMap::from([("input".to_string(), PlanInput::Bytes(b"doc".to_vec()))]);
        let flags = HashMap::from([("input".to_string(), false)]);
        let result = execute_plan(
            &plan,
            runtime,
            inputs,
            flags,
            &crate::orchestrator::run_arguments::RunArgumentLedger::new(&plan, HashMap::new())
                .expect("empty ledger"),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("the transient-capturing runtime executes the same plan");

        let terminal = result.terminal("out").expect("region terminal");
        assert_eq!(terminal.items.len(), 3);

        // The opposite of TEST1456's final assertion: the spooled
        // intermediate SURVIVES plan completion — the TTL reaper owns it now.
        assert!(
            spool_path.exists(),
            "with transient capture on, spooled intermediates are reaper-owned, \
             never plan-deleted"
        );
    }

    /// Structural fixture: input → mapper (inside a ForEach region) → fold →
    /// sink, plus an independent pre-trunk cap off the same input.
    fn plan_with_region_and_fold() -> (MachinePlan, HashSet<String>) {
        let mut plan = MachinePlan::new("region+fold");
        plan.add_node(MachineNode::input_slot(
            "input",
            "input",
            "media:ext=pdf",
            crate::planner::InputCardinality::Single,
        ));
        plan.add_node(MachineNode::cap(
            "render",
            "cap:in=\"media:ext=pdf\";render;out=\"media:ext=png;image\"",
        ));
        plan.add_node(MachineNode::cap(
            "mapper",
            "cap:in=\"media:ext=png;image\";upscale;out=\"media:ext=png;image;up\"",
        ));
        plan.add_node(MachineNode::for_each_token(
            "fe",
            "render",
            "mapper",
            "mapper",
            "stable-foreach-token".parse().unwrap(),
        ));
        plan.add_node(MachineNode::cap(
            "fold",
            "cap:in=\"media:ext=png;image\";animate;out=\"media:ext=gif;image\"",
        ));
        plan.add_node(MachineNode::cap(
            "meta",
            "cap:in=\"media:ext=pdf\";meta;out=\"media:enc=utf-8;record\"",
        ));
        plan.add_node(MachineNode::output("out_gif", "result", "fold"));
        plan.add_node(MachineNode::output("out_meta", "result", "meta"));

        plan.add_edge(MachinePlanEdge::direct("input", "render"));
        plan.add_edge(MachinePlanEdge::direct("render", "fe"));
        plan.add_edge(MachinePlanEdge::iteration("fe", "mapper"));
        plan.add_edge(MachinePlanEdge::direct("mapper", "fold"));
        plan.add_edge(MachinePlanEdge::direct("input", "meta"));
        plan.add_edge(MachinePlanEdge::direct("fold", "out_gif"));
        plan.add_edge(MachinePlanEdge::direct("meta", "out_meta"));

        let region_nodes: HashSet<String> = ["mapper".to_string()].into_iter().collect();
        (plan, region_nodes)
    }

    // TEST1434: the post-region partition is exactly the transitive consumers
    // of region output — the fold, NOT the independent pre-trunk cap. The
    // pre-trunk subplan excludes both region and post caps; the post subplan
    // carries the fold with its external-producer edge intact.
    #[test]
    fn test1434_post_region_partition() {
        let (plan, region_nodes) = plan_with_region_and_fold();

        let post = compute_post_region_caps(&plan, &region_nodes);
        assert!(
            post.contains("fold"),
            "the fold consumes region output → post"
        );
        assert!(!post.contains("meta"), "an independent cap stays pre-trunk");
        assert!(!post.contains("mapper"), "region caps are never post");
        assert_eq!(post.len(), 1);

        let mut excluded = region_nodes.clone();
        excluded.extend(post.iter().cloned());
        let trunk = build_trunk_subplan(&plan, &excluded);
        assert!(trunk.nodes.contains_key("render"));
        assert!(trunk.nodes.contains_key("meta"));
        assert!(trunk.nodes.contains_key("input"));
        assert!(!trunk.nodes.contains_key("mapper"));
        assert!(!trunk.nodes.contains_key("fold"));

        let post_plan = build_post_subplan(&plan, &post);
        assert!(post_plan.nodes.contains_key("fold"));
        assert_eq!(post_plan.nodes.len(), 1);
        // The external-producer edge (mapper → fold) survives so the segment
        // can seed 'mapper' as a materialised root.
        assert!(
            post_plan
                .edges
                .iter()
                .any(|e| e.from_node == "mapper" && e.to_node == "fold"),
            "the post subplan keeps the edge from its external root"
        );
    }

    // TEST7119: the actual ForEach body-progress path emits the immutable strand
    // token consumed by the rendered graph, never its structural node id.
    #[tokio::test]
    async fn test7119_foreach_progress_emits_the_stable_strand_token() {
        let (plan, _) = plan_with_region_and_fold();
        let registry = FabricRegistry::new_for_test();
        registry.add_caps_to_cache(vec![
            test_cap(
                "cap:in=\"media:ext=pdf\";render;out=\"media:ext=png;image\"",
                true,
            ),
            test_cap(
                "cap:in=\"media:ext=png;image\";upscale;out=\"media:ext=png;image;up\"",
                false,
            ),
            test_cap(
                "cap:in=\"media:ext=png;image\";animate;out=\"media:ext=gif;image\"",
                false,
            ),
            test_cap(
                "cap:in=\"media:ext=pdf\";meta;out=\"media:enc=utf-8;record\"",
                false,
            ),
        ]);
        let registry = Arc::new(registry);
        let regions = compute_regions(&plan, &registry)
            .await
            .expect("derive ForEach region");
        let region = regions
            .iter()
            .find(|region| region.fe_id == "fe")
            .expect("fixture region");
        let body_plan = build_body_subplan(&plan, region);
        let runtime: Arc<dyn EngineRuntime> = Arc::new(RecordingRuntime {
            registry: registry.clone(),
        });
        let reported_tokens = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let token_sink = reported_tokens.clone();
        let step_progress: CapStepProgressFn = Arc::new(move |_step, _cap_urn, token_id| {
            token_sink
                .lock()
                .expect("token sink lock")
                .push(token_id.to_string());
        });
        let overall_progress: CapProgressFn = Arc::new(|_, _, _| {});
        let mut body_outcomes = Vec::new();

        run_region_bodies(
            &runtime,
            &registry,
            &body_plan,
            &HashSet::new(),
            region,
            region_source_from_memory(vec![b"item".to_vec()]),
            &crate::orchestrator::run_arguments::RunArgumentLedger::new(&body_plan, HashMap::new())
                .expect("empty ledger"),
            Some(&overall_progress),
            Some(&step_progress),
            None,
            None,
            None,
            None,
            0.0,
            1.0,
            &mut body_outcomes,
        )
        .await
        .expect("execute ForEach body");

        let tokens = reported_tokens.lock().expect("reported token lock");
        assert!(
            !tokens.is_empty(),
            "ForEach aggregate progress must be emitted"
        );
        assert!(tokens.iter().all(|token| token == "stable-foreach-token"));
        assert!(tokens.iter().all(|token| token != "fe"));
    }
}
