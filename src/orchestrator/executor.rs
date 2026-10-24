//! DAG Execution Engine
//!
//! Executes a resolved DOT DAG by:
//! 1. Discovering and downloading cartridges that provide the required caps
//! 2. Connecting all cartridges to a single CartridgeHostRuntime
//! 3. Routing cap requests through a RelaySwitch
//! 4. Executing edge groups in topological order, streaming frames between caps
//!
//! Fan-in: multiple edges pointing to the same `(to, cap_urn)` are grouped and
//! executed as ONE cap invocation with multiple input streams. The cartridge handler
//! receives all streams and decides how to handle partial availability — it may
//! wait for all, use whatever arrives, or fail.
//!
//! Architecture:
//! ```text
//!   capdag CLI ←→ RelaySwitch ←→ RelaySlave ←→ CartridgeHostRuntime ←→ Cartridge A
//!                                                             ←→ Cartridge B
//!                                                             ←→ Cartridge C
//! ```

use super::stream_io::{PipelineLogFn, PipelineLogRecord, PipelineProgressTracker, TerminalMeta};
use super::types::{ResolvedGraph, ResolvedStep, StepSource};
use crate::{
    handshake, CapManifest, CapUrn, CartridgeHostRuntime, CartridgeRepo, FabricRegistry,
    Frame, FrameReader, FrameType, FrameWriter, Limits, MessageId, RelayNotifyCapabilitiesPayload,
    RelaySlave, RelaySwitch, DEFAULT_MAX_CHUNK,
};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

/// Detect the current platform in the format used by the registry (e.g., "darwin-arm64").
/// Thin alias over the shared [`crate::host_platform`] — the single source of
/// truth for the host {os}-{arch} string.
fn detect_platform() -> String {
    crate::host_platform()
}

fn platform_binary_name(cartridge_id: &str) -> String {
    if cfg!(target_os = "windows") {
        format!("{cartridge_id}.exe")
    } else {
        cartridge_id.to_string()
    }
}

/// Default cap-level activity timeout in seconds.
/// If a cartridge sends no frames (Chunk, Log, progress, peer requests) for this
/// duration, the executor aborts with `ExecutionError::ActivityTimeout`.
const DEFAULT_ACTIVITY_TIMEOUT_SECS: u64 = 120;

/// Cap metadata key for per-cap activity timeout override.
const ACTIVITY_TIMEOUT_METADATA_KEY: &str = "activity_timeout_secs";

/// How long the segment executor polls for a cap to become dispatchable before
/// failing hard. Cap registration is asynchronous — a freshly attached cartridge
/// host's RelayNotify (carrying its real cap_groups) lands some time after
/// `add_master` returns. On a long-lived switch (the engine) the cap is already
/// registered and the first probe succeeds, so this bound only ever elapses for a
/// genuinely unprovided cap.
const CAP_DISPATCH_READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
use crate::bifaci::local_socket::UnixStream;
use crate::planner::StepToken;
use tokio::io::{BufReader, BufWriter};

/// Callback for reporting per-cap progress.
/// Parameters: (progress 0.0–1.0, cap URN string, human-readable message)
pub type CapProgressFn = Arc<dyn Fn(f32, &str, &str) + Send + Sync>;

/// Side-channel callback reporting a single cap's OWN completion fraction (the
/// mapper's un-mapped child value, before it is folded into the overall progress).
/// Parameters: (step_progress 0.0–1.0, cap URN string, step token_id). The token_id
/// is the stable per-step identity (`StrandStep.token_id`) of the reporting cap —
/// the unambiguous key a consumer uses to attribute this fraction to the exact
/// strand step (a cap URN alone is ambiguous when the same URN repeats). Attached
/// only to the per-cap group mappers so a consumer can persist each section's own
/// progress distinct from the whole-strand `overall` carried by `CapProgressFn`.
pub type CapStepProgressFn = Arc<dyn Fn(f32, &str, &str) + Send + Sync>;

/// Maps child progress [0.0, 1.0] into a parent range [base, base + weight].
///
/// This is the single progress mapping computation used everywhere:
/// - DAG execution: per-group subdivision
/// - ForEach plans: per-item subdivision
/// - Peer calls: caller's progress range delegation
/// - LLM cartridge client: frame-to-callback mapping
///
/// All child progress values are clamped to [0.0, 1.0] before mapping.
/// The mapped result is `base + child_progress.clamp(0.0, 1.0) * weight`.

/// Map child progress [0.0, 1.0] into parent range [base, base + weight].
///
/// This is the canonical progress mapping formula. Every place in the system
/// that subdivides progress must use this function — no ad-hoc derivations.
#[inline]
pub fn map_progress(child_progress: f32, base: f32, weight: f32) -> f32 {
    base + child_progress.clamp(0.0, 1.0) * weight
}

/// Wraps a `CapProgressFn` with a progress range subdivision.
#[derive(Clone)]
pub struct ProgressMapper {
    base: f32,
    weight: f32,
    parent: CapProgressFn,
    /// When set, every `report` also emits the raw (un-mapped) child value — this
    /// cap's OWN completion fraction — so a per-step consumer sees each section's
    /// progress, paired with the reporting cap's stable per-step identity so the
    /// consumer can attribute the fraction unambiguously. Attached only to the
    /// per-cap group mappers; sub-mappers created via `sub_mapper` deliberately do
    /// NOT inherit it (their child is an intra-cap phase fraction, not the cap's own
    /// progress).
    ///
    /// Sink and token live in ONE option because neither is meaningful alone: a sink
    /// with no step to attribute to has nothing to say, and a token with no sink has
    /// nowhere to say it.
    step_sink: Option<(CapStepProgressFn, StepToken)>,
}

impl ProgressMapper {
    /// Create a mapper that maps child [0.0, 1.0] into parent [base, base + weight].
    pub fn new(parent: &CapProgressFn, base: f32, weight: f32) -> Self {
        Self {
            base,
            weight,
            parent: Arc::clone(parent),
            step_sink: None,
        }
    }

    /// Attach a per-step sink that receives this mapper's raw child value (the cap's
    /// own completion fraction) on every `report`, tagged with `token_id` — the
    /// reporting cap step's stable identity.
    pub fn with_step_sink(mut self, sink: &CapStepProgressFn, token_id: &StepToken) -> Self {
        self.step_sink = Some((Arc::clone(sink), token_id.clone()));
        self
    }

    /// Report child progress. The value is clamped to [0.0, 1.0] and mapped.
    pub fn report(&self, child_progress: f32, cap_urn: &str, msg: &str) {
        let clamped = child_progress.clamp(0.0, 1.0);
        // Emit the cap's own fraction BEFORE the overall so a consumer that reads a
        // shared "latest step" cell inside the parent callback sees the fresh value.
        if let Some((sink, token_id)) = &self.step_sink {
            sink(clamped, cap_urn, token_id);
        }
        let overall = map_progress(clamped, self.base, self.weight);
        (self.parent)(overall, cap_urn, msg);
    }

    /// Convert into a `CapProgressFn` for passing to APIs that expect one.
    pub fn as_cap_progress_fn(&self) -> CapProgressFn {
        let mapper = self.clone();
        Arc::new(move |p: f32, cap_urn: &str, msg: &str| {
            mapper.report(p, cap_urn, msg);
        })
    }

    /// Create a sub-mapper that maps a child range within this mapper's range.
    ///
    /// Example: if this mapper maps to [0.2, 0.8] (base=0.2, weight=0.6),
    /// and you create a sub-mapper with sub_base=0.5, sub_weight=0.5,
    /// the sub-mapper maps to [0.5, 0.8] in the parent's coordinate space.
    ///
    /// The sub-mapper does NOT inherit `step_sink`: it flattens onto this mapper's
    /// `parent` (the grandparent), and its child is an intra-cap phase fraction, not
    /// the cap's own progress. The live per-step value is sourced from the per-cap
    /// group mappers' direct reports, which the machine streaming path drives; this
    /// intra-cap subdivision is used by non-streaming phases only.
    pub fn sub_mapper(&self, sub_base: f32, sub_weight: f32) -> Self {
        Self {
            base: self.base + sub_base * self.weight,
            weight: sub_weight * self.weight,
            parent: Arc::clone(&self.parent),
            step_sink: None,
        }
    }
}


// =============================================================================
// Error Types
// =============================================================================

#[derive(Debug, Error)]
pub enum ExecutionError {
    /// An execution failure attributed to the exact immutable strand step.
    #[error("Step {step_token_id} failed: {source}")]
    StepFailed {
        step_token_id: StepToken,
        #[source]
        source: Box<ExecutionError>,
    },

    #[error("Cartridge not found for cap: {cap_urn}")]
    CartridgeNotFound { cap_urn: String },

    #[error("Cartridge download failed: {0}")]
    CartridgeDownloadFailed(String),

    /// A cap invocation failed. `code`/`class` carry the failure identity
    /// DECLARED at the emit source (docs/failure-taxonomy.md) — read from the
    /// ERR frame, never re-derived from message text. Engine-detected
    /// failures carry `None` + `Internal`. `arg_urn` is the emit source's
    /// argument attribution (the media URN of the ONE argument the failure
    /// is about), threaded from the ERR frame; `None` when the source did
    /// not attribute — never inferred here.
    #[error("Cartridge execution failed for cap {cap_urn}: {details}")]
    CartridgeExecutionFailed {
        cap_urn: String,
        code: Option<String>,
        class: crate::failure::AttributionClass,
        details: String,
        arg_urn: Option<String>,
    },

    /// A cap emitted an output STREAM_START whose media URN violates its
    /// declared effect contract (`CapUrn::is_conformant_runtime_output`).
    /// The plan's downstream type refinement was built on that promise, so
    /// the violation fails hard at receipt — before the stream is forwarded
    /// or collected — attributed Internal: the cartridge broke its own
    /// declared contract, not a user input problem.
    #[error(
        "Cap '{cap_urn}' violated its effect contract: effect={effect}, runtime input '{runtime_input}', expected output '{expected}', emitted '{actual}'"
    )]
    EffectContractViolation {
        cap_urn: String,
        effect: String,
        runtime_input: String,
        expected: String,
        actual: String,
    },

    /// A cap emitted an output STREAM_START whose shape violates its declared
    /// stream contract (15.2 §Streaming Contracts): an UNBOUNDED stream from
    /// an output declared `streaming: false`, or a cardinality mode other
    /// than the declared `is_sequence`. The executor's hop rule and every
    /// whole-value consumer downstream were built on that declaration, so
    /// the violation fails hard at receipt — attributed Internal: the
    /// cartridge broke its own declaration, not a user input problem.
    #[error(
        "Cap '{cap_urn}' violated its stream contract: declared is_sequence={declared_is_sequence} streaming={declared_streaming}, emitted is_sequence={emitted_is_sequence} unbounded={emitted_unbounded}"
    )]
    StreamContractViolation {
        cap_urn: String,
        declared_is_sequence: bool,
        declared_streaming: bool,
        emitted_is_sequence: bool,
        emitted_unbounded: bool,
    },

    /// A cap's response broke what its definition declares of its outputs
    /// (13.2 §Outputs): a stream for a main output the cap does not have, a
    /// stream naming an undeclared output or the main one, a second stream
    /// for one output, an output carrying a type it does not
    /// declare, or a request that ended without a stream for a required
    /// output. Fails hard at receipt — attributed Internal: the cartridge
    /// broke its own declaration, not a user input problem.
    #[error("Cap '{cap_urn}' violated its output contract: {detail}")]
    OutputContractViolation { cap_urn: String, detail: String },

    #[error("Node {node} has no incoming data")]
    NoIncomingData { node: String },

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Host error: {0}")]
    HostError(String),

    /// The cartridge that would serve this step is not available — it left its
    /// host's inventory and did not return within the admission grace window.
    /// Its own class because the deployment changed under a valid request; the
    /// engine did nothing wrong, so this is not `HostError`/`internal`.
    #[error("Cartridge unavailable for cap '{cap_urn}': {details}")]
    CartridgeUnavailable { cap_urn: String, details: String },

    #[error("Registry error: {0}")]
    FabricRegistryError(String),

    /// The sequences a map zips are of different lengths (15.2 §Maps): run
    /// `i` of the map is given item `i` of each, so there is no run to give
    /// the items one has and another lacks. Each source is named with how
    /// many items it gave. The run's data is what is wrong: `input`.
    #[error("{}", zip_length_mismatch_message(foreach_token_id, sources))]
    ZipLengthMismatch {
        foreach_token_id: StepToken,
        /// `(source node, items it gave)`, in the map's source order.
        sources: Vec<(String, usize)>,
    },

    /// This step did not fail: it was stopped because another did. Carries
    /// the class of the failure it is the collateral of, and what failed.
    #[error("stopped as collateral: {detail}")]
    Collateral {
        class: crate::failure::AttributionClass,
        detail: String,
    },
}

fn zip_length_mismatch_message(foreach_token_id: &StepToken, sources: &[(String, usize)]) -> String {
    let lengths: Vec<String> = sources
        .iter()
        .map(|(node, items)| format!("'{node}' gave {items}"))
        .collect();
    format!(
        "map {foreach_token_id} zips sequences of different lengths: {}",
        lengths.join(", ")
    )
}

impl ExecutionError {
    /// The failure class this error DECLARES (docs/failure-taxonomy.md).
    /// `CartridgeExecutionFailed` carries the class the emit source declared;
    /// every other variant declares its class here, at its definition:
    /// missing/undownloadable cartridges and registry failures are deployment
    /// problems (Environment); everything else is ours (Internal).
    pub fn attribution_class(&self) -> crate::failure::AttributionClass {
        use crate::failure::AttributionClass;
        match self {
            ExecutionError::StepFailed { source, .. } => source.attribution_class(),
            ExecutionError::CartridgeExecutionFailed { class, .. } => *class,
            ExecutionError::Collateral { class, .. } => *class,
            ExecutionError::ZipLengthMismatch { .. } => AttributionClass::Input,
            ExecutionError::CartridgeNotFound { .. }
            | ExecutionError::CartridgeDownloadFailed(_)
            | ExecutionError::CartridgeUnavailable { .. }
            | ExecutionError::FabricRegistryError(_) => AttributionClass::Environment,
            ExecutionError::NoIncomingData { .. }
            | ExecutionError::IoError(_)
            | ExecutionError::HostError(_)
            | ExecutionError::EffectContractViolation { .. }
            | ExecutionError::StreamContractViolation { .. }
            | ExecutionError::OutputContractViolation { .. } => AttributionClass::Internal,
        }
    }

    /// The machine-readable code declared at the emit source, when carried.
    pub fn failure_code(&self) -> Option<&str> {
        match self {
            ExecutionError::StepFailed { source, .. } => source.failure_code(),
            ExecutionError::CartridgeExecutionFailed { code, .. } => code.as_deref(),
            _ => None,
        }
    }

    /// Media URN of the argument the failure is attributed to, when the
    /// emit source declared one on the ERR frame. Every other variant is
    /// not about one argument and returns `None` — never a guess.
    pub fn failure_arg_urn(&self) -> Option<&str> {
        match self {
            ExecutionError::StepFailed { source, .. } => source.failure_arg_urn(),
            ExecutionError::CartridgeExecutionFailed { arg_urn, .. } => arg_urn.as_deref(),
            _ => None,
        }
    }

    /// The LEAF human reason — the emit source's own message for cap
    /// failures, the Display chain otherwise.
    pub fn failure_reason(&self) -> String {
        match self {
            ExecutionError::StepFailed { source, .. } => source.failure_reason(),
            ExecutionError::CartridgeExecutionFailed { details, .. } => details.clone(),
            other => other.to_string(),
        }
    }

    pub fn step_token_id(&self) -> Option<&StepToken> {
        match self {
            ExecutionError::StepFailed { step_token_id, .. } => Some(step_token_id),
            _ => None,
        }
    }

    pub fn failure_cap_urn(&self) -> Option<&str> {
        match self {
            ExecutionError::StepFailed { source, .. } => source.failure_cap_urn(),
            ExecutionError::CartridgeExecutionFailed { cap_urn, .. } => Some(cap_urn),
            ExecutionError::EffectContractViolation { cap_urn, .. } => Some(cap_urn),
            ExecutionError::StreamContractViolation { cap_urn, .. } => Some(cap_urn),
            ExecutionError::OutputContractViolation { cap_urn, .. } => Some(cap_urn),
            _ => None,
        }
    }

    /// Whether this is not a failure of its own but the end of something
    /// stopped because another step failed.
    pub fn is_collateral(&self) -> bool {
        match self {
            ExecutionError::StepFailed { source, .. } => source.is_collateral(),
            ExecutionError::Collateral { .. } => true,
            _ => false,
        }
    }

    pub(crate) fn at_step(self, step_token_id: &StepToken) -> Self {
        if matches!(self, ExecutionError::StepFailed { .. }) {
            self
        } else {
            ExecutionError::StepFailed {
                step_token_id: step_token_id.clone(),
                source: Box::new(self),
            }
        }
    }
}

// =============================================================================
// Ending a run's other work when a step fails
// =============================================================================

/// What a stopped step is the collateral of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollateralCause {
    /// The class of the failure that ended the run: what is stopped because
    /// of it is attributed to what it was attributed to.
    pub class: crate::failure::AttributionClass,
    /// The step that failed and why, for the cancelled requests' record.
    pub detail: String,
}

impl CollateralCause {
    /// The cause a failure is, for everything stopped because of it.
    pub fn of(failure: &ExecutionError) -> Self {
        Self {
            class: failure.attribution_class(),
            detail: format!(
                "step {} ({}) failed: {}",
                failure
                    .step_token_id()
                    .map(|t| t.to_string())
                    .unwrap_or_else(|| "?".to_string()),
                failure.failure_cap_urn().unwrap_or("?"),
                failure.failure_reason()
            ),
        }
    }
}

/// The signal that something has failed for good and what runs beside it is
/// to end as its collateral (15.2 §Scheduling).
///
/// A run has one for the machine, and one more for every run of a map's body
/// ([`child`](Self::child)). A failure raises the signal of the scope it
/// happened in: that ends everything in the scope and in every scope inside
/// it. Whether it goes further is decided where the scope's failure is
/// received — a map whose host drops a failed item stops there; anything
/// else raises the signal of the scope around it, and so on out to the
/// machine.
///
/// A signal is raised once: the first failure is the cause, later ones are
/// consequences.
#[derive(Clone)]
pub struct RunCancel {
    tx: Arc<tokio::sync::watch::Sender<Option<CollateralCause>>>,
    /// The signal of the scope around this one. Raised there, raised here.
    parent: Option<Arc<RunCancel>>,
}

impl Default for RunCancel {
    fn default() -> Self {
        Self::new()
    }
}

impl RunCancel {
    pub fn new() -> Self {
        Self {
            tx: Arc::new(tokio::sync::watch::channel(None).0),
            parent: None,
        }
    }

    /// The signal of a scope inside this one: raised when this is, and
    /// raised on its own without raising this.
    pub fn child(&self) -> Self {
        Self {
            tx: Arc::new(tokio::sync::watch::channel(None).0),
            parent: Some(Arc::new(self.clone())),
        }
    }

    /// Say that this scope has failed, and of what. The first cause stands.
    pub fn raise(&self, cause: CollateralCause) {
        self.tx.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(cause);
            true
        });
    }

    /// The cause, if this scope or one around it has failed: the outermost
    /// one's, which is what everything inside is the collateral of.
    pub fn cause(&self) -> Option<CollateralCause> {
        self.parent
            .as_ref()
            .and_then(|parent| parent.cause())
            .or_else(|| self.tx.borrow().clone())
    }

    /// Resolves when this scope or one around it has failed, with the cause.
    /// Never resolves for a scope that does not fail.
    pub fn raised(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = CollateralCause> + Send + '_>> {
        Box::pin(async move {
            let own = async {
                let mut rx = self.tx.subscribe();
                loop {
                    if let Some(cause) = rx.borrow_and_update().clone() {
                        return cause;
                    }
                    if rx.changed().await.is_err() {
                        // Unreachable while `self` holds the sender.
                        std::future::pending::<()>().await;
                    }
                }
            };
            match &self.parent {
                None => own.await,
                Some(parent) => tokio::select! {
                    biased;
                    cause = parent.raised() => cause,
                    cause = own => cause,
                },
            }
        })
    }
}

// =============================================================================
// Step ordering and chains
// =============================================================================

/// The step that produces each node of a graph: a step produces its own node
/// (its main output) and the node of every other output of it something takes.
fn producers_of(steps: &[ResolvedStep]) -> HashMap<&str, usize> {
    let mut produced_by: HashMap<&str, usize> = HashMap::new();
    for (index, step) in steps.iter().enumerate() {
        for node in step.produced_nodes() {
            produced_by.insert(node.as_str(), index);
        }
    }
    produced_by
}

/// Topological sort of steps.
///
/// A step can run when every step that produces a node it reads has
/// completed. Returns step indices in an order that respects that, the same
/// for the same graph every time; a cycle is refused.
pub(crate) fn topological_sort_steps(steps: &[ResolvedStep]) -> Result<Vec<usize>, ExecutionError> {
    let n = steps.len();
    let produced_by = producers_of(steps);

    let mut in_degree: Vec<usize> = vec![0; n];
    // dependents[i] = the steps that wait on step i.
    let mut dependents: Vec<std::collections::BTreeSet<usize>> =
        (0..n).map(|_| std::collections::BTreeSet::new()).collect();
    for (i, step) in steps.iter().enumerate() {
        for source in step.source_nodes() {
            if let Some(&dep) = produced_by.get(source.as_str()) {
                // A step that reads what it produces waits on itself: a cycle.
                if dependents[dep].insert(i) {
                    in_degree[i] += 1;
                }
            }
        }
    }

    let mut ready: std::collections::VecDeque<usize> =
        (0..n).filter(|&i| in_degree[i] == 0).collect();
    let mut sorted: Vec<usize> = Vec::with_capacity(n);
    while let Some(i) = ready.pop_front() {
        sorted.push(i);
        for &j in &dependents[i] {
            in_degree[j] -= 1;
            if in_degree[j] == 0 {
                ready.push_back(j);
            }
        }
    }

    if sorted.len() != n {
        // A cyclic graph reached execution — planner bug, ours: Internal.
        return Err(ExecutionError::CartridgeExecutionFailed {
            cap_urn: String::new(),
            code: None,
            class: crate::failure::AttributionClass::Internal,
            details: "Cycle detected in graph".to_string(),
            arg_urn: None,
        });
    }

    Ok(sorted)
}

/// Decompose topologically-ordered steps into maximal PIPELINED chains (lists
/// of indices into `steps`).
///
/// Inside a chain every step's response streams live into the next step's
/// main input: nothing of it is held. That is only right for a hop where
/// nothing else needs what the producer emits and nothing about the hop can
/// change once it has begun. A step continues its producer's chain iff:
///
/// - it has exactly one input stream, and that is the producer's MAIN output
///   as it is (not a gather, not another output of it);
/// - nothing else in the graph reads anything the producer produces, the
///   producer has no other output that something takes, and its main output
///   is not in `keep` (what something outside this graph reads);
/// - the producer's main output is not `optional` — an output that may not
///   fire is absent or present only once its request ends, so the consumer
///   is opened only then (15.2 §Absent values and skipped steps);
/// - neither step has a policy — a step that may be attempted again, or
///   whose failure is tolerated, runs alone: nothing it emits is forwarded
///   before an attempt has ended, and what it is given must be there to give
///   again (15.2 §Error handling).
///
/// Every other step heads a chain, and is fed from materialised values.
/// Chains are returned in head-topological order, so every chain's head
/// inputs are produced by an earlier chain's last step.
pub(crate) fn decompose_step_chains(
    steps: &[ResolvedStep],
    step_order: &[usize],
    keep: &HashSet<String>,
) -> Vec<Vec<usize>> {
    let n = steps.len();
    let produced_by = producers_of(steps);

    // readers[i] = how many input streams in the graph read something step i
    // produces (a gather member counts).
    let mut readers: Vec<usize> = vec![0; n];
    for step in steps {
        for source in step.source_nodes() {
            if let Some(&src) = produced_by.get(source.as_str()) {
                readers[src] += 1;
            }
        }
    }

    // The step whose chain `i` continues, if it continues one.
    let continues: Vec<Option<usize>> = (0..n)
        .map(|i| {
            let step = &steps[i];
            let [input] = step.inputs.as_slice() else {
                return None;
            };
            let StepSource::Node(from) = &input.source else {
                return None;
            };
            let src = *produced_by.get(from.as_str())?;
            let producer = &steps[src];
            // A producer with no other taken output produces one node, its
            // main output: `from` is that.
            let pipelined = src != i
                && readers[src] == 1
                && producer.outputs.is_empty()
                && !keep.contains(&producer.node)
                && !producer.cap.main_output().is_some_and(|output| output.optional)
                && producer.policy.is_default()
                && step.policy.is_default();
            pipelined.then_some(src)
        })
        .collect();
    let mut continued_by: Vec<Option<usize>> = vec![None; n];
    for (i, src) in continues.iter().enumerate() {
        if let Some(src) = src {
            continued_by[*src] = Some(i);
        }
    }

    let mut chains: Vec<Vec<usize>> = Vec::new();
    for &start in step_order {
        if continues[start].is_some() {
            continue;
        }
        let mut chain = vec![start];
        let mut tail = start;
        while let Some(next) = continued_by[tail] {
            chain.push(next);
            tail = next;
        }
        chains.push(chain);
    }
    chains
}

// =============================================================================
// Cartridge Manager
// =============================================================================

/// Manages cartridge discovery, download, and caching.
pub struct CartridgeManager {
    cartridge_repo: CartridgeRepo,
    /// Channel-partitioned root: cartridges install under
    /// `{cartridge_dir}/{channel}/{cartridge_id}/{version}/`.
    cartridge_dir: PathBuf,
    /// The cartridge registry this manager installs from. `None` = a build
    /// with no baked registry (dev): only dev binaries and bundled cartridges
    /// exist, and any cap that would need a registry download hard-errors.
    registry_url: Option<String>,
    /// Signing trust anchors (baked roots + environment). Required for every
    /// registry operation: the manifest chain-verifies on sync, and every
    /// downloaded/installed binary verifies under a certificate-authorized
    /// release key. `Some(registry) + None(trust)` cannot happen in a
    /// product build (capdag's build.rs enforces the triple); the runtime
    /// guard below covers library/test misuse.
    trust: Option<crate::bifaci::release_cert::RegistryTrust>,
    /// Channel this manager is operating in. The orchestrator can only
    /// install/run cartridges that match its channel — release builds
    /// never touch nightly artefacts and vice versa.
    channel: crate::bifaci::cartridge_repo::CartridgeChannel,
    /// Fabric registry manifest version stamped into every cartridge.json
    /// this manager writes. Typically the engine's build-time-baked
    /// `capdag::FABRIC_MANIFEST_VERSION`; passed in at construction so
    /// tests can write provenance at arbitrary versions.
    fabric_manifest_version: u32,
    dev_cartridges: HashMap<PathBuf, CapManifest>,
}

impl CartridgeManager {
    pub fn new(
        cartridge_dir: PathBuf,
        registry_url: Option<String>,
        channel: crate::bifaci::cartridge_repo::CartridgeChannel,
        fabric_manifest_version: u32,
        dev_binaries: Vec<PathBuf>,
        trust: Option<crate::bifaci::release_cert::RegistryTrust>,
        // The fabric registry this client resolves caps against. Every cartridge
        // registry fetched must declare this same fabric (enforced in
        // `CartridgeRepoServer::new`) so all registries share one fabric.
        fabric_registry_url: String,
        // The roots every registry's charter must chain to: the build's own
        // (`CharterTrust::from_build_constants`).
        charter_trust: crate::bifaci::charter::CharterTrust,
    ) -> Self {
        use crate::bifaci::cartridge_json::CartridgeJson;

        // Resolve dev paths: directories with cartridge.json → resolve entry point.
        // Files → standalone binary. Directories without cartridge.json → each
        // executable file inside is a separate binary cartridge.
        let mut resolved: Vec<PathBuf> = Vec::new();
        // Dev cartridges resolved via this code path always live under
        // the dev tree, so the folder they are held to is `dev`.
        // Registry-installed cartridges go through CartridgeManager's
        // download path, not here.
        let dev_folder = crate::bifaci::charter::RegistryFolder::Dev;
        for p in dev_binaries {
            if p.is_file() {
                resolved.push(p);
            } else if p.is_dir() {
                match CartridgeJson::read_from_dir(&p, &dev_folder) {
                    Ok(cj) => {
                        let entry_point = cj.resolve_entry_point(&p);
                        resolved.push(entry_point);
                    }
                    Err(crate::bifaci::cartridge_json::CartridgeJsonError::NotFound(_)) => {
                        // No cartridge.json — treat each executable file as a separate binary cartridge
                        if let Ok(entries) = std::fs::read_dir(&p) {
                            for entry in entries.flatten() {
                                let path = entry.path();
                                if path.is_file() {
                                    #[cfg(unix)]
                                    {
                                        use std::os::unix::fs::PermissionsExt;
                                        if let Ok(meta) = std::fs::metadata(&path) {
                                            if meta.permissions().mode() & 0o111 != 0 {
                                                resolved.push(path);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            "[DevMode] Invalid cartridge.json in {:?}: {} — skipping",
                            p,
                            e
                        );
                    }
                }
            }
        }

        Self {
            cartridge_repo: CartridgeRepo::new(3600, fabric_registry_url, charter_trust),
            cartridge_dir,
            registry_url,
            trust,
            channel,
            fabric_manifest_version,
            dev_cartridges: resolved
                .into_iter()
                .map(|p| {
                    (
                        p,
                        CapManifest::new(
                            String::new(),
                            String::new(),
                            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
                            None,
                            String::new(),
                            Vec::new(),
                        ),
                    )
                })
                .collect(),
        }
    }

    /// The same manager, fetching its registry with `client` — one that
    /// trusts the certificate authorities a deployment serves its registries
    /// under.
    pub fn with_http_client(mut self, client: reqwest::Client) -> Self {
        self.cartridge_repo = self.cartridge_repo.with_http_client(client);
        self
    }

    pub async fn init(&mut self) -> Result<(), ExecutionError> {
        fs::create_dir_all(&self.cartridge_dir)?;

        for (bin_path, _) in &self.dev_cartridges.clone() {
            match self.discover_manifest(bin_path).await {
                Ok(manifest) => {
                    self.dev_cartridges.insert(bin_path.clone(), manifest);
                }
                Err(e) => {
                    tracing::error!("[DevMode] Failed: {:?}: {}", bin_path, e);
                    return Err(e);
                }
            }
        }

        if let Some(registry_url) = self.registry_url.clone() {
            // Manifest verification happens inside the fetch when trust is
            // set: the `<url>.sig` sidecar must chain-verify over the exact
            // fetched bytes before the manifest is cached.
            self.cartridge_repo.set_trust(self.trust.clone()).await;
            self.cartridge_repo
                .sync_repos(&[registry_url.clone()])
                .await;
            // This manager's registry is mandatory — a sync failure (network,
            // parse, or SIGNATURE) surfaces here with its real cause instead
            // of a later misleading "cartridge not found".
            // A registry this manager depends on must have VERIFIED. Any other
            // verdict — unreachable, unsigned, a signature this build cannot
            // read — means the cartridges it lists have unconfirmed
            // provenance, and the state says which so the operator is not sent
            // to the wrong remedy.
            let verdict = self.cartridge_repo.registry_verdict(&registry_url).await;
            if !verdict.permits_attachment() {
                return Err(ExecutionError::HostError(format!(
                    "cartridge registry '{registry_url}' is {}: {}",
                    verdict.state.wire_name(),
                    if verdict.detail.is_empty() {
                        "no verdict has been reached yet".to_string()
                    } else {
                        verdict.detail.clone()
                    }
                )));
            }
        }

        Ok(())
    }

    /// The configured registry URL, or the hard error every registry
    /// operation raises in a registry-less (dev) build.
    /// This manager's registry, and the folder its cartridges live in —
    /// opened through the registry's charter, which the last sync verified.
    /// A registry whose charter was never verified has no folder: nothing is
    /// installed from it or run out of it.
    async fn registry_charter(
        &self,
        context: &str,
    ) -> Result<(String, crate::bifaci::charter::VerifiedCharter), ExecutionError> {
        let registry_url = self.registry_url_required(context)?.to_string();
        let charter = self.cartridge_repo.charter(&registry_url).await.ok_or_else(|| {
            ExecutionError::CartridgeDownloadFailed(format!(
                "{context}: the registry {registry_url} has no verified charter — it has not \
                 been synced, or its charter was refused (its verdict says why)"
            ))
        })?;
        Ok((registry_url, charter))
    }

    fn registry_url_required(&self, context: &str) -> Result<&str, ExecutionError> {
        self.registry_url.as_deref().ok_or_else(|| {
            ExecutionError::CartridgeDownloadFailed(format!(
                "{context}: this build bakes no cartridge registry — registry installs and \
                 downloads are disabled in dev builds (use --dev-bins or a bundled cartridge)"
            ))
        })
    }

    /// The signing trust anchors, or the hard error raised when a registry
    /// operation is attempted without them.
    fn trust_required(
        &self,
        context: &str,
    ) -> Result<&crate::bifaci::release_cert::RegistryTrust, ExecutionError> {
        self.trust.as_ref().ok_or_else(|| {
            ExecutionError::CartridgeDownloadFailed(format!(
                "{context}: this build bakes no cartridge signing root keys — registry \
                 downloads are disabled without them (a product build bakes \
                 MFR_CARTRIDGE_ROOT_PUBKEYS beside the registry URL)"
            ))
        })
    }

    async fn discover_manifest(&self, bin_path: &Path) -> Result<CapManifest, ExecutionError> {
        // Through the launcher, like every other start of a cartridge: a dev
        // cartridge's entry may be a script, and Windows cannot execute one —
        // this was the one start that still named the entry as the program,
        // and a Python dev cartridge failed here with "%1 is not a valid Win32
        // application". See `bifaci::launch`.
        let mut child = crate::bifaci::launch::tokio_command(bin_path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| ExecutionError::CartridgeExecutionFailed {
                cap_urn: "manifest-discovery".to_string(),
                code: None,
                // An unspawnable cartridge binary is a deployment problem.
                class: crate::failure::AttributionClass::Environment,
                details: format!("Failed to spawn cartridge: {}", e),
                arg_urn: None,
            })?;

        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();

        let mut reader = FrameReader::new(stdout);
        let mut writer = FrameWriter::new(stdin);

        let result = handshake(&mut reader, &mut writer)
            .await
            .map_err(|e| ExecutionError::HostError(format!("Handshake failed: {:?}", e)))?;

        let _ = child.kill().await;
        CapManifest::from_advertised(&result.manifest)
            .map_err(|e| ExecutionError::HostError(format!("Bad manifest: {}", e)))
    }

    /// Resolve all cap URNs from the graph to unique (binary_path, cap_groups) pairs.
    ///
    /// For dev cartridges (with discovered manifests), forwards the
    /// cartridge's full `cap_groups` to the host so every cap declared
    /// in the manifest is registered — not just the DAG-edge caps.
    /// This is critical because cartridges send peer requests for caps
    /// that aren't in the DAG (e.g. candlecartridge peer-invokes
    /// modelcartridge's `download-model` cap during ML inference).
    /// Without full cap registration, the `CartridgeHostRuntime` can't
    /// route these peer requests.
    ///
    /// `adapter_urns` declared by the cartridge propagate verbatim
    /// inside the cap groups so the engine can register
    /// content-inspection adapters per cartridge once the host's
    /// RelayNotify reaches the relay.
    pub async fn resolve_cartridges(
        &self,
        cap_urns: &[&str],
    ) -> Result<
        Vec<(
            PathBuf,
            Option<(
                String,
                String,
                crate::bifaci::cartridge_repo::CartridgeChannel,
            )>,
            Vec<crate::bifaci::manifest::CapGroup>,
        )>,
        ExecutionError,
    > {
        // Collect unique cartridge binaries needed for the DAG
        let mut cartridge_paths: HashSet<PathBuf> = HashSet::new();

        for &cap_urn in cap_urns {
            let (bin_path, _cartridge_id) = self.find_cartridge_binary(cap_urn).await?;
            cartridge_paths.insert(bin_path);
        }

        // Also include ALL dev cartridge binaries — they may be needed for peer request
        // routing even if they don't directly appear in the DAG.
        for dev_path in self.dev_cartridges.keys() {
            cartridge_paths.insert(dev_path.clone());
        }

        // Every cartridge is hosted with the full cap_groups it declares —
        // adapter_urns and all — so the host advertises its caps before it
        // runs and a request for any of them starts it.
        //
        // A registry install's groups are the ones its own binary reports,
        // read the way a dev cartridge's are: the installed version is what
        // runs, and it may be older than the registry's latest. These were a
        // placeholder group holding only the identity cap, on the expectation
        // that the switch's identity probe would start the cartridge and its
        // HELLO would bring the real caps. The probe goes to the host, which
        // hands it to the first cartridge serving identity — and every
        // cartridge serves identity. So only the first cartridge hosted was
        // ever started that way: with a bundled cartridge ahead of it, or a
        // second registry cartridge after it, a registry cap waited 15 s for
        // an advertisement that never came.
        let mut result: Vec<(
            PathBuf,
            Option<(
                String,
                String,
                crate::bifaci::cartridge_repo::CartridgeChannel,
            )>,
            Vec<crate::bifaci::manifest::CapGroup>,
        )> = Vec::with_capacity(cartridge_paths.len());
        for path in cartridge_paths {
            if let Some(manifest) = self.dev_cartridges.get(&path) {
                let identity = Some((
                    manifest.name.clone(),
                    manifest.version.clone(),
                    manifest.channel,
                ));
                result.push((path, identity, manifest.cap_groups.clone()));
            } else {
                let manifest = self.discover_manifest(&path).await?;
                result.push((path, None, manifest.cap_groups));
            }
        }

        Ok(result)
    }

    /// Registry suggestions for a cap URN (which cartridges provide it),
    /// resolved from the synced, signature-verified manifest — no download.
    /// Powers `capdag resolve`.
    pub async fn suggestions_for_cap(
        &self,
        cap_urn: &str,
    ) -> Vec<crate::bifaci::cartridge_repo::CartridgeSuggestion> {
        self.cartridge_repo.get_suggestions_for_cap(cap_urn).await
    }

    /// The synced registry's entry for a cartridge id in this manager's
    /// channel, if any. Powers `capdag resolve`'s per-cartridge detail
    /// (version, platforms, signed-binary presence).
    pub async fn registry_cartridge(
        &self,
        cartridge_id: &str,
    ) -> Option<crate::bifaci::cartridge_repo::CartridgeInfo> {
        let registry_url = self.registry_url.as_deref()?;
        self.cartridge_repo
            .get_cartridge(registry_url, self.channel, cartridge_id)
            .await
    }

    /// Find the binary path for a cap URN.
    async fn find_cartridge_binary(
        &self,
        cap_urn: &str,
    ) -> Result<(PathBuf, String), ExecutionError> {
        let requested_urn =
            CapUrn::from_string(cap_urn).map_err(|e| ExecutionError::CartridgeNotFound {
                cap_urn: format!("Invalid URN: {}: {}", cap_urn, e),
            })?;

        // This is RESOLUTION, not dispatch: `requested_urn` is a fully-resolved,
        // concrete cap (an alias resolved to exactly one cap URN), and we must
        // run the cartridge that implements THAT cap — never silently substitute
        // a merely-dispatchable one. So we match with `is_equivalent` (symmetric
        // exact match), the SAME relation the registry path uses in
        // `get_suggestions_for_cap`. Using the looser `is_dispatchable` here
        // would let a dev cartridge declaring a more-general cap short-circuit
        // the exact cap the alias named. See ../docs/07-dispatch.md,
        // "Resolution vs. dispatch: which predicate?".
        for (bin_path, manifest) in &self.dev_cartridges {
            for cap in manifest.all_caps() {
                // cap.urn is the declared candidate cap; requested_urn is the
                // resolved cap we must run.
                if cap.urn.is_equivalent(&requested_urn) {
                    return Ok((bin_path.clone(), format!("dev:{}", bin_path.display())));
                }
            }
        }

        // Fall back to registry
        let suggestions = self.cartridge_repo.get_suggestions_for_cap(cap_urn).await;
        if suggestions.is_empty() {
            return Err(ExecutionError::CartridgeNotFound {
                cap_urn: cap_urn.to_string(),
            });
        }

        let cartridge_id = &suggestions[0].cartridge_id;
        let bin_path = self.get_cartridge_path(cartridge_id).await?;
        Ok((bin_path, cartridge_id.clone()))
    }

    pub async fn get_cartridge_path(&self, cartridge_id: &str) -> Result<PathBuf, ExecutionError> {
        if let Some(dev_path) = cartridge_id.strip_prefix("dev:") {
            let path = PathBuf::from(dev_path);
            if !path.exists() {
                // A missing dev binary is a deployment problem — Environment.
                return Err(ExecutionError::CartridgeExecutionFailed {
                    cap_urn: cartridge_id.to_string(),
                    code: None,
                    class: crate::failure::AttributionClass::Environment,
                    details: format!("Dev binary not found: {:?}", path),
                    arg_urn: None,
                });
            }
            return Ok(path);
        }

        // Look for an existing installed cartridge in the registry-partitioned,
        // version-partitioned, channel-partitioned versioned layout:
        // `{cartridge_dir}/{slug}/v{cartridge_registry_version}/{channel}/{cartridge_id}/{version}/cartridge.json`.
        // The orchestrator's manager is bound to a single registry — that's
        // the registry it just fetched the manifest from, so the folder it
        // walks is that registry's charter slug.
        let (registry_url, charter) = self.registry_charter(cartridge_id).await?;
        let folder = crate::bifaci::charter::RegistryFolder::Chartered(charter);
        let name_dir = self
            .cartridge_dir
            .join(folder.slug())
            .join(format!("v{}", crate::CARTRIDGE_REGISTRY_VERSION))
            .join(self.channel.as_str())
            .join(cartridge_id);
        if name_dir.is_dir() {
            if let Some(entry_point) =
                self.find_latest_installed_entry_point(&name_dir, &folder, &registry_url)
            {
                // Re-verify the installed binary against the registry's
                // signed manifest on every use — a post-install tamper of
                // the on-disk executable must never run. A FAILED
                // verification is not a dead end, though: whether the local
                // bytes rotted or the registry re-published this version
                // with new bytes (staging does, via --overwrite), the
                // remedy is identical and fully gated — discard the stale
                // install and re-download through the same
                // sha256+size+signature pipeline. Only a fresh download
                // that ALSO fails verification is terminal (the registry
                // itself is inconsistent).
                match self
                    .verify_cartridge_integrity(cartridge_id, &entry_point)
                    .await
                {
                    Ok(()) => return Ok(entry_point),
                    Err(e) => {
                        tracing::warn!(
                            cartridge_id,
                            entry_point = %entry_point.display(),
                            error = %e,
                            "installed cartridge failed integrity verification against the \
                             current signed manifest — discarding the stale install and \
                             re-downloading (every byte re-verifies before it can run)"
                        );
                        if let Some(version_dir) = entry_point.parent() {
                            if let Err(rm) = fs::remove_dir_all(version_dir) {
                                // If the stale install cannot even be removed,
                                // the original verification failure stands —
                                // never run unverified bytes.
                                tracing::error!(
                                    version_dir = %version_dir.display(),
                                    error = %rm,
                                    "failed to remove stale cartridge install"
                                );
                                return Err(e);
                            }
                        }
                    }
                }
            }
        }

        self.download_cartridge(cartridge_id).await
    }

    /// Find the entry point of the latest installed version in a cartridge name directory.
    /// `folder` is the registry folder the caller reached through (the one
    /// `self.registry_url`'s charter names); the per-version cartridge.json is
    /// validated against it via the charter rule.
    fn find_latest_installed_entry_point(
        &self,
        name_dir: &Path,
        folder: &crate::bifaci::charter::RegistryFolder,
        registry_url: &str,
    ) -> Option<PathBuf> {
        let mut versions: Vec<(String, PathBuf)> = Vec::new();

        for entry in fs::read_dir(name_dir).ok()? {
            let entry = entry.ok()?;
            let version_dir = entry.path();
            if !version_dir.is_dir() {
                continue;
            }
            match crate::bifaci::cartridge_json::CartridgeJson::read_from_dir(&version_dir, folder) {
                Ok(cj) => {
                    // Hard mismatch — never run a cartridge from a different
                    // channel even if it landed under our channel's tree.
                    if cj.channel != self.channel {
                        tracing::warn!(
                            "Skipping cartridge at {:?}: cartridge.json channel '{}' \
                             does not match orchestrator channel '{}'",
                            version_dir,
                            cj.channel,
                            self.channel
                        );
                        continue;
                    }
                    // Three-place rule: cartridge.json's registry_url
                    // must match the orchestrator's. The slug check
                    // inside read_from_dir only proves folder ⇔ json
                    // agreement; here we check json ⇔ orchestrator's
                    // configured registry. Mismatches are skipped, not
                    // deleted — a stale install from a previously
                    // configured registry is a user-visible state, not
                    // garbage.
                    if cj.registry_url.as_deref() != Some(registry_url) {
                        tracing::warn!(
                            "Skipping cartridge at {:?}: cartridge.json registry_url={:?} \
                             does not match orchestrator registry_url='{}'",
                            version_dir,
                            cj.registry_url,
                            registry_url
                        );
                        continue;
                    }
                    let entry_point = cj.resolve_entry_point(&version_dir);
                    versions.push((cj.version, entry_point));
                }
                Err(_) => continue,
            }
        }

        if versions.is_empty() {
            return None;
        }

        // Sort by version descending (latest first)
        versions.sort_by(|a, b| {
            let parts_a: Vec<u32> = a.0.split('.').filter_map(|p| p.parse().ok()).collect();
            let parts_b: Vec<u32> = b.0.split('.').filter_map(|p| p.parse().ok()).collect();
            let max_len = parts_a.len().max(parts_b.len());
            for i in 0..max_len {
                let na = parts_a.get(i).copied().unwrap_or(0);
                let nb = parts_b.get(i).copied().unwrap_or(0);
                match nb.cmp(&na) {
                    std::cmp::Ordering::Equal => continue,
                    other => return other,
                }
            }
            std::cmp::Ordering::Equal
        });

        Some(versions.into_iter().next()?.1)
    }

    /// The manifest `binary` entry for a cartridge's host-platform build,
    /// or the hard error for a build that publishes none. There is NO
    /// installer fallback — the pure binary is the only artifact this
    /// execution path runs.
    async fn registry_binary_info(
        &self,
        registry_url: &str,
        cartridge_id: &str,
    ) -> Result<
        (
            crate::bifaci::cartridge_repo::CartridgeInfo,
            crate::bifaci::cartridge_repo::CartridgeBinaryInfo,
        ),
        ExecutionError,
    > {
        let cartridge_info = self
            .cartridge_repo
            .get_cartridge(registry_url, self.channel, cartridge_id)
            .await
            .ok_or_else(|| ExecutionError::CartridgeNotFound {
                cap_urn: format!(
                    "Cartridge {} not found in {} registry",
                    cartridge_id, self.channel
                ),
            })?;

        let platform = detect_platform();
        let build = cartridge_info
            .build_for_platform(&platform)
            .ok_or_else(|| {
                ExecutionError::CartridgeDownloadFailed(format!(
                    "Cartridge {} v{} has no build for platform '{}'. Available: {:?}",
                    cartridge_id,
                    cartridge_info.version,
                    platform,
                    cartridge_info.available_platforms()
                ))
            })?;

        let binary = build.binary.clone().ok_or_else(|| {
            ExecutionError::CartridgeDownloadFailed(format!(
                "Cartridge {} v{} for '{}' publishes no signed pure-binary artifact — \
                 refusing installer fallback. Re-publish the version under the signing \
                 regime: a published release build of cartridge {}.",
                cartridge_id, cartridge_info.version, platform, cartridge_id
            ))
        })?;
        Ok((cartridge_info, binary))
    }

    /// Verify `bytes` against a manifest `binary` entry: sha256, size, and
    /// the minisign signature under a certificate-authorized release key
    /// from the registry's verified manifest sidecar. Every failure is a
    /// hard, named error.
    async fn verify_binary_against_manifest(
        &self,
        registry_url: &str,
        cartridge_id: &str,
        binary: &crate::bifaci::cartridge_repo::CartridgeBinaryInfo,
        bytes: &[u8],
    ) -> Result<(), ExecutionError> {
        // Trust must exist for any registry operation.
        self.trust_required(cartridge_id)?;

        use sha2::{Digest, Sha256};
        let computed = format!("{:x}", Sha256::digest(bytes));
        if computed != binary.sha256 {
            return Err(ExecutionError::CartridgeDownloadFailed(format!(
                "SECURITY: SHA256 mismatch for {}!\n  Expected: {}\n  Computed: {}",
                cartridge_id, binary.sha256, computed
            )));
        }
        if bytes.len() as u64 != binary.size {
            return Err(ExecutionError::CartridgeDownloadFailed(format!(
                "SECURITY: size mismatch for {} (manifest says {} bytes, got {})",
                cartridge_id,
                binary.size,
                bytes.len()
            )));
        }

        // The signature must verify under a release key the manifest's
        // chain-verified certificate list authorizes. The list is populated
        // by the verified sidecar at sync; empty means the sync didn't
        // verify — which init() already turns into a hard error, so this is
        // a defense-in-depth check with its own message.
        let release_keys = self
            .cartridge_repo
            .verified_release_keys(registry_url)
            .await;
        if release_keys.is_empty() {
            return Err(ExecutionError::CartridgeDownloadFailed(format!(
                "SECURITY: no certificate-authorized release keys for registry '{}' — the \
                 manifest signature sidecar was never verified; refusing to trust {}'s \
                 binary signature",
                registry_url, cartridge_id
            )));
        }
        let mut last_error: Option<crate::SignatureError> = None;
        for (_key_id, pubkey) in &release_keys {
            match crate::verify_binary_signature(pubkey, &binary.signature, bytes) {
                Ok(()) => return Ok(()),
                Err(e) => last_error = Some(e),
            }
        }
        Err(ExecutionError::CartridgeDownloadFailed(format!(
            "SECURITY: binary signature for {} does not verify under any of the {} \
             certificate-authorized release key(s) of registry '{}': {}",
            cartridge_id,
            release_keys.len(),
            registry_url,
            last_error.expect("non-empty key list implies at least one error")
        )))
    }

    /// Re-verify an INSTALLED cartridge binary against the registry's signed
    /// manifest: recompute the sha256 of the on-disk executable and check
    /// its manifest signature chain. Runs on every reuse of an installed
    /// registry cartridge — a post-install tamper must never execute.
    async fn verify_cartridge_integrity(
        &self,
        cartridge_id: &str,
        binary_path: &Path,
    ) -> Result<(), ExecutionError> {
        let registry_url = self.registry_url_required(cartridge_id)?.to_string();
        let (_info, binary) = self
            .registry_binary_info(&registry_url, cartridge_id)
            .await?;
        let bytes = fs::read(binary_path).map_err(|e| {
            // An unreadable installed binary is a deployment problem.
            ExecutionError::CartridgeExecutionFailed {
                cap_urn: cartridge_id.to_string(),
                code: None,
                class: crate::failure::AttributionClass::Environment,
                details: format!(
                    "failed to read installed binary {:?} for integrity verification: {}",
                    binary_path, e
                ),
                arg_urn: None,
            }
        })?;
        self.verify_binary_against_manifest(&registry_url, cartridge_id, &binary, &bytes)
            .await
            .map_err(|e| ExecutionError::CartridgeExecutionFailed {
                cap_urn: cartridge_id.to_string(),
                code: None,
                // A tampered/mismatched installed binary is a deployment
                // problem — Environment.
                class: crate::failure::AttributionClass::Environment,
                details: format!(
                    "installed binary at {:?} failed integrity verification: {}",
                    binary_path, e
                ),
                arg_urn: None,
            })
    }

    /// Download a cartridge's signed PURE-BINARY artifact from the registry
    /// and install it into the versioned directory layout:
    /// `{cartridge_dir}/{slug}/{channel}/{id}/{version}/cartridge.json + binary`.
    ///
    /// Nothing touches disk until the bytes pass ALL of: sha256, size, and
    /// the minisign signature under a certificate-authorized release key
    /// (chain: baked roots → release-key certificate from the verified
    /// manifest sidecar → binary signature). Installer packages are never a
    /// fallback.
    async fn download_cartridge(&self, cartridge_id: &str) -> Result<PathBuf, ExecutionError> {
        let registry_url = self.registry_url_required(cartridge_id)?.to_string();
        self.trust_required(cartridge_id)?;
        let (cartridge_info, binary) = self
            .registry_binary_info(&registry_url, cartridge_id)
            .await?;

        // The v5 manifest carries the absolute URL on the binary itself.
        // No URL derivation: if the manifest's URL is wrong, we want to fail
        // hard against the URL the publisher actually committed to. The
        // manifest also pins the artifact's sha256 — appending it as a query
        // param keys any CDN cache to the CONTENT: a republished
        // (byte-different) artifact at the same path gets a new cache entry
        // instead of serving the stale object until TTL expiry.
        let download_url = format!(
            "{}{}sha256={}",
            binary.url,
            if binary.url.contains('?') { "&" } else { "?" },
            binary.sha256
        );
        let download_url = download_url.as_str();

        let response = reqwest::get(download_url).await.map_err(|e| {
            ExecutionError::CartridgeDownloadFailed(format!("Download failed: {}", e))
        })?;

        if !response.status().is_success() {
            return Err(ExecutionError::CartridgeDownloadFailed(format!(
                "HTTP {} from {}",
                response.status(),
                download_url
            )));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| ExecutionError::CartridgeDownloadFailed(format!("Read failed: {}", e)))?
            .to_vec();

        self.verify_binary_against_manifest(&registry_url, cartridge_id, &binary, &bytes)
            .await?;

        // Registry-partitioned, version-partitioned, channel-partitioned
        // layout. The orchestrator only ever installs from its own configured
        // registry, into the folder its charter names — and stores the charter
        // there, so the folder proves itself to every host that scans it.
        let (_, charter) = self.registry_charter(cartridge_id).await?;
        let registry_root = self.cartridge_dir.join(&charter.charter.slug);
        crate::bifaci::charter::store(&charter, &registry_root).map_err(|e| {
            ExecutionError::CartridgeDownloadFailed(format!(
                "Failed to store the charter of {} in {}: {}",
                registry_url,
                registry_root.display(),
                e
            ))
        })?;
        let version_dir = registry_root
            .join(format!("v{}", crate::CARTRIDGE_REGISTRY_VERSION))
            .join(self.channel.as_str())
            .join(cartridge_id)
            .join(&cartridge_info.version);
        fs::create_dir_all(&version_dir)?;

        let binary_name = platform_binary_name(cartridge_id);
        let binary_path = version_dir.join(&binary_name);
        fs::write(&binary_path, &bytes)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mut perms = fs::metadata(&binary_path)?.permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&binary_path, perms)?;
        }

        // Write cartridge.json. `registry_url` is verbatim the manager's
        // registry's anchor; the cartridge was downloaded from there, so the
        // charter rule (folder ⇔ provenance ⇔ HELLO) is satisfied by
        // construction. sha/size describe the BINARY artifact.
        let cj = crate::CartridgeJson {
            name: cartridge_id.to_string(),
            version: cartridge_info.version.clone(),
            channel: self.channel,
            registry_url: Some(registry_url.clone()),
            entry: binary_name.to_string(),
            installed_at: crate::bifaci::cartridge_json::install_timestamp_now(),
            installed_from: Some(crate::CartridgeInstallSource::Registry),
            // Provenance records the URL the publisher committed to, not the
            // content-keyed fetch URL.
            source_url: binary.url.clone(),
            package_sha256: binary.sha256.clone(),
            package_size: binary.size,
            fabric_manifest_version: self.fabric_manifest_version,
        };
        cj.write_to_dir(&version_dir).map_err(|e| {
            ExecutionError::CartridgeDownloadFailed(format!(
                "Failed to write cartridge.json: {}",
                e
            ))
        })?;

        Ok(binary_path)
    }
}

// =============================================================================
// Execution Context — Arc<RelaySwitch> for concurrent DAG execution
// =============================================================================

/// Handle for cleanup of a master's associated resources.
struct MasterCleanupHandle {
    /// Task handles to abort after shutdown.
    task_handles: Vec<tokio::task::JoinHandle<()>>,
}

/// Execution context for DAG execution.
///
/// Each `ExecutionContext` is an isolated execution environment that:
/// - Shares the `RelaySwitch` via `Arc` for concurrent access
/// - Owns its own `node_data` HashMap (isolated per execution)
/// - Tracks cleanup handles for managed tasks
///
/// This design enables concurrent DAG executions:
/// - Multiple contexts can share the same switch
/// - Each context has isolated node data
/// - The switch handles concurrent frame routing internally
pub struct ExecutionContext {
    /// Shared relay switch (interior mutability)
    switch: Arc<RelaySwitch>,
    /// Raw bytes at each DAG node. Isolated per execution context.
    node_data: HashMap<String, Vec<u8>>,
    /// Per-node stream metadata. Carries provenance context (e.g. {"title": "page_3"})
    /// through ForEach splits so body caps receive the upstream item's metadata.
    node_meta: HashMap<String, crate::StreamMeta>,
    /// Tracks which nodes hold sequence data (CBOR sequence of items).
    /// When true, the node's data is an RFC 8742 CBOR sequence that should be
    /// sent with is_sequence=true on STREAM_START so the receiver gets
    /// properly framed per-item chunks.
    node_is_sequence: HashMap<String, bool>,
    /// Nodes whose data is a LIVE-FEED REFERENCE selector (13.2 §Reference
    /// Media, live family): node id → reference urn. The send path labels
    /// such a node's outgoing stream with the REFERENCE urn (never the
    /// consuming edge's in_media) so the receiving cartridge's demux
    /// intercepts and resolves capture — the op stays transport-blind.
    node_live_reference: HashMap<String, String>,
    /// Nodes whose data lives in a DISK SPOOL, not in `node_data`: an
    /// UNBOUNDED intermediate at a mandatory chain-split boundary streams to
    /// a temp file (L16 — never unbounded memory) and downstream chain heads
    /// feed from the file (`send_file_stream`). node id → spool path.
    node_spool: HashMap<String, std::path::PathBuf>,
    /// Cached max chunk size from the relay.
    max_chunk: usize,
    /// Cleanup handles for masters added via add_cartridge_host.
    cleanup_handles: Vec<MasterCleanupHandle>,
}

impl ExecutionContext {
    /// Create a new ExecutionContext with an empty RelaySwitch.
    ///
    /// The RelaySwitch starts with no masters. Use `add_master()` or
    /// `add_cartridge_host()` to add masters before executing caps.
    ///
    /// Requires a `FabricRegistry` for the RelaySwitch to use when building
    /// the LiveCapFab for path finding queries. The registry is read at
    /// every LiveCapFab sync to compute the bookend-eligible URN set.
    ///
    /// `policy` is what the cartridges it runs may do (12.2 §Policy); the
    /// switch gives it to every host added to it.
    pub async fn new(
        fabric_registry: Arc<FabricRegistry>,
        policy: crate::bifaci::policy::HostPolicy,
    ) -> Result<Self, ExecutionError> {
        let switch = RelaySwitch::new(vec![], fabric_registry, policy)
            .await
            .map_err(|e| ExecutionError::HostError(format!("RelaySwitch init: {}", e)))?;

        let max_chunk = switch.limits().await.max_chunk as usize;
        let max_chunk = if max_chunk == 0 {
            DEFAULT_MAX_CHUNK as usize
        } else {
            max_chunk
        };

        let switch = Arc::new(switch);
        // Start the background frame pump so master-side frames (notably
        // RelayNotify capability updates) are continuously dispatched
        // through `handle_master_frame` even while the orchestrator is
        // not actively executing a cap. Without this, `wait_for_cap`
        // polls a master.caps that never updates because no consumer is
        // draining `frame_rx`.
        switch.start_background_pump();

        Ok(Self {
            switch,
            node_data: HashMap::new(),
            node_live_reference: HashMap::new(),
            node_meta: HashMap::new(),
            node_is_sequence: HashMap::new(),
            node_spool: HashMap::new(),
            max_chunk,
            cleanup_handles: Vec::new(),
        })
    }

    /// Create a new ExecutionContext from an existing shared RelaySwitch.
    ///
    /// This is used for concurrent DAG executions that share the same infrastructure.
    /// Each context has its own isolated node_data.
    pub async fn from_switch(switch: Arc<RelaySwitch>) -> Result<Self, ExecutionError> {
        let max_chunk = switch.limits().await.max_chunk as usize;
        let max_chunk = if max_chunk == 0 {
            DEFAULT_MAX_CHUNK as usize
        } else {
            max_chunk
        };

        Ok(Self {
            switch,
            node_data: HashMap::new(),
            node_live_reference: HashMap::new(),
            node_meta: HashMap::new(),
            node_is_sequence: HashMap::new(),
            node_spool: HashMap::new(),
            max_chunk,
            cleanup_handles: Vec::new(),
        })
    }

    /// Get the shared RelaySwitch.
    pub fn switch(&self) -> &Arc<RelaySwitch> {
        &self.switch
    }

    /// Add a master connection from an externally managed socket.
    ///
    /// `id` is the stable identity of the cardinality slot — see
    /// [`crate::bifaci::RelaySwitch::add_master`] for the contract
    /// (reattach on reconnect, cardinality enforcement).
    ///
    /// The caller is responsible for the lifecycle of the connected endpoint
    /// (e.g., an InProcessCartridgeHost or external cartridge connection).
    ///
    /// Returns the master index on success.
    pub async fn add_master(
        &mut self,
        id: impl Into<String>,
        socket: UnixStream,
    ) -> Result<usize, ExecutionError> {
        let idx = self
            .switch
            .add_master(id, socket)
            .await
            .map_err(|e| ExecutionError::HostError(format!("add_master: {}", e)))?;

        self.update_max_chunk().await;
        Ok(idx)
    }

    /// Add a CartridgeHostRuntime as a master, spawning all required infrastructure.
    ///
    /// This creates:
    /// - CartridgeHostRuntime (async, in tokio task)
    /// - RelaySlave (async, in tokio task)
    /// - Socket pairs connecting them to the switch
    ///
    /// The ExecutionContext manages cleanup of these resources.
    pub async fn add_cartridge_host(
        &mut self,
        cartridges: Vec<(
            PathBuf,
            Option<(
                String,
                String,
                crate::bifaci::cartridge_repo::CartridgeChannel,
            )>,
            Vec<crate::bifaci::manifest::CapGroup>,
        )>,
    ) -> Result<usize, ExecutionError> {
        // Create socket pairs:
        //   switch_sock <-> slave_ext_sock (switch to slave)
        //   slave_int_sock <-> host_sock (slave to host runtime)
        let (switch_sock, slave_ext_sock) = UnixStream::pair().map_err(ExecutionError::IoError)?;
        let (slave_int_sock, host_sock) = UnixStream::pair().map_err(ExecutionError::IoError)?;

        // --- CartridgeHostRuntime (async, in tokio task) ---
        // Identity comes from one of two sources:
        //
        //   1. Installed cartridges live at
        //      `.../{slug}/v{registry_version}/{channel}/{name}/{version}/{entry}`.
        //      The binary's parent dir holds cartridge.json; we open
        //      the registry folder through its stored charter and
        //      verify the charter rule (folder anchor ⇔ provenance
        //      registry_url).
        //
        //   2. Dev binaries live wherever cargo dropped them (a
        //      target directory's `release/<name>`) and have no
        //      cartridge.json. We fall back to the
        //      orchestrator's `dev_fallback_channel` and treat the
        //      registry_url as `None` (dev install). The cartridge
        //      itself reports the same via HELLO at attach time.
        //
        // We choose between these at runtime by checking for
        // cartridge.json's presence; the file is absent for dev
        // binaries (no installer wrote it) and present for installed
        // ones. Anywhere else fails hard — we never silently guess.
        let mut host = CartridgeHostRuntime::new();
        for (path, manifest_identity, cap_groups) in &cartridges {
            let version_dir = path.parent().ok_or_else(|| {
                ExecutionError::HostError(format!(
                    "cartridge binary {} has no parent directory",
                    path.display()
                ))
            })?;
            let cartridge_json_path = version_dir.join("cartridge.json");
            if cartridge_json_path.exists() {
                // Installed-cartridge path. Walk up: version → name →
                // channel → v{registry_version} → registry folder. The folder
                // is FOUR levels up from the version dir (the registry-version
                // level sits between it and the channel); it is opened through
                // its stored charter so the charter rule is enforced inside
                // read_from_dir.
                let folder_path = version_dir.ancestors().nth(4).ok_or_else(|| {
                    ExecutionError::HostError(format!(
                        "cartridge path {} is not under a valid \
                         {{slug}}/v{{registry_version}}/{{channel}}/{{name}}/{{version}}/ tree",
                        path.display()
                    ))
                })?;
                let folder = crate::bifaci::charter::RegistryFolder::open(
                    folder_path,
                    &crate::bifaci::charter::CharterTrust::from_build_constants(),
                    crate::SIGNING_ENVIRONMENT,
                    crate::bifaci::release_cert::unix_now(),
                )
                .map_err(|e| {
                    ExecutionError::HostError(format!(
                        "the registry folder of {}: {}",
                        path.display(),
                        e
                    ))
                })?;
                let provenance = crate::bifaci::cartridge_json::CartridgeJson::read_from_dir(
                    version_dir,
                    &folder,
                )
                .map_err(|e| {
                    ExecutionError::HostError(format!(
                        "reading cartridge.json for {}: {}",
                        path.display(),
                        e
                    ))
                })?;
                host.register_cartridge(
                    path,
                    &provenance.name,
                    &provenance.version,
                    provenance.channel,
                    provenance.registry_url.as_deref(),
                    cap_groups,
                );
            } else {
                // Dev binary (no cartridge.json on disk). Identity comes from the
                // manifest the cartridge reported during the pre-registration HELLO
                // probe. registry_url is None (dev install = absent registry).
                let (name, version, channel) = manifest_identity.as_ref().ok_or_else(|| {
                    ExecutionError::HostError(format!(
                        "dev binary {} has no manifest identity \
                         (discover_manifest should have populated this)",
                        path.display()
                    ))
                })?;
                host.register_cartridge(path, name, version, *channel, None, cap_groups);
            }
        }

        let (host_read, host_write) = host_sock.into_split();

        let host_handle = tokio::spawn(async move {
            if let Err(e) = host.run(host_read, host_write, || Vec::new()).await {
                tracing::error!("[CartridgeHostRuntime] Fatal: {}", e);
            }
        });

        // --- RelaySlave (async, in tokio task) ---
        let (slave_int_read, slave_int_write) = slave_int_sock.into_split();
        let slave = RelaySlave::new(
            BufReader::new(slave_int_read),
            BufWriter::new(slave_int_write),
        );

        // Initial RelayNotify advertises an empty `installed_cartridges`
        // list — the orchestrator hasn't attached any cartridges to
        // the host yet, so there is nothing real to declare. The
        // relay's `add_master` path treats an empty cap set as
        // "host present, no handler chain to probe yet" and skips
        // identity verification at this point. The RelayNotify the
        // CartridgeHostRuntime sends after spawning each cartridge
        // (with that cartridge's real cap_groups) is what triggers
        // the engine's identity probe, end-to-end through the
        // cartridge process.
        let initial_caps_json =
            serde_json::to_vec(&RelayNotifyCapabilitiesPayload::new(Vec::new()))
                .map_err(|e| ExecutionError::HostError(format!("serialize caps: {}", e)))?;

        let (slave_ext_read, slave_ext_write) = slave_ext_sock.into_split();

        let slave_handle = tokio::spawn(async move {
            if let Err(e) = slave
                .run(
                    FrameReader::new(BufReader::new(slave_ext_read)),
                    FrameWriter::new(BufWriter::new(slave_ext_write)),
                    Some((&initial_caps_json, &Limits::default())),
                )
                .await
            {
                tracing::error!("[RelaySlave] Fatal: {}", e);
            }
        });

        // --- Add to switch ---
        //
        // Synthesise a stable per-host id from the cartridges this
        // host wraps. Each call to `add_cartridge_host` creates a
        // new orchestrator-owned slot; the id distinguishes one
        // host's slot from another in logs / telemetry. For
        // single-cartridge hosts the id is the cartridge binary's
        // path; for multi-cartridge hosts it's the joined sorted
        // path list. Same input → same id, so a reconnect under
        // the same set of cartridges reattaches in place.
        let host_id: String = {
            let mut paths: Vec<String> = cartridges
                .iter()
                .map(|(path, _, _)| path.display().to_string())
                .collect();
            paths.sort();
            format!("cartridge-host:{}", paths.join("|"))
        };
        let master_idx = self
            .switch
            .add_master(host_id, switch_sock)
            .await
            .map_err(|e| ExecutionError::HostError(format!("add_master: {}", e)))?;

        // Store cleanup handles
        self.cleanup_handles.push(MasterCleanupHandle {
            task_handles: vec![host_handle, slave_handle],
        });

        self.update_max_chunk().await;
        Ok(master_idx)
    }

    /// Update max_chunk from current switch limits.
    async fn update_max_chunk(&mut self) {
        let chunk = self.switch.limits().await.max_chunk as usize;
        self.max_chunk = if chunk == 0 {
            DEFAULT_MAX_CHUNK as usize
        } else {
            chunk
        };
    }

    /// Get the current max chunk size.
    pub fn max_chunk(&self) -> usize {
        self.max_chunk
    }

    /// Get the aggregate capabilities of all connected masters.
    pub async fn capabilities(&self) -> Vec<u8> {
        self.switch.capabilities().await
    }

    /// Get the negotiated limits.
    pub async fn limits(&self) -> Limits {
        self.switch.limits().await
    }

    /// Set data for a node.
    pub fn set_node_data(&mut self, node: String, data: Vec<u8>) {
        self.node_data.insert(node, data);
    }

    /// Mark a node as carrying a live-feed reference selector (see
    /// `node_live_reference`).
    pub fn set_node_live_reference(&mut self, node: String, reference_urn: String) {
        self.node_live_reference.insert(node, reference_urn);
    }

    /// Get the full node → live-reference-urn map.
    pub fn node_live_reference(&self) -> &HashMap<String, String> {
        &self.node_live_reference
    }

    /// Mark a node's data as living in a disk spool (unbounded intermediate
    /// at a chain-split boundary); downstream feeds stream from the file.
    pub fn set_node_spool(&mut self, node: String, path: std::path::PathBuf) {
        self.node_spool.insert(node, path);
    }

    /// Get the full node → spool-path map.
    pub fn node_spool(&self) -> &HashMap<String, std::path::PathBuf> {
        &self.node_spool
    }

    /// Set stream metadata for a node (provenance context for ForEach propagation).
    pub fn set_node_meta(&mut self, node: String, meta: crate::StreamMeta) {
        self.node_meta.insert(node, meta);
    }

    /// Get stream metadata for a node.
    pub fn get_node_meta(&self, node: &str) -> Option<&crate::StreamMeta> {
        self.node_meta.get(node)
    }

    /// Get immutable reference to node_meta map.
    pub fn node_meta(&self) -> &HashMap<String, crate::StreamMeta> {
        &self.node_meta
    }

    /// Get mutable reference to node_meta map.
    pub fn node_meta_mut(&mut self) -> &mut HashMap<String, crate::StreamMeta> {
        &mut self.node_meta
    }

    /// Mark a node as holding sequence data.
    pub fn set_node_is_sequence(&mut self, node: String, is_sequence: bool) {
        self.node_is_sequence.insert(node, is_sequence);
    }

    /// Check if a node holds sequence data.
    pub fn is_node_sequence(&self, node: &str) -> bool {
        self.node_is_sequence.get(node).copied().unwrap_or(false)
    }

    /// Get the full node_is_sequence map.
    pub fn node_is_sequence(&self) -> &HashMap<String, bool> {
        &self.node_is_sequence
    }

    /// Get data for a node.
    pub fn get_node_data(&self, node: &str) -> Option<&Vec<u8>> {
        self.node_data.get(node)
    }

    /// Get immutable reference to node_data map.
    pub fn node_data(&self) -> &HashMap<String, Vec<u8>> {
        &self.node_data
    }

    /// Get mutable reference to node_data map.
    pub fn node_data_mut(&mut self) -> &mut HashMap<String, Vec<u8>> {
        &mut self.node_data
    }

    /// Consume and return the node_data map.
    pub fn into_node_data(self) -> HashMap<String, Vec<u8>> {
        // Abort all managed tasks
        for handle in self.cleanup_handles {
            for task in handle.task_handles {
                task.abort();
            }
        }
        self.node_data
    }

    /// Shut down the infrastructure and return accumulated node data.
    ///
    /// This:
    /// 1. Drops the switch reference (may or may not release the switch)
    /// 2. Aborts all managed tasks
    ///
    /// For masters added via `add_master()`, the caller is responsible for
    /// shutting down their endpoints.
    pub fn shutdown(self) -> HashMap<String, Vec<u8>> {
        self.into_node_data()
    }
}

// =============================================================================
// DAG Executor
// =============================================================================

/// Run one resolved graph on an already-built [`ExecutionContext`]. This is
/// THE segment executor, behind [`EngineRuntime::run_segment`] for every
/// backend.
///
/// The graph's steps are decomposed into pipelined chains
/// ([`decompose_step_chains`]) and the chains run in dependency order. Inside
/// a chain all invocations open up front and frames stream cap→cap live
/// (downstream consumes upstream chunks before END exists) with credit
/// flowing per hop (L9–L15). At a chain's end every output of its last step
/// that is kept — the main one, and each other output something takes — is
/// collected on its own: to disk through a writer for a persisted sink, to a
/// spool file when it turns out unbounded, in memory otherwise; an output
/// that may not fire and did not is recorded absent. A step nothing is wired
/// into is sent its argument values and END.
///
/// `execute_plan` hands this one chain at a time and runs independent chains
/// at once; a graph of several chains is run chain after chain. Either way a
/// pipelined intermediate is never materialised, so the graph given must be
/// one whose intermediates nothing else needs.
///
/// `observer` correlates each invocation's request id to its strand step for
/// the run's flow snapshots. `cancel` is the run's failure signal: a chain
/// that is running when it is raised cancels its requests as the collateral
/// of that failure and ends with [`ExecutionError::Collateral`]; one about to
/// start does not start.
///
/// [`EngineRuntime::run_segment`]: super::execute_plan::EngineRuntime::run_segment
#[allow(clippy::too_many_arguments)]
pub async fn run_dag_on_context(
    ctx: &mut ExecutionContext,
    graph: &ResolvedGraph,
    cap_arguments: &HashMap<String, Vec<(String, Vec<u8>)>>,
    progress_fn: Option<&CapProgressFn>,
    step_progress_fn: Option<&CapStepProgressFn>,
    log_fn: Option<&PipelineLogFn>,
    stall_tracker: Option<Arc<PipelineProgressTracker>>,
    writer_factory: Option<&super::stream_io::SegmentWriterFactory>,
    body_coordinate: Option<super::execute_plan::ForEachBodyCoordinate>,
    persist_sinks: &HashSet<String>,
    activity_timeout_secs: u64,
    observer: Option<&dyn super::stream_io::FlowObserver>,
    // Transient-artifact capture (engine only): root + publication hook.
    // Intermediate chain sinks are captured HERE, mid-run, the moment they
    // materialize. ForEach bodies never capture (body guts are excluded by
    // design — region items/outputs have their own surfaces).
    transient_root: Option<&std::path::Path>,
    on_transient: Option<
        &(dyn Fn(&super::transient::TransientArtifact) -> Result<(), ExecutionError> + Sync),
    >,
    cancel: Option<&RunCancel>,
) -> Result<DagOutput, ExecutionError> {
    let body_index = body_coordinate
        .as_ref()
        .map(|coordinate| coordinate.body_index);
    let steps = &graph.steps;
    let step_order = topological_sort_steps(steps)?;

    // Seed the outputs with every already-materialised node (the graph's inputs).
    let mut node_data: HashMap<String, Vec<Vec<u8>>> = ctx
        .node_data()
        .iter()
        .map(|(k, v)| (k.clone(), vec![v.clone()]))
        .collect();
    let mut node_is_sequence: HashMap<String, bool> = ctx.node_is_sequence().clone();
    let mut writer_results: HashMap<String, Vec<super::execute_plan::WriterResult>> =
        HashMap::new();
    let mut terminal_meta: HashMap<String, TerminalMeta> = HashMap::new();
    let mut absent: HashSet<String> = HashSet::new();

    if steps.is_empty() {
        return Ok(DagOutput {
            node_data,
            node_is_sequence,
            writer_results,
            terminal_meta,
            node_spool: HashMap::new(),
            absent,
        });
    }

    // A materialisation boundary is ALSO forced between two ADJACENT caps
    // that resolve to the SAME capacity-bounded cartridge: opening both
    // invocations before feeding the chain head deadlocks — the first
    // invocation owns the permit but has no input while opening the second
    // waits for that permit before the feed and relay pump exist. That is
    // the ONLY capacity case that must split: bounded caps sharing NO
    // bounded pool contend for nothing, and splitting them anyway would
    // materialise every intermediate — which an UNBOUNDED live pipeline
    // (13.2 §Reference Media) cannot survive (its intermediates must
    // stream; collection refuses unbounded, L16). The permit domain is the
    // POOL CHAIN (master + cartridge identity + pool), NOT the master
    // index: one relay slot can aggregate many cartridge processes
    // (floom-engine's external-cartridges RelaySlave), each with independent
    // pools — comparing master indices there would split every junction
    // and break live pipelines. Two same-cartridge caps in disjoint
    // bounded pools likewise share no permit and must NOT split.
    let linear_chains = decompose_step_chains(steps, &step_order, &HashSet::new());
    let mut cap_admission: HashMap<
        String,
        Vec<(crate::bifaci::request_state::PoolKey, usize)>,
    > = HashMap::new();
    let mut step_admission: Vec<Vec<(crate::bifaci::request_state::PoolKey, usize)>> =
        Vec::with_capacity(steps.len());
    for step in steps.iter() {
        let admission_chain = if let Some(cached) = cap_admission.get(&step.cap_urn) {
            cached.clone()
        } else {
            if ctx
                .switch()
                .wait_for_cap(&step.cap_urn, CAP_DISPATCH_READY_TIMEOUT)
                .await
                .is_none()
            {
                return Err(ExecutionError::HostError(format!(
                    "resolve admission capacity for cap '{}': no master advertised a cap \
                     dispatchable for this request within {}s",
                    step.cap_urn,
                    CAP_DISPATCH_READY_TIMEOUT.as_secs(),
                )));
            }
            let target = ctx
                .switch()
                .admission_target_for_cap(&step.cap_urn)
                .await
                .map_err(|error| {
                    ExecutionError::HostError(format!(
                        "resolve admission capacity for cap '{}': {}",
                        step.cap_urn, error
                    ))
                })?;
            cap_admission.insert(step.cap_urn.clone(), target.clone());
            target
        };
        step_admission.push(admission_chain);
    }
    // The declared stream shape of every step's cap (15.2 §Streaming
    // Contracts): the output contract every receipt point audits against,
    // and the main input's `streaming` flag the hop rule reads. Resolved
    // from the fabric definition the plan was built on; a cap the plan names
    // that the registry cannot produce is a plan/registry inconsistency and
    // fails here, before anything is dispatched.
    let mut cap_shapes: HashMap<String, (CapStreamShape, super::stream_io::ResponseContract)> =
        HashMap::new();
    let mut step_shapes: Vec<CapStreamShape> = Vec::with_capacity(steps.len());
    // What each step's response is held to: every output its cap declares.
    let mut step_contracts: Vec<super::stream_io::ResponseContract> =
        Vec::with_capacity(steps.len());
    for step in steps.iter() {
        let (shape, contract) = if let Some(cached) = cap_shapes.get(&step.cap_urn) {
            cached.clone()
        } else {
            let cap = ctx
                .switch()
                .fabric_registry()
                .get_cap(&step.cap_urn)
                .await
                .map_err(|e| {
                    ExecutionError::FabricRegistryError(format!(
                        "resolve stream contract for cap '{}': {}",
                        step.cap_urn, e
                    ))
                })?;
            let shape = CapStreamShape::of(&cap);
            let contract = super::stream_io::ResponseContract::of(&cap)
                .map_err(|e| ExecutionError::HostError(e.to_string()))?;
            cap_shapes.insert(step.cap_urn.clone(), (shape, contract.clone()));
            (shape, contract)
        };
        step_shapes.push(shape);
        step_contracts.push(contract);
    }
    let chains = split_chains_at_shared_bounded_pool(linear_chains, &step_admission);
    let chains = split_chains_at_streaming_boundary(chains, &step_shapes);
    let n_chains = chains.len();

    // Spool files created for unbounded intermediates this segment; consumed
    // by later chains in THIS loop, removed when the segment completes —
    // success or error (Drop), a leftover spool is temp litter, not state.
    struct SpoolCleanup(Vec<std::path::PathBuf>);
    impl Drop for SpoolCleanup {
        fn drop(&mut self) {
            for path in &self.0 {
                let _ = std::fs::remove_file(path);
            }
        }
    }
    let mut spool_files = SpoolCleanup(Vec::new());
    let mut node_spool: HashMap<String, std::path::PathBuf> = HashMap::new();

    for (ci, chain_idxs) in chains.iter().enumerate() {
        // A run that has already failed starts nothing more.
        if let Some(cause) = cancel.and_then(|cancel| cancel.cause()) {
            return Err(ExecutionError::Collateral {
                class: cause.class,
                detail: cause.detail,
            });
        }

        let chain_steps: Vec<&ResolvedStep> = chain_idxs.iter().map(|&i| &steps[i]).collect();
        let last_idx = *chain_idxs.last().expect("a chain has at least one step");
        let last = &steps[last_idx];
        let last_contract = &step_contracts[last_idx];

        // Every output of the chain's last step that is kept: its main one,
        // stored at the step's node, and each other output something takes,
        // stored at that output's node. One sink each.
        //
        // A sink persists to disk iff its node is a plan terminal AND a writer
        // factory was supplied (the engine persists; the in-memory path does
        // not). Any other sink gets a lazy disk spool: the collector engages
        // it only if the stream declares UNBOUNDED, so a materialisation
        // boundary never buffers unbounded data in memory (L16) and never
        // refuses a legitimate machine. Terminals keep the strict contract:
        // unbounded without a persisted sink is refused.
        let mut slots: Vec<SinkSlot> = Vec::with_capacity(1 + last.outputs.len());
        let slot = |node: &str, media_urn: &str, declared: usize| SinkSlot {
            node: node.to_string(),
            media_urn: media_urn.to_string(),
            declared,
            writer: match writer_factory {
                Some(f) if persist_sinks.contains(node) => Some(f(node, body_coordinate.clone())),
                _ => None,
            },
            spool: if persist_sinks.contains(node) {
                None
            } else {
                Some(super::stream_io::SpoolWriter::new(std::env::temp_dir().join(
                    format!(
                        "capdag-spool-{}-{}",
                        std::process::id(),
                        uuid::Uuid::new_v4()
                    ),
                )))
            },
        };
        if let Some(main) = last_contract.outputs().iter().position(|o| o.main) {
            slots.push(slot(&last.node, &last.out_media, main));
        }
        for output in &last.outputs {
            let urn = crate::MediaUrn::from_string(&output.output_media_urn).map_err(|e| {
                ExecutionError::HostError(format!(
                    "step '{}' takes output '{}' of cap '{}', which is not a media URN: {e}",
                    last.node, output.output_media_urn, last.cap_urn
                ))
            })?;
            let declared = last_contract
                .outputs()
                .iter()
                .position(|o| !o.main && o.urn.is_equivalent(&urn).unwrap_or(false))
                .ok_or_else(|| {
                    ExecutionError::HostError(format!(
                        "step '{}' takes output '{}', which cap '{}' does not declare beside \
                         its main one",
                        last.node, output.output_media_urn, last.cap_urn
                    ))
                })?;
            slots.push(slot(&output.node, &output.output_media_urn, declared));
        }

        let progress_base = ci as f32 / n_chains as f32;
        let progress_span = 1.0 / n_chains as f32;

        let chain_contracts: Vec<super::stream_io::ResponseContract> =
            chain_idxs.iter().map(|&i| step_contracts[i].clone()).collect();
        let chain_result = run_step_chain(
            ctx,
            &chain_steps,
            &chain_contracts,
            cap_arguments,
            progress_fn,
            progress_base,
            progress_span,
            step_progress_fn,
            log_fn,
            body_index,
            stall_tracker.clone(),
            &mut slots,
            activity_timeout_secs,
            observer,
            cancel,
        )
        .await;
        let results = match chain_result {
            Ok(v) => v,
            Err(e) => {
                // Nothing a failed chain wrote stands: a terminal's writer
                // discards what it took, and an engaged spool is removed.
                // Later chains' spools are handled by the Drop guard.
                for slot in slots {
                    if let Some(writer) = slot.writer {
                        writer.abort();
                    }
                    if let Some(sp) = slot.spool {
                        if sp.engaged() {
                            let _ = std::fs::remove_file(sp.path());
                        }
                    }
                }
                return Err(e);
            }
        };

        let capture_transients = body_coordinate.is_none();
        for (slot, result) in slots.into_iter().zip(results) {
            let SinkSlot {
                node,
                media_urn,
                writer,
                spool,
                ..
            } = slot;
            // An output that may not fire and did not: the node is absent —
            // which is not empty, and not a value of any cardinality.
            if !result.fired {
                absent.insert(node);
                continue;
            }
            let has_writer = writer.is_some();
            if let Some(w) = writer {
                writer_results.entry(node.clone()).or_default().push(w.finish());
            }

            let spool_engaged = spool.as_ref().is_some_and(|s| s.engaged());
            if spool_engaged {
                let sp = spool.expect("engaged spool exists");
                let path = sp.path().to_path_buf();
                if let Some(m) = sp.stream_meta() {
                    ctx.set_node_meta(node.clone(), m.clone());
                }
                ctx.set_node_is_sequence(node.clone(), sp.is_sequence());
                match transient_root {
                    Some(root) if capture_transients => {
                        // The spool IS the artifact: adopt it under the transient
                        // root — downstream chains and region drivers read the
                        // adopted path; the TTL reaper owns its lifetime, so it
                        // is NOT registered for segment-end deletion.
                        let artifact = super::transient::adopt_spool_as_transient(
                            root,
                            &node,
                            &media_urn,
                            &path,
                            sp.is_sequence(),
                        )?;
                        ctx.set_node_spool(node.clone(), artifact.data_path.clone());
                        node_spool.insert(node.clone(), artifact.data_path.clone());
                        if let Some(publish) = on_transient {
                            publish(&artifact)?;
                        }
                    }
                    _ => {
                        ctx.set_node_spool(node.clone(), path.clone());
                        spool_files.0.push(path.clone());
                        node_spool.insert(node.clone(), path);
                    }
                }
            }

            // Materialise this sink so later chains' heads can read it (via
            // send_step_input from node_data). When persisted to a writer there
            // are no in-memory items and nothing downstream reads it; a
            // spool-engaged sink's data is on disk and downstream feeds stream
            // from the file. Sequences re-assemble to the CBOR sequence the
            // next head will re-split.
            if !has_writer && !spool_engaged {
                // Sequence node_data is an RFC 8742 CBOR sequence of self-delimiting
                // values; `items` are the raw, unwrapped item bytes from
                // `decode_terminal_output`, so each must be re-encoded as a `CBOR::Bytes`
                // value here (scalar node_data stays raw — `send_one_stream` wraps it at
                // send). Dropping this re-encode is what broke sequence chains: raw PNG /
                // JSON bytes are not themselves CBOR.
                let bytes = if result.is_sequence {
                    crate::orchestrator::cbor_util::wrap_raw_items_as_cbor_sequence(&result.items)
                        .map_err(|e| {
                            ExecutionError::HostError(format!(
                                "materialise chain output '{node}': {e}"
                            ))
                        })?
                } else {
                    result.items.first().cloned().unwrap_or_default()
                };
                ctx.set_node_data(node.clone(), bytes);
                ctx.set_node_is_sequence(node.clone(), result.is_sequence);
                if let (Some(root), true) = (transient_root, capture_transients) {
                    // A bounded intermediate: one write, and the mid-strand node
                    // becomes inspectable while later chains still run.
                    let artifact = super::transient::capture_memory_intermediate(
                        root,
                        &node,
                        &media_urn,
                        &result.items,
                        result.is_sequence,
                    )?;
                    if let Some(publish) = on_transient {
                        publish(&artifact)?;
                    }
                }
            }

            node_is_sequence.insert(node.clone(), result.is_sequence);
            terminal_meta.insert(node.clone(), result.meta);
            // A spool-engaged sink's data lives ON DISK (`node_spool`) — the
            // in-memory `items` is only the pre-spool remainder. Recording it
            // here would present an (empty) in-memory value for a node that
            // produced a full stream, and any consumer that prefers memory over
            // spool (the ForEach region driver) would silently run on nothing.
            // One node, one truth: memory OR spool, never both.
            if !spool_engaged {
                node_data.insert(node, result.items);
            }
        }
    }

    // The segment completed: ownership of the spool files transfers to the
    // caller through `DagOutput.node_spool` (a ForEach region driver may
    // still stream from them) — disarm the error-path guard instead of
    // deleting. The caller (execute_plan) removes them at plan end.
    spool_files.0.clear();
    drop(spool_files);

    Ok(DagOutput {
        node_data,
        node_is_sequence,
        writer_results,
        terminal_meta,
        node_spool,
        absent,
    })
}

/// One output of a chain's last step that is kept: where its value is stored
/// and how it is taken as it arrives.
struct SinkSlot {
    /// The node the value is stored at.
    node: String,
    /// The output's media URN in the cap's definition.
    media_urn: String,
    /// Its index among the outputs the cap declares.
    declared: usize,
    /// Streams it to disk, when its node is a persisted plan terminal.
    writer: Option<Box<dyn super::stream_io::IncrementalWriter>>,
    /// Engaged if the stream turns out unbounded, when it is not persisted.
    spool: Option<super::stream_io::SpoolWriter>,
}

/// What a chain's last step produced on one kept output.
struct SinkResult {
    /// Decoded items: one for a single value, any number for a sequence.
    /// Empty when a writer or the spool took the stream.
    items: Vec<Vec<u8>>,
    is_sequence: bool,
    meta: TerminalMeta,
    /// False only for an optional output that did not fire.
    fired: bool,
}

/// Output of running a resolved graph on a context: every materialised node's
/// decoded output items, each node's cardinality, and — for persisted
/// terminal sinks — the writer results and per-sink terminal metadata.
pub struct DagOutput {
    /// Node id → decoded output items (one for a scalar, N for a sequence). Persisted
    /// terminal sinks carry an empty vec (their data is on disk, in `writer_results`).
    pub node_data: HashMap<String, Vec<Vec<u8>>>,
    /// Node id → whether its output is a sequence.
    pub node_is_sequence: HashMap<String, bool>,
    /// Persisted terminal sink node id → its writer result(s).
    pub writer_results: HashMap<String, Vec<super::execute_plan::WriterResult>>,
    /// Terminal sink node id → its terminal metadata (titles, final progress, …).
    pub terminal_meta: HashMap<String, TerminalMeta>,
    /// UNBOUNDED intermediates spooled to disk at chain-split boundaries
    /// (L16): node id → spool path, in the node_data byte form. The caller
    /// owns the files (a ForEach region driver streams items from them) and
    /// removes them when the plan completes.
    pub node_spool: HashMap<String, std::path::PathBuf>,
    /// The nodes of outputs that may not fire and did not. A node here is in
    /// none of the maps above: it is absent, which is not empty.
    pub absent: HashSet<String>,
}

/// Split each linear chain at the permit-deadlock boundary: between two
/// ADJACENT groups whose admission chains share a capacity-BOUNDED pool
/// (opening both invocations up front would have the second wait on the
/// pool slot the first holds while neither has input). A bounded
/// invocation owns a concrete pool slot from REQ through terminal
/// response, so it must receive its input and finish before a dependent
/// invocation contending for the same pool is acquired. Groups sharing no
/// bounded pool — different cartridges, or same-cartridge caps in
/// disjoint bounded pools — share no permit and keep streaming in one
/// live chain: the property unbounded (live-feed) pipelines depend on,
/// since only chain SINKS are collected/persisted and unbounded
/// intermediates cannot be materialised (L16).
/// The declared stream shape of a cap at its boundary (15.2 §Streaming
/// Contracts): whether its main input consumes without a length promise, and
/// the output contract (cardinality + boundedness) its emissions are audited
/// against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CapStreamShape {
    pub input_streams: bool,
    pub output: super::stream_io::OutputContract,
}

impl CapStreamShape {
    pub fn of(cap: &crate::Cap) -> Self {
        let (input_streams, _) = cap.streaming_shape();
        Self {
            input_streams,
            output: super::stream_io::OutputContract::of(cap),
        }
    }
}

/// Split chains at every hop where the producer MAY emit without a length
/// promise and the consumer does not consume without one — the streaming
/// contract boundary (15.2 §Streaming Contracts). Such a hop is never
/// pipelined: the producer runs to completion, its output is materialised
/// (spooled, when it turns out unbounded), and the consumer is fed the bounded
/// whole once the upstream ends. Forwarding an unbounded stream live into a
/// whole-value consumer can only end in the consumer's L16 refusal; deciding
/// it here, from the declarations, is what makes the same machine behave the
/// same way whatever cartridges happen to serve it.
fn split_chains_at_streaming_boundary(
    chains: Vec<Vec<usize>>,
    shapes: &[CapStreamShape],
) -> Vec<Vec<usize>> {
    let mut split = Vec::new();
    for chain in chains {
        let mut segment: Vec<usize> = Vec::new();
        for group_idx in chain {
            let contract_boundary = segment.last().is_some_and(|&prev| {
                shapes[prev].output.streaming && !shapes[group_idx].input_streams
            });
            if contract_boundary {
                split.push(std::mem::take(&mut segment));
            }
            segment.push(group_idx);
        }
        if !segment.is_empty() {
            split.push(segment);
        }
    }
    split
}

fn split_chains_at_shared_bounded_pool<K: PartialEq>(
    chains: Vec<Vec<usize>>,
    group_chain: &[Vec<(K, usize)>],
) -> Vec<Vec<usize>> {
    let mut split = Vec::new();
    for chain in chains {
        let mut segment: Vec<usize> = Vec::new();
        for group_idx in chain {
            let deadlock_boundary = segment.last().is_some_and(|&prev| {
                group_chain[prev].iter().any(|(pool, prev_capacity)| {
                    group_chain[group_idx]
                        .iter()
                        .any(|(other, capacity)| {
                            pool == other && (*prev_capacity > 0 || *capacity > 0)
                        })
                })
            });
            if deadlock_boundary {
                split.push(std::mem::take(&mut segment));
            }
            segment.push(group_idx);
        }
        if !segment.is_empty() {
            split.push(segment);
        }
    }
    split
}

/// Send an upstream CREDIT grant, tolerating exactly one failure mode: the
/// request no longer existing because it TERMINATED while the grant was being
/// prepared. Credits for a finished request are moot — failing the body for
/// one would turn the ordinary teardown race of credit-based flow control
/// into a spurious execution failure. Every other send failure is a real
/// host error and stays hard.
async fn send_upstream_credit(
    switch: &std::sync::Arc<crate::bifaci::relay_switch::RelaySwitch>,
    rid: crate::bifaci::frame::MessageId,
    stream_id: String,
    n: u64,
    context: &str,
) -> Result<(), ExecutionError> {
    let credit = crate::bifaci::frame::Frame::credit(
        rid,
        Some(stream_id),
        n,
        crate::bifaci::frame::CreditDirection::Response,
    );
    match switch.send_to_master(credit, None).await {
        Ok(()) => Ok(()),
        Err(crate::bifaci::relay_switch::RelaySwitchError::UnknownRequest(rid)) => {
            tracing::debug!(
                rid = %rid,
                context,
                "upstream grant arrived after the request terminated — moot, not an error"
            );
            Ok(())
        }
        Err(e) => Err(ExecutionError::HostError(format!("{context}: {e}"))),
    }
}

/// Execute ONE pipelined chain of steps as a live pipeline: the head's inputs
/// are read from `ctx`'s materialised node_data ([`send_step_input`] sends
/// one stream per input — none for a step nothing is wired into),
/// intermediate hops stream cap-to-cap through [`forward_frames`] with
/// per-hop credit, and every kept output of the last step is collected into
/// its sink in `slots`. Returns one [`SinkResult`] per slot. Progress is
/// scaled into `[progress_base, progress_base + progress_span]`.
#[allow(clippy::too_many_arguments)]
async fn run_step_chain(
    ctx: &ExecutionContext,
    chain: &[&ResolvedStep],
    contracts: &[super::stream_io::ResponseContract],
    cap_arguments: &HashMap<String, Vec<(String, Vec<u8>)>>,
    progress_fn: Option<&CapProgressFn>,
    progress_base: f32,
    progress_span: f32,
    step_progress_fn: Option<&CapStepProgressFn>,
    log_fn: Option<&PipelineLogFn>,
    body_index: Option<usize>,
    stall_tracker: Option<Arc<PipelineProgressTracker>>,
    slots: &mut [SinkSlot],
    activity_timeout_secs: u64,
    observer: Option<&dyn super::stream_io::FlowObserver>,
    cancel: Option<&RunCancel>,
) -> Result<Vec<SinkResult>, ExecutionError> {
    use tokio::sync::mpsc;

    let n = chain.len();

    // Per-step progress base within this chain, scaled into the chain's span.
    let step_base = |i: usize| progress_base + progress_span * (i as f32 / n as f32);
    let step_weight = progress_span / n as f32;

    let switch = ctx.switch().clone();
    let max_chunk = ctx.max_chunk();
    let initial_credit = switch.limits().await.initial_credit;

    // ── Step 1: Set up all cap invocations ──
    // Each execute_cap sends REQ and returns (rid, response_rx). Each invocation
    // gets a CreditRouter: inbound CREDIT frames on its response channel route to
    // the gates registered by whoever feeds that cap (L14).
    let mut invocations: Vec<(MessageId, mpsc::UnboundedReceiver<Frame>, String)> =
        Vec::with_capacity(n);
    let mut credit_routers: Vec<crate::bifaci::credit::CreditRouter> = Vec::with_capacity(n);
    for step in chain {
        let cap_urn = &step.cap_urn;
        // Cap registration is async: a freshly attached cartridge host's
        // RelayNotify (its real cap_groups) arrives some time after `add_master`
        // returns. Poll until some master advertises a cap this request CONFORMS
        // to (`find_master_for_cap` matches by tagged-URN dispatchability, never
        // string equality) — the synchronization point that lets dispatch route
        // correctly. On the engine's long-lived switch the cap is already present
        // and this returns on the first probe; bounded so a genuinely unprovided
        // cap surfaces as a typed error rather than hanging on execute_cap.
        if switch
            .wait_for_cap(cap_urn, CAP_DISPATCH_READY_TIMEOUT)
            .await
            .is_none()
        {
            return Err(ExecutionError::HostError(format!(
                "execute_cap '{}': no master advertised a cap dispatchable for \
                 this request within {}s — RelayNotify never arrived, the \
                 identity probe failed, or no candidate conforms to this cap",
                cap_urn,
                CAP_DISPATCH_READY_TIMEOUT.as_secs(),
            ))
            .at_step(&step.token_id));
        }
        let (rid, rx) = switch
            .execute_cap(cap_urn, vec![], "application/cbor")
            .await
            .map_err(|e| match e {
                // Preserve the switch's classification instead of flattening
                // every switch failure into `internal`.
                crate::bifaci::relay_switch::RelaySwitchError::CartridgeUnavailable(details) => {
                    ExecutionError::CartridgeUnavailable {
                        cap_urn: cap_urn.clone(),
                        details,
                    }
                }
                other => {
                    ExecutionError::HostError(format!("execute_cap '{}': {}", cap_urn, other))
                }
            })
            .map_err(|e| e.at_step(&step.token_id))?;
        // Correlate this invocation to its strand step for the run's live flow
        // snapshots (L8): the only point where both ids exist.
        if let Some(obs) = observer {
            obs.record(&rid, &step.token_id);
        }
        invocations.push((rid, rx, cap_urn.clone()));
        credit_routers.push(crate::bifaci::credit::CreditRouter::new());
    }

    // The chain HEAD is FEED-BEARING when any of its inputs is a live
    // reference: the selector we send it resolves into an open device tap in
    // the receiving cartridge. Recorded so the stop-input control can close
    // exactly these taps (non-force Cancel → close-tap → drain, 15.2 §Runs
    // Stop) without touching other requests.
    if let Some(obs) = observer {
        if chain[0]
            .source_nodes()
            .any(|node| ctx.node_live_reference().contains_key(node))
        {
            obs.record_feed_bearing(&invocations[0].0);
        }
    }

    // ── Step 2: Spawn pump task ──
    // Reads from switch.read_from_masters_timeout to route peer requests. Without
    // this, cartridge→cartridge peer calls would deadlock. The pump must exit via
    // the stop flag — never via abort() (aborting can drop a frame mid-route).
    let pump_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let pump_stop_flag = pump_stop.clone();
    let pump_switch = switch.clone();
    let pump_handle = tokio::spawn(async move {
        loop {
            if pump_stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
            match pump_switch
                .read_from_masters_timeout(std::time::Duration::from_millis(200))
                .await
            {
                Ok(Some(_frame)) => {}
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!("[pipeline pump] relay error (continuing): {}", e);
                }
            }
        }
    });

    // A chain fault channel: the FIRST task of the chain to fail (input send
    // or an intermediate forward) reports its step and error here, so the
    // terminal collect does not sit waiting for an END that will never come
    // while an upstream cap has already failed — the chain is torn down the
    // moment the originating failure is known (design: teardown before join).
    let (chain_fault_tx, mut chain_fault_rx) =
        mpsc::unbounded_channel::<(usize, ExecutionError)>();

    // ── Step 3: Feed the first step's input CONCURRENTLY (L15) ──
    let send_task = {
        let switch = switch.clone();
        let fault_tx = chain_fault_tx.clone();
        let first_rid = invocations[0].0.clone();
        let first_step = chain[0].clone();
        let cap_arguments = cap_arguments.clone();
        let node_data = ctx.node_data().clone();
        let node_meta = ctx.node_meta().clone();
        let node_is_sequence = ctx.node_is_sequence().clone();
        let node_live_reference = ctx.node_live_reference().clone();
        let node_spool = ctx.node_spool().clone();
        let router = credit_routers[0].clone();
        tokio::spawn(async move {
            let result = send_step_input(
                &switch,
                &first_rid,
                &first_step,
                &cap_arguments,
                &node_data,
                &node_meta,
                &node_is_sequence,
                &node_live_reference,
                &node_spool,
                max_chunk,
                Some((&router, initial_credit)),
            )
            .await;
            // The typed error travels on the fault channel (it is not
            // Clone); the join handle only says whether the task faulted.
            match result {
                Ok(()) => Ok(()),
                Err(e) => {
                    let _ = fault_tx.send((0, e));
                    Err(())
                }
            }
        })
    };

    // ── Step 4: Spawn forwarding tasks for intermediate steps ──
    let mut forwarding_handles: Vec<tokio::task::JoinHandle<Result<(), ()>>> = Vec::new();
    for i in 0..(n - 1) {
        let next_rid = invocations[i + 1].0.clone();
        let next = chain[i + 1];
        let prev_cap_urn = invocations[i].2.clone();
        let prev_contract = contracts[i].clone();

        // The downstream cap demuxes input streams by arg URN equivalence
        // (spec 13.2) — every stream the orchestrator sends is labeled with
        // the input's declared stream URN (`send_step_input` does this for
        // chain heads). The pipelined hop must do the same: a step inside a
        // chain has ONE input stream, its producer's main output, and
        // STREAM_START is relabeled from the producer's (richer) output URN
        // to that stream URN.
        let prev_node = &chain[i].node;
        let next_in_media = match next.inputs.as_slice() {
            [input] if input.source == StepSource::Node(prev_node.clone()) => {
                input.stream_urn.clone()
            }
            _ => {
                return Err(ExecutionError::HostError(format!(
                    "pipelined forward: chain wiring bug — step '{}' (cap '{}') is not fed \
                     by exactly the main output of '{}'",
                    next.node, next.cap_urn, prev_node
                )));
            }
        };

        let (dummy_tx, dummy_rx) = mpsc::unbounded_channel();
        let taken_rx = std::mem::replace(&mut invocations[i].1, dummy_rx);
        drop(dummy_tx);

        let fwd_switch = switch.clone();
        let extra_args: Vec<(String, Vec<u8>)> = cap_arguments
            .get(&next.node)
            .cloned()
            .unwrap_or_default();

        let step_token_id = chain[i].token_id.clone();
        let fwd_step_token_id = step_token_id.clone();
        let pfn: Option<CapProgressFn> = progress_fn.map(|parent| {
            let base = step_base(i);
            let weight = step_weight;
            let mapper = ProgressMapper::new(parent, base, weight);
            let mapper = match step_progress_fn {
                Some(sink) => mapper.with_step_sink(sink, &step_token_id),
                None => mapper,
            };
            mapper.as_cap_progress_fn()
        });
        let fwd_max_chunk = max_chunk;
        let fwd_log_fn = log_fn.cloned();
        let fwd_body_index = body_index;
        let fwd_stall_tracker = stall_tracker.clone();
        let prev_rid = invocations[i].0.clone();
        let router_up = credit_routers[i].clone();
        let router_down = credit_routers[i + 1].clone();

        let fault_tx = chain_fault_tx.clone();
        let fault_index = i;
        forwarding_handles.push(tokio::spawn(async move {
            let result = forward_frames(
                taken_rx,
                &fwd_switch,
                prev_rid,
                next_rid,
                router_up,
                router_down,
                initial_credit,
                &extra_args,
                fwd_max_chunk,
                pfn.as_ref(),
                &prev_cap_urn,
                prev_contract,
                &fwd_step_token_id,
                &next_in_media,
                fwd_log_fn.as_ref(),
                fwd_body_index,
                fwd_stall_tracker,
                activity_timeout_secs,
            )
            .await;
            match result {
                Ok(()) => Ok(()),
                Err(e) => {
                    let _ = fault_tx.send((fault_index, e));
                    Err(())
                }
            }
        }));
    }
    drop(chain_fault_tx);

    // ── Step 5: Collect the last step's outputs ──
    let last_idx = n - 1;
    let last_cap_urn_owned = invocations[last_idx].2.clone();
    let last_cap_urn = last_cap_urn_owned.as_str();
    let last_rid = invocations[last_idx].0.clone();
    let (dummy_tx2, dummy_rx2) = mpsc::unbounded_channel();
    let taken_last_rx = std::mem::replace(&mut invocations[last_idx].1, dummy_rx2);
    drop(dummy_tx2);

    let last_step_token_id = chain[last_idx].token_id.clone();
    let last_pfn: Option<CapProgressFn> = progress_fn.map(|parent| {
        let base = step_base(last_idx);
        let weight = step_weight;
        let mapper = ProgressMapper::new(parent, base, weight);
        let mapper = match step_progress_fn {
            Some(sink) => mapper.with_step_sink(sink, &last_step_token_id),
            None => mapper,
        };
        mapper.as_cap_progress_fn()
    });

    // Terminal credit plumbing: grant the last cap's output as we consume it, and
    // route its inbound CREDIT frames to the gates feeding it (L10/L14).
    let terminal_plumbing = {
        let grant_switch = switch.clone();
        let grant_rid = last_rid.clone();
        let grant: super::stream_io::CreditGrantFn = Arc::new(move |stream_id, n| {
            let switch = grant_switch.clone();
            let rid = grant_rid.clone();
            tokio::spawn(async move {
                let frame = Frame::credit(
                    rid,
                    stream_id,
                    n,
                    crate::bifaci::frame::CreditDirection::Response,
                );
                match switch.send_to_master(frame, None).await {
                    Ok(()) => {}
                    Err(crate::bifaci::relay_switch::RelaySwitchError::UnknownRequest(rid)) => {
                        // The request terminated while this grant was in
                        // flight — the ordinary teardown race, moot.
                        tracing::debug!(
                            rid = %rid,
                            "[pipeline] terminal grant after termination — moot"
                        );
                    }
                    Err(e) => {
                        // Any OTHER failure is a real host problem; a debug
                        // line would bury it. It cannot propagate from this
                        // spawned task, so it is at least loud.
                        tracing::error!("[pipeline] terminal grant failed: {}", e);
                    }
                }
            });
        });
        super::stream_io::CreditPlumbing {
            router: credit_routers[last_idx].clone(),
            grant,
            window: initial_credit,
        }
    };

    // Scoped so the collect future (which holds the sinks' `&mut`) is
    // dropped the moment the race resolves.
    let collect_result: Result<Vec<super::stream_io::CollectedOutput>, ExecutionError> = {
    let mut sinks: Vec<super::stream_io::OutputSink<'_>> = slots
        .iter_mut()
        .map(|slot| super::stream_io::OutputSink {
            output: slot.declared,
            writer: slot
                .writer
                .as_mut()
                .map(|w| w.as_mut() as &mut dyn super::stream_io::IncrementalWriter),
            spool: slot
                .spool
                .as_mut()
                .map(|s| s as &mut dyn super::stream_io::IncrementalWriter),
        })
        .collect();
    let collect = super::stream_io::collect_response(
        taken_last_rx,
        last_pfn.as_ref(),
        last_cap_urn,
        &contracts[last_idx],
        &last_step_token_id,
        log_fn,
        body_index,
        stall_tracker.as_ref(),
        &mut sinks,
        activity_timeout_secs,
        Some(&terminal_plumbing),
    );
    let map_collect_err = |e: super::stream_io::StreamIoError| match e {
        // The terminal cap failure keeps its declared identity
        // (docs/failure-taxonomy.md) — never flattened into HostError.
        super::stream_io::StreamIoError::Terminal {
            cap_urn,
            code,
            class,
            details,
            arg_urn,
        } => ExecutionError::CartridgeExecutionFailed {
            cap_urn,
            code,
            class,
            details,
            arg_urn,
        }
        .at_step(&last_step_token_id),
        // An effect-contract violation keeps its typed identity (the audit
        // fired at receipt inside collect) — never flattened into HostError.
        super::stream_io::StreamIoError::EffectContract {
            cap_urn,
            effect,
            runtime_input,
            expected,
            actual,
        } => ExecutionError::EffectContractViolation {
            cap_urn,
            effect,
            runtime_input,
            expected,
            actual,
        }
        .at_step(&last_step_token_id),
        super::stream_io::StreamIoError::StreamContract {
            cap_urn,
            declared_is_sequence,
            declared_streaming,
            emitted_is_sequence,
            emitted_unbounded,
        } => ExecutionError::StreamContractViolation {
            cap_urn,
            declared_is_sequence,
            declared_streaming,
            emitted_is_sequence,
            emitted_unbounded,
        }
        .at_step(&last_step_token_id),
        super::stream_io::StreamIoError::OutputContract { cap_urn, detail } => {
            ExecutionError::OutputContractViolation { cap_urn, detail }.at_step(&last_step_token_id)
        }
        other => ExecutionError::HostError(other.to_string()),
    };

    // The terminal collect, the chain's fault channel and the run's failure
    // signal race: whichever speaks first decides the chain. An upstream
    // failure wins immediately — the terminal is not awaited for an END that
    // cannot come — and so does a failure elsewhere in the run.
    let mut collect = std::pin::pin!(collect);
    let mut faults_open = true;
    loop {
        tokio::select! {
            biased;
            fault = chain_fault_rx.recv(), if faults_open => match fault {
                Some((i, e)) => break Err(e.at_step(&chain[i].token_id)),
                // Every upstream task finished without fault; the terminal
                // collect alone decides from here.
                None => faults_open = false,
            },
            cause = async {
                match cancel {
                    Some(cancel) => cancel.raised().await,
                    None => std::future::pending().await,
                }
            } => break Err(ExecutionError::Collateral {
                class: cause.class,
                detail: cause.detail,
            }),
            collected = &mut collect => break collected.map_err(map_collect_err),
        }
    }
    };

    // ── Step 6: Teardown BEFORE join ──
    //
    // On failure the chain is torn down FIRST: every still-live request of
    // this chain is cancelled as COLLATERAL of the failing step (the cause
    // names it), live feeds close, credit waiters release. Only then are the
    // send/forward tasks joined — they end promptly because their requests
    // are gone, so a live source upstream of a failed cap can never hold the
    // run in RUNNING. The ORIGINATING error is what this chain returns; the
    // collateral errors the joined tasks report are consequences, not causes.
    let teardown = |originating: &ExecutionError| {
        // The collateral cancel carries the ORIGINATING failure's class: the
        // cancelled steps are consequences of that failure, attributed to
        // whatever it was attributed to — never to the user, never "ours"
        // because a cancel happened. A chain stopped by a failure elsewhere in
        // the run passes that failure's cause on as it was given.
        let cause = match originating {
            ExecutionError::Collateral { class, detail } => CollateralCause {
                class: *class,
                detail: detail.clone(),
            },
            other => CollateralCause::of(other),
        };
        let reason = crate::bifaci::frame::CancelReason::collateral(cause.class, cause.detail);
        let switch = switch.clone();
        let rids: Vec<MessageId> = invocations.iter().map(|(rid, _, _)| rid.clone()).collect();
        async move {
            for rid in &rids {
                switch.cancel_request(rid, &reason).await;
            }
        }
    };
    if let Err(e) = &collect_result {
        teardown(e).await;
    }

    for ((rid, _, _), router) in invocations.iter().zip(credit_routers.iter()) {
        router.close_request(rid, "terminal");
    }
    pump_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = pump_handle.await;

    // Join: a task's own error is on the fault channel (drained below, in
    // fault order); a join failure is a panic, reported as its own error.
    let mut first_error: Option<ExecutionError> = None;
    if let Err(e) = send_task.await {
        first_error.get_or_insert(
            ExecutionError::HostError(format!("Input send task panicked: {}", e))
                .at_step(&chain[0].token_id),
        );
    }
    for (i, handle) in forwarding_handles.into_iter().enumerate() {
        if let Err(e) = handle.await {
            first_error.get_or_insert(
                ExecutionError::HostError(format!("Forwarding task {} panicked: {}", i, e))
                    .at_step(&chain[i].token_id),
            );
        }
    }
    // Every sender is gone once the tasks are joined, so this drains to None.
    while let Some((i, e)) = chain_fault_rx.recv().await {
        let e = e.at_step(&chain[i].token_id);
        if first_error.is_none() {
            first_error = Some(e);
        } else {
            tracing::debug!(error = %e, "[pipeline] further chain fault after the first");
        }
    }

    let collected = match collect_result {
        Ok(ok) => {
            if let Some(err) = first_error {
                // The chain broke upstream after the terminal had already
                // ENDed — a terminal END is not trustworthy. Tear down the
                // chain as collateral of that failure and fail hard with it.
                teardown(&err).await;
                return Err(err);
            }
            ok
        }
        Err(e) => {
            if let Some(consequence) = first_error {
                tracing::debug!(
                    originating = %e,
                    consequence = %consequence,
                    "[pipeline] chain torn down; joined task reported a consequence of the originating failure"
                );
            }
            return Err(e);
        }
    };

    if let Some(pfn) = &progress_fn {
        pfn(progress_base + progress_span, last_cap_urn, "Completed");
    }

    // ── Step 7: Decode each kept output ──
    //
    // A writer present (or the spool engaged) ⇒ the data is on disk, no
    // in-memory items. An output that did not fire has none either. Otherwise
    // decode the accumulated bytes (CBOR transport stripped).
    let mut results: Vec<SinkResult> = Vec::with_capacity(slots.len());
    for (slot, output) in slots.iter().zip(collected) {
        let on_disk = slot.writer.is_some() || slot.spool.as_ref().is_some_and(|s| s.engaged());
        let items = if on_disk || !output.fired {
            Vec::new()
        } else {
            super::stream_io::decode_terminal_output(&output.bytes, output.is_sequence)
                .map_err(|e| ExecutionError::HostError(e.to_string()).at_step(&last_step_token_id))?
        };
        results.push(SinkResult {
            items,
            is_sequence: output.is_sequence.unwrap_or(false),
            meta: output.meta,
            fired: output.fired,
        });
    }
    Ok(results)
}

/// Send a step's input frames from `node_data`: one named STREAM per input,
/// then the step's argument values, then END. A step nothing is wired into
/// is sent its argument values and END. Provenance meta on a source node is
/// forwarded on STREAM_START. Every source node MUST carry a sequence flag —
/// a missing flag is a wiring bug and fails hard.
#[allow(clippy::too_many_arguments)]
async fn send_step_input(
    switch: &Arc<RelaySwitch>,
    rid: &MessageId,
    step: &ResolvedStep,
    cap_arguments: &HashMap<String, Vec<(String, Vec<u8>)>>,
    node_data: &HashMap<String, Vec<u8>>,
    node_meta: &HashMap<String, crate::StreamMeta>,
    node_is_sequence: &HashMap<String, bool>,
    node_live_reference: &HashMap<String, String>,
    node_spool: &HashMap<String, std::path::PathBuf>,
    max_chunk: usize,
    credit: Option<(&crate::bifaci::credit::CreditRouter, u64)>,
) -> Result<(), ExecutionError> {
    /// Where a node's value is read from.
    enum Held<'a> {
        Mem(&'a [u8]),
        /// The value lives in a disk spool (an unbounded intermediate at a
        /// materialisation boundary) — streamed, never loaded.
        Spool(&'a std::path::Path),
    }
    let held = |node: &'_ str| -> Result<(Held<'_>, bool), ExecutionError> {
        let data = match node_spool.get(node) {
            Some(path) => Held::Spool(path.as_path()),
            None => Held::Mem(
                node_data
                    .get(node)
                    .ok_or_else(|| {
                        ExecutionError::HostError(format!(
                            "Missing input data at node '{}' for cap '{}'",
                            node, step.cap_urn
                        ))
                    })?
                    .as_slice(),
            ),
        };
        let is_sequence = *node_is_sequence.get(node).ok_or_else(|| {
            ExecutionError::HostError(format!(
                "Missing sequence flag at node '{}' for cap '{}'. Either the input \
                 map was constructed without a flag for this node, or an intermediate \
                 cap completed without setting its sequence flag — both are bugs.",
                node, step.cap_urn,
            ))
        })?;
        Ok((data, is_sequence))
    };

    let extra_args = cap_arguments
        .get(&step.node)
        .map(|v| v.as_slice())
        .unwrap_or(&[]);

    // The cartridge tells its inputs apart by stream URN (tagged-URN
    // equivalence), so exactly ONE stream per distinct URN may go out: an
    // input stream, a live reference and an argument value never share one.
    let mut sent_urns: Vec<crate::MediaUrn> = Vec::new();
    let mut claim = |urn_str: &str, what: &str| -> Result<(), ExecutionError> {
        let urn = crate::MediaUrn::from_string(urn_str).map_err(|e| {
            ExecutionError::HostError(format!(
                "{what} URN '{urn_str}' for cap '{}' is not a valid media URN: {e}",
                step.cap_urn
            ))
        })?;
        if sent_urns
            .iter()
            .any(|sent| sent.is_equivalent(&urn).unwrap_or(false))
        {
            return Err(ExecutionError::HostError(format!(
                "cap '{}' would receive two input streams with the same URN '{urn_str}'; \
                 each stream carries a distinct URN — a cap has one main input, each arg \
                 one stream, and an arg a node is wired into takes no value beside it",
                step.cap_urn
            )));
        }
        sent_urns.push(urn);
        Ok(())
    };

    for input in &step.inputs {
        match &input.source {
            StepSource::Node(node) => {
                let (data, is_sequence) = held(node)?;
                let meta = node_meta.get(node);
                // A live-feed reference node sends its selector labeled with
                // the REFERENCE urn — never relabeled to the input's stream
                // URN — so the receiving cartridge's demux intercepts it and
                // resolves capture (13.2 §Reference Media). The reference is
                // ONE record: wire is_sequence=false regardless of the node's
                // content cardinality.
                let (stream_urn, is_sequence) = match node_live_reference.get(node) {
                    Some(reference_urn) => (reference_urn.as_str(), false),
                    None => (input.stream_urn.as_str(), is_sequence),
                };
                claim(stream_urn, "input stream")?;
                match data {
                    Held::Mem(bytes) => super::stream_io::send_one_stream(
                        switch,
                        rid,
                        stream_urn,
                        bytes,
                        meta.cloned(),
                        is_sequence,
                        max_chunk,
                        credit,
                    )
                    .await,
                    Held::Spool(path) => super::stream_io::send_file_stream(
                        switch,
                        rid,
                        stream_urn,
                        path,
                        meta.cloned(),
                        is_sequence,
                        max_chunk,
                        credit,
                    )
                    .await,
                }
                .map_err(|e| ExecutionError::HostError(e.to_string()))?;
            }
            // A gather: the members' values in order as ONE sequence. A single
            // value contributes one item, a sequence its items, and a SPOOLED
            // member (an ended unbounded intermediate) streams its data from
            // the file — a gather never re-buffers an unbounded stream into
            // memory. No members is an empty sequence.
            StepSource::Gather(nodes) => {
                claim(&input.stream_urn, "input stream")?;
                let mut members: Vec<super::stream_io::GatherMember> =
                    Vec::with_capacity(nodes.len());
                for node in nodes {
                    if node_live_reference.contains_key(node) {
                        return Err(ExecutionError::HostError(format!(
                            "cap '{}' gathers live-feed reference '{}' — a device capture \
                             is resolved by the one cap it is sent to, never gathered",
                            step.cap_urn, node
                        )));
                    }
                    let (data, is_sequence) = held(node)?;
                    members.push(match data {
                        Held::Mem(bytes) => super::stream_io::GatherMember::Memory {
                            data: bytes.to_vec(),
                            is_sequence,
                        },
                        Held::Spool(path) => super::stream_io::GatherMember::Spooled {
                            path: path.to_path_buf(),
                            is_sequence,
                        },
                    });
                }
                super::stream_io::send_gathered_stream(
                    switch,
                    rid,
                    &input.stream_urn,
                    members,
                    max_chunk,
                    credit,
                )
                .await
                .map_err(|e| ExecutionError::HostError(e.to_string()))?;
            }
        }
    }
    for (media_urn, data) in extra_args {
        claim(media_urn, "argument")?;
        super::stream_io::send_one_stream(
            switch, rid, media_urn, data, None, false, max_chunk, credit,
        )
        .await
        .map_err(|e| ExecutionError::HostError(e.to_string()))?;
    }

    let end_frame = Frame::end(rid.clone(), None);
    switch
        .send_to_master(end_frame, None)
        .await
        .map_err(|e| ExecutionError::HostError(format!("END: {}", e)))?;

    Ok(())
}

/// Forward one cap's response frames live into the next cap's input, re-stamping
/// request/stream ids for wire fidelity. STREAM_START is relabeled from the
/// producer's output URN to `next_in_media` — the downstream edge's declared
/// input arg URN — because the consumer demuxes args by URN equivalence
/// (spec 13.2); the plan proved the producer's output conforms, and that
/// conformance is re-asserted here (a non-conforming stream is a wiring bug,
/// failed hard). Credit flows both ways per hop (L10/L14):
/// forwarded chunks acquire from the downstream cap's window; consumed upstream
/// chunks are granted back. On the upstream END, the next group's extra-arg
/// streams are sent, then END. LOG frames drive progress/log callbacks. Activity
/// silence surfaces as a one-shot warning — never an abort (long caps are legit;
/// cancellation is the user's explicit path).
#[allow(clippy::too_many_arguments)]
async fn forward_frames(
    mut prev_rx: tokio::sync::mpsc::UnboundedReceiver<Frame>,
    switch: &Arc<RelaySwitch>,
    prev_rid: MessageId,
    next_rid: MessageId,
    router_up: crate::bifaci::credit::CreditRouter,
    router_down: crate::bifaci::credit::CreditRouter,
    initial_credit: u64,
    extra_args: &[(String, Vec<u8>)],
    max_chunk: usize,
    progress_fn: Option<&CapProgressFn>,
    prev_cap_urn: &str,
    prev_contract: super::stream_io::ResponseContract,
    prev_step_token_id: &StepToken,
    next_in_media: &str,
    log_fn: Option<&PipelineLogFn>,
    body_index: Option<usize>,
    stall_tracker: Option<Arc<PipelineProgressTracker>>,
    activity_timeout_secs: u64,
) -> Result<(), ExecutionError> {
    use crate::bifaci::credit::CreditGate;
    use std::time::Duration;

    let target_arg_urn = crate::MediaUrn::from_string(next_in_media).map_err(|e| {
        ExecutionError::HostError(format!(
            "pipelined forward: downstream arg URN '{}' is not a valid media URN: {}",
            next_in_media, e
        ))
    })?;

    // The producer's emission is audited against its declared effect
    // contract BEFORE the relabel below — the relabel deliberately coarsens
    // the label to the downstream declared arg URN for demux (spec 13.2),
    // so an unaudited relabel would substitute the plan's belief for the
    // cartridge's claim and mask a lying cap.
    let mut effect_audit = super::stream_io::ResponseAudit::new(prev_cap_urn, prev_contract)
        .map_err(|e| ExecutionError::HostError(format!("pipelined forward: {}", e)))?;

    // The hop carries the producer's MAIN output. A stream of another output
    // is audited and consumed here — the producer is credited for it so it
    // never stalls on a window nobody reads — and is not forwarded.
    let mut consumed_only: HashMap<String, crate::bifaci::credit::CreditWindow> = HashMap::new();

    // Per upstream stream: its downstream stream id, the gate on the downstream
    // window, and the upstream window — what has been consumed of the producer
    // and is owed back to it as grants.
    let mut stream_id_map: HashMap<String, (String, Arc<CreditGate>, crate::bifaci::credit::CreditWindow)> =
        HashMap::new();
    let mut timer = super::stream_io::ActivityTimer::new(activity_timeout_secs);
    let mut activity_warning_logged = false;

    loop {
        let frame = tokio::time::timeout(Duration::from_millis(500), prev_rx.recv()).await;

        match frame {
            Ok(Some(frame)) => {
                if let Some(ref tracker) = stall_tracker {
                    tracker.touch();
                }
                activity_warning_logged = false;
                match frame.frame_type {
                    FrameType::StreamStart => {
                        timer.touch();
                        let prev_sid = frame.stream_id.clone().unwrap_or_default();

                        // The audit FIRST, on the producer's own claim: the
                        // stream must name a declared output and carry what
                        // that output carries, in its declared shape, before
                        // anything else touches it.
                        let is_main = effect_audit
                            .audit_frame(&frame)
                            .map(|output| output.main)
                            .map_err(|e| match e {
                                super::stream_io::StreamIoError::EffectContract {
                                    cap_urn,
                                    effect,
                                    runtime_input,
                                    expected,
                                    actual,
                                } => ExecutionError::EffectContractViolation {
                                    cap_urn,
                                    effect,
                                    runtime_input,
                                    expected,
                                    actual,
                                },
                                super::stream_io::StreamIoError::StreamContract {
                                    cap_urn,
                                    declared_is_sequence,
                                    declared_streaming,
                                    emitted_is_sequence,
                                    emitted_unbounded,
                                } => ExecutionError::StreamContractViolation {
                                    cap_urn,
                                    declared_is_sequence,
                                    declared_streaming,
                                    emitted_is_sequence,
                                    emitted_unbounded,
                                },
                                super::stream_io::StreamIoError::OutputContract {
                                    cap_urn,
                                    detail,
                                } => ExecutionError::OutputContractViolation { cap_urn, detail },
                                other => ExecutionError::HostError(format!(
                                    "pipelined forward: {}",
                                    other
                                )),
                            })?;
                        if !is_main {
                            consumed_only.insert(
                                prev_sid,
                                crate::bifaci::credit::CreditWindow::new(initial_credit),
                            );
                            continue;
                        }

                        let new_sid = uuid::Uuid::new_v4().to_string();
                        let gate = Arc::new(CreditGate::new(initial_credit));
                        router_down.register(
                            next_rid.clone(),
                            Some(new_sid.clone()),
                            Arc::clone(&gate),
                        );
                        stream_id_map.insert(
                            prev_sid,
                            (
                                new_sid.clone(),
                                gate,
                                crate::bifaci::credit::CreditWindow::new(initial_credit),
                            ),
                        );

                        // Then relabel to the downstream edge's declared arg
                        // URN: the consumer matches input streams by arg-URN
                        // equivalence (spec 13.2), and the producer's audited
                        // output URN is a refinement the plan already proved
                        // conforms. Re-assert that conformance — a violation
                        // is a wiring/type bug that must surface here, not as
                        // a missing-arg error inside the cartridge. The
                        // comparison itself failing is equally hard: an
                        // unanswerable conformance question is an engine
                        // inconsistency, never a pass.
                        let produced_urn_str = frame.media_urn.as_deref().unwrap_or_default();
                        let produced_urn =
                            crate::MediaUrn::from_string(produced_urn_str).map_err(|e| {
                                ExecutionError::HostError(format!(
                                    "pipelined forward: cap '{}' emitted a stream with \
                                     invalid media URN '{}': {}",
                                    prev_cap_urn, produced_urn_str, e
                                ))
                            })?;
                        // What was produced is a value, complete in its tags; the
                        // arg it feeds is a declared type. So it is asked to
                        // SATISFY the type, not to refine it as one type refines
                        // another.
                        let conforms =
                            produced_urn.satisfies(&target_arg_urn).map_err(|e| {
                                ExecutionError::HostError(format!(
                                    "pipelined forward: cap '{}' output URN '{}' could not \
                                     be compared to downstream arg URN '{}': {}",
                                    prev_cap_urn, produced_urn_str, next_in_media, e
                                ))
                            })?;
                        if !conforms {
                            return Err(ExecutionError::HostError(format!(
                                "pipelined forward: cap '{}' output stream URN '{}' does \
                                 not conform to the downstream declared arg URN '{}'",
                                prev_cap_urn, produced_urn_str, next_in_media
                            )));
                        }

                        let mut new_frame = frame.clone();
                        new_frame.id = next_rid.clone();
                        new_frame.routing_id = None;
                        new_frame.seq = 0;
                        new_frame.stream_id = Some(new_sid);
                        new_frame.media_urn = Some(next_in_media.to_string());
                        switch.send_to_master(new_frame, None).await.map_err(|e| {
                            ExecutionError::HostError(format!("forward STREAM_START: {}", e))
                        })?;
                    }
                    FrameType::Chunk => {
                        timer.touch();
                        let prev_sid = frame.stream_id.clone().unwrap_or_default();
                        if let Some(upstream) = consumed_only.get(&prev_sid) {
                            if let Some(n) = upstream.consumed() {
                                send_upstream_credit(
                                    &switch,
                                    prev_rid.clone(),
                                    prev_sid.clone(),
                                    n,
                                    "consumed-output upstream CREDIT",
                                )
                                .await?;
                            }
                            continue;
                        }
                        let has_window = {
                            let (_, gate, _) = stream_id_map.get(&prev_sid).ok_or_else(|| {
                                ExecutionError::HostError(format!(
                                    "forward CHUNK: unknown stream_id '{}'",
                                    prev_sid
                                ))
                            })?;
                            match gate.try_acquire(1) {
                                Ok(w) => w,
                                // A closed downstream gate means the consumer's
                                // request terminated (Step 6 `close_request`) or was
                                // cancelled. By the credit contract the sender must
                                // stop; the cap's real outcome is carried by its own
                                // END/ERR (collected separately), so stopping here is
                                // success, not a forwarding failure. Turning it into an
                                // ExecutionError is what broke fan-in/fan-out chains
                                // (a cap that ends before draining its input closes the
                                // gate while the upstream is still forwarding).
                                Err(closed) => {
                                    tracing::debug!(
                                        "[pipeline] forwarding stops early: downstream credit gate closed ({})",
                                        closed.reason
                                    );
                                    return Ok(());
                                }
                            }
                        };
                        if !has_window {
                            // Flush-before-block (L10 corollary): the downstream
                            // acquire is about to wait, and the upstream producer
                            // may be stalled on exactly the sub-batch grants we hold.
                            for (flush_sid, (_, _, upstream)) in stream_id_map.iter() {
                                if let Some(n) = upstream.flush() {
                                    send_upstream_credit(
                                        &switch,
                                        prev_rid.clone(),
                                        flush_sid.clone(),
                                        n,
                                        "flush upstream CREDIT",
                                    )
                                    .await?;
                                }
                            }
                            let (_, gate, _) = stream_id_map.get(&prev_sid).ok_or_else(|| {
                                ExecutionError::HostError(format!(
                                    "forward CHUNK: unknown stream_id '{}'",
                                    prev_sid
                                ))
                            })?;
                            // Hard block on downstream credit — but never
                            // silently: this await is outside the main loop's
                            // 500ms heartbeat, so a starved credit edge here
                            // is otherwise invisible (the exact shape of the
                            // rare pipelined-chain deadlock). Surface the
                            // blocked state with its credit balance at the
                            // activity-timeout cadence while continuing to
                            // wait (L8: stalls are observable, never silent).
                            loop {
                                match tokio::time::timeout(
                                    Duration::from_secs(activity_timeout_secs.max(1)),
                                    gate.acquire(1),
                                )
                                .await
                                {
                                    Ok(Ok(())) => break,
                                    Ok(Err(closed)) => {
                                        // Downstream consumer terminated while we
                                        // waited for credit — stop forwarding
                                        // gracefully (see the try_acquire arm
                                        // above for the rationale).
                                        tracing::debug!(
                                            "[pipeline] forwarding stops early: downstream credit gate closed ({})",
                                            closed.reason
                                        );
                                        return Ok(());
                                    }
                                    Err(_elapsed) => {
                                        tracing::warn!(
                                            cap_urn = %prev_cap_urn,
                                            stream_id = %prev_sid,
                                            downstream_available = gate.available(),
                                            gate_closed = gate.is_closed(),
                                            "[pipeline] forwarder blocked on downstream credit for {}s — \
                                             the consumer is not consuming (or its grants are not arriving); \
                                             continuing to wait",
                                            activity_timeout_secs.max(1)
                                        );
                                    }
                                }
                            }
                        }
                        let (new_sid, _, upstream) =
                            stream_id_map.get(&prev_sid).ok_or_else(|| {
                                ExecutionError::HostError(format!(
                                    "forward CHUNK: unknown stream_id '{}'",
                                    prev_sid
                                ))
                            })?;

                        let mut new_frame = frame.clone();
                        new_frame.id = next_rid.clone();
                        new_frame.routing_id = None;
                        new_frame.seq = 0;
                        new_frame.stream_id = Some(new_sid.clone());
                        switch.send_to_master(new_frame, None).await.map_err(|e| {
                            ExecutionError::HostError(format!("forward CHUNK: {}", e))
                        })?;

                        if let Some(n) = upstream.consumed() {
                            send_upstream_credit(
                                &switch,
                                prev_rid.clone(),
                                prev_sid.clone(),
                                n,
                                "forward upstream CREDIT",
                            )
                            .await?;
                        }
                    }
                    FrameType::StreamEnd => {
                        timer.touch();
                        let prev_sid = frame.stream_id.as_deref().unwrap_or("");
                        if consumed_only.contains_key(prev_sid) {
                            continue;
                        }
                        let (new_sid, _, _) = stream_id_map.get(prev_sid).ok_or_else(|| {
                            ExecutionError::HostError(format!(
                                "forward STREAM_END: unknown stream_id '{}'",
                                prev_sid
                            ))
                        })?;

                        let mut new_frame = frame.clone();
                        new_frame.id = next_rid.clone();
                        new_frame.routing_id = None;
                        new_frame.seq = 0;
                        new_frame.stream_id = Some(new_sid.clone());
                        switch.send_to_master(new_frame, None).await.map_err(|e| {
                            ExecutionError::HostError(format!("forward STREAM_END: {}", e))
                        })?;
                    }
                    FrameType::Credit => {
                        router_up.grant(&frame);
                    }
                    FrameType::End => {
                        if frame.exit_code() != Some(0) {
                            let details = format!(
                                "Cap '{}' END without success: exit_code={:?}",
                                prev_cap_urn,
                                frame.exit_code()
                            );
                            if let Some(lfn) = &log_fn {
                                let mut record = PipelineLogRecord::attributed(
                                    prev_step_token_id,
                                    prev_cap_urn,
                                    "error",
                                    crate::failure::AttributionClass::Internal,
                                    &details,
                                );
                                record.body_index = body_index;
                                lfn(record);
                            }
                            return Err(ExecutionError::CartridgeExecutionFailed {
                                cap_urn: prev_cap_urn.to_string(),
                                code: None,
                                class: crate::failure::AttributionClass::Internal,
                                details,
                                arg_urn: None,
                            });
                        }
                        // A request that succeeded owes a stream for every
                        // required output.
                        effect_audit.audit_end().map_err(|e| match e {
                            super::stream_io::StreamIoError::OutputContract { cap_urn, detail } => {
                                ExecutionError::OutputContractViolation { cap_urn, detail }
                            }
                            other => ExecutionError::HostError(format!("pipelined forward: {}", other)),
                        })?;
                        let final_progress = frame.final_progress().unwrap_or(1.0) as f32;
                        if let Some(pfn) = &progress_fn {
                            pfn(
                                final_progress,
                                prev_cap_urn,
                                frame.final_message().unwrap_or(""),
                            );
                        }
                        for (media_urn, data) in extra_args {
                            super::stream_io::send_one_stream(
                                switch,
                                &next_rid,
                                media_urn,
                                data,
                                None,
                                false,
                                max_chunk,
                                Some((&router_down, initial_credit)),
                            )
                            .await
                            .map_err(|e| ExecutionError::HostError(e.to_string()))?;
                        }
                        let end_frame = Frame::end(next_rid.clone(), None);
                        switch.send_to_master(end_frame, None).await.map_err(|e| {
                            ExecutionError::HostError(format!("forward END: {}", e))
                        })?;
                        return Ok(());
                    }
                    FrameType::Log => {
                        let level = frame.log_level().ok_or_else(|| {
                            ExecutionError::HostError(format!(
                                "Cap '{}' emitted a LOG frame without required text level",
                                prev_cap_urn
                            ))
                        })?;
                        timer.handle_log_level(level);

                        if let Some(p) = frame.log_progress() {
                            let msg = frame.log_message().ok_or_else(|| {
                                ExecutionError::HostError(format!(
                                    "Cap '{}' emitted a progress LOG without required text message",
                                    prev_cap_urn
                                ))
                            })?;
                            if let Some(pfn) = &progress_fn {
                                pfn(p, prev_cap_urn, msg);
                            }
                        } else {
                            let msg = frame.log_message().ok_or_else(|| {
                                ExecutionError::HostError(format!(
                                    "Cap '{}' emitted a LOG frame without required text message",
                                    prev_cap_urn
                                ))
                            })?;
                            let class = frame.attribution_class().map_err(|error| {
                                ExecutionError::HostError(format!(
                                    "Cap '{}' emitted an invalid LOG frame: {}",
                                    prev_cap_urn, error
                                ))
                            })?;
                            let arg_urn = frame
                                .attribution_arg_urn()
                                .map_err(|error| {
                                    ExecutionError::HostError(format!(
                                        "Cap '{}' emitted an invalid LOG frame: {}",
                                        prev_cap_urn, error
                                    ))
                                })?
                                .map(str::to_string);
                            if let Some(lfn) = &log_fn {
                                let mut record = PipelineLogRecord::attributed(
                                    prev_step_token_id,
                                    prev_cap_urn,
                                    level,
                                    class,
                                    msg,
                                );
                                record.meta = frame.meta.clone();
                                record.body_index = body_index;
                                record.arg_urn = arg_urn;
                                lfn(record);
                            }
                        }
                    }
                    FrameType::Err => {
                        let class = frame.attribution_class().map_err(|error| {
                            ExecutionError::HostError(format!(
                                "Cap '{}' emitted an invalid ERR frame: {}",
                                prev_cap_urn, error
                            ))
                        })?;
                        let code = frame.error_code().ok_or_else(|| {
                            ExecutionError::HostError(format!(
                                "Cap '{}' emitted an ERR frame without required text code",
                                prev_cap_urn
                            ))
                        })?;
                        let msg = frame
                            .error_message()
                            .ok_or_else(|| {
                                ExecutionError::HostError(format!(
                                    "Cap '{}' emitted an ERR frame without required text message",
                                    prev_cap_urn
                                ))
                            })?
                            .to_string();
                        let arg_urn = frame
                            .attribution_arg_urn()
                            .map_err(|error| {
                                ExecutionError::HostError(format!(
                                    "Cap '{}' emitted an invalid ERR frame: {}",
                                    prev_cap_urn, error
                                ))
                            })?
                            .map(str::to_string);
                        if let Some(lfn) = &log_fn {
                            let mut record = PipelineLogRecord::attributed(
                                prev_step_token_id,
                                prev_cap_urn,
                                "error",
                                class,
                                &msg,
                            );
                            record.body_index = body_index;
                            record.arg_urn = arg_urn.clone();
                            lfn(record);
                        }
                        return Err(ExecutionError::CartridgeExecutionFailed {
                            cap_urn: prev_cap_urn.to_string(),
                            code: Some(code.to_string()),
                            class,
                            details: msg,
                            arg_urn,
                        });
                    }
                    _ => {}
                }
            }
            Ok(None) => {
                let msg = format!("Cap '{}' response channel closed without END", prev_cap_urn);
                if let Some(lfn) = &log_fn {
                    let mut record = PipelineLogRecord::attributed(
                        prev_step_token_id,
                        prev_cap_urn,
                        "error",
                        crate::failure::AttributionClass::Internal,
                        &msg,
                    );
                    record.body_index = body_index;
                    lfn(record);
                }
                return Err(ExecutionError::HostError(msg));
            }
            Err(_timeout) => {
                for (prev_sid, (_, _, upstream)) in stream_id_map.iter() {
                    if let Some(n) = upstream.flush() {
                        send_upstream_credit(
                            &switch,
                            prev_rid.clone(),
                            prev_sid.clone(),
                            n,
                            "flush upstream CREDIT",
                        )
                        .await?;
                    }
                }
                if timer.is_expired() && !activity_warning_logged {
                    // Credit-state dump (L8): a silent pipelined stall is a
                    // starved credit edge somewhere in the per-hop loop; the
                    // per-stream gate balance and pending upstream grants
                    // name the starved edge directly. `available` is the
                    // downstream window this forwarder can still send into;
                    // `pending_upstream_grants` are consumed-but-ungranted
                    // credits the producer is owed (should be 0 after the
                    // flush above).
                    let credit_state: Vec<String> = stream_id_map
                        .iter()
                        .map(|(prev_sid, (new_sid, gate, upstream))| {
                            format!(
                                "stream {}→{}: downstream_available={} pending_upstream_grants={} gate_closed={}",
                                prev_sid,
                                new_sid,
                                gate.available(),
                                upstream.pending(),
                                gate.is_closed(),
                            )
                        })
                        .collect();
                    let msg = format!(
                        "Cap '{}' has had no activity for {}s — continuing to wait. Use Cancel to abort. \
                         Forwarding credit state: [{}]",
                        prev_cap_urn,
                        activity_timeout_secs,
                        credit_state.join("; "),
                    );
                    if let Some(lfn) = &log_fn {
                        let mut record = PipelineLogRecord::attributed(
                            prev_step_token_id,
                            prev_cap_urn,
                            "warn",
                            crate::failure::AttributionClass::Internal,
                            &msg,
                        );
                        record.body_index = body_index;
                        lfn(record);
                    }
                    tracing::warn!(
                        cap_urn = %prev_cap_urn,
                        credit_state = ?credit_state,
                        "[cap] No activity for {}s; continuing to wait for completion or cancel",
                        activity_timeout_secs
                    );
                    activity_warning_logged = true;
                }
            }
        }
    }
}

/// The per-item activity timeout for a segment, read from the segment's terminal cap
/// metadata (`activity_timeout_secs`), falling to the documented default only when the
/// key is absent or malformed.
pub(crate) fn segment_activity_timeout(graph: &ResolvedGraph) -> u64 {
    // The terminal cap: the last step of the segment's order.
    let terminal = topological_sort_steps(&graph.steps)
        .ok()
        .and_then(|order| order.last().map(|&i| &graph.steps[i]));
    terminal
        .and_then(|step| step.cap.metadata.get(ACTIVITY_TIMEOUT_METADATA_KEY))
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(DEFAULT_ACTIVITY_TIMEOUT_SECS)
}

/// One hostable cartridge: its entry-point binary, optional installed identity
/// `(id, version, channel)` (absent for dev binaries), and the cap groups it serves.
pub(crate) type HostableCartridge = (
    PathBuf,
    Option<(
        String,
        String,
        crate::bifaci::cartridge_repo::CartridgeChannel,
    )>,
    Vec<crate::bifaci::manifest::CapGroup>,
);

/// Discover the host's BUNDLED cartridges (shipped beside the executor, e.g. the capdag
/// CLI's own `bundled-cartridges/` tree) as hostable cartridges. Uses the shared
/// `discover_cartridges`, so they pass the same identity checks the engine applies and
/// are held to the same signed bundle manifest. Each `Incompatible` entry is logged and
/// skipped (discovery already surfaced the reason). An absent directory yields an empty
/// list.
///
/// The bundle's proof is established HERE, once, from the roots this build bakes: a
/// build that trusts no roots can host no bundled cartridge, and every refusal names
/// the same reason rather than each cartridge discovering it separately.
pub(crate) async fn discover_bundled_cartridges(
    bundled_cartridges_dir: &std::path::Path,
    channel: crate::bifaci::cartridge_repo::CartridgeChannel,
    registry_url: Option<&str>,
    fabric_manifest_version: u32,
) -> Result<Vec<HostableCartridge>, ExecutionError> {
    let trust = crate::bifaci::release_cert::RegistryTrust::from_build_constants();
    let identity = crate::cartridge_discovery::DiscoveryIdentity {
        channel,
        registry_url: registry_url.map(str::to_string),
        fabric_manifest_version,
        cartridge_registry_version: crate::CARTRIDGE_REGISTRY_VERSION,
        signing_environment: crate::SIGNING_ENVIRONMENT.map(str::to_string),
        charter_trust: crate::bifaci::charter::CharterTrust::from_build_constants(),
        now_unix: crate::bifaci::release_cert::unix_now(),
        bundle: crate::bifaci::bundle_manifest::BundleProof::load(
            bundled_cartridges_dir,
            trust.as_ref(),
            crate::bifaci::release_cert::unix_now(),
        ),
    };
    let mut out = Vec::new();
    for discovered in
        crate::cartridge_discovery::discover_cartridges(bundled_cartridges_dir, &identity)
            .await
            .map_err(|e| {
                ExecutionError::HostError(format!("bundled cartridge discovery failed: {e}"))
            })?
    {
        match discovered {
            crate::cartridge_discovery::DiscoveredCartridge::Directory {
                entry_point,
                id,
                channel: cart_channel,
                version,
                cap_groups,
                ..
            } => {
                out.push((entry_point, Some((id, version, cart_channel)), cap_groups));
            }
            crate::cartridge_discovery::DiscoveredCartridge::Incompatible {
                id,
                version,
                error,
                ..
            } => {
                tracing::error!(
                    cartridge = %id, version = %version, reason = %error.message,
                    "bundled cartridge rejected at discovery — not hosted"
                );
            }
        }
    }
    Ok(out)
}

/// A bundled cartridge that declares a cap: what `capdag install` and
/// `capdag find` report for it in place of a registry download.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundledProvider {
    pub id: String,
    pub version: String,
    pub entry_point: PathBuf,
}

/// The bundled cartridges that declare `cap_urn` itself (`is_equivalent`, the
/// relation a run resolves a cap to a cartridge by). A cap the bundle provides
/// is the bundle's: a run hosts it from the bundle and never looks it up in a
/// registry. Discovery holds each one to the build's signed bundle manifest,
/// as for a run.
pub async fn bundled_providers_of(
    bundled_cartridges_dir: &std::path::Path,
    channel: crate::bifaci::cartridge_repo::CartridgeChannel,
    registry_url: Option<&str>,
    fabric_manifest_version: u32,
    cap_urn: &CapUrn,
) -> Result<Vec<BundledProvider>, ExecutionError> {
    let found = discover_bundled_cartridges(
        bundled_cartridges_dir,
        channel,
        registry_url,
        fabric_manifest_version,
    )
    .await?;
    Ok(found
        .into_iter()
        .filter(|(_, _, groups)| {
            groups
                .iter()
                .any(|group| group.caps.iter().any(|cap| cap.urn.is_equivalent(cap_urn)))
        })
        .map(|(entry_point, identity, _)| {
            let (id, version, _) = identity
                .expect("discovery gives every hostable bundled cartridge its identity");
            BundledProvider { id, version, entry_point }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    // TEST1448: chains split ONLY at the permit-deadlock boundary — two
    // adjacent groups whose admission chains SHARE a bounded pool (the
    // pool behind the cartridge behind the master, never the master
    // index: one relay slot can aggregate many cartridges). Bounded caps
    // sharing no pool keep streaming in one live chain (the property
    // unbounded live pipelines depend on: only chain sinks are collected,
    // and unbounded intermediates cannot be materialised, L16).
    #[test]
    fn test1448_chain_split_only_at_shared_bounded_pool_adjacency() {
        // encode(audiocartridge) → transcribe(candle) → generate(candle),
        // both candle caps in the shared bounded "gpu" pool: the electron
        // shape — ONE aggregated master slot, THREE caps, TWO cartridges.
        // The only boundary is between the two candle caps;
        // encode|transcribe streams.
        let chains = vec![vec![0, 1, 2]];
        let group_chain = vec![
            vec![("audio/encode", 0), ("audio/all", 0)],
            vec![("candle/transcribe", 0), ("candle/gpu", 1), ("candle/all", 0)],
            vec![("candle/generate", 0), ("candle/gpu", 1), ("candle/all", 0)],
        ];
        let split = super::split_chains_at_shared_bounded_pool(chains, &group_chain);
        assert_eq!(
            split,
            vec![vec![0, 1], vec![2]],
            "split exactly at the shared bounded-pool junction"
        );

        // Different cartridges everywhere — ONE live chain, no
        // materialisation, even with a bounded singleton in the middle.
        let chains = vec![vec![0, 1, 2]];
        let group_chain = vec![
            vec![("a/x", 0), ("a/all", 0)],
            vec![("b/y", 3), ("b/all", 0)],
            vec![("c/z", 0), ("c/all", 0)],
        ];
        let split = super::split_chains_at_shared_bounded_pool(chains, &group_chain);
        assert_eq!(split, vec![vec![0, 1, 2]], "different cartridges never split");

        // Same cartridge but every shared pool UNBOUNDED: no permit to
        // contend — stays one chain.
        let chains = vec![vec![0, 1]];
        let group_chain = vec![
            vec![("d/x", 0), ("d/all", 0)],
            vec![("d/y", 0), ("d/all", 0)],
        ];
        let split = super::split_chains_at_shared_bounded_pool(chains, &group_chain);
        assert_eq!(split, vec![vec![0, 1]], "unbounded same-cartridge streams");

        // Same cartridge, both caps bounded, but in DISJOINT bounded pools
        // (cpu vs gpu) with `all` unbounded: no shared permit — the pool
        // refinement over the old whole-cartridge domain. Streams.
        let chains = vec![vec![0, 1]];
        let group_chain = vec![
            vec![("e/x", 1), ("e/cpu", 2), ("e/all", 0)],
            vec![("e/y", 1), ("e/gpu", 1), ("e/all", 0)],
        ];
        let split = super::split_chains_at_shared_bounded_pool(chains, &group_chain);
        assert_eq!(
            split,
            vec![vec![0, 1]],
            "disjoint bounded pools on one cartridge stream"
        );

        // Same cap twice in a row: the bounded SINGLETON is shared — split.
        let chains = vec![vec![0, 1]];
        let group_chain = vec![
            vec![("f/x", 1), ("f/all", 0)],
            vec![("f/x", 1), ("f/all", 0)],
        ];
        let split = super::split_chains_at_shared_bounded_pool(chains, &group_chain);
        assert_eq!(split, vec![vec![0], vec![1]], "shared bounded singleton splits");
    }

    // TEST1956: the streaming-boundary split is decided by the DECLARED
    // contracts alone — the incident shape encode(streaming out) →
    // transcribe(streaming in, streaming out) → generate(whole-value in)
    // splits exactly before generate, whatever pool topology serves it; a
    // streaming producer into a streaming consumer pipelines; a bounded
    // producer into a whole-value consumer pipelines (there is nothing
    // unbounded to refuse).
    #[test]
    fn test1956_chain_split_at_declared_streaming_boundary() {
        use crate::orchestrator::stream_io::OutputContract;
        let shape = |input_streams: bool, out_streaming: bool| super::CapStreamShape {
            input_streams,
            output: OutputContract {
                is_sequence: out_streaming,
                streaming: out_streaming,
            },
        };
        let shapes = vec![shape(true, true), shape(true, true), shape(false, false)];
        let split = super::split_chains_at_streaming_boundary(vec![vec![0, 1, 2]], &shapes);
        assert_eq!(
            split,
            vec![vec![0, 1], vec![2]],
            "split exactly where a streaming producer meets a whole-value consumer"
        );

        let shapes = vec![shape(false, false), shape(false, false), shape(false, false)];
        let split = super::split_chains_at_streaming_boundary(vec![vec![0, 1, 2]], &shapes);
        assert_eq!(split, vec![vec![0, 1, 2]], "bounded hops never split");

        let shapes = vec![shape(true, true), shape(true, true), shape(true, true)];
        let split = super::split_chains_at_streaming_boundary(vec![vec![0, 1, 2]], &shapes);
        assert_eq!(split, vec![vec![0, 1, 2]], "streaming into streaming pipelines");

        // A pool split already made upstream is respected: a boundary inside
        // the second segment splits that segment only.
        let shapes = vec![shape(false, false), shape(false, true), shape(false, false)];
        let split =
            super::split_chains_at_streaming_boundary(vec![vec![0], vec![1, 2]], &shapes);
        assert_eq!(split, vec![vec![0], vec![1], vec![2]]);
    }

    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A step for chain tests: `node` runs `urn`, fed on its main input by
    /// `from` (none for a step nothing is wired into).
    fn step(node: &str, urn: &str, from: Option<&str>) -> ResolvedStep {
        let cap_urn = crate::CapUrn::from_string(urn).unwrap();
        let out_media = cap_urn.out_spec().to_string();
        let mut cap = crate::Cap::new(cap_urn.clone(), "t".to_string(), vec!["t".to_string()]);
        cap.outputs = vec![crate::cap::definition::CapOutput::new(
            out_media.clone(),
            "output",
            "out".to_string(),
        )];
        ResolvedStep {
            token_id: format!("tok-{node}").parse().unwrap(),
            node: node.to_string(),
            cap_urn: urn.to_string(),
            cap,
            inputs: from
                .map(|from| crate::orchestrator::types::StepInput {
                    stream_urn: cap_urn.in_spec().to_string(),
                    source: StepSource::Node(from.to_string()),
                })
                .into_iter()
                .collect(),
            outputs: Vec::new(),
            out_media,
            policy: crate::machine::StepPolicy::default(),
        }
    }

    const OP_A: &str = "cap:in=\"media:document;ext=pdf\";op-a;out=\"media:enc=utf-8\"";
    const OP_B: &str = "cap:in=\"media:ext=md\";op-b;out=\"media:enc=utf-8\"";
    const OP_D: &str = "cap:in=\"media:enc=utf-8\";op-d;out=\"media:enc=utf-8;ext=txt\"";
    const FOLD: &str = "cap:in=\"media:enc=utf-8\";fold;out=\"media:enc=utf-8;ext=txt\"";

    fn chains_of(steps: &[ResolvedStep], keep: &[&str]) -> Vec<Vec<String>> {
        let order = topological_sort_steps(steps).expect("acyclic");
        let keep: HashSet<String> = keep.iter().map(|s| s.to_string()).collect();
        decompose_step_chains(steps, &order, &keep)
            .into_iter()
            .map(|chain| chain.into_iter().map(|i| steps[i].node.clone()).collect())
            .collect()
    }

    // TEST1433: a step fed by a gather ALWAYS heads its own chain — a
    // pipelined hop forwards exactly one producer stream, so a gather must be
    // fed from materialised values. So does a step with a second input
    // stream. A step with one dedicated producer still chains.
    #[test]
    fn test1433_a_gather_or_a_second_input_heads_a_chain() {
        // Gather shape: in→A, in2→B, then (A, B) gathered → C.
        let mut c = step("C", FOLD, None);
        c.inputs.push(crate::orchestrator::types::StepInput {
            stream_urn: "media:enc=utf-8".to_string(),
            source: StepSource::Gather(vec!["A".to_string(), "B".to_string()]),
        });
        let steps = vec![step("A", OP_A, Some("in")), step("B", OP_B, Some("in2")), c];
        assert_eq!(
            chains_of(&steps, &[]),
            vec![vec!["A".to_string()], vec!["B".to_string()], vec!["C".to_string()]],
            "the gathered step heads its own chain, never continuing A's or B's"
        );

        // A gather of ONE member is still a gather: its value is delivered as
        // a one-item sequence, not forwarded as it streams.
        let mut d = step("D", OP_D, None);
        d.inputs.push(crate::orchestrator::types::StepInput {
            stream_urn: "media:enc=utf-8".to_string(),
            source: StepSource::Gather(vec!["A".to_string()]),
        });
        assert_eq!(
            chains_of(&[step("A", OP_A, Some("in")), d], &[]),
            vec![vec!["A".to_string()], vec!["D".to_string()]]
        );

        // Two input streams: the main input from A and an arg from a graph input.
        let mut d = step("D", OP_D, Some("A"));
        d.inputs.push(crate::orchestrator::types::StepInput {
            stream_urn: "media:context".to_string(),
            source: StepSource::Node("ctx".to_string()),
        });
        assert_eq!(
            chains_of(&[step("A", OP_A, Some("in")), d], &[]),
            vec![vec!["A".to_string()], vec!["D".to_string()]]
        );

        // Control: a dedicated single-stream consumer fuses into one chain,
        // whatever order the steps are listed in.
        let linear = vec![step("D", OP_D, Some("A")), step("A", OP_A, Some("in"))];
        assert_eq!(chains_of(&linear, &[]), vec![vec!["A".to_string(), "D".to_string()]]);
        // And a step nothing is wired into heads a chain it can be the whole of.
        assert_eq!(
            chains_of(&[step("now", "cap:in=\"media:void\";now;out=\"media:stamp\"", None)], &[]),
            vec![vec!["now".to_string()]]
        );
    }

    // TEST12758: a hop is pipelined only when nothing else needs what the
    // producer emits and nothing about the hop can change once it has begun.
    // Each of these breaks the chain A→D into two: a second reader of A; an
    // output of A other than its main one that something takes; a reader of A
    // outside the graph; a main output of A that may not fire; a policy on
    // either step.
    #[test]
    fn test12758_a_hop_is_pipelined_only_when_nothing_else_needs_it() {
        let apart = vec![vec!["A".to_string()], vec!["D".to_string()]];
        let base = || vec![step("A", OP_A, Some("in")), step("D", OP_D, Some("A"))];
        assert_eq!(chains_of(&base(), &[]), vec![vec!["A".to_string(), "D".to_string()]]);

        // A second reader of A.
        let mut steps = base();
        steps.push(step("E", FOLD, Some("A")));
        assert_eq!(
            chains_of(&steps, &[]),
            vec![vec!["A".to_string()], vec!["D".to_string()], vec!["E".to_string()]]
        );

        // Another output of A that something takes (here, nothing in the graph
        // reads it: it is kept for a reader elsewhere).
        let mut steps = base();
        steps[0].outputs.push(crate::orchestrator::types::StepOutput {
            output_media_urn: "media:rest".to_string(),
            node: "A.rest".to_string(),
        });
        assert_eq!(chains_of(&steps, &[]), apart);

        // D reads A's other output, not its main one: not a hop of A's main stream.
        let mut steps = base();
        steps[0].outputs.push(crate::orchestrator::types::StepOutput {
            output_media_urn: "media:enc=utf-8;rest".to_string(),
            node: "A.rest".to_string(),
        });
        steps[1].inputs[0].source = StepSource::Node("A.rest".to_string());
        assert_eq!(chains_of(&steps, &[]), apart);

        // Something outside the graph reads A.
        assert_eq!(chains_of(&base(), &["A"]), apart);
        // ...while keeping D, the chain's last step, changes nothing.
        assert_eq!(chains_of(&base(), &["D"]), vec![vec!["A".to_string(), "D".to_string()]]);

        // A's main output may not fire.
        let mut steps = base();
        steps[0].cap.outputs[0].optional = true;
        assert_eq!(chains_of(&steps, &[]), apart);

        // A policy on the producer, and on the consumer.
        for which in [0, 1] {
            for policy in [
                crate::machine::StepPolicy { retry: 1, on_error: crate::machine::OnError::Fail },
                crate::machine::StepPolicy { retry: 0, on_error: crate::machine::OnError::Absent },
            ] {
                let mut steps = base();
                steps[which].policy = policy;
                assert_eq!(chains_of(&steps, &[]), apart, "policy {policy:?} on step {which}");
            }
        }
    }

    // TEST12759: steps are ordered so that a step follows every step that
    // produces something it reads — its main output or another — the same way
    // every time, and a graph in which steps wait on one another is refused.
    #[test]
    fn test12759_steps_are_ordered_by_what_they_read() {
        let mut a = step("A", OP_A, Some("in"));
        a.outputs.push(crate::orchestrator::types::StepOutput {
            output_media_urn: "media:enc=utf-8;rest".to_string(),
            node: "A.rest".to_string(),
        });
        // E reads A's other output; D reads A's main one; listed consumers first.
        let steps = vec![step("E", FOLD, Some("A.rest")), step("D", OP_D, Some("A")), a];
        let order = topological_sort_steps(&steps).unwrap();
        assert_eq!(order, vec![2, 0, 1]);
        assert_eq!(topological_sort_steps(&steps).unwrap(), order, "the same every time");

        let cyclic = vec![step("A", OP_D, Some("B")), step("B", OP_D, Some("A"))];
        assert!(topological_sort_steps(&cyclic).is_err());
        let own = vec![step("A", OP_D, Some("A"))];
        assert!(topological_sort_steps(&own).is_err(), "a step that reads what it produces");
    }

    // TEST12760: a failure signal is raised once — the first cause stands —
    // and whoever waits on it is told, whether they began waiting before or
    // after it was raised. A scope inside another is told of the outer
    // failure and keeps its own to itself.
    #[tokio::test]
    async fn test12760_the_first_failure_is_the_cause() {
        use crate::failure::AttributionClass;
        let cancel = RunCancel::new();
        assert_eq!(cancel.cause(), None);
        let early = {
            let cancel = cancel.clone();
            tokio::spawn(async move { cancel.raised().await })
        };
        tokio::task::yield_now().await;
        assert!(!early.is_finished(), "nothing has failed yet");

        let first = CollateralCause { class: AttributionClass::Resource, detail: "first".to_string() };
        cancel.raise(first.clone());
        cancel.raise(CollateralCause { class: AttributionClass::Input, detail: "second".to_string() });
        assert_eq!(cancel.cause(), Some(first.clone()));
        assert_eq!(early.await.unwrap(), first);
        assert_eq!(cancel.raised().await, first, "a late waiter is told at once");

        // A scope inside another: raised when the outer one is — with the
        // outer cause — and raised on its own without raising the outer.
        let outer = RunCancel::new();
        let inner = outer.child();
        let sibling = outer.child();
        let inner_cause = CollateralCause { class: AttributionClass::Internal, detail: "inner".to_string() };
        inner.raise(inner_cause.clone());
        assert_eq!(inner.cause(), Some(inner_cause.clone()));
        assert_eq!(inner.raised().await, inner_cause);
        assert_eq!(outer.cause(), None, "a failure inside does not fail what is around it");
        assert_eq!(sibling.cause(), None, "nor what is beside it");
        let waiting = {
            let sibling = sibling.clone();
            tokio::spawn(async move { sibling.raised().await })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        let outer_cause = CollateralCause { class: AttributionClass::Environment, detail: "outer".to_string() };
        outer.raise(outer_cause.clone());
        assert_eq!(waiting.await.unwrap(), outer_cause);
        assert_eq!(sibling.cause(), Some(outer_cause.clone()));
        assert_eq!(
            inner.cause(),
            Some(outer_cause.clone()),
            "what is inside a failed scope is that failure's collateral"
        );
        assert_eq!(outer.child().child().raised().await, outer_cause, "at any depth");

        // What a failure is the cause of: its class, its step and its reason.
        let failure = ExecutionError::CartridgeExecutionFailed {
            cap_urn: "cap:x".to_string(),
            code: Some("E_FULL".to_string()),
            class: AttributionClass::Resource,
            details: "disk full".to_string(),
            arg_urn: None,
        }
        .at_step(&"tok-7".parse().unwrap());
        let cause = CollateralCause::of(&failure);
        assert_eq!(cause.class, AttributionClass::Resource);
        assert_eq!(cause.detail, "step tok-7 (cap:x) failed: disk full");
        // And what is stopped because of it is not a failure of its own.
        let stopped = ExecutionError::Collateral { class: cause.class, detail: cause.detail.clone() };
        assert!(stopped.is_collateral());
        assert_eq!(stopped.attribution_class(), AttributionClass::Resource);
        assert!(stopped.at_step(&"tok-9".parse().unwrap()).is_collateral());
        assert!(!failure.is_collateral());
    }

    // TEST1125: map_progress clamps child to [0.0, 1.0] and maps to [base, base+weight]
    #[test]
    fn test1125_map_progress_basic_mapping() {
        // Identity mapping: base=0, weight=1
        assert_eq!(map_progress(0.0, 0.0, 1.0), 0.0);
        assert_eq!(map_progress(0.5, 0.0, 1.0), 0.5);
        assert_eq!(map_progress(1.0, 0.0, 1.0), 1.0);

        // Subdivision: base=0.2, weight=0.6 → range [0.2, 0.8]
        assert_eq!(map_progress(0.0, 0.2, 0.6), 0.2);
        assert_eq!(map_progress(0.5, 0.2, 0.6), 0.5);
        assert_eq!(map_progress(1.0, 0.2, 0.6), 0.8);

        // Clamping: values outside [0, 1] are clamped before mapping
        assert_eq!(map_progress(-0.5, 0.2, 0.6), 0.2); // clamp to 0 → base
        assert_eq!(map_progress(1.5, 0.2, 0.6), 0.8); // clamp to 1 → base+weight
    }

    // TEST1126: map_progress is deterministic — same inputs always produce same output
    #[test]
    fn test1126_map_progress_deterministic() {
        for i in 0..100 {
            let p = i as f32 / 100.0;
            let a = map_progress(p, 0.1, 0.8);
            let b = map_progress(p, 0.1, 0.8);
            assert_eq!(a, b, "map_progress must be deterministic for p={}", p);
        }
    }

    // TEST910: map_progress output is monotonic for monotonically increasing input
    #[test]
    fn test910_map_progress_monotonic() {
        let mut prev = map_progress(0.0, 0.1, 0.7);
        for i in 1..=100 {
            let p = i as f32 / 100.0;
            let curr = map_progress(p, 0.1, 0.7);
            assert!(
                curr >= prev,
                "map_progress must be monotonic: p={}, prev={}, curr={}",
                p,
                prev,
                curr
            );
            prev = curr;
        }
    }

    // TEST911: map_progress output is bounded within [base, base+weight]
    #[test]
    fn test911_map_progress_bounded() {
        let base = 0.15;
        let weight = 0.55;
        for i in -10..=110 {
            let p = i as f32 / 100.0;
            let result = map_progress(p, base, weight);
            assert!(
                result >= base && result <= base + weight,
                "map_progress({}, {}, {}) = {} must be in [{}, {}]",
                p,
                base,
                weight,
                result,
                base,
                base + weight
            );
        }
    }

    // TEST912: ProgressMapper correctly maps through a CapProgressFn
    #[test]
    fn test912_progress_mapper_reports_through_parent() {
        let reported = Arc::new(std::sync::Mutex::new(Vec::new()));
        let reported_clone = Arc::clone(&reported);
        let parent: CapProgressFn = Arc::new(move |p: f32, _cap: &str, msg: &str| {
            reported_clone.lock().unwrap().push((p, msg.to_string()));
        });

        let mapper = ProgressMapper::new(&parent, 0.2, 0.6);
        mapper.report(0.0, "", "start");
        mapper.report(0.5, "", "half");
        mapper.report(1.0, "", "done");

        let reports = reported.lock().unwrap();
        assert_eq!(reports.len(), 3);
        assert!((reports[0].0 - 0.2).abs() < 0.001, "0% maps to base=0.2");
        assert!((reports[1].0 - 0.5).abs() < 0.001, "50% maps to 0.5");
        assert!(
            (reports[2].0 - 0.8).abs() < 0.001,
            "100% maps to base+weight=0.8"
        );
    }

    // TEST913: ProgressMapper.as_cap_progress_fn produces same mapping
    #[test]
    fn test913_progress_mapper_as_cap_progress_fn() {
        let reported = Arc::new(std::sync::Mutex::new(Vec::new()));
        let reported_clone = Arc::clone(&reported);
        let parent: CapProgressFn = Arc::new(move |p: f32, _cap: &str, _msg: &str| {
            reported_clone.lock().unwrap().push(p);
        });

        let mapper = ProgressMapper::new(&parent, 0.1, 0.3);
        let pfn = mapper.as_cap_progress_fn();

        pfn(0.0, "", "a");
        pfn(0.5, "", "b");
        pfn(1.0, "", "c");

        let reports = reported.lock().unwrap();
        assert_eq!(reports.len(), 3);
        assert!((reports[0] - 0.1).abs() < 0.001);
        assert!((reports[1] - 0.25).abs() < 0.001);
        assert!((reports[2] - 0.4).abs() < 0.001);
    }

    // TEST914: ProgressMapper.sub_mapper chains correctly
    #[test]
    fn test914_progress_mapper_sub_mapper() {
        let reported = Arc::new(std::sync::Mutex::new(Vec::new()));
        let reported_clone = Arc::clone(&reported);
        let parent: CapProgressFn = Arc::new(move |p: f32, _cap: &str, _msg: &str| {
            reported_clone.lock().unwrap().push(p);
        });

        // Parent maps [0, 1] to [0.2, 0.8] (base=0.2, weight=0.6)
        let mapper = ProgressMapper::new(&parent, 0.2, 0.6);

        // Sub-mapper maps [0, 1] to the second half of parent's range
        // sub_base=0.5, sub_weight=0.5 → [0.2 + 0.5*0.6, 0.2 + (0.5+0.5)*0.6] = [0.5, 0.8]
        let sub = mapper.sub_mapper(0.5, 0.5);
        sub.report(0.0, "", "sub_start");
        sub.report(1.0, "", "sub_end");

        let reports = reported.lock().unwrap();
        assert_eq!(reports.len(), 2);
        assert!((reports[0] - 0.5).abs() < 0.001, "sub 0% maps to 0.5");
        assert!((reports[1] - 0.8).abs() < 0.001, "sub 100% maps to 0.8");
    }

    // TEST914b: ProgressMapper.with_step_sink emits the RAW child (the cap's own
    // fraction) to the step sink while the parent still receives the MAPPED overall.
    #[test]
    fn test914b_progress_mapper_step_sink_reports_raw_child() {
        let overall = Arc::new(std::sync::Mutex::new(Vec::new()));
        let overall_clone = Arc::clone(&overall);
        let parent: CapProgressFn = Arc::new(move |p: f32, _cap: &str, _msg: &str| {
            overall_clone.lock().unwrap().push(p);
        });
        let steps = Arc::new(std::sync::Mutex::new(Vec::new()));
        let steps_clone = Arc::clone(&steps);
        let sink: CapStepProgressFn = Arc::new(move |step: f32, cap: &str, token: &str| {
            steps_clone
                .lock()
                .unwrap()
                .push((step, cap.to_string(), token.to_string()));
        });

        // This cap occupies [0.5, 0.75] of the overall run (base=0.5, weight=0.25).
        let mapper = ProgressMapper::new(&parent, 0.5, 0.25).with_step_sink(&sink, &"tok-cap-x".parse().unwrap());
        mapper.report(0.0, "cap:x", "start");
        mapper.report(0.4, "cap:x", "mid");
        mapper.report(1.0, "cap:x", "end");
        // A sub-mapper of a step-sink mapper does NOT re-fire the sink (intra-cap
        // phase) — its child is not the cap's own progress.
        let sub = mapper.sub_mapper(0.0, 0.5);
        sub.report(1.0, "cap:x", "phase");

        let steps = steps.lock().unwrap();
        let overall = overall.lock().unwrap();
        // The sink saw the cap's OWN progress, unmapped, exactly 3 times (not from the sub).
        assert_eq!(
            steps.len(),
            3,
            "only the group mapper fires the sink; sub_mapper does not"
        );
        assert!((steps[0].0 - 0.0).abs() < 0.001);
        assert!(
            (steps[1].0 - 0.4).abs() < 0.001,
            "sink gets the raw child, not the overall"
        );
        assert!((steps[2].0 - 1.0).abs() < 0.001);
        assert_eq!(steps[1].1, "cap:x");
        // Every step report carries the reporting cap's stable identity — the key
        // that disambiguates a repeated cap URN back to its exact strand step.
        assert_eq!(steps[0].2, "tok-cap-x");
        assert_eq!(steps[1].2, "tok-cap-x");
        assert_eq!(steps[2].2, "tok-cap-x");
        // The parent still saw that child mapped into [0.5, 0.75].
        assert!(
            (overall[1] - (0.5 + 0.4 * 0.25)).abs() < 0.001,
            "parent gets the mapped overall"
        );
    }

    // TEST915: Per-group subdivision produces monotonic, bounded progress for N groups
    //
    // Uses pre-computed boundaries (same pattern as production code) to guarantee
    // monotonicity regardless of f32 rounding.
    #[test]
    fn test915_per_group_subdivision_monotonic_bounded() {
        let all_progress = Arc::new(std::sync::Mutex::new(Vec::new()));
        let all_clone = Arc::clone(&all_progress);
        let parent: CapProgressFn = Arc::new(move |p: f32, _cap: &str, _msg: &str| {
            all_clone.lock().unwrap().push(p);
        });

        let n_groups: usize = 5;
        let boundaries: Vec<f32> = (0..=n_groups).map(|i| i as f32 / n_groups as f32).collect();

        for i in 0..n_groups {
            let base = boundaries[i];
            let weight = boundaries[i + 1] - base;
            let mapper = ProgressMapper::new(&parent, base, weight);

            // Each group reports 0%, 50%, 100%
            mapper.report(0.0, "", "start");
            mapper.report(0.5, "", "half");
            mapper.report(1.0, "", "done");
        }

        let progress = all_progress.lock().unwrap();
        assert_eq!(progress.len(), 15); // 5 groups * 3 reports

        // Verify monotonicity
        for i in 1..progress.len() {
            assert!(
                progress[i] >= progress[i - 1],
                "monotonic violation at index {}: {} < {}",
                i,
                progress[i],
                progress[i - 1]
            );
        }

        // Verify bounded [0.0, 1.0]
        for (i, &p) in progress.iter().enumerate() {
            assert!(
                p >= 0.0 && p <= 1.0,
                "Progress[{}]={} must be in [0.0, 1.0]",
                i,
                p
            );
        }

        // First should be 0.0 (group 0, 0%)
        assert!((progress[0] - 0.0).abs() < 0.001);
        // Last should be 1.0 (group 4, 100%)
        assert!((progress[14] - 1.0).abs() < 0.001);
    }

    // TEST916: ForEach item subdivision produces correct, monotonic ranges
    //
    // Mirrors the production code in interpreter.rs: pre-compute item boundaries
    // from the same formula so the end of item N and the start of item N+1 are
    // the same f32 value (no divergent accumulation paths).
    #[test]
    fn test916_foreach_item_subdivision() {
        let all_progress = Arc::new(std::sync::Mutex::new(Vec::new()));
        let all_clone = Arc::clone(&all_progress);
        let parent: CapProgressFn = Arc::new(move |p: f32, _cap: &str, _msg: &str| {
            all_clone.lock().unwrap().push(p);
        });

        // ForEach: prefix [0.0, 0.05), body [0.05, 0.95), suffix [0.95, 1.0)
        let body_base = 0.05_f32;
        let body_weight = 0.90_f32;
        let item_count: usize = 4;

        // Pre-compute boundaries from a single formula — same as production code
        let item_boundaries: Vec<f32> = (0..=item_count)
            .map(|i| body_base + body_weight * (i as f32 / item_count as f32))
            .collect();

        for i in 0..item_count {
            let item_base = item_boundaries[i];
            let item_weight = item_boundaries[i + 1] - item_base;
            let mapper = ProgressMapper::new(&parent, item_base, item_weight);

            // Each item reports 0% and 100%
            mapper.report(0.0, "", "item_start");
            mapper.report(1.0, "", "item_done");
        }

        let progress = all_progress.lock().unwrap();
        assert_eq!(progress.len(), 8); // 4 items * 2 reports

        // Item 0 start: body_base = 0.05
        assert!(
            (progress[0] - 0.05).abs() < 0.01,
            "item 0 start: got {}",
            progress[0]
        );
        // Item 0 end: boundary[1] = 0.05 + 0.90 * 0.25 = 0.275
        assert!(
            (progress[1] - 0.275).abs() < 0.01,
            "item 0 end: got {}",
            progress[1]
        );
        // Item 3 end: boundary[4] = 0.05 + 0.90 * 1.0 = 0.95
        assert!(
            (progress[7] - 0.95).abs() < 0.01,
            "item 3 end: got {}",
            progress[7]
        );

        // All monotonic — this is the core invariant
        for i in 1..progress.len() {
            assert!(
                progress[i] >= progress[i - 1],
                "monotonic violation at index {}: {} < {}",
                i,
                progress[i],
                progress[i - 1]
            );
        }
    }

    // TEST917: High-frequency progress emission does not violate bounds
    // (Regression test for the deadlock scenario — verifies computation stays bounded)
    #[test]
    fn test917_high_frequency_progress_bounded() {
        let count = Arc::new(AtomicU32::new(0));
        let max_val = Arc::new(std::sync::Mutex::new(f32::MIN));
        let min_val = Arc::new(std::sync::Mutex::new(f32::MAX));

        let count_clone = Arc::clone(&count);
        let max_clone = Arc::clone(&max_val);
        let min_clone = Arc::clone(&min_val);
        let parent: CapProgressFn = Arc::new(move |p: f32, _cap: &str, _msg: &str| {
            count_clone.fetch_add(1, Ordering::Relaxed);
            let mut max = max_clone.lock().unwrap();
            if p > *max {
                *max = p;
            }
            let mut min = min_clone.lock().unwrap();
            if p < *min {
                *min = p;
            }
        });

        let mapper = ProgressMapper::new(&parent, 0.1, 0.8);

        // Simulate 100,000 rapid progress updates (like model download without throttle)
        for i in 0..100_000 {
            let p = i as f32 / 100_000.0;
            mapper.report(p, "", "downloading");
        }

        assert_eq!(count.load(Ordering::Relaxed), 100_000);
        let min = *min_val.lock().unwrap();
        let max = *max_val.lock().unwrap();
        assert!(min >= 0.1, "min {} must be >= base 0.1", min);
        assert!(max <= 0.9, "max {} must be <= base+weight 0.9", max);
    }

    // ActivityTimeout error variants removed: the runtime no longer
    // aborts a cap on activity-silence; long silences surface as
    // warnings only and the user cancels via the explicit
    // cancel-task path. The TEST918 assertion of the
    // `ExecutionError::ActivityTimeout` Display has been deleted
    // along with the variant — not a tautological removal of a real
    // test, but the deletion of an assertion about a code path that
    // no longer exists.
}
