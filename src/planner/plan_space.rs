//! The unified, configuration-driven path planner's vocabulary.
//!
//! One request type (`PlanRequest`) parameterizes the ENTIRE space of machine
//! topologies the cap category and tagged-URN order theory permit — from today's
//! single-source linear transmute to heterogeneous multi-source convergence,
//! multi-target divergence, and general DAGs. See
//! `docs/planner-configuration-space.md` for the theory; this module is the
//! concrete surface.
//!
//! ## The picture
//!
//! A plan connects a **source multiset** to a **target set**. Convergence (fan-in)
//! is a cospan whose apex slides along the strand: at the source end it degenerates
//! to a single (possibly generalized) source — today's default; at the target end
//! it is fully-independent legs meeting only at the target; absent, it is
//! independent parallel paths (a map). Divergence (fan-out) is the dual span.
//!
//! Every field below is a knob. `Auto` fills unset knobs by intent-inference and
//! returns MULTIPLE ranked candidates (top = the "magic" default); `Configured`
//! honours the caller's knobs. `|sources| == 1` with the default policies
//! reproduces the current single-source strands exactly.

use crate::urn::media_urn::MediaUrn;

// =============================================================================
// REQUEST — the source/target shape and every configuration knob (axes A–M)
// =============================================================================

/// Cardinality of a source anchor (axis A): one item, or a sequence of items of
/// the same media type. This is the wire-protocol `is_sequence`, surfaced as a
/// planner input so a set of N same-typed files enters as one sequence anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceCardinality {
    /// A single item of the media type.
    Single,
    /// A sequence of items of the media type (`is_sequence = true`).
    Sequence,
}

impl SourceCardinality {
    /// The wire-protocol `is_sequence` bit this cardinality carries.
    pub fn is_sequence(self) -> bool {
        matches!(self, SourceCardinality::Sequence)
    }
}

/// One source anchor of a plan (axis A). Heterogeneous inputs are simply several
/// `SourceSpec`s with different `media_urn`s; N same-typed files are one
/// `SourceSpec` with `Sequence` cardinality.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSpec {
    /// The detected media type of this source.
    pub media_urn: MediaUrn,
    /// Whether this anchor carries a sequence (axis A / cardinality).
    pub cardinality: SourceCardinality,
}

impl SourceSpec {
    pub fn single(media_urn: MediaUrn) -> Self {
        Self {
            media_urn,
            cardinality: SourceCardinality::Single,
        }
    }
    pub fn sequence(media_urn: MediaUrn) -> Self {
        Self {
            media_urn,
            cardinality: SourceCardinality::Sequence,
        }
    }
}

/// What the plan aims at (axis B).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetSpec {
    /// Plan toward these exact target media types (one ⇒ single target, many ⇒
    /// fan-out to several result types).
    Exact(Vec<MediaUrn>),
    /// Discover which targets are reachable from ALL sources under the
    /// convergence policy and return them (no full path enumeration); a chooser
    /// UI then picks one and re-plans with `Exact`. This is the multi-source
    /// generalization of "reachable targets for a dropped file set".
    Discover,
}

// --- Convergence policy (axes C, D, E, F, G) --------------------------------

/// Whether the source legs meet (axis C).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConvergencePresence {
    /// Legs meet at an apex and share a downstream tail (one combined result).
    Converged,
    /// Legs never meet — independent parallel paths (a map: each source → target).
    Independent,
    /// Let the planner infer from the source/target shape and intent.
    Auto,
}

/// Where the convergence apex sits along the strand (axis D) — the cut position
/// of the meet-in-the-middle search between forward source reachability and
/// backward target reachability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConvergenceLocation {
    /// Apex at the sources (depth 0): one cap already accepts all sources
    /// (generalization / denominator) ⇒ collapses to single-source planning.
    AtSource,
    /// Shallowest apex: legs transform as little as possible before meeting.
    Earliest,
    /// Apex at a specific leg depth.
    AtDepth(usize),
    /// Deepest apex: legs take long independent routes, meeting near the target.
    Latest,
    /// Apex at the target: fully-independent legs meet only at the final node.
    AtTarget,
    /// Let the planner choose the cut by intent (default: a shallow, cheap apex).
    Auto,
}

/// How a convergence apex is realized (axis E) — the three apex shapes the model
/// executes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConvergenceMechanism {
    /// Type-poset join (∨): one cap whose input accepts the least common
    /// generalization of the incoming types — no homogenizing transform.
    Generalize,
    /// Free-monoid / list: legs homogenized to one apex type, gathered into a
    /// sequence (Collect) consumed by one cap.
    Collect,
    /// Product: distinct typed legs feed distinct args of one multi-input cap
    /// (bipartite `conforms_to` arg-matching).
    Merge,
    /// Any admissible mechanism (planner picks per apex).
    Any,
}

/// The convergence colimit shape (axis G).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConvergenceArity {
    /// All legs meet at one apex.
    Single,
    /// Subsets meet at intermediate apexes that then meet (a join tree).
    Staged,
    /// Some legs converge; others stay independent to the target.
    Partial,
    /// Planner infers.
    Auto,
}

/// Convergence configuration (axes C, D, E, F, G).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConvergencePolicy {
    pub presence: ConvergencePresence,
    pub location: ConvergenceLocation,
    pub mechanism: ConvergenceMechanism,
    /// Pin the apex object (axis F) — plan only apexes conforming to this type.
    pub at_type: Option<MediaUrn>,
    pub arity: ConvergenceArity,
}

impl Default for ConvergencePolicy {
    fn default() -> Self {
        Self {
            presence: ConvergencePresence::Auto,
            location: ConvergenceLocation::Auto,
            mechanism: ConvergenceMechanism::Any,
            at_type: None,
            arity: ConvergenceArity::Auto,
        }
    }
}

// --- Divergence policy (axes H, I) ------------------------------------------

/// Whether/how a plan fans out (axis H).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DivergencePresence {
    /// No fan-out — a single tail to a single target.
    None,
    /// Copy one producer's output to several independent downstream tails
    /// (comonoid / tee), reaching several targets.
    Broadcast,
    /// Decompose one input into several typed streams via distinct caps on the
    /// same source, each routed onward (content decomposition).
    Split,
    /// Planner infers (e.g. from a multi-target request).
    Auto,
}

/// Where a fan-out sits (axis I) — dual of `ConvergenceLocation`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DivergenceLocation {
    AtSource,
    AtDepth(usize),
    AtTarget,
    Auto,
}

/// Divergence configuration (axes H, I).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DivergencePolicy {
    pub presence: DivergencePresence,
    pub location: DivergenceLocation,
}

impl Default for DivergencePolicy {
    fn default() -> Self {
        Self {
            presence: DivergencePresence::Auto,
            location: DivergenceLocation::Auto,
        }
    }
}

// --- Ranking, search, mode (axes K, L) --------------------------------------

/// How candidate plans are ranked (axis K). All rankings are stable and
/// deterministic; ties break by canonical notation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RankPolicy {
    /// Fewest cap steps first (then shallowest, then canonical notation).
    Shortest,
    /// By the cost model (leg + tail + apex cost), cheapest first.
    Cost,
    /// By inferred user intent — the planner's best guess at what the user meant
    /// (shape-appropriate convergence, natural intermediates, fewest surprises).
    /// This is the `Auto`-mode default and drives the "magic" preselection.
    Intent,
}

/// Which direction the apex/target search runs (axis L). `Bidirectional`
/// (meet-in-the-middle) is required for multi-source apex discovery; single
/// source degenerates to a forward search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchDirection {
    Forward,
    Backward,
    Bidirectional,
    Auto,
}

/// Who sets the knobs. Both modes return a RANKED LIST of candidates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanMode {
    /// The planner infers every unset knob from the source/target shape and
    /// returns candidates ranked by intent (top = the magic default). A singleton
    /// space collapses to one candidate — today's behaviour.
    Auto,
    /// A drill-down UI supplies knobs at multiple levels; the planner honours them
    /// and returns the ranked candidates consistent with that configuration.
    Configured,
}

/// The full plan request. Every field is a knob; `Auto` mode fills the unset ones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanRequest {
    pub sources: Vec<SourceSpec>,
    pub targets: TargetSpec,
    pub convergence: ConvergencePolicy,
    pub divergence: DivergencePolicy,
    pub ranking: RankPolicy,
    pub search: SearchDirection,
    pub mode: PlanMode,
    /// Max leg/tail depth explored (per segment).
    pub max_depth: usize,
    /// Max distinct paths enumerated per segment.
    pub max_paths: usize,
    /// Max candidate plans returned (after ranking).
    pub max_candidates: usize,
}

impl PlanRequest {
    /// Default search bounds (mirrors the historical single-source limits).
    pub const DEFAULT_MAX_DEPTH: usize = 8;
    pub const DEFAULT_MAX_PATHS: usize = 64;
    pub const DEFAULT_MAX_CANDIDATES: usize = 32;

    /// The MAGIC default: given the detected sources and a chosen target set,
    /// infer everything and return intent-ranked candidates. This is what the
    /// Finder transmute flow uses. With one source and one target it reproduces
    /// the current single-source strand enumeration.
    pub fn auto(sources: Vec<SourceSpec>, targets: TargetSpec) -> Self {
        Self {
            sources,
            targets,
            convergence: ConvergencePolicy::default(),
            divergence: DivergencePolicy::default(),
            ranking: RankPolicy::Intent,
            search: SearchDirection::Auto,
            mode: PlanMode::Auto,
            max_depth: Self::DEFAULT_MAX_DEPTH,
            max_paths: Self::DEFAULT_MAX_PATHS,
            max_candidates: Self::DEFAULT_MAX_CANDIDATES,
        }
    }

    /// The single-source, single-target preset — the exact case the legacy
    /// `find_paths_to_exact_target` served, now expressed over the unified core.
    pub fn single(
        source: SourceSpec,
        target: MediaUrn,
        max_depth: usize,
        max_paths: usize,
    ) -> Self {
        Self {
            sources: vec![source],
            targets: TargetSpec::Exact(vec![target]),
            convergence: ConvergencePolicy::default(),
            divergence: DivergencePolicy::default(),
            ranking: RankPolicy::Shortest,
            search: SearchDirection::Forward,
            mode: PlanMode::Auto,
            max_depth,
            max_paths,
            max_candidates: Self::DEFAULT_MAX_CANDIDATES,
        }
    }

    /// Discovery preset: given a source set, discover the reachable (convergent)
    /// targets without enumerating full paths — the multi-source generalization of
    /// "reachable targets for a dropped file set".
    pub fn discover(sources: Vec<SourceSpec>, max_depth: usize) -> Self {
        let mut req = Self::auto(sources, TargetSpec::Discover);
        req.max_depth = max_depth;
        req
    }

    /// True iff the request has a single source anchor — the degenerate region.
    pub fn is_single_source(&self) -> bool {
        self.sources.len() == 1
    }
}

// =============================================================================
// OUTPUT — ranked candidate plans, each a machine-notation string + its profile
// =============================================================================

/// The realized apex of a convergence, described for the UI and for ranking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanApex {
    /// The media type at which the legs meet.
    pub media_urn: MediaUrn,
    /// The mechanism actually used (never `Any` — a concrete choice).
    pub mechanism: ConvergenceMechanism,
    /// The apex depth (max leg length before the meet) — the cut position.
    pub depth: usize,
}

/// The shape of a produced candidate — its (sources, apex, targets) profile, used
/// for UI grouping/labelling and to prove `|S|=1` collapses to the linear case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanProfile {
    /// The source media types this candidate consumes (in request order).
    pub source_media: Vec<MediaUrn>,
    /// The target media types this candidate produces.
    pub target_media: Vec<MediaUrn>,
    /// The convergence apex(es), if the plan converges. Empty ⇒ independent.
    pub apexes: Vec<PlanApex>,
    /// Whether the plan converges (has ≥1 apex feeding a shared tail).
    pub converged: bool,
    /// Whether the plan fans out (a producer feeding multiple tails/targets).
    pub diverged: bool,
}

impl PlanProfile {
    /// The linear single-source/single-target profile (no apex, no divergence).
    pub fn linear(source: MediaUrn, target: MediaUrn) -> Self {
        Self {
            source_media: vec![source],
            target_media: vec![target],
            apexes: Vec::new(),
            converged: false,
            diverged: false,
        }
    }
}

/// The cost of a candidate — drives ranking and the intent score.
#[derive(Debug, Clone, PartialEq)]
pub struct PlanCost {
    /// Total cap steps across the whole DAG (excludes ForEach/Collect shape ops).
    pub cap_steps: usize,
    /// Total steps including shape transitions.
    pub total_steps: usize,
    /// The deepest leg (apex depth) — how much independent work precedes a meet.
    pub max_leg_depth: usize,
    /// Inferred-intent score in [0,1]; higher = closer to what the user likely
    /// meant. `RankPolicy::Intent` sorts by this (desc), then cost (asc).
    pub intent_score: f64,
}

/// One ranked candidate plan. `notation` is the complete, executable interchange
/// (it realizes to a `Machine`); everything else is metadata for ranking/UI.
#[derive(Debug, Clone)]
pub struct PlanCandidate {
    /// Canonical machine notation — the plan itself, ready to realize/run.
    pub notation: String,
    /// The plan's shape.
    pub profile: PlanProfile,
    /// The plan's cost.
    pub cost: PlanCost,
    /// A human label ("Combine via text → PDF", "Convert each to PNG", …).
    pub label: String,
    /// 0-based rank within the returned list (0 = the magic pick).
    pub rank: usize,
}

/// One discovered target for a source set (`TargetSpec::Discover`) — the
/// multi-source generalization of `ReachableTargetInfo`. `apex` names HOW the
/// sources can combine to reach it; `convergent == false` marks a target every
/// source can reach independently (a map), with no combined single result.
#[derive(Debug, Clone)]
pub struct ConvergentTargetInfo {
    /// The target media URN (always bookend-eligible).
    pub media_def: MediaUrn,
    /// Human-readable display name.
    pub display_name: String,
    /// Minimum total cap steps (deepest leg + fold + tail for a convergent
    /// target; per-source minimum for an independent one).
    pub min_total_steps: i32,
    /// The shallowest apex through which ALL sources combine to reach this
    /// target. `None` for an independent (map) target.
    pub apex: Option<PlanApex>,
    /// Whether a combined single result at this target exists.
    pub convergent: bool,
}

/// The result of one `plan()` call: the ranked candidates PLUS the sources the
/// planner proved unroutable (dead ends). Dead ends are FIRST-CLASS, never
/// silent: when some sources cannot reach any target within the search bounds,
/// planning continues over the routable subset and every dead end is named
/// here so clients indicate it explicitly. `dead_end_sources` is empty when
/// every source is covered by the candidates.
#[derive(Debug, Clone)]
pub struct PlanOutcome {
    /// Ranked candidates (rank 0 first). Their profiles cover exactly the
    /// routable sources.
    pub candidates: Vec<PlanCandidate>,
    /// Sources (media URNs, request order) with no route to ANY requested
    /// target within `max_depth`. Empty ⇒ full coverage.
    pub dead_end_sources: Vec<MediaUrn>,
}

/// The result of target discovery for a source set: the reachable targets
/// PLUS the sources that reach nothing (dead ends) — same fault-tolerance
/// contract as [`PlanOutcome`]: discovery continues over the routable subset
/// and names every dead end explicitly.
#[derive(Debug, Clone)]
pub struct ConvergentTargets {
    /// Discovered targets over the routable sources, convergent-first.
    pub targets: Vec<ConvergentTargetInfo>,
    /// Sources (media URNs, request order) that reach NO bookend target at
    /// all within `max_depth`. Empty ⇒ every source is routable.
    pub dead_end_sources: Vec<MediaUrn>,
}

/// Errors from planning. Fail-hard: no silent partial results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanError {
    /// The request named no sources.
    NoSources,
    /// A `Configured` request asked for something the space cannot satisfy
    /// (e.g. `Converged` but the sources share no apex reaching the target).
    Unsatisfiable(String),
    /// Locally admissible, globally inconsistent: every source individually
    /// has admissible routes (none is a dead end — those are reported in
    /// `PlanOutcome::dead_end_sources`), yet no compatible FAMILY of choices
    /// exists — the sources cannot agree on a common apex/type. In the
    /// presheaf reading of the configuration space (see
    /// docs/planner-configuration-space.md §1), each node's admissible
    /// choices are nonempty but no global section exists.
    ///
    /// This is a DIFFERENT failure from `NoPlan` ("no candidate at node v")
    /// and is reported with the conflict named, never as a generic dead
    /// end: `conflicting_sources` are the sources that fail to agree, and
    /// `restriction` says which compatibility constraint failed between
    /// them.
    GloballyInconsistent {
        /// The sources (media URNs, request order) that fail to agree.
        conflicting_sources: Vec<String>,
        /// The restriction that failed between them — which compatibility
        /// constraint admits no agreement (e.g. disjoint reach sets, no
        /// media reachable by ALL sources, or shared media that violate
        /// the convergence constraints).
        restriction: String,
    },
    /// No plan connects the sources to the target(s) under the policy.
    NoPlan { detail: String },
    /// The definition of a cap in the graph is in the registry and could not
    /// be had from it: its fetch was refused or got no answer, or it is not
    /// fetched yet. `error` is the registry's account of it — the URL, the
    /// status and the fabric manifest version where a fetch failed.
    DefinitionUnavailable { cap_urn: String, error: String },
    /// An internal invariant was violated (a bug, surfaced not swallowed).
    Internal(String),
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanError::NoSources => write!(f, "plan request has no sources"),
            PlanError::Unsatisfiable(d) => write!(f, "plan configuration is unsatisfiable: {d}"),
            PlanError::GloballyInconsistent {
                conflicting_sources,
                restriction,
            } => write!(
                f,
                "plan is locally admissible but globally inconsistent: sources [{}] cannot agree — {}",
                conflicting_sources.join(", "),
                restriction
            ),
            PlanError::NoPlan { detail } => {
                write!(f, "no plan connects the sources to the target(s): {detail}")
            }
            PlanError::DefinitionUnavailable { cap_urn, error } => write!(
                f,
                "the definition of cap '{cap_urn}' could not be had from the registry: {error}"
            ),
            PlanError::Internal(d) => write!(f, "planner internal error: {d}"),
        }
    }
}

impl std::error::Error for PlanError {}
