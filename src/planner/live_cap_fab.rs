//! LiveCapFab — Precomputed capability graph for path finding
//!
//! This module provides a live, incrementally-updated graph of capabilities
//! for efficient path finding and reachability queries. Unlike MachinePlanBuilder
//! which rebuilds the graph for each query, LiveCapFab maintains a persistent
//! graph structure that is updated when capabilities change.
//!
//! ## Design Principles
//!
//! 1. **Typed URNs**: Store MediaUrn and CapUrn directly, not strings.
//!    This avoids reparsing and provides order-theoretic methods.
//!
//! 2. **Exact matching**: For target matching, use `is_equivalent()` not `conforms_to()`.
//!    This ensures "media:X" does NOT match paths ending in "media:X;list".
//!
//! 3. **Conformance for traversal**: Use `conforms_to()` only for graph traversal
//!    (can this output feed into that input?).
//!
//! 4. **Deterministic ordering**: Results are sorted by (path_length, specificity, urn).
//!
//! 5. **Cardinality is not topology**: The `list` tag is a cardinality marker, not a
//!    type identity tag. ForEach (list→item) and Collect (item→list) are universal
//!    operations that apply to any media URN based solely on whether it has the `list`
//!    tag. They are synthesized dynamically during traversal, not stored as graph edges.
//!    Collect is the single scalar→list transition — whether wrapping 1 item or
//!    gathering N ForEach results, it is the same concept.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::cap::registry::FabricRegistry;
use crate::planner::cardinality::InputCardinality;
use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::MediaUrn;
use crate::Cap;

// =============================================================================
// DATA STRUCTURES
// =============================================================================

/// Type of edge in the capability graph.
///
/// Cap edges are stored in the graph. Cardinality transitions (ForEach, Collect)
/// are synthesized dynamically by `get_outgoing_edges()` — they are universal
/// operations derived from the `list` tag, not graph contents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveMachinePlanEdgeType {
    /// A real capability that transforms media
    Cap {
        cap_urn: CapUrn,
        cap_title: String,
        specificity: usize,
        /// Whether the cap's main input expects a sequence of items
        input_is_sequence: bool,
        /// Whether the cap's output produces a sequence of items
        output_is_sequence: bool,
    },
    /// Fan-out: iterate over list items (list → item, remove `list` tag)
    /// Synthesized for any list-typed source.
    ForEach,
    /// Collect: scalar → list (item → list, add `list` tag)
    /// The universal scalar-to-list transition. Synthesized for any scalar source.
    /// Works in two contexts: standalone (wrap scalar in list-of-one) or after
    /// ForEach (gather iteration results).
    Collect,
}

/// Event emitted during streaming path finding.
#[derive(Debug, Clone)]
pub enum PathFindingEvent {
    /// A depth level of IDDFS has completed
    DepthComplete {
        depth: usize,
        max_depth: usize,
        nodes_explored: u64,
        paths_found: usize,
    },
    /// A new path was discovered
    PathFound(Strand),
    /// Search is complete
    Complete {
        total_paths: usize,
        total_nodes_explored: u64,
    },
}

/// An edge in the live capability graph.
///
/// Stored edges represent capabilities that transform one media type to another.
/// Cardinality transitions (ForEach/Collect) are synthesized dynamically
/// and use the same struct for uniformity in path traversal.
///
/// URNs are stored as typed values, not strings, for order-theoretic operations.
///
/// Cardinality (single vs sequence) is NOT stored on edges. It is a property
/// of the data flow tracked by `is_sequence` on the wire protocol, determined
/// by context (how many input files), not by URN tags.
#[derive(Debug, Clone)]
pub struct LiveMachinePlanEdge {
    /// Input media type (what this edge consumes)
    pub from_spec: MediaUrn,
    /// Output media type (what this edge produces)
    pub to_spec: MediaUrn,
    /// Type of edge (cap or cardinality transition)
    pub edge_type: LiveMachinePlanEdgeType,
}

/// Precomputed graph of capabilities for path finding.
///
/// The graph stores only Cap edges. Cardinality transitions (ForEach, Collect)
/// are universal shape transitions synthesized dynamically by
/// `get_outgoing_edges()` during traversal based on the `is_sequence` state.
///
/// This graph is designed to be:
/// - Updated incrementally when caps change
/// - Queried efficiently for reachability and path finding
/// - Deterministic in its results
///
/// The graph's indexes are keyed on `MediaUrn` / `CapUrn`
/// directly via their derived `Hash`/`Eq` impls (which route
/// to `TaggedUrn`'s structural `(prefix, tags-BTreeMap)`
/// identity). No index key is ever a flat URN string.
#[derive(Debug)]
pub struct LiveCapFab {
    /// Cap edges only — cardinality transitions are synthesized during traversal
    edges: Vec<LiveMachinePlanEdge>,
    /// Index: from_spec → edge indices.
    outgoing: HashMap<MediaUrn, Vec<usize>>,
    /// Index: to_spec → edge indices.
    incoming: HashMap<MediaUrn, Vec<usize>>,
    /// All unique media URN nodes reachable in the graph.
    nodes: HashSet<MediaUrn>,
    /// Cap URN → edge indices for removal.
    cap_to_edges: HashMap<CapUrn, Vec<usize>>,
    /// Subset of `nodes` that are eligible to serve as a strand bookend
    /// (source or target of a transmute) — a URN has at least one file
    /// extension registered in the media registry, i.e. concrete content
    /// can actually exist as a file of that type.
    ///
    /// Computed at sync time from the registry snapshot supplied to
    /// `sync_from_caps` / `sync_from_cap_urns`. The graph is rebuilt on
    /// every cap-set change, so the bookend set tracks the live registry
    /// state exactly: new media defs with extensions added between
    /// syncs become bookends after the next sync; specs whose extensions
    /// are removed stop being bookends after the next sync.
    bookend_nodes: HashSet<MediaUrn>,
}

/// Information about a reachable target from a source media type.
#[derive(Debug, Clone)]
pub struct ReachableTargetInfo {
    /// The target media URN
    pub media_def: MediaUrn,
    /// Human-readable display name (from media registry)
    pub display_name: String,
    /// Minimum number of steps to reach this target
    pub min_path_length: i32,
    /// Number of distinct paths to this target
    pub path_count: i32,
}

impl LiveMachinePlanEdge {
    /// Get the title for this edge (for display purposes)
    pub fn title(&self) -> String {
        match &self.edge_type {
            LiveMachinePlanEdgeType::Cap { cap_title, .. } => cap_title.clone(),
            LiveMachinePlanEdgeType::ForEach => "ForEach (iterate over list)".to_string(),
            LiveMachinePlanEdgeType::Collect => "Collect (scalar to list)".to_string(),
        }
    }

    /// Get the specificity of this edge (for ordering purposes)
    pub fn specificity(&self) -> usize {
        match &self.edge_type {
            LiveMachinePlanEdgeType::Cap { specificity, .. } => *specificity,
            // Cardinality transitions have no specificity preference
            LiveMachinePlanEdgeType::ForEach | LiveMachinePlanEdgeType::Collect => 0,
        }
    }

    /// Check if this is a cap edge (not a cardinality transition)
    pub fn is_cap(&self) -> bool {
        matches!(self.edge_type, LiveMachinePlanEdgeType::Cap { .. })
    }

    /// Get the cap URN if this is a cap edge
    pub fn cap_urn(&self) -> Option<&CapUrn> {
        match &self.edge_type {
            LiveMachinePlanEdgeType::Cap { cap_urn, .. } => Some(cap_urn),
            _ => None,
        }
    }
}

/// The stable identity of one step of a resolved strand — the ONLY address by
/// which a step, or an argument value destined for it, is ever named.
///
/// This is a newtype rather than a bare `String` so that an unidentified step is
/// not a state the program can be in. There is exactly one way to make a token
/// ([`StepToken::mint`]) and exactly one way to recover one that was already
/// minted ([`StepToken::parse`], which refuses an empty id). `Deserialize` goes
/// through `parse`, so a persisted strand carrying `""` fails to load rather
/// than loading into a strand whose steps cannot be addressed.
///
/// Nothing derives a token. Not from a position — a strand is a DAG, and
/// parallel branches merging downstream have no ordinal, so two identical caps
/// on separate branches differ only by token. Not from notation — a plan holds
/// strictly more than the notation it was planned from, and reducing one back
/// to the other discards exactly the identities this type exists to carry. A
/// token comes from the plan that minted it or it does not exist.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize)]
#[serde(transparent)]
pub struct StepToken(String);

impl StepToken {
    /// Mint a fresh identity. This is how every step in production is born.
    pub fn mint() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    /// Recover an already-minted token — from deserialization, from a protocol
    /// message, from a persisted run. An empty id is not a token: it names no
    /// step, so a value bound to it could never be delivered.
    pub fn parse(raw: impl Into<String>) -> Result<Self, StepTokenError> {
        let raw = raw.into();
        if raw.is_empty() {
            return Err(StepTokenError::Empty);
        }
        Ok(Self(raw))
    }

    /// The token's text, for protocol encoding and diagnostics.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::str::FromStr for StepToken {
    type Err = StepTokenError;
    /// The same single recovery path as [`StepToken::parse`], so `"…".parse()`
    /// and an explicit `parse` call cannot disagree about what a token is.
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::parse(raw)
    }
}

impl std::fmt::Display for StepToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::ops::Deref for StepToken {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for StepToken {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::borrow::Borrow<str> for StepToken {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for StepToken {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<StepToken> for str {
    fn eq(&self, other: &StepToken) -> bool {
        self == other.0
    }
}

impl PartialEq<&str> for StepToken {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

/// The only way a [`StepToken`] can fail to exist.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StepTokenError {
    #[error(
        "a strand step token_id is empty; a step without a token came from no plan \
         and cannot be addressed"
    )]
    Empty,
}

impl<'de> serde::Deserialize<'de> for StepToken {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        StepToken::parse(raw).map_err(serde::de::Error::custom)
    }
}

/// The producer of one of a cap step's inputs.
///
/// A `Strand` is a DAG of steps: an input is fed either by the strand's own input
/// (an input anchor) or by another cap step's output. There are no positional
/// assumptions — every input names its producer explicitly, so fan-out (one producer
/// feeding several caps' main inputs) and convergence (one cap fed by several
/// producers) are both expressible.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ArgSourceRef {
    /// Fed by the strand's input anchor — the strand's own input flows into this arg.
    StrandInput,
    /// Fed by another cap step's output, identified by that step's stable token.
    Step { token_id: StepToken },
}

/// One input to a cap step: the cap argument it feeds (by the argument's slot media
/// URN) and the producer of that input.
///
/// A cap step lists ALL of its data-flow inputs — the primary (stdin/main) input and
/// every convergence input — with no positional ordering assumptions. The PRIMARY
/// input is the one whose `arg_urn` is the cap's stdin argument URN; it threads the
/// step's `from_spec` runtime media. Arg URNs into one cap are all distinct (RULE1: a
/// cap's args have unique media URNs), so `arg_urn` alone identifies the slot.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CapInput {
    /// The cap argument's slot media URN this input feeds.
    pub arg_urn: MediaUrn,
    /// The producer of this input.
    pub source: ArgSourceRef,
}

/// Type of step in a capability chain path.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StrandStepType {
    /// A real capability step
    Cap {
        cap_urn: CapUrn,
        title: String,
        specificity: usize,
        /// Whether the cap's main input expects a sequence
        input_is_sequence: bool,
        /// Whether the cap's output produces a sequence
        output_is_sequence: bool,
        /// ALL of this cap's data-flow inputs — the primary (stdin) input plus any
        /// convergence inputs — each naming its producer explicitly. The primary is
        /// the input whose `arg_urn` is the cap's stdin argument URN.
        inputs: Vec<CapInput>,
    },
    /// Fan-out: iterate over sequence items (is_sequence flips true → false).
    /// The media URN does not change — ForEach is a shape transition, not a type transition.
    ForEach {
        /// The media type being iterated over
        media_def: MediaUrn,
    },
    /// Collect: gather items into a sequence (is_sequence flips false → true).
    /// The media URN does not change — Collect is a shape transition, not a type transition.
    Collect {
        /// The media type being collected
        media_def: MediaUrn,
    },
}

/// Information about a single step in a capability chain path.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StrandStep {
    /// Stable per-step identity, minted once when the step is created (the very
    /// source of a resolved strand). It is the single key that ties this element
    /// of the realized graph to every live update the run emits for it — so a
    /// repeated cap URN in one strand is never ambiguous. Alias-free and
    /// notation-independent (aliases are display-only); it travels verbatim
    /// through serialization, the run's persisted `resolved_strand`, the render
    /// payload, and every progress message.
    pub token_id: StepToken,
    /// Type of step (cap or cardinality transition)
    pub step_type: StrandStepType,
    /// Input media type for this step
    pub from_spec: MediaUrn,
    /// Output media type for this step
    pub to_spec: MediaUrn,
}

impl StrandStep {
    /// Create a step, minting its stable `token_id`. This is the ONLY way a step
    /// is born in production, so every step in a resolved strand carries an id
    /// from creation; deserialization preserves the stored id verbatim.
    pub fn new(step_type: StrandStepType, from_spec: MediaUrn, to_spec: MediaUrn) -> Self {
        Self {
            token_id: StepToken::mint(),
            step_type,
            from_spec,
            to_spec,
        }
    }

    /// The step's stable identity (see `token_id`).
    pub fn token_id(&self) -> &StepToken {
        &self.token_id
    }

    /// Get the title for this step (for display purposes)
    pub fn title(&self) -> String {
        match &self.step_type {
            StrandStepType::Cap { title, .. } => title.clone(),
            StrandStepType::ForEach { .. } => "ForEach".to_string(),
            StrandStepType::Collect { .. } => "Collect".to_string(),
        }
    }

    /// Get the specificity of this step (for ordering purposes)
    pub fn specificity(&self) -> usize {
        match &self.step_type {
            StrandStepType::Cap { specificity, .. } => *specificity,
            _ => 0,
        }
    }

    /// Get the cap URN if this is a cap step
    pub fn cap_urn(&self) -> Option<&CapUrn> {
        match &self.step_type {
            StrandStepType::Cap { cap_urn, .. } => Some(cap_urn),
            _ => None,
        }
    }

    /// Check if this is a cap step
    pub fn is_cap(&self) -> bool {
        matches!(self.step_type, StrandStepType::Cap { .. })
    }
}

/// Information about a complete capability chain path.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Strand {
    /// Steps in the path, in order
    pub steps: Vec<StrandStep>,
    /// Source media URN
    pub source_media_urn: MediaUrn,
    /// Target media URN
    pub target_media_urn: MediaUrn,
    /// Total number of steps (including cardinality transitions)
    pub total_steps: i32,
    /// Number of cap steps only (excluding ForEach/Collect)
    /// This is used for sorting - cardinality transitions don't count as "steps" for user display
    pub cap_step_count: i32,
    /// Human-readable description
    pub description: String,
}

impl Strand {
    /// Convert this resolved strand into a single-strand
    /// `Machine`. Each `Cap` step becomes one resolved
    /// `MachineEdge`; `ForEach` sets `is_loop` on the next cap;
    /// `Collect` is elided.
    ///
    /// Resolution requires the cap registry to look up each
    /// cap's argument list (used by the Hungarian source-to-
    /// arg matching algorithm).
    ///
    /// Fails if the strand contains no capability steps, if
    /// any cap is not in the registry, if a source cannot be
    /// matched to a cap arg, if the matching is ambiguous, or
    /// if the resolved data-flow graph contains a cycle.
    pub fn knit(
        &self,
        registry: &crate::cap::registry::FabricRegistry,
    ) -> Result<crate::machine::Machine, crate::machine::MachineAbstractionError> {
        crate::machine::Machine::from_strand(self, registry)
    }

    /// Serialize this resolved strand to canonical one-line machine notation.
    /// This is the primary identifier used for accessibility and persistence —
    /// it is alias-INDEPENDENT (caps render as canonical URNs). Aliases are a
    /// display-only concern applied at the UI layer via
    /// `FabricRegistry::display_alias_for_urn`; storage and identity never carry
    /// them, so a later alias rename/removal can never change a stored machine.
    ///
    /// Same failure modes as `knit`, since this method first
    /// builds the `Machine` and then serializes it.
    pub fn to_machine_notation(
        &self,
        registry: &crate::cap::registry::FabricRegistry,
    ) -> Result<String, crate::machine::MachineAbstractionError> {
        self.knit(registry)?.to_machine_notation()
    }
}

// =============================================================================
// IMPLEMENTATION
// =============================================================================

impl LiveCapFab {
    /// Create a new empty capability graph.
    pub fn new() -> Self {
        Self {
            edges: Vec::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            nodes: HashSet::new(),
            cap_to_edges: HashMap::new(),
            bookend_nodes: HashSet::new(),
        }
    }

    /// Clear the graph completely.
    pub fn clear(&mut self) {
        self.edges.clear();
        self.outgoing.clear();
        self.incoming.clear();
        self.nodes.clear();
        self.cap_to_edges.clear();
        self.bookend_nodes.clear();
    }

    /// Returns `true` if the given URN is a bookend-eligible node — a node
    /// whose registered media def has at least one file extension, so
    /// concrete file content of that type can exist on disk.
    ///
    /// Strand bookends (transmute source or target) MUST be bookend-eligible.
    /// Internal URNs that appear as cap inputs/outputs but do not name a
    /// concrete file format (wildcards like `media:enc=utf-8`, primitives
    /// like `media:integer;numeric`, role markers like
    /// `media:enc=utf-8;page`) are tracked as nodes for path-finding but
    /// are not valid bookends and never appear as transmute sources/targets.
    pub fn is_bookend(&self, urn: &MediaUrn) -> bool {
        self.bookend_nodes.contains(urn)
    }

    /// Rebuild the graph from a list of capabilities.
    ///
    /// This completely replaces the current graph contents.
    /// Call this when the set of available capabilities changes.
    ///
    /// Only Cap edges are stored in the graph. Cardinality transitions
    /// (ForEach/Collect) are synthesized dynamically by
    /// `get_outgoing_edges()` based on source cardinality.
    ///
    /// `bookend_urns` is the live snapshot of media-registry URNs whose
    /// stored spec has at least one file extension. Nodes in the graph
    /// are intersected with this set to mark which can serve as strand
    /// bookends. The intersection is computed once per sync; lookups
    /// during traversal are O(1) HashSet hits with no registry calls.
    pub fn sync_from_caps(&mut self, caps: &[Cap], bookend_urns: &HashSet<MediaUrn>) {
        self.clear();

        for cap in caps {
            self.add_cap(cap);
        }

        self.refresh_bookends(bookend_urns);
    }

    /// Recompute the bookend node set as the intersection of `self.nodes`
    /// and the supplied registry snapshot. Public so callers that build the
    /// graph incrementally via `add_cap` (e.g. tests) can refresh the
    /// bookend set independently of a full sync.
    pub fn set_bookends(&mut self, bookend_urns: &HashSet<MediaUrn>) {
        self.refresh_bookends(bookend_urns);
    }

    /// Recompute the bookend node set as the intersection of `self.nodes`
    /// and the supplied registry snapshot.
    fn refresh_bookends(&mut self, bookend_urns: &HashSet<MediaUrn>) {
        self.bookend_nodes = self
            .nodes
            .iter()
            .filter(|n| bookend_urns.contains(*n))
            .cloned()
            .collect();
    }

    /// Rebuild the graph from a list of cap URN strings using the registry.
    ///
    /// This is the primary method for RelaySwitch integration. Given the list of
    /// available cap URN strings (from cartridges), it looks up the Cap definitions
    /// from the registry and builds the graph.
    ///
    /// Caps are matched by equivalence (`is_equivalent`): the cartridge's reported URN
    /// must have an exact semantic match in the registry. Unmatched caps are rejected
    /// with an error and excluded from the graph — a cartridge advertising an unregistered
    /// capability is a configuration bug that must be fixed.
    pub async fn sync_from_cap_urns(
        &mut self,
        cap_urns: &[String],
        registry: &Arc<FabricRegistry>,
        bookend_urns: &HashSet<MediaUrn>,
    ) {
        self.clear();

        // Get all cached caps from registry
        let all_caps = match registry.get_cached_caps().await {
            Ok(caps) => caps,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "[LiveCapFab] Failed to get cached caps from registry"
                );
                return;
            }
        };

        let mut matched_count = 0;
        let mut identity_count = 0;
        let mut rejected_count = 0;

        for cap_urn_str in cap_urns.iter() {
            // Parse the cap URN
            let cap_urn = match CapUrn::from_string(cap_urn_str) {
                Ok(u) => u,
                Err(e) => {
                    tracing::error!(
                        cap_urn = cap_urn_str,
                        error = %e,
                        "[LiveCapFab] Cartridge reported invalid cap URN - this is a bug in the cartridge"
                    );
                    continue;
                }
            };

            // Skip identity caps - they don't contribute to path finding
            if cap_urn.is_equivalent(&crate::standard::caps::identity_urn()) {
                identity_count += 1;
                continue;
            }

            // Find the exact matching Cap in registry using is_equivalent.
            // The cartridge reports the specific cap URN it implements — we need to find
            // that same cap in the registry. Using is_dispatchable here was wrong because
            // it would match a wildcard registry cap (e.g. in=media:) before reaching
            // the specific one (e.g. in=media:enc=utf-8;ext=txt), since .find() returns the
            // first match.
            let matching_cap_owned = all_caps
                .iter()
                .find(|registry_cap| cap_urn.is_equivalent(&registry_cap.urn))
                .cloned();

            let resolved_cap = match matching_cap_owned {
                Some(cap) => Some(cap),
                None => {
                    registry.request_cap_cache_hydration(cap_urn_str);
                    registry.get_cached_cap_in_memory(cap_urn_str)
                }
            };

            match resolved_cap {
                Some(cap) => {
                    self.add_cap(&cap);
                    matched_count += 1;
                }
                None => {
                    rejected_count += 1;
                    // Warn rather than error: this fires during the
                    // narrow window where a cartridge advertises a
                    // cap before the registry has finished hydrating
                    // its in-memory cache from disk + R2. The cap
                    // is dropped from THIS LiveCapFab pass, but the
                    // registry's background fetcher will pick it up
                    // and cache-revision subscribers will rebuild the
                    // graph when it lands. This path must remain
                    // instantaneous for the Finder transmute menu.
                    tracing::warn!(
                        cap_urn = %cap_urn,
                        cap_urn_raw = cap_urn_str,
                        "[LiveCapFab] dropped: cartridge reported cap URN has no equivalent \
                         in the registry yet. The registry's background fetcher will pull it \
                         in; subsequent LiveCapFab refreshes will add it to the graph."
                    );
                }
            }
        }

        let _ = (matched_count, identity_count, rejected_count);

        self.refresh_bookends(bookend_urns);
    }

    /// Add a capability as an edge in the graph.
    pub fn add_cap(&mut self, cap: &Cap) {
        // Abstract caps are dispatch umbrellas — never backed by a cartridge and
        // never a runnable edge. Adding one would put an unbacked edge in the
        // graph (the wizard/planner could offer a path that fails at execution).
        // They are narrowed to a concrete specialization at the CLI layer instead.
        if cap.is_abstract {
            return;
        }

        let in_spec_str = cap.urn.in_spec();
        let out_spec_str = cap.urn.out_spec();

        // Skip caps with empty specs
        if in_spec_str.is_empty() || out_spec_str.is_empty() {
            tracing::warn!(
                cap_urn = %cap.urn,
                in_spec = in_spec_str,
                out_spec = out_spec_str,
                "[LiveCapFab] Skipping cap with empty spec"
            );
            return;
        }

        // Skip identity caps (passthrough caps that don't transform anything)
        // These are is_equivalent to the CAP_IDENTITY constant
        if cap
            .urn
            .is_equivalent(&crate::standard::caps::identity_urn())
        {
            return;
        }

        // Parse media URNs
        let from_spec = match MediaUrn::from_string(in_spec_str) {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!(
                    cap_urn = %cap.urn,
                    in_spec = in_spec_str,
                    error = %e,
                    "[LiveCapFab] Failed to parse in_spec, skipping cap"
                );
                return;
            }
        };

        let to_spec = match MediaUrn::from_string(out_spec_str) {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!(
                    cap_urn = %cap.urn,
                    out_spec = out_spec_str,
                    error = %e,
                    "[LiveCapFab] Failed to parse out_spec, skipping cap"
                );
                return;
            }
        };

        // Create edge
        let edge_idx = self.edges.len();
        // Cardinality shape from the single canonical definition (`Cap::sequence_shape`)
        // so path search, editor realization, and notation resolution never diverge.
        let (input_is_sequence, output_is_sequence) = cap.sequence_shape();

        // Update indices with URN clones — MediaUrn and CapUrn
        // are the HashMap keys directly via their derived
        // `Hash`/`Eq` impls; no string intermediaries.
        self.outgoing
            .entry(from_spec.clone())
            .or_default()
            .push(edge_idx);
        self.incoming
            .entry(to_spec.clone())
            .or_default()
            .push(edge_idx);
        self.nodes.insert(from_spec.clone());
        self.nodes.insert(to_spec.clone());
        self.cap_to_edges
            .entry(cap.urn.clone())
            .or_default()
            .push(edge_idx);

        let edge = LiveMachinePlanEdge {
            from_spec,
            to_spec,
            edge_type: LiveMachinePlanEdgeType::Cap {
                cap_urn: cap.urn.clone(),
                cap_title: cap.title.clone(),
                specificity: cap.urn.specificity(),
                input_is_sequence,
                output_is_sequence,
            },
        };
        self.edges.push(edge);
    }

    /// Get all edges reachable from a source media URN.
    ///
    /// Returns Cap edges where the source conforms to the edge's input requirement
    /// (with matching cardinality), plus synthesized cardinality transitions.
    ///
    /// Get outgoing edges from a source media URN at a given `is_sequence` state.
    ///
    /// Cap edges are matched purely on `conforms_to` — cardinality is irrelevant
    /// to type matching. Cardinality transitions (ForEach/Collect) are synthesized
    /// based on the current `is_sequence` state:
    ///
    /// - **ForEach** (is_sequence=true → false): iterate over sequence items.
    ///   The media URN does not change — ForEach is a shape transition, not a type transition.
    /// - **Collect** (is_sequence=false → true): gather items into a sequence.
    ///   The media URN does not change — Collect is a shape transition, not a type transition.
    pub(crate) fn get_outgoing_edges(
        &self,
        source: &MediaUrn,
        is_sequence: bool,
    ) -> Vec<(LiveMachinePlanEdge, bool)> {
        let mut result: Vec<(LiveMachinePlanEdge, bool)> = self
            .edges
            .iter()
            .filter(|edge| {
                debug_assert!(
                    edge.is_cap(),
                    "Non-cap edge found in graph storage: {:?}",
                    edge.edge_type
                );
                if !source.conforms_to(&edge.from_spec).unwrap_or(false) {
                    return false;
                }
                // Check cardinality compatibility:
                // - sequence data can only go to caps that expect sequences
                // - scalar data can go to scalar or sequence caps (single item wraps into 1-item sequence)
                match &edge.edge_type {
                    LiveMachinePlanEdgeType::Cap {
                        input_is_sequence, ..
                    } => {
                        if is_sequence && !input_is_sequence {
                            // Sequence data → scalar cap: needs ForEach first, skip direct match
                            false
                        } else {
                            true
                        }
                    }
                    _ => true,
                }
            })
            .filter_map(|edge| {
                // Determine outgoing is_sequence from the cap's output flag
                match &edge.edge_type {
                    LiveMachinePlanEdgeType::Cap {
                        cap_urn,
                        cap_title,
                        specificity,
                        input_is_sequence,
                        output_is_sequence,
                    } => {
                        let runtime_out = cap_urn.infer_runtime_output_media(source).ok()?;
                        Some((
                            LiveMachinePlanEdge {
                                from_spec: source.clone(),
                                to_spec: runtime_out,
                                edge_type: LiveMachinePlanEdgeType::Cap {
                                    cap_urn: cap_urn.clone(),
                                    cap_title: cap_title.clone(),
                                    specificity: *specificity,
                                    input_is_sequence: *input_is_sequence,
                                    output_is_sequence: *output_is_sequence,
                                },
                            },
                            *output_is_sequence,
                        ))
                    }
                    _ => Some((edge.clone(), is_sequence)),
                }
            })
            .collect();

        // Synthesize ForEach when data is a sequence
        if is_sequence {
            // ForEach: sequence → scalar (same media URN, is_sequence flips to false)
            // Check if any scalar cap could consume items after ForEach
            let has_scalar_consumers = self.edges.iter().any(|edge| {
                if let LiveMachinePlanEdgeType::Cap {
                    input_is_sequence, ..
                } = &edge.edge_type
                {
                    !input_is_sequence && source.conforms_to(&edge.from_spec).unwrap_or(false)
                } else {
                    false
                }
            });
            if has_scalar_consumers {
                result.push((
                    LiveMachinePlanEdge {
                        from_spec: source.clone(),
                        to_spec: source.clone(),
                        edge_type: LiveMachinePlanEdgeType::ForEach,
                    },
                    false,
                ));
            }
        }
        // Collect is NOT synthesized during path finding. It pairs with ForEach
        // implicitly at execution time — the plan builder handles it.
        // Synthesizing Collect here creates ForEach↔Collect cycles that cause
        // infinite loops in the DFS.

        result
    }

    /// Fast reachability check using runtime-instantiated traversal semantics.
    ///
    /// This deliberately avoids checking only registered literal graph nodes,
    /// because `effect=none` and `effect=patch` can materialize concrete runtime
    /// outputs that do not appear verbatim as stored edge endpoints.
    pub(crate) fn has_reachable_exact_target(
        &self,
        source: &MediaUrn,
        target: &MediaUrn,
        is_sequence: bool,
        max_depth: usize,
    ) -> bool {
        use std::collections::VecDeque;

        let mut queue = VecDeque::from([(source.clone(), is_sequence, false, 0usize)]);
        let mut visited: HashSet<(MediaUrn, bool, bool)> =
            HashSet::from([(source.clone(), is_sequence, false)]);

        while let Some((current, current_is_seq, pending_foreach, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            for (edge, next_is_seq) in self.get_outgoing_edges(&current, current_is_seq) {
                if pending_foreach && !Self::can_follow_foreach(&edge) {
                    continue;
                }
                if edge.is_cap() && edge.to_spec.is_equivalent(target).unwrap_or(false) {
                    return true;
                }

                let next_pending_foreach =
                    matches!(&edge.edge_type, LiveMachinePlanEdgeType::ForEach);
                let visit_key = (edge.to_spec.clone(), next_is_seq, next_pending_foreach);
                if visited.insert(visit_key.clone()) {
                    queue.push_back((visit_key.0, visit_key.1, visit_key.2, depth + 1));
                }
            }
        }

        false
    }

    /// Get statistics about the graph.
    pub fn stats(&self) -> (usize, usize) {
        (self.nodes.len(), self.edges.len())
    }

    /// The bookend-eligible node set (see `is_bookend`). Used by the unified
    /// plan engine (`plan_engine.rs`) to restrict discovered targets.
    pub(crate) fn bookends(&self) -> &HashSet<MediaUrn> {
        &self.bookend_nodes
    }

    /// The distinct cap URNs in the graph, for registry-backed lookups by the
    /// unified plan engine (e.g. multi-input Merge-cap discovery).
    pub(crate) fn cap_urns(&self) -> Vec<CapUrn> {
        let mut urns: Vec<CapUrn> = self.cap_to_edges.keys().cloned().collect();
        urns.sort();
        urns
    }

    // =========================================================================
    // REACHABLE TARGETS (BFS)
    // =========================================================================

    /// Find all reachable targets from a source media URN.
    ///
    /// Uses **BFS** — visits each (MediaUrn, is_sequence) node once to discover
    /// which targets are reachable and how many edges reach each one. O(V+E),
    /// completes in microseconds. Does NOT enumerate actual paths.
    ///
    /// Used for the transmute menu where we need target names and path counts
    /// but not the routes themselves. IDDFS would be orders of magnitude slower
    /// here because it enumerates every distinct path combinatorially — with 80+
    /// edges and depth 10 that can mean thousands of paths explored per target,
    /// multiplied by 20+ reachable targets.
    ///
    /// `is_sequence` is the initial cardinality state of the input (from context).
    /// Returns targets sorted by (min_path_length, display_name).
    pub fn get_reachable_targets(
        &self,
        source: &MediaUrn,
        is_sequence: bool,
        max_depth: usize,
    ) -> Vec<ReachableTargetInfo> {
        // `results` and `visited` are keyed on `MediaUrn`
        // directly — their derived `Hash`/`Eq` go through
        // `TaggedUrn`'s structural tag-set identity.
        let mut results: HashMap<MediaUrn, ReachableTargetInfo> = HashMap::new();
        let mut visited: HashSet<(MediaUrn, bool, bool)> = HashSet::new();
        let mut queue: VecDeque<(MediaUrn, bool, bool, usize)> = VecDeque::new();

        queue.push_back((source.clone(), is_sequence, false, 0));
        visited.insert((source.clone(), is_sequence, false));

        while let Some((current, current_is_seq, pending_foreach, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            for (edge, next_is_seq) in self.get_outgoing_edges(&current, current_is_seq) {
                if pending_foreach && !Self::can_follow_foreach(&edge) {
                    continue;
                }
                let new_depth = depth + 1;

                let next_pending_foreach =
                    matches!(&edge.edge_type, LiveMachinePlanEdgeType::ForEach);
                if !next_pending_foreach {
                    // A ForEach boundary is not a result. Record only complete
                    // cap transitions; structural equality collapses tag-order
                    // equivalent MediaUrns.
                    let entry = results.entry(edge.to_spec.clone()).or_insert_with(|| {
                        ReachableTargetInfo {
                            media_def: edge.to_spec.clone(),
                            display_name: edge.to_spec.to_string(),
                            min_path_length: new_depth as i32,
                            path_count: 0,
                        }
                    });
                    entry.path_count += 1;
                }

                // Continue BFS if not visited at this cardinality state.
                let visit_key = (edge.to_spec.clone(), next_is_seq, next_pending_foreach);
                if !visited.contains(&visit_key) {
                    visited.insert(visit_key);
                    queue.push_back((
                        edge.to_spec.clone(),
                        next_is_seq,
                        next_pending_foreach,
                        new_depth,
                    ));
                }
            }
        }

        // Filter to bookend-eligible targets. Reachability in the cap graph
        // includes URNs that exist only as cap-input wildcards (e.g.
        // `media:enc=utf-8`, `media:integer;numeric`). Such URNs
        // describe the value type a cap accepts, not a file format that
        // can sit on disk, so they are never valid transmute targets. The
        // bookend set is precomputed at sync time from the live media
        // registry — no registry call here.
        let mut targets: Vec<_> = results
            .into_values()
            .filter(|t| self.bookend_nodes.contains(&t.media_def))
            .collect();

        // Sort by (min_path_length, display_name).
        //
        // `display_name` is a presentation string (not an
        // identity key), so lex-comparing it as a String is
        // the correct semantics — this is user-visible
        // alphabetical sort, not URN equivalence.
        targets.sort_by(|a, b| {
            a.min_path_length
                .cmp(&b.min_path_length)
                .then_with(|| a.display_name.cmp(&b.display_name))
        });

        targets
    }

    // =========================================================================
    // PATH FINDING (DFS with exact target matching)
    // =========================================================================

    /// Find all paths from source to a specific target media URN.
    ///
    /// Uses **IDDFS** (iterative deepening DFS) — enumerates every distinct route
    /// (sequence of cap/ForEach/Collect steps) between source and target. Finds
    /// shortest paths first (depth 1, then 2, etc.). Can take 10-100ms per call
    /// depending on graph density due to combinatorial path explosion.
    ///
    /// Used when the user has already chosen a target and needs to see or select
    /// the specific transformation chain. Not suitable for discovery (use BFS
    /// `get_reachable_targets` instead — it's O(V+E) vs combinatorial).
    ///
    /// **Critical**: Uses `is_equivalent()` for target matching, NOT `conforms_to()`.
    /// `is_sequence` is the initial cardinality state (from input context).
    ///
    /// Returns paths sorted by structural path score, then specificity,
    /// then structural step order.
    pub fn find_paths_to_exact_target(
        &self,
        source: &MediaUrn,
        target: &MediaUrn,
        is_sequence: bool,
        max_depth: usize,
        max_paths: usize,
    ) -> Vec<Strand> {
        self.find_paths_to_exact_target_with_step_title_query(
            source,
            target,
            is_sequence,
            max_depth,
            max_paths,
            None,
        )
    }

    pub fn find_paths_to_exact_target_with_step_title_query(
        &self,
        source: &MediaUrn,
        target: &MediaUrn,
        is_sequence: bool,
        max_depth: usize,
        max_paths: usize,
        step_title_query: Option<&str>,
    ) -> Vec<Strand> {
        if !self.has_reachable_exact_target(source, target, is_sequence, max_depth) {
            return Vec::new();
        }

        // Iterative deepening: find ALL paths at depth N before any at depth N+1.
        let mut all_paths: Vec<Strand> = Vec::new();
        let mut total_nodes_explored: u64 = 0;
        let not_cancelled = std::sync::atomic::AtomicBool::new(false);

        for depth_limit in 1..=max_depth {
            if all_paths.len() >= max_paths {
                break;
            }

            let mut current_path: Vec<StrandStep> = Vec::new();
            let mut visited: HashSet<(MediaUrn, bool)> = HashSet::new();
            let paths_before = all_paths.len();
            let mut nodes_this_depth: u64 = 0;

            self.iddfs_find_paths(
                source,
                target,
                source,
                is_sequence,
                step_title_query,
                &mut current_path,
                &mut visited,
                &mut all_paths,
                depth_limit,
                max_paths,
                &mut nodes_this_depth,
                &not_cancelled,
            );

            total_nodes_explored += nodes_this_depth;

            // Safety: abort if exploring too many nodes (combinatorial explosion)
            if total_nodes_explored > 100_000 {
                tracing::warn!(
                    "find_paths_to_exact_target: aborting after {} nodes explored. \
                     Returning {} paths found so far.",
                    total_nodes_explored,
                    all_paths.len()
                );
                break;
            }
        }

        // Sort paths deterministically
        all_paths.sort_by(|a, b| Self::compare_paths(a, b));

        all_paths
    }

    /// Find paths with streaming progress reporting.
    ///
    /// Same IDDFS algorithm as `find_paths_to_exact_target`, but calls `on_event`
    /// for each progress update and each path found so the UI can show results
    /// incrementally. Returns the final sorted list of paths.
    pub fn find_paths_streaming<F>(
        &self,
        source: &MediaUrn,
        target: &MediaUrn,
        is_sequence: bool,
        max_depth: usize,
        max_paths: usize,
        cancelled: &std::sync::atomic::AtomicBool,
        mut on_event: F,
    ) -> Vec<Strand>
    where
        F: FnMut(PathFindingEvent),
    {
        self.find_paths_streaming_with_step_title_query(
            source,
            target,
            is_sequence,
            max_depth,
            max_paths,
            None,
            cancelled,
            on_event,
        )
    }

    pub fn find_paths_streaming_with_step_title_query<F>(
        &self,
        source: &MediaUrn,
        target: &MediaUrn,
        is_sequence: bool,
        max_depth: usize,
        max_paths: usize,
        step_title_query: Option<&str>,
        cancelled: &std::sync::atomic::AtomicBool,
        mut on_event: F,
    ) -> Vec<Strand>
    where
        F: FnMut(PathFindingEvent),
    {
        if !self.has_reachable_exact_target(source, target, is_sequence, max_depth) {
            return Vec::new();
        }

        let mut all_paths: Vec<Strand> = Vec::new();
        let mut total_nodes_explored: u64 = 0;

        for depth_limit in 1..=max_depth {
            if all_paths.len() >= max_paths {
                break;
            }
            if cancelled.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            let mut current_path: Vec<StrandStep> = Vec::new();
            let mut visited: HashSet<(MediaUrn, bool)> = HashSet::new();
            let paths_before = all_paths.len();
            let mut nodes_this_depth: u64 = 0;

            self.iddfs_find_paths(
                source,
                target,
                source,
                is_sequence,
                step_title_query,
                &mut current_path,
                &mut visited,
                &mut all_paths,
                depth_limit,
                max_paths,
                &mut nodes_this_depth,
                cancelled,
            );

            total_nodes_explored += nodes_this_depth;

            // Report progress after each depth
            on_event(PathFindingEvent::DepthComplete {
                depth: depth_limit,
                max_depth,
                nodes_explored: total_nodes_explored,
                paths_found: all_paths.len(),
            });

            // Report each new path found at this depth
            for path in &all_paths[paths_before..] {
                on_event(PathFindingEvent::PathFound(path.clone()));
            }

            if cancelled.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            if total_nodes_explored > 100_000 {
                break;
            }
        }

        all_paths.sort_by(|a, b| Self::compare_paths(a, b));

        on_event(PathFindingEvent::Complete {
            total_paths: all_paths.len(),
            total_nodes_explored,
        });

        all_paths
    }

    /// Depth-limited DFS helper for iterative deepening path finding.
    ///
    /// `is_sequence` tracks the current cardinality state through the path.
    /// Only records paths whose length equals `depth_limit` exactly.
    fn iddfs_find_paths(
        &self,
        source: &MediaUrn,
        target: &MediaUrn,
        current: &MediaUrn,
        is_sequence: bool,
        step_title_query: Option<&str>,
        current_path: &mut Vec<StrandStep>,
        visited: &mut HashSet<(MediaUrn, bool)>,
        all_paths: &mut Vec<Strand>,
        depth_limit: usize,
        max_paths: usize,
        nodes_explored: &mut u64,
        cancelled: &std::sync::atomic::AtomicBool,
    ) {
        *nodes_explored += 1;
        if all_paths.len() >= max_paths {
            return;
        }
        if cancelled.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        // Safety: bail out if exploring too many nodes
        if *nodes_explored > 100_000 {
            return;
        }

        // Check if we've reached the EXACT target using is_equivalent().
        // Skip this check at the starting node (empty path) — when source==target,
        // we still want to explore edges to find round-trip transformation paths.
        let pending_foreach = matches!(
            current_path.last().map(|step| &step.step_type),
            Some(StrandStepType::ForEach { .. })
        );
        if !current_path.is_empty() && current.is_equivalent(target).unwrap_or(false) {
            if current_path.len() == depth_limit {
                let cap_step_count = current_path.iter().filter(|s| s.is_cap()).count() as i32;

                // A valid machine requires at least one capability step.
                if cap_step_count > 0 && !pending_foreach {
                    let description = current_path
                        .iter()
                        .map(|s| s.title())
                        .collect::<Vec<_>>()
                        .join(" → ");

                    let path = Strand {
                        steps: current_path.clone(),
                        source_media_urn: source.clone(),
                        target_media_urn: target.clone(),
                        total_steps: current_path.len() as i32,
                        cap_step_count,
                        description,
                    };
                    if Self::path_matches_step_title_query(&path, step_title_query) {
                        all_paths.push(path);
                    }
                }
            }
            // For round-trip paths (source==target), don't return early —
            // continue exploring edges to find longer paths through this node.
            if !pending_foreach && !source.is_equivalent(target).unwrap_or(false) {
                return;
            }
        }

        if current_path.len() >= depth_limit {
            return;
        }

        let visit_key = (current.clone(), is_sequence);
        // For round-trip paths (source==target), don't mark target-equivalent nodes
        // as visited. This allows the DFS to return to the target through different
        // intermediate paths. Cycle prevention is handled by depth_limit.
        let is_roundtrip = source.is_equivalent(target).unwrap_or(false);
        if !(is_roundtrip && current.is_equivalent(target).unwrap_or(false)) {
            visited.insert(visit_key.clone());
        }

        for (edge, next_is_seq) in self.get_outgoing_edges(current, is_sequence) {
            if pending_foreach && !Self::can_follow_foreach(&edge) {
                continue;
            }
            let next_visit_key = (edge.to_spec.clone(), next_is_seq);

            if !visited.contains(&next_visit_key) {
                // A fabricated cap's single input is its MAIN (stdin) input, fed by the
                // previous cap in the path being built, or by the strand input when
                // this is the first cap. Path finding routes solely on main input→
                // output (one main input, one output); non-main args are never routed
                // by the planner — they are user-supplied (wizards / scenarios / CLI),
                // and cap-output→non-main-arg convergence exists only in hand-written
                // notation, populated by `realize`. So a fabricated step has exactly
                // one input by construction.
                let cap_primary_source = current_path
                    .iter()
                    .rev()
                    .find(|s| s.is_cap())
                    .map(|s| ArgSourceRef::Step {
                        token_id: s.token_id().clone(),
                    })
                    .unwrap_or(ArgSourceRef::StrandInput);
                let step_type = match &edge.edge_type {
                    LiveMachinePlanEdgeType::Cap {
                        cap_urn,
                        cap_title,
                        specificity,
                        input_is_sequence,
                        output_is_sequence,
                    } => StrandStepType::Cap {
                        cap_urn: cap_urn.clone(),
                        title: cap_title.clone(),
                        specificity: *specificity,
                        input_is_sequence: *input_is_sequence,
                        output_is_sequence: *output_is_sequence,
                        inputs: vec![CapInput {
                            arg_urn: MediaUrn::from_string(cap_urn.in_spec())
                                .expect("cap URN in= is a valid media URN"),
                            source: cap_primary_source,
                        }],
                    },
                    LiveMachinePlanEdgeType::ForEach => StrandStepType::ForEach {
                        media_def: edge.from_spec.clone(),
                    },
                    LiveMachinePlanEdgeType::Collect => StrandStepType::Collect {
                        media_def: edge.from_spec.clone(),
                    },
                };

                current_path.push(StrandStep::new(
                    step_type,
                    edge.from_spec.clone(),
                    edge.to_spec.clone(),
                ));

                self.iddfs_find_paths(
                    source,
                    target,
                    &edge.to_spec,
                    next_is_seq,
                    step_title_query,
                    current_path,
                    visited,
                    all_paths,
                    depth_limit,
                    max_paths,
                    nodes_explored,
                    cancelled,
                );

                current_path.pop();
            }
        }

        visited.remove(&visit_key);
    }

    /// A synthesized ForEach boundary qualifies exactly the immediately
    /// following scalar-input cap. Allowing a sequence-input cap here creates
    /// a cardinality no-op (`ForEach -> concat`) that cannot be represented by
    /// the resolved machine and is semantically dominated by the direct cap.
    fn can_follow_foreach(edge: &LiveMachinePlanEdge) -> bool {
        matches!(
            &edge.edge_type,
            LiveMachinePlanEdgeType::Cap {
                input_is_sequence: false,
                ..
            }
        )
    }

    fn path_matches_step_title_query(path: &Strand, step_title_query: Option<&str>) -> bool {
        let Some(step_title_query) = step_title_query
            .map(str::trim)
            .filter(|query| !query.is_empty())
        else {
            return true;
        };

        let needle = step_title_query.to_lowercase();
        path.steps
            .iter()
            .any(|step| step.title().to_lowercase().contains(&needle))
    }

    /// Compare two paths for deterministic ordering.
    ///
    /// Sort by:
    /// 1. `cap_step_count` (ascending — fewer actual cap
    ///    steps first; ForEach/Collect don't count)
    /// 2. total specificity (descending — more specific first)
    /// 3. structural step-sequence ordering (for tie-breaking
    ///    stability)
    ///
    /// The step-sequence comparison routes cap steps through
    /// the `CapUrn` structural `Ord` impl, cardinality steps
    /// through a fixed discriminator (Cap < ForEach < Collect),
    /// and falls through to the step's `from_spec` / `to_spec`
    /// via `MediaUrn`'s structural `Ord`. No URN is ever
    /// compared as a flat string.
    fn compare_paths(a: &Strand, b: &Strand) -> Ordering {
        a.cap_step_count
            .cmp(&b.cap_step_count)
            .then_with(|| {
                // Higher specificity first.
                let spec_a: usize = a.steps.iter().map(|s| s.specificity()).sum();
                let spec_b: usize = b.steps.iter().map(|s| s.specificity()).sum();
                spec_b.cmp(&spec_a)
            })
            .then_with(|| Self::compare_step_sequences(&a.steps, &b.steps))
    }

    /// Lexicographic comparison over step sequences using the
    /// structural step ordering. Stable and deterministic
    /// because every component routes through `MediaUrn` /
    /// `CapUrn` structural `Ord` — never flat-string
    /// comparison.
    fn compare_step_sequences(a: &[StrandStep], b: &[StrandStep]) -> Ordering {
        for (step_a, step_b) in a.iter().zip(b.iter()) {
            match Self::compare_steps(step_a, step_b) {
                Ordering::Equal => continue,
                ord => return ord,
            }
        }
        a.len().cmp(&b.len())
    }

    /// Structural comparison of two strand steps. Routes
    /// through the structural `Ord` of `CapUrn` / `MediaUrn`;
    /// cardinality step discriminators use fixed integer
    /// ranks (Cap = 0, ForEach = 1, Collect = 2).
    fn compare_steps(a: &StrandStep, b: &StrandStep) -> Ordering {
        const RANK_CAP: u8 = 0;
        const RANK_FOREACH: u8 = 1;
        const RANK_COLLECT: u8 = 2;

        let rank = |s: &StrandStep| -> u8 {
            match &s.step_type {
                StrandStepType::Cap { .. } => RANK_CAP,
                StrandStepType::ForEach { .. } => RANK_FOREACH,
                StrandStepType::Collect { .. } => RANK_COLLECT,
            }
        };

        match rank(a).cmp(&rank(b)) {
            Ordering::Equal => {}
            ord => return ord,
        }

        // Same rank — compare structural details.
        match (&a.step_type, &b.step_type) {
            (StrandStepType::Cap { cap_urn: ca, .. }, StrandStepType::Cap { cap_urn: cb, .. }) => {
                match ca.cmp(cb) {
                    Ordering::Equal => {}
                    ord => return ord,
                }
            }
            (
                StrandStepType::ForEach { media_def: ma },
                StrandStepType::ForEach { media_def: mb },
            )
            | (
                StrandStepType::Collect { media_def: ma },
                StrandStepType::Collect { media_def: mb },
            ) => match ma.cmp(mb) {
                Ordering::Equal => {}
                ord => return ord,
            },
            _ => unreachable!("rank comparison already discriminated mismatched step types"),
        }

        // Final tiebreaker: structural from_spec / to_spec.
        match a.from_spec.cmp(&b.from_spec) {
            Ordering::Equal => {}
            ord => return ord,
        }
        a.to_spec.cmp(&b.to_spec)
    }
}

impl Default for LiveCapFab {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cap::definition::Cap;
    use crate::urn::cap_urn::CapUrn;

    /// Bookend set that treats every URN appearing in the supplied caps
    /// (in or out direction) as bookend-eligible. Tests use this when
    /// they want graph reachability without an extra "filter to file
    /// formats" constraint — the planner's bookend filter is exercised
    /// elsewhere.
    fn all_bookends(caps: &[Cap]) -> HashSet<MediaUrn> {
        let mut s = HashSet::new();
        for cap in caps {
            if let Ok(u) = MediaUrn::from_string(cap.urn.in_spec()) {
                s.insert(u);
            }
            if let Ok(u) = MediaUrn::from_string(cap.urn.out_spec()) {
                s.insert(u);
            }
        }
        s
    }

    /// Build a `Cap` that satisfies the registry invariant every real cap
    /// satisfies: a non-void cap declares its MAIN input as an arg carrying a
    /// `Stdin` source whose URN is the cap URN's `in=`. `sequence_shape()` reads
    /// that arg to decide cardinality, so a fixture without it is not a cap the
    /// planner can reason about at all — it would panic rather than mis-plan.
    fn make_test_cap(in_spec: &str, out_spec: &str, op: &str, title: &str) -> Cap {
        use crate::cap::definition::{ArgSource, CapArg};
        use crate::urn::cap_urn::CapUrnBuilder;

        let cap_urn = CapUrnBuilder::new()
            .in_spec(in_spec)
            .out_spec(out_spec)
            .marker(op)
            .build()
            .expect("Failed to build test cap URN");

        Cap {
            urn: cap_urn,
            version: 1,
            title: title.to_string(),
            cap_description: None,
            documentation: None,
            metadata: Default::default(),
            aliases: vec!["test".to_string()],
            is_abstract: false,
            output: None,
            args: vec![CapArg::new(
                in_spec.to_string(),
                true,
                vec![ArgSource::Stdin {
                    stdin: in_spec.to_string(),
                }],
            )],
            metadata_json: None,
            registered_by: None,
            supported_model_types: Vec::new(),
            default_model_spec: None,
        }
    }

    /// `make_test_cap` plus a declared `CapOutput`. Required for tests that pass
    /// a strand built from this cap into `Strand::knit` or
    /// `Strand::to_machine_notation`, since the resolver reads the output slot.
    fn make_test_cap_with_arg(in_spec: &str, out_spec: &str, op: &str, title: &str) -> Cap {
        use crate::cap::definition::CapOutput;

        let mut cap = make_test_cap(in_spec, out_spec, op, title);
        cap.output = Some(CapOutput::new(out_spec.to_string(), title.to_string()));
        cap
    }

    // TEST1150: Adding one cap creates one edge and makes its output reachable in one step.
    #[test]
    fn test1150_add_cap_and_basic_traversal() {
        let mut graph = LiveCapFab::new();

        let cap = make_test_cap(
            "media:ext=pdf",
            "media:digitized-text",
            "extract_text",
            "Extract Text",
        );
        graph.add_cap(&cap);
        graph.set_bookends(&all_bookends(&[cap.clone()]));

        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.nodes.len(), 2);

        let source = MediaUrn::from_string("media:ext=pdf").unwrap();
        let targets = graph.get_reachable_targets(&source, false, 5);

        // Reachable targets include only media:digitized-text
        // (via the cap, depth 1). Collect is not synthesized
        // during reachability traversal — cardinality variants
        // are handled by the plan builder at execution time.
        let digitized_text = MediaUrn::from_string("media:digitized-text").unwrap();
        let cap_target = targets
            .iter()
            .find(|t| t.media_def.is_equivalent(&digitized_text).unwrap_or(false));
        assert!(cap_target.is_some(), "digitized-text should be reachable");
        assert_eq!(cap_target.unwrap().min_path_length, 1);
    }

    // TEST1151: Exact target lookup prefers the direct singular or list-producing path over longer alternatives.
    #[test]
    fn test1151_exact_vs_conformance_matching() {
        // First verify our assumption about is_equivalent
        let singular = MediaUrn::from_string("media:analysis-result").unwrap();
        let list = MediaUrn::from_string("media:analysis-result;list").unwrap();

        // These should NOT be equivalent
        assert!(
            !singular.is_equivalent(&list).unwrap(),
            "singular and list should NOT be equivalent"
        );
        assert!(
            !list.is_equivalent(&singular).unwrap(),
            "list and singular should NOT be equivalent (reverse check)"
        );

        let mut graph = LiveCapFab::new();

        // Add cap: pdf -> result (singular)
        let cap1 = make_test_cap(
            "media:ext=pdf",
            "media:analysis-result",
            "analyze",
            "Analyze PDF",
        );
        graph.add_cap(&cap1);

        // Add cap: pdf -> result;list (plural)
        let cap2 = make_test_cap(
            "media:ext=pdf",
            "media:analysis-result;list",
            "analyze_multi",
            "Analyze PDF Multi",
        );
        graph.add_cap(&cap2);

        let source = MediaUrn::from_string("media:ext=pdf").unwrap();

        // Query for EXACT target: singular result
        // Two valid paths exist:
        // 1. Direct: pdf → result (via analyze) — 1 cap step, 1 total step
        // 2. Indirect: pdf → result;list (via analyze_multi) → ForEach → result — 1 cap step, 2 total steps
        // Both are valid. Path 1 ranks first (fewer total steps at same cap count).
        let target_singular = MediaUrn::from_string("media:analysis-result").unwrap();
        let paths_singular =
            graph.find_paths_to_exact_target(&source, &target_singular, false, 5, 10);

        assert!(
            paths_singular.len() >= 1,
            "singular query should find at least 1 path"
        );
        assert_eq!(
            paths_singular[0].steps[0].title(),
            "Analyze PDF",
            "First path should be the direct cap (fewer total steps)"
        );

        // Query for EXACT target: result;list (plural)
        // Two valid paths exist:
        // 1. Direct: pdf → result;list (via analyze_multi) — 1 cap step
        // 2. Indirect: pdf → result (via analyze) + Collect → result;list — 1 cap step + Collect
        // Both are valid. The direct path is shorter (fewer total steps).
        let target_plural = MediaUrn::from_string("media:analysis-result;list").unwrap();
        let paths_plural = graph.find_paths_to_exact_target(&source, &target_plural, false, 5, 10);

        assert!(
            paths_plural.len() >= 1,
            "list query should find at least 1 path"
        );
        // The shortest path (fewest cap steps, then fewest total steps) should be the direct one
        assert_eq!(
            paths_plural[0].steps[0].title(),
            "Analyze PDF Multi",
            "First path should be the direct cap (fewer total steps)"
        );
    }

    // TEST1152: Path finding returns the expected two-cap chain through an intermediate media type.
    #[test]
    fn test1152_multi_step_path() {
        let mut graph = LiveCapFab::new();

        // pdf -> digitized-text
        let cap1 = make_test_cap(
            "media:ext=pdf",
            "media:digitized-text",
            "extract",
            "Extract",
        );
        // digitized-text -> summary-text
        let cap2 = make_test_cap(
            "media:digitized-text",
            "media:summary-text",
            "summarize",
            "Summarize",
        );

        graph.add_cap(&cap1);
        graph.add_cap(&cap2);

        let source = MediaUrn::from_string("media:ext=pdf").unwrap();
        let target = MediaUrn::from_string("media:summary-text").unwrap();

        let paths = graph.find_paths_to_exact_target(&source, &target, false, 5, 10);

        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].total_steps, 2);
        assert_eq!(paths[0].steps[0].title(), "Extract");
        assert_eq!(paths[0].steps[1].title(), "Summarize");
    }

    // TEST1153: Repeated path searches return the same path order for the same graph and target.
    #[test]
    fn test1153_deterministic_ordering() {
        let mut graph = LiveCapFab::new();

        // Two paths to the same target with different specificities
        let cap1 = make_test_cap(
            "media:ext=pdf",
            "media:digitized-text",
            "extract_a",
            "Extract A",
        );
        let cap2 = make_test_cap(
            "media:ext=pdf",
            "media:digitized-text",
            "extract_b",
            "Extract B",
        );

        graph.add_cap(&cap1);
        graph.add_cap(&cap2);

        let source = MediaUrn::from_string("media:ext=pdf").unwrap();
        let target = MediaUrn::from_string("media:digitized-text").unwrap();

        // Run multiple times - should always get the same order
        let paths1 = graph.find_paths_to_exact_target(&source, &target, false, 5, 10);
        let paths2 = graph.find_paths_to_exact_target(&source, &target, false, 5, 10);

        assert_eq!(paths1.len(), paths2.len());
        for (p1, p2) in paths1.iter().zip(paths2.iter()) {
            // Determinism: two runs of find_paths_to_exact_target
            // over the same input must produce paths in the
            // same order with the same cap URNs at each step.
            // CapUrn equivalence is checked structurally via
            // `is_equivalent`, not via string comparison.
            let u1 = p1.steps[0].cap_urn().expect("first step is a cap");
            let u2 = p2.steps[0].cap_urn().expect("first step is a cap");
            assert!(
                u1.is_equivalent(u2),
                "determinism: first cap URN differs across runs: {} vs {}",
                u1,
                u2
            );
        }
    }

    // TEST1154: Syncing from caps replaces the existing graph contents with the new cap set.
    #[test]
    fn test1154_sync_from_caps() {
        let mut graph = LiveCapFab::new();

        let caps = vec![
            make_test_cap("media:ext=pdf", "media:digitized-text", "op1", "Op1"),
            make_test_cap("media:digitized-text", "media:summary-text", "op2", "Op2"),
        ];

        let __caps = &caps;

        graph.sync_from_caps(__caps, &all_bookends(__caps));

        assert_eq!(graph.edges.len(), 2);
        assert_eq!(graph.nodes.len(), 3);

        // Sync again with different caps - should replace
        let new_caps = vec![make_test_cap(
            "media:image",
            "media:digitized-text",
            "ocr",
            "OCR",
        )];

        let __caps = &new_caps;

        graph.sync_from_caps(__caps, &all_bookends(__caps));

        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.nodes.len(), 2);
    }

    // ==========================================================================
    // PATH FINDING TESTS (moved from plan_builder.rs)
    // ==========================================================================
    // These tests verify path finding behavior. Availability filtering is now
    // implicit - only caps added to the graph via sync_from_caps are available.

    // TEST772: Tests find_paths_to_exact_target() finds multi-step paths
    // Verifies that paths through intermediate nodes are found correctly
    #[test]
    fn test772_find_paths_finds_multi_step_paths() {
        let mut graph = LiveCapFab::new();

        let cap1 = make_test_cap("media:a", "media:b", "step1", "A to B");
        let cap2 = make_test_cap("media:b", "media:c", "step2", "B to C");

        graph.add_cap(&cap1);
        graph.add_cap(&cap2);

        let source = MediaUrn::from_string("media:a").unwrap();
        let target = MediaUrn::from_string("media:c").unwrap();

        let paths = graph.find_paths_to_exact_target(&source, &target, false, 5, 10);

        assert_eq!(
            paths.len(),
            1,
            "Should find one path through intermediate node"
        );
        assert_eq!(
            paths[0].steps.len(),
            2,
            "Path should have 2 steps (A->B, B->C)"
        );
        assert_eq!(paths[0].steps[0].title(), "A to B");
        assert_eq!(paths[0].steps[1].title(), "B to C");
    }

    // TEST773: Tests find_paths_to_exact_target() returns empty when no path exists
    // Verifies that pathfinding returns no paths when target is unreachable
    #[test]
    fn test773_find_paths_returns_empty_when_no_path() {
        let mut graph = LiveCapFab::new();

        // Only add cap A->B, not B->C
        let cap1 = make_test_cap("media:a", "media:b", "step1", "A to B");
        graph.add_cap(&cap1);

        let source = MediaUrn::from_string("media:a").unwrap();
        let target = MediaUrn::from_string("media:c").unwrap();

        let paths = graph.find_paths_to_exact_target(&source, &target, false, 5, 10);

        assert!(
            paths.is_empty(),
            "Should find no paths when target is unreachable"
        );
    }

    // TEST774: Tests get_reachable_targets() returns all reachable targets
    // Verifies that reachable targets include direct cap targets and
    // cardinality variants (list versions via Collect)
    #[test]
    fn test774_get_reachable_targets_finds_all_targets() {
        let mut graph = LiveCapFab::new();

        let cap1 = make_test_cap("media:a", "media:b", "step1", "A to B");
        let cap2 = make_test_cap("media:a", "media:d", "step3", "A to D");

        graph.add_cap(&cap1);
        graph.add_cap(&cap2);
        graph.set_bookends(&all_bookends(&[cap1.clone(), cap2.clone()]));

        let source = MediaUrn::from_string("media:a").unwrap();
        let targets = graph.get_reachable_targets(&source, false, 5);

        let media_b = MediaUrn::from_string("media:b").unwrap();
        let media_d = MediaUrn::from_string("media:d").unwrap();
        let reaches = |needle: &MediaUrn| -> bool {
            targets
                .iter()
                .any(|t| t.media_def.is_equivalent(needle).unwrap_or(false))
        };
        assert!(reaches(&media_b), "B should be reachable");
        assert!(reaches(&media_d), "D should be reachable");
        // Collect is not synthesized during reachability
        // traversal — see `get_outgoing_edges`. Cardinality
        // variants (e.g. `media:a;list`) therefore are NOT in
        // the reachability graph. The plan builder pairs
        // Collect with ForEach implicitly at execution time.
    }

    // TEST777: Tests type checking prevents using PDF-specific cap with PNG input
    // Verifies that media type compatibility is enforced during pathfinding
    #[test]
    fn test777_type_mismatch_pdf_cap_does_not_match_png_input() {
        let mut graph = LiveCapFab::new();

        // Only add PDF->text cap
        let pdf_to_text = make_test_cap(
            "media:ext=pdf",
            "media:enc=utf-8",
            "pdf2text",
            "PDF to Text",
        );
        graph.add_cap(&pdf_to_text);

        // Try to find path from PNG (not PDF)
        let source = MediaUrn::from_string("media:ext=png;image").unwrap();
        let target = MediaUrn::from_string("media:enc=utf-8").unwrap();

        let paths = graph.find_paths_to_exact_target(&source, &target, false, 5, 10);

        assert!(
            paths.is_empty(),
            "Should NOT find path from PNG to text via PDF cap"
        );
    }

    // TEST778: Tests type checking prevents using PNG-specific cap with PDF input
    // Verifies that media type compatibility is enforced during pathfinding
    #[test]
    fn test778_type_mismatch_png_cap_does_not_match_pdf_input() {
        let mut graph = LiveCapFab::new();

        // Only add PNG->thumbnail cap
        let png_to_thumb = make_test_cap(
            "media:ext=png;image",
            "media:thumbnail",
            "png2thumb",
            "PNG to Thumbnail",
        );
        graph.add_cap(&png_to_thumb);

        // Try to find path from PDF (not PNG)
        let source = MediaUrn::from_string("media:ext=pdf").unwrap();
        let target = MediaUrn::from_string("media:thumbnail").unwrap();

        let paths = graph.find_paths_to_exact_target(&source, &target, false, 5, 10);

        assert!(
            paths.is_empty(),
            "Should NOT find path from PDF to thumbnail via PNG cap"
        );
    }

    // TEST779: Tests get_reachable_targets() only returns targets reachable via type-compatible caps
    // Verifies that PNG and PDF inputs reach different cap targets (not each other's)
    #[test]
    fn test779_get_reachable_targets_respects_type_matching() {
        let mut graph = LiveCapFab::new();

        let pdf_to_text = make_test_cap(
            "media:ext=pdf",
            "media:enc=utf-8",
            "pdf2text",
            "PDF to Text",
        );
        let png_to_thumb = make_test_cap(
            "media:ext=png;image",
            "media:thumbnail",
            "png2thumb",
            "PNG to Thumbnail",
        );

        graph.add_cap(&pdf_to_text);
        graph.add_cap(&png_to_thumb);
        graph.set_bookends(&all_bookends(&[pdf_to_text.clone(), png_to_thumb.clone()]));

        // PNG should reach thumbnail (cap target) but NOT text (PDF-only cap)
        let png_source = MediaUrn::from_string("media:ext=png;image").unwrap();
        let png_targets = graph.get_reachable_targets(&png_source, false, 5);
        let media_thumbnail = MediaUrn::from_string("media:thumbnail").unwrap();
        let media_textable = MediaUrn::from_string("media:enc=utf-8").unwrap();
        assert!(
            png_targets
                .iter()
                .any(|t| t.media_def.is_equivalent(&media_thumbnail).unwrap_or(false)),
            "PNG should reach thumbnail"
        );
        assert!(
            !png_targets
                .iter()
                .any(|t| t.media_def.is_equivalent(&media_textable).unwrap_or(false)),
            "PNG should NOT reach textable"
        );

        // PDF should reach text (cap target) but NOT thumbnail (PNG-only cap)
        let pdf_source = MediaUrn::from_string("media:ext=pdf").unwrap();
        let pdf_targets = graph.get_reachable_targets(&pdf_source, false, 5);
        assert!(
            pdf_targets
                .iter()
                .any(|t| t.media_def.is_equivalent(&media_textable).unwrap_or(false)),
            "PDF should reach textable"
        );
        assert!(
            !pdf_targets
                .iter()
                .any(|t| t.media_def.is_equivalent(&media_thumbnail).unwrap_or(false)),
            "PDF should NOT reach thumbnail"
        );
    }

    // TEST781: Tests find_paths_to_exact_target() enforces type compatibility across multi-step chains
    // Verifies that paths are only found when all intermediate types are compatible
    #[test]
    fn test781_find_paths_respects_type_chain() {
        let mut graph = LiveCapFab::new();

        let resize_png = make_test_cap(
            "media:ext=png;image",
            "media:resized-png",
            "resize",
            "Resize PNG",
        );
        let to_thumb = make_test_cap(
            "media:resized-png",
            "media:thumbnail",
            "thumb",
            "To Thumbnail",
        );

        graph.add_cap(&resize_png);
        graph.add_cap(&to_thumb);

        // PNG should find path through resized-png to thumbnail
        let png_source = MediaUrn::from_string("media:ext=png;image").unwrap();
        let thumb_target = MediaUrn::from_string("media:thumbnail").unwrap();
        let png_paths = graph.find_paths_to_exact_target(&png_source, &thumb_target, false, 5, 10);
        assert_eq!(
            png_paths.len(),
            1,
            "Should find 1 path from PNG to thumbnail"
        );
        assert_eq!(png_paths[0].steps.len(), 2, "Path should have 2 steps");

        // PDF should NOT find path to thumbnail (no PDF->resized-png cap)
        let pdf_source = MediaUrn::from_string("media:ext=pdf").unwrap();
        let pdf_paths = graph.find_paths_to_exact_target(&pdf_source, &thumb_target, false, 5, 10);
        assert!(
            pdf_paths.is_empty(),
            "Should find NO paths from PDF to thumbnail (type mismatch)"
        );
    }

    // TEST788: ForEach is only synthesized when is_sequence=true
    // With scalar input (is_sequence=false), disbind output goes directly to choose
    // since media:enc=utf-8;page conforms to media:enc=utf-8.
    // With sequence input (is_sequence=true), ForEach splits the sequence so each
    // item can be processed by disbind individually, then choose.
    #[test]
    fn test788_foreach_only_with_sequence_input() {
        let mut graph = LiveCapFab::new();

        let disbind = make_test_cap(
            "media:ext=pdf",
            "media:enc=utf-8;page",
            "disbind",
            "Disbind PDF",
        );

        let choose = make_test_cap(
            "media:enc=utf-8",
            "media:decision;fmt=json;record",
            "choose",
            "Make a Decision",
        );

        let __caps = &[disbind, choose];

        graph.sync_from_caps(__caps, &all_bookends(__caps));
        assert_eq!(
            graph.edges.len(),
            2,
            "Graph should contain exactly 2 Cap edges"
        );

        let source = MediaUrn::from_string("media:ext=pdf").unwrap();
        let target = MediaUrn::from_string("media:decision;fmt=json;record").unwrap();

        // Scalar input: no ForEach, direct path disbind → choose
        let scalar_paths = graph.find_paths_to_exact_target(&source, &target, false, 10, 20);
        let has_foreach_scalar = scalar_paths.iter().any(|p| {
            p.steps
                .iter()
                .any(|s| matches!(s.step_type, StrandStepType::ForEach { .. }))
        });
        assert!(
            !has_foreach_scalar,
            "Scalar input should NOT produce ForEach"
        );
        assert!(
            !scalar_paths.is_empty(),
            "Should find direct path disbind → choose"
        );

        // Sequence input: ForEach should appear
        let seq_paths = graph.find_paths_to_exact_target(&source, &target, true, 10, 20);
        let has_foreach_seq = seq_paths.iter().any(|p| {
            p.steps
                .iter()
                .any(|s| matches!(s.step_type, StrandStepType::ForEach { .. }))
        });
        assert!(
            has_foreach_seq,
            "Sequence input should produce ForEach step"
        );
    }

    // TEST791: Tests sync_from_cap_urns actually adds edges
    #[tokio::test]
    async fn test791_sync_from_cap_urns_adds_edges() {
        use crate::FabricRegistry;
        use std::sync::Arc;

        // Create a registry with test caps
        let registry = FabricRegistry::new_for_test();
        let disbind = make_test_cap(
            "media:ext=pdf",
            "media:enc=utf-8;page",
            "disbind",
            "Disbind PDF",
        );
        let choose = make_test_cap(
            "media:enc=utf-8",
            "media:decision;fmt=json;record",
            "choose",
            "Make a Decision",
        );
        registry.add_caps_to_cache(vec![disbind.clone(), choose.clone()]);

        // Create cap URN strings as cartridges would report them
        let cap_urns: Vec<String> = vec![disbind.urn.to_string(), choose.urn.to_string()];

        // Sync from URNs. The bookend set treats every URN appearing in
        // either cap as eligible — this is a registry-graph sync test,
        // not a bookend-filter test.
        let bookends = all_bookends(&[disbind.clone(), choose.clone()]);
        let mut graph = LiveCapFab::new();
        graph
            .sync_from_cap_urns(&cap_urns, &Arc::new(registry), &bookends)
            .await;

        // Should have exactly 2 Cap edges (no pre-computed cardinality edges)
        assert_eq!(
            graph.edges.len(),
            2,
            "Should have exactly 2 Cap edges, got {}",
            graph.edges.len()
        );
    }

    // TEST790: Tests identity_urn is specific and doesn't match everything
    #[test]
    fn test790_identity_urn_is_specific() {
        let identity = crate::standard::caps::identity_urn();

        // The identity URN should have wildcard in/out specs (media:)
        assert_eq!(identity.in_spec(), "media:");
        assert_eq!(identity.out_spec(), "media:");

        // A specific cap should NOT be equivalent to identity
        let specific_cap = crate::CapUrn::from_string(
            r#"cap:disbind;in="media:ext=pdf";out="media:disbound-page;enc=utf-8""#,
        )
        .unwrap();

        assert!(
            !specific_cap.is_equivalent(&identity),
            "A specific disbind cap should NOT be equivalent to identity"
        );
    }

    // TEST789: Tests that caps loaded from JSON have correct in_spec/out_spec
    #[test]
    fn test789_cap_from_json_has_valid_specs() {
        let json = r#"{
            "urn": "cap:disbind;in=\"media:ext=pdf\";out=\"media:disbound-page;enc=utf-8\"",
            "aliases": ["disbind"],
            "title": "Disbind PDF",
            "args": [],
            "output": null
        }"#;

        let cap: crate::Cap = serde_json::from_str(json).expect("Failed to parse cap JSON");

        let in_spec = cap.urn.in_spec();
        let out_spec = cap.urn.out_spec();

        assert!(!in_spec.is_empty(), "in_spec should not be empty");
        assert!(!out_spec.is_empty(), "out_spec should not be empty");
        assert_eq!(in_spec, "media:ext=pdf");
        assert!(
            out_spec.contains("disbound-page"),
            "out_spec should contain disbound-page: {}",
            out_spec
        );
    }

    // TEST787: Tests find_paths_to_exact_target() sorts paths by length, preferring shorter ones
    // Verifies that among multiple paths, the shortest is ranked first
    #[test]
    fn test787_find_paths_sorting_prefers_shorter() {
        let mut graph = LiveCapFab::new();

        // Direct path: format-a -> format-c
        let direct = make_test_cap("media:format-a", "media:format-c", "direct", "Direct");
        // Indirect path: format-a -> format-b -> format-c
        let step1 = make_test_cap("media:format-a", "media:format-b", "step1", "Step 1");
        let step2 = make_test_cap("media:format-b", "media:format-c", "step2", "Step 2");

        graph.add_cap(&direct);
        graph.add_cap(&step1);
        graph.add_cap(&step2);

        let source = MediaUrn::from_string("media:format-a").unwrap();
        let target = MediaUrn::from_string("media:format-c").unwrap();

        let paths = graph.find_paths_to_exact_target(&source, &target, false, 5, 10);

        assert!(
            paths.len() >= 2,
            "Should find at least 2 paths (got {})",
            paths.len()
        );
        assert_eq!(
            paths[0].steps.len(),
            1,
            "Shortest path should be first (1 step)"
        );
        assert_eq!(paths[0].steps[0].title(), "Direct");
    }

    // TEST1110: Strand serializes to JSON and deserializes back preserving all step types
    #[test]
    fn test1110_strand_round_trips_through_serde_without_losing_step_types() {
        let strand = Strand {
            steps: vec![
                StrandStep::new(
                    StrandStepType::Cap {
                        cap_urn: CapUrn::from_string(
                            r#"cap:disbind;in="media:ext=pdf";out="media:enc=utf-8;page""#,
                        )
                        .unwrap(),
                        title: "Disbind PDF Into Pages".to_string(),
                        specificity: 4,
                        input_is_sequence: false,
                        output_is_sequence: true,
                        inputs: vec![CapInput {
                            arg_urn: MediaUrn::from_string("media:ext=pdf").unwrap(),
                            source: ArgSourceRef::StrandInput,
                        }],
                    },
                    MediaUrn::from_string("media:ext=pdf").unwrap(),
                    MediaUrn::from_string("media:enc=utf-8;page").unwrap(),
                ),
                StrandStep::new(
                    StrandStepType::ForEach {
                        media_def: MediaUrn::from_string("media:enc=utf-8;page").unwrap(),
                    },
                    MediaUrn::from_string("media:enc=utf-8;page").unwrap(),
                    MediaUrn::from_string("media:enc=utf-8;page").unwrap(),
                ),
            ],
            source_media_urn: MediaUrn::from_string("media:ext=pdf").unwrap(),
            target_media_urn: MediaUrn::from_string("media:enc=utf-8;page").unwrap(),
            total_steps: 2,
            cap_step_count: 1,
            description: "Transform PDF into text pages".to_string(),
        };

        let json = serde_json::to_string(&strand).expect("strand should serialize");
        let recovered: Strand = serde_json::from_str(&json).expect("strand should deserialize");
        // The stable per-step token_id must survive the serde round-trip verbatim —
        // it's the identity the run's live updates key off, so losing/regenerating
        // it would silently break update→element routing.
        assert_eq!(recovered.steps.len(), strand.steps.len());
        for (orig, got) in strand.steps.iter().zip(recovered.steps.iter()) {
            assert_eq!(
                orig.token_id, got.token_id,
                "token_id must round-trip unchanged"
            );
        }
        assert_ne!(
            strand.steps[0].token_id, strand.steps[1].token_id,
            "distinct steps get distinct token_ids",
        );

        let expected_source = MediaUrn::from_string("media:ext=pdf").unwrap();
        let expected_target = MediaUrn::from_string("media:enc=utf-8;page").unwrap();
        assert!(
            recovered
                .source_media_urn
                .is_equivalent(&expected_source)
                .expect("URN equivalence check"),
            "source_media_urn must round-trip structurally as media:ext=pdf"
        );
        assert!(
            recovered
                .target_media_urn
                .is_equivalent(&expected_target)
                .expect("URN equivalence check"),
            "target_media_urn must round-trip structurally as media:enc=utf-8;page"
        );
        assert_eq!(recovered.steps.len(), 2);
        assert!(matches!(
            recovered.steps[0].step_type,
            StrandStepType::Cap { .. }
        ));
        assert!(matches!(
            recovered.steps[1].step_type,
            StrandStepType::ForEach { .. }
        ));
    }

    // TEST1111: ForEach works for user-provided list sources not in the graph.
    // This is the original bug — media:enc=utf-8;ext=txt;list is a user import source,
    // not a cap output. Previously, no ForEach edge existed for it because
    // insert_cardinality_transitions() only pre-computed edges for cap outputs.
    // With dynamic synthesis, ForEach is available for ANY list source.
    #[test]
    fn test1111_foreach_for_user_provided_list_source() {
        let mut graph = LiveCapFab::new();

        // Cap: text → decision (accepts singular enc=utf-8)
        let make_decision = make_test_cap(
            "media:enc=utf-8",
            "media:decision;fmt=json;record",
            "make_decision",
            "Make Decision",
        );
        let __caps = &[make_decision];
        graph.sync_from_caps(__caps, &all_bookends(__caps));

        // Source is a user-provided list that no cap outputs
        let source = MediaUrn::from_string("media:enc=utf-8;ext=txt;list").unwrap();
        let target = MediaUrn::from_string("media:decision;fmt=json;record").unwrap();

        // User provides multiple files → is_sequence=true
        let paths = graph.find_paths_to_exact_target(&source, &target, true, 10, 20);

        // Expected path: ForEach → make_decision
        // ForEach iterates over items, make_decision accepts media:enc=utf-8
        let path = paths.iter().find(|p| {
            p.steps.len() == 2
                && matches!(p.steps[0].step_type, StrandStepType::ForEach { .. })
                && matches!(p.steps[1].step_type, StrandStepType::Cap { .. })
        });

        assert!(
            path.is_some(),
            "Should find path: ForEach → make_decision. \
             User-provided list source media:enc=utf-8;ext=txt;list must be iterable. \
             Found {} paths: {:?}",
            paths.len(),
            paths.iter().map(|p| &p.description).collect::<Vec<_>>()
        );

        let path = path.unwrap();
        // Verify the ForEach step correctly derives item type from list source
        if let StrandStepType::ForEach { media_def } = &path.steps[0].step_type {
            // ForEach doesn't change the media URN — same type, different shape (is_sequence)
            assert!(
                media_def.is_equivalent(&source).unwrap(),
                "ForEach media_def should be the same as source"
            );
        }
    }

    // TEST1112: Collect is not synthesized during path finding.
    // Reaching a list target type requires the cap itself to output a list type.
    #[test]
    fn test1112_no_collect_in_path_finding() {
        let mut graph = LiveCapFab::new();

        let summarize = make_test_cap(
            "media:enc=utf-8",
            "media:enc=utf-8;summary",
            "summarize",
            "Summarize",
        );
        let __caps = &[summarize];
        graph.sync_from_caps(__caps, &all_bookends(__caps));

        let source = MediaUrn::from_string("media:enc=utf-8").unwrap();
        // enc=utf-8;list;summary is a different semantic type — can't reach it
        // without a cap that outputs it or a Collect step (not synthesized)
        let target = MediaUrn::from_string("media:enc=utf-8;list;summary").unwrap();

        let paths = graph.find_paths_to_exact_target(&source, &target, false, 10, 20);
        assert!(
            paths.is_empty(),
            "Should NOT find path to list type without a cap that produces it"
        );
    }

    // TEST1113: Multi-cap path without Collect — Collect is not synthesized
    #[test]
    fn test1113_multi_cap_path_no_collect() {
        let mut graph = LiveCapFab::new();

        let disbind = make_test_cap(
            "media:ext=pdf",
            "media:enc=utf-8;page",
            "disbind",
            "Disbind PDF",
        );
        let summarize = make_test_cap(
            "media:enc=utf-8;page",
            "media:enc=utf-8;summary",
            "summarize",
            "Summarize Page",
        );
        let __caps = &[disbind, summarize];
        graph.sync_from_caps(__caps, &all_bookends(__caps));

        // Scalar path: pdf → disbind → enc=utf-8;page → summarize → enc=utf-8;summary
        let source = MediaUrn::from_string("media:ext=pdf").unwrap();
        let target = MediaUrn::from_string("media:enc=utf-8;summary").unwrap();

        let paths = graph.find_paths_to_exact_target(&source, &target, false, 10, 20);
        assert!(!paths.is_empty(), "Should find direct cap path");
        assert_eq!(paths[0].cap_step_count, 2, "Should have 2 cap steps");
    }

    // TEST1114: Graph stores only Cap edges after sync
    #[test]
    fn test1114_graph_stores_only_cap_edges() {
        let mut graph = LiveCapFab::new();

        let caps = vec![
            make_test_cap(
                "media:ext=pdf",
                "media:enc=utf-8;page",
                "disbind",
                "Disbind",
            ),
            make_test_cap(
                "media:enc=utf-8;page",
                "media:enc=utf-8;summary",
                "summarize",
                "Summarize",
            ),
            make_test_cap(
                "media:enc=utf-8",
                "media:decision;fmt=json;record",
                "decide",
                "Decide",
            ),
        ];

        let __caps = &caps;

        graph.sync_from_caps(__caps, &all_bookends(__caps));

        // All stored edges must be Cap edges
        assert_eq!(graph.edges.len(), 3, "Should have exactly 3 Cap edges");
        for edge in &graph.edges {
            assert!(
                edge.is_cap(),
                "Stored edge {:?} should be a Cap edge, not a cardinality transition",
                edge.edge_type
            );
        }
    }

    // TEST1115: ForEach is synthesized when is_sequence=true AND caps can consume items
    #[test]
    fn test1115_dynamic_foreach_with_is_sequence() {
        let mut graph = LiveCapFab::new();

        // Need a cap that accepts the source type for ForEach to be synthesized
        let cap = make_test_cap(
            "media:enc=utf-8",
            "media:enc=utf-8;summary",
            "summarize",
            "Summarize",
        );
        let __caps = &[cap];
        graph.sync_from_caps(__caps, &all_bookends(__caps));

        let source = MediaUrn::from_string("media:enc=utf-8").unwrap();
        let edges = graph.get_outgoing_edges(&source, true);

        let foreach_edge = edges
            .iter()
            .find(|(e, _)| matches!(e.edge_type, LiveMachinePlanEdgeType::ForEach));
        assert!(
            foreach_edge.is_some(),
            "Should synthesize ForEach when is_sequence=true and caps exist"
        );

        let (fe, next_is_seq) = foreach_edge.unwrap();
        assert!(!next_is_seq, "ForEach should flip is_sequence to false");
        assert!(
            fe.from_spec.is_equivalent(&source).unwrap(),
            "ForEach from_spec should be the source"
        );
        assert!(
            fe.to_spec.is_equivalent(&source).unwrap(),
            "ForEach to_spec should be the same URN"
        );
    }

    // TEST1116: Collect is never synthesized during path finding
    #[test]
    fn test1116_collect_never_synthesized() {
        let graph = LiveCapFab::new();

        let source = MediaUrn::from_string("media:enc=utf-8;page").unwrap();

        // Neither scalar nor sequence should produce Collect
        let edges_scalar = graph.get_outgoing_edges(&source, false);
        let collect_scalar = edges_scalar
            .iter()
            .find(|(e, _)| matches!(e.edge_type, LiveMachinePlanEdgeType::Collect));
        assert!(
            collect_scalar.is_none(),
            "Should NOT synthesize Collect for scalar"
        );

        let edges_seq = graph.get_outgoing_edges(&source, true);
        let collect_seq = edges_seq
            .iter()
            .find(|(e, _)| matches!(e.edge_type, LiveMachinePlanEdgeType::Collect));
        assert!(
            collect_seq.is_none(),
            "Should NOT synthesize Collect for sequence"
        );
    }

    // TEST1117: ForEach is NOT synthesized when is_sequence=false
    #[test]
    fn test1117_no_foreach_when_not_sequence() {
        let mut graph = LiveCapFab::new();

        // Even with caps that could consume, ForEach requires is_sequence=true
        let cap = make_test_cap(
            "media:enc=utf-8",
            "media:enc=utf-8;summary",
            "summarize",
            "Summarize",
        );
        let __caps = &[cap];
        graph.sync_from_caps(__caps, &all_bookends(__caps));

        let source = MediaUrn::from_string("media:enc=utf-8").unwrap();
        let edges = graph.get_outgoing_edges(&source, false);

        let foreach_edge = edges
            .iter()
            .find(|(e, _)| matches!(e.edge_type, LiveMachinePlanEdgeType::ForEach));
        assert!(
            foreach_edge.is_none(),
            "Should NOT synthesize ForEach when is_sequence=false"
        );
    }

    // TEST1118: ForEach not synthesized without cap consumers even with is_sequence=true
    #[test]
    fn test1118_no_foreach_without_cap_consumers() {
        let graph = LiveCapFab::new();

        let source = MediaUrn::from_string("media:enc=utf-8").unwrap();
        // Empty graph — no caps to consume items
        let edges = graph.get_outgoing_edges(&source, true);

        let foreach_edge = edges
            .iter()
            .find(|(e, _)| matches!(e.edge_type, LiveMachinePlanEdgeType::ForEach));
        assert!(
            foreach_edge.is_none(),
            "Should NOT synthesize ForEach without cap consumers"
        );
    }

    // TEST8064: a sequence-consuming cap may be reached directly from sequence
    // data, but never through a dangling ForEach boundary. The latter was emitted
    // as `ForEach -> concat` and then correctly rejected by machine resolution,
    // aborting the transmute strand stream. A ForEach followed by a SCALAR cap
    // stays legal — that is the map half of the ordinary map-then-fold plan
    // (TEST1418) — so the invariant is about what may follow the boundary, not
    // about ForEach appearing at all.
    #[test]
    fn test8064_sequence_consumer_never_follows_foreach_directly() {
        use crate::cap::registry::FabricRegistry;

        let mut concat = make_test_cap_with_arg(
            "media:enc=utf-8",
            "media:enc=utf-8;ext=txt",
            "concat",
            "Concat Text",
        );
        concat.args[0].is_sequence = true;
        let scalar_consumer = make_test_cap_with_arg(
            "media:enc=utf-8",
            "media:enc=utf-8;summary",
            "summarize",
            "Summarize Text",
        );
        let scalar_consumer_for_registry = scalar_consumer.clone();
        let mut graph = LiveCapFab::new();
        graph.sync_from_caps(
            &[concat.clone(), scalar_consumer.clone()],
            &all_bookends(&[concat.clone(), scalar_consumer]),
        );

        let source = MediaUrn::from_string("media:enc=utf-8;page").unwrap();
        let target = MediaUrn::from_string("media:enc=utf-8;ext=txt").unwrap();
        let paths = graph.find_paths_to_exact_target(&source, &target, true, 4, 20);

        assert!(
            !paths.is_empty(),
            "the direct sequence -> concat path must exist"
        );
        assert!(
            paths.iter().any(|path| {
                path.steps.len() == 1
                    && matches!(
                        &path.steps[0].step_type,
                        StrandStepType::Cap {
                            input_is_sequence: true,
                            ..
                        }
                    )
            }),
            "sequence data must reach the sequence consumer directly, with no boundary"
        );
        for path in &paths {
            for pair in path.steps.windows(2) {
                if matches!(pair[0].step_type, StrandStepType::ForEach { .. }) {
                    assert!(
                        matches!(
                            &pair[1].step_type,
                            StrandStepType::Cap {
                                input_is_sequence: false,
                                ..
                            }
                        ),
                        "a ForEach boundary must qualify a scalar-input cap, got {:?}",
                        pair[1].step_type
                    );
                }
            }
        }

        let registry = FabricRegistry::new_for_test();
        registry.add_caps_to_cache(vec![concat, scalar_consumer_for_registry]);
        for path in paths {
            path.to_machine_notation(&registry)
                .expect("every enumerated path must satisfy machine cardinality invariants");
        }
    }

    // TEST1119: Strand::knit returns a single-strand Machine via the new
    // resolver. Smoke test the registry-threaded API end-to-end.
    #[test]
    fn test1119_strand_knit_with_registry_returns_single_strand_machine() {
        use crate::cap::registry::FabricRegistry;

        let cap = make_test_cap_with_arg(
            "media:ext=pdf",
            "media:enc=utf-8;ext=txt",
            "extract",
            "Extract",
        );
        let registry = FabricRegistry::new_for_test();
        registry.add_caps_to_cache(vec![cap]);

        let cap_urn =
            CapUrn::from_string("cap:extract;in=\"media:ext=pdf\";out=\"media:enc=utf-8;ext=txt\"")
                .unwrap();
        let strand = Strand {
            steps: vec![StrandStep::new(
                StrandStepType::Cap {
                    cap_urn: cap_urn.clone(),
                    title: "Extract".to_string(),
                    specificity: 0,
                    input_is_sequence: false,
                    output_is_sequence: false,
                    inputs: vec![CapInput {
                        arg_urn: MediaUrn::from_string("media:ext=pdf").unwrap(),
                        source: ArgSourceRef::StrandInput,
                    }],
                },
                MediaUrn::from_string("media:ext=pdf").unwrap(),
                MediaUrn::from_string("media:enc=utf-8;ext=txt").unwrap(),
            )],
            source_media_urn: MediaUrn::from_string("media:ext=pdf").unwrap(),
            target_media_urn: MediaUrn::from_string("media:enc=utf-8;ext=txt").unwrap(),
            total_steps: 1,
            cap_step_count: 1,
            description: "pdf to txt".to_string(),
        };

        let machine = strand.knit(&registry).expect("knit must succeed");
        assert_eq!(machine.strand_count(), 1);
        assert_eq!(machine.strands()[0].edges().len(), 1);

        // Same registry → `to_machine_notation` produces the
        // same canonical form as the explicit knit + serialize.
        let direct = strand
            .to_machine_notation(&registry)
            .expect("must serialize");
        let via_machine = machine.to_machine_notation().unwrap();
        assert_eq!(direct, via_machine);
    }

    // TEST1120: Strand::knit fails hard when the cap is not in
    // the registry — the planner produces strands referencing
    // caps that must be present in the cap registry's cache for
    // resolution to succeed.
    #[test]
    fn test1120_strand_knit_unknown_cap_fails_hard() {
        use crate::cap::registry::FabricRegistry;
        use crate::machine::MachineAbstractionError;

        let registry = FabricRegistry::new_for_test();
        // Note: no caps added to the registry.

        let cap_urn =
            CapUrn::from_string("cap:ghost;in=\"media:ext=pdf\";out=\"media:enc=utf-8;ext=txt\"")
                .unwrap();
        let strand = Strand {
            steps: vec![StrandStep::new(
                StrandStepType::Cap {
                    cap_urn: cap_urn.clone(),
                    title: "Ghost".to_string(),
                    specificity: 0,
                    input_is_sequence: false,
                    output_is_sequence: false,
                    inputs: vec![CapInput {
                        arg_urn: MediaUrn::from_string("media:ext=pdf").unwrap(),
                        source: ArgSourceRef::StrandInput,
                    }],
                },
                MediaUrn::from_string("media:ext=pdf").unwrap(),
                MediaUrn::from_string("media:enc=utf-8;ext=txt").unwrap(),
            )],
            source_media_urn: MediaUrn::from_string("media:ext=pdf").unwrap(),
            target_media_urn: MediaUrn::from_string("media:enc=utf-8;ext=txt").unwrap(),
            total_steps: 1,
            cap_step_count: 1,
            description: "ghost strand".to_string(),
        };

        let err = strand.knit(&registry).unwrap_err();
        assert!(matches!(err, MachineAbstractionError::UnknownCap { .. }));
    }

    // =========================================================================
    // Round-trip path tests (source == target)
    // =========================================================================

    // TEST1289: BFS reachable targets includes the source itself when round-trip paths exist.
    // A→B and B→A means A is reachable from A (via A→B→A).
    #[test]
    fn test1289_bfs_reachable_includes_source_roundtrip() {
        let mut graph = LiveCapFab::new();

        // text → integer (coerce)
        let cap1 = make_test_cap(
            "media:enc=utf-8",
            "media:integer;numeric",
            "coerce_to_int",
            "Coerce to Integer",
        );
        graph.add_cap(&cap1);
        // integer → text (coerce back)
        let cap2 = make_test_cap(
            "media:integer;numeric",
            "media:enc=utf-8",
            "coerce_to_text",
            "Coerce to Text",
        );
        graph.add_cap(&cap2);
        graph.set_bookends(&all_bookends(&[cap1, cap2]));

        let source = MediaUrn::from_string("media:enc=utf-8").unwrap();
        let targets = graph.get_reachable_targets(&source, false, 5);

        // Source should be reachable (via text→integer→text)
        let has_self = targets
            .iter()
            .any(|t| t.media_def.is_equivalent(&source).unwrap_or(false));
        assert!(
            has_self,
            "BFS must find source as reachable target in round-trip graph. Found: {:?}",
            targets
                .iter()
                .map(|t| t.media_def.to_string())
                .collect::<Vec<_>>()
        );
    }

    // TEST1290: IDDFS find_paths_to_exact_target finds round-trip paths when source == target.
    // This was a bug where the visited set blocked returning to the source, and
    // early return on target hit at wrong depth prevented exploration.
    #[test]
    fn test1290_iddfs_finds_roundtrip_paths() {
        let mut graph = LiveCapFab::new();

        // text → integer
        graph.add_cap(&make_test_cap(
            "media:enc=utf-8",
            "media:integer;numeric",
            "coerce_to_int",
            "Coerce to Integer",
        ));
        // integer → text
        graph.add_cap(&make_test_cap(
            "media:integer;numeric",
            "media:enc=utf-8",
            "coerce_to_text",
            "Coerce to Text",
        ));

        let source = MediaUrn::from_string("media:enc=utf-8").unwrap();
        let target = MediaUrn::from_string("media:enc=utf-8").unwrap();

        let paths = graph.find_paths_to_exact_target(&source, &target, false, 5, 100);

        assert!(
            !paths.is_empty(),
            "IDDFS must find round-trip paths (textable→integer→textable). Got 0 paths."
        );

        // The shortest round-trip should be 2 steps
        let shortest = paths.iter().min_by_key(|p| p.total_steps).unwrap();
        assert_eq!(
            shortest.total_steps, 2,
            "Shortest round-trip should be 2 steps (coerce + coerce back)"
        );
    }

    // TEST1291: IDDFS round-trip paths are also found with is_sequence=true.
    // The ForEach/Collect edges must not block round-trip discovery.
    #[test]
    fn test1291_iddfs_roundtrip_with_sequence() {
        let mut graph = LiveCapFab::new();

        // text → integer
        graph.add_cap(&make_test_cap(
            "media:enc=utf-8",
            "media:integer;numeric",
            "coerce_to_int",
            "Coerce to Integer",
        ));
        // integer → text
        graph.add_cap(&make_test_cap(
            "media:integer;numeric",
            "media:enc=utf-8",
            "coerce_to_text",
            "Coerce to Text",
        ));

        let source = MediaUrn::from_string("media:enc=utf-8").unwrap();
        let target = MediaUrn::from_string("media:enc=utf-8").unwrap();

        // With is_sequence=true, the path goes through ForEach first
        let paths = graph.find_paths_to_exact_target(&source, &target, true, 5, 100);

        assert!(
            !paths.is_empty(),
            "IDDFS must find round-trip paths even with is_sequence=true. Got 0 paths."
        );
    }

    // TEST1292: BFS and IDDFS agree that round-trip targets exist.
    // If BFS says target X is reachable from source X, IDDFS must find at least one path.
    #[test]
    fn test1292_bfs_iddfs_roundtrip_consistency() {
        let mut graph = LiveCapFab::new();

        // Build a small graph: A→B, B→C, C→A. Use concrete media
        // URNs that the test then declares as bookends so they are
        // valid transmute sources/targets — `get_reachable_targets`
        // filters its results down to bookend nodes only (the
        // production wiring populates this via the live media
        // registry; tests do it explicitly via `set_bookends`).
        graph.add_cap(&make_test_cap("media:a", "media:b", "a_to_b", "A to B"));
        graph.add_cap(&make_test_cap("media:b", "media:c", "b_to_c", "B to C"));
        graph.add_cap(&make_test_cap("media:c", "media:a", "c_to_a", "C to A"));

        let bookends: HashSet<MediaUrn> = ["media:a", "media:b", "media:c"]
            .iter()
            .map(|s| MediaUrn::from_string(s).unwrap())
            .collect();
        graph.set_bookends(&bookends);

        let source = MediaUrn::from_string("media:a").unwrap();

        // BFS should find source as reachable (via A→B→C→A)
        let bfs_targets = graph.get_reachable_targets(&source, false, 5);
        let bfs_has_self = bfs_targets
            .iter()
            .any(|t| t.media_def.is_equivalent(&source).unwrap_or(false));
        assert!(
            bfs_has_self,
            "BFS must find A reachable from A in cyclic graph"
        );

        // IDDFS must also find paths
        let target = MediaUrn::from_string("media:a").unwrap();
        let iddfs_paths = graph.find_paths_to_exact_target(&source, &target, false, 5, 100);
        assert!(
            !iddfs_paths.is_empty(),
            "IDDFS must find round-trip paths when BFS says target is reachable. BFS found {} targets including self, IDDFS found 0 paths.",
            bfs_targets.len()
        );

        // Shortest path should be 3 steps (A→B→C→A)
        let shortest = iddfs_paths.iter().min_by_key(|p| p.total_steps).unwrap();
        assert_eq!(shortest.total_steps, 3);
    }

    // TEST1293: IDDFS round-trip does not produce paths with 0 cap steps.
    // Identity-only round trips (no real transformation) must be excluded.
    #[test]
    fn test1293_roundtrip_requires_cap_steps() {
        let mut graph = LiveCapFab::new();

        // Only one direction — no round trip possible
        graph.add_cap(&make_test_cap("media:a", "media:b", "a_to_b", "A to B"));

        let source = MediaUrn::from_string("media:a").unwrap();
        let target = MediaUrn::from_string("media:a").unwrap();

        let paths = graph.find_paths_to_exact_target(&source, &target, false, 5, 100);
        assert!(
            paths.is_empty(),
            "No round-trip should exist when there's no return edge. Got {} paths.",
            paths.len()
        );
    }

    // TEST0121: Step title query filters paths server side
    #[test]
    fn test0121_step_title_query_filters_paths_server_side() {
        let mut graph = LiveCapFab::new();

        graph.add_cap(&make_test_cap("media:a", "media:b", "a_to_b", "Resize"));
        graph.add_cap(&make_test_cap("media:b", "media:c", "b_to_c", "Export"));
        graph.add_cap(&make_test_cap("media:a", "media:d", "a_to_d", "Decimate"));
        graph.add_cap(&make_test_cap("media:d", "media:c", "d_to_c", "Export"));

        let source = MediaUrn::from_string("media:a").unwrap();
        let target = MediaUrn::from_string("media:c").unwrap();

        let paths = graph.find_paths_to_exact_target_with_step_title_query(
            &source,
            &target,
            false,
            5,
            10,
            Some("decimate"),
        );

        assert_eq!(
            paths.len(),
            1,
            "Only the path containing a step title matching 'decimate' should be returned. Got {} paths.",
            paths.len()
        );
        assert!(
            paths[0]
                .steps
                .iter()
                .any(|step| step.title().contains("Decimate")),
            "Filtered result must include the matching step title. Got: {:?}",
            paths[0]
                .steps
                .iter()
                .map(|step| step.title())
                .collect::<Vec<_>>()
        );
    }

    // TEST0122: Step title query constrains streaming progress counts
    #[test]
    fn test0122_step_title_query_constrains_streaming_progress_counts() {
        let mut graph = LiveCapFab::new();

        graph.add_cap(&make_test_cap("media:a", "media:b", "a_to_b", "Resize"));
        graph.add_cap(&make_test_cap("media:b", "media:c", "b_to_c", "Export"));
        graph.add_cap(&make_test_cap("media:a", "media:d", "a_to_d", "Decimate"));
        graph.add_cap(&make_test_cap("media:d", "media:c", "d_to_c", "Export"));

        let source = MediaUrn::from_string("media:a").unwrap();
        let target = MediaUrn::from_string("media:c").unwrap();
        let cancelled = std::sync::atomic::AtomicBool::new(false);
        let mut max_paths_found = 0usize;
        let mut emitted_paths = Vec::new();

        let paths = graph.find_paths_streaming_with_step_title_query(
            &source,
            &target,
            false,
            5,
            10,
            Some("decimate"),
            &cancelled,
            |event| match event {
                PathFindingEvent::DepthComplete { paths_found, .. } => {
                    max_paths_found = max_paths_found.max(paths_found);
                }
                PathFindingEvent::PathFound(path) => emitted_paths.push(path),
                PathFindingEvent::Complete { .. } => {}
            },
        );

        assert_eq!(
            max_paths_found, 1,
            "Streaming progress must count only paths matching the query. Got {}.",
            max_paths_found
        );
        assert_eq!(
            emitted_paths.len(),
            1,
            "Streaming path events must emit only matching paths. Got {}.",
            emitted_paths.len()
        );
        assert_eq!(
            paths.len(),
            1,
            "Final streaming result must contain only matching paths. Got {}.",
            paths.len()
        );
    }

    // TEST8062: An abstract cap is a dispatch umbrella — never backed by a
    // cartridge and never a runnable graph edge. `LiveCapFab::add_cap` must skip
    // it, or the wizard/planner could offer a path that fails at execution. This
    // fails hard if abstract caps ever leak into the graph.
    #[test]
    fn test8062_abstract_cap_excluded_from_graph() {
        let mut graph = LiveCapFab::new();
        let before = graph.stats();

        let mut abstract_cap = make_test_cap(
            "media:",
            "media:enc=utf-8;ext=txt;page;plain-text",
            "disbind",
            "Disbind (abstract)",
        );
        abstract_cap.set_abstract(true);
        graph.add_cap(&abstract_cap);
        assert_eq!(
            graph.stats(),
            before,
            "an abstract cap must not add any node or edge to the runnable graph"
        );

        // A concrete cap in the same family DOES become a runnable edge.
        let concrete = make_test_cap(
            "media:ext=pdf",
            "media:enc=utf-8;ext=txt;page;plain-text",
            "disbind",
            "Disbind PDF",
        );
        graph.add_cap(&concrete);
        let (n_after, e_after) = graph.stats();
        assert!(
            e_after > before.1,
            "a concrete cap must add a runnable edge"
        );
        assert!(n_after > before.0, "a concrete cap must add graph nodes");
    }
}
