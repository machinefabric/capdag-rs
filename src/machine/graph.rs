//! Machine graph — anchor-realized representation of one or more strands.
//!
//! A `Machine` is the canonical, anchor-realized form of a set
//! of capability strands. It is the layer between a planner
//! `Strand` (linear cap-step sequence, no anchor commitment)
//! and a `MachineRun` (concrete execution against actual
//! input data).
//!
//! ## Layers
//!
//! | Layer | Type | Anchor commitment? | Cap-arg assignment? | Inputs from? |
//! |-------|------|--------------------|---------------------|--------------|
//! | Planner | `Strand` | no | no | media URN patterns |
//! | Anchored | `Machine` (this module) | yes | yes (resolved) | anchor URNs |
//! | Concrete | `MachineRun` | yes | yes | actual input files |
//!
//! ## Structure
//!
//! ```text
//! Machine
//!  └── strands: Vec<MachineStrand>     // ordered, declaration-order matters
//!       ├── nodes: Vec<MediaUrn>        // data positions in this strand
//!       ├── edges: Vec<MachineEdge>     // canonical-order resolved cap steps
//!       │    └── assignment: Vec<EdgeAssignmentBinding>
//!       │         └── (cap_arg_media_urn, source: NodeId)
//!       ├── input_anchor_ids: Vec<NodeId>   // root nodes (no producer)
//!       └── output_anchor_ids: Vec<NodeId>  // leaf nodes (no consumer)
//! ```
//!
//! Each `MachineStrand` is a maximal connected component of
//! the machine's wiring graph: two edges share a strand iff
//! they share at least one node identity. Crossings (shared
//! data positions used by multiple edges) are internal to a
//! single strand. Strands within a `Machine` are disjoint at
//! the node-identity level.
//!
//! ## Equivalence
//!
//! `Machine::is_equivalent` is **strict, positional**:
//!
//! - Same number of `MachineStrand`s in the `strands` vec
//! - For every i, `self.strands[i].is_equivalent(&other.strands[i])`
//!
//! Strand declaration order matters because the executor walks
//! the strands in that order at runtime. Two `Machine`s with the
//! same strands in different order are not equivalent.
//!
//! `MachineStrand::is_equivalent` walks both strands in their
//! canonical edge order, comparing edges position-by-position.
//! Each edge's `assignment` vec is pre-sorted by cap arg media
//! URN, so the comparison is over a canonical form. The walk
//! also builds a bijection between `NodeId`s in self and other;
//! any inconsistency fails the comparison.
//!
//! Anchors (`input_anchors` and `output_anchors`) are sorted
//! multisets of `MediaUrn` (sorted by structural `MediaUrn`
//! `Ord`); they are compared positionally on the sorted form,
//! which is multiset equality.
//!
//! This is the only equivalence relation on `Machine`. There is
//! no looser variant. If a "drop-in replaceable but
//! order-flexible" relation is ever needed it will get its own
//! descriptive name and will not be called `is_equivalent`.

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cap::registry::FabricRegistry;
use crate::planner::{StepToken, Strand};
use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::MediaUrn;

use super::error::MachineAbstractionError;
use super::resolve;

/// A node identity within a single `MachineStrand`.
///
/// `NodeId` is an index into the strand's `nodes` vec. It is
/// dense, starts at 0, and is stable for the lifetime of the
/// `MachineStrand`. Node IDs are scoped to a single strand —
/// two strands in the same `Machine` use disjoint `NodeId`
/// spaces (and the serializer assigns them disjoint global node
/// names).
pub type NodeId = u32;

/// One slot in a resolved `MachineEdge`'s source-to-cap-arg
/// assignment.
///
/// Each binding records WHICH cap argument (`cap_arg_media_urn`)
/// is fed by WHICH data-position in the strand (`source`). The
/// `cap_arg_media_urn` is the cap argument's identity per the
/// cap definition (RULE1 in `10-VALIDATION-RULES` requires
/// uniqueness of `media_urn` across a cap's args, so this field
/// is sufficient to identify the slot).
///
/// `MachineEdge::assignment` holds these bindings sorted by
/// `cap_arg_media_urn` via `MediaUrn`'s structural `Ord`,
/// producing a canonical form for equivalence comparison.
#[derive(Debug, Clone)]
pub struct EdgeAssignmentBinding {
    pub cap_arg_media_urn: MediaUrn,
    /// The arg's name in the cap definition: the label notation writes the
    /// binding under. It is carried so a machine can be written back without
    /// the definitions, and it is NOT identity — `cap_arg_media_urn` is, and
    /// equivalence never reads this.
    pub arg_name: String,
    pub source: BindingSource,
}

/// What feeds one cap argument of a step.
#[derive(Debug, Clone, PartialEq)]
pub enum BindingSource {
    /// A data position of the strand.
    Node(NodeId),
    /// A constant written in the machine: any JSON value. It is part of the
    /// binding, not a node — nothing else can consume it.
    Constant(serde_json::Value),
}

impl BindingSource {
    /// The node, when the source is one.
    pub fn node(&self) -> Option<NodeId> {
        match self {
            BindingSource::Node(id) => Some(*id),
            BindingSource::Constant(_) => None,
        }
    }
}

impl EdgeAssignmentBinding {
    /// A binding of `source` (a node of the strand) to the arg
    /// `cap_arg_media_urn` identifies and the definition names `arg_name`.
    pub fn from_node(cap_arg_media_urn: MediaUrn, arg_name: impl Into<String>, source: NodeId) -> Self {
        Self {
            cap_arg_media_urn,
            arg_name: arg_name.into(),
            source: BindingSource::Node(source),
        }
    }

    /// The node this binding takes its value from, when it takes it from one.
    pub fn node(&self) -> Option<NodeId> {
        self.source.node()
    }

    fn is_equivalent_with(
        &self,
        other: &EdgeAssignmentBinding,
        node_map: &mut NodeBijection,
        self_strand: &MachineStrand,
        other_strand: &MachineStrand,
    ) -> bool {
        if !self
            .cap_arg_media_urn
            .is_equivalent(&other.cap_arg_media_urn)
            .unwrap_or(false)
        {
            return false;
        }
        match (&self.source, &other.source) {
            (BindingSource::Node(a), BindingSource::Node(b)) => {
                node_map.bind(*a, *b, self_strand, other_strand)
            }
            // Two machines that give an arg different constants behave differently.
            (BindingSource::Constant(a), BindingSource::Constant(b)) => a == b,
            _ => false,
        }
    }
}

/// One output of a step that its wiring takes, and the node it goes to.
///
/// `output_media_urn` is the output's identity per the cap definition
/// (RULE20: unique among a cap's outputs). `MachineEdge::outputs` holds these
/// sorted by `output_media_urn`, a canonical form like the assignment's.
#[derive(Debug, Clone)]
pub struct EdgeOutput {
    pub output_media_urn: MediaUrn,
    /// The output's name in the cap definition: a label, carried like
    /// `EdgeAssignmentBinding::arg_name` and never compared.
    pub output_name: String,
    pub node: NodeId,
}

/// One resolved cap-step inside a `MachineStrand`.
///
/// Each edge represents one application of a capability. The
/// `assignment` field carries the explicit source-to-cap-arg
/// mapping computed by the resolver: pairs of (cap arg media
/// URN, the strand node ID that feeds it). The pairs are sorted
/// by `cap_arg_media_urn` so two semantically-equivalent edges
/// produce identical assignment vecs.
#[derive(Debug, Clone)]
pub struct MachineEdge {
    pub cap_urn: CapUrn,
    pub assignment: Vec<EdgeAssignmentBinding>,
    /// The outputs the wiring takes, sorted by output media URN. An output the
    /// cap declares and no entry names is discarded; an empty list is a sink.
    pub outputs: Vec<EdgeOutput>,
    /// What the step does when it fails. Part of what the machine is: two
    /// machines that differ in it behave differently.
    pub policy: super::syntax::StepPolicy,
    /// The machine steps this step was included through, outermost first,
    /// each as `alias#use`: the alias its header gave the included machine
    /// and which use of it this is, counted from one in the order written.
    /// Empty for a step the machine wrote itself. It says where a step of
    /// the inlined whole came from — a label, like `arg_name`: equivalence
    /// never reads it, and canonical notation does not write it.
    pub included_as: Vec<String>,
    pub is_loop: bool,
    /// Stable identity of the cardinality boundary that maps this edge over a
    /// sequence. Present exactly when `is_loop` is true. This is distinct from
    /// `token_id`: a ForEach boundary and its body-entry cap are separate graph
    /// elements and must never share an attribution identity.
    pub foreach_token_id: Option<StepToken>,
    /// Stable identity of the originating resolved-strand step (see
    /// `StrandStep::token_id`), carried onto the executable DAG unit so a running
    /// cap can report *which* step it is — the key the run's live updates route
    /// by. It is IDENTITY, not semantics: `is_equivalent_with` deliberately
    /// ignores it (two structurally-identical machines stay equivalent even with
    /// different per-run token_ids).
    pub token_id: StepToken,
}

impl MachineEdge {
    /// The node the step's MAIN output goes to: the taken output whose URN is
    /// the cap URN's `out=`. `None` when the wiring does not take it.
    pub fn main_target(&self) -> Option<NodeId> {
        let out = self.cap_urn.out_media_urn().ok()?;
        self.outputs
            .iter()
            .find(|o| o.output_media_urn.is_equivalent(&out).unwrap_or(false))
            .map(|o| o.node)
    }

    /// Every node this step produces.
    pub fn output_nodes(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.outputs.iter().map(|o| o.node)
    }

    /// Every node this step consumes. A constant is not a node.
    pub fn source_nodes(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.assignment.iter().filter_map(|b| b.node())
    }

    fn is_equivalent_with(
        &self,
        other: &MachineEdge,
        node_map: &mut NodeBijection,
        self_strand: &MachineStrand,
        other_strand: &MachineStrand,
    ) -> bool {
        if self.is_loop != other.is_loop {
            return false;
        }
        if self.policy != other.policy {
            return false;
        }
        if !self.cap_urn.is_equivalent(&other.cap_urn) {
            return false;
        }
        if self.assignment.len() != other.assignment.len() {
            return false;
        }
        // The assignment vecs are pre-sorted by cap arg media
        // URN at construction time, so positional comparison is
        // canonical.
        for (self_b, other_b) in self.assignment.iter().zip(other.assignment.iter()) {
            if !self_b.is_equivalent_with(other_b, node_map, self_strand, other_strand) {
                return false;
            }
        }
        if self.outputs.len() != other.outputs.len() {
            return false;
        }
        // Sorted by output media URN at construction time, like the assignment.
        for (self_o, other_o) in self.outputs.iter().zip(other.outputs.iter()) {
            if !self_o
                .output_media_urn
                .is_equivalent(&other_o.output_media_urn)
                .unwrap_or(false)
            {
                return false;
            }
            if !node_map.bind(self_o.node, other_o.node, self_strand, other_strand) {
                return false;
            }
        }
        true
    }
}

impl fmt::Display for MachineEdge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let assignments: Vec<String> = self
            .assignment
            .iter()
            .map(|b| match &b.source {
                BindingSource::Node(id) => format!("{}<-#{}", b.cap_arg_media_urn, id),
                BindingSource::Constant(value) => format!("{}<-{}", b.cap_arg_media_urn, value),
            })
            .collect();
        let outputs: Vec<String> = self
            .outputs
            .iter()
            .map(|o| format!("{}->#{}", o.output_media_urn, o.node))
            .collect();
        // Debug-only marker for a per-item map edge. Not notation syntax (the `LOOP`
        // keyword is retired); `is_loop` is a derived cardinality property.
        let loop_prefix = if self.is_loop { "map " } else { "" };
        write!(
            f,
            "{}{}{} ({}) -> ({})",
            loop_prefix,
            self.cap_urn,
            self.policy.to_notation(),
            assignments.join(", "),
            outputs.join(", ")
        )
    }
}

/// One connected component of resolved cap edges with explicit
/// anchor commitments.
///
/// A `MachineStrand` is a maximal connected sub-graph: every
/// edge in it shares at least one `NodeId` (transitively) with
/// every other edge. Within a strand, "crossings" are simply
/// shared internal nodes — they do not represent inter-strand
/// joining.
///
/// Built once via `resolve::resolve_strand` (planner path) or
/// `resolve::resolve_wiring_set` (parser path). After
/// construction the strand is immutable.
#[derive(Debug, Clone)]
pub struct MachineStrand {
    /// Distinct data positions in this strand. Indexed by
    /// `NodeId`. Each entry is the `MediaUrn` of that data
    /// position — the canonical type at this point in the
    /// flow.
    nodes: Vec<MediaUrn>,
    /// Resolved cap-step edges in canonical topological order.
    /// Each edge references nodes by `NodeId` from `nodes`.
    edges: Vec<MachineEdge>,
    /// `NodeId`s of root nodes (nodes that no edge in this
    /// strand produces as its target). Sorted by canonical
    /// `MediaUrn` order, ties broken by `NodeId`.
    input_anchor_ids: Vec<NodeId>,
    /// `NodeId`s of leaf nodes (nodes that no edge in this
    /// strand consumes via any assignment binding). Sorted by
    /// canonical `MediaUrn` order, ties broken by `NodeId`.
    output_anchor_ids: Vec<NodeId>,
    /// Whether each node holds a sequence, as its producer's output declares
    /// (a node nothing produces is a single value here: what a run gives it
    /// is the run's to say). Indexed by `NodeId`. A sequence bound to an arg
    /// that takes a single value is what makes a step run once per item.
    node_is_sequence: Vec<bool>,
}

impl MachineStrand {
    /// Construct a `MachineStrand` from already-resolved fields.
    /// Used by `resolve::resolve_wiring_set` after it has built
    /// the canonical node and edge vecs.
    pub(crate) fn from_resolved(
        nodes: Vec<MediaUrn>,
        edges: Vec<MachineEdge>,
        input_anchor_ids: Vec<NodeId>,
        output_anchor_ids: Vec<NodeId>,
        node_is_sequence: Vec<bool>,
    ) -> Self {
        assert_eq!(
            nodes.len(),
            node_is_sequence.len(),
            "every node of a strand has a cardinality"
        );
        Self {
            nodes,
            edges,
            input_anchor_ids,
            output_anchor_ids,
            node_is_sequence,
        }
    }

    /// Whether node `id` holds a sequence (see the field's documentation).
    pub fn node_is_sequence(&self, id: NodeId) -> bool {
        self.node_is_sequence[id as usize]
    }

    /// All distinct data positions in this strand, indexed by `NodeId`.
    pub fn nodes(&self) -> &[MediaUrn] {
        &self.nodes
    }

    /// The cap-step edges of this strand, in canonical
    /// topological order.
    pub fn edges(&self) -> &[MachineEdge] {
        &self.edges
    }

    /// `NodeId`s of the strand's input anchor nodes.
    pub fn input_anchor_ids(&self) -> &[NodeId] {
        &self.input_anchor_ids
    }

    /// `NodeId`s of the strand's output anchor nodes.
    pub fn output_anchor_ids(&self) -> &[NodeId] {
        &self.output_anchor_ids
    }

    /// Look up a node's `MediaUrn` by `NodeId`. Panics if the
    /// id is out of range — that's a structural bug, never a
    /// runtime condition for a well-formed strand.
    pub fn node_urn(&self, id: NodeId) -> &MediaUrn {
        &self.nodes[id as usize]
    }

    /// Sorted multiset of input anchor URNs.
    pub fn input_anchors(&self) -> Vec<MediaUrn> {
        self.input_anchor_ids
            .iter()
            .map(|id| self.nodes[*id as usize].clone())
            .collect()
    }

    /// Sorted multiset of output anchor URNs.
    pub fn output_anchors(&self) -> Vec<MediaUrn> {
        self.output_anchor_ids
            .iter()
            .map(|id| self.nodes[*id as usize].clone())
            .collect()
    }

    /// Strict equivalence with another `MachineStrand`.
    ///
    /// Walks both strands in canonical edge order, building a
    /// bijection between `NodeId`s on the fly. Any edge mismatch (cap URN,
    /// policy, assignment, outputs, or `is_loop`) fails the comparison, and
    /// so does an inconsistent bijection (a node of either strand paired
    /// with two of the other) or two paired nodes that hold different types.
    pub fn is_equivalent(&self, other: &Self) -> bool {
        if self.edges.len() != other.edges.len() {
            return false;
        }

        let mut node_map = NodeBijection::new(self.nodes.len(), other.nodes.len());

        // The edges decide which node is which: every node is a source or an
        // output of some edge, and an edge says what each of its nodes is FOR
        // (which arg it feeds, which output it is). Two inputs of one type are
        // told apart by that and by nothing else — not by the order they were
        // first written in, which is not part of what a machine is.
        for (self_edge, other_edge) in self.edges.iter().zip(other.edges.iter()) {
            if !self_edge.is_equivalent_with(other_edge, &mut node_map, self, other) {
                return false;
            }
        }

        // Every node of a strand is a source or an output of one of its
        // edges, so the walk has paired every node of each strand with one of
        // the other: the strands have the same nodes, and the same of them
        // are anchors.
        true
    }
}

impl PartialEq for MachineStrand {
    fn eq(&self, other: &Self) -> bool {
        self.is_equivalent(other)
    }
}

impl Eq for MachineStrand {}

/// Helper that maps `NodeId`s in `self` to `NodeId`s in `other`
/// during a strand-equivalence walk. Each `bind` call either
/// records a new mapping or confirms an existing one; if the
/// same self-id is mapped to two different other-ids (or vice
/// versa), `bind` returns `false` and the strands are not
/// equivalent.
struct NodeBijection {
    self_to_other: Vec<Option<NodeId>>,
    other_to_self: Vec<Option<NodeId>>,
}

impl NodeBijection {
    fn new(self_len: usize, other_len: usize) -> Self {
        Self {
            self_to_other: vec![None; self_len],
            other_to_self: vec![None; other_len],
        }
    }

    fn bind(
        &mut self,
        self_id: NodeId,
        other_id: NodeId,
        self_strand: &MachineStrand,
        other_strand: &MachineStrand,
    ) -> bool {
        let self_idx = self_id as usize;
        let other_idx = other_id as usize;

        // The two NodeIds must point to URNs that are
        // structurally equivalent — even before checking the
        // bijection. Otherwise nodes that the bijection maps
        // to each other would carry different types.
        if !self_strand.nodes[self_idx]
            .is_equivalent(&other_strand.nodes[other_idx])
            .unwrap_or(false)
        {
            return false;
        }

        // A node of either strand already paired with another node of the
        // other refuses the pair, and a refused pair leaves nothing behind.
        if self.self_to_other[self_idx].is_some_and(|existing| existing != other_id)
            || self.other_to_self[other_idx].is_some_and(|existing| existing != self_id)
        {
            return false;
        }
        self.self_to_other[self_idx] = Some(other_id);
        self.other_to_self[other_idx] = Some(self_id);
        true
    }
}

/// An ordered collection of resolved `MachineStrand`s.
///
/// Strand declaration order matters: the executor walks the
/// strands in this order at runtime, and `is_equivalent`
/// compares strand-by-strand positionally.
#[derive(Debug, Clone)]
pub struct Machine {
    strands: Vec<MachineStrand>,
}

impl Machine {
    /// Construct a `Machine` from already-resolved strands.
    /// Used by the resolver after it has built and ordered the
    /// strand list.
    pub(crate) fn from_resolved_strands(strands: Vec<MachineStrand>) -> Self {
        Self { strands }
    }

    /// Build a `Machine` containing exactly one `MachineStrand`
    /// from a planner-produced `Strand`.
    ///
    /// Each `Cap` step in the planner strand becomes one resolved
    /// `MachineEdge`; `ForEach`/`Collect` steps are elided — `is_loop` is
    /// derived from cardinality in the resolver, not from those steps. The cap
    /// registry is consulted
    /// to look up each cap's `args` list, which the resolver
    /// uses to compute the source-to-arg assignment via
    /// minimum-cost bipartite matching.
    pub fn from_strand(
        strand: &Strand,
        registry: &FabricRegistry,
    ) -> Result<Self, MachineAbstractionError> {
        let resolved = resolve::resolve_strand(strand, registry, 0)?;
        Ok(Self::from_resolved_strands(vec![resolved]))
    }

    /// Build a `Machine` containing N `MachineStrand`s, one per
    /// input strand, in the given order.
    ///
    /// Each strand is resolved independently. **No cross-strand
    /// joining is attempted** — even if two strands have
    /// type-compatible URNs internally, this constructor produces
    /// two disjoint `MachineStrand`s. Crossings only arise from
    /// notation, where the user explicitly shares node names
    /// across wirings.
    pub fn from_strands(
        strands: &[Strand],
        registry: &FabricRegistry,
    ) -> Result<Self, MachineAbstractionError> {
        if strands.is_empty() {
            return Err(MachineAbstractionError::NoCapabilitySteps);
        }
        let mut resolved_strands = Vec::with_capacity(strands.len());
        for (idx, strand) in strands.iter().enumerate() {
            resolved_strands.push(resolve::resolve_strand(strand, registry, idx)?);
        }
        Ok(Self::from_resolved_strands(resolved_strands))
    }

    /// All resolved strands in this machine, in declaration order.
    pub fn strands(&self) -> &[MachineStrand] {
        &self.strands
    }

    /// Number of strands in this machine.
    pub fn strand_count(&self) -> usize {
        self.strands.len()
    }

    /// Whether this machine has no strands at all.
    pub fn is_empty(&self) -> bool {
        self.strands.is_empty()
    }

    /// Strict, positional equivalence with another `Machine`.
    ///
    /// Two `Machine`s are equivalent iff they have the same
    /// number of strands and `self.strands[i].is_equivalent(
    /// &other.strands[i])` for every i. Strand order matters.
    pub fn is_equivalent(&self, other: &Self) -> bool {
        if self.strands.len() != other.strands.len() {
            return false;
        }
        for (self_strand, other_strand) in self.strands.iter().zip(other.strands.iter()) {
            if !self_strand.is_equivalent(other_strand) {
                return false;
            }
        }
        true
    }
}

impl PartialEq for Machine {
    fn eq(&self, other: &Self) -> bool {
        self.is_equivalent(other)
    }
}

impl Eq for Machine {}

impl fmt::Display for Machine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.strands.is_empty() {
            return write!(f, "Machine(empty)");
        }
        let edge_count: usize = self.strands.iter().map(|s| s.edges.len()).sum();
        write!(
            f,
            "Machine({} strands, {} edges)",
            self.strands.len(),
            edge_count
        )
    }
}

/// A single execution attempt of a [`Machine`].
#[derive(Debug, Clone)]
pub struct MachineRun {
    pub id: String,
    pub machine_notation: String,
    pub resolved_strand: Strand,
    pub status: MachineRunStatus,
    pub error_message: Option<String>,
    pub created_at_unix: i64,
    pub started_at_unix: Option<i64>,
    pub completed_at_unix: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MachineRunStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after UNIX_EPOCH")
        .as_secs() as i64
}

impl MachineRun {
    /// Construct a new `MachineRun` bound to a machine and its
    /// resolved strand. The machine's canonical notation is
    /// computed and stored as the run's stable identifier.
    ///
    /// Fails hard with `MachineAbstractionError` if the machine
    /// has no strands or its data-flow cannot be serialized.
    pub fn new(
        id: String,
        machine: &Machine,
        resolved_strand: Strand,
    ) -> Result<Self, MachineAbstractionError> {
        let machine_notation = machine.to_machine_notation()?;
        if machine_notation.is_empty() {
            return Err(MachineAbstractionError::NoCapabilitySteps);
        }
        Ok(Self {
            id,
            machine_notation,
            resolved_strand,
            status: MachineRunStatus::Pending,
            error_message: None,
            created_at_unix: unix_now(),
            started_at_unix: None,
            completed_at_unix: None,
        })
    }

    pub fn start(&mut self) {
        self.status = MachineRunStatus::Running;
        self.started_at_unix = Some(unix_now());
    }

    pub fn complete(&mut self) {
        self.status = MachineRunStatus::Completed;
        self.completed_at_unix = Some(unix_now());
        self.error_message = None;
    }

    pub fn fail(&mut self, error_message: String) {
        self.status = MachineRunStatus::Failed;
        self.completed_at_unix = Some(unix_now());
        self.error_message = Some(error_message);
    }

    pub fn cancel(&mut self) {
        self.status = MachineRunStatus::Cancelled;
        self.completed_at_unix = Some(unix_now());
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::{Machine, MachineAbstractionError, MachineRun, MachineRunStatus, MachineStrand, NodeBijection};
    use crate::machine::test_fixtures::{build_cap, cap_step, media, registry_with, strand_from_steps};

    fn extract_cap_def() -> crate::cap::definition::Cap {
        build_cap(
            "cap:extract;in=\"media:document;ext=pdf\";out=\"media:enc=utf-8;ext=txt\"",
            "extract",
            &["media:document;ext=pdf"],
            "media:enc=utf-8;ext=txt",
        )
    }

    fn embed_cap_def() -> crate::cap::definition::Cap {
        build_cap(
            "cap:embed;in=\"media:enc=utf-8\";out=\"media:vec;record\"",
            "embed",
            &["media:enc=utf-8"],
            "media:vec;record",
        )
    }

    fn pdf_to_txt_strand() -> crate::planner::Strand {
        strand_from_steps(
            vec![cap_step(
                "cap:extract;in=\"media:document;ext=pdf\";out=\"media:enc=utf-8;ext=txt\"",
                "extract",
                "media:document;ext=pdf",
                "media:enc=utf-8;ext=txt",
            )],
            "pdf to txt",
        )
    }

    fn txt_to_vec_strand() -> crate::planner::Strand {
        strand_from_steps(
            vec![cap_step(
                "cap:embed;in=\"media:enc=utf-8\";out=\"media:vec;record\"",
                "embed",
                "media:enc=utf-8;ext=txt",
                "media:vec;record",
            )],
            "txt to vec",
        )
    }

    // TEST1135: MachineStrand::node_urn(id) returns the MediaUrn at that NodeId. For a single-cap
    // strand (pdf → extract → txt), there are exactly two nodes and each returns a valid URN.
    #[test]
    fn test1135_strand_node_urn_returns_media_urn_at_node_id() {
        let registry = registry_with(vec![extract_cap_def()]);
        let m = Machine::from_strand(&pdf_to_txt_strand(), &registry).unwrap();
        let strand = &m.strands()[0];

        for (id, _node) in strand.nodes().iter().enumerate() {
            let urn = strand.node_urn(id as u32);
            assert!(
                !urn.to_string().is_empty(),
                "node_urn({id}) must return a non-empty URN"
            );
        }
    }

    // TEST1155: Building a machine from one strand produces one strand with one resolved edge.
    #[test]
    fn test1155_from_strand_produces_single_strand_machine() {
        let registry = registry_with(vec![extract_cap_def()]);
        let m = Machine::from_strand(&pdf_to_txt_strand(), &registry).unwrap();
        assert_eq!(m.strand_count(), 1);
        assert_eq!(m.strands()[0].edges().len(), 1);
    }

    // TEST1156: Building from multiple strands keeps them disjoint and preserves input strand order.
    #[test]
    fn test1156_from_strands_keeps_strands_disjoint() {
        // Two strands, each with one cap. Even though both
        // touch `media:enc=utf-8;ext=txt` (one as input, one as
        // output), `from_strands` does NOT join them — that's
        // the contract: programmatic construction never
        // creates crossings.
        let registry = registry_with(vec![extract_cap_def(), embed_cap_def()]);
        let m =
            Machine::from_strands(&[pdf_to_txt_strand(), txt_to_vec_strand()], &registry).unwrap();
        assert_eq!(
            m.strand_count(),
            2,
            "from_strands must keep input strands as disjoint MachineStrands"
        );
        // Each strand should have its own one-edge DAG.
        assert_eq!(m.strands()[0].edges().len(), 1);
        assert_eq!(m.strands()[1].edges().len(), 1);
        // Strand order matches input order.
        assert!(m.strands()[0].edges()[0]
            .cap_urn
            .to_string()
            .contains("extract"));
        assert!(m.strands()[1].edges()[0]
            .cap_urn
            .to_string()
            .contains("embed"));
    }

    // TEST1157: Building from zero strands fails with NoCapabilitySteps.
    #[test]
    fn test1157_from_strands_empty_input_fails_hard() {
        let registry = registry_with(vec![]);
        let err = Machine::from_strands(&[], &registry).unwrap_err();
        assert!(matches!(err, MachineAbstractionError::NoCapabilitySteps));
    }

    // TEST1158: Machine equivalence is strict about strand order and rejects reordered strands.
    #[test]
    fn test1158_machine_is_equivalent_is_strict_positional() {
        // Same two strands in two different orders are NOT
        // strictly equivalent — strand declaration order is
        // part of the machine's identity.
        let registry = registry_with(vec![extract_cap_def(), embed_cap_def()]);
        let forward =
            Machine::from_strands(&[pdf_to_txt_strand(), txt_to_vec_strand()], &registry).unwrap();
        let reversed =
            Machine::from_strands(&[txt_to_vec_strand(), pdf_to_txt_strand()], &registry).unwrap();
        assert!(
            !forward.is_equivalent(&reversed),
            "swapping strand order must break strict equivalence"
        );
        // But each machine is equivalent to itself.
        assert!(forward.is_equivalent(&forward));
        assert!(reversed.is_equivalent(&reversed));
    }

    // TEST1159: MachineStrand equivalence accepts two separately built but structurally identical strands.
    #[test]
    fn test1159_machine_strand_is_equivalent_walks_node_bijection() {
        // Two `MachineStrand`s built from the same strand twice
        // are equivalent. The NodeBijection is built on the
        // fly during the walk and confirms that every NodeId
        // pair carries equivalent URNs.
        let registry = registry_with(vec![extract_cap_def()]);
        let m1 = Machine::from_strand(&pdf_to_txt_strand(), &registry).unwrap();
        let m2 = Machine::from_strand(&pdf_to_txt_strand(), &registry).unwrap();
        assert!(m1.strands()[0].is_equivalent(&m2.strands()[0]));
    }

    // TEST1160: Creating a MachineRun stores the canonical notation and starts in the pending state.
    #[test]
    fn test1160_machine_run_new_stores_canonical_notation() {
        let registry = registry_with(vec![extract_cap_def()]);
        let strand = pdf_to_txt_strand();
        let machine = Machine::from_strand(&strand, &registry).unwrap();
        let canonical = machine.to_machine_notation().unwrap();
        let run = MachineRun::new("run-id-1".to_string(), &machine, strand.clone()).unwrap();
        assert_eq!(run.id, "run-id-1");
        assert_eq!(run.machine_notation, canonical);
        assert_eq!(run.status, MachineRunStatus::Pending);
    }

    // TEST12834: the correspondence between the nodes of two strands is one
    // to one in both directions: a pair made once is confirmed, and a node of
    // either strand paired with a second node of the other is refused.
    #[test]
    fn test12834_a_node_stands_for_one_node() {
        // Three nodes of one type on each side: what is refused is refused
        // by the correspondence, not by a difference of type.
        let strand = MachineStrand::from_resolved(
            vec![media("media:enc=utf-8"); 3],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![false; 3],
        );
        let mut nodes = NodeBijection::new(3, 3);
        assert!(nodes.bind(0, 2, &strand, &strand));
        assert!(nodes.bind(0, 2, &strand, &strand), "the same pair again");
        assert!(!nodes.bind(0, 1, &strand, &strand), "one node of this strand, two of the other");
        assert!(!nodes.bind(1, 2, &strand, &strand), "two nodes of this strand, one of the other");
        // A refused pair leaves nothing behind: both nodes it named are still free.
        assert!(nodes.bind(1, 1, &strand, &strand));
        assert!(nodes.bind(2, 0, &strand, &strand));
        assert!(nodes.bind(1, 1, &strand, &strand), "and a pair made stays made");

        // Nodes that stand for each other hold one type.
        let other = MachineStrand::from_resolved(
            vec![media("media:enc=utf-8"), media("media:integer")],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![false; 2],
        );
        let mut typed = NodeBijection::new(3, 2);
        assert!(typed.bind(0, 0, &strand, &other));
        assert!(!typed.bind(1, 1, &strand, &other), "a text is not an integer");
    }
}
