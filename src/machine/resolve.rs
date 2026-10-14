//! Anchor-realization for `MachineStrand`s.
//!
//! This module turns either:
//!
//! - a planner-produced `Strand` (linear sequence of cap steps,
//!   one source per step), or
//! - a parser-produced wiring set (potentially multi-source
//!   per wiring),
//!
//! into a fully-resolved `MachineStrand`: a connected sub-graph
//! whose every edge has explicit source-to-cap-arg assignment,
//! anchored by the input root URNs and output leaf URNs of the
//! strand.
//!
//! Resolution requires `&FabricRegistry` access to look up each
//! cap's full argument list (`cap.args`) so the matching
//! algorithm has the per-arg media URN identities to match
//! against.
//!
//! ## Source-to-cap-arg matching
//!
//! For each edge being resolved, the algorithm runs a
//! minimum-cost assignment in two strict-precedence phases:
//!
//! - **Sources**: the URNs feeding this edge (one for the
//!   planner path; one or more for the parser path).
//! - **Cap arguments**: the cap definition's `args` list, each
//!   identified by its `media_urn` (RULE1 in
//!   `10-VALIDATION-RULES` enforces uniqueness across the args
//!   of a single cap).
//! - **Cost** of pairing source `s` with arg `a`:
//!   `spec(s) - spec(a)` if `s.conforms_to(a)` — the specificity distance,
//!   how much more the source says than the arg asks; a conforming source
//!   is never less specific than the arg. If `s` does not conform to `a`,
//!   the pair is impossible.
//! - **Phase 1 — product (§13.4):** each source claims a *distinct*
//!   arg (an injection). Whenever a valid injection exists, this
//!   phase decides — byte-identical to the historical matcher.
//! - **Phase 2 — gather (implicit Collect):** only when NO injection
//!   exists, a **sequence** cap-arg may absorb several conforming
//!   sources (scalar args still take at most one). The N sources
//!   become N bindings on that arg; downstream, plan construction
//!   synthesizes the `Collect` that concatenates the N scalar
//!   producer outputs into the one sequence the arg consumes. This
//!   is cardinality-driven, like the retired `LOOP` — never authored
//!   syntax.
//! - The minimum-cost assignment of the deciding phase must be
//!   **unique**. If two distinct assignments tie, the result is
//!   `AmbiguousMachineNotation` and resolution fails hard.
//!   Source-vec position is NOT used as a tiebreaker (it only fixes
//!   the item order *within* one gathered arg).
//!
//! ## Connected components and the strand boundary
//!
//! The planner only ever produces one strand per call to
//! `resolve_strand`. The parser may produce multiple strands
//! (one per connected component of the wiring graph) and calls
//! `resolve_wiring_set` once per component. Each call yields a
//! single `MachineStrand` whose anchors and edges are derived
//! solely from that component.

use std::collections::HashMap;

use crate::cap::registry::FabricRegistry;
use crate::planner::{ArgSourceRef, StepToken, Strand, StrandStepType};
use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::MediaUrn;

use super::error::{MachineAbstractionError, UndecidedMember};
use super::graph::{
    BindingSource, EdgeAssignmentBinding, EdgeOutput, MachineEdge, MachineStrand, NodeId,
};
use super::iteration::{IterationStructure, MemberShape, StepShape};
use super::syntax::StepPolicy;

/// One wiring after the caller has pre-interned its source and
/// target slots into `NodeId`s against a parallel
/// `nodes: Vec<MediaUrn>` table.
///
/// The resolver consumes this shape via `resolve_pre_interned`
/// and does NOT do any URN-based interning of its own. Two
/// distinct `NodeId`s whose underlying URNs are
/// `is_equivalent` stay distinct — this is what the notation
/// parser needs in order to honor the user's node-name
/// identity contract (two different node names are two
/// different data positions even if they share a URN).
///
/// The planner-strand path uses `resolve_wiring_set`, which
/// translates `ResolvedWiring`s into `PreInternedWiring`s by
/// interning equivalent URNs into the same `NodeId`.
#[derive(Debug, Clone)]
pub struct PreInternedWiring {
    /// The originating resolved-strand step's stable identity, carried onto the
    /// resulting `MachineEdge` (see `MachineEdge::token_id`).
    pub token_id: StepToken,
    /// Origin of the identity for a possible ForEach boundary on this wiring.
    /// Resolved strands carry an exact boundary (or exact absence); notation and
    /// programmatic machine assembly have no shape node yet, so resolution mints
    /// the identity only if fabric cardinality proves that a loop is required.
    pub foreach_identity: ForEachIdentity,
    pub cap_urn: CapUrn,
    /// Members that say which arg they are for, by the arg's NAME in the cap
    /// definition, in the order written. They bind first. A name written more
    /// than once gathers, and only a sequence arg may be gathered into.
    pub named_sources: Vec<(String, BindingSource)>,
    /// Source NodeIds that name no arg, in the order the upstream layer wrote
    /// them. Position carries no semantics — the matching algorithm assigns
    /// each to a cap arg no named member bound, by minimum-cost bipartite
    /// matching, not by index.
    pub source_node_ids: Vec<NodeId>,
    /// The outputs taken: the output's NAME in the cap definition, or `None`
    /// for the main output, and the node it goes to. Empty: a sink.
    pub targets: Vec<(Option<String>, NodeId)>,
    /// What the step does when it fails.
    pub policy: StepPolicy,
    /// The machine steps this step was included through, outermost first;
    /// empty for a step the machine wrote itself (see
    /// `MachineEdge::included_as`).
    pub included_as: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum ForEachIdentity {
    Exact(Option<StepToken>),
    MintIfRequired,
}

/// Resolve a planner-produced `Strand` into a single
/// `MachineStrand`.
///
/// Walks the strand step-by-step and pre-interns `NodeId`s
/// using **positional flow** — each cap step's input position
/// is linked to the preceding cap step's output position iff
/// their URNs are on the same specialization chain
/// (`is_comparable`). Each step's output always allocates a
/// FRESH `NodeId`.
///
/// This is the correct interning policy for planner-produced
/// strands because the planner chains caps by conformance:
/// cap A's `out_spec` may be more specific than cap B's
/// `in_spec`, but at runtime the more-specific data flows
/// through both. The more-specific URN wins as the canonical
/// representative of the shared data position.
///
/// `ForEach` and `Collect` steps are elided here — they preserve the boundary
/// position through the cardinality transition. `is_loop` is not carried on the
/// wiring; it is derived from cardinality in `resolve_pre_interned`
/// (`node_is_sequence[source] && !cap_input_is_sequence`), the single source of
/// truth shared with path search.
pub fn resolve_strand(
    strand: &Strand,
    registry: &FabricRegistry,
    strand_index: usize,
) -> Result<MachineStrand, MachineAbstractionError> {
    // A strand carries meaning only through its cap steps: ForEach and Collect
    // are shape boundaries AROUND caps, never work in themselves. Checked before
    // the walk so a strand made only of boundaries reports the defect that
    // actually explains it ("no capability steps") rather than the consequence
    // the walk would hit first (a ForEach with nothing to map).
    if !strand
        .steps
        .iter()
        .any(|step| matches!(step.step_type, StrandStepType::Cap { .. }))
    {
        return Err(MachineAbstractionError::NoCapabilitySteps);
    }

    let mut nodes: Vec<MediaUrn> = Vec::new();
    let mut pre_interned: Vec<PreInternedWiring> = Vec::new();

    // Producer of each node, by producing step's stable `token_id` → its output
    // `NodeId`. Explicit input sources (`CapInput::source`) resolve against this — no
    // positional predecessor assumption, so fan-out and convergence both wire
    // correctly.
    let mut producer_node: HashMap<StepToken, NodeId> = HashMap::new();
    // The single shared strand input anchor node, allocated on first `StrandInput`
    // reference and refined to the most specific consuming `from_spec`.
    let mut strand_input_node: Option<NodeId> = None;
    let mut pending_foreach_token_id: Option<StepToken> = None;

    for step in &strand.steps {
        match &step.step_type {
            StrandStepType::Cap {
                cap_urn,
                inputs,
                output_urn,
                ..
            } => {
                // Resolve every declared input to its producer's `NodeId`. The primary
                // (stdin) input's runtime media is `from_spec`; it is the only input
                // that may be fed by the strand input anchor (`realize` guarantees a
                // non-primary source is always a produced node).
                let mut source_node_ids: Vec<NodeId> = Vec::with_capacity(inputs.len());
                for input in inputs {
                    let source_id = match &input.source {
                        ArgSourceRef::StrandInput => match strand_input_node {
                            Some(id) => {
                                if step.from_spec.specificity() > nodes[id as usize].specificity() {
                                    nodes[id as usize] = step.from_spec.clone();
                                }
                                id
                            }
                            None => {
                                let id = nodes.len() as NodeId;
                                nodes.push(step.from_spec.clone());
                                strand_input_node = Some(id);
                                id
                            }
                        },
                        ArgSourceRef::Step { token_id } => *producer_node
                            .get(token_id)
                            .ok_or(MachineAbstractionError::DisconnectedStrand { strand_index })?,
                    };
                    source_node_ids.push(source_id);
                }

                // Target: always a fresh position.
                let target_id = nodes.len() as NodeId;
                nodes.push(step.to_spec.clone());

                pre_interned.push(PreInternedWiring {
                    token_id: step.token_id.clone(),
                    foreach_identity: ForEachIdentity::Exact(pending_foreach_token_id.take()),
                    cap_urn: cap_urn.clone(),
                    named_sources: Vec::new(),
                    source_node_ids,
                    // A strand step produces the output it takes: its cap's
                    // main output, or the other output its URN identifies.
                    targets: vec![(strand_output_name(registry, cap_urn, output_urn.as_ref(), strand_index)?, target_id)],
                    policy: StepPolicy::default(),
                    included_as: Vec::new(),
                });

                producer_node.insert(step.token_id.clone(), target_id);
            }
            StrandStepType::ForEach { .. } => {
                if let Some(previous_token_id) =
                    pending_foreach_token_id.replace(step.token_id.clone())
                {
                    return Err(MachineAbstractionError::ConsecutiveForEach {
                        strand_index,
                        previous_token_id,
                        token_id: step.token_id.clone(),
                    });
                }
            }
            StrandStepType::Collect { .. } => {
                // Collect does not qualify the next cap. It closes the preceding
                // mapped region and is represented by the plan's region boundary.
            }
        }
    }

    if let Some(token_id) = pending_foreach_token_id {
        return Err(MachineAbstractionError::DanglingForEach {
            strand_index,
            token_id,
        });
    }

    if pre_interned.is_empty() {
        return Err(MachineAbstractionError::NoCapabilitySteps);
    }

    resolve_pre_interned(nodes, &pre_interned, registry, strand_index)
}

/// The name a strand step's taken output has in its cap's definition: `None`
/// for the main output. A strand records an output by its media URN, its
/// identity; a wiring takes it by its name, a label that is unique per cap.
fn strand_output_name(
    registry: &FabricRegistry,
    cap_urn: &CapUrn,
    output_urn: Option<&MediaUrn>,
    strand_index: usize,
) -> Result<Option<String>, MachineAbstractionError> {
    let Some(output_urn) = output_urn else {
        return Ok(None);
    };
    let cap = registry
        .get_cached_cap(&cap_urn.to_string())
        .ok_or_else(|| MachineAbstractionError::UnknownCap {
            cap_urn: cap_urn.to_string(),
        })?;
    let main = cap.main_output().map(|main| main.media_urn.clone());
    match cap.output_by_urn(&output_urn.to_string()) {
        Some(output) if Some(&output.media_urn) != main.as_ref() => Ok(Some(output.name.clone())),
        _ => Err(MachineAbstractionError::UnknownStrandOutput {
            strand_index,
            cap_urn: cap_urn.to_string(),
            output_urn: output_urn.to_string(),
            declared: cap
                .get_outputs()
                .iter()
                .filter(|output| Some(&output.media_urn) != main.as_ref())
                .map(|output| output.media_urn.clone())
                .collect(),
        }),
    }
}

/// Resolve a planner-path wiring set into a `MachineStrand`.
///
/// The caller supplies `ResolvedWiring`s whose sources and
/// targets are concrete `MediaUrn`s (no NodeIds yet). This
/// function interns equivalent URNs into shared `NodeId`s —
/// the planner-path identity rule — and then delegates to
/// `resolve_pre_interned` for matching, ordering, anchor
/// computation, and cycle detection.
///
/// The notation parser path uses `resolve_pre_interned`
/// directly with `NodeId`s pre-allocated by user node name, so
/// that two distinct names that share a `MediaUrn` stay
/// distinct nodes.

/// Resolve a pre-interned wiring set into a `MachineStrand`.
///
/// The caller has already allocated `NodeId`s for every
/// distinct data position in the strand and built the parallel
/// `nodes: Vec<MediaUrn>` table. The resolver does NOT touch
/// the interning policy — two NodeIds whose URNs happen to be
/// `is_equivalent` stay distinct — and runs:
///
/// 1. Per-wiring source-to-cap-arg matching (Hungarian, with
///    uniqueness check).
/// 2. Cycle detection via Kahn's algorithm over the resulting
///    NodeId-keyed dependency graph.
/// 3. Canonical edge ordering with a structural tiebreaker.
/// 4. Anchor computation (NodeIds with no producer / no
///    consumer in the strand).
pub fn resolve_pre_interned(
    nodes: Vec<MediaUrn>,
    wirings: &[PreInternedWiring],
    registry: &FabricRegistry,
    strand_index: usize,
) -> Result<MachineStrand, MachineAbstractionError> {
    resolve_wirings(nodes, None, wirings, registry, strand_index)
}

/// [`resolve_pre_interned`] for wirings read from notation: `node_names` is
/// the name the text gave each node, indexed by `NodeId`, so that a refusal
/// about a member can say which member.
pub(crate) fn resolve_named_wirings(
    nodes: Vec<MediaUrn>,
    node_names: &[String],
    wirings: &[PreInternedWiring],
    registry: &FabricRegistry,
    strand_index: usize,
) -> Result<MachineStrand, MachineAbstractionError> {
    assert_eq!(nodes.len(), node_names.len(), "every node of a strand read from notation has a name");
    resolve_wirings(nodes, Some(node_names), wirings, registry, strand_index)
}

fn resolve_wirings(
    nodes: Vec<MediaUrn>,
    node_names: Option<&[String]>,
    wirings: &[PreInternedWiring],
    registry: &FabricRegistry,
    strand_index: usize,
) -> Result<MachineStrand, MachineAbstractionError> {
    if wirings.is_empty() {
        return Err(MachineAbstractionError::NoCapabilitySteps);
    }

    // Step 0: the outputs each wiring takes, by the names its targets wrote,
    // and with them the cardinality of every node.
    //
    // A node holds a sequence, IN THE SCOPE IT LIVES IN, iff the output that
    // produces it is declared a sequence: one run of the step yields a
    // sequence. (A node a mapped step produces has one such value per item;
    // what it is from outside the map is `IterationStructure`'s to say.) An
    // input of the machine is one value: notation has no run shape.
    let mut node_is_sequence = vec![false; nodes.len()];
    let mut wiring_outputs: Vec<Vec<EdgeOutput>> = Vec::with_capacity(wirings.len());
    for wiring in wirings {
        let cap = registry
            .get_cached_cap(&wiring.cap_urn.to_string())
            .ok_or_else(|| MachineAbstractionError::UnknownCap {
                cap_urn: wiring.cap_urn.to_string(),
            })?;
        let mut outputs: Vec<EdgeOutput> = Vec::with_capacity(wiring.targets.len());
        for (name, node) in &wiring.targets {
            let output = match name {
                // The unnamed target takes the main output.
                None => cap
                    .main_output()
                    .ok_or_else(|| MachineAbstractionError::NoMainOutput {
                        strand_index,
                        cap_urn: wiring.cap_urn.to_string(),
                    })?,
                Some(name) => cap.output_named(name).ok_or_else(|| {
                    MachineAbstractionError::UnknownOutputName {
                        strand_index,
                        cap_urn: wiring.cap_urn.to_string(),
                        name: name.clone(),
                        declared: cap.get_outputs().iter().map(|o| o.name.clone()).collect(),
                    }
                })?,
            };
            let output_media_urn = MediaUrn::from_string(&output.media_urn)
                .expect("cap registry invariant: every output media_urn is a valid MediaUrn");
            if outputs.iter().any(|taken| {
                taken
                    .output_media_urn
                    .is_equivalent(&output_media_urn)
                    .unwrap_or(false)
            }) {
                return Err(MachineAbstractionError::OutputTakenTwice {
                    strand_index,
                    cap_urn: wiring.cap_urn.to_string(),
                    output: output.name.clone(),
                });
            }
            node_is_sequence[*node as usize] = output.is_sequence;
            outputs.push(EdgeOutput {
                output_media_urn,
                output_name: output.name.clone(),
                node: *node,
            });
        }
        // Canonical order, like the assignment's.
        outputs.sort_by(|a, b| a.output_media_urn.cmp(&b.output_media_urn));
        wiring_outputs.push(outputs);
    }

    // Step 1: per-wiring binding of sources to cap args. Named members bind
    // first; the rest are matched by type over the args no name bound. The
    // result is a sorted assignment of cap-arg → source pairs.
    let mut shapes: Vec<StepShape> = Vec::with_capacity(wirings.len());
    let mut bound: Vec<(&PreInternedWiring, Vec<EdgeAssignmentBinding>, Vec<EdgeOutput>, bool)> =
        Vec::with_capacity(wirings.len());
    for (wiring, outputs) in wirings.iter().zip(wiring_outputs) {
        let cap = registry
            .get_cached_cap(&wiring.cap_urn.to_string())
            .ok_or_else(|| MachineAbstractionError::UnknownCap {
                cap_urn: wiring.cap_urn.to_string(),
            })?;
        let cap_in_spec = MediaUrn::from_string(cap.urn.in_spec()).map_err(|e| {
            MachineAbstractionError::RuntimeMediaInference {
                strand_index,
                cap_urn: wiring.cap_urn.to_string(),
                runtime_input: cap.urn.in_spec().to_string(),
                reason: format!("cap `in=` is not a valid media URN: {e}"),
            }
        })?;
        let void_media = MediaUrn::from_string(crate::urn::media_urn::MEDIA_VOID)
            .expect("MEDIA_VOID is a valid MediaUrn");
        let primary_arg_index = if cap_in_spec.is_equivalent(&void_media).map_err(|e| {
            MachineAbstractionError::RuntimeMediaInference {
                strand_index,
                cap_urn: wiring.cap_urn.to_string(),
                runtime_input: cap.urn.in_spec().to_string(),
                reason: format!("cap `in=` cannot be compared with media:void: {e}"),
            }
        })? {
            None
        } else {
            Some(
                cap.args
                    .iter()
                    .position(|arg| arg.is_main_input(&cap_in_spec))
                    .ok_or_else(|| MachineAbstractionError::CapDoesNotDeclareInput {
                        strand_index,
                        cap_urn: wiring.cap_urn.to_string(),
                    })?,
            )
        };

        // Every arg, by what it ACCEPTS (the type a source must conform to:
        // its stdin source URN for the main input, its `accepts` when it gives
        // one, its own URN otherwise) and by its SLOT (its `media_urn`, the
        // identity a binding records and the URN its stream is delivered
        // under). How an arg is supplied on a command line plays no part: every
        // arg can be bound by a wiring.
        let arg_accepts: Vec<MediaUrn> = cap
            .args
            .iter()
            .map(|arg| {
                MediaUrn::from_string(arg.accepted_urn())
                    .expect("cap registry invariant: every arg accepts a valid MediaUrn")
            })
            .collect();
        let arg_slots: Vec<MediaUrn> = cap
            .args
            .iter()
            .map(|arg| {
                MediaUrn::from_string(&arg.media_urn)
                    .expect("cap registry invariant: every cap arg media_urn is a valid MediaUrn")
            })
            .collect();

        // Named members bind first.
        let mut bindings: Vec<EdgeAssignmentBinding> =
            Vec::with_capacity(wiring.named_sources.len() + wiring.source_node_ids.len());
        let mut named_count = vec![0usize; cap.args.len()];
        for (name, source) in &wiring.named_sources {
            let index = cap.args.iter().position(|arg| arg.name == *name).ok_or_else(|| {
                MachineAbstractionError::UnknownArgName {
                    strand_index,
                    cap_urn: wiring.cap_urn.to_string(),
                    name: name.clone(),
                    declared: cap.args.iter().map(|arg| arg.name.clone()).collect(),
                }
            })?;
            match source {
                BindingSource::Node(id) => {
                    let source_urn = &nodes[*id as usize];
                    if !source_urn.conforms_to(&arg_accepts[index]).unwrap_or(false) {
                        return Err(MachineAbstractionError::SourceDoesNotConformToArg {
                            strand_index,
                            cap_urn: wiring.cap_urn.to_string(),
                            name: name.clone(),
                            accepts: arg_accepts[index].to_string(),
                            source_urn: source_urn.to_string(),
                        });
                    }
                }
                BindingSource::Constant(value) => {
                    check_constant(registry, &wiring.cap_urn, &cap.args[index], value, strand_index)?
                }
            }
            named_count[index] += 1;
            bindings.push(EdgeAssignmentBinding {
                cap_arg_media_urn: arg_slots[index].clone(),
                arg_name: cap.args[index].name.clone(),
                source: source.clone(),
            });
        }
        for (index, count) in named_count.iter().enumerate() {
            if *count > 1 && !cap.args[index].is_sequence {
                return Err(MachineAbstractionError::ArgBoundTwice {
                    strand_index,
                    cap_urn: wiring.cap_urn.to_string(),
                    name: cap.args[index].name.clone(),
                    count: *count,
                });
            }
        }

        // The unnamed members are matched by type over the args no name bound.
        // The Hungarian matcher assigns each source to the arg it conforms to;
        // genuine ambiguity is a hard failure (in `match_sources_to_args`).
        let free: Vec<usize> = (0..cap.args.len()).filter(|i| named_count[*i] == 0).collect();
        let free_accepts: Vec<MediaUrn> = free.iter().map(|i| arg_accepts[*i].clone()).collect();
        let free_slots: Vec<MediaUrn> = free.iter().map(|i| arg_slots[*i].clone()).collect();
        let free_is_sequence: Vec<bool> = free.iter().map(|i| cap.args[*i].is_sequence).collect();

        // Pull the source URNs out of the nodes table for
        // this wiring's unnamed source NodeIds.
        let source_urns: Vec<MediaUrn> = wiring
            .source_node_ids
            .iter()
            .map(|id| nodes[*id as usize].clone())
            .collect();

        // The matching returns `(accepted URN, source URN)` pairs; each
        // accepted URN is translated back to its slot identity for the binding.
        let sorted_assignment = if source_urns.is_empty() {
            Vec::new()
        } else {
            match_sources_to_args(
                &source_urns,
                &free_accepts,
                &free_is_sequence,
                // The main input, when no named member bound it.
                primary_arg_index.and_then(|main| free.iter().position(|arg| *arg == main)),
            )
            .map_err(|failure| match failure {
                MatchFailure::Unmatched { source } => MachineAbstractionError::UnmatchedSourceInCapArgs {
                    strand_index,
                    cap_urn: wiring.cap_urn.to_string(),
                    source_urn: source_urns[source].to_string(),
                },
                // Which members the types leave open, by the names the text
                // gave them, and the args each could go to.
                MatchFailure::Ambiguous { undecided } => MachineAbstractionError::AmbiguousMachineNotation {
                    strand_index,
                    cap_urn: wiring.cap_urn.to_string(),
                    members: undecided
                        .into_iter()
                        .map(|(source, args)| UndecidedMember {
                            node: node_names
                                .map(|names| names[wiring.source_node_ids[source] as usize].clone()),
                            source_urn: source_urns[source].to_string(),
                            could_be: args.iter().map(|arg| cap.args[free[*arg]].name.clone()).collect(),
                        })
                        .collect(),
                },
            })?
        };

        // We also map each source URN back to its NodeId position in
        // `wiring.source_node_ids`, walking the unconsumed positions to handle
        // the case where two source NodeIds happen to share a URN.
        let mut consumed_positions: Vec<bool> = vec![false; wiring.source_node_ids.len()];
        for (matched_accepts, source_urn) in &sorted_assignment {
            let matched = free_accepts
                .iter()
                .position(|accepts| accepts.is_equivalent(matched_accepts).unwrap_or(false))
                .expect("matching returned an accepted URN that isn't in the cap's free args");
            let slot_urn = free_slots[matched].clone();
            let arg_name = cap.args[free[matched]].name.clone();

            // Find the source NodeId position by URN equivalence.
            let mut chosen_pos: Option<usize> = None;
            for (pos, sid) in wiring.source_node_ids.iter().enumerate() {
                if consumed_positions[pos] {
                    continue;
                }
                if nodes[*sid as usize]
                    .is_equivalent(source_urn)
                    .unwrap_or(false)
                {
                    chosen_pos = Some(pos);
                    break;
                }
            }
            let pos = chosen_pos.expect(
                "matching returned a source URN that doesn't appear in the wiring's source positions",
            );
            consumed_positions[pos] = true;
            bindings.push(EdgeAssignmentBinding::from_node(
                slot_urn,
                arg_name,
                wiring.source_node_ids[pos],
            ));
        }

        // The bindings are in the order they were made: the named ones as
        // written, then the matched ones sorted by accepted URN. To keep the
        // canonical equivalence comparison stable, re-sort by slot identity
        // (`cap_arg_media_urn`); the sort is stable, so several bindings
        // gathered onto one arg keep their declaration order — the gathered
        // sequence's item order.
        bindings.sort_by(|a, b| a.cap_arg_media_urn.cmp(&b.cap_arg_media_urn));

        // A cap that takes an input is given it.
        if let Some(index) = primary_arg_index {
            let bound = bindings
                .iter()
                .any(|b| b.cap_arg_media_urn.is_equivalent(&arg_slots[index]).unwrap_or(false));
            if !bound {
                return Err(MachineAbstractionError::MainInputUnbound {
                    strand_index,
                    cap_urn: wiring.cap_urn.to_string(),
                    main_input: cap.args[index].name.clone(),
                });
            }
        }

        // A slot with several bindings is the implicit Collect: only a sequence
        // arg takes it. Members may be scalar or sequence — execution flattens
        // deterministically: each member contributes its item(s) to the
        // gathered sequence in binding (declaration) order.

        // What the step is given, as far as iteration is concerned: each node
        // and whether the arg it is bound to takes a sequence. A constant is
        // one value wherever the step runs.
        let arg_of = |binding: &EdgeAssignmentBinding| {
            arg_slots
                .iter()
                .position(|slot| slot.is_equivalent(&binding.cap_arg_media_urn).unwrap_or(false))
                .expect("every binding names a slot of the cap")
        };
        let members: Vec<MemberShape> = bindings
            .iter()
            .filter_map(|binding| {
                binding.node().map(|node| MemberShape {
                    node,
                    arg_is_sequence: cap.args[arg_of(binding)].is_sequence,
                })
            })
            .collect();
        // The run's shape can make a machine INPUT a sequence the definition
        // knows nothing of: an exact ForEach boundary on the main input's one
        // source, when that source is an input of the strand.
        let primary_sources: Vec<NodeId> = primary_arg_index
            .map(|index| {
                bindings
                    .iter()
                    .filter(|binding| arg_of(binding) == index)
                    .filter_map(|binding| binding.node())
                    .collect()
            })
            .unwrap_or_default();
        let primary_stdin_source_is_anchor = matches!(
            primary_sources.as_slice(),
            [primary]
                if !wirings
                    .iter()
                    .any(|candidate| candidate.targets.iter().any(|(_, node)| node == primary))
        );
        shapes.push(StepShape {
            members,
            outputs: outputs
                .iter()
                .map(|output| (output.node, node_is_sequence[output.node as usize]))
                .collect(),
        });
        bound.push((wiring, bindings, outputs, primary_stdin_source_is_anchor));
    }

    // Which steps map. Iteration is DERIVED from cardinality, never authored
    // (docs 09 §Iteration); `IterationStructure` is the one place the rule is
    // computed, and the plan builder reads the same structure for a run.
    // Notation has no run, so an input of the machine is one value here.
    let iteration = IterationStructure::of(&shapes, |_| false)
        .map_err(|_| MachineAbstractionError::CyclicMachineStrand { strand_index })?;

    let mut indexed_edges: Vec<MachineEdge> = Vec::with_capacity(wirings.len());
    for (index, (wiring, bindings, outputs, primary_stdin_source_is_anchor)) in
        bound.into_iter().enumerate()
    {
        let derived_is_loop = iteration.steps()[index].map.is_some();
        let (is_loop, foreach_token_id) = match &wiring.foreach_identity {
            ForEachIdentity::Exact(Some(token_id))
                if derived_is_loop || primary_stdin_source_is_anchor =>
            {
                (true, Some(token_id.clone()))
            }
            ForEachIdentity::Exact(None) if !derived_is_loop => (false, None),
            ForEachIdentity::MintIfRequired if derived_is_loop => {
                (true, Some(StepToken::mint()))
            }
            ForEachIdentity::MintIfRequired => (false, None),
            ForEachIdentity::Exact(explicit) => {
                return Err(MachineAbstractionError::ForEachShapeMismatch {
                    strand_index,
                    cap_urn: wiring.cap_urn.to_string(),
                    has_explicit_boundary: explicit.is_some(),
                    is_loop: derived_is_loop,
                });
            }
        };

        indexed_edges.push(MachineEdge {
            token_id: wiring.token_id.clone(),
            foreach_token_id,
            cap_urn: wiring.cap_urn.clone(),
            assignment: bindings,
            outputs,
            policy: wiring.policy,
            included_as: wiring.included_as.clone(),
            is_loop,
        });
    }

    // Step 2: canonical edge order. Edge B depends on edge A iff B is given a
    // node A produces. The edges are acyclic: steps that wait on one another
    // were refused above, when the steps were placed.
    let canonical_order = topo_sort(&indexed_edges, &nodes);
    let edges: Vec<MachineEdge> = canonical_order
        .into_iter()
        .map(|i| indexed_edges[i].clone())
        .collect();

    // Step 3: anchor computation.
    let mut produced_node_ids: std::collections::HashSet<NodeId> = Default::default();
    let mut consumed_node_ids: std::collections::HashSet<NodeId> = Default::default();
    for e in &edges {
        produced_node_ids.extend(e.output_nodes());
        consumed_node_ids.extend(e.source_nodes());
    }

    // Every node is a source or an output of some step — that is what makes
    // it a node of the strand. One nothing produces is an input of the
    // strand; one nothing consumes is an output of it.
    for id in 0..nodes.len() as NodeId {
        assert!(
            produced_node_ids.contains(&id) || consumed_node_ids.contains(&id),
            "node {id} of strand {strand_index} is touched by no wiring"
        );
    }
    let mut input_anchor_ids: Vec<NodeId> = (0..nodes.len() as NodeId)
        .filter(|id| !produced_node_ids.contains(id))
        .collect();
    let mut output_anchor_ids: Vec<NodeId> = (0..nodes.len() as NodeId)
        .filter(|id| !consumed_node_ids.contains(id))
        .collect();

    // Sort anchors by canonical (URN, NodeId) order so the
    // result is stable across different node-allocation orders
    // that nevertheless yield equivalent strands.
    input_anchor_ids.sort_by(|a, b| {
        let urn_cmp = nodes[*a as usize].cmp(&nodes[*b as usize]);
        if urn_cmp == std::cmp::Ordering::Equal {
            a.cmp(b)
        } else {
            urn_cmp
        }
    });
    output_anchor_ids.sort_by(|a, b| {
        let urn_cmp = nodes[*a as usize].cmp(&nodes[*b as usize]);
        if urn_cmp == std::cmp::Ordering::Equal {
            a.cmp(b)
        } else {
            urn_cmp
        }
    });

    Ok(MachineStrand::from_resolved(
        nodes,
        edges,
        input_anchor_ids,
        output_anchor_ids,
        node_is_sequence,
    ))
}

/// Hold a constant to the validation of the arg it is bound to: the rules and
/// the schema the arg's media definition declares. A constant is a value like
/// any other the arg is given; it is known when the machine is resolved, so
/// that is when it is checked.
fn check_constant(
    registry: &FabricRegistry,
    cap_urn: &CapUrn,
    arg: &crate::cap::definition::CapArg,
    value: &serde_json::Value,
    strand_index: usize,
) -> Result<(), MachineAbstractionError> {
    let refused = |reason: String| MachineAbstractionError::ConstantFailsValidation {
        strand_index,
        cap_urn: cap_urn.to_string(),
        name: arg.name.clone(),
        constant: value.to_string(),
        reason,
    };
    // Without the definition there is nothing to hold the constant to, and a
    // constant nobody checked is not bound.
    let definition = registry.get_cached_media_def(&arg.media_urn).ok_or_else(|| {
        refused(format!(
            "the media definition of '{}' is not available, so the constant cannot be validated",
            arg.media_urn
        ))
    })?;
    if let Some(validation) = &definition.validation {
        match validation.violation(value) {
            Ok(None) => {}
            Ok(Some(rule)) => return Err(refused(format!("it breaks the rule: {rule}"))),
            Err(issue) => return Err(refused(issue)),
        }
    }
    if let Some(schema) = &definition.schema {
        let compiled = jsonschema::JSONSchema::compile(schema).map_err(|e| {
            refused(format!(
                "the schema of '{}' does not compile: {}",
                arg.media_urn, e
            ))
        })?;
        let details: Vec<String> = match compiled.validate(value) {
            Ok(()) => Vec::new(),
            Err(errors) => errors
                .map(|e| format!("{}: {}", e.instance_path, e))
                .collect(),
        };
        if !details.is_empty() {
            return Err(refused(format!("it does not match the schema: {}", details.join("; "))));
        }
    }
    Ok(())
}

// =============================================================================
// Source-to-cap-arg matching (Hungarian-style minimum-cost bipartite assignment
// with brute-force uniqueness check)
// =============================================================================

/// Match a wiring's sources to a cap's input args by minimum
/// total specificity-distance, with a uniqueness requirement.
///
/// Two phases, in strict precedence order:
///
/// 1. **Product (exact bipartite injection)** — each source claims a
///    *distinct* arg. This is the §13.4 fan-in semantics and it WINS
///    whenever a valid injection exists: the outcome (including the
///    ambiguity hard-fail on tied minimum-cost injections) is byte-identical
///    to the historical matcher.
/// 2. **Gather (implicit Collect)** — only when NO injection exists
///    (more sources than args, or a Hall violation): a **sequence** cap-arg
///    (`CapArg::is_sequence`) may absorb *several* conforming sources; a
///    scalar arg still takes at most one. The minimum-cost such assignment
///    must be unique, else `AmbiguousMachineNotation`. The N sources
///    gathered into one sequence arg become N bindings on that arg — the
///    resolver's cardinality-driven equivalent of the retired `LOOP`:
///    "how many items feed this sequence slot" is a run/wiring fact, never
///    authored syntax.
///
/// Returns the matched pairs as `(cap_arg_media_urn, source_urn)`,
/// sorted by `cap_arg_media_urn` (stable: several sources gathered into one
/// arg keep their source-declaration order, which fixes the gathered
/// sequence's item order deterministically). Fails when:
///
/// - A source has no candidate arg, or no valid assignment exists under the
///   capacity rules ([`MatchFailure::Unmatched`]).
/// - The minimum-cost assignment of the winning phase is not unique
///   ([`MatchFailure::Ambiguous`]).
pub(crate) fn match_sources_to_args(
    sources: &[MediaUrn],
    args: &[MediaUrn],
    arg_is_sequence: &[bool],
    must_bind: Option<usize>,
) -> Result<Vec<(MediaUrn, MediaUrn)>, MatchFailure> {
    debug_assert_eq!(args.len(), arg_is_sequence.len());
    let has_sequence_arg = arg_is_sequence.iter().any(|s| *s);

    if sources.len() > args.len() && !has_sequence_arg {
        // Pigeonhole: at least one source has no arg slot and no
        // sequence arg exists to gather the surplus. The first source with
        // no candidate arg is the one named; when every source has one, at
        // least one of them MUST end up unmatched, and the first stands for
        // it.
        let source = sources
            .iter()
            .position(|source| !args.iter().any(|a| source.conforms_to(a).unwrap_or(false)))
            .unwrap_or(0);
        return Err(MatchFailure::Unmatched { source });
    }

    // Build the candidate matrix. cost[s][a] is Some(distance)
    // if `sources[s]` conforms to `args[a]`, else None.
    let n_sources = sources.len();
    let n_args = args.len();
    let mut cost: Vec<Vec<Option<i64>>> = vec![vec![None; n_args]; n_sources];

    for (s_idx, source) in sources.iter().enumerate() {
        for (a_idx, arg) in args.iter().enumerate() {
            if source.conforms_to(arg).unwrap_or(false) {
                // The specificity distance: how much more the source says
                // than the arg asks. It is never negative — refining never
                // scores lower (04 §Predicates, `tagScore_monotone`).
                let distance = source
                    .specificity()
                    .checked_sub(arg.specificity())
                    .expect("a source that conforms to an arg is never less specific than it");
                cost[s_idx][a_idx] = Some(distance as i64);
            }
        }
        // Per-source: at least one candidate, else unmatched.
        if cost[s_idx].iter().all(|c| c.is_none()) {
            return Err(MatchFailure::Unmatched { source: s_idx });
        }
    }

    // An assignment binds the arg that MUST be bound — the cap's main input,
    // when these sources are what can feed it. A cap that takes an input is
    // given it, so a way of placing the sources that leaves the main input
    // out is not an assignment at all; it does not tie with one that is. This
    // is what makes `series -> compare -> result` resolve when `compare` has a
    // second arg that accepts a series: the lone source is the main input. It
    // is a constraint, not a tiebreak: two sources that both fit the main
    // input and the other arg still tie.
    let must_bind = must_bind.filter(|arg| cost.iter().any(|row| row[*arg].is_some()));

    // ── Phase 1: product — brute-force enumeration of injections. ──
    //
    // For each ordered injection f: [0..n_sources) ↣ [0..n_args)
    // such that cost[s][f(s)].is_some() for all s, compute
    // total cost. Track the minimum and how many matchings
    // achieve it.
    //
    // For the input sizes the system actually encounters (a
    // cap typically has 1–5 args, edges typically have 1–5
    // sources), brute force is bounded.
    let mut best_cost: Option<i64> = None;
    let mut best_assignments: Vec<Vec<usize>> = Vec::new();

    if n_sources <= n_args {
        let all_scalar_capacity: Vec<bool> = vec![false; n_args];
        let mut current: Vec<usize> = vec![usize::MAX; n_sources];
        let mut used: Vec<bool> = vec![false; n_args];
        enumerate_assignments(
            &cost,
            &all_scalar_capacity,
            must_bind,
            0,
            &mut current,
            &mut used,
            &mut best_cost,
            &mut best_assignments,
        );

        if best_cost.is_some() {
            // A valid injection exists — product wins. A tie among
            // minimum-cost injections is a hard ambiguity (never fall
            // through to gather: that would mask a genuinely ambiguous
            // product wiring).
            return one_assignment(best_assignments, sources, args);
        }
    }

    // ── Phase 2: gather — no injection exists. Sequence args may absorb
    // several sources; scalar args still take at most one. ──
    if !has_sequence_arg {
        // No injection and nothing can gather: every per-source candidate
        // set is non-empty, but the candidate sets collectively can't all
        // be claimed by distinct args (Hall's theorem violation). The first
        // source stands for the one that goes unmatched.
        return Err(MatchFailure::Unmatched { source: 0 });
    }

    let mut current: Vec<usize> = vec![usize::MAX; n_sources];
    let mut used: Vec<bool> = vec![false; n_args];
    enumerate_assignments(
        &cost,
        arg_is_sequence,
        must_bind,
        0,
        &mut current,
        &mut used,
        &mut best_cost,
        &mut best_assignments,
    );

    if best_cost.is_none() {
        return Err(MatchFailure::Unmatched { source: 0 });
    }
    one_assignment(best_assignments, sources, args)
}

/// Why the unnamed members of a wiring could not be placed. It speaks in
/// positions — of the sources and args the matching was given — because what
/// a source or an arg is CALLED is its caller's to say.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MatchFailure {
    /// The source at this position fits no arg left for it.
    Unmatched { source: usize },
    /// The types leave more than one answer. Each entry is a source the
    /// minimum-cost assignments place differently, and the args they place
    /// it on, in the order the args were given.
    Ambiguous { undecided: Vec<(usize, Vec<usize>)> },
}

/// The one minimum-cost assignment, as pairs — or, when several tie, the
/// sources they disagree about.
fn one_assignment(
    best: Vec<Vec<usize>>,
    sources: &[MediaUrn],
    args: &[MediaUrn],
) -> Result<Vec<(MediaUrn, MediaUrn)>, MatchFailure> {
    if let [only] = best.as_slice() {
        return Ok(assignment_to_pairs(only, sources, args));
    }
    let undecided = (0..sources.len())
        .filter_map(|source| {
            let mut placed: Vec<usize> = best.iter().map(|assignment| assignment[source]).collect();
            placed.sort_unstable();
            placed.dedup();
            (placed.len() > 1).then_some((source, placed))
        })
        .collect();
    Err(MatchFailure::Ambiguous { undecided })
}

/// Convert a source→arg assignment into `(cap_arg, source)` pairs sorted by
/// `cap_arg_media_urn`. The sort is stable and pairs are emitted in source
/// order, so several sources gathered into one arg keep their declaration
/// order — the deterministic item order of the gathered sequence.
fn assignment_to_pairs(
    assignment: &[usize],
    sources: &[MediaUrn],
    args: &[MediaUrn],
) -> Vec<(MediaUrn, MediaUrn)> {
    let mut pairs: Vec<(MediaUrn, MediaUrn)> = assignment
        .iter()
        .enumerate()
        .map(|(s_idx, &a_idx)| (args[a_idx].clone(), sources[s_idx].clone()))
        .collect();
    pairs.sort_by(|x, y| x.0.cmp(&y.0));
    pairs
}

/// Bind N concrete sources to N input anchors by minimum-cost unique assignment —
/// the SAME discipline as the resolver's source-to-cap-arg matching, applied at the
/// strand boundary: a run over a multi-input-anchor machine binds exactly one source
/// per anchor, decided by `conforms_to` + specificity distance, never by position.
///
/// Returns `assignment` where `assignment[s]` is the anchor index bound to
/// `sources[s]`. Fails hard when:
///
/// - `sources.len() != anchors.len()` (`SourceAnchorCountMismatch`);
/// - no bijection exists in which every source conforms to its anchor
///   (`UnbindableAnchorSource`);
/// - the minimum-cost bijection is not unique (`AmbiguousAnchorBinding`).
///
/// `strand_index` is used only for diagnostics.
pub fn assign_sources_to_anchors(
    sources: &[MediaUrn],
    anchors: &[MediaUrn],
    strand_index: usize,
) -> Result<Vec<usize>, MachineAbstractionError> {
    if sources.len() != anchors.len() {
        return Err(MachineAbstractionError::SourceAnchorCountMismatch {
            strand_index,
            sources: sources.len(),
            anchors: anchors.len(),
        });
    }

    // cost[s][a] = specificity distance when sources[s] conforms to anchors[a],
    // None when the pairing is impossible — identical cost model to
    // `match_sources_to_args`.
    let mut cost: Vec<Vec<Option<i64>>> = vec![vec![None; anchors.len()]; sources.len()];
    for (s_idx, source) in sources.iter().enumerate() {
        for (a_idx, anchor) in anchors.iter().enumerate() {
            if source.conforms_to(anchor).unwrap_or(false) {
                cost[s_idx][a_idx] =
                    Some(source.specificity() as i64 - anchor.specificity() as i64);
            }
        }
        if cost[s_idx].iter().all(|c| c.is_none()) {
            return Err(MachineAbstractionError::UnbindableAnchorSource {
                strand_index,
                source_urn: source.to_string(),
            });
        }
    }

    // Anchors are strictly one-source-each: all-scalar capacity restricts the
    // enumeration to bijections.
    let all_scalar: Vec<bool> = vec![false; anchors.len()];
    let mut current: Vec<usize> = vec![usize::MAX; sources.len()];
    let mut used: Vec<bool> = vec![false; anchors.len()];
    let mut best_cost: Option<i64> = None;
    let mut best_assignments: Vec<Vec<usize>> = Vec::new();
    enumerate_assignments(
        &cost,
        &all_scalar,
        None,
        0,
        &mut current,
        &mut used,
        &mut best_cost,
        &mut best_assignments,
    );

    if best_cost.is_none() {
        // Every source has a candidate anchor, but no bijection exists (Hall
        // violation). The first source is the canonical representative.
        return Err(MachineAbstractionError::UnbindableAnchorSource {
            strand_index,
            source_urn: sources.first().map(|s| s.to_string()).unwrap_or_default(),
        });
    }
    if best_assignments.len() != 1 {
        return Err(MachineAbstractionError::AmbiguousAnchorBinding { strand_index });
    }
    Ok(best_assignments
        .into_iter()
        .next()
        .expect("checked len == 1"))
}

/// Recursively enumerate all source→arg assignments with a defined cost,
/// tracking the minimum total cost and the assignments that achieve it.
///
/// `arg_can_gather[a]` gives arg `a` unbounded capacity (a sequence arg in
/// the gather phase); a `false` entry is a scalar arg claimable by at most
/// one source. Passing all-`false` restricts the enumeration to injections —
/// the historical product matching, byte-identical.
fn enumerate_assignments(
    cost: &[Vec<Option<i64>>],
    arg_can_gather: &[bool],
    must_bind: Option<usize>,
    s_idx: usize,
    current: &mut Vec<usize>,
    used: &mut Vec<bool>,
    best_cost: &mut Option<i64>,
    best_assignments: &mut Vec<Vec<usize>>,
) {
    let n_sources = cost.len();
    if s_idx == n_sources {
        // A placement that leaves out the arg that must be bound is not an
        // assignment.
        if must_bind.is_some_and(|arg| !current.contains(&arg)) {
            return;
        }
        // Compute total cost of `current`.
        let total: i64 = (0..n_sources)
            .map(|s| cost[s][current[s]].expect("assignments filter on Some(_)"))
            .sum();
        match best_cost {
            None => {
                *best_cost = Some(total);
                best_assignments.clear();
                best_assignments.push(current.clone());
            }
            Some(prev) if total < *prev => {
                *best_cost = Some(total);
                best_assignments.clear();
                best_assignments.push(current.clone());
            }
            Some(prev) if total == *prev => {
                best_assignments.push(current.clone());
            }
            Some(_) => {} // total > prev — discard
        }
        return;
    }

    for a_idx in 0..cost[s_idx].len() {
        if used[a_idx] && !arg_can_gather[a_idx] {
            continue;
        }
        if cost[s_idx][a_idx].is_none() {
            continue;
        }
        let was_used = used[a_idx];
        used[a_idx] = true;
        current[s_idx] = a_idx;
        enumerate_assignments(
            cost,
            arg_can_gather,
            must_bind,
            s_idx + 1,
            current,
            used,
            best_cost,
            best_assignments,
        );
        used[a_idx] = was_used;
    }
}

// =============================================================================
// Topological sort with structural tiebreaker
// =============================================================================

/// Kahn's algorithm over the resolved data-flow dependency
/// graph. Returns the canonical ordering of edge indices.
///
/// Edge B depends on edge A iff B is given a node A produces. The edges it
/// is given are acyclic: steps that wait on one another are refused when the
/// steps are placed (`IterationStructure::of`), before any edge is made of
/// them.
fn topo_sort(edges: &[MachineEdge], nodes: &[MediaUrn]) -> Vec<usize> {
    let n = edges.len();

    // Map: NodeId → the edges that produce it.
    let mut producers_of: HashMap<NodeId, Vec<usize>> = HashMap::new();
    for (idx, e) in edges.iter().enumerate() {
        for node in e.output_nodes() {
            producers_of.entry(node).or_default().push(idx);
        }
    }

    // Edge B's predecessors: every edge that produces a node B is given.
    let mut indegree: Vec<usize> = vec![0; n];
    let mut successors: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (b_idx, b) in edges.iter().enumerate() {
        for source in b.source_nodes() {
            if let Some(producers) = producers_of.get(&source) {
                for &a_idx in producers {
                    successors[a_idx].push(b_idx);
                    indegree[b_idx] += 1;
                }
            }
        }
    }

    let mut result: Vec<usize> = Vec::with_capacity(n);
    let mut ready: Vec<usize> = (0..n).filter(|&i| indegree[i] == 0).collect();
    sort_ready(&mut ready, edges, nodes);

    while let Some(idx) = ready.first().copied() {
        ready.remove(0);
        result.push(idx);
        for &succ in &successors[idx] {
            indegree[succ] -= 1;
            if indegree[succ] == 0 {
                ready.push(succ);
                sort_ready(&mut ready, edges, nodes);
            }
        }
    }

    assert_eq!(result.len(), n, "placed steps wait on one another");
    result
}

/// Sort the ready set in canonical structural order so Kahn's
/// algorithm produces a deterministic output. The order is:
///
/// 1. cap URN (structural `CapUrn::Ord`)
/// 2. assignment vec (element-wise structural `MediaUrn::Ord`
///    on `cap_arg_media_urn` then on the source's URN)
/// 3. target node URN (structural `MediaUrn::Ord`)
/// 4. `is_loop` flag
fn sort_ready(ready: &mut Vec<usize>, edges: &[MachineEdge], nodes: &[MediaUrn]) {
    ready.sort_by(|&a, &b| {
        let ea = &edges[a];
        let eb = &edges[b];
        match ea.cap_urn.cmp(&eb.cap_urn) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        // Compare assignments element-wise; pre-sorted by
        // cap_arg_media_urn so positional comparison is
        // canonical.
        for (ba, bb) in ea.assignment.iter().zip(eb.assignment.iter()) {
            match ba.cap_arg_media_urn.cmp(&bb.cap_arg_media_urn) {
                std::cmp::Ordering::Equal => {}
                ord => return ord,
            }
            // A node source orders by its URN; a constant by its text, after
            // every node.
            let ord = match (&ba.source, &bb.source) {
                (BindingSource::Node(na), BindingSource::Node(nb)) => {
                    nodes[*na as usize].cmp(&nodes[*nb as usize])
                }
                (BindingSource::Node(_), BindingSource::Constant(_)) => std::cmp::Ordering::Less,
                (BindingSource::Constant(_), BindingSource::Node(_)) => std::cmp::Ordering::Greater,
                (BindingSource::Constant(ca), BindingSource::Constant(cb)) => {
                    ca.to_string().cmp(&cb.to_string())
                }
            };
            match ord {
                std::cmp::Ordering::Equal => {}
                ord => return ord,
            }
        }
        match ea.assignment.len().cmp(&eb.assignment.len()) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        for (oa, ob) in ea.outputs.iter().zip(eb.outputs.iter()) {
            match oa.output_media_urn.cmp(&ob.output_media_urn) {
                std::cmp::Ordering::Equal => {}
                ord => return ord,
            }
            match nodes[oa.node as usize].cmp(&nodes[ob.node as usize]) {
                std::cmp::Ordering::Equal => {}
                ord => return ord,
            }
        }
        match ea.outputs.len().cmp(&eb.outputs.len()) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        ea.is_loop.cmp(&eb.is_loop)
    });
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::{
        match_sources_to_args, resolve_pre_interned, resolve_strand, ForEachIdentity, MatchFailure,
        PreInternedWiring,
    };
    use crate::machine::error::MachineAbstractionError;
    use crate::machine::graph::{BindingSource, NodeId};
    use crate::machine::StepPolicy;
    use crate::planner::{ArgSourceRef, StepToken, Strand, StrandStep, StrandStepType};
    use crate::machine::test_fixtures::{
        build_cap, build_cap_with_slot_stdin_pairs, cap_step, collect_step, for_each_step, media,
        registry_with, strand_from_steps,
    };
    use crate::urn::cap_urn::CapUrn;

    // ----- match_sources_to_args -------------------------------------------

    // TEST1178: One source is assigned to the single compatible cap argument.
    #[test]
    fn test1178_match_single_source_picks_unique_arg() {
        // Single source `media:document;ext=pdf` against a one-arg cap. Trivial
        // bipartite matching: one source, one arg, exact tag-set
        // equivalence → distance 0 → unique → assignment is the
        // single pair.
        let sources = vec![media("media:document;ext=pdf")];
        let args = vec![media("media:document;ext=pdf")];
        let pairs = match_sources_to_args(&sources, &args, &vec![false; args.len()], None)
            .expect("trivial single-source match must succeed");
        assert_eq!(pairs.len(), 1);
        assert!(pairs[0].0.is_equivalent(&media("media:document;ext=pdf")).unwrap());
        assert!(pairs[0].1.is_equivalent(&media("media:document;ext=pdf")).unwrap());
    }

    // TEST1179: Source-to-arg matching assigns a more specific source to a compatible general argument.
    #[test]
    fn test1179_match_more_specific_source_assigned_to_general_arg() {
        // The cap declares `media:enc=utf-8`. The source is the
        // more-specific `media:enc=utf-8;page`. The source must
        // conform (it does, enc=utf-8;page ⪯ enc=utf-8) and get
        // assigned to that arg with distance > 0.
        let sources = vec![media("media:enc=utf-8;page")];
        let args = vec![media("media:enc=utf-8")];
        let pairs = match_sources_to_args(&sources, &args, &vec![false; args.len()], None)
            .expect("more-specific source must be matched to its arg");
        assert!(pairs[0].0.is_equivalent(&media("media:enc=utf-8")).unwrap());
        assert!(pairs[0]
            .1
            .is_equivalent(&media("media:enc=utf-8;page"))
            .unwrap());
    }

    // TEST1180: Matching fails when a source does not conform to any cap input argument.
    #[test]
    fn test1180_match_unmatched_source_fails_hard() {
        // The source URN does not conform to any of the cap's
        // args. Must surface as `UnmatchedSourceInCapArgs` —
        // never as a silent zero-cost or random pairing.
        let sources = vec![media("media:numeric")];
        let args = vec![media("media:enc=utf-8")];
        assert_eq!(
            match_sources_to_args(&sources, &args, &vec![false; args.len()], None),
            Err(MatchFailure::Unmatched { source: 0 })
        );
        // It is the source that fits nothing that is named, wherever it stands.
        let sources = vec![media("media:enc=utf-8;page"), media("media:numeric")];
        let args = vec![media("media:enc=utf-8"), media("media:enc=utf-8;page")];
        assert_eq!(
            match_sources_to_args(&sources, &args, &vec![false; args.len()], None),
            Err(MatchFailure::Unmatched { source: 1 })
        );
    }

    // TEST1181: Two sources are matched deterministically when specificity breaks the tie.
    #[test]
    fn test1181_match_two_sources_disambiguated_by_specificity() {
        // Two sources, two args. One source perfectly matches one
        // arg with distance 0; the other can only conform to the
        // remaining arg. The resolver picks the unique minimum-
        // cost matching.
        //
        // sources: [media:ext=png;image, media:enc=utf-8;model-spec]
        // args:    [media:ext=png;image, media:enc=utf-8]
        //
        // image;png ⪯ image;png (dist 0); image;png ⪯ enc=utf-8? no
        // enc=utf-8;model-spec ⪯ image;png? no
        // enc=utf-8;model-spec ⪯ enc=utf-8 (dist 1)
        //
        // Unique optimum: (image;png → image;png), (enc=utf-8;model-spec → enc=utf-8)
        let sources = vec![
            media("media:ext=png;image"),
            media("media:enc=utf-8;model-spec"),
        ];
        let args = vec![media("media:ext=png;image"), media("media:enc=utf-8")];
        let pairs =
            match_sources_to_args(&sources, &args, &vec![false; args.len()], None).unwrap();
        assert_eq!(pairs.len(), 2);
        // Pairs are sorted by cap_arg_media_urn structurally.
        // image;png and enc=utf-8: structural Ord places
        // image;png first (more tags / different prefix string).
        // Don't depend on the exact sort order; check the
        // mapping by content instead.
        let mut found_image = false;
        let mut found_text = false;
        for (arg, src) in &pairs {
            if arg.is_equivalent(&media("media:ext=png;image")).unwrap() {
                assert!(src.is_equivalent(&media("media:ext=png;image")).unwrap());
                found_image = true;
            } else if arg.is_equivalent(&media("media:enc=utf-8")).unwrap() {
                assert!(src
                    .is_equivalent(&media("media:enc=utf-8;model-spec"))
                    .unwrap());
                found_text = true;
            }
        }
        assert!(found_image && found_text, "both arg slots must be assigned");

        // The pairs come by arg URN — its parts, not its text — whatever
        // order the sources stand in.
        for ordered in [sources.clone(), vec![sources[1].clone(), sources[0].clone()]] {
            let pairs =
                match_sources_to_args(&ordered, &args, &vec![false; args.len()], None).unwrap();
            assert_eq!(pairs.len(), 2);
            assert!(pairs[0].0 < pairs[1].0);
        }
        let file_things = vec![media("media:file-path"), media("media:file;sample")];
        let by_parts =
            match_sources_to_args(&file_things, &file_things, &[false, false], None).unwrap();
        assert_eq!(
            by_parts.iter().map(|(arg, _)| arg.to_string()).collect::<Vec<_>>(),
            vec![media("media:file;sample").to_string(), media("media:file-path").to_string()]
        );

        // One source that fits two args takes the one it is closest to,
        // whichever of the two is tried first.
        let page = vec![media("media:enc=utf-8;page")];
        let (general, exact) = (media("media:enc=utf-8"), media("media:enc=utf-8;page"));
        for either in [vec![general.clone(), exact.clone()], vec![exact.clone(), general.clone()]] {
            let pairs = match_sources_to_args(&page, &either, &[false, false], None).unwrap();
            assert_eq!(pairs.len(), 1);
            assert!(
                pairs[0].0.is_equivalent(&exact).unwrap(),
                "the exact arg, not the general one: {}",
                pairs[0].0
            );
        }
    }

    // TEST1182: Matching fails as ambiguous when two sources can be swapped at equal minimum cost.
    #[test]
    fn test1182_match_ambiguous_when_two_sources_could_swap() {
        // Two sources that can both feed both args at exactly
        // the same total cost. The minimum-cost matching is not
        // unique → AmbiguousMachineNotation.
        //
        // Both sources are `media:enc=utf-8`; both args are
        // `media:enc=utf-8`. Distance 0 either way; both
        // permutations are tied at total cost 0.
        let sources = vec![media("media:enc=utf-8"), media("media:enc=utf-8")];
        let args = vec![media("media:enc=utf-8"), media("media:enc=utf-8")];
        // Both are left open, and each could go to either arg.
        assert_eq!(
            match_sources_to_args(&sources, &args, &vec![false; args.len()], None),
            Err(MatchFailure::Ambiguous { undecided: vec![(0, vec![0, 1]), (1, vec![0, 1])] })
        );

        // Only the sources the tie is about are named: a third that fits one
        // arg alone is decided, whatever the other two do.
        let sources = vec![media("media:enc=utf-8"), media("media:numeric"), media("media:enc=utf-8")];
        let args = vec![media("media:enc=utf-8"), media("media:enc=utf-8"), media("media:numeric")];
        assert_eq!(
            match_sources_to_args(&sources, &args, &vec![false; args.len()], None),
            Err(MatchFailure::Ambiguous { undecided: vec![(0, vec![0, 1]), (2, vec![0, 1])] })
        );
    }

    // TEST1183: Matching fails when more sources are provided than the cap has input arguments.
    #[test]
    fn test1183_match_more_sources_than_args_fails_hard() {
        let sources = vec![
            media("media:document;ext=pdf"),
            media("media:document;ext=pdf"),
            media("media:document;ext=pdf"),
        ];
        let args = vec![media("media:document;ext=pdf"), media("media:document;ext=pdf")];
        // Every source has somewhere to go and one of them must go without:
        // the first stands for it.
        assert_eq!(
            match_sources_to_args(&sources, &args, &vec![false; args.len()], None),
            Err(MatchFailure::Unmatched { source: 0 })
        );

        // Where one of them fits nothing at all, that is the one named.
        let sources = vec![
            media("media:document;ext=pdf"),
            media("media:numeric"),
            media("media:document;ext=pdf"),
        ];
        assert_eq!(
            match_sources_to_args(&sources, &args, &vec![false; args.len()], None),
            Err(MatchFailure::Unmatched { source: 1 })
        );
    }

    // ----- resolve_strand (planner path) -----------------------------------

    // TEST1184: Resolving a strand with one cap produces one resolved machine edge.
    #[test]
    fn test1184_resolve_strand_single_cap_produces_one_edge() {
        let extract_cap = build_cap(
            "cap:in=\"media:document;ext=pdf\";extract;out=\"media:enc=utf-8;ext=txt\"",
            "extract",
            &["media:document;ext=pdf"],
            "media:enc=utf-8;ext=txt",
        );
        let registry = registry_with(vec![extract_cap]);
        let strand = strand_from_steps(
            vec![cap_step(
                "cap:in=\"media:document;ext=pdf\";extract;out=\"media:enc=utf-8;ext=txt\"",
                "extract",
                "media:document;ext=pdf",
                "media:enc=utf-8;ext=txt",
            )],
            "pdf to txt",
        );
        let resolved = resolve_strand(&strand, &registry, 0).expect("must resolve");
        assert_eq!(resolved.edges().len(), 1);
        assert_eq!(resolved.edges()[0].assignment.len(), 1);
        // The single edge's assignment maps the cap arg
        // media:document;ext=pdf to a node whose URN is media:pdf.
        let binding = &resolved.edges()[0].assignment[0];
        assert!(binding
            .cap_arg_media_urn
            .is_equivalent(&media("media:document;ext=pdf"))
            .unwrap());
        let src_urn = resolved.node_urn(binding.node().expect("a node binding"));
        assert!(src_urn.is_equivalent(&media("media:document;ext=pdf")).unwrap());
        // Anchors: input is media:document;ext=pdf, output is media:enc=utf-8;ext=txt.
        let inputs = resolved.input_anchors();
        let outputs = resolved.output_anchors();
        assert_eq!(inputs.len(), 1);
        assert_eq!(outputs.len(), 1);
        assert!(inputs[0].is_equivalent(&media("media:document;ext=pdf")).unwrap());
        assert!(outputs[0]
            .is_equivalent(&media("media:enc=utf-8;ext=txt"))
            .unwrap());
    }

    // TEST1185: Resolving a chained strand reuses the intermediate node between adjacent caps.
    #[test]
    fn test1185_resolve_strand_chained_caps_share_intermediate_node() {
        // Two-step strand: pdf → extract → txt → embed → vec.
        // The intermediate node `media:enc=utf-8;ext=txt` is produced
        // by extract and consumed by embed. The resolver must
        // intern these as the SAME NodeId, so the strand has
        // exactly three node positions, not four.
        let extract = build_cap(
            "cap:in=\"media:document;ext=pdf\";extract;out=\"media:enc=utf-8;ext=txt\"",
            "extract",
            &["media:document;ext=pdf"],
            "media:enc=utf-8;ext=txt",
        );
        let embed = build_cap(
            "cap:in=\"media:enc=utf-8\";embed;out=\"media:vec;record\"",
            "embed",
            &["media:enc=utf-8"],
            "media:vec;record",
        );
        let registry = registry_with(vec![extract, embed]);

        let strand = strand_from_steps(
            vec![
                cap_step(
                    "cap:in=\"media:document;ext=pdf\";extract;out=\"media:enc=utf-8;ext=txt\"",
                    "extract",
                    "media:document;ext=pdf",
                    "media:enc=utf-8;ext=txt",
                ),
                cap_step(
                    "cap:in=\"media:enc=utf-8\";embed;out=\"media:vec;record\"",
                    "embed",
                    "media:enc=utf-8;ext=txt",
                    "media:vec;record",
                ),
            ],
            "pdf to vec",
        );

        let resolved = resolve_strand(&strand, &registry, 0).expect("must resolve");
        assert_eq!(resolved.edges().len(), 2);
        assert_eq!(
            resolved.nodes().len(),
            3,
            "three distinct data positions: pdf, txt;enc=utf-8, vec;record"
        );

        // The first edge's target NodeId must equal the second
        // edge's primary source NodeId.
        let extract_target = resolved.edges()[0].main_target().expect("the main output is taken");
        let embed_source = resolved.edges()[1].assignment[0].node().expect("a node binding");
        assert_eq!(
            extract_target, embed_source,
            "intermediate data position must be one shared NodeId"
        );

        // Anchors.
        let inputs = resolved.input_anchors();
        let outputs = resolved.output_anchors();
        assert_eq!(inputs.len(), 1);
        assert_eq!(outputs.len(), 1);
        assert!(inputs[0].is_equivalent(&media("media:document;ext=pdf")).unwrap());
        assert!(outputs[0]
            .is_equivalent(&media("media:vec;record"))
            .unwrap());
    }

    // TEST1186: Resolving a strand with ForEach marks the following cap edge as a loop.
    #[test]
    fn test1186_resolve_strand_foreach_marks_following_cap_as_loop() {
        // ForEach immediately followed by a cap. `is_loop` is derived from
        // cardinality: disbind produces a SEQUENCE of pages, and make_decision
        // consumes a scalar page, so make_decision's edge maps per item. Collect at
        // the end is elided.
        let mut disbind = build_cap(
            "cap:in=\"media:document;ext=pdf\";disbind;out=\"media:enc=utf-8;page\"",
            "disbind",
            &["media:document;ext=pdf"],
            "media:enc=utf-8;page",
        );
        disbind.outputs[0].is_sequence = true;
        let make_decision = build_cap(
            "cap:in=\"media:enc=utf-8\";make-decision;out=\"media:decision;fmt=json;record\"",
            "make_decision",
            &["media:enc=utf-8"],
            "media:decision;fmt=json;record",
        );
        let registry = registry_with(vec![disbind, make_decision]);

        let strand = strand_from_steps(
            vec![
                cap_step(
                    "cap:in=\"media:document;ext=pdf\";disbind;out=\"media:enc=utf-8;page\"",
                    "disbind",
                    "media:document;ext=pdf",
                    "media:enc=utf-8;page",
                ),
                for_each_step("media:enc=utf-8;page"),
                cap_step(
                    "cap:in=\"media:enc=utf-8\";make-decision;out=\"media:decision;fmt=json;record\"",
                    "make_decision",
                    "media:enc=utf-8",
                    "media:decision;fmt=json;record",
                ),
                collect_step("media:decision;fmt=json;record"),
            ],
            "disbind+foreach+make_decision",
        );

        let resolved = resolve_strand(&strand, &registry, 0).expect("must resolve");
        assert_eq!(resolved.edges().len(), 2);
        // First edge (disbind) is not a loop; second
        // (make-decision) is. The URN tag uses hyphens; the cap
        // title is separately stored with underscores but isn't
        // part of the URN serialization.
        let disbind_edge = resolved
            .edges()
            .iter()
            .find(|e| e.cap_urn.to_string().contains("disbind"))
            .expect("disbind edge present");
        let decision_edge = resolved
            .edges()
            .iter()
            .find(|e| e.cap_urn.to_string().contains("make-decision"))
            .expect("make-decision edge present");
        assert!(!disbind_edge.is_loop, "disbind is not in a loop");
        assert!(decision_edge.is_loop, "make_decision is inside ForEach");

        // Critical: disbind's target NodeId must be the same
        // as make_decision's source NodeId — the intermediate
        // data position (media:enc=utf-8;page) is shared even
        // though disbind declares out=media:enc=utf-8;page and
        // make_decision declares in=media:enc=utf-8 (less
        // specific, but on the same specialization chain).
        // Positional interning collapses them.
        let disbind_target = disbind_edge.main_target().expect("the main output is taken");
        let decision_source = decision_edge.assignment[0].node().expect("a node binding");
        assert_eq!(
            disbind_target, decision_source,
            "disbind target and make_decision source must share the same NodeId (positional interning)"
        );
        // The canonical URN at that shared node must be
        // the more-specific one: media:page;enc=utf-8.
        assert!(
            resolved
                .node_urn(disbind_target)
                .is_equivalent(&media("media:enc=utf-8;page"))
                .unwrap(),
            "shared node URN must be the more-specific media:enc=utf-8;page, got: {}",
            resolved.node_urn(disbind_target)
        );
    }

    // TEST1187: Strand resolution fails when a referenced cap is not found in the registry.
    #[test]
    fn test1187_resolve_strand_unknown_cap_fails_hard() {
        let registry = registry_with(vec![]);
        let strand = strand_from_steps(
            vec![cap_step(
                "cap:in=\"media:document;ext=pdf\";extract;out=\"media:enc=utf-8;ext=txt\"",
                "extract",
                "media:document;ext=pdf",
                "media:enc=utf-8;ext=txt",
            )],
            "pdf to txt with empty registry",
        );
        let err = resolve_strand(&strand, &registry, 0).unwrap_err();
        assert!(matches!(err, MachineAbstractionError::UnknownCap { .. }));
    }

    // TEST1188: Strand resolution fails when the strand contains no capability steps.
    #[test]
    fn test1188_resolve_strand_no_cap_steps_fails_hard() {
        let registry = registry_with(vec![]);
        let strand = strand_from_steps(
            vec![
                for_each_step("media:document;ext=pdf"),
                collect_step("media:document;ext=pdf"),
            ],
            "no caps at all",
        );
        let err = resolve_strand(&strand, &registry, 0).unwrap_err();
        assert!(matches!(err, MachineAbstractionError::NoCapabilitySteps));
    }

    // TEST1189: Strand resolution keeps canonical anchor ordering stable across equivalent inputs.
    #[test]
    fn test1189_resolve_strand_canonical_anchor_order_is_stable() {
        // Two strands built from identical caps in identical
        // positions must produce byte-identical canonical
        // anchor URN order. This pins the structural sort.
        let extract = build_cap(
            "cap:in=\"media:document;ext=pdf\";extract;out=\"media:enc=utf-8;ext=txt\"",
            "extract",
            &["media:document;ext=pdf"],
            "media:enc=utf-8;ext=txt",
        );
        let registry = registry_with(vec![extract]);
        let strand = strand_from_steps(
            vec![cap_step(
                "cap:in=\"media:document;ext=pdf\";extract;out=\"media:enc=utf-8;ext=txt\"",
                "extract",
                "media:document;ext=pdf",
                "media:enc=utf-8;ext=txt",
            )],
            "pdf to txt",
        );
        let r1 = resolve_strand(&strand, &registry, 0).unwrap();
        let r2 = resolve_strand(&strand, &registry, 0).unwrap();
        let i1 = r1.input_anchors();
        let i2 = r2.input_anchors();
        assert_eq!(i1.len(), i2.len());
        for (a, b) in i1.iter().zip(i2.iter()) {
            assert!(a.is_equivalent(b).unwrap());
        }
    }

    // TEST1190: Inverse format converters resolve without introducing a cycle in the strand graph.
    #[test]
    fn test1190_resolve_strand_inverse_format_converters_no_cycle() {
        // A strand that visits two inverse format converters
        // (numeric → integer;numeric →
        // numeric). Under positional interning, each
        // cap step's target is a FRESH NodeId, so the strand's
        // source NodeId(0) (numeric) and the second
        // step's target NodeId(2) (also numeric) are
        // DISTINCT positions. There is no cycle.
        //
        // The planner's visited-set prevents the path finder
        // from producing this strand in practice (it would
        // revisit the same visited key). But programmatic
        // strand construction can produce it, and the resolver
        // must handle it correctly.
        let to_int = build_cap(
            "cap:in=\"media:numeric\";coerce-int;out=\"media:integer;numeric\"",
            "coerce_int",
            &["media:numeric"],
            "media:integer;numeric",
        );
        let to_num = build_cap(
            "cap:in=\"media:integer;numeric\";coerce-num;out=\"media:numeric\"",
            "coerce_num",
            &["media:integer;numeric"],
            "media:numeric",
        );
        let registry = registry_with(vec![to_int, to_num]);
        let strand = strand_from_steps(
            vec![
                cap_step(
                    "cap:in=\"media:numeric\";coerce-int;out=\"media:integer;numeric\"",
                    "coerce_int",
                    "media:numeric",
                    "media:integer;numeric",
                ),
                cap_step(
                    "cap:in=\"media:integer;numeric\";coerce-num;out=\"media:numeric\"",
                    "coerce_num",
                    "media:integer;numeric",
                    "media:numeric",
                ),
            ],
            "round-trip numeric coercion",
        );

        let resolved = resolve_strand(&strand, &registry, 0).expect(
            "inverse format converters must resolve without cycle under positional interning",
        );
        // Three distinct data positions: input
        // (numeric), intermediate
        // (integer;numeric), and output
        // (numeric). Input and output share a URN
        // but are distinct NodeIds.
        assert_eq!(resolved.nodes().len(), 3);
        assert_eq!(resolved.edges().len(), 2);
        // coerce_int's target (intermediate) is shared with
        // coerce_num's source — same positional boundary.
        let int_target = resolved.edges()[0].main_target().expect("the main output is taken");
        let num_source = resolved.edges()[1].assignment[0].node().expect("a node binding");
        assert_eq!(int_target, num_source);
    }

    // TEST1191: Disbinding a PDF with a file-path slot preserves the expected identity of the slot binding.
    #[test]
    fn test1191_resolve_strand_disbind_pdf_with_file_path_slot_identity() {
        // Regression: a cap whose arg slot identity differs
        // from its stdin source URN. The disbind cap declares
        // `media:enc=utf-8;file-path` as the slot identity but
        // its stdin source delivers `media:document;ext=pdf` (this is the
        // wire-level wraparound: cartridge_runtime auto-converts
        // a file-path argument into a stdin byte stream of
        // the inner type).
        //
        // The resolver MUST match the wiring's `media:document;ext=pdf`
        // source against the stdin URN of the arg, NOT against
        // the slot identity. Before this fix the resolver
        // would have returned `UnmatchedSourceInCapArgs`
        // because `media:document;ext=pdf` does not conform to
        // `media:enc=utf-8;file-path`.
        let disbind = build_cap_with_slot_stdin_pairs(
            "cap:in=\"media:document;ext=pdf\";disbind;out=\"media:enc=utf-8;page\"",
            "disbind",
            &[("media:enc=utf-8;file-path", "media:document;ext=pdf")],
            "media:enc=utf-8;page",
        );
        let registry = registry_with(vec![disbind]);

        let strand = strand_from_steps(
            vec![cap_step(
                "cap:in=\"media:document;ext=pdf\";disbind;out=\"media:enc=utf-8;page\"",
                "disbind",
                "media:document;ext=pdf",
                "media:enc=utf-8;page",
            )],
            "pdf to pages",
        );

        let resolved = resolve_strand(&strand, &registry, 0)
            .expect("disbind strand must resolve via stdin URN matching, not slot identity");
        assert_eq!(resolved.edges().len(), 1);
        let binding = &resolved.edges()[0].assignment[0];

        // The binding's `cap_arg_media_urn` must be the SLOT
        // identity (`media:enc=utf-8;file-path`), since that is
        // what the cap definition uses to label the arg slot
        // (RULE1).
        assert!(
            binding
                .cap_arg_media_urn
                .is_equivalent(&media("media:enc=utf-8;file-path"))
                .unwrap(),
            "binding cap_arg_media_urn must be the slot identity, got: {}",
            binding.cap_arg_media_urn
        );

        // The source NodeId must point at a node whose URN is
        // `media:document;ext=pdf` — the data-type URN, what the planner
        // sees flowing on the wire.
        let source_urn = resolved.node_urn(binding.node().expect("a node binding"));
        assert!(
            source_urn.is_equivalent(&media("media:document;ext=pdf")).unwrap(),
            "source node URN must be media:document;ext=pdf (the data-type URN), got: {}",
            source_urn
        );
    }

    // TEST1138: EdgeAssignmentBinding list is sorted by cap_arg_media_urn for canonical form.
    // A two-source cap whose args are added in reverse-alphabetical order must still produce
    // bindings sorted alphabetically by cap_arg_media_urn, enabling canonical comparison
    // regardless of creation order.
    #[test]
    fn test1138_assignment_bindings_are_sorted_by_cap_arg_media_urn() {
        // Cap with two stdin args: enc=utf-8 (later alphabetically) and pdf (earlier).
        // Args are listed in reverse order so the test fails if sorting is skipped.
        let merge_cap = build_cap(
            "cap:in=\"media:document;ext=pdf\";merge;out=\"media:enc=utf-8;ext=txt\"",
            "merge",
            &["media:enc=utf-8", "media:document;ext=pdf"],
            "media:enc=utf-8;ext=txt",
        );
        let registry = registry_with(vec![merge_cap]);

        // Pre-interned nodes: 0=pdf, 1=enc=utf-8, 2=enc=utf-8;ext=txt (output)
        let nodes = vec![
            media("media:document;ext=pdf"),
            media("media:enc=utf-8"),
            media("media:enc=utf-8;ext=txt"),
        ];
        let cap_urn =
            CapUrn::from_string("cap:in=\"media:document;ext=pdf\";merge;out=\"media:enc=utf-8;ext=txt\"")
                .unwrap();
        let wirings = vec![PreInternedWiring {
            token_id: "tok-1".parse().unwrap(),
            foreach_identity: ForEachIdentity::MintIfRequired,
            cap_urn,
            source_node_ids: vec![0, 1], // pdf first, enc=utf-8 second
            named_sources: Vec::new(),
            targets: vec![(None, 2)],
            policy: StepPolicy::default(),
            included_as: Vec::new(),
        }];

        let strand = resolve_pre_interned(nodes, &wirings, &registry, 0).unwrap();
        assert_eq!(strand.edges().len(), 1);

        let bindings = &strand.edges()[0].assignment;
        assert_eq!(bindings.len(), 2);

        let slot_urns: Vec<String> = bindings
            .iter()
            .map(|b| b.cap_arg_media_urn.to_string())
            .collect();
        let mut sorted = slot_urns.clone();
        sorted.sort();
        assert_eq!(
            slot_urns, sorted,
            "bindings must be sorted by cap_arg_media_urn, got: {:?}",
            slot_urns
        );
    }

    // TEST1308: A wiring set that feeds a cap's output back into an ancestor
    // forms a cycle and must fail hard with CyclicMachineStrand carrying the
    // strand index. Cycle: node 0 → cap A → node 1 → cap B → node 0.
    #[test]
    fn test1308_cyclic_strand_fails_hard() {
        let urn_a = "cap:in=\"media:document;ext=pdf\";op-a;out=\"media:enc=utf-8;ext=txt\"";
        let urn_b = "cap:in=\"media:enc=utf-8;ext=txt\";op-b;out=\"media:document;ext=pdf\"";
        let cap_a = build_cap(urn_a, "op_a", &["media:document;ext=pdf"], "media:enc=utf-8;ext=txt");
        let cap_b = build_cap(urn_b, "op_b", &["media:enc=utf-8;ext=txt"], "media:document;ext=pdf");
        let registry = registry_with(vec![cap_a, cap_b]);

        let nodes = vec![media("media:document;ext=pdf"), media("media:enc=utf-8;ext=txt")];
        // node 0 -> cap_a -> node 1  and  node 1 -> cap_b -> node 0 (cycle)
        let wirings = vec![
            PreInternedWiring {
                token_id: "tok-2".parse().unwrap(),
                foreach_identity: ForEachIdentity::MintIfRequired,
                cap_urn: CapUrn::from_string(urn_a).unwrap(),
                source_node_ids: vec![0],
                named_sources: Vec::new(),
                targets: vec![(None, 1)],
                policy: StepPolicy::default(),
                included_as: Vec::new(),
            },
            PreInternedWiring {
                token_id: "tok-3".parse().unwrap(),
                foreach_identity: ForEachIdentity::MintIfRequired,
                cap_urn: CapUrn::from_string(urn_b).unwrap(),
                source_node_ids: vec![1],
                named_sources: Vec::new(),
                targets: vec![(None, 0)],
                policy: StepPolicy::default(),
                included_as: Vec::new(),
            },
        ];

        let err = resolve_pre_interned(nodes, &wirings, &registry, 5).unwrap_err();
        match err {
            MachineAbstractionError::CyclicMachineStrand { strand_index } => {
                assert_eq!(strand_index, 5);
            }
            other => panic!("expected CyclicMachineStrand, got {other:?}"),
        }
    }

    // ----- sequence-arg gather (implicit Collect) ---------------------------

    // TEST1400: N sources gather into a single sequence arg when no injection
    // exists. Three page sources feed a one-arg concat cap whose arg is a
    // sequence — all three land on the same arg, in source-declaration order.
    #[test]
    fn test1400_gather_n_sources_into_sequence_arg() {
        let sources = vec![
            media("media:enc=utf-8;page"),
            media("media:enc=utf-8;page"),
            media("media:enc=utf-8;page"),
        ];
        let args = vec![media("media:enc=utf-8")];
        let arg_seq = vec![true];
        let pairs = match_sources_to_args(&sources, &args, &arg_seq, None)
            .expect("three sources must gather into the sequence arg");
        assert_eq!(pairs.len(), 3, "one binding per gathered source");
        for (arg, src) in &pairs {
            assert!(arg.is_equivalent(&media("media:enc=utf-8")).unwrap());
            assert!(src.is_equivalent(&media("media:enc=utf-8;page")).unwrap());
        }
    }

    // TEST1401: product precedence — when a valid injection exists it wins,
    // even though gathering everything into the sequence arg is also valid.
    // s1 exactly fits the scalar page arg; s2 only fits the sequence arg.
    #[test]
    fn test1401_product_precedence_over_gather() {
        let sources = vec![
            media("media:enc=utf-8;page"),
            media("media:enc=utf-8;summary"),
        ];
        let args = vec![media("media:enc=utf-8;page"), media("media:enc=utf-8")];
        let arg_seq = vec![false, true];
        let pairs = match_sources_to_args(&sources, &args, &arg_seq, None)
            .expect("unique injection must resolve");
        assert_eq!(pairs.len(), 2);
        // The page source must claim the scalar page arg (injection), NOT be
        // gathered into the sequence arg alongside the summary.
        let page_pair = pairs
            .iter()
            .find(|(a, _)| a.is_equivalent(&media("media:enc=utf-8;page")).unwrap())
            .expect("scalar page arg must be claimed");
        assert!(page_pair
            .1
            .is_equivalent(&media("media:enc=utf-8;page"))
            .unwrap());
        let seq_pair = pairs
            .iter()
            .find(|(a, _)| a.is_equivalent(&media("media:enc=utf-8")).unwrap())
            .expect("sequence arg must take the remaining source");
        assert!(seq_pair
            .1
            .is_equivalent(&media("media:enc=utf-8;summary"))
            .unwrap());

        // Two pages, a sequence arg that takes pages and a scalar arg that
        // takes any text: each page can take either arg, at one cost — a tie.
        // Gathering both into the sequence arg would be cheaper and unique,
        // and it is not what is asked: a tied product is ambiguous, it does
        // not become a gather.
        let pages = vec![media("media:enc=utf-8;page"), media("media:enc=utf-8;page")];
        assert_eq!(
            match_sources_to_args(&pages, &args, &[true, false], None),
            Err(MatchFailure::Ambiguous { undecided: vec![(0, vec![0, 1]), (1, vec![0, 1])] })
        );
    }

    // TEST1402: more sources than args with NO sequence arg still hard-fails
    // (the historical pigeonhole), and a gather tie between two sequence args
    // is ambiguous, not silently ordered.
    #[test]
    fn test1402_gather_ambiguity_and_scalar_pigeonhole_fail_hard() {
        // Two equally-costed sequence args (each drops a different one of the
        // source's tags → the same absolute specificity gap) → every
        // distribution of the three sources ties → ambiguous.
        let sources = vec![
            media("media:chunk;enc=utf-8;page"),
            media("media:chunk;enc=utf-8;page"),
            media("media:chunk;enc=utf-8;page"),
        ];
        let args = vec![
            media("media:enc=utf-8;page"),
            media("media:chunk;enc=utf-8"),
        ];
        let arg_seq = vec![true, true];
        let err = match_sources_to_args(&sources, &args, &arg_seq, None).unwrap_err();
        assert_eq!(
            err,
            MatchFailure::Ambiguous { undecided: vec![(0, vec![0, 1]), (1, vec![0, 1]), (2, vec![0, 1])] },
            "tied gather distributions must be ambiguous"
        );
    }

    // TEST1403: resolve_pre_interned emits N bindings on ONE sequence arg for a
    // fan-in gather `(x, y) -> concat`, with is_loop=false (the gathered value
    // IS the sequence the arg consumes) and stable source order.
    #[test]
    fn test1403_resolve_gather_emits_n_bindings_on_sequence_arg() {
        let cap_a = build_cap(
            "cap:in=\"media:document;ext=pdf\";op-a;out=\"media:enc=utf-8;page\"",
            "op_a",
            &["media:document;ext=pdf"],
            "media:enc=utf-8;page",
        );
        let cap_b = build_cap(
            "cap:in=\"media:ext=md\";op-b;out=\"media:enc=utf-8;page\"",
            "op_b",
            &["media:ext=md"],
            "media:enc=utf-8;page",
        );
        let mut concat = build_cap(
            "cap:in=\"media:enc=utf-8\";concat;out=\"media:enc=utf-8;ext=txt\"",
            "concat",
            &["media:enc=utf-8"],
            "media:enc=utf-8;ext=txt",
        );
        concat.args[0].is_sequence = true;
        let registry = registry_with(vec![cap_a, cap_b, concat]);

        // nodes: 0=pdf, 1=md, 2=page(from a), 3=page(from b), 4=txt
        let nodes = vec![
            media("media:document;ext=pdf"),
            media("media:ext=md"),
            media("media:enc=utf-8;page"),
            media("media:enc=utf-8;page"),
            media("media:enc=utf-8;ext=txt"),
        ];
        let wirings = vec![
            PreInternedWiring {
                token_id: "tok-a".parse().unwrap(),
                foreach_identity: ForEachIdentity::MintIfRequired,
                cap_urn: CapUrn::from_string(
                    "cap:in=\"media:document;ext=pdf\";op-a;out=\"media:enc=utf-8;page\"",
                )
                .unwrap(),
                source_node_ids: vec![0],
                named_sources: Vec::new(),
                targets: vec![(None, 2)],
                policy: StepPolicy::default(),
                included_as: Vec::new(),
            },
            PreInternedWiring {
                token_id: "tok-b".parse().unwrap(),
                foreach_identity: ForEachIdentity::MintIfRequired,
                cap_urn: CapUrn::from_string(
                    "cap:in=\"media:ext=md\";op-b;out=\"media:enc=utf-8;page\"",
                )
                .unwrap(),
                source_node_ids: vec![1],
                named_sources: Vec::new(),
                targets: vec![(None, 3)],
                policy: StepPolicy::default(),
                included_as: Vec::new(),
            },
            PreInternedWiring {
                token_id: "tok-concat".parse().unwrap(),
                foreach_identity: ForEachIdentity::MintIfRequired,
                cap_urn: CapUrn::from_string(
                    "cap:in=\"media:enc=utf-8\";concat;out=\"media:enc=utf-8;ext=txt\"",
                )
                .unwrap(),
                source_node_ids: vec![2, 3],
                named_sources: Vec::new(),
                targets: vec![(None, 4)],
                policy: StepPolicy::default(),
                included_as: Vec::new(),
            },
        ];

        let strand = resolve_pre_interned(nodes, &wirings, &registry, 0)
            .expect("gather wiring must resolve");
        let concat_edge = strand
            .edges()
            .iter()
            .find(|e| e.cap_urn.to_string().contains("concat"))
            .expect("concat edge present");
        assert_eq!(
            concat_edge.assignment.len(),
            2,
            "the sequence arg gathers BOTH producers as two bindings"
        );
        // Both bindings target the same slot.
        assert!(concat_edge.assignment[0]
            .cap_arg_media_urn
            .is_equivalent(&concat_edge.assignment[1].cap_arg_media_urn)
            .unwrap());
        // Deterministic gather order = source declaration order (node 2 then 3).
        assert_eq!(concat_edge.assignment[0].node(), Some(2));
        assert_eq!(concat_edge.assignment[1].node(), Some(3));
        // The gathered value IS the sequence the arg consumes — no ForEach.
        assert!(
            !concat_edge.is_loop,
            "gather-fed sequence arg must not loop"
        );
        // Anchors: two inputs (pdf, md), one output (txt).
        assert_eq!(strand.input_anchors().len(), 2);
        assert_eq!(strand.output_anchors().len(), 1);
        // Anchors come by the URN they hold, not by when their node was made.
        assert_eq!(strand.input_anchor_ids(), &[0, 1]);
        assert!(strand.node_urn(0) < strand.node_urn(1));
    }

    // TEST1404: a sequence PRODUCER inside a gather resolves — the gather
    // flattens deterministically at execution (each member contributes its
    // item(s) in binding order). This is the batch-plus-single convergence
    // shape: one leg maps a sequence, the other produces one item, and the
    // fold consumes them all as one sequence.
    #[test]
    fn test1404_sequence_producer_in_gather_flattens() {
        let mut cap_a = build_cap(
            "cap:in=\"media:document;ext=pdf\";op-a;out=\"media:enc=utf-8;page\"",
            "op_a",
            &["media:document;ext=pdf"],
            "media:enc=utf-8;page",
        );
        // op-a emits a SEQUENCE of pages.
        cap_a.outputs[0].is_sequence = true;
        let cap_b = build_cap(
            "cap:in=\"media:ext=md\";op-b;out=\"media:enc=utf-8;page\"",
            "op_b",
            &["media:ext=md"],
            "media:enc=utf-8;page",
        );
        let mut concat = build_cap(
            "cap:in=\"media:enc=utf-8\";concat;out=\"media:enc=utf-8;ext=txt\"",
            "concat",
            &["media:enc=utf-8"],
            "media:enc=utf-8;ext=txt",
        );
        concat.args[0].is_sequence = true;
        let registry = registry_with(vec![cap_a, cap_b, concat]);

        let nodes = vec![
            media("media:document;ext=pdf"),
            media("media:ext=md"),
            media("media:enc=utf-8;page"),
            media("media:enc=utf-8;page"),
            media("media:enc=utf-8;ext=txt"),
        ];
        let wirings = vec![
            PreInternedWiring {
                token_id: "tok-a".parse().unwrap(),
                foreach_identity: ForEachIdentity::MintIfRequired,
                cap_urn: CapUrn::from_string(
                    "cap:in=\"media:document;ext=pdf\";op-a;out=\"media:enc=utf-8;page\"",
                )
                .unwrap(),
                source_node_ids: vec![0],
                named_sources: Vec::new(),
                targets: vec![(None, 2)],
                policy: StepPolicy::default(),
                included_as: Vec::new(),
            },
            PreInternedWiring {
                token_id: "tok-b".parse().unwrap(),
                foreach_identity: ForEachIdentity::MintIfRequired,
                cap_urn: CapUrn::from_string(
                    "cap:in=\"media:ext=md\";op-b;out=\"media:enc=utf-8;page\"",
                )
                .unwrap(),
                source_node_ids: vec![1],
                named_sources: Vec::new(),
                targets: vec![(None, 3)],
                policy: StepPolicy::default(),
                included_as: Vec::new(),
            },
            PreInternedWiring {
                token_id: "tok-concat".parse().unwrap(),
                foreach_identity: ForEachIdentity::MintIfRequired,
                cap_urn: CapUrn::from_string(
                    "cap:in=\"media:enc=utf-8\";concat;out=\"media:enc=utf-8;ext=txt\"",
                )
                .unwrap(),
                source_node_ids: vec![2, 3],
                named_sources: Vec::new(),
                targets: vec![(None, 4)],
                policy: StepPolicy::default(),
                included_as: Vec::new(),
            },
        ];

        let strand = resolve_pre_interned(nodes, &wirings, &registry, 3)
            .expect("a gather with a sequence member must resolve (execution flattens)");
        let concat_edge = strand
            .edges()
            .iter()
            .find(|e| e.cap_urn.to_string().contains("concat"))
            .expect("concat edge present");
        assert_eq!(
            concat_edge.assignment.len(),
            2,
            "both members bound to the sequence arg"
        );
        assert!(
            !concat_edge.is_loop,
            "the gathered value IS the sequence the arg consumes — no ForEach"
        );
    }

    // TEST7122: Knitting a strand into a machine preserves the ForEach boundary
    // identity independently from its body-entry cap: the machine is what a run's
    // plan is built from, so an identity lost here is lost to the run.
    #[test]
    fn test7122_foreach_identity_survives_knit() {
        let cap_urn =
            "cap:constrained;in=\"media:enc=utf-8\";language=en;out=\"media:enc=utf-8;ext=txt;plain-text\";summarize";
        let cap = build_cap(
            cap_urn,
            "summarize",
            &["media:enc=utf-8"],
            "media:enc=utf-8;ext=txt;plain-text",
        );
        let registry = registry_with(vec![cap]);
        let mut boundary = for_each_step("media:enc=utf-8;page");
        boundary.token_id = "tok-foreach".parse().unwrap();
        let mut body = cap_step(
            cap_urn,
            "Summarize",
            "media:enc=utf-8;page",
            "media:enc=utf-8;ext=txt;plain-text",
        );
        body.token_id = "tok-cap".parse().unwrap();
        let strand = strand_from_steps(vec![boundary, body], "foreach identity");

        let machine_strand = resolve_strand(&strand, &registry, 0).unwrap();
        assert_eq!(machine_strand.edges()[0].token_id, "tok-cap");
        assert_eq!(
            machine_strand.edges()[0].foreach_token_id.as_deref(),
            Some("tok-foreach")
        );
        assert!(machine_strand.edges()[0].is_loop);
        assert_ne!(
            machine_strand.edges()[0].foreach_token_id.as_deref(),
            Some("tok-cap"),
            "the boundary and its body-entry cap are separate identities"
        );
    }

    // TEST12836: a strand's steps name where each input comes from. A
    // strand's own input, taken by two steps, is one node holding the most
    // specific type either declares for it; a step that names a producer no
    // step of the strand is makes the strand disconnected, and is refused.
    #[test]
    fn test12836_a_strand_input_is_one_node_and_a_missing_producer_is_refused() {
        const LOOSE: &str = r#"cap:in="media:enc=utf-8";count;out="media:count;numeric""#;
        const TIGHT: &str = r#"cap:in="media:enc=utf-8;page";title;out="media:enc=utf-8;title""#;
        let registry = registry_with(vec![
            build_cap(LOOSE, "count", &["media:enc=utf-8"], "media:count;numeric"),
            build_cap(TIGHT, "title", &["media:enc=utf-8;page"], "media:enc=utf-8;title"),
        ]);
        let loose = || cap_step(LOOSE, "count", "media:enc=utf-8", "media:count;numeric");
        let tight = || cap_step(TIGHT, "title", "media:enc=utf-8;page", "media:enc=utf-8;title");
        // The steps as given: each names its own source, and none is chained
        // to the one before it.
        let strand_of = |steps: Vec<StrandStep>| Strand {
            source_media_urn: steps[0].from_spec.clone(),
            target_media_urn: steps[steps.len() - 1].to_spec.clone(),
            total_steps: steps.len() as i32,
            cap_step_count: steps.len() as i32,
            description: "fan-out".to_string(),
            steps,
        };

        // Both steps take the strand's input; one declares it more tightly.
        for steps in [vec![loose(), tight()], vec![tight(), loose()]] {
            let resolved = resolve_strand(&strand_of(steps), &registry, 0).unwrap();
            assert_eq!(resolved.nodes().len(), 3, "the input once, and what each step makes");
            assert_eq!(resolved.input_anchor_ids().len(), 1);
            let anchor = resolved.input_anchor_ids()[0];
            assert!(
                resolved.node_urn(anchor).is_equivalent(&media("media:enc=utf-8;page")).unwrap(),
                "the most specific type a step declares for it, whichever step comes first"
            );
            for edge in resolved.edges() {
                assert_eq!(edge.source_nodes().collect::<Vec<_>>(), vec![anchor]);
            }
        }

        // A producer that is no step of the strand.
        let mut orphan = tight();
        if let StrandStepType::Cap { inputs, .. } = &mut orphan.step_type {
            inputs[0].source = ArgSourceRef::Step {
                token_id: "no-such-step".parse().unwrap(),
            };
        }
        match resolve_strand(&strand_of(vec![loose(), orphan]), &registry, 4) {
            Err(MachineAbstractionError::DisconnectedStrand { strand_index }) => {
                assert_eq!(strand_index, 4)
            }
            other => panic!("expected a disconnected strand, got {:?}", other.map(|_| ())),
        }
    }

    // TEST12837: the resolver holds a wiring it is handed to the names its
    // cap's definition declares, whoever built the wiring: a member or a
    // target naming what the cap does not declare is refused by name, with
    // what it does declare.
    #[test]
    fn test12837_the_resolver_refuses_names_a_definition_does_not_declare() {
        const CAP: &str = r#"cap:in="media:document;ext=pdf";extract;out="media:enc=utf-8;ext=txt""#;
        let registry = registry_with(vec![build_cap(
            CAP,
            "extract",
            &["media:document;ext=pdf"],
            "media:enc=utf-8;ext=txt",
        )]);
        let nodes = || vec![media("media:document;ext=pdf"), media("media:enc=utf-8;ext=txt")];
        let wiring = |arg: &str, target: (Option<String>, NodeId)| {
            vec![PreInternedWiring {
                token_id: StepToken::mint(),
                foreach_identity: ForEachIdentity::MintIfRequired,
                cap_urn: CapUrn::from_string(CAP).unwrap(),
                source_node_ids: Vec::new(),
                named_sources: vec![(arg.to_string(), BindingSource::Node(0))],
                targets: vec![target],
                policy: StepPolicy::default(),
                included_as: Vec::new(),
            }]
        };

        // By the names the definition declares, it resolves.
        let input = registry
            .get_cached_cap(CAP)
            .map(|cap| (cap.args[0].name.clone(), cap.outputs[0].name.clone()))
            .expect("the fixture is in the registry");
        let resolved =
            resolve_pre_interned(nodes(), &wiring(&input.0, (Some(input.1.clone()), 1)), &registry, 0)
                .unwrap();
        assert_eq!(resolved.edges()[0].main_target(), Some(1));

        match resolve_pre_interned(nodes(), &wiring("document", (None, 1)), &registry, 2) {
            Err(error @ MachineAbstractionError::UnknownArgName { .. }) => {
                let said = error.to_string();
                assert!(said.starts_with("strand 2: "), "{said}");
                assert!(
                    said.contains(&format!("declares no arg named 'document' (it declares: {})", input.0)),
                    "{said}"
                );
            }
            other => panic!("expected an unknown arg name, got {:?}", other.map(|_| ())),
        }
        match resolve_pre_interned(
            nodes(),
            &wiring(&input.0, (Some("text".to_string()), 1)),
            &registry,
            2,
        ) {
            Err(error @ MachineAbstractionError::UnknownOutputName { .. }) => {
                let said = error.to_string();
                assert!(
                    said.contains(&format!("declares no output named 'text' (it declares: {})", input.1)),
                    "{said}"
                );
            }
            other => panic!("expected an unknown output name, got {:?}", other.map(|_| ())),
        }
    }
}
