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
//!   `|spec(s) - spec(a)|` if `s.conforms_to(a)` — the absolute
//!   specificity gap (a wildcard-carrying source such as the
//!   planner's join `media:ext` conforms to a value-exact arg
//!   while being LESS specific; both directions of mismatch cost).
//!   If `s` does not conform to `a`, the pair is impossible.
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

use super::error::MachineAbstractionError;
use super::graph::{EdgeAssignmentBinding, MachineEdge, MachineStrand, NodeId};

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
    /// Source NodeIds in the order the upstream layer wrote
    /// them. Position carries no semantics — the matching
    /// algorithm assigns each source slot to a cap arg by
    /// minimum-cost bipartite matching, not by index.
    pub source_node_ids: Vec<NodeId>,
    /// Target NodeId.
    pub target_node_id: NodeId,
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
                cap_urn, inputs, ..
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
                    source_node_ids,
                    target_node_id: target_id,
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
    if wirings.is_empty() {
        return Err(MachineAbstractionError::NoCapabilitySteps);
    }

    // Cardinality of every node, derived from the single canonical rule
    // (`Cap::sequence_shape`): a node holds a sequence iff the cap that PRODUCES it
    // has a sequence output. Root-node shape is supplied by an exact explicit
    // ForEach boundary when the strand was resolved for a sequence run; notation
    // has no run shape and therefore treats roots as scalar. This is
    // exactly the `is_sequence` state `live_cap_fab::get_outgoing_edges` threads
    // through a path (its line 706: outgoing `is_sequence == output_is_sequence`),
    // evaluated over the resolved graph so `is_loop` is DERIVED from cardinality,
    // never authored. It replaces the retired `LOOP` keyword.
    let mut node_is_sequence = vec![false; nodes.len()];
    for wiring in wirings {
        let cap = registry
            .get_cached_cap(&wiring.cap_urn.to_string())
            .ok_or_else(|| MachineAbstractionError::UnknownCap {
                cap_urn: wiring.cap_urn.to_string(),
            })?;

        let (_input_is_sequence, output_is_sequence) = cap.sequence_shape();
        node_is_sequence[wiring.target_node_id as usize] = output_is_sequence;
    }

    // Step 1: per-wiring source-to-cap-arg matching. The
    // matching is computed against the URNs of the source
    // NodeIds (looked up from the `nodes` table); the result
    // is a sorted assignment of cap-arg → NodeId pairs.
    let mut indexed_edges: Vec<MachineEdge> = Vec::with_capacity(wirings.len());
    for wiring in wirings {
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

        // Build the list of data-flow input slots for this cap.
        //
        // Each cap arg may declare any of three sources:
        //   - `Stdin { stdin: <media URN> }` — runtime delivers
        //     the named-typed data to the arg slot via the
        //     bifaci stdin stream. THIS is the data-flow input.
        //   - `Position { ... }` — positional CLI argument.
        //   - `CliFlag { ... }` — named CLI flag.
        //
        // Args with NO stdin source are CLI / positional config
        // only — they receive their values at execution time
        // from cap_settings, slot_values, or default_value.
        // They are never matched against a wiring's source
        // URNs.
        //
        // For args that DO have a stdin source, the URN that
        // matters for matching is the stdin source's inner
        // type (e.g. `media:ext=png;image`), NOT the arg's outer
        // `media_urn` (e.g. `media:enc=utf-8;file-path`). The
        // outer is the slot identity that cartridge_runtime uses
        // to label the stream and to drive file-path
        // auto-conversion; the inner is the type the runtime
        // actually delivers into the slot. The resolver
        // matches against the inner type because that is what
        // upstream caps actually produce.
        //
        // We build two parallel vecs: `stdin_arg_urns` (the
        // URNs to match against, in the order they appear in
        // `cap.args`) and `stdin_arg_slot_urns` (the
        // corresponding slot identities the bindings will
        // record).
        // Wiring sources are matched against EVERY arg by its stream URN (its Stdin
        // source URN if it declares one, else its declared URN — a cap may have no
        // stdin at all). ALL args are producer-feedable: a producer is anything that
        // supplies a value (a prior cap's output, config, a literal, …), so every arg
        // is a candidate. The Hungarian matcher assigns each source to the arg it
        // conforms to; args no source matches take their value from config/defaults;
        // genuine ambiguity is a hard failure (in `match_sources_to_args`). `stdin_*`
        // names are historical — these are the per-arg stream and slot URNs.
        let mut stdin_arg_urns: Vec<MediaUrn> = Vec::new();
        let mut stdin_arg_slot_urns: Vec<MediaUrn> = Vec::new();
        let mut stdin_arg_is_sequence: Vec<bool> = Vec::new();
        for arg in &cap.args {
            let stream_urn = MediaUrn::from_string(arg.stream_urn())
                .expect("cap registry invariant: every arg stream URN is a valid MediaUrn");
            let slot_urn = MediaUrn::from_string(&arg.media_urn)
                .expect("cap registry invariant: every cap arg media_urn is a valid MediaUrn");
            stdin_arg_urns.push(stream_urn);
            stdin_arg_slot_urns.push(slot_urn);
            stdin_arg_is_sequence.push(arg.is_sequence);
        }

        // Pull the source URNs out of the nodes table for
        // this wiring's source NodeIds.
        let source_urns: Vec<MediaUrn> = wiring
            .source_node_ids
            .iter()
            .map(|id| nodes[*id as usize].clone())
            .collect();

        // Run the bipartite minimum-cost matching against
        // the stdin URNs. The matching returns
        // `(matched_arg_urn, source_urn)` pairs where
        // `matched_arg_urn` is the stdin URN that the source
        // was assigned to. We then translate each matched
        // stdin URN back to its slot identity for the binding.
        let sorted_assignment = match_sources_to_args(
            &source_urns,
            &stdin_arg_urns,
            &stdin_arg_is_sequence,
            &wiring.cap_urn,
            strand_index,
        )?;

        // Build the bindings. The `cap_arg_media_urn` field
        // on each binding records the **slot identity**
        // (the cap arg's outer `media_urn`), since that is
        // the canonical identifier per RULE1. We look up the
        // slot identity by matching the assignment's stdin
        // URN back to the position in `stdin_arg_urns`.
        //
        // We also map each source URN back to its NodeId
        // position in `wiring.source_node_ids`, walking the
        // unconsumed positions to handle the case where two
        // source NodeIds happen to share a URN.
        let mut bindings: Vec<EdgeAssignmentBinding> = Vec::with_capacity(sorted_assignment.len());
        let mut consumed_positions: Vec<bool> = vec![false; wiring.source_node_ids.len()];
        for (matched_stdin_urn, source_urn) in &sorted_assignment {
            // Find the slot identity for this matched stdin URN.
            let slot_urn = stdin_arg_urns
                .iter()
                .zip(stdin_arg_slot_urns.iter())
                .find(|(stdin, _)| stdin.is_equivalent(matched_stdin_urn).unwrap_or(false))
                .map(|(_, slot)| slot.clone())
                .expect("matching returned a stdin URN that isn't in the cap's stdin args list");

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
            bindings.push(EdgeAssignmentBinding {
                cap_arg_media_urn: slot_urn,
                source: wiring.source_node_ids[pos],
            });
        }

        // The bindings vec is currently in the order produced by
        // `sorted_assignment` (sorted by stdin URN, source-order-stable within
        // one arg). To keep the canonical equivalence comparison stable,
        // re-sort by slot identity (`cap_arg_media_urn`); the sort is stable,
        // so several bindings gathered onto one arg keep their source
        // declaration order — the gathered sequence's item order.
        bindings.sort_by(|a, b| a.cap_arg_media_urn.cmp(&b.cap_arg_media_urn));

        // A slot with several bindings is the implicit Collect: the matcher only
        // produces it for a sequence arg. Members may be scalar or sequence —
        // execution flattens deterministically: each member contributes its
        // item(s) to the gathered sequence in binding (source-declaration)
        // order (a scalar member contributes one item, a sequence member its
        // items). No further validation is needed here.

        // Derive `is_loop` from cardinality — the single ForEach rule
        // (`Cap::needs_foreach`, mirroring `get_outgoing_edges` line 673): the
        // primary data input (the arg whose stdin matches `in=`) carries a sequence but this cap
        // consumes it as a scalar, so it maps per-item. The primary stdin source node
        // is the binding feeding the main arg's slot; a GATHER on the primary
        // arg (several bindings) forms a sequence by construction. A cap with no
        // stdin arg (config-only) never loops.
        let primary_stdin_sources: Vec<&EdgeAssignmentBinding> = primary_arg_index
            .map(|index| {
                let primary_slot = &stdin_arg_slot_urns[index];
                bindings
                    .iter()
                    .filter(|binding| {
                        binding
                            .cap_arg_media_urn
                            .is_equivalent(primary_slot)
                            .unwrap_or(false)
                    })
                    .collect()
            })
            .unwrap_or_default();
        let primary_stdin_source_is_sequence = match primary_stdin_sources.as_slice() {
            [] => false,
            [primary] => node_is_sequence[primary.source as usize],
            // Gathered: N scalar producers form one sequence.
            _ => true,
        };
        let primary_stdin_source_is_anchor = matches!(
            primary_stdin_sources.as_slice(),
            [primary]
                if !wirings
                    .iter()
                    .any(|candidate| candidate.target_node_id == primary.source)
        );
        let derived_is_loop = cap.needs_foreach(primary_stdin_source_is_sequence);

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
            target: wiring.target_node_id,
            is_loop,
        });
    }

    // Step 2: cycle detection + canonical edge order.
    //
    // The data-flow dependency relation: edge B depends on
    // edge A iff some binding in B's assignment has
    // `source == A.target` (NodeId equality).
    let canonical_order = topo_sort(&indexed_edges, &nodes, strand_index)?;
    let edges: Vec<MachineEdge> = canonical_order
        .into_iter()
        .map(|i| indexed_edges[i].clone())
        .collect();

    // Step 3: anchor computation.
    let mut produced_node_ids: std::collections::HashSet<NodeId> = Default::default();
    let mut consumed_node_ids: std::collections::HashSet<NodeId> = Default::default();
    for e in &edges {
        produced_node_ids.insert(e.target);
        for b in &e.assignment {
            consumed_node_ids.insert(b.source);
        }
    }

    let mut input_anchor_ids: Vec<NodeId> = (0..nodes.len() as NodeId)
        .filter(|id| !produced_node_ids.contains(id) && consumed_node_ids.contains(id))
        .collect();
    let mut output_anchor_ids: Vec<NodeId> = (0..nodes.len() as NodeId)
        .filter(|id| !consumed_node_ids.contains(id) && produced_node_ids.contains(id))
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
    ))
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
/// sequence's item order deterministically). Returns errors when:
///
/// - A source has no candidate arg, or no valid assignment exists under the
///   capacity rules (`UnmatchedSourceInCapArgs`).
/// - The minimum-cost assignment of the winning phase is not unique
///   (`AmbiguousMachineNotation`).
fn match_sources_to_args(
    sources: &[MediaUrn],
    args: &[MediaUrn],
    arg_is_sequence: &[bool],
    cap_urn: &CapUrn,
    strand_index: usize,
) -> Result<Vec<(MediaUrn, MediaUrn)>, MachineAbstractionError> {
    debug_assert_eq!(args.len(), arg_is_sequence.len());
    let has_sequence_arg = arg_is_sequence.iter().any(|s| *s);

    if sources.len() > args.len() && !has_sequence_arg {
        // Pigeonhole: at least one source has no arg slot and no
        // sequence arg exists to gather the surplus. Find the first
        // source with no candidate arg and report it. (If all
        // sources DO conform to some arg, we still can't match —
        // but that's still a structural unmatched-source condition.)
        for source in sources {
            if !args.iter().any(|a| source.conforms_to(a).unwrap_or(false)) {
                return Err(MachineAbstractionError::UnmatchedSourceInCapArgs {
                    strand_index,
                    cap_urn: cap_urn.to_string(),
                    source_urn: source.to_string(),
                });
            }
        }
        // All sources have a candidate, but there are more
        // sources than args — at least one source MUST end up
        // unmatched. Treat the first source as unmatched.
        return Err(MachineAbstractionError::UnmatchedSourceInCapArgs {
            strand_index,
            cap_urn: cap_urn.to_string(),
            source_urn: sources[0].to_string(),
        });
    }

    // Build the candidate matrix. cost[s][a] is Some(distance)
    // if `sources[s]` conforms to `args[a]`, else None.
    let n_sources = sources.len();
    let n_args = args.len();
    let mut cost: Vec<Vec<Option<i64>>> = vec![vec![None; n_args]; n_sources];

    for (s_idx, source) in sources.iter().enumerate() {
        for (a_idx, arg) in args.iter().enumerate() {
            if source.conforms_to(arg).unwrap_or(false) {
                // Absolute specificity gap. A source is USUALLY at least as
                // specific as the arg it conforms to, but a wildcard-carrying
                // source (e.g. the planner's join `media:ext`) conforms to a
                // value-exact arg (`media:ext=md`) while being LESS specific —
                // runtime narrowing closes the gap. Either direction of
                // mismatch is a worse fit than an exact one, so both cost.
                let distance = source.specificity().abs_diff(arg.specificity()) as i64;
                cost[s_idx][a_idx] = Some(distance);
            }
        }
        // Per-source: at least one candidate, else unmatched.
        if cost[s_idx].iter().all(|c| c.is_none()) {
            return Err(MachineAbstractionError::UnmatchedSourceInCapArgs {
                strand_index,
                cap_urn: cap_urn.to_string(),
                source_urn: source.to_string(),
            });
        }
    }

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
            if best_assignments.len() != 1 {
                return Err(MachineAbstractionError::AmbiguousMachineNotation {
                    strand_index,
                    cap_urn: cap_urn.to_string(),
                });
            }
            return Ok(assignment_to_pairs(&best_assignments[0], sources, args));
        }
    }

    // ── Phase 2: gather — no injection exists. Sequence args may absorb
    // several sources; scalar args still take at most one. ──
    if !has_sequence_arg {
        // No injection and nothing can gather: every per-source candidate
        // set is non-empty, but the candidate sets collectively can't all
        // be claimed by distinct args (Hall's theorem violation). Pick the
        // first source as the canonical "unmatched" representative.
        return Err(MachineAbstractionError::UnmatchedSourceInCapArgs {
            strand_index,
            cap_urn: cap_urn.to_string(),
            source_urn: sources[0].to_string(),
        });
    }

    let mut current: Vec<usize> = vec![usize::MAX; n_sources];
    let mut used: Vec<bool> = vec![false; n_args];
    enumerate_assignments(
        &cost,
        arg_is_sequence,
        0,
        &mut current,
        &mut used,
        &mut best_cost,
        &mut best_assignments,
    );

    if best_cost.is_none() {
        return Err(MachineAbstractionError::UnmatchedSourceInCapArgs {
            strand_index,
            cap_urn: cap_urn.to_string(),
            source_urn: sources[0].to_string(),
        });
    }
    if best_assignments.len() != 1 {
        return Err(MachineAbstractionError::AmbiguousMachineNotation {
            strand_index,
            cap_urn: cap_urn.to_string(),
        });
    }
    Ok(assignment_to_pairs(&best_assignments[0], sources, args))
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
    s_idx: usize,
    current: &mut Vec<usize>,
    used: &mut Vec<bool>,
    best_cost: &mut Option<i64>,
    best_assignments: &mut Vec<Vec<usize>>,
) {
    let n_sources = cost.len();
    if s_idx == n_sources {
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
/// Edge B depends on edge A iff some binding in B.assignment
/// has `source == A.target` (NodeId equality, since interning
/// has already collapsed equivalent URNs).
fn topo_sort(
    edges: &[MachineEdge],
    nodes: &[MediaUrn],
    strand_index: usize,
) -> Result<Vec<usize>, MachineAbstractionError> {
    let n = edges.len();
    if n == 0 {
        return Ok(Vec::new());
    }

    // Map: NodeId → list of edge indices that produce this
    // NodeId as their target. (In a well-formed strand at most
    // one edge produces a given target node, but we don't
    // assume that — multiple producers would mean non-
    // deterministic data flow at runtime, which is itself a
    // structural error worth being permissive about and
    // letting the cycle / unmatched checks catch.)
    let mut producers_of: HashMap<NodeId, Vec<usize>> = HashMap::new();
    for (idx, e) in edges.iter().enumerate() {
        producers_of.entry(e.target).or_default().push(idx);
    }

    // Edge B's predecessors: any edge whose target is the
    // source of any binding in B.assignment.
    //
    // A self-dependency — an edge whose own target is one of its
    // own source nodes (`a_idx == b_idx`) — is a structural cycle
    // by definition. Historically this loop skipped those pairs;
    // that silently let self-loops like `[A -> cap -> A]` through
    // the DAG check. We now record the self-edge so it contributes
    // to its own indegree, which guarantees `topo_sort` fails for
    // any self-loop.
    let mut indegree: Vec<usize> = vec![0; n];
    let mut successors: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (b_idx, b) in edges.iter().enumerate() {
        for binding in &b.assignment {
            if let Some(producers) = producers_of.get(&binding.source) {
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

    if result.len() < n {
        return Err(MachineAbstractionError::CyclicMachineStrand { strand_index });
    }

    Ok(result)
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
            match nodes[ba.source as usize].cmp(&nodes[bb.source as usize]) {
                std::cmp::Ordering::Equal => {}
                ord => return ord,
            }
        }
        match ea.assignment.len().cmp(&eb.assignment.len()) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        match nodes[ea.target as usize].cmp(&nodes[eb.target as usize]) {
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
        match_sources_to_args, resolve_pre_interned, resolve_strand, ForEachIdentity,
        PreInternedWiring,
    };
    use crate::machine::error::MachineAbstractionError;
    use crate::machine::test_fixtures::{
        build_cap, build_cap_with_slot_stdin_pairs, cap, cap_step, collect_step, for_each_step,
        media, registry_with, strand_from_steps,
    };
    use crate::urn::cap_urn::CapUrn;

    // ----- match_sources_to_args -------------------------------------------

    // TEST1178: One source is assigned to the single compatible cap argument.
    #[test]
    fn test1178_match_single_source_picks_unique_arg() {
        // Single source `media:ext=pdf` against a one-arg cap. Trivial
        // bipartite matching: one source, one arg, exact tag-set
        // equivalence → distance 0 → unique → assignment is the
        // single pair.
        let sources = vec![media("media:ext=pdf")];
        let args = vec![media("media:ext=pdf")];
        let cap_urn = cap("cap:in=\"media:ext=pdf\";extract;out=\"media:enc=utf-8;ext=txt\"");
        let pairs = match_sources_to_args(&sources, &args, &vec![false; args.len()], &cap_urn, 0)
            .expect("trivial single-source match must succeed");
        assert_eq!(pairs.len(), 1);
        assert!(pairs[0].0.is_equivalent(&media("media:ext=pdf")).unwrap());
        assert!(pairs[0].1.is_equivalent(&media("media:ext=pdf")).unwrap());
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
        let cap_urn =
            cap("cap:in=\"media:enc=utf-8\";make-decision;out=\"media:decision;enc=utf-8\"");
        let pairs = match_sources_to_args(&sources, &args, &vec![false; args.len()], &cap_urn, 0)
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
        let cap_urn = cap("cap:in=\"media:enc=utf-8\";t;out=\"media:enc=utf-8\"");
        let err = match_sources_to_args(&sources, &args, &vec![false; args.len()], &cap_urn, 7)
            .unwrap_err();
        match err {
            MachineAbstractionError::UnmatchedSourceInCapArgs {
                strand_index,
                cap_urn: cu,
                source_urn,
            } => {
                assert_eq!(strand_index, 7);
                assert!(cu.contains("make_decision") || cu.contains("t"));
                assert_eq!(source_urn, "media:numeric");
            }
            other => panic!("expected UnmatchedSourceInCapArgs, got {:?}", other),
        }
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
        let cap_urn = cap(
            "cap:in=\"media:ext=png;image\";describe;out=\"media:enc=utf-8;image-description\"",
        );
        let pairs =
            match_sources_to_args(&sources, &args, &vec![false; args.len()], &cap_urn, 0).unwrap();
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
        let cap_urn = cap("cap:in=\"media:enc=utf-8\";t;out=\"media:enc=utf-8\"");
        let err = match_sources_to_args(&sources, &args, &vec![false; args.len()], &cap_urn, 0)
            .unwrap_err();
        assert!(
            matches!(
                err,
                MachineAbstractionError::AmbiguousMachineNotation { .. }
            ),
            "expected ambiguous, got {:?}",
            err
        );
    }

    // TEST1183: Matching fails when more sources are provided than the cap has input arguments.
    #[test]
    fn test1183_match_more_sources_than_args_fails_hard() {
        let sources = vec![
            media("media:ext=pdf"),
            media("media:ext=pdf"),
            media("media:ext=pdf"),
        ];
        let args = vec![media("media:ext=pdf"), media("media:ext=pdf")];
        let cap_urn = cap("cap:in=\"media:ext=pdf\";t;out=\"media:ext=pdf\"");
        let err = match_sources_to_args(&sources, &args, &vec![false; args.len()], &cap_urn, 0)
            .unwrap_err();
        assert!(matches!(
            err,
            MachineAbstractionError::UnmatchedSourceInCapArgs { .. }
        ));
    }

    // ----- resolve_strand (planner path) -----------------------------------

    // TEST1184: Resolving a strand with one cap produces one resolved machine edge.
    #[test]
    fn test1184_resolve_strand_single_cap_produces_one_edge() {
        let extract_cap = build_cap(
            "cap:in=\"media:ext=pdf\";extract;out=\"media:enc=utf-8;ext=txt\"",
            "extract",
            &["media:ext=pdf"],
            "media:enc=utf-8;ext=txt",
        );
        let registry = registry_with(vec![extract_cap]);
        let strand = strand_from_steps(
            vec![cap_step(
                "cap:in=\"media:ext=pdf\";extract;out=\"media:enc=utf-8;ext=txt\"",
                "extract",
                "media:ext=pdf",
                "media:enc=utf-8;ext=txt",
            )],
            "pdf to txt",
        );
        let resolved = resolve_strand(&strand, &registry, 0).expect("must resolve");
        assert_eq!(resolved.edges().len(), 1);
        assert_eq!(resolved.edges()[0].assignment.len(), 1);
        // The single edge's assignment maps the cap arg
        // media:ext=pdf to a node whose URN is media:pdf.
        let binding = &resolved.edges()[0].assignment[0];
        assert!(binding
            .cap_arg_media_urn
            .is_equivalent(&media("media:ext=pdf"))
            .unwrap());
        let src_urn = resolved.node_urn(binding.source);
        assert!(src_urn.is_equivalent(&media("media:ext=pdf")).unwrap());
        // Anchors: input is media:ext=pdf, output is media:enc=utf-8;ext=txt.
        let inputs = resolved.input_anchors();
        let outputs = resolved.output_anchors();
        assert_eq!(inputs.len(), 1);
        assert_eq!(outputs.len(), 1);
        assert!(inputs[0].is_equivalent(&media("media:ext=pdf")).unwrap());
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
            "cap:in=\"media:ext=pdf\";extract;out=\"media:enc=utf-8;ext=txt\"",
            "extract",
            &["media:ext=pdf"],
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
                    "cap:in=\"media:ext=pdf\";extract;out=\"media:enc=utf-8;ext=txt\"",
                    "extract",
                    "media:ext=pdf",
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
        let extract_target = resolved.edges()[0].target;
        let embed_source = resolved.edges()[1].assignment[0].source;
        assert_eq!(
            extract_target, embed_source,
            "intermediate data position must be one shared NodeId"
        );

        // Anchors.
        let inputs = resolved.input_anchors();
        let outputs = resolved.output_anchors();
        assert_eq!(inputs.len(), 1);
        assert_eq!(outputs.len(), 1);
        assert!(inputs[0].is_equivalent(&media("media:ext=pdf")).unwrap());
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
            "cap:in=\"media:ext=pdf\";disbind;out=\"media:enc=utf-8;page\"",
            "disbind",
            &["media:ext=pdf"],
            "media:enc=utf-8;page",
        );
        disbind.output.as_mut().unwrap().is_sequence = true;
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
                    "cap:in=\"media:ext=pdf\";disbind;out=\"media:enc=utf-8;page\"",
                    "disbind",
                    "media:ext=pdf",
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
        let disbind_target = disbind_edge.target;
        let decision_source = decision_edge.assignment[0].source;
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
                "cap:in=\"media:ext=pdf\";extract;out=\"media:enc=utf-8;ext=txt\"",
                "extract",
                "media:ext=pdf",
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
                for_each_step("media:ext=pdf"),
                collect_step("media:ext=pdf"),
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
            "cap:in=\"media:ext=pdf\";extract;out=\"media:enc=utf-8;ext=txt\"",
            "extract",
            &["media:ext=pdf"],
            "media:enc=utf-8;ext=txt",
        );
        let registry = registry_with(vec![extract]);
        let strand = strand_from_steps(
            vec![cap_step(
                "cap:in=\"media:ext=pdf\";extract;out=\"media:enc=utf-8;ext=txt\"",
                "extract",
                "media:ext=pdf",
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
        let int_target = resolved.edges()[0].target;
        let num_source = resolved.edges()[1].assignment[0].source;
        assert_eq!(int_target, num_source);
    }

    // TEST1191: Disbinding a PDF with a file-path slot preserves the expected identity of the slot binding.
    #[test]
    fn test1191_resolve_strand_disbind_pdf_with_file_path_slot_identity() {
        // Regression: a cap whose arg slot identity differs
        // from its stdin source URN. The disbind cap declares
        // `media:enc=utf-8;file-path` as the slot identity but
        // its stdin source delivers `media:ext=pdf` (this is the
        // wire-level wraparound: cartridge_runtime auto-converts
        // a file-path argument into a stdin byte stream of
        // the inner type).
        //
        // The resolver MUST match the wiring's `media:ext=pdf`
        // source against the stdin URN of the arg, NOT against
        // the slot identity. Before this fix the resolver
        // would have returned `UnmatchedSourceInCapArgs`
        // because `media:ext=pdf` does not conform to
        // `media:enc=utf-8;file-path`.
        let disbind = build_cap_with_slot_stdin_pairs(
            "cap:in=\"media:ext=pdf\";disbind;out=\"media:enc=utf-8;page\"",
            "disbind",
            &[("media:enc=utf-8;file-path", "media:ext=pdf")],
            "media:enc=utf-8;page",
        );
        let registry = registry_with(vec![disbind]);

        let strand = strand_from_steps(
            vec![cap_step(
                "cap:in=\"media:ext=pdf\";disbind;out=\"media:enc=utf-8;page\"",
                "disbind",
                "media:ext=pdf",
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
        // `media:ext=pdf` — the data-type URN, what the planner
        // sees flowing on the wire.
        let source_urn = resolved.node_urn(binding.source);
        assert!(
            source_urn.is_equivalent(&media("media:ext=pdf")).unwrap(),
            "source node URN must be media:ext=pdf (the data-type URN), got: {}",
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
            "cap:in=\"media:ext=pdf\";merge;out=\"media:enc=utf-8;ext=txt\"",
            "merge",
            &["media:enc=utf-8", "media:ext=pdf"],
            "media:enc=utf-8;ext=txt",
        );
        let registry = registry_with(vec![merge_cap]);

        // Pre-interned nodes: 0=pdf, 1=enc=utf-8, 2=enc=utf-8;ext=txt (output)
        let nodes = vec![
            media("media:ext=pdf"),
            media("media:enc=utf-8"),
            media("media:enc=utf-8;ext=txt"),
        ];
        let cap_urn =
            CapUrn::from_string("cap:in=\"media:ext=pdf\";merge;out=\"media:enc=utf-8;ext=txt\"")
                .unwrap();
        let wirings = vec![PreInternedWiring {
            token_id: "tok-1".parse().unwrap(),
            foreach_identity: ForEachIdentity::MintIfRequired,
            cap_urn,
            source_node_ids: vec![0, 1], // pdf first, enc=utf-8 second
            target_node_id: 2,
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
        let urn_a = "cap:in=\"media:ext=pdf\";op-a;out=\"media:enc=utf-8;ext=txt\"";
        let urn_b = "cap:in=\"media:enc=utf-8;ext=txt\";op-b;out=\"media:ext=pdf\"";
        let cap_a = build_cap(urn_a, "op_a", &["media:ext=pdf"], "media:enc=utf-8;ext=txt");
        let cap_b = build_cap(urn_b, "op_b", &["media:enc=utf-8;ext=txt"], "media:ext=pdf");
        let registry = registry_with(vec![cap_a, cap_b]);

        let nodes = vec![media("media:ext=pdf"), media("media:enc=utf-8;ext=txt")];
        // node 0 -> cap_a -> node 1  and  node 1 -> cap_b -> node 0 (cycle)
        let wirings = vec![
            PreInternedWiring {
                token_id: "tok-2".parse().unwrap(),
                foreach_identity: ForEachIdentity::MintIfRequired,
                cap_urn: CapUrn::from_string(urn_a).unwrap(),
                source_node_ids: vec![0],
                target_node_id: 1,
            },
            PreInternedWiring {
                token_id: "tok-3".parse().unwrap(),
                foreach_identity: ForEachIdentity::MintIfRequired,
                cap_urn: CapUrn::from_string(urn_b).unwrap(),
                source_node_ids: vec![1],
                target_node_id: 0,
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
        let cap_urn = cap("cap:in=\"media:enc=utf-8\";concat;out=\"media:enc=utf-8;ext=txt\"");
        let pairs = match_sources_to_args(&sources, &args, &arg_seq, &cap_urn, 0)
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
        let cap_urn =
            cap("cap:in=\"media:enc=utf-8;page\";compose;out=\"media:enc=utf-8;ext=txt\"");
        let pairs = match_sources_to_args(&sources, &args, &arg_seq, &cap_urn, 0)
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
        let cap_urn = cap("cap:in=\"media:enc=utf-8\";t;out=\"media:enc=utf-8\"");
        let err = match_sources_to_args(&sources, &args, &arg_seq, &cap_urn, 0).unwrap_err();
        assert!(
            matches!(
                err,
                MachineAbstractionError::AmbiguousMachineNotation { .. }
            ),
            "tied gather distributions must be ambiguous, got {err:?}"
        );
    }

    // TEST1403: resolve_pre_interned emits N bindings on ONE sequence arg for a
    // fan-in gather `(x, y) -> concat`, with is_loop=false (the gathered value
    // IS the sequence the arg consumes) and stable source order.
    #[test]
    fn test1403_resolve_gather_emits_n_bindings_on_sequence_arg() {
        let cap_a = build_cap(
            "cap:in=\"media:ext=pdf\";op-a;out=\"media:enc=utf-8;page\"",
            "op_a",
            &["media:ext=pdf"],
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
            media("media:ext=pdf"),
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
                    "cap:in=\"media:ext=pdf\";op-a;out=\"media:enc=utf-8;page\"",
                )
                .unwrap(),
                source_node_ids: vec![0],
                target_node_id: 2,
            },
            PreInternedWiring {
                token_id: "tok-b".parse().unwrap(),
                foreach_identity: ForEachIdentity::MintIfRequired,
                cap_urn: CapUrn::from_string(
                    "cap:in=\"media:ext=md\";op-b;out=\"media:enc=utf-8;page\"",
                )
                .unwrap(),
                source_node_ids: vec![1],
                target_node_id: 3,
            },
            PreInternedWiring {
                token_id: "tok-concat".parse().unwrap(),
                foreach_identity: ForEachIdentity::MintIfRequired,
                cap_urn: CapUrn::from_string(
                    "cap:in=\"media:enc=utf-8\";concat;out=\"media:enc=utf-8;ext=txt\"",
                )
                .unwrap(),
                source_node_ids: vec![2, 3],
                target_node_id: 4,
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
        assert_eq!(concat_edge.assignment[0].source, 2);
        assert_eq!(concat_edge.assignment[1].source, 3);
        // The gathered value IS the sequence the arg consumes — no ForEach.
        assert!(
            !concat_edge.is_loop,
            "gather-fed sequence arg must not loop"
        );
        // Anchors: two inputs (pdf, md), one output (txt).
        assert_eq!(strand.input_anchors().len(), 2);
        assert_eq!(strand.output_anchors().len(), 1);
    }

    // TEST1404: a sequence PRODUCER inside a gather resolves — the gather
    // flattens deterministically at execution (each member contributes its
    // item(s) in binding order). This is the batch-plus-single convergence
    // shape: one leg maps a sequence, the other produces one item, and the
    // fold consumes them all as one sequence.
    #[test]
    fn test1404_sequence_producer_in_gather_flattens() {
        let mut cap_a = build_cap(
            "cap:in=\"media:ext=pdf\";op-a;out=\"media:enc=utf-8;page\"",
            "op_a",
            &["media:ext=pdf"],
            "media:enc=utf-8;page",
        );
        // op-a emits a SEQUENCE of pages.
        cap_a.output.as_mut().unwrap().is_sequence = true;
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
            media("media:ext=pdf"),
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
                    "cap:in=\"media:ext=pdf\";op-a;out=\"media:enc=utf-8;page\"",
                )
                .unwrap(),
                source_node_ids: vec![0],
                target_node_id: 2,
            },
            PreInternedWiring {
                token_id: "tok-b".parse().unwrap(),
                foreach_identity: ForEachIdentity::MintIfRequired,
                cap_urn: CapUrn::from_string(
                    "cap:in=\"media:ext=md\";op-b;out=\"media:enc=utf-8;page\"",
                )
                .unwrap(),
                source_node_ids: vec![1],
                target_node_id: 3,
            },
            PreInternedWiring {
                token_id: "tok-concat".parse().unwrap(),
                foreach_identity: ForEachIdentity::MintIfRequired,
                cap_urn: CapUrn::from_string(
                    "cap:in=\"media:enc=utf-8\";concat;out=\"media:enc=utf-8;ext=txt\"",
                )
                .unwrap(),
                source_node_ids: vec![2, 3],
                target_node_id: 4,
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

    // TEST7122: Knitting and re-realizing a strand preserves the ForEach boundary
    // identity independently from its body-entry cap. This is the complete route
    // used by machine execution and catches the identity loss that made persisted
    // body coordinates fail realized-path validation.
    #[test]
    fn test7122_foreach_identity_survives_knit_and_realize() {
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
        let source = media("media:enc=utf-8;page");
        let strand = strand_from_steps(vec![boundary, body], "foreach identity");

        let machine_strand = resolve_strand(&strand, &registry, 0).unwrap();
        assert_eq!(machine_strand.edges()[0].token_id, "tok-cap");
        assert_eq!(
            machine_strand.edges()[0].foreach_token_id.as_deref(),
            Some("tok-foreach")
        );

        let realized =
            crate::machine::realize::realize_strand(&machine_strand, &registry, &source, 0)
                .unwrap();
        assert_eq!(realized.steps[0].token_id, "tok-foreach");
        assert!(matches!(
            realized.steps[0].step_type,
            crate::planner::StrandStepType::ForEach { .. }
        ));
        assert_eq!(realized.steps[1].token_id, "tok-cap");
    }
}
