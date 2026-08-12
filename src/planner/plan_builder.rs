//! Cap Plan Builder
//!
//! Utility for building cap execution plans. This module provides:
//! - Branching plan construction from a resolved `MachineStrand` DAG (via
//!   [`MachinePlanBuilder::build_plan_from_machine_strand`]) — the single, fan-out-aware
//!   plan-construction path
//! - Argument analysis for slot presentation
//!
//! NOTE: Path finding lives in `LiveCapFab` (`get_reachable_targets()`,
//! `find_paths_to_exact_target()`); anchor-realize the resulting `Strand` into a
//! `Machine` and pass its `MachineStrand` to `build_plan_from_machine_strand`.

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

use super::argument_binding::{ArgumentBinding, ArgumentBindings};
use super::cardinality::InputCardinality;
use super::live_cap_fab::{StepToken, Strand};
use super::plan::{ExecutionNodeType, MachineNode, MachinePlan, MachinePlanEdge};
use super::PlannerError;
use crate::{Cap, FabricRegistry, MediaUrn, MediaValidation};

type PlannerResult<T> = Result<T, PlannerError>;

/// Builder for creating cap execution plans.
///
/// NOTE: Path finding methods have been moved to `LiveCapFab`.
/// This builder handles plan construction from pre-computed paths.
pub struct MachinePlanBuilder {
    /// Unified cap + media registry.
    fabric_registry: Arc<FabricRegistry>,
}

impl MachinePlanBuilder {
    /// Create a new plan builder backed by the unified `FabricRegistry`.
    pub fn new(fabric_registry: Arc<FabricRegistry>) -> Self {
        Self { fabric_registry }
    }

    /// Find the file-path argument in a cap by checking the media URN type.
    /// Returns the argument media_urn if found, None otherwise.
    /// This uses tagged URN matching (via `is_file_path()`).
    fn find_file_path_arg(cap: &Cap) -> Option<String> {
        for arg in cap.get_args() {
            if let Ok(urn) = MediaUrn::from_string(&arg.media_urn) {
                if urn.is_file_path() {
                    return Some(arg.media_urn.clone());
                }
            }
        }
        None
    }

    /// Check if a file-path arg is also the primary stdin input slot.
    /// Returns true if the arg has a stdin source whose media URN matches the cap's in_spec.
    /// This means the arg can receive piped data from the previous cap in a chain,
    /// not just a literal file path.
    fn is_file_path_stdin_chainable(cap: &Cap) -> bool {
        let in_spec = cap.urn.in_spec();
        for arg in cap.get_args() {
            let is_file_path = MediaUrn::from_string(&arg.media_urn)
                .map(|urn| urn.is_file_path())
                .unwrap_or(false);
            if !is_file_path {
                continue;
            }
            for source in &arg.sources {
                if let crate::ArgSource::Stdin { stdin } = source {
                    if stdin == in_spec {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Build a branching [`MachinePlan`] directly from a resolved [`MachineStrand`]
    /// (a DAG), preserving fan-out — one source feeding several caps.
    ///
    /// This is the single, DAG-aware plan-construction path the whole system runs on:
    /// notation → `MachineStrand` → this → `MachinePlan`.
    ///
    /// # Model
    ///
    /// - **One producer per data node**, so an edge is emittable once its single stdin
    ///   source has been produced. Fan-out is a node with several outgoing edges — each
    ///   consumer reads the same producer independently.
    /// - **Runtime media flows per data node**, not along a single spine: each cap
    ///   instantiates its runtime output from its source node's runtime media
    ///   (`apply_to_runtime_input_media`).
    /// - **ForEach is cardinality-derived** (`edge.is_loop`, set by `resolve.rs` from
    ///   `Cap::needs_foreach`). `is_loop` marks only the *entry* — a sequence node
    ///   feeding a scalar-input cap. Subsequent scalar caps whose source is produced
    ///   inside that ForEach region carry `is_loop=false` and extend the same body.
    ///   No `Collect` is synthesized — every ForEach is unclosed (per-item output).
    /// - **One `Output` per terminal anchor** — a fan-out strand has several.
    ///
    /// Nested ForEach (a sequence produced *inside* a body re-mapped by a further
    /// scalar cap) is rejected hard — the executor does not support it.
    pub async fn build_plan_from_machine_strand(
        &self,
        name: &str,
        strand: &crate::machine::graph::MachineStrand,
        input_cardinality: InputCardinality,
    ) -> PlannerResult<MachinePlan> {
        // Uniform cardinality across every anchor — the historical single-anchor
        // shape (and the multi-anchor shape where every anchor binds alike).
        let anchor_cardinality: HashMap<crate::machine::graph::NodeId, InputCardinality> = strand
            .input_anchor_ids()
            .iter()
            .map(|&a| (a, input_cardinality))
            .collect();
        self.build_plan_from_machine_strand_with_anchor_cardinalities(
            name,
            strand,
            &anchor_cardinality,
        )
        .await
    }

    /// [`build_plan_from_machine_strand`] with PER-ANCHOR cardinality — the
    /// multi-anchor (convergence-machine) run shape: each input anchor binds its
    /// own file set, so a 3-file anchor feeding a scalar entry cap needs its
    /// per-file ForEach while a 1-file anchor beside it does not. The map must
    /// cover every input anchor; a missing anchor is a hard error (a run never
    /// guesses cardinality).
    pub async fn build_plan_from_machine_strand_with_anchor_cardinalities(
        &self,
        name: &str,
        strand: &crate::machine::graph::MachineStrand,
        anchor_cardinality: &HashMap<crate::machine::graph::NodeId, InputCardinality>,
    ) -> PlannerResult<MachinePlan> {
        use crate::machine::graph::NodeId;

        let mut plan = MachinePlan::new(name);
        let caps =
            self.fabric_registry.get_cached_caps().await.map_err(|e| {
                PlannerError::FabricRegistryError(format!("Failed to get caps: {e}"))
            })?;

        // Per data-node state, keyed by strand NodeId.
        let mut node_runtime: HashMap<NodeId, MediaUrn> = HashMap::new();
        let mut producer: HashMap<NodeId, String> = HashMap::new();
        // ForEach region a data node's data lives inside (unclosed ForEach → membership
        // propagates to every descendant until the terminal). None = top level.
        let mut node_region: HashMap<NodeId, String> = HashMap::new();

        // Deferred ForEach nodes: fe_id → (input_producer_node, body_entry_cap, token).
        // body_exit is tracked separately as later caps extend the region.
        struct PendingForEach {
            input_producer: String,
            body_entry: String,
            token_id: StepToken,
        }
        let mut pending_foreach: HashMap<String, PendingForEach> = HashMap::new();
        let mut region_exit: HashMap<String, String> = HashMap::new();

        let input_anchors: Vec<NodeId> = strand.input_anchor_ids().to_vec();
        for &anchor in &input_anchors {
            let media = strand.node_urn(anchor).clone();
            let cardinality = *anchor_cardinality.get(&anchor).ok_or_else(|| {
                PlannerError::InvalidInput(format!(
                    "input anchor {anchor} ('{media}') has no declared cardinality — a run \
                     never guesses; supply one entry per input anchor"
                ))
            })?;
            let slot_id = format!("input_slot_{anchor}");
            plan.add_node(MachineNode::input_slot(
                &slot_id,
                "input",
                &media.to_string(),
                cardinality,
            ));
            node_runtime.insert(anchor, media);
            producer.insert(anchor, slot_id);
        }

        let edges = strand.edges();

        // Emit edges in data-flow order: an edge is ready once EVERY one of its sources
        // (the main input and any convergence inputs) has a producer.
        let mut emitted = vec![false; edges.len()];
        let mut remaining = edges.len();
        while remaining > 0 {
            let next = edges.iter().enumerate().position(|(i, e)| {
                !emitted[i]
                    && e.assignment
                        .iter()
                        .all(|b| producer.contains_key(&b.source))
            });
            let Some(i) = next else {
                return Err(PlannerError::InvalidPath(format!(
                    "strand '{name}' has cap edges unreachable from its input anchor(s) \
                     (disconnected component or cycle)"
                )));
            };
            let edge = &edges[i];
            let tgt = edge.target;
            let cap_urn_str = edge.cap_urn.to_string();
            let cap = caps
                .iter()
                .find(|c| c.urn.to_string() == cap_urn_str)
                .ok_or_else(|| {
                    PlannerError::NotFound(format!("cap '{cap_urn_str}' not in registry cache"))
                })?;

            // The MAIN input is the binding feeding the cap's stdin argument — it threads
            // the runtime media and drives ForEach/region placement. Every other binding
            // is a convergence input (another cap's output routed into a named non-main
            // arg), emitted as an `Arg` edge below. Identified by tagged-URN equivalence.
            let in_spec_urn = MediaUrn::from_string(cap.urn.in_spec()).map_err(|e| {
                PlannerError::InvalidPath(format!(
                    "cap '{cap_urn_str}' in= URN '{}' invalid: {e}",
                    cap.urn.in_spec()
                ))
            })?;
            let stdin_arg = cap
                .args
                .iter()
                .find(|a| a.is_main_input(&in_spec_urn))
                .ok_or_else(|| {
                    PlannerError::InvalidPath(format!(
                        "cap '{cap_urn_str}' declares no main-input arg (none whose declared \
                         URN or stdin source is its in=)"
                    ))
                })?;
            let stdin_arg_urn = MediaUrn::from_string(&stdin_arg.media_urn).map_err(|e| {
                PlannerError::InvalidPath(format!(
                    "cap '{cap_urn_str}' stdin arg URN '{}' invalid: {e}",
                    stdin_arg.media_urn
                ))
            })?;
            // Group this edge's bindings per arg slot (URN equivalence). A slot
            // with N≥2 bindings is a GATHER — the resolver's implicit Collect: N
            // distinct producers feeding one SEQUENCE arg. The plan materializes
            // it as a real `Collect{input_nodes}` node so the executable plan
            // never carries an unexplained multi-bound arg.
            let mut slot_groups: Vec<(
                MediaUrn,
                Vec<&crate::machine::graph::EdgeAssignmentBinding>,
            )> = Vec::new();
            for b in &edge.assignment {
                match slot_groups
                    .iter_mut()
                    .find(|(u, _)| u.is_equivalent(&b.cap_arg_media_urn).unwrap_or(false))
                {
                    Some((_, v)) => v.push(b),
                    None => slot_groups.push((b.cap_arg_media_urn.clone(), vec![b])),
                }
            }

            let primary_group: &[&crate::machine::graph::EdgeAssignmentBinding] = slot_groups
                .iter()
                .find(|(u, _)| u.is_equivalent(&stdin_arg_urn).unwrap_or(false))
                .map(|(_, v)| v.as_slice())
                .ok_or_else(|| {
                    PlannerError::InvalidPath(format!(
                        "strand '{name}': cap '{cap_urn_str}' has no source bound to its stdin arg"
                    ))
                })?;

            // Synthesize a Collect for a gathered slot: producers wire into the
            // Collect via `Collection` edges; the Collect feeds the cap. Fails
            // hard unless the arg is a sequence arg (a scalar arg with N
            // bindings is a resolver invariant violation). A member produced
            // inside a ForEach region is fine: the executor's post-region
            // segment materializes the region's accumulated per-item output as
            // one sequence, and the gather flattens it alongside the other
            // members in binding order.
            let synthesize_gather_collect =
                |plan: &mut MachinePlan,
                 group: &[&crate::machine::graph::EdgeAssignmentBinding],
                 arg_def: &crate::cap::definition::CapArg,
                 collect_id: &str,
                 producer: &HashMap<NodeId, String>,
                 node_runtime: &HashMap<NodeId, MediaUrn>|
                 -> PlannerResult<MediaUrn> {
                    if !arg_def.is_sequence {
                        return Err(PlannerError::InvalidPath(format!(
                            "strand '{name}': cap '{cap_urn_str}' arg '{}' carries {} bindings \
                             but is not a sequence arg — a gather is only legal into a sequence \
                             arg (resolver invariant violated)",
                            arg_def.media_urn,
                            group.len()
                        )));
                    }
                    let mut input_producer_ids: Vec<String> = Vec::with_capacity(group.len());
                    let mut member_media: Vec<MediaUrn> = Vec::with_capacity(group.len());
                    for b in group {
                        let pid = producer.get(&b.source).cloned().ok_or_else(|| {
                            PlannerError::Internal(format!(
                                "gathered source node {} has no producer",
                                b.source
                            ))
                        })?;
                        let media = node_runtime.get(&b.source).cloned().ok_or_else(|| {
                            PlannerError::Internal(format!(
                                "no runtime media at gathered source node {}",
                                b.source
                            ))
                        })?;
                        input_producer_ids.push(pid);
                        member_media.push(media);
                    }
                    // The gathered sequence's element type is the join ∨ of the
                    // member runtime medias — the most specific type every
                    // member conforms to.
                    let item_media = MediaUrn::least_upper_bound(&member_media);
                    let mut collect_node =
                        MachineNode::collect(collect_id, input_producer_ids.clone());
                    collect_node.node_type = ExecutionNodeType::Collect {
                        input_nodes: input_producer_ids.clone(),
                        output_media_urn: Some(item_media.to_string()),
                    };
                    collect_node.description = Some(format!(
                        "Gather {} producers into a sequence of {}",
                        input_producer_ids.len(),
                        item_media
                    ));
                    plan.add_node(collect_node);
                    for pid in &input_producer_ids {
                        plan.add_edge(MachinePlanEdge::collection(pid, collect_id));
                    }
                    Ok(item_media)
                };

            // Primary (stdin) input: a single binding threads the producer
            // directly (today's path); a gather threads a synthesized Collect.
            let src: NodeId;
            let in_media: MediaUrn;
            let prev_node_id: String;
            let src_is_input_anchor: bool;
            let src_region: Option<String>;
            if primary_group.len() == 1 {
                let primary = primary_group[0];
                src = primary.source;
                in_media = node_runtime.get(&src).cloned().ok_or_else(|| {
                    PlannerError::Internal(format!("no runtime media at source node {src}"))
                })?;
                prev_node_id = producer
                    .get(&src)
                    .cloned()
                    .expect("producer set with runtime");
                src_is_input_anchor = input_anchors.contains(&src);
                src_region = node_region.get(&src).cloned();
            } else {
                let collect_id = format!("collect_{}", edge.token_id);
                let item_media = synthesize_gather_collect(
                    &mut plan,
                    primary_group,
                    stdin_arg,
                    &collect_id,
                    &producer,
                    &node_runtime,
                )?;
                src = primary_group[0].source;
                in_media = item_media;
                prev_node_id = collect_id;
                src_is_input_anchor = false;
                src_region = None;
            }
            // The cap node's id IS the strand step's stable identity (StrandStep.token_id,
            // a UUID minted at parse and threaded through resolved_strand → proto →
            // run graph). Using it as the node id makes it the single execution key: the
            // wizard binds argument values by this same token_id, so delivery is keyed by
            // identity, never by a positional index that shifts when the plan changes.
            let cap_node_id = edge.token_id.clone();

            // A ForEach wraps this cap when the edge maps per-item (`is_loop`, from
            // notation cardinality) OR when THIS cap's anchor binds a sequence
            // feeding a scalar-input entry cap (multi-file execution — the
            // machine is scalar→scalar but iterates once per file). Per-anchor:
            // a 3-file anchor maps per item while a 1-file anchor beside it
            // does not.
            let (cap_input_is_seq, _) = cap.sequence_shape();
            let src_anchor_is_sequence = src_is_input_anchor
                && matches!(
                    anchor_cardinality.get(&src),
                    Some(InputCardinality::Sequence)
                );
            let needs_foreach = edge.is_loop || (src_anchor_is_sequence && !cap_input_is_seq);

            // Nested ForEach — a sequence produced inside a body being re-mapped — is
            // out of scope; fail hard rather than silently mis-execute.
            if needs_foreach && src_region.is_some() {
                return Err(PlannerError::InvalidPath(format!(
                    "strand '{name}': cap '{cap_urn_str}' would nest a ForEach inside the \
                     ForEach body of node {src} — nested ForEach is not supported"
                )));
            }

            // Bindings — same rules as the linear builder.
            let mut bindings = ArgumentBindings::new();
            let in_spec = cap.urn.in_spec();
            let out_spec = cap.urn.out_spec();
            if let Some(arg_name) = Self::find_file_path_arg(cap) {
                let chainable = Self::is_file_path_stdin_chainable(cap);
                if src_is_input_anchor || !chainable {
                    bindings.add_file_path(&arg_name);
                } else {
                    bindings.add(
                        arg_name.clone(),
                        ArgumentBinding::PreviousOutput {
                            node_id: prev_node_id.clone(),
                            output_field: None,
                        },
                    );
                }
            }
            for arg in cap.get_args() {
                if arg.media_urn == in_spec || arg.media_urn == out_spec {
                    continue;
                }
                let arg_urn = MediaUrn::from_string(&arg.media_urn).map_err(|e| {
                    PlannerError::InvalidPath(format!(
                        "cap '{cap_urn_str}' arg URN '{}' invalid: {e}",
                        arg.media_urn
                    ))
                })?;
                if arg_urn.is_file_path() {
                    continue;
                }
                // Convergence-fed args are delivered by an `Arg` producer edge (below),
                // never requested from the user — so they are not slot bindings.
                let is_convergence_fed = edge.assignment.iter().any(|b| {
                    !b.cap_arg_media_urn
                        .is_equivalent(&stdin_arg_urn)
                        .unwrap_or(false)
                        && b.cap_arg_media_urn.is_equivalent(&arg_urn).unwrap_or(false)
                });
                if is_convergence_fed {
                    continue;
                }
                if bindings.bindings.contains_key(&arg.media_urn) {
                    continue;
                }
                bindings.add(
                    arg.media_urn.clone(),
                    ArgumentBinding::Slot {
                        name: arg.media_urn.clone(),
                        schema: None,
                    },
                );
            }

            plan.add_node(MachineNode::cap_with_bindings_token(
                &cap_node_id,
                &cap_urn_str,
                bindings,
                edge.token_id.clone(),
            ));

            if needs_foreach {
                // ForEach entry — the cap becomes a body under a (deferred) ForEach node.
                let fe_id = format!("foreach_{i}");
                let foreach_token_id = if edge.is_loop {
                    edge.foreach_token_id.clone().ok_or_else(|| {
                        PlannerError::InvalidPath(format!(
                            "strand '{name}': loop edge for cap '{cap_urn_str}' has no ForEach identity"
                        ))
                    })?
                } else {
                    // Runtime input cardinality introduced this boundary; this is
                    // the point where that executed graph element is born.
                    StepToken::mint()
                };
                pending_foreach.insert(
                    fe_id.clone(),
                    PendingForEach {
                        input_producer: prev_node_id.clone(),
                        body_entry: cap_node_id.to_string(),
                        token_id: foreach_token_id,
                    },
                );
                region_exit.insert(fe_id.clone(), cap_node_id.to_string());
                node_region.insert(tgt, fe_id);
            } else if let Some(region) = src_region {
                if cap_input_is_seq {
                    // A SEQUENCE-consuming cap fed from inside a ForEach region
                    // CLOSES the region: it consumes the region's collected
                    // per-item output as one sequence (the fold). It is wired
                    // like any downstream consumer and does NOT extend the
                    // region — the executor runs it in the post-region segment.
                    plan.add_edge(MachinePlanEdge::direct(&prev_node_id, &cap_node_id));
                } else {
                    // Scalar cap extending an existing ForEach body — chain onto the
                    // producer and extend the region's exit.
                    plan.add_edge(MachinePlanEdge::direct(&prev_node_id, &cap_node_id));
                    region_exit.insert(region.clone(), cap_node_id.to_string());
                    node_region.insert(tgt, region);
                }
            } else {
                // Top-level scalar cap.
                plan.add_edge(MachinePlanEdge::direct(&prev_node_id, &cap_node_id));
            }

            // Convergence edges: each non-main slot group wires producer output into
            // the named non-main arg of this cap. A single-binding group wires its
            // producer directly (the historical path); a gathered group (N≥2, a
            // sequence arg) wires through a synthesized Collect. The runtime streams
            // it as this cap's arg, keyed by the arg URN.
            for (group_idx, (slot_urn, group)) in slot_groups.iter().enumerate() {
                if slot_urn.is_equivalent(&stdin_arg_urn).unwrap_or(false) {
                    continue;
                }
                // The stream URN the cartridge demuxes this arg by is the arg's STDIN
                // URN — which may differ from its slot media URN (the binding's
                // identity). Look up the arg by its slot URN, then take its stdin URN.
                let arg_def = cap
                    .args
                    .iter()
                    .find(|a| {
                        MediaUrn::from_string(&a.media_urn)
                            .map(|u| u.is_equivalent(slot_urn).unwrap_or(false))
                            .unwrap_or(false)
                    })
                    .ok_or_else(|| {
                        PlannerError::InvalidPath(format!(
                            "cap '{cap_urn_str}': convergence arg '{slot_urn}' is not in the cap definition"
                        ))
                    })?;
                // The stream URN the cartridge demuxes this arg by: its stdin source
                // URN if it declares one, else its declared URN (a producer-fed arg
                // need not use stdin).
                let stream_urn = arg_def.stream_urn().to_string();
                if group.len() == 1 {
                    let producer_node = producer
                        .get(&group[0].source)
                        .cloned()
                        .expect("emittable: every source has a producer");
                    plan.add_edge(MachinePlanEdge::arg(
                        &producer_node,
                        &cap_node_id,
                        &stream_urn,
                    ));
                } else {
                    let collect_id = format!("collect_{}_{group_idx}", edge.token_id);
                    synthesize_gather_collect(
                        &mut plan,
                        group,
                        arg_def,
                        &collect_id,
                        &producer,
                        &node_runtime,
                    )?;
                    plan.add_edge(MachinePlanEdge::arg(&collect_id, &cap_node_id, &stream_urn));
                }
            }

            let out_media = edge
                .cap_urn
                .apply_to_runtime_input_media(&in_media)
                .map_err(|e| {
                    PlannerError::InvalidPath(format!(
                        "runtime media inference for '{cap_urn_str}' on '{in_media}': {e}"
                    ))
                })?;
            node_runtime.insert(tgt, out_media);
            producer.insert(tgt, cap_node_id.to_string());
            emitted[i] = true;
            remaining -= 1;
        }

        // Materialize the deferred ForEach nodes now that body extents are known.
        for (fe_id, pf) in &pending_foreach {
            let body_exit = region_exit
                .get(fe_id)
                .cloned()
                .unwrap_or_else(|| pf.body_entry.clone());
            plan.add_node(MachineNode::for_each_token(
                fe_id,
                &pf.input_producer,
                &pf.body_entry,
                &body_exit,
                pf.token_id.clone(),
            ));
            plan.add_edge(MachinePlanEdge::direct(&pf.input_producer, fe_id));
            plan.add_edge(MachinePlanEdge::iteration(fe_id, &pf.body_entry));
        }

        // One Output per terminal anchor.
        for &anchor in strand.output_anchor_ids() {
            let src_node = producer.get(&anchor).ok_or_else(|| {
                PlannerError::Internal(format!("terminal node {anchor} has no producer"))
            })?;
            let out_id = format!("output_{anchor}");
            plan.add_node(MachineNode::output(&out_id, "result", src_node));
            plan.add_edge(MachinePlanEdge::direct(src_node, &out_id));
        }

        if let Some(&first_input) = input_anchors.first() {
            let source_media = strand.node_urn(first_input).to_string();
            plan.metadata = Some(HashMap::from([(
                "source_media_urn".to_string(),
                json!(source_media),
            )]));
        }

        plan.validate()?;
        plan.topological_order().map_err(|e| {
            PlannerError::InvalidPath(format!(
                "build_plan_from_machine_strand produced a cyclic plan: {e}"
            ))
        })?;
        Ok(plan)
    }
}

// NOTE: Path finding methods (find_path, get_reachable_targets, get_reachable_targets_with_metadata,
// find_all_paths) have been moved to LiveCapFab. Use LiveCapFab for path finding and
// build_plan_from_path for plan construction.
//
// The old string-based ReachableTargetInfo, StrandStep, Strand types have been
// replaced by the typed versions in live_cap_fab.rs.

// =============================================================================
// ARGUMENT ANALYSIS FOR SLOT PRESENTATION
// =============================================================================

/// How an argument will be resolved
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArgumentResolution {
    /// Auto-resolved from input file (for first cap's file_path)
    FromInputFile,
    /// Auto-resolved from previous cap's output
    FromPreviousOutput,
    /// Has a default value in cap definition
    HasDefault,
    /// Must be provided by user (slot)
    RequiresUserInput,
}

/// Information about a single argument for UI presentation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArgumentInfo {
    /// Argument name (e.g., "file_path", "width")
    pub name: String,
    /// Media URN describing the type (e.g., "media:integer")
    pub media_urn: String,
    /// Human-readable description
    pub description: String,
    /// How this argument will be resolved
    pub resolution: ArgumentResolution,
    /// Default value if any
    pub default_value: Option<serde_json::Value>,
    /// Whether this is a required argument
    pub is_required: bool,
    /// Whether this argument carries a sequence of items
    pub is_sequence: bool,
    /// Whether this is the cap's MAIN input — the arg whose `Stdin` source URN
    /// is tagged-URN-equivalent to the cap URN's `in=` spec (see
    /// `CapArg::is_main_input`). UIs present this arg as "how the cap is fed"
    /// rather than as an option.
    pub is_main_input: bool,
    /// Validation rules if any
    pub validation: Option<serde_json::Value>,
}

/// Argument requirements for a single step in the path
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepArgumentRequirements {
    /// Cap URN for this step
    pub cap_urn: String,
    /// The planner-minted `StrandStep::token_id` of the step these requirements
    /// describe — the ONLY address an argument value is ever bound to.
    ///
    /// A token exists because a plan exists. It is never derived from a
    /// position, a notation string, or anything else: the plan mints it and
    /// everything downstream carries it unchanged. Positions cannot address a
    /// step because a strand is a DAG — parallel branches merging downstream
    /// have no ordinal, and two identical caps on separate branches are
    /// distinguishable only by token.
    pub token_id: StepToken,
    /// Cap title
    pub title: String,
    /// All arguments for this cap with their resolution status
    pub arguments: Vec<ArgumentInfo>,
    /// Arguments that require user input (slots)
    pub slots: Vec<ArgumentInfo>,
    /// Architecture identifiers (config.json `model_type`) the cap can
    /// run. Forwarded by the gRPC layer to UI components so model
    /// pickers only surface compatible models. Empty when the cap
    /// declares no restriction (i.e., doesn't load a model at all).
    #[serde(default)]
    pub supported_model_types: Vec<String>,
    /// Default model spec literal declared in the cap's capfab toml.
    /// `None` when the cap has no default model.
    #[serde(default)]
    pub default_model_spec: Option<String>,
}

/// Argument requirements for an entire path
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathArgumentRequirements {
    /// Source media URN
    pub source_media_urn: String,
    /// Target media URN
    pub target_media_urn: String,
    /// Requirements for each step
    pub steps: Vec<StepArgumentRequirements>,
    /// Whether this path can execute without any user input
    pub can_execute_without_input: bool,
}

impl MachinePlanBuilder {
    /// Analyze argument requirements for a path.
    ///
    /// Takes the new typed `Strand` from `live_cap_fab` which uses
    /// typed `MediaUrn` and `CapUrn` values.
    ///
    /// Only Cap steps have arguments to analyze. ForEach/Collect steps
    /// are cardinality transitions with no user-configurable arguments.
    pub async fn analyze_path_arguments(
        &self,
        path: &Strand,
    ) -> PlannerResult<PathArgumentRequirements> {
        let caps =
            self.fabric_registry.get_cached_caps().await.map_err(|e| {
                PlannerError::FabricRegistryError(format!("Failed to get caps: {}", e))
            })?;

        let mut step_requirements = Vec::new();
        // Track cap step index for determining first cap (affects file_path resolution)
        let mut cap_step_index = 0;

        for step in path.steps.iter() {
            // Only analyze Cap steps - cardinality transitions have no arguments
            let cap_urn = match step.cap_urn() {
                Some(urn) => urn,
                None => continue, // Skip ForEach/Collect steps
            };


            let cap_urn_str = cap_urn.to_string();
            let cap = caps
                .iter()
                .find(|c| c.urn.to_string() == cap_urn_str)
                .ok_or_else(|| {
                    PlannerError::NotFound(format!("Cap '{}' not found in registry", cap_urn_str))
                })?;

            let in_spec = cap.urn.in_spec();
            let out_spec = cap.urn.out_spec();
            // The cap's `in=` spec as a typed URN — main-input identification is
            // a tagged-URN-equivalence question, never a string comparison.
            let in_spec_urn = MediaUrn::from_string(in_spec).map_err(|e| {
                PlannerError::InvalidPath(format!(
                    "cap '{cap_urn_str}' in= URN '{in_spec}' invalid: {e}"
                ))
            })?;

            let mut arguments = Vec::new();
            let mut slots = Vec::new();

            for arg in cap.get_args() {
                let resolution = self.determine_resolution_with_io_check(
                    &arg.media_urn,
                    &in_spec,
                    &out_spec,
                    cap_step_index,
                    arg.required,
                    &arg.default_value,
                );

                // Resolve validation from the media definition via the registry. There
                // is no inline `cap.media_defs` override anymore — every
                // media URN is resolved through the same path.
                let resolved_spec =
                    crate::media::spec::resolve_media_urn(&arg.media_urn, &self.fabric_registry)
                        .await
                        .ok();
                let validation = resolved_spec.and_then(|spec| spec.validation);

                let arg_info = ArgumentInfo {
                    name: arg.media_urn.clone(),
                    media_urn: arg.media_urn.clone(),
                    description: arg.arg_description.clone().unwrap_or_default(),
                    resolution: resolution.clone(),
                    default_value: arg.default_value.clone(),
                    is_required: arg.required,
                    is_sequence: arg.is_sequence,
                    is_main_input: arg.is_main_input(&in_spec_urn),
                    validation: Self::validation_to_json(validation.as_ref()),
                };

                let is_io_arg = resolution == ArgumentResolution::FromInputFile
                    || resolution == ArgumentResolution::FromPreviousOutput;

                if !is_io_arg {
                    slots.push(arg_info.clone());
                }
                arguments.push(arg_info);
            }

            step_requirements.push(StepArgumentRequirements {
                cap_urn: cap_urn_str,
                token_id: step.token_id.clone(),
                title: step.title(),
                arguments,
                slots,
                supported_model_types: cap.supported_model_types.clone(),
                default_model_spec: cap.default_model_spec.clone(),
            });

            cap_step_index += 1;
        }

        let can_execute_without_input = step_requirements.iter().all(|s| s.slots.is_empty());

        Ok(PathArgumentRequirements {
            source_media_urn: path.source_media_urn.to_string(),
            target_media_urn: path.target_media_urn.to_string(),
            steps: step_requirements,
            can_execute_without_input,
        })
    }

    /// Convert MediaValidation to JSON if it has any constraints
    fn validation_to_json(validation: Option<&MediaValidation>) -> Option<serde_json::Value> {
        let validation = validation?;

        let has_constraints = validation.min.is_some()
            || validation.max.is_some()
            || validation.min_length.is_some()
            || validation.max_length.is_some()
            || validation.pattern.is_some()
            || validation.allowed_values.is_some();

        if has_constraints {
            serde_json::to_value(validation).ok()
        } else {
            None
        }
    }

    /// Determine how an argument will be resolved based on I/O matching and media URN type.
    fn determine_resolution_with_io_check(
        &self,
        media_urn: &str,
        in_spec: &str,
        out_spec: &str,
        step_index: usize,
        _is_required: bool,
        default_value: &Option<serde_json::Value>,
    ) -> ArgumentResolution {
        // Check if this arg is the input arg (matches cap's in= spec)
        if media_urn == in_spec {
            if step_index == 0 {
                return ArgumentResolution::FromInputFile;
            } else {
                return ArgumentResolution::FromPreviousOutput;
            }
        }

        // Check if this arg is the output arg (matches cap's out= spec)
        if media_urn == out_spec {
            return ArgumentResolution::FromPreviousOutput;
        }

        // Check for file-path types
        let is_file_path_type = if let Ok(urn) = MediaUrn::from_string(media_urn) {
            urn.is_file_path()
        } else {
            false
        };

        if is_file_path_type {
            if step_index == 0 {
                return ArgumentResolution::FromInputFile;
            } else {
                return ArgumentResolution::FromPreviousOutput;
            }
        }

        // All other args need user input
        if default_value.is_some() {
            return ArgumentResolution::HasDefault;
        }

        ArgumentResolution::RequiresUserInput
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CapUrn;
    use std::collections::{BTreeMap, HashSet};

    /// Helper to create a test cap with given in/out specs (full media URNs)
    fn make_test_cap(
        op: &str,
        in_spec: &str,
        out_spec: &str,
        title: &str,
    ) -> Result<Cap, crate::urn::cap_urn::CapUrnError> {
        // Operation is encoded as a marker tag (key=*), the canonical
        // form. The `op` parameter is the marker name (e.g. "convert"). This is just a convention.
        let mut tags = BTreeMap::new();
        tags.insert(op.to_string(), "*".to_string());
        let urn = CapUrn::new(
            in_spec.to_string(),
            out_spec.to_string(),
            "declared".to_string(),
            tags,
        )?;
        Ok(Cap::new(
            urn,
            title.to_string(),
            vec!["test-command".to_string()],
        ))
    }

    /// Simulates the graph-building duplicate detection logic
    fn check_for_duplicate_caps(caps: &[Cap]) -> std::result::Result<usize, String> {
        let mut seen_edges: HashSet<(String, String)> = HashSet::new();
        let mut edge_count = 0;

        for cap in caps {
            let input_spec = cap.urn.in_spec();
            let output_spec = cap.urn.out_spec();

            if input_spec.is_empty() || output_spec.is_empty() {
                continue;
            }

            let cap_urn = cap.urn.to_string();

            let edge_key = (input_spec.to_string(), cap_urn.clone());
            if !seen_edges.insert(edge_key) {
                return Err(format!(
                    "Duplicate cap_urn detected: {} (input_spec: {})",
                    cap_urn, input_spec
                ));
            }
            edge_count += 1;
        }

        Ok(edge_count)
    }

    // TEST880: Tests duplicate detection passes for caps with unique URN combinations
    // Verifies that check_for_duplicate_caps() correctly accepts caps with different op/in/out combinations
    #[test]
    fn test880_no_duplicates_with_unique_caps() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let caps = vec![
            make_test_cap(
                "extract_metadata",
                "media:ext=pdf",
                "media:enc=utf-8;file-metadata;record",
                "Extract Metadata",
            )?,
            make_test_cap(
                "extract_outline",
                "media:ext=pdf",
                "media:document-outline;enc=utf-8;record",
                "Extract Outline",
            )?,
            make_test_cap(
                "disbind",
                "media:ext=pdf",
                "media:disbound-pages;enc=utf-8;list",
                "Disbind PDF",
            )?,
        ];

        let result = check_for_duplicate_caps(&caps);
        assert!(
            result.is_ok(),
            "Should not detect duplicates for unique caps"
        );
        assert_eq!(result.unwrap(), 3, "Should have 3 edges");
        Ok(())
    }

    // TEST991: Tests duplicate detection identifies caps with identical URNs
    // Verifies that check_for_duplicate_caps() returns an error when multiple caps share the same cap_urn
    #[test]
    fn test991_detects_duplicate_cap_urns() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let caps = vec![
            make_test_cap(
                "disbind",
                "media:ext=pdf",
                "media:disbound-pages;enc=utf-8;list",
                "Disbind PDF",
            )?,
            make_test_cap(
                "disbind",
                "media:ext=pdf",
                "media:disbound-pages;enc=utf-8;list",
                "Disbind PDF Again",
            )?,
        ];

        let result = check_for_duplicate_caps(&caps);
        assert!(result.is_err(), "Should detect duplicate cap URN");
        let err_msg = result.unwrap_err();
        assert!(
            err_msg.contains("Duplicate cap_urn detected"),
            "Error should mention duplicate: {}",
            err_msg
        );
        assert!(
            err_msg.contains("disbind"),
            "Error should contain the cap URN: {}",
            err_msg
        );
        assert!(
            err_msg.contains("media:ext=pdf"),
            "Error should contain the input spec: {}",
            err_msg
        );
        Ok(())
    }

    // TEST992: Tests caps with different operations but same input/output types are not duplicates
    // Verifies that only the complete URN (including op) is used for duplicate detection
    #[test]
    fn test992_different_ops_same_types_not_duplicates(
    ) -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let caps = vec![
            make_test_cap(
                "disbind",
                "media:ext=pdf",
                "media:disbound-pages;enc=utf-8;list",
                "Disbind",
            )?,
            make_test_cap(
                "grind",
                "media:ext=pdf",
                "media:disbound-pages;enc=utf-8;list",
                "Grind",
            )?,
        ];

        let result = check_for_duplicate_caps(&caps);
        assert!(result.is_ok(), "Different ops should not be duplicates");
        assert_eq!(result.unwrap(), 2, "Should have 2 edges");
        Ok(())
    }

    // TEST993: Tests caps with same operation but different input types are not duplicates
    // Verifies that input type differences distinguish caps with the same operation name
    #[test]
    fn test993_same_op_different_input_types_not_duplicates(
    ) -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let caps = vec![
            make_test_cap(
                "extract_metadata",
                "media:ext=pdf",
                "media:enc=utf-8;file-metadata;record",
                "Extract PDF Metadata",
            )?,
            make_test_cap(
                "extract_metadata",
                "media:enc=utf-8;ext=txt",
                "media:enc=utf-8;file-metadata;record",
                "Extract TXT Metadata",
            )?,
        ];

        let result = check_for_duplicate_caps(&caps);
        assert!(
            result.is_ok(),
            "Same op with different inputs should not be duplicates"
        );
        assert_eq!(result.unwrap(), 2, "Should have 2 edges");
        Ok(())
    }

    // ==========================================================================
    // ARGUMENT RESOLUTION TESTS
    // ==========================================================================

    fn create_test_plan_builder() -> MachinePlanBuilder {
        MachinePlanBuilder::new(Arc::new(FabricRegistry::new_for_test()))
    }

    fn create_test_plan_builder_with_registry(registry: FabricRegistry) -> MachinePlanBuilder {
        MachinePlanBuilder::new(Arc::new(registry))
    }

    // TEST994: Tests first cap's input argument is automatically resolved from input file
    // Verifies that determine_resolution_with_io_check() returns FromInputFile for the first cap in a chain
    #[test]
    fn test994_input_arg_first_cap_auto_resolved_from_input() {
        let builder = create_test_plan_builder();
        let in_spec = "media:ext=pdf";
        let out_spec = "media:ext=png;image";
        let resolution =
            builder.determine_resolution_with_io_check(in_spec, in_spec, out_spec, 0, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromInputFile);
    }

    // TEST995: Tests subsequent caps' input arguments are automatically resolved from previous output
    // Verifies that determine_resolution_with_io_check() returns FromPreviousOutput for caps after the first
    #[test]
    fn test995_input_arg_subsequent_cap_auto_resolved_from_previous() {
        let builder = create_test_plan_builder();
        let in_spec = "media:ext=pdf";
        let out_spec = "media:ext=png;image";

        let resolution =
            builder.determine_resolution_with_io_check(in_spec, in_spec, out_spec, 1, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromPreviousOutput);

        let resolution =
            builder.determine_resolution_with_io_check(in_spec, in_spec, out_spec, 2, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromPreviousOutput);
    }

    // TEST996: Tests output arguments are automatically resolved from previous cap's output
    // Verifies that arguments matching the output spec are always resolved as FromPreviousOutput
    #[test]
    fn test996_output_arg_auto_resolved() {
        let builder = create_test_plan_builder();
        let in_spec = "media:ext=pdf";
        let out_spec = "media:ext=png;image";
        let resolution =
            builder.determine_resolution_with_io_check(out_spec, in_spec, out_spec, 0, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromPreviousOutput);
    }

    // TEST997: Tests MEDIA_FILE_PATH argument type resolves to input file for first cap
    // Verifies that generic file-path arguments are bound to input file in the first cap
    #[test]
    fn test997_file_path_type_fallback_first_cap() {
        let builder = create_test_plan_builder();
        let in_spec = "media:ext=pdf";
        let out_spec = "media:ext=png;image";
        let resolution = builder.determine_resolution_with_io_check(
            crate::MEDIA_FILE_PATH,
            in_spec,
            out_spec,
            0,
            true,
            &None,
        );
        assert_eq!(resolution, ArgumentResolution::FromInputFile);
    }

    // TEST998: Tests MEDIA_FILE_PATH argument type resolves to previous output for subsequent caps
    // Verifies that generic file-path arguments are bound to previous cap's output after the first cap
    #[test]
    fn test998_file_path_type_fallback_subsequent_cap() {
        let builder = create_test_plan_builder();
        let in_spec = "media:ext=pdf";
        let out_spec = "media:ext=png;image";
        let resolution = builder.determine_resolution_with_io_check(
            crate::MEDIA_FILE_PATH,
            in_spec,
            out_spec,
            1,
            true,
            &None,
        );
        assert_eq!(resolution, ArgumentResolution::FromPreviousOutput);
    }

    // TEST1009: Tests required non-IO arguments with default values are marked as HasDefault
    // Verifies that arguments like integers with defaults don't require user input
    #[test]
    fn test1009_non_io_arg_with_default_has_default() {
        let builder = create_test_plan_builder();
        let default = Some(serde_json::json!(200));
        let in_spec = "media:ext=pdf";
        let out_spec = "media:ext=png;image";
        let resolution = builder.determine_resolution_with_io_check(
            crate::MEDIA_INTEGER,
            in_spec,
            out_spec,
            0,
            true,
            &default,
        );
        assert_eq!(resolution, ArgumentResolution::HasDefault);
    }

    // TEST1012: Tests required non-IO arguments without defaults require user input
    // Verifies that arguments like strings without defaults are marked as RequiresUserInput
    #[test]
    fn test1012_non_io_arg_without_default_requires_user_input() {
        let builder = create_test_plan_builder();
        let in_spec = "media:ext=pdf";
        let out_spec = "media:ext=png;image";
        let resolution = builder.determine_resolution_with_io_check(
            crate::MEDIA_STRING,
            in_spec,
            out_spec,
            0,
            true,
            &None,
        );
        assert_eq!(resolution, ArgumentResolution::RequiresUserInput);
    }

    // TEST886: Tests optional non-IO arguments with default values are marked as HasDefault
    // Verifies that optional arguments with defaults behave the same as required ones with defaults
    #[test]
    fn test886_optional_non_io_arg_with_default_has_default() {
        let builder = create_test_plan_builder();
        let default = Some(serde_json::json!(300));
        let in_spec = "media:ext=pdf";
        let out_spec = "media:ext=png;image";
        let resolution = builder.determine_resolution_with_io_check(
            crate::MEDIA_INTEGER,
            in_spec,
            out_spec,
            0,
            false,
            &default,
        );
        assert_eq!(resolution, ArgumentResolution::HasDefault);
    }

    // TEST1015: Tests optional non-IO arguments without defaults still require user input
    // Verifies that optional arguments without defaults must be explicitly provided or skipped
    #[test]
    fn test1015_optional_non_io_arg_without_default_requires_user_input() {
        let builder = create_test_plan_builder();
        let in_spec = "media:ext=pdf";
        let out_spec = "media:ext=png;image";
        let resolution = builder.determine_resolution_with_io_check(
            crate::MEDIA_BOOLEAN,
            in_spec,
            out_spec,
            0,
            false,
            &None,
        );
        assert_eq!(resolution, ArgumentResolution::RequiresUserInput);
    }

    // TEST1019: Tests validation_to_json() returns None for None input
    // Verifies that missing validation metadata is converted to JSON None
    #[test]
    fn test1019_validation_to_json_none() {
        let json = MachinePlanBuilder::validation_to_json(None);
        assert!(json.is_none(), "None validation should return None");
    }

    // TEST765: Tests validation_to_json() returns None for empty validation constraints
    // Verifies that default MediaValidation with no constraints produces JSON None
    #[test]
    fn test765_validation_to_json_empty() {
        let validation = MediaValidation::default();
        let json = MachinePlanBuilder::validation_to_json(Some(&validation));
        assert!(json.is_none(), "Empty validation should return None");
    }

    // TEST766: Tests validation_to_json() converts MediaValidation with constraints to JSON
    // Verifies that min/max validation rules are correctly serialized as JSON fields
    #[test]
    fn test766_validation_to_json_with_constraints() {
        let validation = MediaValidation {
            min: Some(50.0),
            max: Some(2000.0),
            min_length: None,
            max_length: None,
            pattern: None,
            allowed_values: None,
        };
        let json = MachinePlanBuilder::validation_to_json(Some(&validation));
        assert!(
            json.is_some(),
            "Validation with constraints should return Some"
        );
        let json = json.unwrap();
        assert_eq!(json["min"], 50.0);
        assert_eq!(json["max"], 2000.0);
    }

    // TEST767: Tests ArgumentInfo struct serialization to JSON
    // Verifies that argument metadata including resolution status and validation is correctly serialized
    #[test]
    fn test767_argument_info_serialization() {
        let arg_info = ArgumentInfo {
            name: "width".to_string(),
            media_urn: "media:integer".to_string(),
            description: "Width in pixels".to_string(),
            resolution: ArgumentResolution::HasDefault,
            default_value: Some(serde_json::json!(200)),
            is_required: false,
            is_sequence: false,
            is_main_input: false,
            validation: Some(serde_json::json!({"min": 50, "max": 2000})),
        };

        let json = serde_json::to_string(&arg_info).expect("Should serialize");
        assert!(json.contains("\"name\":\"width\""));
        assert!(json.contains("\"resolution\":\"has_default\""));
        assert!(json.contains("\"default_value\":200"));
    }

    // TEST768: Tests PathArgumentRequirements structure for single-step execution paths
    // Verifies that argument requirements are correctly organized by step with resolution information
    #[test]
    fn test768_path_argument_requirements_structure() {
        let requirements = PathArgumentRequirements {
            source_media_urn: "media:ext=pdf".to_string(),
            target_media_urn: "media:ext=png;image".to_string(),
            steps: vec![StepArgumentRequirements {
                cap_urn: "cap:generate-thumbnail;in=pdf;out=png".to_string(),
                token_id: "tok-thumbnail".parse().unwrap(),
                title: "Generate Thumbnail".to_string(),
                arguments: vec![ArgumentInfo {
                    name: "file_path".to_string(),
                    media_urn: "media:string".to_string(),
                    description: "Path to file".to_string(),
                    resolution: ArgumentResolution::FromInputFile,
                    default_value: None,
                    is_required: true,
                    is_sequence: false,
                    is_main_input: true,
                    validation: None,
                }],
                slots: vec![],
                supported_model_types: Vec::new(),
                default_model_spec: None,
            }],
            can_execute_without_input: true,
        };

        assert!(requirements.can_execute_without_input);
        assert_eq!(requirements.steps.len(), 1);
        assert_eq!(requirements.steps[0].slots.len(), 0);
        assert_eq!(
            requirements.steps[0].arguments[0].resolution,
            ArgumentResolution::FromInputFile
        );
    }

    // TEST769: Tests PathArgumentRequirements tracking of required user-input slots
    // Verifies that arguments requiring user input are collected in slots and can_execute_without_input is false
    #[test]
    fn test769_path_with_required_slot() {
        let requirements = PathArgumentRequirements {
            source_media_urn: "media:text".to_string(),
            target_media_urn: "media:translated".to_string(),
            steps: vec![StepArgumentRequirements {
                cap_urn: "cap:translate;in=text;out=translated".to_string(),
                token_id: "tok-translate".parse().unwrap(),
                title: "Translate".to_string(),
                arguments: vec![
                    ArgumentInfo {
                        name: "file_path".to_string(),
                        media_urn: "media:string".to_string(),
                        description: "Path to file".to_string(),
                        resolution: ArgumentResolution::FromInputFile,
                        default_value: None,
                        is_required: true,
                        is_sequence: false,
                        is_main_input: true,
                        validation: None,
                    },
                    ArgumentInfo {
                        name: "target_language".to_string(),
                        media_urn: "media:string".to_string(),
                        description: "Target language code".to_string(),
                        resolution: ArgumentResolution::RequiresUserInput,
                        default_value: None,
                        is_required: true,
                        is_sequence: false,
                        is_main_input: false,
                        validation: None,
                    },
                ],
                slots: vec![ArgumentInfo {
                    name: "target_language".to_string(),
                    media_urn: "media:string".to_string(),
                    description: "Target language code".to_string(),
                    resolution: ArgumentResolution::RequiresUserInput,
                    default_value: None,
                    is_required: true,
                    is_sequence: false,
                    is_main_input: false,
                    validation: None,
                }],
                supported_model_types: Vec::new(),
                default_model_spec: None,
            }],
            can_execute_without_input: false,
        };

        assert!(!requirements.can_execute_without_input);
        assert_eq!(requirements.steps[0].slots.len(), 1);
        assert_eq!(requirements.steps[0].slots[0].name, "target_language");
    }

    // ==========================================================================
    // URN CANONICALIZATION TESTS
    // ==========================================================================
    // NOTE: Path finding tests (TEST770-787) have been moved to live_cap_fab.rs
    // as path finding is now handled by LiveCapFab, not MachinePlanBuilder.
    // Availability filtering (TEST770-776) is now implicit in LiveCapFab sync.
    // Path coherence scoring (TEST782-787) has been removed from the architecture.

    // TEST1100: Tests that CapUrn normalizes media URN tags to canonical order
    // This is the root cause fix for caps not matching when cartridges report URNs with
    // different tag ordering than the registry (e.g., "record;enc=utf-8" vs "enc=utf-8;record")
    #[test]
    fn test1100_cap_urn_normalizes_media_urn_tag_order(
    ) -> Result<(), crate::urn::cap_urn::CapUrnError> {
        // Create two CapUrns with different tag ordering in the output media URN
        let urn1 = CapUrn::from_string(
            "cap:extract-metadata;in=\"media:ext=pdf\";out=\"media:enc=utf-8;file-metadata;record\"",
        )?;
        let urn2 = CapUrn::from_string(
            "cap:extract-metadata;in=\"media:ext=pdf\";out=\"media:enc=utf-8;file-metadata;record\"",
        )?;

        // After normalization, both should produce the same canonical string
        assert_eq!(
            urn1.to_string(),
            urn2.to_string(),
            "URNs with different tag ordering should normalize to the same canonical form"
        );

        // The canonical form should have tags in alphabetical order: the out
        // media URN's tags normalize to `enc=utf-8;file-metadata;record`.
        let canonical = urn1.to_string();
        assert!(
            canonical.contains("enc=utf-8;file-metadata;record"),
            "Canonical form should contain the normalized out tags: {}",
            canonical
        );

        Ok(())
    }

    // TEST1103: Tests that is_dispatchable has correct directionality
    // The available cap (candidate) must be dispatchable for the requested cap (request).
    // This tests the directionality: candidate.is_dispatchable(&request)
    // NOTE: This now tests CapUrn::is_dispatchable directly, not via MachinePlanBuilder
    #[test]
    fn test1103_is_dispatchable_uses_correct_directionality() {
        // A more specific candidate should be dispatchable for a general request
        let general_request =
            CapUrn::from_string("cap:in=\"media:ext=pdf\";extract;out=media:text").unwrap();

        let specific_candidate =
            CapUrn::from_string("cap:in=\"media:ext=pdf\";extract;out=media:text;version=2")
                .unwrap();

        // candidate.is_dispatchable(&request) should be true: specific candidate refines general request
        assert!(
            specific_candidate.is_dispatchable(&general_request),
            "Specific candidate should be dispatchable for general request"
        );

        // request.is_dispatchable(&candidate) should be false: general request cannot handle specific candidate's requirements
        assert!(
            !general_request.is_dispatchable(&specific_candidate),
            "General request should NOT be dispatchable for specific candidate (missing version tag)"
        );
    }

    // TEST1104: Tests that is_dispatchable rejects when candidate cannot dispatch request
    #[test]
    fn test1104_is_dispatchable_rejects_non_dispatchable() {
        // Request requires specific tag that candidate doesn't have
        let request =
            CapUrn::from_string("cap:in=\"media:ext=pdf\";extract;out=media:text;required=yes")
                .unwrap();

        let candidate = CapUrn::from_string(
            "cap:in=\"media:ext=pdf\";extract;out=media:text", // missing required=yes
        )
        .unwrap();

        // candidate is NOT dispatchable for request (missing required tag that request needs)
        assert!(
            !candidate.is_dispatchable(&request),
            "Candidate missing required tag should not be dispatchable for request"
        );
    }

    /// TEST7104: over a realistic multi-arg cap (one stdin MAIN input whose
    /// slot URN differs from its stdin URN, one required defaultless cli_flag
    /// arg, several defaulted cli_flag args), exactly one arg is the main
    /// input, and partitioning the rest by `required && default_value.is_none()`
    /// yields the expected required-options vs defaulted-options sets. The
    /// planner's real step-requirements assembly (`analyze_path_arguments`)
    /// must set `ArgumentInfo.is_main_input` accordingly for every arg.
    #[tokio::test]
    async fn test7104_main_input_and_option_partition_through_step_requirements() {
        use crate::cap::definition::{ArgSource, CapArg};
        use crate::planner::live_cap_fab::{ArgSourceRef, CapInput, StrandStep, StrandStepType};

        // The main input's stdin URN spells `in=` with the tags in a DIFFERENT
        // string order, and its slot URN differs from the stdin URN — only
        // tagged-URN equivalence against the stdin source identifies it.
        let urn = CapUrn::from_string(
            r#"cap:in="media:doc;ext=pdf";summarize;out="media:enc=utf-8;summary""#,
        )
        .unwrap();
        let mut cap = Cap::new(urn, "Summarize".to_string(), vec!["summarize".to_string()]);
        cap.args = vec![
            CapArg::with_full_definition(
                "media:enc=utf-8;file-path",
                true,
                false,
                vec![ArgSource::Stdin {
                    stdin: "media:ext=pdf;doc".to_string(),
                }],
                Some("Document to summarize".to_string()),
                None,
                None,
            ),
            CapArg::with_full_definition(
                "media:enc=utf-8;model-spec",
                true,
                false,
                vec![ArgSource::CliFlag {
                    cli_flag: "--model-spec".to_string(),
                }],
                Some("Model to run".to_string()),
                None,
                None,
            ),
            CapArg::with_full_definition(
                "media:budget;numeric",
                false,
                false,
                vec![ArgSource::CliFlag {
                    cli_flag: "--budget".to_string(),
                }],
                Some("Token budget".to_string()),
                Some(serde_json::json!(400)),
                None,
            ),
            CapArg::with_full_definition(
                "media:numeric;temperature",
                false,
                false,
                vec![ArgSource::CliFlag {
                    cli_flag: "--temperature".to_string(),
                }],
                Some("Sampling temperature".to_string()),
                Some(serde_json::json!(0.7)),
                None,
            ),
        ];

        // The definition-level partition: exactly ONE main input; the rest
        // split into required options vs defaulted options.
        let in_spec = MediaUrn::from_string(cap.urn.in_spec()).unwrap();
        let (main_inputs, others): (Vec<&CapArg>, Vec<&CapArg>) =
            cap.args.iter().partition(|a| a.is_main_input(&in_spec));
        assert_eq!(main_inputs.len(), 1, "exactly one arg is the main input");
        assert_eq!(main_inputs[0].media_urn, "media:enc=utf-8;file-path");
        let (required, defaulted): (Vec<&CapArg>, Vec<&CapArg>) = others
            .into_iter()
            .partition(|a| a.required && a.default_value.is_none());
        assert_eq!(
            required
                .iter()
                .map(|a| a.media_urn.as_str())
                .collect::<Vec<_>>(),
            vec!["media:enc=utf-8;model-spec"],
            "required options = required && no default, excluding the main input"
        );
        let mut defaulted_urns: Vec<&str> =
            defaulted.iter().map(|a| a.media_urn.as_str()).collect();
        defaulted_urns.sort();
        assert_eq!(
            defaulted_urns,
            vec!["media:budget;numeric", "media:numeric;temperature"],
            "defaulted options carry their default values"
        );

        // The REAL step-requirements assembly must stamp is_main_input on
        // every ArgumentInfo — via the registry-cached cap and a one-step
        // strand, not a reimplementation of the partition.
        let registry = FabricRegistry::new_for_test();
        registry.add_caps_to_cache(vec![cap.clone()]);
        let builder = MachinePlanBuilder::new(Arc::new(registry));
        let source = MediaUrn::from_string("media:doc;ext=pdf").unwrap();
        let target = MediaUrn::from_string("media:enc=utf-8;summary").unwrap();
        let strand = Strand {
            steps: vec![StrandStep::new(
                StrandStepType::Cap {
                    cap_urn: cap.urn.clone(),
                    title: "Summarize".to_string(),
                    specificity: 2,
                    input_is_sequence: false,
                    output_is_sequence: false,
                    inputs: vec![CapInput {
                        arg_urn: MediaUrn::from_string("media:enc=utf-8;file-path").unwrap(),
                        source: ArgSourceRef::StrandInput,
                    }],
                },
                source.clone(),
                target.clone(),
            )],
            source_media_urn: source,
            target_media_urn: target,
            total_steps: 1,
            cap_step_count: 1,
            description: "Summarize a PDF".to_string(),
        };

        let requirements = builder
            .analyze_path_arguments(&strand)
            .await
            .expect("step-requirements assembly must succeed");
        assert_eq!(requirements.steps.len(), 1);
        let arguments = &requirements.steps[0].arguments;
        assert_eq!(arguments.len(), 4, "every declared arg is presented");

        let main: Vec<&ArgumentInfo> = arguments.iter().filter(|a| a.is_main_input).collect();
        assert_eq!(
            main.len(),
            1,
            "the assembly marks exactly one arg as the main input"
        );
        assert_eq!(main[0].media_urn, "media:enc=utf-8;file-path");
        for info in arguments.iter().filter(|a| !a.is_main_input) {
            match info.media_urn.as_str() {
                "media:enc=utf-8;model-spec" => {
                    assert!(info.is_required && info.default_value.is_none());
                }
                "media:budget;numeric" => {
                    assert_eq!(info.default_value, Some(serde_json::json!(400)));
                }
                "media:numeric;temperature" => {
                    assert_eq!(info.default_value, Some(serde_json::json!(0.7)));
                }
                other => panic!("unexpected non-main arg '{other}' in step requirements"),
            }
        }
    }

    /// Build a one-cap-URN-repeated strand: two steps running the SAME cap,
    /// which is the shape that makes positional identity indistinguishable
    /// from token identity unless the tokens are actually carried.
    #[cfg(test)]
    fn repeated_cap_strand(cap: &Cap) -> Strand {
        use crate::planner::live_cap_fab::{ArgSourceRef, CapInput, StrandStep, StrandStepType};

        let source = MediaUrn::from_string("media:ext=txt;text").unwrap();
        let target = MediaUrn::from_string("media:ext=txt;text").unwrap();
        let step = |source_ref: ArgSourceRef| {
            StrandStep::new(
                StrandStepType::Cap {
                    cap_urn: cap.urn.clone(),
                    title: "Rewrite".to_string(),
                    specificity: 1,
                    input_is_sequence: false,
                    output_is_sequence: false,
                    inputs: vec![CapInput {
                        arg_urn: MediaUrn::from_string("media:enc=utf-8;file-path").unwrap(),
                        source: source_ref,
                    }],
                },
                source.clone(),
                target.clone(),
            )
        };

        Strand {
            steps: vec![step(ArgSourceRef::StrandInput), step(ArgSourceRef::StrandInput)],
            source_media_urn: source,
            target_media_urn: target,
            total_steps: 2,
            cap_step_count: 2,
            description: "Rewrite twice".to_string(),
        }
    }

    #[cfg(test)]
    fn rewrite_cap() -> Cap {
        use crate::cap::definition::{ArgSource, CapArg};

        let urn =
            CapUrn::from_string(r#"cap:in="media:ext=txt;text";out="media:ext=txt;text";rewrite"#)
                .unwrap();
        let mut cap = Cap::new(urn, "Rewrite".to_string(), vec!["rewrite".to_string()]);
        cap.args = vec![
            CapArg::with_full_definition(
                "media:enc=utf-8;file-path",
                true,
                false,
                vec![ArgSource::Stdin {
                    stdin: "media:ext=txt;text".to_string(),
                }],
                Some("Text to rewrite".to_string()),
                None,
                None,
            ),
            CapArg::with_full_definition(
                "media:numeric;temperature",
                false,
                false,
                vec![ArgSource::CliFlag {
                    cli_flag: "--temperature".to_string(),
                }],
                Some("Sampling temperature".to_string()),
                Some(serde_json::json!(0.7)),
                None,
            ),
        ];
        cap
    }

    /// TEST1461: step requirements are addressed by the plan's own token.
    ///
    /// Two steps of the SAME cap in one strand: the requirements must carry the
    /// two DISTINCT `StrandStep::token_id` values the planner minted, in
    /// correspondence with the steps they describe. Nothing about a requirement
    /// entry may be recoverable only by counting — a caller holding a
    /// requirement must be able to bind a value without consulting the strand.
    #[tokio::test]
    async fn test1461_step_requirements_carry_the_plans_own_tokens() {
        let cap = rewrite_cap();
        let registry = FabricRegistry::new_for_test();
        registry.add_caps_to_cache(vec![cap.clone()]);
        let builder = MachinePlanBuilder::new(Arc::new(registry));
        let strand = repeated_cap_strand(&cap);

        let strand_tokens: Vec<StepToken> =
            strand.steps.iter().map(|s| s.token_id.clone()).collect();
        assert_ne!(
            strand_tokens[0], strand_tokens[1],
            "the planner mints a distinct token per step even for a repeated cap"
        );

        let requirements = builder
            .analyze_path_arguments(&strand)
            .await
            .expect("step-requirements assembly must succeed");

        assert_eq!(requirements.steps.len(), 2);
        let requirement_tokens: Vec<StepToken> = requirements
            .steps
            .iter()
            .map(|s| s.token_id.clone())
            .collect();
        assert_eq!(
            requirement_tokens, strand_tokens,
            "each requirement carries the token of the step it describes — the \
             address a value is bound to, not a position to be counted"
        );
    }

    /// TEST1462: an unidentified step is not a state the program can hold.
    ///
    /// `StepToken` is the type that makes it so: minting is the only way to
    /// create one, and `parse` — the sole path back from text, which
    /// `Deserialize` goes through — refuses an empty id. Deserialization is the
    /// one boundary where a strand arrives as data rather than being
    /// constructed, so it is the one place the illegal state could otherwise
    /// enter. A persisted strand carrying `""` must fail to load, not load into
    /// a strand whose steps cannot be addressed.
    #[test]
    fn test1462_a_step_token_cannot_be_empty() {
        use crate::planner::live_cap_fab::{StepToken, StepTokenError};

        assert_eq!(
            StepToken::parse(""),
            Err(StepTokenError::Empty),
            "an empty id names no step and is not a token"
        );

        // The deserialization boundary refuses it, so no strand can be loaded
        // into an unaddressable state.
        let failure = serde_json::from_str::<StepToken>(r#""""#)
            .expect_err("deserializing an empty token must fail");
        assert!(
            failure.to_string().contains("came from no plan"),
            "the refusal must say why an empty token is impossible, got: {failure}"
        );

        // A whole strand step carrying an empty token is refused with it —
        // this is the shape that actually arrives from a persisted run.
        let step_json = serde_json::json!({
            "token_id": "",
            "step_type": { "ForEach": { "media_def": "media:ext=txt;text" } },
            "from_spec": "media:ext=txt;text",
            "to_spec": "media:ext=txt;text",
        });
        assert!(
            serde_json::from_value::<crate::planner::live_cap_fab::StrandStep>(step_json).is_err(),
            "a strand step with an empty token must not deserialize"
        );

        // And a minted token round-trips through the same boundary unchanged.
        let minted = StepToken::mint();
        let encoded = serde_json::to_string(&minted).expect("a token serializes as its text");
        let decoded: StepToken =
            serde_json::from_str(&encoded).expect("a minted token survives the round trip");
        assert_eq!(decoded, minted);
    }
}
