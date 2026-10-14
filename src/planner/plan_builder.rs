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
use super::live_cap_fab::StepToken;
use super::plan::{
    EdgeType, ExecutionNodeType, ForEachSource, MachineNode, MachinePlan, MachinePlanEdge,
};
use super::PlannerError;
use crate::{Cap, FabricRegistry, MediaUrn, MediaValidation};

type PlannerResult<T> = Result<T, PlannerError>;

/// What a run binds to one input of a strand: how many values, and their
/// type — the input's declared type, or one that conforms to it.
#[derive(Debug, Clone, PartialEq)]
pub struct AnchorInput {
    pub cardinality: InputCardinality,
    pub media: MediaUrn,
}

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
    /// Returns true if the arg has a stdin source whose media URN is
    /// equivalent to the cap's in_spec. This means the arg can receive piped
    /// data from the previous cap in a chain, not just a literal file path.
    fn is_file_path_stdin_chainable(cap: &Cap) -> bool {
        let Ok(in_spec) = MediaUrn::from_string(cap.urn.in_spec()) else {
            return false;
        };
        for arg in cap.get_args() {
            let is_file_path = MediaUrn::from_string(&arg.media_urn)
                .map(|urn| urn.is_file_path())
                .unwrap_or(false);
            if !is_file_path {
                continue;
            }
            for source in &arg.sources {
                if let crate::ArgSource::Stdin { stdin } = source {
                    let equivalent = MediaUrn::from_string(stdin)
                        .ok()
                        .is_some_and(|stdin| stdin.is_equivalent(&in_spec).unwrap_or(false));
                    if equivalent {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Build the `MachinePlan` of a resolved `MachineStrand`, for a run.
    ///
    /// Every edge of the strand is one step of the plan; its node id is the
    /// step's token. Where each step runs is `IterationStructure`'s to say
    /// (docs 09 §Iteration), computed here for THIS run — the cardinality the
    /// run binds each input of the strand to is what notation could not know:
    ///
    /// - A step that maps runs in a `ForEach`: the map names every sequence it
    ///   zips (`sources`), each with the `Item` node its body is given, and
    ///   every node that runs once per item (`body`). Steps that map over the
    ///   same sequences in the same place share one map; a step given a value a
    ///   map produces joins it; a map opened inside a body is listed in that
    ///   body.
    /// - Several nodes bound to one sequence arg are a gather: a `Collect`.
    /// - Every output of a step other than its main one that something takes
    ///   is a `CapOutput` node.
    /// - A constant is the step's binding for that arg; nothing is wired.
    /// - A step whose cap takes no input (`in=media:void`) has no main-input
    ///   edge; a strand with no inputs has no `InputSlot`, one with no outputs
    ///   no `Output`.
    ///
    /// An edge between two nodes says what is delivered, not how often: a node
    /// inside a map, read from outside it, is the sequence of its values over
    /// the runs. The executor reads that from where the two nodes are.
    pub async fn build_plan_from_machine_strand(
        &self,
        name: &str,
        strand: &crate::machine::graph::MachineStrand,
        input_cardinality: InputCardinality,
    ) -> PlannerResult<MachinePlan> {
        // Every input bound alike, each to its declared type.
        let inputs: HashMap<crate::machine::graph::NodeId, AnchorInput> = strand
            .input_anchor_ids()
            .iter()
            .map(|&anchor| {
                (
                    anchor,
                    AnchorInput {
                        cardinality: input_cardinality,
                        media: strand.node_urn(anchor).clone(),
                    },
                )
            })
            .collect();
        self.build_plan_from_machine_strand_for_run(name, strand, &inputs).await
    }

    /// [`build_plan_from_machine_strand`] for a run that says, per input, how
    /// many values it binds and what type they are: a 3-file input feeding a
    /// scalar arg opens a map while a 1-file input beside it does not, and a
    /// step whose output follows its input (an `effect` that passes it
    /// through or patches it) produces the type the run actually gives it.
    /// The map must cover every input; a missing one is a hard error (a run
    /// never guesses), and a type that does not conform to the input's
    /// declared one is refused.
    pub async fn build_plan_from_machine_strand_for_run(
        &self,
        name: &str,
        strand: &crate::machine::graph::MachineStrand,
        inputs: &HashMap<crate::machine::graph::NodeId, AnchorInput>,
    ) -> PlannerResult<MachinePlan> {
        use crate::machine::graph::{BindingSource, EdgeAssignmentBinding, NodeId};
        use crate::machine::iteration::{IterationStructure, MemberShape, ScopeId, StepShape, ROOT};

        let mut plan = MachinePlan::new(name);
        let caps =
            self.fabric_registry.get_cached_caps().await.map_err(|e| {
                PlannerError::FabricRegistryError(format!("Failed to get caps: {e}"))
            })?;
        let void_media = MediaUrn::from_string(crate::urn::media_urn::MEDIA_VOID)
            .expect("MEDIA_VOID is a valid MediaUrn");

        // The media each strand node holds in this run, and the plan node that
        // holds its value.
        let mut node_runtime: HashMap<NodeId, MediaUrn> = HashMap::new();
        let mut producer: HashMap<NodeId, String> = HashMap::new();

        let input_anchors: Vec<NodeId> = strand.input_anchor_ids().to_vec();
        if let Some(stray) = inputs.keys().find(|node| !input_anchors.contains(node)) {
            return Err(PlannerError::InvalidInput(format!(
                "node {stray} is not an input of the strand; a run binds inputs only"
            )));
        }
        for &anchor in &input_anchors {
            let declared = strand.node_urn(anchor);
            let given = inputs.get(&anchor).ok_or_else(|| {
                PlannerError::InvalidInput(format!(
                    "input anchor {anchor} ('{declared}') is not bound by the run — a run \
                     never guesses; supply one entry per input anchor"
                ))
            })?;
            if !given.media.conforms_to(declared).map_err(|e| {
                PlannerError::InvalidInput(format!(
                    "comparing '{}' with input anchor {anchor}'s type '{declared}': {e}",
                    given.media
                ))
            })? {
                return Err(PlannerError::InvalidInput(format!(
                    "the run binds '{}' to input anchor {anchor}, which takes '{declared}'",
                    given.media
                )));
            }
            let media = given.media.clone();
            let cardinality = given.cardinality;
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

        // Each step's cap definition, and the arg each of its bindings names.
        let mut step_caps: Vec<&Cap> = Vec::with_capacity(edges.len());
        for edge in edges {
            let cap_urn_str = edge.cap_urn.to_string();
            let cap = caps
                .iter()
                .find(|c| c.urn.to_string() == cap_urn_str)
                .ok_or_else(|| {
                    PlannerError::NotFound(format!("cap '{cap_urn_str}' not in registry cache"))
                })?;
            step_caps.push(cap);
        }
        let arg_of = |cap: &'_ Cap, binding: &EdgeAssignmentBinding| -> PlannerResult<usize> {
            cap.args
                .iter()
                .position(|arg| {
                    MediaUrn::from_string(&arg.media_urn)
                        .map(|urn| urn.is_equivalent(&binding.cap_arg_media_urn).unwrap_or(false))
                        .unwrap_or(false)
                })
                .ok_or_else(|| {
                    PlannerError::InvalidPath(format!(
                        "cap '{}': a binding names arg '{}', which is not in the cap definition",
                        cap.urn, binding.cap_arg_media_urn
                    ))
                })
        };

        // Where every step runs, for this run.
        let mut shapes: Vec<StepShape> = Vec::with_capacity(edges.len());
        for (edge, cap) in edges.iter().zip(&step_caps) {
            let mut members = Vec::new();
            for binding in &edge.assignment {
                if let Some(node) = binding.node() {
                    members.push(MemberShape {
                        node,
                        arg_is_sequence: cap.args[arg_of(cap, binding)?].is_sequence,
                    });
                }
            }
            shapes.push(StepShape {
                members,
                outputs: edge
                    .outputs
                    .iter()
                    .map(|output| (output.node, strand.node_is_sequence(output.node)))
                    .collect(),
            });
        }
        let iteration = IterationStructure::of(&shapes, |node| {
            matches!(
                inputs.get(&node).map(|input| input.cardinality),
                Some(InputCardinality::Sequence)
            )
        })
        .map_err(|cyclic| {
            PlannerError::InvalidPath(format!(
                "strand '{name}' has steps that wait on one another (steps {:?})",
                cyclic.0
            ))
        })?;

        // One `ForEach` per map. Its item nodes are made when the first step
        // that runs in it is emitted (every source has its plan node by then);
        // its body is filled as steps are placed in it.
        let fe_id = |scope: ScopeId| -> String {
            format!(
                "foreach_{}",
                iteration
                    .opened_by(scope)
                    .expect("a map is opened by a step")
            )
        };
        let mut bodies: Vec<Vec<String>> = vec![Vec::new(); iteration.scope_count()];
        let mut items: HashMap<(ScopeId, NodeId), String> = HashMap::new();
        let mut map_sources: Vec<Vec<ForEachSource>> = vec![Vec::new(); iteration.scope_count()];

        // Emit steps in data-flow order: a step is ready once every node it is
        // given has its plan node. (A constant needs none.)
        let mut emitted = vec![false; edges.len()];
        let mut remaining = edges.len();
        while remaining > 0 {
            let next = edges.iter().enumerate().position(|(i, e)| {
                !emitted[i] && e.source_nodes().all(|node| producer.contains_key(&node))
            });
            let Some(i) = next else {
                return Err(PlannerError::InvalidPath(format!(
                    "strand '{name}' has cap edges unreachable from its input anchor(s) \
                     (disconnected component or cycle)"
                )));
            };
            let edge = &edges[i];
            let cap = step_caps[i];
            let cap_urn_str = edge.cap_urn.to_string();
            let placement = &iteration.steps()[i];
            let scope = placement.scope();

            // The MAIN input is the binding feeding the cap's stdin argument: it
            // threads the runtime media. A cap whose `in=` is `media:void` has
            // none: it is a source, started by nothing but its own args.
            let in_spec_urn = MediaUrn::from_string(cap.urn.in_spec()).map_err(|e| {
                PlannerError::InvalidPath(format!(
                    "cap '{cap_urn_str}' in= URN '{}' invalid: {e}",
                    cap.urn.in_spec()
                ))
            })?;
            let takes_input = !in_spec_urn.is_equivalent(&void_media).unwrap_or(false);
            let main_arg_index = if takes_input {
                Some(
                    cap.args
                        .iter()
                        .position(|a| a.is_main_input(&in_spec_urn))
                        .ok_or_else(|| {
                            PlannerError::InvalidPath(format!(
                                "cap '{cap_urn_str}' declares no main-input arg (none whose declared \
                                 URN or stdin source is its in=)"
                            ))
                        })?,
                )
            } else {
                None
            };

            // The map this step runs in, when it is the first to: make the
            // map's item nodes.
            if let Some(map) = placement.map {
                for &source in iteration.sources(map) {
                    if items.contains_key(&(map, source)) {
                        continue;
                    }
                    let source_plan_node = producer.get(&source).cloned().ok_or_else(|| {
                        PlannerError::Internal(format!("mapped source node {source} has no producer"))
                    })?;
                    let media = node_runtime.get(&source).cloned().ok_or_else(|| {
                        PlannerError::Internal(format!("no runtime media at mapped source node {source}"))
                    })?;
                    let id = fe_id(map);
                    let item_id = format!("{id}_item_{}", map_sources[map].len());
                    plan.add_node(MachineNode::item(
                        &item_id,
                        &id,
                        &source_plan_node,
                        &media.to_string(),
                    ));
                    bodies[map].push(item_id.clone());
                    map_sources[map].push(ForEachSource {
                        node: source_plan_node,
                        item: item_id.clone(),
                    });
                    items.insert((map, source), item_id);
                }
            }

            // The plan node a member is read from: the item of the map that
            // iterates it, when the step is inside one and the arg takes one
            // value; the node that holds it otherwise (whole — or, read from
            // outside the map it is in, the sequence the map collected).
            let read_from = |node: NodeId, arg_is_sequence: bool| -> PlannerResult<String> {
                if !arg_is_sequence {
                    if let Some(map) = iteration.item_map(node, scope) {
                        return items.get(&(map, node)).cloned().ok_or_else(|| {
                            PlannerError::Internal(format!(
                                "map {map} has no item node for source {node}"
                            ))
                        });
                    }
                }
                producer.get(&node).cloned().ok_or_else(|| {
                    PlannerError::Internal(format!("source node {node} has no producer"))
                })
            };

            // The step's NODE bindings per arg. An arg with several is a
            // GATHER, which only an arg that takes a sequence may be.
            let mut arg_groups: Vec<(usize, Vec<NodeId>)> = Vec::new();
            for binding in &edge.assignment {
                let Some(node) = binding.node() else { continue };
                let index = arg_of(cap, binding)?;
                match arg_groups.iter_mut().find(|(arg, _)| *arg == index) {
                    Some((_, nodes)) => nodes.push(node),
                    None => arg_groups.push((index, vec![node])),
                }
            }
            let main_is_constant = main_arg_index.is_some_and(|main| {
                edge.assignment.iter().any(|binding| {
                    binding.node().is_none()
                        && arg_of(cap, binding).map_or(false, |index| index == main)
                })
            });
            let main_group: Option<&Vec<NodeId>> = main_arg_index.and_then(|main| {
                arg_groups
                    .iter()
                    .find(|(arg, _)| *arg == main)
                    .map(|(_, nodes)| nodes)
            });
            if takes_input && main_group.is_none() && !main_is_constant {
                return Err(PlannerError::InvalidPath(format!(
                    "strand '{name}': cap '{cap_urn_str}' has no source bound to its main input"
                )));
            }

            // The cap node's id IS the strand step's stable identity (the step
            // token minted at parse and threaded through to the run graph):
            // argument values and progress are keyed by it, never by a
            // position that shifts when the plan changes.
            let cap_node_id = edge.token_id.to_string();

            // What each arg is wired from: one node, or a gather of several.
            // `(arg index, plan node)`.
            let mut wired: Vec<(usize, String)> = Vec::new();
            let mut main_media: Option<MediaUrn> = None;
            let mut main_from_anchor = false;
            for (group_idx, (arg_index, nodes)) in arg_groups.iter().enumerate() {
                let arg = &cap.args[*arg_index];
                let is_main = Some(*arg_index) == main_arg_index;
                let media_of = |node: NodeId| {
                    node_runtime.get(&node).cloned().ok_or_else(|| {
                        PlannerError::Internal(format!("no runtime media at source node {node}"))
                    })
                };
                // One node on an arg is wired as it is — unless the arg takes a
                // sequence and the node is one value there: it is then delivered
                // as a sequence of one, which is a gather of one.
                let wired_as_it_is = nodes.len() == 1
                    && (!arg.is_sequence || iteration.is_sequence_from(nodes[0], scope));
                let (from, media) = if wired_as_it_is {
                    (read_from(nodes[0], arg.is_sequence)?, media_of(nodes[0])?)
                } else {
                    if !arg.is_sequence {
                        return Err(PlannerError::InvalidPath(format!(
                            "strand '{name}': cap '{cap_urn_str}' arg '{}' carries {} bindings \
                             but is not a sequence arg — a gather is only legal into a sequence \
                             arg (resolver invariant violated)",
                            arg.media_urn,
                            nodes.len()
                        )));
                    }
                    let mut members: Vec<String> = Vec::with_capacity(nodes.len());
                    let mut member_media: Vec<MediaUrn> = Vec::with_capacity(nodes.len());
                    for node in nodes {
                        members.push(read_from(*node, true)?);
                        member_media.push(media_of(*node)?);
                    }
                    // The gathered sequence's element type is the join of the
                    // members' — the most specific type every member conforms to.
                    let item_media = MediaUrn::least_upper_bound(&member_media);
                    let collect_id = if is_main {
                        format!("collect_{}", edge.token_id)
                    } else {
                        format!("collect_{}_{group_idx}", edge.token_id)
                    };
                    plan.add_node(MachineNode::collect(
                        &collect_id,
                        members.clone(),
                        &item_media.to_string(),
                    ));
                    for member in &members {
                        plan.add_edge(MachinePlanEdge::collection(member, &collect_id));
                    }
                    if scope != ROOT {
                        bodies[scope].push(collect_id.clone());
                    }
                    (collect_id, item_media)
                };
                if is_main {
                    main_media = Some(media);
                    main_from_anchor = nodes.len() == 1 && input_anchors.contains(&nodes[0]);
                }
                wired.push((*arg_index, from));
            }
            // A source: nothing is wired into its main input, and the type its
            // effect is applied to is its declared input.
            let in_media = main_media.unwrap_or_else(|| in_spec_urn.clone());
            let main_from: Option<String> = main_arg_index.and_then(|main| {
                wired
                    .iter()
                    .find(|(arg, _)| *arg == main)
                    .map(|(_, from)| from.clone())
            });

            // Bindings: what the step's args are given other than by a wire.
            let mut bindings = ArgumentBindings::new();
            if let (Some(arg_name), Some(main_from)) = (Self::find_file_path_arg(cap), &main_from) {
                let chainable = Self::is_file_path_stdin_chainable(cap);
                if main_from_anchor || !chainable {
                    bindings.add_file_path(&arg_name);
                } else {
                    bindings.add(
                        arg_name.clone(),
                        ArgumentBinding::PreviousOutput {
                            node_id: main_from.clone(),
                        },
                    );
                }
            }
            // Constants written in the machine: the arg's value, bound here,
            // keyed by the URN the arg's value is delivered under. An arg so
            // bound takes no value from the run.
            for binding in &edge.assignment {
                if let BindingSource::Constant(value) = &binding.source {
                    let arg = &cap.args[arg_of(cap, binding)?];
                    bindings.add(
                        arg.stream_urn().to_string(),
                        ArgumentBinding::Literal {
                            value: value.clone(),
                        },
                    );
                }
            }
            for (index, arg) in cap.get_args().iter().enumerate() {
                // The main input is fed by a wire or, for a source, by nothing.
                if Some(index) == main_arg_index {
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
                // An arg a node is wired into is delivered by that wire, and an
                // arg bound to a constant has its value: neither is asked for.
                if wired.iter().any(|(arg, _)| *arg == index)
                    || bindings.bindings.contains_key(arg.stream_urn())
                    || bindings.bindings.contains_key(&arg.media_urn)
                {
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

            let mut cap_node = MachineNode::cap_with_bindings_token(
                &cap_node_id,
                &cap_urn_str,
                bindings,
                edge.token_id.clone(),
            );
            if let ExecutionNodeType::Cap { policy, .. } = &mut cap_node.node_type {
                *policy = edge.policy;
            }
            plan.add_node(cap_node);
            if scope != ROOT {
                bodies[scope].push(cap_node_id.clone());
            }

            // The wires: the main input is a `Direct` edge, every other arg an
            // `Arg` edge naming the URN its stream is delivered under.
            for (arg_index, from) in &wired {
                if Some(*arg_index) == main_arg_index {
                    plan.add_edge(MachinePlanEdge::direct(from, &cap_node_id));
                } else {
                    plan.add_edge(MachinePlanEdge::arg(
                        from,
                        &cap_node_id,
                        cap.args[*arg_index].stream_urn(),
                    ));
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
            // Every output the wiring takes is a value this step produces: the
            // main one at the step's own node, holding the type the cap's
            // effect derives; another at a node of its own, holding its own
            // type — or the input's, when it carries the input.
            let declared_out = edge.cap_urn.out_media_urn().map_err(|e| {
                PlannerError::InvalidPath(format!("cap '{cap_urn_str}' out= URN invalid: {e}"))
            })?;
            for output in &edge.outputs {
                let is_main = output
                    .output_media_urn
                    .is_equivalent(&declared_out)
                    .unwrap_or(false);
                if is_main {
                    node_runtime.insert(output.node, out_media.clone());
                    producer.insert(output.node, cap_node_id.clone());
                    continue;
                }
                let declared = cap
                    .output_by_urn(&output.output_media_urn.to_string())
                    .ok_or_else(|| {
                        PlannerError::InvalidPath(format!(
                            "cap '{cap_urn_str}': the wiring takes output '{}', which is not in \
                             the cap definition",
                            output.output_media_urn
                        ))
                    })?;
                let carries_input = declared.carries.is_some();
                let output_node_id = format!("{cap_node_id}.{}", declared.name);
                plan.add_node(MachineNode::cap_output(
                    &output_node_id,
                    &cap_node_id,
                    &declared.media_urn,
                    &declared.name,
                    carries_input,
                ));
                plan.add_edge(MachinePlanEdge::direct(&cap_node_id, &output_node_id));
                if scope != ROOT {
                    bodies[scope].push(output_node_id.clone());
                }
                node_runtime.insert(
                    output.node,
                    if carries_input {
                        in_media.clone()
                    } else {
                        output.output_media_urn.clone()
                    },
                );
                producer.insert(output.node, output_node_id);
            }
            emitted[i] = true;
            remaining -= 1;
        }

        // The maps, now that every body is known. A map opened inside another
        // is a node of that other's body.
        for map in 1..iteration.scope_count() {
            let parent = iteration.parent(map);
            if parent != ROOT {
                bodies[parent].push(fe_id(map));
            }
        }
        for map in 1..iteration.scope_count() {
            let id = fe_id(map);
            let opener = &edges[iteration.opened_by(map).expect("a map is opened by a step")];
            // The map's identity is its opening step's: the one the strand
            // already carries when notation derives the map, minted here when
            // only this run's input cardinality brings it about.
            let token_id = match (&opener.foreach_token_id, opener.is_loop) {
                (Some(token), _) => token.clone(),
                (None, false) => StepToken::mint(),
                (None, true) => {
                    return Err(PlannerError::InvalidPath(format!(
                        "strand '{name}': loop edge for cap '{}' has no ForEach identity",
                        opener.cap_urn
                    )));
                }
            };
            for source in &map_sources[map] {
                plan.add_edge(MachinePlanEdge::direct(&source.node, &id));
                plan.add_edge(MachinePlanEdge::iteration(&id, &source.item));
            }
            plan.add_node(MachineNode::for_each_token(
                &id,
                map_sources[map].clone(),
                bodies[map].clone(),
                token_id,
            ));
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

/// Where a step's argument gets its value in a run of a plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArgumentResolution {
    /// Wired from an input of the machine (through a map's item or a gather
    /// of inputs, when it is reached through one).
    FromInput,
    /// Wired from what another step produces.
    FromStep,
    /// Bound to a constant the machine writes.
    Constant,
    /// Not bound by the machine; its definition gives a default.
    HasDefault,
    /// Not bound by the machine and no default: the run supplies it.
    RequiresUserInput,
}

impl ArgumentResolution {
    /// Whether the machine itself binds the arg. A bound arg takes no value
    /// from the run (09 §Who owns an arg's value).
    pub fn is_bound(&self) -> bool {
        matches!(self, Self::FromInput | Self::FromStep | Self::Constant)
    }
}

/// Information about a single argument for UI presentation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArgumentInfo {
    /// The arg's name in its cap definition: a label, unique within the cap.
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

/// Argument requirements for a single step of a plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepArgumentRequirements {
    /// Cap URN for this step
    pub cap_urn: String,
    /// The token of the plan's cap node these requirements describe — the
    /// ONLY address an argument value is ever bound to.
    ///
    /// A token exists because a plan exists. It is never derived from a
    /// position, a notation string, or anything else: the plan carries it and
    /// everything downstream carries it unchanged. Positions cannot address a
    /// step because a plan is a DAG — parallel branches merging downstream
    /// have no ordinal, and two identical caps on separate branches are
    /// distinguishable only by token.
    pub token_id: StepToken,
    /// Cap title
    pub title: String,
    /// All arguments for this cap with their resolution status
    pub arguments: Vec<ArgumentInfo>,
    /// The arguments the machine does not bind: the ones a run may give a
    /// value (and must, when they have no default).
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

/// Argument requirements for a whole plan: one entry per cap node, in the
/// plan's topological order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanArgumentRequirements {
    /// Requirements for each step
    pub steps: Vec<StepArgumentRequirements>,
    /// Whether a run of the plan needs no value from the user: every arg the
    /// machine leaves unbound has a default.
    pub can_execute_without_input: bool,
}

impl MachinePlanBuilder {
    /// What a run of `plan` binds and what it leaves to the run, step by step.
    ///
    /// An arg is bound when a wire feeds it (a `Direct` edge into the step's
    /// main input, an `Arg` edge naming the arg's stream) or a binding gives
    /// it a value the machine wrote (a constant) or the plan derived (a file
    /// path of the input). Every other arg is the run's: it is a slot, with
    /// its default when the definition declares one.
    pub async fn analyze_plan_arguments(
        &self,
        plan: &MachinePlan,
    ) -> PlannerResult<PlanArgumentRequirements> {
        let caps =
            self.fabric_registry.get_cached_caps().await.map_err(|e| {
                PlannerError::FabricRegistryError(format!("Failed to get caps: {}", e))
            })?;
        let order = plan.topological_order().map_err(|e| {
            PlannerError::InvalidPath(format!("plan '{}' is not a DAG: {e}", plan.name))
        })?;

        let mut step_requirements = Vec::new();
        for node in order {
            let ExecutionNodeType::Cap {
                token_id,
                cap_urn: cap_urn_str,
                arg_bindings,
                ..
            } = &node.node_type
            else {
                continue;
            };
            // The plan names the registry cap it was built from, in the
            // registry's own spelling: the definition is that cap.
            let cap = caps
                .iter()
                .find(|c| c.urn.to_string() == *cap_urn_str)
                .ok_or_else(|| {
                    PlannerError::NotFound(format!("Cap '{}' not found in registry", cap_urn_str))
                })?;
            let in_spec = cap.urn.in_spec();
            // Main-input identification is a tagged-URN-equivalence question,
            // never a string comparison.
            let in_spec_urn = MediaUrn::from_string(in_spec).map_err(|e| {
                PlannerError::InvalidPath(format!(
                    "cap '{cap_urn_str}' in= URN '{in_spec}' invalid: {e}"
                ))
            })?;

            let mut arguments = Vec::new();
            let mut slots = Vec::new();
            for arg in cap.get_args() {
                let is_main_input = arg.is_main_input(&in_spec_urn);
                let wire = plan.edges.iter().find(|edge| {
                    edge.to_node == node.id
                        && match &edge.edge_type {
                            EdgeType::Direct => is_main_input,
                            EdgeType::Arg { arg_urn } => arg_urn == arg.stream_urn(),
                            EdgeType::Iteration | EdgeType::Collection => false,
                        }
                });
                let binding = arg_bindings
                    .bindings
                    .get(arg.stream_urn())
                    .or_else(|| arg_bindings.bindings.get(&arg.media_urn));
                let resolution = match (wire, binding) {
                    (Some(edge), _) => {
                        if Self::fed_by_inputs_only(plan, &edge.from_node)? {
                            ArgumentResolution::FromInput
                        } else {
                            ArgumentResolution::FromStep
                        }
                    }
                    (None, Some(ArgumentBinding::Literal { .. })) => ArgumentResolution::Constant,
                    (
                        None,
                        Some(
                            ArgumentBinding::InputFile { .. }
                            | ArgumentBinding::InputFilePath
                            | ArgumentBinding::InputMediaUrn,
                        ),
                    ) => ArgumentResolution::FromInput,
                    (None, Some(ArgumentBinding::PreviousOutput { .. })) => {
                        ArgumentResolution::FromStep
                    }
                    (
                        None,
                        None
                        | Some(
                            ArgumentBinding::Slot { .. }
                            | ArgumentBinding::CapDefault
                            | ArgumentBinding::CapSetting { .. }
                            | ArgumentBinding::PlanMetadata { .. },
                        ),
                    ) => {
                        if arg.default_value.is_some() {
                            ArgumentResolution::HasDefault
                        } else {
                            ArgumentResolution::RequiresUserInput
                        }
                    }
                };

                // Validation comes from the media definition via the registry.
                let resolved_spec =
                    crate::media::spec::resolve_media_urn(&arg.media_urn, &self.fabric_registry)
                        .await
                        .ok();
                let validation = resolved_spec.and_then(|spec| spec.validation);

                let arg_info = ArgumentInfo {
                    name: arg.name.clone(),
                    media_urn: arg.media_urn.clone(),
                    description: arg.arg_description.clone().unwrap_or_default(),
                    resolution: resolution.clone(),
                    default_value: arg.default_value.clone(),
                    is_required: arg.required,
                    is_sequence: arg.is_sequence,
                    is_main_input,
                    validation: Self::validation_to_json(validation.as_ref()),
                };
                if !resolution.is_bound() {
                    slots.push(arg_info.clone());
                }
                arguments.push(arg_info);
            }

            step_requirements.push(StepArgumentRequirements {
                cap_urn: cap_urn_str.clone(),
                token_id: token_id.clone(),
                title: cap.title.clone(),
                arguments,
                slots,
                supported_model_types: cap.supported_model_types.clone(),
                default_model_spec: cap.default_model_spec.clone(),
            });
        }

        // A run can go without the user when nothing it must have is
        // missing: an optional arg left without a value is not given one.
        let can_execute_without_input = step_requirements.iter().all(|step| {
            step.slots.iter().all(|slot| {
                !(slot.is_required && slot.resolution == ArgumentResolution::RequiresUserInput)
            })
        });
        Ok(PlanArgumentRequirements {
            steps: step_requirements,
            can_execute_without_input,
        })
    }

    /// Whether the value at `node` comes from the machine's inputs alone:
    /// an input slot, an item of a map over one, or a gather of such values.
    fn fed_by_inputs_only(plan: &MachinePlan, node: &str) -> PlannerResult<bool> {
        let held = plan.get_node(node).ok_or_else(|| {
            PlannerError::InvalidPath(format!("plan '{}' wires from no node '{node}'", plan.name))
        })?;
        match &held.node_type {
            ExecutionNodeType::InputSlot { .. } => Ok(true),
            ExecutionNodeType::Item { source, .. } => Self::fed_by_inputs_only(plan, source),
            ExecutionNodeType::Collect { input_nodes, .. } => {
                for member in input_nodes {
                    if !Self::fed_by_inputs_only(plan, member)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            ExecutionNodeType::Cap { .. }
            | ExecutionNodeType::CapOutput { .. }
            | ExecutionNodeType::ForEach { .. }
            | ExecutionNodeType::Output { .. } => Ok(false),
        }
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
                "media:document;ext=pdf",
                "media:enc=utf-8;file-metadata;record",
                "Extract Metadata",
            )?,
            make_test_cap(
                "extract_outline",
                "media:document;ext=pdf",
                "media:document-outline;enc=utf-8;record",
                "Extract Outline",
            )?,
            make_test_cap(
                "disbind",
                "media:document;ext=pdf",
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
                "media:document;ext=pdf",
                "media:disbound-pages;enc=utf-8;list",
                "Disbind PDF",
            )?,
            make_test_cap(
                "disbind",
                "media:document;ext=pdf",
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
            err_msg.contains("media:document;ext=pdf"),
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
                "media:document;ext=pdf",
                "media:disbound-pages;enc=utf-8;list",
                "Disbind",
            )?,
            make_test_cap(
                "grind",
                "media:document;ext=pdf",
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
                "media:document;ext=pdf",
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
    // ARGUMENT PRESENTATION TESTS
    // (the plan analysis itself is tested over real plans in
    // orchestrator/machine_plan.rs)
    // ==========================================================================

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
            "cap:extract-metadata;in=\"media:document;ext=pdf\";out=\"media:enc=utf-8;file-metadata;record\"",
        )?;
        let urn2 = CapUrn::from_string(
            "cap:extract-metadata;in=\"media:document;ext=pdf\";out=\"media:enc=utf-8;file-metadata;record\"",
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
            CapUrn::from_string("cap:in=\"media:document;ext=pdf\";extract;out=media:text").unwrap();

        let specific_candidate =
            CapUrn::from_string("cap:in=\"media:document;ext=pdf\";extract;out=media:text;version=2")
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
            CapUrn::from_string("cap:in=\"media:document;ext=pdf\";extract;out=media:text;required=yes")
                .unwrap();

        let candidate = CapUrn::from_string(
            "cap:in=\"media:document;ext=pdf\";extract;out=media:text", // missing required=yes
        )
        .unwrap();

        // candidate is NOT dispatchable for request (missing required tag that request needs)
        assert!(
            !candidate.is_dispatchable(&request),
            "Candidate missing required tag should not be dispatchable for request"
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
