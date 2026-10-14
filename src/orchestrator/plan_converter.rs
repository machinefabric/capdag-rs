//! Steps of a `MachinePlan` as a `ResolvedGraph`.
//!
//! The plan is node-centric: steps, the values they read and produce, the maps
//! they run in. The segment executor runs a [`ResolvedGraph`]: steps with
//! their cap definitions, each saying which nodes its input streams are sent
//! from and which nodes its outputs are stored at. This module is the bridge.
//!
//! A graph holds only steps. What else a plan has is read through them:
//!
//! - an edge into a step is one of its inputs — the cap's `in=` stream for a
//!   `Direct` edge, the named arg's stream for an `Arg` edge;
//! - a `Collect` wired into a step is that input's gather: its members, in
//!   order;
//! - a `CapOutput` of a step is an output of it that something takes.
//!
//! Maps are not in a graph at all: `execute_plan` runs each map's body once
//! per item and asks for the steps of one scope at a time
//! ([`steps_to_resolved_graph`]). A node a graph's steps read and none of them
//! produces is an input of the graph, whatever it is in the plan — an input
//! slot, an item, a step of another scope.

use super::types::{
    ParseOrchestrationError, ResolvedGraph, ResolvedStep, StepInput, StepOutput, StepSource,
};
use crate::cap::registry::FabricRegistry;
use crate::planner::{EdgeType, ExecutionNodeType, MachinePlan};
use std::collections::HashMap;

/// The steps `step_ids` of `plan` (ids of its `Cap` nodes) as a graph.
///
/// Fails when an id is not a step of the plan, when a step's cap is not in
/// the registry cache, or when the plan wires a step in a way no invocation
/// can be given: two sources on one stream, or a map wired straight into a
/// step.
pub fn steps_to_resolved_graph(
    plan: &MachinePlan,
    step_ids: &[&str],
    registry: &FabricRegistry,
) -> Result<ResolvedGraph, ParseOrchestrationError> {
    let invalid = |message: String| ParseOrchestrationError::InvalidGraph { message };
    let mut nodes: HashMap<String, String> = HashMap::new();
    let mut steps: Vec<ResolvedStep> = Vec::with_capacity(step_ids.len());

    // A step's other taken outputs: the `CapOutput` nodes that name it.
    let mut outputs_of: HashMap<&str, Vec<StepOutput>> = HashMap::new();
    let mut output_ids: Vec<&String> = plan.nodes.keys().collect();
    output_ids.sort();
    for id in output_ids {
        if let ExecutionNodeType::CapOutput {
            cap_node,
            output_media_urn,
            ..
        } = &plan.nodes[id].node_type
        {
            outputs_of.entry(cap_node.as_str()).or_default().push(StepOutput {
                output_media_urn: output_media_urn.clone(),
                node: id.clone(),
            });
        }
    }

    for step_id in step_ids {
        let node = plan.nodes.get(*step_id).ok_or_else(|| {
            invalid(format!("'{step_id}' is not a node of plan '{}'", plan.name))
        })?;
        let ExecutionNodeType::Cap {
            token_id,
            cap_urn,
            policy,
            ..
        } = &node.node_type
        else {
            return Err(invalid(format!(
                "'{step_id}' is not a step of plan '{}'",
                plan.name
            )));
        };
        let cap = registry
            .get_cached_cap(cap_urn)
            .ok_or_else(|| ParseOrchestrationError::CapNotFound {
                cap_urn: cap_urn.clone(),
            })?;

        // The streams the step is sent, in the order the plan wires them.
        let mut inputs: Vec<StepInput> = Vec::new();
        for edge in plan.edges.iter().filter(|edge| edge.to_node == *step_id) {
            let stream_urn = match &edge.edge_type {
                EdgeType::Direct => cap.urn.in_spec().to_string(),
                EdgeType::Arg { arg_urn } => arg_urn.clone(),
                other => {
                    return Err(invalid(format!(
                        "step '{step_id}' has an incoming {other:?} edge from '{}' — a step is \
                         wired by its main input and its args only",
                        edge.from_node
                    )));
                }
            };
            let from = plan.nodes.get(&edge.from_node).ok_or_else(|| {
                invalid(format!(
                    "step '{step_id}' is wired from '{}', which is not in the plan",
                    edge.from_node
                ))
            })?;
            let source = match &from.node_type {
                ExecutionNodeType::Collect { input_nodes, .. } => {
                    StepSource::Gather(input_nodes.clone())
                }
                ExecutionNodeType::ForEach { .. } | ExecutionNodeType::Output { .. } => {
                    return Err(invalid(format!(
                        "step '{step_id}' is wired from '{}', which holds no value",
                        edge.from_node
                    )));
                }
                _ => StepSource::Node(edge.from_node.clone()),
            };
            let urn = crate::MediaUrn::from_string(&stream_urn).map_err(|e| {
                invalid(format!(
                    "step '{step_id}': input stream URN '{stream_urn}' is not a media URN: {e}"
                ))
            })?;
            let taken = inputs.iter().any(|input: &StepInput| {
                crate::MediaUrn::from_string(&input.stream_urn)
                    .map(|other| other.is_equivalent(&urn).unwrap_or(false))
                    .unwrap_or(false)
            });
            if taken {
                return Err(invalid(format!(
                    "step '{step_id}' is wired twice on input stream '{stream_urn}' — one \
                     stream has one source; several values into one arg are a gather"
                )));
            }
            inputs.push(StepInput { stream_urn, source });
        }

        let out_media = cap.urn.out_spec().to_string();
        nodes.insert(step_id.to_string(), out_media.clone());
        let outputs = outputs_of.remove(*step_id).unwrap_or_default();
        for output in &outputs {
            nodes.insert(output.node.clone(), output.output_media_urn.clone());
        }
        steps.push(ResolvedStep {
            token_id: token_id.clone(),
            node: step_id.to_string(),
            cap_urn: cap_urn.clone(),
            cap,
            inputs,
            outputs,
            out_media,
            policy: *policy,
        });
    }

    // The nodes the graph is given: what its steps read and none produces.
    for step in &steps {
        for source in step.source_nodes() {
            if nodes.contains_key(source) {
                continue;
            }
            let media = match plan.nodes.get(source).map(|n| &n.node_type) {
                Some(ExecutionNodeType::InputSlot {
                    expected_media_urn, ..
                }) => expected_media_urn.clone(),
                Some(ExecutionNodeType::Item { media_urn, .. }) => media_urn.clone(),
                Some(ExecutionNodeType::CapOutput {
                    output_media_urn, ..
                }) => output_media_urn.clone(),
                Some(ExecutionNodeType::Cap { cap_urn, .. }) => registry
                    .get_cached_cap(cap_urn)
                    .ok_or_else(|| ParseOrchestrationError::CapNotFound {
                        cap_urn: cap_urn.clone(),
                    })?
                    .urn
                    .out_spec()
                    .to_string(),
                other => {
                    return Err(invalid(format!(
                        "a step reads '{source}', which holds no value ({other:?})"
                    )));
                }
            };
            nodes.insert(source.clone(), media);
        }
    }

    Ok(ResolvedGraph {
        nodes,
        steps,
        graph_name: Some(plan.name.clone()),
    })
}

/// Every step of a plan that has no map, as one graph.
///
/// A plan with a map is not one graph: its body runs once per item, which is
/// `execute_plan`'s to do. Asking for it here is refused, naming the map.
pub fn plan_to_resolved_graph(
    plan: &MachinePlan,
    registry: &FabricRegistry,
) -> Result<ResolvedGraph, ParseOrchestrationError> {
    let mut step_ids: Vec<&str> = Vec::new();
    let mut ids: Vec<&String> = plan.nodes.keys().collect();
    ids.sort();
    for id in ids {
        match &plan.nodes[id].node_type {
            ExecutionNodeType::Cap { .. } => step_ids.push(id.as_str()),
            ExecutionNodeType::ForEach { .. } => {
                return Err(ParseOrchestrationError::InvalidGraph {
                    message: format!(
                        "plan '{}' has a map ('{id}'): its body runs once per item and is not \
                         one graph — run the plan with execute_plan",
                        plan.name
                    ),
                });
            }
            _ => {}
        }
    }
    steps_to_resolved_graph(plan, &step_ids, registry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cap::definition::{ArgSource, Cap, CapArg, CapOutput};
    use crate::machine::{OnError, StepPolicy};
    use crate::planner::{ForEachSource, InputCardinality, MachineNode, MachinePlanEdge};
    use crate::urn::cap_urn::CapUrn;

    const EXTRACT: &str = r#"cap:in="media:document;ext=pdf";extract;out="media:enc=utf-8;ext=txt""#;
    const SUMMARIZE: &str = r#"cap:in="media:enc=utf-8;ext=txt";summarize;out="media:enc=utf-8;summary""#;
    const CONCAT: &str = r#"cap:in="media:enc=utf-8";concat;out="media:enc=utf-8;joined""#;
    const SPLIT: &str = r#"cap:in="media:enc=utf-8;ext=txt";split;out="media:enc=utf-8;head""#;
    const NOW: &str = r#"cap:in="media:void";now;out="media:stamp""#;
    const CONTEXT: &str = "media:context;enc=utf-8";
    const REST: &str = "media:enc=utf-8;rest";

    fn cap(urn: &str) -> Cap {
        let cap_urn = CapUrn::from_string(urn).unwrap();
        let in_spec = cap_urn.in_spec().to_string();
        let out_spec = cap_urn.out_spec().to_string();
        let mut cap = Cap::new(cap_urn, "t".to_string(), vec!["t".to_string()]);
        if in_spec != "media:void" {
            cap.args.push(CapArg::new(
                in_spec.clone(),
                "input",
                true,
                vec![ArgSource::Stdin { stdin: in_spec }],
            ));
        }
        cap.outputs = vec![CapOutput::new(out_spec, "output", "out".to_string())];
        cap
    }

    fn registry() -> FabricRegistry {
        let registry = FabricRegistry::new_for_test();
        let mut summarize = cap(SUMMARIZE);
        summarize.args.push(CapArg::new(
            CONTEXT.to_string(),
            "context",
            false,
            vec![ArgSource::CliFlag {
                cli_flag: "--context".to_string(),
            }],
        ));
        let mut concat = cap(CONCAT);
        concat.args[0].is_sequence = true;
        let mut split = cap(SPLIT);
        split
            .outputs
            .push(CapOutput::new(REST.to_string(), "rest", "the rest".to_string()));
        registry.add_caps_to_cache(vec![cap(EXTRACT), summarize, concat, split, cap(NOW)]);
        registry
    }

    /// doc → extract → summarize (context ← now) → out; extract also feeds
    /// split, whose `rest` output is taken; concat gathers summarize and rest.
    fn plan() -> MachinePlan {
        let mut plan = MachinePlan::new("convert");
        plan.add_node(MachineNode::input_slot("doc", "input", "media:document;ext=pdf", InputCardinality::Single));
        plan.add_node(MachineNode::cap("extract", EXTRACT));
        plan.add_node(MachineNode::cap("summarize", SUMMARIZE));
        plan.add_node(MachineNode::cap("now", NOW));
        plan.add_node(MachineNode::cap("split", SPLIT));
        plan.add_node(MachineNode::cap_output("split.rest", "split", REST, "rest", false));
        plan.add_node(MachineNode::collect(
            "both",
            vec!["summarize".to_string(), "split.rest".to_string()],
            "media:enc=utf-8",
        ));
        plan.add_node(MachineNode::cap("concat", CONCAT));
        plan.add_node(MachineNode::output("out", "result", "concat"));
        plan.add_edge(MachinePlanEdge::direct("doc", "extract"));
        plan.add_edge(MachinePlanEdge::direct("extract", "summarize"));
        plan.add_edge(MachinePlanEdge::arg("now", "summarize", CONTEXT));
        plan.add_edge(MachinePlanEdge::direct("extract", "split"));
        plan.add_edge(MachinePlanEdge::direct("split", "split.rest"));
        plan.add_edge(MachinePlanEdge::collection("summarize", "both"));
        plan.add_edge(MachinePlanEdge::collection("split.rest", "both"));
        plan.add_edge(MachinePlanEdge::direct("both", "concat"));
        plan.add_edge(MachinePlanEdge::direct("concat", "out"));
        plan.validate().expect("valid");
        plan
    }

    fn step<'g>(graph: &'g ResolvedGraph, node: &str) -> &'g ResolvedStep {
        graph
            .steps
            .iter()
            .find(|step| step.node == node)
            .unwrap_or_else(|| panic!("no step '{node}'"))
    }

    // TEST1161: every step of a plan becomes one step of the graph, keyed by
    // its node; its main input is the stream its cap's `in=` names and a
    // named arg the stream the edge names; a step nothing is wired into has no
    // inputs; a gather is one input with its members in order; an output of a
    // step that something takes is listed with the node it is stored at.
    #[test]
    fn test1161_a_plan_converts_to_steps_with_their_inputs_and_outputs() {
        let graph = plan_to_resolved_graph(&plan(), &registry()).expect("converts");
        assert_eq!(graph.steps.len(), 5);
        assert_eq!(graph.graph_name.as_deref(), Some("convert"));

        let extract = step(&graph, "extract");
        assert_eq!(
            extract.inputs,
            vec![StepInput {
                stream_urn: "media:document;ext=pdf".to_string(),
                source: StepSource::Node("doc".to_string()),
            }]
        );
        assert!(extract.outputs.is_empty());
        assert_eq!(extract.out_media, "media:enc=utf-8;ext=txt");

        let summarize = step(&graph, "summarize");
        assert_eq!(
            summarize.inputs,
            vec![
                StepInput {
                    stream_urn: "media:enc=utf-8;ext=txt".to_string(),
                    source: StepSource::Node("extract".to_string()),
                },
                StepInput {
                    stream_urn: CONTEXT.to_string(),
                    source: StepSource::Node("now".to_string()),
                },
            ]
        );

        assert!(step(&graph, "now").inputs.is_empty(), "a source is sent no stream");

        let split = step(&graph, "split");
        assert_eq!(
            split.outputs,
            vec![StepOutput {
                output_media_urn: REST.to_string(),
                node: "split.rest".to_string(),
            }]
        );
        assert_eq!(
            split.produced_nodes().cloned().collect::<Vec<_>>(),
            vec!["split".to_string(), "split.rest".to_string()]
        );

        let concat = step(&graph, "concat");
        assert_eq!(
            concat.inputs,
            vec![StepInput {
                stream_urn: "media:enc=utf-8".to_string(),
                source: StepSource::Gather(vec!["summarize".to_string(), "split.rest".to_string()]),
            }]
        );
        assert_eq!(
            concat.source_nodes().cloned().collect::<Vec<_>>(),
            vec!["summarize".to_string(), "split.rest".to_string()]
        );

        // Every node a step reads or produces has its media; the plan's
        // input is there as what the graph is given.
        assert_eq!(graph.nodes["doc"], "media:document;ext=pdf");
        assert_eq!(graph.nodes["split.rest"], REST);
        assert_eq!(graph.nodes["concat"], "media:enc=utf-8;joined");
        assert!(!graph.nodes.contains_key("both"), "a gather is an input, not a node of the graph");
        assert!(!graph.nodes.contains_key("out"));

        let media = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
            pairs.iter().map(|(node, urn)| (node.to_string(), urn.to_string())).collect()
        };
        // A step alone: what it produces has its media whether or not a step
        // of the graph reads it, and what it reads is what the graph is given.
        let alone = steps_to_resolved_graph(&plan(), &["split"], &registry()).expect("converts");
        assert_eq!(
            alone.nodes,
            media(&[
                ("split", "media:enc=utf-8;head"),
                ("split.rest", REST),
                ("extract", "media:enc=utf-8;ext=txt"),
            ])
        );
        // And a step that reads a step's other output from outside the graph
        // is given it under that output's own type.
        let gathering = steps_to_resolved_graph(&plan(), &["concat"], &registry()).expect("converts");
        assert_eq!(
            gathering.nodes,
            media(&[
                ("concat", "media:enc=utf-8;joined"),
                ("summarize", "media:enc=utf-8;summary"),
                ("split.rest", REST),
            ])
        );
    }

    // TEST770: a plan with a map is not one graph, and the refusal names the
    // map; the steps of one scope of it are, with what they read from outside
    // (an item, a step of the enclosing scope) as the graph's inputs.
    #[test]
    fn test770_a_plan_with_a_map_is_run_scope_by_scope() {
        let mut plan = MachinePlan::new("mapped");
        plan.add_node(MachineNode::input_slot("docs", "input", "media:document;ext=pdf", InputCardinality::Sequence));
        plan.add_node(MachineNode::cap("now", NOW));
        plan.add_node(MachineNode::for_each(
            "each",
            vec![ForEachSource { node: "docs".to_string(), item: "doc".to_string() }],
            vec!["doc".to_string(), "extract".to_string(), "summarize".to_string()],
        ));
        plan.add_node(MachineNode::item("doc", "each", "docs", "media:document;ext=pdf"));
        plan.add_node(MachineNode::cap("extract", EXTRACT));
        plan.add_node(MachineNode::cap("summarize", SUMMARIZE));
        plan.add_edge(MachinePlanEdge::direct("docs", "each"));
        plan.add_edge(MachinePlanEdge::iteration("each", "doc"));
        plan.add_edge(MachinePlanEdge::direct("doc", "extract"));
        plan.add_edge(MachinePlanEdge::direct("extract", "summarize"));
        plan.add_edge(MachinePlanEdge::arg("now", "summarize", CONTEXT));
        plan.validate().expect("valid");

        let registry = registry();
        match plan_to_resolved_graph(&plan, &registry) {
            Err(ParseOrchestrationError::InvalidGraph { message }) => {
                assert!(message.contains("has a map ('each')"), "{message}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }

        let body = steps_to_resolved_graph(&plan, &["extract", "summarize"], &registry).expect("the body converts");
        assert_eq!(body.steps.len(), 2);
        assert_eq!(step(&body, "extract").inputs[0].source, StepSource::Node("doc".to_string()));
        assert_eq!(body.nodes["doc"], "media:document;ext=pdf", "the item is what the body is given");
        assert_eq!(body.nodes["now"], "media:stamp", "a step of the enclosing scope is given whole");

        // Not a step, and not in the plan.
        for (ids, expect) in [
            (vec!["each"], "'each' is not a step of plan 'mapped'"),
            (vec!["nowhere"], "'nowhere' is not a node of plan 'mapped'"),
        ] {
            match steps_to_resolved_graph(&plan, &ids, &registry) {
                Err(ParseOrchestrationError::InvalidGraph { message }) => assert_eq!(message, expect),
                other => panic!("expected a refusal, got {other:?}"),
            }
        }
    }

    // TEST771: a plan that wires a step in a way no invocation can be given is
    // refused by what is wrong: two sources on one input stream (two main
    // inputs; the same arg named by two edges, however its URN is spelled),
    // a map wired straight into a step, a cap the registry does not have.
    #[test]
    fn test771_a_step_no_invocation_can_be_given_is_refused() {
        let registry = registry();
        let refused = |change: &dyn Fn(&mut MachinePlan), expect: &str| {
            let mut plan = plan();
            change(&mut plan);
            match plan_to_resolved_graph(&plan, &registry) {
                Err(ParseOrchestrationError::InvalidGraph { message }) => {
                    assert!(message.contains(expect), "expected '{expect}' in: {message}");
                }
                other => panic!("expected InvalidGraph ({expect}), got {other:?}"),
            }
        };
        refused(
            &|plan| plan.add_edge(MachinePlanEdge::direct("now", "summarize")),
            "step 'summarize' is wired twice on input stream 'media:enc=utf-8;ext=txt'",
        );
        refused(
            // The same arg, its tags in another order.
            &|plan| plan.add_edge(MachinePlanEdge::arg("doc", "summarize", "media:enc=utf-8;context")),
            "step 'summarize' is wired twice on input stream",
        );
        refused(
            &|plan| plan.add_edge(MachinePlanEdge::iteration("now", "summarize")),
            "step 'summarize' has an incoming Iteration edge from 'now'",
        );
        refused(
            &|plan| plan.add_edge(MachinePlanEdge::collection("now", "summarize")),
            "step 'summarize' has an incoming Collection edge from 'now'",
        );
        refused(
            &|plan| plan.add_edge(MachinePlanEdge::arg("out", "extract", CONTEXT)),
            "step 'extract' is wired from 'out', which holds no value",
        );
        refused(
            &|plan| plan.add_edge(MachinePlanEdge::arg("doc", "extract", "not a urn")),
            "step 'extract': input stream URN 'not a urn' is not a media URN",
        );

        let mut unknown = plan();
        unknown.add_node(MachineNode::cap("mystery", r#"cap:in="media:x";mystery;out="media:y""#));
        match plan_to_resolved_graph(&unknown, &registry) {
            Err(ParseOrchestrationError::CapNotFound { cap_urn }) => assert!(cap_urn.contains("mystery")),
            other => panic!("expected CapNotFound, got {other:?}"),
        }
    }

    // TEST953: a step's policy and its token go with it into the graph.
    #[test]
    fn test953_a_step_keeps_its_token_and_policy() {
        let mut plan = plan();
        let policy = StepPolicy {
            retry: 2,
            on_error: OnError::Absent,
        };
        let token = match &mut plan.nodes.get_mut("extract").unwrap().node_type {
            ExecutionNodeType::Cap {
                policy: p, token_id, ..
            } => {
                *p = policy;
                token_id.clone()
            }
            _ => unreachable!(),
        };
        let graph = plan_to_resolved_graph(&plan, &registry()).unwrap();
        assert_eq!(step(&graph, "extract").policy, policy);
        assert_eq!(step(&graph, "extract").token_id, token);
        assert!(step(&graph, "summarize").policy.is_default());
        assert_ne!(step(&graph, "summarize").token_id, token, "each step has its own");
    }
}
