//! Convert MachinePlan to ResolvedGraph
//!
//! This module bridges the planner's MachinePlan (node-centric) to the
//! orchestrator's ResolvedGraph (edge-centric) format for execution via execute_dag.
//!
//! The planner creates execution plans where caps are nodes with edges representing
//! data flow. The orchestrator expects caps to be edge labels connecting data nodes.
//!
//! Conversion strategy:
//! - InputSlot nodes become source data nodes
//! - Cap nodes become edges from their input source to their output target
//! - Output nodes mark terminal data nodes
//! - Standalone Collect nodes (scalar→list, no ForEach) are pass-throughs
//! - ForEach/Merge/Split nodes are rejected — the caller must decompose
//!   ForEach plans into sub-plans before conversion (see MachinePlan::extract_*)

use super::types::{ParseOrchestrationError, ResolvedEdge, ResolvedGraph};
use crate::cap::registry::FabricRegistry;
use crate::planner::{ExecutionNodeType, MachinePlan, StepToken};
use std::collections::HashMap;

/// Convert a MachinePlan to a ResolvedGraph for execution.
///
/// This transforms the node-centric plan (where caps are nodes) into the
/// edge-centric graph (where caps are edge labels) that execute_dag expects.
///
/// # Arguments
/// * `plan` - The execution plan from the planner
/// * `registry` - Cap registry for resolving full Cap definitions
///
/// # Returns
/// A ResolvedGraph suitable for execute_dag, or an error if conversion fails
pub async fn plan_to_resolved_graph(
    plan: &MachinePlan,
    registry: &FabricRegistry,
) -> Result<ResolvedGraph, ParseOrchestrationError> {
    let mut nodes: HashMap<String, String> = HashMap::new();
    let mut resolved_edges: Vec<ResolvedEdge> = Vec::new();

    let lookup_cached =
        |cap_urn: &str| -> Result<crate::cap::definition::Cap, ParseOrchestrationError> {
            registry
                .get_cached_cap(cap_urn)
                .ok_or_else(|| ParseOrchestrationError::CapNotFound {
                    cap_urn: cap_urn.to_string(),
                })
        };

    // First pass: identify all data nodes (InputSlots and cap outputs)
    // and their media URNs
    for (node_id, node) in &plan.nodes {
        match &node.node_type {
            ExecutionNodeType::InputSlot {
                expected_media_urn, ..
            } => {
                nodes.insert(node_id.clone(), expected_media_urn.clone());
            }
            ExecutionNodeType::Cap { cap_urn, .. } => {
                // Cap nodes produce output - get the out_spec from the cap URN
                let cap = lookup_cached(cap_urn)?;
                let out_media = cap.urn.out_spec().to_string();
                // The cap's output is associated with this node's ID
                nodes.insert(node_id.clone(), out_media);
            }
            ExecutionNodeType::Output { source_node, .. } => {
                // Output nodes inherit media from their source
                if let Some(source) = plan.nodes.get(source_node) {
                    if let ExecutionNodeType::Cap { cap_urn, .. } = &source.node_type {
                        let cap = lookup_cached(cap_urn)?;
                        nodes.insert(node_id.clone(), cap.urn.out_spec().to_string());
                    }
                }
            }
            ExecutionNodeType::Collect {
                output_media_urn, ..
            } => {
                if let Some(media_urn) = output_media_urn {
                    // Standalone Collect (scalar→list): pass-through at execution time.
                    // The data flows unchanged, only the type annotation changes.
                    // Register the node with the list media URN so downstream edges
                    // can find data at it.
                    nodes.insert(node_id.clone(), media_urn.clone());
                } else {
                    // ForEach-paired Collect without output_media_urn should not reach
                    // plan_converter — the plan should have been decomposed first.
                    return Err(ParseOrchestrationError::InvalidGraph {
                        message: format!(
                            "Plan contains ForEach-paired Collect node '{}'. Decompose the plan \
                             using extract_prefix_to/extract_foreach_body/extract_suffix_from \
                             before converting to ResolvedGraph.",
                            node_id
                        ),
                    });
                }
            }
            ExecutionNodeType::ForEach { .. } => {
                return Err(ParseOrchestrationError::InvalidGraph {
                    message: format!(
                        "Plan contains ForEach node '{}'. Decompose the plan using \
                         extract_prefix_to/extract_foreach_body/extract_suffix_from \
                         before converting to ResolvedGraph.",
                        node_id
                    ),
                });
            }
            ExecutionNodeType::Merge { .. } => {
                return Err(ParseOrchestrationError::InvalidGraph {
                    message: format!(
                        "Plan contains Merge node '{}' which is not yet supported for execution.",
                        node_id
                    ),
                });
            }
            ExecutionNodeType::Split { .. } => {
                return Err(ParseOrchestrationError::InvalidGraph {
                    message: format!(
                        "Plan contains Split node '{}' which is not yet supported for execution.",
                        node_id
                    ),
                });
            }
        }
    }

    // Build a map from standalone Collect nodes to their input predecessors.
    // A single-predecessor Collect (scalar→list) is a pass-through: data at the
    // predecessor flows through unchanged. A multi-predecessor Collect is a
    // GATHER (N distinct producers → one sequence arg): each predecessor
    // becomes its own ResolvedEdge into the consumer, all carrying the same
    // arg URN — the segment executor concatenates them into one sequence
    // stream (send_group_input), gated on the consuming arg being a sequence
    // arg. Predecessor order is the plan's edge insertion order, which is the
    // resolver's gather (source-declaration) order — the sequence's item order.
    let mut collect_predecessors: HashMap<String, Vec<String>> = HashMap::new();
    for edge in &plan.edges {
        if let Some(to_node) = plan.nodes.get(&edge.to_node) {
            if let ExecutionNodeType::Collect {
                output_media_urn: Some(_),
                ..
            } = &to_node.node_type
            {
                collect_predecessors
                    .entry(edge.to_node.clone())
                    .or_default()
                    .push(edge.from_node.clone());
            }
        }
    }

    // Second pass: convert edges that lead INTO Cap nodes into ResolvedEdges
    // In MachinePlan, data flows: source_node --edge--> cap_node
    // In ResolvedGraph, this becomes: source_node --cap_edge--> cap_node
    // The cap's output is stored AT the cap node (cap_0, cap_1, etc.)
    for edge in &plan.edges {
        let to_node =
            plan.nodes
                .get(&edge.to_node)
                .ok_or_else(|| ParseOrchestrationError::CapNotFound {
                    cap_urn: format!("Node '{}' not found in plan", edge.to_node),
                })?;

        // Only create ResolvedEdges for edges that point to Cap nodes
        if let ExecutionNodeType::Cap {
            cap_urn, token_id, ..
        } = &to_node.node_type
        {
            let cap = lookup_cached(cap_urn)?;
            // The stream URN this edge carries: a convergence (`Arg`) edge feeds a
            // specific non-main argument, so it carries that arg's URN; a `Direct` edge
            // feeds the cap's main (stdin) input, carrying the cap's `in=` URN.
            let in_media = match &edge.edge_type {
                crate::planner::EdgeType::Arg { arg_urn } => arg_urn.clone(),
                _ => cap.urn.in_spec().to_string(),
            };
            let out_media = cap.urn.out_spec().to_string();

            // If the source is a standalone Collect node, resolve through to the
            // actual data source(s). A single-predecessor Collect is transparent
            // (data flows unchanged); a multi-predecessor Collect (gather) fans
            // into one ResolvedEdge per predecessor, all with the same arg URN —
            // the executor gathers them into one sequence stream.
            let froms: Vec<String> = match collect_predecessors.get(&edge.from_node) {
                Some(preds) => {
                    if preds.is_empty() {
                        return Err(ParseOrchestrationError::InvalidGraph {
                            message: format!(
                                "Collect node '{}' has no input edges",
                                edge.from_node
                            ),
                        });
                    }
                    preds.clone()
                }
                None => vec![edge.from_node.clone()],
            };

            // The cap's output is stored at the cap node itself
            // This allows the next edge (cap_0 → cap_1) to find data at cap_0
            for from in froms {
                resolved_edges.push(ResolvedEdge {
                    token_id: token_id.clone(),
                    from,
                    to: edge.to_node.clone(), // Store output at the cap node
                    cap_urn: cap_urn.clone(),
                    cap: cap.clone(),
                    in_media: in_media.clone(),
                    out_media: out_media.clone(),
                });
            }
        }
    }

    Ok(ResolvedGraph {
        nodes,
        edges: resolved_edges,
        graph_name: Some(plan.name.clone()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cap::definition::{ArgSource, Cap, CapArg, CapOutput};
    use crate::cap::registry::FabricRegistry;
    use crate::planner::{
        ExecutionNodeType, InputCardinality, MachineNode, MachinePlan, MachinePlanEdge,
    };
    use crate::urn::cap_urn::CapUrn;

    /// Build a `FabricRegistry::new_for_test()` populated with the
    /// supplied cap URNs. plan_converter doesn't exercise the
    /// resolver's source-to-arg matching (it walks plan nodes
    /// directly), so each cap gets a single stdin arg matching
    /// its in= spec to keep cap definitions consistent with
    /// the rest of the system.
    fn build_test_registry(cap_urns: &[&str]) -> FabricRegistry {
        let registry = FabricRegistry::new_for_test();
        let mut caps = Vec::new();
        for urn in cap_urns {
            let cap_urn = CapUrn::from_string(urn).unwrap();
            let in_spec = cap_urn.in_spec().to_string();
            let out_spec = cap_urn.out_spec().to_string();
            caps.push(Cap {
                urn: cap_urn,
                version: 1,
                title: "Test Cap".to_string(),
                cap_description: None,
                documentation: None,
                metadata: HashMap::new(),
                aliases: vec!["test".to_string()],
                is_abstract: false,
                args: vec![CapArg::new(
                    in_spec.clone(),
                    true,
                    vec![ArgSource::Stdin { stdin: in_spec }],
                )],
                output: Some(CapOutput::new(out_spec, "Test output".to_string())),
                metadata_json: None,
                registered_by: None,
                supported_model_types: Vec::new(),
                default_model_spec: None,
            });
        }
        registry.add_caps_to_cache(caps);
        registry
    }

    // TEST1161: Converting a simple linear plan produces resolved edges for the cap-to-cap chain.
    #[tokio::test]
    async fn test1161_simple_linear_chain_conversion() {
        let registry = build_test_registry(&[
            "cap:in=\"media:ext=pdf\";extract;out=media:text",
            "cap:in=media:text;summarize;out=media:summary",
        ]);

        let mut plan = MachinePlan::new("test_chain");

        // Add input slot
        plan.add_node(MachineNode::input_slot(
            "input",
            "input",
            "media:ext=pdf",
            InputCardinality::Single,
        ));

        // Add two caps in sequence
        plan.add_node(MachineNode::cap(
            "cap_0",
            "cap:in=\"media:ext=pdf\";extract;out=media:text",
        ));
        plan.add_node(MachineNode::cap(
            "cap_1",
            "cap:in=media:text;summarize;out=media:summary",
        ));

        // Add output
        plan.add_node(MachineNode::output("output", "result", "cap_1"));

        // Connect them
        plan.add_edge(MachinePlanEdge::direct("input", "cap_0"));
        plan.add_edge(MachinePlanEdge::direct("cap_0", "cap_1"));
        plan.add_edge(MachinePlanEdge::direct("cap_1", "output"));

        let graph = plan_to_resolved_graph(&plan, &registry).await.unwrap();

        // Edges: input→cap_0 (extract), cap_0→cap_1 (summarize)
        // The output edge (cap_1→output) doesn't generate a ResolvedEdge
        // because output nodes are not Cap nodes
        assert_eq!(graph.edges.len(), 2);

        // The two edges should reference the two caps; HashMap
        // iteration order is non-deterministic so we check by
        // membership rather than by index.
        let edge_keys: std::collections::HashSet<(String, String, String)> = graph
            .edges
            .iter()
            .map(|e| (e.from.clone(), e.to.clone(), e.cap_urn.clone()))
            .collect();
        assert!(edge_keys.contains(&(
            "input".to_string(),
            "cap_0".to_string(),
            "cap:in=\"media:ext=pdf\";extract;out=media:text".to_string(),
        )));
        assert!(edge_keys.contains(&(
            "cap_0".to_string(),
            "cap_1".to_string(),
            "cap:in=media:text;summarize;out=media:summary".to_string(),
        )));
    }

    // TEST770: plan_to_resolved_graph rejects plans containing ForEach nodes
    #[tokio::test]
    async fn test770_rejects_foreach() {
        let registry = build_test_registry(&[
            "cap:in=\"media:ext=pdf\";disbind;out=media:pdf-page",
            "cap:in=media:pdf-page;process;out=media:text",
        ]);

        let mut plan = MachinePlan::new("foreach_plan");
        plan.add_node(MachineNode::input_slot(
            "input",
            "input",
            "media:ext=pdf",
            InputCardinality::Single,
        ));
        plan.add_node(MachineNode::cap(
            "cap_0",
            "cap:in=\"media:ext=pdf\";disbind;out=media:pdf-page",
        ));
        plan.add_node(MachineNode::for_each(
            "foreach_0",
            "cap_0",
            "cap_1",
            "cap_1",
        ));
        plan.add_node(MachineNode::cap(
            "cap_1",
            "cap:in=media:pdf-page;process;out=media:text",
        ));
        plan.add_node(MachineNode::output("output", "result", "cap_1"));

        plan.add_edge(MachinePlanEdge::direct("input", "cap_0"));
        plan.add_edge(MachinePlanEdge::direct("cap_0", "foreach_0"));
        plan.add_edge(MachinePlanEdge::iteration("foreach_0", "cap_1"));
        plan.add_edge(MachinePlanEdge::direct("cap_1", "output"));

        let result = plan_to_resolved_graph(&plan, &registry).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("ForEach node"),
            "Expected ForEach rejection, got: {}",
            err
        );
        assert!(
            err.contains("Decompose"),
            "Should mention decomposition, got: {}",
            err
        );
    }

    // TEST771: plan_to_resolved_graph rejects plans containing Collect nodes
    #[tokio::test]
    async fn test771_rejects_collect() {
        let registry = build_test_registry(&[
            "cap:in=\"media:ext=pdf\";disbind;out=media:pdf-page",
            "cap:in=media:pdf-page;process;out=media:text",
        ]);

        let mut plan = MachinePlan::new("collect_plan");
        plan.add_node(MachineNode::input_slot(
            "input",
            "input",
            "media:ext=pdf",
            InputCardinality::Single,
        ));
        plan.add_node(MachineNode::cap(
            "cap_0",
            "cap:in=\"media:ext=pdf\";disbind;out=media:pdf-page",
        ));
        plan.add_node(MachineNode::for_each(
            "foreach_0",
            "cap_0",
            "cap_1",
            "cap_1",
        ));
        plan.add_node(MachineNode::cap(
            "cap_1",
            "cap:in=media:pdf-page;process;out=media:text",
        ));
        plan.add_node(MachineNode::collect("collect_0", vec!["cap_1".to_string()]));
        plan.add_node(MachineNode::output("output", "result", "collect_0"));

        plan.add_edge(MachinePlanEdge::direct("input", "cap_0"));
        plan.add_edge(MachinePlanEdge::direct("cap_0", "foreach_0"));
        plan.add_edge(MachinePlanEdge::iteration("foreach_0", "cap_1"));
        plan.add_edge(MachinePlanEdge::collection("cap_1", "collect_0"));
        plan.add_edge(MachinePlanEdge::direct("collect_0", "output"));

        let result = plan_to_resolved_graph(&plan, &registry).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        // Could hit either ForEach or Collect first depending on HashMap iteration order
        assert!(
            err.contains("ForEach node") || err.contains("Collect node"),
            "Expected ForEach or Collect rejection, got: {}",
            err
        );
    }

    // TEST953: Linear plans (no ForEach/Collect) still convert successfully
    #[tokio::test]
    async fn test953_linear_plan_still_works() {
        let registry = build_test_registry(&["cap:in=\"media:ext=pdf\";extract;out=media:text"]);

        let mut plan = MachinePlan::new("linear_plan");
        plan.add_node(MachineNode::input_slot(
            "input",
            "input",
            "media:ext=pdf",
            InputCardinality::Single,
        ));
        plan.add_node(MachineNode::cap(
            "cap_0",
            "cap:in=\"media:ext=pdf\";extract;out=media:text",
        ));
        plan.add_node(MachineNode::output("output", "result", "cap_0"));

        plan.add_edge(MachinePlanEdge::direct("input", "cap_0"));
        plan.add_edge(MachinePlanEdge::direct("cap_0", "output"));

        let result = plan_to_resolved_graph(&plan, &registry).await;
        assert!(
            result.is_ok(),
            "Linear plan should still convert: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap().edges.len(), 1);
    }

    // TEST955: A multi-predecessor (gather) Collect fans into one ResolvedEdge
    // per predecessor, all carrying the same arg URN — the segment executor
    // concatenates them into the consumer's sequence stream. Predecessor order
    // (= resolver source-declaration order) is preserved in the edge order.
    #[tokio::test]
    async fn test955_gather_collect_expands_to_per_producer_edges() {
        let registry = build_test_registry(&[
            r#"cap:in="media:ext=pdf";op-a;out="media:enc=utf-8;page""#,
            r#"cap:in="media:ext=md";op-b;out="media:enc=utf-8;page""#,
            r#"cap:in="media:enc=utf-8";concat;out="media:enc=utf-8;ext=txt""#,
        ]);

        let mut plan = MachinePlan::new("gather_plan");
        plan.add_node(MachineNode::input_slot(
            "in_a",
            "input",
            "media:ext=pdf",
            InputCardinality::Single,
        ));
        plan.add_node(MachineNode::input_slot(
            "in_b",
            "input",
            "media:ext=md",
            InputCardinality::Single,
        ));
        plan.add_node(MachineNode::cap(
            "cap_a",
            r#"cap:in="media:ext=pdf";op-a;out="media:enc=utf-8;page""#,
        ));
        plan.add_node(MachineNode::cap(
            "cap_b",
            r#"cap:in="media:ext=md";op-b;out="media:enc=utf-8;page""#,
        ));
        let mut collect_node =
            MachineNode::collect("collect_0", vec!["cap_a".to_string(), "cap_b".to_string()]);
        collect_node.node_type = ExecutionNodeType::Collect {
            input_nodes: vec!["cap_a".to_string(), "cap_b".to_string()],
            output_media_urn: Some("media:enc=utf-8;page".to_string()),
        };
        plan.add_node(collect_node);
        plan.add_node(MachineNode::cap(
            "cap_cat",
            r#"cap:in="media:enc=utf-8";concat;out="media:enc=utf-8;ext=txt""#,
        ));
        plan.add_node(MachineNode::output("output", "result", "cap_cat"));

        plan.add_edge(MachinePlanEdge::direct("in_a", "cap_a"));
        plan.add_edge(MachinePlanEdge::direct("in_b", "cap_b"));
        plan.add_edge(MachinePlanEdge::collection("cap_a", "collect_0"));
        plan.add_edge(MachinePlanEdge::collection("cap_b", "collect_0"));
        plan.add_edge(MachinePlanEdge::direct("collect_0", "cap_cat"));
        plan.add_edge(MachinePlanEdge::direct("cap_cat", "output"));

        let graph = plan_to_resolved_graph(&plan, &registry)
            .await
            .expect("gather plan must convert");

        // Edges: in_a→cap_a, in_b→cap_b, cap_a→cap_cat, cap_b→cap_cat.
        // The Collect resolves through into one edge per predecessor.
        assert_eq!(
            graph.edges.len(),
            4,
            "collect expands to one edge per producer"
        );
        let into_cat: Vec<&ResolvedEdge> =
            graph.edges.iter().filter(|e| e.to == "cap_cat").collect();
        assert_eq!(into_cat.len(), 2, "both producers feed concat");
        let froms: Vec<&str> = into_cat.iter().map(|e| e.from.as_str()).collect();
        assert_eq!(
            froms,
            vec!["cap_a", "cap_b"],
            "gather member order must be the plan's Collection-edge order"
        );
        // Both carry the SAME in_media (concat's stdin) — the executor's gather
        // discriminator.
        assert_eq!(into_cat[0].in_media, into_cat[1].in_media);
        assert!(
            !graph
                .edges
                .iter()
                .any(|e| e.from == "collect_0" || e.to == "collect_0"),
            "the Collect node itself must not appear as an edge endpoint"
        );
    }

    // TEST954: Standalone Collect nodes are handled as pass-through
    // Plan: input → cap_0 → Collect → cap_1 → output
    // The standalone Collect is transparent — the resolved edge from Collect to cap_1
    // should be rewritten to go from cap_0 to cap_1 directly.
    #[tokio::test]
    async fn test954_standalone_collect_passthrough() {
        let registry = build_test_registry(&[
            r#"cap:in="media:ext=pdf";extract;out="media:enc=utf-8;text""#,
            r#"cap:in="media:enc=utf-8;list;text";embed;out="media:embedding-vector;enc=utf-8;record""#,
        ]);

        let mut plan = MachinePlan::new("collect_plan");
        plan.add_node(MachineNode::input_slot(
            "input",
            "input",
            "media:ext=pdf",
            InputCardinality::Single,
        ));
        plan.add_node(MachineNode::cap(
            "cap_0",
            r#"cap:in="media:ext=pdf";extract;out="media:enc=utf-8;text""#,
        ));

        // Standalone Collect: scalar→list with output_media_urn set
        let mut collect_node = MachineNode::collect("collect_0", vec!["cap_0".to_string()]);
        collect_node.node_type = ExecutionNodeType::Collect {
            input_nodes: vec!["cap_0".to_string()],
            output_media_urn: Some("media:enc=utf-8;list;text".to_string()),
        };
        collect_node.description = Some("Collect: scalar to list-of-one".to_string());
        plan.add_node(collect_node);

        plan.add_node(MachineNode::cap("cap_1", r#"cap:in="media:enc=utf-8;list;text";embed;out="media:embedding-vector;enc=utf-8;record""#));
        plan.add_node(MachineNode::output("output", "result", "cap_1"));

        plan.add_edge(MachinePlanEdge::direct("input", "cap_0"));
        plan.add_edge(MachinePlanEdge::direct("cap_0", "collect_0"));
        plan.add_edge(MachinePlanEdge::direct("collect_0", "cap_1"));
        plan.add_edge(MachinePlanEdge::direct("cap_1", "output"));

        let result = plan_to_resolved_graph(&plan, &registry).await;
        assert!(
            result.is_ok(),
            "Plan with standalone Collect should convert: {:?}",
            result.err()
        );

        let graph = result.unwrap();
        // Two resolved edges: input→cap_0 and cap_0→cap_1 (the
        // standalone Collect node is resolved through, so its
        // outgoing edge becomes a direct cap_0 → cap_1 edge).
        // HashMap iteration order is non-deterministic, so
        // assert by membership rather than by index.
        assert_eq!(
            graph.edges.len(),
            2,
            "Expected 2 edges, got {}: {:?}",
            graph.edges.len(),
            graph
                .edges
                .iter()
                .map(|e| format!("{}→{}", e.from, e.to))
                .collect::<Vec<_>>()
        );

        let edge_pairs: std::collections::HashSet<(String, String)> = graph
            .edges
            .iter()
            .map(|e| (e.from.clone(), e.to.clone()))
            .collect();
        assert!(
            edge_pairs.contains(&("input".to_string(), "cap_0".to_string())),
            "expected input → cap_0 edge"
        );
        assert!(
            edge_pairs.contains(&("cap_0".to_string(), "cap_1".to_string())),
            "expected cap_0 → cap_1 edge (Collect resolved through)"
        );
        // The standalone Collect should NOT appear as an
        // edge endpoint — it's a pass-through.
        assert!(
            !edge_pairs
                .iter()
                .any(|(f, t)| f == "collect_0" || t == "collect_0"),
            "standalone Collect must not appear as an edge endpoint"
        );

        // has_foreach should be false (standalone Collect does NOT trigger ForEach execution path)
        assert!(
            !plan.has_foreach(),
            "Plan with only standalone Collect should NOT trigger ForEach execution path"
        );
    }
}
