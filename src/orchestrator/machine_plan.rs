//! Notation → `MachinePlan`: the single ForEach-aware front-end.
//!
//! This replaces the flat `parse_machine_to_cap_dag` → `ResolvedGraph` path. It
//! resolves machine notation into the **same** `MachinePlan` the engine executes, by
//! reusing the canonical plan builder (`MachinePlanBuilder::build_plan_from_path`)
//! rather than constructing a parallel plan of its own. The only new logic is the
//! conversion of a resolved `MachineStrand` (which already carries, per edge, the
//! cardinality-derived `is_loop` and the single-stdin source assignment) into the
//! `Strand` of steps that builder consumes — the same shape the engine's `realize()`
//! produces for editor runs.
//!
//! ## Execution model (invariants, enforced here)
//!
//! - **One stdin per cap.** A cap is a process; it has exactly one main data input.
//!   An edge that carries more than one incoming data source is a hard error — there
//!   is no second data stream to route it to.
//! - **Every arg is a stream.** The single stdin arg streams from the upstream node;
//!   all other args (model-spec, budgets, …) stream from `node_values` as extra-arg
//!   streams at execution. This conversion therefore only wires the *data* chain.
//! - **ForEach is cardinality-derived.** When a cap's stdin source is a sequence but
//!   the cap consumes a scalar (`MachineEdge::is_loop`, from `Cap::needs_foreach`), a
//!   `ForEach` step precedes it so it maps per item. Nothing authors this — the
//!   `LOOP` keyword is retired.
//!
//! Strands are independent connected components, so each becomes its own plan; the
//! executor runs them and merges outputs.

use std::sync::Arc;

use crate::cap::registry::FabricRegistry;
use crate::machine::parse_machine_with_node_names_async;
use crate::planner::{InputCardinality, MachinePlan, MachinePlanBuilder};

use super::parser::translate_machine_parse_error;
use super::types::ParseOrchestrationError;

/// Build one [`MachinePlan`] per strand from machine notation, using the canonical
/// [`MachinePlanBuilder::build_plan_from_path`] over the shared
/// [`realize_strand`](crate::machine::realize_strand) conversion — the same path the
/// engine's editor run uses. No plan construction is duplicated here.
pub async fn build_plans_from_notation(
    notation: &str,
    registry: Arc<FabricRegistry>,
) -> Result<Vec<MachinePlan>, ParseOrchestrationError> {
    let (machine, _names) = parse_machine_with_node_names_async(notation, &registry)
        .await
        .map_err(translate_machine_parse_error)?;

    let builder = MachinePlanBuilder::new(registry.clone());
    let mut plans = Vec::with_capacity(machine.strands().len());
    for (idx, strand) in machine.strands().iter().enumerate() {
        if strand.input_anchor_ids().is_empty() || strand.output_anchor_ids().is_empty() {
            return Err(ParseOrchestrationError::InvalidGraph {
                message: format!("strand {idx} has no input or output anchor"),
            });
        }
        // Build a branching plan directly from the strand DAG — preserving fan-out (one
        // source feeding several caps). A single runtime input per strand is scalar; a
        // ForEach inside the strand is driven by an intermediate cap's sequence output,
        // not by the input.
        let plan = builder
            .build_plan_from_machine_strand(
                &format!("machine_strand_{idx}"),
                strand,
                InputCardinality::Single,
            )
            .await
            .map_err(|e| ParseOrchestrationError::InvalidGraph {
                message: format!("strand {idx}: plan construction failed: {e}"),
            })?;
        plans.push(plan);
    }

    if plans.is_empty() {
        return Err(ParseOrchestrationError::InvalidGraph {
            message: "notation resolved to zero strands (no capability steps)".to_string(),
        });
    }
    Ok(plans)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cap::definition::{ArgSource, Cap, CapArg, CapOutput};
    use crate::planner::ExecutionNodeType;
    use crate::urn::cap_urn::CapUrn;

    /// A registry cap. `output_is_sequence` models a cap that emits a sequence
    /// (render-page-image → many pages) — the driver of ForEach.
    pub(crate) fn cap(urn: &str, output_is_sequence: bool) -> Cap {
        let cap_urn = CapUrn::from_string(urn).unwrap();
        let in_spec = cap_urn.in_spec().to_string();
        let out_spec = cap_urn.out_spec().to_string();
        let mut output = CapOutput::new(out_spec, "out".to_string());
        output.is_sequence = output_is_sequence;
        Cap {
            urn: cap_urn,
            version: 1,
            title: "t".to_string(),
            cap_description: None,
            documentation: None,
            metadata: std::collections::HashMap::new(),
            aliases: vec!["test".to_string()],
            is_abstract: false,
            args: vec![CapArg::new(
                in_spec.clone(),
                true,
                vec![ArgSource::Stdin { stdin: in_spec }],
            )],
            output: Some(output),
            metadata_json: None,
            registered_by: None,
            supported_model_types: Vec::new(),
            default_model_spec: None,
        }
    }

    pub(crate) fn registry(caps: Vec<Cap>) -> Arc<FabricRegistry> {
        let r = FabricRegistry::new_for_test();
        r.add_caps_to_cache(caps);
        Arc::new(r)
    }

    fn foreach_nodes(plan: &MachinePlan) -> Vec<(String, String)> {
        plan.nodes
            .values()
            .filter_map(|n| match &n.node_type {
                ExecutionNodeType::ForEach {
                    input_node,
                    body_entry,
                    ..
                } => Some((input_node.clone(), body_entry.clone())),
                _ => None,
            })
            .collect()
    }

    fn cap_urn_of(plan: &MachinePlan, node_id: &str) -> String {
        match &plan.nodes.get(node_id).expect("node").node_type {
            ExecutionNodeType::Cap { cap_urn, .. } => cap_urn.clone(),
            other => panic!("node '{node_id}' is not a cap: {other:?}"),
        }
    }

    // TEST1303: A linear machine (no sequence output) lowers to a ForEach-free plan.
    #[tokio::test]
    async fn test1303_linear_machine_no_foreach() {
        let reg = registry(vec![
            cap(
                r#"cap:in="media:document;ext=pdf";extract;out="media:enc=utf-8;ext=txt""#,
                false,
            ),
            cap(
                r#"cap:in="media:enc=utf-8;ext=txt";summarize;out="media:enc=utf-8;summary""#,
                false,
            ),
        ]);
        let notation = concat!(
            r#"[extract cap:in="media:document;ext=pdf";extract;out="media:enc=utf-8;ext=txt"]"#,
            r#"[summarize cap:in="media:enc=utf-8;ext=txt";summarize;out="media:enc=utf-8;summary"]"#,
            "[doc -> extract -> text]",
            "[text -> summarize -> summary]",
        );
        let plans = build_plans_from_notation(notation, reg)
            .await
            .expect("build");
        assert_eq!(plans.len(), 1, "one connected strand");
        assert!(
            foreach_nodes(&plans[0]).is_empty(),
            "a linear machine must not introduce a ForEach"
        );
        // Two cap nodes present.
        assert_eq!(plans[0].nodes.values().filter(|n| n.is_cap()).count(), 2);
    }

    // TEST1304: A sequence-output cap feeding a scalar-input cap lowers to a ForEach
    // whose body is the consumer cap — the render→map→consume shape the flat executor
    // could not express. Asserted structurally (no reliance on generated node ids).
    #[tokio::test]
    async fn test1304_sequence_into_scalar_wraps_foreach() {
        let reg = registry(vec![
            cap(
                r#"cap:in="media:document;ext=pdf";render-page-image;out="media:ext=png;image""#,
                true,
            ),
            cap(
                r#"cap:in="media:ext=png;image";convert-image;out="media:ext=jpeg;image""#,
                false,
            ),
        ]);
        let notation = concat!(
            r#"[render cap:in="media:document;ext=pdf";render-page-image;out="media:ext=png;image"]"#,
            r#"[to_jpeg cap:in="media:ext=png;image";convert-image;out="media:ext=jpeg;image"]"#,
            "[input -> render -> thumbnail]",
            "[thumbnail -> to_jpeg -> output]",
        );
        let plans = build_plans_from_notation(notation, reg)
            .await
            .expect("build");
        assert_eq!(plans.len(), 1);
        let plan = &plans[0];
        let fe = foreach_nodes(plan);
        assert_eq!(fe.len(), 1, "exactly one per-item map");
        // The ForEach body is the convert-image cap (the scalar consumer), not render.
        assert!(
            cap_urn_of(plan, &fe[0].1).contains("convert-image"),
            "ForEach body must be the scalar consumer (convert-image), got {}",
            cap_urn_of(plan, &fe[0].1)
        );
    }

    // TEST1305: A disconnected multi-strand machine yields one plan per strand.
    #[tokio::test]
    async fn test1305_multi_strand_yields_one_plan_each() {
        let reg = registry(vec![
            cap(
                r#"cap:in="media:document;ext=pdf";render-page-image;out="media:ext=png;image""#,
                true,
            ),
            cap(
                r#"cap:in="media:ext=png;image";convert-image;out="media:ext=jpeg;image""#,
                false,
            ),
        ]);
        let notation = concat!(
            r#"[render cap:in="media:document;ext=pdf";render-page-image;out="media:ext=png;image"]"#,
            r#"[to_jpeg cap:in="media:ext=png;image";convert-image;out="media:ext=jpeg;image"]"#,
            "[a_in -> render -> a_thumb]",
            "[a_thumb -> to_jpeg -> a_out]",
            "[b_in -> render -> b_thumb]",
            "[b_thumb -> to_jpeg -> b_out]",
        );
        let plans = build_plans_from_notation(notation, reg)
            .await
            .expect("build");
        assert_eq!(plans.len(), 2, "two disconnected strands → two plans");
        for plan in &plans {
            assert_eq!(
                foreach_nodes(plan).len(),
                1,
                "each strand keeps its own ForEach"
            );
        }
    }

    /// A registry cap whose single stdin arg is a SEQUENCE arg — the gather
    /// consumer shape (e.g. `concat`: sequence of text items → one text).
    fn seq_arg_cap(urn: &str) -> Cap {
        let mut c = cap(urn, false);
        c.args[0].is_sequence = true;
        c
    }

    // TEST1307: Fan-in `(x, y) -> concat` where concat's stdin arg is a SEQUENCE
    // arg gathers both producers through a synthesized Collect node: the plan
    // carries a real `Collect{input_nodes=[a,b]}` feeding concat (never a bare
    // multi-bound arg), and no ForEach is introduced (the gathered value IS the
    // sequence the arg consumes).
    #[tokio::test]
    async fn test1307_fan_in_gather_synthesizes_collect() {
        let reg = registry(vec![
            cap(
                r#"cap:in="media:document;ext=pdf";op-a;out="media:enc=utf-8;page""#,
                false,
            ),
            cap(
                r#"cap:in="media:ext=md";op-b;out="media:enc=utf-8;page""#,
                false,
            ),
            seq_arg_cap(r#"cap:in="media:enc=utf-8";concat;out="media:enc=utf-8;ext=txt""#),
        ]);
        let notation = concat!(
            r#"[a cap:in="media:document;ext=pdf";op-a;out="media:enc=utf-8;page"]"#,
            r#"[b cap:in="media:ext=md";op-b;out="media:enc=utf-8;page"]"#,
            r#"[cat cap:in="media:enc=utf-8";concat;out="media:enc=utf-8;ext=txt"]"#,
            "[doc_a -> a -> x]",
            "[doc_b -> b -> y]",
            "[(x, y) -> cat -> z]",
        );
        let plans = build_plans_from_notation(notation, reg)
            .await
            .expect("a fan-in gather into a sequence arg must build (the implicit Collect)");
        assert_eq!(plans.len(), 1, "one connected strand");
        let plan = &plans[0];

        // Exactly one Collect node, gathering BOTH producer caps.
        let collects: Vec<(&String, &Vec<String>)> = plan
            .nodes
            .iter()
            .filter_map(|(id, n)| match &n.node_type {
                ExecutionNodeType::Collect {
                    input_nodes,
                    output_media_urn,
                } => {
                    assert!(
                        output_media_urn.is_some(),
                        "gather Collect must carry its element media"
                    );
                    Some((id, input_nodes))
                }
                _ => None,
            })
            .collect();
        assert_eq!(collects.len(), 1, "exactly one gather Collect");
        let (collect_id, input_nodes) = collects[0];
        assert_eq!(input_nodes.len(), 2, "the Collect gathers both producers");

        // The Collect's inputs are the two producer cap nodes.
        for input in input_nodes {
            let node = plan.nodes.get(input).expect("collect input node exists");
            assert!(node.is_cap(), "collect input '{input}' must be a cap node");
        }

        // The Collect feeds the concat cap via a Direct edge (it is the stdin arg).
        let concat_node_id = plan
            .nodes
            .iter()
            .find_map(|(id, n)| {
                n.cap_urn()
                    .filter(|u| u.contains("concat"))
                    .map(|_| id.clone())
            })
            .expect("concat cap node present");
        assert!(
            plan.edges.iter().any(|e| &e.from_node == collect_id
                && e.to_node == concat_node_id
                && matches!(e.edge_type, crate::planner::EdgeType::Direct)),
            "the gather Collect must feed concat's stdin directly"
        );
        // Producer → Collect wiring is Collection edges, one per member.
        let collection_edges = plan
            .edges
            .iter()
            .filter(|e| {
                &e.to_node == collect_id
                    && matches!(e.edge_type, crate::planner::EdgeType::Collection)
            })
            .count();
        assert_eq!(
            collection_edges, 2,
            "one Collection edge per gathered producer"
        );

        // No ForEach: the gathered value IS the sequence the arg consumes.
        assert!(
            foreach_nodes(plan).is_empty(),
            "a gather into a sequence arg must not introduce a ForEach"
        );
    }

    // TEST1309: a SEQUENCE-consuming cap fed from inside a ForEach region
    // CLOSES the region (the fold): it must NOT extend the ForEach body. The
    // body stays [upscale]; the fold is wired as a plain downstream consumer
    // and the executor runs it post-region on the collected sequence.
    #[tokio::test]
    async fn test1309_fold_after_mapped_region_closes_the_region() {
        let mut render = cap(
            r#"cap:in="media:document;ext=pdf";render;out="media:ext=png;image""#,
            true, // sequence of page images
        );
        render.output.as_mut().unwrap().is_sequence = true;
        let upscale = cap(
            r#"cap:in="media:ext=png;image";upscale;out="media:ext=png;image;upscaled""#,
            false,
        );
        let fold = seq_arg_cap(r#"cap:in="media:ext=png;image";animate;out="media:ext=gif;image""#);
        let reg = registry(vec![render, upscale, fold]);
        let notation = concat!(
            r#"[render cap:in="media:document;ext=pdf";render;out="media:ext=png;image"]"#,
            r#"[upscale cap:in="media:ext=png;image";upscale;out="media:ext=png;image;upscaled"]"#,
            r#"[animate cap:in="media:ext=png;image";animate;out="media:ext=gif;image"]"#,
            "[doc -> render -> pics]",
            "[pics -> upscale -> big]",
            "[big -> animate -> out]",
        );
        let plans = build_plans_from_notation(notation, reg)
            .await
            .expect("must build");
        assert_eq!(plans.len(), 1);
        let plan = &plans[0];
        let fe = foreach_nodes(plan);
        assert_eq!(fe.len(), 1, "upscale maps per item under one ForEach");
        assert!(
            cap_urn_of(plan, &fe[0].1).contains("upscale"),
            "the body entry is the scalar mapper"
        );
        // The fold cap must NOT be the region's exit — the region closed at it.
        let animate_id = plan
            .nodes
            .iter()
            .find_map(|(id, n)| {
                n.cap_urn()
                    .filter(|u| u.contains("animate"))
                    .map(|_| id.clone())
            })
            .expect("animate node present");
        for n in plan.nodes.values() {
            if let ExecutionNodeType::ForEach { body_exit, .. } = &n.node_type {
                assert_ne!(
                    body_exit, &animate_id,
                    "the fold must not extend the ForEach body"
                );
            }
        }
        // The fold is a plain Direct consumer of the mapper's output.
        let upscale_id = plan
            .nodes
            .iter()
            .find_map(|(id, n)| {
                n.cap_urn()
                    .filter(|u| u.contains("upscale"))
                    .map(|_| id.clone())
            })
            .expect("upscale node present");
        assert!(
            plan.edges
                .iter()
                .any(|e| e.from_node == upscale_id && e.to_node == animate_id),
            "fold consumes the mapped output directly"
        );
    }

    // TEST1306: A cap wired with two incoming data sources is a hard error — there is
    // no second stdin. (Constructed via a synthetic two-source edge is impossible in
    // notation under one-stdin, so we assert the resolver/one-stdin path rejects the
    // multi-binding case rather than silently mis-routing.)
    #[tokio::test]
    async fn test1306_two_data_sources_is_hard_error() {
        // `merge` declares a single stdin; feeding it two sources cannot resolve to
        // one stdin, so notation resolution rejects it before we even build a plan.
        let reg = registry(vec![
            cap(r#"cap:in="media:document;ext=pdf";a;out="media:enc=utf-8""#, false),
            cap(r#"cap:in="media:document;ext=pdf";b;out="media:enc=utf-8""#, false),
            cap(
                r#"cap:in="media:enc=utf-8";merge;out="media:enc=utf-8;merged""#,
                false,
            ),
        ]);
        let notation = concat!(
            r#"[a cap:in="media:document;ext=pdf";a;out="media:enc=utf-8"]"#,
            r#"[b cap:in="media:document;ext=pdf";b;out="media:enc=utf-8"]"#,
            r#"[merge cap:in="media:enc=utf-8";merge;out="media:enc=utf-8;merged"]"#,
            "[doc_a -> a -> x]",
            "[doc_b -> b -> y]",
            "[(x, y) -> merge -> z]",
        );
        let result = build_plans_from_notation(notation, reg).await;
        assert!(
            result.is_err(),
            "two data sources into one cap must be rejected (no second stdin)"
        );
    }
}

#[cfg(test)]
mod anchor_cardinality_tests {
    use super::tests::{cap, registry};
    use crate::machine::{parse_machine_with_node_names_async, Machine};
    use crate::planner::{ExecutionNodeType, InputCardinality, MachinePlanBuilder};
    use std::collections::HashMap;

    // TEST1446: per-anchor cardinality — a Sequence anchor feeding a scalar
    // entry cap gets its per-file ForEach; a Single anchor beside it does not.
    #[tokio::test]
    async fn test1446_per_anchor_cardinality_drives_foreach() {
        let mut concat = cap(
            r#"cap:in="media:enc=utf-8";concat;out="media:enc=utf-8;ext=txt""#,
            false,
        );
        concat.args[0].is_sequence = true;
        let reg = registry(vec![
            cap(
                r#"cap:in="media:document;ext=pdf";op-a;out="media:enc=utf-8;page""#,
                false,
            ),
            cap(
                r#"cap:in="media:ext=md";op-b;out="media:enc=utf-8;page""#,
                false,
            ),
            concat,
        ]);
        let notation = concat!(
            r#"[a cap:in="media:document;ext=pdf";op-a;out="media:enc=utf-8;page"]"#,
            r#"[b cap:in="media:ext=md";op-b;out="media:enc=utf-8;page"]"#,
            r#"[cat cap:in="media:enc=utf-8";concat;out="media:enc=utf-8;ext=txt"]"#,
            "[doc_a -> a -> x]",
            "[doc_b -> b -> y]",
            "[(x, y) -> cat -> z]",
        );
        let (machine, _) = parse_machine_with_node_names_async(notation, &reg)
            .await
            .expect("machine parses");
        let _: &Machine = &machine;
        let strand = &machine.strands()[0];

        // pdf anchor: Sequence (a 3-file batch); md anchor: Single.
        let mut cardinalities: HashMap<crate::machine::NodeId, InputCardinality> = HashMap::new();
        for &anchor in strand.input_anchor_ids() {
            let urn = strand.node_urn(anchor).to_string();
            let cardinality = if urn.contains("pdf") {
                InputCardinality::Sequence
            } else {
                InputCardinality::Single
            };
            cardinalities.insert(anchor, cardinality);
        }

        let builder = MachinePlanBuilder::new(reg.clone());
        let plan = builder
            .build_plan_from_machine_strand_with_anchor_cardinalities(
                "mixed anchors",
                strand,
                &cardinalities,
            )
            .await
            .expect("mixed-cardinality plan builds");

        // Exactly ONE ForEach, and its body is op-a (the pdf leg's entry).
        let foreach_bodies: Vec<String> = plan
            .nodes
            .values()
            .filter_map(|n| match &n.node_type {
                ExecutionNodeType::ForEach { body_entry, .. } => Some(body_entry.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            foreach_bodies.len(),
            1,
            "only the Sequence anchor maps per file"
        );
        let body_cap = plan
            .nodes
            .get(&foreach_bodies[0])
            .and_then(|n| n.cap_urn())
            .expect("body entry is a cap");
        assert!(
            body_cap.contains("op-a"),
            "the pdf leg maps per file, got {body_cap}"
        );

        // Slot cardinalities recorded per anchor.
        let slot_cardinalities: Vec<(String, InputCardinality)> = plan
            .nodes
            .values()
            .filter_map(|n| match &n.node_type {
                ExecutionNodeType::InputSlot {
                    expected_media_urn,
                    cardinality,
                    ..
                } => Some((expected_media_urn.clone(), *cardinality)),
                _ => None,
            })
            .collect();
        assert_eq!(slot_cardinalities.len(), 2);
        for (urn, cardinality) in slot_cardinalities {
            if urn.contains("pdf") {
                assert_eq!(cardinality, InputCardinality::Sequence);
            } else {
                assert_eq!(cardinality, InputCardinality::Single);
            }
        }
    }
}
