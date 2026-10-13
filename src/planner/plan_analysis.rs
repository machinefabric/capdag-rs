//! Pure `MachinePlan` analysis used by plan execution.
//!
//! These functions derive output nodes, the media URN a node holds in a run, and the
//! step that produces it — all from the plan structure alone, with no engine
//! dependency. They live in capdag's planner so the reference executor and the engine
//! share one implementation.

use std::collections::{BTreeMap, HashMap};

use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::MediaUrn;

use super::plan::{EdgeType, ExecutionNodeType, MachinePlan, MachinePlanEdge};
use super::{PlannerError, PlannerResult};

/// Resolve a plan's terminal output node and its data from executed node data.
///
/// Tries each declared output node directly, then through an `Output` meta-node to its
/// source. Fails hard if no output data is present — the executor must not silently
/// return empty.
pub fn resolve_plan_output(
    plan: &MachinePlan,
    node_data: &HashMap<String, Vec<Vec<u8>>>,
) -> PlannerResult<(String, Vec<Vec<u8>>)> {
    for output_id in &plan.output_nodes {
        if let Some(items) = node_data.get(output_id) {
            return Ok((output_id.clone(), items.clone()));
        }
        if let Some(node) = plan.get_node(output_id) {
            if let ExecutionNodeType::Output { source_node, .. } = &node.node_type {
                if let Some(items) = node_data.get(source_node) {
                    return Ok((output_id.clone(), items.clone()));
                }
            }
        }
    }

    Err(PlannerError::Internal(format!(
        "No output data found. output_nodes={:?}, available={:?}",
        plan.output_nodes,
        node_data.keys().collect::<Vec<_>>()
    )))
}

/// Derive the runtime output media URN for a plan's output node by walking back to the
/// cap that produces it and applying each cap to its runtime input.
pub fn derive_output_media_urn(plan: &MachinePlan, output_node_id: &str) -> PlannerResult<String> {
    Ok(resolve_node_output_media(plan, output_node_id, &mut BTreeMap::new())?.to_string())
}

/// The media URN a step's main input holds in a run: what is wired into it, or
/// its declared `in=` when nothing is (a source, or a main input bound to a
/// constant).
fn step_runtime_input(
    plan: &MachinePlan,
    node_id: &str,
    cap: &CapUrn,
    memo: &mut BTreeMap<String, MediaUrn>,
) -> PlannerResult<MediaUrn> {
    let main: Vec<&MachinePlanEdge> = plan
        .edges
        .iter()
        .filter(|edge| edge.to_node == node_id && matches!(edge.edge_type, EdgeType::Direct))
        .collect();
    match main.as_slice() {
        [edge] => resolve_node_output_media(plan, &edge.from_node, memo),
        [] => cap.in_media_urn().map_err(|e| {
            PlannerError::Internal(format!(
                "step '{}': cap `in=` is not a valid media URN: {}",
                node_id, e
            ))
        }),
        several => Err(PlannerError::Internal(format!(
            "step '{}' has {} edges into its main input; a main input has one source",
            node_id,
            several.len()
        ))),
    }
}

fn resolve_node_output_media(
    plan: &MachinePlan,
    node_id: &str,
    memo: &mut BTreeMap<String, MediaUrn>,
) -> PlannerResult<MediaUrn> {
    if let Some(cached) = memo.get(node_id) {
        return Ok(cached.clone());
    }

    let node = plan
        .get_node(node_id)
        .ok_or_else(|| PlannerError::Internal(format!("Plan node '{}' not found", node_id)))?;
    let parse = |what: &str, urn: &str| {
        MediaUrn::from_string(urn).map_err(|e| {
            PlannerError::Internal(format!("Failed to parse {what} media URN '{urn}': {e}"))
        })
    };
    let step_cap = |id: &str| -> PlannerResult<CapUrn> {
        match plan.get_node(id).map(|n| &n.node_type) {
            Some(ExecutionNodeType::Cap { cap_urn, .. }) => CapUrn::from_string(cap_urn)
                .map_err(|e| {
                    PlannerError::Internal(format!("Failed to parse cap URN '{}': {}", cap_urn, e))
                }),
            _ => Err(PlannerError::Internal(format!("Plan node '{}' is not a step", id))),
        }
    };

    let resolved = match &node.node_type {
        ExecutionNodeType::Output { source_node, .. } => {
            resolve_node_output_media(plan, source_node, memo)?
        }
        ExecutionNodeType::InputSlot {
            expected_media_urn, ..
        } => parse("input slot", expected_media_urn)?,
        ExecutionNodeType::Item { media_urn, .. } => parse("item", media_urn)?,
        ExecutionNodeType::Collect {
            output_media_urn, ..
        } => parse("gather", output_media_urn)?,
        ExecutionNodeType::Cap { cap_urn, .. } => {
            let cap = step_cap(node_id)?;
            let runtime_input = step_runtime_input(plan, node_id, &cap, memo)?;
            cap.apply_to_runtime_input_media(&runtime_input).map_err(|e| {
                PlannerError::Internal(format!(
                    "Failed to apply cap '{}' to runtime input '{}': {}",
                    cap_urn, runtime_input, e
                ))
            })?
        }
        // An output other than the main one holds its own type — or, when it
        // carries the input, what the step was given.
        ExecutionNodeType::CapOutput {
            cap_node,
            output_media_urn,
            carries_input,
            ..
        } => {
            if *carries_input {
                let cap = step_cap(cap_node)?;
                step_runtime_input(plan, cap_node, &cap, memo)?
            } else {
                parse("output", output_media_urn)?
            }
        }
        ExecutionNodeType::ForEach { .. } => {
            return Err(PlannerError::Internal(format!(
                "Plan node '{}' is a map: it holds no value of its own",
                node_id
            )));
        }
    };

    memo.insert(node_id.to_string(), resolved.clone());
    Ok(resolved)
}

/// Walk back from a node to the step that produces it and return that step's cap
/// URN — the cap whose realized-path edge live activity lights up. A gather is
/// produced by its first member's step; an item by its source's. Fails hard if
/// the node resolves to an input of the machine.
pub fn derive_output_producing_cap_urn(
    plan: &MachinePlan,
    output_node_id: &str,
) -> PlannerResult<String> {
    fn walk(plan: &MachinePlan, node_id: &str, depth: usize) -> PlannerResult<String> {
        if depth > plan.nodes.len() + 1 {
            return Err(PlannerError::Internal(format!(
                "Cycle while resolving producing cap at node '{}'",
                node_id
            )));
        }
        let node = plan
            .get_node(node_id)
            .ok_or_else(|| PlannerError::Internal(format!("Plan node '{}' not found", node_id)))?;
        match &node.node_type {
            ExecutionNodeType::Cap { cap_urn, .. } => Ok(cap_urn.clone()),
            ExecutionNodeType::CapOutput { cap_node, .. } => walk(plan, cap_node, depth + 1),
            ExecutionNodeType::Output { source_node, .. } => walk(plan, source_node, depth + 1),
            ExecutionNodeType::Item { source, .. } => walk(plan, source, depth + 1),
            ExecutionNodeType::Collect { input_nodes, .. } => {
                let first = input_nodes.first().ok_or_else(|| {
                    PlannerError::Internal(format!(
                        "Node '{}' has no input nodes while resolving producing cap",
                        node_id
                    ))
                })?;
                walk(plan, first, depth + 1)
            }
            ExecutionNodeType::ForEach { .. } => Err(PlannerError::Internal(format!(
                "Plan node '{}' is a map: no step produces it",
                node_id
            ))),
            ExecutionNodeType::InputSlot { .. } => Err(PlannerError::Internal(format!(
                "Output node '{}' resolves to an input slot with no producing cap",
                node_id
            ))),
        }
    }
    walk(plan, output_node_id, 0)
}

/// The media URN of the items of a map's source: the type the source node holds.
/// A map changes shape (a sequence becomes one value per run), not type, so the
/// item URN is the source's URN. Returned parsed and as a string.
pub fn derive_foreach_media_urns(
    plan: &MachinePlan,
    source_node_id: &str,
) -> PlannerResult<(MediaUrn, String)> {
    let media_urn = resolve_node_output_media(plan, source_node_id, &mut BTreeMap::new())?;
    let item_urn_str = media_urn.to_string();
    Ok((media_urn, item_urn_str))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::plan::{ForEachSource, MachineNode};
    use crate::planner::InputCardinality;

    const SPLIT: &str = r#"cap:in="media:document;ext=pdf";split;out="media:page;textable""#;
    const ROUTE: &str = r#"cap:in="media:textable";route;out="media:textable""#;

    /// doc → split (a sequence of pages) → map [ item → route ] with route's
    /// `rejected` output (carrying the input) and `score` output taken, and a
    /// source step beside them.
    fn plan() -> MachinePlan {
        let mut plan = MachinePlan::new("analysis");
        plan.add_node(MachineNode::input_slot("doc", "input", "media:document;ext=pdf", InputCardinality::Single));
        plan.add_node(MachineNode::cap("split", SPLIT));
        plan.add_node(MachineNode::for_each(
            "each",
            vec![ForEachSource { node: "split".to_string(), item: "page".to_string() }],
            vec!["page".to_string(), "route".to_string(), "rejected".to_string(), "score".to_string()],
        ));
        plan.add_node(MachineNode::item("page", "each", "split", "media:page;textable"));
        plan.add_node(MachineNode::cap("route", ROUTE));
        plan.add_node(MachineNode::cap_output("rejected", "route", "media:rejected", "rejected", true));
        plan.add_node(MachineNode::cap_output("score", "route", "media:numeric;score", "score", false));
        plan.add_node(MachineNode::cap("now", r#"cap:in="media:void";now;out="media:stamp""#));
        plan.add_node(MachineNode::collect("both", vec!["route".to_string(), "rejected".to_string()], "media:textable"));
        plan.add_node(MachineNode::output("out", "result", "rejected"));
        plan.add_edge(MachinePlanEdge::direct("doc", "split"));
        plan.add_edge(MachinePlanEdge::direct("split", "each"));
        plan.add_edge(MachinePlanEdge::iteration("each", "page"));
        plan.add_edge(MachinePlanEdge::direct("page", "route"));
        plan.add_edge(MachinePlanEdge::direct("route", "rejected"));
        plan.add_edge(MachinePlanEdge::direct("route", "score"));
        plan.add_edge(MachinePlanEdge::collection("route", "both"));
        plan.add_edge(MachinePlanEdge::collection("rejected", "both"));
        plan.add_edge(MachinePlanEdge::direct("rejected", "out"));
        plan.validate().expect("valid");
        plan
    }

    fn media(plan: &MachinePlan, node: &str) -> String {
        derive_output_media_urn(plan, node).unwrap_or_else(|e| panic!("{node}: {e}"))
    }

    fn same(a: &str, b: &str) -> bool {
        MediaUrn::from_string(a).unwrap().is_equivalent(&MediaUrn::from_string(b).unwrap()).unwrap()
    }

    // TEST12755: the media a node holds in a run, by what the node is. A
    // step's main output is its cap applied to what it was given; an item is
    // its source's element; an output that carries the input holds what the
    // step was given and any other output its own type; a source step is
    // applied to its declared (void) input; a plan output holds its source's.
    #[test]
    fn test12755_the_media_of_a_node_follows_what_the_node_is() {
        let plan = plan();
        assert!(same(&media(&plan, "split"), "media:page;textable"));
        assert!(same(&media(&plan, "page"), "media:page;textable"));
        // route's main output: the declared out= refined by the page it was given.
        let routed = media(&plan, "route");
        assert!(
            MediaUrn::from_string(&routed).unwrap().conforms_to(&MediaUrn::from_string("media:textable").unwrap()).unwrap(),
            "{routed}"
        );
        // The output that carries the input holds the page, not `media:rejected`.
        assert!(same(&media(&plan, "rejected"), "media:page;textable"), "{}", media(&plan, "rejected"));
        assert!(same(&media(&plan, "score"), "media:numeric;score"));
        assert!(same(&media(&plan, "now"), "media:stamp"));
        assert!(same(&media(&plan, "both"), "media:textable"));
        assert!(same(&media(&plan, "out"), &media(&plan, "rejected")));
        let (item, text) = derive_foreach_media_urns(&plan, "split").unwrap();
        assert!(same(&text, "media:page;textable"));
        assert_eq!(item.to_string(), text);
        // A map holds no value.
        assert!(derive_output_media_urn(&plan, "each").unwrap_err().to_string().contains("is a map"));
    }

    // TEST12756: the step that produces a node: its own for a step, the step
    // whose output it is for an output node, its source's for an item and a
    // plan output, its first member's for a gather. An input of the machine
    // has none.
    #[test]
    fn test12756_the_step_that_produces_a_node() {
        let plan = plan();
        let producer = |node: &str| derive_output_producing_cap_urn(&plan, node).unwrap();
        assert_eq!(producer("route"), ROUTE);
        assert_eq!(producer("rejected"), ROUTE);
        assert_eq!(producer("out"), ROUTE);
        assert_eq!(producer("page"), SPLIT);
        assert_eq!(producer("both"), ROUTE);
        assert!(derive_output_producing_cap_urn(&plan, "doc").is_err());
        assert!(derive_output_producing_cap_urn(&plan, "each").is_err());
    }

    // TEST12757: a step whose main input two edges feed is refused: a main
    // input has one source (a gather is one node).
    #[test]
    fn test12757_a_main_input_has_one_source() {
        let mut plan = plan();
        plan.add_edge(MachinePlanEdge::direct("doc", "route"));
        let error = derive_output_media_urn(&plan, "route").unwrap_err().to_string();
        assert!(error.contains("2 edges into its main input"), "{error}");
    }
}
