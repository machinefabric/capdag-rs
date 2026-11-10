//! Diagram rendering for execution plans.
//!
//! This renders the full [`MachinePlan`] the planner produces, so every
//! construct machine notation can model appears: `Cap` steps and the outputs
//! they are taken by, `ForEach` maps and their items, `Collect` gathers,
//! `InputSlot`s and `Output`s, plus every typed edge (`Direct`, `Arg`,
//! `Iteration`, `Collection`).
//! A machine can compile to several strands, so a list of plans renders as one
//! diagram with one labelled subgraph per plan.
//!
//! Caps and media types are named as [`DiagramNames`] names them: by the
//! aliases a person reads, or by their URNs.

use super::plan::{EdgeType, ExecutionNodeType, MachineNode, MachinePlan};
use std::collections::HashMap;

/// How a diagram names the caps and media types it shows, each given its URN.
pub struct DiagramNames<'a> {
    pub cap: &'a dyn Fn(&str) -> String,
    pub media: &'a dyn Fn(&str) -> String,
}

/// One rendered node: a diagram-safe identifier plus the human label lines and
/// the semantic role (which drives the shape in each backend).
struct RenderedNode {
    id: String,
    label_lines: Vec<String>,
    role: NodeRole,
}

#[derive(Clone, Copy, PartialEq)]
enum NodeRole {
    Input,
    Output,
    Cap,
    ForEach,
    Item,
    Collect,
    CapOutput,
}

/// Human-readable label lines and role for a node, independent of backend.
fn describe_node(node: &MachineNode, names: &DiagramNames<'_>) -> (Vec<String>, NodeRole) {
    match &node.node_type {
        ExecutionNodeType::Cap {
            cap_urn,
            preferred_cap,
            ..
        } => {
            let mut lines = vec![node.id.clone(), (names.cap)(cap_urn)];
            if let Some(pref) = preferred_cap {
                lines.push(format!("prefer: {}", (names.cap)(pref)));
            }
            (lines, NodeRole::Cap)
        }
        ExecutionNodeType::ForEach { .. } => (
            vec![node.id.clone(), "ForEach".to_string()],
            NodeRole::ForEach,
        ),
        ExecutionNodeType::Collect {
            output_media_urn, ..
        } => (
            vec![node.id.clone(), "Collect".to_string(), (names.media)(output_media_urn)],
            NodeRole::Collect,
        ),
        ExecutionNodeType::Item { media_urn, .. } => (
            vec![node.id.clone(), "item".to_string(), (names.media)(media_urn)],
            NodeRole::Item,
        ),
        ExecutionNodeType::CapOutput {
            output_name,
            output_media_urn,
            ..
        } => (
            vec![output_name.clone(), (names.media)(output_media_urn)],
            NodeRole::CapOutput,
        ),
        ExecutionNodeType::InputSlot {
            slot_name,
            expected_media_urn,
            cardinality,
        } => (
            vec![
                slot_name.clone(),
                (names.media)(expected_media_urn),
                format!("{cardinality:?}"),
            ],
            NodeRole::Input,
        ),
        ExecutionNodeType::Output { output_name, .. } => {
            (vec![output_name.clone()], NodeRole::Output)
        }
    }
}

/// The label shown ON an edge, by type (empty for a plain direct edge).
fn edge_label(edge_type: &EdgeType, names: &DiagramNames<'_>) -> String {
    match edge_type {
        EdgeType::Direct => String::new(),
        EdgeType::Arg { arg_urn } => format!("arg: {}", (names.media)(arg_urn)),
        EdgeType::Iteration => "each".to_string(),
        EdgeType::Collection => "collect".to_string(),
    }
}

/// Assign each plan node a diagram-safe id (`p<plan>_n<seq>`), deterministically
/// (nodes are sorted by their original id so output is stable).
fn assign_ids(plan_idx: usize, plan: &MachinePlan, names: &DiagramNames<'_>) -> HashMap<String, RenderedNode> {
    let mut ids: Vec<&String> = plan.nodes.keys().collect();
    ids.sort();
    ids.into_iter()
        .enumerate()
        .map(|(seq, node_id)| {
            let node = &plan.nodes[node_id];
            let (label_lines, role) = describe_node(node, names);
            (
                node_id.clone(),
                RenderedNode {
                    id: format!("p{plan_idx}_n{seq}"),
                    label_lines,
                    role,
                },
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Mermaid
// ---------------------------------------------------------------------------

/// Render the plans as a single Mermaid `flowchart`, one subgraph per plan.
pub fn plans_to_mermaid(plans: &[MachinePlan], names: &DiagramNames<'_>) -> String {
    let mut out = String::from("flowchart LR\n");
    for (i, plan) in plans.iter().enumerate() {
        let rendered = assign_ids(i, plan, names);
        let title = mermaid_escape(if plan.name.is_empty() {
            "plan"
        } else {
            plan.name.as_str()
        });
        out.push_str(&format!("  subgraph p{i} [\"{title}\"]\n"));
        // Stable node order.
        let mut nodes: Vec<&RenderedNode> = rendered.values().collect();
        nodes.sort_by(|a, b| a.id.cmp(&b.id));
        for rn in nodes {
            let label = rn
                .label_lines
                .iter()
                .map(|l| mermaid_escape(l))
                .collect::<Vec<_>>()
                .join("<br/>");
            let (open, close) = match rn.role {
                NodeRole::Input => ("([\"", "\"])"),
                NodeRole::Output => ("(((\"", "\")))"),
                NodeRole::ForEach => ("{{\"", "\"}}"),
                NodeRole::Collect => ("[[\"", "\"]]"),
                NodeRole::Item | NodeRole::CapOutput => ("(\"", "\")"),
                NodeRole::Cap => ("[\"", "\"]"),
            };
            out.push_str(&format!("    {}{}{}{}\n", rn.id, open, label, close));
        }
        for edge in &plan.edges {
            let (Some(from), Some(to)) =
                (rendered.get(&edge.from_node), rendered.get(&edge.to_node))
            else {
                continue;
            };
            let label = edge_label(&edge.edge_type, names);
            let thick = matches!(edge.edge_type, EdgeType::Iteration | EdgeType::Collection);
            let arrow = if thick { "==>" } else { "-->" };
            if label.is_empty() {
                out.push_str(&format!("    {} {} {}\n", from.id, arrow, to.id));
            } else {
                out.push_str(&format!(
                    "    {} {}|\"{}\"| {}\n",
                    from.id,
                    arrow,
                    mermaid_escape(&label),
                    to.id
                ));
            }
        }
        out.push_str("  end\n");
    }
    out
}

fn mermaid_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "#quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

// ---------------------------------------------------------------------------
// Graphviz DOT
// ---------------------------------------------------------------------------

/// Render the plans as a single Graphviz `digraph`, one `cluster` subgraph per
/// plan. Same node roles and edge semantics as [`plans_to_mermaid`].
pub fn plans_to_dot(plans: &[MachinePlan], names: &DiagramNames<'_>) -> String {
    let mut out = String::from("digraph {\n  rankdir=LR;\n");
    for (i, plan) in plans.iter().enumerate() {
        let rendered = assign_ids(i, plan, names);
        let title = dot_escape(if plan.name.is_empty() {
            "plan"
        } else {
            plan.name.as_str()
        });
        out.push_str(&format!(
            "  subgraph cluster_{i} {{\n    label=\"{title}\";\n"
        ));
        let mut nodes: Vec<&RenderedNode> = rendered.values().collect();
        nodes.sort_by(|a, b| a.id.cmp(&b.id));
        for rn in nodes {
            let label = rn
                .label_lines
                .iter()
                .map(|l| dot_escape(l))
                .collect::<Vec<_>>()
                .join("\\n");
            let shape = match rn.role {
                NodeRole::Input => "ellipse",
                NodeRole::Output => "doublecircle",
                NodeRole::ForEach => "hexagon",
                NodeRole::Collect => "box3d",
                NodeRole::Item | NodeRole::CapOutput => "oval",
                NodeRole::Cap => "box",
            };
            out.push_str(&format!(
                "    \"{}\" [label=\"{}\", shape={}];\n",
                rn.id, label, shape
            ));
        }
        for edge in &plan.edges {
            let (Some(from), Some(to)) =
                (rendered.get(&edge.from_node), rendered.get(&edge.to_node))
            else {
                continue;
            };
            let label = edge_label(&edge.edge_type, names);
            let style = if matches!(edge.edge_type, EdgeType::Iteration | EdgeType::Collection) {
                ", style=bold"
            } else {
                ""
            };
            if label.is_empty() {
                out.push_str(&format!("    \"{}\" -> \"{}\";\n", from.id, to.id));
            } else {
                out.push_str(&format!(
                    "    \"{}\" -> \"{}\" [label=\"{}\"{}];\n",
                    from.id,
                    to.id,
                    dot_escape(&label),
                    style
                ));
            }
        }
        out.push_str("  }\n");
    }
    out.push_str("}\n");
    out
}

fn dot_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::cardinality::InputCardinality;
    use crate::planner::plan::MachinePlanEdge;

    const DISBIND: &str = r#"cap:in="media:document;ext=pdf";disbind-pdf;out="media:enc=utf8;ext=txt;page;plain-text""#;
    const PDF: &str = "media:document;ext=pdf";

    fn plan() -> MachinePlan {
        let mut plan = MachinePlan::new("pages");
        plan.add_node(MachineNode::input_slot("doc", "doc", PDF, InputCardinality::Single));
        plan.add_node(MachineNode::cap("step_0", DISBIND));
        plan.add_node(MachineNode::output("pages", "pages", "step_0"));
        plan.add_edge(MachinePlanEdge::direct("doc", "step_0"));
        plan.add_edge(MachinePlanEdge::direct("step_0", "pages"));
        plan
    }

    fn aliases() -> DiagramNames<'static> {
        DiagramNames {
            cap: &|urn| if urn == DISBIND { "disbind-pdf".to_string() } else { urn.to_string() },
            media: &|urn| if urn == PDF { "pdf".to_string() } else { urn.to_string() },
        }
    }

    // TEST20106: a diagram names caps and media types as it is told to — by
    // their aliases, with no URN in it — and by their URNs when told to.
    #[test]
    fn test20106_diagrams_name_caps_and_media_as_told() {
        for source in [plans_to_mermaid(&[plan()], &aliases()), plans_to_dot(&[plan()], &aliases())] {
            assert!(source.contains("disbind-pdf"), "{source}");
            assert!(source.contains("pdf"), "{source}");
            assert!(!source.contains("cap:"), "no cap URN in an aliased diagram:\n{source}");
            assert!(!source.contains("media:"), "no media URN in an aliased diagram:\n{source}");
        }
        let urns = DiagramNames { cap: &|urn| urn.to_string(), media: &|urn| urn.to_string() };
        let source = plans_to_dot(&[plan()], &urns);
        assert!(source.contains("disbind-pdf;out="), "{source}");
        assert!(source.contains("media:document;ext=pdf"), "{source}");
    }
}
