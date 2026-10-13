//! Diagram rendering for execution plans.
//!
//! Unlike the orchestrator's `ResolvedGraph::to_mermaid` (a FLAT cap-to-cap
//! view that cannot express fan-out/fan-in), this renders the full
//! [`MachinePlan`] the planner produces — so every construct machine notation
//! can model appears: `Cap` steps and the outputs they are taken by, `ForEach`
//! maps and their items, `Collect` gathers, `InputSlot`s and `Output`s, plus
//! every typed edge (`Direct`, `Arg`, `Iteration`, `Collection`).
//! A machine can compile to several strands, so a list of plans renders as one
//! diagram with one labelled subgraph per plan.

use super::plan::{EdgeType, ExecutionNodeType, MachineNode, MachinePlan};
use std::collections::HashMap;

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
fn describe_node(node: &MachineNode) -> (Vec<String>, NodeRole) {
    match &node.node_type {
        ExecutionNodeType::Cap {
            cap_urn,
            preferred_cap,
            ..
        } => {
            let mut lines = vec![node.id.clone(), cap_urn.clone()];
            if let Some(pref) = preferred_cap {
                lines.push(format!("prefer: {pref}"));
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
            vec![node.id.clone(), "Collect".to_string(), output_media_urn.clone()],
            NodeRole::Collect,
        ),
        ExecutionNodeType::Item { media_urn, .. } => (
            vec![node.id.clone(), "item".to_string(), media_urn.clone()],
            NodeRole::Item,
        ),
        ExecutionNodeType::CapOutput {
            output_name,
            output_media_urn,
            ..
        } => (
            vec![output_name.clone(), output_media_urn.clone()],
            NodeRole::CapOutput,
        ),
        ExecutionNodeType::InputSlot {
            slot_name,
            expected_media_urn,
            cardinality,
        } => (
            vec![
                slot_name.clone(),
                expected_media_urn.clone(),
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
fn edge_label(edge_type: &EdgeType) -> String {
    match edge_type {
        EdgeType::Direct => String::new(),
        EdgeType::Arg { arg_urn } => format!("arg: {arg_urn}"),
        EdgeType::Iteration => "each".to_string(),
        EdgeType::Collection => "collect".to_string(),
    }
}

/// Assign each plan node a diagram-safe id (`p<plan>_n<seq>`), deterministically
/// (nodes are sorted by their original id so output is stable).
fn assign_ids(plan_idx: usize, plan: &MachinePlan) -> HashMap<String, RenderedNode> {
    let mut ids: Vec<&String> = plan.nodes.keys().collect();
    ids.sort();
    ids.into_iter()
        .enumerate()
        .map(|(seq, node_id)| {
            let node = &plan.nodes[node_id];
            let (label_lines, role) = describe_node(node);
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
pub fn plans_to_mermaid(plans: &[MachinePlan]) -> String {
    let mut out = String::from("flowchart LR\n");
    for (i, plan) in plans.iter().enumerate() {
        let rendered = assign_ids(i, plan);
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
            let label = edge_label(&edge.edge_type);
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
pub fn plans_to_dot(plans: &[MachinePlan]) -> String {
    let mut out = String::from("digraph {\n  rankdir=LR;\n");
    for (i, plan) in plans.iter().enumerate() {
        let rendered = assign_ids(i, plan);
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
            let label = edge_label(&edge.edge_type);
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
