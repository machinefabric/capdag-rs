//! Types for machine notation orchestration
//!
//! This module defines the error types and IR structures used by the orchestrator.

use crate::planner::StepToken;
use crate::{Cap, InputStructure};
use std::collections::HashMap;
use thiserror::Error;

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur during machine notation parsing and orchestration
#[derive(Debug, Error)]
pub enum ParseOrchestrationError {
    /// Machine notation parsing failed
    #[error("Machine notation parse failed: {0}")]
    MachineSyntaxParseFailed(String),

    /// Cap URN not found in registry
    #[error("Cap URN '{cap_urn}' not found in registry")]
    CapNotFound { cap_urn: String },

    /// Node media URN conflicts with existing assignment
    #[error(
        "Node '{node}' has conflicting media URNs: existing='{existing}', required_by_cap='{required_by_cap}'"
    )]
    NodeMediaConflict {
        node: String,
        existing: String,
        required_by_cap: String,
    },

    /// Graph contains a cycle (not a DAG)
    #[error("Graph is not a DAG, contains cycle involving nodes: {cycle_nodes:?}")]
    NotADag { cycle_nodes: Vec<String> },

    /// Graph contains unsupported or undecomposed control-flow nodes
    #[error("Invalid graph: {message}")]
    InvalidGraph { message: String },

    /// Cap URN parsing error
    #[error("Failed to parse Cap URN: {0}")]
    CapUrnParseError(String),

    /// Media URN parsing error
    #[error("Failed to parse Media URN: {0}")]
    MediaUrnParseError(String),

    /// Registry error
    #[error("Registry error: {0}")]
    FabricRegistryError(String),

    /// Structure mismatch between connected nodes (record vs opaque)
    #[error(
        "Structure mismatch at node '{node}': source is {source_structure:?} but cap expects {expected_structure:?}"
    )]
    StructureMismatch {
        node: String,
        source_structure: InputStructure,
        expected_structure: InputStructure,
    },
}

// =============================================================================
// IR Structures
// =============================================================================

/// A resolved edge in the orchestration graph
#[derive(Debug, Clone)]
pub struct ResolvedEdge {
    /// Stable per-step identity carried from the originating `StrandStep.token_id`
    /// (via the plan's Cap node). This is the key by which a run's live progress
    /// updates map deterministically back to the client-rendered graph element.
    pub token_id: StepToken,
    /// Source node name
    pub from: String,
    /// Target node name
    pub to: String,
    /// Cap URN string
    pub cap_urn: String,
    /// Resolved cap definition
    pub cap: Cap,
    /// Input media URN from cap definition
    pub in_media: String,
    /// Output media URN from cap definition
    pub out_media: String,
}

/// A resolved orchestration graph
#[derive(Debug, Clone)]
pub struct ResolvedGraph {
    /// Map from node name to derived media URN
    pub nodes: HashMap<String, String>,
    /// Resolved edges with cap definitions
    pub edges: Vec<ResolvedEdge>,
    /// Original graph name (if any)
    pub graph_name: Option<String>,
}

impl ResolvedGraph {
    /// Generate Mermaid flowchart code from this resolved graph.
    pub fn to_mermaid(&self) -> String {
        use std::collections::HashSet;
        let mut out = String::new();
        out.push_str("graph LR\n");

        let mut targets: HashSet<&str> = HashSet::new();
        let mut sources: HashSet<&str> = HashSet::new();
        for edge in &self.edges {
            sources.insert(&edge.from);
            targets.insert(&edge.to);
        }

        for (name, media_urn) in &self.nodes {
            let is_input = sources.contains(name.as_str()) && !targets.contains(name.as_str());
            let is_output = targets.contains(name.as_str()) && !sources.contains(name.as_str());

            let esc_name = mermaid_escape(name);
            let esc_urn = mermaid_escape(media_urn);

            if is_input {
                out.push_str(&format!(
                    "    {}([\"{}<br/><small>{}</small>\"])\n",
                    name, esc_name, esc_urn
                ));
            } else if is_output {
                out.push_str(&format!(
                    "    {}(((\"{}<br/><small>{}</small>\")))\n",
                    name, esc_name, esc_urn
                ));
            } else {
                out.push_str(&format!(
                    "    {}[\"{}<br/><small>{}</small>\"]\n",
                    name, esc_name, esc_urn
                ));
            }
        }

        out.push('\n');

        let mut seen_edges: HashSet<(String, String, String)> = HashSet::new();
        for edge in &self.edges {
            let key = (edge.from.clone(), edge.to.clone(), edge.cap_urn.clone());
            if !seen_edges.insert(key) {
                continue;
            }
            let title = mermaid_escape(&edge.cap.title);
            let urn = mermaid_escape(&edge.cap_urn);
            out.push_str(&format!(
                "    {} -->|\"{}<br/><small>{}</small>\"| {}\n",
                edge.from, title, urn, edge.to
            ));
        }

        out
    }
}

fn mermaid_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "#quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

// =============================================================================
// Cap Registry Trait
// =============================================================================

// `FabricRegistryTrait` is gone. The orchestrator parser used it
// only to indirect over the concrete `FabricRegistry` for testing,
// but under the new resolver regime `Machine::from_string`
// itself takes `&FabricRegistry` to compute source-to-arg
// matching. Tests use `FabricRegistry::new_for_test()` +
// `add_caps_to_cache(...)` instead of a hand-built mock.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::urn::cap_urn::CapUrn;
    use std::collections::HashMap;

    fn make_test_cap(urn: &str, title: &str) -> Cap {
        Cap::new(
            CapUrn::from_string(urn).expect("valid cap urn"),
            title.to_string(),
            vec!["test-command".to_string()],
        )
    }

    // TEST1142: ResolvedGraph.to_mermaid() renders node shapes, deduplicates edges, and escapes labels
    #[test]
    fn test1142_resolved_graph_to_mermaid_renders_shapes_dedupes_edges_and_escapes() {
        let cap = make_test_cap(
            r#"cap:in="media:ext=pdf";extract;out="media:enc=utf-8;ext=txt""#,
            r#"Extract "Title" <One>\path"#,
        );
        let second_cap = make_test_cap(
            r#"cap:in="media:enc=utf-8;ext=txt";embed;out="media:embedding;record""#,
            "Embed",
        );

        let graph = ResolvedGraph {
            nodes: HashMap::from([
                ("input".to_string(), r#"media:ext=pdf"#.to_string()),
                (
                    "middle".to_string(),
                    r#"media:enc=utf-8;ext=txt"#.to_string(),
                ),
                (
                    "output".to_string(),
                    r#"media:embedding;record"#.to_string(),
                ),
            ]),
            edges: vec![
                ResolvedEdge {
                    token_id: "tok-0".parse().unwrap(),
                    from: "input".to_string(),
                    to: "middle".to_string(),
                    cap_urn: cap.urn.to_string(),
                    cap: cap.clone(),
                    in_media: "media:ext=pdf".to_string(),
                    out_media: "media:enc=utf-8;ext=txt".to_string(),
                },
                ResolvedEdge {
                    token_id: "tok-1".parse().unwrap(),
                    from: "input".to_string(),
                    to: "middle".to_string(),
                    cap_urn: cap.urn.to_string(),
                    cap,
                    in_media: "media:ext=pdf".to_string(),
                    out_media: "media:enc=utf-8;ext=txt".to_string(),
                },
                ResolvedEdge {
                    token_id: "tok-2".parse().unwrap(),
                    from: "middle".to_string(),
                    to: "output".to_string(),
                    cap_urn: second_cap.urn.to_string(),
                    cap: second_cap,
                    in_media: "media:enc=utf-8;ext=txt".to_string(),
                    out_media: "media:embedding;record".to_string(),
                },
            ],
            graph_name: Some("demo".to_string()),
        };

        let mermaid = graph.to_mermaid();

        assert!(mermaid.starts_with("graph LR\n"));
        assert!(mermaid.contains(r#"input(["input<br/><small>media:ext=pdf</small>"])"#));
        assert!(mermaid.contains(r#"middle["middle<br/><small>media:enc=utf-8;ext=txt</small>"]"#));
        assert!(
            mermaid.contains(r#"output((("output<br/><small>media:embedding;record</small>")))"#)
        );
        assert!(mermaid.contains(r#"input -->|"Extract #quot;Title#quot; &lt;One&gt;\\path"#));
        assert!(mermaid.contains(r#"<small>cap:"#));
        assert_eq!(mermaid.matches("input -->|").count(), 1);
    }
}
