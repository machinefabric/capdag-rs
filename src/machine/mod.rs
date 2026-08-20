//! Machine notation — anchor-realized DAG of capability strands.
//!
//! A `Machine` is the canonical, anchor-realized form of one
//! or more capability strands. Each strand inside a machine is
//! a `MachineStrand` — a maximal connected sub-graph of resolved
//! cap edges with explicit input and output anchors.
//!
//! See the [machine notation specification](../../../docs/09-machine-notation.md).
//! for the full specification.
//!
//! ## Layers
//!
//! - `Strand` (planner) — linear cap-step sequence, no anchors
//! - `Machine` (this module) — anchor-realized graph
//! - `MachineRun` — concrete execution against actual inputs
//!
//! ## Format
//!
//! Machine notation has two equally valid surface forms:
//!
//! ```text
//! [extract cap:in="media:ext=pdf";extract-text;out="media:enc=utf-8;ext=txt"]
//! [embed cap:in="media:enc=utf-8";generate-embeddings;out="media:embedding-vector;enc=utf-8;record"]
//! [doc -> extract -> text]
//! [text -> embed -> vectors]
//! ```
//!
//! and the line-based form (one statement per line, no
//! brackets). Both can be freely mixed in the same input.

pub mod error;
pub mod graph;
pub mod notation_ast;
pub mod parser;
pub mod realize;
pub mod resolve;
pub mod serializer;

#[cfg(test)]
pub(crate) mod test_fixtures;

pub use error::{MachineAbstractionError, MachineParseError, MachineSyntaxError};
pub use graph::{
    EdgeAssignmentBinding, Machine, MachineEdge, MachineRun, MachineRunStatus, MachineStrand,
    NodeId,
};
pub use notation_ast::{
    build_editor_model, byte_offset_to_position, emit_semantic_tokens, get_completion_context,
    parse_notation_ast, CompletionContextType, NotationAST, NotationEntityInfo, NotationEntityKind,
    NotationGraphElementInfo, NotationGraphElementKind, NotationPosition, NotationSpan,
    ParsedHeader, ParsedStatement, ParsedWiring, SemanticTokenInfo, SemanticTokenType,
};
pub use parser::{
    parse_machine, parse_machine_async, parse_machine_with_node_names,
    parse_machine_with_node_names_async, StrandNodeNames,
};
pub use realize::{realize_strand, realize_strand_with_anchor_sources};
pub use resolve::assign_sources_to_anchors;
pub use serializer::NotationFormat;
