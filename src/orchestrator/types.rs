//! Types for machine notation orchestration
//!
//! This module defines the error types and IR structures used by the orchestrator.

use crate::machine::StepPolicy;
use crate::planner::StepToken;
use crate::Cap;
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

    /// Graph contains a cycle (not a DAG)
    #[error("Graph is not a DAG, contains cycle involving nodes: {cycle_nodes:?}")]
    NotADag { cycle_nodes: Vec<String> },

    /// The plan or graph is not one that can be run as asked
    #[error("Invalid graph: {message}")]
    InvalidGraph { message: String },

}

// =============================================================================
// IR Structures
// =============================================================================

/// What one input of a step is sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepSource {
    /// One node's value, as it is.
    Node(String),
    /// Several nodes' values gathered, in order, into one sequence: a single
    /// value contributes one item, a sequence its items. With no members it
    /// is an empty sequence.
    Gather(Vec<String>),
}

impl StepSource {
    /// Every node this source reads, in order.
    pub fn nodes(&self) -> &[String] {
        match self {
            StepSource::Node(node) => std::slice::from_ref(node),
            StepSource::Gather(nodes) => nodes,
        }
    }
}

/// One input stream of a step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepInput {
    /// The URN the stream is delivered under: the cap's `in=` for its main
    /// input, the arg's stream URN for any other. A cartridge tells its
    /// inputs apart by this.
    pub stream_urn: String,
    pub source: StepSource,
}

/// An output of a step other than its main one that something takes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepOutput {
    /// The output's media URN in the cap's definition: what its response
    /// stream names.
    pub output_media_urn: String,
    /// The node its value is stored at.
    pub node: String,
}

/// One step of a resolved graph: one invocation of a cap.
#[derive(Debug, Clone)]
pub struct ResolvedStep {
    /// Stable per-step identity carried from the originating strand step (via
    /// the plan's Cap node). The key by which a run's live progress maps back
    /// to the graph element a client rendered.
    pub token_id: StepToken,
    /// The step's node: where its MAIN output is stored.
    pub node: String,
    /// Cap URN string
    pub cap_urn: String,
    /// Resolved cap definition
    pub cap: Cap,
    /// The streams the step is sent, besides the values its args are given
    /// from the run's argument ledger. None for a step nothing is wired into.
    pub inputs: Vec<StepInput>,
    /// The outputs other than the main one that something takes. A response
    /// stream for any other declared output is audited, consumed and dropped.
    pub outputs: Vec<StepOutput>,
    /// The media URN of the main output, from the cap definition.
    pub out_media: String,
    /// What the step does when it fails.
    pub policy: StepPolicy,
}

impl ResolvedStep {
    /// Every node the step reads, in input order.
    pub fn source_nodes(&self) -> impl Iterator<Item = &String> {
        self.inputs.iter().flat_map(|input| input.source.nodes().iter())
    }

    /// Every node the step produces: its own, then its other taken outputs'.
    pub fn produced_nodes(&self) -> impl Iterator<Item = &String> {
        std::iter::once(&self.node).chain(self.outputs.iter().map(|output| &output.node))
    }
}

/// A resolved graph: steps with the cap definitions they run, wired by the
/// nodes they read and produce. A node no step of the graph produces is an
/// input of the graph: its value is supplied when the graph is run.
#[derive(Debug, Clone)]
pub struct ResolvedGraph {
    /// Map from node name to the media URN it holds
    pub nodes: HashMap<String, String>,
    /// The steps, in no particular order
    pub steps: Vec<ResolvedStep>,
    /// Original graph name (if any)
    pub graph_name: Option<String>,
}
