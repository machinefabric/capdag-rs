//! Planner — planning, discovery, and execution for machines
//!
//! This module provides:
//! - **Shape analysis** from media URNs (cardinality + structure)
//! - **Argument binding** and resolution for cap execution
//! - **Execution plan** structures (DAG of caps)
//! - **Plan builder** — path finding and plan construction
//!
//! Plans are executed by the single ForEach/Collect-aware
//! [`execute_plan`](crate::orchestrator::execute_plan) in the orchestrator — there is
//! no planner-local executor.
//!
//! ## Shape Dimensions
//!
//! Media shapes have two orthogonal dimensions:
//!
//! 1. **Cardinality** - scalar (Single) vs list (Sequence)
//!    - Detected from `list` marker tag
//! 2. **Structure** - opaque vs record
//!    - Detected from `record` marker tag
//!
//! Both floom-engine (desktop app) and capdag CLI (CLI harness) use this same code.

use thiserror::Error;

pub mod argument_binding;
pub mod cardinality;
pub mod collection_input;
pub mod live_cap_fab;
pub mod plan;
pub mod plan_analysis;
pub mod plan_builder;
pub mod plan_engine;
pub mod plan_space;
pub mod viz;

// Re-exports - Shape types (cardinality + structure)
pub use argument_binding::{
    resolve_binding, ArgumentBinding, ArgumentBindings, ArgumentResolutionContext, ArgumentSource,
    CapFileMetadata, CapInputFile, ResolvedArgument, SourceEntityType, StrandInput,
};
pub use cardinality::{
    // Per-cap shape info and chain analysis
    CapShapeInfo,
    CardinalityCompatibility,
    CardinalityPattern,
    // Cardinality dimension
    InputCardinality,
    // Structure dimension
    InputStructure,
    // Combined shape
    MediaShape,
    ShapeCompatibility,
    StrandShapeAnalysis,
    StructureCompatibility,
};
pub use collection_input::{CapInputCollection, CollectionFile};
pub use live_cap_fab::{
    ArgSourceRef, CapInput, LiveCapFab, LiveMachinePlanEdge, LiveMachinePlanEdgeType,
    PathFindingEvent, ReachableTargetInfo, StepToken, StepTokenError, Strand, StrandStep,
    StrandStepType,
};
pub use plan::{
    BodyOutcome, EdgeType, ExecutionNodeType, MachineNode, MachinePlan, MachinePlanEdge,
    MachineResult, MergeStrategy, NodeExecutionResult, NodeId,
};
pub use plan_analysis::{
    derive_collected_media_urn, derive_foreach_media_urns, derive_output_media_urn,
    derive_output_producing_cap_urn, find_collect_for_foreach, resolve_plan_output,
};
pub use plan_builder::{
    ArgumentInfo, ArgumentResolution, MachinePlanBuilder, PathArgumentRequirements,
    StepArgumentRequirements,
};
pub use plan_space::{
    ConvergenceArity, ConvergenceLocation, ConvergenceMechanism, ConvergencePolicy,
    ConvergencePresence, ConvergentTargetInfo, ConvergentTargets, DivergenceLocation,
    DivergencePolicy, DivergencePresence, PlanApex, PlanCandidate, PlanCost, PlanError, PlanMode,
    PlanOutcome, PlanProfile, PlanRequest, RankPolicy, SearchDirection, SourceCardinality,
    SourceSpec, TargetSpec,
};
pub use viz::{plans_to_dot, plans_to_mermaid};

// =============================================================================
// Error Type
// =============================================================================

#[derive(Debug, Error)]
pub enum PlannerError {
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Registry error: {0}")]
    FabricRegistryError(String),
    #[error("Execution error: {0}")]
    ExecutionError(String),
    #[error("Invalid path: {0}")]
    InvalidPath(String),
}

pub type PlannerResult<T> = Result<T, PlannerError>;
