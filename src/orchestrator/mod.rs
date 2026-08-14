//! Orchestrator: Machine Notation Parser with CapDag Orchestration
//!
//! This module parses machine notation through `Machine::from_string`,
//! looks up the resolved caps in the registry, and produces a
//! validated executable DAG IR (`ResolvedGraph`).
//!
//! # Example
//!
//! ```ignore
//! use capdag::orchestrator::parse_machine_to_cap_dag;
//! use capdag::FabricRegistry;
//!
//! let route = r#"
//!     [extract cap:in="media:bytes;ext=pdf";extract;out="media:enc=utf-8;ext=txt"]
//!     [A -> extract -> B]
//! "#;
//!
//! let registry = FabricRegistry::new().await?;
//! let graph = parse_machine_to_cap_dag(route, &registry).await?;
//! ```

pub mod cbor_util;
pub mod cli_cap;
pub mod cli_output;
pub mod cli_runtime;
pub mod cli_writer;
pub mod execute_plan;
pub mod executor;
pub mod machine_plan;
pub mod parser;
pub mod plan_converter;
pub mod run_arguments;
pub mod stream_io;
pub mod transient;
pub mod types;

// Re-export key types
pub use types::{ParseOrchestrationError, ResolvedEdge, ResolvedGraph};

pub use parser::parse_machine_to_cap_dag;

pub use machine_plan::build_plans_from_notation;

pub use execute_plan::{
    execute_plan, BodyOutcomeFn, EngineRuntime, ForEachBodyCoordinate, ForEachItemSnapshot,
    ForEachItemsFn, OutputItem, PipelineItemFn, PipelineResult, PlanInput, SegmentOutput,
    WriterResult,
};

pub use cli_cap::{
    classify_cap_token, map_invocation, render_cap_interface, synthesize_single_cap_notation,
    CapInvocation, CapToken, SINGLE_CAP_INPUT_NODE, SINGLE_CAP_OUTPUT_NODE, SINGLE_CAP_STEP_NAME,
};
pub use cli_output::{emit_terminals, extension_for_media, EmitOptions};
pub use cli_runtime::CliRuntime;
pub use cli_writer::{CliDiskWriter, PART_FILE_PREFIX};
pub use transient::{
    read_sidecar, read_transient_item, TransientArtifact, TransientSidecar, TRANSIENT_DATA_FILE,
    TRANSIENT_SIDECAR, TRANSIENT_STAGING_SUFFIX,
};

pub use plan_converter::plan_to_resolved_graph;

pub use run_arguments::{
    AppliedArgumentUpdate, ArgumentUpdate, ArgumentUpdateDisposition, ArgumentUpdateOutcome,
    RunArgumentError, RunArgumentLedger,
};

pub use cbor_util::{
    assemble_cbor_array, assemble_cbor_sequence, split_cbor_array, split_cbor_sequence,
    wrap_raw_items_as_cbor_sequence, CborUtilError,
};

pub use executor::{
    execute_dag, map_progress, run_dag_on_context, CapProgressFn, CapStepProgressFn,
    CartridgeManager, DagOutput, EdgeGroup, ExecutionContext, ExecutionError, NodeData,
    ProgressMapper,
};

pub use stream_io::{
    collect_terminal_output, decode_terminal_output, send_one_stream, unwrap_cbor_value,
    ActivityTimer, CreditGrantFn, CreditPlumbing, FlowObserver, IncrementalWriter, PipelineLogFn,
    PipelineLogRecord, PipelineProgressTracker, SegmentWriterFactory, StreamIoError, TerminalItem,
    TerminalMeta, TerminalOutput, PIPELINE_STALL_TIMEOUT_SECS,
};
