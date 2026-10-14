//! Orchestrator: running a machine.
//!
//! Machine notation resolves to a [`MachinePlan`](crate::planner::MachinePlan)
//! ([`build_plans_from_notation`]); [`execute_plan`] runs a plan on an
//! [`EngineRuntime`] — the capdag CLI's is [`CliRuntime`] — deciding what runs
//! when, what maps, and what is skipped, and handing each pipelined chain of
//! steps to the segment executor as a [`ResolvedGraph`].
//!
//! # Example
//!
//! ```ignore
//! use capdag::orchestrator::{build_plans_from_notation, execute_plan};
//!
//! let notation = r#"
//!     [extract cap:in="media:document;ext=pdf";extract;out="media:enc=utf-8;ext=txt"]
//!     [doc -> extract -> text]
//! "#;
//! let plans = build_plans_from_notation(notation, registry.clone()).await?;
//! ```

pub mod cbor_util;
pub mod cli_cap;
pub mod cli_output;
pub mod cli_runtime;
pub mod cli_writer;
pub mod execute_plan;
pub mod executor;
pub mod machine_plan;
pub mod plan_converter;
pub mod run_arguments;
pub mod stream_io;
pub mod transient;
pub mod types;

// Re-export key types
pub use types::{
    ParseOrchestrationError, ResolvedGraph, ResolvedStep, StepInput, StepOutput, StepSource,
};

pub use machine_plan::{
    build_plans_from_notation, build_plans_from_notation_from, MachineAnchor, ResolvedNotation,
    RunInputs, StrandPlan,
};

pub use execute_plan::{
    execute_plan, BodyOutcomeFn, EngineRuntime, ForEachBodyCoordinate, ForEachItemSnapshot,
    ForEachItemsFn, MapFailurePolicy, OutputItem, PipelineResult, PlanInput, ProducedTerminal,
    RunCallbacks, RunVerdict, SegmentOutput, StepFailure, StepOutcome, StepReport, StepReportFn,
    PlanTerminal, WriterResult,
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

pub use plan_converter::{plan_to_resolved_graph, steps_to_resolved_graph};

pub use run_arguments::{
    AppliedArgumentUpdate, ArgumentUpdate, ArgumentUpdateDisposition, ArgumentUpdateOutcome,
    RunArgumentError, RunArgumentLedger,
};

pub use cbor_util::{
    assemble_cbor_array, assemble_cbor_sequence, split_cbor_array, split_cbor_sequence,
    wrap_raw_items_as_cbor_sequence, CborUtilError,
};

pub use executor::{
    map_progress, run_dag_on_context, CapProgressFn, CapStepProgressFn, CartridgeManager,
    CollateralCause, DagOutput, ExecutionContext, ExecutionError, ProgressMapper, RunCancel,
};

pub use stream_io::{
    collect_response, collect_terminal_output, decode_terminal_output, send_one_stream,
    unwrap_cbor_value, ActivityTimer, CollectedOutput, CreditGrantFn, CreditPlumbing,
    FlowObserver, IncrementalWriter, OutputSink, PipelineLogFn,
    PipelineLogRecord, PipelineProgressTracker, SegmentWriterFactory, StreamIoError, TerminalItem,
    TerminalMeta, TerminalOutput, PIPELINE_STALL_TIMEOUT_SECS,
};
