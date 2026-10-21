//! Running planned machine runs from the command line.
//!
//! What the CLI does with a machine once it is planned is the same whether it
//! prints the run as text or draws it in a terminal: each run's strands are
//! started side by side on their own scopes of one [`CliRuntime`]
//! ([`spawn_strand`]), and once a strand returns, what it produced is written
//! by the pipe discipline and what did not simply complete is said
//! ([`finish_strand`]). The two front ends differ only in how they show the
//! run meanwhile and where they say what they say.

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use crate::orchestrator::cli_output::{emit_terminals_by_output, EmitOptions};
use crate::orchestrator::cli_runtime::{CliRuntime, TransientSink};
use crate::orchestrator::execute_plan::{
    execute_plan, EngineRuntime, PipelineResult, PlanInput, RunVerdict, StepFailure, StepOutcome,
};
use crate::orchestrator::machine_plan::StrandPlan;
use crate::orchestrator::observe::{RunKey, RunObserver};
use crate::orchestrator::run_arguments::RunArgumentLedger;
use crate::planner::ExecutionNodeType;
use crate::{AttributionClass, ExecutionError};

/// How a run ended, least to most severe: a process's exit status is the
/// worst over everything it ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RunEnd {
    /// Every output was produced, or was reported not produced by a branch
    /// that did not fire.
    Completed,
    /// Outputs were written, and at least one step failed under a policy
    /// that tolerates it.
    Tolerated,
    /// A step failed and nothing tolerated it.
    Failed,
    /// The user cancelled.
    Cancelled,
}

impl RunEnd {
    /// The CLI's exit status for it (18.1 §Exit status).
    pub fn exit_code(self) -> i32 {
        match self {
            RunEnd::Completed => 0,
            RunEnd::Tolerated => 3,
            RunEnd::Failed => 1,
            RunEnd::Cancelled => 130,
        }
    }
}

/// One strand of one run, planned: its plan, what each input slot is given
/// and whether it is a sequence, and the values of the arguments the machine
/// leaves unbound.
#[derive(Debug, Clone)]
pub struct PlannedStrand {
    pub strand: StrandPlan,
    pub inputs: HashMap<String, PlanInput>,
    pub is_sequence: HashMap<String, bool>,
    pub cap_arguments: HashMap<String, Vec<(String, Vec<u8>)>>,
}

/// One strand of one run, returned: its plan and how it ended.
pub type StrandRun = (StrandPlan, Result<PipelineResult, ExecutionError>);

/// A started strand: the ledger its argument values are read from and
/// changed through, and the task running it.
pub struct StrandHandle {
    pub key: RunKey,
    pub ledger: Arc<RunArgumentLedger>,
    pub task: tokio::task::JoinHandle<StrandRun>,
}

/// Start one strand of one run on its own scope of `runtime`, every signal it
/// gives reported to `observer` under `key`. A run whose observer was lost
/// while it spoke failed: what it said is not known.
pub fn spawn_strand(
    runtime: &Arc<CliRuntime>,
    observer: &RunObserver,
    key: RunKey,
    planned: PlannedStrand,
    on_transient: Option<TransientSink>,
) -> Result<StrandHandle, (StrandPlan, ExecutionError)> {
    let ledger = match RunArgumentLedger::new(&planned.strand.plan, planned.cap_arguments) {
        Ok(ledger) => Arc::new(ledger),
        Err(e) => {
            return Err((
                planned.strand,
                ExecutionError::HostError(format!("argument values refused: {e}")),
            ))
        }
    };
    let scope: Arc<dyn EngineRuntime> = Arc::new(runtime.scope(key, on_transient));
    let observer = observer.clone();
    let run_ledger = ledger.clone();
    let strand = planned.strand;
    let inputs = planned.inputs;
    let is_sequence = planned.is_sequence;
    let task = tokio::spawn(async move {
        let result = execute_plan(
            &strand.plan,
            scope,
            inputs,
            is_sequence,
            run_ledger,
            observer.callbacks(key),
        )
        .await;
        if observer.lost() {
            return (
                strand,
                Err(ExecutionError::HostError(format!(
                    "what run {} strand {} said was lost: its observer went away while it ran",
                    key.run, key.strand
                ))),
            );
        }
        (strand, result)
    });
    Ok(StrandHandle { key, ledger, task })
}

/// Where a strand's outputs are written.
#[derive(Debug, Clone)]
pub struct EmitTarget {
    /// `--output`, or `None` for the working directory — and for the stdout
    /// fast path, which only a run that can produce one scalar result takes.
    pub output_dir: Option<PathBuf>,
    pub force: bool,
    /// What the produced files are named for: the input's stem.
    pub stem: String,
}

/// What became of one output of a finished strand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmittedOutput {
    /// Written as these files, under their final names.
    Files(Vec<PathBuf>),
    /// Streamed raw to stdout: the run's one scalar result.
    Stdout,
    /// The branch that would have produced it did not fire.
    NotProduced,
}

/// A finished strand, reported: how it ended, the lines the CLI says on
/// stderr about it (in order), and what became of each output.
#[derive(Debug, Clone)]
pub struct StrandReport {
    pub end: RunEnd,
    pub notes: Vec<String>,
    /// By plan `Output` node id. Empty when the strand failed.
    pub outputs: BTreeMap<String, EmittedOutput>,
}

/// Write what a finished strand produced, by the pipe discipline, to `stdout`
/// and the output directory, and say what did not simply complete: the
/// failure that ended it, outputs that were not produced, steps skipped,
/// attempted again or tolerated. Whatever the run persisted and did not turn
/// into an output — everything, when it failed — is removed: nothing of a run
/// is left behind but its outputs.
pub fn finish_strand(
    strand: &StrandPlan,
    result: Result<PipelineResult, ExecutionError>,
    target: &EmitTarget,
    stdout: &mut dyn Write,
    runtime: &CliRuntime,
) -> StrandReport {
    let report = emit_strand(strand, result, target, stdout);
    runtime.discard_parts(&strand.plan);
    report
}

fn emit_strand(
    strand: &StrandPlan,
    result: Result<PipelineResult, ExecutionError>,
    target: &EmitTarget,
    stdout: &mut dyn Write,
) -> StrandReport {
    let mut notes: Vec<String> = Vec::new();
    let result = match result {
        Ok(result) => result,
        Err(e) => {
            notes.push(e.to_string());
            let end = if e.attribution_class() == AttributionClass::User {
                RunEnd::Cancelled
            } else {
                RunEnd::Failed
            };
            return StrandReport {
                end,
                notes,
                outputs: BTreeMap::new(),
            };
        }
    };
    let options = EmitOptions {
        output_dir: target.output_dir.clone(),
        force: target.force,
        input_stem: target.stem.clone(),
        output_names: strand.output_names(),
    };
    let written = match emit_terminals_by_output(&result, &options, stdout) {
        Ok(written) => written,
        Err(e) => {
            notes.push(e);
            return StrandReport {
                end: RunEnd::Failed,
                notes,
                outputs: BTreeMap::new(),
            };
        }
    };
    let mut outputs: BTreeMap<String, EmittedOutput> = BTreeMap::new();
    for (node, path) in written {
        match outputs.entry(node).or_insert_with(|| EmittedOutput::Files(Vec::new())) {
            EmittedOutput::Files(files) => files.push(path),
            _ => unreachable!("only files are recorded from what was written"),
        }
    }
    let output_names = strand.output_names();
    for terminal in &result.terminals {
        match terminal.produced() {
            Some(_) => {
                // Produced and not written as a file: the stdout fast path.
                outputs
                    .entry(terminal.output_node_id().to_string())
                    .or_insert(EmittedOutput::Stdout);
            }
            None => {
                // Every output of a plan is one the notation named.
                let Some(name) = output_names.get(terminal.output_node_id()) else {
                    notes.push(format!(
                        "internal error: the run reports output '{}', which its plan does not name",
                        terminal.output_node_id()
                    ));
                    return StrandReport {
                        end: RunEnd::Failed,
                        notes,
                        outputs,
                    };
                };
                notes.push(format!("Not produced: output '{name}' ({})", terminal.media_urn()));
                outputs.insert(terminal.output_node_id().to_string(), EmittedOutput::NotProduced);
            }
        }
    }
    let describe = |failure: &StepFailure| {
        format!(
            "[{}{}] {}",
            failure.class,
            failure
                .code
                .as_deref()
                .map(|code| format!("/{code}"))
                .unwrap_or_default(),
            failure.message
        )
    };
    // A value is named as the machine named it. An item of a map is named
    // by the sequence it is an item of.
    let name_of = |node: &String| -> String {
        if let Some(name) = strand.node_names.get(node) {
            return format!("'{name}'");
        }
        match strand.plan.nodes.get(node).map(|n| &n.node_type) {
            Some(ExecutionNodeType::Item { source, .. }) => match strand.node_names.get(source) {
                Some(name) => format!("an item of '{name}'"),
                None => format!("an item of {source}"),
            },
            _ => node.clone(),
        }
    };
    for report in &result.steps {
        let run = report
            .body
            .as_ref()
            .map(|body| format!(" (item {})", body.body_index))
            .unwrap_or_default();
        match &report.outcome {
            StepOutcome::Skipped { absent } => notes.push(format!(
                "Skipped: step {}{run} — it needs {}, which was not produced",
                report.cap_urn,
                absent.iter().map(name_of).collect::<Vec<_>>().join(", ")
            )),
            StepOutcome::Completed { failed_attempts } => {
                for (attempt, failure) in failed_attempts.iter().enumerate() {
                    notes.push(format!(
                        "Attempted again: step {}{run}, attempt {} failed {}",
                        report.cap_urn,
                        attempt + 1,
                        describe(failure)
                    ));
                }
            }
            StepOutcome::FailedTolerated {
                failure,
                failed_attempts,
            } => notes.push(format!(
                "Tolerated failure: step {}{run} {}{}",
                report.cap_urn,
                describe(failure),
                if failed_attempts.is_empty() {
                    String::new()
                } else {
                    format!(" (after {} earlier attempt(s))", failed_attempts.len())
                }
            )),
        }
    }
    let end = match result.verdict {
        RunVerdict::Completed => RunEnd::Completed,
        RunVerdict::CompletedWithToleratedFailures => RunEnd::Tolerated,
    };
    StrandReport { end, notes, outputs }
}
