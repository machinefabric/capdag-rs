//! What is known of the runs a command started, folded from their signals.
//!
//! Every way a run is shown — the live view, the plain lines, the JSON events —
//! reads the same [`Watch`]. A run is a list of rows in the order its steps
//! run: each step of the machine, and each map with the steps of its body
//! (a map runs its body once per item; the executor reports a map's progress
//! under the map, and its items as they are dispatched and as they end).
//!
//! What a row shows of its step — waiting, running, done, skipped, failed —
//! is the model's decision (`CapDAG.Run.Step.on`, `capdag/formal`). A signal
//! the model refuses for the step's state is a run other than the one being
//! watched, and is an error, never drawn.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use crate::fabric::registry::FabricRegistry;
use crate::formal::run::step as model;
use crate::orchestrator::observe::{RunEvent, RunKey, RunSignal};
use crate::orchestrator::run_control::RunOutcome;
use crate::orchestrator::machine_plan::StrandPlan;
use crate::orchestrator::StepOutcome;
use crate::planner::{ExecutionNodeType, MachinePlan};

pub use model::StepState;

/// One row of a run: a step, or a map with its body.
#[derive(Debug, Clone)]
pub struct Row {
    /// The step's or the map's token: what the executor reports it under.
    pub token: String,
    /// What a person reads: the cap's alias; for a map, its body's caps.
    pub name: String,
    pub map: Option<MapItems>,
    pub state: StepState,
    /// 0..=1, never going back.
    pub progress: f32,
    /// The latest thing its cartridge said, at the level shown.
    pub last: Option<String>,
    pub changed: Instant,
}

/// A map's items: how many were started, ended, failed, and of how many.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MapItems {
    pub started: usize,
    pub done: usize,
    pub failed: usize,
    /// Known once the source has ended, or from the start for a bounded one.
    pub total: Option<usize>,
    /// The item being run, as the executor previews it.
    pub current: Option<String>,
    /// What the items are called: the machine's name for the sequence.
    pub of: String,
}

/// One line a cartridge said.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogLine {
    /// The row it is of, by name; `None` for the machine.
    pub step: Option<String>,
    pub level: String,
    pub message: String,
}

/// One run of a machine, watched.
#[derive(Debug, Clone)]
pub struct RunWatch {
    pub key: RunKey,
    /// What the run is of: its input file, or the machine.
    pub label: String,
    pub rows: Vec<Row>,
    pub logs: Vec<LogLine>,
    pub started: Instant,
    pub ended: Option<(RunOutcome, Duration)>,
}

/// What a watcher could not fold.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WatchError {
    #[error("a signal for run {run} strand {strand}, which this command did not start")]
    UnknownRun { run: usize, strand: usize },
    #[error("the run reports step {token}, which its plan does not have")]
    UnknownStep { token: String },
    #[error("the run reports {report:?} for step '{name}', which is {state:?}: a step in that state cannot have it")]
    Refused { name: String, state: StepState, report: model::StepReport },
}

/// Every run a command started.
#[derive(Debug, Default)]
pub struct Watch {
    pub runs: BTreeMap<RunKey, RunWatch>,
    /// Lowest log level shown: 0 warnings and errors, 1 info, 2 debug.
    verbosity: i8,
}

impl Watch {
    pub fn new(verbosity: i8) -> Self {
        Self {
            runs: BTreeMap::new(),
            verbosity,
        }
    }

    /// Start watching a run: its rows are its plan's steps and maps, in the
    /// order they run, named as a person names them.
    pub fn add_run(&mut self, key: RunKey, label: String, strand: &StrandPlan, registry: &FabricRegistry) {
        let now = Instant::now();
        let rows = rows_of(&strand.plan, &strand.node_names, registry, now);
        self.runs.insert(
            key,
            RunWatch {
                key,
                label,
                rows,
                logs: Vec::new(),
                started: now,
                ended: None,
            },
        );
    }

    /// Fold one signal in. Returns the index of the row it changed, if any.
    pub fn apply(&mut self, event: &RunEvent) -> Result<Option<usize>, WatchError> {
        let verbosity = self.verbosity;
        let run = self.runs.get_mut(&event.key).ok_or(WatchError::UnknownRun {
            run: event.key.run,
            strand: event.key.strand,
        })?;
        let now = Instant::now();
        match &event.signal {
            RunSignal::Progress { .. } => Ok(None),
            RunSignal::StepProgress { token_id, progress, .. } => {
                let index = run.row(token_id)?;
                let row = &mut run.rows[index];
                let complete = *progress >= 1.0;
                row.state = report(row, model::StepReport::Progress { complete })?;
                row.progress = row.progress.max(progress.clamp(0.0, 1.0));
                row.changed = now;
                Ok(Some(index))
            }
            RunSignal::StepReport(step) => {
                let Ok(index) = run.row(step.step_token_id.as_str()) else {
                    // A step inside a map's body: its report is its map's
                    // item's, and the body outcome says how the item ended.
                    return Ok(None);
                };
                let row = &mut run.rows[index];
                let report_of = match &step.outcome {
                    StepOutcome::Skipped { .. } => Some(model::StepReport::Skipped),
                    StepOutcome::FailedTolerated { .. } => Some(model::StepReport::Tolerated),
                    StepOutcome::Completed { .. } => None,
                };
                if let Some(report_of) = report_of {
                    row.state = report(row, report_of)?;
                    row.changed = now;
                }
                Ok(Some(index))
            }
            RunSignal::ForEachItems { map_token, items } => {
                let Ok(index) = run.row(map_token) else {
                    // A map inside another map's body: shown as its outer map's items.
                    return Ok(None);
                };
                let row = &mut run.rows[index];
                if let Some(map) = &mut row.map {
                    map.started += items.iter().filter(|item| item.enclosing.is_empty()).count();
                    if let Some(last) = items.last() {
                        map.current = last.item_preview_text.clone();
                    }
                }
                if row.state == StepState::Waiting {
                    row.state = report(row, model::StepReport::Progress { complete: false })?;
                }
                row.changed = now;
                Ok(Some(index))
            }
            RunSignal::BodyOutcomes(outcomes) => {
                let mut changed = None;
                for (index, row) in run.rows.iter_mut().enumerate() {
                    let Some(map) = &mut row.map else { continue };
                    let mine = outcomes.iter().filter(|o| {
                        o.enclosing.is_empty()
                            && o.foreach_token_id.as_ref().map(|t| t.as_str()) == Some(row.token.as_str())
                    });
                    let (done, failed) = mine.fold((0, 0), |(d, f), o| if o.success { (d + 1, f) } else { (d, f + 1) });
                    if (done, failed) != (map.done, map.failed) {
                        map.done = done;
                        map.failed = failed;
                        row.changed = now;
                        changed = Some(index);
                    }
                }
                Ok(changed)
            }
            RunSignal::Log(record) => {
                let level_rank = match record.level.as_str() {
                    "error" | "warn" | "warning" => 0,
                    "info" => 1,
                    _ => 2,
                };
                if level_rank > verbosity {
                    return Ok(None);
                }
                let index = record
                    .step_token_id
                    .as_ref()
                    .and_then(|token| run.rows.iter().position(|row| row.token == token.as_str()));
                let step = index.map(|i| run.rows[i].name.clone());
                if let Some(i) = index {
                    run.rows[i].last = Some(record.message.clone());
                    run.rows[i].changed = now;
                }
                run.logs.push(LogLine {
                    step,
                    level: record.level.clone(),
                    message: record.message.clone(),
                });
                Ok(index)
            }
        }
    }

    /// The step a run failed at, by its token: shown failed.
    pub fn failed_at(&mut self, key: RunKey, token: &str) -> Result<(), WatchError> {
        let Some(run) = self.runs.get_mut(&key) else {
            return Err(WatchError::UnknownRun { run: key.run, strand: key.strand });
        };
        let Ok(index) = run.row(token) else {
            // A step inside a map's body: its map failed.
            return Ok(());
        };
        let row = &mut run.rows[index];
        row.state = report(row, model::StepReport::Failed)?;
        Ok(())
    }

    /// A run ended: no row is left waiting or running.
    pub fn end_run(&mut self, key: RunKey, outcome: RunOutcome) -> Result<(), WatchError> {
        let Some(run) = self.runs.get_mut(&key) else {
            return Err(WatchError::UnknownRun { run: key.run, strand: key.strand });
        };
        let completed = matches!(outcome, RunOutcome::Completed | RunOutcome::Tolerated);
        let aborted = matches!(outcome, RunOutcome::Aborted | RunOutcome::Failed);
        for row in &mut run.rows {
            row.state = report(row, model::StepReport::RunEnded { completed, aborted })?;
            if let Some(map) = &mut row.map {
                if completed {
                    map.total = Some(map.started);
                }
            }
        }
        run.ended = Some((outcome, run.started.elapsed()));
        Ok(())
    }
}

impl RunWatch {
    fn row(&self, token: &str) -> Result<usize, WatchError> {
        self.rows
            .iter()
            .position(|row| row.token == token)
            .ok_or_else(|| WatchError::UnknownStep { token: token.to_string() })
    }
}

/// The model's decision about one report for a row.
fn report(row: &Row, report: model::StepReport) -> Result<StepState, WatchError> {
    model::on(row.state.clone(), report.clone()).ok_or_else(|| WatchError::Refused {
        name: row.name.clone(),
        state: row.state.clone(),
        report,
    })
}

/// The rows of a plan: its steps outside every map, and its maps outside every
/// other map, in the order they run.
fn rows_of(
    plan: &MachinePlan,
    node_names: &HashMap<String, String>,
    registry: &FabricRegistry,
    now: Instant,
) -> Vec<Row> {
    let in_a_body: std::collections::HashSet<&str> = plan
        .nodes
        .values()
        .filter_map(|node| match &node.node_type {
            ExecutionNodeType::ForEach { body, .. } => Some(body.iter().map(String::as_str)),
            _ => None,
        })
        .flatten()
        .collect();
    let order: Vec<&str> = match plan.topological_order() {
        Ok(nodes) => nodes.iter().map(|node| node.id.as_str()).collect(),
        Err(_) => {
            let mut ids: Vec<&str> = plan.nodes.keys().map(String::as_str).collect();
            ids.sort();
            ids
        }
    };
    let names: HashMap<&str, String> = plan
        .nodes
        .iter()
        .filter_map(|(id, node)| match &node.node_type {
            ExecutionNodeType::Cap { cap_urn, .. } => {
                Some((id.as_str(), super::names::cap_urn(registry, cap_urn)))
            }
            _ => None,
        })
        .collect();
    let mut rows = Vec::new();
    for id in order {
        if in_a_body.contains(id) {
            continue;
        }
        let node = &plan.nodes[id];
        match &node.node_type {
            ExecutionNodeType::Cap { token_id, .. } => rows.push(Row {
                token: token_id.as_str().to_string(),
                name: names[id].clone(),
                map: None,
                state: StepState::Waiting,
                progress: 0.0,
                last: None,
                changed: now,
            }),
            ExecutionNodeType::ForEach { token_id, sources, body } => {
                let body_ids: std::collections::HashSet<&str> = body.iter().map(String::as_str).collect();
                let steps: Vec<String> = plan
                    .topological_order()
                    .map(|nodes| nodes.iter().map(|n| n.id.clone()).collect::<Vec<_>>())
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|n| body_ids.contains(n.as_str()))
                    .filter_map(|n| names.get(n.as_str()).cloned())
                    .collect();
                let of = sources
                    .iter()
                    .map(|source| node_names.get(&source.node).cloned().unwrap_or_else(|| source.node.clone()))
                    .collect::<Vec<_>>()
                    .join(", ");
                rows.push(Row {
                    token: token_id.as_str().to_string(),
                    name: steps.join(" → "),
                    map: Some(MapItems {
                        of,
                        ..MapItems::default()
                    }),
                    state: StepState::Waiting,
                    progress: 0.0,
                    last: None,
                    changed: now,
                });
            }
            _ => {}
        }
    }
    rows
}
