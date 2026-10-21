//! What is known of one run of one strand, folded from what it said.
//!
//! [`RunLive`] is a pure reducer: [`RunLive::apply`] takes each
//! [`RunSignal`] the run gives, in order, and the sampler's protocol readings
//! ([`RunLive::apply_flow`], [`RunLive::apply_pools`]) as they are taken. It
//! keeps exactly what the desktop's run view is given — per-step progress
//! under each step's token, the items each map runs on and how each run of it
//! ended, step reports, diagnostics, captured intermediates, the run's end —
//! and refuses anything that does not fit the plan it was made for: a token no
//! step or map of the plan has, an item that does not continue its map's
//! items, an outcome for an item no map ran on. Such a signal is a defect
//! somewhere upstream, and drawing around it would hide it.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use crate::bifaci::pools::PoolState;
use crate::orchestrator::observe::RunSignal;
use crate::orchestrator::{
    ForEachItemSnapshot, PlanView, PlanViewKind, RunFlowState, RunVerdict, StepFailure,
    StepOutcome, StepReport, TransientArtifact,
};
use crate::planner::{BodyOutcome, MapRun};
use crate::AttributionClass;

/// A signal or a reading that does not fit the run's plan.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LiveError {
    #[error("the run speaks of step '{0}', which its plan has no step or map for")]
    UnknownToken(String),
    #[error("the run reports a map item under '{0}', which is no map of its plan")]
    UnknownMap(String),
    #[error("map '{map}' published item {got} where its next item is {expected}")]
    ItemsNotContiguous {
        map: String,
        expected: usize,
        got: usize,
    },
    #[error("map '{map}' published an item of map '{item_map}'")]
    ItemOfAnotherMap { map: String, item_map: String },
    #[error("the run reports an outcome for '{0}', which is no item a map's body ran on")]
    OutcomeForNoItem(String),
    #[error("the run reports two outcomes for '{0}'")]
    DuplicateOutcome(String),
    #[error("the run reports a whole-run outcome {0}")]
    MalformedWholeRunOutcome(&'static str),
    #[error("the run reports that step '{0}' left a queue it never entered")]
    DequeuedWithoutQueue(String),
    #[error("intermediate value captured at '{0}', which is no value a step of the plan makes")]
    TransientAtNoStep(String),
    #[error("intermediate value at '{0}' captured twice")]
    DuplicateTransient(String),
    #[error("the run speaks after it ended")]
    AfterEnd,
    #[error("the run ended twice")]
    EndedTwice,
    #[error("output '{0}' is no output of the run's plan")]
    UnknownOutput(String),
}

/// Where a run stands.
#[derive(Debug, Clone, PartialEq)]
pub enum RunStatus {
    /// Started, nothing said yet.
    Pending,
    Running,
    Completed(RunVerdict),
    Failed(RunFailure),
    /// Cancelled by the user (or ended as the collateral of a cancel).
    Cancelled(RunFailure),
}

impl RunStatus {
    pub fn is_live(&self) -> bool {
        matches!(self, RunStatus::Pending | RunStatus::Running)
    }
}

/// How a failed or cancelled run's failure is attributed.
#[derive(Debug, Clone, PartialEq)]
pub struct RunFailure {
    pub class: AttributionClass,
    pub code: Option<String>,
    pub reason: String,
    /// The step the failure is of, when it is a step's.
    pub step_token: Option<String>,
    pub cap_urn: Option<String>,
    /// The argument the failure is attributed to, when its source named one.
    pub arg_urn: Option<String>,
    /// Not a failure of its own: the end of something stopped because
    /// another step failed.
    pub collateral: bool,
    /// The whole error, as it reads.
    pub message: String,
}

impl RunFailure {
    /// The failure an execution error declares.
    pub fn of(error: &crate::ExecutionError) -> Self {
        Self {
            class: error.attribution_class(),
            code: error.failure_code().map(str::to_string),
            reason: error.failure_reason(),
            step_token: error.step_token_id().map(|token| token.to_string()),
            cap_urn: error.failure_cap_urn().map(str::to_string),
            arg_urn: error.failure_arg_urn().map(str::to_string),
            collateral: error.is_collateral(),
            message: error.to_string(),
        }
    }
}

/// Where one step stands, from its own progress and its queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepPhase {
    /// Nothing heard of it yet.
    Pending,
    /// Running.
    Active,
    /// Its own progress reached its end.
    Done,
}

/// What is known of one step.
#[derive(Debug, Clone, PartialEq)]
pub struct StepLive {
    pub phase: StepPhase,
    /// Its own completion, `0.0..=1.0`, when it reported one.
    pub progress: Option<f32>,
    /// How many of its requests wait for pool capacity right now.
    pub queued: usize,
    /// The last queue message it gave ("position 2 on pool 'gpu'").
    pub queue_message: Option<String>,
    /// What became of it when it did not simply run once and complete.
    pub reports: Vec<StepReport>,
}

impl StepLive {
    fn new() -> Self {
        Self {
            phase: StepPhase::Pending,
            progress: None,
            queued: 0,
            queue_message: None,
            reports: Vec::new(),
        }
    }

    pub fn skipped(&self) -> usize {
        self.reports
            .iter()
            .filter(|r| matches!(r.outcome, StepOutcome::Skipped { .. }))
            .count()
    }

    pub fn retried(&self) -> usize {
        self.reports
            .iter()
            .filter(|r| matches!(r.outcome, StepOutcome::Completed { .. }))
            .count()
    }

    pub fn tolerated(&self) -> usize {
        self.reports
            .iter()
            .filter(|r| matches!(r.outcome, StepOutcome::FailedTolerated { .. }))
            .count()
    }

    /// The failures of its tolerated runs, in the order they happened.
    pub fn tolerated_failures(&self) -> Vec<&StepFailure> {
        self.reports
            .iter()
            .filter_map(|r| match &r.outcome {
                StepOutcome::FailedTolerated { failure, .. } => Some(failure),
                _ => None,
            })
            .collect()
    }
}

/// The address of one run of a map's body: every run it is inside, outermost
/// first, then its own.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ItemKey(pub Vec<(String, usize)>);

impl ItemKey {
    fn of(map_token: &str, body_index: usize, enclosing: &[MapRun]) -> Self {
        let mut path: Vec<(String, usize)> = enclosing
            .iter()
            .map(|run| (run.foreach_token_id.to_string(), run.body_index))
            .collect();
        path.push((map_token.to_string(), body_index));
        Self(path)
    }

    /// The run's name as the uikit gives it: `Item 3`, `Item 1.2` for the
    /// second run of a map opened inside the first run of another.
    pub fn label(&self) -> String {
        let indexes: Vec<String> = self.0.iter().map(|(_, index)| (index + 1).to_string()).collect();
        format!("Item {}", indexes.join("."))
    }

    /// The map this is a run of.
    pub fn map_token(&self) -> &str {
        &self.0.last().expect("an item key names at least its own run").0
    }

    pub fn body_index(&self) -> usize {
        self.0.last().expect("an item key names at least its own run").1
    }

    /// The text key the uikit addresses the same run by.
    pub fn as_text(&self) -> String {
        self.0
            .iter()
            .map(|(token, index)| format!("{token}:{index}"))
            .collect::<Vec<_>>()
            .join("/")
    }
}

/// One run of a map's body.
#[derive(Debug, Clone)]
pub struct ItemLive {
    pub key: ItemKey,
    pub preview_text: Option<String>,
    pub byte_count: u64,
    /// How the run ended, once it has.
    pub outcome: Option<BodyOutcome>,
}

/// Everything known of one map.
#[derive(Debug, Clone)]
pub struct MapLive {
    /// Its aggregate progress over its runs.
    pub progress: Option<f32>,
    /// Its runs, in the order they were published (item order within each
    /// enclosing run).
    pub items: Vec<ItemLive>,
}

/// One diagnostic the run gave.
#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    /// Since the run started.
    pub at: Duration,
    pub level: String,
    pub class: AttributionClass,
    pub step_token: Option<String>,
    pub cap_urn: Option<String>,
    pub body_index: Option<usize>,
    pub arg_urn: Option<String>,
    pub message: String,
}

/// One output of the run, as it ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputEnd {
    /// Written: the files that hold it, under their final names.
    Produced { paths: Vec<std::path::PathBuf>, bytes: u64 },
    /// The run's one scalar result, streamed to standard output — held until
    /// the terminal is given back, then written there.
    Stdout { bytes: u64 },
    /// The branch that would have produced it did not fire.
    NotProduced,
}

/// How the protocol's flow on a step's edge reads, with the rate the uikit
/// smooths from successive samples.
#[derive(Debug, Clone, PartialEq)]
pub struct FlowReading {
    pub state: RunFlowState,
    /// Smoothed bytes per second (EWMA 0.4 new / 0.6 old).
    pub bytes_per_second: f64,
    total_bytes: u64,
    sampled_at: Instant,
}

impl FlowReading {
    /// Both directions of every stream of every live invocation.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

/// The pools a step's requests are admitted through: the cartridge serving
/// it, and each pool of the chain with its state, tightest last.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolChain {
    pub cartridge_id: String,
    pub pools: Vec<(String, PoolState)>,
}

/// What is known of one run of one strand.
#[derive(Debug, Clone)]
pub struct RunLive {
    /// Token → plan node id, for every step and map of the plan.
    steps_by_token: HashMap<String, String>,
    maps_by_token: HashMap<String, String>,
    /// Plan node ids of the values steps make (the only nodes an
    /// intermediate can be captured at), and of the outputs.
    value_nodes: Vec<String>,
    output_nodes: Vec<String>,
    has_map: bool,

    pub status: RunStatus,
    /// Overall progress; never goes down.
    pub progress: f32,
    /// What the run last said it was doing.
    pub operation: Option<String>,
    pub started: Instant,
    pub ended: Option<Instant>,
    /// When the run last said anything.
    pub last_signal: Instant,
    pub steps: HashMap<String, StepLive>,
    pub maps: HashMap<String, MapLive>,
    /// The whole run's outcome, for a plan with no map.
    pub whole_run: Option<BodyOutcome>,
    pub logs: Vec<LogEntry>,
    pub transients: BTreeMap<String, TransientArtifact>,
    pub outputs: BTreeMap<String, OutputEnd>,
    /// The latest protocol flow reading of each step, by token.
    pub flow: HashMap<String, FlowReading>,
    /// The latest pool chain of each step, by token.
    pub pools: HashMap<String, PoolChain>,
}

impl RunLive {
    /// A run of `plan`, started at `started`.
    pub fn new(plan: &PlanView, started: Instant) -> Self {
        let mut steps_by_token = HashMap::new();
        let mut maps_by_token = HashMap::new();
        let mut value_nodes = Vec::new();
        let mut output_nodes = Vec::new();
        let mut steps = HashMap::new();
        let mut maps = HashMap::new();
        for node in &plan.nodes {
            match &node.kind {
                PlanViewKind::Step(step) => {
                    steps_by_token.insert(step.token_id.to_string(), node.id.clone());
                    steps.insert(step.token_id.to_string(), StepLive::new());
                    value_nodes.push(node.id.clone());
                }
                PlanViewKind::StepOutput { .. } => value_nodes.push(node.id.clone()),
                PlanViewKind::Map { token_id, .. } => {
                    maps_by_token.insert(token_id.to_string(), node.id.clone());
                    maps.insert(
                        token_id.to_string(),
                        MapLive {
                            progress: None,
                            items: Vec::new(),
                        },
                    );
                }
                PlanViewKind::Output { .. } => output_nodes.push(node.id.clone()),
                PlanViewKind::Item { .. }
                | PlanViewKind::Gather { .. }
                | PlanViewKind::Input { .. } => {}
            }
        }
        Self {
            has_map: !maps_by_token.is_empty(),
            steps_by_token,
            maps_by_token,
            value_nodes,
            output_nodes,
            status: RunStatus::Pending,
            progress: 0.0,
            operation: None,
            started,
            ended: None,
            last_signal: started,
            steps,
            maps,
            whole_run: None,
            logs: Vec::new(),
            transients: BTreeMap::new(),
            outputs: BTreeMap::new(),
            flow: HashMap::new(),
            pools: HashMap::new(),
        }
    }

    /// The plan node a step's token is of.
    pub fn step_node(&self, token: &str) -> Option<&str> {
        self.steps_by_token.get(token).map(String::as_str)
    }

    /// The plan node a map's token is of.
    pub fn map_node(&self, token: &str) -> Option<&str> {
        self.maps_by_token.get(token).map(String::as_str)
    }

    fn require_token(&self, token: &str) -> Result<(), LiveError> {
        if self.steps_by_token.contains_key(token) || self.maps_by_token.contains_key(token) {
            Ok(())
        } else {
            Err(LiveError::UnknownToken(token.to_string()))
        }
    }

    /// Fold one signal of the run, received at `now`.
    pub fn apply(&mut self, signal: &RunSignal, now: Instant) -> Result<(), LiveError> {
        if !self.status.is_live() {
            return Err(LiveError::AfterEnd);
        }
        self.status = RunStatus::Running;
        self.last_signal = now;
        match signal {
            RunSignal::Progress {
                progress,
                cap_urn,
                message,
            } => {
                self.progress = self.progress.max(progress.clamp(0.0, 1.0));
                if !message.is_empty() {
                    self.operation = Some(if cap_urn.is_empty() {
                        message.clone()
                    } else {
                        format!("{message} — {cap_urn}")
                    });
                }
            }
            RunSignal::StepProgress {
                token_id,
                progress,
                ..
            } => {
                let progress = progress.clamp(0.0, 1.0);
                if let Some(step) = self.steps.get_mut(token_id) {
                    step.progress = Some(progress);
                    // The engine's reading: a step that has reported is
                    // running until its own progress reaches its end.
                    step.phase = if progress >= 0.999 {
                        StepPhase::Done
                    } else {
                        StepPhase::Active
                    };
                } else if let Some(map) = self.maps.get_mut(token_id) {
                    map.progress = Some(progress);
                } else {
                    return Err(LiveError::UnknownToken(token_id.clone()));
                }
            }
            RunSignal::Log(record) => {
                if let Some(token) = &record.step_token_id {
                    self.require_token(token)?;
                    if let Some(step) = self.steps.get_mut(token.as_str()) {
                        match record.level.as_str() {
                            "queued" => {
                                step.queued += 1;
                                step.queue_message = Some(record.message.clone());
                            }
                            "dequeued" => {
                                if step.queued == 0 {
                                    return Err(LiveError::DequeuedWithoutQueue(token.to_string()));
                                }
                                step.queued -= 1;
                                if step.queued == 0 {
                                    step.queue_message = None;
                                }
                                if step.phase == StepPhase::Pending {
                                    step.phase = StepPhase::Active;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                self.logs.push(LogEntry {
                    at: now.saturating_duration_since(self.started),
                    level: record.level.clone(),
                    class: record.attribution_class,
                    step_token: record.step_token_id.as_ref().map(|t| t.to_string()),
                    cap_urn: record.cap_urn.clone(),
                    body_index: record.body_index,
                    arg_urn: record.arg_urn.clone(),
                    message: record.message.clone(),
                });
            }
            RunSignal::ForEachItems { map_token, items } => {
                self.apply_items(map_token, items)?;
            }
            RunSignal::BodyOutcomes(outcomes) => self.apply_outcomes(outcomes)?,
            RunSignal::StepReport(report) => {
                let token = report.step_token_id.to_string();
                let step = self
                    .steps
                    .get_mut(&token)
                    .ok_or_else(|| LiveError::UnknownToken(token.clone()))?;
                step.reports.push(report.clone());
            }
        }
        Ok(())
    }

    fn apply_items(
        &mut self,
        map_token: &str,
        items: &[ForEachItemSnapshot],
    ) -> Result<(), LiveError> {
        let map = self
            .maps
            .get_mut(map_token)
            .ok_or_else(|| LiveError::UnknownMap(map_token.to_string()))?;
        for item in items {
            if item.foreach_token_id.as_str() != map_token {
                return Err(LiveError::ItemOfAnotherMap {
                    map: map_token.to_string(),
                    item_map: item.foreach_token_id.to_string(),
                });
            }
            // Items are published append-only per run of the map: the next
            // item of a run is the count it already has.
            let expected = map
                .items
                .iter()
                .filter(|existing| {
                    existing.key.0[..existing.key.0.len() - 1]
                        == ItemKey::of(map_token, 0, &item.enclosing).0[..item.enclosing.len()]
                })
                .count();
            if item.body_index != expected {
                return Err(LiveError::ItemsNotContiguous {
                    map: map_token.to_string(),
                    expected,
                    got: item.body_index,
                });
            }
            map.items.push(ItemLive {
                key: ItemKey::of(map_token, item.body_index, &item.enclosing),
                preview_text: item.item_preview_text.clone(),
                byte_count: item.item_byte_count,
                outcome: None,
            });
        }
        Ok(())
    }

    fn apply_outcomes(&mut self, outcomes: &[BodyOutcome]) -> Result<(), LiveError> {
        // The callback delivers the running set: what it holds now replaces
        // what was held.
        for map in self.maps.values_mut() {
            for item in &mut map.items {
                item.outcome = None;
            }
        }
        self.whole_run = None;
        for outcome in outcomes {
            match &outcome.foreach_token_id {
                None => {
                    if self.has_map {
                        return Err(LiveError::MalformedWholeRunOutcome(
                            "for a plan that has maps",
                        ));
                    }
                    if outcome.body_index != 0 || !outcome.enclosing.is_empty() {
                        return Err(LiveError::MalformedWholeRunOutcome(
                            "that is not the run's one body",
                        ));
                    }
                    if self.whole_run.is_some() {
                        return Err(LiveError::DuplicateOutcome("the whole run".to_string()));
                    }
                    self.whole_run = Some(outcome.clone());
                }
                Some(token) => {
                    let key = ItemKey::of(token, outcome.body_index, &outcome.enclosing);
                    let map = self
                        .maps
                        .get_mut(token.as_str())
                        .ok_or_else(|| LiveError::OutcomeForNoItem(key.as_text()))?;
                    let item = map
                        .items
                        .iter_mut()
                        .find(|item| item.key == key)
                        .ok_or_else(|| LiveError::OutcomeForNoItem(key.as_text()))?;
                    if item.outcome.is_some() {
                        return Err(LiveError::DuplicateOutcome(key.as_text()));
                    }
                    item.outcome = Some(outcome.clone());
                }
            }
        }
        Ok(())
    }

    /// An intermediate value the run captured.
    pub fn apply_transient(&mut self, artifact: TransientArtifact) -> Result<(), LiveError> {
        if !self.value_nodes.contains(&artifact.node_id) {
            return Err(LiveError::TransientAtNoStep(artifact.node_id));
        }
        if self.transients.contains_key(&artifact.node_id) {
            return Err(LiveError::DuplicateTransient(artifact.node_id));
        }
        self.transients.insert(artifact.node_id.clone(), artifact);
        Ok(())
    }

    /// The run's protocol flow, read at `now`. Every state is of a step of
    /// the plan. The rate is smoothed from the previous reading of the same
    /// step, as the uikit smooths it: a gap under 50ms keeps the previous
    /// estimate.
    pub fn apply_flow(&mut self, states: Vec<RunFlowState>, now: Instant) -> Result<(), LiveError> {
        for state in states {
            if !self.steps_by_token.contains_key(&state.token_id) {
                return Err(LiveError::UnknownToken(state.token_id));
            }
            let total: u64 = state
                .streams
                .iter()
                .map(|stream| stream.bytes_in + stream.bytes_out)
                .sum();
            let previous = self.flow.get(&state.token_id);
            let (rate, sampled_at, kept_total) = match previous {
                None => (0.0, now, total),
                Some(prev) => {
                    let gap = now.saturating_duration_since(prev.sampled_at);
                    if gap < Duration::from_millis(50) {
                        (prev.bytes_per_second, prev.sampled_at, prev.total_bytes)
                    } else {
                        let instant =
                            total.saturating_sub(prev.total_bytes) as f64 / gap.as_secs_f64();
                        let smoothed = if prev.bytes_per_second == 0.0 {
                            instant
                        } else {
                            0.4 * instant + 0.6 * prev.bytes_per_second
                        };
                        (smoothed, now, total)
                    }
                }
            };
            // A terminated step's live reading carries no streams (the switch
            // released them); its last cumulative is the truth there is.
            let kept_total = if total == 0 {
                previous.map(|prev| prev.total_bytes).unwrap_or(0).max(kept_total)
            } else {
                kept_total
            };
            self.flow.insert(
                state.token_id.clone(),
                FlowReading {
                    state,
                    bytes_per_second: rate,
                    total_bytes: kept_total,
                    sampled_at,
                },
            );
        }
        Ok(())
    }

    /// The pool chains of the run's steps, read now.
    pub fn apply_pools(
        &mut self,
        pools: HashMap<String, PoolChain>,
    ) -> Result<(), LiveError> {
        for token in pools.keys() {
            if !self.steps_by_token.contains_key(token) {
                return Err(LiveError::UnknownToken(token.clone()));
            }
        }
        self.pools = pools;
        Ok(())
    }

    /// The run ended: completed with `verdict` and its outputs, or not.
    pub fn finish(&mut self, end: LiveEnd, now: Instant) -> Result<(), LiveError> {
        if !self.status.is_live() {
            return Err(LiveError::EndedTwice);
        }
        match end {
            LiveEnd::Completed { verdict, outputs } => {
                for node in outputs.keys() {
                    if !self.output_nodes.contains(node) {
                        return Err(LiveError::UnknownOutput(node.clone()));
                    }
                }
                self.outputs = outputs;
                self.progress = 1.0;
                self.status = RunStatus::Completed(verdict);
            }
            LiveEnd::Failed(failure) => {
                if let Some(token) = &failure.step_token {
                    self.require_token(token)?;
                }
                self.status = if failure.class == AttributionClass::User {
                    RunStatus::Cancelled(failure)
                } else {
                    RunStatus::Failed(failure)
                };
            }
        }
        self.ended = Some(now);
        Ok(())
    }

    /// How long the run ran, or has been running at `now`.
    pub fn elapsed(&self, now: Instant) -> Duration {
        self.ended.unwrap_or(now).saturating_duration_since(self.started)
    }

    /// Every step token that failed for good in this run: the run's own
    /// failure, and every run of a map's body that failed at a step. A
    /// collateral end is not a failure of its own, and a cancel is never
    /// painted as one.
    pub fn failed_tokens(&self) -> Vec<String> {
        let mut tokens: Vec<String> = Vec::new();
        if let RunStatus::Failed(failure) = &self.status {
            if !failure.collateral {
                if let Some(token) = &failure.step_token {
                    tokens.push(token.clone());
                }
            }
        }
        for map in self.maps.values() {
            for item in &map.items {
                if let Some(outcome) = &item.outcome {
                    if !outcome.success {
                        if let Some(token) = &outcome.failed_token_id {
                            if !tokens.iter().any(|t| t == token.as_str()) {
                                tokens.push(token.to_string());
                            }
                        }
                    }
                }
            }
        }
        tokens
    }
}

/// How a run ended, as the session reports it to the live state.
#[derive(Debug, Clone)]
pub enum LiveEnd {
    Completed {
        verdict: RunVerdict,
        /// Every output of the plan, by output node id.
        outputs: BTreeMap<String, OutputEnd>,
    },
    Failed(RunFailure),
}
