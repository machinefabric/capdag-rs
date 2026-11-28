//! A run in a world: the signals the engine gives while a strand runs, played
//! in order with their waits, and how the strand ends.
//!
//! A script names the steps and maps it speaks of as the run's view names
//! them — a step by its cap's alias, a map by its body's caps (`words →
//! shout`) — and the outputs by the media type each holds. Before a
//! script is played every name is looked up in the strand's plan: one the plan
//! does not have refuses the run, as the engine cannot report a step its plan
//! does not hold. What is played is the engine's own signals, in the order the
//! script gives them, delivered to the run's observer as the engine delivers
//! them; the strand then ends with the engine's own result or error.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use capdag::orchestrator::machine_plan::StrandPlan;
use capdag::orchestrator::run_control::RunControl;
use capdag::orchestrator::{
    ForEachBodyCoordinate, ForEachItemSnapshot, OutputItem, PlanTerminal, ProducedTerminal, RunCallbacks, RunVerdict,
    StepFailure, StepOutcome, StepReport,
};
use capdag::planner::{BodyOutcome, ExecutionNodeType, MachinePlan, StepToken};
use capdag::{AttributionClass, ExecutionError, FabricRegistry, PipelineLogRecord, PipelineResult};
use serde::Deserialize;

/// How one strand of a run goes.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunScript {
    /// The machine the run is of, in notation, when no machine file of the
    /// world holds it and it is not one cap run on its own: a machine `capdag
    /// plan` makes. What the consistency check plans the script against.
    #[serde(default)]
    pub machine: Option<String>,
    /// What the engine says while it runs, in order.
    pub signals: Vec<Signal>,
    /// How it ends.
    pub end: End,
    /// What happens instead of the rest when the run is asked to stop while
    /// it plays: what started finishes, and nothing new starts.
    #[serde(default)]
    pub on_stop: Option<Branch>,
}

/// The rest of a run, from the moment it is asked to stop.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Branch {
    pub signals: Vec<Signal>,
    pub end: End,
}

/// One signal of a running strand, or a wait between two.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Signal {
    /// Nothing is said for this many milliseconds (at speed 1). An abort ends
    /// the run during a wait; a stop takes the script's `on_stop`.
    Wait(u64),
    /// The run's overall progress, 0 to 1.
    Progress {
        value: f32,
        #[serde(default)]
        message: String,
    },
    /// A step's own progress, 0 to 1 — a map's, under the map's name.
    Step { step: String, progress: f32 },
    /// A log line: of a step, or of the machine.
    Log {
        #[serde(default)]
        step: Option<String>,
        level: String,
        message: String,
    },
    /// The next items a map's body runs on, each by the text it begins with.
    Items { map: String, items: Vec<String> },
    /// The outcomes of a map's runs so far: every one that has ended.
    Outcomes { map: String, ended: Vec<ItemEnd> },
    /// What became of a step that did not simply run once and complete: in
    /// a map's body, of the run on item `item`.
    Report {
        step: String,
        #[serde(default)]
        item: Option<usize>,
        outcome: Outcome,
    },
}

/// How one run of a map's body ended.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ItemEnd {
    pub item: usize,
    pub ok: bool,
    /// Why it failed.
    #[serde(default)]
    pub error: Option<String>,
}

/// A step's report.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Outcome {
    /// It failed, and its policy tolerated the failure: its outputs are absent.
    Tolerated {
        failure: Failure,
        #[serde(default)]
        earlier: Vec<Failure>,
    },
    /// It completed after attempts that failed.
    Retried { earlier: Vec<Failure> },
    /// It did not run: what it needs was not produced.
    Skipped { absent: Vec<String> },
}

/// A failure, as its source declares it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Failure {
    pub class: AttributionClass,
    #[serde(default)]
    pub code: Option<String>,
    pub message: String,
}

impl Failure {
    fn step_failure(&self) -> StepFailure {
        StepFailure { class: self.class, code: self.code.clone(), message: self.message.clone() }
    }
}

/// How a strand ends.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum End {
    /// It completed: what each output holds, by the media type it is, as
    /// `capdag run <machine> --explain` names it (`page-text`, `txt`) — a text is one value, a
    /// list of texts a sequence, `null` not produced. The same caps run as a
    /// machine file, as a cap on its own and as a plan's pick label their
    /// outputs differently; what the outputs hold is the same.
    Completed { outputs: BTreeMap<String, serde_json::Value> },
    /// A step failed, and the run with it.
    Failed { step: String, failure: Failure },
}

/// The steps and maps of a strand by the names its view gives them, and its
/// outputs by the media type each holds.
pub struct Names {
    steps: HashMap<String, (StepToken, String)>,
    maps: HashMap<String, (StepToken, Vec<String>)>,
    outputs: HashMap<String, (String, String)>,
    /// The map each step of a map's body runs in.
    body_of: HashMap<String, StepToken>,
}

impl Names {
    /// The names of `strand`'s steps (by cap alias), maps (by their body's
    /// cap aliases, as the run's view shows them) and outputs (by the media
    /// type each holds).
    pub fn of(strand: &StrandPlan, registry: &FabricRegistry) -> Result<Names, String> {
        let plan = &strand.plan;
        let mut steps: HashMap<String, (StepToken, String)> = HashMap::new();
        let mut twice: Vec<String> = Vec::new();
        for node in plan.nodes.values() {
            if let ExecutionNodeType::Cap { token_id, cap_urn, .. } = &node.node_type {
                let name = crate::cli::names::cap_urn(registry, cap_urn);
                if steps.insert(name.clone(), (token_id.clone(), cap_urn.clone())).is_some() {
                    twice.push(name);
                }
            }
        }
        if !twice.is_empty() {
            return Err(format!("the plan runs {} more than once, so a script cannot name one", twice.join(", ")));
        }
        let mut watch = crate::cli::watch::Watch::new(2);
        let key = capdag::orchestrator::observe::RunKey { run: 0, strand: 0 };
        watch.add_run(key, String::new(), strand, registry);
        let mut maps: HashMap<String, (StepToken, Vec<String>)> = HashMap::new();
        let mut body_of: HashMap<String, StepToken> = HashMap::new();
        for row in watch.runs[&key].rows.iter().filter(|row| row.map.is_some()) {
            let token = StepToken::parse(row.token.clone()).map_err(|e| format!("the map '{}' has no token: {e}", row.name))?;
            let body = row.name.split(" → ").map(str::to_string).collect::<Vec<_>>();
            for step in &body {
                body_of.insert(step.clone(), token.clone());
            }
            let caps = body.iter().filter_map(|step| steps.get(step).map(|(_, urn)| urn.clone())).collect();
            maps.insert(row.name.clone(), (token, caps));
        }
        let mut outputs: HashMap<String, (String, String)> = HashMap::new();
        for (id, label) in strand.output_names() {
            let media = output_media(plan, &id).map_err(|e| format!("output '{label}': {e}"))?;
            let name = crate::cli::names::media_urn(registry, &media);
            if let Some((other, _)) = outputs.insert(name.clone(), (id.clone(), media)) {
                return Err(format!(
                    "the outputs '{other}' and '{id}' both hold {name}, so a script cannot tell them apart"
                ));
            }
        }
        Ok(Names { steps, maps, outputs, body_of })
    }

    fn step(&self, name: &str) -> Result<&(StepToken, String), String> {
        self.steps.get(name).ok_or_else(|| {
            let mut known: Vec<&str> = self.steps.keys().map(String::as_str).collect();
            known.sort();
            format!("the plan has no step '{name}'; its steps are {}", known.join(", "))
        })
    }

    fn map(&self, name: &str) -> Result<&(StepToken, Vec<String>), String> {
        self.maps.get(name).ok_or_else(|| {
            let mut known: Vec<&str> = self.maps.keys().map(String::as_str).collect();
            known.sort();
            if known.is_empty() {
                format!("the plan has no map '{name}'; it has no map")
            } else {
                format!("the plan has no map '{name}'; its maps are {}", known.join(", "))
            }
        })
    }

    /// Every name `script` uses is one the plan has, and every output is
    /// accounted for at each end.
    pub fn check(&self, script: &RunScript) -> Result<(), String> {
        let mut branches = vec![(&script.signals, &script.end)];
        if let Some(stop) = &script.on_stop {
            branches.push((&stop.signals, &stop.end));
        }
        for (signals, end) in branches {
            for signal in signals {
                match signal {
                    Signal::Step { step, .. } => {
                        if self.maps.get(step).is_none() {
                            self.step(step)?;
                        }
                    }
                    Signal::Log { step: Some(step), .. } => {
                        self.step(step)?;
                    }
                    Signal::Items { map, .. } | Signal::Outcomes { map, .. } => {
                        self.map(map)?;
                    }
                    Signal::Report { step, item, .. } => {
                        self.step(step)?;
                        match (item, self.body_of.get(step)) {
                            (Some(_), None) => return Err(format!("'{step}' is in no map, and its report names an item")),
                            (None, Some(_)) => return Err(format!("'{step}' runs in a map, and its report names no item")),
                            _ => {}
                        }
                    }
                    Signal::Wait(_) | Signal::Progress { .. } | Signal::Log { step: None, .. } => {}
                }
            }
            match end {
                End::Completed { outputs } => {
                    for name in self.outputs.keys() {
                        if !outputs.contains_key(name) {
                            return Err(format!("the script's end does not say what output '{name}' holds"));
                        }
                    }
                    for (name, value) in outputs {
                        if !self.outputs.contains_key(name) {
                            return Err(format!("the plan has no output '{name}'"));
                        }
                        match value {
                            serde_json::Value::String(_) | serde_json::Value::Null => {}
                            serde_json::Value::Array(items) if items.iter().all(|item| item.is_string()) => {}
                            other => return Err(format!("output '{name}' is given {other}: a text, a list of texts or null")),
                        }
                    }
                }
                End::Failed { step, .. } => {
                    self.step(step)?;
                }
            }
        }
        Ok(())
    }
}

/// Play `script` on a strand's callbacks at `speed`, and end as it ends.
pub async fn play(
    script: RunScript,
    names: Names,
    callbacks: RunCallbacks,
    control: Arc<RunControl>,
    speed: f64,
) -> Result<PipelineResult, ExecutionError> {
    let mut signals = script.signals;
    let mut end = script.end;
    let mut on_stop = script.on_stop;
    let mut reports: Vec<StepReport> = Vec::new();
    let mut started: HashMap<String, usize> = HashMap::new();
    let mut at = 0;
    while at < signals.len() {
        if !control.admits() {
            if let Some(stop) = on_stop.take() {
                signals = stop.signals;
                end = stop.end;
                at = 0;
                continue;
            }
        }
        let signal = signals[at].clone();
        at += 1;
        match signal {
            Signal::Wait(ms) => {
                let wait = Duration::from_secs_f64(ms as f64 / 1000.0 / speed);
                tokio::select! {
                    () = tokio::time::sleep(wait) => {}
                    cause = control.cancel().raised() => {
                        return Err(ExecutionError::Collateral { class: cause.class, detail: cause.detail });
                    }
                }
            }
            Signal::Progress { value, message } => {
                if let Some(progress) = &callbacks.progress {
                    progress(value, "", &message);
                }
            }
            Signal::Step { step, progress } => {
                let (token, urn) = match names.maps.get(&step) {
                    Some((token, caps)) => (token.clone(), caps.first().cloned().unwrap_or_default()),
                    None => names.step(&step).map(|(token, urn)| (token.clone(), urn.clone())).map_err(script_error)?,
                };
                if let Some(step_progress) = &callbacks.step_progress {
                    step_progress(progress, &urn, token.as_str());
                }
            }
            Signal::Log { step, level, message } => {
                let (token, urn) = match &step {
                    Some(step) => {
                        let (token, urn) = names.step(step).map_err(script_error)?;
                        (Some(token.clone()), Some(urn.clone()))
                    }
                    None => (None, None),
                };
                if let Some(log) = &callbacks.log {
                    log(PipelineLogRecord {
                        step_token_id: token,
                        cap_urn: urn,
                        level,
                        attribution_class: AttributionClass::Internal,
                        message,
                        meta: None,
                        body_index: None,
                        arg_urn: None,
                    });
                }
            }
            Signal::Items { map, items } => {
                let (token, _) = names.map(&map).map_err(script_error)?;
                let first = started.entry(map.clone()).or_insert(0);
                let snapshots: Vec<ForEachItemSnapshot> = items
                    .iter()
                    .enumerate()
                    .map(|(at, text)| ForEachItemSnapshot {
                        foreach_token_id: token.clone(),
                        body_index: *first + at,
                        enclosing: Vec::new(),
                        item_preview_text: Some(text.clone()),
                        item_byte_count: text.len() as u64,
                    })
                    .collect();
                *first += items.len();
                if let Some(foreach_items) = &callbacks.foreach_items {
                    foreach_items(token.as_str(), &snapshots)?;
                }
            }
            Signal::Outcomes { map, ended } => {
                let (token, caps) = names.map(&map).map_err(script_error)?;
                let outcomes: Vec<BodyOutcome> = ended
                    .iter()
                    .map(|end| BodyOutcome {
                        foreach_token_id: Some(token.clone()),
                        body_index: end.item,
                        enclosing: Vec::new(),
                        success: end.ok,
                        cap_urns: caps.clone(),
                        failed_token_id: None,
                        error: end.error.clone(),
                        failed_arg_urn: None,
                        title: None,
                        saved_paths: Vec::new(),
                        total_bytes: 0,
                        duration_ms: 0,
                        item_preview_text: None,
                        item_byte_count: 0,
                    })
                    .collect();
                if let Some(body_outcome) = &callbacks.body_outcome {
                    body_outcome(&outcomes)?;
                }
            }
            Signal::Report { step, item, outcome } => {
                let (token, urn) = names.step(&step).map_err(script_error)?;
                let body = item.map(|index| ForEachBodyCoordinate {
                    foreach_token_id: names.body_of[&step].clone(),
                    body_index: index,
                    enclosing: Vec::new(),
                });
                let outcome = match outcome {
                    Outcome::Tolerated { failure, earlier } => StepOutcome::FailedTolerated {
                        failure: failure.step_failure(),
                        failed_attempts: earlier.iter().map(Failure::step_failure).collect(),
                    },
                    Outcome::Retried { earlier } => {
                        StepOutcome::Completed { failed_attempts: earlier.iter().map(Failure::step_failure).collect() }
                    }
                    Outcome::Skipped { absent } => StepOutcome::Skipped { absent },
                };
                let report = StepReport { step_token_id: token.clone(), cap_urn: urn.clone(), body, outcome };
                if let Some(step_report) = &callbacks.step_report {
                    step_report(&report);
                }
                reports.push(report);
            }
        }
    }
    match end {
        End::Failed { step, failure } => {
            let (token, urn) = names.step(&step).map_err(script_error)?;
            Err(ExecutionError::StepFailed {
                step_token_id: token.clone(),
                source: Box::new(ExecutionError::CartridgeExecutionFailed {
                    cap_urn: urn.clone(),
                    code: failure.code,
                    class: failure.class,
                    details: failure.message,
                    arg_urn: None,
                }),
            })
        }
        End::Completed { outputs } => {
            let tolerated = reports.iter().any(|report| matches!(report.outcome, StepOutcome::FailedTolerated { .. }));
            let mut terminals: Vec<PlanTerminal> = Vec::new();
            let mut ids: Vec<(&String, &(String, String))> = names.outputs.iter().collect();
            ids.sort_by(|a, b| a.1 .0.cmp(&b.1 .0));
            for (name, (id, media)) in ids {
                let value = outputs.get(name).ok_or_else(|| script_error(format!("output '{name}' is not given")))?;
                let (items, is_sequence) = match value {
                    serde_json::Value::Null => {
                        terminals.push(PlanTerminal::NotProduced { output_node_id: id.clone(), media_urn: media.clone() });
                        continue;
                    }
                    serde_json::Value::String(text) => (vec![text.clone()], false),
                    serde_json::Value::Array(items) => {
                        (items.iter().map(|item| item.as_str().unwrap_or_default().to_string()).collect(), true)
                    }
                    other => return Err(script_error(format!("output '{name}' is given {other}"))),
                };
                terminals.push(PlanTerminal::Produced(ProducedTerminal {
                    output_node_id: id.clone(),
                    items: items
                        .into_iter()
                        .enumerate()
                        .map(|(index, text)| OutputItem { data: text.into_bytes(), index })
                        .collect(),
                    is_sequence,
                    media_urn: media.clone(),
                    writer_results: Vec::new(),
                }));
            }
            Ok(PipelineResult {
                terminals,
                body_outcomes: Vec::new(),
                steps: reports,
                verdict: if tolerated { RunVerdict::CompletedWithToleratedFailures } else { RunVerdict::Completed },
            })
        }
    }
}

/// The media an output of `plan` holds: what the node it reads holds.
fn output_media(plan: &MachinePlan, output: &str) -> Result<String, String> {
    let mut at = output.to_string();
    loop {
        let node = plan.nodes.get(&at).ok_or_else(|| format!("the plan has no node '{at}'"))?;
        match &node.node_type {
            ExecutionNodeType::Output { source_node, .. } => at = source_node.clone(),
            ExecutionNodeType::Cap { cap_urn, .. } => {
                let urn = capdag::CapUrn::from_string(cap_urn).map_err(|e| e.to_string())?;
                return Ok(urn.out_spec().to_string());
            }
            ExecutionNodeType::CapOutput { output_media_urn, .. } => return Ok(output_media_urn.clone()),
            ExecutionNodeType::Collect { output_media_urn, .. } => return Ok(output_media_urn.clone()),
            ExecutionNodeType::Item { media_urn, .. } => return Ok(media_urn.clone()),
            ExecutionNodeType::InputSlot { expected_media_urn, .. } => return Ok(expected_media_urn.clone()),
            ExecutionNodeType::ForEach { .. } => return Err(format!("output reads the map node '{at}'")),
        }
    }
}

/// A script that does not fit the plan it is played on: checked before it
/// is played, so reaching this is a defect of the check.
fn script_error(message: impl Into<String>) -> ExecutionError {
    ExecutionError::HostError(format!("the run's script does not fit its plan: {}", message.into()))
}
