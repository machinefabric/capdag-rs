//! The run view: a machine's runs, drawn live, with the desktop's controls.
//!
//! A session runs what the command line planned exactly as the plain CLI
//! does — runs one after another, the strands of a run side by side, each on
//! its own scope of one [`CliRuntime`], outputs written by the pipe
//! discipline the moment a strand ends — and shows it: every signal a run
//! gives folds into its [`RunLive`], the switch's protocol is sampled every
//! 250ms (the desktop's poll cadence) for flow, pools and health, and the
//! picture is the uikit's run scene, laid out in cells.
//!
//! The controls are the desktop's: stop input (the first Ctrl-C, or `s`),
//! abort (the second Ctrl-C), argument edits applied to the running run
//! through its ledger or as a new run with the changes, outputs opened,
//! revealed or copied. Nothing a run writes to standard output or says on
//! standard error reaches the terminal while the view holds it: both are kept
//! and handed back with the session's outcome, to be written once the
//! terminal is the shell's again.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseEvent, MouseEventKind};
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use ratatui::layout::{Constraint, Layout as Split, Rect as Area};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget, Wrap};
use ratatui::Frame;
use tokio::sync::mpsc;

use crate::bifaci::relay_switch::{RelaySwitchError, RelaySwitchProtocolStats};
use crate::orchestrator::{
    finish_strand, join_flow_states, spawn_strand, ArgumentUpdate, ArgumentUpdateDisposition, CliRuntime,
    EmitTarget, EmittedOutput, PlanView, PlanViewKind, PlannedStrand, RunEnd, RunEvent, RunKey, RunObserver,
    RunVerdict, StrandReport, StrandRun, TransientArtifact,
};
use crate::planner::{ArgumentResolution, MachinePlanBuilder};
use crate::tui::app::dock::{self, DockContext, DockState, DockTab, Editing, Selection};
use crate::tui::app::graph::{GraphAction, GraphView};
use crate::tui::app::palette::{Command, Palette, PaletteResult};
use crate::tui::app::{copy_to_clipboard, open_path, too_small, Display, Tui, TuiError, TuiSettings};
use crate::tui::live::{LiveEnd, OutputEnd, PoolChain, RunFailure, RunLive, RunStatus};
use crate::tui::scene::{run_scene, ArgOrigin, RunContext, Scene, Source, StepArgument};
use crate::tui::text;
use crate::tui::theme::{Theme, Tone};
use crate::{AttributionClass, ExecutionError, FabricRegistry, MediaUrn};

/// One strand of a run, as the session is given it.
#[derive(Debug, Clone)]
pub struct SessionStrand {
    pub planned: PlannedStrand,
    pub view: PlanView,
    /// Every argument of every step, as the run binds it, by step token.
    pub arguments: HashMap<String, Vec<StepArgument>>,
    /// What each input was given, by input plan node id.
    pub sources: BTreeMap<String, Vec<Source>>,
}

/// One run: what it is called, what its outputs are named for, its strands.
#[derive(Debug, Clone)]
pub struct SessionRun {
    pub label: String,
    pub stem: String,
    pub strands: Vec<SessionStrand>,
}

/// What a session runs and where it writes.
#[derive(Debug, Clone)]
pub struct RunSessionSpec {
    pub machine: String,
    pub notation: String,
    pub runs: Vec<SessionRun>,
    /// Where outputs go (`--output`, or the working directory; `None` keeps
    /// the stdout fast path for a run that can produce one scalar).
    pub output_dir: Option<PathBuf>,
    pub force: bool,
}

/// What a session did: what it would have printed, and how its runs ended.
#[derive(Debug, Clone)]
pub struct SessionOutcome {
    /// What the runs wrote to standard output, in the order they ended.
    pub stdout: Vec<u8>,
    /// What the CLI says on standard error about each strand, in order,
    /// under the label of the run it is of.
    pub notes: Vec<(String, Vec<String>)>,
    /// How each run ended. A run never started (the session was aborted
    /// first) is not listed.
    pub ends: Vec<RunEnd>,
    /// The user aborted.
    pub aborted: bool,
}

/// Every argument of every step of `strand`, as a run of it binds it: what
/// the machine wires or writes, the cap's defaults, and the values the run
/// gives.
pub async fn step_arguments(
    registry: Arc<FabricRegistry>,
    strand: &PlannedStrand,
    view: &PlanView,
) -> Result<HashMap<String, Vec<StepArgument>>, String> {
    let requirements = MachinePlanBuilder::new(registry)
        .analyze_plan_arguments(&strand.strand.plan)
        .await
        .map_err(|e| format!("the arguments of '{}' cannot be analyzed: {e}", strand.strand.plan.name))?;
    let mut out: HashMap<String, Vec<StepArgument>> = HashMap::new();
    for step in &requirements.steps {
        let token = step.token_id.to_string();
        let constants: Vec<(&str, &str)> = view
            .nodes
            .iter()
            .find_map(|n| match &n.kind {
                PlanViewKind::Step(s) if s.token_id.as_str() == token => Some(
                    s.constants
                        .iter()
                        .map(|c| (c.arg_name.as_str(), c.value_json.as_str()))
                        .collect(),
                ),
                _ => None,
            })
            .ok_or_else(|| format!("step '{}' of the plan is not in its view", step.title))?;
        let given = strand.cap_arguments.get(&token);
        let mut arguments = Vec::with_capacity(step.arguments.len());
        for arg in &step.arguments {
            let arg_urn = MediaUrn::from_string(&arg.media_urn)
                .map_err(|e| format!("argument '{}' of '{}' has an invalid media URN: {e}", arg.name, step.title))?;
            let user_value = given.and_then(|values| {
                values.iter().find_map(|(urn, bytes)| {
                    MediaUrn::from_string(urn)
                        .ok()
                        .filter(|u| u.is_equivalent(&arg_urn).unwrap_or(false))
                        .map(|_| String::from_utf8_lossy(bytes).into_owned())
                })
            });
            let default_text = arg.default_value.as_ref().map(json_text);
            let (origin, value_text) = match (&arg.resolution, user_value) {
                (_, Some(value)) => (ArgOrigin::User, Some(value)),
                (ArgumentResolution::FromInput, None) => (ArgOrigin::Input, None),
                (ArgumentResolution::FromStep, None) => (ArgOrigin::Upstream, None),
                (ArgumentResolution::Constant, None) => {
                    let constant = constants
                        .iter()
                        .find(|(name, _)| *name == arg.name)
                        .map(|(_, value)| {
                            serde_json::from_str::<serde_json::Value>(value)
                                .map(|v| json_text(&v))
                                .unwrap_or_else(|_| value.to_string())
                        })
                        .ok_or_else(|| {
                            format!("argument '{}' of '{}' is bound to a constant the plan does not hold", arg.name, step.title)
                        })?;
                    (ArgOrigin::Constant, Some(constant))
                }
                (ArgumentResolution::HasDefault, None) => (ArgOrigin::Default, default_text.clone()),
                (ArgumentResolution::RequiresUserInput, None) => {
                    if arg.is_required {
                        return Err(format!(
                            "step '{}' has no value for its required arg '{}'",
                            step.title, arg.name
                        ));
                    }
                    // An optional argument nothing gives does not feed the
                    // step, and has no value to show.
                    continue;
                }
            };
            arguments.push(StepArgument {
                name: arg.name.clone(),
                media_urn: arg.media_urn.clone(),
                is_main_input: arg.is_main_input,
                is_required: arg.is_required,
                is_sequence: arg.is_sequence,
                origin,
                value_text,
                default_text,
            });
        }
        out.insert(token, arguments);
    }
    Ok(out)
}

/// A JSON value as text: a string as itself, anything else as JSON.
fn json_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Waiting,
    Running,
    Ended,
}

struct Strand {
    key: RunKey,
    run_label: String,
    stem: String,
    session: SessionStrand,
    live: RunLive,
    ledger: Option<Arc<crate::orchestrator::RunArgumentLedger>>,
    phase: Phase,
    report: Option<StrandReport>,
    stdout: Vec<u8>,
    graph: GraphView,
    /// The request ids of this strand's invocations, for the protocol tab.
    rids: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FocusArea {
    Graph,
    Dock,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Action {
    Quit,
    StopInput,
    Abort,
    ToggleDock,
    Dock(DockTab),
    NextStrand,
    PreviousStrand,
    ZoomIn,
    ZoomOut,
    Fit,
    Centre,
    Wrap,
    Minimap,
    CopyNotation,
    Open,
    Reveal,
    CopyPath,
    ApplyEdits,
    NewRunWithEdits,
    DiscardEdits,
    Help,
}

fn commands() -> Vec<Command<Action>> {
    let c = |name: &str, keys: &str, action: Action| Command {
        name: name.to_string(),
        keys: keys.to_string(),
        action,
    };
    vec![
        c("Stop input — let the run drain", "s", Action::StopInput),
        c("Abort the run", "^C ^C", Action::Abort),
        c("Show the run", "1", Action::Dock(DockTab::Run)),
        c("Inspect the focused step", "2", Action::Dock(DockTab::Step)),
        c("Show a map's items", "3", Action::Dock(DockTab::Items)),
        c("Show the outputs", "4", Action::Dock(DockTab::Outputs)),
        c("Show the messages", "5", Action::Dock(DockTab::Messages)),
        c("Show the protocol", "6", Action::Dock(DockTab::Protocol)),
        c("Toggle the dock", "d", Action::ToggleDock),
        c("Next strand or run", ">", Action::NextStrand),
        c("Previous strand or run", "<", Action::PreviousStrand),
        c("More detail", "+", Action::ZoomIn),
        c("Less detail", "-", Action::ZoomOut),
        c("Fit the graph", "f", Action::Fit),
        c("Centre on what matters", "c", Action::Centre),
        c("Wrap the graph to the width", "w", Action::Wrap),
        c("Toggle the minimap", "m", Action::Minimap),
        c("Copy the machine's notation", "y", Action::CopyNotation),
        c("Open the selected file", "o", Action::Open),
        c("Reveal the selected file", "R", Action::Reveal),
        c("Copy the selected file's path", "Y", Action::CopyPath),
        c("Apply argument edits to this run", "a", Action::ApplyEdits),
        c("New run with the argument edits", "N", Action::NewRunWithEdits),
        c("Discard argument edits", "x", Action::DiscardEdits),
        c("Keys", "?", Action::Help),
        c("Quit", "q", Action::Quit),
    ]
}

/// What a background action answered.
enum Reply {
    Toast(String, Tone),
}

struct App {
    settings: TuiSettings,
    machine: String,
    notation: String,
    output_dir: Option<PathBuf>,
    force: bool,
    /// How many runs the command line planned; later ones are reruns.
    planned_runs: usize,
    runtime: Arc<CliRuntime>,
    observer: RunObserver,
    strands: Vec<Strand>,
    /// Each run's label and its strands, by index into `strands`.
    runs: Vec<(String, Vec<usize>)>,
    running: Option<usize>,
    next_run: usize,
    ends: Vec<Option<RunEnd>>,
    selected: usize,
    dock: DockState,
    focus: FocusArea,
    selection: Selection,
    palette: Option<Palette<Action>>,
    help: bool,
    confirm_quit: bool,
    toasts: VecDeque<(String, Tone, Instant)>,
    stop_requested: bool,
    aborted: bool,
    abort_deadline: Option<Instant>,
    protocol: Option<RelaySwitchProtocolStats>,
    frame: usize,
    quit: bool,
    /// Ctrl-Z was pressed: the loop suspends the display.
    suspend: bool,
    graph_area: Area,
    dock_area: Area,
    strand_tabs: Vec<(u16, u16, usize)>,
    transient_tx: mpsc::UnboundedSender<(RunKey, TransientArtifact)>,
    reply_tx: mpsc::UnboundedSender<Reply>,
    stdout: Vec<u8>,
    notes: Vec<(String, Vec<String>)>,
    fatal: Option<TuiError>,
}

type Joined = (usize, Result<StrandRun, tokio::task::JoinError>);
type Tasks = FuturesUnordered<futures_util::future::BoxFuture<'static, Joined>>;

impl App {
    fn inconsistent(error: impl std::fmt::Display) -> TuiError {
        TuiError::Inconsistent(error.to_string())
    }

    fn toast(&mut self, text: impl Into<String>, tone: Tone) {
        self.toasts.push_back((text.into(), tone, Instant::now()));
        while self.toasts.len() > 3 {
            self.toasts.pop_front();
        }
    }

    /// Start the next run, when none runs and the session was not aborted.
    fn start_next_run(&mut self, tasks: &mut Tasks, handles: &mut HashMap<usize, tokio::task::AbortHandle>) {
        if self.running.is_some() || self.aborted || self.next_run >= self.runs.len() {
            return;
        }
        let run = self.next_run;
        self.next_run += 1;
        self.running = Some(run);
        let now = Instant::now();
        for &index in &self.runs[run].1.clone() {
            let strand = &mut self.strands[index];
            strand.live = RunLive::new(&strand.session.view, now);
            strand.phase = Phase::Running;
            let key = strand.key;
            let tx = self.transient_tx.clone();
            let sink: crate::orchestrator::TransientSink = Arc::new(move |artifact: &TransientArtifact| {
                tx.send((key, artifact.clone())).map_err(|_| {
                    ExecutionError::HostError("the run view is gone while the run captures values".to_string())
                })
            });
            match spawn_strand(&self.runtime, &self.observer, key, strand.session.planned.clone(), Some(sink)) {
                Ok(handle) => {
                    strand.ledger = Some(handle.ledger);
                    handles.insert(index, handle.task.abort_handle());
                    let task = handle.task;
                    tasks.push(Box::pin(async move { (index, task.await) }));
                }
                Err((plan, error)) => {
                    tasks.push(Box::pin(async move { (index, Ok((plan, Err(error)))) }));
                }
            }
        }
        // Show the run that started.
        self.selected = self.runs[run].1[0];
    }

    fn strand_ended(
        &mut self,
        index: usize,
        joined: Result<StrandRun, tokio::task::JoinError>,
        tasks: &mut Tasks,
        handles: &mut HashMap<usize, tokio::task::AbortHandle>,
    ) -> Result<(), TuiError> {
        handles.remove(&index);
        let now = Instant::now();
        let strand = &mut self.strands[index];
        let (plan, result) = match joined {
            Ok(finished) => finished,
            Err(join) => {
                let error = if join.is_cancelled() {
                    // Aborted past the grace an abort gives its requests:
                    // the user's cancel, never painted as a failure.
                    ExecutionError::Collateral {
                        class: AttributionClass::User,
                        detail: "aborted by the user".to_string(),
                    }
                } else {
                    ExecutionError::HostError(format!("the run panicked: {join}"))
                };
                (strand.session.planned.strand.clone(), Err(error))
            }
        };
        let failure = result.as_ref().err().map(RunFailure::of);
        let verdict = result.as_ref().ok().map(|r| r.verdict);
        // A rerun's results go to files: a second raw result on stdout would
        // run into the first.
        let output_dir = if strand.key.run >= self.planned_runs {
            Some(self.output_dir.clone().unwrap_or_else(|| PathBuf::from(".")))
        } else {
            self.output_dir.clone()
        };
        let target = EmitTarget {
            output_dir,
            force: self.force,
            stem: strand.stem.clone(),
        };
        let mut out: Vec<u8> = Vec::new();
        let report = finish_strand(&plan, result, &target, &mut out, &self.runtime);
        let end = match (failure, verdict) {
            (Some(failure), _) => LiveEnd::Failed(failure),
            (None, Some(_)) if report.end == RunEnd::Failed => LiveEnd::Failed(RunFailure {
                class: AttributionClass::Environment,
                code: None,
                reason: report.notes.last().cloned().unwrap_or_default(),
                step_token: None,
                cap_urn: None,
                arg_urn: None,
                collateral: false,
                message: report.notes.join("\n"),
            }),
            (None, Some(verdict)) => LiveEnd::Completed {
                verdict,
                outputs: report
                    .outputs
                    .iter()
                    .map(|(node, emitted)| {
                        let end = match emitted {
                            EmittedOutput::Files(paths) => OutputEnd::Produced {
                                bytes: paths.iter().filter_map(|p| std::fs::metadata(p).ok()).map(|m| m.len()).sum(),
                                paths: paths.clone(),
                            },
                            EmittedOutput::Stdout => OutputEnd::Stdout { bytes: out.len() as u64 },
                            EmittedOutput::NotProduced => OutputEnd::NotProduced,
                        };
                        (node.clone(), end)
                    })
                    .collect(),
            },
            (None, None) => unreachable!("a result is either an error or a verdict"),
        };
        // The run's durable flow summary, now its scope is retired.
        let summary = self.runtime.run_flows().take_flow_summary(&strand.key.flow_scope());
        strand.live.apply_flow(summary, now).map_err(Self::inconsistent)?;
        strand.live.finish(end, now).map_err(Self::inconsistent)?;
        strand.phase = Phase::Ended;
        strand.stdout = out.clone();
        self.stdout.extend_from_slice(&out);
        self.notes.push((strand.run_label.clone(), report.notes.clone()));
        let run_end = report.end;
        strand.report = Some(report);
        self.dock.forget_previews();

        // The run ends when every strand of it has.
        let run = self
            .runs
            .iter()
            .position(|(_, strands)| strands.contains(&index))
            .expect("every strand is of a run");
        let worst = self.ends[run].map_or(run_end, |e| e.max(run_end));
        self.ends[run] = Some(worst);
        if self.runs[run].1.iter().all(|&i| self.strands[i].phase == Phase::Ended) {
            self.running = None;
            self.start_next_run(tasks, handles);
        }
        Ok(())
    }

    fn apply_event(&mut self, event: RunEvent) -> Result<(), TuiError> {
        let strand = self
            .strands
            .iter_mut()
            .find(|s| s.key == event.key)
            .ok_or_else(|| Self::inconsistent(format!("a signal of run {} strand {}, which the session never started", event.key.run, event.key.strand)))?;
        strand.live.apply(&event.signal, Instant::now()).map_err(Self::inconsistent)
    }

    fn apply_transient(&mut self, key: RunKey, artifact: TransientArtifact) -> Result<(), TuiError> {
        let strand = self
            .strands
            .iter_mut()
            .find(|s| s.key == key)
            .ok_or_else(|| Self::inconsistent("a capture of a run the session never started"))?;
        strand.live.apply_transient(artifact).map_err(Self::inconsistent)
    }

    /// Read the switch: every running strand's flow and pools, and the
    /// protocol's health.
    async fn sample(&mut self) -> Result<bool, TuiError> {
        let Some(switch) = self.runtime.switch().await else {
            return Ok(false);
        };
        let now = Instant::now();
        let stats = switch.protocol_stats().await;
        for strand in self.strands.iter_mut().filter(|s| s.phase == Phase::Running) {
            if let Some(map) = self.runtime.run_flows().scope_map(&strand.key.flow_scope()) {
                strand.rids = map.keys().cloned().collect();
                let states = join_flow_states(&map, &stats);
                strand.live.apply_flow(states, now).map_err(Self::inconsistent)?;
            }
            let mut pools: HashMap<String, PoolChain> = HashMap::new();
            let running_steps: Vec<(String, String)> = strand
                .session
                .view
                .nodes
                .iter()
                .filter_map(|n| match &n.kind {
                    PlanViewKind::Step(step) => {
                        let live = &strand.live.steps[step.token_id.as_str()];
                        let busy = live.queued > 0
                            || live.phase == crate::tui::live::StepPhase::Active
                            || strand.live.flow.get(step.token_id.as_str()).is_some_and(|f| f.state.active_requests > 0);
                        busy.then(|| (step.token_id.to_string(), step.cap_urn.clone()))
                    }
                    _ => None,
                })
                .collect();
            for (token, cap) in running_steps {
                match switch.pool_chain_states_for_cap(&cap).await {
                    Ok((cartridge_id, chain)) => {
                        pools.insert(
                            token,
                            PoolChain {
                                cartridge_id,
                                pools: chain,
                            },
                        );
                    }
                    // Not hosted yet: the step has not reached a cartridge.
                    Err(RelaySwitchError::NoHandler(_)) => {}
                    Err(e) => {
                        return Err(Self::inconsistent(format!("the pools of '{cap}' cannot be read: {e}")));
                    }
                }
            }
            strand.live.apply_pools(pools).map_err(Self::inconsistent)?;
        }
        self.protocol = Some(stats);
        Ok(true)
    }

    fn animating(&self) -> bool {
        self.settings.motion
            && self.strands.iter().any(|s| s.phase == Phase::Running)
    }

    fn scene(&self, index: usize) -> Result<Scene, TuiError> {
        let strand = &self.strands[index];
        let context = RunContext {
            live: &strand.live,
            sources: &strand.session.sources,
        };
        let run = (strand.phase != Phase::Waiting).then_some(&context);
        run_scene(&strand.session.view, &strand.session.arguments, run).map_err(Self::inconsistent)
    }

    fn draw(&mut self, frame: &mut Frame) {
        if let Err(e) = self.try_draw(frame) {
            self.fatal = Some(e);
            self.quit = true;
        }
    }

    fn try_draw(&mut self, frame: &mut Frame) -> Result<(), TuiError> {
        let theme = self.settings.theme();
        let area = frame.area();
        frame.buffer_mut().set_style(area, theme.canvas());
        if too_small(frame, &theme) {
            return Ok(());
        }
        let [top, body, footer] =
            Split::vertical([Constraint::Length(1), Constraint::Min(1), Constraint::Length(1)]).areas(area);
        self.draw_top(frame, top, &theme);
        // The dock beside the graph on a wide terminal, under it on a narrow
        // one, over it on a small one.
        let (graph_area, dock_area) = if !self.dock.open {
            (body, None)
        } else if body.width >= 140 {
            let [g, d] = Split::horizontal([Constraint::Percentage(58), Constraint::Percentage(42)]).areas(body);
            (g, Some(d))
        } else if body.width >= 90 {
            let [g, d] = Split::vertical([Constraint::Percentage(55), Constraint::Percentage(45)]).areas(body);
            (g, Some(d))
        } else if self.focus == FocusArea::Dock {
            (Area::new(body.x, body.y, body.width, 0), Some(body))
        } else {
            (body, None)
        };
        self.graph_area = graph_area;
        self.dock_area = dock_area.unwrap_or_default();
        let scene = self.scene(self.selected)?;
        let live_flow = self.strands[self.selected].phase != Phase::Waiting;
        let frame_no = self.frame;
        if graph_area.height > 0 {
            let strand = &mut self.strands[self.selected];
            strand
                .graph
                .draw(frame.buffer_mut(), graph_area, &scene, &theme, frame_no, if live_flow { 18 } else { 0 })
                .map_err(Self::inconsistent)?;
        }
        if let Some(dock_area) = dock_area {
            let strand = &self.strands[self.selected];
            let focus_node = strand.graph.focus.as_ref().and_then(|id| scene.node(id));
            let empty: Vec<String> = Vec::new();
            let notes = strand.report.as_ref().map(|r| &r.notes).unwrap_or(&empty);
            let ctx = DockContext {
                theme: &theme,
                now: Instant::now(),
                machine: &self.machine,
                run_label: &strand.run_label,
                view: &strand.session.view,
                scene: &scene,
                live: &strand.live,
                focus: focus_node,
                arguments: &strand.session.arguments,
                sources: &strand.session.sources,
                protocol: self.protocol.as_ref(),
                run_rids: &strand.rids,
                stdout: &strand.stdout,
                notes,
            };
            self.selection = dock::draw(frame.buffer_mut(), dock_area, &mut self.dock, &ctx, self.focus == FocusArea::Dock);
        }
        self.draw_footer(frame, footer, &theme);
        if self.help {
            draw_help(frame, body, &theme);
        }
        if let Some(palette) = &self.palette {
            palette.draw(frame.buffer_mut(), body, &theme);
        }
        if self.confirm_quit {
            let text = "Runs are still going. Abort them and quit?  y yes · n no";
            let w = (text::width(text) as u16 + 4).min(body.width);
            let x = body.x + (body.width - w) / 2;
            let y = body.y + body.height / 2;
            frame.buffer_mut().set_string(
                x,
                y,
                text::pad(&format!("  {text}"), w as usize),
                theme.fg(Tone::Ink).add_modifier(Modifier::BOLD).patch(theme.soft(Tone::Warning)),
            );
        }
        Ok(())
    }

    fn draw_top(&mut self, frame: &mut Frame, area: Area, theme: &Theme) {
        let buffer = frame.buffer_mut();
        buffer.set_style(area, theme.chrome());
        let mut x = area.x + 1;
        let title = format!("capdag ▸ {}", self.machine);
        let title = text::truncate(&title, (area.width / 3) as usize);
        buffer.set_string(x, area.y, &title, theme.fg(Tone::Ink).add_modifier(Modifier::BOLD).patch(theme.chrome()));
        x += text::width(&title) as u16 + 2;
        // One tab per strand of every run: its run and strand, and its state.
        self.strand_tabs.clear();
        let several = self.strands.len() > 1;
        for (index, strand) in self.strands.iter().enumerate() {
            if !several {
                break;
            }
            let mark = match (&strand.phase, &strand.live.status) {
                (Phase::Waiting, _) => "·".to_string(),
                (_, RunStatus::Running) | (_, RunStatus::Pending) => {
                    theme.glyphs.status(crate::tui::theme::StatusMark::Spinner(self.frame)).to_string()
                }
                (_, RunStatus::Completed(RunVerdict::Completed)) => "✓".to_string(),
                (_, RunStatus::Completed(_)) => "△".to_string(),
                (_, RunStatus::Failed(_)) => "✗".to_string(),
                (_, RunStatus::Cancelled(_)) => "◼".to_string(),
            };
            let label = format!(" {}·{} {mark} ", strand.key.run + 1, strand.key.strand + 1);
            let w = text::width(&label) as u16;
            if x + w >= area.x + area.width / 2 + area.width / 4 {
                break;
            }
            let style = if index == self.selected {
                theme.fg(Tone::Ink).add_modifier(Modifier::BOLD).patch(theme.soft(Tone::Accent))
            } else {
                theme.fg(Tone::Secondary).patch(theme.chrome())
            };
            buffer.set_string(x, area.y, &label, style);
            self.strand_tabs.push((x, x + w, index));
            x += w + 1;
        }
        let strand = &self.strands[self.selected];
        let (status, tone) = if strand.phase == Phase::Waiting {
            ("waiting for the run before it".to_string(), Tone::Muted)
        } else {
            dock::status_text(&strand.live)
        };
        let elapsed = if strand.phase == Phase::Waiting {
            String::new()
        } else {
            format!(" · {}", text::duration(strand.live.elapsed(Instant::now())))
        };
        let right = format!(" {status}{elapsed} ");
        let w = text::width(&right) as u16;
        if area.width > w + x - area.x {
            buffer.set_string(area.x + area.width - w, area.y, &right, theme.fg(tone).add_modifier(Modifier::BOLD).patch(theme.soft(tone)));
        }
    }

    fn draw_footer(&mut self, frame: &mut Frame, area: Area, theme: &Theme) {
        let buffer = frame.buffer_mut();
        buffer.set_style(area, theme.chrome());
        let now = Instant::now();
        self.toasts.retain(|(_, _, at)| now.duration_since(*at) < Duration::from_secs(6));
        if let Some((text_value, tone, _)) = self.toasts.back() {
            buffer.set_string(area.x + 1, area.y, text::truncate(text_value, area.width as usize - 2), theme.fg(*tone).add_modifier(Modifier::BOLD).patch(theme.chrome()));
            return;
        }
        let hints = match (self.focus, self.dock.tab, self.dock.editing.is_some()) {
            (_, _, true) => "type the value · ⏎ keep · esc cancel",
            (FocusArea::Dock, DockTab::Step, false) => "↑↓ argument · ⏎ edit · a apply · N new run · x discard · ← → tabs · esc graph",
            (FocusArea::Dock, DockTab::Outputs, false) => "↑↓ file · o open · R reveal · Y copy path · PgUp/PgDn scroll · esc graph",
            (FocusArea::Dock, DockTab::Messages, false) => "↑↓ scroll · l level · F follow · ← → tabs · esc graph",
            (FocusArea::Dock, _, false) => "↑↓ select · PgUp/PgDn scroll · ← → tabs · esc graph",
            (FocusArea::Graph, _, false) => "←↑↓→ move · ⏎ inspect · tab next · +/- detail · f fit · d dock · s stop · ^C abort · : commands · ? keys · q quit",
        };
        buffer.set_string(area.x + 1, area.y, text::truncate(hints, area.width as usize - 2), theme.fg(Tone::Muted).patch(theme.chrome()));
    }

    fn act(&mut self, action: Action, tasks: &mut Tasks, handles: &mut HashMap<usize, tokio::task::AbortHandle>) {
        match action {
            Action::Quit => {
                if self.strands.iter().any(|s| s.phase == Phase::Running) {
                    self.confirm_quit = true;
                } else {
                    self.quit = true;
                }
            }
            Action::StopInput => self.stop_input(),
            Action::Abort => self.abort(),
            Action::ToggleDock => {
                self.dock.open = !self.dock.open;
                if !self.dock.open {
                    self.focus = FocusArea::Graph;
                }
            }
            Action::Dock(tab) => {
                self.dock.show(tab);
                self.focus = FocusArea::Dock;
            }
            Action::NextStrand => self.selected = (self.selected + 1) % self.strands.len(),
            Action::PreviousStrand => self.selected = (self.selected + self.strands.len() - 1) % self.strands.len(),
            Action::ZoomIn => self.strands[self.selected].graph.zoom_in(),
            Action::ZoomOut => self.strands[self.selected].graph.zoom_out(),
            Action::Fit => self.strands[self.selected].graph.fit(),
            Action::Centre => self.strands[self.selected].graph.centre(),
            Action::Wrap => {
                self.strands[self.selected].graph.toggle_wrap();
            }
            Action::Minimap => {
                let graph = &mut self.strands[self.selected].graph;
                graph.minimap = !graph.minimap;
            }
            Action::CopyNotation => match copy_to_clipboard(&self.notation) {
                Ok(()) => self.toast("The machine's notation is on the clipboard.", Tone::Success),
                Err(e) => self.toast(format!("Copying failed: {e}"), Tone::Danger),
            },
            Action::Open | Action::Reveal | Action::CopyPath => self.on_file(action),
            Action::ApplyEdits => self.apply_edits(),
            Action::NewRunWithEdits => self.new_run_with_edits(tasks, handles),
            Action::DiscardEdits => {
                self.dock.pending.clear();
                self.dock.editing = None;
                self.toast("Argument edits discarded.", Tone::Muted);
            }
            Action::Help => self.help = !self.help,
        }
    }

    fn on_file(&mut self, action: Action) {
        let path = match &self.selection {
            Selection::File(path) => path.clone(),
            _ => {
                self.toast("Select a file in the Outputs tab first.", Tone::Warning);
                return;
            }
        };
        let done = match action {
            Action::Open => open_path(&path, false).map(|_| "Opened"),
            Action::Reveal => open_path(&path, true).map(|_| "Revealed"),
            Action::CopyPath => copy_to_clipboard(&path.display().to_string()).map(|_| "Copied the path of"),
            _ => unreachable!("only file actions reach here"),
        };
        match done {
            Ok(verb) => self.toast(format!("{verb} {}", path.display()), Tone::Success),
            Err(e) => self.toast(format!("{}: {e}", path.display()), Tone::Danger),
        }
    }

    fn stop_input(&mut self) {
        if !self.strands.iter().any(|s| s.phase == Phase::Running) {
            self.toast("No run is going: there is no input to stop.", Tone::Warning);
            return;
        }
        self.stop_requested = true;
        self.toast("Stopping input… Ctrl-C again aborts.", Tone::Warning);
        let runtime = self.runtime.clone();
        let reply = self.reply_tx.clone();
        tokio::spawn(async move {
            let answer = match runtime.stop_live_inputs().await {
                Ok(outcome) => Reply::Toast(
                    format!(
                        "Input stopped: {} host tap(s) closed, {} feed request(s) stopped, {} ended — the run drains to complete outputs.",
                        outcome.host_taps_closed, outcome.feed_requests_stopped, outcome.feeds_ended
                    ),
                    Tone::Success,
                ),
                Err(e) => Reply::Toast(format!("Stop input refused: {e}"), Tone::Warning),
            };
            let _ = reply.send(answer);
        });
    }

    fn abort(&mut self) {
        if self.aborted {
            return;
        }
        self.aborted = true;
        self.abort_deadline = Some(Instant::now() + Duration::from_secs(5));
        self.toast("Aborting: every request is cancelled.", Tone::Danger);
        let runtime = self.runtime.clone();
        tokio::spawn(async move {
            if let Some(switch) = runtime.switch().await {
                switch
                    .cancel_all_requests(&crate::bifaci::frame::CancelReason::user(false))
                    .await;
            }
        });
    }

    /// The pending edits of the selected strand, as ledger updates.
    fn pending_updates(&self) -> Vec<ArgumentUpdate> {
        self.dock
            .pending
            .iter()
            .flat_map(|(token, args)| {
                args.values().map(move |(value, media_urn)| ArgumentUpdate {
                    token_id: token.clone(),
                    media_urn: media_urn.clone(),
                    value: value.clone().into_bytes(),
                })
            })
            .collect()
    }

    fn apply_edits(&mut self) {
        let updates = self.pending_updates();
        if updates.is_empty() {
            self.toast("No argument edits to apply.", Tone::Warning);
            return;
        }
        let strand = &mut self.strands[self.selected];
        let (Phase::Running, Some(ledger)) = (strand.phase, &strand.ledger) else {
            self.toast("This run is not going: start a new run with the edits (N).", Tone::Warning);
            return;
        };
        match ledger.apply(&updates) {
            Ok(applied) => {
                let mut verdicts = Vec::new();
                for outcome in &applied.outcomes {
                    let name = strand
                        .session
                        .arguments
                        .get(&outcome.token_id)
                        .and_then(|args| args.iter().find(|a| a.media_urn == outcome.media_urn))
                        .map(|a| a.name.clone())
                        .unwrap_or_else(|| outcome.media_urn.clone());
                    let reached = match &outcome.disposition {
                        ArgumentUpdateDisposition::Applied => "applied: the step has not started".to_string(),
                        ArgumentUpdateDisposition::AppliedToRemainingBodies { bodies_dispatched } => format!(
                            "applied to the runs not yet started ({bodies_dispatched} already sent with the old value)"
                        ),
                        ArgumentUpdateDisposition::AlreadyDispatched => "not applied: the step was already sent".to_string(),
                    };
                    verdicts.push(format!("{name}: {reached}"));
                    if outcome.disposition != ArgumentUpdateDisposition::AlreadyDispatched {
                        if let Some(arg) = strand
                            .session
                            .arguments
                            .get_mut(&outcome.token_id)
                            .and_then(|args| args.iter_mut().find(|a| a.media_urn == outcome.media_urn))
                        {
                            let update = updates
                                .iter()
                                .find(|u| u.token_id == outcome.token_id && u.media_urn == outcome.media_urn)
                                .expect("every outcome is of an update");
                            arg.value_text = Some(String::from_utf8_lossy(&update.value).into_owned());
                            arg.origin = ArgOrigin::User;
                        }
                    }
                }
                self.dock.verdicts = verdicts;
                self.dock.pending.clear();
                self.toast(format!("Applied (revision {}).", applied.revision), Tone::Success);
            }
            Err(e) => self.toast(format!("Refused: {e}"), Tone::Danger),
        }
    }

    fn new_run_with_edits(&mut self, tasks: &mut Tasks, handles: &mut HashMap<usize, tokio::task::AbortHandle>) {
        let updates = self.pending_updates();
        if updates.is_empty() {
            self.toast("No argument edits for a new run.", Tone::Warning);
            return;
        }
        let base = &self.strands[self.selected];
        let mut session = base.session.clone();
        for update in &updates {
            let values = session.planned.cap_arguments.entry(update.token_id.clone()).or_default();
            let urn = match MediaUrn::from_string(&update.media_urn) {
                Ok(urn) => urn,
                Err(e) => {
                    self.toast(format!("Refused: '{}' is no media URN: {e}", update.media_urn), Tone::Danger);
                    return;
                }
            };
            values.retain(|(stored, _)| {
                !MediaUrn::from_string(stored).is_ok_and(|s| s.is_equivalent(&urn).unwrap_or(false))
            });
            values.push((update.media_urn.clone(), update.value.clone()));
            if let Some(arg) = session
                .arguments
                .get_mut(&update.token_id)
                .and_then(|args| args.iter_mut().find(|a| a.media_urn == update.media_urn))
            {
                arg.value_text = Some(String::from_utf8_lossy(&update.value).into_owned());
                arg.origin = ArgOrigin::User;
            }
        }
        let run_index = self.runs.len();
        let label = format!("{} · with changes", base.run_label);
        let stem = format!("{}.rerun{run_index}", base.stem);
        let index = self.strands.len();
        let mut graph = GraphView::default();
        graph.wrapped = base.graph.wrapped;
        self.strands.push(Strand {
            key: RunKey { run: run_index, strand: 0 },
            run_label: label.clone(),
            stem,
            live: RunLive::new(&session.view, Instant::now()),
            session,
            ledger: None,
            phase: Phase::Waiting,
            report: None,
            stdout: Vec::new(),
            graph,
            rids: Vec::new(),
        });
        self.runs.push((label, vec![index]));
        self.ends.push(None);
        self.dock.pending.clear();
        self.toast("A new run with the edits is queued.", Tone::Success);
        self.start_next_run(tasks, handles);
    }

    fn on_key(&mut self, key: KeyEvent, tasks: &mut Tasks, handles: &mut HashMap<usize, tokio::task::AbortHandle>) -> Result<(), TuiError> {
        if key.kind == KeyEventKind::Release {
            return Ok(());
        }
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        // Ctrl-C is the CLI's: the first stops input, the second aborts.
        if ctrl && key.code == KeyCode::Char('c') {
            if self.stop_requested || !self.strands.iter().any(|s| s.phase == Phase::Running) {
                if self.strands.iter().any(|s| s.phase == Phase::Running) {
                    self.abort();
                } else {
                    self.quit = true;
                }
            } else {
                self.stop_input();
            }
            return Ok(());
        }
        if ctrl && key.code == KeyCode::Char('z') {
            self.suspend = true;
            return Ok(());
        }
        if self.confirm_quit {
            match key.code {
                KeyCode::Char('y') => {
                    self.abort();
                    self.quit = true;
                }
                _ => self.confirm_quit = false,
            }
            return Ok(());
        }
        if let Some(palette) = &mut self.palette {
            match palette.on_key(key) {
                PaletteResult::Continue => {}
                PaletteResult::Close => self.palette = None,
                PaletteResult::Run(action) => {
                    self.palette = None;
                    self.act(action, tasks, handles);
                }
            }
            return Ok(());
        }
        if self.help {
            self.help = false;
            return Ok(());
        }
        if let Some(editing) = &mut self.dock.editing {
            match key.code {
                KeyCode::Esc => self.dock.editing = None,
                KeyCode::Enter => {
                    let editing = self.dock.editing.take().expect("matched above");
                    self.dock
                        .pending
                        .entry(editing.token)
                        .or_default()
                        .insert(editing.arg, (editing.text, editing.media_urn));
                }
                KeyCode::Backspace => {
                    editing.text.pop();
                }
                KeyCode::Char(c) => editing.text.push(c),
                _ => {}
            }
            return Ok(());
        }
        if (ctrl && key.code == KeyCode::Char('k')) || key.code == KeyCode::Char(':') {
            self.palette = Some(Palette::new(commands()));
            return Ok(());
        }
        // Keys every focus shares.
        let global = match key.code {
            KeyCode::Char('q') => Some(Action::Quit),
            KeyCode::Char('?') => Some(Action::Help),
            KeyCode::Char('s') => Some(Action::StopInput),
            KeyCode::Char('d') => Some(Action::ToggleDock),
            KeyCode::Char('y') => Some(Action::CopyNotation),
            KeyCode::Char('>') => Some(Action::NextStrand),
            KeyCode::Char('<') => Some(Action::PreviousStrand),
            KeyCode::Char('o') => Some(Action::Open),
            KeyCode::Char('R') => Some(Action::Reveal),
            KeyCode::Char('Y') => Some(Action::CopyPath),
            KeyCode::Char('a') => Some(Action::ApplyEdits),
            KeyCode::Char('N') => Some(Action::NewRunWithEdits),
            KeyCode::Char('x') => Some(Action::DiscardEdits),
            KeyCode::Char(c @ '1'..='6') => Some(Action::Dock(DockTab::ALL[c as usize - '1' as usize])),
            _ => None,
        };
        if let Some(action) = global {
            self.act(action, tasks, handles);
            return Ok(());
        }
        match self.focus {
            FocusArea::Graph => {
                let graph = &mut self.strands[self.selected].graph;
                if let GraphAction::Inspect(id) = graph.on_key(key) {
                    self.inspect(&id);
                }
            }
            FocusArea::Dock => self.on_dock_key(key),
        }
        Ok(())
    }

    /// Open the dock on what a node is.
    fn inspect(&mut self, id: &str) {
        let Ok(scene) = self.scene(self.selected) else { return };
        let Some(node) = scene.node(id) else { return };
        let tab = match node.role {
            crate::tui::scene::NodeRole::Map => DockTab::Items,
            crate::tui::scene::NodeRole::Output => DockTab::Outputs,
            _ => DockTab::Step,
        };
        self.dock.show(tab);
        self.focus = FocusArea::Dock;
    }

    fn on_dock_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => self.focus = FocusArea::Graph,
            KeyCode::Left | KeyCode::Char('h') => self.dock.next_tab(false),
            KeyCode::Right | KeyCode::Char('l') if self.dock.tab != DockTab::Messages => self.dock.next_tab(true),
            KeyCode::Char('l') => self.dock.levels = self.dock.levels.next(),
            KeyCode::Right => self.dock.next_tab(true),
            KeyCode::Char('F') => self.dock.follow = !self.dock.follow,
            KeyCode::Up | KeyCode::Char('k') => {
                if self.dock.tab == DockTab::Messages {
                    self.dock.follow = false;
                    self.dock.scroll = self.dock.scroll.saturating_sub(1);
                } else {
                    self.dock.selected = self.dock.selected.saturating_sub(1);
                    self.dock.scroll = 0;
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.dock.tab == DockTab::Messages {
                    self.dock.scroll += 1;
                } else {
                    self.dock.selected += 1;
                    self.dock.scroll = 0;
                }
            }
            KeyCode::PageUp => self.dock.scroll = self.dock.scroll.saturating_sub(10),
            KeyCode::PageDown => self.dock.scroll += 10,
            KeyCode::Enter => {
                if let Selection::Argument { token, name, editable } = &self.selection {
                    if !*editable {
                        self.toast("The machine binds this argument: it takes no value from a run.", Tone::Warning);
                        return;
                    }
                    let strand = &self.strands[self.selected];
                    if let Some(arg) = strand
                        .session
                        .arguments
                        .get(token)
                        .and_then(|args| args.iter().find(|a| &a.name == name))
                    {
                        self.dock.editing = Some(Editing {
                            token: token.clone(),
                            arg: name.clone(),
                            media_urn: arg.media_urn.clone(),
                            text: arg.value_text.clone().unwrap_or_default(),
                        });
                    }
                }
            }
            _ => {}
        }
    }

    fn on_mouse(&mut self, mouse: MouseEvent) {
        let in_dock = self.dock.open
            && mouse.column >= self.dock_area.x
            && mouse.row >= self.dock_area.y
            && mouse.column < self.dock_area.x + self.dock_area.width
            && mouse.row < self.dock_area.y + self.dock_area.height;
        if mouse.row == 0 {
            if let MouseEventKind::Down(_) = mouse.kind {
                if let Some(&(_, _, index)) = self.strand_tabs.iter().find(|(a, b, _)| mouse.column >= *a && mouse.column < *b) {
                    self.selected = index;
                }
            }
            return;
        }
        if in_dock {
            match mouse.kind {
                MouseEventKind::Down(_) => self.focus = FocusArea::Dock,
                MouseEventKind::ScrollDown => self.dock.scroll += 2,
                MouseEventKind::ScrollUp => self.dock.scroll = self.dock.scroll.saturating_sub(2),
                _ => {}
            }
            return;
        }
        let action = self.strands[self.selected].graph.on_mouse(mouse);
        if let GraphAction::Inspect(id) = action {
            self.focus = FocusArea::Graph;
            self.inspect(&id);
            self.focus = FocusArea::Graph;
        }
    }

    fn outcome(self) -> SessionOutcome {
        SessionOutcome {
            stdout: self.stdout,
            notes: self.notes,
            ends: self.ends.into_iter().flatten().collect(),
            aborted: self.aborted,
        }
    }
}

fn draw_help(frame: &mut Frame, area: Area, theme: &Theme) {
    let groups: [(&str, &[(&str, &str)]); 4] = [
        (
            "Graph",
            &[
                ("←↑↓→ hjkl", "move the focus"),
                ("HJKL ⇧←↑↓→", "pan"),
                ("tab ⇧tab", "next / previous node"),
                ("] [", "follow an edge out / in"),
                ("⏎", "inspect the focused node"),
                ("+ -", "more / less detail"),
                ("f c", "fit · centre on what matters"),
                ("w m", "wrap · minimap"),
            ],
        ),
        (
            "Run",
            &[
                ("s ^C", "stop input — the run drains"),
                ("^C ^C", "abort"),
                ("< >", "previous / next strand or run"),
                ("y", "copy the notation"),
                ("q", "quit"),
            ],
        ),
        (
            "Dock",
            &[
                ("1–6", "run · step · items · outputs · messages · protocol"),
                ("d", "show / hide"),
                ("⏎", "edit the selected argument"),
                ("a N x", "apply · new run · discard edits"),
                ("o R Y", "open · reveal · copy path"),
            ],
        ),
        ("Anywhere", &[(": ^K", "all commands"), ("^Z", "suspend"), ("?", "these keys")]),
    ];
    let mut lines: Vec<Line> = Vec::new();
    for (group, keys) in groups {
        lines.push(Line::from(Span::styled(group, theme.fg(Tone::Ink).add_modifier(Modifier::BOLD))));
        for (keys, what) in keys {
            lines.push(Line::from(vec![
                Span::styled(format!("  {:<14}", keys), theme.fg(Tone::Accent)),
                Span::styled(what.to_string(), theme.fg(Tone::Secondary)),
            ]));
        }
        lines.push(Line::raw(""));
    }
    let w = 62.min(area.width);
    let h = (lines.len() as u16 + 2).min(area.height);
    let rect = Area::new(area.x + (area.width - w) / 2, area.y + (area.height - h) / 2, w, h);
    frame.buffer_mut().set_style(rect, theme.chrome());
    Paragraph::new(lines)
        .wrap(Wrap { trim: false })
        .block(
            ratatui::widgets::Block::bordered()
                .title(" Keys ")
                .border_style(theme.fg(Tone::Accent))
                .style(theme.chrome()),
        )
        .render(rect, frame.buffer_mut());
}

/// Show `spec`'s runs and run them, until the user quits. The terminal is
/// the view's until then.
pub async fn run_session(
    runtime: Arc<CliRuntime>,
    spec: RunSessionSpec,
    settings: TuiSettings,
) -> Result<SessionOutcome, TuiError> {
    let mut tui = Tui::enter()?;
    let outcome = run_session_on(&mut tui, EventStream::new(), runtime, spec, settings).await;
    drop(tui);
    outcome
}

/// [`run_session`] on any display, reading events from `input`.
pub async fn run_session_on<D, S>(
    display: &mut D,
    mut input: S,
    runtime: Arc<CliRuntime>,
    spec: RunSessionSpec,
    settings: TuiSettings,
) -> Result<SessionOutcome, TuiError>
where
    D: Display,
    S: futures_util::Stream<Item = std::io::Result<Event>> + Unpin,
{
    let (observer, mut events) = RunObserver::new();
    let (transient_tx, mut transient_rx) = mpsc::unbounded_channel();
    let (reply_tx, mut reply_rx) = mpsc::unbounded_channel();
    let now = Instant::now();
    let mut strands = Vec::new();
    let mut runs = Vec::new();
    for (run_index, run) in spec.runs.into_iter().enumerate() {
        let mut indexes = Vec::new();
        for (strand_index, session) in run.strands.into_iter().enumerate() {
            indexes.push(strands.len());
            strands.push(Strand {
                key: RunKey {
                    run: run_index,
                    strand: strand_index,
                },
                run_label: run.label.clone(),
                stem: run.stem.clone(),
                live: RunLive::new(&session.view, now),
                session,
                ledger: None,
                phase: Phase::Waiting,
                report: None,
                stdout: Vec::new(),
                graph: GraphView::default(),
                rids: Vec::new(),
            });
        }
        runs.push((run.label, indexes));
    }
    if strands.is_empty() {
        return Err(TuiError::Inconsistent("the session has nothing to run".to_string()));
    }
    let run_count = runs.len();
    let mut app = App {
        settings,
        machine: spec.machine,
        notation: spec.notation,
        output_dir: spec.output_dir,
        force: spec.force,
        planned_runs: run_count,
        runtime,
        observer,
        strands,
        runs,
        running: None,
        next_run: 0,
        ends: vec![None; run_count],
        selected: 0,
        dock: DockState::default(),
        focus: FocusArea::Graph,
        selection: Selection::None,
        palette: None,
        help: false,
        confirm_quit: false,
        toasts: VecDeque::new(),
        stop_requested: false,
        aborted: false,
        abort_deadline: None,
        protocol: None,
        frame: 0,
        quit: false,
        suspend: false,
        graph_area: Area::default(),
        dock_area: Area::default(),
        strand_tabs: Vec::new(),
        transient_tx,
        reply_tx,
        stdout: Vec::new(),
        notes: Vec::new(),
        fatal: None,
    };
    let mut tasks: Tasks = FuturesUnordered::new();
    let mut handles: HashMap<usize, tokio::task::AbortHandle> = HashMap::new();
    app.start_next_run(&mut tasks, &mut handles);
    let mut sampler = tokio::time::interval(Duration::from_millis(250));
    sampler.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut animation = tokio::time::interval(Duration::from_millis(90));
    animation.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut dirty = true;
    loop {
        if app.suspend {
            app.suspend = false;
            display.suspend()?;
            dirty = true;
        }
        if dirty {
            display.draw(|frame| app.draw(frame))?;
            dirty = false;
        }
        if let Some(fatal) = app.fatal.take() {
            for handle in handles.values() {
                handle.abort();
            }
            return Err(fatal);
        }
        if app.quit {
            break;
        }
        // An abort gives the requests a grace to end as cancelled; what is
        // still running after it is stopped.
        if app.abort_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            for handle in handles.values() {
                handle.abort();
            }
            app.abort_deadline = None;
        }
        tokio::select! {
            event = input.next() => match event {
                Some(Ok(Event::Key(key))) => {
                    app.on_key(key, &mut tasks, &mut handles)?;
                    dirty = true;
                }
                Some(Ok(Event::Mouse(mouse))) => {
                    app.on_mouse(mouse);
                    dirty = true;
                }
                Some(Ok(Event::Resize(_, _))) => dirty = true,
                Some(Ok(Event::Paste(text))) => {
                    if let Some(editing) = &mut app.dock.editing {
                        editing.text.push_str(&text);
                        dirty = true;
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e.into()),
                None => break,
            },
            Some(event) = events.recv() => {
                app.apply_event(event)?;
                dirty = true;
            }
            Some((key, artifact)) = transient_rx.recv() => {
                app.apply_transient(key, artifact)?;
                dirty = true;
            }
            Some((index, joined)) = tasks.next(), if !tasks.is_empty() => {
                // Everything the strand said before it returned is folded
                // first: its end follows its signals.
                while let Ok(event) = events.try_recv() {
                    app.apply_event(event)?;
                }
                app.strand_ended(index, joined, &mut tasks, &mut handles)?;
                dirty = true;
            }
            Some(reply) = reply_rx.recv() => {
                match reply {
                    Reply::Toast(text, tone) => app.toast(text, tone),
                }
                dirty = true;
            }
            _ = sampler.tick() => {
                if app.sample().await? {
                    dirty = true;
                }
            }
            _ = animation.tick(), if app.animating() => {
                app.frame = app.frame.wrapping_add(1);
                dirty = true;
            }
        }
    }
    // Quitting while runs go aborted them: wait for them to end as cancelled
    // (bounded by the abort's grace), so their outputs are swept and their
    // ends reported.
    while !tasks.is_empty() {
        let grace = app
            .abort_deadline
            .map(|d| d.saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::from_secs(5));
        match tokio::time::timeout(grace, tasks.next()).await {
            Ok(Some((index, joined))) => {
                while let Ok(event) = events.try_recv() {
                    app.apply_event(event)?;
                }
                app.strand_ended(index, joined, &mut tasks, &mut handles)?;
            }
            Ok(None) => break,
            Err(_) => {
                for handle in handles.values() {
                    handle.abort();
                }
                app.abort_deadline = Some(Instant::now() + Duration::from_secs(5));
            }
        }
    }
    Ok(app.outcome())
}
