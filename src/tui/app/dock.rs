//! The dock: the run view's inspectors, one tab each, as the uikit's dock
//! has them — the run and its protocol health, the focused step, a map's
//! items, the outputs, the run's messages, the protocol in full.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Layout as Split, Rect as Area};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, Tabs, Widget, Wrap};

use crate::bifaci::relay_switch::RelaySwitchProtocolStats;
use crate::orchestrator::{PlanView, PlanViewKind};
use crate::tui::app::preview::{self, Preview};
use crate::tui::live::{ItemLive, OutputEnd, RunLive, RunStatus, StepPhase};
use crate::tui::scene::{attribution_label, classify_flow, ArgOrigin, NodeRole, Scene, SceneNode, Source, StepArgument};
use crate::tui::text;
use crate::tui::theme::{MediaKind, Theme, Tone};

/// The dock's tabs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DockTab {
    Run,
    Step,
    Items,
    Outputs,
    Messages,
    Protocol,
}

impl DockTab {
    pub const ALL: [DockTab; 6] = [
        DockTab::Run,
        DockTab::Step,
        DockTab::Items,
        DockTab::Outputs,
        DockTab::Messages,
        DockTab::Protocol,
    ];

    pub fn title(self) -> &'static str {
        match self {
            DockTab::Run => "Run",
            DockTab::Step => "Step",
            DockTab::Items => "Items",
            DockTab::Outputs => "Outputs",
            DockTab::Messages => "Messages",
            DockTab::Protocol => "Protocol",
        }
    }

    pub fn index(self) -> usize {
        DockTab::ALL.iter().position(|t| *t == self).expect("listed")
    }
}

/// Which log levels the messages tab shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LevelFilter {
    All,
    InfoAndUp,
    WarnAndUp,
    Errors,
}

impl LevelFilter {
    pub fn next(self) -> Self {
        match self {
            LevelFilter::All => LevelFilter::InfoAndUp,
            LevelFilter::InfoAndUp => LevelFilter::WarnAndUp,
            LevelFilter::WarnAndUp => LevelFilter::Errors,
            LevelFilter::Errors => LevelFilter::All,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            LevelFilter::All => "all",
            LevelFilter::InfoAndUp => "info and up",
            LevelFilter::WarnAndUp => "warnings and errors",
            LevelFilter::Errors => "errors",
        }
    }

    pub fn admits(self, level: &str) -> bool {
        let rank = match level {
            "error" => 3,
            "warn" => 2,
            "info" | "status" => 1,
            _ => 0,
        };
        match self {
            LevelFilter::All => true,
            LevelFilter::InfoAndUp => rank >= 1,
            LevelFilter::WarnAndUp => rank >= 2,
            LevelFilter::Errors => rank >= 3,
        }
    }
}

/// An argument value being typed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Editing {
    pub token: String,
    pub arg: String,
    pub media_urn: String,
    pub text: String,
}

/// The dock's own state.
#[derive(Debug, Clone)]
pub struct DockState {
    pub open: bool,
    pub tab: DockTab,
    /// The selected row of the current tab's list.
    pub selected: usize,
    /// How far the current tab's preview or log is scrolled.
    pub scroll: usize,
    pub levels: LevelFilter,
    /// Messages follow the newest.
    pub follow: bool,
    pub editing: Option<Editing>,
    /// Argument values typed and not yet applied: by step token, then by
    /// argument name, the value and the media URN it is delivered under.
    pub pending: BTreeMap<String, BTreeMap<String, (String, String)>>,
    /// The last application's verdict per step and argument.
    pub verdicts: Vec<String>,
    /// Loaded previews, by what they preview.
    previews: HashMap<String, Preview>,
}

impl Default for DockState {
    fn default() -> Self {
        Self {
            open: true,
            tab: DockTab::Run,
            selected: 0,
            scroll: 0,
            levels: LevelFilter::All,
            follow: true,
            editing: None,
            pending: BTreeMap::new(),
            verdicts: Vec::new(),
            previews: HashMap::new(),
        }
    }
}

impl DockState {
    pub fn show(&mut self, tab: DockTab) {
        if self.tab != tab {
            self.selected = 0;
            self.scroll = 0;
        }
        self.tab = tab;
        self.open = true;
    }

    pub fn next_tab(&mut self, forward: bool) {
        let i = self.tab.index();
        let n = DockTab::ALL.len();
        let next = if forward { (i + 1) % n } else { (i + n - 1) % n };
        self.show(DockTab::ALL[next]);
    }

    /// Forget a preview (its file changed or its size did).
    pub fn forget_previews(&mut self) {
        self.previews.clear();
    }

    fn preview_of(&mut self, key: String, load: impl FnOnce() -> Preview) -> &Preview {
        self.previews.entry(key).or_insert_with(load)
    }
}

/// Everything a dock tab draws from.
pub struct DockContext<'a> {
    pub theme: &'a Theme,
    pub now: Instant,
    pub machine: &'a str,
    pub run_label: &'a str,
    pub view: &'a PlanView,
    pub scene: &'a Scene,
    pub live: &'a RunLive,
    pub focus: Option<&'a SceneNode>,
    pub arguments: &'a HashMap<String, Vec<StepArgument>>,
    pub sources: &'a BTreeMap<String, Vec<Source>>,
    pub protocol: Option<&'a RelaySwitchProtocolStats>,
    /// The request ids this run's invocations were registered under.
    pub run_rids: &'a [String],
    /// What the run printed to standard output (its scalar result).
    pub stdout: &'a [u8],
    /// What the CLI says about the run once it ended.
    pub notes: &'a [String],
}

/// A row that can be selected in the current tab: what Enter acts on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selection {
    /// An argument of a step, by token and name.
    Argument { token: String, name: String, editable: bool },
    /// A file, to preview, open, reveal or copy.
    File(std::path::PathBuf),
    /// The run's standard output.
    Stdout,
    /// A map's run.
    Item { map_token: String, index: usize },
    None,
}

fn label(theme: &Theme, text: &str) -> Span<'static> {
    Span::styled(text.to_string(), theme.fg(Tone::Muted))
}

fn value(theme: &Theme, text: impl Into<String>) -> Span<'static> {
    Span::styled(text.into(), theme.fg(Tone::Ink))
}

fn heading(theme: &Theme, text: &str) -> Line<'static> {
    Line::from(Span::styled(text.to_string(), theme.fg(Tone::Ink).add_modifier(Modifier::BOLD)))
}

/// How a run's status reads, and its tone.
pub fn status_text(live: &RunLive) -> (String, Tone) {
    match &live.status {
        RunStatus::Pending => ("queued".to_string(), Tone::Muted),
        RunStatus::Running => (format!("running · {:.0}%", live.progress * 100.0), Tone::Accent),
        RunStatus::Completed(crate::orchestrator::RunVerdict::Completed) => ("done".to_string(), Tone::Success),
        RunStatus::Completed(crate::orchestrator::RunVerdict::CompletedWithToleratedFailures) => {
            ("done, with tolerated failures".to_string(), Tone::Warning)
        }
        RunStatus::Failed(_) => ("failed".to_string(), Tone::Danger),
        RunStatus::Cancelled(_) => ("stopped".to_string(), Tone::Muted),
    }
}

/// Draw the dock into `area`, and say what the selected row is.
pub fn draw(buffer: &mut Buffer, area: Area, state: &mut DockState, ctx: &DockContext<'_>, focused: bool) -> Selection {
    let theme = ctx.theme;
    let border_tone = if focused { Tone::Accent } else { Tone::Border };
    let block = Block::default()
        .borders(Borders::LEFT | Borders::TOP)
        .border_style(theme.fg(border_tone))
        .style(theme.chrome());
    let inner = block.inner(area);
    block.render(area, buffer);
    if inner.height < 3 || inner.width < 10 {
        return Selection::None;
    }
    let [tabs_area, body] = Split::vertical([Constraint::Length(1), Constraint::Min(1)]).areas(inner);
    let titles: Vec<Line> = DockTab::ALL
        .iter()
        .enumerate()
        .map(|(i, tab)| Line::from(format!("{} {}", i + 1, tab.title())))
        .collect();
    Tabs::new(titles)
        .select(state.tab.index())
        .style(theme.fg(Tone::Muted))
        .highlight_style(theme.fg(Tone::Ink).add_modifier(Modifier::BOLD | Modifier::UNDERLINED))
        .divider(" ")
        .render(tabs_area, buffer);
    let body = Area::new(body.x + 1, body.y + 1, body.width.saturating_sub(2), body.height.saturating_sub(1));
    match state.tab {
        DockTab::Run => {
            draw_run(buffer, body, ctx);
            Selection::None
        }
        DockTab::Step => draw_step(buffer, body, state, ctx),
        DockTab::Items => draw_items(buffer, body, state, ctx),
        DockTab::Outputs => draw_outputs(buffer, body, state, ctx),
        DockTab::Messages => {
            draw_messages(buffer, body, state, ctx);
            Selection::None
        }
        DockTab::Protocol => {
            draw_protocol(buffer, body, state, ctx);
            Selection::None
        }
    }
}

fn draw_run(buffer: &mut Buffer, area: Area, ctx: &DockContext<'_>) {
    let theme = ctx.theme;
    let live = ctx.live;
    let (status, tone) = status_text(live);
    let mut lines: Vec<Line> = vec![
        Line::from(vec![
            Span::styled(ctx.run_label.to_string(), theme.fg(Tone::Ink).add_modifier(Modifier::BOLD)),
            Span::raw("  "),
            Span::styled(format!(" {status} "), theme.fg(tone).patch(theme.soft(tone))),
        ]),
        Line::from(vec![label(theme, "machine  "), value(theme, ctx.machine)]),
    ];
    let elapsed = live.elapsed(ctx.now);
    let inputs = ctx.sources.values().map(Vec::len).sum::<usize>();
    let outputs: usize = live
        .outputs
        .values()
        .map(|o| match o {
            OutputEnd::Produced { paths, .. } => paths.len(),
            OutputEnd::Stdout { .. } => 1,
            OutputEnd::NotProduced => 0,
        })
        .sum();
    lines.push(Line::from(vec![
        label(theme, "elapsed  "),
        value(theme, text::duration(elapsed)),
        label(theme, "   inputs  "),
        value(theme, inputs.to_string()),
        label(theme, " → outputs  "),
        value(theme, outputs.to_string()),
    ]));
    // The progress bar.
    let bar_w = area.width.saturating_sub(8) as usize;
    let filled = ((live.progress.clamp(0.0, 1.0) * bar_w as f32).round() as usize).min(bar_w);
    let (full, empty) = match theme.glyphs {
        crate::tui::theme::GlyphSet::Unicode => ('━', '─'),
        crate::tui::theme::GlyphSet::Ascii => ('#', '-'),
    };
    lines.push(Line::from(vec![
        Span::styled(full.to_string().repeat(filled), theme.fg(tone)),
        Span::styled(empty.to_string().repeat(bar_w - filled), theme.fg(Tone::Border)),
        Span::styled(format!(" {:>3.0}%", live.progress * 100.0), theme.fg(Tone::Secondary)),
    ]));
    if let Some(operation) = &live.operation {
        lines.push(Line::from(vec![label(theme, "now  "), value(theme, operation.clone())]));
    }
    if live.status.is_live() {
        let silent = ctx.now.saturating_duration_since(live.last_signal);
        if silent >= Duration::from_secs(30) {
            lines.push(Line::from(Span::styled(
                format!("No update from the run for {}", text::duration(silent)),
                theme.fg(Tone::Warning),
            )));
        }
    }
    if let RunStatus::Failed(failure) | RunStatus::Cancelled(failure) = &live.status {
        lines.push(Line::raw(""));
        let class_tone = if matches!(live.status, RunStatus::Cancelled(_)) { Tone::Muted } else { Tone::Danger };
        let mut head = vec![Span::styled(
            format!(" {} ", attribution_label(failure.class)),
            theme.fg(class_tone).patch(theme.soft(class_tone)),
        )];
        if let Some(code) = &failure.code {
            head.push(Span::raw(" "));
            head.push(Span::styled(code.clone(), theme.fg(Tone::Secondary)));
        }
        lines.push(Line::from(head));
        lines.push(Line::from(Span::styled(failure.reason.clone(), theme.fg(Tone::Ink))));
        if let Some(arg) = &failure.arg_urn {
            lines.push(Line::from(vec![label(theme, "argument  "), value(theme, arg.clone())]));
        }
        if let Some(token) = &failure.step_token {
            if let Some(node) = ctx.scene.node_of_token(token) {
                lines.push(Line::from(vec![label(theme, "at step  "), value(theme, node.title.clone())]));
            }
        }
    }
    if let Some(stats) = ctx.protocol {
        lines.push(Line::raw(""));
        lines.push(heading(theme, "Protocol"));
        let active = stats
            .requests
            .active
            .iter()
            .filter(|r| ctx.run_rids.contains(&r.rid))
            .count();
        lines.push(Line::from(vec![
            label(theme, "active requests  "),
            value(theme, active.to_string()),
            label(theme, "   registered  "),
            value(theme, stats.requests.total_registered.to_string()),
        ]));
        lines.push(Line::from(vec![
            label(theme, "drops  "),
            Span::styled(
                stats.drops.total.to_string(),
                theme.fg(if stats.drops.total > 0 { Tone::Danger } else { Tone::Ink }),
            ),
            label(theme, "   benign stragglers  "),
            value(theme, stats.stragglers.total.to_string()),
        ]));
    }
    if !ctx.notes.is_empty() {
        lines.push(Line::raw(""));
        lines.push(heading(theme, "Said on stderr"));
        for note in ctx.notes {
            lines.push(Line::from(Span::styled(note.clone(), theme.fg(Tone::Secondary))));
        }
    }
    Paragraph::new(lines).wrap(Wrap { trim: false }).render(area, buffer);
}

/// The arguments of a step the dock lists: every one, wired or not, in the
/// uikit's grouping — the input, the required options, then the options.
fn argument_rows<'a>(ctx: &'a DockContext<'_>, token: &str) -> Vec<&'a StepArgument> {
    let mut rows: Vec<&StepArgument> = ctx.arguments.get(token).map(|a| a.iter().collect()).unwrap_or_default();
    rows.sort_by_key(|a| (!a.is_main_input, !a.is_required, a.name.clone()));
    rows
}

fn draw_step(buffer: &mut Buffer, area: Area, state: &mut DockState, ctx: &DockContext<'_>) -> Selection {
    let theme = ctx.theme;
    let Some(node) = ctx.focus else {
        Paragraph::new(Line::from(Span::styled(
            "Nothing is focused. Move with the arrow keys, or click a node.",
            theme.fg(Tone::Muted),
        )))
        .wrap(Wrap { trim: true })
        .render(area, buffer);
        return Selection::None;
    };
    let mut lines: Vec<Line> = vec![
        Line::from(Span::styled(node.title.clone(), theme.fg(Tone::Ink).add_modifier(Modifier::BOLD))),
        Line::from(Span::styled(node.subtitle.clone(), theme.fg(Tone::Secondary))),
    ];
    if let Some(urn) = &node.full_urn {
        lines.push(Line::from(Span::styled(urn.clone(), theme.fg(Tone::Muted))));
    }
    let selection;
    match node.role {
        NodeRole::Step => {
            let token = node.token.clone().expect("a step card carries its token");
            let step = &ctx.live.steps[&token];
            let phase = match (step.phase, step.queued) {
                (_, q) if q > 0 => format!("queued · {}", step.queue_message.clone().unwrap_or_default()),
                (StepPhase::Pending, _) => "not started".to_string(),
                (StepPhase::Active, _) => format!("running · {:.0}%", step.progress.unwrap_or(0.0) * 100.0),
                (StepPhase::Done, _) => "done".to_string(),
            };
            lines.push(Line::from(vec![label(theme, "state  "), value(theme, phase)]));
            if let Some(meta) = &node.meta {
                lines.push(Line::from(Span::styled(meta.clone(), theme.fg(Tone::Warning))));
            }
            // Why it failed.
            let failed_bodies: Vec<&ItemLive> = ctx
                .live
                .maps
                .values()
                .flat_map(|m| m.items.iter())
                .filter(|i| {
                    i.outcome
                        .as_ref()
                        .is_some_and(|o| !o.success && o.failed_token_id.as_deref() == Some(token.as_str()))
                })
                .collect();
            let own_failure = match &ctx.live.status {
                RunStatus::Failed(f) if f.step_token.as_deref() == Some(token.as_str()) => Some(f),
                _ => None,
            };
            if own_failure.is_some() || !failed_bodies.is_empty() {
                lines.push(Line::raw(""));
                if let Some(failure) = own_failure {
                    let mut head = vec![Span::styled(
                        format!(" {} ", attribution_label(failure.class)),
                        theme.fg(Tone::Danger).patch(theme.soft(Tone::Danger)),
                    )];
                    if let Some(code) = &failure.code {
                        head.push(Span::raw(" "));
                        head.push(Span::styled(code.clone(), theme.fg(Tone::Secondary)));
                    }
                    lines.push(Line::from(head));
                    lines.push(Line::from(Span::styled(failure.reason.clone(), theme.fg(Tone::Ink))));
                }
                if !failed_bodies.is_empty() {
                    lines.push(Line::from(Span::styled(
                        text::count(failed_bodies.len(), "failed body"),
                        theme.fg(Tone::Danger),
                    )));
                    // Reasons, counted.
                    let mut reasons: BTreeMap<String, usize> = BTreeMap::new();
                    for item in &failed_bodies {
                        let reason = item
                            .outcome
                            .as_ref()
                            .and_then(|o| o.error.clone())
                            .unwrap_or_else(|| "no reason given".to_string());
                        *reasons.entry(reason).or_default() += 1;
                    }
                    for (reason, count) in reasons {
                        lines.push(Line::from(vec![
                            Span::styled(format!("{count}× "), theme.fg(Tone::Danger)),
                            Span::styled(reason, theme.fg(Tone::Ink)),
                        ]));
                    }
                }
            }
            // Its arguments.
            let rows = argument_rows(ctx, &token);
            lines.push(Line::raw(""));
            lines.push(heading(theme, "Arguments"));
            let mut selectable: Vec<Selection> = Vec::new();
            for arg in &rows {
                let editable = matches!(arg.origin, ArgOrigin::Default | ArgOrigin::User);
                let pending = state.pending.get(&token).and_then(|p| p.get(&arg.name));
                let shown = match (&state.editing, pending) {
                    (Some(editing), _) if editing.token == token && editing.arg == arg.name => {
                        format!("{}▏", editing.text)
                    }
                    (_, Some((value, _))) => format!("{value} (edited)"),
                    _ => arg.value_text.clone().unwrap_or_else(|| "—".to_string()),
                };
                let selected = selectable.len() == state.selected;
                let style = if selected {
                    theme.fg(Tone::Ink).patch(theme.soft(Tone::Accent))
                } else {
                    theme.fg(Tone::Secondary)
                };
                let group = if arg.is_main_input {
                    "input"
                } else if arg.is_required {
                    "required"
                } else {
                    "option"
                };
                lines.push(Line::from(vec![
                    Span::styled(format!("{} ", if selected { '›' } else { ' ' }), style),
                    Span::styled(text::pad(&arg.name, 16), style.add_modifier(Modifier::BOLD)),
                    Span::styled(format!(" {shown}"), theme.fg(Tone::Ink)),
                    Span::styled(format!("  {} · {group}", arg.origin.label()), theme.fg(Tone::Muted)),
                ]));
                selectable.push(Selection::Argument {
                    token: token.clone(),
                    name: arg.name.clone(),
                    editable,
                });
            }
            if rows.is_empty() {
                lines.push(Line::from(label(theme, "no arguments recorded")));
            }
            if state.pending.get(&token).is_some_and(|p| !p.is_empty()) {
                let offer = if ctx.live.status.is_live() {
                    "a  apply to this run   N  new run with changes   x  discard"
                } else {
                    "N  new run with changes   x  discard"
                };
                lines.push(Line::from(Span::styled(offer, theme.fg(Tone::Accent))));
                if ctx.live.status.is_live() {
                    lines.push(Line::from(Span::styled(
                        "Applying to this run changes only work not yet sent to a cartridge — remaining items and unreached steps.",
                        theme.fg(Tone::Muted),
                    )));
                }
            }
            for verdict in &state.verdicts {
                lines.push(Line::from(Span::styled(verdict.clone(), theme.fg(Tone::Secondary))));
            }
            // Its pools.
            if let Some(chain) = ctx.live.pools.get(&token) {
                lines.push(Line::raw(""));
                lines.push(heading(theme, &format!("Concurrency — {}", chain.cartridge_id)));
                for (pool, pool_state) in &chain.pools {
                    let configured = if pool_state.configured == 0 {
                        "∞".to_string()
                    } else {
                        pool_state.configured.to_string()
                    };
                    lines.push(Line::from(vec![
                        Span::styled(text::pad(pool, 14), theme.fg(Tone::Secondary)),
                        value(theme, format!(" {}/{configured}", pool_state.active)),
                        label(theme, format!("  queued {}", pool_state.queued).as_str()),
                        label(theme, format!("  declared {}", pool_state.declared).as_str()),
                    ]));
                }
            }
            // Its protocol flow.
            if let Some(reading) = ctx.live.flow.get(&token) {
                let (_, chip) = classify_flow(reading);
                let flow = &reading.state;
                lines.push(Line::raw(""));
                lines.push(heading(theme, "Flow"));
                lines.push(Line::from(vec![
                    label(theme, "phase  "),
                    value(theme, flow.phase.clone()),
                    label(theme, "   live  "),
                    value(theme, flow.active_requests.to_string()),
                    label(theme, "   completed  "),
                    value(theme, flow.completed_requests.to_string()),
                ]));
                lines.push(Line::from(vec![
                    label(theme, "age  "),
                    value(theme, text::duration(Duration::from_millis(flow.age_ms))),
                    label(theme, "   idle  "),
                    value(theme, text::duration(Duration::from_millis(flow.idle_ms))),
                    label(theme, "   "),
                    Span::styled(chip, theme.fg(Tone::Accent)),
                ]));
                for stream in &flow.streams {
                    lines.push(Line::from(vec![
                        Span::styled(
                            text::pad(stream.stream_id.as_deref().unwrap_or("·"), 12),
                            theme.fg(Tone::Secondary),
                        ),
                        value(
                            theme,
                            format!(
                                " in {} out {} chunks {}/{} credit {}{}",
                                text::bytes(stream.bytes_in),
                                text::bytes(stream.bytes_out),
                                stream.chunks_in,
                                stream.chunks_out,
                                stream.credit_outstanding,
                                if stream.ended { " ended" } else { "" }
                            ),
                        ),
                    ]));
                }
            }
            // Its messages.
            let messages: Vec<_> = ctx
                .live
                .logs
                .iter()
                .filter(|l| l.step_token.as_deref() == Some(token.as_str()))
                .collect();
            if !messages.is_empty() {
                lines.push(Line::raw(""));
                lines.push(heading(theme, "Messages"));
                for log in messages.iter().rev().take(8).rev() {
                    lines.push(log_line(theme, log));
                }
            }
            selection = selectable.get(state.selected).cloned().unwrap_or(Selection::None);
            if state.selected >= selectable.len() && !selectable.is_empty() {
                state.selected = selectable.len() - 1;
            }
        }
        NodeRole::Input => {
            lines.push(Line::raw(""));
            lines.push(heading(theme, "Sources"));
            let sources = ctx.sources.get(&node.plan_node).map(Vec::as_slice).unwrap_or(&[]);
            for (i, source) in sources.iter().enumerate() {
                let style = if i == state.selected {
                    theme.fg(Tone::Ink).patch(theme.soft(Tone::Accent))
                } else {
                    theme.fg(Tone::Secondary)
                };
                lines.push(Line::from(Span::styled(
                    match source {
                        Source::File(path) => path.display().to_string(),
                        Source::Value(value) => format!("value: {value}"),
                        Source::Live {
                            reference_urn,
                            content_urn,
                        } => format!("LIVE — {reference_urn} → {content_urn}"),
                    },
                    style,
                )));
            }
            selection = match sources.get(state.selected) {
                Some(Source::File(path)) => Selection::File(path.clone()),
                _ => Selection::None,
            };
        }
        NodeRole::Map => {
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                "Its runs are in the Items tab (3).",
                theme.fg(Tone::Muted),
            )));
            selection = Selection::None;
        }
        NodeRole::Output | NodeRole::Value | NodeRole::Gather => {
            lines.push(Line::raw(""));
            match ctx.live.outputs.get(&node.plan_node) {
                Some(OutputEnd::Produced { paths, bytes }) => {
                    lines.push(Line::from(vec![
                        label(theme, "written  "),
                        value(theme, format!("{} · {}", text::count(paths.len(), "file"), text::bytes(*bytes))),
                    ]));
                    lines.push(Line::from(Span::styled("Its files are in the Outputs tab (4).", theme.fg(Tone::Muted))));
                }
                Some(OutputEnd::Stdout { bytes }) => lines.push(Line::from(vec![
                    label(theme, "streamed to stdout  "),
                    value(theme, text::bytes(*bytes)),
                ])),
                Some(OutputEnd::NotProduced) => {
                    lines.push(Line::from(Span::styled("Not produced: the branch that makes it did not fire.", theme.fg(Tone::Warning))))
                }
                None => {
                    if let Some(artifact) = ctx.live.transients.get(&node.plan_node) {
                        lines.push(Line::from(vec![
                            label(theme, "captured  "),
                            value(
                                theme,
                                format!("{} · {}", text::count(artifact.item_count, "item"), text::bytes(artifact.byte_count)),
                            ),
                        ]));
                        lines.push(Line::from(Span::styled(artifact.data_path.display().to_string(), theme.fg(Tone::Muted))));
                    }
                }
            }
            selection = Selection::None;
        }
    }
    let scrolled: Vec<Line> = lines.into_iter().skip(state.scroll).collect();
    Paragraph::new(scrolled).wrap(Wrap { trim: false }).render(area, buffer);
    selection
}

fn log_line(theme: &Theme, log: &crate::tui::live::LogEntry) -> Line<'static> {
    let tone = match log.level.as_str() {
        "error" => Tone::Danger,
        "warn" => Tone::Warning,
        "queued" | "dequeued" => Tone::Muted,
        _ => Tone::Secondary,
    };
    let body = log.body_index.map(|b| format!(" #{}", b + 1)).unwrap_or_default();
    Line::from(vec![
        Span::styled(format!("{:>7} ", text::duration(log.at)), theme.fg(Tone::Muted)),
        Span::styled(format!("{:<6}", log.level), theme.fg(tone)),
        Span::styled(body, theme.fg(Tone::Muted)),
        Span::raw(" "),
        Span::styled(log.message.clone(), theme.fg(Tone::Ink)),
    ])
}

fn item_state(item: &ItemLive, theme: &Theme) -> (String, Style) {
    match &item.outcome {
        None => ("running".to_string(), theme.fg(Tone::Accent)),
        Some(o) if o.success => (format!("done · {}", text::duration(Duration::from_millis(o.duration_ms))), theme.fg(Tone::Success)),
        Some(_) => ("failed".to_string(), theme.fg(Tone::Danger)),
    }
}

fn draw_items(buffer: &mut Buffer, area: Area, state: &mut DockState, ctx: &DockContext<'_>) -> Selection {
    let theme = ctx.theme;
    // The focused map's runs, or every map's when no map is focused.
    let focused_map = ctx
        .focus
        .filter(|n| n.role == NodeRole::Map)
        .and_then(|n| n.token.clone());
    let mut maps: Vec<(&String, &crate::tui::live::MapLive)> = ctx
        .live
        .maps
        .iter()
        .filter(|(token, _)| focused_map.as_ref().is_none_or(|f| f == *token))
        .collect();
    maps.sort_by_key(|(token, _)| ctx.scene.node_of_token(token).map(|n| n.title.clone()));
    let mut items: Vec<(&String, &ItemLive)> = Vec::new();
    for (token, map) in &maps {
        // Failed runs first, then in order.
        let mut runs: Vec<&ItemLive> = map.items.iter().collect();
        runs.sort_by_key(|i| (i.outcome.as_ref().is_none_or(|o| o.success), i.key.clone()));
        for item in runs {
            items.push((token, item));
        }
    }
    if items.is_empty() {
        Paragraph::new(Line::from(Span::styled(
            if ctx.live.maps.is_empty() { "This run has no map." } else { "No items yet." },
            theme.fg(Tone::Muted),
        )))
        .render(area, buffer);
        return Selection::None;
    }
    if state.selected >= items.len() {
        state.selected = items.len() - 1;
    }
    let [list, detail] = Split::vertical([Constraint::Percentage(45), Constraint::Min(3)]).areas(area);
    let rows: Vec<Row> = items
        .iter()
        .enumerate()
        .map(|(i, (token, item))| {
            let (state_text, state_style) = item_state(item, theme);
            let map_title = ctx.scene.node_of_token(token).map(|n| n.title.clone()).unwrap_or_default();
            let preview = item.preview_text.as_deref().unwrap_or("").lines().next().unwrap_or("").to_string();
            let row = Row::new(vec![
                Cell::from(item.key.label()),
                Cell::from(Span::styled(state_text, state_style)),
                Cell::from(text::bytes(item.byte_count)),
                Cell::from(if maps.len() > 1 { map_title } else { preview }),
            ]);
            if i == state.selected {
                row.style(theme.soft(Tone::Accent))
            } else {
                row
            }
        })
        .collect();
    let visible = list.height.saturating_sub(1) as usize;
    let start = state.selected.saturating_sub(visible.saturating_sub(1));
    Table::new(
        rows.into_iter().skip(start),
        [Constraint::Length(10), Constraint::Length(16), Constraint::Length(9), Constraint::Min(8)],
    )
    .header(Row::new(vec!["Run", "State", "Bytes", "Item"]).style(theme.fg(Tone::Muted)))
    .style(theme.fg(Tone::Ink))
    .render(list, buffer);
    let (token, item) = items[state.selected];
    let mut lines = vec![Line::from(Span::styled(item.key.label(), theme.fg(Tone::Ink).add_modifier(Modifier::BOLD)))];
    if let Some(outcome) = &item.outcome {
        if let Some(error) = &outcome.error {
            lines.push(Line::from(Span::styled(error.clone(), theme.fg(Tone::Danger))));
        }
        if let Some(arg) = &outcome.failed_arg_urn {
            lines.push(Line::from(vec![label(theme, "argument  "), value(theme, arg.clone())]));
        }
        for path in &outcome.saved_paths {
            lines.push(Line::from(Span::styled(path.clone(), theme.fg(Tone::Secondary))));
        }
    }
    if let Some(preview) = &item.preview_text {
        lines.push(Line::raw(""));
        for line in preview.lines().skip(state.scroll) {
            lines.push(Line::from(Span::styled(line.to_string(), theme.fg(Tone::Ink))));
        }
    }
    Paragraph::new(lines).wrap(Wrap { trim: false }).render(detail, buffer);
    Selection::Item {
        map_token: token.clone(),
        index: item.key.body_index(),
    }
}

fn draw_outputs(buffer: &mut Buffer, area: Area, state: &mut DockState, ctx: &DockContext<'_>) -> Selection {
    let theme = ctx.theme;
    // Every file of every output, in the plan's output order, and the run's
    // stdout when its one result went there.
    let mut rows: Vec<(String, Selection, MediaKind)> = Vec::new();
    for node in &ctx.view.nodes {
        let PlanViewKind::Output { name, .. } = &node.kind else { continue };
        let kind = node.media.as_ref().map(|m| MediaKind::of(&m.urn)).unwrap_or(MediaKind::Binary);
        match ctx.live.outputs.get(&node.id) {
            Some(OutputEnd::Produced { paths, .. }) => {
                for path in paths {
                    rows.push((format!("{name}  {}", path.display()), Selection::File(path.clone()), kind));
                }
            }
            Some(OutputEnd::Stdout { bytes }) => {
                rows.push((format!("{name}  stdout · {}", text::bytes(*bytes)), Selection::Stdout, kind));
            }
            Some(OutputEnd::NotProduced) => rows.push((format!("{name}  not produced"), Selection::None, kind)),
            None => rows.push((
                format!("{name}  {}", if ctx.live.status.is_live() { "nothing yet" } else { "nothing produced" }),
                Selection::None,
                kind,
            )),
        }
    }
    if rows.is_empty() {
        Paragraph::new(Line::from(Span::styled("This machine has no output.", theme.fg(Tone::Muted)))).render(area, buffer);
        return Selection::None;
    }
    if state.selected >= rows.len() {
        state.selected = rows.len() - 1;
    }
    let list_h = (rows.len() as u16 + 1).min(area.height / 3).max(2);
    let [list, detail] = Split::vertical([Constraint::Length(list_h), Constraint::Min(2)]).areas(area);
    let start = state.selected.saturating_sub(list.height as usize - 1);
    for (i, (label_text, _, _)) in rows.iter().enumerate().skip(start).take(list.height as usize) {
        let style = if i == state.selected {
            theme.fg(Tone::Ink).patch(theme.soft(Tone::Accent))
        } else {
            theme.fg(Tone::Secondary)
        };
        buffer.set_string(list.x, list.y + (i - start) as u16, text::pad(label_text, list.width as usize), style);
    }
    let (_, selection, kind) = rows[state.selected].clone();
    let detail = Area::new(detail.x, detail.y + 1, detail.width, detail.height.saturating_sub(1));
    match &selection {
        Selection::File(path) => {
            let key = format!("{}@{}x{}", path.display(), detail.width, detail.height);
            let (w, h) = (detail.width, detail.height);
            let loaded = state.preview_of(key, || preview::of_file(path, kind, w, h)).clone();
            preview::draw(buffer, detail, &loaded, state.scroll, theme);
            buffer.set_string(
                detail.x,
                detail.y.saturating_sub(1),
                text::truncate("o open · R reveal · y copy path · PgUp/PgDn scroll", detail.width as usize),
                theme.fg(Tone::Muted),
            );
        }
        Selection::Stdout => {
            let loaded = preview::of_bytes(ctx.stdout, kind, ctx.stdout.len() as u64);
            preview::draw(buffer, detail, &loaded, state.scroll, theme);
        }
        _ => {}
    }
    selection
}

fn draw_messages(buffer: &mut Buffer, area: Area, state: &mut DockState, ctx: &DockContext<'_>) {
    let theme = ctx.theme;
    let logs: Vec<&crate::tui::live::LogEntry> = ctx.live.logs.iter().filter(|l| state.levels.admits(&l.level)).collect();
    buffer.set_string(
        area.x,
        area.y,
        text::truncate(
            &format!(
                "{} of {} · showing {} · l level · F {}",
                logs.len(),
                ctx.live.logs.len(),
                state.levels.label(),
                if state.follow { "following" } else { "follow" }
            ),
            area.width as usize,
        ),
        theme.fg(Tone::Muted),
    );
    let room = area.height.saturating_sub(1) as usize;
    let max_scroll = logs.len().saturating_sub(room);
    if state.follow {
        state.scroll = max_scroll;
    }
    state.scroll = state.scroll.min(max_scroll);
    for (row, log) in logs.iter().skip(state.scroll).take(room).enumerate() {
        let step = log
            .step_token
            .as_deref()
            .and_then(|t| ctx.scene.node_of_token(t))
            .map(|n| format!("{} · ", n.title))
            .unwrap_or_default();
        let mut line = log_line(theme, log);
        line.spans.insert(3, Span::styled(step, theme.fg(Tone::Secondary)));
        let area_row = Area::new(area.x, area.y + 1 + row as u16, area.width, 1);
        Paragraph::new(line).render(area_row, buffer);
    }
}

fn draw_protocol(buffer: &mut Buffer, area: Area, state: &mut DockState, ctx: &DockContext<'_>) {
    let theme = ctx.theme;
    let Some(stats) = ctx.protocol else {
        Paragraph::new(Line::from(Span::styled(
            "No protocol reading yet: nothing has been hosted.",
            theme.fg(Tone::Muted),
        )))
        .render(area, buffer);
        return;
    };
    let mut lines: Vec<Line> = Vec::new();
    lines.push(heading(theme, "Active requests"));
    let mut active: Vec<_> = stats
        .requests
        .active
        .iter()
        .filter(|r| ctx.run_rids.contains(&r.rid))
        .collect();
    active.sort_by_key(|r| std::cmp::Reverse(r.age_ms));
    for request in active.iter().take(50) {
        let bytes: u64 = request.streams.iter().map(|s| s.stats.bytes_in + s.stats.bytes_out).sum();
        lines.push(Line::from(vec![
            Span::styled(text::truncate(&request.rid, 10), theme.fg(Tone::Muted)),
            Span::raw(" "),
            value(theme, format!("{:?}", request.phase).to_lowercase()),
            label(theme, "  age "),
            value(theme, text::duration(Duration::from_millis(request.age_ms))),
            label(theme, "  idle "),
            value(theme, text::duration(Duration::from_millis(request.idle_ms))),
            label(theme, "  streams "),
            value(theme, request.streams.len().to_string()),
            label(theme, "  "),
            value(theme, text::bytes(bytes)),
            Span::raw(if request.is_peer { "  peer" } else { "" }),
        ]));
        if let Some(cap) = &request.cap_urn {
            lines.push(Line::from(Span::styled(format!("  {cap}"), theme.fg(Tone::Muted))));
        }
    }
    if active.is_empty() {
        lines.push(Line::from(label(theme, "none")));
    }
    lines.push(Line::raw(""));
    lines.push(heading(theme, "Recently terminated"));
    let terminated: Vec<_> = stats
        .requests
        .recent_terminated
        .iter()
        .filter(|t| ctx.run_rids.contains(&t.rid))
        .collect();
    for t in terminated.iter().rev().take(20) {
        let tone = match t.kind {
            crate::bifaci::request_state::TerminalKind::End => Tone::Success,
            crate::bifaci::request_state::TerminalKind::Cancelled => Tone::Muted,
            _ => Tone::Danger,
        };
        let mut spans = vec![
            Span::styled(text::truncate(&t.rid, 10), theme.fg(Tone::Muted)),
            Span::raw(" "),
            Span::styled(t.kind.as_str().to_string(), theme.fg(tone)),
            label(theme, "  lived "),
            value(theme, text::duration(Duration::from_millis(t.lifetime_ms))),
            label(theme, "  "),
            value(theme, text::bytes(t.bytes_in + t.bytes_out)),
        ];
        if let Some(code) = &t.cancel_code {
            spans.push(label(theme, "  "));
            spans.push(Span::styled(code.clone(), theme.fg(Tone::Warning)));
        }
        lines.push(Line::from(spans));
    }
    if terminated.is_empty() {
        lines.push(Line::from(label(theme, "none")));
    }
    lines.push(Line::raw(""));
    lines.push(heading(theme, "Drops by reason"));
    if stats.drops.by_reason.is_empty() {
        lines.push(Line::from(label(theme, "none")));
    }
    for (reason, count) in &stats.drops.by_reason {
        lines.push(Line::from(vec![Span::styled(reason.clone(), theme.fg(Tone::Danger)), label(theme, "  "), value(theme, count.to_string())]));
    }
    lines.push(Line::raw(""));
    lines.push(heading(theme, "Benign post-terminal stragglers"));
    lines.push(Line::from(Span::styled(
        "Expected teardown crossings — frames that raced a request's end. Nothing went wrong.",
        theme.fg(Tone::Muted),
    )));
    for (frame, count) in &stats.stragglers.by_frame_type {
        lines.push(Line::from(vec![Span::styled(frame.clone(), theme.fg(Tone::Secondary)), label(theme, "  "), value(theme, count.to_string())]));
    }
    lines.push(Line::raw(""));
    lines.push(heading(theme, "Cartridge hosts"));
    if stats.hosts.is_empty() {
        lines.push(Line::from(label(theme, "none reported")));
    }
    for (master, host) in &stats.hosts {
        lines.push(Line::from(vec![
            Span::styled(master.clone(), theme.fg(Tone::Secondary)),
            label(theme, "  out "),
            value(theme, host.outgoing_rids.to_string()),
            label(theme, "  in "),
            value(theme, host.incoming_rxids.to_string()),
            label(theme, "  to peers "),
            value(theme, host.incoming_to_peer_rids.to_string()),
            label(theme, "  drops "),
            Span::styled(
                host.drops.total.to_string(),
                theme.fg(if host.drops.total > 0 { Tone::Danger } else { Tone::Ink }),
            ),
            label(theme, "  stragglers "),
            value(theme, host.stragglers.total.to_string()),
        ]));
    }
    let scrolled: Vec<Line> = lines.into_iter().skip(state.scroll).collect();
    Paragraph::new(scrolled).wrap(Wrap { trim: false }).render(area, buffer);
}
