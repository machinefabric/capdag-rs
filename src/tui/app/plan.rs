//! The planner: what a set of files can become, the machines that make it,
//! and a picture of each before anything runs.
//!
//! Two lists, as the desktop's planner has them: the reachable targets, then
//! the candidate machines for the chosen one. Candidates arrive as the
//! planner finds them and are re-ordered by rank when it finishes. The
//! highlighted candidate is resolved and drawn beside the list exactly as
//! the run view will draw it. Enter runs it; `s` saves its notation; `y`
//! copies it.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use futures_util::StreamExt;
use ratatui::layout::{Constraint, Layout as Split, Margin, Rect as Area};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Widget, Wrap};
use ratatui::Frame;
use tokio::sync::mpsc;

use crate::orchestrator::ResolvedNotation;
use crate::planner::{LiveCapFab, PlanApex, PlanCandidate, PlanError, PlanRequest};
use crate::tui::app::graph::GraphView;
use crate::tui::app::{copy_to_clipboard, too_small, Display, Tui, TuiError, TuiSettings};
use crate::tui::scene::{planned_scenes, Scene};
use crate::tui::text;
use crate::tui::theme::{Theme, Tone};
use crate::{FabricRegistry, MediaUrn};

/// A target the files can become.
#[derive(Debug, Clone)]
pub struct PlannerTarget {
    pub media: MediaUrn,
    pub title: String,
    /// Where the files combine on the way, when they do.
    pub apex: Option<PlanApex>,
    /// The fewest cap steps to it.
    pub steps: i32,
}

/// What the planner is given.
pub struct PlannerSpec {
    /// The files, and what each group of them is: the top bar's summary.
    pub files: Vec<PathBuf>,
    pub sources: Vec<(MediaUrn, usize)>,
    pub targets: Vec<PlannerTarget>,
    /// The target to plan for at once (`--to`), by index into `targets`.
    pub chosen: Option<usize>,
    /// Sources that reach no target: said, never hidden.
    pub dead_ends: Vec<MediaUrn>,
    pub fab: Arc<LiveCapFab>,
    pub registry: Arc<FabricRegistry>,
    /// The request for a target, with the command line's planning options.
    pub request_for: Box<dyn Fn(MediaUrn) -> PlanRequest + Send + Sync>,
    /// Where `s` saves, unless another path is typed.
    pub save_to: Option<PathBuf>,
}

/// How the planner ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannerOutcome {
    Closed,
    /// Run this candidate on the files.
    Run { notation: String, rank: usize },
}

/// A planning task's report: a candidate as found, or the ranked end.
enum Found {
    Candidate(u64, PlanCandidate),
    Done(u64, Result<Vec<PlanCandidate>, PlanError>),
}

/// A preview's verdict: the pictures of a candidate's strands, or why not.
struct Preview(u64, String, Result<Vec<Scene>, String>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Targets,
    Candidates,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Focus {
    List,
    Picture,
}

struct Planner {
    spec: PlannerSpec,
    settings: TuiSettings,
    phase: Phase,
    focus: Focus,
    target_selected: usize,
    /// The target being planned for.
    target: Option<usize>,
    /// Bumped whenever planning restarts: reports of older plans are dropped.
    generation: u64,
    candidates: Vec<PlanCandidate>,
    /// The planner finished, ranked.
    ranked: bool,
    plan_error: Option<String>,
    selected: usize,
    /// Pictures by notation; a candidate's is computed when it is selected.
    previews: HashMap<String, Result<Vec<Scene>, String>>,
    previewing: Option<String>,
    strand: usize,
    graph: GraphView,
    save_prompt: Option<String>,
    help: bool,
    toasts: Vec<(String, Tone, Instant)>,
    quit: Option<PlannerOutcome>,
    found: mpsc::UnboundedSender<Found>,
    pictures: mpsc::UnboundedSender<Preview>,
    preview_generation: u64,
}

const TOAST_FOR: Duration = Duration::from_secs(6);

const KEYS: &[&str] = &[
    "Lists    ↑↓ jk select · enter choose · esc back",
    "Picture  tab focus it · ←↑↓→ move · +/- detail · f fit · w wrap · t next strand",
    "Machine  enter run it · s save its notation · y copy it",
    "         q quit · ? these keys",
];

impl Planner {
    fn toast(&mut self, text: impl Into<String>, tone: Tone) {
        self.toasts.push((text.into(), tone, Instant::now()));
    }

    /// Plan for `target`: candidates stream in as the planner finds them.
    fn plan(&mut self, target: usize) {
        self.generation += 1;
        let generation = self.generation;
        self.phase = Phase::Candidates;
        self.focus = Focus::List;
        self.target = Some(target);
        self.candidates.clear();
        self.ranked = false;
        self.plan_error = None;
        self.selected = 0;
        self.strand = 0;
        let request = (self.spec.request_for)(self.spec.targets[target].media.clone());
        let fab = self.spec.fab.clone();
        let registry = self.spec.registry.clone();
        let found = self.found.clone();
        // Planning is a search on this thread's time: off the interface's.
        tokio::task::spawn_blocking(move || {
            let streamed = found.clone();
            let outcome = fab.plan_with_observer(&request, &registry, |candidate| {
                let _ = streamed.send(Found::Candidate(generation, candidate.clone()));
            });
            let _ = found.send(Found::Done(generation, outcome.map(|o| o.candidates)));
        });
    }

    fn on_found(&mut self, found: Found) {
        match found {
            Found::Candidate(generation, candidate) if generation == self.generation && !self.ranked => {
                self.candidates.push(candidate);
            }
            Found::Done(generation, result) if generation == self.generation => {
                let kept = self.candidates.get(self.selected).map(|c| c.notation.clone());
                self.ranked = true;
                match result {
                    Ok(ranked) => {
                        self.candidates = ranked;
                        // The highlighted machine stays highlighted as the list
                        // re-orders.
                        self.selected = kept
                            .and_then(|n| self.candidates.iter().position(|c| c.notation == n))
                            .unwrap_or(0);
                    }
                    Err(e) => {
                        self.candidates.clear();
                        self.plan_error = Some(e.to_string());
                    }
                }
            }
            // A report for a plan no longer shown.
            _ => {}
        }
        self.request_preview();
    }

    fn current(&self) -> Option<&PlanCandidate> {
        match self.phase {
            Phase::Candidates => self.candidates.get(self.selected),
            Phase::Targets => None,
        }
    }

    /// Resolve and draw the highlighted candidate, unless it is already.
    fn request_preview(&mut self) {
        let Some(notation) = self.current().map(|c| c.notation.clone()) else {
            return;
        };
        if self.previews.contains_key(&notation) || self.previewing.as_ref() == Some(&notation) {
            return;
        }
        self.preview_generation += 1;
        let generation = self.preview_generation;
        self.previewing = Some(notation.clone());
        let registry = self.spec.registry.clone();
        let pictures = self.pictures.clone();
        tokio::spawn(async move {
            let result = match ResolvedNotation::resolve(&notation, registry.clone()).await {
                Ok(resolved) => planned_scenes(&resolved, &registry).await,
                Err(e) => Err(e.to_string()),
            };
            let _ = pictures.send(Preview(generation, notation, result));
        });
    }

    fn on_preview(&mut self, preview: Preview) {
        let Preview(generation, notation, result) = preview;
        // Every picture is kept: it is the same picture whenever its
        // candidate is highlighted again.
        self.previews.insert(notation, result);
        if generation == self.preview_generation {
            self.previewing = None;
        }
        self.request_preview();
    }

    fn select(&mut self, delta: i32) {
        let (selected, len) = match self.phase {
            Phase::Targets => (&mut self.target_selected, self.spec.targets.len()),
            Phase::Candidates => (&mut self.selected, self.candidates.len()),
        };
        if len == 0 {
            return;
        }
        let next = (*selected as i32 + delta).clamp(0, len as i32 - 1) as usize;
        if next != *selected {
            *selected = next;
            self.strand = 0;
            self.graph = GraphView::default();
        }
        self.request_preview();
    }

    fn save(&mut self, path: &str) {
        let Some(candidate) = self.current() else {
            return;
        };
        let notation = candidate.notation.clone();
        let rank = candidate.rank;
        let path = PathBuf::from(path);
        let Some(file_name) = path.file_name() else {
            self.toast(format!("'{}' names no file.", path.display()), Tone::Danger);
            return;
        };
        let mut staged_name = std::ffi::OsString::from(".");
        staged_name.push(file_name);
        staged_name.push(".saving");
        let staged = path.with_file_name(staged_name);
        match std::fs::write(&staged, &notation).and_then(|_| std::fs::rename(&staged, &path)) {
            Ok(()) => self.toast(format!("Saved candidate [{rank}] to {}", path.display()), Tone::Success),
            Err(e) => {
                let _ = std::fs::remove_file(&staged);
                self.toast(format!("Saving {} failed: {e}", path.display()), Tone::Danger);
            }
        }
    }

    fn on_key(&mut self, key: KeyEvent) {
        if key.kind == KeyEventKind::Release {
            return;
        }
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        if ctrl && key.code == KeyCode::Char('c') {
            self.quit = Some(PlannerOutcome::Closed);
            return;
        }
        if self.help {
            self.help = false;
            return;
        }
        if let Some(typed) = &mut self.save_prompt {
            match key.code {
                KeyCode::Esc => self.save_prompt = None,
                KeyCode::Backspace => {
                    typed.pop();
                }
                KeyCode::Char(c) if !ctrl => typed.push(c),
                KeyCode::Enter => {
                    let path = typed.trim().to_string();
                    self.save_prompt = None;
                    if path.is_empty() {
                        self.toast("Saving needs a file name.", Tone::Warning);
                    } else {
                        self.save(&path);
                    }
                }
                _ => {}
            }
            return;
        }
        match key.code {
            KeyCode::Char('q') => {
                self.quit = Some(PlannerOutcome::Closed);
                return;
            }
            KeyCode::Char('?') => {
                self.help = true;
                return;
            }
            KeyCode::Tab | KeyCode::BackTab if self.phase == Phase::Candidates => {
                self.focus = if self.focus == Focus::List { Focus::Picture } else { Focus::List };
                return;
            }
            KeyCode::Esc => {
                if self.focus == Focus::Picture {
                    self.focus = Focus::List;
                } else if self.phase == Phase::Candidates {
                    // Back to the targets: what was planned is let go.
                    self.generation += 1;
                    self.phase = Phase::Targets;
                    self.target = None;
                } else {
                    self.quit = Some(PlannerOutcome::Closed);
                }
                return;
            }
            KeyCode::Char('t') if self.phase == Phase::Candidates => {
                if let Some(Ok(scenes)) = self.current().and_then(|c| self.previews.get(&c.notation)) {
                    if !scenes.is_empty() {
                        self.strand = (self.strand + 1) % scenes.len();
                        self.graph = GraphView::default();
                    }
                }
                return;
            }
            KeyCode::Char('s') if self.phase == Phase::Candidates => {
                if self.current().is_some() {
                    let default = self
                        .spec
                        .save_to
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_default();
                    self.save_prompt = Some(default);
                }
                return;
            }
            KeyCode::Char('y') if self.phase == Phase::Candidates => {
                if let Some(notation) = self.current().map(|c| c.notation.clone()) {
                    match copy_to_clipboard(&notation) {
                        Ok(()) => self.toast("Copied the machine's notation.", Tone::Success),
                        Err(e) => self.toast(format!("Copying failed: {e}"), Tone::Danger),
                    }
                }
                return;
            }
            _ => {}
        }
        if self.focus == Focus::Picture {
            self.graph.on_key(key);
            return;
        }
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => self.select(-1),
            KeyCode::Down | KeyCode::Char('j') => self.select(1),
            KeyCode::PageUp => self.select(-10),
            KeyCode::PageDown => self.select(10),
            KeyCode::Home => self.select(i32::MIN / 2),
            KeyCode::End => self.select(i32::MAX / 2),
            KeyCode::Enter => match self.phase {
                Phase::Targets => {
                    if !self.spec.targets.is_empty() {
                        self.plan(self.target_selected);
                    }
                }
                Phase::Candidates => {
                    if let Some(candidate) = self.current() {
                        self.quit = Some(PlannerOutcome::Run {
                            notation: candidate.notation.clone(),
                            rank: candidate.rank,
                        });
                    }
                }
            },
            _ => {}
        }
    }

    fn draw(&mut self, frame: &mut Frame) -> Result<(), TuiError> {
        let theme = self.settings.theme();
        let area = frame.area();
        frame.buffer_mut().set_style(area, theme.canvas());
        if too_small(frame, &theme) {
            return Ok(());
        }
        let [top, body, footer] =
            Split::vertical([Constraint::Length(1), Constraint::Min(1), Constraint::Length(1)]).areas(area);
        self.draw_top(frame, top, &theme);
        let wide = body.width >= 110;
        let [list, picture] = if wide {
            Split::horizontal([Constraint::Percentage(38), Constraint::Percentage(62)]).areas(body)
        } else {
            Split::vertical([Constraint::Percentage(40), Constraint::Percentage(60)]).areas(body)
        };
        let block = Block::default()
            .borders(if wide { Borders::RIGHT } else { Borders::BOTTOM })
            .border_style(theme.rule())
            .style(theme.chrome());
        let inner = block.inner(list);
        block.render(list, frame.buffer_mut());
        match self.phase {
            Phase::Targets => self.draw_targets(frame, inner, &theme),
            Phase::Candidates => self.draw_candidates(frame, inner, &theme),
        }
        self.draw_picture(frame, picture, &theme)?;
        self.draw_footer(frame, footer, &theme);
        if let Some(typed) = &self.save_prompt {
            let width = body.width.saturating_sub(2) as usize;
            let line = format!(" Save the notation to: {typed}▏");
            frame.buffer_mut().set_string(
                body.x + 1,
                body.y + body.height / 2,
                text::pad(&text::truncate(&line, width), width),
                theme.soft(Tone::Accent).patch(theme.fg(Tone::Ink)).add_modifier(Modifier::BOLD),
            );
        }
        if self.help {
            let w = (KEYS.iter().map(|l| text::width(l)).max().unwrap_or(0) as u16 + 4).min(body.width);
            let h = (KEYS.len() as u16 + 2).min(body.height);
            let card = Area::new(body.x + (body.width - w) / 2, body.y + (body.height - h) / 2, w, h);
            Clear.render(card, frame.buffer_mut());
            let block = Block::bordered().border_style(theme.fg(Tone::Border)).style(theme.chrome());
            let inner = block.inner(card);
            block.render(card, frame.buffer_mut());
            let lines: Vec<Line> = KEYS.iter().map(|l| Line::raw(format!(" {l}"))).collect();
            Paragraph::new(lines).render(inner, frame.buffer_mut());
        }
        Ok(())
    }

    fn draw_top(&self, frame: &mut Frame, top: Area, theme: &Theme) {
        let buffer = frame.buffer_mut();
        buffer.set_style(top, theme.chrome());
        let files = match self.spec.files.len() {
            1 => self.spec.files[0].display().to_string(),
            n => format!("{n} files"),
        };
        let target = self
            .target
            .map(|t| format!(" → {}", self.spec.targets[t].title))
            .unwrap_or_default();
        let title = format!(" capdag plan ▸ {files}{target}");
        let state = match (self.phase, self.ranked, &self.plan_error) {
            (Phase::Targets, _, _) => format!(" {} targets ", self.spec.targets.len()),
            (Phase::Candidates, _, Some(_)) => " no machine ".to_string(),
            (Phase::Candidates, false, None) => format!(" planning… {} found ", self.candidates.len()),
            (Phase::Candidates, true, None) => format!(" {} machines ", self.candidates.len()),
        };
        let tone = match (self.phase, self.ranked, &self.plan_error) {
            (Phase::Candidates, _, Some(_)) => Tone::Danger,
            (Phase::Candidates, false, None) => Tone::Accent,
            _ => Tone::Secondary,
        };
        let state_width = text::width(&state) as u16;
        buffer.set_string(
            top.x,
            top.y,
            text::truncate_middle(&title, top.width.saturating_sub(state_width + 1) as usize),
            theme.chrome().patch(theme.fg(Tone::Ink)).add_modifier(Modifier::BOLD),
        );
        buffer.set_string(
            top.x + top.width.saturating_sub(state_width),
            top.y,
            state,
            theme.soft(tone).patch(theme.fg(tone)),
        );
    }

    /// A list of rows, scrolled so the selected one shows.
    fn draw_rows(frame: &mut Frame, area: Area, theme: &Theme, rows: Vec<(Vec<Line<'static>>, bool)>, header: Vec<Line<'static>>) {
        let area = area.inner(Margin::new(1, 0));
        let mut y = area.y;
        for line in header {
            if y >= area.y + area.height {
                return;
            }
            frame.buffer_mut().set_line(area.x, y, &line, area.width);
            y += 1;
        }
        if rows.is_empty() {
            return;
        }
        let room = (area.y + area.height).saturating_sub(y) as usize;
        let selected = rows.iter().position(|(_, s)| *s).unwrap_or(0);
        // Rows before the selected one, as many as leave it in view.
        let mut first = 0;
        let mut used: usize = rows[..=selected]
            .iter()
            .map(|(l, _)| l.len())
            .sum();
        while used > room && first < selected {
            used -= rows[first].0.len();
            first += 1;
        }
        for (lines, is_selected) in rows.into_iter().skip(first) {
            for line in lines {
                if y >= area.y + area.height {
                    return;
                }
                if is_selected {
                    frame
                        .buffer_mut()
                        .set_style(Area::new(area.x, y, area.width, 1), theme.soft(Tone::Accent));
                }
                frame.buffer_mut().set_line(area.x, y, &line, area.width);
                y += 1;
            }
        }
    }

    fn draw_targets(&self, frame: &mut Frame, area: Area, theme: &Theme) {
        let mut header = vec![Line::from(Span::styled(
            "What these files can become",
            theme.fg(Tone::Ink).add_modifier(Modifier::BOLD),
        ))];
        for (media, count) in &self.spec.sources {
            let title = self
                .spec
                .registry
                .get_cached_media_def(&media.to_string())
                .map(|d| d.title)
                .unwrap_or_else(|| media.to_string());
            header.push(Line::from(Span::styled(format!("  {title} × {count}"), theme.fg(Tone::Source))));
        }
        for dead in &self.spec.dead_ends {
            header.push(Line::from(Span::styled(
                format!("  {dead} reaches nothing: it is left as it is"),
                theme.fg(Tone::Warning),
            )));
        }
        header.push(Line::raw(""));
        if self.spec.targets.is_empty() {
            header.push(Line::from(Span::styled("Nothing is reachable from these files.", theme.fg(Tone::Danger))));
        }
        let rows = self
            .spec
            .targets
            .iter()
            .enumerate()
            .map(|(k, t)| {
                let how = match &t.apex {
                    Some(apex) => format!("combine all, via {} · ~{} steps", apex.media_urn, t.steps),
                    None => format!("convert each · ~{} steps", t.steps),
                };
                (
                    vec![
                        Line::from(Span::styled(t.title.clone(), theme.fg(Tone::Target).add_modifier(Modifier::BOLD))),
                        Line::from(Span::styled(format!("  {how}"), theme.fg(Tone::Muted))),
                    ],
                    k == self.target_selected,
                )
            })
            .collect();
        Self::draw_rows(frame, area, theme, rows, header);
    }

    fn draw_candidates(&self, frame: &mut Frame, area: Area, theme: &Theme) {
        let mut header = Vec::new();
        if let Some(error) = &self.plan_error {
            header.push(Line::from(Span::styled("No machine makes this", theme.fg(Tone::Danger).add_modifier(Modifier::BOLD))));
            header.push(Line::from(Span::styled(error.clone(), theme.fg(Tone::Ink))));
        } else if self.candidates.is_empty() {
            header.push(Line::from(Span::styled(
                if self.ranked { "No machine makes this." } else { "Planning…" },
                theme.fg(Tone::Muted),
            )));
        }
        let rows = self
            .candidates
            .iter()
            .enumerate()
            .map(|(k, c)| {
                let mut shape = vec![format!("{} steps", c.cost.cap_steps)];
                if c.profile.converged {
                    shape.push("combined result".to_string());
                }
                if c.profile.diverged {
                    shape.push("fan-out".to_string());
                }
                let rank = if self.ranked { format!("[{}] ", c.rank) } else { String::new() };
                (
                    vec![
                        Line::from(vec![
                            Span::styled(rank, theme.fg(Tone::Muted)),
                            Span::styled(c.label.clone(), theme.fg(Tone::Ink).add_modifier(Modifier::BOLD)),
                        ]),
                        Line::from(Span::styled(format!("  {}", shape.join(" · ")), theme.fg(Tone::Secondary))),
                    ],
                    k == self.selected,
                )
            })
            .collect();
        Self::draw_rows(frame, area, theme, rows, header);
    }

    fn draw_picture(&mut self, frame: &mut Frame, area: Area, theme: &Theme) -> Result<(), TuiError> {
        let note = |frame: &mut Frame, message: String, tone: Tone| {
            Paragraph::new(message)
                .wrap(Wrap { trim: true })
                .style(theme.fg(tone))
                .render(area.inner(Margin::new(2, 1)), frame.buffer_mut());
        };
        let Some(candidate) = self.current() else {
            let message = match self.phase {
                Phase::Targets => "Choose a target: its machines are planned and drawn here.",
                Phase::Candidates => "",
            };
            note(frame, message.to_string(), Tone::Muted);
            return Ok(());
        };
        match self.previews.get(&candidate.notation) {
            None => note(frame, "Resolving the machine…".to_string(), Tone::Muted),
            Some(Err(why)) => note(frame, format!("This machine cannot be drawn: {why}"), Tone::Danger),
            Some(Ok(scenes)) => {
                let Some(scene) = scenes.get(self.strand) else {
                    return Err(TuiError::Inconsistent(format!(
                        "strand {} of a machine of {}",
                        self.strand + 1,
                        scenes.len()
                    )));
                };
                let mut graph_area = area;
                if scenes.len() > 1 {
                    let label = format!(" strand {} of {} · t next ", self.strand + 1, scenes.len());
                    frame.buffer_mut().set_string(area.x + 1, area.y, label, theme.fg(Tone::Secondary));
                    graph_area = Area::new(area.x, area.y + 1, area.width, area.height.saturating_sub(1));
                }
                let scene = scene.clone();
                self.graph
                    .draw(frame.buffer_mut(), graph_area, &scene, theme, 0, 0)
                    .map_err(|e| TuiError::Inconsistent(format!("the machine cannot be laid out: {e}")))?;
                if self.focus == Focus::Picture {
                    let mark = " picture · esc list ";
                    let w = text::width(mark) as u16;
                    frame.buffer_mut().set_string(
                        area.x + area.width.saturating_sub(w + 1),
                        area.y,
                        mark,
                        theme.soft(Tone::Accent).patch(theme.fg(Tone::Ink)),
                    );
                }
            }
        }
        Ok(())
    }

    fn draw_footer(&mut self, frame: &mut Frame, footer: Area, theme: &Theme) {
        let now = Instant::now();
        self.toasts.retain(|(_, _, at)| now.duration_since(*at) < TOAST_FOR);
        let (line, tone) = match self.toasts.last() {
            Some((t, tone, _)) => (t.clone(), *tone),
            None => (
                match (self.phase, self.focus) {
                    (Phase::Targets, _) => "↑↓ choose · enter plan · ? keys · q quit",
                    (Phase::Candidates, Focus::List) => "↑↓ choose · enter run · s save · y copy · tab picture · esc targets · ? keys · q quit",
                    (Phase::Candidates, Focus::Picture) => "←↑↓→ move · +/- detail · f fit · t strand · esc list",
                }
                .to_string(),
                Tone::Muted,
            ),
        };
        let buffer = frame.buffer_mut();
        buffer.set_style(footer, theme.chrome());
        buffer.set_string(
            footer.x + 1,
            footer.y,
            text::truncate(&line, footer.width.saturating_sub(2) as usize),
            theme.chrome().patch(theme.fg(tone)),
        );
    }
}

/// Show the planner until the user closes it or chooses a machine to run.
pub async fn plan_session(spec: PlannerSpec, settings: TuiSettings) -> Result<PlannerOutcome, TuiError> {
    let mut tui = Tui::enter()?;
    plan_session_on(&mut tui, EventStream::new(), spec, settings).await
}

/// [`plan_session`] on any display, reading events from `input`.
pub async fn plan_session_on<D, S>(
    display: &mut D,
    mut input: S,
    spec: PlannerSpec,
    settings: TuiSettings,
) -> Result<PlannerOutcome, TuiError>
where
    D: Display,
    S: futures_util::Stream<Item = std::io::Result<Event>> + Unpin,
{
    if let Some(chosen) = spec.chosen {
        if chosen >= spec.targets.len() {
            return Err(TuiError::Inconsistent(format!(
                "target {chosen} was chosen of {} targets",
                spec.targets.len()
            )));
        }
    }
    let (found_tx, mut found_rx) = mpsc::unbounded_channel();
    let (pictures_tx, mut pictures_rx) = mpsc::unbounded_channel();
    let chosen = spec.chosen;
    let mut planner = Planner {
        settings,
        phase: Phase::Targets,
        focus: Focus::List,
        target_selected: chosen.unwrap_or(0),
        target: None,
        generation: 0,
        candidates: Vec::new(),
        ranked: false,
        plan_error: None,
        selected: 0,
        previews: HashMap::new(),
        previewing: None,
        strand: 0,
        graph: GraphView::default(),
        save_prompt: None,
        help: false,
        toasts: Vec::new(),
        quit: None,
        found: found_tx,
        pictures: pictures_tx,
        preview_generation: 0,
        spec,
    };
    if let Some(chosen) = chosen {
        planner.plan(chosen);
    }
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let mut drawn = Ok(());
        display.draw(|frame| drawn = planner.draw(frame))?;
        drawn?;
        if let Some(outcome) = planner.quit.take() {
            return Ok(outcome);
        }
        tokio::select! {
            event = input.next() => match event {
                Some(Ok(Event::Key(key))) => planner.on_key(key),
                Some(Ok(Event::Mouse(mouse))) => {
                    planner.graph.on_mouse(mouse);
                }
                Some(Ok(Event::Paste(value))) => {
                    if let Some(typed) = &mut planner.save_prompt {
                        typed.push_str(value.trim());
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e.into()),
                None => return Ok(PlannerOutcome::Closed),
            },
            Some(found) = found_rx.recv() => planner.on_found(found),
            Some(preview) = pictures_rx.recv() => planner.on_preview(preview),
            _ = tick.tick(), if !planner.toasts.is_empty() => {}
        }
    }
}
