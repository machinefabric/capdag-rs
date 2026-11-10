//! Showing runs as they go: inline on a terminal, as lines on a pipe, or as
//! JSON events.
//!
//! - **Live** (`--progress auto` on a terminal, or `live`): a compact block
//!   drawn under the command in the normal scrollback — never the alternate
//!   screen — with each step's state and progress, a map's items, and the
//!   last thing the selected step's cartridge said. Keys: Ctrl-C stops,
//!   Ctrl-C again aborts, `l` shows more of the log, ↑ ↓ select a step.
//!   When the run ends the block is replaced by its summary, so the
//!   scrollback keeps what happened and nothing else.
//! - **Plain** (`auto` on a pipe, or `plain`): one line when a step starts,
//!   ends or fails, and its progress at most once a second.
//! - **JSON** (`--json`): every change as a record on stdout.
//! - **None** (`--progress none`): only the summary.

use std::collections::HashMap;
use std::io::Write;
use std::time::{Duration, Instant};

use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style as Paint};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};
use ratatui::{Terminal, TerminalOptions, Viewport};

use super::error::{CliError, CliResult};
use super::term::{emit_record, Style, Term};
use super::watch::{MapItems, RunWatch, StepState, Watch};
use crate::orchestrator::observe::RunKey;
use crate::orchestrator::run_control::{RunOutcome, RunPhase};

/// `--progress`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ProgressChoice {
    /// The live view on a terminal, lines otherwise
    Auto,
    /// The live view
    Live,
    /// A line per change
    Plain,
    /// Only the summary
    None,
}

/// What a person asked of the run from the keyboard: Ctrl-C, read as a key
/// while the live view holds the terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Key {
    Interrupt,
}

/// How runs are shown.
pub enum Shower {
    Live(LiveView),
    Plain(PlainLines),
    Json,
    Quiet,
}

impl Shower {
    pub fn new(term: &Term, choice: ProgressChoice) -> CliResult<Self> {
        if term.json {
            return Ok(Shower::Json);
        }
        Ok(match choice {
            ProgressChoice::None => Shower::Quiet,
            ProgressChoice::Plain => Shower::Plain(PlainLines::new(term.style)),
            ProgressChoice::Auto if !(term.interactive && term.stderr_is_terminal) => {
                if term.verbosity < 0 {
                    Shower::Quiet
                } else {
                    Shower::Plain(PlainLines::new(term.style))
                }
            }
            ProgressChoice::Auto | ProgressChoice::Live => {
                if !term.stderr_is_terminal {
                    return Err(CliError::usage("--progress live needs a terminal on stderr")
                        .hint("use --progress plain, or leave --progress out"));
                }
                Shower::Live(LiveView::open(term.style)?)
            }
        })
    }

    /// Whether keys arrive through this view (raw mode): Ctrl-C is then a key,
    /// not a signal.
    pub fn reads_keys(&self) -> bool {
        matches!(self, Shower::Live(_))
    }

    /// A row of a run changed.
    pub fn changed(&mut self, watch: &Watch, key: RunKey, row: Option<usize>) {
        let Some(run) = watch.runs.get(&key) else { return };
        match self {
            Shower::Live(_) | Shower::Quiet => {}
            Shower::Plain(lines) => lines.changed(watch, run, row),
            Shower::Json => {
                if let Some(index) = row {
                    emit_record(&row_record(run, index));
                }
                if let Some(log) = run.logs.last() {
                    if row.is_none() || log.step.as_deref() == row.map(|i| run.rows[i].name.as_str()) {
                        emit_record(&serde_json::json!({
                            "type": "log",
                            "run": key.run, "strand": key.strand,
                            "step": log.step, "level": log.level, "message": log.message,
                        }));
                    }
                }
            }
        }
    }

    /// Something to say about the run as a whole: the stop's verdict, a note.
    pub fn say(&mut self, line: &str, record: serde_json::Value) {
        match self {
            Shower::Live(view) => view.say(line),
            Shower::Plain(_) | Shower::Quiet => eprintln!("{line}"),
            Shower::Json => emit_record(&record),
        }
    }

    /// Redraw the live view; nothing for the others.
    pub fn tick(&mut self, watch: &Watch, phase: RunPhase) -> CliResult<()> {
        match self {
            Shower::Live(view) => view.draw(watch, phase),
            _ => Ok(()),
        }
    }

    /// The next key, for the live view; never resolves for the others.
    pub async fn key(&mut self) -> Option<Key> {
        match self {
            Shower::Live(view) => view.key().await,
            _ => std::future::pending().await,
        }
    }

    /// A run ended: its summary goes to the scrollback.
    pub fn run_ended(&mut self, watch: &Watch, key: RunKey, outputs: &[String]) {
        let Some(run) = watch.runs.get(&key) else { return };
        match self {
            Shower::Json => {
                let (outcome, elapsed) = run.ended.clone().expect("a run is shown ended once it is");
                emit_record(&serde_json::json!({
                    "type": "run_finished",
                    "run": key.run, "strand": key.strand,
                    "label": run.label,
                    "outcome": outcome_word(&outcome),
                    "seconds": elapsed.as_secs_f64(),
                    "outputs": outputs,
                }));
            }
            Shower::Live(view) => view.say_lines(&summary(run, outputs, &view.style)),
            Shower::Plain(lines) => {
                for line in summary(run, outputs, &lines.style) {
                    eprintln!("{line}");
                }
            }
            Shower::Quiet => {
                for line in summary(run, outputs, &Style::plain()) {
                    eprintln!("{line}");
                }
            }
        }
    }

    /// Put the terminal back as it was.
    pub fn close(self) -> CliResult<()> {
        match self {
            Shower::Live(view) => view.close(),
            _ => Ok(()),
        }
    }
}

/// The word an outcome is reported as.
pub fn outcome_word(outcome: &RunOutcome) -> &'static str {
    match outcome {
        RunOutcome::Completed => "completed",
        RunOutcome::Tolerated => "tolerated",
        RunOutcome::Stopped => "stopped",
        RunOutcome::Failed => "failed",
        RunOutcome::Aborted => "aborted",
    }
}

fn state_word(state: &StepState) -> &'static str {
    match state {
        StepState::Waiting => "waiting",
        StepState::Running => "running",
        StepState::Done => "done",
        StepState::Skipped => "skipped",
        StepState::Tolerated => "failed (tolerated)",
        StepState::Failed => "failed",
        StepState::NotRun => "not run",
        StepState::Cancelled => "cancelled",
    }
}

fn row_record(run: &RunWatch, index: usize) -> serde_json::Value {
    let row = &run.rows[index];
    serde_json::json!({
        "type": "step",
        "run": run.key.run, "strand": run.key.strand,
        "index": index,
        "step": row.name,
        "token": row.token,
        "state": state_word(&row.state),
        "progress": row.progress,
        "items": row.map.as_ref().map(|m| serde_json::json!({
            "of": m.of, "started": m.started, "done": m.done, "failed": m.failed, "total": m.total,
        })),
    })
}

/// `3/5 pages` for a map's items.
fn items_text(map: &MapItems) -> String {
    let ended = map.done + map.failed;
    let failed = if map.failed > 0 { format!(", {} failed", map.failed) } else { String::new() };
    match map.total {
        Some(total) => format!("{ended}/{total} {}{failed}", map.of),
        None => format!("{ended}/{} {}{failed}", map.started, map.of),
    }
}

/// The lines a run leaves behind: how it ended, how long it took, what each
/// step did when it did not simply complete, and where the outputs went.
pub fn summary(run: &RunWatch, outputs: &[String], style: &Style) -> Vec<String> {
    let Some((outcome, elapsed)) = run.ended.clone() else { return Vec::new() };
    let mark = match outcome {
        RunOutcome::Completed => style.good(style.glyph("✓", "+")),
        RunOutcome::Tolerated => style.warning(style.glyph("✓", "+")),
        RunOutcome::Stopped => style.warning(style.glyph("■", "#")),
        RunOutcome::Failed | RunOutcome::Aborted => style.bad(style.glyph("✗", "x")),
    };
    let steps = run.rows.len();
    let items: usize = run.rows.iter().filter_map(|r| r.map.as_ref()).map(|m| m.done).sum();
    let dot = style.glyph(" · ", ", ");
    let mut head = format!(
        "{mark} {}  {} step{}",
        style.name(&run.label),
        steps,
        if steps == 1 { "" } else { "s" }
    );
    if items > 0 {
        head.push_str(&format!("{dot}{items} item{}", if items == 1 { "" } else { "s" }));
    }
    head.push_str(&format!("{dot}{:.1} s", elapsed.as_secs_f64()));
    match outcome {
        RunOutcome::Completed => {}
        other => head.push_str(&format!("{dot}{}", outcome_word(&other))),
    }
    let mut lines = vec![head];
    for row in &run.rows {
        let said = match row.state {
            StepState::Done | StepState::Waiting | StepState::Running => continue,
            ref other => state_word(other),
        };
        let items = row.map.as_ref().map(|m| format!(" ({})", items_text(m))).unwrap_or_default();
        lines.push(format!("  {} {said}{items}", row.name));
    }
    for output in outputs {
        lines.push(format!("  {} {}", style.glyph("→", "->"), style.dim(output)));
    }
    lines
}

/// One line per change on stderr: a step starting, ending, failing, and its
/// progress at most once a second.
pub struct PlainLines {
    style: Style,
    shown: HashMap<(RunKey, usize), (String, Instant)>,
}

impl PlainLines {
    fn new(style: Style) -> Self {
        Self {
            style,
            shown: HashMap::new(),
        }
    }

    fn changed(&mut self, _watch: &Watch, run: &RunWatch, row: Option<usize>) {
        let Some(index) = row else {
            if let Some(log) = run.logs.last() {
                eprintln!("  {}: {}", log.level, log.message);
            }
            return;
        };
        let r = &run.rows[index];
        let state = state_word(&r.state).to_string();
        let now = Instant::now();
        let key = (run.key, index);
        let detail = match &r.map {
            Some(map) => items_text(map),
            None if r.state == StepState::Running => format!("{:>3.0}%", r.progress * 100.0),
            None => String::new(),
        };
        let line = format!(
            "[{}/{}] {}  {}{}",
            index + 1,
            run.rows.len(),
            r.name,
            if r.state == StepState::Running { detail.clone() } else { state.clone() },
            if r.state != StepState::Running && !detail.is_empty() && r.map.is_some() {
                format!(" ({detail})")
            } else {
                String::new()
            }
        );
        let say = match self.shown.get(&key) {
            None => true,
            Some((last_state, at)) => *last_state != state || now.duration_since(*at) >= Duration::from_secs(1),
        };
        if say {
            let line = match r.state {
                StepState::Failed => self.style.bad(&line),
                _ => line,
            };
            eprintln!("{line}");
            if let Some(said) = &r.last {
                if r.state == StepState::Failed {
                    eprintln!("    {said}");
                }
            }
            self.shown.insert(key, (state, now));
        }
    }
}

/// The inline block: drawn in the scrollback under the command.
pub struct LiveView {
    terminal: Terminal<CrosstermBackend<std::io::Stderr>>,
    style: Style,
    events: crossterm::event::EventStream,
    selected: usize,
    show_log: bool,
    said: Vec<String>,
}

/// How many lines the block takes: a header, up to 10 steps, the selected
/// step's last line, the keys.
const BLOCK_HEIGHT: u16 = 14;

impl LiveView {
    fn open(style: Style) -> CliResult<Self> {
        crossterm::terminal::enable_raw_mode()
            .map_err(|e| CliError::failed("the terminal could not be set up for the live view").context(e.to_string()))?;
        let terminal = Terminal::with_options(
            CrosstermBackend::new(std::io::stderr()),
            TerminalOptions {
                viewport: Viewport::Inline(BLOCK_HEIGHT),
            },
        )
        .map_err(|e| {
            let _ = crossterm::terminal::disable_raw_mode();
            CliError::failed("the live view could not be drawn").context(e.to_string())
        })?;
        Ok(Self {
            terminal,
            style,
            events: crossterm::event::EventStream::new(),
            selected: 0,
            show_log: false,
            said: Vec::new(),
        })
    }

    /// Lines that go to the scrollback above the block.
    fn say_lines(&mut self, lines: &[String]) {
        for line in lines {
            self.said.push(line.clone());
        }
    }

    fn say(&mut self, line: &str) {
        self.said.push(line.to_string());
    }

    async fn key(&mut self) -> Option<Key> {
        use futures_util::StreamExt;
        loop {
            let event = self.events.next().await?;
            let Ok(Event::Key(KeyEvent { code, modifiers, kind, .. })) = event else { continue };
            if kind != KeyEventKind::Press {
                continue;
            }
            match (code, modifiers) {
                (KeyCode::Char('c'), m) if m.contains(KeyModifiers::CONTROL) => return Some(Key::Interrupt),
                (KeyCode::Char('l'), _) => self.show_log = !self.show_log,
                (KeyCode::Up, _) => self.selected = self.selected.saturating_sub(1),
                (KeyCode::Down, _) => self.selected += 1,
                _ => {}
            }
        }
    }

    fn draw(&mut self, watch: &Watch, phase: RunPhase) -> CliResult<()> {
        let said = std::mem::take(&mut self.said);
        for line in said {
            let text = strip_escapes(&line);
            self.terminal
                .insert_before(1, |buf| Paragraph::new(text.clone()).render(buf.area, buf))
                .map_err(|e| CliError::failed("the live view could not be drawn").context(e.to_string()))?;
        }
        let style = self.style;
        let selected = &mut self.selected;
        let show_log = self.show_log;
        self.terminal
            .draw(|frame| {
                let area = frame.area();
                let lines = block_lines(watch, phase, selected, show_log, style, area.width);
                let height = (lines.len() as u16).min(area.height);
                frame.render_widget(Paragraph::new(lines), Rect { height, ..area });
            })
            .map_err(|e| CliError::failed("the live view could not be drawn").context(e.to_string()))?;
        Ok(())
    }

    fn close(mut self) -> CliResult<()> {
        let said = std::mem::take(&mut self.said);
        let _ = self.terminal.clear();
        let _ = crossterm::terminal::disable_raw_mode();
        // The block is gone; what it leaves is the summary, in the scrollback.
        let mut stderr = std::io::stderr();
        let _ = crossterm::execute!(stderr, crossterm::cursor::MoveToColumn(0));
        for line in said {
            let _ = writeln!(stderr, "{line}");
        }
        Ok(())
    }
}

/// Text without the escape codes the plain style adds: the block is painted
/// by ratatui, not by escape codes in its strings.
fn strip_escapes(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            for c in chars.by_ref() {
                if c == 'm' {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// The block's lines: the run being watched, its steps, the selected step's
/// last line (or more of the log), the keys.
fn block_lines(
    watch: &Watch,
    phase: RunPhase,
    selected: &mut usize,
    show_log: bool,
    style: Style,
    width: u16,
) -> Vec<Line<'static>> {
    let colour = style.is_colour();
    let paint = |color: Color| if colour { Paint::default().fg(color) } else { Paint::default() };
    let bold = if colour { Paint::default().add_modifier(Modifier::BOLD) } else { Paint::default() };
    let mut lines = Vec::new();
    // The run to show: the first that has not ended, else the last.
    let Some(run) = watch.runs.values().find(|r| r.ended.is_none()).or_else(|| watch.runs.values().last()) else {
        return lines;
    };
    let total = watch.runs.keys().map(|k| k.run).max().map(|m| m + 1).unwrap_or(1);
    let elapsed = run.started.elapsed().as_secs_f64();
    let counter = if total > 1 { format!("  run {}/{total}", run.key.run + 1) } else { String::new() };
    lines.push(Line::from(vec![
        Span::styled(run.label.clone(), bold),
        Span::raw(format!("{counter}  {elapsed:.1} s")),
    ]));
    *selected = (*selected).min(run.rows.len().saturating_sub(1));
    for (index, row) in run.rows.iter().enumerate().take(10) {
        let (mark, color) = match row.state {
            StepState::Waiting => (style.glyph("·", "."), Color::DarkGray),
            StepState::Running => (style.glyph("◐", "*"), Color::Cyan),
            StepState::Done => (style.glyph("✓", "+"), Color::Green),
            StepState::Skipped => (style.glyph("↷", ">"), Color::DarkGray),
            StepState::Tolerated => (style.glyph("⚠", "!"), Color::Yellow),
            StepState::Failed => (style.glyph("✗", "x"), Color::Red),
            StepState::NotRun => (style.glyph("–", "-"), Color::DarkGray),
            StepState::Cancelled => (style.glyph("✗", "x"), Color::Yellow),
        };
        let detail = match (&row.map, &row.state) {
            (Some(map), _) => items_text(map),
            (None, StepState::Running) => bar(row.progress, 10, &style),
            (None, other) => state_word(other).to_string(),
        };
        let pointer = if index == *selected { style.glyph("›", ">") } else { " " };
        let name_width = (width as usize).saturating_sub(detail.chars().count() + 6).clamp(8, 40);
        let name: String = row.name.chars().take(name_width).collect();
        lines.push(Line::from(vec![
            Span::raw(format!("{pointer}")),
            Span::styled(format!("{mark} "), paint(color)),
            Span::raw(format!("{name:<name_width$} ")),
            Span::styled(detail, paint(Color::Gray)),
        ]));
    }
    if run.rows.len() > 10 {
        lines.push(Line::from(format!("  … {} more steps", run.rows.len() - 10)));
    }
    let log_lines: Vec<String> = if show_log {
        run.logs
            .iter()
            .rev()
            .take(4)
            .rev()
            .map(|l| format!("{}: {}", l.step.as_deref().unwrap_or("machine"), l.message))
            .collect()
    } else {
        run.rows
            .get(*selected)
            .and_then(|row| row.last.as_ref().map(|said| format!("{}: {said}", row.name)))
            .into_iter()
            .collect()
    };
    for line in log_lines {
        let line: String = line.chars().take(width as usize).collect();
        lines.push(Line::from(Span::styled(line, paint(Color::DarkGray))));
    }
    let keys = match phase {
        RunPhase::Running => style.glyph("Ctrl-C stop · Ctrl-C twice abort · l log · ↑↓ step", "Ctrl-C stop, Ctrl-C twice abort, l log, up/down step"),
        RunPhase::Stopping => "stopping: nothing new starts, what started finishes · Ctrl-C abort",
        RunPhase::Aborting => "aborting: the cartridges are ending · Ctrl-C end now",
        RunPhase::Ended => "",
    };
    lines.push(Line::from(Span::styled(keys.to_string(), paint(Color::DarkGray))));
    lines
}

/// `███████░░░` for a fraction.
fn bar(fraction: f32, width: usize, style: &Style) -> String {
    let fraction = fraction.clamp(0.0, 1.0);
    let filled = ((fraction * width as f32).round() as usize).min(width);
    format!(
        "{}{} {:>3.0}%",
        style.glyph("█", "#").repeat(filled),
        style.glyph("░", "-").repeat(width - filled),
        fraction * 100.0
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST20116: a map's items read as how many ended of how many, and a
    // failed item is counted as failed.
    #[test]
    fn test20116_items_read_as_ended_of_total() {
        let map = MapItems { started: 5, done: 3, failed: 0, total: None, current: None, of: "pages".into() };
        assert_eq!(items_text(&map), "3/5 pages");
        let map = MapItems { started: 5, done: 3, failed: 1, total: Some(5), current: None, of: "pages".into() };
        assert_eq!(items_text(&map), "4/5 pages, 1 failed");
    }

    // TEST20117: the progress bar fills in proportion and states the percent,
    // never past the whole.
    #[test]
    fn test20117_progress_bar() {
        let style = Style::plain();
        assert_eq!(bar(0.5, 10, &style), "█████░░░░░  50%");
        assert_eq!(bar(1.2, 4, &style), "████ 100%", "progress past the whole is shown as the whole");
    }
}
