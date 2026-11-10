//! Choosing one of several: a target, a machine, a cap, a model.
//!
//! Wherever the command line needs a choice it shows the options by their
//! aliases, with the URN only on request (`-v`), and:
//!
//! - on a terminal, opens an inline picker — type to filter, ↑ ↓ to move,
//!   Enter to choose, Esc to give up — drawn under the command and cleared
//!   when it is done;
//! - without one (a pipe, CI, `--no-input`), prompts for nothing: the choice
//!   not given is an error naming the flag that gives it and the values it
//!   takes;
//! - under `--json`, writes each option as a `choice` record before that
//!   error, so a program can read the options and ask again with the flag.

use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::backend::CrosstermBackend;
use ratatui::style::{Color, Modifier, Style as Paint};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::{Terminal, TerminalOptions, Viewport};

use super::ctx::Ctx;
use super::error::{CliError, CliResult};
use super::term::emit_record;

/// One option.
#[derive(Debug, Clone)]
pub struct Choice {
    /// What a person reads and types: an alias.
    pub label: String,
    /// What it is: a title, `pdf → txt`, a step count.
    pub detail: String,
    /// The URN behind the label, shown on request.
    pub urn: Option<String>,
    /// What the flag takes to choose it.
    pub value: String,
}

/// The question, and the flag that answers it without asking.
pub struct Question<'a> {
    /// What is being chosen: "the target", "the machine".
    pub what: &'a str,
    /// The flag, as typed: `--to`, `--pick`.
    pub flag: &'a str,
}

/// Choose one of `choices`, or say how to choose.
pub async fn choose(ctx: &Ctx, question: &Question<'_>, choices: &[Choice]) -> CliResult<usize> {
    if choices.len() == 1 {
        return Ok(0);
    }
    if choices.is_empty() {
        return Err(CliError::failed(format!("there is nothing to choose {} from", question.what)));
    }
    if ctx.term.json {
        for (index, choice) in choices.iter().enumerate() {
            emit_record(&serde_json::json!({
                "type": "choice",
                "of": question.what,
                "flag": question.flag,
                "index": index + 1,
                "value": choice.value,
                "label": choice.label,
                "detail": choice.detail,
                "urn": choice.urn,
            }));
        }
    }
    if !ctx.term.interactive {
        return Err(not_chosen(question, choices));
    }
    pick(question, choices, ctx.term.verbosity >= 1, ctx.term.style).await
}

/// The error for a choice not made.
pub fn not_chosen(question: &Question<'_>, choices: &[Choice]) -> CliError {
    const SHOWN: usize = 12;
    let mut error = CliError::usage(format!(
        "{} options for {}, and none was chosen",
        choices.len(),
        question.what
    ));
    for choice in choices.iter().take(SHOWN) {
        error = error.context(format!("{} {}  {}", question.flag, choice.value, choice.detail));
    }
    if choices.len() > SHOWN {
        error = error.context(format!("… and {} more", choices.len() - SHOWN));
    }
    error.hint(format!("choose with {} <value>", question.flag))
}

/// The inline picker.
async fn pick(question: &Question<'_>, choices: &[Choice], show_urn: bool, style: super::term::Style) -> CliResult<usize> {
    use futures_util::StreamExt;
    const HEIGHT: u16 = 12;
    let failed = |e: std::io::Error| CliError::failed("the picker could not be drawn").context(e.to_string());
    crossterm::terminal::enable_raw_mode().map_err(failed)?;
    let mut terminal = match Terminal::with_options(
        CrosstermBackend::new(std::io::stderr()),
        TerminalOptions { viewport: Viewport::Inline(HEIGHT) },
    ) {
        Ok(terminal) => terminal,
        Err(e) => {
            let _ = crossterm::terminal::disable_raw_mode();
            return Err(failed(e));
        }
    };
    let mut filter = String::new();
    let mut selected = 0usize;
    let mut events = crossterm::event::EventStream::new();
    let chosen = loop {
        let matching: Vec<usize> = choices
            .iter()
            .enumerate()
            .filter(|(_, c)| matches(&filter, c))
            .map(|(i, _)| i)
            .collect();
        selected = selected.min(matching.len().saturating_sub(1));
        let drawn = terminal.draw(|frame| {
            let area = frame.area();
            let lines = picker_lines(question, choices, &matching, &filter, selected, show_urn, style, area.width, HEIGHT);
            frame.render_widget(Paragraph::new(lines), area);
        });
        if let Err(e) = drawn {
            break Err(failed(e));
        }
        let Some(event) = events.next().await else {
            break Err(CliError::failed("the terminal closed while choosing"));
        };
        let Ok(Event::Key(KeyEvent { code, modifiers, kind, .. })) = event else { continue };
        if kind != KeyEventKind::Press {
            continue;
        }
        match (code, modifiers) {
            (KeyCode::Enter, _) => match matching.get(selected) {
                Some(&index) => break Ok(index),
                None => continue,
            },
            (KeyCode::Esc, _) => break Err(gave_up(question)),
            (KeyCode::Char('c'), m) if m.contains(KeyModifiers::CONTROL) => break Err(gave_up(question)),
            (KeyCode::Up, _) => selected = selected.saturating_sub(1),
            (KeyCode::Down, _) => selected = (selected + 1).min(matching.len().saturating_sub(1)),
            (KeyCode::Backspace, _) => {
                filter.pop();
                selected = 0;
            }
            (KeyCode::Char(c), m) if !m.contains(KeyModifiers::CONTROL) => {
                filter.push(c);
                selected = 0;
            }
            _ => {}
        }
    };
    let _ = terminal.clear();
    let _ = crossterm::terminal::disable_raw_mode();
    chosen
}

fn gave_up(question: &Question<'_>) -> CliError {
    CliError::usage(format!("no choice was made for {}", question.what))
        .hint(format!("choose with {} <value>, or run the command again", question.flag))
}

/// Whether an option matches what was typed: every word of it appears in the
/// label, the detail or the URN, case aside.
fn matches(filter: &str, choice: &Choice) -> bool {
    let haystack = format!(
        "{} {} {}",
        choice.label.to_lowercase(),
        choice.detail.to_lowercase(),
        choice.urn.as_deref().unwrap_or("").to_lowercase()
    );
    filter
        .to_lowercase()
        .split_whitespace()
        .all(|word| haystack.contains(word))
}

#[allow(clippy::too_many_arguments)]
fn picker_lines(
    question: &Question<'_>,
    choices: &[Choice],
    matching: &[usize],
    filter: &str,
    selected: usize,
    show_urn: bool,
    style: super::term::Style,
    width: u16,
    height: u16,
) -> Vec<Line<'static>> {
    let colour = style.is_colour();
    let dim = if colour { Paint::default().fg(Color::DarkGray) } else { Paint::default() };
    let strong = if colour { Paint::default().add_modifier(Modifier::BOLD) } else { Paint::default() };
    let marked = if colour {
        Paint::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)
    } else {
        Paint::default()
    };
    let mut lines = vec![Line::from(vec![
        Span::styled(format!("Choose {}: ", question.what), strong),
        Span::raw(filter.to_string()),
        Span::styled(format!("  {}/{}", matching.len(), choices.len()), dim),
    ])];
    let room = height.saturating_sub(2) as usize;
    let first = selected.saturating_sub(room.saturating_sub(1));
    let label_width = matching
        .iter()
        .map(|&i| choices[i].label.chars().count())
        .max()
        .unwrap_or(0)
        .min(36);
    for (row, &index) in matching.iter().enumerate().skip(first).take(room) {
        let choice = &choices[index];
        let pointer = if row == selected { style.glyph("› ", "> ") } else { "  " };
        let mut detail = choice.detail.clone();
        if show_urn {
            if let Some(urn) = &choice.urn {
                detail = format!("{detail}  {urn}");
            }
        }
        let text: String = format!("{pointer}{:<label_width$}  ", choice.label);
        let room = (width as usize).saturating_sub(text.chars().count());
        let detail: String = detail.chars().take(room).collect();
        lines.push(Line::from(vec![
            Span::styled(text, if row == selected { marked } else { Paint::default() }),
            Span::styled(detail, dim),
        ]));
    }
    lines.push(Line::from(Span::styled(
        style
            .glyph("type to filter · ↑↓ move · Enter choose · Esc cancel", "type to filter, up/down move, Enter choose, Esc cancel")
            .to_string(),
        dim,
    )));
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    fn choice(label: &str, detail: &str) -> Choice {
        Choice {
            label: label.into(),
            detail: detail.into(),
            urn: None,
            value: label.into(),
        }
    }

    // TEST20118: a choice not made without a terminal is a usage error naming
    // the flag and the values it takes.
    #[test]
    fn test20118_a_choice_not_made_names_the_flag_and_its_values() {
        let question = Question { what: "the target", flag: "--to" };
        let error = not_chosen(&question, &[choice("txt", "plain text"), choice("png", "PNG image")]);
        assert_eq!(error.exit, super::super::error::Exit::Usage);
        assert_eq!(error.problem, "2 options for the target, and none was chosen");
        assert_eq!(error.context, ["--to txt  plain text", "--to png  PNG image"]);
        assert_eq!(error.hint.as_deref(), Some("choose with --to <value>"));
    }

    // TEST20119: typing filters by every word, in the label or the detail.
    #[test]
    fn test20119_typing_filters_by_every_word() {
        let c = choice("disbind-pdf", "Disbind PDF Into Pages");
        assert!(matches("pdf pages", &c));
        assert!(matches("DISBIND", &c));
        assert!(!matches("pdf png", &c));
    }
}
