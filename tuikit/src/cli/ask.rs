//! Asking a person: one of several, or a value.
//!
//! The model of the command line decides when a person is asked
//! ([`super::formal`]); this is how. A choice shows the options by their
//! names, with the URN only on request (`-v`), in an inline picker — type to
//! filter, ↑ ↓ to move, Enter to choose, Esc or Ctrl-C to decline — drawn
//! under the command, as tall as its options need, and cleared when it is
//! done, so what the command prints next starts on the row it began on. A
//! value is typed on one line, Enter to give it. Declining ends the command
//! as an interrupted one (exit 130).
//!
//! Where nobody can be asked, the command fails naming the choices, or the
//! value, and the flag that gives it ([`named`]); under `--json` each option
//! is first written as a `choice` record, so a program can read the options
//! and run the line again with the flag.

use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::style::{Color, Modifier, Style as Paint};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use super::ctx::Ctx;
use super::error::{CliError, CliResult, Exit};
use super::inline::Surface;
use super::term::emit_record;

/// One option.
#[derive(Debug, Clone)]
pub struct Choice {
    /// What a person reads and types: a name.
    pub label: String,
    /// What it is: a title, `pdf → txt`, a step count.
    pub detail: String,
    /// The URN behind the label, shown on request.
    pub urn: Option<String>,
    /// What the flag takes to choose it.
    pub value: String,
}

/// The question, and the flag that answers it without asking.
pub struct Question {
    /// What is being chosen or given: "what to make", "the machine".
    pub what: String,
    /// The flag, as typed, up to the value: `--to `, `--input doc=`.
    pub flag: String,
}

/// A value asked for: what it is for, the flag that gives it, and the forms
/// it takes.
pub struct ValueQuestion {
    /// What it is for: "the input doc (pdf)".
    pub what: String,
    /// The flag, as typed, up to the value.
    pub flag: String,
    /// The forms it takes: "@<file>, - for stdin, or text".
    pub forms: String,
}

/// What a person did with a question.
pub enum Answered<T> {
    Given(T),
    /// Esc or Ctrl-C.
    Declined,
}

/// The error a declined question ends the command with: exit 130, as an
/// interrupted command.
pub fn declined() -> CliError {
    CliError { exit: Exit::Declined, ..CliError::failed("nothing was chosen, and the command ends here") }
}

/// Ask a person to choose one of `choices`.
pub async fn pick(ctx: &Ctx, question: &Question, choices: &[Choice]) -> CliResult<Answered<usize>> {
    if choices.is_empty() {
        return Err(CliError::failed(format!("there is nothing to choose {} from", question.what))
            .hint("report it: a person is asked only to choose among several"));
    }
    picker(question, choices, ctx.term.verbosity >= 1, ctx.term.style).await
}

/// Ask a person to type a value.
pub async fn value(ctx: &Ctx, question: &ValueQuestion) -> CliResult<Answered<String>> {
    let failed = |e: std::io::Error| CliError::failed("the question could not be drawn").context(e.to_string());
    let mut surface = Surface::open(2).map_err(failed)?;
    let mut typed = String::new();
    let style = ctx.term.style;
    let answered = loop {
        let drawn = surface.draw(|frame| {
            let area = frame.area();
            let colour = style.is_colour();
            let strong = if colour { Paint::default().add_modifier(Modifier::BOLD) } else { Paint::default() };
            let dim = if colour { Paint::default().fg(Color::DarkGray) } else { Paint::default() };
            let lines = vec![
                Line::from(vec![Span::styled(format!("A value for {}: ", question.what), strong), Span::raw(typed.clone())]),
                Line::from(Span::styled(
                    format!(
                        "{}{}{}",
                        question.forms,
                        style.glyph(" · Enter give · Esc cancel · the same as ", "; Enter give, Esc cancel; the same as "),
                        question.flag
                    ) + "<value>",
                    dim,
                )),
            ];
            frame.render_widget(Paragraph::new(lines), area);
        });
        if let Err(e) = drawn {
            break Err(failed(e));
        }
        let KeyEvent { code, modifiers, .. } = match next_key().await {
            Ok(key) => key,
            Err(e) => break Err(failed(e)),
        };
        match (code, modifiers) {
            (KeyCode::Enter, _) if !typed.is_empty() => break Ok(Answered::Given(typed.clone())),
            (KeyCode::Esc, _) => break Ok(Answered::Declined),
            (KeyCode::Char('c'), m) if m.contains(KeyModifiers::CONTROL) => break Ok(Answered::Declined),
            (KeyCode::Char('z'), m) if m.contains(KeyModifiers::CONTROL) => {
                if let Err(e) = surface.suspend() {
                    break Err(failed(e));
                }
            }
            (KeyCode::Backspace, _) => {
                typed.pop();
            }
            (KeyCode::Char(c), m) if !m.contains(KeyModifiers::CONTROL) => typed.push(c),
            _ => {}
        }
    };
    surface.close().map_err(failed)?;
    answered
}

/// Say why what was typed is not taken, before the question is asked again.
pub fn say_retry(ctx: &Ctx, why: &str) {
    eprintln!("{} {why}", ctx.term.style.warning("not taken:"));
}

/// Where nobody can be asked: the command fails naming the choices and the
/// flag that gives one. Under `--json` each is a `choice` record first.
pub fn named(ctx: &Ctx, question: &Question, choices: &[Choice]) -> CliError {
    const SHOWN: usize = 12;
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
    let mut error = CliError::usage(format!("{} options for {}, and nobody to ask", choices.len(), question.what));
    for choice in choices.iter().take(SHOWN) {
        let detail = if choice.detail.is_empty() { String::new() } else { format!("  {}", choice.detail) };
        error = error.context(format!("{}{}{detail}", question.flag, super::shell_word(&choice.value)));
    }
    if choices.len() > SHOWN {
        error = error.context(format!("… and {} more", choices.len() - SHOWN));
    }
    error.hint(format!("give one with {}<value>", question.flag))
}

/// Where nobody can be asked: the command fails naming the value it needs and
/// the flag that gives it. Under `--json` it is a `choice` record first, with
/// no options.
pub fn named_value(ctx: &Ctx, question: &ValueQuestion) -> CliError {
    if ctx.term.json {
        emit_record(&serde_json::json!({
            "type": "choice", "of": question.what, "flag": question.flag, "forms": question.forms, "value": null,
        }));
    }
    CliError::usage(format!("{} is given nothing, and nobody to ask", question.what))
        .hint(format!("give it: {}<value>, where the value is {}", question.flag, question.forms))
}

impl ValueQuestion {
    /// What `-` holds.
    pub fn stdin_type() -> Self {
        ValueQuestion {
            what: "what - holds".into(),
            flag: "--from ".into(),
            forms: "a media type: an alias, an extension or a media URN".into(),
        }
    }

    /// A value for an input of a machine, by its name.
    pub fn input(name: &str, media: &str) -> Self {
        ValueQuestion {
            what: format!("the input {name} ({media})"),
            flag: format!("--input {name}="),
            forms: "@<file>, - for stdin, or text".into(),
        }
    }

    /// A value for a step option a machine leaves to the run.
    pub fn option(name: &str, step: &str, flag: &str, media: &str) -> Self {
        ValueQuestion {
            what: format!("the option {name} of step {step}"),
            flag: format!("--arg {flag}="),
            forms: format!("{media}, or @<file> to read one"),
        }
    }
}

/// The inline picker: as tall as its options need, and no taller than twelve
/// rows.
async fn picker(question: &Question, choices: &[Choice], show_urn: bool, style: super::term::Style) -> CliResult<Answered<usize>> {
    let height = picker_height(choices.len());
    let failed = |e: std::io::Error| CliError::failed("the picker could not be drawn").context(e.to_string());
    let mut surface = Surface::open(height).map_err(failed)?;
    let mut filter = String::new();
    let mut selected = 0usize;
    let chosen = loop {
        let matching: Vec<usize> = choices
            .iter()
            .enumerate()
            .filter(|(_, c)| matches(&filter, c))
            .map(|(i, _)| i)
            .collect();
        selected = selected.min(matching.len().saturating_sub(1));
        let rows = surface.height();
        let drawn = surface.draw(|frame| {
            let area = frame.area();
            let lines = picker_lines(question, choices, &matching, &filter, selected, show_urn, style, area.width, rows);
            frame.render_widget(Paragraph::new(lines), area);
        });
        if let Err(e) = drawn {
            break Err(failed(e));
        }
        let KeyEvent { code, modifiers, .. } = match next_key().await {
            Ok(key) => key,
            Err(e) => break Err(failed(e)),
        };
        match (code, modifiers) {
            (KeyCode::Enter, _) => match matching.get(selected) {
                Some(&index) => break Ok(Answered::Given(index)),
                None => continue,
            },
            (KeyCode::Esc, _) => break Ok(Answered::Declined),
            (KeyCode::Char('c'), m) if m.contains(KeyModifiers::CONTROL) => break Ok(Answered::Declined),
            (KeyCode::Char('z'), m) if m.contains(KeyModifiers::CONTROL) => {
                if let Err(e) = surface.suspend() {
                    break Err(failed(e));
                }
            }
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
    // What the command prints next starts on the row the picker began on.
    surface.close().map_err(failed)?;
    chosen
}

/// The next key pressed. Keys are read here, between draws, and never by a
/// thread of their own: a thread left reading the terminal would take the
/// answer the surface waits for when it asks where the cursor is.
async fn next_key() -> std::io::Result<KeyEvent> {
    const EVERY: std::time::Duration = std::time::Duration::from_millis(20);
    loop {
        while crossterm::event::poll(std::time::Duration::ZERO)? {
            if let Event::Key(key) = crossterm::event::read()? {
                if key.kind == KeyEventKind::Press {
                    return Ok(key);
                }
            }
        }
        tokio::time::sleep(EVERY).await;
    }
}

/// The rows a picker of `choices` options takes: its question, an option a
/// row, and its keys, up to twelve.
fn picker_height(choices: usize) -> u16 {
    const MOST: usize = 12;
    (choices + 2).min(MOST) as u16
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
    question: &Question,
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
        Choice { label: label.into(), detail: detail.into(), urn: None, value: label.into() }
    }

    // TEST20119: typing filters by every word, in the label or the detail.
    #[test]
    fn test20119_typing_filters_by_every_word() {
        let c = choice("disbind-pdf", "Disbind PDF Into Pages");
        assert!(matches("pdf pages", &c));
        assert!(matches("DISBIND", &c));
        assert!(!matches("pdf png", &c));
    }

    // TEST20124: a declined question ends the command as an interrupted one.
    #[test]
    fn test20124_a_declined_question_exits_130() {
        assert_eq!(declined().exit.code(), 130);
    }
}
