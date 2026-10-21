//! The command palette: every action a surface offers, by name, found by a
//! few letters of it — the way modern editors and terminal tools make a large
//! surface reachable without memorising its keys.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect as Area;
use ratatui::style::Modifier;

use crate::tui::text;
use crate::tui::theme::{Theme, Tone};

/// One action of the palette.
#[derive(Debug, Clone)]
pub struct Command<A> {
    pub name: String,
    /// The key that does the same thing, shown beside the name.
    pub keys: String,
    pub action: A,
}

/// What a key did to the palette.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaletteResult<A> {
    Continue,
    Close,
    Run(A),
}

/// The palette while it is open.
#[derive(Debug, Clone)]
pub struct Palette<A> {
    pub query: String,
    pub selected: usize,
    commands: Vec<Command<A>>,
}

/// How well `query` matches `text`: `None` when its letters do not occur in
/// order; otherwise higher for consecutive letters, letters that start
/// words, and an early first match. Case-insensitive.
pub fn fuzzy_score(query: &str, text: &str) -> Option<i64> {
    if query.is_empty() {
        return Some(0);
    }
    let haystack: Vec<char> = text.chars().flat_map(char::to_lowercase).collect();
    let needle: Vec<char> = query.chars().flat_map(char::to_lowercase).collect();
    let mut score = 0i64;
    let mut position = 0usize;
    let mut previous: Option<usize> = None;
    for &wanted in &needle {
        let found = haystack[position..].iter().position(|&c| c == wanted)? + position;
        let word_start = found == 0 || !haystack[found - 1].is_alphanumeric();
        score += 1;
        if word_start {
            score += 8;
        }
        if previous.is_some_and(|p| p + 1 == found) {
            score += 5;
        }
        if previous.is_none() {
            score -= found as i64 / 2;
        }
        previous = Some(found);
        position = found + 1;
    }
    Some(score)
}

impl<A: Clone> Palette<A> {
    pub fn new(commands: Vec<Command<A>>) -> Self {
        Self {
            query: String::new(),
            selected: 0,
            commands,
        }
    }

    /// The commands matching the query, best first; ties keep their order.
    pub fn matches(&self) -> Vec<&Command<A>> {
        let mut scored: Vec<(i64, usize)> = self
            .commands
            .iter()
            .enumerate()
            .filter_map(|(i, c)| fuzzy_score(&self.query, &c.name).map(|s| (s, i)))
            .collect();
        scored.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        scored.into_iter().map(|(_, i)| &self.commands[i]).collect()
    }

    pub fn on_key(&mut self, key: KeyEvent) -> PaletteResult<A> {
        match key.code {
            KeyCode::Esc => PaletteResult::Close,
            KeyCode::Enter => match self.matches().get(self.selected) {
                Some(command) => PaletteResult::Run(command.action.clone()),
                None => PaletteResult::Continue,
            },
            KeyCode::Up => {
                self.selected = self.selected.saturating_sub(1);
                PaletteResult::Continue
            }
            KeyCode::Down => {
                let count = self.matches().len();
                if self.selected + 1 < count {
                    self.selected += 1;
                }
                PaletteResult::Continue
            }
            KeyCode::Char('n') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                let count = self.matches().len();
                if self.selected + 1 < count {
                    self.selected += 1;
                }
                PaletteResult::Continue
            }
            KeyCode::Char('p') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.selected = self.selected.saturating_sub(1);
                PaletteResult::Continue
            }
            KeyCode::Backspace => {
                self.query.pop();
                self.selected = 0;
                PaletteResult::Continue
            }
            KeyCode::Char(c) => {
                self.query.push(c);
                self.selected = 0;
                PaletteResult::Continue
            }
            _ => PaletteResult::Continue,
        }
    }

    /// Draw the palette centred over `area`.
    pub fn draw(&self, buffer: &mut Buffer, area: Area, theme: &Theme) {
        let w = (area.width * 3 / 5).clamp(30.min(area.width), 72);
        let matches = self.matches();
        let h = ((matches.len() as u16) + 4).min(area.height.saturating_sub(4)).max(5);
        let x = area.x + (area.width - w) / 2;
        let y = area.y + 2;
        let ground = theme.chrome();
        for row in y..y + h {
            for col in x..x + w {
                buffer[(col, row)].set_char(' ').set_style(ground);
            }
        }
        let border = theme.fg(Tone::Accent).patch(ground);
        let glyphs = theme.glyphs;
        use crate::tui::theme::{Weight, EAST, NORTH, SOUTH, WEST};
        for col in x..x + w {
            buffer[(col, y)].set_char(glyphs.line(EAST | WEST, Weight::Light, false)).set_style(border);
            buffer[(col, y + h - 1)].set_char(glyphs.line(EAST | WEST, Weight::Light, false)).set_style(border);
        }
        for row in y..y + h {
            buffer[(x, row)].set_char(glyphs.line(NORTH | SOUTH, Weight::Light, false)).set_style(border);
            buffer[(x + w - 1, row)].set_char(glyphs.line(NORTH | SOUTH, Weight::Light, false)).set_style(border);
        }
        buffer[(x, y)].set_char(glyphs.line(EAST | SOUTH, Weight::Light, true)).set_style(border);
        buffer[(x + w - 1, y)].set_char(glyphs.line(SOUTH | WEST, Weight::Light, true)).set_style(border);
        buffer[(x, y + h - 1)].set_char(glyphs.line(NORTH | EAST, Weight::Light, true)).set_style(border);
        buffer[(x + w - 1, y + h - 1)].set_char(glyphs.line(NORTH | WEST, Weight::Light, true)).set_style(border);
        let inner = (w - 4) as usize;
        buffer.set_string(
            x + 2,
            y + 1,
            text::pad(&format!("› {}▏", self.query), inner),
            theme.fg(Tone::Ink).add_modifier(Modifier::BOLD).patch(ground),
        );
        let rows = (h - 3) as usize;
        let start = self.selected.saturating_sub(rows.saturating_sub(1));
        for (i, command) in matches.iter().enumerate().skip(start).take(rows) {
            let row = y + 2 + (i - start) as u16;
            let selected = i == self.selected;
            let style = if selected {
                theme.fg(Tone::Ink).add_modifier(Modifier::BOLD).patch(theme.soft(Tone::Accent))
            } else {
                theme.fg(Tone::Secondary).patch(ground)
            };
            let keys_w = text::width(&command.keys);
            let name = text::pad(&command.name, inner.saturating_sub(keys_w + 1));
            buffer.set_string(x + 2, row, format!("{name} "), style);
            buffer.set_string(
                x + 2 + (inner - keys_w) as u16,
                row,
                &command.keys,
                theme.fg(Tone::Muted).patch(if selected { theme.soft(Tone::Accent) } else { ground }),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST13043: the palette finds a command by letters in order, prefers
    // letters at word starts and in a row, and finds nothing for letters out
    // of order.
    #[test]
    fn test13043_fuzzy_score_prefers_word_starts_and_runs() {
        assert!(fuzzy_score("sti", "Stop input").is_some());
        assert!(fuzzy_score("its", "Stop input").is_none(), "letters must occur in order");
        let word_starts = fuzzy_score("si", "Stop input").unwrap();
        let scattered = fuzzy_score("si", "Saved files").unwrap();
        assert!(word_starts > scattered, "{word_starts} vs {scattered}");
        // Letters in a row beat the same letters apart, word starts aside.
        let run = fuzzy_score("abc", "xabcx").unwrap();
        let spread = fuzzy_score("abc", "xaxbxcx").unwrap();
        assert!(run > spread, "a run of letters beats the same letters apart: {run} vs {spread}");
        // Letters that each start a word are an acronym, and rank as one.
        let acronym = fuzzy_score("abo", "A big outline").unwrap();
        let inside = fuzzy_score("abo", "Sabotage").unwrap();
        assert!(acronym > inside, "{acronym} vs {inside}");
        let commands = vec![
            Command { name: "Saved files".to_string(), keys: "5".into(), action: 1 },
            Command { name: "Stop input".to_string(), keys: "s".into(), action: 2 },
        ];
        let mut palette = Palette::new(commands);
        palette.query = "si".to_string();
        assert_eq!(palette.matches()[0].action, 2);
    }
}
