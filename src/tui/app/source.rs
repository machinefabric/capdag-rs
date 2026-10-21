//! The notation source pane: a multi-line text buffer with a cursor, the
//! notation's semantic tokens coloured as an editor colours them, and the
//! text that belongs to what the graph has focused marked.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect as Area;
use ratatui::style::{Modifier, Style};
use unicode_segmentation::UnicodeSegmentation;

use crate::machine::notation_ast::{emit_semantic_tokens, parse_notation_ast, SemanticTokenType};
use crate::tui::text;
use crate::tui::theme::{Theme, Tone};

/// A text being edited.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceBuffer {
    lines: Vec<String>,
    /// Line, and grapheme index within it.
    cursor: (usize, usize),
    /// The first line shown.
    top: usize,
}

impl SourceBuffer {
    pub fn new(text: &str) -> Self {
        let mut lines: Vec<String> = text.split('\n').map(str::to_string).collect();
        if lines.is_empty() {
            lines.push(String::new());
        }
        Self {
            lines,
            cursor: (0, 0),
            top: 0,
        }
    }

    pub fn text(&self) -> String {
        self.lines.join("\n")
    }

    pub fn cursor(&self) -> (usize, usize) {
        self.cursor
    }

    fn graphemes(line: &str) -> Vec<&str> {
        line.graphemes(true).collect()
    }

    fn line_len(&self, line: usize) -> usize {
        Self::graphemes(&self.lines[line]).len()
    }

    /// The byte offset in the whole text of the cursor.
    pub fn cursor_byte(&self) -> usize {
        let (line, col) = self.cursor;
        let before: usize = self.lines[..line].iter().map(|l| l.len() + 1).sum();
        let in_line: usize = Self::graphemes(&self.lines[line])[..col].iter().map(|g| g.len()).sum();
        before + in_line
    }

    /// Put the cursor at byte offset `byte` of the whole text.
    pub fn set_cursor_byte(&mut self, byte: usize) {
        let mut remaining = byte;
        for (index, line) in self.lines.iter().enumerate() {
            if remaining <= line.len() {
                let mut col = 0;
                let mut used = 0;
                for g in Self::graphemes(line) {
                    if used + g.len() > remaining {
                        break;
                    }
                    used += g.len();
                    col += 1;
                }
                self.cursor = (index, col);
                return;
            }
            remaining -= line.len() + 1;
        }
        let last = self.lines.len() - 1;
        self.cursor = (last, self.line_len(last));
    }

    fn insert(&mut self, value: &str) {
        for (k, piece) in value.split('\n').enumerate() {
            if k > 0 {
                self.newline();
            }
            let (line, col) = self.cursor;
            let graphemes = Self::graphemes(&self.lines[line]);
            let byte: usize = graphemes[..col].iter().map(|g| g.len()).sum();
            self.lines[line].insert_str(byte, piece);
            self.cursor.1 += piece.graphemes(true).count();
        }
    }

    fn newline(&mut self) {
        let (line, col) = self.cursor;
        let graphemes = Self::graphemes(&self.lines[line]);
        let byte: usize = graphemes[..col].iter().map(|g| g.len()).sum();
        let rest = self.lines[line].split_off(byte);
        self.lines.insert(line + 1, rest);
        self.cursor = (line + 1, 0);
    }

    fn backspace(&mut self) {
        let (line, col) = self.cursor;
        if col > 0 {
            let graphemes = Self::graphemes(&self.lines[line]);
            let start: usize = graphemes[..col - 1].iter().map(|g| g.len()).sum();
            let end = start + graphemes[col - 1].len();
            self.lines[line].replace_range(start..end, "");
            self.cursor.1 -= 1;
        } else if line > 0 {
            let removed = self.lines.remove(line);
            let previous_len = self.line_len(line - 1);
            self.lines[line - 1].push_str(&removed);
            self.cursor = (line - 1, previous_len);
        }
    }

    fn delete(&mut self) {
        let (line, col) = self.cursor;
        if col < self.line_len(line) {
            self.cursor.1 += 1;
            self.backspace();
        } else if line + 1 < self.lines.len() {
            let next = self.lines.remove(line + 1);
            self.lines[line].push_str(&next);
        }
    }

    /// Handle an editing or a moving key; `true` when it was one.
    pub fn on_key(&mut self, key: KeyEvent) -> bool {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Char(c) if !ctrl => self.insert(&c.to_string()),
            KeyCode::Enter => self.newline(),
            KeyCode::Tab => self.insert("    "),
            KeyCode::Backspace => self.backspace(),
            KeyCode::Delete => self.delete(),
            _ => return self.move_cursor(key),
        }
        true
    }

    /// Handle a key that moves the cursor and changes nothing; `true` when
    /// it was one. All a read-only source takes.
    pub fn move_cursor(&mut self, key: KeyEvent) -> bool {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Left => {
                if self.cursor.1 > 0 {
                    self.cursor.1 -= 1;
                } else if self.cursor.0 > 0 {
                    self.cursor.0 -= 1;
                    self.cursor.1 = self.line_len(self.cursor.0);
                }
            }
            KeyCode::Right => {
                if self.cursor.1 < self.line_len(self.cursor.0) {
                    self.cursor.1 += 1;
                } else if self.cursor.0 + 1 < self.lines.len() {
                    self.cursor = (self.cursor.0 + 1, 0);
                }
            }
            KeyCode::Up if self.cursor.0 > 0 => {
                self.cursor.0 -= 1;
                self.cursor.1 = self.cursor.1.min(self.line_len(self.cursor.0));
            }
            KeyCode::Down if self.cursor.0 + 1 < self.lines.len() => {
                self.cursor.0 += 1;
                self.cursor.1 = self.cursor.1.min(self.line_len(self.cursor.0));
            }
            KeyCode::Home => self.cursor.1 = 0,
            KeyCode::End => self.cursor.1 = self.line_len(self.cursor.0),
            KeyCode::Char('a') if ctrl => self.cursor.1 = 0,
            KeyCode::Char('e') if ctrl => self.cursor.1 = self.line_len(self.cursor.0),
            KeyCode::PageUp => {
                self.cursor.0 = self.cursor.0.saturating_sub(20);
                self.cursor.1 = self.cursor.1.min(self.line_len(self.cursor.0));
            }
            KeyCode::PageDown => {
                self.cursor.0 = (self.cursor.0 + 20).min(self.lines.len() - 1);
                self.cursor.1 = self.cursor.1.min(self.line_len(self.cursor.0));
            }
            _ => return false,
        }
        true
    }

    /// Insert pasted text at the cursor.
    pub fn paste(&mut self, value: &str) {
        self.insert(&value.replace("\r\n", "\n").replace('\r', "\n"));
    }

    /// Draw into `area`: line numbers, the coloured text, the cursor (when
    /// `focused`), and the byte ranges in `marked` on a soft ground.
    pub fn draw(&mut self, buffer: &mut Buffer, area: Area, theme: &Theme, focused: bool, marked: &[(usize, usize)]) {
        let height = area.height as usize;
        if self.cursor.0 < self.top {
            self.top = self.cursor.0;
        } else if self.cursor.0 >= self.top + height {
            self.top = self.cursor.0 + 1 - height;
        }
        let gutter = (self.lines.len().to_string().len() + 1) as u16;
        let text_value = self.text();
        let ast = parse_notation_ast(&text_value);
        let tokens = emit_semantic_tokens(&ast, &text_value);
        let ground = theme.chrome();
        buffer.set_style(area, ground);
        let mut line_start = 0usize;
        let starts: Vec<usize> = self
            .lines
            .iter()
            .map(|l| {
                let s = line_start;
                line_start += l.len() + 1;
                s
            })
            .collect();
        for (row, index) in (self.top..self.lines.len()).take(height).enumerate() {
            let y = area.y + row as u16;
            buffer.set_string(
                area.x,
                y,
                format!("{:>width$} ", index + 1, width = gutter as usize - 1),
                theme.fg(Tone::Muted).patch(ground),
            );
            let line = &self.lines[index];
            let mut x = area.x + gutter;
            let mut byte = 0usize;
            for (col, g) in line.graphemes(true).enumerate() {
                let w = text::width(g) as u16;
                if x + w > area.x + area.width {
                    break;
                }
                // Tokens are positioned by line and UTF-16-agnostic character
                // index: count characters up to this grapheme.
                let char_index = line[..byte].chars().count();
                let token = tokens.iter().find(|t| {
                    t.line == index && char_index >= t.start_character && char_index < t.start_character + t.length
                });
                let mut style = match token.map(|t| t.token_type) {
                    Some(SemanticTokenType::Alias) => theme.fg(Tone::Accent),
                    Some(SemanticTokenType::Node) => theme.fg(Tone::Source),
                    Some(SemanticTokenType::CapPrefix) | Some(SemanticTokenType::MediaPrefix) => theme.fg(Tone::Muted),
                    Some(SemanticTokenType::TagKey) | Some(SemanticTokenType::KeywordIn) | Some(SemanticTokenType::KeywordOut) => {
                        theme.fg(Tone::Secondary)
                    }
                    Some(SemanticTokenType::TagValue) | Some(SemanticTokenType::String) => theme.fg(Tone::Target),
                    Some(SemanticTokenType::Constant) => theme.fg(Tone::Warning),
                    Some(SemanticTokenType::Policy) | Some(SemanticTokenType::Keyword) => {
                        theme.fg(Tone::Warning).add_modifier(Modifier::ITALIC)
                    }
                    Some(SemanticTokenType::Name) => theme.fg(Tone::Ink).add_modifier(Modifier::ITALIC),
                    Some(SemanticTokenType::Arrow) => theme.fg(Tone::Muted),
                    _ => theme.fg(Tone::Ink),
                }
                .patch(ground);
                let absolute = starts[index] + byte;
                if marked.iter().any(|(s, e)| absolute >= *s && absolute < *e) {
                    style = style.patch(theme.soft(Tone::Accent));
                }
                if focused && self.cursor == (index, col) {
                    style = style.add_modifier(Modifier::REVERSED);
                }
                buffer.set_string(x, y, g, style);
                x += w.max(1);
                byte += g.len();
            }
            if focused && self.cursor == (index, Self::graphemes(line).len()) && x < area.x + area.width {
                buffer.set_string(x, y, " ", Style::default().add_modifier(Modifier::REVERSED));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    // TEST13063: the source buffer edits by grapheme — a wide character is
    // one step of the cursor — joins and splits lines, and keeps the cursor's
    // byte offset in step with the text, so the graph can find what the
    // cursor is in.
    #[test]
    fn test13063_source_buffer_edits_by_grapheme_and_line() {
        let mut buffer = SourceBuffer::new("[a -> x -> b]\n[b -> y -> 日本]");
        let after_wide = "[a -> x -> b]\n[b -> y -> 日".len();
        buffer.set_cursor_byte(after_wide);
        assert_eq!(buffer.cursor(), (1, 12));
        assert_eq!(buffer.cursor_byte(), after_wide, "the offset counts the wide character's bytes");
        buffer.on_key(key(KeyCode::Backspace));
        assert_eq!(buffer.text(), "[a -> x -> b]\n[b -> y -> 本]");
        buffer.on_key(key(KeyCode::Home));
        buffer.on_key(key(KeyCode::Backspace));
        assert_eq!(buffer.text(), "[a -> x -> b][b -> y -> 本]", "backspace at a line's start joins it to the one above");
        assert_eq!(buffer.cursor_byte(), "[a -> x -> b]".len());
        buffer.on_key(key(KeyCode::Enter));
        buffer.paste("[c -> z -> d]\n");
        assert_eq!(buffer.text(), "[a -> x -> b]\n[c -> z -> d]\n[b -> y -> 本]");
        // Moving changes nothing; a typed key is not a move.
        let before = buffer.text();
        assert!(buffer.move_cursor(key(KeyCode::Up)));
        assert!(!buffer.move_cursor(key(KeyCode::Char('x'))));
        assert!(!buffer.move_cursor(key(KeyCode::Backspace)));
        assert_eq!(buffer.text(), before);
        assert_eq!(buffer.cursor(), (1, 0));
    }
}
