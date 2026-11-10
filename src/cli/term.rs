//! Who is on the other end: a person at a terminal, or a pipe.
//!
//! Every choice between a person's output and a program's is made here, once,
//! from what is attached and what was asked:
//!
//! - **Interactive** (a picker may open, the live view may draw) only when
//!   stdin and stderr are both terminals, `--json` is not given, `--no-input`
//!   is not given, and `CI` is not set. Otherwise nothing prompts: a choice
//!   that was not made is an error naming the flag that makes it, as gh does.
//! - **Colour** follows `--color auto|always|never`; `auto` colours a terminal
//!   and nothing else, and `NO_COLOR` turns it off (no-color.org), as cargo does.
//! - **Machine output** (`--json`) is JSON Lines on stdout: one record per line.

use std::io::IsTerminal;

/// `--color`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ColorChoice {
    /// Colour a terminal, and nothing else; `NO_COLOR` turns it off.
    Auto,
    Always,
    Never,
}

/// What is attached and what was asked, decided once per command.
#[derive(Debug, Clone)]
pub struct Term {
    /// A picker may open and the live view may draw.
    pub interactive: bool,
    /// stderr is a terminal: progress may redraw in place.
    pub stderr_is_terminal: bool,
    /// Records, not prose, on stdout.
    pub json: bool,
    /// How much a run says: -1 quiet, 0 normal, 1 info logs, 2 debug.
    pub verbosity: i8,
    pub style: Style,
}

impl Term {
    pub fn decide(json: bool, no_input: bool, color: ColorChoice, verbose: u8, quiet: bool) -> Self {
        let stdin_is_terminal = std::io::stdin().is_terminal();
        let stderr_is_terminal = std::io::stderr().is_terminal();
        let ci = std::env::var_os("CI").is_some_and(|value| !value.is_empty());
        let interactive = stdin_is_terminal && stderr_is_terminal && !json && !no_input && !ci;
        let colour = match color {
            ColorChoice::Always => true,
            ColorChoice::Never => false,
            ColorChoice::Auto => {
                stderr_is_terminal && std::env::var_os("NO_COLOR").map_or(true, |v| v.is_empty())
            }
        };
        Self {
            interactive,
            stderr_is_terminal,
            json,
            verbosity: if quiet { -1 } else { verbose.min(2) as i8 },
            style: Style {
                colour,
                unicode: crate::tui::theme::GlyphSet::from_environment() == crate::tui::theme::GlyphSet::Unicode,
            },
        }
    }
}

/// How words are marked: in colour on a terminal that wants it, plain
/// otherwise; with Unicode marks in a UTF-8 locale, ASCII otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Style {
    colour: bool,
    unicode: bool,
}

impl Style {
    /// No colour, Unicode marks.
    pub fn plain() -> Self {
        Self { colour: false, unicode: true }
    }

    pub fn is_colour(&self) -> bool {
        self.colour
    }

    /// A mark: the Unicode one, or its ASCII stand-in where the locale cannot
    /// show it.
    pub fn glyph(&self, unicode: &'static str, ascii: &'static str) -> &'static str {
        if self.unicode {
            unicode
        } else {
            ascii
        }
    }

    fn paint(&self, code: &str, text: &str) -> String {
        if self.colour {
            format!("\x1b[{code}m{text}\x1b[0m")
        } else {
            text.to_string()
        }
    }

    /// `error:`: bold red.
    pub fn error(&self, text: &str) -> String {
        self.paint("1;31", text)
    }

    /// `hint:`: bold cyan.
    pub fn hint(&self, text: &str) -> String {
        self.paint("1;36", text)
    }

    /// `warning:`: bold yellow.
    pub fn warning(&self, text: &str) -> String {
        self.paint("1;33", text)
    }

    /// A name a person types: a cap alias, a command.
    pub fn name(&self, text: &str) -> String {
        self.paint("1", text)
    }

    /// Something said in passing: a URN, a path, a count.
    pub fn dim(&self, text: &str) -> String {
        self.paint("2", text)
    }

    /// Success: green.
    pub fn good(&self, text: &str) -> String {
        self.paint("32", text)
    }

    /// Failure: red.
    pub fn bad(&self, text: &str) -> String {
        self.paint("31", text)
    }
}

/// Print one JSON record as a line on stdout.
pub fn emit_record(record: &serde_json::Value) {
    emit_line(&record.to_string());
}

/// Print one line of JSON, already written, on stdout.
pub fn emit_line(line: &str) {
    use std::io::Write;
    let mut stdout = std::io::stdout().lock();
    // A closed pipe ends the command the way it ends any Unix filter: there is
    // no one left to tell.
    if writeln!(stdout, "{line}").is_err() {
        std::process::exit(0);
    }
}
