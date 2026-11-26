//! A journey: one thing a person does with the capdag command, in a world —
//! the command line, and in each mode it is run in, the keys typed and what
//! it shows. Declared once (`scenarios/journeys/CJRN-nnn.json`), it is played
//! for a person by `capdag-tuikit journey` and checked by the journey tests.

use std::collections::BTreeMap;

use serde::Deserialize;

use super::world::{names_in, Scenarios};

/// A journey, as its file declares it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Journey {
    /// What a person does, in a sentence.
    pub title: String,
    /// The world it is done in.
    pub world: String,
    /// The capdag command line, after `capdag`.
    pub args: Vec<String>,
    /// Each mode it is run in, and how it goes there.
    pub modes: BTreeMap<Mode, Leg>,
}

/// How the command is attached, as a person or a script runs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// stdin, stdout and stderr a terminal: `capdag view`, `capdag edit`.
    FullScreen,
    /// stdin and stderr a terminal, stdout a pipe: the live view and the
    /// pickers draw inline.
    Inline,
    /// Pipes, as a script or CI runs it.
    Plain,
    /// Pipes, with `--json`.
    Json,
}

impl Mode {
    pub const ALL: [Mode; 4] = [Mode::FullScreen, Mode::Inline, Mode::Plain, Mode::Json];

    pub fn name(self) -> &'static str {
        match self {
            Mode::FullScreen => "full_screen",
            Mode::Inline => "inline",
            Mode::Plain => "plain",
            Mode::Json => "json",
        }
    }

    pub fn named(name: &str) -> Result<Mode, String> {
        Mode::ALL
            .into_iter()
            .find(|mode| mode.name() == name)
            .ok_or_else(|| format!("'{name}' is no mode; the modes are full_screen, inline, plain and json"))
    }

    /// Whether the command draws on a terminal in this mode.
    pub fn on_terminal(self) -> bool {
        matches!(self, Mode::FullScreen | Mode::Inline)
    }
}

/// The journey in one mode.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Leg {
    /// What is done at the terminal, in order.
    #[serde(default)]
    pub keys: Vec<Step>,
    /// What it comes to.
    pub expect: Expect,
}

/// One thing done at the terminal.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Step {
    /// Wait until the screen shows this text.
    Wait(String),
    /// Press a key: a character, or one of Enter, Esc, Tab, Backspace, Up,
    /// Down, Left, Right, PageUp, PageDown, Home, End, Space, Ctrl-<letter>.
    Press(String),
    /// Type this text.
    Type(String),
}

/// What a journey comes to in one mode.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expect {
    /// The command's exit status.
    pub exit: i32,
    /// Texts stdout holds.
    #[serde(default)]
    pub stdout: Vec<String>,
    /// Texts stderr holds (on pipes), or the terminal shows by the end (on a
    /// terminal).
    #[serde(default)]
    pub shown: Vec<String>,
    /// Texts nothing holds: neither stdout nor what is shown.
    #[serde(default)]
    pub absent: Vec<String>,
}

/// The bytes a terminal sends for `key`.
pub fn key_bytes(key: &str) -> Result<Vec<u8>, String> {
    let named: &[(&str, &str)] = &[
        ("Enter", "\r"),
        ("Esc", "\x1b"),
        ("Tab", "\t"),
        ("Backspace", "\x7f"),
        ("Up", "\x1b[A"),
        ("Down", "\x1b[B"),
        ("Right", "\x1b[C"),
        ("Left", "\x1b[D"),
        ("Home", "\x1b[H"),
        ("End", "\x1b[F"),
        ("PageUp", "\x1b[5~"),
        ("PageDown", "\x1b[6~"),
        ("Space", " "),
    ];
    if let Some((_, bytes)) = named.iter().find(|(name, _)| *name == key) {
        return Ok(bytes.as_bytes().to_vec());
    }
    if let Some(letter) = key.strip_prefix("Ctrl-") {
        let mut chars = letter.chars();
        if let (Some(c), None) = (chars.next(), chars.next()) {
            if c.is_ascii_alphabetic() {
                return Ok(vec![(c.to_ascii_lowercase() as u8) & 0x1f]);
            }
        }
    }
    let mut chars = key.chars();
    match (chars.next(), chars.next()) {
        (Some(c), None) => Ok(c.to_string().into_bytes()),
        _ => Err(format!("'{key}' is no key a journey can press")),
    }
}

impl Scenarios {
    /// The journeys' ids, in order.
    pub fn journeys(&self) -> Result<Vec<String>, String> {
        names_in(&self.dir.join("journeys"))
    }

    /// The journey `id`.
    pub fn journey(&self, id: &str) -> Result<Journey, String> {
        let path = self.dir.join("journeys").join(format!("{id}.json"));
        let text = std::fs::read_to_string(&path).map_err(|e| {
            let known = self.journeys().map(|ids| ids.join(", ")).unwrap_or_default();
            format!("there is no journey '{id}' ({}: {e}); the journeys are {known}", path.display())
        })?;
        let journey: Journey =
            serde_json::from_str(&text).map_err(|e| format!("the journey '{id}' ({}) is not valid: {e}", path.display()))?;
        for leg in journey.modes.values() {
            for step in &leg.keys {
                if let Step::Press(key) = step {
                    key_bytes(key).map_err(|e| format!("the journey '{id}': {e}"))?;
                }
            }
        }
        Ok(journey)
    }
}
