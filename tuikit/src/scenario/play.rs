//! Playing a journey: the capdag command run on its world in one mode, on a
//! terminal or on pipes as the mode says, the journey's keys typed into it,
//! and what it comes to checked against what the journey expects.
//!
//! The command is this binary on the world (`capdag-tuikit --world …`), in a
//! process of its own, so every mode is the command as a person or a script
//! meets it: its own terminal, or its own pipes. A person can watch a journey
//! played, and play it a key at a time.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use super::journey::{key_bytes, Expect, Journey, Leg, Mode, Step};
use super::terminal::{Attached, Session};
use super::world::Scenarios;

/// The terminal a journey is checked on: wide and tall enough that no line it
/// looks for wraps.
pub const COLUMNS: u16 = 100;
pub const ROWS: u16 = 32;

/// How long a journey may wait for what it waits for, and for the command to
/// end.
const PATIENCE: Duration = Duration::from_secs(60);

/// What a journey came to in one mode.
#[derive(Debug, Clone)]
pub struct Played {
    pub code: Option<i32>,
    pub stdout: String,
    /// stderr on pipes; the screen at the end on a terminal.
    pub shown: String,
}

/// Plays journeys with a capdag-tuikit binary.
pub struct Player {
    /// The capdag-tuikit binary.
    pub exe: PathBuf,
    pub scenarios: Scenarios,
    /// How many times as fast as their scripts' waits runs go.
    pub speed: f64,
}

/// A person watching a journey played, on this process's terminal.
pub struct Watching {
    /// Wait for Space before each key the journey presses.
    pub step: bool,
}

impl Player {
    /// The command line that runs `journey` in `mode`: this binary on the
    /// world, in `dir`.
    fn command(&self, journey: &Journey, mode: Mode, dir: &Path) -> Command {
        let mut command = Command::new(&self.exe);
        command
            .arg("--scenarios")
            .arg(&self.scenarios.dir)
            .args(["--world", &journey.world, "--speed", &self.speed.to_string(), "--"]);
        if mode == Mode::Json {
            command.arg("--json");
        }
        command.args(&journey.args).current_dir(dir);
        if mode.on_terminal() {
            // A person's terminal: no CI to make the command keep to plain lines.
            command.env_remove("CI").env("TERM", "xterm-256color");
        } else {
            command.env("CI", "1");
        }
        command
    }

    /// Play `journey` in `mode` in `dir`, the world's files written there.
    pub fn play(&self, journey: &Journey, mode: Mode, dir: &Path, watching: Option<Watching>) -> Result<Played, String> {
        let leg = journey
            .modes
            .get(&mode)
            .ok_or_else(|| format!("the journey is not played in {} mode", mode.name()))?;
        let world = self.scenarios.world(&journey.world)?;
        world.write_files(dir)?;
        let command = self.command(journey, mode, dir);
        match mode {
            Mode::Plain | Mode::Json => on_pipes(command, watching.is_some()),
            Mode::FullScreen | Mode::Inline => {
                let attached = if mode == Mode::FullScreen { Attached::All } else { Attached::StdinAndStderr };
                on_terminal(command, attached, leg, watching)
            }
        }
    }
}

fn on_pipes(mut command: Command, watched: bool) -> Result<Played, String> {
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().map_err(|e| format!("the command could not be started: {e}"))?;
    let pass = |mut from: Box<dyn Read + Send>, to_stdout: bool| {
        std::thread::spawn(move || {
            let mut kept = Vec::new();
            let mut buffer = [0u8; 4096];
            while let Ok(read) = from.read(&mut buffer) {
                if read == 0 {
                    break;
                }
                kept.extend_from_slice(&buffer[..read]);
                if watched {
                    let _ = if to_stdout {
                        std::io::stdout().write_all(&buffer[..read])
                    } else {
                        std::io::stderr().write_all(&buffer[..read])
                    };
                }
            }
            kept
        })
    };
    let stdout = pass(Box::new(child.stdout.take().expect("stdout is a pipe")), true);
    let stderr = pass(Box::new(child.stderr.take().expect("stderr is a pipe")), false);
    let status = child.wait().map_err(|e| format!("the command could not be waited for: {e}"))?;
    let stdout = stdout.join().unwrap_or_default();
    let stderr = stderr.join().unwrap_or_default();
    Ok(Played {
        code: status.code(),
        stdout: String::from_utf8_lossy(&stdout).into_owned(),
        shown: String::from_utf8_lossy(&stderr).into_owned(),
    })
}

/// Whether the journey may go on to its next key: what a person presses Space
/// for when they step through it.
type Gate = Arc<(Mutex<bool>, Condvar)>;

fn on_terminal(command: Command, attached: Attached, leg: &Leg, watching: Option<Watching>) -> Result<Played, String> {
    let (columns, rows) = match &watching {
        Some(_) => crossterm::terminal::size().map_err(|e| format!("this terminal's size could not be read: {e}"))?,
        None => (COLUMNS, ROWS),
    };
    let watcher: Option<Box<dyn Write + Send>> = watching.as_ref().map(|_| Box::new(std::io::stdout()) as _);
    let mut session = Session::start(command, attached, columns, rows, watcher)?;
    let step = watching.as_ref().is_some_and(|w| w.step);
    let gate: Gate = Arc::new((Mutex::new(false), Condvar::new()));
    let raw = watching.is_some();
    if raw {
        crossterm::terminal::enable_raw_mode().map_err(|e| format!("this terminal could not be taken: {e}"))?;
        // What the person types goes to the command; in a step, Space lets the
        // journey's next key go instead.
        let mut typist = session.typist()?;
        let gate = gate.clone();
        std::thread::spawn(move || {
            let mut stdin = std::io::stdin();
            let mut byte = [0u8; 1];
            while let Ok(1) = stdin.read(&mut byte) {
                let (open, waiting) = &*gate;
                let mut opened = open.lock().expect("the step's lock is poisoned");
                if step && byte[0] == b' ' && !*opened {
                    *opened = true;
                    waiting.notify_all();
                    continue;
                }
                drop(opened);
                if typist.write_all(&byte).is_err() {
                    return;
                }
            }
        });
    }
    let played = (|| {
        for key in &leg.keys {
            match key {
                Step::Wait(text) => session.wait_for(text, PATIENCE)?,
                Step::Press(name) => {
                    if step {
                        let (open, waiting) = &*gate;
                        let mut opened = open.lock().expect("the step's lock is poisoned");
                        while !*opened {
                            opened = waiting.wait(opened).expect("the step's lock is poisoned");
                        }
                        *opened = false;
                    }
                    session.type_bytes(&key_bytes(name)?)?;
                    std::thread::sleep(Duration::from_millis(120));
                }
                Step::Type(text) => {
                    session.type_bytes(text.as_bytes())?;
                    std::thread::sleep(Duration::from_millis(120));
                }
            }
        }
        let ended = session.finish(if raw { Duration::from_secs(24 * 3600) } else { PATIENCE * 2 })?;
        Ok(Played { code: ended.code, stdout: ended.stdout, shown: ended.screen })
    })();
    if raw {
        let _ = crossterm::terminal::disable_raw_mode();
    }
    played
}

/// What `played` should have come to, and does not; nothing when it did.
pub fn unmet(expect: &Expect, played: &Played) -> Vec<String> {
    let mut unmet = Vec::new();
    if played.code != Some(expect.exit) {
        unmet.push(format!("it exited {:?}, not {}", played.code, expect.exit));
    }
    for text in &expect.stdout {
        if !played.stdout.contains(text.as_str()) {
            unmet.push(format!("stdout does not hold '{text}'"));
        }
    }
    for text in &expect.shown {
        if !played.shown.contains(text.as_str()) {
            unmet.push(format!("'{text}' is not shown"));
        }
    }
    for text in &expect.absent {
        if played.stdout.contains(text.as_str()) || played.shown.contains(text.as_str()) {
            unmet.push(format!("'{text}' is there, and should not be"));
        }
    }
    unmet
}
