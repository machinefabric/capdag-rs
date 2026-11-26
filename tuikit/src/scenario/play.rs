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
        // What the command keeps in a temporary directory is kept in the
        // journey's, and goes with it.
        command.args(&journey.args).current_dir(dir).env("TMPDIR", dir);
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

/// What `played`, in `dir`, should have come to, and does not; nothing when
/// it did.
pub fn unmet(expect: &Expect, played: &Played, dir: &Path) -> Vec<String> {
    let mut unmet = Vec::new();
    if let Some(screen) = &expect.screen {
        // The command names the directory as it resolves it.
        let dir = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
        let expected: Vec<String> = screen.iter().map(|line| line.replace("{dir}", &dir.display().to_string())).collect();
        let shown = screen_lines(&played.shown);
        let same = shown.len() == expected.len() && shown.iter().zip(&expected).all(|(line, pattern)| line_matches(line, pattern));
        if !same {
            unmet.push(format!(
                "the screen is not what it should be\n--- expected ---\n{}\n--- on the screen ---\n{}",
                expected.join("\n"),
                shown.join("\n")
            ));
        }
    }
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

/// A screen's lines, as a person copies them: from the top row to the last
/// row with text, each without the spaces at its end.
fn screen_lines(screen: &str) -> Vec<String> {
    let mut lines: Vec<String> = screen.lines().map(|line| line.trim_end().to_string()).collect();
    while lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }
    lines
}

/// Whether `line` is `pattern`, where each `{*}` in the pattern stands for any
/// text: how long a run took, which no journey can say.
fn line_matches(line: &str, pattern: &str) -> bool {
    let mut pieces = pattern.split("{*}");
    let first = pieces.next().unwrap_or("");
    let Some(mut rest) = line.strip_prefix(first) else { return false };
    let pieces: Vec<&str> = pieces.collect();
    let Some((last, middle)) = pieces.split_last() else { return rest.is_empty() };
    for piece in middle {
        match rest.find(piece) {
            Some(at) => rest = &rest[at + piece.len()..],
            None => return false,
        }
    }
    rest.ends_with(last)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn played(shown: &str) -> Played {
        Played { code: Some(0), stdout: String::new(), shown: shown.to_string() }
    }

    fn expecting(screen: &[&str]) -> Expect {
        Expect {
            exit: 0,
            stdout: Vec::new(),
            shown: Vec::new(),
            absent: Vec::new(),
            screen: Some(screen.iter().map(|line| line.to_string()).collect()),
        }
    }

    // TEST22289: a screen is met only by every line from the top, blank rows
    // and where each line starts included, with spaces at a line's end and
    // the empty rows under the last line aside, {dir} the journey's directory
    // and {*} any text.
    #[test]
    fn test22289_a_screen_is_met_line_for_line() {
        let dir = tempfile::tempdir().unwrap();
        let canonical = dir.path().canonicalize().unwrap();
        let expect = expecting(&["✓ done", "  → {dir}/a.txt"]);
        let exact = format!("✓ done   \n  → {}/a.txt\n\n\n", canonical.display());
        assert!(unmet(&expect, &played(&exact), dir.path()).is_empty());
        let blank_row = format!("\n✓ done\n  → {}/a.txt", canonical.display());
        assert_eq!(unmet(&expect, &played(&blank_row), dir.path()).len(), 1);
        let moved = format!("✓ done\n        → {}/a.txt", canonical.display());
        assert_eq!(unmet(&expect, &played(&moved), dir.path()).len(), 1);
        let gap = format!("✓ done\n\n  → {}/a.txt", canonical.display());
        assert_eq!(unmet(&expect, &played(&gap), dir.path()).len(), 1);
        let timed = expecting(&["✓ report.pdf  1 step · {*} s"]);
        assert!(unmet(&timed, &played("✓ report.pdf  1 step · 0.4 s"), dir.path()).is_empty());
        assert_eq!(unmet(&timed, &played("✓ report.pdf  1 step · 0.4 s · failed"), dir.path()).len(), 1);
        assert_eq!(unmet(&timed, &played("✗ report.pdf  1 step · 0.4 s"), dir.path()).len(), 1);
    }
}
