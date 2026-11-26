//! A terminal for the command to draw on, read as a person reads it.
//!
//! The command is started on a pseudo-terminal whose far end this holds:
//! everything it draws is read through a terminal emulator, so what it shows
//! can be waited for and read as text; what a terminal is asked (where its
//! cursor is, what it is) is answered as a terminal answers it; and keys are
//! typed into it. What is drawn can also be passed on to another terminal as
//! it comes, for a person to watch.

use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Which of the command's streams are the terminal; the others are pipes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Attached {
    /// stdin, stdout and stderr.
    All,
    /// stdin and stderr, with stdout a pipe.
    StdinAndStderr,
}

/// Held while a terminal is opened and its command started: a command another
/// thread started meanwhile would hold the terminal open, and it would never
/// be seen to end.
static STARTING: Mutex<()> = Mutex::new(());

/// What the terminal shows.
struct Screen {
    parser: vt100::Parser,
    /// When something was last drawn.
    drawn_at: Instant,
}

/// The command running on a terminal.
pub struct Session {
    child: Child,
    keys: File,
    screen: Arc<Mutex<Screen>>,
    reader: Option<JoinHandle<()>>,
    stdout: Option<JoinHandle<Vec<u8>>>,
}

/// How a session ended.
pub struct Ended {
    pub code: Option<i32>,
    /// stdout, when it was a pipe.
    pub stdout: String,
    /// The screen at the end.
    pub screen: String,
}

impl Session {
    /// Start `command` on a new terminal of `columns` × `rows` as its
    /// controlling terminal, the streams `attached` names on it. What it
    /// draws is passed on to `watcher` as it comes.
    pub fn start(
        mut command: Command,
        attached: Attached,
        columns: u16,
        rows: u16,
        watcher: Option<Box<dyn Write + Send>>,
    ) -> Result<Session, String> {
        let starting = STARTING.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let (far, near) = open_pty(columns, rows)?;
        let end = |stream: &str| -> Result<Stdio, String> {
            near.try_clone()
                .map(Stdio::from)
                .map_err(|e| format!("the terminal could not be given as {stream}: {e}"))
        };
        command.stdin(end("stdin")?).stderr(end("stderr")?);
        match attached {
            Attached::All => command.stdout(end("stdout")?),
            Attached::StdinAndStderr => command.stdout(Stdio::piped()),
        };
        // SAFETY: only async-signal-safe calls, between fork and exec.
        unsafe {
            command.pre_exec(|| {
                // A session of its own, with the terminal as its controlling
                // terminal: what /dev/tty opens, and what a full-screen
                // surface reads its keys from.
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::ioctl(libc::STDIN_FILENO, libc::TIOCSCTTY as _, 0) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command.spawn().map_err(|e| format!("the command could not be started: {e}"))?;
        drop(command);
        drop(near);
        drop(starting);
        let stdout = child.stdout.take().map(|mut out| {
            std::thread::spawn(move || {
                let mut bytes = Vec::new();
                let _ = out.read_to_end(&mut bytes);
                bytes
            })
        });
        let far = File::from(far);
        let keys = far.try_clone().map_err(|e| format!("the terminal could not be typed on: {e}"))?;
        let answers = far.try_clone().map_err(|e| format!("the terminal could not answer: {e}"))?;
        let screen = Arc::new(Mutex::new(Screen { parser: vt100::Parser::new(rows, columns, 0), drawn_at: Instant::now() }));
        let reader = {
            let screen = screen.clone();
            std::thread::spawn(move || read_drawn(far, answers, screen, watcher))
        };
        Ok(Session { child, keys, screen, reader: Some(reader), stdout })
    }

    /// The screen, as text.
    pub fn contents(&self) -> String {
        self.screen.lock().expect("the screen's lock is poisoned").parser.screen().contents()
    }

    /// The screen once nothing has been drawn on it for a moment: a frame whole.
    pub fn settled(&self) -> String {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let quiet = self.screen.lock().expect("the screen's lock is poisoned").drawn_at.elapsed();
            if quiet >= Duration::from_millis(300) || Instant::now() >= deadline {
                return self.contents();
            }
            std::thread::sleep(Duration::from_millis(30));
        }
    }

    /// Wait until the screen shows `text`.
    pub fn wait_for(&mut self, text: &str, timeout: Duration) -> Result<(), String> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.contents().contains(text) {
                return Ok(());
            }
            if let Some(status) = self.child.try_wait().map_err(|e| e.to_string())? {
                return Err(format!("the command ended ({status}) before it showed '{text}'\n{}", self.contents()));
            }
            if Instant::now() >= deadline {
                return Err(format!("the command did not show '{text}' within {timeout:?}\n{}", self.contents()));
            }
            std::thread::sleep(Duration::from_millis(30));
        }
    }

    /// Type `bytes`, as keys are typed.
    pub fn type_bytes(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.keys.write_all(bytes).and_then(|()| self.keys.flush()).map_err(|e| format!("the terminal could not be typed on: {e}"))
    }

    /// A handle to type on from another thread.
    pub fn typist(&self) -> Result<File, String> {
        self.keys.try_clone().map_err(|e| format!("the terminal could not be typed on: {e}"))
    }

    /// Wait for the command to end, and say how it ended.
    pub fn finish(mut self, timeout: Duration) -> Result<Ended, String> {
        let deadline = Instant::now() + timeout;
        let status = loop {
            if let Some(status) = self.child.try_wait().map_err(|e| e.to_string())? {
                break status;
            }
            if Instant::now() >= deadline {
                // Ended by its own handle: the process this session started.
                let _ = self.child.kill();
                let _ = self.child.wait();
                return Err(format!("the command did not end within {timeout:?}\n{}", self.contents()));
            }
            std::thread::sleep(Duration::from_millis(30));
        };
        let stdout = self.stdout.take().map(|reader| reader.join().unwrap_or_default()).unwrap_or_default();
        // What was drawn up to the command's end is read once nothing holds
        // the terminal's near end.
        if let Some(reader) = self.reader.take() {
            let grace = Instant::now() + Duration::from_secs(5);
            while !reader.is_finished() && Instant::now() < grace {
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        Ok(Ended { code: status.code(), stdout: String::from_utf8_lossy(&stdout).into_owned(), screen: self.contents() })
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        // A session given up on leaves no process behind: the one it started.
        if let Ok(None) = self.child.try_wait() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// A new pseudo-terminal, closed in every process started from here: its far
/// end and its near end.
fn open_pty(columns: u16, rows: u16) -> Result<(OwnedFd, OwnedFd), String> {
    let mut far: libc::c_int = -1;
    let mut near: libc::c_int = -1;
    let mut size = libc::winsize { ws_row: rows, ws_col: columns, ws_xpixel: 0, ws_ypixel: 0 };
    // SAFETY: openpty writes the two descriptors it opens; the size is read.
    let opened = unsafe { libc::openpty(&mut far, &mut near, std::ptr::null_mut(), std::ptr::null_mut(), &mut size) };
    if opened != 0 {
        return Err(format!("a terminal could not be opened: {}", std::io::Error::last_os_error()));
    }
    // SAFETY: both were just opened by openpty and are owned here.
    let (far, near) = unsafe { (OwnedFd::from_raw_fd(far), OwnedFd::from_raw_fd(near)) };
    for fd in [&far, &near] {
        // SAFETY: fd is open; F_SETFD sets its descriptor flags.
        if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
            return Err(format!("a terminal could not be kept to this process: {}", std::io::Error::last_os_error()));
        }
    }
    Ok((far, near))
}

/// Read what is drawn into the screen, passing it on to `watcher`, and answer
/// what a terminal is asked: where its cursor is, and what it is (one that
/// speaks the legacy keyboard protocol).
fn read_drawn(mut far: File, mut answers: File, screen: Arc<Mutex<Screen>>, mut watcher: Option<Box<dyn Write + Send>>) {
    const CURSOR: &[u8] = b"\x1b[6n";
    const WHAT: &[u8] = b"\x1b[c";
    let mut buffer = [0u8; 8192];
    let mut pending: Vec<u8> = Vec::new();
    loop {
        let read = match far.read(&mut buffer) {
            Ok(0) | Err(_) => return,
            Ok(read) => read,
        };
        if let Some(watcher) = watcher.as_mut() {
            let _ = watcher.write_all(&buffer[..read]);
            let _ = watcher.flush();
        }
        pending.extend_from_slice(&buffer[..read]);
        let mut screen = screen.lock().expect("the screen's lock is poisoned");
        // A question is answered once what was drawn before it is on the
        // screen, so the cursor it is told is the one that left.
        loop {
            let asked = [CURSOR, WHAT]
                .into_iter()
                .filter_map(|question| find(&pending, question).map(|at| (at, question)))
                .min_by_key(|(at, _)| *at);
            let Some((at, question)) = asked else { break };
            screen.parser.process(&pending[..at + question.len()]);
            pending.drain(..at + question.len());
            let answer = if question == CURSOR {
                let (row, column) = screen.parser.screen().cursor_position();
                format!("\x1b[{};{}R", row + 1, column + 1).into_bytes()
            } else {
                b"\x1b[?1;2c".to_vec()
            };
            if answers.write_all(&answer).is_err() {
                return;
            }
        }
        // A question can arrive split: keep a tail that could begin one.
        let keep = match pending.iter().rposition(|&byte| byte == 0x1b) {
            Some(at) if pending.len() - at < CURSOR.len() => pending.len() - at,
            _ => 0,
        };
        let drawable = pending.len() - keep;
        screen.parser.process(&pending[..drawable]);
        pending.drain(..drawable);
        screen.drawn_at = Instant::now();
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|window| window == needle)
}
