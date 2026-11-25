//! The terminal an inline surface draws on: stderr.
//!
//! The live view and the picker are drawn under the command on stderr, so
//! stdout stays the run's own, whether it is the terminal, a pipe or a file.
//! An inline viewport is placed where the cursor is, and the cursor is asked
//! of the terminal the viewport is drawn on: crossterm asks on stdout, which
//! puts the question into whatever stdout was redirected to and waits for an
//! answer that never comes. This backend is crossterm's in every other
//! respect.

use std::io::{self, Stderr, Write};

use ratatui::backend::{Backend, ClearType, CrosstermBackend, WindowSize};
use ratatui::buffer::Cell;
use ratatui::layout::{Position, Size};

/// How long the terminal is given to say where its cursor is: crossterm's
/// own allowance.
#[cfg(unix)]
const ANSWER_WITHIN: std::time::Duration = std::time::Duration::from_secs(2);

/// crossterm's backend on stderr, asking the cursor position on stderr.
pub struct InlineBackend {
    inner: CrosstermBackend<Stderr>,
}

impl InlineBackend {
    pub fn new() -> Self {
        Self {
            inner: CrosstermBackend::new(io::stderr()),
        }
    }
}

impl Backend for InlineBackend {
    type Error = io::Error;

    fn draw<'a, I>(&mut self, content: I) -> io::Result<()>
    where
        I: Iterator<Item = (u16, u16, &'a Cell)>,
    {
        self.inner.draw(content)
    }

    fn append_lines(&mut self, n: u16) -> io::Result<()> {
        self.inner.append_lines(n)
    }

    fn hide_cursor(&mut self) -> io::Result<()> {
        self.inner.hide_cursor()
    }

    fn show_cursor(&mut self) -> io::Result<()> {
        self.inner.show_cursor()
    }

    fn get_cursor_position(&mut self) -> io::Result<Position> {
        // What was drawn reaches the terminal before the question does.
        Backend::flush(&mut self.inner)?;
        cursor_position(&mut self.inner)
    }

    fn set_cursor_position<P: Into<Position>>(&mut self, position: P) -> io::Result<()> {
        self.inner.set_cursor_position(position)
    }

    fn clear(&mut self) -> io::Result<()> {
        self.inner.clear()
    }

    fn clear_region(&mut self, clear_type: ClearType) -> io::Result<()> {
        self.inner.clear_region(clear_type)
    }

    fn size(&self) -> io::Result<Size> {
        self.inner.size()
    }

    fn window_size(&mut self) -> io::Result<WindowSize> {
        self.inner.window_size()
    }

    fn flush(&mut self) -> io::Result<()> {
        Backend::flush(&mut self.inner)
    }
}

/// Where the cursor is, asked on `terminal` and answered on the terminal's
/// input: standard input when it is the terminal, `/dev/tty` otherwise, as
/// crossterm reads keys. Asked only in raw mode, where the answer arrives
/// without waiting for Enter. Nothing else reads the terminal meanwhile: the
/// inline surfaces read keys only between draws. A key typed in the instant
/// between the question and its answer is read with the answer and dropped.
#[cfg(unix)]
fn cursor_position(terminal: &mut impl Write) -> io::Result<Position> {
    use std::os::fd::AsRawFd;

    if !crossterm::terminal::is_raw_mode_enabled()? {
        return Err(io::Error::other(
            "the cursor position is asked only in raw mode, where the terminal's answer is not held for Enter",
        ));
    }
    let tty;
    let input = if io::IsTerminal::is_terminal(&io::stdin()) {
        libc::STDIN_FILENO
    } else {
        tty = std::fs::File::open("/dev/tty")?;
        tty.as_raw_fd()
    };
    terminal.write_all(b"\x1b[6n")?;
    terminal.flush()?;
    let deadline = std::time::Instant::now() + ANSWER_WITHIN;
    let mut said = Vec::new();
    loop {
        if let Some(position) = cursor_answer(&said) {
            return Ok(position);
        }
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        if left.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("the terminal did not say where its cursor is within {} s", ANSWER_WITHIN.as_secs()),
            ));
        }
        let mut ready = libc::pollfd { fd: input, events: libc::POLLIN, revents: 0 };
        let wait = libc::c_int::try_from(left.as_millis()).unwrap_or(libc::c_int::MAX).max(1);
        // SAFETY: `ready` is one valid pollfd for the duration of the call.
        match unsafe { libc::poll(&mut ready, 1, wait) } {
            -1 => {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
            0 => {}
            _ => {
                let mut byte = 0u8;
                // SAFETY: reads at most one byte into `byte`, which outlives the call.
                match unsafe { libc::read(input, (&mut byte as *mut u8).cast(), 1) } {
                    1 => said.push(byte),
                    0 => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "the terminal closed while saying where its cursor is")),
                    _ => {
                        let error = io::Error::last_os_error();
                        if error.kind() != io::ErrorKind::Interrupted {
                            return Err(error);
                        }
                    }
                }
            }
        }
    }
}

/// A Windows console is asked through the console API, not on a stream.
#[cfg(not(unix))]
fn cursor_position(_terminal: &mut impl Write) -> io::Result<Position> {
    let (x, y) = crossterm::cursor::position()?;
    Ok(Position { x, y })
}

/// The cursor position a terminal's answer (`ESC [ row ; column R`, counted
/// from 1) gives, once `said` ends with a whole answer; whatever came before
/// it is not part of it.
#[cfg(unix)]
fn cursor_answer(said: &[u8]) -> Option<Position> {
    let body = said.strip_suffix(b"R")?;
    let start = body.windows(2).rposition(|pair| pair == b"\x1b[")?;
    let (row, column) = std::str::from_utf8(&body[start + 2..]).ok()?.split_once(';')?;
    let number = |text: &str| -> Option<u16> {
        if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        text.parse::<u16>().ok()?.checked_sub(1)
    };
    Some(Position { x: number(column)?, y: number(row)? })
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    // TEST22114: the cursor position is read from the terminal's answer
    // alone, counted from 0, once the answer is whole: keys typed before it
    // are not part of it, and a partial or malformed answer gives none.
    #[test]
    fn test22114_the_cursor_is_read_from_a_whole_answer() {
        assert_eq!(cursor_answer(b"\x1b[1;1R"), Some(Position { x: 0, y: 0 }));
        assert_eq!(cursor_answer(b"\x1b[12;40R"), Some(Position { x: 39, y: 11 }));
        assert_eq!(cursor_answer(b"ab\x1b[A\x1b[7;3R"), Some(Position { x: 2, y: 6 }));
        assert_eq!(cursor_answer(b"\x1b[12;4"), None);
        assert_eq!(cursor_answer(b"\x1b[12R"), None);
        assert_eq!(cursor_answer(b"\x1b[;4R"), None);
        assert_eq!(cursor_answer(b"\x1b[0;4R"), None);
        assert_eq!(cursor_answer(b"\x1b[1;x4R"), None);
    }
}
