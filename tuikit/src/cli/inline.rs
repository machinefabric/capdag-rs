//! The terminal an inline surface draws on: stderr.
//!
//! The live view and the picker are drawn under the command on stderr, so
//! stdout stays the run's own, whether it is the terminal, a pipe or a file.
//! An inline viewport is placed where the cursor is, and the cursor is asked
//! of the terminal the viewport is drawn on: crossterm asks on stdout, which
//! puts the question into whatever stdout was redirected to and waits for an
//! answer that never comes. This backend is crossterm's in every other
//! respect.
//!
//! A [`Surface`] is a block drawn there, and leaves the terminal as it found
//! it: it takes only the rows it shows; text printed while it is up goes
//! above it as ordinary text, which the terminal wraps and a person copies
//! whole; when it closes, what comes next is printed from the row it began
//! on, so it leaves no blank rows; and the terminal is given back — out of
//! raw mode, its cursor shown — whether it closes, is dropped on an error, or
//! the command panics.

use std::io::{self, Stderr, Write};
use std::sync::atomic::{AtomicBool, Ordering};

use ratatui::backend::{Backend, ClearType, CrosstermBackend, WindowSize};
use ratatui::buffer::Cell;
use ratatui::layout::{Position, Size};
use ratatui::{Frame, Terminal, TerminalOptions, Viewport};

/// Which stream a line printed above a surface goes to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stream {
    Stdout,
    Stderr,
}

/// Whether a surface holds the terminal: what a panic gives back.
static HELD: AtomicBool = AtomicBool::new(false);

/// A block drawn inline under the command, on stderr.
pub struct Surface {
    terminal: Terminal<InlineBackend>,
    /// The rows it takes.
    height: u16,
    /// Closed: the terminal is given back, and dropping it does nothing more.
    closed: bool,
}

impl Surface {
    /// Take the terminal and open a block of `height` rows (no more than the
    /// terminal has) under what is already there, on a row of its own.
    pub fn open(height: u16) -> io::Result<Self> {
        install_panic_hook();
        crossterm::terminal::enable_raw_mode()?;
        HELD.store(true, Ordering::SeqCst);
        match Self::block(height) {
            Ok(terminal) => Ok(Self { terminal, height, closed: false }),
            Err(e) => {
                give_back();
                Err(e)
            }
        }
    }

    /// A block of `height` rows from the cursor's row, the cursor moved to a
    /// row of its own first when a line was left unfinished.
    fn block(height: u16) -> io::Result<Terminal<InlineBackend>> {
        let mut backend = InlineBackend::new();
        let rows = backend.size()?.height.max(1);
        if backend.get_cursor_position()?.x > 0 {
            backend.inner.write_all(b"\r\n")?;
        }
        let mut terminal = Terminal::with_options(backend, TerminalOptions { viewport: Viewport::Inline(height.clamp(1, rows)) })?;
        terminal.hide_cursor()?;
        Ok(terminal)
    }

    /// The rows it takes.
    pub fn height(&self) -> u16 {
        self.height
    }

    /// The columns the terminal has.
    pub fn width(&self) -> io::Result<u16> {
        Ok(self.terminal.size()?.width)
    }

    /// Draw one frame.
    pub fn draw(&mut self, render: impl FnOnce(&mut Frame)) -> io::Result<()> {
        self.terminal.draw(render).map(|_| ())
    }

    /// Take `height` rows from now on: the block is opened again, as tall,
    /// where it began.
    pub fn resize(&mut self, height: u16) -> io::Result<()> {
        if height == self.height {
            return Ok(());
        }
        self.clear()?;
        self.terminal = Self::block(height)?;
        self.height = height;
        Ok(())
    }

    /// Print `text` above the block, as a line to `stream` would print
    /// without it: the block is cleared, the text printed from the row it
    /// began on, and the block opened again under it.
    pub fn print_above(&mut self, stream: Stream, text: &[u8]) -> io::Result<()> {
        self.clear()?;
        // Out of raw mode, the text is printed as any line is: a newline is
        // a new line, and the terminal wraps what is wider than it.
        crossterm::terminal::disable_raw_mode()?;
        let printed = print(stream, text);
        crossterm::terminal::enable_raw_mode()?;
        printed?;
        self.terminal = Self::block(self.height)?;
        Ok(())
    }

    /// Ctrl-Z: give the terminal back, stop this process as the shell asks,
    /// and open the block again when the shell continues it.
    #[cfg(unix)]
    pub fn suspend(&mut self) -> io::Result<()> {
        self.clear()?;
        self.terminal.show_cursor()?;
        crossterm::terminal::disable_raw_mode()?;
        // SAFETY: raising a signal on the current process has no memory
        // effects; SIGTSTP's default action stops the process until SIGCONT.
        unsafe {
            libc::raise(libc::SIGTSTP);
        }
        crossterm::terminal::enable_raw_mode()?;
        self.terminal = Self::block(self.height)?;
        Ok(())
    }

    /// A Windows console has no job control: there is nothing to stop to.
    #[cfg(not(unix))]
    pub fn suspend(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// Clear the block and put the cursor on the row it began on: what is
    /// printed next is printed there.
    fn clear(&mut self) -> io::Result<()> {
        // ratatui puts the cursor back where the last frame left it, inside
        // the cleared block.
        self.terminal.clear()?;
        let top = self.terminal.get_frame().area().as_position();
        self.terminal.set_cursor_position(top)?;
        Backend::flush(self.terminal.backend_mut())
    }

    /// Clear the block and give the terminal back.
    pub fn close(mut self) -> io::Result<()> {
        let cleared = self.clear();
        give_back();
        self.closed = true;
        cleared
    }
}

impl Drop for Surface {
    /// A surface dropped on an error is closed all the same.
    fn drop(&mut self) {
        if !self.closed {
            let _ = self.clear();
            give_back();
        }
    }
}

/// Print `text` on `stream`.
fn print(stream: Stream, text: &[u8]) -> io::Result<()> {
    match stream {
        Stream::Stdout => {
            let mut out = io::stdout().lock();
            out.write_all(text)?;
            out.flush()
        }
        Stream::Stderr => {
            let mut err = io::stderr().lock();
            err.write_all(text)?;
            err.flush()
        }
    }
}

/// Out of raw mode, the cursor shown: the terminal as the shell left it,
/// if a surface held it.
pub fn give_back() {
    if HELD.swap(false, Ordering::SeqCst) {
        let _ = crossterm::execute!(io::stderr(), crossterm::cursor::Show);
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

/// A panic gives the terminal back before it reports, so the report is
/// readable and the shell usable.
fn install_panic_hook() {
    static INSTALLED: std::sync::Once = std::sync::Once::new();
    INSTALLED.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            give_back();
            previous(info);
        }));
    });
}

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
