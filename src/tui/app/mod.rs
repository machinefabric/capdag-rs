//! The interactive surfaces, and the terminal they draw on.
//!
//! [`Tui`] owns the terminal for as long as a surface is shown: the
//! alternate screen, raw mode, mouse capture, bracketed paste, and — where
//! the terminal speaks it — the kitty keyboard protocol, so keys arrive
//! unambiguous. Every frame is drawn inside a synchronized update (DEC mode
//! 2026): the terminal shows the frame whole or not at all, never half
//! drawn. ratatui diffs each frame against the last, so only cells that
//! changed are written. Everything turned on is turned off again when the
//! [`Tui`] is dropped — on a panic too, and around a suspend (Ctrl-Z).
//!
//! The surfaces: [`edit`] (a machine, drawn as its graph and edited
//! structurally — or only looked at, read-only, by `capdag view`). A running
//! machine is watched inline instead (`crate::cli::live`), in the normal
//! scrollback.

pub mod edit;
pub mod graph;
pub mod palette;
pub mod source;

use std::io::{self, Stdout, Write};
use std::sync::atomic::{AtomicBool, Ordering};

use crossterm::event::{
    DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
    KeyboardEnhancementFlags, PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, BeginSynchronizedUpdate, EndSynchronizedUpdate,
    EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::{execute, queue};
use ratatui::backend::CrosstermBackend;
use ratatui::{Frame, Terminal};

use crate::tui::theme::{Appearance, ColorDepth, GlyphSet, Theme};

/// How the interface looks, as the command line chose.
#[derive(Debug, Clone, Copy)]
pub struct TuiSettings {
    pub appearance: Appearance,
    pub glyphs: GlyphSet,
    pub depth: ColorDepth,
}

impl TuiSettings {
    pub fn theme(&self) -> Theme {
        Theme::new(self.appearance, self.depth, self.glyphs)
    }
}

/// Why the terminal interface could not be shown, or stopped.
#[derive(Debug, thiserror::Error)]
pub enum TuiError {
    #[error("the terminal: {0}")]
    Terminal(#[from] io::Error),
    /// What is drawn contradicts what it is drawn from: a defect upstream,
    /// never drawn around.
    #[error("{0}")]
    Inconsistent(String),
}

/// Whether the terminal is ours right now: what the panic hook and the
/// restore path check, so the terminal is given back exactly once.
static ACTIVE: AtomicBool = AtomicBool::new(false);
/// Whether this terminal took the kitty keyboard flags (and so must pop them).
static ENHANCED_KEYS: AtomicBool = AtomicBool::new(false);

fn enter(stdout: &mut Stdout) -> io::Result<()> {
    enable_raw_mode()?;
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture, EnableBracketedPaste)?;
    // Keys the legacy encoding cannot tell apart (Esc from Alt, Ctrl-I from
    // Tab) arrive distinct where the terminal speaks the kitty protocol. A
    // terminal that does not answer the query is left as it is.
    if crossterm::terminal::supports_keyboard_enhancement().unwrap_or(false) {
        execute!(
            stdout,
            PushKeyboardEnhancementFlags(KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES)
        )?;
        ENHANCED_KEYS.store(true, Ordering::SeqCst);
    }
    ACTIVE.store(true, Ordering::SeqCst);
    Ok(())
}

/// Give the terminal back as it was. Safe to call more than once.
fn leave() -> io::Result<()> {
    if !ACTIVE.swap(false, Ordering::SeqCst) {
        return Ok(());
    }
    let mut stdout = io::stdout();
    if ENHANCED_KEYS.swap(false, Ordering::SeqCst) {
        execute!(stdout, PopKeyboardEnhancementFlags)?;
    }
    execute!(
        stdout,
        DisableBracketedPaste,
        DisableMouseCapture,
        LeaveAlternateScreen,
        crossterm::cursor::Show
    )?;
    disable_raw_mode()
}

/// The terminal, while a surface is shown on it.
pub struct Tui {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl Tui {
    /// Take the terminal. Standard input and output must both be terminals:
    /// the caller checks that before anything runs, so a run never starts
    /// for a view that cannot be shown.
    pub fn enter() -> Result<Self, TuiError> {
        let mut stdout = io::stdout();
        enter(&mut stdout)?;
        // A panic gives the terminal back before it reports, so the report is
        // readable and the shell usable.
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let _ = leave();
            previous(info);
        }));
        let mut terminal = Terminal::new(CrosstermBackend::new(stdout))?;
        terminal.clear()?;
        Ok(Self { terminal })
    }

    /// Draw one frame, whole: inside a synchronized update.
    pub fn draw(&mut self, render: impl FnOnce(&mut Frame)) -> Result<(), TuiError> {
        queue!(self.terminal.backend_mut(), BeginSynchronizedUpdate)?;
        let drawn = self.terminal.draw(render).map(|_| ());
        queue!(self.terminal.backend_mut(), EndSynchronizedUpdate)?;
        self.terminal.backend_mut().flush()?;
        drawn.map_err(TuiError::from)
    }

    /// Ctrl-Z: give the terminal back, stop this process as the shell asked,
    /// and take the terminal again when the shell continues it.
    #[cfg(unix)]
    pub fn suspend(&mut self) -> Result<(), TuiError> {
        leave()?;
        // SAFETY: raising a signal on the current process has no memory
        // effects; SIGTSTP's default action stops the process until SIGCONT.
        unsafe {
            libc::raise(libc::SIGTSTP);
        }
        enter(&mut io::stdout())?;
        self.terminal.clear()?;
        Ok(())
    }

    #[cfg(not(unix))]
    pub fn suspend(&mut self) -> Result<(), TuiError> {
        // A Windows console has no job control: there is nothing to stop to.
        Ok(())
    }

    pub fn size(&self) -> Result<ratatui::layout::Size, TuiError> {
        Ok(self.terminal.size()?)
    }
}

impl Drop for Tui {
    fn drop(&mut self) {
        let _ = leave();
    }
}

/// Where a surface draws, and what it can ask of it.
pub trait Display {
    /// Draw one frame.
    fn draw(&mut self, render: impl FnOnce(&mut Frame)) -> Result<(), TuiError>;
    /// Ctrl-Z: stop until the shell continues this process.
    fn suspend(&mut self) -> Result<(), TuiError>;
}

impl Display for Tui {
    fn draw(&mut self, render: impl FnOnce(&mut Frame)) -> Result<(), TuiError> {
        Tui::draw(self, render)
    }

    fn suspend(&mut self) -> Result<(), TuiError> {
        Tui::suspend(self)
    }
}

/// A display with no terminal behind it: every frame is drawn into a buffer
/// of a fixed size, which can be read back — what an automated test of a
/// surface, or a picture of it, reads.
pub struct HeadlessDisplay {
    terminal: Terminal<ratatui::backend::TestBackend>,
    last: std::sync::Arc<std::sync::Mutex<ratatui::buffer::Buffer>>,
}

impl HeadlessDisplay {
    pub fn new(width: u16, height: u16) -> Self {
        let terminal = Terminal::new(ratatui::backend::TestBackend::new(width, height))
            .expect("a test backend always initialises");
        let area = ratatui::layout::Rect::new(0, 0, width, height);
        Self {
            terminal,
            last: std::sync::Arc::new(std::sync::Mutex::new(ratatui::buffer::Buffer::empty(area))),
        }
    }

    /// A handle to the last frame drawn, readable while the surface runs.
    pub fn frames(&self) -> std::sync::Arc<std::sync::Mutex<ratatui::buffer::Buffer>> {
        self.last.clone()
    }
}

impl Display for HeadlessDisplay {
    fn draw(&mut self, render: impl FnOnce(&mut Frame)) -> Result<(), TuiError> {
        // A test backend cannot fail to draw.
        let completed = match self.terminal.draw(render) {
            Ok(completed) => completed,
            Err(never) => match never {},
        };
        *self.last.lock().expect("frame lock poisoned") = completed.buffer.clone();
        Ok(())
    }

    fn suspend(&mut self) -> Result<(), TuiError> {
        Ok(())
    }
}

/// The smallest terminal the surfaces draw in.
pub const MIN_WIDTH: u16 = 60;
pub const MIN_HEIGHT: u16 = 18;

/// Draw the "terminal too small" screen, when it is.
pub fn too_small(frame: &mut Frame, theme: &Theme) -> bool {
    let area = frame.area();
    if area.width >= MIN_WIDTH && area.height >= MIN_HEIGHT {
        return false;
    }
    let message = format!(
        "The terminal is {}×{}; this view needs {MIN_WIDTH}×{MIN_HEIGHT}.",
        area.width, area.height
    );
    let paragraph = ratatui::widgets::Paragraph::new(message)
        .wrap(ratatui::widgets::Wrap { trim: true })
        .style(theme.chrome());
    frame.render_widget(paragraph, area);
    true
}
