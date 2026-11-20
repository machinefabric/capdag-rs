//! What went wrong, said the one way every command says it.
//!
//! An error names the problem in one sentence — the file, the step, the
//! argument — then what was expected and what was found, then the next command
//! to type:
//!
//! ```text
//! error: people.csv is not a PDF
//!   disbind-pdf takes a PDF; pdfium could not read people.csv as one (format error)
//! hint: see what people.csv can become: capdag plan people.csv
//! ```
//!
//! The shape is rustc's and cargo's (`error:` and `help:`/`hint:`). Under
//! `--json` the same error is one record on stdout, so a program reading the
//! command's output learns why it stopped without parsing prose.
//!
//! Every error says what to do next. [`CliError::failed`] and
//! [`CliError::usage`] start an [`Unhinted`] error, and only its
//! [`hint`](Unhinted::hint) makes it a [`CliError`] a command can return:
//!
//! ```
//! use capdag::cli::error::CliError;
//! let error: CliError = CliError::failed("people.csv is not a PDF")
//!     .context("disbind-pdf takes a PDF")
//!     .hint("see what people.csv can become: capdag plan people.csv");
//! assert_eq!(error.hint(), "see what people.csv can become: capdag plan people.csv");
//! ```
//!
//! An error without its next step is not one:
//!
//! ```compile_fail
//! use capdag::cli::error::CliError;
//! let error: CliError = CliError::failed("people.csv is not a PDF").context("disbind-pdf takes a PDF");
//! ```

use std::fmt;

use crate::orchestrator::run_control::{exit_status, RunOutcome};

/// How a command ended, as its exit status says it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Exit {
    /// The command did what it was asked.
    Ok,
    /// It could not: a failure.
    Failed,
    /// It was not asked properly: an unknown command or flag, a missing or
    /// malformed value, a choice that was not made.
    Usage,
    /// A run ended as it did.
    Run(RunOutcome),
}

impl Exit {
    /// The process's exit status: 0, 1, 2, and for a run the model's
    /// (`capdag/formal` `Run.Control.exitStatus`): 3 tolerated, 130 stopped or
    /// aborted.
    pub fn code(&self) -> i32 {
        match self {
            Exit::Ok => 0,
            Exit::Failed => 1,
            Exit::Usage => 2,
            Exit::Run(outcome) => exit_status(outcome.clone()),
        }
    }
}

/// A failure of a command: what is wrong, what was expected and found, and
/// what to type next. Made only by [`Unhinted::hint`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliError {
    /// One sentence: what is wrong, naming the file, step or argument.
    problem: String,
    /// What was expected and what was found, a line each.
    context: Vec<String>,
    /// The next command to type, or what to do.
    hint: String,
    /// How the process exits.
    pub exit: Exit,
}

/// An error still missing what to do next: its problem, its context, and how
/// the process exits. [`Unhinted::hint`] completes it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "an error says what to do next: complete it with .hint(…)"]
pub struct Unhinted {
    problem: String,
    context: Vec<String>,
    exit: Exit,
}

impl Unhinted {
    /// One more line of what was expected and what was found.
    pub fn context(mut self, line: impl Into<String>) -> Self {
        let line = line.into();
        if !line.is_empty() {
            self.context.push(line);
        }
        self
    }

    /// What to do next — the command to type, or the change to make — which
    /// makes this an error a command can return. A hint that says nothing is
    /// a defect in the command that raised it.
    pub fn hint(self, hint: impl Into<String>) -> CliError {
        let hint = hint.into();
        assert!(
            !hint.trim().is_empty(),
            "the error '{}' was given an empty hint: every error says what to do next",
            self.problem
        );
        CliError {
            problem: self.problem,
            context: self.context,
            hint,
            exit: self.exit,
        }
    }
}

impl CliError {
    /// A failure: the command could not do what it was asked.
    pub fn failed(problem: impl Into<String>) -> Unhinted {
        Unhinted {
            problem: problem.into(),
            context: Vec::new(),
            exit: Exit::Failed,
        }
    }

    /// A usage error: the command was not asked properly.
    pub fn usage(problem: impl Into<String>) -> Unhinted {
        Unhinted {
            exit: Exit::Usage,
            ..Self::failed(problem)
        }
    }

    /// The same error with one more line of what was expected and what was
    /// found; its hint stays.
    pub fn with_context(mut self, line: impl Into<String>) -> CliError {
        let line = line.into();
        if !line.is_empty() {
            self.context.push(line);
        }
        self
    }

    /// What is wrong, in one sentence.
    pub fn problem(&self) -> &str {
        &self.problem
    }

    /// What was expected and what was found, a line each.
    pub fn context(&self) -> &[String] {
        &self.context
    }

    /// What to do next.
    pub fn hint(&self) -> &str {
        &self.hint
    }

    /// The error as a person reads it, on stderr.
    pub fn render(&self, style: &super::term::Style) -> String {
        let mut out = format!("{} {}\n", style.error("error:"), self.problem);
        for line in &self.context {
            for part in line.lines() {
                out.push_str("  ");
                out.push_str(part);
                out.push('\n');
            }
        }
        out.push_str(&format!("{} {}\n", style.hint("hint:"), self.hint));
        out
    }

    /// The error as one JSON record, for `--json`.
    pub fn record(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "error",
            "problem": self.problem,
            "context": self.context,
            "hint": self.hint,
            "exit": self.exit.code(),
        })
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.render(&super::term::Style::plain()))
    }
}

impl std::error::Error for CliError {}

pub type CliResult<T> = Result<T, CliError>;

/// The directory the command was started in could not be read: it was
/// removed, or may not be listed.
pub fn working_directory_unreadable(error: std::io::Error) -> CliError {
    CliError::failed("the working directory could not be read")
        .context(error.to_string())
        .hint("cd to a directory that exists and that you may read, then run the command again")
}

/// A machine file whose path could not be made absolute, which is how the
/// machines it includes are found: it is not there, or its path is not UTF-8.
pub fn machine_path_unusable(path: &std::path::Path, error: impl fmt::Display) -> CliError {
    CliError::failed(format!("the machine file '{}' could not be read", path.display()))
        .context(error.to_string())
        .hint(format!("check that '{}' is there, and that its path is UTF-8 text", path.display()))
}

/// What to do when `path` could not be read, by why it could not.
pub fn read_hint(path: &std::path::Path, error: &std::io::Error) -> String {
    let path = path.display();
    match error.kind() {
        std::io::ErrorKind::NotFound => format!("check the path: nothing is at '{path}'"),
        std::io::ErrorKind::PermissionDenied => format!("you may not read '{path}': change its permissions, or copy it somewhere you can read it"),
        std::io::ErrorKind::InvalidData => format!("'{path}' is not UTF-8 text: check that it is the file you meant"),
        std::io::ErrorKind::IsADirectory => format!("'{path}' is a directory: name a file in it"),
        _ => format!("check that '{path}' can be read, then run the command again"),
    }
}

/// What to do when `path` could not be written, by why it could not.
pub fn write_hint(path: &std::path::Path, error: &std::io::Error) -> String {
    let path = path.display();
    match error.kind() {
        std::io::ErrorKind::NotFound => format!("the folder for '{path}' does not exist: create it, or write somewhere else"),
        std::io::ErrorKind::PermissionDenied => format!("you may not write '{path}': write somewhere else, or change the folder's permissions"),
        std::io::ErrorKind::StorageFull => "the disk is full: free some room, then run the command again".to_string(),
        std::io::ErrorKind::IsADirectory => format!("'{path}' is a directory: name a file to write"),
        _ => format!("check that '{path}' can be written, then run the command again"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST20110: an error reads as its problem, its context indented under it,
    // and its hint last — and as the same three parts in its JSON record.
    #[test]
    fn test20110_an_error_names_the_problem_its_context_and_the_next_step() {
        let error = CliError::failed("people.csv is not a PDF")
            .context("disbind-pdf takes a PDF; pdfium could not read people.csv as one")
            .hint("see what people.csv can become: capdag plan people.csv");
        assert_eq!(
            error.render(&super::super::term::Style::plain()),
            "error: people.csv is not a PDF\n  disbind-pdf takes a PDF; pdfium could not read \
             people.csv as one\nhint: see what people.csv can become: capdag plan people.csv\n"
        );
        let record = error.record();
        assert_eq!(record["type"], "error");
        assert_eq!(record["problem"], "people.csv is not a PDF");
        assert_eq!(record["hint"], "see what people.csv can become: capdag plan people.csv");
        assert_eq!(record["exit"], 1);
    }

    // TEST20111: every usage error exits 2, a failure 1, and a run as the
    // model says its outcome exits.
    #[test]
    fn test20111_exit_statuses() {
        assert_eq!(CliError::usage("x").hint("y").exit.code(), 2);
        assert_eq!(CliError::failed("x").hint("y").exit.code(), 1);
        assert_eq!(Exit::Ok.code(), 0);
        assert_eq!(Exit::Run(RunOutcome::Tolerated).code(), 3);
        assert_eq!(Exit::Run(RunOutcome::Stopped).code(), 130);
    }

    // TEST20124: a hint that says nothing is refused where it is given, naming
    // the error it was given to: an error without its next step is a defect.
    #[test]
    #[should_panic(expected = "the error 'people.csv is not a PDF' was given an empty hint")]
    fn test20124_an_empty_hint_is_refused() {
        let _ = CliError::failed("people.csv is not a PDF").hint("  ");
    }

    // TEST20125: a file that could not be read or written is answered by why:
    // a missing file with its path, a refused one with its permissions, bytes
    // that are not text as not UTF-8, a full disk as room to free.
    #[test]
    fn test20125_an_io_failure_is_answered_by_why() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("pages.machine");
        let not_found = std::fs::read(&missing).unwrap_err();
        assert!(read_hint(&missing, &not_found).starts_with("check the path: nothing is at"));
        let garbled = dir.path().join("garbled.machine");
        std::fs::write(&garbled, [0xff, 0xfe]).unwrap();
        let not_text = std::fs::read_to_string(&garbled).unwrap_err();
        assert_eq!(read_hint(&garbled, &not_text), format!("'{}' is not UTF-8 text: check that it is the file you meant", garbled.display()));
        let into_missing = dir.path().join("no-such-folder").join("out.machine");
        let no_folder = std::fs::write(&into_missing, "x").unwrap_err();
        assert!(write_hint(&into_missing, &no_folder).contains("does not exist: create it"));
        let denied = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        assert!(read_hint(&missing, &denied).contains("change its permissions"));
        assert!(write_hint(&missing, &denied).contains("write somewhere else"));
        let full = std::io::Error::from(std::io::ErrorKind::StorageFull);
        assert_eq!(write_hint(&missing, &full), "the disk is full: free some room, then run the command again");
    }
}
