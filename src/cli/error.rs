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
/// what to type next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliError {
    /// One sentence: what is wrong, naming the file, step or argument.
    pub problem: String,
    /// What was expected and what was found, a line each.
    pub context: Vec<String>,
    /// The next command to type, or what to do.
    pub hint: Option<String>,
    /// How the process exits.
    pub exit: Exit,
}

impl CliError {
    /// A failure: the command could not do what it was asked.
    pub fn failed(problem: impl Into<String>) -> Self {
        Self {
            problem: problem.into(),
            context: Vec::new(),
            hint: None,
            exit: Exit::Failed,
        }
    }

    /// A usage error: the command was not asked properly.
    pub fn usage(problem: impl Into<String>) -> Self {
        Self {
            exit: Exit::Usage,
            ..Self::failed(problem)
        }
    }

    /// One more line of what was expected and what was found.
    pub fn context(mut self, line: impl Into<String>) -> Self {
        let line = line.into();
        if !line.is_empty() {
            self.context.push(line);
        }
        self
    }

    /// The next thing to do.
    pub fn hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
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
        if let Some(hint) = &self.hint {
            out.push_str(&format!("{} {hint}\n", style.hint("hint:")));
        }
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
        assert_eq!(record["exit"], 1);
    }

    // TEST20111: every usage error exits 2, a failure 1, and a run as the
    // model says its outcome exits.
    #[test]
    fn test20111_exit_statuses() {
        assert_eq!(CliError::usage("x").exit.code(), 2);
        assert_eq!(CliError::failed("x").exit.code(), 1);
        assert_eq!(Exit::Ok.code(), 0);
        assert_eq!(Exit::Run(RunOutcome::Tolerated).code(), 3);
        assert_eq!(Exit::Run(RunOutcome::Stopped).code(), 130);
    }
}
