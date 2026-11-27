//! What the command decides of a command line, from the proved model of it.
//!
//! Every decision the `capdag` command makes about a line — whether it runs a
//! machine, answers, opens a view, asks a person one question, fails naming
//! the choices, or is refused — is made by code generated from the model in
//! `capdag/tuikit-formal` (Lean, through lungo) and checked in under
//! `src/generated`. The commands gather what the model asks of the world —
//! the files, the fabric, the planner, the cartridges — and do what its
//! verdict says.

/// The generated package: the model's types and the functions the commands
/// call. What each function means is the model's (`tuikit-formal/CapdagCli`),
/// and what is proved of it is in its laws.
#[doc(hidden)]
#[allow(warnings, clippy::all, non_snake_case, non_camel_case_types, non_upper_case_globals)]
pub mod generated {
    include!("../generated/capdagCliFormal/capdagCliFormal.rs");
}

pub use generated::*;

/// A count or a place, as the model takes it.
pub fn nat(n: usize) -> lungo::Nat {
    lungo::Nat::from(n)
}

/// A place the model gave back, as an index. The model names places by the
/// numbers it was given, so one that does not fit an index is a defect.
pub fn index(n: &lungo::Nat) -> usize {
    n.to_u64()
        .and_then(|v| usize::try_from(v).ok())
        .unwrap_or_else(|| panic!("the model gave back a place {n:?} that no table holds"))
}

/// A list, as the model takes it.
pub fn list<T>(items: impl IntoIterator<Item = T>) -> lungo::List<T> {
    items.into_iter().collect()
}

/// The exit status a verdict of the whole command ends it with, when it ends
/// it: what `CliVerdict.exit` says.
pub fn exit_of(verdict: &CliVerdict) -> Option<i32> {
    cli_verdict::exit(verdict.clone()).map(|n| index(&n) as i32)
}

/// What the command says when it refuses a line: the model's sentence.
pub fn says(refusal: &CliRefusal) -> String {
    cli_refusal::says(refusal.clone())
}

/// A world that answers nothing of a line: the stand-in for the world of a
/// command whose own facts play no part in what is asked (its preflight).
pub fn no_world(terminal: Terminal) -> World {
    World {
        terminal,
        file_there: lungo::LeanClosure::from_fn(|_: lungo::Nat| false),
        machine_file: lungo::LeanClosure::from_fn(|_: lungo::Nat| false),
        live_there: lungo::LeanClosure::from_fn(|_: lungo::Nat| false),
        types_known: true,
        output: OutputDir::Ok,
        resolves: lungo::LeanClosure::from_fn(|_: lungo::Nat| false),
        targets: Choices::None,
        machines: Choices::None,
        planned: lungo::LeanClosure::from_fn(|k: lungo::Nat| k),
        planned_input: lungo::LeanClosure::from_fn(|_: lungo::Nat, _: Positional| nat(0)),
        inputs: lungo::LeanClosure::from_fn(|_: lungo::Nat| nat(0)),
        fit: lungo::LeanClosure::from_fn(|_: lungo::Nat, _: lungo::Nat| list(Vec::new())),
        options: lungo::LeanClosure::from_fn(|_: lungo::Nat| nat(0)),
        option_shared: lungo::LeanClosure::from_fn(|_: lungo::Nat, _: lungo::Nat| false),
        option_choices: lungo::LeanClosure::from_fn(|_: lungo::Nat, _: lungo::Nat| Choices::Free),
        needs_cartridge: lungo::LeanClosure::from_fn(|_: lungo::Nat| false),
        providers: lungo::LeanClosure::from_fn(|_: lungo::Nat| Choices::None),
    }
}

/// A `run` line that gives nothing: the stand-in for a line's own facts.
pub fn no_run_line() -> RunLine {
    RunLine {
        machine: None,
        inputs: list(Vec::new()),
        named: list(Vec::new()),
        args: list(Vec::new()),
        targets: nat(0),
        has_from: false,
        has_selector: false,
        selector_reads: true,
    }
}
