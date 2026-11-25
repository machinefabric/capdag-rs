//! The capdag command's terminal interface.
//!
//! Everything the `capdag` command shows and every choice of what to show:
//! the command line and its flags ([`cli`]), each command's flow, its plain
//! and `--json` output, the inline live view of a run and the pickers, and the
//! full-screen machine viewer and editor ([`tui`]). What a command needs of the
//! engine it asks of a [`cli::host::Host`]; the `capdag` binary runs it on the
//! engine itself.

pub mod cli;
pub mod tui;
