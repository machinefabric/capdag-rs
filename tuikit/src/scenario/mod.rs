//! The capdag command on worlds of test data.
//!
//! A world (`world`) is what the engine would hold — a fabric (a snapshot of
//! a real one), cartridges, files — and how each run of its machines goes
//! (`script`). The scenario host (`host`) serves it to the command through
//! the same [`crate::cli::host::Host`] the engine serves, so every command,
//! flag and surface runs as it does on the engine, with no fabric, cartridge
//! or network behind it. A journey (`journey`) is one thing a person does,
//! in a world, in each mode it is done in; played (`play`) on a terminal of
//! its own (`terminal`), it is checked against what it should come to.

pub mod check;
pub mod host;
pub mod journey;
#[cfg(unix)]
pub mod play;
pub mod script;
#[cfg(unix)]
pub mod terminal;
pub mod world;
