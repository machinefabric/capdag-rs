//! The terminal interface: machines and their runs drawn as graphs.
//!
//! The desktop clients draw a machine and a run of it with FabricLoomUIKit —
//! input stacks, step cards with their arguments, maps and their items,
//! gathers, outputs, the protocol's flow on every edge, a dock of inspectors.
//! This module draws the same pictures in a terminal, from the same sources:
//! a run's [`PlanView`](crate::orchestrator::PlanView) and the signals its
//! [`RunCallbacks`](crate::orchestrator::RunCallbacks) give, joined to the
//! relay switch's protocol state by the shared run-flow correlation.
//!
//! The pieces, bottom up:
//!
//! - [`live`]: what is known of a run, folded from its signals by a pure
//!   reducer keyed by step token — the uikit's live state, without a browser.
//! - [`scene`]: what is drawn — nodes and edges of a run or a machine, with
//!   their titles, subtitles, ports, states — built the way the uikit's scene
//!   builders build theirs, so the picture has the same parts.
//! - [`layout`]: where each part sits, in terminal cells — a layered layout
//!   whose columns follow the uikit's longest-path rule, with orthogonal edge
//!   routing in the channels between columns.
//! - [`canvas`]: the cells — box-drawing glyphs chosen from the directions
//!   that meet in each cell — drawn into a ratatui buffer, which ratatui diffs
//!   against the previous frame so only changed cells reach the terminal.
//! - [`app`]: the interactive surfaces — the run view, the planner, the
//!   machine editor — on one event loop.
//!
//! Nothing in the picture is guessed. A signal naming a step the plan does
//! not have, a map item that does not continue its map's items, an outcome
//! for an item no map ran on: each is a hard error, as it is in the uikit.

pub mod app;
pub mod canvas;
pub mod edit;
pub mod layout;
pub mod live;
pub mod scene;
pub mod text;
pub mod theme;

#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod tests;
