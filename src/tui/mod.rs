//! The terminal interface: machines drawn as graphs, looked at and edited.
//!
//! The desktop clients draw a machine with FabricLoomUIKit — input cards,
//! step cards, maps and their items, gathers, outputs. This module draws the
//! same pictures in a terminal, from the same sources: a planned machine's
//! [`PlanView`](crate::orchestrator::PlanView), or the text of a machine
//! being edited.
//!
//! The pieces, bottom up:
//!
//! - [`text`]: text as the terminal measures it — display width, ellipsis,
//!   padding.
//! - [`theme`]: the uikit's palette, the glyphs lines and marks are drawn
//!   with, and what kind of thing a medium is.
//! - [`edit`]: a machine's text as a draft the editor changes structurally,
//!   written back as notation.
//! - [`scene`]: what is drawn — nodes and edges of a planned machine or of a
//!   draft, with their titles and subtitles — built the way the uikit's scene
//!   builders build theirs, so the picture has the same parts.
//! - [`layout`]: where each part sits, in terminal cells — a layered layout
//!   whose columns follow the uikit's longest-path rule, with orthogonal edge
//!   routing in the channels between columns.
//! - [`canvas`]: the cells — box-drawing glyphs chosen from the directions
//!   that meet in each cell — drawn into a ratatui buffer, which ratatui diffs
//!   against the previous frame so only changed cells reach the terminal.
//! - [`app`]: the interactive surface — the machine editor, which is also
//!   the read-only viewer — on one event loop.
//!
//! Nothing in the picture is guessed. A plan that contradicts what a picture
//! of it needs — an edge naming a node the plan does not have, an output fed
//! by more than one edge, a cycle — is a hard error, as it is in the uikit.

pub mod app;
pub mod canvas;
pub mod edit;
pub mod layout;
pub mod scene;
pub mod text;
pub mod theme;

#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod tests;
