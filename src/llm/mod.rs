//! Talking to a language model.
//!
//! [`protocol`] holds the canonical types for the LLM media defs — one half of
//! a definition capdag itself declares, which is why they belong here rather
//! than in a package that could version away from them. [`prompt`] decides how
//! a downloaded model wants its input framed, from the dim profile
//! `cap:download-model` returns beside the local path. [`structured_queries`]
//! renders a prompt per declared query and reads the answer back.
//!
//! This was `capdag-cartridge-sdk`, a separate package per language. Two of
//! its modules were never about language models at all and did not come with
//! it: page ranges are [`crate::pages`], and the shared HTTP retry policy is
//! [`crate::net_retry`].

pub mod protocol;
pub mod prompt;
pub mod structured_queries;

// The structured-query surface, at the module root, because a caller building
// one reaches for the builder and the result types together.
//
// `pub use capdag::*` used to sit here too: this was a separate crate, and it
// re-exported the crate it wrapped so a cartridge needed one dependency
// instead of two. Inside capdag that is circular, and unnecessary — a caller
// already has capdag.
pub use structured_queries::{
    DecisionItem, MakeDecisionResult, MakeMultipleDecisionsResult, StructuredQuery,
    StructuredQueryBuilder, StructuredQueryRegistry,
};
