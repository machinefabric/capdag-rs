//! Prompt preparation for instruction-tuned LLMs.
//!
//! Every instruct/chat-tuned model carries a `tokenizer.chat_template`
//! in its metadata that frames a user-message string with the
//! special-token scaffolding the model was trained on. Feeding such a
//! model raw user text — without the template — produces degenerate
//! output: the model interprets the bytes as an arbitrary continuation
//! and (especially with a fixed seed) collapses to the same
//! high-prior completion regardless of input.
//!
//! `cap:download-model` returns a [`RefinedDims`] view of the model's
//! detected dim profile alongside the local path. The consumer cap
//! reads `chat_template` from that view and uses
//! [`classify_prompt`] to decide how to prepare its tokenizer input:
//!
//! - `chat-template-jinja` / `chat-template-short` →
//!   [`PromptStrategy::ChatTemplated`] — the cartridge MUST render the
//!   user message (and optional system prompt) through the model's
//!   chat-template machinery and tokenize the rendered string with
//!   special-token parsing enabled. Each backend has its own
//!   chat-template surface (llama.cpp's `apply_chat_template`,
//!   tokenizers crate's `Tokenizer::encode`, MLX-Swift), so the
//!   rendering stays in the cartridge — only the *decision* is shared.
//! - empty / `chat-template-absent` → [`PromptStrategy::Raw`] — the
//!   model is a base/completion model. The user prompt is tokenized
//!   verbatim, no specials, no system prompt (a base model has no
//!   notion of system messages).
//!
//! The system-prompt arg on each text-generation cap defaults to
//! [`DEFAULT_SYSTEM_PROMPT`] — a generic instruction that makes any
//! arbitrary user input a sensible "respond to this" turn. Cap
//! authors that want a task-specific default can override the cap
//! TOML's `default_value`.

use serde::{Deserialize, Serialize};

/// Default system prompt baked into every text-generation cap when
/// the user / orchestrator doesn't supply one. Generic enough to
/// frame any input — code, prose, JSON, transcripts — as a request
/// for a useful response. Concrete prompts (summarise, translate,
/// explain) belong in the user-message turn.
pub const DEFAULT_SYSTEM_PROMPT: &str =
    "You are a helpful assistant. Respond to the user's input with a useful, accurate, \
     and concise reply. If the input is a document or excerpt, treat it as the subject \
     of the user's request and respond about it.";

/// The dim profile of a downloaded model — the subset of
/// `RefinedModelSpec` fields that affect prompt preparation.
///
/// Constructed by the consumer cartridge from the response it gets
/// back from `cap:download-model`. Strings are the kebab-case dim
/// values (`"chat-template-jinja"`, `"chat-template-short"`, `""`
/// for absent) — same wire form as the `media:model-spec` URN tags.
#[derive(Debug, Clone, Default)]
pub struct RefinedDims {
    /// `chat_template` field from the wire `RefinedModelSpec`. Empty
    /// string means the model carries no chat template (a base /
    /// completion model).
    pub chat_template: String,
    /// `family` field — used by future family-specific quirks. Not
    /// consulted by `classify_prompt` today; carried so cartridges
    /// can branch on family without re-deriving it.
    pub family: String,
    /// `model_task` field — `llm` / `vision` / `embeddings` / etc.
    /// `classify_prompt` doesn't read it; carried for completeness
    /// and future use (e.g. an `embeddings` task wouldn't call
    /// `classify_prompt` at all but might still want the dims in
    /// one place).
    pub model_task: String,
}

/// What `classify_prompt` says the cartridge should do with the
/// user's prompt before feeding it to the tokenizer.
///
/// The variant carries everything the renderer needs; the cartridge
/// hands the rendered string to its backend's tokenizer with the
/// matching special-token-parsing flag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PromptStrategy {
    /// The model expects chat-formatted input. The cartridge MUST
    /// render the system + user messages through the model's
    /// chat-template machinery, then tokenize the rendered string
    /// with `parse_specials = true`. The shared classifier returns
    /// the raw turn texts; the cartridge owns the rendering surface
    /// because each backend's API differs.
    ChatTemplated {
        /// The system message to render. `None` when the caller
        /// passed `None` for the system prompt AND chose not to use
        /// the default — chat templates usually accept zero or one
        /// system message.
        system: Option<String>,
        /// The user's message. Always non-empty by contract — an
        /// empty user turn is rejected upstream by the
        /// text-generation cap's required-stdin arg.
        user: String,
    },
    /// The model has no chat template. Tokenize the user's text as
    /// a raw completion prompt. A system prompt (if any) is
    /// dropped — base models have no notion of system messages,
    /// and prepending one as plain text would corrupt the
    /// completion-style invocation.
    Raw {
        /// The user's text, fed verbatim to the tokenizer.
        text: String,
    },
}

/// Classify how to prepare a user prompt given the model's detected
/// dims and an optional system prompt.
///
/// `system` is the system prompt to use:
/// - `Some(s)` where `s` is non-empty — render as the system turn
///   (chat-templated path) or drop (raw path).
/// - `Some("")` or `None` — no system message; the caller did not
///   want one. The default the cap should fall back to before
///   calling here is [`DEFAULT_SYSTEM_PROMPT`].
///
/// `user` is the user's prompt text. Empty `user` is the caller's
/// bug; this function does not check for it because the cap surface
/// already declares the prompt arg as required.
///
/// The returned strategy is consumed by the cartridge's renderer.
/// See `PromptStrategy` for the contract.
pub fn classify_prompt(dims: &RefinedDims, user: String, system: Option<String>) -> PromptStrategy {
    // The chat-template axis values that demand templated rendering.
    // The catalogue defines `chat-template-jinja` and
    // `chat-template-short`; an empty string means "absent" (the
    // model's metadata didn't carry a template at all, so it's a
    // base / completion model). Any other value is unknown — treat
    // as Raw so an unfamiliar future template tag doesn't silently
    // get fed through chat formatting that doesn't apply.
    match dims.chat_template.as_str() {
        "chat-template-jinja" | "chat-template-short" => {
            // Drop empty system strings — the caller may have passed
            // `Some("")` to mean "I have no system prompt"; the
            // chat-template renderer should not see an empty system
            // turn (some templates emit `<|im_start|>system\n\n<|im_end|>`
            // for an empty body, which wastes tokens and confuses
            // sampling).
            let system_clean = system.filter(|s| !s.trim().is_empty());
            PromptStrategy::ChatTemplated {
                system: system_clean,
                user,
            }
        }
        _ => PromptStrategy::Raw { text: user },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dims_with(chat_template: &str) -> RefinedDims {
        RefinedDims {
            chat_template: chat_template.to_string(),
            family: String::new(),
            model_task: String::new(),
        }
    }

    /// Jinja-template models route through `ChatTemplated`. Forgetting
    /// this collapses to `Raw`, which feeds the rendered chat
    /// scaffolding to the tokenizer as plain text — produces
    /// degenerate output where the model treats `<|im_start|>` as
    /// arbitrary characters instead of a special token.
    #[test]
    fn test0011_jinja_template_yields_chat_templated() {
        let s = classify_prompt(
            &dims_with("chat-template-jinja"),
            "summarise this".to_string(),
            Some("you are helpful".to_string()),
        );
        match s {
            PromptStrategy::ChatTemplated { system, user } => {
                assert_eq!(system.as_deref(), Some("you are helpful"));
                assert_eq!(user, "summarise this");
            }
            other => panic!("expected ChatTemplated, got {:?}", other),
        }
    }

    /// `chat-template-short` (model identifies its template by
    /// registered short-name) must also route through chat
    /// templating — the cartridge will resolve the short name via
    /// its backend's template registry.
    #[test]
    fn test0012_short_name_template_yields_chat_templated() {
        let s = classify_prompt(
            &dims_with("chat-template-short"),
            "user input".to_string(),
            None,
        );
        match s {
            PromptStrategy::ChatTemplated { system, user } => {
                assert_eq!(system, None);
                assert_eq!(user, "user input");
            }
            other => panic!("expected ChatTemplated, got {:?}", other),
        }
    }

    /// **Core regression guard.** The empty `chat_template` field
    /// means a base / completion model — the cartridge must NOT
    /// chat-template the input. Routing here is what makes a
    /// well-formed instruct model degrade to raw completion was the
    /// bug; routing the OTHER way (raw model into chat-template
    /// rendering) would be the equivalent regression — the rendered
    /// `<|im_start|>` etc. would tokenize as plain text and corrupt
    /// the completion.
    #[test]
    fn test0013_absent_template_yields_raw() {
        let s = classify_prompt(
            &dims_with(""),
            "the rest of the story is".to_string(),
            Some("ignored — base models have no system slot".to_string()),
        );
        match s {
            PromptStrategy::Raw { text } => {
                assert_eq!(text, "the rest of the story is");
            }
            other => panic!("expected Raw, got {:?}", other),
        }
    }

    /// Whitespace-only system prompt is dropped. Some templates
    /// emit a `<|im_start|>system\n\n<|im_end|>` envelope around the
    /// empty body — wasted tokens and a confused turn structure.
    /// `Some("")` and `Some("   ")` both collapse to `None`.
    #[test]
    fn test0014_whitespace_only_system_prompt_dropped_for_chat_templated() {
        for s_in in ["", "   ", "\n\t\n"] {
            let s = classify_prompt(
                &dims_with("chat-template-jinja"),
                "user".to_string(),
                Some(s_in.to_string()),
            );
            match s {
                PromptStrategy::ChatTemplated { system, .. } => {
                    assert_eq!(
                        system, None,
                        "whitespace-only system='{}' must drop to None to avoid an empty system turn",
                        s_in
                    );
                }
                other => panic!("expected ChatTemplated, got {:?}", other),
            }
        }
    }

    /// An unknown `chat_template` value falls into `Raw` — we don't
    /// invent a chat-template behaviour for a tag we don't know.
    /// Future template tags must be classified explicitly here
    /// rather than silently routed through `ChatTemplated` (which
    /// might call into a backend code path that can't handle them).
    #[test]
    fn test0015_unknown_chat_template_value_yields_raw() {
        let s = classify_prompt(
            &dims_with("chat-template-some-future-tag"),
            "user".to_string(),
            Some("system".to_string()),
        );
        match s {
            PromptStrategy::Raw { text } => assert_eq!(text, "user"),
            other => panic!("expected Raw for unknown tag, got {:?}", other),
        }
    }

    /// `DEFAULT_SYSTEM_PROMPT` is generic enough to work for any
    /// input. Pin this constant so a future change that tightens it
    /// (e.g. inserts task-specific instructions) is a deliberate
    /// commit, not an accidental one.
    #[test]
    fn test0016_default_system_prompt_is_task_agnostic() {
        // Substring checks — the precise wording can drift, but the
        // contract that it's task-agnostic must not. If a future
        // edit adds e.g. "You are a code assistant", this test
        // fails and the author has to either revert or update the
        // assertion intentionally.
        assert!(DEFAULT_SYSTEM_PROMPT.contains("helpful"));
        assert!(DEFAULT_SYSTEM_PROMPT.contains("respond"));
        assert!(
            !DEFAULT_SYSTEM_PROMPT.to_lowercase().contains("code"),
            "default prompt must NOT bias toward code: '{}'",
            DEFAULT_SYSTEM_PROMPT
        );
        assert!(
            !DEFAULT_SYSTEM_PROMPT.to_lowercase().contains("summari"),
            "default prompt must NOT bias toward summarisation: '{}'",
            DEFAULT_SYSTEM_PROMPT
        );
        assert!(
            !DEFAULT_SYSTEM_PROMPT.to_lowercase().contains("translat"),
            "default prompt must NOT bias toward translation: '{}'",
            DEFAULT_SYSTEM_PROMPT
        );
    }
}
