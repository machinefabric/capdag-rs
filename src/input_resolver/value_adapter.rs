//! ValueAdapter trait — pluggable value-based content inspection
//!
//! This is the value-based counterpart to `MediaAdapter` (which handles files).
//! Both solve the same problem: determining a specific media URN variant from content.
//!
//! - `MediaAdapter`: inspects file content (bytes) to refine a base media URN with
//!   structural markers (list, record, enc=utf8)
//! - `ValueAdapter`: inspects a string argument value to refine a base media URN with
//!   domain-specific markers (e.g., model family for model-spec arguments)
//!
//! The base media URN comes from the argument slot declaration (e.g., a cap's arg
//! specifies `media:enc=utf8;llm;model-spec`). The value filling the slot is inspected
//! to produce a more specific URN (e.g., `media:enc=utf8;llm;mistral;model-spec`).

use std::fmt;

/// Why the value-adapter registry refused a key, or could not refine a base.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueAdapterError {
    /// The key an adapter was registered under is no media URN.
    BadKey { key: String, error: String },
    /// An adapter is already registered under a key equivalent to this one.
    DuplicateKey { key: String, existing: String },
    /// The base falls under two most specific keys, neither of which refines
    /// the other: no one adapter is the one to ask.
    Ambiguous {
        base: String,
        first: String,
        second: String,
    },
    /// The adapter's refinement is no media URN.
    BadRefinement {
        adapter: String,
        base: String,
        refined: String,
        error: String,
    },
    /// The adapter's refinement does not lie inside the base it refines.
    NotInside {
        adapter: String,
        base: String,
        refined: String,
    },
}

impl fmt::Display for ValueAdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValueAdapterError::BadKey { key, error } => {
                write!(f, "value adapter key '{key}' is no media URN: {error}")
            }
            ValueAdapterError::DuplicateKey { key, existing } => write!(
                f,
                "value adapter key '{key}' is equivalent to the registered key '{existing}'"
            ),
            ValueAdapterError::Ambiguous { base, first, second } => write!(
                f,
                "'{base}' falls under the value adapter keys '{first}' and '{second}', \
                 neither of which refines the other"
            ),
            ValueAdapterError::BadRefinement {
                adapter,
                base,
                refined,
                error,
            } => write!(
                f,
                "value adapter {adapter} refined '{base}' to '{refined}', which is no media URN: {error}"
            ),
            ValueAdapterError::NotInside {
                adapter,
                base,
                refined,
            } => write!(
                f,
                "value adapter {adapter} refined '{base}' to '{refined}', which does not conform to it"
            ),
        }
    }
}

impl std::error::Error for ValueAdapterError {}

/// Result of value-based content inspection
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueAdapterResult {
    /// The refined media URN with additional marker tags
    pub media_urn: String,
}

/// Trait for value-based content inspection adapters
///
/// Implementations inspect string argument values to refine a base media URN.
/// This follows the same content-inspection pattern as `MediaAdapter`, but
/// operates on string values rather than file paths and byte content.
///
/// # Example
///
/// A `ModelSpecValueAdapter` inspects a model spec string like
/// `hf:MaziyarPanahi/Mistral-7B-Instruct-v0.3-GGUF` and adds a `mistral`
/// marker tag to the base URN `media:enc=utf8;llm;model-spec`, producing
/// `media:enc=utf8;mistral;model-spec` (canonical sorted form).
pub trait ValueAdapter: Send + Sync {
    /// Unique name for this adapter (for debugging/logging)
    fn name(&self) -> &'static str;

    /// Refine a base media URN based on the value filling the argument slot.
    ///
    /// - `base_media_urn`: The media URN declared by the argument slot
    ///   (e.g., `media:enc=utf8;llm;model-spec`)
    /// - `value`: The string value filling the slot
    ///   (e.g., `hf:MaziyarPanahi/Mistral-7B-Instruct-v0.3-GGUF?include=...`)
    ///
    /// Returns `Some(refined_urn)` if this adapter can refine the URN,
    /// or `None` if this adapter does not handle this base URN.
    fn refine(&self, base_media_urn: &str, value: &str) -> Option<ValueAdapterResult>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A test adapter that adds a "test-marker" tag to any URN containing "test"
    struct TestValueAdapter;

    impl ValueAdapter for TestValueAdapter {
        fn name(&self) -> &'static str {
            "TestValueAdapter"
        }

        fn refine(&self, base_media_urn: &str, value: &str) -> Option<ValueAdapterResult> {
            if !base_media_urn.contains("test") {
                return None;
            }
            if value.contains("special") {
                Some(ValueAdapterResult {
                    media_urn: format!("{};special", base_media_urn),
                })
            } else {
                None
            }
        }
    }

    // TEST1228: Value adapters can append a more specific marker when both base URN and value match.
    #[test]
    fn test1228_value_adapter_refine_match() {
        let adapter = TestValueAdapter;
        let result = adapter.refine("media:enc=utf8;test", "something-special");
        assert_eq!(
            result,
            Some(ValueAdapterResult {
                media_urn: "media:enc=utf8;test;special".to_string(),
            })
        );
    }

    // TEST1229: Value adapters return no refinement when the base media URN is outside their domain.
    #[test]
    fn test1229_value_adapter_refine_no_match_base() {
        let adapter = TestValueAdapter;
        let result = adapter.refine("media:enc=utf8;other", "something-special");
        assert_eq!(result, None);
    }

    // TEST1230: Value adapters return no refinement when the inspected value does not match.
    #[test]
    fn test1230_value_adapter_refine_no_match_value() {
        let adapter = TestValueAdapter;
        let result = adapter.refine("media:enc=utf8;test", "ordinary-value");
        assert_eq!(result, None);
    }
}
