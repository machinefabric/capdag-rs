//! ValueAdapterRegistry — collection of value-based content inspection adapters
//!
//! This is the value-based counterpart to `MediaAdapterRegistry` (which handles files).
//! It maps base media URN prefixes to `ValueAdapter` instances and provides a
//! `refine_media_urn` entry point for resolving a refined URN from a value.
//!
//! Both registries serve the same general purpose: determining a specific media URN
//! variant from content. `MediaAdapterRegistry` handles file content (bytes),
//! `ValueAdapterRegistry` handles argument string values.

use std::collections::HashMap;
use std::sync::Arc;

use crate::input_resolver::value_adapter::ValueAdapter;

/// Registry of value-based content inspection adapters
///
/// Adapters are registered by a base URN key. When `refine_media_urn` is called,
/// the registry finds the adapter whose key is a prefix of the base media URN
/// and delegates refinement to it.
///
/// # Example
///
/// ```ignore
/// let mut registry = ValueAdapterRegistry::new();
/// registry.register("media:model-spec", Arc::new(ModelSpecValueAdapter));
///
/// // The registry finds the adapter for "media:model-spec" prefix
/// let refined = registry.refine_media_urn(
///     "media:enc=utf-8;llm;model-spec",
///     "hf:MaziyarPanahi/Mistral-7B-Instruct-v0.3-GGUF",
/// );
/// // refined == "media:enc=utf-8;mistral;model-spec"
/// ```
pub struct ValueAdapterRegistry {
    /// Adapters indexed by base URN prefix they handle
    /// e.g., "media:model-spec" -> ModelSpecValueAdapter
    adapters: HashMap<String, Arc<dyn ValueAdapter>>,
}

impl ValueAdapterRegistry {
    /// Create an empty registry
    pub fn new() -> Self {
        Self {
            adapters: HashMap::new(),
        }
    }

    /// Register a value adapter for a base URN prefix
    ///
    /// The key should be the shortest URN prefix that uniquely identifies the
    /// domain this adapter handles (e.g., "media:model-spec" for all model-spec URNs).
    pub fn register(&mut self, base_urn_prefix: &str, adapter: Arc<dyn ValueAdapter>) {
        self.adapters.insert(base_urn_prefix.to_string(), adapter);
    }

    /// Refine a media URN based on the value filling an argument slot.
    ///
    /// Finds the adapter whose registered prefix matches the base_media_urn,
    /// calls its `refine()` method, and returns the refined URN.
    ///
    /// If no adapter matches or the adapter returns None (value doesn't trigger
    /// refinement), returns the base_media_urn unchanged.
    ///
    /// - `base_media_urn`: The media URN declared by the argument slot
    /// - `value`: The string value filling the slot
    pub fn refine_media_urn(&self, base_media_urn: &str, value: &str) -> String {
        // Find the adapter with the longest matching prefix
        let mut best_match: Option<(&str, &Arc<dyn ValueAdapter>)> = None;

        for (prefix, adapter) in &self.adapters {
            if base_media_urn.starts_with(prefix.as_str()) {
                match best_match {
                    None => best_match = Some((prefix.as_str(), adapter)),
                    Some((current_prefix, _)) if prefix.len() > current_prefix.len() => {
                        best_match = Some((prefix.as_str(), adapter));
                    }
                    _ => {}
                }
            }
        }

        match best_match {
            Some((_, adapter)) => match adapter.refine(base_media_urn, value) {
                Some(result) => result.media_urn,
                None => base_media_urn.to_string(),
            },
            None => base_media_urn.to_string(),
        }
    }

    /// Check if an adapter exists for the given base URN prefix
    pub fn has_adapter(&self, base_urn_prefix: &str) -> bool {
        self.adapters.contains_key(base_urn_prefix)
    }

    /// Get all registered adapter prefixes
    pub fn registered_prefixes(&self) -> Vec<&str> {
        self.adapters.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for ValueAdapterRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input_resolver::value_adapter::ValueAdapterResult;

    /// Adapter that detects "special" values and adds a marker
    struct SpecialAdapter;

    impl ValueAdapter for SpecialAdapter {
        fn name(&self) -> &'static str {
            "SpecialAdapter"
        }

        fn refine(&self, base_media_urn: &str, value: &str) -> Option<ValueAdapterResult> {
            if value.contains("special") {
                Some(ValueAdapterResult {
                    media_urn: format!("{};refined", base_media_urn),
                })
            } else {
                None
            }
        }
    }

    /// More specific adapter for a longer prefix
    struct SpecificAdapter;

    impl ValueAdapter for SpecificAdapter {
        fn name(&self) -> &'static str {
            "SpecificAdapter"
        }

        fn refine(&self, _base_media_urn: &str, _value: &str) -> Option<ValueAdapterResult> {
            Some(ValueAdapterResult {
                media_urn: "media:specific;result".to_string(),
            })
        }
    }

    // TEST1221: Matching value adapters refine the base media URN when the value fits.
    #[test]
    fn test1221_refine_with_matching_adapter() {
        let mut registry = ValueAdapterRegistry::new();
        registry.register("media:test", Arc::new(SpecialAdapter));

        // The adapter is keyed by the `media:test` prefix; the base URN starts
        // with that prefix, so the adapter fires and appends `;refined`.
        let result = registry.refine_media_urn("media:test;enc=utf-8", "a-special-value");
        assert_eq!(result, "media:test;enc=utf-8;refined");
    }

    // TEST1222: Base URNs without a registered adapter are returned unchanged.
    #[test]
    fn test1222_refine_no_matching_adapter() {
        let mut registry = ValueAdapterRegistry::new();
        registry.register("media:test", Arc::new(SpecialAdapter));

        let result = registry.refine_media_urn("media:other;enc=utf-8", "a-special-value");
        assert_eq!(result, "media:other;enc=utf-8");
    }

    // TEST1223: Adapters that decline to refine leave the original media URN intact.
    #[test]
    fn test1223_refine_adapter_returns_none() {
        let mut registry = ValueAdapterRegistry::new();
        registry.register("media:test", Arc::new(SpecialAdapter));

        let result = registry.refine_media_urn("media:test;enc=utf-8", "ordinary-value");
        assert_eq!(result, "media:test;enc=utf-8");
    }

    // TEST1224: When multiple adapter prefixes match, the longest prefix wins.
    #[test]
    fn test1224_refine_longest_prefix_match() {
        let mut registry = ValueAdapterRegistry::new();
        registry.register("media:test", Arc::new(SpecialAdapter));
        registry.register("media:test;specific", Arc::new(SpecificAdapter));

        // "media:test;specific;foo" matches both prefixes, but "media:test;specific" is longer
        let result = registry.refine_media_urn("media:test;specific;foo", "any-value");
        assert_eq!(result, "media:specific;result");
    }

    // TEST1225: An empty value adapter registry returns the input media URN unchanged.
    #[test]
    fn test1225_empty_registry() {
        let registry = ValueAdapterRegistry::new();
        let result = registry.refine_media_urn("media:anything", "any-value");
        assert_eq!(result, "media:anything");
    }

    // TEST1226: Adapter presence checks report only the prefixes that were registered.
    #[test]
    fn test1226_has_adapter() {
        let mut registry = ValueAdapterRegistry::new();
        registry.register("media:test", Arc::new(SpecialAdapter));

        assert!(registry.has_adapter("media:test"));
        assert!(!registry.has_adapter("media:other"));
    }
}
