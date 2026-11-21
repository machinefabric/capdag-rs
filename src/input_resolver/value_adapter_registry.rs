//! ValueAdapterRegistry — collection of value-based content inspection adapters
//!
//! This is the value-based counterpart to `MediaAdapterRegistry` (which handles files).
//! It keys `ValueAdapter` instances by media URNs and provides a
//! `refine_media_urn` entry point for resolving a refined URN from a value.
//!
//! Both registries serve the same general purpose: determining a specific media URN
//! variant from content. `MediaAdapterRegistry` handles file content (bytes),
//! `ValueAdapterRegistry` handles argument string values.

use std::sync::Arc;

use crate::formal::detect::AdapterChoice;
use crate::input_resolver::value_adapter::{ValueAdapter, ValueAdapterError};
use crate::urn::media_urn::MediaUrn;

/// Registry of value-based content inspection adapters
///
/// Each adapter is registered under a media URN, its key: the type whose
/// values it can refine. A base media URN is refined by the adapter chosen
/// by inclusion, as the proved model chooses (`CapDAG.Detect.adapterFor`):
/// of the adapters whose key the base falls under, the one registered for the
/// most specific key. Two most specific keys neither of which refines the
/// other are ambiguous, and refused.
///
/// # Example
///
/// ```ignore
/// let mut registry = ValueAdapterRegistry::new();
/// registry.register("media:model-spec", Arc::new(ModelSpecValueAdapter))?;
///
/// // `media:enc=utf8;llm;model-spec` falls under `media:model-spec`.
/// let base = MediaUrn::from_string("media:enc=utf8;llm;model-spec")?;
/// let refined = registry.refine_media_urn(
///     &base,
///     "hf:MaziyarPanahi/Mistral-7B-Instruct-v0.3-GGUF",
/// )?;
/// // refined is media:enc=utf8;llm;mistral;model-spec
/// ```
pub struct ValueAdapterRegistry {
    /// Adapters by the key they are registered under, in registration order.
    adapters: Vec<(MediaUrn, Arc<dyn ValueAdapter>)>,
}

impl ValueAdapterRegistry {
    /// Create an empty registry
    pub fn new() -> Self {
        Self {
            adapters: Vec::new(),
        }
    }

    /// Register a value adapter under `key`, the media URN of the type whose
    /// values it refines. A key that is no media URN, or one equivalent to a
    /// registered key, is refused.
    pub fn register(&mut self, key: &str, adapter: Arc<dyn ValueAdapter>) -> Result<(), ValueAdapterError> {
        let key_urn = MediaUrn::from_string(key).map_err(|e| ValueAdapterError::BadKey {
            key: key.to_string(),
            error: e.to_string(),
        })?;
        if let Some((existing, _)) = self.adapters.iter().find(|(k, _)| {
            k.is_equivalent(&key_urn)
                .expect("every key is a media URN, as the one registered")
        }) {
            return Err(ValueAdapterError::DuplicateKey {
                key: key_urn.to_string(),
                existing: existing.to_string(),
            });
        }
        self.adapters.push((key_urn, adapter));
        Ok(())
    }

    /// Refine `base`, the media URN an argument slot declares, by the value
    /// filling the slot.
    ///
    /// The adapter is the one chosen by inclusion. With none covering the
    /// base, or when the chosen adapter declines (`None`), the base is kept.
    /// A refinement must be a media URN that conforms to the base: the adapter
    /// says which of the base's values this one is, never something else.
    pub fn refine_media_urn(&self, base: &MediaUrn, value: &str) -> Result<MediaUrn, ValueAdapterError> {
        let keys: lungo::List<::tagged_urn::FormalUrn> = self
            .adapters
            .iter()
            .map(|(k, _)| k.inner().formal().clone())
            .collect();
        let position = |n: lungo::Nat| -> usize {
            n.to_u64().expect("a key's position fits in u64") as usize
        };
        match crate::formal::detect::adapter_for(keys, base.inner().formal().clone()) {
            AdapterChoice::Uncovered => Ok(base.clone()),
            AdapterChoice::Ambiguous { i, j } => Err(ValueAdapterError::Ambiguous {
                base: base.to_string(),
                first: self.adapters[position(i)].0.to_string(),
                second: self.adapters[position(j)].0.to_string(),
            }),
            AdapterChoice::One { i } => {
                let adapter = &self.adapters[position(i)].1;
                let Some(result) = adapter.refine(&base.to_string(), value) else {
                    return Ok(base.clone());
                };
                let refined = MediaUrn::from_string(&result.media_urn).map_err(|e| {
                    ValueAdapterError::BadRefinement {
                        adapter: adapter.name().to_string(),
                        base: base.to_string(),
                        refined: result.media_urn.clone(),
                        error: e.to_string(),
                    }
                })?;
                let inside = refined
                    .conforms_to(base)
                    .expect("both are media URNs");
                if !inside {
                    return Err(ValueAdapterError::NotInside {
                        adapter: adapter.name().to_string(),
                        base: base.to_string(),
                        refined: refined.to_string(),
                    });
                }
                Ok(refined)
            }
        }
    }

    /// Whether an adapter is registered under a key equivalent to `key`.
    /// A key that is no media URN is refused.
    pub fn has_adapter(&self, key: &str) -> Result<bool, ValueAdapterError> {
        let key_urn = MediaUrn::from_string(key).map_err(|e| ValueAdapterError::BadKey {
            key: key.to_string(),
            error: e.to_string(),
        })?;
        Ok(self.adapters.iter().any(|(k, _)| {
            k.is_equivalent(&key_urn)
                .expect("every key is a media URN, as the one asked")
        }))
    }

    /// Every registered key, in canonical form and registration order.
    pub fn registered_keys(&self) -> Vec<String> {
        self.adapters.iter().map(|(k, _)| k.to_string()).collect()
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
    use tagged_urn::Constraint;

    fn media(s: &str) -> MediaUrn {
        MediaUrn::from_string(s).unwrap_or_else(|e| panic!("{s}: {e}"))
    }

    /// `base` with a marker added, through the tagged-URN API.
    fn with_marker(base: &str, marker: &str) -> String {
        media(base)
            .with_tag(marker, Constraint::Present)
            .unwrap()
            .to_string()
    }

    /// Adapter that detects "special" values and adds the `refined` marker
    struct SpecialAdapter;

    impl ValueAdapter for SpecialAdapter {
        fn name(&self) -> &'static str {
            "SpecialAdapter"
        }

        fn refine(&self, base_media_urn: &str, value: &str) -> Option<ValueAdapterResult> {
            if value.contains("special") {
                Some(ValueAdapterResult {
                    media_urn: with_marker(base_media_urn, "refined"),
                })
            } else {
                None
            }
        }
    }

    /// Adapter for a more specific key, which marks what it chose
    struct SpecificAdapter;

    impl ValueAdapter for SpecificAdapter {
        fn name(&self) -> &'static str {
            "SpecificAdapter"
        }

        fn refine(&self, base_media_urn: &str, _value: &str) -> Option<ValueAdapterResult> {
            Some(ValueAdapterResult {
                media_urn: with_marker(base_media_urn, "chosen-specific"),
            })
        }
    }

    /// Adapter that answers with a fixed media URN, whatever the base
    struct FixedAdapter(&'static str);

    impl ValueAdapter for FixedAdapter {
        fn name(&self) -> &'static str {
            "FixedAdapter"
        }

        fn refine(&self, _base_media_urn: &str, _value: &str) -> Option<ValueAdapterResult> {
            Some(ValueAdapterResult {
                media_urn: self.0.to_string(),
            })
        }
    }

    /// Adapter that records it was asked
    struct Asked(std::sync::atomic::AtomicBool);

    impl ValueAdapter for Asked {
        fn name(&self) -> &'static str {
            "Asked"
        }

        fn refine(&self, _base_media_urn: &str, _value: &str) -> Option<ValueAdapterResult> {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            None
        }
    }

    // TEST1221: Matching value adapters refine the base media URN when the value fits.
    #[test]
    fn test1221_refine_with_matching_adapter() {
        let mut registry = ValueAdapterRegistry::new();
        registry.register("media:test", Arc::new(SpecialAdapter)).unwrap();

        // The base falls under the key `media:test`, so its adapter is asked.
        let result = registry
            .refine_media_urn(&media("media:test;enc=utf8"), "a-special-value")
            .unwrap();
        assert!(result
            .is_equivalent(&media("media:enc=utf8;refined;test"))
            .unwrap());
    }

    // TEST1222: Base URNs without a registered adapter are returned unchanged.
    #[test]
    fn test1222_refine_no_matching_adapter() {
        let mut registry = ValueAdapterRegistry::new();
        registry.register("media:test", Arc::new(SpecialAdapter)).unwrap();

        let base = media("media:other;enc=utf8");
        let result = registry.refine_media_urn(&base, "a-special-value").unwrap();
        assert_eq!(result, base);
    }

    // TEST1223: Adapters that decline to refine leave the original media URN intact.
    #[test]
    fn test1223_refine_adapter_returns_none() {
        let mut registry = ValueAdapterRegistry::new();
        registry.register("media:test", Arc::new(SpecialAdapter)).unwrap();

        let base = media("media:test;enc=utf8");
        let result = registry.refine_media_urn(&base, "ordinary-value").unwrap();
        assert_eq!(result, base);
    }

    // TEST1224: When several adapter keys cover the base, the adapter for the most specific key answers.
    #[test]
    fn test1224_refine_most_specific_key() {
        let mut registry = ValueAdapterRegistry::new();
        registry.register("media:test", Arc::new(SpecialAdapter)).unwrap();
        registry.register("media:specific;test", Arc::new(SpecificAdapter)).unwrap();

        let result = registry
            .refine_media_urn(&media("media:foo;specific;test"), "a-special-value")
            .unwrap();
        assert!(result.has_marker_tag("chosen-specific"), "{result}");
        assert!(!result.has_marker_tag("refined"), "{result}");
    }

    // TEST1225: An empty value adapter registry returns the input media URN unchanged.
    #[test]
    fn test1225_empty_registry() {
        let registry = ValueAdapterRegistry::new();
        let base = media("media:anything");
        assert_eq!(registry.refine_media_urn(&base, "any-value").unwrap(), base);
    }

    // TEST1226: Adapter presence checks report only the keys that were registered.
    #[test]
    fn test1226_has_adapter() {
        let mut registry = ValueAdapterRegistry::new();
        registry.register("media:test", Arc::new(SpecialAdapter)).unwrap();

        assert!(registry.has_adapter("media:test").unwrap());
        assert!(!registry.has_adapter("media:other").unwrap());
    }

    // TEST6758: Value adapters are chosen by inclusion: keys are media URNs, the most specific key covering the base wins, two unordered ones are refused, and a refinement must lie inside the base
    #[test]
    fn test6758_value_adapters_chosen_by_inclusion() {
        let mut registry = ValueAdapterRegistry::new();
        assert!(matches!(
            registry.register("media:a=", Arc::new(SpecialAdapter)),
            Err(ValueAdapterError::BadKey { .. })
        ));
        registry.register("media:a", Arc::new(SpecialAdapter)).unwrap();
        assert!(matches!(
            registry.register("MEDIA:A", Arc::new(SpecialAdapter)),
            Err(ValueAdapterError::DuplicateKey { .. })
        ));

        // Two keys the base falls under, neither refining the other.
        registry.register("media:b", Arc::new(SpecialAdapter)).unwrap();
        match registry.refine_media_urn(&media("media:a;b"), "special") {
            Err(ValueAdapterError::Ambiguous { first, second, .. }) => {
                assert_eq!((first.as_str(), second.as_str()), ("media:a", "media:b"));
            }
            other => panic!("expected Ambiguous, got {other:?}"),
        }

        // A refinement outside the base is refused.
        let mut outside = ValueAdapterRegistry::new();
        outside.register("media:a", Arc::new(FixedAdapter("media:other"))).unwrap();
        assert!(matches!(
            outside.refine_media_urn(&media("media:a"), "v"),
            Err(ValueAdapterError::NotInside { .. })
        ));

        // A key fires for a base that falls under it whatever the base's text
        // begins with: prefix matching never asked this adapter.
        let asked = Arc::new(Asked(std::sync::atomic::AtomicBool::new(false)));
        let mut model_spec = ValueAdapterRegistry::new();
        model_spec.register("media:model-spec", asked.clone()).unwrap();
        model_spec
            .refine_media_urn(&media("media:enc=utf8;llm;model-spec"), "hf:x")
            .unwrap();
        assert!(asked.0.load(std::sync::atomic::Ordering::SeqCst));
    }
}
