//! MediaAdapterRegistry — tracks cartridge-provided content inspection adapters
//!
//! The registry records which cartridges have registered adapter URNs for content
//! inspection, detects ambiguity at registration time (rejecting entire cap groups),
//! and maps file extensions to the cartridges that can inspect them.

use std::fmt;
use std::sync::Arc;

use crate::media::registry::FabricRegistry;
use crate::urn::media_urn::MediaUrn;

/// Error returned when cap group registration fails due to adapter ambiguity
#[derive(Debug, Clone)]
pub struct AdapterRegistrationError {
    /// The cap group that was rejected
    pub group_name: String,
    /// The adapter URN from the new group that caused the conflict
    pub new_adapter_urn: String,
    /// The existing adapter URN it conflicts with
    pub existing_adapter_urn: String,
    /// The cap group that owns the existing adapter
    pub existing_group_name: String,
    /// The cartridge that owns the existing adapter
    pub existing_cartridge_id: String,
}

impl fmt::Display for AdapterRegistrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Cap group '{}' rejected: adapter URN '{}' conflicts with '{}' \
             (registered by group '{}' in cartridge '{}'). \
             One conforms to the other, creating ambiguity.",
            self.group_name,
            self.new_adapter_urn,
            self.existing_adapter_urn,
            self.existing_group_name,
            self.existing_cartridge_id,
        )
    }
}

impl std::error::Error for AdapterRegistrationError {}

/// A registered adapter URN with its owning group and cartridge
struct RegisteredAdapter {
    media_urn: MediaUrn,
    /// The raw URN string (for error messages and lookups)
    urn_string: String,
    group_name: String,
    cartridge_id: String,
}

/// Registry of cartridge-provided content inspection adapters
///
/// This registry:
/// 1. Tracks which cartridges have registered adapter URNs
/// 2. Detects ambiguity at registration time (rejects entire cap groups)
/// 3. Maps file extensions to cartridges that can inspect them
pub struct MediaAdapterRegistry {
    /// Registered adapter URNs from cartridge cap groups
    registered_adapters: Vec<RegisteredAdapter>,

    /// Reference to the media URN registry for extension lookups
    fabric_registry: Arc<FabricRegistry>,
}

impl std::fmt::Debug for MediaAdapterRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MediaAdapterRegistry")
            .field("adapter_count", &self.registered_adapters.len())
            .finish()
    }
}

impl MediaAdapterRegistry {
    /// Create a new empty registry with the given FabricRegistry.
    /// No adapters are registered by default — cartridges register them
    /// via `register_cap_group()`.
    pub fn new(fabric_registry: Arc<FabricRegistry>) -> Self {
        MediaAdapterRegistry {
            registered_adapters: Vec::new(),
            fabric_registry,
        }
    }

    /// Get the media URN registry
    pub fn fabric_registry(&self) -> &FabricRegistry {
        &self.fabric_registry
    }

    /// Register a cap group's adapter URNs.
    ///
    /// Checks each new adapter URN against ALL existing registered URNs.
    /// If any pair has a `conforms_to` relationship in either direction,
    /// the entire group is rejected — none of its adapters get registered.
    ///
    /// Exact re-registration is IDEMPOTENT: an adapter URN equivalent to one
    /// this same `(cartridge_id, group_name)` already registered is neither
    /// a conflict nor a second row — a cartridge attached through more than
    /// one hosting route (e.g. app-bundled and system-installed) is still
    /// one adapter provider, not an ambiguity with itself.
    ///
    /// On success, all adapter URNs from the group are added atomically.
    pub fn register_cap_group(
        &mut self,
        group_name: &str,
        adapter_urn_strs: &[String],
        cartridge_id: &str,
    ) -> Result<(), AdapterRegistrationError> {
        // Parse all new adapter URNs first — fail hard on invalid URNs
        let new_adapters: Vec<(MediaUrn, &String)> = adapter_urn_strs
            .iter()
            .map(|s| {
                let urn = MediaUrn::from_string(s).unwrap_or_else(|e| {
                    panic!(
                        "Cap group '{}' has invalid adapter URN '{}': {}",
                        group_name, s, e
                    )
                });
                (urn, s)
            })
            .collect();

        // Which new adapters are exact re-registrations by the same owner —
        // skipped in the conflict scan AND in the final insert, so repeated
        // attachment of the same cartridge stays a no-op instead of either
        // refusing (self-conflict) or duplicating rows.
        let already_registered: Vec<bool> = new_adapters
            .iter()
            .map(|(urn, urn_str)| {
                let dup = self.registered_adapters.iter().any(|existing| {
                    existing.cartridge_id == cartridge_id
                        && existing.group_name == group_name
                        && existing.media_urn.is_equivalent(urn).unwrap_or(false)
                });
                if dup {
                    tracing::warn!(
                        "Adapter URN '{}' of cap group '{}' is already registered by \
                         cartridge '{}' — the cartridge is attached through more than \
                         one hosting route; keeping the first registration and \
                         skipping this one",
                        urn_str,
                        group_name,
                        cartridge_id,
                    );
                }
                dup
            })
            .collect();

        // Check each new adapter against all existing registered adapters
        for ((new_urn, new_str), is_rereg) in new_adapters.iter().zip(&already_registered) {
            if *is_rereg {
                continue;
            }
            for existing in &self.registered_adapters {
                let new_conforms_to_existing =
                    new_urn.conforms_to(&existing.media_urn).unwrap_or(false);
                let existing_conforms_to_new =
                    existing.media_urn.conforms_to(new_urn).unwrap_or(false);

                if new_conforms_to_existing || existing_conforms_to_new {
                    return Err(AdapterRegistrationError {
                        group_name: group_name.to_string(),
                        new_adapter_urn: (*new_str).clone(),
                        existing_adapter_urn: existing.urn_string.clone(),
                        existing_group_name: existing.group_name.clone(),
                        existing_cartridge_id: existing.cartridge_id.clone(),
                    });
                }
            }
        }

        // Also check new adapters against each other within the same group
        for i in 0..new_adapters.len() {
            for j in (i + 1)..new_adapters.len() {
                let (a_urn, a_str) = &new_adapters[i];
                let (b_urn, b_str) = &new_adapters[j];

                let a_conforms_to_b = a_urn.conforms_to(b_urn).unwrap_or(false);
                let b_conforms_to_a = b_urn.conforms_to(a_urn).unwrap_or(false);

                if a_conforms_to_b || b_conforms_to_a {
                    return Err(AdapterRegistrationError {
                        group_name: group_name.to_string(),
                        new_adapter_urn: (*a_str).clone(),
                        existing_adapter_urn: (*b_str).clone(),
                        existing_group_name: group_name.to_string(),
                        existing_cartridge_id: cartridge_id.to_string(),
                    });
                }
            }
        }

        // No conflicts — register atomically (re-registered URNs already
        // have their row).
        for ((urn, urn_str), is_rereg) in new_adapters.into_iter().zip(already_registered) {
            if is_rereg {
                continue;
            }
            self.registered_adapters.push(RegisteredAdapter {
                media_urn: urn,
                urn_string: urn_str.clone(),
                group_name: group_name.to_string(),
                cartridge_id: cartridge_id.to_string(),
            });
        }

        Ok(())
    }

    /// Find adapters that can handle candidate URNs for a given file extension.
    ///
    /// 1. Queries FabricRegistry for candidate URNs via extension
    /// 2. For each candidate, finds registered adapters where the candidate
    ///    `conforms_to` the registered adapter URN
    /// 3. Returns `(cartridge_id, adapter_media_urn)` pairs
    pub fn find_adapters_for_extension(&self, ext: &str) -> Vec<(String, MediaUrn)> {
        let candidate_strings = match self.fabric_registry.media_urns_for_extension(ext) {
            Ok(urns) if !urns.is_empty() => urns,
            _ => return Vec::new(),
        };

        let candidates: Vec<MediaUrn> = candidate_strings
            .iter()
            .filter_map(|s| MediaUrn::from_string(s).ok())
            .collect();

        let mut results: Vec<(String, MediaUrn)> = Vec::new();
        let mut seen_cartridges: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        for registered in &self.registered_adapters {
            // Check if any candidate conforms to this registered adapter's URN
            let matches = candidates
                .iter()
                .any(|c| c.conforms_to(&registered.media_urn).unwrap_or(false));

            if matches && seen_cartridges.insert(registered.cartridge_id.clone()) {
                results.push((
                    registered.cartridge_id.clone(),
                    registered.media_urn.clone(),
                ));
            }
        }

        results
    }

    /// Quick check for UI queries — returns true if any registered adapter
    /// handles candidate URNs for this extension.
    pub fn has_adapter_for_extension(&self, ext: &str) -> bool {
        let candidate_strings = match self.fabric_registry.media_urns_for_extension(ext) {
            Ok(urns) if !urns.is_empty() => urns,
            _ => return false,
        };

        let candidates: Vec<MediaUrn> = candidate_strings
            .iter()
            .filter_map(|s| MediaUrn::from_string(s).ok())
            .collect();

        self.registered_adapters.iter().any(|registered| {
            candidates
                .iter()
                .any(|c| c.conforms_to(&registered.media_urn).unwrap_or(false))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Build a `FabricRegistry` pre-seeded with a JSON media def (the
    /// only extension these tests reference). The registry hydrates
    /// extensions from spec arrival; tests must seed explicitly.
    fn create_test_registry() -> (Arc<FabricRegistry>, TempDir) {
        use crate::StoredMediaDef;
        let temp_dir = TempDir::new().unwrap();
        let registry = FabricRegistry::new_for_test();
        registry.insert_cached_media_def_for_test(StoredMediaDef {
            version: 0,
            urn: "media:fmt=json;record".to_string(),
            media_type: "application/json".to_string(),
            title: "JSON".to_string(),
            profile_uri: None,
            schema: None,
            description: None,
            documentation: None,
            validation: None,
            metadata: None,
            extensions: vec!["json".to_string()],
        });
        (Arc::new(registry), temp_dir)
    }

    // TEST1276: Registration of a cap group with non-conflicting adapters succeeds
    #[test]
    fn test1276_register_non_conflicting() {
        let (fabric_registry, _temp) = create_test_registry();
        let mut registry = MediaAdapterRegistry::new(fabric_registry);

        let result = registry.register_cap_group(
            "text-formats",
            &["media:fmt=json".to_string(), "media:fmt=yaml".to_string()],
            "txtcartridge",
        );
        assert!(
            result.is_ok(),
            "Non-conflicting adapters must register: {:?}",
            result.err()
        );
        assert_eq!(registry.registered_adapters.len(), 2);
    }

    // TEST1478: exact re-registration is idempotent — the SAME cartridge
    // re-registering the SAME group with the SAME adapter URNs (a cartridge
    // attached through two hosting routes, e.g. app-bundled AND
    // system-installed) is a no-op, not a self-conflict and not duplicate
    // rows. A DIFFERENT cartridge claiming the same URN stays rejected.
    #[test]
    fn test1478_exact_reregistration_is_idempotent() {
        let (fabric_registry, _temp) = create_test_registry();
        let mut registry = MediaAdapterRegistry::new(fabric_registry);

        let urns = ["media:fmt=json".to_string(), "media:fmt=yaml".to_string()];
        registry
            .register_cap_group("text-formats", &urns, "txtcartridge")
            .unwrap();
        let result = registry.register_cap_group("text-formats", &urns, "txtcartridge");
        assert!(
            result.is_ok(),
            "re-registering the identical group must be a no-op: {:?}",
            result.err()
        );
        assert_eq!(
            registry.registered_adapters.len(),
            2,
            "re-registration must not duplicate adapter rows"
        );

        // A partially-new group from the same owner registers only the new URN.
        let extended = [
            "media:fmt=json".to_string(),
            "media:fmt=toml".to_string(),
        ];
        registry
            .register_cap_group("text-formats", &extended, "txtcartridge")
            .expect("known URNs skip, new URNs register");
        assert_eq!(registry.registered_adapters.len(), 3);

        // Another cartridge claiming an identical URN is still ambiguity.
        let result =
            registry.register_cap_group("text-formats", &["media:fmt=json".to_string()], "other");
        assert!(
            result.is_err(),
            "an identical URN from a DIFFERENT cartridge must stay rejected"
        );
    }

    // TEST1277: Registration of a cap group with an adapter that conforms_to an existing adapter is rejected
    #[test]
    fn test1277_reject_conforming_overlap() {
        let (fabric_registry, _temp) = create_test_registry();
        let mut registry = MediaAdapterRegistry::new(fabric_registry);

        // Register group A with media:fmt=json
        registry
            .register_cap_group("group-a", &["media:fmt=json".to_string()], "cartridge-a")
            .unwrap();

        // Try to register group B with media:fmt=json;record (conforms to media:fmt=json)
        let result = registry.register_cap_group(
            "group-b",
            &["media:fmt=json;record".to_string()],
            "cartridge-b",
        );
        assert!(result.is_err(), "Conforming overlap must be rejected");

        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("group-b"),
            "Error must name the rejected group"
        );
        assert!(
            err.to_string().contains("group-a"),
            "Error must name the conflicting group"
        );
    }

    // TEST1278: Registration rejects the entire group — no partial registration
    #[test]
    fn test1278_reject_entire_group() {
        let (fabric_registry, _temp) = create_test_registry();
        let mut registry = MediaAdapterRegistry::new(fabric_registry);

        // Register an adapter for media:fmt=json
        registry
            .register_cap_group("group-a", &["media:fmt=json".to_string()], "cartridge-a")
            .unwrap();

        // Try to register group with 3 adapters, one of which conflicts
        let result = registry.register_cap_group(
            "group-b",
            &[
                "media:fmt=yaml".to_string(), // ok
                "media:fmt=json".to_string(), // conflicts with media:fmt=json
                "media:fmt=csv".to_string(),  // ok
            ],
            "cartridge-b",
        );
        assert!(result.is_err());

        // Only the original adapter should remain
        assert_eq!(
            registry.registered_adapters.len(),
            1,
            "Rejected group must not leave partial registrations"
        );
    }

    // TEST1279: Intra-group conflict (two adapters within same group overlap) is rejected
    #[test]
    fn test1279_intra_group_conflict() {
        let (fabric_registry, _temp) = create_test_registry();
        let mut registry = MediaAdapterRegistry::new(fabric_registry);

        let result = registry.register_cap_group(
            "bad-group",
            &[
                "media:fmt=json".to_string(),
                "media:fmt=json".to_string(), // conforms to media:fmt=json
            ],
            "cartridge-x",
        );
        assert!(result.is_err(), "Intra-group conflict must be rejected");
        assert_eq!(registry.registered_adapters.len(), 0);
    }

    // TEST1280: find_adapters_for_extension returns correct cartridge IDs
    #[test]
    fn test1280_find_adapters_for_extension() {
        let (fabric_registry, _temp) = create_test_registry();
        let mut registry = MediaAdapterRegistry::new(fabric_registry);

        // Register adapter for media:fmt=json (which should match .json extension candidates)
        registry
            .register_cap_group(
                "text-group",
                &["media:fmt=json".to_string()],
                "txtcartridge",
            )
            .unwrap();

        let results = registry.find_adapters_for_extension("json");
        // Should find txtcartridge since json extension candidates conform to media:fmt=json
        assert!(
            !results.is_empty(),
            "Must find adapter for json extension (found: {:?})",
            results
        );
        assert_eq!(results[0].0, "txtcartridge");
    }

    // TEST1281: has_adapter_for_extension returns false for unregistered extension
    #[test]
    fn test1281_no_adapter_for_unknown() {
        let (fabric_registry, _temp) = create_test_registry();
        let registry = MediaAdapterRegistry::new(fabric_registry);

        assert!(
            !registry.has_adapter_for_extension("xyz_unknown"),
            "Unknown extension must return false"
        );
    }
}
