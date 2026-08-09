//! Unified cap-based manifest interface
//!
//! This module defines the unified manifest interface with standardized cap-based declarations.
//! This replaces the earlier separate per-language manifest types with a single canonical format.

use crate::bifaci::cartridge_repo::CartridgeChannel;
use crate::standard::caps::CAP_IDENTITY;
use crate::urn::cap_urn::CapUrn;
use crate::Cap;
use serde::{Deserialize, Serialize};

/// A cap group bundles caps and adapter URNs as an atomic registration unit.
///
/// If any adapter in the group creates ambiguity with an already-registered adapter,
/// the entire group is rejected — none of its caps or adapters get registered.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapGroup {
    /// Group name (for diagnostics and error messages)
    pub name: String,

    /// Caps in this group
    pub caps: Vec<Cap>,

    /// Media URNs this group's adapter handles.
    /// These are matched via `conforms_to` during registration — they are not patterns,
    /// they are declared URNs checked for overlap with existing registrations.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub adapter_urns: Vec<String>,
}

/// Unified cap manifest for --manifest output.
///
/// `(registry_url, channel, name, version)` is the cartridge's full
/// identity. The channel and registry are reported by the cartridge
/// process during HELLO so the host can verify the cartridge it's
/// about to attach matches what the install context (cartridge.json)
/// declared. Mismatches at any leg are caught early instead of
/// silently merging artefacts that came from different registries
/// or channels.
///
/// `registry_url` is `None` for dev builds (the cartridge was built
/// without `MFR_CARTRIDGE_REGISTRY_URL` set). It is required-but-nullable on
/// the wire — present-and-null means dev; absent means the cartridge
/// SDK is too old to know the field exists, which is a parse error.
/// The `Deserialize` impl is manual to enforce this stricter
/// contract; stock serde collapses absent and explicit-null for
/// `Option<T>`.
#[derive(Debug, Clone, Serialize)]
pub struct CapManifest {
    /// Component name
    pub name: String,

    /// Component version
    pub version: String,

    /// Distribution channel the cartridge was built for.
    /// (release / nightly). Required.
    pub channel: CartridgeChannel,

    /// Registry the cartridge was built for. Baked into the binary
    /// at compile time from `MFR_CARTRIDGE_REGISTRY_URL` (Rust:
    /// `option_env!()`). `None` means the cartridge was built as a
    /// dev artefact and is only valid under the on-disk `dev/`
    /// folder. Re-publishing a dev cartridge to a registry requires
    /// rebuilding with the env var set; the registry URL is part
    /// of the build's identity, not install-time metadata.
    pub registry_url: Option<String>,

    /// Component description
    pub description: String,

    /// Cap groups — bundles of caps + adapter URNs registered atomically.
    /// All caps must be in a cap group. Groups without adapter URNs are valid
    /// (they just don't contribute content inspection adapters).
    pub cap_groups: Vec<CapGroup>,

    /// Component author/maintainer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,

    /// Human-readable page URL for the cartridge (e.g., repository page, documentation)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_url: Option<String>,
}

impl<'de> Deserialize<'de> for CapManifest {
    /// Manual deserializer enforcing "required-but-nullable" for
    /// `registry_url`: the JSON key MUST be present, the value MAY
    /// be null. Mirrors the same enforcement in `CartridgeJson`.
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;

        let value = serde_json::Value::deserialize(deserializer)?;
        let obj = value
            .as_object()
            .ok_or_else(|| D::Error::custom("CapManifest must be a JSON object"))?;
        if !obj.contains_key("registry_url") {
            return Err(D::Error::missing_field("registry_url"));
        }

        #[derive(Deserialize)]
        struct CapManifestInner {
            name: String,
            version: String,
            channel: CartridgeChannel,
            registry_url: Option<String>,
            description: String,
            cap_groups: Vec<CapGroup>,
            #[serde(default)]
            author: Option<String>,
            #[serde(default)]
            page_url: Option<String>,
        }
        let inner = serde_json::from_value::<CapManifestInner>(value).map_err(D::Error::custom)?;
        Ok(CapManifest {
            name: inner.name,
            version: inner.version,
            channel: inner.channel,
            registry_url: inner.registry_url,
            description: inner.description,
            cap_groups: inner.cap_groups,
            author: inner.author,
            page_url: inner.page_url,
        })
    }
}

impl CapManifest {
    /// Create a new cap manifest with cap groups.
    /// `channel` is required — the cartridge must declare which
    /// channel it was built for so the install context (cartridge.json)
    /// and the cartridge's self-report agree. `registry_url` is the
    /// optional URL of the registry the cartridge was built for
    /// (`None` ⇔ dev build); the cartridge SDK macro reads this from
    /// `option_env!("MFR_CARTRIDGE_REGISTRY_URL")` so it is set correctly at
    /// compile time and never inferred at runtime.
    pub fn new(
        name: String,
        version: String,
        channel: CartridgeChannel,
        registry_url: Option<String>,
        description: String,
        cap_groups: Vec<CapGroup>,
    ) -> Self {
        Self {
            name,
            version,
            channel,
            registry_url,
            description,
            cap_groups,
            author: None,
            page_url: None,
        }
    }

    /// Returns all caps from all cap groups.
    pub fn all_caps(&self) -> Vec<&Cap> {
        let mut result: Vec<&Cap> = Vec::new();
        for group in &self.cap_groups {
            result.extend(group.caps.iter());
        }
        result
    }

    /// Set the author of the component
    pub fn with_author(mut self, author: String) -> Self {
        self.author = Some(author);
        self
    }

    /// Set the page URL for the cartridge (human-readable page, e.g., repository)
    pub fn with_page_url(mut self, page_url: String) -> Self {
        self.page_url = Some(page_url);
        self
    }

    /// Validate that CAP_IDENTITY is declared in this manifest.
    /// Checks both top-level caps and caps within cap_groups.
    /// Fails hard if missing — identity is mandatory in every capset.
    pub fn validate(&self) -> Result<(), String> {
        let identity_urn = CapUrn::from_string(CAP_IDENTITY)
            .map_err(|e| format!("BUG: CAP_IDENTITY constant is invalid: {}", e))?;
        let has_identity = self
            .all_caps()
            .iter()
            .any(|cap| identity_urn.conforms_to(&cap.urn));
        if !has_identity {
            return Err(format!(
                "Manifest missing required CAP_IDENTITY ({})",
                CAP_IDENTITY
            ));
        }
        Ok(())
    }
}

/// Compile-time validation for `MFR_CARTRIDGE_REGISTRY_URL`.
///
/// Valid states:
/// - `None`   => dev build; registry identity is absent and the build
///               must use the on-disk `dev/` slot.
/// - `Some(s)` where `s` is non-empty => published-registry build.
///
/// Invalid state:
/// - `Some("")` => caller exported the variable with an empty value.
///   This is neither a dev build nor a valid registry identity, and it
///   must fail hard at compile time so the build cannot silently hash the
///   empty string into a fake registry slug.
pub const fn registry_url_from_build_env(raw: Option<&'static str>) -> Option<&'static str> {
    match raw {
        None => None,
        Some(url) => {
            if url.len() == 0 {
                panic!(
                    "MFR_CARTRIDGE_REGISTRY_URL must be unset for dev builds or set to a non-empty registry URL for published builds; empty string is invalid"
                );
            }
            Some(url)
        }
    }
}

/// Trait for components to provide metadata about themselves
pub trait ComponentMetadata {
    /// Get component manifest
    fn component_manifest(&self) -> CapManifest;

    /// Get all component caps from all cap groups
    fn caps(&self) -> Vec<Cap> {
        let manifest = self.component_manifest();
        let mut all = Vec::new();
        for group in manifest.cap_groups {
            all.extend(group.caps);
        }
        all
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Cap, CapUrn};
    use std::collections::HashMap;

    fn test_urn(tags: &str) -> String {
        format!(r#"cap:in="media:void";out="media:record";{}"#, tags)
    }

    /// Helper: wrap caps in a default cap group with no adapter URNs
    fn default_group(caps: Vec<Cap>) -> CapGroup {
        CapGroup {
            name: "default".to_string(),
            caps,
            adapter_urns: Vec::new(),
        }
    }

    // TEST148: Manifest creation with cap groups
    #[test]
    fn test148_cap_manifest_creation() {
        let urn = CapUrn::from_string(&test_urn("extract;target=metadata")).unwrap();
        let cap = Cap::new(
            urn,
            "Extract Metadata".to_string(),
            vec!["extract-metadata".to_string()],
        );

        let manifest = CapManifest::new(
            "TestComponent".to_string(),
            "0.1.0".to_string(),
            CartridgeChannel::Release,
            None,
            "A test component for validation".to_string(),
            vec![default_group(vec![cap])],
        );

        assert_eq!(manifest.name, "TestComponent");
        assert_eq!(manifest.channel, CartridgeChannel::Release);
        assert!(manifest.registry_url.is_none());
        assert_eq!(manifest.cap_groups.len(), 1);
        assert_eq!(manifest.all_caps().len(), 1);
        assert!(manifest.author.is_none());
    }

    // TEST117: A manifest's channel round-trips through serde and the
    // serialized form uses the canonical lowercase wire word
    // ("release" / "nightly"). A missing or unrecognized channel is
    // a hard parse error — no defaults.
    #[test]
    fn test117_cap_manifest_channel_roundtrip() {
        let urn = CapUrn::from_string(&test_urn("extract;target=metadata")).unwrap();
        let cap = Cap::new(
            urn,
            "Extract Metadata".to_string(),
            vec!["extract-metadata".to_string()],
        );

        let manifest = CapManifest::new(
            "TestComponent".to_string(),
            "0.1.0".to_string(),
            CartridgeChannel::Nightly,
            Some("https://cartridges.machinefabric.com/manifest".to_string()),
            "Channel round-trip".to_string(),
            vec![default_group(vec![cap])],
        );
        let json = serde_json::to_string(&manifest).unwrap();
        assert!(
            json.contains("\"channel\":\"nightly\""),
            "expected lowercase wire form, got: {}",
            json
        );
        // registry_url round-trips as the exact string the operator
        // typed — used to validate against the on-disk slug at scan
        // time, so a single byte of drift here would silently break
        // discovery.
        assert!(
            json.contains("\"registry_url\":\"https://cartridges.machinefabric.com/manifest\""),
            "expected verbatim registry_url in serialized form, got: {}",
            json
        );

        let parsed: CapManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.channel, CartridgeChannel::Nightly);
        assert_eq!(
            parsed.registry_url.as_deref(),
            Some("https://cartridges.machinefabric.com/manifest")
        );

        // No-channel JSON must fail to parse.
        let no_channel = r#"{"name":"X","version":"1.0.0","registry_url":null,"description":"x","cap_groups":[]}"#;
        let result: Result<CapManifest, _> = serde_json::from_str(no_channel);
        assert!(
            result.is_err(),
            "manifest without `channel` must fail to parse, got: {:?}",
            result
        );

        // No-registry_url JSON must fail to parse — the field is
        // required-but-nullable, so a missing key means an old SDK,
        // which can't be trusted to know the new schema.
        let no_registry = r#"{"name":"X","version":"1.0.0","channel":"nightly","description":"x","cap_groups":[]}"#;
        let result: Result<CapManifest, _> = serde_json::from_str(no_registry);
        assert!(
            result.is_err(),
            "manifest without `registry_url` must fail to parse, got: {:?}",
            result
        );

        // Bogus channel string must fail.
        let bogus = r#"{"name":"X","version":"1.0.0","channel":"staging","registry_url":null,"description":"x","cap_groups":[]}"#;
        let result: Result<CapManifest, _> = serde_json::from_str(bogus);
        assert!(
            result.is_err(),
            "manifest with channel='staging' must fail to parse, got: {:?}",
            result
        );
    }

    // TEST118: A dev manifest (built without `MFR_CARTRIDGE_REGISTRY_URL`) carries
    // `registry_url: null` and serializes the field explicitly. The
    // null-vs-absent distinction matters because the parser refuses
    // to accept absent (test117) — so an old SDK can't accidentally
    // pass for a dev build.
    #[test]
    fn test118_dev_manifest_registry_url_is_explicit_null() {
        let urn = CapUrn::from_string(&test_urn("dev")).unwrap();
        let cap = Cap::new(urn, "Dev".to_string(), vec!["dev".to_string()]);
        let manifest = CapManifest::new(
            "DevComponent".to_string(),
            "0.1.0".to_string(),
            CartridgeChannel::Nightly,
            None,
            "Dev build".to_string(),
            vec![default_group(vec![cap])],
        );
        let json = serde_json::to_string(&manifest).unwrap();
        assert!(
            json.contains("\"registry_url\":null"),
            "dev manifest must serialize registry_url=null explicitly, got: {}",
            json
        );
        let parsed: CapManifest = serde_json::from_str(&json).unwrap();
        assert!(parsed.registry_url.is_none());
    }

    // TEST149: Author field
    #[test]
    fn test149_cap_manifest_with_author() {
        let urn = CapUrn::from_string(&test_urn("extract;target=metadata")).unwrap();
        let cap = Cap::new(
            urn,
            "Extract Metadata".to_string(),
            vec!["extract-metadata".to_string()],
        );

        let manifest = CapManifest::new(
            "TestComponent".to_string(),
            "0.1.0".to_string(),
            CartridgeChannel::Release,
            None,
            "A test component".to_string(),
            vec![default_group(vec![cap])],
        )
        .with_author("Test Author".to_string());

        assert_eq!(manifest.author, Some("Test Author".to_string()));
    }

    // TEST150: JSON roundtrip
    #[test]
    fn test150_cap_manifest_json_serialization() {
        use crate::{ArgSource, CapArg};

        let urn = CapUrn::from_string(&test_urn("extract;target=metadata")).unwrap();
        let mut cap = Cap::new(
            urn,
            "Extract Metadata".to_string(),
            vec!["extract-metadata".to_string()],
        );
        cap.add_arg(CapArg::new(
            "media:ext=pdf",
            true,
            vec![ArgSource::Stdin {
                stdin: "media:ext=pdf".to_string(),
            }],
        ));
        cap.add_arg(CapArg::with_full_definition(
            "media:chunk-size;numeric",
            false,
            false,
            vec![ArgSource::CliFlag {
                cli_flag: "--chunk-size".to_string(),
            }],
            Some("Chunk size".to_string()),
            Some(serde_json::json!(400)),
            Some(serde_json::json!({"unit": "words"})),
        ));
        cap.add_arg(CapArg::with_full_definition(
            "media:bool;enc=utf-8;timestamps",
            false,
            false,
            vec![ArgSource::CliFlag {
                cli_flag: "--timestamps".to_string(),
            }],
            Some("Include timestamps".to_string()),
            Some(serde_json::json!(false)),
            None,
        ));

        let manifest = CapManifest::new(
            "TestComponent".to_string(),
            "0.1.0".to_string(),
            CartridgeChannel::Release,
            None,
            "A test component".to_string(),
            vec![default_group(vec![cap])],
        )
        .with_author("Test Author".to_string());

        let json = serde_json::to_string(&manifest).unwrap();
        assert!(json.contains("\"name\":\"TestComponent\""));
        assert!(json.contains("\"author\":\"Test Author\""));
        assert!(json.contains("\"cap_groups\""));
        assert!(json.contains("\"default_value\":400"));
        assert!(json.contains("\"default_value\":false"));

        let deserialized: CapManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, manifest.name);
        assert_eq!(deserialized.all_caps().len(), manifest.all_caps().len());
        let decoded_cap = &deserialized.all_caps()[0];
        assert_eq!(
            decoded_cap.args[1].default_value,
            Some(serde_json::json!(400))
        );
        assert_eq!(
            decoded_cap.args[1].metadata,
            Some(serde_json::json!({"unit": "words"}))
        );
        assert_eq!(
            decoded_cap.args[2].default_value,
            Some(serde_json::json!(false))
        );
    }

    // TEST151: Missing required fields fail
    #[test]
    fn test151_cap_manifest_required_fields() {
        let invalid_json = r#"{"name": "TestComponent"}"#;
        let result: Result<CapManifest, _> = serde_json::from_str(invalid_json);
        assert!(result.is_err());
    }

    // TEST152: Multiple caps across groups
    #[test]
    fn test152_cap_manifest_with_multiple_caps() {
        let id1 = CapUrn::from_string(&test_urn("extract;target=metadata")).unwrap();
        let cap1 = Cap::new(
            id1,
            "Extract Metadata".to_string(),
            vec!["extract-metadata".to_string()],
        );

        let id2 = CapUrn::from_string(&test_urn("extract;target=outline")).unwrap();
        let mut metadata = HashMap::new();
        metadata.insert("supports_outline".to_string(), "true".to_string());
        let cap2 = Cap::with_metadata(
            id2,
            "Extract Outline".to_string(),
            vec!["extract-outline".to_string()],
            metadata,
        );

        let manifest = CapManifest::new(
            "MultiCapComponent".to_string(),
            "1.0.0".to_string(),
            CartridgeChannel::Release,
            None,
            "Component with multiple caps".to_string(),
            vec![default_group(vec![cap1, cap2])],
        );

        let all = manifest.all_caps();
        assert_eq!(all.len(), 2);
        assert!(all[0].urn_string().contains("target=metadata"));
        assert!(all[1].urn_string().contains("target=outline"));
        assert!(all[1].has_metadata("supports_outline"));
    }

    // TEST153: Empty cap groups
    #[test]
    fn test153_cap_manifest_empty_cap_groups() {
        let manifest = CapManifest::new(
            "EmptyComponent".to_string(),
            "1.0.0".to_string(),
            CartridgeChannel::Release,
            None,
            "Component with no caps".to_string(),
            vec![],
        );

        assert_eq!(manifest.all_caps().len(), 0);

        let json = serde_json::to_string(&manifest).unwrap();
        let deserialized: CapManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.all_caps().len(), 0);
    }

    // TEST154: Optional author field omitted in serialization
    #[test]
    fn test154_cap_manifest_optional_author_field() {
        let urn = CapUrn::from_string(&test_urn("validate;file")).unwrap();
        let cap = Cap::new(urn, "Validate".to_string(), vec!["validate".to_string()]);

        let manifest = CapManifest::new(
            "ValidatorComponent".to_string(),
            "1.0.0".to_string(),
            CartridgeChannel::Release,
            None,
            "File validation component".to_string(),
            vec![default_group(vec![cap])],
        );

        let json = serde_json::to_string(&manifest).unwrap();
        assert!(!json.contains("\"author\""));
    }

    // TEST155: ComponentMetadata trait
    #[test]
    fn test155_component_metadata_trait() {
        struct TestComponent {
            name: String,
            cap_groups: Vec<CapGroup>,
        }

        impl ComponentMetadata for TestComponent {
            fn component_manifest(&self) -> CapManifest {
                CapManifest::new(
                    self.name.clone(),
                    "1.0.0".to_string(),
                    CartridgeChannel::Release,
                    None,
                    "Test component".to_string(),
                    self.cap_groups.clone(),
                )
            }
        }

        let urn = CapUrn::from_string(&test_urn("test;type=component")).unwrap();
        let cap = Cap::new(urn, "Test Component".to_string(), vec!["test".to_string()]);

        let component = TestComponent {
            name: "TestImpl".to_string(),
            cap_groups: vec![default_group(vec![cap])],
        };

        let caps = component.caps();
        assert_eq!(caps.len(), 1);
        assert!(caps[0].urn_string().contains("test"));
    }

    // TEST475: validate() passes with CAP_IDENTITY in a cap group
    #[test]
    fn test475_validate_passes_with_identity() {
        let identity_urn = CapUrn::from_string(CAP_IDENTITY).unwrap();
        let cap = Cap::new(
            identity_urn,
            "Identity".to_string(),
            vec!["identity".to_string()],
        );
        let manifest = CapManifest::new(
            "TestCartridge".to_string(),
            "1.0.0".to_string(),
            CartridgeChannel::Release,
            None,
            "Test".to_string(),
            vec![default_group(vec![cap])],
        );
        assert!(
            manifest.validate().is_ok(),
            "Manifest with CAP_IDENTITY must validate"
        );
    }

    // TEST476: validate() fails without CAP_IDENTITY
    #[test]
    fn test476_validate_fails_without_identity() {
        let specific_urn = CapUrn::from_string(&test_urn("convert")).unwrap();
        let cap = Cap::new(
            specific_urn,
            "Convert".to_string(),
            vec!["convert".to_string()],
        );
        let manifest = CapManifest::new(
            "TestCartridge".to_string(),
            "1.0.0".to_string(),
            CartridgeChannel::Release,
            None,
            "Test".to_string(),
            vec![default_group(vec![cap])],
        );
        let result = manifest.validate();
        assert!(
            result.is_err(),
            "Manifest without CAP_IDENTITY must fail validation"
        );
        assert!(result.unwrap_err().contains("CAP_IDENTITY"));
    }

    // TEST1284: Cap group with adapter URNs serializes and deserializes correctly
    #[test]
    fn test1284_cap_group_with_adapter_urns() {
        let urn = CapUrn::from_string(&test_urn("convert")).unwrap();
        let cap = Cap::new(urn, "Convert".to_string(), vec!["convert".to_string()]);

        let group = CapGroup {
            name: "data-formats".to_string(),
            caps: vec![cap],
            adapter_urns: vec!["media:fmt=json".to_string(), "media:fmt=csv".to_string()],
        };

        let manifest = CapManifest::new(
            "TestCartridge".to_string(),
            "1.0.0".to_string(),
            CartridgeChannel::Release,
            None,
            "Test".to_string(),
            vec![group],
        );

        let json = serde_json::to_string(&manifest).unwrap();
        assert!(json.contains("\"adapter_urns\""));
        assert!(json.contains("media:fmt=json"));
        assert!(json.contains("media:fmt=csv"));

        let deserialized: CapManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.cap_groups[0].adapter_urns.len(), 2);
    }

    // TEST1872: `registry_url_from_build_env` passes a non-empty registry URL
    // through unchanged. This is the function that decides the engine's baked
    // PRIMARY registry (surfaced over SystemService.HealthStatus); a published
    // build must report exactly the URL it was compiled with.
    #[test]
    fn test1872_registry_url_from_build_env_passes_through_nonempty() {
        let url = "https://cartridges.machinefabric.com/manifest";
        assert_eq!(registry_url_from_build_env(Some(url)), Some(url));
    }

    // TEST1873: an unset env (None) yields None — a dev build has no baked
    // registry, so the engine reports an empty primary-registry URL and loads
    // only `dev/` cartridges. This is the dev-engine contract the registry
    // sheets rely on to omit the read-only "Primary · built-in" row.
    #[test]
    fn test1873_registry_url_from_build_env_none_for_dev() {
        assert_eq!(registry_url_from_build_env(None), None);
    }

    // TEST1874: an exported-but-empty env (`Some("")`) is neither a dev build
    // nor a valid identity and MUST fail hard at compile time, so the build can
    // never silently hash the empty string into a fake registry slug. We assert
    // the panic rather than letting a bogus empty primary registry ship.
    #[test]
    #[should_panic(expected = "MFR_CARTRIDGE_REGISTRY_URL must be unset")]
    fn test1874_registry_url_from_build_env_rejects_empty_string() {
        let _ = registry_url_from_build_env(Some(""));
    }

    // TEST6363: Cap manifest with page_url — the optional page_url is carried
    // and serialized as `page_url`.
    #[test]
    fn test6363_cap_manifest_with_page_url() {
        let urn = CapUrn::from_string(&test_urn("extract;target=metadata")).unwrap();
        let cap = Cap::new(
            urn,
            "Metadata Extractor".to_string(),
            vec!["extract-metadata".to_string()],
        );
        let manifest = CapManifest::new(
            "TestComponent".to_string(),
            "0.1.0".to_string(),
            CartridgeChannel::Release,
            None,
            "A test component for validation".to_string(),
            vec![default_group(vec![cap])],
        )
        .with_author("Test Author".to_string())
        .with_page_url("https://github.com/example/test".to_string());

        assert_eq!(
            manifest.page_url.as_deref(),
            Some("https://github.com/example/test")
        );
        let json = serde_json::to_string(&manifest).unwrap();
        assert!(
            json.contains("\"page_url\":\"https://github.com/example/test\""),
            "expected page_url in serialized form, got: {}",
            json
        );
    }

    // TEST6371: Cap manifest compatibility — cartridge-style and legacy-style
    // manifests serialize to the same JSON shape (same keys).
    #[test]
    fn test6371_cap_manifest_compatibility() {
        let urn = CapUrn::from_string(&test_urn("process")).unwrap();
        let cap = Cap::new(
            urn,
            "Data Processor".to_string(),
            vec!["process".to_string()],
        );
        let cartridge = CapManifest::new(
            "CartridgeComponent".to_string(),
            "0.1.0".to_string(),
            CartridgeChannel::Release,
            None,
            "Cartridge-style component".to_string(),
            vec![default_group(vec![cap.clone()])],
        );
        let legacy_cartridge = CapManifest::new(
            "LegacyCartridgeComponent".to_string(),
            "0.1.0".to_string(),
            CartridgeChannel::Release,
            None,
            "Legacy-style component".to_string(),
            vec![default_group(vec![cap])],
        );
        let cartridge_map: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&serde_json::to_string(&cartridge).unwrap()).unwrap();
        let legacy_cartridge_map: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&serde_json::to_string(&legacy_cartridge).unwrap()).unwrap();
        assert_eq!(cartridge_map.len(), legacy_cartridge_map.len());
        for key in ["name", "version", "description", "cap_groups", "channel"] {
            assert!(cartridge_map.contains_key(key), "missing key {key}");
            assert!(legacy_cartridge_map.contains_key(key), "missing key {key}");
        }
    }

    // TEST7152: an empty `adapter_urns` is omitted from a serialized cap group.
    //
    // Most cartridges claim no adapters, so a mirror that wrote `[]` put an
    // extra key in nearly every manifest it produced — invisible to that
    // mirror's own tests, and a difference the moment two languages' manifests
    // for the same cartridge are compared.
    #[test]
    fn test7152_empty_adapter_urns_is_omitted() {
        let group = CapGroup {
            name: "default".to_string(),
            caps: Vec::new(),
            adapter_urns: Vec::new(),
        };
        let json = serde_json::to_value(&group).expect("group serializes");
        assert!(
            json.get("adapter_urns").is_none(),
            "an empty adapter_urns must be omitted, not written as []"
        );

        // A group that DOES claim adapters still writes them.
        let claiming = CapGroup {
            name: "default".to_string(),
            caps: Vec::new(),
            adapter_urns: vec!["media:ext=pdf".to_string()],
        };
        let claiming_json = serde_json::to_value(&claiming).expect("group serializes");
        assert_eq!(
            claiming_json
                .get("adapter_urns")
                .and_then(|v| v.as_array())
                .map(|a| a.len()),
            Some(1),
            "a non-empty adapter_urns must be written"
        );
    }
}
