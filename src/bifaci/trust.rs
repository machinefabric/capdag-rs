//! MachineFabric's signing, as its clients verify it.
//!
//! The chain itself — 2-of-3 roots, release-key certificates bound to a label,
//! signatures over exact bytes, charters — is `signed_release`'s. What is
//! MachineFabric's is declared here: the words its documents are written in,
//! the label its charters are signed under, the kinds of registry it charters,
//! and the roots and label this build was baked with.
//!
//! Two trusts, kept apart by type: the **content trust** ([`content_trust`])
//! verifies manifests and artifacts under this build's label; the **charter
//! trust** ([`charter_trust`]) verifies charters, always under [`CHARTER_LABEL`].
//! A charter can never vouch for an artifact, and a content key can never
//! charter a registry.

use std::sync::OnceLock;

use signed_release::{CharterRules, CharterTrust, Formats, KindRule, LocationRule, Trust};

/// The `format` of a release-key certificate.
pub const CERTIFICATE_FORMAT: &str = "machinefabric-release-key-cert/1";
/// The `format` of a manifest's signature envelope.
pub const MANIFEST_ENVELOPE_FORMAT: &str = "machinefabric-manifest-sig/1";
/// The `format` of a charter body.
pub const CHARTER_FORMAT: &str = "machinefabric-registry-charter/1";
/// The `format` of a charter's signature envelope. Distinct from a manifest's,
/// so a prod-signed manifest can never be presented as a charter.
pub const CHARTER_ENVELOPE_FORMAT: &str = "machinefabric-charter-sig/1";
/// The label every charter's certificate carries: MachineFabric charters every
/// registry with its prod key, staging's and lab's included.
pub const CHARTER_LABEL: &str = "prod";
/// The folder name of cartridges built with no registry. No charter may claim
/// it.
pub const DEV_SLUG: &str = "dev";
/// The charter field a cartridges registry names its fabric in.
pub const FABRIC_FIELD: &str = "fabric";

/// The kinds of registry MachineFabric charters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RegistryKind {
    Cartridges,
    Fabric,
    Releases,
}

impl RegistryKind {
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::Cartridges => "cartridges",
            Self::Fabric => "fabric",
            Self::Releases => "releases",
        }
    }
}

/// Compile-time check of `MFR_CARTRIDGE_ROOT_PUBKEYS`. Unset is a dev build,
/// which verifies no registry's contents; set, it is the comma-separated root
/// public keys. Exported but empty is neither, and fails the build, so no build
/// can ship with verification off while claiming a registry.
pub const fn root_pubkeys_from_build_env(raw: Option<&'static str>) -> Option<&'static str> {
    match raw {
        Some(keys) if keys.is_empty() => panic!(
            "MFR_CARTRIDGE_ROOT_PUBKEYS must be unset for dev builds or set to a comma-separated list of base64 minisign root public keys for published builds; empty string is invalid"
        ),
        other => other,
    }
}

/// Compile-time check of `MFR_SIGNING_ENVIRONMENT`, the label this build's
/// certificates must bear. Unset is a dev build; exported but empty fails the
/// build.
pub const fn signing_environment_from_build_env(raw: Option<&'static str>) -> Option<&'static str> {
    match raw {
        Some(label) if label.is_empty() => panic!(
            "MFR_SIGNING_ENVIRONMENT must be unset for dev builds or set to the label published builds verify; empty string is invalid"
        ),
        other => other,
    }
}

/// How MachineFabric's manifests and certificates declare themselves.
pub fn content_formats() -> Formats {
    Formats { certificate: CERTIFICATE_FORMAT.to_string(), envelope: MANIFEST_ENVELOPE_FORMAT.to_string() }
}

/// What MachineFabric's charters are.
pub fn charter_rules() -> &'static CharterRules {
    static RULES: OnceLock<CharterRules> = OnceLock::new();
    RULES.get_or_init(|| {
        let location = |name: &str, placeholders: &[&str]| LocationRule {
            name: name.to_string(),
            placeholders: placeholders.iter().map(|p| p.to_string()).collect(),
        };
        CharterRules {
            format: CHARTER_FORMAT.to_string(),
            fields: vec![FABRIC_FIELD.to_string()],
            kinds: vec![
                KindRule {
                    name: RegistryKind::Cartridges.wire_name().to_string(),
                    locations: vec![location("manifest", &["registry_version"])],
                    urls: vec![FABRIC_FIELD.to_string()],
                },
                KindRule {
                    name: RegistryKind::Fabric.wire_name().to_string(),
                    locations: vec![
                        location("manifest", &["manifest_version"]),
                        location("latest", &[]),
                        location("definition", &["kind", "urn_hash", "version"]),
                    ],
                    urls: vec![],
                },
                KindRule {
                    name: RegistryKind::Releases.wire_name().to_string(),
                    locations: vec![location("manifest", &["app_id"])],
                    urls: vec![],
                },
            ],
            reserved_slugs: vec![DEV_SLUG.to_string()],
        }
    })
}

/// The content trust this build was baked with: its roots
/// (`MFR_CARTRIDGE_ROOT_PUBKEYS`) under its label (`MFR_SIGNING_ENVIRONMENT`).
/// `None` for a dev build, which verifies no registry's contents. `build.rs`
/// guarantees a build that bakes a registry bakes both, and at least three
/// distinct keys.
pub fn content_trust() -> Option<Trust> {
    let (roots, label) = (crate::CARTRIDGE_ROOT_PUBKEYS?, crate::SIGNING_ENVIRONMENT?);
    Some(
        Trust::new(&signed_release::split_roots(roots), label, content_formats())
            .expect("build.rs admits only valid, distinct root keys"),
    )
}

/// The charter trust every build bakes (`MFR_CHARTER_ROOT_PUBKEYS`), under
/// [`CHARTER_LABEL`].
pub fn charter_trust() -> CharterTrust {
    charter_trust_from(&signed_release::split_roots(crate::CHARTER_ROOT_PUBKEYS))
        .expect("build.rs admits only valid, distinct charter root keys")
}

/// A charter trust over `roots`, as a test or a tool that is not this build
/// gives them.
pub fn charter_trust_from(roots: &[&str]) -> Result<CharterTrust, signed_release::SignatureError> {
    let formats =
        Formats { certificate: CERTIFICATE_FORMAT.to_string(), envelope: CHARTER_ENVELOPE_FORMAT.to_string() };
    Ok(CharterTrust { trust: Trust::new(roots, CHARTER_LABEL, formats)? })
}

#[cfg(test)]
mod tests {
    use super::*;
    use signed_release::{verify_manifest, ChainError};

    // Committed real-crypto fixtures: a 2-of-3 certificate authorizing a
    // release key that signed the manifest, in MachineFabric's formats.
    const ROOT_A: &str = include_str!("../../tests/fixtures/nocommit/signing/root_a.pubkey");
    const ROOT_B: &str = include_str!("../../tests/fixtures/nocommit/signing/root_b.pubkey");
    const ROOT_C: &str = include_str!("../../tests/fixtures/nocommit/signing/root_c.pubkey");
    const WRONG: &str = include_str!("../../tests/fixtures/nocommit/signing/wrong.pubkey");
    const MANIFEST: &[u8] = include_bytes!("../../tests/fixtures/nocommit/signing/manifest.json");
    const ENVELOPE: &[u8] = include_bytes!("../../tests/fixtures/nocommit/signing/manifest.json.sig");
    const META: &str = include_str!("../../tests/fixtures/nocommit/signing/meta.json");
    const RELEASE_PUBKEY: &str = include_str!("../../tests/fixtures/nocommit/signing/release.pubkey");
    const ARTIFACT: &[u8] = include_bytes!("../../tests/fixtures/nocommit/signing/artifact.bin");
    const ARTIFACT_SIG: &str = include_str!("../../tests/fixtures/nocommit/signing/artifact.bin.minisig");

    #[derive(serde::Deserialize)]
    struct Meta {
        issued_at: u64,
        not_after: u64,
        environment: String,
    }

    fn meta() -> Meta {
        serde_json::from_str(META).expect("meta.json parses")
    }

    fn mid_validity() -> u64 {
        let m = meta();
        (m.issued_at + m.not_after) / 2
    }

    fn trust(roots: &[&str], label: &str) -> Trust {
        Trust::new(roots, label, content_formats()).expect("the fixture roots are keys")
    }

    // TEST8049: the full 2-of-3 chain in MachineFabric's formats verifies —
    // roots A+B authorize the release key, which signed the manifest.
    #[test]
    fn test8049_full_2of3_chain_verifies() {
        let chain = verify_manifest(ENVELOPE, MANIFEST, &trust(&[ROOT_A, ROOT_B, ROOT_C], &meta().environment), mid_validity())
            .expect("the committed 2-of-3 chain verifies");
        assert_eq!(chain.release_keys.len(), 1);
    }

    // TEST8050: tampered manifest bytes fail — the signature binds exact bytes.
    #[test]
    fn test8050_tampered_manifest_rejected() {
        let mut tampered = MANIFEST.to_vec();
        tampered[10] ^= 0x01;
        assert!(matches!(
            verify_manifest(ENVELOPE, &tampered, &trust(&[ROOT_A, ROOT_B, ROOT_C], &meta().environment), mid_validity()),
            Err(ChainError::SignatureInvalid(_))
        ));
    }

    // TEST8051: fewer than 2 distinct trusted roots is insufficient. The
    // certificate was signed by A and B, so a client baking [C, wrong] sees no
    // trusted signer.
    #[test]
    fn test8051_insufficient_root_signatures_rejected() {
        assert!(matches!(
            verify_manifest(ENVELOPE, MANIFEST, &trust(&[ROOT_C, WRONG], &meta().environment), mid_validity()),
            Err(ChainError::InsufficientRootSignatures { have: 0, need: 2, .. })
        ));
    }

    // TEST8052: expiry and not-yet-valid are wall-clock law.
    #[test]
    fn test8052_expiry_and_not_yet_valid_enforced() {
        let m = meta();
        let trust = trust(&[ROOT_A, ROOT_B, ROOT_C], &m.environment);
        assert!(matches!(
            verify_manifest(ENVELOPE, MANIFEST, &trust, m.not_after + 1),
            Err(ChainError::ExpiredCertificate { .. })
        ));
        assert!(matches!(
            verify_manifest(ENVELOPE, MANIFEST, &trust, m.issued_at - 1),
            Err(ChainError::NotYetValidCertificate { .. })
        ));
    }

    // TEST8053: environment binding — the prod certificate is rejected by a
    // staging-baked client.
    #[test]
    fn test8053_environment_binding_enforced() {
        assert!(matches!(
            verify_manifest(ENVELOPE, MANIFEST, &trust(&[ROOT_A, ROOT_B, ROOT_C], "staging"), mid_validity()),
            Err(ChainError::LabelMismatch { .. })
        ));
    }

    // TEST8038: a non-empty baked root set and label pass through unchanged.
    #[test]
    fn test8038_root_pubkeys_from_build_env_passes_through_nonempty() {
        let keys = "RWRootAKeyBase64,RWRootBKeyBase64,RWRootCKeyBase64";
        assert_eq!(root_pubkeys_from_build_env(Some(keys)), Some(keys));
        assert_eq!(signing_environment_from_build_env(Some("staging")), Some("staging"));
    }

    // TEST8045: absent ⇒ dev build ⇒ None for both the root set and the label.
    #[test]
    fn test8045_root_pubkeys_from_build_env_none_for_dev() {
        assert_eq!(root_pubkeys_from_build_env(None), None);
        assert_eq!(signing_environment_from_build_env(None), None);
    }

    // TEST8046: an exported-but-empty root set is a hard failure.
    #[test]
    #[should_panic(expected = "MFR_CARTRIDGE_ROOT_PUBKEYS")]
    fn test8046_root_pubkeys_from_build_env_panics_on_empty() {
        let _ = root_pubkeys_from_build_env(Some(""));
    }

    // TEST8029: a real release-key minisign signature verifies over the exact
    // artifact bytes with the release public key.
    #[test]
    fn test8029_valid_binary_signature_verifies() {
        signed_release::verify_minisign(RELEASE_PUBKEY, ARTIFACT_SIG, ARTIFACT)
            .expect("the committed signature verifies over the committed artifact");
    }

    // TEST8030: flipping a single artifact byte breaks verification.
    #[test]
    fn test8030_tampered_artifact_rejected() {
        let mut tampered = ARTIFACT.to_vec();
        tampered[0] ^= 0x01;
        assert!(matches!(
            signed_release::verify_minisign(RELEASE_PUBKEY, ARTIFACT_SIG, &tampered),
            Err(signed_release::SignatureError::VerificationFailed(_))
        ));
    }

    // TEST8031: the same signature under a different public key fails over the
    // identical artifact — verification binds the key.
    #[test]
    fn test8031_wrong_key_rejected() {
        assert!(matches!(
            signed_release::verify_minisign(WRONG, ARTIFACT_SIG, ARTIFACT),
            Err(signed_release::SignatureError::VerificationFailed(_))
        ));
    }
}
