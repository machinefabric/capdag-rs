//! Release-key certificate and signed-manifest envelope verification.
//!
//! capdag verifies registry manifests and cartridge binaries against the trusted
//! ROOT public keys baked into this build. The trust protocol:
//!
//! 1. **Root keys** — minisign keypairs whose PUBLIC keys are baked into the
//!    binary ([`crate::CARTRIDGE_ROOT_PUBKEYS`]). A root signs release-key
//!    certificates. Authorization is **2-of-3**: a certificate is trusted only
//!    when at least [`REQUIRED_ROOT_SIGNATURES`] DISTINCT baked roots have each
//!    signed its exact bytes.
//! 2. **Release keys** — each authorized by a 2-of-3 certificate carrying an
//!    expiry and an environment label. The release key signs every published
//!    cartridge binary (minisign — see [`super::binary_signing`]) and every
//!    manifest (raw ed25519 over the exact bytes served at the manifest URL).
//!
//! **Certificate** — a JSON document. The signed bytes are the EXACT certificate
//! JSON string carried verbatim inside envelopes (no re-serialization, no
//! canonicalization, no cross-language ordering hazards).
//!
//! **Manifest signature envelope** — a sidecar at `<manifest-url>.sig` carrying
//! the certificate(s) and the manifest signature, so a verifier needs exactly
//! one extra fetch. `certificates` is a list; every chain-valid certificate in
//! it is trusted, so more than one release key can be accepted at once.
//!
//! Verifier policy (all hard failures, never warnings):
//! - a certificate must be signed by ≥2 DISTINCT baked roots (2-of-3),
//! - must not be expired (wall clock vs `not_after`) or not-yet-issued,
//! - must carry this build's baked environment label,
//! - the manifest signature must verify, under a chain-valid certificate's
//!   release key, over the exact manifest bytes fetched.

use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::binary_signing::{parse_minisign_public_key, raw_verify, SignatureError};
use super::registry_verdict::ChainFailureReason;

/// Format discriminator for release-key certificates.
pub const RELEASE_KEY_CERT_FORMAT: &str = "machinefabric-release-key-cert/1";
/// Format discriminator for manifest signature envelopes.
pub const MANIFEST_SIG_FORMAT: &str = "machinefabric-manifest-sig/1";

/// The 2-of-3 threshold: a release-key certificate is trusted only when at
/// least this many DISTINCT baked roots have signed it.
pub const REQUIRED_ROOT_SIGNATURES: usize = 2;

/// The parsed body of a release-key certificate. The wire form is the exact
/// JSON string preserved in [`CertificateEntry::certificate`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseKeyCertificate {
    pub format: String,
    /// Base64 minisign public key of the release key this certificate
    /// authorizes.
    pub release_pubkey: String,
    /// The release key's minisign keynum, lowercase hex.
    pub key_id: String,
    /// Environment the certificate is bound to (`prod` / `staging`).
    pub environment: String,
    /// Unix seconds at issuance.
    pub issued_at: u64,
    /// Unix seconds after which the certificate is invalid.
    pub not_after: u64,
}

/// One root's signature over a certificate body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootSignature {
    /// The signing root's minisign keynum (hex) — operator legibility only;
    /// verification tries every baked root regardless.
    pub root_key_id: String,
    /// Base64 raw ed25519 signature by this root over the certificate's exact
    /// UTF-8 bytes.
    pub signature: String,
}

/// One certificate inside a manifest envelope: the exact signed JSON string
/// plus the root signatures that authorize it (≥2 distinct baked roots must
/// verify — 2-of-3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificateEntry {
    /// The certificate as its EXACT JSON string — the signed bytes.
    pub certificate: String,
    /// The distinct root signatures over `certificate`.
    pub root_signatures: Vec<RootSignature>,
}

/// The manifest signature inside an envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSignature {
    /// key_id of the release key that signed (must match a chain-valid
    /// certificate in the same envelope).
    pub key_id: String,
    /// Base64 raw ed25519 signature over the exact manifest bytes.
    pub signature: String,
}

/// The `<manifest-url>.sig` sidecar document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSigEnvelope {
    pub format: String,
    pub certificates: Vec<CertificateEntry>,
    pub manifest_signature: ManifestSignature,
}

/// Why a certificate / manifest chain failed verification. Every variant is a
/// hard failure; none are advisory.
#[derive(Debug, thiserror::Error)]
pub enum ChainError {
    #[error("malformed manifest signature envelope: {0}")]
    MalformedEnvelope(String),
    #[error("manifest signature envelope has unsupported format '{0}' (expected '{MANIFEST_SIG_FORMAT}')")]
    UnsupportedEnvelopeFormat(String),
    #[error("malformed release-key certificate: {0}")]
    MalformedCertificate(String),
    #[error("release-key certificate has unsupported format '{0}' (expected '{RELEASE_KEY_CERT_FORMAT}')")]
    UnsupportedCertificateFormat(String),
    #[error("release-key certificate (key_id {key_id}) is signed by only {have} trusted root(s); {need} required (2-of-3)")]
    InsufficientRootSignatures {
        key_id: String,
        have: usize,
        need: usize,
    },
    #[error("release-key certificate (key_id {key_id}) expired at unix {not_after} (now {now})")]
    ExpiredCertificate {
        key_id: String,
        not_after: u64,
        now: u64,
    },
    #[error("release-key certificate (key_id {key_id}) is not yet valid: issued_at {issued_at} is in the future (now {now})")]
    NotYetValidCertificate {
        key_id: String,
        issued_at: u64,
        now: u64,
    },
    #[error("release-key certificate (key_id {key_id}) is bound to environment '{cert_env}' but this build is '{build_env}'")]
    EnvironmentMismatch {
        key_id: String,
        cert_env: String,
        build_env: String,
    },
    #[error("certificate key_id '{stated}' does not match its release_pubkey's keynum '{actual}'")]
    KeyIdMismatch { stated: String, actual: String },
    #[error("no chain-valid certificate in the envelope authorizes the manifest signature (key_id {key_id})")]
    NoAuthorizingCertificate { key_id: String },
    #[error("manifest signature does not verify over the fetched manifest bytes: {0}")]
    ManifestSignatureInvalid(#[source] SignatureError),
    #[error("envelope carries no certificates")]
    EmptyCertificateList,
    #[error(transparent)]
    Signature(#[from] SignatureError),
}

impl ChainError {
    /// Which chain check failed, as the closed vocabulary every implementation
    /// classifies through.
    ///
    /// A verifier's own error type is free to say as much as it likes in its
    /// message; the REASON is what decides whether the registry is untrusted
    /// (we judged it and said no) or unverifiable (we could not judge it at
    /// all). Leaving that decision to each consumer is how "unsupported
    /// envelope format" came to be reported as a network outage in one client
    /// and nothing at all in another.
    pub fn reason(&self) -> ChainFailureReason {
        match self {
            Self::MalformedEnvelope(_) => ChainFailureReason::MalformedEnvelope,
            Self::UnsupportedEnvelopeFormat(_) => ChainFailureReason::UnsupportedEnvelopeFormat,
            Self::MalformedCertificate(_) => ChainFailureReason::MalformedCertificate,
            Self::UnsupportedCertificateFormat(_) => {
                ChainFailureReason::UnsupportedCertificateFormat
            }
            Self::EmptyCertificateList => ChainFailureReason::EmptyCertificateList,
            Self::InsufficientRootSignatures { .. } => {
                ChainFailureReason::InsufficientRootSignatures
            }
            Self::ExpiredCertificate { .. } => ChainFailureReason::ExpiredCertificate,
            Self::NotYetValidCertificate { .. } => ChainFailureReason::NotYetValidCertificate,
            Self::EnvironmentMismatch { .. } => ChainFailureReason::EnvironmentMismatch,
            Self::KeyIdMismatch { .. } => ChainFailureReason::KeyIdMismatch,
            Self::NoAuthorizingCertificate { .. } => ChainFailureReason::NoAuthorizingCertificate,
            // A raw signature failure reaches here only from verifying the
            // manifest signature itself; the certificate paths wrap theirs in
            // the variants above.
            Self::ManifestSignatureInvalid(_) | Self::Signature(_) => {
                ChainFailureReason::ManifestSignatureInvalid
            }
        }
    }
}

/// The outcome of a successful chain verification: the set of release keys (as
/// base64 minisign public keys) that chain-valid certificates authorize. Binary
/// signatures must verify under one of these.
#[derive(Debug, Clone)]
pub struct VerifiedChain {
    /// `(key_id, release_pubkey_b64)` of every chain-valid certificate.
    pub trusted_release_keys: Vec<(String, String)>,
}

impl VerifiedChain {
    /// The release pubkey for a key id, if a chain-valid certificate authorized
    /// it.
    pub fn release_pubkey(&self, key_id: &str) -> Option<&str> {
        self.trusted_release_keys
            .iter()
            .find(|(id, _)| id == key_id)
            .map(|(_, key)| key.as_str())
    }
}

/// The trust anchors a registry consumer verifies against: the baked ROOT
/// public keys and the environment label certificates must be bound to.
/// Constructed from the build-time constants for product binaries
/// ([`RegistryTrust::from_build_constants`]) or injected explicitly in tests.
#[derive(Debug, Clone)]
pub struct RegistryTrust {
    /// Base64 minisign root public keys (Root A, Root B, Root C). 2-of-3.
    pub root_pubkeys: Vec<String>,
    /// `prod` / `staging` — certificates for the other environment are rejected
    /// even under valid root signatures.
    pub environment: String,
}

impl RegistryTrust {
    /// The trust baked into this build (`MFR_CARTRIDGE_ROOT_PUBKEYS` +
    /// `MFR_SIGNING_ENVIRONMENT`). `None` = dev build — registry manifest
    /// verification and binary downloads are disabled. capdag's build.rs
    /// guarantees a build with a baked registry URL also has both constants.
    pub fn from_build_constants() -> Option<Self> {
        match (crate::CARTRIDGE_ROOT_PUBKEYS, crate::SIGNING_ENVIRONMENT) {
            (Some(roots), Some(environment)) => Some(Self {
                root_pubkeys: super::binary_signing::split_root_pubkeys(roots)
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
                environment: environment.to_string(),
            }),
            _ => None,
        }
    }
}

/// Current wall-clock time as unix seconds.
pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the unix epoch")
        .as_secs()
}

/// Count how many DISTINCT baked roots have at least one valid signature over
/// `cert_bytes`. Distinctness is by baked-root position, so a signature from a
/// non-trusted key contributes nothing and two signatures from the same root
/// count once.
fn distinct_trusted_root_signers(
    signatures: &[RootSignature],
    root_pubkeys: &[&str],
    cert_bytes: &[u8],
) -> BTreeSet<usize> {
    let mut signers = BTreeSet::new();
    for (idx, root) in root_pubkeys.iter().enumerate() {
        let signed = signatures
            .iter()
            .any(|rs| raw_verify(root, &rs.signature, cert_bytes).is_ok());
        if signed {
            signers.insert(idx);
        }
    }
    signers
}

/// Validate one certificate entry against the baked roots / environment / clock
/// and return its parsed body. Every check is a hard error.
fn verify_certificate_entry(
    entry: &CertificateEntry,
    root_pubkeys: &[&str],
    build_environment: &str,
    now_unix: u64,
) -> Result<ReleaseKeyCertificate, ChainError> {
    let cert: ReleaseKeyCertificate = serde_json::from_str(&entry.certificate)
        .map_err(|e| ChainError::MalformedCertificate(e.to_string()))?;
    if cert.format != RELEASE_KEY_CERT_FORMAT {
        return Err(ChainError::UnsupportedCertificateFormat(cert.format));
    }
    // The stated key_id must be the release pubkey's actual keynum — a mismatch
    // means a hand-edited certificate.
    let parsed_release = parse_minisign_public_key(&cert.release_pubkey)?;
    if parsed_release.key_id != cert.key_id {
        return Err(ChainError::KeyIdMismatch {
            stated: cert.key_id,
            actual: parsed_release.key_id,
        });
    }
    // 2-of-3: at least REQUIRED_ROOT_SIGNATURES distinct baked roots signed.
    let signers = distinct_trusted_root_signers(
        &entry.root_signatures,
        root_pubkeys,
        entry.certificate.as_bytes(),
    );
    if signers.len() < REQUIRED_ROOT_SIGNATURES {
        return Err(ChainError::InsufficientRootSignatures {
            key_id: cert.key_id,
            have: signers.len(),
            need: REQUIRED_ROOT_SIGNATURES,
        });
    }
    if cert.environment != build_environment {
        return Err(ChainError::EnvironmentMismatch {
            key_id: cert.key_id,
            cert_env: cert.environment,
            build_env: build_environment.to_string(),
        });
    }
    if now_unix > cert.not_after {
        return Err(ChainError::ExpiredCertificate {
            key_id: cert.key_id,
            not_after: cert.not_after,
            now: now_unix,
        });
    }
    if cert.issued_at > now_unix {
        return Err(ChainError::NotYetValidCertificate {
            key_id: cert.key_id,
            issued_at: cert.issued_at,
            now: now_unix,
        });
    }
    Ok(cert)
}

/// Verify a manifest signature envelope over the exact fetched manifest bytes.
/// Returns the set of trusted release keys for subsequent artifact (binary)
/// signature verification.
///
/// EVERY certificate in the envelope must be chain-valid — an envelope carrying
/// even one bad certificate is a publish error or tampering, and a verifier
/// that skips over bad entries would mask it.
pub fn verify_manifest_envelope(
    envelope_json: &str,
    manifest_bytes: &[u8],
    root_pubkeys: &[&str],
    build_environment: &str,
    now_unix: u64,
) -> Result<VerifiedChain, ChainError> {
    let envelope: ManifestSigEnvelope = serde_json::from_str(envelope_json)
        .map_err(|e| ChainError::MalformedEnvelope(e.to_string()))?;
    if envelope.format != MANIFEST_SIG_FORMAT {
        return Err(ChainError::UnsupportedEnvelopeFormat(envelope.format));
    }
    if envelope.certificates.is_empty() {
        return Err(ChainError::EmptyCertificateList);
    }

    let mut trusted_release_keys: Vec<(String, String)> = Vec::new();
    for entry in &envelope.certificates {
        let cert = verify_certificate_entry(entry, root_pubkeys, build_environment, now_unix)?;
        trusted_release_keys.push((cert.key_id, cert.release_pubkey));
    }

    let signer_id = &envelope.manifest_signature.key_id;
    let Some((_, release_pubkey)) = trusted_release_keys.iter().find(|(id, _)| id == signer_id)
    else {
        return Err(ChainError::NoAuthorizingCertificate {
            key_id: signer_id.clone(),
        });
    };
    raw_verify(
        release_pubkey,
        &envelope.manifest_signature.signature,
        manifest_bytes,
    )
    .map_err(ChainError::ManifestSignatureInvalid)?;

    Ok(VerifiedChain {
        trusted_release_keys,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Committed real-crypto fixtures: a 2-of-3 certificate authorizing a
    // release key that signed the manifest. capdag verifies them and tampers
    // in-memory to prove rejection.
    const ROOT_A: &str = include_str!("../../tests/fixtures/nocommit/signing/root_a.pubkey");
    const ROOT_B: &str = include_str!("../../tests/fixtures/nocommit/signing/root_b.pubkey");
    const ROOT_C: &str = include_str!("../../tests/fixtures/nocommit/signing/root_c.pubkey");
    const WRONG: &str = include_str!("../../tests/fixtures/nocommit/signing/wrong.pubkey");
    const MANIFEST: &[u8] = include_bytes!("../../tests/fixtures/nocommit/signing/manifest.json");
    const ENVELOPE: &str = include_str!("../../tests/fixtures/nocommit/signing/manifest.json.sig");
    const META: &str = include_str!("../../tests/fixtures/nocommit/signing/meta.json");

    #[derive(serde::Deserialize)]
    struct Meta {
        issued_at: u64,
        not_after: u64,
        environment: String,
    }

    fn meta() -> Meta {
        serde_json::from_str(META).expect("meta.json must parse")
    }

    fn mid_validity() -> u64 {
        let m = meta();
        (m.issued_at + m.not_after) / 2
    }

    // TEST8049: the full 2-of-3 chain verifies — roots A+B authorize the
    // release key, which signed the manifest; a verifier baking [A,B,C] accepts
    // the envelope and returns the release key as trusted.
    #[test]
    fn test8049_full_2of3_chain_verifies() {
        let roots = [ROOT_A, ROOT_B, ROOT_C];
        let chain = verify_manifest_envelope(
            ENVELOPE,
            MANIFEST,
            &roots,
            &meta().environment,
            mid_validity(),
        )
        .expect("the committed 2-of-3 chain must verify");
        assert_eq!(chain.trusted_release_keys.len(), 1);
    }

    // TEST8050: tampered manifest bytes fail — the signature binds exact bytes.
    #[test]
    fn test8050_tampered_manifest_rejected() {
        let roots = [ROOT_A, ROOT_B, ROOT_C];
        let mut tampered = MANIFEST.to_vec();
        tampered[10] ^= 0x01;
        assert!(matches!(
            verify_manifest_envelope(
                ENVELOPE,
                &tampered,
                &roots,
                &meta().environment,
                mid_validity()
            ),
            Err(ChainError::ManifestSignatureInvalid(_))
        ));
    }

    // TEST8051: fewer than 2 distinct trusted roots is insufficient (2-of-3).
    // The cert was signed by A and B, so a verifier baking only [C, wrong] sees
    // zero trusted signers, and baking only [A] sees one.
    #[test]
    fn test8051_insufficient_root_signatures_rejected() {
        let env = meta().environment;
        let now = mid_validity();
        assert!(matches!(
            verify_manifest_envelope(ENVELOPE, MANIFEST, &[ROOT_C, WRONG], &env, now),
            Err(ChainError::InsufficientRootSignatures {
                have: 0,
                need: 2,
                ..
            })
        ));
        assert!(matches!(
            verify_manifest_envelope(ENVELOPE, MANIFEST, &[ROOT_A], &env, now),
            Err(ChainError::InsufficientRootSignatures {
                have: 1,
                need: 2,
                ..
            })
        ));
    }

    // TEST8052: expiry and not-yet-valid are wall-clock law.
    #[test]
    fn test8052_expiry_and_not_yet_valid_enforced() {
        let roots = [ROOT_A, ROOT_B, ROOT_C];
        let m = meta();
        assert!(matches!(
            verify_manifest_envelope(ENVELOPE, MANIFEST, &roots, &m.environment, m.not_after + 1),
            Err(ChainError::ExpiredCertificate { .. })
        ));
        assert!(matches!(
            verify_manifest_envelope(ENVELOPE, MANIFEST, &roots, &m.environment, m.issued_at - 1),
            Err(ChainError::NotYetValidCertificate { .. })
        ));
    }

    // TEST8053: environment binding — the prod certificate is rejected by a
    // staging-baked verifier.
    #[test]
    fn test8053_environment_binding_enforced() {
        let roots = [ROOT_A, ROOT_B, ROOT_C];
        assert!(matches!(
            verify_manifest_envelope(ENVELOPE, MANIFEST, &roots, "staging", mid_validity()),
            Err(ChainError::EnvironmentMismatch { .. })
        ));
    }
}
