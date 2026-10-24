//! Registry charters: what makes a URL a MachineFabric registry.
//!
//! A registry — of cartridges, of fabric definitions, or of app releases — is
//! named by its **anchor**, which may be any URL: a domain, a subdomain, a
//! sub-path. The one convention anywhere is that `<anchor>/charter.json` and
//! `<anchor>/charter.json.sig` exist. Everything else a client needs is read
//! from that charter, never derived from the shape of a URL:
//!
//! * the registry's **slug** — its name on disk (install folder, cache folder);
//! * its **kind**, and the **environment** its contents are signed under;
//! * the **locations** of its manifest(s), as templates;
//! * for a cartridge registry, the **fabric** its caps resolve against.
//!
//! A charter is signed by MachineFabric's **prod** release key, whatever the
//! registry's environment: a registry exists because MachineFabric chartered
//! it. The signature uses the same chain as everything else — 2-of-3 roots →
//! release-key certificate → ed25519 over the exact bytes — but it is verified
//! against the **charter trust** (`MFR_CHARTER_ROOT_PUBKEYS`, baked into every
//! build), never against a build's content trust. The two are kept apart so a
//! charter can never vouch for an artifact, and a content key can never
//! charter a registry.
//!
//! The CHARTER is not the MANIFEST. A manifest lists a registry's contents and
//! is signed by the key of the registry's own environment; a charter declares
//! the registry and is signed once, by prod, and again whenever the prod
//! release key rotates.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::binary_signing::raw_verify;
use super::registry_verdict::ChainFailureReason;
use super::release_cert::{verify_certificate_entry, CertificateEntry, ChainError};

/// Format discriminator inside every charter body.
pub const CHARTER_FORMAT: &str = "machinefabric-registry-charter/1";
/// Format discriminator of a charter's signature envelope. Distinct from the
/// manifest envelope's, so a prod-signed manifest can never be presented as a
/// charter (and the body carries its own format besides).
pub const CHARTER_SIG_FORMAT: &str = "machinefabric-charter-sig/1";
/// The charter's file name under its anchor, and in a registry's folder.
pub const CHARTER_FILE: &str = "charter.json";
/// The charter signature's file name under its anchor, and in a registry's
/// folder.
pub const CHARTER_SIG_FILE: &str = "charter.json.sig";
/// The environment label a charter's certificate carries. Charters are signed
/// by prod for every environment — that is what makes a registry exist.
pub const CHARTER_SIGNING_ENVIRONMENT: &str = "prod";
/// The folder name of cartridges built with no registry. It is reserved: no
/// charter may claim it.
pub const DEV_SLUG: &str = "dev";

/// The three kinds of registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

    /// The locations a charter of this kind must declare, each with the
    /// placeholders its template must use — all of them, and no others. These
    /// are the only addresses a client composes; the charter supplies the
    /// rest of each one.
    pub fn locations(self) -> &'static [(&'static str, &'static [&'static str])] {
        match self {
            Self::Cartridges => &[("manifest", &["registry_version"])],
            Self::Fabric => &[
                ("manifest", &["manifest_version"]),
                ("latest", &[]),
                ("definition", &["kind", "urn_hash", "version"]),
            ],
            Self::Releases => &[("manifest", &["app_id"])],
        }
    }
}

/// A charter body, as signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Charter {
    pub format: String,
    /// The URL this registry is named by — byte-equal to the anchor a client
    /// was asked to use, and the `registry_url` every cartridge from it records.
    pub anchor: String,
    pub kind: RegistryKind,
    /// The registry's name on disk. Unique across every charter MachineFabric
    /// has issued; never `dev`.
    pub slug: String,
    /// The label this registry's CONTENTS are signed under (`prod`, `staging`,
    /// `lab`). Not the charter's own signing label, which is always prod.
    pub environment: String,
    /// The lab experiment this registry belongs to, when it belongs to one.
    #[serde(default)]
    pub experiment: Option<String>,
    /// Location templates, by name (see [`RegistryKind::locations`]).
    pub locations: BTreeMap<String, String>,
    /// For a cartridge registry: the anchor of the fabric registry its caps
    /// resolve against. Absent for every other kind.
    #[serde(default)]
    pub fabric: Option<String>,
    /// Unix seconds at issuance.
    pub issued_at: u64,
    /// Unix seconds after which the charter is void. `None`: valid until the
    /// key that signed it is no longer trusted.
    pub not_after: Option<u64>,
}

/// The `charter.json.sig` document.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CharterSigEnvelope {
    pub format: String,
    pub certificates: Vec<CertificateEntry>,
    pub charter_signature: CharterSignature,
}

/// The signature over a charter's exact bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CharterSignature {
    /// The key id of the release key that signed; a chain-valid certificate in
    /// the same envelope must authorize it.
    pub key_id: String,
    /// Base64 raw ed25519 signature over the exact charter bytes.
    pub signature: String,
}

/// The roots charters are verified against: MachineFabric's, baked into every
/// build whatever its environment.
#[derive(Debug, Clone)]
pub struct CharterTrust {
    pub root_pubkeys: Vec<String>,
}

impl CharterTrust {
    /// The charter roots this build bakes. `build.rs` refuses a build without
    /// at least three, so there is no build in which this is absent.
    pub fn from_build_constants() -> Self {
        Self {
            root_pubkeys: super::binary_signing::split_root_pubkeys(crate::CHARTER_ROOT_PUBKEYS)
                .into_iter()
                .map(str::to_string)
                .collect(),
        }
    }
}

/// What the client expects of the charter it is about to trust.
#[derive(Debug, Clone, Copy)]
pub struct CharterExpectation<'a> {
    /// The anchor the client was asked to use. `None` only for a charter read
    /// from a registry's folder, whose anchor is then checked against every
    /// cartridge in it.
    pub anchor: Option<&'a str>,
    pub kind: RegistryKind,
    /// The build's content label. A registry whose contents are signed under
    /// another environment is refused here, at its charter, rather than on its
    /// first signature. `None` for a build with no content trust (a dev build),
    /// and for a fabric, whose contents are not signed.
    pub environment: Option<&'a str>,
}

/// A charter that verified: the parsed body and the exact bytes, kept so it can
/// be stored beside what it vouches for and verified again offline.
#[derive(Debug, Clone)]
pub struct VerifiedCharter {
    pub charter: Charter,
    pub body: Vec<u8>,
    pub envelope: String,
}

/// Why a charter was refused. Every variant is a hard failure.
#[derive(Debug, thiserror::Error)]
pub enum CharterError {
    #[error("the charter is not a charter document: {0}")]
    Malformed(String),
    #[error("the charter declares format '{0}' (this build reads '{CHARTER_FORMAT}')")]
    UnsupportedFormat(String),
    #[error("the charter's signature envelope is malformed: {0}")]
    MalformedEnvelope(String),
    #[error("the charter's signature envelope declares format '{0}' (this build reads '{CHARTER_SIG_FORMAT}')")]
    UnsupportedEnvelopeFormat(String),
    #[error("the charter's signature envelope carries no certificates")]
    EmptyCertificateList,
    #[error("the charter's certificate does not chain to MachineFabric's roots: {0}")]
    Chain(#[source] ChainError),
    #[error("no chain-valid certificate authorizes the key that signed the charter (key_id {0})")]
    NoAuthorizingCertificate(String),
    #[error("the charter's signature does not verify over its bytes: {0}")]
    SignatureInvalid(String),
    #[error("the charter is for '{charter}', not for '{asked}'")]
    AnchorMismatch { charter: String, asked: String },
    #[error("the charter is for a {charter} registry, not a {expected} one")]
    KindMismatch { charter: &'static str, expected: &'static str },
    #[error("the charter's registry is signed under '{charter}', and this build verifies '{build}'")]
    EnvironmentMismatch { charter: String, build: String },
    #[error("the charter is not valid until unix {issued_at} (now {now})")]
    NotYetValid { issued_at: u64, now: u64 },
    #[error("the charter expired at unix {not_after} (now {now})")]
    Expired { not_after: u64, now: u64 },
    #[error("the charter for '{anchor}' names slug '{slug}', but it is stored in a folder named '{folder}'")]
    FolderMismatch { anchor: String, slug: String, folder: String },
}

impl CharterError {
    /// The closed reason a verdict reports. Certificate failures keep their
    /// own reason; everything about the charter itself has a `charter_` one.
    pub fn reason(&self) -> ChainFailureReason {
        match self {
            Self::Malformed(_) => ChainFailureReason::CharterMalformed,
            Self::UnsupportedFormat(_) => ChainFailureReason::CharterUnsupportedFormat,
            Self::MalformedEnvelope(_) => ChainFailureReason::MalformedEnvelope,
            Self::UnsupportedEnvelopeFormat(_) => ChainFailureReason::UnsupportedEnvelopeFormat,
            Self::EmptyCertificateList => ChainFailureReason::EmptyCertificateList,
            Self::Chain(e) => e.reason(),
            Self::NoAuthorizingCertificate(_) => ChainFailureReason::NoAuthorizingCertificate,
            Self::SignatureInvalid(_) => ChainFailureReason::CharterSignatureInvalid,
            Self::AnchorMismatch { .. } => ChainFailureReason::CharterAnchorMismatch,
            Self::KindMismatch { .. } => ChainFailureReason::CharterKindMismatch,
            Self::EnvironmentMismatch { .. } => ChainFailureReason::CharterEnvironmentMismatch,
            Self::NotYetValid { .. } => ChainFailureReason::CharterNotYetValid,
            Self::Expired { .. } => ChainFailureReason::CharterExpired,
            Self::FolderMismatch { .. } => ChainFailureReason::CharterFolderMismatch,
        }
    }
}

/// Verify a charter's exact bytes and its envelope, then hold the body to what
/// the client expects. In this order, each a hard failure:
///
/// 1. the envelope's format, and every certificate in it chain-valid against
///    the charter roots with the `prod` label;
/// 2. the signature, by a key one of those certificates authorizes, over the
///    exact bytes;
/// 3. the body: its format, then its shape ([`Charter::validate`]);
/// 4. the expectation: anchor, kind, content environment;
/// 5. time: issued, and not expired.
pub fn verify(
    body: &[u8],
    envelope_json: &str,
    trust: &CharterTrust,
    expect: &CharterExpectation<'_>,
    now_unix: u64,
) -> Result<VerifiedCharter, CharterError> {
    // The format first, then the shape: a signature document of another kind
    // (a manifest's, say) is refused for what it is, not as a broken charter
    // signature.
    let value: serde_json::Value = serde_json::from_str(envelope_json)
        .map_err(|e| CharterError::MalformedEnvelope(e.to_string()))?;
    match value.get("format").and_then(serde_json::Value::as_str) {
        Some(CHARTER_SIG_FORMAT) => {}
        Some(other) => return Err(CharterError::UnsupportedEnvelopeFormat(other.to_string())),
        None => return Err(CharterError::MalformedEnvelope("no format".to_string())),
    }
    let envelope: CharterSigEnvelope = serde_json::from_value(value)
        .map_err(|e| CharterError::MalformedEnvelope(e.to_string()))?;
    if envelope.certificates.is_empty() {
        return Err(CharterError::EmptyCertificateList);
    }
    let roots: Vec<&str> = trust.root_pubkeys.iter().map(String::as_str).collect();
    let mut authorized: Vec<(String, String)> = Vec::new();
    for entry in &envelope.certificates {
        let cert = verify_certificate_entry(entry, &roots, CHARTER_SIGNING_ENVIRONMENT, now_unix)
            .map_err(CharterError::Chain)?;
        authorized.push((cert.key_id, cert.release_pubkey));
    }
    let signer = &envelope.charter_signature.key_id;
    let Some((_, release_pubkey)) = authorized.iter().find(|(id, _)| id == signer) else {
        return Err(CharterError::NoAuthorizingCertificate(signer.clone()));
    };
    raw_verify(release_pubkey, &envelope.charter_signature.signature, body)
        .map_err(|e| CharterError::SignatureInvalid(e.to_string()))?;

    // Only now is the body read: nothing about an unsigned document is
    // believed, including what it says its format is.
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| CharterError::Malformed(e.to_string()))?;
    match value.get("format").and_then(serde_json::Value::as_str) {
        Some(CHARTER_FORMAT) => {}
        Some(other) => return Err(CharterError::UnsupportedFormat(other.to_string())),
        None => return Err(CharterError::Malformed("no format".to_string())),
    }
    let charter: Charter =
        serde_json::from_value(value).map_err(|e| CharterError::Malformed(e.to_string()))?;
    charter.validate().map_err(CharterError::Malformed)?;

    if let Some(asked) = expect.anchor {
        if charter.anchor != asked {
            return Err(CharterError::AnchorMismatch {
                charter: charter.anchor.clone(),
                asked: asked.to_string(),
            });
        }
    }
    if charter.kind != expect.kind {
        return Err(CharterError::KindMismatch {
            charter: charter.kind.wire_name(),
            expected: expect.kind.wire_name(),
        });
    }
    if let Some(build) = expect.environment {
        if charter.environment != build {
            return Err(CharterError::EnvironmentMismatch {
                charter: charter.environment.clone(),
                build: build.to_string(),
            });
        }
    }
    if charter.issued_at > now_unix {
        return Err(CharterError::NotYetValid { issued_at: charter.issued_at, now: now_unix });
    }
    if let Some(not_after) = charter.not_after {
        if now_unix > not_after {
            return Err(CharterError::Expired { not_after, now: now_unix });
        }
    }
    Ok(VerifiedCharter { charter, body: body.to_vec(), envelope: envelope_json.to_string() })
}

impl Charter {
    /// The shape every charter must have, whoever signed it. A violation is a
    /// malformed charter: MachineFabric's tooling never issues one, so meeting
    /// one means the issuer is broken, and nothing in it is used.
    pub fn validate(&self) -> Result<(), String> {
        if self.format != CHARTER_FORMAT {
            return Err(format!("format '{}' is not '{CHARTER_FORMAT}'", self.format));
        }
        check_https("anchor", &self.anchor)?;
        if self.anchor.ends_with('/') {
            return Err(format!("anchor '{}' ends with '/'", self.anchor));
        }
        if !is_slug(&self.slug) {
            return Err(format!(
                "slug '{}' is not a registry slug ([a-z0-9][a-z0-9.-]{{0,62}}, not '{DEV_SLUG}')",
                self.slug
            ));
        }
        if !is_label(&self.environment) {
            return Err(format!("environment '{}' is not a label ([a-z0-9-]+)", self.environment));
        }
        if let Some(experiment) = &self.experiment {
            if !is_experiment_id(experiment) {
                return Err(format!(
                    "experiment '{experiment}' is not an experiment id ([a-z0-9][a-z0-9-]{{0,39}})"
                ));
            }
        }
        let required = self.kind.locations();
        for name in self.locations.keys() {
            if !required.iter().any(|(declared, _)| declared == name) {
                return Err(format!(
                    "location '{name}' is not one a {} registry has",
                    self.kind.wire_name()
                ));
            }
        }
        for (name, placeholders) in required {
            let template = self.locations.get(*name).ok_or_else(|| {
                format!("a {} registry must declare location '{name}'", self.kind.wire_name())
            })?;
            check_https(&format!("location '{name}'"), template)?;
            let used = placeholders_in(template).map_err(|e| format!("location '{name}': {e}"))?;
            for placeholder in &used {
                if !placeholders.contains(&placeholder.as_str()) {
                    return Err(format!("location '{name}' uses '{{{placeholder}}}', which it does not take"));
                }
            }
            for placeholder in *placeholders {
                if !used.iter().any(|u| u == placeholder) {
                    return Err(format!("location '{name}' must use '{{{placeholder}}}'"));
                }
            }
        }
        match (self.kind, &self.fabric) {
            (RegistryKind::Cartridges, Some(fabric)) => {
                check_https("fabric", fabric)?;
                if fabric.ends_with('/') {
                    return Err(format!("fabric '{fabric}' ends with '/'"));
                }
            }
            (RegistryKind::Cartridges, None) => {
                return Err("a cartridges registry must name its fabric".to_string())
            }
            (_, Some(_)) => {
                return Err(format!(
                    "only a cartridges registry names a fabric, and this is a {} registry",
                    self.kind.wire_name()
                ))
            }
            (_, None) => {}
        }
        if let Some(not_after) = self.not_after {
            if not_after <= self.issued_at {
                return Err(format!(
                    "not_after {not_after} is not after issued_at {}",
                    self.issued_at
                ));
            }
        }
        Ok(())
    }

    /// One location, with its placeholders filled. Every placeholder the
    /// template uses must be given, and nothing else may be: a value nobody
    /// substitutes is a caller asking for a different location than it gets.
    pub fn location(&self, name: &str, values: &[(&str, &str)]) -> Result<String, String> {
        let template = self.locations.get(name).ok_or_else(|| {
            format!("the {} registry at {} has no location '{name}'", self.kind.wire_name(), self.anchor)
        })?;
        let used = placeholders_in(template)?;
        for (key, _) in values {
            if !used.iter().any(|u| u == key) {
                return Err(format!("location '{name}' does not take '{key}'"));
            }
        }
        let mut out = template.clone();
        for placeholder in &used {
            let value = values
                .iter()
                .find(|(key, _)| key == placeholder)
                .map(|(_, value)| *value)
                .ok_or_else(|| format!("location '{name}' needs '{placeholder}'"))?;
            out = out.replace(&format!("{{{placeholder}}}"), value);
        }
        Ok(out)
    }
}

/// Whether `s` can be a registry slug: `[a-z0-9][a-z0-9.-]{0,62}`, and not the
/// reserved `dev`.
pub fn is_slug(s: &str) -> bool {
    let bytes = s.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && s != DEV_SLUG
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'.' || *b == b'-')
}

/// Whether `s` can be a lab experiment id: `[a-z0-9][a-z0-9-]{0,39}`.
pub fn is_experiment_id(s: &str) -> bool {
    let bytes = s.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 40
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes.iter().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

fn is_label(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

fn check_https(what: &str, url: &str) -> Result<(), String> {
    match url.split_once("://") {
        Some((scheme, rest)) if scheme == "https" && !rest.is_empty() && !rest.starts_with('/') => {
            if url.contains(['?', '#']) {
                Err(format!("{what} '{url}' carries a query or fragment"))
            } else {
                Ok(())
            }
        }
        _ => Err(format!("{what} '{url}' is not an https URL")),
    }
}

/// The `{name}` placeholders a template uses, in order of first use. A stray
/// brace is an error rather than literal text: templates are written by our
/// tooling, and a brace it did not mean is a defect.
fn placeholders_in(template: &str) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = Vec::new();
    let mut rest = template;
    while let Some(open) = rest.find(['{', '}']) {
        if rest.as_bytes()[open] == b'}' {
            return Err(format!("'{template}' has an unmatched '}}'"));
        }
        let after = &rest[open + 1..];
        let close = after.find('}').ok_or_else(|| format!("'{template}' has an unclosed '{{'"))?;
        let name = &after[..close];
        if name.is_empty() || !name.bytes().all(|b| b.is_ascii_lowercase() || b == b'_') {
            return Err(format!("'{template}' has a placeholder '{{{name}}}' that is not a name"));
        }
        if !out.iter().any(|n| n == name) {
            out.push(name.to_string());
        }
        rest = &after[close + 1..];
    }
    Ok(out)
}

/// Store a verified charter in a registry's folder, so what it vouches for can
/// be verified again with no network. Both files are replaced atomically.
pub fn store(verified: &VerifiedCharter, folder: &Path) -> Result<(), std::io::Error> {
    std::fs::create_dir_all(folder)?;
    write_atomic(&folder.join(CHARTER_FILE), &verified.body)?;
    write_atomic(&folder.join(CHARTER_SIG_FILE), verified.envelope.as_bytes())
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)
}

/// Read and verify the charter stored in a registry's folder, and require the
/// folder to be named for it. The anchor is not known here; the caller holds
/// every cartridge in the folder to the charter's anchor.
pub fn verify_stored(
    folder: &Path,
    trust: &CharterTrust,
    kind: RegistryKind,
    environment: Option<&str>,
    now_unix: u64,
) -> Result<VerifiedCharter, StoredCharterError> {
    let body = std::fs::read(folder.join(CHARTER_FILE)).map_err(|e| StoredCharterError::Unreadable {
        path: folder.join(CHARTER_FILE).display().to_string(),
        error: e.to_string(),
    })?;
    let envelope = std::fs::read_to_string(folder.join(CHARTER_SIG_FILE)).map_err(|e| {
        StoredCharterError::Unreadable {
            path: folder.join(CHARTER_SIG_FILE).display().to_string(),
            error: e.to_string(),
        }
    })?;
    let expect = CharterExpectation { anchor: None, kind, environment };
    let verified = verify(&body, &envelope, trust, &expect, now_unix).map_err(StoredCharterError::Refused)?;
    let folder_name = folder.file_name().and_then(|n| n.to_str()).unwrap_or_default();
    if folder_name != verified.charter.slug {
        return Err(StoredCharterError::Refused(CharterError::FolderMismatch {
            anchor: verified.charter.anchor.clone(),
            slug: verified.charter.slug.clone(),
            folder: folder_name.to_string(),
        }));
    }
    Ok(verified)
}

/// What one top-level folder of a cartridge tree holds: the cartridges built
/// with no registry, or a chartered registry's — proven by the charter stored
/// in it, never by its name.
#[derive(Debug, Clone)]
pub enum RegistryFolder {
    /// The reserved `dev` folder. Its cartridges record no registry.
    Dev,
    /// A registry's folder, named for its charter's slug.
    Chartered(VerifiedCharter),
}

impl RegistryFolder {
    /// Open a top-level folder: `dev` by its reserved name, any other by
    /// verifying the cartridges charter stored in it.
    pub fn open(
        folder: &Path,
        trust: &CharterTrust,
        environment: Option<&str>,
        now_unix: u64,
    ) -> Result<Self, StoredCharterError> {
        if folder.file_name().and_then(|n| n.to_str()) == Some(DEV_SLUG) {
            return Ok(Self::Dev);
        }
        verify_stored(folder, trust, RegistryKind::Cartridges, environment, now_unix)
            .map(Self::Chartered)
    }

    /// The folder's name.
    pub fn slug(&self) -> &str {
        match self {
            Self::Dev => DEV_SLUG,
            Self::Chartered(verified) => &verified.charter.slug,
        }
    }

    /// The anchor every cartridge in the folder must record: `None` for `dev`.
    pub fn anchor(&self) -> Option<&str> {
        match self {
            Self::Dev => None,
            Self::Chartered(verified) => Some(&verified.charter.anchor),
        }
    }
}

/// The folder under `root` that holds the registry chartered for `anchor`,
/// with its verified charter — or `None` when no folder does. A folder whose
/// stored charter does not verify is not a match, whatever its name.
pub fn find_registry_folder(
    root: &Path,
    anchor: &str,
    trust: &CharterTrust,
    kind: RegistryKind,
    environment: Option<&str>,
    now_unix: u64,
) -> Option<(std::path::PathBuf, VerifiedCharter)> {
    let entries = std::fs::read_dir(root).ok()?;
    for entry in entries.flatten() {
        let folder = entry.path();
        if !folder.join(CHARTER_FILE).is_file() {
            continue;
        }
        if let Ok(verified) = verify_stored(&folder, trust, kind, environment, now_unix) {
            if verified.charter.anchor == anchor {
                return Some((folder, verified));
            }
        }
    }
    None
}

/// Why a stored charter could not be used.
#[derive(Debug, thiserror::Error)]
pub enum StoredCharterError {
    #[error("cannot read {path}: {error}")]
    Unreadable { path: String, error: String },
    #[error(transparent)]
    Refused(CharterError),
}

/// Why fetching a charter from its anchor failed.
#[derive(Debug, thiserror::Error)]
pub enum CharterFetchError {
    #[error("network access is blocked by policy — cannot fetch the charter of {0}")]
    Offline(String),
    #[error("cannot reach {url}: {error}")]
    Unreachable { url: String, error: String },
    /// Nothing at `<anchor>/charter.json`: the URL is not a chartered registry.
    #[error("{anchor} has no charter ({url} answered 404) — it is not a MachineFabric registry")]
    Unchartered { anchor: String, url: String },
    #[error("{url} answered HTTP {status}")]
    Http { url: String, status: u16 },
    #[error("the charter of {anchor} was refused: {error}")]
    Refused { anchor: String, error: CharterError },
}

/// Fetch `<anchor>/charter.json` and its signature, and verify them.
///
/// The two are fetched with one cache-bust token, because the signature holds
/// only over a coherent pair and a CDN may cache them from different issues.
pub async fn fetch(
    client: &reqwest::Client,
    anchor: &str,
    trust: &CharterTrust,
    kind: RegistryKind,
    environment: Option<&str>,
    now_unix: u64,
) -> Result<VerifiedCharter, CharterFetchError> {
    let cache_bust = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis();
    let body = get(client, anchor, &format!("{anchor}/{CHARTER_FILE}"), cache_bust).await?;
    let envelope = get(client, anchor, &format!("{anchor}/{CHARTER_SIG_FILE}"), cache_bust).await?;
    let envelope = String::from_utf8(envelope).map_err(|e| CharterFetchError::Refused {
        anchor: anchor.to_string(),
        error: CharterError::MalformedEnvelope(e.to_string()),
    })?;
    let expect = CharterExpectation { anchor: Some(anchor), kind, environment };
    verify(&body, &envelope, trust, &expect, now_unix)
        .map_err(|error| CharterFetchError::Refused { anchor: anchor.to_string(), error })
}

async fn get(
    client: &reqwest::Client,
    anchor: &str,
    url: &str,
    cache_bust: u128,
) -> Result<Vec<u8>, CharterFetchError> {
    let response = client
        .get(format!("{url}?cb={cache_bust}"))
        .send()
        .await
        .map_err(|e| CharterFetchError::Unreachable { url: url.to_string(), error: e.to_string() })?;
    let status = response.status().as_u16();
    if status == 404 {
        return Err(CharterFetchError::Unchartered { anchor: anchor.to_string(), url: url.to_string() });
    }
    if !response.status().is_success() {
        return Err(CharterFetchError::Http { url: url.to_string(), status });
    }
    response
        .bytes()
        .await
        .map(|b| b.to_vec())
        .map_err(|e| CharterFetchError::Unreachable { url: url.to_string(), error: e.to_string() })
}

#[cfg(test)]
#[path = "charter_tests.rs"]
mod tests;
