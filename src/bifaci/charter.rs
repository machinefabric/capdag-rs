//! Registry charters: what makes a URL a MachineFabric registry.
//!
//! A registry — of cartridges, of fabric definitions, or of app releases — is
//! named by its **anchor**, and everything a client needs about it is read from
//! the charter at `<anchor>/charter.json`, never derived from the shape of a
//! URL: its slug, its kind, the environment its contents are signed under, the
//! locations of its manifests, and for a cartridges registry the fabric its caps
//! resolve against.
//!
//! The charter itself, and its verification, are `mg_srel`'s; the rules
//! it is held to are MachineFabric's ([`super::trust::charter_rules`]). What is
//! here is capdag's own use of charters: fetching one from its anchor, and the
//! folders of a cartridge tree, each of which a charter stored in it proves.

use std::path::Path;

use mg_srel::{CharterExpectation, CharterTrust, StoredCharterError, VerifiedCharter, CHARTER_FILE, CHARTER_SIG_FILE};

use super::trust::{charter_rules, RegistryKind, DEV_SLUG, FABRIC_FIELD};

/// Verify a charter's exact bytes and envelope against the charter trust, by
/// MachineFabric's charter rules.
pub fn verify(
    body: &[u8],
    envelope: &[u8],
    trust: &CharterTrust,
    expect: &CharterExpectation<'_>,
    now_unix: u64,
) -> Result<VerifiedCharter, mg_srel::CharterError> {
    mg_srel::verify_charter(body, envelope, trust, charter_rules(), expect, now_unix)
}

/// Read and verify the charter stored in a registry's folder, and require the
/// folder to be named for it.
pub fn verify_stored(
    folder: &Path,
    trust: &CharterTrust,
    kind: RegistryKind,
    environment: Option<&str>,
    now_unix: u64,
) -> Result<VerifiedCharter, StoredCharterError> {
    mg_srel::verify_stored(folder, trust, charter_rules(), kind.wire_name(), environment, now_unix)
}

/// The anchor of the fabric a cartridges registry's caps resolve against, as
/// its charter names it. `None` for every other kind.
pub fn fabric_of(charter: &mg_srel::Charter) -> Option<&str> {
    charter.field(FABRIC_FIELD)
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
        verify_stored(folder, trust, RegistryKind::Cartridges, environment, now_unix).map(Self::Chartered)
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
    Refused { anchor: String, error: mg_srel::CharterError },
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
    let expect = CharterExpectation { anchor: Some(anchor), kind: kind.wire_name(), environment };
    verify(&body, &envelope, trust, &expect, now_unix)
        .map_err(|error| CharterFetchError::Refused { anchor: anchor.to_string(), error })
}

async fn get(client: &reqwest::Client, anchor: &str, url: &str, cache_bust: u128) -> Result<Vec<u8>, CharterFetchError> {
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
