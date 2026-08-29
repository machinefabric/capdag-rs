//! The signed manifest a build ships beside its bundled cartridges.
//!
//! # What this replaces, and why
//!
//! Bundled cartridges — the ones that ship inside the engine's, the daemon's or
//! the capdag CLI's own `bundled-cartridges/` tree — have no upstream registry
//! to verify against, so they need their own integrity proof. That proof used
//! to be a content hash baked into the binary at build time, and it was
//! **disabled on macOS**: the distribution step re-signs every cartridge when
//! it seals the `.app`, which rewrites their bytes long after the engine was
//! compiled, so a baked hash could not survive. macOS was left trusting
//! Gatekeeper instead.
//!
//! That made Apple's signature the load-bearing check on one platform and ours
//! the load-bearing check on the others. It is the wrong way round. Apple's
//! signature is what stops the operating system warning a user; OUR chain is
//! what decides whether code runs, and it has to say the same thing everywhere.
//!
//! So the proof is a **signed manifest**, in the same envelope every published
//! manifest already uses:
//!
//! ```text
//! bundled-cartridges/bundle.json       ← what this build ships
//! bundled-cartridges/bundle.json.sig   ← certificates + signature over it
//! ```
//!
//! It is produced at the END of a build, after every platform signing step, so
//! there is no ordering problem left to have. It is verified by
//! [`super::release_cert::verify_manifest_envelope`] — the same function that
//! verifies a cartridge registry's manifest — against the roots baked into the
//! build. Bundled and registry cartridges are therefore proven by one chain,
//! one verifier and one set of errors.
//!
//! # Why a manifest and not a signature per binary
//!
//! A per-binary signature says "somebody with the key signed these bytes". It
//! does not say "this build ships THIS cartridge at THIS version" — so a stale
//! cartridge left in a staging tree from an earlier build carries a perfectly
//! valid signature and passes. The manifest states the set, so a bundled
//! cartridge that is not listed, or is listed at another version, is refused.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// The `format` every bundle manifest carries. A manifest without exactly this
/// is refused rather than interpreted: the shape is the contract between the
/// build that writes it and the runtime that reads it.
pub const BUNDLE_MANIFEST_FORMAT: &str = "capdag.bundle/v1";

/// The manifest's file name, inside the bundled-cartridges root.
pub const BUNDLE_MANIFEST_FILE: &str = "bundle.json";

/// The signature envelope's file name, beside the manifest.
///
/// A sidecar rather than a wrapper, for the reason every published manifest
/// uses one: the bytes that are signed have to be the bytes that are read, and
/// a signature inside the document would have to sign around itself.
pub const BUNDLE_MANIFEST_SIG_FILE: &str = "bundle.json.sig";

/// One cartridge a build ships.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundledCartridge {
    pub name: String,
    pub version: String,
    /// `release` or `nightly`. Stated so a manifest cannot vouch for a
    /// cartridge from the other channel.
    pub channel: String,
    /// The directory hash, as [`super::cartridge_json::hash_cartridge_directory`]
    /// computes it — sorted relative paths and file contents, `cartridge.json`
    /// excluded.
    ///
    /// `cartridge.json` is excluded by that function, which is what lets a
    /// build write the manifest without changing what the manifest attests.
    pub sha256: String,
}

/// What a build ships beside its executable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleManifest {
    pub format: String,
    /// The signing environment this bundle was built for. Checked against the
    /// build's own, so a staging bundle cannot be verified by a prod build
    /// even under valid signatures.
    pub environment: String,
    pub cartridges: Vec<BundledCartridge>,
}

impl BundleManifest {
    /// A manifest for a set of cartridges, in a stable order.
    ///
    /// Sorted by (name, version) so two builds of the same tree produce the
    /// same bytes and therefore the same signature — a manifest that reordered
    /// itself would make every build look like a change.
    pub fn new(environment: impl Into<String>, mut cartridges: Vec<BundledCartridge>) -> Self {
        cartridges.sort_by(|a, b| (&a.name, &a.version).cmp(&(&b.name, &b.version)));
        Self {
            format: BUNDLE_MANIFEST_FORMAT.to_string(),
            environment: environment.into(),
            cartridges,
        }
    }

    /// The exact bytes to sign and to write.
    ///
    /// Pretty-printed with a trailing newline because it is a file people read
    /// in a diff; what matters for the signature is only that the writer and
    /// the reader agree, and they agree by both going through here.
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut bytes = serde_json::to_vec_pretty(self)?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    /// What this manifest says about one cartridge, if it says anything.
    pub fn entry(&self, name: &str, version: &str) -> Option<&BundledCartridge> {
        self.cartridges
            .iter()
            .find(|one| one.name == name && one.version == version)
    }
}

/// Why a bundled cartridge could not be proven.
///
/// Every variant is a refusal. There is no advisory state and no "verified
/// except" — a bundled cartridge either has a chain that reaches the roots this
/// build trusts, or it does not run.
#[derive(Debug, thiserror::Error)]
pub enum BundleError {
    #[error(
        "this build trusts no signing roots, so nothing can prove a bundled cartridge. \
         A build that ships bundled cartridges must bake MFR_CARTRIDGE_ROOT_PUBKEYS and \
         MFR_SIGNING_ENVIRONMENT"
    )]
    NoTrust,
    #[error("no bundle manifest at {0} — this build shipped cartridges it cannot vouch for")]
    Missing(PathBuf),
    #[error("no signature at {0} — an unsigned bundle manifest proves nothing")]
    Unsigned(PathBuf),
    #[error("cannot read {path}: {source}")]
    Unreadable {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("{path} is not a bundle manifest: {reason}")]
    Malformed { path: PathBuf, reason: String },
    #[error(
        "bundle manifest has format '{found}' (expected '{BUNDLE_MANIFEST_FORMAT}')"
    )]
    UnsupportedFormat { found: String },
    #[error(
        "bundle manifest was built for the '{manifest}' signing environment and this build \
         is '{build}'"
    )]
    WrongEnvironment { manifest: String, build: String },
    #[error("the bundle manifest's signature does not verify: {0}")]
    Signature(#[from] super::release_cert::ChainError),
    #[error(
        "the bundle manifest does not list {name} {version}; this build ships a cartridge it \
         did not record"
    )]
    NotListed { name: String, version: String },
    #[error(
        "{name} {version} does not match the bundle manifest: recorded {expected}, on disk \
         {actual}"
    )]
    ContentMismatch {
        name: String,
        version: String,
        expected: String,
        actual: String,
    },
}

/// Where the manifest and its signature live under a bundled-cartridges root.
pub fn manifest_paths(bundled_root: &Path) -> (PathBuf, PathBuf) {
    (
        bundled_root.join(BUNDLE_MANIFEST_FILE),
        bundled_root.join(BUNDLE_MANIFEST_SIG_FILE),
    )
}

/// Whether a name in a bundled-cartridges root belongs to this mechanism.
///
/// Discovery reports unmanaged files in that directory, and the manifest and
/// its signature are managed — a warning about them on every startup would
/// train an operator to ignore the one that matters.
pub fn is_manifest_file(file_name: &str) -> bool {
    file_name == BUNDLE_MANIFEST_FILE || file_name == BUNDLE_MANIFEST_SIG_FILE
}

/// What a discovery run knows about the bundle it is scanning.
///
/// Carried rather than looked up, and that is deliberate. Verification is one
/// act per discovery — a chain check per cartridge would do the same work five
/// times and give five chances to disagree — and making it a value means the
/// thing that LOADS a manifest and the thing that USES one are separable, so
/// each can be tested against what it actually does.
#[derive(Debug, Clone)]
pub enum BundleProof {
    /// The manifest this root's bundled cartridges are held to.
    Verified(Box<BundleManifest>),
    /// Nothing here can vouch for a bundled cartridge, and why.
    ///
    /// Not an absence: every `installed_from: bundle` cartridge under this root
    /// is refused with this reason. A root that legitimately ships none — the
    /// operator's installed-cartridges directory — carries a reason saying so,
    /// and if a bundled cartridge ever turns up there it is refused for
    /// exactly the right reason rather than quietly hosted.
    Refused(String),
}

impl BundleProof {
    /// Read and verify the manifest under `bundled_root`.
    ///
    /// A failure becomes [`BundleProof::Refused`] rather than an error: a
    /// build with an unprovable bundle still runs, still hosts registry
    /// cartridges, and reports each bundled one it had to refuse — which is
    /// more useful than refusing to start.
    pub fn load(
        bundled_root: &Path,
        trust: Option<&super::release_cert::RegistryTrust>,
        now_unix: u64,
    ) -> Self {
        match load_verified(bundled_root, trust, now_unix) {
            Ok(manifest) => Self::Verified(Box::new(manifest)),
            Err(why) => Self::Refused(why.to_string()),
        }
    }

    /// A root that ships no bundle at all.
    ///
    /// The operator's installed-cartridges directory is one: nothing there was
    /// put there by a build, so a cartridge claiming to be bundled is in the
    /// wrong place and is refused saying so.
    pub fn none(reason: impl Into<String>) -> Self {
        Self::Refused(reason.into())
    }

    /// Hold one bundled cartridge to what this proof allows.
    pub fn check(&self, name: &str, version: &str, version_dir: &Path) -> Result<(), String> {
        match self {
            Self::Verified(manifest) => {
                verify_cartridge(manifest, name, version, version_dir).map_err(|e| e.to_string())
            }
            Self::Refused(reason) => Err(reason.clone()),
        }
    }
}

/// Read and verify the bundle manifest under `bundled_root`.
///
/// `trust` is the build's own — the roots it bakes and the environment it was
/// built for. `None` means this build trusts nothing, and a build that trusts
/// nothing cannot host a bundled cartridge; that is [`BundleError::NoTrust`]
/// rather than a silent pass.
pub fn load_verified(
    bundled_root: &Path,
    trust: Option<&super::release_cert::RegistryTrust>,
    now_unix: u64,
) -> Result<BundleManifest, BundleError> {
    let Some(trust) = trust else {
        return Err(BundleError::NoTrust);
    };
    let (manifest_path, sig_path) = manifest_paths(bundled_root);

    if !manifest_path.is_file() {
        return Err(BundleError::Missing(manifest_path));
    }
    if !sig_path.is_file() {
        return Err(BundleError::Unsigned(sig_path));
    }

    let manifest_bytes =
        std::fs::read(&manifest_path).map_err(|source| BundleError::Unreadable {
            path: manifest_path.clone(),
            source,
        })?;
    let envelope = std::fs::read_to_string(&sig_path).map_err(|source| BundleError::Unreadable {
        path: sig_path.clone(),
        source,
    })?;

    // The signature is checked over the EXACT bytes on disk, before anything
    // in them is believed — including the format and the environment. Parsing
    // first and verifying second would mean acting on an attacker's document
    // for as long as it took to decide not to.
    let roots: Vec<&str> = trust.root_pubkeys.iter().map(String::as_str).collect();
    super::release_cert::verify_manifest_envelope(
        &envelope,
        &manifest_bytes,
        &roots,
        &trust.environment,
        now_unix,
    )?;

    let manifest: BundleManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|e| BundleError::Malformed {
            path: manifest_path,
            reason: e.to_string(),
        })?;
    if manifest.format != BUNDLE_MANIFEST_FORMAT {
        return Err(BundleError::UnsupportedFormat {
            found: manifest.format,
        });
    }
    // The certificate chain already pinned the environment; this pins what the
    // manifest SAYS to the same value, so a manifest signed for one
    // environment cannot claim to describe another.
    if manifest.environment != trust.environment {
        return Err(BundleError::WrongEnvironment {
            manifest: manifest.environment,
            build: trust.environment.clone(),
        });
    }
    Ok(manifest)
}

/// Hold one bundled cartridge's directory to what the manifest recorded.
pub fn verify_cartridge(
    manifest: &BundleManifest,
    name: &str,
    version: &str,
    version_dir: &Path,
) -> Result<(), BundleError> {
    let entry = manifest
        .entry(name, version)
        .ok_or_else(|| BundleError::NotListed {
            name: name.to_string(),
            version: version.to_string(),
        })?;
    let actual = super::cartridge_json::hash_cartridge_directory(version_dir).map_err(|source| {
        BundleError::Unreadable {
            path: version_dir.to_path_buf(),
            source,
        }
    })?;
    if actual != entry.sha256 {
        return Err(BundleError::ContentMismatch {
            name: name.to_string(),
            version: version.to_string(),
            expected: entry.sha256.clone(),
            actual,
        });
    }
    Ok(())
}

/// Build a manifest by hashing every cartridge directory a build staged.
///
/// `staged` maps `(name, version, channel)` to the directory that holds it.
/// Used by the build tooling; kept here so the thing that WRITES the manifest
/// and the thing that CHECKS it hash the same way by construction.
pub fn manifest_for(
    environment: &str,
    staged: &BTreeMap<(String, String, String), PathBuf>,
) -> Result<BundleManifest, BundleError> {
    let mut cartridges = Vec::new();
    for ((name, version, channel), dir) in staged {
        let sha256 = super::cartridge_json::hash_cartridge_directory(dir).map_err(|source| {
            BundleError::Unreadable {
                path: dir.clone(),
                source,
            }
        })?;
        cartridges.push(BundledCartridge {
            name: name.clone(),
            version: version.clone(),
            channel: channel.clone(),
            sha256,
        });
    }
    Ok(BundleManifest::new(environment, cartridges))
}

#[cfg(test)]
#[path = "bundle_manifest_tests.rs"]
mod tests;
