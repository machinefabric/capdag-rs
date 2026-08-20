//! Cartridge Repository
//!
//! Fetches and caches cartridge registry data from configured cartridge repositories.
//! Provides cartridge suggestions when a cap isn't available but a cartridge exists that could provide it.

use reqwest::Client;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::RwLock;

/// Cartridge repository errors
#[derive(Debug, Error)]
pub enum CartridgeRepoError {
    #[error("HTTP request failed: {0}")]
    HttpError(String),
    #[error("Failed to parse registry response: {0}")]
    ParseError(String),
    #[error("Registry request failed with status {0}")]
    StatusError(u16),
    #[error("Network access blocked: {0}")]
    NetworkBlocked(String),
    #[error("Manifest signature verification failed: {0}")]
    ManifestVerificationFailed(String),
}

pub type Result<T> = std::result::Result<T, CartridgeRepoError>;

/// Deserialize a possibly-null string as an empty string.
/// Handles API responses where string fields may be `null` instead of absent.
fn null_as_empty_string<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    Option::<String>::deserialize(deserializer).map(|opt| opt.unwrap_or_default())
}

// =============================================================================
// Registry wire schema (v5.0)
//
// These types deserialize the JSON returned by /api/cartridges (and the
// canonical source `cartridges/registry.json`). The wire mixes camelCase
// for cartridge-level fields and snake_case for cap-level fields:
// the snake_case keys are the schema names — we name them explicitly with
// `#[serde(rename = "…")]` rather than letting the global `rename_all`
// rule transform them.
//
// Schema v5.0 adds release/nightly channel partitioning. Both channels
// are always present (possibly empty) so consumers never need conditional
// fallbacks. Each channel has its own `cartridges` map and per-cartridge
// `latestVersion`.
// =============================================================================

/// Distribution channel a cartridge entry belongs to. Top-level partition
/// of the registry. The wire form is the lowercase string the registry
/// uses for keys under `channels.`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CartridgeChannel {
    /// User-facing builds. Promoted via the publish script's `--release`.
    Release,
    /// In-flight builds. Default for the publish scripts.
    Nightly,
}

impl CartridgeChannel {
    /// Wire-form key string ("release" / "nightly").
    pub fn as_str(self) -> &'static str {
        match self {
            CartridgeChannel::Release => "release",
            CartridgeChannel::Nightly => "nightly",
        }
    }

    /// Parse the wire-form string ("release" / "nightly") at runtime.
    /// Used by hosts that read the channel out of cartridge.json /
    /// dictionary payloads — anything else is a hard error so a
    /// typo never silently masquerades as a known channel.
    pub fn parse(s: &str) -> std::result::Result<Self, String> {
        match s {
            "release" => Ok(CartridgeChannel::Release),
            "nightly" => Ok(CartridgeChannel::Nightly),
            other => Err(format!(
                "invalid CartridgeChannel '{}'; expected 'release' or 'nightly'",
                other
            )),
        }
    }
}

impl std::fmt::Display for CartridgeChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Parse a channel string in a `const` context. Used by cartridge
/// `build_manifest()` to convert `env!("MFR_CARTRIDGE_CHANNEL")` into
/// a `CartridgeChannel` at compile time. Compile-time `panic!` is the
/// right behaviour: a cartridge built without the env set or with a
/// typo is a build-system bug we want to fail before the binary ever
/// runs. Use:
/// ```ignore
/// const CHANNEL: CartridgeChannel =
///     capdag::CartridgeChannel::from_build_env(env!("MFR_CARTRIDGE_CHANNEL"));
/// ```
impl CartridgeChannel {
    pub const fn from_build_env(s: &str) -> CartridgeChannel {
        // const fn comparison: byte-by-byte equality.
        if const_eq(s, "release") {
            CartridgeChannel::Release
        } else if const_eq(s, "nightly") {
            CartridgeChannel::Nightly
        } else {
            panic!(
                "MFR_CARTRIDGE_CHANNEL must be 'release' or 'nightly'; \
                 build the cartridge with a workspace cartridge release build or \
                 `--nightly` so the env var is set"
            );
        }
    }
}

const fn const_eq(a: &str, b: &str) -> bool {
    let ab = a.as_bytes();
    let bb = b.as_bytes();
    if ab.len() != bb.len() {
        return false;
    }
    let mut i = 0;
    while i < ab.len() {
        if ab[i] != bb[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// One cap as it appears inside a `cap_group` in the registry response.
///
/// `urn`, `title`, and `aliases` are always present. `abstract` defaults to
/// false. `cap_description`, `args`, and `output` are only emitted by
/// cartridges that document them; the identity cap, for example, omits all three.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RegistryCap {
    pub urn: String,
    pub title: String,
    pub aliases: Vec<String>,
    #[serde(
        rename = "abstract",
        default,
        skip_serializing_if = "std::ops::Not::not"
    )]
    pub is_abstract: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cap_description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<RegistryCapArg>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<RegistryCapOutput>,
}

/// One argument descriptor for a registry cap.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RegistryCapArg {
    pub media_urn: String,
    pub required: bool,
    #[serde(default)]
    pub is_sequence: bool,
    #[serde(default)]
    pub sources: Vec<RegistryArgSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arg_description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_value: Option<serde_json::Value>,
}

/// One source entry on a `RegistryCapArg`. The wire form is one of three
/// shapes — stdin/position/cli_flag — at most one populated per entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RegistryArgSource {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cli_flag: Option<String>,
}

/// Output descriptor for a registry cap.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RegistryCapOutput {
    pub media_urn: String,
    #[serde(default)]
    pub is_sequence: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_description: Option<String>,
}

/// A `cap_groups[i]` entry in the registry response: a named bundle of
/// caps plus the media URNs the bundle's adapter inspects.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RegistryCapGroup {
    pub name: String,
    #[serde(default)]
    pub caps: Vec<RegistryCap>,
    #[serde(default)]
    pub adapter_urns: Vec<String>,
}

/// A cartridge entry as returned by /api/cartridges.
///
/// Top-level fields are camelCased on the wire; `cap_groups` is the only
/// snake_case field at this level and is named explicitly.
///
/// `channel` is set by the registry transformer when flattening the
/// channel-partitioned registry into the API response — every entry
/// reports which channel it lives in so consumers can render the
/// release/nightly distinction without re-deriving it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeInfo {
    pub id: String,
    pub name: String,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub version: String,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub description: String,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub author: String,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub team_id: String,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub signed_at: String,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub min_app_version: String,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub page_url: String,
    #[serde(default)]
    pub categories: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    /// Cap groups exactly as carried on the wire. Snake-cased on the wire
    /// (`cap_groups`) so we name it explicitly to override `rename_all`.
    #[serde(rename = "cap_groups")]
    pub cap_groups: Vec<RegistryCapGroup>,
    /// All versions with their builds (platform-specific packages).
    pub versions: HashMap<String, CartridgeVersionData>,
    /// All available versions (newest first)
    #[serde(default)]
    pub available_versions: Vec<String>,
    /// Channel this entry belongs to. Set by the transformer; consumers
    /// must not synthesize this field — it comes from the registry's
    /// `channels` partitioning.
    pub channel: CartridgeChannel,
    /// Registry URL this entry was fetched from. Stamped onto each
    /// entry by `fetch_registry` based on the URL the manifest was
    /// served from. Verbatim string — never trimmed, normalized, or
    /// re-derived from the manifest body. Identity comparison is byte
    /// equality.
    pub registry_url: String,
}

/// The cartridge registry response from the API (flat format)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeRegistryResponse {
    pub cartridges: Vec<CartridgeInfo>,
}

/// A platform-specific build within a version. A platform may ship more than one
/// installer format (e.g. linux-x86_64 → `.deb` + `.rpm`), so `packages` is a
/// list; consumers pick the format the host can run via [`Self::primary_package`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CartridgeBuild {
    pub platform: String,
    /// Per-format installer list (`.pkg`/`.deb`/`.rpm`/`.msi`/`.exe`).
    /// `#[serde(default)]` so a registry manifest published before
    /// `packages[]` existed (which carries only the legacy singular
    /// `package`) still deserializes instead of failing the whole parse.
    #[serde(default)]
    pub packages: Vec<CartridgeDistributionInfo>,
    /// Legacy singular installer (`{name,url,sha256,size}`, no `format`).
    /// Required-forever on the wire for already-installed macOS apps; read
    /// here only as a fallback when `packages[]` is absent, so a registry
    /// not yet republished with the dual-write keeps installing. See the
    /// cartridge-package compat seam in MACHINEFABRIC.md.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub package: Option<CartridgeDistributionInfo>,
    /// The pure-binary artifact: the raw cartridge executable, signed by the
    /// registry's release key. This is what the capdag CLI's download path
    /// consumes (installer packages are for the desktop apps). `Option` on
    /// the wire only because pre-signing versions never published one —
    /// consumers that need the binary hard-error on `None`; there is NO
    /// fallback to installer bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binary: Option<CartridgeBinaryInfo>,
}

/// The signed pure-binary artifact of a [`CartridgeBuild`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CartridgeBinaryInfo {
    /// Artifact file name (`{id}-{version}-{platform}.bin`).
    pub name: String,
    /// SHA-256 of the binary bytes (lowercase hex).
    pub sha256: String,
    /// Size in bytes.
    pub size: u64,
    /// Download URL of the raw executable. A detached `.minisig` sidecar is
    /// published at `<url>.minisig` for out-of-band `minisign -V` checks.
    pub url: String,
    /// The complete minisign signature document over the binary bytes,
    /// embedded in the manifest so one fetch carries everything a client
    /// needs to verify a download. Signed by the environment's release key,
    /// whose authority chains to the baked roots via the release-key
    /// certificate in the manifest's `.sig` envelope.
    pub signature: String,
}

impl CartridgeBuild {
    /// The installer package the host should use, preferring the platform's
    /// native format. Falls back to the legacy singular `package` when
    /// `packages[]` is empty (pre-dual-write manifests). Returns `None` only
    /// when the build ships no installer at all.
    pub fn primary_package(&self) -> Option<&CartridgeDistributionInfo> {
        let os = self.platform.split('-').next().unwrap_or("");
        let preference: &[&str] = match os {
            "darwin" => &["pkg"],
            "linux" => &["deb", "rpm"],
            "windows" => &["msi", "exe"],
            _ => &[],
        };
        for format in preference {
            if let Some(pkg) = self.packages.iter().find(|p| p.format == *format) {
                return Some(pkg);
            }
        }
        self.packages.first().or(self.package.as_ref())
    }
}

/// The platform string ({os}-{arch}) of the binary that calls this, in the
/// exact form the registry uses (`darwin-arm64`, `darwin-x86_64`,
/// `linux-x86_64`, `windows-x86_64`). Derived from `cfg!`/`std::env::consts`
/// at compile time — the engine binary literally runs on this platform, so
/// this is the authoritative host string for compatibility resolution.
/// Single source of truth: every consumer that needs "what am I running on?"
/// calls this rather than re-deriving the os/arch mapping.
pub fn host_platform() -> String {
    let os = if cfg!(target_os = "macos") {
        "darwin"
    } else if cfg!(target_os = "linux") {
        "linux"
    } else if cfg!(target_os = "windows") {
        "windows"
    } else {
        std::env::consts::OS
    };
    let arch = match std::env::consts::ARCH {
        "aarch64" => "arm64",
        other => other,
    };
    format!("{os}-{arch}")
}

/// Host-compatibility status of a registry cartridge, resolved against a
/// specific host platform string and the engine's baked fabric manifest
/// version. Mirrors the proto `CartridgeCompatibilityStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatStatus {
    /// The latest version has a build for this host platform — install as-is.
    Compatible,
    /// The latest version has no host build, but an older version does;
    /// `resolved_version` names that older version. Install it, mark outdated.
    CompatibleOutdated,
    /// No version has a build for this host platform. Nothing to install.
    Incompatible,
}

/// The resolved verdict the engine attaches to an available cartridge: which
/// version/package the host should install (if any) and a human reason when
/// it is not the latest-and-greatest. Computed once server-side so both
/// clients render identically without re-deriving platform/version logic.
#[derive(Debug, Clone)]
pub struct CartridgeCompatibilityResolution {
    pub status: CompatStatus,
    pub host_platform: String,
    /// Newest version that has a build for this host (None when Incompatible).
    pub resolved_version: Option<String>,
    /// Host-preferred installer package within `resolved_version` (None when
    /// Incompatible). Cloned from the registry data.
    pub resolved_package: Option<CartridgeDistributionInfo>,
    /// Explanation, set whenever status is not `Compatible`.
    pub reason: Option<String>,
}

impl CartridgeInfo {
    /// Find this cartridge's build for `host_platform` within a given version,
    /// if any. The host package within it is then chosen by
    /// [`CartridgeBuild::primary_package`].
    fn build_for_host<'a>(
        &'a self,
        version: &str,
        host_platform: &str,
    ) -> Option<&'a CartridgeBuild> {
        self.versions
            .get(version)
            .and_then(|v| v.builds.iter().find(|b| b.platform == host_platform))
    }

    /// Resolve which version/package this host should install, scanning
    /// versions newest-first (`available_versions` is the authoritative
    /// newest-first ordering). The newest version with a host build wins:
    ///   * it IS the latest version → `Compatible`
    ///   * it is older than the latest → `CompatibleOutdated`
    ///   * no version has a host build → `Incompatible`
    ///
    /// `host_platform` is normally [`host_platform()`]; passed in so the
    /// resolution is unit-testable for arbitrary hosts.
    ///
    /// "Latest" is `self.version` — the same field [`Self::build_for_platform`]
    /// trusts — not `available_versions.first()`. They must agree; if they do
    /// not, that is a registry transformer bug, and the host build found at
    /// `self.version` still classifies as `Compatible` while any other found
    /// version classifies as `CompatibleOutdated`. We do not paper over a
    /// `self.version` with no host build by silently calling it latest.
    pub fn resolve_for_host(&self, host_platform: &str) -> CartridgeCompatibilityResolution {
        let latest = self.version.as_str();

        for ver in &self.available_versions {
            let Some(build) = self.build_for_host(ver, host_platform) else {
                continue;
            };
            // primary_package() returns None only when the build ships no
            // installer at all — a build entry with an empty packages[] and no
            // legacy package. That is a malformed registry build; skip it
            // rather than resolve to a version the host cannot actually
            // download, and keep scanning older versions for a usable one.
            let Some(pkg) = build.primary_package().cloned() else {
                continue;
            };
            if ver == latest {
                return CartridgeCompatibilityResolution {
                    status: CompatStatus::Compatible,
                    host_platform: host_platform.to_string(),
                    resolved_version: Some(ver.clone()),
                    resolved_package: Some(pkg),
                    reason: None,
                };
            }
            return CartridgeCompatibilityResolution {
                status: CompatStatus::CompatibleOutdated,
                host_platform: host_platform.to_string(),
                resolved_version: Some(ver.clone()),
                resolved_package: Some(pkg),
                reason: Some(format!(
                    "Latest {latest} has no {host_platform} build; newest compatible is {ver}"
                )),
            };
        }

        CartridgeCompatibilityResolution {
            status: CompatStatus::Incompatible,
            host_platform: host_platform.to_string(),
            resolved_version: None,
            resolved_package: None,
            reason: Some(format!(
                "No installable {host_platform} build available in any version"
            )),
        }
    }
}

/// A cartridge version's data (v5.0 schema).
/// Each version has one or more platform-specific builds.
///
/// `notes_url` is the absolute URL of the version's release-notes
/// Markdown file, when one was uploaded at publish time. Optional —
/// cartridges historically did not ship per-version notes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeVersionData {
    pub release_date: String,
    #[serde(default)]
    pub changelog: Vec<String>,
    #[serde(default)]
    pub min_app_version: String,
    pub builds: Vec<CartridgeBuild>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes_url: Option<String>,
}

/// Distribution file info (package). `url` is the absolute URL of
/// the package — every consumer downloads from that URL directly.
/// There is no derived URL pattern any more.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CartridgeDistributionInfo {
    pub name: String,
    pub sha256: String,
    pub size: u64,
    pub url: String,
    /// Installer format: "pkg" (macOS), "deb"/"rpm" (Linux), "msi"/"exe" (Windows).
    /// `#[serde(default)]` + skip-when-empty so the legacy singular `package`
    /// (which has no `format`) round-trips through this same struct.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub format: String,
}

/// A cartridge entry in the source-of-truth registry (nested
/// `channels.<channel>.cartridges.{id}` map). The transformer in
/// `CartridgeRepoServer` produces a `CartridgeInfo` from each entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeRegistryEntry {
    pub name: String,
    pub description: String,
    pub author: String,
    #[serde(default)]
    pub page_url: String,
    pub team_id: String,
    #[serde(default)]
    pub min_app_version: String,
    /// Snake-cased on the wire; named explicitly to override `rename_all`.
    #[serde(rename = "cap_groups", default)]
    pub cap_groups: Vec<RegistryCapGroup>,
    #[serde(default)]
    pub categories: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub latest_version: String,
    pub versions: HashMap<String, CartridgeVersionData>,
}

/// One channel's cartridges map. Always present in the parent
/// `CartridgeRegistry.channels`, possibly empty.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeChannelEntries {
    #[serde(default)]
    pub cartridges: HashMap<String, CartridgeRegistryEntry>,
}

/// Per-channel partitioning of the registry. Each channel is a
/// distinct namespace — a cartridge id can exist independently in
/// release and nightly with potentially different versions and
/// metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeRegistryChannels {
    /// User-facing release channel.
    pub release: CartridgeChannelEntries,
    /// In-flight nightly channel.
    pub nightly: CartridgeChannelEntries,
}

/// The v5.0 cartridge registry (channel-partitioned schema). Both
/// `release` and `nightly` are always present (possibly empty) so
/// every consumer can iterate them without conditional fallbacks.
///
/// `registry_url` is self-referential — the verbatim URL operators
/// use to reference this registry. The fetch path cross-checks the
/// URL it dereferenced against this field; a mismatch is treated as
/// manifest corruption rather than a silent reinterpretation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeRegistry {
    pub schema_version: String,
    /// Cartridge registry regime version. v0 was the pre-versioning legacy at
    /// the bare `/manifest` path; this workspace speaks only v>=1 at the
    /// versioned `/v<version>/manifest` path. Validated against the baked
    /// [`crate::CARTRIDGE_REGISTRY_VERSION`] in [`CartridgeRepoServer::new`].
    pub registry_version: u32,
    pub last_updated: String,
    pub registry_url: String,
    /// The fabric registry (caps/media/aliases) this cartridge registry's cap
    /// definitions were resolved against — the verbatim `CDG_FABRIC_REGISTRY_URL`
    /// the caps were published under. A client only ingests a cartridge registry
    /// whose `fabric_registry_url` equals its own baked fabric, so every
    /// cartridge registry a client uses shares ONE fabric and their caps stay
    /// mutually comparable. Enforced in [`CartridgeRepoServer::new`].
    pub fabric_registry_url: String,
    pub channels: CartridgeRegistryChannels,
}

/// A cartridge suggestion for a missing cap. `channel` and
/// `registry_url` together identify which (registry, channel) the
/// suggesting cartridge lives in so the UI can show the
/// (registry, release/nightly) distinction. The same id can be
/// suggested from multiple registries simultaneously, each entry
/// carrying its own provenance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CartridgeSuggestion {
    pub cartridge_id: String,
    pub cartridge_name: String,
    pub cartridge_description: String,
    pub cap_urn: String,
    pub cap_title: String,
    pub latest_version: String,
    pub repo_url: String,
    pub page_url: String,
    pub channel: CartridgeChannel,
    /// Verbatim URL of the registry that surfaced this suggestion.
    /// Always non-empty for registry-sourced suggestions; suggestions
    /// never come from dev installs.
    pub registry_url: String,
}

/// Composite key — `(registry_url, channel, id)` is the authoritative
/// cache key. A cartridge id is unique within a (registry × channel)
/// pair but can appear independently across multiple registries and
/// across both channels with completely different metadata. The
/// registry URL is the verbatim byte string used to fetch the
/// manifest; the cache holds one cartridge entry per (registry,
/// channel, id) triple.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CartridgeKey {
    registry_url: String,
    channel: CartridgeChannel,
    id: String,
}

/// Cached cartridge repository data
struct CartridgeRepoCache {
    /// All cartridges indexed by `(channel, id)`.
    cartridges: HashMap<CartridgeKey, CartridgeInfo>,
    /// Cap URN (canonical normalized form) → list of cartridges that
    /// provide it. Each entry references a `(channel, id)` pair so the
    /// suggestion path can preserve channel provenance.
    cap_to_cartridges: HashMap<String, Vec<CartridgeKey>>,
    /// When the cache was last updated
    last_updated: Instant,
    /// The repo URL this cache is from
    repo_url: String,
}

/// Service for fetching and caching cartridge repository data
pub struct CartridgeRepo {
    http_client: Client,
    /// Cache per repo URL
    caches: Arc<RwLock<HashMap<String, CartridgeRepoCache>>>,
    /// Cache TTL in seconds
    cache_ttl: Duration,
    offline_flag: Arc<AtomicBool>,
    /// When set, every manifest fetch REQUIRES the `<repo-url>.sig` sidecar
    /// to verify (root → certificate → release key → exact manifest bytes)
    /// before the manifest is trusted — a missing or invalid signature
    /// rejects the whole manifest. `None` = no verification (dev builds, and
    /// server-side uses that only serve manifests they don't consume).
    trust: Arc<RwLock<Option<crate::bifaci::release_cert::RegistryTrust>>>,
    /// Per-repo release keys extracted from the verified manifest sidecar:
    /// repo_url → [(key_id, release_pubkey_b64)]. Binary artifact signatures
    /// must verify under one of these. Only populated when `trust` is set.
    verified_release_keys: Arc<RwLock<HashMap<String, Vec<(String, String)>>>>,
    /// Last sync outcome per repo URL: `Some(error)` when the most recent
    /// `sync_repos` fetch/verify for that URL failed. Callers that treat a
    /// registry as mandatory (the CLI's `CartridgeManager::init`) hard-fail
    /// on this instead of discovering it later as a misleading
    /// "cartridge not found".
    sync_errors: Arc<RwLock<HashMap<String, String>>>,
    /// The fabric registry THIS client resolves caps against (its baked
    /// `CDG_FABRIC_REGISTRY_URL`). Every fetched cartridge registry must declare
    /// this same fabric (`CartridgeRepoServer::new` enforces it) so all
    /// registries a client uses share one fabric and their caps are comparable.
    expected_fabric_url: String,
}

impl CartridgeInfo {
    /// Check if cartridge is signed (has team_id and signed_at)
    pub fn is_signed(&self) -> bool {
        !self.team_id.is_empty() && !self.signed_at.is_empty()
    }

    /// Get the build for a specific platform from the latest version.
    pub fn build_for_platform(&self, platform: &str) -> Option<&CartridgeBuild> {
        self.versions
            .get(&self.version)
            .and_then(|v| v.builds.iter().find(|b| b.platform == platform))
    }

    /// Get all platforms available across all versions.
    pub fn available_platforms(&self) -> Vec<String> {
        let mut platforms: Vec<String> = self
            .versions
            .values()
            .flat_map(|v| v.builds.iter().map(|b| b.platform.clone()))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        platforms.sort();
        platforms
    }

    /// Iterate every `RegistryCap` across every group, in declaration order.
    /// Use this whenever you need a flat view of the cartridge's caps —
    /// it is the only sanctioned way to walk caps now that the on-wire
    /// shape groups them.
    pub fn iter_caps(&self) -> impl Iterator<Item = &RegistryCap> {
        self.cap_groups.iter().flat_map(|g| g.caps.iter())
    }
}

impl CartridgeRepo {
    /// Create a new cartridge repo service. `expected_fabric_url` is the fabric
    /// this client resolves caps against (its baked `CDG_FABRIC_REGISTRY_URL`);
    /// every fetched cartridge registry must declare the same fabric.
    pub fn new(cache_ttl_seconds: u64, expected_fabric_url: impl Into<String>) -> Self {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent("MachineFabricEngine/1.0.0")
            .build()
            .expect("Failed to create HTTP client");

        Self {
            http_client,
            caches: Arc::new(RwLock::new(HashMap::new())),
            cache_ttl: Duration::from_secs(cache_ttl_seconds),
            offline_flag: Arc::new(AtomicBool::new(false)),
            trust: Arc::new(RwLock::new(None)),
            verified_release_keys: Arc::new(RwLock::new(HashMap::new())),
            sync_errors: Arc::new(RwLock::new(HashMap::new())),
            expected_fabric_url: expected_fabric_url.into(),
        }
    }

    /// Set the offline flag. When true, all registry fetches are blocked.
    pub fn set_offline(&self, offline: bool) {
        self.offline_flag.store(offline, Ordering::Relaxed);
    }

    /// Configure the signing trust anchors. With trust set, every manifest
    /// fetch requires a chain-valid `<repo-url>.sig` sidecar (see the field
    /// doc); without, manifests are consumed unverified (dev builds only).
    pub async fn set_trust(&self, trust: Option<crate::bifaci::release_cert::RegistryTrust>) {
        *self.trust.write().await = trust;
    }

    /// The most recent sync failure for a repo URL, if any. Cleared by a
    /// subsequent successful sync of the same URL.
    pub async fn sync_error(&self, repo_url: &str) -> Option<String> {
        self.sync_errors.read().await.get(repo_url).cloned()
    }

    /// The release keys `(key_id, pubkey_b64)` authorized by the verified
    /// manifest sidecar of `repo_url`. Empty when trust is unset or the repo
    /// has not been (successfully) synced.
    pub async fn verified_release_keys(&self, repo_url: &str) -> Vec<(String, String)> {
        self.verified_release_keys
            .read()
            .await
            .get(repo_url)
            .cloned()
            .unwrap_or_default()
    }

    /// Fetch and chain-verify the manifest signature sidecar at
    /// `<repo_url>.sig` over the exact `manifest_bytes`, returning the
    /// trusted release keys. Every failure names its cause; none fall
    /// through.
    async fn verify_manifest_sidecar(
        &self,
        repo_url: &str,
        cache_bust: u64,
        manifest_bytes: &[u8],
        trust: &crate::bifaci::release_cert::RegistryTrust,
    ) -> Result<Vec<(String, String)>> {
        // Same cache-bust token as the manifest fetch: signature verification
        // only holds over a COHERENT (manifest, sidecar) pair, and two
        // independently CDN-cached objects can come from different publishes.
        let sidecar_url = format!("{repo_url}.sig?cb={cache_bust}");
        let response = self
            .http_client
            .get(&sidecar_url)
            .send()
            .await
            .map_err(|e| {
                CartridgeRepoError::HttpError(format!(
                    "Failed to fetch manifest signature sidecar {sidecar_url}: {e}"
                ))
            })?;
        if !response.status().is_success() {
            return Err(CartridgeRepoError::ManifestVerificationFailed(format!(
                "manifest signature sidecar missing (HTTP {} from {sidecar_url}) — refusing \
                 to trust an unsigned manifest. The registry must be re-published under the \
                 signing regime.",
                response.status().as_u16()
            )));
        }
        let envelope_json = response.text().await.map_err(|e| {
            CartridgeRepoError::HttpError(format!(
                "Failed to read manifest signature sidecar {sidecar_url}: {e}"
            ))
        })?;
        let roots: Vec<&str> = trust.root_pubkeys.iter().map(String::as_str).collect();
        let chain = crate::bifaci::release_cert::verify_manifest_envelope(
            &envelope_json,
            manifest_bytes,
            &roots,
            &trust.environment,
            crate::bifaci::release_cert::unix_now(),
        )
        .map_err(|e| {
            CartridgeRepoError::ManifestVerificationFailed(format!(
                "manifest signature chain verification FAILED for {repo_url}: {e}"
            ))
        })?;
        Ok(chain.trusted_release_keys)
    }

    /// Fetch the v5.0 channel-partitioned cartridge manifest from a URL
    /// and flatten it via `CartridgeRepoServer` into the
    /// `CartridgeRegistryResponse` shape the cache expects (one
    /// `CartridgeInfo` per `(channel, id)` pair, channel set on each).
    ///
    /// 404 is treated as "no cartridges published yet" — equivalent to
    /// an empty manifest. Any other non-success status, network failure,
    /// JSON-parse error, or schema validation error surfaces as a hard
    /// `CartridgeRepoError`. There is no fallback to a stale cached
    /// shape — the manifest is the source of truth.
    async fn fetch_registry(&self, repo_url: &str) -> Result<CartridgeRegistryResponse> {
        if self.offline_flag.load(Ordering::Relaxed) {
            return Err(CartridgeRepoError::NetworkBlocked(format!(
                "Network access blocked by policy — cannot fetch cartridge registry '{}'",
                repo_url
            )));
        }
        // The manifest is mutable and signed by a separate sidecar object; a
        // CDN may cache the two from DIFFERENT publishes, which fails
        // signature verification (and, downstream, artifact sha checks) with
        // no client-side error. A fresh cache-bust token on both fetches
        // forces one coherent origin read per sync.
        let cache_bust = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_millis() as u64;
        let manifest_fetch_url = format!(
            "{repo_url}{}cb={cache_bust}",
            if repo_url.contains('?') { "&" } else { "?" }
        );
        let response = self
            .http_client
            .get(&manifest_fetch_url)
            .send()
            .await
            .map_err(|e| {
                CartridgeRepoError::HttpError(format!("Failed to fetch from {}: {}", repo_url, e))
            })?;

        if response.status().as_u16() == 404 {
            // Manifest not published yet. Return an empty response so
            // the cache reflects "no cartridges available" without
            // poisoning future syncs.
            return Ok(CartridgeRegistryResponse {
                cartridges: Vec::new(),
            });
        }
        if !response.status().is_success() {
            return Err(CartridgeRepoError::StatusError(response.status().as_u16()));
        }

        // Two-step parse so the error message names the precise failure
        // (HTTP read, JSON syntax, schema mismatch) and includes a
        // sample of the body. `response.json()` collapses everything
        // into "error decoding response body".
        let status = response.status();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<missing>")
            .to_string();
        let body_bytes = response.bytes().await.map_err(|e| {
            CartridgeRepoError::HttpError(format!(
                "Failed to read body from {} (status={}, content-type={}): {}",
                repo_url, status, content_type, e
            ))
        })?;
        let body_len = body_bytes.len();

        // With trust configured, the manifest's signature sidecar must
        // chain-verify over these EXACT bytes before anything is parsed or
        // cached — a tampered manifest is rejected wholesale, and the
        // sidecar's certificate-authorized release keys are retained for
        // binary artifact verification.
        let trust = self.trust.read().await.clone();
        if let Some(trust) = trust {
            let release_keys = self
                .verify_manifest_sidecar(repo_url, cache_bust, &body_bytes, &trust)
                .await?;
            self.verified_release_keys
                .write()
                .await
                .insert(repo_url.to_string(), release_keys);
        }
        let manifest: CartridgeRegistry = serde_json::from_slice(&body_bytes).map_err(|e| {
            // Truncate body sample for log readability but keep enough
            // to see HTML error pages, schema-drift fields, etc.
            let preview_max = 1024usize.min(body_len);
            let preview = String::from_utf8_lossy(&body_bytes[..preview_max]);
            tracing::error!(
                target: "cartridge_repo",
                url = repo_url,
                status = status.as_u16(),
                content_type = %content_type,
                body_len = body_len,
                error_line = e.line(),
                error_column = e.column(),
                error_classify = ?e.classify(),
                body_preview = %preview,
                "[CartridgeRepo] manifest JSON parse failed"
            );
            CartridgeRepoError::ParseError(format!(
                "Failed to parse from {} (status={}, content-type={}, body_len={}): {} at line {} col {}",
                repo_url,
                status,
                content_type,
                body_len,
                e,
                e.line(),
                e.column()
            ))
        })?;

        // Self-referential check: the manifest declares its own URL.
        // It must match the URL we just fetched from byte-for-byte —
        // a mismatch is a manifest-corruption signal (the publisher
        // wrote the wrong self-URL, or the manifest is being served
        // from an unexpected mirror). Either way, refuse to ingest;
        // identity downstream depends on this string.
        if manifest.registry_url != repo_url {
            return Err(CartridgeRepoError::ParseError(format!(
                "Manifest from {} declares registry_url='{}' — these must match byte-for-byte",
                repo_url, manifest.registry_url
            )));
        }

        // Flatten via the server transformer so `channel` and
        // `registry_url` are set on every CartridgeInfo and schema
        // validation runs once at the entry point rather than smeared
        // across the cache. The server stamps `registry_url` from the
        // URL we just fetched the manifest from — verbatim string,
        // identity comparison downstream is byte equality.
        let server = CartridgeRepoServer::new(manifest, repo_url, &self.expected_fabric_url)?;
        server.get_cartridges()
    }

    /// Update cache from a registry response.
    ///
    /// The flat response wrapper carries `channel` and `registry_url`
    /// on every entry. The cache key is `(registry_url, channel, id)`
    /// so the same id can coexist across multiple registries × both
    /// channels with separate metadata/versions.
    ///
    /// The cap-URN → cartridges index uses the *normalized* form of each
    /// declared URN as the key (parse via `CapUrn::from_string`, then
    /// `to_string()`). Two URNs that are textually different but
    /// canonically identical collapse into the same bucket. A cap URN
    /// that fails to parse is a registry corruption — propagated as
    /// `ParseError` rather than silently inserting the malformed
    /// string, per the no-fallback regime.
    fn update_cache(
        caches: &mut HashMap<String, CartridgeRepoCache>,
        repo_url: &str,
        registry: CartridgeRegistryResponse,
    ) -> Result<()> {
        use crate::urn::cap_urn::CapUrn;
        let mut cartridges: HashMap<CartridgeKey, CartridgeInfo> = HashMap::new();
        let mut cap_to_cartridges: HashMap<String, Vec<CartridgeKey>> = HashMap::new();

        for cartridge_info in registry.cartridges {
            let key = CartridgeKey {
                registry_url: cartridge_info.registry_url.clone(),
                channel: cartridge_info.channel,
                id: cartridge_info.id.clone(),
            };
            for cap in cartridge_info.iter_caps() {
                let parsed = CapUrn::from_string(&cap.urn).map_err(|e| {
                    CartridgeRepoError::ParseError(format!(
                        "cartridge {} ({} @ {}): invalid cap URN '{}': {}",
                        key.id, key.channel, key.registry_url, cap.urn, e
                    ))
                })?;
                let normalized = parsed.to_string();
                cap_to_cartridges
                    .entry(normalized)
                    .or_default()
                    .push(key.clone());
            }
            cartridges.insert(key, cartridge_info);
        }

        caches.insert(
            repo_url.to_string(),
            CartridgeRepoCache {
                cartridges,
                cap_to_cartridges,
                last_updated: Instant::now(),
                repo_url: repo_url.to_string(),
            },
        );
        Ok(())
    }

    /// Sync cartridge data from the given repository URLs.
    ///
    /// A fetch error or a malformed registry response surfaces in the log
    /// and we move on to the next repo: a single bad repo must not stall
    /// the others. The error is logged at error level so it is still
    /// visible — there is no silent swallowing.
    pub async fn sync_repos(&self, repo_urls: &[String]) {
        for repo_url in repo_urls {
            match self.fetch_registry(repo_url).await {
                Ok(registry) => {
                    let mut caches = self.caches.write().await;
                    match Self::update_cache(&mut caches, repo_url, registry) {
                        Ok(()) => {
                            self.sync_errors.write().await.remove(repo_url);
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to index cartridge repo {} into cache: {}",
                                repo_url,
                                e
                            );
                            self.sync_errors
                                .write()
                                .await
                                .insert(repo_url.clone(), e.to_string());
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to sync cartridge repo {}: {}", repo_url, e);
                    // Recorded so callers that treat this registry as
                    // mandatory (CartridgeManager::init) can hard-fail on
                    // the REAL cause (e.g. a signature-verification
                    // failure) instead of a later misleading
                    // "cartridge not found".
                    self.sync_errors
                        .write()
                        .await
                        .insert(repo_url.clone(), e.to_string());
                }
            }
        }
    }

    /// Check if a cache is stale
    fn is_cache_stale(&self, cache: &CartridgeRepoCache) -> bool {
        cache.last_updated.elapsed() > self.cache_ttl
    }

    /// Get cartridge suggestions for a cap URN that isn't available.
    ///
    /// `cap_urn` is parsed via `CapUrn::from_string`; the parsed-and-
    /// re-serialized form is the canonical key used to look up the
    /// cap-to-cartridges index. Inside each candidate cartridge we walk
    /// its groups via `iter_caps()` and match on `conforms_to` so the
    /// requested cap (treated as the pattern) is checked against the
    /// declared cap (the candidate): cap dispatch is order-theoretic,
    /// not string-equality, and the `op` tag has no functional role —
    /// only `in` and `out` are semantically meaningful, encoded by the
    /// parsed `CapUrn` predicates.
    pub async fn get_suggestions_for_cap(&self, cap_urn: &str) -> Vec<CartridgeSuggestion> {
        use crate::urn::cap_urn::CapUrn;
        let caches = self.caches.read().await;
        let mut suggestions = Vec::new();

        let requested = match CapUrn::from_string(cap_urn) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(
                    "get_suggestions_for_cap: invalid cap URN '{}': {}",
                    cap_urn,
                    e
                );
                return Vec::new();
            }
        };
        // This is RESOLUTION, not dispatch. `requested` is a fully-resolved,
        // concrete cap — an alias resolved to exactly one cap URN — and we must
        // find the cartridge that implements THAT cap, not merely one that could
        // handle it. So a cartridge provides it iff one of its DECLARED caps is
        // EQUIVALENT to it: `is_equivalent` is the SYMMETRIC relation (each side
        // accepts the other — same position in the specificity lattice), never
        // the looser directional `is_dispatchable`/`conforms_to` used for
        // "find anything that would match" (get_cartridges_by_cap). Choosing
        // dispatch here would let a cartridge declaring a more-general cap
        // silently stand in for the exact cap the alias named — a surprising,
        // non-deterministic substitution. See ../docs/07-dispatch.md,
        // "Resolution vs. dispatch: which predicate?".
        //
        // Equivalence is decided on in/out/effect by the CapUrn parser, NEVER by
        // string equality. We iterate the cartridges rather than probe a
        // string-keyed index because two semantically-equivalent cap URNs can
        // serialize to different strings (an arbitrary tag, the position of the
        // op marker), so a string-keyed prefilter silently drops equivalent
        // candidates before equivalence is ever tested. The op marker and every
        // non-in/out/effect tag are arbitrary and carry no functional weight.
        for cache in caches.values() {
            for (key, cartridge) in &cache.cartridges {
                let Some(cap_info) = cartridge.iter_caps().find(|c| {
                    CapUrn::from_string(&c.urn)
                        .map(|declared| declared.is_equivalent(&requested))
                        .unwrap_or(false)
                }) else {
                    continue;
                };
                let page_url = if cartridge.page_url.is_empty() {
                    cache.repo_url.clone()
                } else {
                    cartridge.page_url.clone()
                };
                suggestions.push(CartridgeSuggestion {
                    cartridge_id: key.id.clone(),
                    cartridge_name: cartridge.name.clone(),
                    cartridge_description: cartridge.description.clone(),
                    cap_urn: requested.to_string(),
                    cap_title: cap_info.title.clone(),
                    latest_version: cartridge.version.clone(),
                    repo_url: cache.repo_url.clone(),
                    page_url,
                    channel: key.channel,
                    registry_url: key.registry_url.clone(),
                });
            }
        }

        suggestions
    }

    /// Get all available cartridges from all repos. Returns
    /// `(registry_url, channel, id, info)` so consumers can render
    /// the (registry, channel) distinction without looking it up
    /// separately. Registry URL is the verbatim string the operator
    /// configured.
    pub async fn get_all_cartridges(
        &self,
    ) -> Vec<(String, CartridgeChannel, String, CartridgeInfo)> {
        let caches = self.caches.read().await;
        let mut all_cartridges = Vec::new();

        for cache in caches.values() {
            for (key, cartridge_info) in &cache.cartridges {
                all_cartridges.push((
                    key.registry_url.clone(),
                    key.channel,
                    key.id.clone(),
                    cartridge_info.clone(),
                ));
            }
        }

        all_cartridges
    }

    /// Get all caps available from cartridges (not necessarily installed)
    pub async fn get_all_available_caps(&self) -> Vec<String> {
        let caches = self.caches.read().await;
        let mut caps: Vec<String> = caches
            .values()
            .flat_map(|cache| cache.cap_to_cartridges.keys().cloned())
            .collect();
        caps.sort();
        caps.dedup();
        caps
    }

    /// Check if any repo needs syncing (cache is stale or missing)
    pub async fn needs_sync(&self, repo_urls: &[String]) -> bool {
        let caches = self.caches.read().await;

        for repo_url in repo_urls {
            match caches.get(repo_url) {
                None => return true,
                Some(cache) if self.is_cache_stale(cache) => return true,
                _ => {}
            }
        }

        false
    }

    /// Get cartridge info by `(registry_url, channel, id)`. All three
    /// fields are required because the same id can independently
    /// exist across multiple registries × both channels with
    /// distinct version sets and metadata — there is no implicit
    /// fallback that picks one over another. `registry_url` is the
    /// verbatim string the cache was indexed under (the URL the
    /// operator configured).
    pub async fn get_cartridge(
        &self,
        registry_url: &str,
        channel: CartridgeChannel,
        cartridge_id: &str,
    ) -> Option<CartridgeInfo> {
        let caches = self.caches.read().await;
        let key = CartridgeKey {
            registry_url: registry_url.to_string(),
            channel,
            id: cartridge_id.to_string(),
        };

        // Cache is keyed by repo_url at the outer level, so look up
        // directly to avoid scanning every cache for unrelated
        // registries.
        if let Some(cache) = caches.get(registry_url) {
            return cache.cartridges.get(&key).cloned();
        }

        None
    }

    /// Get suggestions for caps that could be provided by cartridges but aren't currently available
    /// Takes a list of currently available cap URNs and returns suggestions for missing ones
    pub async fn get_suggestions_for_missing_caps(
        &self,
        available_caps: &[String],
        requested_caps: &[String],
    ) -> Vec<CartridgeSuggestion> {
        let available_set: std::collections::HashSet<&String> = available_caps.iter().collect();
        let mut suggestions = Vec::new();

        for cap_urn in requested_caps {
            if !available_set.contains(cap_urn) {
                let cap_suggestions = self.get_suggestions_for_cap(cap_urn).await;
                suggestions.extend(cap_suggestions);
            }
        }

        suggestions
    }
}

/// Cartridge repository server - serves registry data with queries
/// Transforms v5.0 nested registry schema to flat API response format
#[derive(Debug)]
pub struct CartridgeRepoServer {
    registry: CartridgeRegistry,
    /// Verbatim registry URL the manifest was served from. Stamped onto
    /// every `CartridgeInfo` the server emits so consumers downstream
    /// can carry the (registry_url, channel, id) identity without
    /// re-deriving it. The server has no way to determine this on its
    /// own — the caller passes the URL it just fetched from.
    registry_url: String,
}

impl CartridgeRepoServer {
    /// Create a new server instance from a v5.0 channel-partitioned
    /// registry, tagged with the URL it was fetched from. The URL is
    /// the verbatim string the operator/installer used; identity
    /// comparison downstream is byte-equality.
    /// `expected_fabric_url` is the fabric registry THIS build resolves caps
    /// against (its baked `CDG_FABRIC_REGISTRY_URL`). The cartridge registry's
    /// declared `fabric_registry_url` must equal it byte-for-byte — otherwise
    /// the registry's caps were resolved against a different fabric and are not
    /// comparable with this client's, which would silently break cap routing.
    pub fn new(
        registry: CartridgeRegistry,
        registry_url: impl Into<String>,
        expected_fabric_url: impl AsRef<str>,
    ) -> Result<Self> {
        if registry.schema_version != "5.0" {
            return Err(CartridgeRepoError::ParseError(format!(
                "Unsupported registry schema version: {}. Required: 5.0",
                registry.schema_version
            )));
        }
        // The registry regime version must match the version this build speaks.
        // A mismatch means we resolved a manifest from the wrong version path
        // (e.g. a v0 legacy manifest reached a v1 build); fail hard rather than
        // reinterpret cap definitions across an incompatible regime.
        if registry.registry_version != crate::CARTRIDGE_REGISTRY_VERSION {
            return Err(CartridgeRepoError::ParseError(format!(
                "Unsupported cartridge registry version: {}. This build speaks v{}.",
                registry.registry_version,
                crate::CARTRIDGE_REGISTRY_VERSION
            )));
        }
        let expected_fabric_url = expected_fabric_url.as_ref();
        if registry.fabric_registry_url != expected_fabric_url {
            return Err(CartridgeRepoError::ParseError(format!(
                "Cartridge registry fabric mismatch: registry declares fabric '{}', but this \
                 client resolves caps against '{}'. Every cartridge registry a client uses must \
                 reference the same fabric so their caps are comparable.",
                registry.fabric_registry_url, expected_fabric_url
            )));
        }
        Ok(Self {
            registry,
            registry_url: registry_url.into(),
        })
    }

    /// Validate version data has all required fields
    fn validate_version_data(
        id: &str,
        version: &str,
        version_data: &CartridgeVersionData,
    ) -> Result<()> {
        if version_data.builds.is_empty() {
            return Err(CartridgeRepoError::ParseError(format!(
                "Cartridge {} v{}: no builds",
                id, version
            )));
        }
        for (i, build) in version_data.builds.iter().enumerate() {
            if build.platform.is_empty() {
                return Err(CartridgeRepoError::ParseError(format!(
                    "Cartridge {} v{}: build[{}] missing platform",
                    id, version, i
                )));
            }
            if build.packages.is_empty() {
                // A manifest published before `packages[]` existed carries
                // only the legacy singular `package`. Accept it (read as a
                // fallback by primary_package); reject only a build with no
                // installer at all. The legacy object has no `format`.
                match &build.package {
                    Some(legacy) if !legacy.name.is_empty() => {}
                    Some(_) => {
                        return Err(CartridgeRepoError::ParseError(format!(
                            "Cartridge {} v{}: build[{}] ({}) legacy package missing name",
                            id, version, i, build.platform
                        )));
                    }
                    None => {
                        return Err(CartridgeRepoError::ParseError(format!(
                            "Cartridge {} v{}: build[{}] ({}) ships no packages",
                            id, version, i, build.platform
                        )));
                    }
                }
            }
            for (j, package) in build.packages.iter().enumerate() {
                if package.name.is_empty() {
                    return Err(CartridgeRepoError::ParseError(format!(
                        "Cartridge {} v{}: build[{}] ({}) package[{}] missing name",
                        id, version, i, build.platform, j
                    )));
                }
                if package.format.is_empty() {
                    return Err(CartridgeRepoError::ParseError(format!(
                        "Cartridge {} v{}: build[{}] ({}) package[{}] ({}) missing format",
                        id, version, i, build.platform, j, package.name
                    )));
                }
            }
        }
        Ok(())
    }

    /// Compare semantic version strings
    fn compare_versions(a: &str, b: &str) -> std::cmp::Ordering {
        let parts_a: Vec<u32> = a.split('.').filter_map(|p| p.parse().ok()).collect();
        let parts_b: Vec<u32> = b.split('.').filter_map(|p| p.parse().ok()).collect();

        let max_len = parts_a.len().max(parts_b.len());

        for i in 0..max_len {
            let num_a = parts_a.get(i).copied().unwrap_or(0);
            let num_b = parts_b.get(i).copied().unwrap_or(0);

            match num_a.cmp(&num_b) {
                std::cmp::Ordering::Equal => continue,
                other => return other,
            }
        }

        std::cmp::Ordering::Equal
    }

    /// Walk both channels and emit a `(channel, id, entry)` tuple for
    /// every cartridge entry, in iteration order. Used by the
    /// transform/search/lookup helpers.
    fn iter_entries(
        &self,
    ) -> impl Iterator<Item = (CartridgeChannel, &String, &CartridgeRegistryEntry)> {
        let release = self
            .registry
            .channels
            .release
            .cartridges
            .iter()
            .map(|(id, e)| (CartridgeChannel::Release, id, e));
        let nightly = self
            .registry
            .channels
            .nightly
            .cartridges
            .iter()
            .map(|(id, e)| (CartridgeChannel::Nightly, id, e));
        release.chain(nightly)
    }

    /// Transform a single channel-entry into a flat `CartridgeInfo`. Fails
    /// hard if the entry's `latestVersion` is not present in `versions`,
    /// or if the latest version has no valid build.
    /// `registry_url` is the verbatim URL the manifest was served from
    /// — stamped onto every entry as part of identity.
    fn entry_to_cartridge_info(
        channel: CartridgeChannel,
        registry_url: &str,
        id: &str,
        entry: &CartridgeRegistryEntry,
    ) -> Result<CartridgeInfo> {
        let latest_version = &entry.latest_version;
        let version_data = entry.versions.get(latest_version).ok_or_else(|| {
            CartridgeRepoError::ParseError(format!(
                "Cartridge {} ({}): latestVersion {} not found in versions",
                id, channel, latest_version
            ))
        })?;
        Self::validate_version_data(id, latest_version, version_data)?;

        let mut available_versions: Vec<String> = entry.versions.keys().cloned().collect();
        available_versions.sort_by(|a, b| Self::compare_versions(b, a));

        Ok(CartridgeInfo {
            id: id.to_string(),
            name: entry.name.clone(),
            version: latest_version.clone(),
            description: entry.description.clone(),
            author: entry.author.clone(),
            team_id: entry.team_id.clone(),
            signed_at: version_data.release_date.clone(),
            min_app_version: if !version_data.min_app_version.is_empty() {
                version_data.min_app_version.clone()
            } else {
                entry.min_app_version.clone()
            },
            page_url: entry.page_url.clone(),
            categories: entry.categories.clone(),
            tags: entry.tags.clone(),
            cap_groups: entry.cap_groups.clone(),
            versions: entry.versions.clone(),
            available_versions,
            channel,
            registry_url: registry_url.to_string(),
        })
    }

    /// Transform the registry to a flat array of `CartridgeInfo`,
    /// preserving (registry_url, channel) provenance on every entry.
    pub fn transform_to_cartridge_array(&self) -> Result<Vec<CartridgeInfo>> {
        let mut result = Vec::new();
        for (channel, id, entry) in self.iter_entries() {
            result.push(Self::entry_to_cartridge_info(
                channel,
                &self.registry_url,
                id,
                entry,
            )?);
        }
        Ok(result)
    }

    /// Get all cartridges (API response format) — both channels.
    pub fn get_cartridges(&self) -> Result<CartridgeRegistryResponse> {
        let cartridges = self.transform_to_cartridge_array()?;
        Ok(CartridgeRegistryResponse { cartridges })
    }

    /// Get cartridge by `(channel, id)`. Channel is required because
    /// the same id can independently exist in both channels.
    pub fn get_cartridge_by_id(
        &self,
        channel: CartridgeChannel,
        id: &str,
    ) -> Result<Option<CartridgeInfo>> {
        let entries = match channel {
            CartridgeChannel::Release => &self.registry.channels.release.cartridges,
            CartridgeChannel::Nightly => &self.registry.channels.nightly.cartridges,
        };
        match entries.get(id) {
            None => Ok(None),
            Some(entry) => {
                Self::entry_to_cartridge_info(channel, &self.registry_url, id, entry).map(Some)
            }
        }
    }

    /// Search cartridges by free-text query across both channels.
    ///
    /// Matches the query against cartridge name, description, tags, and
    /// cap titles. Cap URNs themselves are NOT substring-matched: a cap
    /// URN is a tagged identifier, and substring matching against it is
    /// a category error. Use `get_cartridges_by_cap` to look up cartridges
    /// that provide a specific cap.
    pub fn search_cartridges(&self, query: &str) -> Result<Vec<CartridgeInfo>> {
        let all = self.transform_to_cartridge_array()?;
        let lower_query = query.to_lowercase();

        Ok(all
            .into_iter()
            .filter(|p| {
                p.name.to_lowercase().contains(&lower_query)
                    || p.description.to_lowercase().contains(&lower_query)
                    || p.tags
                        .iter()
                        .any(|t| t.to_lowercase().contains(&lower_query))
                    || p.iter_caps()
                        .any(|c| c.title.to_lowercase().contains(&lower_query))
            })
            .collect())
    }

    /// Get cartridges by category — both channels.
    pub fn get_cartridges_by_category(&self, category: &str) -> Result<Vec<CartridgeInfo>> {
        let all = self.transform_to_cartridge_array()?;
        Ok(all
            .into_iter()
            .filter(|p| p.categories.contains(&category.to_string()))
            .collect())
    }

    /// Get every cartridge that could HANDLE a cap request — the
    /// "find anything that would match" query, distinct from exact
    /// resolution (see [`Self::get_suggestions_for_cap`]).
    ///
    /// The requested URN is parsed via `CapUrn::from_string`; each
    /// declared cartridge cap is parsed too and matched via
    /// `is_dispatchable`, the dispatch predicate — "can this declared
    /// candidate cap legally handle the requested pattern?" (input
    /// contravariant, output covariant, tags invariant; see
    /// ../docs/07-dispatch.md). This is deliberately LOOSER than
    /// the equivalence used to resolve an alias to its exact candidate:
    /// here we enumerate everything capable, not the one exact match.
    /// The `op` tag has no functional role in matching — only the
    /// parsed predicate machinery is used, never string comparison. A
    /// malformed input URN is a `ParseError`; a malformed declared URN
    /// in the registry is also propagated rather than silently dropped.
    pub fn get_cartridges_by_cap(&self, cap_urn: &str) -> Result<Vec<CartridgeInfo>> {
        use crate::urn::cap_urn::CapUrn;
        let requested = CapUrn::from_string(cap_urn).map_err(|e| {
            CartridgeRepoError::ParseError(format!(
                "get_cartridges_by_cap: invalid cap URN '{}': {}",
                cap_urn, e
            ))
        })?;
        let all = self.transform_to_cartridge_array()?;
        let mut matched = Vec::new();
        for cart in all {
            for cap in cart.iter_caps() {
                let declared = CapUrn::from_string(&cap.urn).map_err(|e| {
                    CartridgeRepoError::ParseError(format!(
                        "cartridge {} ({}): invalid declared cap URN '{}': {}",
                        cart.id, cart.channel, cap.urn, e
                    ))
                })?;
                if declared.is_dispatchable(&requested) {
                    matched.push(cart.clone());
                    break;
                }
            }
        }
        Ok(matched)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The fabric URL every test fixture declares and every test
    /// `CartridgeRepoServer::new` / `CartridgeRepo::new` expects — so the
    /// fabric-match check passes and the OTHER behaviour under test is exercised.
    const TEST_FABRIC_URL: &str = "https://fabric.test";

    // ----------------------------------------------------------------------
    // Fixture builders shared by the cap_groups test suite.
    // ----------------------------------------------------------------------

    fn build_version_data(pkg_name: &str) -> CartridgeVersionData {
        CartridgeVersionData {
            release_date: "2026-02-07".to_string(),
            changelog: vec![],
            min_app_version: String::new(),
            builds: vec![CartridgeBuild {
                platform: "darwin-arm64".to_string(),
                packages: vec![CartridgeDistributionInfo {
                    name: pkg_name.to_string(),
                    sha256: "abc123".to_string(),
                    size: 1000,
                    url: format!("https://cartridges.machinefabric.com/{}", pkg_name),
                    format: "pkg".to_string(),
                }],
                package: None,
                binary: None,
            }],
            notes_url: None,
        }
    }

    fn build_cap(urn: &str, title: &str, command: &str) -> RegistryCap {
        RegistryCap {
            urn: urn.to_string(),
            title: title.to_string(),
            aliases: vec![command.to_string()],
            is_abstract: false,
            cap_description: None,
            args: None,
            output: None,
        }
    }

    fn build_cap_group(
        name: &str,
        caps: Vec<RegistryCap>,
        adapter_urns: Vec<String>,
    ) -> RegistryCapGroup {
        RegistryCapGroup {
            name: name.to_string(),
            caps,
            adapter_urns,
        }
    }

    fn build_cartridge_info(
        id: &str,
        name: &str,
        cap_groups: Vec<RegistryCapGroup>,
    ) -> CartridgeInfo {
        build_cartridge_info_in(CartridgeChannel::Release, id, name, cap_groups)
    }

    fn build_cartridge_info_in(
        channel: CartridgeChannel,
        id: &str,
        name: &str,
        cap_groups: Vec<RegistryCapGroup>,
    ) -> CartridgeInfo {
        let pkg = format!("{}-1.0.0.pkg", id);
        let mut versions = HashMap::new();
        versions.insert("1.0.0".to_string(), build_version_data(&pkg));
        CartridgeInfo {
            id: id.to_string(),
            name: name.to_string(),
            version: "1.0.0".to_string(),
            description: String::new(),
            author: String::new(),
            team_id: "TEAM123".to_string(),
            signed_at: "2026-02-07T00:00:00Z".to_string(),
            min_app_version: String::new(),
            page_url: String::new(),
            categories: vec![],
            tags: vec![],
            cap_groups,
            versions,
            available_versions: vec!["1.0.0".to_string()],
            channel,
            // Default test fixture lives at a fake URL; tests that
            // care about (registry_url, channel, id) tuple distinctness
            // use a different helper or override this field directly.
            registry_url: "https://example.com/cartridges".to_string(),
        }
    }

    fn build_registry_entry(
        name: &str,
        cap_groups: Vec<RegistryCapGroup>,
    ) -> CartridgeRegistryEntry {
        let mut versions = HashMap::new();
        versions.insert("1.0.0".to_string(), build_version_data("entry-1.0.0.pkg"));
        CartridgeRegistryEntry {
            name: name.to_string(),
            description: format!("{} description", name),
            author: "Test Author".to_string(),
            page_url: String::new(),
            team_id: "TEAM123".to_string(),
            min_app_version: String::new(),
            cap_groups,
            categories: vec![],
            tags: vec![],
            latest_version: "1.0.0".to_string(),
            versions,
        }
    }

    // ----------------------------------------------------------------------
    // Empty-state tests.
    // ----------------------------------------------------------------------

    // TEST630: CartridgeRepo creation starts with empty cartridge list.
    #[tokio::test]
    async fn test630_cartridge_repo_creation() {
        let repo = CartridgeRepo::new(3600, TEST_FABRIC_URL);
        assert!(repo.get_all_cartridges().await.is_empty());
    }

    // TEST631: needs_sync returns true with empty cache and non-empty URLs.
    #[tokio::test]
    async fn test631_needs_sync_empty_cache() {
        let repo = CartridgeRepo::new(3600, TEST_FABRIC_URL);
        let urls = vec!["https://example.com/cartridges".to_string()];
        assert!(repo.needs_sync(&urls).await);
    }

    // ----------------------------------------------------------------------
    // Wire-shape deserialization. These bind the struct definitions to the
    // exact JSON the registry function returns so a server-side schema
    // change shows up here as a parse failure.
    // ----------------------------------------------------------------------

    // TEST632: A registry cap with only the three required fields parses.
    #[test]
    fn test632_deserialize_minimal_registry_cap() {
        let json = r#"{"urn": "cap:effect=none", "title": "Identity", "aliases": ["identity"]}"#;
        let cap: RegistryCap = serde_json::from_str(json).unwrap();
        assert_eq!(cap.urn, "cap:effect=none");
        assert_eq!(cap.title, "Identity");
        assert_eq!(cap.aliases, vec!["identity".to_string()]);
        assert!(cap.cap_description.is_none());
        assert!(cap.args.is_none());
        assert!(cap.output.is_none());
    }

    // TEST633: A registry cap with cap_description, args, output all parses.
    #[test]
    fn test633_deserialize_rich_registry_cap() {
        let json = r#"{
            "urn": "cap:in=\"media:ext=pdf\";disbind;out=\"media:enc=utf-8;page\"",
            "title": "Disbind PDF",
            "aliases": ["disbind"],
            "cap_description": "Extract each PDF page as plain page text.",
            "args": [
                {
                    "media_urn": "media:enc=utf-8;file-path",
                    "required": true,
                    "is_sequence": false,
                    "sources": [{"stdin": "media:ext=pdf"}, {"position": 0}],
                    "arg_description": "Path to the PDF file to process"
                }
            ],
            "output": {
                "media_urn": "media:enc=utf-8;page",
                "is_sequence": true,
                "output_description": "One page text per PDF page"
            }
        }"#;
        let cap: RegistryCap = serde_json::from_str(json).unwrap();
        assert_eq!(cap.aliases, vec!["disbind".to_string()]);
        assert_eq!(
            cap.cap_description.as_deref(),
            Some("Extract each PDF page as plain page text.")
        );
        let args = cap.args.unwrap();
        assert_eq!(args.len(), 1);
        assert_eq!(args[0].media_urn, "media:enc=utf-8;file-path");
        assert_eq!(args[0].sources[0].stdin.as_deref(), Some("media:ext=pdf"));
        assert_eq!(args[0].sources[1].position, Some(0));
        let output = cap.output.unwrap();
        assert_eq!(output.media_urn, "media:enc=utf-8;page");
        assert!(output.is_sequence);
    }

    // TEST634: A registry cap_group parses with caps + adapter_urns.
    #[test]
    fn test634_deserialize_cap_group() {
        let json = r#"{
            "name": "pdf-formats",
            "caps": [
                {"urn": "cap:effect=none", "title": "Identity", "aliases": ["identity"]}
            ],
            "adapter_urns": ["media:ext=pdf"]
        }"#;
        let group: RegistryCapGroup = serde_json::from_str(json).unwrap();
        assert_eq!(group.name, "pdf-formats");
        assert_eq!(group.caps.len(), 1);
        assert_eq!(group.adapter_urns, vec!["media:ext=pdf".to_string()]);
    }

    // TEST635: CartridgeInfo deserializes the wire shape exactly as
    // returned by /api/cartridges (camelCase top-level + snake_case
    // cap_groups). Null camelCase string fields fall back to empty.
    #[test]
    fn test635_deserialize_cartridge_info_wire_shape() {
        let json = r#"{
            "id": "pdfcartridge",
            "name": "pdfcartridge",
            "version": "0.179.441",
            "description": "PDF page renderer",
            "author": "https://github.com/machinefabric",
            "pageUrl": "https://github.com/machinefabric/pdfcartridge",
            "teamId": "P336JK947M",
            "signedAt": "2026-04-25T14:53:55Z",
            "minAppVersion": "1.0.0",
            "cap_groups": [
                {
                    "name": "pdf-formats",
                    "caps": [
                        {"urn": "cap:effect=none", "title": "Identity", "aliases": ["identity"]},
                        {"urn": "cap:in=\"media:ext=pdf\";disbind;out=\"media:enc=utf-8;page\"", "title": "Disbind PDF Into Page Text", "aliases": ["disbind"]}
                    ],
                    "adapter_urns": ["media:ext=pdf"]
                }
            ],
            "categories": [],
            "tags": [],
            "versions": {},
            "availableVersions": [],
            "channel": "release",
            "registryUrl": "https://test.example/manifest"
        }"#;
        let cartridge: CartridgeInfo = serde_json::from_str(json).unwrap();
        assert_eq!(cartridge.id, "pdfcartridge");
        assert_eq!(cartridge.team_id, "P336JK947M");
        assert_eq!(cartridge.cap_groups.len(), 1);
        assert_eq!(cartridge.cap_groups[0].caps.len(), 2);
        assert_eq!(cartridge.iter_caps().count(), 2);
        assert_eq!(cartridge.channel, CartridgeChannel::Release);
        assert_eq!(cartridge.registry_url, "https://test.example/manifest");
    }

    // TEST636: CartridgeInfo with null version/description/author still
    // deserializes (the null_as_empty_string deserializer is the only
    // tolerated coercion — every other malformed input is a hard error).
    #[test]
    fn test636_deserialize_cartridge_info_with_null_strings() {
        let json = r#"{
            "id": "mlxcartridge",
            "name": "MLX Cartridge",
            "version": null,
            "description": null,
            "author": null,
            "cap_groups": [],
            "versions": {},
            "channel": "nightly",
            "registryUrl": "https://test.example/manifest"
        }"#;
        let cartridge: CartridgeInfo = serde_json::from_str(json).unwrap();
        assert_eq!(cartridge.version, "");
        assert_eq!(cartridge.description, "");
        assert_eq!(cartridge.author, "");
        assert!(cartridge.cap_groups.is_empty());
    }

    // TEST637: A full /api/cartridges-shaped response with two cartridges
    // and nested cap_groups round-trips through the response wrapper.
    #[test]
    fn test637_deserialize_full_registry_response() {
        let json = r#"{
            "cartridges": [
                {
                    "id": "pdfcartridge",
                    "name": "pdfcartridge",
                    "version": "0.179.441",
                    "description": "PDF",
                    "author": "https://github.com/machinefabric",
                    "pageUrl": "",
                    "teamId": "P336JK947M",
                    "signedAt": "2026-04-25T14:53:55Z",
                    "minAppVersion": "1.0.0",
                    "cap_groups": [
                        {
                            "name": "pdf-formats",
                            "caps": [
                                {"urn": "cap:effect=none", "title": "Identity", "aliases": ["identity"]}
                            ],
                            "adapter_urns": ["media:ext=pdf"]
                        }
                    ],
                    "categories": [],
                    "tags": [],
                    "versions": {},
                    "availableVersions": [],
                    "channel": "release",
                    "registryUrl": "https://test.example/manifest"
                },
                {
                    "id": "imagecartridge",
                    "name": "imagecartridge",
                    "version": "0.1.6",
                    "description": "image",
                    "author": "",
                    "teamId": "P336JK947M",
                    "signedAt": "2026-04-25T21:53:45Z",
                    "minAppVersion": "1.0.0",
                    "cap_groups": [
                        {
                            "name": "image-formats",
                            "caps": [
                                {"urn": "cap:in=\"media:convert-image;image;jpeg;png\";out=\"media:image\"", "title": "Convert JPEG to PNG", "aliases": ["convert-image"]}
                            ],
                            "adapter_urns": ["media:ext=bmp;image", "media:ext=jpeg;image", "media:ext=png;image", "media:ext=tiff;image", "media:ext=webp;image", "media:ext=gif;image"]
                        }
                    ],
                    "categories": [],
                    "tags": [],
                    "versions": {},
                    "availableVersions": [],
                    "channel": "nightly",
                    "registryUrl": "https://test.example/manifest"
                }
            ],
            "total": 2,
            "page": 1,
            "limit": 20,
            "totalPages": 1
        }"#;
        let response: CartridgeRegistryResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.cartridges.len(), 2);
        let img = response
            .cartridges
            .iter()
            .find(|c| c.id == "imagecartridge")
            .unwrap();
        assert_eq!(img.cap_groups.len(), 1);
        assert_eq!(img.cap_groups[0].adapter_urns.len(), 6);
    }

    // TEST1847: A build from a registry manifest published BEFORE `packages[]`
    // existed carries only the legacy singular `package` (no `format`). It must
    // still deserialize (a missing `packages` must not fail the whole parse) and
    // `primary_package()` must fall back to that legacy package, so a registry
    // not yet republished with the dual-write keeps installing. When `packages[]`
    // is present it is preferred over the legacy field.
    #[test]
    fn test1847_cartridge_build_legacy_package_fallback() {
        // Legacy-only: `package`, no `packages`.
        let legacy_json = r#"{
            "platform": "linux-x86_64",
            "package": {
                "name": "imagecartridge-1.0.0.pkg",
                "url": "https://cartridges.machinefabric.com/imagecartridge-1.0.0.pkg",
                "sha256": "abc123",
                "size": 1000
            }
        }"#;
        let legacy: CartridgeBuild = serde_json::from_str(legacy_json).unwrap();
        assert!(legacy.packages.is_empty());
        let primary = legacy
            .primary_package()
            .expect("legacy package must be read as a fallback");
        assert_eq!(primary.name, "imagecartridge-1.0.0.pkg");
        assert_eq!(primary.format, ""); // legacy object has no format
        assert!(primary.url.ends_with("imagecartridge-1.0.0.pkg"));

        // packages[] present: preferred over the legacy field, native format wins.
        let modern_json = r#"{
            "platform": "linux-x86_64",
            "package": {
                "name": "legacy.pkg", "url": "https://x/legacy.pkg",
                "sha256": "dead", "size": 1
            },
            "packages": [
                {"name": "c.rpm", "url": "https://x/c.rpm", "sha256": "a", "size": 2, "format": "rpm"},
                {"name": "c.deb", "url": "https://x/c.deb", "sha256": "b", "size": 3, "format": "deb"}
            ]
        }"#;
        let modern: CartridgeBuild = serde_json::from_str(modern_json).unwrap();
        // linux prefers deb over rpm; the legacy `package` is ignored.
        assert_eq!(modern.primary_package().unwrap().name, "c.deb");
    }

    // TEST8032: the `binary` field (signed pure-binary artifact) round-trips
    // through serde; a build without it — every pre-signing manifest —
    // deserializes to `None` and serializes without the key (so a manifest
    // rewritten by new tooling doesn't grow a null field old validators
    // would reject).
    #[test]
    fn test8032_cartridge_build_binary_field_roundtrip() {
        let with_binary_json = r#"{
            "platform": "linux-x86_64",
            "package": {
                "name": "txtcartridge-1.0.0-linux-x86_64.deb",
                "url": "https://x/txtcartridge-1.0.0-linux-x86_64.deb",
                "sha256": "aa", "size": 10
            },
            "packages": [
                {"name": "txtcartridge-1.0.0-linux-x86_64.deb", "url": "https://x/d.deb", "sha256": "aa", "size": 10, "format": "deb"}
            ],
            "binary": {
                "name": "txtcartridge-1.0.0-linux-x86_64.bin",
                "url": "https://x/txtcartridge-1.0.0-linux-x86_64.bin",
                "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                "size": 123456,
                "signature": "untrusted comment: signature from minisign secret key\nRUQ...\ntrusted comment: file:txtcartridge-1.0.0-linux-x86_64.bin sha256=01...\nAAA...\n"
            }
        }"#;
        let build: CartridgeBuild = serde_json::from_str(with_binary_json).unwrap();
        let binary = build.binary.as_ref().expect("binary must deserialize");
        assert_eq!(binary.name, "txtcartridge-1.0.0-linux-x86_64.bin");
        assert_eq!(binary.size, 123456);
        assert!(binary.signature.starts_with("untrusted comment: "));
        // Round-trip preserves the binary object.
        let reserialized = serde_json::to_string(&build).unwrap();
        let reparsed: CartridgeBuild = serde_json::from_str(&reserialized).unwrap();
        assert_eq!(reparsed.binary.as_ref().unwrap().sha256, binary.sha256);

        // Absent on the wire → None, and stays absent when re-serialized.
        let without_json = r#"{
            "platform": "linux-x86_64",
            "package": {"name": "l.deb", "url": "https://x/l.deb", "sha256": "aa", "size": 1},
            "packages": [{"name": "l.deb", "url": "https://x/l.deb", "sha256": "aa", "size": 1, "format": "deb"}]
        }"#;
        let without: CartridgeBuild = serde_json::from_str(without_json).unwrap();
        assert!(without.binary.is_none());
        let reserialized = serde_json::to_string(&without).unwrap();
        assert!(
            !reserialized.contains("\"binary\""),
            "absent binary must not serialize as null: {reserialized}"
        );
    }

    // ----------------------------------------------------------------------
    // CartridgeInfo behaviour.
    // ----------------------------------------------------------------------

    // TEST320: Construct CartridgeInfo and verify round-trip of fields.
    #[test]
    fn test320_cartridge_info_construction() {
        let group = build_cap_group(
            "test-group",
            vec![build_cap("cap:effect=none", "Identity", "identity")],
            vec![],
        );
        let cartridge = build_cartridge_info("testcartridge", "Test Cartridge", vec![group]);
        assert_eq!(cartridge.id, "testcartridge");
        assert_eq!(cartridge.cap_groups.len(), 1);
        assert_eq!(cartridge.iter_caps().count(), 1);
    }

    // TEST321: CartridgeInfo.is_signed() returns true when signature
    // (team_id + signed_at) is present, false when either is empty.
    #[test]
    fn test321_cartridge_info_is_signed() {
        let mut cartridge = build_cartridge_info("testcartridge", "Test", vec![]);
        assert!(cartridge.is_signed());

        cartridge.team_id = String::new();
        assert!(!cartridge.is_signed());

        cartridge.team_id = "TEAM123".to_string();
        cartridge.signed_at = String::new();
        assert!(!cartridge.is_signed());
    }

    // TEST322: CartridgeInfo.build_for_platform() returns the build that
    // matches the requested platform string and None otherwise.
    #[test]
    fn test322_cartridge_info_build_for_platform() {
        let cartridge = build_cartridge_info("testcartridge", "Test", vec![]);

        let build = cartridge.build_for_platform("darwin-arm64");
        assert!(build.is_some());
        assert_eq!(
            build.unwrap().primary_package().unwrap().name,
            "testcartridge-1.0.0.pkg"
        );

        let no_build = cartridge.build_for_platform("linux-x86_64");
        assert!(no_build.is_none());

        let mut empty_cartridge = build_cartridge_info("empty", "Empty", vec![]);
        empty_cartridge.versions = HashMap::new();
        assert!(empty_cartridge.build_for_platform("darwin-arm64").is_none());
    }

    // ----------------------------------------------------------------------
    // Host-compatibility resolution (resolve_for_host).
    // ----------------------------------------------------------------------

    /// One platform build carrying a single native-format package, so a
    /// resolution test can assert exactly which package URL the host gets.
    fn build_for_platform_with_format(
        platform: &str,
        format: &str,
        pkg_name: &str,
    ) -> CartridgeBuild {
        CartridgeBuild {
            platform: platform.to_string(),
            packages: vec![CartridgeDistributionInfo {
                name: pkg_name.to_string(),
                sha256: "deadbeef".to_string(),
                size: 4242,
                url: format!("https://cartridges.machinefabric.com/{pkg_name}"),
                format: format.to_string(),
            }],
            package: None,
            binary: None,
        }
    }

    /// Construct a cartridge whose versions/platform-builds are fully
    /// specified by the caller. `versions` is given newest-first; `version`
    /// (the "latest" field) is set to the first entry. Each version lists the
    /// (platform, format, pkg_name) builds it ships.
    fn cartridge_with_versions(
        id: &str,
        versions: Vec<(&str, Vec<(&str, &str, &str)>)>,
    ) -> CartridgeInfo {
        let mut version_map = HashMap::new();
        let mut available = Vec::new();
        for (ver, builds) in &versions {
            available.push(ver.to_string());
            version_map.insert(
                ver.to_string(),
                CartridgeVersionData {
                    release_date: "2026-02-07".to_string(),
                    changelog: vec![],
                    min_app_version: String::new(),
                    builds: builds
                        .iter()
                        .map(|(plat, fmt, name)| build_for_platform_with_format(plat, fmt, name))
                        .collect(),
                    notes_url: None,
                },
            );
        }
        let latest = versions
            .first()
            .map(|(v, _)| v.to_string())
            .unwrap_or_default();
        CartridgeInfo {
            id: id.to_string(),
            name: id.to_string(),
            version: latest,
            description: String::new(),
            author: String::new(),
            team_id: "TEAM123".to_string(),
            signed_at: "2026-02-07T00:00:00Z".to_string(),
            min_app_version: String::new(),
            page_url: String::new(),
            categories: vec![],
            tags: vec![],
            cap_groups: vec![],
            versions: version_map,
            available_versions: available,
            channel: CartridgeChannel::Release,
            registry_url: "https://example.com/cartridges".to_string(),
        }
    }

    // TEST1849: latest version has a host build → Compatible, resolving to the
    // latest version and that platform's native-format package.
    #[test]
    fn test1849_resolve_for_host_compatible_latest() {
        let cartridge = cartridge_with_versions(
            "c",
            vec![
                (
                    "1.2.0",
                    vec![
                        ("darwin-arm64", "pkg", "c-1.2.0.pkg"),
                        ("linux-x86_64", "deb", "c-1.2.0.deb"),
                    ],
                ),
                ("1.1.0", vec![("darwin-arm64", "pkg", "c-1.1.0.pkg")]),
            ],
        );

        let r = cartridge.resolve_for_host("linux-x86_64");
        assert_eq!(r.status, CompatStatus::Compatible);
        assert_eq!(r.resolved_version.as_deref(), Some("1.2.0"));
        assert_eq!(r.resolved_package.as_ref().unwrap().name, "c-1.2.0.deb");
        assert_eq!(r.resolved_package.as_ref().unwrap().format, "deb");
        assert!(r.reason.is_none(), "Compatible carries no reason");
        assert_eq!(r.host_platform, "linux-x86_64");
    }

    // TEST1850: the latest version lacks a host build but an older version has
    // one → CompatibleOutdated, resolving to the older version with a reason
    // naming both the latest and the resolved version.
    #[test]
    fn test1850_resolve_for_host_compatible_outdated() {
        let cartridge = cartridge_with_versions(
            "c",
            vec![
                // Latest 1.3.0 ships only macOS.
                ("1.3.0", vec![("darwin-arm64", "pkg", "c-1.3.0.pkg")]),
                // 1.2.0 still shipped Linux.
                (
                    "1.2.0",
                    vec![
                        ("darwin-arm64", "pkg", "c-1.2.0.pkg"),
                        ("linux-x86_64", "deb", "c-1.2.0.deb"),
                    ],
                ),
                ("1.1.0", vec![("linux-x86_64", "deb", "c-1.1.0.deb")]),
            ],
        );

        let r = cartridge.resolve_for_host("linux-x86_64");
        assert_eq!(r.status, CompatStatus::CompatibleOutdated);
        // Newest-with-host-build is 1.2.0, NOT the oldest 1.1.0 that also has it.
        assert_eq!(r.resolved_version.as_deref(), Some("1.2.0"));
        assert_eq!(r.resolved_package.as_ref().unwrap().name, "c-1.2.0.deb");
        let reason = r.reason.expect("outdated carries a reason");
        assert!(
            reason.contains("1.3.0"),
            "reason names the latest: {reason}"
        );
        assert!(
            reason.contains("1.2.0"),
            "reason names the resolved: {reason}"
        );
    }

    // TEST1851: no version ships a host build → Incompatible, no resolved
    // version/package, reason states the host platform.
    #[test]
    fn test1851_resolve_for_host_incompatible() {
        let cartridge = cartridge_with_versions(
            "c",
            vec![
                ("1.2.0", vec![("darwin-arm64", "pkg", "c-1.2.0.pkg")]),
                ("1.1.0", vec![("darwin-arm64", "pkg", "c-1.1.0.pkg")]),
            ],
        );

        let r = cartridge.resolve_for_host("windows-x86_64");
        assert_eq!(r.status, CompatStatus::Incompatible);
        assert!(r.resolved_version.is_none());
        assert!(r.resolved_package.is_none());
        assert!(r.reason.as_deref().unwrap().contains("windows-x86_64"));
    }

    // TEST1852: a host build whose packages[] is empty AND has no legacy
    // `package` ships no installer; resolution must SKIP it (not resolve to an
    // un-downloadable version) and fall through to an older usable version.
    #[test]
    fn test1852_resolve_for_host_skips_build_with_no_installer() {
        let mut cartridge = cartridge_with_versions(
            "c",
            vec![
                // Latest has a linux build entry but we strip its installer below.
                ("2.0.0", vec![("linux-x86_64", "deb", "c-2.0.0.deb")]),
                ("1.0.0", vec![("linux-x86_64", "deb", "c-1.0.0.deb")]),
            ],
        );
        // Make 2.0.0's linux build ship nothing installable.
        let v2 = cartridge.versions.get_mut("2.0.0").unwrap();
        v2.builds[0].packages.clear();
        v2.builds[0].package = None;

        let r = cartridge.resolve_for_host("linux-x86_64");
        // 2.0.0 is skipped (no installer); newest USABLE host build is 1.0.0.
        assert_eq!(r.status, CompatStatus::CompatibleOutdated);
        assert_eq!(r.resolved_version.as_deref(), Some("1.0.0"));
        assert_eq!(r.resolved_package.as_ref().unwrap().name, "c-1.0.0.deb");
    }

    // TEST1853: host_platform() returns a normalized {os}-{arch} string with
    // arch aarch64 mapped to arm64 — the exact form the registry uses.
    #[test]
    fn test1853_host_platform_normalized_form() {
        let p = host_platform();
        let (os, arch) = p
            .split_once('-')
            .unwrap_or_else(|| panic!("host_platform must be os-arch, got {p}"));
        assert!(
            matches!(os, "darwin" | "linux" | "windows") || !os.is_empty(),
            "os segment present: {os}"
        );
        // The registry never uses the raw "aarch64"; it must be normalized.
        assert_ne!(arch, "aarch64", "arch must be normalized to arm64");
    }

    // ----------------------------------------------------------------------
    // CartridgeRepoServer end-to-end tests on the v4.0 nested registry.
    // ----------------------------------------------------------------------

    /// Build a v5.0 registry placing every entry under the `release`
    /// channel. The `nightly` channel is left empty — tests that need a
    /// mixed-channel state use `build_registry_in_channels` directly.
    fn build_registry(entries: Vec<(&str, CartridgeRegistryEntry)>) -> CartridgeRegistry {
        build_registry_in_channels(entries, vec![])
    }

    fn build_registry_in_channels(
        release: Vec<(&str, CartridgeRegistryEntry)>,
        nightly: Vec<(&str, CartridgeRegistryEntry)>,
    ) -> CartridgeRegistry {
        let mut release_map = HashMap::new();
        for (id, entry) in release {
            release_map.insert(id.to_string(), entry);
        }
        let mut nightly_map = HashMap::new();
        for (id, entry) in nightly {
            nightly_map.insert(id.to_string(), entry);
        }
        CartridgeRegistry {
            schema_version: "5.0".to_string(),
            registry_version: crate::CARTRIDGE_REGISTRY_VERSION,
            fabric_registry_url: TEST_FABRIC_URL.to_string(),
            last_updated: "2026-02-07".to_string(),
            // Test fixture URL — matches the `https://test.example/manifest`
            // used by the test calls to CartridgeRepoServer::new so the
            // self-referential check downstream stays consistent.
            registry_url: "https://test.example/manifest".to_string(),
            channels: CartridgeRegistryChannels {
                release: CartridgeChannelEntries {
                    cartridges: release_map,
                },
                nightly: CartridgeChannelEntries {
                    cartridges: nightly_map,
                },
            },
        }
    }

    // TEST323: CartridgeRepoServer requires schema 5.0 and rejects older.
    #[test]
    fn test323_cartridge_repo_server_validate_registry() {
        let server = CartridgeRepoServer::new(
            build_registry(vec![]),
            "https://test.example/manifest",
            TEST_FABRIC_URL,
        );
        assert!(server.is_ok());

        let mut bad = build_registry(vec![]);
        bad.schema_version = "4.0".to_string();
        let result =
            CartridgeRepoServer::new(bad, "https://test.example/manifest", TEST_FABRIC_URL);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("5.0"));

        // A registry from a different regime version (e.g. a v0 legacy manifest
        // that somehow reached this v>=1 build) must be rejected, not silently
        // reinterpreted across an incompatible cap-shape regime.
        let mut wrong_version = build_registry(vec![]);
        wrong_version.registry_version = crate::CARTRIDGE_REGISTRY_VERSION + 1;
        let result = CartridgeRepoServer::new(
            wrong_version,
            "https://test.example/manifest",
            TEST_FABRIC_URL,
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("cartridge registry version"));

        // A registry that references a DIFFERENT fabric than this client resolves
        // caps against must be rejected — its caps are not comparable with ours.
        let ok = build_registry(vec![]);
        let result = CartridgeRepoServer::new(
            ok,
            "https://test.example/manifest",
            "https://other-fabric.test",
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("fabric"));
    }

    // TEST324: CartridgeRepoServer transforms a v4.0 entry into a flat
    // CartridgeInfo, preserving cap_groups verbatim.
    #[test]
    fn test324_cartridge_repo_server_transform_to_array() {
        let group = build_cap_group(
            "g1",
            vec![build_cap("cap:effect=none", "Identity", "identity")],
            vec!["media:test".to_string()],
        );
        let entry = build_registry_entry("Test Cartridge", vec![group]);
        let server = CartridgeRepoServer::new(
            build_registry(vec![("testcartridge", entry)]),
            "https://test.example/manifest",
            TEST_FABRIC_URL,
        )
        .unwrap();

        let array = server.transform_to_cartridge_array().unwrap();
        assert_eq!(array.len(), 1);
        assert_eq!(array[0].id, "testcartridge");
        assert_eq!(array[0].cap_groups.len(), 1);
        assert_eq!(
            array[0].cap_groups[0].adapter_urns,
            vec!["media:test".to_string()]
        );
        assert_eq!(array[0].iter_caps().count(), 1);
    }

    // TEST325: get_cartridges() wraps the transformed array in the
    // response envelope.
    #[test]
    fn test325_cartridge_repo_server_get_cartridges() {
        let entry = build_registry_entry(
            "Test Cartridge",
            vec![build_cap_group(
                "g",
                vec![build_cap("cap:effect=none", "Identity", "identity")],
                vec![],
            )],
        );
        let server = CartridgeRepoServer::new(
            build_registry(vec![("testcartridge", entry)]),
            "https://test.example/manifest",
            TEST_FABRIC_URL,
        )
        .unwrap();
        let response = server.get_cartridges().unwrap();
        assert_eq!(response.cartridges.len(), 1);
        assert_eq!(response.cartridges[0].id, "testcartridge");
    }

    // TEST326: get_cartridge_by_id requires a channel and returns Some
    // for a known (channel, id), None otherwise. The same id looked up
    // in the wrong channel must miss — channels are independent
    // namespaces.
    #[test]
    fn test326_cartridge_repo_server_get_cartridge_by_id() {
        let entry = build_registry_entry(
            "Test Cartridge",
            vec![build_cap_group(
                "g",
                vec![build_cap("cap:effect=none", "Identity", "identity")],
                vec![],
            )],
        );
        let server = CartridgeRepoServer::new(
            build_registry(vec![("testcartridge", entry)]),
            "https://test.example/manifest",
            TEST_FABRIC_URL,
        )
        .unwrap();
        assert!(server
            .get_cartridge_by_id(CartridgeChannel::Release, "testcartridge")
            .unwrap()
            .is_some());
        assert!(server
            .get_cartridge_by_id(CartridgeChannel::Release, "nonexistent")
            .unwrap()
            .is_none());
        // Looked up in the wrong channel — id exists only in release
        // (build_registry's default channel) but the nightly side is
        // empty. Channel partitioning is the whole point.
        assert!(server
            .get_cartridge_by_id(CartridgeChannel::Nightly, "testcartridge")
            .unwrap()
            .is_none());
    }

    // TEST300: A cartridge with the same id can independently exist in
    // both channels. Each lookup must return the channel-specific entry.
    #[test]
    fn test300_get_cartridge_by_id_channel_isolation() {
        let mut release_entry = build_registry_entry(
            "Foo (release)",
            vec![build_cap_group(
                "g",
                vec![build_cap("cap:effect=none", "Identity", "identity")],
                vec![],
            )],
        );
        release_entry.versions.clear();
        release_entry
            .versions
            .insert("1.0.0".to_string(), build_version_data("foo-1.0.0.pkg"));
        release_entry.latest_version = "1.0.0".to_string();

        let mut nightly_entry = build_registry_entry(
            "Foo (nightly)",
            vec![build_cap_group(
                "g",
                vec![build_cap("cap:effect=none", "Identity", "identity")],
                vec![],
            )],
        );
        nightly_entry.versions.clear();
        nightly_entry
            .versions
            .insert("2.0.0".to_string(), build_version_data("foo-2.0.0.pkg"));
        nightly_entry.latest_version = "2.0.0".to_string();

        let registry = build_registry_in_channels(
            vec![("foocartridge", release_entry)],
            vec![("foocartridge", nightly_entry)],
        );
        let server =
            CartridgeRepoServer::new(registry, "https://test.example/manifest", TEST_FABRIC_URL)
                .unwrap();

        let r = server
            .get_cartridge_by_id(CartridgeChannel::Release, "foocartridge")
            .unwrap()
            .unwrap();
        assert_eq!(r.name, "Foo (release)");
        assert_eq!(r.version, "1.0.0");
        assert_eq!(r.channel, CartridgeChannel::Release);

        let n = server
            .get_cartridge_by_id(CartridgeChannel::Nightly, "foocartridge")
            .unwrap()
            .unwrap();
        assert_eq!(n.name, "Foo (nightly)");
        assert_eq!(n.version, "2.0.0");
        assert_eq!(n.channel, CartridgeChannel::Nightly);
    }

    // TEST301: Walking both channels produces release entries first.
    #[test]
    fn test301_transform_walks_both_channels_release_first() {
        let release_entry = build_registry_entry(
            "R",
            vec![build_cap_group(
                "g",
                vec![build_cap("cap:effect=none", "Identity", "identity")],
                vec![],
            )],
        );
        let nightly_entry = build_registry_entry(
            "N",
            vec![build_cap_group(
                "g",
                vec![build_cap("cap:effect=none", "Identity", "identity")],
                vec![],
            )],
        );
        let registry =
            build_registry_in_channels(vec![("foo", release_entry)], vec![("bar", nightly_entry)]);
        let server =
            CartridgeRepoServer::new(registry, "https://test.example/manifest", TEST_FABRIC_URL)
                .unwrap();

        let cartridges = server.transform_to_cartridge_array().unwrap();
        let channels: Vec<CartridgeChannel> = cartridges.iter().map(|c| c.channel).collect();
        assert_eq!(
            channels,
            vec![CartridgeChannel::Release, CartridgeChannel::Nightly],
            "release entries must come before nightly entries"
        );
    }

    // TEST327: search_cartridges matches against name/description/tags
    // and cap titles, but never against cap URN strings.
    #[test]
    fn test327_cartridge_repo_server_search_cartridges() {
        let mut entry = build_registry_entry(
            "PDF Cartridge",
            vec![build_cap_group(
                "pdf",
                vec![build_cap(
                    "cap:in=\"media:ext=pdf\";disbind;out=\"media:enc=utf-8;page\"",
                    "Disbind PDF",
                    "disbind",
                )],
                vec![],
            )],
        );
        entry.tags = vec!["document".to_string()];
        entry.description = "Process PDF documents".to_string();
        let server = CartridgeRepoServer::new(
            build_registry(vec![("pdfcartridge", entry)]),
            "https://test.example/manifest",
            TEST_FABRIC_URL,
        )
        .unwrap();

        // Match on name.
        let by_name = server.search_cartridges("pdf").unwrap();
        assert_eq!(by_name.len(), 1);
        // Match on cap title.
        let by_title = server.search_cartridges("disbind").unwrap();
        assert_eq!(by_title.len(), 1);
        // No match.
        let none = server.search_cartridges("nonexistent").unwrap();
        assert_eq!(none.len(), 0);
    }

    // TEST328: get_cartridges_by_category filters on the categories
    // string list.
    #[test]
    fn test328_cartridge_repo_server_get_by_category() {
        let mut entry = build_registry_entry(
            "Doc Cartridge",
            vec![build_cap_group(
                "g",
                vec![build_cap("cap:effect=none", "Identity", "identity")],
                vec![],
            )],
        );
        entry.categories = vec!["document".to_string()];
        let server = CartridgeRepoServer::new(
            build_registry(vec![("doccartridge", entry)]),
            "https://test.example/manifest",
            TEST_FABRIC_URL,
        )
        .unwrap();
        assert_eq!(
            server.get_cartridges_by_category("document").unwrap().len(),
            1
        );
        assert_eq!(
            server
                .get_cartridges_by_category("nonexistent")
                .unwrap()
                .len(),
            0
        );
    }

    // TEST329: get_cartridges_by_cap parses the input URN and matches
    // each cartridge cap via tagged-URN equivalence — not string ==.
    // This proves a request URN whose tags appear in a different order
    // than the cap's declared form still resolves.
    #[test]
    fn test329_cartridge_repo_server_get_by_cap() {
        let declared_urn =
            "cap:in=\"media:ext=pdf\";disbind;out=\"media:disbound-page;enc=utf-8;list\"";
        // Same cap URN with the in/out spec tags in a different declared
        // order. Tagged-URN normalization treats them as identical.
        let request_urn =
            "cap:in=\"media:ext=pdf\";disbind;out=\"media:disbound-page;enc=utf-8;list\"";

        let entry = build_registry_entry(
            "PDF Cartridge",
            vec![build_cap_group(
                "pdf",
                vec![build_cap(declared_urn, "Disbind PDF", "disbind")],
                vec![],
            )],
        );
        let server = CartridgeRepoServer::new(
            build_registry(vec![("pdfcartridge", entry)]),
            "https://test.example/manifest",
            TEST_FABRIC_URL,
        )
        .unwrap();

        let exact = server.get_cartridges_by_cap(declared_urn).unwrap();
        assert_eq!(exact.len(), 1);

        let reordered = server.get_cartridges_by_cap(request_urn).unwrap();
        assert_eq!(
            reordered.len(),
            1,
            "tagged-URN equivalence must match across declared tag order"
        );

        let bogus = server.get_cartridges_by_cap("cap:in=media:bogus;out=media:nonexistent");
        assert!(bogus.unwrap().is_empty());
    }

    // ----------------------------------------------------------------------
    // CartridgeRepo (client/cache) tests.
    // ----------------------------------------------------------------------

    // TEST330: update_cache populates the cartridge map keyed by
    // (channel, id) and the cap-to-cartridge index keyed by normalized
    // URNs.
    #[tokio::test]
    async fn test330_cartridge_repo_client_update_cache() {
        let repo = CartridgeRepo::new(3600, TEST_FABRIC_URL);
        let registry = CartridgeRegistryResponse {
            cartridges: vec![build_cartridge_info(
                "testcartridge",
                "Test Cartridge",
                vec![build_cap_group(
                    "g",
                    vec![build_cap("cap:effect=none", "Identity", "identity")],
                    vec![],
                )],
            )],
        };
        let mut caches = repo.caches.write().await;
        CartridgeRepo::update_cache(&mut caches, "https://example.com/cartridges", registry)
            .expect("update_cache must succeed for a well-formed registry");
        drop(caches);
        let cartridge = repo
            .get_cartridge(
                "https://example.com/cartridges",
                CartridgeChannel::Release,
                "testcartridge",
            )
            .await;
        assert!(cartridge.is_some());
        assert_eq!(cartridge.unwrap().channel, CartridgeChannel::Release);
        // Same id in nightly is absent — channels are independent.
        assert!(repo
            .get_cartridge(
                "https://example.com/cartridges",
                CartridgeChannel::Nightly,
                "testcartridge"
            )
            .await
            .is_none());
    }

    // TEST331: get_suggestions_for_cap returns a suggestion when the
    // cache has a cartridge whose cap is tagged-URN equivalent to the
    // request, even if declared with different tag order.
    #[tokio::test]
    async fn test331_cartridge_repo_client_get_suggestions() {
        let repo = CartridgeRepo::new(3600, TEST_FABRIC_URL);
        let declared_urn =
            "cap:in=\"media:ext=pdf\";disbind;out=\"media:disbound-page;enc=utf-8;list\"";
        let request_urn =
            "cap:in=\"media:ext=pdf\";disbind;out=\"media:disbound-page;enc=utf-8;list\"";

        let registry = CartridgeRegistryResponse {
            cartridges: vec![{
                let mut info = build_cartridge_info(
                    "pdfcartridge",
                    "PDF Cartridge",
                    vec![build_cap_group(
                        "pdf",
                        vec![build_cap(declared_urn, "Disbind PDF", "disbind")],
                        vec![],
                    )],
                );
                info.page_url = "https://example.com/pdf".to_string();
                info
            }],
        };
        let mut caches = repo.caches.write().await;
        CartridgeRepo::update_cache(&mut caches, "https://example.com/cartridges", registry)
            .expect("update_cache must succeed");
        drop(caches);

        let suggestions = repo.get_suggestions_for_cap(request_urn).await;
        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].cartridge_id, "pdfcartridge");
        assert_eq!(suggestions[0].cap_title, "Disbind PDF");
        // Channel must propagate from cache to suggestion — UI needs it
        // to render the release/nightly distinction without re-deriving.
        assert_eq!(suggestions[0].channel, CartridgeChannel::Release);
    }

    // TEST332: get_cartridge requires a (channel, id) pair and returns
    // the cached entry for known pairs, None otherwise. The same id in
    // the wrong channel must miss.
    #[tokio::test]
    async fn test332_cartridge_repo_client_get_cartridge() {
        let repo = CartridgeRepo::new(3600, TEST_FABRIC_URL);
        let registry = CartridgeRegistryResponse {
            cartridges: vec![build_cartridge_info_in(
                CartridgeChannel::Nightly,
                "testcartridge",
                "Test Cartridge",
                vec![build_cap_group(
                    "g",
                    vec![build_cap("cap:effect=none", "Identity", "identity")],
                    vec![],
                )],
            )],
        };
        let mut caches = repo.caches.write().await;
        CartridgeRepo::update_cache(&mut caches, "https://example.com/cartridges", registry)
            .expect("update_cache must succeed");
        drop(caches);

        assert!(repo
            .get_cartridge(
                "https://example.com/cartridges",
                CartridgeChannel::Nightly,
                "testcartridge"
            )
            .await
            .is_some());
        assert!(repo
            .get_cartridge(
                "https://example.com/cartridges",
                CartridgeChannel::Release,
                "testcartridge"
            )
            .await
            .is_none());
        assert!(repo
            .get_cartridge(
                "https://example.com/cartridges",
                CartridgeChannel::Nightly,
                "nonexistent"
            )
            .await
            .is_none());
    }

    // TEST333: get_all_available_caps returns the deduplicated set of
    // normalized URNs across cartridges.
    #[tokio::test]
    async fn test333_cartridge_repo_client_get_all_caps() {
        let repo = CartridgeRepo::new(3600, TEST_FABRIC_URL);
        let cap1 = "cap:in=\"media:ext=pdf\";disbind;out=\"media:disbound-page;enc=utf-8;list\"";
        let cap2 =
            "cap:in=\"media:enc=utf-8;ext=txt\";disbind;out=\"media:disbound-page;enc=utf-8;list\"";

        let registry = CartridgeRegistryResponse {
            cartridges: vec![
                build_cartridge_info(
                    "cartridge1",
                    "Cartridge 1",
                    vec![build_cap_group(
                        "g",
                        vec![build_cap(cap1, "Cap 1", "x")],
                        vec![],
                    )],
                ),
                build_cartridge_info(
                    "cartridge2",
                    "Cartridge 2",
                    vec![build_cap_group(
                        "g",
                        vec![build_cap(cap2, "Cap 2", "x")],
                        vec![],
                    )],
                ),
            ],
        };
        let mut caches = repo.caches.write().await;
        CartridgeRepo::update_cache(&mut caches, "https://example.com/cartridges", registry)
            .expect("update_cache must succeed");
        drop(caches);

        let caps = repo.get_all_available_caps().await;
        assert_eq!(caps.len(), 2, "two distinct caps expected, got {:?}", caps);
    }

    // TEST334: needs_sync returns true on an empty cache, false right
    // after a successful update.
    #[tokio::test]
    async fn test334_cartridge_repo_client_needs_sync() {
        let repo = CartridgeRepo::new(3600, TEST_FABRIC_URL);
        let urls = vec!["https://example.com/cartridges".to_string()];
        assert!(repo.needs_sync(&urls).await);

        let registry = CartridgeRegistryResponse { cartridges: vec![] };
        let mut caches = repo.caches.write().await;
        CartridgeRepo::update_cache(&mut caches, "https://example.com/cartridges", registry)
            .expect("update_cache must succeed for an empty registry");
        drop(caches);

        assert!(!repo.needs_sync(&urls).await);
    }

    // TEST335: A v4.0 nested registry round-trips through Server →
    // CartridgeInfo → fingerprint, preserving the cap_groups structure
    // and the signed flag.
    #[test]
    fn test335_cartridge_repo_server_client_integration() {
        let cap_urn = "cap:in=\"media:test\";test;out=\"media:result\"";
        let entry = build_registry_entry(
            "Test Cartridge",
            vec![build_cap_group(
                "test-group",
                vec![build_cap(cap_urn, "Test Cap", "test")],
                vec!["media:test".to_string()],
            )],
        );
        let server = CartridgeRepoServer::new(
            build_registry(vec![("testcartridge", entry)]),
            "https://test.example/manifest",
            TEST_FABRIC_URL,
        )
        .unwrap();
        let response = server.get_cartridges().unwrap();

        assert_eq!(response.cartridges.len(), 1);
        let cartridge = &response.cartridges[0];
        assert!(cartridge.is_signed());
        assert!(!cartridge.versions.is_empty());
        assert_eq!(cartridge.cap_groups.len(), 1);
        assert_eq!(
            cartridge.cap_groups[0].adapter_urns,
            vec!["media:test".to_string()]
        );
        assert_eq!(cartridge.iter_caps().count(), 1);
    }

    // TEST319: A registry response with a malformed cap URN inside
    // cap_groups must propagate as ParseError when indexed into the
    // cache, not silently disappear.
    #[tokio::test]
    async fn test319_update_cache_rejects_malformed_cap_urn() {
        let repo = CartridgeRepo::new(3600, TEST_FABRIC_URL);
        let registry = CartridgeRegistryResponse {
            cartridges: vec![build_cartridge_info(
                "broken",
                "Broken",
                vec![build_cap_group(
                    "g",
                    vec![build_cap("not a valid urn at all", "Bad", "x")],
                    vec![],
                )],
            )],
        };
        let mut caches = repo.caches.write().await;
        let result = CartridgeRepo::update_cache(&mut caches, "https://x", registry);
        assert!(matches!(result, Err(CartridgeRepoError::ParseError(_))));
    }
}
