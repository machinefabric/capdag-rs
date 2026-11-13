//! Unified fabric registry: caps, media defs, aliases and dims.
//!
//! Four domain payload types:
//! - `Cap` (cap definitions) at `<base>/caps/<sha256-of-canonical-urn>`
//! - `StoredMediaDef` (media defs) at `<base>/media/<sha256-of-canonical-urn>`
//! - `StoredAlias` (aliases) at `<base>/aliases/<sha256-of-name>`
//! - `StoredDim` (dims) at `<base>/dims/<sha256-of-name>`
//!
//! On disk, under the folder of the manifest version the registry is pinned
//! at (`<os cache>/capdag/<charter slug>/v<N>/`), each at
//! `<kind>/<sha256>/<defver>.json`, beside `manifest.json`. Entries are
//! replaced whole and a corrupt one is an error naming its file: see the
//! `cache` module.
//!
//! Resolution policy (same for both domains):
//!   1. In-memory cache hit → return immediately.
//!   2. Synchronous fetch attempt with hard 500 ms deadline.
//!   3. Deadline miss / error → enqueue for background consumer, and return
//!      the error: a refusal, a timeout or the deadline running out is a
//!      `FetchFailed` naming the URL, the status and the manifest version;
//!      only the registry's own answer that it has no such definition is a
//!      `NotFound`.
//!
//! The cap fetch is **atomic**: if any media URN referenced by a cap fails
//! to fetch, the cap is NOT cached. This guarantees that any cap landing
//! in the cap cache has every one of its referenced media defs already in
//! the media cache (and the extension index).
//!
//! A cap or media def is likewise cached only once every dim it names
//! (`dims`) is cached, at the version the manifest pins, with the value it
//! names. So a reader holding a cached definition can always read the
//! documentation of the dims it was built from (`get_cached_dim`).

use crate::bifaci::trust::RegistryKind;
use crate::fabric::cache;
use mg_srel::{Charter, CharterTrust, VerifiedCharter};
use crate::fabric::alias::{classify_alias_target, normalize_alias_name, AliasTargetKind};
use crate::fabric::dim::{is_dim_name, DimRef, StoredDim};
use crate::media::spec::MediaDef;
use crate::Cap;
use crate::StoredAlias;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};

/// The anchor of MachineFabric's production fabric, used when nothing names
/// another. Only the anchor: where its objects are is read from its charter.
const DEFAULT_FABRIC_ANCHOR: &str = "https://fabric.capdag.com";

/// Wall-clock TTL retained only for the v0 (legacy, flat-path) resolution
/// mode. Versioned objects at v >= 1 are immutable by protocol — once a
/// definition is published at `caps/<sha>/<defver>.json`, its bytes
/// never change — so versioned cache entries never expire.
const CACHE_DURATION_HOURS: u64 = 24;

/// Hard wall-clock budget for the synchronous fetch attempt that
/// `get_cached_cap` and `get_cached_media_def` each make on a cache
/// miss. Anything that doesn't return inside this window times out and
/// falls through to the queue path; the next call hits warm cache.
const SYNC_FETCH_DEADLINE: Duration = Duration::from_millis(500);

// =============================================================================
// CONFIGURATION
// =============================================================================

/// What a fabric registry client is asked to use.
///
/// The ANCHOR names the registry and is all a client is told; where its
/// manifest and definitions are, and the folder its cache lives in, come from
/// the registry's charter ([`FabricLocations`]). Sources, in priority order:
/// the builder methods, then `CDG_FABRIC_REGISTRY_URL`, then MachineFabric's
/// production fabric.
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    pub anchor: String,
    /// When true, the registry ignores every on-disk cache entry: the
    /// manifest is re-fetched from the network (rather than read from
    /// `manifests/<v>.json`) and the disk cap/media/alias bodies are NOT
    /// preloaded, so every lookup is served fresh from the registry. This
    /// is the correct mode against a mutable channel (e.g. staging, which
    /// re-publishes the SAME manifest version with new/changed caps): the
    /// version-keyed cache would otherwise return a stale snapshot. Fresh
    /// bytes still overwrite the cache as they arrive.
    pub bypass_cache: bool,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            anchor: env::var("CDG_FABRIC_REGISTRY_URL")
                .unwrap_or_else(|_| DEFAULT_FABRIC_ANCHOR.to_string()),
            bypass_cache: false,
        }
    }
}

impl RegistryConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_anchor(mut self, anchor: impl Into<String>) -> Self {
        self.anchor = anchor.into();
        self
    }

    /// Enable cache-bypass mode (see [`RegistryConfig::bypass_cache`]).
    pub fn with_bypass_cache(mut self, bypass: bool) -> Self {
        self.bypass_cache = bypass;
        self
    }
}

/// Where a fabric registry's objects are, as its charter states them.
///
/// Built once from a verified charter, whose shape check guarantees each
/// template uses exactly the placeholders filled here — so filling one cannot
/// fail, and nothing in the client composes a fabric URL any other way.
#[derive(Debug, Clone)]
pub struct FabricLocations {
    manifest: String,
    definition: String,
}

impl FabricLocations {
    pub fn from_charter(charter: &Charter) -> Result<Self, FabricRegistryError> {
        if charter.kind != RegistryKind::Fabric.wire_name() {
            return Err(FabricRegistryError::Charter(format!(
                "{} is chartered as a {} registry, not a fabric",
                charter.anchor, charter.kind
            )));
        }
        let take = |name: &str| {
            charter.locations.get(name).cloned().ok_or_else(|| {
                FabricRegistryError::Charter(format!(
                    "the fabric charter of {} declares no '{name}' location",
                    charter.anchor
                ))
            })
        };
        Ok(Self { manifest: take("manifest")?, definition: take("definition")? })
    }

    /// The URL of manifest `version`.
    pub fn manifest(&self, version: u32) -> String {
        self.manifest.replace("{manifest_version}", &version.to_string())
    }

    /// The URL of one definition: `kind` is `caps`, `media`, `aliases` or `dims`.
    pub fn definition(&self, kind: &str, urn_hash: &str, version: u32) -> String {
        self.definition
            .replace("{kind}", kind)
            .replace("{urn_hash}", urn_hash)
            .replace("{version}", &version.to_string())
    }
}

// =============================================================================
// PAYLOAD TYPES
// =============================================================================

/// Stored media def format (matches registry API response)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StoredMediaDef {
    pub urn: String,
    /// Per-definition version. 0 ⇒ v0 (frozen flat-path); >= 1 ⇒ pinned
    /// at `media/<sha256-of-urn>/<version>.json` and referenced by a
    /// manifest at that defver.
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub version: u32,
    pub media_type: String,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
    /// The dims this media def was built from (see `MediaDef::dims`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dims: Vec<crate::DimRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation: Option<crate::MediaValidation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extensions: Vec<String>,
}

fn is_zero_u32(v: &u32) -> bool {
    *v == 0
}

impl StoredMediaDef {
    pub fn to_media_def_def(&self) -> MediaDef {
        MediaDef {
            urn: self.urn.clone(),
            media_type: self.media_type.clone(),
            title: self.title.clone(),
            profile_uri: self.profile_uri.clone(),
            schema: self.schema.clone(),
            description: self.description.clone(),
            documentation: self.documentation.clone(),
            dims: self.dims.clone(),
            validation: self.validation.clone(),
            metadata: self.metadata.clone(),
            extensions: self.extensions.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CapCacheEntry {
    definition: Cap,
    cached_at: u64,
    ttl_hours: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MediaCacheEntry {
    spec: StoredMediaDef,
    cached_at: u64,
    ttl_hours: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AliasCacheEntry {
    alias: StoredAlias,
    cached_at: u64,
    ttl_hours: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DimCacheEntry {
    dim: StoredDim,
    cached_at: u64,
    ttl_hours: u64,
}

trait CacheEntryExt {
    fn cached_at(&self) -> u64;
    fn ttl_hours(&self) -> u64;
    fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now > self.cached_at() + (self.ttl_hours() * 3600)
    }
}
impl CacheEntryExt for CapCacheEntry {
    fn cached_at(&self) -> u64 {
        self.cached_at
    }
    fn ttl_hours(&self) -> u64 {
        self.ttl_hours
    }
}
impl CacheEntryExt for MediaCacheEntry {
    fn cached_at(&self) -> u64 {
        self.cached_at
    }
    fn ttl_hours(&self) -> u64 {
        self.ttl_hours
    }
}
impl CacheEntryExt for AliasCacheEntry {
    fn cached_at(&self) -> u64 {
        self.cached_at
    }
    fn ttl_hours(&self) -> u64 {
        self.ttl_hours
    }
}

// =============================================================================
// URN NORMALISATION
// =============================================================================

/// Pick the display alias from a set of alias names that all target the same
/// URN: the SHORTEST name, ties broken alphabetically. Returns `None` for an
/// empty set.
///
/// The ordering is total and deterministic: `(len, name)` lexicographic. So
/// `png` beats `png-image` (shorter), and between equal-length `a16` / `a09`
/// the alphabetical-smaller `a09` wins. Stable across processes for a given
/// alias set, which is what makes aliased UI/notation reproducible.
fn select_display_alias<'a>(names: impl Iterator<Item = &'a str>) -> Option<&'a str> {
    names.min_by(|a, b| a.len().cmp(&b.len()).then_with(|| a.cmp(b)))
}

fn normalize_cap_urn(urn: &str) -> Result<String, FabricRegistryError> {
    crate::CapUrn::from_string(urn)
        .map(|parsed| parsed.to_string())
        .map_err(|e| FabricRegistryError::ParseError(format!("malformed cap URN '{}': {}", urn, e)))
}

fn normalize_media_urn(urn: &str) -> Result<String, FabricRegistryError> {
    crate::MediaUrn::from_string(urn)
        .map(|parsed| parsed.to_string())
        .map_err(|e| {
            FabricRegistryError::ParseError(format!("malformed media URN '{}': {}", urn, e))
        })
}

/// Distinguishes domain on the background-fetch queue. Pairs URN with
/// defver so the consumer always hits the right R2 path. Alias keys carry
/// the (normalized) alias name instead of a URN. Dims are never queued: they
/// are fetched with the definitions that name them, or by `get_dim`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum FetchKey {
    Cap { urn: String, defver: u32 },
    Media { urn: String, defver: u32 },
    Alias { name: String, defver: u32 },
}

/// A versioned registry snapshot. Mirrors `fabric/manifest.schema.json`
/// on the wire.
///
/// v0 (the implicit pre-versioning state) has no manifest object — the
/// registry resolves URNs via the frozen flat R2 paths in that mode.
/// Manifests at version >= 1 explicitly name every URN that belongs to
/// the snapshot, paired with the defver at which it is published.
///
/// A defver of 0 in this manifest's `caps` or `media` map means the
/// entry resolves through the legacy flat path; that is allowed by the
/// wire schema even though no source TOML produces a v0 def.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub previous: u32,
    #[serde(default)]
    pub caps: HashMap<String, u32>,
    #[serde(default)]
    pub media: HashMap<String, u32>,
    /// Map from normalized alias name to its per-definition version. Each
    /// alias resolves to exactly one cap or media URN; the body (the
    /// `name -> target` mapping) lives at `aliases/<sha256-of-name>/<defver>.json`.
    #[serde(default)]
    pub aliases: HashMap<String, u32>,
    /// Map from dim name to its per-definition version. The body (the dim's
    /// title, documentation and values) lives at
    /// `dims/<sha256-of-name>/<defver>.json`. Every dim a cap or media def of
    /// the snapshot names is here. Required: a manifest without it is not one
    /// this library reads.
    pub dims: HashMap<String, u32>,
}

impl Manifest {
    /// Build an empty manifest pinned at `version`. `previous` is set to
    /// `version - 1` so re-publishing the same content stays byte-stable.
    pub fn empty(version: u32) -> Self {
        Self {
            version,
            previous: version.saturating_sub(1),
            caps: HashMap::new(),
            media: HashMap::new(),
            aliases: HashMap::new(),
            dims: HashMap::new(),
        }
    }
}

// =============================================================================
// REGISTRY
// =============================================================================

/// Lower-case extension → the file media definitions that list it.
type FileMediaIndex = HashMap<String, Vec<StoredMediaDef>>;

/// How many file media definitions are fetched at once when the file media
/// index loads on a cold cache.
const FILE_MEDIA_FETCHES_AT_ONCE: usize = 16;

#[derive(Debug)]
pub struct FabricRegistry {
    client: reqwest::Client,
    /// The cache folder of the manifest version this registry is pinned at,
    /// `<os cache>/capdag/<charter slug>/v<N>`. Definitions live at
    /// `<kind>/<sha>/<defver>.json` and the manifest at `manifest.json`
    /// (the `cache` module).
    cache_dir: PathBuf,
    cached_caps: Arc<Mutex<HashMap<String, Cap>>>,
    cached_media_defs: Arc<Mutex<HashMap<String, StoredMediaDef>>>,
    /// Normalized alias name → resolved `StoredAlias`. Populated from the
    /// `aliases/<sha>/<defver>.json` cache on disk and the background/sync
    /// fetch path, filtered to the pinned manifest's defvers.
    cached_aliases: Arc<Mutex<HashMap<String, StoredAlias>>>,
    /// Dim name → resolved `StoredDim`. Populated from the
    /// `dims/<sha>/<defver>.json` cache on disk and the fetch path, filtered
    /// to the pinned manifest's defvers.
    cached_dims: Arc<Mutex<HashMap<String, StoredDim>>>,
    /// Lower-case extension → the file media definitions that list it,
    /// loaded from the whole manifest when a file's type is first asked
    /// (`file_media_for_extension`).
    file_media: Arc<Mutex<Option<Arc<FileMediaIndex>>>>,
    /// Held while the file media index loads, so it loads once.
    file_media_loading: Arc<tokio::sync::Mutex<()>>,
    config: RegistryConfig,
    /// The registry's verified charter: its slug names `cache_dir`, and its
    /// locations are where everything is fetched from.
    charter: Arc<VerifiedCharter>,
    locations: FabricLocations,
    /// Fabric manifest version this registry is pinned to (>= 1). Set at
    /// construction from the caller (the engine bakes
    /// `capdag::FABRIC_MANIFEST_VERSION`).
    manifest_version: u32,
    /// Live snapshot of the registry pinned at `manifest_version`. Every URN
    /// lookup hits this map first to turn the URN into a `(urn, defver)` pair
    /// before fetching.
    /// Wrapped in Mutex because test helpers like `add_caps_to_cache`
    /// mutate it.
    manifest: Arc<Mutex<Manifest>>,
    offline_flag: Arc<AtomicBool>,
    fetch_queue_tx: Option<mpsc::UnboundedSender<FetchKey>>,
    fetch_in_queue: Arc<Mutex<HashSet<FetchKey>>>,
    /// What the last failed fetch of each definition came to, until a fetch
    /// of it succeeds. A lookup that cannot wait on the network says this
    /// rather than "not fetched yet" when the registry already answered.
    fetch_failures: FetchFailures,
    cache_revision_tx: watch::Sender<u64>,
}

/// The last failure of each definition's fetch: see `FabricRegistry::fetch_failures`.
type FetchFailures = Arc<Mutex<HashMap<FetchKey, FabricRegistryError>>>;

/// Keep what a fetch of `key` came to: a failure is remembered, and a success
/// forgets the failure before it.
fn note_fetch<T>(failures: &FetchFailures, key: FetchKey, result: &Result<T, FabricRegistryError>) {
    if let Ok(mut failures) = failures.lock() {
        match result {
            Ok(_) => {
                failures.remove(&key);
            }
            Err(e) => {
                failures.insert(key, e.clone());
            }
        }
    }
}

/// Outcome of trying to narrow an abstract cap to a concrete backed cap given a
/// detected input media (and optional target). The CLI turns each variant into
/// an actionable message.
#[derive(Debug, Clone)]
pub enum CapNarrowError {
    /// The cap cache could not be read.
    Registry(String),
    /// No concrete cap can handle this input (and target).
    NoHandler {
        abstract_urn: String,
        input_media: String,
        target: Option<String>,
    },
    /// More than one concrete cap matches — the caller must disambiguate
    /// (e.g. by passing `--to <target>`, or naming the concrete alias).
    Ambiguous {
        abstract_urn: String,
        candidates: Vec<String>,
    },
}

impl std::fmt::Display for CapNarrowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CapNarrowError::Registry(e) => {
                write!(f, "cap registry unavailable while narrowing: {}", e)
            }
            CapNarrowError::NoHandler {
                abstract_urn,
                input_media,
                target,
            } => {
                write!(
                    f,
                    "no concrete cap specializes '{}' for input '{}'{}",
                    abstract_urn,
                    input_media,
                    target
                        .as_ref()
                        .map(|t| format!(" → '{}'", t))
                        .unwrap_or_default()
                )
            }
            CapNarrowError::Ambiguous {
                abstract_urn,
                candidates,
            } => {
                write!(
                    f,
                    "'{}' is ambiguous for this input — {} concrete caps match: {}. Disambiguate with --to <target> or name the concrete alias.",
                    abstract_urn,
                    candidates.len(),
                    candidates.join(", ")
                )
            }
        }
    }
}

impl std::error::Error for CapNarrowError {}

/// Whether an error means "somebody else is using this right now".
///
/// A shared cache is cleared by several processes at once. One of them
/// removing a directory another is writing into is answered differently by
/// each platform — `ENOTEMPTY` or `EBUSY` — and neither says the clear failed.
fn is_busy(error: &std::io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc_enotempty) if libc_enotempty == ENOTEMPTY || libc_enotempty == EBUSY
    )
}

/// `ENOTEMPTY` is 39 on Linux and 66 on macOS; `EBUSY` is 16 on both. These
/// are raw OS numbers, so Windows needs its own: there 39 is "the disk is
/// full" and the race below went unrecognised — `ERROR_DIR_NOT_EMPTY` (145)
/// and `ERROR_SHARING_VIOLATION` (32, a peer holding an entry open to
/// refresh it) are the same two faces.
#[cfg(target_os = "macos")]
const ENOTEMPTY: i32 = 66;
#[cfg(all(unix, not(target_os = "macos")))]
const ENOTEMPTY: i32 = 39;
#[cfg(unix)]
const EBUSY: i32 = 16;
#[cfg(windows)]
const ENOTEMPTY: i32 = 145;
#[cfg(windows)]
const EBUSY: i32 = 32;

/// Whether a removal refused with "access denied" was a PEER deleting the
/// same entry.
///
/// Windows answers every open of a file that is being deleted — a second
/// delete included — with `ERROR_ACCESS_DENIED` until the deleting process's
/// last handle closes; Unix reports the same race as "not found". Access
/// denied is also what a genuine permission problem says, so the entry is
/// asked for afterwards: gone within a short bound means a peer took it,
/// still there means it really cannot be removed and that is reported.
fn deleted_by_a_peer(path: &Path, error: &std::io::Error) -> bool {
    if !cfg!(windows) || error.kind() != std::io::ErrorKind::PermissionDenied {
        return false;
    }
    for _ in 0..50 {
        if fs::symlink_metadata(path).is_err() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    false
}

/// Empty the cache folder of one manifest version, `version_dir`, and remove
/// the folders the layout before per-version folders kept beside it.
///
/// The cache is SHARED, and the processes that clear it run at the same time:
/// a test run starts eight cartridge builds within milliseconds of each other
/// and every one of them refreshes this folder. Removing the tree and putting
/// it back is not safe between peers — one process deleting an ancestor while
/// another is creating into it is how a build fails with `No such file or
/// directory` on a path it had just made.
///
/// So a kind's folder is never removed, only its contents, and every step
/// tolerates a peer having done the same thing first. The end state is the
/// same — an empty cache — and no peer is ever left with the tree missing
/// underneath it. The manifest, and any temporary file a killed writer left,
/// are removed from the version's folder; other versions' folders and the
/// stored charter are left as they are.
pub(crate) fn clear_cache_folder(version_dir: &Path) -> Result<(), FabricRegistryError> {
    for kind in cache::KIND_FOLDERS {
        let directory = version_dir.join(kind);
        fs::create_dir_all(&directory).map_err(|e| {
            FabricRegistryError::CacheError(format!(
                "Failed to create cache directory {}: {e}",
                directory.display()
            ))
        })?;
        remove_contents(&directory)?;
    }
    // What is left at the top of the version's folder: the manifest, and
    // temporary files. The kinds' folders themselves stay.
    let top = match fs::read_dir(version_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(FabricRegistryError::CacheError(format!(
                "Failed to read cache directory {}: {error}",
                version_dir.display()
            )))
        }
    };
    for entry in top.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            remove_shared(&path)?;
        }
    }
    if let Some(registry_dir) = version_dir.parent() {
        for legacy in cache::LEGACY_FOLDERS {
            let path = registry_dir.join(legacy);
            if path.is_dir() {
                remove_shared(&path)?;
            }
        }
    }
    Ok(())
}

/// Remove everything in `directory`, which peers are clearing and filling at
/// the same time ([`remove_shared`]).
fn remove_contents(directory: &Path) -> Result<(), FabricRegistryError> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        // A peer removed it between the create and the read. It is empty by
        // any reading, which is what was wanted.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(FabricRegistryError::CacheError(format!(
                "Failed to read cache directory {}: {error}",
                directory.display()
            )))
        }
    };
    for entry in entries.flatten() {
        remove_shared(&entry.path())?;
    }
    Ok(())
}

/// Remove one file or folder of the shared cache, which a peer may be
/// removing or refilling at the same moment.
fn remove_shared(path: &Path) -> Result<(), FabricRegistryError> {
    let removed = if path.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    };
    match removed {
        Ok(()) => Ok(()),
        // Already gone: a peer clearing the same cache got there first, which
        // is the outcome either way.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        // Refilled underneath us. macOS reports this as `Directory not empty`
        // where Linux reports the missing path — two faces of the same race,
        // and neither is a failure of this clear: the entries now there were
        // put there by a peer that has just refreshed them, so they are
        // exactly what a refresh was for.
        Err(error) if is_busy(&error) => Ok(()),
        Err(error) if deleted_by_a_peer(path, &error) => Ok(()),
        Err(error) => Err(FabricRegistryError::CacheError(format!(
            "Failed to remove {}: {error}",
            path.display()
        ))),
    }
}

impl FabricRegistry {
    /// Create a new fabric registry pinned at the workspace-baked
    /// `capdag::FABRIC_MANIFEST_VERSION`. Standard entry point — engine
    /// code that doesn't specifically need a different version uses this.
    pub async fn new() -> Result<Self, FabricRegistryError> {
        Self::with_config_and_manifest_version(
            RegistryConfig::default(),
            crate::FABRIC_MANIFEST_VERSION,
        )
        .await
    }

    /// Create a new fabric registry with custom configuration, pinned at
    /// the workspace-baked manifest version.
    pub async fn with_config(config: RegistryConfig) -> Result<Self, FabricRegistryError> {
        Self::with_config_and_manifest_version(config, crate::FABRIC_MANIFEST_VERSION).await
    }

    /// Full constructor: custom config + explicit pinned manifest version.
    ///
    /// The registry's charter is resolved first — the one stored in the cache
    /// when it still verifies, else fetched from the anchor — and everything
    /// after it is read from the charter: the cache folder is its slug, and
    /// the manifest is fetched from its location. The constructor **blocks**
    /// on the network when either is not cached. A version below 1 names no
    /// manifest and is refused.
    pub async fn with_config_and_manifest_version(
        config: RegistryConfig,
        manifest_version: u32,
    ) -> Result<Self, FabricRegistryError> {
        if manifest_version == 0 {
            return Err(FabricRegistryError::ValidationError(
                "fabric manifest version 0 names no manifest; a registry is pinned at 1 or later"
                    .to_string(),
            ));
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| {
                FabricRegistryError::HttpError(format!("Failed to create HTTP client: {}", e))
            })?;
        let capdag_cache = Self::capdag_cache_root()?;
        let charter = resolve_charter(
            &client,
            &capdag_cache,
            &config.anchor,
            config.bypass_cache,
            &crate::bifaci::trust::charter_trust(),
            mg_srel::unix_now(),
        )
        .await?;
        let locations = FabricLocations::from_charter(&charter.charter)?;
        let cache_dir =
            cache::version_dir(&capdag_cache.join(&charter.charter.slug), manifest_version);
        for kind in cache::KIND_FOLDERS {
            let d = cache_dir.join(kind);
            fs::create_dir_all(&d).map_err(|e| {
                FabricRegistryError::CacheError(format!(
                    "Failed to create cache directory {}: {}",
                    d.display(),
                    e
                ))
            })?;
        }

        // Bootstrap the manifest before loading on-disk caches so the cache
        // loaders hydrate the in-memory map with entries matching the
        // manifest's pinned defvers, never bytes from another snapshot.
        let manifest = load_or_fetch_manifest(
            &cache_dir,
            &client,
            &locations,
            manifest_version,
            config.bypass_cache,
        )
        .await?;

        // In bypass-cache mode we do NOT hydrate from disk: every cap/media/
        // alias body is fetched fresh on demand against the (just re-fetched)
        // manifest, so a mutable channel can never serve a stale body under a
        // reused defver. The fresh bytes still write through to the cache.
        let PinnedEntries {
            caps: mut cached_caps_map,
            media: mut cached_specs_map,
            aliases: mut cached_aliases_map,
            dims: mut cached_dims_map,
        } = if config.bypass_cache {
            PinnedEntries::default()
        } else {
            load_pinned_entries(&cache_dir, &manifest)?
        };
        retain_pinned(
            &manifest,
            manifest_version,
            &mut cached_caps_map,
            &mut cached_specs_map,
            &mut cached_aliases_map,
            &mut cached_dims_map,
        );

        let cached_caps = Arc::new(Mutex::new(cached_caps_map));
        let cached_media_defs = Arc::new(Mutex::new(cached_specs_map));
        let cached_aliases = Arc::new(Mutex::new(cached_aliases_map));
        let cached_dims = Arc::new(Mutex::new(cached_dims_map));
        let manifest_arc = Arc::new(Mutex::new(manifest));
        let fetch_in_queue = Arc::new(Mutex::new(HashSet::new()));
        let fetch_failures: FetchFailures = Arc::new(Mutex::new(HashMap::new()));
        let offline_flag = Arc::new(AtomicBool::new(false));
        let (cache_revision_tx, _) = watch::channel(0u64);

        let fetch_queue_tx = match tokio::runtime::Handle::try_current() {
            Ok(_) => {
                let (tx, rx) = mpsc::unbounded_channel::<FetchKey>();
                tokio::spawn(run_fetch_consumer(
                    rx,
                    client.clone(),
                    cache_dir.clone(),
                    Arc::clone(&cached_caps),
                    Arc::clone(&cached_media_defs),
                    Arc::clone(&cached_aliases),
                    Arc::clone(&cached_dims),
                    Arc::clone(&manifest_arc),
                    Arc::clone(&fetch_in_queue),
                    Arc::clone(&fetch_failures),
                    Arc::clone(&offline_flag),
                    locations.clone(),
                    cache_revision_tx.clone(),
                ));
                Some(tx)
            }
            Err(_) => None,
        };

        let registry = Self {
            client,
            cache_dir,
            cached_caps,
            cached_media_defs,
            cached_aliases,
            cached_dims,
            file_media: Arc::new(Mutex::new(None)),
            file_media_loading: Arc::new(tokio::sync::Mutex::new(())),
            config,
            charter: Arc::new(charter),
            locations,
            manifest_version,
            manifest: manifest_arc,
            offline_flag,
            fetch_queue_tx,
            fetch_in_queue,
            fetch_failures,
            cache_revision_tx,
        };

        // The identity cap is the protocol-mandatory categorical
        // identity morphism — every capset must contain it. Seed it
        // into the in-memory cap cache directly (no network round-trip,
        // no disk write) so it is always available even on a fresh
        // install with no prior cache.
        registry.ensure_identity_cap();

        Ok(registry)
    }

    /// Returns the manifest version this registry is pinned to.
    pub fn manifest_version(&self) -> u32 {
        self.manifest_version
    }

    pub fn config(&self) -> &RegistryConfig {
        &self.config
    }

    /// The verified charter of the registry this client reads.
    pub fn charter(&self) -> &VerifiedCharter {
        &self.charter
    }

    pub fn set_offline(&self, offline: bool) {
        self.offline_flag.store(offline, Ordering::Relaxed);
    }

    pub fn subscribe_cache_revisions(&self) -> watch::Receiver<u64> {
        self.cache_revision_tx.subscribe()
    }

    /// The directory every registry's cache folder lives under:
    /// `<os cache>/capdag`. A registry's own folder is its charter's slug,
    /// so two registries never share one, whatever their URLs look like.
    fn capdag_cache_root() -> Result<PathBuf, FabricRegistryError> {
        let mut cache_dir = dirs::cache_dir().ok_or_else(|| {
            FabricRegistryError::CacheError("Could not determine cache directory".to_string())
        })?;
        cache_dir.push("capdag");
        Ok(cache_dir)
    }

    fn ensure_identity_cap(&self) {
        use crate::standard::caps::identity_cap;
        // STANDARD_CAPS travel with the manifest: their per-def version
        // is always the registry's pinned manifest version. The
        // publisher applies the same rule on the wire so the bytes on
        // R2 carry `version = manifestVersion` for every snapshot.
        let mut identity = identity_cap();
        identity.version = self.manifest_version;
        let urn = identity.urn_string();
        // The identity cap is a STANDARD_CAP synthesized in-process from a
        // fixed definition, so its URN should always parse. If it ever does
        // not, that is a build-time defect in the standard-cap definition — but
        // we surface it as a loud error and skip caching the identity cap
        // rather than panic. A bad URN must never crash registry construction
        // (and the whole app with it); downstream identity resolution will then
        // fail with its own clean, handled error instead.
        let normalized_urn = match normalize_cap_urn(&urn) {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(
                    target: "capdag::fabric::registry",
                    urn = %urn, error = %e,
                    "ensure_identity_cap: the standard identity cap URN does not parse; \
                     identity cap will not be cached (this is a standard-cap definition bug)"
                );
                return;
            }
        };
        if let Ok(mut cached_caps) = self.cached_caps.lock() {
            if !cached_caps.contains_key(&normalized_urn) {
                cached_caps.insert(normalized_urn.clone(), identity);
            }
        }
        // Record the identity cap's defver in the manifest so any
        // resolution that consults the manifest finds it. At v0 this is
        // a no-op (manifest is `empty(0)`, never consulted).
        if self.manifest_version >= 1 {
            if let Ok(mut m) = self.manifest.lock() {
                m.caps.insert(normalized_urn, self.manifest_version);
            }
        }
    }

    // -------------------------------------------------------------------------
    // CAP API
    // -------------------------------------------------------------------------

    /// Get a cap from in-memory cache or fetch from registry. Atomic with
    /// respect to referenced media defs: a cap whose media-def footprint
    /// can't be fully fetched is not cached and the call returns `Err`.
    ///
    /// `urn` may be a cap URN (`cap:...`) or an **alias** (a contiguous
    /// token with no `:`). An alias is resolved first; because this is the
    /// typed cap boundary, an alias whose target is not a cap URN is a hard
    /// error (`ValidationError`) — we never silently return a media def
    /// where a cap was demanded.
    pub async fn get_cap(&self, urn: &str) -> Result<Cap, FabricRegistryError> {
        if crate::is_alias_token(urn) {
            let target = self
                .resolve_alias_typed(urn, Some(AliasTargetKind::Cap))
                .await?;
            return Box::pin(self.get_cap(&target)).await;
        }
        let normalized_urn = normalize_cap_urn(urn)?;
        if let Some(cap) = self
            .cached_caps
            .lock()
            .ok()
            .and_then(|m| m.get(&normalized_urn).cloned())
        {
            return Ok(cap);
        }
        let defver = self.cap_defver(&normalized_urn)?;
        fetch_one_cap_atomic(
            &self.client,
            &self.cache_dir,
            &self.cached_caps,
            &self.cached_media_defs,
            &self.cached_dims,
            &self.manifest,
            &self.offline_flag,
            &self.locations,
            self.manifest_version,
            &self.cache_revision_tx,
            &normalized_urn,
            defver,
        )
        .await
    }

    /// Resolve a normalized cap URN to its defver under the pinned
    /// manifest. At v0 this is unconditionally 0 (flat path). At v >= 1
    /// the URN must be in the manifest's `caps` map; if absent the
    /// caller has asked for a URN that is not part of the snapshot and
    /// we surface that as `NotFound` rather than silently fetching from
    /// flat paths (which would mix snapshot versions).
    fn cap_defver(&self, normalized_urn: &str) -> Result<u32, FabricRegistryError> {
        if self.manifest_version == 0 {
            return Ok(0);
        }
        let m = self.manifest.lock().map_err(|e| {
            FabricRegistryError::CacheError(format!("Failed to lock manifest: {}", e))
        })?;
        m.caps.get(normalized_urn).copied().ok_or_else(|| {
            FabricRegistryError::NotFound(format!(
                "cap '{}' is not part of manifest v{}",
                normalized_urn, self.manifest_version
            ))
        })
    }

    /// Resolve a normalized media URN to its defver under the pinned
    /// manifest. Same rules as `cap_defver`.
    fn media_defver(&self, normalized_urn: &str) -> Result<u32, FabricRegistryError> {
        if self.manifest_version == 0 {
            return Ok(0);
        }
        // The empty / wildcard URN `media:` is a sentinel — caps use it
        // to denote "any media", and it has no published spec. Anywhere
        // we resolve a URN to a defver we must skip it; the upstream
        // fetch path already special-cases it for fetching, so we just
        // mirror that here by returning 0 (which would map to a flat
        // path that doesn't exist, but the caller never reaches the
        // fetch with this URN).
        if normalized_urn == "media:" {
            return Ok(0);
        }
        let m = self.manifest.lock().map_err(|e| {
            FabricRegistryError::CacheError(format!("Failed to lock manifest: {}", e))
        })?;
        m.media.get(normalized_urn).copied().ok_or_else(|| {
            FabricRegistryError::NotFound(format!(
                "media def '{}' is not part of manifest v{}",
                normalized_urn, self.manifest_version
            ))
        })
    }

    // -------------------------------------------------------------------------
    // ALIAS API
    // -------------------------------------------------------------------------

    /// Resolve a normalized alias name to its defver under the pinned
    /// manifest. Aliases exist only in the versioned regime: at v0 there
    /// are no aliases, so any alias lookup is a hard `NotFound`. At v >= 1
    /// the name must be in the manifest's `aliases` map.
    fn alias_defver(&self, normalized_name: &str) -> Result<u32, FabricRegistryError> {
        if self.manifest_version == 0 {
            return Err(FabricRegistryError::NotFound(format!(
                "alias '{}' cannot resolve: registry is pinned at v0 (aliases are a versioned-regime concept)",
                normalized_name
            )));
        }
        let m = self.manifest.lock().map_err(|e| {
            FabricRegistryError::CacheError(format!("Failed to lock manifest: {}", e))
        })?;
        m.aliases.get(normalized_name).copied().ok_or_else(|| {
            FabricRegistryError::NotFound(format!(
                "alias '{}' is not part of manifest v{}",
                normalized_name, self.manifest_version
            ))
        })
    }

    /// Resolve an alias name to the cap or media URN it points at, fetching
    /// the alias body if it is not already cached. The input is normalized
    /// per the alias name rules; a malformed name is a hard error.
    ///
    /// This is the **untyped** entry point: it returns whatever the alias
    /// targets (cap or media URN). Callers that demand a specific type use
    /// the typed boundaries (`get_cap` / `get_media_def`) or
    /// [`resolve_alias_typed`].
    pub async fn resolve_alias(&self, name: &str) -> Result<String, FabricRegistryError> {
        let alias = self.get_alias(name).await?;
        Ok(alias.target)
    }

    /// Resolve an alias and assert its target kind. If `expected` is
    /// `Some(kind)` and the resolved target is a different kind, fail hard
    /// — this is what makes a typed lookup ("give me a media") reject an
    /// alias that points at the other kind. `None` accepts either kind.
    pub async fn resolve_alias_typed(
        &self,
        name: &str,
        expected: Option<AliasTargetKind>,
    ) -> Result<String, FabricRegistryError> {
        let alias = self.get_alias(name).await?;
        let actual = classify_alias_target(&alias.target).ok_or_else(|| {
            FabricRegistryError::ValidationError(format!(
                "alias '{}' target '{}' is neither a cap nor a media URN",
                alias.name, alias.target
            ))
        })?;
        if let Some(expected_kind) = expected {
            if actual != expected_kind {
                return Err(FabricRegistryError::ValidationError(format!(
                    "alias '{}' resolves to a {} URN ('{}') but a {} was required here",
                    alias.name,
                    actual.as_str(),
                    alias.target,
                    expected_kind.as_str()
                )));
            }
        }
        Ok(alias.target)
    }

    /// Fetch the full `StoredAlias` for a name (cache-first, then network).
    pub async fn get_alias(&self, name: &str) -> Result<StoredAlias, FabricRegistryError> {
        let normalized = normalize_alias_name(name).map_err(|e| {
            FabricRegistryError::ValidationError(format!("invalid alias name: {}", e))
        })?;
        if let Some(alias) = self
            .cached_aliases
            .lock()
            .ok()
            .and_then(|m| m.get(&normalized).cloned())
        {
            return Ok(alias);
        }
        let defver = self.alias_defver(&normalized)?;
        fetch_one_alias(
            &self.client,
            &self.cache_dir,
            &self.cached_aliases,
            &self.offline_flag,
            &self.locations,
            &self.cache_revision_tx,
            self.manifest_version,
            &normalized,
            defver,
        )
        .await
    }

    /// Synchronous, in-memory-only alias resolution, for synchronous call
    /// sites (the machine-notation resolver) after an async pre-warm.
    ///
    /// `Some(target)` when the alias is in memory. `None` when the registry
    /// has no alias by this name: the pinned manifest does not name it, or
    /// the name is not one an alias can have. An alias the manifest names
    /// that is not in memory is an error: what its last fetch came to (a
    /// refusal is reported as itself), or `Pending` when it has not been
    /// fetched — it exists, so it is never reported as undefined.
    pub fn resolve_alias_cached(&self, name: &str) -> Result<Option<String>, FabricRegistryError> {
        let Ok(normalized) = normalize_alias_name(name) else {
            return Ok(None);
        };
        if let Some(target) = self
            .cached_aliases
            .lock()
            .ok()
            .and_then(|m| m.get(&normalized).map(|a| a.target.clone()))
        {
            return Ok(Some(target));
        }
        let defver = match self.alias_defver(&normalized) {
            Ok(defver) => defver,
            Err(FabricRegistryError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        let key = FetchKey::Alias {
            name: normalized.clone(),
            defver,
        };
        let last_failure = self
            .fetch_failures
            .lock()
            .ok()
            .and_then(|failures| failures.get(&key).cloned());
        let (url, _) = alias_url_and_cache_path(&self.cache_dir, &self.locations, &normalized, defver);
        Err(last_failure.unwrap_or_else(|| {
            FabricRegistryError::Pending(format!(
                "alias '{normalized}' (defver {defver}) is named by manifest v{} and has not been fetched from {url}",
                self.manifest_version
            ))
        }))
    }

    /// Reverse lookup: the display alias for a `cap:`/`media:` URN, or `None`
    /// if no cached alias points at it. This is the canonical primitive every
    /// UI surface and notation generator uses to render an aliased name in
    /// place of a raw URN.
    ///
    /// The query URN is canonicalised through its own parser (cap vs media by
    /// prefix) before matching, because alias targets are stored canonically —
    /// a non-canonical query (different tag order, redundant whitespace) would
    /// otherwise miss. A URN that is neither a cap nor a media URN, or that
    /// fails to parse, returns `None` (it cannot have an alias).
    ///
    /// When multiple aliases target the same URN, the winner is the SHORTEST
    /// name, ties broken alphabetically (see [`select_display_alias`]). This is
    /// deterministic and stable across processes for a given alias set.
    pub fn display_alias_for_urn(&self, urn: &str) -> Option<String> {
        // Canonicalise by kind. classify_alias_target keys off the prefix and
        // is the same classifier the alias publisher uses for targets, so a
        // query and a stored target canonicalise identically.
        let canonical = match classify_alias_target(urn)? {
            AliasTargetKind::Cap => normalize_cap_urn(urn).ok()?,
            AliasTargetKind::Media => normalize_media_urn(urn).ok()?,
        };
        let guard = self.cached_aliases.lock().ok()?;
        let names = guard
            .values()
            .filter(|a| a.target == canonical)
            .map(|a| a.name.as_str());
        select_display_alias(names).map(str::to_string)
    }

    /// All cached aliases whose target is a CAP URN, as `(name, cap_urn)`
    /// pairs. Used by the notation editor to offer registered cap aliases as
    /// wiring completions. Order is unspecified (the caller sorts/filters).
    /// Synchronous, cache-only — relies on the startup alias prefetch having
    /// warmed the cache.
    pub fn cached_cap_aliases(&self) -> Vec<(String, String)> {
        let Ok(guard) = self.cached_aliases.lock() else {
            return Vec::new();
        };
        guard
            .values()
            .filter(|a| classify_alias_target(&a.target) == Some(AliasTargetKind::Cap))
            .map(|a| (a.name.clone(), a.target.clone()))
            .collect()
    }

    /// Request that the background fetcher hydrate an alias into the cache.
    /// Non-blocking; the alias becomes available to `resolve_alias_cached`
    /// once the fetch completes. A malformed name or an unknown alias is a
    /// no-op (nothing is enqueued).
    pub fn request_alias_cache_hydration(&self, name: &str) {
        let Ok(normalized) = normalize_alias_name(name) else {
            return;
        };
        if let Ok(defver) = self.alias_defver(&normalized) {
            self.enqueue_for_background_fetch(FetchKey::Alias {
                name: normalized,
                defver,
            });
        }
    }

    /// Look up an alias name's pinned defver under this registry's manifest
    /// without fetching the body. Public so external callers can pre-check
    /// alias membership.
    pub fn alias_defver_for(&self, name: &str) -> Result<u32, FabricRegistryError> {
        let normalized = normalize_alias_name(name).map_err(|e| {
            FabricRegistryError::ValidationError(format!("invalid alias name: {}", e))
        })?;
        self.alias_defver(&normalized)
    }

    /// Test-only: insert an alias directly into the in-memory cache and
    /// register its defver in the manifest, bypassing the network. Mirrors
    /// `add_caps_to_cache` / `insert_cached_media_def_for_test`.
    pub fn insert_cached_alias_for_test(&self, alias: StoredAlias) {
        let name = alias.name.clone();
        let version = alias.version;
        if let Ok(mut guard) = self.cached_aliases.lock() {
            guard.insert(name.clone(), alias);
        }
        if self.manifest_version >= 1 {
            if let Ok(mut m) = self.manifest.lock() {
                m.aliases.insert(name, version);
            }
        }
    }

    // -------------------------------------------------------------------------
    // DIM API
    // -------------------------------------------------------------------------

    /// Resolve a dim name to its defver under the pinned manifest. Dims exist
    /// only in the versioned regime: at v0 any dim lookup is a hard
    /// `NotFound`. At v >= 1 the name must be in the manifest's `dims` map.
    fn dim_defver(&self, name: &str) -> Result<u32, FabricRegistryError> {
        if self.manifest_version == 0 {
            return Err(FabricRegistryError::NotFound(format!(
                "dim '{}' cannot resolve: registry is pinned at v0 (dims are a versioned-regime concept)",
                name
            )));
        }
        let m = self.manifest.lock().map_err(|e| {
            FabricRegistryError::CacheError(format!("Failed to lock manifest: {}", e))
        })?;
        m.dims.get(name).copied().ok_or_else(|| {
            FabricRegistryError::NotFound(format!(
                "dim '{}' is not part of manifest v{}",
                name, self.manifest_version
            ))
        })
    }

    /// Fetch a dim's definition — its title, documentation and values, each
    /// value with its own — by name (cache-first, then network), at the
    /// version the pinned manifest names.
    pub async fn get_dim(&self, name: &str) -> Result<StoredDim, FabricRegistryError> {
        if !is_dim_name(name) {
            return Err(FabricRegistryError::ValidationError(format!(
                "'{}' is not a dim name",
                name
            )));
        }
        if let Some(dim) = self.get_cached_dim(name) {
            return Ok(dim);
        }
        let defver = self.dim_defver(name)?;
        fetch_one_dim(
            &self.client,
            &self.cache_dir,
            &self.cached_dims,
            &self.offline_flag,
            &self.locations,
            &self.cache_revision_tx,
            self.manifest_version,
            name,
            defver,
        )
        .await
    }

    /// Synchronous, in-memory-only dim lookup. Every dim a cached cap or
    /// media def names is cached with it, so a reader holding a definition
    /// from this registry finds its dims here.
    pub fn get_cached_dim(&self, name: &str) -> Option<StoredDim> {
        self.cached_dims
            .lock()
            .ok()
            .and_then(|m| m.get(name).cloned())
    }

    /// Look up a dim name's pinned defver under this registry's manifest
    /// without fetching the body.
    pub fn dim_defver_for(&self, name: &str) -> Result<u32, FabricRegistryError> {
        self.dim_defver(name)
    }

    /// Test-only: name a media def in the pinned manifest at `defver` without
    /// caching it, so a lookup of it goes to the registry.
    #[cfg(test)]
    pub(crate) fn pin_media_def_for_test(&self, urn: &str, defver: u32) {
        let normalized = normalize_media_urn(urn).expect("a test media URN parses");
        self.manifest.lock().unwrap().media.insert(normalized, defver);
    }

    /// Test-only: insert a dim directly into the in-memory cache and register
    /// its defver in the manifest, bypassing the network. Mirrors
    /// `insert_cached_alias_for_test`.
    pub fn insert_cached_dim_for_test(&self, dim: StoredDim) {
        let name = dim.name.clone();
        let version = dim.version;
        if let Ok(mut guard) = self.cached_dims.lock() {
            guard.insert(name.clone(), dim);
        }
        if self.manifest_version >= 1 {
            if let Ok(mut m) = self.manifest.lock() {
                m.dims.insert(name, version);
            }
        }
        publish_cache_revision(&self.cache_revision_tx);
    }

    /// Get multiple caps at once - fails if any cap is not available.
    pub async fn get_caps(&self, urns: &[&str]) -> Result<Vec<Cap>, FabricRegistryError> {
        let mut caps = Vec::new();
        for urn in urns {
            caps.push(self.get_cap(urn).await?);
        }
        Ok(caps)
    }

    /// Warm the in-memory cap cache for every cap in the pinned manifest
    /// that is not already cached, fetching concurrently.
    ///
    /// The manifest IS the authoritative list of cap definitions the
    /// snapshot contains, so this is the complete set of caps any attached
    /// cartridge could legitimately advertise. Running this once during
    /// engine startup — before cartridges attach and the first
    /// `LiveCapFab` pass runs — means the synchronous `is_equivalent`
    /// lookup in `LiveCapFab` finds every cap already resident, instead of
    /// dropping it (with a warning) and deferring to the background
    /// fetcher. On a fresh install or after a manifest bump this collapses
    /// thousands of "no equivalent in the registry yet" warnings and the
    /// staggered graph rebuilds that follow into one upfront warm-up.
    ///
    /// At v0 (manifest_version == 0) the manifest is empty, so this is a
    /// no-op — legacy flat-path resolution is unchanged. Individual fetch
    /// failures are logged and counted but do not abort the warm-up: a
    /// missing or unreachable cap still hits the existing on-demand
    /// background path later.
    pub async fn prefetch_manifest_caps(&self) {
        if self.manifest_version == 0 {
            return;
        }

        // Snapshot the manifest URNs and the already-cached set under their
        // locks, then release before doing any network work.
        let to_fetch: Vec<String> = {
            let manifest = match self.manifest.lock() {
                Ok(m) => m,
                Err(e) => {
                    tracing::error!(error = %e, "[prefetch] failed to lock manifest");
                    return;
                }
            };
            let cached = self.cached_caps.lock().ok();
            manifest
                .caps
                .keys()
                .filter(|urn| {
                    cached
                        .as_ref()
                        .map(|c| !c.contains_key(*urn))
                        .unwrap_or(true)
                })
                .cloned()
                .collect()
        };

        if to_fetch.is_empty() {
            return;
        }

        let total = to_fetch.len();
        tracing::info!(
            count = total,
            manifest_version = self.manifest_version,
            "[prefetch] warming cap cache from manifest before LiveCapFab builds"
        );

        // Bounded concurrency: drive the network directly through the same
        // atomic fetcher `get_cap` uses (which caches the cap and its
        // referenced media defs on success), but cap the in-flight requests
        // to avoid a thundering-herd of connections against R2.
        const MAX_IN_FLIGHT: usize = 16;
        let mut warmed = 0usize;
        let mut failed = 0usize;
        let mut set: tokio::task::JoinSet<(String, Result<Cap, FabricRegistryError>)> =
            tokio::task::JoinSet::new();
        let mut iter = to_fetch.into_iter();

        // Prime up to MAX_IN_FLIGHT tasks, then refill one-for-one as each
        // completes so at most MAX_IN_FLIGHT fetches are ever in flight.
        for _ in 0..MAX_IN_FLIGHT {
            if let Some(urn) = iter.next() {
                self.spawn_cap_warm(&mut set, urn);
            }
        }
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((_, Ok(_))) => warmed += 1,
                Ok((urn, Err(e))) => {
                    failed += 1;
                    tracing::warn!(
                        cap_urn = %urn,
                        error = %e,
                        "[prefetch] failed to warm cap; on-demand background fetch will retry later"
                    );
                }
                Err(e) => {
                    failed += 1;
                    tracing::warn!(error = %e, "[prefetch] cap warm task panicked");
                }
            }
            if let Some(urn) = iter.next() {
                self.spawn_cap_warm(&mut set, urn);
            }
        }

        tracing::info!(
            warmed,
            failed,
            total,
            "[prefetch] cap cache warm-up complete"
        );
    }

    /// Warm the alias cache from the pinned manifest so the SYNCHRONOUS
    /// alias resolver (`resolve_alias_cached`, used by the machine-notation
    /// parser) finds every registered alias without a per-lookup network
    /// round-trip. Without this, the first parse of a machine that references
    /// a cap-position alias (e.g. `identity`) reports a spurious
    /// "undefined alias" because the cache hasn't been hydrated yet.
    ///
    /// Mirrors `prefetch_manifest_caps`: it fetches every manifest alias the
    /// in-memory cache is missing. `get_alias` caches on success, so this is
    /// the alias analogue of the cap warm-up. Failures are logged and left for
    /// the on-demand background fetcher to retry — a single unreachable alias
    /// must not abort startup.
    pub async fn prefetch_manifest_aliases(&self) {
        if self.manifest_version == 0 {
            return;
        }

        let to_fetch: Vec<String> = {
            let manifest = match self.manifest.lock() {
                Ok(m) => m,
                Err(e) => {
                    tracing::error!(error = %e, "[prefetch] failed to lock manifest for aliases");
                    return;
                }
            };
            let cached = self.cached_aliases.lock().ok();
            manifest
                .aliases
                .keys()
                .filter(|name| {
                    cached
                        .as_ref()
                        .map(|c| !c.contains_key(*name))
                        .unwrap_or(true)
                })
                .cloned()
                .collect()
        };

        if to_fetch.is_empty() {
            return;
        }

        let total = to_fetch.len();
        tracing::info!(
            count = total,
            manifest_version = self.manifest_version,
            "[prefetch] warming alias cache from manifest"
        );

        // Bounded concurrency, same shape as the cap warm above. A SERIAL
        // loop here was a real startup failure: with a COLD disk cache
        // (fresh workdir, cleared cache, bumped manifest) every alias is a
        // network round-trip, and ~280 sequential GETs can exceed the
        // app-side 30s port deadline — the engine never opens gRPC because
        // this warm runs before the server starts, and the splash dies with
        // "did not reach its configuring state". Intermittent by nature:
        // network jitter decides which side of the deadline a cold start
        // lands on. Warm in parallel; per-alias failures stay soft (the
        // on-demand background fetcher retries later).
        const MAX_IN_FLIGHT: usize = 16;
        let mut warmed = 0usize;
        let mut failed = 0usize;
        let mut set: tokio::task::JoinSet<(String, Result<StoredAlias, FabricRegistryError>)> =
            tokio::task::JoinSet::new();
        let mut iter = to_fetch.into_iter();
        for _ in 0..MAX_IN_FLIGHT {
            if let Some(name) = iter.next() {
                self.spawn_alias_warm(&mut set, name);
            }
        }
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((_, Ok(_))) => warmed += 1,
                Ok((name, Err(e))) => {
                    failed += 1;
                    tracing::warn!(
                        alias = %name,
                        error = %e,
                        "[prefetch] failed to warm alias; on-demand background fetch will retry later"
                    );
                }
                Err(e) => {
                    failed += 1;
                    tracing::warn!(error = %e, "[prefetch] alias warm task panicked");
                }
            }
            if let Some(name) = iter.next() {
                self.spawn_alias_warm(&mut set, name);
            }
        }

        tracing::info!(
            warmed,
            failed,
            total,
            "[prefetch] alias cache warm-up complete"
        );
    }

    /// Spawn a single alias warm-up task onto `set`, cloning the `Arc`
    /// handles the atomic fetcher needs so it can run independently of
    /// `&self` — the alias twin of `spawn_cap_warm`.
    fn spawn_alias_warm(
        &self,
        set: &mut tokio::task::JoinSet<(String, Result<StoredAlias, FabricRegistryError>)>,
        name: String,
    ) {
        let client = self.client.clone();
        let cache_dir = self.cache_dir.clone();
        let cached_aliases = Arc::clone(&self.cached_aliases);
        let offline_flag = Arc::clone(&self.offline_flag);
        let locations = self.locations.clone();
        let cache_revision_tx = self.cache_revision_tx.clone();
        let manifest_version = self.manifest_version;
        let fetch_failures = Arc::clone(&self.fetch_failures);
        // Resolve the defver under the manifest lock BEFORE spawning — the
        // task then owns everything it needs.
        let normalized = match normalize_alias_name(&name) {
            Ok(n) => n,
            Err(e) => {
                set.spawn(async move {
                    (
                        name,
                        Err(FabricRegistryError::ValidationError(format!(
                            "invalid alias name: {}",
                            e
                        ))),
                    )
                });
                return;
            }
        };
        let defver = match self.alias_defver(&normalized) {
            Ok(d) => d,
            Err(e) => {
                set.spawn(async move { (name, Err(e)) });
                return;
            }
        };
        set.spawn(async move {
            let result = fetch_one_alias(
                &client,
                &cache_dir,
                &cached_aliases,
                &offline_flag,
                &locations,
                &cache_revision_tx,
                manifest_version,
                &normalized,
                defver,
            )
            .await;
            note_fetch(&fetch_failures, FetchKey::Alias { name: normalized, defver }, &result);
            (name, result)
        });
    }

    /// Spawn a single cap warm-up task onto `set`, cloning the `Arc` handles
    /// the atomic fetcher needs so it can run independently of `&self`.
    fn spawn_cap_warm(
        &self,
        set: &mut tokio::task::JoinSet<(String, Result<Cap, FabricRegistryError>)>,
        urn: String,
    ) {
        let client = self.client.clone();
        let cache_dir = self.cache_dir.clone();
        let cached_caps = Arc::clone(&self.cached_caps);
        let cached_media_defs = Arc::clone(&self.cached_media_defs);
        let cached_dims = Arc::clone(&self.cached_dims);
        let manifest = Arc::clone(&self.manifest);
        let offline_flag = Arc::clone(&self.offline_flag);
        let locations = self.locations.clone();
        let manifest_version = self.manifest_version;
        let cache_revision_tx = self.cache_revision_tx.clone();
        let fetch_failures = Arc::clone(&self.fetch_failures);
        set.spawn(async move {
            let normalized_urn = match normalize_cap_urn(&urn) {
                Ok(n) => n,
                Err(e) => return (urn, Err(e)),
            };
            let defver = match {
                let m = manifest.lock();
                m.ok().and_then(|m| m.caps.get(&normalized_urn).copied())
            } {
                Some(d) => d,
                None => {
                    return (
                        urn,
                        Err(FabricRegistryError::NotFound(format!(
                            "cap '{}' is not part of manifest v{}",
                            normalized_urn, manifest_version
                        ))),
                    )
                }
            };
            let result = fetch_one_cap_atomic(
                &client,
                &cache_dir,
                &cached_caps,
                &cached_media_defs,
                &cached_dims,
                &manifest,
                &offline_flag,
                &locations,
                manifest_version,
                &cache_revision_tx,
                &normalized_urn,
                defver,
            )
            .await;
            note_fetch(&fetch_failures, FetchKey::Cap { urn: normalized_urn, defver }, &result);
            (urn, result)
        });
    }

    /// Get all currently cached caps from in-memory cache.
    pub async fn get_cached_caps(&self) -> Result<Vec<Cap>, FabricRegistryError> {
        let cached_caps = self.cached_caps.lock().map_err(|e| {
            FabricRegistryError::CacheError(format!("Failed to lock cap cache: {}", e))
        })?;
        Ok(cached_caps.values().cloned().collect())
    }

    /// Narrow an ABSTRACT cap to the unique CONCRETE cap that handles a given
    /// input media (and optional target output). This is the CLI's dispatch
    /// step: the alias already resolved to the abstract cap (via `is_equivalent`
    /// — a resolution question); this asks the dispatch question ("which
    /// concrete cap can handle THIS input?") with `is_dispatchable`.
    ///
    /// The request is the abstract cap specialized on `input_media` (and, if
    /// given, `target` as the output). A concrete candidate matches iff its
    /// declared cap URN `is_dispatchable` for that request. Exactly one match →
    /// that cap; zero → `NoHandler`; more than one → `Ambiguous`.
    pub async fn narrow_abstract_cap(
        &self,
        abstract_urn: &crate::CapUrn,
        input_media: &crate::MediaUrn,
        target: Option<&crate::MediaUrn>,
    ) -> Result<crate::CapUrn, CapNarrowError> {
        let out_spec = target
            .map(|m| m.to_string())
            .unwrap_or_else(|| abstract_urn.out_spec().to_string());
        let request = abstract_urn
            .clone()
            .with_in_spec(input_media.to_string())
            .with_out_spec(out_spec);

        let all = self
            .get_cached_caps()
            .await
            .map_err(|e| CapNarrowError::Registry(e.to_string()))?;

        // Concrete candidates whose declared cap can legally handle the request.
        // Dedup by canonical URN string (a cap could appear once per cartridge).
        let mut seen = std::collections::BTreeSet::new();
        let mut matches: Vec<crate::CapUrn> = Vec::new();
        for cap in all.iter() {
            if cap.is_abstract {
                continue;
            }
            if cap.urn.is_dispatchable(&request) && seen.insert(cap.urn.to_string()) {
                matches.push(cap.urn.clone());
            }
        }

        match matches.len() {
            0 => Err(CapNarrowError::NoHandler {
                abstract_urn: abstract_urn.to_string(),
                input_media: input_media.to_string(),
                target: target.map(|m| m.to_string()),
            }),
            1 => Ok(matches.pop().unwrap()),
            _ => {
                matches.sort_by_key(|u| u.to_string());
                Err(CapNarrowError::Ambiguous {
                    abstract_urn: abstract_urn.to_string(),
                    candidates: matches.iter().map(|u| u.to_string()).collect(),
                })
            }
        }
    }

    /// Synchronous cap lookup that warms its own cache. See module docs.
    ///
    /// The cap, or why it is not at hand: `NotFound` when the registry's
    /// answer is that it has no such cap (the pinned manifest does not name
    /// it, or the registry answered 404); `FetchFailed` when a fetch was
    /// refused or got no answer — the registry's status, or the synchronous
    /// deadline running out, with the URL and the manifest version;
    /// `Pending` when the fetch has been handed to the background fetcher
    /// and nothing has failed yet; and the URN's own error when it does not
    /// parse.
    pub fn get_cached_cap(&self, urn: &str) -> Result<Cap, FabricRegistryError> {
        let normalized_urn = normalize_cap_urn(urn)?;
        if let Some(cap) = self
            .cached_caps
            .lock()
            .ok()
            .and_then(|m| m.get(&normalized_urn).cloned())
        {
            return Ok(cap);
        }
        let defver = self.cap_defver(&normalized_urn)?;
        let key = FetchKey::Cap {
            urn: normalized_urn.clone(),
            defver,
        };
        let (url, _) = cap_url_and_cache_path(&self.cache_dir, &self.locations, &normalized_urn, defver);
        self.fetch_synchronously(key, url, format!("cap '{normalized_urn}' (defver {defver})"), || {
            fetch_one_cap_atomic(
                &self.client,
                &self.cache_dir,
                &self.cached_caps,
                &self.cached_media_defs,
                &self.cached_dims,
                &self.manifest,
                &self.offline_flag,
                &self.locations,
                self.manifest_version,
                &self.cache_revision_tx,
                &normalized_urn,
                defver,
            )
        })
    }

    /// The synchronous half of `get_cached_cap` and `get_cached_media_def`,
    /// for a definition the manifest names and memory does not hold.
    ///
    /// On a multi-threaded runtime the fetch is made here, bounded by
    /// `SYNC_FETCH_DEADLINE`: its result is returned as it is, and a fetch
    /// that runs out of time is a `FetchFailed` saying so (and goes on in the
    /// background). Where the fetch cannot be waited on — no runtime, or a
    /// current-thread one — it is handed to the background fetcher, and what
    /// the last fetch of it came to is returned, or `Pending` when none has
    /// failed.
    fn fetch_synchronously<T, F, Fut>(
        &self,
        key: FetchKey,
        url: String,
        what: String,
        fetch: F,
    ) -> Result<T, FabricRegistryError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, FabricRegistryError>>,
    {
        let runtime = tokio::runtime::Handle::try_current()
            .ok()
            .filter(|r| matches!(r.runtime_flavor(), tokio::runtime::RuntimeFlavor::MultiThread));
        let Some(runtime) = runtime else {
            self.enqueue_for_background_fetch(key.clone());
            let last_failure = self
                .fetch_failures
                .lock()
                .ok()
                .and_then(|failures| failures.get(&key).cloned());
            return Err(last_failure.unwrap_or_else(|| {
                FabricRegistryError::Pending(format!(
                    "{what} is named by manifest v{} and is being fetched from {url} in the background",
                    self.manifest_version
                ))
            }));
        };
        let attempt = tokio::task::block_in_place(|| {
            runtime.block_on(async { tokio::time::timeout(SYNC_FETCH_DEADLINE, fetch()).await })
        });
        let result = attempt.unwrap_or_else(|_elapsed| {
            Err(FabricRegistryError::FetchFailed(FetchFailure {
                what,
                url,
                manifest_version: self.manifest_version,
                cause: FetchCause::DeadlineElapsed(SYNC_FETCH_DEADLINE),
            }))
        });
        note_fetch(&self.fetch_failures, key.clone(), &result);
        if result.is_err() {
            self.enqueue_for_background_fetch(key);
        }
        result
    }

    /// In-memory-only cap lookup for latency-critical planner sync.
    ///
    /// This never performs the bounded synchronous network fetch used by
    /// `get_cached_cap`. If the cap is missing, the caller can enqueue it
    /// for asynchronous cache hydration and rely on cache revision events to
    /// retry graph admission.
    pub fn get_cached_cap_in_memory(&self, urn: &str) -> Option<Cap> {
        // A malformed URN is not in the in-memory cache — return `None`, the
        // same graceful outcome as a cache miss. Log it (it usually means an
        // upstream bug handed us a non-canonical string) but never panic; this
        // is a latency-critical planner path that must degrade, not crash.
        let normalized_urn = match normalize_cap_urn(urn) {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(
                    target: "capdag::fabric::registry",
                    urn = %urn, error = %e,
                    "get_cached_cap_in_memory received a malformed URN; treating as not found"
                );
                return None;
            }
        };
        self.cached_caps
            .lock()
            .ok()
            .and_then(|m| m.get(&normalized_urn).cloned())
    }

    /// Request asynchronous hydration of a cap definition without waiting.
    pub fn request_cap_cache_hydration(&self, urn: &str) {
        // A malformed URN cannot be hydrated — there is nothing to enqueue, so
        // this is a graceful no-op (with a warning). Never panic: hydration is
        // a best-effort background request and a bad URN must not crash the app.
        let normalized_urn = match normalize_cap_urn(urn) {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(
                    target: "capdag::fabric::registry",
                    urn = %urn, error = %e,
                    "request_cap_cache_hydration received a malformed URN; nothing to hydrate"
                );
                return;
            }
        };
        if let Ok(defver) = self.cap_defver(&normalized_urn) {
            self.enqueue_for_background_fetch(FetchKey::Cap {
                urn: normalized_urn,
                defver,
            });
        }
    }

    /// Validate a local cap against its canonical definition.
    pub async fn validate_cap(&self, cap: &Cap) -> Result<(), FabricRegistryError> {
        let canonical_cap = self.get_cap(&cap.urn_string()).await?;
        // A cartridge responds to a SUBSET of the fabric cap's canonical alias
        // names (it may implement fewer names, never invent one the fabric does
        // not define). The fabric registry owns the full alias set; each alias a
        // cartridge declares must be one of the canonical aliases.
        let canonical_aliases: std::collections::BTreeSet<&String> =
            canonical_cap.get_aliases().iter().collect();
        let unknown: Vec<&String> = cap
            .get_aliases()
            .iter()
            .filter(|a| !canonical_aliases.contains(a))
            .collect();
        if !unknown.is_empty() {
            return Err(FabricRegistryError::ValidationError(format!(
                "Alias mismatch: {:?} not among the fabric cap's aliases {:?}",
                unknown,
                canonical_cap.get_aliases()
            )));
        }
        if cap.is_abstract() != canonical_cap.is_abstract() {
            return Err(FabricRegistryError::ValidationError(format!(
                "Abstract-flag mismatch. Local: {}, Canonical: {}",
                cap.is_abstract(),
                canonical_cap.is_abstract()
            )));
        }
        let local_stdin = cap.get_stdin_media_urn();
        let canonical_stdin = canonical_cap.get_stdin_media_urn();
        if local_stdin != canonical_stdin {
            return Err(FabricRegistryError::ValidationError(format!(
                "stdin mismatch. Local: {:?}, Canonical: {:?}",
                local_stdin, canonical_stdin
            )));
        }
        Ok(())
    }

    /// Whether the registry holds the cap (cached or online): `false` only
    /// when its answer is that it has none, and the error when it could not
    /// be asked — a refused fetch says nothing about whether the cap exists.
    pub async fn cap_exists(&self, urn: &str) -> Result<bool, FabricRegistryError> {
        match self.get_cap(urn).await {
            Ok(_) => Ok(true),
            Err(FabricRegistryError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Add caps to the in-memory cache. Test helper.
    ///
    /// Each cap is recorded in the manifest. If the cap's own
    /// `version` is 0, it is stamped to the registry's pinned manifest
    /// version (since v0 in this context means "the test forgot to set
    /// it" and the natural assignment is the snapshot we belong to).
    /// An explicitly-non-zero version is honored as-is — test fixtures
    /// can simulate cross-snapshot scenarios when they need to.
    /// Seed the warm alias cache directly, parallel to [`add_caps_to_cache`]
    /// for the alias registry: the given aliases then resolve via
    /// [`get_alias`](Self::get_alias) / [`resolve_alias`](Self::resolve_alias)
    /// without a network round-trip. The dev-cap conflict guard and tests use
    /// this to stage known `alias → target` mappings.
    pub fn add_aliases_to_cache(&self, aliases: Vec<StoredAlias>) {
        if let Ok(mut cache) = self.cached_aliases.lock() {
            for alias in aliases {
                let key = crate::fabric::alias::normalize_alias_name(&alias.name)
                    .unwrap_or_else(|_| alias.name.clone());
                cache.insert(key, alias);
            }
        }
    }

    pub fn add_caps_to_cache(&self, caps: Vec<Cap>) {
        let mut changed = false;
        let pin = self.manifest_version;
        let mut manifest_guard = self.manifest.lock().ok();
        if let Ok(mut cached_caps) = self.cached_caps.lock() {
            for mut cap in caps {
                let urn = cap.urn_string();
                // `urn` is the serialized form of this `Cap`'s own typed URN, so
                // it should round-trip. If it doesn't, the `Cap` is structurally
                // corrupt: skip it (with a warning) rather than cache it under a
                // raw, unresolvable key — and never panic, so one bad cap in a
                // batch can't crash the app or drop the rest of the batch.
                let normalized_urn = match normalize_cap_urn(&urn) {
                    Ok(n) => n,
                    Err(e) => {
                        tracing::warn!(
                            target: "capdag::fabric::registry",
                            urn = %urn, error = %e,
                            "add_caps_to_cache skipping a cap whose URN does not parse"
                        );
                        continue;
                    }
                };
                if cap.version == 0 && pin >= 1 {
                    cap.version = pin;
                }
                let cap_version = cap.version;
                if let Some(m) = manifest_guard.as_mut() {
                    m.caps.insert(normalized_urn.clone(), cap_version);
                }
                cached_caps.insert(normalized_urn, cap);
                changed = true;
            }
        }
        drop(manifest_guard);
        if changed {
            publish_cache_revision(&self.cache_revision_tx);
        }
    }

    // -------------------------------------------------------------------------
    // MEDIA-DEF API
    // -------------------------------------------------------------------------

    /// Get a media def from cache or fetch from registry.
    ///
    /// `urn` may be a media URN (`media:...`) or an **alias** (no `:`). An
    /// alias is resolved first; because this is the typed media boundary,
    /// an alias whose target is not a media URN is a hard error.
    pub async fn get_media_def(&self, urn: &str) -> Result<StoredMediaDef, FabricRegistryError> {
        if crate::is_alias_token(urn) {
            let target = self
                .resolve_alias_typed(urn, Some(AliasTargetKind::Media))
                .await?;
            return Box::pin(self.get_media_def(&target)).await;
        }
        let normalized = normalize_media_urn(urn)?;
        if let Some(spec) = self
            .cached_media_defs
            .lock()
            .ok()
            .and_then(|m| m.get(&normalized).cloned())
        {
            return Ok(spec);
        }
        let defver = self.media_defver(&normalized)?;
        fetch_one_media_def(
            &self.client,
            &self.cache_dir,
            &self.cached_media_defs,
            &self.cached_dims,
            &self.manifest,
            self.manifest_version,
            &self.offline_flag,
            &self.locations,
            &self.cache_revision_tx,
            &normalized,
            defver,
        )
        .await
    }

    /// Get multiple media defs at once.
    pub async fn get_media_defs(
        &self,
        urns: &[&str],
    ) -> Result<Vec<StoredMediaDef>, FabricRegistryError> {
        let mut specs = Vec::new();
        for urn in urns {
            specs.push(self.get_media_def(urn).await?);
        }
        Ok(specs)
    }

    /// Get all currently cached media defs.
    pub async fn get_cached_media_defs(&self) -> Result<Vec<StoredMediaDef>, FabricRegistryError> {
        let cached_specs = self.cached_media_defs.lock().map_err(|e| {
            FabricRegistryError::CacheError(format!("Failed to lock media-def cache: {}", e))
        })?;
        Ok(cached_specs.values().cloned().collect())
    }

    /// Synchronous media-def lookup that warms its own cache: the media
    /// def, or why it is not at hand, as `get_cached_cap` says.
    pub fn get_cached_media_def(&self, urn: &str) -> Result<StoredMediaDef, FabricRegistryError> {
        let normalized = normalize_media_urn(urn)?;
        if let Some(spec) = self
            .cached_media_defs
            .lock()
            .ok()
            .and_then(|m| m.get(&normalized).cloned())
        {
            return Ok(spec);
        }
        let defver = self.media_defver(&normalized)?;
        let key = FetchKey::Media {
            urn: normalized.clone(),
            defver,
        };
        let (url, _) = media_url_and_cache_path(&self.cache_dir, &self.locations, &normalized, defver);
        self.fetch_synchronously(key, url, format!("media def '{normalized}' (defver {defver})"), || {
            fetch_one_media_def(
                &self.client,
                &self.cache_dir,
                &self.cached_media_defs,
                &self.cached_dims,
                &self.manifest,
                self.manifest_version,
                &self.offline_flag,
                &self.locations,
                &self.cache_revision_tx,
                &normalized,
                defver,
            )
        })
    }

    /// Whether the URN is a bookend-eligible file format — its stored spec
    /// has at least one registered file extension — or why its spec is not
    /// at hand (`get_cached_media_def`).
    pub fn is_bookend(&self, urn: &str) -> Result<bool, FabricRegistryError> {
        Ok(!self.get_cached_media_def(urn)?.extensions.is_empty())
    }

    /// Snapshot of every bookend-eligible URN currently in the cache.
    pub fn bookend_urns(&self) -> std::collections::HashSet<crate::MediaUrn> {
        let cached = match self.cached_media_defs.lock() {
            Ok(g) => g,
            Err(_) => return Default::default(),
        };
        cached
            .values()
            .filter(|spec| !spec.extensions.is_empty())
            .filter_map(|spec| crate::MediaUrn::from_string(&spec.urn).ok())
            .collect()
    }

    /// Every LIVE-SOURCE definition the pinned MANIFEST declares: media
    /// defs in the live reference family (`is_live_feed`) with their
    /// CONTENT pairing from `metadata.content` — the urn a resolved feed of
    /// that reference delivers, and the urn PLANNING anchors at
    /// (`is_sequence=true`). Enumerated from the manifest's media listing
    /// (never the warm cache alone: live reference defs are referenced by
    /// NO cap, so cap-driven cache warming never loads them — a cache-only
    /// scan would silently report an empty catalog). Each def is fetched
    /// through `get_media_def`, which caches. Returns
    /// `(reference_urn, content_urn, title)` triples sorted by reference
    /// urn. A live def WITHOUT a content pairing is a fabric defect and is
    /// reported as an error rather than silently skipped.
    pub async fn live_source_defs(
        &self,
    ) -> Result<Vec<(String, String, String)>, FabricRegistryError> {
        // Snapshot the manifest's live-family urns first — the lock must
        // not be held across fetch awaits.
        let live_urns: Vec<String> = {
            let m = self.manifest.lock().map_err(|e| {
                FabricRegistryError::CacheError(format!("Failed to lock manifest: {}", e))
            })?;
            m.media
                .keys()
                .filter(|urn| {
                    crate::MediaUrn::from_string(urn)
                        .map(|u| u.is_live_feed())
                        .unwrap_or(false)
                })
                .cloned()
                .collect()
        };
        let mut out = Vec::new();
        for urn in live_urns {
            let spec = self.get_media_def(&urn).await?;
            let content = spec
                .metadata
                .as_ref()
                .and_then(|m| m.get("content"))
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    FabricRegistryError::CacheError(format!(
                        "live media def '{}' declares no metadata.content pairing — a live \
                         reference is unusable as a machine source without the content urn \
                         its feed delivers; fix the fabric definition",
                        spec.urn
                    ))
                })?;
            crate::MediaUrn::from_string(content).map_err(|e| {
                FabricRegistryError::CacheError(format!(
                    "live media def '{}' has an unparseable metadata.content '{}': {e}",
                    spec.urn, content
                ))
            })?;
            out.push((spec.urn.clone(), content.to_string(), spec.title.clone()));
        }
        out.sort();
        Ok(out)
    }

    /// The CONTENT urn paired with a live reference (`metadata.content` on
    /// the matching live media def, by urn equivalence). `Ok(None)` when no
    /// live def matches the reference.
    pub async fn live_source_content_urn(
        &self,
        reference_urn: &crate::MediaUrn,
    ) -> Result<Option<String>, FabricRegistryError> {
        for (def_urn, content, _) in self.live_source_defs().await? {
            if let Ok(u) = crate::MediaUrn::from_string(&def_urn) {
                if u.is_equivalent(reference_urn).unwrap_or(false) {
                    return Ok(Some(content));
                }
            }
        }
        Ok(None)
    }

    /// The cap a host asks to open a live reference of a CARTRIDGE's family
    /// (`metadata.opened_by` on the matching live media def, by urn
    /// equivalence; 13.2 §Reference Media, host resolution). `Ok(None)` when
    /// no live def matches the reference or the def names no opener — a
    /// device family is opened by the host itself. A named opener that is no
    /// cap URN, or no cap of this fabric, is a fabric defect and an error.
    pub async fn live_source_opened_by(
        &self,
        reference_urn: &crate::MediaUrn,
    ) -> Result<Option<crate::Cap>, FabricRegistryError> {
        for (def_urn, _, _) in self.live_source_defs().await? {
            let matches = crate::MediaUrn::from_string(&def_urn)
                .map(|u| u.is_equivalent(reference_urn).unwrap_or(false))
                .unwrap_or(false);
            if !matches {
                continue;
            }
            let spec = self.get_media_def(&def_urn).await?;
            let Some(opened_by) = spec.metadata.as_ref().and_then(|m| m.get("opened_by")) else {
                return Ok(None);
            };
            let opened_by = opened_by.as_str().ok_or_else(|| {
                FabricRegistryError::CacheError(format!(
                    "live media def '{}' has a metadata.opened_by that is not a cap URN string: \
                     {opened_by}",
                    spec.urn
                ))
            })?;
            let cap = self.get_cap(opened_by).await.map_err(|e| {
                FabricRegistryError::CacheError(format!(
                    "live media def '{}' names '{opened_by}' as the cap that opens it, and the \
                     fabric has no such cap: {e}",
                    spec.urn
                ))
            })?;
            return Ok(Some(cap));
        }
        Ok(None)
    }

    /// The file media types a file with this extension can be: every
    /// definition in the pinned manifest whose URN carries the `ext=`
    /// file-type tag and whose `extensions` list the extension (any case),
    /// in URN order. Empty when the fabric declares no file type for it.
    ///
    /// The first call loads every file media definition the manifest names
    /// (those not cached yet are fetched at once, and kept in this manifest
    /// version's cache); later calls answer from memory. A definition that
    /// cannot be had fails the call, naming each one and why: an answer
    /// from part of the fabric's file types would be a wrong answer.
    pub async fn file_media_for_extension(
        &self,
        extension: &str,
    ) -> Result<Vec<StoredMediaDef>, FabricRegistryError> {
        let index = self.file_media_index().await?;
        Ok(index.get(&extension.to_lowercase()).cloned().unwrap_or_default())
    }

    async fn file_media_index(&self) -> Result<Arc<FileMediaIndex>, FabricRegistryError> {
        if let Some(index) = self.loaded_file_media() {
            return Ok(index);
        }
        let _loading = self.file_media_loading.lock().await;
        if let Some(index) = self.loaded_file_media() {
            return Ok(index);
        }
        let file_urns: Vec<(String, u32)> = {
            let m = self.manifest.lock().map_err(|e| {
                FabricRegistryError::CacheError(format!("Failed to lock manifest: {}", e))
            })?;
            let mut file_urns = Vec::new();
            for (urn, defver) in m.media.iter() {
                let parsed = crate::MediaUrn::from_string(urn).map_err(|e| {
                    FabricRegistryError::FileTypes(format!(
                        "manifest v{} names '{urn}' as a media definition, and it is no media URN: {e}",
                        self.manifest_version
                    ))
                })?;
                if parsed.get_tag("ext").is_some() {
                    file_urns.push((urn.clone(), *defver));
                }
            }
            file_urns
        };
        let mut defs: Vec<StoredMediaDef> = Vec::with_capacity(file_urns.len());
        let mut missing: Vec<(String, u32)> = Vec::new();
        {
            let cached = self.cached_media_defs.lock().map_err(|e| {
                FabricRegistryError::CacheError(format!("Failed to lock media cache: {}", e))
            })?;
            for (urn, defver) in file_urns {
                match cached.get(&urn) {
                    Some(def) => defs.push(def.clone()),
                    None => missing.push((urn, defver)),
                }
            }
        }
        let mut failures: Vec<String> = Vec::new();
        let mut fetches = tokio::task::JoinSet::new();
        let gate = Arc::new(tokio::sync::Semaphore::new(FILE_MEDIA_FETCHES_AT_ONCE));
        for (urn, defver) in missing {
            let gate = Arc::clone(&gate);
            let client = self.client.clone();
            let cache_dir = self.cache_dir.clone();
            let cached_media_defs = Arc::clone(&self.cached_media_defs);
            let cached_dims = Arc::clone(&self.cached_dims);
            let manifest = Arc::clone(&self.manifest);
            let manifest_version = self.manifest_version;
            let offline_flag = Arc::clone(&self.offline_flag);
            let locations = self.locations.clone();
            let cache_revision_tx = self.cache_revision_tx.clone();
            fetches.spawn(async move {
                let _turn = gate.acquire_owned().await.expect("the fetch gate is never closed");
                let result = fetch_one_media_def(
                    &client,
                    &cache_dir,
                    &cached_media_defs,
                    &cached_dims,
                    &manifest,
                    manifest_version,
                    &offline_flag,
                    &locations,
                    &cache_revision_tx,
                    &urn,
                    defver,
                )
                .await;
                (urn, result)
            });
        }
        while let Some(joined) = fetches.join_next().await {
            match joined {
                Ok((_, Ok(def))) => defs.push(def),
                Ok((urn, Err(e))) => failures.push(format!("{urn}: {e}")),
                Err(e) => failures.push(format!("a fetch did not finish: {e}")),
            }
        }
        if !failures.is_empty() {
            failures.sort();
            return Err(FabricRegistryError::FileTypes(format!(
                "{} of the {} file media types of manifest v{} could not be loaded, so no file's \
                 type can be told from its extension: {}",
                failures.len(),
                failures.len() + defs.len(),
                self.manifest_version,
                failures.join("; ")
            )));
        }
        let mut index = FileMediaIndex::new();
        for def in defs {
            for ext in &def.extensions {
                index.entry(ext.to_lowercase()).or_default().push(def.clone());
            }
        }
        for defs in index.values_mut() {
            defs.sort_by(|a, b| a.urn.cmp(&b.urn));
            defs.dedup_by(|a, b| a.urn == b.urn);
        }
        let index = Arc::new(index);
        *self.file_media.lock().map_err(|e| {
            FabricRegistryError::CacheError(format!("Failed to lock the file media index: {}", e))
        })? = Some(Arc::clone(&index));
        Ok(index)
    }

    fn loaded_file_media(&self) -> Option<Arc<FileMediaIndex>> {
        self.file_media.lock().ok().and_then(|g| g.clone())
    }

    /// Name a media definition in the manifest without caching it: a test
    /// registry then fetches it from the registry's location when it is
    /// first needed. Test helper.
    pub fn list_media_in_manifest_for_test(&self, urn: &str, defver: u32) {
        let normalized = normalize_media_urn(urn).expect("a test lists a valid media URN");
        if let Ok(mut m) = self.manifest.lock() {
            m.media.insert(normalized, defver);
        }
    }

    /// Insert a media def into the in-memory cache. Test helper.
    ///
    /// Records the media def in the manifest. If the spec's own
    /// `version` is 0, it is stamped to the registry's pinned manifest
    /// version (same "test forgot to set it" handling as
    /// `add_caps_to_cache`).
    pub fn insert_cached_media_def_for_test(&self, mut spec: StoredMediaDef) {
        // `spec.urn` is the canonical URN of the media def being inserted. If it
        // does not parse, skip the insert (with a warning) rather than cache it
        // under a raw, unresolvable key — and never panic, so a malformed
        // fixture surfaces as the test failing on the missing def, and a bad URN
        // can never crash this `pub` method's (technically production-reachable)
        // caller.
        let normalized = match normalize_media_urn(&spec.urn) {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(
                    target: "capdag::fabric::registry",
                    urn = %spec.urn, error = %e,
                    "insert_cached_media_def_for_test skipping a media def whose URN does not parse"
                );
                return;
            }
        };
        let pin = self.manifest_version;
        if spec.version == 0 && pin >= 1 {
            spec.version = pin;
        }
        let spec_version = spec.version;
        if let Ok(mut cache) = self.cached_media_defs.lock() {
            cache.insert(normalized.clone(), spec.clone());
        }
        if let Ok(mut m) = self.manifest.lock() {
            m.media.insert(normalized, spec_version);
        }
        publish_cache_revision(&self.cache_revision_tx);
    }

    /// Whether the registry holds the media def (cached or online), as
    /// `cap_exists` answers it.
    pub async fn media_def_exists(&self, urn: &str) -> Result<bool, FabricRegistryError> {
        match self.get_media_def(urn).await {
            Ok(_) => Ok(true),
            Err(FabricRegistryError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    // -------------------------------------------------------------------------
    // SHARED ADMIN API
    // -------------------------------------------------------------------------

    /// The cache folder of the manifest version this registry is pinned at,
    /// `<os cache>/capdag/<charter slug>/v<N>`. Exposed so admin surfaces can
    /// report the folder they purge or renew.
    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    /// Invalidate every cache of this registry's manifest version: the
    /// in-memory maps and its whole folder on disk, INCLUDING the manifest.
    /// After this the next lookup (or a freshly constructed `FabricRegistry`)
    /// re-fetches the manifest and all bodies from the network, so this is
    /// the way to force a full renewal against a mutated channel. The folders
    /// of the layout before per-version folders are removed with it
    /// ([`clear_cache_folder`]).
    pub fn clear_cache(&self) -> Result<(), FabricRegistryError> {
        if let Ok(mut g) = self.cached_caps.lock() {
            g.clear();
        }
        if let Ok(mut g) = self.cached_media_defs.lock() {
            g.clear();
        }
        if let Ok(mut g) = self.cached_aliases.lock() {
            g.clear();
        }
        if let Ok(mut g) = self.cached_dims.lock() {
            g.clear();
        }
        if let Ok(mut g) = self.file_media.lock() {
            *g = None;
        }
        clear_cache_folder(&self.cache_dir)
    }

    /// Clear the disk cache of manifest `manifest_version` of the registry
    /// `config` names, without opening a registry on it — so a cache holding a
    /// corrupt entry, which a registry refuses to open, can still be cleared.
    /// The registry's charter is the stored one when it still verifies, else
    /// fetched, as a registry finds it. Answers the folder cleared.
    pub async fn clear_cache_of(
        config: &RegistryConfig,
        manifest_version: u32,
    ) -> Result<PathBuf, FabricRegistryError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| {
                FabricRegistryError::HttpError(format!("Failed to create HTTP client: {}", e))
            })?;
        let capdag_cache = Self::capdag_cache_root()?;
        let charter = resolve_charter(
            &client,
            &capdag_cache,
            &config.anchor,
            false,
            &crate::bifaci::trust::charter_trust(),
            mg_srel::unix_now(),
        )
        .await?;
        let cache_dir =
            cache::version_dir(&capdag_cache.join(&charter.charter.slug), manifest_version);
        clear_cache_folder(&cache_dir)?;
        Ok(cache_dir)
    }

    // -------------------------------------------------------------------------
    // QUEUE
    // -------------------------------------------------------------------------

    /// Look up an arbitrary URN's pinned defver under this registry's
    /// manifest. Public so external callers (e.g. fetchcartridge) can
    /// resolve URN → (urn, defver) before issuing a network request.
    pub fn cap_defver_for(&self, urn: &str) -> Result<u32, FabricRegistryError> {
        let normalized = normalize_cap_urn(urn)?;
        self.cap_defver(&normalized)
    }

    /// As `cap_defver_for` but for media URNs.
    pub fn media_defver_for(&self, urn: &str) -> Result<u32, FabricRegistryError> {
        let normalized = normalize_media_urn(urn)?;
        self.media_defver(&normalized)
    }

    fn enqueue_for_background_fetch(&self, key: FetchKey) {
        let Some(tx) = self.fetch_queue_tx.as_ref() else {
            return;
        };
        let mut in_queue = match self.fetch_in_queue.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        if !in_queue.insert(key.clone()) {
            return;
        }
        if let Err(e) = tx.send(key.clone()) {
            in_queue.remove(&key);
            tracing::warn!(
                target: "capdag::fabric::registry",
                key = ?key, error = %e,
                "Background fetch queue send failed (consumer task is gone); dropping URN."
            );
        }
    }


    // -------------------------------------------------------------------------
    // TEST HELPERS
    // -------------------------------------------------------------------------

    /// Synchronous test constructor with a fresh empty cache. Pins the
    /// registry at v1 with an empty manifest, so test helpers like
    /// `add_caps_to_cache` flow caps into the manifest at their declared
    /// version. Spawns a fetch consumer when called inside a tokio
    /// runtime; otherwise leaves the queue inert.
    pub fn new_for_test() -> Self {
        Self::new_for_test_with_config(RegistryConfig::default())
    }

    /// Test constructor with custom config; pins at v1.
    pub fn new_for_test_with_config(config: RegistryConfig) -> Self {
        Self::new_for_test_with_config_and_version(config, 1)
    }

    /// Full test constructor: custom config + explicit pinned manifest
    /// version. Builds an empty manifest at that version; no network.
    pub fn new_for_test_with_config_and_version(
        config: RegistryConfig,
        manifest_version: u32,
    ) -> Self {
        // The charter a published fabric at this anchor carries. It is not
        // signed: nothing is fetched from it, and a test registry is the one
        // registry that is not resolved through `resolve_charter`.
        let charter = VerifiedCharter {
            charter: publisher_layout_charter(&config.anchor),
            body: Vec::new(),
            envelope: Vec::new(),
        };
        let locations = FabricLocations::from_charter(&charter.charter)
            .expect("the test charter is a fabric charter with both locations");
        let cache_dir = cache::version_dir(Path::new("/tmp/capdag-test-cache"), manifest_version);
        for kind in cache::KIND_FOLDERS {
            let _ = fs::create_dir_all(cache_dir.join(kind));
        }
        let cached_caps = Arc::new(Mutex::new(HashMap::new()));
        let cached_media_defs = Arc::new(Mutex::new(HashMap::new()));
        let cached_aliases = Arc::new(Mutex::new(HashMap::new()));
        let cached_dims = Arc::new(Mutex::new(HashMap::new()));
        let manifest_arc = Arc::new(Mutex::new(Manifest::empty(manifest_version)));
        let fetch_in_queue = Arc::new(Mutex::new(HashSet::new()));
        let fetch_failures: FetchFailures = Arc::new(Mutex::new(HashMap::new()));
        let offline_flag = Arc::new(AtomicBool::new(false));
        let client = reqwest::Client::new();
        let (cache_revision_tx, _) = watch::channel(0u64);

        let fetch_queue_tx = match tokio::runtime::Handle::try_current() {
            Ok(_) => {
                let (tx, rx) = mpsc::unbounded_channel::<FetchKey>();
                tokio::spawn(run_fetch_consumer(
                    rx,
                    client.clone(),
                    cache_dir.clone(),
                    Arc::clone(&cached_caps),
                    Arc::clone(&cached_media_defs),
                    Arc::clone(&cached_aliases),
                    Arc::clone(&cached_dims),
                    Arc::clone(&manifest_arc),
                    Arc::clone(&fetch_in_queue),
                    Arc::clone(&fetch_failures),
                    Arc::clone(&offline_flag),
                    locations.clone(),
                    cache_revision_tx.clone(),
                ));
                Some(tx)
            }
            Err(_) => None,
        };

        let registry = Self {
            client,
            cache_dir,
            cached_caps,
            cached_media_defs,
            cached_aliases,
            cached_dims,
            file_media: Arc::new(Mutex::new(None)),
            file_media_loading: Arc::new(tokio::sync::Mutex::new(())),
            config,
            charter: Arc::new(charter),
            locations,
            manifest_version,
            manifest: manifest_arc,
            offline_flag,
            fetch_queue_tx,
            fetch_in_queue,
            fetch_failures,
            cache_revision_tx,
        };
        registry.ensure_identity_cap();
        registry
    }
}

// =============================================================================
// DISK LOAD
// =============================================================================

/// The definitions a manifest version's cache folder holds at the versions
/// its manifest pins, keyed as the registry keys them in memory.
#[derive(Debug, Default)]
pub(crate) struct PinnedEntries {
    pub(crate) caps: HashMap<String, Cap>,
    pub(crate) media: HashMap<String, StoredMediaDef>,
    pub(crate) aliases: HashMap<String, StoredAlias>,
    pub(crate) dims: HashMap<String, StoredDim>,
}

/// Read, from the cache folder `cache_dir` of manifest `manifest`, every
/// definition the manifest pins that is cached: each from the one file its
/// key and pinned defver name, so nothing at another defver, and nothing
/// another manifest version cached, is ever read.
///
/// An entry that is not there is not cached. One that is there and cannot be
/// read, does not parse, or is not the definition and defver its path names
/// is corrupt, and an error naming the file. Two rules drop an entry and leave
/// it to be fetched anew, naming its file in the log:
/// - a defver-0 cap or media def older than its time to live (defver 0 is the
///   flat, mutable regime);
/// - a cap or dim that breaks the definition rules was written by a capdag
///   with other rules (`capdag/docs/10-validation-rules.md`).
pub(crate) fn load_pinned_entries(
    cache_dir: &Path,
    manifest: &Manifest,
) -> Result<PinnedEntries, FabricRegistryError> {
    let mut entries = PinnedEntries::default();
    for (urn, &defver) in &manifest.caps {
        let path = cache::entry_path(cache_dir, "caps", urn, defver);
        let Some(entry) = cache::read_entry::<CapCacheEntry>(&path, "cap")? else {
            continue;
        };
        let cached_urn = normalize_cap_urn(&entry.definition.urn_string())
            .map_err(|e| cache::corrupt(&path, "cap", &e.to_string()))?;
        if &cached_urn != urn || entry.definition.version != defver {
            return Err(cache::corrupt(
                &path,
                "cap",
                &format!(
                    "it holds '{}' at defver {}, where '{}' at defver {} belongs",
                    cached_urn, entry.definition.version, urn, defver
                ),
            ));
        }
        if defver == 0 && entry.is_expired() {
            tracing::info!("the cached cap {} is past its time to live", path.display());
            continue;
        }
        if let Err(e) = crate::cap::validation::validate_cap_definition(&entry.definition) {
            tracing::warn!("dropping the cached cap {}: {}", path.display(), e);
            let _ = fs::remove_file(&path);
            continue;
        }
        entries.caps.insert(cached_urn, entry.definition);
    }
    for (urn, &defver) in &manifest.media {
        let path = cache::entry_path(cache_dir, "media", urn, defver);
        let Some(entry) = cache::read_entry::<MediaCacheEntry>(&path, "media def")? else {
            continue;
        };
        let cached_urn = normalize_media_urn(&entry.spec.urn)
            .map_err(|e| cache::corrupt(&path, "media def", &e.to_string()))?;
        if &cached_urn != urn || entry.spec.version != defver {
            return Err(cache::corrupt(
                &path,
                "media def",
                &format!(
                    "it holds '{}' at defver {}, where '{}' at defver {} belongs",
                    cached_urn, entry.spec.version, urn, defver
                ),
            ));
        }
        if defver == 0 && entry.is_expired() {
            tracing::info!("the cached media def {} is past its time to live", path.display());
            continue;
        }
        entries.media.insert(cached_urn, entry.spec);
    }
    for (name, &defver) in &manifest.aliases {
        let path = cache::entry_path(cache_dir, "aliases", name, defver);
        let Some(entry) = cache::read_entry::<AliasCacheEntry>(&path, "alias")? else {
            continue;
        };
        validate_fetched_alias(&entry.alias, name, defver)
            .map_err(|e| cache::corrupt(&path, "alias", &e.to_string()))?;
        entries.aliases.insert(entry.alias.name.clone(), entry.alias);
    }
    for (name, &defver) in &manifest.dims {
        let path = cache::entry_path(cache_dir, "dims", name, defver);
        let Some(entry) = cache::read_entry::<DimCacheEntry>(&path, "dim")? else {
            continue;
        };
        if &entry.dim.name != name || entry.dim.version != defver {
            return Err(cache::corrupt(
                &path,
                "dim",
                &format!(
                    "it holds '{}' at defver {}, where '{}' at defver {} belongs",
                    entry.dim.name, entry.dim.version, name, defver
                ),
            ));
        }
        if let Err(e) = entry.dim.validate() {
            tracing::warn!("dropping the cached dim {}: {}", path.display(), e);
            let _ = fs::remove_file(&path);
            continue;
        }
        entries.dims.insert(entry.dim.name.clone(), entry.dim);
    }
    Ok(entries)
}

// =============================================================================
// ATOMIC FETCH HELPERS (free functions)
// =============================================================================

/// The URL of a cap at the given defver, from the charter's `definition`
/// location, and the cache file it is kept in.
fn cap_url_and_cache_path(
    cache_dir: &Path,
    locations: &FabricLocations,
    normalized_urn: &str,
    defver: u32,
) -> (String, PathBuf) {
    (
        locations.definition("caps", &cache::key_hash(normalized_urn), defver),
        cache::entry_path(cache_dir, "caps", normalized_urn, defver),
    )
}

/// The URL of a media definition at the given defver, from the charter's
/// `definition` location, and the cache file it is kept in.
fn media_url_and_cache_path(
    cache_dir: &Path,
    locations: &FabricLocations,
    normalized_urn: &str,
    defver: u32,
) -> (String, PathBuf) {
    (
        locations.definition("media", &cache::key_hash(normalized_urn), defver),
        cache::entry_path(cache_dir, "media", normalized_urn, defver),
    )
}

/// One registry object's whole body and its status, the request repeated
/// when it fails on the way (`net_retry::retry_body`): a connection refused,
/// a timeout, a 503, or a body that broke off part-way — which a test guest's
/// manifest fetch did, failing the cartridge build it was for.
async fn fetch_body(
    client: &reqwest::Client,
    url: &str,
    label: &str,
) -> reqwest::Result<(reqwest::StatusCode, Vec<u8>)> {
    crate::net_retry::retry_body(&crate::net_retry::RetryPolicy::default(), label, || {
        client.get(url).send()
    })
    .await
}

/// The body of one registry object — `what` at `url`, fetched for fabric
/// manifest `manifest_version`.
///
/// A success is the body. 404 or 410 is the registry's answer that it does
/// not hold the object, and only that is `NotFound`. Any other status, and a
/// fetch that got no answer, is `FetchFailed`, saying the URL, the status or
/// what happened on the connection, and the manifest version: a refused fetch
/// is never reported as a missing definition.
async fn fetch_object(
    client: &reqwest::Client,
    url: &str,
    what: &str,
    manifest_version: u32,
) -> Result<Vec<u8>, FabricRegistryError> {
    let (status, body) = fetch_body(client, url, &format!("fetch {what}"))
        .await
        .map_err(|e| {
            FabricRegistryError::FetchFailed(FetchFailure::transport(what, url, manifest_version, &e))
        })?;
    if status.is_success() {
        return Ok(body);
    }
    if matches!(status, reqwest::StatusCode::NOT_FOUND | reqwest::StatusCode::GONE) {
        return Err(FabricRegistryError::NotFound(format!(
            "the registry has no {what} for fabric manifest v{manifest_version} (HTTP {status} from {url})"
        )));
    }
    Err(FabricRegistryError::FetchFailed(FetchFailure {
        what: what.to_string(),
        url: url.to_string(),
        manifest_version,
        cause: FetchCause::Status(status.as_u16()),
    }))
}

/// A cap definition as a registry served it: parsed, then held to the
/// definition rules (`capdag/docs/10-validation-rules.md`).
///
/// The publisher holds a definition to the rules before it is served; a
/// registry is still another program's word, so it is checked on arrival and
/// never cached or returned unchecked.
fn checked_cap_definition(body: &[u8], normalized_urn: &str) -> Result<Cap, FabricRegistryError> {
    let cap: Cap = serde_json::from_slice(body).map_err(|e| {
        FabricRegistryError::ParseError(format!("Failed to parse cap '{}': {}", normalized_urn, e))
    })?;
    crate::cap::validation::validate_cap_definition(&cap)
        .map_err(|e| FabricRegistryError::ValidationError(e.to_string()))?;
    Ok(cap)
}

/// Build the R2 URL for a per-alias object at the given defver. Aliases
/// are keyed by `sha256(normalized_name)` and are versioned-only (defver
/// >= 1); there is no v0 flat path.
fn alias_url_and_cache_path(
    cache_dir: &Path,
    locations: &FabricLocations,
    normalized_name: &str,
    defver: u32,
) -> (String, PathBuf) {
    (
        locations.definition("aliases", &cache::key_hash(normalized_name), defver),
        cache::entry_path(cache_dir, "aliases", normalized_name, defver),
    )
}

/// Fetch a single alias body at its pinned defver, validate it, and cache
/// it in memory + on disk. The fetched body's `name` and `version` must
/// match what was requested (a registry that serves a mismatched object
/// is a hard error, never silently accepted), and the `target` must parse
/// as a cap or media URN.
#[allow(clippy::too_many_arguments)]
async fn fetch_one_alias(
    client: &reqwest::Client,
    cache_dir: &Path,
    cached_aliases: &Arc<Mutex<HashMap<String, StoredAlias>>>,
    offline_flag: &Arc<AtomicBool>,
    locations: &FabricLocations,
    cache_revision_tx: &watch::Sender<u64>,
    manifest_version: u32,
    normalized_name: &str,
    defver: u32,
) -> Result<StoredAlias, FabricRegistryError> {
    if offline_flag.load(Ordering::Relaxed) {
        return Err(FabricRegistryError::NetworkBlocked(format!(
            "offline: cannot fetch alias '{}'",
            normalized_name
        )));
    }
    if defver < 1 {
        return Err(FabricRegistryError::NotFound(format!(
            "alias '{}' has non-positive defver {}; aliases are versioned-only",
            normalized_name, defver
        )));
    }
    let (url, cache_path) = alias_url_and_cache_path(cache_dir, locations, normalized_name, defver);
    let body = fetch_object(
        client,
        &url,
        &format!("alias '{normalized_name}' (defver {defver})"),
        manifest_version,
    )
    .await?;
    let alias: StoredAlias = serde_json::from_slice(&body).map_err(|e| {
        FabricRegistryError::ParseError(format!(
            "Failed to parse alias '{}': {}",
            normalized_name, e
        ))
    })?;
    validate_fetched_alias(&alias, normalized_name, defver)?;
    cache_alias_entry(&alias, &cache_path, cached_aliases, cache_revision_tx)?;
    Ok(alias)
}

/// Shared validation for an alias body fetched from the registry or
/// hydrated from cache: name and version must match the request, and the
/// target must classify as a cap or media URN.
fn validate_fetched_alias(
    alias: &StoredAlias,
    expected_name: &str,
    expected_defver: u32,
) -> Result<(), FabricRegistryError> {
    if alias.name != expected_name {
        return Err(FabricRegistryError::ParseError(format!(
            "alias object name '{}' does not match requested name '{}'",
            alias.name, expected_name
        )));
    }
    if alias.version != expected_defver {
        return Err(FabricRegistryError::ParseError(format!(
            "alias '{}' object reports version {} but manifest pins defver {}",
            alias.name, alias.version, expected_defver
        )));
    }
    if classify_alias_target(&alias.target).is_none() {
        return Err(FabricRegistryError::ValidationError(format!(
            "alias '{}' target '{}' is neither a cap nor a media URN",
            alias.name, alias.target
        )));
    }
    Ok(())
}

/// Write an alias entry to the in-memory cache and the on-disk cache,
/// publishing a cache-revision bump.
fn cache_alias_entry(
    alias: &StoredAlias,
    cache_path: &Path,
    cached_aliases: &Arc<Mutex<HashMap<String, StoredAlias>>>,
    cache_revision_tx: &watch::Sender<u64>,
) -> Result<(), FabricRegistryError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let entry = AliasCacheEntry {
        alias: alias.clone(),
        cached_at: now,
        ttl_hours: CACHE_DURATION_HOURS,
    };
    cache::store_entry(cache_path, &entry, "alias")?;
    if let Ok(mut guard) = cached_aliases.lock() {
        guard.insert(alias.name.clone(), alias.clone());
    }
    publish_cache_revision(cache_revision_tx);
    Ok(())
}

/// Build the R2 URL for a per-dim object at the given defver. Dims are keyed
/// by `sha256(name)` and are versioned-only (defver >= 1).
fn dim_url_and_cache_path(
    cache_dir: &Path,
    locations: &FabricLocations,
    name: &str,
    defver: u32,
) -> (String, PathBuf) {
    (
        locations.definition("dims", &cache::key_hash(name), defver),
        cache::entry_path(cache_dir, "dims", name, defver),
    )
}

/// Fetch a single dim body at its pinned defver, check it, and cache it in
/// memory and on disk. The fetched body's `name` and `version` must match
/// what was requested — a registry that serves another object is a hard
/// error, never silently accepted — and the body must hold
/// ([`StoredDim::validate`]).
#[allow(clippy::too_many_arguments)]
async fn fetch_one_dim(
    client: &reqwest::Client,
    cache_dir: &Path,
    cached_dims: &Arc<Mutex<HashMap<String, StoredDim>>>,
    offline_flag: &Arc<AtomicBool>,
    locations: &FabricLocations,
    cache_revision_tx: &watch::Sender<u64>,
    manifest_version: u32,
    name: &str,
    defver: u32,
) -> Result<StoredDim, FabricRegistryError> {
    if offline_flag.load(Ordering::Relaxed) {
        return Err(FabricRegistryError::NetworkBlocked(format!(
            "offline: cannot fetch dim '{}'",
            name
        )));
    }
    if defver < 1 {
        return Err(FabricRegistryError::NotFound(format!(
            "dim '{}' has non-positive defver {}; dims are versioned-only",
            name, defver
        )));
    }
    let (url, cache_path) = dim_url_and_cache_path(cache_dir, locations, name, defver);
    let body = fetch_object(
        client,
        &url,
        &format!("dim '{name}' (defver {defver})"),
        manifest_version,
    )
    .await?;
    let dim = checked_dim(&body, name, defver)?;
    let entry = DimCacheEntry {
        dim: dim.clone(),
        cached_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        ttl_hours: CACHE_DURATION_HOURS,
    };
    cache::store_entry(&cache_path, &entry, "dim")?;
    if let Ok(mut guard) = cached_dims.lock() {
        guard.insert(dim.name.clone(), dim.clone());
    }
    publish_cache_revision(cache_revision_tx);
    Ok(dim)
}

/// A dim body as a registry served it, for the dim `name` at `defver`:
/// parsed (unknown fields refused), named and versioned as requested, and
/// holding ([`StoredDim::validate`]).
fn checked_dim(body: &[u8], name: &str, defver: u32) -> Result<StoredDim, FabricRegistryError> {
    let dim: StoredDim = serde_json::from_slice(body).map_err(|e| {
        FabricRegistryError::ParseError(format!("Failed to parse dim '{}': {}", name, e))
    })?;
    if dim.name != name {
        return Err(FabricRegistryError::ParseError(format!(
            "dim object name '{}' does not match requested name '{}'",
            dim.name, name
        )));
    }
    if dim.version != defver {
        return Err(FabricRegistryError::ParseError(format!(
            "dim '{}' object reports version {} but manifest pins defver {}",
            dim.name, dim.version, defver
        )));
    }
    dim.validate()
        .map_err(|e| FabricRegistryError::ValidationError(e.to_string()))?;
    Ok(dim)
}

/// Keep, of what was loaded from the disk cache, only what the pinned
/// manifest names at the version it names, and only definitions whose dims
/// all survive with them. At v0 the manifest is empty and caps and media are
/// all retained (the loaders only walk flat paths then); aliases and dims are
/// versioned-regime concepts and are all dropped.
///
/// A definition whose dims did not survive is dropped here and fetched again,
/// with its dims, when it is next asked for — so a cached definition always
/// has its dims cached.
fn retain_pinned(
    manifest: &Manifest,
    manifest_version: u32,
    caps: &mut HashMap<String, Cap>,
    specs: &mut HashMap<String, StoredMediaDef>,
    aliases: &mut HashMap<String, StoredAlias>,
    dims: &mut HashMap<String, StoredDim>,
) {
    if manifest_version >= 1 {
        caps.retain(|urn, cap| manifest.caps.get(urn).copied().unwrap_or(0) == cap.version);
        specs.retain(|urn, spec| manifest.media.get(urn).copied().unwrap_or(0) == spec.version);
        aliases.retain(|name, alias| manifest.aliases.get(name).copied().unwrap_or(0) == alias.version);
        dims.retain(|name, dim| manifest.dims.get(name).copied().unwrap_or(0) == dim.version);
    } else {
        aliases.clear();
        dims.clear();
    }
    caps.retain(|_, cap| dims_resolve_in(&cap.dims, dims));
    specs.retain(|_, spec| dims_resolve_in(&spec.dims, dims));
}

/// Whether every dim `dims` names is in `cached` with the value it names.
fn dims_resolve_in(dims: &[DimRef], cached: &HashMap<String, StoredDim>) -> bool {
    dims.iter().all(|r| {
        cached
            .get(&r.dim)
            .map(|d| d.value(&r.value).is_some())
            .unwrap_or(false)
    })
}

/// Make sure every dim a definition names is cached, fetching the ones that
/// are not at the defver the manifest pins, and that each names a value the
/// dim declares. `owner` names the definition in errors. Nothing about the
/// definition itself is cached here: its fetcher caches it only once this
/// succeeds.
#[allow(clippy::too_many_arguments)]
async fn ensure_dims_cached(
    owner: &str,
    dims: &[DimRef],
    client: &reqwest::Client,
    cache_dir: &Path,
    cached_dims: &Arc<Mutex<HashMap<String, StoredDim>>>,
    manifest: &Arc<Mutex<Manifest>>,
    manifest_version: u32,
    offline_flag: &Arc<AtomicBool>,
    locations: &FabricLocations,
    cache_revision_tx: &watch::Sender<u64>,
) -> Result<(), FabricRegistryError> {
    crate::fabric::dim::check_dim_refs(dims)
        .map_err(|e| FabricRegistryError::ValidationError(format!("{} dims: {}", owner, e)))?;
    for r in dims {
        let cached = cached_dims.lock().ok().and_then(|m| m.get(&r.dim).cloned());
        let dim = match cached {
            Some(dim) => dim,
            None => {
                if manifest_version == 0 {
                    return Err(FabricRegistryError::NotFound(format!(
                        "{} names dim '{}', but the registry is pinned at v0 (dims are a versioned-regime concept)",
                        owner, r.dim
                    )));
                }
                let defver = match manifest.lock() {
                    Ok(m) => m.dims.get(&r.dim).copied(),
                    Err(e) => {
                        return Err(FabricRegistryError::CacheError(format!(
                            "failed to lock manifest while resolving dims: {}",
                            e
                        )))
                    }
                };
                let defver = defver.ok_or_else(|| {
                    FabricRegistryError::NotFound(format!(
                        "{} names dim '{}', which is not in manifest v{}",
                        owner, r.dim, manifest_version
                    ))
                })?;
                // The dim's own failure, as it is (see the media footprint
                // in `fetch_one_cap_atomic`).
                fetch_one_dim(
                    client,
                    cache_dir,
                    cached_dims,
                    offline_flag,
                    locations,
                    cache_revision_tx,
                    manifest_version,
                    &r.dim,
                    defver,
                )
                .await?
            }
        };
        if dim.value(&r.value).is_none() {
            return Err(FabricRegistryError::ValidationError(format!(
                "{} names value '{}' of dim '{}', which the dim does not declare",
                owner, r.value, r.dim
            )));
        }
    }
    Ok(())
}

/// Every media URN a cap definition references, normalized, each once: its
/// `in=` and `out=`, every arg's URN and the type it accepts (its stdin
/// source's, or its `accepts`), and every output's URN — the main one and
/// the others alike, since each is the type of a node a machine can hold.
/// The wildcard `media:` names no definition and is left out.
///
/// A malformed URN means the definition is corrupt: it is an error, never a
/// URN quietly skipped.
fn referenced_media_urns(cap: &Cap) -> Result<Vec<String>, FabricRegistryError> {
    let mut referenced: Vec<String> = Vec::new();
    let mut push = |urn: &str| -> Result<(), FabricRegistryError> {
        let normalized = normalize_media_urn(urn)?;
        if normalized != "media:" && !referenced.contains(&normalized) {
            referenced.push(normalized);
        }
        Ok(())
    };
    push(cap.urn.in_spec())?;
    push(cap.urn.out_spec())?;
    for arg in &cap.args {
        push(&arg.media_urn)?;
        push(arg.accepted_urn())?;
    }
    for output in cap.get_outputs() {
        push(&output.media_urn)?;
    }
    Ok(referenced)
}

/// Atomic cap fetcher. Fetches the cap body, then ensures every media URN
/// it references is in the media cache. Caches the cap only on full
/// success; otherwise returns `Err` and writes nothing.
///
/// At pin >= 1 the referenced media URN footprint is resolved against
/// the manifest so each referenced URN is fetched at its pinned defver.
/// If a referenced URN is absent from the manifest the fetch fails —
/// snapshots are required to be self-consistent.
#[allow(clippy::too_many_arguments)]
async fn fetch_one_cap_atomic(
    client: &reqwest::Client,
    cache_dir: &Path,
    cached_caps: &Arc<Mutex<HashMap<String, Cap>>>,
    cached_media_defs: &Arc<Mutex<HashMap<String, StoredMediaDef>>>,
    cached_dims: &Arc<Mutex<HashMap<String, StoredDim>>>,
    manifest: &Arc<Mutex<Manifest>>,
    offline_flag: &Arc<AtomicBool>,
    locations: &FabricLocations,
    manifest_version: u32,
    cache_revision_tx: &watch::Sender<u64>,
    normalized_urn: &str,
    defver: u32,
) -> Result<Cap, FabricRegistryError> {
    if offline_flag.load(Ordering::Relaxed) {
        return Err(FabricRegistryError::NetworkBlocked(format!(
            "Network access blocked by policy — cannot fetch cap '{}'",
            normalized_urn
        )));
    }

    let (url, cache_file) = cap_url_and_cache_path(cache_dir, locations, normalized_urn, defver);

    let body = fetch_object(
        client,
        &url,
        &format!("cap '{normalized_urn}' (defver {defver})"),
        manifest_version,
    )
    .await?;
    let cap = checked_cap_definition(&body, normalized_urn)?;

    let referenced = referenced_media_urns(&cap)?;

    for media_urn in &referenced {
        let already_cached = cached_media_defs
            .lock()
            .ok()
            .map(|m| m.contains_key(media_urn))
            .unwrap_or(false);
        if already_cached {
            continue;
        }
        // Resolve the referenced media URN's defver under the manifest.
        // At v0 every URN maps to defver 0 (flat path).
        let media_defver = if manifest_version == 0 {
            0
        } else {
            match manifest.lock() {
                Ok(m) => match m.media.get(media_urn).copied() {
                    Some(v) => v,
                    None => {
                        return Err(FabricRegistryError::NotFound(format!(
                            "cap '{}' references media URN '{}' which is not in manifest v{}",
                            normalized_urn, media_urn, manifest_version
                        )));
                    }
                },
                Err(e) => {
                    return Err(FabricRegistryError::CacheError(format!(
                        "failed to lock manifest while resolving referenced media: {}",
                        e
                    )));
                }
            }
        };
        if let Err(e) = fetch_one_media_def(
            client,
            cache_dir,
            cached_media_defs,
            cached_dims,
            manifest,
            manifest_version,
            offline_flag,
            locations,
            cache_revision_tx,
            media_urn,
            media_defver,
        )
        .await
        {
            tracing::warn!(
                target: "capdag::fabric::registry",
                cap_urn = %normalized_urn,
                missing_media_urn = %media_urn,
                error = %e,
                "Aborting cap cache write: a referenced media def could not be fetched. \
                 The cap is NOT cached so the next attempt re-tries cleanly."
            );
            // The media def's own failure, as it is: a refused fetch stays a
            // refused fetch, and only the registry's 404 is a NotFound.
            return Err(e);
        }
    }

    // Every dim the cap names, at its pinned version, with the value named.
    ensure_dims_cached(
        &format!("cap '{}'", normalized_urn),
        &cap.dims,
        client,
        cache_dir,
        cached_dims,
        manifest,
        manifest_version,
        offline_flag,
        locations,
        cache_revision_tx,
    )
    .await?;

    // All referenced media defs and dims in cache. Write the cap.
    let cache_entry = CapCacheEntry {
        definition: cap.clone(),
        cached_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        ttl_hours: CACHE_DURATION_HOURS,
    };
    cache::store_entry(&cache_file, &cache_entry, "cap")?;

    if let Ok(mut cached) = cached_caps.lock() {
        cached.insert(normalized_urn.to_string(), cap.clone());
    }
    publish_cache_revision(cache_revision_tx);

    Ok(cap)
}

/// Atomic media-def fetcher: the media def is cached only once every dim it
/// names is cached (see `ensure_dims_cached`).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn fetch_one_media_def(
    client: &reqwest::Client,
    cache_dir: &Path,
    cached_media_defs: &Arc<Mutex<HashMap<String, StoredMediaDef>>>,
    cached_dims: &Arc<Mutex<HashMap<String, StoredDim>>>,
    manifest: &Arc<Mutex<Manifest>>,
    manifest_version: u32,
    offline_flag: &Arc<AtomicBool>,
    locations: &FabricLocations,
    cache_revision_tx: &watch::Sender<u64>,
    normalized_urn: &str,
    defver: u32,
) -> Result<StoredMediaDef, FabricRegistryError> {
    if offline_flag.load(Ordering::Relaxed) {
        return Err(FabricRegistryError::NetworkBlocked(format!(
            "Network access blocked by policy — cannot fetch media def '{}'",
            normalized_urn
        )));
    }

    let (url, cache_file) = media_url_and_cache_path(cache_dir, locations, normalized_urn, defver);

    let body = fetch_object(
        client,
        &url,
        &format!("media def '{normalized_urn}' (defver {defver})"),
        manifest_version,
    )
    .await?;
    let spec: StoredMediaDef = serde_json::from_slice(&body).map_err(|e| {
        FabricRegistryError::ParseError(format!(
            "Failed to parse media def '{}': {}",
            normalized_urn, e
        ))
    })?;

    ensure_dims_cached(
        &format!("media def '{}'", normalized_urn),
        &spec.dims,
        client,
        cache_dir,
        cached_dims,
        manifest,
        manifest_version,
        offline_flag,
        locations,
        cache_revision_tx,
    )
    .await?;

    let cache_entry = MediaCacheEntry {
        spec: spec.clone(),
        cached_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        ttl_hours: CACHE_DURATION_HOURS,
    };
    cache::store_entry(&cache_file, &cache_entry, "media def")?;

    if let Ok(mut cached) = cached_media_defs.lock() {
        cached.insert(normalized_urn.to_string(), spec.clone());
    }
    publish_cache_revision(cache_revision_tx);
    Ok(spec)
}

/// Manifest bootstrap, in the cache folder `cache_dir` of manifest
/// `version`. Tries the cached manifest first; falls back to a blocking
/// network GET; if neither produces a manifest, returns an error — there is
/// no v0 fallback (caller chose v >= 1 explicitly). A cached manifest that
/// does not parse, or is another version's, is corrupt: an error naming its
/// file.
async fn load_or_fetch_manifest(
    cache_dir: &Path,
    client: &reqwest::Client,
    locations: &FabricLocations,
    version: u32,
    bypass_cache: bool,
) -> Result<Manifest, FabricRegistryError> {
    let cache_file = cache_dir.join(cache::MANIFEST_FILE);
    // In bypass mode skip the cached manifest entirely and fetch fresh — the
    // fresh copy still writes through below so later cached reads are current.
    if !bypass_cache {
        if let Some(m) = cache::read_entry::<Manifest>(&cache_file, "manifest")? {
            if m.version != version {
                return Err(cache::corrupt(
                    &cache_file,
                    "manifest",
                    &format!("it is manifest {}, in the folder of manifest {}", m.version, version),
                ));
            }
            return Ok(m);
        }
    }

    let url = locations.manifest(version);
    let body = fetch_object(client, &url, &format!("manifest v{version}"), version).await?;
    let manifest: Manifest = serde_json::from_slice(&body).map_err(|e| {
        FabricRegistryError::ParseError(format!("Failed to parse manifest v{}: {}", version, e))
    })?;
    if manifest.version != version {
        return Err(FabricRegistryError::ParseError(format!(
            "Manifest fetched as v{} reports version {}",
            version, manifest.version
        )));
    }
    cache::write_atomically(&cache_file, &body)?;
    Ok(manifest)
}

/// The charter of a fabric published at `anchor` the way the publisher lays a
/// fabric out (`fabric/src/fabric.js`). For a registry that is never fetched:
/// the test constructors, and tests of where things are.
pub(crate) fn publisher_layout_charter(anchor: &str) -> Charter {
    Charter {
        format: crate::bifaci::trust::CHARTER_FORMAT.to_string(),
        anchor: anchor.to_string(),
        kind: RegistryKind::Fabric.wire_name().to_string(),
        slug: "test-fabric".to_string(),
        environment: "prod".to_string(),
        experiment: None,
        locations: [
            ("manifest", format!("{anchor}/manifest/{{manifest_version}}.json")),
            ("latest", format!("{anchor}/manifest/latest.json")),
            ("definition", format!("{anchor}/{{kind}}/{{urn_hash}}/{{version}}.json")),
        ]
        .into_iter()
        .map(|(name, url)| (name.to_string(), url))
        .collect(),
        fields: [(crate::bifaci::trust::FABRIC_FIELD.to_string(), None)].into_iter().collect(),
        issued_at: 0,
        not_after: None,
    }
}

/// The locations of a fabric published at `anchor` in the publisher's layout.
#[cfg(test)]
pub(crate) fn layout_locations(anchor: &str) -> FabricLocations {
    FabricLocations::from_charter(&publisher_layout_charter(anchor)).expect("a fabric charter")
}

/// The verified charter of the fabric registry at `anchor`.
///
/// A fabric's contents carry no signature, so no content environment is
/// expected of its charter: any chartered fabric can be resolved against
/// (`capdag --fabric`), and what keeps one client's registries on one fabric is
/// each cartridge registry's charter naming it.
///
/// The cached one, when a folder under `capdag_cache` holds a charter for this
/// anchor that still verifies — so an offline client keeps its fabric. Else,
/// or when the cache is bypassed, the charter is fetched from the anchor and
/// stored in the folder its slug names. A stored charter that no longer
/// verifies (its certificate expired at a key rotation, say) is not used; the
/// registry's current one is fetched in its place.
async fn resolve_charter(
    client: &reqwest::Client,
    capdag_cache: &Path,
    anchor: &str,
    bypass_cache: bool,
    trust: &CharterTrust,
    now: u64,
) -> Result<VerifiedCharter, FabricRegistryError> {
    if !bypass_cache {
        if let Ok(entries) = fs::read_dir(capdag_cache) {
            for entry in entries.flatten() {
                let folder = entry.path();
                if !folder.join(mg_srel::CHARTER_FILE).is_file() {
                    continue;
                }
                match crate::bifaci::charter::verify_stored(
                    &folder,
                    trust,
                    RegistryKind::Fabric,
                    None,
                    now,
                ) {
                    Ok(verified) if verified.charter.anchor == anchor => return Ok(verified),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(
                        "the charter stored in {:?} is not used: {}",
                        folder,
                        e
                    ),
                }
            }
        }
    }
    let verified = crate::bifaci::charter::fetch(
        client,
        anchor,
        trust,
        RegistryKind::Fabric,
        None,
        now,
    )
    .await
    .map_err(|e| FabricRegistryError::Charter(e.to_string()))?;
    let folder = capdag_cache.join(&verified.charter.slug);
    mg_srel::store(&verified, &folder).map_err(|e| {
        FabricRegistryError::CacheError(format!(
            "Failed to store the charter of {} in {:?}: {}",
            anchor, folder, e
        ))
    })?;
    Ok(verified)
}

fn publish_cache_revision(tx: &watch::Sender<u64>) {
    let next = {
        let current = *tx.borrow();
        current.wrapping_add(1)
    };
    let _ = tx.send(next);
}

/// Single shared background fetch consumer for both cap and media URNs.
/// Drains the queue serially; failures are logged and dropped. The
/// queue keys carry both URN and defver, so the consumer never needs to
/// re-resolve through the manifest.
#[allow(clippy::too_many_arguments)]
async fn run_fetch_consumer(
    mut rx: mpsc::UnboundedReceiver<FetchKey>,
    client: reqwest::Client,
    cache_dir: PathBuf,
    cached_caps: Arc<Mutex<HashMap<String, Cap>>>,
    cached_media_defs: Arc<Mutex<HashMap<String, StoredMediaDef>>>,
    cached_aliases: Arc<Mutex<HashMap<String, StoredAlias>>>,
    cached_dims: Arc<Mutex<HashMap<String, StoredDim>>>,
    manifest: Arc<Mutex<Manifest>>,
    fetch_in_queue: Arc<Mutex<HashSet<FetchKey>>>,
    fetch_failures: FetchFailures,
    offline_flag: Arc<AtomicBool>,
    locations: FabricLocations,
    cache_revision_tx: watch::Sender<u64>,
) {
    let manifest_version = manifest.lock().map(|m| m.version).unwrap_or(0);
    while let Some(key) = rx.recv().await {
        match &key {
            FetchKey::Cap {
                urn: normalized_urn,
                defver,
            } => {
                let already_cached = cached_caps
                    .lock()
                    .ok()
                    .map(|m| m.contains_key(normalized_urn))
                    .unwrap_or(false);
                if !already_cached {
                    let result = fetch_one_cap_atomic(
                        &client,
                        &cache_dir,
                        &cached_caps,
                        &cached_media_defs,
                        &cached_dims,
                        &manifest,
                        &offline_flag,
                        &locations,
                        manifest_version,
                        &cache_revision_tx,
                        normalized_urn,
                        *defver,
                    )
                    .await;
                    note_fetch(&fetch_failures, key.clone(), &result);
                    match result {
                        Ok(_) => {
                            tracing::debug!(
                                target: "capdag::fabric::registry::fetch_consumer",
                                urn = %normalized_urn, defver = %defver,
                                "Background-fetched cap; cache is now warm."
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                target: "capdag::fabric::registry::fetch_consumer",
                                urn = %normalized_urn, defver = %defver, error = %e,
                                "Background cap fetch failed; URN dropped from queue (no retry)."
                            );
                        }
                    }
                }
            }
            FetchKey::Media {
                urn: normalized_urn,
                defver,
            } => {
                let already_cached = cached_media_defs
                    .lock()
                    .ok()
                    .map(|m| m.contains_key(normalized_urn))
                    .unwrap_or(false);
                if !already_cached {
                    let result = fetch_one_media_def(
                        &client,
                        &cache_dir,
                        &cached_media_defs,
                        &cached_dims,
                        &manifest,
                        manifest_version,
                        &offline_flag,
                        &locations,
                        &cache_revision_tx,
                        normalized_urn,
                        *defver,
                    )
                    .await;
                    note_fetch(&fetch_failures, key.clone(), &result);
                    match result {
                        Ok(_) => {
                            tracing::debug!(
                                target: "capdag::fabric::registry::fetch_consumer",
                                urn = %normalized_urn, defver = %defver,
                                "Background-fetched media def; cache is now warm."
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                target: "capdag::fabric::registry::fetch_consumer",
                                urn = %normalized_urn, defver = %defver, error = %e,
                                "Background media-def fetch failed; URN dropped from queue (no retry)."
                            );
                        }
                    }
                }
            }
            FetchKey::Alias {
                name: normalized_name,
                defver,
            } => {
                let already_cached = cached_aliases
                    .lock()
                    .ok()
                    .map(|m| m.contains_key(normalized_name))
                    .unwrap_or(false);
                if !already_cached {
                    let result = fetch_one_alias(
                        &client,
                        &cache_dir,
                        &cached_aliases,
                        &offline_flag,
                        &locations,
                        &cache_revision_tx,
                        manifest_version,
                        normalized_name,
                        *defver,
                    )
                    .await;
                    note_fetch(&fetch_failures, key.clone(), &result);
                    match result {
                        Ok(_) => {
                            tracing::debug!(
                                target: "capdag::fabric::registry::fetch_consumer",
                                name = %normalized_name, defver = %defver,
                                "Background-fetched alias; cache is now warm."
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                target: "capdag::fabric::registry::fetch_consumer",
                                name = %normalized_name, defver = %defver, error = %e,
                                "Background alias fetch failed; name dropped from queue (no retry)."
                            );
                        }
                    }
                }
            }
        }
        if let Ok(mut in_queue) = fetch_in_queue.lock() {
            in_queue.remove(&key);
        }
    }
}

// =============================================================================
// ERROR
// =============================================================================

#[derive(Debug, Clone, thiserror::Error)]
pub enum FabricRegistryError {
    /// The HTTP client could not be made.
    #[error("HTTP error: {0}")]
    HttpError(String),

    /// The registry's answer is that it does not hold the definition: the
    /// pinned manifest does not name it, or the registry answered 404 (or 410)
    /// for it. Nothing else is reported as a missing definition.
    #[error("Not found in registry: {0}")]
    NotFound(String),

    /// A fetch from the registry brought back no body: the registry refused
    /// it, or did not answer. The definition may well exist; what is known
    /// is only that this fetch failed, and the failure says where and why.
    #[error("{0}")]
    FetchFailed(FetchFailure),

    /// The pinned manifest names the definition, it is not in memory, and its
    /// fetch has been handed to the background fetcher: it is not at hand
    /// yet. Only a synchronous lookup that cannot wait on the network says
    /// this.
    #[error("Not fetched yet: {0}")]
    Pending(String),

    #[error("Failed to parse registry response: {0}")]
    ParseError(String),

    #[error("Cache error: {0}")]
    CacheError(String),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Network access blocked: {0}")]
    NetworkBlocked(String),

    /// The registry's charter could not be had, or was refused. Nothing the
    /// registry serves is used without one.
    #[error("Charter: {0}")]
    Charter(String),

    /// The fabric's file media types could not all be loaded.
    #[error("{0}")]
    FileTypes(String),
}

/// One fetch from the registry that brought back no body: what was being
/// fetched, from where, for which fabric manifest, and why it failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchFailure {
    /// What was being fetched, e.g. `media def 'media:pdf' (defver 3)` or
    /// `manifest v10`.
    pub what: String,
    /// The URL that was fetched.
    pub url: String,
    /// The fabric manifest version the fetch was made for.
    pub manifest_version: u32,
    pub cause: FetchCause,
}

/// Why a fetch from the registry brought back no body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchCause {
    /// The registry answered with this HTTP status, which is neither a
    /// success nor an answer that the object is absent (404, 410). Statuses
    /// worth repeating (429, 5xx) were repeated before this was concluded.
    Status(u16),
    /// The registry did not answer before the HTTP client's timeout, on any
    /// attempt.
    TimedOut,
    /// A synchronous lookup stopped waiting for the fetch after this long;
    /// the fetch goes on in the background.
    DeadlineElapsed(Duration),
    /// The request could not be made or its answer could not be read: the
    /// connection was refused or broke off.
    Transport(String),
}

impl FetchFailure {
    fn transport(what: &str, url: &str, manifest_version: u32, error: &reqwest::Error) -> Self {
        let cause = if error.is_timeout() {
            FetchCause::TimedOut
        } else {
            // reqwest's own text names the URL; the source chain says what
            // actually happened on the connection.
            let mut detail = error.to_string();
            let mut source = std::error::Error::source(error);
            while let Some(inner) = source {
                detail.push_str(": ");
                detail.push_str(&inner.to_string());
                source = inner.source();
            }
            FetchCause::Transport(detail)
        };
        Self {
            what: what.to_string(),
            url: url.to_string(),
            manifest_version,
            cause,
        }
    }
}

impl std::fmt::Display for FetchFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { what, url, manifest_version, cause } = self;
        match cause {
            FetchCause::Status(code) => {
                let reason = reqwest::StatusCode::from_u16(*code)
                    .ok()
                    .and_then(|s| s.canonical_reason())
                    .map(|r| format!(" {r}"))
                    .unwrap_or_default();
                write!(
                    f,
                    "the registry refused the fetch of {what} for fabric manifest v{manifest_version}: \
                     HTTP {code}{reason} from {url}"
                )
            }
            FetchCause::TimedOut => write!(
                f,
                "the registry did not answer the fetch of {what} for fabric manifest v{manifest_version}: \
                 the request to {url} timed out"
            ),
            FetchCause::DeadlineElapsed(after) => write!(
                f,
                "the fetch of {what} for fabric manifest v{manifest_version} did not finish within \
                 {} ms: no answer yet from {url}",
                after.as_millis()
            ),
            FetchCause::Transport(detail) => write!(
                f,
                "the fetch of {what} for fabric manifest v{manifest_version} from {url} failed: {detail}"
            ),
        }
    }
}

// =============================================================================
// ALIAS TESTS
// =============================================================================

#[cfg(test)]
mod alias_tests {
    use super::*;
    use crate::cap::definition::ArgSource;
    use crate::cap::definition::{Cap, CapArg, CapOutput};
    use crate::CapUrn;

    fn cap_with_urn(urn_str: &str) -> Cap {
        let urn = CapUrn::from_string(urn_str).expect("valid cap urn");
        Cap {
            dims: Vec::new(),
            urn,
            version: 1,
            title: "T".to_string(),
            cap_description: None,
            documentation: None,
            metadata: std::collections::HashMap::new(),
            aliases: vec!["test://cap".to_string()],
            is_abstract: false,
            args: vec![CapArg::new(
                "media:document;ext=pdf".to_string(), "input",
                true,
                vec![ArgSource::Stdin {
                    stdin: "media:document;ext=pdf".to_string(),
                }],
            )],
            outputs: vec![CapOutput::new(
                "media:enc=utf8".to_string(), "output",
                "out".to_string(),
            )],
            metadata_json: None,
            registered_by: None,
            supported_model_types: Vec::new(),
            default_model_spec: None,
        }
    }

    // TEST12816: fetching a cap requires the definition of every media URN a
    // machine can meet through it: its in= and out=, every arg's own URN and
    // the type it accepts, and every output's URN — not the main output's
    // alone. Each once; the wildcard names no definition; a URN that does not
    // parse fails the footprint rather than being left out of it.
    #[test]
    fn test12816_a_caps_media_footprint_covers_every_output_and_accepted_type() {
        let mut cap = cap_with_urn(r#"cap:in="media:document;ext=pdf";footprint;out="media:enc=utf8""#);
        cap.args.push(
            CapArg::new(
                "media:enc=utf8;second".to_string(),
                "second",
                false,
                vec![ArgSource::CliFlag { cli_flag: "--second".to_string() }],
            )
            .accepting("media:enc=utf8;accepted"),
        );
        cap.args.push(CapArg::new(
            "media:".to_string(),
            "anything",
            false,
            vec![ArgSource::CliFlag { cli_flag: "--anything".to_string() }],
        ));
        cap.outputs.push(CapOutput::new("media:enc=utf8;rest".to_string(), "rest", "rest".to_string()));
        let normalized = |urn: &str| normalize_media_urn(urn).unwrap();
        let mut expected: Vec<String> = [
            "media:document;ext=pdf",
            "media:enc=utf8",
            "media:enc=utf8;second",
            "media:enc=utf8;accepted",
            "media:enc=utf8;rest",
        ]
        .iter()
        .map(|urn| normalized(urn))
        .collect();
        expected.sort();
        let mut got = referenced_media_urns(&cap).unwrap();
        got.sort();
        assert_eq!(got, expected);

        cap.outputs.push(CapOutput::new("not a urn".to_string(), "broken", "broken".to_string()));
        assert!(matches!(referenced_media_urns(&cap), Err(FabricRegistryError::ParseError(_))));
    }

    fn media_spec(urn: &str) -> StoredMediaDef {
        StoredMediaDef {
            dims: Vec::new(),
            version: 1,
            urn: urn.to_string(),
            media_type: "application/json".to_string(),
            title: format!("title:{urn}"),
            profile_uri: None,
            schema: None,
            description: None,
            documentation: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        }
    }

    // TEST1887: the Manifest type round-trips an `aliases` map through serde.
    // The wire shape (name -> defver) must deserialize into Manifest.aliases
    // and serialize back identically. A regression here would silently drop
    // the alias section from a fetched manifest.
    #[test]
    fn test1887_manifest_serde_round_trips_aliases() {
        let json = r#"{"version":1,"previous":0,"caps":{},"media":{},"aliases":{"pdf2text":3,"jsondoc":1},"dims":{}}"#;
        let m: Manifest = serde_json::from_str(json).expect("manifest parses");
        assert_eq!(m.aliases.get("pdf2text").copied(), Some(3));
        assert_eq!(m.aliases.get("jsondoc").copied(), Some(1));
        let back = serde_json::to_value(&m).expect("serializes");
        assert_eq!(back["aliases"]["pdf2text"], 3);
        assert_eq!(back["aliases"]["jsondoc"], 1);
    }

    // TEST1888: resolve_alias returns the alias target untyped. Seeding a
    // media alias and resolving it yields the media URN; a malformed alias
    // name is rejected before any lookup.
    #[tokio::test]
    async fn test1888_resolve_alias_returns_target() {
        let registry = FabricRegistry::new_for_test();
        registry.insert_cached_alias_for_test(StoredAlias {
            name: "jsondoc".to_string(),
            target: "media:enc=utf8;fmt=json;record".to_string(),
            version: 1,
        });
        let target = registry.resolve_alias("jsondoc").await.expect("resolves");
        assert_eq!(target, "media:enc=utf8;fmt=json;record");
        // Case-insensitive: the same alias resolves regardless of input case.
        let upper = registry.resolve_alias("JSONDoc").await.expect("resolves");
        assert_eq!(upper, "media:enc=utf8;fmt=json;record");
        // Malformed name fails hard (not silently None).
        assert!(registry.resolve_alias("bad:name").await.is_err());
    }

    // TEST8063: narrow_abstract_cap resolves an abstract cap + detected input to
    // the unique concrete cap via is_dispatchable — the CLI's dispatch step.
    // The abstract caps are typed as the fabric types them (`convert-image` is
    // image → image, `disbind` takes a document). Exercises all three outcomes:
    // unique (fixed-output disbind), ambiguous (convert-image jpeg with no
    // --to), and no-handler — for an image no converter reads, and for a file
    // that is not an image at all, where the refusal names the declared type.
    #[tokio::test]
    async fn test8063_narrow_abstract_cap() {
        use crate::MediaUrn;
        let registry = FabricRegistry::new_for_test();
        let disbind_pdf = Cap::new(
            CapUrn::from_string(
                r#"cap:disbind;in="media:document;ext=pdf";out="media:enc=utf8;ext=txt;page;plain-text""#,
            )
            .unwrap(),
            "Disbind PDF".to_string(),
            vec!["disbind-pdf".to_string()],
        );
        let jpeg_png = Cap::new(
            CapUrn::from_string(
                r#"cap:convert-image;in="media:ext=jpeg;image";out="media:ext=png;image""#,
            )
            .unwrap(),
            "JPEG to PNG".to_string(),
            vec!["convert-image-jpeg-to-png".to_string()],
        );
        let jpeg_webp = Cap::new(
            CapUrn::from_string(
                r#"cap:convert-image;in="media:ext=jpeg;image";out="media:ext=webp;image""#,
            )
            .unwrap(),
            "JPEG to WebP".to_string(),
            vec!["convert-image-jpeg-to-webp".to_string()],
        );
        registry.add_caps_to_cache(vec![disbind_pdf, jpeg_png, jpeg_webp]);

        // disbind has a fixed output → a pdf input narrows uniquely, no --to.
        let disbind_abstract =
            CapUrn::from_string(
                r#"cap:disbind;in="media:document";out="media:enc=utf8;ext=txt;page;plain-text""#,
            )
            .unwrap();
        let pdf = MediaUrn::from_string("media:document;ext=pdf").unwrap();
        let concrete = registry
            .narrow_abstract_cap(&disbind_abstract, &pdf, None)
            .await
            .expect("disbind must narrow uniquely for a pdf input");
        assert!(concrete.to_string().contains("ext=pdf"));

        // convert-image with jpeg but no target is AMBIGUOUS (png or webp).
        let convert_abstract =
            CapUrn::from_string(r#"cap:convert-image;in="media:image";out="media:image""#).unwrap();
        let jpeg = MediaUrn::from_string("media:ext=jpeg;image").unwrap();
        assert!(
            matches!(
                registry
                    .narrow_abstract_cap(&convert_abstract, &jpeg, None)
                    .await,
                Err(CapNarrowError::Ambiguous { .. })
            ),
            "jpeg convert-image without --to must be ambiguous"
        );
        // --to png disambiguates.
        let png = MediaUrn::from_string("media:ext=png").unwrap();
        let picked = registry
            .narrow_abstract_cap(&convert_abstract, &jpeg, Some(&png))
            .await
            .expect("--to png must narrow uniquely");
        assert!(picked.to_string().contains("ext=png"));

        // An input no concrete cap handles → NoHandler (never a silent fallback).
        let gif = MediaUrn::from_string("media:ext=gif;image").unwrap();
        assert!(
            matches!(
                registry
                    .narrow_abstract_cap(&convert_abstract, &gif, None)
                    .await,
                Err(CapNarrowError::NoHandler { .. })
            ),
            "a gif input has no convert-image handler here → NoHandler"
        );

        // A file that is not an image: the same rule refuses it, and the
        // message carries what the abstract cap declares and what it was given.
        let text = MediaUrn::from_string("media:enc=utf8;ext=txt;plain-text").unwrap();
        let refused = registry
            .narrow_abstract_cap(&convert_abstract, &text, Some(&png))
            .await
            .expect_err("a text file is not an image");
        assert!(matches!(refused, CapNarrowError::NoHandler { .. }));
        let said = refused.to_string();
        assert!(said.contains("in=media:image"), "{said}");
        assert!(said.contains("media:enc=utf8;ext=txt;plain-text"), "{said}");

        // Nor does an image go to the document umbrella's handler.
        assert!(matches!(
            registry
                .narrow_abstract_cap(&disbind_abstract, &jpeg, None)
                .await,
            Err(CapNarrowError::NoHandler { .. })
        ));
    }

    // TEST1889: resolve_alias_typed enforces the expected kind. A media
    // alias requested as a cap fails hard; requested as media (or untyped)
    // succeeds. This is the typed-boundary contract.
    #[tokio::test]
    async fn test1889_resolve_alias_typed_enforces_kind() {
        let registry = FabricRegistry::new_for_test();
        registry.insert_cached_alias_for_test(StoredAlias {
            name: "jsondoc".to_string(),
            target: "media:enc=utf8;fmt=json;record".to_string(),
            version: 1,
        });
        // Correct kind: ok.
        assert!(registry
            .resolve_alias_typed("jsondoc", Some(AliasTargetKind::Media))
            .await
            .is_ok());
        // Untyped: ok.
        assert!(registry.resolve_alias_typed("jsondoc", None).await.is_ok());
        // Wrong kind: hard error.
        let err = registry
            .resolve_alias_typed("jsondoc", Some(AliasTargetKind::Cap))
            .await
            .unwrap_err();
        assert!(
            matches!(err, FabricRegistryError::ValidationError(_)),
            "a media alias demanded as a cap must be a ValidationError, got {err:?}"
        );
    }

    // TEST1890: get_cap accepts a cap alias and returns the aliased cap; a
    // media alias passed to get_cap fails hard (typed boundary). This proves
    // alias substitution AND type enforcement at the registry's cap surface.
    #[tokio::test]
    async fn test1890_get_cap_via_alias_and_type_mismatch() {
        let registry = FabricRegistry::new_for_test();
        let cap_urn = "cap:extract;in=\"media:document;ext=pdf\";out=\"media:enc=utf8\"";
        let cap = cap_with_urn(cap_urn);
        let canonical = cap.urn_string();
        registry.add_caps_to_cache(vec![cap]);
        registry.insert_cached_alias_for_test(StoredAlias {
            name: "pdf2text".to_string(),
            target: canonical.clone(),
            version: 1,
        });
        // Cap alias → the aliased cap.
        let got = registry
            .get_cap("pdf2text")
            .await
            .expect("cap alias resolves");
        assert_eq!(got.urn_string(), canonical);

        // Media alias passed to the cap boundary → hard error.
        registry.insert_cached_alias_for_test(StoredAlias {
            name: "jsondoc".to_string(),
            target: "media:enc=utf8;fmt=json;record".to_string(),
            version: 1,
        });
        let err = registry.get_cap("jsondoc").await.unwrap_err();
        assert!(
            matches!(err, FabricRegistryError::ValidationError(_)),
            "a media alias at get_cap must be a ValidationError, got {err:?}"
        );
    }

    // TEST1891: get_media_def accepts a media alias and returns the aliased
    // spec; a cap alias passed to get_media_def fails hard.
    #[tokio::test]
    async fn test1891_get_media_def_via_alias_and_type_mismatch() {
        let registry = FabricRegistry::new_for_test();
        registry.insert_cached_media_def_for_test(media_spec("media:enc=utf8;fmt=json;record"));
        registry.insert_cached_alias_for_test(StoredAlias {
            name: "jsondoc".to_string(),
            target: "media:enc=utf8;fmt=json;record".to_string(),
            version: 1,
        });
        let spec = registry
            .get_media_def("jsondoc")
            .await
            .expect("media alias resolves");
        assert_eq!(spec.urn, "media:enc=utf8;fmt=json;record");

        // A cap alias at the media boundary → hard error.
        let cap_urn = "cap:extract;in=\"media:document;ext=pdf\";out=\"media:enc=utf8\"";
        let cap = cap_with_urn(cap_urn);
        let canonical = cap.urn_string();
        registry.add_caps_to_cache(vec![cap]);
        registry.insert_cached_alias_for_test(StoredAlias {
            name: "pdf2text".to_string(),
            target: canonical,
            version: 1,
        });
        let err = registry.get_media_def("pdf2text").await.unwrap_err();
        assert!(
            matches!(err, FabricRegistryError::ValidationError(_)),
            "a cap alias at get_media_def must be a ValidationError, got {err:?}"
        );
    }

    // TEST1892: an unknown alias name is a hard not-found, never a silent empty;
    // unknown and malformed names are treated the same. This is the "expose
    // issues, no fallback" contract.
    #[tokio::test]
    async fn test1892_unknown_alias_is_not_found() {
        let registry = FabricRegistry::new_for_test();
        let err = registry.get_alias("nosuchalias").await.unwrap_err();
        assert!(
            matches!(err, FabricRegistryError::NotFound(_)),
            "unknown alias must be NotFound, got {err:?}"
        );
        assert!(registry.alias_defver_for("nosuchalias").is_err());
        // resolve_alias_cached answers no alias for an unknown name (and for a
        // malformed one): the manifest names neither.
        assert_eq!(registry.resolve_alias_cached("nosuchalias").unwrap(), None);
        assert_eq!(registry.resolve_alias_cached("bad:name").unwrap(), None);
    }
}

// =============================================================================
// PARITY PORTS — shared tests that existed in the mirrors but were missing
// from the Rust reference. Same number, same behavior, same method.
// =============================================================================

#[cfg(test)]
mod parity_port_tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn spec(urn: &str, title: &str) -> StoredMediaDef {
        StoredMediaDef {
            dims: Vec::new(),
            urn: urn.to_string(),
            version: 0,
            media_type: "application/octet-stream".to_string(),
            title: title.to_string(),
            profile_uri: None,
            schema: None,
            description: None,
            documentation: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        }
    }

    fn spec_with_ext(urn: &str, title: &str, media_type: &str, exts: &[&str]) -> StoredMediaDef {
        let mut s = spec(urn, title);
        s.media_type = media_type.to_string();
        s.extensions = exts.iter().map(|e| e.to_string()).collect();
        s
    }

    // TEST1894: select_display_alias picks the SHORTEST name, ties broken
    // alphabetically. This is the deterministic ordering every aliased-display
    // surface relies on; a regression here silently changes which alias the
    // whole UI renders.
    #[test]
    fn test1894_select_display_alias_ordering() {
        // Shorter wins over longer regardless of alphabetical order.
        assert_eq!(
            select_display_alias(["png-image", "png", "image-png"].into_iter()),
            Some("png")
        );
        // Equal length → alphabetical (a09 < a16).
        assert_eq!(
            select_display_alias(["a16", "a09", "a12"].into_iter()),
            Some("a09")
        );
        // Single candidate returns itself.
        assert_eq!(select_display_alias(["solo"].into_iter()), Some("solo"));
        // Empty set → None.
        assert_eq!(select_display_alias(std::iter::empty()), None);
    }

    // TEST1895: display_alias_for_urn reverse-resolves a URN to its display
    // alias. Proves: (1) the shortest-then-alphabetical winner among multiple
    // aliases on the same target, (2) a NON-canonical query URN (different tag
    // order) still resolves because the query is canonicalised before matching,
    // (3) a URN with no alias returns None, (4) a non-URN string returns None.
    #[test]
    fn test1895_display_alias_for_urn() {
        let registry = FabricRegistry::new_for_test();
        // Two aliases on the same cap target; "i2s" is shorter than "int2str".
        registry.insert_cached_alias_for_test(StoredAlias {
            name: "int2str".to_string(),
            target: "cap:coerce;in=\"media:integer;numeric\";out=\"media:enc=utf8\"".to_string(),
            version: 1,
        });
        registry.insert_cached_alias_for_test(StoredAlias {
            name: "i2s".to_string(),
            target: "cap:coerce;in=\"media:integer;numeric\";out=\"media:enc=utf8\"".to_string(),
            version: 1,
        });
        // A media alias too.
        registry.insert_cached_alias_for_test(StoredAlias {
            name: "json".to_string(),
            target: "media:enc=utf8;fmt=json;record".to_string(),
            version: 1,
        });

        // Canonical query → shortest alias wins.
        assert_eq!(
            registry
                .display_alias_for_urn(
                    "cap:coerce;in=\"media:integer;numeric\";out=\"media:enc=utf8\""
                )
                .as_deref(),
            Some("i2s")
        );
        // NON-canonical query (media tags reordered, cap arg order swapped):
        // must still resolve via canonicalisation. `media:record;fmt=json;enc=utf8`
        // canonicalises to `media:enc=utf8;fmt=json;record`.
        assert_eq!(
            registry
                .display_alias_for_urn("media:record;fmt=json;enc=utf8")
                .as_deref(),
            Some("json")
        );
        // A real URN with no alias → None.
        assert_eq!(
            registry.display_alias_for_urn("media:document;enc=utf8;ext=pdf"),
            None
        );
        // A non-URN (no cap:/media: prefix) → None, never a panic.
        assert_eq!(registry.display_alias_for_urn("int2str"), None);
        // The bare wildcard `cap:` parses but has no alias → None.
        assert_eq!(registry.display_alias_for_urn("cap:"), None);
    }

    // TEST1896: cached_cap_aliases returns only CAP-targeted aliases as
    // (name, target) pairs — media aliases are excluded. Drives the notation
    // editor's registered-alias completions.
    #[test]
    fn test1896_cached_cap_aliases_filters_to_cap_targets() {
        let registry = FabricRegistry::new_for_test();
        registry.insert_cached_alias_for_test(StoredAlias {
            name: "int2str".to_string(),
            target: "cap:coerce;in=\"media:integer;numeric\";out=\"media:enc=utf8\"".to_string(),
            version: 1,
        });
        registry.insert_cached_alias_for_test(StoredAlias {
            name: "json".to_string(),
            target: "media:enc=utf8;fmt=json;record".to_string(),
            version: 1,
        });
        let cap_aliases = registry.cached_cap_aliases();
        // Only the cap alias is returned; the media alias is filtered out.
        assert_eq!(cap_aliases.len(), 1, "got: {cap_aliases:?}");
        assert_eq!(cap_aliases[0].0, "int2str");
        assert_eq!(
            cap_aliases[0].1,
            "cap:coerce;in=\"media:integer;numeric\";out=\"media:enc=utf8\""
        );
    }

    // TEST147: Test registry for test with custom config creates registry with specified URLs
    #[test]
    fn test147_registry_for_test_with_custom_config() {
        let config = RegistryConfig::new().with_anchor("https://example.test/registry");
        let registry = FabricRegistry::new_for_test_with_config(config);
        assert_eq!(registry.config().anchor, "https://example.test/registry");
        assert_eq!(registry.charter().charter.anchor, "https://example.test/registry");
    }


    // TEST1899: a media def resolves to the charter's `definition` location at
    // the defver its manifest pins — `/media/<sha>/<defver>.json` in the
    // publisher's layout. There is no other path: the pre-manifest flat
    // layout is gone, and a client that composed one would 404 every lookup.
    // This pins both the URL rule and the manifest-driven defver resolution.
    #[test]
    fn test1899_media_def_resolves_to_versioned_object_path_under_manifest() {
        // 1. Object-path rule: the charter's definition location, at the defver.
        let locations = layout_locations("https://fabric.example.test");
        let cache = std::path::Path::new("/tmp/capdag-test-cache-0144");
        let urn = "media:enc=utf8;ext=md;plain-text";
        let mut hasher = Sha256::new();
        hasher.update(urn.as_bytes());
        let hash = format!("{:x}", hasher.finalize());

        let (versioned, _) = media_url_and_cache_path(cache, &locations, urn, 1);
        assert_eq!(
            versioned,
            format!("https://fabric.example.test/media/{}/1.json", hash),
            "a def at manifest defver 1 must resolve to the versioned object path"
        );

        // 2. Manifest-driven defver: a registry pinned at v>=1 resolves a
        // published media def to its pinned defver (versioned), never 0.
        let registry = FabricRegistry::new_for_test(); // pinned at manifest v1
        assert!(
            registry.manifest_version() >= 1,
            "the production registry must be pinned at manifest v>=1, never the legacy v0 flat-path mode"
        );
        registry.insert_cached_media_def_for_test(spec_with_ext(
            urn,
            "Markdown",
            "text/markdown",
            &["md"],
        ));
        assert_eq!(
            registry.media_defver_for(urn).unwrap(),
            registry.manifest_version(),
            "a published media def under a v>=1 manifest must resolve to the pinned defver, not 0"
        );
    }

    // (Media documentation propagation/round-trip is already covered by the
    // Rust reference's test1131/test1132/test1133 in media/spec.rs; the
    // mirrors' test288/test289 are the same behavior under a different number
    // and are reconciled by number-alignment, not duplicated here.)

    // TEST607: file_media_for_extension answers no file type for an
    // extension no definition of the manifest declares
    #[tokio::test]
    async fn test607_file_media_for_an_unknown_extension_is_none() {
        let registry = FabricRegistry::new_for_test();
        registry.insert_cached_media_def_for_test(spec_with_ext(
            "media:document;ext=pdf",
            "PDF Document",
            "application/pdf",
            &["pdf"],
        ));
        let defs = registry.file_media_for_extension("zzzzunknown").await.unwrap();
        assert!(defs.is_empty(), "no definition declares .zzzzunknown, got {defs:?}");
    }

    // TEST608: file_media_for_extension answers the file media definitions
    // that list the extension, whatever its case
    #[tokio::test]
    async fn test608_file_media_for_extension_lists_the_declaring_definitions() {
        let registry = FabricRegistry::new_for_test();
        registry.insert_cached_media_def_for_test(spec_with_ext(
            "media:document;ext=pdf",
            "PDF Document",
            "application/pdf",
            &["pdf"],
        ));
        let urns = |defs: Vec<StoredMediaDef>| defs.into_iter().map(|d| d.urn).collect::<Vec<_>>();
        let lower = urns(registry.file_media_for_extension("pdf").await.unwrap());
        assert_eq!(lower, vec!["media:document;ext=pdf".to_string()]);
        let upper = urns(registry.file_media_for_extension("PDF").await.unwrap());
        assert_eq!(lower, upper);
    }

    // TEST609: a definition that lists an extension but carries no `ext=`
    // file-type tag is content, not a file type: it is never a file type of
    // the extension, even when it is cached
    #[tokio::test]
    async fn test609_content_types_that_list_an_extension_are_no_file_types() {
        let registry = FabricRegistry::new_for_test();
        registry.insert_cached_media_def_for_test(spec_with_ext(
            "media:download-result;enc=utf8;fmt=json;record",
            "Model Download Result",
            "application/json",
            &["json"],
        ));
        registry.insert_cached_media_def_for_test(spec_with_ext(
            "media:enc=utf8;ext=json;fmt=json",
            "JSON File",
            "application/json",
            &["json"],
        ));
        let urns: Vec<String> = registry
            .file_media_for_extension("json")
            .await
            .unwrap()
            .into_iter()
            .map(|d| d.urn)
            .collect();
        assert_eq!(urns, vec!["media:enc=utf8;ext=json;fmt=json".to_string()]);
    }

    // TEST610: get_cached_media_def answers NotFound for a media def the
    // manifest does not name, and the def for a known one
    #[test]
    fn test610_get_cached_media_def() {
        let registry = FabricRegistry::new_for_test();
        assert!(matches!(
            registry.get_cached_media_def("media:nonexistent;xyzzy"),
            Err(FabricRegistryError::NotFound(_))
        ));
        registry.insert_cached_media_def_for_test(spec("media:enc=utf8;test;spec", "Test Spec"));
        let got = registry
            .get_cached_media_def("media:enc=utf8;test;spec")
            .unwrap();
        assert_eq!(got.title, "Test Spec");
    }

    // TEST614: Verify registry creation succeeds and cache directory exists
    #[test]
    fn test614_registry_creation() {
        let _registry = FabricRegistry::new_for_test();
    }

    // TEST616: Verify StoredMediaDef converts to MediaDef preserving all fields
    #[test]
    fn test616_stored_media_def_to_def() {
        let mut s = spec_with_ext("media:document;ext=pdf", "PDF Document", "application/pdf", &["pdf"]);
        s.profile_uri = Some("https://capdag.com/schema/pdf".to_string());
        s.description = Some("PDF document data".to_string());
        let def = s.to_media_def_def();
        assert_eq!(def.urn, "media:document;ext=pdf");
        assert_eq!(def.media_type, "application/pdf");
        assert_eq!(def.title, "PDF Document");
        assert_eq!(def.description.as_deref(), Some("PDF document data"));
        assert_eq!(def.extensions, vec!["pdf".to_string()]);
    }

    // TEST617: Verify normalize_media_urn produces consistent non-empty results
    #[test]
    fn test617_normalize_media_urn() {
        let u1 = normalize_media_urn("media:string").expect("valid media URN normalizes");
        let u2 = normalize_media_urn("media:string").expect("valid media URN normalizes");
        assert!(!u1.is_empty());
        assert_eq!(u1, u2);
    }

    // TEST6396: A malformed cap URN must FAIL HARD with a ParseError, not be
    // passed through raw (the old fallback) and surface later as a misleading
    // NotFound. The `out` value below contains an unquoted `=`, which the cap
    // grammar rejects. Against the old `Err(_) => urn.to_string()` fallback,
    // `normalize_cap_urn` returned the raw string and `cap_defver` then reported
    // "not part of manifest" (a NotFound); this test asserts the truthful error.
    #[tokio::test]
    async fn test6396_malformed_cap_urn_fails_hard() {
        let malformed = "cap:coerce;in=\"media:integer;numeric\";out=media:enc=utf8";

        // Direct normalization path.
        let direct = normalize_cap_urn(malformed);
        assert!(
            matches!(direct, Err(FabricRegistryError::ParseError(_))),
            "normalize_cap_urn on malformed URN must be ParseError, got {direct:?}"
        );

        // Public path (get_cap) must surface ParseError, NOT NotFound.
        let registry = FabricRegistry::new_for_test();
        let err = registry
            .get_cap(malformed)
            .await
            .expect_err("malformed cap URN must not resolve");
        assert!(
            matches!(err, FabricRegistryError::ParseError(_)),
            "get_cap on malformed URN must be ParseError (not NotFound), got {err:?}"
        );
    }

    // TEST908: cached caps remain accessible while offline.
    #[tokio::test]
    async fn test908_cached_caps_accessible_when_offline() {
        use crate::cap::definition::{Cap, CapArg, CapOutput};
        let registry = FabricRegistry::new_for_test();
        let urn =
            crate::CapUrn::from_string("cap:in=media:void;test-offline;out=media:void").unwrap();
        let cap = Cap {
            dims: Vec::new(),
            urn,
            version: 0,
            title: "Test Cap".to_string(),
            cap_description: None,
            documentation: None,
            metadata: std::collections::HashMap::new(),
            aliases: vec!["test".to_string()],
            is_abstract: false,
            args: Vec::<CapArg>::new(),
            outputs: vec![CapOutput::new("media:void".to_string(), "output", "void".to_string())],
            metadata_json: None,
            registered_by: None,
            supported_model_types: Vec::new(),
            default_model_spec: None,
        };
        registry.add_caps_to_cache(vec![cap]);
        registry.set_offline(true);
        let got = registry
            .get_cap("cap:in=media:void;test-offline;out=media:void")
            .await
            .expect("cached cap accessible offline");
        assert_eq!(got.title, "Test Cap");
    }

    // ---- RegistryConfig + per-cap URL parity ports ----

    // The per-cap registry URL at defver 1, through `cap_url_and_cache_path`
    // and the publisher's layout: the SHA-256 hex of the canonical cap URN is
    // the `{urn_hash}` of the `definition` location.
    fn cap_registry_url(cap_urn: &str) -> String {
        let normalized = normalize_cap_urn(cap_urn).expect("test passes a valid cap URN");
        let locations = layout_locations("https://fabric.capdag.com");
        cap_url_and_cache_path(Path::new("/tmp/capdag-test-cache-caps"), &locations, &normalized, 1).0
    }

    /// The `{urn_hash}` segment of a per-cap URL.
    fn urn_hash_of(url: &str) -> &str {
        url.split("/caps/").nth(1).and_then(|rest| rest.split('/').next()).expect("URL has /caps/<hash>/")
    }

    // TEST138: Test parsing registry JSON with stdin args verifies stdin media URN extraction
    #[test]
    fn test138_parse_registry_json_with_stdin() {
        let json = r#"{"urn":"cap:in=\"media:document;ext=pdf\";disbind;out=\"media:enc=utf8;page\"","aliases": ["disbind"],"title":"Disbind PDF","args":[{"media_urn":"media:document;ext=pdf","name":"input","required":true,"sources":[{"stdin":"media:document;ext=pdf"}]}]}"#;
        let cap: Cap = serde_json::from_str(json).expect("cap parses");
        assert_eq!(cap.title, "Disbind PDF");
        assert!(cap.accepts_stdin());
        assert_eq!(cap.get_stdin_media_urn(), Some("media:document;ext=pdf"));
    }

    // TEST12714: a definition a registry serves is held to the definition rules
    // before it is cached or returned. One that does not parse is a parse
    // error; one that parses and breaks a rule is a validation error naming
    // the cap and the rule.
    #[test]
    fn test12714_a_served_definition_is_held_to_the_definition_rules() {
        let urn = r#"cap:in="media:document;ext=pdf";disbind;out="media:enc=utf8;page""#;
        let definition = |args: serde_json::Value, outputs: serde_json::Value| {
            serde_json::json!({
                "urn": urn, "aliases": ["disbind"], "title": "Disbind PDF",
                "args": args, "outputs": outputs,
            })
            .to_string()
        };
        let main = serde_json::json!({
            "media_urn": "media:enc=utf8;file-path", "name": "document", "required": true,
            "sources": [{"stdin": "media:document;ext=pdf"}],
        });
        let page = serde_json::json!({"media_urn": "media:enc=utf8;page", "name": "pages", "output_description": "Pages"});

        let good = definition(serde_json::json!([main]), serde_json::json!([page]));
        let cap = checked_cap_definition(good.as_bytes(), urn).expect("a valid definition");
        assert_eq!(cap.main_output().map(|o| o.get_name()), Some("pages"));

        let err = checked_cap_definition(b"{not json", urn).unwrap_err();
        assert!(matches!(err, FabricRegistryError::ParseError(_)), "{err:?}");

        // The cap URN says it produces pages, and the definition declares no such output.
        let no_output = definition(serde_json::json!([main]), serde_json::json!([]));
        let err = checked_cap_definition(no_output.as_bytes(), urn).unwrap_err();
        assert!(matches!(err, FabricRegistryError::ValidationError(_)), "{err:?}");
        let text = err.to_string();
        assert!(text.contains("RULE21") && text.contains("disbind"), "{text}");

        // The main input is declared under a type that is not the cap's `in=`.
        let mut wrong = main.clone();
        wrong["sources"] = serde_json::json!([{"stdin": "media:document"}]);
        let wrong_input = definition(serde_json::json!([wrong]), serde_json::json!([page]));
        let err = checked_cap_definition(wrong_input.as_bytes(), urn).unwrap_err();
        assert!(matches!(err, FabricRegistryError::ValidationError(_)), "{err:?}");
        assert!(err.to_string().contains("RULE15"), "{err}");
    }

    // TEST12715: the cache holds only checked definitions, so one on disk that
    // breaks a rule was written under other rules: loading drops its file and
    // keeps the rest, and the definition is fetched anew when next asked for.
    #[test]
    fn test12715_a_cached_definition_that_breaks_a_rule_is_dropped() {
        let scratch = tempfile::tempdir().unwrap();
        let cache_dir = scratch.path();
        let urn = |op: &str| {
            crate::CapUrn::from_string(&format!(r#"cap:in="media:void";{op};out="media:record""#)).unwrap()
        };
        let mut kept = Cap::new(urn("kept"), "Kept".to_string(), vec!["kept".to_string()]);
        kept.version = 3;
        kept.add_output(crate::cap::definition::CapOutput::new("media:record", "record", "The record"));
        // The same shape without the output its URN states (RULE21).
        let mut stale = Cap::new(urn("stale"), "Stale".to_string(), vec!["stale".to_string()]);
        stale.version = 3;

        let mut manifest = Manifest::empty(11);
        let mut files = Vec::new();
        for cap in [&kept, &stale] {
            let key = normalize_cap_urn(&cap.urn_string()).unwrap();
            manifest.caps.insert(key.clone(), 3);
            let file = cache::entry_path(cache_dir, "caps", &key, 3);
            let entry = CapCacheEntry { definition: cap.clone(), cached_at: 0, ttl_hours: 0 };
            cache::store_entry(&file, &entry, "cap").unwrap();
            files.push(file);
        }

        let loaded = load_pinned_entries(cache_dir, &manifest).unwrap().caps;
        let urns: Vec<&String> = loaded.keys().collect();
        assert_eq!(loaded.len(), 1, "only the valid definition is loaded: {urns:?}");
        assert!(loaded.values().all(|cap| cap.title == "Kept"));
        assert!(files[0].is_file());
        assert!(!files[1].exists(), "the stale entry's file is removed");
    }

    // TEST6382: Test parsing registry JSON without stdin args verifies cap structure
    #[test]
    fn test6382_parse_registry_json_no_stdin() {
        let json = r#"{"urn":"cap:in=\"media:listing-id\";use-grinder;out=\"media:id;task\"","aliases": ["grinder_task"],"title":"Create Grinder Tool Task","args":[{"media_urn":"media:listing-id","name":"opt1","required":true,"sources":[{"cli_flag":"--listing-id"}]}]}"#;
        let cap: Cap = serde_json::from_str(json).expect("cap parses");
        assert_eq!(cap.title, "Create Grinder Tool Task");
        assert_eq!(cap.primary_alias(), "grinder_task");
        assert!(
            cap.get_stdin_media_urn().is_none(),
            "no stdin source means no stdin support"
        );
    }

    // TEST141: URL has the right shape — protocol, host, /caps/ prefix, 64 hex chars, no extension.
    #[test]
    fn test141_per_cap_url_shape() {
        let url = cap_registry_url(
            "cap:in=\"media:listing-id\";use-grinder;out=\"media:id;task\"",
        );
        let hash = urn_hash_of(&url);
        assert_eq!(hash.len(), 64, "SHA-256 hex digest is 64 characters");
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(url.ends_with("/1.json"), "the defver is the last segment: {url}");
    }

    // TEST142: Different tag orders normalise to the same URL — the canonicaliser strips the variation before hashing.
    #[test]
    fn test142_normalize_handles_different_tag_orders() {
        let a = cap_registry_url(
            "cap:test;in=\"media:enc=utf8\";out=\"media:record\"",
        );
        let b = cap_registry_url(
            "cap:in=\"media:enc=utf8\";out=\"media:record\";test",
        );
        assert_eq!(a, b, "different tag orders produce the same URL");
    }

    // TEST143: Default config points at https://fabric.capdag.com/ unless overridden by CDG_FABRIC_REGISTRY_URL.
    #[test]
    fn test143_default_config() {
        let config = RegistryConfig::default();
        match std::env::var("CDG_FABRIC_REGISTRY_URL") {
            Ok(url) if !url.is_empty() => assert_eq!(config.anchor, url),
            _ => assert_eq!(config.anchor, "https://fabric.capdag.com"),
        }
    }

    // TEST144: Test custom registry URL updates the anchor, which is all a client is told
    #[test]
    fn test144_custom_registry_url() {
        let config = RegistryConfig::default().with_anchor("https://localhost:8888");
        assert_eq!(config.anchor, "https://localhost:8888");
    }

    // TEST1893: a fabric's cache folder is its CHARTER's slug, and a charter
    // stored there is what an offline client finds its fabric by — matched by
    // anchor, never by folder name. A prod-populated cache can therefore
    // never satisfy a staging lookup: two registries have two charters and
    // two slugs, whatever their URLs look like. The charter used is a real
    // signed fixture, checked against its own roots.
    #[tokio::test]
    async fn test1893_cache_folder_is_the_charters_slug_and_found_by_anchor() {
        const ROOT_A: &str = include_str!("../../tests/fixtures/nocommit/signing/root_a.pubkey");
        const ROOT_B: &str = include_str!("../../tests/fixtures/nocommit/signing/root_b.pubkey");
        const ROOT_C: &str = include_str!("../../tests/fixtures/nocommit/signing/root_c.pubkey");
        const META: &str = include_str!("../../tests/fixtures/nocommit/signing/meta.json");
        const FABRIC: &[u8] = include_bytes!("../../tests/fixtures/nocommit/signing/charter-fabric.json");
        const FABRIC_SIG: &[u8] = include_bytes!("../../tests/fixtures/nocommit/signing/charter-fabric.json.sig");
        let trust = crate::bifaci::trust::charter_trust_from(&[ROOT_A, ROOT_B, ROOT_C]).unwrap();
        let meta: serde_json::Value = serde_json::from_str(META).unwrap();
        let now = meta["issued_at"].as_u64().unwrap() + 86_400;
        let anchor = "https://registry.example/fabric";
        let verified = crate::bifaci::charter::verify(
            FABRIC,
            FABRIC_SIG,
            &trust,
            &mg_srel::CharterExpectation { anchor: Some(anchor), kind: RegistryKind::Fabric.wire_name(), environment: None },
            now,
        )
        .expect("the fixture fabric charter verifies");

        let cache = tempfile::tempdir().unwrap();
        // Another registry's folder is beside it, and is not taken for it.
        std::fs::create_dir_all(cache.path().join("fabric.capdag.com")).unwrap();
        mg_srel::store(&verified, &cache.path().join("example-fabric")).unwrap();

        let client = reqwest::Client::new();
        let found = resolve_charter(&client, cache.path(), anchor, false, &trust, now)
            .await
            .expect("the stored charter is found offline, by its anchor");
        assert_eq!(found.charter.slug, "example-fabric");
        let locations = FabricLocations::from_charter(&found.charter).unwrap();
        assert_eq!(
            locations.manifest(11),
            "https://registry.example/fabric/manifest/11.json",
            "the manifest is where the charter says, not where a URL convention would put it"
        );
    }

    // TEST8139: live-source enumeration is MANIFEST-backed with the
    // metadata.content pairing — a live def the cap-driven cache warm never
    // touches (live references are referenced by NO cap) still enumerates,
    // and content lookup matches by urn EQUIVALENCE, not spelling. A silent
    // empty catalog here is the bug that shipped a dead microphone button.
    #[tokio::test]
    async fn test8139_live_source_defs_manifest_backed_with_pairing() {
        let registry = FabricRegistry::new_for_test();
        registry.insert_cached_media_def_for_test(StoredMediaDef {
            urn: "media:audio;live;microphone".to_string(),
            version: 1,
            media_type: "application/x-live-feed-reference".to_string(),
            title: "Microphone Feed Reference".to_string(),
            metadata: Some(serde_json::json!({ "content": "media:audio;audio-frames;pcm" })),
            ..Default::default()
        });
        registry.insert_cached_media_def_for_test(StoredMediaDef {
            urn: "media:audio;audio-frames;pcm".to_string(),
            version: 1,
            media_type: "audio/x-raw".to_string(),
            title: "PCM Audio Frames".to_string(),
            ..Default::default()
        });

        let defs = registry.live_source_defs().await.expect("enumeration succeeds");
        assert_eq!(
            defs,
            vec![(
                "media:audio;live;microphone".to_string(),
                "media:audio;audio-frames;pcm".to_string(),
                "Microphone Feed Reference".to_string()
            )],
            "exactly the live def, with its pairing — content defs are not live sources"
        );

        // Content lookup by EQUIVALENCE: a reordered spelling of the same
        // reference resolves the same pairing.
        let reordered = crate::MediaUrn::from_string("media:microphone;live;audio")
            .expect("reordered reference parses");
        assert_eq!(
            registry
                .live_source_content_urn(&reordered)
                .await
                .expect("lookup succeeds"),
            Some("media:audio;audio-frames;pcm".to_string())
        );
    }

    // TEST8140: a live def WITHOUT its metadata.content pairing is a fabric
    // defect reported as a HARD error — never silently skipped (a skipped
    // def is an invisible dead device family).
    #[tokio::test]
    async fn test8140_live_def_without_pairing_is_a_hard_error() {
        let registry = FabricRegistry::new_for_test();
        registry.insert_cached_media_def_for_test(StoredMediaDef {
            urn: "media:image;live;webcam".to_string(),
            version: 1,
            media_type: "application/x-live-feed-reference".to_string(),
            title: "Webcam Feed Reference".to_string(),
            metadata: None,
            ..Default::default()
        });
        let err = registry
            .live_source_defs()
            .await
            .expect_err("a pairing-less live def must fail loudly");
        assert!(
            err.to_string().contains("metadata.content"),
            "the error names the missing pairing: {err}"
        );
    }

    // TEST6388: Per-cap URL is /caps/<sha256-hex> — no URN-grammar characters in the path, no percent-encoding gymnastics.
    #[test]
    fn test6388_per_cap_url_uses_sha256() {
        let url = cap_registry_url(
            "cap:in=\"media:enc=utf8\";test;out=\"media:record\"",
        );
        assert!(url.contains("/caps/"));
        assert!(
            !url.contains("cap:"),
            "URL must not contain raw cap: URN syntax"
        );
        assert!(
            !url.contains("%3A") && !url.contains("%3D") && !url.contains("%3B"),
            "URL must not contain percent-encoded URN characters"
        );
    }

    // TEST6391: Equivalent URNs (different tag order, etc.) hash to the same key.
    #[test]
    fn test6391_same_cap_different_spellings_same_url() {
        let a = cap_registry_url(
            "cap:in=\"media:listing-id\";use-grinder;out=\"media:id;task\"",
        );
        let b = cap_registry_url(
            "cap:out=\"media:id;task\";in=\"media:listing-id\";use-grinder",
        );
        assert_eq!(a, b, "equivalent URNs must hash to the same registry key");
    }
}

/// The disk cache shared by processes that run at once, and by capdag
/// releases pinned at different fabric manifests.
#[cfg(test)]
mod concurrent_cache_tests {
    use super::*;

    /// A registry whose cache is a directory of this test's own.
    fn registry_at(cache_dir: PathBuf) -> FabricRegistry {
        let mut registry = FabricRegistry::new_for_test();
        registry.cache_dir = cache_dir;
        registry
    }

    const URN: &str = r#"cap:in="media:void";shared-entry;out="media:record""#;

    /// The cap `URN` at defver 3, titled `title`, with a documentation large
    /// enough that writing it takes more than one step.
    fn variant(title: &str) -> Cap {
        let mut cap = Cap::new(
            crate::CapUrn::from_string(URN).unwrap(),
            title.to_string(),
            vec!["shared-entry".to_string()],
        );
        cap.version = 3;
        cap.add_output(crate::cap::definition::CapOutput::new("media:record", "record", "The record"));
        cap.documentation = Some(format!("{title}\n").repeat(2_000));
        cap
    }

    fn pinned(version: u32) -> Manifest {
        let mut manifest = Manifest::empty(version);
        manifest.caps.insert(normalize_cap_urn(URN).unwrap(), 3);
        manifest
    }

    fn store(cache_dir: &Path, cap: &Cap) {
        let file = cache::entry_path(cache_dir, "caps", &normalize_cap_urn(URN).unwrap(), 3);
        let entry = CapCacheEntry { definition: cap.clone(), cached_at: 0, ttl_hours: 0 };
        cache::store_entry(&file, &entry, "cap").expect("the entry is written");
    }

    // TEST1990: many readers and writers on one cache entry at once: every
    // read finds no entry or one whole entry a writer wrote, never part of one
    // and never an error, and no temporary file is left behind. The readers
    // read for as long as any writer writes, so every write overlaps reads.
    #[test]
    fn test1990_concurrent_readers_and_writers_never_see_a_torn_entry() {
        use std::sync::atomic::{AtomicBool, AtomicUsize};
        let dir = tempfile::TempDir::new().expect("temp dir");
        let cache_dir = cache::version_dir(dir.path(), 11);
        let manifest = pinned(11);
        let variants: Vec<Cap> = (0..4).map(|n| variant(&format!("written by writer {n}"))).collect();
        let writing = AtomicUsize::new(variants.len());
        let reads = AtomicUsize::new(0);
        let done = AtomicBool::new(false);

        let failures: Vec<String> = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for cap in &variants {
                let (cache_dir, writing, done) = (&cache_dir, &writing, &done);
                handles.push(scope.spawn(move || {
                    for _ in 0..400 {
                        store(cache_dir, cap);
                    }
                    if writing.fetch_sub(1, Ordering::SeqCst) == 1 {
                        done.store(true, Ordering::SeqCst);
                    }
                    Vec::new()
                }));
            }
            for _ in 0..4 {
                let (cache_dir, manifest, variants, done, reads) = (&cache_dir, &manifest, &variants, &done, &reads);
                handles.push(scope.spawn(move || {
                    let mut failures = Vec::new();
                    while !done.load(Ordering::SeqCst) {
                        reads.fetch_add(1, Ordering::Relaxed);
                        match load_pinned_entries(cache_dir, manifest) {
                            Ok(entries) => {
                                if let Some(cap) = entries.caps.values().next() {
                                    if !variants.iter().any(|v| {
                                        v.title == cap.title && v.documentation == cap.documentation
                                    }) {
                                        failures.push(format!("read a cap no writer wrote: {}", cap.title));
                                    }
                                }
                            }
                            Err(error) => failures.push(error.to_string()),
                        }
                    }
                    failures
                }));
            }
            handles.into_iter().flat_map(|h| h.join().expect("no panic")).collect()
        });

        assert!(reads.load(Ordering::Relaxed) > 0, "the readers read while the writers wrote");
        assert!(failures.is_empty(), "{} torn or failed reads, first: {:?}", failures.len(), failures.first());
        let loaded = load_pinned_entries(&cache_dir, &manifest).unwrap().caps;
        assert_eq!(loaded.len(), 1, "the entry is there at the end");
        let folder = cache::entry_path(&cache_dir, "caps", &normalize_cap_urn(URN).unwrap(), 3);
        let left: Vec<_> = fs::read_dir(folder.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(left, vec!["3.json".to_string()], "no temporary file is left behind");
    }

    // TEST1991: two fabric manifest versions share one registry folder: each
    // keeps its entries in a folder of its own, and reads only what was cached
    // for it, even where both manifests pin the same definition at the same
    // defver.
    #[test]
    fn test1991_two_manifest_versions_read_only_their_own_entries() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let v10 = cache::version_dir(dir.path(), 10);
        let v13 = cache::version_dir(dir.path(), 13);
        assert_ne!(v10, v13, "each manifest version has a folder of its own");

        store(&v10, &variant("cached for manifest 10"));
        assert!(
            load_pinned_entries(&v13, &pinned(13)).unwrap().caps.is_empty(),
            "manifest 13 does not read what was cached for manifest 10"
        );

        store(&v13, &variant("cached for manifest 13"));
        let title = |dir: &Path, version: u32| {
            load_pinned_entries(dir, &pinned(version)).unwrap().caps.values().next().unwrap().title.clone()
        };
        assert_eq!(title(&v10, 10), "cached for manifest 10");
        assert_eq!(title(&v13, 13), "cached for manifest 13");

        let a = FabricRegistry::new_for_test_with_config_and_version(RegistryConfig::default(), 10);
        let b = FabricRegistry::new_for_test_with_config_and_version(RegistryConfig::default(), 13);
        assert_ne!(a.cache_dir(), b.cache_dir(), "registries at two manifests cache in two folders");
        assert_eq!(a.cache_dir().parent(), b.cache_dir().parent(), "under one registry folder");
    }

    // TEST1992: an entry that is there and is not what its path names is
    // reported naming its file, and left where it is; it is never taken for
    // a missing entry. That covers a body that does not parse, one holding
    // another definition or defver, and a cached manifest that does not parse.
    #[tokio::test]
    async fn test1992_a_corrupt_entry_is_reported_naming_its_file() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let cache_dir = cache::version_dir(dir.path(), 11);
        let manifest = pinned(11);
        let file = cache::entry_path(&cache_dir, "caps", &normalize_cap_urn(URN).unwrap(), 3);
        fs::create_dir_all(file.parent().unwrap()).unwrap();

        fs::write(&file, r#"{"definition": {"urn": "cap:in="#).unwrap();
        let error = load_pinned_entries(&cache_dir, &manifest).expect_err("a truncated entry is an error");
        let message = error.to_string();
        assert!(matches!(error, FabricRegistryError::CacheError(_)), "{error:?}");
        assert!(message.contains(&file.display().to_string()), "names the file: {message}");
        assert!(message.contains("corrupt"), "{message}");
        assert!(file.is_file(), "the file is left for whoever reads the error");

        let mut other = variant("another defver");
        other.version = 4;
        let entry = CapCacheEntry { definition: other, cached_at: 0, ttl_hours: 0 };
        cache::store_entry(&file, &entry, "cap").unwrap();
        let error = load_pinned_entries(&cache_dir, &manifest).expect_err("another defver is an error");
        assert!(error.to_string().contains(&file.display().to_string()), "{error}");
        assert!(error.to_string().contains("defver 4"), "{error}");

        let manifest_file = cache_dir.join(cache::MANIFEST_FILE);
        fs::write(&manifest_file, "{\"version\": 11, \"prev").unwrap();
        let locations = layout_locations("http://127.0.0.1:9");
        let error = load_or_fetch_manifest(&cache_dir, &reqwest::Client::new(), &locations, 11, false)
            .await
            .expect_err("a corrupt manifest is an error, not a fetch");
        assert!(error.to_string().contains(&manifest_file.display().to_string()), "{error}");
        assert!(manifest_file.is_file());
    }

    // TEST1993: clearing a manifest version's cache empties its folder and
    // removes the folders the shared layout before it kept in the registry's
    // folder; other manifest versions' folders and the stored charter stay.
    #[test]
    fn test1993_clearing_empties_one_version_and_removes_the_old_shared_folders() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let registry_dir = dir.path().join("example-fabric");
        let v11 = cache::version_dir(&registry_dir, 11);
        let v10 = cache::version_dir(&registry_dir, 10);
        store(&v11, &variant("eleven"));
        store(&v10, &variant("ten"));
        fs::write(v11.join(cache::MANIFEST_FILE), "{}").unwrap();
        fs::write(v11.join(".manifest.json.1-0.tmp"), "half").unwrap();
        fs::write(registry_dir.join("charter.json"), "{}").unwrap();
        for legacy in cache::LEGACY_FOLDERS {
            fs::create_dir_all(registry_dir.join(legacy).join("ab")).unwrap();
            fs::write(registry_dir.join(legacy).join("ab").join("1.json"), "{}").unwrap();
        }

        clear_cache_folder(&v11).expect("clears");

        for kind in cache::KIND_FOLDERS {
            let folder = v11.join(kind);
            assert!(folder.is_dir(), "{kind} stays");
            assert_eq!(fs::read_dir(&folder).unwrap().count(), 0, "{kind} is empty");
        }
        assert_eq!(fs::read_dir(&v11).unwrap().count(), cache::KIND_FOLDERS.len(), "only the kinds' folders are left");
        for legacy in cache::LEGACY_FOLDERS {
            assert!(!registry_dir.join(legacy).exists(), "the old shared {legacy} folder is removed");
        }
        assert_eq!(load_pinned_entries(&v10, &pinned(10)).unwrap().caps.len(), 1, "manifest 10's cache stays");
        assert!(registry_dir.join("charter.json").is_file(), "the charter stays");
    }

    #[test]
    fn test11158_clearing_leaves_the_cache_present_and_empty() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let cache = cache::version_dir(&dir.path().join("fabric.example.com"), 1);
        let registry = registry_at(cache.clone());

        std::fs::create_dir_all(cache.join("caps")).expect("caps");
        std::fs::write(cache.join("caps/stale.json"), "{}").expect("stale entry");

        registry.clear_cache().expect("clears");

        assert!(cache.join("caps").is_dir(), "the directory survives");
        assert!(!cache.join("caps/stale.json").exists(), "its contents do not");
    }

    #[test]
    fn test11159_a_cache_that_does_not_exist_yet_is_created() {
        // The first build on a machine finds nothing here. That is not a
        // failure to report, and the old code silently did nothing at all.
        let dir = tempfile::TempDir::new().expect("temp dir");
        let cache = cache::version_dir(&dir.path().join("never-existed"), 1);
        registry_at(cache.clone()).clear_cache().expect("clears");
        for sub in cache::KIND_FOLDERS {
            assert!(cache.join(sub).is_dir(), "{sub} exists after clearing");
        }
    }

    #[test]
    fn test11160_several_processes_clearing_at_once_all_succeed() {
        // The real failure: a test run starts eight cartridge builds within
        // milliseconds of each other and every one refreshes this same shared
        // directory. Removing the tree and putting it back races — one peer
        // deleting an ancestor while another creates into it is exactly the
        // `No such file or directory` a build died on.
        let dir = tempfile::TempDir::new().expect("temp dir");
        let cache = cache::version_dir(&dir.path().join("shared.example.com"), 1);
        std::fs::create_dir_all(cache.join("caps")).expect("caps");
        for n in 0..200 {
            std::fs::write(cache.join(format!("caps/entry-{n}.json")), "{}").expect("entry");
        }

        let failures: Vec<String> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let cache = cache.clone();
                    scope.spawn(move || {
                        // Each peer clears repeatedly, so they genuinely
                        // interleave rather than finishing one after another.
                        let mut errors = Vec::new();
                        for _ in 0..20 {
                            if let Err(error) = registry_at(cache.clone()).clear_cache() {
                                errors.push(error.to_string());
                            }
                        }
                        errors
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|h| h.join().expect("no panic"))
                .collect()
        });

        assert!(
            failures.is_empty(),
            "clearing must tolerate a peer doing the same thing: {failures:?}"
        );
        // The two faces of this race are platform-specific — Linux reports the
        // missing path, macOS reports `Directory not empty` — and a fix for
        // one that leaves the other is half a fix.
        assert!(
            !failures.iter().any(|f| f.contains("not empty") || f.contains("No such file")),
            "neither platform's symptom may survive: {failures:?}"
        );
        for sub in cache::KIND_FOLDERS {
            assert!(cache.join(sub).is_dir(), "{sub} is present at the end");
        }
    }
}

// =============================================================================
// DIM TESTS
// =============================================================================

#[cfg(test)]
mod dim_tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use crate::fabric::dim::{DimTag, StoredDimValue};

    fn dim(name: &str, version: u32, values: &[&str]) -> StoredDim {
        StoredDim {
            name: name.to_string(),
            version,
            title: format!("Title of {name}"),
            documentation: format!("## {name}\nWhat {name} is."),
            values: values
                .iter()
                .map(|v| StoredDimValue {
                    name: v.to_string(),
                    title: v.to_uppercase(),
                    tags: vec![DimTag {
                        key: name.to_string(),
                        value: v.to_string(),
                    }],
                    documentation: Some(format!("The {v} value.")),
                    media_type: None,
                    extensions: Vec::new(),
                })
                .collect(),
        }
    }

    // TEST1966: the Manifest type round-trips a `dims` map (name -> defver)
    // through serde, and a manifest without one is refused: every manifest
    // this library reads names the dims of its snapshot.
    #[test]
    fn test1966_manifest_carries_a_dims_map_and_requires_it() {
        let json = r#"{"version":11,"previous":10,"caps":{},"media":{},"aliases":{},"dims":{"language":1,"file-type":2}}"#;
        let m: Manifest = serde_json::from_str(json).expect("manifest parses");
        assert_eq!(m.dims.get("language").copied(), Some(1));
        assert_eq!(m.dims.get("file-type").copied(), Some(2));
        let back = serde_json::to_value(&m).expect("serializes");
        assert_eq!(back["dims"]["file-type"], 2);

        let without = r#"{"version":11,"previous":10,"caps":{},"media":{},"aliases":{}}"#;
        let err = serde_json::from_str::<Manifest>(without).expect_err("a manifest without dims is refused");
        assert!(err.to_string().contains("dims"), "{err}");
    }

    // TEST1967: get_dim returns a dim the registry holds; a name the pinned
    // manifest does not list is NotFound naming the manifest version, decided
    // before any fetch (the registry is offline, and the error is not the
    // offline one); a string that is no dim name is refused as such.
    #[tokio::test]
    async fn test1967_get_dim_resolves_through_the_manifest() {
        let registry = FabricRegistry::new_for_test();
        registry.insert_cached_dim_for_test(dim("language", 1, &["en", "es"]));
        let got = registry.get_dim("language").await.expect("a held dim resolves");
        assert_eq!(got.value("es").map(|v| v.title.as_str()), Some("ES"));
        assert_eq!(registry.dim_defver_for("language").unwrap(), 1);

        registry.set_offline(true);
        let err = registry.get_dim("tone").await.expect_err("an unlisted dim is not found");
        assert!(
            matches!(&err, FabricRegistryError::NotFound(m) if m.contains("dim 'tone' is not part of manifest v1")),
            "{err}"
        );
        assert!(matches!(
            registry.get_dim("Tone").await,
            Err(FabricRegistryError::ValidationError(_))
        ));
    }

    // TEST1968: a dim is addressed at dims/<sha256-of-name>/<defver>.json, on
    // the registry and in the cache; a body served for it must be that dim at
    // that version, with no field this library does not know, and must hold
    // (values present, names unique).
    #[test]
    fn test1968_a_dim_object_is_the_dim_and_version_asked_for() {
        let locations = layout_locations("https://fabric.example.test");
        let cache = Path::new("/tmp/capdag-test-cache-1968");
        let mut hasher = Sha256::new();
        hasher.update("language".as_bytes());
        let hash = format!("{:x}", hasher.finalize());
        let (url, path) = dim_url_and_cache_path(cache, &locations, "language", 3);
        assert_eq!(url, format!("https://fabric.example.test/dims/{hash}/3.json"));
        assert_eq!(path, cache.join("dims").join(&hash).join("3.json"));

        let good = serde_json::to_vec(&dim("language", 3, &["en"])).unwrap();
        assert_eq!(checked_dim(&good, "language", 3).unwrap().name, "language");
        assert!(matches!(checked_dim(&good, "file-type", 3), Err(FabricRegistryError::ParseError(m)) if m.contains("does not match requested name")));
        assert!(matches!(checked_dim(&good, "language", 2), Err(FabricRegistryError::ParseError(m)) if m.contains("manifest pins defver 2")));

        let mut extra: serde_json::Value = serde_json::from_slice(&good).unwrap();
        extra["predicates"] = serde_json::json!([]);
        assert!(matches!(
            checked_dim(&serde_json::to_vec(&extra).unwrap(), "language", 3),
            Err(FabricRegistryError::ParseError(m)) if m.contains("predicates")
        ));
        let twice = serde_json::to_vec(&dim("language", 3, &["en", "en"])).unwrap();
        assert!(matches!(
            checked_dim(&twice, "language", 3),
            Err(FabricRegistryError::ValidationError(m)) if m.contains("declares value 'en' more than once")
        ));
        let none = serde_json::to_vec(&dim("language", 3, &[])).unwrap();
        assert!(matches!(checked_dim(&none, "language", 3), Err(FabricRegistryError::ValidationError(_))));
    }

    fn cap_with_dims(urn: &str, dims: &[(&str, &str)]) -> Cap {
        let mut cap = Cap::new(CapUrn::from_string(urn).unwrap(), "t".into(), vec!["t".into()]);
        cap.version = 1;
        cap.dims = dims.iter().map(|(d, v)| DimRef::new(*d, *v)).collect();
        cap
    }

    fn spec_with_dims(urn: &str, dims: &[(&str, &str)]) -> StoredMediaDef {
        StoredMediaDef {
            urn: urn.to_string(),
            version: 1,
            media_type: "text/plain".into(),
            title: "t".into(),
            dims: dims.iter().map(|(d, v)| DimRef::new(*d, *v)).collect(),
            ..Default::default()
        }
    }

    use crate::CapUrn;

    // TEST1971: what is loaded from the disk cache is kept only as the pinned
    // manifest names it — a dim at another version is dropped — and a
    // definition is kept only when every dim it names survives with the value
    // it names. At v0 there are no dims.
    #[test]
    fn test1971_the_disk_cache_keeps_definitions_only_with_their_pinned_dims() {
        let mut manifest = Manifest::empty(11);
        manifest.dims.insert("language".into(), 2);
        manifest.dims.insert("medium".into(), 1);
        let caps_urns = ["cap:a;in=media:;out=media:", "cap:b;in=media:;out=media:", "cap:c;in=media:;out=media:"];
        for u in caps_urns {
            manifest.caps.insert(normalize_cap_urn(u).unwrap(), 1);
        }
        manifest.media.insert("media:x".into(), 1);
        manifest.media.insert("media:y".into(), 1);

        let mut caps: HashMap<String, Cap> = HashMap::new();
        caps.insert(normalize_cap_urn(caps_urns[0]).unwrap(), cap_with_dims(caps_urns[0], &[("medium", "text")]));
        caps.insert(normalize_cap_urn(caps_urns[1]).unwrap(), cap_with_dims(caps_urns[1], &[("language", "en")]));
        caps.insert(normalize_cap_urn(caps_urns[2]).unwrap(), cap_with_dims(caps_urns[2], &[]));
        let mut specs: HashMap<String, StoredMediaDef> = HashMap::new();
        specs.insert("media:x".into(), spec_with_dims("media:x", &[("medium", "text")]));
        specs.insert("media:y".into(), spec_with_dims("media:y", &[("medium", "audio")]));
        let mut aliases = HashMap::new();
        let mut dims: HashMap<String, StoredDim> = HashMap::new();
        dims.insert("language".into(), dim("language", 1, &["en"])); // the manifest pins 2
        dims.insert("medium".into(), dim("medium", 1, &["text"]));

        retain_pinned(&manifest, 11, &mut caps, &mut specs, &mut aliases, &mut dims);
        assert_eq!(dims.keys().collect::<Vec<_>>(), vec!["medium"], "a dim at another version is dropped");
        let mut kept: Vec<_> = caps.values().map(|c| c.urn_string()).collect();
        kept.sort();
        assert_eq!(
            kept,
            vec![normalize_cap_urn(caps_urns[0]).unwrap(), normalize_cap_urn(caps_urns[2]).unwrap()],
            "the cap naming the dropped dim is dropped with it"
        );
        assert_eq!(specs.keys().collect::<Vec<_>>(), vec!["media:x"], "a value the dim lacks drops the definition");

        let mut v0_dims = HashMap::from([("medium".to_string(), dim("medium", 1, &["text"]))]);
        retain_pinned(&Manifest::empty(0), 0, &mut HashMap::new(), &mut HashMap::new(), &mut HashMap::new(), &mut v0_dims);
        assert!(v0_dims.is_empty(), "v0 has no dims");
    }

    // TEST1972: a definition is cached only once every dim it names is held,
    // with the value it names. A dim the manifest does not list is refused
    // naming the definition, the dim and the manifest version, before any
    // fetch; a value the dim does not declare is refused naming all three;
    // one dim named twice is refused.
    #[tokio::test]
    async fn test1972_a_definition_needs_every_dim_it_names() {
        let registry = FabricRegistry::new_for_test();
        registry.insert_cached_dim_for_test(dim("language", 1, &["en"]));
        registry.set_offline(true);
        let ensure = |refs: Vec<DimRef>| {
            let r = &registry;
            async move {
                ensure_dims_cached(
                    "cap 'cap:probe'",
                    &refs,
                    &r.client,
                    &r.cache_dir,
                    &r.cached_dims,
                    &r.manifest,
                    r.manifest_version,
                    &r.offline_flag,
                    &r.locations,
                    &r.cache_revision_tx,
                )
                .await
            }
        };
        ensure(vec![DimRef::new("language", "en")]).await.expect("a held dim and value pass");

        let err = ensure(vec![DimRef::new("language", "fr")]).await.unwrap_err();
        assert!(
            matches!(&err, FabricRegistryError::ValidationError(m) if m == "cap 'cap:probe' names value 'fr' of dim 'language', which the dim does not declare"),
            "{err}"
        );
        let err = ensure(vec![DimRef::new("tone", "formal")]).await.unwrap_err();
        assert!(
            matches!(&err, FabricRegistryError::NotFound(m) if m == "cap 'cap:probe' names dim 'tone', which is not in manifest v1"),
            "{err}"
        );
        let err = ensure(vec![DimRef::new("language", "en"), DimRef::new("language", "en")]).await.unwrap_err();
        assert!(matches!(&err, FabricRegistryError::ValidationError(m) if m.contains("named more than once")), "{err}");
    }
}

// =============================================================================
// FETCH FAILURE TESTS
// =============================================================================

/// A fetch the registry refused, or did not answer, is reported as itself —
/// the URL, the status and the manifest version — and never as a definition
/// the registry does not have. Run against a stub registry on a local socket.
#[cfg(test)]
mod fetch_failure_tests {
    use super::*;
    use crate::fabric::stub_registry::{Answer, StubRegistry};

    const MANIFEST_VERSION: u32 = 10;
    const MEDIA: &str = "media:refusal-probe;enc=utf8";
    const CAP: &str = r#"cap:in="media:refusal-probe;enc=utf8";refusal-probe;out="media:refusal-probe;enc=utf8""#;
    const ALIAS: &str = "measure-returns";
    const DIM: &str = "refusal-probe";

    /// A registry pinned at manifest v10 against `stub`: its manifest names
    /// one cap, media def, alias and dim, and none of them is in memory.
    fn registry_at(stub: &StubRegistry) -> FabricRegistry {
        let registry = FabricRegistry::new_for_test_with_config_and_version(
            RegistryConfig::new().with_anchor(stub.anchor.clone()),
            MANIFEST_VERSION,
        );
        {
            let mut manifest = registry.manifest.lock().unwrap();
            manifest.caps.insert(normalize_cap_urn(CAP).unwrap(), 1);
            manifest.media.insert(normalize_media_urn(MEDIA).unwrap(), 1);
            manifest.aliases.insert(ALIAS.to_string(), 2);
            manifest.dims.insert(DIM.to_string(), 1);
        }
        registry
    }

    fn failure(error: FabricRegistryError) -> FetchFailure {
        match error {
            FabricRegistryError::FetchFailed(failure) => failure,
            other => panic!("expected a failed fetch, got {other:?}"),
        }
    }

    fn cap_what() -> String {
        format!("cap '{}' (defver 1)", normalize_cap_urn(CAP).unwrap())
    }

    fn media_what() -> String {
        format!("media def '{}' (defver 1)", normalize_media_urn(MEDIA).unwrap())
    }

    // TEST7167: a manifest fetch the registry refuses (403) is reported as
    // the refusal — the URL, the status and the manifest version — and not as
    // a manifest that is not found. A 403 is not repeated.
    #[tokio::test]
    async fn test7167_a_refused_manifest_fetch_is_reported_as_the_refusal() {
        let stub = StubRegistry::start(|_| Answer::Status(403)).await;
        let cache = tempfile::tempdir().unwrap();
        let locations = layout_locations(&stub.anchor);
        let error = load_or_fetch_manifest(cache.path(), &reqwest::Client::new(), &locations, 10, false)
            .await
            .unwrap_err();
        let text = error.to_string();
        let refusal = failure(error);
        assert_eq!(
            refusal,
            FetchFailure {
                what: "manifest v10".to_string(),
                url: locations.manifest(10),
                manifest_version: 10,
                cause: FetchCause::Status(403),
            }
        );
        for part in ["refused", "HTTP 403 Forbidden", refusal.url.as_str(), "fabric manifest v10"] {
            assert!(text.contains(part), "'{part}' is missing from: {text}");
        }
        assert!(!text.to_lowercase().contains("not found"), "{text}");
        assert_eq!(stub.requests_for("/manifest/10.json"), 1, "a 403 is not repeated");
    }

    // TEST7168: a manifest the registry answers 404 for is the registry's
    // word that it has no such manifest: NotFound, naming the URL and the
    // status.
    #[tokio::test]
    async fn test7168_a_manifest_the_registry_does_not_have_is_not_found() {
        let stub = StubRegistry::start(|_| Answer::Status(404)).await;
        let cache = tempfile::tempdir().unwrap();
        let locations = layout_locations(&stub.anchor);
        let error = load_or_fetch_manifest(cache.path(), &reqwest::Client::new(), &locations, 10, false)
            .await
            .unwrap_err();
        match &error {
            FabricRegistryError::NotFound(detail) => {
                assert!(detail.contains("HTTP 404"), "{detail}");
                assert!(detail.contains(&locations.manifest(10)), "{detail}");
            }
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    // TEST7169: a refused fetch of a cap, a media def, an alias or a dim is
    // reported as the refusal, each with its own URL, the status and the
    // manifest version it was fetched for.
    #[tokio::test]
    async fn test7169_a_refused_definition_fetch_is_reported_as_the_refusal() {
        let stub = StubRegistry::start(|_| Answer::Status(403)).await;
        let registry = registry_at(&stub);
        let refused = [
            (registry.get_cap(CAP).await.unwrap_err(), cap_what()),
            (registry.get_media_def(MEDIA).await.unwrap_err(), media_what()),
            (registry.get_alias(ALIAS).await.unwrap_err(), format!("alias '{ALIAS}' (defver 2)")),
            (registry.get_dim(DIM).await.unwrap_err(), format!("dim '{DIM}' (defver 1)")),
        ];
        for (error, what) in refused {
            let text = error.to_string();
            let refusal = failure(error);
            assert_eq!(refusal.what, what);
            assert_eq!(refusal.cause, FetchCause::Status(403), "{text}");
            assert_eq!(refusal.manifest_version, MANIFEST_VERSION);
            assert_eq!(stub.requests_for(stub.path_of(&refusal.url)), 1, "{what} was fetched from {}", refusal.url);
            assert!(text.contains(&refusal.url) && text.contains("403"), "{text}");
        }
    }

    // TEST7170: a registry that answers 503 is asked again, and when it never
    // answers otherwise the fetch is reported with the 503 — never as a
    // missing definition.
    #[tokio::test]
    async fn test7170_a_registry_that_keeps_failing_is_reported_with_its_status() {
        let stub = StubRegistry::start(|_| Answer::Status(503)).await;
        let registry = registry_at(&stub);
        let refusal = failure(registry.get_media_def(MEDIA).await.unwrap_err());
        assert_eq!(refusal.what, media_what());
        assert_eq!(refusal.cause, FetchCause::Status(503));
        assert_eq!(
            stub.requests_for(stub.path_of(&refusal.url)),
            crate::net_retry::RetryPolicy::default().max_attempts as usize,
            "a 503 is asked again before it is reported"
        );
    }

    // TEST7171: a registry that does not answer is reported as a fetch that
    // timed out, with the URL and the manifest version; a synchronous lookup
    // that stops waiting says how long it waited.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test7171_a_registry_that_does_not_answer_is_reported_as_a_timeout() {
        let stub = StubRegistry::start(|_| Answer::Silence).await;
        let mut registry = registry_at(&stub);
        registry.client = reqwest::Client::builder()
            .timeout(Duration::from_millis(200))
            .build()
            .unwrap();
        let error = registry.get_media_def(MEDIA).await.unwrap_err();
        let text = error.to_string();
        let timeout = failure(error);
        assert_eq!(timeout.what, media_what());
        assert_eq!(timeout.cause, FetchCause::TimedOut);
        assert_eq!(timeout.manifest_version, MANIFEST_VERSION);
        assert!(text.contains("timed out") && text.contains(&timeout.url), "{text}");

        registry.client = reqwest::Client::new();
        let deadline = failure(registry.get_cached_cap(CAP).unwrap_err());
        assert_eq!(deadline.what, cap_what());
        assert_eq!(deadline.cause, FetchCause::DeadlineElapsed(SYNC_FETCH_DEADLINE));
        assert!(deadline.to_string().contains(&deadline.url), "{deadline}");
    }

    // TEST7172: a definition the registry answers 404 for is the registry's
    // word that it does not have it, and only that is NotFound.
    #[tokio::test]
    async fn test7172_a_definition_the_registry_does_not_have_is_not_found() {
        let stub = StubRegistry::start(|_| Answer::Status(404)).await;
        let registry = registry_at(&stub);
        for error in [
            registry.get_cap(CAP).await.unwrap_err(),
            registry.get_media_def(MEDIA).await.unwrap_err(),
            registry.get_alias(ALIAS).await.unwrap_err(),
        ] {
            match &error {
                FabricRegistryError::NotFound(detail) => {
                    assert!(detail.contains("HTTP 404") && detail.contains(&stub.anchor), "{detail}");
                }
                other => panic!("expected NotFound, got {other:?}"),
            }
        }
    }

    // TEST7173: the synchronous lookups report a refused fetch as the refusal,
    // where they used to come back empty — and report a definition the
    // manifest does not name as not found.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test7173_a_synchronous_lookup_reports_a_refused_fetch() {
        let stub = StubRegistry::start(|_| Answer::Status(403)).await;
        let registry = registry_at(&stub);
        let media = failure(registry.get_cached_media_def(MEDIA).unwrap_err());
        assert_eq!((media.what, media.cause), (media_what(), FetchCause::Status(403)));
        let cap = failure(registry.get_cached_cap(CAP).unwrap_err());
        assert_eq!((cap.what, cap.cause), (cap_what(), FetchCause::Status(403)));
        assert!(matches!(
            registry.get_cached_media_def("media:named-by-no-manifest"),
            Err(FabricRegistryError::NotFound(_))
        ));
    }

    // TEST7174: a machine whose cap alias the registry refused to serve fails
    // with the refusal, never as an undefined alias; an alias the registry
    // does not have is still undefined.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test7174_a_refused_alias_is_reported_as_the_refusal_not_as_undefined() {
        let stub = StubRegistry::start(|_| Answer::Status(403)).await;
        let registry = registry_at(&stub);
        let error = crate::machine::parse_machine_async(&format!("[doc -> {ALIAS} -> text]"), &registry)
            .await
            .unwrap_err();
        let text = error.to_string();
        match error {
            crate::machine::MachineParseError::Resolution(
                crate::machine::MachineAbstractionError::DefinitionUnavailable { urn, error },
            ) => {
                assert_eq!(urn, ALIAS);
                let refusal = failure(error);
                assert_eq!(refusal.what, format!("alias '{ALIAS}' (defver 2)"));
                assert_eq!(refusal.cause, FetchCause::Status(403));
            }
            other => panic!("expected the alias to be unavailable, got {other:?}"),
        }
        for part in ["refused", "403", ALIAS, "fabric manifest v10", stub.anchor.as_str()] {
            assert!(text.contains(part), "'{part}' is missing from: {text}");
        }

        let undefined = crate::machine::parse_machine_async("[doc -> named-by-no-manifest -> text]", &registry)
            .await
            .unwrap_err();
        assert!(
            matches!(
                undefined,
                crate::machine::MachineParseError::Syntax(crate::machine::MachineSyntaxError::UndefinedAlias { .. })
            ),
            "{undefined}"
        );
    }

    // TEST7175: what a background fetch came to is kept: a lookup that cannot
    // wait on the network reports the refusal the prefetch got, says a
    // definition it has not fetched yet is pending, and finds an alias the
    // manifest does not name to be no alias.
    #[tokio::test]
    async fn test7175_a_refused_prefetch_is_remembered_by_lookups_that_cannot_wait() {
        let stub = StubRegistry::start(|_| Answer::Status(403)).await;
        let registry = registry_at(&stub);
        assert!(matches!(
            registry.resolve_alias_cached(ALIAS),
            Err(FabricRegistryError::Pending(_))
        ));
        assert!(matches!(
            registry.get_cached_media_def(MEDIA),
            Err(FabricRegistryError::Pending(_))
        ));

        registry.prefetch_manifest_aliases().await;
        registry.prefetch_manifest_caps().await;
        let alias = failure(registry.resolve_alias_cached(ALIAS).unwrap_err());
        assert_eq!(alias.what, format!("alias '{ALIAS}' (defver 2)"));
        assert_eq!(alias.cause, FetchCause::Status(403));
        let cap = failure(registry.get_cached_cap(CAP).unwrap_err());
        assert_eq!((cap.what, cap.cause), (cap_what(), FetchCause::Status(403)));
        assert_eq!(registry.resolve_alias_cached("named-by-no-manifest").unwrap(), None);
    }
}
