//! Unified fabric registry: caps + media defs.
//!
//! Two domain payload types:
//! - `Cap` (cap definitions) at `<base>/caps/<sha256-of-canonical-urn>`
//! - `StoredMediaDef` (media defs) at `<base>/media/<sha256-of-canonical-urn>`
//!
//! On disk:
//! - `<cache_dir>/caps/<sha256>.json`
//! - `<cache_dir>/media/<sha256>.json`
//!
//! Resolution policy (same for both domains):
//!   1. In-memory cache hit → return immediately.
//!   2. Synchronous fetch attempt with hard 500 ms deadline.
//!   3. Deadline miss / error → enqueue for background consumer, return
//!      `None` (sync surface) or `Err` (async surface).
//!
//! The cap fetch is **atomic**: if any media URN referenced by a cap fails
//! to fetch, the cap is NOT cached. This guarantees that any cap landing
//! in the cap cache has every one of its referenced media defs already in
//! the media cache (and the extension index).

use crate::cap::definition::ArgSource;
use crate::fabric::alias::{classify_alias_target, normalize_alias_name, AliasTargetKind};
use crate::media::spec::MediaDef;
use crate::Cap;
use crate::StoredAlias;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};

const DEFAULT_REGISTRY_BASE_URL: &str = "https://fabric.capdag.com";

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

/// Configuration for the fabric registry.
///
/// Sources, in priority order:
/// 1. Builder methods.
/// 2. Environment variables (`CDG_FABRIC_REGISTRY_URL`, `CDG_SCHEMA_BASE_URL`).
/// 3. Defaults: `https://fabric.capdag.com` for the registry, `<registry>/schema`
///    for schemas.
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    pub registry_base_url: String,
    pub schema_base_url: String,
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
        let registry_base = env::var("CDG_FABRIC_REGISTRY_URL")
            .unwrap_or_else(|_| DEFAULT_REGISTRY_BASE_URL.to_string());
        let schema_base =
            env::var("CDG_SCHEMA_BASE_URL").unwrap_or_else(|_| format!("{}/schema", registry_base));
        Self {
            registry_base_url: registry_base,
            schema_base_url: schema_base,
            bypass_cache: false,
        }
    }
}

impl RegistryConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_registry_url(mut self, url: impl Into<String>) -> Self {
        let url = url.into();
        if self.schema_base_url == format!("{}/schema", self.registry_base_url) {
            self.schema_base_url = format!("{}/schema", url);
        }
        self.registry_base_url = url;
        self
    }

    pub fn with_schema_url(mut self, url: impl Into<String>) -> Self {
        self.schema_base_url = url.into();
        self
    }

    /// Enable cache-bypass mode (see [`RegistryConfig::bypass_cache`]).
    pub fn with_bypass_cache(mut self, bypass: bool) -> Self {
        self.bypass_cache = bypass;
        self
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
/// the (normalized) alias name instead of a URN.
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
        }
    }
}

// =============================================================================
// REGISTRY
// =============================================================================

#[derive(Debug)]
pub struct FabricRegistry {
    client: reqwest::Client,
    /// Root cache directory. Caps and media defs live in `caps/` and
    /// `media/` subdirectories respectively, mirroring the registry's
    /// own URL layout. v0 entries live at `caps/<sha>.json` and
    /// `media/<sha>.json`; v >= 1 entries live at `caps/<sha>/<defver>.json`
    /// and `media/<sha>/<defver>.json`. Manifests live in `manifests/<N>.json`.
    cache_dir: PathBuf,
    cached_caps: Arc<Mutex<HashMap<String, Cap>>>,
    cached_media_defs: Arc<Mutex<HashMap<String, StoredMediaDef>>>,
    /// Normalized alias name → resolved `StoredAlias`. Populated from the
    /// `aliases/<sha>/<defver>.json` cache on disk and the background/sync
    /// fetch path, filtered to the pinned manifest's defvers.
    cached_aliases: Arc<Mutex<HashMap<String, StoredAlias>>>,
    /// Lower-case extension → list of canonical media URNs.
    extension_index: Arc<Mutex<HashMap<String, Vec<String>>>>,
    config: RegistryConfig,
    /// Fabric manifest version this registry is pinned to. 0 means
    /// legacy v0 / flat-path resolution (the implicit pre-versioning
    /// mode). >= 1 means manifest-driven resolution. Set at construction
    /// from the caller (engine bakes `capdag::FABRIC_MANIFEST_VERSION`).
    manifest_version: u32,
    /// Live snapshot of the registry pinned at `manifest_version`. For
    /// v0 this is an `empty(0)` placeholder and never consulted for
    /// resolution. For v >= 1 every URN lookup hits this map first to
    /// turn the URN into a `(urn, defver)` pair before fetching.
    /// Wrapped in Mutex because test helpers like `add_caps_to_cache`
    /// mutate it.
    manifest: Arc<Mutex<Manifest>>,
    offline_flag: Arc<AtomicBool>,
    fetch_queue_tx: Option<mpsc::UnboundedSender<FetchKey>>,
    fetch_in_queue: Arc<Mutex<HashSet<FetchKey>>>,
    cache_revision_tx: watch::Sender<u64>,
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

/// `ENOTEMPTY` is 39 on Linux and 66 on macOS; `EBUSY` is 16 on both.
#[cfg(target_os = "macos")]
const ENOTEMPTY: i32 = 66;
#[cfg(not(target_os = "macos"))]
const ENOTEMPTY: i32 = 39;
const EBUSY: i32 = 16;

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
    /// `manifest_version == 0` → legacy v0 / flat-path mode. No manifest
    /// fetch is performed; resolution falls through to the frozen flat
    /// R2 paths.
    ///
    /// `manifest_version >= 1` → manifest-driven. The constructor
    /// **blocks** on a network round-trip to fetch `manifest/<N>.json`
    /// if no local cache copy is present. If neither local cache nor
    /// network can provide it, the constructor returns
    /// `FabricRegistryError::NotFound`. There is no fallback to v0.
    pub async fn with_config_and_manifest_version(
        config: RegistryConfig,
        manifest_version: u32,
    ) -> Result<Self, FabricRegistryError> {
        let cache_dir = Self::default_cache_root(&config.registry_base_url)?;
        let caps_dir = cache_dir.join("caps");
        let media_dir = cache_dir.join("media");
        let aliases_dir = cache_dir.join("aliases");
        let manifests_dir = cache_dir.join("manifests");
        for d in [&caps_dir, &media_dir, &aliases_dir, &manifests_dir] {
            fs::create_dir_all(d).map_err(|e| {
                FabricRegistryError::CacheError(format!(
                    "Failed to create cache directory {:?}: {}",
                    d, e
                ))
            })?;
        }

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| {
                FabricRegistryError::HttpError(format!("Failed to create HTTP client: {}", e))
            })?;

        // Bootstrap the manifest before loading on-disk caches so the
        // cache loaders can hydrate the in-memory map with entries
        // matching the manifest's pinned defvers (rather than blindly
        // pulling in stale v0 flat-path bytes that may belong to a
        // different snapshot).
        let manifest = if manifest_version == 0 {
            Manifest::empty(0)
        } else {
            load_or_fetch_manifest(
                &manifests_dir,
                &client,
                &config,
                manifest_version,
                config.bypass_cache,
            )
            .await?
        };

        // In bypass-cache mode we do NOT hydrate from disk: every cap/media/
        // alias body is fetched fresh on demand against the (just re-fetched)
        // manifest, so a mutable channel can never serve a stale body under a
        // reused defver. The fresh bytes still write through to the cache.
        let (mut cached_caps_map, mut cached_specs_map, mut cached_aliases_map) =
            if config.bypass_cache {
                (HashMap::new(), HashMap::new(), HashMap::new())
            } else {
                (
                    Self::load_all_cached_caps(&caps_dir)?,
                    Self::load_all_cached_media_defs(&media_dir)?,
                    Self::load_all_cached_aliases(&aliases_dir)?,
                )
            };
        // Filter loaded caches by manifest pin: only retain entries
        // whose URN's defver in the manifest matches the cached entry's
        // own version. At v0 the manifest is empty and we retain
        // everything (the load function only walks flat paths anyway
        // because no versioned subdirs are written under v0 mode).
        if manifest_version >= 1 {
            cached_caps_map
                .retain(|urn, cap| manifest.caps.get(urn).copied().unwrap_or(0) == cap.version);
            cached_specs_map
                .retain(|urn, spec| manifest.media.get(urn).copied().unwrap_or(0) == spec.version);
            cached_aliases_map.retain(|name, alias| {
                manifest.aliases.get(name).copied().unwrap_or(0) == alias.version
            });
        } else {
            // Aliases are a versioned-regime concept; there is no v0
            // flat-path alias. At v0 the alias cache is always empty.
            cached_aliases_map.clear();
        }
        let extension_index_map = Self::build_extension_index(&cached_specs_map);

        let cached_caps = Arc::new(Mutex::new(cached_caps_map));
        let cached_media_defs = Arc::new(Mutex::new(cached_specs_map));
        let cached_aliases = Arc::new(Mutex::new(cached_aliases_map));
        let extension_index = Arc::new(Mutex::new(extension_index_map));
        let manifest_arc = Arc::new(Mutex::new(manifest));
        let fetch_in_queue = Arc::new(Mutex::new(HashSet::new()));
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
                    Arc::clone(&extension_index),
                    Arc::clone(&manifest_arc),
                    Arc::clone(&fetch_in_queue),
                    Arc::clone(&offline_flag),
                    config.clone(),
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
            extension_index,
            config,
            manifest_version,
            manifest: manifest_arc,
            offline_flag,
            fetch_queue_tx,
            fetch_in_queue,
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

    pub fn set_offline(&self, offline: bool) {
        self.offline_flag.store(offline, Ordering::Relaxed);
    }

    pub fn subscribe_cache_revisions(&self) -> watch::Receiver<u64> {
        self.cache_revision_tx.subscribe()
    }

    /// The on-disk cache root for a given registry origin.
    ///
    /// The cache holds per-cap, per-media, and per-manifest JSON keyed by
    /// URN-hash / version — values that DIFFER between registry origins (the
    /// staging registry serves different cap/media/manifest bytes than prod for
    /// the same URN). Sharing one directory across origins would let a prod-
    /// populated cache satisfy a staging lookup (and vice versa), silently
    /// serving the wrong snapshot — the exact failure that makes a
    /// `CDG_FABRIC_REGISTRY_URL=staging` run resolve against stale prod data.
    ///
    /// So the root is namespaced by a stable slug of the registry base URL,
    /// using the SAME `slug_for` scheme as the cartridge registry layout
    /// (`<os_cache>/capdag/<registry_slug>/…`). Two origins therefore never
    /// share a cache slot; switching origins switches cache trees.
    fn default_cache_root(registry_base_url: &str) -> Result<PathBuf, FabricRegistryError> {
        let mut cache_dir = dirs::cache_dir().ok_or_else(|| {
            FabricRegistryError::CacheError("Could not determine cache directory".to_string())
        })?;
        cache_dir.push("capdag");
        cache_dir.push(crate::bifaci::cartridge_slug::slug_for(Some(
            registry_base_url,
        )));
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
            &self.extension_index,
            &self.manifest,
            &self.offline_flag,
            &self.config,
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
            &self.config,
            &self.cache_revision_tx,
            &normalized,
            defver,
        )
        .await
    }

    /// Synchronous, in-memory-only alias resolution. Returns the target
    /// URN if the alias is already in the warm cache, else `None`. Used by
    /// synchronous call sites (the machine-notation resolver) after an
    /// async pre-warm has populated the cache. Returns `None` (not an
    /// error) for a malformed name so callers can treat "not a valid alias"
    /// and "not a cached alias" uniformly as "no resolution".
    pub fn resolve_alias_cached(&self, name: &str) -> Option<String> {
        let normalized = normalize_alias_name(name).ok()?;
        self.cached_aliases
            .lock()
            .ok()
            .and_then(|m| m.get(&normalized).map(|a| a.target.clone()))
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
        let config = self.config.clone();
        let cache_revision_tx = self.cache_revision_tx.clone();
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
                &config,
                &cache_revision_tx,
                &normalized,
                defver,
            )
            .await;
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
        let extension_index = Arc::clone(&self.extension_index);
        let manifest = Arc::clone(&self.manifest);
        let offline_flag = Arc::clone(&self.offline_flag);
        let config = self.config.clone();
        let manifest_version = self.manifest_version;
        let cache_revision_tx = self.cache_revision_tx.clone();
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
                &extension_index,
                &manifest,
                &offline_flag,
                &config,
                manifest_version,
                &cache_revision_tx,
                &normalized_urn,
                defver,
            )
            .await;
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
    pub fn get_cached_cap(&self, urn: &str) -> Option<Cap> {
        // A malformed URN cannot be in the cache and cannot be fetched, so it
        // resolves to `None` — the same graceful "not available" outcome this
        // method already returns for a cache miss, a fetch error, or a deadline.
        // We log it (it usually signals an upstream bug passing a non-canonical
        // string) but never panic: this is a latency-critical lookup reached
        // from many call sites, and a bad URN must degrade, not crash the app.
        let normalized_urn = match normalize_cap_urn(urn) {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(
                    target: "capdag::fabric::registry",
                    urn = %urn, error = %e,
                    "get_cached_cap received a malformed URN; treating as not found"
                );
                return None;
            }
        };
        if let Some(cap) = self
            .cached_caps
            .lock()
            .ok()
            .and_then(|m| m.get(&normalized_urn).cloned())
        {
            return Some(cap);
        }
        // If the URN is not in the manifest under v >= 1, there's
        // nothing to fetch — return None without enqueuing.
        let defver = self.cap_defver(&normalized_urn).ok()?;
        let runtime = tokio::runtime::Handle::try_current().ok()?;
        if !matches!(
            runtime.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            self.enqueue_for_background_fetch(FetchKey::Cap {
                urn: normalized_urn,
                defver,
            });
            return None;
        }
        let sync_attempt = tokio::task::block_in_place(|| {
            runtime.block_on(async {
                tokio::time::timeout(
                    SYNC_FETCH_DEADLINE,
                    fetch_one_cap_atomic(
                        &self.client,
                        &self.cache_dir,
                        &self.cached_caps,
                        &self.cached_media_defs,
                        &self.extension_index,
                        &self.manifest,
                        &self.offline_flag,
                        &self.config,
                        self.manifest_version,
                        &self.cache_revision_tx,
                        &normalized_urn,
                        defver,
                    ),
                )
                .await
            })
        });
        match sync_attempt {
            Ok(Ok(cap)) => return Some(cap),
            Ok(Err(e)) => {
                tracing::debug!(
                    target: "capdag::fabric::registry",
                    urn = %normalized_urn, error = %e,
                    "Synchronous cap fetch errored within deadline; enqueueing for background fetch."
                );
            }
            Err(_elapsed) => {
                tracing::debug!(
                    target: "capdag::fabric::registry",
                    urn = %normalized_urn,
                    "Synchronous cap fetch did not complete within deadline; enqueueing for background fetch."
                );
            }
        }
        self.enqueue_for_background_fetch(FetchKey::Cap {
            urn: normalized_urn,
            defver,
        });
        None
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

    /// Check whether a cap URN exists in the registry (cached or online).
    pub async fn cap_exists(&self, urn: &str) -> bool {
        self.get_cap(urn).await.is_ok()
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
            &self.extension_index,
            &self.offline_flag,
            &self.config,
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

    /// Synchronous media-def lookup that warms its own cache.
    pub fn get_cached_media_def(&self, urn: &str) -> Option<StoredMediaDef> {
        // A malformed URN cannot be in the cache and cannot be fetched, so it
        // resolves to `None` — the same graceful "not available" outcome as a
        // cache miss. Log it (it usually means an upstream bug passed a
        // non-canonical string) but never panic: this latency-critical lookup
        // must degrade, not crash the app.
        let normalized = match normalize_media_urn(urn) {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(
                    target: "capdag::fabric::registry",
                    urn = %urn, error = %e,
                    "get_cached_media_def received a malformed URN; treating as not found"
                );
                return None;
            }
        };
        if let Some(spec) = self
            .cached_media_defs
            .lock()
            .ok()
            .and_then(|m| m.get(&normalized).cloned())
        {
            return Some(spec);
        }
        let defver = self.media_defver(&normalized).ok()?;
        let runtime = tokio::runtime::Handle::try_current().ok()?;
        if !matches!(
            runtime.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            self.enqueue_for_background_fetch(FetchKey::Media {
                urn: normalized,
                defver,
            });
            return None;
        }
        let sync_attempt = tokio::task::block_in_place(|| {
            runtime.block_on(async {
                tokio::time::timeout(
                    SYNC_FETCH_DEADLINE,
                    fetch_one_media_def(
                        &self.client,
                        &self.cache_dir,
                        &self.cached_media_defs,
                        &self.extension_index,
                        &self.offline_flag,
                        &self.config,
                        &self.cache_revision_tx,
                        &normalized,
                        defver,
                    ),
                )
                .await
            })
        });
        match sync_attempt {
            Ok(Ok(spec)) => return Some(spec),
            Ok(Err(e)) => {
                tracing::debug!(
                    target: "capdag::fabric::registry",
                    urn = %normalized, error = %e,
                    "Synchronous media-def fetch errored within deadline; enqueueing for background fetch."
                );
            }
            Err(_elapsed) => {
                tracing::debug!(
                    target: "capdag::fabric::registry",
                    urn = %normalized,
                    "Synchronous media-def fetch did not complete within deadline; enqueueing for background fetch."
                );
            }
        }
        self.enqueue_for_background_fetch(FetchKey::Media {
            urn: normalized,
            defver,
        });
        None
    }

    /// Returns `true` if the URN is a bookend-eligible file format — its
    /// stored spec has at least one registered file extension.
    pub fn is_bookend(&self, urn: &str) -> bool {
        match self.get_cached_media_def(urn) {
            Some(spec) => !spec.extensions.is_empty(),
            None => false,
        }
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

    /// Returns all media URNs registered for the given file extension.
    pub fn media_urns_for_extension(
        &self,
        extension: &str,
    ) -> Result<Vec<String>, FabricRegistryError> {
        let ext_lower = extension.to_lowercase();
        let index = self.extension_index.lock().map_err(|e| {
            FabricRegistryError::CacheError(format!("Failed to lock extension index: {}", e))
        })?;
        index
            .get(&ext_lower)
            .cloned()
            .map(|mut urns| {
                // The index is populated from HashMap iteration and incremental
                // fetches, so its per-extension order varies run to run. Every
                // consumer that picks ONE candidate (extension detection, the
                // discriminators) tie-breaks by position, so an unsorted answer
                // makes detection nondeterministic across processes. Sort at
                // the single read choke point.
                urns.sort();
                urns
            })
            .ok_or_else(|| {
                FabricRegistryError::ExtensionNotFound(format!(
                    "No media def registered for extension '{}'",
                    extension
                ))
            })
    }

    /// Get all extension → URNs mappings.
    pub fn get_extension_mappings(
        &self,
    ) -> Result<Vec<(String, Vec<String>)>, FabricRegistryError> {
        let index = self.extension_index.lock().map_err(|e| {
            FabricRegistryError::CacheError(format!("Failed to lock extension index: {}", e))
        })?;
        Ok(index.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
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
        if let Ok(mut idx) = self.extension_index.lock() {
            for ext in &spec.extensions {
                let ext_lower = ext.to_lowercase();
                let urns = idx.entry(ext_lower).or_default();
                if !urns.contains(&spec.urn) {
                    urns.push(spec.urn.clone());
                }
            }
        }
        if let Ok(mut m) = self.manifest.lock() {
            m.media.insert(normalized, spec_version);
        }
        publish_cache_revision(&self.cache_revision_tx);
    }

    /// Check if a media URN exists in registry (cached or online).
    pub async fn media_def_exists(&self, urn: &str) -> bool {
        self.get_media_def(urn).await.is_ok()
    }

    // -------------------------------------------------------------------------
    // SHARED ADMIN API
    // -------------------------------------------------------------------------

    /// The on-disk cache root for this registry (per-registry-URL slug under
    /// the OS cache dir). Exposed so admin surfaces can report the path they
    /// purge/renew.
    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    /// Invalidate every cache for this registry — in-memory maps and the
    /// entire on-disk cache tree, INCLUDING the manifest snapshot. After
    /// this the next lookup (or a freshly constructed `FabricRegistry`)
    /// re-fetches the manifest and all bodies from the network, so this is
    /// the way to force a full renewal against a mutated channel.
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
        if let Ok(mut g) = self.extension_index.lock() {
            g.clear();
        }
        // The cache is SHARED, and the processes that clear it run at the same
        // time: a test run starts eight cartridge builds within milliseconds of
        // each other and every one of them refreshes this directory. Removing
        // the tree and putting it back is not safe between peers — one process
        // deleting an ancestor while another is creating into it is how a
        // build fails with `No such file or directory` on a path it had just
        // made.
        //
        // So the DIRECTORY is never removed, only its contents, and every step
        // tolerates a peer having done the same thing first. The end state is
        // the same — an empty cache — and no peer is ever left with the tree
        // missing underneath it.
        for sub in ["caps", "media", "aliases", "manifests"] {
            let directory = self.cache_dir.join(sub);
            fs::create_dir_all(&directory).map_err(|e| {
                FabricRegistryError::CacheError(format!(
                    "Failed to create cache directory {}: {e}",
                    directory.display()
                ))
            })?;
            let entries = match fs::read_dir(&directory) {
                Ok(entries) => entries,
                // A peer removed it between the create and the read. It is
                // empty by any reading, which is what was wanted.
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(FabricRegistryError::CacheError(format!(
                        "Failed to read cache directory {}: {error}",
                        directory.display()
                    )))
                }
            };
            for entry in entries.flatten() {
                let path = entry.path();
                let removed = if path.is_dir() {
                    fs::remove_dir_all(&path)
                } else {
                    fs::remove_file(&path)
                };
                match removed {
                    Ok(()) => {}
                    // Already gone: a peer clearing the same cache got there
                    // first, which is the outcome either way.
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    // Refilled underneath us. macOS reports this as
                    // `Directory not empty` where Linux reports the missing
                    // path — two faces of the same race, and neither is a
                    // failure of this clear: the entries now there were put
                    // there by a peer that has just refreshed them, so they
                    // are exactly what a refresh was for.
                    Err(error) if is_busy(&error) => {}
                    Err(error) => {
                        return Err(FabricRegistryError::CacheError(format!(
                            "Failed to remove {}: {error}",
                            path.display()
                        )))
                    }
                }
            }
        }
        Ok(())
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
    // DISK LOAD
    // -------------------------------------------------------------------------

    /// Walk the cap cache directory recursively, picking up both v0 flat
    /// files (`caps/<sha>.json`) and v >= 1 versioned files
    /// (`caps/<sha>/<defver>.json`). TTL applies only to v0 entries —
    /// v >= 1 entries are immutable by protocol so no expiry pass.
    fn load_all_cached_caps(caps_dir: &Path) -> Result<HashMap<String, Cap>, FabricRegistryError> {
        let mut caps = HashMap::new();
        if !caps_dir.exists() {
            return Ok(caps);
        }
        let mut stack: Vec<PathBuf> = vec![caps_dir.to_path_buf()];
        let mut is_v0_layer = true;
        while let Some(dir) = stack.pop() {
            // A directory removed between its parent being listed and it
            // being opened contributes nothing — the same answer the absent
            // cache root gives above. See `load_all_cached_aliases`.
            let listing = match fs::read_dir(&dir) {
                Ok(listing) => listing,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => {
                    return Err(FabricRegistryError::CacheError(format!(
                        "Failed to read cap cache directory {:?}: {}",
                        dir, e
                    )))
                }
            };
            for entry in listing {
                let entry = match entry {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!("Failed to read cap cache entry: {}", e);
                        continue;
                    }
                };
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|s| s.to_str()) != Some("json") {
                    continue;
                }
                let content = match fs::read_to_string(&path) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!("Failed to read cap cache file {:?}: {}", path, e);
                        continue;
                    }
                };
                let cache_entry: CapCacheEntry = match serde_json::from_str(&content) {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!("Failed to parse cap cache file {:?}: {}", path, e);
                        let _ = fs::remove_file(&path);
                        continue;
                    }
                };
                // TTL applies only to v0 (flat) entries. Versioned
                // entries are immutable by protocol.
                if cache_entry.definition.version == 0 && cache_entry.is_expired() {
                    let _ = fs::remove_file(&path);
                    continue;
                }
                let urn = cache_entry.definition.urn_string();
                // A cached entry whose URN no longer parses is a corrupt cache
                // entry; surface it as an error rather than silently insert it
                // under a raw, unnormalized key.
                caps.insert(normalize_cap_urn(&urn)?, cache_entry.definition);
            }
            let _ = is_v0_layer;
            is_v0_layer = false;
        }
        Ok(caps)
    }

    /// Same recursive walk as `load_all_cached_caps`, for media defs.
    fn load_all_cached_media_defs(
        media_dir: &Path,
    ) -> Result<HashMap<String, StoredMediaDef>, FabricRegistryError> {
        let mut specs = HashMap::new();
        if !media_dir.exists() {
            return Ok(specs);
        }
        let mut stack: Vec<PathBuf> = vec![media_dir.to_path_buf()];
        while let Some(dir) = stack.pop() {
            // A directory removed between its parent being listed and it
            // being opened contributes nothing — the same answer the absent
            // cache root gives above. See `load_all_cached_aliases`.
            let listing = match fs::read_dir(&dir) {
                Ok(listing) => listing,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => {
                    return Err(FabricRegistryError::CacheError(format!(
                        "Failed to read media cache directory {:?}: {}",
                        dir, e
                    )))
                }
            };
            for entry in listing {
                let entry = match entry {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!("Failed to read media cache entry: {}", e);
                        continue;
                    }
                };
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|s| s.to_str()) != Some("json") {
                    continue;
                }
                let content = match fs::read_to_string(&path) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!("Failed to read media cache file {:?}: {}", path, e);
                        continue;
                    }
                };
                let cache_entry: MediaCacheEntry = match serde_json::from_str(&content) {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!("Failed to parse media cache file {:?}: {}", path, e);
                        let _ = fs::remove_file(&path);
                        continue;
                    }
                };
                if cache_entry.spec.version == 0 && cache_entry.is_expired() {
                    let _ = fs::remove_file(&path);
                    continue;
                }
                // Corrupt cache entry (URN no longer parses) surfaces as an
                // error rather than a silent raw-key insertion.
                specs.insert(
                    normalize_media_urn(&cache_entry.spec.urn)?,
                    cache_entry.spec,
                );
            }
        }
        Ok(specs)
    }

    /// Walk the alias cache directory (`aliases/<sha>/<defver>.json`) and
    /// load every cached `StoredAlias` keyed by its normalized name.
    /// Aliases are versioned-only — there is no v0 flat path and no TTL
    /// expiry (a published defver is immutable).
    fn load_all_cached_aliases(
        aliases_dir: &Path,
    ) -> Result<HashMap<String, StoredAlias>, FabricRegistryError> {
        let mut aliases = HashMap::new();
        if !aliases_dir.exists() {
            return Ok(aliases);
        }
        let mut stack: Vec<PathBuf> = vec![aliases_dir.to_path_buf()];
        while let Some(dir) = stack.pop() {
            // A directory that is gone by the time it is read contributes no
            // aliases, which is the same answer an absent cache root gives
            // above.
            //
            // The check at the top is one moment; this loop reads directories
            // it discovered afterwards. Anything that empties the cache while
            // a registry is being built — a refresh, a clear, another process
            // taking a snapshot — removes a `<sha>` directory between its
            // parent being listed and it being opened. Failing there turns a
            // cache that is merely emptier than expected into "the fabric
            // cannot be reached", and a build refuses over a directory nobody
            // needed.
            //
            // Only NotFound. Any other error is a cache that cannot be read
            // rather than one that is not there, and is still reported.
            let listing = match fs::read_dir(&dir) {
                Ok(listing) => listing,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => {
                    return Err(FabricRegistryError::CacheError(format!(
                        "Failed to read alias cache directory {:?}: {}",
                        dir, e
                    )))
                }
            };
            for entry in listing {
                let entry = match entry {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!("Failed to read alias cache entry: {}", e);
                        continue;
                    }
                };
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|s| s.to_str()) != Some("json") {
                    continue;
                }
                let content = match fs::read_to_string(&path) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!("Failed to read alias cache file {:?}: {}", path, e);
                        continue;
                    }
                };
                let cache_entry: AliasCacheEntry = match serde_json::from_str(&content) {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!("Failed to parse alias cache file {:?}: {}", path, e);
                        let _ = fs::remove_file(&path);
                        continue;
                    }
                };
                aliases.insert(cache_entry.alias.name.clone(), cache_entry.alias);
            }
        }
        Ok(aliases)
    }

    fn build_extension_index(
        specs: &HashMap<String, StoredMediaDef>,
    ) -> HashMap<String, Vec<String>> {
        let mut index: HashMap<String, Vec<String>> = HashMap::new();
        for spec in specs.values() {
            for ext in &spec.extensions {
                let ext_lower = ext.to_lowercase();
                index.entry(ext_lower).or_default().push(spec.urn.clone());
            }
        }
        index
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
        let cache_dir = PathBuf::from("/tmp/capdag-test-cache");
        let _ = fs::create_dir_all(cache_dir.join("caps"));
        let _ = fs::create_dir_all(cache_dir.join("media"));
        let _ = fs::create_dir_all(cache_dir.join("aliases"));
        let _ = fs::create_dir_all(cache_dir.join("manifests"));
        let cached_caps = Arc::new(Mutex::new(HashMap::new()));
        let cached_media_defs = Arc::new(Mutex::new(HashMap::new()));
        let cached_aliases = Arc::new(Mutex::new(HashMap::new()));
        let extension_index = Arc::new(Mutex::new(HashMap::new()));
        let manifest_arc = Arc::new(Mutex::new(Manifest::empty(manifest_version)));
        let fetch_in_queue = Arc::new(Mutex::new(HashSet::new()));
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
                    Arc::clone(&extension_index),
                    Arc::clone(&manifest_arc),
                    Arc::clone(&fetch_in_queue),
                    Arc::clone(&offline_flag),
                    config.clone(),
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
            extension_index,
            config,
            manifest_version,
            manifest: manifest_arc,
            offline_flag,
            fetch_queue_tx,
            fetch_in_queue,
            cache_revision_tx,
        };
        registry.ensure_identity_cap();
        registry
    }
}

// =============================================================================
// ATOMIC FETCH HELPERS (free functions)
// =============================================================================

/// Build the R2 URL for a per-cap object at the given defver. defver==0
/// addresses the frozen v0 flat path; defver>=1 addresses the versioned
/// subpath. The cache file path mirrors the URL structure.
fn cap_url_and_cache_path(
    cache_dir: &Path,
    config: &RegistryConfig,
    normalized_urn: &str,
    defver: u32,
) -> (String, PathBuf) {
    let mut hasher = Sha256::new();
    hasher.update(normalized_urn.as_bytes());
    let hash = format!("{:x}", hasher.finalize());
    if defver == 0 {
        (
            format!("{}/caps/{}", config.registry_base_url, hash),
            cache_dir.join("caps").join(format!("{}.json", hash)),
        )
    } else {
        (
            format!("{}/caps/{}/{}.json", config.registry_base_url, hash, defver),
            cache_dir
                .join("caps")
                .join(&hash)
                .join(format!("{}.json", defver)),
        )
    }
}

/// Build the R2 URL for a per-media object at the given defver.
fn media_url_and_cache_path(
    cache_dir: &Path,
    config: &RegistryConfig,
    normalized_urn: &str,
    defver: u32,
) -> (String, PathBuf) {
    let mut hasher = Sha256::new();
    hasher.update(normalized_urn.as_bytes());
    let hash = format!("{:x}", hasher.finalize());
    if defver == 0 {
        (
            format!("{}/media/{}", config.registry_base_url, hash),
            cache_dir.join("media").join(format!("{}.json", hash)),
        )
    } else {
        (
            format!(
                "{}/media/{}/{}.json",
                config.registry_base_url, hash, defver
            ),
            cache_dir
                .join("media")
                .join(&hash)
                .join(format!("{}.json", defver)),
        )
    }
}

/// Build the R2 URL for a per-alias object at the given defver. Aliases
/// are keyed by `sha256(normalized_name)` and are versioned-only (defver
/// >= 1); there is no v0 flat path.
fn alias_url_and_cache_path(
    cache_dir: &Path,
    config: &RegistryConfig,
    normalized_name: &str,
    defver: u32,
) -> (String, PathBuf) {
    let mut hasher = Sha256::new();
    hasher.update(normalized_name.as_bytes());
    let hash = format!("{:x}", hasher.finalize());
    (
        format!(
            "{}/aliases/{}/{}.json",
            config.registry_base_url, hash, defver
        ),
        cache_dir
            .join("aliases")
            .join(&hash)
            .join(format!("{}.json", defver)),
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
    config: &RegistryConfig,
    cache_revision_tx: &watch::Sender<u64>,
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
    let (url, cache_path) = alias_url_and_cache_path(cache_dir, config, normalized_name, defver);
    let response = client.get(&url).send().await.map_err(|e| {
        FabricRegistryError::HttpError(format!(
            "Failed to fetch alias '{}': {}",
            normalized_name, e
        ))
    })?;
    if !response.status().is_success() {
        return Err(FabricRegistryError::NotFound(format!(
            "alias '{}' not found in registry (HTTP {}) at {}",
            normalized_name,
            response.status(),
            url
        )));
    }
    let body = response.text().await.map_err(|e| {
        FabricRegistryError::HttpError(format!(
            "Failed to read alias '{}' body: {}",
            normalized_name, e
        ))
    })?;
    let alias: StoredAlias = serde_json::from_str(&body).map_err(|e| {
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
    if let Some(parent) = cache_path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            FabricRegistryError::CacheError(format!(
                "Failed to create alias cache dir {:?}: {}",
                parent, e
            ))
        })?;
    }
    let serialized = serde_json::to_string(&entry).map_err(|e| {
        FabricRegistryError::CacheError(format!("Failed to serialize alias cache entry: {}", e))
    })?;
    fs::write(cache_path, serialized).map_err(|e| {
        FabricRegistryError::CacheError(format!(
            "Failed to write alias cache file {:?}: {}",
            cache_path, e
        ))
    })?;
    if let Ok(mut guard) = cached_aliases.lock() {
        guard.insert(alias.name.clone(), alias.clone());
    }
    publish_cache_revision(cache_revision_tx);
    Ok(())
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
    extension_index: &Arc<Mutex<HashMap<String, Vec<String>>>>,
    manifest: &Arc<Mutex<Manifest>>,
    offline_flag: &Arc<AtomicBool>,
    config: &RegistryConfig,
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

    let (url, cache_file) = cap_url_and_cache_path(cache_dir, config, normalized_urn, defver);

    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| FabricRegistryError::HttpError(format!("Failed to fetch cap: {}", e)))?;
    if !response.status().is_success() {
        return Err(FabricRegistryError::NotFound(format!(
            "Cap '{}' (defver {}) not found in registry (HTTP {}) at {}",
            normalized_urn,
            defver,
            response.status(),
            url
        )));
    }
    let cap: Cap = response.json().await.map_err(|e| {
        FabricRegistryError::ParseError(format!("Failed to parse cap '{}': {}", normalized_urn, e))
    })?;

    // Walk every media URN referenced by the cap. Empty/wildcard URN
    // (`media:`) is the identity / wildcard sentinel — it has no
    // fetchable spec and must be skipped.
    let mut referenced: Vec<String> = Vec::new();
    // A malformed media URN here means the fetched cap body is corrupt; the
    // closure returns the parse error so the atomic fetch fails rather than
    // caching a cap that references an unnormalizable media URN.
    let push = |v: &mut Vec<String>, s: &str| -> Result<(), FabricRegistryError> {
        let n = normalize_media_urn(s)?;
        if n != "media:" && !v.contains(&n) {
            v.push(n);
        }
        Ok(())
    };
    push(&mut referenced, cap.urn.in_spec())?;
    push(&mut referenced, cap.urn.out_spec())?;
    for arg in &cap.args {
        push(&mut referenced, &arg.media_urn)?;
        for source in &arg.sources {
            if let ArgSource::Stdin { stdin } = source {
                push(&mut referenced, stdin)?;
            }
        }
    }
    if let Some(out) = &cap.output {
        push(&mut referenced, &out.media_urn)?;
    }

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
            extension_index,
            offline_flag,
            config,
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
            return Err(FabricRegistryError::NotFound(format!(
                "cap '{}' references media URN '{}' which could not be fetched: {}",
                normalized_urn, media_urn, e
            )));
        }
    }

    // All referenced media defs in cache. Write the cap.
    let cache_entry = CapCacheEntry {
        definition: cap.clone(),
        cached_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        ttl_hours: CACHE_DURATION_HOURS,
    };
    if let Some(parent) = cache_file.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            FabricRegistryError::CacheError(format!(
                "Failed to create cap cache parent directory {:?}: {}",
                parent, e
            ))
        })?;
    }
    let content = serde_json::to_string_pretty(&cache_entry).map_err(|e| {
        FabricRegistryError::CacheError(format!("Failed to serialize cap cache entry: {}", e))
    })?;
    fs::write(&cache_file, content).map_err(|e| {
        FabricRegistryError::CacheError(format!("Failed to write cap cache file: {}", e))
    })?;

    if let Ok(mut cached) = cached_caps.lock() {
        cached.insert(normalized_urn.to_string(), cap.clone());
    }
    publish_cache_revision(cache_revision_tx);

    Ok(cap)
}

/// Atomic media-def fetcher.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn fetch_one_media_def(
    client: &reqwest::Client,
    cache_dir: &Path,
    cached_media_defs: &Arc<Mutex<HashMap<String, StoredMediaDef>>>,
    extension_index: &Arc<Mutex<HashMap<String, Vec<String>>>>,
    offline_flag: &Arc<AtomicBool>,
    config: &RegistryConfig,
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

    let (url, cache_file) = media_url_and_cache_path(cache_dir, config, normalized_urn, defver);

    let response =
        client.get(&url).send().await.map_err(|e| {
            FabricRegistryError::HttpError(format!("Failed to fetch media def: {}", e))
        })?;
    if !response.status().is_success() {
        return Err(FabricRegistryError::NotFound(format!(
            "Media def '{}' (defver {}) not found in registry (HTTP {}) at {}",
            normalized_urn,
            defver,
            response.status(),
            url
        )));
    }
    let spec: StoredMediaDef = response.json().await.map_err(|e| {
        FabricRegistryError::ParseError(format!(
            "Failed to parse media def '{}': {}",
            normalized_urn, e
        ))
    })?;

    let cache_entry = MediaCacheEntry {
        spec: spec.clone(),
        cached_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        ttl_hours: CACHE_DURATION_HOURS,
    };
    if let Some(parent) = cache_file.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            FabricRegistryError::CacheError(format!(
                "Failed to create media cache parent directory {:?}: {}",
                parent, e
            ))
        })?;
    }
    let content = serde_json::to_string_pretty(&cache_entry).map_err(|e| {
        FabricRegistryError::CacheError(format!("Failed to serialize media cache entry: {}", e))
    })?;
    fs::write(&cache_file, content).map_err(|e| {
        FabricRegistryError::CacheError(format!("Failed to write media cache file: {}", e))
    })?;

    if let Ok(mut cached) = cached_media_defs.lock() {
        cached.insert(normalized_urn.to_string(), spec.clone());
    }
    if let Ok(mut idx) = extension_index.lock() {
        for ext in &spec.extensions {
            let ext_lower = ext.to_lowercase();
            let urns = idx.entry(ext_lower).or_default();
            if !urns.contains(&spec.urn) {
                urns.push(spec.urn.clone());
            }
        }
    }
    publish_cache_revision(cache_revision_tx);
    Ok(spec)
}

/// Manifest bootstrap. Tries the local cache first; falls back to a
/// blocking network GET; if neither produces a manifest, returns an
/// error — there is no v0 fallback (caller chose v >= 1 explicitly).
async fn load_or_fetch_manifest(
    manifests_dir: &Path,
    client: &reqwest::Client,
    config: &RegistryConfig,
    version: u32,
    bypass_cache: bool,
) -> Result<Manifest, FabricRegistryError> {
    let cache_file = manifests_dir.join(format!("{}.json", version));
    // In bypass mode skip the cached manifest entirely and fetch fresh — the
    // fresh copy still writes through below so later cached reads are current.
    if !bypass_cache && cache_file.exists() {
        let content = fs::read_to_string(&cache_file).map_err(|e| {
            FabricRegistryError::CacheError(format!(
                "Failed to read cached manifest at {:?}: {}",
                cache_file, e
            ))
        })?;
        match serde_json::from_str::<Manifest>(&content) {
            Ok(m) => {
                if m.version != version {
                    return Err(FabricRegistryError::ParseError(format!(
                        "Cached manifest at {:?} reports version {} but file is {}.json",
                        cache_file, m.version, version
                    )));
                }
                return Ok(m);
            }
            Err(e) => {
                tracing::warn!(
                    "Cached manifest at {:?} did not parse: {}; re-fetching from network",
                    cache_file,
                    e
                );
                let _ = fs::remove_file(&cache_file);
            }
        }
    }

    let url = format!("{}/manifest/{}.json", config.registry_base_url, version);
    let response = client.get(&url).send().await.map_err(|e| {
        FabricRegistryError::HttpError(format!(
            "Failed to fetch manifest v{} at {}: {}",
            version, url, e
        ))
    })?;
    if !response.status().is_success() {
        return Err(FabricRegistryError::NotFound(format!(
            "Manifest v{} not found in registry (HTTP {}) at {}",
            version,
            response.status(),
            url
        )));
    }
    let body = response.text().await.map_err(|e| {
        FabricRegistryError::HttpError(format!("Failed to read manifest v{} body: {}", version, e))
    })?;
    let manifest: Manifest = serde_json::from_str(&body).map_err(|e| {
        FabricRegistryError::ParseError(format!("Failed to parse manifest v{}: {}", version, e))
    })?;
    if manifest.version != version {
        return Err(FabricRegistryError::ParseError(format!(
            "Manifest fetched as v{} reports version {}",
            version, manifest.version
        )));
    }
    fs::write(&cache_file, &body).map_err(|e| {
        FabricRegistryError::CacheError(format!(
            "Failed to write manifest cache to {:?}: {}",
            cache_file, e
        ))
    })?;
    Ok(manifest)
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
    extension_index: Arc<Mutex<HashMap<String, Vec<String>>>>,
    manifest: Arc<Mutex<Manifest>>,
    fetch_in_queue: Arc<Mutex<HashSet<FetchKey>>>,
    offline_flag: Arc<AtomicBool>,
    config: RegistryConfig,
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
                    match fetch_one_cap_atomic(
                        &client,
                        &cache_dir,
                        &cached_caps,
                        &cached_media_defs,
                        &extension_index,
                        &manifest,
                        &offline_flag,
                        &config,
                        manifest_version,
                        &cache_revision_tx,
                        normalized_urn,
                        *defver,
                    )
                    .await
                    {
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
                    match fetch_one_media_def(
                        &client,
                        &cache_dir,
                        &cached_media_defs,
                        &extension_index,
                        &offline_flag,
                        &config,
                        &cache_revision_tx,
                        normalized_urn,
                        *defver,
                    )
                    .await
                    {
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
                    match fetch_one_alias(
                        &client,
                        &cache_dir,
                        &cached_aliases,
                        &offline_flag,
                        &config,
                        &cache_revision_tx,
                        normalized_name,
                        *defver,
                    )
                    .await
                    {
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

#[derive(Debug, thiserror::Error)]
pub enum FabricRegistryError {
    #[error("HTTP error: {0}")]
    HttpError(String),

    #[error("Not found in registry: {0}")]
    NotFound(String),

    #[error("Failed to parse registry response: {0}")]
    ParseError(String),

    #[error("Cache error: {0}")]
    CacheError(String),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Network access blocked: {0}")]
    NetworkBlocked(String),

    #[error("No media def registered for extension: {0}")]
    ExtensionNotFound(String),
}

// =============================================================================
// ALIAS TESTS
// =============================================================================

#[cfg(test)]
mod alias_tests {
    use super::*;
    use crate::cap::definition::{Cap, CapArg, CapOutput};
    use crate::CapUrn;

    fn cap_with_urn(urn_str: &str) -> Cap {
        let urn = CapUrn::from_string(urn_str).expect("valid cap urn");
        Cap {
            urn,
            version: 1,
            title: "T".to_string(),
            cap_description: None,
            documentation: None,
            metadata: std::collections::HashMap::new(),
            aliases: vec!["test://cap".to_string()],
            is_abstract: false,
            args: vec![CapArg::new(
                "media:ext=pdf".to_string(),
                true,
                vec![ArgSource::Stdin {
                    stdin: "media:ext=pdf".to_string(),
                }],
            )],
            output: Some(CapOutput::new(
                "media:enc=utf-8".to_string(),
                "out".to_string(),
            )),
            metadata_json: None,
            registered_by: None,
            supported_model_types: Vec::new(),
            default_model_spec: None,
        }
    }

    fn media_spec(urn: &str) -> StoredMediaDef {
        StoredMediaDef {
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
        let json = r#"{"version":1,"previous":0,"caps":{},"media":{},"aliases":{"pdf2text":3,"jsondoc":1}}"#;
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
            target: "media:fmt=json;record".to_string(),
            version: 1,
        });
        let target = registry.resolve_alias("jsondoc").await.expect("resolves");
        assert_eq!(target, "media:fmt=json;record");
        // Case-insensitive: the same alias resolves regardless of input case.
        let upper = registry.resolve_alias("JSONDoc").await.expect("resolves");
        assert_eq!(upper, "media:fmt=json;record");
        // Malformed name fails hard (not silently None).
        assert!(registry.resolve_alias("bad:name").await.is_err());
    }

    // TEST8063: narrow_abstract_cap resolves an abstract cap + detected input to
    // the unique concrete cap via is_dispatchable — the CLI's dispatch step.
    // Exercises all three outcomes: unique (fixed-output disbind), ambiguous
    // (convert-image jpeg with no --to), and no-handler.
    #[tokio::test]
    async fn test8063_narrow_abstract_cap() {
        use crate::MediaUrn;
        let registry = FabricRegistry::new_for_test();
        let disbind_pdf = Cap::new(
            CapUrn::from_string(
                r#"cap:disbind;in="media:ext=pdf";out="media:enc=utf-8;ext=txt;page;plain-text""#,
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
            CapUrn::from_string(r#"cap:disbind;out="media:enc=utf-8;ext=txt;page;plain-text""#)
                .unwrap();
        let pdf = MediaUrn::from_string("media:ext=pdf").unwrap();
        let concrete = registry
            .narrow_abstract_cap(&disbind_abstract, &pdf, None)
            .await
            .expect("disbind must narrow uniquely for a pdf input");
        assert!(concrete.to_string().contains("ext=pdf"));

        // convert-image with jpeg but no target is AMBIGUOUS (png or webp).
        let convert_abstract = CapUrn::from_string("cap:convert-image").unwrap();
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
    }

    // TEST1889: resolve_alias_typed enforces the expected kind. A media
    // alias requested as a cap fails hard; requested as media (or untyped)
    // succeeds. This is the typed-boundary contract.
    #[tokio::test]
    async fn test1889_resolve_alias_typed_enforces_kind() {
        let registry = FabricRegistry::new_for_test();
        registry.insert_cached_alias_for_test(StoredAlias {
            name: "jsondoc".to_string(),
            target: "media:fmt=json;record".to_string(),
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
        let cap_urn = "cap:extract;in=\"media:ext=pdf\";out=\"media:enc=utf-8\"";
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
            target: "media:fmt=json;record".to_string(),
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
        registry.insert_cached_media_def_for_test(media_spec("media:fmt=json;record"));
        registry.insert_cached_alias_for_test(StoredAlias {
            name: "jsondoc".to_string(),
            target: "media:fmt=json;record".to_string(),
            version: 1,
        });
        let spec = registry
            .get_media_def("jsondoc")
            .await
            .expect("media alias resolves");
        assert_eq!(spec.urn, "media:fmt=json;record");

        // A cap alias at the media boundary → hard error.
        let cap_urn = "cap:extract;in=\"media:ext=pdf\";out=\"media:enc=utf-8\"";
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
        // resolve_alias_cached returns None for an unknown (and for malformed).
        assert!(registry.resolve_alias_cached("nosuchalias").is_none());
        assert!(registry.resolve_alias_cached("bad:name").is_none());
    }
}

// =============================================================================
// PARITY PORTS — shared tests that existed in the mirrors but were missing
// from the Rust reference. Same number, same behavior, same method.
// =============================================================================

#[cfg(test)]
mod parity_port_tests {
    use super::*;

    fn spec(urn: &str, title: &str) -> StoredMediaDef {
        StoredMediaDef {
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
            target: "cap:coerce;in=\"media:integer;numeric\";out=\"media:enc=utf-8\"".to_string(),
            version: 1,
        });
        registry.insert_cached_alias_for_test(StoredAlias {
            name: "i2s".to_string(),
            target: "cap:coerce;in=\"media:integer;numeric\";out=\"media:enc=utf-8\"".to_string(),
            version: 1,
        });
        // A media alias too.
        registry.insert_cached_alias_for_test(StoredAlias {
            name: "json".to_string(),
            target: "media:fmt=json;record".to_string(),
            version: 1,
        });

        // Canonical query → shortest alias wins.
        assert_eq!(
            registry
                .display_alias_for_urn(
                    "cap:coerce;in=\"media:integer;numeric\";out=\"media:enc=utf-8\""
                )
                .as_deref(),
            Some("i2s")
        );
        // NON-canonical query (media tags reordered, cap arg order swapped):
        // must still resolve via canonicalisation. `media:record;fmt=json`
        // canonicalises to `media:fmt=json;record`.
        assert_eq!(
            registry
                .display_alias_for_urn("media:record;fmt=json")
                .as_deref(),
            Some("json")
        );
        // A real URN with no alias → None.
        assert_eq!(
            registry.display_alias_for_urn("media:enc=utf-8;ext=pdf"),
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
            target: "cap:coerce;in=\"media:integer;numeric\";out=\"media:enc=utf-8\"".to_string(),
            version: 1,
        });
        registry.insert_cached_alias_for_test(StoredAlias {
            name: "json".to_string(),
            target: "media:fmt=json;record".to_string(),
            version: 1,
        });
        let cap_aliases = registry.cached_cap_aliases();
        // Only the cap alias is returned; the media alias is filtered out.
        assert_eq!(cap_aliases.len(), 1, "got: {cap_aliases:?}");
        assert_eq!(cap_aliases[0].0, "int2str");
        assert_eq!(
            cap_aliases[0].1,
            "cap:coerce;in=\"media:integer;numeric\";out=\"media:enc=utf-8\""
        );
    }

    // TEST147: Test registry for test with custom config creates registry with specified URLs
    #[test]
    fn test147_registry_for_test_with_custom_config() {
        let config = RegistryConfig::new().with_registry_url("https://example.test/registry");
        let registry = FabricRegistry::new_for_test_with_config(config);
        assert_eq!(
            registry.config().registry_base_url,
            "https://example.test/registry"
        );
    }

    // TEST1899: a media def published under a manifest (v>=1) resolves to the
    // VERSIONED object path `/media/<sha>/<defver>.json`, never the legacy
    // flat path `/media/<sha>`. The flat path is the pre-manifest (v0) layout;
    // a registry that silently runs in v0 mode fetches it and 404s every
    // lookup against a versioned registry — the exact regression where a
    // fabric-registry mirror defaulted its manifest version to 0. This pins
    // both the URL rule and the manifest-driven defver resolution.
    #[test]
    fn test1899_media_def_resolves_to_versioned_object_path_under_manifest() {
        // 1. Object-path rule: defver >= 1 → versioned; defver 0 → flat.
        let config = RegistryConfig::new().with_registry_url("https://fabric.example.test");
        let cache = std::path::Path::new("/tmp/capdag-test-cache-0144");
        let urn = "media:enc=utf-8;ext=md";
        let mut hasher = Sha256::new();
        hasher.update(urn.as_bytes());
        let hash = format!("{:x}", hasher.finalize());

        let (versioned, _) = media_url_and_cache_path(cache, &config, urn, 1);
        assert_eq!(
            versioned,
            format!("https://fabric.example.test/media/{}/1.json", hash),
            "a def at manifest defver 1 must resolve to the versioned object path"
        );
        let (flat, _) = media_url_and_cache_path(cache, &config, urn, 0);
        assert_eq!(
            flat,
            format!("https://fabric.example.test/media/{}", hash),
            "defver 0 is the legacy flat path — the wrong target for a versioned registry"
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

    // TEST607: media_urns_for_extension returns error for unknown extension
    #[test]
    fn test607_media_urns_for_extension_unknown() {
        let registry = FabricRegistry::new_for_test();
        let err = registry
            .media_urns_for_extension("zzzzunknown")
            .unwrap_err();
        assert!(format!("{err}").contains("zzzzunknown"));
    }

    // TEST608: media_urns_for_extension returns URNs after adding a spec with extensions
    #[test]
    fn test608_media_urns_for_extension_populated() {
        let registry = FabricRegistry::new_for_test();
        registry.insert_cached_media_def_for_test(spec_with_ext(
            "media:ext=pdf",
            "PDF Document",
            "application/pdf",
            &["pdf"],
        ));
        let urns = registry.media_urns_for_extension("pdf").unwrap();
        assert!(!urns.is_empty());
        assert!(urns.iter().any(|u| u.contains("pdf")));
        let urns_upper = registry.media_urns_for_extension("PDF").unwrap();
        assert_eq!(urns, urns_upper);
    }

    // TEST609: get_extension_mappings returns all registered extension→URN pairs.
    #[test]
    fn test609_get_extension_mappings() {
        let registry = FabricRegistry::new_for_test();
        registry.insert_cached_media_def_for_test(spec_with_ext(
            "media:ext=pdf",
            "PDF",
            "application/octet-stream",
            &["pdf"],
        ));
        registry.insert_cached_media_def_for_test(spec_with_ext(
            "media:ext=epub",
            "EPUB",
            "application/octet-stream",
            &["epub"],
        ));
        let mappings = registry.get_extension_mappings().unwrap();
        let exts: HashSet<String> = mappings.iter().map(|(e, _)| e.clone()).collect();
        assert!(exts.contains("pdf"));
        assert!(exts.contains("epub"));
    }

    // TEST610: get_cached_spec returns None for unknown and Some for known
    #[test]
    fn test610_get_cached_media_def() {
        let registry = FabricRegistry::new_for_test();
        assert!(registry
            .get_cached_media_def("media:nonexistent;xyzzy")
            .is_none());
        registry.insert_cached_media_def_for_test(spec("media:enc=utf-8;test;spec", "Test Spec"));
        let got = registry
            .get_cached_media_def("media:enc=utf-8;test;spec")
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
        let mut s = spec_with_ext("media:ext=pdf", "PDF Document", "application/pdf", &["pdf"]);
        s.profile_uri = Some("https://capdag.com/schema/pdf".to_string());
        s.description = Some("PDF document data".to_string());
        let def = s.to_media_def_def();
        assert_eq!(def.urn, "media:ext=pdf");
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
        let malformed = "cap:coerce;in=\"media:integer;numeric\";out=media:enc=utf-8";

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
            urn,
            version: 0,
            title: "Test Cap".to_string(),
            cap_description: None,
            documentation: None,
            metadata: std::collections::HashMap::new(),
            aliases: vec!["test".to_string()],
            is_abstract: false,
            args: Vec::<CapArg>::new(),
            output: Some(CapOutput::new("media:void".to_string(), "void".to_string())),
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

    // The per-cap registry URL: SHA-256 hex of the canonical cap URN under the
    // `/caps/` prefix. Mirrors the construction in `cap_url_and_cache_path`.
    fn cap_registry_url(config: &RegistryConfig, cap_urn: &str) -> String {
        let normalized = normalize_cap_urn(cap_urn).expect("test passes a valid cap URN");
        let mut hasher = Sha256::new();
        hasher.update(normalized.as_bytes());
        format!("{}/caps/{:x}", config.registry_base_url, hasher.finalize())
    }

    // TEST138: Test parsing registry JSON with stdin args verifies stdin media URN extraction
    #[test]
    fn test138_parse_registry_json_with_stdin() {
        let json = r#"{"urn":"cap:in=\"media:ext=pdf\";disbind;out=\"media:enc=utf-8;page\"","aliases": ["disbind"],"title":"Disbind PDF","args":[{"media_urn":"media:ext=pdf","required":true,"sources":[{"stdin":"media:ext=pdf"}]}]}"#;
        let cap: Cap = serde_json::from_str(json).expect("cap parses");
        assert_eq!(cap.title, "Disbind PDF");
        assert!(cap.accepts_stdin());
        assert_eq!(cap.get_stdin_media_urn(), Some("media:ext=pdf"));
    }

    // TEST6382: Test parsing registry JSON without stdin args verifies cap structure
    #[test]
    fn test6382_parse_registry_json_no_stdin() {
        let json = r#"{"urn":"cap:in=\"media:listing-id\";use-grinder;out=\"media:id;task\"","aliases": ["grinder_task"],"title":"Create Grinder Tool Task","args":[{"media_urn":"media:listing-id","required":true,"sources":[{"cli_flag":"--listing-id"}]}]}"#;
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
        let config = RegistryConfig::default();
        let url = cap_registry_url(
            &config,
            "cap:in=\"media:listing-id\";use-grinder;out=\"media:id;task\"",
        );
        let after = url.split("/caps/").nth(1).expect("URL has /caps/ segment");
        assert_eq!(after.len(), 64, "SHA-256 hex digest is 64 characters");
        assert!(after.chars().all(|c| c.is_ascii_hexdigit()));
    }

    // TEST142: Different tag orders normalise to the same URL — the canonicaliser strips the variation before hashing.
    #[test]
    fn test142_normalize_handles_different_tag_orders() {
        let config = RegistryConfig::default();
        let a = cap_registry_url(
            &config,
            "cap:test;in=\"media:enc=utf-8\";out=\"media:record\"",
        );
        let b = cap_registry_url(
            &config,
            "cap:in=\"media:enc=utf-8\";out=\"media:record\";test",
        );
        assert_eq!(a, b, "different tag orders produce the same URL");
    }

    // TEST143: Default config points at https://fabric.capdag.com/ unless overridden by CDG_FABRIC_REGISTRY_URL.
    #[test]
    fn test143_default_config() {
        let config = RegistryConfig::default();
        match std::env::var("CDG_FABRIC_REGISTRY_URL") {
            Ok(url) if !url.is_empty() => assert_eq!(config.registry_base_url, url),
            _ => assert_eq!(config.registry_base_url, "https://fabric.capdag.com"),
        }
        assert!(config.schema_base_url.contains("/schema"));
    }

    // TEST144: Test custom registry URL updates both registry and schema base URLs
    #[test]
    fn test144_custom_registry_url() {
        let config = RegistryConfig::default().with_registry_url("https://localhost:8888");
        assert_eq!(config.registry_base_url, "https://localhost:8888");
        assert_eq!(config.schema_base_url, "https://localhost:8888/schema");
    }

    // TEST145: Test custom registry and schema URLs set independently
    #[test]
    fn test145_custom_registry_and_schema_url() {
        let config = RegistryConfig::default()
            .with_registry_url("https://localhost:8888")
            .with_schema_url("https://schemas.example.com");
        assert_eq!(config.registry_base_url, "https://localhost:8888");
        assert_eq!(config.schema_base_url, "https://schemas.example.com");
    }

    // TEST146: Test schema URL not overwritten when set explicitly before registry URL
    #[test]
    fn test146_schema_url_not_overwritten_when_explicit() {
        let config = RegistryConfig::default()
            .with_schema_url("https://schemas.example.com")
            .with_registry_url("https://localhost:8888");
        assert_eq!(config.registry_base_url, "https://localhost:8888");
        assert_eq!(config.schema_base_url, "https://schemas.example.com");
    }

    // TEST1893: The on-disk cache root is namespaced per registry origin, so a
    // prod-populated cache can never satisfy a staging lookup (and vice versa).
    // Without this, a `CDG_FABRIC_REGISTRY_URL=staging` run reuses the
    // prod-cached manifest/caps under one shared `capdag/` directory and
    // resolves against the wrong snapshot — the bug that made `--staging`
    // appear not to reach the scenario tests. This pins three properties:
    // distinct origins → distinct roots; same origin → identical root
    // (deterministic, so caching actually hits); and the slug is the same
    // `slug_for` scheme the cartridge registry layout uses.
    #[test]
    fn test1893_cache_root_is_namespaced_per_registry_origin() {
        let prod = FabricRegistry::default_cache_root("https://fabric.capdag.com")
            .expect("prod cache root");
        let staging = FabricRegistry::default_cache_root("https://fabric-staging.capdag.com")
            .expect("staging cache root");
        let staging_again = FabricRegistry::default_cache_root("https://fabric-staging.capdag.com")
            .expect("staging cache root again");

        assert_ne!(
            prod, staging,
            "prod and staging must not share a cache root — they serve different bytes for the same URN/version"
        );
        assert_eq!(
            staging, staging_again,
            "the same registry origin must map to a stable cache root, or caching never hits"
        );

        // The final path component is exactly the cartridge-registry slug of
        // the origin URL — one slug scheme across the codebase.
        let slug =
            crate::bifaci::cartridge_slug::slug_for(Some("https://fabric-staging.capdag.com"));
        assert_eq!(
            staging.file_name().and_then(|s| s.to_str()),
            Some(slug.as_str()),
            "cache root must end in slug_for(registry_url)"
        );
        // And the parent of that slug is the shared `capdag` cache directory.
        assert_eq!(
            staging
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|s| s.to_str()),
            Some("capdag"),
            "the per-origin slug must live under the capdag cache directory"
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
            metadata: Some(serde_json::json!({ "content": "media:audio-frames;pcm" })),
            ..Default::default()
        });
        registry.insert_cached_media_def_for_test(StoredMediaDef {
            urn: "media:audio-frames;pcm".to_string(),
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
                "media:audio-frames;pcm".to_string(),
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
            Some("media:audio-frames;pcm".to_string())
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
        let config = RegistryConfig::default();
        let url = cap_registry_url(
            &config,
            "cap:in=\"media:enc=utf-8\";test;out=\"media:record\"",
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
        let config = RegistryConfig::default();
        let a = cap_registry_url(
            &config,
            "cap:in=\"media:listing-id\";use-grinder;out=\"media:id;task\"",
        );
        let b = cap_registry_url(
            &config,
            "cap:out=\"media:id;task\";in=\"media:listing-id\";use-grinder",
        );
        assert_eq!(a, b, "equivalent URNs must hash to the same registry key");
    }
}

/// Clearing a cache that other processes are clearing at the same time.
#[cfg(test)]
mod concurrent_cache_tests {
    use super::*;

    /// A registry whose cache is a directory of this test's own.
    fn registry_at(cache_dir: PathBuf) -> FabricRegistry {
        let mut registry = FabricRegistry::new_for_test();
        registry.cache_dir = cache_dir;
        registry
    }

    #[test]
    fn test11158_clearing_leaves_the_cache_present_and_empty() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let cache = dir.path().join("fabric.example.com");
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
        let cache = dir.path().join("never-existed");
        registry_at(cache.clone()).clear_cache().expect("clears");
        for sub in ["caps", "media", "aliases", "manifests"] {
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
        let cache = dir.path().join("shared.example.com");
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
        for sub in ["caps", "media", "aliases", "manifests"] {
            assert!(cache.join(sub).is_dir(), "{sub} is present at the end");
        }
    }
}
