//! Profile Schema Registry
//!
//! Registry for JSON Schema profiles. Downloads and caches schemas from profile URLs
//! for validating data against media def type definitions.
//! Uses a two-level cache: disk-based cached schemas and in-memory compiled schemas.
//!
//! # The cache on disk
//!
//! The disk cache is shared: every capdag process on a machine reads and
//! writes it, and they run at once. It follows the fabric definition cache's
//! rules ([`crate::fabric::cache`]):
//!
//! ```text
//! <os cache>/capdag/profile_schemas/              the profile-schema cache
//!   capdag-rs-<version>/<sha256 of URL>.json      one schema, as this library at <version> keeps it
//! ```
//!
//! - **One folder per library and version.** The schemas are capdag's own,
//!   versioned with it, and each capdag library compiles them with its own
//!   JSON Schema validator. So this library at one version reads and writes
//!   only its own folder, and never an entry another library, or another
//!   version of it, wrote under other rules.
//! - **An entry is replaced whole** ([`crate::cache_file`]): written to a
//!   temporary file in its folder and renamed over the entry, so a reader sees
//!   the old entry or the new one, never part of one.
//! - **A corrupt entry is an error.** An entry that cannot be read, does not
//!   parse, holds another profile URL than its name is the hash of, or holds a
//!   schema that does not compile stops the registry from opening with an
//!   error that names the file. An entry past its time to live is removed and
//!   fetched again when it is next needed.
//!
//! Before this layout, every library and version kept its entries directly in
//! `profile_schemas/`, named by the first 16 hex digits of the URL's hash. This
//! library never reads those files; [`ProfileSchemaRegistry::clear_cache`]
//! removes them.

use jsonschema::JSONSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CACHE_DURATION_HOURS: u64 = 24 * 7; // Cache for 1 week

/// The folder this library keeps its entries in, in the profile-schema cache
/// folder: the library and its version.
pub const PROFILE_CACHE_LIBRARY_FOLDER: &str = concat!("capdag-rs-", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheEntry {
    schema_json: JsonValue,
    profile_url: String,
    cached_at: u64,
    ttl_hours: u64,
}

impl CacheEntry {
    fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now > self.cached_at + (self.ttl_hours * 3600)
    }
}

/// Compiled schema with its source JSON
struct CompiledSchema {
    compiled: JSONSchema,
    #[allow(dead_code)]
    source: JsonValue,
}

impl std::fmt::Debug for CompiledSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledSchema")
            .field("source", &self.source)
            .finish()
    }
}

#[derive(Debug)]
pub struct ProfileSchemaRegistry {
    client: reqwest::Client,
    /// The profile-schema cache folder, `<os cache>/capdag/profile_schemas`.
    cache_root: PathBuf,
    /// This library's folder in it, [`PROFILE_CACHE_LIBRARY_FOLDER`].
    cache_dir: PathBuf,
    /// In-memory cache of compiled schemas
    compiled_schemas: Arc<Mutex<HashMap<String, Arc<CompiledSchema>>>>,
    offline_flag: Arc<AtomicBool>,
}

impl ProfileSchemaRegistry {
    /// Create a registry over the profile-schema cache folder in the operating
    /// system's cache folder, loading the entries this library keeps there.
    pub async fn new() -> Result<Self, ProfileSchemaError> {
        let cache_root = Self::get_cache_root()?;
        Self::new_with_cache_dir(cache_root).await
    }

    /// Create a registry over the profile-schema cache folder `cache_root`:
    /// its entries are kept in `cache_root/`[`PROFILE_CACHE_LIBRARY_FOLDER`],
    /// and loaded from there now. A corrupt entry is an error naming its file.
    pub async fn new_with_cache_dir(cache_root: PathBuf) -> Result<Self, ProfileSchemaError> {
        let cache_dir = cache_root.join(PROFILE_CACHE_LIBRARY_FOLDER);
        fs::create_dir_all(&cache_dir).map_err(|e| {
            ProfileSchemaError::CacheError(format!(
                "failed to create the profile-schema cache folder {}: {e}",
                cache_dir.display()
            ))
        })?;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| {
                ProfileSchemaError::HttpError(format!("Failed to create HTTP client: {}", e))
            })?;

        // Load all cached schemas into memory
        let compiled_schemas_map = Self::load_all_cached_schemas(&cache_dir)?;
        let compiled_schemas = Arc::new(Mutex::new(compiled_schemas_map));

        Ok(Self {
            client,
            cache_root,
            cache_dir,
            compiled_schemas,
            offline_flag: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Set the offline flag. When true, all schema fetches are blocked.
    pub fn set_offline(&self, offline: bool) {
        self.offline_flag.store(offline, Ordering::Relaxed);
    }

    fn get_cache_root() -> Result<PathBuf, ProfileSchemaError> {
        let cache_dir = dirs::cache_dir().ok_or_else(|| {
            ProfileSchemaError::CacheError("Could not determine cache directory".to_string())
        })?;
        Ok(cache_dir.join("capdag").join("profile_schemas"))
    }

    /// The name an entry for `profile_url` is kept under: the SHA-256 of the
    /// URL, in hex, with `.json`.
    fn entry_file_name(profile_url: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(profile_url.as_bytes());
        format!("{:x}.json", hasher.finalize())
    }

    fn cache_file_path(&self, profile_url: &str) -> PathBuf {
        self.cache_dir.join(Self::entry_file_name(profile_url))
    }

    /// The error for an entry that is there and is not what its name says.
    fn corrupt(path: &Path, detail: &str) -> ProfileSchemaError {
        ProfileSchemaError::CacheError(format!(
            "the cached profile schema {} is corrupt: {detail}; remove the file",
            path.display()
        ))
    }

    /// Every entry in this library's folder, compiled, by profile URL. A
    /// temporary file is not an entry and is passed over; an expired entry is
    /// removed; anything else that is not a whole, valid entry is an error
    /// naming its file.
    fn load_all_cached_schemas(
        cache_dir: &Path,
    ) -> Result<HashMap<String, Arc<CompiledSchema>>, ProfileSchemaError> {
        let mut schemas = HashMap::new();

        let listing = fs::read_dir(cache_dir).map_err(|e| {
            ProfileSchemaError::CacheError(format!(
                "failed to read the profile-schema cache folder {}: {e}",
                cache_dir.display()
            ))
        })?;
        for entry in listing {
            let entry = entry.map_err(|e| {
                ProfileSchemaError::CacheError(format!(
                    "failed to read the profile-schema cache folder {}: {e}",
                    cache_dir.display()
                ))
            })?;
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_str().ok_or_else(|| {
                Self::corrupt(&path, "its name is not an entry's name")
            })?;
            if crate::cache_file::is_temporary(name) {
                continue;
            }
            if !name.ends_with(".json") || path.is_dir() {
                return Err(Self::corrupt(
                    &path,
                    "it is not an entry: the folder holds only `<sha256 of URL>.json` files",
                ));
            }

            let content = match fs::read(&path) {
                Ok(content) => content,
                // A peer removed it (expired) between the listing and the read.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => {
                    return Err(ProfileSchemaError::CacheError(format!(
                        "the cached profile schema {} cannot be read: {e}",
                        path.display()
                    )))
                }
            };
            let cache_entry: CacheEntry = serde_json::from_slice(&content)
                .map_err(|e| Self::corrupt(&path, &e.to_string()))?;
            if Self::entry_file_name(&cache_entry.profile_url) != name {
                return Err(Self::corrupt(
                    &path,
                    &format!(
                        "it holds the schema of {}, whose entry is {}",
                        cache_entry.profile_url,
                        Self::entry_file_name(&cache_entry.profile_url)
                    ),
                ));
            }

            if cache_entry.is_expired() {
                match fs::remove_file(&path) {
                    Ok(()) => {}
                    // A peer removed or replaced it first.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        return Err(ProfileSchemaError::CacheError(format!(
                            "failed to remove the expired cached profile schema {}: {e}",
                            path.display()
                        )))
                    }
                }
                continue;
            }

            let compiled = JSONSchema::compile(&cache_entry.schema_json).map_err(|e| {
                Self::corrupt(&path, &format!("its schema does not compile: {e}"))
            })?;
            schemas.insert(
                cache_entry.profile_url.clone(),
                Arc::new(CompiledSchema {
                    compiled,
                    source: cache_entry.schema_json,
                }),
            );
        }

        Ok(schemas)
    }

    /// Put the entry for `profile_url` in this library's folder, whole.
    fn save_to_cache(
        &self,
        profile_url: &str,
        schema_json: &JsonValue,
    ) -> Result<(), ProfileSchemaError> {
        let cache_file = self.cache_file_path(profile_url);
        let cache_entry = CacheEntry {
            schema_json: schema_json.clone(),
            profile_url: profile_url.to_string(),
            cached_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            ttl_hours: CACHE_DURATION_HOURS,
        };

        let content = serde_json::to_vec_pretty(&cache_entry).map_err(|e| {
            ProfileSchemaError::CacheError(format!(
                "failed to serialize the cached profile schema of {profile_url}: {e}"
            ))
        })?;
        crate::cache_file::write_atomically(&cache_file, &content)
            .map_err(|e| ProfileSchemaError::CacheError(e.to_string()))
    }

    /// Get a compiled schema for a profile URL.
    /// Returns None if the profile can't be fetched or isn't a valid schema.
    async fn get_schema(&self, profile_url: &str) -> Option<Arc<CompiledSchema>> {
        // Check in-memory cache first
        {
            let schemas = self.compiled_schemas.lock().ok()?;
            if let Some(schema) = schemas.get(profile_url) {
                return Some(Arc::clone(schema));
            }
        }

        // Not in memory cache - try to fetch from URL
        match self.fetch_schema(profile_url).await {
            Ok((schema_json, compiled)) => {
                let compiled_schema = Arc::new(CompiledSchema {
                    compiled,
                    source: schema_json.clone(),
                });

                // Save to disk cache. The schema is at hand either way; a cache
                // that cannot be written is reported, not hidden.
                if let Err(e) = self.save_to_cache(profile_url, &schema_json) {
                    tracing::warn!("profile schema {profile_url} is not cached: {e}");
                }

                // Add to memory cache
                if let Ok(mut schemas) = self.compiled_schemas.lock() {
                    schemas.insert(profile_url.to_string(), Arc::clone(&compiled_schema));
                }

                Some(compiled_schema)
            }
            Err(_) => None, // Fetch failed - skip validation for this profile
        }
    }

    async fn fetch_schema(
        &self,
        profile_url: &str,
    ) -> Result<(JsonValue, JSONSchema), ProfileSchemaError> {
        if self.offline_flag.load(Ordering::Relaxed) {
            return Err(ProfileSchemaError::NetworkBlocked(format!(
                "Network access blocked by policy — cannot fetch schema '{}'",
                profile_url
            )));
        }
        let response = self.client.get(profile_url).send().await.map_err(|e| {
            ProfileSchemaError::HttpError(format!(
                "Failed to fetch schema from {}: {}",
                profile_url, e
            ))
        })?;

        if !response.status().is_success() {
            return Err(ProfileSchemaError::NotFound(format!(
                "Schema not found at {} (HTTP {})",
                profile_url,
                response.status()
            )));
        }

        let content = response.text().await.map_err(|e| {
            ProfileSchemaError::HttpError(format!(
                "Failed to read response from {}: {}",
                profile_url, e
            ))
        })?;

        let schema_json: JsonValue = serde_json::from_str(&content).map_err(|e| {
            ProfileSchemaError::ParseError(format!("Invalid JSON from {}: {}", profile_url, e))
        })?;

        let compiled = JSONSchema::compile(&schema_json).map_err(|e| {
            ProfileSchemaError::InvalidSchema(format!(
                "Invalid JSON Schema from {}: {}",
                profile_url, e
            ))
        })?;

        Ok((schema_json, compiled))
    }

    /// Validate a value against a profile's schema.
    /// Returns Ok(()) if valid or if schema not available (logs warning and skips validation).
    /// Returns Err with validation errors if invalid.
    pub async fn validate(&self, profile_url: &str, value: &JsonValue) -> Result<(), Vec<String>> {
        match self.get_schema(profile_url).await {
            Some(schema) => match schema.compiled.validate(value) {
                Ok(()) => Ok(()),
                Err(errors) => {
                    let error_messages: Vec<String> = errors.map(|e| e.to_string()).collect();
                    Err(error_messages)
                }
            },
            None => {
                tracing::warn!(
                    "Schema not available for profile '{}' - skipping validation",
                    profile_url
                );
                Ok(())
            }
        }
    }

    /// Validate synchronously using only cached schemas.
    /// Returns Ok(()) if valid or if schema not cached.
    /// Returns Err with validation errors if invalid.
    pub fn validate_cached(&self, profile_url: &str, value: &JsonValue) -> Result<(), Vec<String>> {
        let schemas = match self.compiled_schemas.lock() {
            Ok(s) => s,
            Err(_) => return Ok(()), // Lock failed - skip validation
        };

        match schemas.get(profile_url) {
            Some(schema) => match schema.compiled.validate(value) {
                Ok(()) => Ok(()),
                Err(errors) => {
                    let error_messages: Vec<String> = errors.map(|e| e.to_string()).collect();
                    Err(error_messages)
                }
            },
            None => Ok(()), // Schema not cached - skip validation
        }
    }

    /// Check if a profile URL exists in the in-memory cache.
    pub fn schema_exists(&self, profile_url: &str) -> bool {
        let schemas = match self.compiled_schemas.lock() {
            Ok(s) => s,
            Err(_) => return false,
        };
        schemas.contains_key(profile_url)
    }

    /// Get all cached profile URLs
    pub fn get_cached_profiles(&self) -> Vec<String> {
        let schemas = match self.compiled_schemas.lock() {
            Ok(s) => s,
            Err(_) => return vec![],
        };
        schemas.keys().cloned().collect()
    }

    /// Insert a schema directly into the in-memory and disk caches without
    /// fetching it over HTTP. Intended for tests and local seeding only — production
    /// callers should rely on the on-demand fetch path.
    pub fn insert_schema(
        &self,
        profile_url: &str,
        schema_json: JsonValue,
    ) -> Result<(), ProfileSchemaError> {
        let compiled = JSONSchema::compile(&schema_json).map_err(|e| {
            ProfileSchemaError::InvalidSchema(format!(
                "Failed to compile schema for {}: {}",
                profile_url, e
            ))
        })?;

        // Persist to disk cache so subsequent process starts pick it up.
        self.save_to_cache(profile_url, &schema_json)?;

        let mut schemas = self
            .compiled_schemas
            .lock()
            .map_err(|e| ProfileSchemaError::CacheError(format!("Failed to lock cache: {}", e)))?;
        schemas.insert(
            profile_url.to_string(),
            Arc::new(CompiledSchema {
                compiled,
                source: schema_json,
            }),
        );
        Ok(())
    }

    /// Clear all caches: the in-memory one, this library's folder on disk
    /// (emptied, not removed), and the files the older layout kept directly
    /// in the profile-schema cache folder. Other libraries' and versions'
    /// folders are left to them. Nothing is read first, so this clears a cache
    /// holding a corrupt entry; a file a peer removed first is not an error.
    pub fn clear_cache(&self) -> Result<(), ProfileSchemaError> {
        {
            let mut schemas = self.compiled_schemas.lock().map_err(|e| {
                ProfileSchemaError::CacheError(format!("Failed to lock cache: {}", e))
            })?;
            schemas.clear();
        }
        Self::remove_files_in(&self.cache_dir)?;
        Self::remove_files_in(&self.cache_root)
    }

    /// Remove every file directly in `folder`, leaving its folders.
    fn remove_files_in(folder: &Path) -> Result<(), ProfileSchemaError> {
        let listing = match fs::read_dir(folder) {
            Ok(listing) => listing,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                return Err(ProfileSchemaError::CacheError(format!(
                    "failed to read the profile-schema cache folder {}: {e}",
                    folder.display()
                )))
            }
        };
        for entry in listing {
            let entry = entry.map_err(|e| {
                ProfileSchemaError::CacheError(format!(
                    "failed to read the profile-schema cache folder {}: {e}",
                    folder.display()
                ))
            })?;
            let path = entry.path();
            if path.is_dir() {
                continue;
            }
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(ProfileSchemaError::CacheError(format!(
                        "failed to remove {}: {e}",
                        path.display()
                    )))
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProfileSchemaError {
    #[error("HTTP error: {0}")]
    HttpError(String),

    #[error("Schema not found: {0}")]
    NotFound(String),

    #[error("Failed to parse schema: {0}")]
    ParseError(String),

    #[error("Invalid JSON Schema: {0}")]
    InvalidSchema(String),

    #[error("Cache error: {0}")]
    CacheError(String),

    #[error("Network access blocked: {0}")]
    NetworkBlocked(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::standard::media::{
        PROFILE_BOOL, PROFILE_BOOL_ARRAY, PROFILE_INT, PROFILE_NUM, PROFILE_NUM_ARRAY, PROFILE_OBJ,
        PROFILE_OBJ_ARRAY, PROFILE_STR, PROFILE_STR_ARRAY,
    };
    use serde_json::json;
    use tempfile::TempDir;

    /// Construct a JSON Schema body for one of the well-known scalar/array
    /// profile types. Tests use this to seed the registry without HTTP because
    /// the registry no longer ships embedded schema bodies — production callers
    /// fetch on demand.
    fn schema_body(profile_url: &str) -> JsonValue {
        let (json_type, items) = match profile_url {
            PROFILE_STR => ("string", None),
            PROFILE_INT => ("integer", None),
            PROFILE_NUM => ("number", None),
            PROFILE_BOOL => ("boolean", None),
            PROFILE_OBJ => ("object", None),
            PROFILE_STR_ARRAY => ("array", Some("string")),
            PROFILE_NUM_ARRAY => ("array", Some("number")),
            PROFILE_BOOL_ARRAY => ("array", Some("boolean")),
            PROFILE_OBJ_ARRAY => ("array", Some("object")),
            other => panic!("schema_body: unknown profile URL '{}'", other),
        };
        let mut schema = json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": profile_url,
            "type": json_type,
        });
        if let Some(item_type) = items {
            schema["items"] = json!({"type": item_type});
        }
        schema
    }

    fn seed_standard_schemas(registry: &ProfileSchemaRegistry) {
        for url in [
            PROFILE_STR,
            PROFILE_INT,
            PROFILE_NUM,
            PROFILE_BOOL,
            PROFILE_OBJ,
            PROFILE_STR_ARRAY,
            PROFILE_NUM_ARRAY,
            PROFILE_BOOL_ARRAY,
            PROFILE_OBJ_ARRAY,
        ] {
            registry
                .insert_schema(url, schema_body(url))
                .unwrap_or_else(|e| panic!("seed {}: {}", url, e));
        }
    }

    /// Create a registry with an isolated temporary cache directory and the
    /// well-known scalar/array profile schemas seeded into the cache. Tests
    /// that exercise validation against those profiles use this to bypass the
    /// network fetch path.
    async fn create_test_registry() -> (ProfileSchemaRegistry, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let registry = ProfileSchemaRegistry::new_with_cache_dir(temp_dir.path().to_path_buf())
            .await
            .expect("Failed to create registry");
        seed_standard_schemas(&registry);
        (registry, temp_dir)
    }

    /// Create a fresh, unseeded registry — no schemas, no network.
    /// Used by tests that assert the post-construction cache state.
    async fn create_empty_test_registry() -> (ProfileSchemaRegistry, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let registry = ProfileSchemaRegistry::new_with_cache_dir(temp_dir.path().to_path_buf())
            .await
            .expect("Failed to create registry");
        (registry, temp_dir)
    }

    // TEST618: Verify profile schema registry creation succeeds with temp cache
    #[tokio::test]
    async fn test618_registry_creation() {
        let (registry, _temp_dir) = create_empty_test_registry().await;
        assert!(registry.cache_dir.exists());
    }

    // TEST619: A freshly constructed registry has an empty cache. The well-known
    // profile schemas are no longer bundled in the binary; callers must either
    // fetch them on demand or seed via insert_schema.
    #[tokio::test]
    async fn test619_fresh_registry_cache_is_empty() {
        let (registry, _temp_dir) = create_empty_test_registry().await;

        assert!(
            registry.get_cached_profiles().is_empty(),
            "Fresh registry must have no cached schemas; nothing is bundled into the binary"
        );
        assert!(!registry.schema_exists(PROFILE_STR));
        assert!(!registry.schema_exists(PROFILE_OBJ_ARRAY));
    }

    // TEST620: Verify string schema validates strings and rejects non-strings
    #[tokio::test]
    async fn test620_string_validation() {
        let (registry, _temp_dir) = create_test_registry().await;

        // Valid string
        assert!(registry
            .validate(PROFILE_STR, &json!("hello"))
            .await
            .is_ok());

        // Invalid: not a string
        assert!(registry.validate(PROFILE_STR, &json!(42)).await.is_err());
    }

    // TEST621: Verify integer schema validates integers and rejects floats and strings
    #[tokio::test]
    async fn test621_integer_validation() {
        let (registry, _temp_dir) = create_test_registry().await;

        // Valid integer
        assert!(registry.validate(PROFILE_INT, &json!(42)).await.is_ok());

        // Invalid: not an integer (float)
        assert!(registry.validate(PROFILE_INT, &json!(3.14)).await.is_err());

        // Invalid: not a number
        assert!(registry
            .validate(PROFILE_INT, &json!("hello"))
            .await
            .is_err());
    }

    // TEST622: Verify number schema validates integers and floats, rejects strings
    #[tokio::test]
    async fn test622_number_validation() {
        let (registry, _temp_dir) = create_test_registry().await;

        // Valid number (integer)
        assert!(registry.validate(PROFILE_NUM, &json!(42)).await.is_ok());

        // Valid number (float)
        assert!(registry.validate(PROFILE_NUM, &json!(3.14)).await.is_ok());

        // Invalid: not a number
        assert!(registry
            .validate(PROFILE_NUM, &json!("hello"))
            .await
            .is_err());
    }

    // TEST623: Verify boolean schema validates true/false and rejects string "true"
    #[tokio::test]
    async fn test623_boolean_validation() {
        let (registry, _temp_dir) = create_test_registry().await;

        // Valid boolean
        assert!(registry.validate(PROFILE_BOOL, &json!(true)).await.is_ok());
        assert!(registry.validate(PROFILE_BOOL, &json!(false)).await.is_ok());

        // Invalid: not a boolean
        assert!(registry
            .validate(PROFILE_BOOL, &json!("true"))
            .await
            .is_err());
    }

    // TEST624: Verify object schema validates objects and rejects arrays
    #[tokio::test]
    async fn test624_object_validation() {
        let (registry, _temp_dir) = create_test_registry().await;

        // Valid object
        assert!(registry
            .validate(PROFILE_OBJ, &json!({"key": "value"}))
            .await
            .is_ok());

        // Invalid: not an object
        assert!(registry
            .validate(PROFILE_OBJ, &json!([1, 2, 3]))
            .await
            .is_err());
    }

    // TEST625: Verify string array schema validates string arrays and rejects mixed arrays
    #[tokio::test]
    async fn test625_string_array_validation() {
        let (registry, _temp_dir) = create_test_registry().await;

        // Valid string array
        assert!(registry
            .validate(PROFILE_STR_ARRAY, &json!(["a", "b", "c"]))
            .await
            .is_ok());

        // Invalid: contains non-strings
        assert!(registry
            .validate(PROFILE_STR_ARRAY, &json!(["a", 1, "c"]))
            .await
            .is_err());

        // Invalid: not an array
        assert!(registry
            .validate(PROFILE_STR_ARRAY, &json!("hello"))
            .await
            .is_err());
    }

    // TEST626: Verify unknown profile URL skips validation and returns Ok
    #[tokio::test]
    async fn test626_unknown_profile_skips_validation() {
        let (registry, _temp_dir) = create_test_registry().await;

        // Unknown profile should return Ok - skip validation
        let result = registry
            .validate("https://example.com/unknown-profile", &json!("anything"))
            .await;
        assert!(result.is_ok());
    }

    // TEST6605: insert_schema is the production seam for non-HTTP schema injection.
    // It must persist to the in-memory cache so subsequent schema_exists/validate
    // calls succeed without network access.
    #[tokio::test]
    async fn test6605_insert_schema_populates_cache() {
        let (registry, _temp_dir) = create_empty_test_registry().await;
        assert!(!registry.schema_exists(PROFILE_STR));

        registry
            .insert_schema(PROFILE_STR, schema_body(PROFILE_STR))
            .expect("insert_schema must succeed for a valid JSON Schema");

        assert!(
            registry.schema_exists(PROFILE_STR),
            "After insert_schema the URL must be cached"
        );
        // The seeded schema must actually validate values, not silently pass.
        assert!(registry.validate_cached(PROFILE_STR, &json!("ok")).is_ok());
        assert!(
            registry.validate_cached(PROFILE_STR, &json!(7)).is_err(),
            "Number must not validate against the string schema"
        );
    }

    // TEST627: insert_schema rejects malformed JSON Schemas instead of caching them.
    // A registry that silently accepted invalid schemas would hide compilation
    // problems until the first validation call.
    #[tokio::test]
    async fn test627_insert_schema_rejects_invalid_schema() {
        let (registry, _temp_dir) = create_empty_test_registry().await;
        // `type` of 99 is not a valid JSON Schema type — compile must fail.
        let bad = json!({"$schema": "https://json-schema.org/draft/2020-12/schema", "type": 99});
        let result = registry.insert_schema("https://capdag.com/schema/bad", bad);
        assert!(result.is_err(), "Invalid schema must not be cached");
        assert!(
            !registry.schema_exists("https://capdag.com/schema/bad"),
            "Failed insert must not leave the URL in the cache"
        );
    }

    // TEST612: clear_cache empties the in-memory cache for seeded schemas.
    #[tokio::test]
    async fn test612_clear_cache() {
        let (registry, _temp_dir) = create_test_registry().await;

        // Seeded schemas should be loaded
        assert!(registry.schema_exists(PROFILE_STR));
        assert!(!registry.get_cached_profiles().is_empty());

        // Clear
        registry.clear_cache().expect("clear_cache should succeed");

        // All schemas should be gone
        assert!(!registry.schema_exists(PROFILE_STR));
        assert!(registry.get_cached_profiles().is_empty());
    }

    // TEST613: validate_cached validates against cached standard schemas
    #[tokio::test]
    async fn test613_validate_cached() {
        let (registry, _temp_dir) = create_test_registry().await;

        // Valid string against string schema
        assert!(registry
            .validate_cached(PROFILE_STR, &json!("hello"))
            .is_ok());

        // Invalid: number against string schema
        let result = registry.validate_cached(PROFILE_STR, &json!(42));
        assert!(result.is_err(), "Number should not validate as string");

        // Valid integer
        assert!(registry.validate_cached(PROFILE_INT, &json!(42)).is_ok());

        // Valid object array
        assert!(registry
            .validate_cached(PROFILE_OBJ_ARRAY, &json!([{"a": 1}]))
            .is_ok());

        // Invalid: string array against object array schema
        let result = registry.validate_cached(PROFILE_OBJ_ARRAY, &json!(["a", "b"]));
        assert!(result.is_err());

        // Non-cached profile returns Ok (skip validation)
        assert!(registry
            .validate_cached("https://example.com/unknown", &json!("anything"))
            .is_ok());
    }

    /// Every cache entry under `root`, at whatever depth the layout keeps
    /// them: the `.json` files, leaving out temporary files (named from a
    /// `.`). The tests find entries this way rather than by the registry's own
    /// path helpers, so that they say what is on disk.
    fn cached_entry_files(root: &std::path::Path) -> Vec<PathBuf> {
        let mut files = Vec::new();
        let mut folders = vec![root.to_path_buf()];
        while let Some(folder) = folders.pop() {
            for entry in fs::read_dir(&folder).expect("the cache folder is readable") {
                let path = entry.expect("a cache folder entry").path();
                let name = path.file_name().unwrap().to_string_lossy().to_string();
                if path.is_dir() {
                    folders.push(path);
                } else if name.ends_with(".json") && !name.starts_with('.') {
                    files.push(path);
                }
            }
        }
        files.sort();
        files
    }

    /// A string schema whose description is `size` copies of `marker`: large
    /// enough that writing it in place takes long enough for a reader to open
    /// it half-written.
    fn large_string_schema(marker: char, size: usize) -> JsonValue {
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "string",
            "description": std::iter::repeat(marker).take(size).collect::<String>(),
        })
    }

    // TEST604: Processes loading the profile-schema cache while others replace
    // the same entry never read part of an entry: every load succeeds and
    // holds the schema whole, the old one or the new one.
    #[test]
    fn test604_concurrent_readers_never_read_a_torn_entry() {
        const URL: &str = "https://capdag.com/schema/test604-large";
        const SIZE: usize = 512 * 1024;
        const WRITES: usize = 60;
        let temp_dir = TempDir::new().expect("a temporary folder");
        let root = temp_dir.path().to_path_buf();
        let runtime = || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a runtime")
        };
        runtime()
            .block_on(ProfileSchemaRegistry::new_with_cache_dir(root.clone()))
            .expect("the registry opens")
            .insert_schema(URL, large_string_schema('a', SIZE))
            .expect("the seed is written");

        let writing = Arc::new(AtomicBool::new(true));
        let writers: Vec<_> = ['b', 'c']
            .into_iter()
            .map(|marker| {
                let root = root.clone();
                std::thread::spawn(move || {
                    let registry = runtime()
                        .block_on(ProfileSchemaRegistry::new_with_cache_dir(root))
                        .expect("the writer's registry opens");
                    for _ in 0..WRITES {
                        registry
                            .insert_schema(URL, large_string_schema(marker, SIZE))
                            .expect("the entry is written");
                    }
                })
            })
            .collect();
        let readers: Vec<_> = (0..3)
            .map(|_| {
                let root = root.clone();
                let writing = Arc::clone(&writing);
                std::thread::spawn(move || {
                    let runtime = runtime();
                    let mut loads = 0;
                    while writing.load(Ordering::Relaxed) || loads == 0 {
                        let registry = runtime
                            .block_on(ProfileSchemaRegistry::new_with_cache_dir(root.clone()))
                            .unwrap_or_else(|e| panic!("load {loads} failed: {e}"));
                        assert!(
                            registry.schema_exists(URL),
                            "load {loads} lost the entry: it read part of it"
                        );
                        assert!(registry.validate_cached(URL, &json!("text")).is_ok());
                        assert!(registry.validate_cached(URL, &json!(7)).is_err());
                        loads += 1;
                    }
                    loads
                })
            })
            .collect();
        for writer in writers {
            writer.join().expect("a writer panicked");
        }
        writing.store(false, Ordering::Relaxed);
        for reader in readers {
            let loads = reader.join().expect("a reader read a torn entry");
            assert!(loads > 0);
        }
        let entries = cached_entry_files(&root);
        assert_eq!(entries.len(), 1, "one entry, and no temporary file left: {entries:?}");
    }

    // TEST611: A cache entry that is there and does not parse is an error that
    // names its file; the registry does not open over it as if it were absent.
    #[tokio::test]
    async fn test611_corrupt_entry_is_an_error_naming_its_file() {
        let (registry, temp_dir) = create_empty_test_registry().await;
        registry
            .insert_schema(PROFILE_STR, schema_body(PROFILE_STR))
            .expect("the entry is written");
        drop(registry);
        let entries = cached_entry_files(temp_dir.path());
        assert_eq!(entries.len(), 1, "{entries:?}");
        fs::write(&entries[0], "{\"schema_json\": {\"type\": \"str").expect("the entry is cut short");

        let err = ProfileSchemaRegistry::new_with_cache_dir(temp_dir.path().to_path_buf())
            .await
            .expect_err("a corrupt entry must stop the registry from opening");
        let message = err.to_string();
        assert!(
            message.contains(&entries[0].display().to_string()),
            "the error names the file: {message}"
        );
        assert!(message.contains("corrupt"), "{message}");
    }

    // TEST615: A cache entry that parses but is not the schema its file names
    // (it holds another profile URL, or a body that does not compile) is an
    // error naming its file.
    #[tokio::test]
    async fn test615_entry_not_matching_its_file_is_an_error() {
        // Another profile URL than its file name says.
        let (registry, temp_dir) = create_empty_test_registry().await;
        registry
            .insert_schema(PROFILE_STR, schema_body(PROFILE_STR))
            .expect("str is written");
        registry
            .insert_schema(PROFILE_INT, schema_body(PROFILE_INT))
            .expect("int is written");
        drop(registry);
        let entries = cached_entry_files(temp_dir.path());
        assert_eq!(entries.len(), 2, "{entries:?}");
        let (int_entry, str_entry): (Vec<_>, Vec<_>) = entries.iter().partition(|path| {
            fs::read_to_string(path).unwrap().contains(PROFILE_INT)
        });
        fs::copy(int_entry[0], str_entry[0]).expect("str's file now holds int");
        let err = ProfileSchemaRegistry::new_with_cache_dir(temp_dir.path().to_path_buf())
            .await
            .expect_err("an entry holding another URL must stop the registry from opening");
        assert!(
            err.to_string().contains(&str_entry[0].display().to_string()),
            "the error names the file: {err}"
        );

        // A body that does not compile.
        let (registry, temp_dir) = create_empty_test_registry().await;
        registry
            .insert_schema(PROFILE_STR, schema_body(PROFILE_STR))
            .expect("str is written");
        drop(registry);
        let entries = cached_entry_files(temp_dir.path());
        let mut entry: JsonValue =
            serde_json::from_str(&fs::read_to_string(&entries[0]).unwrap()).unwrap();
        entry["schema_json"]["type"] = json!(99);
        fs::write(&entries[0], serde_json::to_string(&entry).unwrap()).unwrap();
        let err = ProfileSchemaRegistry::new_with_cache_dir(temp_dir.path().to_path_buf())
            .await
            .expect_err("a schema that does not compile must stop the registry from opening");
        assert!(
            err.to_string().contains(&entries[0].display().to_string()),
            "the error names the file: {err}"
        );
    }

    // TEST654: A library keeps its entries in its own folder,
    // `<cache>/<library>-<version>/`, and reads no other: neither another
    // library's or version's folder nor the older layout's files directly in
    // the cache folder. Clearing the cache empties its own folder and removes
    // the older layout's files, and leaves other folders alone.
    #[tokio::test]
    async fn test654_entries_live_in_the_library_version_folder() {
        let temp_dir = TempDir::new().expect("a temporary folder");
        let root = temp_dir.path();
        let own = root.join(format!("capdag-rs-{}", env!("CARGO_PKG_VERSION")));
        let other = root.join("capdag-go-0.0.1");
        fs::create_dir_all(&other).unwrap();
        fs::write(other.join("not-read.json"), "not json").unwrap();
        let legacy = root.join("0123456789abcdef.json");
        fs::write(&legacy, "not json either").unwrap();
        let legacy_temporary = root.join(".0123456789abcdef.json.1-0.tmp");
        fs::write(&legacy_temporary, "").unwrap();

        let registry = ProfileSchemaRegistry::new_with_cache_dir(root.to_path_buf())
            .await
            .expect("other folders and the older layout are not read");
        registry
            .insert_schema(PROFILE_STR, schema_body(PROFILE_STR))
            .expect("the entry is written");
        let mut hasher = Sha256::new();
        hasher.update(PROFILE_STR.as_bytes());
        let entry = own.join(format!("{:x}.json", hasher.finalize()));
        assert!(entry.is_file(), "the entry is {}", entry.display());

        registry.clear_cache().expect("the cache clears");
        assert!(!entry.exists());
        assert!(own.is_dir(), "the library's folder stays, emptied");
        assert!(!legacy.exists(), "the older layout's files are removed");
        assert!(!legacy_temporary.exists());
        assert!(other.join("not-read.json").exists(), "another library's folder is left");
    }
}
