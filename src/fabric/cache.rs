//! Where a fabric registry's definitions are kept on disk, and how an entry is
//! written and read there.
//!
//! The cache is shared: every capdag process on a machine that reads the same
//! fabric reads and writes the same folder, and they run at once (a test run
//! starts many builds within milliseconds of each other). Two rules follow.
//!
//! - **One folder per fabric manifest version.** A registry pinned at manifest
//!   `N` keeps everything under `<registry folder>/v<N>/`:
//!
//!   ```text
//!   <os cache>/capdag/<charter slug>/        the registry's folder; its charter
//!     v<N>/manifest.json                     manifest N, as the registry served it
//!     v<N>/file_types.json                   which of manifest N's media are file types, worked
//!                                            out once from manifest.json (and its hash)
//!     v<N>/caps/<sha256 of URN>/<defver>.json
//!     v<N>/media/<sha256 of URN>/<defver>.json
//!     v<N>/aliases/<sha256 of name>/<defver>.json
//!     v<N>/dims/<sha256 of name>/<defver>.json
//!   ```
//!
//!   capdag releases built against different manifests can be installed side
//!   by side; each reads and writes only its own `v<N>`, so none ever reads a
//!   definition another wrote under other rules.
//!
//! - **An entry is replaced whole.** It is written to a temporary file in the
//!   folder it belongs in and renamed over the entry, so a reader sees the old
//!   entry or the new one, never part of one ([`crate::cache_file`]). A
//!   temporary file is named `.<entry>.<pid>-<n>.tmp`; one a killed writer
//!   left behind is never read, and `capdag cache clear` removes it.
//!
//! An entry that is there and cannot be read, or does not parse, or is not
//! the definition its path names, is corrupt: it is an error naming the file,
//! never treated as missing.
//!
//! Before this layout, every manifest version shared the registry's folder
//! (`<charter slug>/{caps,media,aliases,dims,manifests}`). This library
//! neither reads nor writes those folders: capdag releases from before it
//! still do. `capdag cache clear` removes them.

use super::registry::FabricRegistryError;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};

/// The folders a manifest version's definitions live in, one per kind.
pub(crate) const KIND_FOLDERS: [&str; 4] = ["caps", "media", "aliases", "dims"];

/// The file a manifest version's manifest is kept in, in its folder.
pub(crate) const MANIFEST_FILE: &str = "manifest.json";

/// The file the file types a manifest version's manifest names are kept in,
/// in its folder: which of its media definitions carry the `ext=` tag, with
/// the hash of the manifest file they were worked out from.
pub(crate) const FILE_TYPES_FILE: &str = "file_types.json";

/// The folders the shared layout before this one kept in a registry's folder.
/// Nothing reads them; clearing the cache removes them.
pub(crate) const LEGACY_FOLDERS: [&str; 5] = ["caps", "media", "aliases", "dims", "manifests"];

/// The folder manifest `manifest_version` of a registry keeps its cache in.
pub(crate) fn version_dir(registry_dir: &Path, manifest_version: u32) -> PathBuf {
    registry_dir.join(format!("v{manifest_version}"))
}

/// The file the definition `key` (a normalized URN, or an alias or dim name)
/// of `kind` at `defver` is kept in, under a manifest version's folder.
pub(crate) fn entry_path(version_dir: &Path, kind: &str, key: &str, defver: u32) -> PathBuf {
    version_dir
        .join(kind)
        .join(key_hash(key))
        .join(format!("{defver}.json"))
}

/// The hash a definition is filed under, in the cache as in the registry.
pub(crate) fn key_hash(key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Put `bytes` at `path` whole ([`crate::cache_file::write_atomically`]): a
/// reader opening `path` meanwhile reads what was there before, or `bytes`,
/// never part of them.
pub(crate) fn write_atomically(path: &Path, bytes: &[u8]) -> Result<(), FabricRegistryError> {
    crate::cache_file::write_atomically(path, bytes)
        .map_err(|e| FabricRegistryError::CacheError(e.to_string()))
}

/// Serialize `entry` and put it at `path` whole ([`write_atomically`]).
pub(crate) fn store_entry<T: Serialize>(
    path: &Path,
    entry: &T,
    what: &str,
) -> Result<(), FabricRegistryError> {
    let bytes = serde_json::to_vec_pretty(entry).map_err(|e| {
        FabricRegistryError::CacheError(format!("failed to serialize the {what} cache entry: {e}"))
    })?;
    write_atomically(path, &bytes)
}

/// The entry at `path`: `None` when there is none, the entry when it parses,
/// and an error naming the file when it is there and cannot be read or does
/// not parse. `what` names the kind of entry in the error.
pub(crate) fn read_entry<T: DeserializeOwned>(
    path: &Path,
    what: &str,
) -> Result<Option<T>, FabricRegistryError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(FabricRegistryError::CacheError(format!(
                "the cached {what} {} cannot be read: {e}",
                path.display()
            )))
        }
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|e| corrupt(path, what, &e.to_string()))
}

/// The error for a cache entry that is there and is not what its path says.
pub(crate) fn corrupt(path: &Path, what: &str, detail: &str) -> FabricRegistryError {
    FabricRegistryError::CacheError(format!(
        "the cached {what} {} is corrupt: {detail}; remove the file, or run `capdag cache clear`",
        path.display()
    ))
}
