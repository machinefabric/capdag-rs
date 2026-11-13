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
//!   entry or the new one, never part of one. A temporary file is named
//!   `.<entry>.<pid>-<n>.tmp`; one a killed writer left behind is never read,
//!   and `capdag cache clear` removes it.
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
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// The folders a manifest version's definitions live in, one per kind.
pub(crate) const KIND_FOLDERS: [&str; 4] = ["caps", "media", "aliases", "dims"];

/// The file a manifest version's manifest is kept in, in its folder.
pub(crate) const MANIFEST_FILE: &str = "manifest.json";

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

/// Tells apart the temporary files one process writes at once.
static TEMPORARY_FILES: AtomicU64 = AtomicU64::new(0);

/// Put `bytes` at `path` whole: written to a temporary file beside it, then
/// renamed over it. A reader opening `path` meanwhile reads what was there
/// before, or `bytes`, never part of them. The folder is made when missing.
pub(crate) fn write_atomically(path: &Path, bytes: &[u8]) -> Result<(), FabricRegistryError> {
    let folder = path.parent().ok_or_else(|| {
        FabricRegistryError::CacheError(format!(
            "the cache file {} has no folder",
            path.display()
        ))
    })?;
    fs::create_dir_all(folder).map_err(|e| {
        FabricRegistryError::CacheError(format!(
            "failed to create the cache folder {}: {e}",
            folder.display()
        ))
    })?;
    let name = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
        FabricRegistryError::CacheError(format!(
            "the cache file {} has no name",
            path.display()
        ))
    })?;
    let temporary = folder.join(format!(
        ".{name}.{}-{}.tmp",
        std::process::id(),
        TEMPORARY_FILES.fetch_add(1, Ordering::Relaxed)
    ));
    let written = fs::File::create(&temporary).and_then(|mut file| file.write_all(bytes));
    if let Err(e) = written {
        let _ = fs::remove_file(&temporary);
        return Err(FabricRegistryError::CacheError(format!(
            "failed to write the cache file {}: {e}",
            temporary.display()
        )));
    }
    if let Err(e) = rename_over(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        return Err(FabricRegistryError::CacheError(format!(
            "failed to put the cache file {} in place: {e}",
            path.display()
        )));
    }
    Ok(())
}

/// Rename `from` over `to`, replacing it.
///
/// Unix replaces the name in one step whatever else has it open. Windows
/// refuses the replacement with "access denied" while another process is
/// replacing the same entry at that moment; that is a peer writing the same
/// definition, so the rename is asked again for a short bound, after which the
/// refusal is reported as it stands.
fn rename_over(from: &Path, to: &Path) -> std::io::Result<()> {
    let mut attempts = 0;
    loop {
        match fs::rename(from, to) {
            Err(e)
                if cfg!(windows)
                    && e.kind() == std::io::ErrorKind::PermissionDenied
                    && attempts < 50 =>
            {
                attempts += 1;
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            other => return other,
        }
    }
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
