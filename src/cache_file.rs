//! Writing a file of a cache that several processes share.
//!
//! capdag keeps two caches on disk that every capdag process on a machine
//! reads and writes at once: the fabric definition cache
//! ([`crate::fabric::cache`]) and the profile-schema cache
//! ([`crate::media::profile`]). Both replace an entry whole, the same way: it
//! is written to a temporary file in the folder it belongs in and renamed over
//! the entry, so a reader sees the old entry or the new one, never part of
//! one. A temporary file is named `.<entry>.<pid>-<n>.tmp`; one a killed
//! writer left behind is never read as an entry.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// Why a cache file could not be put in place; the message names the file.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct CacheWriteError(String);

/// Tells apart the temporary files one process writes at once.
static TEMPORARY_FILES: AtomicU64 = AtomicU64::new(0);

/// Whether `file_name` is a temporary file [`write_atomically`] writes, or
/// left behind when its writer was killed: never an entry.
pub(crate) fn is_temporary(file_name: &str) -> bool {
    file_name.starts_with('.') && file_name.ends_with(".tmp")
}

/// Put `bytes` at `path` whole: written to a temporary file beside it, then
/// renamed over it. A reader opening `path` meanwhile reads what was there
/// before, or `bytes`, never part of them. The folder is made when missing.
pub(crate) fn write_atomically(path: &Path, bytes: &[u8]) -> Result<(), CacheWriteError> {
    let folder = path.parent().ok_or_else(|| {
        CacheWriteError(format!("the cache file {} has no folder", path.display()))
    })?;
    fs::create_dir_all(folder).map_err(|e| {
        CacheWriteError(format!(
            "failed to create the cache folder {}: {e}",
            folder.display()
        ))
    })?;
    let name = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
        CacheWriteError(format!("the cache file {} has no name", path.display()))
    })?;
    let temporary = folder.join(format!(
        ".{name}.{}-{}.tmp",
        std::process::id(),
        TEMPORARY_FILES.fetch_add(1, Ordering::Relaxed)
    ));
    let written = fs::File::create(&temporary).and_then(|mut file| file.write_all(bytes));
    if let Err(e) = written {
        let _ = fs::remove_file(&temporary);
        return Err(CacheWriteError(format!(
            "failed to write the cache file {}: {e}",
            temporary.display()
        )));
    }
    if let Err(e) = rename_over(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        return Err(CacheWriteError(format!(
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
/// entry, so the rename is asked again for a short bound, after which the
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
