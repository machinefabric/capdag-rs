//! Where a model's file is, and so how it is brought into memory.
//!
//! Shared by the cartridges that load model weights from files the host
//! resolved: the same disk answers the same way for every loader.

use std::path::Path;

/// How a model's file is brought into memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelLoading {
    /// Mapped from a local disk; read into memory from a network one.
    Auto,
    /// Mapped: pages come from the file as they are touched.
    Map,
    /// Read into memory whole before the model is used.
    Read,
}

/// The model-loading setting's media URN: the argument a model-loading cap
/// takes (`--model-loading`), declared in the fabric as
/// `inference-model-loading`.
pub const MEDIA_MODEL_LOADING: &str =
    "media:enc=utf-8;inference;model;model-loading;operator;policy";

impl ModelLoading {
    /// The setting's value, as the fabric declares it: `auto`, `map` or
    /// `read`. Anything else is refused rather than taken for `auto`: a
    /// misspelt `raed` would otherwise map a model from the share it was
    /// set to keep away from.
    pub fn from_setting(value: &str) -> Result<Self, String> {
        match value.trim() {
            "auto" => Ok(Self::Auto),
            "map" => Ok(Self::Map),
            "read" => Ok(Self::Read),
            other => Err(format!(
                "model loading is `auto`, `map` or `read`; `{other}` is none of them"
            )),
        }
    }
}

/// Whether `model_path` is mapped rather than read, under `loading`.
///
/// Mapping is the right way to load from a local disk: nothing is copied, and
/// pages the model never touches are never read. From a network filesystem it
/// is not. Pages of a mapped file on an SMB share stopped coming in on macOS
/// with no transfer happening: a moondream2 load waited inside the kernel as
/// Metal made the mapping resident, and a Candle load waited in a plain copy
/// out of it — while other models from the same share had loaded. So `Auto`
/// reads a model on a network filesystem into memory, and maps everything
/// else. A filesystem that cannot be identified is
/// an error rather than a guess.
pub fn maps_from(model_path: &Path, loading: ModelLoading) -> Result<bool, String> {
    match loading {
        ModelLoading::Map => Ok(true),
        ModelLoading::Read => Ok(false),
        ModelLoading::Auto => Ok(!on_network_filesystem(model_path)?),
    }
}

/// Whether the file at `path` lives on a filesystem served over the network.
#[cfg(target_os = "macos")]
fn on_network_filesystem(path: &Path) -> Result<bool, String> {
    let stat = statfs(path)?;
    let name: Vec<u8> = stat
        .f_fstypename
        .iter()
        .take_while(|c| **c != 0)
        .map(|c| *c as u8)
        .collect();
    Ok(matches!(
        name.as_slice(),
        b"smbfs" | b"nfs" | b"afpfs" | b"webdav" | b"cifs"
    ))
}

/// Whether the file at `path` lives on a filesystem served over the network.
#[cfg(target_os = "linux")]
fn on_network_filesystem(path: &Path) -> Result<bool, String> {
    // Magic numbers from linux/magic.h and the SMB clients' own headers.
    const NFS: i64 = 0x6969;
    const SMB: i64 = 0x517B;
    const CIFS: i64 = 0xFF53_4D42;
    const SMB2: i64 = 0xFE53_4D42;
    let stat = statfs(path)?;
    Ok(matches!(stat.f_type as i64, NFS | SMB | CIFS | SMB2))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn statfs(path: &Path) -> Result<libc::statfs, String> {
    use std::os::unix::ffi::OsStrExt;
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| format!("{}: the model path contains a NUL byte", path.display()))?;
    let mut stat: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c_path` is NUL-terminated and `stat` is a valid out-pointer.
    if unsafe { libc::statfs(c_path.as_ptr(), &mut stat) } != 0 {
        return Err(format!(
            "{}: cannot tell what filesystem the model is on, which decides how it is loaded: {}",
            path.display(),
            std::io::Error::last_os_error()
        ));
    }
    Ok(stat)
}

/// Whether the file at `path` lives on a filesystem served over the network:
/// a UNC path, or a drive letter Windows reports as remote.
#[cfg(windows)]
fn on_network_filesystem(path: &Path) -> Result<bool, String> {
    use std::path::{Component, Prefix};
    match path.components().next() {
        Some(Component::Prefix(prefix)) => match prefix.kind() {
            Prefix::UNC(..) | Prefix::VerbatimUNC(..) => Ok(true),
            Prefix::Disk(letter) | Prefix::VerbatimDisk(letter) => {
                let root: Vec<u16> = format!("{}:\\", letter as char)
                    .encode_utf16()
                    .chain(std::iter::once(0))
                    .collect();
                // SAFETY: `root` is a NUL-terminated wide string.
                let kind = unsafe {
                    windows_sys::Win32::Storage::FileSystem::GetDriveTypeW(root.as_ptr())
                };
                Ok(kind == windows_sys::Win32::Storage::FileSystem::DRIVE_REMOTE)
            }
            _ => Ok(false),
        },
        _ => Err(format!(
            "{}: the model path is not absolute, so its drive cannot be told",
            path.display()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST12511: the setting is read as the fabric declares it, and a value
    // that is not one of its three is refused, not taken for `auto`.
    #[test]
    fn test12511_the_setting_is_one_of_three() {
        assert_eq!(ModelLoading::from_setting("auto").unwrap(), ModelLoading::Auto);
        assert_eq!(ModelLoading::from_setting(" map\n").unwrap(), ModelLoading::Map);
        assert_eq!(ModelLoading::from_setting("read").unwrap(), ModelLoading::Read);
        for bad in ["", "raed", "Auto", "mmap"] {
            assert!(ModelLoading::from_setting(bad).is_err(), "`{bad}` was accepted");
        }
    }

    // TEST12503: a model on a local disk is mapped, an explicit choice is
    // honoured whatever the disk, and a path whose filesystem cannot be told
    // is an error rather than a guess. (A model on an SMB share is read: the
    // GPU waited in the kernel for good on mapped pages from one.)
    #[test]
    fn test12503_how_a_model_is_loaded_follows_its_filesystem() {
        let dir = tempfile::tempdir().unwrap();
        let model = dir.path().join("model.gguf");
        std::fs::write(&model, b"GGUF").unwrap();
        assert!(maps_from(&model, ModelLoading::Auto).unwrap(), "a local model is mapped");
        assert!(maps_from(&model, ModelLoading::Map).unwrap());
        assert!(!maps_from(&model, ModelLoading::Read).unwrap());
        // On Unix the file's own filesystem is asked; Windows asks its drive,
        // which answers for a file that is not there.
        #[cfg(unix)]
        {
            let nowhere = dir.path().join("gone").join("model.gguf");
            assert!(
                maps_from(&nowhere, ModelLoading::Auto).is_err(),
                "a filesystem that cannot be asked about is an error"
            );
        }
    }
}
