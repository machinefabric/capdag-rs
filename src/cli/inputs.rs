//! What a run is given: files, directories, globs, stdin, a live source.

use std::path::{Path, PathBuf};

use super::error::{CliError, CliResult};
use super::names;
use crate::fabric::registry::FabricRegistry;
use crate::MediaUrn;

/// Extensions a directory's listing leaves out: what lies beside content, not
/// content.
const SKIP_EXTENSIONS: &[&str] = &[
    "json", "log", "txt", "md", "yml", "yaml", "toml", "sh", "py", "rb", "js", "ts", "rs", "go",
    "c", "h", "cpp", "zip", "tar", "gz", "bz2", "xz",
];

/// Files a directory's listing always leaves out.
const SKIP_FILES: &[&str] = &[".DS_Store", "Thumbs.db", ".gitignore", ".gitkeep"];

/// A path as given: a file, a directory (its content files, not recursive), or
/// a glob. Each given path must name something.
pub fn expand(given: &str) -> CliResult<Vec<PathBuf>> {
    let path = PathBuf::from(given);
    if given.contains('*') || given.contains('?') {
        let pattern = glob::glob(given).map_err(|e| {
            CliError::usage(format!("'{given}' is not a valid glob pattern")).context(e.to_string())
        })?;
        let mut files: Vec<PathBuf> = pattern.filter_map(Result::ok).filter(|p| p.is_file()).collect();
        if files.is_empty() {
            return Err(CliError::usage(format!("no file matches '{given}'"))
                .hint("check the pattern, or give the files by name"));
        }
        files.sort();
        return Ok(files);
    }
    if path.is_dir() {
        let entries = std::fs::read_dir(&path).map_err(|e| {
            CliError::failed(format!("the directory '{given}' could not be read")).context(e.to_string())
        })?;
        let mut files: Vec<PathBuf> = entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|p| is_content_file(p))
            .collect();
        if files.is_empty() {
            return Err(CliError::usage(format!("the directory '{given}' holds no input files"))
                .context(format!(
                    "a directory's listing leaves out hidden files and these extensions: {}",
                    SKIP_EXTENSIONS.join(", ")
                ))
                .hint("give the files by name, or a glob such as 'dir/*.txt'"));
        }
        files.sort();
        return Ok(files);
    }
    if path.is_file() {
        return Ok(vec![path]);
    }
    Err(CliError::usage(format!("'{given}' does not exist"))
        .hint("check the path; inputs are files, directories or globs"))
}

fn is_content_file(path: &Path) -> bool {
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    if name.starts_with('.') || SKIP_FILES.contains(&name) || path.is_dir() {
        return false;
    }
    match path.extension().and_then(|e| e.to_str()) {
        Some(ext) => !SKIP_EXTENSIONS.contains(&ext.to_lowercase().as_str()),
        None => true,
    }
}

/// Read a file the run is given.
pub fn read(path: &Path) -> CliResult<Vec<u8>> {
    std::fs::read(path).map_err(|e| {
        CliError::failed(format!("'{}' could not be read", path.display())).context(e.to_string())
    })
}

/// Read a value given as `@<path>` (the file's bytes) or as itself.
pub fn value(flag: &str, name: &str, given: &str) -> CliResult<Vec<u8>> {
    match given.strip_prefix('@') {
        Some(path) => std::fs::read(path).map_err(|e| {
            CliError::failed(format!("{flag} {name}: '{path}' could not be read")).context(e.to_string())
        }),
        None => Ok(given.as_bytes().to_vec()),
    }
}

/// `name=value`, as `--input` and `--arg` take it.
pub fn pair<'a>(flag: &str, given: &'a str) -> CliResult<(&'a str, &'a str)> {
    match given.split_once('=') {
        Some((name, value)) if !name.is_empty() => Ok((name, value)),
        _ => Err(CliError::usage(format!("{flag} '{given}' is not <name>=<value>"))
            .hint(format!("for example: {flag} doc=@report.pdf"))),
    }
}

/// The media a file is, as the desktop clients detect it: by its extension,
/// then by what its content validates as. A file the fabric cannot identify is
/// an error naming it — never the bare `media:`, which would plan into a dead
/// end or merge files of different types.
pub fn detect(file: &Path, registry: &FabricRegistry) -> CliResult<MediaUrn> {
    let resolved = crate::detect_file_discriminated(file, registry, &crate::ValueAdapterRegistry::new())
        .map_err(|e| {
            CliError::failed(format!("the type of '{}' could not be detected", file.display()))
                .context(e.to_string())
        })?;
    if resolved.media_urn == "media:" {
        return Err(match file.extension().and_then(|e| e.to_str()) {
            Some(ext) => CliError::failed(format!(
                "capdag does not know what kind of file '{}' is",
                file.display()
            ))
            .context(format!("the fabric defines no media type for the extension .{ext}"))
            .hint("name its input explicitly with --input <name>=@<path>, or report the missing type"),
            None => CliError::failed(format!(
                "capdag does not know what kind of file '{}' is",
                file.display()
            ))
            .context("the file has no extension to detect its type from")
            .hint("give the file its usual extension, or --input <name>=@<path>"),
        });
    }
    MediaUrn::from_string(&resolved.media_urn).map_err(|e| {
        CliError::failed(format!("'{}' was detected as an invalid media type", file.display()))
            .context(e.to_string())
    })
}

/// A file's media, as a person names it.
pub fn describe(file: &Path, media: &MediaUrn, registry: &FabricRegistry) -> String {
    format!("{} ({})", file.display(), names::media(registry, media))
}

/// The stem outputs of a file are named for: `report` for `report.pdf`.
pub fn stem(path: &Path) -> String {
    path.file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "input".to_string())
}

/// Text made safe for a file name: `media:audio;live;microphone` →
/// `audio-live-microphone`.
pub fn file_safe(text: &str) -> String {
    text.trim_start_matches("media:")
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

/// `--dev-bin`: a cartridge binary, or a directory of them.
pub fn dev_binaries(given: &[PathBuf]) -> CliResult<Vec<PathBuf>> {
    let mut out = Vec::new();
    for path in given {
        if path.is_file() {
            out.push(path.clone());
        } else if path.is_dir() {
            let entries = std::fs::read_dir(path).map_err(|e| {
                CliError::failed(format!("--dev-bin {}: the directory could not be read", path.display()))
                    .context(e.to_string())
            })?;
            let found: Vec<PathBuf> = entries
                .filter_map(Result::ok)
                .map(|e| e.path())
                .filter(|p| is_executable(p))
                .collect();
            if found.is_empty() {
                return Err(CliError::usage(format!(
                    "--dev-bin {}: the directory holds no executable",
                    path.display()
                )));
            }
            out.extend(found);
        } else {
            return Err(CliError::usage(format!("--dev-bin {}: no such file or directory", path.display())));
        }
    }
    Ok(out)
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    path.is_file() && path.metadata().map(|m| m.permissions().mode() & 0o111 != 0).unwrap_or(false)
}

#[cfg(windows)]
fn is_executable(path: &Path) -> bool {
    path.is_file()
        && path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("exe"))
}

#[cfg(not(any(unix, windows)))]
fn is_executable(_path: &Path) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST20115: an input that names nothing is a usage error naming it; a
    // directory lists its content files, sorted.
    #[test]
    fn test20115_inputs_name_what_is_missing() {
        let error = expand("no/such/file.pdf").unwrap_err();
        assert_eq!(error.problem, "'no/such/file.pdf' does not exist");
        assert_eq!(error.exit, super::super::error::Exit::Usage);

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("b.pdf"), b"b").unwrap();
        std::fs::write(dir.path().join("a.pdf"), b"a").unwrap();
        std::fs::write(dir.path().join(".hidden"), b"h").unwrap();
        std::fs::write(dir.path().join("notes.md"), b"n").unwrap();
        let files = expand(dir.path().to_str().unwrap()).unwrap();
        let names: Vec<String> = files.iter().map(|f| f.file_name().unwrap().to_string_lossy().into_owned()).collect();
        assert_eq!(names, ["a.pdf", "b.pdf"]);
    }
}
