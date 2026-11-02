//! Core types for InputResolver

use std::fmt;
use std::path::PathBuf;

/// A single input specification from the user
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputItem {
    /// A single file path
    File(PathBuf),
    /// A directory path (resolve recursively)
    Directory(PathBuf),
    /// A glob pattern (e.g., "*.pdf", "/tmp/**/*.json")
    Glob(String),
}

impl InputItem {
    /// Create from a string, auto-detecting the type
    pub fn from_string(s: &str) -> Self {
        // Check for glob metacharacters
        if s.contains('*') || s.contains('?') || s.contains('[') {
            return InputItem::Glob(s.to_string());
        }

        let path = PathBuf::from(s);

        // If path exists, check if it's a directory
        if path.is_dir() {
            InputItem::Directory(path)
        } else {
            // Assume file (existence checked during resolution)
            InputItem::File(path)
        }
    }
}

/// The detected internal structure of file content
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ContentStructure {
    /// Single opaque value (no list, no record markers)
    /// Examples: PDF, PNG, single string
    ScalarOpaque,

    /// Single structured record (no list, has record marker)
    /// Examples: JSON object, TOML file, config file
    ScalarRecord,

    /// List of opaque values (has list, no record markers)
    /// Examples: array of strings, multi-line text, JSON array of primitives
    ListOpaque,

    /// List of records (has list and record markers)
    /// Examples: CSV with headers, NDJSON of objects, JSON array of objects
    ListRecord,
}

impl ContentStructure {
    /// Returns true if this structure has the `list` marker
    pub fn is_list(&self) -> bool {
        matches!(
            self,
            ContentStructure::ListOpaque | ContentStructure::ListRecord
        )
    }

    /// Returns true if this structure has the `record` marker
    pub fn is_record(&self) -> bool {
        matches!(
            self,
            ContentStructure::ScalarRecord | ContentStructure::ListRecord
        )
    }
}

impl fmt::Display for ContentStructure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContentStructure::ScalarOpaque => write!(f, "scalar/opaque"),
            ContentStructure::ScalarRecord => write!(f, "scalar/record"),
            ContentStructure::ListOpaque => write!(f, "list/opaque"),
            ContentStructure::ListRecord => write!(f, "list/record"),
        }
    }
}

/// A single resolved file with detected media information
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedFile {
    /// Absolute path to the file
    pub path: PathBuf,

    /// Detected media URN (includes list/record markers if applicable)
    pub media_urn: String,

    /// File size in bytes
    pub size_bytes: u64,

    /// Content structure detected from inspection
    pub content_structure: ContentStructure,
}

/// The complete result of input resolution
#[derive(Debug, Clone)]
pub struct ResolvedInputSet {
    /// All resolved files
    pub files: Vec<ResolvedFile>,

    /// Whether the input is a sequence (multiple files).
    /// Determined solely by file count — content structure is irrelevant.
    pub is_sequence: bool,

    /// Common base media type (if files share a type), or None if heterogeneous
    pub common_media: Option<String>,
}

impl ResolvedInputSet {
    /// Create a new ResolvedInputSet from files
    pub fn new(files: Vec<ResolvedFile>) -> Self {
        let is_sequence = Self::compute_cardinality(&files);
        let common_media = Self::compute_common_media(&files);

        ResolvedInputSet {
            files,
            is_sequence,
            common_media,
        }
    }

    fn compute_cardinality(files: &[ResolvedFile]) -> bool {
        // is_sequence is determined solely by file count.
        // Content structure (list vs scalar) describes what's *inside* a file,
        // not how many items the user provided.  A single JSON file containing
        // an array is still one input item — is_sequence = false.
        files.len() > 1
    }

    fn compute_common_media(files: &[ResolvedFile]) -> Option<String> {
        if files.is_empty() {
            return None;
        }

        // Check if all files share an equivalent media URN via proper URN parsing.
        // Two URNs are "common" if they are equivalent (same tags in any order).
        let first = crate::urn::media_urn::MediaUrn::from_string(&files[0].media_urn)
            .unwrap_or_else(|e| {
                panic!(
                    "ResolvedInputSet: invalid media URN '{}': {}",
                    files[0].media_urn, e
                )
            });

        for file in files.iter().skip(1) {
            let other = crate::urn::media_urn::MediaUrn::from_string(&file.media_urn)
                .unwrap_or_else(|e| {
                    panic!(
                        "ResolvedInputSet: invalid media URN '{}': {}",
                        file.media_urn, e
                    )
                });
            if !first.is_equivalent(&other).unwrap_or(false) {
                return None;
            }
        }

        Some(files[0].media_urn.clone())
    }

    /// Returns true if all files have the same base media type
    pub fn is_homogeneous(&self) -> bool {
        self.common_media.is_some()
    }

    /// Returns the number of files
    pub fn len(&self) -> usize {
        self.files.len()
    }

    /// Returns true if no files
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
}

/// Errors that can occur during input resolution
#[derive(Debug)]
pub enum InputResolverError {
    /// Path does not exist
    NotFound(PathBuf),

    /// Permission denied when accessing path
    PermissionDenied(PathBuf),

    /// Invalid glob pattern
    InvalidGlob { pattern: String, reason: String },

    /// IO error during resolution
    IoError {
        path: PathBuf,
        error: std::io::Error,
    },

    /// Content inspection failed
    InspectionFailed { path: PathBuf, reason: String },

    /// Empty input (no paths provided)
    EmptyInput,

    /// All paths resolved to zero files
    NoFilesResolved,

    /// Symlink cycle detected
    SymlinkCycle { path: PathBuf },
}

impl fmt::Display for InputResolverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InputResolverError::NotFound(path) => {
                write!(f, "Path not found: {}", path.display())
            }
            InputResolverError::PermissionDenied(path) => {
                write!(f, "Permission denied: {}", path.display())
            }
            InputResolverError::InvalidGlob { pattern, reason } => {
                write!(f, "Invalid glob pattern '{}': {}", pattern, reason)
            }
            InputResolverError::IoError { path, error } => {
                write!(f, "IO error at {}: {}", path.display(), error)
            }
            InputResolverError::InspectionFailed { path, reason } => {
                write!(
                    f,
                    "Content inspection failed for {}: {}",
                    path.display(),
                    reason
                )
            }
            InputResolverError::EmptyInput => {
                write!(f, "No input paths provided")
            }
            InputResolverError::NoFilesResolved => {
                write!(f, "No files found after resolving all inputs")
            }
            InputResolverError::SymlinkCycle { path } => {
                write!(f, "Symlink cycle detected at: {}", path.display())
            }
        }
    }
}

impl std::error::Error for InputResolverError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            InputResolverError::IoError { error, .. } => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use std::io;
    use tempfile::tempdir;

    // TEST1143: InputItem::from_string distinguishes glob patterns, directories, and files
    #[test]
    fn test1143_input_item_from_string_distinguishes_glob_directory_and_file() {
        let dir = tempdir().expect("temp dir");
        let dir_item = InputItem::from_string(dir.path().to_str().expect("utf8 path"));
        assert_eq!(dir_item, InputItem::Directory(dir.path().to_path_buf()));

        let file_path = dir.path().join("missing.txt");
        let file_item = InputItem::from_string(file_path.to_str().expect("utf8 path"));
        assert_eq!(file_item, InputItem::File(file_path));

        let glob_item = InputItem::from_string("fixtures/**/*.pdf");
        assert_eq!(glob_item, InputItem::Glob("fixtures/**/*.pdf".to_string()));
    }

    // TEST1144: ContentStructure is_list/is_record helpers and Display implementation are correct
    #[test]
    fn test1144_content_structure_helpers_and_display() {
        assert!(!ContentStructure::ScalarOpaque.is_list());
        assert!(!ContentStructure::ScalarOpaque.is_record());
        assert_eq!(ContentStructure::ScalarOpaque.to_string(), "scalar/opaque");

        assert!(ContentStructure::ListRecord.is_list());
        assert!(ContentStructure::ListRecord.is_record());
        assert_eq!(ContentStructure::ListRecord.to_string(), "list/record");
    }

    // TEST1145: ResolvedInputSet uses URN equivalence for common_media and file count for is_sequence
    #[test]
    fn test1145_resolved_input_set_uses_equivalent_media_and_file_count_cardinality() {
        let single_list_file = ResolvedInputSet::new(vec![ResolvedFile {
            path: PathBuf::from("/tmp/items.json"),
            media_urn: "media:application;enc=utf8;fmt=json;list;record".to_string(),
            size_bytes: 42,
            content_structure: ContentStructure::ListRecord,
        }]);
        assert!(!single_list_file.is_sequence);
        assert!(single_list_file.is_homogeneous());
        assert_eq!(
            single_list_file.common_media.as_deref(),
            Some("media:application;enc=utf8;fmt=json;list;record")
        );

        let equivalent_ordering = ResolvedInputSet::new(vec![
            ResolvedFile {
                path: PathBuf::from("/tmp/a.json"),
                media_urn: "media:application;enc=utf8;fmt=json;record".to_string(),
                size_bytes: 10,
                content_structure: ContentStructure::ScalarRecord,
            },
            ResolvedFile {
                path: PathBuf::from("/tmp/b.json"),
                media_urn: "media:application;enc=utf8;fmt=json;record".to_string(),
                size_bytes: 11,
                content_structure: ContentStructure::ScalarRecord,
            },
        ]);
        assert!(equivalent_ordering.is_sequence);
        assert!(equivalent_ordering.is_homogeneous());
        assert_eq!(
            equivalent_ordering.common_media.as_deref(),
            Some("media:application;enc=utf8;fmt=json;record")
        );
    }

    // TEST1146: InputResolverError Display and source() implementations produce correct messages
    #[test]
    fn test1146_input_resolver_error_display_and_source() {
        let io_error = InputResolverError::IoError {
            path: PathBuf::from("/tmp/data.bin"),
            error: io::Error::new(io::ErrorKind::PermissionDenied, "no access"),
        };
        assert!(io_error
            .to_string()
            .contains("IO error at /tmp/data.bin: no access"));
        assert!(io_error.source().is_some());

        let invalid_glob = InputResolverError::InvalidGlob {
            pattern: "[".to_string(),
            reason: "unclosed character class".to_string(),
        };
        assert_eq!(
            invalid_glob.to_string(),
            "Invalid glob pattern '[': unclosed character class"
        );
        assert!(invalid_glob.source().is_none());
    }
}
