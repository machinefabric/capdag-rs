//! Collection Input Types for Machine Processing
//!
//! This module defines the machine-facing collection structure for representing
//! folder hierarchies as structured input to caps.
//!
//! The collection structure is a machine internal representation of a
//! FILESYSTEM directory tree, built on-demand when a machine needs collection
//! input. It is not persisted: the engine has no folder entity.

use super::argument_binding::{CapInputFile, SourceEntityType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Media URN for a collection input structure (machine internal)
const COLLECTION_MEDIA_URN: &str = "media:collection;enc=utf-8;record";

/// A collection as structured input for machine processing.
///
/// This represents a folder hierarchy with files and nested subfolders,
/// suitable for passing to caps that accept collection input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapInputCollection {
    /// The folder ID from the database
    pub folder_id: String,
    /// Human-readable folder name
    pub folder_name: String,
    /// Files directly in this folder
    pub files: Vec<CollectionFile>,
    /// Nested subfolders (folder_name -> collection)
    pub folders: HashMap<String, CapInputCollection>,
    /// Media URN for this collection
    pub media_urn: String,
}

/// A file entry within a collection map.
///
/// Contains the information needed to process or reference a file
/// within a collection hierarchy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionFile {
    /// The listing ID from the database
    pub listing_id: String,
    /// Full filesystem path to the file
    pub file_path: String,
    /// Media URN describing the file type (e.g., "media:document;ext=pdf")
    pub media_urn: String,
    /// Optional human-readable title
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Security bookmark for sandboxed access.
    /// Runtime-only — never serialized (macOS sandbox bookmark, opaque binary).
    #[serde(skip)]
    pub security_bookmark: Option<Vec<u8>>,
}

impl CapInputCollection {
    /// Create a new empty collection
    pub fn new(folder_id: String, folder_name: String) -> Self {
        Self {
            folder_id,
            folder_name,
            files: Vec::new(),
            folders: HashMap::new(),
            media_urn: COLLECTION_MEDIA_URN.to_string(),
        }
    }

    /// Serialize to JSON Value for cap processing
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("CapInputCollection is always serializable")
    }

    /// Flatten to a list of CapInputFile for list handling.
    ///
    /// This recursively collects all files from this collection and
    /// all nested subfolders into a flat list.
    pub fn flatten_to_files(&self) -> Vec<CapInputFile> {
        let mut files = Vec::new();
        self.collect_files_recursive(&mut files);
        files
    }

    fn collect_files_recursive(&self, result: &mut Vec<CapInputFile>) {
        // Add files from this folder
        for file in &self.files {
            let mut input_file = CapInputFile::new(file.file_path.clone(), file.media_urn.clone());
            input_file.source_id = Some(file.listing_id.clone());
            input_file.source_type = Some(SourceEntityType::Listing);
            if let Some(ref bookmark) = file.security_bookmark {
                input_file.security_bookmark = Some(bookmark.clone());
            }
            result.push(input_file);
        }

        // Recursively add files from subfolders
        for subfolder in self.folders.values() {
            subfolder.collect_files_recursive(result);
        }
    }

    /// Get the total number of files in this collection (including nested)
    pub fn total_file_count(&self) -> usize {
        let mut count = self.files.len();
        for subfolder in self.folders.values() {
            count += subfolder.total_file_count();
        }
        count
    }

    /// Get the total number of folders in this collection (including nested)
    pub fn total_folder_count(&self) -> usize {
        let mut count = self.folders.len();
        for subfolder in self.folders.values() {
            count += subfolder.total_folder_count();
        }
        count
    }

    /// Check if this collection is empty (no files and no subfolders)
    pub fn is_empty(&self) -> bool {
        self.files.is_empty() && self.folders.is_empty()
    }
}

impl CollectionFile {
    /// Create a new collection file entry
    pub fn new(listing_id: String, file_path: String, media_urn: String) -> Self {
        Self {
            listing_id,
            file_path,
            media_urn,
            title: None,
            security_bookmark: None,
        }
    }

    /// Set the title
    pub fn with_title(mut self, title: String) -> Self {
        self.title = Some(title);
        self
    }

    /// Set the security bookmark
    pub fn with_security_bookmark(mut self, bookmark: Vec<u8>) -> Self {
        self.security_bookmark = Some(bookmark);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST716: Tests CapInputCollection empty collection has zero files and folders
    // Verifies is_empty() returns true and counts are zero for new collection
    #[test]
    fn test716_empty_collection() {
        let collection =
            CapInputCollection::new("folder-123".to_string(), "Test Folder".to_string());
        assert!(collection.is_empty());
        assert_eq!(collection.total_file_count(), 0);
        assert_eq!(collection.total_folder_count(), 0);
    }

    // TEST717: Tests CapInputCollection correctly counts files in flat collection
    // Verifies total_file_count() returns 2 for collection with 2 files, no folders
    #[test]
    fn test717_collection_with_files() {
        let mut collection =
            CapInputCollection::new("folder-123".to_string(), "Test Folder".to_string());
        collection.files.push(CollectionFile::new(
            "listing-1".to_string(),
            "/path/to/file1.pdf".to_string(),
            "media:document;ext=pdf".to_string(),
        ));
        collection.files.push(CollectionFile::new(
            "listing-2".to_string(),
            "/path/to/file2.md".to_string(),
            "media:enc=utf-8;ext=md".to_string(),
        ));

        assert!(!collection.is_empty());
        assert_eq!(collection.total_file_count(), 2);
        assert_eq!(collection.total_folder_count(), 0);
    }

    // TEST718: Tests CapInputCollection correctly counts files and folders in nested structure
    // Verifies total_file_count() includes subfolder files and total_folder_count() counts subfolders
    #[test]
    fn test718_nested_collection() {
        let mut root = CapInputCollection::new("folder-root".to_string(), "Root".to_string());
        root.files.push(CollectionFile::new(
            "listing-1".to_string(),
            "/path/file1.pdf".to_string(),
            "media:document;ext=pdf".to_string(),
        ));

        let mut subfolder =
            CapInputCollection::new("folder-sub".to_string(), "Subfolder".to_string());
        subfolder.files.push(CollectionFile::new(
            "listing-2".to_string(),
            "/path/sub/file2.pdf".to_string(),
            "media:document;ext=pdf".to_string(),
        ));
        subfolder.files.push(CollectionFile::new(
            "listing-3".to_string(),
            "/path/sub/file3.pdf".to_string(),
            "media:document;ext=pdf".to_string(),
        ));

        root.folders.insert("Subfolder".to_string(), subfolder);

        assert_eq!(root.total_file_count(), 3);
        assert_eq!(root.total_folder_count(), 1);
    }

    // TEST719: Tests CapInputCollection flatten_to_files recursively collects all files
    // Verifies flatten() extracts files from root and all subfolders into flat list
    #[test]
    fn test719_flatten_to_files() {
        let mut root = CapInputCollection::new("folder-root".to_string(), "Root".to_string());
        root.files.push(CollectionFile::new(
            "listing-1".to_string(),
            "/path/file1.pdf".to_string(),
            "media:document;ext=pdf".to_string(),
        ));

        let mut subfolder =
            CapInputCollection::new("folder-sub".to_string(), "Subfolder".to_string());
        subfolder.files.push(CollectionFile::new(
            "listing-2".to_string(),
            "/path/sub/file2.pdf".to_string(),
            "media:document;ext=pdf".to_string(),
        ));

        root.folders.insert("Subfolder".to_string(), subfolder);

        let flattened = root.flatten_to_files();
        assert_eq!(flattened.len(), 2);
        assert_eq!(flattened[0].file_path, "/path/file1.pdf");
        assert_eq!(flattened[1].file_path, "/path/sub/file2.pdf");
    }

    // TEST933: Tests CapInputCollection serializes to JSON and deserializes correctly
    // Verifies JSON round-trip preserves folder_id, folder_name, files and file metadata
    #[test]
    fn test933_serialization_roundtrip() {
        let mut collection =
            CapInputCollection::new("folder-123".to_string(), "Test Folder".to_string());
        collection.files.push(
            CollectionFile::new(
                "listing-1".to_string(),
                "/path/to/file.pdf".to_string(),
                "media:document;ext=pdf".to_string(),
            )
            .with_title("My Document".to_string()),
        );

        let json = collection.to_json();
        let roundtrip: CapInputCollection = serde_json::from_value(json).unwrap();

        assert_eq!(roundtrip.folder_id, collection.folder_id);
        assert_eq!(roundtrip.folder_name, collection.folder_name);
        assert_eq!(roundtrip.files.len(), 1);
        assert_eq!(roundtrip.files[0].title, Some("My Document".to_string()));
    }
}
