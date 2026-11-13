//! Resolving a run's inputs — files, directories and globs — to the files
//! they name and what each of them is.
//!
//! What a file is comes from the one detection every host runs
//! ([`detect_file_media`]); this module adds only the path resolution around
//! it and the shape of the result.

use std::path::Path;

use crate::input_resolver::adapter::CartridgeAdapterInvoker;
use crate::input_resolver::detection::{detect_file_media, AdapterSource};
use crate::input_resolver::path_resolver;
use crate::input_resolver::value_adapter_registry::ValueAdapterRegistry;
use crate::input_resolver::{
    ContentStructure, InputItem, InputResolverError, ResolvedFile, ResolvedInputSet,
};
use crate::media::registry::FabricRegistry;
use crate::urn::media_urn::MediaUrn;

/// How a host detects its inputs: its fabric, the cartridges it can ask, and
/// its value adapters.
pub struct Detection<'a> {
    pub registry: &'a FabricRegistry,
    pub adapters: &'a dyn AdapterSource,
    pub invoker: &'a dyn CartridgeAdapterInvoker,
    pub value_adapters: &'a ValueAdapterRegistry,
}

/// What one file is, as a run's input: the media a host that cannot ask
/// takes ([`crate::input_resolver::detection::primary_media`]).
pub async fn resolve_file(path: &Path, detection: &Detection<'_>) -> Result<ResolvedFile, InputResolverError> {
    let found = detect_file_media(
        path,
        detection.registry,
        detection.adapters,
        detection.invoker,
        detection.value_adapters,
    )
    .await?;
    let media_urn = found.primary().to_string();
    let urn = MediaUrn::from_string(&media_urn).map_err(|e| InputResolverError::InspectionFailed {
        path: path.to_path_buf(),
        reason: format!("'{media_urn}' is no media URN: {e}"),
    })?;
    Ok(ResolvedFile {
        path: found.path,
        media_urn,
        size_bytes: found.size_bytes,
        content_structure: structure_from_marker_tags(&urn),
    })
}

/// Resolve one input item.
pub async fn resolve_input(
    item: InputItem,
    detection: &Detection<'_>,
) -> Result<ResolvedInputSet, InputResolverError> {
    resolve_inputs(vec![item], detection).await
}

/// Resolve input items to the files they name and what each is.
pub async fn resolve_inputs(
    items: Vec<InputItem>,
    detection: &Detection<'_>,
) -> Result<ResolvedInputSet, InputResolverError> {
    let paths = path_resolver::resolve_items(&items)?;
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        files.push(resolve_file(&path, detection).await?);
    }
    if files.is_empty() {
        return Err(InputResolverError::NoFilesResolved);
    }
    Ok(ResolvedInputSet::new(files))
}

/// Resolve string paths (a file, a directory or a glob each).
pub async fn resolve_paths(
    paths: &[&str],
    detection: &Detection<'_>,
) -> Result<ResolvedInputSet, InputResolverError> {
    let items: Vec<InputItem> = paths.iter().map(|s| InputItem::from_string(s)).collect();
    resolve_inputs(items, detection).await
}

/// Determine content structure from a MediaUrn's marker tags
fn structure_from_marker_tags(urn: &MediaUrn) -> ContentStructure {
    let has_list = urn.has_marker_tag("list");
    let has_record = urn.has_marker_tag("record");

    match (has_list, has_record) {
        (true, true) => ContentStructure::ListRecord,
        (true, false) => ContentStructure::ListOpaque,
        (false, true) => ContentStructure::ScalarRecord,
        (false, false) => ContentStructure::ScalarOpaque,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input_resolver::detection::{NoAdapters, NoInvoker};
    use std::fs;
    use std::io::Write;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_dir() -> TempDir {
        TempDir::new().unwrap()
    }

    fn create_file(dir: &TempDir, name: &str, content: &[u8]) -> std::path::PathBuf {
        let path = dir.path().join(name);
        let mut file = fs::File::create(&path).unwrap();
        file.write_all(content).unwrap();
        path
    }

    /// Build a `FabricRegistry` pre-seeded with the media defs the
    /// resolver tests reference (`pdf`, `txt`, `json`).
    /// The registry hydrates extension lookups from spec arrival —
    /// there is no compiled-in fallback table — so tests must seed
    /// the specs they exercise explicitly.
    fn create_test_fabric_registry() -> (Arc<FabricRegistry>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let registry = FabricRegistry::new_for_test();
        seed_resolver_test_specs(&registry);
        (Arc::new(registry), temp_dir)
    }

    fn seed_resolver_test_specs(registry: &FabricRegistry) {
        use crate::StoredMediaDef;

        // PDF
        registry.insert_cached_media_def_for_test(StoredMediaDef {
            dims: Vec::new(),
            version: 0,
            urn: "media:document;ext=pdf".to_string(),
            media_type: "application/pdf".to_string(),
            title: "PDF".to_string(),
            profile_uri: None,
            schema: None,
            description: None,
            documentation: None,
            validation: None,
            metadata: None,
            extensions: vec!["pdf".to_string()],
        });

        // JSON family
        registry.insert_cached_media_def_for_test(StoredMediaDef {
            dims: Vec::new(),
            version: 0,
            urn: "media:enc=utf8;ext=json;fmt=json;record".to_string(),
            media_type: "application/json".to_string(),
            title: "JSON".to_string(),
            profile_uri: None,
            schema: None,
            description: None,
            documentation: None,
            validation: None,
            metadata: None,
            extensions: vec!["json".to_string()],
        });

        // Plain text
        registry.insert_cached_media_def_for_test(StoredMediaDef {
            dims: Vec::new(),
            version: 0,
            urn: "media:enc=utf8;ext=txt;list;plain-text".to_string(),
            media_type: "text/plain".to_string(),
            title: "Text".to_string(),
            profile_uri: None,
            schema: None,
            description: None,
            documentation: None,
            validation: None,
            metadata: None,
            extensions: vec!["txt".to_string()],
        });
    }

    async fn resolve(paths: &[&str], registry: &FabricRegistry) -> ResolvedInputSet {
        let value_adapters = ValueAdapterRegistry::new();
        let detection = Detection {
            registry,
            adapters: &NoAdapters,
            invoker: &NoInvoker,
            value_adapters: &value_adapters,
        };
        resolve_paths(paths, &detection).await.unwrap()
    }

    // TEST1090: 1 file → is_sequence=false
    #[tokio::test]
    async fn test1090_single_file_scalar() {
        let (registry, _temp) = create_test_fabric_registry();
        let dir = create_test_dir();
        let path = create_file(&dir, "doc.pdf", b"%PDF-1.4");

        let result = resolve(&[path.to_str().unwrap()], &registry).await;

        assert_eq!(result.files.len(), 1);
        assert!(!result.is_sequence, "single file must be is_sequence=false");
    }

    // TEST1092: 2 files → is_sequence=true
    #[tokio::test]
    async fn test1092_two_files() {
        let (registry, _temp) = create_test_fabric_registry();
        let dir = create_test_dir();
        let path1 = create_file(&dir, "a.pdf", b"%PDF-1.4");
        let path2 = create_file(&dir, "b.pdf", b"%PDF-1.5");

        let result = resolve(&[path1.to_str().unwrap(), path2.to_str().unwrap()], &registry).await;

        assert_eq!(result.files.len(), 2);
        assert!(
            result.is_sequence,
            "multiple files must be is_sequence=true"
        );
    }

    // TEST1093: 1 dir with 1 file → is_sequence=false
    #[tokio::test]
    async fn test1093_dir_single_file() {
        let (registry, _temp) = create_test_fabric_registry();
        let dir = create_test_dir();
        create_file(&dir, "only.pdf", b"%PDF-1.4");

        let result = resolve(&[dir.path().to_str().unwrap()], &registry).await;

        assert_eq!(result.files.len(), 1);
        assert!(
            !result.is_sequence,
            "directory with single file must be is_sequence=false"
        );
    }

    // TEST1094: 1 dir with 3 files → is_sequence=true
    #[tokio::test]
    async fn test1094_dir_multiple_files() {
        let (registry, _temp) = create_test_fabric_registry();
        let dir = create_test_dir();
        create_file(&dir, "a.txt", b"hello");
        create_file(&dir, "b.txt", b"world");
        create_file(&dir, "c.txt", b"test");

        let result = resolve(&[dir.path().to_str().unwrap()], &registry).await;

        assert_eq!(result.files.len(), 3);
        assert!(
            result.is_sequence,
            "directory with multiple files must be is_sequence=true"
        );
    }

    // TEST977: OS files excluded in resolve_paths
    #[tokio::test]
    async fn test977_os_files_excluded_integration() {
        let (registry, _temp) = create_test_fabric_registry();
        let dir = create_test_dir();
        create_file(&dir, ".DS_Store", b"");
        create_file(&dir, "real.txt", b"content");

        let result = resolve(&[dir.path().to_str().unwrap()], &registry).await;

        assert_eq!(result.files.len(), 1);
        assert!(result.files[0].path.to_str().unwrap().contains("real.txt"));
    }

    // TEST1288: structure_from_marker_tags correctly maps tag combinations to ContentStructure
    #[test]
    fn test1288_structure_from_marker_tags() {
        let scalar_opaque = MediaUrn::from_string("media:document;ext=pdf").unwrap();
        assert_eq!(
            structure_from_marker_tags(&scalar_opaque),
            ContentStructure::ScalarOpaque
        );

        let scalar_record = MediaUrn::from_string("media:enc=utf8;fmt=json;record").unwrap();
        assert_eq!(
            structure_from_marker_tags(&scalar_record),
            ContentStructure::ScalarRecord
        );

        let list_opaque = MediaUrn::from_string("media:enc=utf8;list").unwrap();
        assert_eq!(
            structure_from_marker_tags(&list_opaque),
            ContentStructure::ListOpaque
        );

        let list_record = MediaUrn::from_string("media:enc=utf8;fmt=json;list;record").unwrap();
        assert_eq!(
            structure_from_marker_tags(&list_record),
            ContentStructure::ListRecord
        );
    }

}
