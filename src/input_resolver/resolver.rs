//! Main resolver — combines path resolution with media detection
//!
//! Two tiers of detection:
//! 1. **Synchronous extension-based lookup** — fast, unconfirmed, for UI/menu queries.
//!    Returns candidate URNs from the media registry based on file extension alone.
//! 2. **Async cartridge-confirmed detection** — invokes cartridge adapter-selection caps
//!    to confirm file type at a content/binary level.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use crate::input_resolver::adapter::{
    AdapterResult, CartridgeAdapterInvoker, MAX_CONTENT_INSPECTION_BYTES,
};
use crate::input_resolver::value_adapter_registry::ValueAdapterRegistry;
use crate::input_resolver::adapters::MediaAdapterRegistry;
use crate::input_resolver::path_resolver;
use crate::input_resolver::{
    ContentStructure, InputItem, InputResolverError, ResolvedFile, ResolvedInputSet,
};
use crate::media::registry::FabricRegistry;
use crate::urn::media_urn::MediaUrn;

/// Discriminate candidate media URNs by validation rules in their specs.
///
/// Given file content and a set of candidate URN strings (e.g. all URNs for
/// a file extension), eliminates candidates whose media def validation
/// rules reject the content. Candidates with no validation rules survive
/// (no rules = no basis for elimination).
///
/// Returns the surviving candidate URN strings in their original order.
/// The baseline URN is the adapter's structural detection result (e.g., "media:fmt=json;record"
/// for a JSON object). Candidates more specific than the baseline must have validation rules
/// that positively match the content — otherwise they're eliminated (they overclaim without proof).
/// Candidates equivalent to or less specific than the baseline survive without validation.
pub fn discriminate_candidates_by_validation(
    content: &[u8],
    candidate_urns: &[String],
    fabric_registry: &FabricRegistry,
    baseline_urn: &str,
) -> Vec<String> {
    let content_str = std::str::from_utf8(content).ok();
    let content_len = content.len();

    let baseline = MediaUrn::from_string(baseline_urn).unwrap_or_else(|e| {
        panic!(
            "discriminate_candidates_by_validation: invalid baseline URN '{}': {}",
            baseline_urn, e
        )
    });

    candidate_urns
        .iter()
        .filter(|urn| {
            let spec = match fabric_registry.get_cached_media_def(urn) {
                Some(spec) => spec,
                None => return true, // No spec in cache → cannot eliminate
            };

            let validation = match &spec.validation {
                Some(v) if !v.is_empty() => v,
                _ => {
                    // No validation rules. Only keep if the candidate is not more
                    // specific than the baseline (more specific without validation = overclaiming).
                    let candidate_urn = match MediaUrn::from_string(urn) {
                        Ok(u) => u,
                        Err(_) => return true, // Can't parse → keep
                    };
                    // Keep if baseline conforms to candidate (candidate is same or more general)
                    return baseline.conforms_to(&candidate_urn).unwrap_or(true);
                }
            };

            // Check pattern (regex against content as UTF-8)
            if let Some(ref pattern) = validation.pattern {
                match content_str {
                    Some(text) => {
                        match regex::Regex::new(pattern) {
                            Ok(re) => {
                                if !re.is_match(text) {
                                    return false; // Pattern didn't match → eliminate
                                }
                            }
                            Err(_) => {
                                // Invalid regex in spec is a spec authoring bug — hard fail
                                // would block all candidates with that spec. Log and keep
                                // the candidate (don't eliminate based on broken rule).
                                tracing::error!(
                                    "Media def '{}' has invalid validation pattern '{}' — \
                                     fix the TOML definition in capfab/src/media",
                                    urn,
                                    pattern
                                );
                            }
                        }
                    }
                    None => {
                        // Binary content cannot match a text pattern → eliminate
                        return false;
                    }
                }
            }

            // Check min_length (byte length)
            if let Some(min_len) = validation.min_length {
                if content_len < min_len {
                    return false;
                }
            }

            // Check max_length (byte length)
            if let Some(max_len) = validation.max_length {
                if content_len > max_len {
                    return false;
                }
            }

            // Check allowed_values
            if let Some(ref allowed) = validation.allowed_values {
                match content_str {
                    Some(text) => {
                        let trimmed = text.trim();
                        if !allowed.iter().any(|v| v == trimmed) {
                            return false;
                        }
                    }
                    None => return false, // Binary content can't match allowed text values
                }
            }

            true // Survived all checks
        })
        .cloned()
        .collect()
}

// =============================================================================
// SYNCHRONOUS EXTENSION-BASED DETECTION (preliminary, for UI queries)
// =============================================================================

/// Resolve a single input item using the supplied unified
/// `FabricRegistry` for extension lookups.
pub fn resolve_input(
    item: InputItem,
    fabric_registry: &FabricRegistry,
) -> Result<ResolvedInputSet, InputResolverError> {
    resolve_inputs(vec![item], fabric_registry)
}

/// Resolve multiple input items using the supplied unified
/// `FabricRegistry` for extension lookups.
pub fn resolve_inputs(
    items: Vec<InputItem>,
    fabric_registry: &FabricRegistry,
) -> Result<ResolvedInputSet, InputResolverError> {
    let paths = path_resolver::resolve_items(&items)?;

    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        let resolved = detect_file_by_extension_with_registry(&path, fabric_registry)?;
        files.push(resolved);
    }

    if files.is_empty() {
        return Err(InputResolverError::NoFilesResolved);
    }

    Ok(ResolvedInputSet::new(files))
}

/// Convenience: resolve from string paths (auto-detect file/dir/glob)
/// using the supplied `FabricRegistry` for extension lookups.
pub fn resolve_paths(
    paths: &[&str],
    fabric_registry: &FabricRegistry,
) -> Result<ResolvedInputSet, InputResolverError> {
    let items: Vec<InputItem> = paths.iter().map(|s| InputItem::from_string(s)).collect();
    resolve_inputs(items, fabric_registry)
}

/// Detect media type for a file using extension and a `FabricRegistry`.
pub fn detect_file_with_fabric_registry(
    path: &Path,
    fabric_registry: Arc<FabricRegistry>,
) -> Result<ResolvedFile, InputResolverError> {
    detect_file_by_extension_with_registry(path, &fabric_registry)
}

/// Post-discrimination survivor refinement — the engine's `analyze_file_content`
/// steps 6–8, shared VERBATIM by the engine and [`detect_file_discriminated`]
/// so they can never diverge:
///
/// 6. refine each survivor through the value adapters (content-aware URN
///    refinement, e.g. model-spec family tags);
/// 7. strip the `list` cardinality tag (cardinality is the planner's job, not
///    detection's);
/// 8. dedup, preserving order.
pub fn refine_survivors(
    survivors: Vec<String>,
    value_adapters: &ValueAdapterRegistry,
    content: &[u8],
) -> Vec<String> {
    let content_str = std::str::from_utf8(content).unwrap_or("");
    let mut seen = std::collections::HashSet::new();
    survivors
        .into_iter()
        .map(|urn| value_adapters.refine_media_urn(&urn, content_str))
        .map(|urn| match MediaUrn::from_string(&urn) {
            Ok(u) => u.without_tag("list").to_string(),
            Err(_) => urn,
        })
        .filter(|u| seen.insert(u.clone()))
        .collect()
}

/// Everything the content-discrimination core learns about one file — enough
/// for a host to continue the pipeline (cartridge-adapter gating, value
/// refinement) without re-reading or re-deriving anything.
#[derive(Debug, Clone)]
pub struct FileDiscrimination {
    /// The bounded content prefix that was inspected
    /// (`MAX_CONTENT_INSPECTION_BYTES` cap).
    pub content: Vec<u8>,
    /// Baseline structural detection (most specific extension candidate).
    pub baseline: ResolvedFile,
    /// The file's lowercased extension ("" when it has none).
    pub extension: String,
    /// ALL candidate media URNs registered for the extension.
    pub candidates: Vec<String>,
    /// The static-validation survivors: candidates whose declared
    /// `validation` rules accept the content (a candidate more specific than
    /// the baseline must PROVE its claim or be eliminated). When the
    /// extension has NO candidates, this is the baseline URN alone — there is
    /// nothing to discriminate between.
    pub survivors: Vec<String>,
}

/// The content-discrimination CORE — the engine's `analyze_file_content`
/// steps 2–5a, extracted verbatim so every consumer runs the same pipeline:
///
/// 2. read a bounded content prefix (`MAX_CONTENT_INSPECTION_BYTES`);
/// 3. collect ALL candidate media URNs for the file's extension;
/// 4. take the baseline structural detection
///    ([`detect_file_with_fabric_registry`]);
/// 5a. discriminate the candidates by their declared `validation` rules
///     against the content ([`discriminate_candidates_by_validation`]).
///
/// The two consumers compose it differently and share every step:
/// - the ENGINE gates `survivors` through its live cartridge adapters
///   (step 5b, [`discriminate_by_cartridge_handlers`]) and then refines
///   ([`refine_survivors`], steps 6–8);
/// - the CLI ([`detect_file_discriminated`]) has no hosted cartridges — the
///   engine's own fallback when no adapter matches — so it refines directly
///   and picks the most specific survivor.
pub fn discriminate_file(
    path: &Path,
    fabric_registry: &FabricRegistry,
) -> Result<FileDiscrimination, InputResolverError> {
    let baseline = detect_file_by_extension_with_registry(path, fabric_registry)?;

    // Bounded content prefix, same cap for every consumer.
    let content = {
        use std::io::Read;
        let read_size = (baseline.size_bytes as usize).min(MAX_CONTENT_INSPECTION_BYTES);
        let mut buf = vec![0u8; read_size];
        let mut file = fs::File::open(path).map_err(|e| InputResolverError::IoError {
            path: path.to_path_buf(),
            error: e,
        })?;
        let n = file
            .read(&mut buf)
            .map_err(|e| InputResolverError::IoError {
                path: path.to_path_buf(),
                error: e,
            })?;
        buf.truncate(n);
        buf
    };

    let extension = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_lowercase())
        .unwrap_or_default();
    let candidates = if extension.is_empty() {
        Vec::new()
    } else {
        fabric_registry
            .media_urns_for_extension(&extension)
            .unwrap_or_default()
    };

    let survivors = if candidates.is_empty() {
        vec![baseline.media_urn.clone()]
    } else {
        discriminate_candidates_by_validation(
            &content,
            &candidates,
            fabric_registry,
            &baseline.media_urn,
        )
    };

    Ok(FileDiscrimination {
        content,
        baseline,
        extension,
        candidates,
        survivors,
    })
}

/// Content-discriminated detection for hosts with NO live cartridges: the
/// discrimination core ([`discriminate_file`]) followed by survivor
/// refinement ([`refine_survivors`]) and a single most-specific answer
/// (first wins a specificity tie — survivor order preserves the registry's
/// sorted candidate order, so the pick is deterministic).
///
/// This matches the engine with zero adapters registered — the engine's own
/// fallback verdict. Hosts with live cartridges run the same core and gate
/// the survivor set through [`discriminate_by_cartridge_handlers`] instead.
pub fn detect_file_discriminated(
    path: &Path,
    fabric_registry: &FabricRegistry,
    value_adapters: &ValueAdapterRegistry,
) -> Result<ResolvedFile, InputResolverError> {
    let disc = discriminate_file(path, fabric_registry)?;
    let FileDiscrimination {
        content,
        baseline,
        extension,
        candidates,
        survivors,
    } = disc;

    if survivors.is_empty() {
        // Every extension candidate's validation rejected the content: the
        // file claims an extension whose declared types all disown it.
        return Err(InputResolverError::InspectionFailed {
            path: path.to_path_buf(),
            reason: format!(
                "content matches none of the {} media definitions declared for extension '{extension}'",
                candidates.len()
            ),
        });
    }

    let refined = refine_survivors(survivors, value_adapters, &content);

    let mut best: Option<(MediaUrn, String)> = None;
    for urn_str in &refined {
        if let Ok(urn) = MediaUrn::from_string(urn_str) {
            let wins = match &best {
                Some((b, _)) => urn.specificity() > b.specificity(),
                None => true,
            };
            if wins {
                best = Some((urn, urn_str.clone()));
            }
        }
    }
    let Some((chosen, chosen_str)) = best else {
        return Err(InputResolverError::InspectionFailed {
            path: path.to_path_buf(),
            reason: "no surviving media candidate parses as a media URN".to_string(),
        });
    };

    Ok(ResolvedFile {
        path: baseline.path,
        media_urn: chosen_str,
        size_bytes: baseline.size_bytes,
        content_structure: structure_from_marker_tags(&chosen),
    })
}

/// Extension-based detection using a specific FabricRegistry.
fn detect_file_by_extension_with_registry(
    path: &Path,
    fabric_registry: &FabricRegistry,
) -> Result<ResolvedFile, InputResolverError> {
    let metadata = fs::metadata(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            InputResolverError::NotFound(path.to_path_buf())
        } else if e.kind() == std::io::ErrorKind::PermissionDenied {
            InputResolverError::PermissionDenied(path.to_path_buf())
        } else {
            InputResolverError::IoError {
                path: path.to_path_buf(),
                error: e,
            }
        }
    })?;

    let size_bytes = metadata.len();

    // Get extension and look up candidates
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_lowercase());

    let (media_urn, content_structure) = match ext {
        Some(ref ext_str) => {
            match fabric_registry.media_urns_for_extension(ext_str) {
                Ok(urns) if !urns.is_empty() => {
                    // Parse and pick the most specific candidate
                    let mut best_urn: Option<(MediaUrn, String)> = None;
                    for urn_str in &urns {
                        if let Ok(urn) = MediaUrn::from_string(urn_str) {
                            let dominated = match &best_urn {
                                Some((best, _)) => urn.specificity() > best.specificity(),
                                None => true,
                            };
                            if dominated {
                                best_urn = Some((urn, urn_str.clone()));
                            }
                        }
                    }
                    match best_urn {
                        Some((urn, urn_str)) => {
                            let structure = structure_from_marker_tags(&urn);
                            (urn_str, structure)
                        }
                        None => ("media:".to_string(), ContentStructure::ScalarOpaque),
                    }
                }
                _ => ("media:".to_string(), ContentStructure::ScalarOpaque),
            }
        }
        None => ("media:".to_string(), ContentStructure::ScalarOpaque),
    };

    Ok(ResolvedFile {
        path: path.to_path_buf(),
        media_urn,
        size_bytes,
        content_structure,
    })
}

// =============================================================================
// ASYNC CARTRIDGE-CONFIRMED DETECTION
// =============================================================================

/// Detect media type for a file with cartridge adapter confirmation.
///
/// This is the full detection flow:
/// 1. Extension lookup → candidate URNs
/// 2. Find registered adapters for those candidates
/// 3. Invoke adapter-selection cap on each matched cartridge
/// 4. Select most specific confirmed URN
///
/// Fails hard if no adapters are registered, if all cartridges return no match,
/// or if the response is invalid.
pub async fn detect_file_confirmed(
    path: &Path,
    adapter_registry: &MediaAdapterRegistry,
    invoker: &dyn CartridgeAdapterInvoker,
) -> Result<ResolvedFile, InputResolverError> {
    let metadata = fs::metadata(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            InputResolverError::NotFound(path.to_path_buf())
        } else if e.kind() == std::io::ErrorKind::PermissionDenied {
            InputResolverError::PermissionDenied(path.to_path_buf())
        } else {
            InputResolverError::IoError {
                path: path.to_path_buf(),
                error: e,
            }
        }
    })?;

    let size_bytes = metadata.len();

    // Step 1: Extension lookup
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_lowercase());

    let ext_str = ext.as_deref().unwrap_or("");

    // Step 2: Find adapters
    let adapters = adapter_registry.find_adapters_for_extension(ext_str);

    if adapters.is_empty() {
        return Err(InputResolverError::InspectionFailed {
            path: path.to_path_buf(),
            reason: format!(
                "No content-inspection adapter registered for extension '.{}'. \
                 A cartridge must register an adapter for this file type.",
                ext_str,
            ),
        });
    }

    // Step 3: Invoke each cartridge's adapter-selection cap
    let mut all_returned_urns: Vec<(String, String)> = Vec::new(); // (urn_str, cartridge_id)

    for (cartridge_id, _adapter_urn) in &adapters {
        let result = invoker.invoke_adapter_selection(cartridge_id, path).await?;

        if let Some(media_urns) = result {
            for urn_str in media_urns {
                all_returned_urns.push((urn_str, cartridge_id.clone()));
            }
        }
    }

    // Step 4: All cartridges returned empty END — none matched
    if all_returned_urns.is_empty() {
        let adapter_names: Vec<&str> = adapters.iter().map(|(id, _)| id.as_str()).collect();
        return Err(InputResolverError::InspectionFailed {
            path: path.to_path_buf(),
            reason: format!(
                "All registered adapters returned no match (extension '.{}'). \
                 Adapters consulted: {:?}. The file content does not match any registered media type.",
                ext_str,
                adapter_names,
            ),
        });
    }

    // Step 5: Validate and parse returned URNs
    let mut parsed_urns: Vec<(MediaUrn, String, String)> = Vec::new(); // (urn, urn_str, cartridge_id)

    for (urn_str, cartridge_id) in &all_returned_urns {
        let urn =
            MediaUrn::from_string(urn_str).map_err(|e| InputResolverError::InspectionFailed {
                path: path.to_path_buf(),
                reason: format!(
                    "Cartridge '{}' returned invalid media URN '{}': {}",
                    cartridge_id, urn_str, e
                ),
            })?;
        parsed_urns.push((urn, urn_str.clone(), cartridge_id.clone()));
    }

    // Step 6: Select by specificity
    let (best_idx, _) = parsed_urns
        .iter()
        .enumerate()
        .max_by_key(|(_, (urn, _, _))| urn.specificity())
        .unwrap(); // parsed_urns is non-empty (checked above)

    // Check for ties at the same specificity
    let best_specificity = parsed_urns[best_idx].0.specificity();
    let ties: Vec<&(MediaUrn, String, String)> = parsed_urns
        .iter()
        .filter(|(urn, _, _)| urn.specificity() == best_specificity)
        .collect();

    if ties.len() > 1 {
        // Check if one conforms to the other (which would make it not a real tie)
        let mut real_ties: Vec<&(MediaUrn, String, String)> = Vec::new();
        for tie in &ties {
            let dominated = ties.iter().any(|other| {
                std::ptr::eq(*tie, *other) == false && tie.0.conforms_to(&other.0).unwrap_or(false)
            });
            if !dominated {
                real_ties.push(tie);
            }
        }

        if real_ties.len() > 1 {
            let tie_descs: Vec<String> = real_ties
                .iter()
                .map(|(_, urn_str, cid)| format!("'{}' (from cartridge '{}')", urn_str, cid))
                .collect();
            return Err(InputResolverError::InspectionFailed {
                path: path.to_path_buf(),
                reason: format!(
                    "Ambiguous adapter selection: multiple adapters returned URNs \
                     at the same specificity level with no conformance relationship: {}. \
                     This indicates a registration conflict that should have been caught \
                     at cap group registration time.",
                    tie_descs.join(", "),
                ),
            });
        }
    }

    let (selected_urn, selected_urn_str, _) = &parsed_urns[best_idx];
    let content_structure = structure_from_marker_tags(selected_urn);

    Ok(ResolvedFile {
        path: path.to_path_buf(),
        media_urn: selected_urn_str.clone(),
        size_bytes,
        content_structure,
    })
}

/// Resolve multiple input items with cartridge-confirmed detection.
pub async fn resolve_inputs_confirmed(
    items: Vec<InputItem>,
    adapter_registry: &MediaAdapterRegistry,
    invoker: &dyn CartridgeAdapterInvoker,
) -> Result<ResolvedInputSet, InputResolverError> {
    let paths = path_resolver::resolve_items(&items)?;

    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        let resolved = detect_file_confirmed(&path, adapter_registry, invoker).await?;
        files.push(resolved);
    }

    if files.is_empty() {
        return Err(InputResolverError::NoFilesResolved);
    }

    Ok(ResolvedInputSet::new(files))
}

// =============================================================================
// HELPERS
// =============================================================================

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
    use std::io::Write;
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
    /// resolver tests reference (`pdf`, `txt`, `json`, `model-spec`).
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
            urn: "media:fmt=json;record".to_string(),
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
            urn: "media:enc=utf-8;ext=txt;list".to_string(),
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

        // Model-spec is a value-type URN with no file extension. The
        // discrimination tests pass the bare `media:enc=utf-8;model-spec`
        // URN explicitly as a candidate; the validation pattern
        // matches the canonical `family:model:variant` shape so plain
        // prose is filtered out.
        registry.insert_cached_media_def_for_test(StoredMediaDef {
            dims: Vec::new(),
            version: 0,
            urn: "media:enc=utf-8;model-spec".to_string(),
            media_type: "text/plain".to_string(),
            title: "Model spec".to_string(),
            profile_uri: None,
            schema: None,
            description: None,
            documentation: None,
            validation: Some(crate::media::spec::MediaValidation {
                // `scheme:rest` where `rest` is any non-empty
                // run of non-whitespace characters. Matches forms
                // like `hf:user/repo`, `hf:user/repo:variant`, and
                // `local:./path`. Rejects plain prose with internal
                // whitespace.
                pattern: Some(r"^[A-Za-z0-9._-]+:\S+$".to_string()),
                ..Default::default()
            }),
            metadata: None,
            extensions: Vec::new(),
        });
    }

    // TEST1090: 1 file → is_sequence=false
    #[test]
    fn test1090_single_file_scalar() {
        let (registry, _temp) = create_test_fabric_registry();
        let dir = create_test_dir();
        let path = create_file(&dir, "doc.pdf", b"%PDF-1.4");

        let result = resolve_paths(&[path.to_str().unwrap()], &registry).unwrap();

        assert_eq!(result.files.len(), 1);
        assert!(!result.is_sequence, "single file must be is_sequence=false");
    }

    // TEST1092: 2 files → is_sequence=true
    #[test]
    fn test1092_two_files() {
        let (registry, _temp) = create_test_fabric_registry();
        let dir = create_test_dir();
        let path1 = create_file(&dir, "a.pdf", b"%PDF-1.4");
        let path2 = create_file(&dir, "b.pdf", b"%PDF-1.5");

        let result = resolve_paths(
            &[path1.to_str().unwrap(), path2.to_str().unwrap()],
            &registry,
        )
        .unwrap();

        assert_eq!(result.files.len(), 2);
        assert!(
            result.is_sequence,
            "multiple files must be is_sequence=true"
        );
    }

    // TEST1093: 1 dir with 1 file → is_sequence=false
    #[test]
    fn test1093_dir_single_file() {
        let (registry, _temp) = create_test_fabric_registry();
        let dir = create_test_dir();
        create_file(&dir, "only.pdf", b"%PDF-1.4");

        let result = resolve_paths(&[dir.path().to_str().unwrap()], &registry).unwrap();

        assert_eq!(result.files.len(), 1);
        assert!(
            !result.is_sequence,
            "directory with single file must be is_sequence=false"
        );
    }

    // TEST1094: 1 dir with 3 files → is_sequence=true
    #[test]
    fn test1094_dir_multiple_files() {
        let (registry, _temp) = create_test_fabric_registry();
        let dir = create_test_dir();
        create_file(&dir, "a.txt", b"hello");
        create_file(&dir, "b.txt", b"world");
        create_file(&dir, "c.txt", b"test");

        let result = resolve_paths(&[dir.path().to_str().unwrap()], &registry).unwrap();

        assert_eq!(result.files.len(), 3);
        assert!(
            result.is_sequence,
            "directory with multiple files must be is_sequence=true"
        );
    }

    // TEST977: OS files excluded in resolve_paths
    #[test]
    fn test977_os_files_excluded_integration() {
        let (registry, _temp) = create_test_fabric_registry();
        let dir = create_test_dir();
        create_file(&dir, ".DS_Store", b"");
        create_file(&dir, "real.txt", b"content");

        let result = resolve_paths(&[dir.path().to_str().unwrap()], &registry).unwrap();

        assert_eq!(result.files.len(), 1);
        assert!(result.files[0].path.to_str().unwrap().contains("real.txt"));
    }

    // TEST1098: Extension-based detection picks up pdf tag for .pdf files
    #[test]
    fn test1098_extension_based_pdf() {
        let (registry, _temp) = create_test_fabric_registry();
        let dir = create_test_dir();
        let path = create_file(&dir, "doc.pdf", b"%PDF-1.4");

        let resolved = detect_file_by_extension_with_registry(&path, &registry).unwrap();
        let urn = MediaUrn::from_string(&resolved.media_urn).unwrap();
        assert!(
            urn.has_tag("ext", "pdf"),
            "PDF extension must produce URN with ext=pdf tag, got: {}",
            resolved.media_urn
        );
    }

    // Discrimination Tests (kept — they test validation logic, not adapter detection)
    //
    // TEST1235 was removed: it tried to assert that "model-spec URNs in
    // the txt-extension list don't survive plain-text content". But
    // `media:model-spec` is a value-type URN with no file extension —
    // it's never in the txt extension list to begin with — so the
    // assertion was vacuously true and the test had no signal.
    // Test1236 carries the real discrimination assertion (explicit
    // candidate list, validation regex eliminates prose).

    // TEST1236: Colon-delimited model spec text survives TXT candidate discrimination.
    // TEST1236: Discrimination matches a candidate's validation
    // pattern against the file content. media:model-spec is a value
    // type with no associated file extension, so it does NOT appear
    // among txt candidates. When passed in explicitly as a candidate,
    // content that matches its `^(scheme):\S+$` regex must survive;
    // content that doesn't (plain prose with whitespace) must be
    // filtered out.
    #[test]
    fn test1236_disc_2_model_spec_validation_pattern_filters_content() {
        let (registry, _temp) = create_test_fabric_registry();
        let candidates = vec!["media:enc=utf-8;model-spec".to_string()];

        // Spec-shaped content survives the regex filter.
        let survivors = discriminate_candidates_by_validation(
            b"hf:MaziyarPanahi/Mistral-7B-Instruct-v0.3-GGUF",
            &candidates,
            &registry,
            "media:enc=utf-8",
        );
        assert!(
            survivors.iter().any(|u| u == "media:enc=utf-8;model-spec"),
            "spec-shaped content must survive, got: {:?}",
            survivors
        );

        // Plain prose with internal whitespace is rejected by the same regex.
        let survivors_prose = discriminate_candidates_by_validation(
            b"this is not a model spec",
            &candidates,
            &registry,
            "media:enc=utf-8",
        );
        assert!(
            !survivors_prose
                .iter()
                .any(|u| u == "media:enc=utf-8;model-spec"),
            "prose must NOT survive, got: {:?}",
            survivors_prose
        );
    }

    // TEST1237: Empty candidates → empty result
    #[test]
    fn test1237_disc_5_empty_candidates() {
        let (registry, _temp) = create_test_fabric_registry();
        let survivors =
            discriminate_candidates_by_validation(b"anything", &[], &registry, "media:");
        assert!(survivors.is_empty());
    }

    // TEST1238: Unknown URN survives discrimination
    #[test]
    fn test1238_disc_6_unknown_urn_survives() {
        let (registry, _temp) = create_test_fabric_registry();
        let candidates = vec!["media:nonexistent;fake".to_string()];
        let survivors =
            discriminate_candidates_by_validation(b"anything", &candidates, &registry, "media:");
        assert_eq!(
            survivors, candidates,
            "Unknown URN should survive — no spec to eliminate it"
        );
    }

    // TEST1288: structure_from_marker_tags correctly maps tag combinations to ContentStructure
    #[test]
    fn test1288_structure_from_marker_tags() {
        let scalar_opaque = MediaUrn::from_string("media:document;ext=pdf").unwrap();
        assert_eq!(
            structure_from_marker_tags(&scalar_opaque),
            ContentStructure::ScalarOpaque
        );

        let scalar_record = MediaUrn::from_string("media:fmt=json;record").unwrap();
        assert_eq!(
            structure_from_marker_tags(&scalar_record),
            ContentStructure::ScalarRecord
        );

        let list_opaque = MediaUrn::from_string("media:enc=utf-8;list").unwrap();
        assert_eq!(
            structure_from_marker_tags(&list_opaque),
            ContentStructure::ListOpaque
        );

        let list_record = MediaUrn::from_string("media:fmt=json;list;record").unwrap();
        assert_eq!(
            structure_from_marker_tags(&list_record),
            ContentStructure::ListRecord
        );
    }

    // =========================================================================
    // Async confirmed detection tests (with mock invoker)
    // =========================================================================

    /// Mock invoker that returns predefined media URNs for any cartridge
    struct MockInvoker {
        response: Option<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl CartridgeAdapterInvoker for MockInvoker {
        async fn invoke_adapter_selection(
            &self,
            _cartridge_id: &str,
            _file_path: &Path,
        ) -> Result<Option<Vec<String>>, InputResolverError> {
            Ok(self.response.clone())
        }
    }

    // TEST1139: resolve_inputs_confirmed delegates to detect_file_confirmed and returns the
    // resolved URN for each file. A mock invoker returning a single URN must propagate through
    // to the ResolvedInputSet.
    #[tokio::test]
    async fn test1139_resolve_inputs_confirmed_delegates_to_detect_file_confirmed() {
        let dir = create_test_dir();
        let path = create_file(&dir, "data.json", br#"{"key":"value"}"#);

        let (fabric_registry, _temp) = create_test_fabric_registry();
        let mut adapter_registry = MediaAdapterRegistry::new(fabric_registry);
        adapter_registry
            .register_cap_group(
                "test-group",
                &["media:fmt=json".to_string()],
                "test-cartridge",
            )
            .unwrap();

        let invoker = MockInvoker {
            response: Some(vec!["media:fmt=json;record".to_string()]),
        };

        let result =
            resolve_inputs_confirmed(vec![InputItem::File(path)], &adapter_registry, &invoker)
                .await
                .expect("resolve_inputs_confirmed must succeed when adapter returns a URN");

        assert_eq!(result.files.len(), 1);
        assert_eq!(
            result.files[0].media_urn, "media:fmt=json;record",
            "resolved URN must match what the adapter returned"
        );
    }

    // TEST1285: detect_file_confirmed fails when no adapters are registered for the extension
    #[tokio::test]
    async fn test1285_confirmed_no_adapters_fails() {
        let dir = create_test_dir();
        let path = create_file(&dir, "data.json", br#"{"key": "value"}"#);

        let (fabric_registry, _temp) = create_test_fabric_registry();
        let adapter_registry = MediaAdapterRegistry::new(fabric_registry);
        let invoker = MockInvoker { response: None };

        let result = detect_file_confirmed(&path, &adapter_registry, &invoker).await;
        assert!(
            result.is_err(),
            "Must fail when no adapters are registered for the extension"
        );
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("No content-inspection adapter"),
            "Error must mention missing adapter, got: {}",
            err_msg
        );
    }

    // TEST1286: detect_file_confirmed succeeds when adapter returns URNs
    #[tokio::test]
    async fn test1286_confirmed_adapter_returns_urns() {
        let dir = create_test_dir();
        let path = create_file(&dir, "data.json", br#"{"key": "value"}"#);

        let (fabric_registry, _temp) = create_test_fabric_registry();
        let mut adapter_registry = MediaAdapterRegistry::new(fabric_registry);

        // Register an adapter for media:fmt=json
        adapter_registry
            .register_cap_group(
                "test-group",
                &["media:fmt=json".to_string()],
                "test-cartridge",
            )
            .unwrap();

        let invoker = MockInvoker {
            response: Some(vec!["media:fmt=json;record".to_string()]),
        };

        let result = detect_file_confirmed(&path, &adapter_registry, &invoker).await;
        assert!(
            result.is_ok(),
            "Must succeed when adapter returns URNs: {:?}",
            result.err()
        );

        let resolved = result.unwrap();
        assert!(
            resolved.media_urn.contains("json"),
            "Resolved URN must contain json, got: {}",
            resolved.media_urn
        );
        assert_eq!(resolved.content_structure, ContentStructure::ScalarRecord);
    }

    // TEST1287: detect_file_confirmed fails when all adapters return empty END (no match)
    #[tokio::test]
    async fn test1287_confirmed_all_adapters_no_match() {
        let dir = create_test_dir();
        let path = create_file(&dir, "data.json", br#"not json"#);

        let (fabric_registry, _temp) = create_test_fabric_registry();
        let mut adapter_registry = MediaAdapterRegistry::new(fabric_registry);

        adapter_registry
            .register_cap_group(
                "test-group",
                &["media:fmt=json".to_string()],
                "test-cartridge",
            )
            .unwrap();

        // Invoker returns None (empty END — no match)
        let invoker = MockInvoker { response: None };

        let result = detect_file_confirmed(&path, &adapter_registry, &invoker).await;
        assert!(
            result.is_err(),
            "Must fail when all adapters return no match"
        );
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("returned no match"),
            "Error must mention no match, got: {}",
            err_msg
        );
    }
}
