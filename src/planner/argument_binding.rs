//! Argument Binding for Cap Execution
//!
//! This module provides the file presentation layer and argument binding system.
//!
//! Design principles:
//! 1. **No domain leakage**: Caps see FILES only, never listings/chips/blocks
//! 2. **Pure data flow**: Caps receive only declared outputs from predecessors
//! 3. **Explicit sources**: Arguments come from explicit bindings, no ambient context

use super::cardinality::InputCardinality;
use super::PlannerError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A file presented to a cap for processing.
///
/// This is the uniform interface caps see - they never see listings, chips, or blocks directly.
/// Everything is converted to CapInputFile before being passed to a cap.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapInputFile {
    /// Actual filesystem path to the file
    pub file_path: String,
    /// Media URN describing the file type (e.g., "media:ext=pdf")
    pub media_urn: String,
    /// Optional file metadata
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub metadata: Option<CapFileMetadata>,
    /// Original source entity ID (for traceability, not passed to cap)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub source_id: Option<String>,
    /// Type of source entity
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub source_type: Option<SourceEntityType>,
    /// Tracked file ID for file lifecycle management with cartridges.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub tracked_file_id: Option<String>,
    /// Security bookmark for accessing the file from the sandboxed cartridge.
    /// Runtime-only — never serialized (macOS sandbox bookmark, opaque binary).
    #[serde(skip)]
    pub security_bookmark: Option<Vec<u8>>,
    /// Original file path before container path resolution.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub original_path: Option<String>,
    /// LIVE-SOURCE reference urn (13.2 §Reference Media, live family). When
    /// set, this input is a capture device, not a file: `file_path` MUST be
    /// empty, `media_urn` carries the feed's CONTENT urn (the slot-binding
    /// and planning anchor, from the fabric live def's `metadata.content`),
    /// and execution forwards the reference + selector to the first
    /// consuming cap's cartridge — or bridges host-captured frames on
    /// sandboxed platforms. The interpreter enforces the exclusivity.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub live_reference: Option<String>,
    /// The live selector record (JSON) accompanying `live_reference`.
    /// Empty/absent = the all-defaults selector.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub live_selector: Option<String>,
}

/// Metadata about a cap input file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapFileMetadata {
    /// File name (without path)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    /// File size in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// MIME type if known
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    /// Additional metadata as JSON
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra: Option<serde_json::Value>,
}

/// Type of source entity (for internal tracking, not exposed to caps)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceEntityType {
    Listing,
    Chip,
    Block,
    CapOutput,
    Temporary,
}

impl CapInputFile {
    pub fn new(file_path: String, media_urn: String) -> Self {
        Self {
            file_path,
            media_urn,
            metadata: None,
            source_id: None,
            source_type: None,
            tracked_file_id: None,
            security_bookmark: None,
            original_path: None,
            live_reference: None,
            live_selector: None,
        }
    }

    /// A LIVE-SOURCE input: `reference_urn` names the device family, the
    /// fabric-paired `content_urn` is what the feed delivers (the slot
    /// binding/planning anchor), and `selector` configures the capture
    /// ("" = all defaults). No file is involved.
    pub fn live_source(reference_urn: &str, content_urn: &str, selector: &str) -> Self {
        Self {
            file_path: String::new(),
            media_urn: content_urn.to_string(),
            metadata: None,
            source_id: None,
            source_type: None,
            tracked_file_id: None,
            security_bookmark: None,
            original_path: None,
            live_reference: Some(reference_urn.to_string()),
            live_selector: Some(selector.to_string()),
        }
    }

    pub fn from_listing(listing_id: &str, file_path: &str, media_urn: &str) -> Self {
        Self {
            file_path: file_path.to_string(),
            media_urn: media_urn.to_string(),
            metadata: None,
            source_id: Some(listing_id.to_string()),
            source_type: Some(SourceEntityType::Listing),
            tracked_file_id: None,
            security_bookmark: None,
            original_path: None,
            live_reference: None,
            live_selector: None,
        }
    }

    pub fn from_chip(chip_id: &str, cache_path: &str, media_urn: &str) -> Self {
        Self {
            file_path: cache_path.to_string(),
            media_urn: media_urn.to_string(),
            metadata: None,
            source_id: Some(chip_id.to_string()),
            source_type: Some(SourceEntityType::Chip),
            tracked_file_id: None,
            security_bookmark: None,
            original_path: None,
            live_reference: None,
            live_selector: None,
        }
    }

    pub fn from_cap_output(output_path: String, media_urn: String) -> Self {
        Self {
            file_path: output_path,
            media_urn,
            metadata: None,
            source_id: None,
            source_type: Some(SourceEntityType::CapOutput),
            tracked_file_id: None,
            security_bookmark: None,
            original_path: None,
            live_reference: None,
            live_selector: None,
        }
    }

    pub fn with_metadata(mut self, metadata: CapFileMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn with_file_reference(
        mut self,
        tracked_file_id: String,
        security_bookmark: Vec<u8>,
        original_path: String,
    ) -> Self {
        self.tracked_file_id = Some(tracked_file_id);
        self.security_bookmark = Some(security_bookmark);
        self.original_path = Some(original_path);
        self
    }

    pub fn filename(&self) -> Option<&str> {
        // Use original_path (pre-container) when available — container paths
        // have UUID prefixes that must never leak into user-facing output names.
        let path = self.original_path.as_deref().unwrap_or(&self.file_path);
        std::path::Path::new(path)
            .file_name()
            .and_then(|s| s.to_str())
    }

    pub fn has_file_reference(&self) -> bool {
        self.tracked_file_id.is_some() && self.security_bookmark.is_some()
    }
}

/// How to resolve an argument value for cap execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ArgumentBinding {
    InputFile {
        index: usize,
    },
    InputFilePath,
    InputMediaUrn,
    PreviousOutput {
        node_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        output_field: Option<String>,
    },
    CapDefault,
    CapSetting {
        setting_urn: String,
    },
    Literal {
        value: serde_json::Value,
    },
    Slot {
        name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        schema: Option<serde_json::Value>,
    },
    PlanMetadata {
        key: String,
    },
}

impl ArgumentBinding {
    pub fn literal_string(s: &str) -> Self {
        Self::Literal {
            value: serde_json::Value::String(s.to_string()),
        }
    }

    pub fn literal_number(n: i64) -> Self {
        Self::Literal {
            value: serde_json::Value::Number(n.into()),
        }
    }

    pub fn literal_bool(b: bool) -> Self {
        Self::Literal {
            value: serde_json::Value::Bool(b),
        }
    }

    pub fn requires_input(&self) -> bool {
        matches!(self, Self::Slot { .. })
    }

    pub fn references_previous(&self) -> bool {
        matches!(self, Self::PreviousOutput { .. })
    }
}

/// A resolved argument ready for cap execution.
#[derive(Debug, Clone)]
pub struct ResolvedArgument {
    pub name: String,
    pub value: Vec<u8>,
    pub source: ArgumentSource,
}

/// Source of a resolved argument value
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArgumentSource {
    InputFile,
    PreviousOutput,
    CapDefault,
    CapSetting,
    Literal,
    Slot,
    PlanMetadata,
}

/// Context for resolving argument bindings during execution.
#[derive(Debug)]
pub struct ArgumentResolutionContext<'a> {
    pub input_files: &'a [CapInputFile],
    pub current_file_index: usize,
    pub previous_outputs: &'a HashMap<String, serde_json::Value>,
    pub plan_metadata: Option<&'a HashMap<String, serde_json::Value>>,
    pub cap_settings: Option<&'a HashMap<String, HashMap<String, serde_json::Value>>>,
    pub slot_values: Option<&'a HashMap<String, Vec<u8>>>,
}

/// Static empty HashMap for use in context creation
static EMPTY_OUTPUTS: std::sync::LazyLock<HashMap<String, serde_json::Value>> =
    std::sync::LazyLock::new(HashMap::new);

impl<'a> ArgumentResolutionContext<'a> {
    pub fn with_inputs(input_files: &'a [CapInputFile]) -> Self {
        Self {
            input_files,
            current_file_index: 0,
            previous_outputs: &EMPTY_OUTPUTS,
            plan_metadata: None,
            cap_settings: None,
            slot_values: None,
        }
    }

    pub fn current_file(&self) -> Option<&CapInputFile> {
        self.input_files.get(self.current_file_index)
    }
}

/// Convert a `serde_json::Value` to the raw bytes the receiving
/// cartridge will see on its arg stream.
///
/// The wire contract for an arg stream is "bytes of the typed
/// media URN" — for a `media:enc=utf-8` arg, plain UTF-8 text. A
/// scalar JSON value (string / number / bool / null) is encoded
/// as its lexical wire form, matching exactly what the same value
/// typed at a CLI flag would produce. Composite values (object,
/// array) ARE JSON on the wire by design and pass through
/// `serde_json::to_vec`. Mirrors the dispatch in
/// `bifaci::cartridge_runtime::extract_arg_value`.
fn json_value_to_bytes(value: &serde_json::Value) -> Vec<u8> {
    match value {
        serde_json::Value::String(s) => s.as_bytes().to_vec(),
        serde_json::Value::Bool(b) => {
            if *b {
                b"true".to_vec()
            } else {
                b"false".to_vec()
            }
        }
        serde_json::Value::Number(n) => n.to_string().into_bytes(),
        serde_json::Value::Null => Vec::new(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            serde_json::to_vec(value).unwrap_or_default()
        }
    }
}

/// Resolve an argument binding to raw bytes.
///
/// `cap_urn` is used for CapSetting lookups and error messages.
/// `node_id` is used for slot_values key construction (e.g., "step_0:media:width").
pub fn resolve_binding(
    binding: &ArgumentBinding,
    context: &ArgumentResolutionContext,
    cap_urn: &str,
    node_id: &str,
    default_value: Option<&serde_json::Value>,
    is_required: bool,
) -> Result<Option<ResolvedArgument>, PlannerError> {
    let (value, source) = match binding {
        ArgumentBinding::InputFile { index } => {
            let file = context.input_files.get(*index).ok_or_else(|| {
                PlannerError::Internal(format!(
                    "Input file index {} out of bounds (have {} files)",
                    index,
                    context.input_files.len()
                ))
            })?;
            (
                file.file_path.as_bytes().to_vec(),
                ArgumentSource::InputFile,
            )
        }

        ArgumentBinding::InputFilePath => {
            let file = context.current_file().ok_or_else(|| {
                PlannerError::Internal("No current input file available".to_string())
            })?;
            (
                file.file_path.as_bytes().to_vec(),
                ArgumentSource::InputFile,
            )
        }

        ArgumentBinding::InputMediaUrn => {
            let file = context.current_file().ok_or_else(|| {
                PlannerError::Internal("No current input file available".to_string())
            })?;
            (
                file.media_urn.as_bytes().to_vec(),
                ArgumentSource::InputFile,
            )
        }

        ArgumentBinding::PreviousOutput {
            node_id,
            output_field,
        } => {
            let output = context.previous_outputs.get(node_id).ok_or_else(|| {
                PlannerError::Internal(format!("No output from node '{}'", node_id))
            })?;

            let json_value = if let Some(field) = output_field {
                output.get(field).ok_or_else(|| {
                    PlannerError::Internal(format!(
                        "Field '{}' not found in output from node '{}'",
                        field, node_id
                    ))
                })?
            } else {
                output
            };

            (
                json_value_to_bytes(json_value),
                ArgumentSource::PreviousOutput,
            )
        }

        ArgumentBinding::CapDefault => {
            let value = default_value.ok_or_else(|| {
                PlannerError::Internal(format!(
                    "Cap '{}' has no default value for argument",
                    cap_urn
                ))
            })?;
            (json_value_to_bytes(value), ArgumentSource::CapDefault)
        }

        ArgumentBinding::CapSetting { setting_urn } => {
            let cap_settings = context
                .cap_settings
                .ok_or_else(|| PlannerError::Internal("No cap settings available".to_string()))?;

            let settings = cap_settings.get(cap_urn).ok_or_else(|| {
                PlannerError::Internal(format!("No settings for cap '{}'", cap_urn))
            })?;

            let value = settings.get(setting_urn).ok_or_else(|| {
                PlannerError::Internal(format!(
                    "Setting '{}' not found for cap '{}'",
                    setting_urn, cap_urn
                ))
            })?;

            (json_value_to_bytes(value), ArgumentSource::CapSetting)
        }

        ArgumentBinding::Literal { value } => (json_value_to_bytes(value), ArgumentSource::Literal),

        ArgumentBinding::Slot { name, .. } => {
            let key = format!("{}:{}", node_id, name);

            if let Some(slot_values) = context.slot_values {
                if let Some(bytes) = slot_values.get(&key) {
                    return Ok(Some(ResolvedArgument {
                        name: String::new(),
                        value: bytes.clone(),
                        source: ArgumentSource::Slot,
                    }));
                }
            }

            if let Some(cap_settings) = context.cap_settings {
                if let Some(settings) = cap_settings.get(cap_urn) {
                    if let Some(value) = settings.get(name) {
                        return Ok(Some(ResolvedArgument {
                            name: String::new(),
                            value: json_value_to_bytes(value),
                            source: ArgumentSource::CapSetting,
                        }));
                    }
                }
            }

            if let Some(default) = default_value {
                return Ok(Some(ResolvedArgument {
                    name: String::new(),
                    value: json_value_to_bytes(default),
                    source: ArgumentSource::CapDefault,
                }));
            }

            if is_required {
                return Err(PlannerError::Internal(format!(
                    "Missing required argument '{}': no value in slot_values (key: {}), settings, or default",
                    name, key
                )));
            } else {
                return Ok(None);
            }
        }

        ArgumentBinding::PlanMetadata { key } => {
            let metadata = context
                .plan_metadata
                .ok_or_else(|| PlannerError::Internal("No plan metadata available".to_string()))?;

            let value = metadata.get(key).ok_or_else(|| {
                PlannerError::Internal(format!("Key '{}' not found in plan metadata", key))
            })?;

            (json_value_to_bytes(value), ArgumentSource::PlanMetadata)
        }
    };

    Ok(Some(ResolvedArgument {
        name: String::new(),
        value,
        source,
    }))
}

/// Collection of argument bindings for a cap node
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ArgumentBindings {
    pub bindings: HashMap<String, ArgumentBinding>,
}

impl ArgumentBindings {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, name: String, binding: ArgumentBinding) {
        self.bindings.insert(name, binding);
    }

    pub fn add_file_path(&mut self, arg_name: &str) {
        self.bindings
            .insert(arg_name.to_string(), ArgumentBinding::InputFilePath);
    }

    pub fn add_literal(&mut self, arg_name: &str, value: serde_json::Value) {
        self.bindings
            .insert(arg_name.to_string(), ArgumentBinding::Literal { value });
    }

    pub fn has_unresolved_slots(&self) -> bool {
        self.bindings.values().any(|b| b.requires_input())
    }

    pub fn get_unresolved_slots(&self) -> Vec<&str> {
        self.bindings
            .iter()
            .filter_map(|(name, b)| {
                if b.requires_input() {
                    Some(name.as_str())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn resolve_all(
        &self,
        context: &ArgumentResolutionContext,
        cap_urn: &str,
        node_id: &str,
        cap_defaults: Option<&HashMap<String, serde_json::Value>>,
        arg_required: Option<&HashMap<String, bool>>,
    ) -> Result<Vec<ResolvedArgument>, PlannerError> {
        let mut resolved = Vec::new();

        for (name, binding) in &self.bindings {
            let default = cap_defaults.and_then(|d| d.get(name));
            let is_required = arg_required
                .and_then(|r| r.get(name))
                .copied()
                .unwrap_or(false);

            if let Some(mut arg) =
                resolve_binding(binding, context, cap_urn, node_id, default, is_required)?
            {
                arg.name = name.clone();
                resolved.push(arg);
            }
        }

        Ok(resolved)
    }
}

/// Input specification for machine execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrandInput {
    pub files: Vec<CapInputFile>,
    pub expected_media_urn: String,
    pub cardinality: InputCardinality,
}

impl StrandInput {
    pub fn single(file: CapInputFile) -> Self {
        let media_urn = file.media_urn.clone();
        Self {
            files: vec![file],
            expected_media_urn: media_urn,
            cardinality: InputCardinality::Single,
        }
    }

    pub fn sequence(files: Vec<CapInputFile>, media_urn: String) -> Self {
        Self {
            files,
            expected_media_urn: media_urn,
            cardinality: InputCardinality::Sequence,
        }
    }

    pub fn is_valid(&self) -> bool {
        match self.cardinality {
            InputCardinality::Single => self.files.len() == 1,
            InputCardinality::Sequence => !self.files.is_empty(),
            InputCardinality::AtLeastOne => !self.files.is_empty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // TEST7107: the live-source CapInputFile contract — `live_source()`
    // builds a device input (empty file_path, CONTENT urn as media_urn,
    // reference + selector set), and BOTH live fields survive the serde
    // round-trip the task payload performs (a run resumed from persistence
    // must still know its input is a device, or the interpreter would try
    // to read an empty path as a file).
    #[test]
    fn test7107_live_source_input_file_contract_and_roundtrip() {
        let live = CapInputFile::live_source(
            "media:audio;live;microphone",
            "media:audio-frames;pcm",
            r#"{"stop":{"duration_ms":5000}}"#,
        );
        assert_eq!(live.file_path, "", "a device input has NO file path");
        assert_eq!(live.media_urn, "media:audio-frames;pcm", "media_urn is the CONTENT urn");
        assert_eq!(live.live_reference.as_deref(), Some("media:audio;live;microphone"));
        assert_eq!(
            live.live_selector.as_deref(),
            Some(r#"{"stop":{"duration_ms":5000}}"#)
        );

        let json = serde_json::to_string(&live).expect("serializes");
        let back: CapInputFile = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back.live_reference, live.live_reference, "reference survives persistence");
        assert_eq!(back.live_selector, live.live_selector, "selector survives persistence");
        assert_eq!(back.file_path, "");

        // A plain file input carries NO live fields — and omits them from
        // its serialized form entirely.
        let file = CapInputFile::new("/tmp/a.wav".to_string(), "media:audio;ext=wav".to_string());
        assert!(file.live_reference.is_none());
        let json = serde_json::to_string(&file).expect("serializes");
        assert!(
            !json.contains("live_reference"),
            "absent live fields are omitted from the wire form: {json}"
        );
    }

    // TEST957: Tests CapInputFile constructor creates file with correct path and media URN
    // Verifies new() initializes file_path, media_urn and leaves metadata/source_id as None
    #[test]
    fn test957_cap_input_file_new() {
        let file = CapInputFile::new("/path/to/file.pdf".to_string(), "media:ext=pdf".to_string());
        assert_eq!(file.file_path, "/path/to/file.pdf");
        assert_eq!(file.media_urn, "media:ext=pdf");
        assert!(file.metadata.is_none());
        assert!(file.source_id.is_none());
    }

    // TEST958: Tests CapInputFile from_listing sets source metadata correctly
    // Verifies from_listing() populates source_id and source_type as Listing
    #[test]
    fn test958_cap_input_file_from_listing() {
        let file = CapInputFile::from_listing("listing-123", "/path/to/file.pdf", "media:ext=pdf");
        assert_eq!(file.source_id, Some("listing-123".to_string()));
        assert_eq!(file.source_type, Some(SourceEntityType::Listing));
    }

    // TEST959: Tests CapInputFile extracts filename from full path correctly
    // Verifies filename() returns just the basename without directory path
    #[test]
    fn test959_cap_input_file_filename() {
        let file = CapInputFile::new(
            "/path/to/document.pdf".to_string(),
            "media:ext=pdf".to_string(),
        );
        assert_eq!(file.filename(), Some("document.pdf"));
    }

    // TEST960: Tests ArgumentBinding literal_string creates Literal variant with string value
    // Verifies literal_string() wraps string in JSON Value::String
    #[test]
    fn test960_argument_binding_literal_string() {
        let binding = ArgumentBinding::literal_string("test");
        if let ArgumentBinding::Literal { value } = binding {
            assert_eq!(value, serde_json::Value::String("test".to_string()));
        } else {
            panic!("Expected Literal binding");
        }
    }

    // TEST792: Tests ArgumentBinding requires_input distinguishes Slots from Literals
    // Verifies Slot returns true (needs user input) while Literal returns false
    #[test]
    fn test792_argument_binding_requires_input() {
        let slot = ArgumentBinding::Slot {
            name: "width".to_string(),
            schema: None,
        };
        assert!(slot.requires_input());
        let literal = ArgumentBinding::Literal { value: json!(100) };
        assert!(!literal.requires_input());
    }

    // TEST793: Tests ArgumentBinding PreviousOutput serializes/deserializes correctly
    // Verifies JSON round-trip preserves node_id and output_field values
    #[test]
    fn test793_argument_binding_serialization() {
        let binding = ArgumentBinding::PreviousOutput {
            node_id: "node_0".to_string(),
            output_field: Some("result_path".to_string()),
        };
        let json = serde_json::to_string(&binding).unwrap();
        assert!(json.contains("previous_output"));
        assert!(json.contains("node_0"));
        let deserialized: ArgumentBinding = serde_json::from_str(&json).unwrap();
        if let ArgumentBinding::PreviousOutput {
            node_id,
            output_field,
        } = deserialized
        {
            assert_eq!(node_id, "node_0");
            assert_eq!(output_field, Some("result_path".to_string()));
        } else {
            panic!("Expected PreviousOutput binding");
        }
    }

    // TEST794: Tests ArgumentBindings add_file_path adds InputFilePath binding
    // Verifies add_file_path() creates binding map entry with InputFilePath variant
    #[test]
    fn test794_argument_bindings_add_file_path() {
        let mut bindings = ArgumentBindings::new();
        bindings.add_file_path("input");
        assert!(bindings.bindings.contains_key("input"));
        assert!(matches!(
            bindings.bindings.get("input"),
            Some(ArgumentBinding::InputFilePath)
        ));
    }

    // TEST795: Tests ArgumentBindings identifies unresolved Slot bindings
    // Verifies has_unresolved_slots() and get_unresolved_slots() detect Slots needing values
    #[test]
    fn test795_argument_bindings_unresolved_slots() {
        let mut bindings = ArgumentBindings::new();
        bindings.add(
            "width".to_string(),
            ArgumentBinding::Slot {
                name: "width".to_string(),
                schema: None,
            },
        );
        bindings.add(
            "height".to_string(),
            ArgumentBinding::Literal { value: json!(100) },
        );
        assert!(bindings.has_unresolved_slots());
        assert_eq!(bindings.get_unresolved_slots(), vec!["width"]);
    }

    // TEST796: Tests resolve_binding resolves InputFilePath to current file path
    // Verifies InputFilePath binding resolves to file path bytes with InputFile source
    #[test]
    fn test796_resolve_input_file_path() {
        let files = vec![CapInputFile::new(
            "/path/to/file.pdf".to_string(),
            "media:ext=pdf".to_string(),
        )];
        let prev_outputs = HashMap::new();
        let context = ArgumentResolutionContext {
            input_files: &files,
            current_file_index: 0,
            previous_outputs: &prev_outputs,
            plan_metadata: None,
            cap_settings: None,
            slot_values: None,
        };
        let binding = ArgumentBinding::InputFilePath;
        let result = resolve_binding(&binding, &context, "cap:test", "step_0", None, true)
            .unwrap()
            .unwrap();
        assert_eq!(result.value, b"/path/to/file.pdf".to_vec());
        assert_eq!(result.source, ArgumentSource::InputFile);
    }

    // TEST797: Tests resolve_binding resolves Literal to JSON-encoded bytes
    // Verifies Literal binding serializes value to bytes with Literal source
    #[test]
    fn test797_resolve_literal() {
        let files = vec![];
        let prev_outputs = HashMap::new();
        let context = ArgumentResolutionContext {
            input_files: &files,
            current_file_index: 0,
            previous_outputs: &prev_outputs,
            plan_metadata: None,
            cap_settings: None,
            slot_values: None,
        };
        let binding = ArgumentBinding::Literal { value: json!(42) };
        let result = resolve_binding(&binding, &context, "cap:test", "step_0", None, true)
            .unwrap()
            .unwrap();
        assert_eq!(result.value, serde_json::to_vec(&json!(42)).unwrap());
        assert_eq!(result.source, ArgumentSource::Literal);
    }

    // TEST798: Tests resolve_binding extracts value from previous node output
    // Verifies PreviousOutput binding fetches field from earlier execution results
    #[test]
    fn test798_resolve_previous_output() {
        let files = vec![];
        let mut prev_outputs = HashMap::new();
        prev_outputs.insert(
            "node_0".to_string(),
            json!({"result_path": "/output/result.png"}),
        );
        let context = ArgumentResolutionContext {
            input_files: &files,
            current_file_index: 0,
            previous_outputs: &prev_outputs,
            plan_metadata: None,
            cap_settings: None,
            slot_values: None,
        };
        let binding = ArgumentBinding::PreviousOutput {
            node_id: "node_0".to_string(),
            output_field: Some("result_path".to_string()),
        };
        let result = resolve_binding(&binding, &context, "cap:test", "step_0", None, true)
            .unwrap()
            .unwrap();
        assert_eq!(result.value, b"/output/result.png".to_vec());
        assert_eq!(result.source, ArgumentSource::PreviousOutput);
    }

    // TEST799: Tests StrandInput single constructor creates valid Single cardinality input
    // Verifies single() wraps one file with Single cardinality and validates correctly
    #[test]
    fn test799_machine_input_single() {
        let file = CapInputFile::new("/path/to/file.pdf".to_string(), "media:ext=pdf".to_string());
        let input = StrandInput::single(file);
        assert_eq!(input.files.len(), 1);
        assert_eq!(input.cardinality, InputCardinality::Single);
        assert!(input.is_valid());
    }

    // TEST800: Tests StrandInput sequence constructor creates valid Sequence cardinality input
    // Verifies sequence() wraps multiple files with Sequence cardinality
    #[test]
    fn test800_machine_input_vector() {
        let files = vec![
            CapInputFile::new("/path/1.pdf".to_string(), "media:ext=pdf".to_string()),
            CapInputFile::new("/path/2.pdf".to_string(), "media:ext=pdf".to_string()),
        ];
        let input = StrandInput::sequence(files, "media:ext=pdf".to_string());
        assert_eq!(input.files.len(), 2);
        assert_eq!(input.cardinality, InputCardinality::Sequence);
        assert!(input.is_valid());
    }

    // TEST801: Tests CapInputFile deserializes from JSON with source metadata fields
    // Verifies JSON with source_id and source_type deserializes to CapInputFile correctly
    #[test]
    fn test801_cap_input_file_deserialization_from_dry_context() {
        let json_str = r#"[
            {
                "file_path": "/fixtures/aws_in_action.pdf",
                "media_urn": "media:ext=pdf",
                "source_id": "1b964d3b-f409-4f51-8684-884348ec2501",
                "source_type": "listing"
            }
        ]"#;
        let result: std::result::Result<Vec<CapInputFile>, _> = serde_json::from_str(json_str);
        assert!(
            result.is_ok(),
            "Deserialization should succeed: {:?}",
            result.err()
        );
        let files = result.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].source_type, Some(SourceEntityType::Listing));
    }

    // TEST802: Tests CapInputFile deserializes from compact JSON via serde_json::Value
    // Verifies deserialization through Value intermediate works correctly
    #[test]
    fn test802_cap_input_file_deserialization_via_value() {
        let json_str = r#"[{"file_path": "/path/to/file.pdf","media_urn": "media:ext=pdf","source_id": "abc123","source_type": "listing"}]"#;
        let value: serde_json::Value = serde_json::from_str(json_str).expect("Parse to Value");
        let result: std::result::Result<Vec<CapInputFile>, _> = serde_json::from_value(value);
        assert!(result.is_ok());
    }

    // TEST668: resolve_binding returns byte values when slot is populated with data
    #[test]
    fn test668_resolve_slot_with_populated_byte_slot_values() {
        let files = vec![];
        let prev_outputs = HashMap::new();
        let mut slot_values: HashMap<String, Vec<u8>> = HashMap::new();
        slot_values.insert("step_0:media:numeric;width".to_string(), b"800".to_vec());
        let context = ArgumentResolutionContext {
            input_files: &files,
            current_file_index: 0,
            previous_outputs: &prev_outputs,
            plan_metadata: None,
            cap_settings: None,
            slot_values: Some(&slot_values),
        };
        let binding = ArgumentBinding::Slot {
            name: "media:numeric;width".to_string(),
            schema: None,
        };
        let result = resolve_binding(
            &binding,
            &context,
            "cap:in=\"media:ext=pdf\";resize;out=\"media:ext=pdf\"",
            "step_0",
            None,
            true,
        )
        .unwrap()
        .unwrap();
        assert_eq!(result.value, b"800".to_vec());
        assert_eq!(result.source, ArgumentSource::Slot);
    }

    // TEST669: resolve_binding falls back to cap default value when slot has no data
    #[test]
    fn test669_resolve_slot_falls_back_to_default() {
        let files = vec![];
        let prev_outputs = HashMap::new();
        let context = ArgumentResolutionContext {
            input_files: &files,
            current_file_index: 0,
            previous_outputs: &prev_outputs,
            plan_metadata: None,
            cap_settings: None,
            slot_values: None,
        };
        let binding = ArgumentBinding::Slot {
            name: "media:numeric;quality".to_string(),
            schema: None,
        };
        let default = json!(85);
        let result = resolve_binding(
            &binding,
            &context,
            "cap:compress",
            "step_0",
            Some(&default),
            false,
        )
        .unwrap()
        .unwrap();
        assert_eq!(result.value, serde_json::to_vec(&json!(85)).unwrap());
        assert_eq!(result.source, ArgumentSource::CapDefault);
    }

    // TEST670: resolve_binding returns error when required slot has no value and no default
    #[test]
    fn test670_resolve_required_slot_no_value_returns_err() {
        let files = vec![];
        let prev_outputs = HashMap::new();
        let context = ArgumentResolutionContext {
            input_files: &files,
            current_file_index: 0,
            previous_outputs: &prev_outputs,
            plan_metadata: None,
            cap_settings: None,
            slot_values: None,
        };
        let binding = ArgumentBinding::Slot {
            name: "media:enc=utf-8;question".to_string(),
            schema: None,
        };
        let result = resolve_binding(&binding, &context, "cap:generate", "step_0", None, true);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("media:enc=utf-8;question"));
    }

    // TEST671: resolve_binding returns None when optional slot has no value and no default
    #[test]
    fn test671_resolve_optional_slot_no_value_returns_none() {
        let files = vec![];
        let prev_outputs = HashMap::new();
        let context = ArgumentResolutionContext {
            input_files: &files,
            current_file_index: 0,
            previous_outputs: &prev_outputs,
            plan_metadata: None,
            cap_settings: None,
            slot_values: None,
        };
        let binding = ArgumentBinding::Slot {
            name: "media:enc=utf-8;suffix".to_string(),
            schema: None,
        };
        let result =
            resolve_binding(&binding, &context, "cap:rename", "step_0", None, false).unwrap();
        assert!(result.is_none());
    }

    // TEST803: Tests StrandInput validation detects mismatched Single cardinality with multiple files
    // Verifies is_valid() returns false when Single cardinality has more than one file
    #[test]
    fn test803_machine_input_invalid_single() {
        let files = vec![
            CapInputFile::new("/path/1.pdf".to_string(), "media:ext=pdf".to_string()),
            CapInputFile::new("/path/2.pdf".to_string(), "media:ext=pdf".to_string()),
        ];
        let input = StrandInput {
            files,
            expected_media_urn: "media:ext=pdf".to_string(),
            cardinality: InputCardinality::Single,
        };
        assert!(!input.is_valid());
    }

    // TEST1105: Two steps with the same cap_urn get distinct slot values via different node_ids.
    // This is the core disambiguation scenario that step-index keying was designed to solve.
    #[test]
    fn test1105_two_steps_same_cap_urn_different_slot_values() {
        let cap_urn = "cap:in=\"media:ext=pdf\";make-decision;out=\"media:bool;enc=utf-8\"";
        let slot_name = "media:enc=utf-8;list;question";
        let files = vec![];
        let prev_outputs = HashMap::new();
        let mut slot_values: HashMap<String, Vec<u8>> = HashMap::new();
        slot_values.insert(
            format!("step_0:{}", slot_name),
            b"Is this a contract?".to_vec(),
        );
        slot_values.insert(
            format!("step_2:{}", slot_name),
            b"Is this confidential?".to_vec(),
        );

        let context = ArgumentResolutionContext {
            input_files: &files,
            current_file_index: 0,
            previous_outputs: &prev_outputs,
            plan_metadata: None,
            cap_settings: None,
            slot_values: Some(&slot_values),
        };
        let binding = ArgumentBinding::Slot {
            name: slot_name.to_string(),
            schema: None,
        };

        // step_0 resolves to "Is this a contract?"
        let r0 = resolve_binding(&binding, &context, cap_urn, "step_0", None, true)
            .unwrap()
            .unwrap();
        assert_eq!(r0.value, b"Is this a contract?");
        assert_eq!(r0.source, ArgumentSource::Slot);

        // step_2 resolves to "Is this confidential?"
        let r2 = resolve_binding(&binding, &context, cap_urn, "step_2", None, true)
            .unwrap()
            .unwrap();
        assert_eq!(r2.value, b"Is this confidential?");
        assert_eq!(r2.source, ArgumentSource::Slot);

        // Confirm they differ
        assert_ne!(r0.value, r2.value);
    }

    // TEST1106: Slot resolution falls through to cap_settings when no slot_value exists.
    // cap_settings are keyed by cap_urn (shared across steps), so both steps get the same value.
    #[test]
    fn test1106_slot_falls_through_to_cap_settings_shared() {
        let cap_urn = "cap:in=\"media:ext=pdf\";make-decision;out=\"media:bool;enc=utf-8\"";
        let slot_name = "media:enc=utf-8;language";
        let files = vec![];
        let prev_outputs = HashMap::new();
        let mut cap_settings: HashMap<String, HashMap<String, serde_json::Value>> = HashMap::new();
        let mut decision_settings = HashMap::new();
        decision_settings.insert(slot_name.to_string(), json!("en"));
        cap_settings.insert(cap_urn.to_string(), decision_settings);

        let context = ArgumentResolutionContext {
            input_files: &files,
            current_file_index: 0,
            previous_outputs: &prev_outputs,
            plan_metadata: None,
            cap_settings: Some(&cap_settings),
            slot_values: None, // no per-step values
        };
        let binding = ArgumentBinding::Slot {
            name: slot_name.to_string(),
            schema: None,
        };

        // Both steps fall through to cap_settings — same value
        let r0 = resolve_binding(&binding, &context, cap_urn, "step_0", None, false)
            .unwrap()
            .unwrap();
        let r1 = resolve_binding(&binding, &context, cap_urn, "step_1", None, false)
            .unwrap()
            .unwrap();
        assert_eq!(r0.value, b"en");
        assert_eq!(r1.value, b"en");
        assert_eq!(r0.source, ArgumentSource::CapSetting);
        assert_eq!(r1.source, ArgumentSource::CapSetting);
    }

    // TEST1107: step_0 has a slot_value override, step_1 falls through to cap_settings.
    // Proves per-step override works while shared settings remain as fallback.
    #[test]
    fn test1107_slot_value_overrides_cap_settings_per_step() {
        let cap_urn = "cap:in=\"media:ext=pdf\";make-decision;out=\"media:bool;enc=utf-8\"";
        let slot_name = "media:enc=utf-8;language";
        let files = vec![];
        let prev_outputs = HashMap::new();

        let mut slot_values: HashMap<String, Vec<u8>> = HashMap::new();
        slot_values.insert(format!("step_0:{}", slot_name), b"fr".to_vec());
        // step_1 has no slot_value entry

        let mut cap_settings: HashMap<String, HashMap<String, serde_json::Value>> = HashMap::new();
        let mut decision_settings = HashMap::new();
        decision_settings.insert(slot_name.to_string(), json!("en"));
        cap_settings.insert(cap_urn.to_string(), decision_settings);

        let context = ArgumentResolutionContext {
            input_files: &files,
            current_file_index: 0,
            previous_outputs: &prev_outputs,
            plan_metadata: None,
            cap_settings: Some(&cap_settings),
            slot_values: Some(&slot_values),
        };
        let binding = ArgumentBinding::Slot {
            name: slot_name.to_string(),
            schema: None,
        };

        // step_0: slot_value "fr" (priority 1)
        let r0 = resolve_binding(&binding, &context, cap_urn, "step_0", None, false)
            .unwrap()
            .unwrap();
        assert_eq!(r0.value, b"fr");
        assert_eq!(r0.source, ArgumentSource::Slot);

        // step_1: no slot_value → falls to cap_settings "en" (priority 2)
        let r1 = resolve_binding(&binding, &context, cap_urn, "step_1", None, false)
            .unwrap()
            .unwrap();
        assert_eq!(r1.value, b"en");
        assert_eq!(r1.source, ArgumentSource::CapSetting);
    }

    // TEST1108: ResolveAll with node_id threads correctly through to each binding.
    #[test]
    fn test1108_resolve_all_passes_node_id() {
        let files = vec![];
        let prev_outputs = HashMap::new();
        let mut slot_values: HashMap<String, Vec<u8>> = HashMap::new();
        slot_values.insert("step_3:media:numeric;width".to_string(), b"1024".to_vec());
        slot_values.insert("step_3:media:numeric;quality".to_string(), b"95".to_vec());

        let context = ArgumentResolutionContext {
            input_files: &files,
            current_file_index: 0,
            previous_outputs: &prev_outputs,
            plan_metadata: None,
            cap_settings: None,
            slot_values: Some(&slot_values),
        };

        let mut bindings = ArgumentBindings::new();
        bindings.add(
            "media:numeric;width".to_string(),
            ArgumentBinding::Slot {
                name: "media:numeric;width".to_string(),
                schema: None,
            },
        );
        bindings.add(
            "media:numeric;quality".to_string(),
            ArgumentBinding::Slot {
                name: "media:numeric;quality".to_string(),
                schema: None,
            },
        );

        let results = bindings
            .resolve_all(&context, "cap:resize", "step_3", None, None)
            .unwrap();
        assert_eq!(results.len(), 2);

        let width = results
            .iter()
            .find(|r| r.name == "media:numeric;width")
            .unwrap();
        assert_eq!(width.value, b"1024");
        assert_eq!(width.source, ArgumentSource::Slot);

        let quality = results
            .iter()
            .find(|r| r.name == "media:numeric;quality")
            .unwrap();
        assert_eq!(quality.value, b"95");
        assert_eq!(quality.source, ArgumentSource::Slot);
    }

    // TEST1109: Slot key uses node_id, NOT cap_urn — a slot_value keyed by cap_urn must not match.
    #[test]
    fn test1109_slot_key_uses_node_id_not_cap_urn() {
        let cap_urn = "cap:in=\"media:ext=pdf\";resize;out=\"media:ext=pdf\"";
        let slot_name = "media:numeric;width";
        let files = vec![];
        let prev_outputs = HashMap::new();
        let mut slot_values: HashMap<String, Vec<u8>> = HashMap::new();
        // Deliberately key by cap_urn (the OLD format) — should NOT match
        slot_values.insert(format!("{}:{}", cap_urn, slot_name), b"800".to_vec());

        let context = ArgumentResolutionContext {
            input_files: &files,
            current_file_index: 0,
            previous_outputs: &prev_outputs,
            plan_metadata: None,
            cap_settings: None,
            slot_values: Some(&slot_values),
        };
        let binding = ArgumentBinding::Slot {
            name: slot_name.to_string(),
            schema: None,
        };

        // Should NOT find the value because the key format is wrong (cap_urn instead of node_id)
        let result = resolve_binding(&binding, &context, cap_urn, "step_0", None, false).unwrap();
        assert!(
            result.is_none(),
            "Old cap_urn-based key must not match node_id-based lookup"
        );
    }
}
