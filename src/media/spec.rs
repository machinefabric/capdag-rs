//! MediaDef parsing and media URN resolution
//!
//! This module provides:
//! - Media URN resolution (e.g., `media:string` → resolved media def)
//! - MediaDef parsing (canonical form: `text/plain; profile=https://...`)
//! - MediaDef for defining specs in cap definitions
//! - MediaValidation for validation rules inherent to media types
//!
//! ## Media URN Format
//! Media URNs are tagged URNs with "media" prefix, e.g., `media:string`
//! Built-in primitives are available without explicit declaration.
//!
//! ## MediaDef Format
//! Canonical form: `<media-type>; profile=<url>`
//! Example: `text/plain; profile=https://capdag.com/schema/str`

use serde::{Deserialize, Serialize};
use std::fmt;

// =============================================================================
// PROFILE URLS (canonical /schema/ path)
// =============================================================================

/// The namespace every profile URL is in. A profile URL is an identity a
/// media definition carries, the same in every environment — it is not where
/// a registry is, so nothing configures it.
pub const SCHEMA_BASE: &str = "https://capdag.com/schema";

/// Profile URL for string type
pub const PROFILE_STR: &str = "https://capdag.com/schema/str";
/// Profile URL for integer type
pub const PROFILE_INT: &str = "https://capdag.com/schema/int";
/// Profile URL for number type
pub const PROFILE_NUM: &str = "https://capdag.com/schema/num";
/// Profile URL for boolean type
pub const PROFILE_BOOL: &str = "https://capdag.com/schema/bool";
/// Profile URL for JSON object type
pub const PROFILE_OBJ: &str = "https://capdag.com/schema/obj";
/// Profile URL for string array type
pub const PROFILE_STR_ARRAY: &str = "https://capdag.com/schema/str-array";
/// Profile URL for integer array type
pub const PROFILE_INT_ARRAY: &str = "https://capdag.com/schema/int-array";
/// Profile URL for number array type
pub const PROFILE_NUM_ARRAY: &str = "https://capdag.com/schema/num-array";
/// Profile URL for boolean array type
pub const PROFILE_BOOL_ARRAY: &str = "https://capdag.com/schema/bool-array";
/// Profile URL for object array type
pub const PROFILE_OBJ_ARRAY: &str = "https://capdag.com/schema/obj-array";
/// Profile URL for void (no input)
pub const PROFILE_VOID: &str = "https://capdag.com/schema/void";

// =============================================================================
// SEMANTIC CONTENT TYPE PROFILE URLS
// =============================================================================

/// Profile URL for image data (png, jpg, gif, etc.)
pub const PROFILE_IMAGE: &str = "https://capdag.com/schema/image";
/// Profile URL for audio data (wav, mp3, flac, etc.)
pub const PROFILE_AUDIO: &str = "https://capdag.com/schema/audio";
/// Profile URL for video data (mp4, webm, mov, etc.)
pub const PROFILE_VIDEO: &str = "https://capdag.com/schema/video";
/// Profile URL for generic text
pub const PROFILE_TEXT: &str = "https://capdag.com/schema/text";

// =============================================================================
// DOCUMENT TYPE PROFILE URLS (PRIMARY naming)
// =============================================================================

/// Profile URL for PDF documents
pub const PROFILE_PDF: &str = "https://capdag.com/schema/pdf";
/// Profile URL for EPUB documents
pub const PROFILE_EPUB: &str = "https://capdag.com/schema/epub";

// =============================================================================
// TEXT FORMAT TYPE PROFILE URLS (PRIMARY naming)
// =============================================================================

/// Profile URL for Markdown text
pub const PROFILE_MD: &str = "https://capdag.com/schema/md";
/// Profile URL for plain text
pub const PROFILE_TXT: &str = "https://capdag.com/schema/txt";
/// Profile URL for reStructuredText
pub const PROFILE_RST: &str = "https://capdag.com/schema/rst";
/// Profile URL for log files
pub const PROFILE_LOG: &str = "https://capdag.com/schema/log";
/// Profile URL for HTML documents
pub const PROFILE_HTML: &str = "https://capdag.com/schema/html";
/// Profile URL for XML documents
pub const PROFILE_XML: &str = "https://capdag.com/schema/xml";
/// Profile URL for JSON data
pub const PROFILE_JSON: &str = "https://capdag.com/schema/json";
/// Profile URL for YAML data
pub const PROFILE_YAML: &str = "https://capdag.com/schema/yaml";

// =============================================================================
// CAPDAG OUTPUT PROFILE URLS
// =============================================================================

/// Profile URL for model download output
pub const PROFILE_CAPDAG_DOWNLOAD_OUTPUT: &str = "https://capdag.com/schema/download-output";
/// Profile URL for model load output
pub const PROFILE_CAPDAG_LOAD_OUTPUT: &str = "https://capdag.com/schema/load-output";
/// Profile URL for model unload output
pub const PROFILE_CAPDAG_UNLOAD_OUTPUT: &str = "https://capdag.com/schema/unload-output";
/// Profile URL for model list output
pub const PROFILE_CAPDAG_LIST_OUTPUT: &str = "https://capdag.com/schema/model-list";
/// Profile URL for model status output
pub const PROFILE_CAPDAG_STATUS_OUTPUT: &str = "https://capdag.com/schema/status-output";
/// Profile URL for model contents output
pub const PROFILE_CAPDAG_CONTENTS_OUTPUT: &str = "https://capdag.com/schema/contents-output";
/// Profile URL for embeddings generate output
pub const PROFILE_CAPDAG_GENERATE_OUTPUT: &str = "https://capdag.com/schema/embeddings";
/// Profile URL for questions array
pub const PROFILE_CAPDAG_QUESTIONS_ARRAY: &str = "https://capdag.com/schema/questions-array";

// =============================================================================
// MEDIA VALIDATION (for media definitions)
// =============================================================================

/// Validation rules for media types
///
/// These rules are inherent to the semantic media type and are defined
/// in the media def, not on individual arguments or outputs.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct MediaValidation {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_length: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_length: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_values: Option<Vec<String>>,
}

impl MediaValidation {
    /// The first of these rules `value` breaks, described, or `None` when it
    /// breaks none. `Err` when a rule itself cannot be applied (a pattern that
    /// is not a regular expression): a defect in the media definition, not in
    /// the value.
    ///
    /// A rule applies to the values it is about and passes the rest: `min` and
    /// `max` to numbers, the length rules to strings and arrays, `pattern` and
    /// `allowed_values` to strings. This is the one decision of whether a value
    /// satisfies a media definition's validation — what an argument value is
    /// held to at run time and what a constant written in a machine is held to
    /// when the machine is resolved.
    pub fn violation(&self, value: &serde_json::Value) -> Result<Option<String>, String> {
        if let (Some(min), Some(num)) = (self.min, value.as_f64()) {
            if num < min {
                return Ok(Some(format!("minimum value {}", min)));
            }
        }
        if let (Some(max), Some(num)) = (self.max, value.as_f64()) {
            if num > max {
                return Ok(Some(format!("maximum value {}", max)));
            }
        }
        // Length, for strings and arrays.
        let length = match (value.as_str(), value.as_array()) {
            (Some(s), _) => Some((s.len(), "")),
            (_, Some(arr)) => Some((arr.len(), "array ")),
            _ => None,
        };
        if let Some((len, kind)) = length {
            if let Some(min_length) = self.min_length {
                if len < min_length {
                    return Ok(Some(format!("minimum {}length {}", kind, min_length)));
                }
            }
            if let Some(max_length) = self.max_length {
                if len > max_length {
                    return Ok(Some(format!("maximum {}length {}", kind, max_length)));
                }
            }
        }
        if let (Some(pattern), Some(text)) = (&self.pattern, value.as_str()) {
            let regex = regex::Regex::new(pattern)
                .map_err(|e| format!("Invalid regex pattern '{}': {}", pattern, e))?;
            if !regex.is_match(text) {
                return Ok(Some(format!("pattern '{}'", pattern)));
            }
        }
        if let (Some(allowed_values), Some(text)) = (&self.allowed_values, value.as_str()) {
            if !allowed_values.iter().any(|allowed| allowed == text) {
                return Ok(Some(format!("allowed values: {:?}", allowed_values)));
            }
        }
        Ok(None)
    }

    /// Check if all validation fields are empty/None
    pub fn is_empty(&self) -> bool {
        self.min.is_none()
            && self.max.is_none()
            && self.min_length.is_none()
            && self.max_length.is_none()
            && self.pattern.is_none()
            && self.allowed_values.is_none()
    }

    /// Create validation with min/max numeric constraints
    pub fn numeric_range(min: Option<f64>, max: Option<f64>) -> Self {
        Self {
            min,
            max,
            min_length: None,
            max_length: None,
            pattern: None,
            allowed_values: None,
        }
    }

    /// Create validation with string length constraints
    pub fn string_length(min_length: Option<usize>, max_length: Option<usize>) -> Self {
        Self {
            min: None,
            max: None,
            min_length,
            max_length,
            pattern: None,
            allowed_values: None,
        }
    }

    /// Create validation with pattern
    pub fn with_pattern(pattern: String) -> Self {
        Self {
            min: None,
            max: None,
            min_length: None,
            max_length: None,
            pattern: Some(pattern),
            allowed_values: None,
        }
    }

    /// Create validation with allowed values
    pub fn with_allowed_values(values: Vec<String>) -> Self {
        Self {
            min: None,
            max: None,
            min_length: None,
            max_length: None,
            pattern: None,
            allowed_values: Some(values),
        }
    }
}

// =============================================================================
// MEDIA DEFINITION DEFINITION (for cap definitions)
// =============================================================================

/// Media definition - can be string (compact) or object (rich)
///
/// Used in the `media_defs` map of a cap definition.
///
/// ## String Form (compact)
/// ```json
/// "media:string": "text/plain; profile=https://capdag.com/schema/str"
/// ```
///
/// Media definition for inline media_defs in cap definitions
///
/// This is the same structure as media def JSON files in the registry.
/// Each media def has a unique URN that identifies it.
///
/// ## Example
/// ```json
/// {
///   "urn": "media:fmt=json;my-output;record",
///   "media_type": "application/json",
///   "title": "My Output",
///   "profile_uri": "https://example.com/schema/my-output",
///   "schema": { "type": "object", ... }
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MediaDef {
    /// The media URN identifier (e.g., "media:binary;document;ext=pdf")
    pub urn: String,
    /// The MIME media type (e.g., "application/json", "text/plain")
    pub media_type: String,
    /// Human-readable title for the media type (required)
    pub title: String,
    /// Profile URI for schema reference
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_uri: Option<String>,
    /// Optional local JSON Schema for validation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
    /// Optional short plain-text description of the media type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Optional long-form markdown documentation.
    ///
    /// Rendered in media info panels, the cap navigator,
    /// capdag-dot-com, and anywhere else a rich-text explanation of
    /// the media def is useful. Authored in TOML sources as a
    /// triple-quoted literal string (`'''...'''`) so markdown
    /// punctuation and newlines pass through unchanged; the JSON
    /// generator escapes newlines per JSON rules on output.
    ///
    /// Only what is specific to this media def: what the dims it was built
    /// from mean is documented with each dim (see `dims`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
    /// The dims this media def was built from, and the value chosen from
    /// each, in the order its source references them. Each dim is published
    /// on its own (`FabricRegistry::get_dim`); a reader shows its
    /// documentation beside `documentation`. Empty when built from no dim.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dims: Vec<crate::DimRef>,
    /// Optional validation rules for this media type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation: Option<MediaValidation>,
    /// Optional metadata (arbitrary key-value pairs for display/categorization)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    /// File extensions for storing this media type (e.g., ["pdf"], ["jpg", "jpeg"])
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extensions: Vec<String>,
}

impl MediaDef {
    /// Create a new media definition
    pub fn new(
        urn: impl Into<String>,
        media_type: impl Into<String>,
        title: impl Into<String>,
    ) -> Self {
        Self {
            urn: urn.into(),
            media_type: media_type.into(),
            title: title.into(),
            profile_uri: None,
            schema: None,
            description: None,
            documentation: None,
            dims: Vec::new(),
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        }
    }

    /// Create a media definition with profile URI
    pub fn with_profile(
        urn: impl Into<String>,
        media_type: impl Into<String>,
        title: impl Into<String>,
        profile_uri: impl Into<String>,
    ) -> Self {
        Self {
            urn: urn.into(),
            media_type: media_type.into(),
            title: title.into(),
            profile_uri: Some(profile_uri.into()),
            schema: None,
            description: None,
            documentation: None,
            dims: Vec::new(),
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        }
    }

    /// Create a media definition with schema
    pub fn with_schema(
        urn: impl Into<String>,
        media_type: impl Into<String>,
        title: impl Into<String>,
        profile_uri: impl Into<String>,
        schema: serde_json::Value,
    ) -> Self {
        Self {
            urn: urn.into(),
            media_type: media_type.into(),
            title: title.into(),
            profile_uri: Some(profile_uri.into()),
            schema: Some(schema),
            description: None,
            documentation: None,
            dims: Vec::new(),
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        }
    }

    /// Create a media definition with validation rules
    pub fn with_validation(
        urn: impl Into<String>,
        media_type: impl Into<String>,
        title: impl Into<String>,
        validation: MediaValidation,
    ) -> Self {
        Self {
            urn: urn.into(),
            media_type: media_type.into(),
            title: title.into(),
            profile_uri: None,
            schema: None,
            description: None,
            documentation: None,
            dims: Vec::new(),
            validation: Some(validation),
            metadata: None,
            extensions: Vec::new(),
        }
    }

    /// Get the long-form markdown documentation, if any.
    pub fn get_documentation(&self) -> Option<&str> {
        self.documentation.as_deref()
    }

    /// Set the long-form markdown documentation.
    pub fn set_documentation(&mut self, documentation: impl Into<String>) {
        self.documentation = Some(documentation.into());
    }

    /// Clear the long-form markdown documentation.
    pub fn clear_documentation(&mut self) {
        self.documentation = None;
    }
}

// =============================================================================
// RESOLVED MEDIA DEFINITION
// =============================================================================

/// Fully resolved media def with all fields populated
///
/// This is the result of resolving a media URN through the media_defs table
/// or from a built-in definition.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedMediaDef {
    /// The media URN that was resolved
    pub media_urn: String,
    /// The MIME media type (e.g., "application/json", "text/plain")
    pub media_type: String,
    /// Optional profile URI
    pub profile_uri: Option<String>,
    /// Optional local JSON Schema for validation
    pub schema: Option<serde_json::Value>,
    /// Display-friendly title for the media type
    pub title: Option<String>,
    /// Optional short plain-text description of the media type
    pub description: Option<String>,
    /// Optional long-form markdown documentation.
    ///
    /// Rendered in media info panels, the cap navigator,
    /// capdag-dot-com, and anywhere else a rich-text explanation of
    /// the media def is useful. Only what is specific to the media def.
    pub documentation: Option<String>,
    /// The dims the media def was built from, and the value chosen from each.
    pub dims: Vec<crate::DimRef>,
    /// Optional validation rules from the media definition
    pub validation: Option<MediaValidation>,
    /// Optional metadata (arbitrary key-value pairs for display/categorization)
    pub metadata: Option<serde_json::Value>,
    /// File extensions for storing this media type (e.g., ["pdf"], ["jpg", "jpeg"])
    pub extensions: Vec<String>,
}

impl ResolvedMediaDef {
    /// Parse the media URN, panicking if invalid (should never happen for resolved specs)
    fn parse_media_urn(&self) -> crate::MediaUrn {
        crate::MediaUrn::from_string(&self.media_urn)
            .expect("ResolvedMediaDef has invalid media_urn - this indicates a bug in resolution")
    }

    /// Check if this represents a record (has internal key-value structure).
    /// This indicates the data has internal fields (e.g., JSON object).
    pub fn is_record(&self) -> bool {
        self.parse_media_urn().is_record()
    }

    /// Check if this represents a scalar value (no list marker).
    /// Scalar is the default cardinality - a single value, not a collection.
    pub fn is_scalar(&self) -> bool {
        self.parse_media_urn().is_scalar()
    }

    /// Check if this represents a list/array structure (has list marker).
    /// This indicates an ordered collection of values.
    pub fn is_list(&self) -> bool {
        self.parse_media_urn().is_list()
    }

    /// Check if this represents structured data (record or list).
    /// Structured data can be serialized as JSON when transmitted as text.
    /// Note: This does NOT check for the explicit `json` tag - use is_json() for that.
    pub fn is_structured(&self) -> bool {
        self.is_record() || self.is_list()
    }

    /// Check if this represents JSON representation specifically.
    /// Returns true if the "json" marker tag is present in the source media URN.
    /// Note: This only checks for explicit JSON format marker.
    /// For checking if data is structured (map/list), use is_structured().
    pub fn is_json(&self) -> bool {
        self.parse_media_urn().is_json()
    }

    /// Check if this represents image data.
    /// Returns true if the "image" marker tag is present in the source media URN.
    pub fn is_image(&self) -> bool {
        self.parse_media_urn().is_image()
    }

    /// Check if this represents audio data.
    /// Returns true if the "audio" marker tag is present in the source media URN.
    pub fn is_audio(&self) -> bool {
        self.parse_media_urn().is_audio()
    }

    /// Check if this represents video data.
    /// Returns true if the "video" marker tag is present in the source media URN.
    pub fn is_video(&self) -> bool {
        self.parse_media_urn().is_video()
    }

    /// Check if this represents numeric data.
    /// Returns true if the "numeric" marker tag is present in the source media URN.
    pub fn is_numeric(&self) -> bool {
        self.parse_media_urn().is_numeric()
    }

    /// Check if this represents boolean data.
    /// Returns true if the "bool" marker tag is present in the source media URN.
    pub fn is_bool(&self) -> bool {
        self.parse_media_urn().is_bool()
    }
}

// =============================================================================
// MEDIA URN RESOLUTION
// =============================================================================

/// Resolve a media URN to a full media definition.
///
/// This is the SINGLE resolution path for all media URN lookups.
///
/// Resolution order:
/// 1. Cap's local `media_defs` array (HIGHEST - cap-specific definitions)
/// 2. Registry's in-memory + disk cache
/// 3. Online registry fetch (blocked by the registry's offline flag if set)
/// 4. If none resolve → Error
///
/// # Arguments
/// * `media_urn` - The media URN to resolve (e.g., "media:enc=utf-8")
/// * `registry` - The FabricRegistry for cache and remote lookups
///
/// # Errors
/// Returns `MediaDefError::UnresolvableMediaUrn` if the media URN cannot be resolved.
pub async fn resolve_media_urn(
    media_urn: &str,
    registry: &crate::media::registry::FabricRegistry,
) -> Result<ResolvedMediaDef, MediaDefError> {
    match registry.get_media_def(media_urn).await {
        Ok(stored_spec) => Ok(ResolvedMediaDef {
            media_urn: media_urn.to_string(),
            media_type: stored_spec.media_type,
            profile_uri: stored_spec.profile_uri,
            schema: stored_spec.schema,
            title: Some(stored_spec.title),
            description: stored_spec.description,
            documentation: stored_spec.documentation,
            dims: stored_spec.dims,
            validation: stored_spec.validation,
            metadata: stored_spec.metadata,
            extensions: stored_spec.extensions,
        }),
        Err(e) => Err(MediaDefError::UnresolvableMediaUrn(format!(
            "cannot resolve media URN '{}' via registry: {}",
            media_urn, e
        ))),
    }
}

/// Validate that media_defs array has no duplicate URNs.
///
/// # Arguments
/// * `media_defs` - The media_defs array to validate
///
/// # Errors
/// Returns `MediaDefError::DuplicateMediaUrn` if any URN appears more than once.
pub fn validate_media_defs_no_duplicates(media_defs: &[MediaDef]) -> Result<(), MediaDefError> {
    let mut seen = std::collections::HashSet::new();
    for spec in media_defs {
        if !seen.insert(&spec.urn) {
            return Err(MediaDefError::DuplicateMediaUrn(spec.urn.clone()));
        }
    }
    Ok(())
}

// =============================================================================
// ERRORS
// =============================================================================

/// Errors that can occur when resolving media defs
#[derive(Debug, Clone, PartialEq)]
pub enum MediaDefError {
    /// Media URN cannot be resolved (not in media_defs and not in registry)
    UnresolvableMediaUrn(String),
    /// Duplicate media URN in media_defs array
    DuplicateMediaUrn(String),
}

impl fmt::Display for MediaDefError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MediaDefError::UnresolvableMediaUrn(urn) => {
                write!(
                    f,
                    "cannot resolve media URN '{}' - not found in media_defs or registry",
                    urn
                )
            }
            MediaDefError::DuplicateMediaUrn(urn) => {
                write!(
                    f,
                    "duplicate media URN '{}' in media_defs - each URN must be unique",
                    urn
                )
            }
        }
    }
}

impl std::error::Error for MediaDefError {}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // Media URN resolution tests
    // -------------------------------------------------------------------------

    // Helper to create a test registry. Test specs are seeded directly
    // through the registry's `insert_cached_media_def_for_test` helper —
    // there is no longer any cap-local media_defs override path.
    async fn test_registry() -> crate::media::registry::FabricRegistry {
        crate::media::registry::FabricRegistry::new_for_test()
    }

    // TEST088: Resolving a media URN seeded into the registry returns
    // the seeded spec verbatim. A regression in the registry-resolution
    // path would surface as a `None`-shaped result here, since there is
    // no local-override fallback to mask it.
    #[tokio::test]
    async fn test088_resolve_seeded_spec() {
        let registry = test_registry().await;
        registry.insert_cached_media_def_for_test(crate::StoredMediaDef {
            version: 0,
            urn: "media:enc=utf-8".to_string(),
            media_type: "text/plain".to_string(),
            title: "UTF-8 text".to_string(),
            profile_uri: None,
            schema: None,
            description: None,
            documentation: None,
            dims: Vec::new(),
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        });
        let resolved = resolve_media_urn("media:enc=utf-8", &registry)
            .await
            .unwrap();
        assert_eq!(resolved.media_type, "text/plain");
        assert!(resolved.profile_uri.is_none());
    }

    // TEST089: A seeded record-shaped media def carries its schema and
    // profile_uri intact through resolution. Catches a regression that
    // dropped optional fields when copying into ResolvedMediaDef.
    #[tokio::test]
    async fn test089_resolve_seeded_record_spec() {
        let registry = test_registry().await;
        let schema = serde_json::json!({
            "type": "object",
            "properties": { "name": { "type": "string" } }
        });
        registry.insert_cached_media_def_for_test(crate::StoredMediaDef {
            version: 0,
            urn: "media:fmt=json;output-spec;record".to_string(),
            media_type: "application/json".to_string(),
            title: "Output Spec".to_string(),
            profile_uri: Some("https://example.com/schema/output".to_string()),
            schema: Some(schema.clone()),
            description: None,
            documentation: None,
            dims: Vec::new(),
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        });
        let resolved = resolve_media_urn("media:fmt=json;output-spec;record", &registry)
            .await
            .unwrap();
        assert_eq!(resolved.media_type, "application/json");
        assert_eq!(
            resolved.profile_uri,
            Some("https://example.com/schema/output".to_string())
        );
        assert_eq!(resolved.schema, Some(schema));
    }

    // TEST093: Resolving a URN that is neither in the registry cache nor
    // available online fails hard. A regression that made the fail path
    // silently return a stub `ResolvedMediaDef` would surface here as a
    // missing error.
    #[tokio::test]
    async fn test093_resolve_unresolvable_fails_hard() {
        let registry = test_registry().await;
        registry.set_offline(true);
        let result =
            resolve_media_urn("media:completely-unknown-urn-not-in-registry", &registry).await;
        assert!(result.is_err(), "unknown URN must produce an error");
        if let Err(MediaDefError::UnresolvableMediaUrn(msg)) = result {
            assert!(
                msg.contains("media:completely-unknown-urn-not-in-registry"),
                "error must name the failing URN; got: {}",
                msg
            );
        } else {
            panic!("expected UnresolvableMediaUrn error");
        }
    }

    // -------------------------------------------------------------------------
    // MediaDef serialization tests
    // -------------------------------------------------------------------------

    // TEST095: Test MediaDef serializes with required fields and skips None fields
    #[test]
    fn test095_media_def_def_serialize() {
        let def = MediaDef {
            urn: "media:fmt=json;test".to_string(),
            media_type: "application/json".to_string(),
            title: "Test Media".to_string(),
            profile_uri: Some("https://example.com/profile".to_string()),
            schema: None,
            description: None,
            documentation: None,
            dims: Vec::new(),
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        };
        let json = serde_json::to_string(&def).unwrap();
        assert!(json.contains("\"urn\":\"media:fmt=json;test\""));
        assert!(json.contains("\"media_type\":\"application/json\""));
        assert!(json.contains("\"profile_uri\":\"https://example.com/profile\""));
        assert!(json.contains("\"title\":\"Test Media\""));
        // None schema is skipped
        assert!(!json.contains("\"schema\":"));
        // None description is also skipped
        assert!(!json.contains("\"description\":"));
    }

    // TEST096: Test deserializing MediaDef from JSON object
    #[test]
    fn test096_media_def_def_deserialize() {
        let json =
            r#"{"urn":"media:fmt=json;test","media_type":"application/json","title":"Test"}"#;
        let def: MediaDef = serde_json::from_str(json).unwrap();
        assert_eq!(def.urn, "media:fmt=json;test");
        assert_eq!(def.media_type, "application/json");
        assert_eq!(def.title, "Test");
        assert!(def.profile_uri.is_none());
    }

    // -------------------------------------------------------------------------
    // Duplicate URN validation tests
    // -------------------------------------------------------------------------

    // TEST097: Test duplicate URN validation catches duplicates
    #[test]
    fn test097_validate_no_duplicate_urns_catches_duplicates() {
        let media_defs = vec![
            MediaDef::new("media:dup;fmt=json", "application/json", "First"),
            MediaDef::new("media:dup;fmt=json", "application/json", "Second"), // duplicate
        ];
        let result = validate_media_defs_no_duplicates(&media_defs);
        assert!(result.is_err());
        if let Err(MediaDefError::DuplicateMediaUrn(urn)) = result {
            assert_eq!(urn, "media:dup;fmt=json");
        } else {
            panic!("Expected DuplicateMediaUrn error");
        }
    }

    // TEST098: Test duplicate URN validation passes for unique URNs
    #[test]
    fn test098_validate_no_duplicate_urns_passes_for_unique() {
        let media_defs = vec![
            MediaDef::new("media:first;fmt=json", "application/json", "First"),
            MediaDef::new("media:fmt=json;second", "application/json", "Second"),
        ];
        let result = validate_media_defs_no_duplicates(&media_defs);
        assert!(result.is_ok());
    }

    // -------------------------------------------------------------------------
    // ResolvedMediaDef tests
    // -------------------------------------------------------------------------

    // TEST099: The identity media (`media:`) carries no encoding, no record
    // marker, and no format. The old is_binary() delegate is gone (binary/text is
    // no longer a distinction); a media is text-representable iff it declares enc=.
    #[test]
    fn test099_resolved_is_binary() {
        let resolved = ResolvedMediaDef {
            media_urn: "media:".to_string(),
            media_type: "application/octet-stream".to_string(),
            profile_uri: None,
            schema: None,
            title: None,
            description: None,
            documentation: None,
            dims: Vec::new(),
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        };
        assert!(resolved.parse_media_urn().get_tag("enc").is_none());
        assert!(!resolved.is_record());
        assert!(!resolved.is_json());
    }

    // TEST100: Test ResolvedMediaDef is_record returns true when record marker is present
    #[test]
    fn test100_resolved_is_record() {
        let resolved = ResolvedMediaDef {
            media_urn: "media:enc=utf-8;record".to_string(),
            media_type: "application/json".to_string(),
            profile_uri: None,
            schema: None,
            title: None,
            description: None,
            documentation: None,
            dims: Vec::new(),
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        };
        assert!(resolved.is_record());
        assert!(resolved.is_scalar(), "record without list marker is scalar");
        assert!(!resolved.is_list());
    }

    // TEST101: Test ResolvedMediaDef is_scalar returns true when list marker is absent
    #[test]
    fn test101_resolved_is_scalar() {
        let resolved = ResolvedMediaDef {
            media_urn: "media:enc=utf-8".to_string(),
            media_type: "text/plain".to_string(),
            profile_uri: None,
            schema: None,
            title: None,
            description: None,
            documentation: None,
            dims: Vec::new(),
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        };
        assert!(resolved.is_scalar());
        assert!(!resolved.is_record());
        assert!(!resolved.is_list());
    }

    // TEST102: Test ResolvedMediaDef is_list returns true when list marker is present
    #[test]
    fn test102_resolved_is_list() {
        let resolved = ResolvedMediaDef {
            media_urn: "media:enc=utf-8;list".to_string(),
            media_type: "application/json".to_string(),
            profile_uri: None,
            schema: None,
            title: None,
            description: None,
            documentation: None,
            dims: Vec::new(),
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        };
        assert!(resolved.is_list());
        assert!(!resolved.is_record());
        assert!(!resolved.is_scalar());
    }

    // TEST103: Test ResolvedMediaDef is_json returns true when json tag is present
    #[test]
    fn test103_resolved_is_json() {
        let resolved = ResolvedMediaDef {
            media_urn: "media:fmt=json;record".to_string(),
            media_type: "application/json".to_string(),
            profile_uri: None,
            schema: None,
            title: None,
            description: None,
            documentation: None,
            dims: Vec::new(),
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        };
        assert!(resolved.is_json());
        assert!(resolved.is_record());
    }

    // TEST104: Text-representability is now carried by the orthogonal `enc=` tag.
    // The old is_text()/is_binary() delegates on ResolvedMediaDef are gone; a media
    // is text iff its URN declares an encoding. `media:enc=utf-8` is plain UTF-8
    // text — has enc, is not JSON.
    #[test]
    fn test104_resolved_is_text() {
        let resolved = ResolvedMediaDef {
            media_urn: "media:enc=utf-8".to_string(),
            media_type: "text/plain".to_string(),
            profile_uri: None,
            schema: None,
            title: None,
            description: None,
            documentation: None,
            dims: Vec::new(),
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        };
        assert!(resolved.parse_media_urn().get_tag("enc").is_some());
        assert!(!resolved.is_json());
    }

    // -------------------------------------------------------------------------
    // Metadata propagation tests
    // -------------------------------------------------------------------------

    // TEST105: Test metadata propagates from media def def to resolved media def
    #[tokio::test]
    async fn test105_metadata_propagation() {
        let registry = test_registry().await;
        registry.insert_cached_media_def_for_test(crate::StoredMediaDef {
            dims: Vec::new(),
            version: 0,
            urn: "media:custom-setting".to_string(),
            media_type: "text/plain".to_string(),
            title: "Custom Setting".to_string(),
            profile_uri: Some("https://example.com/schema".to_string()),
            schema: None,
            description: Some("A custom setting".to_string()),
            documentation: None,
            validation: None,
            metadata: Some(serde_json::json!({
                "category_key": "interface",
                "ui_type": "SETTING_UI_TYPE_CHECKBOX"
            })),
            extensions: Vec::new(),
        });

        let resolved = resolve_media_urn("media:custom-setting", &registry)
            .await
            .unwrap();
        assert!(resolved.metadata.is_some());
        let metadata = resolved.metadata.unwrap();
        assert_eq!(metadata.get("category_key").unwrap(), "interface");
        assert_eq!(metadata.get("ui_type").unwrap(), "SETTING_UI_TYPE_CHECKBOX");
    }

    // TEST106: Test metadata and validation can coexist in media definition
    #[tokio::test]
    async fn test106_metadata_with_validation() {
        let registry = test_registry().await;
        registry.insert_cached_media_def_for_test(crate::StoredMediaDef {
            dims: Vec::new(),
            version: 0,
            urn: "media:bounded-number;numeric".to_string(),
            media_type: "text/plain".to_string(),
            title: "Bounded Number".to_string(),
            profile_uri: Some("https://example.com/schema".to_string()),
            schema: None,
            description: None,
            documentation: None,
            validation: Some(MediaValidation {
                min: Some(0.0),
                max: Some(100.0),
                min_length: None,
                max_length: None,
                pattern: None,
                allowed_values: None,
            }),
            metadata: Some(serde_json::json!({
                "category_key": "inference",
                "ui_type": "SETTING_UI_TYPE_SLIDER"
            })),
            extensions: Vec::new(),
        });

        let resolved = resolve_media_urn("media:bounded-number;numeric", &registry)
            .await
            .unwrap();

        // Verify validation
        assert!(resolved.validation.is_some());
        let validation = resolved.validation.unwrap();
        assert_eq!(validation.min, Some(0.0));
        assert_eq!(validation.max, Some(100.0));

        // Verify metadata
        assert!(resolved.metadata.is_some());
        let metadata = resolved.metadata.unwrap();
        assert_eq!(metadata.get("category_key").unwrap(), "inference");
    }

    // -------------------------------------------------------------------------
    // Extension field tests
    // -------------------------------------------------------------------------

    // TEST107: Test extensions field propagates from media def def to resolved
    #[tokio::test]
    async fn test107_extensions_propagation() {
        let registry = test_registry().await;
        registry.insert_cached_media_def_for_test(crate::StoredMediaDef {
            dims: Vec::new(),
            version: 0,
            urn: "media:custom-pdf".to_string(),
            media_type: "application/pdf".to_string(),
            title: "PDF Document".to_string(),
            profile_uri: Some("https://capdag.com/schema/pdf".to_string()),
            schema: None,
            description: Some("A PDF document".to_string()),
            documentation: None,
            validation: None,
            metadata: None,
            extensions: vec!["pdf".to_string()],
        });

        let resolved = resolve_media_urn("media:custom-pdf", &registry)
            .await
            .unwrap();
        assert_eq!(resolved.extensions, vec!["pdf".to_string()]);
    }

    // TEST892: Test extensions serializes/deserializes correctly in MediaDef
    #[test]
    fn test892_extensions_serialization() {
        let def = MediaDef {
            urn: "media:json-data".to_string(),
            media_type: "application/json".to_string(),
            title: "JSON Data".to_string(),
            profile_uri: Some("https://example.com/profile".to_string()),
            schema: None,
            description: None,
            documentation: None,
            dims: Vec::new(),
            validation: None,
            metadata: None,
            extensions: vec!["json".to_string()],
        };
        let json = serde_json::to_string(&def).unwrap();
        assert!(json.contains("\"extensions\":[\"json\"]"));

        // Deserialize and verify
        let parsed: MediaDef = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.extensions, vec!["json".to_string()]);
    }

    // TEST893: Test extensions can coexist with metadata and validation
    #[tokio::test]
    async fn test893_extensions_with_metadata_and_validation() {
        let registry = test_registry().await;
        registry.insert_cached_media_def_for_test(crate::StoredMediaDef {
            dims: Vec::new(),
            version: 0,
            urn: "media:custom-output;fmt=json".to_string(),
            media_type: "application/json".to_string(),
            title: "Custom Output".to_string(),
            profile_uri: Some("https://example.com/schema".to_string()),
            schema: None,
            description: None,
            documentation: None,
            validation: Some(MediaValidation {
                min: None,
                max: None,
                min_length: Some(1),
                max_length: Some(1000),
                pattern: None,
                allowed_values: None,
            }),
            metadata: Some(serde_json::json!({
                "category": "output"
            })),
            extensions: vec!["json".to_string()],
        });

        let resolved = resolve_media_urn("media:custom-output;fmt=json", &registry)
            .await
            .unwrap();

        // Verify all fields are present
        assert!(resolved.validation.is_some());
        assert!(resolved.metadata.is_some());
        assert_eq!(resolved.extensions, vec!["json".to_string()]);
    }

    // TEST894: Test multiple extensions in a media def
    #[tokio::test]
    async fn test894_multiple_extensions() {
        let registry = test_registry().await;
        registry.insert_cached_media_def_for_test(crate::StoredMediaDef {
            dims: Vec::new(),
            version: 0,
            urn: "media:ext=jpeg;image".to_string(),
            media_type: "image/jpeg".to_string(),
            title: "JPEG Image".to_string(),
            profile_uri: Some("https://capdag.com/schema/jpeg".to_string()),
            schema: None,
            description: Some("JPEG image data".to_string()),
            documentation: None,
            validation: None,
            metadata: None,
            extensions: vec!["jpg".to_string(), "jpeg".to_string()],
        });

        let resolved = resolve_media_urn("media:ext=jpeg;image", &registry)
            .await
            .unwrap();
        assert_eq!(
            resolved.extensions,
            vec!["jpg".to_string(), "jpeg".to_string()]
        );
        assert_eq!(resolved.extensions.len(), 2);
    }

    // TEST288: Documentation propagates from MediaDef through resolve_media_urn
    // into ResolvedMediaDef.
    //
    // This is the resolution path used by every consumer that asks the
    // registry for a media def — info panels, the cap navigator, the UI
    // — so a regression here makes the new field invisible everywhere.
    #[tokio::test]
    async fn test288_media_documentation_propagates_through_resolve() {
        let registry = test_registry().await;
        let body = "## Markdown body\n\nWith `code` and a [link](https://example.com).";
        registry.insert_cached_media_def_for_test(crate::StoredMediaDef {
            dims: Vec::new(),
            version: 0,
            urn: "media:doc-test;enc=utf-8".to_string(),
            media_type: "text/plain".to_string(),
            title: "Documented".to_string(),
            profile_uri: None,
            schema: None,
            description: Some("short desc".to_string()),
            documentation: Some(body.to_string()),
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        });

        let resolved = resolve_media_urn("media:doc-test;enc=utf-8", &registry)
            .await
            .unwrap();
        assert_eq!(
            resolved.documentation.as_deref(),
            Some(body),
            "documentation must propagate from MediaDef into ResolvedMediaDef"
        );
        // The short description must remain distinct from the long
        // documentation body — they are different fields with different
        // semantics, and the resolver must not collapse one into the other.
        assert_eq!(resolved.description.as_deref(), Some("short desc"));
    }

    // TEST289: MediaDef serializes documentation only when present and
    // round-trips losslessly. Mirrors TEST1127/1128 for the cap side.
    #[test]
    fn test289_media_def_def_documentation_round_trip() {
        let body = "Body with newline\nand backslash \\";
        let with_doc = MediaDef {
            dims: Vec::new(),
            urn: "media:rt-test".to_string(),
            media_type: "text/plain".to_string(),
            title: "Round Trip".to_string(),
            profile_uri: None,
            schema: None,
            description: None,
            documentation: Some(body.to_string()),
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        };
        let json = serde_json::to_string(&with_doc).unwrap();
        assert!(json.contains("\"documentation\""));
        let parsed: MediaDef = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.documentation.as_deref(), Some(body));

        let without_doc = MediaDef {
            urn: "media:rt-test-2".to_string(),
            media_type: "text/plain".to_string(),
            title: "No Doc".to_string(),
            profile_uri: None,
            schema: None,
            description: None,
            documentation: None,
            dims: Vec::new(),
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        };
        let json2 = serde_json::to_string(&without_doc).unwrap();
        assert!(
            !json2.contains("documentation"),
            "documentation must be omitted from MediaDef JSON when None, got: {}",
            json2
        );
    }

    // TEST1133: MediaDef set/clear lifecycle for documentation. Catches a
    // regression where the setter or clearer accidentally writes to or reads
    // from `description` (the short field) instead of `documentation` (the
    // long markdown body).
    #[test]
    fn test1133_media_def_def_documentation_lifecycle() {
        let mut spec = MediaDef {
            dims: Vec::new(),
            urn: "media:doc-test".to_string(),
            media_type: "text/plain".to_string(),
            title: "Doc Test".to_string(),
            profile_uri: None,
            schema: None,
            description: Some("short".to_string()),
            documentation: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        };
        assert!(spec.get_documentation().is_none());
        assert_eq!(spec.description.as_deref(), Some("short"));

        spec.set_documentation("body");
        assert_eq!(spec.get_documentation(), Some("body"));
        // setter must not touch description
        assert_eq!(spec.description.as_deref(), Some("short"));

        spec.clear_documentation();
        assert!(spec.get_documentation().is_none());
        // clearer must not touch description
        assert_eq!(spec.description.as_deref(), Some("short"));
    }

    // TEST1970: a media def's `dims` survive from the registry body through
    // StoredMediaDef to MediaDef and back, in order; a media def built from
    // no dim carries no `dims` key; an entry with a key this capdag does not
    // know is refused.
    #[test]
    fn test1970_media_def_dims_survive_and_refuse_unknown_keys() {
        use crate::fabric::registry::StoredMediaDef;
        let body = r#"{"urn":"media:document;ext=pdf","version":2,"media_type":"application/pdf","title":"PDF Document","documentation":"Only about PDFs.","dims":[{"dim":"file-type","value":"pdf"},{"dim":"medium","value":"document"}],"extensions":["pdf"]}"#;
        let stored: StoredMediaDef = serde_json::from_str(body).expect("a media def with dims parses");
        let expected = vec![
            crate::DimRef::new("file-type", "pdf"),
            crate::DimRef::new("medium", "document"),
        ];
        assert_eq!(stored.dims, expected);
        let def = stored.to_media_def_def();
        assert_eq!(def.dims, expected);
        let json = serde_json::to_value(&def).unwrap();
        assert_eq!(
            json["dims"],
            serde_json::json!([{"dim":"file-type","value":"pdf"},{"dim":"medium","value":"document"}])
        );
        let back: MediaDef = serde_json::from_value(json).unwrap();
        assert_eq!(back.dims, expected);

        let bare = MediaDef::new("media:void", "application/x-void", "Void");
        assert!(serde_json::to_value(&bare).unwrap().get("dims").is_none());

        let extra = body.replace(r#""value":"pdf"}"#, r#""value":"pdf","title":"PDF"}"#);
        let err = serde_json::from_str::<StoredMediaDef>(&extra).expect_err("an unknown dims key is refused");
        assert!(err.to_string().contains("title"), "{err}");
    }

    // TEST12835: the one decision of whether a value breaks a media
    // definition's validation. A rule applies to the values it is about and
    // passes the rest: min and max to numbers, the length rules to strings and
    // arrays, pattern and allowed values to strings. The first rule broken is
    // named; a pattern that is no regular expression is a defect of the
    // definition, not of the value.
    #[test]
    fn test12835_a_value_is_held_to_the_rules_that_are_about_it() {
        use serde_json::json;
        let broken = |validation: &MediaValidation, value: serde_json::Value| -> Option<String> {
            validation.violation(&value).expect("the rules can be applied")
        };
        let rule = |text: &str| Some(text.to_string());

        let range = MediaValidation { min: Some(1.0), max: Some(365.0), ..Default::default() };
        assert_eq!(broken(&range, json!(1)), None);
        assert_eq!(broken(&range, json!(365.0)), None);
        assert_eq!(broken(&range, json!(0)), rule("minimum value 1"));
        assert_eq!(broken(&range, json!(365.5)), rule("maximum value 365"));
        assert_eq!(broken(&range, json!(0.5)), rule("minimum value 1"));
        assert_eq!(broken(&range, json!(366u64)), rule("maximum value 365"));
        // A range says nothing of what is not a number.
        assert_eq!(broken(&range, json!("0")), None);
        assert_eq!(broken(&range, json!([0])), None);
        assert_eq!(broken(&range, json!(null)), None);
        assert_eq!(broken(&range, json!(false)), None, "a truth value is no number");

        let length = MediaValidation { min_length: Some(2), max_length: Some(3), ..Default::default() };
        assert_eq!(broken(&length, json!("ab")), None);
        assert_eq!(broken(&length, json!("abc")), None);
        assert_eq!(broken(&length, json!("a")), rule("minimum length 2"));
        assert_eq!(broken(&length, json!("abcd")), rule("maximum length 3"));
        // A string's length is its bytes.
        assert_eq!(broken(&length, json!("éé")), rule("maximum length 3"));
        // An array has a length too, and the rule says which it measured.
        assert_eq!(broken(&length, json!(["a", "b"])), None);
        assert_eq!(broken(&length, json!(["a"])), rule("minimum array length 2"));
        assert_eq!(broken(&length, json!([1, 2, 3, 4])), rule("maximum array length 3"));
        // A number has none.
        assert_eq!(broken(&length, json!(12345)), None);

        let pattern = MediaValidation { pattern: Some("^[a-z]+$".to_string()), ..Default::default() };
        assert_eq!(broken(&pattern, json!("abc")), None);
        assert_eq!(broken(&pattern, json!("abc1")), rule("pattern '^[a-z]+$'"));
        assert_eq!(broken(&pattern, json!(1)), None, "a pattern is about strings");
        // A pattern is searched for, not anchored unless it says so.
        let anywhere = MediaValidation { pattern: Some("b".to_string()), ..Default::default() };
        assert_eq!(broken(&anywhere, json!("abc")), None);
        let unreadable = MediaValidation { pattern: Some("(".to_string()), ..Default::default() };
        let defect = unreadable.violation(&json!("x")).expect_err("a pattern that is no regular expression");
        assert!(defect.contains("Invalid regex pattern '('"), "{defect}");
        // The defect is the definition's only when the rule is applied.
        assert_eq!(unreadable.violation(&json!(1)), Ok(None));

        let allowed = MediaValidation {
            allowed_values: Some(vec!["mean".to_string(), "median".to_string()]),
            ..Default::default()
        };
        assert_eq!(broken(&allowed, json!("median")), None);
        assert_eq!(broken(&allowed, json!("mode")), rule(r#"allowed values: ["mean", "median"]"#));
        assert_eq!(broken(&allowed, json!(3)), None, "allowed values are about strings");
        // A list that allows nothing is a rule that refuses every string.
        let nothing = MediaValidation {
            allowed_values: Some(Vec::new()),
            ..allowed.clone()
        };
        assert_eq!(broken(&nothing, json!("mean")), rule("allowed values: []"));

        // The first rule broken is the one named: the length before the
        // pattern, the pattern before the allowed values.
        let every = MediaValidation {
            min_length: Some(3),
            pattern: Some("^x".to_string()),
            allowed_values: Some(vec!["xyz".to_string()]),
            ..Default::default()
        };
        assert_eq!(broken(&every, json!("ab")), rule("minimum length 3"));
        assert_eq!(broken(&every, json!("abc")), rule("pattern '^x'"));
        assert_eq!(broken(&every, json!("xab")), rule(r#"allowed values: ["xyz"]"#));
        assert_eq!(broken(&every, json!("xyz")), None);
        // No rule, nothing broken.
        assert_eq!(broken(&MediaValidation::default(), json!("anything")), None);
    }
}
