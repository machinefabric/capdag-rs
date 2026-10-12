//! Cardinality and Structure Detection from Media URNs
//!
//! This module provides shape analysis for cap inputs and outputs across two orthogonal dimensions:
//!
//! ## 1. Cardinality (how many items)
//! Detected from the `list` marker tag:
//! - `media:document;ext=pdf` → Single (scalar, no list marker)
//! - `media:document;ext=pdf;list` → Sequence (array, has list marker)
//!
//! ## 2. Structure (internal shape of each item)
//! Detected from the `record` marker tag:
//! - `media:enc=utf-8` → Opaque (no internal fields, no record marker)
//! - `media:fmt=json;record` → Record (has key-value fields, record marker)
//!
//! ## The Four Combinations
//! | Cardinality | Structure | Example |
//! |-------------|-----------|---------|
//! | scalar | opaque | `media:enc=utf-8` - one string |
//! | scalar | record | `media:fmt=json;record` - one JSON object |
//! | list | opaque | `media:file-path;list` - array of paths |
//! | list | record | `media:fmt=json;list;record` - array of objects |
//!
//! Design principle: URN handling uses proper parsing via MediaUrn, never string comparison.

use crate::MediaUrn;
use serde::{Deserialize, Serialize};

/// Cardinality of cap inputs/outputs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputCardinality {
    /// Exactly 1 item (no list marker = scalar by default)
    Single,
    /// Array of items (has list marker)
    Sequence,
    /// 1 or more items (cap can handle either)
    AtLeastOne,
}

impl Default for InputCardinality {
    fn default() -> Self {
        Self::Single
    }
}

impl InputCardinality {
    /// Check if this cardinality accepts multiple items
    pub fn is_multiple(&self) -> bool {
        matches!(self, Self::Sequence | Self::AtLeastOne)
    }

    /// Check if this cardinality can accept a single item
    pub fn accepts_single(&self) -> bool {
        matches!(self, Self::Single | Self::AtLeastOne)
    }

    /// Check if cardinalities are compatible for data flow
    ///
    /// Returns true if data with `source` cardinality can flow into
    /// an input expecting `self` cardinality.
    pub fn is_compatible_with(&self, source: InputCardinality) -> CardinalityCompatibility {
        match (source, self) {
            (InputCardinality::Single, InputCardinality::Single) => {
                CardinalityCompatibility::Direct
            }
            (InputCardinality::Single, InputCardinality::Sequence) => {
                CardinalityCompatibility::WrapInArray
            }
            (InputCardinality::Sequence, InputCardinality::Single) => {
                CardinalityCompatibility::RequiresFanOut
            }
            (InputCardinality::Sequence, InputCardinality::Sequence) => {
                CardinalityCompatibility::Direct
            }
            (InputCardinality::AtLeastOne, _) | (_, InputCardinality::AtLeastOne) => {
                CardinalityCompatibility::Direct
            }
        }
    }

    /// Create a media URN with this cardinality from a base URN.
    ///
    /// DEPRECATED: Cardinality is tracked by is_sequence on the wire protocol,
    /// not by URN tags. This method is a no-op that returns the URN unchanged.
    /// Callers should stop using this and track cardinality separately.
    pub fn apply_to_urn(&self, base_urn: &str) -> String {
        // Validate the URN is parseable
        let _media_urn = MediaUrn::from_string(base_urn)
            .unwrap_or_else(|e| panic!("Invalid media URN in apply_to_urn: {} - {}", base_urn, e));

        // Cardinality does not change the URN — shape is tracked by is_sequence
        base_urn.to_string()
    }
}

/// Result of checking cardinality compatibility
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CardinalityCompatibility {
    /// Direct flow, no transformation needed
    Direct,
    /// Need to wrap single item in array
    WrapInArray,
    /// Need to fan-out: iterate over sequence, run for each item
    RequiresFanOut,
}

// =============================================================================
// Structure Dimension (record vs opaque)
// =============================================================================

/// Structure of media data - whether it has internal fields or is opaque
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputStructure {
    /// Indivisible, no internal fields we recognize (no record marker = opaque by default)
    /// Examples: raw bytes, a string, a number, binary blob
    Opaque,
    /// Has internal key-value fields (record marker present)
    /// Examples: JSON object, config file, CSV row
    Record,
}

impl Default for InputStructure {
    fn default() -> Self {
        Self::Opaque
    }
}

impl InputStructure {
    /// Parse structure from a media URN string.
    ///
    /// Uses the `record` marker tag to determine if this has internal fields.
    /// No record marker = opaque (default), record marker = record.
    pub fn from_media_urn(urn: &str) -> Self {
        match MediaUrn::from_string(urn) {
            Ok(media_urn) => {
                if media_urn.is_record() {
                    InputStructure::Record
                } else {
                    InputStructure::Opaque
                }
            }
            Err(_) => {
                // Invalid URN - fail hard, don't hide the issue
                panic!("Invalid media URN in structure detection: {}", urn);
            }
        }
    }

    /// Check if structures are compatible for data flow.
    ///
    /// Structure compatibility is strict - no coercion allowed:
    /// - Opaque → Opaque: Direct
    /// - Record → Record: Direct
    /// - Opaque → Record: Error (can't add structure)
    /// - Record → Opaque: Error (can't discard structure)
    pub fn is_compatible_with(&self, source: InputStructure) -> StructureCompatibility {
        match (source, self) {
            (InputStructure::Opaque, InputStructure::Opaque) => StructureCompatibility::Direct,
            (InputStructure::Record, InputStructure::Record) => StructureCompatibility::Direct,
            (InputStructure::Opaque, InputStructure::Record) => {
                StructureCompatibility::Incompatible("cannot add structure to opaque data")
            }
            (InputStructure::Record, InputStructure::Opaque) => {
                StructureCompatibility::Incompatible("cannot discard structure from record")
            }
        }
    }

    /// Create a media URN with this structure from a base URN
    pub fn apply_to_urn(&self, base_urn: &str) -> String {
        let media_urn = MediaUrn::from_string(base_urn)
            .unwrap_or_else(|e| panic!("Invalid media URN in apply_to_urn: {} - {}", base_urn, e));
        let has_record = media_urn.is_record();

        match self {
            InputStructure::Opaque => {
                if has_record {
                    // Remove record marker
                    media_urn.without_tag("record").to_string()
                } else {
                    base_urn.to_string()
                }
            }
            InputStructure::Record => {
                if has_record {
                    base_urn.to_string()
                } else {
                    // Add record marker (wildcard value)
                    media_urn
                        .with_tag("record", "*")
                        .unwrap_or_else(|e| panic!("Failed to add record marker: {}", e))
                        .to_string()
                }
            }
        }
    }
}

/// Result of checking structure compatibility
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StructureCompatibility {
    /// Direct flow, structures match
    Direct,
    /// Incompatible structures - this is an error
    Incompatible(&'static str),
}

impl StructureCompatibility {
    /// Check if this compatibility result is an error
    pub fn is_error(&self) -> bool {
        matches!(self, StructureCompatibility::Incompatible(_))
    }
}

// =============================================================================
// Combined Shape (cardinality + structure)
// =============================================================================

/// Complete shape of media data combining cardinality and structure
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaShape {
    /// Cardinality: scalar (Single) or list (Sequence)
    pub cardinality: InputCardinality,
    /// Structure: opaque or record
    pub structure: InputStructure,
}

impl Default for MediaShape {
    fn default() -> Self {
        Self {
            cardinality: InputCardinality::Single,
            structure: InputStructure::Opaque,
        }
    }
}

impl MediaShape {
    /// Parse structure from a media URN string.
    ///
    /// Cardinality defaults to Single — it comes from context (is_sequence),
    /// not from the URN. Only structure (Opaque vs Record) is derived from the URN.
    pub fn from_media_urn(urn: &str) -> Self {
        Self {
            cardinality: InputCardinality::Single,
            structure: InputStructure::from_media_urn(urn),
        }
    }

    /// Create scalar opaque shape (most common default)
    pub fn scalar_opaque() -> Self {
        Self {
            cardinality: InputCardinality::Single,
            structure: InputStructure::Opaque,
        }
    }

    /// Create scalar record shape
    pub fn scalar_record() -> Self {
        Self {
            cardinality: InputCardinality::Single,
            structure: InputStructure::Record,
        }
    }

    /// Create list opaque shape
    pub fn list_opaque() -> Self {
        Self {
            cardinality: InputCardinality::Sequence,
            structure: InputStructure::Opaque,
        }
    }

    /// Create list record shape
    pub fn list_record() -> Self {
        Self {
            cardinality: InputCardinality::Sequence,
            structure: InputStructure::Record,
        }
    }

    /// Check if shapes are compatible for data flow.
    ///
    /// Returns combined compatibility result considering both dimensions.
    pub fn is_compatible_with(&self, source: MediaShape) -> ShapeCompatibility {
        let cardinality_compat = self.cardinality.is_compatible_with(source.cardinality);
        let structure_compat = self.structure.is_compatible_with(source.structure);

        // Structure incompatibility is always an error
        if let StructureCompatibility::Incompatible(msg) = structure_compat {
            return ShapeCompatibility::Incompatible(msg);
        }

        // Structure is OK, return cardinality compatibility
        match cardinality_compat {
            CardinalityCompatibility::Direct => ShapeCompatibility::Direct,
            CardinalityCompatibility::WrapInArray => ShapeCompatibility::WrapInArray,
            CardinalityCompatibility::RequiresFanOut => ShapeCompatibility::RequiresFanOut,
        }
    }

    /// Apply this shape to a base URN
    pub fn apply_to_urn(&self, base_urn: &str) -> String {
        let with_cardinality = self.cardinality.apply_to_urn(base_urn);
        self.structure.apply_to_urn(&with_cardinality)
    }
}

/// Result of checking complete shape compatibility
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShapeCompatibility {
    /// Direct flow, no transformation needed
    Direct,
    /// Need to wrap single item in array (cardinality adjustment)
    WrapInArray,
    /// Need to fan-out: iterate over sequence (cardinality adjustment)
    RequiresFanOut,
    /// Incompatible structures - this is an error
    Incompatible(&'static str),
}

impl ShapeCompatibility {
    /// Check if this compatibility result is an error
    pub fn is_error(&self) -> bool {
        matches!(self, ShapeCompatibility::Incompatible(_))
    }

    /// Check if fan-out is required
    pub fn requires_fan_out(&self) -> bool {
        matches!(self, ShapeCompatibility::RequiresFanOut)
    }

    /// Check if wrap-in-array is needed
    pub fn requires_wrap(&self) -> bool {
        matches!(self, ShapeCompatibility::WrapInArray)
    }
}

/// Complete shape analysis for a cap transformation (cardinality + structure)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapShapeInfo {
    /// Input shape from cap's in_spec
    pub input: MediaShape,
    /// Output shape from cap's out_spec
    pub output: MediaShape,
    /// Cap URN this applies to
    pub cap_urn: String,
}

impl CapShapeInfo {
    /// Create shape info by parsing a cap's input and output specs.
    /// Cardinality defaults to Single — use `from_cap_specs_with_sequence`
    /// when is_sequence flags are known.
    pub fn from_cap_specs(cap_urn: &str, in_spec: &str, out_spec: &str) -> Self {
        Self {
            input: MediaShape::from_media_urn(in_spec),
            output: MediaShape::from_media_urn(out_spec),
            cap_urn: cap_urn.to_string(),
        }
    }

    /// Create shape info with explicit is_sequence flags from the Cap definition.
    /// This is the primary constructor — cardinality comes from is_sequence, not the URN.
    pub fn from_cap_specs_with_sequence(
        cap_urn: &str,
        in_spec: &str,
        out_spec: &str,
        input_is_sequence: bool,
        output_is_sequence: bool,
    ) -> Self {
        let mut input = MediaShape::from_media_urn(in_spec);
        let mut output = MediaShape::from_media_urn(out_spec);
        if input_is_sequence {
            input.cardinality = InputCardinality::Sequence;
        }
        if output_is_sequence {
            output.cardinality = InputCardinality::Sequence;
        }
        Self {
            input,
            output,
            cap_urn: cap_urn.to_string(),
        }
    }

    /// Describe the cardinality transformation pattern
    pub fn cardinality_pattern(&self) -> CardinalityPattern {
        match (self.input.cardinality, self.output.cardinality) {
            (InputCardinality::Single, InputCardinality::Single) => CardinalityPattern::OneToOne,
            (InputCardinality::Single, InputCardinality::Sequence) => CardinalityPattern::OneToMany,
            (InputCardinality::Sequence, InputCardinality::Single) => CardinalityPattern::ManyToOne,
            (InputCardinality::Sequence, InputCardinality::Sequence) => {
                CardinalityPattern::ManyToMany
            }
            (InputCardinality::AtLeastOne, InputCardinality::Single) => {
                CardinalityPattern::OneToOne
            }
            (InputCardinality::AtLeastOne, InputCardinality::Sequence) => {
                CardinalityPattern::OneToMany
            }
            (InputCardinality::Single, InputCardinality::AtLeastOne) => {
                CardinalityPattern::OneToOne
            }
            (InputCardinality::Sequence, InputCardinality::AtLeastOne) => {
                CardinalityPattern::ManyToMany
            }
            (InputCardinality::AtLeastOne, InputCardinality::AtLeastOne) => {
                CardinalityPattern::OneToOne
            }
        }
    }

    /// Check if input/output structures match
    pub fn structures_match(&self) -> bool {
        self.input.structure == self.output.structure
    }
}

/// Pattern describing input/output cardinality relationship
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CardinalityPattern {
    /// Single input → Single output (e.g., resize image)
    OneToOne,
    /// Single input → Multiple outputs (e.g., PDF to pages)
    OneToMany,
    /// Multiple inputs → Single output (e.g., merge PDFs)
    ManyToOne,
    /// Multiple inputs → Multiple outputs (e.g., batch process)
    ManyToMany,
}

impl CardinalityPattern {
    /// Check if this pattern may produce multiple outputs
    pub fn produces_vector(&self) -> bool {
        matches!(self, Self::OneToMany | Self::ManyToMany)
    }

    /// Check if this pattern requires multiple inputs
    pub fn requires_vector(&self) -> bool {
        matches!(self, Self::ManyToOne | Self::ManyToMany)
    }
}

/// Analyze shape chain for a sequence of caps (cardinality + structure)
#[derive(Debug, Clone)]
pub struct StrandShapeAnalysis {
    /// Per-cap shape info
    pub cap_infos: Vec<CapShapeInfo>,
    /// Points where fan-out is needed (index into cap_infos)
    pub fan_out_points: Vec<usize>,
    /// Points where fan-in/collect is needed (index into cap_infos)
    pub fan_in_points: Vec<usize>,
    /// Whether the chain is valid (no impossible transitions)
    pub is_valid: bool,
    /// Error message if chain is invalid
    pub error: Option<String>,
}

impl StrandShapeAnalysis {
    /// Analyze a chain of caps for shape transitions (cardinality + structure)
    ///
    /// This validates both:
    /// 1. Cardinality transitions (fan-out/fan-in requirements)
    /// 2. Structure compatibility (record/opaque must match)
    pub fn analyze(cap_infos: Vec<CapShapeInfo>) -> Self {
        if cap_infos.is_empty() {
            return Self {
                cap_infos: vec![],
                fan_out_points: vec![],
                fan_in_points: vec![],
                is_valid: true,
                error: None,
            };
        }

        let mut fan_out_points = Vec::new();
        let mut fan_in_points = Vec::new();
        let mut current_shape = cap_infos[0].input;
        let mut error_msg: Option<String> = None;

        for (i, info) in cap_infos.iter().enumerate() {
            let compatibility = info.input.is_compatible_with(current_shape);

            match compatibility {
                ShapeCompatibility::Direct => {}
                ShapeCompatibility::WrapInArray => {}
                ShapeCompatibility::RequiresFanOut => {
                    fan_out_points.push(i);
                }
                ShapeCompatibility::Incompatible(msg) => {
                    error_msg = Some(format!(
                        "Shape mismatch at cap {} ({}): {} - source has {:?}/{:?}, cap expects {:?}/{:?}",
                        i, info.cap_urn, msg,
                        current_shape.cardinality, current_shape.structure,
                        info.input.cardinality, info.input.structure
                    ));
                    break;
                }
            }

            current_shape = info.output;
        }

        if let Some(err) = error_msg {
            return Self {
                cap_infos,
                fan_out_points,
                fan_in_points,
                is_valid: false,
                error: Some(err),
            };
        }

        if !fan_out_points.is_empty() {
            fan_in_points.push(cap_infos.len());
        }

        Self {
            cap_infos,
            fan_out_points,
            fan_in_points,
            is_valid: true,
            error: None,
        }
    }

    /// Check if this chain requires any cardinality transformations
    pub fn requires_transformation(&self) -> bool {
        !self.fan_out_points.is_empty() || !self.fan_in_points.is_empty()
    }

    /// Get the final output shape of the chain
    pub fn final_output_shape(&self) -> Option<MediaShape> {
        self.cap_infos.last().map(|info| info.output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== InputCardinality Tests ====================
    // NOTE: InputCardinality::from_media_urn was removed — cardinality comes from
    // context (is_sequence on the wire), not from URN tags.

    // TEST688: Tests is_multiple method correctly identifies multi-value cardinalities
    // Verifies Single returns false while Sequence and AtLeastOne return true
    #[test]
    fn test688_is_multiple() {
        assert!(!InputCardinality::Single.is_multiple());
        assert!(InputCardinality::Sequence.is_multiple());
        assert!(InputCardinality::AtLeastOne.is_multiple());
    }

    // TEST689: Tests accepts_single method identifies cardinalities that accept single values
    // Verifies Single and AtLeastOne accept singles while Sequence does not
    #[test]
    fn test689_accepts_single() {
        assert!(InputCardinality::Single.accepts_single());
        assert!(!InputCardinality::Sequence.accepts_single());
        assert!(InputCardinality::AtLeastOne.accepts_single());
    }

    // ==================== Compatibility Tests ====================

    // TEST690: Tests cardinality compatibility for single-to-single data flow
    // Verifies Direct compatibility when both input and output are Single
    #[test]
    fn test690_compatibility_single_to_single() {
        assert_eq!(
            InputCardinality::Single.is_compatible_with(InputCardinality::Single),
            CardinalityCompatibility::Direct
        );
    }

    // TEST691: Tests cardinality compatibility when wrapping single value into array
    // Verifies WrapInArray compatibility when Sequence expects Single input
    #[test]
    fn test691_compatibility_single_to_vector() {
        assert_eq!(
            InputCardinality::Sequence.is_compatible_with(InputCardinality::Single),
            CardinalityCompatibility::WrapInArray
        );
    }

    // TEST692: Tests cardinality compatibility when unwrapping array to singles
    // Verifies RequiresFanOut compatibility when Single expects Sequence input
    #[test]
    fn test692_compatibility_vector_to_single() {
        assert_eq!(
            InputCardinality::Single.is_compatible_with(InputCardinality::Sequence),
            CardinalityCompatibility::RequiresFanOut
        );
    }

    // TEST693: Tests cardinality compatibility for sequence-to-sequence data flow
    // Verifies Direct compatibility when both input and output are Sequence
    #[test]
    fn test693_compatibility_vector_to_vector() {
        assert_eq!(
            InputCardinality::Sequence.is_compatible_with(InputCardinality::Sequence),
            CardinalityCompatibility::Direct
        );
    }

    // ==================== CapShapeInfo Cardinality Pattern Tests ====================

    // TEST697: Tests CapShapeInfo correctly identifies one-to-one pattern
    // Verifies Single input and Single output result in OneToOne pattern
    #[test]
    fn test697_cap_shape_info_one_to_one() {
        let info = CapShapeInfo::from_cap_specs("cap:test", "media:document;ext=pdf", "media:ext=png;image");
        assert_eq!(info.input.cardinality, InputCardinality::Single);
        assert_eq!(info.output.cardinality, InputCardinality::Single);
        assert_eq!(info.cardinality_pattern(), CardinalityPattern::OneToOne);
    }

    // TEST698: CapShapeInfo cardinality is always Single when derived from URN
    // Cardinality comes from context (is_sequence), not from URN tags.
    // The list tag is a semantic type property, not a cardinality indicator.
    #[test]
    fn test698_cap_shape_info_cardinality_always_single_from_urn() {
        let info =
            CapShapeInfo::from_cap_specs("cap:pdf-to-pages", "media:document;ext=pdf", "media:ext=png;list");
        assert_eq!(info.input.cardinality, InputCardinality::Single);
        assert_eq!(info.output.cardinality, InputCardinality::Single);
        assert_eq!(info.cardinality_pattern(), CardinalityPattern::OneToOne);
    }

    // TEST699: CapShapeInfo cardinality from URN is always Single; ManyToOne requires is_sequence
    #[test]
    fn test699_cap_shape_info_list_urn_still_single_cardinality() {
        // URN parsing always yields Single — the "list" tag is a structure marker, not cardinality
        let from_urn =
            CapShapeInfo::from_cap_specs("cap:merge-pdfs", "media:document;ext=pdf;list", "media:document;ext=pdf");
        assert_eq!(from_urn.input.cardinality, InputCardinality::Single);
        assert_eq!(from_urn.output.cardinality, InputCardinality::Single);
        assert_eq!(from_urn.cardinality_pattern(), CardinalityPattern::OneToOne);

        // With is_sequence=true on input, cardinality becomes ManyToOne
        let with_seq = CapShapeInfo::from_cap_specs_with_sequence(
            "cap:merge-pdfs",
            "media:document;ext=pdf;list",
            "media:document;ext=pdf",
            true,
            false,
        );
        assert_eq!(with_seq.input.cardinality, InputCardinality::Sequence);
        assert_eq!(with_seq.output.cardinality, InputCardinality::Single);
        assert_eq!(
            with_seq.cardinality_pattern(),
            CardinalityPattern::ManyToOne
        );
    }

    // ==================== CardinalityPattern Tests ====================

    // TEST709: Tests CardinalityPattern correctly identifies patterns that produce vectors
    // Verifies OneToMany and ManyToMany return true, others return false
    #[test]
    fn test709_pattern_produces_vector() {
        assert!(!CardinalityPattern::OneToOne.produces_vector());
        assert!(CardinalityPattern::OneToMany.produces_vector());
        assert!(!CardinalityPattern::ManyToOne.produces_vector());
        assert!(CardinalityPattern::ManyToMany.produces_vector());
    }

    // TEST710: Tests CardinalityPattern correctly identifies patterns that require vectors
    // Verifies ManyToOne and ManyToMany return true, others return false
    #[test]
    fn test710_pattern_requires_vector() {
        assert!(!CardinalityPattern::OneToOne.requires_vector());
        assert!(!CardinalityPattern::OneToMany.requires_vector());
        assert!(CardinalityPattern::ManyToOne.requires_vector());
        assert!(CardinalityPattern::ManyToMany.requires_vector());
    }

    // ==================== Shape Chain Analysis Tests ====================

    // TEST711: Tests shape chain analysis for simple linear one-to-one capability chains
    // Verifies chains with no fan-out are valid and require no transformation
    #[test]
    fn test711_strand_shape_analysis_simple_linear() {
        let infos = vec![
            CapShapeInfo::from_cap_specs("cap:pdf-to-png", "media:document;ext=pdf", "media:ext=png;image"),
            CapShapeInfo::from_cap_specs(
                "cap:resize",
                "media:ext=png;image",
                "media:ext=png;image",
            ),
        ];
        let analysis = StrandShapeAnalysis::analyze(infos);
        assert!(analysis.is_valid);
        assert!(analysis.fan_out_points.is_empty());
        assert!(!analysis.requires_transformation());
    }

    // TEST712: Tests shape chain analysis detects fan-out points in capability chains
    // Fan-out requires is_sequence=true on the cap's output, not a "list" URN tag
    #[test]
    fn test712_strand_shape_analysis_with_fan_out() {
        let infos = vec![
            CapShapeInfo::from_cap_specs_with_sequence(
                "cap:pdf-to-pages",
                "media:document;ext=pdf",
                "media:ext=png;image",
                false,
                true,
            ),
            CapShapeInfo::from_cap_specs(
                "cap:thumbnail",
                "media:ext=png;image",
                "media:ext=png;image",
            ),
        ];
        let analysis = StrandShapeAnalysis::analyze(infos);
        assert!(analysis.is_valid);
        assert_eq!(analysis.fan_out_points, vec![1]);
        assert!(analysis.requires_transformation());
    }

    // TEST713: Tests shape chain analysis handles empty capability chains correctly
    // Verifies empty chains are valid and require no transformation
    #[test]
    fn test713_strand_shape_analysis_empty() {
        let analysis = StrandShapeAnalysis::analyze(vec![]);
        assert!(analysis.is_valid);
        assert!(!analysis.requires_transformation());
    }

    // ==================== Serialization Tests ====================

    // TEST714: Tests InputCardinality serializes and deserializes correctly to/from JSON
    // Verifies JSON round-trip preserves cardinality values
    #[test]
    fn test714_cardinality_serialization() {
        let single = InputCardinality::Single;
        let json = serde_json::to_string(&single).unwrap();
        assert_eq!(json, "\"single\"");
        let deserialized: InputCardinality = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, InputCardinality::Single);
    }

    // TEST715: Tests CardinalityPattern serializes and deserializes correctly to/from JSON
    // Verifies JSON round-trip preserves pattern values with snake_case formatting
    #[test]
    fn test715_pattern_serialization() {
        let pattern = CardinalityPattern::OneToMany;
        let json = serde_json::to_string(&pattern).unwrap();
        assert_eq!(json, "\"one_to_many\"");
        let deserialized: CardinalityPattern = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, CardinalityPattern::OneToMany);
    }

    // ==================== InputStructure Tests ====================

    // TEST720: Tests InputStructure correctly identifies opaque media URNs
    // Verifies that URNs without record marker are parsed as Opaque
    #[test]
    fn test720_from_media_urn_opaque() {
        assert_eq!(
            InputStructure::from_media_urn("media:document;ext=pdf"),
            InputStructure::Opaque
        );
        assert_eq!(
            InputStructure::from_media_urn("media:enc=utf-8"),
            InputStructure::Opaque
        );
        assert_eq!(
            InputStructure::from_media_urn("media:integer"),
            InputStructure::Opaque
        );
        // List marker doesn't affect structure
        assert_eq!(
            InputStructure::from_media_urn("media:file-path;list"),
            InputStructure::Opaque
        );
    }

    // TEST721: Tests InputStructure correctly identifies record media URNs
    // Verifies that URNs with record marker tag are parsed as Record
    #[test]
    fn test721_from_media_urn_record() {
        assert_eq!(
            InputStructure::from_media_urn("media:fmt=json;record"),
            InputStructure::Record
        );
        assert_eq!(
            InputStructure::from_media_urn("media:enc=utf-8;record"),
            InputStructure::Record
        );
        assert_eq!(
            InputStructure::from_media_urn("media:enc=utf-8;file-metadata;record"),
            InputStructure::Record
        );
        // List of records
        assert_eq!(
            InputStructure::from_media_urn("media:fmt=json;list;record"),
            InputStructure::Record
        );
    }

    // TEST722: Tests structure compatibility for opaque-to-opaque data flow
    #[test]
    fn test722_structure_compatibility_opaque_to_opaque() {
        assert_eq!(
            InputStructure::Opaque.is_compatible_with(InputStructure::Opaque),
            StructureCompatibility::Direct
        );
    }

    // TEST723: Tests structure compatibility for record-to-record data flow
    #[test]
    fn test723_structure_compatibility_record_to_record() {
        assert_eq!(
            InputStructure::Record.is_compatible_with(InputStructure::Record),
            StructureCompatibility::Direct
        );
    }

    // TEST724: Tests structure incompatibility for opaque-to-record flow
    #[test]
    fn test724_structure_incompatibility_opaque_to_record() {
        let compat = InputStructure::Record.is_compatible_with(InputStructure::Opaque);
        assert!(compat.is_error());
        assert!(matches!(compat, StructureCompatibility::Incompatible(_)));
    }

    // TEST725: Tests structure incompatibility for record-to-opaque flow
    #[test]
    fn test725_structure_incompatibility_record_to_opaque() {
        let compat = InputStructure::Opaque.is_compatible_with(InputStructure::Record);
        assert!(compat.is_error());
        assert!(matches!(compat, StructureCompatibility::Incompatible(_)));
    }

    // TEST726: Tests applying Record structure adds record marker to URN
    #[test]
    fn test726_apply_structure_add_record() {
        let result = InputStructure::Record.apply_to_urn("media:fmt=json");
        assert!(result.contains("record"));
    }

    // TEST727: Tests applying Opaque structure removes record marker from URN
    #[test]
    fn test727_apply_structure_remove_record() {
        let result = InputStructure::Opaque.apply_to_urn("media:fmt=json;record");
        assert!(!result.contains("record"));
    }

    // ==================== MediaShape Tests ====================

    // TEST730: Tests MediaShape correctly parses all four combinations
    #[test]
    fn test730_media_shape_from_urn_all_combinations() {
        // Scalar opaque (default)
        let shape = MediaShape::from_media_urn("media:enc=utf-8");
        assert_eq!(shape.cardinality, InputCardinality::Single);
        assert_eq!(shape.structure, InputStructure::Opaque);

        // Scalar record
        let shape = MediaShape::from_media_urn("media:fmt=json;record");
        assert_eq!(shape.cardinality, InputCardinality::Single);
        assert_eq!(shape.structure, InputStructure::Record);

        // List opaque — cardinality is always Single from URN (shape comes from context)
        let shape = MediaShape::from_media_urn("media:file-path;list");
        assert_eq!(shape.cardinality, InputCardinality::Single);
        assert_eq!(shape.structure, InputStructure::Opaque);

        // List record — cardinality is always Single from URN (shape comes from context)
        let shape = MediaShape::from_media_urn("media:fmt=json;list;record");
        assert_eq!(shape.cardinality, InputCardinality::Single);
        assert_eq!(shape.structure, InputStructure::Record);
    }

    // TEST731: Tests MediaShape compatibility for matching shapes
    #[test]
    fn test731_media_shape_compatible_direct() {
        let scalar_opaque = MediaShape::scalar_opaque();
        let scalar_record = MediaShape::scalar_record();
        let list_opaque = MediaShape::list_opaque();
        let list_record = MediaShape::list_record();

        // Same shape = Direct
        assert_eq!(
            scalar_opaque.is_compatible_with(scalar_opaque),
            ShapeCompatibility::Direct
        );
        assert_eq!(
            scalar_record.is_compatible_with(scalar_record),
            ShapeCompatibility::Direct
        );
        assert_eq!(
            list_opaque.is_compatible_with(list_opaque),
            ShapeCompatibility::Direct
        );
        assert_eq!(
            list_record.is_compatible_with(list_record),
            ShapeCompatibility::Direct
        );
    }

    // TEST732: Tests MediaShape compatibility for cardinality changes with matching structure
    #[test]
    fn test732_media_shape_cardinality_changes() {
        let scalar_opaque = MediaShape::scalar_opaque();
        let list_opaque = MediaShape::list_opaque();
        let scalar_record = MediaShape::scalar_record();
        let list_record = MediaShape::list_record();

        // Scalar to list (same structure) = WrapInArray
        assert_eq!(
            list_opaque.is_compatible_with(scalar_opaque),
            ShapeCompatibility::WrapInArray
        );
        assert_eq!(
            list_record.is_compatible_with(scalar_record),
            ShapeCompatibility::WrapInArray
        );

        // List to scalar (same structure) = RequiresFanOut
        assert_eq!(
            scalar_opaque.is_compatible_with(list_opaque),
            ShapeCompatibility::RequiresFanOut
        );
        assert_eq!(
            scalar_record.is_compatible_with(list_record),
            ShapeCompatibility::RequiresFanOut
        );
    }

    // TEST733: Tests MediaShape incompatibility when structures don't match
    #[test]
    fn test733_media_shape_structure_mismatch() {
        let scalar_opaque = MediaShape::scalar_opaque();
        let scalar_record = MediaShape::scalar_record();
        let list_opaque = MediaShape::list_opaque();
        let list_record = MediaShape::list_record();

        // Structure mismatch = Incompatible (regardless of cardinality)
        assert!(scalar_record.is_compatible_with(scalar_opaque).is_error());
        assert!(scalar_opaque.is_compatible_with(scalar_record).is_error());
        assert!(list_record.is_compatible_with(list_opaque).is_error());
        assert!(list_opaque.is_compatible_with(list_record).is_error());

        // Cross cardinality + structure mismatch
        assert!(list_record.is_compatible_with(scalar_opaque).is_error());
        assert!(scalar_opaque.is_compatible_with(list_record).is_error());
    }

    // ==================== CapShapeInfo Tests ====================

    // TEST740: Tests CapShapeInfo correctly parses cap specs
    #[test]
    fn test740_cap_shape_info_from_specs() {
        let info =
            CapShapeInfo::from_cap_specs("cap:test", "media:enc=utf-8", "media:fmt=json;record");
        assert_eq!(info.input.cardinality, InputCardinality::Single);
        assert_eq!(info.input.structure, InputStructure::Opaque);
        assert_eq!(info.output.cardinality, InputCardinality::Single);
        assert_eq!(info.output.structure, InputStructure::Record);
    }

    // TEST741: Tests CapShapeInfo pattern detection — OneToMany requires output is_sequence=true
    #[test]
    fn test741_cap_shape_info_pattern() {
        let one_to_many = CapShapeInfo::from_cap_specs_with_sequence(
            "cap:disbind",
            "media:document;ext=pdf",
            "media:disbound-page;enc=utf-8",
            false,
            true,
        );
        assert_eq!(
            one_to_many.cardinality_pattern(),
            CardinalityPattern::OneToMany
        );
    }

    // ==================== StrandShapeAnalysis Tests ====================

    // TEST750: Tests shape chain analysis for valid chain with matching structures
    #[test]
    fn test750_strand_shape_valid() {
        let infos = vec![
            CapShapeInfo::from_cap_specs(
                "cap:resize",
                "media:ext=png;image",
                "media:ext=png;image",
            ),
            CapShapeInfo::from_cap_specs(
                "cap:compress",
                "media:ext=png;image",
                "media:ext=png;image",
            ),
        ];
        let analysis = StrandShapeAnalysis::analyze(infos);
        assert!(analysis.is_valid);
        assert!(analysis.error.is_none());
    }

    // TEST751: Tests shape chain analysis detects structure mismatch
    #[test]
    fn test751_strand_shape_structure_mismatch() {
        let infos = vec![
            CapShapeInfo::from_cap_specs("cap:extract", "media:document;ext=pdf", "media:enc=utf-8"),
            // This cap expects record but gets opaque - should fail
            CapShapeInfo::from_cap_specs("cap:parse", "media:fmt=json;record", "media:data;record"),
        ];
        let analysis = StrandShapeAnalysis::analyze(infos);
        assert!(!analysis.is_valid);
        assert!(analysis.error.is_some());
        assert!(analysis.error.as_ref().unwrap().contains("Shape mismatch"));
    }

    // TEST752: Tests shape chain analysis with fan-out (matching structures)
    // Fan-out requires output is_sequence=true on the disbind cap
    #[test]
    fn test752_strand_shape_with_fanout() {
        let infos = vec![
            CapShapeInfo::from_cap_specs_with_sequence(
                "cap:disbind",
                "media:document;ext=pdf",
                "media:enc=utf-8;page",
                false,
                true,
            ),
            CapShapeInfo::from_cap_specs(
                "cap:process",
                "media:enc=utf-8",
                "media:enc=utf-8;result",
            ),
        ];
        let analysis = StrandShapeAnalysis::analyze(infos);
        assert!(analysis.is_valid);
        assert!(analysis.requires_transformation());
        assert_eq!(analysis.fan_out_points, vec![1]);
    }

    // TEST753: Tests shape chain analysis correctly handles list-to-list record flow
    #[test]
    fn test753_strand_shape_list_record_to_list_record() {
        let infos = vec![
            CapShapeInfo::from_cap_specs(
                "cap:parse_csv",
                "media:fmt=csv",
                "media:fmt=json;list;record",
            ),
            CapShapeInfo::from_cap_specs(
                "cap:transform",
                "media:fmt=json;list;record",
                "media:result;list;record",
            ),
        ];
        let analysis = StrandShapeAnalysis::analyze(infos);
        assert!(analysis.is_valid);
        assert!(!analysis.requires_transformation());
    }
}
