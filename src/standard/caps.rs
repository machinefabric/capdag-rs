//! Standard capability definitions with arguments
//!
//! This module provides the standard capability definitions used across
//! all MACHFAB cartridges, including their formal argument specifications.
//! These definitions should match the TOML definitions in capfab/src/

use crate::urn::media_urn::{
    MEDIA_AVAILABILITY_OUTPUT,
    MEDIA_BOOLEAN,
    MEDIA_CAP_DEFINITION,
    MEDIA_CAP_URN,
    MEDIA_CONTENTS_OUTPUT,
    MEDIA_CSV,
    MEDIA_DECISION,
    MEDIA_DOWNLOAD_OUTPUT,
    MEDIA_EMBEDDING_VECTOR,
    MEDIA_EPUB,
    MEDIA_FABRIC_DEFVER,
    MEDIA_IDENTITY,
    MEDIA_INTEGER,
    MEDIA_JSON,
    MEDIA_JSON_LIST,
    MEDIA_JSON_LIST_RECORD,
    MEDIA_JSON_RECORD,
    MEDIA_JSON_SCHEMA,
    // Format conversion types (JSON, YAML, CSV variants)
    MEDIA_JSON_VALUE,
    MEDIA_LIST_OUTPUT,
    MEDIA_LOG,
    // Text format types
    MEDIA_MD,
    MEDIA_MEDIA_DEFINITION,
    MEDIA_MEDIA_URN,
    // CAPDAG output types
    MEDIA_MODEL_DIM,
    MEDIA_MODEL_REPO,
    // Semantic input types
    MEDIA_MODEL_SPEC,
    MEDIA_OBJECT,
    MEDIA_PATH_OUTPUT,
    // Document types
    MEDIA_PDF,
    MEDIA_PLAIN_TEXT,
    // Semantic media types
    MEDIA_PNG,
    MEDIA_RST,
    MEDIA_STATUS_OUTPUT,
    // Primitives (needed for coercion functions)
    MEDIA_STRING,
    // Bare text list type (no format tag)
    MEDIA_STRING_LIST,
    // Semantic output types
    MEDIA_TEXTABLE_PAGE,
    MEDIA_TXT,
    MEDIA_VOID,
    MEDIA_YAML_LIST,
    MEDIA_YAML_LIST_RECORD,
    MEDIA_YAML_RECORD,
    MEDIA_YAML_VALUE,
};
use crate::{Cap, CapOutput, CapUrn, CapUrnBuilder, FabricRegistry, FabricRegistryError};
use std::sync::Arc;

// =============================================================================
// STANDARD CAP URN CONSTANTS
// =============================================================================

/// Identity capability — the categorical identity morphism. MANDATORY in every capset.
/// Accepts any media type as input and returns the same runtime media type unchanged.
pub const CAP_IDENTITY: &str = "cap:effect=none";

/// Discard capability — the terminal morphism. Standard, NOT mandatory.
/// Accepts any media type as input and produces void output.
/// Canonical form: `in=` defaults to the top media URN (`media:`) and is
/// omitted; `effect` is `declared` (the default — the runtime output IS the
/// declared `out=media:void`, nothing is inferred) and is likewise omitted.
/// The capdag lib provides a default implementation; cartridges may override.
pub const CAP_DISCARD: &str = "cap:out=media:void";

/// Adapter selection capability — content inspection for file type detection.
/// Standard, NOT mandatory. Every cartridge gets a default implementation that
/// returns empty END (no match). Cartridges that inspect file content override
/// this with a handler that returns `{"media_urns": [...]}`.
pub const CAP_ADAPTER_SELECTION: &str =
    "cap:in=\"media:\";out=\"media:adapter-selection;fmt=json;record\"";

/// Cap that resolves a canonical cap URN to its full flattened cap definition
/// by fetching the registry-published entry. Implemented by the engine-bundled
/// `fetchcartridge`. Sandboxed cartridges peer-invoke this when they need
/// to read a cap's `default_value` or any other registry-only field.
pub const CAP_LOOKUP_CAP_FABRIC: &str =
    "cap:in=\"media:cap-urn;enc=utf-8\";fabric;lookup-cap;out=\"media:cap-definition;fmt=json;record\"";

/// Cap that resolves a canonical media URN to its full media definition
/// by fetching the registry-published entry. Implemented by the engine-bundled
/// `fetchcartridge`. Sandboxed cartridges peer-invoke this when they need
/// to read a media def's title, MIME type, profile URI, or any other
/// registry-only field.
pub const CAP_LOOKUP_MEDIA_DEF_FABRIC: &str =
    "cap:in=\"media:enc=utf-8;media-urn\";fabric;lookup-media-def;out=\"media:fmt=json;media-definition;record\"";

/// Parse and return the canonical `cap:lookup-cap;fabric` `CapUrn`.
pub fn lookup_cap_fabric_urn() -> CapUrn {
    CapUrn::from_string(CAP_LOOKUP_CAP_FABRIC)
        .unwrap_or_else(|e| panic!("BUG: CAP_LOOKUP_CAP_FABRIC constant is invalid: {}", e))
}

/// Parse and return the canonical `cap:lookup-media-def;fabric` `CapUrn`.
pub fn lookup_media_def_fabric_urn() -> CapUrn {
    CapUrn::from_string(CAP_LOOKUP_MEDIA_DEF_FABRIC).unwrap_or_else(|e| {
        panic!(
            "BUG: CAP_LOOKUP_MEDIA_DEF_FABRIC constant is invalid: {}",
            e
        )
    })
}

/// Construct the canonical `cap:lookup-cap;fabric` `Cap` definition.
///
/// Mirrors the `fabric/caps/lookup-cap-fabric.toml` source-of-truth so that
/// the cartridge can declare it in its manifest without duplicating the wire
/// shape across two places.
pub fn lookup_cap_fabric_cap() -> Cap {
    let urn = lookup_cap_fabric_urn();
    let mut cap = Cap::with_description(
        urn,
        "Look Up Cap Definition (Fabric)".to_string(),
        vec!["lookup-cap-fabric".to_string()],
        "Resolve a canonical cap URN to its full registry-published cap definition by \
         fetching from the public fabric registry."
            .to_string(),
    );
    cap.args
        .push(crate::cap::definition::CapArg::with_description(
            MEDIA_CAP_URN,
            true,
            vec![
                crate::cap::definition::ArgSource::Stdin {
                    stdin: MEDIA_CAP_URN.to_string(),
                },
                crate::cap::definition::ArgSource::Position { position: 0 },
            ],
            "Canonical cap URN to look up.".to_string(),
        ));
    // Optional per-def version. A caller that pins a definition version
    // names it here; absent, the lookup cartridge resolves the URN's
    // defver under its own pinned fabric manifest. There is no
    // unversioned path: 0 is not a version.
    cap.args
        .push(crate::cap::definition::CapArg::with_description(
            MEDIA_FABRIC_DEFVER,
            false,
            vec![crate::cap::definition::ArgSource::CliFlag {
                cli_flag: "--defver".to_string(),
            }],
            "Per-definition version under the caller's manifest snapshot, 1 or greater. Absent ⇒ resolved under the fabric manifest the lookup cartridge is pinned to; a URN that manifest does not list is not found."
                .to_string(),
        ));
    cap.set_output(crate::cap::definition::CapOutput::new(
        MEDIA_CAP_DEFINITION,
        "Full flattened cap definition as published in the registry.",
    ));
    cap
}

/// Construct the canonical `cap:lookup-media-def;fabric` `Cap` definition.
///
/// Mirrors `fabric/caps/lookup-media-def-fabric.toml`.
pub fn lookup_media_def_fabric_cap() -> Cap {
    let urn = lookup_media_def_fabric_urn();
    let mut cap = Cap::with_description(
        urn,
        "Look Up Media Definition (Fabric)".to_string(),
        vec!["lookup-media-def-fabric".to_string()],
        "Resolve a canonical media URN to its full registry-published media def \
         definition by fetching from the public fabric registry."
            .to_string(),
    );
    cap.args
        .push(crate::cap::definition::CapArg::with_description(
            MEDIA_MEDIA_URN,
            true,
            vec![
                crate::cap::definition::ArgSource::Stdin {
                    stdin: MEDIA_MEDIA_URN.to_string(),
                },
                crate::cap::definition::ArgSource::Position { position: 0 },
            ],
            "Canonical media URN to look up.".to_string(),
        ));
    cap.args
        .push(crate::cap::definition::CapArg::with_description(
            MEDIA_FABRIC_DEFVER,
            false,
            vec![crate::cap::definition::ArgSource::CliFlag {
                cli_flag: "--defver".to_string(),
            }],
            "Per-definition version under the caller's manifest snapshot, 1 or greater. Absent ⇒ resolved under the fabric manifest the lookup cartridge is pinned to; a URN that manifest does not list is not found."
                .to_string(),
        ));
    cap.set_output(crate::cap::definition::CapOutput::new(
        MEDIA_MEDIA_DEFINITION,
        "Full media definition as published in the registry.",
    ));
    cap
}

/// Parse and return the canonical identity `CapUrn` from `CAP_IDENTITY`.
pub fn identity_urn() -> CapUrn {
    CapUrn::from_string(CAP_IDENTITY)
        .unwrap_or_else(|e| panic!("BUG: CAP_IDENTITY constant is invalid: {}", e))
}

/// Parse and return the canonical discard `CapUrn` from `CAP_DISCARD`.
pub fn discard_urn() -> CapUrn {
    CapUrn::from_string(CAP_DISCARD)
        .unwrap_or_else(|e| panic!("BUG: CAP_DISCARD constant is invalid: {}", e))
}

/// Parse and return the canonical adapter selection `CapUrn` from `CAP_ADAPTER_SELECTION`.
pub fn adapter_selection_urn() -> CapUrn {
    CapUrn::from_string(CAP_ADAPTER_SELECTION)
        .unwrap_or_else(|e| panic!("BUG: CAP_ADAPTER_SELECTION constant is invalid: {}", e))
}

/// Construct the canonical Identity `Cap` definition.
///
/// The identity cap declares one wildcard input arg
/// (`media:`) that any concrete media URN conforms to. This
/// makes the resolver's source-to-cap-arg matching trivially
/// succeed for identity: any source URN feeds the wildcard
/// arg with specificity-distance equal to the source's own
/// specificity. Without this arg the resolver would fail
/// with `UnmatchedSourceInCapArgs` because an empty args list
/// has no candidate slot for any source.
pub fn identity_cap() -> Cap {
    let urn = identity_urn();

    let mut cap = Cap::with_description(
        urn,
        "Identity".to_string(),
        vec!["identity".to_string()],
        "The categorical identity morphism. Echoes input as output unchanged. Mandatory in every capability set.".to_string(),
    );

    cap.args.push(crate::cap::definition::CapArg::new(
        "media:",
        true,
        vec![crate::cap::definition::ArgSource::Stdin {
            stdin: "media:".to_string(),
        }],
    ));
    cap.set_output(crate::cap::definition::CapOutput::new(
        "media:",
        "The input data, unchanged",
    ));
    cap
}

/// Construct the canonical Discard `Cap` definition.
///
/// Like the identity cap, discard declares a wildcard input
/// arg (`media:`) so the resolver's source-to-cap-arg
/// matching can route any source URN through it. The output
/// is `media:void`.
pub fn discard_cap() -> Cap {
    let urn = discard_urn();

    let mut cap = Cap::with_description(
        urn,
        "Discard".to_string(),
        vec!["discard".to_string()],
        "The terminal morphism. Accepts any input and produces void output. Standard but not mandatory.".to_string(),
    );

    cap.args.push(crate::cap::definition::CapArg::new(
        "media:",
        true,
        vec![crate::cap::definition::ArgSource::Stdin {
            stdin: "media:".to_string(),
        }],
    ));
    cap.set_output(crate::cap::definition::CapOutput::new(
        MEDIA_VOID,
        "Void (no output)",
    ));
    cap
}

/// Construct the canonical Adapter Selection `Cap` definition.
///
/// The adapter selection cap declares a wildcard input arg
/// (`media:`) so it can accept any file content for inspection.
/// The output is `media:adapter-selection;fmt=json;record` — a JSON
/// object with `media_urns` listing the media URNs the adapter
/// is confident match the inspected file content.
///
/// The default implementation (AdapterSelectionOp) returns an
/// empty END frame, meaning "I don't handle this file type".
/// Cartridges that inspect file content override this handler.
pub fn adapter_selection_cap() -> Cap {
    use crate::urn::media_urn::MEDIA_ADAPTER_SELECTION;

    let urn = adapter_selection_urn();

    let mut cap = Cap::with_description(
        urn,
        "Adapter Selection".to_string(),
        vec!["adapter-selection".to_string()],
        "Content inspection adapter. Inspects file content and returns media URNs matching the detected file type. Default returns empty END (no match).".to_string(),
    );

    cap.args.push(crate::cap::definition::CapArg::new(
        "media:",
        true,
        vec![crate::cap::definition::ArgSource::Stdin {
            stdin: "media:".to_string(),
        }],
    ));
    cap.set_output(crate::cap::definition::CapOutput::new(
        MEDIA_ADAPTER_SELECTION,
        "JSON object with media_urns array identifying the detected file type",
    ));
    cap
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================


// =============================================================================
// URN BUILDER FUNCTIONS (synchronous, return CapUrn directly)
// =============================================================================
// These are the SINGLE SOURCE OF TRUTH for URN construction.
// All _cap functions below MUST use these to build URNs.

// -----------------------------------------------------------------------------
// LLM URN BUILDERS
// -----------------------------------------------------------------------------

/// Build URN for generic text-generation capability.
pub fn llm_generate_text_urn() -> CapUrn {
    CapUrnBuilder::new()
        .marker("generate-text")
        .marker("llm")
        .marker("ml-model")
        .in_spec(MEDIA_STRING)
        .out_spec(MEDIA_PLAIN_TEXT)
        .build()
        .expect("Failed to build generate_text cap URN")
}

// -----------------------------------------------------------------------------
// EMBEDDING URN BUILDERS
// -----------------------------------------------------------------------------

/// Build URN for embeddings-dimensions capability
/// Output uses MEDIA_MODEL_DIM per CATALOG: media:integer;model-dim;numeric
pub fn embeddings_dimensions_urn() -> CapUrn {
    CapUrnBuilder::new()
        .marker("embeddings-dimensions")
        .in_spec(MEDIA_MODEL_SPEC)
        .out_spec(MEDIA_MODEL_DIM)
        .build()
        .expect("Failed to build embeddings-dimensions cap URN")
}

/// Build URN for text embeddings-generation capability
/// Input: media:enc=utf-8 (text)
/// Output: media:embedding-vector;enc=utf-8;record
pub fn embeddings_generation_urn() -> CapUrn {
    CapUrnBuilder::new()
        .marker("generate-embeddings")
        .in_spec(MEDIA_STRING)
        .out_spec(MEDIA_EMBEDDING_VECTOR)
        .build()
        .expect("Failed to build embeddings-generation cap URN")
}

/// Build URN for image embeddings-generation capability
/// Input: media:ext=png;image
/// Output: media:embedding-vector;enc=utf-8;record
pub fn image_embeddings_generation_urn() -> CapUrn {
    CapUrnBuilder::new()
        .marker("generate-image-embeddings")
        .marker("ml-model")
        .marker("candle")
        .in_spec(MEDIA_PNG)
        .out_spec(MEDIA_EMBEDDING_VECTOR)
        .build()
        .expect("Failed to build image-embeddings-generation cap URN")
}

// -----------------------------------------------------------------------------
// MODEL MANAGEMENT URN BUILDERS
// -----------------------------------------------------------------------------

/// Build URN for model-download capability
pub fn model_download_urn() -> CapUrn {
    CapUrnBuilder::new()
        .marker("download-model")
        .in_spec(MEDIA_MODEL_SPEC)
        .out_spec(MEDIA_DOWNLOAD_OUTPUT)
        .build()
        .expect("Failed to build model-download cap URN")
}

/// Build URN for model-list capability
/// Input uses MEDIA_MODEL_REPO per CATALOG: media:enc=utf-8;model-repo;record
pub fn model_list_urn() -> CapUrn {
    CapUrnBuilder::new()
        .marker("list-models")
        .in_spec(MEDIA_MODEL_REPO)
        .out_spec(MEDIA_LIST_OUTPUT)
        .build()
        .expect("Failed to build model-list cap URN")
}

/// Build URN for model-status capability
pub fn model_status_urn() -> CapUrn {
    CapUrnBuilder::new()
        .marker("model-status")
        .in_spec(MEDIA_MODEL_SPEC)
        .out_spec(MEDIA_STATUS_OUTPUT)
        .build()
        .expect("Failed to build model-status cap URN")
}

/// Build URN for model-contents capability
pub fn model_contents_urn() -> CapUrn {
    CapUrnBuilder::new()
        .marker("model-contents")
        .in_spec(MEDIA_MODEL_SPEC)
        .out_spec(MEDIA_CONTENTS_OUTPUT)
        .build()
        .expect("Failed to build model-contents cap URN")
}

/// Build URN for model-availability capability
pub fn model_availability_urn() -> CapUrn {
    CapUrnBuilder::new()
        .marker("model-availability")
        .in_spec(MEDIA_MODEL_SPEC)
        .out_spec(MEDIA_AVAILABILITY_OUTPUT)
        .build()
        .expect("Failed to build model-availability cap URN")
}

/// Build URN for model-path capability
pub fn model_path_urn() -> CapUrn {
    CapUrnBuilder::new()
        .marker("model-path")
        .in_spec(MEDIA_MODEL_SPEC)
        .out_spec(MEDIA_PATH_OUTPUT)
        .build()
        .expect("Failed to build model-path cap URN")
}

// -----------------------------------------------------------------------------
// DOCUMENT PROCESSING URN BUILDERS
// -----------------------------------------------------------------------------

/// Build URN for page-image rendering capability.
///
/// `input_media` is the media URN for the input type (e.g., MEDIA_PDF, MEDIA_IDENTITY).
/// Output is always a PNG page image.
pub fn render_page_image_urn(input_media: &str) -> CapUrn {
    CapUrnBuilder::new()
        .marker("render-page-image")
        .in_spec(input_media)
        .out_spec(MEDIA_PNG)
        .build()
        .expect("Failed to build render_page_image cap URN")
}

/// Build URN for disbind capability.
///
/// `input_media` is the media URN for the input type (e.g., MEDIA_PDF, MEDIA_TXT).
pub fn disbind_urn(input_media: &str) -> CapUrn {
    CapUrnBuilder::new()
        .marker("disbind")
        .in_spec(input_media)
        .out_spec(MEDIA_TEXTABLE_PAGE)
        .build()
        .expect("Failed to build disbind cap URN")
}

// -----------------------------------------------------------------------------
// TEXT PROCESSING URN BUILDERS
// -----------------------------------------------------------------------------

/// Build URN for generate-json capability.
///
/// Takes text content as its data-flow input and a caller-supplied JSON
/// schema (via `--schema`), runs schema-constrained LLM generation, returns
/// a JSON object guaranteed to validate against the schema. See
/// `capfab/src/caps/generate-json-en.toml` for the full contract.
pub fn generate_json_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .marker("generate-json")
        .tag("language", lang_code)
        .marker("constrained")
        .in_spec(MEDIA_STRING)
        .out_spec(MEDIA_JSON)
        .build()
        .expect("Failed to build generate-json cap URN")
}

/// Build URN for the `same` semantic-equivalence capability.
/// Two items + optional context in; a semantic-judgment record
/// (`{same, confidence, reason}`) out. The first of the
/// semantic-primitive family (docs/semantic-primitives.md).
pub fn same_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .marker("same")
        .tag("language", lang_code)
        .marker("constrained")
        .in_spec(MEDIA_STRING)
        .out_spec(crate::urn::media_urn::MEDIA_SEMANTIC_JUDGMENT)
        .build()
        .expect("Failed to build same cap URN")
}

/// Shared shape of the semantic-primitive judgment cap URNs
/// (docs/semantic-primitives.md): a bare marker + language +
/// constrained, text in, judgment-envelope out.
fn semantic_judgment_urn(marker: &str, lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .marker(marker)
        .tag("language", lang_code)
        .marker("constrained")
        .in_spec(MEDIA_STRING)
        .out_spec(crate::urn::media_urn::MEDIA_SEMANTIC_JUDGMENT)
        .build()
        .unwrap_or_else(|e| panic!("Failed to build {} cap URN: {}", marker, e))
}

/// Build URN for the `classify` closed-set labeling capability.
pub fn classify_urn(lang_code: &str) -> CapUrn {
    semantic_judgment_urn("classify", lang_code)
}

/// Build URN for the `score` rubric-scoring capability.
pub fn score_urn(lang_code: &str) -> CapUrn {
    semantic_judgment_urn("score", lang_code)
}

/// Build URN for the `verify` requirements-check capability.
pub fn verify_urn(lang_code: &str) -> CapUrn {
    semantic_judgment_urn("verify", lang_code)
}

/// Build URN for the `route` dispatch-judgment capability.
pub fn route_urn(lang_code: &str) -> CapUrn {
    semantic_judgment_urn("route", lang_code)
}

/// Build URN for the `normalize` entity-canonicalization capability.
pub fn normalize_urn(lang_code: &str) -> CapUrn {
    semantic_judgment_urn("normalize", lang_code)
}

/// Build URN for the `extract` schema-guided extraction capability
/// (the judgment-envelope generalization of `generate-json`).
pub fn extract_urn(lang_code: &str) -> CapUrn {
    semantic_judgment_urn("extract", lang_code)
}

/// Build URN for the `ask` grounded question-answering capability.
pub fn ask_urn(lang_code: &str) -> CapUrn {
    semantic_judgment_urn("ask", lang_code)
}

/// Build URN for the `explain` opaque-data root-cause capability.
pub fn explain_urn(lang_code: &str) -> CapUrn {
    semantic_judgment_urn("explain", lang_code)
}

/// Build URN for the `summarize` purpose-driven compression
/// capability. Unlike the judgment caps its output is finalised
/// plain text (the summary itself), not a judgment record.
pub fn summarize_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .marker("summarize")
        .tag("language", lang_code)
        .marker("constrained")
        .in_spec(MEDIA_STRING)
        .out_spec(crate::urn::media_urn::MEDIA_PLAIN_TEXT)
        .build()
        .expect("Failed to build summarize cap URN")
}

/// Build URN for make-decision capability
/// Output is MEDIA_DECISION: media:decision;fmt=json;record
pub fn make_decision_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .marker("make-decision")
        .tag("language", lang_code)
        .marker("constrained")
        .in_spec(MEDIA_STRING)
        .out_spec(MEDIA_DECISION)
        .build()
        .expect("Failed to build make-decision cap URN")
}

/// Build URN for make-multiple-decisions capability
/// Output is MEDIA_DECISION with is_sequence=true (one decision per question).
pub fn make_multiple_decisions_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .marker("make-multiple-decisions")
        .tag("language", lang_code)
        .marker("constrained")
        .in_spec(MEDIA_STRING)
        .out_spec(MEDIA_DECISION)
        .build()
        .expect("Failed to build make-multiple-decisions cap URN")
}

// -----------------------------------------------------------------------------
// MACHFAB-SPECIFIC TASK URN BUILDERS
// -----------------------------------------------------------------------------
// Note: These are legitimate task capabilities for document analysis workflows.
// They represent phases of document processing, NOT tool wrappers.

/// Build URN for recategorization-task capability
/// Input: binary document data
/// Output: categorization result object
pub fn recategorization_task_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .marker("recategorize-listing")
        .tag("language", lang_code)
        .in_spec(MEDIA_IDENTITY) // Binary document
        .out_spec(MEDIA_OBJECT) // Categorization results
        .build()
        .expect("Failed to build recategorization-task cap URN")
}

/// Build URN for listing-analysis-task capability
/// Input: binary document data
/// Output: analysis result object
pub fn listing_analysis_task_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .marker("request-listing-analysis")
        .tag("language", lang_code)
        .in_spec(MEDIA_IDENTITY) // Binary document
        .out_spec(MEDIA_OBJECT) // Analysis results
        .build()
        .expect("Failed to build listing-analysis-task cap URN")
}

// -----------------------------------------------------------------------------
// COERCION URN BUILDERS
// -----------------------------------------------------------------------------
// Coercion is converting data from one media type to another.
// Each coercion is a cap with a specific input and output type.

/// Build URN for coercing any type to string
/// Input: source data (any text type)
/// Output: string representation
pub fn coerce_to_string_urn(source_type: &str) -> CapUrn {
    coercion_urn(source_type, "string")
}

/// Build URN for coercing to integer
/// Input: source data (numeric or parseable string)
/// Output: integer
pub fn coerce_to_integer_urn(source_type: &str) -> CapUrn {
    coercion_urn(source_type, "integer")
}

/// Build URN for coercing to number
/// Input: source data (numeric or parseable string)
/// Output: number
pub fn coerce_to_number_urn(source_type: &str) -> CapUrn {
    coercion_urn(source_type, "number")
}

/// Build URN for coercing to object
/// Input: any data type
/// Output: JSON object (possibly wrapped)
pub fn coerce_to_object_urn(source_type: &str) -> CapUrn {
    coercion_urn(source_type, "object")
}

/// Map a type name to its full media URN constant
pub fn media_urn_for_type(type_name: &str) -> &'static str {
    match type_name {
        "string" => MEDIA_STRING,
        "integer" => MEDIA_INTEGER,
        "number" => crate::urn::media_urn::MEDIA_NUMBER,
        "boolean" => MEDIA_BOOLEAN,
        "object" => MEDIA_OBJECT,
        "string-array" => crate::urn::media_urn::MEDIA_STRING_LIST,
        "integer-array" => crate::urn::media_urn::MEDIA_INTEGER_LIST,
        "number-array" => crate::urn::media_urn::MEDIA_NUMBER_LIST,
        "boolean-array" => crate::urn::media_urn::MEDIA_BOOLEAN_LIST,
        "object-array" => crate::urn::media_urn::MEDIA_OBJECT_LIST,
        other => panic!("Unknown media type: {}. Valid types are: string, integer, number, boolean, object, string-array, integer-array, number-array, boolean-array, object-array", other),
    }
}

/// Build a generic coercion URN given source and target types
/// Panics if source_type or target_type is not a known media type
pub fn coercion_urn(source_type: &str, target_type: &str) -> CapUrn {
    let in_spec = media_urn_for_type(source_type);
    let out_spec = media_urn_for_type(target_type);
    CapUrnBuilder::new()
        .marker("coerce")
        .in_spec(in_spec)
        .out_spec(out_spec)
        .build()
        .expect("Failed to build coercion cap URN")
}

/// Get list of all valid coercion paths
/// Returns (source_type, target_type) pairs for all supported coercions
pub fn all_coercion_paths() -> Vec<(&'static str, &'static str)> {
    vec![
        // To string (from all text types)
        ("integer", "string"),
        ("number", "string"),
        ("boolean", "string"),
        ("object", "string"),
        ("string-array", "string"),
        ("integer-array", "string"),
        ("number-array", "string"),
        ("boolean-array", "string"),
        ("object-array", "string"),
        // To integer
        ("string", "integer"),
        ("number", "integer"),
        ("boolean", "integer"),
        // To number
        ("string", "number"),
        ("integer", "number"),
        ("boolean", "number"),
        // To boolean (strict spellings — see datacartridge's
        // coerce_to_boolean for the accepted set)
        ("string", "boolean"),
        ("integer", "boolean"),
        ("number", "boolean"),
        // To object (wrap in object)
        ("string", "object"),
        ("integer", "object"),
        ("number", "object"),
        ("boolean", "object"),
    ]
}

// -----------------------------------------------------------------------------
// FORMAT CONVERSION URN BUILDERS
// -----------------------------------------------------------------------------
// Format conversion is transforming data between JSON, YAML, and CSV formats.
// Each conversion is a cap with a specific input and output media type.

/// Build a format conversion URN for a specific input → output media type pair.
/// All format conversions use op="convert_format".
pub fn format_conversion_urn(in_media: &str, out_media: &str) -> CapUrn {
    CapUrnBuilder::new()
        .marker("convert-format")
        .in_spec(in_media)
        .out_spec(out_media)
        .build()
        .expect("Failed to build format conversion cap URN")
}

/// A format conversion path with authoritative title and description.
pub struct FormatConversionPath {
    pub in_media: &'static str,
    pub out_media: &'static str,
    pub title: &'static str,
    pub description: &'static str,
}

/// All valid format conversion paths between JSON, YAML, CSV, and text lists.
pub fn all_format_conversion_paths() -> Vec<FormatConversionPath> {
    vec![
        // JSON <-> YAML value
        FormatConversionPath {
            in_media: MEDIA_JSON_VALUE,
            out_media: MEDIA_YAML_VALUE,
            title: "Convert JSON Value to YAML",
            description: "Convert a JSON scalar value to YAML format",
        },
        FormatConversionPath {
            in_media: MEDIA_YAML_VALUE,
            out_media: MEDIA_JSON_VALUE,
            title: "Convert YAML Value to JSON",
            description: "Convert a YAML scalar value to JSON format",
        },
        // JSON <-> YAML record
        FormatConversionPath {
            in_media: MEDIA_JSON_RECORD,
            out_media: MEDIA_YAML_RECORD,
            title: "Convert JSON Object to YAML Mapping",
            description: "Convert a JSON object to a YAML mapping",
        },
        FormatConversionPath {
            in_media: MEDIA_YAML_RECORD,
            out_media: MEDIA_JSON_RECORD,
            title: "Convert YAML Mapping to JSON Object",
            description: "Convert a YAML mapping to a JSON object",
        },
        // JSON <-> YAML list
        FormatConversionPath {
            in_media: MEDIA_JSON_LIST,
            out_media: MEDIA_YAML_LIST,
            title: "Convert JSON Array to YAML Sequence",
            description: "Convert a JSON array to a YAML sequence",
        },
        FormatConversionPath {
            in_media: MEDIA_YAML_LIST,
            out_media: MEDIA_JSON_LIST,
            title: "Convert YAML Sequence to JSON Array",
            description: "Convert a YAML sequence to a JSON array",
        },
        // JSON <-> YAML list of records
        FormatConversionPath {
            in_media: MEDIA_JSON_LIST_RECORD,
            out_media: MEDIA_YAML_LIST_RECORD,
            title: "Convert JSON Array of Objects to YAML List of Mappings",
            description: "Convert a JSON array of objects to a YAML list of mappings",
        },
        FormatConversionPath {
            in_media: MEDIA_YAML_LIST_RECORD,
            out_media: MEDIA_JSON_LIST_RECORD,
            title: "Convert YAML List of Mappings to JSON Array of Objects",
            description: "Convert a YAML list of mappings to a JSON array of objects",
        },
        // JSON list of records <-> CSV
        FormatConversionPath {
            in_media: MEDIA_JSON_LIST_RECORD,
            out_media: MEDIA_CSV,
            title: "Convert JSON Array of Objects to CSV",
            description: "Convert a JSON array of objects to CSV format",
        },
        FormatConversionPath {
            in_media: MEDIA_CSV,
            out_media: MEDIA_JSON_LIST_RECORD,
            title: "Convert CSV to JSON Array of Objects",
            description: "Convert CSV data to a JSON array of objects",
        },
        // YAML list of records <-> CSV
        FormatConversionPath {
            in_media: MEDIA_YAML_LIST_RECORD,
            out_media: MEDIA_CSV,
            title: "Convert YAML List of Mappings to CSV",
            description: "Convert a YAML list of mappings to CSV format",
        },
        FormatConversionPath {
            in_media: MEDIA_CSV,
            out_media: MEDIA_YAML_LIST_RECORD,
            title: "Convert CSV to YAML List of Mappings",
            description: "Convert CSV data to a YAML list of mappings",
        },
        // Text list <-> JSON list
        FormatConversionPath {
            in_media: MEDIA_STRING_LIST,
            out_media: MEDIA_JSON_LIST,
            title: "Convert Text List to JSON Array",
            description: "Convert a list of text values to a JSON array",
        },
        FormatConversionPath {
            in_media: MEDIA_JSON_LIST,
            out_media: MEDIA_STRING_LIST,
            title: "Convert JSON Array to Text List",
            description: "Convert a JSON array to a list of text values",
        },
        // Text list <-> YAML list
        FormatConversionPath {
            in_media: MEDIA_STRING_LIST,
            out_media: MEDIA_YAML_LIST,
            title: "Convert Text List to YAML Sequence",
            description: "Convert a list of text values to a YAML sequence",
        },
        FormatConversionPath {
            in_media: MEDIA_YAML_LIST,
            out_media: MEDIA_STRING_LIST,
            title: "Convert YAML Sequence to Text List",
            description: "Convert a YAML sequence to a list of text values",
        },
        // Text list <-> CSV
        FormatConversionPath {
            in_media: MEDIA_STRING_LIST,
            out_media: MEDIA_CSV,
            title: "Convert Text List to CSV",
            description: "Convert a list of text values to CSV format",
        },
        FormatConversionPath {
            in_media: MEDIA_CSV,
            out_media: MEDIA_STRING_LIST,
            title: "Convert CSV to Text List",
            description: "Convert CSV data to a list of text values",
        },
    ]
}

// =============================================================================
// REGISTRY LOOKUP FUNCTIONS (async, return Cap from registry)
// =============================================================================
// These functions use the _urn functions above to build URNs, then look up
// the capability from the registry.

// -----------------------------------------------------------------------------
// LLM CAPABILITIES
// -----------------------------------------------------------------------------

/// Get generic text-generation cap from registry.
pub async fn llm_generate_text_cap(
    registry: Arc<FabricRegistry>,
) -> Result<Cap, FabricRegistryError> {
    let urn = llm_generate_text_urn();
    registry.get_cap(&urn.to_string()).await
}

// -----------------------------------------------------------------------------
// EMBEDDING CAPABILITIES
// -----------------------------------------------------------------------------

/// Get embeddings-dimensions cap from registry
pub async fn embeddings_dimensions_cap(
    registry: Arc<FabricRegistry>,
) -> Result<Cap, FabricRegistryError> {
    let urn = embeddings_dimensions_urn();
    registry.get_cap(&urn.to_string()).await
}

/// Get text embeddings-generation cap from registry
pub async fn embeddings_generation_cap(
    registry: Arc<FabricRegistry>,
) -> Result<Cap, FabricRegistryError> {
    let urn = embeddings_generation_urn();
    registry.get_cap(&urn.to_string()).await
}

/// Get image embeddings-generation cap from registry
pub async fn image_embeddings_generation_cap(
    registry: Arc<FabricRegistry>,
) -> Result<Cap, FabricRegistryError> {
    let urn = image_embeddings_generation_urn();
    registry.get_cap(&urn.to_string()).await
}

// -----------------------------------------------------------------------------
// MODEL MANAGEMENT CAPABILITIES
// -----------------------------------------------------------------------------

/// Get model download cap from registry
pub async fn model_download_cap(registry: Arc<FabricRegistry>) -> Result<Cap, FabricRegistryError> {
    let urn = model_download_urn();
    registry.get_cap(&urn.to_string()).await
}

/// Get model list cap from registry
pub async fn model_list_cap(registry: Arc<FabricRegistry>) -> Result<Cap, FabricRegistryError> {
    let urn = model_list_urn();
    registry.get_cap(&urn.to_string()).await
}

/// Get model status cap from registry
pub async fn model_status_cap(registry: Arc<FabricRegistry>) -> Result<Cap, FabricRegistryError> {
    let urn = model_status_urn();
    registry.get_cap(&urn.to_string()).await
}

/// Get model contents cap from registry
pub async fn model_contents_cap(registry: Arc<FabricRegistry>) -> Result<Cap, FabricRegistryError> {
    let urn = model_contents_urn();
    registry.get_cap(&urn.to_string()).await
}

/// Get model availability cap from registry
pub async fn model_availability_cap(
    registry: Arc<FabricRegistry>,
) -> Result<Cap, FabricRegistryError> {
    let urn = model_availability_urn();
    registry.get_cap(&urn.to_string()).await
}

/// Get model path cap from registry
pub async fn model_path_cap(registry: Arc<FabricRegistry>) -> Result<Cap, FabricRegistryError> {
    let urn = model_path_urn();
    registry.get_cap(&urn.to_string()).await
}

// -----------------------------------------------------------------------------
// DOCUMENT PROCESSING CAPABILITIES
// -----------------------------------------------------------------------------

/// Get page-image rendering cap from registry.
pub async fn render_page_image_cap(
    registry: Arc<FabricRegistry>,
    input_media: &str,
) -> Result<Cap, FabricRegistryError> {
    let urn = render_page_image_urn(input_media);
    registry.get_cap(&urn.to_string()).await
}

/// Get disbind cap from registry
pub async fn disbind_cap(
    registry: Arc<FabricRegistry>,
    input_media: &str,
) -> Result<Cap, FabricRegistryError> {
    let urn = disbind_urn(input_media);
    registry.get_cap(&urn.to_string()).await
}

// -----------------------------------------------------------------------------
// TEXT PROCESSING CAPABILITIES
// -----------------------------------------------------------------------------

/// Get generate-json cap from registry
pub async fn generate_json_cap(
    registry: Arc<FabricRegistry>,
    lang_code: &str,
) -> Result<Cap, FabricRegistryError> {
    let urn = generate_json_urn(lang_code);
    registry.get_cap(&urn.to_string()).await
}

/// Get make-decision cap from registry
pub async fn make_decision_cap(
    registry: Arc<FabricRegistry>,
    lang_code: &str,
) -> Result<Cap, FabricRegistryError> {
    let urn = make_decision_urn(lang_code);
    registry.get_cap(&urn.to_string()).await
}

/// Get make-multiple-decisions cap from registry
pub async fn make_multiple_decisions_cap(
    registry: Arc<FabricRegistry>,
    lang_code: &str,
) -> Result<Cap, FabricRegistryError> {
    let urn = make_multiple_decisions_urn(lang_code);
    registry.get_cap(&urn.to_string()).await
}

// -----------------------------------------------------------------------------
// COERCION CAPABILITIES
// -----------------------------------------------------------------------------

/// Get a single coercion cap from registry
pub async fn coercion_cap(
    registry: Arc<FabricRegistry>,
    source_type: &str,
    target_type: &str,
) -> Result<Cap, FabricRegistryError> {
    let urn = coercion_urn(source_type, target_type);
    registry.get_cap(&urn.to_string()).await
}

/// Get all coercion caps from registry
/// Returns a vector of (source_type, target_type, Cap) tuples
/// Fails if any coercion cap is missing from the registry
pub async fn all_coercion_caps(
    registry: Arc<FabricRegistry>,
) -> Result<Vec<(&'static str, &'static str, Cap)>, FabricRegistryError> {
    let mut caps = Vec::new();
    for (source_type, target_type) in all_coercion_paths() {
        let cap = coercion_cap(registry.clone(), source_type, target_type).await?;
        caps.push((source_type, target_type, cap));
    }
    Ok(caps)
}

// -----------------------------------------------------------------------------
// FORMAT CONVERSION CAPABILITIES
// -----------------------------------------------------------------------------

/// Get a single format conversion cap from the registry
pub async fn format_conversion_cap(
    registry: Arc<FabricRegistry>,
    in_media: &str,
    out_media: &str,
) -> Result<Cap, FabricRegistryError> {
    let urn = format_conversion_urn(in_media, out_media);
    registry.get_cap(&urn.to_string()).await
}

/// Get all format conversion caps from the registry
/// Returns a vector of (in_media, out_media, Cap) tuples
/// Fails if any conversion cap is missing from the registry
pub async fn all_format_conversion_caps(
    registry: Arc<FabricRegistry>,
) -> Result<Vec<(&'static str, &'static str, Cap)>, FabricRegistryError> {
    let mut caps = Vec::new();
    for path in all_format_conversion_paths() {
        let cap = format_conversion_cap(registry.clone(), path.in_media, path.out_media).await?;
        caps.push((path.in_media, path.out_media, cap));
    }
    Ok(caps)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::standard::media::MEDIA_STRING;
    use crate::urn::media_urn::{MEDIA_AVAILABILITY_OUTPUT, MEDIA_MODEL_SPEC, MEDIA_PATH_OUTPUT};

    // TEST307: Test model_availability_urn builds valid cap URN with correct op and media defs
    #[test]
    fn test307_model_availability_urn() {
        let urn = model_availability_urn();
        assert!(
            urn.has_marker_tag("model-availability"),
            "URN must have model-availability marker"
        );
        assert_eq!(urn.in_spec(), MEDIA_MODEL_SPEC, "input must be model-spec");
        assert_eq!(
            urn.out_spec(),
            MEDIA_AVAILABILITY_OUTPUT,
            "output must be availability output"
        );
    }

    // TEST308: Test model_path_urn builds valid cap URN with correct op and media defs
    #[test]
    fn test308_model_path_urn() {
        let urn = model_path_urn();
        assert!(
            urn.has_marker_tag("model-path"),
            "URN must have model-path marker"
        );
        assert_eq!(urn.in_spec(), MEDIA_MODEL_SPEC, "input must be model-spec");
        assert_eq!(
            urn.out_spec(),
            MEDIA_PATH_OUTPUT,
            "output must be path output"
        );
    }

    // TEST309: Test model_availability_urn and model_path_urn produce distinct URNs
    #[test]
    fn test309_model_availability_and_path_are_distinct() {
        let avail = model_availability_urn();
        let path = model_path_urn();
        assert_ne!(
            avail.to_string(),
            path.to_string(),
            "availability and path must be distinct cap URNs"
        );
    }

    // TEST310: llm_generate_text_urn() produces a valid cap URN with a
    // UTF-8 text input and plain-text terminal output.
    #[test]
    fn test310_llm_generate_text_urn_shape() {
        use crate::urn::media_urn::MediaUrn;
        let urn = llm_generate_text_urn();

        let in_spec = MediaUrn::from_string(urn.in_spec()).expect("in_spec must parse");
        let out_spec = MediaUrn::from_string(urn.out_spec()).expect("out_spec must parse");
        let expected_in = MediaUrn::from_string(MEDIA_STRING).expect("MEDIA_STRING must parse");
        let expected_out =
            MediaUrn::from_string(MEDIA_PLAIN_TEXT).expect("MEDIA_PLAIN_TEXT must parse");

        assert!(
            urn.has_marker_tag("generate-text"),
            "must have generate-text marker"
        );
        assert!(urn.has_marker_tag("llm"), "must have llm tag");
        assert!(urn.has_marker_tag("ml-model"), "must have ml-model tag");
        assert!(
            in_spec.conforms_to(&expected_in).unwrap(),
            "in_spec '{}' must match MEDIA_STRING '{}'",
            urn.in_spec(),
            MEDIA_STRING
        );
        assert!(
            out_spec.conforms_to(&expected_out).unwrap(),
            "out_spec '{}' must match MEDIA_PLAIN_TEXT '{}'",
            urn.out_spec(),
            MEDIA_PLAIN_TEXT
        );
    }

    // TEST312: Test all URN builders produce parseable cap URNs
    #[test]
    fn test312_all_urn_builders_produce_valid_urns() {
        // Each of these must not panic
        let _avail = model_availability_urn();
        let _path = model_path_urn();
        let _conv = llm_generate_text_urn();

        // Verify they roundtrip through CapUrn parsing
        let avail_str = model_availability_urn().to_string();
        let parsed = crate::urn::cap_urn::CapUrn::from_string(&avail_str);
        assert!(
            parsed.is_ok(),
            "model_availability_urn must be parseable: {:?}",
            parsed.err()
        );

        let path_str = model_path_urn().to_string();
        let parsed = crate::urn::cap_urn::CapUrn::from_string(&path_str);
        assert!(
            parsed.is_ok(),
            "model_path_urn must be parseable: {:?}",
            parsed.err()
        );
    }

    // TEST473: CAP_DISCARD parses as valid CapUrn with in=media: and out=media:void
    #[test]
    fn test473_cap_discard_parses_as_valid_urn() {
        use crate::urn::cap_urn::CapUrn;
        use crate::urn::media_urn::MEDIA_VOID;

        let urn = CapUrn::from_string(CAP_DISCARD).expect("CAP_DISCARD must parse");
        assert_eq!(
            urn.in_spec(),
            "media:",
            "CAP_DISCARD input must be wildcard media:"
        );
        assert_eq!(
            urn.out_spec(),
            MEDIA_VOID,
            "CAP_DISCARD output must be media:void"
        );
        // The constant is the canonical rendering: default in= (media:) and
        // default effect (declared) are omitted; only out=media:void remains.
        assert_eq!(
            urn.to_string(),
            CAP_DISCARD,
            "CAP_DISCARD must be the canonical rendering of the discard cap"
        );
        // Surface spellings with the defaults written out normalize to it.
        let explicit = CapUrn::from_string("cap:in=media:;out=media:void")
            .expect("explicit surface spelling must parse");
        assert_eq!(
            explicit.to_string(),
            CAP_DISCARD,
            "explicit in=media: surface form must normalize to CAP_DISCARD"
        );
    }

    // TEST474: CAP_DISCARD accepts specific-input/void-output caps
    #[test]
    fn test474_cap_discard_accepts_specific_void_cap() {
        use crate::urn::cap_urn::CapUrn;

        let discard = CapUrn::from_string(CAP_DISCARD).expect("CAP_DISCARD must parse");
        let specific = CapUrn::from_string("cap:in=\"media:ext=pdf\";shred;out=\"media:void\"")
            .expect("specific cap must parse");

        // discard (pattern) accepts specific (instance)? No — discard has no op tag,
        // but the specific cap has op=shred. As pattern, discard accepts instances
        // that are at least as specific. The specific cap IS more specific.
        // As instance, does the specific cap conform to the discard pattern?
        // specific.conforms_to(discard) == discard.accepts(specific)
        assert!(
            discard.accepts(&specific),
            "CAP_DISCARD must accept a more specific cap with void output"
        );

        // But a cap with non-void output must NOT conform to discard
        let non_void = CapUrn::from_string("cap:in=\"media:ext=pdf\";convert;out=\"media:string\"")
            .expect("non-void cap must parse");
        assert!(
            !discard.accepts(&non_void),
            "CAP_DISCARD must NOT accept a cap with non-void output"
        );
    }

    // TEST605: all_coercion_paths each entry builds a valid parseable CapUrn
    #[test]
    fn test605_all_coercion_paths_build_valid_urns() {
        let paths = all_coercion_paths();
        assert!(!paths.is_empty(), "Coercion paths must not be empty");

        for (source, target) in &paths {
            let urn = coercion_urn(source, target);
            assert!(
                urn.has_marker_tag("coerce"),
                "Coercion URN for {}→{} must have coerce marker",
                source,
                target
            );

            // Verify roundtrip through string parsing
            let urn_str = urn.to_string();
            let reparsed = crate::urn::cap_urn::CapUrn::from_string(&urn_str);
            assert!(
                reparsed.is_ok(),
                "Coercion URN for {}→{} must roundtrip through parsing: {:?}",
                source,
                target,
                reparsed.err()
            );
        }
    }

    // TEST606: coercion_urn in/out specs match the type's media URN constant
    #[test]
    fn test606_coercion_urn_specs() {
        use crate::urn::media_urn::MediaUrn;

        let urn = coercion_urn("string", "integer");
        // in_spec should conform to MEDIA_STRING
        let in_urn = MediaUrn::from_string(urn.in_spec()).expect("in_spec should parse");
        let expected_in = MediaUrn::from_string(MEDIA_STRING).expect("MEDIA_STRING should parse");
        assert!(
            in_urn.conforms_to(&expected_in).unwrap(),
            "in_spec '{}' should conform to '{}'",
            urn.in_spec(),
            MEDIA_STRING
        );

        // out_spec should conform to MEDIA_INTEGER
        let out_urn = MediaUrn::from_string(urn.out_spec()).expect("out_spec should parse");
        let expected_out =
            MediaUrn::from_string(MEDIA_INTEGER).expect("MEDIA_INTEGER should parse");
        assert!(
            out_urn.conforms_to(&expected_out).unwrap(),
            "out_spec '{}' should conform to '{}'",
            urn.out_spec(),
            MEDIA_INTEGER
        );
    }

    // TEST850: all_format_conversion_paths each entry builds a valid parseable CapUrn
    #[test]
    fn test850_all_format_conversion_paths_build_valid_urns() {
        let paths = all_format_conversion_paths();
        assert_eq!(paths.len(), 18, "Expected 18 format conversion paths");

        for path in &paths {
            let urn = format_conversion_urn(path.in_media, path.out_media);
            assert!(
                urn.has_marker_tag("convert-format"),
                "Format conversion URN for {}→{} must have convert-format marker",
                path.in_media,
                path.out_media
            );

            // Verify roundtrip through string parsing
            let urn_str = urn.to_string();
            let reparsed = crate::urn::cap_urn::CapUrn::from_string(&urn_str);
            assert!(
                reparsed.is_ok(),
                "Format conversion URN for {}→{} must roundtrip through parsing: {:?}",
                path.in_media,
                path.out_media,
                reparsed.err()
            );
        }
    }

    // TEST851: format_conversion_urn in/out specs match the input constants
    #[test]
    fn test851_format_conversion_urn_specs() {
        use crate::urn::media_urn::MediaUrn;

        let urn = format_conversion_urn(MEDIA_JSON_VALUE, MEDIA_YAML_VALUE);
        let in_urn = MediaUrn::from_string(urn.in_spec()).expect("in_spec should parse");
        let expected_in =
            MediaUrn::from_string(MEDIA_JSON_VALUE).expect("MEDIA_JSON_VALUE should parse");
        assert!(
            in_urn.conforms_to(&expected_in).unwrap(),
            "in_spec '{}' should conform to '{}'",
            urn.in_spec(),
            MEDIA_JSON_VALUE
        );

        let out_urn = MediaUrn::from_string(urn.out_spec()).expect("out_spec should parse");
        let expected_out =
            MediaUrn::from_string(MEDIA_YAML_VALUE).expect("MEDIA_YAML_VALUE should parse");
        assert!(
            out_urn.conforms_to(&expected_out).unwrap(),
            "out_spec '{}' should conform to '{}'",
            urn.out_spec(),
            MEDIA_YAML_VALUE
        );
    }

    // =========================================================================
    // Adapter Selection Cap Tests
    // =========================================================================

    // TEST1272: CAP_ADAPTER_SELECTION constant parses as a valid CapUrn
    #[test]
    fn test1272_adapter_cap_constant_parses() {
        let urn = CapUrn::from_string(CAP_ADAPTER_SELECTION);
        assert!(
            urn.is_ok(),
            "CAP_ADAPTER_SELECTION must be a valid cap URN: {:?}",
            urn.err()
        );
    }

    // TEST1273: adapter_selection_urn() returns a valid CapUrn with correct in/out specs
    #[test]
    fn test1273_adapter_selection_urn_builder() {
        let urn = adapter_selection_urn();
        // in_spec should be bare "media:" (accepts any)
        assert_eq!(urn.in_spec(), "media:");
        // out_spec should be the adapter selection media URN
        let out = urn.out_spec();
        let out_urn = crate::MediaUrn::from_string(out).unwrap();
        let expected_out =
            crate::MediaUrn::from_string(crate::urn::media_urn::MEDIA_ADAPTER_SELECTION).unwrap();
        assert!(
            out_urn.conforms_to(&expected_out).unwrap(),
            "out_spec '{}' should conform to adapter-selection URN",
            out
        );
    }

    // TEST1274: adapter_selection_cap() builds a valid Cap with correct args and output
    #[test]
    fn test1274_adapter_selection_cap_builder() {
        let cap = adapter_selection_cap();
        assert_eq!(cap.title, "Adapter Selection");
        assert_eq!(cap.primary_alias(), "adapter-selection");

        // Must have exactly one arg (wildcard media: input)
        assert_eq!(cap.args.len(), 1, "Adapter selection cap must have one arg");
        assert_eq!(cap.args[0].media_urn, "media:");
        assert!(cap.args[0].required, "The input arg must be required");

        // Must have output
        let output = cap
            .output
            .as_ref()
            .expect("Adapter selection cap must have output");
        assert_eq!(
            output.media_urn,
            crate::urn::media_urn::MEDIA_ADAPTER_SELECTION
        );
    }

    // TEST1275: A cap whose output is adapter-selection can dispatch adapter-selection requests;
    // identity (wildcard output) cannot, because wildcard output cannot satisfy a specific output requirement.
    #[test]
    fn test1275_adapter_selection_dispatchable_by_specific_candidate() {
        let adapter_request = adapter_selection_urn();

        // A candidate that outputs exactly adapter-selection media can dispatch the request
        let specific_candidate = CapUrn::from_string(CAP_ADAPTER_SELECTION).unwrap();
        assert!(
            specific_candidate.is_dispatchable(&adapter_request),
            "A cap with adapter-selection output must be dispatchable for adapter-selection requests"
        );

        // Identity has wildcard output (media:) — cannot guarantee adapter-selection output
        let identity = identity_urn();
        assert!(
            !identity.is_dispatchable(&adapter_request),
            "Identity (wildcard output) must NOT dispatch adapter-selection requests: \
             wildcard output cannot satisfy a specific output requirement"
        );
    }
}
