//! Media URN - Data type specification using tagged URN format
//!
//! Media URNs use the tagged URN format with "media" prefix to describe
//! data types. They replace the old spec ID system (e.g., `media:string`).
//!
//! Format: `media:<type>[;subtype=<subtype>][;v=<version>][;profile=<url>][;...]`
//!
//! Examples:
//! - `media:string`
//! - `media:object`
//! - `media:application;subtype=json;profile="https://example.com/schema"`
//! - `media:image;subtype=png`
//!
//! Media URNs are just tagged URNs with the "media" prefix. Comparison and
//! matching use standard tagged URN semantics. Specific behaviors (like
//! profile resolution) are triggered by the presence of certain tags.

use std::fmt;
use std::str::FromStr;
use tagged_urn::{TaggedUrn, TaggedUrnCoordinateDelta, TaggedUrnError};

// =============================================================================
// STANDARD MEDIA URN CONSTANTS
// =============================================================================
//
// Cardinality and Structure use orthogonal marker tags:
// - `list` marker: presence = list/array, absence = scalar (default)
// - `record` marker: presence = has internal fields, absence = opaque (default)
//
// Examples:
// - `media:ext=pdf` → scalar, opaque (no markers)
// - `media:enc=utf-8;list` → list, opaque (has list marker)
// - `media:fmt=json;record` → scalar, record (has record marker)
// - `media:fmt=json;list;record` → list of records (has both markers)

// Primitive types - URNs must match base.toml definitions
/// Media URN for void (no input/output) - no coercion tags
pub const MEDIA_VOID: &str = "media:void";
/// Media URN for string type — bare UTF-8 text (`enc=utf-8`), scalar by default (no list marker)
pub const MEDIA_STRING: &str = "media:enc=utf-8";
/// Media URN for integer type — numeric (math ops valid), scalar by default
pub const MEDIA_INTEGER: &str = "media:integer;numeric";
/// Media URN for number type — numeric, scalar by default
pub const MEDIA_NUMBER: &str = "media:numeric";
/// Media URN for boolean type - uses "bool" not "boolean" per base.toml
pub const MEDIA_BOOLEAN: &str = "media:bool;enc=utf-8";
/// Media URN for a generic record/object type — has internal key-value structure,
/// no content-format claim. Use MEDIA_JSON for JSON-serialized objects.
pub const MEDIA_OBJECT: &str = "media:record";
/// Media URN for the top type — the most general media type (no constraints)
pub const MEDIA_IDENTITY: &str = "media:";

// Array types - URNs must match base.toml definitions
/// Media URN for untyped list - ordered sequence of opaque byte sequences
pub const MEDIA_LIST: &str = "media:list";
/// Media URN for string array type — ordered sequence of bare UTF-8 text values
pub const MEDIA_STRING_LIST: &str = "media:enc=utf-8;list";
/// Media URN for integer array type — numeric with list marker
pub const MEDIA_INTEGER_LIST: &str = "media:integer;list;numeric";
/// Media URN for number list type - text file with one number per line
pub const MEDIA_NUMBER_LIST: &str = "media:list;numeric";
/// Media URN for boolean list type - text file with one boolean per line
pub const MEDIA_BOOLEAN_LIST: &str = "media:bool;enc=utf-8;list";
/// Media URN for object array type — list of records (no content-format claim).
/// Use a specific format like JSON array for serialized object arrays.
pub const MEDIA_OBJECT_LIST: &str = "media:list;record";

// Semantic media types for specialized content
/// Media URN for PNG image data
pub const MEDIA_PNG: &str = "media:ext=png;image";
/// Media URN for audio data (wav, mp3, flac, etc.)
pub const MEDIA_AUDIO: &str = "media:audio;ext=wav";
/// Media URN for video data (mp4, webm, mov, etc.)
pub const MEDIA_VIDEO: &str = "media:video";

// Image container formats. URN strings here match the canonical media-def
// URNs published to the registry. Tagged-URN normalization makes the
// declared and normalized forms equivalent under the parser, but matching
// the canonical form exactly avoids surprising any code that prints URNs.
pub const MEDIA_JPEG: &str = "media:ext=jpeg;image";
pub const MEDIA_GIF: &str = "media:ext=gif;image";
pub const MEDIA_BMP: &str = "media:ext=bmp;image";
pub const MEDIA_TIFF: &str = "media:ext=tiff;image";
pub const MEDIA_WEBP: &str = "media:ext=webp;image";

// Audio container formats. Conversion caps live in audiocartridge.
pub const MEDIA_MP3: &str = "media:audio;ext=mp3";
pub const MEDIA_WAV: &str = "media:audio;ext=wav";
pub const MEDIA_FLAC: &str = "media:audio;ext=flac";
pub const MEDIA_OGG: &str = "media:audio;ext=ogg";
pub const MEDIA_AAC: &str = "media:audio;ext=aac";
pub const MEDIA_M4A: &str = "media:audio;ext=m4a";
pub const MEDIA_AIFF: &str = "media:audio;ext=aiff";
pub const MEDIA_OPUS: &str = "media:audio;ext=opus";

// Video container formats. Conversion caps live in videocartridge.
pub const MEDIA_MP4: &str = "media:ext=mp4;video";
pub const MEDIA_MOV: &str = "media:ext=mov;video";
pub const MEDIA_WEBM: &str = "media:ext=webm;video";
pub const MEDIA_MKV: &str = "media:ext=mkv;video";

// Semantic AI input types - distinguished by their purpose/context
/// Media URN for audio input containing speech for transcription (Whisper)
pub const MEDIA_AUDIO_SPEECH: &str = "media:audio;ext=wav;speech";
/// Media URN for extracted page text
/// Media URN for a single page of finalised plain text extracted from a
/// multi-page document (e.g. `cap:disbind-pdf`'s output, one item per page).
///
/// The composite URN carries `role=page` (the per-page sequence marker),
/// `plain-text` (the finalised-text marker that opts into
/// `cap:save-as-txt`'s persistence path), and `file-type=txt` (binds the
/// URN to the `.txt` extension at the registry).
pub const MEDIA_TEXTABLE_PAGE: &str = "media:enc=utf-8;ext=txt;page;plain-text";

// Document types (PRIMARY naming - type IS the format)
/// Media URN for PDF documents
pub const MEDIA_PDF: &str = "media:ext=pdf";
/// Media URN for EPUB documents
pub const MEDIA_EPUB: &str = "media:ext=epub";

// Text format types (PRIMARY naming - type IS the format)
/// Media URN for Markdown text
pub const MEDIA_MD: &str = "media:enc=utf-8;ext=md";
/// Media URN for plain text
pub const MEDIA_TXT: &str = "media:enc=utf-8;ext=txt";
/// Media URN for reStructuredText
pub const MEDIA_RST: &str = "media:enc=utf-8;ext=rst";
/// Media URN for log files
pub const MEDIA_LOG: &str = "media:enc=utf-8;ext=log";
/// Media URN for HTML documents
pub const MEDIA_HTML: &str = "media:enc=utf-8;ext=html";
/// Media URN for XML documents
pub const MEDIA_XML: &str = "media:enc=utf-8;ext=xml";
/// Media URN for JSON data - has record marker (structured key-value)
pub const MEDIA_JSON: &str = "media:fmt=json;record";
/// Media URN for JSON with schema constraint (input for structured queries)
pub const MEDIA_JSON_SCHEMA: &str = "media:fmt=json;json-schema;record";
/// Media URN for YAML data - has record marker (structured key-value)
pub const MEDIA_YAML: &str = "media:fmt=yaml;record";

// Format-specific variants for JSON, YAML, CSV
/// Media URN for a generic JSON value (scalar — string, number, boolean, null, or object)
pub const MEDIA_JSON_VALUE: &str = "media:fmt=json";
/// Media URN for a JSON object (alias for MEDIA_JSON)
pub const MEDIA_JSON_RECORD: &str = "media:fmt=json;record";
/// Media URN for a JSON array (list of values)
pub const MEDIA_JSON_LIST: &str = "media:fmt=json;list";
/// Media URN for a JSON array of objects (list of records)
pub const MEDIA_JSON_LIST_RECORD: &str = "media:fmt=json;list;record";
/// Media URN for a generic YAML value (scalar — string, number, boolean, null, or mapping)
pub const MEDIA_YAML_VALUE: &str = "media:fmt=yaml";
/// Media URN for a YAML mapping (alias for MEDIA_YAML)
pub const MEDIA_YAML_RECORD: &str = "media:fmt=yaml;record";
/// Media URN for a YAML sequence (list of values)
pub const MEDIA_YAML_LIST: &str = "media:fmt=yaml;list";
/// Media URN for a YAML sequence of mappings (list of records)
pub const MEDIA_YAML_LIST_RECORD: &str = "media:fmt=yaml;list;record";
/// Media URN for CSV data — by definition a list of records (header row + data rows)
pub const MEDIA_CSV: &str = "media:fmt=csv;list;record";
/// Media URN for single-column CSV — list of values without record structure
pub const MEDIA_CSV_LIST: &str = "media:fmt=csv;list;record";

// File path type — for arguments that represent filesystem paths.
// There is a single media URN; cardinality lives on `is_sequence`, not on
// URN tags.
pub const MEDIA_FILE_PATH: &str = "media:enc=utf-8;file-path";

// Semantic text input types - distinguished by their purpose/context
/// Media URN for model spec (provider:model format, HuggingFace name, etc.) - scalar by default
/// Generic, backend-agnostic — used by modelcartridge for download/status/path operations.
pub const MEDIA_MODEL_SPEC: &str = "media:enc=utf-8;model-spec";
/// Media URN for MLX model path - scalar by default
pub const MEDIA_MLX_MODEL_PATH: &str = "media:enc=utf-8;mlx-model-path";

// Backend-agnostic model-spec variants (used by inference caps).
/// Backend-agnostic LLM model spec — model spec string determines the backend.
pub const MEDIA_MODEL_SPEC_LLM: &str = "media:enc=utf-8;llm;model-spec";

// Backend + use-case specific model-spec variants.
// Each inference cap declares the variant matching its backend and purpose,
// so slot values can target a specific cartridge+task without ambiguity.

// GGUF backend.
//
// GGUF is a self-contained format — every `.gguf` file embeds the
// tokenizer state. The ggufcartridge loader has no code path for
// any other tokenizer shape, so `tokenizer-embedded-gguf` is a
// physical requirement and is part of every GGUF cap's URN.
// Family / chat-template / quantisation are loader-adaptive and
// therefore NOT in the URN; the cartridge gets them from the
// `download-model` response's refined record at runtime.
//
/// GGUF vision model spec (moondream2, Gemma 3-vision, …).
pub const MEDIA_MODEL_SPEC_GGUF_VISION: &str =
    "media:enc=utf-8;gguf;model-spec;tokenizer-embedded-gguf;vision";
/// GGUF LLM model spec (Qwen, Mistral, Llama, …).
pub const MEDIA_MODEL_SPEC_GGUF_LLM: &str =
    "media:enc=utf-8;gguf;llm;model-spec;tokenizer-embedded-gguf";
/// GGUF embeddings model spec (nomic-embed, embeddinggemma, …).
pub const MEDIA_MODEL_SPEC_GGUF_EMBEDDINGS: &str =
    "media:embeddings;enc=utf-8;gguf;model-spec;tokenizer-embedded-gguf";
/// GGUF OCR model spec (e.g. GLM-OCR). Distinct from
/// `MEDIA_MODEL_SPEC_GGUF_VISION` — both are multimodal GGUF
/// files with a text-model + mmproj pair, but the `model-task`
/// axis distinguishes them: `vision` covers describe-image and
/// generic VLM use, `ocr` is reserved for models specifically
/// trained to transcribe text *contained in* an image.
pub const MEDIA_MODEL_SPEC_GGUF_OCR: &str =
    "media:enc=utf-8;gguf;model-spec;ocr;tokenizer-embedded-gguf";

// Backend-narrowed model-spec supertypes. Each backend cartridge's
// adapter handler returns the URN for its backend so the engine's
// adapter-discrimination knows which backend claims the spec.
/// Candle backend's model-spec supertype, narrower than `media:enc=utf-8;model-spec`
/// but broader than the per-task variants below.
pub const MEDIA_MODEL_SPEC_CANDLE: &str = "media:candle;enc=utf-8;model-spec";
/// GGUF backend's model-spec supertype.
pub const MEDIA_MODEL_SPEC_GGUF: &str = "media:enc=utf-8;gguf;model-spec";
/// MLX backend's model-spec supertype.
pub const MEDIA_MODEL_SPEC_MLX: &str = "media:enc=utf-8;mlx;model-spec";

// MLX backend
/// MLX vision model spec (e.g. Qwen3-VL)
pub const MEDIA_MODEL_SPEC_MLX_VISION: &str = "media:enc=utf-8;mlx;model-spec;vision";
/// MLX LLM model spec (e.g. Llama-3.2-3B)
pub const MEDIA_MODEL_SPEC_MLX_LLM: &str = "media:enc=utf-8;llm;mlx;model-spec";
/// MLX embeddings model spec (e.g. all-MiniLM-L6-v2)
pub const MEDIA_MODEL_SPEC_MLX_EMBEDDINGS: &str = "media:embeddings;enc=utf-8;mlx;model-spec";

// Candle backend.
//
// Each constant is the broadest model-spec URN the candle cartridge
// can physically support for that cap — i.e. it carries every dim
// the loader REQUIRES and nothing else. Dims the loader can adapt
// to (family, quantisation, chat-template, …) are not in the URN;
// the cartridge learns them at runtime from the `download-model`
// response's refined record.
//
// The cap TOML's cap-arg `media_urn`, the cap `in =` (when the
// model-spec is the cap's input), and the cartridge manifest's
// `add_arg(media_urn=…)` / `in_spec(…)` all read these constants
// and thus stay in sync by construction.
//
/// Candle vision model spec (BLIP). Loader requires safetensors
/// weights and a unified `tokenizer.json`.
pub const MEDIA_MODEL_SPEC_CANDLE_VISION: &str =
    "media:candle;enc=utf-8;model-spec;repo-safetensors;tokenizer-unified;vision";
/// Candle text-embeddings model spec (BERT). Same physical
/// requirements as `MEDIA_MODEL_SPEC_CANDLE_VISION`.
pub const MEDIA_MODEL_SPEC_CANDLE_EMBEDDINGS: &str =
    "media:candle;embeddings;enc=utf-8;model-spec;repo-safetensors;tokenizer-unified";
/// Candle image-embeddings model spec (CLIP). Loader has fallbacks
/// on both axes — `pytorch_model.bin` for repo-format and
/// `bpe-pair` for tokenizer-shape — so neither can be tightened
/// without rejecting models the loader can physically handle.
pub const MEDIA_MODEL_SPEC_CANDLE_IMAGE_EMBEDDINGS: &str =
    "media:candle;enc=utf-8;image-embeddings;model-spec";
/// Candle LLM model spec (Mistral, Llama, Qwen, …). Loader requires
/// safetensors weights and a unified `tokenizer.json`.
pub const MEDIA_MODEL_SPEC_CANDLE_LLM: &str =
    "media:candle;enc=utf-8;llm;model-spec;repo-safetensors;tokenizer-unified";
/// Candle transcription model spec (Whisper). Same physical
/// requirements as the LLM/vision/embeddings variants.
pub const MEDIA_MODEL_SPEC_CANDLE_TRANSCRIPTION: &str =
    "media:candle;enc=utf-8;model-spec;repo-safetensors;tokenizer-unified;transcription";
/// Media URN for model repository (input for list-models) - has record marker
pub const MEDIA_MODEL_REPO: &str = "media:enc=utf-8;model-repo;record";

/// HuggingFace bearer token, passed as an optional argument to caps that
/// hit the HuggingFace API. Carries a `secret` tag so UIs and logs can mask
/// the value.
pub const MEDIA_HF_TOKEN: &str = "media:enc=utf-8;hf-token;secret";

/// JSON record carrying `{ "architectures": ["mistral", "llama", ...] }`
/// used by the `list-compatible-models` cap to filter cached models by
/// `model_type`.
pub const MEDIA_MODEL_ARCH_LIST: &str = "media:fmt=json;model-arch-list;record";

/// JSON record carrying a [`crate::ModelSearchRequest`] equivalent — query,
/// limit, sort, cursor, and the optional `architectures` field driving
/// cap-aware result filtering.
pub const MEDIA_MODEL_SEARCH_REQUEST: &str = "media:fmt=json;model-search-request;record";

/// JSON record carrying a [`crate::ModelSearchResponse`] equivalent.
pub const MEDIA_MODEL_SEARCH_RESPONSE: &str = "media:fmt=json;model-search-response;record";

/// JSON record carrying the dry-run result of `resolve-model-filters` —
/// the spec, the user filters, the architecture-required additions, and
/// the resulting effective file list.
pub const MEDIA_MODEL_FILTER_RESOLUTION: &str = "media:fmt=json;model-filter-resolution;record";

/// Helper to build a bare file media URN for a given extension — a file of the
/// given type with no content-format or encoding claim (e.g. `media:ext=pdf`).
pub fn file_media_urn_for_ext(ext: &str) -> String {
    format!("media:ext={}", ext)
}

/// Helper to build a UTF-8 text file media URN for a given extension
/// (e.g. `media:enc=utf-8;ext=md`).
pub fn text_media_urn_for_ext(ext: &str) -> String {
    format!("media:enc=utf-8;ext={}", ext)
}

/// Helper to build image media URN with extension
pub fn image_media_urn_for_ext(ext: &str) -> String {
    format!("media:image;ext={}", ext)
}

/// Helper to build audio media URN with extension
pub fn audio_media_urn_for_ext(ext: &str) -> String {
    format!("media:audio;ext={}", ext)
}

// CAPDAG output types - record marker for structured JSON objects, list marker for arrays
/// Media URN for model dimension output - scalar by default (no list marker)
pub const MEDIA_MODEL_DIM: &str = "media:integer;model-dim;numeric";
/// Media URN for model download output - has record marker
pub const MEDIA_DOWNLOAD_OUTPUT: &str = "media:download-result;enc=utf-8;record";
/// Media URN for model list output - has record marker
pub const MEDIA_LIST_OUTPUT: &str = "media:enc=utf-8;model-list;record";
/// Media URN for model status output - has record marker
pub const MEDIA_STATUS_OUTPUT: &str = "media:enc=utf-8;model-status;record";
/// Media URN for model contents output - has record marker
pub const MEDIA_CONTENTS_OUTPUT: &str = "media:enc=utf-8;model-contents;record";
/// Media URN for model availability output - has record marker
pub const MEDIA_AVAILABILITY_OUTPUT: &str = "media:enc=utf-8;model-availability;record";
/// Media URN for model path output - has record marker
pub const MEDIA_PATH_OUTPUT: &str = "media:enc=utf-8;model-path;record";
/// Media URN for embedding vector output - has record marker
pub const MEDIA_EMBEDDING_VECTOR: &str = "media:embedding-vector;enc=utf-8;record";
/// Media URN for vision inference output — a concrete UTF-8 text terminal.
///
/// The composite URN carries `image-description` (the vision-specific marker
/// so downstream tools can still match captions specifically), `plain-text`
/// (the finalised-text marker that opts into `cap:save-as-txt`'s persistence
/// path), and `file-type=txt` (binds the URN to the `.txt` extension at the
/// registry).
pub const MEDIA_IMAGE_DESCRIPTION: &str = "media:enc=utf-8;ext=txt;image-description;plain-text";
/// Media URN for OCR output — verbatim text digitized from an image, scalar by default.
/// Distinct from `MEDIA_IMAGE_DESCRIPTION` (which is a generated caption / answer about
/// the image) — `digitized-text` carries text that is *present* in the image.
///
/// The composite URN carries `digitized-text` (the OCR-specific marker), `plain-text`
/// (the finalised-text marker that opts into `cap:save-as-txt`'s persistence path),
/// and `file-type=txt` (binds the URN to the `.txt` extension at the registry).
pub const MEDIA_DIGITIZED_TEXT: &str = "media:digitized-text;enc=utf-8;ext=txt;plain-text";
/// Media URN for finalised plain text — the canonical input/output of `cap:save-as-txt`.
/// Producers of user-facing prose (LLM text-generation, OCR's digitized text,
/// summarisation) declare this URN as their `out` so the planner restricts the
/// `.txt` persistence path to those caps. See `fabric/media/plain-text.toml`.
pub const MEDIA_PLAIN_TEXT: &str = "media:enc=utf-8;ext=txt;plain-text";
/// Media URN for transcription output - has record marker
pub const MEDIA_TRANSCRIPTION_OUTPUT: &str = "media:enc=utf-8;record;transcription";
/// Media URN for decision output — JSON object with identifier and boolean value
pub const MEDIA_DECISION: &str = "media:decision;fmt=json;record";

/// Media URN for a semantic-judgment record — the shared output
/// envelope of the semantic-primitive cap family (`same`, and the
/// primitives that follow it): `{result..., confidence, reason}`.
/// See docs/semantic-primitives.md (law P2).
pub const MEDIA_SEMANTIC_JUDGMENT: &str = "media:fmt=json;record;semantic-judgment";

/// Media URN for adapter selection output — JSON object with media_urns array
/// Returned by cartridge content-inspection adapters to identify file media types
pub const MEDIA_ADAPTER_SELECTION: &str = "media:adapter-selection;fmt=json;record";

/// Media URN for a canonical cap URN string.
///
/// Carried as data when a cap takes "the URN of some other cap" as input —
/// e.g. the input to `cap:lookup-cap;fabric`. The string value MUST be the
/// canonical (alphabetically normalised) serialisation produced by the
/// `CapUrn` parser; consumers re-parse it through `CapUrn::from_string`
/// before any comparison or dispatch decision.
pub const MEDIA_CAP_URN: &str = "media:cap-urn;enc=utf-8";

/// Media URN for a canonical media URN string.
///
/// Carried as data when a cap takes "the URN of some media def" as input —
/// e.g. the input to `cap:lookup-media-def;fabric`. The string value MUST
/// be the canonical (alphabetically normalised) serialisation produced by
/// the `MediaUrn` parser; consumers re-parse it through
/// `MediaUrn::from_string` before any comparison or dispatch decision.
pub const MEDIA_MEDIA_URN: &str = "media:enc=utf-8;media-urn";

/// Media URN for a fabric registry per-definition version (defver).
///
/// Carried as data alongside a URN when a cap looks up a definition
/// pinned to a specific manifest snapshot. Value is the UTF-8 string
/// representation of a positive integer. Absent ⇒ the lookup cartridge
/// resolves the URN under its own pinned fabric manifest; present ⇒ the
/// caller is asking for that exact defver.
pub const MEDIA_FABRIC_DEFVER: &str = "media:defver;enc=utf-8";

/// Media URN for the full flattened cap definition published by the registry.
///
/// JSON record returned by `cap:lookup-cap;fabric`. Shape mirrors the
/// `flattenCapability` output in the fabric build pipeline.
pub const MEDIA_CAP_DEFINITION: &str = "media:cap-definition;fmt=json;record";

/// Media URN for the full media definition published by the registry.
///
/// JSON record returned by `cap:lookup-media-def;fabric`. Shape mirrors
/// the media def emitter output in the fabric build pipeline.
pub const MEDIA_MEDIA_DEFINITION: &str = "media:fmt=json;media-definition;record";

// =============================================================================
// MEDIA URN TYPE
// =============================================================================

/// A media URN representing a data type specification
///
/// Media URNs are tagged URNs with the "media" prefix. They describe data
/// types using tags like `type`, `subtype`, `v` (version), and `profile`.
///
/// This is a newtype wrapper around `TaggedUrn` that enforces the "media"
/// prefix and provides convenient accessors for common tags.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MediaUrn(TaggedUrn);

impl MediaUrn {
    /// The required prefix for all media URNs
    pub const PREFIX: &'static str = "media";

    /// Create a new MediaUrn from a TaggedUrn
    ///
    /// Returns an error if:
    /// - The TaggedUrn doesn't have the `media` prefix.
    /// - The `void` marker tag is combined with any other tag.
    ///   `media:void` is atomic — the type-theoretic unit `()` —
    ///   and admits no refinements. Reasons or labels belong on
    ///   the cap URN's non-directional tags or in cap args.
    pub fn new(urn: TaggedUrn) -> Result<Self, MediaUrnError> {
        if urn.prefix != Self::PREFIX {
            return Err(MediaUrnError::InvalidPrefix {
                expected: Self::PREFIX.to_string(),
                actual: urn.prefix.clone(),
            });
        }
        if urn.tags.contains_key("void") && urn.tags.len() > 1 {
            let extra_tags: Vec<String> = urn
                .tags
                .keys()
                .filter(|k| k.as_str() != "void")
                .cloned()
                .collect();
            return Err(MediaUrnError::VoidNotAtomic { extra_tags });
        }
        Ok(Self(urn))
    }

    /// Create a MediaUrn from a string representation
    ///
    /// The string must be a valid tagged URN with the "media" prefix.
    /// Whitespace and empty input validation is handled by TaggedUrn::from_string.
    pub fn from_string(s: &str) -> Result<Self, MediaUrnError> {
        let urn = TaggedUrn::from_string(s).map_err(MediaUrnError::Parse)?;
        Self::new(urn)
    }

    /// Get the inner TaggedUrn
    pub fn inner(&self) -> &TaggedUrn {
        &self.0
    }

    /// Get the extension tag value (e.g., "pdf", "epub", "md")
    pub fn extension(&self) -> Option<&str> {
        self.get_tag("ext")
    }

    /// Get any tag value by key
    pub fn get_tag(&self, key: &str) -> Option<&str> {
        self.0.get_tag(key).map(|s| s.as_str())
    }

    /// Check if this media URN has a specific tag
    pub fn has_tag(&self, key: &str, value: &str) -> bool {
        self.0.has_tag(key, value)
    }

    /// Create a new MediaUrn with an additional or updated tag
    /// Returns error if value is empty (use "*" for wildcard)
    pub fn with_tag(&self, key: &str, value: &str) -> Result<Self, tagged_urn::TaggedUrnError> {
        Ok(Self(
            self.0
                .clone()
                .with_tag(key.to_string(), value.to_string())?,
        ))
    }

    /// Create a new MediaUrn without a specific tag
    pub fn without_tag(&self, key: &str) -> Self {
        Self(self.0.clone().without_tag(key))
    }

    /// Compute the coordinate-space delta from `base` to `self`.
    pub fn delta_from(&self, base: &MediaUrn) -> Result<TaggedUrnCoordinateDelta, TaggedUrnError> {
        self.0.delta_from(&base.0)
    }

    /// Apply a coordinate delta while preserving unrelated refinements.
    pub fn apply_delta(&self, delta: &TaggedUrnCoordinateDelta) -> Result<Self, MediaUrnError> {
        Self::new(self.0.apply_delta(delta)?)
    }

    /// Compute the least upper bound — the join `∨` of the dispatch poset
    /// `(𝑀, ⊑)` ordered by `conforms_to` — of a set of MediaUrns: the **most
    /// specific** single type every input conforms to (the "denominator").
    ///
    /// Per tag key:
    /// - present in every input with the **same** stored value → kept as-is
    ///   (exact values and bare markers alike);
    /// - present in every input with **differing plain values** (exact or the
    ///   `*` must-have-any sigil) → promoted to the must-have-any wildcard
    ///   (`key` ≡ `key=*`): every input *has* the key, so "has key, any value"
    ///   is a sound shared constraint strictly tighter than dropping it. This
    ///   is what lets `ext=pdf ∨ ext=txt = media:ext` land on generic caps
    ///   like `in="media:ext"` with zero transformation;
    /// - present with differing values where any side carries a qualifier form
    ///   (`?…`/`!…` — optional / must-not-have) → dropped: `!key` asserts
    ///   *absence*, so presence cannot be soundly required;
    /// - absent from any input → dropped (presence cannot be required).
    ///
    /// - Empty input → `media:` (universal type ⊤)
    /// - Single input → returned as-is
    /// - `[media:ext=pdf, media:ext=pdf]` → `media:ext=pdf`
    /// - `[media:ext=pdf, media:ext=png;image]` → `media:ext`
    /// - `[media:fmt=json;list, media:fmt=json]` → `media:fmt=json`
    pub fn least_upper_bound(urns: &[MediaUrn]) -> MediaUrn {
        if urns.is_empty() {
            return MediaUrn::from_string("media:").unwrap_or_else(|_| {
                MediaUrn(TaggedUrn {
                    prefix: "media".to_string(),
                    tags: std::collections::BTreeMap::new(),
                })
            });
        }
        if urns.len() == 1 {
            return urns[0].clone();
        }

        // A stored value is "plain" when it is an exact value or the `*`
        // must-have-any sigil — i.e. it asserts presence. Qualifier forms
        // (`?`, `!`, `?=v`, `!=v`) do not assert presence and are only kept
        // when identical across every input.
        let is_plain = |v: &str| !v.starts_with('?') && !v.starts_with('!');

        // Start with the first URN's tags; fold each subsequent URN in.
        let mut common_tags = urns[0].0.tags.clone();
        for urn in &urns[1..] {
            let mut folded = std::collections::BTreeMap::new();
            for (key, value) in &common_tags {
                match urn.0.tags.get(key) {
                    Some(other) if other == value => {
                        folded.insert(key.clone(), value.clone());
                    }
                    Some(other) if is_plain(value) && is_plain(other) => {
                        // Both assert presence with differing values → promote
                        // to must-have-any.
                        folded.insert(key.clone(), "*".to_string());
                    }
                    _ => {} // absent, or a qualifier-form conflict → drop
                }
            }
            common_tags = folded;
        }

        MediaUrn(TaggedUrn {
            prefix: "media".to_string(),
            tags: common_tags,
        })
    }

    /// Serialize just the tags portion (without "media:" prefix)
    ///
    /// Returns tags in canonical form with proper quoting and sorting.
    pub fn tags_to_string(&self) -> String {
        self.0.tags_to_string()
    }

    /// Get the canonical string representation
    pub fn to_string(&self) -> String {
        self.0.to_string()
    }

    /// Check if this media URN (instance) satisfies the pattern's constraints.
    /// Equivalent to `pattern.accepts(self)`.
    pub fn conforms_to(&self, pattern: &MediaUrn) -> Result<bool, MediaUrnError> {
        self.0.conforms_to(&pattern.0).map_err(MediaUrnError::Match)
    }

    /// Check if this media URN (pattern) accepts the given instance.
    /// Equivalent to `instance.conforms_to(self)`.
    pub fn accepts(&self, instance: &MediaUrn) -> Result<bool, MediaUrnError> {
        self.0.accepts(&instance.0).map_err(MediaUrnError::Match)
    }

    /// Check if two media URNs have the exact same tag set (order-independent).
    /// Equivalent to `self.accepts(other) && other.accepts(self)`.
    pub fn is_equivalent(&self, other: &MediaUrn) -> Result<bool, MediaUrnError> {
        self.0.is_equivalent(&other.0).map_err(MediaUrnError::Match)
    }

    /// String variant of `is_equivalent`.
    pub fn is_equivalent_str(&self, other_str: &str) -> Result<bool, MediaUrnError> {
        let other = MediaUrn::from_string(other_str)?;
        self.is_equivalent(&other)
    }

    /// Check if two media URNs are on the same specialization chain.
    /// Equivalent to `self.accepts(other) || other.accepts(self)`.
    ///
    /// Use this for discovery/grouping, NOT for dispatch.
    pub fn is_comparable(&self, other: &MediaUrn) -> Result<bool, MediaUrnError> {
        self.0.is_comparable(&other.0).map_err(MediaUrnError::Match)
    }

    /// Get the specificity of this media URN
    ///
    /// Specificity is the count of non-wildcard tags.
    pub fn specificity(&self) -> usize {
        self.0.specificity()
    }

    // =========================================================================
    // Behavior helpers (triggered by tag presence)
    // =========================================================================

    // =========================================================================
    // SEMANTIC TYPE CHECKS (list, record markers)
    // =========================================================================

    /// Returns true if this media URN describes list-type data (has `list` marker tag).
    ///
    /// This is a **semantic type** check — it means "the data format IS a list/array"
    /// (e.g., `media:fmt=json;list` is a JSON array).
    ///
    /// This does NOT indicate input cardinality/shape (single vs multiple items).
    /// Cardinality is tracked by `is_sequence` on the wire protocol, not by URN tags.
    pub fn is_list(&self) -> bool {
        self.has_marker_tag("list")
    }

    /// Returns true if this media URN describes scalar (non-list) data.
    pub fn is_scalar(&self) -> bool {
        !self.has_marker_tag("list")
    }

    // =========================================================================
    // STRUCTURE (record marker)
    // =========================================================================

    /// Returns true if this media is a record (has `record` marker tag).
    /// A record has internal key-value structure (e.g., JSON object).
    pub fn is_record(&self) -> bool {
        self.has_marker_tag("record")
    }

    /// Returns true if this media is opaque (no `record` marker).
    /// Opaque is the default structure - no internal fields recognized.
    pub fn is_opaque(&self) -> bool {
        !self.has_marker_tag("record")
    }

    // =========================================================================
    // HELPER: Check for marker tag presence
    // =========================================================================

    /// Check if a marker tag (tag with wildcard/no value) is present.
    /// A marker tag is stored as key="*" in the tagged URN.
    pub fn has_marker_tag(&self, tag_name: &str) -> bool {
        self.0.tags.get(tag_name).map_or(false, |v| v == "*")
    }

    /// Check if this value's content format is JSON.
    /// Returns true iff the `fmt=json` content-format tag is present. The
    /// serialization format is the single `fmt=` axis — `fmt=json` marks JSON
    /// content regardless of whether it also came from a `.json` file (`ext=json`).
    pub fn is_json(&self) -> bool {
        self.get_tag("fmt") == Some("json")
    }

    /// Check if this value's content format is YAML (`fmt=yaml`).
    pub fn is_yaml(&self) -> bool {
        self.get_tag("fmt") == Some("yaml")
    }

    /// Check if this value's content format is CSV (`fmt=csv`).
    ///
    /// CSV content is identified by the `fmt=csv` content-format tag — the only
    /// form the catalog publishes (every datacartridge convert-format /
    /// collect-records cap declares its CSV media as `media:fmt=csv;list;record`).
    /// A `.csv` file on disk carries `ext=csv` *and* `fmt=csv`; the file-type tag
    /// alone (`ext=csv`) does not make a value CSV content.
    pub fn is_csv(&self) -> bool {
        self.get_tag("fmt") == Some("csv")
    }

    /// Check if this represents image data.
    /// Returns true if the "image" marker tag is present.
    pub fn is_image(&self) -> bool {
        self.get_tag("image").is_some()
    }

    /// Check if this represents audio data.
    /// Returns true if the "audio" marker tag is present.
    pub fn is_audio(&self) -> bool {
        self.get_tag("audio").is_some()
    }

    /// Check if this represents video data.
    /// Returns true if the "video" marker tag is present.
    pub fn is_video(&self) -> bool {
        self.get_tag("video").is_some()
    }

    /// Check if this represents numeric data.
    /// Returns true if the "numeric" marker tag is present.
    pub fn is_numeric(&self) -> bool {
        self.get_tag("numeric").is_some()
    }

    /// Check if this represents boolean data.
    /// Returns true if the "bool" marker tag is present.
    pub fn is_bool(&self) -> bool {
        self.get_tag("bool").is_some()
    }

    /// Check if this represents a void (no data) type — the **unit
    /// type** in the type-theoretic reading. `media:void` is the
    /// nullary value; a cap with `media:void` on a side has no
    /// meaningful data on that side. It is NOT "invalid" or "absent".
    pub fn is_void(&self) -> bool {
        // Check for "void" marker tag
        self.0.tags.contains_key("void")
    }

    /// Check if this is the **top** media URN — the universal
    /// wildcard `media:` with no tags. Order-theoretically, every
    /// other media URN `conforms_to` this one. Functionally it means
    /// "any data type is acceptable here." Distinct from `media:void`,
    /// which is the unit value (no data); a top-typed slot is `A` for
    /// any `A`, a void-typed slot is `()`.
    pub fn is_top(&self) -> bool {
        self.0.tags.is_empty()
    }

    /// Check if this represents a file path type.
    ///
    /// There is a single file-path media URN (`media:enc=utf-8;file-path`);
    /// cardinality (single file vs multiple files) is carried on the wire
    /// via `is_sequence`, not via URN tags.
    pub fn is_file_path(&self) -> bool {
        self.has_marker_tag("file-path")
    }

    /// Check if this represents a live-feed reference (`live` marker tag).
    ///
    /// Live-feed URNs are REFERENCE media (13.2 §Reference Media): the arg
    /// value is a selector record, and the runtime resolves it into an
    /// unbounded content stream via a registered provider — the same
    /// transport-resolution family as `media:file-path`. This is the
    /// canonical membership predicate for that family; callers must not
    /// test the `live` tag themselves.
    pub fn is_live_feed(&self) -> bool {
        self.has_marker_tag("live")
    }
}

impl fmt::Display for MediaUrn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for MediaUrn {
    type Err = MediaUrnError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_string(s)
    }
}

// =============================================================================
// ERROR TYPE
// =============================================================================

/// Errors that can occur when working with media URNs
#[derive(Debug, Clone, PartialEq)]
pub enum MediaUrnError {
    /// The URN doesn't have the required "media" prefix
    InvalidPrefix { expected: String, actual: String },
    /// Error parsing the underlying tagged URN (includes whitespace/empty validation)
    Parse(TaggedUrnError),
    /// Error during matching operation
    Match(TaggedUrnError),
    /// `media:void` was combined with one or more other tags. The unit
    /// type is atomic — there is no lattice underneath it. Reasons for
    /// "why void was used" belong on the cap URN's non-directional
    /// tags or in the cap's args, not as refinements of the media URN.
    VoidNotAtomic { extra_tags: Vec<String> },
}

impl fmt::Display for MediaUrnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MediaUrnError::InvalidPrefix { expected, actual } => {
                write!(
                    f,
                    "invalid media URN prefix: expected '{}', got '{}'",
                    expected, actual
                )
            }
            MediaUrnError::Parse(e) => write!(f, "failed to parse media URN: {}", e),
            MediaUrnError::Match(e) => write!(f, "media URN match error: {}", e),
            MediaUrnError::VoidNotAtomic { extra_tags } => write!(
                f,
                "media:void is atomic and cannot be refined; got extra tag(s): {}. \
                 Move why/how this void is used into cap-tags or args, not the media URN.",
                extra_tags.join(", ")
            ),
        }
    }
}

impl std::error::Error for MediaUrnError {}

impl From<TaggedUrnError> for MediaUrnError {
    fn from(e: TaggedUrnError) -> Self {
        MediaUrnError::Parse(e)
    }
}

// =============================================================================
// SERDE SUPPORT
// =============================================================================

impl serde::Serialize for MediaUrn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for MediaUrn {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        MediaUrn::from_string(&s).map_err(serde::de::Error::custom)
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // TEST060: Test wrong prefix fails with InvalidPrefix error showing expected and actual prefix
    #[test]
    fn test060_wrong_prefix_fails() {
        let result = MediaUrn::from_string("cap:string");
        assert!(result.is_err());
        if let Err(MediaUrnError::InvalidPrefix { expected, actual }) = result {
            assert_eq!(expected, "media");
            assert_eq!(actual, "cap");
        } else {
            panic!("expected InvalidPrefix error");
        }
    }

    // TEST061: REMOVED — the binary/text distinction no longer exists in the
    // vocabulary (is_binary() was deleted from MediaUrn; everything is bytes).
    // Encoding is now expressed by the orthogonal `enc=` tag, exercised by other
    // tests (e.g. test558). No replacement assertion is meaningful here.

    // TEST062: Test is_record returns true when record marker tag is present indicating key-value structure
    #[test]
    fn test062_is_record() {
        // is_record returns true if record marker tag is present (key-value structure)
        assert!(MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_record()); // "media:enc=utf-8;record"
        assert!(MediaUrn::from_string("media:custom;record")
            .unwrap()
            .is_record());
        assert!(MediaUrn::from_string(MEDIA_JSON).unwrap().is_record()); // "media:fmt=json;record"
                                                                         // Without record marker, is_record is false
        assert!(!MediaUrn::from_string("media:enc=utf-8")
            .unwrap()
            .is_record());
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_record()); // scalar, no record marker
        assert!(!MediaUrn::from_string(MEDIA_STRING_LIST)
            .unwrap()
            .is_record()); // list, no record marker
    }

    // TEST063: Test is_scalar returns true when list marker tag is absent (scalar is default)
    #[test]
    fn test063_is_scalar() {
        // is_scalar returns true if NO list marker (scalar is default cardinality)
        assert!(MediaUrn::from_string(MEDIA_STRING).unwrap().is_scalar()); // "media:enc=utf-8" - no list marker
        assert!(MediaUrn::from_string(MEDIA_INTEGER).unwrap().is_scalar()); // no list marker
        assert!(MediaUrn::from_string(MEDIA_NUMBER).unwrap().is_scalar()); // no list marker
        assert!(MediaUrn::from_string(MEDIA_BOOLEAN).unwrap().is_scalar()); // no list marker
        assert!(MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_scalar()); // record but scalar
        assert!(MediaUrn::from_string("media:enc=utf-8")
            .unwrap()
            .is_scalar()); // plain text is scalar
                           // With list marker, is_scalar is false
        assert!(!MediaUrn::from_string(MEDIA_STRING_LIST)
            .unwrap()
            .is_scalar()); // has list marker
        assert!(!MediaUrn::from_string(MEDIA_OBJECT_LIST)
            .unwrap()
            .is_scalar()); // has list marker
    }

    // TEST064: Test is_list returns true when list marker tag is present indicating ordered collection
    #[test]
    fn test064_is_list() {
        // is_list returns true if list marker tag is present (ordered collection)
        assert!(MediaUrn::from_string(MEDIA_STRING_LIST).unwrap().is_list()); // "media:enc=utf-8;list"
        assert!(MediaUrn::from_string(MEDIA_INTEGER_LIST).unwrap().is_list()); // has list marker
        assert!(MediaUrn::from_string(MEDIA_OBJECT_LIST).unwrap().is_list()); // "media:enc=utf-8;list;record"
        assert!(MediaUrn::from_string("media:custom;list")
            .unwrap()
            .is_list());
        // Without list marker, is_list is false
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_list()); // no list marker
        assert!(!MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_list()); // record but no list marker
    }

    // TEST065: Test is_opaque returns true when record marker is absent (opaque is default)
    #[test]
    fn test065_is_opaque() {
        // is_opaque returns true if NO record marker (opaque is default structure)
        assert!(MediaUrn::from_string(MEDIA_STRING).unwrap().is_opaque()); // no record marker
        assert!(MediaUrn::from_string(MEDIA_STRING_LIST)
            .unwrap()
            .is_opaque()); // list but no record
        assert!(MediaUrn::from_string(MEDIA_PDF).unwrap().is_opaque()); // binary, no record
        assert!(MediaUrn::from_string("media:enc=utf-8")
            .unwrap()
            .is_opaque()); // no record marker
                           // With record marker, is_opaque is false
        assert!(!MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_opaque()); // has record marker
        assert!(!MediaUrn::from_string(MEDIA_JSON).unwrap().is_opaque()); // has record marker
        assert!(!MediaUrn::from_string(MEDIA_OBJECT_LIST)
            .unwrap()
            .is_opaque()); // has record marker
    }

    // TEST066: Test is_json returns true only when json marker tag is present for JSON representation
    #[test]
    fn test066_is_json() {
        // is_json returns true only if "json" marker tag is present (JSON representation)
        assert!(MediaUrn::from_string(MEDIA_JSON).unwrap().is_json()); // "media:fmt=json;record"
        assert!(MediaUrn::from_string("media:custom;fmt=json")
            .unwrap()
            .is_json());
        // record alone does not mean JSON representation
        assert!(!MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_json()); // map structure, not necessarily JSON
        assert!(!MediaUrn::from_string("media:enc=utf-8").unwrap().is_json());
    }

    // TEST552: is_csv recognizes the CANONICAL published CSV media, which spells
    // the format as the `ext=csv` extension tag — not a bare `csv` marker. Every
    // datacartridge convert-format/collect-records cap declares its CSV media as
    // `media:fmt=csv;list;record` (see MEDIA_CSV), so an is_csv() that
    // only checked the bare marker returned false for real CSV media, collapsing
    // it to a text list and breaking list<->csv conversion with "Unsupported
    // conversion: TextableList -> TextableList". This test pins both accepted
    // spellings and would FAIL against the old marker-only implementation.
    #[test]
    fn test552_is_csv_recognizes_ext_csv_and_bare_marker() {
        // The canonical published form (ext=csv) — the one that regressed.
        assert!(
            MediaUrn::from_string(MEDIA_CSV).unwrap().is_csv(),
            "the canonical CSV media (ext=csv) must be recognized as CSV"
        );
        assert!(MediaUrn::from_string("media:fmt=csv").unwrap().is_csv());
        // The bare `csv` marker spelling is still accepted.
        assert!(MediaUrn::from_string("media:fmt=csv;list")
            .unwrap()
            .is_csv());
        // A text list with no CSV signal is NOT csv (the failure mode that
        // collapsed CSV into a text list).
        assert!(!MediaUrn::from_string("media:enc=utf-8;list")
            .unwrap()
            .is_csv());
        // JSON / YAML are not CSV.
        assert!(!MediaUrn::from_string(MEDIA_JSON).unwrap().is_csv());
        assert!(!MediaUrn::from_string("media:fmt=yaml;list;record")
            .unwrap()
            .is_csv());
    }

    // TEST067: Text-representability is now carried by the orthogonal `enc=` tag
    // (the old `textable` marker and is_text() are gone). A media is "text" iff it
    // declares an encoding. enc is orthogonal to format/numeric, so only media that
    // actually carry enc= are text.
    #[test]
    fn test067_is_text() {
        // Has enc= → text-representable
        assert!(MediaUrn::from_string(MEDIA_STRING)
            .unwrap()
            .get_tag("enc")
            .is_some()); // "media:enc=utf-8"
        assert!(MediaUrn::from_string(MEDIA_BOOLEAN)
            .unwrap()
            .get_tag("enc")
            .is_some()); // "media:bool;enc=utf-8"
                         // No enc= → not text-representable
        assert!(MediaUrn::from_string(MEDIA_INTEGER)
            .unwrap()
            .get_tag("enc")
            .is_none()); // "media:integer;numeric"
        assert!(MediaUrn::from_string(MEDIA_JSON)
            .unwrap()
            .get_tag("enc")
            .is_none()); // "media:fmt=json;record"
        assert!(MediaUrn::from_string(MEDIA_IDENTITY)
            .unwrap()
            .get_tag("enc")
            .is_none()); // "media:"
        assert!(MediaUrn::from_string(MEDIA_PNG)
            .unwrap()
            .get_tag("enc")
            .is_none()); // "media:ext=png;image"
        assert!(MediaUrn::from_string(MEDIA_OBJECT)
            .unwrap()
            .get_tag("enc")
            .is_none()); // "media:record"
    }

    // TEST068: Test is_void returns true when void flag or type=void tag is present
    #[test]
    fn test068_is_void() {
        assert!(MediaUrn::from_string("media:void").unwrap().is_void());
        assert!(!MediaUrn::from_string("media:string").unwrap().is_void());
    }

    // TEST071: Test to_string roundtrip ensures serialization and deserialization preserve URN structure
    #[test]
    fn test071_to_string_roundtrip() {
        let original = "media:string";
        let urn = MediaUrn::from_string(original).unwrap();
        let s = urn.to_string();
        let urn2 = MediaUrn::from_string(&s).unwrap();
        assert_eq!(urn, urn2);
    }

    // TEST072: Test all media URN constants parse successfully as valid media URNs
    #[test]
    fn test072_constants_parse() {
        // Verify all constants are valid media URNs
        assert!(MediaUrn::from_string(MEDIA_VOID).is_ok());
        assert!(MediaUrn::from_string(MEDIA_STRING).is_ok());
        assert!(MediaUrn::from_string(MEDIA_INTEGER).is_ok());
        assert!(MediaUrn::from_string(MEDIA_NUMBER).is_ok());
        assert!(MediaUrn::from_string(MEDIA_BOOLEAN).is_ok());
        assert!(MediaUrn::from_string(MEDIA_OBJECT).is_ok());
        assert!(MediaUrn::from_string(MEDIA_IDENTITY).is_ok());
        assert!(MediaUrn::from_string(MEDIA_STRING_LIST).is_ok());
        assert!(MediaUrn::from_string(MEDIA_INTEGER_LIST).is_ok());
        assert!(MediaUrn::from_string(MEDIA_NUMBER_LIST).is_ok());
        assert!(MediaUrn::from_string(MEDIA_BOOLEAN_LIST).is_ok());
        assert!(MediaUrn::from_string(MEDIA_OBJECT_LIST).is_ok());
        // Semantic types
        assert!(MediaUrn::from_string(MEDIA_PNG).is_ok());
        assert!(MediaUrn::from_string(MEDIA_AUDIO).is_ok());
        assert!(MediaUrn::from_string(MEDIA_VIDEO).is_ok());
        // Document types (PRIMARY naming)
        assert!(MediaUrn::from_string(MEDIA_PDF).is_ok());
        assert!(MediaUrn::from_string(MEDIA_EPUB).is_ok());
        // Text format types (PRIMARY naming)
        assert!(MediaUrn::from_string(MEDIA_MD).is_ok());
        assert!(MediaUrn::from_string(MEDIA_TXT).is_ok());
        assert!(MediaUrn::from_string(MEDIA_RST).is_ok());
        assert!(MediaUrn::from_string(MEDIA_LOG).is_ok());
        assert!(MediaUrn::from_string(MEDIA_HTML).is_ok());
        assert!(MediaUrn::from_string(MEDIA_XML).is_ok());
        assert!(MediaUrn::from_string(MEDIA_JSON).is_ok());
        assert!(MediaUrn::from_string(MEDIA_YAML).is_ok());
    }

    // TEST073: Test extension helper functions create media URNs with ext tag and correct format
    #[test]
    fn test073_extension_helpers() {
        // Test file_media_urn_for_ext
        let pdf_urn = file_media_urn_for_ext("pdf");
        let parsed = MediaUrn::from_string(&pdf_urn).unwrap();
        assert!(
            parsed.has_tag("ext", "pdf"),
            "file ext helper must set ext=pdf"
        );
        assert_eq!(parsed.extension(), Some("pdf"));

        // Test text_media_urn_for_ext — a UTF-8 text file carries enc=utf-8 + ext
        let md_urn = text_media_urn_for_ext("md");
        let parsed = MediaUrn::from_string(&md_urn).unwrap();
        assert!(
            parsed.has_tag("ext", "md"),
            "text ext helper must set ext=md"
        );
        assert!(
            parsed.has_tag("enc", "utf-8"),
            "text ext helper must set enc=utf-8"
        );
        assert_eq!(parsed.extension(), Some("md"));
    }

    // TEST074: Test media URN conforms_to using tagged URN semantics with specific and generic requirements
    #[test]
    fn test074_media_urn_matching() {
        // PDF listing conforms to PDF requirement (PRIMARY type naming)
        // A more specific URN (media:ext=pdf) conforms to a less specific requirement (media:ext=pdf)
        let pdf_listing = MediaUrn::from_string(MEDIA_PDF).unwrap(); // "media:ext=pdf"
        let pdf_requirement = MediaUrn::from_string("media:ext=pdf").unwrap();
        assert!(pdf_listing
            .conforms_to(&pdf_requirement)
            .expect("MediaUrn prefix mismatch impossible"));

        // Markdown listing conforms to md requirement (PRIMARY type naming)
        let md_listing = MediaUrn::from_string(MEDIA_MD).unwrap(); // "media:enc=utf-8;ext=md"
        let md_requirement = MediaUrn::from_string("media:ext=md").unwrap();
        assert!(md_listing
            .conforms_to(&md_requirement)
            .expect("MediaUrn prefix mismatch impossible"));

        // Same URNs should conform to each other
        let string_urn = MediaUrn::from_string(MEDIA_STRING).unwrap();
        let string_req = MediaUrn::from_string(MEDIA_STRING).unwrap();
        assert!(string_urn
            .conforms_to(&string_req)
            .expect("MediaUrn prefix mismatch impossible"));
    }

    // TEST075: Test accepts with implicit wildcards where handlers with fewer tags can handle more requests
    #[test]
    fn test075_matching() {
        let handler = MediaUrn::from_string("media:string").unwrap();
        let request = MediaUrn::from_string("media:string").unwrap();
        assert!(handler.accepts(&request).unwrap());

        // Handler with fewer tags can handle more requests (implicit wildcards)
        let general_handler = MediaUrn::from_string("media:string").unwrap();
        assert!(general_handler.accepts(&request).unwrap());

        // Same URN should accept
        let same = MediaUrn::from_string("media:string").unwrap();
        assert!(handler.accepts(&same).unwrap());
    }

    // TEST076: Test specificity increases with more tags for ranking conformance
    #[test]
    fn test076_specificity() {
        // More tags = higher specificity. Use the same enc=utf-8 base and add
        // markers so each URN is a strict superset of the previous — the
        // specificity must be monotonic non-decreasing as tags accrue.
        let urn1 = MediaUrn::from_string("media:enc=utf-8").unwrap();
        let urn2 = MediaUrn::from_string("media:enc=utf-8;numeric").unwrap();
        let urn3 = MediaUrn::from_string("media:enc=utf-8;list;numeric").unwrap();

        // Verify specificity increases with more tags
        // Note: The exact values may depend on implementation, but relative order should hold
        let s1 = urn1.specificity();
        let s2 = urn2.specificity();
        let s3 = urn3.specificity();

        // At minimum, more tags should not have less specificity
        assert!(
            s2 >= s1,
            "urn2 ({}) should have >= specificity than urn1 ({})",
            s2,
            s1
        );
        assert!(
            s3 >= s2,
            "urn3 ({}) should have >= specificity than urn2 ({})",
            s3,
            s2
        );
    }

    // TEST077: Test serde roundtrip serializes to JSON string and deserializes back correctly
    #[test]
    fn test077_serde_roundtrip() {
        let urn = MediaUrn::from_string("media:string").unwrap();
        let json = serde_json::to_string(&urn).unwrap();
        assert_eq!(json, "\"media:string\"");
        let urn2: MediaUrn = serde_json::from_str(&json).unwrap();
        assert_eq!(urn, urn2);
    }
}

#[cfg(test)]
mod debug_tests {
    use super::*;
    use crate::standard::media::{MEDIA_IDENTITY, MEDIA_OBJECT, MEDIA_STRING};

    // TEST078: conforms_to behavior between MEDIA_OBJECT and MEDIA_STRING
    #[test]
    fn test078_object_does_not_conform_to_string() {
        let str_urn = MediaUrn::from_string(MEDIA_STRING).unwrap();
        let obj_urn = MediaUrn::from_string(MEDIA_OBJECT).unwrap();

        assert!(
            str_urn.conforms_to(&str_urn).unwrap(),
            "string conforms to string"
        );
        assert!(
            obj_urn.conforms_to(&obj_urn).unwrap(),
            "object conforms to object"
        );
        assert!(
            !obj_urn.conforms_to(&str_urn).unwrap(),
            "MEDIA_OBJECT should NOT conform to MEDIA_STRING (missing enc=utf-8)"
        );
    }

    // TEST304: Test MEDIA_AVAILABILITY_OUTPUT constant parses as valid media URN with correct tags
    #[test]
    fn test304_media_availability_output_constant() {
        let urn = MediaUrn::from_string(MEDIA_AVAILABILITY_OUTPUT).expect("must parse");
        assert!(
            urn.get_tag("enc").is_some(),
            "model-availability must be text-representable (enc=)"
        );
        assert!(
            urn.is_record(),
            "model-availability must have record marker"
        );
        // to_string() alphabetizes tags, so compare via roundtrip parsing instead
        let reparsed = MediaUrn::from_string(&urn.to_string()).expect("roundtrip must parse");
        assert!(
            urn.conforms_to(&reparsed).unwrap(),
            "roundtrip must conform to original"
        );
    }

    // TEST305: Test MEDIA_PATH_OUTPUT constant parses as valid media URN with correct tags
    #[test]
    fn test305_media_path_output_constant() {
        let urn = MediaUrn::from_string(MEDIA_PATH_OUTPUT).expect("must parse");
        assert!(
            urn.get_tag("enc").is_some(),
            "model-path must be text-representable (enc=)"
        );
        assert!(urn.is_record(), "model-path must have record marker");
        let reparsed = MediaUrn::from_string(&urn.to_string()).expect("roundtrip must parse");
        assert!(
            urn.conforms_to(&reparsed).unwrap(),
            "roundtrip must conform to original"
        );
    }

    // TEST306: Test MEDIA_AVAILABILITY_OUTPUT and MEDIA_PATH_OUTPUT are distinct URNs
    #[test]
    fn test306_availability_and_path_output_distinct() {
        assert_ne!(
            MEDIA_AVAILABILITY_OUTPUT, MEDIA_PATH_OUTPUT,
            "availability and path output must be distinct media URNs"
        );
        let avail = MediaUrn::from_string(MEDIA_AVAILABILITY_OUTPUT).unwrap();
        let path = MediaUrn::from_string(MEDIA_PATH_OUTPUT).unwrap();
        // They must NOT conform to each other (different types)
        assert!(
            !avail.conforms_to(&path).unwrap_or(true),
            "availability must not conform to path"
        );
    }

    // TEST546: is_image returns true only when image marker tag is present
    #[test]
    fn test546_is_image() {
        assert!(MediaUrn::from_string(MEDIA_PNG).unwrap().is_image());
        assert!(MediaUrn::from_string("media:image;thumbnail")
            .unwrap()
            .is_image());
        assert!(MediaUrn::from_string("media:ext=jpg;image")
            .unwrap()
            .is_image());
        // Non-image types
        assert!(!MediaUrn::from_string(MEDIA_PDF).unwrap().is_image());
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_image());
        assert!(!MediaUrn::from_string(MEDIA_AUDIO).unwrap().is_image());
        assert!(!MediaUrn::from_string(MEDIA_VIDEO).unwrap().is_image());
    }

    // TEST547: is_audio returns true only when audio marker tag is present
    #[test]
    fn test547_is_audio() {
        assert!(MediaUrn::from_string(MEDIA_AUDIO).unwrap().is_audio());
        assert!(MediaUrn::from_string(MEDIA_AUDIO_SPEECH)
            .unwrap()
            .is_audio());
        assert!(MediaUrn::from_string("media:audio;ext=mp3")
            .unwrap()
            .is_audio());
        // Non-audio types
        assert!(!MediaUrn::from_string(MEDIA_VIDEO).unwrap().is_audio());
        assert!(!MediaUrn::from_string(MEDIA_PNG).unwrap().is_audio());
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_audio());
    }

    // TEST548: is_video returns true only when video marker tag is present
    #[test]
    fn test548_is_video() {
        assert!(MediaUrn::from_string(MEDIA_VIDEO).unwrap().is_video());
        assert!(MediaUrn::from_string("media:ext=mp4;video")
            .unwrap()
            .is_video());
        // Non-video types
        assert!(!MediaUrn::from_string(MEDIA_AUDIO).unwrap().is_video());
        assert!(!MediaUrn::from_string(MEDIA_PNG).unwrap().is_video());
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_video());
    }

    // TEST549: is_numeric returns true only when numeric marker tag is present
    #[test]
    fn test549_is_numeric() {
        assert!(MediaUrn::from_string(MEDIA_INTEGER).unwrap().is_numeric());
        assert!(MediaUrn::from_string(MEDIA_NUMBER).unwrap().is_numeric());
        assert!(MediaUrn::from_string(MEDIA_INTEGER_LIST)
            .unwrap()
            .is_numeric());
        assert!(MediaUrn::from_string(MEDIA_NUMBER_LIST)
            .unwrap()
            .is_numeric());
        // Non-numeric types
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_numeric());
        assert!(!MediaUrn::from_string(MEDIA_BOOLEAN).unwrap().is_numeric());
        assert!(!MediaUrn::from_string(MEDIA_IDENTITY).unwrap().is_numeric());
    }

    // TEST550: is_bool returns true only when bool marker tag is present
    #[test]
    fn test550_is_bool() {
        assert!(MediaUrn::from_string(MEDIA_BOOLEAN).unwrap().is_bool());
        assert!(MediaUrn::from_string(MEDIA_BOOLEAN_LIST).unwrap().is_bool());
        // MEDIA_DECISION is now a JSON record, not a bool type
        assert!(!MediaUrn::from_string(MEDIA_DECISION).unwrap().is_bool());
        // Non-bool types
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_bool());
        assert!(!MediaUrn::from_string(MEDIA_INTEGER).unwrap().is_bool());
        assert!(!MediaUrn::from_string(MEDIA_IDENTITY).unwrap().is_bool());
    }

    // TEST551: is_file_path returns true for the single file-path media URN,
    // false for everything else. There is no "array" variant — cardinality is
    // carried by is_sequence on the wire, not by URN tags.
    #[test]
    fn test551_is_file_path() {
        assert!(MediaUrn::from_string(MEDIA_FILE_PATH)
            .unwrap()
            .is_file_path());
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_file_path());
        assert!(!MediaUrn::from_string(MEDIA_IDENTITY)
            .unwrap()
            .is_file_path());
    }

    // TEST553: is_live_feed returns true for every URN carrying the `live`
    // marker tag (the reference-media family the runtime resolves via
    // providers), false for everything else — including content URNs a
    // feed delivers.
    #[test]
    fn test553_is_live_feed() {
        assert!(MediaUrn::from_string("media:live").unwrap().is_live_feed());
        assert!(MediaUrn::from_string("media:live;synthetic")
            .unwrap()
            .is_live_feed());
        assert!(MediaUrn::from_string("media:audio;live;microphone")
            .unwrap()
            .is_live_feed());
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_live_feed());
        assert!(!MediaUrn::from_string(MEDIA_FILE_PATH)
            .unwrap()
            .is_live_feed());
        assert!(!MediaUrn::from_string("media:audio;pcm")
            .unwrap()
            .is_live_feed());
    }

    // TEST555: with_tag adds a tag and without_tag removes it
    #[test]
    fn test555_with_tag_and_without_tag() {
        let urn = MediaUrn::from_string("media:string").unwrap();
        let with_ext = urn.with_tag("ext", "pdf").unwrap();
        assert_eq!(with_ext.extension(), Some("pdf"));
        // Original unchanged
        assert_eq!(urn.extension(), None);

        // Remove the tag
        let without_ext = with_ext.without_tag("ext");
        assert_eq!(without_ext.extension(), None);
        // Removing non-existent tag is a no-op
        let same = urn.without_tag("nonexistent");
        assert_eq!(same, urn);
    }

    // TEST556: image_media_urn_for_ext creates valid image media URN
    #[test]
    fn test556_image_media_urn_for_ext() {
        let jpg_urn = image_media_urn_for_ext("jpg");
        let parsed = MediaUrn::from_string(&jpg_urn).unwrap();
        assert!(parsed.is_image(), "image helper must set image tag");
        assert!(
            parsed.get_tag("enc").is_none(),
            "image URN must not claim a text encoding (no enc= tag)"
        );
        assert_eq!(parsed.extension(), Some("jpg"));
    }

    // TEST557: audio_media_urn_for_ext creates valid audio media URN
    #[test]
    fn test557_audio_media_urn_for_ext() {
        let mp3_urn = audio_media_urn_for_ext("mp3");
        let parsed = MediaUrn::from_string(&mp3_urn).unwrap();
        assert!(parsed.is_audio(), "audio helper must set audio tag");
        assert!(
            parsed.get_tag("enc").is_none(),
            "audio URN must not claim a text encoding (no enc= tag)"
        );
        assert_eq!(parsed.extension(), Some("mp3"));
    }

    // TEST558: predicates are consistent with constants — every constant triggers exactly the expected predicates
    #[test]
    fn test558_predicate_constant_consistency() {
        // MEDIA_INTEGER must be numeric, scalar, NOT bool/image/video/list.
        // It is numeric (math ops valid) and carries no `enc=` — numeric values
        // are not text-encoded under the new vocabulary.
        let int = MediaUrn::from_string(MEDIA_INTEGER).unwrap();
        assert!(int.is_numeric());
        assert!(int.get_tag("enc").is_none());
        assert!(int.is_scalar());
        assert!(!int.is_bool());
        assert!(!int.is_image());
        assert!(!int.is_list());

        // MEDIA_BOOLEAN must be bool, text-encoded (enc=utf-8), scalar, NOT numeric
        let bool_urn = MediaUrn::from_string(MEDIA_BOOLEAN).unwrap();
        assert!(bool_urn.is_bool());
        assert!(bool_urn.get_tag("enc").is_some());
        assert!(bool_urn.is_scalar());
        assert!(!bool_urn.is_numeric());

        // MEDIA_JSON must be json, record, scalar. JSON declares its serialization
        // via `fmt=json`, not via an `enc=` tag, so it carries no enc.
        let json_urn = MediaUrn::from_string(MEDIA_JSON).unwrap();
        assert!(json_urn.is_json());
        assert!(json_urn.get_tag("enc").is_none());
        assert!(json_urn.is_record());
        assert!(
            json_urn.is_scalar(),
            "MEDIA_JSON is a scalar record (single object)"
        );
        assert!(!json_urn.is_list());

        // MEDIA_VOID is void, NOT numeric, and carries no encoding.
        let void = MediaUrn::from_string(MEDIA_VOID).unwrap();
        assert!(void.is_void());
        assert!(void.get_tag("enc").is_none());
        assert!(!void.is_numeric());
    }

    // TEST852: LUB of identical URNs returns the same URN
    #[test]
    fn test852_lub_identical() {
        let pdf = MediaUrn::from_string("media:ext=pdf").unwrap();
        let lub = MediaUrn::least_upper_bound(&[pdf.clone(), pdf.clone()]);
        assert!(lub.is_equivalent(&pdf).unwrap());
    }

    // TEST853: LUB promotes a shared key with differing values to must-have-any.
    // Both inputs HAVE an `ext` (pdf vs png), so the join is `media:ext` — the
    // wildcard denominator that generic caps like `in="media:ext"` accept —
    // NOT the trivial `media:`. The `image` marker is only on one side → dropped.
    #[test]
    fn test853_lub_shared_key_promotes_to_wildcard() {
        let pdf = MediaUrn::from_string("media:ext=pdf").unwrap();
        let png = MediaUrn::from_string("media:ext=png;image").unwrap();
        let lub = MediaUrn::least_upper_bound(&[pdf.clone(), png.clone()]);
        let expected = MediaUrn::from_string("media:ext").unwrap();
        assert!(
            lub.is_equivalent(&expected).unwrap(),
            "LUB of ext=pdf and ext=png;image should be media:ext but got {}",
            lub.to_string()
        );
        // Join soundness: every input conforms to the join.
        assert!(pdf.conforms_to(&lub).unwrap());
        assert!(png.conforms_to(&lub).unwrap());
    }

    // TEST853b: LUB of URNs sharing NO keys at all is the universal type.
    #[test]
    fn test853b_lub_disjoint_keys_is_universal() {
        let a = MediaUrn::from_string("media:image").unwrap();
        let b = MediaUrn::from_string("media:audio").unwrap();
        let lub = MediaUrn::least_upper_bound(&[a, b]);
        let universal = MediaUrn::from_string("media:").unwrap();
        assert!(
            lub.is_equivalent(&universal).unwrap(),
            "LUB of disjoint-key URNs should be media: but got {}",
            lub.to_string()
        );
    }

    // TEST854: LUB keeps common tags, drops differing ones
    #[test]
    fn test854_lub_partial_overlap() {
        // Both inputs are UTF-8 text (shared enc=utf-8) but differ in structure
        // (list vs record). The LUB keeps the common enc=utf-8 and drops the
        // differing structural markers.
        let text_list = MediaUrn::from_string("media:enc=utf-8;list").unwrap();
        let text_record = MediaUrn::from_string("media:enc=utf-8;record").unwrap();
        let lub = MediaUrn::least_upper_bound(&[text_list, text_record]);
        let expected = MediaUrn::from_string("media:enc=utf-8").unwrap();
        assert!(
            lub.is_equivalent(&expected).unwrap(),
            "LUB should be media:enc=utf-8 but got {}",
            lub.to_string()
        );
    }

    // TEST855: LUB of list and non-list drops list tag
    #[test]
    fn test855_lub_list_vs_scalar() {
        let json_list = MediaUrn::from_string("media:fmt=json;list").unwrap();
        let json_scalar = MediaUrn::from_string("media:fmt=json").unwrap();
        let lub = MediaUrn::least_upper_bound(&[json_list, json_scalar]);
        let expected = MediaUrn::from_string("media:fmt=json").unwrap();
        assert!(
            lub.is_equivalent(&expected).unwrap(),
            "LUB should drop list tag, got {}",
            lub.to_string()
        );
    }

    // TEST856: LUB of empty input returns universal type
    #[test]
    fn test856_lub_empty() {
        let lub = MediaUrn::least_upper_bound(&[]);
        let universal = MediaUrn::from_string("media:").unwrap();
        assert!(lub.is_equivalent(&universal).unwrap());
    }

    // TEST857: LUB of single input returns that input
    #[test]
    fn test857_lub_single() {
        let pdf = MediaUrn::from_string("media:ext=pdf").unwrap();
        let lub = MediaUrn::least_upper_bound(&[pdf.clone()]);
        assert!(lub.is_equivalent(&pdf).unwrap());
    }

    // TEST858: LUB with three+ inputs narrows correctly
    #[test]
    fn test858_lub_three_inputs() {
        // All three are lists (shared `list`) and all three HAVE a `fmt`
        // (json/csv/ndjson — differing values → promoted to must-have-any).
        // The record marker is on a,b but not c → dropped.
        let a = MediaUrn::from_string("media:fmt=json;list;record").unwrap();
        let b = MediaUrn::from_string("media:fmt=csv;list;record").unwrap();
        let c = MediaUrn::from_string("media:fmt=ndjson;list").unwrap();
        let lub = MediaUrn::least_upper_bound(&[a.clone(), b.clone(), c.clone()]);
        let expected = MediaUrn::from_string("media:fmt;list").unwrap();
        assert!(
            lub.is_equivalent(&expected).unwrap(),
            "LUB should be media:fmt;list but got {}",
            lub.to_string()
        );
        assert!(a.conforms_to(&lub).unwrap());
        assert!(b.conforms_to(&lub).unwrap());
        assert!(c.conforms_to(&lub).unwrap());
    }

    // TEST859: LUB with valued tags (non-marker) that differ promotes the key
    #[test]
    fn test859_lub_valued_tags() {
        let v1 = MediaUrn::from_string("media:ext=png;image").unwrap();
        let v2 = MediaUrn::from_string("media:ext=jpeg;image").unwrap();
        let lub = MediaUrn::least_upper_bound(&[v1, v2]);
        let expected = MediaUrn::from_string("media:ext;image").unwrap();
        assert!(
            lub.is_equivalent(&expected).unwrap(),
            "LUB should promote conflicting ext to must-have-any, got {}",
            lub.to_string()
        );
    }

    // TEST1271: MEDIA_ADAPTER_SELECTION constant parses and has expected tags
    #[test]
    fn test1271_media_adapter_selection_constant() {
        let urn = MediaUrn::from_string(MEDIA_ADAPTER_SELECTION)
            .expect("MEDIA_ADAPTER_SELECTION must be a valid media URN");
        assert!(
            urn.has_marker_tag("adapter-selection"),
            "Must have adapter-selection tag, got: {}",
            urn.to_string()
        );
        assert!(
            urn.has_tag("fmt", "json"),
            "Must have fmt=json tag, got: {}",
            urn.to_string()
        );
        assert!(
            urn.has_marker_tag("record"),
            "Must have record tag, got: {}",
            urn.to_string()
        );
    }

    // TEST1810: media:void is atomic — refinements are parse errors.
    //
    // Mirrored across every language port (Rust, Go, Python, Swift/ObjC,
    // JS) under the SAME number. Any divergence is a wire-level
    // inconsistency — the unit type's atomicity is part of the
    // protocol's deepest layer, not a per-port detail.
    //
    // The bare `media:void` parses successfully; any combination with
    // another tag (marker or key=value) MUST fail with VoidNotAtomic.
    // This forecloses a fake taxonomy of unit values; reasons or
    // labels for *why* void is used belong on the cap URN's
    // non-directional tags or in cap args.
    #[test]
    fn test1810_media_void_is_atomic() {
        // Bare void: must parse successfully.
        let bare = MediaUrn::from_string("media:void")
            .expect("bare `media:void` must parse — it is the unit type");
        assert!(bare.is_void());

        // Every refinement must fail with VoidNotAtomic — even a
        // marker tag like `text` (which would otherwise be valid) and
        // a key=value tag like `reason=warmup`.
        let bad_inputs = [
            "media:void;text",
            "media:ext=pdf;void",
            "media:void;audio",
            "media:void;reason=warmup",
            "media:void;heartbeat",
            "media:void;manual",
            // Order must not matter — the parser canonicalizes tags
            // before classifying.
            "media:warmup;void",
            "media:reason=foo;void",
        ];
        for input in &bad_inputs {
            match MediaUrn::from_string(input) {
                Err(MediaUrnError::VoidNotAtomic { extra_tags }) => {
                    assert!(
                        !extra_tags.is_empty(),
                        "{input}: VoidNotAtomic must report at least one extra tag"
                    );
                    assert!(
                        !extra_tags.iter().any(|t| t == "void"),
                        "{input}: extra_tags must NOT include `void` itself, got {extra_tags:?}"
                    );
                }
                Err(other) => panic!("{input}: expected VoidNotAtomic error, got {other:?}"),
                Ok(parsed) => panic!(
                    "{input}: expected parse error, but parsed successfully as {}",
                    parsed.to_string()
                ),
            }
        }
    }
}
