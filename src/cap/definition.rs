//! Formal cap definition
//!
//! This module defines the structure for formal cap definitions that include
//! the cap URN, versioning, and metadata. Caps are general-purpose
//! and do not assume any specific domain like files or documents.
//!
//! ## Cap Definition Format
//!
//! Caps use media URNs in `media_urn` fields. Every media URN is looked up
//! through the unified `FabricRegistry` — there is no inline media def
//! storage on a cap.
//!
//! Example:
//!
//! ```json
//! {
//!   "urn": "cap:in=\"media:string\";conversation;out=\"media:fmt=json;my-output;record\"",
//!   "args": [
//!     { "media_urn": "media:string", "required": true, "sources": [{"cli_flag": "--input"}] }
//!   ],
//!   "output": { "media_urn": "media:fmt=json;my-output;record", ... }
//! }
//! ```

use crate::urn::cap_urn::CapUrn;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

/// Source specification for argument input
///
/// Each variant serializes to a distinct JSON object with a unique key:
/// - `{"stdin": "media:..."}`
/// - `{"position": 0}`
/// - `{"cli_flag": "--flag-name"}`
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged, deny_unknown_fields)]
pub enum ArgSource {
    /// Argument can be provided via stdin
    Stdin {
        /// Media URN for stdin input
        stdin: String,
    },
    /// Argument is positional
    Position {
        /// 0-based position in argument list
        position: usize,
    },
    /// Argument uses a CLI flag
    CliFlag {
        /// CLI flag (e.g., "--input" or "-i")
        cli_flag: String,
    },
}

impl ArgSource {
    pub fn get_type(&self) -> &'static str {
        match self {
            ArgSource::Stdin { .. } => "stdin",
            ArgSource::Position { .. } => "position",
            ArgSource::CliFlag { .. } => "cli_flag",
        }
    }

    pub fn stdin_media_urn(&self) -> Option<&str> {
        match self {
            ArgSource::Stdin { stdin } => Some(stdin),
            _ => None,
        }
    }

    pub fn position(&self) -> Option<usize> {
        match self {
            ArgSource::Position { position } => Some(*position),
            _ => None,
        }
    }

    pub fn cli_flag(&self) -> Option<&str> {
        match self {
            ArgSource::CliFlag { cli_flag } => Some(cli_flag),
            _ => None,
        }
    }
}

/// Cap argument definition - media_urn is the unique identifier
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CapArg {
    /// Unique media URN for this argument
    pub media_urn: String,

    /// Whether this argument is required
    pub required: bool,

    /// Whether this argument carries a sequence of items (is_sequence=true)
    /// or a single item (is_sequence=false, the default).
    /// When true, the argument data is a sequence of values of the media type,
    /// not a single value. This is independent of the media type — e.g.,
    /// media:enc=utf-8;question with is_sequence=true means "multiple questions".
    #[serde(default)]
    pub is_sequence: bool,

    /// Whether this argument is consumed WITHOUT a length promise — incrementally,
    /// item by item or chunk by chunk, as the stream arrives (streaming=true) — or
    /// only as a complete value (streaming=false, the default).
    ///
    /// This is the consumer's capability with respect to boundedness (12.4
    /// §Unbounded Streams, L16), orthogonal to `is_sequence` (cardinality): a
    /// scalar input can stream (a transcriber windowing an open-ended wav) and a
    /// sequence input can fold (a concat that needs every item). It is a
    /// definition-level property, not a URN property — it changes how the data
    /// path is delivered, not what the cap produces. The executor forwards an
    /// unbounded upstream live only into a streaming argument; into a
    /// non-streaming argument the hop is a split boundary and the consumer is
    /// fed the bounded whole once the upstream ends (15.2 §Streaming Contracts).
    /// RULE14: only the main input may stream — side arguments are values.
    #[serde(default)]
    pub streaming: bool,

    /// How this argument can be provided
    pub sources: Vec<ArgSource>,

    /// Human-readable description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arg_description: Option<String>,

    /// Default value for optional arguments
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<serde_json::Value>,

    /// Arbitrary metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl CapArg {
    /// Declare this argument a streaming consumer (see `streaming`).
    pub fn streaming(mut self, streaming: bool) -> Self {
        self.streaming = streaming;
        self
    }

    /// The media URN the runtime demuxes this arg's input stream by: its `Stdin`
    /// source URN if it declares one, otherwise its declared slot media URN. A cap
    /// need not declare any `Stdin` source at all — a producer-fed arg may be
    /// delivered by its declared URN — so this never assumes a stdin source exists.
    pub fn stream_urn(&self) -> &str {
        self.sources
            .iter()
            .find_map(|s| match s {
                ArgSource::Stdin { stdin } => Some(stdin.as_str()),
                _ => None,
            })
            .unwrap_or(&self.media_urn)
    }

    /// Whether this arg is the cap's MAIN input relative to `in_spec` (the cap URN's
    /// `in=` value): it declares a `Stdin` source whose URN is `in=`. The main input
    /// is always the value piped in on stdin (like a Unix command's stdin), so the
    /// main arg always declares a `Stdin` source carrying `in=`. Its DECLARED slot URN
    /// may differ from that stdin URN (e.g. a `file-path` slot whose piped content is a
    /// `pdf-stream`) — the stdin URN, not the slot URN, is `in=`. The main input may
    /// ALSO be delivered by position/cli-flag, but stdin is the defining route.
    /// Compared by tagged-URN equivalence, never as strings.
    pub fn is_main_input(&self, in_spec: &crate::urn::media_urn::MediaUrn) -> bool {
        use crate::urn::media_urn::MediaUrn;
        self.sources.iter().any(|s| match s {
            ArgSource::Stdin { stdin } => MediaUrn::from_string(stdin)
                .map(|u| u.is_equivalent(in_spec).unwrap_or(false))
                .unwrap_or(false),
            _ => false,
        })
    }

    /// Create a new cap argument
    pub fn new(media_urn: impl Into<String>, required: bool, sources: Vec<ArgSource>) -> Self {
        Self {
            media_urn: media_urn.into(),
            required,
            is_sequence: false,
            streaming: false,
            sources,
            arg_description: None,
            default_value: None,
            metadata: None,
        }
    }

    /// Create a new cap argument with description
    pub fn with_description(
        media_urn: impl Into<String>,
        required: bool,
        sources: Vec<ArgSource>,
        description: impl Into<String>,
    ) -> Self {
        Self {
            media_urn: media_urn.into(),
            required,
            is_sequence: false,
            streaming: false,
            sources,
            arg_description: Some(description.into()),
            default_value: None,
            metadata: None,
        }
    }

    /// Create a fully specified argument
    pub fn with_full_definition(
        media_urn: impl Into<String>,
        required: bool,
        is_sequence: bool,
        streaming: bool,
        sources: Vec<ArgSource>,
        description: Option<String>,
        default: Option<serde_json::Value>,
        metadata: Option<serde_json::Value>,
    ) -> Self {
        Self {
            media_urn: media_urn.into(),
            required,
            is_sequence,
            streaming,
            sources,
            arg_description: description,
            default_value: default,
            metadata,
        }
    }

    /// Get the media URN
    pub fn get_media_urn(&self) -> &str {
        &self.media_urn
    }

    /// Get metadata JSON
    pub fn get_metadata(&self) -> Option<&serde_json::Value> {
        self.metadata.as_ref()
    }

    /// Set metadata JSON
    pub fn set_metadata(&mut self, metadata: serde_json::Value) {
        self.metadata = Some(metadata);
    }

    /// Clear metadata JSON
    pub fn clear_metadata(&mut self) {
        self.metadata = None;
    }
}

/// Output definition
///
/// The `media_urn` field contains a media URN (e.g., "media:object") that
/// the unified `FabricRegistry` resolves on demand.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CapOutput {
    /// Media URN referencing a media definition
    /// e.g., "media:object" or a custom media URN like "media:my-output"
    pub media_urn: String,

    pub output_description: String,

    /// Whether this output produces a sequence of items (is_sequence=true)
    /// or a single item (is_sequence=false, the default).
    #[serde(default)]
    pub is_sequence: bool,

    /// Whether this output MAY be emitted without a length promise — an
    /// unbounded stream (`STREAM_START` `unbounded=true`, 12.4 §Unbounded
    /// Streams): an open-ended capture, a transcription of one, a generator.
    /// `false` (the default) is a contract: every stream this output emits is
    /// bounded, and the executor audits each `STREAM_START` against it at
    /// receipt (a violation is `internal`, named at the cap). Orthogonal to
    /// `is_sequence`; a definition-level property, not a URN property.
    #[serde(default)]
    pub streaming: bool,

    /// Arbitrary metadata as JSON object
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl CapOutput {
    /// Create a new output definition with media URN
    ///
    /// # Arguments
    /// * `media_urn` - Media URN resolved through the FabricRegistry (e.g., "media:object")
    /// * `description` - Human-readable description of the output
    pub fn new(media_urn: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            media_urn: media_urn.into(),
            output_description: description.into(),
            is_sequence: false,
            streaming: false,
            metadata: None,
        }
    }

    /// Create a fully specified output
    pub fn with_full_definition(
        media_urn: impl Into<String>,
        description: impl Into<String>,
        is_sequence: bool,
        streaming: bool,
        metadata: Option<serde_json::Value>,
    ) -> Self {
        Self {
            media_urn: media_urn.into(),
            output_description: description.into(),
            is_sequence,
            streaming,
            metadata,
        }
    }

    /// Declare this output a possibly-unbounded emitter (see `streaming`).
    pub fn streaming(mut self, streaming: bool) -> Self {
        self.streaming = streaming;
        self
    }

    /// Get the media URN
    pub fn get_media_urn(&self) -> &str {
        &self.media_urn
    }

    /// Get metadata JSON
    pub fn get_metadata(&self) -> Option<&serde_json::Value> {
        self.metadata.as_ref()
    }

    /// Set metadata JSON
    pub fn set_metadata(&mut self, metadata: serde_json::Value) {
        self.metadata = Some(metadata);
    }

    /// Clear metadata JSON
    pub fn clear_metadata(&mut self) {
        self.metadata = None;
    }
}

/// Registration attribution - who registered this capability and when
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RegisteredBy {
    /// Username of the user who registered this capability
    pub username: String,

    /// ISO 8601 timestamp of when the capability was registered
    pub registered_at: String,
}

impl RegisteredBy {
    /// Create a new registration attribution
    pub fn new(username: impl Into<String>, registered_at: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            registered_at: registered_at.into(),
        }
    }
}

/// Formal cap definition
///
/// A cap definition includes:
/// - URN with tags (including `op`, `in`, `out` which use media URNs)
/// - Arguments with media URN references (resolved through `FabricRegistry`)
/// - Output with media URN reference (resolved through `FabricRegistry`)
#[derive(Debug, Clone, PartialEq)]
pub struct Cap {
    /// Formal cap URN with hierarchical naming
    /// Tags can include `op`, `in`, `out` (which should be media URNs)
    pub urn: CapUrn,

    /// Per-definition version. 0 means v0 (the implicit pre-versioning
    /// state at the frozen flat R2 path). >= 1 means the cap is published
    /// at `caps/<sha256-of-urn>/<version>.json` and pinned by the manifest
    /// at that defver. Source TOMLs always declare >= 1.
    pub version: u32,

    /// Human-readable title of the capability (required)
    pub title: String,

    /// Optional short plain-text description
    pub cap_description: Option<String>,

    /// Optional long-form markdown documentation.
    ///
    /// Rendered in capability info panels, the cap navigator,
    /// capdag-dot-com, and anywhere else a rich-text explanation of
    /// the cap is useful. Authored in TOML sources as a triple-quoted
    /// literal string (`'''...'''`) so markdown punctuation and
    /// newlines pass through unchanged; the JSON generator escapes
    /// newlines per JSON rules on output.
    pub documentation: Option<String>,

    /// Optional metadata as key-value pairs
    pub metadata: HashMap<String, String>,

    /// Globally-unique human-facing names that select this cap in both the
    /// capdag CLI (fabric-wide) and the direct cartridge CLI. Replaces the
    /// former non-unique `command` field. At least one; uniqueness across the
    /// whole catalogue (and against media aliases) is enforced at publish.
    pub aliases: Vec<String>,

    /// True when this cap is a generic-input dispatch umbrella: a valid alias
    /// target that is never backed by a cartridge and never a runnable graph
    /// edge. The capdag CLI narrows an abstract cap to a concrete
    /// specialization by the detected input media (`is_dispatchable`).
    pub is_abstract: bool,

    /// Cap arguments
    pub args: Vec<CapArg>,

    /// Output definition
    pub output: Option<CapOutput>,

    /// Arbitrary metadata as JSON object
    pub metadata_json: Option<serde_json::Value>,

    /// Registration attribution - who registered this capability and when
    pub registered_by: Option<RegisteredBy>,

    /// Architectures (HuggingFace `config.json` `model_type` values) the
    /// cap can run. Drives cap-aware UI filtering: model pickers and
    /// search wizards forward this list to modelcartridge so users only
    /// see runnable models. Empty means the cap accepts any
    /// architecture (or has no model dependency).
    pub supported_model_types: Vec<String>,

    /// Default model spec literal used when the cap is invoked without
    /// an explicit model-spec argument. Persisted as the unaltered
    /// input form — modelcartridge applies any architecture-driven
    /// filter adjustments at download time without changing this
    /// identity. Empty means the cap has no default model.
    pub default_model_spec: Option<String>,
}

impl Serialize for Cap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Cap", 12)?;

        // Serialize urn as canonical string format
        state.serialize_field("urn", &self.urn.to_string())?;

        // Emit version only when non-zero; absent ⇒ 0 in the wire form
        // (matches the wire-schema's optional integer-with-default-0 rule).
        if self.version != 0 {
            state.serialize_field("version", &self.version)?;
        }

        state.serialize_field("title", &self.title)?;
        state.serialize_field("aliases", &self.aliases)?;

        // Emit `abstract` only when true; absent ⇒ false in the wire form.
        if self.is_abstract {
            state.serialize_field("abstract", &self.is_abstract)?;
        }

        if self.cap_description.is_some() {
            state.serialize_field("cap_description", &self.cap_description)?;
        }

        if self.documentation.is_some() {
            state.serialize_field("documentation", &self.documentation)?;
        }

        if !self.metadata.is_empty() {
            state.serialize_field("metadata", &self.metadata)?;
        }

        if !self.args.is_empty() {
            state.serialize_field("args", &self.args)?;
        }

        if self.output.is_some() {
            state.serialize_field("output", &self.output)?;
        }

        if self.metadata_json.is_some() {
            state.serialize_field("metadata_json", &self.metadata_json)?;
        }

        if self.registered_by.is_some() {
            state.serialize_field("registered_by", &self.registered_by)?;
        }

        if !self.supported_model_types.is_empty() {
            state.serialize_field("supported_model_types", &self.supported_model_types)?;
        }

        if self.default_model_spec.is_some() {
            state.serialize_field("default_model_spec", &self.default_model_spec)?;
        }

        state.end()
    }
}

impl<'de> Deserialize<'de> for Cap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct CapWire {
            urn: serde_json::Value,
            #[serde(default)]
            version: u32,
            title: String,
            cap_description: Option<String>,
            documentation: Option<String>,
            #[serde(default)]
            metadata: HashMap<String, String>,
            aliases: Vec<String>,
            #[serde(rename = "abstract", default)]
            is_abstract: bool,
            #[serde(default)]
            args: Vec<CapArg>,
            output: Option<CapOutput>,
            metadata_json: Option<serde_json::Value>,
            registered_by: Option<RegisteredBy>,
            #[serde(default)]
            supported_model_types: Vec<String>,
            #[serde(default)]
            default_model_spec: Option<String>,
        }

        let wire = CapWire::deserialize(deserializer)?;

        // URN must be a string in canonical format
        let urn = match wire.urn {
            serde_json::Value::String(urn_str) => {
                CapUrn::from_string(&urn_str).map_err(serde::de::Error::custom)?
            },
            _ => return Err(serde::de::Error::custom("urn must be a string in canonical format (e.g., 'cap:in=\"media:...\";op=...;out=\"media:...\"')")),
        };

        // A cap must declare at least one alias — it is how the cap is
        // selected in both CLIs. An empty (or absent) list is a hard error,
        // never silently defaulted.
        if wire.aliases.is_empty() {
            return Err(serde::de::Error::custom(format!(
                "cap '{}' must declare at least one alias (the `aliases` field is required and non-empty)",
                urn
            )));
        }

        Ok(Cap {
            urn,
            version: wire.version,
            title: wire.title,
            cap_description: wire.cap_description,
            documentation: wire.documentation,
            metadata: wire.metadata,
            aliases: wire.aliases,
            is_abstract: wire.is_abstract,
            args: wire.args,
            output: wire.output,
            metadata_json: wire.metadata_json,
            registered_by: wire.registered_by,
            supported_model_types: wire.supported_model_types,
            default_model_spec: wire.default_model_spec,
        })
    }
}

impl Cap {
    /// Create a new cap
    pub fn new(urn: CapUrn, title: String, aliases: Vec<String>) -> Self {
        Self {
            urn,
            version: 0,
            title,
            cap_description: None,
            documentation: None,
            metadata: HashMap::new(),
            aliases,
            is_abstract: false,
            args: Vec::new(),
            output: None,
            metadata_json: None,
            registered_by: None,
            supported_model_types: Vec::new(),
            default_model_spec: None,
        }
    }

    /// Create a new cap with description
    pub fn with_description(
        urn: CapUrn,
        title: String,
        aliases: Vec<String>,
        description: String,
    ) -> Self {
        Self {
            urn,
            version: 0,
            title,
            cap_description: Some(description),
            documentation: None,
            metadata: HashMap::new(),
            aliases,
            is_abstract: false,
            args: Vec::new(),
            output: None,
            metadata_json: None,
            registered_by: None,
            supported_model_types: Vec::new(),
            default_model_spec: None,
        }
    }

    /// Create a new cap with metadata
    pub fn with_metadata(
        urn: CapUrn,
        title: String,
        aliases: Vec<String>,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            urn,
            version: 0,
            title,
            cap_description: None,
            documentation: None,
            metadata,
            aliases,
            is_abstract: false,
            args: Vec::new(),
            output: None,
            metadata_json: None,
            registered_by: None,
            supported_model_types: Vec::new(),
            default_model_spec: None,
        }
    }

    /// Create a new cap with description and metadata
    pub fn with_description_and_metadata(
        urn: CapUrn,
        title: String,
        aliases: Vec<String>,
        description: String,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            urn,
            version: 0,
            title,
            cap_description: Some(description),
            documentation: None,
            metadata,
            aliases,
            is_abstract: false,
            args: Vec::new(),
            output: None,
            metadata_json: None,
            registered_by: None,
            supported_model_types: Vec::new(),
            default_model_spec: None,
        }
    }

    /// Create a new cap with args
    pub fn with_args(urn: CapUrn, title: String, aliases: Vec<String>, args: Vec<CapArg>) -> Self {
        Self {
            urn,
            version: 0,
            title,
            cap_description: None,
            documentation: None,
            metadata: HashMap::new(),
            aliases,
            is_abstract: false,
            args,
            output: None,
            metadata_json: None,
            registered_by: None,
            supported_model_types: Vec::new(),
            default_model_spec: None,
        }
    }

    /// Create a fully specified cap
    pub fn with_full_definition(
        urn: CapUrn,
        title: String,
        description: Option<String>,
        metadata: HashMap<String, String>,
        aliases: Vec<String>,
        args: Vec<CapArg>,
        output: Option<CapOutput>,
        metadata_json: Option<serde_json::Value>,
    ) -> Self {
        Self {
            urn,
            version: 0,
            title,
            cap_description: description,
            documentation: None,
            metadata,
            aliases,
            is_abstract: false,
            args,
            output,
            metadata_json,
            registered_by: None,
            supported_model_types: Vec::new(),
            default_model_spec: None,
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

    /// Get the stdin media URN from args (first stdin source found)
    pub fn get_stdin_media_urn(&self) -> Option<&str> {
        for arg in &self.args {
            for source in &arg.sources {
                if let ArgSource::Stdin { stdin } = source {
                    return Some(stdin);
                }
            }
        }
        None
    }

    /// Check if this cap accepts stdin
    pub fn accepts_stdin(&self) -> bool {
        self.get_stdin_media_urn().is_some()
    }

    /// Cardinality shape of this cap's primary data path:
    /// `(input_is_sequence, output_is_sequence)`.
    ///
    /// `input_is_sequence` is the `is_sequence` flag of the arg whose `Stdin`
    /// source matches the cap URN's `in=` spec — argument declaration order has
    /// no semantics. `output_is_sequence` is the output's `is_sequence` flag.
    ///
    /// This is THE single definition of cap cardinality. Path search
    /// (`planner::live_cap_fab::get_outgoing_edges`), editor realization
    /// (`realize_runtime_linear_machine_strand`), and notation resolution
    /// (`machine::resolve`) all read it here so they can never diverge — the
    /// distinction that decides whether a ForEach is synthesized.
    pub fn sequence_shape(&self) -> (bool, bool) {
        let in_spec = crate::urn::media_urn::MediaUrn::from_string(self.urn.in_spec())
            .expect("cap registry invariant: cap in= is a valid MediaUrn");
        let void_media =
            crate::urn::media_urn::MediaUrn::from_string(crate::urn::media_urn::MEDIA_VOID)
                .expect("MEDIA_VOID is a valid MediaUrn");
        let input_is_sequence = if in_spec
            .is_equivalent(&void_media)
            .expect("cap registry invariant: cardinality media URNs are comparable")
            || self.args.is_empty()
        {
            // Void input, or a cap that declares no arguments at all — the
            // published `discard` cap (`cap:out=media:void`, the terminal
            // morphism) is argless by design. Nothing declares a cardinality,
            // so the input is scalar.
            false
        } else {
            match self.args.iter().find(|arg| arg.is_main_input(&in_spec)) {
                Some(arg) => arg.is_sequence,
                None => {
                    // Publisher RULE11 guarantees every cap with arguments
                    // declares its main input; a definition that reaches here
                    // slipped through publish validation. Registry content
                    // must never crash a client at graph-build, so report the
                    // violation loudly and read the input as scalar.
                    tracing::error!(
                        cap_urn = %self.urn,
                        "cap violates RULE11: it declares arguments but none is the main input \
                         (stdin source equivalent to in=) — fix the fabric definition"
                    );
                    false
                }
            }
        };
        let output_is_sequence = self.output.as_ref().map_or(false, |o| o.is_sequence);
        (input_is_sequence, output_is_sequence)
    }

    /// The main input argument — the one whose `Stdin` source is equivalent
    /// to the cap URN's `in=` spec. `None` for a void-input or argless cap.
    pub fn main_input_arg(&self) -> Option<&CapArg> {
        let in_spec = crate::urn::media_urn::MediaUrn::from_string(self.urn.in_spec()).ok()?;
        self.args.iter().find(|arg| arg.is_main_input(&in_spec))
    }

    /// Streaming shape of this cap's primary data path:
    /// `(input_streams, output_may_be_unbounded)` — the `streaming` flags of the
    /// main input argument and of the output (15.2 §Streaming Contracts).
    ///
    /// THE single definition, read by the executor's hop rule (a streaming
    /// producer into a non-streaming consumer is a split boundary) and by the
    /// stream-contract audit (an unbounded STREAM_START from an output declared
    /// non-streaming is a violation). A void-input or argless cap has a
    /// non-streaming input: there is nothing to stream into it.
    pub fn streaming_shape(&self) -> (bool, bool) {
        let input_streams = self.main_input_arg().map_or(false, |arg| arg.streaming);
        let output_streams = self.output.as_ref().map_or(false, |o| o.streaming);
        (input_streams, output_streams)
    }

    /// Whether a data position of cardinality `source_is_sequence` feeding this cap's
    /// primary input requires a ForEach (per-item map) to be inserted before it.
    ///
    /// The one rule, shared by every planner/resolver path: a sequence feeding a
    /// scalar-input cap must be mapped. Mirrors `get_outgoing_edges` line 673
    /// (`is_sequence && !input_is_sequence`). The media URN does not change — ForEach
    /// is a shape transition, not a type transition.
    pub fn needs_foreach(&self, source_is_sequence: bool) -> bool {
        let (input_is_sequence, _) = self.sequence_shape();
        source_is_sequence && !input_is_sequence
    }

    /// Check if this cap (candidate) can dispatch the given request.
    ///
    /// Uses `is_dispatchable` which correctly handles the 3-axis Cap URN matching:
    /// - Input axis: candidate can handle request's input (same or more specific)
    /// - Output axis: candidate meets request's output needs (same or more specific)
    /// - Cap-tags axis: candidate satisfies all explicit request constraints
    pub fn accepts_request(&self, request: &str) -> bool {
        let request_urn = CapUrn::from_string(request).expect("Invalid cap URN in request");
        self.urn.is_dispatchable(&request_urn)
    }

    /// Get the cap URN as a string
    pub fn urn_string(&self) -> String {
        self.urn.to_string()
    }

    /// Check if this cap is more specific than another for the same request
    pub fn is_more_specific_than(&self, other: &Cap, request: &str) -> bool {
        if !self.accepts_request(request) || !other.accepts_request(request) {
            return false;
        }
        self.urn.is_more_specific_than(&other.urn)
    }

    /// Get a metadata value by key
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }

    /// Set a metadata value
    pub fn set_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    /// Remove a metadata value
    pub fn remove_metadata(&mut self, key: &str) -> Option<String> {
        self.metadata.remove(key)
    }

    /// Check if this cap has specific metadata
    pub fn has_metadata(&self, key: &str) -> bool {
        self.metadata.contains_key(key)
    }

    /// Get the registration attribution
    pub fn get_registered_by(&self) -> Option<&RegisteredBy> {
        self.registered_by.as_ref()
    }

    /// Set the registration attribution
    pub fn set_registered_by(&mut self, registered_by: RegisteredBy) {
        self.registered_by = Some(registered_by);
    }

    /// Clear the registration attribution
    pub fn clear_registered_by(&mut self) {
        self.registered_by = None;
    }

    /// Get the cap's aliases (globally-unique selection names).
    pub fn get_aliases(&self) -> &[String] {
        &self.aliases
    }

    /// Set the cap's aliases.
    pub fn set_aliases(&mut self, aliases: Vec<String>) {
        self.aliases = aliases;
    }

    /// The primary (first) alias — used for single-name display (help text,
    /// listings). A cap always has at least one alias.
    pub fn primary_alias(&self) -> &str {
        self.aliases.first().map(|s| s.as_str()).unwrap_or_default()
    }

    /// Whether `name` is one of this cap's aliases (exact match).
    pub fn has_alias(&self, name: &str) -> bool {
        self.aliases.iter().any(|a| a == name)
    }

    /// Whether this cap is an abstract dispatch umbrella (never backed by a
    /// cartridge, never a runnable graph edge).
    pub fn is_abstract(&self) -> bool {
        self.is_abstract
    }

    /// Mark this cap abstract or concrete.
    pub fn set_abstract(&mut self, is_abstract: bool) {
        self.is_abstract = is_abstract;
    }

    /// Get the title
    pub fn get_title(&self) -> &String {
        &self.title
    }

    /// Set the title
    pub fn set_title(&mut self, title: String) {
        self.title = title;
    }

    /// Get the args
    pub fn get_args(&self) -> &Vec<CapArg> {
        &self.args
    }

    /// Set the args
    pub fn set_args(&mut self, args: Vec<CapArg>) {
        self.args = args;
    }

    /// Add an argument
    pub fn add_arg(&mut self, arg: CapArg) {
        self.args.push(arg);
    }

    /// Get the output definition if defined
    pub fn get_output(&self) -> Option<&CapOutput> {
        self.output.as_ref()
    }

    /// Set the output definition
    pub fn set_output(&mut self, output: CapOutput) {
        self.output = Some(output);
    }

    /// Get metadata JSON
    pub fn get_metadata_json(&self) -> Option<&serde_json::Value> {
        self.metadata_json.as_ref()
    }

    /// Set metadata JSON
    pub fn set_metadata_json(&mut self, metadata: serde_json::Value) {
        self.metadata_json = Some(metadata);
    }

    /// Clear metadata JSON
    pub fn clear_metadata_json(&mut self) {
        self.metadata_json = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create test URN with required in/out specs
    fn test_urn(tags: &str) -> String {
        format!(r#"cap:in="media:void";out="media:record";{}"#, tags)
    }

    // TEST108: Test creating new cap with URN, title, and command verifies correct initialization
    #[test]
    fn test108_cap_creation() {
        let urn = CapUrn::from_string(&test_urn("transform;format=json;data_processing")).unwrap();
        let cap = Cap::new(
            urn,
            "Transform JSON Data".to_string(),
            vec!["test-command".to_string()],
        );

        assert!(cap.urn_string().contains("transform"));
        // Check that in/out specs are present (format may vary due to canonicalization)
        assert!(cap.urn_string().contains("in="));
        assert!(cap.urn_string().contains("media:void"));
        assert!(cap.urn_string().contains("out="));
        assert!(cap.urn_string().contains("record"));
        assert_eq!(cap.title, "Transform JSON Data");
        assert!(cap.metadata.is_empty());
    }

    // TEST109: Test creating cap with metadata initializes and retrieves metadata correctly
    #[test]
    fn test109_cap_with_metadata() {
        let urn = CapUrn::from_string(&test_urn("arithmetic;compute;subtype=math")).unwrap();
        let mut metadata = HashMap::new();
        metadata.insert("precision".to_string(), "double".to_string());
        metadata.insert(
            "operations".to_string(),
            "add,subtract,multiply,divide".to_string(),
        );

        let cap = Cap::with_metadata(
            urn,
            "Perform Mathematical Operations".to_string(),
            vec!["test-command".to_string()],
            metadata,
        );

        assert_eq!(cap.title, "Perform Mathematical Operations");
        assert_eq!(cap.get_metadata("precision"), Some(&"double".to_string()));
        assert_eq!(
            cap.get_metadata("operations"),
            Some(&"add,subtract,multiply,divide".to_string())
        );
        assert!(cap.has_metadata("precision"));
        assert!(!cap.has_metadata("nonexistent"));
    }

    // TEST110: Test cap matching with subset semantics for request fulfillment
    #[test]
    fn test110_cap_matching() {
        // Use type=data_processing key-value instead of flag for proper matching
        let urn =
            CapUrn::from_string(&test_urn("transform;format=json;type=data_processing")).unwrap();
        let cap = Cap::new(
            urn,
            "Transform JSON Data".to_string(),
            vec!["test-command".to_string()],
        );

        assert!(cap.accepts_request(&test_urn("transform;format=json;type=data_processing")));
        assert!(cap.accepts_request(&test_urn("transform;format=*;type=data_processing")));
        assert!(cap.accepts_request(&test_urn("type=data_processing")));
        assert!(!cap.accepts_request(&test_urn("type=compute")));
    }

    // TEST111: Test getting and setting cap title updates correctly
    #[test]
    fn test111_cap_title() {
        let urn = CapUrn::from_string(&test_urn("extract;target=metadata")).unwrap();
        let mut cap = Cap::new(
            urn,
            "Extract Document Metadata".to_string(),
            vec!["extract-metadata".to_string()],
        );

        assert_eq!(cap.get_title(), &"Extract Document Metadata".to_string());
        assert_eq!(cap.title, "Extract Document Metadata");

        cap.set_title("Extract File Metadata".to_string());
        assert_eq!(cap.get_title(), &"Extract File Metadata".to_string());
        assert_eq!(cap.title, "Extract File Metadata");
    }

    // TEST112: Test cap equality based on URN and title matching
    #[test]
    fn test112_cap_definition_equality() {
        let urn1 = CapUrn::from_string(&test_urn("transform;format=json")).unwrap();
        let urn2 = CapUrn::from_string(&test_urn("transform;format=json")).unwrap();

        let cap1 = Cap::new(
            urn1,
            "Transform JSON Data".to_string(),
            vec!["transform".to_string()],
        );
        let cap2 = Cap::new(
            urn2.clone(),
            "Transform JSON Data".to_string(),
            vec!["transform".to_string()],
        );
        let cap3 = Cap::new(
            urn2,
            "Convert JSON Format".to_string(),
            vec!["transform".to_string()],
        );

        assert_eq!(cap1, cap2);
        assert_ne!(cap1, cap3);
        assert_ne!(cap2, cap3);
    }

    // TEST113: Test cap stdin support via args with stdin source and serialization roundtrip
    #[test]
    fn test113_cap_stdin() {
        let urn = CapUrn::from_string(&test_urn("generate;target=embeddings")).unwrap();
        let mut cap = Cap::new(
            urn,
            "Generate Embeddings".to_string(),
            vec!["generate".to_string()],
        );

        // By default, caps should not accept stdin
        assert!(!cap.accepts_stdin());
        assert!(cap.get_stdin_media_urn().is_none());

        // Enable stdin support by adding an arg with a stdin source
        let stdin_arg = CapArg {
            media_urn: "media:enc=utf-8".to_string(),
            required: true,
            is_sequence: false,
            streaming: false,
            sources: vec![ArgSource::Stdin {
                stdin: "media:enc=utf-8".to_string(),
            }],
            arg_description: Some("Input text".to_string()),
            default_value: None,
            metadata: None,
        };
        cap.add_arg(stdin_arg);

        assert!(cap.accepts_stdin());
        assert_eq!(cap.get_stdin_media_urn(), Some("media:enc=utf-8"));

        // Test serialization/deserialization preserves the args
        let serialized = serde_json::to_string(&cap).unwrap();
        assert!(serialized.contains("\"args\""));
        assert!(serialized.contains("\"stdin\""));
        let deserialized: Cap = serde_json::from_str(&serialized).unwrap();
        assert!(deserialized.accepts_stdin());
        assert_eq!(deserialized.get_stdin_media_urn(), Some("media:enc=utf-8"));
    }

    // TEST114: Test ArgSource type variants stdin, position, and cli_flag with their accessors
    #[test]
    fn test114_arg_source_types() {
        // Test stdin source
        let stdin_source = ArgSource::Stdin {
            stdin: "media:text".to_string(),
        };
        assert_eq!(stdin_source.get_type(), "stdin");
        assert_eq!(stdin_source.stdin_media_urn(), Some("media:text"));
        assert_eq!(stdin_source.position(), None);
        assert_eq!(stdin_source.cli_flag(), None);

        // Test position source
        let position_source = ArgSource::Position { position: 0 };
        assert_eq!(position_source.get_type(), "position");
        assert_eq!(position_source.stdin_media_urn(), None);
        assert_eq!(position_source.position(), Some(0));
        assert_eq!(position_source.cli_flag(), None);

        // Test cli_flag source
        let cli_flag_source = ArgSource::CliFlag {
            cli_flag: "--input".to_string(),
        };
        assert_eq!(cli_flag_source.get_type(), "cli_flag");
        assert_eq!(cli_flag_source.stdin_media_urn(), None);
        assert_eq!(cli_flag_source.position(), None);
        assert_eq!(cli_flag_source.cli_flag(), Some("--input"));
    }

    // TEST115: Test CapArg serialization and deserialization with multiple sources
    #[test]
    fn test115_cap_arg_serialization() {
        let arg = CapArg {
            media_urn: "media:string".to_string(),
            required: true,
            is_sequence: false,
            streaming: false,
            sources: vec![
                ArgSource::CliFlag {
                    cli_flag: "--name".to_string(),
                },
                ArgSource::Position { position: 0 },
            ],
            arg_description: Some("The name argument".to_string()),
            default_value: Some(serde_json::json!(400)),
            metadata: Some(serde_json::json!({
                "kind": "example",
                "flags": [true, false]
            })),
        };

        let serialized = serde_json::to_string(&arg).unwrap();
        assert!(serialized.contains("\"media_urn\":\"media:string\""));
        assert!(serialized.contains("\"required\":true"));
        assert!(serialized.contains("\"cli_flag\":\"--name\""));
        assert!(serialized.contains("\"position\":0"));
        assert!(serialized.contains("\"default_value\":400"));
        let serialized_value: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        assert_eq!(
            serialized_value["metadata"],
            serde_json::json!({
                "kind": "example",
                "flags": [true, false]
            })
        );

        let deserialized: CapArg = serde_json::from_str(&serialized).unwrap();
        assert_eq!(arg, deserialized);
    }

    // TEST116: Test CapArg constructor methods basic and with_description create args correctly
    #[test]
    fn test116_cap_arg_constructors() {
        // Test basic constructor
        let arg = CapArg::new(
            "media:string",
            true,
            vec![ArgSource::CliFlag {
                cli_flag: "--name".to_string(),
            }],
        );
        assert_eq!(arg.media_urn, "media:string");
        assert!(arg.required);
        assert_eq!(arg.sources.len(), 1);
        assert!(arg.arg_description.is_none());

        // Test with description
        let arg = CapArg::with_description(
            "media:integer",
            false,
            vec![ArgSource::Position { position: 0 }],
            "The count argument",
        );
        assert_eq!(arg.media_urn, "media:integer");
        assert!(!arg.required);
        assert_eq!(arg.arg_description, Some("The count argument".to_string()));
    }

    // TEST591: is_more_specific_than returns true when self has more tags for same request
    #[test]
    fn test591_is_more_specific_than() {
        let general = Cap::new(
            CapUrn::from_string(&test_urn("transform")).unwrap(),
            "General".to_string(),
            vec!["cmd".to_string()],
        );
        let specific = Cap::new(
            CapUrn::from_string(&test_urn("transform;format=json")).unwrap(),
            "Specific".to_string(),
            vec!["cmd".to_string()],
        );
        let unrelated = Cap::new(
            CapUrn::from_string(&test_urn("convert")).unwrap(),
            "Unrelated".to_string(),
            vec!["cmd".to_string()],
        );

        // Specific is more specific than general for the general request
        assert!(
            specific.is_more_specific_than(&general, &test_urn("transform")),
            "specific cap must be more specific than general"
        );
        assert!(
            !general.is_more_specific_than(&specific, &test_urn("transform")),
            "general cap must not be more specific than specific"
        );

        // If either doesn't accept the request, returns false
        assert!(
            !general.is_more_specific_than(&unrelated, &test_urn("transform")),
            "unrelated cap doesn't accept request, so no comparison possible"
        );
    }

    // TEST592: remove_metadata adds then removes metadata correctly
    #[test]
    fn test592_remove_metadata() {
        let urn = CapUrn::from_string(&test_urn("test")).unwrap();
        let mut cap = Cap::new(urn, "Test".to_string(), vec!["cmd".to_string()]);

        cap.set_metadata("key1".to_string(), "val1".to_string());
        cap.set_metadata("key2".to_string(), "val2".to_string());
        assert!(cap.has_metadata("key1"));
        assert!(cap.has_metadata("key2"));

        let removed = cap.remove_metadata("key1");
        assert_eq!(removed, Some("val1".to_string()));
        assert!(!cap.has_metadata("key1"));
        assert!(cap.has_metadata("key2"));

        // Removing non-existent returns None
        assert_eq!(cap.remove_metadata("nonexistent"), None);
    }

    // TEST593: registered_by lifecycle — set, get, clear
    #[test]
    fn test593_registered_by_lifecycle() {
        let urn = CapUrn::from_string(&test_urn("test")).unwrap();
        let mut cap = Cap::new(urn, "Test".to_string(), vec!["cmd".to_string()]);

        // Initially None
        assert!(cap.get_registered_by().is_none());

        // Set
        let reg = RegisteredBy::new("alice", "2026-02-19T10:00:00Z");
        cap.set_registered_by(reg);
        let got = cap.get_registered_by().expect("should have registered_by");
        assert_eq!(got.username, "alice");
        assert_eq!(got.registered_at, "2026-02-19T10:00:00Z");

        // Clear
        cap.clear_registered_by();
        assert!(cap.get_registered_by().is_none());
    }

    // TEST594: metadata_json lifecycle — set, get, clear
    #[test]
    fn test594_metadata_json_lifecycle() {
        let urn = CapUrn::from_string(&test_urn("test")).unwrap();
        let mut cap = Cap::new(urn, "Test".to_string(), vec!["cmd".to_string()]);

        // Initially None
        assert!(cap.get_metadata_json().is_none());

        // Set
        let json = serde_json::json!({"version": 2, "tags": ["experimental"]});
        cap.set_metadata_json(json.clone());
        assert_eq!(cap.get_metadata_json(), Some(&json));

        // Clear
        cap.clear_metadata_json();
        assert!(cap.get_metadata_json().is_none());
    }

    // TEST595: with_args constructor stores args correctly
    #[test]
    fn test595_with_args_constructor() {
        let urn = CapUrn::from_string(&test_urn("test")).unwrap();
        let args = vec![
            CapArg::new(
                "media:string",
                true,
                vec![ArgSource::Position { position: 0 }],
            ),
            CapArg::new(
                "media:integer",
                false,
                vec![ArgSource::CliFlag {
                    cli_flag: "--count".to_string(),
                }],
            ),
        ];

        let cap = Cap::with_args(urn, "Test".to_string(), vec!["cmd".to_string()], args);
        assert_eq!(cap.get_args().len(), 2);
        assert_eq!(cap.get_args()[0].media_urn, "media:string");
        assert!(cap.get_args()[0].required);
        assert_eq!(cap.get_args()[1].media_urn, "media:integer");
        assert!(!cap.get_args()[1].required);
    }

    // TEST596: with_full_definition constructor stores all fields
    #[test]
    fn test596_with_full_definition_constructor() {
        let urn = CapUrn::from_string(&test_urn("test")).unwrap();
        let mut metadata = HashMap::new();
        metadata.insert("env".to_string(), "prod".to_string());
        let args = vec![CapArg::new("media:string", true, vec![])];
        let output = CapOutput::new("media:object", "Output object");
        let json_meta = serde_json::json!({"v": 1});

        let cap = Cap::with_full_definition(
            urn,
            "Full Cap".to_string(),
            Some("Description".to_string()),
            metadata,
            vec!["full-cmd".to_string()],
            args,
            Some(output),
            Some(json_meta.clone()),
        );

        assert_eq!(cap.title, "Full Cap");
        assert_eq!(cap.cap_description, Some("Description".to_string()));
        assert_eq!(cap.get_metadata("env"), Some(&"prod".to_string()));
        assert_eq!(cap.primary_alias(), "full-cmd");
        assert_eq!(cap.get_args().len(), 1);
        assert!(cap.get_output().is_some());
        assert_eq!(cap.get_output().unwrap().media_urn, "media:object");
        assert_eq!(cap.get_metadata_json(), Some(&json_meta));
        // registered_by is not set by with_full_definition
        assert!(cap.get_registered_by().is_none());
    }

    // TEST597: CapArg::with_full_definition stores all fields including optional ones
    #[test]
    fn test597_cap_arg_with_full_definition() {
        let default_val = serde_json::json!({
            "chunk_size": 400,
            "timestamps": false
        });
        let meta = serde_json::json!({"hint": "enter name"});

        let arg = CapArg::with_full_definition(
            "media:string",
            true,
            false,
            false,
            vec![ArgSource::CliFlag {
                cli_flag: "--name".to_string(),
            }],
            Some("User name".to_string()),
            Some(default_val.clone()),
            Some(meta.clone()),
        );

        assert_eq!(arg.media_urn, "media:string");
        assert!(arg.required);
        assert_eq!(arg.arg_description, Some("User name".to_string()));
        assert_eq!(arg.default_value, Some(default_val));
        assert_eq!(arg.get_metadata(), Some(&meta));

        // Metadata lifecycle
        let mut arg2 = arg.clone();
        arg2.clear_metadata();
        assert!(arg2.get_metadata().is_none());
        arg2.set_metadata(serde_json::json!("new"));
        assert_eq!(arg2.get_metadata(), Some(&serde_json::json!("new")));
    }

    // TEST598: CapOutput lifecycle — set_output, set/clear metadata
    #[test]
    fn test598_cap_output_lifecycle() {
        let urn = CapUrn::from_string(&test_urn("test")).unwrap();
        let mut cap = Cap::new(urn, "Test".to_string(), vec!["cmd".to_string()]);

        // Initially no output
        assert!(cap.get_output().is_none());

        // Set output
        let mut output = CapOutput::new("media:string", "Text output");
        output.set_metadata(serde_json::json!({"format": "plain"}));
        cap.set_output(output);

        let got = cap.get_output().expect("output should be set");
        assert_eq!(got.get_media_urn(), "media:string");
        assert_eq!(got.output_description, "Text output");
        assert!(got.get_metadata().is_some());

        // CapOutput with_full_definition
        let output2 = CapOutput::with_full_definition(
            "media:fmt=json",
            "JSON output",
            false,
            false,
            Some(serde_json::json!({"v": 2})),
        );
        assert_eq!(output2.get_media_urn(), "media:fmt=json");
        assert!(output2.get_metadata().is_some());

        // Clear metadata on output
        let mut output3 = output2.clone();
        output3.clear_metadata();
        assert!(output3.get_metadata().is_none());
    }

    // TEST8060: A cap wire body MUST declare at least one alias. Aliases are how
    // a cap is selected in both CLIs; there is no `command` fallback and no
    // silent default — a body without aliases (or with an empty list) is a hard
    // deserialization error. This exposes any producer that emits a cap without
    // the required selection name(s).
    #[test]
    fn test8060_deserialize_requires_non_empty_aliases() {
        // Absent `aliases` → error.
        let no_aliases = r#"{"urn":"cap:effect=none","title":"Identity"}"#;
        assert!(
            serde_json::from_str::<Cap>(no_aliases).is_err(),
            "a cap wire body without `aliases` must fail to deserialize"
        );
        // Empty `aliases` → error.
        let empty_aliases = r#"{"urn":"cap:effect=none","title":"Identity","aliases":[]}"#;
        assert!(
            serde_json::from_str::<Cap>(empty_aliases).is_err(),
            "a cap wire body with an empty `aliases` list must fail to deserialize"
        );
        // A legacy body carrying only `command` (no aliases) must ALSO fail —
        // the old field is gone, not a fallback.
        let legacy_command = r#"{"urn":"cap:effect=none","title":"Identity","command":"identity"}"#;
        assert!(
            serde_json::from_str::<Cap>(legacy_command).is_err(),
            "a legacy `command`-only cap body must fail — `command` is not a fallback for `aliases`"
        );
        // A valid body with one alias round-trips.
        let ok = r#"{"urn":"cap:effect=none","title":"Identity","aliases":["identity"]}"#;
        let cap: Cap = serde_json::from_str(ok).expect("valid cap must deserialize");
        assert_eq!(cap.get_aliases(), &["identity".to_string()]);
        assert_eq!(cap.primary_alias(), "identity");
        assert!(cap.has_alias("identity"));
        assert!(!cap.is_abstract());
    }

    // TEST8061: The `abstract` flag round-trips and is absent-⇒-false. It is
    // emitted ONLY when true (matching the wire schema's optional-with-default),
    // so a concrete cap never carries `"abstract":false`.
    #[test]
    fn test8061_abstract_flag_roundtrip() {
        // Absent → false; serialize omits it. (The URN's media-spec quotes are
        // JSON-escaped as \" so the wire body is valid JSON.)
        let concrete: Cap = serde_json::from_str(
            r#"{"urn":"cap:disbind;in=\"media:ext=pdf\";out=\"media:enc=utf-8\"","title":"Disbind PDF","aliases":["disbind-pdf"]}"#,
        )
        .expect("concrete cap must deserialize");
        assert!(!concrete.is_abstract());
        let concrete_json = serde_json::to_string(&concrete).unwrap();
        assert!(
            !concrete_json.contains("abstract"),
            "a concrete cap must not serialize an `abstract` field, got: {concrete_json}"
        );

        // Present true → is_abstract, and serialize emits it.
        let abstract_cap: Cap = serde_json::from_str(
            r#"{"urn":"cap:disbind;in=\"media:\";out=\"media:enc=utf-8\"","title":"Disbind","aliases":["disbind"],"abstract":true}"#,
        )
        .expect("abstract cap must deserialize");
        assert!(abstract_cap.is_abstract());
        let abstract_json = serde_json::to_string(&abstract_cap).unwrap();
        assert!(
            abstract_json.contains("\"abstract\":true"),
            "an abstract cap must serialize `\"abstract\":true`, got: {abstract_json}"
        );
    }

    // TEST1127: Documentation field round-trips through JSON serialize/deserialize.
    //
    // The documentation field carries an arbitrary markdown body authored
    // in the source TOML via the triple-quoted literal string syntax. The
    // round-trip must preserve every character — including newlines,
    // backticks, double quotes, and Unicode — because consumers (info
    // panels, capdag.com, etc.) render it directly. JSON.stringify on the
    // capfab side and the Rust serializer on this side must agree on
    // escaping; this test fails hard if they don't.
    #[test]
    fn test1127_cap_documentation_round_trip_with_markdown_body() {
        let urn = CapUrn::from_string(&test_urn("documented")).unwrap();
        let mut cap = Cap::new(
            urn,
            "Documented Cap".to_string(),
            vec!["documented".to_string()],
        );

        // A non-trivial markdown body — multi-line, headings, code blocks,
        // backticks, embedded quotes, and a literal CRLF and Unicode dingbat
        // (★) — to make sure escaping is end-to-end correct.
        let body =
            "# Documented Cap\r\n\nDoes the thing.\n\n```bash\necho \"hi\"\n```\n\nSee also: ★\n";
        cap.set_documentation(body);
        assert_eq!(cap.get_documentation(), Some(body));

        let serialized = serde_json::to_string(&cap).unwrap();
        // The serializer must emit the documentation field; if it doesn't,
        // the JSON regression test for the absent case will mask this.
        assert!(
            serialized.contains("\"documentation\""),
            "documentation field absent in JSON output: {}",
            serialized
        );

        let deserialized: Cap = serde_json::from_str(&serialized).unwrap();
        assert_eq!(
            deserialized.get_documentation(),
            Some(body),
            "documentation body mutated during round-trip"
        );

        // Identity through clone/equality
        let cloned = deserialized.clone();
        assert_eq!(cloned, deserialized);
    }

    // TEST1128: When documentation is None, the serializer must skip the
    // field entirely. This matches the behaviour of the JS toJSON, the
    // ObjC toDictionary, and the schema's "if present" semantics — there
    // is no null sentinel, only absence. A bug here would silently start
    // emitting `"documentation":null` and break consumers that distinguish
    // between absent and explicit null.
    #[test]
    fn test1128_cap_documentation_omitted_when_none() {
        let urn = CapUrn::from_string(&test_urn("undocumented")).unwrap();
        let cap = Cap::new(
            urn,
            "Undocumented Cap".to_string(),
            vec!["undocumented".to_string()],
        );
        assert!(cap.get_documentation().is_none());

        let serialized = serde_json::to_string(&cap).unwrap();
        assert!(
            !serialized.contains("documentation"),
            "documentation field must be omitted when None, got: {}",
            serialized
        );

        // Round-trip through deserialize: should still be None.
        let deserialized: Cap = serde_json::from_str(&serialized).unwrap();
        assert!(deserialized.get_documentation().is_none());
    }

    // TEST1129: A JSON document produced by capfab (the canonical source)
    // with a `documentation` field must deserialize into a Cap with the
    // body intact. Models the actual on-disk shape — not a synthetic
    // round-trip — to catch a mismatch between the JSON schema and the
    // Rust struct field naming.
    #[test]
    fn test1129_cap_documentation_parses_from_capfab_json() {
        // Build JSON via serde_json::json! so we don't have to fight raw
        // string escaping rules — the URN value contains both backslashes
        // and embedded double quotes.
        let json = serde_json::json!({
            "urn": "cap:in=\"media:enc=utf-8\";docparse;out=\"media:enc=utf-8\"",
            "title": "Doc Parse",
            "aliases": ["docparse"],
            "cap_description": "short",
            "documentation": "## Heading\n\nbody text",
            "metadata": {}
        })
        .to_string();
        let cap: Cap = serde_json::from_str(&json).expect("must parse capfab-shaped JSON");
        assert_eq!(cap.get_documentation(), Some("## Heading\n\nbody text"));
        assert_eq!(cap.cap_description.as_deref(), Some("short"));
    }

    // TEST1130: documentation set/clear lifecycle parallels cap_description.
    // Catches a regression where the setter or clearer is wired to the wrong
    // field — for example, set_documentation accidentally writing to
    // cap_description.
    #[test]
    fn test1130_cap_documentation_set_and_clear_lifecycle() {
        let urn = CapUrn::from_string(&test_urn("lifecycle")).unwrap();
        let mut cap = Cap::with_description(
            urn,
            "Lifecycle".to_string(),
            vec!["lifecycle".to_string()],
            "short".to_string(),
        );
        assert_eq!(cap.cap_description.as_deref(), Some("short"));
        assert!(cap.get_documentation().is_none());

        cap.set_documentation("long body");
        assert_eq!(cap.get_documentation(), Some("long body"));
        // setter must not touch cap_description
        assert_eq!(cap.cap_description.as_deref(), Some("short"));

        cap.clear_documentation();
        assert!(cap.get_documentation().is_none());
        // clearer must not touch cap_description
        assert_eq!(cap.cap_description.as_deref(), Some("short"));
    }

    // ==========================================================================
    // MAIN-INPUT CONTRACT (stream_urn / is_main_input) — TEST7100-7104
    // ==========================================================================

    use crate::urn::media_urn::MediaUrn;

    /// TEST7100: stream_urn() returns the Stdin source's URN when it differs
    /// from the declared slot media_urn — the runtime demuxes by the PIPED
    /// media, not the slot type (e.g. a file-path slot fed pdf bytes).
    #[test]
    fn test7100_stream_urn_prefers_stdin_source_over_slot_urn() {
        let arg = CapArg::new(
            "media:enc=utf-8;file-path",
            true,
            vec![ArgSource::Stdin {
                stdin: "media:ext=pdf".to_string(),
            }],
        );
        assert_eq!(arg.stream_urn(), "media:ext=pdf");
        assert_ne!(
            arg.stream_urn(),
            arg.media_urn,
            "the stdin URN, not the slot URN, must win when they differ"
        );
    }

    /// TEST7101: stream_urn() falls back to the declared slot media_urn when
    /// the arg declares no Stdin source (producer-fed args are delivered by
    /// their declared URN).
    #[test]
    fn test7101_stream_urn_falls_back_to_declared_urn_without_stdin() {
        let arg = CapArg::new(
            "media:enc=utf-8;model-spec",
            true,
            vec![
                ArgSource::CliFlag {
                    cli_flag: "--model-spec".to_string(),
                },
                ArgSource::Position { position: 0 },
            ],
        );
        assert_eq!(arg.stream_urn(), "media:enc=utf-8;model-spec");
    }

    /// TEST7102: is_main_input() is true when the Stdin source URN is
    /// order-theoretically equivalent to the cap's `in=` spec even with the
    /// tags listed in a different string order — the comparison is tagged-URN
    /// equivalence, never a string comparison.
    #[test]
    fn test7102_is_main_input_by_tagged_urn_equivalence_not_strings() {
        let in_spec = MediaUrn::from_string("media:doc;ext=pdf").unwrap();
        let stdin_urn = "media:ext=pdf;doc";
        assert_ne!(
            stdin_urn,
            in_spec.to_string(),
            "precondition: the raw strings must differ so string comparison would fail"
        );
        let arg = CapArg::new(
            "media:enc=utf-8;file-path",
            true,
            vec![ArgSource::Stdin {
                stdin: stdin_urn.to_string(),
            }],
        );
        assert!(
            arg.is_main_input(&in_spec),
            "tag order must not matter — equivalence is order-independent"
        );
    }

    /// TEST7103: is_main_input() is false for cli_flag-only and position-only
    /// args (no Stdin source at all), and false when the arg's Stdin URN is
    /// NOT equivalent to the cap's `in=` spec.
    #[test]
    fn test7103_is_main_input_false_without_matching_stdin() {
        let in_spec = MediaUrn::from_string("media:ext=pdf").unwrap();

        let flag_only = CapArg::new(
            "media:ext=pdf",
            true,
            vec![ArgSource::CliFlag {
                cli_flag: "--input".to_string(),
            }],
        );
        assert!(
            !flag_only.is_main_input(&in_spec),
            "a cli_flag-only arg is never the main input, even with a matching slot URN"
        );

        let position_only = CapArg::new(
            "media:ext=pdf",
            true,
            vec![ArgSource::Position { position: 0 }],
        );
        assert!(
            !position_only.is_main_input(&in_spec),
            "a position-only arg is never the main input"
        );

        let wrong_stdin = CapArg::new(
            "media:ext=png",
            true,
            vec![ArgSource::Stdin {
                stdin: "media:ext=png".to_string(),
            }],
        );
        assert!(
            !wrong_stdin.is_main_input(&in_spec),
            "a Stdin source whose URN is not `in=` does not mark the main input"
        );
    }

    /// TEST8065: cardinality follows the declared main input even when a
    /// secondary stdin-capable argument appears first in declaration order.
    #[test]
    fn test8065_sequence_shape_uses_main_input_identity_not_arg_order() {
        let urn = CapUrn::from_string(r#"cap:in="media:enc=utf-8";ordered;out="media:enc=utf-8""#)
            .unwrap();
        let secondary = CapArg::new(
            "media:enc=utf-8;context",
            false,
            vec![ArgSource::Stdin {
                stdin: "media:enc=utf-8;context".to_string(),
            }],
        );
        let mut main = CapArg::new(
            "media:enc=utf-8",
            true,
            vec![ArgSource::Stdin {
                stdin: "media:enc=utf-8".to_string(),
            }],
        );
        main.is_sequence = true;
        let cap = Cap::with_args(
            urn,
            "Ordered args".to_string(),
            vec!["ordered".to_string()],
            vec![secondary, main],
        );

        assert_eq!(cap.sequence_shape(), (true, false));
        assert!(!cap.needs_foreach(true));
    }

    /// TEST8066: a declared void-input producer has no main-input argument and
    /// therefore has scalar input cardinality without inventing an arg.
    #[test]
    fn test8066_void_input_sequence_shape_is_scalar_without_arguments() {
        let cap = Cap::new(
            CapUrn::from_string(r#"cap:in="media:void";clock;out="media:time""#).unwrap(),
            "Clock".to_string(),
            vec!["clock".to_string()],
        );

        assert_eq!(cap.sequence_shape(), (false, false));
    }

    // TEST7150: a cap's OUTPUT survives a manifest round-trip, under the wire
    // key names the other implementations read.
    #[test]
    fn test7150_cap_output_survives_serialization_roundtrip() {
        let mut cap = Cap::new(
            CapUrn::from_string(r#"cap:in="media:enc=utf-8;in";out="media:enc=utf-8;tag";tag"#)
                .expect("valid urn"),
            "tag".to_string(),
            vec!["tag".to_string()],
        );
        cap.set_output(CapOutput {
            media_urn: "media:enc=utf-8;tag".to_string(),
            output_description: "One of 'positive', 'neutral', or 'negative'.".to_string(),
            is_sequence: false,
            streaming: false,
            metadata: None,
        });

        let json = serde_json::to_value(&cap).expect("cap serializes");
        let output = json
            .get("output")
            .expect("a cap that declares an output must serialize one");
        assert_eq!(
            output.get("media_urn").and_then(|v| v.as_str()),
            Some("media:enc=utf-8;tag")
        );
        assert_eq!(
            output.get("output_description").and_then(|v| v.as_str()),
            Some("One of 'positive', 'neutral', or 'negative'.")
        );

        let back: Cap = serde_json::from_value(json).expect("cap deserializes");
        let back_output = back.output.expect("output survives the round-trip");
        assert_eq!(back_output.media_urn, "media:enc=utf-8;tag");
        assert_eq!(back_output.is_sequence, false);

        // A cap with no output must not carry the key at all.
        let bare = Cap::new(
            CapUrn::from_string("cap:effect=none").expect("valid urn"),
            "Identity".to_string(),
            vec!["identity".to_string()],
        );
        let bare_json = serde_json::to_value(&bare).expect("cap serializes");
        assert!(
            bare_json.get("output").is_none(),
            "a cap without an output must omit the key"
        );
    }

    // TEST7151: `is_sequence` is serialized even when false, on both CapArg and
    // CapOutput.
    //
    // It is not a `skip_serializing_if` field. Mirrors that omitted it produced
    // a manifest for the identical cap that differed from this one's bytes,
    // which is how a cross-language manifest comparison finds drift that every
    // per-mirror test passes through.
    #[test]
    fn test7151_is_sequence_is_serialized_even_when_false() {
        let arg = CapArg::new(
            "media:enc=utf-8;in",
            true,
            vec![ArgSource::Stdin {
                stdin: "media:enc=utf-8;in".to_string(),
            }],
        );
        let arg_json = serde_json::to_value(&arg).expect("arg serializes");
        assert_eq!(
            arg_json.get("is_sequence").and_then(|v| v.as_bool()),
            Some(false),
            "CapArg must write is_sequence even when false"
        );

        let output = CapOutput {
            media_urn: "media:enc=utf-8;tag".to_string(),
            output_description: "a tag".to_string(),
            is_sequence: false,
            streaming: false,
            metadata: None,
        };
        let output_json = serde_json::to_value(&output).expect("output serializes");
        assert_eq!(
            output_json.get("is_sequence").and_then(|v| v.as_bool()),
            Some(false),
            "CapOutput must write is_sequence even when false"
        );
    }
}
