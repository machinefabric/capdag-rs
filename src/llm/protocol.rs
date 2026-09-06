//! LLM Protocol Types
//!
//! Canonical Rust types matching the capdag media def schemas for LLM operations.
//! These types are the single source of truth — both floom-engine and the LLM cartridges
//! serialize/deserialize against them.
//!
//! Media defs:
//! - media:fmt=json;llm-generation-request;record
//! - media:fmt=ndjson;llm-text-stream
//! - media:fmt=json;llm-vocab-response;record
//! - media:fmt=json;llm-model-info;record
//!
//! Caps:
//! - cap:op=llm_inference               — text generation
//! - cap:op=llm_inference_constrained   — constrained generation with LLGuidance
//! - cap:op=llm_vocab                   — vocabulary extraction
//! - cap:op=llm_model_info              — model info query

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

// =============================================================================
// Request Type (optional discriminator)
// =============================================================================

/// Type of LLM request — used when a single command handles multiple operations.
///
/// Prefer using different caps for different operations when possible.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RequestType {
    Generate,
    GetVocab,
    GetInfo,
}

impl Default for RequestType {
    fn default() -> Self {
        Self::Generate
    }
}

// =============================================================================
// media:fmt=json;llm-generation-request;record
// =============================================================================

/// LLM Generation Request — input for all LLM caps.
///
/// Matches: media:fmt=json;llm-generation-request;record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmGenerationRequest {
    pub prompt: String,
    pub model_spec: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_type: Option<RequestType>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_k: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_p: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub grammar: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub json_schema: Option<JsonValue>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub constraint: Option<ConstraintSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat_template: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_sequences: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_context_length: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub rope_freq_base: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub rope_freq_scale: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub repeat_penalty: Option<f32>,

    /// HuggingFace bearer token. Forwarded to modelcartridge for the model
    /// download triggered by this request. Required when targeting a
    /// gated repository — without it the download fails hard with a clear
    /// authentication error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hf_token: Option<String>,
}

impl LlmGenerationRequest {
    /// Generation request populated with the system defaults for sampling.
    pub fn with_defaults(prompt: impl Into<String>, model_spec: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
            model_spec: model_spec.into(),
            request_type: Some(RequestType::Generate),
            system_prompt: None,
            request_id: None,
            max_tokens: Some(512),
            temperature: Some(0.7),
            top_k: Some(40),
            top_p: Some(0.9),
            min_p: Some(0.05),
            seed: Some(42),
            grammar: None,
            json_schema: None,
            constraint: None,
            chat_template: None,
            stop_sequences: None,
            // 0 = auto-size the context to prompt + max_tokens, capped
            // at the model's trained context (the new regime — see
            // docs/cartridge-flexibility-audit.md).
            max_context_length: Some(0),
            batch_size: Some(2048),
            rope_freq_base: Some(10000.0),
            rope_freq_scale: Some(1.0),
            repeat_penalty: Some(1.1),
            hf_token: None,
        }
    }

    /// Effective request type — defaults to Generate when unset.
    pub fn effective_request_type(&self) -> RequestType {
        self.request_type.unwrap_or(RequestType::Generate)
    }

    /// Serialize to JSON. Panics on serialization failure (the type is plain data;
    /// serialization cannot fail in practice).
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("LlmGenerationRequest serialization should never fail")
    }
}

/// Constraint specification for guided generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConstraintSpec {
    JsonSchema {
        schema: JsonValue,
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<String>,
    },
    Regex {
        pattern: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<String>,
    },
    Grammar {
        grammar: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<String>,
    },
    ToolCall {
        tools: Vec<ToolDefinition>,
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<String>,
    },
}

/// Tool definition for constrained tool calling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    /// JSON Schema for tool parameters.
    pub parameters: JsonValue,
}

// =============================================================================
// media:fmt=ndjson;llm-text-stream
// =============================================================================

/// LLM Text Stream Message — NDJSON streaming output.
///
/// Matches: media:fmt=ndjson;llm-text-stream. Each line is one of these message types,
/// serialized as JSON. All variants are wire-protocol mandated by the media def.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LlmStreamMessage {
    Token {
        text: String,
    },
    Status {
        operation: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        details: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        progress: Option<f32>,
    },
    Complete {
        generated_text: String,
        tokens_generated: usize,
        prompt_tokens: usize,
        finish_reason: String,
        generation_time_ms: u64,
    },
    ToolRequest {
        partial_text: String,
        tool_name: String,
        tool_args: JsonValue,
    },
    Error {
        code: String,
        message: String,
    },
}

impl LlmStreamMessage {
    pub fn token(text: impl Into<String>) -> Self {
        Self::Token { text: text.into() }
    }

    pub fn complete(
        generated_text: impl Into<String>,
        tokens_generated: usize,
        prompt_tokens: usize,
        finish_reason: impl Into<String>,
        generation_time_ms: u64,
    ) -> Self {
        Self::Complete {
            generated_text: generated_text.into(),
            tokens_generated,
            prompt_tokens,
            finish_reason: finish_reason.into(),
            generation_time_ms,
        }
    }

    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Error {
            code: code.into(),
            message: message.into(),
        }
    }

    /// Serialize to a single NDJSON line (no trailing newline).
    pub fn to_line(&self) -> String {
        serde_json::to_string(self).expect("LlmStreamMessage serialization should never fail")
    }

    /// Parse a single NDJSON line.
    pub fn from_line(line: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(line)
    }
}

/// Common finish reasons as constants.
pub mod finish_reason {
    pub const STOP: &str = "stop";
    pub const LENGTH: &str = "length";
    pub const INTERRUPTED: &str = "interrupted";
    pub const CONSTRAINT_SATISFIED: &str = "constraint_satisfied";
}

// =============================================================================
// media:fmt=json;llm-vocab-response;record
// =============================================================================

/// LLM Vocabulary Response.
///
/// Matches: media:fmt=json;llm-vocab-response;record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmVocabResponse {
    pub vocab: Vec<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub vocab_size: Option<usize>,
}

impl LlmVocabResponse {
    pub fn new(vocab: Vec<String>) -> Self {
        let vocab_size = vocab.len();
        Self {
            vocab,
            vocab_size: Some(vocab_size),
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("LlmVocabResponse serialization should never fail")
    }
}

// =============================================================================
// media:fmt=json;llm-model-info;record
// =============================================================================

/// LLM Model Info Response.
///
/// Matches: media:fmt=json;llm-model-info;record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmModelInfo {
    pub model_spec: String,
    pub vocab_size: usize,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_length: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub embedding_dim: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_size_bytes: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_count: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub layer_count: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub supports_chat: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub supports_tools: Option<bool>,
}

impl LlmModelInfo {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("LlmModelInfo serialization should never fail")
    }
}

// =============================================================================
// Media URNs — canonical definitions (single source of truth)
// =============================================================================

pub const MEDIA_LLM_GENERATION_REQUEST: &str = "media:fmt=json;llm-generation-request;record";
pub const MEDIA_LLM_TEXT_STREAM: &str = "media:fmt=ndjson;llm-text-stream";
pub const MEDIA_LLM_VOCAB_RESPONSE: &str = "media:fmt=json;llm-vocab-response;record";
pub const MEDIA_LLM_MODEL_INFO_RESPONSE: &str = "media:fmt=json;llm-model-info;record";

// =============================================================================
// Cap URNs — canonical definitions
// =============================================================================

pub const CAP_LLM_INFERENCE_GGUF: &str = "cap:gguf;in=\"media:fmt=json;llm-generation-request;record\";llm;ml-model;llm-inference;out=\"media:fmt=ndjson;llm-text-stream\"";
pub const CAP_LLM_INFERENCE_MLX: &str = "cap:in=\"media:fmt=json;llm-generation-request;record\";llm;ml-model;mlx;llm-inference;out=\"media:fmt=ndjson;llm-text-stream\"";
pub const CAP_LLM_INFERENCE_CANDLE: &str = "cap:candle;in=\"media:fmt=json;llm-generation-request;record\";llm;ml-model;llm-inference;out=\"media:fmt=ndjson;llm-text-stream\"";
pub const CAP_LLM_INFERENCE_CONSTRAINED: &str = "cap:constrained;gguf;in=\"media:fmt=json;llm-generation-request;record\";llm;ml-model;llm-inference-constrained;out=\"media:fmt=ndjson;llm-text-stream\"";
pub const CAP_LLM_VOCAB: &str = "cap:llm-vocab;llm;ml-model;gguf;in=\"media:fmt=json;llm-generation-request;record\";out=\"media:fmt=json;llm-vocab-response;record\"";
pub const CAP_LLM_MODEL_INFO: &str = "cap:llm-model-info;llm;ml-model;gguf;in=\"media:fmt=json;llm-generation-request;record\";out=\"media:fmt=json;llm-model-info;record\"";
pub const CAP_GENERATE_EMBEDDINGS: &str = "cap:generate-embeddings;ml-model;gguf;in=\"media:enc=utf-8\";out=\"media:embedding-vector;enc=utf-8;record\"";
pub const CAP_EMBEDDINGS_DIMENSIONS: &str = "cap:gguf;in=\"media:embeddings;enc=utf-8;gguf;model-spec;tokenizer-embedded-gguf\";ml-model;embeddings-dimensions;out=\"media:integer;model-dim;numeric\"";
pub const CAP_DESCRIBE_IMAGE: &str = "cap:gguf;in=\"media:ext=png;image\";ml-model;describe-image;out=\"media:enc=utf-8;ext=txt;image-description;plain-text\";vision";

// =============================================================================
// Model spec → backend classification
// =============================================================================

pub const BACKEND_GGUF: &str = "gguf";
pub const BACKEND_MLX: &str = "mlx";
pub const BACKEND_CANDLE: &str = "candle";

/// Classify a model spec string into its inference backend.
///
/// Model specs from MLX repos (`mlx-community/*` or `;mlx` tag) route to MLX.
/// Specs with GGUF indicators route to GGUF. Everything else (safetensors)
/// routes to Candle. This is the single source of truth for spec → backend,
/// used by both the model service (to populate `Model.backend`) and the
/// cartridge client (to select the correct cap URN for dispatch).
pub fn backend_for_model_spec(model_spec: &str) -> &'static str {
    let lower = model_spec.to_lowercase();
    if lower.contains("mlx-community") || lower.contains(";mlx") {
        BACKEND_MLX
    } else if lower.contains(".gguf") || lower.contains("gguf") {
        BACKEND_GGUF
    } else {
        BACKEND_CANDLE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Round-trip: a generation request serializes and deserializes to equivalent content.
    #[test]
    fn test0001_generation_request_round_trip() {
        let req = LlmGenerationRequest::with_defaults("Hello", "model/test");
        let json = req.to_json();
        let parsed: LlmGenerationRequest =
            serde_json::from_str(&json).expect("request must round-trip");

        assert_eq!(parsed.prompt, "Hello");
        assert_eq!(parsed.model_spec, "model/test");
        assert_eq!(parsed.max_tokens, Some(512));
    }

    // Round-trip: each stream message variant serializes and deserializes to itself.
    #[test]
    fn test0002_stream_message_token_round_trip() {
        let msg = LlmStreamMessage::token("Hello");
        let line = msg.to_line();
        assert!(line.contains("\"type\":\"token\""));

        let parsed = LlmStreamMessage::from_line(&line).expect("token must parse");
        match parsed {
            LlmStreamMessage::Token { text } => assert_eq!(text, "Hello"),
            other => panic!("expected Token, got {:?}", other),
        }
    }

    // TEST0003: Stream message complete round trip
    #[test]
    fn test0003_stream_message_complete_round_trip() {
        let msg = LlmStreamMessage::complete("Generated text", 10, 5, finish_reason::STOP, 100);
        let line = msg.to_line();
        let parsed = LlmStreamMessage::from_line(&line).expect("complete must parse");

        match parsed {
            LlmStreamMessage::Complete {
                tokens_generated,
                finish_reason,
                ..
            } => {
                assert_eq!(tokens_generated, 10);
                assert_eq!(finish_reason, "stop");
            }
            other => panic!("expected Complete, got {:?}", other),
        }
    }

    // TEST0004: Stream message error round trip
    #[test]
    fn test0004_stream_message_error_round_trip() {
        let msg = LlmStreamMessage::error("MODEL_NOT_FOUND", "Model not available");
        let line = msg.to_line();
        let parsed = LlmStreamMessage::from_line(&line).expect("error must parse");

        match parsed {
            LlmStreamMessage::Error { code, message } => {
                assert_eq!(code, "MODEL_NOT_FOUND");
                assert_eq!(message, "Model not available");
            }
            other => panic!("expected Error, got {:?}", other),
        }
    }

    // TEST0005: Vocab response round trip
    #[test]
    fn test0005_vocab_response_round_trip() {
        let resp = LlmVocabResponse::new(vec!["a".into(), "b".into(), "c".into()]);
        let json = resp.to_json();
        let parsed: LlmVocabResponse =
            serde_json::from_str(&json).expect("vocab response must round-trip");

        assert_eq!(parsed.vocab.len(), 3);
        assert_eq!(parsed.vocab_size, Some(3));
    }

    // TEST0006: Model info round trip
    #[test]
    fn test0006_model_info_round_trip() {
        let info = LlmModelInfo {
            model_spec: "test-model".into(),
            vocab_size: 32000,
            context_length: Some(4096),
            embedding_dim: None,
            file_size_bytes: None,
            head_count: None,
            layer_count: None,
            supports_chat: None,
            supports_tools: None,
        };
        let json = info.to_json();
        let parsed: LlmModelInfo =
            serde_json::from_str(&json).expect("model info must round-trip");

        assert_eq!(parsed.model_spec, "test-model");
        assert_eq!(parsed.vocab_size, 32000);
        assert_eq!(parsed.context_length, Some(4096));
    }

    // TEST0007: Constraint spec tags
    #[test]
    fn test0007_constraint_spec_tags() {
        let json_schema = ConstraintSpec::JsonSchema {
            schema: serde_json::json!({"type": "object"}),
            description: None,
        };
        let json = serde_json::to_string(&json_schema).unwrap();
        assert!(json.contains("\"type\":\"json_schema\""));

        let regex = ConstraintSpec::Regex {
            pattern: r"\d+".into(),
            description: None,
        };
        let json = serde_json::to_string(&regex).unwrap();
        assert!(json.contains("\"type\":\"regex\""));
    }

    // TEST0008: Backend for model spec gguf
    #[test]
    fn test0008_backend_for_model_spec_gguf() {
        assert_eq!(
            backend_for_model_spec("hf:bartowski/Llama-3.2-3B-Instruct-GGUF"),
            BACKEND_GGUF
        );
        assert_eq!(
            backend_for_model_spec("hf:TheBloke/Mistral-7B-v0.1-GGUF?include=*Q4_K_M*.gguf"),
            BACKEND_GGUF
        );
        assert_eq!(backend_for_model_spec("local:/path/to/model.gguf"), BACKEND_GGUF);
    }

    // TEST0009: Backend for model spec mlx
    #[test]
    fn test0009_backend_for_model_spec_mlx() {
        assert_eq!(
            backend_for_model_spec("hf:mlx-community/Mistral-7B-Instruct-v0.3-4bit"),
            BACKEND_MLX
        );
        assert_eq!(
            backend_for_model_spec("hf:mlx-community/Meta-Llama-3.1-8B-Instruct-4bit"),
            BACKEND_MLX
        );
        assert_eq!(backend_for_model_spec("hf:some-model;mlx"), BACKEND_MLX);
    }

    // TEST0010: Backend for model spec candle
    #[test]
    fn test0010_backend_for_model_spec_candle() {
        assert_eq!(
            backend_for_model_spec("hf:meta-llama/Llama-3.1-8B-Instruct"),
            BACKEND_CANDLE
        );
        assert_eq!(backend_for_model_spec("hf:microsoft/phi-2"), BACKEND_CANDLE);
        assert_eq!(backend_for_model_spec("hf:google/gemma-2b"), BACKEND_CANDLE);
    }
}
