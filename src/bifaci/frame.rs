//! CBOR Frame Types for Cartridge Communication
//!
//! This module defines the binary CBOR frame format that replaces JSON messages.
//! Frames use integer keys for compact encoding and support native binary payloads.
//!
//! ## Frame Format
//!
//! Each frame is a CBOR map with integer keys:
//! ```text
//! {
//!   0: version (u8, always 2)
//!   1: frame_type (u8)
//!   2: id (bytes[16] or uint)
//!   3: seq (u64)
//!   4: content_type (tstr, optional)
//!   5: meta (map, optional)
//!   6: payload (bstr, optional)
//!   7: len (u64, optional - total payload length for chunked)
//!   8: offset (u64, optional - byte offset in chunked stream)
//!   9: eof (bool, optional - true on final chunk)
//!   10: cap (tstr, optional - cap URN for requests)
//!   14: chunk_index (u64, optional - chunk sequence index within stream, starts at 0)
//!   15: chunk_count (u64, optional - total chunks in STREAM_END, by source's count)
//!   16: checksum (u64, optional - FNV-1a hash of payload for CHUNK frames)
//!   17: is_sequence (bool, optional - true if producer used emit_list_item, false if write)
//! }
//! ```
//!
//! ## Frame Types
//!
//! - HELLO (0): Handshake to negotiate limits
//! - REQ (1): Request to invoke a cap
//! - RES (2): Single complete response
//! - CHUNK (3): Streaming data chunk
//! - END (4): Stream complete marker
//! - LOG (5): Log/progress message
//! - ERR (6): Error message

use crate::CapUrn;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Protocol version 4 requires explicit handler capacity and strict diagnostic
/// attribution in addition to credit-based flow control and terminal metadata.
/// Every other version is rejected during handshake.
pub const PROTOCOL_VERSION: u8 = 4;

/// Default maximum frame size (3.5 MB) - safe margin below 3.75MB limit
/// Larger payloads automatically use CHUNK frames
pub const DEFAULT_MAX_FRAME: usize = 3_670_016;

/// Default maximum chunk size (256 KB)
pub const DEFAULT_MAX_CHUNK: usize = 262_144;

/// Default maximum reorder buffer size (per-flow frame count)
pub const DEFAULT_MAX_REORDER_BUFFER: usize = 64;

/// Default initial credit window per stream, in CHUNK frames.
/// A sender may emit this many CHUNKs per stream before it must wait for a
/// CREDIT grant. 32 chunks ≈ 8 MiB at the default max_chunk (256 KiB).
pub const DEFAULT_INITIAL_CREDIT: u64 = 32;

/// Frame type discriminator
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum FrameType {
    /// Handshake frame for negotiating limits
    Hello = 0,
    /// Request to invoke a cap
    Req = 1,
    // Res = 2 REMOVED - old single-response protocol no longer supported
    /// Streaming data chunk
    Chunk = 3,
    /// Stream complete marker
    End = 4,
    /// Log/progress message
    Log = 5,
    /// Error message
    Err = 6,
    /// Health monitoring ping/pong - either side can send, receiver must respond with same ID
    Heartbeat = 7,
    /// Announce new stream for a request (multiplexed streaming)
    StreamStart = 8,
    /// End a specific stream (multiplexed streaming)
    StreamEnd = 9,
    /// Relay capability advertisement (slave → master). Carries aggregate manifest + limits.
    RelayNotify = 10,
    /// Relay host system resources + cap demands (master → slave). Carries opaque resource payload.
    RelayState = 11,
    /// Cancel a specific in-flight request by RID. Carries optional force_kill flag.
    Cancel = 12,
    /// Grant per-stream flow-control credit (in CHUNK units) to the sender of a
    /// stream. Non-flow: bypasses seq assignment and reorder buffers, and is
    /// forwarded end-to-end by intermediaries (never originated or absorbed).
    Credit = 13,
}

impl FrameType {
    /// All variants, for counter arrays and snapshot serialization.
    pub const ALL: [FrameType; 13] = [
        FrameType::Hello,
        FrameType::Req,
        FrameType::Chunk,
        FrameType::End,
        FrameType::Log,
        FrameType::Err,
        FrameType::Heartbeat,
        FrameType::StreamStart,
        FrameType::StreamEnd,
        FrameType::RelayNotify,
        FrameType::RelayState,
        FrameType::Cancel,
        FrameType::Credit,
    ];

    /// Stable snake_case name (the snapshot contract for mirrors and traces).
    pub fn as_str(&self) -> &'static str {
        match self {
            FrameType::Hello => "hello",
            FrameType::Req => "req",
            FrameType::Chunk => "chunk",
            FrameType::End => "end",
            FrameType::Log => "log",
            FrameType::Err => "err",
            FrameType::Heartbeat => "heartbeat",
            FrameType::StreamStart => "stream_start",
            FrameType::StreamEnd => "stream_end",
            FrameType::RelayNotify => "relay_notify",
            FrameType::RelayState => "relay_state",
            FrameType::Cancel => "cancel",
            FrameType::Credit => "credit",
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(FrameType::Hello),
            1 => Some(FrameType::Req),
            // 2 = Res REMOVED - old protocol no longer supported
            3 => Some(FrameType::Chunk),
            4 => Some(FrameType::End),
            5 => Some(FrameType::Log),
            6 => Some(FrameType::Err),
            7 => Some(FrameType::Heartbeat),
            8 => Some(FrameType::StreamStart),
            9 => Some(FrameType::StreamEnd),
            10 => Some(FrameType::RelayNotify),
            11 => Some(FrameType::RelayState),
            12 => Some(FrameType::Cancel),
            13 => Some(FrameType::Credit),
            _ => None,
        }
    }
}

/// Message ID - either a 16-byte UUID or a simple integer
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MessageId {
    /// 16-byte UUID
    Uuid([u8; 16]),
    /// Simple integer ID
    Uint(u64),
}

impl MessageId {
    /// Create a new random UUID message ID
    pub fn new_uuid() -> Self {
        let uuid = uuid::Uuid::new_v4();
        MessageId::Uuid(*uuid.as_bytes())
    }

    /// Create from a UUID string
    pub fn from_uuid_str(s: &str) -> Option<Self> {
        uuid::Uuid::parse_str(s)
            .ok()
            .map(|u| MessageId::Uuid(*u.as_bytes()))
    }

    /// Convert to UUID string if this is a UUID
    pub fn to_uuid_string(&self) -> Option<String> {
        match self {
            MessageId::Uuid(bytes) => uuid::Uuid::from_bytes(*bytes).to_string().into(),
            MessageId::Uint(_) => None,
        }
    }

    /// Get as bytes for comparison
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            MessageId::Uuid(bytes) => bytes.to_vec(),
            MessageId::Uint(n) => n.to_be_bytes().to_vec(),
        }
    }
}

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageId::Uuid(bytes) => write!(f, "{}", uuid::Uuid::from_bytes(*bytes)),
            MessageId::Uint(n) => write!(f, "{}", n),
        }
    }
}

impl Default for MessageId {
    fn default() -> Self {
        MessageId::new_uuid()
    }
}

/// Negotiated protocol limits
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Maximum frame size in bytes
    pub max_frame: usize,
    /// Maximum chunk payload size in bytes
    pub max_chunk: usize,
    /// Maximum reorder buffer size per flow (frame count)
    pub max_reorder_buffer: usize,
    /// Initial per-stream credit window in CHUNK frames
    pub initial_credit: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame: DEFAULT_MAX_FRAME,
            max_chunk: DEFAULT_MAX_CHUNK,
            max_reorder_buffer: DEFAULT_MAX_REORDER_BUFFER,
            initial_credit: DEFAULT_INITIAL_CREDIT,
        }
    }
}

/// A CBOR protocol frame
#[derive(Debug, Clone)]
pub struct Frame {
    /// Protocol version (always 2)
    pub version: u8,
    /// Frame type
    pub frame_type: FrameType,
    /// Message ID for correlation (request ID)
    pub id: MessageId,
    /// Routing ID assigned by RelaySwitch for routing decisions
    /// Separates logical request ID (id) from routing concerns
    /// RelaySwitch assigns this when REQ arrives, all response frames carry it
    pub routing_id: Option<MessageId>,
    /// Stream ID for multiplexed streams (used in STREAM_START, CHUNK, STREAM_END)
    pub stream_id: Option<String>,
    /// Media URN for stream type identification (used in STREAM_START)
    pub media_urn: Option<String>,
    /// Sequence number within a flow (per request ID).
    /// Assigned centrally by SeqAssigner at the output stage (writer thread).
    /// Monotonically increasing for all frame types within the same RID.
    pub seq: u64,
    /// Content type of payload (MIME-like)
    pub content_type: Option<String>,
    /// Metadata map
    pub meta: Option<BTreeMap<String, ciborium::Value>>,
    /// Binary payload
    pub payload: Option<Vec<u8>>,
    /// Total length for chunked transfers (first chunk only)
    pub len: Option<u64>,
    /// Byte offset in chunked stream
    pub offset: Option<u64>,
    /// End of stream marker
    pub eof: Option<bool>,
    /// Cap URN (for requests)
    pub cap: Option<String>,
    /// Chunk sequence index within stream (CHUNK frames only, starts at 0)
    pub chunk_index: Option<u64>,
    /// Total chunk count (STREAM_END frames only, by source's reckoning)
    pub chunk_count: Option<u64>,
    /// FNV-1a checksum of payload (CHUNK frames only)
    pub checksum: Option<u64>,
    /// Whether the producer used emit_list_item (true) or write (false).
    /// Present on STREAM_START frames only. None means unknown (empty stream).
    pub is_sequence: Option<bool>,
    /// Whether Cancel should force-kill the cartridge process (true) or cooperatively cancel (false).
    /// Present on Cancel frames only.
    pub force_kill: Option<bool>,
    /// Flow-control credit grant in CHUNK units. Present on Credit frames only.
    pub credit: Option<u64>,
    /// Whether the stream makes no length promise (no chunk_count on STREAM_END,
    /// receivers must consume incrementally). Present on STREAM_START frames only.
    pub unbounded: Option<bool>,
}

impl Frame {
    /// Create a new frame with required fields
    pub fn new(frame_type: FrameType, id: MessageId) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            frame_type,
            id,
            routing_id: None,
            stream_id: None,
            media_urn: None,
            seq: 0,
            content_type: None,
            meta: None,
            payload: None,
            len: None,
            offset: None,
            eof: None,
            cap: None,
            chunk_index: None,
            chunk_count: None,
            checksum: None,
            is_sequence: None,
            force_kill: None,
            credit: None,
            unbounded: None,
        }
    }

    /// Create a HELLO frame for handshake (host side - no manifest)
    pub fn hello(limits: &Limits) -> Self {
        let mut meta = BTreeMap::new();
        meta.insert(
            "max_frame".to_string(),
            ciborium::Value::Integer((limits.max_frame as i64).into()),
        );
        meta.insert(
            "max_chunk".to_string(),
            ciborium::Value::Integer((limits.max_chunk as i64).into()),
        );
        meta.insert(
            "max_reorder_buffer".to_string(),
            ciborium::Value::Integer((limits.max_reorder_buffer as i64).into()),
        );
        meta.insert(
            "initial_credit".to_string(),
            ciborium::Value::Integer((limits.initial_credit as i64).into()),
        );
        meta.insert(
            "version".to_string(),
            ciborium::Value::Integer((PROTOCOL_VERSION as i64).into()),
        );

        let mut frame = Self::new(FrameType::Hello, MessageId::Uint(0));
        frame.meta = Some(meta);
        frame
    }

    /// Create a HELLO frame for handshake with manifest (cartridge side).
    /// The manifest is JSON-encoded cartridge metadata including name, version, and caps.
    /// This is the ONLY way for cartridges to communicate their capabilities.
    pub fn hello_with_manifest(
        limits: &Limits,
        manifest: &[u8],
        pool_states: &crate::bifaci::pools::PoolStates,
    ) -> Self {
        let mut meta = BTreeMap::new();
        meta.insert(
            "max_frame".to_string(),
            ciborium::Value::Integer((limits.max_frame as i64).into()),
        );
        meta.insert(
            "max_chunk".to_string(),
            ciborium::Value::Integer((limits.max_chunk as i64).into()),
        );
        meta.insert(
            "max_reorder_buffer".to_string(),
            ciborium::Value::Integer((limits.max_reorder_buffer as i64).into()),
        );
        meta.insert(
            "initial_credit".to_string(),
            ciborium::Value::Integer((limits.initial_credit as i64).into()),
        );
        meta.insert(
            "version".to_string(),
            ciborium::Value::Integer((PROTOCOL_VERSION as i64).into()),
        );
        meta.insert(
            "manifest".to_string(),
            ciborium::Value::Bytes(manifest.to_vec()),
        );
        // The full concurrency-pool state map (JSON bytes — the manifest's
        // own transport). Mandatory: pools are the protocol's one capacity
        // concept, and a HELLO without them is a handshake error at the host.
        meta.insert(
            crate::bifaci::pools::META_POOLS.to_string(),
            ciborium::Value::Bytes(crate::bifaci::pools::encode_pool_states(pool_states)),
        );

        let mut frame = Self::new(FrameType::Hello, MessageId::Uint(0));
        frame.meta = Some(meta);
        frame
    }

    /// Create a REQ frame for invoking a cap
    ///
    /// # Panics
    /// Panics if cap_urn is not a valid cap URN.
    pub fn req(id: MessageId, cap_urn: &str, payload: Vec<u8>, content_type: &str) -> Self {
        // HARD VALIDATION: cap URN must be valid
        CapUrn::from_string(cap_urn).unwrap_or_else(|_| panic!("Invalid cap URN: '{}'", cap_urn));

        let mut frame = Self::new(FrameType::Req, id);
        frame.cap = Some(cap_urn.to_string());
        frame.payload = Some(payload);
        frame.content_type = Some(content_type.to_string());
        frame
    }

    // Frame::res() REMOVED - old single-response protocol no longer supported
    // Use stream multiplexing: STREAM_START + CHUNK + STREAM_END + END

    /// Create a CHUNK frame for multiplexed streaming.
    /// Each chunk belongs to a specific stream within a request.
    ///
    /// # Arguments
    /// * `req_id` - The request ID this chunk belongs to
    /// * `stream_id` - The stream ID this chunk belongs to
    /// * `seq` - Sequence number within the stream
    /// * `payload` - Chunk data
    pub fn chunk(
        req_id: MessageId,
        stream_id: String,
        seq: u64,
        payload: Vec<u8>,
        chunk_index: u64,
        checksum: u64,
    ) -> Self {
        let mut frame = Self::new(FrameType::Chunk, req_id);
        frame.stream_id = Some(stream_id);
        frame.seq = seq;
        frame.payload = Some(payload);
        frame.chunk_index = Some(chunk_index);
        frame.checksum = Some(checksum);
        frame
    }

    /// Create a CHUNK frame with offset info (for large binary transfers).
    /// Used for multiplexed streaming with offset tracking.
    pub fn chunk_with_offset(
        req_id: MessageId,
        stream_id: String,
        seq: u64,
        payload: Vec<u8>,
        offset: u64,
        total_len: Option<u64>,
        is_last: bool,
        chunk_index: u64,
        checksum: u64,
    ) -> Self {
        let mut frame = Self::new(FrameType::Chunk, req_id);
        frame.stream_id = Some(stream_id);
        frame.seq = seq;
        frame.payload = Some(payload);
        frame.offset = Some(offset);
        frame.chunk_index = Some(chunk_index);
        frame.checksum = Some(checksum);
        if chunk_index == 0 {
            frame.len = total_len;
        }
        if is_last {
            frame.eof = Some(true);
        }
        frame
    }

    /// Create an END frame to mark stream completion.
    /// Does NOT set exit_code — absence of exit_code in meta means failure.
    /// Use `end_ok` for successful completion (exit_code=0).
    pub fn end(id: MessageId, final_payload: Option<Vec<u8>>) -> Self {
        let mut frame = Self::new(FrameType::End, id);
        frame.payload = final_payload;
        frame.eof = Some(true);
        frame
    }

    /// Create an END frame with exit_code=0 (success).
    /// Only exit_code=0 means success. Absence of exit_code or any non-zero value means failure.
    pub fn end_ok(id: MessageId, final_payload: Option<Vec<u8>>) -> Self {
        let mut meta = BTreeMap::new();
        meta.insert("exit_code".to_string(), ciborium::Value::Integer(0.into()));
        let mut frame = Self::new(FrameType::End, id);
        frame.payload = final_payload;
        frame.eof = Some(true);
        frame.meta = Some(meta);
        frame
    }

    /// Create an END frame with exit_code=0 (success) carrying terminal metadata.
    /// `progress` is the authoritative final progress value delivered with the
    /// terminal frame itself (so it can never race it); `message` is an optional
    /// final status message. A successful END without an explicit progress reads
    /// as 1.0 via `final_progress()`.
    pub fn end_ok_with(
        id: MessageId,
        final_payload: Option<Vec<u8>>,
        progress: Option<f64>,
        message: Option<&str>,
    ) -> Self {
        let mut frame = Self::end_ok(id, final_payload);
        let meta = frame.meta.get_or_insert_with(BTreeMap::new);
        if let Some(p) = progress {
            meta.insert("progress".to_string(), ciborium::Value::Float(p));
        }
        if let Some(m) = message {
            meta.insert("message".to_string(), ciborium::Value::Text(m.to_string()));
        }
        frame
    }

    /// Read the final progress from an END frame's terminal metadata.
    /// Returns the explicit `progress` meta value when present; a successful END
    /// (exit_code=0) without an explicit value reads as 1.0. Non-END frames and
    /// unsuccessful ENDs without a value return None.
    pub fn final_progress(&self) -> Option<f64> {
        if self.frame_type != FrameType::End {
            return None;
        }
        let explicit = self.meta.as_ref().and_then(|m| {
            m.get("progress").and_then(|v| match v {
                ciborium::Value::Float(f) => Some(*f),
                ciborium::Value::Integer(i) => {
                    let n: i128 = (*i).into();
                    Some(n as f64)
                }
                _ => None,
            })
        });
        match explicit {
            Some(p) => Some(p),
            None if self.exit_code() == Some(0) => Some(1.0),
            None => None,
        }
    }

    /// Read the final status message from an END frame's terminal metadata.
    pub fn final_message(&self) -> Option<&str> {
        if self.frame_type != FrameType::End {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("message").and_then(|v| {
                if let ciborium::Value::Text(s) = v {
                    Some(s.as_str())
                } else {
                    None
                }
            })
        })
    }

    /// Read exit_code from an END frame's meta. Returns None if absent.
    pub fn exit_code(&self) -> Option<i64> {
        self.meta.as_ref()?.get("exit_code").and_then(|v| {
            if let ciborium::Value::Integer(i) = v {
                let n: i128 = (*i).into();
                Some(n as i64)
            } else {
                None
            }
        })
    }

    /// Create an attributed, non-progress LOG frame.
    pub fn log(
        id: MessageId,
        level: &str,
        attribution_class: crate::failure::AttributionClass,
        message: &str,
        arg_urn: Option<&str>,
    ) -> Self {
        assert!(!level.trim().is_empty(), "LOG level must be nonempty");
        assert_ne!(level, "progress", "progress logs must use Frame::progress");
        assert!(!message.trim().is_empty(), "LOG message must be nonempty");
        assert!(
            arg_urn.map_or(true, |urn| !urn.is_empty()),
            "LOG arg_urn must be nonempty when present"
        );
        let mut meta = BTreeMap::new();
        meta.insert(
            "level".to_string(),
            ciborium::Value::Text(level.to_string()),
        );
        meta.insert(
            "message".to_string(),
            ciborium::Value::Text(message.to_string()),
        );
        meta.insert(
            "attribution_class".to_string(),
            ciborium::Value::Text(attribution_class.as_str().to_string()),
        );
        if let Some(urn) = arg_urn {
            meta.insert(
                "arg_urn".to_string(),
                ciborium::Value::Text(urn.to_string()),
            );
        }

        let mut frame = Self::new(FrameType::Log, id);
        frame.meta = Some(meta);
        frame
    }

    /// Create a LOG frame with progress (0.0–1.0) and a human-readable status message.
    /// Uses level="progress" with an additional "progress" key in metadata.
    pub fn progress(id: MessageId, progress: f32, message: &str) -> Self {
        let mut meta = BTreeMap::new();
        meta.insert(
            "level".to_string(),
            ciborium::Value::Text("progress".to_string()),
        );
        meta.insert(
            "message".to_string(),
            ciborium::Value::Text(message.to_string()),
        );
        meta.insert(
            "progress".to_string(),
            ciborium::Value::Float(progress as f64),
        );

        let mut frame = Self::new(FrameType::Log, id);
        frame.meta = Some(meta);
        frame
    }

    /// Create an ERR frame carrying the full failure identity: the emitter's
    /// machine-readable `code` (e.g. `CONTEXT_OVERFLOW`), the failure CLASS
    /// (whose problem it is — declared at the error's definition site, see
    /// `capdag::AttributionClass`), the human message, and — when the emitter
    /// attributed the failure to ONE argument — the media URN of that
    /// argument. ERR meta contract (`../docs/12.2-frame-protocol.md`): `code` + `attribution_class` +
    /// `message`, all text, plus optional `arg_urn`; the key is ABSENT when
    /// unattributed, never an empty string.
    pub fn err(
        id: MessageId,
        code: &str,
        class: crate::failure::AttributionClass,
        message: &str,
        arg_urn: Option<&str>,
    ) -> Self {
        assert!(!code.trim().is_empty(), "ERR code must be nonempty");
        assert!(!message.trim().is_empty(), "ERR message must be nonempty");
        assert!(
            arg_urn.map_or(true, |urn| !urn.is_empty()),
            "ERR arg_urn must be nonempty when present"
        );
        let mut meta = BTreeMap::new();
        meta.insert("code".to_string(), ciborium::Value::Text(code.to_string()));
        meta.insert(
            "attribution_class".to_string(),
            ciborium::Value::Text(class.as_str().to_string()),
        );
        meta.insert(
            "message".to_string(),
            ciborium::Value::Text(message.to_string()),
        );
        if let Some(urn) = arg_urn {
            meta.insert(
                "arg_urn".to_string(),
                ciborium::Value::Text(urn.to_string()),
            );
        }

        let mut frame = Self::new(FrameType::Err, id);
        frame.meta = Some(meta);
        frame
    }

    /// Create a HEARTBEAT frame for health monitoring.
    /// Either side can send; receiver must respond with HEARTBEAT using the same ID.
    pub fn heartbeat(id: MessageId) -> Self {
        Self::new(FrameType::Heartbeat, id)
    }

    /// Create a STREAM_START frame to announce a new stream within a request.
    /// Used for multiplexed streaming - multiple streams can exist per request.
    ///
    /// # Arguments
    /// * `req_id` - The request ID this stream belongs to
    /// * `stream_id` - Unique ID for this stream (UUID generated by sender)
    /// * `media_urn` - Media URN identifying the stream's data type
    pub fn stream_start(
        req_id: MessageId,
        stream_id: String,
        media_urn: String,
        is_sequence: Option<bool>,
    ) -> Self {
        let mut frame = Self::new(FrameType::StreamStart, req_id);
        frame.stream_id = Some(stream_id);
        frame.media_urn = Some(media_urn);
        frame.is_sequence = is_sequence;
        frame
    }

    /// Create a STREAM_START frame for an UNBOUNDED stream — one that makes no
    /// length promise. Its STREAM_END may omit chunk_count, and receivers must
    /// consume it incrementally (never buffer to completion).
    pub fn stream_start_unbounded(
        req_id: MessageId,
        stream_id: String,
        media_urn: String,
        is_sequence: Option<bool>,
    ) -> Self {
        let mut frame = Self::stream_start(req_id, stream_id, media_urn, is_sequence);
        frame.unbounded = Some(true);
        frame
    }

    /// Whether this STREAM_START announces an unbounded stream.
    /// Absent flag means bounded.
    pub fn is_unbounded(&self) -> bool {
        self.unbounded.unwrap_or(false)
    }

    /// Create a STREAM_END frame to mark completion of a specific stream.
    /// After this, any CHUNK for this stream_id is a fatal protocol error.
    ///
    /// # Arguments
    /// * `req_id` - The request ID this stream belongs to
    /// * `stream_id` - The stream being ended
    /// * `chunk_count` - Total number of chunks sent in this stream (by source's reckoning)
    pub fn stream_end(req_id: MessageId, stream_id: String, chunk_count: u64) -> Self {
        let mut frame = Self::new(FrameType::StreamEnd, req_id);
        frame.stream_id = Some(stream_id);
        frame.chunk_count = Some(chunk_count);
        frame
    }

    /// Create a STREAM_END frame for an unbounded stream — no chunk_count promise.
    /// Valid only for streams announced with `stream_start_unbounded`.
    pub fn stream_end_unbounded(req_id: MessageId, stream_id: String) -> Self {
        let mut frame = Self::new(FrameType::StreamEnd, req_id);
        frame.stream_id = Some(stream_id);
        frame
    }

    /// Create a RelayNotify frame for capability advertisement (slave → master).
    /// Carries the aggregate manifest of all cartridge capabilities and negotiated limits.
    ///
    /// # Arguments
    /// * `manifest` - Aggregate manifest bytes (JSON-encoded list of all cartridge caps)
    /// * `limits` - Protocol limits for the relay connection
    pub fn relay_notify(manifest: &[u8], limits: &Limits) -> Self {
        let mut meta = BTreeMap::new();
        meta.insert(
            "manifest".to_string(),
            ciborium::Value::Bytes(manifest.to_vec()),
        );
        meta.insert(
            "max_frame".to_string(),
            ciborium::Value::Integer((limits.max_frame as i64).into()),
        );
        meta.insert(
            "max_chunk".to_string(),
            ciborium::Value::Integer((limits.max_chunk as i64).into()),
        );
        meta.insert(
            "max_reorder_buffer".to_string(),
            ciborium::Value::Integer((limits.max_reorder_buffer as i64).into()),
        );
        meta.insert(
            "initial_credit".to_string(),
            ciborium::Value::Integer((limits.initial_credit as i64).into()),
        );

        let mut frame = Self::new(FrameType::RelayNotify, MessageId::Uint(0));
        frame.meta = Some(meta);
        frame
    }

    /// Create a RelayState frame for host system resources + cap demands (master → slave).
    /// Carries an opaque resource payload whose format is defined by the host.
    ///
    /// # Arguments
    /// * `resources` - Opaque resource payload (CBOR or JSON encoded by the host)
    pub fn relay_state(resources: &[u8]) -> Self {
        let mut frame = Self::new(FrameType::RelayState, MessageId::Uint(0));
        frame.payload = Some(resources.to_vec());
        frame
    }

    /// Create a CANCEL frame targeting a specific request by RID.
    ///
    /// # Arguments
    /// * `target_rid` - The request ID to cancel
    /// * `force_kill` - If true, force-kill the cartridge process. If false, cooperative cancel.
    pub fn cancel(target_rid: MessageId, force_kill: bool) -> Self {
        let mut frame = Self::new(FrameType::Cancel, target_rid);
        frame.force_kill = Some(force_kill);
        frame
    }

    /// Create a CREDIT frame granting per-stream flow-control credit to the
    /// sender of a stream.
    ///
    /// # Arguments
    /// * `target_rid` - The request whose stream is being credited
    /// * `stream_id` - The stream being credited (None credits the request's
    ///   sole/default stream)
    /// * `credits` - Number of additional CHUNK frames the sender may emit
    /// * `direction` - Which side's stream is being credited. Hosts route
    ///   grants by this: a `Request` grant travels toward the requester (the
    ///   sender of argument streams), a `Response` grant toward the handler
    ///   (the sender of output streams). Required — the (xid, rid) key alone
    ///   is ambiguous for self-loop peer calls.
    pub fn credit(
        target_rid: MessageId,
        stream_id: Option<String>,
        credits: u64,
        direction: CreditDirection,
    ) -> Self {
        let mut frame = Self::new(FrameType::Credit, target_rid);
        frame.stream_id = stream_id;
        frame.credit = Some(credits);
        let mut meta = BTreeMap::new();
        meta.insert(
            "credit_dir".to_string(),
            ciborium::Value::Text(direction.as_str().to_string()),
        );
        frame.meta = Some(meta);
        frame
    }

    /// Read the credit grant from a CREDIT frame. None for other frame types.
    pub fn credit_count(&self) -> Option<u64> {
        if self.frame_type != FrameType::Credit {
            return None;
        }
        self.credit
    }

    /// Read the direction of a CREDIT frame's grant. None for other frame
    /// types or a Credit frame without the mandatory direction (a protocol
    /// violation the receiving router treats as unroutable).
    pub fn credit_direction(&self) -> Option<CreditDirection> {
        if self.frame_type != FrameType::Credit {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("credit_dir").and_then(|v| {
                if let ciborium::Value::Text(s) = v {
                    CreditDirection::from_str_name(s)
                } else {
                    None
                }
            })
        })
    }

    /// Extract manifest from RelayNotify metadata.
    /// Returns None if not a RelayNotify frame or no manifest present.
    pub fn relay_notify_manifest(&self) -> Option<&[u8]> {
        if self.frame_type != FrameType::RelayNotify {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("manifest").and_then(|v| {
                if let ciborium::Value::Bytes(bytes) = v {
                    Some(bytes.as_slice())
                } else {
                    None
                }
            })
        })
    }

    /// Extract limits from RelayNotify metadata.
    /// Returns None if not a RelayNotify frame or limits are missing.
    pub fn relay_notify_limits(&self) -> Option<Limits> {
        if self.frame_type != FrameType::RelayNotify {
            return None;
        }
        let meta = self.meta.as_ref()?;
        let max_frame = meta.get("max_frame").and_then(|v| {
            if let ciborium::Value::Integer(i) = v {
                let n: i128 = (*i).into();
                if n > 0 && n <= usize::MAX as i128 {
                    Some(n as usize)
                } else {
                    None
                }
            } else {
                None
            }
        })?;
        let max_chunk = meta.get("max_chunk").and_then(|v| {
            if let ciborium::Value::Integer(i) = v {
                let n: i128 = (*i).into();
                if n > 0 && n <= usize::MAX as i128 {
                    Some(n as usize)
                } else {
                    None
                }
            } else {
                None
            }
        })?;
        let max_reorder_buffer = meta.get("max_reorder_buffer").and_then(|v| {
            if let ciborium::Value::Integer(i) = v {
                let n: i128 = (*i).into();
                if n > 0 && n <= usize::MAX as i128 {
                    Some(n as usize)
                } else {
                    None
                }
            } else {
                None
            }
        })?;
        let initial_credit = meta.get("initial_credit").and_then(|v| {
            if let ciborium::Value::Integer(i) = v {
                let n: i128 = (*i).into();
                if n > 0 && n <= u64::MAX as i128 {
                    Some(n as u64)
                } else {
                    None
                }
            } else {
                None
            }
        })?;
        Some(Limits {
            max_frame,
            max_chunk,
            max_reorder_buffer,
            initial_credit,
        })
    }

    /// Check if this is the final frame in a stream
    pub fn is_eof(&self) -> bool {
        self.eof.unwrap_or(false)
    }

    /// Get error code if this is an ERR frame
    pub fn error_code(&self) -> Option<&str> {
        if self.frame_type != FrameType::Err {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("code").and_then(|v| {
                if let ciborium::Value::Text(s) = v {
                    Some(s.as_str())
                } else {
                    None
                }
            })
        })
    }

    /// Parse the mandatory attribution class on an ERR or non-progress LOG.
    pub fn attribution_class(&self) -> Result<crate::failure::AttributionClass, String> {
        let is_attributed_log =
            self.frame_type == FrameType::Log && self.log_level() != Some("progress");
        if self.frame_type != FrameType::Err && !is_attributed_log {
            return Err(format!(
                "{:?} frames do not carry attribution",
                self.frame_type
            ));
        }
        let token = self
            .meta
            .as_ref()
            .and_then(|m| m.get("attribution_class"))
            .and_then(|v| match v {
                ciborium::Value::Text(s) => Some(s.as_str()),
                _ => None,
            })
            .ok_or_else(|| "frame missing required text attribution_class".to_string())?;
        crate::failure::AttributionClass::from_wire(token)
            .ok_or_else(|| format!("unknown attribution_class '{token}'"))
    }

    /// Media URN of the argument the emitter attributed this diagnostic to.
    /// Valid on ERR and non-progress LOG frames. Absence means the emit source
    /// did not attribute the record to one argument; receivers never infer it.
    pub fn attribution_arg_urn(&self) -> Result<Option<&str>, String> {
        let is_attributed_log =
            self.frame_type == FrameType::Log && self.log_level() != Some("progress");
        if self.frame_type != FrameType::Err && !is_attributed_log {
            return Err(format!(
                "{:?} frames do not carry diagnostic argument attribution",
                self.frame_type
            ));
        }
        let Some(value) = self.meta.as_ref().and_then(|meta| meta.get("arg_urn")) else {
            return Ok(None);
        };
        match value {
            ciborium::Value::Text(arg_urn) if !arg_urn.trim().is_empty() => {
                Ok(Some(arg_urn.as_str()))
            }
            ciborium::Value::Text(_) => Err("diagnostic arg_urn must not be empty".to_string()),
            _ => Err("diagnostic arg_urn must be text when present".to_string()),
        }
    }

    /// Get error message if this is an ERR frame
    pub fn error_message(&self) -> Option<&str> {
        if self.frame_type != FrameType::Err {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("message").and_then(|v| {
                if let ciborium::Value::Text(s) = v {
                    Some(s.as_str())
                } else {
                    None
                }
            })
        })
    }

    /// Get log level if this is a LOG frame
    pub fn log_level(&self) -> Option<&str> {
        if self.frame_type != FrameType::Log {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("level").and_then(|v| {
                if let ciborium::Value::Text(s) = v {
                    Some(s.as_str())
                } else {
                    None
                }
            })
        })
    }

    /// Get log message if this is a LOG frame
    pub fn log_message(&self) -> Option<&str> {
        if self.frame_type != FrameType::Log {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("message").and_then(|v| {
                if let ciborium::Value::Text(s) = v {
                    Some(s.as_str())
                } else {
                    None
                }
            })
        })
    }

    /// Get progress value (0.0–1.0) if this is a LOG frame with level="progress"
    /// Describe what the `progress` meta slot actually holds, for the protocol
    /// error a receiver raises when [`Frame::log_progress`] returns `None` on a
    /// `level="progress"` LOG.
    ///
    /// A violation must carry its own evidence: "absent" and "a text value" are
    /// different defects in the emitter, and an error that cannot tell them
    /// apart forces the reader to reproduce the failure to learn which one it
    /// is. Returns the CBOR type name, or `"absent"` when the key is missing.
    pub fn log_progress_slot_description(&self) -> &'static str {
        let Some(meta) = self.meta.as_ref() else {
            return "absent (frame carries no meta)";
        };
        match meta.get("progress") {
            None => "absent",
            Some(ciborium::Value::Text(_)) => "a text value",
            Some(ciborium::Value::Bytes(_)) => "a byte string",
            Some(ciborium::Value::Bool(_)) => "a boolean",
            Some(ciborium::Value::Null) => "null",
            Some(ciborium::Value::Array(_)) => "an array",
            Some(ciborium::Value::Map(_)) => "a map",
            Some(ciborium::Value::Tag(_, _)) => "a tagged value",
            Some(ciborium::Value::Float(_)) => "a float (readable — not this error)",
            Some(ciborium::Value::Integer(_)) => "an integer (readable — not this error)",
            Some(_) => "an unrecognized CBOR type",
        }
    }

    pub fn log_progress(&self) -> Option<f32> {
        if self.frame_type != FrameType::Log {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            // Only return progress if level is "progress"
            let is_progress = m.get("level").and_then(|v| {
                if let ciborium::Value::Text(s) = v {
                    if s == "progress" {
                        Some(())
                    } else {
                        None
                    }
                } else {
                    None
                }
            });
            is_progress?;
            m.get("progress").and_then(|v| match v {
                ciborium::Value::Float(f) => Some(*f as f32),
                ciborium::Value::Integer(i) => {
                    let val: i128 = (*i).into();
                    Some(val as f32)
                }
                _ => None,
            })
        })
    }

    /// Extract max_frame from HELLO metadata
    pub fn hello_max_frame(&self) -> Option<usize> {
        if self.frame_type != FrameType::Hello {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("max_frame").and_then(|v| {
                if let ciborium::Value::Integer(i) = v {
                    let n: i128 = (*i).into();
                    if n > 0 && n <= usize::MAX as i128 {
                        Some(n as usize)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
        })
    }

    /// Extract the JSON-encoded pool-state map (`bifaci::pools`) carried in
    /// this frame's meta — present on the cartridge's HELLO and on every
    /// heartbeat reply. `None` when the frame carries no map; decoding is
    /// the caller's boundary and fails hard there.
    pub fn pool_state_bytes(&self) -> Option<&[u8]> {
        self.meta.as_ref().and_then(|m| {
            m.get(crate::bifaci::pools::META_POOLS).and_then(|v| {
                if let ciborium::Value::Bytes(bytes) = v {
                    Some(bytes.as_slice())
                } else {
                    None
                }
            })
        })
    }

    /// Extract the JSON-encoded desired-capacities map from a heartbeat
    /// PROBE's meta — the host delivering operator `configured` values.
    pub fn desired_capacity_bytes(&self) -> Option<&[u8]> {
        if self.frame_type != FrameType::Heartbeat {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get(crate::bifaci::pools::META_DESIRED_CAPACITIES).and_then(|v| {
                if let ciborium::Value::Bytes(bytes) = v {
                    Some(bytes.as_slice())
                } else {
                    None
                }
            })
        })
    }

    /// A heartbeat PROBE carrying the operator's desired `configured`
    /// values (host→cartridge). The plain probe is `Frame::heartbeat`.
    pub fn heartbeat_with_desired(
        id: MessageId,
        desired: &crate::bifaci::pools::DesiredCapacities,
    ) -> Self {
        let mut frame = Self::heartbeat(id);
        let mut meta = BTreeMap::new();
        meta.insert(
            crate::bifaci::pools::META_DESIRED_CAPACITIES.to_string(),
            ciborium::Value::Bytes(crate::bifaci::pools::encode_desired(desired)),
        );
        frame.meta = Some(meta);
        frame
    }

    /// Extract max_chunk from HELLO metadata
    pub fn hello_max_chunk(&self) -> Option<usize> {
        if self.frame_type != FrameType::Hello {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("max_chunk").and_then(|v| {
                if let ciborium::Value::Integer(i) = v {
                    let n: i128 = (*i).into();
                    if n > 0 && n <= usize::MAX as i128 {
                        Some(n as usize)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
        })
    }

    /// Extract max_reorder_buffer from HELLO metadata
    pub fn hello_max_reorder_buffer(&self) -> Option<usize> {
        if self.frame_type != FrameType::Hello {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("max_reorder_buffer").and_then(|v| {
                if let ciborium::Value::Integer(i) = v {
                    let n: i128 = (*i).into();
                    if n > 0 && n <= usize::MAX as i128 {
                        Some(n as usize)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
        })
    }

    /// Extract initial_credit from HELLO metadata
    pub fn hello_initial_credit(&self) -> Option<u64> {
        if self.frame_type != FrameType::Hello {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("initial_credit").and_then(|v| {
                if let ciborium::Value::Integer(i) = v {
                    let n: i128 = (*i).into();
                    if n > 0 && n <= u64::MAX as i128 {
                        Some(n as u64)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
        })
    }

    /// Extract the protocol version declared in HELLO metadata.
    pub fn hello_version(&self) -> Option<u8> {
        if self.frame_type != FrameType::Hello {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("version").and_then(|v| {
                if let ciborium::Value::Integer(i) = v {
                    let n: i128 = (*i).into();
                    Some(n as u8)
                } else {
                    None
                }
            })
        })
    }

    /// Extract manifest from HELLO metadata (cartridge side sends this).
    /// Returns None if no manifest present (host HELLO) or not a HELLO frame.
    /// The manifest is JSON-encoded cartridge metadata.
    pub fn hello_manifest(&self) -> Option<&[u8]> {
        if self.frame_type != FrameType::Hello {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("manifest").and_then(|v| {
                if let ciborium::Value::Bytes(bytes) = v {
                    Some(bytes.as_slice())
                } else {
                    None
                }
            })
        })
    }

    /// Compute FNV-1a 64-bit checksum of bytes.
    /// This is a simple, fast hash function suitable for detecting transmission errors.
    pub fn compute_checksum(data: &[u8]) -> u64 {
        const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;

        let mut hash = FNV_OFFSET_BASIS;
        for &byte in data {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    /// Returns true if this frame type participates in flow ordering (seq tracking).
    /// Non-flow frames (Hello, Heartbeat, RelayNotify, RelayState, Cancel, Credit)
    /// bypass seq assignment and reorder buffers entirely — Credit in particular must
    /// never queue behind the data it is flow-controlling.
    pub fn is_flow_frame(&self) -> bool {
        !matches!(
            self.frame_type,
            FrameType::Hello
                | FrameType::Heartbeat
                | FrameType::RelayNotify
                | FrameType::RelayState
                | FrameType::Cancel
                | FrameType::Credit
        )
    }
}

impl Default for Frame {
    fn default() -> Self {
        Self::new(FrameType::Req, MessageId::default())
    }
}

// =============================================================================
// FLOW KEY — Composite key for frame ordering (RID + optional XID)
// =============================================================================

/// Composite key identifying a frame flow for seq ordering.
/// Absence of XID (routing_id) is a valid separate flow from presence of XID.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FlowKey {
    pub rid: MessageId,
    pub xid: Option<MessageId>,
}

impl FlowKey {
    /// Extract flow key from a frame.
    pub fn from_frame(frame: &Frame) -> Self {
        Self {
            rid: frame.id.clone(),
            xid: frame.routing_id.clone(),
        }
    }
}

// =============================================================================
// SEQ ASSIGNER — Centralized seq assignment at output stages
// =============================================================================

use std::collections::HashMap;

/// Assigns monotonically increasing seq numbers per FlowKey (RID + optional XID).
/// Used at output stages (writer threads) to ensure each flow's frames
/// carry a contiguous, gap-free seq sequence starting at 0.
///
/// Keyed by FlowKey to match ReorderBuffer's key space exactly:
/// (RID=A, XID=nil) and (RID=A, XID=5) are separate flows with independent counters.
///
/// Non-flow frames (Hello, Heartbeat, RelayNotify, RelayState) are skipped
/// and their seq stays at 0.
#[derive(Debug)]
pub struct SeqAssigner {
    counters: HashMap<FlowKey, u64>,
}

impl SeqAssigner {
    pub fn new() -> Self {
        Self {
            counters: HashMap::new(),
        }
    }

    /// Assign the next seq number to a frame.
    /// Non-flow frames are left unchanged (seq stays 0).
    pub fn assign(&mut self, frame: &mut Frame) {
        if !frame.is_flow_frame() {
            return;
        }
        let key = FlowKey::from_frame(frame);
        let counter = self.counters.entry(key).or_insert(0);
        frame.seq = *counter;
        *counter += 1;
    }

    /// Remove tracking for a flow (call after END/ERR delivery).
    pub fn remove(&mut self, key: &FlowKey) {
        self.counters.remove(key);
    }
}

// =============================================================================
// REORDER BUFFER — Per-flow frame reordering at relay boundaries
// =============================================================================

use crate::bifaci::io::CborError;

/// Per-flow state for the reorder buffer.
struct FlowState {
    expected_seq: u64,
    buffer: BTreeMap<u64, Frame>,
}

/// Reorder buffer for validating and reordering frames at relay boundaries.
/// Keyed by FlowKey (RID + optional XID). Each flow tracks expected seq
/// and buffers out-of-order frames until gaps are filled.
///
/// Protocol errors:
/// - Stale/duplicate seq (frame.seq < expected_seq)
/// - Buffer overflow (buffered frames exceed max_buffer_per_flow)
pub struct ReorderBuffer {
    flows: HashMap<FlowKey, FlowState>,
    max_buffer_per_flow: usize,
}

impl ReorderBuffer {
    pub fn new(max_buffer_per_flow: usize) -> Self {
        Self {
            flows: HashMap::new(),
            max_buffer_per_flow,
        }
    }

    /// Accept a frame into the reorder buffer.
    /// Returns a Vec of frames ready for delivery (in seq order).
    /// Non-flow frames bypass reordering and are returned immediately.
    pub fn accept(&mut self, frame: Frame) -> Result<Vec<Frame>, CborError> {
        if !frame.is_flow_frame() {
            return Ok(vec![frame]);
        }

        let key = FlowKey::from_frame(&frame);
        let state = self.flows.entry(key).or_insert_with(|| FlowState {
            expected_seq: 0,
            buffer: BTreeMap::new(),
        });

        if frame.seq == state.expected_seq {
            // In-order: deliver this frame + drain consecutive buffered frames
            let mut ready = vec![frame];
            state.expected_seq += 1;
            while let Some(buffered) = state.buffer.remove(&state.expected_seq) {
                ready.push(buffered);
                state.expected_seq += 1;
            }
            Ok(ready)
        } else if frame.seq > state.expected_seq {
            // Out-of-order: buffer it
            // Check if this seq is already buffered (duplicate)
            if state.buffer.contains_key(&frame.seq) {
                return Err(CborError::Protocol(format!(
                    "stale/duplicate seq: seq {} already buffered (expected >= {})",
                    frame.seq, state.expected_seq,
                )));
            }
            if state.buffer.len() >= self.max_buffer_per_flow {
                return Err(CborError::Protocol(format!(
                    "reorder buffer overflow: flow has {} buffered frames (max {}), \
                     expected seq {} but got seq {}",
                    state.buffer.len(),
                    self.max_buffer_per_flow,
                    state.expected_seq,
                    frame.seq,
                )));
            }
            state.buffer.insert(frame.seq, frame);
            Ok(vec![])
        } else {
            // Stale or duplicate
            Err(CborError::Protocol(format!(
                "stale/duplicate seq: expected >= {} but got {}",
                state.expected_seq, frame.seq,
            )))
        }
    }

    /// Remove flow state after terminal frame delivery (END/ERR).
    pub fn cleanup_flow(&mut self, key: &FlowKey) {
        self.flows.remove(key);
    }
}

/// Integer keys for CBOR map fields
pub mod keys {
    pub const VERSION: u64 = 0;
    pub const FRAME_TYPE: u64 = 1;
    pub const ID: u64 = 2;
    pub const SEQ: u64 = 3;
    pub const CONTENT_TYPE: u64 = 4;
    pub const META: u64 = 5;
    pub const PAYLOAD: u64 = 6;
    pub const LEN: u64 = 7;
    pub const OFFSET: u64 = 8;
    pub const EOF: u64 = 9;
    pub const CAP: u64 = 10;
    pub const STREAM_ID: u64 = 11; // Stream ID for multiplexed streams
    pub const MEDIA_URN: u64 = 12; // Media URN for stream type identification
    pub const ROUTING_ID: u64 = 13; // Routing ID assigned by RelaySwitch
    pub const INDEX: u64 = 14; // Chunk sequence index within stream (starts at 0)
    pub const CHUNK_COUNT: u64 = 15; // Total chunk count in STREAM_END
    pub const CHECKSUM: u64 = 16; // FNV-1a checksum of payload for CHUNK frames
    pub const IS_SEQUENCE: u64 = 17; // Whether producer used emit_list_item (true) or write (false)
    pub const FORCE_KILL: u64 = 18; // Whether Cancel should force-kill the cartridge process
    pub const CREDIT: u64 = 19; // Flow-control credit grant in CHUNK units (Credit frames)
    pub const UNBOUNDED: u64 = 20; // Stream makes no length promise (STREAM_START frames)
}

/// Which side's stream a CREDIT frame credits (L11 routing discriminator).
/// `Request` credits a request-direction stream (arguments flowing toward the
/// handler): the grant travels toward the REQUESTER. `Response` credits a
/// response-direction stream (handler output): the grant travels toward the
/// HANDLER. Required on every CREDIT frame — (xid, rid) alone cannot
/// disambiguate grant direction for self-loop peer calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CreditDirection {
    Request,
    Response,
}

impl CreditDirection {
    pub fn as_str(&self) -> &'static str {
        match self {
            CreditDirection::Request => "request",
            CreditDirection::Response => "response",
        }
    }

    pub fn from_str_name(s: &str) -> Option<Self> {
        match s {
            "request" => Some(CreditDirection::Request),
            "response" => Some(CreditDirection::Response),
            _ => None,
        }
    }
}

/// Why a frame was dropped instead of delivered. The shared vocabulary for
/// counted drops across every runtime (cartridge writer, host, relay switch,
/// executor); every dropped frame increments exactly one of these counters,
/// observable via the protocol stats snapshots. Frames are never dropped
/// silently.
///
/// A DROP means something went wrong. The benign teardown crossing — a flow
/// frame that arrives after its request's terminal, which the protocol
/// expects (in-flight frames legally race END/ERR, L13) — is NOT a drop and
/// has no reason here: it is counted as a post-terminal STRAGGLER
/// (`StragglerCounters` in `stats`), indicated as benign in every stats
/// surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DropReason {
    /// Flow frame for a request with no routing state (never registered, or
    /// released for a reason the terminated-ledger cannot vouch for). A drop
    /// with this reason indicates something actually went wrong — a frame
    /// arriving after its request's terminal is NOT a drop, it is a benign
    /// straggler counted separately (`StragglerCounters`).
    NoRoute,
    /// Send attempted on a closed channel (receiver gone).
    ChannelClosed,
    /// CHUNK received beyond the granted credit window.
    CreditViolation,
    /// Frame discarded because its request was cancelled.
    Cancelled,
    /// Frame discarded because the owning master/host connection died.
    MasterDied,
}

impl DropReason {
    /// All variants, for counter arrays and snapshot serialization.
    pub const ALL: [DropReason; 5] = [
        DropReason::NoRoute,
        DropReason::ChannelClosed,
        DropReason::CreditViolation,
        DropReason::Cancelled,
        DropReason::MasterDied,
    ];

    /// Stable snake_case name (the wire/snapshot contract for mirrors).
    pub fn as_str(&self) -> &'static str {
        match self {
            DropReason::NoRoute => "no_route",
            DropReason::ChannelClosed => "channel_closed",
            DropReason::CreditViolation => "credit_violation",
            DropReason::Cancelled => "cancelled",
            DropReason::MasterDied => "master_died",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST171: Test all FrameType discriminants roundtrip through u8 conversion preserving identity
    #[test]
    fn test171_frame_type_roundtrip() {
        for t in [
            FrameType::Hello,
            FrameType::Req,
            // Res REMOVED - old protocol
            FrameType::Chunk,
            FrameType::End,
            FrameType::Log,
            FrameType::Err,
            FrameType::Heartbeat,
            FrameType::StreamStart,
            FrameType::StreamEnd,
            FrameType::RelayNotify,
            FrameType::RelayState,
            FrameType::Cancel,
            FrameType::Credit,
        ] {
            let v = t as u8;
            let recovered = FrameType::from_u8(v).expect("should recover frame type");
            assert_eq!(t, recovered);
        }
    }

    // TEST172: Test FrameType::from_u8 returns None for values outside the valid discriminant range
    #[test]
    fn test172_invalid_frame_type() {
        assert!(
            FrameType::from_u8(14).is_none(),
            "value 14 is one past Credit"
        );
        assert!(FrameType::from_u8(100).is_none());
        assert!(FrameType::from_u8(255).is_none());
    }

    // TEST173: Test FrameType discriminant values match the wire protocol specification exactly
    #[test]
    fn test173_frame_type_discriminant_values() {
        assert_eq!(FrameType::Hello as u8, 0);
        assert_eq!(FrameType::Req as u8, 1);
        // 2 = Res REMOVED - old protocol
        assert_eq!(FrameType::Chunk as u8, 3);
        assert_eq!(FrameType::End as u8, 4);
        assert_eq!(FrameType::Log as u8, 5);
        assert_eq!(FrameType::Err as u8, 6);
        assert_eq!(FrameType::Heartbeat as u8, 7);
        assert_eq!(FrameType::StreamStart as u8, 8);
        assert_eq!(FrameType::StreamEnd as u8, 9);
        assert_eq!(FrameType::RelayNotify as u8, 10);
        assert_eq!(FrameType::RelayState as u8, 11);
        assert_eq!(FrameType::Cancel as u8, 12);
    }

    // TEST174: Test MessageId::new_uuid generates valid UUID that roundtrips through string conversion
    #[test]
    fn test174_message_id_uuid() {
        let id = MessageId::new_uuid();
        let s = id.to_uuid_string().expect("should be uuid");
        let recovered = MessageId::from_uuid_str(&s).expect("should parse");
        assert_eq!(id, recovered);
    }

    // TEST175: Test two MessageId::new_uuid calls produce distinct IDs (no collisions)
    #[test]
    fn test175_message_id_uuid_uniqueness() {
        let id1 = MessageId::new_uuid();
        let id2 = MessageId::new_uuid();
        assert_ne!(id1, id2, "two UUIDs must be distinct");
    }

    // TEST176: Test MessageId::Uint does not produce a UUID string, to_uuid_string returns None
    #[test]
    fn test176_message_id_uint_has_no_uuid_string() {
        let id = MessageId::Uint(42);
        assert!(
            id.to_uuid_string().is_none(),
            "Uint IDs have no UUID representation"
        );
    }

    // TEST177: Test MessageId::from_uuid_str rejects invalid UUID strings
    #[test]
    fn test177_message_id_from_invalid_uuid_str() {
        assert!(MessageId::from_uuid_str("not-a-uuid").is_none());
        assert!(MessageId::from_uuid_str("").is_none());
        assert!(MessageId::from_uuid_str("12345678").is_none());
    }

    // TEST178: Test MessageId::as_bytes produces correct byte representations for Uuid and Uint variants
    #[test]
    fn test178_message_id_as_bytes() {
        let uuid_id = MessageId::new_uuid();
        let uuid_bytes = uuid_id.as_bytes();
        assert_eq!(uuid_bytes.len(), 16, "UUID must be 16 bytes");

        let uint_id = MessageId::Uint(0x0102030405060708);
        let uint_bytes = uint_id.as_bytes();
        assert_eq!(uint_bytes.len(), 8, "Uint ID must be 8 bytes big-endian");
        assert_eq!(
            uint_bytes,
            vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
        );
    }

    // TEST179: Test MessageId::default creates a UUID variant (not Uint)
    #[test]
    fn test179_message_id_default_is_uuid() {
        let id = MessageId::default();
        assert!(
            id.to_uuid_string().is_some(),
            "default MessageId must be UUID"
        );
    }

    // TEST180: Test Frame::hello without manifest produces correct HELLO frame for host side
    #[test]
    fn test180_hello_frame() {
        let frame = Frame::hello(&Limits {
            max_frame: 1_000_000,
            max_chunk: 100_000,
            max_reorder_buffer: DEFAULT_MAX_REORDER_BUFFER,
            initial_credit: DEFAULT_INITIAL_CREDIT,
        });
        assert_eq!(frame.frame_type, FrameType::Hello);
        assert_eq!(frame.version, PROTOCOL_VERSION);
        assert_eq!(frame.hello_max_frame(), Some(1_000_000));
        assert_eq!(frame.hello_max_chunk(), Some(100_000));
        assert!(
            frame.hello_manifest().is_none(),
            "Host HELLO must not include manifest"
        );
        assert!(frame.payload.is_none(), "HELLO has no payload");
        // ID should be Uint(0) for HELLO
        assert_eq!(frame.id, MessageId::Uint(0));
    }

    // TEST181: Test Frame::hello_with_manifest produces HELLO with manifest bytes for cartridge side
    #[test]
    fn test181_hello_frame_with_manifest() {
        let manifest_json = r#"{"name":"TestCartridge","version":"1.0.0","channel":"release","description":"Test","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"]}]}]}"#;
        let frame = Frame::hello_with_manifest(
            &Limits {
                max_frame: 1_000_000,
                max_chunk: 100_000,
                max_reorder_buffer: DEFAULT_MAX_REORDER_BUFFER,
                initial_credit: DEFAULT_INITIAL_CREDIT,
            },
            manifest_json.as_bytes(),
            &{
                let mut pools = crate::bifaci::pools::PoolStates::new();
                pools.insert(
                    crate::bifaci::pools::POOL_ALL.to_string(),
                    crate::bifaci::pools::PoolState::declared(0, vec!["cap:effect=none".to_string()]),
                );
                pools
            },
        );
        assert_eq!(frame.frame_type, FrameType::Hello);
        assert_eq!(frame.hello_max_frame(), Some(1_000_000));
        assert_eq!(frame.hello_max_chunk(), Some(100_000));
        let manifest = frame
            .hello_manifest()
            .expect("Cartridge HELLO must include manifest");
        assert_eq!(manifest, manifest_json.as_bytes());
        // The pool map is mandatory HELLO meta and round-trips intact.
        let pools = crate::bifaci::pools::decode_pool_states(
            frame.pool_state_bytes().expect("Cartridge HELLO must include the pool map"),
        )
        .expect("HELLO pool map decodes");
        assert_eq!(pools[crate::bifaci::pools::POOL_ALL].declared, 0);
    }

    // TEST182: Test Frame::req stores cap URN, payload, and content_type correctly
    #[test]
    fn test182_req_frame() {
        let id = MessageId::new_uuid();
        let frame = Frame::req(
            id.clone(),
            r#"cap:in="media:void";test;out="media:void""#,
            b"payload".to_vec(),
            "application/json",
        );
        assert_eq!(frame.frame_type, FrameType::Req);
        assert_eq!(frame.id, id);
        assert_eq!(
            frame.cap,
            Some(r#"cap:in="media:void";test;out="media:void""#.to_string())
        );
        assert_eq!(frame.payload, Some(b"payload".to_vec()));
        assert_eq!(frame.content_type, Some("application/json".to_string()));
        assert_eq!(frame.version, PROTOCOL_VERSION);
    }

    // TEST183 REMOVED: Frame::res() and FrameType::Res removed - old protocol no longer supported
    // NEW PROTOCOL: Use stream multiplexing (STREAM_START + CHUNK + STREAM_END + END)

    // TEST184: Test Frame::chunk stores seq and payload for streaming (with stream_id)
    #[test]
    fn test184_chunk_frame() {
        let id = MessageId::new_uuid();
        let stream_id = "stream-123".to_string();
        let payload = b"data".to_vec();
        let checksum = Frame::compute_checksum(&payload);
        let frame = Frame::chunk(id.clone(), stream_id.clone(), 3, payload, 3, checksum);
        assert_eq!(frame.frame_type, FrameType::Chunk);
        assert_eq!(frame.id, id);
        assert_eq!(frame.stream_id, Some(stream_id));
        assert_eq!(frame.seq, 3);
        assert_eq!(frame.payload, Some(b"data".to_vec()));
        assert!(!frame.is_eof(), "plain chunk should not be EOF");
    }

    // TEST185: Test Frame::err stores error code and message in metadata
    #[test]
    fn test185_err_frame() {
        let id = MessageId::new_uuid();
        let frame = Frame::err(
            id,
            "NOT_FOUND",
            crate::failure::AttributionClass::Internal,
            "Cap not found",
            None,
        );
        assert_eq!(frame.frame_type, FrameType::Err);
        assert_eq!(frame.error_code(), Some("NOT_FOUND"));
        assert_eq!(frame.error_message(), Some("Cap not found"));
    }

    // TEST1900: the ERR and non-progress LOG attribution-class wire contract
    // (docs/failure-taxonomy.md): err writes meta
    // code+attribution_class+message; missing and unknown attribution are
    // rejected as protocol violations. Mirrored in all runtime SDKs.
    #[test]
    fn test1900_err_frame_attribution_class_wire_contract() {
        use crate::failure::AttributionClass;

        let id = MessageId::new_uuid();
        let classified = Frame::err(
            id.clone(),
            "CONTEXT_OVERFLOW",
            AttributionClass::Input,
            "prompt too large",
            None,
        );
        assert_eq!(classified.error_code(), Some("CONTEXT_OVERFLOW"));
        assert_eq!(classified.attribution_class(), Ok(AttributionClass::Input));
        assert_eq!(classified.error_message(), Some("prompt too large"));

        let mut missing = Frame::err(id.clone(), "BOOM", AttributionClass::Internal, "x", None);
        missing.meta.as_mut().unwrap().remove("attribution_class");
        assert!(missing.attribution_class().is_err());

        let mut weird = Frame::err(id, "BOOM", AttributionClass::Internal, "x", None);
        weird.meta.as_mut().unwrap().insert(
            "attribution_class".to_string(),
            ciborium::Value::Text("user-error".to_string()),
        );
        assert!(weird.attribution_class().is_err());

        let mut malformed_arg = Frame::err(
            MessageId::new_uuid(),
            "BOOM",
            AttributionClass::Internal,
            "x",
            None,
        );
        malformed_arg
            .meta
            .as_mut()
            .unwrap()
            .insert("arg_urn".to_string(), ciborium::Value::Integer(7.into()));
        assert!(malformed_arg.attribution_arg_urn().is_err());
        malformed_arg
            .meta
            .as_mut()
            .unwrap()
            .insert("arg_urn".to_string(), ciborium::Value::Text(String::new()));
        assert!(malformed_arg.attribution_arg_urn().is_err());
    }

    /// TEST7105: an ERR frame built WITH argument attribution round-trips it
    /// through encode→decode: meta carries `arg_urn`, `attribution_arg_urn()`
    /// serves it, and code/class/message arrive intact beside it. Mirrored
    /// in the Go/Python/Swift runtimes (shared 7105/7106 range).
    #[test]
    fn test7105_err_frame_arg_urn_round_trip() {
        use crate::bifaci::io::{decode_frame, encode_frame};
        use crate::failure::AttributionClass;

        let id = MessageId::new_uuid();
        let frame = Frame::err(
            id,
            "CONTEXT_OVERFLOW",
            AttributionClass::Input,
            "prompt too large",
            Some("media:prompt;textable"),
        );
        assert_eq!(
            frame.attribution_arg_urn().unwrap(),
            Some("media:prompt;textable")
        );

        let decoded = decode_frame(&encode_frame(&frame).expect("encode")).expect("decode");
        assert_eq!(
            decoded
                .meta
                .as_ref()
                .expect("ERR frame has meta")
                .get("arg_urn"),
            Some(&ciborium::Value::Text("media:prompt;textable".to_string())),
            "the wire meta map carries the attribution key verbatim"
        );
        assert_eq!(
            decoded.attribution_arg_urn().unwrap(),
            Some("media:prompt;textable")
        );
        assert_eq!(decoded.error_code(), Some("CONTEXT_OVERFLOW"));
        assert_eq!(decoded.attribution_class(), Ok(AttributionClass::Input));
        assert_eq!(decoded.error_message(), Some("prompt too large"));
    }

    // TEST7117: non-progress LOG carries the same source attribution tuple as
    // ERR, including an optional argument URN, through the actual wire codec.
    #[test]
    fn test7117_log_frame_arg_urn_round_trip() {
        use crate::bifaci::io::{decode_frame, encode_frame};
        use crate::failure::AttributionClass;

        let frame = Frame::log(
            MessageId::new_uuid(),
            "warn",
            AttributionClass::Resource,
            "model cache is under memory pressure",
            Some("media:model-spec"),
        );
        let decoded = decode_frame(&encode_frame(&frame).expect("encode")).expect("decode");
        assert_eq!(decoded.frame_type, FrameType::Log);
        assert_eq!(decoded.log_level(), Some("warn"));
        assert_eq!(decoded.attribution_class(), Ok(AttributionClass::Resource));
        assert_eq!(
            decoded.attribution_arg_urn().unwrap(),
            Some("media:model-spec")
        );
        assert_eq!(
            decoded.log_message(),
            Some("model cache is under memory pressure")
        );
    }

    /// TEST7106: attribution is OPTIONAL and source-declared at every hop.
    /// An ERR frame built WITHOUT attribution has NO `arg_urn` key in meta
    /// (absent, not empty) and `attribution_arg_urn()` is `Ok(None)` after a wire round
    /// trip; the `OpError::Classified` and `ExecutionError` accessors serve
    /// the emit source's attribution in the Some case and None — never a
    /// guess — in the unattributed case.
    #[test]
    fn test7106_arg_urn_absent_when_unattributed_and_accessor_threading() {
        use crate::bifaci::io::{decode_frame, encode_frame};
        use crate::failure::AttributionClass;
        use crate::orchestrator::executor::ExecutionError;

        let id = MessageId::new_uuid();
        let frame = Frame::err(
            id,
            "GPU_OUT_OF_MEMORY",
            AttributionClass::Resource,
            "no VRAM",
            None,
        );
        let decoded = decode_frame(&encode_frame(&frame).expect("encode")).expect("decode");
        assert!(
            !decoded
                .meta
                .as_ref()
                .expect("ERR frame has meta")
                .contains_key("arg_urn"),
            "unattributed ERR must carry NO arg_urn key — absence, never an empty string"
        );
        assert_eq!(decoded.attribution_arg_urn().unwrap(), None);
        assert_eq!(decoded.error_code(), Some("GPU_OUT_OF_MEMORY"));
        assert_eq!(decoded.attribution_class(), Ok(AttributionClass::Resource));

        // L1 — the op layer serves the emit source's attribution verbatim.
        let attributed = ops_rs::OpError::Classified {
            code: "CONTEXT_OVERFLOW".to_string(),
            class: AttributionClass::Input,
            message: "prompt too large".to_string(),
            arg_urn: Some("media:prompt;textable".to_string()),
        };
        assert_eq!(attributed.failure_arg_urn(), Some("media:prompt;textable"));
        let unattributed = ops_rs::OpError::Classified {
            code: "CONTEXT_OVERFLOW".to_string(),
            class: AttributionClass::Input,
            message: "prompt too large".to_string(),
            arg_urn: None,
        };
        assert_eq!(unattributed.failure_arg_urn(), None);

        // L4 — the orchestrator serves the attribution the ERR frame carried.
        let attributed = ExecutionError::CartridgeExecutionFailed {
            cap_urn: "cap:in=media:prompt;textable".to_string(),
            code: Some("CONTEXT_OVERFLOW".to_string()),
            class: AttributionClass::Input,
            details: "prompt too large".to_string(),
            arg_urn: Some("media:prompt;textable".to_string()),
        };
        assert_eq!(attributed.failure_arg_urn(), Some("media:prompt;textable"));
        let unattributed = ExecutionError::CartridgeExecutionFailed {
            cap_urn: "cap:in=media:prompt;textable".to_string(),
            code: Some("GPU_OUT_OF_MEMORY".to_string()),
            class: AttributionClass::Resource,
            details: "no VRAM".to_string(),
            arg_urn: None,
        };
        assert_eq!(unattributed.failure_arg_urn(), None);
        // A variant with no attribution channel is never about one argument.
        assert_eq!(
            ExecutionError::HostError("boom".to_string()).failure_arg_urn(),
            None
        );
    }

    // TEST186: Test Frame::log stores level and message in metadata
    #[test]
    fn test186_log_frame() {
        let id = MessageId::new_uuid();
        let frame = Frame::log(
            id.clone(),
            "info",
            crate::AttributionClass::Internal,
            "Processing started",
            None,
        );
        assert_eq!(frame.frame_type, FrameType::Log);
        assert_eq!(frame.id, id);
        assert_eq!(frame.log_level(), Some("info"));
        assert_eq!(frame.log_message(), Some("Processing started"));
    }

    // TEST187: Test Frame::end with payload sets eof and optional final payload
    #[test]
    fn test187_end_frame_with_payload() {
        let id = MessageId::new_uuid();
        let frame = Frame::end(id.clone(), Some(b"final".to_vec()));
        assert_eq!(frame.frame_type, FrameType::End);
        assert!(frame.is_eof());
        assert_eq!(frame.payload, Some(b"final".to_vec()));
    }

    // TEST188: Test Frame::end without payload still sets eof marker
    #[test]
    fn test188_end_frame_without_payload() {
        let id = MessageId::new_uuid();
        let frame = Frame::end(id, None);
        assert_eq!(frame.frame_type, FrameType::End);
        assert!(frame.is_eof());
        assert!(frame.payload.is_none());
    }

    // TEST189: Test chunk_with_offset sets offset on all chunks but len only on seq=0 (with stream_id)
    #[test]
    fn test189_chunk_with_offset() {
        let id = MessageId::new_uuid();
        let stream_id = "stream-456".to_string();
        let payload1 = b"data".to_vec();
        let checksum1 = Frame::compute_checksum(&payload1);
        let first = Frame::chunk_with_offset(
            id.clone(),
            stream_id.clone(),
            0,
            payload1,
            0,
            Some(1000),
            false,
            0,
            checksum1,
        );
        assert_eq!(first.seq, 0);
        assert_eq!(first.offset, Some(0));
        assert_eq!(first.len, Some(1000), "first chunk must carry total len");
        assert!(!first.is_eof());

        let payload2 = b"mid".to_vec();
        let checksum2 = Frame::compute_checksum(&payload2);
        let mid = Frame::chunk_with_offset(
            id.clone(),
            stream_id.clone(),
            3,
            payload2,
            500,
            Some(9999),
            false,
            3,
            checksum2,
        );
        assert!(
            mid.len.is_none(),
            "non-first chunk must not carry len (chunk_index != 0)"
        );
        assert_eq!(mid.offset, Some(500));

        let payload3 = b"last".to_vec();
        let checksum3 = Frame::compute_checksum(&payload3);
        let last =
            Frame::chunk_with_offset(id, stream_id, 5, payload3, 900, None, true, 5, checksum3);
        assert!(last.is_eof());
        assert!(last.len.is_none());
    }

    // TEST190: Test Frame::heartbeat creates minimal frame with no payload or metadata
    #[test]
    fn test190_heartbeat_frame() {
        let id = MessageId::new_uuid();
        let frame = Frame::heartbeat(id.clone());
        assert_eq!(frame.frame_type, FrameType::Heartbeat);
        assert_eq!(frame.id, id);
        assert!(frame.payload.is_none());
        assert!(frame.meta.is_none());
        assert_eq!(frame.seq, 0);
    }

    // TEST1162: Heartbeat frames preserve self-reported memory values stored in metadata.
    #[test]
    fn test1162_heartbeat_frame_with_memory_meta() {
        let id = MessageId::new_uuid();
        let mut frame = Frame::heartbeat(id.clone());

        // Simulate cartridge attaching memory info to heartbeat response
        let mut meta = std::collections::BTreeMap::new();
        meta.insert(
            "footprint_mb".to_string(),
            ciborium::Value::Integer((4096i64).into()),
        );
        meta.insert(
            "rss_mb".to_string(),
            ciborium::Value::Integer((5120i64).into()),
        );
        frame.meta = Some(meta);

        assert_eq!(frame.frame_type, FrameType::Heartbeat);
        assert_eq!(frame.id, id);

        // Verify memory values can be extracted (same pattern as host_runtime.rs)
        let meta = frame.meta.as_ref().unwrap();
        match meta.get("footprint_mb") {
            Some(ciborium::Value::Integer(v)) => {
                let mb: u64 = (*v).try_into().unwrap();
                assert_eq!(mb, 4096);
            }
            other => panic!("Expected Integer(4096), got {:?}", other),
        }
        match meta.get("rss_mb") {
            Some(ciborium::Value::Integer(v)) => {
                let mb: u64 = (*v).try_into().unwrap();
                assert_eq!(mb, 5120);
            }
            other => panic!("Expected Integer(5120), got {:?}", other),
        }
    }

    // TEST191: Test error_code and error_message return None for non-Err frame types
    #[test]
    fn test191_error_accessors_on_non_err_frame() {
        let req = Frame::req(
            MessageId::new_uuid(),
            r#"cap:in="media:void";test;out="media:void""#,
            vec![],
            "text/plain",
        );
        assert!(req.error_code().is_none(), "REQ must have no error_code");
        assert!(
            req.error_message().is_none(),
            "REQ must have no error_message"
        );

        let hello = Frame::hello(&Limits {
            max_frame: 1000,
            max_chunk: 500,
            max_reorder_buffer: DEFAULT_MAX_REORDER_BUFFER,
            initial_credit: DEFAULT_INITIAL_CREDIT,
        });
        assert!(hello.error_code().is_none());
    }

    // TEST192: Test log_level and log_message return None for non-Log frame types
    #[test]
    fn test192_log_accessors_on_non_log_frame() {
        let req = Frame::req(
            MessageId::new_uuid(),
            r#"cap:in="media:void";test;out="media:void""#,
            vec![],
            "text/plain",
        );
        assert!(req.log_level().is_none(), "REQ must have no log_level");
        assert!(req.log_message().is_none(), "REQ must have no log_message");
    }

    // TEST193: Test hello_max_frame and hello_max_chunk return None for non-Hello frame types
    #[test]
    fn test193_hello_accessors_on_non_hello_frame() {
        let err = Frame::err(
            MessageId::new_uuid(),
            "E",
            crate::failure::AttributionClass::Internal,
            "m",
            None,
        );
        assert!(err.hello_max_frame().is_none());
        assert!(err.hello_max_chunk().is_none());
        assert!(err.hello_manifest().is_none());
    }

    // TEST194: Test Frame::new sets version and defaults correctly, optional fields are None
    #[test]
    fn test194_frame_new_defaults() {
        let id = MessageId::new_uuid();
        let frame = Frame::new(FrameType::Chunk, id.clone());
        assert_eq!(frame.version, PROTOCOL_VERSION);
        assert_eq!(frame.frame_type, FrameType::Chunk);
        assert_eq!(frame.id, id);
        assert_eq!(frame.seq, 0);
        assert!(frame.content_type.is_none());
        assert!(frame.meta.is_none());
        assert!(frame.payload.is_none());
        assert!(frame.len.is_none());
        assert!(frame.offset.is_none());
        assert!(frame.eof.is_none());
        assert!(frame.cap.is_none());
    }

    // TEST195: Test Frame::default creates a Req frame (the documented default)
    #[test]
    fn test195_frame_default() {
        let frame = Frame::default();
        assert_eq!(frame.frame_type, FrameType::Req);
        assert_eq!(frame.version, PROTOCOL_VERSION);
    }

    // TEST196: Test is_eof returns false when eof field is None (unset)
    #[test]
    fn test196_is_eof_when_none() {
        let frame = Frame::new(FrameType::Chunk, MessageId::Uint(0));
        assert!(!frame.is_eof(), "eof=None must mean not EOF");
    }

    // TEST197: Test is_eof returns false when eof field is explicitly Some(false)
    #[test]
    fn test197_is_eof_when_false() {
        let mut frame = Frame::new(FrameType::Chunk, MessageId::Uint(0));
        frame.eof = Some(false);
        assert!(!frame.is_eof());
    }

    // TEST198: Test Limits::default provides the documented default values
    #[test]
    fn test198_limits_default() {
        let limits = Limits::default();
        assert_eq!(limits.max_frame, DEFAULT_MAX_FRAME);
        assert_eq!(limits.max_chunk, DEFAULT_MAX_CHUNK);
        assert_eq!(limits.max_frame, 3_670_016, "default max_frame = 3.5 MB");
        assert_eq!(limits.max_chunk, 262_144, "default max_chunk = 256 KB");
        assert_eq!(
            limits.initial_credit, 32,
            "default initial_credit = 32 chunks"
        );
    }

    // TEST199: Test PROTOCOL_VERSION is 4
    #[test]
    fn test199_protocol_version_constant() {
        assert_eq!(PROTOCOL_VERSION, 4);
    }

    // TEST200: Test integer key constants match the protocol specification
    #[test]
    fn test200_key_constants() {
        assert_eq!(keys::VERSION, 0);
        assert_eq!(keys::FRAME_TYPE, 1);
        assert_eq!(keys::ID, 2);
        assert_eq!(keys::SEQ, 3);
        assert_eq!(keys::CONTENT_TYPE, 4);
        assert_eq!(keys::META, 5);
        assert_eq!(keys::PAYLOAD, 6);
        assert_eq!(keys::LEN, 7);
        assert_eq!(keys::OFFSET, 8);
        assert_eq!(keys::EOF, 9);
        assert_eq!(keys::CAP, 10);
    }

    // TEST201: Test hello_with_manifest preserves binary manifest data (not just JSON text)
    #[test]
    fn test201_hello_manifest_binary_data() {
        let binary_manifest = vec![0x00, 0x01, 0xFF, 0xFE, 0x80];
        let frame = Frame::hello_with_manifest(
            &Limits {
                max_frame: 1000,
                max_chunk: 500,
                max_reorder_buffer: DEFAULT_MAX_REORDER_BUFFER,
                initial_credit: DEFAULT_INITIAL_CREDIT,
            },
            &binary_manifest,
            &crate::bifaci::pools::PoolStates::new(),
        );
        assert_eq!(frame.hello_manifest().unwrap(), &binary_manifest);
    }

    // TEST202: Test MessageId Eq/Hash semantics: equal UUIDs are equal, different ones are not
    #[test]
    fn test202_message_id_equality_and_hash() {
        use std::collections::HashSet;

        let id1 = MessageId::Uuid([1; 16]);
        let id2 = MessageId::Uuid([1; 16]);
        let id3 = MessageId::Uuid([2; 16]);
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);

        let mut set = HashSet::new();
        set.insert(id1.clone());
        assert!(set.contains(&id2), "equal IDs must hash the same");
        assert!(!set.contains(&id3));

        let uint1 = MessageId::Uint(42);
        let uint2 = MessageId::Uint(42);
        let uint3 = MessageId::Uint(43);
        assert_eq!(uint1, uint2);
        assert_ne!(uint1, uint3);
    }

    // TEST203: Test Uuid and Uint variants of MessageId are never equal even for coincidental byte values
    #[test]
    fn test203_message_id_cross_variant_inequality() {
        let uuid_id = MessageId::Uuid([0; 16]);
        let uint_id = MessageId::Uint(0);
        assert_ne!(uuid_id, uint_id, "different variants must not be equal");
    }

    // TEST204: Test Frame::req with empty payload stores Some(empty vec) not None
    #[test]
    fn test204_req_frame_empty_payload() {
        let frame = Frame::req(
            MessageId::new_uuid(),
            r#"cap:in="media:void";test;out="media:void""#,
            vec![],
            "text/plain",
        );
        assert_eq!(
            frame.payload,
            Some(vec![]),
            "empty payload is still Some(vec![])"
        );
    }

    // TEST365: Frame::stream_start stores request_id, stream_id, and media_urn
    #[test]
    fn test365_stream_start_frame() {
        let req_id = MessageId::new_uuid();
        let stream_id = "stream-abc-123".to_string();
        let media_urn = "media:".to_string();

        let frame = Frame::stream_start(req_id.clone(), stream_id.clone(), media_urn.clone(), None);

        assert_eq!(frame.frame_type, FrameType::StreamStart);
        assert_eq!(frame.id, req_id);
        assert_eq!(frame.stream_id, Some(stream_id));
        assert_eq!(frame.media_urn, Some(media_urn));
        assert_eq!(frame.seq, 0);
        assert!(frame.payload.is_none());
    }

    // TEST366: Frame::stream_end stores request_id and stream_id
    #[test]
    fn test366_stream_end_frame() {
        let req_id = MessageId::new_uuid();
        let stream_id = "stream-xyz-789".to_string();

        let frame = Frame::stream_end(req_id.clone(), stream_id.clone(), 5);

        assert_eq!(frame.frame_type, FrameType::StreamEnd);
        assert_eq!(frame.id, req_id);
        assert_eq!(frame.stream_id, Some(stream_id));
        assert!(
            frame.media_urn.is_none(),
            "StreamEnd should not have media_urn"
        );
        assert_eq!(frame.seq, 0);
        assert!(frame.payload.is_none());
    }

    // TEST367: StreamStart frame with empty stream_id still constructs (validation happens elsewhere)
    #[test]
    fn test367_stream_start_with_empty_stream_id() {
        let req_id = MessageId::new_uuid();
        let frame = Frame::stream_start(req_id.clone(), String::new(), "media:".to_string(), None);

        assert_eq!(frame.frame_type, FrameType::StreamStart);
        assert_eq!(frame.stream_id, Some(String::new()));
        // Protocol validation happens at a higher level, not in constructor
    }

    // TEST368: StreamStart frame with empty media_urn still constructs (validation happens elsewhere)
    #[test]
    fn test368_stream_start_with_empty_media_urn() {
        let req_id = MessageId::new_uuid();
        let frame =
            Frame::stream_start(req_id.clone(), "stream-id".to_string(), String::new(), None);

        assert_eq!(frame.frame_type, FrameType::StreamStart);
        assert_eq!(frame.media_urn, Some(String::new()));
        // Protocol validation happens at a higher level, not in constructor
    }

    // TEST399: Verify RelayNotify frame type discriminant roundtrips through u8 (value 10)
    #[test]
    fn test399_relay_notify_discriminant_roundtrip() {
        let v = FrameType::RelayNotify as u8;
        assert_eq!(v, 10);
        let recovered = FrameType::from_u8(v).expect("10 must map to RelayNotify");
        assert_eq!(recovered, FrameType::RelayNotify);
    }

    // TEST400: Verify RelayState frame type discriminant roundtrips through u8 (value 11)
    #[test]
    fn test400_relay_state_discriminant_roundtrip() {
        let v = FrameType::RelayState as u8;
        assert_eq!(v, 11);
        let recovered = FrameType::from_u8(v).expect("11 must map to RelayState");
        assert_eq!(recovered, FrameType::RelayState);
    }

    // TEST401: Verify relay_notify factory stores manifest and limits, and accessors extract them
    #[test]
    fn test401_relay_notify_frame() {
        let manifest = br#"{"name":"Test","version":"1.0","channel":"release","description":"Test","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=\"media:void\";test;out=\"media:void\"","title":"Test","aliases": ["test"]}]}]}"#;
        let limits = Limits {
            max_frame: 2_000_000,
            max_chunk: 128_000,
            ..Limits::default()
        };
        let frame = Frame::relay_notify(manifest, &limits);

        assert_eq!(frame.frame_type, FrameType::RelayNotify);
        assert_eq!(frame.id, MessageId::Uint(0));
        assert_eq!(frame.relay_notify_manifest(), Some(manifest.as_slice()));

        let extracted_limits = frame.relay_notify_limits().expect("must have limits");
        assert_eq!(extracted_limits.max_frame, 2_000_000);
        assert_eq!(extracted_limits.max_chunk, 128_000);
    }

    // TEST402: Verify relay_state factory stores resource payload in frame payload field
    #[test]
    fn test402_relay_state_frame() {
        let resources = b"{\"memory_mb\":4096,\"cpu_percent\":50}";
        let frame = Frame::relay_state(resources);

        assert_eq!(frame.frame_type, FrameType::RelayState);
        assert_eq!(frame.id, MessageId::Uint(0));
        assert_eq!(frame.payload, Some(resources.to_vec()));
        assert!(
            frame.meta.is_none(),
            "RelayState carries data in payload, not meta"
        );
    }

    // TEST403: Verify from_u8 returns None for values past the last valid frame type
    #[test]
    fn test403_invalid_frame_type_past_cancel() {
        assert_eq!(
            FrameType::from_u8(12),
            Some(FrameType::Cancel),
            "12 is Cancel"
        );
        assert_eq!(
            FrameType::from_u8(13),
            Some(FrameType::Credit),
            "13 is Credit (v4)"
        );
        assert!(
            FrameType::from_u8(14).is_none(),
            "14 is past the last valid frame type"
        );
        assert!(
            FrameType::from_u8(2).is_none(),
            "2 (old Res) is permanently retired"
        );
    }

    // TEST436: Verify FNV-1a checksum function produces consistent results
    #[test]
    fn test436_compute_checksum() {
        let data1 = b"hello world";
        let data2 = b"hello world";
        let data3 = b"hello world!";

        let checksum1 = Frame::compute_checksum(data1);
        let checksum2 = Frame::compute_checksum(data2);
        let checksum3 = Frame::compute_checksum(data3);

        assert_eq!(checksum1, checksum2, "same data produces same checksum");
        assert_ne!(
            checksum1, checksum3,
            "different data produces different checksum"
        );
        assert_ne!(
            checksum1, 0,
            "checksum should not be zero for non-empty data"
        );
    }

    // TEST902: Verify FNV-1a checksum handles empty data
    #[test]
    fn test902_compute_checksum_empty() {
        let empty = b"";
        let checksum = Frame::compute_checksum(empty);
        assert_eq!(
            checksum, 0xcbf29ce484222325,
            "empty data produces FNV offset basis"
        );
    }

    // TEST903: Verify CHUNK frame can store chunk_index and checksum fields
    #[test]
    fn test903_chunk_with_chunk_index_and_checksum() {
        let id = MessageId::Uuid([1; 16]);
        let stream_id = "test-stream".to_string();
        let payload = b"chunk data".to_vec();
        let checksum = Frame::compute_checksum(&payload);

        let frame = Frame::chunk(
            id.clone(),
            stream_id.clone(),
            5,
            payload.clone(),
            3,
            checksum,
        );

        assert_eq!(frame.frame_type, FrameType::Chunk);
        assert_eq!(frame.id, id);
        assert_eq!(frame.stream_id, Some(stream_id));
        assert_eq!(frame.seq, 5);
        assert_eq!(frame.chunk_index, Some(3), "chunk_index should be set");
        assert_eq!(frame.checksum, Some(checksum), "checksum should be set");
    }

    // TEST904: Verify STREAM_END frame can store chunk_count field
    #[test]
    fn test904_stream_end_with_chunk_count() {
        let id = MessageId::Uuid([1; 16]);
        let stream_id = "test-stream".to_string();

        let frame = Frame::stream_end(id.clone(), stream_id.clone(), 42);

        assert_eq!(frame.frame_type, FrameType::StreamEnd);
        assert_eq!(frame.id, id);
        assert_eq!(frame.stream_id, Some(stream_id));
        assert_eq!(frame.chunk_count, Some(42), "chunk_count should be set");
    }

    // =========================================================================
    // SeqAssigner tests
    // =========================================================================

    // TEST442: SeqAssigner assigns seq 0,1,2,3 for consecutive frames with same RID
    #[test]
    fn test442_seq_assigner_monotonic_same_rid() {
        let mut assigner = SeqAssigner::new();
        let rid = MessageId::new_uuid();

        let mut f0 = Frame::new(FrameType::Req, rid.clone());
        let mut f1 = Frame::new(FrameType::StreamStart, rid.clone());
        let mut f2 = Frame::new(FrameType::Chunk, rid.clone());
        let mut f3 = Frame::new(FrameType::End, rid.clone());

        assigner.assign(&mut f0);
        assigner.assign(&mut f1);
        assigner.assign(&mut f2);
        assigner.assign(&mut f3);

        assert_eq!(f0.seq, 0);
        assert_eq!(f1.seq, 1);
        assert_eq!(f2.seq, 2);
        assert_eq!(f3.seq, 3);
    }

    // TEST443: SeqAssigner maintains independent counters for different RIDs
    #[test]
    fn test443_seq_assigner_independent_rids() {
        let mut assigner = SeqAssigner::new();
        let rid_a = MessageId::new_uuid();
        let rid_b = MessageId::new_uuid();

        let mut a0 = Frame::new(FrameType::Req, rid_a.clone());
        let mut b0 = Frame::new(FrameType::Req, rid_b.clone());
        let mut a1 = Frame::new(FrameType::Chunk, rid_a.clone());
        let mut b1 = Frame::new(FrameType::Chunk, rid_b.clone());
        let mut a2 = Frame::new(FrameType::End, rid_a.clone());

        assigner.assign(&mut a0);
        assigner.assign(&mut b0);
        assigner.assign(&mut a1);
        assigner.assign(&mut b1);
        assigner.assign(&mut a2);

        assert_eq!(a0.seq, 0);
        assert_eq!(a1.seq, 1);
        assert_eq!(a2.seq, 2);
        assert_eq!(b0.seq, 0);
        assert_eq!(b1.seq, 1);
    }

    // TEST444: SeqAssigner skips non-flow frames (Heartbeat, RelayNotify, RelayState, Hello)
    #[test]
    fn test444_seq_assigner_skips_non_flow() {
        let mut assigner = SeqAssigner::new();

        let mut hello = Frame::new(FrameType::Hello, MessageId::Uint(0));
        let mut hb = Frame::new(FrameType::Heartbeat, MessageId::new_uuid());
        let mut notify = Frame::new(FrameType::RelayNotify, MessageId::Uint(0));
        let mut state = Frame::new(FrameType::RelayState, MessageId::Uint(0));

        assigner.assign(&mut hello);
        assigner.assign(&mut hb);
        assigner.assign(&mut notify);
        assigner.assign(&mut state);

        assert_eq!(hello.seq, 0, "Hello seq must stay 0");
        assert_eq!(hb.seq, 0, "Heartbeat seq must stay 0");
        assert_eq!(notify.seq, 0, "RelayNotify seq must stay 0");
        assert_eq!(state.seq, 0, "RelayState seq must stay 0");
    }

    // TEST445: SeqAssigner.remove with FlowKey(rid, None) resets that flow; FlowKey(rid, Some(xid)) is unaffected
    #[test]
    fn test445_seq_assigner_remove_by_flow_key() {
        let mut assigner = SeqAssigner::new();
        let rid = MessageId::new_uuid();
        let xid = MessageId::new_uuid();

        // Flow 1: (rid, None) — cartridge peer invoke
        let mut f0 = Frame::new(FrameType::Req, rid.clone());
        let mut f1 = Frame::new(FrameType::End, rid.clone());
        assigner.assign(&mut f0);
        assigner.assign(&mut f1);
        assert_eq!(f1.seq, 1);

        // Flow 2: (rid, Some(xid)) — relay response
        let mut g0 = Frame::new(FrameType::Req, rid.clone());
        g0.routing_id = Some(xid.clone());
        let mut g1 = Frame::new(FrameType::Chunk, rid.clone());
        g1.routing_id = Some(xid.clone());
        assigner.assign(&mut g0);
        assigner.assign(&mut g1);
        assert_eq!(g0.seq, 0);
        assert_eq!(g1.seq, 1);

        // Remove Flow 1 only
        assigner.remove(&FlowKey {
            rid: rid.clone(),
            xid: None,
        });

        // Flow 1 restarts at 0
        let mut f2 = Frame::new(FrameType::Req, rid.clone());
        assigner.assign(&mut f2);
        assert_eq!(
            f2.seq, 0,
            "after remove(rid, None), that flow restarts at 0"
        );

        // Flow 2 continues unaffected
        let mut g2 = Frame::new(FrameType::End, rid.clone());
        g2.routing_id = Some(xid.clone());
        assigner.assign(&mut g2);
        assert_eq!(
            g2.seq, 2,
            "remove(rid, None) must not affect (rid, Some(xid))"
        );
    }

    // TEST860: Same RID with different XIDs get independent seq counters
    #[test]
    fn test860_seq_assigner_same_rid_different_xids_independent() {
        let mut assigner = SeqAssigner::new();
        let rid = MessageId::new_uuid();
        let xid_a = MessageId::Uint(1);
        let xid_b = MessageId::Uint(2);

        // Flow A: (rid, xid_a)
        let mut a0 = Frame::new(FrameType::Req, rid.clone());
        a0.routing_id = Some(xid_a.clone());
        let mut a1 = Frame::new(FrameType::Chunk, rid.clone());
        a1.routing_id = Some(xid_a.clone());

        // Flow B: (rid, xid_b)
        let mut b0 = Frame::new(FrameType::Req, rid.clone());
        b0.routing_id = Some(xid_b.clone());

        // Flow C: (rid, None) — no XID
        let mut c0 = Frame::new(FrameType::Req, rid.clone());

        assigner.assign(&mut a0);
        assigner.assign(&mut b0);
        assigner.assign(&mut a1);
        assigner.assign(&mut c0);

        assert_eq!(a0.seq, 0, "flow (rid, xid_a) starts at 0");
        assert_eq!(a1.seq, 1, "flow (rid, xid_a) increments to 1");
        assert_eq!(b0.seq, 0, "flow (rid, xid_b) starts at 0 independently");
        assert_eq!(c0.seq, 0, "flow (rid, None) starts at 0 independently");
    }

    // TEST446: SeqAssigner handles mixed frame types (REQ, CHUNK, LOG, END) for same RID
    #[test]
    fn test446_seq_assigner_mixed_types() {
        let mut assigner = SeqAssigner::new();
        let rid = MessageId::new_uuid();

        let mut req = Frame::new(FrameType::Req, rid.clone());
        let mut log = Frame::log(
            rid.clone(),
            "info",
            crate::AttributionClass::Internal,
            "progress",
            None,
        );
        let mut chunk = Frame::new(FrameType::Chunk, rid.clone());
        let mut end = Frame::end(rid.clone(), None);

        assigner.assign(&mut req);
        assigner.assign(&mut log);
        assigner.assign(&mut chunk);
        assigner.assign(&mut end);

        assert_eq!(req.seq, 0);
        assert_eq!(log.seq, 1);
        assert_eq!(chunk.seq, 2);
        assert_eq!(end.seq, 3);
    }

    // =========================================================================
    // FlowKey tests
    // =========================================================================

    // TEST447: FlowKey::from_frame extracts (rid, Some(xid)) when routing_id present
    #[test]
    fn test447_flow_key_with_xid() {
        let rid = MessageId::new_uuid();
        let xid = MessageId::new_uuid();
        let mut frame = Frame::new(FrameType::Chunk, rid.clone());
        frame.routing_id = Some(xid.clone());

        let key = FlowKey::from_frame(&frame);
        assert_eq!(key.rid, rid);
        assert_eq!(key.xid, Some(xid));
    }

    // TEST448: FlowKey::from_frame extracts (rid, None) when routing_id absent
    #[test]
    fn test448_flow_key_without_xid() {
        let rid = MessageId::new_uuid();
        let frame = Frame::new(FrameType::Req, rid.clone());

        let key = FlowKey::from_frame(&frame);
        assert_eq!(key.rid, rid);
        assert_eq!(key.xid, None);
    }

    // TEST449: FlowKey equality: same rid+xid equal, different xid different key
    #[test]
    fn test449_flow_key_equality() {
        let rid = MessageId::new_uuid();
        let xid1 = MessageId::new_uuid();
        let xid2 = MessageId::new_uuid();

        let key_with_xid1 = FlowKey {
            rid: rid.clone(),
            xid: Some(xid1.clone()),
        };
        let key_with_xid1_dup = FlowKey {
            rid: rid.clone(),
            xid: Some(xid1.clone()),
        };
        let key_with_xid2 = FlowKey {
            rid: rid.clone(),
            xid: Some(xid2),
        };
        let key_no_xid = FlowKey {
            rid: rid.clone(),
            xid: None,
        };

        assert_eq!(
            key_with_xid1, key_with_xid1_dup,
            "same rid+xid must be equal"
        );
        assert_ne!(
            key_with_xid1, key_with_xid2,
            "different xid must not be equal"
        );
        assert_ne!(
            key_with_xid1, key_no_xid,
            "Some(xid) vs None must not be equal"
        );
    }

    // TEST450: FlowKey hash: same keys hash equal (HashMap lookup)
    #[test]
    fn test450_flow_key_hash_lookup() {
        let rid = MessageId::new_uuid();
        let xid = MessageId::new_uuid();

        let key1 = FlowKey {
            rid: rid.clone(),
            xid: Some(xid.clone()),
        };
        let key2 = FlowKey {
            rid: rid.clone(),
            xid: Some(xid.clone()),
        };

        let mut map = std::collections::HashMap::new();
        map.insert(key1, 42);
        assert_eq!(
            map.get(&key2),
            Some(&42),
            "equal FlowKeys must hash to same bucket"
        );
    }

    // =========================================================================
    // ReorderBuffer tests
    // =========================================================================

    /// Helper: create a flow frame with a specific seq, rid, and optional xid
    fn make_flow_frame(rid: &MessageId, xid: Option<&MessageId>, seq: u64) -> Frame {
        let mut f = Frame::new(FrameType::Chunk, rid.clone());
        f.seq = seq;
        f.routing_id = xid.cloned();
        f
    }

    // TEST451: ReorderBuffer in-order delivery: seq 0,1,2 delivered immediately
    #[test]
    fn test451_reorder_buffer_in_order() {
        let mut buf = ReorderBuffer::new(64);
        let rid = MessageId::new_uuid();

        let ready0 = buf.accept(make_flow_frame(&rid, None, 0)).unwrap();
        assert_eq!(ready0.len(), 1);
        assert_eq!(ready0[0].seq, 0);

        let ready1 = buf.accept(make_flow_frame(&rid, None, 1)).unwrap();
        assert_eq!(ready1.len(), 1);
        assert_eq!(ready1[0].seq, 1);

        let ready2 = buf.accept(make_flow_frame(&rid, None, 2)).unwrap();
        assert_eq!(ready2.len(), 1);
        assert_eq!(ready2[0].seq, 2);
    }

    // TEST452: ReorderBuffer out-of-order: seq 1 then 0 delivers both in order
    #[test]
    fn test452_reorder_buffer_out_of_order() {
        let mut buf = ReorderBuffer::new(64);
        let rid = MessageId::new_uuid();

        let ready1 = buf.accept(make_flow_frame(&rid, None, 1)).unwrap();
        assert!(ready1.is_empty(), "seq 1 without seq 0 must be buffered");

        let ready0 = buf.accept(make_flow_frame(&rid, None, 0)).unwrap();
        assert_eq!(ready0.len(), 2);
        assert_eq!(ready0[0].seq, 0);
        assert_eq!(ready0[1].seq, 1);
    }

    // TEST453: ReorderBuffer gap fill: seq 0,2,1 delivers 0, buffers 2, then delivers 1+2
    #[test]
    fn test453_reorder_buffer_gap_fill() {
        let mut buf = ReorderBuffer::new(64);
        let rid = MessageId::new_uuid();

        let ready0 = buf.accept(make_flow_frame(&rid, None, 0)).unwrap();
        assert_eq!(ready0.len(), 1);

        let ready2 = buf.accept(make_flow_frame(&rid, None, 2)).unwrap();
        assert!(ready2.is_empty(), "seq 2 without seq 1 must be buffered");

        let ready1 = buf.accept(make_flow_frame(&rid, None, 1)).unwrap();
        assert_eq!(ready1.len(), 2);
        assert_eq!(ready1[0].seq, 1);
        assert_eq!(ready1[1].seq, 2);
    }

    // TEST454: ReorderBuffer stale seq is hard error
    #[test]
    fn test454_reorder_buffer_stale_seq() {
        let mut buf = ReorderBuffer::new(64);
        let rid = MessageId::new_uuid();

        buf.accept(make_flow_frame(&rid, None, 0)).unwrap();
        buf.accept(make_flow_frame(&rid, None, 1)).unwrap();

        let result = buf.accept(make_flow_frame(&rid, None, 0));
        assert!(result.is_err(), "stale seq must be protocol error");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("stale"),
            "error message must mention stale: {}",
            err
        );
    }

    // TEST455: ReorderBuffer overflow triggers protocol error
    #[test]
    fn test455_reorder_buffer_overflow() {
        let mut buf = ReorderBuffer::new(3); // tiny buffer
        let rid = MessageId::new_uuid();

        buf.accept(make_flow_frame(&rid, None, 1)).unwrap();
        buf.accept(make_flow_frame(&rid, None, 2)).unwrap();
        buf.accept(make_flow_frame(&rid, None, 3)).unwrap();

        let result = buf.accept(make_flow_frame(&rid, None, 4));
        assert!(result.is_err(), "buffer overflow must be protocol error");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("overflow"),
            "error message must mention overflow: {}",
            err
        );
    }

    // TEST456: Multiple concurrent flows reorder independently
    #[test]
    fn test456_reorder_buffer_independent_flows() {
        let mut buf = ReorderBuffer::new(64);
        let rid_a = MessageId::new_uuid();
        let rid_b = MessageId::new_uuid();

        let ready_a1 = buf.accept(make_flow_frame(&rid_a, None, 1)).unwrap();
        assert!(ready_a1.is_empty());

        let ready_b0 = buf.accept(make_flow_frame(&rid_b, None, 0)).unwrap();
        assert_eq!(ready_b0.len(), 1);
        assert_eq!(ready_b0[0].seq, 0);

        let ready_a0 = buf.accept(make_flow_frame(&rid_a, None, 0)).unwrap();
        assert_eq!(ready_a0.len(), 2);
        assert_eq!(ready_a0[0].seq, 0);
        assert_eq!(ready_a0[1].seq, 1);

        let ready_b1 = buf.accept(make_flow_frame(&rid_b, None, 1)).unwrap();
        assert_eq!(ready_b1.len(), 1);
    }

    // TEST457: cleanup_flow removes state; new frames start at seq 0
    #[test]
    fn test457_reorder_buffer_cleanup() {
        let mut buf = ReorderBuffer::new(64);
        let rid = MessageId::new_uuid();

        buf.accept(make_flow_frame(&rid, None, 0)).unwrap();
        buf.accept(make_flow_frame(&rid, None, 1)).unwrap();

        let key = FlowKey {
            rid: rid.clone(),
            xid: None,
        };
        buf.cleanup_flow(&key);

        let ready = buf.accept(make_flow_frame(&rid, None, 0)).unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].seq, 0);
    }

    // TEST458: Non-flow frames bypass reorder entirely
    #[test]
    fn test458_reorder_buffer_non_flow_bypass() {
        let mut buf = ReorderBuffer::new(64);

        let hello = Frame::new(FrameType::Hello, MessageId::Uint(0));
        let hb = Frame::new(FrameType::Heartbeat, MessageId::new_uuid());
        let notify = Frame::new(FrameType::RelayNotify, MessageId::Uint(0));
        let state = Frame::new(FrameType::RelayState, MessageId::Uint(0));

        assert_eq!(
            buf.accept(hello).unwrap().len(),
            1,
            "Hello must bypass reorder"
        );
        assert_eq!(
            buf.accept(hb).unwrap().len(),
            1,
            "Heartbeat must bypass reorder"
        );
        assert_eq!(
            buf.accept(notify).unwrap().len(),
            1,
            "RelayNotify must bypass reorder"
        );
        assert_eq!(
            buf.accept(state).unwrap().len(),
            1,
            "RelayState must bypass reorder"
        );
    }

    // TEST459: Terminal END frame flows through correctly
    #[test]
    fn test459_reorder_buffer_end_frame() {
        let mut buf = ReorderBuffer::new(64);
        let rid = MessageId::new_uuid();

        let mut req = Frame::new(FrameType::Req, rid.clone());
        req.seq = 0;
        buf.accept(req).unwrap();

        let mut end = Frame::end(rid.clone(), None);
        end.seq = 1;
        let ready = buf.accept(end).unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].frame_type, FrameType::End);
        assert_eq!(ready[0].seq, 1);
    }

    // TEST460: Terminal ERR frame flows through correctly
    #[test]
    fn test460_reorder_buffer_err_frame() {
        let mut buf = ReorderBuffer::new(64);
        let rid = MessageId::new_uuid();

        let mut req = Frame::new(FrameType::Req, rid.clone());
        req.seq = 0;
        buf.accept(req).unwrap();

        let mut err = Frame::err(
            rid.clone(),
            "TEST",
            crate::failure::AttributionClass::Internal,
            "test error",
            None,
        );
        err.seq = 1;
        let ready = buf.accept(err).unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].frame_type, FrameType::Err);
        assert_eq!(ready[0].seq, 1);
    }

    // =========================================================================
    // New Protocol Fields Tests (routing_id, chunk_index, chunk_count, checksum)
    // =========================================================================

    // TEST491: Frame::chunk constructor requires and sets chunk_index and checksum
    #[test]
    fn test491_chunk_requires_chunk_index_and_checksum() {
        let req_id = MessageId::new_uuid();
        let payload = b"test data".to_vec();
        let checksum = Frame::compute_checksum(&payload);

        let frame = Frame::chunk(
            req_id.clone(),
            "stream-1".to_string(),
            0,
            payload.clone(),
            5,
            checksum,
        );

        assert_eq!(frame.frame_type, FrameType::Chunk);
        assert_eq!(frame.chunk_index, Some(5), "chunk_index must be set");
        assert_eq!(frame.checksum, Some(checksum), "checksum must be set");
        assert_eq!(frame.payload, Some(payload));
    }

    // TEST492: Frame::stream_end constructor requires and sets chunk_count
    #[test]
    fn test492_stream_end_requires_chunk_count() {
        let req_id = MessageId::new_uuid();

        let frame = Frame::stream_end(req_id.clone(), "stream-1".to_string(), 42);

        assert_eq!(frame.frame_type, FrameType::StreamEnd);
        assert_eq!(frame.chunk_count, Some(42), "chunk_count must be set");
        assert_eq!(frame.stream_id, Some("stream-1".to_string()));
    }

    // TEST493: compute_checksum produces correct FNV-1a hash for known test vectors
    #[test]
    fn test493_compute_checksum_fnv1a_test_vectors() {
        // FNV-1a standard test vectors
        assert_eq!(
            Frame::compute_checksum(b""),
            0xcbf29ce484222325,
            "empty string hash"
        );
        assert_eq!(
            Frame::compute_checksum(b"a"),
            0xaf63dc4c8601ec8c,
            "single byte 'a'"
        );
        assert_eq!(
            Frame::compute_checksum(b"foobar"),
            0x85944171f73967e8,
            "foobar string"
        );
    }

    // TEST494: compute_checksum is deterministic
    #[test]
    fn test494_compute_checksum_deterministic() {
        let data = b"test data for hashing".to_vec();
        let hash1 = Frame::compute_checksum(&data);
        let hash2 = Frame::compute_checksum(&data);
        let hash3 = Frame::compute_checksum(&data);

        assert_eq!(hash1, hash2);
        assert_eq!(hash2, hash3);
    }

    // TEST495: CBOR decode REJECTS CHUNK frame missing chunk_index field
    #[test]
    fn test495_cbor_rejects_chunk_without_chunk_index() {
        use crate::bifaci::io::{decode_frame, encode_frame};

        let req_id = MessageId::new_uuid();
        let payload = b"data".to_vec();
        let checksum = Frame::compute_checksum(&payload);

        // Create frame with chunk_index, then remove it to simulate corruption
        let mut frame = Frame::new(FrameType::Chunk, req_id);
        frame.stream_id = Some("s1".to_string());
        frame.payload = Some(payload);
        frame.checksum = Some(checksum);
        // chunk_index deliberately missing

        let encoded = encode_frame(&frame).expect("encoding corrupted frame");

        // Decode should FAIL
        let result = decode_frame(&encoded);
        assert!(
            result.is_err(),
            "decode must reject CHUNK without chunk_index"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("chunk_index") || err.contains("CHUNK"),
            "error must mention missing chunk_index: {}",
            err
        );
    }

    // TEST496: CBOR decode REJECTS CHUNK frame missing checksum field
    #[test]
    fn test496_cbor_rejects_chunk_without_checksum() {
        use crate::bifaci::io::{decode_frame, encode_frame};

        let req_id = MessageId::new_uuid();
        let payload = b"data".to_vec();

        // Create frame without checksum (will fail decoding)
        let mut frame = Frame::new(FrameType::Chunk, req_id);
        frame.stream_id = Some("s1".to_string());
        frame.payload = Some(payload);
        frame.chunk_index = Some(0);
        // checksum deliberately missing

        let encoded = encode_frame(&frame).expect("encoding should succeed");

        // Decode should FAIL
        let result = decode_frame(&encoded);
        assert!(result.is_err(), "decode must reject CHUNK without checksum");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("checksum") || err.contains("CHUNK"),
            "error must mention missing checksum: {}",
            err
        );
    }

    // TEST6672: CBOR decode ACCEPTS STREAM_END without chunk_count — unbounded streams make no length promise (v4, L16)
    #[test]
    fn test6672_cbor_accepts_stream_end_without_chunk_count() {
        use crate::bifaci::io::{decode_frame, encode_frame};

        let req_id = MessageId::new_uuid();
        let frame = Frame::stream_end_unbounded(req_id.clone(), "s1".to_string());
        assert_eq!(frame.chunk_count, None);

        let encoded = encode_frame(&frame).expect("encoding should succeed");
        let decoded = decode_frame(&encoded).expect("v4 accepts STREAM_END without chunk_count");
        assert_eq!(decoded.frame_type, FrameType::StreamEnd);
        assert_eq!(decoded.id, req_id);
        assert_eq!(decoded.stream_id.as_deref(), Some("s1"));
        assert_eq!(
            decoded.chunk_count, None,
            "absent chunk_count must decode as None, not a default"
        );
    }

    // TEST7010: CREDIT frame round-trips encode/decode with rid, stream_id, and credit count
    #[test]
    fn test7010_credit_frame_roundtrip() {
        use crate::bifaci::io::{decode_frame, encode_frame};

        let rid = MessageId::new_uuid();
        let frame = Frame::credit(
            rid.clone(),
            Some("s1".to_string()),
            17,
            CreditDirection::Response,
        );
        assert_eq!(frame.credit_count(), Some(17));

        let decoded = decode_frame(&encode_frame(&frame).unwrap()).unwrap();
        assert_eq!(decoded.frame_type, FrameType::Credit);
        assert_eq!(decoded.id, rid);
        assert_eq!(decoded.stream_id.as_deref(), Some("s1"));
        assert_eq!(decoded.credit_count(), Some(17));
        assert_eq!(
            decoded.credit_direction(),
            Some(CreditDirection::Response),
            "the routing direction must survive the wire (L11)"
        );

        // Stream-less grant (request's sole stream) round-trips too
        let frame = Frame::credit(rid.clone(), None, 3, CreditDirection::Request);
        let decoded = decode_frame(&encode_frame(&frame).unwrap()).unwrap();
        assert_eq!(decoded.stream_id, None);
        assert_eq!(decoded.credit_count(), Some(3));
        assert_eq!(decoded.credit_direction(), Some(CreditDirection::Request));

        // A Credit frame with no direction reports None — hosts drop it as
        // unroutable (counted), since (xid, rid) alone cannot place it.
        let mut dirless = Frame::new(FrameType::Credit, MessageId::new_uuid());
        dirless.credit = Some(1);
        assert_eq!(dirless.credit_direction(), None);

        // credit_count is None on non-Credit frames even if the field is set
        let mut chunkish = Frame::new(FrameType::Log, rid);
        chunkish.credit = Some(9);
        assert_eq!(chunkish.credit_count(), None);
    }

    // TEST7011: CREDIT is a non-flow frame — no seq assigned, passes the reorder buffer untouched regardless of flow state
    #[test]
    fn test7011_credit_is_non_flow() {
        let rid = MessageId::new_uuid();

        // SeqAssigner leaves Credit at seq 0 while flow frames advance
        let mut assigner = SeqAssigner::new();
        let mut chunk = Frame::chunk(rid.clone(), "s1".to_string(), 0, vec![1], 0, 1);
        assigner.assign(&mut chunk);
        assert_eq!(chunk.seq, 0);
        let mut credit = Frame::credit(
            rid.clone(),
            Some("s1".to_string()),
            4,
            CreditDirection::Response,
        );
        assigner.assign(&mut credit);
        assert_eq!(credit.seq, 0, "Credit must not consume a flow seq");
        let mut chunk2 = Frame::chunk(rid.clone(), "s1".to_string(), 0, vec![2], 1, 1);
        assigner.assign(&mut chunk2);
        assert_eq!(chunk2.seq, 1, "flow seq must be contiguous across a Credit");

        // ReorderBuffer returns Credit immediately even while the flow is gapped
        let mut buffer = ReorderBuffer::new(8);
        let mut gapped = Frame::chunk(rid.clone(), "s1".to_string(), 5, vec![3], 5, 1);
        gapped.seq = 5;
        assert!(
            buffer.accept(gapped).unwrap().is_empty(),
            "out-of-order flow frame must be buffered"
        );
        let credit = Frame::credit(rid, Some("s1".to_string()), 4, CreditDirection::Response);
        let delivered = buffer.accept(credit).unwrap();
        assert_eq!(
            delivered.len(),
            1,
            "Credit must bypass the reorder buffer and deliver immediately"
        );
        assert_eq!(delivered[0].frame_type, FrameType::Credit);
    }

    // TEST7012: STREAM_START unbounded flag round-trips through CBOR; absent flag means bounded
    #[test]
    fn test7012_stream_start_unbounded_roundtrip() {
        use crate::bifaci::io::{decode_frame, encode_frame};

        let rid = MessageId::new_uuid();
        let bounded = Frame::stream_start(
            rid.clone(),
            "s1".to_string(),
            "media:enc=utf-8".to_string(),
            Some(false),
        );
        assert!(!bounded.is_unbounded());
        let decoded = decode_frame(&encode_frame(&bounded).unwrap()).unwrap();
        assert!(!decoded.is_unbounded(), "absent flag must read as bounded");
        assert_eq!(decoded.unbounded, None, "bounded frames omit the key");

        let unbounded = Frame::stream_start_unbounded(
            rid,
            "s2".to_string(),
            "media:enc=utf-8".to_string(),
            Some(true),
        );
        assert!(unbounded.is_unbounded());
        let decoded = decode_frame(&encode_frame(&unbounded).unwrap()).unwrap();
        assert!(decoded.is_unbounded());
        assert_eq!(decoded.stream_id.as_deref(), Some("s2"));
        assert_eq!(decoded.is_sequence, Some(true));
    }

    // TEST7013: CBOR decode REJECTS a CREDIT frame missing its credit count
    #[test]
    fn test7013_cbor_rejects_credit_without_count() {
        use crate::bifaci::io::{decode_frame, encode_frame};

        let mut frame = Frame::new(FrameType::Credit, MessageId::new_uuid());
        frame.stream_id = Some("s1".to_string());
        // credit deliberately missing

        let encoded = encode_frame(&frame).expect("encoding should succeed");
        let result = decode_frame(&encoded);
        assert!(
            result.is_err(),
            "decode must reject CREDIT without a credit count"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("credit") || err.contains("CREDIT"),
            "error must name the missing field: {}",
            err
        );
    }

    // TEST7026: An out-of-order terminal is buffered until the gap fills; buffered pre-terminal frames flush ahead of it in seq order, and only then may the flow be cleaned up
    #[test]
    fn test7026_reorder_flushes_pre_terminal_before_cleanup() {
        let rid = MessageId::new_uuid();
        let mut buffer = ReorderBuffer::new(8);

        let mk = |seq: u64, ftype: FrameType| -> Frame {
            let mut f = Frame::new(ftype, rid.clone());
            f.seq = seq;
            f
        };

        // seq 0 delivers immediately.
        let delivered = buffer.accept(mk(0, FrameType::Chunk)).unwrap();
        assert_eq!(delivered.len(), 1);

        // seq 2 (chunk) and seq 3 (END) arrive out of order — both buffered,
        // nothing delivered, no premature cleanup possible.
        assert!(buffer.accept(mk(2, FrameType::Chunk)).unwrap().is_empty());
        assert!(buffer.accept(mk(3, FrameType::End)).unwrap().is_empty());

        // The gap fills: seq 1 arrives → 1, 2, 3(END) all deliver in order.
        // The terminal is DELIVERED strictly after every pre-terminal frame.
        let delivered = buffer.accept(mk(1, FrameType::Chunk)).unwrap();
        let seqs: Vec<u64> = delivered.iter().map(|f| f.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
        assert_eq!(delivered.last().unwrap().frame_type, FrameType::End);

        // Cleanup after delivered terminal (as the relay does, post-drain):
        // the flow state resets and a fresh flow under the same key starts
        // cleanly at seq 0.
        buffer.cleanup_flow(&FlowKey::from_frame(&delivered[2]));
        let fresh = buffer.accept(mk(0, FrameType::Chunk)).unwrap();
        assert_eq!(fresh.len(), 1, "cleaned flow accepts a fresh seq 0");
    }

    // TEST7014: END terminal meta (progress, message) round-trips; successful END without progress reads as 1.0; failed END without progress reads as None
    #[test]
    fn test7014_end_terminal_meta_roundtrip() {
        use crate::bifaci::io::{decode_frame, encode_frame};

        let rid = MessageId::new_uuid();

        // Explicit terminal progress + message round-trip
        let end = Frame::end_ok_with(rid.clone(), None, Some(0.87), Some("partial corpus"));
        let decoded = decode_frame(&encode_frame(&end).unwrap()).unwrap();
        assert_eq!(decoded.final_progress(), Some(0.87));
        assert_eq!(decoded.final_message(), Some("partial corpus"));
        assert_eq!(decoded.exit_code(), Some(0), "end_ok_with implies success");

        // Successful END with no explicit progress reads as 1.0
        let end = Frame::end_ok(rid.clone(), None);
        let decoded = decode_frame(&encode_frame(&end).unwrap()).unwrap();
        assert_eq!(decoded.final_progress(), Some(1.0));
        assert_eq!(decoded.final_message(), None);

        // Non-successful END (no exit_code) with no explicit progress: None —
        // failure must not synthesize a completion value.
        let end = Frame::end(rid.clone(), None);
        let decoded = decode_frame(&encode_frame(&end).unwrap()).unwrap();
        assert_eq!(decoded.final_progress(), None);

        // Non-END frames never report a final progress
        let log = Frame::progress(rid, 0.5, "halfway");
        assert_eq!(log.final_progress(), None);
    }

    // TEST498: routing_id field roundtrips through CBOR encoding
    #[test]
    fn test498_routing_id_cbor_roundtrip() {
        use crate::bifaci::io::{decode_frame, encode_frame};

        let req_id = MessageId::new_uuid();
        let routing_id = MessageId::new_uuid();

        let mut frame = Frame::req(
            req_id.clone(),
            r#"cap:in="media:void";test;out="media:void""#,
            vec![],
            "text/plain",
        );
        frame.routing_id = Some(routing_id.clone());

        let encoded = encode_frame(&frame).expect("encoding should succeed");
        let decoded = decode_frame(&encoded).expect("decoding should succeed");

        assert_eq!(
            decoded.routing_id,
            Some(routing_id),
            "routing_id must roundtrip"
        );
        assert_eq!(decoded.id, req_id);
    }

    // TEST499: chunk_index and checksum roundtrip through CBOR encoding
    #[test]
    fn test499_chunk_index_checksum_cbor_roundtrip() {
        use crate::bifaci::io::{decode_frame, encode_frame};

        let req_id = MessageId::new_uuid();
        let payload = b"test payload".to_vec();
        let checksum = Frame::compute_checksum(&payload);

        let frame = Frame::chunk(
            req_id.clone(),
            "s1".to_string(),
            0,
            payload.clone(),
            7,
            checksum,
        );

        let encoded = encode_frame(&frame).expect("encoding should succeed");
        let decoded = decode_frame(&encoded).expect("decoding should succeed");

        assert_eq!(decoded.chunk_index, Some(7), "chunk_index must roundtrip");
        assert_eq!(decoded.checksum, Some(checksum), "checksum must roundtrip");
        assert_eq!(decoded.payload, Some(payload));
    }

    // TEST500: chunk_count roundtrips through CBOR encoding
    #[test]
    fn test500_chunk_count_cbor_roundtrip() {
        use crate::bifaci::io::{decode_frame, encode_frame};

        let req_id = MessageId::new_uuid();

        let frame = Frame::stream_end(req_id.clone(), "s1".to_string(), 42);

        let encoded = encode_frame(&frame).expect("encoding should succeed");
        let decoded = decode_frame(&encoded).expect("decoding should succeed");

        assert_eq!(decoded.chunk_count, Some(42), "chunk_count must roundtrip");
        assert_eq!(decoded.stream_id, Some("s1".to_string()));
    }

    // TEST501: Frame::new initializes new fields to None
    #[test]
    fn test501_frame_new_initializes_optional_fields_none() {
        let frame = Frame::new(FrameType::Req, MessageId::new_uuid());

        assert_eq!(frame.routing_id, None);
        assert_eq!(frame.chunk_index, None);
        assert_eq!(frame.chunk_count, None);
        assert_eq!(frame.checksum, None);
    }

    // TEST502: Keys module has constants for new fields
    #[test]
    fn test502_keys_module_new_field_constants() {
        assert_eq!(keys::ROUTING_ID, 13);
        assert_eq!(keys::INDEX, 14);
        assert_eq!(keys::CHUNK_COUNT, 15);
        assert_eq!(keys::CHECKSUM, 16);
        assert_eq!(keys::IS_SEQUENCE, 17);
    }

    // TEST503: compute_checksum handles empty data correctly
    #[test]
    fn test503_compute_checksum_empty_data() {
        let hash = Frame::compute_checksum(b"");
        assert_eq!(
            hash, 0xcbf29ce484222325,
            "empty data should produce FNV offset basis"
        );
    }

    // TEST504: compute_checksum handles large payloads without overflow
    #[test]
    fn test504_compute_checksum_large_payload() {
        let large_data = vec![0xAA; 1_000_000];
        let hash = Frame::compute_checksum(&large_data);
        assert_ne!(hash, 0, "large payload should produce non-zero hash");

        // Verify determinism with large data
        let hash2 = Frame::compute_checksum(&large_data);
        assert_eq!(hash, hash2, "large payload hash must be deterministic");
    }

    // TEST505: chunk_with_offset sets chunk_index correctly
    #[test]
    fn test505_chunk_with_offset_sets_chunk_index() {
        let req_id = MessageId::new_uuid();
        let payload = b"data".to_vec();
        let checksum = Frame::compute_checksum(&payload);

        let frame = Frame::chunk_with_offset(
            req_id,
            "s1".to_string(),
            0, // seq
            payload,
            1024,        // offset
            Some(10000), // total_len
            false,       // is_last
            5,           // chunk_index
            checksum,
        );

        assert_eq!(frame.chunk_index, Some(5), "chunk_index must be set");
        assert_eq!(frame.checksum, Some(checksum), "checksum must be set");
        assert_eq!(frame.offset, Some(1024));
    }

    // TEST506: Different data produces different checksums
    #[test]
    fn test506_compute_checksum_different_data_different_hash() {
        let data1 = b"hello".to_vec();
        let data2 = b"world".to_vec();

        let hash1 = Frame::compute_checksum(&data1);
        let hash2 = Frame::compute_checksum(&data2);

        assert_ne!(hash1, hash2, "different data must produce different hashes");
    }

    // =========================================================================
    // ReorderBuffer Advanced Edge Cases
    // =========================================================================

    // TEST507: ReorderBuffer isolates flows by XID (routing_id) - same RID different XIDs
    #[test]
    fn test507_reorder_buffer_xid_isolation() {
        let mut buf = ReorderBuffer::new(64);
        let rid = MessageId::new_uuid();
        let xid_a = MessageId::new_uuid();
        let xid_b = MessageId::new_uuid();

        // Flow A (rid, xid_a): receive seq 1 first
        let ready_a1 = buf.accept(make_flow_frame(&rid, Some(&xid_a), 1)).unwrap();
        assert!(ready_a1.is_empty(), "xid_a seq 1 buffered");

        // Flow B (rid, xid_b): receive seq 0 (different flow, should deliver immediately)
        let ready_b0 = buf.accept(make_flow_frame(&rid, Some(&xid_b), 0)).unwrap();
        assert_eq!(ready_b0.len(), 1, "xid_b seq 0 delivers immediately");
        assert_eq!(ready_b0[0].seq, 0);

        // Flow A: receive seq 0, should deliver 0+1
        let ready_a0 = buf.accept(make_flow_frame(&rid, Some(&xid_a), 0)).unwrap();
        assert_eq!(ready_a0.len(), 2, "xid_a delivers 0 and buffered 1");
        assert_eq!(ready_a0[0].seq, 0);
        assert_eq!(ready_a0[1].seq, 1);

        // Verify both flows are independent
        let ready_b1 = buf.accept(make_flow_frame(&rid, Some(&xid_b), 1)).unwrap();
        assert_eq!(ready_b1.len(), 1);
    }

    // TEST508: ReorderBuffer rejects duplicate seq already in buffer
    #[test]
    fn test508_reorder_buffer_duplicate_buffered_seq() {
        let mut buf = ReorderBuffer::new(64);
        let rid = MessageId::new_uuid();

        // Buffer seq 1 (waiting for seq 0)
        buf.accept(make_flow_frame(&rid, None, 1)).unwrap();

        // Try to buffer seq 1 again - this is a duplicate
        let result = buf.accept(make_flow_frame(&rid, None, 1));
        assert!(result.is_err(), "duplicate buffered seq must fail");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("stale") || err.contains("duplicate"),
            "error must mention stale/duplicate: {}",
            err
        );
    }

    // TEST509: ReorderBuffer handles large seq gaps without DOS
    #[test]
    fn test509_reorder_buffer_large_gap_rejected() {
        let mut buf = ReorderBuffer::new(64);
        let rid = MessageId::new_uuid();

        buf.accept(make_flow_frame(&rid, None, 0)).unwrap();

        // Try to send seq 1000 - huge gap that would require buffering 999 frames
        // This should fail because we can't buffer that many frames
        buf.accept(make_flow_frame(&rid, None, 2)).unwrap(); // buffer 1 frame
        buf.accept(make_flow_frame(&rid, None, 3)).unwrap(); // buffer 2 frames

        // Keep adding until we hit the limit
        for seq in 4..=65 {
            buf.accept(make_flow_frame(&rid, None, seq)).unwrap();
        }

        // This should overflow the buffer
        let result = buf.accept(make_flow_frame(&rid, None, 66));
        assert!(
            result.is_err(),
            "large gap causing buffer overflow must fail"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("overflow"),
            "error must mention overflow: {}",
            err
        );
    }

    // TEST510: ReorderBuffer with multiple interleaved gaps fills correctly
    #[test]
    fn test510_reorder_buffer_multiple_gaps() {
        let mut buf = ReorderBuffer::new(64);
        let rid = MessageId::new_uuid();

        // Send: 0, 3, 5, then fill the gaps
        let ready0 = buf.accept(make_flow_frame(&rid, None, 0)).unwrap();
        assert_eq!(ready0.len(), 1);

        let ready3 = buf.accept(make_flow_frame(&rid, None, 3)).unwrap();
        assert!(ready3.is_empty(), "seq 3 buffered");

        let ready5 = buf.accept(make_flow_frame(&rid, None, 5)).unwrap();
        assert!(ready5.is_empty(), "seq 5 buffered");

        // Fill gap with seq 1
        let ready1 = buf.accept(make_flow_frame(&rid, None, 1)).unwrap();
        assert_eq!(ready1.len(), 1, "only seq 1 delivered, still missing 2");

        // Fill gap with seq 2 - should deliver 2, 3 (but not 5, still missing 4)
        let ready2 = buf.accept(make_flow_frame(&rid, None, 2)).unwrap();
        assert_eq!(ready2.len(), 2, "delivers 2 and 3");
        assert_eq!(ready2[0].seq, 2);
        assert_eq!(ready2[1].seq, 3);

        // Fill final gap with seq 4 - should deliver 4, 5
        let ready4 = buf.accept(make_flow_frame(&rid, None, 4)).unwrap();
        assert_eq!(ready4.len(), 2, "delivers 4 and 5");
        assert_eq!(ready4[0].seq, 4);
        assert_eq!(ready4[1].seq, 5);
    }

    // TEST511: ReorderBuffer cleanup with buffered frames discards them
    #[test]
    fn test511_reorder_buffer_cleanup_with_buffered_frames() {
        let mut buf = ReorderBuffer::new(64);
        let rid = MessageId::new_uuid();

        buf.accept(make_flow_frame(&rid, None, 0)).unwrap();
        buf.accept(make_flow_frame(&rid, None, 2)).unwrap(); // buffered
        buf.accept(make_flow_frame(&rid, None, 3)).unwrap(); // buffered

        let key = FlowKey {
            rid: rid.clone(),
            xid: None,
        };
        buf.cleanup_flow(&key);

        // After cleanup, seq 0 should work again (flow reset)
        let ready = buf.accept(make_flow_frame(&rid, None, 0)).unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].seq, 0);

        // And buffered frames 2,3 were discarded (seq 1 is now expected)
        let ready1 = buf.accept(make_flow_frame(&rid, None, 1)).unwrap();
        assert_eq!(ready1.len(), 1);
    }

    // TEST512: ReorderBuffer delivers burst of consecutive buffered frames
    #[test]
    fn test512_reorder_buffer_burst_delivery() {
        let mut buf = ReorderBuffer::new(64);
        let rid = MessageId::new_uuid();

        // Buffer seq 1-10 (all waiting for seq 0)
        for seq in 1..=10 {
            let ready = buf.accept(make_flow_frame(&rid, None, seq)).unwrap();
            assert!(ready.is_empty(), "seq {} buffered", seq);
        }

        // Now send seq 0 - should deliver all 11 frames at once
        let ready = buf.accept(make_flow_frame(&rid, None, 0)).unwrap();
        assert_eq!(ready.len(), 11, "delivers seq 0 plus 10 buffered frames");
        for (i, frame) in ready.iter().enumerate() {
            assert_eq!(frame.seq, i as u64, "frame {} has correct seq", i);
        }
    }

    // TEST513: ReorderBuffer different frame types in same flow maintain order
    #[test]
    fn test513_reorder_buffer_mixed_types_same_flow() {
        let mut buf = ReorderBuffer::new(64);
        let rid = MessageId::new_uuid();

        // Create different frame types with same RID
        let mut req = Frame::new(FrameType::Req, rid.clone());
        req.seq = 1;
        let mut log = Frame::new(FrameType::Log, rid.clone());
        log.seq = 2;
        let mut chunk = Frame::new(FrameType::Chunk, rid.clone());
        chunk.seq = 0;

        // Send out of order: REQ(1), LOG(2), then CHUNK(0)
        buf.accept(req).unwrap(); // buffered
        buf.accept(log).unwrap(); // buffered

        let ready = buf.accept(chunk).unwrap();
        assert_eq!(ready.len(), 3, "all three frames delivered in order");
        assert_eq!(ready[0].frame_type, FrameType::Chunk);
        assert_eq!(ready[1].frame_type, FrameType::Req);
        assert_eq!(ready[2].frame_type, FrameType::Log);
    }

    // TEST514: ReorderBuffer with XID cleanup doesn't affect different XID
    #[test]
    fn test514_reorder_buffer_xid_cleanup_isolation() {
        let mut buf = ReorderBuffer::new(64);
        let rid = MessageId::new_uuid();
        let xid_a = MessageId::new_uuid();
        let xid_b = MessageId::new_uuid();

        buf.accept(make_flow_frame(&rid, Some(&xid_a), 0)).unwrap();
        buf.accept(make_flow_frame(&rid, Some(&xid_b), 0)).unwrap();

        // Cleanup flow A
        let key_a = FlowKey {
            rid: rid.clone(),
            xid: Some(xid_a.clone()),
        };
        buf.cleanup_flow(&key_a);

        // Flow B should still expect seq 1
        let ready = buf.accept(make_flow_frame(&rid, Some(&xid_b), 1)).unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].seq, 1);

        // Flow A was reset, seq 0 works again
        let ready_a = buf.accept(make_flow_frame(&rid, Some(&xid_a), 0)).unwrap();
        assert_eq!(ready_a.len(), 1);
    }

    // TEST515: ReorderBuffer overflow error includes diagnostic information
    #[test]
    fn test515_reorder_buffer_overflow_error_details() {
        let max_buffer = 3;
        let mut buf = ReorderBuffer::new(max_buffer);
        let rid = MessageId::new_uuid();

        // Fill buffer to capacity
        for seq in 1..=3 {
            buf.accept(make_flow_frame(&rid, None, seq)).unwrap();
        }

        // Overflow
        let result = buf.accept(make_flow_frame(&rid, None, 4));
        assert!(result.is_err());

        let err = result.unwrap_err().to_string();
        assert!(err.contains("overflow"), "must mention overflow");
        assert!(
            err.contains(&max_buffer.to_string()),
            "must include max buffer size"
        );
        assert!(err.contains("expected seq 0"), "must show expected seq");
        assert!(err.contains("got seq 4"), "must show actual seq");
    }

    // TEST516: ReorderBuffer stale error includes diagnostic information
    #[test]
    fn test516_reorder_buffer_stale_error_details() {
        let mut buf = ReorderBuffer::new(64);
        let rid = MessageId::new_uuid();

        buf.accept(make_flow_frame(&rid, None, 0)).unwrap();
        buf.accept(make_flow_frame(&rid, None, 1)).unwrap();
        buf.accept(make_flow_frame(&rid, None, 2)).unwrap();

        // Send stale seq 1
        let result = buf.accept(make_flow_frame(&rid, None, 1));
        assert!(result.is_err());

        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("stale") || err.contains("duplicate"),
            "must mention stale/duplicate"
        );
        assert!(err.contains("expected >= 3"), "must show expected seq");
        assert!(err.contains("got 1"), "must show actual seq");
    }

    // TEST517: FlowKey with None XID differs from Some(xid)
    #[test]
    fn test517_flow_key_none_vs_some_xid() {
        let rid = MessageId::new_uuid();
        let xid = MessageId::new_uuid();

        let key_none = FlowKey {
            rid: rid.clone(),
            xid: None,
        };
        let key_some = FlowKey {
            rid: rid.clone(),
            xid: Some(xid.clone()),
        };

        assert_ne!(key_none, key_some, "None XID must differ from Some(xid)");

        // Hash equality check (for HashMap)
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher1 = DefaultHasher::new();
        key_none.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        key_some.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        assert_ne!(hash1, hash2, "different XID states must hash differently");
    }

    // TEST518: ReorderBuffer handles zero-length ready vec correctly
    #[test]
    fn test518_reorder_buffer_empty_ready_vec() {
        let mut buf = ReorderBuffer::new(64);
        let rid = MessageId::new_uuid();

        // Send seq 1 first - should return empty vec (buffered)
        let ready = buf.accept(make_flow_frame(&rid, None, 1)).unwrap();
        assert!(ready.is_empty(), "buffered frame returns empty vec");
        assert_eq!(ready.len(), 0, "explicit length check");
    }

    // TEST519: ReorderBuffer state persists across accept calls
    #[test]
    fn test519_reorder_buffer_state_persistence() {
        let mut buf = ReorderBuffer::new(64);
        let rid = MessageId::new_uuid();

        // First call: establish expected_seq = 0, buffer seq 2
        buf.accept(make_flow_frame(&rid, None, 2)).unwrap();

        // Second call: send seq 1, should still be buffered (missing 0)
        let ready = buf.accept(make_flow_frame(&rid, None, 1)).unwrap();
        assert!(ready.is_empty(), "seq 1 buffered, still waiting for seq 0");

        // Third call: send seq 0, should deliver 0, 1, 2
        let ready = buf.accept(make_flow_frame(&rid, None, 0)).unwrap();
        assert_eq!(ready.len(), 3, "state persisted correctly");
    }

    // TEST520: ReorderBuffer max_buffer_per_flow is per-flow not global
    #[test]
    fn test520_reorder_buffer_per_flow_limit() {
        let mut buf = ReorderBuffer::new(2); // max 2 buffered per flow
        let rid_a = MessageId::new_uuid();
        let rid_b = MessageId::new_uuid();

        // Flow A: buffer 2 frames (at limit)
        buf.accept(make_flow_frame(&rid_a, None, 1)).unwrap();
        buf.accept(make_flow_frame(&rid_a, None, 2)).unwrap();

        // Flow B: can still buffer 2 frames (separate limit)
        buf.accept(make_flow_frame(&rid_b, None, 1)).unwrap();
        buf.accept(make_flow_frame(&rid_b, None, 2)).unwrap();

        // Flow A: overflow
        let result = buf.accept(make_flow_frame(&rid_a, None, 3));
        assert!(result.is_err(), "flow A overflows");

        // Flow B: also overflow
        let result = buf.accept(make_flow_frame(&rid_b, None, 3));
        assert!(result.is_err(), "flow B also overflows");
    }

    // =========================================================================
    // Relay Frame Types - Comprehensive Tests
    // =========================================================================

    // TEST521: RelayNotify CBOR roundtrip preserves manifest and limits
    #[test]
    fn test521_relay_notify_cbor_roundtrip() {
        use crate::bifaci::io::{decode_frame, encode_frame};

        let manifest = br#"{"name":"Test","version":"1.0","channel":"release","description":"Test","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=\"media:void\";convert;out=\"media:image\"","title":"Convert","aliases": ["convert"]}]}]}"#;
        let limits = Limits {
            max_frame: 3_000_000,
            max_chunk: 256_000,
            max_reorder_buffer: 128,
            initial_credit: DEFAULT_INITIAL_CREDIT,
        };

        let frame = Frame::relay_notify(manifest, &limits);
        let encoded = encode_frame(&frame).expect("encoding must succeed");
        let decoded = decode_frame(&encoded).expect("decoding must succeed");

        assert_eq!(decoded.frame_type, FrameType::RelayNotify);
        assert_eq!(
            decoded.relay_notify_manifest(),
            Some(manifest.as_slice()),
            "manifest must roundtrip"
        );

        let decoded_limits = decoded
            .relay_notify_limits()
            .expect("limits must be present");
        assert_eq!(
            decoded_limits.max_frame, 3_000_000,
            "max_frame must roundtrip"
        );
        assert_eq!(
            decoded_limits.max_chunk, 256_000,
            "max_chunk must roundtrip"
        );
        assert_eq!(
            decoded_limits.max_reorder_buffer, 128,
            "max_reorder_buffer must roundtrip"
        );
    }

    // TEST522: RelayState CBOR roundtrip preserves payload
    #[test]
    fn test522_relay_state_cbor_roundtrip() {
        use crate::bifaci::io::{decode_frame, encode_frame};

        let state_data = br#"{"memory_mb":8192,"cpu_cores":16,"active_flows":42}"#;
        let frame = Frame::relay_state(state_data);

        let encoded = encode_frame(&frame).expect("encoding must succeed");
        let decoded = decode_frame(&encoded).expect("decoding must succeed");

        assert_eq!(decoded.frame_type, FrameType::RelayState);
        assert_eq!(
            decoded.payload,
            Some(state_data.to_vec()),
            "state payload must roundtrip exactly"
        );
        assert_eq!(decoded.id, MessageId::Uint(0));
    }

    // TEST523: is_flow_frame returns false for RelayNotify
    #[test]
    fn test523_relay_notify_not_flow_frame() {
        let manifest = b"test";
        let limits = Limits::default();
        let frame = Frame::relay_notify(manifest, &limits);

        assert!(
            !frame.is_flow_frame(),
            "RelayNotify must not be a flow frame (bypasses reordering)"
        );
    }

    // TEST524: is_flow_frame returns false for RelayState
    #[test]
    fn test524_relay_state_not_flow_frame() {
        let state = b"test";
        let frame = Frame::relay_state(state);

        assert!(
            !frame.is_flow_frame(),
            "RelayState must not be a flow frame (bypasses reordering)"
        );
    }

    // TEST525: RelayNotify with empty manifest is valid
    #[test]
    fn test525_relay_notify_empty_manifest() {
        let empty_manifest = b"";
        let limits = Limits::default();
        let frame = Frame::relay_notify(empty_manifest, &limits);

        assert_eq!(frame.frame_type, FrameType::RelayNotify);
        assert_eq!(
            frame.relay_notify_manifest(),
            Some(empty_manifest.as_slice())
        );
    }

    // TEST526: RelayState with empty payload is valid
    #[test]
    fn test526_relay_state_empty_payload() {
        let empty_state = b"";
        let frame = Frame::relay_state(empty_state);

        assert_eq!(frame.frame_type, FrameType::RelayState);
        assert_eq!(frame.payload, Some(vec![]));
    }

    // TEST527: RelayNotify with large manifest roundtrips correctly
    #[test]
    fn test527_relay_notify_large_manifest() {
        use crate::bifaci::io::{decode_frame, encode_frame};

        // Create a large manifest (simulating many caps)
        let mut large_manifest = String::from(
            r#"{"name":"Large","version":"1.0","channel":"release","description":"Large test","cap_groups":[{"name":"default","caps":["#,
        );
        // First cap is identity (required)
        large_manifest
            .push_str(r#"{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"]}"#);
        for i in 0..99 {
            large_manifest.push_str(&format!(
                r#",{{"urn":"cap:in=\"media:void\";op{i};out=\"media:void\"","title":"Op{i}","aliases":["op{i}"]}}"#,
            ));
        }
        large_manifest.push_str("]}]}");

        let limits = Limits::default();
        let frame = Frame::relay_notify(large_manifest.as_bytes(), &limits);

        let encoded = encode_frame(&frame).expect("large manifest must encode");
        let decoded = decode_frame(&encoded).expect("large manifest must decode");

        assert_eq!(
            decoded.relay_notify_manifest(),
            Some(large_manifest.as_bytes())
        );
    }

    // TEST528: RelayNotify and RelayState use MessageId::Uint(0)
    #[test]
    fn test528_relay_frames_use_uint_zero_id() {
        let notify = Frame::relay_notify(b"test", &Limits::default());
        let state = Frame::relay_state(b"test");

        assert_eq!(
            notify.id,
            MessageId::Uint(0),
            "RelayNotify must use Uint(0) as sentinel ID"
        );
        assert_eq!(
            state.id,
            MessageId::Uint(0),
            "RelayState must use Uint(0) as sentinel ID"
        );

        // Verify they're not UUIDs
        assert!(notify.id.to_uuid_string().is_none());
        assert!(state.id.to_uuid_string().is_none());
    }

    // TEST667: verify_chunk_checksum detects corrupted payload
    #[test]
    fn test667_verify_chunk_checksum_detects_corruption() {
        let id = MessageId::new_uuid();
        let stream_id = "stream-test".to_string();
        let payload = b"original payload data".to_vec();
        let checksum = Frame::compute_checksum(&payload);

        // Create valid chunk frame
        let mut frame = Frame::chunk(id, stream_id, 0, payload.clone(), 0, checksum);

        // Valid frame should pass verification
        let expected = Frame::compute_checksum(frame.payload.as_ref().unwrap());
        assert_eq!(
            frame.checksum,
            Some(expected),
            "Valid frame should pass verification"
        );

        // Corrupt the payload (simulate transmission error)
        frame.payload = Some(b"corrupted payload!!".to_vec());

        // Corrupted frame should fail verification
        let expected = Frame::compute_checksum(frame.payload.as_ref().unwrap());
        assert_ne!(
            frame.checksum,
            Some(expected),
            "Corrupted frame should have mismatched checksum"
        );

        // Missing checksum should fail
        frame.checksum = None;
        assert!(
            frame.checksum.is_none(),
            "Frame without checksum should fail verification"
        );
    }
}
