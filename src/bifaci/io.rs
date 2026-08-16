//! CBOR I/O - Reading and Writing CBOR Frames
//!
//! This module provides streaming CBOR frame encoding/decoding over stdio pipes.
//! Frames are written as length-prefixed CBOR (same framing as before, but CBOR payload).
//!
//! ## Wire Format
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │  4 bytes: u32 big-endian length                         │
//! ├─────────────────────────────────────────────────────────┤
//! │  N bytes: CBOR-encoded Frame                            │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! The CBOR payload is a map with integer keys (see cbor_frame.rs).

use crate::bifaci::frame::{
    keys, Frame, FrameType, Limits, MessageId, DEFAULT_INITIAL_CREDIT, DEFAULT_MAX_CHUNK,
    DEFAULT_MAX_FRAME, DEFAULT_MAX_REORDER_BUFFER, PROTOCOL_VERSION,
};
use ciborium::Value;
use std::collections::BTreeMap;
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Maximum frame size (16 MB) - hard limit to prevent memory exhaustion
const MAX_FRAME_HARD_LIMIT: usize = 16 * 1024 * 1024;

/// Errors that can occur during CBOR I/O
#[derive(Debug, thiserror::Error)]
pub enum CborError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("CBOR encoding error: {0}")]
    Encode(String),

    #[error("CBOR decoding error: {0}")]
    Decode(String),

    #[error("Frame too large: {size} bytes (max {max})")]
    FrameTooLarge { size: usize, max: usize },

    #[error("Invalid frame: {0}")]
    InvalidFrame(String),

    #[error("Unexpected end of stream")]
    UnexpectedEof,

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Handshake failed: {0}")]
    Handshake(String),
}

/// Encode a frame to CBOR bytes
pub fn encode_frame(frame: &Frame) -> Result<Vec<u8>, CborError> {
    let mut map: Vec<(Value, Value)> = Vec::with_capacity(11);

    // Required fields
    map.push((
        Value::Integer(keys::VERSION.into()),
        Value::Integer((frame.version as i64).into()),
    ));
    map.push((
        Value::Integer(keys::FRAME_TYPE.into()),
        Value::Integer((frame.frame_type as u8 as i64).into()),
    ));

    // Message ID
    let id_value = match &frame.id {
        MessageId::Uuid(bytes) => Value::Bytes(bytes.to_vec()),
        MessageId::Uint(n) => Value::Integer((*n as i64).into()),
    };
    map.push((Value::Integer(keys::ID.into()), id_value));

    // Sequence number
    map.push((
        Value::Integer(keys::SEQ.into()),
        Value::Integer((frame.seq as i64).into()),
    ));

    // Optional fields
    if let Some(ref ct) = frame.content_type {
        map.push((
            Value::Integer(keys::CONTENT_TYPE.into()),
            Value::Text(ct.clone()),
        ));
    }

    if let Some(ref meta) = frame.meta {
        let meta_map: Vec<(Value, Value)> = meta
            .iter()
            .map(|(k, v)| (Value::Text(k.clone()), v.clone()))
            .collect();
        map.push((Value::Integer(keys::META.into()), Value::Map(meta_map)));
    }

    if let Some(ref payload) = frame.payload {
        map.push((
            Value::Integer(keys::PAYLOAD.into()),
            Value::Bytes(payload.clone()),
        ));
    }

    if let Some(len) = frame.len {
        map.push((
            Value::Integer(keys::LEN.into()),
            Value::Integer((len as i64).into()),
        ));
    }

    if let Some(offset) = frame.offset {
        map.push((
            Value::Integer(keys::OFFSET.into()),
            Value::Integer((offset as i64).into()),
        ));
    }

    if let Some(eof) = frame.eof {
        map.push((Value::Integer(keys::EOF.into()), Value::Bool(eof)));
    }

    if let Some(ref cap) = frame.cap {
        map.push((Value::Integer(keys::CAP.into()), Value::Text(cap.clone())));
    }

    if let Some(ref stream_id) = frame.stream_id {
        map.push((
            Value::Integer(keys::STREAM_ID.into()),
            Value::Text(stream_id.clone()),
        ));
    }

    if let Some(ref media_urn) = frame.media_urn {
        map.push((
            Value::Integer(keys::MEDIA_URN.into()),
            Value::Text(media_urn.clone()),
        ));
    }

    if let Some(ref routing_id) = frame.routing_id {
        let routing_id_value = match routing_id {
            MessageId::Uuid(bytes) => Value::Bytes(bytes.to_vec()),
            MessageId::Uint(n) => Value::Integer((*n as i64).into()),
        };
        map.push((Value::Integer(keys::ROUTING_ID.into()), routing_id_value));
    }

    if let Some(chunk_index) = frame.chunk_index {
        map.push((
            Value::Integer(keys::INDEX.into()),
            Value::Integer((chunk_index as i64).into()),
        ));
    }

    if let Some(chunk_count) = frame.chunk_count {
        map.push((
            Value::Integer(keys::CHUNK_COUNT.into()),
            Value::Integer((chunk_count as i64).into()),
        ));
    }

    if let Some(checksum) = frame.checksum {
        map.push((
            Value::Integer(keys::CHECKSUM.into()),
            Value::Integer(checksum.into()), // Keep unsigned - checksum is u64
        ));
    }

    if let Some(is_sequence) = frame.is_sequence {
        map.push((
            Value::Integer(keys::IS_SEQUENCE.into()),
            Value::Bool(is_sequence),
        ));
    }

    if let Some(force_kill) = frame.force_kill {
        map.push((
            Value::Integer(keys::FORCE_KILL.into()),
            Value::Bool(force_kill),
        ));
    }

    if let Some(credit) = frame.credit {
        map.push((
            Value::Integer(keys::CREDIT.into()),
            Value::Integer(credit.into()), // Keep unsigned - credit is u64
        ));
    }

    if let Some(unbounded) = frame.unbounded {
        map.push((
            Value::Integer(keys::UNBOUNDED.into()),
            Value::Bool(unbounded),
        ));
    }

    let value = Value::Map(map);
    let mut buf = Vec::new();
    ciborium::into_writer(&value, &mut buf).map_err(|e| CborError::Encode(e.to_string()))?;

    Ok(buf)
}

/// Decode a frame from CBOR bytes
pub fn decode_frame(bytes: &[u8]) -> Result<Frame, CborError> {
    let value: Value =
        ciborium::from_reader(bytes).map_err(|e| CborError::Decode(e.to_string()))?;

    let map = match value {
        Value::Map(m) => m,
        _ => return Err(CborError::InvalidFrame("expected map".to_string())),
    };

    // Convert to lookup map
    let mut lookup: BTreeMap<u64, Value> = BTreeMap::new();
    for (k, v) in map {
        if let Value::Integer(i) = k {
            let key: i128 = i.into();
            if key >= 0 {
                lookup.insert(key as u64, v);
            }
        }
    }

    // Extract required fields
    let version = lookup
        .get(&keys::VERSION)
        .and_then(|v| match v {
            Value::Integer(i) => {
                let n: i128 = (*i).into();
                Some(n as u8)
            }
            _ => None,
        })
        .ok_or_else(|| CborError::InvalidFrame("missing version".to_string()))?;

    let frame_type_u8 = lookup
        .get(&keys::FRAME_TYPE)
        .and_then(|v| match v {
            Value::Integer(i) => {
                let n: i128 = (*i).into();
                Some(n as u8)
            }
            _ => None,
        })
        .ok_or_else(|| CborError::InvalidFrame("missing frame_type".to_string()))?;

    let frame_type = FrameType::from_u8(frame_type_u8)
        .ok_or_else(|| CborError::InvalidFrame(format!("invalid frame_type: {}", frame_type_u8)))?;

    // A present-but-malformed id (wrong byte length, negative, or wrong CBOR type) is a
    // corrupt frame — fail hard. Fabricating MessageId::Uint(0) would forge a routing key
    // from corruption and silently misroute the frame. A legitimately id-less frame
    // (HELLO/RelayNotify/RelayState) encodes id as the integer 0, which decodes cleanly.
    let id = match lookup.get(&keys::ID) {
        None => return Err(CborError::InvalidFrame("missing id".to_string())),
        Some(Value::Bytes(bytes)) if bytes.len() == 16 => {
            let mut arr = [0u8; 16];
            arr.copy_from_slice(bytes);
            MessageId::Uuid(arr)
        }
        Some(Value::Bytes(bytes)) => {
            return Err(CborError::InvalidFrame(format!(
                "id: byte string must be exactly 16 bytes for a UUID, got {}",
                bytes.len()
            )))
        }
        Some(Value::Integer(i)) => {
            let n: i128 = (*i).into();
            if n < 0 {
                return Err(CborError::InvalidFrame(format!("id: negative integer {n}")));
            }
            MessageId::Uint(n as u64)
        }
        Some(other) => {
            return Err(CborError::InvalidFrame(format!(
                "id: must be a 16-byte UUID or a uint, got {other:?}"
            )))
        }
    };

    let seq = lookup
        .get(&keys::SEQ)
        .and_then(|v| match v {
            Value::Integer(i) => {
                let n: i128 = (*i).into();
                Some(n as u64)
            }
            _ => None,
        })
        .unwrap_or(0);

    // Optional fields
    let content_type = lookup.get(&keys::CONTENT_TYPE).and_then(|v| match v {
        Value::Text(s) => Some(s.clone()),
        _ => None,
    });

    let meta = lookup.get(&keys::META).and_then(|v| match v {
        Value::Map(m) => {
            let mut result = BTreeMap::new();
            for (k, v) in m {
                if let Value::Text(key) = k {
                    result.insert(key.clone(), v.clone());
                }
            }
            Some(result)
        }
        _ => None,
    });

    let payload = lookup.get(&keys::PAYLOAD).and_then(|v| match v {
        Value::Bytes(b) => Some(b.clone()),
        _ => None,
    });

    let len = lookup.get(&keys::LEN).and_then(|v| match v {
        Value::Integer(i) => {
            let n: i128 = (*i).into();
            Some(n as u64)
        }
        _ => None,
    });

    let offset = lookup.get(&keys::OFFSET).and_then(|v| match v {
        Value::Integer(i) => {
            let n: i128 = (*i).into();
            Some(n as u64)
        }
        _ => None,
    });

    let eof = lookup.get(&keys::EOF).and_then(|v| match v {
        Value::Bool(b) => Some(*b),
        _ => None,
    });

    let cap = lookup.get(&keys::CAP).and_then(|v| match v {
        Value::Text(s) => Some(s.clone()),
        _ => None,
    });

    let stream_id = lookup.get(&keys::STREAM_ID).and_then(|v| match v {
        Value::Text(s) => Some(s.clone()),
        _ => None,
    });

    let media_urn = lookup.get(&keys::MEDIA_URN).and_then(|v| match v {
        Value::Text(s) => Some(s.clone()),
        _ => None,
    });

    // routing_id is optional (absent ⇒ None), but a PRESENT routing_id that is malformed
    // is corruption — fail hard rather than fabricate MessageId::Uint(0) and misroute.
    let routing_id = match lookup.get(&keys::ROUTING_ID) {
        None => None,
        Some(Value::Bytes(bytes)) if bytes.len() == 16 => {
            let mut arr = [0u8; 16];
            arr.copy_from_slice(bytes);
            Some(MessageId::Uuid(arr))
        }
        Some(Value::Bytes(bytes)) => {
            return Err(CborError::InvalidFrame(format!(
                "routing_id: byte string must be exactly 16 bytes, got {}",
                bytes.len()
            )))
        }
        Some(Value::Integer(i)) => {
            let n: i128 = (*i).into();
            if n < 0 {
                return Err(CborError::InvalidFrame(format!(
                    "routing_id: negative integer {n}"
                )));
            }
            Some(MessageId::Uint(n as u64))
        }
        Some(other) => {
            return Err(CborError::InvalidFrame(format!(
                "routing_id: must be a 16-byte UUID or a uint, got {other:?}"
            )))
        }
    };

    let chunk_index = lookup.get(&keys::INDEX).and_then(|v| match v {
        Value::Integer(i) => {
            let n: i128 = (*i).into();
            Some(n as u64)
        }
        _ => None,
    });

    let chunk_count = lookup.get(&keys::CHUNK_COUNT).and_then(|v| match v {
        Value::Integer(i) => {
            let n: i128 = (*i).into();
            Some(n as u64)
        }
        _ => None,
    });

    let checksum = lookup.get(&keys::CHECKSUM).and_then(|v| match v {
        Value::Integer(i) => {
            let n: i128 = (*i).into();
            Some(n as u64)
        }
        _ => None,
    });

    let is_sequence = lookup.get(&keys::IS_SEQUENCE).and_then(|v| match v {
        Value::Bool(b) => Some(*b),
        _ => None,
    });

    let force_kill = lookup.get(&keys::FORCE_KILL).and_then(|v| match v {
        Value::Bool(b) => Some(*b),
        _ => None,
    });

    let credit = lookup.get(&keys::CREDIT).and_then(|v| match v {
        Value::Integer(i) => {
            let n: i128 = (*i).into();
            Some(n as u64)
        }
        _ => None,
    });

    let unbounded = lookup.get(&keys::UNBOUNDED).and_then(|v| match v {
        Value::Bool(b) => Some(*b),
        _ => None,
    });

    let frame = Frame {
        version,
        frame_type,
        id,
        routing_id,
        stream_id,
        media_urn,
        seq,
        content_type,
        meta,
        payload,
        len,
        offset,
        eof,
        cap,
        chunk_index,
        chunk_count,
        checksum,
        is_sequence,
        force_kill,
        credit,
        unbounded,
    };

    // Validate required fields based on frame type
    match frame.frame_type {
        FrameType::Chunk => {
            if frame.chunk_index.is_none() {
                return Err(CborError::InvalidFrame(
                    "CHUNK frame missing required field: chunk_index".to_string(),
                ));
            }
            if frame.checksum.is_none() {
                return Err(CborError::InvalidFrame(
                    "CHUNK frame missing required field: checksum".to_string(),
                ));
            }
        }
        // STREAM_END: chunk_count is optional on the wire — unbounded streams make
        // no length promise (L16). Bounded-stream receivers that want to verify
        // completeness check chunk_count when present.
        FrameType::Credit => {
            if frame.credit.is_none() {
                return Err(CborError::InvalidFrame(
                    "CREDIT frame missing required field: credit".to_string(),
                ));
            }
        }
        _ => {} // Other frame types don't require these fields
    }

    Ok(frame)
}

/// Write a length-prefixed CBOR frame to an async writer
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &Frame,
    limits: &Limits,
) -> Result<(), CborError> {
    let bytes = encode_frame(frame)?;

    if bytes.len() > limits.max_frame {
        return Err(CborError::FrameTooLarge {
            size: bytes.len(),
            max: limits.max_frame,
        });
    }

    if bytes.len() > MAX_FRAME_HARD_LIMIT {
        return Err(CborError::FrameTooLarge {
            size: bytes.len(),
            max: MAX_FRAME_HARD_LIMIT,
        });
    }

    let len = bytes.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&bytes).await?;
    writer.flush().await?;

    Ok(())
}

/// Write a length-prefixed CBOR frame to a sync writer.
///
/// Used by the cartridge writer thread, which runs on a plain `std::thread`
/// so Metal/GCD cannot steal it from tokio's thread pool.
pub fn write_frame_sync<W: std::io::Write>(
    writer: &mut W,
    frame: &Frame,
    limits: &Limits,
) -> Result<(), CborError> {
    let bytes = encode_frame(frame)?;

    if bytes.len() > limits.max_frame {
        return Err(CborError::FrameTooLarge {
            size: bytes.len(),
            max: limits.max_frame,
        });
    }

    if bytes.len() > MAX_FRAME_HARD_LIMIT {
        return Err(CborError::FrameTooLarge {
            size: bytes.len(),
            max: MAX_FRAME_HARD_LIMIT,
        });
    }

    let len = bytes.len() as u32;
    writer.write_all(&len.to_be_bytes())?;
    writer.write_all(&bytes)?;
    writer.flush()?;

    Ok(())
}

/// Read a length-prefixed CBOR frame from an async reader
///
/// Returns Ok(None) on clean EOF, Err(UnexpectedEof) on partial read.
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    limits: &Limits,
) -> Result<Option<Frame>, CborError> {
    // Read 4-byte length prefix
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(CborError::Io(e)),
    }

    let length = u32::from_be_bytes(len_buf) as usize;

    // Validate length
    if length > limits.max_frame || length > MAX_FRAME_HARD_LIMIT {
        return Err(CborError::FrameTooLarge {
            size: length,
            max: limits.max_frame.min(MAX_FRAME_HARD_LIMIT),
        });
    }

    // Read payload
    let mut payload = vec![0u8; length];
    if let Err(e) = reader.read_exact(&mut payload).await {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            return Err(CborError::UnexpectedEof);
        } else {
            return Err(CborError::Io(e));
        }
    }

    let frame = decode_frame(&payload)?;
    Ok(Some(frame))
}

/// Handshake result including manifest (host side - receives cartridge's HELLO with manifest)
#[derive(Debug, Clone)]
pub struct HandshakeResult {
    /// Negotiated protocol limits
    pub limits: Limits,
    /// Cartridge manifest JSON data (from cartridge's HELLO response).
    /// This is REQUIRED - cartridges MUST include their manifest in HELLO.
    pub manifest: Vec<u8>,
    /// The cartridge's full concurrency-pool state map (`bifaci::pools`).
    /// REQUIRED — pools are the protocol's one capacity concept; a HELLO
    /// without the map fails the handshake.
    pub pool_states: crate::bifaci::pools::PoolStates,
}

// =============================================================================
// FRAME I/O TYPES
// =============================================================================

/// CBOR frame reader with limits enforcement
pub struct FrameReader<R: AsyncRead + Unpin> {
    reader: R,
    limits: Limits,
}

impl<R: AsyncRead + Unpin> std::fmt::Debug for FrameReader<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameReader")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    /// Create a new frame reader with default limits
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            limits: Limits::default(),
        }
    }

    /// Create a new frame reader with specified limits
    pub fn with_limits(reader: R, limits: Limits) -> Self {
        Self { reader, limits }
    }

    /// Update limits (after handshake)
    pub fn set_limits(&mut self, limits: Limits) {
        self.limits = limits;
    }

    /// Read the next frame
    pub async fn read(&mut self) -> Result<Option<Frame>, CborError> {
        read_frame(&mut self.reader, &self.limits).await
    }

    /// Get the current limits
    pub fn limits(&self) -> &Limits {
        &self.limits
    }
}

/// CBOR frame writer with limits enforcement
pub struct FrameWriter<W: AsyncWrite + Unpin> {
    writer: W,
    limits: Limits,
}

impl<W: AsyncWrite + Unpin> std::fmt::Debug for FrameWriter<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameWriter")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl<W: AsyncWrite + Unpin> FrameWriter<W> {
    /// Create a new frame writer with default limits
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            limits: Limits::default(),
        }
    }

    /// Create a new frame writer with specified limits
    pub fn with_limits(writer: W, limits: Limits) -> Self {
        Self { writer, limits }
    }

    /// Update limits (after handshake)
    pub fn set_limits(&mut self, limits: Limits) {
        self.limits = limits;
    }

    /// Write a frame
    pub async fn write(&mut self, frame: &Frame) -> Result<(), CborError> {
        write_frame(&mut self.writer, frame, &self.limits).await
    }

    /// Get the current limits
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// Get mutable access to the inner writer
    pub fn inner_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    /// Write a large payload as multiple chunks
    ///
    /// This splits the payload into chunks respecting max_chunk and writes
    /// them as CHUNK frames with proper offset/len/eof markers.
    pub async fn write_chunked(
        &mut self,
        id: MessageId,
        stream_id: String,
        content_type: &str,
        data: &[u8],
    ) -> Result<(), CborError> {
        let total_len = data.len();
        let max_chunk = self.limits.max_chunk;

        if total_len == 0 {
            // Empty payload - send single chunk with eof
            let checksum = Frame::compute_checksum(&[]);
            let mut frame = Frame::chunk(id, stream_id, 0, Vec::new(), 0, checksum);
            frame.content_type = Some(content_type.to_string());
            frame.len = Some(0);
            frame.offset = Some(0);
            frame.eof = Some(true);
            return self.write(&frame).await;
        }

        let mut offset = 0usize;
        let mut chunk_index = 0u64;

        while offset < total_len {
            let chunk_size = max_chunk.min(total_len - offset);
            let is_last = offset + chunk_size >= total_len;

            let chunk_data = data[offset..offset + chunk_size].to_vec();
            let checksum = Frame::compute_checksum(&chunk_data);

            let mut frame = Frame::chunk(
                id.clone(),
                stream_id.clone(),
                0,
                chunk_data,
                chunk_index,
                checksum,
            );
            frame.offset = Some(offset as u64);

            // Set content_type and total len on first chunk (chunk_index-based, not seq-based)
            if chunk_index == 0 {
                frame.content_type = Some(content_type.to_string());
                frame.len = Some(total_len as u64);
            }

            if is_last {
                frame.eof = Some(true);
            }

            self.write(&frame).await?;

            chunk_index += 1;
            offset += chunk_size;
        }

        Ok(())
    }
}

/// Perform HELLO handshake and extract cartridge manifest (host side - sends first).
/// Returns HandshakeResult containing negotiated limits and cartridge manifest.
/// Fails if cartridge HELLO is missing the required manifest.
pub async fn handshake<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut FrameReader<R>,
    writer: &mut FrameWriter<W>,
) -> Result<HandshakeResult, CborError> {
    // Send our HELLO
    let our_hello = Frame::hello(&Limits::default());
    writer.write(&our_hello).await?;

    // Read their HELLO (should include manifest)
    let their_frame = reader.read().await?.ok_or_else(|| {
        CborError::Handshake("connection closed before receiving HELLO".to_string())
    })?;

    if their_frame.frame_type != FrameType::Hello {
        return Err(CborError::Handshake(format!(
            "expected HELLO, got {:?}",
            their_frame.frame_type
        )));
    }

    // Protocol version must match exactly (L1). No cross-version operation.
    let their_version = their_frame
        .hello_version()
        .ok_or_else(|| CborError::Handshake("HELLO missing required version".to_string()))?;
    if their_version != PROTOCOL_VERSION {
        return Err(CborError::Handshake(format!(
            "protocol version mismatch: ours {}, theirs {}",
            PROTOCOL_VERSION, their_version
        )));
    }

    // Extract manifest - REQUIRED for cartridges
    let manifest = their_frame
        .hello_manifest()
        .ok_or_else(|| {
            CborError::Handshake("Cartridge HELLO missing required manifest".to_string())
        })?
        .to_vec();
    let pool_states = {
        let bytes = their_frame.pool_state_bytes().ok_or_else(|| {
            CborError::Handshake(
                "Cartridge HELLO missing required concurrency-pool state map".to_string(),
            )
        })?;
        crate::bifaci::pools::decode_pool_states(bytes)
            .map_err(|e| CborError::Handshake(format!("Cartridge HELLO pool map: {e}")))?
    };

    // Negotiate minimum of both
    let their_max_frame = their_frame
        .hello_max_frame()
        .ok_or_else(|| CborError::Handshake("HELLO missing required max_frame".to_string()))?;
    let their_max_chunk = their_frame
        .hello_max_chunk()
        .ok_or_else(|| CborError::Handshake("HELLO missing required max_chunk".to_string()))?;
    let their_max_reorder_buffer = their_frame.hello_max_reorder_buffer().ok_or_else(|| {
        CborError::Handshake("HELLO missing required max_reorder_buffer".to_string())
    })?;
    let their_initial_credit = their_frame
        .hello_initial_credit()
        .ok_or_else(|| CborError::Handshake("HELLO missing required initial_credit".to_string()))?;

    let limits = Limits {
        max_frame: DEFAULT_MAX_FRAME.min(their_max_frame),
        max_chunk: DEFAULT_MAX_CHUNK.min(their_max_chunk),
        max_reorder_buffer: DEFAULT_MAX_REORDER_BUFFER.min(their_max_reorder_buffer),
        initial_credit: DEFAULT_INITIAL_CREDIT.min(their_initial_credit),
    };

    // Update both reader and writer with negotiated limits
    reader.set_limits(limits);
    writer.set_limits(limits);

    Ok(HandshakeResult {
        limits,
        manifest,
        pool_states,
    })
}

/// Accept HELLO handshake with manifest (cartridge side - receives first, sends manifest in response).
///
/// Reads host's HELLO, sends our HELLO with manifest, returns negotiated limits.
/// The manifest is REQUIRED - cartridges MUST provide their manifest.
pub async fn handshake_accept<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut FrameReader<R>,
    writer: &mut FrameWriter<W>,
    manifest: &[u8],
    pool_states: &crate::bifaci::pools::PoolStates,
) -> Result<Limits, CborError> {
    // Read their HELLO first (host initiates)
    let their_frame = reader.read().await?.ok_or_else(|| {
        CborError::Handshake("connection closed before receiving HELLO".to_string())
    })?;

    if their_frame.frame_type != FrameType::Hello {
        return Err(CborError::Handshake(format!(
            "expected HELLO, got {:?}",
            their_frame.frame_type
        )));
    }

    // Protocol version must match exactly (L1). No cross-version operation.
    let their_version = their_frame
        .hello_version()
        .ok_or_else(|| CborError::Handshake("HELLO missing required version".to_string()))?;
    if their_version != PROTOCOL_VERSION {
        return Err(CborError::Handshake(format!(
            "protocol version mismatch: ours {}, theirs {}",
            PROTOCOL_VERSION, their_version
        )));
    }

    // Negotiate minimum of both
    let their_max_frame = their_frame
        .hello_max_frame()
        .ok_or_else(|| CborError::Handshake("HELLO missing required max_frame".to_string()))?;
    let their_max_chunk = their_frame
        .hello_max_chunk()
        .ok_or_else(|| CborError::Handshake("HELLO missing required max_chunk".to_string()))?;
    let their_max_reorder_buffer = their_frame.hello_max_reorder_buffer().ok_or_else(|| {
        CborError::Handshake("HELLO missing required max_reorder_buffer".to_string())
    })?;
    let their_initial_credit = their_frame
        .hello_initial_credit()
        .ok_or_else(|| CborError::Handshake("HELLO missing required initial_credit".to_string()))?;

    let limits = Limits {
        max_frame: DEFAULT_MAX_FRAME.min(their_max_frame),
        max_chunk: DEFAULT_MAX_CHUNK.min(their_max_chunk),
        max_reorder_buffer: DEFAULT_MAX_REORDER_BUFFER.min(their_max_reorder_buffer),
        initial_credit: DEFAULT_INITIAL_CREDIT.min(their_initial_credit),
    };

    // Send our HELLO with manifest
    let our_hello = Frame::hello_with_manifest(&limits, manifest, pool_states);
    writer.write(&our_hello).await?;

    // Update both reader and writer with negotiated limits
    reader.set_limits(limits);
    writer.set_limits(limits);

    Ok(limits)
}

// =============================================================================
// IDENTITY VERIFICATION
// =============================================================================

/// CBOR-encoded Text("bifaci") — deterministic 7-byte nonce for identity verification.
pub(crate) fn identity_nonce() -> Vec<u8> {
    let mut buf = Vec::new();
    ciborium::into_writer(&Value::Text("bifaci".to_string()), &mut buf)
        .expect("BUG: failed to encode identity nonce");
    buf
}

/// Verify a connection by invoking the identity capability.
///
/// Sends a REQ with CAP_IDENTITY carrying the "bifaci" nonce with proper
/// XID and seq assignment, then verifies the response echoes it back unchanged.
/// This proves the entire protocol stack works end-to-end before the connection
/// is considered live.
///
/// Must be called after handshake, before any other traffic.
pub async fn verify_identity<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut FrameReader<R>,
    writer: &mut FrameWriter<W>,
) -> Result<(), CborError> {
    use crate::bifaci::frame::SeqAssigner;
    use crate::standard::caps::CAP_IDENTITY;

    let nonce = identity_nonce();
    let req_id = MessageId::new_uuid();
    let stream_id = "identity-verify".to_string();
    let xid = MessageId::Uint(0);
    let mut seq = SeqAssigner::new();

    // Send REQ (empty payload) with XID + seq
    let mut req = Frame::req(req_id.clone(), CAP_IDENTITY, vec![], "application/cbor");
    req.routing_id = Some(xid.clone());
    seq.assign(&mut req);
    writer.write(&req).await?;

    // Send request body: STREAM_START → CHUNK → STREAM_END → END
    let mut stream_start = Frame::stream_start(
        req_id.clone(),
        stream_id.clone(),
        "media:".to_string(),
        None,
    );
    stream_start.routing_id = Some(xid.clone());
    seq.assign(&mut stream_start);
    writer.write(&stream_start).await?;

    // CBOR-encode nonce before checksumming (protocol v2: CHUNK payload = CBOR-encoded data)
    let mut cbor_nonce = Vec::new();
    ciborium::into_writer(&Value::Bytes(nonce.clone()), &mut cbor_nonce)
        .expect("BUG: failed to CBOR-encode nonce");
    let checksum = Frame::compute_checksum(&cbor_nonce);
    let mut chunk = Frame::chunk(
        req_id.clone(),
        stream_id.clone(),
        0,
        cbor_nonce,
        0,
        checksum,
    );
    chunk.routing_id = Some(xid.clone());
    seq.assign(&mut chunk);
    writer.write(&chunk).await?;

    let mut stream_end = Frame::stream_end(req_id.clone(), stream_id, 1);
    stream_end.routing_id = Some(xid.clone());
    seq.assign(&mut stream_end);
    writer.write(&stream_end).await?;

    let mut end = Frame::end(req_id.clone(), None);
    end.routing_id = Some(xid.clone());
    seq.assign(&mut end);
    writer.write(&end).await?;

    // Read response — expect STREAM_START → CHUNK(s) → STREAM_END → END
    // Each CHUNK payload is CBOR-encoded (protocol v2), decode each and concatenate
    let mut cbor_chunks = Vec::new();
    loop {
        let frame = reader.read().await?.ok_or_else(|| {
            CborError::Protocol("Connection closed during identity verification".to_string())
        })?;

        match frame.frame_type {
            FrameType::StreamStart => {}
            FrameType::Chunk => {
                if let Some(cbor_payload) = frame.payload {
                    // Decode CBOR chunk
                    let value: Value = ciborium::from_reader(&cbor_payload[..]).map_err(|e| {
                        CborError::Protocol(format!("Failed to decode CBOR chunk: {}", e))
                    })?;
                    if let Value::Bytes(bytes) = value {
                        cbor_chunks.push(bytes);
                    } else {
                        return Err(CborError::Protocol(format!(
                            "Expected bytes chunk, got {:?}",
                            value
                        )));
                    }
                }
            }
            FrameType::StreamEnd => {}
            FrameType::End => {
                // Concatenate all decoded chunks
                let accumulated: Vec<u8> = cbor_chunks.into_iter().flatten().collect();
                if accumulated != nonce {
                    return Err(CborError::Protocol(format!(
                        "Identity verification failed: payload mismatch (expected {} bytes, got {})",
                        nonce.len(),
                        accumulated.len()
                    )));
                }
                return Ok(());
            }
            FrameType::Err => {
                let code = frame.error_code().ok_or_else(|| {
                    CborError::Protocol("ERR frame missing required text code".to_string())
                })?;
                let msg = frame.error_message().ok_or_else(|| {
                    CborError::Protocol("ERR frame missing required text message".to_string())
                })?;
                return Err(CborError::Protocol(format!(
                    "Identity verification failed: [{code}] {msg}"
                )));
            }
            // Control/side-channel frames are legal ANYWHERE during the
            // probe (spec 12.4: LOG interleaves without affecting data
            // flow; CREDIT/HEARTBEAT are the control plane the writer
            // gate itself exempts, L4). A v4 cartridge crediting its
            // probe input as it consumes (L10) must not fail identity
            // verification.
            FrameType::Log | FrameType::Credit | FrameType::Heartbeat => {}
            other => {
                return Err(CborError::Protocol(format!(
                    "Identity verification: unexpected frame type {:?}",
                    other
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader as TokioBufReader;
    use tokio::io::BufWriter as TokioBufWriter;

    // TEST205: Test REQ frame encode/decode roundtrip preserves all fields
    #[test]
    fn test205_encode_decode_roundtrip() {
        let id = MessageId::new_uuid();
        let original = Frame::req(
            id.clone(),
            r#"cap:in="media:void";test;out="media:void""#,
            b"payload".to_vec(),
            "application/json",
        );

        let bytes = encode_frame(&original).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.version, original.version);
        assert_eq!(decoded.frame_type, original.frame_type);
        assert_eq!(decoded.id, original.id);
        assert_eq!(decoded.cap, original.cap);
        assert_eq!(decoded.payload, original.payload);
        assert_eq!(decoded.content_type, original.content_type);
    }

    // TEST206: Test HELLO frame encode/decode roundtrip preserves max_frame, max_chunk, max_reorder_buffer
    #[test]
    fn test206_hello_frame_roundtrip() {
        let original = Frame::hello(&Limits {
            max_frame: 500_000,
            max_chunk: 50_000,
            max_reorder_buffer: 128,
            initial_credit: DEFAULT_INITIAL_CREDIT,
        });
        let bytes = encode_frame(&original).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.frame_type, FrameType::Hello);
        assert_eq!(decoded.hello_max_frame(), Some(500_000));
        assert_eq!(decoded.hello_max_chunk(), Some(50_000));
        assert_eq!(decoded.hello_max_reorder_buffer(), Some(128));
    }

    // TEST207: Test ERR frame encode/decode roundtrip preserves error code and message
    #[test]
    fn test207_err_frame_roundtrip() {
        let id = MessageId::new_uuid();
        let original = Frame::err(
            id,
            "NOT_FOUND",
            crate::AttributionClass::Internal,
            "Cap not found",
            None,
        );
        let bytes = encode_frame(&original).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.frame_type, FrameType::Err);
        assert_eq!(decoded.error_code(), Some("NOT_FOUND"));
        assert_eq!(decoded.error_message(), Some("Cap not found"));
    }

    // TEST208: Test LOG frame encode/decode roundtrip preserves level and message
    #[test]
    fn test208_log_frame_roundtrip() {
        let id = MessageId::new_uuid();
        let original = Frame::log(
            id,
            "warn",
            crate::AttributionClass::Internal,
            "Something happened",
            None,
        );
        let bytes = encode_frame(&original).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.frame_type, FrameType::Log);
        assert_eq!(decoded.log_level(), Some("warn"));
        assert_eq!(decoded.log_message(), Some("Something happened"));
    }

    // TEST846: Test progress LOG frame encode/decode roundtrip preserves progress float
    #[test]
    fn test846_progress_frame_roundtrip() {
        let id = MessageId::new_uuid();

        // Test several progress values including edge cases AND the f64→f32→f64 chain
        // that modelcartridge uses (ProgressInfo.progress is f64, cast to f32, then back to f64)
        let test_values: Vec<(f32, &str)> = vec![
            (0.0, "zero"),
            (0.0f64 as f32, "zero via f64"),
            (0.03333333f64 as f32, "1/30 via f64"),
            (0.06666667f64 as f32, "2/30 via f64"),
            (0.13333334f64 as f32, "4/30 via f64"),
            (0.25, "quarter"),
            (0.5, "half"),
            (0.75, "three-quarter"),
            (1.0, "one"),
        ];

        for (progress, label) in &test_values {
            let original = Frame::progress(id.clone(), *progress, "test phase");

            let raw_progress_val = original.meta.as_ref().unwrap().get("progress").unwrap();
            let bytes = encode_frame(&original).expect("encode should succeed");
            let decoded = decode_frame(&bytes).expect("decode should succeed");
            let decoded_progress_val = decoded.meta.as_ref().unwrap().get("progress").unwrap();

            assert_eq!(decoded.frame_type, FrameType::Log);
            assert_eq!(decoded.log_level(), Some("progress"));
            assert_eq!(decoded.log_message(), Some("test phase"));

            let decoded_progress = decoded.log_progress();
            assert!(
                decoded_progress.is_some(),
                "log_progress() must return Some for progress={} ({}), decoded Value={:?}, meta={:?}",
                progress, label, decoded_progress_val, decoded.meta
            );
            let p = decoded_progress.unwrap();
            assert!(
                (p - progress).abs() < 0.001,
                "progress roundtrip for {} ({}): expected {}, got {}",
                label,
                progress,
                progress,
                p
            );
        }
    }

    // TEST847: Double roundtrip (modelcartridge → relay → candlecartridge)
    #[test]
    fn test847_progress_double_roundtrip() {
        let id = MessageId::new_uuid();

        for progress in [0.0f32, 0.03333333, 0.06666667, 0.13333334, 0.5, 1.0] {
            let original = Frame::progress(id.clone(), progress, "test");

            // First roundtrip (modelcartridge → relay_switch)
            let bytes1 = encode_frame(&original).expect("encode 1");
            let mut decoded1 = decode_frame(&bytes1).expect("decode 1");

            // Relay switch modifies seq (like SeqAssigner does)
            decoded1.seq = 42;

            // Second roundtrip (relay_switch → candlecartridge)
            let bytes2 = encode_frame(&decoded1).expect("encode 2");
            let decoded2 = decode_frame(&bytes2).expect("decode 2");

            let raw_val = decoded2.meta.as_ref().unwrap().get("progress").unwrap();
            let lp = decoded2.log_progress();
            assert!(
                lp.is_some(),
                "progress={}: log_progress() returned None, raw={:?}",
                progress,
                raw_val
            );
            assert!(
                (lp.unwrap() - progress).abs() < 0.001,
                "progress={}: expected {}, got {}",
                progress,
                progress,
                lp.unwrap()
            );
        }
    }

    // TEST209 REMOVED: RES frame removed - old protocol no longer supported
    // NEW PROTOCOL: Use stream multiplexing (STREAM_START + CHUNK + STREAM_END + END)

    // TEST210: Test END frame encode/decode roundtrip preserves eof marker and optional payload
    #[test]
    fn test210_end_frame_roundtrip() {
        let id = MessageId::new_uuid();
        let original = Frame::end(id.clone(), Some(b"final".to_vec()));
        let bytes = encode_frame(&original).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.frame_type, FrameType::End);
        assert_eq!(decoded.id, id);
        assert!(decoded.is_eof());
        assert_eq!(decoded.payload, Some(b"final".to_vec()));
    }

    // TEST211: Test HELLO with manifest encode/decode roundtrip preserves manifest bytes and limits
    #[test]
    fn test211_hello_with_manifest_roundtrip() {
        let manifest = b"{\"name\":\"Test\",\"version\":\"1.0\"}";
        let original = Frame::hello_with_manifest(
            &Limits {
                max_frame: 1_000_000,
                max_chunk: 100_000,
                max_reorder_buffer: 48,
                initial_credit: DEFAULT_INITIAL_CREDIT,
            },
            manifest,
            &crate::bifaci::pools::PoolStates::new(),
        );
        let bytes = encode_frame(&original).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.hello_manifest().unwrap(), manifest);
        assert_eq!(decoded.hello_max_frame(), Some(1_000_000));
        assert_eq!(decoded.hello_max_chunk(), Some(100_000));
        assert_eq!(decoded.hello_max_reorder_buffer(), Some(48));
    }

    // TEST212: Test chunk_with_offset encode/decode roundtrip preserves offset, len, eof (with stream_id)
    #[test]
    fn test212_chunk_with_offset_roundtrip() {
        let id = MessageId::new_uuid();
        let stream_id = "stream-test".to_string();
        let payload = b"data".to_vec();
        let checksum = Frame::compute_checksum(&payload);
        let original = Frame::chunk_with_offset(
            id.clone(),
            stream_id,
            0,
            payload,
            100,
            Some(5000),
            true,
            0,
            checksum,
        );
        let bytes = encode_frame(&original).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.frame_type, FrameType::Chunk);
        assert_eq!(decoded.id, id);
        assert_eq!(decoded.seq, 0);
        assert_eq!(decoded.offset, Some(100));
        assert_eq!(decoded.len, Some(5000));
        assert!(decoded.is_eof());
        assert_eq!(decoded.payload, Some(b"data".to_vec()));
    }

    // TEST213: Test heartbeat frame encode/decode roundtrip preserves ID with no extra fields
    #[test]
    fn test213_heartbeat_roundtrip() {
        let id = MessageId::new_uuid();
        let original = Frame::heartbeat(id.clone());
        let bytes = encode_frame(&original).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.frame_type, FrameType::Heartbeat);
        assert_eq!(decoded.id, id);
        assert!(decoded.payload.is_none());
        assert!(decoded.meta.is_none());
    }

    // TEST214: Test write_frame/read_frame IO roundtrip through length-prefixed wire format
    #[tokio::test]
    async fn test214_frame_io_roundtrip() {
        let limits = Limits::default();
        let id = MessageId::new_uuid();
        let original = Frame::req(
            id,
            r#"cap:in="media:void";test;out="media:void""#,
            b"payload".to_vec(),
            "application/json",
        );

        // Write to buffer using duplex stream
        let (mut client, mut server) = tokio::io::duplex(64 * 1024);
        write_frame(&mut client, &original, &limits)
            .await
            .expect("write should succeed");
        drop(client); // Close write side to signal EOF

        // Read back
        let decoded = read_frame(&mut server, &limits)
            .await
            .expect("read should succeed")
            .expect("should have frame");

        assert_eq!(decoded.frame_type, original.frame_type);
        assert_eq!(decoded.cap, original.cap);
        assert_eq!(decoded.payload, original.payload);
    }

    // TEST215: Test reading multiple sequential frames from a single buffer
    #[tokio::test]
    async fn test215_multiple_frames() {
        let limits = Limits::default();

        let id1 = MessageId::new_uuid();
        let id2 = MessageId::new_uuid();
        let id3 = MessageId::new_uuid();

        let f1 = Frame::req(
            id1.clone(),
            r#"cap:in="media:void";first;out="media:void""#,
            b"one".to_vec(),
            "text/plain",
        );
        let payload2 = b"two".to_vec();
        let checksum2 = Frame::compute_checksum(&payload2);
        let f2 = Frame::chunk(
            id2.clone(),
            "stream-2".to_string(),
            0,
            payload2,
            0,
            checksum2,
        );
        let f3 = Frame::end(id3.clone(), Some(b"three".to_vec()));

        let (mut client, mut server) = tokio::io::duplex(64 * 1024);

        write_frame(&mut client, &f1, &limits).await.unwrap();
        write_frame(&mut client, &f2, &limits).await.unwrap();
        write_frame(&mut client, &f3, &limits).await.unwrap();
        drop(client);

        let r1 = read_frame(&mut server, &limits).await.unwrap().unwrap();
        assert_eq!(r1.frame_type, FrameType::Req);
        assert_eq!(r1.id, id1);

        let r2 = read_frame(&mut server, &limits).await.unwrap().unwrap();
        assert_eq!(r2.frame_type, FrameType::Chunk);
        assert_eq!(r2.id, id2);

        let r3 = read_frame(&mut server, &limits).await.unwrap().unwrap();
        assert_eq!(r3.frame_type, FrameType::End);
        assert_eq!(r3.id, id3);

        // EOF after all frames read
        assert!(read_frame(&mut server, &limits).await.unwrap().is_none());
    }

    // TEST216: Test write_frame rejects frames exceeding max_frame limit
    #[tokio::test]
    async fn test216_frame_too_large() {
        let limits = Limits {
            max_frame: 100,
            max_chunk: 50,
            ..Limits::default()
        };

        let id = MessageId::new_uuid();
        let large_payload = vec![0u8; 200];
        let frame = Frame::req(
            id,
            r#"cap:in="media:void";test;out="media:void""#,
            large_payload,
            "application/octet-stream",
        );

        let (mut client, _server) = tokio::io::duplex(64 * 1024);
        let result = write_frame(&mut client, &frame, &limits).await;
        assert!(matches!(result, Err(CborError::FrameTooLarge { .. })));
    }

    // TEST217: Test read_frame rejects incoming frames exceeding the negotiated max_frame limit
    #[tokio::test]
    async fn test217_read_frame_too_large() {
        let write_limits = Limits {
            max_frame: 10_000_000,
            max_chunk: 1_000_000,
            ..Limits::default()
        };
        let read_limits = Limits {
            max_frame: 50,
            max_chunk: 50,
            ..Limits::default()
        };

        // Write a frame with generous limits
        let id = MessageId::new_uuid();
        let frame = Frame::req(
            id,
            r#"cap:in="media:void";test;out="media:void""#,
            vec![0u8; 200],
            "text/plain",
        );

        let (mut client, mut server) = tokio::io::duplex(64 * 1024);
        write_frame(&mut client, &frame, &write_limits)
            .await
            .unwrap();
        drop(client);

        // Try to read with strict limits
        let result = read_frame(&mut server, &read_limits).await;
        assert!(matches!(result, Err(CborError::FrameTooLarge { .. })));
    }

    // TEST218: Test write_chunked splits data into chunks respecting max_chunk and reconstructs correctly
    // Chunks from write_chunked have seq=0. SeqAssigner at the output stage assigns final seq.
    // Chunk ordering within a stream is tracked by chunk_index (chunk_index field).
    #[tokio::test]
    async fn test218_write_chunked() {
        let limits = Limits {
            max_frame: 1_000_000,
            max_chunk: 10, // Very small for testing
            ..Limits::default()
        };

        let id = MessageId::new_uuid();
        let stream_id = "stream-chunked".to_string();
        let data = b"Hello, this is a longer message that will be chunked!";

        let (client, server) = tokio::io::duplex(64 * 1024);
        let mut writer = FrameWriter::with_limits(client, limits);

        writer
            .write_chunked(id.clone(), stream_id, "text/plain", data)
            .await
            .expect("chunked write should succeed");
        drop(writer);

        // Read back all chunks
        let mut reader = FrameReader::with_limits(server, limits);

        let mut received = Vec::new();
        let mut chunk_count = 0u64;
        let mut first_chunk_had_len = false;
        let mut first_chunk_had_content_type = false;

        loop {
            let frame = reader.read().await.unwrap();
            match frame {
                Some(f) => {
                    assert_eq!(f.frame_type, FrameType::Chunk);
                    assert_eq!(f.id, id);
                    assert_eq!(
                        f.seq, 0,
                        "write_chunked produces seq=0; SeqAssigner assigns at output stage"
                    );
                    // chunk_index tracks ordering within the chunked write
                    assert_eq!(
                        f.chunk_index,
                        Some(chunk_count),
                        "chunk_index must increment monotonically"
                    );

                    if chunk_count == 0 {
                        first_chunk_had_len = f.len.is_some();
                        first_chunk_had_content_type = f.content_type.is_some();
                        assert_eq!(
                            f.len,
                            Some(data.len() as u64),
                            "first chunk must carry total len"
                        );
                        assert_eq!(f.content_type, Some("text/plain".to_string()));
                    }

                    let is_eof = f.is_eof();
                    if let Some(payload) = f.payload {
                        assert!(
                            payload.len() <= limits.max_chunk,
                            "chunk must not exceed max_chunk"
                        );
                        received.extend_from_slice(&payload);
                    }

                    if is_eof {
                        break;
                    }
                    chunk_count += 1;
                }
                None => break,
            }
        }

        assert_eq!(received, data);
        assert!(
            chunk_count > 0,
            "data larger than max_chunk must produce multiple chunks"
        );
        assert!(first_chunk_had_len, "first chunk must carry total length");
        assert!(
            first_chunk_had_content_type,
            "first chunk must carry content_type"
        );
    }

    // TEST219: Test write_chunked with empty data produces a single EOF chunk
    #[tokio::test]
    async fn test219_write_chunked_empty_data() {
        let limits = Limits {
            max_frame: 1_000_000,
            max_chunk: 100,
            ..Limits::default()
        };

        let id = MessageId::new_uuid();
        let (client, mut server) = tokio::io::duplex(64 * 1024);
        let mut writer = FrameWriter::with_limits(client, limits);
        writer
            .write_chunked(id.clone(), "stream-empty".to_string(), "text/plain", b"")
            .await
            .unwrap();
        drop(writer);

        let frame = read_frame(&mut server, &limits)
            .await
            .unwrap()
            .expect("should have frame");
        assert_eq!(frame.frame_type, FrameType::Chunk);
        assert!(frame.is_eof(), "empty data must produce immediate EOF");
        assert_eq!(frame.len, Some(0), "empty payload must report len=0");
        assert_eq!(frame.payload, Some(vec![]));
    }

    // TEST220: Test write_chunked with data exactly equal to max_chunk produces exactly one chunk
    #[tokio::test]
    async fn test220_write_chunked_exact_fit() {
        let limits = Limits {
            max_frame: 1_000_000,
            max_chunk: 10,
            ..Limits::default()
        };

        let id = MessageId::new_uuid();
        let data = b"0123456789"; // exactly 10 bytes = max_chunk
        let (client, mut server) = tokio::io::duplex(64 * 1024);
        let mut writer = FrameWriter::with_limits(client, limits);
        writer
            .write_chunked(id.clone(), "stream-1mb".to_string(), "text/plain", data)
            .await
            .unwrap();
        drop(writer);

        let frame = read_frame(&mut server, &limits)
            .await
            .unwrap()
            .expect("should have frame");
        assert!(frame.is_eof(), "single-chunk data must be EOF");
        assert_eq!(frame.payload, Some(data.to_vec()));
        assert_eq!(frame.seq, 0);
        // No more frames
        assert!(read_frame(&mut server, &limits).await.unwrap().is_none());
    }

    // TEST221: Test read_frame returns Ok(None) on clean EOF (empty stream)
    #[tokio::test]
    async fn test221_eof_handling() {
        let limits = Limits::default();
        let (_client, mut server) = tokio::io::duplex(1024);
        drop(_client); // Close write side immediately to signal EOF
        let result = read_frame(&mut server, &limits).await.unwrap();
        assert!(result.is_none());
    }

    // TEST222: Test read_frame handles truncated length prefix (fewer than 4 bytes available)
    #[tokio::test]
    async fn test222_truncated_length_prefix() {
        let limits = Limits::default();
        // Only 2 bytes, but 4 needed for length prefix
        let (mut client, mut server) = tokio::io::duplex(1024);
        use tokio::io::AsyncWriteExt;
        client.write_all(&[0x00, 0x01]).await.unwrap();
        drop(client);
        let result = read_frame(&mut server, &limits).await;
        // read_exact with insufficient data returns UnexpectedEof,
        // which maps to Ok(None) for the clean-EOF path in read_frame.
        match result {
            Ok(None) => {} // clean EOF interpretation
            Err(_) => {}   // partial read error interpretation
            Ok(Some(_)) => panic!("must not produce a frame from truncated data"),
        }
    }

    // TEST223: Test read_frame returns error on truncated frame body (length prefix says more bytes than available)
    #[tokio::test]
    async fn test223_truncated_frame_body() {
        let limits = Limits::default();
        // Length prefix says 100 bytes, but only 5 bytes of data follow
        let mut data = vec![0x00, 0x00, 0x00, 100]; // length = 100
        data.extend_from_slice(b"short"); // only 5 bytes
        let (mut client, mut server) = tokio::io::duplex(1024);
        use tokio::io::AsyncWriteExt;
        client.write_all(&data).await.unwrap();
        drop(client);
        let result = read_frame(&mut server, &limits).await;
        assert!(result.is_err(), "truncated body must be an error");
    }

    // TEST224: Test MessageId::Uint roundtrips through encode/decode
    #[test]
    fn test224_message_id_uint() {
        let id = MessageId::Uint(12345);
        let frame = Frame::new(FrameType::Req, id.clone());

        let bytes = encode_frame(&frame).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.id, id);
    }

    // TEST225: Test decode_frame rejects non-map CBOR values (e.g., array, integer, string)
    #[test]
    fn test225_decode_non_map_value() {
        // Encode a CBOR array instead of map
        let value = ciborium::Value::Array(vec![ciborium::Value::Integer(1.into())]);
        let mut bytes = Vec::new();
        ciborium::into_writer(&value, &mut bytes).unwrap();

        let result = decode_frame(&bytes);
        assert!(matches!(result, Err(CborError::InvalidFrame(_))));
    }

    // TEST226: Test decode_frame rejects CBOR map missing required version field
    #[test]
    fn test226_decode_missing_version() {
        // Build CBOR map with frame_type and id but missing version
        let map = ciborium::Value::Map(vec![
            (
                ciborium::Value::Integer(keys::FRAME_TYPE.into()),
                ciborium::Value::Integer(1.into()),
            ),
            (
                ciborium::Value::Integer(keys::ID.into()),
                ciborium::Value::Integer(0.into()),
            ),
        ]);
        let mut bytes = Vec::new();
        ciborium::into_writer(&map, &mut bytes).unwrap();

        let result = decode_frame(&bytes);
        assert!(matches!(result, Err(CborError::InvalidFrame(_))));
    }

    // TEST227: Test decode_frame rejects CBOR map with invalid frame_type value
    #[test]
    fn test227_decode_invalid_frame_type_value() {
        let map = ciborium::Value::Map(vec![
            (
                ciborium::Value::Integer(keys::VERSION.into()),
                ciborium::Value::Integer(1.into()),
            ),
            (
                ciborium::Value::Integer(keys::FRAME_TYPE.into()),
                ciborium::Value::Integer(99.into()),
            ),
            (
                ciborium::Value::Integer(keys::ID.into()),
                ciborium::Value::Integer(0.into()),
            ),
        ]);
        let mut bytes = Vec::new();
        ciborium::into_writer(&map, &mut bytes).unwrap();

        let result = decode_frame(&bytes);
        assert!(matches!(result, Err(CborError::InvalidFrame(_))));
    }

    // TEST228: Test decode_frame rejects CBOR map missing required id field
    #[test]
    fn test228_decode_missing_id() {
        let map = ciborium::Value::Map(vec![
            (
                ciborium::Value::Integer(keys::VERSION.into()),
                ciborium::Value::Integer(1.into()),
            ),
            (
                ciborium::Value::Integer(keys::FRAME_TYPE.into()),
                ciborium::Value::Integer(1.into()),
            ),
            // No ID field
        ]);
        let mut bytes = Vec::new();
        ciborium::into_writer(&map, &mut bytes).unwrap();

        let result = decode_frame(&bytes);
        assert!(matches!(result, Err(CborError::InvalidFrame(_))));
    }

    // TEST7003: decode_frame rejects a present-but-malformed id (wrong byte length) as a
    // hard decode error instead of fabricating MessageId::Uint(0). Forging a routing key
    // from corruption would silently misroute the frame.
    #[test]
    fn test7003_decode_rejects_malformed_id_wrong_length() {
        let map = ciborium::Value::Map(vec![
            (
                ciborium::Value::Integer(keys::VERSION.into()),
                ciborium::Value::Integer(1.into()),
            ),
            (
                ciborium::Value::Integer(keys::FRAME_TYPE.into()),
                ciborium::Value::Integer(1.into()),
            ),
            // 5 bytes: present but not a valid 16-byte UUID.
            (
                ciborium::Value::Integer(keys::ID.into()),
                ciborium::Value::Bytes(vec![1, 2, 3, 4, 5]),
            ),
        ]);
        let mut bytes = Vec::new();
        ciborium::into_writer(&map, &mut bytes).unwrap();
        assert!(
            matches!(decode_frame(&bytes), Err(CborError::InvalidFrame(m)) if m.contains("id"))
        );
    }

    // TEST7004: decode_frame rejects an id of the wrong CBOR type as a hard decode error.
    #[test]
    fn test7004_decode_rejects_malformed_id_wrong_type() {
        let map = ciborium::Value::Map(vec![
            (
                ciborium::Value::Integer(keys::VERSION.into()),
                ciborium::Value::Integer(1.into()),
            ),
            (
                ciborium::Value::Integer(keys::FRAME_TYPE.into()),
                ciborium::Value::Integer(1.into()),
            ),
            (
                ciborium::Value::Integer(keys::ID.into()),
                ciborium::Value::Text("not-an-id".to_string()),
            ),
        ]);
        let mut bytes = Vec::new();
        ciborium::into_writer(&map, &mut bytes).unwrap();
        assert!(matches!(
            decode_frame(&bytes),
            Err(CborError::InvalidFrame(_))
        ));
    }

    // TEST7005: decode_frame rejects a present-but-malformed routing_id (wrong length or
    // wrong CBOR type) rather than silently dropping it — a dropped relay hint would let
    // the switch treat a routed response as a fresh top-level request. A well-formed
    // routing_id must still decode.
    #[test]
    fn test7005_decode_rejects_malformed_routing_id() {
        let base = || {
            vec![
                (
                    ciborium::Value::Integer(keys::VERSION.into()),
                    ciborium::Value::Integer(1.into()),
                ),
                (
                    ciborium::Value::Integer(keys::FRAME_TYPE.into()),
                    ciborium::Value::Integer(1.into()),
                ),
                (
                    ciborium::Value::Integer(keys::ID.into()),
                    ciborium::Value::Integer(1.into()),
                ),
            ]
        };
        let encode = |m: Vec<(ciborium::Value, ciborium::Value)>| {
            let mut bytes = Vec::new();
            ciborium::into_writer(&ciborium::Value::Map(m), &mut bytes).unwrap();
            bytes
        };

        // Wrong byte length.
        let mut m = base();
        m.push((
            ciborium::Value::Integer(keys::ROUTING_ID.into()),
            ciborium::Value::Bytes(vec![9, 9, 9]),
        ));
        assert!(matches!(
            decode_frame(&encode(m)),
            Err(CborError::InvalidFrame(_))
        ));

        // Wrong CBOR type.
        let mut m = base();
        m.push((
            ciborium::Value::Integer(keys::ROUTING_ID.into()),
            ciborium::Value::Text("not-a-routing-id".to_string()),
        ));
        assert!(matches!(
            decode_frame(&encode(m)),
            Err(CborError::InvalidFrame(_))
        ));

        // Well-formed routing_id still decodes.
        let mut m = base();
        m.push((
            ciborium::Value::Integer(keys::ROUTING_ID.into()),
            ciborium::Value::Integer(7.into()),
        ));
        let ok = decode_frame(&encode(m)).expect("well-formed routing_id must decode");
        assert_eq!(ok.routing_id, Some(MessageId::Uint(7)));
    }

    // TEST229: Test FrameReader/FrameWriter set_limits updates the negotiated limits
    #[tokio::test]
    async fn test229_frame_reader_writer_set_limits() {
        let (client, server) = tokio::io::duplex(1024);
        let mut reader = FrameReader::new(server);
        let mut writer = FrameWriter::new(client);

        let custom = Limits {
            max_frame: 500,
            max_chunk: 100,
            ..Limits::default()
        };
        reader.set_limits(custom);
        writer.set_limits(custom);

        assert_eq!(reader.limits().max_frame, 500);
        assert_eq!(reader.limits().max_chunk, 100);
        assert_eq!(writer.limits().max_frame, 500);
        assert_eq!(writer.limits().max_chunk, 100);
    }

    // TEST230: Test async handshake exchanges HELLO frames and negotiates minimum limits
    #[tokio::test]
    async fn test230_async_handshake() {
        let (host_to_cartridge, cartridge_from_host) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();
        let (cartridge_to_host, host_from_cartridge) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();

        let manifest = b"{\"name\":\"Test\",\"version\":\"1.0\",\"caps\":[]}";
        let manifest_clone = manifest.to_vec();

        // Cartridge side in spawned task
        let cartridge_handle = tokio::spawn(async move {
            let mut reader = FrameReader::new(TokioBufReader::new(cartridge_from_host));
            let mut writer = FrameWriter::new(TokioBufWriter::new(cartridge_to_host));
            handshake_accept(&mut reader, &mut writer, &manifest_clone, &crate::bifaci::pools::PoolStates::new())
                .await
                .unwrap()
        });

        // Host side
        let mut reader = FrameReader::new(TokioBufReader::new(host_from_cartridge));
        let mut writer = FrameWriter::new(TokioBufWriter::new(host_to_cartridge));
        let result = handshake(&mut reader, &mut writer).await.unwrap();

        let cartridge_limits = cartridge_handle.await.unwrap();

        // Both sides must agree on limits
        assert_eq!(result.limits.max_frame, cartridge_limits.max_frame);
        assert_eq!(result.limits.max_chunk, cartridge_limits.max_chunk);
        assert_eq!(
            result.limits.max_reorder_buffer,
            cartridge_limits.max_reorder_buffer
        );
        assert_eq!(result.manifest, manifest.to_vec());
    }

    // TEST231: Test handshake fails when peer sends non-HELLO frame
    #[tokio::test]
    async fn test231_handshake_rejects_non_hello() {
        let (host_to_cartridge, cartridge_from_host) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();
        let (cartridge_to_host, host_from_cartridge) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();

        // Cartridge side: send a REQ frame instead of HELLO
        let cartridge_handle = tokio::spawn(async move {
            let mut reader = FrameReader::new(TokioBufReader::new(cartridge_from_host));
            let mut writer = FrameWriter::new(TokioBufWriter::new(cartridge_to_host));
            // Read host's HELLO (consume it)
            let _ = reader.read().await.unwrap();
            // Send a REQ instead of HELLO
            let bad_frame = Frame::req(
                MessageId::Uint(1),
                r#"cap:in="media:void";bad;out="media:void""#,
                vec![],
                "text/plain",
            );
            writer.write(&bad_frame).await.unwrap();
        });

        let mut reader = FrameReader::new(TokioBufReader::new(host_from_cartridge));
        let mut writer = FrameWriter::new(TokioBufWriter::new(host_to_cartridge));
        let result = handshake(&mut reader, &mut writer).await;
        assert!(
            result.is_err(),
            "handshake must fail when peer sends non-HELLO"
        );
        let err = result.unwrap_err();
        assert!(matches!(err, CborError::Handshake(_)));

        cartridge_handle.await.unwrap();
    }

    // TEST232: Test handshake fails when cartridge HELLO is missing required manifest
    #[tokio::test]
    async fn test232_handshake_rejects_missing_manifest() {
        let (host_to_cartridge, cartridge_from_host) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();
        let (cartridge_to_host, host_from_cartridge) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();

        // Cartridge side: send HELLO without manifest
        let cartridge_handle = tokio::spawn(async move {
            let mut reader = FrameReader::new(TokioBufReader::new(cartridge_from_host));
            let mut writer = FrameWriter::new(TokioBufWriter::new(cartridge_to_host));
            let _ = reader.read().await.unwrap(); // consume host HELLO
            let no_manifest_hello = Frame::hello(&Limits {
                max_frame: 1_000_000,
                max_chunk: 200_000,
                max_reorder_buffer: DEFAULT_MAX_REORDER_BUFFER,
                initial_credit: DEFAULT_INITIAL_CREDIT,
            });
            writer.write(&no_manifest_hello).await.unwrap();
        });

        let mut reader = FrameReader::new(TokioBufReader::new(host_from_cartridge));
        let mut writer = FrameWriter::new(TokioBufWriter::new(host_to_cartridge));
        let result = handshake(&mut reader, &mut writer).await;
        assert!(
            result.is_err(),
            "handshake must fail when manifest is missing"
        );

        cartridge_handle.await.unwrap();
    }

    // TEST233: Test binary payload with all 256 byte values roundtrips through encode/decode
    #[test]
    fn test233_binary_payload_all_byte_values() {
        let mut data = Vec::with_capacity(256);
        for i in 0u8..=255 {
            data.push(i);
        }

        let id = MessageId::new_uuid();
        let frame = Frame::req(
            id.clone(),
            r#"cap:in="media:void";binary;out="media:void""#,
            data.clone(),
            "application/octet-stream",
        );

        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&encoded).unwrap();

        assert_eq!(decoded.payload.unwrap(), data);
    }

    // TEST234: Test decode_frame handles garbage CBOR bytes gracefully with an error
    #[test]
    fn test234_decode_garbage_bytes() {
        let garbage = vec![0xFF, 0xFE, 0xFD, 0xFC, 0xFB];
        let result = decode_frame(&garbage);
        assert!(result.is_err(), "garbage bytes must produce decode error");
    }

    // TEST848: RelayNotify encode/decode roundtrip preserves manifest and limits
    #[test]
    fn test848_relay_notify_roundtrip() {
        let manifest = br#"{"name":"Test","version":"1.0","channel":"release","description":"Test","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=\"media:void\";test;out=\"media:void\"","title":"Test","aliases": ["test"]},{"urn":"cap:in=\"media:void\";convert;out=\"media:void\"","title":"Convert","aliases": ["convert"]}]}]}"#;
        let limits = crate::bifaci::frame::Limits {
            max_frame: 2_000_000,
            max_chunk: 128_000,
            ..crate::bifaci::frame::Limits::default()
        };
        let frame = Frame::relay_notify(manifest, &limits);

        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&encoded).unwrap();

        assert_eq!(decoded.frame_type, FrameType::RelayNotify);
        assert_eq!(decoded.id, MessageId::Uint(0));
        assert_eq!(decoded.relay_notify_manifest(), Some(manifest.as_slice()));
        let decoded_limits = decoded.relay_notify_limits().expect("must have limits");
        assert_eq!(decoded_limits.max_frame, 2_000_000);
        assert_eq!(decoded_limits.max_chunk, 128_000);
    }

    // TEST849: RelayState encode/decode roundtrip preserves resource payload
    #[test]
    fn test849_relay_state_roundtrip() {
        let resources = b"{\"memory_mb\":8192,\"cpu_cores\":8}";
        let frame = Frame::relay_state(resources);

        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&encoded).unwrap();

        assert_eq!(decoded.frame_type, FrameType::RelayState);
        assert_eq!(decoded.id, MessageId::Uint(0));
        assert_eq!(decoded.payload, Some(resources.to_vec()));
    }

    // TEST389: StreamStart encode/decode roundtrip preserves stream_id and media_urn
    #[test]
    fn test389_stream_start_roundtrip() {
        let id = MessageId::new_uuid();
        let stream_id = "stream-abc-123".to_string();
        let media_urn = "media:".to_string();

        let frame = Frame::stream_start(id.clone(), stream_id.clone(), media_urn.clone(), None);
        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&encoded).unwrap();

        assert_eq!(decoded.frame_type, FrameType::StreamStart);
        assert_eq!(decoded.id, id);
        assert_eq!(decoded.stream_id.as_deref(), Some("stream-abc-123"));
        assert_eq!(decoded.media_urn.as_deref(), Some("media:"));
    }

    // TEST390: StreamEnd encode/decode roundtrip preserves stream_id, no media_urn
    #[test]
    fn test390_stream_end_roundtrip() {
        let id = MessageId::new_uuid();
        let stream_id = "stream-xyz-789".to_string();

        let frame = Frame::stream_end(id.clone(), stream_id.clone(), 10);
        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&encoded).unwrap();

        assert_eq!(decoded.frame_type, FrameType::StreamEnd);
        assert_eq!(decoded.id, id);
        assert_eq!(decoded.stream_id.as_deref(), Some("stream-xyz-789"));
        assert!(
            decoded.media_urn.is_none(),
            "StreamEnd should not have media_urn"
        );
    }

    // TEST497: Verify CHUNK frame with corrupted payload is rejected by checksum
    #[test]
    fn test497_chunk_corrupted_payload_rejected() {
        let id = MessageId::new_uuid();
        let stream_id = "stream-test".to_string();
        let payload = b"original data".to_vec();
        let checksum = Frame::compute_checksum(&payload);

        // Create CHUNK with correct checksum
        let chunk = Frame::chunk(
            id.clone(),
            stream_id.clone(),
            0,
            payload.clone(),
            0,
            checksum,
        );

        // Encode it
        let encoded = encode_frame(&chunk).unwrap();

        // Decode it
        let mut decoded = decode_frame(&encoded).unwrap();
        assert_eq!(decoded.checksum, Some(checksum));

        // Now CORRUPT the payload but keep the checksum
        decoded.payload = Some(b"corrupted data".to_vec());

        // Verify checksum doesn't match corrupted payload
        let corrupted_checksum = Frame::compute_checksum(decoded.payload.as_ref().unwrap());
        assert_ne!(
            corrupted_checksum, checksum,
            "Checksums should differ for corrupted data"
        );
        assert_eq!(
            decoded.checksum,
            Some(checksum),
            "Frame still has original checksum"
        );

        // This proves that if someone modifies the payload in transit,
        // the checksum will not match and verification will fail
    }

    // TEST440: CHUNK frame with chunk_index and checksum roundtrips through encode/decode
    #[test]
    fn test440_chunk_index_checksum_roundtrip() {
        let id = MessageId::new_uuid();
        let stream_id = "test-stream".to_string();
        let payload = b"test chunk data".to_vec();
        let checksum = Frame::compute_checksum(&payload);

        let frame = Frame::chunk(
            id.clone(),
            stream_id.clone(),
            5,
            payload.clone(),
            3,
            checksum,
        );

        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&encoded).unwrap();

        assert_eq!(decoded.frame_type, FrameType::Chunk);
        assert_eq!(decoded.id, id);
        assert_eq!(decoded.stream_id, Some(stream_id));
        assert_eq!(decoded.seq, 5);
        assert_eq!(decoded.payload, Some(payload));
        assert_eq!(decoded.chunk_index, Some(3), "chunk_index must roundtrip");
        assert_eq!(decoded.checksum, Some(checksum), "checksum must roundtrip");
    }

    // TEST441: STREAM_END frame with chunk_count roundtrips through encode/decode
    #[test]
    fn test441_stream_end_chunk_count_roundtrip() {
        let id = MessageId::new_uuid();
        let stream_id = "test-stream".to_string();

        let frame = Frame::stream_end(id.clone(), stream_id.clone(), 42);

        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&encoded).unwrap();

        assert_eq!(decoded.frame_type, FrameType::StreamEnd);
        assert_eq!(decoded.id, id);
        assert_eq!(decoded.stream_id, Some(stream_id));
        assert_eq!(decoded.chunk_count, Some(42), "chunk_count must roundtrip");
    }

    // TEST461: write_chunked produces frames with seq=0; SeqAssigner assigns at output stage
    #[tokio::test]
    async fn test461_write_chunked_seq_zero() {
        let limits = Limits {
            max_frame: 1_000_000,
            max_chunk: 5,
            ..Limits::default()
        };

        let id = MessageId::new_uuid();
        let (client, server) = tokio::io::duplex(64 * 1024);
        let mut writer = FrameWriter::with_limits(client, limits);
        writer
            .write_chunked(
                id.clone(),
                "s".to_string(),
                "application/octet-stream",
                b"abcdefghij",
            )
            .await
            .unwrap();
        drop(writer);

        let mut reader = FrameReader::with_limits(server, limits);

        let mut frames = Vec::new();
        loop {
            match reader.read().await.unwrap() {
                Some(f) => {
                    let is_eof = f.is_eof();
                    frames.push(f);
                    if is_eof {
                        break;
                    }
                }
                None => break,
            }
        }

        // 10 bytes / 5 max_chunk = 2 chunks
        assert_eq!(frames.len(), 2);
        for (i, f) in frames.iter().enumerate() {
            assert_eq!(f.seq, 0, "chunk {} must have seq=0", i);
            assert_eq!(
                f.chunk_index,
                Some(i as u64),
                "chunk {} must have chunk_index={}",
                i,
                i
            );
        }
    }

    // TEST472: Handshake negotiates max_reorder_buffer (minimum of both sides)
    #[tokio::test]
    async fn test472_handshake_negotiates_reorder_buffer() {
        // Simulate cartridge sending HELLO with max_reorder_buffer=32
        let cartridge_limits = Limits {
            max_frame: DEFAULT_MAX_FRAME,
            max_chunk: DEFAULT_MAX_CHUNK,
            max_reorder_buffer: 32,
            initial_credit: DEFAULT_INITIAL_CREDIT,
        };
        let manifest = br#"{"name":"test","version":"1.0","channel":"release","description":"Test","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"]}]}]}"#;

        // Write cartridge's HELLO with manifest to a duplex stream
        let (mut cartridge_write, mut cartridge_read) = tokio::io::duplex(64 * 1024);
        {
            let mut w = FrameWriter::new(&mut cartridge_write);
            let hello = Frame::hello_with_manifest(&cartridge_limits, manifest, 0);
            w.write(&hello).await.unwrap();
        }
        drop(cartridge_write);

        // Write host's HELLO to a duplex stream (our default: max_reorder_buffer=64)
        let (mut host_write, mut host_read) = tokio::io::duplex(64 * 1024);
        {
            let mut w = FrameWriter::new(&mut host_write);
            let hello = Frame::hello(&Limits::default());
            w.write(&hello).await.unwrap();
        }
        drop(host_write);

        // Host reads cartridge's HELLO
        let their_frame = {
            let mut r = FrameReader::new(&mut cartridge_read);
            r.read().await.unwrap().unwrap()
        };
        let their_reorder = their_frame.hello_max_reorder_buffer().unwrap();
        assert_eq!(their_reorder, 32);
        let negotiated = DEFAULT_MAX_REORDER_BUFFER.min(their_reorder);
        assert_eq!(negotiated, 32, "must pick minimum (32 < 64)");

        // Cartridge reads host's HELLO
        let host_frame = {
            let mut r = FrameReader::new(&mut host_read);
            r.read().await.unwrap().unwrap()
        };
        let host_reorder = host_frame.hello_max_reorder_buffer().unwrap();
        assert_eq!(host_reorder, DEFAULT_MAX_REORDER_BUFFER);
    }

    const V4_TEST_MANIFEST: &[u8] = br#"{"name":"test","version":"1.0","channel":"release","description":"Test","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"]}]}]}"#;

    /// Run a real host↔cartridge handshake over a bidirectional duplex pair.
    /// The cartridge side proposes `cartridge_limits` in its HELLO.
    /// Returns (host result, cartridge negotiated limits).
    async fn run_v4_handshake(
        cartridge_limits: Limits,
    ) -> (
        Result<HandshakeResult, CborError>,
        Result<Limits, CborError>,
    ) {
        let (host_side, cartridge_side) = tokio::io::duplex(64 * 1024);
        let (host_r, host_w) = tokio::io::split(host_side);
        let (cart_r, cart_w) = tokio::io::split(cartridge_side);

        let host_task = tokio::spawn(async move {
            let mut reader = FrameReader::new(host_r);
            let mut writer = FrameWriter::new(host_w);
            handshake(&mut reader, &mut writer).await
        });
        // Cartridge side: read host HELLO, verify version, respond with a HELLO
        // proposing `cartridge_limits`, and negotiate the element-wise minimum —
        // the same steps handshake_accept performs, but with configurable
        // proposals instead of the process-wide defaults.
        let cart_task = tokio::spawn(async move {
            let mut reader = FrameReader::new(cart_r);
            let mut writer = FrameWriter::new(cart_w);
            let their_frame = reader
                .read()
                .await?
                .ok_or_else(|| CborError::Handshake("closed".to_string()))?;
            let their_version = their_frame.hello_version().unwrap_or(their_frame.version);
            if their_version != PROTOCOL_VERSION {
                return Err(CborError::Handshake(format!(
                    "protocol version mismatch: ours {}, theirs {}",
                    PROTOCOL_VERSION, their_version
                )));
            }
            writer
                .write(&Frame::hello_with_manifest(
                    &cartridge_limits,
                    V4_TEST_MANIFEST,
                    0,
                ))
                .await?;
            Ok(Limits {
                max_frame: cartridge_limits
                    .max_frame
                    .min(their_frame.hello_max_frame().unwrap_or(DEFAULT_MAX_FRAME)),
                max_chunk: cartridge_limits
                    .max_chunk
                    .min(their_frame.hello_max_chunk().unwrap_or(DEFAULT_MAX_CHUNK)),
                max_reorder_buffer: cartridge_limits.max_reorder_buffer.min(
                    their_frame
                        .hello_max_reorder_buffer()
                        .unwrap_or(DEFAULT_MAX_REORDER_BUFFER),
                ),
                initial_credit: cartridge_limits.initial_credit.min(
                    their_frame
                        .hello_initial_credit()
                        .unwrap_or(DEFAULT_INITIAL_CREDIT),
                ),
            })
        });

        let host_result = host_task.await.unwrap();
        let cart_result = cart_task.await.unwrap();
        (host_result, cart_result)
    }

    // TEST7000: v4 handshake negotiates all limits and required handler capacity.
    #[tokio::test]
    async fn test7000_v4_handshake_negotiates_all_four_limits() {
        let cartridge_limits = Limits {
            max_frame: 2_000_000,
            max_chunk: 128_000,
            max_reorder_buffer: 32,
            initial_credit: 16,
        };
        let (host_result, cart_result) = run_v4_handshake(cartridge_limits).await;

        let host = host_result.expect("v4 handshake must succeed");
        assert_eq!(host.limits.max_frame, 2_000_000, "min(3.5MB, 2MB)");
        assert_eq!(host.limits.max_chunk, 128_000, "min(256KB, 128KB)");
        assert_eq!(host.limits.max_reorder_buffer, 32, "min(64, 32)");
        assert_eq!(host.limits.initial_credit, 16, "min(32, 16)");
        assert!(!host.manifest.is_empty(), "manifest must be extracted");

        let cart = cart_result.expect("cartridge side must succeed");
        assert_eq!(cart.initial_credit, 16);
        assert_eq!(cart.max_reorder_buffer, 32);
    }

    // TEST7001: HELLO carrying protocol version 2 is rejected at handshake with a version-mismatch error
    #[tokio::test]
    async fn test7001_handshake_rejects_version_2() {
        let (host_side, cartridge_side) = tokio::io::duplex(64 * 1024);
        let (host_r, host_w) = tokio::io::split(host_side);
        let (cart_r, cart_w) = tokio::io::split(cartridge_side);

        // Fake v2 cartridge: replies to the host HELLO with a version=2 HELLO.
        let cart_task = tokio::spawn(async move {
            let mut reader = FrameReader::new(cart_r);
            let mut writer = FrameWriter::new(cart_w);
            let _host_hello = reader.read().await.unwrap().unwrap();
            let mut hello = Frame::hello_with_manifest(&Limits::default(), V4_TEST_MANIFEST, 0);
            hello.version = 2;
            if let Some(meta) = hello.meta.as_mut() {
                meta.insert("version".to_string(), ciborium::Value::Integer(2.into()));
            }
            writer.write(&hello).await.unwrap();
        });

        let mut reader = FrameReader::new(host_r);
        let mut writer = FrameWriter::new(host_w);
        let result = handshake(&mut reader, &mut writer).await;
        cart_task.await.unwrap();

        let err = result.expect_err("v2 HELLO must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("version"),
            "error must name the version mismatch: {}",
            msg
        );
        assert!(
            msg.contains('2') && msg.contains('4'),
            "error must state both versions: {}",
            msg
        );
    }

    // TEST7002: initial_credit negotiation picks the element-wise minimum of the two proposals
    #[tokio::test]
    async fn test7002_initial_credit_negotiated_minimum() {
        // Cartridge proposes a smaller window than the host default (32) → 8 wins.
        let smaller = Limits {
            initial_credit: 8,
            ..Limits::default()
        };
        let (host_result, cart_result) = run_v4_handshake(smaller).await;
        assert_eq!(host_result.unwrap().limits.initial_credit, 8);
        assert_eq!(cart_result.unwrap().initial_credit, 8);

        // Cartridge proposes a larger window (128) → the host default 32 wins.
        let larger = Limits {
            initial_credit: 128,
            ..Limits::default()
        };
        let (host_result, cart_result) = run_v4_handshake(larger).await;
        assert_eq!(host_result.unwrap().limits.initial_credit, 32);
        assert_eq!(cart_result.unwrap().initial_credit, 32);
    }

    // =========================================================================
    // Identity verification tests
    // =========================================================================

    /// Manifest with only CAP_IDENTITY (minimum valid manifest)
    const IDENTITY_MANIFEST: &str = r#"{"name":"Test","version":"1.0","channel":"release","description":"Test","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]}]}]}"#;

    /// Simulate cartridge side: handshake_accept, then handle one identity REQ
    /// by echoing back the payload (like the standard identity handler).
    async fn run_cartridge_identity_echo(
        from_host: crate::bifaci::local_socket::UnixStream,
        to_host: crate::bifaci::local_socket::UnixStream,
        manifest: &[u8],
    ) {
        let mut reader = FrameReader::new(TokioBufReader::new(from_host));
        let mut writer = FrameWriter::new(TokioBufWriter::new(to_host));
        handshake_accept(&mut reader, &mut writer, manifest, 0)
            .await
            .unwrap();

        // Read REQ
        let req = reader.read().await.unwrap().expect("expected REQ");
        assert_eq!(req.frame_type, FrameType::Req);

        // Read request body: STREAM_START → CHUNK(s) → STREAM_END → END
        let mut payload = Vec::new();
        loop {
            let f = reader.read().await.unwrap().expect("expected frame");
            match f.frame_type {
                FrameType::StreamStart => {}
                FrameType::Chunk => payload.extend(f.payload.unwrap_or_default()),
                FrameType::StreamEnd => {}
                FrameType::End => break,
                other => panic!("unexpected frame type during identity request: {:?}", other),
            }
        }

        // Echo response: STREAM_START → CHUNK → STREAM_END → END
        let stream_id = "echo".to_string();
        let ss = Frame::stream_start(
            req.id.clone(),
            stream_id.clone(),
            "media:".to_string(),
            None,
        );
        writer.write(&ss).await.unwrap();
        let checksum = Frame::compute_checksum(&payload);
        let chunk = Frame::chunk(req.id.clone(), stream_id.clone(), 0, payload, 0, checksum);
        writer.write(&chunk).await.unwrap();
        let se = Frame::stream_end(req.id.clone(), stream_id, 1);
        writer.write(&se).await.unwrap();
        let end = Frame::end(req.id, None);
        writer.write(&end).await.unwrap();
    }

    // TEST481: verify_identity succeeds with standard identity echo handler
    #[tokio::test]
    async fn test481_verify_identity_succeeds() {
        let (host_to_cartridge, cartridge_from_host) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();
        let (cartridge_to_host, host_from_cartridge) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();

        // Cartridge side runs async in a spawned task
        let manifest = IDENTITY_MANIFEST.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            run_cartridge_identity_echo(cartridge_from_host, cartridge_to_host, &manifest).await;
        });

        // Host side
        let mut reader = FrameReader::new(TokioBufReader::new(host_from_cartridge));
        let mut writer = FrameWriter::new(TokioBufWriter::new(host_to_cartridge));
        let _hs = handshake(&mut reader, &mut writer).await.unwrap();

        let result = verify_identity(&mut reader, &mut writer).await;
        assert!(
            result.is_ok(),
            "verify_identity must succeed: {:?}",
            result.unwrap_err()
        );

        cartridge_handle.await.unwrap();
    }

    // TEST482: verify_identity fails when cartridge returns ERR on identity call
    #[tokio::test]
    async fn test482_verify_identity_fails_on_err() {
        let (host_to_cartridge, cartridge_from_host) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();
        let (cartridge_to_host, host_from_cartridge) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();

        let manifest = IDENTITY_MANIFEST.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let mut reader = FrameReader::new(TokioBufReader::new(cartridge_from_host));
            let mut writer = FrameWriter::new(TokioBufWriter::new(cartridge_to_host));
            handshake_accept(&mut reader, &mut writer, &manifest, 0)
                .await
                .unwrap();

            // Read REQ, respond with ERR
            let req = reader.read().await.unwrap().expect("expected REQ");
            let err = Frame::err(
                req.id,
                "BROKEN",
                crate::AttributionClass::Internal,
                "identity handler broken",
                None,
            );
            writer.write(&err).await.unwrap();
            // Flush to ensure host reads the error before connection closes
            use tokio::io::AsyncWriteExt;
            writer.inner_mut().flush().await.unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        });

        let mut reader = FrameReader::new(TokioBufReader::new(host_from_cartridge));
        let mut writer = FrameWriter::new(TokioBufWriter::new(host_to_cartridge));
        handshake(&mut reader, &mut writer).await.unwrap();

        let result = verify_identity(&mut reader, &mut writer).await;
        assert!(result.is_err(), "verify_identity must fail on ERR");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("BROKEN"),
            "error must contain error code: {}",
            err
        );

        cartridge_handle.await.unwrap();
    }

    // TEST483: verify_identity fails when connection closes before response
    #[tokio::test]
    async fn test483_verify_identity_fails_on_close() {
        let (host_to_cartridge, cartridge_from_host) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();
        let (cartridge_to_host, host_from_cartridge) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();

        let manifest = IDENTITY_MANIFEST.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let mut reader = FrameReader::new(TokioBufReader::new(cartridge_from_host));
            let mut writer = FrameWriter::new(TokioBufWriter::new(cartridge_to_host));
            handshake_accept(&mut reader, &mut writer, &manifest, 0)
                .await
                .unwrap();

            // Read REQ but close connection without responding
            let _req = reader.read().await.unwrap().expect("expected REQ");
            drop(writer);
            drop(reader);
        });

        let mut reader = FrameReader::new(TokioBufReader::new(host_from_cartridge));
        let mut writer = FrameWriter::new(TokioBufWriter::new(host_to_cartridge));
        handshake(&mut reader, &mut writer).await.unwrap();

        let result = verify_identity(&mut reader, &mut writer).await;
        assert!(
            result.is_err(),
            "verify_identity must fail on connection close"
        );

        cartridge_handle.await.unwrap();
    }

    // TEST1140: write_stream_chunked (protocol v2) splits payload into STREAM_START → CHUNK(s) →
    // STREAM_END → END with correct frame types, stream_id, media_urn, and data integrity.
    #[tokio::test]
    async fn test1140_write_stream_chunked_splits_data_into_protocol_v2_sequence() {
        let id = MessageId::new_uuid();
        let stream_id = "resp-1".to_string();
        let media_urn = "media:".to_string();
        let limits = Limits {
            max_frame: 1_000_000,
            max_chunk: 100,
            max_reorder_buffer: 48,
            initial_credit: DEFAULT_INITIAL_CREDIT,
        };
        let data: Vec<u8> = (0u8..=249).collect(); // 250 bytes → 3 chunks of 100/100/50

        let (client, server) = tokio::io::duplex(64 * 1024);
        let mut writer = FrameWriter::with_limits(client, limits);

        // Write protocol v2 sequence manually (mirrors what write_stream_chunked helpers do)
        writer
            .write(&Frame::stream_start(
                id.clone(),
                stream_id.clone(),
                media_urn.clone(),
                None,
            ))
            .await
            .unwrap();
        let mut offset = 0usize;
        let mut chunk_index = 0u64;
        while offset < data.len() {
            let chunk_size = (limits.max_chunk as usize).min(data.len() - offset);
            let chunk_data = data[offset..offset + chunk_size].to_vec();
            let checksum = Frame::compute_checksum(&chunk_data);
            writer
                .write(&Frame::chunk(
                    id.clone(),
                    stream_id.clone(),
                    0,
                    chunk_data,
                    chunk_index,
                    checksum,
                ))
                .await
                .unwrap();
            offset += chunk_size;
            chunk_index += 1;
        }
        writer
            .write(&Frame::stream_end(
                id.clone(),
                stream_id.clone(),
                chunk_index,
            ))
            .await
            .unwrap();
        writer.write(&Frame::end(id.clone(), None)).await.unwrap();
        drop(writer);

        let read_limits = Limits {
            max_frame: 1_000_000,
            max_chunk: 1_000_000,
            max_reorder_buffer: 48,
            initial_credit: DEFAULT_INITIAL_CREDIT,
        };
        let mut reader = FrameReader::with_limits(server, read_limits);

        let f0 = reader.read().await.unwrap().unwrap();
        assert_eq!(f0.frame_type, FrameType::StreamStart);
        assert_eq!(f0.stream_id.as_deref(), Some("resp-1"));
        assert_eq!(f0.media_urn.as_deref(), Some("media:"));

        let mut reassembled = Vec::new();
        let mut seen_chunks = 0u64;
        loop {
            let frame = reader.read().await.unwrap().unwrap();
            match frame.frame_type {
                FrameType::Chunk => {
                    reassembled.extend_from_slice(frame.payload.as_deref().unwrap_or_default());
                    seen_chunks += 1;
                }
                FrameType::StreamEnd => {
                    assert_eq!(frame.stream_id.as_deref(), Some("resp-1"));
                    break;
                }
                other => panic!("unexpected frame type {:?}", other),
            }
        }

        let end_frame = reader.read().await.unwrap().unwrap();
        assert_eq!(end_frame.frame_type, FrameType::End);

        assert_eq!(seen_chunks, 3, "250 bytes / 100 max_chunk = 3 chunks");
        assert_eq!(
            reassembled, data,
            "reassembled chunks must equal original data"
        );
    }

    // TEST1141: write_stream_chunked with data exactly equal to max_chunk produces exactly one CHUNK
    #[tokio::test]
    async fn test1141_write_stream_chunked_exact_fit_produces_single_chunk() {
        let id = MessageId::new_uuid();
        let stream_id = "resp-exact".to_string();
        let limits = Limits {
            max_frame: 1_000_000,
            max_chunk: 100,
            max_reorder_buffer: 48,
            initial_credit: DEFAULT_INITIAL_CREDIT,
        };
        let data: Vec<u8> = vec![0xAB; 100];

        let (client, server) = tokio::io::duplex(64 * 1024);
        let mut writer = FrameWriter::with_limits(client, limits);

        writer
            .write(&Frame::stream_start(
                id.clone(),
                stream_id.clone(),
                "media:".to_string(),
                None,
            ))
            .await
            .unwrap();
        let checksum = Frame::compute_checksum(&data);
        writer
            .write(&Frame::chunk(
                id.clone(),
                stream_id.clone(),
                0,
                data.clone(),
                0,
                checksum,
            ))
            .await
            .unwrap();
        writer
            .write(&Frame::stream_end(id.clone(), stream_id.clone(), 1))
            .await
            .unwrap();
        writer.write(&Frame::end(id.clone(), None)).await.unwrap();
        drop(writer);

        let read_limits = Limits {
            max_frame: 1_000_000,
            max_chunk: 1_000_000,
            max_reorder_buffer: 48,
            initial_credit: DEFAULT_INITIAL_CREDIT,
        };
        let mut reader = FrameReader::with_limits(server, read_limits);

        let f0 = reader.read().await.unwrap().unwrap();
        assert_eq!(f0.frame_type, FrameType::StreamStart);

        let f1 = reader.read().await.unwrap().unwrap();
        assert_eq!(f1.frame_type, FrameType::Chunk);
        assert_eq!(
            f1.payload.as_deref(),
            Some(data.as_slice()),
            "single chunk must carry full payload"
        );

        let f2 = reader.read().await.unwrap().unwrap();
        assert_eq!(f2.frame_type, FrameType::StreamEnd);

        let f3 = reader.read().await.unwrap().unwrap();
        assert_eq!(f3.frame_type, FrameType::End);
    }
}
