//! Shared stream I/O operations for cap execution.
//!
//! These functions handle the bifaci protocol's CBOR transport layer:
//! sending input streams to cartridges and collecting/decoding their
//! responses. Used by both the machfab engine (capdag_service) and
//! the capdag CLI orchestrator executor.
//!
//! The key invariant: node data between caps is stored as raw bytes
//! (unwrapped from CBOR transport). Sequence-mode output is stored
//! as an RFC 8742 CBOR sequence where each item's CBOR Bytes/Text
//! wrapper has been unwrapped to raw bytes, then re-encoded as
//! CBOR Bytes for self-delimiting boundaries.

use crate::bifaci::frame::{Frame, FrameType, MessageId};
use crate::bifaci::relay_switch::RelaySwitch;
use crate::orchestrator::executor::CapProgressFn;
use crate::planner::StepToken;
use crate::StreamMeta;
use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Error)]
pub enum StreamIoError {
    #[error("Stream I/O error: {0}")]
    Transport(String),

    #[error("CBOR encoding error: {0}")]
    CborEncode(String),

    #[error("CBOR decoding error: {0}")]
    CborDecode(String),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Protocol error: expected Bytes or Text in CBOR transport at item {index}, got {description}")]
    UnexpectedCborType { index: usize, description: String },

    /// Cap-level failure: the cartridge returned END without success, ERR frame,
    /// or the response channel closed without an END. `cap_urn` identifies the
    /// failing cap; `details` carries the cartridge's error message or the
    /// protocol violation detail. `code` and `class` carry the failure identity
    /// DECLARED at the emit source (docs/failure-taxonomy.md): the ERR frame's
    /// code + class when one arrived, `None` + `Internal` for engine-detected
    /// protocol violations. `arg_urn` is the emit source's argument
    /// attribution — the ERR frame's `arg_urn` when it carried one, `None`
    /// otherwise (never inferred here).
    #[error("Cap '{cap_urn}' failed: {details}")]
    Terminal {
        cap_urn: String,
        code: Option<String>,
        class: crate::failure::AttributionClass,
        details: String,
        arg_urn: Option<String>,
    },

    /// Writer failure — the `IncrementalWriter` returned an error while
    /// persisting chunk data.
    #[error("Writer error: {0}")]
    Writer(String),

    /// A cap emitted an output STREAM_START whose media URN violates its
    /// declared effect contract: the emission does not satisfy
    /// `CapUrn::is_conformant_runtime_output` for the runtime input the
    /// engine fed it (the cap's declared main-input URN — spec 13.2 labels
    /// every stream the orchestrator sends with it). The plan was built on
    /// the effect promise, so a violating emission fails hard HERE — at
    /// receipt, before any relabel or forward can mask it — attributed
    /// Internal (the cartridge broke its own declared contract; not a user
    /// input problem).
    #[error(
        "Cap '{cap_urn}' violated its effect contract: effect={effect}, runtime input '{runtime_input}', expected output '{expected}', emitted '{actual}'"
    )]
    EffectContract {
        cap_urn: String,
        effect: String,
        runtime_input: String,
        expected: String,
        actual: String,
    },

    /// A cap emitted an output STREAM_START whose shape violates its
    /// declared stream contract (15.2 §Streaming Contracts): it opened an
    /// UNBOUNDED stream from an output declared `streaming: false` (every
    /// stream bounded — the promise the executor's hop rule and every
    /// whole-value consumer downstream were built on), or it emitted in a
    /// cardinality mode other than the declared `is_sequence`. Fails hard at
    /// receipt, attributed Internal: the cartridge broke its own declaration.
    #[error(
        "Cap '{cap_urn}' violated its stream contract: declared is_sequence={declared_is_sequence} streaming={declared_streaming}, emitted is_sequence={emitted_is_sequence} unbounded={emitted_unbounded}"
    )]
    StreamContract {
        cap_urn: String,
        declared_is_sequence: bool,
        declared_streaming: bool,
        emitted_is_sequence: bool,
        emitted_unbounded: bool,
    },
}

/// The declared shape of a cap's output stream — `CapOutput::is_sequence`
/// and `CapOutput::streaming` — carried to the receipt points so every
/// STREAM_START is audited against the definition the plan was built on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputContract {
    pub is_sequence: bool,
    pub streaming: bool,
}

impl OutputContract {
    /// The contract of a cap definition's output. A cap with no output
    /// (`out=media:void`) has nothing to audit: both flags are false, and an
    /// emission from it is caught by the effect audit.
    pub fn of(cap: &crate::Cap) -> Self {
        let (_, streaming) = cap.streaming_shape();
        let (_, is_sequence) = cap.sequence_shape();
        Self { is_sequence, streaming }
    }
}

// =============================================================================
// Effect audit
// =============================================================================

/// Audits a cap's emitted output STREAM_START media against its declared
/// effect contract, at the frame boundary where the engine receives it.
///
/// The audit base is the cap's declared main-input media URN (`in=`): the
/// orchestrator labels every stream it feeds a cap with the declared arg URN
/// (spec 13.2 — heads in `send_group_input`, hops via the forward relabel),
/// so that label is the runtime input the cap actually saw and the only
/// basis its emission can honestly be derived from. The decision itself is
/// `CapUrn::is_conformant_runtime_output` — the ONE effect-conformance
/// predicate — never re-derived here.
pub struct EffectAudit {
    cap_urn_str: String,
    cap: crate::CapUrn,
    runtime_input: crate::MediaUrn,
    /// The declared output shape — audited on the same STREAM_START as the
    /// effect, so a cap lying about boundedness or cardinality fails at the
    /// same boundary as a cap lying about its output type.
    contract: OutputContract,
}

impl EffectAudit {
    /// Build the audit for one cap invocation. Fails when the cap URN does
    /// not parse or its declared `in=` is not a valid media URN — engine-side
    /// inconsistencies that must fail hard, not skip the audit.
    pub fn new(cap_urn: &str, contract: OutputContract) -> Result<Self, StreamIoError> {
        let cap = crate::CapUrn::from_string(cap_urn).map_err(|e| {
            StreamIoError::Protocol(format!(
                "effect audit: cap URN '{}' does not parse: {}",
                cap_urn, e
            ))
        })?;
        let runtime_input = cap.in_media_urn().map_err(|e| {
            StreamIoError::Protocol(format!(
                "effect audit: cap '{}' declared input is not a valid media URN: {}",
                cap_urn, e
            ))
        })?;
        Ok(Self {
            cap_urn_str: cap_urn.to_string(),
            cap,
            runtime_input,
            contract,
        })
    }

    /// The declared output contract this audit enforces.
    pub fn contract(&self) -> OutputContract {
        self.contract
    }

    /// Audit one emitted output STREAM_START frame — its media label against
    /// the effect contract, its `is_sequence` and `unbounded` flags against
    /// the declared output shape. Both must hold; the effect is checked first
    /// (an emission of the wrong TYPE is the graver lie).
    pub fn audit_frame(&self, frame: &crate::bifaci::frame::Frame) -> Result<(), StreamIoError> {
        self.audit(frame.media_urn.as_deref())?;
        // An emission that does not state its cardinality mode is an empty
        // stream (no items were ever written): it can be neither a lie about
        // sequence mode nor unbounded in any consequential way — but an
        // unbounded flag on it is still a contract question.
        let emitted_is_sequence = frame.is_sequence.unwrap_or(self.contract.is_sequence);
        let emitted_unbounded = frame.is_unbounded();
        let sequence_lie = emitted_is_sequence != self.contract.is_sequence;
        let boundedness_lie = emitted_unbounded && !self.contract.streaming;
        if sequence_lie || boundedness_lie {
            return Err(StreamIoError::StreamContract {
                cap_urn: self.cap_urn_str.clone(),
                declared_is_sequence: self.contract.is_sequence,
                declared_streaming: self.contract.streaming,
                emitted_is_sequence,
                emitted_unbounded,
            });
        }
        Ok(())
    }

    /// Audit one emitted output STREAM_START label. `Ok(())` iff the
    /// emission satisfies the declared effect contract. An unlabeled or
    /// unparseable emission, and an audit that cannot run (inference
    /// impossible — an upstream inconsistency), are `Protocol` errors; a
    /// clean contract violation is `EffectContract`. Both fail hard.
    pub fn audit(&self, emitted: Option<&str>) -> Result<(), StreamIoError> {
        let emitted_str = match emitted {
            Some(s) if !s.is_empty() => s,
            _ => {
                return Err(StreamIoError::Protocol(format!(
                    "cap '{}' emitted an output STREAM_START without a media URN \
                     label — every output stream must be labeled",
                    self.cap_urn_str
                )));
            }
        };
        let emitted_urn = crate::MediaUrn::from_string(emitted_str).map_err(|e| {
            StreamIoError::Protocol(format!(
                "cap '{}' emitted an output STREAM_START with invalid media URN '{}': {}",
                self.cap_urn_str, emitted_str, e
            ))
        })?;
        let conformant = self
            .cap
            .is_conformant_runtime_output(&self.runtime_input, &emitted_urn)
            .map_err(|e| {
                StreamIoError::Protocol(format!(
                    "effect audit for cap '{}' could not run: {}",
                    self.cap_urn_str, e
                ))
            })?;
        if conformant {
            return Ok(());
        }
        // The predicate ran, so the inference is computable; recompute it
        // only to NAME the expectation in the failure (diagnostics — the
        // predicate above is the sole decision-maker).
        let expected = self
            .cap
            .infer_runtime_output_media(&self.runtime_input)
            .map(|m| m.to_string())
            .unwrap_or_else(|e| format!("<uninferable: {}>", e));
        Err(StreamIoError::EffectContract {
            cap_urn: self.cap_urn_str.clone(),
            effect: self.cap.effect().as_str().to_string(),
            runtime_input: self.runtime_input.to_string(),
            expected,
            actual: emitted_str.to_string(),
        })
    }
}

// =============================================================================
// Activity tracking
// =============================================================================

/// Pipeline-level stall warning threshold in seconds.
///
/// If no progress LOG frame arrives from ANY body in the entire pipeline
/// for this duration, the runtime emits a one-shot warning ("no progress
/// from any body for Ns — continuing to wait. Use Cancel to abort.")
/// and keeps waiting. The pipeline is NOT aborted automatically —
/// long-running caps (model loads, vision/LLM inference, large audio
/// transcription) legitimately exceed this threshold, and aborting them
/// produced false negatives on every honest long workload. Cancellation
/// is the user's call, via the explicit cancel-task path.
///
/// The constant retains the historical "stall timeout" name (and
/// duration) so existing telemetry and log-grep dashboards keyed on
/// "120s" continue to land on the same event. Only the runtime's
/// reaction changed (warn-once vs abort).
pub const PIPELINE_STALL_TIMEOUT_SECS: u64 = 120;

/// Per-cap activity timer used by `collect_terminal_output`.
///
/// Tracks time since the last activity frame from a cap. A "queued" LOG frame
/// pauses the timer (the cartridge has confirmed receipt but is waiting for a
/// handler slot), and any other LOG, progress, or data frame unpauses it and
/// resets the clock.
pub struct ActivityTimer {
    last_activity: Instant,
    paused: bool,
    timeout: Duration,
}

impl ActivityTimer {
    pub fn new(timeout_secs: u64) -> Self {
        Self {
            last_activity: Instant::now(),
            paused: false,
            timeout: Duration::from_secs(timeout_secs),
        }
    }

    /// Record activity and resume if paused.
    pub fn touch(&mut self) {
        self.last_activity = Instant::now();
        self.paused = false;
    }

    /// Pause the timeout (request is queued, no progress expected).
    pub fn pause(&mut self) {
        self.paused = true;
    }

    /// Handle a LOG frame's level:
    /// - `"queued"` → pause (request waiting in cartridge queue)
    /// - anything else → touch (handler active, reset timer)
    pub fn handle_log_level(&mut self, level: &str) {
        match level {
            "queued" => self.pause(),
            _ => self.touch(),
        }
    }

    /// Check if the timeout has been exceeded. Returns false when paused.
    pub fn is_expired(&self) -> bool {
        !self.paused && self.last_activity.elapsed() > self.timeout
    }
}

/// Shared timestamp for pipeline-level stall detection.
///
/// Stores `Instant::elapsed().as_millis()` of the last progress event.
/// Updated by any body's progress callback. Read by the watchdog task.
pub struct PipelineProgressTracker {
    epoch: Instant,
    last_progress_ms: AtomicU64,
}

impl Default for PipelineProgressTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineProgressTracker {
    pub fn new() -> Self {
        let epoch = Instant::now();
        Self {
            epoch,
            last_progress_ms: AtomicU64::new(epoch.elapsed().as_millis() as u64),
        }
    }

    /// Record that progress was observed.
    pub fn touch(&self) {
        self.last_progress_ms
            .store(self.epoch.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    /// Check if the stall timeout has been exceeded.
    pub fn is_stalled(&self) -> bool {
        let last_ms = self.last_progress_ms.load(Ordering::Relaxed);
        let now_ms = self.epoch.elapsed().as_millis() as u64;
        now_ms.saturating_sub(last_ms) > PIPELINE_STALL_TIMEOUT_SECS * 1000
    }
}

// =============================================================================
// Logging callback
// =============================================================================

/// One transient, non-progress diagnostic emitted while a pipeline is live.
/// The stable step token is the graph address; `body_index` only identifies a
/// ForEach item within that step and is never used as a node address.
#[derive(Debug, Clone)]
pub struct PipelineLogRecord {
    pub step_token_id: Option<StepToken>,
    pub cap_urn: Option<String>,
    pub level: String,
    pub attribution_class: crate::failure::AttributionClass,
    pub message: String,
    pub meta: Option<StreamMeta>,
    pub body_index: Option<usize>,
    pub arg_urn: Option<String>,
}

impl PipelineLogRecord {
    pub fn attributed(
        step_token_id: &StepToken,
        cap_urn: impl Into<String>,
        level: impl Into<String>,
        attribution_class: crate::failure::AttributionClass,
        message: impl Into<String>,
    ) -> Self {
        Self {
            step_token_id: Some(step_token_id.clone()),
            cap_urn: Some(cap_urn.into()),
            level: level.into(),
            attribution_class,
            message: message.into(),
            meta: None,
            body_index: None,
            arg_urn: None,
        }
    }
}

pub type PipelineLogFn = Arc<dyn Fn(PipelineLogRecord) + Send + Sync>;

fn emit_pipeline_log(
    log_fn: Option<&PipelineLogFn>,
    step_token_id: &StepToken,
    cap_urn: &str,
    level: &str,
    attribution_class: crate::failure::AttributionClass,
    message: &str,
    meta: Option<StreamMeta>,
    body_index: Option<usize>,
    arg_urn: Option<String>,
) {
    if let Some(log_fn) = log_fn {
        let mut record = PipelineLogRecord::attributed(
            step_token_id,
            cap_urn,
            level,
            attribution_class,
            message,
        );
        record.meta = meta;
        record.body_index = body_index;
        record.arg_urn = arg_urn;
        log_fn(record);
    }
}

// =============================================================================
// Credit plumbing (flow control, L9–L15)
// =============================================================================

/// Grants credit back to the producing cartridge for consumed response
/// chunks. Arguments: `(stream_id, chunks_consumed)`. Implementations send a
/// CREDIT frame toward the cartridge (via the switch for the engine path,
/// via the forwarding channel for pipelined execution).
pub type CreditGrantFn = Arc<dyn Fn(Option<String>, u64) + Send + Sync>;

/// The receive-side credit plumbing for a collect/forward loop (L10/L14):
/// `router` delivers inbound CREDIT frames (the cartridge crediting OUR input
/// streams) to the engine-side send gates; `grant` replenishes the
/// cartridge's output window as response chunks are consumed; `batch` is the
/// grant batching threshold (chunks consumed per CREDIT frame sent).
pub struct CreditPlumbing {
    pub router: crate::bifaci::credit::CreditRouter,
    pub grant: CreditGrantFn,
    pub batch: u64,
}

// =============================================================================
// Terminal output meta
// =============================================================================

/// Metadata collected from the terminal cap's output stream.
///
/// `stream_meta` is set from the STREAM_START frame.
/// `item_metas` collects per-item meta from each CHUNK frame that starts a new
/// sequence item (used by ForEach to propagate per-item provenance).
#[derive(Debug, Clone, Default)]
pub struct TerminalMeta {
    pub stream_meta: Option<StreamMeta>,
    pub item_metas: Vec<StreamMeta>,
}

// =============================================================================
// Incremental writer
// =============================================================================

/// Trait for streaming terminal output to disk as it arrives.
///
/// Implementations decide storage policy (blob vs sequence, provenance
/// sidecars, etc.). The collect loop calls these in order:
/// `on_stream_start` → 0..N `on_chunk_payload` → `on_stream_end`.
#[async_trait]
pub trait IncrementalWriter: Send {
    /// Called on STREAM_START. `is_sequence` mirrors the wire flag;
    /// `media_urn` is the stream's declared media URN; `meta` is the
    /// STREAM_START frame's meta map; `stream_id` is the wire stream id.
    async fn on_stream_start(
        &mut self,
        is_sequence: Option<bool>,
        media_urn: &str,
        meta: Option<StreamMeta>,
        stream_id: Option<String>,
    ) -> Result<(), StreamIoError>;

    /// Called on each CHUNK. `payload` is the raw CBOR payload of the chunk;
    /// `meta` is the CHUNK frame's meta (set on first chunk of each sequence
    /// item; None otherwise).
    async fn on_chunk_payload(
        &mut self,
        payload: &[u8],
        meta: Option<StreamMeta>,
    ) -> Result<(), StreamIoError>;

    /// Called on STREAM_END. Flushes buffered state.
    async fn on_stream_end(&mut self) -> Result<(), StreamIoError>;

    /// Consume the writer and return its persisted result (saved paths, byte counts,
    /// cardinality, per-item/stream meta). Called once, after the terminal, by whoever
    /// owns the writer. A multi-sink segment run creates one writer per persisted sink
    /// via a [`SegmentWriterFactory`] and finalises each here.
    fn finish(self: Box<Self>) -> super::execute_plan::WriterResult;
}

/// Creates a fresh [`IncrementalWriter`] for a persisted terminal sink. The segment
/// executor calls it once per persisted sink, passing the sink's node id and, inside a
/// ForEach body, the boundary's stable step token plus local item index. The engine
/// plugs in a factory bound to the run's artifact directory; the reference/in-memory
/// path supplies none.
pub type SegmentWriterFactory = dyn Fn(&str, Option<super::execute_plan::ForEachBodyCoordinate>) -> Box<dyn IncrementalWriter>
    + Send
    + Sync;

/// Disk spool for an UNBOUNDED INTERMEDIATE at a chain split boundary.
///
/// A mandatory materialisation boundary (same-admission-domain bounded caps,
/// fan-out) turns a chain's sink into collected `node_data` — but an
/// unbounded stream must never be collected into memory (L16). The spool is
/// the third leg: the collector engages it LAZILY when STREAM_START declares
/// unbounded and no persist writer exists, streaming the data to a temp file
/// in the exact `node_data` byte form:
/// - sequence: chunk payloads (raw CBOR fragments) appended verbatim — the
///   file IS the RFC 8742 CBOR sequence;
/// - blob: each chunk's CBOR Bytes/Text value unwrapped, raw bytes appended.
///
/// [`send_file_stream`] then feeds the downstream chain head from the file in
/// bounded windows. The stream has ENDED by the time the file is read, so the
/// downstream cap receives an ordinary bounded stream — which is exactly the
/// semantics a split boundary implies (the consumer needed the complete
/// input before its permit could even be acquired).
pub struct SpoolWriter {
    path: std::path::PathBuf,
    file: Option<tokio::fs::File>,
    is_sequence: bool,
    stream_meta: Option<StreamMeta>,
    total_bytes: usize,
    engaged: bool,
}

impl SpoolWriter {
    pub fn new(path: std::path::PathBuf) -> Self {
        Self {
            path,
            file: None,
            is_sequence: false,
            stream_meta: None,
            total_bytes: 0,
            engaged: false,
        }
    }

    /// Whether the collector routed a stream into this spool.
    pub fn engaged(&self) -> bool {
        self.engaged
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    pub fn is_sequence(&self) -> bool {
        self.is_sequence
    }

    pub fn stream_meta(&self) -> Option<&StreamMeta> {
        self.stream_meta.as_ref()
    }
}

#[async_trait]
impl IncrementalWriter for SpoolWriter {
    async fn on_stream_start(
        &mut self,
        is_sequence: Option<bool>,
        _media_urn: &str,
        meta: Option<StreamMeta>,
        _stream_id: Option<String>,
    ) -> Result<(), StreamIoError> {
        if self.engaged {
            return Err(StreamIoError::Protocol(
                "intermediate spool received a second STREAM_START — a chain sink is \
                 exactly one stream"
                    .to_string(),
            ));
        }
        self.engaged = true;
        self.is_sequence = is_sequence == Some(true);
        self.stream_meta = meta;
        let file = tokio::fs::File::create(&self.path).await.map_err(|e| {
            StreamIoError::Protocol(format!(
                "failed to create spool file '{}': {e}",
                self.path.display()
            ))
        })?;
        self.file = Some(file);
        Ok(())
    }

    async fn on_chunk_payload(
        &mut self,
        payload: &[u8],
        _meta: Option<StreamMeta>,
    ) -> Result<(), StreamIoError> {
        use tokio::io::AsyncWriteExt;
        let Some(file) = self.file.as_mut() else {
            return Err(StreamIoError::Protocol(
                "intermediate spool received a CHUNK before STREAM_START".to_string(),
            ));
        };
        if self.is_sequence {
            // Raw CBOR fragments append verbatim — concatenation of the
            // producer's self-delimiting values is the RFC 8742 form.
            file.write_all(payload).await.map_err(|e| {
                StreamIoError::Protocol(format!(
                    "failed to append to spool '{}': {e}",
                    self.path.display()
                ))
            })?;
            self.total_bytes += payload.len();
        } else {
            // Blob chunks are complete CBOR Bytes/Text values.
            let value: ciborium::Value = ciborium::de::from_reader(payload)
                .map_err(|e| StreamIoError::CborDecode(format!("spool blob chunk: {e}")))?;
            let raw = unwrap_cbor_value(value, 0)?;
            file.write_all(&raw).await.map_err(|e| {
                StreamIoError::Protocol(format!(
                    "failed to append to spool '{}': {e}",
                    self.path.display()
                ))
            })?;
            self.total_bytes += raw.len();
        }
        Ok(())
    }

    async fn on_stream_end(&mut self) -> Result<(), StreamIoError> {
        use tokio::io::AsyncWriteExt;
        if let Some(file) = self.file.as_mut() {
            file.flush().await.map_err(|e| {
                StreamIoError::Protocol(format!(
                    "failed to flush spool '{}': {e}",
                    self.path.display()
                ))
            })?;
        }
        Ok(())
    }

    fn finish(self: Box<Self>) -> super::execute_plan::WriterResult {
        super::execute_plan::WriterResult {
            is_sequence: self.is_sequence,
            media_urn: String::new(),
            saved_paths: vec![self.path.to_string_lossy().into_owned()],
            total_bytes: self.total_bytes,
            stream_meta: self.stream_meta,
            item_metas: Vec::new(),
        }
    }
}

/// How many bytes of a spool file are resident at once while feeding it
/// downstream — the streaming window, not a size cap.
const SPOOL_READ_WINDOW: usize = 256 * 1024;

/// Send one input stream from a SPOOL FILE (STREAM_START → CHUNKs →
/// STREAM_END), mirroring [`send_one_stream`]'s framing and credit handling
/// while keeping only a bounded window of the file in memory. The file holds
/// the `node_data` byte form written by [`SpoolWriter`].
#[allow(clippy::too_many_arguments)]
pub async fn send_file_stream(
    switch: &Arc<RelaySwitch>,
    rid: &MessageId,
    media_urn: &str,
    path: &std::path::Path,
    meta: Option<StreamMeta>,
    is_sequence: bool,
    max_chunk: usize,
    credit: Option<(&crate::bifaci::credit::CreditRouter, u64)>,
) -> Result<(), StreamIoError> {
    use tokio::io::AsyncReadExt;

    let stream_id = uuid::Uuid::new_v4().to_string();
    let credit = credit.map(|(router, initial_credit)| {
        let gate = std::sync::Arc::new(crate::bifaci::credit::CreditGate::new(initial_credit));
        router.register(
            rid.clone(),
            Some(stream_id.clone()),
            std::sync::Arc::clone(&gate),
        );
        gate
    });
    let credit = credit.as_deref();
    async fn acquire(
        credit: Option<&crate::bifaci::credit::CreditGate>,
    ) -> Result<(), StreamIoError> {
        if let Some(gate) = credit {
            gate.acquire(1)
                .await
                .map_err(|e| StreamIoError::Transport(e.to_string()))?;
        }
        Ok(())
    }

    let mut file = tokio::fs::File::open(path).await.map_err(|e| {
        StreamIoError::Protocol(format!("failed to open spool '{}': {e}", path.display()))
    })?;

    let mut ss = Frame::stream_start(
        rid.clone(),
        stream_id.clone(),
        media_urn.to_string(),
        if is_sequence { Some(true) } else { None },
    );
    ss.meta = meta;
    switch
        .send_to_master(ss, None)
        .await
        .map_err(|e| StreamIoError::Transport(format!("STREAM_START: {}", e)))?;

    let mut chunk_index = 0u64;
    let mut send_chunk = |payload: Vec<u8>| {
        let rid = rid.clone();
        let stream_id = stream_id.clone();
        let idx = chunk_index;
        chunk_index += 1;
        let checksum = Frame::compute_checksum(&payload);
        Frame::chunk(rid, stream_id, idx, payload, idx, checksum)
    };

    if is_sequence {
        // Rolling window: read, drain every complete self-delimiting CBOR
        // value as one item chunk, keep the partial tail, repeat.
        let mut buf: Vec<u8> = Vec::with_capacity(SPOOL_READ_WINDOW);
        let mut read_buf = vec![0u8; SPOOL_READ_WINDOW];
        loop {
            let n = file.read(&mut read_buf).await.map_err(|e| {
                StreamIoError::Protocol(format!("failed to read spool '{}': {e}", path.display()))
            })?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&read_buf[..n]);
            loop {
                if buf.is_empty() {
                    break;
                }
                let mut cursor = std::io::Cursor::new(buf.as_slice());
                let ok = ciborium::de::from_reader::<ciborium::Value, _>(&mut cursor).is_ok();
                if !ok {
                    break; // incomplete value — read more
                }
                let consumed = cursor.position() as usize;
                let item: Vec<u8> = buf.drain(..consumed).collect();
                acquire(credit).await?;
                let chunk = send_chunk(item);
                switch
                    .send_to_master(chunk, None)
                    .await
                    .map_err(|e| StreamIoError::Transport(format!("CHUNK: {}", e)))?;
            }
        }
        if !buf.is_empty() {
            return Err(StreamIoError::Protocol(format!(
                "{} bytes of an incomplete CBOR item at the end of spool '{}' — \
                 truncated intermediate",
                buf.len(),
                path.display()
            )));
        }
    } else {
        let mut read_buf = vec![0u8; max_chunk.max(1)];
        let mut sent_any = false;
        loop {
            let n = file.read(&mut read_buf).await.map_err(|e| {
                StreamIoError::Protocol(format!("failed to read spool '{}': {e}", path.display()))
            })?;
            if n == 0 {
                break;
            }
            sent_any = true;
            let cbor_value = ciborium::Value::Bytes(read_buf[..n].to_vec());
            let mut cbor_payload = Vec::new();
            ciborium::into_writer(&cbor_value, &mut cbor_payload)
                .map_err(|e| StreamIoError::CborEncode(format!("{}", e)))?;
            acquire(credit).await?;
            let chunk = send_chunk(cbor_payload);
            switch
                .send_to_master(chunk, None)
                .await
                .map_err(|e| StreamIoError::Transport(format!("CHUNK: {}", e)))?;
        }
        if !sent_any {
            // Mirror send_one_stream: an empty payload still sends one
            // explicit empty chunk.
            let mut cbor_payload = Vec::new();
            ciborium::into_writer(&ciborium::Value::Bytes(vec![]), &mut cbor_payload)
                .map_err(|e| StreamIoError::CborEncode(format!("{}", e)))?;
            acquire(credit).await?;
            let chunk = send_chunk(cbor_payload);
            switch
                .send_to_master(chunk, None)
                .await
                .map_err(|e| StreamIoError::Transport(format!("CHUNK: {}", e)))?;
        }
    }

    let se = Frame::stream_end(rid.clone(), stream_id, chunk_index);
    switch
        .send_to_master(se, None)
        .await
        .map_err(|e| StreamIoError::Transport(format!("STREAM_END: {}", e)))?;
    Ok(())
}

/// One member of a GATHER (N producers concatenating into one sequence
/// arg), in the order the resolver declared its sources.
pub enum GatherMember {
    /// In-memory member: `node_data` form — raw bytes for a scalar (wrapped
    /// as one CBOR Bytes item), an RFC 8742 CBOR sequence for a sequence
    /// (items forwarded as-is).
    Memory { data: Vec<u8>, is_sequence: bool },
    /// A spooled member (an UNBOUNDED intermediate whose feed ended): its
    /// items stream from the file in bounded windows — the member's whole
    /// point is never being memory-resident. A sequence spool contributes
    /// its items; a blob spool contributes ONE item whose CBOR byte-string
    /// streams across chunk payloads (receivers reassemble split items by
    /// contract, 12.4 §Streaming).
    Spooled {
        path: std::path::PathBuf,
        is_sequence: bool,
    },
}

/// The CBOR byte-string HEADER for a payload of `len` bytes (major type 2).
/// Streaming a spooled blob as one item = this header followed by the raw
/// file bytes, split across chunk payloads.
fn cbor_bytes_header(len: u64) -> Vec<u8> {
    match len {
        0..=23 => vec![0x40 | len as u8],
        24..=0xFF => vec![0x58, len as u8],
        0x100..=0xFFFF => {
            let mut v = vec![0x59];
            v.extend_from_slice(&(len as u16).to_be_bytes());
            v
        }
        0x1_0000..=0xFFFF_FFFF => {
            let mut v = vec![0x5A];
            v.extend_from_slice(&(len as u32).to_be_bytes());
            v
        }
        _ => {
            let mut v = vec![0x5B];
            v.extend_from_slice(&len.to_be_bytes());
            v
        }
    }
}

/// Send one GATHERED sequence stream assembled from `members` in order
/// (STREAM_START → members' items → STREAM_END), mirroring
/// [`send_one_stream`]'s framing and credit handling. Spooled members
/// stream from their files in bounded windows — a gather over an ended
/// unbounded intermediate never re-buffers it.
#[allow(clippy::too_many_arguments)]
pub async fn send_gathered_stream(
    switch: &Arc<RelaySwitch>,
    rid: &MessageId,
    media_urn: &str,
    members: Vec<GatherMember>,
    max_chunk: usize,
    credit: Option<(&crate::bifaci::credit::CreditRouter, u64)>,
) -> Result<(), StreamIoError> {
    use tokio::io::AsyncReadExt;

    let stream_id = uuid::Uuid::new_v4().to_string();
    let credit = credit.map(|(router, initial_credit)| {
        let gate = std::sync::Arc::new(crate::bifaci::credit::CreditGate::new(initial_credit));
        router.register(
            rid.clone(),
            Some(stream_id.clone()),
            std::sync::Arc::clone(&gate),
        );
        gate
    });
    let credit = credit.as_deref();
    async fn acquire(
        credit: Option<&crate::bifaci::credit::CreditGate>,
    ) -> Result<(), StreamIoError> {
        if let Some(gate) = credit {
            gate.acquire(1)
                .await
                .map_err(|e| StreamIoError::Transport(e.to_string()))?;
        }
        Ok(())
    }

    let ss = Frame::stream_start(
        rid.clone(),
        stream_id.clone(),
        media_urn.to_string(),
        Some(true),
    );
    switch
        .send_to_master(ss, None)
        .await
        .map_err(|e| StreamIoError::Transport(format!("STREAM_START: {}", e)))?;

    let mut chunk_index = 0u64;
    macro_rules! send_payload {
        ($payload:expr) => {{
            let payload: Vec<u8> = $payload;
            acquire(credit).await?;
            let checksum = Frame::compute_checksum(&payload);
            let chunk = Frame::chunk(
                rid.clone(),
                stream_id.clone(),
                chunk_index,
                payload,
                chunk_index,
                checksum,
            );
            switch
                .send_to_master(chunk, None)
                .await
                .map_err(|e| StreamIoError::Transport(format!("CHUNK: {}", e)))?;
            chunk_index += 1;
        }};
    }

    for member in members {
        match member {
            GatherMember::Memory { data, is_sequence } => {
                if is_sequence {
                    // RFC 8742 sequence: forward each self-delimiting value
                    // as its own item payload.
                    let mut cursor = std::io::Cursor::new(data.as_slice());
                    while (cursor.position() as usize) < data.len() {
                        let start = cursor.position() as usize;
                        let _: ciborium::Value =
                            ciborium::de::from_reader(&mut cursor).map_err(|e| {
                                StreamIoError::CborDecode(format!(
                                    "gather sequence member item: {e}"
                                ))
                            })?;
                        let end = cursor.position() as usize;
                        send_payload!(data[start..end].to_vec());
                    }
                } else {
                    let mut payload = Vec::new();
                    ciborium::into_writer(&ciborium::Value::Bytes(data), &mut payload)
                        .map_err(|e| StreamIoError::CborEncode(format!("{}", e)))?;
                    send_payload!(payload);
                }
            }
            GatherMember::Spooled { path, is_sequence } => {
                let mut file = tokio::fs::File::open(&path).await.map_err(|e| {
                    StreamIoError::Protocol(format!(
                        "failed to open gather spool '{}': {e}",
                        path.display()
                    ))
                })?;
                if is_sequence {
                    // Rolling window: forward each complete value as an item.
                    let mut buf: Vec<u8> = Vec::with_capacity(SPOOL_READ_WINDOW);
                    let mut read_buf = vec![0u8; SPOOL_READ_WINDOW];
                    loop {
                        let n = file.read(&mut read_buf).await.map_err(|e| {
                            StreamIoError::Protocol(format!(
                                "failed to read gather spool '{}': {e}",
                                path.display()
                            ))
                        })?;
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&read_buf[..n]);
                        loop {
                            if buf.is_empty() {
                                break;
                            }
                            let mut cursor = std::io::Cursor::new(buf.as_slice());
                            if ciborium::de::from_reader::<ciborium::Value, _>(&mut cursor)
                                .is_err()
                            {
                                break; // incomplete — read more
                            }
                            let consumed = cursor.position() as usize;
                            let item: Vec<u8> = buf.drain(..consumed).collect();
                            send_payload!(item);
                        }
                    }
                    if !buf.is_empty() {
                        return Err(StreamIoError::Protocol(format!(
                            "{} bytes of an incomplete CBOR item at the end of gather \
                             spool '{}' — truncated intermediate",
                            buf.len(),
                            path.display()
                        )));
                    }
                } else {
                    // A blob spool is ONE item: its CBOR byte-string header,
                    // then the raw file bytes, split across payloads — the
                    // receiver reassembles the split item (12.4 §Streaming).
                    let len = file
                        .metadata()
                        .await
                        .map_err(|e| {
                            StreamIoError::Protocol(format!(
                                "failed to stat gather spool '{}': {e}",
                                path.display()
                            ))
                        })?
                        .len();
                    send_payload!(cbor_bytes_header(len));
                    let mut sent: u64 = 0;
                    let mut read_buf = vec![0u8; max_chunk.max(1)];
                    loop {
                        let n = file.read(&mut read_buf).await.map_err(|e| {
                            StreamIoError::Protocol(format!(
                                "failed to read gather spool '{}': {e}",
                                path.display()
                            ))
                        })?;
                        if n == 0 {
                            break;
                        }
                        sent += n as u64;
                        send_payload!(read_buf[..n].to_vec());
                    }
                    if sent != len {
                        return Err(StreamIoError::Protocol(format!(
                            "gather spool '{}' changed size mid-send ({} bytes sent, \
                             header promised {}) — the item on the wire is corrupt",
                            path.display(),
                            sent,
                            len
                        )));
                    }
                }
            }
        }
    }

    let se = Frame::stream_end(rid.clone(), stream_id, chunk_index);
    switch
        .send_to_master(se, None)
        .await
        .map_err(|e| StreamIoError::Transport(format!("STREAM_END: {}", e)))?;
    Ok(())
}

/// Observes the correlation between a cap invocation's request id and its
/// strand-step identity, at the one point both are known (invocation setup).
/// The engine implements this to feed the run's live protocol/flow snapshots
/// (L8); the reference CLI path passes `None` (no observation).
pub trait FlowObserver: Send + Sync {
    /// Record that request `rid` belongs to strand step `token_id`.
    fn record(&self, rid: &crate::bifaci::frame::MessageId, token_id: &str);

    /// Record that request `rid` is FEED-BEARING: its input carries a live
    /// reference the receiving cartridge resolved into an open device tap.
    /// The stop-input control (15.2 §Runs Stop) sends a CloseStream frame to
    /// exactly these requests — the runtime closes the taps and the machine
    /// drains — without touching any other in-flight request. Default no-op
    /// for observers that don't wire stop.
    fn record_feed_bearing(&self, _rid: &crate::bifaci::frame::MessageId) {}
}

/// Send a single input stream (STREAM_START → CHUNKs → STREAM_END) to a cartridge.
///
/// Handles both scalar and sequence mode:
/// - Scalar (`is_sequence=false`): wraps each chunk in `CBOR::Bytes`
/// - Sequence (`is_sequence=true`): sends raw CBOR item bytes directly
///   (matching `emit_list_item` semantics on the cartridge side)
pub async fn send_one_stream(
    switch: &Arc<RelaySwitch>,
    rid: &MessageId,
    media_urn: &str,
    data: &[u8],
    meta: Option<StreamMeta>,
    is_sequence: bool,
    max_chunk: usize,
    credit: Option<(&crate::bifaci::credit::CreditRouter, u64)>,
) -> Result<(), StreamIoError> {
    let stream_id = uuid::Uuid::new_v4().to_string();

    // Flow-control window for this stream (L9): the receiving cartridge's
    // consumption grants arrive as CREDIT frames which the caller's collect
    // loop routes into `router`; registration under (rid, stream_id) is what
    // lets those grants find this gate. The caller releases the request's
    // gates on terminal via `router.close_request` (L13).
    let credit = credit.map(|(router, initial_credit)| {
        let gate = std::sync::Arc::new(crate::bifaci::credit::CreditGate::new(initial_credit));
        router.register(
            rid.clone(),
            Some(stream_id.clone()),
            std::sync::Arc::clone(&gate),
        );
        gate
    });
    let credit = credit.as_deref();

    // Acquire one flow-control credit (L9). Waits when the receiver's window
    // is exhausted; fails when the request terminates (L13) so a blocked
    // sender stops instead of hanging.
    async fn acquire(
        credit: Option<&crate::bifaci::credit::CreditGate>,
    ) -> Result<(), StreamIoError> {
        if let Some(gate) = credit {
            gate.acquire(1)
                .await
                .map_err(|e| StreamIoError::Transport(e.to_string()))?;
        }
        Ok(())
    }

    let mut ss = Frame::stream_start(
        rid.clone(),
        stream_id.clone(),
        media_urn.to_string(),
        if is_sequence { Some(true) } else { None },
    );
    ss.meta = meta;
    switch
        .send_to_master(ss, None)
        .await
        .map_err(|e| StreamIoError::Transport(format!("STREAM_START: {}", e)))?;

    let mut chunk_index = 0u64;

    if is_sequence {
        // Sequence mode: data is an RFC 8742 CBOR sequence.
        // Each self-delimiting CBOR value is sent as a separate chunk
        // payload. The chunk payload IS the raw CBOR bytes of the item
        // (not re-wrapped).
        if !data.is_empty() {
            let mut cursor = std::io::Cursor::new(data);
            while (cursor.position() as usize) < data.len() {
                let start_pos = cursor.position() as usize;
                let _value: ciborium::Value = ciborium::from_reader(&mut cursor).map_err(|e| {
                    StreamIoError::CborDecode(format!("sequence item {}: {}", chunk_index, e))
                })?;
                let end_pos = cursor.position() as usize;
                let item_cbor = &data[start_pos..end_pos];

                acquire(credit).await?;
                let checksum = Frame::compute_checksum(item_cbor);
                let chunk = Frame::chunk(
                    rid.clone(),
                    stream_id.clone(),
                    chunk_index,
                    item_cbor.to_vec(),
                    chunk_index,
                    checksum,
                );
                switch
                    .send_to_master(chunk, None)
                    .await
                    .map_err(|e| StreamIoError::Transport(format!("CHUNK: {}", e)))?;
                chunk_index += 1;
            }
        }
    } else {
        // Scalar mode: data is raw bytes, wrapped as CBOR::Bytes per chunk.
        if data.is_empty() {
            let cbor_value = ciborium::Value::Bytes(vec![]);
            let mut cbor_payload = Vec::new();
            ciborium::into_writer(&cbor_value, &mut cbor_payload)
                .map_err(|e| StreamIoError::CborEncode(format!("{}", e)))?;
            acquire(credit).await?;
            let checksum = Frame::compute_checksum(&cbor_payload);
            let chunk = Frame::chunk(rid.clone(), stream_id.clone(), 0, cbor_payload, 0, checksum);
            switch
                .send_to_master(chunk, None)
                .await
                .map_err(|e| StreamIoError::Transport(format!("CHUNK: {}", e)))?;
            chunk_index = 1;
        } else {
            let mut offset = 0;
            while offset < data.len() {
                let end = (offset + max_chunk).min(data.len());
                let chunk_data = &data[offset..end];
                let cbor_value = ciborium::Value::Bytes(chunk_data.to_vec());
                let mut cbor_payload = Vec::new();
                ciborium::into_writer(&cbor_value, &mut cbor_payload)
                    .map_err(|e| StreamIoError::CborEncode(format!("{}", e)))?;
                acquire(credit).await?;
                let checksum = Frame::compute_checksum(&cbor_payload);
                let chunk = Frame::chunk(
                    rid.clone(),
                    stream_id.clone(),
                    chunk_index,
                    cbor_payload,
                    chunk_index,
                    checksum,
                );
                switch
                    .send_to_master(chunk, None)
                    .await
                    .map_err(|e| StreamIoError::Transport(format!("CHUNK: {}", e)))?;
                offset = end;
                chunk_index += 1;
            }
        }
    }

    let se = Frame::stream_end(rid.clone(), stream_id, chunk_index);
    switch
        .send_to_master(se, None)
        .await
        .map_err(|e| StreamIoError::Transport(format!("STREAM_END: {}", e)))?;

    Ok(())
}

/// Decode terminal output bytes based on is_sequence flag.
///
/// Returns `Vec<Vec<u8>>` — a list of unwrapped items:
/// - `is_sequence=true` (emit_list_item): each CBOR value in the
///   sequence is unwrapped (Bytes→raw, Text→UTF-8) into a separate item.
/// - `is_sequence=false/None` (write/emit_cbor): CBOR Bytes/Text
///   wrappers are unwrapped and concatenated into a single item.
pub fn decode_terminal_output(
    response_chunks: &[u8],
    is_sequence: Option<bool>,
) -> Result<Vec<Vec<u8>>, StreamIoError> {
    if response_chunks.is_empty() {
        return Ok(vec![vec![]]);
    }

    if is_sequence == Some(true) {
        let mut items: Vec<Vec<u8>> = Vec::new();
        let mut cursor = std::io::Cursor::new(response_chunks);
        while (cursor.position() as usize) < response_chunks.len() {
            let value: ciborium::Value = ciborium::from_reader(&mut cursor).map_err(|e| {
                StreamIoError::CborDecode(format!("sequence item {}: {}", items.len(), e))
            })?;
            let raw = unwrap_cbor_value(value, items.len())?;
            items.push(raw);
        }
        Ok(items)
    } else {
        let mut output_bytes = Vec::new();
        let mut cursor = std::io::Cursor::new(response_chunks);
        while (cursor.position() as usize) < response_chunks.len() {
            let value: ciborium::Value = ciborium::from_reader(&mut cursor)
                .map_err(|e| StreamIoError::CborDecode(format!("terminal response: {}", e)))?;
            let raw = unwrap_cbor_value(value, 0)?;
            output_bytes.extend(raw);
        }
        Ok(vec![output_bytes])
    }
}

/// Unwrap a CBOR transport value to raw bytes.
///
/// Bytes → inner bytes, Text → UTF-8 bytes. Anything else is a
/// protocol error.
pub fn unwrap_cbor_value(
    value: ciborium::Value,
    item_index: usize,
) -> Result<Vec<u8>, StreamIoError> {
    match value {
        ciborium::Value::Bytes(b) => Ok(b),
        ciborium::Value::Text(t) => Ok(t.into_bytes()),
        _ => Err(StreamIoError::UnexpectedCborType {
            index: item_index,
            description: format!("{:?}", value),
        }),
    }
}

// =============================================================================
// Incremental terminal consumption (unbounded streams, L16)
// =============================================================================

/// One item yielded incrementally from a cap's terminal output stream.
#[derive(Debug)]
pub struct TerminalItem {
    /// Raw chunk payload (CBOR transport bytes as sent by the producer).
    pub payload: Vec<u8>,
    /// Per-item metadata from the chunk frame (first chunk of an item).
    pub meta: Option<StreamMeta>,
    /// The stream this item belongs to.
    pub stream_id: Option<String>,
}

/// Incremental consumer for a cap's terminal output: yields items AS THEY
/// ARRIVE (before STREAM_END or END exist — required for unbounded streams,
/// L16), then `finish()` returns the terminal metadata after END.
///
/// This is the incremental counterpart of `collect_terminal_output`, sharing
/// its frame semantics: LOG frames drive the progress/log callbacks, CREDIT
/// frames route to the send gates, chunk consumption emits batched grants,
/// END delivers the terminal progress event (L5), ERR fails.
pub struct TerminalOutput {
    rx: mpsc::UnboundedReceiver<Frame>,
    cap_urn: String,
    step_token_id: StepToken,
    progress_fn: Option<CapProgressFn>,
    log_fn: Option<PipelineLogFn>,
    credit: Option<CreditPlumbing>,
    consumed_since_grant: std::collections::HashMap<Option<String>, u64>,
    is_sequence: Option<bool>,
    terminal_meta: TerminalMeta,
    /// Set once END was seen; `next_item` returns None afterwards.
    ended: bool,
    unbounded: bool,
    /// Audits every STREAM_START emission against the cap's declared
    /// effect contract before its items are yielded.
    effect_audit: EffectAudit,
}

impl TerminalOutput {
    /// Fails when the cap URN (or its declared `in=`) does not parse — the
    /// effect audit is built here and every terminal emission is audited
    /// against it, so an unauditable cap must fail at construction, not be
    /// consumed unaudited.
    pub fn new(
        rx: mpsc::UnboundedReceiver<Frame>,
        cap_urn: &str,
        contract: OutputContract,
        step_token_id: &StepToken,
        progress_fn: Option<CapProgressFn>,
        log_fn: Option<PipelineLogFn>,
        credit: Option<CreditPlumbing>,
    ) -> Result<Self, StreamIoError> {
        let effect_audit = EffectAudit::new(cap_urn, contract)?;
        Ok(Self {
            rx,
            cap_urn: cap_urn.to_string(),
            step_token_id: step_token_id.clone(),
            progress_fn,
            log_fn,
            credit,
            consumed_since_grant: std::collections::HashMap::new(),
            is_sequence: None,
            terminal_meta: TerminalMeta::default(),
            ended: false,
            unbounded: false,
            effect_audit,
        })
    }

    /// Whether the response stream declared itself unbounded (known after the
    /// first STREAM_START has been observed).
    pub fn is_unbounded(&self) -> bool {
        self.unbounded
    }

    /// Whether the producer used sequence mode (known after STREAM_START).
    pub fn is_sequence(&self) -> Option<bool> {
        self.is_sequence
    }

    /// Yield the next data item, or None once the request's END arrived.
    /// LOG/CREDIT/STREAM_* frames are handled internally; ERR and
    /// unsuccessful END fail.
    pub async fn next_item(&mut self) -> Option<Result<TerminalItem, StreamIoError>> {
        if self.ended {
            return None;
        }
        loop {
            let next = match self.rx.try_recv() {
                Ok(f) => Some(f),
                Err(mpsc::error::TryRecvError::Disconnected) => None,
                Err(mpsc::error::TryRecvError::Empty) => {
                    // About to block: flush pending sub-batch grants (L10
                    // deadlock-freedom rule) so a producer with a smaller
                    // send window than our batch threshold can proceed.
                    if let Some(plumbing) = &self.credit {
                        for (stream_id, counter) in self.consumed_since_grant.iter_mut() {
                            if *counter > 0 {
                                (plumbing.grant)(stream_id.clone(), *counter);
                                *counter = 0;
                            }
                        }
                    }
                    self.rx.recv().await
                }
            };
            let frame = match next {
                Some(f) => f,
                None => {
                    self.ended = true;
                    // Engine-detected protocol violation — no source declared
                    // an identity, so this is ours: Internal, no code.
                    return Some(Err(StreamIoError::Terminal {
                        cap_urn: self.cap_urn.clone(),
                        code: None,
                        class: crate::failure::AttributionClass::Internal,
                        details: "response channel closed without END".to_string(),
                        arg_urn: None,
                    }));
                }
            };
            match frame.frame_type {
                FrameType::StreamStart => {
                    // Effect audit at receipt: the emission must satisfy the
                    // cap's declared effect contract before any of its items
                    // are yielded.
                    if let Err(e) = self.effect_audit.audit_frame(&frame) {
                        self.ended = true;
                        return Some(Err(e));
                    }
                    if let Some(seq) = frame.is_sequence {
                        self.is_sequence = Some(seq);
                    }
                    if frame.is_unbounded() {
                        self.unbounded = true;
                    }
                    self.terminal_meta.stream_meta = frame.meta.clone();
                }
                FrameType::Chunk => {
                    if let Some(plumbing) = &self.credit {
                        let counter = self
                            .consumed_since_grant
                            .entry(frame.stream_id.clone())
                            .or_insert(0);
                        *counter += 1;
                        if *counter >= plumbing.batch {
                            (plumbing.grant)(frame.stream_id.clone(), *counter);
                            *counter = 0;
                        }
                    }
                    if let Some(payload) = frame.payload {
                        return Some(Ok(TerminalItem {
                            payload,
                            meta: frame.meta,
                            stream_id: frame.stream_id,
                        }));
                    }
                }
                FrameType::Credit => {
                    if let Some(plumbing) = &self.credit {
                        plumbing.router.grant(&frame);
                    }
                }
                FrameType::Log => {
                    let level = match frame.log_level() {
                        Some(level) => level,
                        None => {
                            return Some(Err(StreamIoError::Protocol(
                                "LOG frame missing required text level".to_string(),
                            )))
                        }
                    };
                    // Branch on the LEVEL, which is the one criterion that
                    // decides whether a LOG is functional progress or an
                    // attributed diagnostic — the same criterion
                    // `attribution_class()` uses. Branching on "did a numeric
                    // progress value parse?" instead let a level="progress"
                    // frame whose number was missing or mistyped fall into the
                    // diagnostic arm, where it failed as "Log frames do not
                    // carry attribution" — an error that names the wrong defect
                    // and sends the reader hunting attribution instead of the
                    // absent progress value.
                    if level == "progress" {
                        let msg = match frame.log_message() {
                            Some(message) => message,
                            None => {
                                return Some(Err(StreamIoError::Protocol(
                                    "progress LOG frame missing required text message".to_string(),
                                )))
                            }
                        };
                        let p = match frame.log_progress() {
                            Some(p) => p,
                            None => {
                                return Some(Err(StreamIoError::Protocol(format!(
                                    "cap '{}' sent a progress LOG whose `progress` value is {} \
                                     — level is \"progress\", so a numeric value is required",
                                    self.cap_urn,
                                    frame.log_progress_slot_description()
                                ))))
                            }
                        };
                        if let Some(pfn) = &self.progress_fn {
                            pfn(p, &self.cap_urn, msg);
                        }
                    } else {
                        let msg = match frame.log_message() {
                            Some(message) => message,
                            None => {
                                return Some(Err(StreamIoError::Protocol(
                                    "LOG frame missing required text message".to_string(),
                                )))
                            }
                        };
                        let class = match frame.attribution_class() {
                            Ok(class) => class,
                            Err(error) => return Some(Err(StreamIoError::Protocol(error))),
                        };
                        if let Some(lfn) = &self.log_fn {
                            let mut record = PipelineLogRecord::attributed(
                                &self.step_token_id,
                                &self.cap_urn,
                                level,
                                class,
                                msg,
                            );
                            record.meta = frame.meta.clone();
                            record.arg_urn = match frame.attribution_arg_urn() {
                                Ok(arg_urn) => arg_urn.map(str::to_string),
                                Err(error) => return Some(Err(StreamIoError::Protocol(error))),
                            };
                            lfn(record);
                        }
                    }
                }
                FrameType::StreamEnd => {
                    // Structural; chunk_count (when present) is the bounded
                    // producer's own count. Unbounded streams omit it (L16).
                }
                FrameType::End => {
                    self.ended = true;
                    if frame.exit_code() != Some(0) {
                        // Non-success END with no ERR frame — the source never
                        // declared an identity: Internal, no code.
                        return Some(Err(StreamIoError::Terminal {
                            cap_urn: self.cap_urn.clone(),
                            code: None,
                            class: crate::failure::AttributionClass::Internal,
                            details: format!(
                                "END without success: exit_code={:?}",
                                frame.exit_code()
                            ),
                            arg_urn: None,
                        }));
                    }
                    // Terminal metadata IS the final progress event (L5).
                    let final_progress = frame.final_progress().unwrap_or(1.0) as f32;
                    let final_message = frame.final_message().unwrap_or("");
                    if let Some(pfn) = &self.progress_fn {
                        pfn(final_progress, &self.cap_urn, final_message);
                    }
                    return None;
                }
                FrameType::Err => {
                    self.ended = true;
                    let class = match frame.attribution_class() {
                        Ok(class) => class,
                        Err(message) => return Some(Err(StreamIoError::Protocol(message))),
                    };
                    // The ERR frame carries the failure identity DECLARED at
                    // its emit source — read it structurally, never re-derive.
                    let code = match frame.error_code() {
                        Some(code) => code.to_string(),
                        None => {
                            return Some(Err(StreamIoError::Protocol(
                                "ERR frame missing required text code".to_string(),
                            )))
                        }
                    };
                    let details = match frame.error_message() {
                        Some(message) => message.to_string(),
                        None => {
                            return Some(Err(StreamIoError::Protocol(
                                "ERR frame missing required text message".to_string(),
                            )))
                        }
                    };
                    return Some(Err(StreamIoError::Terminal {
                        cap_urn: self.cap_urn.clone(),
                        code: Some(code),
                        class,
                        details,
                        arg_urn: match frame.attribution_arg_urn() {
                            Ok(arg_urn) => arg_urn.map(str::to_string),
                            Err(error) => return Some(Err(StreamIoError::Protocol(error))),
                        },
                    }));
                }
                _ => {}
            }
        }
    }

    /// Drain any remaining items (delivering callbacks) and return the
    /// stream-level terminal metadata. Call after `next_item` returned None,
    /// or directly to consume-and-discard the remainder of a bounded stream.
    pub async fn finish(mut self) -> Result<(TerminalMeta, Option<bool>), StreamIoError> {
        while let Some(item) = self.next_item().await {
            item?;
        }
        Ok((self.terminal_meta, self.is_sequence))
    }
}

// =============================================================================
// Terminal collect
// =============================================================================

/// Collect the terminal response from a cap, decoding frames as they arrive.
///
/// Walks the response stream (STREAM_START → CHUNK… → STREAM_END → END), with
/// optional per-cap progress callbacks, pipeline logging, a shared pipeline
/// stall tracker, and an optional `IncrementalWriter` that streams the bytes
/// to disk rather than buffering them in memory.
///
/// Returns `(response_bytes, is_sequence, terminal_meta)`. When a writer is
/// provided, `response_bytes` is empty — the data is already persisted via
/// the writer.
///
/// # Error semantics
/// - `END` with non-zero or absent `exit_code` → `StreamIoError::Terminal`.
/// - `ERR` frame → `StreamIoError::Terminal`.
/// - Response channel closed without `END` → `StreamIoError::Terminal`.
/// - No activity for `activity_timeout_secs` → cancel request at relay,
///   return `StreamIoError::ActivityTimeout`.
/// - Writer failures bubble up as their `StreamIoError::Writer`.
pub async fn collect_terminal_output(
    mut rx: mpsc::UnboundedReceiver<Frame>,
    progress_fn: Option<&CapProgressFn>,
    cap_urn: &str,
    contract: OutputContract,
    step_token_id: &StepToken,
    log_fn: Option<&PipelineLogFn>,
    body_index: Option<usize>,
    stall_tracker: Option<&Arc<PipelineProgressTracker>>,
    writer: Option<&mut dyn IncrementalWriter>,
    spool: Option<&mut dyn IncrementalWriter>,
    activity_timeout_secs: u64,
    credit: Option<&CreditPlumbing>,
) -> Result<(Vec<u8>, Option<bool>, TerminalMeta), StreamIoError> {
    // The terminal cap's emission is audited against its declared effect
    // contract at receipt — before the payload is collected or persisted.
    let effect_audit = EffectAudit::new(cap_urn, contract)?;
    let mut response_chunks: Vec<u8> = Vec::new();
    let mut is_sequence: Option<bool> = None;
    // Consumed-chunk accounting for batched grants (L10): the producer's
    // output window is replenished as we consume, so it can stream past the
    // initial window without stalling.
    let mut consumed_since_grant: std::collections::HashMap<Option<String>, u64> =
        std::collections::HashMap::new();
    let mut timer = ActivityTimer::new(activity_timeout_secs);
    let has_writer = writer.is_some();
    let mut terminal_meta = TerminalMeta::default();
    // Whether we've already emitted the per-cap activity-silence warning
    // for the current quiet window. We do NOT abort on activity timeout:
    // long-running terminal caps (vision/LLM inference, large transcription)
    // legitimately sit silent for far longer than the threshold, and
    // aborting them produced false negatives on every honest workload.
    // The user cancels via the explicit cancel-task path; the timer's
    // job is now to surface the silence as a one-shot warning so the
    // operator knows the cap is alive-but-quiet, without log-spam every
    // 500 ms. The flag resets to false the next time any frame arrives.
    let mut activity_warning_logged = false;

    // Rebind writer as a LOCAL reborrow — we call methods on it inside the
    // loop, and engaging the spool replaces it (both reborrows share the
    // local lifetime, so the two parameters keep independent lifetimes).
    let mut writer: Option<&mut dyn IncrementalWriter> = match writer {
        Some(w) => Some(&mut *w),
        None => None,
    };
    let mut spool = spool;

    loop {
        let frame = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await;

        match frame {
            Ok(Some(frame)) => {
                // Any frame from this cap means the pipeline is alive.
                if let Some(tracker) = stall_tracker {
                    tracker.touch();
                }
                // Re-arm the warn-once gate for any subsequent silence.
                activity_warning_logged = false;
                match frame.frame_type {
                    FrameType::Chunk => {
                        timer.touch();
                        if let Some(payload) = &frame.payload {
                            if let Some(ref mut w) = writer {
                                w.on_chunk_payload(payload, frame.meta.clone()).await?;
                            } else {
                                response_chunks.extend_from_slice(payload);
                                // Collect per-item meta for ForEach propagation.
                                // Each non-None chunk meta marks the first chunk
                                // of a new item.
                                if let Some(meta) = frame.meta.clone() {
                                    terminal_meta.item_metas.push(meta);
                                }
                            }
                        }
                        // Replenish the producer's window (L10), batched.
                        if let Some(plumbing) = credit {
                            let counter = consumed_since_grant
                                .entry(frame.stream_id.clone())
                                .or_insert(0);
                            *counter += 1;
                            if *counter >= plumbing.batch {
                                (plumbing.grant)(frame.stream_id.clone(), *counter);
                                *counter = 0;
                            }
                        }
                    }
                    FrameType::Credit => {
                        // The cartridge crediting OUR input streams — route the
                        // grant to the engine-side send gates (L14). Unmatched
                        // grants are well-defined no-ops (grants only unblock).
                        if let Some(plumbing) = credit {
                            plumbing.router.grant(&frame);
                        }
                    }
                    FrameType::End => {
                        // exit_code in END meta: 0 = success, absent or non-zero
                        // = failure. Absence means the cartridge died (OOM,
                        // crash) and the relay synthesized a bare END — treat
                        // as failure.
                        let exit_code = frame.exit_code();
                        if exit_code != Some(0) {
                            let detail = match exit_code {
                                Some(code) => format!("exit_code={}", code),
                                None => "exit_code absent (cartridge likely crashed)".to_string(),
                            };
                            let details = format!("END without success: {}", detail);
                            emit_pipeline_log(
                                log_fn,
                                step_token_id,
                                cap_urn,
                                "error",
                                crate::failure::AttributionClass::Internal,
                                &details,
                                None,
                                body_index,
                                None,
                            );
                            // Non-success END with no ERR frame — no source
                            // declared an identity: Internal, no code.
                            return Err(StreamIoError::Terminal {
                                cap_urn: cap_urn.to_string(),
                                code: None,
                                class: crate::failure::AttributionClass::Internal,
                                details,
                                arg_urn: None,
                            });
                        }

                        if let Some(payload) = &frame.payload {
                            if let Some(ref mut w) = writer {
                                if !payload.is_empty() {
                                    w.on_chunk_payload(payload, frame.meta.clone()).await?;
                                }
                            } else {
                                response_chunks.extend_from_slice(payload);
                            }
                        }
                        if let Some(ref mut w) = writer {
                            w.on_stream_end().await?;
                        }
                        let _ = has_writer;

                        // Terminal metadata IS the final progress event (L5):
                        // END carries the authoritative final progress (1.0 by
                        // default on success, or the handler's declared value).
                        // Delivering it here — from the terminal frame itself —
                        // is what makes the final progress un-raceable.
                        let final_progress = frame.final_progress().unwrap_or(1.0) as f32;
                        let final_message = frame.final_message().unwrap_or("");
                        if let Some(pfn) = &progress_fn {
                            pfn(final_progress, cap_urn, final_message);
                        }

                        // Drain frames already queued locally before returning
                        // (L4/L5): LOG frames that arrived before END but sit
                        // behind it in this receiver are delivered, not lost.
                        // Anything else post-END is a counted drop, never
                        // silent. try_recv only — never wait after terminal.
                        while let Ok(late) = rx.try_recv() {
                            match late.frame_type {
                                FrameType::Log => {
                                    if late.log_progress().is_none() {
                                        let level = late.log_level().ok_or_else(|| {
                                            StreamIoError::Protocol(
                                                "LOG frame missing required text level".to_string(),
                                            )
                                        })?;
                                        let msg = late.log_message().ok_or_else(|| {
                                            StreamIoError::Protocol(
                                                "LOG frame missing required text message"
                                                    .to_string(),
                                            )
                                        })?;
                                        let class = late
                                            .attribution_class()
                                            .map_err(StreamIoError::Protocol)?;
                                        emit_pipeline_log(
                                            log_fn,
                                            step_token_id,
                                            cap_urn,
                                            level,
                                            class,
                                            msg,
                                            late.meta.clone(),
                                            body_index,
                                            late.attribution_arg_urn()
                                                .map_err(StreamIoError::Protocol)?
                                                .map(str::to_string),
                                        );
                                    }
                                }
                                other => {
                                    // Benign post-terminal straggler: the
                                    // request already delivered its terminal;
                                    // a data/control frame queued behind END
                                    // is moot by protocol — expected teardown
                                    // race, nothing lost.
                                    tracing::debug!(
                                        cap_urn = %cap_urn,
                                        ftype = ?other,
                                        rid = ?late.id,
                                        "[cap] ignoring benign post-terminal straggler queued behind END"
                                    );
                                }
                            }
                        }

                        return Ok((response_chunks, is_sequence, terminal_meta));
                    }
                    FrameType::Err => {
                        let class = frame.attribution_class().map_err(StreamIoError::Protocol)?;
                        let code = frame.error_code().ok_or_else(|| {
                            StreamIoError::Protocol(
                                "ERR frame missing required text code".to_string(),
                            )
                        })?;
                        let msg = frame
                            .error_message()
                            .ok_or_else(|| {
                                StreamIoError::Protocol(
                                    "ERR frame missing required text message".to_string(),
                                )
                            })?
                            .to_string();
                        let arg_urn = frame
                            .attribution_arg_urn()
                            .map_err(StreamIoError::Protocol)?
                            .map(str::to_string);
                        emit_pipeline_log(
                            log_fn,
                            step_token_id,
                            cap_urn,
                            "error",
                            class,
                            &msg,
                            None,
                            body_index,
                            arg_urn.clone(),
                        );
                        // The ERR frame carries the failure identity DECLARED
                        // at its emit source — read it structurally.
                        return Err(StreamIoError::Terminal {
                            cap_urn: cap_urn.to_string(),
                            code: Some(code.to_string()),
                            class,
                            details: msg,
                            arg_urn,
                        });
                    }
                    FrameType::Log => {
                        let level = frame.log_level().ok_or_else(|| {
                            StreamIoError::Protocol(
                                "LOG frame missing required text level".to_string(),
                            )
                        })?;
                        timer.handle_log_level(level);

                        // Branch on the LEVEL — see the streaming path above:
                        // keying on "did a number parse?" misreports a progress
                        // frame with an absent value as an attribution failure.
                        if level == "progress" {
                            let cartridge_msg = frame.log_message().ok_or_else(|| {
                                StreamIoError::Protocol(
                                    "progress LOG frame missing required text message".to_string(),
                                )
                            })?;
                            let p = frame.log_progress().ok_or_else(|| {
                                StreamIoError::Protocol(format!(
                                    "cap '{}' sent a progress LOG whose `progress` value is {} \
                                     — level is \"progress\", so a numeric value is required",
                                    cap_urn,
                                    frame.log_progress_slot_description()
                                ))
                            })?;
                            if let Some(pfn) = &progress_fn {
                                pfn(p, cap_urn, cartridge_msg);
                            }
                        } else {
                            let msg = frame.log_message().ok_or_else(|| {
                                StreamIoError::Protocol(
                                    "LOG frame missing required text message".to_string(),
                                )
                            })?;
                            let class =
                                frame.attribution_class().map_err(StreamIoError::Protocol)?;
                            emit_pipeline_log(
                                log_fn,
                                step_token_id,
                                cap_urn,
                                level,
                                class,
                                msg,
                                frame.meta.clone(),
                                body_index,
                                frame
                                    .attribution_arg_urn()
                                    .map_err(StreamIoError::Protocol)?
                                    .map(str::to_string),
                            );
                        }
                    }
                    FrameType::StreamStart => {
                        timer.touch();
                        // Effect + stream-contract audit at receipt: the
                        // emission must satisfy the cap's declared effect and
                        // declared shape before a single byte of it is
                        // collected or persisted.
                        effect_audit.audit_frame(&frame)?;
                        // An UNBOUNDED stream must be consumed incrementally
                        // (L16): a writer streams it to disk; without one this
                        // buffering collector would be unbounded memory. An
                        // INTERMEDIATE at a mandatory split boundary engages
                        // its disk spool here instead (the caller passed one);
                        // a TERMINAL without a persisted sink is refused
                        // loudly, never an OOM (15.2 §Live-Feed Machines).
                        if frame.is_unbounded() && writer.is_none() {
                            match spool.take() {
                                Some(sp) => writer = Some(&mut *sp),
                                None => {
                                    return Err(StreamIoError::Protocol(format!(
                                        "cap '{}' terminal declared an UNBOUNDED stream but the \
                                         sink is not persisted — unbounded terminals require \
                                         incremental consumption (a persisted sink / \
                                         IncrementalWriter, or TerminalOutput), never a \
                                         buffering collector (L16)",
                                        cap_urn
                                    )));
                                }
                            }
                        }
                        if let Some(seq) = frame.is_sequence {
                            is_sequence = Some(seq);
                        }
                        if let Some(ref mut w) = writer {
                            let media = frame.media_urn.as_deref().unwrap_or("");
                            w.on_stream_start(
                                is_sequence,
                                media,
                                frame.meta.clone(),
                                frame.stream_id.clone(),
                            )
                            .await?;
                        } else {
                            // Capture stream-level meta for ForEach propagation
                            terminal_meta.stream_meta = frame.meta.clone();
                        }
                    }
                    _ => {
                        // STREAM_END and others — structural, skip
                    }
                }
            }
            Ok(None) => {
                let details = "response channel closed without END".to_string();
                emit_pipeline_log(
                    log_fn,
                    step_token_id,
                    cap_urn,
                    "error",
                    crate::failure::AttributionClass::Internal,
                    &details,
                    None,
                    body_index,
                    None,
                );
                // Engine-detected protocol violation — ours: Internal, no code.
                return Err(StreamIoError::Terminal {
                    cap_urn: cap_urn.to_string(),
                    code: None,
                    class: crate::failure::AttributionClass::Internal,
                    details,
                    arg_urn: None,
                });
            }
            Err(_timeout) => {
                // The producer is quiet. Flush any pending sub-batch grants
                // (L10 deadlock-freedom rule): the producer's send window may
                // be smaller than our grant batch, in which case it is stalled
                // waiting for exactly this credit.
                if let Some(plumbing) = credit {
                    for (stream_id, counter) in consumed_since_grant.iter_mut() {
                        if *counter > 0 {
                            (plumbing.grant)(stream_id.clone(), *counter);
                            *counter = 0;
                        }
                    }
                }
                // Per-cap activity-silence observation, NOT an abort.
                // Long-running terminal caps legitimately sit silent
                // for far longer than the threshold; cancellation is
                // the user's call via the explicit cancel-task path.
                // The runtime keeps waiting and emits a one-shot
                // warning at the threshold so the log shows "no
                // activity for Ns from cap X" without log-spam every
                // 500 ms.
                if timer.is_expired() && !activity_warning_logged {
                    // Credit-state dump (L8): if the terminal cap is silent
                    // because it is credit-starved, the pending-grant
                    // counters (all flushed just above, so non-zero means
                    // the flush path itself is broken) and total consumed
                    // bytes locate the starved edge.
                    let pending: Vec<String> = consumed_since_grant
                        .iter()
                        .map(|(sid, n)| format!("{:?}: pending_grants={}", sid, n))
                        .collect();
                    let details = format!(
                        "no activity for {}s — continuing to wait. Use Cancel to abort. \
                         Terminal credit state: consumed_bytes={} [{}]",
                        activity_timeout_secs,
                        response_chunks.len(),
                        pending.join("; "),
                    );
                    emit_pipeline_log(
                        log_fn,
                        step_token_id,
                        cap_urn,
                        "warn",
                        crate::failure::AttributionClass::Internal,
                        &details,
                        None,
                        body_index,
                        None,
                    );
                    tracing::warn!(
                        cap_urn = %cap_urn,
                        consumed_bytes = response_chunks.len(),
                        pending_grants = ?pending,
                        "[cap] No activity for {}s; continuing to wait for completion or cancel",
                        activity_timeout_secs
                    );
                    activity_warning_logged = true;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    // TEST1948: a LOG whose level is "progress" but whose numeric value is
    // missing fails as a MISSING PROGRESS VALUE, not as an attribution error.
    //
    // The two questions "is this frame functional progress?" and "must it carry
    // attribution?" have one answer — the level — but the receiver used to ask
    // them differently: it branched on whether a number parsed, while
    // `attribution_class()` branched on the level. A frame that said
    // level="progress" and carried no number fell between them and surfaced as
    // "Log frames do not carry attribution", sending the reader after the wrong
    // defect entirely.
    #[test]
    fn test1948_progress_log_without_a_value_names_the_missing_value() {
        use crate::bifaci::frame::{Frame, FrameType, MessageId};

        let mut frame = Frame::new(FrameType::Log, MessageId::Uint(1));
        let mut meta = std::collections::BTreeMap::new();
        meta.insert("level".to_string(), ciborium::Value::Text("progress".into()));
        meta.insert("message".to_string(), ciborium::Value::Text("working".into()));
        // No `progress` key: exactly the malformed frame that produced the
        // misleading attribution error.
        frame.meta = Some(meta);

        // The level says progress, so attribution is NOT what is missing.
        assert!(
            frame.attribution_class().is_err(),
            "a progress-level LOG carries no attribution by contract"
        );
        // ...and the frame genuinely has no readable progress value.
        assert!(
            frame.log_progress().is_none(),
            "the fixture must reproduce the absent numeric value"
        );
        // Which is what a receiver must report: the missing value, named.
        assert_eq!(frame.log_level(), Some("progress"));
        assert_eq!(
            frame.log_progress_slot_description(),
            "absent",
            "an absent value must be reported as absent"
        );

        // A DIFFERENT emitter defect — the key present but the wrong type —
        // must not produce the same description, or the error cannot tell the
        // two apart and the reader has to reproduce the failure to find out.
        let mut wrong_type = Frame::new(FrameType::Log, MessageId::Uint(2));
        let mut meta2 = std::collections::BTreeMap::new();
        meta2.insert("level".to_string(), ciborium::Value::Text("progress".into()));
        meta2.insert("message".to_string(), ciborium::Value::Text("working".into()));
        meta2.insert("progress".to_string(), ciborium::Value::Text("0.5".into()));
        wrong_type.meta = Some(meta2);
        assert!(
            wrong_type.log_progress().is_none(),
            "a text progress value is not readable as a number"
        );
        assert_eq!(
            wrong_type.log_progress_slot_description(),
            "a text value",
            "a wrong-typed value must be named as such, distinctly from absent"
        );
        assert_ne!(
            wrong_type.log_progress_slot_description(),
            frame.log_progress_slot_description(),
            "the two emitter defects must be distinguishable from the error alone"
        );

        // And a well-formed progress frame must NOT trip any of this.
        let good = Frame::progress(MessageId::Uint(3), 0.5, "working");
        assert_eq!(good.log_progress(), Some(0.5));
    }

    use super::*;
    use std::sync::Mutex;

    /// Capture progress and log callback invocations for assertions.
    struct Captured {
        progress: Arc<Mutex<Vec<(f32, String)>>>,
        logs: Arc<Mutex<Vec<(String, String)>>>,
        progress_fn: CapProgressFn,
        log_fn: PipelineLogFn,
    }

    fn capture() -> Captured {
        let progress: Arc<Mutex<Vec<(f32, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let logs: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let p2 = Arc::clone(&progress);
        let l2 = Arc::clone(&logs);
        Captured {
            progress,
            logs,
            progress_fn: Arc::new(move |p, _cap, msg| {
                p2.lock().unwrap().push((p, msg.to_string()));
            }),
            log_fn: Arc::new(move |record| {
                l2.lock().unwrap().push((record.level, record.message));
            }),
        }
    }

    /// The contract every existing fixture emits under: a scalar, bounded
    /// output (`stream_start` sends is_sequence=false, no unbounded flag).
    const SCALAR_CONTRACT: OutputContract = OutputContract {
        is_sequence: false,
        streaming: false,
    };
    /// The contract of a cap that legitimately emits an UNBOUNDED sequence
    /// (a live feed consumer, a streaming transcriber): the fixtures that
    /// announce `stream_start_unbounded(.., Some(true))` declare it.
    const STREAMING_SEQUENCE_CONTRACT: OutputContract = OutputContract {
        is_sequence: true,
        streaming: true,
    };

    fn stream_start(rid: &MessageId) -> Frame {
        Frame::stream_start(
            rid.clone(),
            "out".to_string(),
            "media:enc=utf-8".to_string(),
            Some(false),
        )
    }

    fn chunk(rid: &MessageId, payload: &[u8]) -> Frame {
        let checksum = Frame::compute_checksum(payload);
        Frame::chunk(
            rid.clone(),
            "out".to_string(),
            0,
            payload.to_vec(),
            0,
            checksum,
        )
    }

    // TEST8124: the effect audit fires at output STREAM_START receipt — an
    // effect=none cap whose emission is not tag-equivalent to its runtime
    // input fails hard with the violation's typed identity, before any data
    // is collected.
    #[tokio::test]
    async fn test8124_effect_audit_rejects_nonconformant_terminal_emission() {
        let rid = MessageId::new_uuid();
        let (tx, rx) = mpsc::unbounded_channel();
        // cap:echo;effect=none declares in=media: — its emission must be
        // tag-equivalent to media:, and media:enc=utf-8 is not.
        tx.send(Frame::stream_start(
            rid.clone(),
            "out".to_string(),
            "media:enc=utf-8".to_string(),
            Some(false),
        ))
        .unwrap();
        tx.send(chunk(&rid, b"payload")).unwrap();
        tx.send(Frame::end_ok(rid.clone(), None)).unwrap();
        drop(tx);

        let err = collect_terminal_output(
            rx,
            None,
            "cap:echo;effect=none",
            SCALAR_CONTRACT,
            &"step_test".parse().unwrap(),
            None,
            None,
            None,
            None,
            None,
            5,
            None,
        )
        .await
        .expect_err("a nonconformant emission must fail the collect");
        match err {
            StreamIoError::EffectContract {
                cap_urn,
                effect,
                runtime_input,
                expected,
                actual,
            } => {
                assert_eq!(cap_urn, "cap:echo;effect=none");
                assert_eq!(effect, "none");
                assert_eq!(runtime_input, "media:");
                assert_eq!(expected, "media:");
                assert_eq!(actual, "media:enc=utf-8");
            }
            other => panic!("expected EffectContract, got: {other}"),
        }
    }

    // TEST1957: the stream-contract audit fires at receipt, on the same
    // STREAM_START as the effect audit: an UNBOUNDED emission from an output
    // declared `streaming: false`, and an emission whose cardinality mode
    // differs from the declared `is_sequence`, each fail the collect with a
    // typed violation naming both sides; an emission inside the contract
    // passes. `is_sequence` had never been audited before this.
    #[tokio::test]
    async fn test1957_stream_contract_audit_at_receipt() {
        use crate::bifaci::frame::FrameType;
        let rid = MessageId::new_uuid();
        let start_rid = rid.clone();
        let start = move |is_sequence: bool, unbounded: bool| {
            let mut frame = Frame::stream_start(
                start_rid.clone(),
                "out".to_string(),
                "media:".to_string(),
                Some(is_sequence),
            );
            if unbounded {
                frame.unbounded = Some(true);
            }
            assert_eq!(frame.frame_type, FrameType::StreamStart);
            frame
        };
        let run = |first: Frame, contract: OutputContract| {
            let rid = rid.clone();
            async move {
            let (tx, rx) = mpsc::unbounded_channel();
            tx.send(first).unwrap();
            tx.send(chunk(&rid, b"payload")).unwrap();
            tx.send(Frame::end_ok(rid.clone(), None)).unwrap();
            drop(tx);
            collect_terminal_output(
                rx,
                None,
                "cap:echo;effect=none",
                contract,
                &"step_test".parse().unwrap(),
                None,
                None,
                None,
                None,
                None,
                5,
                None,
            )
            .await
            }
        };

        // Unbounded from a non-streaming output.
        match run(start(false, true), SCALAR_CONTRACT).await {
            Err(StreamIoError::StreamContract {
                cap_urn,
                declared_is_sequence,
                declared_streaming,
                emitted_is_sequence,
                emitted_unbounded,
            }) => {
                assert_eq!(cap_urn, "cap:echo;effect=none");
                assert!(!declared_is_sequence && !declared_streaming);
                assert!(!emitted_is_sequence && emitted_unbounded);
            }
            other => panic!("expected StreamContract, got: {other:?}"),
        }

        // Sequence mode from an output declared scalar.
        match run(start(true, false), SCALAR_CONTRACT).await {
            Err(StreamIoError::StreamContract {
                emitted_is_sequence,
                emitted_unbounded,
                ..
            }) => {
                assert!(emitted_is_sequence && !emitted_unbounded);
            }
            other => panic!("expected StreamContract, got: {other:?}"),
        }

        // Inside the contract: a streaming sequence output emitting a bounded
        // sequence (a streaming output MAY be unbounded; this emission is
        // not, so the buffering collector may take it). An unbounded emission
        // under the same contract passes the audit too and is then refused by
        // the collector on L16 grounds — a different check, tested by
        // TEST8135.
        run(start(true, false), STREAMING_SEQUENCE_CONTRACT)
            .await
            .expect("an emission inside the declared contract passes");
    }

    // TEST8125: the incremental terminal consumer audits STREAM_START the
    // same way — a nonconformant emission surfaces as the first item error
    // and the stream yields nothing further.
    #[tokio::test]
    async fn test8125_effect_audit_rejects_nonconformant_incremental_emission() {
        let rid = MessageId::new_uuid();
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(Frame::stream_start(
            rid.clone(),
            "out".to_string(),
            "media:enc=utf-8".to_string(),
            Some(false),
        ))
        .unwrap();
        tx.send(chunk(&rid, b"payload")).unwrap();
        tx.send(Frame::end_ok(rid.clone(), None)).unwrap();
        drop(tx);

        let mut terminal =
            TerminalOutput::new(rx, "cap:echo;effect=none", SCALAR_CONTRACT, &"step_test".parse().unwrap(), None, None, None)
                .expect("cap:echo;effect=none builds a valid effect audit");
        let first = terminal
            .next_item()
            .await
            .expect("the audit failure is delivered as an item error");
        assert!(
            matches!(first, Err(StreamIoError::EffectContract { .. })),
            "expected EffectContract, got: {first:?}"
        );
        assert!(
            terminal.next_item().await.is_none(),
            "a failed audit terminates the stream"
        );
    }

    // TEST8135: an unbounded terminal without a persisted sink is refused by
    // the buffering collector (L16) — routed loudly to incremental
    // consumption, never buffered into unbounded memory and never an OOM.
    #[tokio::test]
    async fn test8135_unbounded_terminal_refused_without_writer() {
        let rid = MessageId::new_uuid();
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(Frame::stream_start_unbounded(
            rid.clone(),
            "out".to_string(),
            "media:enc=utf-8".to_string(),
            Some(true),
        ))
        .unwrap();
        drop(tx);

        let err = collect_terminal_output(
            rx,
            None,
            "cap:test",
            STREAMING_SEQUENCE_CONTRACT,
            &"step_test".parse().unwrap(),
            None,
            None,
            None,
            None,
            None,
            5,
            None,
        )
        .await
        .expect_err("an unbounded terminal must not be buffered");
        assert!(
            err.to_string().contains("UNBOUNDED"),
            "the refusal names the cause: {err}"
        );
    }

    // TEST8145: SpoolWriter — the disk spool for an unbounded INTERMEDIATE at
    // a chain-split boundary. Sequence fragments append verbatim so the file
    // IS the RFC 8742 node_data form (items split across payloads included);
    // blob chunks are unwrapped to raw bytes; a second STREAM_START refuses.
    #[tokio::test]
    async fn test8145_spool_writer_spools_node_data_forms() {
        let dir = tempfile::tempdir().unwrap();

        // Sequence: two CBOR Bytes items, the second split across payloads.
        let mut item0 = Vec::new();
        ciborium::into_writer(&ciborium::Value::Bytes(b"win0".to_vec()), &mut item0).unwrap();
        let mut item1 = Vec::new();
        ciborium::into_writer(&ciborium::Value::Bytes(b"win1".to_vec()), &mut item1).unwrap();
        let mut sp = Box::new(super::SpoolWriter::new(dir.path().join("seq.spool")));
        assert!(!sp.engaged());
        sp.on_stream_start(Some(true), "media:record", None, None)
            .await
            .unwrap();
        assert!(sp.engaged());
        sp.on_chunk_payload(&item0, None).await.unwrap();
        let (a, b) = item1.split_at(2);
        sp.on_chunk_payload(a, None).await.unwrap();
        sp.on_chunk_payload(b, None).await.unwrap();
        sp.on_stream_end().await.unwrap();
        assert!(sp.is_sequence());
        let err = sp
            .on_stream_start(Some(true), "media:record", None, None)
            .await
            .expect_err("a chain sink is exactly one stream");
        assert!(err.to_string().contains("second STREAM_START"), "{err}");
        let path = sp.path().to_path_buf();
        let result = (sp as Box<dyn IncrementalWriter>).finish();
        assert!(result.is_sequence);
        let on_disk = std::fs::read(&path).unwrap();
        let expected: Vec<u8> = [item0.as_slice(), item1.as_slice()].concat();
        assert_eq!(on_disk, expected, "file is the concatenated CBOR sequence");
        let items = crate::orchestrator::cbor_util::split_cbor_sequence(&on_disk).unwrap();
        assert_eq!(items.len(), 2, "both items round-trip");

        // Blob: chunks are complete CBOR Bytes values; raw bytes append.
        let mut sp = Box::new(super::SpoolWriter::new(dir.path().join("blob.spool")));
        sp.on_stream_start(Some(false), "media:audio;ext=wav", None, None)
            .await
            .unwrap();
        let mut c0 = Vec::new();
        ciborium::into_writer(&ciborium::Value::Bytes(b"RIFF".to_vec()), &mut c0).unwrap();
        let mut c1 = Vec::new();
        ciborium::into_writer(&ciborium::Value::Bytes(b"data".to_vec()), &mut c1).unwrap();
        sp.on_chunk_payload(&c0, None).await.unwrap();
        sp.on_chunk_payload(&c1, None).await.unwrap();
        sp.on_stream_end().await.unwrap();
        let path = sp.path().to_path_buf();
        let result = (sp as Box<dyn IncrementalWriter>).finish();
        assert!(!result.is_sequence);
        assert_eq!(result.total_bytes, 8);
        assert_eq!(std::fs::read(&path).unwrap(), b"RIFFdata");
    }

    // TEST8147: the CBOR byte-string header used to stream a spooled BLOB
    // gather member as one split item — every length class encodes exactly
    // as a CBOR decoder expects, so the reassembled item decodes.
    #[test]
    fn test8147_cbor_bytes_header_encodes_every_length_class() {
        for len in [0u64, 1, 23, 24, 255, 256, 65_535, 65_536, 4_294_967_295, 4_294_967_296] {
            let header = super::cbor_bytes_header(len);
            // Decode the header + a truncated body via ciborium only for the
            // small classes (allocating 4GiB in a test is not a test).
            if len <= 65_536 {
                let mut value = header.clone();
                value.extend(std::iter::repeat(0xAB).take(len as usize));
                let decoded: ciborium::Value =
                    ciborium::de::from_reader(value.as_slice()).expect("header + body decodes");
                match decoded {
                    ciborium::Value::Bytes(b) => assert_eq!(b.len() as u64, len),
                    other => panic!("expected bytes, got {other:?}"),
                }
            } else {
                // Large classes: check the major type + length field shape.
                assert_eq!(header[0] & 0xE0, 0x40, "major type 2");
            }
        }
    }

    // TEST8146: an UNBOUNDED stream with no persist writer but a spool
    // available ENGAGES the spool instead of the L16 refusal — the mandatory
    // chain-split boundary streams to disk and the collect succeeds. This is
    // the mic → encode → transcribe → <same-cartridge cap> machine shape that
    // the refusal wrongly killed.
    #[tokio::test]
    async fn test8146_unbounded_intermediate_engages_spool() {
        let dir = tempfile::tempdir().unwrap();
        let rid = MessageId::new_uuid();
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(Frame::stream_start_unbounded(
            rid.clone(),
            "out".to_string(),
            "media:enc=utf-8;record".to_string(),
            Some(true),
        ))
        .unwrap();
        let mut item = Vec::new();
        ciborium::into_writer(&ciborium::Value::Bytes(b"window-1".to_vec()), &mut item).unwrap();
        tx.send(chunk(&rid, &item)).unwrap();
        tx.send(Frame::stream_end(rid.clone(), "out".to_string(), 1))
            .unwrap();
        tx.send(Frame::end_ok(rid.clone(), None)).unwrap();
        drop(tx);

        let mut spool = super::SpoolWriter::new(dir.path().join("mid.spool"));
        let (bytes, is_seq, _meta) = collect_terminal_output(
            rx,
            None,
            "cap:test",
            STREAMING_SEQUENCE_CONTRACT,
            &"step_test".parse().unwrap(),
            None,
            None,
            None,
            None,
            Some(&mut spool as &mut dyn IncrementalWriter),
            5,
            None,
        )
        .await
        .expect("the spool absorbs the unbounded intermediate");
        assert!(spool.engaged(), "STREAM_START-unbounded engages the spool");
        assert!(bytes.is_empty(), "nothing buffers in memory");
        assert_eq!(is_seq, Some(true));
        let on_disk = std::fs::read(spool.path()).unwrap();
        assert_eq!(on_disk, item, "the item streamed to disk verbatim");
    }

    // TEST7022: The receiver delivers final progress exactly once, sourced from END terminal metadata, defaulting to 1.0 on a plain successful END.
    #[tokio::test]
    async fn test7022_final_progress_from_end_meta_default() {
        let rid = MessageId::new_uuid();
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(stream_start(&rid)).unwrap();
        tx.send(chunk(&rid, b"payload")).unwrap();
        tx.send(Frame::progress(rid.clone(), 0.5, "halfway"))
            .unwrap();
        tx.send(Frame::end_ok(rid.clone(), None)).unwrap();
        drop(tx);

        let cap = capture();
        let (bytes, _seq, _meta) = collect_terminal_output(
            rx,
            Some(&cap.progress_fn),
            "cap:test",
            SCALAR_CONTRACT,
            &"step_test".parse().unwrap(),
            Some(&cap.log_fn),
            None,
            None,
            None,
            None,
            5,
            None,
        )
        .await
        .expect("clean END must succeed");
        assert_eq!(bytes, b"payload");

        let events = cap.progress.lock().unwrap().clone();
        assert_eq!(events.len(), 2, "streamed 0.5 + terminal 1.0");
        assert_eq!(events[0].0, 0.5);
        assert_eq!(events[1].0, 1.0, "END without explicit progress reads 1.0");
        assert_eq!(
            events.iter().filter(|(p, _)| *p >= 1.0).count(),
            1,
            "final progress is delivered exactly once (L5)"
        );
    }

    // TEST7023: A handler-declared terminal status (progress + message) in END metadata reaches the progress callback as the final event.
    #[tokio::test]
    async fn test7023_final_progress_handler_override() {
        let rid = MessageId::new_uuid();
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(stream_start(&rid)).unwrap();
        tx.send(chunk(&rid, b"x")).unwrap();
        tx.send(Frame::end_ok_with(
            rid.clone(),
            None,
            Some(0.87),
            Some("partial corpus"),
        ))
        .unwrap();
        drop(tx);

        let cap = capture();
        collect_terminal_output(
            rx,
            Some(&cap.progress_fn),
            "cap:test",
            SCALAR_CONTRACT,
            &"step_test".parse().unwrap(),
            Some(&cap.log_fn),
            None,
            None,
            None,
            None,
            5,
            None,
        )
        .await
        .unwrap();

        let events = cap.progress.lock().unwrap().clone();
        let last = events.last().expect("terminal event must be delivered");
        assert!((last.0 - 0.87).abs() < 1e-6);
        assert_eq!(last.1, "partial corpus");
    }

    // TEST7024: Frames already queued behind END are drained before returning — LOG messages are delivered, and no post-terminal progress value can regress the final progress.
    #[tokio::test]
    async fn test7024_drain_after_end_delivers_logs_without_progress_regression() {
        let rid = MessageId::new_uuid();
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(stream_start(&rid)).unwrap();
        tx.send(chunk(&rid, b"data")).unwrap();
        tx.send(Frame::end_ok(rid.clone(), None)).unwrap();
        // Stragglers already queued when END is processed (the enqueue race):
        tx.send(Frame::log(
            rid.clone(),
            "info",
            crate::AttributionClass::Internal,
            "flushed-after-end",
            None,
        ))
        .unwrap();
        tx.send(Frame::progress(rid.clone(), 0.95, "stale keepalive"))
            .unwrap();
        drop(tx);

        let cap = capture();
        collect_terminal_output(
            rx,
            Some(&cap.progress_fn),
            "cap:test",
            SCALAR_CONTRACT,
            &"step_test".parse().unwrap(),
            Some(&cap.log_fn),
            None,
            None,
            None,
            None,
            5,
            None,
        )
        .await
        .expect("drain must not fail the request");

        // Drained LOG messages are delivered (not lost) ...
        let logs = cap.logs.lock().unwrap().clone();
        assert!(
            logs.iter().any(|(_, m)| m == "flushed-after-end"),
            "post-END queued LOG message must be delivered: {:?}",
            logs
        );
        assert!(
            !logs.iter().any(|(_, m)| m == "stale keepalive"),
            "progress frames are functional signals, never ordinary diagnostics: {:?}",
            logs
        );

        // ... but the progress CALLBACK sequence ends at the terminal 1.0 —
        // the stale 0.95 must not regress it.
        let events = cap.progress.lock().unwrap().clone();
        let last = events.last().unwrap();
        assert_eq!(
            last.0, 1.0,
            "final progress event is END's, not a straggler"
        );
        assert!(
            !events
                .iter()
                .any(|(p, m)| *p == 0.95 && m == "stale keepalive"),
            "post-terminal progress value must not reach the progress callback"
        );
    }

    // TEST7071: The incremental terminal consumer yields items BEFORE the stream has ended — required for unbounded output (L16) — and completes on an unbounded STREAM_END + END.
    #[tokio::test]
    async fn test7071_terminal_output_yields_before_stream_end() {
        let rid = MessageId::new_uuid();
        let (tx, rx) = mpsc::unbounded_channel();
        let mut terminal = TerminalOutput::new(rx, "cap:test", STREAMING_SEQUENCE_CONTRACT, &"step_test".parse().unwrap(), None, None, None)
            .expect("cap:test builds a valid effect audit");

        // Announce an unbounded stream and one item — nothing has ended.
        let ss = Frame::stream_start_unbounded(
            rid.clone(),
            "out".to_string(),
            "media:enc=utf-8".to_string(),
            Some(true),
        );
        tx.send(ss).unwrap();
        tx.send(chunk(&rid, b"first")).unwrap();

        let item = terminal
            .next_item()
            .await
            .expect("item must be yielded live")
            .expect("no error");
        assert_eq!(item.payload, b"first");
        assert!(terminal.is_unbounded());
        assert_eq!(terminal.is_sequence(), Some(true));

        // TEST7072 behavior folded in: STREAM_END without chunk_count + END
        // complete the unbounded stream cleanly.
        tx.send(Frame::stream_end_unbounded(rid.clone(), "out".to_string()))
            .unwrap();
        tx.send(Frame::end_ok(rid.clone(), None)).unwrap();
        drop(tx);
        assert!(
            terminal.next_item().await.is_none(),
            "END completes the request"
        );
        let (_meta, is_seq) = terminal.finish().await.expect("clean completion");
        assert_eq!(is_seq, Some(true));
    }

    // TEST7077: Per-item stream metadata arrives WITH its item through incremental delivery, not batched at the end.
    #[tokio::test]
    async fn test7077_per_item_meta_incremental() {
        let rid = MessageId::new_uuid();
        let (tx, rx) = mpsc::unbounded_channel();
        let mut terminal = TerminalOutput::new(rx, "cap:test", SCALAR_CONTRACT, &"step_test".parse().unwrap(), None, None, None)
            .expect("cap:test builds a valid effect audit");

        tx.send(stream_start(&rid)).unwrap();

        let mut with_meta = chunk(&rid, b"item-0");
        let mut meta = StreamMeta::new();
        meta.insert(
            "title".to_string(),
            ciborium::Value::Text("page_0".to_string()),
        );
        with_meta.meta = Some(meta);
        tx.send(with_meta).unwrap();

        let item = terminal.next_item().await.unwrap().unwrap();
        assert_eq!(item.payload, b"item-0");
        let item_meta = item.meta.expect("meta must ride with its item");
        assert_eq!(
            item_meta.get("title"),
            Some(&ciborium::Value::Text("page_0".to_string()))
        );

        // A later item without meta yields None meta — no leakage between items.
        tx.send(chunk(&rid, b"item-1")).unwrap();
        let item = terminal.next_item().await.unwrap().unwrap();
        assert_eq!(item.payload, b"item-1");
        assert!(item.meta.is_none());

        tx.send(Frame::end_ok(rid, None)).unwrap();
        drop(tx);
        assert!(terminal.next_item().await.is_none());
    }
}
