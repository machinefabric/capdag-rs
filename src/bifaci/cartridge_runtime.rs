//! Cartridge Runtime - Unified I/O handling for cartridge binaries
//!
//! The CartridgeRuntime provides a unified interface for cartridge binaries to handle
//! cap invocations. Cartridges register handlers for caps they provide, and the
//! runtime handles all I/O mechanics:
//!
//! - **Automatic mode detection**: CLI mode vs Cartridge CBOR mode
//! - CBOR frame encoding/decoding (Cartridge mode)
//! - CLI argument parsing from cap definitions (CLI mode)
//! - Handler routing by cap URN
//! - Real-time streaming response support
//! - HELLO handshake for limit negotiation
//! - **Multiplexed concurrent request handling**
//!
//! # Invocation Modes
//!
//! - **No CLI arguments**: Cartridge CBOR mode - HELLO handshake, REQ/RES frames via stdin/stdout
//! - **Any CLI arguments**: CLI mode - parse args based on cap definitions
//!
//! # Example
//!
//! ```ignore
//! use capdag::CartridgeRuntime;
//!
//! fn main() {
//!     let manifest = build_manifest(); // Your manifest with caps
//!     let mut runtime = CartridgeRuntime::new(manifest);
//!
//!     runtime.register::<MyRequest, _>("cap:my-op;...", |request, output, peer| {
//!         output.log("info", capdag::AttributionClass::Internal, "Starting work...");
//!         output.emit_cbor(&ciborium::Value::Bytes(b"result".to_vec()))?;
//!         Ok(())
//!     });
//!
//!     // runtime.run() automatically detects CLI vs Cartridge CBOR mode
//!     runtime.run().unwrap();
//! }
//! ```

use crate::bifaci::frame::{FlowKey, Frame, FrameType, Limits, MessageId, SeqAssigner};
use crate::bifaci::io::{handshake_accept, CborError, FrameReader, FrameWriter};
use crate::bifaci::manifest::CapManifest;
use crate::cap::caller::CapArgumentValue;
use crate::cap::definition::{ArgSource, Cap, CapArg};
use crate::standard::caps::{CAP_ADAPTER_SELECTION, CAP_DISCARD, CAP_IDENTITY};
use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::{MediaUrn, MEDIA_FILE_PATH};
use async_trait::async_trait;
// crossbeam is used for demux_multi_stream (bridging sync stdin reads to async handlers)
use ops_rs::{DryContext, Op, OpError, OpMetadata, OpResult, WetContext};
use std::collections::{BTreeMap, HashMap};
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::task::JoinHandle;

/// Errors that can occur in the cartridge runtime
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("CBOR error: {0}")]
    Cbor(#[from] CborError),

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("No handler registered for cap: {0}")]
    NoHandler(String),

    #[error("Handler error: {0}")]
    Handler(String),

    /// A handler failure carrying its FULL identity: the machine-readable
    /// code the cartridge's typed error declares (`error_code()`), the
    /// failure class it declares (`attribution_class()` — whose problem it is),
    /// and the human message. This is what handler shims construct from
    /// typed errors instead of folding the code into message text; the ERR
    /// frame carries all three fields to the engine. Untyped failures stay
    /// `Handler(String)` and classify as Internal at the frame boundary.
    #[error("{code}: {message}")]
    Classified {
        code: String,
        class: crate::failure::AttributionClass,
        message: String,
        /// Media URN of the ARGUMENT the failure is attributed to, declared
        /// by the emit source when — and only when — the failure is about one
        /// argument (a malformed prompt, an oversized image). `None` means
        /// "not about one argument"; no layer ever infers it downstream
        /// (docs/failure-taxonomy.md).
        arg_urn: Option<String>,
    },

    #[error("Cap URN parse error: {0}")]
    CapUrn(String),

    #[error("Deserialization error: {0}")]
    Deserialize(String),

    #[error("Serialization error: {0}")]
    Serialize(String),

    #[error("Peer request error: {0}")]
    PeerRequest(String),

    #[error("Peer response error: {0}")]
    PeerResponse(String),

    #[error("CLI error: {0}")]
    Cli(String),

    #[error("Missing required argument: {0}")]
    MissingArgument(String),

    #[error("Unknown subcommand: {0}")]
    UnknownSubcommand(String),

    #[error("Manifest error: {0}")]
    Manifest(String),

    #[error("Corrupted data: {0}")]
    CorruptedData(String),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Stream error: {0}")]
    Stream(#[from] StreamError),
}

impl RuntimeError {
    /// The failure class this error DECLARES (docs/failure-taxonomy.md).
    /// `Classified` carries its origin's declaration; a remote peer error
    /// carries the class the PEER's frame declared; everything else is
    /// `Internal` — unclassified means "ours", never a guess.
    pub fn attribution_class(&self) -> crate::failure::AttributionClass {
        match self {
            RuntimeError::Classified { class, .. } => *class,
            RuntimeError::Stream(StreamError::RemoteError { class, .. }) => *class,
            _ => crate::failure::AttributionClass::Internal,
        }
    }

    /// The machine-readable code declared at the emit source, when carried.
    pub fn failure_code(&self) -> Option<&str> {
        match self {
            RuntimeError::Classified { code, .. } => Some(code),
            RuntimeError::Stream(StreamError::RemoteError { code, .. }) => Some(code),
            _ => None,
        }
    }

    /// Media URN of the argument the failure is attributed to, when the
    /// emit source declared one. A remote peer error carries the peer
    /// frame's own attribution. `None` everywhere else — never a guess.
    pub fn failure_arg_urn(&self) -> Option<&str> {
        match self {
            RuntimeError::Classified { arg_urn, .. } => arg_urn.as_deref(),
            RuntimeError::Stream(StreamError::RemoteError { arg_urn, .. }) => arg_urn.as_deref(),
            _ => None,
        }
    }

    /// The LEAF human reason — the origin's own message for classified
    /// failures, the Display chain otherwise.
    pub fn failure_reason(&self) -> String {
        match self {
            RuntimeError::Classified { message, .. } => message.clone(),
            RuntimeError::Stream(StreamError::RemoteError { message, .. }) => message.clone(),
            other => other.to_string(),
        }
    }

    /// Construct a classified handler failure — the cartridge-author entry
    /// point for typed errors: `code` from `error_code()`, `class` from
    /// `attribution_class()`, `message` for humans. No argument attribution;
    /// chain [`RuntimeError::with_arg_urn`] when the failure IS about one
    /// argument.
    pub fn classified(
        code: impl Into<String>,
        class: crate::failure::AttributionClass,
        message: impl Into<String>,
    ) -> Self {
        RuntimeError::Classified {
            code: code.into(),
            class,
            message: message.into(),
            arg_urn: None,
        }
    }

    /// Attribute a classified failure to the argument with the given media
    /// URN — the emit-source declaration that this failure is about ONE
    /// argument. Only classified variants carry the attribution channel;
    /// calling this on any other variant is a contract violation and panics
    /// (attribution without classification cannot reach the wire).
    pub fn with_arg_urn(mut self, urn: impl Into<String>) -> Self {
        match &mut self {
            RuntimeError::Classified { arg_urn, .. } => *arg_urn = Some(urn.into()),
            other => panic!(
                "with_arg_urn on unclassified RuntimeError::{:?} — attribute at a classified emit source only",
                other
            ),
        }
        self
    }
}

/// The handler-side Runtime→Op boundary, used by every cartridge's op
/// wrappers: a classified runtime error crosses into the op layer with its
/// declared identity intact (docs/failure-taxonomy.md); an unclassified one
/// stays a plain execution failure.
impl From<RuntimeError> for ops_rs::OpError {
    fn from(e: RuntimeError) -> Self {
        match e.failure_code() {
            Some(code) => ops_rs::OpError::Classified {
                code: code.to_string(),
                class: e.attribution_class(),
                message: e.failure_reason(),
                arg_urn: e.failure_arg_urn().map(str::to_string),
            },
            None => ops_rs::OpError::ExecutionFailed(e.to_string()),
        }
    }
}

#[cfg(unix)]
type HandshakeStdout = tokio::net::unix::pipe::Sender;

#[cfg(windows)]
type HandshakeStdout = tokio::fs::File;

struct CborStdout {
    handshake_stdout: HandshakeStdout,
    frame_stdout: FrameStdout,
}

#[cfg(unix)]
struct FrameStdout {
    safe_fd: std::os::fd::RawFd,
}

#[cfg(windows)]
struct FrameStdout {
    file: std::fs::File,
}

#[cfg(unix)]
fn prepare_cbor_stdout() -> io::Result<CborStdout> {
    use std::os::fd::{FromRawFd, OwnedFd};

    let safe_fd = unsafe { libc::dup(libc::STDOUT_FILENO) };
    if safe_fd < 0 {
        return Err(io::Error::last_os_error());
    }

    let redirect_rc = unsafe { libc::dup2(libc::STDERR_FILENO, libc::STDOUT_FILENO) };
    if redirect_rc < 0 {
        let err = io::Error::last_os_error();
        unsafe {
            libc::close(safe_fd);
        }
        return Err(err);
    }

    let handshake_fd = unsafe { libc::dup(safe_fd) };
    if handshake_fd < 0 {
        let err = io::Error::last_os_error();
        unsafe {
            libc::close(safe_fd);
        }
        return Err(err);
    }

    let handshake_stdout = tokio::net::unix::pipe::Sender::from_owned_fd(unsafe {
        OwnedFd::from_raw_fd(handshake_fd)
    })?;

    Ok(CborStdout {
        handshake_stdout,
        frame_stdout: FrameStdout { safe_fd },
    })
}

#[cfg(windows)]
fn prepare_cbor_stdout() -> io::Result<CborStdout> {
    use std::os::windows::io::FromRawHandle;
    use windows_sys::Win32::Foundation::{
        DuplicateHandle, DUPLICATE_SAME_ACCESS, HANDLE, INVALID_HANDLE_VALUE,
    };
    use windows_sys::Win32::System::Console::{
        GetStdHandle, SetStdHandle, STD_ERROR_HANDLE, STD_OUTPUT_HANDLE,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    fn invalid_handle(handle: HANDLE) -> bool {
        handle.is_null() || handle == INVALID_HANDLE_VALUE
    }

    fn duplicate_handle(handle: HANDLE) -> io::Result<HANDLE> {
        let current_process = unsafe { GetCurrentProcess() };
        let mut duplicated: HANDLE = std::ptr::null_mut();
        let ok = unsafe {
            DuplicateHandle(
                current_process,
                handle,
                current_process,
                &mut duplicated,
                0,
                0,
                DUPLICATE_SAME_ACCESS,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(duplicated)
    }

    let stdout = unsafe { GetStdHandle(STD_OUTPUT_HANDLE) };
    if invalid_handle(stdout) {
        return Err(io::Error::last_os_error());
    }
    let stderr = unsafe { GetStdHandle(STD_ERROR_HANDLE) };
    if invalid_handle(stderr) {
        return Err(io::Error::last_os_error());
    }

    let frame_handle = duplicate_handle(stdout)?;
    let handshake_handle = duplicate_handle(stdout)?;
    let redirected_stdout = duplicate_handle(stderr)?;
    if unsafe { SetStdHandle(STD_OUTPUT_HANDLE, redirected_stdout) } == 0 {
        return Err(io::Error::last_os_error());
    }

    let handshake_file = unsafe { std::fs::File::from_raw_handle(handshake_handle.cast()) };
    let frame_file = unsafe { std::fs::File::from_raw_handle(frame_handle.cast()) };

    Ok(CborStdout {
        handshake_stdout: tokio::fs::File::from_std(handshake_file),
        frame_stdout: FrameStdout { file: frame_file },
    })
}

#[cfg(unix)]
impl FrameStdout {
    fn into_file(self) -> io::Result<std::fs::File> {
        use std::os::fd::FromRawFd;

        let flags = unsafe { libc::fcntl(self.safe_fd, libc::F_GETFL) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        if flags & libc::O_NONBLOCK != 0 {
            let rc = unsafe { libc::fcntl(self.safe_fd, libc::F_SETFL, flags & !libc::O_NONBLOCK) };
            if rc < 0 {
                return Err(io::Error::last_os_error());
            }
        }

        Ok(unsafe { std::fs::File::from_raw_fd(self.safe_fd) })
    }
}

#[cfg(windows)]
impl FrameStdout {
    fn into_file(self) -> io::Result<std::fs::File> {
        Ok(self.file)
    }
}

// =============================================================================
// STREAM ABSTRACTIONS — hide the frame protocol from handlers
// =============================================================================

/// Per-stream or per-item metadata carried on frames.
///
/// In non-sequence mode, set once on STREAM_START — describes the whole stream.
/// In sequence mode, set per-item on CHUNK frames — describes each item.
pub type StreamMeta = BTreeMap<String, ciborium::Value>;

/// Errors that can occur during stream operations.
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    /// The peer's ERR frame, kept STRUCTURAL: its machine-readable code, the
    /// failure class the peer's frame declared (docs/failure-taxonomy.md),
    /// its message, and the peer's argument attribution when its frame
    /// carried one — never folded into prose.
    #[error("Remote error [{code}]: {message}")]
    RemoteError {
        code: String,
        class: crate::failure::AttributionClass,
        message: String,
        arg_urn: Option<String>,
    },

    #[error("Stream closed")]
    Closed,

    #[error("CBOR decode error: {0}")]
    Decode(String),

    #[error("I/O error: {0}")]
    Io(String),

    #[error("Protocol error: {0}")]
    Protocol(String),
}

fn remote_error_fields(
    frame: &Frame,
) -> Result<
    (
        String,
        crate::failure::AttributionClass,
        String,
        Option<String>,
    ),
    String,
> {
    let code = frame
        .error_code()
        .ok_or_else(|| "ERR frame missing required text code".to_string())?
        .to_string();
    let message = frame
        .error_message()
        .ok_or_else(|| "ERR frame missing required text message".to_string())?
        .to_string();
    let class = frame.attribution_class()?;
    let arg_urn = frame.attribution_arg_urn()?.map(str::to_string);
    Ok((code, class, message, arg_urn))
}

/// Allows sending frames directly through the output channel.
/// Internal to the runtime — handlers never see this.
pub trait FrameSender: Send + Sync {
    fn send(&self, frame: &Frame) -> Result<(), RuntimeError>;
}

/// A single input stream — yields decoded CBOR values from CHUNK frames.
/// Handler never sees Frame, STREAM_START, STREAM_END, checksum, seq, or index.
///
/// This is an async stream. Use `recv()` to get the next value with metadata,
/// or `recv_data()` / `collect_*` methods if you only need the data.
///
/// Metadata semantics depend on mode:
/// - Non-sequence: `stream_meta()` returns the STREAM_START metadata (whole-stream).
/// - Sequence: `recv()` delivers per-item metadata from CHUNK frames.
pub struct InputStream {
    media_urn: String,
    stream_meta: Option<StreamMeta>,
    rx: InputRx,
    /// Whether the sender declared this stream unbounded (no length promise).
    /// Buffering collectors refuse unbounded streams (L16).
    unbounded: bool,
    /// Grant emitter: consuming chunks replenishes the sender's window (L10).
    /// None = uncredited context (in-process host, tests).
    grants: Option<InputGrantEmitter>,
}

/// The delivery channel behind an `InputStream`. Wire-fed streams use an
/// unbounded channel (backpressure is the wire credit window, L10);
/// runtime-resolved LIVE FEEDS use a BOUNDED channel — the op-side stage of
/// the live backpressure chain (12.5 §Overrun): a lagging consumer fills
/// this channel, which blocks the feed's feeder, which fills the capture
/// ring, which applies the overrun policy at the capture edge.
pub(crate) enum InputRx {
    Unbounded(
        tokio::sync::mpsc::UnboundedReceiver<
            Result<(ciborium::Value, Option<StreamMeta>), StreamError>,
        >,
    ),
    Bounded(
        tokio::sync::mpsc::Receiver<
            Result<(ciborium::Value, Option<StreamMeta>), StreamError>,
        >,
    ),
}

impl InputRx {
    fn try_recv(
        &mut self,
    ) -> Result<
        Result<(ciborium::Value, Option<StreamMeta>), StreamError>,
        tokio::sync::mpsc::error::TryRecvError,
    > {
        match self {
            InputRx::Unbounded(rx) => rx.try_recv(),
            InputRx::Bounded(rx) => rx.try_recv(),
        }
    }

    async fn recv(
        &mut self,
    ) -> Option<Result<(ciborium::Value, Option<StreamMeta>), StreamError>> {
        match self {
            InputRx::Unbounded(rx) => rx.recv().await,
            InputRx::Bounded(rx) => rx.recv().await,
        }
    }
}

/// Emits CREDIT grants for one input stream as the handler consumes it (L10).
/// Grants are batched: one CREDIT per `batch` consumed chunks.
pub(crate) struct InputGrantEmitter {
    sender: Arc<dyn FrameSender>,
    rid: MessageId,
    xid: Option<MessageId>,
    /// Some = grant a specific stream; None = grant the request's sole stream
    /// (single-stream peer responses).
    stream_id: Option<String>,
    /// Which side's stream these grants credit (routing discriminator, L11):
    /// Request for handler-input consumption, Response for peer-response
    /// consumption.
    direction: crate::bifaci::frame::CreditDirection,
    batch: u64,
    consumed_since_grant: u64,
    /// Shared with the demux's violation accounting: granting extends the
    /// window the demux checks arriving chunks against.
    window: Arc<std::sync::atomic::AtomicI64>,
}

impl InputGrantEmitter {
    /// Record one consumed chunk; emit a batched CREDIT grant when due.
    fn consumed(&mut self) {
        self.consumed_since_grant += 1;
        if self.consumed_since_grant >= self.batch {
            self.flush();
        }
    }

    /// Build a second emitter over the SAME window/sender for the demux's
    /// fragment crediting on sequence streams, with `batch = 1` so every
    /// grant flushes immediately. Immediate flushing is load-bearing: the
    /// demux only runs when frames arrive, so a batched (held) grant while
    /// the producer is stalled on exactly that credit would deadlock the
    /// stream mid-item (L10 has no other flush point inside the demux).
    fn fragment_sibling(&self) -> InputGrantEmitter {
        InputGrantEmitter {
            sender: Arc::clone(&self.sender),
            rid: self.rid.clone(),
            xid: self.xid.clone(),
            stream_id: self.stream_id.clone(),
            direction: self.direction,
            batch: 1,
            consumed_since_grant: 0,
            window: Arc::clone(&self.window),
        }
    }

    /// Emit any pending (sub-batch) grant immediately.
    ///
    /// Deadlock-freedom rule (L10): a receiver MUST flush pending grants
    /// before blocking on an empty input. Batching is a latency optimization
    /// negotiated per link — the sender's window may come from a DIFFERENT
    /// link's negotiation, so a sender can legally stall below this
    /// receiver's batch threshold. Flushing at the block point guarantees
    /// progress under any window/batch mismatch.
    fn flush(&mut self) {
        if self.consumed_since_grant == 0 {
            return;
        }
        let n = self.consumed_since_grant;
        self.consumed_since_grant = 0;
        self.window
            .fetch_add(n as i64, std::sync::atomic::Ordering::SeqCst);
        let mut frame = Frame::credit(self.rid.clone(), self.stream_id.clone(), n, self.direction);
        frame.routing_id = self.xid.clone();
        // A failed grant send means the runtime is shutting down; the
        // sender-side gate will be closed by the terminal path (counted
        // at the ChannelFrameSender).
        let _ = self.sender.send(&frame);
    }
}

/// Everything the demux needs to credit a request's input streams:
/// grant plumbing for the handler side and per-stream violation windows.
pub(crate) struct InputCreditContext {
    pub(crate) sender: Arc<dyn FrameSender>,
    pub(crate) rid: MessageId,
    pub(crate) xid: Option<MessageId>,
    pub(crate) initial_credit: u64,
}

impl InputStream {
    /// Media URN of this stream (from STREAM_START).
    pub fn media_urn(&self) -> &str {
        &self.media_urn
    }

    /// Stream-level metadata from STREAM_START (non-sequence mode).
    pub fn stream_meta(&self) -> Option<&StreamMeta> {
        self.stream_meta.as_ref()
    }

    /// Whether the sender declared this stream unbounded — no length promise;
    /// consume incrementally with `recv()`, never with the `collect_*`
    /// buffering helpers (L16).
    pub fn is_unbounded(&self) -> bool {
        self.unbounded
    }

    /// Receive the next CBOR value with per-item metadata from this stream.
    /// Returns None when the stream ends.
    ///
    /// Consumption replenishes the sender's flow-control window (L10) — a
    /// slow handler naturally throttles the producer.
    pub async fn recv(
        &mut self,
    ) -> Option<Result<(ciborium::Value, Option<StreamMeta>), StreamError>> {
        let item = match self.rx.try_recv() {
            Ok(item) => Some(item),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => None,
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                // About to block: flush pending grants first (L10 deadlock-
                // freedom rule) — the producer may be stalled waiting for
                // exactly this credit.
                if let Some(grants) = self.grants.as_mut() {
                    grants.flush();
                }
                self.rx.recv().await
            }
        };
        if let (Some(Ok(_)), Some(grants)) = (&item, self.grants.as_mut()) {
            grants.consumed();
        }
        item
    }

    /// Refuse buffering on unbounded streams (L16) — buffering an unbounded
    /// stream is unbounded memory; the failure must be explicit, not an OOM.
    fn check_bounded(&self, method: &str) -> Result<(), StreamError> {
        if self.unbounded {
            return Err(StreamError::Protocol(format!(
                "{} refused: stream is unbounded (no length promise) — consume incrementally with recv() (L16)",
                method
            )));
        }
        Ok(())
    }

    /// Receive the next CBOR value, discarding any per-item metadata.
    /// Convenience for handlers that don't use metadata.
    pub async fn recv_data(&mut self) -> Option<Result<ciborium::Value, StreamError>> {
        match self.rx.recv().await {
            Some(Ok((value, _meta))) => Some(Ok(value)),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }

    /// Collect each chunk as a separate item with its metadata.
    /// For sequence streams (is_sequence=true), each chunk is one item.
    /// Returns a Vec of (raw_bytes, optional_per_item_meta).
    pub async fn collect_items(
        mut self,
    ) -> Result<Vec<(Vec<u8>, Option<StreamMeta>)>, StreamError> {
        self.check_bounded("collect_items")?;
        let mut items = Vec::new();
        while let Some(item) = self.recv().await {
            let (value, meta) = item?;
            let bytes = match value {
                ciborium::Value::Bytes(b) => b,
                ciborium::Value::Text(s) => s.into_bytes(),
                other => {
                    let mut buf = Vec::new();
                    ciborium::into_writer(&other, &mut buf).map_err(|e| {
                        StreamError::Decode(format!("Failed to encode CBOR: {}", e))
                    })?;
                    buf
                }
            };
            items.push((bytes, meta));
        }
        Ok(items)
    }

    /// Collect all chunks into a single byte vector.
    /// Extracts inner bytes from Value::Bytes/Text and concatenates.
    /// Per-item metadata is discarded.
    ///
    /// Fails hard on streams declared unbounded (L16) — there is no finite
    /// buffer for a stream with no length promise.
    pub async fn collect_bytes(mut self) -> Result<Vec<u8>, StreamError> {
        self.check_bounded("collect_bytes")?;
        let mut result = Vec::new();
        while let Some(item) = self.recv().await {
            let (value, _meta) = item?;
            match value {
                ciborium::Value::Bytes(b) => result.extend(b),
                ciborium::Value::Text(s) => result.extend(s.into_bytes()),
                other => {
                    // For non-byte types, CBOR-encode them
                    let mut buf = Vec::new();
                    ciborium::into_writer(&other, &mut buf).map_err(|e| {
                        StreamError::Decode(format!("Failed to encode CBOR: {}", e))
                    })?;
                    result.extend(buf);
                }
            }
        }
        Ok(result)
    }

    /// Collect a single CBOR value (expects exactly one chunk).
    /// Per-item metadata is discarded.
    pub async fn collect_value(mut self) -> Result<ciborium::Value, StreamError> {
        self.check_bounded("collect_value")?;
        match self.recv().await {
            Some(Ok((value, _meta))) => Ok(value),
            Some(Err(e)) => Err(e),
            None => Err(StreamError::Closed),
        }
    }
}

/// A single item from a peer response — either decoded data or a LOG frame.
///
/// `PeerResponse::recv()` yields these interleaved in arrival order. Handlers
/// match on each variant to decide how to react (e.g., forward progress, accumulate data).
pub enum PeerResponseItem {
    /// A decoded CBOR data chunk from the peer response, with optional per-chunk metadata.
    Data(Result<ciborium::Value, StreamError>, Option<StreamMeta>),
    /// A LOG frame from the peer (progress, status messages, etc.).
    Log(Frame),
}

/// Response from a peer call — yields both data items and LOG frames from a single receiver.
///
/// The handler drains this with `recv()` and reacts to each `PeerResponseItem` as it arrives.
/// LOG frames are delivered in real-time as they arrive (not buffered until data starts).
/// Collection helpers fail if a LOG frame is present because silently dropping
/// source diagnostics would violate the protocol's attribution contract. Callers
/// that accept peer diagnostics must drain `recv()` or use a forwarding helper.
pub struct PeerResponse {
    rx: tokio::sync::mpsc::UnboundedReceiver<PeerResponseItem>,
    /// Consumption grants for the responding peer's output window (L10/L14).
    /// None = uncredited context (in-process host, synthetic test responses).
    grants: Option<InputGrantEmitter>,
}

impl PeerResponse {
    /// Receive the next item (data or LOG) from the peer response.
    /// Returns None when the stream ends.
    ///
    /// Data consumption replenishes the responding peer's output window —
    /// a slow consumer naturally throttles the producer (L10).
    pub async fn recv(&mut self) -> Option<PeerResponseItem> {
        let item = match self.rx.try_recv() {
            Ok(item) => Some(item),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => None,
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                // Flush pending grants before blocking (L10) — the responding
                // peer may be stalled on exactly this credit.
                if let Some(grants) = self.grants.as_mut() {
                    grants.flush();
                }
                self.rx.recv().await
            }
        };
        if let (Some(PeerResponseItem::Data(Ok(_), _)), Some(grants)) =
            (&item, self.grants.as_mut())
        {
            grants.consumed();
        }
        item
    }

    /// Construct a `PeerResponse` from a fixed byte payload, for use by
    /// stubbed peer invokers in tests. Yields exactly one `Data(Text)`
    /// item carrying `bytes` interpreted as UTF-8, then closes.
    ///
    /// Production code MUST NOT call this — real peer responses are built
    /// internally by the runtime as frames arrive over the wire. The
    /// `#[doc(hidden)]` tag keeps this off rustdoc; the `pub` is needed
    /// because it's invoked from peer-cartridge test crates.
    #[doc(hidden)]
    pub fn synthetic_text(bytes: Vec<u8>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        // UTF-8 may fail for arbitrary bytes; tests that hand us non-UTF-8
        // would deserve a panic from the test harness, so unwrap is
        // appropriate — non-UTF-8 in a JSON payload is a malformed test.
        let text = String::from_utf8(bytes)
            .expect("PeerResponse::synthetic_text: payload must be valid UTF-8");
        let _ = tx.send(PeerResponseItem::Data(
            Ok(ciborium::Value::Text(text)),
            None,
        ));
        // Dropping `tx` here closes the channel so `recv()` returns None
        // after the single data item.
        drop(tx);
        Self { rx, grants: None }
    }

    /// Collect finite peer data while preserving every peer side-channel frame.
    /// Progress is mapped into the caller's declared range; non-progress LOG
    /// frames retain the source's class and optional argument attribution.
    pub async fn collect_bytes_forwarding(
        mut self,
        output: &OutputStream,
        progress_base: f32,
        progress_weight: f32,
    ) -> Result<Vec<u8>, StreamError> {
        let mut result = Vec::new();
        while let Some(item) = self.recv().await {
            match item {
                PeerResponseItem::Data(Ok(value), _) => match value {
                    ciborium::Value::Bytes(bytes) => result.extend(bytes),
                    ciborium::Value::Text(text) => result.extend(text.into_bytes()),
                    other => {
                        let mut encoded = Vec::new();
                        ciborium::into_writer(&other, &mut encoded).map_err(|error| {
                            StreamError::Decode(format!("Failed to encode CBOR: {error}"))
                        })?;
                        result.extend(encoded);
                    }
                },
                PeerResponseItem::Data(Err(error), _) => return Err(error),
                PeerResponseItem::Log(frame) => {
                    let level = frame.log_level().ok_or_else(|| {
                        StreamError::Protocol("peer LOG missing required text level".to_string())
                    })?;
                    let message = frame.log_message().ok_or_else(|| {
                        StreamError::Protocol("peer LOG missing required text message".to_string())
                    })?;
                    if level == "progress" {
                        let progress = frame.log_progress().ok_or_else(|| {
                            // Carry the evidence: "absent" and "a text value"
                            // are different defects in the PEER, and an error
                            // that cannot tell them apart forces whoever reads
                            // it to reproduce the failure to learn which.
                            StreamError::Protocol(format!(
                                "peer progress LOG has no numeric progress — the `progress` \
                                 slot is {} (level is \"progress\", so a number is required)",
                                frame.log_progress_slot_description()
                            ))
                        })?;
                        output.progress(
                            progress_base + progress.clamp(0.0, 1.0) * progress_weight,
                            message,
                        );
                    } else {
                        let class = frame.attribution_class().map_err(StreamError::Protocol)?;
                        match frame.attribution_arg_urn().map_err(StreamError::Protocol)? {
                            Some(arg_urn) => {
                                output.log_for_argument(level, class, message, arg_urn)
                            }
                            None => output.log(level, class, message),
                        }
                    }
                }
            }
        }
        Ok(result)
    }

    /// Collect all data chunks into a single byte vector.
    ///
    /// Fails if a LOG frame is present; use `collect_bytes_forwarding` when the
    /// peer may emit progress or ordinary diagnostics.
    pub async fn collect_bytes(mut self) -> Result<Vec<u8>, StreamError> {
        let mut result = Vec::new();
        while let Some(item) = self.recv().await {
            match item {
                PeerResponseItem::Data(Ok(value), _meta) => match value {
                    ciborium::Value::Bytes(b) => result.extend(b),
                    ciborium::Value::Text(s) => result.extend(s.into_bytes()),
                    other => {
                        let mut buf = Vec::new();
                        ciborium::into_writer(&other, &mut buf).map_err(|e| {
                            StreamError::Decode(format!("Failed to encode CBOR: {}", e))
                        })?;
                        result.extend(buf);
                    }
                },
                PeerResponseItem::Data(Err(e), _) => return Err(e),
                PeerResponseItem::Log(_) => {
                    return Err(StreamError::Protocol(
                        "peer response emitted a LOG frame; collect with explicit diagnostic forwarding"
                            .to_string(),
                    ));
                }
            }
        }
        Ok(result)
    }

    /// Collect a single CBOR data value, requiring exactly one data chunk and
    /// no LOG frames anywhere in the response.
    pub async fn collect_value(mut self) -> Result<ciborium::Value, StreamError> {
        let mut value = None;
        while let Some(item) = self.recv().await {
            match item {
                PeerResponseItem::Data(Ok(next), _meta) => {
                    if value.replace(next).is_some() {
                        return Err(StreamError::Protocol(
                            "peer response contained more than one value".to_string(),
                        ));
                    }
                }
                PeerResponseItem::Data(Err(e), _) => return Err(e),
                PeerResponseItem::Log(_) => {
                    return Err(StreamError::Protocol(
                        "peer response emitted a LOG frame; collect with explicit diagnostic forwarding"
                            .to_string(),
                    ));
                }
            }
        }
        value.ok_or(StreamError::Closed)
    }
}

/// The bundle of all input arg streams for one request.
/// Yields InputStream objects as STREAM_START frames arrive from the wire.
/// Returns None after END frame (all args delivered).
///
/// This is an async stream. Use `recv()` to get the next stream.
pub struct InputPackage {
    rx: tokio::sync::mpsc::UnboundedReceiver<Result<InputStream, StreamError>>,
    _demux_handle: Option<tokio::task::JoinHandle<()>>,
}

impl InputPackage {
    /// Get the next input stream. Async - waits until STREAM_START or END.
    pub async fn recv(&mut self) -> Option<Result<InputStream, StreamError>> {
        self.rx.recv().await
    }

    /// Collect all streams' bytes into a single Vec<u8>.
    ///
    /// WARNING: Only call this if you know all streams are finite.
    /// Infinite streams will block forever.
    pub async fn collect_all_bytes(mut self) -> Result<Vec<u8>, StreamError> {
        let mut all = Vec::new();
        while let Some(stream_result) = self.recv().await {
            let stream = stream_result?;
            all.extend(stream.collect_bytes().await?);
        }
        Ok(all)
    }

    /// Collect each stream individually into a Vec of (media_urn, bytes) pairs.
    /// Each stream's bytes are accumulated separately — NOT concatenated.
    /// Use `find_stream()` helpers to retrieve args by URN pattern matching.
    ///
    /// WARNING: Only call this if you know all streams are finite.
    pub async fn collect_streams(
        mut self,
    ) -> Result<Vec<(String, Vec<u8>, Option<StreamMeta>)>, StreamError> {
        let mut result = Vec::new();
        while let Some(stream_result) = self.recv().await {
            let stream = stream_result?;
            let urn = stream.media_urn().to_string();
            let meta = stream.stream_meta().cloned();
            let bytes = stream.collect_bytes().await?;
            result.push((urn, bytes, meta));
        }
        Ok(result)
    }
}

/// Find a stream's bytes by exact URN equivalence.
///
/// Uses `MediaUrn::is_equivalent()` — matches only if both URNs have the
/// exact same tag set (order-independent). Both the caller and the cartridge
/// know the arg media URNs from the cap definition, so this is always an
/// exact match — never a subsumption/pattern match.
///
/// The `media_urn` parameter must be the FULL media URN from the cap arg
/// definition (e.g., `"media:enc=utf-8;model-spec"`).
pub fn find_stream<'a>(
    streams: &'a [(String, Vec<u8>, Option<StreamMeta>)],
    media_urn: &str,
) -> Option<&'a [u8]> {
    let target = match crate::MediaUrn::from_string(media_urn) {
        Ok(p) => p,
        Err(_) => return None,
    };
    streams.iter().find_map(|(urn_str, bytes, _meta)| {
        let urn = crate::MediaUrn::from_string(urn_str).ok()?;
        if target.is_equivalent(&urn).unwrap_or(false) {
            Some(bytes.as_slice())
        } else {
            None
        }
    })
}

/// Like `find_stream` but returns a UTF-8 string.
pub fn find_stream_str(
    streams: &[(String, Vec<u8>, Option<StreamMeta>)],
    media_urn: &str,
) -> Option<String> {
    find_stream(streams, media_urn).and_then(|b| String::from_utf8(b.to_vec()).ok())
}

/// Find a stream whose URN *conforms to* `pattern`. Use this when the
/// cap-arg URN declared in the cap TOML is a richer refinement of the
/// bare functional pattern the handler thinks about (e.g. cap TOML
/// declares `media:inference;limit;max-tokens;numeric;task;user`,
/// the handler thinks `media:max-tokens;numeric`). Equality
/// matching via [`find_stream`] silently misses the rich form,
/// the unmatched stream falls through to the text catch-all
/// downstream and overwrites the prompt body — that's the
/// gibberish-output bug class.
pub fn find_stream_conforming<'a>(
    streams: &'a [(String, Vec<u8>, Option<StreamMeta>)],
    pattern: &str,
) -> Option<&'a [u8]> {
    let p = match crate::MediaUrn::from_string(pattern) {
        Ok(p) => p,
        Err(_) => return None,
    };
    streams.iter().find_map(|(urn_str, bytes, _meta)| {
        let urn = crate::MediaUrn::from_string(urn_str).ok()?;
        if urn.conforms_to(&p).unwrap_or(false) {
            Some(bytes.as_slice())
        } else {
            None
        }
    })
}

/// Like `find_stream_conforming` but returns a UTF-8 string.
pub fn find_stream_str_conforming(
    streams: &[(String, Vec<u8>, Option<StreamMeta>)],
    pattern: &str,
) -> Option<String> {
    find_stream_conforming(streams, pattern).and_then(|b| String::from_utf8(b.to_vec()).ok())
}

/// Find the stream-level metadata (from STREAM_START) for a stream by media URN.
pub fn find_stream_meta<'a>(
    streams: &'a [(String, Vec<u8>, Option<StreamMeta>)],
    media_urn: &str,
) -> Option<&'a StreamMeta> {
    let target = match crate::MediaUrn::from_string(media_urn) {
        Ok(p) => p,
        Err(_) => return None,
    };
    streams.iter().find_map(|(urn_str, _bytes, meta)| {
        let urn = crate::MediaUrn::from_string(urn_str).ok()?;
        if target.is_equivalent(&urn).unwrap_or(false) {
            meta.as_ref()
        } else {
            None
        }
    })
}

/// Like `find_stream` but fails hard if not found.
pub fn require_stream<'a>(
    streams: &'a [(String, Vec<u8>, Option<StreamMeta>)],
    media_urn: &str,
) -> Result<&'a [u8], StreamError> {
    find_stream(streams, media_urn)
        .ok_or_else(|| StreamError::Protocol(format!("Missing required arg: {}", media_urn)))
}

/// Like `require_stream` but returns a UTF-8 string.
pub fn require_stream_str(
    streams: &[(String, Vec<u8>, Option<StreamMeta>)],
    media_urn: &str,
) -> Result<String, StreamError> {
    let bytes = require_stream(streams, media_urn)?;
    String::from_utf8(bytes.to_vec())
        .map_err(|e| StreamError::Decode(format!("Arg '{}' is not valid UTF-8: {}", media_urn, e)))
}

/// Detached progress/log emitter that can be moved into `spawn_blocking`.
///
/// Holds an `Arc<dyn FrameSender>` and the request routing info needed to
/// construct LOG frames. `Send + Sync + 'static` by construction.
#[derive(Clone)]
pub struct ProgressSender {
    sender: Arc<dyn FrameSender>,
    request_id: MessageId,
    routing_id: Option<MessageId>,
}

impl ProgressSender {
    /// Emit a progress update (0.0–1.0) with a human-readable status message.
    pub fn progress(&self, progress: f32, message: &str) {
        let mut frame = Frame::progress(self.request_id.clone(), progress, message);
        frame.routing_id = self.routing_id.clone();
        let _ = self.sender.send(&frame);
    }

    /// Emit a log message.
    pub fn log(
        &self,
        level: &str,
        attribution_class: crate::failure::AttributionClass,
        message: &str,
    ) {
        let mut frame = Frame::log(
            self.request_id.clone(),
            level,
            attribution_class,
            message,
            None,
        );
        frame.routing_id = self.routing_id.clone();
        let _ = self.sender.send(&frame);
    }

    /// Emit a log message attributed by the source to one argument media URN.
    pub fn log_for_argument(
        &self,
        level: &str,
        attribution_class: crate::failure::AttributionClass,
        message: &str,
        arg_urn: &str,
    ) {
        let mut frame = Frame::log(
            self.request_id.clone(),
            level,
            attribution_class,
            message,
            Some(arg_urn),
        );
        frame.routing_id = self.routing_id.clone();
        let _ = self.sender.send(&frame);
    }
}

/// Detachable handle that can emit CBOR data chunks from any thread
/// (including `spawn_blocking`).  Obtained via [`OutputStream::stream_sender`].
///
/// Like [`ProgressSender`], this is `Send + Sync + 'static` and does not
/// borrow the parent `OutputStream`.
///
/// **Important:** call [`OutputStream::start()`] *before* moving the
/// `StreamSender` into `spawn_blocking` so that the STREAM_START frame is
/// sent while the async context is still available.
pub struct StreamSender {
    sender: Arc<dyn FrameSender>,
    request_id: MessageId,
    routing_id: Option<MessageId>,
    stream_id: String,
    max_chunk: usize,
    /// Shared chunk_index counter (same instance as OutputStream).
    chunk_index: Arc<Mutex<u64>>,
    /// Shared chunk_count counter (same instance as OutputStream).
    chunk_count: Arc<Mutex<u64>>,
    /// Shared flow-control gate (same instance as OutputStream). Blocking
    /// acquisition — StreamSender lives on blocking threads by design.
    credit_gate: Option<Arc<crate::bifaci::credit::CreditGate>>,
    /// Write-coalescing buffer, SHARED with the owning `OutputStream` (see
    /// [`CoalesceBuf`]) so the runtime's close path flushes bytes buffered
    /// through either handle.
    coalesce: Arc<Mutex<CoalesceBuf>>,
}

impl StreamSender {
    /// Emit a single CBOR value as one or more CHUNK frames.
    ///
    /// Bytes values COALESCE (see [`CoalesceBuf`]): per-token emissions from
    /// blocking inference threads accumulate and ship as one CHUNK per
    /// size/age threshold instead of one frame per token. Non-Bytes values
    /// flush the buffer first so nothing overtakes buffered bytes.
    pub fn emit_cbor(&self, value: &ciborium::Value) -> Result<(), RuntimeError> {
        match value {
            ciborium::Value::Bytes(bytes) => {
                if bytes.is_empty() {
                    return Ok(());
                }
                if let Some(batch) = coalesce_append(&self.coalesce, bytes) {
                    self.send_bytes_batch(&batch)?;
                }
            }
            _ => {
                if let Some(batch) = coalesce_take(&self.coalesce) {
                    self.send_bytes_batch(&batch)?;
                }
                self.send_chunk(value)?;
            }
        }
        Ok(())
    }

    /// Ship one coalesced batch: split at `max_chunk`, one blocking credit
    /// per CHUNK (inside `send_chunk`).
    fn send_bytes_batch(&self, bytes: &[u8]) -> Result<(), RuntimeError> {
        let mut offset = 0;
        while offset < bytes.len() {
            let chunk_size = (bytes.len() - offset).min(self.max_chunk);
            let chunk_bytes = bytes[offset..offset + chunk_size].to_vec();
            self.send_chunk(&ciborium::Value::Bytes(chunk_bytes))?;
            offset += chunk_size;
        }
        Ok(())
    }

    fn send_chunk(&self, value: &ciborium::Value) -> Result<(), RuntimeError> {
        // Blocking credit acquisition (L9) — StreamSender is a
        // blocking-thread emitter by design.
        if let Some(gate) = &self.credit_gate {
            gate.blocking_acquire(1)
                .map_err(|e| RuntimeError::Handler(e.to_string()))?;
        }
        let mut cbor_payload = Vec::new();
        ciborium::into_writer(value, &mut cbor_payload)
            .map_err(|e| RuntimeError::Handler(format!("Failed to encode CBOR: {}", e)))?;

        let chunk_index = {
            let mut guard = self.chunk_index.lock().unwrap();
            let current = *guard;
            *guard += 1;
            current
        };
        {
            let mut guard = self.chunk_count.lock().unwrap();
            *guard += 1;
        }

        let checksum = Frame::compute_checksum(&cbor_payload);
        let mut frame = Frame::chunk(
            self.request_id.clone(),
            self.stream_id.clone(),
            0,
            cbor_payload,
            chunk_index,
            checksum,
        );
        frame.routing_id = self.routing_id.clone();
        self.sender.send(&frame)
    }
}

/// Writable stream handle for handler output or peer call arguments.
/// Manages STREAM_START/CHUNK/STREAM_END framing automatically.
/// Scalar-stream write coalescing: cap on BYTES buffered before a write
/// forces a flush. Small enough that a coalesced batch is far below any
/// negotiated `max_chunk`; large enough to fold a burst of per-token writes
/// (a few bytes each) into one CHUNK frame instead of hundreds.
pub(crate) const COALESCE_MAX_BYTES: usize = 4096;

/// Scalar-stream write coalescing: oldest buffered byte AGE that forces a
/// flush on the next write. Bounds live-preview latency to one write-gap:
/// during steady token generation every write older than this flushes the
/// batch, so the consumer's view lags by at most one token.
pub(crate) const COALESCE_MAX_AGE: std::time::Duration = std::time::Duration::from_millis(20);

/// Shared write-coalescing buffer for one SCALAR stream.
///
/// Chunk boundaries on a scalar stream are non-semantic — every receiver
/// decodes each CHUNK payload as one CBOR Bytes value and concatenates the
/// inner bytes — so folding many small writes into one chunk is invisible to
/// consumers while dividing frame count, credit traffic, and the relay's
/// per-frame work by the batch factor. Sequence streams are NEVER coalesced:
/// their chunk runs delimit items (per-item meta rides the first chunk), and
/// the mode exclusivity enforced by `check_mode` keeps this buffer empty in
/// sequence mode by construction.
///
/// The buffer is shared (`Arc`) between an `OutputStream` and every
/// `StreamSender` detached from it — same discipline as `chunk_index` — so
/// the runtime's close path flushes bytes buffered by EITHER handle. Nothing
/// is ever dropped: `close()` flushes before STREAM_END, and every non-Bytes
/// emission flushes first so ordering within the stream is preserved.
#[derive(Debug, Default)]
pub(crate) struct CoalesceBuf {
    buf: Vec<u8>,
    oldest: Option<std::time::Instant>,
}

/// Append `data`; returns a batch that is DUE (size or age threshold crossed)
/// and must be sent now, or None while the batch is still accumulating.
fn coalesce_append(coalesce: &Mutex<CoalesceBuf>, data: &[u8]) -> Option<Vec<u8>> {
    let mut g = coalesce.lock().unwrap_or_else(|e| e.into_inner());
    if g.buf.is_empty() {
        g.oldest = Some(std::time::Instant::now());
    }
    g.buf.extend_from_slice(data);
    let due = g.buf.len() >= COALESCE_MAX_BYTES
        || g
            .oldest
            .map(|t| t.elapsed() >= COALESCE_MAX_AGE)
            .unwrap_or(false);
    if due {
        g.oldest = None;
        Some(std::mem::take(&mut g.buf))
    } else {
        None
    }
}

/// Take whatever is buffered, unconditionally. The flush/close/ordering
/// barrier primitive.
fn coalesce_take(coalesce: &Mutex<CoalesceBuf>) -> Option<Vec<u8>> {
    let mut g = coalesce.lock().unwrap_or_else(|e| e.into_inner());
    if g.buf.is_empty() {
        return None;
    }
    g.oldest = None;
    Some(std::mem::take(&mut g.buf))
}

pub struct OutputStream {
    sender: Arc<dyn FrameSender>,
    stream_id: String,
    media_urn: String,
    request_id: MessageId,
    routing_id: Option<MessageId>,
    max_chunk: usize,
    /// None = not started, Some(false) = write mode, Some(true) = sequence mode
    stream_mode: Mutex<Option<bool>>,
    chunk_index: Arc<Mutex<u64>>,
    chunk_count: Arc<Mutex<u64>>,
    closed: AtomicBool,
    /// Handler-declared terminal status (progress + message), delivered in the
    /// END frame's terminal metadata (L3/L5). Unset means the runtime stamps
    /// the default: progress 1.0 on success. Shared with the runtime via
    /// `final_status_handle()`.
    final_status: Arc<Mutex<Option<FinalStatus>>>,
    /// Whether this stream was started unbounded (no length promise, L16).
    unbounded: AtomicBool,
    /// Per-stream flow-control window (L9). One credit is acquired per CHUNK
    /// before it is enqueued; the receiver replenishes via CREDIT frames.
    /// None = uncredited context (CLI mode, tests, in-process host) — writes
    /// never wait.
    credit_gate: Option<Arc<crate::bifaci::credit::CreditGate>>,
    /// Router the gate registers with on `start()` so inbound CREDIT frames
    /// find it. Present iff `credit_gate` is.
    credit_router: Option<crate::bifaci::credit::CreditRouter>,
    /// Write-coalescing buffer, shared with detached [`StreamSender`]s (see
    /// [`CoalesceBuf`]).
    coalesce: Arc<Mutex<CoalesceBuf>>,
}

/// A handler's terminal status override, carried in END terminal metadata.
#[derive(Debug, Clone)]
pub struct FinalStatus {
    pub progress: f64,
    pub message: Option<String>,
}

/// `FrameSender` that drops every frame. Used by `OutputStream::discarding`
/// to construct an output that swallows logs and emit calls — handy for
/// unit tests of cap-handler logic that don't care about the wire output.
struct DiscardingFrameSender;

impl FrameSender for DiscardingFrameSender {
    fn send(&self, _frame: &Frame) -> Result<(), RuntimeError> {
        Ok(())
    }
}

impl OutputStream {
    /// Build an `OutputStream` whose every frame is silently dropped.
    /// Intended for tests that exercise handler logic without
    /// inspecting emitted frames; never use in production code where
    /// the operator actually wants to see logs and outputs.
    pub fn discarding() -> Self {
        Self::new(
            Arc::new(DiscardingFrameSender),
            "test".to_string(),
            "*".to_string(),
            MessageId::new_uuid(),
            None,
            Limits::default().max_chunk,
        )
    }

    fn new(
        sender: Arc<dyn FrameSender>,
        stream_id: String,
        media_urn: String,
        request_id: MessageId,
        routing_id: Option<MessageId>,
        max_chunk: usize,
    ) -> Self {
        Self {
            sender,
            stream_id,
            media_urn,
            request_id,
            routing_id,
            max_chunk,
            stream_mode: Mutex::new(None),
            chunk_index: Arc::new(Mutex::new(0)),
            chunk_count: Arc::new(Mutex::new(0)),
            closed: AtomicBool::new(false),
            final_status: Arc::new(Mutex::new(None)),
            unbounded: AtomicBool::new(false),
            credit_gate: None,
            credit_router: None,
            coalesce: Arc::new(Mutex::new(CoalesceBuf::default())),
        }
    }

    /// Attach a flow-control window to this stream (L9). The gate registers
    /// with `router` on `start()` so inbound CREDIT frames replenish it; the
    /// runtime closes it via the router on terminal/cancel (L13).
    pub(crate) fn with_credit(
        mut self,
        initial_credit: u64,
        router: crate::bifaci::credit::CreditRouter,
    ) -> Self {
        self.credit_gate = Some(Arc::new(crate::bifaci::credit::CreditGate::new(
            initial_credit,
        )));
        self.credit_router = Some(router);
        self
    }

    /// Acquire one chunk of credit, waiting if the window is exhausted.
    /// Uncredited streams return immediately. A closed gate (request
    /// terminated/cancelled) fails the write — the producer must stop (L13).
    async fn acquire_credit(&self) -> Result<(), RuntimeError> {
        if let Some(gate) = &self.credit_gate {
            gate.acquire(1)
                .await
                .map_err(|e| RuntimeError::Handler(e.to_string()))?;
        }
        Ok(())
    }

    /// Blocking-context counterpart of `acquire_credit` (FFI threads,
    /// spawn_blocking closures).
    fn blocking_acquire_credit(&self) -> Result<(), RuntimeError> {
        if let Some(gate) = &self.credit_gate {
            gate.blocking_acquire(1)
                .map_err(|e| RuntimeError::Handler(e.to_string()))?;
        }
        Ok(())
    }

    /// Declare the request's terminal status (final progress + message),
    /// delivered in the END frame's terminal metadata when the handler
    /// completes successfully (L3/L5). Optional — without a call, a
    /// successful END carries progress 1.0. The last call before the handler
    /// returns wins. Do NOT emit a trailing 100% progress LOG frame; the END
    /// terminal metadata IS the final progress event and cannot race END.
    pub fn finish(&self, progress: f32, message: &str) {
        let mut fs = self.final_status.lock().unwrap_or_else(|e| e.into_inner());
        *fs = Some(FinalStatus {
            progress: progress as f64,
            message: if message.is_empty() {
                None
            } else {
                Some(message.to_string())
            },
        });
    }

    /// Shared handle to the handler-declared terminal status. The runtime
    /// reads it after the handler returns to stamp the END frame.
    pub(crate) fn final_status_handle(&self) -> Arc<Mutex<Option<FinalStatus>>> {
        Arc::clone(&self.final_status)
    }

    fn check_mode(&self, is_sequence: bool) -> Result<(), RuntimeError> {
        let mode = self.stream_mode.lock().unwrap();
        match *mode {
            None => Err(RuntimeError::Handler(
                "stream not started: call start() before write/emit_list_item".to_string(),
            )),
            Some(existing) if existing == is_sequence => Ok(()),
            Some(existing) => Err(RuntimeError::Handler(format!(
                "stream mode conflict: started as {} but called with {}",
                if existing { "sequence" } else { "write" },
                if is_sequence { "sequence" } else { "write" },
            ))),
        }
    }

    fn send_chunk(&self, value: &ciborium::Value) -> Result<(), RuntimeError> {
        let mut cbor_payload = Vec::new();
        ciborium::into_writer(value, &mut cbor_payload)
            .map_err(|e| RuntimeError::Handler(format!("Failed to encode CBOR: {}", e)))?;

        let chunk_index = {
            let mut chunk_index_guard = self.chunk_index.lock().unwrap();
            let current = *chunk_index_guard;
            *chunk_index_guard += 1;
            current
        };
        {
            let mut count_guard = self.chunk_count.lock().unwrap();
            *count_guard += 1;
        }

        let checksum = Frame::compute_checksum(&cbor_payload);
        let mut frame = Frame::chunk(
            self.request_id.clone(),
            self.stream_id.clone(),
            0,
            cbor_payload,
            chunk_index,
            checksum,
        );
        frame.routing_id = self.routing_id.clone();
        self.sender.send(&frame)
    }

    /// Write raw bytes. Splits into max_chunk pieces, each wrapped as CBOR Bytes.
    /// Requires `start(false)` to have been called first.
    ///
    /// Awaits per chunk when the flow-control window is exhausted (L9); the
    /// receiver's consumption replenishes it. Use `blocking_write` from
    /// non-async contexts.
    pub async fn write(&self, data: &[u8]) -> Result<(), RuntimeError> {
        self.check_mode(false)?;
        if data.is_empty() {
            return Ok(());
        }
        // Coalesce: small writes accumulate and ship as one CHUNK once the
        // size or age threshold is crossed (see [`CoalesceBuf`]); `close()`
        // flushes the tail. Chunk boundaries on a scalar stream are
        // non-semantic, so this is invisible to every consumer.
        match coalesce_append(&self.coalesce, data) {
            Some(batch) => self.write_batch(&batch).await,
            None => Ok(()),
        }
    }

    /// Ship one coalesced batch: split at `max_chunk`, one credit per CHUNK.
    async fn write_batch(&self, data: &[u8]) -> Result<(), RuntimeError> {
        let mut offset = 0;
        while offset < data.len() {
            let chunk_size = (data.len() - offset).min(self.max_chunk);
            let chunk_bytes = data[offset..offset + chunk_size].to_vec();
            self.acquire_credit().await?;
            self.send_chunk(&ciborium::Value::Bytes(chunk_bytes))?;
            offset += chunk_size;
        }
        Ok(())
    }

    /// Flush any coalesced-but-unsent bytes to the wire now. `close()` calls
    /// this; handlers only need it for explicit mid-stream latency barriers.
    pub async fn flush(&self) -> Result<(), RuntimeError> {
        match coalesce_take(&self.coalesce) {
            Some(batch) => self.write_batch(&batch).await,
            None => Ok(()),
        }
    }

    /// Blocking-context counterpart of [`flush`](Self::flush).
    pub fn blocking_flush(&self) -> Result<(), RuntimeError> {
        match coalesce_take(&self.coalesce) {
            Some(batch) => self.blocking_write_batch(&batch),
            None => Ok(()),
        }
    }

    /// Blocking-context counterpart of [`write`](Self::write) — for FFI
    /// threads and `spawn_blocking` closures. Identical framing; the credit
    /// wait blocks the calling thread instead of yielding.
    pub fn blocking_write(&self, data: &[u8]) -> Result<(), RuntimeError> {
        self.check_mode(false)?;
        if data.is_empty() {
            return Ok(());
        }
        match coalesce_append(&self.coalesce, data) {
            Some(batch) => self.blocking_write_batch(&batch),
            None => Ok(()),
        }
    }

    /// Blocking-context counterpart of [`write_batch`](Self::write_batch).
    fn blocking_write_batch(&self, data: &[u8]) -> Result<(), RuntimeError> {
        let mut offset = 0;
        while offset < data.len() {
            let chunk_size = (data.len() - offset).min(self.max_chunk);
            let chunk_bytes = data[offset..offset + chunk_size].to_vec();
            self.blocking_acquire_credit()?;
            self.send_chunk(&ciborium::Value::Bytes(chunk_bytes))?;
            offset += chunk_size;
        }
        Ok(())
    }

    /// Emit a single CBOR value as one item in an RFC 8742 CBOR sequence.
    ///
    /// For list outputs: the receiver concatenates raw frame payloads and stores
    /// the result as a CBOR sequence. This method CBOR-encodes the value, then
    /// splits the encoded bytes across chunk frames at `max_chunk` boundaries.
    /// The receiver's concatenation reconstructs the original CBOR encoding,
    /// producing exactly one self-delimiting CBOR value in the sequence per call.
    ///
    /// Unlike `emit_cbor` (which re-wraps each piece as a separate CBOR value),
    /// this sends raw CBOR bytes as frame payloads directly.
    ///
    /// `meta` is per-item metadata, placed on the first chunk frame of this item only.
    ///
    /// Awaits per chunk when the flow-control window is exhausted (L9). Use
    /// `blocking_emit_list_item` from non-async contexts.
    pub async fn emit_list_item(
        &self,
        value: &ciborium::Value,
        meta: Option<StreamMeta>,
    ) -> Result<(), RuntimeError> {
        self.check_mode(true)?;
        let cbor_bytes = Self::encode_item(value)?;
        let mut offset = 0;
        let mut first_chunk = true;
        while offset < cbor_bytes.len() {
            let chunk_size = (cbor_bytes.len() - offset).min(self.max_chunk);
            self.acquire_credit().await?;
            self.send_item_chunk(
                &cbor_bytes[offset..offset + chunk_size],
                if first_chunk { meta.clone() } else { None },
            )?;
            first_chunk = false;
            offset += chunk_size;
        }
        Ok(())
    }

    /// Blocking-context counterpart of [`emit_list_item`](Self::emit_list_item).
    pub fn blocking_emit_list_item(
        &self,
        value: &ciborium::Value,
        meta: Option<StreamMeta>,
    ) -> Result<(), RuntimeError> {
        self.check_mode(true)?;
        let cbor_bytes = Self::encode_item(value)?;
        let mut offset = 0;
        let mut first_chunk = true;
        while offset < cbor_bytes.len() {
            let chunk_size = (cbor_bytes.len() - offset).min(self.max_chunk);
            self.blocking_acquire_credit()?;
            self.send_item_chunk(
                &cbor_bytes[offset..offset + chunk_size],
                if first_chunk { meta.clone() } else { None },
            )?;
            first_chunk = false;
            offset += chunk_size;
        }
        Ok(())
    }

    /// CBOR-encode one sequence item.
    fn encode_item(value: &ciborium::Value) -> Result<Vec<u8>, RuntimeError> {
        let mut cbor_bytes = Vec::new();
        ciborium::into_writer(value, &mut cbor_bytes)
            .map_err(|e| RuntimeError::Handler(format!("Failed to encode CBOR: {}", e)))?;
        Ok(cbor_bytes)
    }

    /// Send one raw sequence-item chunk (payload = raw CBOR fragment bytes).
    fn send_item_chunk(
        &self,
        chunk_payload: &[u8],
        meta: Option<StreamMeta>,
    ) -> Result<(), RuntimeError> {
        let chunk_index = {
            let mut guard = self.chunk_index.lock().unwrap();
            let current = *guard;
            *guard += 1;
            current
        };
        {
            let mut guard = self.chunk_count.lock().unwrap();
            *guard += 1;
        }

        let checksum = Frame::compute_checksum(chunk_payload);
        let mut frame = Frame::chunk(
            self.request_id.clone(),
            self.stream_id.clone(),
            0,
            chunk_payload.to_vec(),
            chunk_index,
            checksum,
        );
        frame.routing_id = self.routing_id.clone();
        // Per-item meta goes on the first chunk frame only
        frame.meta = meta;
        self.sender.send(&frame)
    }

    /// Emit a CBOR value. Handles Bytes/Text/Array/Map chunking.
    /// Uses write mode (is_sequence=false) — each chunk is a complete CBOR value.
    /// Requires `start(false)` to have been called first.
    ///
    /// Awaits per chunk when the flow-control window is exhausted (L9).
    pub async fn emit_cbor(&self, value: &ciborium::Value) -> Result<(), RuntimeError> {
        self.check_mode(false)?;
        match value {
            ciborium::Value::Bytes(bytes) => {
                // Byte emissions coalesce exactly like `write` — on a scalar
                // stream a Bytes chunk is pure byte-stream continuation.
                if bytes.is_empty() {
                    return Ok(());
                }
                if let Some(batch) = coalesce_append(&self.coalesce, bytes) {
                    self.write_batch(&batch).await?;
                }
            }
            ciborium::Value::Text(text) => {
                // ORDERING BARRIER: a non-Bytes value must not overtake bytes
                // still sitting in the coalescing buffer.
                self.flush().await?;
                let text_bytes = text.as_bytes();
                let mut offset = 0;
                while offset < text_bytes.len() {
                    let mut chunk_size = (text_bytes.len() - offset).min(self.max_chunk);
                    while chunk_size > 0 && !text.is_char_boundary(offset + chunk_size) {
                        chunk_size -= 1;
                    }
                    if chunk_size == 0 {
                        return Err(RuntimeError::Handler(
                            "Cannot split text on character boundary".to_string(),
                        ));
                    }
                    let chunk_text = text[offset..offset + chunk_size].to_string();
                    self.acquire_credit().await?;
                    self.send_chunk(&ciborium::Value::Text(chunk_text))?;
                    offset += chunk_size;
                }
            }
            ciborium::Value::Array(elements) => {
                for element in elements {
                    self.acquire_credit().await?;
                    self.send_chunk(element)?;
                }
            }
            ciborium::Value::Map(entries) => {
                self.flush().await?;
                for (key, val) in entries {
                    let entry = ciborium::Value::Array(vec![key.clone(), val.clone()]);
                    self.acquire_credit().await?;
                    self.send_chunk(&entry)?;
                }
            }
            _ => {
                self.flush().await?;
                self.acquire_credit().await?;
                self.send_chunk(value)?;
            }
        }
        Ok(())
    }

    /// Emit a log message.
    pub fn log(
        &self,
        level: &str,
        attribution_class: crate::failure::AttributionClass,
        message: &str,
    ) {
        let mut frame = Frame::log(
            self.request_id.clone(),
            level,
            attribution_class,
            message,
            None,
        );
        frame.routing_id = self.routing_id.clone();
        let _ = self.sender.send(&frame);
    }

    /// Emit a log message attributed by the source to one argument media URN.
    pub fn log_for_argument(
        &self,
        level: &str,
        attribution_class: crate::failure::AttributionClass,
        message: &str,
        arg_urn: &str,
    ) {
        let mut frame = Frame::log(
            self.request_id.clone(),
            level,
            attribution_class,
            message,
            Some(arg_urn),
        );
        frame.routing_id = self.routing_id.clone();
        let _ = self.sender.send(&frame);
    }

    /// Emit a progress update (0.0–1.0) with a human-readable status message.
    pub fn progress(&self, progress: f32, message: &str) {
        let mut frame = Frame::progress(self.request_id.clone(), progress, message);
        frame.routing_id = self.routing_id.clone();
        let _ = self.sender.send(&frame);
    }

    /// Create a detached progress sender that can be moved into `spawn_blocking`.
    ///
    /// The returned `ProgressSender` is `Send + Sync + 'static` and can emit
    /// progress and log frames from any thread without holding a reference to
    /// this `OutputStream`. Use this when blocking work (FFI model loads, inference)
    /// needs to emit per-token or keepalive progress from a dedicated thread.
    pub fn progress_sender(&self) -> ProgressSender {
        ProgressSender {
            sender: Arc::clone(&self.sender),
            request_id: self.request_id.clone(),
            routing_id: self.routing_id.clone(),
        }
    }

    /// Create a detached stream sender that can emit CBOR data chunks from any
    /// thread (including `spawn_blocking`).
    ///
    /// Shares chunk counters with this `OutputStream` so that `close()` reports
    /// the correct total chunk count.
    ///
    /// **Call `start()` before creating the `StreamSender`** so that
    /// STREAM_START is sent while the async context is still active.
    pub fn stream_sender(&self) -> StreamSender {
        StreamSender {
            sender: Arc::clone(&self.sender),
            request_id: self.request_id.clone(),
            routing_id: self.routing_id.clone(),
            stream_id: self.stream_id.clone(),
            max_chunk: self.max_chunk,
            chunk_index: Arc::clone(&self.chunk_index),
            chunk_count: Arc::clone(&self.chunk_count),
            credit_gate: self.credit_gate.clone(),
            coalesce: Arc::clone(&self.coalesce),
        }
    }

    /// Send STREAM_START with the given mode. Must be called exactly once
    /// before any write/emit_list_item/emit_cbor calls.
    ///
    /// * `is_sequence = false` — write mode: each chunk is a complete CBOR value.
    ///   `meta` is placed on the STREAM_START frame (whole-stream metadata).
    /// * `is_sequence = true`  — sequence mode: chunks are CBOR fragments (RFC 8742).
    ///   `meta` is placed on the STREAM_START frame. Per-item metadata goes via `emit_list_item`.
    /// Send STREAM_START for an UNBOUNDED stream — one that makes no length
    /// promise (L16). The receiver must consume it incrementally; buffering
    /// collectors refuse it. `close()` on an unbounded stream sends
    /// STREAM_END without a chunk_count. Otherwise identical to `start()`.
    pub fn start_unbounded(
        &self,
        is_sequence: bool,
        meta: Option<StreamMeta>,
    ) -> Result<(), RuntimeError> {
        {
            let mut mode = self.stream_mode.lock().unwrap();
            if mode.is_some() {
                return Err(RuntimeError::Handler("stream already started".to_string()));
            }
            *mode = Some(is_sequence);
        }
        self.unbounded.store(true, Ordering::SeqCst);
        if let (Some(gate), Some(router)) = (&self.credit_gate, &self.credit_router) {
            router.register(
                self.request_id.clone(),
                Some(self.stream_id.clone()),
                Arc::clone(gate),
            );
        }
        let mut start_frame = Frame::stream_start_unbounded(
            self.request_id.clone(),
            self.stream_id.clone(),
            self.media_urn.clone(),
            Some(is_sequence),
        );
        start_frame.routing_id = self.routing_id.clone();
        start_frame.meta = meta;
        self.sender.send(&start_frame)
    }

    pub fn start(&self, is_sequence: bool, meta: Option<StreamMeta>) -> Result<(), RuntimeError> {
        let mut mode = self.stream_mode.lock().unwrap();
        if mode.is_some() {
            return Err(RuntimeError::Handler("stream already started".to_string()));
        }
        *mode = Some(is_sequence);
        drop(mode);
        // Register this stream's credit gate so inbound CREDIT frames find it.
        if let (Some(gate), Some(router)) = (&self.credit_gate, &self.credit_router) {
            router.register(
                self.request_id.clone(),
                Some(self.stream_id.clone()),
                Arc::clone(gate),
            );
        }
        let mut start_frame = Frame::stream_start(
            self.request_id.clone(),
            self.stream_id.clone(),
            self.media_urn.clone(),
            Some(is_sequence),
        );
        start_frame.routing_id = self.routing_id.clone();
        start_frame.meta = meta;
        self.sender.send(&start_frame)
    }

    /// Run a blocking closure on a dedicated OS thread while emitting keepalive
    /// progress frames every 30 seconds from a separate ticker thread.
    ///
    /// Model loading (GGUF, Candle, Metal, etc.) is synchronous FFI that can take
    /// minutes for large models. The engine's 120s activity timeout kills the task
    /// if no frames arrive.
    ///
    /// Uses `std::thread::spawn` (not `tokio::task::spawn_blocking`) so that heavy
    /// FFI — particularly Metal/GCD on macOS which can consume all threads in
    /// tokio's blocking pool — cannot starve the async runtime or the keepalive
    /// ticker. The ticker also runs on a plain OS thread so it is immune to tokio
    /// scheduler pressure.
    pub async fn run_with_keepalive<T: Send + 'static>(
        &self,
        progress: f32,
        message: &str,
        f: impl FnOnce() -> T + Send + 'static,
    ) -> T {
        let sender = Arc::clone(&self.sender);
        let request_id = self.request_id.clone();
        let routing_id = self.routing_id.clone();
        let msg = message.to_string();

        // Channel: work thread signals completion to the ticker thread so it stops.
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();

        // Spawn keepalive ticker on a plain OS thread — immune to tokio pool pressure.
        let ticker_sender = Arc::clone(&sender);
        let ticker_rid = request_id.clone();
        let ticker_xid = routing_id.clone();
        let ticker_msg = msg.clone();
        // Diagnostic hooks — keepalive ticker observability emitted
        // as Log frames (not tracing). Tracing inside the cartridge
        // process either goes to stderr (drained, not surfaced) or
        // to a subscriber the cartridge installs at startup; neither
        // reaches the engine reliably. Log frames travel the same
        // wire path as the keepalive itself, so they're guaranteed
        // visible end-to-end. When a long-running blocking handler
        // (e.g. GGUF model load) hits the engine's 120s activity
        // timeout despite this mechanism being in place, the cause
        // is one of:
        //   1. The work thread panicked / crashed before the ticker
        //      could fire (we'll see a [keepalive] panic Log frame).
        //   2. The ticker is firing but the frame writer wedged
        //      (we see ticker-start, then no further ticks).
        //   3. The OS thread is starved (no [keepalive] frames at
        //      all — diagnose by absence).
        let tick_counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let tick_counter_for_ticker = tick_counter.clone();

        // Helper: build an attributed diagnostic Log frame stamped with the
        // request's rid + routing_id. Keepalive lifecycle diagnostics describe
        // runtime internals; progress ticks remain the separate, unattributed
        // functional progress channel below.
        fn keepalive_log_frame(
            rid: &MessageId,
            xid: &Option<MessageId>,
            level: &str,
            message: &str,
        ) -> Frame {
            let mut frame = Frame::log(
                rid.clone(),
                level,
                crate::AttributionClass::Internal,
                message,
                None,
            );
            frame.routing_id = xid.clone();
            frame
        }

        // Emit a one-shot "ticker started" Log frame so absence of
        // this line in the log means the ticker thread itself never
        // ran (OS thread exhaustion, panic on spawn).
        {
            let started = keepalive_log_frame(
                &ticker_rid,
                &ticker_xid,
                "debug",
                &format!("[keepalive] ticker started (interval=5s, msg={:?})", msg),
            );
            let _ = sender.send(&started);
        }

        std::thread::spawn(move || {
            loop {
                // 5s interval — short enough to survive OS thread suspension under
                // memory pressure (e.g. Metal loading large models) while still
                // resetting the engine's 120s activity timer with plenty of margin.
                match done_rx.recv_timeout(std::time::Duration::from_secs(5)) {
                    Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                        let n = tick_counter_for_ticker.load(std::sync::atomic::Ordering::Relaxed);
                        let stopped = keepalive_log_frame(
                            &ticker_rid,
                            &ticker_xid,
                            "debug",
                            &format!("[keepalive] ticker stopped after {} ticks", n),
                        );
                        let _ = ticker_sender.send(&stopped);
                        break;
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        let n = tick_counter_for_ticker
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                            + 1;
                        let mut frame = Frame::progress(ticker_rid.clone(), progress, &ticker_msg);
                        frame.routing_id = ticker_xid.clone();
                        if ticker_sender.send(&frame).is_err() {
                            // Sender closed — frame writer is gone.
                            // Can't even emit a Log frame to report
                            // it; the channel is dead. Just bail.
                            break;
                        }
                    }
                }
            }
        });

        // Run the blocking work on a dedicated OS thread. Catch
        // panics so the ticker gets a clean shutdown signal even on
        // FFI explosion (Metal/CUDA/etc. can panic from native code)
        // and the panic payload reaches the engine as a Log frame.
        let panic_sender = Arc::clone(&sender);
        let panic_rid = request_id.clone();
        let panic_xid = routing_id.clone();
        let (result_tx, result_rx) = std::sync::mpsc::channel::<T>();
        std::thread::spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
            match result {
                Ok(v) => {
                    let _ = result_tx.send(v);
                }
                Err(payload) => {
                    let payload_str = if let Some(s) = payload.downcast_ref::<&'static str>() {
                        (*s).to_string()
                    } else if let Some(s) = payload.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "<non-string panic payload>".to_string()
                    };
                    let panic_frame = keepalive_log_frame(
                        &panic_rid,
                        &panic_xid,
                        "error",
                        &format!("[keepalive] work thread panicked: {}", payload_str),
                    );
                    let _ = panic_sender.send(&panic_frame);
                    // Drop result_tx → result_rx.recv() returns Err → spawn_blocking awaits an Err
                }
            }
            // Dropping done_tx signals the ticker to stop.
            drop(done_tx);
        });

        // Await result without blocking the async runtime.
        // `result_rx.recv()` returns Err when the work thread
        // panicked (sender dropped without sending) — re-panic with
        // a clear message so callers see it as a handler error
        // rather than a silent hang. The panic-catch wrapper above
        // already logged the original payload.
        tokio::task::spawn_blocking(move || {
            result_rx
                .recv()
                .unwrap_or_else(|_| panic!("run_with_keepalive: work thread panicked (see [keepalive] log line above for payload)"))
        })
        .await
        .expect("spawn_blocking join failed")
    }

    /// Close the output stream (sends STREAM_END). Idempotent.
    /// If `start()` was never called, this is a no-op (no STREAM_START was sent,
    /// so no STREAM_END is needed — the handler produced no output).
    pub async fn close(&self) -> Result<(), RuntimeError> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(()); // Already closed
        }
        {
            let mode = self.stream_mode.lock().unwrap();
            if mode.is_none() {
                return Ok(()); // Never started — no output produced, nothing to close
            }
        }
        // Coalesced tail bytes ship BEFORE the STREAM_END that promises the
        // chunk count — flushing here is what makes coalescing lossless.
        self.flush().await?;
        self.send_stream_end()
    }

    /// Blocking-context counterpart of [`close`](Self::close) — FFI threads
    /// and `spawn_blocking` closures. The credit wait for the flushed tail
    /// blocks the calling thread instead of yielding.
    pub fn blocking_close(&self) -> Result<(), RuntimeError> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(()); // Already closed
        }
        {
            let mode = self.stream_mode.lock().unwrap();
            if mode.is_none() {
                return Ok(());
            }
        }
        self.blocking_flush()?;
        self.send_stream_end()
    }

    /// Build and send this stream's STREAM_END (chunk_count only for bounded
    /// streams, L16). Shared tail of both close variants.
    fn send_stream_end(&self) -> Result<(), RuntimeError> {
        let mut frame = if self.unbounded.load(Ordering::SeqCst) {
            // Unbounded streams made no length promise — their STREAM_END
            // carries no chunk_count (L16).
            Frame::stream_end_unbounded(self.request_id.clone(), self.stream_id.clone())
        } else {
            let chunk_count = {
                let count_guard = self.chunk_count.lock().unwrap();
                *count_guard
            };
            Frame::stream_end(self.request_id.clone(), self.stream_id.clone(), chunk_count)
        };
        frame.routing_id = self.routing_id.clone();
        self.sender.send(&frame)
    }
}

/// Handle for an in-progress peer invocation.
/// Handler creates arg streams with `arg()`, writes data, then calls `finish()`
/// to get a `PeerResponse` that yields both data and LOG frames.
pub struct PeerCall {
    pub(crate) sender: Arc<dyn FrameSender>,
    pub(crate) request_id: MessageId,
    pub(crate) max_chunk: usize,
    pub(crate) response_rx: Option<tokio::sync::mpsc::UnboundedReceiver<Frame>>,
    pub(crate) credit_router: Option<crate::bifaci::credit::CreditRouter>,
    pub(crate) initial_credit: u64,
}

impl PeerCall {
    /// Create a new arg OutputStream for this peer call.
    /// Each arg is an independent stream (own stream_id, no routing_id),
    /// flow-controlled by the callee's consumption (L14).
    pub fn arg(&self, media_urn: &str) -> OutputStream {
        let stream_id = uuid::Uuid::new_v4().to_string();
        let output = OutputStream::new(
            Arc::clone(&self.sender),
            stream_id,
            media_urn.to_string(),
            self.request_id.clone(),
            None, // No routing_id for peer requests
            self.max_chunk,
        );
        match &self.credit_router {
            Some(router) => output.with_credit(self.initial_credit, router.clone()),
            None => output,
        }
    }

    /// Finish sending args and get the peer response.
    /// Sends END for the peer request, spawns Demux on response channel.
    ///
    /// Returns a `PeerResponse` that yields `PeerResponseItem::Data` and
    /// `PeerResponseItem::Log` interleaved in arrival order. The handler
    /// decides how to react to each (e.g., forward progress, accumulate data).
    pub async fn finish(mut self) -> Result<PeerResponse, RuntimeError> {
        // Send END frame for the peer request
        let end_frame = Frame::end(self.request_id.clone(), None);
        self.sender.send(&end_frame)?;

        // Take the response receiver
        let response_rx = self
            .response_rx
            .take()
            .ok_or_else(|| RuntimeError::PeerRequest("PeerCall already finished".to_string()))?;

        // Start demux — returns immediately so LOG frames can be consumed
        // before data arrives (critical for keeping activity timer alive).
        // Consumption grants keep the responding peer's output window
        // replenished (L10/L14); single-stream response → stream-less grants.
        let grants = self.credit_router.as_ref().map(|_| InputGrantEmitter {
            sender: Arc::clone(&self.sender),
            rid: self.request_id.clone(),
            xid: None,
            stream_id: None,
            // Peer-response consumption credits the CALLEE's output streams —
            // response direction, routed toward the handler (L11).
            direction: crate::bifaci::frame::CreditDirection::Response,
            batch: (self.initial_credit / 2).max(1),
            consumed_since_grant: 0,
            window: Arc::new(std::sync::atomic::AtomicI64::new(0)),
        });
        let peer_response = demux_single_stream(response_rx, grants);

        Ok(peer_response)
    }
}

/// Allows handlers to invoke caps on the peer (host).
///
/// This trait enables bidirectional communication where a cartridge handler can
/// invoke caps on the host while processing a request.
///
/// The `call` method starts a peer invocation and returns a `PeerCall`.
/// The handler creates arg streams with `call.arg()`, writes data, then
/// calls `call.finish()` to get a `PeerResponse` with data + LOG frames.
#[async_trait]
pub trait PeerInvoker: Send + Sync {
    /// Start a peer call. Sends REQ, registers response channel.
    fn call(&self, cap_urn: &str) -> Result<PeerCall, RuntimeError>;

    /// Convenience: open call, write each arg's bytes, finish, return response.
    ///
    /// Returns a `PeerResponse`. Use `recv()` or a forwarding collector when
    /// the peer may emit diagnostics; plain collectors fail on LOG frames.
    async fn call_with_bytes(
        &self,
        cap_urn: &str,
        args: &[(&str, &[u8])],
    ) -> Result<PeerResponse, RuntimeError> {
        self.call_with_bytes_and_meta(cap_urn, args, None).await
    }

    /// Like `call_with_bytes`, but sets stream metadata on each arg's STREAM_START.
    ///
    /// The meta carries provenance context (e.g. {"title": "page_3"}) through
    /// peer calls so the receiving cap can propagate it to its output.
    async fn call_with_bytes_and_meta(
        &self,
        cap_urn: &str,
        args: &[(&str, &[u8])],
        meta: Option<&crate::StreamMeta>,
    ) -> Result<PeerResponse, RuntimeError> {
        let call = self.call(cap_urn)?;
        for &(media_urn, data) in args {
            let arg = call.arg(media_urn);
            arg.start(false, meta.cloned())?;
            arg.write(data).await?;
            arg.close().await?;
        }
        call.finish().await
    }
}

/// A no-op PeerInvoker that always returns an error.
/// Used when peer invocation is not supported (e.g., CLI mode).
pub struct NoPeerInvoker;

#[async_trait]
impl PeerInvoker for NoPeerInvoker {
    fn call(&self, _cap_urn: &str) -> Result<PeerCall, RuntimeError> {
        Err(RuntimeError::PeerRequest(
            "Peer invocation not supported in this context".to_string(),
        ))
    }
}

/// Channel-based frame sender for cartridge output.
/// ALL frames (peer requests AND responses) go through a single output channel.
/// CartridgeRuntime has a writer task that drains this channel and writes to stdout.
pub(crate) struct ChannelFrameSender {
    pub(crate) tx: tokio::sync::mpsc::UnboundedSender<Frame>,
    /// Dropped-frame accounting: a send on a closed channel is a counted
    /// channel_closed drop (L8), never a silent loss — even when the caller
    /// treats the send as infallible (log/progress emitters).
    pub(crate) drops: Arc<crate::bifaci::stats::DropCounters>,
}

impl FrameSender for ChannelFrameSender {
    fn send(&self, frame: &Frame) -> Result<(), RuntimeError> {
        // UnboundedSender::send is sync-compatible (no .await needed)
        self.tx.send(frame.clone()).map_err(|_| {
            let total = self
                .drops
                .record(crate::bifaci::frame::DropReason::ChannelClosed, frame.frame_type);
            tracing::warn!(
                target: "cartridge_runtime",
                rid = ?frame.id,
                ftype = ?frame.frame_type,
                channel_closed_total = total,
                "[CartridgeRuntime] frame dropped: output channel closed"
            );
            RuntimeError::Handler("Output channel closed".to_string())
        })
    }
}

/// CLI-mode emitter that writes directly to stdout.
/// Used when the cartridge is invoked via CLI (with arguments).
pub struct CliStreamEmitter {
    /// Whether to add newlines after each emit (NDJSON style)
    ndjson: bool,
}

impl CliStreamEmitter {
    /// Create a new CLI emitter with NDJSON formatting (newline after each emit)
    pub fn new() -> Self {
        Self { ndjson: true }
    }

    /// Create a CLI emitter without NDJSON formatting
    pub fn without_ndjson() -> Self {
        Self { ndjson: false }
    }
}

impl Default for CliStreamEmitter {
    fn default() -> Self {
        Self::new()
    }
}

impl CliStreamEmitter {
    /// Emit a CBOR value to stdout (CLI mode)
    pub fn emit_cbor(&self, value: &ciborium::Value) -> Result<(), RuntimeError> {
        let stdout = io::stdout();
        let mut handle = stdout.lock();

        // In CLI mode: extract raw bytes/text from CBOR and emit to stdout
        // Supported types: Bytes, Text, Array (of Bytes/Text), Map (extract "value" field)
        // NO FALLBACK - fail hard if unsupported type

        match value {
            ciborium::Value::Array(arr) => {
                // Array - emit each element's raw content
                for item in arr {
                    match item {
                        ciborium::Value::Bytes(bytes) => {
                            let _ = handle.write_all(bytes);
                        }
                        ciborium::Value::Text(text) => {
                            let _ = handle.write_all(text.as_bytes());
                        }
                        ciborium::Value::Map(map) => {
                            // Map - extract "value" field (for argument structures)
                            if let Some(val) = map
                                .iter()
                                .find(
                                    |(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"),
                                )
                                .map(|(_, v)| v)
                            {
                                match val {
                                    ciborium::Value::Bytes(bytes) => {
                                        let _ = handle.write_all(bytes);
                                    }
                                    ciborium::Value::Text(text) => {
                                        let _ = handle.write_all(text.as_bytes());
                                    }
                                    _ => {
                                        return Err(RuntimeError::Handler(
                                            "Map 'value' field is not bytes/text".to_string(),
                                        ))
                                    }
                                }
                            } else {
                                return Err(RuntimeError::Handler(
                                    "Map in array has no 'value' field".to_string(),
                                ));
                            }
                        }
                        _ => {
                            return Err(RuntimeError::Handler(
                                "Array contains unsupported element type".to_string(),
                            ));
                        }
                    }
                }
            }
            ciborium::Value::Bytes(bytes) => {
                // Simple bytes - emit raw
                let _ = handle.write_all(bytes);
            }
            ciborium::Value::Text(text) => {
                // Simple text - emit as UTF-8
                let _ = handle.write_all(text.as_bytes());
            }
            ciborium::Value::Map(map) => {
                // Single map - extract "value" field
                if let Some(val) = map
                    .iter()
                    .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
                    .map(|(_, v)| v)
                {
                    match val {
                        ciborium::Value::Bytes(bytes) => {
                            let _ = handle.write_all(bytes);
                        }
                        ciborium::Value::Text(text) => {
                            let _ = handle.write_all(text.as_bytes());
                        }
                        _ => {
                            return Err(RuntimeError::Handler(
                                "Map 'value' field is not bytes/text".to_string(),
                            ))
                        }
                    }
                } else {
                    return Err(RuntimeError::Handler(
                        "Map has no 'value' field".to_string(),
                    ));
                }
            }
            _ => {
                return Err(RuntimeError::Handler(
                    "Handler emitted unsupported CBOR type".to_string(),
                ));
            }
        }

        if self.ndjson {
            let _ = handle.write_all(b"\n");
        }
        let _ = handle.flush();
        Ok(())
    }

    fn emit_log(&self, level: &str, message: &str) {
        // In CLI mode, logs go to stderr
        let stderr = io::stderr();
        let mut handle = stderr.lock();
        let _ = writeln!(handle, "[{}] {}", level, message);
    }
}

/// CLI-mode frame sender that extracts payloads from frames and outputs to stdout.
/// Adapts FrameSender trait for CLI mode using CliStreamEmitter.
pub struct CliFrameSender {
    emitter: CliStreamEmitter,
}

impl CliFrameSender {
    pub fn new() -> Self {
        Self {
            emitter: CliStreamEmitter::new(),
        }
    }

    pub fn with_emitter(emitter: CliStreamEmitter) -> Self {
        Self { emitter }
    }
}

impl FrameSender for CliFrameSender {
    fn send(&self, frame: &Frame) -> Result<(), RuntimeError> {
        match frame.frame_type {
            FrameType::Chunk => {
                // Extract CBOR payload from CHUNK frame and emit to stdout
                if let Some(ref payload) = frame.payload {
                    // Verify checksum (protocol v2 integrity check)
                    let expected_checksum = Frame::compute_checksum(payload);
                    let actual_checksum = frame.checksum.ok_or_else(|| {
                        RuntimeError::Protocol("CHUNK frame missing checksum field".to_string())
                    })?;
                    if expected_checksum != actual_checksum {
                        return Err(RuntimeError::CorruptedData(format!(
                            "CHUNK checksum mismatch: expected {}, got {} (payload {} bytes)",
                            expected_checksum,
                            actual_checksum,
                            payload.len()
                        )));
                    }

                    // Decode CBOR payload
                    let value: ciborium::Value =
                        ciborium::from_reader(&payload[..]).map_err(|e| {
                            RuntimeError::Handler(format!("Failed to decode CBOR payload: {}", e))
                        })?;

                    // Emit to stdout via CliStreamEmitter
                    self.emitter.emit_cbor(&value)?;
                }
                Ok(())
            }
            FrameType::Log => {
                // Extract log message and emit to stderr
                let level = frame.log_level().ok_or_else(|| {
                    RuntimeError::Protocol("LOG frame missing required text level".to_string())
                })?;
                let message = frame.log_message().ok_or_else(|| {
                    RuntimeError::Protocol("LOG frame missing required text message".to_string())
                })?;
                if frame.log_progress().is_none() {
                    frame.attribution_class().map_err(RuntimeError::Protocol)?;
                    frame
                        .attribution_arg_urn()
                        .map_err(RuntimeError::Protocol)?;
                }
                self.emitter.emit_log(level, message);
                Ok(())
            }
            FrameType::StreamStart | FrameType::StreamEnd | FrameType::End => {
                // Ignore framing messages in CLI mode
                Ok(())
            }
            FrameType::Err => {
                let (code, class, message, arg_urn) =
                    remote_error_fields(&frame).map_err(RuntimeError::Protocol)?;
                // Keep the frame's declared code/class/message structural —
                // CLI mode still owes the caller the real failure identity.
                Err(RuntimeError::Classified {
                    code,
                    class,
                    message,
                    arg_urn,
                })
            }
            _ => {
                // Fail hard on unexpected frame types
                Err(RuntimeError::Handler(format!(
                    "Unexpected frame type in CLI mode: {:?}",
                    frame.frame_type
                )))
            }
        }
    }
}

// =============================================================================
// OP-BASED HANDLER SYSTEM — handlers implement ops_rs::Op<()>
// =============================================================================

/// Bundles capdag I/O for WetContext. Op handlers extract this from WetContext
/// to access streaming input, output, and peer invocation.
pub struct Request {
    input: Mutex<Option<InputPackage>>,
    output: Arc<OutputStream>,
    peer: Arc<dyn PeerInvoker>,
}

impl Request {
    /// Create a new Request bundling input, output, and peer invoker.
    pub fn new(input: InputPackage, output: OutputStream, peer: Arc<dyn PeerInvoker>) -> Self {
        Self {
            input: Mutex::new(Some(input)),
            output: Arc::new(output),
            peer,
        }
    }

    /// Take the input package. Can only be called once — second call returns error.
    pub fn take_input(&self) -> Result<InputPackage, RuntimeError> {
        self.input
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| RuntimeError::Handler("Input already consumed".to_string()))
    }

    /// Access the output stream.
    pub fn output(&self) -> &OutputStream {
        &self.output
    }

    /// Access the peer invoker.
    pub fn peer(&self) -> &dyn PeerInvoker {
        &*self.peer
    }
}

/// WetContext key for the Request object.
pub const WET_KEY_REQUEST: &str = "request";

/// Factory function that creates a fresh Op<()> instance per invocation.
pub type OpFactory = Arc<dyn Fn() -> Box<dyn Op<()>> + Send + Sync>;

/// Standard identity handler — pure passthrough. Forwards all input chunks to output.
#[derive(Default)]
pub struct IdentityOp;

#[async_trait]
impl Op<()> for IdentityOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req: Arc<Request> = wet
            .get_required(WET_KEY_REQUEST)
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        let mut input = req
            .take_input()
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        let mut started = false;
        while let Some(stream_result) = input.recv().await {
            let mut stream = stream_result
                .map_err(|e| OpError::ExecutionFailed(format!("Identity input error: {}", e)))?;
            // Start output with the first input stream's meta (propagates provenance context)
            if !started {
                req.output()
                    .start(false, stream.stream_meta().cloned())
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                started = true;
            }
            while let Some(chunk_result) = stream.recv_data().await {
                let chunk = chunk_result.map_err(|e| {
                    OpError::ExecutionFailed(format!("Identity chunk error: {}", e))
                })?;
                req.output()
                    .emit_cbor(&chunk)
                    .await
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            }
        }
        // If no input streams arrived, still need to start and close the output
        if !started {
            req.output()
                .start(false, None)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        }
        Ok(())
    }

    fn metadata(&self) -> OpMetadata {
        OpMetadata::builder("IdentityOp")
            .description("Pure passthrough — forwards all input to output")
            .build()
    }
}

/// Standard discard handler — terminal morphism. Drains all input, produces nothing.
#[derive(Default)]
pub struct DiscardOp;

#[async_trait]
impl Op<()> for DiscardOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req: Arc<Request> = wet
            .get_required(WET_KEY_REQUEST)
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        let mut input = req
            .take_input()
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        while let Some(stream_result) = input.recv().await {
            let mut stream = stream_result
                .map_err(|e| OpError::ExecutionFailed(format!("Discard input error: {}", e)))?;
            while let Some(chunk_result) = stream.recv_data().await {
                let _ = chunk_result
                    .map_err(|e| OpError::ExecutionFailed(format!("Discard chunk error: {}", e)))?;
            }
        }
        Ok(())
    }

    fn metadata(&self) -> OpMetadata {
        OpMetadata::builder("DiscardOp")
            .description("Terminal morphism — drains all input, produces nothing")
            .build()
    }
}

/// Default adapter selection handler — returns empty END (no match).
///
/// This is the standard default for cartridges that do not inspect file content.
/// Cartridges that provide content inspection override this by registering their
/// own handler for `CAP_ADAPTER_SELECTION`.
///
/// The empty END frame (exit code 0, no stream output) is the ONLY valid "no match"
/// response. The orchestrator treats any stream output that isn't valid
/// `{"media_urns": [...]}` as a runtime error.
#[derive(Default)]
pub struct AdapterSelectionOp;

#[async_trait]
impl Op<()> for AdapterSelectionOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req: Arc<Request> = wet
            .get_required(WET_KEY_REQUEST)
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        let mut input = req
            .take_input()
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        // Drain all input — we don't inspect it in the default handler
        while let Some(stream_result) = input.recv().await {
            let mut stream = stream_result.map_err(|e| {
                OpError::ExecutionFailed(format!("AdapterSelection input error: {}", e))
            })?;
            while let Some(chunk_result) = stream.recv_data().await {
                let _ = chunk_result.map_err(|e| {
                    OpError::ExecutionFailed(format!("AdapterSelection chunk error: {}", e))
                })?;
            }
        }
        // Return Ok(()) without starting output — produces empty END frame
        Ok(())
    }

    fn metadata(&self) -> OpMetadata {
        OpMetadata::builder("AdapterSelectionOp")
            .description("Default adapter selection — returns empty END (no match)")
            .build()
    }
}

/// Tracks a pending peer request (cartridge invoking host cap).
/// The reader loop forwards response frames to the channel.
/// LOG frames are re-stamped with the origin request ID and forwarded
/// back to the host automatically (no handler involvement).
struct PendingPeerRequest {
    sender: tokio::sync::mpsc::UnboundedSender<Frame>,
    origin_request_id: MessageId,
    origin_routing_id: Option<MessageId>,
}

/// Implementation of PeerInvoker that sends REQ frames to the host.
struct PeerInvokerImpl {
    output_tx: tokio::sync::mpsc::UnboundedSender<Frame>,
    pending_requests: Arc<Mutex<HashMap<MessageId, PendingPeerRequest>>>,
    max_chunk: usize,
    origin_request_id: MessageId,
    origin_routing_id: Option<MessageId>,
    drops: Arc<crate::bifaci::stats::DropCounters>,
    /// Router that delivers inbound CREDIT grants to this cartridge's
    /// outgoing peer-argument streams (L14 — peer args are credited too).
    credit_router: crate::bifaci::credit::CreditRouter,
    initial_credit: u64,
}

/// Extract the effective payload from a CBOR arguments payload.
///
/// Handles file-path auto-conversion for BOTH CLI and CBOR modes:
/// 1. Detects media:file-path arguments
/// 2. Reads file(s) from filesystem
/// 3. Replaces with file bytes and correct media_urn (from arg's stdin source)
/// 4. Validates at least one arg matches in_spec (unless void)
///
/// For non-CBOR content types, returns raw payload as-is.
///
/// `is_cli_mode`: true if CLI mode (args from command line), false if CBOR mode (cartridge protocol)
fn extract_effective_payload(
    payload: &[u8],
    content_type: Option<&str>,
    cap: &Cap,
    is_cli_mode: bool,
) -> Result<Vec<u8>, RuntimeError> {
    // Check if this is CBOR arguments
    if content_type != Some("application/cbor") {
        // Not CBOR arguments - return raw payload
        return Ok(payload.to_vec());
    }

    // Parse cap URN to get expected input media URN
    let cap_urn = CapUrn::from_string(&cap.urn_string())
        .map_err(|e| RuntimeError::CapUrn(format!("Invalid cap URN: {}", e)))?;
    let expected_input = cap_urn.in_spec().to_string();
    let expected_media_urn = MediaUrn::from_string(&expected_input).ok();

    // Build an arg-definition lookup: parsed MediaUrn → (stdin target URN,
    // is_sequence flag). File-path conversion consults this to decide whether
    // to emit a single file's bytes or a sequence of files, and what URN to
    // relabel the stream with so downstream handlers see the target media
    // type rather than the raw `media:file-path` input.
    struct ArgDefInfo {
        stdin_target: Option<String>,
        is_sequence: bool,
    }
    let arg_defs: Vec<(MediaUrn, ArgDefInfo)> = cap
        .get_args()
        .iter()
        .filter_map(|a| {
            let parsed = MediaUrn::from_string(&a.media_urn).ok()?;
            let stdin_target = a.sources.iter().find_map(|s| match s {
                ArgSource::Stdin { stdin } => Some(stdin.clone()),
                _ => None,
            });
            Some((
                parsed,
                ArgDefInfo {
                    stdin_target,
                    is_sequence: a.is_sequence,
                },
            ))
        })
        .collect();

    // Parse the CBOR payload as an array of argument maps
    let cbor_value: ciborium::Value = ciborium::from_reader(payload)
        .map_err(|e| RuntimeError::Deserialize(format!("Failed to parse CBOR arguments: {}", e)))?;

    let mut arguments = match cbor_value {
        ciborium::Value::Array(arr) => arr,
        _ => {
            return Err(RuntimeError::Deserialize(
                "CBOR arguments must be an array".to_string(),
            ));
        }
    };

    // File-path auto-conversion.
    //
    // When an arg's media URN is a specialization of `media:file-path`, the
    // incoming value is treated as one or more filesystem paths (literal or
    // glob) that the runtime reads and turns into file-bytes.
    //
    // Cardinality is driven exclusively by the arg definition's `is_sequence`
    // flag — URN tags carry semantic shape only.
    //
    // - `is_sequence = true`  → emit a CBOR `Array` of file bytes, regardless
    //   of whether the incoming value was a single path or a list.
    // - `is_sequence = false` → expand to exactly one file and emit a single
    //   CBOR `Bytes`. More than one resolved file is a configuration error
    //   at this layer — CLI-mode dispatch is responsible for iterating the
    //   handler when it detects a glob-to-many against a scalar arg.
    let file_path_base = MediaUrn::from_string("media:file-path")
        .map_err(|e| RuntimeError::Handler(format!("Invalid file-path base pattern: {}", e)))?;

    for arg in arguments.iter_mut() {
        let ciborium::Value::Map(ref mut arg_map) = arg else {
            continue;
        };

        let mut urn_str: Option<String> = None;
        let mut value_snapshot: Option<ciborium::Value> = None;
        for (k, v) in arg_map.iter() {
            if let ciborium::Value::Text(key) = k {
                match key.as_str() {
                    "media_urn" => {
                        if let ciborium::Value::Text(s) = v {
                            urn_str = Some(s.clone());
                        }
                    }
                    "value" => value_snapshot = Some(v.clone()),
                    _ => {}
                }
            }
        }

        let (Some(urn_str), Some(value)) = (urn_str, value_snapshot) else {
            continue;
        };

        let arg_urn = MediaUrn::from_string(&urn_str).map_err(|e| {
            RuntimeError::Handler(format!("Invalid argument media URN '{}': {}", urn_str, e))
        })?;

        if !file_path_base
            .accepts(&arg_urn)
            .map_err(|e| RuntimeError::Handler(format!("URN matching failed: {}", e)))?
        {
            continue;
        }

        // Look up the cap's arg definition by URN equivalence (NOT string
        // compare) — the arg we received may carry the same tags in a
        // different textual order.
        let arg_def = arg_defs.iter().find_map(|(parsed, info)| {
            if parsed.is_equivalent(&arg_urn).unwrap_or(false) {
                Some(info)
            } else {
                None
            }
        });

        let Some(arg_def) = arg_def else {
            // File-path arg with no matching definition: leave it alone.
            continue;
        };

        // Args without a stdin source pass the path bytes through verbatim
        // — the handler reads them itself (rare but legal).
        let Some(ref stdin_target) = arg_def.stdin_target else {
            continue;
        };

        let paths = expand_file_path_value(&value, &urn_str, is_cli_mode)?;

        if !arg_def.is_sequence {
            if paths.len() != 1 {
                return Err(RuntimeError::Handler(format!(
                    "File-path arg '{}' declared is_sequence=false resolved to {} files; \
                     expected exactly 1. CLI-mode dispatch should have iterated the \
                     handler across the expanded files before calling the runtime.",
                    urn_str,
                    paths.len()
                )));
            }
            let bytes = std::fs::read(&paths[0]).map_err(|e| {
                RuntimeError::Handler(format!(
                    "Failed to read file '{}': {}",
                    paths[0].display(),
                    e
                ))
            })?;
            replace_arg_value(arg_map, ciborium::Value::Bytes(bytes), stdin_target.clone());
        } else {
            let mut items: Vec<ciborium::Value> = Vec::with_capacity(paths.len());
            for p in &paths {
                let bytes = std::fs::read(p).map_err(|e| {
                    RuntimeError::Handler(format!("Failed to read file '{}': {}", p.display(), e))
                })?;
                items.push(ciborium::Value::Bytes(bytes));
            }
            replace_arg_value(arg_map, ciborium::Value::Array(items), stdin_target.clone());
        }
    }

    // Validate: at least ONE argument must match the cap's declared in=spec,
    // unless the cap takes no input (in=media:void). After file-path
    // auto-conversion, an arg's media_urn may have been relabeled to the
    // arg-def's stdin-source target rather than the original
    // `media:file-path;...`, so we also accept any stdin-source target URN
    // as a valid match.
    let void_urn = MediaUrn::from_string("media:void")
        .map_err(|e| RuntimeError::Handler(format!("Invalid void URN literal: {}", e)))?;
    let is_void_input = expected_media_urn
        .as_ref()
        .and_then(|expected| expected.is_equivalent(&void_urn).ok())
        .unwrap_or(false);

    if !is_void_input {
        // Collect all valid target URNs: in_spec + every arg-def's stdin
        // source target.
        let mut valid_targets: Vec<MediaUrn> = Vec::new();
        if let Some(ref expected) = expected_media_urn {
            valid_targets.push(expected.clone());
        }
        for (_, info) in &arg_defs {
            if let Some(ref stdin_urn_str) = info.stdin_target {
                if let Ok(stdin_urn) = MediaUrn::from_string(stdin_urn_str) {
                    valid_targets.push(stdin_urn);
                }
            }
        }

        let mut found_matching_arg = false;
        for arg in &arguments {
            if let ciborium::Value::Map(map) = arg {
                for (k, v) in map {
                    if let (ciborium::Value::Text(key), ciborium::Value::Text(urn_str)) = (k, v) {
                        if key == "media_urn" {
                            if let Ok(arg_urn) = MediaUrn::from_string(urn_str) {
                                for target in &valid_targets {
                                    // Use is_comparable for discovery: are they on the same chain?
                                    if arg_urn.is_comparable(target).unwrap_or(false) {
                                        found_matching_arg = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if found_matching_arg {
                        break;
                    }
                }
                if found_matching_arg {
                    break;
                }
            }
        }

        if !found_matching_arg {
            return Err(RuntimeError::Deserialize(format!(
                "No argument found matching expected input media type '{}' in CBOR arguments",
                expected_input
            )));
        }
    }

    // After file-path conversion and validation, return the full CBOR array
    // Handler will parse it and extract arguments by matching against in_spec
    let modified_cbor = ciborium::Value::Array(arguments);
    let mut serialized = Vec::new();
    ciborium::into_writer(&modified_cbor, &mut serialized).map_err(|e| {
        RuntimeError::Serialize(format!("Failed to serialize modified CBOR: {}", e))
    })?;

    Ok(serialized)
}

/// Compute the per-iteration CBOR argument payloads for a CLI invocation.
///
/// The input is the raw payload produced by `build_payload_from_cli` — a
/// CBOR array of `{media_urn, value}` maps where file-path values are still
/// raw path or glob strings.
///
/// Rules:
/// - An arg whose media URN specializes `media:file-path` is **iterable**
///   iff its arg-definition declares `is_sequence = false` **and** its raw
///   value expands to more than one concrete file.
/// - Zero iterable args → return the payload unchanged (single iteration).
/// - One iterable arg → return one payload per expanded file, each with the
///   iterable arg's value replaced by that single path as a `Text` value.
///   `extract_effective_payload` then reads the single file and emits bytes.
/// - Two or more iterable args → hard error: the ForEach axis is ambiguous
///   and there is no user-specified policy for a cartesian product.
fn build_cli_foreach_iterations(
    raw_payload: &[u8],
    cap: &Cap,
) -> Result<Vec<Vec<u8>>, RuntimeError> {
    let file_path_base = MediaUrn::from_string("media:file-path")
        .map_err(|e| RuntimeError::Handler(format!("Invalid file-path base pattern: {}", e)))?;

    let cbor_value: ciborium::Value = ciborium::from_reader(raw_payload)
        .map_err(|e| RuntimeError::Deserialize(format!("Failed to parse CBOR arguments: {}", e)))?;
    let arguments = match cbor_value {
        ciborium::Value::Array(ref arr) => arr.clone(),
        _ => {
            return Err(RuntimeError::Deserialize(
                "CBOR arguments must be an array".to_string(),
            ))
        }
    };

    // Build arg-def map for is_sequence lookup via URN equivalence.
    let arg_defs: Vec<(MediaUrn, bool)> = cap
        .get_args()
        .iter()
        .filter_map(|a| {
            MediaUrn::from_string(&a.media_urn)
                .ok()
                .map(|u| (u, a.is_sequence))
        })
        .collect();

    let mut iterable: Option<(usize, Vec<std::path::PathBuf>)> = None;
    for (idx, arg) in arguments.iter().enumerate() {
        let ciborium::Value::Map(arg_map) = arg else {
            continue;
        };
        let mut urn_str: Option<String> = None;
        let mut value: Option<ciborium::Value> = None;
        for (k, v) in arg_map {
            if let ciborium::Value::Text(key) = k {
                match key.as_str() {
                    "media_urn" => {
                        if let ciborium::Value::Text(s) = v {
                            urn_str = Some(s.clone());
                        }
                    }
                    "value" => value = Some(v.clone()),
                    _ => {}
                }
            }
        }
        let (Some(urn_str), Some(value)) = (urn_str, value) else {
            continue;
        };
        let arg_urn = MediaUrn::from_string(&urn_str).map_err(|e| {
            RuntimeError::Handler(format!("Invalid argument media URN '{}': {}", urn_str, e))
        })?;
        if !file_path_base
            .accepts(&arg_urn)
            .map_err(|e| RuntimeError::Handler(format!("URN matching failed: {}", e)))?
        {
            continue;
        }

        let is_sequence_arg = arg_defs
            .iter()
            .find(|(p, _)| p.is_equivalent(&arg_urn).unwrap_or(false))
            .map(|(_, s)| *s)
            .unwrap_or(false);

        if is_sequence_arg {
            // Sequence args take multiple files as-is; no ForEach iteration.
            continue;
        }

        let paths = expand_file_path_value(&value, &urn_str, true)?;
        if paths.len() <= 1 {
            continue;
        }

        if iterable.is_some() {
            return Err(RuntimeError::Handler(
                "Multiple file-path arguments with is_sequence=false each resolved \
                 to more than one file; the ForEach axis is ambiguous. Declare at \
                 most one such arg as scalar, or mark additional args as \
                 is_sequence=true."
                    .to_string(),
            ));
        }
        iterable = Some((idx, paths));
    }

    let Some((idx, paths)) = iterable else {
        return Ok(vec![raw_payload.to_vec()]);
    };

    // Build N per-iteration payloads: clone the CBOR array, replace the
    // iterable arg's value at index `idx` with a single-path Text value.
    let mut out = Vec::with_capacity(paths.len());
    for path in paths {
        let mut args_for_iter = arguments.clone();
        if let ciborium::Value::Map(ref mut arg_map) = args_for_iter[idx] {
            for (k, v) in arg_map.iter_mut() {
                if let ciborium::Value::Text(key) = k {
                    if key == "value" {
                        *v = ciborium::Value::Text(path.to_string_lossy().into_owned());
                    }
                }
            }
        }
        let wrapped = ciborium::Value::Array(args_for_iter);
        let mut buf = Vec::new();
        ciborium::into_writer(&wrapped, &mut buf).map_err(|e| {
            RuntimeError::Serialize(format!("Failed to re-encode iter payload: {}", e))
        })?;
        out.push(buf);
    }

    Ok(out)
}

/// Expand a file-path arg value into a concrete list of filesystem paths.
///
/// The incoming value may be:
/// - `Bytes` or `Text` containing a single path or a single glob pattern
/// - `Array` of `Bytes`/`Text` items, each a path or a glob (CBOR mode only)
///
/// Globs (detected via `*`, `?`, or `[`) are expanded and the results filtered
/// to regular files. Literal paths must exist and point at a regular file.
/// Returns at least one path on success; empty matches fail hard so the
/// caller never has to guard against a silently-empty list.
fn expand_file_path_value(
    value: &ciborium::Value,
    urn_str: &str,
    is_cli_mode: bool,
) -> Result<Vec<std::path::PathBuf>, RuntimeError> {
    let raw_paths: Vec<String> = match value {
        ciborium::Value::Bytes(b) => vec![String::from_utf8_lossy(b).into_owned()],
        ciborium::Value::Text(t) => vec![t.clone()],
        ciborium::Value::Array(arr) => {
            if is_cli_mode {
                return Err(RuntimeError::Handler(format!(
                    "File-path arg '{}' received a CBOR Array value in CLI mode; CLI \
                     dispatch must expand globs before calling into the runtime",
                    urn_str
                )));
            }
            let mut paths = Vec::with_capacity(arr.len());
            for item in arr {
                match item {
                    ciborium::Value::Text(s) => paths.push(s.clone()),
                    ciborium::Value::Bytes(b) => {
                        paths.push(String::from_utf8_lossy(b).into_owned())
                    }
                    other => {
                        return Err(RuntimeError::Handler(format!(
                            "File-path arg '{}' array contained an unsupported CBOR item: {:?}",
                            urn_str, other
                        )));
                    }
                }
            }
            paths
        }
        other => {
            return Err(RuntimeError::Handler(format!(
                "File-path arg '{}' value must be Bytes, Text, or (CBOR mode) Array — got {:?}",
                urn_str, other
            )));
        }
    };

    let mut resolved: Vec<std::path::PathBuf> = Vec::new();
    for raw in &raw_paths {
        let is_glob = raw.contains('*') || raw.contains('?') || raw.contains('[');
        if is_glob {
            let paths = glob::glob(raw).map_err(|e| {
                RuntimeError::Handler(format!("Invalid glob pattern '{}': {}", raw, e))
            })?;
            let before = resolved.len();
            for p in paths {
                let p = p.map_err(|e| RuntimeError::Handler(format!("Glob error: {}", e)))?;
                if p.is_file() {
                    resolved.push(p);
                }
            }
            if resolved.len() == before {
                return Err(RuntimeError::Handler(format!(
                    "No files matched glob pattern '{}'",
                    raw
                )));
            }
        } else {
            let path = std::path::PathBuf::from(raw);
            if !path.exists() {
                return Err(RuntimeError::Handler(format!("File not found: '{}'", raw)));
            }
            if !path.is_file() {
                return Err(RuntimeError::Handler(format!(
                    "Path is not a regular file: '{}'",
                    raw
                )));
            }
            resolved.push(path);
        }
    }

    Ok(resolved)
}

/// Replace an argument map's `value` and `media_urn` entries in place. Used by
/// `extract_effective_payload` after reading file bytes so the downstream
/// handler sees the post-conversion URN, not the original `media:file-path`.
fn replace_arg_value(
    arg_map: &mut Vec<(ciborium::Value, ciborium::Value)>,
    new_value: ciborium::Value,
    new_media_urn: String,
) {
    for (k, v) in arg_map.iter_mut() {
        if let ciborium::Value::Text(key) = k {
            match key.as_str() {
                "value" => *v = new_value.clone(),
                "media_urn" => *v = ciborium::Value::Text(new_media_urn.clone()),
                _ => {}
            }
        }
    }
}

#[async_trait]
impl PeerInvoker for PeerInvokerImpl {
    fn call(&self, cap_urn: &str) -> Result<PeerCall, RuntimeError> {
        let request_id = MessageId::new_uuid();
        // Create tokio channel for response frames (unbounded to avoid backpressure issues)
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

        // Register pending request before sending REQ
        {
            let mut pending = self.pending_requests.lock().unwrap();
            pending.insert(
                request_id.clone(),
                PendingPeerRequest {
                    sender,
                    origin_request_id: self.origin_request_id.clone(),
                    origin_routing_id: self.origin_routing_id.clone(),
                },
            );
        }

        // Send REQ with empty payload, stamped with parent_rid for cancel cascade
        let mut req_frame = Frame::req(request_id.clone(), cap_urn, vec![], "application/cbor");
        let mut meta = req_frame.meta.take().unwrap_or_default();
        meta.insert(
            "parent_rid".to_string(),
            match &self.origin_request_id {
                MessageId::Uuid(bytes) => ciborium::Value::Bytes(bytes.to_vec()),
                MessageId::Uint(n) => ciborium::Value::Integer((*n as i64).into()),
            },
        );
        req_frame.meta = Some(meta);
        self.output_tx.send(req_frame).map_err(|_| {
            self.pending_requests.lock().unwrap().remove(&request_id);
            RuntimeError::PeerRequest("Output channel closed".to_string())
        })?;

        // Create FrameSender for the PeerCall's arg OutputStreams
        let sender_arc: Arc<dyn FrameSender> = Arc::new(ChannelFrameSender {
            tx: self.output_tx.clone(),
            drops: Arc::clone(&self.drops),
        });

        Ok(PeerCall {
            sender: sender_arc,
            request_id,
            max_chunk: self.max_chunk,
            response_rx: Some(receiver),
            credit_router: Some(self.credit_router.clone()),
            initial_credit: self.initial_credit,
        })
    }
}

// =============================================================================
// DEMUX — splits a raw Frame channel into per-stream InputStream channels
// =============================================================================

/// Context for file-path auto-conversion in the Demux.
struct FilePathContext {
    file_path_pattern: MediaUrn,
    cap_urn: String,
    manifest: Option<CapManifest>,
}

impl FilePathContext {
    fn new(cap_urn: &str, manifest: Option<CapManifest>) -> Result<Self, RuntimeError> {
        Ok(Self {
            file_path_pattern: MediaUrn::from_string("media:file-path").map_err(|e| {
                RuntimeError::Handler(format!("Failed to create file-path pattern: {}", e))
            })?,
            cap_urn: cap_urn.to_string(),
            manifest,
        })
    }

    fn is_file_path(&self, media_urn_str: &str) -> bool {
        let arg_urn = match MediaUrn::from_string(media_urn_str) {
            Ok(u) => u,
            Err(_) => return false,
        };
        self.file_path_pattern.accepts(&arg_urn).unwrap_or(false)
    }

    /// Find a cap arg whose media URN is equivalent to the incoming URN.
    /// Uses `MediaUrn::is_equivalent` (tag-set equality) rather than string
    /// comparison so order-normalization and whitespace don't matter.
    fn find_arg<'a>(&'a self, incoming: &MediaUrn) -> Option<&'a CapArg> {
        let manifest = self.manifest.as_ref()?;
        let cap_def = manifest
            .all_caps()
            .into_iter()
            .find(|c| c.urn.to_string() == self.cap_urn)?;
        cap_def.args.iter().find(|a| {
            MediaUrn::from_string(&a.media_urn)
                .map(|arg_urn| arg_urn.is_equivalent(incoming).unwrap_or(false))
                .unwrap_or(false)
        })
    }

    /// Given the media URN of an incoming file-path stream, return the
    /// matching arg's stdin-source target URN.
    fn resolve_stdin_urn(&self, file_path_media_urn: &str) -> Option<String> {
        let incoming = MediaUrn::from_string(file_path_media_urn).ok()?;
        let arg_def = self.find_arg(&incoming)?;
        arg_def.sources.iter().find_map(|s| {
            if let ArgSource::Stdin { stdin } = s {
                Some(stdin.clone())
            } else {
                None
            }
        })
    }

    /// Return the matching arg's `is_sequence` declaration. Defaults to
    /// `false` when no matching arg is found (the conservative scalar path).
    fn arg_is_sequence(&self, file_path_media_urn: &str) -> bool {
        let Ok(incoming) = MediaUrn::from_string(file_path_media_urn) else {
            return false;
        };
        self.find_arg(&incoming)
            .map(|a| a.is_sequence)
            .unwrap_or(false)
    }
}

/// Runtime-side context for LIVE-FEED reference resolution (13.2 §Reference
/// Media, live family) — the live sibling of [`FilePathContext`]. An
/// incoming stream whose media URN carries the `live` marker is a
/// reference: the demux accumulates its selector value, opens the feed
/// through the built-in capture dispatch, and delivers an UNBOUNDED SEQUENCE
/// `InputStream` labeled with the arg's stdin content URN. Opened feeds
/// register their handles so a stop (non-force Cancel on a feed-bearing
/// request) can close the tap and let the run drain (15.2 §Runs Stop).
pub(crate) struct LiveFeedContext {
    cap_urn: String,
    manifest: Option<CapManifest>,
    /// The runtime-wide overrun aggregate the opened feeds count into
    /// (rides heartbeat meta as `overruns_total`).
    overruns_total: Arc<std::sync::atomic::AtomicU64>,
    /// This request's open feed handles, shared with the runtime's per-rid
    /// registry (the stop path closes through the same Arc).
    handles: Arc<Mutex<Vec<crate::bifaci::live_feed::LiveFeedHandle>>>,
}

impl LiveFeedContext {
    fn new(
        cap_urn: &str,
        manifest: Option<CapManifest>,
        overruns_total: Arc<std::sync::atomic::AtomicU64>,
        handles: Arc<Mutex<Vec<crate::bifaci::live_feed::LiveFeedHandle>>>,
    ) -> Result<Self, RuntimeError> {
        // Store the CANONICAL rendering so the manifest lookup compares
        // canonical-to-canonical, independent of the caller's surface
        // spelling (tag order, quoting).
        let canonical = crate::CapUrn::from_string(cap_urn)
            .map_err(|e| {
                RuntimeError::Handler(format!(
                    "live-feed context: cap URN '{}' does not parse: {}",
                    cap_urn, e
                ))
            })?
            .to_string();
        Ok(Self {
            cap_urn: canonical,
            manifest,
            overruns_total,
            handles,
        })
    }

    fn is_live_feed(&self, media_urn_str: &str) -> bool {
        MediaUrn::from_string(media_urn_str)
            .map(|u| u.is_live_feed())
            .unwrap_or(false)
    }

    fn find_arg<'a>(&'a self, incoming: &MediaUrn) -> Option<&'a CapArg> {
        let manifest = self.manifest.as_ref()?;
        let cap_def = manifest
            .all_caps()
            .into_iter()
            .find(|c| c.urn.to_string() == self.cap_urn)?;
        cap_def.args.iter().find(|a| {
            MediaUrn::from_string(&a.media_urn)
                .map(|arg_urn| arg_urn.is_equivalent(incoming).unwrap_or(false))
                .unwrap_or(false)
        })
    }

    /// The cap's MAIN INPUT arg (the stdin-sourced arg carrying `in=`,
    /// via the encapsulated `CapArg::is_main_input` predicate) — the arg a
    /// transport-blind cap consumes a live feed's CONTENT through.
    fn find_main_input_arg(&self) -> Option<&CapArg> {
        let manifest = self.manifest.as_ref()?;
        let cap_def = manifest
            .all_caps()
            .into_iter()
            .find(|c| c.urn.to_string() == self.cap_urn)?;
        let cap_urn = crate::CapUrn::from_string(&self.cap_urn).ok()?;
        let in_spec = cap_urn.in_media_urn().ok()?;
        cap_def.args.iter().find(|a| a.is_main_input(&in_spec))
    }

    /// Resolve the live reference into an open feed and the InputStream
    /// delivering it, by one of two arg matches (13.2 §Reference Media):
    ///
    /// 1. **Explicit reference arg** — the cap declares an arg whose urn is
    ///    equivalent to the incoming reference (a cap that WANTS the feed,
    ///    e.g. `drain_feed`). The delivered stream is labeled with that
    ///    arg's declared stdin content urn.
    /// 2. **Main-input fallback** — the cap is transport-blind (a generic
    ///    consumer planned over the feed's CONTENT type): the reference
    ///    resolves against the cap's main input, and the registered
    ///    provider's `content_urn()` must CONFORM TO the main input's
    ///    declared urn. The delivered stream is labeled with the provider's
    ///    content urn (the more specific side of that conformance).
    ///
    /// Hard errors on: no matching arg, an arg without a stdin source (a
    /// live reference MUST resolve to piped content), an arg not declared
    /// `is_sequence` (a feed is a sequence of items), a provider whose
    /// content does not conform to the main input, an unparseable
    /// selector, or a provider/device failure.
    fn resolve(
        &self,
        reference_urn: &str,
        selector_bytes: &[u8],
    ) -> Result<InputStream, StreamError> {
        use crate::bifaci::live_feed::LiveFeedSelector;

        let incoming = MediaUrn::from_string(reference_urn)
            .map_err(|e| StreamError::Protocol(format!("invalid live-feed reference URN: {e}")))?;

        // Which arg consumes this reference, and what content label the
        // delivered stream carries.
        let (arg, content_urn) = match self.find_arg(&incoming) {
            Some(explicit) => {
                let stdin_urn = explicit
                    .sources
                    .iter()
                    .find_map(|s| match s {
                        ArgSource::Stdin { stdin } => Some(stdin.clone()),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        StreamError::Protocol(format!(
                            "live-feed arg '{}' on cap '{}' declares no stdin source — a live \
                             reference must resolve to piped content (13.2 §Reference Media)",
                            explicit.media_urn, self.cap_urn
                        ))
                    })?;
                (explicit, stdin_urn)
            }
            None => {
                let main = self.find_main_input_arg().ok_or_else(|| {
                    StreamError::Protocol(format!(
                        "cap '{}' declares no arg matching live-feed reference '{}' and no \
                         stdin-sourced main input to resolve it against",
                        self.cap_urn, reference_urn
                    ))
                })?;
                let provider_content = crate::capture::content_urn_for(&incoming)
                    .map(str::to_string)
                    .ok_or_else(|| {
                        StreamError::Protocol(format!(
                            "live reference '{}' is not a known device family — no \
                             capture backend exists for it (13.2 §Reference Media)",
                            reference_urn
                        ))
                    })?;
                let content = MediaUrn::from_string(&provider_content).map_err(|e| {
                    StreamError::Protocol(format!(
                        "provider for '{}' declares an invalid content urn '{}': {e}",
                        reference_urn, provider_content
                    ))
                })?;
                let main_urn = MediaUrn::from_string(&main.media_urn).map_err(|e| {
                    StreamError::Protocol(format!(
                        "main input urn '{}' on cap '{}' is not a valid media URN: {e}",
                        main.media_urn, self.cap_urn
                    ))
                })?;
                if !content.conforms_to(&main_urn).unwrap_or(false) {
                    return Err(StreamError::Protocol(format!(
                        "live-feed reference '{}' delivers '{}' which does not conform to \
                         cap '{}' main input '{}' — this machine cannot consume that device",
                        reference_urn, provider_content, self.cap_urn, main.media_urn
                    )));
                }
                (main, provider_content)
            }
        };

        if !arg.is_sequence {
            return Err(StreamError::Protocol(format!(
                "live-feed arg '{}' on cap '{}' must declare is_sequence=true — a live \
                 feed is an unbounded SEQUENCE of items",
                arg.media_urn, self.cap_urn
            )));
        }
        let selector = LiveFeedSelector::parse(selector_bytes)
            .map_err(|e| StreamError::Protocol(e.to_string()))?;
        let opened =
            crate::capture::open(reference_urn, selector, Arc::clone(&self.overruns_total))
                .map_err(|e| StreamError::Protocol(e.to_string()))?;
        self.handles.lock().unwrap().push(opened.handle);
        Ok(InputStream {
            media_urn: content_urn,
            stream_meta: opened.stream_meta,
            rx: InputRx::Bounded(opened.rx),
            unbounded: true,
            grants: None,
        })
    }
}

/// Reassembly state for one sequence-mode input stream (`is_sequence = true`
/// on STREAM_START). Sequence producers (`emit_list_item`) CBOR-encode each
/// item once and split the encoded bytes across CHUNK frames at `max_chunk`
/// boundaries — a frame payload is a raw RFC 8742 fragment, NOT a
/// self-contained CBOR value. The demux must therefore buffer fragments and
/// decode at item granularity; decoding per frame fails with a CBOR
/// UnexpectedEof on any item larger than `max_chunk` (the bug class that
/// broke cap→cap forwarding of rendered page images).
struct SeqReassembly {
    /// Raw fragment bytes of the item currently being received.
    buf: Vec<u8>,
    /// Per-item metadata — carried on the item's FIRST fragment frame only
    /// (the `emit_list_item` contract), held until the item completes.
    item_meta: Option<StreamMeta>,
    /// Immediate-flush grant emitter for fragment continuation frames.
    /// Credit is frame-granular on the wire but the handler consumes (and
    /// grants) per ITEM; every fragment after an item's first frame is
    /// credited back here on arrival, so an item spanning more frames than
    /// the credit window can still finish arriving. `None` in uncredited
    /// contexts.
    fragment_grants: Option<InputGrantEmitter>,
}

/// Try to decode one self-delimiting CBOR item from the front of `buf`.
///
/// - `Ok(Some((value, consumed)))` — one complete item; `consumed` bytes used.
/// - `Ok(None)` — `buf` holds only a prefix of an item; wait for more frames.
///   (CBOR definite-length encoding is prefix-free, so a truncated item can
///   never mis-decode as a complete one.)
/// - `Err` — the bytes are not valid CBOR at all.
fn try_decode_sequence_item(buf: &[u8]) -> Result<Option<(ciborium::Value, usize)>, StreamError> {
    let mut cursor = std::io::Cursor::new(buf);
    match ciborium::from_reader::<ciborium::Value, _>(&mut cursor) {
        Ok(value) => Ok(Some((value, cursor.position() as usize))),
        Err(ciborium::de::Error::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            Ok(None)
        }
        Err(e) => Err(StreamError::Decode(e.to_string())),
    }
}

/// Demux for multi-stream mode (handler input).
/// Spawns a background tokio task that reads raw Frame channel and splits into
/// per-stream InputStream channels. Handles file-path interception.
///
/// Input: crossbeam channel of raw frames (fed by main loop's active_requests)
/// Output: tokio channels for async stream consumption
fn demux_multi_stream(
    raw_rx: crossbeam_channel::Receiver<Frame>,
    file_path_ctx: Option<FilePathContext>,
    live_feed_ctx: Option<LiveFeedContext>,
    credit: Option<InputCreditContext>,
) -> InputPackage {
    let (streams_tx, streams_rx) = tokio::sync::mpsc::unbounded_channel();

    let handle = tokio::task::spawn_blocking(move || {
        // Per-stream channels: stream_id → chunk sender (tokio unbounded for async recv)
        let mut stream_channels: HashMap<
            String,
            tokio::sync::mpsc::UnboundedSender<
                Result<(ciborium::Value, Option<StreamMeta>), StreamError>,
            >,
        > = HashMap::new();
        // File-path accumulators: stream_id → (media_urn, accumulated_chunk_payloads)
        let mut fp_accumulators: HashMap<String, (String, Vec<Vec<u8>>)> = HashMap::new();
        // Live-feed reference accumulators: stream_id → (reference_urn,
        // accumulated selector-value payloads). Resolved on STREAM_END
        // (mirrors the file-path pattern: the value is a small reference,
        // never the data).
        let mut lf_accumulators: HashMap<String, (String, Vec<Vec<u8>>)> = HashMap::new();
        // Per-stream remaining credit windows (L10/L12). The window starts at
        // the negotiated initial_credit; handler consumption (grants) extends
        // it; a chunk arriving with the window at zero is a fatal
        // CREDIT_VIOLATION. The demux itself never blocks — accounting keeps
        // control frames flowing regardless of data pressure.
        let mut stream_windows: HashMap<String, Arc<std::sync::atomic::AtomicI64>> = HashMap::new();
        // Sequence-mode streams: stream_id → item reassembly state (see
        // `SeqReassembly` — frame payloads are RFC 8742 fragments, decoded
        // at item granularity).
        let mut seq_reassembly: HashMap<String, SeqReassembly> = HashMap::new();

        for frame in raw_rx {
            match frame.frame_type {
                FrameType::StreamStart => {
                    let stream_id = match frame.stream_id.as_ref() {
                        Some(id) => id.clone(),
                        None => {
                            let _ = streams_tx.send(Err(StreamError::Protocol(
                                "STREAM_START missing stream_id".into(),
                            )));
                            break;
                        }
                    };
                    let media_urn = frame.media_urn.as_ref().cloned().unwrap_or_default();

                    // Check if file-path (only when FilePathContext provided)
                    let is_fp = file_path_ctx
                        .as_ref()
                        .map_or(false, |ctx| ctx.is_file_path(&media_urn));
                    // Check if live-feed reference (only when LiveFeedContext provided)
                    let is_lf = live_feed_ctx
                        .as_ref()
                        .map_or(false, |ctx| ctx.is_live_feed(&media_urn));

                    if is_fp {
                        fp_accumulators.insert(stream_id, (media_urn, Vec::new()));
                    } else if is_lf {
                        lf_accumulators.insert(stream_id, (media_urn, Vec::new()));
                    } else {
                        let (chunk_tx, chunk_rx) = tokio::sync::mpsc::unbounded_channel();
                        stream_channels.insert(stream_id.clone(), chunk_tx);
                        let grants = credit.as_ref().map(|ctx| {
                            let window = Arc::new(std::sync::atomic::AtomicI64::new(
                                ctx.initial_credit as i64,
                            ));
                            stream_windows.insert(stream_id.clone(), Arc::clone(&window));
                            InputGrantEmitter {
                                sender: Arc::clone(&ctx.sender),
                                rid: ctx.rid.clone(),
                                xid: ctx.xid.clone(),
                                stream_id: Some(stream_id.clone()),
                                direction: crate::bifaci::frame::CreditDirection::Request,
                                batch: (ctx.initial_credit / 2).max(1),
                                consumed_since_grant: 0,
                                window,
                            }
                        });
                        if frame.is_sequence.unwrap_or(false) {
                            seq_reassembly.insert(
                                stream_id.clone(),
                                SeqReassembly {
                                    buf: Vec::new(),
                                    item_meta: None,
                                    fragment_grants: grants.as_ref().map(|g| g.fragment_sibling()),
                                },
                            );
                        }
                        let input_stream = InputStream {
                            media_urn,
                            stream_meta: frame.meta,
                            rx: InputRx::Unbounded(chunk_rx),
                            unbounded: frame.unbounded.unwrap_or(false),
                            grants,
                        };
                        if streams_tx.send(Ok(input_stream)).is_err() {
                            break; // Handler dropped InputPackage
                        }
                    }
                }

                FrameType::Chunk => {
                    let stream_id = frame.stream_id.as_ref().cloned().unwrap_or_default();

                    // File-path accumulation? The demux itself consumes these
                    // chunks (they never reach the handler), so the sender's
                    // window is replenished implicitly by the accumulator
                    // being unbounded-in-practice: fp streams carry short
                    // path strings, never bulk data, and are consumed on
                    // arrival — no violation accounting needed.
                    if let Some((_, ref mut chunks)) = fp_accumulators.get_mut(&stream_id) {
                        if let Some(payload) = frame.payload {
                            chunks.push(payload);
                        }
                        continue;
                    }
                    // Live-feed reference accumulation — same contract as
                    // file paths: the demux consumes the small selector
                    // value; it never reaches the handler.
                    if let Some((_, ref mut chunks)) = lf_accumulators.get_mut(&stream_id) {
                        if let Some(payload) = frame.payload {
                            chunks.push(payload);
                        }
                        continue;
                    }

                    // Credit-violation check (L12): a chunk beyond the granted
                    // window is a fatal protocol error for this request.
                    if let Some(window) = stream_windows.get(&stream_id) {
                        let before = window.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                        if before <= 0 {
                            if let Some(tx) = stream_channels.get(&stream_id) {
                                let _ = tx.send(Err(StreamError::Protocol(format!(
                                    "CREDIT_VIOLATION: chunk received beyond the granted window on stream {} (L12)",
                                    stream_id
                                ))));
                            }
                            continue;
                        }
                    }

                    // Regular stream — decode CBOR and forward with per-chunk meta
                    if let Some(tx) = stream_channels.get(&stream_id) {
                        if let Some(payload) = frame.payload {
                            // Checksum validation (MANDATORY in protocol v2)
                            let expected_checksum = match frame.checksum {
                                Some(c) => c,
                                None => {
                                    let _ = tx.send(Err(StreamError::Protocol(
                                        "CHUNK frame missing required checksum field".to_string(),
                                    )));
                                    continue;
                                }
                            };
                            let actual = Frame::compute_checksum(&payload);
                            if actual != expected_checksum {
                                let _ = tx.send(Err(StreamError::Protocol(format!(
                                    "Checksum mismatch: expected={}, actual={}",
                                    expected_checksum, actual
                                ))));
                                continue;
                            }
                            let chunk_meta = frame.meta;
                            if let Some(seq) = seq_reassembly.get_mut(&stream_id) {
                                // Sequence stream: the payload is a raw RFC 8742
                                // fragment. Buffer it and deliver at ITEM
                                // granularity (see `SeqReassembly`).
                                if seq.buf.is_empty() {
                                    // First fragment of a new item carries the
                                    // per-item metadata (emit_list_item contract).
                                    seq.item_meta = chunk_meta;
                                } else if let Some(g) = seq.fragment_grants.as_mut() {
                                    // Continuation fragment: credit it back
                                    // immediately — the handler grants one frame
                                    // per consumed ITEM, so without this an item
                                    // spanning more frames than the credit window
                                    // could never finish arriving.
                                    g.consumed();
                                }
                                seq.buf.extend_from_slice(&payload);
                                loop {
                                    match try_decode_sequence_item(&seq.buf) {
                                        Ok(Some((value, consumed))) => {
                                            seq.buf.drain(..consumed);
                                            let meta = seq.item_meta.take();
                                            let _ = tx.send(Ok((value, meta)));
                                            if seq.buf.is_empty() {
                                                break;
                                            }
                                        }
                                        Ok(None) => break, // prefix — need more frames
                                        Err(e) => {
                                            let _ = tx.send(Err(e));
                                            seq.buf.clear();
                                            break;
                                        }
                                    }
                                }
                            } else {
                                // Scalar stream: every frame payload is a
                                // self-contained CBOR value (`write` wraps each
                                // piece as its own Value::Bytes).
                                match ciborium::from_reader::<ciborium::Value, _>(&payload[..]) {
                                    Ok(value) => {
                                        let _ = tx.send(Ok((value, chunk_meta)));
                                    }
                                    Err(e) => {
                                        let _ = tx.send(Err(StreamError::Decode(e.to_string())));
                                    }
                                }
                            }
                        }
                    }
                }

                FrameType::StreamEnd => {
                    let stream_id = frame.stream_id.as_ref().cloned().unwrap_or_default();

                    // Live-feed reference ended — resolve: open the device
                    // through the registered provider and deliver the
                    // unbounded sequence stream (13.2 §Reference Media).
                    if let Some((reference_urn, chunks)) = lf_accumulators.remove(&stream_id) {
                        let ctx = match live_feed_ctx.as_ref() {
                            Some(ctx) => ctx,
                            None => continue,
                        };
                        // Decode accumulated payloads → selector bytes (the
                        // same value-decode contract as file paths).
                        let mut selector_bytes = Vec::new();
                        for chunk_payload in &chunks {
                            match ciborium::from_reader::<ciborium::Value, _>(&chunk_payload[..]) {
                                Ok(ciborium::Value::Bytes(b)) => selector_bytes.extend(b),
                                Ok(ciborium::Value::Text(t)) => selector_bytes.extend(t.into_bytes()),
                                Ok(other) => {
                                    let mut buf = Vec::new();
                                    let _ = ciborium::into_writer(&other, &mut buf);
                                    selector_bytes.extend(buf);
                                }
                                Err(_) => selector_bytes.extend(chunk_payload),
                            }
                        }
                        match ctx.resolve(&reference_urn, &selector_bytes) {
                            Ok(input_stream) => {
                                if streams_tx.send(Ok(input_stream)).is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                let _ = streams_tx.send(Err(e));
                                break;
                            }
                        }
                        continue;
                    }

                    // File-path stream ended — read file and deliver
                    if let Some((media_urn, chunks)) = fp_accumulators.remove(&stream_id) {
                        let ctx = match file_path_ctx.as_ref() {
                            Some(ctx) => ctx,
                            None => continue,
                        };

                        // Concatenate accumulated CBOR payloads → decode each as Value::Bytes → get path bytes
                        let mut path_bytes = Vec::new();
                        for chunk_payload in &chunks {
                            match ciborium::from_reader::<ciborium::Value, _>(&chunk_payload[..]) {
                                Ok(ciborium::Value::Bytes(b)) => path_bytes.extend(b),
                                Ok(ciborium::Value::Text(s)) => path_bytes.extend(s.into_bytes()),
                                Ok(other) => {
                                    let mut buf = Vec::new();
                                    let _ = ciborium::into_writer(&other, &mut buf);
                                    path_bytes.extend(buf);
                                }
                                Err(_) => {
                                    // Raw bytes (not CBOR-encoded)
                                    path_bytes.extend(chunk_payload);
                                }
                            }
                        }

                        // If the arg has a stdin source, read the file(s)
                        // and relabel. If not, pass through the file path as
                        // a plain value.
                        //
                        // Cardinality is driven by the arg's `is_sequence`
                        // declaration. Scalar args read one file; sequence
                        // args read N files and emit each as its own CHUNK
                        // (sequence mode on the output stream).
                        if let Some(resolved_urn) = ctx.resolve_stdin_urn(&media_urn) {
                            let is_sequence_arg = ctx.arg_is_sequence(&media_urn);
                            let paths_raw = String::from_utf8_lossy(&path_bytes).into_owned();
                            let candidates: Vec<String> = if is_sequence_arg {
                                // Sequence arg: allow a newline-separated list
                                // of paths or globs (plain text, no CBOR wrapping).
                                paths_raw
                                    .lines()
                                    .map(|s| s.trim().to_string())
                                    .filter(|s| !s.is_empty())
                                    .collect()
                            } else {
                                vec![paths_raw]
                            };

                            let mut resolved: Vec<std::path::PathBuf> = Vec::new();
                            let mut expansion_error: Option<String> = None;
                            for raw in &candidates {
                                let is_glob =
                                    raw.contains('*') || raw.contains('?') || raw.contains('[');
                                if is_glob {
                                    match glob::glob(raw) {
                                        Ok(paths) => {
                                            let before = resolved.len();
                                            for p in paths {
                                                match p {
                                                    Ok(p) if p.is_file() => resolved.push(p),
                                                    Ok(_) => {}
                                                    Err(e) => {
                                                        expansion_error =
                                                            Some(format!("Glob error: {}", e));
                                                        break;
                                                    }
                                                }
                                            }
                                            if expansion_error.is_none() && resolved.len() == before
                                            {
                                                expansion_error = Some(format!(
                                                    "No files matched glob pattern '{}'",
                                                    raw
                                                ));
                                            }
                                        }
                                        Err(e) => {
                                            expansion_error = Some(format!(
                                                "Invalid glob pattern '{}': {}",
                                                raw, e
                                            ));
                                        }
                                    }
                                } else {
                                    let p = std::path::PathBuf::from(raw);
                                    if !p.exists() {
                                        expansion_error =
                                            Some(format!("File not found: '{}'", raw));
                                    } else if !p.is_file() {
                                        expansion_error =
                                            Some(format!("Path is not a regular file: '{}'", raw));
                                    } else {
                                        resolved.push(p);
                                    }
                                }
                                if expansion_error.is_some() {
                                    break;
                                }
                            }

                            if let Some(err) = expansion_error {
                                let _ = streams_tx.send(Err(StreamError::Io(err)));
                                break;
                            }

                            if !is_sequence_arg && resolved.len() != 1 {
                                let _ = streams_tx.send(Err(StreamError::Protocol(format!(
                                    "File-path arg with is_sequence=false resolved to {} files; \
                                     expected exactly 1. Sender must declare is_sequence=true to send multiple files.",
                                    resolved.len()
                                ))));
                                break;
                            }

                            let (chunk_tx, chunk_rx) = tokio::sync::mpsc::unbounded_channel();
                            let mut send_failed = false;
                            for path in &resolved {
                                match std::fs::read(path) {
                                    Ok(bytes) => {
                                        if chunk_tx
                                            .send(Ok((ciborium::Value::Bytes(bytes), None)))
                                            .is_err()
                                        {
                                            send_failed = true;
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        let _ = chunk_tx.send(Err(StreamError::Io(format!(
                                            "Failed to read file '{}': {}",
                                            path.display(),
                                            e
                                        ))));
                                        send_failed = true;
                                        break;
                                    }
                                }
                            }
                            drop(chunk_tx);

                            if send_failed {
                                break;
                            }

                            let input_stream = InputStream {
                                media_urn: resolved_urn,
                                stream_meta: None,
                                rx: InputRx::Unbounded(chunk_rx),
                                // Materialized from local files before delivery —
                                // bounded and already fully buffered; no grants.
                                unbounded: false,
                                grants: None,
                            };
                            if streams_tx.send(Ok(input_stream)).is_err() {
                                break;
                            }
                        } else {
                            // No stdin source — pass through the path bytes as-is
                            let (chunk_tx, chunk_rx) = tokio::sync::mpsc::unbounded_channel();
                            let _ = chunk_tx.send(Ok((ciborium::Value::Bytes(path_bytes), None)));
                            drop(chunk_tx);
                            let input_stream = InputStream {
                                media_urn: media_urn.clone(),
                                stream_meta: None,
                                rx: InputRx::Unbounded(chunk_rx),
                                unbounded: false,
                                grants: None,
                            };
                            if streams_tx.send(Ok(input_stream)).is_err() {
                                break;
                            }
                        }
                    } else {
                        // Sequence stream ending mid-item is a truncation —
                        // surface it, never silently drop the partial item.
                        if let Some(seq) = seq_reassembly.remove(&stream_id) {
                            if !seq.buf.is_empty() {
                                if let Some(tx) = stream_channels.get(&stream_id) {
                                    let _ = tx.send(Err(StreamError::Decode(format!(
                                        "sequence stream ended mid-item: {} trailing bytes \
                                         do not form a complete CBOR item",
                                        seq.buf.len()
                                    ))));
                                }
                            }
                        }
                        // Regular stream ended — close per-stream channel
                        stream_channels.remove(&stream_id);
                    }
                }

                FrameType::End => {
                    // All streams done
                    break;
                }

                FrameType::Err => {
                    let (code, class, message, arg_urn) = match remote_error_fields(&frame) {
                        Ok(fields) => fields,
                        Err(message) => {
                            for (_, tx) in &stream_channels {
                                let _ = tx.send(Err(StreamError::Protocol(message.clone())));
                            }
                            let _ = streams_tx.send(Err(StreamError::Protocol(message)));
                            break;
                        }
                    };
                    // Error all open streams
                    for (_, tx) in &stream_channels {
                        let _ = tx.send(Err(StreamError::RemoteError {
                            code: code.clone(),
                            class,
                            message: message.clone(),
                            arg_urn: arg_urn.clone(),
                        }));
                    }
                    stream_channels.clear();
                    let _ = streams_tx.send(Err(StreamError::RemoteError {
                        code,
                        class,
                        message,
                        arg_urn,
                    }));
                    break;
                }

                _ => {
                    // Ignore LOG, HEARTBEAT, etc.
                }
            }
        }
        // Dropping stream_channels closes all per-stream channels
        drop(stream_channels);
    });

    InputPackage {
        rx: streams_rx,
        _demux_handle: Some(handle),
    }
}

/// Demux for single-stream mode (peer response).
/// Reads frames from tokio channel expecting a single stream. Returns PeerResponse
/// that yields both data items and LOG frames through a single receiver.
///
/// Returns immediately — LOG frames are delivered in real-time as they arrive,
/// not blocked until the first data frame. This is critical for keeping the
/// engine's activity timer alive during long peer calls (e.g., model downloads).
fn demux_single_stream(
    mut raw_rx: tokio::sync::mpsc::UnboundedReceiver<Frame>,
    grants: Option<InputGrantEmitter>,
) -> PeerResponse {
    let (item_tx, item_rx) = tokio::sync::mpsc::unbounded_channel();

    // Fragment crediting for sequence-mode responses (same scheme as
    // `demux_multi_stream`): the caller grants one frame per consumed ITEM,
    // so continuation fragments are credited back on arrival here.
    let mut fragment_grants = grants.as_ref().map(|g| g.fragment_sibling());

    tokio::spawn(async move {
        // Sequence reassembly for the single response stream (None until a
        // STREAM_START with is_sequence=true arrives). Sequence frame
        // payloads are RFC 8742 fragments — decode at item granularity.
        let mut seq: Option<SeqReassembly> = None;
        while let Some(frame) = raw_rx.recv().await {
            match frame.frame_type {
                FrameType::StreamStart => {
                    if frame.is_sequence.unwrap_or(false) {
                        seq = Some(SeqReassembly {
                            buf: Vec::new(),
                            item_meta: None,
                            fragment_grants: fragment_grants.take(),
                        });
                    }
                }
                FrameType::Chunk => {
                    if let Some(payload) = frame.payload {
                        // Checksum validation (MANDATORY in protocol v2)
                        let expected_checksum = match frame.checksum {
                            Some(c) => c,
                            None => {
                                let _ = item_tx.send(PeerResponseItem::Data(
                                    Err(StreamError::Protocol(
                                        "CHUNK frame missing required checksum field".to_string(),
                                    )),
                                    None,
                                ));
                                continue;
                            }
                        };
                        let actual = Frame::compute_checksum(&payload);
                        if actual != expected_checksum {
                            let _ = item_tx.send(PeerResponseItem::Data(
                                Err(StreamError::Protocol(format!(
                                    "Checksum mismatch: expected={}, actual={}",
                                    expected_checksum, actual
                                ))),
                                None,
                            ));
                            continue;
                        }
                        let chunk_meta = frame.meta;
                        if let Some(seq) = seq.as_mut() {
                            if seq.buf.is_empty() {
                                seq.item_meta = chunk_meta;
                            } else if let Some(g) = seq.fragment_grants.as_mut() {
                                g.consumed();
                            }
                            seq.buf.extend_from_slice(&payload);
                            loop {
                                match try_decode_sequence_item(&seq.buf) {
                                    Ok(Some((value, consumed))) => {
                                        seq.buf.drain(..consumed);
                                        let meta = seq.item_meta.take();
                                        let _ =
                                            item_tx.send(PeerResponseItem::Data(Ok(value), meta));
                                        if seq.buf.is_empty() {
                                            break;
                                        }
                                    }
                                    Ok(None) => break, // prefix — need more frames
                                    Err(e) => {
                                        let _ = item_tx.send(PeerResponseItem::Data(Err(e), None));
                                        seq.buf.clear();
                                        break;
                                    }
                                }
                            }
                        } else {
                            match ciborium::from_reader::<ciborium::Value, _>(&payload[..]) {
                                Ok(value) => {
                                    let _ =
                                        item_tx.send(PeerResponseItem::Data(Ok(value), chunk_meta));
                                }
                                Err(e) => {
                                    let _ = item_tx.send(PeerResponseItem::Data(
                                        Err(StreamError::Decode(e.to_string())),
                                        None,
                                    ));
                                }
                            }
                        }
                    }
                }
                FrameType::Log => {
                    let _ = item_tx.send(PeerResponseItem::Log(frame));
                }
                FrameType::StreamEnd | FrameType::End => {
                    if let Some(seq) = seq.take() {
                        if !seq.buf.is_empty() {
                            let _ = item_tx.send(PeerResponseItem::Data(
                                Err(StreamError::Decode(format!(
                                    "sequence stream ended mid-item: {} trailing bytes \
                                     do not form a complete CBOR item",
                                    seq.buf.len()
                                ))),
                                None,
                            ));
                        }
                    }
                    break;
                }
                FrameType::Err => {
                    let (code, class, message, arg_urn) = match remote_error_fields(&frame) {
                        Ok(fields) => fields,
                        Err(message) => {
                            let _ = item_tx.send(PeerResponseItem::Data(
                                Err(StreamError::Protocol(message)),
                                None,
                            ));
                            break;
                        }
                    };
                    let _ = item_tx.send(PeerResponseItem::Data(
                        Err(StreamError::RemoteError {
                            code,
                            class,
                            message,
                            arg_urn,
                        }),
                        None,
                    ));
                    break;
                }
                _ => {}
            }
        }
    });

    PeerResponse {
        rx: item_rx,
        grants,
    }
}

// =============================================================================
// ACTIVE REQUEST TRACKING
// =============================================================================

/// Tracks an active incoming request. Reader loop routes frames here.
struct ActiveRequest {
    raw_tx: crossbeam_channel::Sender<Frame>,
}

/// A queued incoming request waiting for a handler slot.
/// The crossbeam sender is in `active_requests` for frame routing.
/// The receiver is held here until the handler is spawned.
struct QueuedRequest {
    factory: OpFactory,
    cap_urn: String,
    /// The registered handler pattern (canonical) serving this request —
    /// the singleton pool it queues on and the key of its pool chain.
    pattern: String,
    /// Global arrival ticket: cross-cap admission is FIFO by this.
    ticket: u64,
    routing_id: Option<MessageId>,
    request_id: MessageId,
    raw_rx: crossbeam_channel::Receiver<Frame>,
}

/// The runtime's materialized concurrency pools (see `bifaci::pools`): one
/// singleton pool per registered handler pattern, every declared shared
/// pool from the manifest, and `all`. One mutex guards capacities, active
/// counts and the singleton queues, so an admission decision is atomic
/// across a cap's whole chain — a request is admitted through EVERY pool in
/// its chain or queued on its cap's own queue, never half-admitted.
pub(crate) struct RuntimePools {
    pools: std::collections::BTreeMap<String, RuntimePool>,
    /// Registered handler pattern (canonical) → its pool chain in admission
    /// order: singleton, declared pools containing it, `all`.
    chains: HashMap<String, Vec<String>>,
    /// Singleton queues — queues lead to pools. Keyed by registered pattern.
    queues: std::collections::BTreeMap<String, std::collections::VecDeque<QueuedRequest>>,
    /// Global FIFO ticket counter: cross-cap admission on a shared-pool
    /// release is arrival-ordered, never cap-biased.
    next_ticket: u64,
}

#[derive(Debug)]
struct RuntimePool {
    declared: u64,
    configured: u64,
    /// Cartridge self-report; `None` = static (the normal case). Written
    /// only through [`PoolHandle::set`].
    available: Option<u64>,
    active: u64,
    /// Member patterns (shared pools and `all`); singletons empty.
    members: Vec<String>,
}

impl RuntimePool {
    fn effective(&self) -> u64 {
        crate::bifaci::pools::effective_capacity(self.configured, self.available)
    }

    fn has_room(&self) -> bool {
        let effective = self.effective();
        effective == crate::bifaci::pools::CAPACITY_UNLIMITED || self.active < effective
    }
}

impl std::fmt::Debug for RuntimePools {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // QueuedRequest carries an opaque factory; summarize queues by depth.
        let queue_depths: std::collections::BTreeMap<&String, usize> =
            self.queues.iter().map(|(k, q)| (k, q.len())).collect();
        f.debug_struct("RuntimePools")
            .field("pools", &self.pools)
            .field("chains", &self.chains)
            .field("queue_depths", &queue_depths)
            .field("next_ticket", &self.next_ticket)
            .finish()
    }
}

impl RuntimePools {
    /// Materialize the pools from the registered handler patterns and the
    /// manifest's declarations. Hard errors, never coercion: an invalid
    /// registered pattern, or a declaration cap that matches no registered
    /// pattern, is a cartridge-author bug named precisely.
    fn init(
        handler_patterns: &[String],
        declarations: &crate::bifaci::pools::PoolDeclarations,
    ) -> Result<Self, String> {
        let mut patterns = Vec::with_capacity(handler_patterns.len());
        for raw in handler_patterns {
            let urn = CapUrn::from_string(raw)
                .map_err(|e| format!("registered handler pattern '{raw}' is not a valid cap URN: {e}"))?;
            let canon = urn.to_string();
            if patterns.contains(&canon) {
                return Err(format!("handler pattern '{canon}' is registered twice"));
            }
            patterns.push(canon);
        }
        let resolve = |declared_cap: &str| -> Result<String, String> {
            let urn = CapUrn::from_string(declared_cap)
                .map_err(|e| format!("pool declaration cap '{declared_cap}' is not a valid cap URN: {e}"))?;
            let canon = urn.to_string();
            if !patterns.iter().any(|p| p == &canon) {
                return Err(format!(
                    "pool declaration references cap '{canon}' but no handler is registered \
                     under it — pool declarations bind to the caps the runtime actually serves"
                ));
            }
            Ok(canon)
        };

        let mut pools = std::collections::BTreeMap::new();
        let mut queues = std::collections::BTreeMap::new();
        for pattern in &patterns {
            let declared = declarations
                .capacities
                .get(pattern)
                .copied()
                .unwrap_or(crate::bifaci::pools::CAPACITY_UNLIMITED);
            pools.insert(
                pattern.clone(),
                RuntimePool { declared, configured: declared, available: None, active: 0, members: Vec::new() },
            );
            queues.insert(pattern.clone(), std::collections::VecDeque::new());
        }
        for (name, members) in &declarations.pools {
            let mut resolved = Vec::with_capacity(members.len());
            for member in members {
                resolved.push(resolve(member)?);
            }
            let declared = declarations
                .capacities
                .get(name)
                .copied()
                .unwrap_or(crate::bifaci::pools::CAPACITY_UNLIMITED);
            pools.insert(
                name.clone(),
                RuntimePool { declared, configured: declared, available: None, active: 0, members: resolved },
            );
        }
        for key in declarations.capacities.keys() {
            if key != crate::bifaci::pools::POOL_ALL
                && !pools.contains_key(key)
                && !CapUrn::from_string(key).map(|u| patterns.contains(&u.to_string())).unwrap_or(false)
            {
                return Err(format!(
                    "declared capacity for '{key}' names neither a registered cap, a declared \
                     pool, nor '{}'",
                    crate::bifaci::pools::POOL_ALL
                ));
            }
        }
        let all_declared = declarations
            .capacities
            .get(crate::bifaci::pools::POOL_ALL)
            .copied()
            .unwrap_or(crate::bifaci::pools::CAPACITY_UNLIMITED);
        pools.insert(
            crate::bifaci::pools::POOL_ALL.to_string(),
            RuntimePool {
                declared: all_declared,
                configured: all_declared,
                available: None,
                active: 0,
                members: patterns.clone(),
            },
        );

        let mut chains = HashMap::new();
        for pattern in &patterns {
            let mut chain = vec![pattern.clone()];
            for (name, pool) in &pools {
                if name != crate::bifaci::pools::POOL_ALL
                    && name != pattern
                    && pool.members.iter().any(|m| m == pattern)
                {
                    chain.push(name.clone());
                }
            }
            chain.push(crate::bifaci::pools::POOL_ALL.to_string());
            chains.insert(pattern.clone(), chain);
        }

        Ok(Self { pools, chains, queues, next_ticket: 0 })
    }

    fn chain(&self, pattern: &str) -> &[String] {
        self.chains
            .get(pattern)
            .unwrap_or_else(|| panic!("no pool chain for registered pattern '{pattern}'"))
    }

    fn chain_has_room(&self, pattern: &str) -> bool {
        self.chain(pattern)
            .iter()
            .all(|pool| self.pools[pool].has_room())
    }

    /// Admit one dispatch of `pattern` if its whole chain has room.
    fn try_admit(&mut self, pattern: &str) -> bool {
        if !self.chain_has_room(pattern) {
            return false;
        }
        for pool in self.chain(pattern).to_vec() {
            self.pools.get_mut(&pool).expect("chain pool exists").active += 1;
        }
        true
    }

    /// Release one dispatch of `pattern` across its chain.
    fn release(&mut self, pattern: &str) {
        for pool in self.chain(pattern).to_vec() {
            let slot = self.pools.get_mut(&pool).expect("chain pool exists");
            slot.active = slot
                .active
                .checked_sub(1)
                .unwrap_or_else(|| panic!("pool '{pool}' released below zero active"));
        }
    }

    /// Queue a request on its cap's singleton queue, returning its queue
    /// position (1-based) for the "queued" LOG.
    fn enqueue(&mut self, mut request: QueuedRequest) -> usize {
        request.ticket = self.next_ticket;
        self.next_ticket += 1;
        let queue = self
            .queues
            .get_mut(&request.pattern)
            .unwrap_or_else(|| panic!("no singleton queue for pattern '{}'", request.pattern));
        queue.push_back(request);
        queue.len()
    }

    /// Remove a queued (not-yet-admitted) request by its request id — the
    /// Cancel path. A queued request holds no chain slots, so nothing is
    /// released. Returns the removed request, or `None` when the id is not
    /// queued (it is running, or unknown).
    fn remove_queued(&mut self, request_id: &MessageId) -> Option<QueuedRequest> {
        for queue in self.queues.values_mut() {
            if let Some(pos) = queue.iter().position(|q| &q.request_id == request_id) {
                return queue.remove(pos);
            }
        }
        None
    }

    /// Pop-and-admit the oldest queued request whose chain has room —
    /// arrival-ordered across all caps by the global ticket.
    fn pop_admissible(&mut self) -> Option<QueuedRequest> {
        let mut best: Option<(u64, String)> = None;
        for (pattern, queue) in &self.queues {
            if let Some(front) = queue.front() {
                if self.chain_has_room(pattern)
                    && best.as_ref().map_or(true, |(ticket, _)| front.ticket < *ticket)
                {
                    best = Some((front.ticket, pattern.clone()));
                }
            }
        }
        let (_, pattern) = best?;
        let request = self
            .queues
            .get_mut(&pattern)
            .expect("queue exists")
            .pop_front()
            .expect("front observed above");
        for pool in self.chain(&pattern).to_vec() {
            self.pools.get_mut(&pool).expect("chain pool exists").active += 1;
        }
        Some(request)
    }

    /// Apply an operator's desired `configured` values (heartbeat probe).
    /// The whole batch is validated first — an unknown pool refuses it all.
    fn apply_desired(
        &mut self,
        desired: &crate::bifaci::pools::DesiredCapacities,
    ) -> Result<(), String> {
        for name in desired.keys() {
            if !self.pools.contains_key(name) {
                return Err(format!("unknown pool '{name}'"));
            }
        }
        for (name, configured) in desired {
            self.pools.get_mut(name).expect("validated above").configured = *configured;
        }
        Ok(())
    }

    /// Cartridge self-report for one pool (see [`PoolHandle`]).
    fn set_available(&mut self, pool: &str, available: u64) -> Result<(), String> {
        let slot = self
            .pools
            .get_mut(pool)
            .ok_or_else(|| format!("unknown pool '{pool}'"))?;
        slot.available = Some(available);
        Ok(())
    }

    /// The full wire-shaped state map. `queued` counts each waiting request
    /// on its own singleton pool and on every chain pool that currently
    /// lacks room (its blockers) — so a shared pool's queued figure is the
    /// number of waiters it is actually holding back.
    fn snapshot(&self) -> crate::bifaci::pools::PoolStates {
        let mut states = crate::bifaci::pools::PoolStates::new();
        for (name, pool) in &self.pools {
            states.insert(
                name.clone(),
                crate::bifaci::pools::PoolState {
                    declared: pool.declared,
                    configured: pool.configured,
                    available: pool.available,
                    active: pool.active,
                    queued: 0,
                    caps: pool.members.clone(),
                },
            );
        }
        for (pattern, queue) in &self.queues {
            let waiting = queue.len() as u64;
            if waiting == 0 {
                continue;
            }
            states.get_mut(pattern).expect("singleton state exists").queued += waiting;
            for pool in self.chain(pattern) {
                if pool != pattern && !self.pools[pool].has_room() {
                    states.get_mut(pool).expect("chain state exists").queued += waiting;
                }
            }
        }
        states
    }
}

/// Shared handle for a pool's cartridge SELF-REPORT (`available` — see
/// `bifaci::pools`). Obtained from [`CartridgeRuntime::pool_handle`] with a
/// pool name (a registered cap URN for a single cap, a declared pool name,
/// or `all`). `set(n)` reports what the cartridge can serve right now from
/// its OWN state (0 = unlimited); it never touches the operator's
/// `configured` or the manifest's `declared`. A cartridge that never calls
/// it is fully static — the normal case.
#[derive(Clone)]
pub struct PoolHandle {
    pools: Arc<Mutex<Option<RuntimePools>>>,
    name: String,
}

impl PoolHandle {
    /// Report the pool's current self-limit. Errors name the defect: an
    /// unknown pool name, or a call before the runtime materialized its
    /// pools (`run()` not started).
    pub fn set(&self, available: u64) -> Result<(), String> {
        let mut pools = self.pools.lock().expect("runtime pools mutex poisoned");
        pools
            .as_mut()
            .ok_or_else(|| "runtime pools are not materialized yet (before run())".to_string())?
            .set_available(&self.name, available)
    }
}

/// The cartridge runtime that handles all I/O for cartridge binaries.
///
/// Cartridges create a runtime with their manifest, register handlers for their caps,
/// then call `run()` to process requests.
///
/// The manifest is REQUIRED - cartridges MUST provide their manifest which is sent
/// in the HELLO response during handshake. This is the ONLY way for cartridges to
/// communicate their capabilities to the host.
///
/// **Invocation Modes**:
/// - No CLI args: Cartridge CBOR mode (stdin/stdout binary frames)
/// - Any CLI args: CLI mode (parse args from cap definitions)
///
/// **Multiplexed execution** (CBOR mode): Multiple requests can be processed concurrently.
/// Each request handler runs in its own thread, allowing the runtime to:
/// - Respond to heartbeats while handlers are running
/// - Accept new requests while previous ones are still processing
/// - Handle multiple concurrent cap invocations
///
/// **Concurrency pools**: capacity is per POOL (see `bifaci::pools`) — one
/// queue per registered cap, shared pools declared in the manifest, `all`
/// over everything. A request beyond a pool's effective capacity queues on
/// its cap's own queue; the runtime sends LOG frames with `level="queued"`
/// so the pipeline knows the request is alive but waiting, and admits the
/// oldest eligible waiter (global FIFO across caps) when any chain pool
/// frees. Nothing declared = every pool unlimited.
pub struct CartridgeRuntime {
    /// Registered Op factories by cap URN pattern
    handlers: HashMap<String, OpFactory>,

    /// Cartridge manifest JSON data - sent in HELLO response.
    /// This is REQUIRED - cartridges must provide their manifest.
    manifest_data: Vec<u8>,

    /// Parsed manifest for CLI mode processing
    manifest: Option<CapManifest>,

    /// Negotiated protocol limits
    limits: Limits,

    /// The materialized concurrency pools (singleton per registered cap,
    /// declared shared pools, `all`). `None` until `run()` materializes
    /// them from the handler set + the manifest's declarations; shared via
    /// [`PoolHandle`] so handlers can self-report `available` dynamically.
    pools: Arc<Mutex<Option<RuntimePools>>>,

    /// Process-wide dropped-frame accounting (L8). Shared with every
    /// ChannelFrameSender and the stats surface. Drops mean something went
    /// wrong.
    drop_counters: Arc<crate::bifaci::stats::DropCounters>,

    /// Benign post-terminal stragglers suppressed by the writer's terminal
    /// gate (L4): late keepalive/emitter frames that crossed their flow's
    /// END/ERR. Counted per frame type, indicated as benign — never drops.
    straggler_counters: Arc<crate::bifaci::stats::StragglerCounters>,

    /// Runtime-wide live-feed overrun aggregate (12.5 §Overrun): real-time
    /// items discarded at capture edges because consumers lagged. Rides
    /// heartbeat meta as `overruns_total` — never counted as drops. The
    /// capture backends themselves are the built-in compile-time dispatch
    /// in `crate::capture` — capture is transport resolution, not a plugin
    /// surface, and there is nothing to register.
    live_feed_overruns: Arc<std::sync::atomic::AtomicU64>,
}

/// Dispatch an Op with a Request via WetContext.
/// Closes the output stream on success (sends STREAM_END if stream was started).
async fn dispatch_op(
    op: Box<dyn Op<()>>,
    input: InputPackage,
    output: OutputStream,
    peer: Arc<dyn PeerInvoker>,
) -> Result<(), RuntimeError> {
    let req = Arc::new(Request::new(input, output, peer));
    let mut dry = DryContext::new();
    let mut wet = WetContext::new();
    wet.insert_arc(WET_KEY_REQUEST, req.clone());

    let result = op.perform(&mut dry, &mut wet).await.map_err(|e| {
        // Classified failures keep their declared identity across the
        // Op→Runtime boundary (docs/failure-taxonomy.md); everything else
        // is the handler's own problem — Internal via Handler.
        match e.failure_code() {
            Some(code) => RuntimeError::Classified {
                code: code.to_string(),
                class: e.attribution_class(),
                message: e.failure_reason(),
                arg_urn: e.failure_arg_urn().map(str::to_string),
            },
            None => RuntimeError::Handler(e.to_string()),
        }
    });

    if result.is_ok() {
        let _ = req.output().close().await;
    }
    result
}

/// Outcome of pushing one frame through the terminal gate + writer.
pub(crate) enum GatedWrite {
    Written,
    /// A flow frame arrived at the writer after its flow's END/ERR was
    /// written — the benign detached-sender race (a keepalive tick or late
    /// emitter crossing the terminal). Suppressed and counted as a
    /// straggler, indicated as benign: nothing went wrong.
    SuppressedStraggler,
    WriterDead,
}

/// Write one frame through the terminal gate (L4). Once a flow's END/ERR has
/// been written, any later flow frame for the same FlowKey is a benign
/// post-terminal straggler: it is suppressed and counted as such (never a
/// drop — nothing went wrong), never written. The writer thread is the
/// single point where wire order is decided, so gating here
/// deterministically closes every detached-sender race (ProgressSender,
/// keepalive tickers).
pub(crate) fn write_gated<W: std::io::Write>(
    mut frame: Frame,
    writer: &mut W,
    limits: &Limits,
    seq_assigner: &mut SeqAssigner,
    terminated: &mut crate::bifaci::stats::TerminatedFlows,
    stragglers: &crate::bifaci::stats::StragglerCounters,
) -> GatedWrite {
    let key = FlowKey::from_frame(&frame);
    if frame.is_flow_frame() && terminated.contains(&key) {
        let total = stragglers.record(frame.frame_type);
        tracing::debug!(
            target: "cartridge_runtime",
            rid = ?frame.id,
            ftype = frame.frame_type.as_str(),
            straggler_total = total,
            "[CartridgeRuntime] writer: suppressed benign post-terminal straggler — \
             END/ERR already written for this flow, the frame is moot (L4)"
        );
        return GatedWrite::SuppressedStraggler;
    }
    seq_assigner.assign(&mut frame);
    let ftype = frame.frame_type;
    if let Err(e) = crate::bifaci::io::write_frame_sync(writer, &frame, limits) {
        tracing::error!(
            target: "cartridge_runtime",
            error = %e,
            ftype = ?ftype,
            "[CartridgeRuntime] writer thread: write_frame_sync failed — exiting writer loop. Cartridge → host frames after this point will be lost."
        );
        return GatedWrite::WriterDead;
    }
    if matches!(ftype, FrameType::End | FrameType::Err) {
        seq_assigner.remove(&key);
        terminated.insert(key);
    }
    GatedWrite::Written
}

/// Spawn a handler task for an incoming request.
///
/// The media URN a cap's response STREAM_START must carry, derived from the
/// cap's declared effect over its declared main input — the label every
/// engine-fed input stream carries (spec 13.2):
///
/// - `effect=declared` → the declared `out=`
/// - `effect=none`     → the declared `in=` (the type passes through)
/// - `effect=patch`    → the declared `in=` with the declared delta applied
///
/// This is `CapUrn::apply_to_runtime_input_media` over the declared input —
/// the SAME inference the engine's effect audit checks emissions against
/// (`CapUrn::is_conformant_runtime_output`), so a runtime-labeled response
/// is conformant by construction. Every runtime that labels a response must
/// go through this function; a hand-picked label is how a cap lies about
/// its effect.
pub fn derive_response_media(cap_urn: &str) -> Result<String, RuntimeError> {
    let cap = crate::CapUrn::from_string(cap_urn).map_err(|e| {
        RuntimeError::Handler(format!(
            "response media derivation: cap URN '{}' does not parse: {}",
            cap_urn, e
        ))
    })?;
    let declared_in = cap.in_media_urn().map_err(|e| {
        RuntimeError::Handler(format!(
            "response media derivation: cap '{}' declared input is not a valid media URN: {}",
            cap_urn, e
        ))
    })?;
    cap.apply_to_runtime_input_media(&declared_in)
        .map(|m| m.to_string())
        .map_err(|e| {
            RuntimeError::Handler(format!(
                "response media derivation: cap '{}' effect could not be applied to its declared input: {}",
                cap_urn, e
            ))
        })
}

/// The crossbeam receiver carries frames routed by the main loop's active_requests
/// map. The handler's demux drains them (even if they arrived before this spawn).
fn spawn_handler(
    raw_rx: crossbeam_channel::Receiver<Frame>,
    factory: OpFactory,
    cap_urn: String,
    request_id: MessageId,
    routing_id: Option<MessageId>,
    output_tx: &tokio::sync::mpsc::UnboundedSender<Frame>,
    pending_peer_requests: &Arc<Mutex<HashMap<MessageId, PendingPeerRequest>>>,
    manifest: &Option<CapManifest>,
    max_chunk: usize,
    handler_done_tx: &tokio::sync::mpsc::UnboundedSender<MessageId>,
    drops: &Arc<crate::bifaci::stats::DropCounters>,
    credit_router: &crate::bifaci::credit::CreditRouter,
    initial_credit: u64,
    live_feed_overruns: &Arc<std::sync::atomic::AtomicU64>,
    live_feed_handles: &Arc<Mutex<Vec<crate::bifaci::live_feed::LiveFeedHandle>>>,
) -> JoinHandle<()> {
    let output_tx_clone = output_tx.clone();
    let pending_clone = Arc::clone(pending_peer_requests);
    let manifest_clone = manifest.clone();
    let done_tx = handler_done_tx.clone();
    let drops = Arc::clone(drops);
    let credit_router = credit_router.clone();
    let live_feed_overruns = Arc::clone(live_feed_overruns);
    let live_feed_handles = Arc::clone(live_feed_handles);

    tokio::spawn(async move {
        let fp_ctx = FilePathContext::new(&cap_urn, manifest_clone.clone()).ok();
        let lf_ctx = LiveFeedContext::new(
            &cap_urn,
            manifest_clone,
            live_feed_overruns,
            live_feed_handles,
        )
        .ok();
        let sender: Arc<dyn FrameSender> = Arc::new(ChannelFrameSender {
            tx: output_tx_clone.clone(),
            drops: Arc::clone(&drops),
        });
        // Input streams are credited (L14): the handler's consumption grants
        // the engine's sender window; over-window chunks are CREDIT_VIOLATION.
        let input_package = demux_multi_stream(
            raw_rx,
            fp_ctx,
            lf_ctx,
            Some(InputCreditContext {
                sender: Arc::clone(&sender),
                rid: request_id.clone(),
                xid: routing_id.clone(),
                initial_credit,
            }),
        );
        let stream_id = uuid::Uuid::new_v4().to_string();
        // The response label is DERIVED from the cap's declared effect — not
        // chosen by the op — so an honest lib-runtime cartridge satisfies the
        // engine's effect audit by construction. An underivable label is a
        // broken cap declaration: fail the request, never fall back.
        let out_media = match derive_response_media(&cap_urn) {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(
                    "[CartridgeRuntime] response media derivation FAILED: cap='{}' rid={:?} error={}",
                    cap_urn,
                    request_id,
                    e
                );
                let mut err_frame = Frame::err(
                    request_id.clone(),
                    e.failure_code().unwrap_or("HANDLER_ERROR"),
                    e.attribution_class(),
                    &e.failure_reason(),
                    e.failure_arg_urn(),
                );
                err_frame.routing_id = routing_id;
                let _ = sender.send(&err_frame);
                let _ = done_tx.send(request_id);
                return;
            }
        };
        let output = OutputStream::new(
            Arc::clone(&sender),
            stream_id,
            out_media,
            request_id.clone(),
            routing_id.clone(),
            max_chunk,
        )
        .with_credit(initial_credit, credit_router.clone());
        let final_status = output.final_status_handle();

        let peer_invoker = PeerInvokerImpl {
            output_tx: output_tx_clone.clone(),
            pending_requests: Arc::clone(&pending_clone),
            max_chunk,
            origin_request_id: request_id.clone(),
            origin_routing_id: routing_id.clone(),
            drops: Arc::clone(&drops),
            credit_router: credit_router.clone(),
            initial_credit,
        };

        let op = factory();
        let peer_arc: Arc<dyn PeerInvoker> = Arc::new(peer_invoker);
        let result = dispatch_op(op, input_package, output, peer_arc).await;

        match result {
            Ok(()) => {
                // The END frame carries the terminal metadata (L3/L5): the
                // handler's declared final status, or the 1.0 default. Final
                // progress rides IN the terminal frame — it cannot race it.
                let declared = final_status
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
                let (progress, message) = match &declared {
                    Some(fs) => (fs.progress, fs.message.as_deref()),
                    None => (1.0, None),
                };
                let mut end_frame =
                    Frame::end_ok_with(request_id.clone(), None, Some(progress), message);
                end_frame.routing_id = routing_id;
                let _ = sender.send(&end_frame);
            }
            Err(e) => {
                tracing::error!(
                    "[CartridgeRuntime] handler FAILED: cap='{}' rid={:?} error={}",
                    cap_urn,
                    request_id,
                    e
                );
                // The ERR frame carries the failure's DECLARED identity
                // (docs/failure-taxonomy.md): the code, class, and argument
                // attribution from the emit source when classified,
                // HANDLER_ERROR/Internal/unattributed when the handler never
                // declared one.
                let mut err_frame = Frame::err(
                    request_id.clone(),
                    e.failure_code().unwrap_or("HANDLER_ERROR"),
                    e.attribution_class(),
                    &e.failure_reason(),
                    e.failure_arg_urn(),
                );
                err_frame.routing_id = routing_id;
                let _ = sender.send(&err_frame);
            }
        }
        // Notify the main loop which handler finished so it can
        // check cancelled state and send deferred ERR if needed.
        let _ = done_tx.send(request_id);
    })
}

impl CartridgeRuntime {
    /// Create a new cartridge runtime with the required manifest.
    ///
    /// The manifest is JSON-encoded cartridge metadata including:
    /// - name: Cartridge name
    /// - version: Cartridge version
    /// - caps: Array of capability definitions with args and sources
    ///
    /// This manifest is sent in the HELLO response to the host (CBOR mode)
    /// and used for CLI argument parsing (CLI mode).
    /// **Cartridges MUST provide a manifest - there is no fallback.**
    ///
    /// Auto-registers standard handlers (identity, discard).
    /// **PANICS** if manifest is missing CAP_IDENTITY - cartridges must declare it explicitly.
    pub fn new(manifest: &[u8]) -> Self {
        // Try to parse the manifest for CLI mode support
        let parsed_manifest = serde_json::from_slice::<CapManifest>(manifest).ok();

        // Validate manifest if parseable
        let (manifest_data, parsed_manifest) = match parsed_manifest {
            Some(m) => {
                // FAIL HARD if manifest doesn't have CAP_IDENTITY
                m.validate()
                    .expect("Manifest validation failed - cartridge MUST declare CAP_IDENTITY");
                let data = serde_json::to_vec(&m).unwrap_or_else(|_| manifest.to_vec());
                (data, Some(m))
            }
            None => (manifest.to_vec(), None),
        };

        let mut rt = Self {
            handlers: HashMap::new(),
            manifest_data,
            manifest: parsed_manifest,
            limits: Limits::default(),
            pools: Arc::new(Mutex::new(None)),
            drop_counters: Arc::new(crate::bifaci::stats::DropCounters::new()),
            straggler_counters: Arc::new(crate::bifaci::stats::StragglerCounters::new()),
            live_feed_overruns: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        };
        rt.register_standard_caps();
        rt
    }

    /// Create a new cartridge runtime with a pre-built CapManifest.
    /// This is the preferred method as it ensures the manifest is valid.
    ///
    /// Auto-registers standard handlers (identity, discard).
    /// **PANICS** if manifest is missing CAP_IDENTITY - cartridges must declare it explicitly.
    pub fn with_manifest(manifest: CapManifest) -> Self {
        // FAIL HARD if manifest doesn't have CAP_IDENTITY
        manifest
            .validate()
            .expect("Manifest validation failed - cartridge MUST declare CAP_IDENTITY");

        let manifest_data = serde_json::to_vec(&manifest).unwrap_or_default();
        let mut rt = Self {
            handlers: HashMap::new(),
            manifest_data,
            manifest: Some(manifest),
            limits: Limits::default(),
            pools: Arc::new(Mutex::new(None)),
            drop_counters: Arc::new(crate::bifaci::stats::DropCounters::new()),
            straggler_counters: Arc::new(crate::bifaci::stats::StragglerCounters::new()),
            live_feed_overruns: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        };
        rt.register_standard_caps();
        rt
    }

    /// Create a new cartridge runtime with manifest JSON string.
    ///
    /// Auto-registers standard handlers (identity, discard) and ensures
    /// CAP_IDENTITY is present in the manifest.
    pub fn with_manifest_json(manifest_json: &str) -> Self {
        Self::new(manifest_json.as_bytes())
    }

    /// Protocol observability snapshot (L8): this runtime's dropped-frame
    /// counters (closed-channel sends and other genuine losses).
    pub fn protocol_drops(&self) -> crate::bifaci::stats::DropSnapshot {
        self.drop_counters.snapshot()
    }

    /// Benign post-terminal straggler snapshot (L4): late frames the writer's
    /// terminal gate suppressed, per frame type. Separate from drops —
    /// nothing went wrong.
    pub fn protocol_stragglers(&self) -> crate::bifaci::stats::StragglerSnapshot {
        self.straggler_counters.snapshot()
    }

    /// Runtime-wide overrun total (12.5 §Overrun): real-time items
    /// discarded at capture edges because consumers lagged. Rides
    /// heartbeat meta as `overruns_total` — never counted as drops.
    pub fn protocol_overruns_total(&self) -> u64 {
        self.live_feed_overruns
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Register the standard identity and discard handlers.
    /// Cartridge authors can override either by calling register_op() after construction.
    fn register_standard_caps(&mut self) {
        if self.find_handler(CAP_IDENTITY).is_none() {
            self.register_op_type::<IdentityOp>(CAP_IDENTITY);
        }
        if self.find_handler(CAP_DISCARD).is_none() {
            self.register_op_type::<DiscardOp>(CAP_DISCARD);
        }
        if self.find_handler(CAP_ADAPTER_SELECTION).is_none() {
            self.register_op_type::<AdapterSelectionOp>(CAP_ADAPTER_SELECTION);
        }
    }

    /// Set the maximum number of concurrent handler invocations.
    ///
    /// When set to N > 0, the runtime queues incoming requests beyond N active
    /// handlers. Queued requests receive a LOG frame with `level="queued"` so the
    /// pipeline's activity timeout pauses for that body.
    ///

    /// A handle for one pool's cartridge SELF-REPORT (`available`). The
    /// name is a pool name: a registered cap URN, a declared pool name, or
    /// `all`. Validated at `set` time — an unknown name errors there,
    /// naming it.
    pub fn pool_handle(&self, pool: &str) -> PoolHandle {
        PoolHandle {
            pools: Arc::clone(&self.pools),
            name: pool.to_string(),
        }
    }

    /// Register an Op factory for a cap URN.
    /// The factory creates a fresh Op<()> instance per invocation.
    pub fn register_op<F>(&mut self, cap_urn: &str, factory: F)
    where
        F: Fn() -> Box<dyn Op<()>> + Send + Sync + 'static,
    {
        self.handlers.insert(cap_urn.to_string(), Arc::new(factory));
    }

    /// Register an Op type for a cap URN. The type must implement Op<()> + Default.
    /// Creates instances via Default::default() on each invocation.
    pub fn register_op_type<T: Op<()> + Default + 'static>(&mut self, cap_urn: &str) {
        self.handlers.insert(
            cap_urn.to_string(),
            Arc::new(|| Box::new(T::default()) as Box<dyn Op<()>>),
        );
    }

    /// Find a handler for a cap URN.
    /// Returns the OpFactory if found, None otherwise.
    ///
    /// Uses `is_dispatchable(candidate, request)` to find handlers that can
    /// legally handle the request, then ranks by specificity.
    ///
    /// Ranking prefers:
    /// 1. Equivalent matches (distance 0)
    /// 2. More specific candidates (positive distance) - refinements
    /// 3. More generic candidates (negative distance) - fallbacks
    pub fn find_handler(&self, cap_urn: &str) -> Option<OpFactory> {
        self.find_handler_with_pattern(cap_urn)
            .map(|(_, handler)| handler)
    }

    /// Find a handler AND the canonical registered pattern that won the
    /// dispatch — the pattern is the request's pool identity (its singleton
    /// queue and the key of its pool chain).
    pub fn find_handler_with_pattern(&self, cap_urn: &str) -> Option<(String, OpFactory)> {
        let request_urn = match CapUrn::from_string(cap_urn) {
            Ok(u) => u,
            Err(_) => return None,
        };

        let request_specificity = request_urn.specificity();
        // (canonical pattern, handler, signed_distance)
        let mut best: Option<(String, OpFactory, isize)> = None;

        for (registered_cap_str, handler) in &self.handlers {
            if let Ok(registered_urn) = CapUrn::from_string(registered_cap_str) {
                // Use is_dispatchable: can this candidate handle this request?
                if registered_urn.is_dispatchable(&request_urn) {
                    let specificity = registered_urn.specificity();
                    let signed_distance = specificity as isize - request_specificity as isize;

                    let dominated = match &best {
                        None => false,
                        Some((_, _, best_dist)) => {
                            // Current best dominates if:
                            // - best is non-negative and candidate is negative
                            // - OR both same sign and best has smaller abs distance
                            match (best_dist >= &0, signed_distance >= 0) {
                                (true, false) => true,  // best is refinement, candidate is fallback
                                (false, true) => false, // candidate is refinement, best is fallback
                                _ => best_dist.unsigned_abs() <= signed_distance.unsigned_abs(),
                            }
                        }
                    };

                    if !dominated {
                        best = Some((
                            registered_urn.to_string(),
                            Arc::clone(handler),
                            signed_distance,
                        ));
                    }
                }
            }
        }

        best.map(|(pattern, handler, _)| (pattern, handler))
    }

    /// Run the cartridge runtime.
    ///
    /// **Mode Detection**:
    /// - No CLI arguments: Cartridge CBOR mode (stdin/stdout binary frames)
    /// - Any CLI arguments: CLI mode (parse args from cap definitions)
    ///
    /// **CLI Mode**:
    /// - `manifest` subcommand: output manifest JSON
    /// - `<op>` subcommand: find cap by op tag, parse args, invoke handler
    /// - `--help`: show available subcommands
    ///
    /// **Cartridge CBOR Mode** (no CLI args):
    /// 1. Receive HELLO from host
    /// 2. Send HELLO back with manifest (handshake)
    /// 3. Main loop reads frames:
    ///    - REQ frames: spawn handler thread, continue reading
    ///    - HEARTBEAT frames: respond immediately
    ///    - RES/CHUNK/END frames: route to pending peer requests
    ///    - Other frames: ignore
    /// 4. Exit when stdin closes, wait for active handlers to complete
    ///
    /// **Multiplexing** (CBOR mode): The main loop never blocks on handler execution.
    /// Handlers run in separate threads, allowing concurrent processing
    /// of multiple requests and immediate heartbeat responses.
    ///
    /// **Bidirectional communication** (CBOR mode): Handlers can invoke caps on the host
    /// using the `PeerInvoker` parameter. Response frames from the host are
    /// routed to the appropriate pending request by MessageId.
    pub async fn run(&self) -> Result<(), RuntimeError> {
        let args: Vec<String> = std::env::args().collect();

        // No CLI arguments at all → Cartridge CBOR mode
        if args.len() == 1 {
            return self.run_cbor_mode().await;
        }

        // Any CLI arguments → CLI mode
        self.run_cli_mode(&args).await
    }

    /// Run in CLI mode - parse arguments and invoke handler.
    ///
    /// If stdin is piped (binary data), this streams it in chunks and accumulates.
    /// All modes converge: CLI args and stdin data are sent as CBOR frame streams
    /// through InputPackage, so handlers see the same API regardless of mode.
    async fn run_cli_mode(&self, args: &[String]) -> Result<(), RuntimeError> {
        let manifest = self.manifest.as_ref().ok_or_else(|| {
            RuntimeError::Manifest("Failed to parse manifest for CLI mode".to_string())
        })?;

        // Handle --help at top level
        if args.len() == 2 && (args[1] == "--help" || args[1] == "-h") {
            self.print_help(manifest);
            return Ok(());
        }

        let subcommand = &args[1];

        // Handle manifest subcommand (always provided by runtime)
        if subcommand == "manifest" {
            let json = serde_json::to_string_pretty(manifest)
                .map_err(|e| RuntimeError::Serialize(e.to_string()))?;
            println!("{}", json);
            return Ok(());
        }

        // Handle subcommand --help
        if args.len() == 3 && (args[2] == "--help" || args[2] == "-h") {
            if let Some(cap) = self.find_cap_by_alias(manifest, subcommand) {
                self.print_cap_help(&cap);
                return Ok(());
            }
        }

        // Find cap by command name
        let cap = self
            .find_cap_by_alias(manifest, subcommand)
            .ok_or_else(|| {
                RuntimeError::UnknownSubcommand(format!(
                    "Unknown subcommand '{}'. Run with --help to see available commands.",
                    subcommand
                ))
            })?;

        // Find handler factory
        let factory = self.find_handler(&cap.urn_string()).ok_or_else(|| {
            RuntimeError::NoHandler(format!(
                "No handler registered for cap '{}'",
                cap.urn_string()
            ))
        })?;

        // Extract CLI arguments (everything after subcommand)
        let cli_args = &args[2..];

        // Check if stdin is piped (binary streaming mode)
        let stdin_is_piped = !atty::is(atty::Stream::Stdin);
        let cap_accepts_stdin = cap.accepts_stdin();

        // Priority: CLI args > stdin (args take precedence)
        if !cli_args.is_empty() {
            // ARGUMENT PATH: Build from CLI arguments (may include file paths
            // or globs). If any file-path arg is declared `is_sequence=false`
            // but its value expands to multiple files, the runtime iterates
            // the handler once per file — a single process, N invocations,
            // outputs concatenated to stdout in glob-expansion order.
            let raw_payload = self.build_payload_from_cli(&cap, cli_args)?;
            let iterations = build_cli_foreach_iterations(&raw_payload, &cap)?;
            for per_iter_payload in iterations {
                let payload = extract_effective_payload(
                    &per_iter_payload,
                    Some("application/cbor"),
                    &cap,
                    true, // CLI mode
                )?;
                self.dispatch_cli_payload(&cap, factory.clone(), payload)
                    .await?;
            }
            Ok(())
        } else if stdin_is_piped && cap_accepts_stdin {
            // STREAMING PATH: No args, read stdin in chunks and accumulate
            let payload = self.build_payload_from_streaming_stdin(&cap)?;
            self.dispatch_cli_payload(&cap, factory, payload).await
        } else {
            Err(RuntimeError::MissingArgument(
                "No input provided (expected CLI arguments or piped stdin)".to_string(),
            ))
        }
    }

    /// Dispatch one CLI-mode invocation: take the (already file-path-resolved)
    /// CBOR arguments payload, build input streams, set up a CLI-backed
    /// `OutputStream`, and run the handler to completion.
    async fn dispatch_cli_payload(
        &self,
        cap: &Cap,
        factory: OpFactory,
        payload: Vec<u8>,
    ) -> Result<(), RuntimeError> {
        let cli_emitter = CliStreamEmitter::without_ndjson();
        let frame_sender = CliFrameSender::with_emitter(cli_emitter);
        let peer = NoPeerInvoker;

        let cbor_value: ciborium::Value = ciborium::from_reader(&payload[..]).map_err(|e| {
            RuntimeError::Deserialize(format!("Failed to parse CBOR arguments: {}", e))
        })?;
        let arguments = match cbor_value {
            ciborium::Value::Array(arr) => arr,
            _ => {
                return Err(RuntimeError::Deserialize(
                    "CBOR arguments must be an array".to_string(),
                ))
            }
        };

        let (tx, rx) = crossbeam_channel::unbounded();
        let max_chunk = Limits::default().max_chunk;
        let request_id = MessageId::new_uuid();

        for arg in arguments {
            let ciborium::Value::Map(arg_map) = arg else {
                continue;
            };
            let mut media_urn: Option<String> = None;
            let mut value_bytes: Option<Vec<u8>> = None;
            for (k, v) in arg_map {
                if let ciborium::Value::Text(key) = k {
                    match key.as_str() {
                        "media_urn" => {
                            if let ciborium::Value::Text(s) = v {
                                media_urn = Some(s);
                            }
                        }
                        "value" => {
                            let mut cbor_bytes = Vec::new();
                            ciborium::into_writer(&v, &mut cbor_bytes).map_err(|e| {
                                RuntimeError::Serialize(format!("Failed to encode value: {}", e))
                            })?;
                            value_bytes = Some(cbor_bytes);
                        }
                        _ => {}
                    }
                }
            }

            let (Some(urn), Some(bytes)) = (media_urn, value_bytes) else {
                continue;
            };
            let stream_id = uuid::Uuid::new_v4().to_string();
            let start_frame =
                Frame::stream_start(request_id.clone(), stream_id.clone(), urn.clone(), None);
            tx.send(start_frame)
                .map_err(|_| RuntimeError::Handler("Failed to send STREAM_START".to_string()))?;

            let chunk_count = if bytes.is_empty() {
                let checksum = Frame::compute_checksum(&[]);
                let chunk_frame = Frame::chunk(
                    request_id.clone(),
                    stream_id.clone(),
                    0,
                    vec![],
                    0,
                    checksum,
                );
                tx.send(chunk_frame)
                    .map_err(|_| RuntimeError::Handler("Failed to send CHUNK".to_string()))?;
                1
            } else {
                let mut offset = 0;
                let mut chunk_index = 0u64;
                while offset < bytes.len() {
                    let chunk_size = (bytes.len() - offset).min(max_chunk);
                    let chunk_data = bytes[offset..offset + chunk_size].to_vec();
                    let checksum = Frame::compute_checksum(&chunk_data);
                    let chunk_frame = Frame::chunk(
                        request_id.clone(),
                        stream_id.clone(),
                        0,
                        chunk_data,
                        chunk_index,
                        checksum,
                    );
                    tx.send(chunk_frame)
                        .map_err(|_| RuntimeError::Handler("Failed to send CHUNK".to_string()))?;
                    offset += chunk_size;
                    chunk_index += 1;
                }
                chunk_index
            };

            let end_frame = Frame::stream_end(request_id.clone(), stream_id.clone(), chunk_count);
            tx.send(end_frame)
                .map_err(|_| RuntimeError::Handler("Failed to send STREAM_END".to_string()))?;
        }

        let end_frame = Frame::end(request_id.clone(), None);
        tx.send(end_frame)
            .map_err(|_| RuntimeError::Handler("Failed to send END".to_string()))?;
        drop(tx);

        // Live-feed references resolve in CLI mode exactly as on the wire
        // (13.2 §Reference Media): `mic-selector | cartridge cap` is the
        // standalone form of the same contract. Handles are per-invocation;
        // a direct CLI run stops via its own process lifecycle.
        let cli_feed_handles: Arc<Mutex<Vec<crate::bifaci::live_feed::LiveFeedHandle>>> =
            Arc::new(Mutex::new(Vec::new()));
        let cli_lf_ctx = LiveFeedContext::new(
            &cap.urn_string(),
            self.manifest.clone(),
            Arc::clone(&self.live_feed_overruns),
            cli_feed_handles,
        )
        .ok();
        let input_package = demux_multi_stream(rx, None, cli_lf_ctx, None);

        let cli_sender: Arc<dyn FrameSender> = Arc::new(frame_sender);
        // Same derived response label as the wire path (spawn_handler): the
        // CLI mode is a debugging surface for the same contract, not a
        // wildcard-labeled side channel.
        let out_media = derive_response_media(&cap.urn_string())?;
        let output = OutputStream::new(
            cli_sender.clone(),
            uuid::Uuid::new_v4().to_string(),
            out_media,
            request_id.clone(),
            None,
            Limits::default().max_chunk,
        );

        let op = factory();
        let peer_arc: Arc<dyn PeerInvoker> = Arc::new(peer);
        dispatch_op(op, input_package, output, peer_arc).await
    }

    /// Find a cap by one of its aliases (the CLI subcommand). Aliases are
    /// globally unique, so at most one cap matches — the direct cartridge CLI
    /// selects the exact cap named, with no family narrowing.
    fn find_cap_by_alias<'a>(&self, manifest: &'a CapManifest, alias: &str) -> Option<&'a Cap> {
        manifest
            .all_caps()
            .into_iter()
            .find(|cap| cap.has_alias(alias))
    }

    /// Build payload from streaming stdin (CLI mode with piped binary).
    ///
    /// Public wrapper that reads from actual stdin.
    fn build_payload_from_streaming_stdin(&self, cap: &Cap) -> Result<Vec<u8>, RuntimeError> {
        let stdin = io::stdin();
        let locked = stdin.lock();
        self.build_payload_from_streaming_reader(cap, locked, Limits::default().max_chunk)
    }

    /// Build payload from streaming reader (testable version).
    ///
    /// This simulates the CBOR chunked request flow for CLI piped stdin:
    /// - Pure binary chunks from reader
    /// - Converted to virtual CHUNK frames on-the-fly
    /// - Accumulated via accumulation (same as CBOR mode)
    /// - Handler invoked when reader EOF (simulates END frame)
    ///
    /// This makes all 4 modes use the SAME accumulation code path:
    /// - CLI file path → read file → payload
    /// - CLI piped binary → chunk reader → accumulation → payload
    /// - CBOR chunked → accumulation → payload
    /// - CBOR file path → auto-convert → payload
    fn build_payload_from_streaming_reader<R: io::Read>(
        &self,
        cap: &Cap,
        mut reader: R,
        max_chunk: usize,
    ) -> Result<Vec<u8>, RuntimeError> {
        // Simulate accumulation structure (same as CBOR mode)
        struct PendingRequest {
            cap_urn: String,
            chunks: Vec<Vec<u8>>,
        }

        let mut pending = PendingRequest {
            cap_urn: cap.urn_string(),
            chunks: Vec::new(),
        };
        loop {
            let mut buffer = vec![0u8; max_chunk];
            match reader.read(&mut buffer) {
                Ok(0) => {
                    // EOF - simulate END frame
                    break;
                }
                Ok(n) => {
                    buffer.truncate(n);

                    // Simulate receiving CHUNK frame - add to accumulator immediately
                    pending.chunks.push(buffer);
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {
                    continue;
                }
                Err(e) => {
                    return Err(RuntimeError::Io(e));
                }
            }
        }

        // Concatenate chunks (same as accumulation does on END frame)
        let complete_payload = pending.chunks.concat();

        // Build CBOR arguments array (same format as CBOR mode)
        let cap_urn = CapUrn::from_string(&pending.cap_urn)
            .map_err(|e| RuntimeError::Cli(format!("Invalid cap URN: {}", e)))?;
        let expected_media_urn = cap_urn.in_spec();

        let arg = CapArgumentValue::new(expected_media_urn, complete_payload);
        let mut cbor_payload = Vec::new();
        let cbor_args: Vec<ciborium::Value> = vec![ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text(arg.media_urn.clone()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(arg.value.clone()),
            ),
        ])];
        ciborium::into_writer(&ciborium::Value::Array(cbor_args), &mut cbor_payload)
            .map_err(|e| RuntimeError::Serialize(format!("Failed to serialize CBOR: {}", e)))?;

        Ok(cbor_payload)
    }

    /// Build payload from CLI arguments based on cap's arg definitions.
    ///
    /// This method builds a CBOR arguments array (same format as CBOR mode) to ensure
    /// consistency between CLI mode and CBOR mode. The payload format is:
    /// ```text
    /// [ { media_urn: "...", value: bytes }, ... ]
    /// ```
    fn build_payload_from_cli(
        &self,
        cap: &Cap,
        cli_args: &[String],
    ) -> Result<Vec<u8>, RuntimeError> {
        let mut arguments: Vec<CapArgumentValue> = Vec::new();

        // Piped stdin is read lazily, at most once, and only when an arg
        // whose earlier sources (cli_flag, position) yielded nothing reaches
        // its Stdin source. Args fully satisfied from the command line never
        // touch stdin, so a cap invoked with explicit args can't hang on an
        // inherited never-closing stdin. See `read_piped_stdin` for why the
        // read itself blocks to EOF.
        let mut stdin_cache: Option<Option<Vec<u8>>> = None;
        let mut stdin_source = || -> Result<Option<Vec<u8>>, RuntimeError> {
            if stdin_cache.is_none() {
                stdin_cache = Some(Self::read_piped_stdin()?);
            }
            Ok(stdin_cache.as_ref().unwrap().clone())
        };

        // Process each cap argument
        for arg_def in cap.get_args() {
            let (value, came_from_stdin) =
                self.extract_arg_value(&arg_def, cli_args, &mut stdin_source)?;

            if let Some(val) = value {
                // Determine media_urn: if value came from stdin source, use stdin's media_urn
                // Otherwise use arg's media_urn as-is (file-path conversion happens later)
                let media_urn = if came_from_stdin {
                    // Find stdin source's media_urn
                    arg_def
                        .sources
                        .iter()
                        .find_map(|s| match s {
                            ArgSource::Stdin { stdin } => Some(stdin.clone()),
                            _ => None,
                        })
                        .unwrap_or_else(|| arg_def.media_urn.clone())
                } else {
                    arg_def.media_urn.clone()
                };

                arguments.push(CapArgumentValue {
                    media_urn,
                    value: val,
                });
            } else if arg_def.required {
                return Err(RuntimeError::MissingArgument(format!(
                    "Required argument '{}' not provided",
                    arg_def.media_urn
                )));
            }
        }

        // If no arguments are defined but stdin data exists, use it as raw payload
        if cap.get_args().is_empty() {
            if let Some(data) = stdin_source()? {
                return Ok(data);
            }
            // No args and no stdin - return empty payload
            return Ok(vec![]);
        }

        // Build CBOR arguments array (same format as CBOR mode)
        if !arguments.is_empty() {
            let cbor_args: Vec<ciborium::Value> = arguments
                .iter()
                .map(|arg| {
                    ciborium::Value::Map(vec![
                        (
                            ciborium::Value::Text("media_urn".to_string()),
                            ciborium::Value::Text(arg.media_urn.clone()),
                        ),
                        (
                            ciborium::Value::Text("value".to_string()),
                            ciborium::Value::Bytes(arg.value.clone()),
                        ),
                    ])
                })
                .collect();

            let cbor_array = ciborium::Value::Array(cbor_args);
            let mut payload = Vec::new();
            ciborium::into_writer(&cbor_array, &mut payload).map_err(|e| {
                RuntimeError::Serialize(format!("Failed to encode CBOR payload: {}", e))
            })?;

            return Ok(payload);
        }

        // No arguments and no stdin
        Ok(vec![])
    }

    /// Extract a single argument value from CLI args or stdin.
    /// Returns (value, came_from_stdin) to track the source.
    fn extract_arg_value(
        &self,
        arg_def: &CapArg,
        cli_args: &[String],
        stdin: &mut dyn FnMut() -> Result<Option<Vec<u8>>, RuntimeError>,
    ) -> Result<(Option<Vec<u8>>, bool), RuntimeError> {
        // Try each source in order, returning RAW values (file paths, flags, etc.)
        // File-path auto-conversion happens later in extract_effective_payload()
        //
        // `stdin` is a lazy source so stdin is only consumed when an arg
        // actually reaches its Stdin source — and sources still take
        // precedence over the arg's default value (piped data must beat a
        // declared default).
        for source in &arg_def.sources {
            match source {
                ArgSource::CliFlag { cli_flag } => {
                    if let Some(value) = self.get_cli_flag_value(cli_args, cli_flag) {
                        return Ok((Some(value.into_bytes()), false));
                    }
                }
                ArgSource::Position { position } => {
                    // Positional args: filter out flags and their values
                    let positional = self.get_positional_args(cli_args);
                    if let Some(value) = positional.get(*position) {
                        return Ok((Some(value.clone().into_bytes()), false));
                    }
                }
                ArgSource::Stdin { .. } => {
                    if let Some(data) = stdin()? {
                        return Ok((Some(data), true)); // true = came from stdin
                    }
                }
            }
        }

        // Try default value.
        //
        // The wire contract for an arg stream is "bytes of the typed
        // media URN". For a `media:enc=utf-8`-shaped arg that's plain
        // UTF-8 text — NOT a JSON-encoded form. A naive
        // `serde_json::to_vec(default)` would corrupt every string
        // default by wrapping it in `"…"`: a `default_value =
        // serde_json::json!("hf:foo")` would arrive at the handler
        // as the eight bytes `"hf:foo"` (with quotes), which the
        // handler's `String::from_utf8` would surface as a literal
        // quoted string — and downstream parsers (model-spec,
        // system-prompt, etc.) would silently choke on the
        // quotation.
        //
        // The right behaviour is to encode each scalar JSON value
        // as its lexical wire form, matching exactly what the same
        // value typed at the CLI flag would produce:
        //
        // - `Value::String(s)` ⇒ `s.as_bytes()` — the raw text, no
        //   quoting. The CLI equivalent `--flag value` produces
        //   `b"value"`, so this default must too.
        // - `Value::Number(n)` ⇒ the lexical decimal (`"512"`,
        //   `"0.7"`). `serde_json::to_vec` happens to produce the
        //   same bytes for numbers because JSON-numbers and
        //   decimal-text coincide; we route through `to_string()`
        //   anyway so the contract is explicit.
        // - `Value::Bool(b)` ⇒ `"true"` / `"false"`.
        // - `Value::Null` ⇒ `""` (empty bytes). A null default is
        //   identical in semantics to "no default supplied".
        // - `Value::Array(_)` / `Value::Object(_)` ⇒ JSON-encoded
        //   bytes via `serde_json::to_vec`. Composite defaults are
        //   the case where the wire form genuinely IS JSON — a cap
        //   that declares an array default expects to receive that
        //   array as JSON on its arg stream.
        if let Some(default) = &arg_def.default_value {
            let bytes: Vec<u8> = match default {
                serde_json::Value::String(s) => s.as_bytes().to_vec(),
                serde_json::Value::Number(n) => n.to_string().into_bytes(),
                serde_json::Value::Bool(b) => {
                    if *b {
                        b"true".to_vec()
                    } else {
                        b"false".to_vec()
                    }
                }
                serde_json::Value::Null => Vec::new(),
                serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                    serde_json::to_vec(default)
                        .map_err(|e| RuntimeError::Serialize(e.to_string()))?
                }
            };
            return Ok((Some(bytes), false));
        }

        Ok((None, false))
    }

    /// Get value for a CLI flag (e.g., --model "value")
    fn get_cli_flag_value(&self, args: &[String], flag: &str) -> Option<String> {
        let mut iter = args.iter();
        while let Some(arg) = iter.next() {
            if arg == flag {
                return iter.next().cloned();
            }
            // Handle --flag=value format
            if let Some(stripped) = arg.strip_prefix(&format!("{}=", flag)) {
                return Some(stripped.to_string());
            }
        }
        None
    }

    /// Get positional arguments (non-flag arguments)
    fn get_positional_args(&self, args: &[String]) -> Vec<String> {
        let mut positional = Vec::new();
        let mut skip_next = false;

        for arg in args {
            if skip_next {
                skip_next = false;
                continue;
            }
            if arg.starts_with('-') {
                // This is a flag - skip its value too
                if !arg.contains('=') {
                    skip_next = true;
                }
            } else {
                positional.push(arg.clone());
            }
        }
        positional
    }

    /// Read piped stdin to EOF, blocking until the writer closes its end.
    /// Returns None when stdin is a terminal (interactive — nothing piped)
    /// or delivers zero bytes.
    ///
    /// This deliberately BLOCKS: stdin being non-terminal means the invoker
    /// piped or redirected input, and the only correct interpretation is to
    /// wait for the sender to finish — standard POSIX tool semantics (the
    /// Windows arm always behaved this way). The previous Unix-only
    /// 0-timeout `poll()` peek raced the writer: a spawner that had not yet
    /// written looked identical to "no input at all", so stdin-sourced
    /// required args intermittently went missing under load. Callers keep
    /// stdin consumption LAZY instead — it is only consulted when an arg's
    /// earlier sources yielded nothing — so caps fully satisfied from the
    /// command line never touch stdin and cannot block on it.
    fn read_piped_stdin() -> Result<Option<Vec<u8>>, RuntimeError> {
        use std::io::IsTerminal;

        let stdin = io::stdin();

        // Don't read from stdin if it's a terminal (interactive)
        if stdin.is_terminal() {
            return Ok(None);
        }

        let mut data = Vec::new();
        stdin.lock().read_to_end(&mut data)?;
        if data.is_empty() {
            Ok(None)
        } else {
            Ok(Some(data))
        }
    }

    /// Print help message showing all available subcommands.
    fn print_help(&self, manifest: &CapManifest) {
        let stderr = std::io::stderr();
        let mut handle = stderr.lock();
        use std::io::Write;

        let _ = writeln!(handle, "Usage: {} <command> [options]", manifest.name);
        let _ = writeln!(handle);
        let _ = writeln!(handle, "Commands:");
        let _ = writeln!(
            handle,
            "    {:16} Output cartridge manifest as JSON",
            "manifest"
        );

        for cap in manifest.all_caps() {
            let desc = cap.cap_description.as_deref().unwrap_or(&cap.title);
            let padded_command = format!("{:16}", cap.primary_alias());
            let _ = writeln!(handle, "    {}{}", padded_command, desc);
        }
        let _ = writeln!(handle);
        let _ = writeln!(
            handle,
            "Run '<command> --help' for more information on a command."
        );
    }

    /// Print help for a specific cap.
    fn print_cap_help(&self, cap: &Cap) {
        let stderr = std::io::stderr();
        let mut handle = stderr.lock();
        use std::io::Write;

        let _ = writeln!(handle, "Usage: {} [options]", cap.primary_alias());
        let _ = writeln!(handle);
        let desc = cap.cap_description.as_deref().unwrap_or(&cap.title);
        let _ = writeln!(handle, "{}", desc);
        let _ = writeln!(handle);
        let _ = writeln!(handle, "Arguments:");

        for arg in &cap.args {
            let desc = arg.arg_description.as_deref().unwrap_or("");
            let required_str = if arg.required { " (required)" } else { "" };

            for source in &arg.sources {
                match source {
                    ArgSource::CliFlag { cli_flag } => {
                        let padded_flag = format!("{:16}", cli_flag);
                        let _ = writeln!(handle, "    {}{}{}", padded_flag, desc, required_str);
                    }
                    ArgSource::Position { position } => {
                        let arg_name = format!("<arg{}>", position);
                        let padded_arg = format!("{:16}", arg_name);
                        let _ = writeln!(handle, "    {}{}{}", padded_arg, desc, required_str);
                    }
                    ArgSource::Stdin { .. } => {
                        let _ = writeln!(handle, "    {:16}{}{}", "<stdin>", desc, required_str);
                    }
                }
            }
        }
    }

    /// Run in Cartridge CBOR mode - binary frame protocol via stdin/stdout.
    ///
    /// Requests beyond a pool chain's effective capacity queue on their
    /// cap's own queue. A LOG frame with `level="queued"` is sent back
    /// immediately so the pipeline's per-body activity timeout pauses; when
    /// any chain pool frees, the oldest eligible waiter (global FIFO across
    /// caps) is dequeued and its handler spawned. Frames for queued requests
    /// are buffered in the crossbeam channel (created on REQ) until the
    /// handler's demux drains them.
    async fn run_cbor_mode(&self) -> Result<(), RuntimeError> {
        let stdin = tokio::io::stdin();
        let CborStdout {
            handshake_stdout,
            frame_stdout,
        } = prepare_cbor_stdout().map_err(RuntimeError::Io)?;

        let reader = BufReader::new(stdin);

        let mut frame_reader = FrameReader::new(reader);
        // Handshake uses a temporary async writer on the dup'd fd.
        let mut hs_async_writer = tokio::io::BufWriter::new(handshake_stdout);
        let mut hs_frame_writer = FrameWriter::new(&mut hs_async_writer);

        // Materialize the concurrency pools BEFORE the handshake: the HELLO
        // must carry the full pool-state map, and the map is built from the
        // registered handler patterns + the manifest's declarations. Errors
        // here are cartridge-author bugs (an unresolved pool declaration, a
        // duplicate registration) and fail the process at startup, precisely
        // named, rather than surfacing as capacity misbehavior later.
        let initial_pool_states = {
            let handler_patterns: Vec<String> = self.handlers.keys().cloned().collect();
            let declarations = self
                .manifest
                .as_ref()
                .map(|m| m.pool_declarations.clone())
                .unwrap_or_default();
            let pools = RuntimePools::init(&handler_patterns, &declarations)
                .map_err(RuntimeError::Serialize)?;
            let snapshot = pools.snapshot();
            *self.pools.lock().expect("runtime pools mutex poisoned") = Some(pools);
            snapshot
        };

        let negotiated_limits = handshake_accept(
            &mut frame_reader,
            &mut hs_frame_writer,
            &self.manifest_data,
            &initial_pool_states,
        )
        .await?;
        frame_reader.set_limits(negotiated_limits.clone());
        // Flush and drop the async handshake writer; safe_fd stays open for sync writes.
        drop(hs_frame_writer);
        hs_async_writer.flush().await.map_err(RuntimeError::Io)?;
        drop(hs_async_writer);
        let frame_stdout = frame_stdout.into_file().map_err(RuntimeError::Io)?;

        // Create output channel using std::sync::mpsc so the writer thread is
        // completely decoupled from tokio. Metal/GCD on macOS can steal all
        // tokio worker threads during large model loading, freezing tokio tasks
        // (including tokio::spawn writer tasks and interval timers). A plain
        // std::thread with blocking I/O is immune to this.
        let (output_tx_sync, output_rx_sync) = std::sync::mpsc::channel::<Frame>();

        // Wrap in a newtype so existing code that calls output_tx.send() still works.
        // We bridge tokio::sync::mpsc → std::sync::mpsc via a forwarding task below.
        let (output_tx, mut output_rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();

        // Forward tokio channel → std channel so async handlers can still use output_tx.
        let fwd_tx = output_tx_sync.clone();
        tokio::spawn(async move {
            while let Some(frame) = output_rx.recv().await {
                if fwd_tx.send(frame).is_err() {
                    break;
                }
            }
        });

        // Spawn writer thread on a plain OS thread — immune to tokio/Metal/GCD.
        let writer_limits = negotiated_limits.clone();
        let writer_stragglers = Arc::clone(&self.straggler_counters);
        let writer_handle = std::thread::spawn(move || {
            let mut writer = std::io::BufWriter::new(frame_stdout);
            let mut seq_assigner = SeqAssigner::new();
            let mut terminated = crate::bifaci::stats::TerminatedFlows::new(1024);
            'outer: while let Ok(frame) = output_rx_sync.recv() {
                match write_gated(
                    frame,
                    &mut writer,
                    &writer_limits,
                    &mut seq_assigner,
                    &mut terminated,
                    &writer_stragglers,
                ) {
                    GatedWrite::WriterDead => break,
                    GatedWrite::Written | GatedWrite::SuppressedStraggler => {}
                }
                // Flush when no more frames are immediately available so the
                // host sees progress/log frames without waiting for the
                // BufWriter to fill. We must NOT consume the next queued
                // frame here: peek with a zero-cost emptiness check via a
                // separate try_recv that, when it does pull a frame, must
                // be processed in the next iteration. To avoid losing the
                // frame, we only flush when try_recv reports Empty; if it
                // returns a frame, we re-inject it by handling it inline.
                match output_rx_sync.try_recv() {
                    Err(std::sync::mpsc::TryRecvError::Empty) => {
                        if let Err(e) = writer.flush() {
                            tracing::error!(
                                target: "cartridge_runtime",
                                error = %e,
                                "[CartridgeRuntime] writer thread: flush failed"
                            );
                            break;
                        }
                    }
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
                    Ok(next_frame) => match write_gated(
                        next_frame,
                        &mut writer,
                        &writer_limits,
                        &mut seq_assigner,
                        &mut terminated,
                        &writer_stragglers,
                    ) {
                        GatedWrite::WriterDead => break 'outer,
                        GatedWrite::Written | GatedWrite::SuppressedStraggler => {}
                    },
                }
            }
            let _ = writer.flush();
        });

        // Track pending peer requests (cartridge invoking host caps)
        let pending_peer_requests: Arc<Mutex<HashMap<MessageId, PendingPeerRequest>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Track active requests (incoming, frames routed here regardless of queue state).
        // The crossbeam sender lives here so the frame reader loop can route
        // STREAM_START/CHUNK/STREAM_END/END frames to it. This happens even for
        // queued requests — frames accumulate in the crossbeam channel until the
        // handler is spawned.
        let mut active_requests: HashMap<MessageId, ActiveRequest> = HashMap::new();

        // Track active handler tasks by request ID for per-request abort
        let mut active_handlers: HashMap<MessageId, JoinHandle<()>> = HashMap::new();
        // Track routing IDs per handler for stamping ERR frames on cancel
        let mut handler_routing_ids: HashMap<MessageId, Option<MessageId>> = HashMap::new();
        // Per-request open live-feed handles (13.2 §Live Feeds). A non-force
        // Cancel on a request with open feeds is a STOP: close the tap and
        // let the run drain (15.2 §Runs Stop) — the handler ends naturally.
        let mut live_feed_handles_by_rid: HashMap<
            MessageId,
            Arc<Mutex<Vec<crate::bifaci::live_feed::LiveFeedHandle>>>,
        > = HashMap::new();
        // Track cancelled requests to prevent duplicate ERR frames
        let mut cancelled_requests: std::collections::HashSet<MessageId> =
            std::collections::HashSet::new();

        // Routes inbound CREDIT frames to the gates of streams local senders are
        // writing. Gates register when an OutputStream starts a credited stream;
        // close_request releases waiters on terminal/cancel.
        let credit_router = crate::bifaci::credit::CreditRouter::new();

        // The registered pattern serving each running handler — the pool
        // chain to release when it finishes. (The waiting queues themselves
        // live inside RuntimePools: queues lead to pools.)
        let mut handler_patterns: HashMap<MessageId, String> = HashMap::new();

        // Notification channel: handlers send their RID when they finish so the main
        // loop can check cancelled state and send deferred ERR CANCELLED if needed.
        let (handler_done_tx, mut handler_done_rx) =
            tokio::sync::mpsc::unbounded_channel::<MessageId>();

        // Spawn a reader task that feeds frames into a channel.
        // This decouples stdin reading from the main select loop so that
        // handler-done signals can wake the loop even when no frames arrive.
        let (frame_tx, mut frame_rx) =
            tokio::sync::mpsc::unbounded_channel::<Result<Frame, CborError>>();
        let reader_handle = tokio::spawn(async move {
            loop {
                match frame_reader.read().await {
                    Ok(Some(frame)) => {
                        if frame_tx.send(Ok(frame)).is_err() {
                            break; // Main loop dropped — shutting down
                        }
                    }
                    Ok(None) => {
                        break; // EOF — stdin closed
                    }
                    Err(e) => {
                        let _ = frame_tx.send(Err(e));
                        break;
                    }
                }
            }
        });

        // Main loop: select between incoming frames and handler completion signals.
        // When a handler finishes it sends its RID on handler_done_tx, waking the
        // loop so it can check cancelled state, send deferred ERR if needed, and
        // drain the queue immediately — without waiting for the next frame from stdin.
        loop {
            // Drain queues: admit the oldest waiting request (global FIFO
            // across caps) whose whole pool chain has room, until none is
            // eligible. Admission and the active-count increments are one
            // atomic step inside the pools mutex.
            loop {
                let queued = {
                    let mut pools = self.pools.lock().expect("runtime pools mutex poisoned");
                    match pools.as_mut().expect("pools materialized at startup").pop_admissible() {
                        Some(q) => q,
                        None => break,
                    }
                };

                // Notify the caller that this request has been dequeued and is
                // starting. The "dequeued" level is the counterpart to "queued":
                // on the pipeline side, ActivityTimer unpauses and resets the
                // timeout clock, and the stall tracker is touched.
                let mut dequeued_log = Frame::log(
                    queued.request_id.clone(),
                    "dequeued",
                    crate::failure::AttributionClass::Internal,
                    "Request dequeued, handler starting",
                    None,
                );
                dequeued_log.routing_id = queued.routing_id.clone();
                let _ = output_tx.send(dequeued_log);

                let handler_rid = queued.request_id.clone();
                let handler_xid = queued.routing_id.clone();
                let feed_handles: Arc<Mutex<Vec<crate::bifaci::live_feed::LiveFeedHandle>>> =
                    Arc::new(Mutex::new(Vec::new()));
                live_feed_handles_by_rid.insert(handler_rid.clone(), Arc::clone(&feed_handles));
                handler_patterns.insert(handler_rid.clone(), queued.pattern.clone());
                let handle = spawn_handler(
                    queued.raw_rx,
                    queued.factory,
                    queued.cap_urn,
                    queued.request_id,
                    queued.routing_id,
                    &output_tx,
                    &pending_peer_requests,
                    &self.manifest,
                    negotiated_limits.max_chunk,
                    &handler_done_tx,
                    &self.drop_counters,
                    &credit_router,
                    negotiated_limits.initial_credit,
                    &self.live_feed_overruns,
                    &feed_handles,
                );
                active_handlers.insert(handler_rid.clone(), handle);
                handler_routing_ids.insert(handler_rid, handler_xid);
            }

            // Select: either a frame arrives from stdin or a handler finishes.
            let frame = tokio::select! {
                biased;
                // Handler done — reap by RID, release credit waiters (L13),
                // send deferred ERR if cancelled.
                Some(rid) = handler_done_rx.recv() => {
                    active_handlers.remove(&rid);
                    // Release the finished handler's whole pool chain — the
                    // atomic counterpart of its admission.
                    let pattern = handler_patterns.remove(&rid).unwrap_or_else(|| {
                        panic!("finished handler {rid:?} has no recorded pool pattern")
                    });
                    self.pools
                        .lock()
                        .expect("runtime pools mutex poisoned")
                        .as_mut()
                        .expect("pools materialized at startup")
                        .release(&pattern);
                    credit_router.close_request(&rid, "END");
                    // A finished handler's feeds are over — close any the
                    // provider hasn't observed as ended yet, and forget them.
                    if let Some(handles) = live_feed_handles_by_rid.remove(&rid) {
                        for handle in handles.lock().unwrap().iter() {
                            handle.close();
                        }
                    }
                    if cancelled_requests.remove(&rid) {
                        let routing_id = handler_routing_ids.remove(&rid).flatten();
                        let mut err = Frame::err(
                            rid,
                            "CANCELLED",
                            crate::failure::AttributionClass::Internal,
                            "Request cancelled",
                            None,
                        );
                        err.routing_id = routing_id;
                        let _ = output_tx.send(err);
                    } else {
                        handler_routing_ids.remove(&rid);
                    }
                    continue
                },
                // Frame from reader task.
                result = frame_rx.recv() => {
                    match result {
                        Some(Ok(f)) => f,
                        Some(Err(e)) => return Err(e.into()),
                        None => break, // Reader task ended (EOF)
                    }
                }
            };

            match frame.frame_type {
                FrameType::Req => {
                    // Extract routing_id (XID) FIRST — all error paths must include it
                    let routing_id = frame.routing_id.clone();

                    let cap_urn = match frame.cap.as_ref() {
                        Some(urn) => urn.clone(),
                        None => {
                            let mut err_frame = Frame::err(
                                frame.id,
                                "INVALID_REQUEST",
                                crate::failure::AttributionClass::Internal,
                                "Request missing cap URN",
                                None,
                            );
                            err_frame.routing_id = routing_id;
                            let _ = output_tx.send(err_frame);
                            continue;
                        }
                    };

                    let (pattern, factory) = match self.find_handler_with_pattern(&cap_urn) {
                        Some(found) => found,
                        None => {
                            // A dispatched cap this binary doesn't handle is a
                            // deployment/manifest mismatch — Environment.
                            let mut err_frame = Frame::err(
                                frame.id.clone(),
                                "NO_HANDLER",
                                crate::failure::AttributionClass::Environment,
                                &format!("No handler registered for cap: {}", cap_urn),
                                None,
                            );
                            err_frame.routing_id = routing_id;
                            let _ = output_tx.send(err_frame);
                            continue;
                        }
                    };

                    if frame.payload.as_ref().map_or(false, |p| !p.is_empty()) {
                        let mut err_frame = Frame::err(
                            frame.id,
                            "PROTOCOL_ERROR",
                            crate::failure::AttributionClass::Internal,
                            "REQ frame must have empty payload - use STREAM_START for arguments",
                            None,
                        );
                        err_frame.routing_id = routing_id;
                        let _ = output_tx.send(err_frame);
                        continue;
                    }

                    let request_id = frame.id.clone();

                    // Create channel for streaming frames to handler.
                    // Always created immediately so subsequent frames (STREAM_START,
                    // CHUNK, END) are routed here even if the handler isn't spawned
                    // yet. Frames accumulate in the crossbeam channel until the handler
                    // is spawned and the demux drains them.
                    let (raw_tx, raw_rx) = crossbeam_channel::unbounded();
                    active_requests.insert(request_id.clone(), ActiveRequest { raw_tx });

                    // Admit through the cap's whole pool chain, or queue on
                    // the cap's OWN queue — a saturated sibling cap never
                    // holds this request back except through a pool they
                    // genuinely share.
                    let admitted = self
                        .pools
                        .lock()
                        .expect("runtime pools mutex poisoned")
                        .as_mut()
                        .expect("pools materialized at startup")
                        .try_admit(&pattern);
                    if !admitted {
                        // Chain full — queue the request, send "queued" LOG back.
                        let queue_pos = {
                            let mut pools =
                                self.pools.lock().expect("runtime pools mutex poisoned");
                            pools.as_mut().expect("pools materialized at startup").enqueue(
                                QueuedRequest {
                                    factory,
                                    cap_urn,
                                    pattern: pattern.clone(),
                                    ticket: 0, // assigned by enqueue
                                    routing_id: routing_id.clone(),
                                    request_id: request_id.clone(),
                                    raw_rx,
                                },
                            )
                        };
                        let mut log_frame = Frame::log(
                            request_id,
                            "queued",
                            crate::failure::AttributionClass::Internal,
                            &format!(
                                "Request queued (position {} on pool '{}')",
                                queue_pos, pattern
                            ),
                            None,
                        );
                        log_frame.routing_id = routing_id;
                        let _ = output_tx.send(log_frame);
                    } else {
                        // Chain has room — spawn handler immediately.
                        let handler_rid = request_id.clone();
                        let handler_xid = routing_id.clone();
                        let feed_handles: Arc<Mutex<Vec<crate::bifaci::live_feed::LiveFeedHandle>>> =
                            Arc::new(Mutex::new(Vec::new()));
                        live_feed_handles_by_rid
                            .insert(handler_rid.clone(), Arc::clone(&feed_handles));
                        handler_patterns.insert(handler_rid.clone(), pattern);
                        let handle = spawn_handler(
                            raw_rx,
                            factory,
                            cap_urn,
                            request_id,
                            routing_id,
                            &output_tx,
                            &pending_peer_requests,
                            &self.manifest,
                            negotiated_limits.max_chunk,
                            &handler_done_tx,
                            &self.drop_counters,
                            &credit_router,
                            negotiated_limits.initial_credit,
                            &self.live_feed_overruns,
                            &feed_handles,
                        );
                        active_handlers.insert(handler_rid.clone(), handle);
                        handler_routing_ids.insert(handler_rid, handler_xid);
                    }
                }

                // Route STREAM_START / CHUNK / STREAM_END / LOG to active request or peer response
                FrameType::StreamStart
                | FrameType::Chunk
                | FrameType::StreamEnd
                | FrameType::Log => {
                    // Try active request first
                    if let Some(ar) = active_requests.get(&frame.id) {
                        if ar.raw_tx.send(frame.clone()).is_err() {
                            active_requests.remove(&frame.id);
                        }
                        continue;
                    }

                    // Try peer response
                    let peer = pending_peer_requests.lock().unwrap();
                    if let Some(pr) = peer.get(&frame.id) {
                        let _ = pr.sender.send(frame.clone());
                    } else {
                        tracing::warn!("[CartridgeRuntime] {:?} rid={:?} not found in active_requests or pending_peer_requests", frame.frame_type, frame.id);
                    }
                    drop(peer);
                }

                FrameType::End => {
                    // Try active request first -- send END then remove
                    if let Some(ar) = active_requests.remove(&frame.id) {
                        let _ = ar.raw_tx.send(frame.clone());
                        // raw_tx dropped here → Demux sees channel close after END
                        continue;
                    }

                    // Try peer response — send END then remove
                    let mut peer = pending_peer_requests.lock().unwrap();
                    if let Some(pr) = peer.remove(&frame.id) {
                        let _ = pr.sender.send(frame.clone());
                    } else {
                        tracing::warn!("[CartridgeRuntime] END for unknown rid={:?} (not in active_requests or pending_peer_requests)", frame.id);
                    }
                    drop(peer);
                }

                FrameType::Err => {
                    tracing::error!(
                        "[CartridgeRuntime] ERR received: rid={:?} code={:?} msg={:?}",
                        frame.id,
                        frame.error_code(),
                        frame.error_message()
                    );
                    // Try active request first
                    if let Some(ar) = active_requests.remove(&frame.id) {
                        let _ = ar.raw_tx.send(frame.clone());
                        continue;
                    }

                    // Try peer response
                    let mut peer = pending_peer_requests.lock().unwrap();
                    if let Some(pr) = peer.remove(&frame.id) {
                        let _ = pr.sender.send(frame.clone());
                    }
                    drop(peer);
                }

                FrameType::Cancel => {
                    let target_rid = frame.id.clone();

                    // Skip if already cancelled (prevent duplicate ERR)
                    if cancelled_requests.contains(&target_rid) {
                        continue;
                    }

                    // STOP, not cancel (15.2 §Runs Stop): a non-force Cancel
                    // for a request with OPEN live feeds closes the tap —
                    // the feeds end, the pipeline drains, and the request
                    // terminates naturally with complete outputs. The
                    // handles are forgotten here, so a SECOND Cancel falls
                    // through to the ordinary cooperative cancel (abort).
                    if !frame.force_kill.unwrap_or(false) {
                        if let Some(handles) = live_feed_handles_by_rid.remove(&target_rid) {
                            let handles = handles.lock().unwrap();
                            if !handles.is_empty() {
                                for handle in handles.iter() {
                                    handle.close();
                                }
                                tracing::info!(
                                    target: "cartridge_runtime",
                                    rid = ?target_rid,
                                    feeds = handles.len(),
                                    "[CartridgeRuntime] stop: closed live feeds — the run drains and ends naturally (a second Cancel aborts)"
                                );
                                continue;
                            }
                        }
                    }

                    // Case 1: Request is queued on its singleton pool —
                    // remove it (it holds no chain slots), send ERR.
                    let removed = self
                        .pools
                        .lock()
                        .expect("runtime pools mutex poisoned")
                        .as_mut()
                        .expect("pools materialized at startup")
                        .remove_queued(&target_rid);
                    if let Some(queued) = removed {
                        active_requests.remove(&target_rid);
                        let mut err = Frame::err(
                            target_rid.clone(),
                            "CANCELLED",
                            crate::failure::AttributionClass::Internal,
                            "Request cancelled while queued",
                            None,
                        );
                        err.routing_id = queued.routing_id;
                        let _ = output_tx.send(err);
                        continue;
                    }

                    // Case 2: Request has an active handler — cooperative cancel.
                    // force_kill is handled at the host level (kills the process);
                    // the cartridge runtime only ever sees cooperative cancels.
                    // Close the input channel so the handler's demux sees disconnect
                    // and the handler exits naturally. ERR CANCELLED is deferred
                    // until handlerDone(RID) arrives — this guarantees the handler's
                    // stream lifecycle completes (no orphaned streams) and produces
                    // identical wire behavior regardless of implementation language.
                    if active_handlers.contains_key(&target_rid) {
                        cancelled_requests.insert(target_rid.clone());
                        active_requests.remove(&target_rid);
                        // Release any credit-blocked writers immediately (L13,
                        // L17) — a cancelled producer must not hang on credit.
                        credit_router.close_request(&target_rid, "CANCELLED");

                        // Cancel peer calls originating from this request
                        let peer_rids_to_cancel: Vec<(MessageId, Option<MessageId>)> = {
                            let peer = pending_peer_requests.lock().unwrap();
                            peer.iter()
                                .filter(|(_, pr)| pr.origin_request_id == target_rid)
                                .map(|(rid, pr)| (rid.clone(), pr.origin_routing_id.clone()))
                                .collect()
                        };
                        for (peer_rid, _) in &peer_rids_to_cancel {
                            let cancel =
                                Frame::cancel(peer_rid.clone(), frame.force_kill.unwrap_or(false));
                            let _ = output_tx.send(cancel);
                        }
                        {
                            let mut peer = pending_peer_requests.lock().unwrap();
                            for (peer_rid, _) in &peer_rids_to_cancel {
                                peer.remove(peer_rid);
                            }
                        }
                        continue;
                    }

                    // Case 3: Unknown RID — silently ignore
                }

                FrameType::Credit => {
                    // Flow-control grant for one of this request's output streams.
                    // Grants only ever unblock a credit-waiting sender; a grant for
                    // a request with no registered gate (request finished, or its
                    // output is not credit-blocked) is a correct no-op.
                    if !credit_router.grant(&frame) {
                        tracing::trace!(
                            target: "cartridge_runtime",
                            rid = ?frame.id,
                            stream_id = ?frame.stream_id,
                            credits = ?frame.credit_count(),
                            "CREDIT for request with no registered gate — no-op"
                        );
                    }
                }

                FrameType::Heartbeat => {
                    // The heartbeat is the capacity CONFIG channel: a probe
                    // may carry the operator's desired `configured` values.
                    // The whole batch is validated first — an unknown pool
                    // refuses it all with an ERR naming it, and the probe
                    // gets that ERR instead of a reply, so the host's
                    // awaited apply fails precisely rather than silently.
                    if let Some(bytes) = frame.desired_capacity_bytes() {
                        let applied = crate::bifaci::pools::decode_desired(bytes)
                            .and_then(|desired| {
                                self.pools
                                    .lock()
                                    .expect("runtime pools mutex poisoned")
                                    .as_mut()
                                    .expect("pools materialized at startup")
                                    .apply_desired(&desired)
                            });
                        if let Err(reason) = applied {
                            let err = Frame::err(
                                frame.id,
                                "UNKNOWN_POOL",
                                crate::failure::AttributionClass::Internal,
                                &format!("desired capacities refused: {reason}"),
                                None,
                            );
                            let _ = output_tx.send(err);
                            continue;
                        }
                    }
                    let mut response = Frame::heartbeat(frame.id);
                    let mut meta = std::collections::BTreeMap::new();
                    if let Some((footprint_mb, rss_mb)) = get_own_memory_mb() {
                        meta.insert(
                            "footprint_mb".into(),
                            ciborium::Value::Integer(footprint_mb.into()),
                        );
                        meta.insert("rss_mb".into(), ciborium::Value::Integer(rss_mb.into()));
                    }
                    // Protocol observability (L8): the cartridge's dropped-
                    // frame total rides every heartbeat so the host can
                    // surface it without a dedicated stats round-trip. The
                    // benign straggler total rides alongside, under its own
                    // name — stragglers are not drops.
                    meta.insert(
                        "drops_total".into(),
                        ciborium::Value::Integer(
                            u64::try_from(self.drop_counters.total())
                                .expect("drop total must fit the protocol's uint64 domain")
                                .into(),
                        ),
                    );
                    meta.insert(
                        "stragglers_total".into(),
                        ciborium::Value::Integer(
                            u64::try_from(self.straggler_counters.total())
                                .expect("straggler total must fit the protocol's uint64 domain")
                                .into(),
                        ),
                    );
                    meta.insert(
                        "overruns_total".into(),
                        ciborium::Value::Integer(
                            self.live_feed_overruns
                                .load(std::sync::atomic::Ordering::Relaxed)
                                .into(),
                        ),
                    );
                    // The full concurrency-pool state — capacities, active
                    // and queued per pool — rides every heartbeat reply.
                    // Mandatory: the host hard-errors on a reply without it.
                    let pool_snapshot = self
                        .pools
                        .lock()
                        .expect("runtime pools mutex poisoned")
                        .as_ref()
                        .expect("pools materialized at startup")
                        .snapshot();
                    meta.insert(
                        crate::bifaci::pools::META_POOLS.into(),
                        ciborium::Value::Bytes(crate::bifaci::pools::encode_pool_states(
                            &pool_snapshot,
                        )),
                    );
                    response.meta = Some(meta);
                    let _ = output_tx.send(response);
                }

                FrameType::Hello => {
                    let err_frame = Frame::err(
                        frame.id,
                        "PROTOCOL_ERROR",
                        crate::failure::AttributionClass::Internal,
                        "Unexpected HELLO after handshake",
                        None,
                    );
                    let _ = output_tx.send(err_frame);
                }

                FrameType::RelayNotify | FrameType::RelayState => {
                    return Err(CborError::Protocol(format!(
                        "Relay frame {:?} must not reach cartridge runtime",
                        frame.frame_type
                    ))
                    .into());
                }
            }
        }

        // Graceful shutdown
        reader_handle.abort();
        let _ = reader_handle.await;
        drop(output_tx);

        let _ = tokio::task::spawn_blocking(move || {
            let _ = writer_handle.join();
        })
        .await;

        for (_, handle) in active_handlers {
            let _ = handle.await;
        }

        Ok(())
    }

    /// Get the current protocol limits
    pub fn limits(&self) -> &Limits {
        &self.limits
    }
}

/// Get this process's own physical memory footprint and RSS in MB.
/// Uses `proc_pid_rusage(getpid(), RUSAGE_INFO_V4)` which is always permitted,
/// even inside a macOS sandbox (the sandbox only blocks querying OTHER processes).
/// Returns `(footprint_mb, rss_mb)` or `None` on failure.
#[cfg(target_os = "macos")]
fn get_own_memory_mb() -> Option<(u64, u64)> {
    let mut info: libc::rusage_info_v4 = unsafe { std::mem::zeroed() };
    let result = unsafe {
        libc::proc_pid_rusage(
            std::process::id() as libc::pid_t,
            4, // RUSAGE_INFO_V4
            &mut info as *mut _ as *mut libc::rusage_info_t,
        )
    };
    if result == 0 {
        Some((
            info.ri_phys_footprint / (1024 * 1024),
            info.ri_resident_size / (1024 * 1024),
        ))
    } else {
        None
    }
}

#[cfg(not(target_os = "macos"))]
fn get_own_memory_mb() -> Option<(u64, u64)> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bifaci::frame::DEFAULT_MAX_CHUNK;

    /// Decode every length-prefixed frame from a captured wire buffer.
    fn decode_wire(buf: &[u8]) -> Vec<Frame> {
        let mut frames = Vec::new();
        let mut pos = 0;
        while pos + 4 <= buf.len() {
            let len = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            frames.push(
                crate::bifaci::io::decode_frame(&buf[pos..pos + len])
                    .expect("wire buffer must hold valid frames"),
            );
            pos += len;
        }
        assert_eq!(pos, buf.len(), "trailing bytes on the wire");
        frames
    }

    fn pool_test_request(pattern: &str) -> QueuedRequest {
        let (_tx, raw_rx) = crossbeam_channel::unbounded();
        QueuedRequest {
            factory: Arc::new(|| Box::new(IdentityOp)),
            cap_urn: pattern.to_string(),
            pattern: pattern.to_string(),
            ticket: 0,
            routing_id: None,
            request_id: MessageId::new_uuid(),
            raw_rx,
        }
    }

    fn pool_declarations(
        pools: &[(&str, &[&str])],
        capacities: &[(&str, u64)],
    ) -> crate::bifaci::pools::PoolDeclarations {
        let mut declarations = crate::bifaci::pools::PoolDeclarations::default();
        for (name, members) in pools {
            declarations.pools.insert(
                name.to_string(),
                members.iter().map(|m| m.to_string()).collect(),
            );
        }
        for (name, capacity) in capacities {
            declarations.capacities.insert(name.to_string(), *capacity);
        }
        declarations
    }

    const POOL_CAP_A: &str = "cap:pool-a";
    const POOL_CAP_B: &str = "cap:pool-b";

    // TEST1527: RuntimePools materializes one singleton per registered
    // pattern, every declared shared pool, and `all` — and a declaration
    // referencing a cap no handler serves is a hard cartridge-author error,
    // never a silently ignored name.
    #[test]
    fn test1527_runtime_pools_materialization_and_declaration_resolution() {
        let patterns = vec![POOL_CAP_A.to_string(), POOL_CAP_B.to_string()];
        let pools = RuntimePools::init(
            &patterns,
            &pool_declarations(&[("gpu", &[POOL_CAP_A, POOL_CAP_B])], &[("gpu", 1)]),
        )
        .expect("valid declarations must materialize");
        let snapshot = pools.snapshot();
        assert_eq!(snapshot.len(), 4, "two singletons + gpu + all");
        assert_eq!(snapshot["gpu"].declared, 1);
        assert_eq!(snapshot["gpu"].caps.len(), 2);
        assert_eq!(
            snapshot[crate::bifaci::pools::POOL_ALL].caps,
            vec![POOL_CAP_A.to_string(), POOL_CAP_B.to_string()]
        );
        assert_eq!(
            pools.chain(POOL_CAP_A),
            &[
                POOL_CAP_A.to_string(),
                "gpu".to_string(),
                crate::bifaci::pools::POOL_ALL.to_string()
            ]
        );

        let error = RuntimePools::init(
            &patterns,
            &pool_declarations(&[("gpu", &["cap:ghost"])], &[]),
        )
        .expect_err("a declaration cap with no registered handler must refuse");
        assert!(
            error.contains("cap:ghost"),
            "the refusal must name the unresolved cap: {error}"
        );
    }

    // TEST1528: singleton queues are ISOLATED — saturating one cap queues
    // its requests without touching a sibling cap's admission, and a release
    // admits the queued request.
    #[test]
    fn test1528_singleton_queue_isolation() {
        let patterns = vec![POOL_CAP_A.to_string(), POOL_CAP_B.to_string()];
        let mut pools = RuntimePools::init(
            &patterns,
            &pool_declarations(&[], &[(POOL_CAP_A, 1)]),
        )
        .expect("valid declarations must materialize");

        assert!(pools.try_admit(POOL_CAP_A), "first dispatch admits");
        assert!(!pools.try_admit(POOL_CAP_A), "singleton capacity 1 is full");
        let position = pools.enqueue(pool_test_request(POOL_CAP_A));
        assert_eq!(position, 1, "queue position is 1-based for the LOG");
        assert!(
            pools.try_admit(POOL_CAP_B),
            "a saturated sibling must not block this cap"
        );
        assert!(
            pools.pop_admissible().is_none(),
            "nothing is admissible while the singleton is full"
        );
        pools.release(POOL_CAP_A);
        let admitted = pools
            .pop_admissible()
            .expect("the release must admit the queued request");
        assert_eq!(admitted.pattern, POOL_CAP_A);
    }

    // TEST1529: admission across a shared pool's members on release is
    // arrival-ordered by the GLOBAL ticket — never cap-biased (e.g. by
    // alphabetical queue iteration).
    #[test]
    fn test1529_shared_pool_release_admits_in_global_arrival_order() {
        let patterns = vec![POOL_CAP_A.to_string(), POOL_CAP_B.to_string()];
        let mut pools = RuntimePools::init(
            &patterns,
            &pool_declarations(&[("gpu", &[POOL_CAP_A, POOL_CAP_B])], &[("gpu", 1)]),
        )
        .expect("valid declarations must materialize");

        assert!(pools.try_admit(POOL_CAP_A), "gpu slot taken");
        // B arrives before A — the global ticket must remember that even
        // though "cap:pool-a" sorts first.
        pools.enqueue(pool_test_request(POOL_CAP_B));
        pools.enqueue(pool_test_request(POOL_CAP_A));
        pools.release(POOL_CAP_A);
        let first = pools
            .pop_admissible()
            .expect("released gpu slot must admit the oldest waiter");
        assert_eq!(first.pattern, POOL_CAP_B, "arrival order, not cap order");
        assert!(
            pools.pop_admissible().is_none(),
            "gpu capacity 1 admits exactly one"
        );
    }

    // TEST1530: the operator's desired batch applies atomically — one
    // unknown pool refuses the WHOLE batch (nothing half-applied), a valid
    // batch rewrites `configured` and admission follows immediately.
    #[test]
    fn test1530_apply_desired_is_atomic_and_immediate() {
        let patterns = vec![POOL_CAP_A.to_string()];
        let mut pools = RuntimePools::init(
            &patterns,
            &pool_declarations(&[], &[(POOL_CAP_A, 1)]),
        )
        .expect("valid declarations must materialize");

        let mut bad = crate::bifaci::pools::DesiredCapacities::new();
        bad.insert(POOL_CAP_A.to_string(), 3);
        bad.insert("ghost".to_string(), 1);
        let error = pools
            .apply_desired(&bad)
            .expect_err("an unknown pool must refuse the batch");
        assert!(error.contains("ghost"));
        assert_eq!(
            pools.snapshot()[POOL_CAP_A].configured,
            1,
            "a refused batch must apply NOTHING"
        );

        let mut good = crate::bifaci::pools::DesiredCapacities::new();
        good.insert(POOL_CAP_A.to_string(), 2);
        pools.apply_desired(&good).expect("valid batch applies");
        assert!(pools.try_admit(POOL_CAP_A));
        assert!(pools.try_admit(POOL_CAP_A), "raise admits immediately");
        assert!(!pools.try_admit(POOL_CAP_A), "raised bound still bounds");
    }

    // TEST1531: `available` is the cartridge's self-report — effective =
    // min(configured, available) — and the snapshot counts a waiter on its
    // own singleton AND on every chain pool actually blocking it.
    #[test]
    fn test1531_available_self_report_and_snapshot_queued_attribution() {
        let patterns = vec![POOL_CAP_A.to_string(), POOL_CAP_B.to_string()];
        let mut pools = RuntimePools::init(
            &patterns,
            &pool_declarations(&[("gpu", &[POOL_CAP_A, POOL_CAP_B])], &[("gpu", 2)]),
        )
        .expect("valid declarations must materialize");

        // Self-limit gpu to 1 (model loading): effective = min(2, 1) = 1.
        pools
            .set_available("gpu", 1)
            .expect("gpu is a declared pool");
        assert!(pools.try_admit(POOL_CAP_A));
        assert!(
            !pools.try_admit(POOL_CAP_B),
            "the self-report must bound admission below `configured`"
        );
        pools.enqueue(pool_test_request(POOL_CAP_B));

        let snapshot = pools.snapshot();
        assert_eq!(snapshot["gpu"].available, Some(1));
        assert_eq!(
            snapshot[POOL_CAP_B].queued, 1,
            "a waiter always counts on its own singleton"
        );
        assert_eq!(
            snapshot["gpu"].queued, 1,
            "the full shared pool is the blocker and must own the queued count"
        );
        assert_eq!(
            snapshot[crate::bifaci::pools::POOL_ALL].queued,
            0,
            "an unlimited chain pool blocks nobody"
        );

        pools
            .set_available("gpu", 0)
            .expect("gpu is a declared pool");
        assert!(
            pools.try_admit(POOL_CAP_B),
            "clearing the self-limit (0 = unlimited) restores min(configured, ∞) = 2"
        );
        let error = pools
            .set_available("cap:ghost", 1)
            .expect_err("self-report on an unknown pool must refuse");
        assert!(error.contains("cap:ghost"));
    }

    // TEST7020: A flow frame reaching the writer after the flow's END has been written is suppressed as a benign counted straggler (never a drop) — END is the last flow frame on the wire.
    #[test]
    fn test7020_writer_gate_suppresses_post_terminal_stragglers() {
        let rid = MessageId::new_uuid();
        let limits = Limits::default();
        let mut wire: Vec<u8> = Vec::new();
        let mut seq = SeqAssigner::new();
        let mut terminated = crate::bifaci::stats::TerminatedFlows::new(16);
        let stragglers = crate::bifaci::stats::StragglerCounters::new();

        // In-order: chunk, END — both written.
        let payload = vec![1u8, 2, 3];
        let checksum = Frame::compute_checksum(&payload);
        let chunk = Frame::chunk(rid.clone(), "s1".to_string(), 0, payload, 0, checksum);
        assert!(matches!(
            write_gated(chunk, &mut wire, &limits, &mut seq, &mut terminated, &stragglers),
            GatedWrite::Written
        ));
        let end = Frame::end_ok_with(rid.clone(), None, Some(1.0), None);
        assert!(matches!(
            write_gated(end, &mut wire, &limits, &mut seq, &mut terminated, &stragglers),
            GatedWrite::Written
        ));

        // The detached-sender race: a straggler progress LOG enqueued after
        // the handler returned reaches the writer after END. Dropped+counted.
        let straggler = Frame::progress(rid.clone(), 1.0, "late keepalive");
        assert!(matches!(
            write_gated(
                straggler,
                &mut wire,
                &limits,
                &mut seq,
                &mut terminated,
                &stragglers
            ),
            GatedWrite::SuppressedStraggler
        ));
        assert_eq!(
            stragglers.get(FrameType::Log),
            1,
            "the suppressed straggler is counted as benign, named by frame type"
        );

        let frames = decode_wire(&wire);
        assert_eq!(frames.len(), 2, "straggler must not reach the wire");
        assert_eq!(frames[0].frame_type, FrameType::Chunk);
        assert_eq!(frames[1].frame_type, FrameType::End);
        assert_eq!(
            frames.last().unwrap().frame_type,
            FrameType::End,
            "END is the last flow frame on the wire (L4)"
        );
        // Seq is contiguous and terminal-final
        assert_eq!(frames[0].seq, 0);
        assert_eq!(frames[1].seq, 1);
    }

    // TEST7021: The writer gate is precise — flow frames before END are written, non-flow frames (heartbeat, credit) still pass after a flow's terminal, and only that flow is gated.
    #[test]
    fn test7021_writer_gate_precision() {
        let rid_a = MessageId::Uint(1);
        let rid_b = MessageId::Uint(2);
        let limits = Limits::default();
        let mut wire: Vec<u8> = Vec::new();
        let mut seq = SeqAssigner::new();
        let mut terminated = crate::bifaci::stats::TerminatedFlows::new(16);
        let stragglers = crate::bifaci::stats::StragglerCounters::new();

        // Progress before END is written (the gate never over-suppresses).
        let progress = Frame::progress(rid_a.clone(), 0.5, "halfway");
        assert!(matches!(
            write_gated(
                progress,
                &mut wire,
                &limits,
                &mut seq,
                &mut terminated,
                &stragglers
            ),
            GatedWrite::Written
        ));
        let end_a = Frame::end_ok(rid_a.clone(), None);
        assert!(matches!(
            write_gated(end_a, &mut wire, &limits, &mut seq, &mut terminated, &stragglers),
            GatedWrite::Written
        ));

        // Non-flow frames for the terminated flow still pass (heartbeats and
        // credit must never be blocked by data-flow termination).
        let hb = Frame::heartbeat(rid_a.clone());
        assert!(matches!(
            write_gated(hb, &mut wire, &limits, &mut seq, &mut terminated, &stragglers),
            GatedWrite::Written
        ));
        let credit = Frame::credit(
            rid_a.clone(),
            None,
            4,
            crate::bifaci::frame::CreditDirection::Response,
        );
        assert!(matches!(
            write_gated(
                credit,
                &mut wire,
                &limits,
                &mut seq,
                &mut terminated,
                &stragglers
            ),
            GatedWrite::Written
        ));

        // A different flow is untouched by A's terminal.
        let progress_b = Frame::progress(rid_b.clone(), 0.1, "other request");
        assert!(matches!(
            write_gated(
                progress_b,
                &mut wire,
                &limits,
                &mut seq,
                &mut terminated,
                &stragglers
            ),
            GatedWrite::Written
        ));

        // But a flow frame for A is gated.
        let late_a = Frame::log(
            rid_a,
            "info",
            crate::AttributionClass::Internal,
            "late",
            None,
        );
        assert!(matches!(
            write_gated(
                late_a,
                &mut wire,
                &limits,
                &mut seq,
                &mut terminated,
                &stragglers
            ),
            GatedWrite::SuppressedStraggler
        ));

        let frames = decode_wire(&wire);
        let types: Vec<FrameType> = frames.iter().map(|f| f.frame_type).collect();
        assert_eq!(
            types,
            vec![
                FrameType::Log,
                FrameType::End,
                FrameType::Heartbeat,
                FrameType::Credit,
                FrameType::Log
            ]
        );
        assert_eq!(
            stragglers.get(FrameType::Log),
            1,
            "only A's late flow frame was suppressed, as a benign straggler"
        );
    }

    // TEST7027: A frame sent through a ChannelFrameSender whose receiver is gone is a counted channel_closed drop, never a silent loss.
    #[tokio::test]
    async fn test7027_channel_closed_sends_are_counted() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();
        let drops = Arc::new(crate::bifaci::stats::DropCounters::new());
        let sender = ChannelFrameSender {
            tx,
            drops: Arc::clone(&drops),
        };

        // Receiver alive: send succeeds, nothing counted.
        let frame = Frame::progress(MessageId::new_uuid(), 0.4, "working");
        sender.send(&frame).expect("open channel accepts frames");
        assert_eq!(
            drops.get(crate::bifaci::frame::DropReason::ChannelClosed),
            0
        );

        // Receiver dropped: send fails AND the drop is counted.
        drop(rx);
        let err = sender.send(&frame).expect_err("closed channel rejects");
        assert!(err.to_string().contains("Output channel closed"));
        assert_eq!(
            drops.get(crate::bifaci::frame::DropReason::ChannelClosed),
            1
        );
        let _ = sender.send(&frame);
        assert_eq!(
            drops.get(crate::bifaci::frame::DropReason::ChannelClosed),
            2,
            "every dropped frame increments exactly once (L8)"
        );
    }

    // TEST7050: A credited sender emits exactly its window of chunks then stalls until a CREDIT grant arrives — observed on the frame channel.
    #[tokio::test]
    async fn test7050_sender_stalls_at_window_and_resumes_on_grant() {
        let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();
        let sender: Arc<dyn FrameSender> = Arc::new(ChannelFrameSender {
            tx: out_tx,
            drops: Arc::new(crate::bifaci::stats::DropCounters::new()),
        });
        let router = crate::bifaci::credit::CreditRouter::new();
        let rid = MessageId::new_uuid();
        // Window of 4 chunks; payload needs 6 chunks at max_chunk=4 bytes.
        let output = OutputStream::new(
            Arc::clone(&sender),
            "s1".to_string(),
            "media:enc=utf-8".to_string(),
            rid.clone(),
            None,
            4, // max_chunk: 4 bytes per chunk
        )
        .with_credit(4, router.clone());
        output.start(false, None).unwrap();

        let data: Vec<u8> = (0u8..24).collect(); // 6 chunks of 4 bytes
        let writer = tokio::spawn(async move {
            output.write(&data).await.unwrap();
            // write() coalesces (24 bytes is far under the batch threshold);
            // close() flushes the batch, so the window stall now happens
            // inside close's flush — same wire behavior, same law.
            output.close().await.unwrap();
        });

        // Exactly STREAM_START + 4 chunks appear, then the sender stalls.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let mut got = Vec::new();
        while let Ok(f) = out_rx.try_recv() {
            got.push(f);
        }
        assert_eq!(got[0].frame_type, FrameType::StreamStart);
        let chunks_before = got
            .iter()
            .filter(|f| f.frame_type == FrameType::Chunk)
            .count();
        assert_eq!(chunks_before, 4, "sender must stall at exactly the window");
        assert!(!writer.is_finished(), "writer must be blocked on credit");

        // Grant 2 → the remaining 2 chunks + STREAM_END flow; data is intact
        // and chunk indexes are contiguous (nothing lost or reordered).
        router.grant(&Frame::credit(
            rid,
            Some("s1".to_string()),
            2,
            crate::bifaci::frame::CreditDirection::Response,
        ));
        tokio::time::timeout(std::time::Duration::from_secs(2), writer)
            .await
            .expect("grant must unblock the writer")
            .unwrap();
        let mut rest = Vec::new();
        while let Ok(f) = out_rx.try_recv() {
            rest.push(f);
        }
        let chunks_after = rest
            .iter()
            .filter(|f| f.frame_type == FrameType::Chunk)
            .count();
        assert_eq!(chunks_after, 2, "grant releases exactly the granted chunks");
        assert_eq!(rest.last().unwrap().frame_type, FrameType::StreamEnd);
        let indexes: Vec<u64> = got
            .iter()
            .chain(rest.iter())
            .filter(|f| f.frame_type == FrameType::Chunk)
            .map(|f| f.chunk_index.unwrap())
            .collect();
        assert_eq!(indexes, vec![0, 1, 2, 3, 4, 5], "in order, none lost");
    }

    // TEST7062: LOG/progress frames flow while the data window is exhausted — control frames are never credited.
    #[tokio::test]
    async fn test7062_log_flows_while_window_exhausted() {
        let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();
        let sender: Arc<dyn FrameSender> = Arc::new(ChannelFrameSender {
            tx: out_tx,
            drops: Arc::new(crate::bifaci::stats::DropCounters::new()),
        });
        let router = crate::bifaci::credit::CreditRouter::new();
        let output = Arc::new(
            OutputStream::new(
                Arc::clone(&sender),
                "s1".to_string(),
                "media:enc=utf-8".to_string(),
                MessageId::new_uuid(),
                None,
                4,
            )
            .with_credit(1, router.clone()),
        );
        output.start(false, None).unwrap();

        // Exhaust the window (1 chunk), then block trying to send another.
        // write() coalesces (8 bytes is far under the batch threshold), so
        // the explicit flush is what ships the batch: 2 chunks at max_chunk
        // 4, blocking after the first consumes the whole window.
        let out2 = Arc::clone(&output);
        let writer = tokio::spawn(async move {
            let _ = out2.write(&[0u8; 8]).await;
            let _ = out2.flush().await; // blocks after chunk 1 (window = 1)
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!writer.is_finished(), "data sender must be stalled");

        // Progress still flows — uncredited (L14).
        output.progress(0.5, "still alive");
        let mut saw_progress = false;
        while let Ok(f) = out_rx.try_recv() {
            if f.frame_type == FrameType::Log && f.log_progress() == Some(0.5) {
                saw_progress = true;
            }
        }
        assert!(
            saw_progress,
            "progress must bypass the exhausted data window"
        );
        writer.abort();
    }

    // TEST7086: The runtime's counters keep the two categories apart — benign
    // writer-gate stragglers land in the straggler counters (named by frame
    // type), a closed-channel send is a genuine drop — each counted exactly
    // once, and neither pollutes the other (L8/L4).
    #[tokio::test]
    async fn test7086_drop_snapshot_matches_induced_drops() {
        let drops = Arc::new(crate::bifaci::stats::DropCounters::new());
        let stragglers = crate::bifaci::stats::StragglerCounters::new();
        let rid = MessageId::new_uuid();

        // Source 1: benign post-terminal stragglers at the writer gate (two).
        let limits = Limits::default();
        let mut wire: Vec<u8> = Vec::new();
        let mut seq = SeqAssigner::new();
        let mut terminated = crate::bifaci::stats::TerminatedFlows::new(4);
        write_gated(
            Frame::end_ok(rid.clone(), None),
            &mut wire,
            &limits,
            &mut seq,
            &mut terminated,
            &stragglers,
        );
        for _ in 0..2 {
            write_gated(
                Frame::progress(rid.clone(), 1.0, "straggler"),
                &mut wire,
                &limits,
                &mut seq,
                &mut terminated,
                &stragglers,
            );
        }

        // Source 2: closed-channel send (one genuine drop).
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();
        drop(rx);
        let sender = ChannelFrameSender {
            tx,
            drops: Arc::clone(&drops),
        };
        let _ = sender.send(&Frame::log(
            rid,
            "info",
            crate::AttributionClass::Internal,
            "dead channel",
            None,
        ));

        let straggler_snap = stragglers.snapshot();
        assert_eq!(
            straggler_snap.total, 2,
            "each benign straggler counted exactly once (L4)"
        );
        assert_eq!(straggler_snap.by_frame_type.get("log"), Some(&2));

        let snap = drops.snapshot();
        assert_eq!(snap.total, 1, "each genuine drop counted exactly once (L8)");
        assert_eq!(snap.by_reason.get("channel_closed"), Some(&1));
        assert_eq!(
            snap.by_reason_frame_type
                .get("channel_closed")
                .and_then(|m| m.get("log")),
            Some(&1),
            "the drop is named by frame type"
        );
        assert!(
            !snap.by_reason.contains_key("post_terminal"),
            "benign stragglers never appear among drops"
        );
    }

    // TEST7070: An unbounded input stream is consumed live — the handler observes early items while the producer is still emitting, and the stream reports itself unbounded.
    #[tokio::test]
    async fn test7070_unbounded_input_consumed_live() {
        let rid = MessageId::new_uuid();
        let (raw_tx, raw_rx) = crossbeam_channel::unbounded();

        let mk_chunk = |i: u64| {
            let mut payload = Vec::new();
            ciborium::into_writer(&ciborium::Value::Bytes(vec![i as u8]), &mut payload).unwrap();
            let checksum = Frame::compute_checksum(&payload);
            Frame::chunk(rid.clone(), "live".to_string(), i, payload, i, checksum)
        };

        // Announce an UNBOUNDED stream and send only the first item.
        raw_tx
            .send(Frame::stream_start_unbounded(
                rid.clone(),
                "live".to_string(),
                "media:enc=utf-8".to_string(),
                Some(true),
            ))
            .unwrap();
        raw_tx.send(mk_chunk(0)).unwrap();

        let mut package = demux_multi_stream(raw_rx.clone(), None, None, None);
        let mut stream = package.recv().await.unwrap().unwrap();
        assert!(stream.is_unbounded(), "STREAM_START flag must surface");

        // The handler receives item 0 while the producer has not produced
        // item 1 — no buffering-to-completion (L16).
        let (v0, _) = stream.recv().await.unwrap().unwrap();
        assert_eq!(v0, ciborium::Value::Bytes(vec![0]));

        // Producer continues; consumer keeps up item by item.
        raw_tx.send(mk_chunk(1)).unwrap();
        let (v1, _) = stream.recv().await.unwrap().unwrap();
        assert_eq!(v1, ciborium::Value::Bytes(vec![1]));

        // The unbounded stream still ENDS cleanly — no chunk_count promise.
        raw_tx
            .send(Frame::stream_end_unbounded(rid.clone(), "live".to_string()))
            .unwrap();
        raw_tx.send(Frame::end(rid.clone(), None)).unwrap();
        drop(raw_tx);
        assert!(
            stream.recv().await.is_none(),
            "stream closes after STREAM_END"
        );
    }

    // TEST1300: A sequence item CBOR-encoded once and split across multiple CHUNK frames (the emit_list_item framing) reassembles into exactly one delivered item carrying the first fragment's per-item metadata.
    #[tokio::test]
    async fn test1300_sequence_item_fragments_reassemble_into_one_item() {
        let rid = MessageId::new_uuid();
        let (raw_tx, raw_rx) = crossbeam_channel::unbounded();

        // One large item, encoded once, then fragmented — exactly what
        // emit_list_item does for an item bigger than max_chunk. Per-frame
        // decoding of any fragment fails with a CBOR UnexpectedEof, which is
        // how cap→cap forwarding of rendered page images broke.
        let item_bytes: Vec<u8> = (0..600_000u32).map(|i| (i % 251) as u8).collect();
        let mut encoded = Vec::new();
        ciborium::into_writer(&ciborium::Value::Bytes(item_bytes.clone()), &mut encoded).unwrap();
        assert!(
            encoded.len() > DEFAULT_MAX_CHUNK,
            "item must span multiple fragments"
        );

        raw_tx
            .send(Frame::stream_start(
                rid.clone(),
                "s1".to_string(),
                "media:ext=png;image".to_string(),
                Some(true),
            ))
            .unwrap();

        let mut item_meta = StreamMeta::new();
        item_meta.insert("title".to_string(), ciborium::Value::Text("page 1".into()));
        let fragment_size = DEFAULT_MAX_CHUNK;
        let mut n_frames = 0u64;
        for (i, fragment) in encoded.chunks(fragment_size).enumerate() {
            let payload = fragment.to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut frame = Frame::chunk(
                rid.clone(),
                "s1".to_string(),
                i as u64,
                payload,
                i as u64,
                checksum,
            );
            // emit_list_item puts per-item meta on the FIRST fragment only.
            if i == 0 {
                frame.meta = Some(item_meta.clone());
            }
            raw_tx.send(frame).unwrap();
            n_frames += 1;
        }
        // A second, single-fragment item follows — reassembly must realign
        // on the item boundary, not swallow it into the first.
        let mut second = Vec::new();
        ciborium::into_writer(&ciborium::Value::Bytes(vec![7, 7, 7]), &mut second).unwrap();
        let checksum = Frame::compute_checksum(&second);
        raw_tx
            .send(Frame::chunk(
                rid.clone(),
                "s1".to_string(),
                n_frames,
                second,
                n_frames,
                checksum,
            ))
            .unwrap();
        n_frames += 1;
        raw_tx
            .send(Frame::stream_end(rid.clone(), "s1".to_string(), n_frames))
            .unwrap();
        raw_tx.send(Frame::end(rid.clone(), None)).unwrap();
        drop(raw_tx);

        let mut package = demux_multi_stream(raw_rx, None, None, None);
        let mut stream = package.recv().await.unwrap().unwrap();

        let (v0, m0) = stream.recv().await.unwrap().unwrap();
        assert_eq!(
            v0,
            ciborium::Value::Bytes(item_bytes),
            "fragments must reassemble into the original item"
        );
        assert_eq!(m0, Some(item_meta), "first fragment's meta rides the item");

        let (v1, m1) = stream.recv().await.unwrap().unwrap();
        assert_eq!(v1, ciborium::Value::Bytes(vec![7, 7, 7]));
        assert_eq!(m1, None);

        assert!(stream.recv().await.is_none(), "exactly two items");
    }

    // TEST1301: A sequence stream that ENDs mid-item (trailing fragment bytes that never complete a CBOR item) surfaces a hard decode error instead of silently dropping the partial item.
    #[tokio::test]
    async fn test1301_sequence_stream_truncated_mid_item_fails_hard() {
        let rid = MessageId::new_uuid();
        let (raw_tx, raw_rx) = crossbeam_channel::unbounded();

        let mut encoded = Vec::new();
        ciborium::into_writer(&ciborium::Value::Bytes(vec![42u8; 4096]), &mut encoded).unwrap();
        // Send only a strict prefix of the item, then STREAM_END.
        let payload = encoded[..encoded.len() / 2].to_vec();
        let checksum = Frame::compute_checksum(&payload);

        raw_tx
            .send(Frame::stream_start(
                rid.clone(),
                "s1".to_string(),
                "media:ext=png;image".to_string(),
                Some(true),
            ))
            .unwrap();
        raw_tx
            .send(Frame::chunk(
                rid.clone(),
                "s1".to_string(),
                0,
                payload,
                0,
                checksum,
            ))
            .unwrap();
        raw_tx
            .send(Frame::stream_end(rid.clone(), "s1".to_string(), 1))
            .unwrap();
        raw_tx.send(Frame::end(rid.clone(), None)).unwrap();
        drop(raw_tx);

        let mut package = demux_multi_stream(raw_rx, None, None, None);
        let mut stream = package.recv().await.unwrap().unwrap();
        let err = stream
            .recv()
            .await
            .expect("truncation must surface, not close silently")
            .expect_err("a partial item is an error");
        assert!(
            err.to_string().contains("mid-item"),
            "expected truncation error, got: {}",
            err
        );
    }

    // TEST1302: Continuation fragments of a multi-frame sequence item are credited back by the demux on arrival — the handler grants one frame per consumed item, so without fragment grants an item spanning more frames than the credit window could never finish arriving.
    #[tokio::test]
    async fn test1302_sequence_fragment_frames_are_credited_on_arrival() {
        let (grant_tx, mut grant_rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();
        let sender: Arc<dyn FrameSender> = Arc::new(ChannelFrameSender {
            tx: grant_tx,
            drops: Arc::new(crate::bifaci::stats::DropCounters::new()),
        });
        let rid = MessageId::new_uuid();
        let (raw_tx, raw_rx) = crossbeam_channel::unbounded();

        // One item spanning 4 fragments against a credit window of 2: only
        // demux-side fragment grants keep the producer's window open.
        let item_bytes = vec![9u8; 4 * 1024];
        let mut encoded = Vec::new();
        ciborium::into_writer(&ciborium::Value::Bytes(item_bytes.clone()), &mut encoded).unwrap();
        let fragment_size = encoded.len().div_ceil(4);

        raw_tx
            .send(Frame::stream_start(
                rid.clone(),
                "s1".to_string(),
                "media:ext=png;image".to_string(),
                Some(true),
            ))
            .unwrap();
        let mut n_fragments = 0u64;
        for (i, fragment) in encoded.chunks(fragment_size).enumerate() {
            let payload = fragment.to_vec();
            let checksum = Frame::compute_checksum(&payload);
            raw_tx
                .send(Frame::chunk(
                    rid.clone(),
                    "s1".to_string(),
                    i as u64,
                    payload,
                    i as u64,
                    checksum,
                ))
                .unwrap();
            n_fragments += 1;
        }
        assert_eq!(n_fragments, 4);
        raw_tx
            .send(Frame::stream_end(
                rid.clone(),
                "s1".to_string(),
                n_fragments,
            ))
            .unwrap();
        raw_tx.send(Frame::end(rid.clone(), None)).unwrap();
        drop(raw_tx);

        let mut package = demux_multi_stream(
            raw_rx,
            None,
            None,
            Some(InputCreditContext {
                sender,
                rid: rid.clone(),
                xid: None,
                initial_credit: 2,
            }),
        );
        let mut stream = package.recv().await.unwrap().unwrap();
        let (v0, _) = stream.recv().await.unwrap().unwrap();
        assert_eq!(v0, ciborium::Value::Bytes(item_bytes));
        assert!(stream.recv().await.is_none());
        drop(stream);

        // Continuation fragments (all but the item's first frame) must have
        // been credited by the demux as they arrived: 3 immediate one-frame
        // grants. The item's own frame is granted by handler consumption.
        let mut demux_granted = 0u64;
        while let Ok(frame) = grant_rx.try_recv() {
            if frame.frame_type == FrameType::Credit {
                demux_granted += frame.credit_count().unwrap_or(0);
            }
        }
        assert!(
            demux_granted >= n_fragments - 1,
            "expected at least {} fragment credits, saw {}",
            n_fragments - 1,
            demux_granted
        );
    }

    // TEST7073: Buffering collectors refuse unbounded streams with a hard error instead of buffering without bound.
    #[tokio::test]
    async fn test7073_collect_refuses_unbounded_streams() {
        let make_unbounded = || {
            let (tx, rx) = unbounded_channel();
            tx.send(Ok((ciborium::Value::Bytes(vec![1]), None)))
                .unwrap();
            // Producer stays open — an unbounded collect would hang forever;
            // the guard must reject BEFORE consuming.
            let stream = InputStream {
                media_urn: "media:enc=utf-8".to_string(),
                stream_meta: None,
                rx: InputRx::Unbounded(rx),
                unbounded: true,
                grants: None,
            };
            (stream, tx)
        };

        let (stream, _tx1) = make_unbounded();
        let err = stream.collect_bytes().await.expect_err("must refuse");
        assert!(err.to_string().contains("unbounded"), "{}", err);

        let (stream, _tx2) = make_unbounded();
        let err = stream.collect_items().await.expect_err("must refuse");
        assert!(err.to_string().contains("unbounded"), "{}", err);

        let (stream, _tx3) = make_unbounded();
        let err = stream.collect_value().await.expect_err("must refuse");
        assert!(err.to_string().contains("unbounded"), "{}", err);
    }

    // ── Live-feed transport resolution (13.2 §Reference Media, live family) ──

    /// A manifest whose test cap consumes a live feed: the arg is declared
    /// with the SYNTHETIC reference URN, is_sequence (a feed is a sequence
    /// of items), and a stdin source carrying the CONTENT urn — the exact
    /// file-path reference shape, unbounded.
    const LIVE_FEED_MANIFEST: &str = r#"{"name":"FeedCartridge","version":"1.0.0","channel":"release","registry_url":null,"description":"Live feed test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases":["identity"]},{"urn":"cap:drain;in=\"media:feed-frames\";out=\"media:fmt=json;record\"","title":"Drain","aliases":["drain"],"args":[{"media_urn":"media:live;synthetic","required":true,"is_sequence":true,"sources":[{"stdin":"media:feed-frames"}]}]}]}]}"#;

    fn live_feed_ctx(
        selector_arg_is_sequence: bool,
    ) -> (
        LiveFeedContext,
        Arc<std::sync::atomic::AtomicU64>,
        Arc<Mutex<Vec<crate::bifaci::live_feed::LiveFeedHandle>>>,
    ) {
        let manifest_json = if selector_arg_is_sequence {
            LIVE_FEED_MANIFEST.to_string()
        } else {
            LIVE_FEED_MANIFEST.replace("\"is_sequence\":true", "\"is_sequence\":false")
        };
        let manifest: CapManifest =
            serde_json::from_str(&manifest_json).expect("live-feed manifest must parse");
        let overruns = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let handles: Arc<Mutex<Vec<crate::bifaci::live_feed::LiveFeedHandle>>> =
            Arc::new(Mutex::new(Vec::new()));
        let ctx = LiveFeedContext::new(
            "cap:drain;in=\"media:feed-frames\";out=\"media:fmt=json;record\"",
            Some(manifest),
            Arc::clone(&overruns),
            Arc::clone(&handles),
        )
        .expect("live-feed context must build");
        (ctx, overruns, handles)
    }

    /// Feed a live-feed reference through the demux: STREAM_START with the
    /// reference URN, one CHUNK carrying the selector JSON, STREAM_END, END.
    fn send_live_reference(raw_tx: &crossbeam_channel::Sender<Frame>, rid: &MessageId, selector: &str) {
        let mut payload = Vec::new();
        ciborium::into_writer(
            &ciborium::Value::Text(selector.to_string()),
            &mut payload,
        )
        .unwrap();
        let checksum = Frame::compute_checksum(&payload);
        raw_tx
            .send(Frame::stream_start(
                rid.clone(),
                "ref".to_string(),
                "media:live;synthetic".to_string(),
                None,
            ))
            .unwrap();
        raw_tx
            .send(Frame::chunk(rid.clone(), "ref".to_string(), 0, payload, 0, checksum))
            .unwrap();
        raw_tx
            .send(Frame::stream_end(rid.clone(), "ref".to_string(), 1))
            .unwrap();
        raw_tx.send(Frame::end(rid.clone(), None)).unwrap();
    }

    // TEST8128: a live-feed reference resolves through the demux exactly like
    // a file path — the handler receives an UNBOUNDED SEQUENCE InputStream
    // labeled with the arg's stdin CONTENT urn, delivering the captured items
    // with seq/pts_us/capture_ts_us metadata, and the op is none the wiser.
    #[tokio::test]
    async fn test8128_live_feed_reference_resolves_to_unbounded_content_stream() {
        let (ctx, _overruns, handles) = live_feed_ctx(true);
        let (raw_tx, raw_rx) = crossbeam_channel::unbounded();
        let rid = MessageId::new_uuid();
        send_live_reference(&raw_tx, &rid, r#"{"params":{"items":5,"interval_ms":1,"item_bytes":4}}"#);
        drop(raw_tx);

        let mut package = demux_multi_stream(raw_rx, None, Some(ctx), None);
        let mut stream = package
            .recv()
            .await
            .expect("the resolved feed stream must be delivered")
            .expect("resolution must succeed");
        assert_eq!(stream.media_urn(), "media:feed-frames", "labeled with the CONTENT urn");
        assert!(stream.is_unbounded(), "a live feed makes no length promise (L16)");
        assert_eq!(
            stream
                .stream_meta()
                .and_then(|m| m.get("feed"))
                .and_then(|v| v.as_text()),
            Some("synthetic"),
            "STREAM_START meta carries the provider's format actuals"
        );

        let mut seqs = Vec::new();
        while let Some(item) = stream.recv().await {
            let (value, meta) = item.expect("items must deliver cleanly");
            assert!(matches!(value, ciborium::Value::Bytes(ref b) if b.len() == 4));
            let meta = meta.expect("every live item carries metadata");
            let seq = match meta.get("seq") {
                Some(ciborium::Value::Integer(i)) => u64::try_from(*i).unwrap(),
                other => panic!("item must carry integer seq, got {other:?}"),
            };
            assert!(meta.contains_key("pts_us"), "item carries pts_us");
            assert!(meta.contains_key("capture_ts_us"), "item carries capture_ts_us");
            seqs.push(seq);
        }
        assert_eq!(seqs, vec![0, 1, 2, 3, 4], "all items, in capture order");
        assert_eq!(handles.lock().unwrap().len(), 1, "the open feed registered its handle");
    }

    /// A TRANSPORT-BLIND cap (no explicit live arg): its main input consumes
    /// `main_in` via stdin. Used by the main-input fallback tests.
    fn blind_live_feed_ctx(
        main_in: &str,
    ) -> (
        LiveFeedContext,
        Arc<std::sync::atomic::AtomicU64>,
        Arc<Mutex<Vec<crate::bifaci::live_feed::LiveFeedHandle>>>,
    ) {
        let cap_urn = format!("cap:consume;in=\"{main_in}\";out=\"media:fmt=json;record\"");
        let manifest_json = format!(
            r#"{{"name":"BlindCartridge","version":"1.0.0","channel":"release","registry_url":null,"description":"Transport-blind live consumer","cap_groups":[{{"name":"default","caps":[{{"urn":"{}","title":"Consume","aliases":["consume"],"args":[{{"media_urn":"{main_in}","required":true,"is_sequence":true,"sources":[{{"stdin":"{main_in}"}}]}}]}}]}}]}}"#,
            cap_urn.replace('"', "\\\"")
        );
        let manifest: CapManifest =
            serde_json::from_str(&manifest_json).expect("blind manifest must parse");
        let overruns = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let handles: Arc<Mutex<Vec<crate::bifaci::live_feed::LiveFeedHandle>>> =
            Arc::new(Mutex::new(Vec::new()));
        let ctx = LiveFeedContext::new(
            &cap_urn,
            Some(manifest),
            Arc::clone(&overruns),
            Arc::clone(&handles),
        )
        .expect("blind live-feed context must build");
        (ctx, overruns, handles)
    }

    // TEST8137: main-input fallback — a cap with NO explicit reference arg
    // consumes a live source through its MAIN INPUT when the registered
    // provider's content urn conforms to it. This is what makes generic
    // machines (planned over the CONTENT type) valid live-source machines:
    // the engine forwards the reference, the capture cartridge resolves it,
    // and the op stays transport-blind.
    #[tokio::test]
    async fn test8137_main_input_fallback_resolves_live_reference() {
        let (ctx, _overruns, handles) = blind_live_feed_ctx("media:feed-frames");
        let (raw_tx, raw_rx) = crossbeam_channel::unbounded();
        let rid = MessageId::new_uuid();
        send_live_reference(&raw_tx, &rid, r#"{"params":{"items":3,"interval_ms":1,"item_bytes":4}}"#);
        drop(raw_tx);

        let mut package = demux_multi_stream(raw_rx, None, Some(ctx), None);
        let mut stream = package
            .recv()
            .await
            .expect("the resolved feed stream must be delivered")
            .expect("main-input resolution must succeed");
        assert_eq!(
            stream.media_urn(),
            "media:feed-frames",
            "labeled with the PROVIDER's content urn"
        );
        assert!(stream.is_unbounded());
        let mut count = 0;
        while let Some(item) = stream.recv().await {
            item.expect("items must deliver cleanly");
            count += 1;
        }
        assert_eq!(count, 3, "all captured items delivered through the main input");
        assert_eq!(handles.lock().unwrap().len(), 1, "the open feed registered its handle");
    }

    // TEST8138: main-input fallback content mismatch — a provider whose
    // content does not conform to the cap's main input is a hard error at
    // resolution ("this machine cannot consume that device"), never a
    // mislabeled stream.
    #[tokio::test]
    async fn test8138_main_input_fallback_content_mismatch_rejected() {
        let (ctx, _overruns, _handles) = blind_live_feed_ctx("media:audio-frames;pcm");
        let (raw_tx, raw_rx) = crossbeam_channel::unbounded();
        let rid = MessageId::new_uuid();
        send_live_reference(&raw_tx, &rid, "{}");
        drop(raw_tx);

        let mut package = demux_multi_stream(raw_rx, None, Some(ctx), None);
        let err = match package.recv().await.expect("the failure must be delivered") {
            Err(e) => e,
            Ok(_) => panic!("a non-conforming provider content must be rejected"),
        };
        assert!(
            err.to_string().contains("does not conform"),
            "the error names the conformance failure: {err}"
        );
    }

    // TEST8129: overrun under drop-oldest — a flooding feed with a lagging
    // consumer loses items ONLY at the capture edge, counts every loss, and
    // stamps the next delivered item with a gap marker so the discontinuity
    // is visible in-band. delivered + dropped always equals captured.
    #[tokio::test]
    async fn test8129_overrun_drop_oldest_counts_and_marks_gaps() {
        let (ctx, overruns, _handles) = live_feed_ctx(true);
        let (raw_tx, raw_rx) = crossbeam_channel::unbounded();
        let rid = MessageId::new_uuid();
        send_live_reference(
            &raw_tx,
            &rid,
            r#"{"params":{"items":50,"interval_ms":0,"item_bytes":4,"ring":2}}"#,
        );
        drop(raw_tx);

        let mut package = demux_multi_stream(raw_rx, None, Some(ctx), None);
        let mut stream = package.recv().await.unwrap().unwrap();
        // Lag: let the producer flood to completion before consuming — the
        // bounded delivery channel + tiny ring force capture-edge drops.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let mut delivered = 0u64;
        let mut dropped_via_gaps = 0u64;
        let mut last_seq: Option<u64> = None;
        while let Some(item) = stream.recv().await {
            let (_value, meta) = item.expect("drop-oldest never fails the stream");
            let meta = meta.unwrap();
            let seq = match meta.get("seq") {
                Some(ciborium::Value::Integer(i)) => u64::try_from(*i).unwrap(),
                _ => panic!("missing seq"),
            };
            if let Some(prev) = last_seq {
                assert!(seq > prev, "seq strictly increases across gaps");
            }
            if let Some(ciborium::Value::Map(gap)) = meta.get("gap") {
                let dropped = gap
                    .iter()
                    .find(|(k, _)| matches!(k, ciborium::Value::Text(t) if t == "dropped"))
                    .map(|(_, v)| match v {
                        ciborium::Value::Integer(i) => u64::try_from(*i).unwrap(),
                        _ => panic!("gap.dropped must be an integer"),
                    })
                    .expect("gap carries dropped count");
                assert!(dropped > 0, "a gap marker means real loss");
                dropped_via_gaps += dropped;
            }
            delivered += 1;
            last_seq = Some(seq);
        }
        assert!(delivered < 50, "a lagging consumer cannot receive everything");
        assert!(dropped_via_gaps > 0, "the loss is visible in-band");
        assert_eq!(
            delivered + dropped_via_gaps,
            50,
            "every captured item is either delivered or counted as dropped — nothing silent"
        );
        assert_eq!(
            overruns.load(std::sync::atomic::Ordering::Relaxed),
            dropped_via_gaps,
            "the runtime-wide overrun counter matches the in-band accounting"
        );
    }

    // TEST8130: on_overrun=fail — a pipeline that declares it needs every
    // frame gets a classified FEED_OVERRUN stream error instead of loss.
    #[tokio::test]
    async fn test8130_overrun_fail_ends_feed_with_classified_error() {
        let (ctx, _overruns, _handles) = live_feed_ctx(true);
        let (raw_tx, raw_rx) = crossbeam_channel::unbounded();
        let rid = MessageId::new_uuid();
        send_live_reference(
            &raw_tx,
            &rid,
            r#"{"on_overrun":"fail","params":{"items":50,"interval_ms":0,"item_bytes":4,"ring":2}}"#,
        );
        drop(raw_tx);

        let mut package = demux_multi_stream(raw_rx, None, Some(ctx), None);
        let mut stream = package.recv().await.unwrap().unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let mut saw_overrun_error = false;
        while let Some(item) = stream.recv().await {
            match item {
                Ok(_) => {}
                Err(e) => {
                    assert!(
                        e.to_string().contains("FEED_OVERRUN"),
                        "the failure names the overrun: {e}"
                    );
                    saw_overrun_error = true;
                }
            }
        }
        assert!(saw_overrun_error, "on_overrun=fail must surface the overrun as an error");
    }

    // TEST8131: max_items stop condition — the feed ends itself after
    // exactly N captured items; the stream ends cleanly (a run stops on its
    // own when its input ends, 15.2 §Runs Stop).
    #[tokio::test]
    async fn test8131_max_items_stop_condition_ends_feed() {
        let (ctx, _overruns, _handles) = live_feed_ctx(true);
        let (raw_tx, raw_rx) = crossbeam_channel::unbounded();
        let rid = MessageId::new_uuid();
        send_live_reference(
            &raw_tx,
            &rid,
            r#"{"stop":{"max_items":3},"params":{"items":1000,"interval_ms":1,"item_bytes":4}}"#,
        );
        drop(raw_tx);

        let mut package = demux_multi_stream(raw_rx, None, Some(ctx), None);
        let mut stream = package.recv().await.unwrap().unwrap();
        let mut delivered = 0;
        while let Some(item) = stream.recv().await {
            item.expect("items deliver cleanly");
            delivered += 1;
        }
        assert_eq!(delivered, 3, "the stop condition bounds the feed exactly");
    }

    // TEST8132: stop = close the tap — closing the feed's handle ends the
    // stream cleanly mid-capture; what was already captured drains, then the
    // stream ends without error (the drain path of a stopped run).
    #[tokio::test]
    async fn test8132_handle_close_stops_feed_and_drains() {
        let (ctx, _overruns, handles) = live_feed_ctx(true);
        let (raw_tx, raw_rx) = crossbeam_channel::unbounded();
        let rid = MessageId::new_uuid();
        send_live_reference(
            &raw_tx,
            &rid,
            r#"{"params":{"items":100000,"interval_ms":2,"item_bytes":4}}"#,
        );
        drop(raw_tx);

        let mut package = demux_multi_stream(raw_rx, None, Some(ctx), None);
        let mut stream = package.recv().await.unwrap().unwrap();
        // Consume a couple of live items, then stop.
        let first = stream.recv().await.expect("live item").expect("clean");
        assert!(matches!(first.0, ciborium::Value::Bytes(_)));
        handles.lock().unwrap()[0].close();

        // The stream must END (not hang, not error) — drain then done.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match stream.recv().await {
                Some(item) => {
                    item.expect("drained items are clean");
                    assert!(
                        std::time::Instant::now() < deadline,
                        "a closed feed must end promptly"
                    );
                }
                None => break,
            }
        }
    }

    // TEST8133: a live-feed arg declared is_sequence=false is a contract
    // violation — a feed is an unbounded SEQUENCE — and fails hard at
    // resolution, never delivering a mislabeled stream.
    #[tokio::test]
    async fn test8133_scalar_live_feed_arg_rejected() {
        let (ctx, _overruns, _handles) = live_feed_ctx(false);
        let (raw_tx, raw_rx) = crossbeam_channel::unbounded();
        let rid = MessageId::new_uuid();
        send_live_reference(&raw_tx, &rid, "{}");
        drop(raw_tx);

        let mut package = demux_multi_stream(raw_rx, None, Some(ctx), None);
        let err = match package.recv().await.expect("the failure must be delivered") {
            Err(e) => e,
            Ok(_) => panic!("a scalar live-feed arg must be rejected"),
        };
        assert!(err.to_string().contains("is_sequence"), "{err}");
    }

    // TEST8134: an unparseable selector is a hard error — never a silent
    // all-defaults feed.
    #[tokio::test]
    async fn test8134_invalid_selector_rejected() {
        let (ctx, _overruns, _handles) = live_feed_ctx(true);
        let (raw_tx, raw_rx) = crossbeam_channel::unbounded();
        let rid = MessageId::new_uuid();
        send_live_reference(&raw_tx, &rid, "{not json");
        drop(raw_tx);

        let mut package = demux_multi_stream(raw_rx, None, Some(ctx), None);
        let err = match package.recv().await.expect("the failure must be delivered") {
            Err(e) => e,
            Ok(_) => panic!("garbage selectors must be rejected"),
        };
        assert!(err.to_string().contains("selector"), "{err}");
    }

    // TEST8136: unknown selector fields are rejected at every nesting level
    // — a misspelled stop condition (`duration` for `duration_ms`) silently
    // ignored would run an unbounded feed the caller meant to bound.
    #[tokio::test]
    async fn test8136_unknown_selector_fields_rejected() {
        for bad in [
            r#"{"devise": "mic0"}"#,
            r#"{"stop": {"duration": 1000}}"#,
            r#"{"stop": {"max_item": 3}}"#,
        ] {
            let (ctx, _overruns, _handles) = live_feed_ctx(true);
            let (raw_tx, raw_rx) = crossbeam_channel::unbounded();
            let rid = MessageId::new_uuid();
            send_live_reference(&raw_tx, &rid, bad);
            drop(raw_tx);

            let mut package = demux_multi_stream(raw_rx, None, Some(ctx), None);
            let err = match package.recv().await.expect("the failure must be delivered") {
                Err(e) => e,
                Ok(_) => panic!("unknown selector field must be rejected: {bad}"),
            };
            assert!(err.to_string().contains("selector"), "{bad}: {err}");
        }
    }

    // TEST7052: Input consumption emits batched CREDIT grants — roughly one grant per half-window consumed, not one per chunk.
    #[tokio::test]
    async fn test7052_input_grants_are_batched() {
        let (grant_tx, mut grant_rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();
        let sender: Arc<dyn FrameSender> = Arc::new(ChannelFrameSender {
            tx: grant_tx,
            drops: Arc::new(crate::bifaci::stats::DropCounters::new()),
        });
        let rid = MessageId::new_uuid();
        let (raw_tx, raw_rx) = crossbeam_channel::unbounded();

        // Stream 16 chunks through a credited demux with window 8.
        let mk_chunk = |i: u64| {
            let mut payload = Vec::new();
            ciborium::into_writer(&ciborium::Value::Bytes(vec![i as u8]), &mut payload).unwrap();
            let checksum = Frame::compute_checksum(&payload);
            Frame::chunk(rid.clone(), "s1".to_string(), i, payload, i, checksum)
        };
        let ss = Frame::stream_start(
            rid.clone(),
            "s1".to_string(),
            "media:enc=utf-8".to_string(),
            Some(false),
        );
        raw_tx.send(ss).unwrap();
        // A CONFORMING producer: first burst = the initial window (8)...
        for i in 0..8u64 {
            raw_tx.send(mk_chunk(i)).unwrap();
        }

        let mut package = demux_multi_stream(
            raw_rx,
            None,
            None,
            Some(InputCreditContext {
                sender,
                rid: rid.clone(),
                xid: None,
                initial_credit: 8,
            }),
        );
        let mut stream = package.recv().await.unwrap().unwrap();
        // Let the demux thread forward ALL pre-queued chunks into the
        // handler's channel before consuming — recv() then never hits an
        // empty channel, so no flush-before-block fires and batching is
        // deterministic. (Without the drain pause, the consumer can outpace
        // the demux thread and legally trigger sub-batch flushes.)
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let mut consumed = 0;
        for _ in 0..8 {
            stream.recv().await.unwrap().unwrap();
            consumed += 1;
        }
        // ...then the rest only after consumption granted more window.
        for i in 8..16u64 {
            raw_tx.send(mk_chunk(i)).unwrap();
        }
        raw_tx
            .send(Frame::stream_end(rid.clone(), "s1".to_string(), 16))
            .unwrap();
        raw_tx.send(Frame::end(rid.clone(), None)).unwrap();
        drop(raw_tx);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        while let Some(item) = stream.recv().await {
            item.unwrap();
            consumed += 1;
        }
        assert_eq!(consumed, 16);

        let mut grants = Vec::new();
        while let Ok(f) = grant_rx.try_recv() {
            assert_eq!(f.frame_type, FrameType::Credit);
            grants.push(f.credit_count().unwrap());
        }
        // With both phases fully drained into the handler's channel before
        // consumption, recv() never blocks mid-phase: no flushes fire and
        // batching is fully deterministic — four grants of exactly the batch
        // size (window/2 = 4), one per 4 consumed chunks.
        assert_eq!(
            grants,
            vec![4, 4, 4, 4],
            "drained consumption must batch deterministically at window/2"
        );
        // Note: 16 chunks arrive against an 8-window with grants extending it
        // as the handler consumes — the shared window accounting is what lets
        // the producer legally exceed the initial window (L10).
    }

    // TEST7063: A receiver flushes pending sub-batch grants before blocking on an empty input — progress is guaranteed even when the sender's window is smaller than the receiver's grant batch threshold.
    #[tokio::test]
    async fn test7063_pending_grants_flush_before_blocking() {
        let (grant_tx, mut grant_rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();
        let sender: Arc<dyn FrameSender> = Arc::new(ChannelFrameSender {
            tx: grant_tx,
            drops: Arc::new(crate::bifaci::stats::DropCounters::new()),
        });
        let rid = MessageId::new_uuid();
        let (raw_tx, raw_rx) = crossbeam_channel::unbounded();

        // Receiver negotiated a 32 window → batch threshold 16. The sender
        // (a different link) has a window of only 8: it emits 8 chunks and
        // stalls, BELOW the receiver's batch threshold.
        raw_tx
            .send(Frame::stream_start(
                rid.clone(),
                "s1".to_string(),
                "media:enc=utf-8".to_string(),
                Some(false),
            ))
            .unwrap();
        for i in 0..8u64 {
            let mut payload = Vec::new();
            ciborium::into_writer(&ciborium::Value::Bytes(vec![i as u8]), &mut payload).unwrap();
            let checksum = Frame::compute_checksum(&payload);
            raw_tx
                .send(Frame::chunk(
                    rid.clone(),
                    "s1".to_string(),
                    i,
                    payload,
                    i,
                    checksum,
                ))
                .unwrap();
        }
        // Channel stays open — the sender is stalled, not finished.

        let mut package = demux_multi_stream(
            raw_rx,
            None,
            None,
            Some(InputCreditContext {
                sender,
                rid: rid.clone(),
                xid: None,
                initial_credit: 32,
            }),
        );
        let mut stream = package.recv().await.unwrap().unwrap();

        // Consume all 8 available items, then attempt the 9th — which blocks
        // on the empty channel and MUST flush the pending 8-chunk grant first.
        let consumer = tokio::spawn(async move {
            for _ in 0..8 {
                stream.recv().await.unwrap().unwrap();
            }
            // Blocks (sender stalled) — but only AFTER flushing grants.
            let _ = stream.recv().await;
        });

        // The flushed grant must arrive even though 8 < batch(16).
        let grant = tokio::time::timeout(std::time::Duration::from_secs(2), grant_rx.recv())
            .await
            .expect("pending grants must flush before blocking (L10 corollary)")
            .expect("grant frame");
        assert_eq!(grant.frame_type, FrameType::Credit);
        assert_eq!(
            grant.credit_count(),
            Some(8),
            "the full pending consumption is granted on flush"
        );

        consumer.abort();
    }

    // TEST7053: A chunk received beyond the granted window is a fatal CREDIT_VIOLATION surfaced to the consumer (L12).
    #[tokio::test]
    async fn test7053_over_window_chunk_is_credit_violation() {
        let (grant_tx, _grant_rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();
        let sender: Arc<dyn FrameSender> = Arc::new(ChannelFrameSender {
            tx: grant_tx,
            drops: Arc::new(crate::bifaci::stats::DropCounters::new()),
        });
        let rid = MessageId::new_uuid();
        let (raw_tx, raw_rx) = crossbeam_channel::unbounded();

        let ss = Frame::stream_start(
            rid.clone(),
            "s1".to_string(),
            "media:enc=utf-8".to_string(),
            Some(false),
        );
        raw_tx.send(ss).unwrap();
        // Window is 2; a misbehaving sender pushes 3 chunks with no grants
        // possible (nothing consumed yet).
        for i in 0..3u64 {
            let mut payload = Vec::new();
            ciborium::into_writer(&ciborium::Value::Bytes(vec![i as u8]), &mut payload).unwrap();
            let checksum = Frame::compute_checksum(&payload);
            raw_tx
                .send(Frame::chunk(
                    rid.clone(),
                    "s1".to_string(),
                    i,
                    payload,
                    i,
                    checksum,
                ))
                .unwrap();
        }
        raw_tx.send(Frame::end(rid.clone(), None)).unwrap();
        drop(raw_tx);

        let mut package = demux_multi_stream(
            raw_rx,
            None,
            None,
            Some(InputCreditContext {
                sender,
                rid,
                xid: None,
                initial_credit: 2,
            }),
        );
        let mut stream = package.recv().await.unwrap().unwrap();
        // Let the demux drain all three pre-queued chunks before anything is
        // consumed — no grant can extend the window, so the third chunk is
        // deterministically a violation.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        // First two chunks are within the window.
        assert!(stream.recv().await.unwrap().is_ok());
        assert!(stream.recv().await.unwrap().is_ok());
        // The third is the violation.
        let err = stream
            .recv()
            .await
            .expect("violation must be surfaced, not silently dropped")
            .expect_err("over-window chunk is a protocol error");
        assert!(
            err.to_string().contains("CREDIT_VIOLATION"),
            "error must carry the CREDIT_VIOLATION code: {}",
            err
        );
    }

    // =========================================================================
    // Reusable test Op structs
    // =========================================================================

    /// Test Op: emits a fixed byte value
    struct EmitBytesOp {
        data: Vec<u8>,
    }
    #[async_trait]
    impl Op<()> for EmitBytesOp {
        async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
            let req: Arc<Request> = wet
                .get_required(WET_KEY_REQUEST)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let _input = req
                .take_input()
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            req.output()
                .start(false, None)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            req.output()
                .emit_cbor(&ciborium::Value::Bytes(self.data.clone()))
                .await
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            Ok(())
        }
        fn metadata(&self) -> OpMetadata {
            OpMetadata::builder("EmitBytesOp").build()
        }
    }

    /// Test Op: echoes all input chunks to output, optionally records received bytes
    struct EchoOp {
        received: Option<Arc<Mutex<Vec<u8>>>>,
    }
    impl Default for EchoOp {
        fn default() -> Self {
            Self { received: None }
        }
    }
    #[async_trait]
    impl Op<()> for EchoOp {
        async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
            let req: Arc<Request> = wet
                .get_required(WET_KEY_REQUEST)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let mut input = req
                .take_input()
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            req.output()
                .start(false, None)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let mut total = Vec::new();
            while let Some(stream) = input.recv().await {
                let mut stream = stream.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                while let Some(chunk) = stream.recv_data().await {
                    let chunk = chunk.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                    if let ciborium::Value::Bytes(ref b) = chunk {
                        total.extend(b);
                    }
                    req.output()
                        .emit_cbor(&chunk)
                        .await
                        .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                }
            }
            if let Some(ref received) = self.received {
                *received.lock().unwrap() = total;
            }
            Ok(())
        }
        fn metadata(&self) -> OpMetadata {
            OpMetadata::builder("EchoOp").build()
        }
    }

    /// Test Op: echoes input then appends a tag byte
    struct EchoTagOp {
        tag: Vec<u8>,
    }
    #[async_trait]
    impl Op<()> for EchoTagOp {
        async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
            let req: Arc<Request> = wet
                .get_required(WET_KEY_REQUEST)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let mut input = req
                .take_input()
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            req.output()
                .start(false, None)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            while let Some(stream) = input.recv().await {
                let mut stream = stream.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                while let Some(chunk) = stream.recv_data().await {
                    let chunk = chunk.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                    req.output()
                        .emit_cbor(&chunk)
                        .await
                        .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                }
            }
            req.output()
                .emit_cbor(&ciborium::Value::Bytes(self.tag.clone()))
                .await
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            Ok(())
        }
        fn metadata(&self) -> OpMetadata {
            OpMetadata::builder("EchoTagOp").build()
        }
    }

    /// Test Op: extracts CBOR "value" key from args, stores in shared state
    struct ExtractValueOp {
        received: Arc<Mutex<Vec<u8>>>,
    }
    #[async_trait]
    impl Op<()> for ExtractValueOp {
        async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
            let req: Arc<Request> = wet
                .get_required(WET_KEY_REQUEST)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let input = req
                .take_input()
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            req.output()
                .start(false, None)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let bytes = input
                .collect_all_bytes()
                .await
                .map_err(|e| OpError::ExecutionFailed(format!("Stream error: {}", e)))?;
            let cbor_val: ciborium::Value = ciborium::from_reader(&bytes[..])
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            if let ciborium::Value::Array(args) = cbor_val {
                for arg in args {
                    if let ciborium::Value::Map(map) = arg {
                        for (k, v) in map {
                            if let (ciborium::Value::Text(key), ciborium::Value::Bytes(b)) = (k, v)
                            {
                                if key == "value" {
                                    *self.received.lock().unwrap() = b.clone();
                                    req.output()
                                        .emit_cbor(&ciborium::Value::Bytes(b))
                                        .await
                                        .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        }
        fn metadata(&self) -> OpMetadata {
            OpMetadata::builder("ExtractValueOp").build()
        }
    }

    /// Test Op: no-op (does nothing)
    #[derive(Default)]
    struct NoOpOp;
    #[async_trait]
    impl Op<()> for NoOpOp {
        async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
            let req: Arc<Request> = wet
                .get_required(WET_KEY_REQUEST)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let _input = req
                .take_input()
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            Ok(())
        }
        fn metadata(&self) -> OpMetadata {
            OpMetadata::builder("NoOpOp").build()
        }
    }

    /// Helper: invoke a factory-produced Op with test input/output
    async fn invoke_op(
        factory: &OpFactory,
        input: InputPackage,
        output: OutputStream,
    ) -> Result<(), RuntimeError> {
        let op = factory();
        let peer: Arc<dyn PeerInvoker> = Arc::new(NoPeerInvoker);
        dispatch_op(op, input, output, peer).await
    }

    /// Create an InputPackage from a list of streams for testing.
    /// Each stream is a (media_urn, data_bytes) pair.
    /// The data is CBOR-encoded as Value::Bytes in a CHUNK frame.
    fn test_input_package(streams: &[(&str, &[u8])]) -> InputPackage {
        let (raw_tx, raw_rx) = crossbeam_channel::unbounded();
        let request_id = MessageId::new_uuid();

        for (media_urn, data) in streams {
            let stream_id = uuid::Uuid::new_v4().to_string();
            raw_tx
                .send(Frame::stream_start(
                    request_id.clone(),
                    stream_id.clone(),
                    media_urn.to_string(),
                    None,
                ))
                .ok();

            // Encode data as CBOR Bytes and wrap in CHUNK
            let value = ciborium::Value::Bytes(data.to_vec());
            let mut cbor = Vec::new();
            ciborium::into_writer(&value, &mut cbor).unwrap();
            let checksum = Frame::compute_checksum(&cbor);
            raw_tx
                .send(Frame::chunk(
                    request_id.clone(),
                    stream_id.clone(),
                    0,
                    cbor,
                    0,
                    checksum,
                ))
                .ok();
            raw_tx
                .send(Frame::stream_end(request_id.clone(), stream_id, 1))
                .ok();
        }
        raw_tx.send(Frame::end(request_id, None)).ok();
        drop(raw_tx);

        demux_multi_stream(raw_rx, None, None, None)
    }

    /// Create an OutputStream backed by a channel for testing.
    /// Returns (OutputStream, frame_receiver) so tests can inspect output.
    fn test_output_stream() -> (OutputStream, tokio::sync::mpsc::UnboundedReceiver<Frame>) {
        let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel();
        let sender: Arc<dyn FrameSender> = Arc::new(ChannelFrameSender {
            tx: out_tx,
            drops: Arc::new(crate::bifaci::stats::DropCounters::new()),
        });
        let output = OutputStream::new(
            sender,
            uuid::Uuid::new_v4().to_string(),
            "*".to_string(),
            MessageId::new_uuid(),
            None,
            Limits::default().max_chunk,
        );
        (output, out_rx)
    }

    /// Helper function to create a Cap for tests
    fn create_test_cap(urn_str: &str, title: &str, command: &str, args: Vec<CapArg>) -> Cap {
        let urn = CapUrn::from_string(urn_str).expect("Invalid cap URN");
        Cap::with_args(urn, title.to_string(), vec![command.to_string()], args)
    }

    /// Mock registry for tests - stores caps and returns them by URN lookup
    struct MockRegistry {
        caps: HashMap<String, Cap>,
    }

    impl MockRegistry {
        fn new() -> Self {
            Self {
                caps: HashMap::new(),
            }
        }

        fn add_cap(&mut self, cap: Cap) {
            self.caps.insert(cap.urn_string(), cap);
        }

        fn get(&self, urn_str: &str) -> Option<&Cap> {
            // Normalize the URN for lookup
            let normalized = CapUrn::from_string(urn_str).ok()?.to_string();
            self.caps
                .iter()
                .find(|(k, _)| {
                    if let Ok(k_norm) = CapUrn::from_string(k) {
                        k_norm.to_string() == normalized
                    } else {
                        false
                    }
                })
                .map(|(_, v)| v)
        }

        /// Create a registry with common test caps
        fn with_test_caps() -> Self {
            let mut registry = Self::new();

            // Add common test caps used across tests
            registry.add_cap(create_test_cap(
                r#"cap:in=media:void;test;out=media:void"#,
                "Test",
                "test",
                vec![],
            ));

            registry.add_cap(create_test_cap(
                r#"cap:in=media:;process;out=media:void"#,
                "Process",
                "process",
                vec![],
            ));

            registry.add_cap(create_test_cap(
                r#"cap:in="media:enc=utf-8;string";test;out=media:"#,
                "Test String",
                "test",
                vec![],
            ));

            registry.add_cap(create_test_cap(
                r#"cap:in=media:;test;out=media:"#,
                "Test Wildcard",
                "test",
                vec![],
            ));

            registry.add_cap(create_test_cap(
                r#"cap:in="media:enc=utf-8;model-spec";infer;out=media:"#,
                "Infer",
                "infer",
                vec![],
            ));

            registry.add_cap(create_test_cap(
                r#"cap:in="media:ext=pdf";process;out=media:"#,
                "Process PDF",
                "process",
                vec![],
            ));

            registry
        }
    }

    /// Helper to test file-path array conversion: returns array of file bytes
    fn test_filepath_array_conversion(
        cap: &Cap,
        cli_args: &[String],
        runtime: &CartridgeRuntime,
    ) -> Vec<Vec<u8>> {
        // Extract raw argument value
        let (raw_value, _) = runtime
            .extract_arg_value(&cap.args[0], cli_args, &mut || Ok(None))
            .unwrap();

        // Build CBOR payload
        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text(cap.args[0].media_urn.clone()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(raw_value.unwrap()),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        // Do file-path conversion
        let result =
            extract_effective_payload(&payload, Some("application/cbor"), cap, true).unwrap();

        // Decode and extract array of bytes
        let result_cbor: ciborium::Value = ciborium::from_reader(&result[..]).unwrap();
        let result_array = match result_cbor {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };
        let result_map = match &result_array[0] {
            ciborium::Value::Map(m) => m,
            _ => panic!("Expected map"),
        };
        let value_array = result_map
            .iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| match v {
                ciborium::Value::Array(arr) => arr.clone(),
                _ => panic!("Expected array"),
            })
            .unwrap();

        // Extract bytes from each element
        value_array
            .iter()
            .map(|v| match v {
                ciborium::Value::Bytes(b) => b.clone(),
                _ => panic!("Expected bytes in array"),
            })
            .collect()
    }

    /// Helper to test file-path conversion: takes Cap, CLI args, and returns converted bytes
    fn test_filepath_conversion(
        cap: &Cap,
        cli_args: &[String],
        runtime: &CartridgeRuntime,
    ) -> Vec<u8> {
        // Extract raw argument value
        let (raw_value, _) = runtime
            .extract_arg_value(&cap.args[0], cli_args, &mut || Ok(None))
            .unwrap();

        // Build CBOR payload
        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text(cap.args[0].media_urn.clone()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(raw_value.unwrap()),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        // Do file-path conversion
        let result =
            extract_effective_payload(&payload, Some("application/cbor"), cap, true).unwrap();

        // Decode and extract bytes
        let result_cbor: ciborium::Value = ciborium::from_reader(&result[..]).unwrap();
        let result_array = match result_cbor {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };
        let result_map = match &result_array[0] {
            ciborium::Value::Map(m) => m,
            _ => panic!("Expected map"),
        };
        result_map
            .iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| match v {
                ciborium::Value::Bytes(b) => b.clone(),
                _ => panic!("Expected bytes"),
            })
            .unwrap()
    }

    /// Helper function to create a CapManifest for tests
    fn create_test_manifest(
        name: &str,
        version: &str,
        description: &str,
        mut caps: Vec<Cap>,
    ) -> CapManifest {
        // Always append CAP_IDENTITY at the end - cartridges must declare it
        // (Appending instead of prepending to avoid breaking tests that reference caps[0])
        let identity_urn = crate::CapUrn::from_string(crate::standard::caps::CAP_IDENTITY).unwrap();
        let identity_cap = Cap::new(
            identity_urn,
            "Identity".to_string(),
            vec!["identity".to_string()],
        );
        caps.push(identity_cap);

        CapManifest::new(
            name.to_string(),
            version.to_string(),
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            description.to_string(),
            vec![crate::CapGroup {
                name: "default".to_string(),
                caps,
                adapter_urns: Vec::new(),
            }],
        )
    }

    /// Test manifest JSON with identity and a test cap.
    /// Uses cap_groups format. `cap:test` is a legal fully-generic declared transform.
    const TEST_MANIFEST: &str = r#"{"name":"TestCartridge","version":"1.0.0","channel":"release","registry_url":null,"description":"Test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"]},{"urn":"cap:test","title":"Test","aliases": ["test"]}]}]}"#;

    /// Valid manifest with proper in/out specs for tests that need parsed CapManifest
    const VALID_MANIFEST: &str = r#"{"name":"TestCartridge","version":"1.0.0","channel":"release","registry_url":null,"description":"Test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"]},{"urn":"cap:in=media:void;test;out=media:void","title":"Test","aliases": ["test"]}],"adapter_urns":[]}]}"#;

    // TEST248: Test register_op and find_handler by exact cap URN
    #[test]
    fn test248_register_and_find_handler() {
        let mut runtime = CartridgeRuntime::new(TEST_MANIFEST.as_bytes());
        runtime.register_op("cap:in=media:;test;out=media:", || {
            Box::new(EmitBytesOp {
                data: b"result".to_vec(),
            })
        });
        assert!(runtime
            .find_handler("cap:in=media:;test;out=media:")
            .is_some());
    }

    // TEST249: Test register_op handler echoes bytes directly
    #[tokio::test]
    async fn test249_raw_handler() {
        let mut runtime = CartridgeRuntime::new(TEST_MANIFEST.as_bytes());
        let received: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);

        runtime.register_op("cap:raw", move || {
            Box::new(EchoOp {
                received: Some(Arc::clone(&received_clone)),
            }) as Box<dyn Op<()>>
        });

        let factory = runtime.find_handler("cap:raw").unwrap();
        let input = test_input_package(&[("media:", b"echo this")]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&factory, input, output).await.unwrap();
        assert_eq!(
            &*received.lock().unwrap(),
            b"echo this",
            "raw handler must echo payload"
        );
    }

    // TEST250: Test Op handler collects input and processes it
    #[tokio::test]
    async fn test250_typed_handler_deserialization() {
        /// Test Op: parses JSON, extracts "key" field, emits as bytes
        struct JsonKeyOp {
            received: Arc<Mutex<Vec<u8>>>,
        }
        #[async_trait]
        impl Op<()> for JsonKeyOp {
            async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
                let req: Arc<Request> = wet
                    .get_required(WET_KEY_REQUEST)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let input = req
                    .take_input()
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let all_bytes = input
                    .collect_all_bytes()
                    .await
                    .map_err(|e| OpError::ExecutionFailed(format!("Failed to collect: {}", e)))?;
                let json: serde_json::Value = serde_json::from_slice(&all_bytes)
                    .map_err(|e| OpError::ExecutionFailed(format!("Bad JSON: {}", e)))?;
                let value = json
                    .get("key")
                    .and_then(|v| v.as_str())
                    .unwrap_or("missing");
                let bytes = value.as_bytes();
                req.output()
                    .start(false, None)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                req.output()
                    .emit_cbor(&ciborium::Value::Bytes(bytes.to_vec()))
                    .await
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                *self.received.lock().unwrap() = bytes.to_vec();
                Ok(())
            }
            fn metadata(&self) -> OpMetadata {
                OpMetadata::builder("JsonKeyOp").build()
            }
        }

        let mut runtime = CartridgeRuntime::new(TEST_MANIFEST.as_bytes());
        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);

        runtime.register_op("cap:test", move || {
            Box::new(JsonKeyOp {
                received: Arc::clone(&received_clone),
            }) as Box<dyn Op<()>>
        });

        let factory = runtime.find_handler("cap:test").unwrap();
        let input = test_input_package(&[("media:", b"{\"key\":\"hello\"}")]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&factory, input, output).await.unwrap();
        assert_eq!(&*received.lock().unwrap(), b"hello");
    }

    // TEST251: Test Op handler propagates errors through RuntimeError::Handler
    #[tokio::test]
    async fn test251_typed_handler_rejects_invalid_json() {
        /// Op that parses JSON — fails on invalid input
        struct JsonParseOp;
        #[async_trait]
        impl Op<()> for JsonParseOp {
            async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
                let req: Arc<Request> = wet
                    .get_required(WET_KEY_REQUEST)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let input = req
                    .take_input()
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let all_bytes = input
                    .collect_all_bytes()
                    .await
                    .map_err(|e| OpError::ExecutionFailed(format!("Failed to collect: {}", e)))?;
                let _: serde_json::Value = serde_json::from_slice(&all_bytes)
                    .map_err(|e| OpError::ExecutionFailed(format!("Bad JSON: {}", e)))?;
                Ok(())
            }
            fn metadata(&self) -> OpMetadata {
                OpMetadata::builder("JsonParseOp").build()
            }
        }

        let mut runtime = CartridgeRuntime::new(TEST_MANIFEST.as_bytes());
        runtime.register_op("cap:test", || Box::new(JsonParseOp));

        let factory = runtime.find_handler("cap:test").unwrap();
        let input = test_input_package(&[("media:", b"not json {{{{")]);
        let (output, _out_rx) = test_output_stream();
        let result = invoke_op(&factory, input, output).await;
        assert!(result.is_err(), "Invalid JSON must produce error");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("JSON"),
            "Error should mention JSON: {}",
            err_msg
        );
    }

    // TEST252: Test find_handler returns None for unregistered cap URNs
    #[test]
    fn test252_find_handler_unknown_cap() {
        let runtime = CartridgeRuntime::new(TEST_MANIFEST.as_bytes());
        assert!(runtime.find_handler("cap:nonexistent").is_none());
    }

    // TEST253: Test OpFactory can be cloned via Arc and sent across tasks (Send + Sync)
    #[tokio::test]
    async fn test253_handler_is_send_sync() {
        let mut runtime = CartridgeRuntime::new(TEST_MANIFEST.as_bytes());
        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);

        runtime.register_op("cap:threaded", move || {
            let r = Arc::clone(&received_clone);
            Box::new(EmitAndRecordOp {
                data: b"done".to_vec(),
                received: r,
            }) as Box<dyn Op<()>>
        });

        /// Test Op: emits fixed bytes and records in shared state
        struct EmitAndRecordOp {
            data: Vec<u8>,
            received: Arc<Mutex<Vec<u8>>>,
        }
        #[async_trait]
        impl Op<()> for EmitAndRecordOp {
            async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
                let req: Arc<Request> = wet
                    .get_required(WET_KEY_REQUEST)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let _input = req
                    .take_input()
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                req.output()
                    .start(false, None)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                req.output()
                    .emit_cbor(&ciborium::Value::Bytes(self.data.clone()))
                    .await
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                *self.received.lock().unwrap() = self.data.clone();
                Ok(())
            }
            fn metadata(&self) -> OpMetadata {
                OpMetadata::builder("EmitAndRecordOp").build()
            }
        }

        let factory = runtime.find_handler("cap:threaded").unwrap();
        let factory_clone = Arc::clone(&factory);

        let handle = tokio::spawn(async move {
            let input = test_input_package(&[("media:", b"{}")]);
            let (output, _out_rx) = test_output_stream();
            invoke_op(&factory_clone, input, output).await.unwrap();
        });

        handle.await.unwrap();
        assert_eq!(&*received.lock().unwrap(), b"done");
    }

    // TEST254: Test NoPeerInvoker always returns PeerRequest error
    #[test]
    fn test254_no_peer_invoker() {
        let no_peer = NoPeerInvoker;
        let result = no_peer.call("cap:test");
        assert!(result.is_err());
        match result {
            Err(RuntimeError::PeerRequest(msg)) => {
                assert!(
                    msg.contains("not supported"),
                    "error must indicate peer not supported"
                );
            }
            _ => panic!("Expected PeerRequest error"),
        }
    }

    // TEST255: Test NoPeerInvoker call_with_bytes also returns error
    #[tokio::test]
    async fn test255_no_peer_invoker_with_arguments() {
        let no_peer = NoPeerInvoker;
        let result = no_peer
            .call_with_bytes("cap:test", &[("media:test", b"value".as_slice())])
            .await;
        assert!(result.is_err());
    }

    // TEST256: Test CartridgeRuntime::with_manifest_json stores manifest data and parses when valid
    #[test]
    fn test256_with_manifest_json() {
        // TEST_MANIFEST uses cap_groups format with identity + test cap.
        // "cap:test" has no in/out tags; CapUrn defaults both to media: (wildcard).
        let runtime_basic = CartridgeRuntime::with_manifest_json(TEST_MANIFEST);
        assert!(!runtime_basic.manifest_data.is_empty());
        assert!(
            runtime_basic.manifest.is_some(),
            "TEST_MANIFEST must parse: cap:in=media:;out=media:;test is valid (in/out default to media:)"
        );
        let manifest = runtime_basic.manifest.unwrap();
        assert_eq!(
            manifest.all_caps().len(),
            2,
            "Two caps declared: identity + test"
        );

        // VALID_MANIFEST has proper in/out specs
        let runtime_valid = CartridgeRuntime::with_manifest_json(VALID_MANIFEST);
        assert!(!runtime_valid.manifest_data.is_empty());
        assert!(
            runtime_valid.manifest.is_some(),
            "VALID_MANIFEST must parse into CapManifest"
        );
    }

    // TEST257: Test CartridgeRuntime::new with invalid JSON still creates runtime (manifest is None)
    #[test]
    fn test257_new_with_invalid_json() {
        let runtime = CartridgeRuntime::new(b"not json");
        assert!(!runtime.manifest_data.is_empty());
        assert!(
            runtime.manifest.is_none(),
            "invalid JSON should leave manifest as None"
        );
    }

    // TEST258: Test CartridgeRuntime::with_manifest creates runtime with valid manifest data
    #[test]
    fn test258_with_manifest_struct() {
        let manifest: crate::bifaci::manifest::CapManifest =
            serde_json::from_str(VALID_MANIFEST).unwrap();
        let runtime = CartridgeRuntime::with_manifest(manifest);
        assert!(!runtime.manifest_data.is_empty());
        assert!(runtime.manifest.is_some());
    }

    // TEST259: Test extract_effective_payload with non-CBOR content_type returns raw payload unchanged
    #[test]
    fn test259_extract_effective_payload_non_cbor() {
        let registry = MockRegistry::with_test_caps();
        let cap = registry
            .get(r#"cap:in=media:void;test;out=media:void"#)
            .unwrap();
        let payload = b"raw data";
        let result =
            extract_effective_payload(payload, Some("application/json"), cap, true).unwrap();
        assert_eq!(result, payload, "non-CBOR must return raw payload");
    }

    // TEST260: Test extract_effective_payload with None content_type returns raw payload unchanged
    #[test]
    fn test260_extract_effective_payload_no_content_type() {
        let registry = MockRegistry::with_test_caps();
        let cap = registry
            .get(r#"cap:in=media:void;test;out=media:void"#)
            .unwrap();
        let payload = b"raw data";
        let result = extract_effective_payload(payload, None, cap, true).unwrap();
        assert_eq!(result, payload);
    }

    // TEST261: Test extract_effective_payload with CBOR content extracts matching argument value
    #[test]
    fn test261_extract_effective_payload_cbor_match() {
        // Build CBOR arguments: [{media_urn: "media:enc=utf-8;string", value: bytes("hello")}]
        let args = ciborium::Value::Array(vec![ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:enc=utf-8;string".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(b"hello".to_vec()),
            ),
        ])]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        // The cap URN has in=media:enc=utf-8;string
        let registry = MockRegistry::with_test_caps();
        let cap = registry
            .get(r#"cap:in="media:enc=utf-8;string";test;out=media:"#)
            .unwrap();
        let result = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            cap,
            false, // CBOR mode - tests pass CBOR payloads directly
        )
        .unwrap();

        // NEW REGIME: Result is full CBOR array, handler must parse and extract
        let result_cbor: ciborium::Value = ciborium::from_reader(&result[..]).unwrap();
        let result_array = match result_cbor {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };

        // Extract value from matching argument
        let mut found_value = None;
        for arg in result_array {
            if let ciborium::Value::Map(map) = arg {
                for (k, v) in map {
                    if let ciborium::Value::Text(key) = k {
                        if key == "value" {
                            if let ciborium::Value::Bytes(b) = v {
                                found_value = Some(b);
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(
            found_value,
            Some(b"hello".to_vec()),
            "Handler extracts value from CBOR array"
        );
    }

    // TEST262: Test extract_effective_payload with CBOR content fails when no argument matches expected input
    #[test]
    fn test262_extract_effective_payload_cbor_no_match() {
        let args = ciborium::Value::Array(vec![ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:other-type".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(b"data".to_vec()),
            ),
        ])]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let registry = MockRegistry::with_test_caps();
        let cap = registry
            .get(r#"cap:in="media:enc=utf-8;string";test;out=media:"#)
            .unwrap();
        let result = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            cap,
            false, // CBOR mode
        );
        assert!(result.is_err(), "must fail when no argument matches");
        match result.unwrap_err() {
            RuntimeError::Deserialize(msg) => {
                assert!(msg.contains("No argument found matching"), "{}", msg);
            }
            other => panic!("expected Deserialize, got {:?}", other),
        }
    }

    // TEST263: Test extract_effective_payload with invalid CBOR bytes returns deserialization error
    #[test]
    fn test263_extract_effective_payload_invalid_cbor() {
        let registry = MockRegistry::with_test_caps();
        let cap = registry.get(r#"cap:in=media:;test;out=media:"#).unwrap();
        let result = extract_effective_payload(
            b"not cbor",
            Some("application/cbor"),
            cap,
            false, // CBOR mode
        );
        assert!(result.is_err());
    }

    // TEST264: Test extract_effective_payload with CBOR non-array (e.g. map) returns error
    #[test]
    fn test264_extract_effective_payload_cbor_not_array() {
        let value = ciborium::Value::Map(vec![]);
        let mut payload = Vec::new();
        ciborium::into_writer(&value, &mut payload).unwrap();

        let registry = MockRegistry::with_test_caps();
        let cap = registry.get(r#"cap:in=media:;test;out=media:"#).unwrap();
        let result = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            cap,
            false, // CBOR mode
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            RuntimeError::Deserialize(msg) => {
                assert!(msg.contains("must be an array"), "{}", msg);
            }
            other => panic!("expected Deserialize, got {:?}", other),
        }
    }

    // TEST266: Test CliFrameSender wraps CliStreamEmitter correctly (basic construction)
    #[test]
    fn test266_cli_frame_sender_construction() {
        let sender = CliFrameSender::new();
        assert!(sender.emitter.ndjson, "default CLI sender must use NDJSON");

        let emitter2 = CliStreamEmitter::without_ndjson();
        let sender2 = CliFrameSender::with_emitter(emitter2);
        assert!(!sender2.emitter.ndjson);
    }

    // TEST268: Test RuntimeError variants display correct messages
    #[test]
    fn test268_runtime_error_display() {
        let err = RuntimeError::NoHandler("cap:missing".to_string());
        assert!(format!("{}", err).contains("cap:missing"));

        let err2 = RuntimeError::MissingArgument("model".to_string());
        assert!(format!("{}", err2).contains("model"));

        let err3 = RuntimeError::UnknownSubcommand("badcmd".to_string());
        assert!(format!("{}", err3).contains("badcmd"));

        let err4 = RuntimeError::Manifest("parse failed".to_string());
        assert!(format!("{}", err4).contains("parse failed"));

        let err5 = RuntimeError::PeerRequest("denied".to_string());
        assert!(format!("{}", err5).contains("denied"));

        let err6 = RuntimeError::PeerResponse("timeout".to_string());
        assert!(format!("{}", err6).contains("timeout"));
    }

    // TEST270: Test registering multiple Op handlers for different caps and finding each independently
    #[tokio::test]
    async fn test270_multiple_handlers() {
        let mut runtime = CartridgeRuntime::new(TEST_MANIFEST.as_bytes());

        runtime.register_op("cap:alpha", || Box::new(EchoTagOp { tag: b"a".to_vec() }));
        runtime.register_op("cap:beta", || Box::new(EchoTagOp { tag: b"b".to_vec() }));
        runtime.register_op("cap:gamma", || Box::new(EchoTagOp { tag: b"g".to_vec() }));

        let f_alpha = runtime.find_handler("cap:alpha").unwrap();
        let input = test_input_package(&[("media:", b"")]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&f_alpha, input, output).await.unwrap();

        let f_beta = runtime.find_handler("cap:beta").unwrap();
        let input = test_input_package(&[("media:", b"")]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&f_beta, input, output).await.unwrap();

        let f_gamma = runtime.find_handler("cap:gamma").unwrap();
        let input = test_input_package(&[("media:", b"")]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&f_gamma, input, output).await.unwrap();
    }

    // TEST271: Test Op handler replacing an existing registration for the same cap URN
    #[tokio::test]
    async fn test271_handler_replacement() {
        let mut runtime = CartridgeRuntime::new(TEST_MANIFEST.as_bytes());

        let result1: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let result2: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let result2_clone = Arc::clone(&result2);

        runtime.register_op("cap:test", move || {
            Box::new(EchoTagOp {
                tag: b"first".to_vec(),
            }) as Box<dyn Op<()>>
        });
        runtime.register_op("cap:test", move || {
            let r = Arc::clone(&result2_clone);
            Box::new(EmitAndRecordOp2 {
                data: b"second".to_vec(),
                received: r,
            }) as Box<dyn Op<()>>
        });

        /// Op that emits fixed data and records it
        struct EmitAndRecordOp2 {
            data: Vec<u8>,
            received: Arc<Mutex<Vec<u8>>>,
        }
        #[async_trait]
        impl Op<()> for EmitAndRecordOp2 {
            async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
                let req: Arc<Request> = wet
                    .get_required(WET_KEY_REQUEST)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let mut input = req
                    .take_input()
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                while let Some(stream_result) = input.recv().await {
                    let mut stream =
                        stream_result.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                    while let Some(chunk) = stream.recv_data().await {
                        let _ = chunk.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                    }
                }
                req.output()
                    .start(false, None)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                req.output()
                    .emit_cbor(&ciborium::Value::Bytes(self.data.clone()))
                    .await
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                *self.received.lock().unwrap() = self.data.clone();
                Ok(())
            }
            fn metadata(&self) -> OpMetadata {
                OpMetadata::builder("EmitAndRecordOp2").build()
            }
        }

        let factory = runtime.find_handler("cap:test").unwrap();
        let input = test_input_package(&[("media:", b"")]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&factory, input, output).await.unwrap();
        assert_eq!(
            &*result2.lock().unwrap(),
            b"second",
            "later registration must replace earlier"
        );
        // result1 should NOT have been called
        assert!(
            result1.lock().unwrap().is_empty(),
            "first handler must not be called after replacement"
        );
    }

    // TEST272: Test extract_effective_payload CBOR with multiple arguments selects the correct one
    #[test]
    fn test272_extract_effective_payload_multiple_args() {
        let args = ciborium::Value::Array(vec![
            ciborium::Value::Map(vec![
                (
                    ciborium::Value::Text("media_urn".to_string()),
                    ciborium::Value::Text("media:enc=utf-8;other-type".to_string()),
                ),
                (
                    ciborium::Value::Text("value".to_string()),
                    ciborium::Value::Bytes(b"wrong".to_vec()),
                ),
            ]),
            ciborium::Value::Map(vec![
                (
                    ciborium::Value::Text("media_urn".to_string()),
                    ciborium::Value::Text("media:enc=utf-8;model-spec".to_string()),
                ),
                (
                    ciborium::Value::Text("value".to_string()),
                    ciborium::Value::Bytes(b"correct".to_vec()),
                ),
            ]),
        ]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let registry = MockRegistry::with_test_caps();
        let cap = registry
            .get(r#"cap:in="media:enc=utf-8;model-spec";infer;out=media:"#)
            .unwrap();
        let result = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            cap,
            false, // CBOR mode - tests pass CBOR payloads directly
        )
        .unwrap();

        // NEW REGIME: Handler receives full CBOR array with BOTH arguments
        // Handler must match against in_spec to find main input
        let result_cbor: ciborium::Value = ciborium::from_reader(&result[..]).unwrap();
        let result_array = match result_cbor {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };

        assert_eq!(
            result_array.len(),
            2,
            "Both arguments present in CBOR array"
        );

        // Find the argument matching in_spec (media:model-spec)
        let in_spec = MediaUrn::from_string("media:enc=utf-8;model-spec").unwrap();
        let mut found_value = None;
        for arg in result_array {
            if let ciborium::Value::Map(map) = arg {
                let mut arg_urn_str = None;
                let mut arg_value = None;
                for (k, v) in map {
                    if let ciborium::Value::Text(key) = k {
                        if key == "media_urn" {
                            if let ciborium::Value::Text(s) = v {
                                arg_urn_str = Some(s);
                            }
                        } else if key == "value" {
                            if let ciborium::Value::Bytes(b) = v {
                                arg_value = Some(b);
                            }
                        }
                    }
                }

                // Match against in_spec using is_comparable for discovery
                if let (Some(urn_str), Some(val)) = (arg_urn_str, arg_value) {
                    if let Ok(arg_urn) = MediaUrn::from_string(&urn_str) {
                        if in_spec.is_comparable(&arg_urn).unwrap_or(false) {
                            found_value = Some(val);
                            break;
                        }
                    }
                }
            }
        }

        assert_eq!(
            found_value,
            Some(b"correct".to_vec()),
            "Handler finds correct argument by matching in_spec"
        );
    }

    // TEST273: Test extract_effective_payload with binary data in CBOR value (not just text)
    #[test]
    fn test273_extract_effective_payload_binary_value() {
        let binary_data: Vec<u8> = (0u8..=255).collect();
        let args = ciborium::Value::Array(vec![ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:ext=pdf".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(binary_data.clone()),
            ),
        ])]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let registry = MockRegistry::with_test_caps();
        let cap = registry
            .get(r#"cap:in="media:ext=pdf";process;out=media:"#)
            .unwrap();
        let result = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            cap,
            false, // CBOR mode - tests pass CBOR payloads directly
        )
        .unwrap();

        // NEW REGIME: Parse CBOR array and extract value
        let result_cbor: ciborium::Value = ciborium::from_reader(&result[..]).unwrap();
        let result_array = match result_cbor {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };

        let mut found_value = None;
        for arg in result_array {
            if let ciborium::Value::Map(map) = arg {
                for (k, v) in map {
                    if let ciborium::Value::Text(key) = k {
                        if key == "value" {
                            if let ciborium::Value::Bytes(b) = v {
                                found_value = Some(b);
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(
            found_value,
            Some(binary_data),
            "binary values must roundtrip through CBOR array"
        );
    }

    // TEST336: Single file-path arg with stdin source reads file and passes bytes to handler
    #[tokio::test]
    async fn test336_file_path_reads_file_passes_bytes() {
        use std::sync::{Arc, Mutex};

        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test336_input.pdf");
        std::fs::write(&test_file, b"PDF binary content 336").unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:ext=pdf\";process;out=media:void",
            "Process PDF",
            "process",
            vec![CapArg::new(
                "media:enc=utf-8;file-path",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:ext=pdf".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let mut runtime = CartridgeRuntime::with_manifest(manifest);

        // Track what handler receives
        let received_payload = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received_payload);

        runtime.register_op(
            "cap:in=\"media:ext=pdf\";process;out=media:void",
            move || {
                Box::new(ExtractValueOp {
                    received: Arc::clone(&received_clone),
                }) as Box<dyn Op<()>>
            },
        );

        // Simulate CLI invocation: cartridge process /path/to/file.pdf
        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();
        let raw_payload = runtime.build_payload_from_cli(&cap, &cli_args).unwrap();

        // Extract effective payload (simulates what run_cli_mode does)
        // This does file-path auto-conversion: path → bytes
        let payload = extract_effective_payload(
            &raw_payload,
            Some("application/cbor"),
            &cap,
            true, // CLI mode
        )
        .unwrap();

        let factory = runtime.find_handler(&cap.urn_string()).unwrap();

        // Simulate CLI mode: parse CBOR args → send as streams → InputPackage
        let input = test_input_package(&[("media:", &payload)]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&factory, input, output).await.unwrap();

        // Verify handler received file bytes (not file path string)
        let received = received_payload.lock().unwrap();
        assert_eq!(
            &*received, b"PDF binary content 336",
            "Handler receives file bytes after auto-conversion"
        );

        std::fs::remove_file(test_file).ok();
    }

    // TEST337: file-path arg without stdin source passes path as string (no conversion)
    #[test]
    fn test337_file_path_without_stdin_passes_string() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test337_input.txt");
        std::fs::write(&test_file, b"content").unwrap();

        let cap = create_test_cap(
            "cap:in=media:void;test;out=media:void",
            "Test",
            "test",
            vec![CapArg::new(
                "media:enc=utf-8;file-path",
                true,
                vec![ArgSource::Position { position: 0 }], // NO stdin source!
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();
        let result = runtime
            .extract_arg_value(&cap.args[0], &cli_args, &mut || Ok(None))
            .unwrap();

        // Should get file PATH as string, not file CONTENTS
        let value_str = String::from_utf8(result.0.unwrap()).unwrap();
        assert!(
            value_str.contains("test337_input.txt"),
            "Should receive file path string when no stdin source"
        );

        std::fs::remove_file(test_file).ok();
    }

    // TEST338: file-path arg reads file via --file CLI flag
    #[test]
    fn test338_file_path_via_cli_flag() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test338.pdf");
        std::fs::write(&test_file, b"PDF via flag 338").unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:ext=pdf\";process;out=media:void",
            "Process",
            "process",
            vec![CapArg::new(
                "media:enc=utf-8;file-path",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:ext=pdf".to_string(),
                    },
                    ArgSource::CliFlag {
                        cli_flag: "--file".to_string(),
                    },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec![
            "--file".to_string(),
            test_file.to_string_lossy().to_string(),
        ];
        let file_contents = test_filepath_conversion(&cap, &cli_args, &runtime);

        assert_eq!(
            file_contents, b"PDF via flag 338",
            "Should read file from --file flag"
        );

        std::fs::remove_file(test_file).ok();
    }

    // TEST339: file-path-array reads multiple files with glob pattern
    #[test]
    fn test339_file_path_array_glob_expansion() {
        // A sequence-declared file-path arg (`is_sequence = true`) expands a
        // glob to N files and the runtime delivers them as a CBOR Array of
        // Bytes — one array item per matched file. List-ness comes from the
        // arg declaration, not from any `;list` URN tag.
        let temp_dir = std::env::temp_dir().join("test339");
        std::fs::create_dir_all(&temp_dir).unwrap();

        let file1 = temp_dir.join("doc1.txt");
        let file2 = temp_dir.join("doc2.txt");
        std::fs::write(&file1, b"content1").unwrap();
        std::fs::write(&file2, b"content2").unwrap();

        let mut batch_arg = CapArg::new(
            "media:enc=utf-8;file-path",
            true,
            vec![
                ArgSource::Stdin {
                    stdin: "media:".to_string(),
                },
                ArgSource::Position { position: 0 },
            ],
        );
        batch_arg.is_sequence = true;

        let cap = create_test_cap(
            "cap:in=media:;batch;out=media:void",
            "Batch",
            "batch",
            vec![batch_arg],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let pattern = format!("{}/*.txt", temp_dir.display());
        let cli_args = vec![pattern];
        let files_bytes = test_filepath_array_conversion(&cap, &cli_args, &runtime);

        assert_eq!(files_bytes.len(), 2, "Should find 2 files");

        let mut sorted = files_bytes.clone();
        sorted.sort();
        assert_eq!(sorted, vec![b"content1".to_vec(), b"content2".to_vec()]);

        std::fs::remove_dir_all(temp_dir).ok();
    }

    // TEST340: File not found error provides clear message
    #[test]
    fn test340_file_not_found_clear_error() {
        let cap = create_test_cap(
            "cap:in=\"media:ext=pdf\";test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:enc=utf-8;file-path",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:ext=pdf".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec!["/nonexistent/file.pdf".to_string()];

        // Build CBOR payload and try conversion - should fail on file read
        let (raw_value, _) = runtime
            .extract_arg_value(&cap.args[0], &cli_args, &mut || Ok(None))
            .unwrap();
        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:enc=utf-8;file-path".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(raw_value.unwrap()),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        // extract_effective_payload should fail when trying to read nonexistent file
        let result = extract_effective_payload(&payload, Some("application/cbor"), &cap, true);

        assert!(result.is_err(), "Should fail when file doesn't exist");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("/nonexistent/file.pdf"),
            "Error should mention file path; got: {}",
            err_msg,
        );
        assert!(
            err_msg.contains("File not found") || err_msg.contains("Failed to read file"),
            "Error should be clear; got: {}",
            err_msg,
        );
    }

    // TEST341: stdin takes precedence over file-path in source order
    #[test]
    fn test341_stdin_precedence_over_file_path() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test341_input.txt");
        std::fs::write(&test_file, b"file content").unwrap();

        // Stdin source comes BEFORE position source
        let cap = create_test_cap(
            "cap:in=media:;test;out=media:void",
            "Test",
            "test",
            vec![CapArg::new(
                "media:enc=utf-8;file-path",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    }, // First
                    ArgSource::Position { position: 0 }, // Second
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let stdin_data = b"stdin content 341";
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();

        let (result, _) = runtime
            .extract_arg_value(&cap.args[0], &cli_args, &mut || {
                Ok(Some(stdin_data.to_vec()))
            })
            .unwrap();
        let result = result.unwrap();

        // Should get stdin data, not file content (stdin source tried first)
        assert_eq!(
            result, b"stdin content 341",
            "stdin source should take precedence"
        );

        std::fs::remove_file(test_file).ok();
    }

    // TEST342: file-path with position 0 reads first positional arg as file
    #[test]
    fn test342_file_path_position_zero_reads_first_arg() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test342.dat");
        std::fs::write(&test_file, b"binary data 342").unwrap();

        let cap = create_test_cap(
            "cap:in=media:;test;out=media:void",
            "Test",
            "test",
            vec![CapArg::new(
                "media:enc=utf-8;file-path",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        // CLI: cartridge test /path/to/file (position 0 after subcommand)
        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let result = test_filepath_conversion(&cap, &cli_args, &runtime);

        assert_eq!(result, b"binary data 342", "Should read file at position 0");

        std::fs::remove_file(test_file).ok();
    }

    // TEST343: Non-file-path args are not affected by file reading
    #[test]
    fn test343_non_file_path_args_unaffected() {
        // Arg with different media type should NOT trigger file reading
        let cap = create_test_cap(
            "cap:in=media:void;test;out=media:void",
            "Test",
            "test",
            vec![CapArg::new(
                "media:enc=utf-8;model-spec", // NOT file-path
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:enc=utf-8;model-spec".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec!["mlx-community/Llama-3.2-3B-Instruct-4bit".to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();
        let (result, _) = runtime
            .extract_arg_value(&cap.args[0], &cli_args, &mut || Ok(None))
            .unwrap();
        let result = result.unwrap();

        // Should get the string value, not attempt file read
        let value_str = String::from_utf8(result).unwrap();
        assert_eq!(value_str, "mlx-community/Llama-3.2-3B-Instruct-4bit");
    }

    // TEST6586: file-path-array with nonexistent path fails clearly
    #[test]
    fn test6586_file_path_array_invalid_json_fails() {
        let cap = create_test_cap(
            "cap:in=media:;batch;out=media:void",
            "Test",
            "batch",
            vec![CapArg::new(
                "media:enc=utf-8;file-path",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        // Pass nonexistent path (without `;json` tag, this is NOT JSON - it's a path/pattern)
        let cli_args = vec!["/nonexistent/path/to/nothing".to_string()];

        // Build CBOR payload and try conversion - should fail on file read
        let (raw_value, _) = runtime
            .extract_arg_value(&cap.args[0], &cli_args, &mut || Ok(None))
            .unwrap();
        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:enc=utf-8;file-path".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(raw_value.unwrap()),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let result = extract_effective_payload(&payload, Some("application/cbor"), &cap, true);

        assert!(result.is_err(), "Should fail when path doesn't exist");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("/nonexistent/path/to/nothing"),
            "Error should mention the path"
        );
        assert!(
            err.contains("File not found") || err.contains("Failed to read"),
            "Error should be clear about file access failure"
        );
    }

    // TEST6587: file-path-array with literal nonexistent path fails hard
    #[test]
    fn test6587_file_path_array_one_file_missing_fails_hard() {
        let temp_dir = std::env::temp_dir();
        let missing_path = temp_dir.join("test345_missing.txt");

        let cap = create_test_cap(
            "cap:in=media:;batch;out=media:void",
            "Test",
            "batch",
            vec![CapArg::new(
                "media:enc=utf-8;file-path",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        // Pass literal path (non-glob) that doesn't exist - should fail
        let cli_args = vec![missing_path.to_string_lossy().to_string()];

        // Build CBOR payload and try conversion - should fail on file read
        let (raw_value, _) = runtime
            .extract_arg_value(&cap.args[0], &cli_args, &mut || Ok(None))
            .unwrap();
        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:enc=utf-8;file-path".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(raw_value.unwrap()),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let result = extract_effective_payload(&payload, Some("application/cbor"), &cap, true);

        assert!(
            result.is_err(),
            "Should fail hard when literal path doesn't exist"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("test345_missing.txt"),
            "Error should mention the missing file"
        );
        assert!(
            err.contains("File not found") || err.contains("doesn't exist"),
            "Error should be clear about missing file"
        );
    }

    // TEST346: Large file (1MB) reads successfully
    #[test]
    fn test346_large_file_reads_successfully() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test346_large.bin");

        // Create 1MB file
        let large_data = vec![42u8; 1_000_000];
        std::fs::write(&test_file, &large_data).unwrap();

        let cap = create_test_cap(
            "cap:in=media:;test;out=media:void",
            "Test",
            "test",
            vec![CapArg::new(
                "media:enc=utf-8;file-path",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let result = test_filepath_conversion(&cap, &cli_args, &runtime);

        assert_eq!(result.len(), 1_000_000, "Should read entire 1MB file");
        assert_eq!(result, large_data, "Content should match exactly");

        std::fs::remove_file(test_file).ok();
    }

    // TEST347: Empty file reads as empty bytes
    #[test]
    fn test347_empty_file_reads_as_empty_bytes() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test347_empty.txt");
        std::fs::write(&test_file, b"").unwrap();

        let cap = create_test_cap(
            "cap:in=media:;test;out=media:void",
            "Test",
            "test",
            vec![CapArg::new(
                "media:enc=utf-8;file-path",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let result = test_filepath_conversion(&cap, &cli_args, &runtime);

        assert_eq!(result, b"", "Empty file should produce empty bytes");

        std::fs::remove_file(test_file).ok();
    }

    // TEST348: file-path conversion respects source order
    #[test]
    fn test348_file_path_conversion_respects_source_order() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test348.txt");
        std::fs::write(&test_file, b"file content 348").unwrap();

        // Position source BEFORE stdin source
        let cap = create_test_cap(
            "cap:in=media:;test;out=media:void",
            "Test",
            "test",
            vec![CapArg::new(
                "media:enc=utf-8;file-path",
                true,
                vec![
                    ArgSource::Position { position: 0 }, // First
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    }, // Second
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();

        // Use helper to properly test file-path conversion
        let result = test_filepath_conversion(&cap, &cli_args, &runtime);

        // Position source tried first, so file is read
        assert_eq!(
            result, b"file content 348",
            "Position source tried first, file read"
        );

        std::fs::remove_file(test_file).ok();
    }

    // TEST349: file-path arg with multiple sources tries all in order
    #[test]
    fn test349_file_path_multiple_sources_fallback() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test349.txt");
        std::fs::write(&test_file, b"content 349").unwrap();

        let cap = create_test_cap(
            "cap:in=media:;test;out=media:void",
            "Test",
            "test",
            vec![CapArg::new(
                "media:enc=utf-8;file-path",
                true,
                vec![
                    ArgSource::CliFlag {
                        cli_flag: "--file".to_string(),
                    }, // First (not provided)
                    ArgSource::Position { position: 0 }, // Second (provided)
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    }, // Third (not used)
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        // Only provide position arg, no --file flag
        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();

        // Use helper to properly test file-path conversion
        let result = test_filepath_conversion(&cap, &cli_args, &runtime);

        assert_eq!(
            result, b"content 349",
            "Should fall back to position source and read file"
        );

        std::fs::remove_file(test_file).ok();
    }

    // TEST350: Integration test - full CLI mode invocation with file-path
    #[tokio::test]
    async fn test350_full_cli_mode_with_file_path_integration() {
        use std::sync::{Arc, Mutex};

        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test350_input.pdf");
        let test_content = b"PDF file content for integration test";
        std::fs::write(&test_file, test_content).unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:ext=pdf\";process;out=\"media:enc=utf-8;result\"",
            "Process PDF",
            "process",
            vec![CapArg::new(
                "media:enc=utf-8;file-path",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:ext=pdf".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let mut runtime = CartridgeRuntime::with_manifest(manifest);

        // Track what the handler receives
        let received_payload = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received_payload);

        runtime.register_op(
            "cap:in=\"media:ext=pdf\";process;out=\"media:enc=utf-8;result\"",
            move || {
                Box::new(ExtractValueOp {
                    received: Arc::clone(&received_clone),
                }) as Box<dyn Op<()>>
            },
        );

        // Simulate full CLI invocation
        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();
        let raw_payload = runtime.build_payload_from_cli(&cap, &cli_args).unwrap();

        // Extract effective payload (what run_cli_mode does)
        let payload = extract_effective_payload(
            &raw_payload,
            Some("application/cbor"),
            &cap,
            true, // CLI mode
        )
        .unwrap();

        let factory = runtime.find_handler(&cap.urn_string()).unwrap();

        let input = test_input_package(&[("media:", &payload)]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&factory, input, output).await.unwrap();

        // Verify handler received file bytes
        let received = received_payload.lock().unwrap();
        assert_eq!(
            &*received, test_content,
            "Handler receives file bytes after auto-conversion"
        );

        std::fs::remove_file(test_file).ok();
    }

    // TEST6588: sequence-declared file-path arg with empty input array (CBOR
    // mode) passes through as an empty CBOR Array — no implicit expansion,
    // no spurious error. Declaring `is_sequence = true` is what makes the
    // runtime emit an Array shape; URN tags are semantic only.
    #[test]
    fn test6588_file_path_array_empty_array() {
        let mut batch_arg = CapArg::new(
            "media:enc=utf-8;file-path",
            false, // Not required
            vec![ArgSource::Stdin {
                stdin: "media:".to_string(),
            }],
        );
        batch_arg.is_sequence = true;

        let cap = create_test_cap(
            "cap:in=media:;batch;out=media:void",
            "Test",
            "batch",
            vec![batch_arg],
        );

        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:enc=utf-8;file-path".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Array(vec![]),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let result =
            extract_effective_payload(&payload, Some("application/cbor"), &cap, false).unwrap();

        let result_cbor: ciborium::Value = ciborium::from_reader(&result[..]).unwrap();
        let result_array = match result_cbor {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };
        let result_map = match &result_array[0] {
            ciborium::Value::Map(m) => m,
            _ => panic!("Expected map"),
        };
        let value_array = result_map
            .iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| match v {
                ciborium::Value::Array(arr) => arr,
                _ => panic!("Expected array"),
            })
            .unwrap();

        assert_eq!(
            value_array.len(),
            0,
            "Empty array should produce empty result"
        );
    }

    // TEST352: file permission denied error is clear (Unix-specific)
    #[test]
    #[cfg(unix)]
    fn test352_file_permission_denied_clear_error() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test352_noperm.txt");

        // Clean up any existing file from previous test runs (might have restricted permissions)
        if test_file.exists() {
            if let Ok(metadata) = std::fs::metadata(&test_file) {
                let mut perms = metadata.permissions();
                perms.set_mode(0o644);
                let _ = std::fs::set_permissions(&test_file, perms);
            }
            std::fs::remove_file(&test_file).ok();
        }

        std::fs::write(&test_file, b"content").unwrap();

        // Remove read permissions
        let mut perms = std::fs::metadata(&test_file).unwrap().permissions();
        perms.set_mode(0o000);
        std::fs::set_permissions(&test_file, perms).unwrap();

        let cap = create_test_cap(
            "cap:in=media:;test;out=media:void",
            "Test",
            "test",
            vec![CapArg::new(
                "media:enc=utf-8;file-path",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();

        // Build full CBOR payload and attempt file-path conversion
        let (raw_value, _) = runtime
            .extract_arg_value(&cap.args[0], &cli_args, &mut || Ok(None))
            .unwrap();
        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:enc=utf-8;file-path".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(raw_value.unwrap()),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let result = extract_effective_payload(&payload, Some("application/cbor"), &cap, true);

        assert!(result.is_err(), "Should fail on permission denied");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("test352_noperm.txt"),
            "Error should mention the file"
        );

        // Cleanup: restore permissions then delete
        let mut perms = std::fs::metadata(&test_file).unwrap().permissions();
        perms.set_mode(0o644);
        std::fs::set_permissions(&test_file, perms).unwrap();
        std::fs::remove_file(test_file).ok();
    }

    // TEST353: CBOR payload format matches between CLI and CBOR mode
    #[test]
    fn test353_cbor_payload_format_consistency() {
        let cap = create_test_cap(
            "cap:in=\"media:enc=utf-8;text\";test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:enc=utf-8;text",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:enc=utf-8;text".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec!["test value".to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();
        let payload = runtime.build_payload_from_cli(&cap, &cli_args).unwrap();

        // Decode CBOR payload
        let cbor_value: ciborium::Value = ciborium::from_reader(&payload[..]).unwrap();
        let args_array = match cbor_value {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };

        assert_eq!(args_array.len(), 1, "Should have 1 argument");

        // Verify structure: { media_urn: "...", value: bytes }
        let arg_map = match &args_array[0] {
            ciborium::Value::Map(m) => m,
            _ => panic!("Expected CBOR map"),
        };

        assert_eq!(arg_map.len(), 2, "Argument should have media_urn and value");

        // Check media_urn key
        let media_urn_val = arg_map
            .iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "media_urn"))
            .map(|(_, v)| v)
            .expect("Should have media_urn key");

        match media_urn_val {
            ciborium::Value::Text(s) => assert_eq!(s, "media:enc=utf-8;text"),
            _ => panic!("media_urn should be text"),
        }

        // Check value key
        let value_val = arg_map
            .iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| v)
            .expect("Should have value key");

        match value_val {
            ciborium::Value::Bytes(b) => assert_eq!(b, b"test value"),
            _ => panic!("value should be bytes"),
        }
    }

    // TEST354: Glob pattern with no matches fails hard (NO FALLBACK)
    #[test]
    fn test354_glob_pattern_no_matches_empty_array() {
        let temp_dir = std::env::temp_dir();

        let cap = create_test_cap(
            "cap:in=media:;batch;out=media:void",
            "Test",
            "batch",
            vec![CapArg::new(
                "media:enc=utf-8;file-path",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        // Glob pattern that matches nothing - should FAIL HARD (no fallback to empty array)
        let pattern = format!("{}/nonexistent_*.xyz", temp_dir.display());
        let cli_args = vec![pattern]; // NOT JSON - just the pattern

        // Build CBOR payload and try conversion - should fail when glob matches nothing
        let (raw_value, _) = runtime
            .extract_arg_value(&cap.args[0], &cli_args, &mut || Ok(None))
            .unwrap();
        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:enc=utf-8;file-path".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(raw_value.unwrap()),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let result = extract_effective_payload(&payload, Some("application/cbor"), &cap, true);

        assert!(
            result.is_err(),
            "Should fail hard when glob matches nothing - NO FALLBACK"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("No files matched") || err.contains("nonexistent"),
            "Error should explain glob matched nothing"
        );
    }

    // TEST355: Glob pattern skips directories
    #[test]
    fn test355_glob_pattern_skips_directories() {
        let temp_dir = std::env::temp_dir().join("test355");
        std::fs::create_dir_all(&temp_dir).unwrap();

        let subdir = temp_dir.join("subdir");
        std::fs::create_dir_all(&subdir).unwrap();

        let file1 = temp_dir.join("file1.txt");
        std::fs::write(&file1, b"content1").unwrap();

        let mut batch_arg = CapArg::new(
            "media:enc=utf-8;file-path",
            true,
            vec![
                ArgSource::Stdin {
                    stdin: "media:".to_string(),
                },
                ArgSource::Position { position: 0 },
            ],
        );
        batch_arg.is_sequence = true;

        let cap = create_test_cap(
            "cap:in=media:;batch;out=media:void",
            "Test",
            "batch",
            vec![batch_arg],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        // Glob that matches both file and directory
        let pattern = format!("{}/*", temp_dir.display());
        let cli_args = vec![pattern]; // NOT JSON - just the glob pattern
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();

        // Use helper to test file-path array conversion
        let files_array = test_filepath_array_conversion(&cap, &cli_args, &runtime);

        // Should only include the file, not the directory
        assert_eq!(
            files_array.len(),
            1,
            "Should only include files, not directories"
        );
        assert_eq!(files_array[0], b"content1");

        std::fs::remove_dir_all(temp_dir).ok();
    }

    // TEST356: Multiple glob patterns combined
    #[test]
    fn test356_multiple_glob_patterns_combined() {
        let temp_dir = std::env::temp_dir().join("test356");
        std::fs::create_dir_all(&temp_dir).unwrap();

        let file1 = temp_dir.join("doc.txt");
        let file2 = temp_dir.join("data.json");
        std::fs::write(&file1, b"text").unwrap();
        std::fs::write(&file2, b"json").unwrap();

        let mut batch_arg = CapArg::new(
            "media:enc=utf-8;file-path",
            true,
            vec![
                ArgSource::Stdin {
                    stdin: "media:".to_string(),
                },
                ArgSource::Position { position: 0 },
            ],
        );
        batch_arg.is_sequence = true;

        let cap = create_test_cap(
            "cap:in=media:;batch;out=media:void",
            "Test",
            "batch",
            vec![batch_arg],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        // Multiple patterns as CBOR Array (CBOR mode)
        let pattern1 = format!("{}/*.txt", temp_dir.display());
        let pattern2 = format!("{}/*.json", temp_dir.display());

        // Build CBOR payload with Array of patterns
        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:enc=utf-8;file-path".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Array(vec![
                    ciborium::Value::Text(pattern1),
                    ciborium::Value::Text(pattern2),
                ]),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();

        // Do file-path conversion with is_cli_mode=false (CBOR mode allows Arrays)
        let result =
            extract_effective_payload(&payload, Some("application/cbor"), &cap, false).unwrap();

        // Decode and verify both files found
        let result_cbor: ciborium::Value = ciborium::from_reader(&result[..]).unwrap();
        let result_array = match result_cbor {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };
        let result_map = match &result_array[0] {
            ciborium::Value::Map(m) => m,
            _ => panic!("Expected map"),
        };
        let files_array = result_map
            .iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| match v {
                ciborium::Value::Array(arr) => arr,
                _ => panic!("Expected array"),
            })
            .unwrap();

        assert_eq!(
            files_array.len(),
            2,
            "Should find both files from different patterns"
        );

        // Collect contents (order may vary)
        let mut contents = Vec::new();
        for val in files_array {
            match val {
                ciborium::Value::Bytes(b) => contents.push(b.as_slice()),
                _ => panic!("Expected bytes"),
            }
        }
        contents.sort();
        assert_eq!(contents, vec![b"json" as &[u8], b"text" as &[u8]]);

        std::fs::remove_dir_all(temp_dir).ok();
    }

    // TEST357: Symlinks are followed when reading files
    #[test]
    #[cfg(unix)]
    fn test357_symlinks_followed() {
        use std::os::unix::fs as unix_fs;

        let temp_dir = std::env::temp_dir().join("test357");
        // Clean up from previous test runs
        std::fs::remove_dir_all(&temp_dir).ok();
        std::fs::create_dir_all(&temp_dir).unwrap();

        let real_file = temp_dir.join("real.txt");
        let link_file = temp_dir.join("link.txt");
        std::fs::write(&real_file, b"real content").unwrap();
        unix_fs::symlink(&real_file, &link_file).unwrap();

        let cap = create_test_cap(
            "cap:in=media:;test;out=media:void",
            "Test",
            "test",
            vec![CapArg::new(
                "media:enc=utf-8;file-path",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec![link_file.to_string_lossy().to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();

        // Use helper to test file-path conversion
        let result = test_filepath_conversion(&cap, &cli_args, &runtime);

        assert_eq!(
            result, b"real content",
            "Should follow symlink and read real file"
        );

        std::fs::remove_dir_all(temp_dir).ok();
    }

    // TEST358: Binary file with non-UTF8 data reads correctly
    #[test]
    fn test358_binary_file_non_utf8() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test358.bin");

        // Binary data that's not valid UTF-8
        let binary_data = vec![0xFF, 0xFE, 0x00, 0x01, 0x80, 0x7F, 0xAB, 0xCD];
        std::fs::write(&test_file, &binary_data).unwrap();

        let cap = create_test_cap(
            "cap:in=media:;test;out=media:void",
            "Test",
            "test",
            vec![CapArg::new(
                "media:enc=utf-8;file-path",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let result = test_filepath_conversion(&cap, &cli_args, &runtime);

        assert_eq!(result, binary_data, "Binary data should read correctly");

        std::fs::remove_file(test_file).ok();
    }

    // TEST359: Invalid glob pattern fails with clear error
    #[test]
    fn test359_invalid_glob_pattern_fails() {
        let cap = create_test_cap(
            "cap:in=media:;batch;out=media:void",
            "Test",
            "batch",
            vec![CapArg::new(
                "media:enc=utf-8;file-path",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        // Invalid glob pattern (unclosed bracket)
        let pattern = "[invalid";

        // Build CBOR payload with invalid pattern
        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:enc=utf-8;file-path".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Text(pattern.to_string()),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();

        // Try file-path conversion with invalid glob - should fail
        let result = extract_effective_payload(&payload, Some("application/cbor"), &cap, true);

        assert!(result.is_err(), "Should fail on invalid glob pattern");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Invalid glob pattern") || err.contains("Pattern"),
            "Error should mention invalid glob"
        );
    }

    // TEST360: Extract effective payload handles file-path data correctly
    #[test]
    fn test360_extract_effective_payload_with_file_data() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test360.pdf");
        let pdf_content = b"PDF content for extraction test";
        std::fs::write(&test_file, pdf_content).unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:ext=pdf\";process;out=media:void",
            "Process",
            "process",
            vec![CapArg::new(
                "media:enc=utf-8;file-path",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:ext=pdf".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();

        // Build CBOR payload (what build_payload_from_cli does)
        let raw_payload = runtime.build_payload_from_cli(&cap, &cli_args).unwrap();

        // Extract effective payload (what run_cli_mode does)
        // This does file-path auto-conversion and returns full CBOR array
        let effective = extract_effective_payload(
            &raw_payload,
            Some("application/cbor"),
            &cap,
            true, // CLI mode
        )
        .unwrap();

        // NEW REGIME: Parse CBOR array and extract file bytes
        let result_cbor: ciborium::Value = ciborium::from_reader(&effective[..]).unwrap();
        let result_array = match result_cbor {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };

        // Extract value from argument matching in_spec
        let in_spec = MediaUrn::from_string("media:ext=pdf").unwrap();
        let mut found_value = None;
        for arg in result_array {
            if let ciborium::Value::Map(map) = arg {
                let mut arg_urn_str = None;
                let mut arg_value = None;
                for (k, v) in map {
                    if let ciborium::Value::Text(key) = k {
                        if key == "media_urn" {
                            if let ciborium::Value::Text(s) = v {
                                arg_urn_str = Some(s);
                            }
                        } else if key == "value" {
                            if let ciborium::Value::Bytes(b) = v {
                                arg_value = Some(b);
                            }
                        }
                    }
                }

                if let (Some(urn_str), Some(val)) = (arg_urn_str, arg_value) {
                    if let Ok(arg_urn) = MediaUrn::from_string(&urn_str) {
                        let matches = in_spec.accepts(&arg_urn).unwrap_or(false)
                            || arg_urn.conforms_to(&in_spec).unwrap_or(false);
                        if matches {
                            found_value = Some(val);
                            break;
                        }
                    }
                }
            }
        }

        assert_eq!(
            found_value,
            Some(pdf_content.to_vec()),
            "File-path auto-converted to bytes"
        );

        std::fs::remove_file(test_file).ok();
    }

    // TEST361: CLI mode with file path - pass file path as command-line argument
    #[test]
    fn test361_cli_mode_file_path() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test361.pdf");
        let pdf_content = b"PDF content for CLI file path test";
        std::fs::write(&test_file, pdf_content).unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:ext=pdf\";process;out=media:void",
            "Process",
            "process",
            vec![CapArg::new(
                MEDIA_FILE_PATH,
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:ext=pdf".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        // CLI mode: pass file path as positional argument
        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();
        let payload = runtime.build_payload_from_cli(&cap, &cli_args).unwrap();

        // Verify payload is CBOR array with file-path argument
        let cbor_val: ciborium::Value = ciborium::from_reader(&payload[..]).unwrap();
        assert!(
            matches!(cbor_val, ciborium::Value::Array(_)),
            "CLI mode produces CBOR array"
        );

        std::fs::remove_file(test_file).ok();
    }

    // TEST362: CLI mode with binary piped in - pipe binary data via stdin
    //
    // This test simulates real-world conditions:
    // - Pure binary data piped to stdin (NOT CBOR)
    // - CLI mode detected (command arg present)
    // - Cap accepts stdin source
    // - Binary is chunked on-the-fly and accumulated
    // - Handler receives complete CBOR payload
    #[test]
    fn test362_cli_mode_piped_binary() {
        use std::io::Cursor;

        // Simulate large binary being piped (1MB PDF)
        let pdf_content = vec![0xAB; 1_000_000];

        // Create cap that accepts stdin
        let cap = create_test_cap(
            "cap:in=\"media:ext=pdf\";process;out=media:void",
            "Process",
            "process",
            vec![CapArg::new(
                "media:ext=pdf",
                true,
                vec![ArgSource::Stdin {
                    stdin: "media:ext=pdf".to_string(),
                }],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        // Mock stdin with Cursor (simulates piped binary)
        let mock_stdin = Cursor::new(pdf_content.clone());

        // Build payload from streaming reader (what CLI piped mode does)
        let payload = runtime
            .build_payload_from_streaming_reader(&cap, mock_stdin, Limits::default().max_chunk)
            .unwrap();

        // Verify payload is CBOR array with correct structure
        let cbor_val: ciborium::Value = ciborium::from_reader(&payload[..]).unwrap();
        match cbor_val {
            ciborium::Value::Array(arr) => {
                assert_eq!(arr.len(), 1, "CBOR array has one argument");

                if let ciborium::Value::Map(map) = &arr[0] {
                    let mut media_urn = None;
                    let mut value = None;

                    for (k, v) in map {
                        if let ciborium::Value::Text(key) = k {
                            match key.as_str() {
                                "media_urn" => {
                                    if let ciborium::Value::Text(s) = v {
                                        media_urn = Some(s.clone());
                                    }
                                }
                                "value" => {
                                    if let ciborium::Value::Bytes(b) = v {
                                        value = Some(b.clone());
                                    }
                                }
                                _ => {}
                            }
                        }
                    }

                    assert_eq!(
                        media_urn,
                        Some("media:ext=pdf".to_string()),
                        "Media URN matches cap in_spec"
                    );
                    assert_eq!(value, Some(pdf_content), "Binary content preserved exactly");
                } else {
                    panic!("Expected Map in CBOR array");
                }
            }
            _ => panic!("Expected CBOR Array"),
        }
    }

    // TEST363: CBOR mode with chunked content - send file content streaming as chunks
    #[tokio::test]
    async fn test363_cbor_mode_chunked_content() {
        use std::sync::{Arc, Mutex};

        let pdf_content = vec![0xAA; 10000]; // 10KB of data
        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);

        let cap = create_test_cap(
            "cap:in=\"media:ext=pdf\";process;out=media:void",
            "Process",
            "process",
            vec![CapArg::new(
                "media:ext=pdf",
                true,
                vec![ArgSource::Stdin {
                    stdin: "media:ext=pdf".to_string(),
                }],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let mut runtime = CartridgeRuntime::with_manifest(manifest);
        runtime.register_op(&cap.urn_string(), move || {
            Box::new(ExtractValueOp {
                received: Arc::clone(&received_clone),
            }) as Box<dyn Op<()>>
        });

        // Build CBOR payload with pdf_content
        let mut payload_bytes = Vec::new();
        let cbor_args = ciborium::Value::Array(vec![ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:ext=pdf".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(pdf_content.clone()),
            ),
        ])]);
        ciborium::into_writer(&cbor_args, &mut payload_bytes).unwrap();

        let factory = runtime.find_handler(&cap.urn_string()).unwrap();

        // Send payload as InputPackage
        let input = test_input_package(&[("media:", &payload_bytes)]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&factory, input, output).await.unwrap();

        assert_eq!(
            *received.lock().unwrap(),
            pdf_content,
            "Handler receives chunked content"
        );
    }

    // TEST364: CBOR mode with file path - send file path in CBOR arguments (auto-conversion)
    #[test]
    fn test364_cbor_mode_file_path() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test364.pdf");
        let pdf_content = b"PDF content for CBOR file path test";
        std::fs::write(&test_file, pdf_content).unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:ext=pdf\";process;out=media:void",
            "Process",
            "process",
            vec![CapArg::new(
                MEDIA_FILE_PATH,
                true,
                vec![ArgSource::Stdin {
                    stdin: "media:ext=pdf".to_string(),
                }],
            )],
        );

        // Build CBOR arguments with file-path URN
        let args = vec![CapArgumentValue::new(
            MEDIA_FILE_PATH.to_string(),
            test_file.to_string_lossy().as_bytes().to_vec(),
        )];
        let mut payload = Vec::new();
        let cbor_args: Vec<ciborium::Value> = args
            .iter()
            .map(|arg| {
                ciborium::Value::Map(vec![
                    (
                        ciborium::Value::Text("media_urn".to_string()),
                        ciborium::Value::Text(arg.media_urn.clone()),
                    ),
                    (
                        ciborium::Value::Text("value".to_string()),
                        ciborium::Value::Bytes(arg.value.clone()),
                    ),
                ])
            })
            .collect();
        ciborium::into_writer(&ciborium::Value::Array(cbor_args), &mut payload).unwrap();

        // Extract effective payload (triggers file-path auto-conversion)
        let effective = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            &cap,
            false, // CBOR mode
        )
        .unwrap();

        // Verify the result is modified CBOR with PDF bytes (not file path)
        let result: ciborium::Value = ciborium::from_reader(&effective[..]).unwrap();
        if let ciborium::Value::Array(arr) = result {
            if let ciborium::Value::Map(map) = &arr[0] {
                let mut media_urn = None;
                let mut value = None;
                for (k, v) in map {
                    if let ciborium::Value::Text(key) = k {
                        match key.as_str() {
                            "media_urn" => {
                                if let ciborium::Value::Text(s) = v {
                                    media_urn = Some(s);
                                }
                            }
                            "value" => {
                                if let ciborium::Value::Bytes(b) = v {
                                    value = Some(b);
                                }
                            }
                            _ => {}
                        }
                    }
                }
                assert_eq!(
                    media_urn,
                    Some(&"media:ext=pdf".to_string()),
                    "URN converted to expected input"
                );
                assert_eq!(
                    value,
                    Some(&pdf_content.to_vec()),
                    "File auto-converted to bytes"
                );
            }
        }

        std::fs::remove_file(test_file).ok();
    }

    // TEST1121: CBOR Array of file-paths in CBOR mode (validates new Array support)
    #[test]
    fn test1121_cbor_array_file_paths_in_cbor_mode() {
        let temp_dir = std::env::temp_dir().join("test361");
        std::fs::create_dir_all(&temp_dir).unwrap();

        // Create three test files
        let file1 = temp_dir.join("file1.txt");
        let file2 = temp_dir.join("file2.txt");
        let file3 = temp_dir.join("file3.txt");
        std::fs::write(&file1, b"content1").unwrap();
        std::fs::write(&file2, b"content2").unwrap();
        std::fs::write(&file3, b"content3").unwrap();

        let mut batch_arg = CapArg::new(
            "media:enc=utf-8;file-path",
            true,
            vec![ArgSource::Stdin {
                stdin: "media:".to_string(),
            }],
        );
        batch_arg.is_sequence = true;

        let cap = create_test_cap(
            "cap:in=media:;batch;out=media:void",
            "Test",
            "batch",
            vec![batch_arg],
        );

        // Build CBOR payload with Array of file paths (CBOR mode only)
        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:enc=utf-8;file-path".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Array(vec![
                    ciborium::Value::Text(file1.to_string_lossy().to_string()),
                    ciborium::Value::Text(file2.to_string_lossy().to_string()),
                    ciborium::Value::Text(file3.to_string_lossy().to_string()),
                ]),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        // Do file-path conversion with is_cli_mode=false (CBOR mode allows Arrays)
        let result =
            extract_effective_payload(&payload, Some("application/cbor"), &cap, false).unwrap();

        // Decode and verify all three files read
        let result_cbor: ciborium::Value = ciborium::from_reader(&result[..]).unwrap();
        let result_array = match result_cbor {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };
        let result_map = match &result_array[0] {
            ciborium::Value::Map(m) => m,
            _ => panic!("Expected map"),
        };
        let files_array = result_map
            .iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| match v {
                ciborium::Value::Array(arr) => arr,
                _ => panic!("Expected array"),
            })
            .unwrap();

        // Verify all three files were read
        assert_eq!(
            files_array.len(),
            3,
            "Should read all three files from CBOR Array"
        );

        // Verify contents
        let mut contents = Vec::new();
        for val in files_array {
            match val {
                ciborium::Value::Bytes(b) => contents.push(b.clone()),
                _ => panic!("Expected bytes"),
            }
        }
        contents.sort();
        assert_eq!(
            contents,
            vec![
                b"content1".to_vec(),
                b"content2".to_vec(),
                b"content3".to_vec()
            ]
        );

        // Verify media_urn was converted
        let media_urn = result_map
            .iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "media_urn"))
            .map(|(_, v)| match v {
                ciborium::Value::Text(s) => s,
                _ => panic!("Expected text"),
            })
            .unwrap();
        assert_eq!(
            media_urn, "media:",
            "media_urn should be converted to stdin source"
        );

        std::fs::remove_dir_all(temp_dir).ok();
    }

    // TEST395: Small payload (< max_chunk) produces correct CBOR arguments
    #[test]
    fn test395_build_payload_small() {
        use std::io::Cursor;

        let cap = create_test_cap(
            "cap:in=media:;process;out=media:void",
            "Process",
            "process",
            vec![],
        );

        let runtime = CartridgeRuntime::new(VALID_MANIFEST.as_bytes());
        let data = b"small payload";
        let reader = Cursor::new(data.to_vec());

        let payload = runtime
            .build_payload_from_streaming_reader(&cap, reader, Limits::default().max_chunk)
            .unwrap();

        // Verify CBOR structure
        let cbor_val: ciborium::Value = ciborium::from_reader(&payload[..]).unwrap();
        match cbor_val {
            ciborium::Value::Array(arr) => {
                assert_eq!(arr.len(), 1, "Should have one argument");
                match &arr[0] {
                    ciborium::Value::Map(map) => {
                        let value = map
                            .iter()
                            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
                            .map(|(_, v)| v)
                            .unwrap();
                        match value {
                            ciborium::Value::Bytes(b) => {
                                assert_eq!(b, &data.to_vec(), "Payload bytes should match");
                            }
                            _ => panic!("Expected Bytes"),
                        }
                    }
                    _ => panic!("Expected Map"),
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    // TEST396: Large payload (> max_chunk) accumulates across chunks correctly
    #[test]
    fn test396_build_payload_large() {
        use std::io::Cursor;

        let cap = create_test_cap(
            "cap:in=media:;process;out=media:void",
            "Process",
            "process",
            vec![],
        );

        let runtime = CartridgeRuntime::new(VALID_MANIFEST.as_bytes());
        // Use small max_chunk to force multi-chunk
        let data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let reader = Cursor::new(data.clone());

        let payload = runtime
            .build_payload_from_streaming_reader(&cap, reader, 100)
            .unwrap();

        let cbor_val: ciborium::Value = ciborium::from_reader(&payload[..]).unwrap();
        let arr = match cbor_val {
            ciborium::Value::Array(a) => a,
            _ => panic!("Expected Array"),
        };
        let map = match &arr[0] {
            ciborium::Value::Map(m) => m,
            _ => panic!("Expected Map"),
        };
        let value = map
            .iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| v)
            .unwrap();
        match value {
            ciborium::Value::Bytes(b) => {
                assert_eq!(b.len(), 1000, "All bytes should be accumulated");
                assert_eq!(b, &data, "Data should match exactly");
            }
            _ => panic!("Expected Bytes"),
        }
    }

    // TEST397: Empty reader produces valid empty CBOR arguments
    #[test]
    fn test397_build_payload_empty() {
        use std::io::Cursor;

        let cap = create_test_cap(
            "cap:in=media:;process;out=media:void",
            "Process",
            "process",
            vec![],
        );

        let runtime = CartridgeRuntime::new(VALID_MANIFEST.as_bytes());
        let reader = Cursor::new(Vec::<u8>::new());

        let payload = runtime
            .build_payload_from_streaming_reader(&cap, reader, Limits::default().max_chunk)
            .unwrap();

        let cbor_val: ciborium::Value = ciborium::from_reader(&payload[..]).unwrap();
        let arr = match cbor_val {
            ciborium::Value::Array(a) => a,
            _ => panic!("Expected Array"),
        };
        let map = match &arr[0] {
            ciborium::Value::Map(m) => m,
            _ => panic!("Expected Map"),
        };
        let value = map
            .iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| v)
            .unwrap();
        match value {
            ciborium::Value::Bytes(b) => {
                assert!(b.is_empty(), "Empty reader should produce empty bytes");
            }
            _ => panic!("Expected Bytes"),
        }
    }

    // TEST398: IO error from reader propagates as RuntimeError::Io
    #[test]
    fn test398_build_payload_io_error() {
        struct ErrorReader;
        impl std::io::Read for ErrorReader {
            fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "simulated read error",
                ))
            }
        }

        let cap = create_test_cap(
            "cap:in=media:;process;out=media:void",
            "Process",
            "process",
            vec![],
        );

        let runtime = CartridgeRuntime::new(VALID_MANIFEST.as_bytes());
        let result = runtime.build_payload_from_streaming_reader(
            &cap,
            ErrorReader,
            Limits::default().max_chunk,
        );

        assert!(result.is_err(), "IO error should propagate");
        match result {
            Err(RuntimeError::Io(e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::BrokenPipe);
            }
            Err(e) => panic!("Expected RuntimeError::Io, got: {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    // TEST478: CartridgeRuntime auto-registers identity and discard handlers on construction
    #[test]
    fn test478_auto_registers_identity_handler() {
        let runtime = CartridgeRuntime::new(VALID_MANIFEST.as_bytes());

        // Identity handler must be registered at exact CAP_IDENTITY URN
        assert!(
            runtime.find_handler(CAP_IDENTITY).is_some(),
            "CartridgeRuntime must auto-register identity handler"
        );

        // Discard handler must be registered at exact CAP_DISCARD URN
        assert!(
            runtime.find_handler(CAP_DISCARD).is_some(),
            "CartridgeRuntime must auto-register discard handler"
        );

        // Standard handlers must NOT match arbitrary specific requests
        // (request is pattern, registered cap is instance — broad caps don't satisfy specific patterns)
        assert!(
            runtime
                .find_handler("cap:in=\"media:void\";nonexistent;out=\"media:void\"")
                .is_none(),
            "Standard handlers must not catch arbitrary specific requests"
        );
    }

    // TEST1282: AdapterSelectionOp is auto-registered by CartridgeRuntime
    #[test]
    fn test1282_adapter_selection_auto_registered() {
        let runtime = CartridgeRuntime::new(VALID_MANIFEST.as_bytes());

        assert!(
            runtime.find_handler(CAP_ADAPTER_SELECTION).is_some(),
            "CartridgeRuntime must auto-register adapter selection handler"
        );
    }

    // TEST1283: Custom adapter selection Op overrides the default
    #[test]
    fn test1283_adapter_selection_custom_override() {
        let mut runtime = CartridgeRuntime::new(VALID_MANIFEST.as_bytes());

        // Verify default is registered
        assert!(runtime.find_handler(CAP_ADAPTER_SELECTION).is_some());

        // Override with custom handler
        #[derive(Default)]
        struct CustomAdapterOp;
        #[async_trait]
        impl Op<()> for CustomAdapterOp {
            async fn perform(&self, _dry: &mut DryContext, _wet: &mut WetContext) -> OpResult<()> {
                Ok(())
            }
            fn metadata(&self) -> OpMetadata {
                OpMetadata::builder("CustomAdapterOp").build()
            }
        }

        runtime.register_op_type::<CustomAdapterOp>(CAP_ADAPTER_SELECTION);

        // Must still find a handler (the custom one)
        assert!(
            runtime.find_handler(CAP_ADAPTER_SELECTION).is_some(),
            "Custom adapter selection handler must be findable after override"
        );
    }

    // TEST479: Custom identity Op overrides auto-registered default
    #[test]
    fn test479_custom_identity_overrides_default() {
        /// Op that always fails (to verify it's the custom handler that gets called)
        #[derive(Default)]
        struct FailOp;
        #[async_trait]
        impl Op<()> for FailOp {
            async fn perform(&self, _dry: &mut DryContext, _wet: &mut WetContext) -> OpResult<()> {
                Err(OpError::ExecutionFailed("custom identity".to_string()))
            }
            fn metadata(&self) -> OpMetadata {
                OpMetadata::builder("FailOp").build()
            }
        }

        let mut runtime = CartridgeRuntime::new(VALID_MANIFEST.as_bytes());

        // Auto-registered identity handler must exist
        assert!(
            runtime.find_handler(CAP_IDENTITY).is_some(),
            "Auto-registered identity must exist before override"
        );

        // Count handlers before override
        let handlers_before = runtime.handlers.len();

        // Override identity with a custom Op
        runtime.register_op_type::<FailOp>(CAP_IDENTITY);

        // Handler count must not change (HashMap insert replaces, doesn't add)
        assert_eq!(
            runtime.handlers.len(),
            handlers_before,
            "Overriding identity must replace, not add a new entry"
        );

        // The handler at CAP_IDENTITY must still be findable
        assert!(
            runtime.find_handler(CAP_IDENTITY).is_some(),
            "Identity handler must be findable after override"
        );

        // Also verify discard was NOT affected by the override
        assert!(
            runtime.find_handler(CAP_DISCARD).is_some(),
            "Discard handler must still be present after overriding identity"
        );
    }

    // =========================================================================
    // Stream Abstractions Tests (InputStream, InputPackage, OutputStream, PeerCall)
    // =========================================================================

    use ciborium::Value;
    use std::sync::Arc;
    use tokio::sync::mpsc::unbounded_channel;

    // Helper: Create test InputStream from chunks (using tokio channels)
    fn create_test_input_stream(
        media_urn: &str,
        chunks: Vec<Result<Value, StreamError>>,
    ) -> InputStream {
        let (tx, rx) = unbounded_channel();
        for chunk in chunks {
            match chunk {
                Ok(value) => tx.send(Ok((value, None))).unwrap(),
                Err(e) => tx.send(Err(e)).unwrap(),
            }
        }
        drop(tx); // Close channel
        InputStream {
            media_urn: media_urn.to_string(),
            stream_meta: None,
            rx: InputRx::Unbounded(rx),
            unbounded: false,
            grants: None,
        }
    }

    // TEST529: InputStream recv yields chunks in order
    #[tokio::test]
    async fn test529_input_stream_recv_order() {
        let chunks = vec![
            Ok(Value::Bytes(b"chunk1".to_vec())),
            Ok(Value::Bytes(b"chunk2".to_vec())),
            Ok(Value::Bytes(b"chunk3".to_vec())),
        ];
        let mut stream = create_test_input_stream("media:test", chunks);

        let mut collected = Vec::new();
        while let Some(item) = stream.recv_data().await {
            collected.push(item);
        }
        assert_eq!(collected.len(), 3);
        assert_eq!(
            collected[0].as_ref().unwrap(),
            &Value::Bytes(b"chunk1".to_vec())
        );
        assert_eq!(
            collected[1].as_ref().unwrap(),
            &Value::Bytes(b"chunk2".to_vec())
        );
        assert_eq!(
            collected[2].as_ref().unwrap(),
            &Value::Bytes(b"chunk3".to_vec())
        );
    }

    // TEST530: InputStream::collect_bytes concatenates byte chunks
    #[tokio::test]
    async fn test530_input_stream_collect_bytes() {
        let chunks = vec![
            Ok(Value::Bytes(b"hello".to_vec())),
            Ok(Value::Bytes(b" ".to_vec())),
            Ok(Value::Bytes(b"world".to_vec())),
        ];
        let stream = create_test_input_stream("media:", chunks);

        let result = stream.collect_bytes().await.expect("collect must succeed");
        assert_eq!(result, b"hello world");
    }

    // TEST531: InputStream::collect_bytes handles text chunks
    #[tokio::test]
    async fn test531_input_stream_collect_bytes_text() {
        let chunks = vec![
            Ok(Value::Text("hello".to_string())),
            Ok(Value::Text(" world".to_string())),
        ];
        let stream = create_test_input_stream("media:text", chunks);

        let result = stream.collect_bytes().await.expect("collect must succeed");
        assert_eq!(result, b"hello world");
    }

    // TEST532: InputStream empty stream produces empty bytes
    #[tokio::test]
    async fn test532_input_stream_empty() {
        let chunks = vec![];
        let stream = create_test_input_stream("media:void", chunks);

        let result = stream
            .collect_bytes()
            .await
            .expect("empty stream must succeed");
        assert_eq!(result, b"");
    }

    // TEST533: InputStream propagates errors
    #[tokio::test]
    async fn test533_input_stream_error_propagation() {
        let chunks = vec![
            Ok(Value::Bytes(b"data".to_vec())),
            Err(StreamError::Protocol("test error".to_string())),
        ];
        let stream = create_test_input_stream("media:test", chunks);

        let result = stream.collect_bytes().await;
        assert!(result.is_err(), "error must propagate");

        if let Err(StreamError::Protocol(msg)) = result {
            assert_eq!(msg, "test error");
        } else {
            panic!("expected Protocol error");
        }
    }

    // TEST534: InputStream::media_urn returns correct URN
    #[test]
    fn test534_input_stream_media_urn() {
        let chunks = vec![Ok(Value::Bytes(b"data".to_vec()))];
        let stream = create_test_input_stream("media:image;format=png", chunks);

        assert_eq!(stream.media_urn(), "media:image;format=png");
    }

    // TEST535: InputPackage recv yields streams
    #[tokio::test]
    async fn test535_input_package_iteration() {
        let (tx, rx) = unbounded_channel();

        // Send 3 streams
        for i in 0..3 {
            let (stream_tx, stream_rx) = unbounded_channel();
            stream_tx
                .send(Ok((
                    Value::Bytes(format!("stream{}", i).into_bytes()),
                    None,
                )))
                .unwrap();
            drop(stream_tx);

            tx.send(Ok(InputStream {
                media_urn: format!("media:stream{}", i),
                stream_meta: None,
                rx: InputRx::Unbounded(stream_rx),
                unbounded: false,
                grants: None,
            }))
            .unwrap();
        }
        drop(tx);

        let mut package = InputPackage {
            rx,
            _demux_handle: None,
        };

        let mut streams = Vec::new();
        while let Some(result) = package.recv().await {
            streams.push(result);
        }
        assert_eq!(streams.len(), 3, "must yield 3 streams");

        for (i, result) in streams.iter().enumerate() {
            assert!(result.is_ok(), "stream {} must be Ok", i);
            let stream = result.as_ref().unwrap();
            assert_eq!(stream.media_urn(), format!("media:stream{}", i));
        }
    }

    // TEST536: InputPackage::collect_all_bytes aggregates all streams
    #[tokio::test]
    async fn test536_input_package_collect_all_bytes() {
        let (tx, rx) = unbounded_channel();

        // Stream 1: "hello"
        let (s1_tx, s1_rx) = unbounded_channel();
        s1_tx
            .send(Ok((Value::Bytes(b"hello".to_vec()), None)))
            .unwrap();
        drop(s1_tx);
        tx.send(Ok(InputStream {
            media_urn: "media:s1".to_string(),
            stream_meta: None,
            rx: InputRx::Unbounded(s1_rx),
            unbounded: false,
            grants: None,
        }))
        .unwrap();

        // Stream 2: " world"
        let (s2_tx, s2_rx) = unbounded_channel();
        s2_tx
            .send(Ok((Value::Bytes(b" world".to_vec()), None)))
            .unwrap();
        drop(s2_tx);
        tx.send(Ok(InputStream {
            media_urn: "media:s2".to_string(),
            stream_meta: None,
            rx: InputRx::Unbounded(s2_rx),
            unbounded: false,
            grants: None,
        }))
        .unwrap();

        drop(tx);

        let package = InputPackage {
            rx,
            _demux_handle: None,
        };

        let all_bytes = package.collect_all_bytes().await.expect("must succeed");
        assert_eq!(all_bytes, b"hello world");
    }

    // TEST537: InputPackage empty package produces empty bytes
    #[tokio::test]
    async fn test537_input_package_empty() {
        let (tx, rx) = unbounded_channel();
        drop(tx); // No streams

        let package = InputPackage {
            rx,
            _demux_handle: None,
        };

        let all_bytes = package
            .collect_all_bytes()
            .await
            .expect("empty package must succeed");
        assert_eq!(all_bytes, b"");
    }

    // TEST538: InputPackage propagates stream errors
    #[tokio::test]
    async fn test538_input_package_error_propagation() {
        let (tx, rx) = unbounded_channel();

        // Good stream
        let (s1_tx, s1_rx) = unbounded_channel();
        s1_tx
            .send(Ok((Value::Bytes(b"data".to_vec()), None)))
            .unwrap();
        drop(s1_tx);
        tx.send(Ok(InputStream {
            media_urn: "media:good".to_string(),
            stream_meta: None,
            rx: InputRx::Unbounded(s1_rx),
            unbounded: false,
            grants: None,
        }))
        .unwrap();

        // Error stream
        let (s2_tx, s2_rx) = unbounded_channel();
        s2_tx
            .send(Err(StreamError::Protocol("stream error".to_string())))
            .unwrap();
        drop(s2_tx);
        tx.send(Ok(InputStream {
            media_urn: "media:bad".to_string(),
            stream_meta: None,
            rx: InputRx::Unbounded(s2_rx),
            unbounded: false,
            grants: None,
        }))
        .unwrap();

        drop(tx);

        let package = InputPackage {
            rx,
            _demux_handle: None,
        };

        let result = package.collect_all_bytes().await;
        assert!(result.is_err(), "error must propagate from bad stream");
    }

    // Mock FrameSender for testing OutputStream
    struct MockFrameSender {
        frames: Arc<Mutex<Vec<Frame>>>,
    }

    impl MockFrameSender {
        fn new() -> (Self, Arc<Mutex<Vec<Frame>>>) {
            let frames = Arc::new(Mutex::new(Vec::new()));
            let sender = Self {
                frames: Arc::clone(&frames),
            };
            (sender, frames)
        }
    }

    impl FrameSender for MockFrameSender {
        fn send(&self, frame: &Frame) -> Result<(), RuntimeError> {
            self.frames.lock().unwrap().push(frame.clone());
            Ok(())
        }
    }

    // TEST8126: derive_response_media — the response label is the effect
    // inference over the declared input, per effect value; an unparseable
    // cap URN fails hard instead of falling back.
    #[test]
    fn test8126_derive_response_media_per_effect() {
        // effect=declared (default): the declared out=.
        assert_eq!(
            derive_response_media("cap:extract;in=\"media:ext=pdf\";out=\"media:record\"")
                .unwrap(),
            "media:record"
        );
        // effect=none: the declared in= passes through.
        assert_eq!(
            derive_response_media(
                "cap:decimate-sequence;effect=none;in=\"media:ext=png;image\";out=\"media:image\""
            )
            .unwrap(),
            "media:ext=png;image"
        );
        // effect=patch: the declared in= with the declared delta applied
        // (which reconstructs the declared out= at the declared-input base).
        assert_eq!(
            derive_response_media(
                "cap:convert;effect=patch;in=\"media:ext=jpeg;image\";out=\"media:ext=png;image\""
            )
            .unwrap(),
            "media:ext=png;image"
        );
        // An unparseable cap URN is a broken declaration: hard error.
        assert!(derive_response_media("not-a-cap-urn").is_err());
    }

    // TEST539: OutputStream sends STREAM_START on first write
    #[tokio::test]
    async fn test539_output_stream_sends_stream_start() {
        let (sender, frames) = MockFrameSender::new();
        let mut stream = OutputStream::new(
            Arc::new(sender),
            "stream-1".to_string(),
            "media:test".to_string(),
            MessageId::new_uuid(),
            None,
            256_000,
        );

        stream.start(false, None).expect("start must succeed");
        stream
            .emit_cbor(&Value::Bytes(b"test".to_vec()))
            .await
            .expect("write must succeed");

        let captured = frames.lock().unwrap();
        assert!(captured.len() >= 1, "must send at least STREAM_START");
        assert_eq!(
            captured[0].frame_type,
            FrameType::StreamStart,
            "first frame must be STREAM_START"
        );
        assert_eq!(captured[0].stream_id, Some("stream-1".to_string()));
    }

    // TEST540: OutputStream::close sends STREAM_END with correct chunk_count
    #[tokio::test]
    async fn test540_output_stream_close_sends_stream_end() {
        let (sender, frames) = MockFrameSender::new();
        let mut stream = OutputStream::new(
            Arc::new(sender),
            "stream-1".to_string(),
            "media:test".to_string(),
            MessageId::new_uuid(),
            None,
            256_000,
        );

        // Three small byte emissions in quick succession COALESCE into one
        // CHUNK (scalar-stream chunk boundaries are non-semantic), flushed by
        // close() BEFORE the STREAM_END that promises the count — coalescing
        // must be lossless and the count must match what was written.
        stream.start(false, None).unwrap();
        stream
            .emit_cbor(&Value::Bytes(b"chunk1".to_vec()))
            .await
            .unwrap();
        stream
            .emit_cbor(&Value::Bytes(b"chunk2".to_vec()))
            .await
            .unwrap();
        stream
            .emit_cbor(&Value::Bytes(b"chunk3".to_vec()))
            .await
            .unwrap();

        stream.close().await.expect("close must succeed");

        let captured = frames.lock().unwrap();
        let chunks: Vec<_> = captured
            .iter()
            .filter(|f| f.frame_type == FrameType::Chunk)
            .collect();
        assert_eq!(
            chunks.len(),
            1,
            "small rapid emissions coalesce into one chunk"
        );
        let payload: Vec<u8> = {
            let decoded: ciborium::Value =
                ciborium::from_reader(chunks[0].payload.as_ref().unwrap().as_slice())
                    .expect("chunk payload is one CBOR value");
            match decoded {
                ciborium::Value::Bytes(b) => b,
                other => panic!("coalesced chunk must be CBOR Bytes, got {other:?}"),
            }
        };
        assert_eq!(
            payload, b"chunk1chunk2chunk3",
            "coalescing is lossless and order-preserving"
        );
        let stream_end = captured
            .iter()
            .find(|f| f.frame_type == FrameType::StreamEnd)
            .expect("must have STREAM_END");
        assert_eq!(
            stream_end.chunk_count,
            Some(1),
            "STREAM_END promises the COALESCED chunk count"
        );
        let end_pos = captured
            .iter()
            .position(|f| f.frame_type == FrameType::StreamEnd)
            .unwrap();
        let chunk_pos = captured
            .iter()
            .position(|f| f.frame_type == FrameType::Chunk)
            .unwrap();
        assert!(
            chunk_pos < end_pos,
            "the flushed tail ships BEFORE STREAM_END"
        );
    }

    // TEST541: OutputStream chunks large data correctly
    #[tokio::test]
    async fn test541_output_stream_chunks_large_data() {
        let (sender, frames) = MockFrameSender::new();
        let max_chunk = 100; // Small chunk size for testing
        let mut stream = OutputStream::new(
            Arc::new(sender),
            "stream-1".to_string(),
            "media:".to_string(),
            MessageId::new_uuid(),
            None,
            max_chunk,
        );

        // Write 250 bytes (should create 3 chunks: 100, 100, 50)
        stream.start(false, None).unwrap();
        let large_data = vec![0xAA; 250];
        stream.emit_cbor(&Value::Bytes(large_data)).await.unwrap();
        stream.close().await.unwrap();

        let captured = frames.lock().unwrap();
        let chunks: Vec<_> = captured
            .iter()
            .filter(|f| f.frame_type == FrameType::Chunk)
            .collect();

        assert!(
            chunks.len() >= 3,
            "large data must be chunked (got {} chunks)",
            chunks.len()
        );
    }

    // TEST542: OutputStream empty stream sends STREAM_START and STREAM_END only
    #[test]
    fn test542_output_stream_empty() {
        let (sender, frames) = MockFrameSender::new();
        let mut stream = OutputStream::new(
            Arc::new(sender),
            "stream-1".to_string(),
            "media:void".to_string(),
            MessageId::new_uuid(),
            None,
            256_000,
        );

        stream.start(false, None).expect("start must succeed");
        stream.blocking_close().expect("close must succeed");

        let captured = frames.lock().unwrap();
        assert!(captured
            .iter()
            .any(|f| f.frame_type == FrameType::StreamStart));
        assert!(captured
            .iter()
            .any(|f| f.frame_type == FrameType::StreamEnd));

        let chunk_count = captured
            .iter()
            .filter(|f| f.frame_type == FrameType::Chunk)
            .count();
        assert_eq!(chunk_count, 0, "empty stream must have zero chunks");
    }

    // TEST543: PeerCall::arg creates OutputStream with correct stream_id
    #[test]
    fn test543_peer_call_arg_creates_stream() {
        let (sender, _frames) = MockFrameSender::new();
        let (_response_tx, response_rx) = unbounded_channel();

        let peer = PeerCall {
            sender: Arc::new(sender),
            request_id: MessageId::new_uuid(),
            max_chunk: 256_000,
            response_rx: Some(response_rx),
            credit_router: None,
            initial_credit: crate::bifaci::frame::DEFAULT_INITIAL_CREDIT,
        };

        let arg_stream = peer.arg("media:argument");
        assert_eq!(arg_stream.media_urn, "media:argument");
        assert!(
            !arg_stream.stream_id.is_empty(),
            "stream_id must be generated"
        );
    }

    // TEST544: PeerCall::finish sends END frame
    #[tokio::test]
    async fn test544_peer_call_finish_sends_end() {
        let (sender, frames) = MockFrameSender::new();
        let (response_tx, response_rx) = unbounded_channel();

        // Close response channel immediately (simulates empty response)
        drop(response_tx);

        let request_id = MessageId::new_uuid();
        let peer = PeerCall {
            sender: Arc::new(sender),
            request_id: request_id.clone(),
            max_chunk: 256_000,
            response_rx: Some(response_rx),
            credit_router: None,
            initial_credit: crate::bifaci::frame::DEFAULT_INITIAL_CREDIT,
        };

        let _response = peer.finish().await.expect("finish must succeed");

        let captured = frames.lock().unwrap();
        let end_frame = captured
            .iter()
            .find(|f| f.frame_type == FrameType::End)
            .expect("must send END frame");

        assert_eq!(end_frame.id, request_id, "END must have correct request ID");
    }

    // TEST545: PeerCall::finish returns PeerResponse with data
    #[tokio::test]
    async fn test545_peer_call_finish_returns_response_stream() {
        let (sender, _frames) = MockFrameSender::new();
        let (response_tx, response_rx) = unbounded_channel();

        // Send response frames (simulating STREAM_START + CHUNK + STREAM_END)
        let req_id = MessageId::new_uuid();

        // STREAM_START
        let mut start = Frame::new(FrameType::StreamStart, req_id.clone());
        start.stream_id = Some("response-stream".to_string());
        start.media_urn = Some("media:response".to_string());
        response_tx.send(start).unwrap();

        // CHUNK - payload must be CBOR-encoded
        let raw_data = b"response data".to_vec();
        let mut cbor_payload = Vec::new();
        ciborium::into_writer(&Value::Bytes(raw_data.clone()), &mut cbor_payload).unwrap();
        let checksum = Frame::compute_checksum(&cbor_payload);
        response_tx
            .send(Frame::chunk(
                req_id.clone(),
                "response-stream".to_string(),
                0,
                cbor_payload,
                0,
                checksum,
            ))
            .unwrap();

        // STREAM_END
        response_tx
            .send(Frame::stream_end(
                req_id.clone(),
                "response-stream".to_string(),
                1,
            ))
            .unwrap();
        drop(response_tx);

        let peer = PeerCall {
            sender: Arc::new(sender),
            request_id: req_id,
            max_chunk: 256_000,
            response_rx: Some(response_rx),
            credit_router: None,
            initial_credit: crate::bifaci::frame::DEFAULT_INITIAL_CREDIT,
        };

        let response = peer.finish().await.expect("finish must succeed");

        let bytes = response
            .collect_bytes()
            .await
            .expect("collect must succeed");
        assert_eq!(bytes, b"response data");
    }

    // TEST839: LOG frames arriving BEFORE StreamStart are delivered immediately
    //
    // This tests the critical fix: during a peer call, the peer (e.g., modelcartridge)
    // sends LOG frames for minutes during model download BEFORE sending any data
    // (StreamStart + Chunk). The handler must receive these LOGs in real-time so it
    // can re-emit progress and keep the engine's activity timer alive.
    //
    // Previously, demux_single_stream blocked on awaiting StreamStart before returning
    // PeerResponse, which meant the handler couldn't call recv() until data arrived —
    // causing 120s activity timeouts during long downloads.
    #[tokio::test]
    async fn test839_peer_response_delivers_logs_before_stream_start() {
        let (sender, _frames) = MockFrameSender::new();
        let (response_tx, response_rx) = unbounded_channel();

        let req_id = MessageId::new_uuid();

        // Send LOG frames BEFORE any StreamStart — simulates modelcartridge
        // sending download progress before the actual data response
        response_tx
            .send(Frame::progress(
                req_id.clone(),
                0.1,
                "downloading file 1/10",
            ))
            .unwrap();
        response_tx
            .send(Frame::progress(
                req_id.clone(),
                0.5,
                "downloading file 5/10",
            ))
            .unwrap();
        response_tx
            .send(Frame::log(
                req_id.clone(),
                "status",
                crate::AttributionClass::Internal,
                "large file in progress",
                None,
            ))
            .unwrap();

        let peer = PeerCall {
            sender: Arc::new(sender),
            request_id: req_id.clone(),
            max_chunk: 256_000,
            response_rx: Some(response_rx),
            credit_router: None,
            initial_credit: crate::bifaci::frame::DEFAULT_INITIAL_CREDIT,
        };

        // finish() must return immediately — NOT block waiting for StreamStart
        let mut response = peer.finish().await.expect("finish must succeed");

        // Handler must be able to recv() LOG frames right away
        let item1 = response.recv().await.expect("first LOG must arrive");
        match item1 {
            PeerResponseItem::Log(f) => {
                assert_eq!(f.log_progress(), Some(0.1));
                assert_eq!(f.log_message(), Some("downloading file 1/10"));
            }
            PeerResponseItem::Data(..) => panic!("expected LOG frame, got Data"),
        }

        let item2 = response.recv().await.expect("second LOG must arrive");
        match item2 {
            PeerResponseItem::Log(f) => {
                assert_eq!(f.log_progress(), Some(0.5));
                assert_eq!(f.log_message(), Some("downloading file 5/10"));
            }
            PeerResponseItem::Data(..) => panic!("expected LOG frame, got Data"),
        }

        let item3 = response.recv().await.expect("third LOG must arrive");
        match item3 {
            PeerResponseItem::Log(f) => {
                assert_eq!(f.log_message(), Some("large file in progress"));
            }
            PeerResponseItem::Data(..) => panic!("expected LOG frame, got Data"),
        }

        // Now send the actual data (StreamStart, Chunk, StreamEnd, End)
        let mut start = Frame::new(FrameType::StreamStart, req_id.clone());
        start.stream_id = Some("s1".to_string());
        start.media_urn = Some("media:binary".to_string());
        response_tx.send(start).unwrap();

        let raw_data = b"model output".to_vec();
        let mut cbor_payload = Vec::new();
        ciborium::into_writer(&Value::Bytes(raw_data.clone()), &mut cbor_payload).unwrap();
        let checksum = Frame::compute_checksum(&cbor_payload);
        response_tx
            .send(Frame::chunk(
                req_id.clone(),
                "s1".to_string(),
                0,
                cbor_payload,
                0,
                checksum,
            ))
            .unwrap();

        response_tx
            .send(Frame::stream_end(req_id.clone(), "s1".to_string(), 1))
            .unwrap();
        drop(response_tx);

        // Data must arrive after the LOGs
        let item4 = response.recv().await.expect("data item must arrive");
        match item4 {
            PeerResponseItem::Data(Ok(value), _meta) => {
                assert_eq!(value, Value::Bytes(b"model output".to_vec()));
            }
            PeerResponseItem::Data(Err(e), _) => panic!("expected data, got error: {}", e),
            PeerResponseItem::Log(_) => panic!("expected Data, got LOG"),
        }

        assert!(
            response.recv().await.is_none(),
            "stream must end after STREAM_END"
        );
    }

    // TEST7118: finite peer collection preserves source diagnostics instead
    // of consuming them as data or dropping them. Progress is mapped into the
    // caller's range and argument attribution survives byte-for-byte.
    #[tokio::test]
    async fn test7118_collect_bytes_forwarding_preserves_peer_side_channels() {
        let request_id = MessageId::new_uuid();
        let (item_tx, item_rx) = unbounded_channel();
        item_tx
            .send(PeerResponseItem::Log(Frame::progress(
                request_id.clone(),
                0.5,
                "halfway",
            )))
            .unwrap();
        item_tx
            .send(PeerResponseItem::Log(Frame::log(
                request_id,
                "warn",
                crate::failure::AttributionClass::Resource,
                "cache pressure",
                Some("media:model-spec"),
            )))
            .unwrap();
        item_tx
            .send(PeerResponseItem::Data(
                Ok(ciborium::Value::Text("payload".to_string())),
                None,
            ))
            .unwrap();
        drop(item_tx);

        let response = PeerResponse {
            rx: item_rx,
            grants: None,
        };
        let (sender, frames) = MockFrameSender::new();
        let output = OutputStream::new(
            Arc::new(sender),
            "output".to_string(),
            "media:test".to_string(),
            MessageId::new_uuid(),
            None,
            256_000,
        );

        let bytes = response
            .collect_bytes_forwarding(&output, 0.2, 0.4)
            .await
            .unwrap();
        assert_eq!(bytes, b"payload");

        let frames = frames.lock().unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].log_progress(), Some(0.4));
        assert_eq!(frames[0].log_message(), Some("halfway"));
        assert_eq!(
            frames[1].attribution_class(),
            Ok(crate::failure::AttributionClass::Resource)
        );
        assert_eq!(
            frames[1].attribution_arg_urn().unwrap(),
            Some("media:model-spec")
        );
    }

    // TEST1949: a peer progress LOG with no numeric value FAILS HARD. Forwarding
    // must not silently drop it or substitute a value — a malformed frame is an
    // emitter defect and must surface as one, which is exactly the failure the
    // engine raises for the same frame.
    #[tokio::test]
    async fn test1949_peer_progress_without_numeric_value_fails_hard() {
        let request_id = MessageId::new_uuid();
        let (item_tx, item_rx) = unbounded_channel();

        // level="progress" with no `progress` key — malformed at the emitter.
        let mut malformed = Frame::new(FrameType::Log, request_id);
        let mut meta = std::collections::BTreeMap::new();
        meta.insert(
            "level".to_string(),
            ciborium::Value::Text("progress".to_string()),
        );
        meta.insert(
            "message".to_string(),
            ciborium::Value::Text("no number here".to_string()),
        );
        malformed.meta = Some(meta);
        item_tx.send(PeerResponseItem::Log(malformed)).unwrap();
        drop(item_tx);

        let response = PeerResponse {
            rx: item_rx,
            grants: None,
        };
        let (sender, frames) = MockFrameSender::new();
        let output = OutputStream::new(
            Arc::new(sender),
            "output".to_string(),
            "media:test".to_string(),
            MessageId::new_uuid(),
            None,
            256_000,
        );

        let error = response
            .collect_bytes_forwarding(&output, 0.0, 1.0)
            .await
            .expect_err("a progress LOG without a numeric value must fail, not pass silently");
        let message = error.to_string();
        assert!(
            message.contains("progress"),
            "the failure must name the missing progress value: {message}"
        );
        assert!(
            frames.lock().unwrap().is_empty(),
            "no frame may be emitted from a malformed peer frame"
        );
    }

    // TEST840: PeerResponse::collect_bytes rejects unhandled LOG frames.
    #[tokio::test]
    async fn test840_peer_response_collect_bytes_rejects_unhandled_logs() {
        let (sender, _frames) = MockFrameSender::new();
        let (response_tx, response_rx) = unbounded_channel();

        let req_id = MessageId::new_uuid();

        // STREAM_START
        let mut start = Frame::new(FrameType::StreamStart, req_id.clone());
        start.stream_id = Some("s1".to_string());
        start.media_urn = Some("media:binary".to_string());
        response_tx.send(start).unwrap();

        // LOG frames require explicit forwarding.
        response_tx
            .send(Frame::progress(req_id.clone(), 0.25, "working"))
            .unwrap();
        response_tx
            .send(Frame::progress(req_id.clone(), 0.75, "almost"))
            .unwrap();

        // CHUNK
        let mut cbor_payload = Vec::new();
        ciborium::into_writer(&Value::Bytes(b"hello".to_vec()), &mut cbor_payload).unwrap();
        let checksum = Frame::compute_checksum(&cbor_payload);
        response_tx
            .send(Frame::chunk(
                req_id.clone(),
                "s1".to_string(),
                0,
                cbor_payload,
                0,
                checksum,
            ))
            .unwrap();

        // Another LOG
        response_tx
            .send(Frame::log(
                req_id.clone(),
                "info",
                crate::AttributionClass::Internal,
                "done",
                None,
            ))
            .unwrap();

        // STREAM_END
        response_tx
            .send(Frame::stream_end(req_id.clone(), "s1".to_string(), 1))
            .unwrap();
        drop(response_tx);

        let peer = PeerCall {
            sender: Arc::new(sender),
            request_id: req_id,
            max_chunk: 256_000,
            response_rx: Some(response_rx),
            credit_router: None,
            initial_credit: crate::bifaci::frame::DEFAULT_INITIAL_CREDIT,
        };

        let response = peer.finish().await.expect("finish must succeed");
        let error = response
            .collect_bytes()
            .await
            .expect_err("collect must reject an unhandled diagnostic");
        assert!(error.to_string().contains("explicit diagnostic forwarding"));
    }

    // TEST841: PeerResponse::collect_value rejects unhandled LOG frames.
    #[tokio::test]
    async fn test841_peer_response_collect_value_rejects_unhandled_logs() {
        let (sender, _frames) = MockFrameSender::new();
        let (response_tx, response_rx) = unbounded_channel();

        let req_id = MessageId::new_uuid();

        // STREAM_START
        let mut start = Frame::new(FrameType::StreamStart, req_id.clone());
        start.stream_id = Some("s1".to_string());
        start.media_urn = Some("media:binary".to_string());
        response_tx.send(start).unwrap();

        // LOG frames before the data value
        response_tx
            .send(Frame::progress(req_id.clone(), 0.5, "half"))
            .unwrap();
        response_tx
            .send(Frame::log(
                req_id.clone(),
                "debug",
                crate::AttributionClass::Internal,
                "processing",
                None,
            ))
            .unwrap();

        // Single CHUNK with a CBOR integer
        let mut cbor_payload = Vec::new();
        ciborium::into_writer(&Value::Integer(42.into()), &mut cbor_payload).unwrap();
        let checksum = Frame::compute_checksum(&cbor_payload);
        response_tx
            .send(Frame::chunk(
                req_id.clone(),
                "s1".to_string(),
                0,
                cbor_payload,
                0,
                checksum,
            ))
            .unwrap();

        // STREAM_END
        response_tx
            .send(Frame::stream_end(req_id.clone(), "s1".to_string(), 1))
            .unwrap();
        drop(response_tx);

        let peer = PeerCall {
            sender: Arc::new(sender),
            request_id: req_id,
            max_chunk: 256_000,
            response_rx: Some(response_rx),
            credit_router: None,
            initial_credit: crate::bifaci::frame::DEFAULT_INITIAL_CREDIT,
        };

        let response = peer.finish().await.expect("finish must succeed");
        let error = response
            .collect_value()
            .await
            .expect_err("collect must reject an unhandled diagnostic");
        assert!(error.to_string().contains("explicit diagnostic forwarding"));
    }

    // ==================== find_stream / require_stream Tests ====================

    // TEST678: find_stream with exact equivalent URN (same tags, different order) succeeds
    #[test]
    fn test678_find_stream_equivalent_urn_different_tag_order() {
        let streams = vec![(
            "media:fmt=json;llm-generation-request;record".to_string(),
            b"data".to_vec(),
            None,
        )];
        // Tags in different order — is_equivalent is order-independent
        let found = super::find_stream(&streams, "media:fmt=json;llm-generation-request;record");
        assert!(
            found.is_some(),
            "Same tags in different order must match via is_equivalent"
        );
        assert_eq!(found.unwrap(), b"data");
    }

    // TEST679: find_stream with base URN vs full URN fails — is_equivalent is strict
    // This is the root cause of the cartridge_client.rs bug. Sender sent
    // "media:llm-generation-request" but receiver looked for
    // "media:fmt=json;llm-generation-request;record".
    #[test]
    fn test679_find_stream_base_urn_does_not_match_full_urn() {
        let streams = vec![(
            "media:llm-generation-request".to_string(),
            b"data".to_vec(),
            None,
        )];
        let found = super::find_stream(&streams, "media:fmt=json;llm-generation-request;record");
        assert!(
            found.is_none(),
            "Base URN without tags must NOT match full URN with tags"
        );
    }

    // TEST680: require_stream with missing URN returns hard StreamError
    #[test]
    fn test680_require_stream_missing_urn_returns_error() {
        let streams = vec![(
            "media:enc=utf-8;model-spec".to_string(),
            b"gpt-4".to_vec(),
            None,
        )];
        let result =
            super::require_stream(&streams, "media:fmt=json;llm-generation-request;record");
        assert!(result.is_err(), "Missing stream must fail hard");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("media:fmt=json;llm-generation-request;record"),
            "Error must name the missing media URN, got: {}",
            err
        );
    }

    // TEST681: find_stream with multiple streams returns the correct one
    #[test]
    fn test681_find_stream_multiple_streams_returns_correct() {
        let streams = vec![
            (
                "media:enc=utf-8;model-spec".to_string(),
                b"gpt-4".to_vec(),
                None,
            ),
            (
                "media:fmt=json;llm-generation-request;record".to_string(),
                b"{\"prompt\":\"test\"}".to_vec(),
                None,
            ),
            (
                "media:numeric;temperature".to_string(),
                b"0.7".to_vec(),
                None,
            ),
        ];
        let found = super::find_stream(&streams, "media:fmt=json;llm-generation-request;record");
        assert!(found.is_some());
        assert_eq!(found.unwrap(), b"{\"prompt\":\"test\"}");
    }

    // TEST682: require_stream_str returns UTF-8 string for text data
    #[test]
    fn test682_require_stream_str_returns_utf8() {
        let streams = vec![("media:enc=utf-8".to_string(), b"hello world".to_vec(), None)];
        let result = super::require_stream_str(&streams, "media:enc=utf-8");
        assert_eq!(result.unwrap(), "hello world");
    }

    // TEST683: find_stream returns None for invalid media URN string (not a parse error — just None)
    #[test]
    fn test683_find_stream_invalid_urn_returns_none() {
        let streams = vec![("media:valid".to_string(), b"data".to_vec(), None)];
        // Empty string is not a valid media URN
        let found = super::find_stream(&streams, "");
        assert!(found.is_none(), "Invalid URN must return None, not panic");
    }

    // TEST842: run_with_keepalive returns closure result (fast operation, no keepalive PROGRESS frames).
    //
    // `run_with_keepalive` emits two distinct families of Log
    // frames: keepalive PROGRESS ticks (built via `Frame::progress`,
    // `meta.level == "progress"`, fired only when the 5s ticker
    // expires) and diagnostic ticker-lifecycle frames (built via
    // the local `keepalive_log_frame` helper, `meta.level ==
    // "debug"`, ALWAYS fired once at start and once at stop —
    // independent of how long the work took). For an instant
    // operation we expect exactly the two diagnostic frames and
    // zero progress frames. Filtering by `frame_type == Log`
    // alone would also match the diagnostic frames and produce a
    // false positive; the test must discriminate by the `level`
    // meta field, not the frame type.
    #[tokio::test]
    async fn test842_run_with_keepalive_returns_result() {
        let (sender, frames) = MockFrameSender::new();
        let stream = OutputStream::new(
            Arc::new(sender),
            "stream-1".to_string(),
            "media:test".to_string(),
            MessageId::new_uuid(),
            None,
            DEFAULT_MAX_CHUNK,
        );

        // Run a fast operation — no keepalive PROGRESS frame
        // expected (the 5s ticker won't fire before completion).
        let result: i32 = stream
            .run_with_keepalive(0.25, "Loading model", || 42)
            .await;
        assert_eq!(result, 42, "Closure result must be returned");

        let captured = frames.lock().unwrap();
        let progress_ticks: Vec<_> = captured
            .iter()
            .filter(|f| {
                if f.frame_type != FrameType::Log {
                    return false;
                }
                f.meta
                    .as_ref()
                    .and_then(|m| m.get("level"))
                    .and_then(|v| match v {
                        ciborium::Value::Text(s) => Some(s.as_str()),
                        _ => None,
                    })
                    == Some("progress")
            })
            .collect();
        assert_eq!(
            progress_ticks.len(),
            0,
            "No keepalive PROGRESS tick for instant operation. \
             Diagnostic ticker-lifecycle frames (level=\"debug\") are expected \
             and not counted here. Total Log frames captured: {}.",
            captured
                .iter()
                .filter(|f| f.frame_type == FrameType::Log)
                .count()
        );
        let diagnostics: Vec<_> = captured
            .iter()
            .filter(|frame| {
                frame.frame_type == FrameType::Log && frame.log_level() != Some("progress")
            })
            .collect();
        assert!(
            !diagnostics.is_empty(),
            "the synchronous ticker-start diagnostic must be observable"
        );
        for diagnostic in diagnostics {
            assert_eq!(
                diagnostic.attribution_class(),
                Ok(crate::AttributionClass::Internal),
                "keepalive lifecycle diagnostics must satisfy the attributed LOG wire contract",
            );
        }
    }

    // TEST843: run_with_keepalive returns Ok/Err from closure
    #[tokio::test]
    async fn test843_run_with_keepalive_returns_result_type() {
        let (sender, _frames) = MockFrameSender::new();
        let stream = OutputStream::new(
            Arc::new(sender),
            "stream-1".to_string(),
            "media:test".to_string(),
            MessageId::new_uuid(),
            None,
            DEFAULT_MAX_CHUNK,
        );

        let result: Result<String, String> = stream
            .run_with_keepalive(0.5, "Loading", || Ok("model_loaded".to_string()))
            .await;
        assert_eq!(result.unwrap(), "model_loaded");
    }

    // TEST844: run_with_keepalive propagates errors from closure
    #[tokio::test]
    async fn test844_run_with_keepalive_propagates_error() {
        let (sender, _frames) = MockFrameSender::new();
        let stream = OutputStream::new(
            Arc::new(sender),
            "stream-1".to_string(),
            "media:test".to_string(),
            MessageId::new_uuid(),
            None,
            DEFAULT_MAX_CHUNK,
        );

        let result: Result<(), RuntimeError> = stream
            .run_with_keepalive(0.25, "Loading", || {
                Err(RuntimeError::Handler("load failed".to_string()))
            })
            .await;
        assert!(result.is_err(), "Error from closure must propagate");
        let err = result.unwrap_err();
        match err {
            RuntimeError::Handler(msg) => assert_eq!(msg, "load failed"),
            other => panic!("Expected Handler error, got: {:?}", other),
        }
    }

    // TEST845: ProgressSender emits progress and log frames independently of OutputStream
    #[test]
    fn test845_progress_sender_emits_frames() {
        let (sender, frames) = MockFrameSender::new();
        let stream = OutputStream::new(
            Arc::new(sender),
            "stream-1".to_string(),
            "media:test".to_string(),
            MessageId::new_uuid(),
            None,
            DEFAULT_MAX_CHUNK,
        );

        let ps = stream.progress_sender();
        ps.progress(0.5, "halfway there");
        ps.log(
            "info",
            crate::AttributionClass::Internal,
            "loading complete",
        );

        let captured = frames.lock().unwrap();
        assert_eq!(captured.len(), 2, "ProgressSender should emit 2 frames");
        assert_eq!(captured[0].frame_type, FrameType::Log);
        assert_eq!(captured[1].frame_type, FrameType::Log);
        // Verify progress frame has correct progress value
        assert_eq!(captured[0].log_progress(), Some(0.5));
        assert_eq!(captured[0].log_message(), Some("halfway there"));
        // Verify log frame
        assert_eq!(captured[1].log_level(), Some("info"));
        assert_eq!(captured[1].log_message(), Some("loading complete"));
    }

    /// Verify get_own_memory_mb returns non-zero values on macOS.
    /// This function calls proc_pid_rusage(getpid()) which must always work —
    /// even in a sandbox. If it returns None on macOS, the self-reporting
    /// mechanism is broken and cartridges will report 0 footprint.
    #[test]
    #[cfg(target_os = "macos")]
    // TEST1270: Runtime memory inspection returns non-negative resident and virtual memory values.
    fn test1270_get_own_memory_mb_returns_values() {
        let result = get_own_memory_mb();
        assert!(
            result.is_some(),
            "proc_pid_rusage(getpid()) must succeed on macOS"
        );
        let (footprint_mb, rss_mb) = result.unwrap();
        // A running test process should use at least some memory
        assert!(
            rss_mb > 0,
            "RSS should be non-zero for a running process, got {}",
            rss_mb
        );
        // Footprint should also be non-zero (it's the physical memory charged to us)
        assert!(
            footprint_mb > 0,
            "Footprint should be non-zero for a running process, got {}",
            footprint_mb
        );
    }

    /// Decode a CHUNK frame's payload as the CBOR Bytes value every scalar
    /// stream carries, returning the inner bytes.
    fn chunk_inner_bytes(frame: &Frame) -> Vec<u8> {
        let decoded: ciborium::Value =
            ciborium::from_reader(frame.payload.as_ref().expect("chunk has payload").as_slice())
                .expect("chunk payload is one CBOR value");
        match decoded {
            ciborium::Value::Bytes(b) => b,
            other => panic!("scalar chunk must be CBOR Bytes, got {other:?}"),
        }
    }

    // TEST8119: the coalescing AGE bound — a write arriving after the buffer's
    // oldest byte crossed COALESCE_MAX_AGE flushes the accumulated batch, so
    // steady token emission lags the wire by at most one write-gap; the tail
    // written after that flush ships with close(). Nothing is lost, order is
    // preserved, and the frame count is the batch count, not the write count.
    #[tokio::test]
    async fn test8119_coalesce_age_bound_flushes_on_next_write() {
        let (sender, frames) = MockFrameSender::new();
        let stream = OutputStream::new(
            Arc::new(sender),
            "stream-1".to_string(),
            "media:enc=utf-8".to_string(),
            MessageId::new_uuid(),
            None,
            256_000,
        );
        stream.start(false, None).unwrap();

        stream.write(b"ab").await.unwrap();
        stream.write(b"cd").await.unwrap();
        // Cross the age bound, then write again: THIS write must flush all
        // three fragments as one chunk.
        tokio::time::sleep(COALESCE_MAX_AGE + std::time::Duration::from_millis(10)).await;
        stream.write(b"ef").await.unwrap();
        // A fresh fragment after the flush stays buffered until close.
        stream.write(b"gh").await.unwrap();
        stream.close().await.unwrap();

        let captured = frames.lock().unwrap();
        let chunks: Vec<_> = captured
            .iter()
            .filter(|f| f.frame_type == FrameType::Chunk)
            .collect();
        assert_eq!(
            chunks.len(),
            2,
            "one age-flushed batch + one close-flushed tail — never one frame per write"
        );
        assert_eq!(chunk_inner_bytes(chunks[0]), b"abcdef".to_vec());
        assert_eq!(chunk_inner_bytes(chunks[1]), b"gh".to_vec());
    }

    // TEST8120: the coalescing buffer is SHARED between an OutputStream and
    // its detached StreamSender (the blocking-thread token emitter), and a
    // non-Bytes emission is an ordering barrier: buffered bytes ship first.
    // close() on the OutputStream flushes bytes buffered through the sender —
    // the runtime's auto-close is what makes detached-sender coalescing
    // lossless.
    #[tokio::test]
    async fn test8120_stream_sender_shares_buffer_and_barriers_non_bytes() {
        let (sender, frames) = MockFrameSender::new();
        let stream = OutputStream::new(
            Arc::new(sender),
            "stream-1".to_string(),
            "media:enc=utf-8".to_string(),
            MessageId::new_uuid(),
            None,
            256_000,
        );
        stream.start(false, None).unwrap();
        let ss = stream.stream_sender();

        // Buffered through the detached sender…
        ss.emit_cbor(&Value::Bytes(b"tok1".to_vec())).unwrap();
        ss.emit_cbor(&Value::Bytes(b"tok2".to_vec())).unwrap();
        // …a non-Bytes value must NOT overtake them: barrier-flush first.
        ss.emit_cbor(&Value::Integer(7.into())).unwrap();
        // …and bytes buffered after the barrier flush with close().
        ss.emit_cbor(&Value::Bytes(b"tok3".to_vec())).unwrap();
        stream.close().await.unwrap();

        let captured = frames.lock().unwrap();
        let chunks: Vec<_> = captured
            .iter()
            .filter(|f| f.frame_type == FrameType::Chunk)
            .collect();
        assert_eq!(chunks.len(), 3, "batch, barrier value, close-flushed tail");
        assert_eq!(chunk_inner_bytes(chunks[0]), b"tok1tok2".to_vec());
        let barrier: ciborium::Value =
            ciborium::from_reader(chunks[1].payload.as_ref().unwrap().as_slice()).unwrap();
        assert_eq!(barrier, ciborium::Value::Integer(7.into()));
        assert_eq!(chunk_inner_bytes(chunks[2]), b"tok3".to_vec());

        let end_pos = captured
            .iter()
            .position(|f| f.frame_type == FrameType::StreamEnd)
            .expect("STREAM_END present");
        let last_chunk_pos = captured
            .iter()
            .rposition(|f| f.frame_type == FrameType::Chunk)
            .unwrap();
        assert!(last_chunk_pos < end_pos, "tail ships before STREAM_END");
        let stream_end = &captured[end_pos];
        assert_eq!(
            stream_end.chunk_count,
            Some(3),
            "STREAM_END promises the coalesced count"
        );
    }
}

