//! Async Cartridge Host Runtime — Multi-cartridge management with frame routing
//!
//! The CartridgeHostRuntime manages multiple cartridge binaries, routing CBOR protocol
//! frames between a relay connection (to the engine) and individual cartridge processes.
//!
//! ## Architecture
//!
//! ```text
//! Relay (engine) ←→ CartridgeHostRuntime ←→ Cartridge A (stdin/stdout)
//!                                   ←→ Cartridge B (stdin/stdout)
//!                                   ←→ Cartridge C (stdin/stdout)
//! ```
//!
//! ## Frame Routing
//!
//! Engine → Cartridge:
//! - REQ: route by cap_urn to the cartridge that handles it, spawn on demand
//! - STREAM_START/CHUNK/STREAM_END/END/ERR: route by req_id to the mapped cartridge
//! - All other frame types: hard protocol error (must never arrive from engine)
//!
//! Cartridge → Engine:
//! - HELLO: fatal error (consumed during handshake, never during run)
//! - HEARTBEAT: responded to locally, never forwarded
//! - REQ (peer invoke): registered in routing table, forwarded to relay
//! - RelayNotify/RelayState: fatal error (cartridges must never send these)
//! - Everything else: forwarded to relay (pass-through)

use crate::bifaci::frame::{
    CancelReason, FlowKey, Frame, FrameType, Limits, MessageId, SeqAssigner,
};
use crate::bifaci::io::{handshake, verify_identity, CborError, FrameReader, FrameWriter};
use crate::bifaci::relay_switch::{
    CartridgeAttachmentError, CartridgeAttachmentErrorKind, CartridgeLifecycle,
    CartridgeRuntimeStats, InstalledCartridgeRecord, RelayNotifyCapabilitiesPayload,
};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

/// Interval between heartbeat probes sent to each running cartridge.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// How long a retired cartridge is allowed to finish the requests it is already
/// serving before the host kills it.
///
/// Retirement means "stop giving this install NEW work", not "destroy the work
/// it is doing". The cartridge is dropped from the cap table immediately (so
/// nothing new routes to it) and killed only once its in-flight requests have
/// terminated. This bound is a backstop for a cartridge that never finishes;
/// heartbeat monitoring still applies during the drain, so a wedged process is
/// caught by health long before this expires.
const RETIRE_DRAIN_TIMEOUT: Duration = Duration::from_secs(600);

/// Maximum time to wait for a heartbeat response before considering a cartridge unhealthy.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);

// =============================================================================
// CARTRIDGE HOST OBSERVER — Lifecycle callbacks for spawn/death
// =============================================================================

/// Lifecycle observer for `CartridgeHostRuntime`.
///
/// Mirrors the Swift `CartridgeHostObserver` protocol in
/// `capdag-objc/Sources/Bifaci/CartridgeHost.swift`. The host invokes the
/// registered observer when a cartridge becomes runnable (`cartridge_spawned`)
/// or stops running (`cartridge_died`).
///
/// Implementations MUST NOT block or take long-held locks: the host's
/// internal locks are not held during the call, but the call still runs on
/// the run loop or the spawn caller's task.
///
/// Used by host-side bridges (e.g., a remote-IPC service that needs to push
/// process lifecycle to a separate process); not used by the engine's
/// in-process runtime, which leaves the observer unset.
pub trait CartridgeHostObserver: Send + Sync {
    /// A cartridge has just transitioned to running (handshake completed,
    /// caps extracted, reader task started).
    ///
    /// `pid` is `None` for in-process cartridges that have no OS process.
    /// `name` is the last path component of the cartridge binary path
    /// (or empty for attached cartridges with no path).
    fn cartridge_spawned(
        &self,
        cartridge_index: usize,
        pid: Option<u32>,
        name: &str,
        caps: &[String],
    );

    /// A cartridge has just transitioned to not-running (reader task EOF,
    /// process reaped, OOM kill, or clean shutdown).
    fn cartridge_died(&self, cartridge_index: usize, pid: Option<u32>, name: &str);
}

// =============================================================================
// CARTRIDGE PROCESS INFO — External visibility into managed cartridge processes
// =============================================================================

/// Snapshot of a managed cartridge process.
#[derive(Debug, Clone)]
pub struct CartridgeProcessInfo {
    /// Index of the cartridge in the host's cartridge list.
    pub cartridge_index: usize,
    /// OS process ID (from `Child::id()` on Rust side, `pid_t` on Swift side).
    pub pid: u32,
    /// Binary name (e.g. "ggufcartridge", "modelcartridge").
    pub name: String,
    /// Whether the cartridge is currently running and responsive.
    pub running: bool,
    /// Cap URN strings this cartridge handles.
    pub caps: Vec<String>,
    /// Physical memory footprint in MB (self-reported by cartridge via heartbeat).
    /// This is `ri_phys_footprint` — the metric macOS jetsam uses for kill decisions.
    /// Updated every 30s when the cartridge responds to a heartbeat probe.
    pub memory_footprint_mb: u64,
    /// Resident set size in MB (self-reported by cartridge via heartbeat).
    pub memory_rss_mb: u64,
}

/// Why a cartridge was killed. Determines whether pending requests get ERR frames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShutdownReason {
    /// App is exiting. No ERR frames — the relay connection is closing anyway
    /// and there are no callers left to notify.
    AppExit,
    /// OOM watchdog killed the cartridge while it was actively processing requests.
    /// Pending requests MUST get ERR frames with code "OOM_KILLED" so callers
    /// can fail fast instead of hanging forever.
    OomKill,
    /// A force-kill Cancel ended the process. Pending requests get ERR frames
    /// in the cancel's own attribution (`CANCELLED`/`user` for an operator's
    /// cancel, `ABORTED_COLLATERAL` / `ABORTED` with their class otherwise)
    /// — the kill is never reported as "cancelled" unless a human cancelled.
    Cancelled(CancelReason),
    /// The host's health probe expired. Pending requests get ERR frames with code
    /// "CARTRIDGE_UNHEALTHY" and the process is fully retired before it may respawn.
    HeartbeatTimeout,
    /// A roster sync retired this install: the daemon/XPC service says it is no
    /// longer a cartridge this host should run (unpublished, disabled, or
    /// replaced on disk). Distinct from `Cancelled` because NOBODY cancelled
    /// anything — reusing the cancel reason reported an environment change to
    /// operators as a user cancellation. Pending requests get ERR frames with
    /// code "CARTRIDGE_RETIRED" and class `environment`.
    RosterRetired,
}

/// A directory-registered cartridge in a roster sync. Mirrors the parameters of
/// [`CartridgeHostRuntime::register_cartridge_dir`] so a caller can describe the
/// full desired registered-dir set without reaching into runtime internals.
#[derive(Debug, Clone)]
pub struct RegisteredDirSpec {
    pub entry_point: PathBuf,
    pub version_dir: PathBuf,
    pub id: String,
    pub channel: crate::bifaci::cartridge_repo::CartridgeChannel,
    pub registry_url: Option<String>,
    pub version: String,
    pub cap_groups: Vec<crate::bifaci::manifest::CapGroup>,
}

/// Commands that can be sent to the host runtime from external code.
pub enum HostCommand {
    /// Kill a cartridge process by PID for memory pressure. The host sets
    /// `shutdown_reason = Some(OomKill)` before killing, so death handling
    /// sends ERR frames with "OOM_KILLED" for all pending requests.
    KillCartridge { pid: u32 },
    /// Replace the live discovery picture — BOTH halves — with a freshly
    /// discovered one and re-publish RelayNotify, so the engine sees added,
    /// removed and newly-attachable cartridges without reconnecting. This is
    /// the equivalent of the macOS XPC service's `host.syncDiscoveryOutcomes(...)`
    /// after a rescan (e.g. a registry verdict flipped a held cartridge to
    /// Listed), and it carries the same two kinds in one message for the same
    /// reason: a rejected install must be able to become an attachable one.
    ///
    /// `cartridges` are the attachable specs — running cartridges no longer in
    /// the set are killed; survivors keep their live process and stats.
    /// `static_records` REPLACES the rejected-install set that rides every
    /// advertisement (see [`CartridgeHostRuntime::set_static_inventory_records`]).
    /// Passing them separately, with only the first refreshed, leaves a stale
    /// rejection advertised forever over a cartridge that has since become
    /// attachable — the two halves move together or not at all.
    SyncRoster {
        cartridges: Vec<RegisteredDirSpec>,
        static_records: Vec<InstalledCartridgeRecord>,
    },
    /// Deliver operator `configured` values for one cartridge's pools (the
    /// heartbeat is the config channel — see `bifaci::pools` and
    /// [`CartridgeHostRuntime::apply_desired_capacities`]). The reply
    /// reports the validation outcome; delivery to the process rides the
    /// immediate out-of-cycle probe (or the attach-time probe when cold).
    ApplyDesiredCapacities {
        cartridge_id: String,
        desired: crate::bifaci::pools::DesiredCapacities,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
}

/// Thread-safe handle for querying cartridge process info and sending commands
/// to a running `CartridgeHostRuntime`. Obtained via `process_handle()` before
/// calling `run()`. The handle remains valid for the lifetime of `run()`.
#[derive(Clone)]
pub struct CartridgeProcessHandle {
    snapshot: Arc<RwLock<Vec<CartridgeProcessInfo>>>,
    command_tx: mpsc::UnboundedSender<HostCommand>,
}

impl CartridgeProcessHandle {
    /// Get a snapshot of all managed cartridge processes (running or not).
    pub fn running_cartridges(&self) -> Vec<CartridgeProcessInfo> {
        self.snapshot.read().unwrap().clone()
    }

    /// Replace the live discovery picture — attachable specs AND the
    /// rejected-install records that ride every advertisement (see
    /// [`HostCommand::SyncRoster`]). Both halves come from one discovery pass,
    /// so a cartridge that moves between them is corrected rather than
    /// duplicated.
    ///
    /// Returns `Err(())` if the host's run loop has exited.
    pub fn sync_roster(
        &self,
        cartridges: Vec<RegisteredDirSpec>,
        static_records: Vec<InstalledCartridgeRecord>,
    ) -> Result<(), ()> {
        self.command_tx
            .send(HostCommand::SyncRoster {
                cartridges,
                static_records,
            })
            .map_err(|_| ())
    }

    /// Request that the host kill a specific cartridge process by PID.
    /// Returns `Err(())` if the host's run loop has exited.
    pub fn kill_cartridge(&self, pid: u32) -> Result<(), ()> {
        self.command_tx
            .send(HostCommand::KillCartridge { pid })
            .map_err(|_| ())
    }

    /// Deliver operator `configured` values for one cartridge's pools (the
    /// heartbeat is the config channel — see `bifaci::pools`). Resolves
    /// once the host has VALIDATED and queued/probed the values; the
    /// refreshed pool map arrives on the next heartbeat reply and surfaces
    /// through the roster. Errors name the defect: an unknown cartridge or
    /// pool, or a host whose run loop has exited.
    pub async fn apply_desired_capacities(
        &self,
        cartridge_id: &str,
        desired: crate::bifaci::pools::DesiredCapacities,
    ) -> Result<(), String> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.command_tx
            .send(HostCommand::ApplyDesiredCapacities {
                cartridge_id: cartridge_id.to_string(),
                desired,
                reply: reply_tx,
            })
            .map_err(|_| "cartridge host run loop has exited".to_string())?;
        reply_rx
            .await
            .map_err(|_| "cartridge host dropped the desired-capacities reply".to_string())?
    }
}

// =============================================================================
// ERROR TYPES
// =============================================================================

/// Errors that can occur in the async cartridge host runtime.
#[derive(Debug, Clone, thiserror::Error)]
pub enum AsyncHostError {
    #[error("CBOR error: {0}")]
    Cbor(String),

    #[error("I/O error: {0}")]
    Io(String),

    #[error("Cartridge returned error: [{code}] {message}")]
    CartridgeError { code: String, message: String },

    #[error("Unexpected frame type: {0:?}")]
    UnexpectedFrameType(FrameType),

    #[error("Cartridge process exited unexpectedly")]
    ProcessExited,

    #[error("Handshake failed: {0}")]
    Handshake(String),

    #[error("Host is closed")]
    Closed,

    #[error("Send error: channel closed")]
    SendError,

    #[error("Protocol violation: Stream ID '{0}' already exists for request")]
    DuplicateStreamId(String),

    #[error("Protocol violation: Chunk for unknown stream ID '{0}'")]
    UnknownStreamId(String),

    #[error("Protocol violation: Chunk received for ended stream ID '{0}'")]
    ChunkAfterStreamEnd(String),

    #[error("Protocol violation: Stream activity after request END")]
    StreamAfterRequestEnd,

    #[error("Protocol violation: StreamStart missing stream_id")]
    StreamStartMissingId,

    #[error("Protocol violation: StreamStart missing media_urn")]
    StreamStartMissingUrn,

    #[error("Protocol violation: Chunk missing stream_id")]
    ChunkMissingStreamId,

    #[error("Protocol violation: {0}")]
    Protocol(String),

    #[error("Receive error: channel closed")]
    RecvError,

    #[error("Peer invoke not supported for cap: {0}")]
    PeerInvokeNotSupported(String),

    #[error("No handler found for cap: {0}")]
    NoHandler(String),
}

impl From<CborError> for AsyncHostError {
    fn from(e: CborError) -> Self {
        AsyncHostError::Cbor(e.to_string())
    }
}

impl From<std::io::Error> for AsyncHostError {
    fn from(e: std::io::Error) -> Self {
        AsyncHostError::Io(e.to_string())
    }
}

// =============================================================================
// RESPONSE TYPES (used by engine-side code reading from relay)
// =============================================================================

/// A response chunk from a cartridge.
#[derive(Debug, Clone)]
pub struct ResponseChunk {
    pub payload: Vec<u8>,
    pub seq: u64,
    pub offset: Option<u64>,
    pub len: Option<u64>,
    pub is_eof: bool,
}

/// A complete response from a cartridge, which may be single or streaming.
#[derive(Debug)]
pub enum CartridgeResponse {
    Single(Vec<u8>),
    Streaming(Vec<ResponseChunk>),
}

impl CartridgeResponse {
    pub fn final_payload(&self) -> Option<&[u8]> {
        match self {
            CartridgeResponse::Single(data) => Some(data),
            CartridgeResponse::Streaming(chunks) => chunks.last().map(|c| c.payload.as_slice()),
        }
    }

    pub fn concatenated(&self) -> Vec<u8> {
        match self {
            CartridgeResponse::Single(data) => data.clone(),
            CartridgeResponse::Streaming(chunks) => {
                let total_len: usize = chunks.iter().map(|c| c.payload.len()).sum();
                let mut result = Vec::with_capacity(total_len);
                for chunk in chunks {
                    result.extend_from_slice(&chunk.payload);
                }
                result
            }
        }
    }
}

/// A streaming response that can be iterated asynchronously.
pub struct StreamingResponse {
    receiver: mpsc::UnboundedReceiver<Result<ResponseChunk, AsyncHostError>>,
}

impl StreamingResponse {
    pub async fn next(&mut self) -> Option<Result<ResponseChunk, AsyncHostError>> {
        self.receiver.recv().await
    }
}

// =============================================================================
// INTERNAL TYPES
// =============================================================================

/// Events from cartridge reader loops, delivered to the main run() loop.
enum CartridgeEvent {
    /// A frame was received from a cartridge's stdout.
    Frame {
        cartridge_idx: usize,
        generation: u64,
        frame: Frame,
    },
    /// A cartridge's reader loop exited (process died or stdout closed).
    Death {
        cartridge_idx: usize,
        generation: u64,
    },
}

/// A managed cartridge binary.
struct ManagedCartridge {
    /// Path to the cartridge entry point binary (empty for attached/pre-connected cartridges).
    /// For directory cartridges this is the resolved entry point from cartridge.json.
    path: PathBuf,
    /// Version directory for directory-based cartridges.
    /// When set, identity hashing uses the full directory tree.
    /// When None, this is a legacy probe-based registration (cartridges path).
    cartridge_dir: Option<PathBuf>,
    /// Child process handle (None for attached cartridges).
    process: Option<tokio::process::Child>,
    /// Channel to write frames to this cartridge's stdin.
    writer_tx: Option<mpsc::UnboundedSender<Frame>>,
    /// Cartridge manifest from HELLO handshake.
    manifest: Vec<u8>,
    /// Negotiated limits for this cartridge.
    limits: Limits,
    /// Cap groups this cartridge handles. Single source of truth for
    /// what caps this cartridge claims — populated at registration
    /// time (probe HELLO at discovery) and refreshed on each
    /// spawn/HELLO. The flat cap-URN list is derived from these on
    /// demand via `cap_urns()`; we don't carry a parallel
    /// `known_caps` field that could drift.
    cap_groups: Vec<crate::bifaci::manifest::CapGroup>,
    /// Installed cartridge identity derived from the registered binary path.
    installed_identity: Option<InstalledCartridgeRecord>,
    /// Whether the cartridge is currently running and healthy.
    running: bool,
    /// Monotonic process generation. Reader events carry this value so a late
    /// event from a retired process cannot affect its replacement.
    generation: u64,
    /// The cartridge's full concurrency-pool state map (`bifaci::pools`),
    /// from HELLO and refreshed by every heartbeat reply. Empty until the
    /// first handshake; mandatory on the wire thereafter.
    pool_states: crate::bifaci::pools::PoolStates,
    /// Operator `configured` values queued for delivery on the next
    /// heartbeat probe (the heartbeat IS the capacity config channel).
    /// Cleared when the probe carrying them is sent.
    pending_desired: crate::bifaci::pools::DesiredCapacities,
    /// Reader task handle.
    reader_handle: Option<JoinHandle<()>>,
    /// Writer task handle.
    writer_handle: Option<JoinHandle<()>>,
    /// Whether HELLO handshake permanently failed (binary is broken, no relaunch).
    hello_failed: bool,
    /// Retired by a roster sync (the install was removed/replaced on disk).
    /// A removed cartridge disappears from the inventory entirely — unlike
    /// `hello_failed`, which stays visible carrying an attachment error.
    /// Slots are never physically removed (reader/death events hold indices),
    /// so this flag is the retirement mechanism. Mirrors Swift's `isRemoved`.
    removed: bool,
    /// Pending heartbeats sent to this cartridge (ID → sent time).
    pending_heartbeats: HashMap<MessageId, Instant>,
    /// Stderr handle for capturing crash output.
    stderr_handle: Option<tokio::process::ChildStderr>,
    /// Last death error message (includes stderr if available). Used for ERR frames
    /// sent when attempting to write to a dead cartridge.
    last_death_message: Option<String>,
    /// Set before killing the process to signal why the death occurred.
    /// `handle_cartridge_death` checks this to determine ERR frame behavior:
    /// - `None` → unexpected crash → ERR "CARTRIDGE_DIED"
    /// - `Some(OomKill)` → OOM watchdog kill → ERR "OOM_KILLED"
    /// - `Some(HeartbeatTimeout)` → health failure → ERR "CARTRIDGE_UNHEALTHY"
    /// - `Some(AppExit)` → clean shutdown → no ERR frames
    shutdown_reason: Option<ShutdownReason>,
    /// Physical memory footprint in MB (self-reported via heartbeat response meta).
    /// Updated every 30s when the cartridge echoes a heartbeat probe with its
    /// `ri_phys_footprint` from `proc_pid_rusage(getpid())`.
    memory_footprint_mb: u64,
    /// Resident set size in MB (self-reported via heartbeat response meta).
    memory_rss_mb: u64,
    /// Unix timestamp seconds of the last heartbeat response. `None` until
    /// the first successful heartbeat round-trip completes.
    last_heartbeat_unix_seconds: Option<i64>,
    /// Number of times this cartridge has been respawned after death.
    restart_count: u64,
    /// Cumulative protocol drop count self-reported by the cartridge as
    /// `drops_total` in heartbeat response meta (closed-channel sends, …).
    /// `None` until the first heartbeat round-trip carries the counter.
    /// Survives across readings (each heartbeat carries the cartridge's
    /// running total). Drops mean something went wrong.
    protocol_drops_total: Option<u64>,
    /// Cumulative BENIGN straggler count self-reported by the cartridge as
    /// `stragglers_total` in heartbeat response meta (writer-gate
    /// suppressions of late frames that crossed their flow's terminal —
    /// the expected teardown race, nothing wrong). `None` until the first
    /// reading.
    protocol_stragglers_total: Option<u64>,
    /// Cumulative live-feed OVERRUN count self-reported by the cartridge as
    /// `overruns_total` in heartbeat response meta (12.5 §Overrun:
    /// real-time items discarded at a capture edge because the consumer
    /// lagged — inherent to live capture, indicated as its own category,
    /// never a drop). `None` until the first reading.
    protocol_overruns_total: Option<u64>,
    /// Set when a roster sync retired this cartridge while it still had work in
    /// flight. It is already out of the cap table and the inventory, so nothing
    /// new routes to it; the process stays alive until its in-flight requests
    /// terminate or [`RETIRE_DRAIN_TIMEOUT`] expires.
    retiring_since: Option<tokio::time::Instant>,
}

impl ManagedCartridge {
    /// Create a registered cartridge from a binary path (probe-based discovery).
    /// Identity is computed from the binary's name and content hash.
    /// `channel` and `registry_url` must be supplied by the caller —
    /// the filename alone cannot tell us which (channel, registry) a
    /// standalone-binary install belongs to, and inferring would
    /// silently merge release/nightly or different-registry artefacts.
    fn new_registered_binary(
        path: PathBuf,
        name: String,
        version: String,
        channel: crate::bifaci::cartridge_repo::CartridgeChannel,
        registry_url: Option<String>,
        cap_groups: Vec<crate::bifaci::manifest::CapGroup>,
    ) -> Self {
        let installed_identity =
            installed_cartridge_record_from_binary(&path, name, version, channel, registry_url);
        Self {
            path,
            cartridge_dir: None,
            process: None,
            writer_tx: None,
            manifest: Vec::new(),
            limits: Limits::default(),
            cap_groups,
            installed_identity,
            running: false,
            generation: 0,
            pool_states: crate::bifaci::pools::PoolStates::new(),
            pending_desired: crate::bifaci::pools::DesiredCapacities::new(),
            reader_handle: None,
            writer_handle: None,
            hello_failed: false,
            removed: false,
            pending_heartbeats: HashMap::new(),
            stderr_handle: None,
            last_death_message: None,
            shutdown_reason: None,
            memory_footprint_mb: 0,
            memory_rss_mb: 0,
            last_heartbeat_unix_seconds: None,
            restart_count: 0,
            protocol_drops_total: None,
            protocol_stragglers_total: None,
            protocol_overruns_total: None,
            retiring_since: None,
        }
    }

    /// Create a registered cartridge from a version directory containing cartridge.json.
    /// Identity is computed from the directory tree hash.
    ///
    /// A directory-registered cartridge always has a resolvable identity.
    /// If the directory turns out to be unhashable at construction time,
    /// we pre-record an attachment failure so the upstream aggregate
    /// reports the real reason instead of silently dropping the cartridge.
    ///
    /// `registry_url` is sourced from the `cartridge.json:registry_url`
    /// the host already validated (three-place rule). `None` ⇔ dev
    /// install; `Some(url)` ⇔ the cartridge was placed under
    /// `slug_for(url)`. Pass-through; this constructor never derives
    /// it from the path.
    fn new_registered_dir(
        entry_point: PathBuf,
        cartridge_dir: PathBuf,
        id: String,
        channel: crate::bifaci::cartridge_repo::CartridgeChannel,
        registry_url: Option<String>,
        version: String,
        cap_groups: Vec<crate::bifaci::manifest::CapGroup>,
    ) -> Self {
        let (installed_identity, hello_failed) =
            match crate::bifaci::cartridge_json::hash_cartridge_directory(&cartridge_dir) {
                Ok(sha256) => (
                    Some(InstalledCartridgeRecord {
                        registry_url: registry_url.clone(),
                        id,
                        channel,
                        version,
                        sha256,
                        cap_groups: Vec::new(),
                        attachment_error: None,
                        runtime_stats: None,
                        // Engine-bundled / engine-spawned external
                        // cartridges are operational by construction:
                        // the engine walked its own `bundled-cartridges/`
                        // tree, validated the install context, and
                        // probed the cartridge synchronously before
                        // calling this constructor. There is no
                        // separate "still inspecting / verifying"
                        // phase to model on this path — the work
                        // already happened. The XPC-cartridge path
                        // (machfab-mac) is the one that transitions
                        // through `Discovered` → `Inspecting` →
                        // `Verifying` → `Operational` because its
                        // hashing + verifier round-trips are
                        // user-visible.
                        lifecycle: CartridgeLifecycle::Operational,
                    }),
                    false,
                ),
                Err(e) => {
                    let detected_at = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0);
                    let err = CartridgeAttachmentError {
                        kind: CartridgeAttachmentErrorKind::EntryPointMissing,
                        message: format!(
                            "Cartridge directory not hashable at '{}': {}",
                            cartridge_dir.display(),
                            e
                        ),
                        detected_at_unix_seconds: detected_at,
                    };
                    tracing::error!(
                        dir = %cartridge_dir.display(),
                        error = %e,
                        "Cartridge directory not hashable — recording attachment failure"
                    );
                    (
                        Some(InstalledCartridgeRecord {
                            registry_url: registry_url.clone(),
                            id,
                            channel,
                            version,
                            sha256: String::new(),
                            cap_groups: Vec::new(),
                            attachment_error: Some(err),
                            runtime_stats: None,
                            // attachment_error is Some, so
                            // lifecycle is irrelevant per the
                            // mutual-exclusivity contract; default
                            // to Discovered (the safe sentinel)
                            // rather than asserting Operational.
                            lifecycle: CartridgeLifecycle::Discovered,
                        }),
                        true,
                    )
                }
            };
        Self {
            path: entry_point,
            cartridge_dir: Some(cartridge_dir),
            process: None,
            writer_tx: None,
            manifest: Vec::new(),
            limits: Limits::default(),
            cap_groups,
            installed_identity,
            running: false,
            generation: 0,
            pool_states: crate::bifaci::pools::PoolStates::new(),
            pending_desired: crate::bifaci::pools::DesiredCapacities::new(),
            reader_handle: None,
            writer_handle: None,
            hello_failed,
            removed: false,
            pending_heartbeats: HashMap::new(),
            stderr_handle: None,
            last_death_message: None,
            shutdown_reason: None,
            memory_footprint_mb: 0,
            memory_rss_mb: 0,
            last_heartbeat_unix_seconds: None,
            restart_count: 0,
            protocol_drops_total: None,
            protocol_stragglers_total: None,
            protocol_overruns_total: None,
            retiring_since: None,
        }
    }

    fn new_attached(
        manifest: Vec<u8>,
        limits: Limits,
        pool_states: crate::bifaci::pools::PoolStates,
        cap_groups: Vec<crate::bifaci::manifest::CapGroup>,
        installed_identity: Option<InstalledCartridgeRecord>,
    ) -> Self {
        Self {
            path: PathBuf::new(),
            cartridge_dir: None,
            process: None,
            writer_tx: None,
            manifest,
            limits,
            cap_groups,
            installed_identity,
            running: true,
            generation: 1,
            pool_states,
            pending_desired: crate::bifaci::pools::DesiredCapacities::new(),
            reader_handle: None,
            writer_handle: None,
            hello_failed: false,
            removed: false,
            pending_heartbeats: HashMap::new(),
            stderr_handle: None,
            last_death_message: None,
            shutdown_reason: None,
            memory_footprint_mb: 0,
            memory_rss_mb: 0,
            last_heartbeat_unix_seconds: None,
            restart_count: 0,
            protocol_drops_total: None,
            protocol_stragglers_total: None,
            protocol_overruns_total: None,
            retiring_since: None,
        }
    }

    fn installed_cartridge_record(&self) -> Option<InstalledCartridgeRecord> {
        self.installed_identity.clone()
    }

    /// True for a cartridge registered from a version directory (the lazily-
    /// spawned, dir-backed kind). Distinguishes roster-managed installs from
    /// attached/internal cartridges during a `SyncRoster`.
    fn is_registered_dir(&self) -> bool {
        self.cartridge_dir.is_some()
    }

    /// Flat de-duplicated cap-URN view derived from `cap_groups`.
    /// Order is the cartridge's manifest declaration order, with
    /// duplicates dropped on the second appearance. Computed each
    /// call so the host never carries a parallel representation that
    /// could drift from the structural source.
    fn cap_urns(&self) -> Vec<String> {
        let mut seen: HashSet<String> = HashSet::new();
        let mut out: Vec<String> = Vec::new();
        for group in &self.cap_groups {
            for cap in &group.caps {
                let urn = cap.urn.to_string();
                if seen.insert(urn.clone()) {
                    out.push(urn);
                }
            }
        }
        out
    }

    /// Record an attachment failure for this cartridge.
    ///
    /// Flips `hello_failed` so the cartridge is treated as permanently broken
    /// (no on-demand respawn) and stamps `installed_identity` with the error
    /// so it surfaces in the next `RelayNotify` aggregate.
    ///
    /// If the cartridge had no resolvable identity (bad directory hash,
    /// unparseable binary name), we synthesize a minimum identity so the
    /// failure is still reportable to the UI.
    fn record_attachment_error(&mut self, kind: CartridgeAttachmentErrorKind, message: String) {
        self.hello_failed = true;
        let detected_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let error = CartridgeAttachmentError {
            kind,
            message,
            detected_at_unix_seconds: detected_at,
        };
        match self.installed_identity.as_mut() {
            Some(existing) => {
                existing.attachment_error = Some(error);
            }
            None => {
                // Reaching this branch means a HELLO failed against a
                // cartridge whose registration path didn't supply an
                // `InstalledCartridgeRecord`. In production both
                // `new_registered_binary` and `new_registered_dir`
                // synthesize an identity at construction time, so the
                // only legitimate path here is an ad-hoc test attach
                // via `new_attached` — which never reaches the engine's
                // RelayNotify aggregate. Panic loudly: silently
                // synthesizing an identity without channel info would
                // collapse the release/nightly distinction at the
                // wire boundary.
                panic!(
                    "BUG: record_attachment_error fired on a cartridge without an \
                     InstalledCartridgeRecord (path '{}'). Channels are part of \
                     identity; we never synthesize one without channel info.",
                    self.path.display()
                );
            }
        }
    }
}

/// Compute identity for a standalone binary cartridge (probe-based discovery path).
/// Parses id and version from the binary filename, hashes the binary content.
/// `channel` and `registry_url` are supplied by the caller — the
/// filename does not carry them and we never silently default a
/// value. The probe path is exercised by tests and by the rare
/// "unmanaged binary inside a cartridge dir" diagnostic; the
/// production directory-cartridge path goes through
/// `new_registered_dir`.
fn installed_cartridge_record_from_binary(
    path: &Path,
    name: String,
    version: String,
    channel: crate::bifaci::cartridge_repo::CartridgeChannel,
    registry_url: Option<String>,
) -> Option<InstalledCartridgeRecord> {
    let bytes = std::fs::read(path).ok()?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let sha256 = format!("{:x}", hasher.finalize());
    Some(InstalledCartridgeRecord {
        registry_url,
        id: name,
        channel,
        version,
        sha256,
        cap_groups: Vec::new(),
        attachment_error: None,
        runtime_stats: None,
        lifecycle: CartridgeLifecycle::Discovered,
    })
}

/// Build the install identity for a cartridge attached over raw streams
/// (no on-disk anchor: the dev/host-embedded/interop path).
///
/// Advertisement is identity-gated — a cartridge with no
/// `installed_identity` is silently dropped from every `RelayNotify`, so an
/// attached cartridge MUST carry a resolvable identity or the host
/// advertises an empty inventory and the engine can never route to it. An
/// attached cartridge has already completed HELLO + identity verification by
/// the time this is called, so it is operational by construction; its
/// identity is sourced from the manifest it sent during HELLO (the same
/// `(registry_url, channel, id, version)` tuple a registered install carries),
/// with the sha256 taken over the manifest bytes (the only stable artefact
/// available without a file on disk). This mirrors
/// `installed_cartridge_record_from_binary` but anchors on the manifest
/// rather than a binary path.
fn installed_cartridge_record_from_manifest(manifest: &[u8]) -> Option<InstalledCartridgeRecord> {
    let parsed: crate::CapManifest = serde_json::from_slice(manifest).ok()?;
    let mut hasher = Sha256::new();
    hasher.update(manifest);
    let sha256 = format!("{:x}", hasher.finalize());
    Some(InstalledCartridgeRecord {
        registry_url: parsed.registry_url,
        id: parsed.name,
        channel: parsed.channel,
        version: parsed.version,
        sha256,
        cap_groups: Vec::new(),
        attachment_error: None,
        // Attached ⇒ HELLO + identity verification already succeeded.
        lifecycle: CartridgeLifecycle::Operational,
        runtime_stats: None,
    })
}

// =============================================================================
// ASYNC CARTRIDGE HOST RUNTIME
// =============================================================================

/// Async host-side runtime managing multiple cartridge processes.
///
/// Routes CBOR protocol frames between a relay connection (engine) and
/// individual cartridge processes. Handles HELLO handshake, heartbeat health
/// monitoring, spawn-on-demand, crash recovery, and capability advertisement.
pub struct CartridgeHostRuntime {
    /// Managed cartridge binaries.
    cartridges: Vec<ManagedCartridge>,
    /// Routing: cap_urn → cartridge index (for finding which cartridge handles a cap).
    cap_table: Vec<(String, usize)>,
    /// List 1: OUTGOING_RIDS - tracks peer requests sent by cartridges (RID → cartridge_idx).
    /// Used only to detect same-cartridge peer calls (not for routing).
    /// Bounded by `ROUTING_TABLE_HARD_CAP`; the GC evicts the
    /// least-recently-touched entries when the table exceeds the
    /// soft watermark.
    outgoing_rids: HashMap<MessageId, usize>,
    /// Parallel touched-at clock for `outgoing_rids` (key set
    /// kept in sync). Read by the GC to pick eviction victims.
    outgoing_rids_touched: HashMap<MessageId, u64>,
    /// List 2: INCOMING_RXIDS - tracks incoming requests from relay ((XID, RID) → cartridge_idx).
    /// Continuations for these requests are routed by this table.
    /// Same GC discipline as `outgoing_rids`.
    incoming_rxids: HashMap<(MessageId, MessageId), usize>,
    incoming_rxids_touched: HashMap<(MessageId, MessageId), u64>,
    /// Tracks which incoming request spawned which outgoing peer RIDs.
    /// Maps parent (xid, rid) → list of child peer RIDs. Used for cancel cascade.
    /// Same GC discipline; eviction is keyed off the parent's
    /// touched-at, not the children's.
    incoming_to_peer_rids: HashMap<(MessageId, MessageId), Vec<MessageId>>,
    incoming_to_peer_rids_touched: HashMap<(MessageId, MessageId), u64>,
    /// Max-seen seq per flow for cartridge-originated frames.
    /// Used to set seq on host-generated ERR frames (max_seen + 1).
    /// Same GC discipline.
    outgoing_max_seq: HashMap<FlowKey, u64>,
    outgoing_max_seq_touched: HashMap<FlowKey, u64>,
    /// Monotonic counter that the touch-helpers increment to stamp
    /// each entry's age. Avoids a `std::time::Instant`-per-entry
    /// (Instant is 16 bytes vs. u64's 8) and side-steps clock
    /// quirks (CLOCK_MONOTONIC_RAW etc.) — we only need a strict
    /// ordering, not wall-clock semantics, so a simple counter
    /// is the right primitive. Wraps after 2^64 inserts; in
    /// practice that means never.
    routing_touch_seq: u64,
    /// Monotonic count of GC passes that have run on this host.
    /// Logged with each pass and exposed for tests.
    routing_gc_runs_total: u64,
    /// Monotonic count of entries evicted across all GC passes.
    routing_gc_evicted_total: u64,
    /// Channel sender for cartridge events (shared with reader tasks).
    event_tx: mpsc::UnboundedSender<CartridgeEvent>,
    /// Channel receiver for cartridge events (consumed by run()).
    event_rx: Option<mpsc::UnboundedReceiver<CartridgeEvent>>,
    /// Shared process snapshot, readable from outside the run loop via `CartridgeProcessHandle`.
    process_snapshot: Arc<RwLock<Vec<CartridgeProcessInfo>>>,
    /// Channel for receiving external commands (e.g., kill requests).
    command_tx: mpsc::UnboundedSender<HostCommand>,
    /// Receiver end — consumed by `run()`.
    command_rx: Option<mpsc::UnboundedReceiver<HostCommand>>,
    /// Lifecycle observer. Set by callers that want to be notified when a
    /// cartridge transitions in/out of the running state. Mirrors the Swift
    /// `CartridgeHost.observer` field.
    observer: Option<Arc<dyn CartridgeHostObserver>>,
    /// Dropped-frame accounting (L8): unroutable continuations and frames for
    /// dead cartridges are counted drops, never silent losses. Drops mean
    /// something went wrong.
    drops: Arc<crate::bifaci::stats::DropCounters>,
    /// Benign post-terminal stragglers: frames that crossed their request's
    /// terminal in flight — the expected teardown race, counted per frame
    /// type and indicated as benign, never as drops.
    stragglers: Arc<crate::bifaci::stats::StragglerCounters>,
    /// Incoming requests whose REQUEST BODY has completed (body END routed to
    /// the handler) but whose RESPONSE has not yet terminated. The current
    /// protocol keeps
    /// `incoming_rxids` alive through this phase — engine→cartridge CREDIT
    /// grants for the handler's OUTPUT arrive throughout it (earlier code
    /// removed the entry at body END, silently killing every output grant and
    /// deadlocking any response larger than the initial window). Data frames
    /// arriving from the relay during this phase are self-loop peer responses
    /// and fall through to `outgoing_rids` as before.
    incoming_body_done: HashSet<(MessageId, MessageId)>,
    /// Bounded ring of RIDs whose routing entries were released by an
    /// OBSERVED terminal — a completed request, a completed peer response, or
    /// a cartridge death (which synthesizes the ERR terminal itself). This is
    /// the discriminator between the two ways a frame can arrive with no
    /// routing entry: a hit means the frame crossed its request's terminal in
    /// flight (the ordinary teardown race of credit-based flow control —
    /// counted as a benign straggler), a miss means the host never routed this RID
    /// within the ring's horizon (`no_route`, a genuine anomaly). GC
    /// evictions are deliberately NOT recorded here: an evicted entry never
    /// saw its terminal, so a frame for it is real routing loss and stays
    /// `no_route` beside `routing_gc_evicted_total`.
    recent_released_rids: VecDeque<MessageId>,
    /// Incoming requests whose RESPONSE terminal already passed outbound while
    /// the request body was still open (response-first race). When the body
    /// END later arrives, the entry is released immediately instead of being
    /// marked body-done.
    incoming_response_done: HashSet<(MessageId, MessageId)>,
    /// Inventory records the host does NOT manage as processes — discovery
    /// outcomes like incompatible installs (verdict-rejected, wrong manifest
    /// version, quarantined). Merged into EVERY capabilities advertisement so
    /// a host-originated RelayNotify can never erase them from the engine's
    /// inventory. Failure visibility is a hard requirement: a cartridge that
    /// exists on disk always appears in the inventory, healthy or not.
    static_inventory_records: Vec<InstalledCartridgeRecord>,
}

/// The host runtime's protocol observability snapshot (L8): per-reason drop
/// counters, benign post-terminal straggler counters, routing-table sizes,
/// and GC totals. Serializable; field names are the mirror contract.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HostProtocolStats {
    pub drops: crate::bifaci::stats::DropSnapshot,
    /// Benign post-terminal stragglers — the expected teardown crossing,
    /// counted per frame type. Separate from `drops`: nothing went wrong.
    #[serde(default)]
    pub stragglers: crate::bifaci::stats::StragglerSnapshot,
    pub outgoing_rids: usize,
    pub incoming_rxids: usize,
    pub incoming_to_peer_rids: usize,
    pub outgoing_max_seq: usize,
    pub routing_gc_runs_total: u64,
    pub routing_gc_evicted_total: u64,
}

impl CartridgeHostRuntime {
    /// Generous cap on the per-host routing tables. The
    /// "intentionally leaked until cartridge death" semantics on
    /// `incoming_rxids` (and the parallel structure on the other
    /// three tables) means a cartridge that creates many distinct
    /// request IDs without dying will accumulate entries forever.
    /// In normal use we observed ~568 entries across a long
    /// session (the Swift mirror's measurement); 8192 gives ~14×
    /// headroom before the GC fires, which is enough to cover
    /// bursts (PDF disbind→ForEach×N→LLM-call patterns) while
    /// still catching a runaway producer well before it grows
    /// memory by megabytes.
    pub(crate) const ROUTING_TABLE_HARD_CAP: usize = 8192;
    /// How many terminal-released RIDs the discrimination ring retains
    /// (`recent_released_rids`). The teardown race window is milliseconds;
    /// 64 releases of horizon mirrors `RequestTable`'s recently-terminated
    /// ring on the relay side.
    pub(crate) const RECENT_RELEASED_RIDS_CAP: usize = 64;
    /// Soft watermark — when an insertion brings a table at or
    /// above this size, the GC fires and evicts the oldest 25 %
    /// by `routing_touch_seq`. Set to ~80 % of `HARD_CAP` so the
    /// GC runs ahead of the cap rather than spinning right at it.
    pub(crate) const ROUTING_TABLE_SOFT_WATERMARK: usize = 6553;
    /// Fraction of entries to drop in one GC pass. Lower values
    /// re-fire the GC more often (more log noise, more lock
    /// churn); higher values discard entries that may still be
    /// live (more likely to drop a continuation frame). 25 % is a
    /// balance — matches the watermark distance so two consecutive
    /// GC passes can carry the table back down to half-full if
    /// traffic briefly stays above the watermark.
    pub(crate) const ROUTING_TABLE_GC_EVICTION_FRACTION: f64 = 0.25;

    /// Provide inventory records for cartridges this host does NOT manage as
    /// processes — discovery outcomes such as incompatible installs, carrying
    /// their `attachment_error`. They are merged into every capabilities
    /// advertisement (initial and republished), so the engine's inventory —
    /// and therefore the UI — always shows every on-disk cartridge with its
    /// status. Silence on failure is a bug; this is the mechanism that
    /// prevents it.
    pub fn set_static_inventory_records(&mut self, records: Vec<InstalledCartridgeRecord>) {
        self.static_inventory_records = records;
    }

    /// Protocol observability snapshot (L8): drop counters, routing-table
    /// sizes, and GC totals for this host.
    pub fn protocol_stats(&self) -> HostProtocolStats {
        HostProtocolStats {
            drops: self.drops.snapshot(),
            stragglers: self.stragglers.snapshot(),
            outgoing_rids: self.outgoing_rids.len(),
            incoming_rxids: self.incoming_rxids.len(),
            incoming_to_peer_rids: self.incoming_to_peer_rids.len(),
            outgoing_max_seq: self.outgoing_max_seq.len(),
            routing_gc_runs_total: self.routing_gc_runs_total,
            routing_gc_evicted_total: self.routing_gc_evicted_total,
        }
    }

    /// Stamp `key` in `incoming_rxids_touched` with a fresh
    /// touch sequence. Called both on insert and on every read
    /// that hits the entry, so a still-streaming flow stays
    /// "fresh" for the GC.
    fn touch_incoming_rxid(&mut self, key: &(MessageId, MessageId)) {
        self.routing_touch_seq = self.routing_touch_seq.wrapping_add(1);
        self.incoming_rxids_touched
            .insert(key.clone(), self.routing_touch_seq);
    }

    fn touch_outgoing_rid(&mut self, rid: &MessageId) {
        self.routing_touch_seq = self.routing_touch_seq.wrapping_add(1);
        self.outgoing_rids_touched
            .insert(rid.clone(), self.routing_touch_seq);
    }

    fn touch_incoming_to_peer_rids(&mut self, key: &(MessageId, MessageId)) {
        self.routing_touch_seq = self.routing_touch_seq.wrapping_add(1);
        self.incoming_to_peer_rids_touched
            .insert(key.clone(), self.routing_touch_seq);
    }

    fn touch_outgoing_max_seq(&mut self, key: &FlowKey) {
        self.routing_touch_seq = self.routing_touch_seq.wrapping_add(1);
        self.outgoing_max_seq_touched
            .insert(key.clone(), self.routing_touch_seq);
    }

    /// Record that `rid`'s routing entry was released by an observed terminal
    /// (see `recent_released_rids`). Deduplicated; bounded at
    /// `RECENT_RELEASED_RIDS_CAP`.
    fn note_released_rid(&mut self, rid: &MessageId) {
        if self.recent_released_rids.iter().any(|r| r == rid) {
            return;
        }
        if self.recent_released_rids.len() == Self::RECENT_RELEASED_RIDS_CAP {
            self.recent_released_rids.pop_front();
        }
        self.recent_released_rids.push_back(rid.clone());
    }

    /// Whether `rid`'s routing entry was recently released by a terminal —
    /// the benign-straggler / no_route discriminator for unroutable frames.
    fn recently_released_rid(&self, rid: &MessageId) -> bool {
        self.recent_released_rids.iter().any(|r| r == rid)
    }

    /// Run the GC if any routing table has crossed its soft
    /// watermark. Logs at `tracing::error` level — this is
    /// unusual enough that we want it visible by default in
    /// `tracing` filters, even when the user hasn't enabled
    /// info-level capture. Each table is GC'd independently
    /// (their key sets don't overlap so there's no benefit to
    /// ganging them).
    fn gc_routing_tables_if_needed(&mut self) {
        if self.incoming_rxids.len() >= Self::ROUTING_TABLE_SOFT_WATERMARK {
            Self::gc_routing_table(
                "incoming_rxids",
                &mut self.incoming_rxids,
                &mut self.incoming_rxids_touched,
                &mut self.routing_gc_runs_total,
                &mut self.routing_gc_evicted_total,
            );
        }
        if self.outgoing_rids.len() >= Self::ROUTING_TABLE_SOFT_WATERMARK {
            Self::gc_routing_table(
                "outgoing_rids",
                &mut self.outgoing_rids,
                &mut self.outgoing_rids_touched,
                &mut self.routing_gc_runs_total,
                &mut self.routing_gc_evicted_total,
            );
        }
        if self.incoming_to_peer_rids.len() >= Self::ROUTING_TABLE_SOFT_WATERMARK {
            Self::gc_routing_table(
                "incoming_to_peer_rids",
                &mut self.incoming_to_peer_rids,
                &mut self.incoming_to_peer_rids_touched,
                &mut self.routing_gc_runs_total,
                &mut self.routing_gc_evicted_total,
            );
        }
        if self.outgoing_max_seq.len() >= Self::ROUTING_TABLE_SOFT_WATERMARK {
            Self::gc_routing_table(
                "outgoing_max_seq",
                &mut self.outgoing_max_seq,
                &mut self.outgoing_max_seq_touched,
                &mut self.routing_gc_runs_total,
                &mut self.routing_gc_evicted_total,
            );
        }
    }

    /// Generic GC pass: drop the oldest
    /// `ROUTING_TABLE_GC_EVICTION_FRACTION` of `primary` (and its
    /// matching `touched` entries) by touch-sequence ascending.
    /// Keys missing from `touched` are treated as oldest (sequence
    /// = 0) — they're either pre-touch state or a buggy
    /// non-touched insert; either way evicting them is safer than
    /// letting them linger.
    fn gc_routing_table<K>(
        table_name: &'static str,
        primary: &mut HashMap<K, impl Sized>,
        touched: &mut HashMap<K, u64>,
        runs_total: &mut u64,
        evicted_total: &mut u64,
    ) where
        K: std::hash::Hash + Eq + Clone,
    {
        let before_count = primary.len();
        let evict_count = std::cmp::max(
            1,
            (before_count as f64 * Self::ROUTING_TABLE_GC_EVICTION_FRACTION) as usize,
        );

        // Collect (key, touched_at) pairs and pick the oldest N.
        // O(n log n) sort over n = before_count; with n bounded
        // at ~hard cap, this is microseconds.
        let mut candidates: Vec<(K, u64)> = primary
            .keys()
            .map(|k| (k.clone(), touched.get(k).copied().unwrap_or(0)))
            .collect();
        candidates.sort_by_key(|(_, t)| *t);

        for (key, _) in candidates.iter().take(evict_count) {
            primary.remove(key);
            touched.remove(key);
        }
        *runs_total = runs_total.wrapping_add(1);
        *evicted_total = evicted_total.wrapping_add(evict_count as u64);

        tracing::error!(
            target: "cartridge_host_runtime",
            table = table_name,
            before = before_count,
            evicted = evict_count,
            after = primary.len(),
            total_runs = *runs_total,
            total_evicted = *evicted_total,
            hard_cap = Self::ROUTING_TABLE_HARD_CAP,
            "[routing-gc] least-recently-touched entries dropped to keep the table under cap. \
             If this fires repeatedly, a cartridge or relay path is producing request IDs \
             without ever terminating their flows."
        );

        // Secondary "hard cap" pass: if still above the hard cap
        // (extreme runaway), evict more aggressively until we're
        // back under the soft watermark. Bounded loop — runs at
        // most a couple of iterations even at pathological growth.
        while primary.len() >= Self::ROUTING_TABLE_HARD_CAP {
            let extra_evict = std::cmp::max(1, primary.len() - Self::ROUTING_TABLE_SOFT_WATERMARK);
            let mut extras: Vec<(K, u64)> = primary
                .keys()
                .map(|k| (k.clone(), touched.get(k).copied().unwrap_or(0)))
                .collect();
            extras.sort_by_key(|(_, t)| *t);
            for (key, _) in extras.iter().take(extra_evict) {
                primary.remove(key);
                touched.remove(key);
            }
            *evicted_total = evicted_total.wrapping_add(extra_evict as u64);
            tracing::error!(
                target: "cartridge_host_runtime",
                table = table_name,
                evicted = extra_evict,
                new_size = primary.len(),
                "[routing-gc] HARD CAP secondary pass"
            );
        }
    }

    /// Create a new cartridge host runtime.
    ///
    /// After creation, register cartridges with `register_cartridge()` or
    /// attach pre-connected cartridges with `attach_cartridge()`, then call `run()`.
    pub fn new() -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        Self {
            cartridges: Vec::new(),
            cap_table: Vec::new(),
            outgoing_rids: HashMap::new(),
            outgoing_rids_touched: HashMap::new(),
            incoming_rxids: HashMap::new(),
            incoming_rxids_touched: HashMap::new(),
            incoming_to_peer_rids: HashMap::new(),
            incoming_to_peer_rids_touched: HashMap::new(),
            outgoing_max_seq: HashMap::new(),
            outgoing_max_seq_touched: HashMap::new(),
            drops: Arc::new(crate::bifaci::stats::DropCounters::new()),
            stragglers: Arc::new(crate::bifaci::stats::StragglerCounters::new()),
            incoming_body_done: HashSet::new(),
            incoming_response_done: HashSet::new(),
            recent_released_rids: VecDeque::new(),
            static_inventory_records: Vec::new(),
            routing_touch_seq: 0,
            routing_gc_runs_total: 0,
            routing_gc_evicted_total: 0,
            event_tx,
            event_rx: Some(event_rx),
            process_snapshot: Arc::new(RwLock::new(Vec::new())),
            command_tx,
            command_rx: Some(command_rx),
            observer: None,
        }
    }

    /// Register a lifecycle observer that will be notified when cartridges
    /// transition in/out of the running state. Replaces any previously set
    /// observer. Pass `None` to clear.
    pub fn set_observer(&mut self, observer: Option<Arc<dyn CartridgeHostObserver>>) {
        self.observer = observer;
    }

    /// Get a handle for querying cartridge process info and sending commands.
    /// Must be called before `run()`. The returned handle is `Send + Sync + Clone`
    /// and remains valid for the lifetime of the `run()` loop.
    pub fn process_handle(&self) -> CartridgeProcessHandle {
        CartridgeProcessHandle {
            snapshot: self.process_snapshot.clone(),
            command_tx: self.command_tx.clone(),
        }
    }

    /// Register a cartridge binary for on-demand spawning (probe-based discovery).
    ///
    /// The cartridge is not spawned until a REQ arrives for one of
    /// its known caps. `cap_groups` is the cartridge's full manifest
    /// cap-group structure — captured at probe-time HELLO during
    /// discovery, so this registration carries the same wire shape
    /// the engine receives in `installed_cartridges[*].cap_groups`.
    /// The flat cap-URN view used for routing is derived on demand
    /// via `ManagedCartridge::cap_urns`; we don't carry a parallel
    /// `known_caps` field that could drift.
    /// `channel` and `registry_url` are part of the install's identity
    /// Identity fields (`name`, `version`, `channel`, `registry_url`) are
    /// supplied by the caller from the cartridge's own manifest — the binary
    /// path has no bearing on them. `registry_url == None` ⇔ dev install.
    pub fn register_cartridge(
        &mut self,
        path: &Path,
        name: &str,
        version: &str,
        channel: crate::bifaci::cartridge_repo::CartridgeChannel,
        registry_url: Option<&str>,
        cap_groups: &[crate::bifaci::manifest::CapGroup],
    ) {
        let cartridge_idx = self.cartridges.len();
        let groups_owned = cap_groups.to_vec();
        let cartridge = ManagedCartridge::new_registered_binary(
            path.to_path_buf(),
            name.to_string(),
            version.to_string(),
            channel,
            registry_url.map(|s| s.to_string()),
            groups_owned,
        );
        let urns = cartridge.cap_urns();
        self.cartridges.push(cartridge);
        for cap in urns {
            self.cap_table.push((cap, cartridge_idx));
        }
    }

    /// Register a directory-based cartridge for on-demand spawning.
    ///
    /// The `version_dir` must contain a valid `cartridge.json` with an entry point.
    /// Identity is computed from the directory tree hash. `channel`
    /// and `registry_url` must come from `cartridge.json` (the host
    /// has already validated the three-place rule before calling
    /// this); they propagate through `InstalledCartridgeRecord` to
    /// the engine's RelayNotify so consumers preserve the
    /// `(registry, channel)` provenance end-to-end.
    pub fn register_cartridge_dir(
        &mut self,
        entry_point: &Path,
        version_dir: &Path,
        id: &str,
        channel: crate::bifaci::cartridge_repo::CartridgeChannel,
        registry_url: Option<&str>,
        version: &str,
        cap_groups: &[crate::bifaci::manifest::CapGroup],
    ) {
        let cartridge_idx = self.cartridges.len();
        let cartridge = ManagedCartridge::new_registered_dir(
            entry_point.to_path_buf(),
            version_dir.to_path_buf(),
            id.to_string(),
            channel,
            registry_url.map(|s| s.to_string()),
            version.to_string(),
            cap_groups.to_vec(),
        );
        let urns = cartridge.cap_urns();
        self.cartridges.push(cartridge);
        for cap in urns {
            self.cap_table.push((cap, cartridge_idx));
        }
    }

    /// Attach a pre-connected cartridge (already running, e.g., pre-spawned or in tests).
    ///
    /// Performs HELLO handshake immediately. On success, the cartridge is ready for requests.
    /// On HELLO failure, returns error (permanent — the binary is broken).
    pub async fn attach_cartridge<R, W>(
        &mut self,
        cartridge_read: R,
        cartridge_write: W,
    ) -> Result<usize, AsyncHostError>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let mut reader = FrameReader::new(cartridge_read);
        let mut writer = FrameWriter::new(cartridge_write);

        let result = handshake(&mut reader, &mut writer)
            .await
            .map_err(|e| AsyncHostError::Handshake(e.to_string()))?;

        let cap_groups = parse_cap_groups_from_manifest(&result.manifest)
            .map_err(|e| e.into_async_host_error())?;

        // Verify identity — proves the protocol stack works end-to-end
        verify_identity(&mut reader, &mut writer)
            .await
            .map_err(|e| {
                AsyncHostError::Protocol(format!("Identity verification failed: {}", e))
            })?;

        let cartridge_idx = self.cartridges.len();

        // Derive the install identity from the manifest the cartridge sent
        // during HELLO. Advertisement is identity-gated, so without this the
        // attached cartridge is silently excluded from every RelayNotify and
        // the engine can never route to it (the dev/interop relay path).
        let installed_identity = installed_cartridge_record_from_manifest(&result.manifest);

        // Start writer task
        let (writer_tx, writer_rx) = mpsc::unbounded_channel::<Frame>();
        let wh = Self::start_writer_task(writer, writer_rx);

        // Start reader task
        let generation = 1;
        let rh = Self::start_reader_task(cartridge_idx, generation, reader, self.event_tx.clone());

        let mut cartridge = ManagedCartridge::new_attached(
            result.manifest,
            result.limits,
            result.pool_states.clone(),
            cap_groups,
            installed_identity,
        );
        cartridge.writer_tx = Some(writer_tx);
        cartridge.reader_handle = Some(rh);
        cartridge.writer_handle = Some(wh);

        self.cartridges.push(cartridge);
        self.update_cap_table();
        self.rebuild_capabilities(None); // No relay during initialization

        Ok(cartridge_idx)
    }

    /// Aggregate installed-cartridge inventory the host advertises to
    /// the engine. Identity is the gating filter — cartridges without
    /// a resolvable identity (no on-disk anchor) are excluded.
    /// Identical to the structure carried in `RelayNotify` payloads.
    pub fn aggregate_installed_cartridges(&self) -> Vec<InstalledCartridgeRecord> {
        self.build_installed_cartridge_identities()
    }

    /// Main run loop — reads from relay, routes to cartridges; reads from cartridges,
    /// forwards to relay. Handles HELLO/heartbeats per cartridge locally.
    ///
    /// Blocks until the relay closes or a fatal error occurs.
    /// On exit, all managed cartridge processes are killed.
    pub async fn run<R, W>(
        &mut self,
        relay_read: R,
        relay_write: W,
        resource_fn: impl Fn() -> Vec<u8> + Send + 'static,
    ) -> Result<(), AsyncHostError>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel::<Frame>();

        // Spawn outbound writer task (runtime → relay)
        let outbound_writer = tokio::spawn(Self::outbound_writer_loop(relay_write, outbound_rx));

        // Spawn relay reader task — reads frames from the relay and sends them
        // through a channel. This MUST be a dedicated task because read_exact is
        // NOT cancel-safe: if a partially-complete read_exact is dropped (e.g.,
        // by tokio::select! choosing another branch), the bytes already read are
        // lost and the byte stream desynchronizes.
        let (relay_tx, mut relay_rx) = mpsc::unbounded_channel::<Result<Frame, AsyncHostError>>();
        let mut relay_connected = true; // Track relay connection state
        let relay_reader_task = tokio::spawn(async move {
            let mut reader = FrameReader::new(relay_read);
            loop {
                match reader.read().await {
                    Ok(Some(frame)) => {
                        if relay_tx.send(Ok(frame)).is_err() {
                            break; // Main loop dropped
                        }
                    }
                    Ok(None) => {
                        break; // Relay closed cleanly
                    }
                    Err(e) => {
                        let _ = relay_tx.send(Err(e.into()));
                        break;
                    }
                }
            }
        });

        let mut event_rx = self
            .event_rx
            .take()
            .expect("run() must only be called once");
        let mut command_rx = self
            .command_rx
            .take()
            .expect("run() must only be called once");

        let mut heartbeat_interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        heartbeat_interval.tick().await; // skip initial tick

        // Runtime-stats refresh cadence. Request counts and memory change
        // continuously; structural changes (spawn/death) already trigger
        // RelayNotify synchronously via `rebuild_capabilities`, so this
        // interval only needs to cover the continuous signals. Engine-side
        // watch dedup drops no-op frames when no stat actually changed.
        let mut stats_interval = tokio::time::interval(Duration::from_secs(2));
        stats_interval.tick().await; // skip initial tick

        // Send initial RelayNotify so the switch knows about pre-registered cartridges.
        self.rebuild_capabilities(Some(&outbound_tx));

        let result = loop {
            tokio::select! {
                biased;

                // Cartridge events (frames from cartridges, death notifications)
                Some(event) = event_rx.recv() => {
                    match event {
                        CartridgeEvent::Frame { cartridge_idx, generation, frame } => {
                            if self.cartridges.get(cartridge_idx).map(|c| c.generation)
                                != Some(generation)
                            {
                                continue;
                            }
                            if let Err(e) = self.handle_cartridge_frame(cartridge_idx, frame, &outbound_tx) {
                                break Err(e);
                            }
                        }
                        CartridgeEvent::Death { cartridge_idx, generation } => {
                            if let Err(e) = self.handle_cartridge_death(cartridge_idx, generation, &outbound_tx).await {
                                break Err(e);
                            }

                            // If relay disconnected AND all cartridges dead, exit cleanly
                            let all_cartridges_dead = self.cartridges.iter().all(|p| !p.running);
                            if !relay_connected && all_cartridges_dead {
                                break Ok(());
                            }
                        }
                    }
                }

                // Frames from relay reader task (cancel-safe: channel recv is cancel-safe)
                relay_result = relay_rx.recv(), if relay_connected => {
                    match relay_result {
                        Some(Ok(frame)) => {
                            if let Err(e) = self.handle_relay_frame(frame, &outbound_tx, &resource_fn).await {
                                break Err(e);
                            }
                        }
                        Some(Err(_)) => {
                            relay_connected = false; // Disable relay branch, continue processing cartridges

                            // If all cartridges are also dead, exit cleanly
                            let all_cartridges_dead = self.cartridges.iter().all(|p| !p.running);
                            if all_cartridges_dead {
                                break Ok(());
                            }
                        }
                        None => {
                            relay_connected = false; // Disable relay branch, continue processing cartridges

                            // If all cartridges are also dead, exit cleanly
                            let all_cartridges_dead = self.cartridges.iter().all(|p| !p.running);
                            if all_cartridges_dead {
                                break Ok(());
                            }
                        }
                    }
                }

                // Periodic heartbeat probes
                _ = heartbeat_interval.tick() => {
                    if let Err(e) = self.send_heartbeats_and_check_timeouts(&outbound_tx).await {
                        break Err(e);
                    }
                }

                // Periodic runtime-stats refresh — republish RelayNotify so
                // the engine sees current request counts, memory, and
                // heartbeat ages. Only fires the publish if at least one
                // cartridge is running, keeping idle hosts quiet.
                _ = stats_interval.tick() => {
                    // Retired-but-draining cartridges are reaped here: the tick
                    // is the host's regular opportunity to notice that the last
                    // in-flight request of a retired install has terminated.
                    self.reap_drained_cartridges().await;
                    let any_running = self.cartridges.iter().any(|c| c.running);
                    if any_running {
                        self.rebuild_capabilities(Some(&outbound_tx));
                    }
                }

                // External commands via CartridgeProcessHandle
                Some(cmd) = command_rx.recv() => {
                    if let Err(e) = self.handle_command(cmd, &outbound_tx).await {
                        break Err(e);
                    }
                }
            }
        };

        // Cleanup: kill all managed cartridge processes
        self.kill_all_cartridges().await;
        relay_reader_task.abort();
        outbound_writer.abort();

        result
    }

    // =========================================================================
    // FRAME HANDLING
    // =========================================================================

    /// Handle a frame arriving from the relay (engine → cartridge direction).
    async fn handle_relay_frame(
        &mut self,
        frame: Frame,
        outbound_tx: &mpsc::UnboundedSender<Frame>,
        resource_fn: &(impl Fn() -> Vec<u8> + Send),
    ) -> Result<(), AsyncHostError> {
        match frame.frame_type {
            FrameType::Req => {
                // PATH C: REQ coming FROM relay
                // MUST have XID (else FATAL - only switch can assign XIDs)
                let xid = match frame.routing_id.as_ref() {
                    Some(xid) => xid.clone(),
                    None => {
                        return Err(AsyncHostError::Protocol(
                            "REQ from relay missing XID - all frames from relay must have XID"
                                .to_string(),
                        ));
                    }
                };

                let cap_urn = match frame.cap.as_ref() {
                    Some(c) => c.clone(),
                    None => {
                        return Err(AsyncHostError::Protocol(
                            "REQ from relay missing cap URN".to_string(),
                        ));
                    }
                };

                // Check for target_cartridge in meta — if present, route directly
                // to that cartridge instead of using cap-based dispatch
                let target_cartridge_id = frame.meta.as_ref().and_then(|m| {
                    m.get("target_cartridge").and_then(|v| {
                        if let ciborium::Value::Text(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                });

                let cartridge_idx = if let Some(ref target_id) = target_cartridge_id {
                    // Direct routing by cartridge identity
                    let found = self.cartridges.iter().position(|c| {
                        c.installed_identity
                            .as_ref()
                            .map_or(false, |identity| identity.id == *target_id)
                    });
                    match found {
                        Some(idx) => {
                            // Check if cartridge is usable
                            if self.cartridges[idx].hello_failed {
                                // Handshake failure is a broken runtime
                                // deployment — Environment.
                                let mut err = Frame::err(
                                    frame.id.clone(),
                                    "CARTRIDGE_UNAVAILABLE",
                                    crate::failure::AttributionClass::Environment,
                                    &format!(
                                        "Cartridge '{}' failed handshake and cannot be spawned",
                                        target_id
                                    ),
                                    None,
                                );
                                err.routing_id = frame.routing_id.clone();
                                outbound_tx
                                    .send(err)
                                    .map_err(|_| AsyncHostError::SendError)?;
                                return Ok(());
                            }
                            idx
                        }
                        None => {
                            // Missing cartridge on this host is a deployment
                            // problem — Environment.
                            let mut err = Frame::err(
                                frame.id.clone(),
                                "CARTRIDGE_NOT_FOUND",
                                crate::failure::AttributionClass::Environment,
                                &format!("Cartridge '{}' not found on this host", target_id),
                                None,
                            );
                            err.routing_id = frame.routing_id.clone();
                            outbound_tx
                                .send(err)
                                .map_err(|_| AsyncHostError::SendError)?;
                            return Ok(());
                        }
                    }
                } else {
                    // Standard cap-based dispatch
                    match self.find_cartridge_for_cap(&cap_urn) {
                        Some(idx) => idx,
                        None => {
                            tracing::error!(
                                target: "host_runtime",
                                cap_urn = %cap_urn,
                                cap_table_size = self.cap_table.len(),
                                cap_table_sample = ?self.cap_table.iter().take(5).map(|(c, i)| (c.as_str(), *i)).collect::<Vec<_>>(),
                                "[CartridgeHostRuntime] NO_HANDLER for incoming REQ — no cartridge in cap_table is dispatchable"
                            );
                            // No dispatchable cartridge for a planned cap is a
                            // deployment/manifest mismatch — Environment.
                            let mut err = Frame::err(
                                frame.id.clone(),
                                "NO_HANDLER",
                                crate::failure::AttributionClass::Environment,
                                &format!("no cartridge handles cap: {}", cap_urn),
                                None,
                            );
                            err.routing_id = frame.routing_id.clone();
                            outbound_tx
                                .send(err)
                                .map_err(|_| AsyncHostError::SendError)?;
                            return Ok(());
                        }
                    }
                };

                // Spawn on demand if not running
                if !self.cartridges[cartridge_idx].running {
                    let spawn_outcome = self.spawn_cartridge(cartridge_idx, resource_fn).await;
                    // Always rebuild so the RelayNotify carries the latest
                    // per-cartridge attachment state — including freshly
                    // recorded failures — to the engine's RelaySwitch.
                    self.rebuild_capabilities(Some(outbound_tx));
                    spawn_outcome?;
                }

                // Record in List 2: INCOMING_RXIDS (XID, RID) → cartridge_idx
                let rxid_key = (xid.clone(), frame.id.clone());
                self.incoming_rxids.insert(rxid_key.clone(), cartridge_idx);
                self.touch_incoming_rxid(&rxid_key);
                self.gc_routing_tables_if_needed();

                // Forward to cartridge WITH XID
                self.send_to_cartridge(cartridge_idx, frame)
            }

            FrameType::StreamStart
            | FrameType::Chunk
            | FrameType::StreamEnd
            | FrameType::End
            | FrameType::Err
            | FrameType::Credit => {
                // PATH C: Continuation frame from relay. Credit rides the same
                // route as data continuations: it targets whichever cartridge is
                // sending the credited stream — the handler cartridge for a normal
                // request (via incoming_rxids) or the requester cartridge for a
                // peer call's argument streams (via outgoing_rids).
                // MUST have XID (else FATAL)
                let xid = match frame.routing_id.as_ref() {
                    Some(xid) => xid.clone(),
                    None => {
                        return Err(AsyncHostError::Protocol(format!(
                            "{:?} from relay missing XID - all frames from relay must have XID",
                            frame.frame_type
                        )));
                    }
                };

                // Route by checking BOTH maps. For self-loop peer requests (where
                // source and destination are behind the same relay connection), the
                // same (XID, RID) appears in BOTH incoming_rxids and outgoing_rids:
                //   incoming_rxids[(XID, RID)] = handler cartridge (receives request body)
                //   outgoing_rids[RID] = requester cartridge (receives peer response)
                //
                // Phase tracking: incoming_rxids entry is removed when the request
                // body END is delivered to the handler. After that, frames from
                // relay with the same (XID, RID) are peer responses and fall through
                // to outgoing_rids. This is safe because:
                //   1. Frames on a single socket are ordered — END is always last
                //   2. For non-peer requests, no further relay frames arrive after END
                let key = (xid.clone(), frame.id.clone());
                // Route selection:
                // - CREDIT routes by its mandatory direction (L11): a
                //   `response` grant credits the HANDLER's output → incoming
                //   side; a `request` grant credits the REQUESTER's argument
                //   streams → outgoing side. The (xid, rid) key alone cannot
                //   distinguish these for self-loop peer calls.
                // - Data/terminal frames prefer the incoming side while the
                //   request body is still flowing; after body END they are
                //   self-loop peer responses and fall through to outgoing.
                let prefer_incoming = match frame.frame_type {
                    FrameType::Credit => match frame.credit_direction() {
                        Some(crate::bifaci::frame::CreditDirection::Response) => true,
                        Some(crate::bifaci::frame::CreditDirection::Request) => false,
                        None => {
                            let total = self.drops.record(
                                crate::bifaci::frame::DropReason::NoRoute,
                                frame.frame_type,
                            );
                            tracing::warn!(
                                target: "host_runtime",
                                rid = ?frame.id,
                                no_route_total = total,
                                "[CartridgeHostRuntime] dropped CREDIT without direction — v4 requires credit_dir (no_route, L11)"
                            );
                            return Ok(());
                        }
                    },
                    _ => !self.incoming_body_done.contains(&key),
                };
                let incoming_hit = if prefer_incoming {
                    self.incoming_rxids.get(&key).copied()
                } else {
                    None
                };
                let (cartridge_idx, routed_via_incoming) = if let Some(idx) = incoming_hit {
                    // Hit on incoming side — touch so the GC
                    // doesn't evict an entry that's still seeing
                    // continuations.
                    self.touch_incoming_rxid(&key);
                    (idx, true)
                } else if let Some(&idx) = self.outgoing_rids.get(&frame.id) {
                    self.touch_outgoing_rid(&frame.id);
                    (idx, false)
                } else if let Some(&idx) = self.incoming_rxids.get(&key) {
                    // Fallback: no outgoing entry, so this cannot be a
                    // self-loop peer response — route to the handler even
                    // post-body-END (defensive; normal requests only ever
                    // see Credit here, handled above).
                    self.touch_incoming_rxid(&key);
                    (idx, true)
                } else {
                    // Discriminate the teardown race from real routing loss:
                    // a RID released by an observed terminal is a benign
                    // post-terminal straggler (the ordinary END/Credit race —
                    // nothing went wrong, counted separately from drops); a
                    // RID this host never routed is a genuine anomaly
                    // (`no_route` drop, warn).
                    if self.recently_released_rid(&frame.id) {
                        let total = self.stragglers.record(frame.frame_type);
                        tracing::debug!(
                            target: "host_runtime",
                            ftype = frame.frame_type.as_str(),
                            rid = ?frame.id,
                            xid = ?xid,
                            straggler_total = total,
                            "[CartridgeHostRuntime] benign post-terminal straggler — frame crossed \
                             its request's terminal in flight (expected teardown race, L4/L6)"
                        );
                    } else {
                        let total = self
                            .drops
                            .record(crate::bifaci::frame::DropReason::NoRoute, frame.frame_type);
                        tracing::warn!(
                            target: "host_runtime",
                            ftype = frame.frame_type.as_str(),
                            rid = ?frame.id,
                            xid = ?xid,
                            incoming_rxids_size = self.incoming_rxids.len(),
                            outgoing_rids_size = self.outgoing_rids.len(),
                            no_route_total = total,
                            "[CartridgeHostRuntime] dropped continuation frame — no routing entry (no_route, L6/L8)"
                        );
                    }
                    return Ok(()); // Already cleaned up
                };

                let is_terminal =
                    frame.frame_type == FrameType::End || frame.frame_type == FrameType::Err;

                // If the cartridge is dead, send ERR to engine and clean up routing.
                if self
                    .send_to_cartridge(cartridge_idx, frame.clone())
                    .is_err()
                {
                    let flow_key = FlowKey {
                        rid: frame.id.clone(),
                        xid: Some(xid.clone()),
                    };
                    let next_seq = self
                        .outgoing_max_seq
                        .remove(&flow_key)
                        .map(|s| s + 1)
                        .unwrap_or(0);
                    let death_msg = self.cartridges[cartridge_idx]
                        .last_death_message
                        .as_deref()
                        .unwrap_or("Cartridge exited while processing request");
                    // A dead cartridge process is a runtime-environment
                    // failure — Environment (docs/failure-taxonomy.md).
                    let mut err = Frame::err(
                        frame.id.clone(),
                        "CARTRIDGE_DIED",
                        crate::failure::AttributionClass::Environment,
                        death_msg,
                        None,
                    );
                    err.routing_id = frame.routing_id.clone();
                    err.seq = next_seq;
                    let _ = outbound_tx.send(err);

                    self.outgoing_rids.remove(&frame.id);
                    self.outgoing_rids_touched.remove(&frame.id);
                    self.incoming_rxids.remove(&key);
                    self.incoming_rxids_touched.remove(&key);
                    self.incoming_body_done.remove(&key);
                    self.incoming_response_done.remove(&key);
                    // The synthesized ERR terminated this request; stragglers
                    // for it are benign stragglers, not routing anomalies.
                    self.note_released_rid(&frame.id);
                    return Ok(());
                }

                // Terminal bookkeeping.
                // - Via incoming_rxids: the REQUEST BODY completed. The entry
                //   STAYS — the handler's response is still flowing and its
                //   output CREDIT grants route through it (v4). It is removed
                //   when the handler's response terminal passes outbound
                //   (handle_cartridge_frame) or on cartridge death.
                // - Via outgoing_rids: a peer RESPONSE completed — clean up.
                if is_terminal {
                    if routed_via_incoming {
                        if self.incoming_response_done.remove(&key) {
                            // Response already terminated (response-first
                            // race): the request is fully over — release.
                            self.incoming_rxids.remove(&key);
                            self.incoming_rxids_touched.remove(&key);
                            self.note_released_rid(&frame.id);
                        } else {
                            self.incoming_body_done.insert(key.clone());
                        }
                    } else {
                        // Peer response completed - clean up outgoing_rids
                        self.outgoing_rids.remove(&frame.id);
                        self.outgoing_rids_touched.remove(&frame.id);
                        self.note_released_rid(&frame.id);
                    }
                }

                Ok(())
            }

            // Everything else is a hard protocol error — these must never reach the runtime.
            FrameType::Hello => Err(AsyncHostError::Protocol(
                "HELLO from relay — engine must not send HELLO to runtime".to_string(),
            )),
            FrameType::Heartbeat => Err(AsyncHostError::Protocol(
                "HEARTBEAT from relay — engine must not send heartbeats to runtime".to_string(),
            )),
            FrameType::Log => {
                // LOG frames from peer responses — route back to the cartridge
                // that made the peer request, identified by outgoing_rids[RID].
                if let Some(&cartridge_idx) = self.outgoing_rids.get(&frame.id) {
                    let rid_for_touch = frame.id.clone();
                    self.touch_outgoing_rid(&rid_for_touch);
                    let _ = self.send_to_cartridge(cartridge_idx, frame);
                } else if self.recently_released_rid(&frame.id) {
                    // A LOG that crossed its peer request's terminal in
                    // flight — benign straggler, counted as such (never a
                    // drop): the request is over and the diagnostic is moot.
                    let total = self.stragglers.record(frame.frame_type);
                    tracing::debug!(
                        target: "host_runtime",
                        rid = ?frame.id,
                        straggler_total = total,
                        "[CartridgeHostRuntime] benign post-terminal straggler LOG — \
                         crossed its peer request's terminal (expected teardown race)"
                    );
                } else {
                    // No routing entry and never terminated here: COUNTED
                    // drop, never silent (L8) — a genuine routing anomaly.
                    let total = self
                        .drops
                        .record(crate::bifaci::frame::DropReason::NoRoute, frame.frame_type);
                    tracing::warn!(
                        target: "host_runtime",
                        rid = ?frame.id,
                        no_route_total = total,
                        "[CartridgeHostRuntime] dropped LOG with no routing entry (no_route)"
                    );
                }
                Ok(())
            }
            FrameType::CloseStream => {
                // CloseStream from relay — the tap-off (15.2 §Runs Stop).
                // Forwarded to the cartridge handling the request so it can
                // close the request's live feed(s); never cascaded (the tap
                // is on the feed-bearing request alone — its peers drain
                // naturally), never a kill, and no routing state changes:
                // the request ends on its own, later, with END.
                let xid = frame.routing_id.clone().ok_or_else(|| {
                    AsyncHostError::Protocol("CloseStream frame missing XID".to_string())
                })?;
                let key = (xid, frame.id.clone());
                if let Some(&cartridge_idx) = self.incoming_rxids.get(&key) {
                    self.touch_incoming_rxid(&key);
                    let _ = self.send_to_cartridge(cartridge_idx, frame);
                } else {
                    tracing::warn!(
                        target: "host_runtime",
                        rid = ?key.1,
                        "[CartridgeHostRuntime] CloseStream for a request this host is not serving — ignoring"
                    );
                }
                Ok(())
            }

            FrameType::Cancel => {
                // Cancel from relay — route to the cartridge handling this request.
                let xid = frame.routing_id.clone().ok_or_else(|| {
                    AsyncHostError::Protocol("Cancel frame missing XID".to_string())
                })?;
                let rid = frame.id.clone();
                let key = (xid.clone(), rid.clone());
                // The attribution rides in meta like an ERR's; an unattributed
                // Cancel is still a cancel.
                let reason = frame
                    .cancel_reason()
                    .expect("a Cancel frame always yields a reason");
                let force_kill = reason.force_kill;

                if let Some(&cartridge_idx) = self.incoming_rxids.get(&key) {
                    // Touch on cancel-route — the cancel itself is
                    // routing activity for this entry, and the
                    // cooperative branch below may produce more
                    // frames before the cartridge actually exits.
                    self.touch_incoming_rxid(&key);
                    if force_kill {
                        // Force kill: set shutdown reason and kill the process
                        self.cartridges[cartridge_idx].shutdown_reason =
                            Some(ShutdownReason::Cancelled(reason.clone()));
                        if let Some(ref mut child) = self.cartridges[cartridge_idx].process {
                            let _ = child.kill().await;
                        }
                    } else {
                        // Cooperative cancel: forward Cancel frame to the cartridge
                        let _ = self.send_to_cartridge(cartridge_idx, frame);

                        // Also cascade: send Cancel to relay for each peer call spawned by this request,
                        // under the same reason.
                        // Clone the peer-rid list out from under the immutable borrow before
                        // calling `touch_*` (which takes `&mut self`); otherwise the borrow
                        // checker rejects the simultaneous shared/mutable use.
                        let peer_rids_snapshot: Option<Vec<MessageId>> =
                            self.incoming_to_peer_rids.get(&key).cloned();
                        if let Some(peer_rids) = peer_rids_snapshot {
                            self.touch_incoming_to_peer_rids(&key);
                            let peer_reason = CancelReason {
                                force_kill: false,
                                ..reason.clone()
                            };
                            for peer_rid in peer_rids {
                                let cancel = Frame::cancel(peer_rid, &peer_reason);
                                let _ = outbound_tx.send(cancel);
                            }
                        }
                    }
                }
                Ok(())
            }
            FrameType::RelayNotify | FrameType::RelayState => {
                Err(AsyncHostError::Protocol(format!(
                    "{:?} reached runtime — relay must intercept these, never forward",
                    frame.frame_type
                )))
            }
        }
    }

    /// Handle a frame arriving from a cartridge (cartridge → engine direction).
    fn handle_cartridge_frame(
        &mut self,
        cartridge_idx: usize,
        frame: Frame,
        outbound_tx: &mpsc::UnboundedSender<Frame>,
    ) -> Result<(), AsyncHostError> {
        // Heartbeats and high-volume Log frames stay at debug; everything
        // else is logged at info level so we can trace REQ→response
        // round-trips (notably the engine's identity probe) end-to-end
        // without enabling debug logs.
        match frame.frame_type {
            // HELLO after handshake is a fatal protocol error.
            FrameType::Hello => Err(AsyncHostError::Protocol(format!(
                "Cartridge {} sent HELLO after handshake — fatal protocol violation",
                cartridge_idx
            ))),

            // Heartbeat: handle locally, never forward.
            FrameType::Heartbeat => {
                let is_our_probe = self.cartridges[cartridge_idx]
                    .pending_heartbeats
                    .remove(&frame.id)
                    .is_some();

                if is_our_probe {
                    // Response to our health probe — cartridge is alive.
                    // Extract self-reported memory from heartbeat response meta.
                    // Cartridges include their own ri_phys_footprint and ri_resident_size
                    // (via proc_pid_rusage(getpid())) in the meta map.
                    if let Some(ref meta) = frame.meta {
                        if let Some(ciborium::Value::Integer(v)) = meta.get("footprint_mb") {
                            self.cartridges[cartridge_idx].memory_footprint_mb =
                                u64::try_from(*v).unwrap_or(0);
                        }
                        if let Some(ciborium::Value::Integer(v)) = meta.get("rss_mb") {
                            self.cartridges[cartridge_idx].memory_rss_mb =
                                u64::try_from(*v).unwrap_or(0);
                        }
                        // Cumulative protocol drop counter (L8). The reading
                        // is the cartridge's running total — stored as-is,
                        // never merged or maxed.
                        if let Some(ciborium::Value::Integer(v)) = meta.get("drops_total") {
                            self.cartridges[cartridge_idx].protocol_drops_total =
                                Some(u64::try_from(*v).unwrap_or(0));
                        }
                        // Cumulative BENIGN straggler counter — indicated
                        // separately from drops (nothing went wrong).
                        if let Some(ciborium::Value::Integer(v)) = meta.get("stragglers_total") {
                            self.cartridges[cartridge_idx].protocol_stragglers_total =
                                Some(u64::try_from(*v).unwrap_or(0));
                        }
                        // Cumulative live-feed overrun counter — its own
                        // category (12.5 §Overrun), never folded into drops.
                        if let Some(ciborium::Value::Integer(v)) = meta.get("overruns_total") {
                            self.cartridges[cartridge_idx].protocol_overruns_total =
                                Some(u64::try_from(*v).unwrap_or(0));
                        }
                        // The full pool-state map is MANDATORY on every
                        // heartbeat reply — pools are the protocol's one
                        // capacity concept, and a reply without them is a
                        // protocol error, not a default.
                        let pool_bytes = meta
                            .get(crate::bifaci::pools::META_POOLS)
                            .and_then(|value| match value {
                                ciborium::Value::Bytes(bytes) => Some(bytes.as_slice()),
                                _ => None,
                            })
                            .ok_or_else(|| {
                                AsyncHostError::Protocol(format!(
                                    "Cartridge {} heartbeat missing required concurrency-pool state map",
                                    cartridge_idx
                                ))
                            })?;
                        let pool_states = crate::bifaci::pools::decode_pool_states(pool_bytes)
                            .map_err(|e| {
                                AsyncHostError::Protocol(format!(
                                    "Cartridge {cartridge_idx} heartbeat pool map: {e}"
                                ))
                            })?;
                        self.cartridges[cartridge_idx].pool_states = pool_states;
                    }
                    // Stamp the round-trip completion timestamp so the
                    // runtime-stats snapshot can surface heartbeat age to the UI.
                    let now_secs = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0);
                    self.cartridges[cartridge_idx].last_heartbeat_unix_seconds = Some(now_secs);
                    self.update_process_snapshot();
                } else {
                    // Cartridge-initiated heartbeat — respond immediately
                    let response = Frame::heartbeat(frame.id.clone());
                    self.send_to_cartridge(cartridge_idx, response)?;
                }
                Ok(())
            }

            // Relay frames from a cartridge: fatal protocol error.
            FrameType::RelayNotify | FrameType::RelayState => {
                Err(AsyncHostError::Protocol(format!(
                    "Cartridge {} sent {:?} — cartridges must never send relay frames",
                    cartridge_idx, frame.frame_type
                )))
            }

            // PATH A: REQ from cartridge (peer invoke)
            // MUST have RID, MUST NOT have XID (cartridges never send XID)
            FrameType::Req => {
                if frame.routing_id.is_some() {
                    return Err(AsyncHostError::Protocol(format!(
                        "Cartridge {} sent REQ with XID - cartridges must never send XID",
                        cartridge_idx
                    )));
                }

                // Record in List 1: OUTGOING_RIDS
                self.outgoing_rids.insert(frame.id.clone(), cartridge_idx);
                let rid_for_touch = frame.id.clone();
                self.touch_outgoing_rid(&rid_for_touch);

                // Track parent→child peer call mapping for cancel cascade
                if let Some(parent_rid) = frame
                    .meta
                    .as_ref()
                    .and_then(|m| m.get("parent_rid"))
                    .and_then(|v| match v {
                        ciborium::Value::Bytes(bytes) if bytes.len() == 16 => {
                            let mut arr = [0u8; 16];
                            arr.copy_from_slice(bytes);
                            Some(MessageId::Uuid(arr))
                        }
                        ciborium::Value::Integer(i) => {
                            let n: i128 = (*i).into();
                            Some(MessageId::Uint(n as u64))
                        }
                        _ => None,
                    })
                {
                    // Find the parent's incoming_rxids entry to get its (xid, rid) key
                    let parent_key = self
                        .incoming_rxids
                        .keys()
                        .find(|(_, rid)| *rid == parent_rid)
                        .cloned();
                    if let Some(pk) = parent_key {
                        self.incoming_to_peer_rids
                            .entry(pk.clone())
                            .or_default()
                            .push(frame.id.clone());
                        self.touch_incoming_to_peer_rids(&pk);
                    }
                }

                // Track max-seen seq for host-generated ERR on death
                let flow_key = FlowKey::from_frame(&frame);
                self.outgoing_max_seq.insert(flow_key.clone(), frame.seq);
                self.touch_outgoing_max_seq(&flow_key);
                // GC after recording — covers all four tables
                // touched in this branch.
                self.gc_routing_tables_if_needed();

                // Forward as-is to relay (no XID - will be assigned by RelaySwitch)
                outbound_tx
                    .send(frame)
                    .map_err(|_| AsyncHostError::SendError)
            }

            // PATH A: Continuation frames from cartridge (request body or response)
            // When responding to relay requests, frames WILL have XID (routing_id)
            // When responding to direct requests, frames will NOT have XID
            // NO routing decisions - only one destination (relay)
            _ => {
                // Track max-seen seq for flow, clean up on terminal
                if frame.is_flow_frame() {
                    let flow_key = FlowKey::from_frame(&frame);
                    let is_terminal =
                        frame.frame_type == FrameType::End || frame.frame_type == FrameType::Err;
                    if is_terminal {
                        self.outgoing_max_seq.remove(&flow_key);
                        self.outgoing_max_seq_touched.remove(&flow_key);

                        // The handler's RESPONSE terminal is the request's true
                        // end at this host (v4): once the body has completed
                        // too, release the incoming routing entry and its
                        // body-done marker. If the response terminates BEFORE
                        // the body END arrives (response-first race), remember
                        // it so the body END releases the entry immediately.
                        if let Some(xid) = frame.routing_id.clone() {
                            let key = (xid, frame.id.clone());
                            if self.incoming_body_done.remove(&key) {
                                self.incoming_rxids.remove(&key);
                                self.incoming_rxids_touched.remove(&key);
                                self.note_released_rid(&frame.id);
                            } else if self.incoming_rxids.contains_key(&key) {
                                self.incoming_response_done.insert(key);
                            }
                        }
                    } else {
                        self.outgoing_max_seq.insert(flow_key.clone(), frame.seq);
                        self.touch_outgoing_max_seq(&flow_key);
                        self.gc_routing_tables_if_needed();
                    }
                }

                // Forward as-is to relay (no routing, no XID manipulation)
                outbound_tx
                    .send(frame)
                    .map_err(|_| AsyncHostError::SendError)
            }
        }
    }

    /// Handle a cartridge death (reader loop exited).
    ///
    /// Four cases based on `shutdown_reason`:
    /// 1. **`None`** (unexpected death): Genuine crash. Send ERR "CARTRIDGE_DIED"
    ///    for all pending requests, store death message.
    /// 2. **`Some(OomKill)`**: OOM watchdog killed the cartridge while it was
    ///    actively processing. Send ERR "OOM_KILLED" for all pending requests
    ///    so callers fail fast instead of hanging.
    /// 3. **`Some(HeartbeatTimeout)`**: Unresponsive process. Send ERR
    ///    "CARTRIDGE_UNHEALTHY" and retire the full process generation.
    /// 4. **`Some(AppExit)`**: Clean shutdown. No ERR frames — the relay
    ///    connection is closing anyway.
    async fn handle_cartridge_death(
        &mut self,
        cartridge_idx: usize,
        generation: u64,
        outbound_tx: &mpsc::UnboundedSender<Frame>,
    ) -> Result<(), AsyncHostError> {
        use tokio::io::AsyncReadExt;

        if self
            .cartridges
            .get(cartridge_idx)
            .map(|cartridge| cartridge.generation)
            != Some(generation)
        {
            return Ok(());
        }

        // Scope the mutable borrow of the cartridge so we can access self later.
        let reason;
        let stderr_content;
        let exit_info: String;
        let reader_handle;
        let writer_handle;
        // Capture observer payload before we mutate state and clear the
        // process handle.
        let observer_pid_at_death;
        let observer_name;
        {
            let cartridge = &mut self.cartridges[cartridge_idx];
            observer_pid_at_death = cartridge.process.as_ref().and_then(|c| c.id());
            observer_name = cartridge
                .path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned();
            cartridge.generation = cartridge
                .generation
                .checked_add(1)
                .expect("cartridge process generation overflow");
            cartridge.running = false;
            cartridge.writer_tx = None;
            writer_handle = cartridge.writer_handle.take();
            reader_handle = cartridge.reader_handle.take();
            cartridge.pending_heartbeats.clear();
            // One completed death (any reason) counts as one restart cycle.
            // The next on-demand spawn will increment `running` again with
            // a fresh process.
            cartridge.restart_count = cartridge.restart_count.saturating_add(1);
            reason = cartridge.shutdown_reason.take();

            // Capture stderr content BEFORE killing the process
            let mut captured = String::new();
            if let Some(ref mut stderr) = cartridge.stderr_handle {
                let mut buf = vec![0u8; 4096];
                loop {
                    match tokio::time::timeout(Duration::from_millis(100), stderr.read(&mut buf))
                        .await
                    {
                        Ok(Ok(0)) => break,
                        Ok(Ok(n)) => {
                            if let Ok(s) = std::str::from_utf8(&buf[..n]) {
                                captured.push_str(s);
                            }
                            if captured.len() > 2000 {
                                captured.truncate(2000);
                                captured.push_str("... [truncated]");
                                break;
                            }
                        }
                        Ok(Err(_)) | Err(_) => break,
                    }
                }
            }
            cartridge.stderr_handle = None;

            // Capture exit status and kill the process if it's still around
            if let Some(ref mut child) = cartridge.process {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        #[cfg(unix)]
                        {
                            use std::os::unix::process::ExitStatusExt;
                            if let Some(sig) = status.signal() {
                                exit_info = format!("killed by signal {}", sig);
                            } else {
                                exit_info = format!("exit code {}", status.code().unwrap_or(-1));
                            }
                        }
                        #[cfg(not(unix))]
                        {
                            exit_info = format!("exit code {:?}", status.code());
                        }
                    }
                    Ok(None) => {
                        // Still running — kill it
                        let _ = child.kill().await;
                        exit_info = "still running (killed)".to_string();
                    }
                    Err(e) => {
                        exit_info = format!("try_wait failed: {}", e);
                    }
                }
            } else {
                exit_info = String::new();
            }
            cartridge.process = None;
            stderr_content = captured;
        }

        if let Some(handle) = writer_handle {
            let _ = handle.await;
        }
        if let Some(handle) = reader_handle {
            handle.abort();
            let _ = handle.await;
        }

        // Clean up routing tables regardless of death cause.
        // outgoing_rids: peer requests the cartridge initiated.
        // Collect (rid, flow_key) under immutable borrow first,
        // then drain `outgoing_max_seq` in a second pass.
        let failed_outgoing_keys: Vec<(MessageId, FlowKey)> = self
            .outgoing_rids
            .iter()
            .filter(|(_, &idx)| idx == cartridge_idx)
            .map(|(rid, _)| {
                let flow_key = FlowKey {
                    rid: rid.clone(),
                    xid: None,
                };
                (rid.clone(), flow_key)
            })
            .collect();
        let failed_outgoing: Vec<(MessageId, u64)> = failed_outgoing_keys
            .into_iter()
            .map(|(rid, flow_key)| {
                let next_seq = self
                    .outgoing_max_seq
                    .remove(&flow_key)
                    .map(|s| s + 1)
                    .unwrap_or(0);
                self.outgoing_max_seq_touched.remove(&flow_key);
                (rid, next_seq)
            })
            .collect();

        for (rid, _) in &failed_outgoing {
            self.outgoing_rids.remove(rid);
            self.outgoing_rids_touched.remove(rid);
        }
        let released_outgoing: Vec<MessageId> =
            failed_outgoing.iter().map(|(rid, _)| rid.clone()).collect();
        for rid in &released_outgoing {
            // The death sweep synthesizes ERR terminals for these RIDs below;
            // frames for them classify as benign stragglers.
            self.note_released_rid(rid);
        }

        // incoming_rxids: requests from the relay that this cartridge was handling.
        // Collect (xid, rid, flow_key) under an immutable borrow,
        // then drain `outgoing_max_seq` in a second pass with the
        // mutable borrow.
        let failed_incoming_keys: Vec<(MessageId, MessageId, FlowKey)> = self
            .incoming_rxids
            .iter()
            .filter(|(_, &idx)| idx == cartridge_idx)
            .map(|((xid, rid), _)| {
                let flow_key = FlowKey {
                    rid: rid.clone(),
                    xid: Some(xid.clone()),
                };
                (xid.clone(), rid.clone(), flow_key)
            })
            .collect();
        let failed_incoming: Vec<(MessageId, MessageId, u64)> = failed_incoming_keys
            .into_iter()
            .map(|(xid, rid, flow_key)| {
                let next_seq = self
                    .outgoing_max_seq
                    .remove(&flow_key)
                    .map(|s| s + 1)
                    .unwrap_or(0);
                self.outgoing_max_seq_touched.remove(&flow_key);
                (xid, rid, next_seq)
            })
            .collect();
        // Collect dying keys first so the touched-map cleanup
        // doesn't double-borrow `self`.
        let dying_rxids_keys: Vec<(MessageId, MessageId)> = self
            .incoming_rxids
            .iter()
            .filter_map(|(k, &idx)| {
                if idx == cartridge_idx {
                    Some(k.clone())
                } else {
                    None
                }
            })
            .collect();
        for k in &dying_rxids_keys {
            self.incoming_rxids.remove(k);
            self.incoming_rxids_touched.remove(k);
            self.incoming_body_done.remove(k);
            self.incoming_response_done.remove(k);
        }
        let released_incoming: Vec<MessageId> =
            dying_rxids_keys.iter().map(|(_, rid)| rid.clone()).collect();
        for rid in &released_incoming {
            self.note_released_rid(rid);
        }

        // Clean up incoming_to_peer_rids for all requests from this cartridge
        for (xid, rid, _) in &failed_incoming {
            self.incoming_to_peer_rids
                .remove(&(xid.clone(), rid.clone()));
            self.incoming_to_peer_rids_touched
                .remove(&(xid.clone(), rid.clone()));
        }

        // Determine error code, failure class, and message based on shutdown
        // reason — the class is DECLARED here at the emit source
        // (docs/failure-taxonomy.md): a crash is an Environment problem, an
        // OOM kill is a Resource problem, a cancel stays Internal.
        // Both unexpected deaths and OOM kills send ERR frames for pending work.
        // Only AppExit suppresses ERR frames (relay is closing, no callers left).
        let err_info: Option<(&str, crate::failure::AttributionClass, String)> = match &reason {
            None => {
                // Unexpected death — genuine crash mid-flight
                let exit_suffix = if exit_info.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", exit_info)
                };
                let error_message = if stderr_content.is_empty() {
                    format!(
                        "Cartridge {} exited unexpectedly{}.",
                        self.cartridges[cartridge_idx].path.display(),
                        exit_suffix
                    )
                } else {
                    format!(
                        "Cartridge {} exited unexpectedly{}. stderr:\n{}",
                        self.cartridges[cartridge_idx].path.display(),
                        exit_suffix,
                        stderr_content
                    )
                };
                Some((
                    "CARTRIDGE_DIED",
                    crate::failure::AttributionClass::Environment,
                    error_message,
                ))
            }
            Some(ShutdownReason::OomKill) => {
                // OOM watchdog killed the cartridge — callers must be notified
                let exit_suffix = if exit_info.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", exit_info)
                };
                let error_message = if stderr_content.is_empty() {
                    format!(
                        "Cartridge {} killed by OOM watchdog{}.",
                        self.cartridges[cartridge_idx].path.display(),
                        exit_suffix
                    )
                } else {
                    format!(
                        "Cartridge {} killed by OOM watchdog{}. stderr:\n{}",
                        self.cartridges[cartridge_idx].path.display(),
                        exit_suffix,
                        stderr_content
                    )
                };
                Some((
                    "OOM_KILLED",
                    crate::failure::AttributionClass::Resource,
                    error_message,
                ))
            }
            Some(ShutdownReason::Cancelled(reason)) => {
                // Force-kill under a cancel — every pending request ends in
                // the cancel's OWN attribution, so a collateral or host abort
                // never reads as a user cancel.
                Some((
                    reason.terminal_code(),
                    reason.terminal_class(),
                    format!(
                        "Cartridge {} killed by a force-kill cancel: {}",
                        self.cartridges[cartridge_idx].path.display(),
                        reason.terminal_message()
                    ),
                ))
            }
            Some(ShutdownReason::RosterRetired) => {
                // The install left the desired roster. The cause is the
                // deployment (registry listing, operator disable, on-disk
                // replacement), so the class is `environment` — declared here,
                // at the emit source, per docs/failure-taxonomy.md.
                Some((
                    "CARTRIDGE_RETIRED",
                    crate::failure::AttributionClass::Environment,
                    format!(
                        "Cartridge {} retired: it is no longer in the host's desired roster.",
                        self.cartridges[cartridge_idx].path.display()
                    ),
                ))
            }
            Some(ShutdownReason::HeartbeatTimeout) => Some((
                "CARTRIDGE_UNHEALTHY",
                crate::failure::AttributionClass::Environment,
                "Cartridge stopped responding to heartbeats".to_string(),
            )),
            Some(ShutdownReason::AppExit) => {
                // Clean shutdown — no ERR frames, relay is closing
                None
            }
        };

        if let Some((error_code, attribution_class, error_message)) = err_info {
            self.cartridges[cartridge_idx].last_death_message = Some(error_message.clone());

            // Surface the death (with the OS exit status / signal captured above)
            // in the host log. Previously this only travelled as ERR frames to
            // pending callers, so a cartridge dying with e.g. "killed by signal
            // 11" left no trace in the saved logs — only the relay's bare "master
            // died". A silent signal-kill (SIGSEGV/SIGKILL) vs a clean non-zero
            // exit is the first thing needed to diagnose a crashing cartridge.
            tracing::error!(
                target: "host_runtime",
                cartridge = %self.cartridges[cartridge_idx].path.display(),
                code = error_code,
                "{}",
                error_message
            );

            for (rid, next_seq) in &failed_outgoing {
                let mut err_frame = Frame::err(
                    rid.clone(),
                    error_code,
                    attribution_class,
                    &error_message,
                    None,
                );
                err_frame.seq = *next_seq;
                let _ = outbound_tx.send(err_frame);
            }
            for (xid, rid, next_seq) in &failed_incoming {
                let mut err_frame = Frame::err(
                    rid.clone(),
                    error_code,
                    attribution_class,
                    &error_message,
                    None,
                );
                err_frame.routing_id = Some(xid.clone());
                err_frame.seq = *next_seq;
                let _ = outbound_tx.send(err_frame);
            }
        } else {
            self.cartridges[cartridge_idx].last_death_message = None;
        }

        // Rebuild cap table for on-demand respawn routing
        self.update_cap_table();
        self.rebuild_capabilities(Some(outbound_tx));
        self.update_process_snapshot();

        // Notify lifecycle observer (e.g., XPC reverse-callback bridge).
        if let Some(ref obs) = self.observer {
            obs.cartridge_died(cartridge_idx, observer_pid_at_death, &observer_name);
        }

        Ok(())
    }

    /// Handle an external command received via the `CartridgeProcessHandle`.
    async fn handle_command(
        &mut self,
        command: HostCommand,
        outbound_tx: &mpsc::UnboundedSender<Frame>,
    ) -> Result<(), AsyncHostError> {
        match command {
            HostCommand::KillCartridge { pid } => {
                // Find the cartridge with the matching PID
                let cartridge_idx = self.cartridges.iter().position(|p| {
                    p.running && p.process.as_ref().and_then(|c| c.id()) == Some(pid)
                });
                if let Some(idx) = cartridge_idx {
                    self.cartridges[idx].shutdown_reason = Some(ShutdownReason::OomKill);
                    if let Some(ref mut child) = self.cartridges[idx].process {
                        let _ = child.kill().await;
                    }
                    // Death event will arrive via the reader task; handle_cartridge_death
                    // will do the full cleanup.
                } else {
                    tracing::warn!(
                        target: "host_runtime",
                        pid = pid,
                        "Kill command for unknown/dead PID — ignoring"
                    );
                }
            }
            HostCommand::SyncRoster {
                cartridges,
                static_records,
            } => {
                self.sync_registered_roster(cartridges, static_records, outbound_tx)
                    .await?;
            }
            HostCommand::ApplyDesiredCapacities {
                cartridge_id,
                desired,
                reply,
            } => {
                let outcome = self
                    .apply_desired_capacities(&cartridge_id, &desired)
                    .map_err(|e| e.to_string());
                // A dropped receiver means the caller gave up waiting —
                // the values are already queued/probed; nothing to undo.
                let _ = reply.send(outcome);
            }
        }
        Ok(())
    }

    /// Apply a freshly-discovered registered-dir roster to the LIVE host and
    /// re-publish RelayNotify. Mirrors the macOS XPC `syncDiscoveryOutcomes`:
    /// the engine's cap inventory updates in place, with no reconnect.
    ///
    /// Identity is the `(registry_url, channel, id, version)` 4-tuple (the same
    /// key the daemon/engine use everywhere). For each desired spec not already
    /// present we append a registered-dir cartridge (lazily spawned on first
    /// REQ, exactly like initial registration). For each currently-present
    /// registered-dir cartridge absent from the desired set we retire it: a
    /// running one is killed (its death cleanup runs as usual) and it is marked
    /// `hello_failed` so it drops out of the cap table and the RelayNotify
    /// inventory. We never physically remove entries from `self.cartridges`,
    /// because `cap_table` and the in-flight request maps are index-keyed and a
    /// shift would corrupt routing; marking `hello_failed` + rebuilding the cap
    /// table is the established "remove from inventory" mechanism (see
    /// `update_cap_table`).
    /// Requests this cartridge is currently serving or awaiting a peer response
    /// for. Both directions count: killing mid-peer-call strands the caller just
    /// as surely as killing mid-request.
    fn in_flight_count(&self, cartridge_idx: usize) -> usize {
        self.incoming_rxids
            .values()
            .filter(|idx| **idx == cartridge_idx)
            .count()
            + self
                .outgoing_rids
                .values()
                .filter(|idx| **idx == cartridge_idx)
                .count()
    }

    /// Kill a retired cartridge, declaring the retirement as the reason so its
    /// pending work (if any) is attributed to the environment rather than
    /// reported as a cancellation.
    async fn retire_kill(&mut self, cartridge_idx: usize) {
        self.cartridges[cartridge_idx].retiring_since = None;
        self.cartridges[cartridge_idx].shutdown_reason = Some(ShutdownReason::RosterRetired);
        if let Some(ref mut child) = self.cartridges[cartridge_idx].process {
            let _ = child.kill().await;
        }
    }

    /// Kill retired cartridges that have finished draining, and any whose drain
    /// outlived [`RETIRE_DRAIN_TIMEOUT`]. Called on the host's periodic tick.
    async fn reap_drained_cartridges(&mut self) {
        let now = tokio::time::Instant::now();
        let ready: Vec<usize> = self
            .cartridges
            .iter()
            .enumerate()
            .filter_map(|(idx, cartridge)| {
                let since = cartridge.retiring_since?;
                if !cartridge.running {
                    return Some(idx);
                }
                let drained = self.in_flight_count(idx) == 0;
                let expired = now.duration_since(since) >= RETIRE_DRAIN_TIMEOUT;
                (drained || expired).then_some(idx)
            })
            .collect();
        for idx in ready {
            if self.cartridges[idx].running {
                tracing::info!(
                    target: "host_runtime",
                    cartridge = %self.cartridges[idx].path.display(),
                    in_flight = self.in_flight_count(idx),
                    "retired cartridge drained — shutting it down"
                );
                self.retire_kill(idx).await;
            } else {
                self.cartridges[idx].retiring_since = None;
            }
        }
    }

    async fn sync_registered_roster(
        &mut self,
        desired: Vec<RegisteredDirSpec>,
        static_records: Vec<InstalledCartridgeRecord>,
        outbound_tx: &mpsc::UnboundedSender<Frame>,
    ) -> Result<(), AsyncHostError> {
        fn identity(
            rec: &InstalledCartridgeRecord,
        ) -> (
            Option<String>,
            crate::bifaci::cartridge_repo::CartridgeChannel,
            String,
            String,
        ) {
            (
                rec.registry_url.clone(),
                rec.channel,
                rec.id.clone(),
                rec.version.clone(),
            )
        }
        let desired_keys: std::collections::HashSet<_> = desired
            .iter()
            .map(|s| {
                (
                    s.registry_url.clone(),
                    s.channel,
                    s.id.clone(),
                    s.version.clone(),
                )
            })
            .collect();

        // Retire registered-dir cartridges no longer desired.
        for idx in 0..self.cartridges.len() {
            if self.cartridges[idx].removed {
                continue;
            }
            let Some(rec) = self.cartridges[idx].installed_cartridge_record() else {
                continue; // no resolvable identity (e.g. internal cartridge) — leave it
            };
            // Only retire dir-registered cartridges (those carry a version_dir);
            // attached/internal cartridges are not part of a dir roster sync.
            if !self.cartridges[idx].is_registered_dir() {
                continue;
            }
            if desired_keys.contains(&identity(&rec)) {
                continue; // still desired — keep, preserving any live process
            }
            // Retire = stop giving it NEW work. Dropping it from the cap table
            // and the inventory does that immediately; whether the process dies
            // now depends on whether it is mid-request.
            self.cartridges[idx].removed = true; // retire: drop from cap table + inventory
            self.cartridges[idx].hello_failed = true; // keep out of dispatch/spawn paths
            if !self.cartridges[idx].running {
                continue;
            }
            if self.in_flight_count(idx) == 0 {
                self.retire_kill(idx).await;
            } else {
                // DRAIN. Killing here would ERR every in-flight request of a
                // cartridge that is healthy and doing exactly what it was asked
                // to do. It finishes, then dies (see `reap_drained_cartridges`).
                self.cartridges[idx].retiring_since = Some(tokio::time::Instant::now());
                tracing::info!(
                    target: "host_runtime",
                    cartridge = %self.cartridges[idx].path.display(),
                    in_flight = self.in_flight_count(idx),
                    "cartridge retired from the roster — draining in-flight requests before shutdown"
                );
            }
        }

        // A roster that flaps — retire, then restore the same identity moments
        // later, exactly what a transient registry outage produces — must find
        // the DRAINING process again rather than leave it to die and spawn a
        // second one beside it. Un-retiring keeps the live process, its warm
        // model, and its queue.
        for idx in 0..self.cartridges.len() {
            if self.cartridges[idx].retiring_since.is_none() {
                continue;
            }
            let Some(rec) = self.cartridges[idx].installed_cartridge_record() else {
                continue;
            };
            if !desired_keys.contains(&identity(&rec)) {
                continue;
            }
            self.cartridges[idx].retiring_since = None;
            self.cartridges[idx].removed = false;
            self.cartridges[idx].hello_failed = false;
            tracing::info!(
                target: "host_runtime",
                cartridge = %self.cartridges[idx].path.display(),
                "cartridge returned to the desired roster while draining — retirement cancelled"
            );
        }

        // Add newly-desired specs not already registered.
        let present_keys: std::collections::HashSet<_> = self
            .cartridges
            .iter()
            .filter(|c| !c.hello_failed)
            .filter_map(|c| c.installed_cartridge_record().map(|r| identity(&r)))
            .collect();
        for spec in desired {
            let key = (
                spec.registry_url.clone(),
                spec.channel,
                spec.id.clone(),
                spec.version.clone(),
            );
            if present_keys.contains(&key) {
                continue;
            }
            self.register_cartridge_dir(
                &spec.entry_point,
                &spec.version_dir,
                &spec.id,
                spec.channel,
                spec.registry_url.as_deref(),
                &spec.version,
                &spec.cap_groups,
            );
        }

        // The rejected-install half of the same discovery pass. Replaced
        // wholesale, exactly like the attachable half above: a cartridge that
        // was rejected last pass and is attachable now must STOP being
        // advertised as rejected, or the engine and the UI keep reporting a
        // failure reason for a cartridge that is serving requests.
        self.static_inventory_records = static_records;

        self.update_cap_table();
        self.rebuild_capabilities(Some(outbound_tx));
        self.update_process_snapshot();
        Ok(())
    }

    // =========================================================================
    // CARTRIDGE LIFECYCLE
    // =========================================================================

    /// Spawn a registered cartridge binary on demand.
    async fn spawn_cartridge(
        &mut self,
        cartridge_idx: usize,
        _resource_fn: &(impl Fn() -> Vec<u8> + Send),
    ) -> Result<(), AsyncHostError> {
        let cartridge = &self.cartridges[cartridge_idx];

        if cartridge.hello_failed {
            return Err(AsyncHostError::Protocol(format!(
                "Cartridge '{}' permanently failed — HELLO failure, binary is broken",
                cartridge.path.display()
            )));
        }

        if cartridge.path.as_os_str().is_empty() {
            return Err(AsyncHostError::Protocol(format!(
                "Cartridge {} has no binary path — cannot spawn",
                cartridge_idx
            )));
        }

        let mut child = match tokio::process::Command::new(&cartridge.path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped()) // Capture stderr for crash diagnostics
            .kill_on_drop(true) // No orphan processes
            .spawn()
        {
            Ok(child) => child,
            Err(e) => {
                let msg = format!(
                    "Failed to spawn cartridge '{}': {}",
                    cartridge.path.display(),
                    e
                );
                self.cartridges[cartridge_idx].record_attachment_error(
                    CartridgeAttachmentErrorKind::EntryPointMissing,
                    msg.clone(),
                );
                return Err(AsyncHostError::Io(msg));
            }
        };

        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take();

        // Forward cartridge stderr to the host tracing output, line by line. A
        // cartridge that dies ("Master died — Connection closed unexpectedly")
        // prints its panic / abort message on stderr; draining and discarding
        // those lines (as this did) hides every cartridge-side crash and leaves
        // only the relay's after-the-fact "master died" with no cause. Emitted at
        // debug on this module's target, which the scenario harness's default
        // filter (`capdag::bifaci::host_runtime=debug`) captures into the saved
        // test log so cartridge crashes are diagnosable from the log alone.
        if let Some(cartridge_stderr) = stderr {
            let cartridge_path = cartridge.path.clone();
            tokio::spawn(async move {
                use tokio::io::AsyncBufReadExt;
                let mut reader = tokio::io::BufReader::new(cartridge_stderr);
                let mut line = String::new();
                loop {
                    line.clear();
                    match reader.read_line(&mut line).await {
                        Ok(0) | Err(_) => break, // EOF or read error: cartridge closed stderr
                        Ok(_) => {
                            let text = line.trim_end();
                            if !text.is_empty() {
                                tracing::debug!(
                                    cartridge = %cartridge_path.display(),
                                    "[cartridge stderr] {text}"
                                );
                            }
                        }
                    }
                }
            });
        }
        let stderr: Option<tokio::process::ChildStderr> = None; // Already consumed above

        // HELLO handshake — bounded by a hard timeout so a cartridge
        // that fails to start its CBOR-mode reader cannot hold up the
        // host event loop indefinitely. Cold-start of a Rust cartridge
        // is normally <1s; a Swift cartridge with sandbox-deferred
        // init can stretch to a few seconds. 15s is generous but
        // still bounded.
        const HELLO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
        let hs_started_at = std::time::Instant::now();
        let mut reader = FrameReader::new(stdout);
        let mut writer = FrameWriter::new(stdin);

        let hs_outcome =
            tokio::time::timeout(HELLO_TIMEOUT, handshake(&mut reader, &mut writer)).await;
        let handshake_result = match hs_outcome {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => {
                // HELLO failure = permanent removal. Binary is broken.
                let msg = format!(
                    "Cartridge '{}' HELLO failed: {} — permanently removed",
                    self.cartridges[cartridge_idx].path.display(),
                    e
                );
                tracing::error!(target: "host_runtime", error = %msg, "[CartridgeHostRuntime] HELLO failed");
                self.cartridges[cartridge_idx].record_attachment_error(
                    CartridgeAttachmentErrorKind::HandshakeFailed,
                    msg.clone(),
                );
                let _ = child.kill().await;
                return Err(AsyncHostError::Handshake(msg));
            }
            Err(_) => {
                let msg = format!(
                    "Cartridge '{}' HELLO timed out after {:?} — cartridge process did not respond. Permanently quarantining.",
                    self.cartridges[cartridge_idx].path.display(),
                    HELLO_TIMEOUT
                );
                tracing::error!(target: "host_runtime", error = %msg, "[CartridgeHostRuntime] HELLO timed out");
                self.cartridges[cartridge_idx].record_attachment_error(
                    CartridgeAttachmentErrorKind::HandshakeFailed,
                    msg.clone(),
                );
                let _ = child.kill().await;
                return Err(AsyncHostError::Handshake(msg));
            }
        };

        let cap_groups = match parse_cap_groups_from_manifest(&handshake_result.manifest) {
            Ok(groups) => groups,
            Err(parse_err) => {
                let kind = parse_err.attachment_kind();
                let inner = parse_err.into_async_host_error();
                let label = match kind {
                    CartridgeAttachmentErrorKind::ManifestInvalid => "manifest invalid",
                    CartridgeAttachmentErrorKind::Incompatible => "manifest incompatible",
                    _ => "manifest rejected",
                };
                let msg = format!(
                    "Cartridge '{}' {}: {}",
                    self.cartridges[cartridge_idx].path.display(),
                    label,
                    inner
                );
                self.cartridges[cartridge_idx].record_attachment_error(kind, msg.clone());
                let _ = child.kill().await;
                return Err(inner);
            }
        };

        // Verify identity — proves the protocol stack works end-to-end.
        //
        // Bounded by a hard timeout so a cartridge that handshakes
        // successfully but then fails to respond to the identity
        // probe (because its IdentityOp dispatch is broken, its
        // frame writer wedged, or it crashed mid-flight) is
        // diagnosed and quarantined immediately instead of holding
        // up `spawn_cartridge` indefinitely. Without this,
        // `spawn_cartridge.await` from the cap-dispatch path would
        // never return, the entire host event loop would stall on
        // that one REQ, and every subsequent REQ — even to other
        // cartridges — would queue forever.
        const PER_CARTRIDGE_IDENTITY_TIMEOUT: std::time::Duration =
            std::time::Duration::from_secs(15);
        let id_started_at = std::time::Instant::now();
        let id_outcome = tokio::time::timeout(
            PER_CARTRIDGE_IDENTITY_TIMEOUT,
            verify_identity(&mut reader, &mut writer),
        )
        .await;
        match id_outcome {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                let msg = format!(
                    "Cartridge '{}' identity verification failed: {} — permanently removed",
                    self.cartridges[cartridge_idx].path.display(),
                    e
                );
                tracing::error!(
                    target: "host_runtime",
                    error = %msg,
                    "[CartridgeHostRuntime] cartridge identity verification failed"
                );
                self.cartridges[cartridge_idx].record_attachment_error(
                    CartridgeAttachmentErrorKind::IdentityRejected,
                    msg.clone(),
                );
                let _ = child.kill().await;
                return Err(AsyncHostError::Protocol(msg));
            }
            Err(_) => {
                let msg = format!(
                    "Cartridge '{}' identity verification timed out after {:?} — cartridge handshaked but did not respond to the identity REQ. Permanently quarantining.",
                    self.cartridges[cartridge_idx].path.display(),
                    PER_CARTRIDGE_IDENTITY_TIMEOUT
                );
                tracing::error!(
                    target: "host_runtime",
                    error = %msg,
                    "[CartridgeHostRuntime] cartridge identity verification TIMED OUT"
                );
                self.cartridges[cartridge_idx].record_attachment_error(
                    CartridgeAttachmentErrorKind::IdentityRejected,
                    msg.clone(),
                );
                let _ = child.kill().await;
                return Err(AsyncHostError::Protocol(msg));
            }
        }

        // Start writer task
        let (writer_tx, writer_rx) = mpsc::unbounded_channel::<Frame>();
        let wh = Self::start_writer_task(writer, writer_rx);

        let generation = self.cartridges[cartridge_idx]
            .generation
            .checked_add(1)
            .expect("cartridge process generation overflow");

        // Start reader task. Every event is stamped with this process
        // generation so an old process cannot tear down a later respawn.
        let rh = Self::start_reader_task(cartridge_idx, generation, reader, self.event_tx.clone());

        // Update cartridge state
        let cartridge = &mut self.cartridges[cartridge_idx];
        cartridge.manifest = handshake_result.manifest;
        cartridge.limits = handshake_result.limits;
        cartridge.pool_states = handshake_result.pool_states.clone();
        cartridge.cap_groups = cap_groups;
        cartridge.running = true;
        cartridge.generation = generation;
        cartridge.process = Some(child);
        cartridge.writer_tx = Some(writer_tx);
        cartridge.reader_handle = Some(rh);
        cartridge.writer_handle = Some(wh);
        cartridge.stderr_handle = stderr;
        cartridge.last_death_message = None; // Clear any previous death message

        // Capture observer payload while we still have an exclusive borrow.
        let observer_pid = cartridge.process.as_ref().and_then(|c| c.id());
        let observer_name = cartridge
            .path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        let observer_caps: Vec<String> = cartridge
            .cap_groups
            .iter()
            .flat_map(|g| g.caps.iter())
            .map(|c| c.urn.to_string())
            .collect();

        self.update_cap_table();
        self.update_process_snapshot();

        // Notify lifecycle observer (e.g., XPC reverse-callback bridge).
        if let Some(ref obs) = self.observer {
            obs.cartridge_spawned(cartridge_idx, observer_pid, &observer_name, &observer_caps);
        }

        Ok(())
    }

    /// Update the shared process snapshot with current cartridge state.
    /// Called after every spawn and death event.
    fn update_process_snapshot(&self) {
        let mut snap = self.process_snapshot.write().unwrap();
        snap.clear();
        for (idx, cartridge) in self.cartridges.iter().enumerate() {
            if let Some(ref child) = cartridge.process {
                if let Some(pid) = child.id() {
                    snap.push(CartridgeProcessInfo {
                        cartridge_index: idx,
                        pid,
                        name: cartridge
                            .path
                            .file_name()
                            .unwrap_or_default()
                            .to_string_lossy()
                            .into_owned(),
                        running: cartridge.running,
                        caps: cartridge
                            .cap_groups
                            .iter()
                            .flat_map(|g| g.caps.iter())
                            .map(|c| c.urn.to_string())
                            .collect(),
                        memory_footprint_mb: cartridge.memory_footprint_mb,
                        memory_rss_mb: cartridge.memory_rss_mb,
                    });
                }
            }
        }
    }

    /// Send a frame to a specific cartridge's stdin.
    fn send_to_cartridge(&self, cartridge_idx: usize, frame: Frame) -> Result<(), AsyncHostError> {
        let cartridge = &self.cartridges[cartridge_idx];
        let writer_tx = cartridge.writer_tx.as_ref().ok_or_else(|| {
            AsyncHostError::Protocol(format!(
                "Cartridge {} not running — no writer channel",
                cartridge_idx
            ))
        })?;
        writer_tx.send(frame).map_err(|_| AsyncHostError::SendError)
    }

    /// Find which cartridge handles a given cap URN.
    ///
    /// Uses `is_dispatchable(candidate, request)` to find cartridges that can
    /// legally handle the request, then ranks by specificity.
    ///
    /// Ranking prefers:
    /// 1. Equivalent matches (distance 0)
    /// 2. More specific candidates (positive distance) - refinements
    /// 3. More generic candidates (negative distance) - fallbacks
    fn find_cartridge_for_cap(&self, cap_urn: &str) -> Option<usize> {
        let request_urn = match crate::CapUrn::from_string(cap_urn) {
            Ok(u) => u,
            Err(_) => return None,
        };

        let request_specificity = request_urn.specificity();

        // Collect ALL dispatchable cartridges with their specificity scores
        let mut matches: Vec<(usize, isize)> = Vec::new(); // (cartridge_idx, signed_distance)

        for (registered_cap, cartridge_idx) in &self.cap_table {
            if let Ok(registered_urn) = crate::CapUrn::from_string(registered_cap) {
                // Use is_dispatchable: can this candidate handle this request?
                if registered_urn.is_dispatchable(&request_urn) {
                    let specificity = registered_urn.specificity();
                    let signed_distance = specificity as isize - request_specificity as isize;
                    matches.push((*cartridge_idx, signed_distance));
                }
            }
        }

        if matches.is_empty() {
            return None;
        }

        // Ranking: prefer equivalent (0), then more specific (+), then more generic (-)
        matches.sort_by(|a, b| {
            let (_, dist_a) = a;
            let (_, dist_b) = b;

            // First: non-negative distances before negative
            match (dist_a >= &0, dist_b >= &0) {
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                _ => {
                    // Same sign: prefer smaller absolute distance
                    dist_a.unsigned_abs().cmp(&dist_b.unsigned_abs())
                }
            }
        });

        matches.first().map(|(idx, _)| *idx)
    }

    // =========================================================================
    // HEARTBEAT HEALTH MONITORING
    // =========================================================================

    /// Send heartbeat probes to all running cartridges and fully retire any
    /// process whose previous probe expired. Teardown completes before the
    /// cartridge becomes eligible for on-demand respawn.
    async fn send_heartbeats_and_check_timeouts(
        &mut self,
        outbound_tx: &mpsc::UnboundedSender<Frame>,
    ) -> Result<(), AsyncHostError> {
        let now = Instant::now();

        let timed_out: Vec<(usize, u64)> = self
            .cartridges
            .iter()
            .enumerate()
            .filter(|(_, cartridge)| {
                cartridge.running
                    && cartridge
                        .pending_heartbeats
                        .values()
                        .any(|sent| now.duration_since(*sent) > HEARTBEAT_TIMEOUT)
            })
            .map(|(idx, cartridge)| (idx, cartridge.generation))
            .collect();

        for (cartridge_idx, generation) in timed_out {
            self.cartridges[cartridge_idx].shutdown_reason = Some(ShutdownReason::HeartbeatTimeout);
            self.handle_cartridge_death(cartridge_idx, generation, outbound_tx)
                .await?;
        }

        for cartridge in self.cartridges.iter_mut().filter(|c| c.running) {
            // Send a new heartbeat probe. Pending operator capacities ride
            // it (the heartbeat IS the capacity config channel) and are
            // cleared once carried — the reply's mandatory pool map is the
            // application's confirmation.
            if let Some(ref writer_tx) = cartridge.writer_tx {
                let hb_id = MessageId::new_uuid();
                let hb = if cartridge.pending_desired.is_empty() {
                    Frame::heartbeat(hb_id.clone())
                } else {
                    let frame =
                        Frame::heartbeat_with_desired(hb_id.clone(), &cartridge.pending_desired);
                    cartridge.pending_desired.clear();
                    frame
                };
                if writer_tx.send(hb).is_ok() {
                    cartridge.pending_heartbeats.insert(hb_id, now);
                }
            }
        }

        // Rebuild after potential cap changes
        self.update_cap_table();
        self.rebuild_capabilities(Some(outbound_tx)); // Send RelayNotify to relay
        Ok(())
    }

    /// Deliver the operator's desired `configured` values to one hosted
    /// cartridge — the heartbeat is the config channel, and the host owns
    /// its timing, so the values ride an IMMEDIATE out-of-cycle probe
    /// rather than waiting for the interval. Validated hard against the
    /// cartridge's last-known pool map: an unknown cartridge or pool name
    /// is refused with the offender named, and nothing is queued.
    pub fn apply_desired_capacities(
        &mut self,
        cartridge_id: &str,
        desired: &crate::bifaci::pools::DesiredCapacities,
    ) -> Result<(), AsyncHostError> {
        let idx = {
            let mut matches = self
                .cartridges
                .iter()
                .enumerate()
                .filter(|(_, c)| {
                    !c.removed
                        && c.installed_identity
                            .as_ref()
                            .map(|record| record.id == cartridge_id)
                            .unwrap_or(false)
                });
            let (idx, cartridge) = matches.next().ok_or_else(|| {
                AsyncHostError::Protocol(format!(
                    "desired capacities address cartridge '{cartridge_id}', which this host does not carry"
                ))
            })?;
            if matches.next().is_some() {
                return Err(AsyncHostError::Protocol(format!(
                    "cartridge id '{cartridge_id}' is ambiguous on this host"
                )));
            }
            for pool in desired.keys() {
                if !cartridge.pool_states.contains_key(pool) {
                    return Err(AsyncHostError::Protocol(format!(
                        "desired capacities name pool '{pool}', which cartridge '{cartridge_id}' does not declare"
                    )));
                }
            }
            idx
        };
        let cartridge = &mut self.cartridges[idx];
        for (pool, configured) in desired {
            cartridge.pending_desired.insert(pool.clone(), *configured);
        }
        // Immediate out-of-cycle probe when the process is up; a cold
        // cartridge keeps the values queued for the attach-time probe.
        if cartridge.running {
            if let Some(ref writer_tx) = cartridge.writer_tx {
                let hb_id = MessageId::new_uuid();
                let frame = Frame::heartbeat_with_desired(hb_id.clone(), &cartridge.pending_desired);
                cartridge.pending_desired.clear();
                if writer_tx.send(frame).is_ok() {
                    cartridge.pending_heartbeats.insert(hb_id, Instant::now());
                }
            }
        }
        Ok(())
    }

    // =========================================================================
    // INTERNAL HELPERS
    // =========================================================================

    /// Rebuild the cap_table from all cartridges (running or registered).
    /// Failed cartridges contribute no caps. The flat URN view comes
    /// from each cartridge's `cap_urns()` over its `cap_groups` (the
    /// single source of truth — populated at registration time so the
    /// table is correct even before the cartridge has been spawned).
    fn update_cap_table(&mut self) {
        self.cap_table.clear();
        for (idx, cartridge) in self.cartridges.iter().enumerate() {
            if cartridge.hello_failed {
                continue; // Permanently removed
            }
            for cap_urn in cartridge.cap_urns() {
                self.cap_table.push((cap_urn, idx));
            }
        }
    }

    /// Build the `installed_cartridges` list for a RelayNotify payload,
    /// injecting live runtime stats derived from the routing tables and
    /// cartridge process state. One source of truth — the engine sees what
    /// the host sees with no time skew beyond the send itself.
    fn build_installed_cartridge_identities(&self) -> Vec<InstalledCartridgeRecord> {
        // Count active incoming requests per cartridge index.
        let mut active_counts: HashMap<usize, u64> = HashMap::new();
        for &idx in self.incoming_rxids.values() {
            *active_counts.entry(idx).or_insert(0) += 1;
        }
        // Count outgoing peer requests per cartridge index.
        let mut peer_counts: HashMap<usize, u64> = HashMap::new();
        for &idx in self.outgoing_rids.values() {
            *peer_counts.entry(idx).or_insert(0) += 1;
        }

        let mut result: Vec<InstalledCartridgeRecord> = self
            .cartridges
            .iter()
            .enumerate()
            .filter_map(|(_idx, cartridge)| {
                // Retired installs are gone from the inventory entirely —
                // retirement is not a failure, there is nothing to report.
                if cartridge.removed {
                    return None;
                }
                let base = cartridge.installed_cartridge_record()?;
                let pid = cartridge.process.as_ref().and_then(|c| c.id());
                let stats = CartridgeRuntimeStats {
                    running: cartridge.running,
                    pools: cartridge.pool_states.clone(),
                    pid,
                    active_request_count: *active_counts.get(&_idx).unwrap_or(&0),
                    peer_request_count: *peer_counts.get(&_idx).unwrap_or(&0),
                    memory_footprint_mb: cartridge.memory_footprint_mb,
                    memory_rss_mb: cartridge.memory_rss_mb,
                    last_heartbeat_unix_seconds: cartridge.last_heartbeat_unix_seconds,
                    restart_count: cartridge.restart_count,
                    protocol_drops_total: cartridge.protocol_drops_total,
                    protocol_stragglers_total: cartridge.protocol_stragglers_total,
                    protocol_overruns_total: cartridge.protocol_overruns_total,
                };
                // A cartridge whose HELLO failed (e.g. a pre-v4 binary hard-
                // rejected by the version check) stays IN the inventory with
                // an attachment error — never silently absent. It carries no
                // cap_groups, so it is never routable.
                if cartridge.hello_failed {
                    return Some(InstalledCartridgeRecord {
                        runtime_stats: Some(stats),
                        cap_groups: Vec::new(),
                        attachment_error: Some(CartridgeAttachmentError {
                            kind: CartridgeAttachmentErrorKind::HandshakeFailed,
                            message: cartridge
                                .last_death_message
                                .clone()
                                .unwrap_or_else(|| {
                                    "HELLO handshake failed (protocol version mismatch or malformed manifest) — rebuild the cartridge against the current protocol"
                                        .to_string()
                                }),
                            detected_at_unix_seconds: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_secs() as i64)
                                .unwrap_or(0),
                        }),
                        ..base
                    });
                }
                Some(InstalledCartridgeRecord {
                    runtime_stats: Some(stats),
                    cap_groups: cartridge.cap_groups.clone(),
                    ..base
                })
            })
            .collect();
        // Discovery outcomes the host doesn't manage (incompatible installs)
        // ride every advertisement so no republish can erase them.
        result.extend(self.static_inventory_records.iter().cloned());
        result
    }

    /// Rebuild the aggregate capabilities from all running, healthy cartridges.
    ///
    /// If outbound_tx is Some (i.e., running in relay mode), sends a RelayNotify
    /// frame with the updated capabilities. This allows RelaySwitch/RelayMaster
    /// to track capability changes dynamically as cartridges connect/disconnect/fail.
    fn rebuild_capabilities(&mut self, outbound_tx: Option<&mpsc::UnboundedSender<Frame>>) {
        // The relay payload is the single source of truth. Identity
        // gates advertisement: cartridges without a resolvable
        // installed-cartridge record are not part of the inventory
        // the engine can route to. Cartridges with `hello_failed`
        // are also filtered out (they have a record but cannot
        // service requests). Both filters live inside
        // `build_installed_cartridge_identities`.
        if let Some(tx) = outbound_tx {
            let installed_cartridges = self.build_installed_cartridge_identities();
            let notify_payload = RelayNotifyCapabilitiesPayload::new(installed_cartridges)
                .with_host_protocol_stats(self.protocol_stats());
            let notify_bytes = serde_json::to_vec(&notify_payload)
                .expect("Failed to serialize RelayNotify capabilities payload");
            // Advertise the host's REAL aggregate limits — the element-wise
            // minimum over every running cartridge's negotiated handshake
            // limits. The switch overwrites the master's limits on each
            // RelayNotify, so sending defaults here would clobber genuine
            // negotiations (and misreport initial_credit end-to-end).
            let notify_frame = Frame::relay_notify(&notify_bytes, &self.aggregate_limits());
            let _ = tx.send(notify_frame); // Ignore error if relay closed
        }
    }

    /// Element-wise minimum over the negotiated limits of every running
    /// cartridge; defaults when none are running. This is what the host is
    /// actually able to honor across its fleet.
    fn aggregate_limits(&self) -> Limits {
        let mut limits = Limits::default();
        for cartridge in self.cartridges.iter().filter(|c| c.running) {
            limits.max_frame = limits.max_frame.min(cartridge.limits.max_frame);
            limits.max_chunk = limits.max_chunk.min(cartridge.limits.max_chunk);
            limits.max_reorder_buffer = limits
                .max_reorder_buffer
                .min(cartridge.limits.max_reorder_buffer);
            limits.initial_credit = limits.initial_credit.min(cartridge.limits.initial_credit);
        }
        limits
    }

    /// Kill all managed cartridge processes.
    ///
    /// Order matters: drop writer_tx first (closes the channel), then AWAIT the
    /// writer handle (so it exits naturally and drops the write stream, which
    /// causes the cartridge to see EOF). Only then abort the reader handle.
    /// Aborting the writer instead of awaiting it can leave the write stream
    /// open in a single-threaded runtime, deadlocking any sync thread that
    /// blocks on the cartridge's read().
    async fn kill_all_cartridges(&mut self) {
        // Collect death notifications under exclusive borrow; fire callbacks
        // afterward to avoid borrow conflicts and to keep the observer call
        // outside the kill path.
        let mut death_notifications: Vec<(usize, Option<u32>, String)> = Vec::new();

        for (idx, cartridge) in self.cartridges.iter_mut().enumerate() {
            let was_running = cartridge.running;
            let pid_at_death = cartridge.process.as_ref().and_then(|c| c.id());
            let name = cartridge
                .path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned();

            cartridge.shutdown_reason = Some(ShutdownReason::AppExit);
            if let Some(ref mut child) = cartridge.process {
                let _ = child.kill().await;
            }
            cartridge.process = None;
            cartridge.running = false;

            // Close the channel → writer task's rx.recv() returns None → task exits
            cartridge.writer_tx = None;

            // AWAIT (not abort) the writer handle so it drops the write stream cleanly.
            if let Some(handle) = cartridge.writer_handle.take() {
                let _ = handle.await;
            }

            // Now the write stream is closed → cartridge sees EOF.
            // Safe to abort the reader (it will exit on its own anyway).
            if let Some(handle) = cartridge.reader_handle.take() {
                handle.abort();
            }

            if was_running {
                death_notifications.push((idx, pid_at_death, name));
            }
        }

        // Notify lifecycle observer for each cartridge that was running.
        if let Some(ref obs) = self.observer {
            for (idx, pid, name) in &death_notifications {
                obs.cartridge_died(*idx, *pid, name);
            }
        }
    }

    /// Spawn a writer task that reads frames from a channel and writes to a cartridge's stdin.
    fn start_writer_task<W: AsyncWrite + Unpin + Send + 'static>(
        mut writer: FrameWriter<W>,
        mut rx: mpsc::UnboundedReceiver<Frame>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut seq_assigner = SeqAssigner::new();
            while let Some(mut frame) = rx.recv().await {
                seq_assigner.assign(&mut frame);
                if let Err(_) = writer.write(&frame).await {
                    break;
                }
                if matches!(frame.frame_type, FrameType::End | FrameType::Err) {
                    seq_assigner.remove(&FlowKey::from_frame(&frame));
                }
            }
        })
    }

    /// Spawn a reader task that reads frames from a cartridge's stdout and sends events.
    fn start_reader_task<R: AsyncRead + Unpin + Send + 'static>(
        cartridge_idx: usize,
        generation: u64,
        mut reader: FrameReader<R>,
        event_tx: mpsc::UnboundedSender<CartridgeEvent>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                match reader.read().await {
                    Ok(Some(frame)) => {
                        if event_tx
                            .send(CartridgeEvent::Frame {
                                cartridge_idx,
                                generation,
                                frame,
                            })
                            .is_err()
                        {
                            break; // Runtime dropped
                        }
                    }
                    Ok(None) => {
                        // EOF — cartridge closed stdout
                        let _ = event_tx.send(CartridgeEvent::Death {
                            cartridge_idx,
                            generation,
                        });
                        break;
                    }
                    Err(_) => {
                        // Read error — treat as death
                        let _ = event_tx.send(CartridgeEvent::Death {
                            cartridge_idx,
                            generation,
                        });
                        break;
                    }
                }
            }
        })
    }

    /// Outbound writer loop: reads frames from channel, writes to relay.
    /// Frames arrive with seq already assigned by CartridgeRuntime — no modification needed.
    async fn outbound_writer_loop<W: AsyncWrite + Unpin>(
        relay_write: W,
        mut rx: mpsc::UnboundedReceiver<Frame>,
    ) {
        let mut writer = FrameWriter::new(relay_write);
        while let Some(frame) = rx.recv().await {
            if writer.write(&frame).await.is_err() {
                break;
            }
        }
    }
}

impl Drop for CartridgeHostRuntime {
    fn drop(&mut self) {
        // Drop cannot be async, so we close channels (triggering writer exit)
        // and abort reader tasks. Writer tasks exit naturally when writer_tx
        // is dropped (channel closes → rx.recv() returns None → task exits
        // → OwnedWriteHalf dropped → cartridge sees EOF).
        // Child processes with kill_on_drop will be killed when Child is dropped.
        for cartridge in &mut self.cartridges {
            cartridge.writer_tx = None; // Close channel → writer task exits naturally
            if let Some(handle) = cartridge.reader_handle.take() {
                handle.abort();
            }
            // Don't abort writer — let it exit naturally so the stream closes cleanly.
        }
    }
}

// =============================================================================
// HELPERS
// =============================================================================

/// Reason a manifest was rejected by `parse_cap_groups_from_manifest`. Carries
/// the specific failure mode so the caller can pick the right
/// `CartridgeAttachmentErrorKind` — `ManifestInvalid` when the JSON itself
/// is malformed, `Incompatible` when the JSON parses but violates the
/// cartridge schema (missing CAP_IDENTITY, old shape, etc.).
#[derive(Debug)]
enum ParseCapsError {
    /// JSON failed to parse or did not deserialize into `CapManifest`.
    InvalidJson(AsyncHostError),
    /// JSON parsed but the manifest is structurally incompatible with
    /// the host's expectations (e.g. missing CAP_IDENTITY).
    Incompatible(AsyncHostError),
}

impl std::fmt::Display for ParseCapsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseCapsError::InvalidJson(e) | ParseCapsError::Incompatible(e) => write!(f, "{}", e),
        }
    }
}

impl ParseCapsError {
    fn into_async_host_error(self) -> AsyncHostError {
        match self {
            ParseCapsError::InvalidJson(e) | ParseCapsError::Incompatible(e) => e,
        }
    }

    fn attachment_kind(&self) -> CartridgeAttachmentErrorKind {
        match self {
            ParseCapsError::InvalidJson(_) => CartridgeAttachmentErrorKind::ManifestInvalid,
            ParseCapsError::Incompatible(_) => CartridgeAttachmentErrorKind::Incompatible,
        }
    }
}

fn parse_cap_groups_from_manifest(
    manifest: &[u8],
) -> Result<Vec<crate::bifaci::manifest::CapGroup>, ParseCapsError> {
    use crate::standard::caps::CAP_IDENTITY;
    use crate::urn::cap_urn::CapUrn;
    use crate::CapManifest;

    let manifest_obj: CapManifest = serde_json::from_slice(manifest).map_err(|e| {
        ParseCapsError::InvalidJson(AsyncHostError::Protocol(format!(
            "Invalid CapManifest from cartridge: {}",
            e
        )))
    })?;

    let identity_urn =
        CapUrn::from_string(CAP_IDENTITY).expect("BUG: CAP_IDENTITY constant is invalid");
    let has_identity = manifest_obj
        .all_caps()
        .iter()
        .any(|cap| identity_urn.conforms_to(&cap.urn));
    if !has_identity {
        return Err(ParseCapsError::Incompatible(AsyncHostError::Protocol(
            format!(
                "Cartridge manifest missing required CAP_IDENTITY ({})",
                CAP_IDENTITY
            ),
        )));
    }

    Ok(manifest_obj.cap_groups)
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bifaci::local_socket::UnixStream;
    use crate::standard::caps::CAP_IDENTITY;
    use crate::CapUrn;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{BufReader, BufWriter};

    /// Build a single synthetic CapGroup whose `caps` list mirrors the
    /// given flat cap-URN slice. Tests use this to satisfy the
    /// `cap_groups`-shaped registration API without restating the
    /// structural hierarchy each time. The cartridge's flat URN view
    /// is derived from the returned groups via `cap_urns()`.
    fn cap_groups_from_urns(urns: &[&str]) -> Vec<crate::bifaci::manifest::CapGroup> {
        use crate::cap::definition::Cap as CapDefinition;
        let caps: Vec<CapDefinition> = urns
            .iter()
            .map(|u| {
                let parsed = CapUrn::from_string(u)
                    .unwrap_or_else(|e| panic!("invalid cap URN in test fixture '{}': {}", u, e));
                CapDefinition::new(parsed, "test".to_string(), vec!["test".to_string()])
            })
            .collect();
        vec![crate::bifaci::manifest::CapGroup {
            name: "test".to_string(),
            caps,
            adapter_urns: Vec::new(),
        }]
    }

    /// Flatten the host's identity-filtered installed-cartridge inventory
    /// into the union of cap-URN strings it advertises. Tests use this to
    /// reason about advertised caps without re-implementing the
    /// `installed_cartridges[*].cap_groups[*].caps[*].urn` walk.
    fn host_advertised_cap_urns(runtime: &CartridgeHostRuntime) -> Vec<String> {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut out: Vec<String> = Vec::new();
        for ic in runtime.aggregate_installed_cartridges() {
            for group in &ic.cap_groups {
                for cap in &group.caps {
                    let urn = cap.urn.to_string();
                    if seen.insert(urn.clone()) {
                        out.push(urn);
                    }
                }
            }
        }
        out
    }

    /// Records spawn/death counts for `CartridgeHostObserver` contract
    /// tests. Mirrors `RecordingObserver` in the Swift Bifaci tests.
    struct RecordingObserver {
        spawn_count: AtomicUsize,
        death_count: AtomicUsize,
    }

    impl RecordingObserver {
        fn new() -> Self {
            Self {
                spawn_count: AtomicUsize::new(0),
                death_count: AtomicUsize::new(0),
            }
        }
        fn spawns(&self) -> usize {
            self.spawn_count.load(Ordering::Acquire)
        }
        fn deaths(&self) -> usize {
            self.death_count.load(Ordering::Acquire)
        }
    }

    impl CartridgeHostObserver for RecordingObserver {
        fn cartridge_spawned(
            &self,
            _cartridge_index: usize,
            _pid: Option<u32>,
            _name: &str,
            _caps: &[String],
        ) {
            self.spawn_count.fetch_add(1, Ordering::AcqRel);
        }
        fn cartridge_died(&self, _cartridge_index: usize, _pid: Option<u32>, _name: &str) {
            self.death_count.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Pins the optional-observer contract: a brand-new runtime with
    /// no observer attached must close cleanly on an empty cartridge
    /// list. A regression here would mean the observer-firing path
    /// became non-optional and broke every call site that doesn't
    /// register an observer (engine in-process runtime, in-process
    /// host tests, integration tests).
    #[tokio::test]
    async fn test990_observer_is_optional() {
        let mut runtime = CartridgeHostRuntime::new();
        // Ensure nothing fires when no observer is set and we
        // immediately tear the runtime down.
        runtime.kill_all_cartridges().await;
    }

    /// Pins the observer-clearing contract: a setObserver(None)
    /// after a previous registration must drop the strong ref so a
    /// subsequent lifecycle moment doesn't fire into a torn-down
    /// bridge. Matches the Swift `setObserver(nil)` test.
    #[tokio::test]
    async fn test989_set_observer_none_clears_previous() {
        let observer = Arc::new(RecordingObserver::new());
        let mut runtime = CartridgeHostRuntime::new();
        runtime.set_observer(Some(observer.clone() as Arc<dyn CartridgeHostObserver>));
        runtime.set_observer(None);
        runtime.kill_all_cartridges().await;
        assert_eq!(
            observer.spawns(),
            0,
            "Observer was cleared via set_observer(None) before any \
             spawn moment, yet recorded {} spawn events — the runtime is \
             firing into a cleared observer slot.",
            observer.spawns()
        );
        assert_eq!(
            observer.deaths(),
            0,
            "Observer was cleared via set_observer(None) before any \
             death moment, yet recorded {} death events — the runtime is \
             firing into a cleared observer slot.",
            observer.deaths()
        );
    }

    /// Helper: perform handshake_accept and handle the identity verification REQ.
    /// Returns (FrameReader, FrameWriter) ready for further communication.
    async fn cartridge_handshake_with_identity<R, W>(
        from_runtime: R,
        to_runtime: W,
        manifest: &[u8],
    ) -> (
        crate::bifaci::io::FrameReader<BufReader<R>>,
        crate::bifaci::io::FrameWriter<BufWriter<W>>,
    )
    where
        R: tokio::io::AsyncRead + Unpin,
        W: tokio::io::AsyncWrite + Unpin,
    {
        use crate::bifaci::io::{handshake_accept, FrameReader, FrameWriter};

        let mut reader = FrameReader::new(BufReader::new(from_runtime));
        let mut writer = FrameWriter::new(BufWriter::new(to_runtime));
        handshake_accept(
            &mut reader,
            &mut writer,
            manifest,
            &crate::bifaci::pools::PoolStates::new(),
        )
        .await
        .unwrap();

        // Handle identity verification REQ
        let req = reader.read().await.unwrap().expect("expected identity REQ");
        assert_eq!(
            req.frame_type,
            FrameType::Req,
            "first frame after handshake must be REQ"
        );

        // Read request body: STREAM_START → CHUNK(s) → STREAM_END → END
        let mut payload = Vec::new();
        loop {
            let f = reader.read().await.unwrap().expect("expected frame");
            match f.frame_type {
                FrameType::StreamStart => {}
                FrameType::Chunk => payload.extend(f.payload.unwrap_or_default()),
                FrameType::StreamEnd => {}
                FrameType::End => break,
                other => panic!(
                    "unexpected frame type during identity verification: {:?}",
                    other
                ),
            }
        }

        // Echo response: STREAM_START → CHUNK → STREAM_END → END
        let stream_id = "identity-echo".to_string();
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

        (reader, writer)
    }

    // TEST6600: parse_cap_groups_from_manifest classifies failures by kind
    //
    // Manifest JSON that parses but lacks CAP_IDENTITY is `Incompatible`
    // (schema-rejected). Manifest bytes that don't parse as CapManifest are
    // `ManifestInvalid` (JSON-level failure). The split lets the host's
    // attachment-error reporter surface the right kind to the UI.
    #[test]
    fn test6600_parse_cap_groups_rejects_manifest_without_identity() {
        // JSON-valid manifest, missing CAP_IDENTITY → Incompatible.
        let manifest = r#"{"name":"Test","version":"1.0","channel":"release","registry_url":null,"description":"Test","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=\"media:void\";convert;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}],"adapter_urns":[]}]}"#;
        let result = parse_cap_groups_from_manifest(manifest.as_bytes());
        let err = result.expect_err("Manifest without CAP_IDENTITY must be rejected");
        assert!(
            matches!(err, ParseCapsError::Incompatible(_)),
            "Missing CAP_IDENTITY must classify as Incompatible, got {:?}",
            err
        );
        assert_eq!(
            err.attachment_kind(),
            CartridgeAttachmentErrorKind::Incompatible,
            "attachment_kind() must agree with the variant"
        );
        assert!(
            format!("{}", err).contains("CAP_IDENTITY"),
            "Error must mention CAP_IDENTITY, got: {}",
            err
        );

        // Garbage bytes that don't deserialize → ManifestInvalid.
        let bad_json = b"{not even json";
        let result_bad = parse_cap_groups_from_manifest(bad_json);
        let err_bad = result_bad.expect_err("Non-JSON manifest must be rejected");
        assert!(
            matches!(err_bad, ParseCapsError::InvalidJson(_)),
            "Non-JSON manifest must classify as InvalidJson, got {:?}",
            err_bad
        );
        assert_eq!(
            err_bad.attachment_kind(),
            CartridgeAttachmentErrorKind::ManifestInvalid,
            "attachment_kind() must agree with the variant"
        );

        // Valid manifest WITH CAP_IDENTITY must succeed.
        let manifest_ok = r#"{"name":"Test","version":"1.0","channel":"release","registry_url":null,"description":"Test","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";convert;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}],"adapter_urns":[]}]}"#;
        let result_ok = parse_cap_groups_from_manifest(manifest_ok.as_bytes());
        let groups = result_ok.expect("Manifest with CAP_IDENTITY must be accepted");
        let total_caps: usize = groups.iter().map(|g| g.caps.len()).sum();
        assert_eq!(total_caps, 2, "Must parse both caps");
    }

    // TEST6601: An attached cartridge (raw-stream, no on-disk anchor) must
    // get a resolvable install identity derived from its HELLO manifest.
    //
    // Advertisement is identity-gated: build_installed_cartridge_identities
    // drops any cartridge whose installed_cartridge_record() is None. If an
    // attached cartridge had no identity it would be silently excluded from
    // every RelayNotify, the host would advertise an empty inventory, and the
    // engine could never route to it — the deadlock that hung the
    // rust-rust-rust interop echo test forever.
    #[test]
    fn test6601_attached_cartridge_identity_derived_from_manifest() {
        let manifest = r#"{"name":"InteropCartridge","version":"2.3.4","channel":"nightly","registry_url":null,"description":"x","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]}],"adapter_urns":[]}]}"#;

        let record = installed_cartridge_record_from_manifest(manifest.as_bytes())
            .expect("attached cartridge must have a resolvable identity from its manifest");

        // Identity tuple comes straight from the manifest.
        assert_eq!(record.id, "InteropCartridge");
        assert_eq!(record.version, "2.3.4");
        assert_eq!(record.registry_url, None, "null registry_url ⇒ dev install");
        assert!(
            matches!(
                record.channel,
                crate::bifaci::cartridge_repo::CartridgeChannel::Nightly
            ),
            "channel must round-trip from the manifest"
        );
        // sha256 is over the manifest bytes — non-empty, deterministic.
        assert_eq!(record.sha256.len(), 64, "sha256 hex must be 64 chars");
        let again = installed_cartridge_record_from_manifest(manifest.as_bytes()).unwrap();
        assert_eq!(
            record.sha256, again.sha256,
            "identity must be deterministic"
        );
        // Attached ⇒ already verified ⇒ operational, no attachment error.
        assert!(record.attachment_error.is_none());
        assert!(matches!(record.lifecycle, CartridgeLifecycle::Operational));

        // A ManagedCartridge built via new_attached with this identity must
        // surface it through installed_cartridge_record() — the gate that
        // build_installed_cartridge_identities consults.
        let cartridge = ManagedCartridge::new_attached(
            manifest.as_bytes().to_vec(),
            Limits::default(),
            crate::bifaci::pools::PoolStates::new(),
            Vec::new(),
            Some(record.clone()),
        );
        assert!(
            cartridge.installed_cartridge_record().is_some(),
            "attached cartridge must not be filtered out of the advertisement"
        );

        // Garbage manifest ⇒ no identity (caller still attaches, but the
        // record is honestly absent rather than fabricated).
        assert!(installed_cartridge_record_from_manifest(b"{not json").is_none());
    }

    // TEST235: Test ResponseChunk stores payload, seq, offset, len, and eof fields correctly
    #[test]
    fn test235_response_chunk() {
        let chunk = ResponseChunk {
            payload: b"hello".to_vec(),
            seq: 0,
            offset: None,
            len: None,
            is_eof: false,
        };
        assert_eq!(chunk.payload, b"hello");
        assert_eq!(chunk.seq, 0);
        assert!(chunk.offset.is_none());
        assert!(!chunk.is_eof);
    }

    // TEST236: Test ResponseChunk with all fields populated preserves offset, len, and eof
    #[test]
    fn test236_response_chunk_with_all_fields() {
        let chunk = ResponseChunk {
            payload: b"data".to_vec(),
            seq: 5,
            offset: Some(1024),
            len: Some(8192),
            is_eof: true,
        };
        assert_eq!(chunk.seq, 5);
        assert_eq!(chunk.offset, Some(1024));
        assert_eq!(chunk.len, Some(8192));
        assert!(chunk.is_eof);
    }

    // TEST237: Test CartridgeResponse::Single final_payload returns the single payload slice
    #[test]
    fn test237_cartridge_response_single() {
        let response = CartridgeResponse::Single(b"result".to_vec());
        assert_eq!(response.final_payload(), Some(b"result".as_slice()));
        assert_eq!(response.concatenated(), b"result");
    }

    // TEST238: Test CartridgeResponse::Single with empty payload returns empty slice and empty vec
    #[test]
    fn test238_cartridge_response_single_empty() {
        let response = CartridgeResponse::Single(vec![]);
        assert_eq!(response.final_payload(), Some(b"".as_slice()));
        assert_eq!(response.concatenated(), b"");
    }

    // TEST239: Test CartridgeResponse::Streaming concatenated joins all chunk payloads in order
    #[test]
    fn test239_cartridge_response_streaming() {
        let chunks = vec![
            ResponseChunk {
                payload: b"hello".to_vec(),
                seq: 0,
                offset: Some(0),
                len: Some(11),
                is_eof: false,
            },
            ResponseChunk {
                payload: b" world".to_vec(),
                seq: 1,
                offset: Some(5),
                len: None,
                is_eof: true,
            },
        ];
        let response = CartridgeResponse::Streaming(chunks);
        assert_eq!(response.concatenated(), b"hello world");
    }

    // TEST240: Test CartridgeResponse::Streaming final_payload returns the last chunk's payload
    #[test]
    fn test240_cartridge_response_streaming_final_payload() {
        let chunks = vec![
            ResponseChunk {
                payload: b"first".to_vec(),
                seq: 0,
                offset: None,
                len: None,
                is_eof: false,
            },
            ResponseChunk {
                payload: b"last".to_vec(),
                seq: 1,
                offset: None,
                len: None,
                is_eof: true,
            },
        ];
        let response = CartridgeResponse::Streaming(chunks);
        assert_eq!(response.final_payload(), Some(b"last".as_slice()));
    }

    // TEST241: Test CartridgeResponse::Streaming with empty chunks vec returns empty concatenation
    #[test]
    fn test241_cartridge_response_streaming_empty_chunks() {
        let response = CartridgeResponse::Streaming(vec![]);
        assert_eq!(response.concatenated(), b"");
        assert!(response.final_payload().is_none());
    }

    // TEST242: Test CartridgeResponse::Streaming concatenated capacity is pre-allocated correctly for large payloads
    #[test]
    fn test242_cartridge_response_streaming_large_payload() {
        let chunk1_data = vec![0xAA; 1000];
        let chunk2_data = vec![0xBB; 2000];
        let chunks = vec![
            ResponseChunk {
                payload: chunk1_data.clone(),
                seq: 0,
                offset: None,
                len: None,
                is_eof: false,
            },
            ResponseChunk {
                payload: chunk2_data.clone(),
                seq: 1,
                offset: None,
                len: None,
                is_eof: true,
            },
        ];
        let response = CartridgeResponse::Streaming(chunks);
        let result = response.concatenated();
        assert_eq!(result.len(), 3000);
        assert_eq!(&result[..1000], &chunk1_data);
        assert_eq!(&result[1000..], &chunk2_data);
    }

    // TEST243: Test AsyncHostError variants display correct error messages
    #[test]
    fn test243_async_host_error_display() {
        let err = AsyncHostError::CartridgeError {
            code: "NOT_FOUND".to_string(),
            message: "Cap not found".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("NOT_FOUND"));
        assert!(msg.contains("Cap not found"));

        assert_eq!(format!("{}", AsyncHostError::Closed), "Host is closed");
        assert_eq!(
            format!("{}", AsyncHostError::ProcessExited),
            "Cartridge process exited unexpectedly"
        );
        assert_eq!(
            format!("{}", AsyncHostError::SendError),
            "Send error: channel closed"
        );
        assert_eq!(
            format!("{}", AsyncHostError::RecvError),
            "Receive error: channel closed"
        );
    }

    // TEST244: Test AsyncHostError::from converts CborError to Cbor variant
    #[test]
    fn test244_async_host_error_from_cbor() {
        let cbor_err = crate::bifaci::io::CborError::InvalidFrame("test".to_string());
        let host_err: AsyncHostError = cbor_err.into();
        match host_err {
            AsyncHostError::Cbor(msg) => assert!(msg.contains("test")),
            _ => panic!("expected Cbor variant"),
        }
    }

    // TEST245: Test AsyncHostError::from converts io::Error to Io variant
    #[test]
    fn test245_async_host_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe broken");
        let host_err: AsyncHostError = io_err.into();
        match host_err {
            AsyncHostError::Io(msg) => assert!(msg.contains("pipe broken")),
            _ => panic!("expected Io variant"),
        }
    }

    // TEST246: Test AsyncHostError Clone implementation produces equal values
    #[test]
    fn test246_async_host_error_clone() {
        let err = AsyncHostError::CartridgeError {
            code: "ERR".to_string(),
            message: "msg".to_string(),
        };
        let cloned = err.clone();
        assert_eq!(format!("{}", err), format!("{}", cloned));
    }

    // TEST247: Test ResponseChunk Clone produces independent copy with same data
    #[test]
    fn test247_response_chunk_clone() {
        let chunk = ResponseChunk {
            payload: b"data".to_vec(),
            seq: 3,
            offset: Some(100),
            len: Some(500),
            is_eof: true,
        };
        let cloned = chunk.clone();
        assert_eq!(chunk.payload, cloned.payload);
        assert_eq!(chunk.seq, cloned.seq);
        assert_eq!(chunk.offset, cloned.offset);
        assert_eq!(chunk.len, cloned.len);
        assert_eq!(chunk.is_eof, cloned.is_eof);
    }

    // TEST119: CartridgeResponse::Streaming concatenated() and final_payload() diverge for
    // multi-chunk responses: concatenated returns all chunk data joined; final_payload returns
    // only the last chunk. A consumer that confuses the two will silently drop all but the
    // last chunk of a multi-chunk response.
    #[test]
    fn test119_cartridge_response_concatenated_and_final_payload_diverge_for_multi_chunk() {
        let chunks = vec![
            ResponseChunk {
                payload: b"AAAA".to_vec(),
                seq: 0,
                offset: None,
                len: None,
                is_eof: false,
            },
            ResponseChunk {
                payload: b"BBBB".to_vec(),
                seq: 1,
                offset: None,
                len: None,
                is_eof: false,
            },
            ResponseChunk {
                payload: b"CCCC".to_vec(),
                seq: 2,
                offset: None,
                len: None,
                is_eof: true,
            },
        ];
        let response = CartridgeResponse::Streaming(chunks);

        assert_eq!(response.concatenated(), b"AAAABBBBCCCC");
        assert_eq!(response.final_payload(), Some(b"CCCC".as_ref()));
        assert_ne!(
            response.concatenated(),
            response.final_payload().unwrap_or_default(),
            "concatenated and final_payload must diverge for multi-chunk responses"
        );
    }

    // TEST413: Register cartridge adds entries to cap_table.
    //
    // The cap_table stores canonical URN strings (alphabetical tag order,
    // no unnecessary quotes around single-tag media URNs). The input
    // forms below get canonicalized at parse-time and the table reads
    // back as the canonical form.
    #[test]
    fn test413_register_cartridge_adds_to_cap_table() {
        let mut runtime = CartridgeHostRuntime::new();
        runtime.register_cartridge(
            Path::new("/usr/bin/test-cartridge"),
            "test-cartridge",
            "0.0.0",
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            &cap_groups_from_urns(&[
                "cap:convert;in=media:void;out=media:void",
                "cap:analyze;in=media:void;out=media:void",
            ]),
        );

        assert_eq!(runtime.cap_table.len(), 2);
        assert_eq!(
            runtime.cap_table[0].0,
            "cap:convert;in=media:void;out=media:void"
        );
        assert_eq!(runtime.cap_table[0].1, 0);
        assert_eq!(
            runtime.cap_table[1].0,
            "cap:analyze;in=media:void;out=media:void"
        );
        assert_eq!(runtime.cap_table[1].1, 0);
        assert_eq!(runtime.cartridges.len(), 1);
        assert!(!runtime.cartridges[0].running);
    }

    // TEST6594: aggregate_installed_cartridges() is empty before any
    // cartridge with a resolvable identity is attached. A binary path
    // that does not exist on disk has no identity (`installed_identity`
    // is `None`) and therefore does not appear in the relay payload.
    #[test]
    fn test6594_capabilities_empty_initially() {
        let runtime = CartridgeHostRuntime::new();
        assert!(
            runtime.aggregate_installed_cartridges().is_empty(),
            "No cartridges registered = empty inventory"
        );

        let mut runtime2 = CartridgeHostRuntime::new();
        runtime2.register_cartridge(
            Path::new("/nonexistent/path/to/cartridge"),
            "test",
            "0.0.0",
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            &cap_groups_from_urns(&["cap:in=\"media:void\";test;out=\"media:void\""]),
        );
        // Binary path does not exist on disk so sha256 hashing fails
        // and installed_identity is None — the cartridge is not advertised.
        assert!(
            runtime2.aggregate_installed_cartridges().is_empty(),
            "Registered cartridge with unhashable binary is not advertised"
        );
    }

    // TEST415: REQ for known cap triggers spawn attempt (verified by expected spawn error for non-existent binary)
    #[tokio::test]
    async fn test415_req_for_known_cap_triggers_spawn() {
        // Production install layout: a versioned cartridge directory
        // containing cartridge.json (which carries the channel) plus an
        // entry-point binary. Point at a non-executable file so spawn
        // fails — that exercises the "REQ → spawn attempt → spawn
        // failure" path on a cartridge with a real installed identity.
        let cartridge_dir = tempfile::tempdir().unwrap();
        std::fs::write(
            cartridge_dir.path().join("cartridge.json"),
            r#"{"name":"test","version":"0.0.1","channel":"release","registry_url":null,"entry":"bin","installed_at":"2026-01-01T00:00:00Z","installed_from":"dev"}"#,
        )
        .unwrap();
        let entry_point = cartridge_dir.path().join("bin");
        std::fs::write(&entry_point, b"not an executable").unwrap();

        let mut runtime = CartridgeHostRuntime::new();
        runtime.register_cartridge_dir(
            &entry_point,
            cartridge_dir.path(),
            "test",
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            "0.0.1",
            &cap_groups_from_urns(&["cap:in=\"media:void\";test;out=\"media:void\""]),
        );

        // Create relay pipe pair
        let (runtime_read, engine_write_stream) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();
        let (_engine_read, runtime_write) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();

        let (runtime_read_half, _) = runtime_read.into_split();
        let (_, runtime_write_half) = runtime_write.into_split();
        let (_, engine_write_half) = engine_write_stream.into_split();

        // Send a REQ through the relay (must have XID since it's from relay)
        let send_handle = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let mut writer = FrameWriter::new(engine_write_half);
            let mut req = Frame::req(
                MessageId::new_uuid(),
                "cap:in=\"media:void\";test;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req.routing_id = Some(MessageId::Uint(1)); // XID from RelaySwitch
            seq.assign(&mut req);
            writer.write(&req).await.unwrap();
            seq.remove(&FlowKey::from_frame(&req));
        });

        // Run the runtime — should attempt to spawn, fail (entry-point
        // file exists but isn't executable)
        let result = runtime
            .run(runtime_read_half, runtime_write_half, || vec![])
            .await;

        assert!(
            result.is_err(),
            "Should fail because entry point is not executable"
        );
        let err = result.unwrap_err();
        let err_str = format!("{}", err);
        assert!(
            err_str.to_lowercase().contains("spawn")
                || err_str.contains("permission")
                || err_str.contains("Exec"),
            "Error should mention spawn failure, got: {}",
            err_str
        );

        send_handle.await.unwrap();
    }

    // TEST416: Attach cartridge performs HELLO handshake, extracts manifest, updates capabilities
    #[tokio::test]
    async fn test416_attach_cartridge_handshake_updates_capabilities() {
        let manifest = r#"{"name":"Test","version":"1.0","channel":"release","registry_url":null,"description":"Test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Test","aliases": ["test"],"args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair (tokio sockets)
        let (cartridge_to_runtime, runtime_from_cartridge) = UnixStream::pair().unwrap();
        let (runtime_to_cartridge, cartridge_from_runtime) = UnixStream::pair().unwrap();

        let (cartridge_read, _) = runtime_from_cartridge.into_split();
        let (_, cartridge_write) = runtime_to_cartridge.into_split();

        // Cartridge task does handshake + identity verification
        let manifest_bytes = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            cartridge_handshake_with_identity(
                cartridge_from_runtime,
                cartridge_to_runtime,
                &manifest_bytes,
            )
            .await;
        });

        let mut runtime = CartridgeHostRuntime::new();
        let idx = runtime
            .attach_cartridge(cartridge_read, cartridge_write)
            .await
            .unwrap();

        assert_eq!(idx, 0);
        assert!(runtime.cartridges[0].running);
        // Verify cartridge has identity cap via semantic comparison (not string comparison)
        let identity_urn = crate::CapUrn::from_string(CAP_IDENTITY).unwrap();
        assert!(
            runtime.cartridges[0]
                .cap_groups
                .iter()
                .flat_map(|g| g.caps.iter())
                .any(|c| identity_urn.conforms_to(&c.urn)),
            "Cartridge must have identity cap"
        );
        // Cap routing table is populated from the cartridge's
        // cap_groups regardless of identity. The identity-filtered
        // inventory is a separate concern — it gates *advertisement*
        // to the engine, not in-process routing.
        assert!(
            runtime.cap_table.iter().any(|(urn, _)| {
                crate::CapUrn::from_string(urn)
                    .map(|u| identity_urn.conforms_to(&u))
                    .unwrap_or(false)
            }),
            "Cap table must include the identity cap for routing"
        );

        cartridge_handle.await.unwrap();
    }

    // TEST417: Route REQ to correct cartridge by cap_urn (with two attached cartridges)
    #[tokio::test]
    async fn test417_route_req_to_correct_cartridge() {
        let manifest_a = r#"{"name":"CartridgeA","version":"1.0","channel":"release","registry_url":null,"description":"Cartridge A","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";convert;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}],"adapter_urns":[]}]}"#;
        let manifest_b = r#"{"name":"CartridgeB","version":"1.0","channel":"release","registry_url":null,"description":"Cartridge B","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";analyze;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}],"adapter_urns":[]}]}"#;

        // Create two cartridge pipe pairs (tokio sockets)
        let (pa_to_rt, rt_from_pa) = UnixStream::pair().unwrap();
        let (rt_to_pa, pa_from_rt) = UnixStream::pair().unwrap();
        let (pb_to_rt, rt_from_pb) = UnixStream::pair().unwrap();
        let (rt_to_pb, pb_from_rt) = UnixStream::pair().unwrap();

        let (pa_read, _) = rt_from_pa.into_split();
        let (_, pa_write) = rt_to_pa.into_split();
        let (pb_read, _) = rt_from_pb.into_split();
        let (_, pb_write) = rt_to_pb.into_split();

        // Cartridge A task
        let ma = manifest_a.as_bytes().to_vec();
        let pa_handle = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = cartridge_handshake_with_identity(pa_from_rt, pa_to_rt, &ma).await;
            // Read one REQ and verify cap
            let frame = r.read().await.unwrap().expect("expected REQ");
            assert_eq!(frame.frame_type, FrameType::Req);
            assert_eq!(
                frame.cap.as_deref(),
                Some("cap:in=\"media:void\";convert;out=\"media:void\""),
                "Cartridge A should receive convert REQ"
            );
            // Send END response
            let stream_id = "s1".to_string();
            let mut ss = Frame::stream_start(
                frame.id.clone(),
                stream_id.clone(),
                "media:".to_string(),
                None,
            );
            seq.assign(&mut ss);
            w.write(&ss).await.unwrap();
            let payload = b"converted".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk =
                Frame::chunk(frame.id.clone(), stream_id.clone(), 0, payload, 0, checksum);
            seq.assign(&mut chunk);
            w.write(&chunk).await.unwrap();
            let mut se = Frame::stream_end(frame.id.clone(), stream_id, 1);
            seq.assign(&mut se);
            w.write(&se).await.unwrap();
            let mut end = Frame::end(frame.id.clone(), None);
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: frame.id.clone(),
                xid: None,
            });
        });

        // Cartridge B task
        let mb = manifest_b.as_bytes().to_vec();
        let pb_handle = tokio::spawn(async move {
            let (r, w) = cartridge_handshake_with_identity(pb_from_rt, pb_to_rt, &mb).await;
            // Cartridge B should NOT receive the convert REQ
            // It may receive heartbeats, but the REQ should only go to Cartridge A
            // Just exit - the runtime will handle heartbeat timeouts
            drop(r);
            drop(w);
        });

        // Setup runtime
        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(pa_read, pa_write).await.unwrap();
        runtime.attach_cartridge(pb_read, pb_write).await.unwrap();

        // Create relay pipes (tokio sockets)
        let (relay_rt_read, relay_eng_write) = UnixStream::pair().unwrap();
        let (relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();

        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();
        let (_, eng_write_half) = relay_eng_write.into_split();
        let (eng_read_half, _) = relay_eng_read.into_split();

        // Engine: send REQ, read response, THEN close relay
        let req_id = MessageId::new_uuid();
        let engine_task = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let mut w = FrameWriter::new(eng_write_half);
            let mut r = FrameReader::new(eng_read_half);

            let xid = MessageId::Uint(1);
            let sid = uuid::Uuid::new_v4().to_string();
            let mut req = Frame::req(
                req_id.clone(),
                "cap:in=\"media:void\";convert;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let mut stream_start =
                Frame::stream_start(req_id.clone(), sid.clone(), "media:".to_string(), None);
            stream_start.routing_id = Some(xid.clone());
            seq.assign(&mut stream_start);
            w.write(&stream_start).await.unwrap();
            let payload = b"input".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req_id.clone(), sid.clone(), 0, payload, 0, checksum);
            chunk.routing_id = Some(xid.clone());
            seq.assign(&mut chunk);
            w.write(&chunk).await.unwrap();
            let mut stream_end = Frame::stream_end(req_id.clone(), sid, 1);
            stream_end.routing_id = Some(xid.clone());
            seq.assign(&mut stream_end);
            w.write(&stream_end).await.unwrap();
            let mut end = Frame::end(req_id.clone(), None);
            end.routing_id = Some(xid.clone());
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: req_id.clone(),
                xid: Some(xid.clone()),
            });

            let mut payload = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk {
                            payload.extend(f.payload.unwrap_or_default());
                        }
                        if f.frame_type == FrameType::End {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            drop(w); // Close relay AFTER response received
            payload
        });

        // Run runtime
        let runtime_result = runtime.run(rt_read_half, rt_write_half, || vec![]).await;
        assert!(
            runtime_result.is_ok(),
            "Runtime should exit cleanly: {:?}",
            runtime_result
        );

        let response_payload = engine_task.await.unwrap();
        assert_eq!(response_payload, b"converted");

        pa_handle.await.unwrap();
        pb_handle.await.unwrap();
    }

    // TEST419: Cartridge HEARTBEAT handled locally (not forwarded to relay)
    #[tokio::test]
    async fn test419_cartridge_heartbeat_handled_locally() {
        let manifest = r#"{"name":"HBCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Heartbeat cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";hb;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair (tokio sockets)
        let (p_to_rt, rt_from_p) = UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = UnixStream::pair().unwrap();

        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = cartridge_handshake_with_identity(p_from_rt, p_to_rt, &m).await;

            // Send a heartbeat from cartridge
            let hb_id = MessageId::new_uuid();
            let mut hb = Frame::heartbeat(hb_id.clone());
            seq.assign(&mut hb);
            w.write(&hb).await.unwrap();

            // Read the heartbeat response
            let response = r
                .read()
                .await
                .unwrap()
                .expect("Expected heartbeat response");
            assert_eq!(response.frame_type, FrameType::Heartbeat);
            assert_eq!(response.id, hb_id, "Response must echo the same ID");

            drop(w); // Close to signal EOF
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(p_read, p_write).await.unwrap();

        // Relay pipes (tokio sockets)
        let (relay_rt_read, relay_eng_write) = UnixStream::pair().unwrap();
        let (relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();

        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();
        let (eng_read_half, _) = relay_eng_read.into_split();

        // Drop engine write to close relay after cartridge finishes
        drop(relay_eng_write);

        // Engine reads — should NOT receive any heartbeat frame
        let engine_recv = tokio::spawn(async move {
            let mut r = FrameReader::new(eng_read_half);
            let mut frames = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => frames.push(f.frame_type),
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            frames
        });

        let _ = runtime.run(rt_read_half, rt_write_half, || vec![]).await;

        let received_types = engine_recv.await.unwrap();
        assert!(
            !received_types.contains(&FrameType::Heartbeat),
            "Heartbeat must NOT be forwarded to relay. Received frame types: {:?}",
            received_types
        );

        cartridge_handle.await.unwrap();
    }

    // TEST420: Cartridge non-HELLO/non-HB frames forwarded to relay (pass-through)
    #[tokio::test]
    async fn test420_cartridge_frames_forwarded_to_relay() {
        let manifest = r#"{"name":"FwdCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Forward cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";fwd;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair (tokio sockets)
        let (p_to_rt, rt_from_p) = UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = UnixStream::pair().unwrap();

        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let req_id = MessageId::new_uuid();
        let req_id_for_cartridge = req_id.clone();
        let cartridge_handle = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = cartridge_handshake_with_identity(p_from_rt, p_to_rt, &m).await;

            // Read the REQ
            let frame = r.read().await.unwrap().expect("Expected REQ");
            assert_eq!(frame.frame_type, FrameType::Req);

            // Consume incoming streams until END
            loop {
                let f = r.read().await.unwrap().expect("Expected frame");
                if f.frame_type == FrameType::End {
                    break;
                }
            }

            // Send LOG + response (LOG should be forwarded too)
            let mut log = Frame::log(
                req_id_for_cartridge.clone(),
                "info",
                crate::AttributionClass::Internal,
                "Processing",
                None,
            );
            seq.assign(&mut log);
            w.write(&log).await.unwrap();
            let sid = "rs".to_string();
            let mut ss = Frame::stream_start(
                req_id_for_cartridge.clone(),
                sid.clone(),
                "media:".to_string(),
                None,
            );
            seq.assign(&mut ss);
            w.write(&ss).await.unwrap();
            let payload = b"result".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(
                req_id_for_cartridge.clone(),
                sid.clone(),
                0,
                payload,
                0,
                checksum,
            );
            seq.assign(&mut chunk);
            w.write(&chunk).await.unwrap();
            let mut se = Frame::stream_end(req_id_for_cartridge.clone(), sid, 1);
            seq.assign(&mut se);
            w.write(&se).await.unwrap();
            let mut end = Frame::end(req_id_for_cartridge.clone(), None);
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: req_id_for_cartridge.clone(),
                xid: None,
            });
            drop(w);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(p_read, p_write).await.unwrap();

        // Relay (tokio sockets)
        let (relay_rt_read, relay_eng_write) = UnixStream::pair().unwrap();
        let (relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();

        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();
        let (_, eng_write_half) = relay_eng_write.into_split();
        let (eng_read_half, _) = relay_eng_read.into_split();

        // Engine: send REQ, read response (keep relay open until response received)
        let req_id_send = req_id.clone();
        let engine_task = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let mut w = FrameWriter::new(eng_write_half);
            let mut r = FrameReader::new(eng_read_half);

            let xid = MessageId::Uint(1);
            let sid = uuid::Uuid::new_v4().to_string();
            let mut req = Frame::req(
                req_id_send.clone(),
                "cap:in=\"media:void\";fwd;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let mut stream_start =
                Frame::stream_start(req_id_send.clone(), sid.clone(), "media:".to_string(), None);
            stream_start.routing_id = Some(xid.clone());
            seq.assign(&mut stream_start);
            w.write(&stream_start).await.unwrap();
            let mut stream_end = Frame::stream_end(req_id_send.clone(), sid, 0);
            stream_end.routing_id = Some(xid.clone());
            seq.assign(&mut stream_end);
            w.write(&stream_end).await.unwrap();
            let mut end = Frame::end(req_id_send.clone(), None);
            end.routing_id = Some(xid.clone());
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: req_id_send.clone(),
                xid: Some(xid.clone()),
            });

            let mut types = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        let is_end = f.frame_type == FrameType::End;
                        types.push(f.frame_type);
                        if is_end {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            drop(w); // Close relay AFTER response received
            types
        });

        let _ = runtime.run(rt_read_half, rt_write_half, || vec![]).await;

        let received_types = engine_task.await.unwrap();

        // Should see: LOG, STREAM_START, CHUNK, STREAM_END, END
        assert!(
            received_types.contains(&FrameType::Log),
            "LOG should be forwarded. Got: {:?}",
            received_types
        );
        assert!(
            received_types.contains(&FrameType::StreamStart),
            "STREAM_START should be forwarded"
        );
        assert!(
            received_types.contains(&FrameType::Chunk),
            "CHUNK should be forwarded"
        );
        assert!(
            received_types.contains(&FrameType::End),
            "END should be forwarded"
        );

        cartridge_handle.await.unwrap();
    }

    // TEST418: Route STREAM_START/CHUNK/STREAM_END/END by req_id (not cap_urn)
    // Verifies that after the initial REQ→cartridge routing, all subsequent continuation
    // frames with the same req_id are routed to the same cartridge — even though no cap_urn
    // is present on those frames.
    #[tokio::test]
    async fn test418_route_continuation_frames_by_req_id() {
        let manifest = r#"{"name":"ContCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Continuation cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";cont;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair (tokio sockets)
        let (p_to_rt, rt_from_p) = UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = UnixStream::pair().unwrap();

        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = cartridge_handshake_with_identity(p_from_rt, p_to_rt, &m).await;

            // Read REQ
            let req = r.read().await.unwrap().expect("Expected REQ");
            assert_eq!(req.frame_type, FrameType::Req);

            // Continuation frames must arrive with same req_id
            let mut received_types = Vec::new();
            let mut data = Vec::new();
            loop {
                let f = r.read().await.unwrap().expect("Expected frame");
                received_types.push(f.frame_type);
                if f.frame_type == FrameType::Chunk {
                    data.extend(f.payload.unwrap_or_default());
                }
                if f.frame_type == FrameType::End {
                    break;
                }
                assert_eq!(
                    f.id, req.id,
                    "All continuation frames must have same req_id"
                );
            }

            // Verify we got the full sequence
            assert!(
                received_types.contains(&FrameType::StreamStart),
                "Must receive STREAM_START"
            );
            assert!(
                received_types.contains(&FrameType::Chunk),
                "Must receive CHUNK"
            );
            assert!(
                received_types.contains(&FrameType::StreamEnd),
                "Must receive STREAM_END"
            );
            assert!(received_types.contains(&FrameType::End), "Must receive END");
            assert_eq!(data, b"payload-data", "Must receive full payload");

            // Send response
            let sid = "rs".to_string();
            let mut ss =
                Frame::stream_start(req.id.clone(), sid.clone(), "media:".to_string(), None);
            seq.assign(&mut ss);
            w.write(&ss).await.unwrap();
            let payload = b"ok".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req.id.clone(), sid.clone(), 0, payload, 0, checksum);
            seq.assign(&mut chunk);
            w.write(&chunk).await.unwrap();
            let mut se = Frame::stream_end(req.id.clone(), sid, 1);
            seq.assign(&mut se);
            w.write(&se).await.unwrap();
            let mut end = Frame::end(req.id.clone(), None);
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: req.id.clone(),
                xid: None,
            });
            drop(w);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(p_read, p_write).await.unwrap();

        // Relay (tokio sockets)
        let (relay_rt_read, relay_eng_write) = UnixStream::pair().unwrap();
        let (relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();

        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();
        let (_, eng_write_half) = relay_eng_write.into_split();
        let (eng_read_half, _) = relay_eng_read.into_split();

        let req_id = MessageId::new_uuid();
        let engine_task = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let mut w = FrameWriter::new(eng_write_half);
            let mut r = FrameReader::new(eng_read_half);

            let xid = MessageId::Uint(1);
            // Send REQ + stream continuation frames
            let mut req = Frame::req(
                req_id.clone(),
                "cap:in=\"media:void\";cont;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let sid = uuid::Uuid::new_v4().to_string();
            let mut stream_start =
                Frame::stream_start(req_id.clone(), sid.clone(), "media:".to_string(), None);
            stream_start.routing_id = Some(xid.clone());
            seq.assign(&mut stream_start);
            w.write(&stream_start).await.unwrap();
            let payload = b"payload-data".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req_id.clone(), sid.clone(), 0, payload, 0, checksum);
            chunk.routing_id = Some(xid.clone());
            seq.assign(&mut chunk);
            w.write(&chunk).await.unwrap();
            let mut stream_end = Frame::stream_end(req_id.clone(), sid, 1);
            stream_end.routing_id = Some(xid.clone());
            seq.assign(&mut stream_end);
            w.write(&stream_end).await.unwrap();
            let mut end = Frame::end(req_id.clone(), None);
            end.routing_id = Some(xid.clone());
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: req_id.clone(),
                xid: Some(xid.clone()),
            });

            // Read response
            let mut payload = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk {
                            payload.extend(f.payload.unwrap_or_default());
                        }
                        if f.frame_type == FrameType::End {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            drop(w);
            payload
        });

        let result = runtime.run(rt_read_half, rt_write_half, || vec![]).await;
        assert!(result.is_ok(), "Runtime should exit cleanly: {:?}", result);

        let response = engine_task.await.unwrap();
        assert_eq!(response, b"ok");

        cartridge_handle.await.unwrap();
    }

    // TEST421: Cartridge death updates capability list (caps removed)
    #[tokio::test]
    async fn test421_cartridge_death_updates_capabilities() {
        let manifest = r#"{"name":"Dying","version":"1.0","channel":"release","registry_url":null,"description":"Dying cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";die;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair (tokio sockets)
        let (p_to_rt, rt_from_p) = UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = UnixStream::pair().unwrap();

        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (r, w) = cartridge_handshake_with_identity(p_from_rt, p_to_rt, &m).await;
            // Die immediately after identity verification
            drop(w);
            drop(r);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(p_read, p_write).await.unwrap();

        // Before death: cap_table is the routing source of truth — it
        // must include the cartridge's caps so on-demand spawn /
        // respawn can dispatch to them. (Inventory advertisement to
        // the engine goes through `aggregate_installed_cartridges()`
        // and is identity-gated; an attached cartridge with no
        // on-disk anchor has no identity and is therefore not
        // advertised, but is still locally routable.)
        let expected_urn = CapUrn::from_string("cap:in=\"media:void\";die;out=\"media:void\"")
            .expect("Expected URN should parse");
        let cap_table_urns: Vec<String> =
            runtime.cap_table.iter().map(|(u, _)| u.clone()).collect();
        let found = cap_table_urns.iter().any(|urn_str| {
            CapUrn::from_string(urn_str)
                .map(|u| expected_urn.is_comparable(&u))
                .unwrap_or(false)
        });
        assert!(
            found,
            "cap_table should contain cartridge's cap. Expected URN with die marker, got: {:?}",
            cap_table_urns
        );

        // Relay (close immediately to let runtime exit after processing death) - tokio sockets
        let (relay_rt_read, _relay_eng_write) = UnixStream::pair().unwrap();
        let (_relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();

        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();

        // Drop engine write side to close relay
        drop(_relay_eng_write);

        let _ = runtime.run(rt_read_half, rt_write_half, || vec![]).await;

        // After death: cap_table should STILL include the cartridge's
        // caps (derived from its `cap_groups`, which survive the
        // process). Dead cartridges keep their routing entries so
        // on-demand spawn can dispatch a fresh REQ to them.
        let cap_table_urns_after: Vec<String> =
            runtime.cap_table.iter().map(|(u, _)| u.clone()).collect();
        let found_after = cap_table_urns_after.iter().any(|urn_str| {
            CapUrn::from_string(urn_str)
                .map(|u| expected_urn.is_comparable(&u))
                .unwrap_or(false)
        });
        assert!(
            found_after,
            "Dead cartridge's caps (from cap_groups) should still be in cap_table for on-demand respawn. Expected URN with die marker, got: {:?}",
            cap_table_urns_after
        );

        cartridge_handle.await.unwrap();
    }

    // TEST422: Cartridge death sends ERR for all pending requests via relay
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test422_cartridge_death_sends_err_for_pending_requests() {
        let manifest = r#"{"name":"DieCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Die cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";die;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair (tokio sockets)
        let (p_to_rt, rt_from_p) = UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = UnixStream::pair().unwrap();

        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (mut r, w) = cartridge_handshake_with_identity(p_from_rt, p_to_rt, &m).await;

            // Read REQ and consume all frames until END, then die
            let _req = r.read().await.unwrap().expect("Expected REQ");
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::End {
                            break;
                        }
                    }
                    _ => break,
                }
            }
            // Die — drop everything
            drop(w);
            drop(r);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(p_read, p_write).await.unwrap();

        // Relay (tokio sockets)
        let (relay_rt_read, relay_eng_write) = UnixStream::pair().unwrap();
        let (relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();

        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();
        let (_, eng_write_half) = relay_eng_write.into_split();
        let (eng_read_half, _) = relay_eng_read.into_split();

        let req_id = MessageId::new_uuid();
        let engine_task = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let mut w = FrameWriter::new(eng_write_half);

            let xid = MessageId::Uint(1);
            // Send REQ (cartridge will die after reading it)
            let mut req = Frame::req(
                req_id.clone(),
                "cap:in=\"media:void\";die;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let mut end = Frame::end(req_id.clone(), None);
            end.routing_id = Some(xid.clone());
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: req_id.clone(),
                xid: Some(xid.clone()),
            });

            // Close relay connection after sending request
            // (in real use, engine would implement timeout for pending requests)
            drop(w);
        });

        // Runtime should handle cartridge death gracefully and exit when relay disconnects
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            runtime.run(rt_read_half, rt_write_half, || vec![]),
        )
        .await;
        assert!(
            result.is_ok(),
            "Runtime should exit cleanly when cartridge dies and relay disconnects"
        );

        engine_task.await.unwrap();

        cartridge_handle.await.unwrap();
    }

    // TEST423: Multiple cartridges registered with distinct caps route independently
    #[tokio::test]
    async fn test423_multiple_cartridges_route_independently() {
        let manifest_a = r#"{"name":"PA","version":"1.0","channel":"release","registry_url":null,"description":"Cartridge A","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";alpha;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}],"adapter_urns":[]}]}"#;
        let manifest_b = r#"{"name":"PB","version":"1.0","channel":"release","registry_url":null,"description":"Cartridge B","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";beta;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge A (tokio sockets)
        let (pa_to_rt, rt_from_pa) = UnixStream::pair().unwrap();
        let (rt_to_pa, pa_from_rt) = UnixStream::pair().unwrap();
        let (pa_read, _) = rt_from_pa.into_split();
        let (_, pa_write) = rt_to_pa.into_split();

        // Cartridge B (tokio sockets)
        let (pb_to_rt, rt_from_pb) = UnixStream::pair().unwrap();
        let (rt_to_pb, pb_from_rt) = UnixStream::pair().unwrap();
        let (pb_read, _) = rt_from_pb.into_split();
        let (_, pb_write) = rt_to_pb.into_split();

        let ma = manifest_a.as_bytes().to_vec();
        let pa_handle = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = cartridge_handshake_with_identity(pa_from_rt, pa_to_rt, &ma).await;
            let req = r.read().await.unwrap().expect("Expected REQ");
            assert_eq!(
                req.cap.as_deref(),
                Some("cap:in=\"media:void\";alpha;out=\"media:void\"")
            );
            loop {
                let f = r.read().await.unwrap().expect("f");
                if f.frame_type == FrameType::End {
                    break;
                }
            }
            let sid = "a".to_string();
            let mut ss =
                Frame::stream_start(req.id.clone(), sid.clone(), "media:".to_string(), None);
            seq.assign(&mut ss);
            w.write(&ss).await.unwrap();
            let payload = b"from-A".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req.id.clone(), sid.clone(), 0, payload, 0, checksum);
            seq.assign(&mut chunk);
            w.write(&chunk).await.unwrap();
            let mut se = Frame::stream_end(req.id.clone(), sid, 1);
            seq.assign(&mut se);
            w.write(&se).await.unwrap();
            let mut end = Frame::end(req.id.clone(), None);
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: req.id.clone(),
                xid: None,
            });
            drop(w);
        });

        let mb = manifest_b.as_bytes().to_vec();
        let pb_handle = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = cartridge_handshake_with_identity(pb_from_rt, pb_to_rt, &mb).await;
            let req = r.read().await.unwrap().expect("Expected REQ");
            assert_eq!(
                req.cap.as_deref(),
                Some("cap:in=\"media:void\";beta;out=\"media:void\"")
            );
            loop {
                let f = r.read().await.unwrap().expect("f");
                if f.frame_type == FrameType::End {
                    break;
                }
            }
            let sid = "b".to_string();
            let mut ss =
                Frame::stream_start(req.id.clone(), sid.clone(), "media:".to_string(), None);
            seq.assign(&mut ss);
            w.write(&ss).await.unwrap();
            let payload = b"from-B".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req.id.clone(), sid.clone(), 0, payload, 0, checksum);
            seq.assign(&mut chunk);
            w.write(&chunk).await.unwrap();
            let mut se = Frame::stream_end(req.id.clone(), sid, 1);
            seq.assign(&mut se);
            w.write(&se).await.unwrap();
            let mut end = Frame::end(req.id.clone(), None);
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: req.id.clone(),
                xid: None,
            });
            drop(w);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(pa_read, pa_write).await.unwrap();
        runtime.attach_cartridge(pb_read, pb_write).await.unwrap();

        // Relay (tokio sockets)
        let (relay_rt_read, relay_eng_write) = UnixStream::pair().unwrap();
        let (relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();
        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();
        let (_, eng_write_half) = relay_eng_write.into_split();
        let (eng_read_half, _) = relay_eng_read.into_split();

        let alpha_id = MessageId::new_uuid();
        let beta_id = MessageId::new_uuid();
        let alpha_c = alpha_id.clone();
        let beta_c = beta_id.clone();

        let engine_task = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let mut w = FrameWriter::new(eng_write_half);
            let mut r = FrameReader::new(eng_read_half);

            let xid_alpha = MessageId::Uint(1);
            let xid_beta = MessageId::Uint(2);
            // Send two requests to different caps
            let mut req_alpha = Frame::req(
                alpha_c.clone(),
                "cap:in=\"media:void\";alpha;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req_alpha.routing_id = Some(xid_alpha.clone());
            seq.assign(&mut req_alpha);
            w.write(&req_alpha).await.unwrap();
            let mut end_alpha = Frame::end(alpha_c.clone(), None);
            end_alpha.routing_id = Some(xid_alpha.clone());
            seq.assign(&mut end_alpha);
            w.write(&end_alpha).await.unwrap();
            seq.remove(&FlowKey {
                rid: alpha_c.clone(),
                xid: Some(xid_alpha.clone()),
            });
            let mut req_beta = Frame::req(
                beta_c.clone(),
                "cap:in=\"media:void\";beta;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req_beta.routing_id = Some(xid_beta.clone());
            seq.assign(&mut req_beta);
            w.write(&req_beta).await.unwrap();
            let mut end_beta = Frame::end(beta_c.clone(), None);
            end_beta.routing_id = Some(xid_beta.clone());
            seq.assign(&mut end_beta);
            w.write(&end_beta).await.unwrap();
            seq.remove(&FlowKey {
                rid: beta_c.clone(),
                xid: Some(xid_beta.clone()),
            });

            // Collect responses by req_id
            let mut alpha_data = Vec::new();
            let mut beta_data = Vec::new();
            let mut ends = 0;
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk {
                            if f.id == alpha_c {
                                alpha_data.extend(f.payload.unwrap_or_default());
                            } else if f.id == beta_c {
                                beta_data.extend(f.payload.unwrap_or_default());
                            }
                        }
                        if f.frame_type == FrameType::End {
                            ends += 1;
                            if ends >= 2 {
                                break;
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            drop(w);
            (alpha_data, beta_data)
        });

        let _ = runtime.run(rt_read_half, rt_write_half, || vec![]).await;

        let (alpha_data, beta_data) = engine_task.await.unwrap();
        assert_eq!(alpha_data, b"from-A", "Alpha response from Cartridge A");
        assert_eq!(beta_data, b"from-B", "Beta response from Cartridge B");

        pa_handle.await.unwrap();
        pb_handle.await.unwrap();
    }

    // TEST424: Concurrent requests to the same cartridge are handled independently
    #[tokio::test]
    async fn test424_concurrent_requests_to_same_cartridge() {
        let manifest = r#"{"name":"ConcCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Concurrent cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";conc;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair (tokio sockets)
        let (p_to_rt, rt_from_p) = UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = UnixStream::pair().unwrap();
        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = cartridge_handshake_with_identity(p_from_rt, p_to_rt, &m).await;

            // Read two REQs and their streams, then respond to each
            let mut pending: Vec<MessageId> = Vec::new();
            let mut active_requests = 0;
            loop {
                let f = r.read().await.unwrap().expect("frame");
                match f.frame_type {
                    FrameType::Req => {
                        pending.push(f.id.clone());
                        active_requests += 1;
                    }
                    FrameType::End => {
                        // When we've seen END for both requests, respond to both
                        active_requests -= 1;
                        if active_requests == 0 && pending.len() == 2 {
                            break;
                        }
                    }
                    _ => {}
                }
            }

            // Respond to each with different data
            for (i, req_id) in pending.iter().enumerate() {
                let data = format!("response-{}", i).into_bytes();
                let checksum = Frame::compute_checksum(&data);
                let sid = format!("s{}", i);
                let mut ss =
                    Frame::stream_start(req_id.clone(), sid.clone(), "media:".to_string(), None);
                seq.assign(&mut ss);
                w.write(&ss).await.unwrap();
                let mut chunk = Frame::chunk(req_id.clone(), sid.clone(), 0, data, 0, checksum);
                seq.assign(&mut chunk);
                w.write(&chunk).await.unwrap();
                let mut se = Frame::stream_end(req_id.clone(), sid, 1);
                seq.assign(&mut se);
                w.write(&se).await.unwrap();
                let mut end = Frame::end(req_id.clone(), None);
                seq.assign(&mut end);
                w.write(&end).await.unwrap();
                seq.remove(&FlowKey {
                    rid: req_id.clone(),
                    xid: None,
                });
            }
            drop(w);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(p_read, p_write).await.unwrap();

        // Relay (tokio sockets)
        let (relay_rt_read, relay_eng_write) = UnixStream::pair().unwrap();
        let (relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();
        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();
        let (_, eng_write_half) = relay_eng_write.into_split();
        let (eng_read_half, _) = relay_eng_read.into_split();

        let req_id_0 = MessageId::new_uuid();
        let req_id_1 = MessageId::new_uuid();
        let r0 = req_id_0.clone();
        let r1 = req_id_1.clone();

        let engine_task = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let mut w = FrameWriter::new(eng_write_half);
            let mut r = FrameReader::new(eng_read_half);

            // Send two REQs concurrently (same cap)
            let xid_0 = MessageId::Uint(1);
            let xid_1 = MessageId::Uint(2);
            let mut req_0 = Frame::req(
                r0.clone(),
                "cap:in=\"media:void\";conc;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req_0.routing_id = Some(xid_0.clone());
            seq.assign(&mut req_0);
            w.write(&req_0).await.unwrap();
            let mut end_0 = Frame::end(r0.clone(), None);
            end_0.routing_id = Some(xid_0.clone());
            seq.assign(&mut end_0);
            w.write(&end_0).await.unwrap();
            seq.remove(&FlowKey {
                rid: r0.clone(),
                xid: Some(xid_0.clone()),
            });
            let mut req_1 = Frame::req(
                r1.clone(),
                "cap:in=\"media:void\";conc;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req_1.routing_id = Some(xid_1.clone());
            seq.assign(&mut req_1);
            w.write(&req_1).await.unwrap();
            let mut end_1 = Frame::end(r1.clone(), None);
            end_1.routing_id = Some(xid_1.clone());
            seq.assign(&mut end_1);
            w.write(&end_1).await.unwrap();
            seq.remove(&FlowKey {
                rid: r1.clone(),
                xid: Some(xid_1.clone()),
            });

            // Collect responses by req_id
            let mut data_0 = Vec::new();
            let mut data_1 = Vec::new();
            let mut ends = 0;
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk {
                            if f.id == r0 {
                                data_0.extend(f.payload.unwrap_or_default());
                            } else if f.id == r1 {
                                data_1.extend(f.payload.unwrap_or_default());
                            }
                        }
                        if f.frame_type == FrameType::End {
                            ends += 1;
                            if ends >= 2 {
                                break;
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            drop(w);
            (data_0, data_1)
        });

        let _ = runtime.run(rt_read_half, rt_write_half, || vec![]).await;

        let (data_0, data_1) = engine_task.await.unwrap();
        assert_eq!(data_0, b"response-0", "First concurrent request response");
        assert_eq!(data_1, b"response-1", "Second concurrent request response");

        cartridge_handle.await.unwrap();
    }

    // TEST425: find_cartridge_for_cap returns None for unregistered cap
    #[test]
    fn test425_find_cartridge_for_cap_unknown() {
        let mut runtime = CartridgeHostRuntime::new();
        runtime.register_cartridge(
            Path::new("/test"),
            "test",
            "0.0.0",
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            &cap_groups_from_urns(&["cap:in=\"media:void\";known;out=\"media:void\""]),
        );
        assert!(runtime
            .find_cartridge_for_cap("cap:in=\"media:void\";known;out=\"media:void\"")
            .is_some());
        assert!(runtime
            .find_cartridge_for_cap("cap:in=\"media:void\";unknown;out=\"media:void\"")
            .is_none());
    }

    // =========================================================================
    // Identity verification integration tests
    // =========================================================================

    // TEST485: attach_cartridge completes identity verification with working cartridge
    #[tokio::test]
    async fn test485_attach_cartridge_identity_verification_succeeds() {
        let manifest = r#"{"name":"IdentityTest","version":"1.0","channel":"release","registry_url":null,"description":"Test","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";test;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair (tokio sockets)
        let (p_to_rt, rt_from_p) = UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = UnixStream::pair().unwrap();

        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            cartridge_handshake_with_identity(p_from_rt, p_to_rt, &m).await;
        });

        let mut runtime = CartridgeHostRuntime::new();
        let idx = runtime.attach_cartridge(p_read, p_write).await.unwrap();
        assert_eq!(idx, 0);
        assert!(
            runtime.cartridges[0].running,
            "Cartridge must be running after identity verification"
        );

        // Verify both caps are registered (semantic comparison, not string)
        let identity_urn = crate::CapUrn::from_string(CAP_IDENTITY).unwrap();
        let parsed_caps: Vec<&crate::Cap> = runtime.cartridges[0]
            .cap_groups
            .iter()
            .flat_map(|g| g.caps.iter())
            .collect();
        assert!(
            parsed_caps.iter().any(|c| identity_urn.conforms_to(&c.urn)),
            "Must have identity cap"
        );
        assert_eq!(parsed_caps.len(), 2, "Must have both caps");

        cartridge_handle.await.unwrap();
    }

    // TEST486: attach_cartridge rejects cartridge that fails identity verification
    #[tokio::test]
    async fn test486_attach_cartridge_identity_verification_fails() {
        let manifest = r#"{"name":"BrokenIdentity","version":"1.0","channel":"release","registry_url":null,"description":"Test","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair (tokio sockets)
        let (p_to_rt, rt_from_p) = UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = UnixStream::pair().unwrap();

        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            use crate::bifaci::io::{handshake_accept, FrameReader, FrameWriter};
            let mut reader = FrameReader::new(BufReader::new(p_from_rt));
            let mut writer = FrameWriter::new(BufWriter::new(p_to_rt));
            handshake_accept(
                &mut reader,
                &mut writer,
                &m,
                &crate::bifaci::pools::PoolStates::new(),
            )
            .await
            .unwrap();

            // Read identity REQ, respond with ERR (broken identity handler)
            let req = reader.read().await.unwrap().expect("expected identity REQ");
            assert_eq!(req.frame_type, FrameType::Req);
            let err = Frame::err(
                req.id,
                "BROKEN",
                crate::AttributionClass::Internal,
                "identity handler is broken",
                None,
            );
            writer.write(&err).await.unwrap();
        });

        let mut runtime = CartridgeHostRuntime::new();
        let result = runtime.attach_cartridge(p_read, p_write).await;
        assert!(
            result.is_err(),
            "attach_cartridge must fail when identity verification fails"
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("Identity verification failed"),
            "Error must mention identity verification: {}",
            err
        );

        cartridge_handle.await.unwrap();
    }

    // TEST6623: Cartridge death keeps caps advertised for on-demand respawn.
    // The cartridge's `cap_groups` survive process death, so the host can
    // continue advertising the cartridge's caps and the relay can route
    // a fresh REQ to it (which triggers an on-demand respawn).
    #[tokio::test]
    async fn test6623_cartridge_death_keeps_caps_advertised() {
        // Real on-disk file so sha256 hashing succeeds and the cartridge
        // gets a valid installed_identity — required for inventory advertisement.
        let bin_dir = tempfile::tempdir().expect("create temp dir");
        let bin_path = bin_dir.path().join("thumbnailcartridge");
        std::fs::write(&bin_path, b"#!/bin/false\n").expect("write binary");

        let mut runtime = CartridgeHostRuntime::new();
        let cap_groups = cap_groups_from_urns(&[
            CAP_IDENTITY,
            "cap:in=\"media:ext=pdf\";thumbnail;out=\"media:ext=png;image\"",
        ]);
        runtime.register_cartridge(
            &bin_path,
            "thumbnailcartridge",
            "0.0.0",
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            &cap_groups,
        );

        // cap_table is the routing source of truth.
        assert_eq!(runtime.cap_table.len(), 2);
        assert_eq!(runtime.cap_table[0].0, CAP_IDENTITY);
        assert_eq!(
            runtime.cap_table[1].0,
            "cap:in=\"media:ext=pdf\";out=\"media:ext=png;image\";thumbnail"
        );

        // Build capabilities (no outbound_tx, so no RelayNotify sent).
        runtime.rebuild_capabilities(None);

        // Inventory advertised to the engine: identity-filtered. The
        // cartridge has a hashable binary so it appears, and its
        // cap_groups are the source of truth — even though its
        // process has not been spawned (running == false).
        let advertised = host_advertised_cap_urns(&runtime);
        assert!(
            advertised.contains(&CAP_IDENTITY.to_string()),
            "Identity cap must be advertised, got {:?}",
            advertised
        );
        assert!(
            advertised.iter().any(|s| s.contains("thumbnail")),
            "Thumbnail cap must be advertised, got {:?}",
            advertised
        );
    }

    // TEST662: rebuild_capabilities includes non-running cartridges' caps
    // (each cartridge's `cap_groups` is the source of truth, regardless
    // of whether its process has been spawned yet).
    #[tokio::test]
    async fn test662_rebuild_capabilities_includes_non_running_cartridges() {
        let bin_dir = tempfile::tempdir().expect("create temp dir");
        let bin_path_1 = bin_dir.path().join("extractcartridge");
        let bin_path_2 = bin_dir.path().join("ocrcartridge");
        std::fs::write(&bin_path_1, b"#!/bin/false\n").expect("write binary 1");
        std::fs::write(&bin_path_2, b"#!/bin/false\n").expect("write binary 2");

        let mut runtime = CartridgeHostRuntime::new();
        let groups_1 = cap_groups_from_urns(&[
            CAP_IDENTITY,
            "cap:in=\"media:ext=pdf\";extract;out=\"media:text\"",
        ]);
        let groups_2 = cap_groups_from_urns(&[
            CAP_IDENTITY,
            "cap:in=\"media:image\";ocr;out=\"media:text\"",
        ]);

        runtime.register_cartridge(
            &bin_path_1,
            "extractcartridge",
            "0.0.0",
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            &groups_1,
        );
        runtime.register_cartridge(
            &bin_path_2,
            "ocrcartridge",
            "0.0.0",
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            &groups_2,
        );

        runtime.rebuild_capabilities(None);

        // Both cartridges advertised; each is one entry in the
        // identity-filtered inventory; the union of their cap_groups
        // contains identity + extract + ocr.
        let advertised = host_advertised_cap_urns(&runtime);
        assert!(
            advertised.contains(&CAP_IDENTITY.to_string()),
            "Identity cap must be advertised, got {:?}",
            advertised
        );
        assert!(
            advertised.iter().any(|s| s.contains("extract")),
            "Extract cap must be advertised, got {:?}",
            advertised
        );
        assert!(
            advertised.iter().any(|s| s.contains("ocr")),
            "OCR cap must be advertised, got {:?}",
            advertised
        );
    }

    // TEST663: Cartridge with hello_failed is permanently removed from capabilities
    #[tokio::test]
    async fn test663_hello_failed_cartridge_removed_from_capabilities() {
        let bin_dir = tempfile::tempdir().expect("create temp dir");
        let bin_path = bin_dir.path().join("brokencartridge");
        std::fs::write(&bin_path, b"#!/bin/false\n").expect("write binary");

        let mut runtime = CartridgeHostRuntime::new();
        let cap_groups = cap_groups_from_urns(&[
            CAP_IDENTITY,
            "cap:in=\"media:void\";broken;out=\"media:void\"",
        ]);
        runtime.register_cartridge(
            &bin_path,
            "brokencartridge",
            "0.0.0",
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            &cap_groups,
        );

        // Manually mark it as hello_failed (simulating HELLO handshake failure)
        runtime.cartridges[0].hello_failed = true;

        // update_cap_table should exclude hello_failed cartridges
        runtime.update_cap_table();

        // cap_table is empty: hello_failed cartridges are not routable.
        let found_broken = runtime
            .cap_table
            .iter()
            .any(|(urn, _)| urn.contains("broken"));
        assert!(
            !found_broken,
            "hello_failed cartridge caps should not be in cap_table"
        );

        // The host-level inventory likewise excludes hello_failed
        // cartridges — even though their identity record exists.
        runtime.rebuild_capabilities(None);
        let advertised = host_advertised_cap_urns(&runtime);
        assert!(
            !advertised.iter().any(|s| s.contains("broken")),
            "hello_failed cartridge must not be advertised, got {:?}",
            advertised
        );
    }

    // TEST664: Attached cartridge replaces pre-registration caps with
    // manifest caps. The pre-attach `cap_groups` (from probe-time
    // discovery) get superseded by the post-HELLO `cap_groups` from
    // the actual handshake.
    #[tokio::test]
    async fn test664_running_cartridge_uses_manifest_caps() {
        // Manifest declares different caps than the pre-registration
        // probe — the post-HELLO snapshot must win.
        let manifest = r#"{"name":"Test","version":"1.0","channel":"release","registry_url":null,"description":"Test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:text\";uppercase;out=\"media:text\"","title":"Uppercase","aliases": ["uppercase"],"args":[]}],"adapter_urns":[]}]}"#;

        // Create socket pairs (runtime side and cartridge side)
        let (rt_sock, cartridge_sock) = UnixStream::pair().unwrap();

        // Split runtime socket for attach_cartridge
        let (p_read, p_write) = rt_sock.into_split();

        // Split cartridge socket for handshake
        let (cartridge_from_rt, cartridge_to_rt) = cartridge_sock.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (_r, _w) =
                cartridge_handshake_with_identity(cartridge_from_rt, cartridge_to_rt, &m).await;
            // Keep alive for test
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });

        let mut runtime = CartridgeHostRuntime::new();

        // Register with stale (probe-time) cap_groups BEFORE attaching.
        let bin_dir = tempfile::tempdir().expect("create temp dir");
        let bin_path = bin_dir.path().join("extractcartridge");
        std::fs::write(&bin_path, b"#!/bin/false\n").expect("write binary");
        let pre_attach_groups = cap_groups_from_urns(&[
            CAP_IDENTITY,
            "cap:in=\"media:ext=pdf\";extract;out=\"media:text\"",
        ]);
        runtime.register_cartridge(
            &bin_path,
            "extractcartridge",
            "0.0.0",
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            &pre_attach_groups,
        );

        // Now attach the actual cartridge (which sends different manifest)
        // This simulates what happens when a registered cartridge spawns
        let _cartridge_idx = runtime.attach_cartridge(p_read, p_write).await.unwrap();

        // cap_table is the routing source of truth and includes both
        // the registered cartridge (with identity) AND the attached
        // cartridge (without identity). The running cartridge's
        // manifest caps (uppercase) must be routable.
        let cap_table_urns: Vec<String> =
            runtime.cap_table.iter().map(|(u, _)| u.clone()).collect();
        assert!(
            cap_table_urns.iter().any(|s| s.contains("uppercase")),
            "Running cartridge's manifest cap must be in cap_table. Got: {:?}",
            cap_table_urns
        );

        cartridge_handle.await.unwrap();
    }

    // TEST665: Cap table aggregates caps from every healthy cartridge —
    // attached/running cartridges contribute their post-HELLO cap_groups,
    // registered-but-not-yet-spawned cartridges contribute their
    // probe-time cap_groups. Both flow through the same `cap_urns()` view.
    #[tokio::test]
    async fn test665_cap_table_mixed_running_and_non_running() {
        // Set up a running cartridge
        let manifest = r#"{"name":"Running","version":"1.0","channel":"release","registry_url":null,"description":"Running cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:text\";running-op;out=\"media:text\"","title":"RunningOp","aliases": ["running"],"args":[]}],"adapter_urns":[]}]}"#;

        // Create socket pairs (runtime side and cartridge side)
        let (rt_sock, cartridge_sock) = UnixStream::pair().unwrap();

        // Split runtime socket for attach_cartridge
        let (p_read, p_write) = rt_sock.into_split();

        // Split cartridge socket for handshake
        let (cartridge_from_rt, cartridge_to_rt) = cartridge_sock.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (_r, _w) =
                cartridge_handshake_with_identity(cartridge_from_rt, cartridge_to_rt, &m).await;
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });

        let mut runtime = CartridgeHostRuntime::new();

        // Attach running cartridge
        runtime.attach_cartridge(p_read, p_write).await.unwrap();

        // Register a non-running cartridge with probe-time cap_groups
        let dormant_groups = cap_groups_from_urns(&[
            CAP_IDENTITY,
            "cap:in=\"media:ext=pdf\";not-running-op;out=\"media:text\"",
        ]);
        runtime.register_cartridge(
            std::path::Path::new("/fake/not-running"),
            "not-running",
            "0.0.0",
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            &dormant_groups,
        );

        // Update cap table
        runtime.update_cap_table();

        // Cap table should have:
        // - Running cartridge's manifest caps (running-op)
        // - Non-running cartridge's probe-time caps (not-running-op)
        let has_running_op = runtime
            .cap_table
            .iter()
            .any(|(urn, _)| urn.contains("running-op"));
        let has_not_running_op = runtime
            .cap_table
            .iter()
            .any(|(urn, _)| urn.contains("not-running-op"));

        assert!(
            has_running_op,
            "Cap table should have running cartridge's manifest caps"
        );
        assert!(
            has_not_running_op,
            "Cap table should have non-running cartridge's probe-time caps"
        );

        cartridge_handle.await.unwrap();
    }

    // =========================================================================
    // TEST: CartridgeProcessHandle — snapshot and kill
    // =========================================================================

    // TEST1250: Process snapshots start empty before any cartridges are attached or spawned.
    #[tokio::test]
    async fn test1250_process_handle_snapshot_empty_initially() {
        let runtime = CartridgeHostRuntime::new();
        let handle = runtime.process_handle();
        let cartridges = handle.running_cartridges();
        assert!(
            cartridges.is_empty(),
            "Snapshot should be empty before any cartridges are spawned"
        );
    }

    // TEST1251: Attached cartridges without child PIDs are excluded from process snapshots.
    #[tokio::test]
    async fn test1251_process_handle_snapshot_excludes_attached_cartridges() {
        // Attached cartridges are connected via socketpair, not spawned as separate
        // processes — they have no PID and should not appear in the process snapshot.
        let (runtime_sock, cartridge_sock) = UnixStream::pair().unwrap();
        let (r_read, r_write) = runtime_sock.into_split();
        let (p_read, p_write) = cartridge_sock.into_split();

        let manifest = r#"{"name":"SnapCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Snapshot test","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";snap;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}],"adapter_urns":[]}]}"#;

        let cartridge_handle = tokio::spawn(async move {
            let (_reader, _writer) =
                cartridge_handshake_with_identity(p_read, p_write, manifest.as_bytes()).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        });

        let mut runtime = CartridgeHostRuntime::new();
        let handle = runtime.process_handle();

        runtime.attach_cartridge(r_read, r_write).await.unwrap();

        // Attached cartridges have process=None → no PID → excluded from snapshot
        let cartridges = handle.running_cartridges();
        assert!(
            cartridges.is_empty(),
            "Attached cartridges have no PID and should not appear in process snapshot"
        );

        cartridge_handle.await.unwrap();
    }

    // TEST1252: Cartridge process handles remain usable after clone-and-send across tasks.
    #[tokio::test]
    async fn test1252_process_handle_is_clone_and_send() {
        let runtime = CartridgeHostRuntime::new();
        let handle = runtime.process_handle();
        let handle2 = handle.clone();

        // Verify Send + Sync by moving to another task
        let join = tokio::spawn(async move { handle2.running_cartridges() });
        let result = join.await.unwrap();
        assert!(result.is_empty());

        // Original handle still works
        assert!(handle.running_cartridges().is_empty());
    }

    // TEST1253: Killing an unknown PID is accepted as an asynchronous no-op command.
    #[tokio::test]
    async fn test1253_process_handle_kill_unknown_pid_is_noop() {
        let runtime = CartridgeHostRuntime::new();
        let handle = runtime.process_handle();

        // Kill for a PID that doesn't exist should succeed (command sent)
        // but do nothing (the run loop would handle it as a no-op).
        // Since run() hasn't been called, the command sits in the channel.
        let result = handle.kill_cartridge(99999);
        assert!(
            result.is_ok(),
            "kill_cartridge should succeed even if PID is unknown — command is async"
        );
    }

    // TEST1254: OOM shutdowns emit OOM_KILLED ERR frames for in-flight requests.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "OOM death detection for attached cartridges not yet implemented"]
    async fn test1254_oom_kill_sends_err_with_oom_killed_code() {
        let manifest = r#"{"name":"OomCartridge","version":"1.0","channel":"release","registry_url":null,"description":"OOM test","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";oom;out=\"media:void\"","title":"OOM","aliases": ["oom"],"args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair
        let (p_to_rt, rt_from_p) = UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = UnixStream::pair().unwrap();

        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (mut r, w) = cartridge_handshake_with_identity(p_from_rt, p_to_rt, &m).await;

            // Read REQ and body END, then die (simulating OOM kill mid-flight)
            let _req = r.read().await.unwrap().expect("Expected REQ");
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::End {
                            break;
                        }
                    }
                    _ => break,
                }
            }
            // Die — OOM watchdog killed us
            drop(w);
            drop(r);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(p_read, p_write).await.unwrap();

        // Set shutdown_reason to OomKill BEFORE the cartridge dies.
        // In production this is set by handle_command(KillCartridge) which runs
        // in the event loop before child.kill(). For attached cartridges (no child
        // process), we set it directly.
        runtime.cartridges[0].shutdown_reason = Some(ShutdownReason::OomKill);

        // Relay pipe pair
        let (relay_rt_read, relay_eng_write) = UnixStream::pair().unwrap();
        let (relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();

        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();
        let (_, eng_write_half) = relay_eng_write.into_split();
        let (eng_read_half, _) = relay_eng_read.into_split();

        let req_id = MessageId::new_uuid();
        let req_id_clone = req_id.clone();
        let engine_task = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let mut w = FrameWriter::new(eng_write_half);
            let mut r = FrameReader::new(eng_read_half);

            let xid = MessageId::Uint(1);
            // Send REQ
            let mut req = Frame::req(
                req_id_clone.clone(),
                "cap:in=\"media:void\";oom;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let mut end = Frame::end(req_id_clone.clone(), None);
            end.routing_id = Some(xid.clone());
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: req_id_clone.clone(),
                xid: Some(xid),
            });

            // Read frames from relay — should get ERR with OOM_KILLED
            let mut got_oom_err = false;
            loop {
                match tokio::time::timeout(Duration::from_secs(5), r.read()).await {
                    Ok(Ok(Some(frame))) => {
                        if frame.frame_type == FrameType::Err {
                            let code = frame.error_code().unwrap_or("");
                            let msg = frame.error_message().unwrap_or("");
                            assert_eq!(
                                code, "OOM_KILLED",
                                "ERR code must be OOM_KILLED, got: {:?}",
                                code
                            );
                            assert!(
                                msg.contains("OOM watchdog"),
                                "ERR message must mention OOM watchdog, got: {}",
                                msg
                            );
                            got_oom_err = true;
                            break;
                        }
                        // Skip other frames (e.g. RelayNotify for cap rebuild)
                    }
                    Ok(Ok(None)) => break, // EOF
                    Ok(Err(_)) => break,   // Read error
                    Err(_) => panic!(
                        "Timed out waiting for OOM_KILLED ERR frame — this is the bug we're fixing"
                    ),
                }
            }
            assert!(
                got_oom_err,
                "Must receive ERR frame with OOM_KILLED code after OOM kill"
            );

            drop(w); // Close relay to let runtime exit
        });

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            runtime.run(rt_read_half, rt_write_half, || vec![]),
        )
        .await;
        assert!(result.is_ok(), "Runtime should exit cleanly");

        engine_task.await.unwrap();
        cartridge_handle.await.unwrap();
    }

    // TEST1255: App-exit shutdowns suppress ERR frames and close cleanly without noise.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test1255_app_exit_suppresses_err_frames() {
        let manifest = r#"{"name":"ExitCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Exit test","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";exit;out=\"media:void\"","title":"Exit","aliases": ["exit"],"args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair
        let (p_to_rt, rt_from_p) = UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = UnixStream::pair().unwrap();

        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (mut r, w) = cartridge_handshake_with_identity(p_from_rt, p_to_rt, &m).await;

            // Read REQ and body END, then die
            let _req = r.read().await.unwrap().expect("Expected REQ");
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::End {
                            break;
                        }
                    }
                    _ => break,
                }
            }
            drop(w);
            drop(r);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(p_read, p_write).await.unwrap();

        // Set AppExit — should suppress ERR frames
        runtime.cartridges[0].shutdown_reason = Some(ShutdownReason::AppExit);

        // Relay pipe pair
        let (relay_rt_read, relay_eng_write) = UnixStream::pair().unwrap();
        let (relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();

        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();
        let (_, eng_write_half) = relay_eng_write.into_split();
        let (eng_read_half, _) = relay_eng_read.into_split();

        let req_id = MessageId::new_uuid();
        let req_id_clone = req_id.clone();
        let engine_task = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let mut w = FrameWriter::new(eng_write_half);
            let mut r = FrameReader::new(eng_read_half);

            let xid = MessageId::Uint(1);
            let mut req = Frame::req(
                req_id_clone.clone(),
                "cap:in=\"media:void\";exit;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let mut end = Frame::end(req_id_clone.clone(), None);
            end.routing_id = Some(xid.clone());
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: req_id_clone.clone(),
                xid: Some(xid),
            });

            // Read frames — should NOT get any ERR frame.
            // We expect only RelayNotify (cap table rebuild) and then EOF.
            loop {
                match tokio::time::timeout(Duration::from_secs(3), r.read()).await {
                    Ok(Ok(Some(frame))) => {
                        assert_ne!(
                            frame.frame_type,
                            FrameType::Err,
                            "AppExit must suppress ERR frames, but got ERR with code={:?} msg={:?}",
                            frame.error_code(),
                            frame.error_message()
                        );
                        // Continue reading (might get RelayNotify)
                    }
                    Ok(Ok(None)) => break, // EOF — expected
                    Ok(Err(_)) => break,   // Read error — relay closed
                    Err(_) => break,       // Timeout — no more frames, good
                }
            }

            drop(w);
        });

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            runtime.run(rt_read_half, rt_write_half, || vec![]),
        )
        .await;
        assert!(result.is_ok(), "Runtime should exit cleanly");

        engine_task.await.unwrap();
        cartridge_handle.await.unwrap();
    }

    // -------------------------------------------------------------
    // Routing-table GC contract tests
    //
    // Mirror the Swift `CartridgeHostRoutingTableGCTests` in
    // capdag-objc. Pin down two invariants that protect the host's
    // routing tables from unbounded growth:
    //
    //   1. CAP IS ENFORCED. When the soft watermark is crossed,
    //      the GC fires and reduces the table size. After enough
    //      passes — at most one per insertion — no routing table
    //      can exceed the hard cap. Failure means a cartridge or
    //      relay path could create RIDs faster than the cleanup
    //      paths drain them, regressing the leak class we just
    //      fixed in capdag-objc.
    //
    //   2. EVICTION IS ORDERED BY touch-sequence, OLDEST FIRST.
    //      A still-active flow (one that has been routed through
    //      recently) must NOT be evicted before a stale one. A
    //      regression where the GC drops dictionary-iteration-
    //      order victims would still pass invariant #1 but fail
    //      this one — and dropping fresh entries silently kills
    //      in-flight continuation frames.
    // -------------------------------------------------------------

    /// Direct-seed helper: insert `count` synthetic
    /// `incoming_rxids` entries with deterministic touch
    /// sequences. Returns the keys in insertion order so the
    /// test can compute the expected victim/survivor sets.
    fn seed_incoming_rxids_for_test(
        runtime: &mut CartridgeHostRuntime,
        count: usize,
    ) -> Vec<(MessageId, MessageId)> {
        let mut keys = Vec::with_capacity(count);
        for i in 0..count {
            let xid = MessageId::Uint(i as u64);
            let rid = MessageId::Uint(i as u64);
            let key = (xid, rid);
            runtime.incoming_rxids.insert(key.clone(), 0);
            // Bypass `touch_incoming_rxid` so we can assign a
            // deterministic age. Production paths always go
            // through `touch_*` which uses the monotonic
            // `routing_touch_seq` counter — but that doesn't
            // give the test control over which entry is
            // "oldest." Direct-seeding the touched map with the
            // insertion index produces the same ordering the
            // production counter would have produced if entries
            // had been inserted at exactly these times.
            runtime.incoming_rxids_touched.insert(key.clone(), i as u64);
            keys.push(key);
        }
        keys
    }

    /// Contract #1 — the GC keeps the table strictly below the
    /// hard cap. Seed the table well above the soft watermark
    /// (matching what a runaway producer would do mid-frame-
    /// burst) and call the production GC entry point. The
    /// post-state must be at most `SOFT_WATERMARK` entries
    /// because the GC drops at least
    /// `EVICTION_FRACTION × pre_state` entries in one pass and
    /// the pre-state is below the hard cap (i.e. one pass is
    /// enough; the secondary "hard cap" pass would only fire if
    /// pre-state crossed the hard cap before insertion completed,
    /// which production prevents by gc-ing on every insert).
    #[test]
    fn test988_gc_reduces_table_below_soft_watermark_in_one_pass() {
        let mut runtime = CartridgeHostRuntime::new();
        let pre_count = CartridgeHostRuntime::ROUTING_TABLE_SOFT_WATERMARK + 256;
        assert!(
            pre_count < CartridgeHostRuntime::ROUTING_TABLE_HARD_CAP,
            "Test precondition: pre_count must stay under the hard cap so we verify \
             the SOFT watermark path, not the secondary hard-cap pass."
        );

        seed_incoming_rxids_for_test(&mut runtime, pre_count);
        assert_eq!(
            runtime.incoming_rxids.len(),
            pre_count,
            "Seeder must populate exactly pre_count entries before the GC runs"
        );

        runtime.gc_routing_tables_if_needed();

        assert!(
            runtime.incoming_rxids.len() < CartridgeHostRuntime::ROUTING_TABLE_HARD_CAP,
            "Post-GC table size {} must stay strictly under the hard cap ({}). \
             If this fires, the GC is not evicting enough to recover headroom — \
             the routing table can grow unbounded between GC firings.",
            runtime.incoming_rxids.len(),
            CartridgeHostRuntime::ROUTING_TABLE_HARD_CAP
        );
        assert_eq!(
            runtime.routing_gc_runs_total, 1,
            "Exactly one GC pass should have fired; {} runs means the single-pass \
             invariant has changed.",
            runtime.routing_gc_runs_total
        );
        let expected_evicted = std::cmp::max(
            1,
            (pre_count as f64 * CartridgeHostRuntime::ROUTING_TABLE_GC_EVICTION_FRACTION) as usize,
        );
        assert_eq!(
            runtime.routing_gc_evicted_total as usize,
            expected_evicted,
            "GC pass evicted {} entries; expected {} (eviction fraction {} of pre_count {}).",
            runtime.routing_gc_evicted_total,
            expected_evicted,
            CartridgeHostRuntime::ROUTING_TABLE_GC_EVICTION_FRACTION,
            pre_count
        );
    }

    /// Contract #2 — the GC drops the OLDEST entries by
    /// touch-sequence, not arbitrary keys. Seed a known age
    /// distribution and assert the post-GC keyset is exactly
    /// what the test computes should survive (test recomputes
    /// independently of production code).
    ///
    /// A regression where the GC e.g. iterates the HashMap and
    /// drops the first N (HashMap iteration order is arbitrary
    /// in Rust) would still pass contract #1 but fail this one —
    /// the more dangerous bug because it silently drops
    /// in-flight continuation frames.
    #[test]
    fn test0129_gc_evicts_oldest_entries_by_touch_sequence() {
        let mut runtime = CartridgeHostRuntime::new();
        let pre_count = CartridgeHostRuntime::ROUTING_TABLE_SOFT_WATERMARK + 256;
        let eviction_count = std::cmp::max(
            1,
            (pre_count as f64 * CartridgeHostRuntime::ROUTING_TABLE_GC_EVICTION_FRACTION) as usize,
        );

        // Seed: key i has touched_at == i. Smallest i means oldest.
        // Expected victims: keys 0 ..< eviction_count.
        // Expected survivors: keys eviction_count ..< pre_count.
        let keys = seed_incoming_rxids_for_test(&mut runtime, pre_count);

        runtime.gc_routing_tables_if_needed();

        for (i, key) in keys.iter().enumerate().take(eviction_count) {
            assert!(
                !runtime.incoming_rxids.contains_key(key),
                "Key index {} should have been evicted (touched_at={}, one of the {} \
                 oldest), but it survived the GC. The eviction-by-age contract has \
                 regressed; the GC is choosing victims by something other than \
                 touched_at.",
                i,
                i,
                eviction_count
            );
            assert!(
                !runtime.incoming_rxids_touched.contains_key(key),
                "Touched-map entry for key index {} must be removed alongside the \
                 primary entry; it lingering means the touched map can grow past \
                 the primary table size.",
                i
            );
        }
        for (i, key) in keys.iter().enumerate().skip(eviction_count) {
            assert!(
                runtime.incoming_rxids.contains_key(key),
                "Key index {} should have survived the GC (touched_at={}, one of the \
                 {} most-recently-touched), but was evicted. The eviction-by-age \
                 contract has regressed; the GC is dropping fresh entries before \
                 stale ones.",
                i,
                i,
                pre_count - eviction_count
            );
        }
    }

    /// Contract #3 — the secondary hard-cap pass kicks in if the
    /// table somehow exceeds `HARD_CAP` (extreme runaway). Without
    /// it, a single GC at the soft watermark would not be enough
    /// to recover headroom and the table could grow without bound
    /// between bursts.
    #[test]
    fn test987_gc_secondary_pass_enforces_hard_cap() {
        let mut runtime = CartridgeHostRuntime::new();
        // Size the seed so a SINGLE eviction-fraction pass is NOT
        // enough to bring the table under the hard cap. We need
        // `pre * (1 - eviction_fraction) >= hard_cap`, i.e.
        // `pre >= hard_cap / (1 - eviction_fraction)`. With
        // hard_cap=8192, eviction_fraction=0.25 that's pre >=
        // 10923. Add 256 of headroom so a small change to the
        // eviction fraction doesn't accidentally make the test
        // pass via the primary pass alone.
        let one_minus_fraction = 1.0 - CartridgeHostRuntime::ROUTING_TABLE_GC_EVICTION_FRACTION;
        let pre_count = (CartridgeHostRuntime::ROUTING_TABLE_HARD_CAP as f64 / one_minus_fraction)
            .ceil() as usize
            + 256;
        seed_incoming_rxids_for_test(&mut runtime, pre_count);
        assert!(
            runtime.incoming_rxids.len() >= CartridgeHostRuntime::ROUTING_TABLE_HARD_CAP,
            "Seeder must populate at or above the hard cap so the secondary pass \
             actually fires. If this assertion fires, the test setup is wrong."
        );

        runtime.gc_routing_tables_if_needed();

        assert!(
            runtime.incoming_rxids.len() < CartridgeHostRuntime::ROUTING_TABLE_HARD_CAP,
            "Post-GC table size {} must be strictly under the hard cap ({}). The \
             secondary pass exists precisely to catch the case where one \
             eviction-fraction pass isn't enough; if this fails, that pass is broken.",
            runtime.incoming_rxids.len(),
            CartridgeHostRuntime::ROUTING_TABLE_HARD_CAP
        );
        // The secondary pass logs a separate `tracing::error` line
        // (and uses the same `routing_gc_evicted_total` counter)
        // but does not increment `routing_gc_runs_total`. We
        // verify the eviction count instead, which must exceed
        // one full eviction-fraction pass over the pre-count.
        let single_pass_max =
            (pre_count as f64 * CartridgeHostRuntime::ROUTING_TABLE_GC_EVICTION_FRACTION) as u64;
        assert!(
            runtime.routing_gc_evicted_total > single_pass_max,
            "Total evicted {} should exceed single-pass max {} (the secondary pass \
             must have evicted additional entries). If equal, the secondary pass \
             didn't fire.",
            runtime.routing_gc_evicted_total,
            single_pass_max
        );
    }

    // TEST1950: A roster sync REPLACES both halves of the discovery picture — an install advertised as rejected becomes attachable, with its failure reason gone and its caps routable, and is never advertised twice.
    #[tokio::test]
    async fn test1950_sync_roster_clears_a_rejected_install_that_became_attachable() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("cartridge.json"),
            r#"{"name":"heldcart","version":"1.0.0","channel":"release","registry_url":null,"entry":"bin","installed_at":"2026-01-01T00:00:00Z","installed_from":"dev"}"#,
        )
        .unwrap();
        let entry = dir.path().join("bin");
        std::fs::write(&entry, b"#!/bin/sh\n").unwrap();

        let mut runtime = CartridgeHostRuntime::new();

        // The state a held cartridge is discovered in: no verdict yet, so the
        // install is rejected and rides the advertisement with its reason.
        runtime.set_static_inventory_records(vec![InstalledCartridgeRecord {
            registry_url: None,
            channel: crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            id: "heldcart".to_string(),
            version: "1.0.0".to_string(),
            sha256: String::new(),
            cap_groups: Vec::new(),
            attachment_error: Some(CartridgeAttachmentError {
                kind: CartridgeAttachmentErrorKind::RegistryUnreachable,
                message: "registry verdict unavailable".to_string(),
                detected_at_unix_seconds: 0,
            }),
            runtime_stats: None,
            lifecycle: CartridgeLifecycle::Discovered,
        }]);

        let records = runtime.build_installed_cartridge_identities();
        assert_eq!(records.len(), 1, "the held install is advertised");
        assert_eq!(
            records[0]
                .attachment_error
                .as_ref()
                .expect("a held install names its reason")
                .kind,
            CartridgeAttachmentErrorKind::RegistryUnreachable
        );

        // The verdict arrives: the same install re-discovers as attachable, so
        // the sync carries it as a spec and NO LONGER as a rejected record.
        let (tx, _rx) = mpsc::unbounded_channel::<Frame>();
        runtime
            .sync_registered_roster(
                vec![RegisteredDirSpec {
                    entry_point: entry.clone(),
                    version_dir: dir.path().to_path_buf(),
                    id: "heldcart".to_string(),
                    channel: crate::bifaci::cartridge_repo::CartridgeChannel::Release,
                    registry_url: None,
                    version: "1.0.0".to_string(),
                    cap_groups: cap_groups_from_urns(&[
                        "cap:in=\"media:void\";held;out=\"media:void\"",
                    ]),
                }],
                Vec::new(),
                &tx,
            )
            .await
            .expect("roster sync must succeed");

        let records = runtime.build_installed_cartridge_identities();
        assert_eq!(
            records.len(),
            1,
            "the install is advertised ONCE — a stale rejected record beside \
             the live one would report a failure for a cartridge that serves"
        );
        assert_eq!(records[0].id, "heldcart");
        assert!(
            records[0].attachment_error.is_none(),
            "the rejection must be gone: it is the reason the operator is told \
             the cartridge cannot attach, and it no longer holds"
        );
        assert!(
            !records[0].cap_groups.is_empty(),
            "an attachable install advertises its caps so the engine can route"
        );
    }

    // TEST7089: A cartridge whose HELLO permanently failed stays IN the inventory advertisement carrying a handshake_failed attachment error and no cap groups — failure is named, never silently absent; a roster-retired cartridge disappears entirely.
    #[tokio::test]
    async fn test7089_hello_failed_stays_in_inventory_with_error() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("cartridge.json"),
            r#"{"name":"stalecart","version":"1.0.0","channel":"release","registry_url":null,"entry":"bin","installed_at":"2026-01-01T00:00:00Z","installed_from":"dev"}"#,
        )
        .unwrap();
        let entry = dir.path().join("bin");
        std::fs::write(&entry, b"#!/bin/sh\n").unwrap();

        let mut runtime = CartridgeHostRuntime::new();
        runtime.register_cartridge_dir(
            &entry,
            dir.path(),
            "stalecart",
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            "1.0.0",
            &[],
        );

        // Healthy (never spawned): advertised without an attachment error.
        let records = runtime.build_installed_cartridge_identities();
        assert_eq!(records.len(), 1);
        assert!(records[0].attachment_error.is_none());

        // HELLO permanently fails (e.g. a pre-v4 binary rejected by the
        // version check): the record STAYS, carrying the failure — the UI
        // must always be able to name why a cartridge is not serving.
        runtime.cartridges[0].hello_failed = true;
        let records = runtime.build_installed_cartridge_identities();
        assert_eq!(
            records.len(),
            1,
            "a hello-failed cartridge must remain in the inventory (never silent)"
        );
        let record = &records[0];
        assert_eq!(record.id, "stalecart");
        assert!(record.cap_groups.is_empty(), "failed ⇒ never routable");
        let error = record
            .attachment_error
            .as_ref()
            .expect("failure must be named on the record");
        assert_eq!(
            error.kind,
            CartridgeAttachmentErrorKind::HandshakeFailed,
            "the failure kind identifies the handshake as the cause"
        );

        // Static inventory records (discovery outcomes the host doesn't
        // manage) ride every advertisement too.
        runtime.set_static_inventory_records(vec![InstalledCartridgeRecord {
            registry_url: None,
            channel: crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            id: "rejectedcart".to_string(),
            version: "2.0.0".to_string(),
            sha256: String::new(),
            cap_groups: Vec::new(),
            attachment_error: Some(CartridgeAttachmentError {
                kind: CartridgeAttachmentErrorKind::Incompatible,
                message: "version not listed in registry".to_string(),
                detected_at_unix_seconds: 1,
            }),
            runtime_stats: None,
            lifecycle: CartridgeLifecycle::Discovered,
        }]);
        let records = runtime.build_installed_cartridge_identities();
        assert_eq!(
            records.len(),
            2,
            "static inventory merges into every advertisement"
        );
        assert!(records.iter().any(|r| r.id == "rejectedcart"));

        // Roster retirement is NOT a failure: a removed cartridge disappears
        // from the inventory entirely (there is nothing to report).
        runtime.cartridges[0].removed = true;
        let records = runtime.build_installed_cartridge_identities();
        assert_eq!(
            records.len(),
            1,
            "retired installs vanish from the inventory"
        );
        assert_eq!(records[0].id, "rejectedcart");
    }

    // TEST7090: The cartridge's cumulative protocol drop counter (`drops_total`
    // heartbeat meta, L8) is ingested by the host and surfaces on the
    // cartridge's inventory runtime stats as `protocol_drops_total` — absent
    // until the first reading, then tracking the running total as-is.
    #[tokio::test]
    async fn test7090_heartbeat_drops_total_reaches_inventory_stats() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("cartridge.json"),
            r#"{"name":"dropcart","version":"1.0.0","channel":"release","registry_url":null,"entry":"bin","installed_at":"2026-01-01T00:00:00Z","installed_from":"dev"}"#,
        )
        .unwrap();
        let entry = dir.path().join("bin");
        std::fs::write(&entry, b"#!/bin/sh\n").unwrap();

        let mut runtime = CartridgeHostRuntime::new();
        runtime.register_cartridge_dir(
            &entry,
            dir.path(),
            "dropcart",
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            "1.0.0",
            &[],
        );

        // No heartbeat round-trip yet: the reading must be ABSENT, never a
        // fabricated zero (a zero claims "measured: no drops").
        let records = runtime.build_installed_cartridge_identities();
        let stats = records[0]
            .runtime_stats
            .as_ref()
            .expect("inventory records always carry runtime stats");
        assert!(
            stats.protocol_drops_total.is_none(),
            "no reading before the first heartbeat round-trip"
        );

        // Heartbeat response to our pending probe, carrying the cartridge's
        // running drop total exactly as cartridge_runtime emits it.
        let (outbound_tx, _outbound_rx) = mpsc::unbounded_channel();
        let hb_id = MessageId::new_uuid();
        runtime.cartridges[0]
            .pending_heartbeats
            .insert(hb_id.clone(), std::time::Instant::now().into());
        let mut response = Frame::heartbeat(hb_id);
        let mut meta = std::collections::BTreeMap::new();
        meta.insert("drops_total".into(), ciborium::Value::Integer(42.into()));
        meta.insert(
            "stragglers_total".into(),
            ciborium::Value::Integer(7.into()),
        );
        meta.insert(
            crate::bifaci::pools::META_POOLS.into(),
            ciborium::Value::Bytes(crate::bifaci::pools::encode_pool_states(
                &crate::bifaci::pools::PoolStates::new(),
            )),
        );
        response.meta = Some(meta);
        runtime
            .handle_cartridge_frame(0, response, &outbound_tx)
            .expect("heartbeat response must be handled locally");

        let records = runtime.build_installed_cartridge_identities();
        let stats = records[0].runtime_stats.as_ref().unwrap();
        assert_eq!(
            stats.protocol_drops_total,
            Some(42),
            "the heartbeat's drops_total must reach the inventory stats"
        );
        assert_eq!(
            stats.protocol_stragglers_total,
            Some(7),
            "the heartbeat's stragglers_total rides alongside, under its own name —              benign stragglers are never folded into drops"
        );

        // A later heartbeat carries a larger running total — stored as-is.
        let hb_id = MessageId::new_uuid();
        runtime.cartridges[0]
            .pending_heartbeats
            .insert(hb_id.clone(), std::time::Instant::now().into());
        let mut response = Frame::heartbeat(hb_id);
        let mut meta = std::collections::BTreeMap::new();
        meta.insert("drops_total".into(), ciborium::Value::Integer(45.into()));
        meta.insert(
            crate::bifaci::pools::META_POOLS.into(),
            ciborium::Value::Bytes(crate::bifaci::pools::encode_pool_states(
                &crate::bifaci::pools::PoolStates::new(),
            )),
        );
        response.meta = Some(meta);
        runtime
            .handle_cartridge_frame(0, response, &outbound_tx)
            .expect("heartbeat response must be handled locally");
        let records = runtime.build_installed_cartridge_identities();
        assert_eq!(
            records[0]
                .runtime_stats
                .as_ref()
                .unwrap()
                .protocol_drops_total,
            Some(45),
        );
    }

    /// A registered-dir cartridge, marked running, for roster-retire tests.
    fn retire_fixture() -> (CartridgeHostRuntime, tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("cartridge.json"),
            r#"{"name":"retiring","version":"1.0.0","channel":"release","registry_url":null,"entry":"bin","installed_at":"2026-01-01T00:00:00Z","installed_from":"dev"}"#,
        )
        .unwrap();
        let entry = dir.path().join("bin");
        std::fs::write(&entry, b"#!/bin/sh\n").unwrap();

        let mut runtime = CartridgeHostRuntime::new();
        runtime.register_cartridge_dir(
            &entry,
            dir.path(),
            "retiring",
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            "1.0.0",
            &cap_groups_from_urns(&["cap:in=\"media:void\";out=\"media:void\";retiring"]),
        );
        // Pretend it started: retirement only has to make a decision about a
        // LIVE process.
        runtime.cartridges[0].running = true;
        (runtime, dir, entry)
    }

    // TEST1945: a roster retire DRAINS a busy cartridge instead of killing it.
    //
    // The incident this pins: a transient registry outage shrank the roster and
    // the host killed three cartridges outright, ERRing every request they were
    // serving. Retirement means "no NEW work" — the process must survive until
    // the requests it is already handling terminate.
    #[tokio::test]
    async fn test1945_roster_retire_drains_a_busy_cartridge_before_killing_it() {
        let (mut runtime, _dir, _entry) = retire_fixture();
        let (outbound_tx, _outbound_rx) = mpsc::unbounded_channel();

        // One request in flight on this cartridge.
        runtime.incoming_rxids.insert(
            (MessageId::Uint(1), MessageId::Uint(2)),
            0,
        );

        runtime.sync_registered_roster(vec![], Vec::new(), &outbound_tx).await.unwrap();

        assert!(
            runtime.cartridges[0].removed && runtime.cartridges[0].hello_failed,
            "a retired cartridge must leave the cap table and inventory immediately"
        );
        assert!(
            runtime.cartridges[0].retiring_since.is_some(),
            "a busy retired cartridge must be marked draining"
        );
        assert!(
            runtime.cartridges[0].shutdown_reason.is_none(),
            "a cartridge mid-request must not be killed by a roster change"
        );

        // Still busy → still alive.
        runtime.reap_drained_cartridges().await;
        assert!(runtime.cartridges[0].shutdown_reason.is_none());

        // The request terminates; the next reap collects it.
        runtime.incoming_rxids.remove(&(MessageId::Uint(1), MessageId::Uint(2)));
        runtime.reap_drained_cartridges().await;
        assert_eq!(
            runtime.cartridges[0].shutdown_reason,
            Some(ShutdownReason::RosterRetired),
            "a drained cartridge must be shut down as RETIRED — not as a cancellation"
        );
    }

    // TEST1947: a roster that flaps — retire then restore the same identity —
    // keeps the SAME live process. This is the incident's shape end to end: the
    // registry became unreachable, the roster shrank, and 26 seconds later it
    // came back. Nothing about that sequence should cost a running cartridge,
    // its warm model, or the work queued on it.
    #[tokio::test]
    async fn test1947_roster_flap_cancels_retirement_instead_of_respawning() {
        let (mut runtime, dir, entry) = retire_fixture();
        let (outbound_tx, _outbound_rx) = mpsc::unbounded_channel();
        let spec = RegisteredDirSpec {
            entry_point: entry,
            version_dir: dir.path().to_path_buf(),
            id: "retiring".to_string(),
            channel: crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            registry_url: None,
            version: "1.0.0".to_string(),
            cap_groups: cap_groups_from_urns(&[
                "cap:in=\"media:void\";out=\"media:void\";retiring",
            ]),
        };

        // Busy, so the outage puts it into a drain rather than killing it.
        runtime
            .incoming_rxids
            .insert((MessageId::Uint(1), MessageId::Uint(2)), 0);
        runtime.sync_registered_roster(vec![], Vec::new(), &outbound_tx).await.unwrap();
        assert!(runtime.cartridges[0].retiring_since.is_some());

        // The registry answers again and the roster is restored.
        runtime
            .sync_registered_roster(vec![spec], Vec::new(), &outbound_tx)
            .await
            .unwrap();

        assert_eq!(
            runtime.cartridges.len(),
            1,
            "the restored identity must reuse the draining process, not spawn a second one"
        );
        assert!(
            runtime.cartridges[0].retiring_since.is_none()
                && !runtime.cartridges[0].removed
                && !runtime.cartridges[0].hello_failed,
            "retirement must be cancelled outright, putting the cartridge back in dispatch"
        );
        assert!(
            runtime.cartridges[0].shutdown_reason.is_none(),
            "the process must never have been killed"
        );

        // And it is not reaped afterwards.
        runtime.incoming_rxids.clear();
        runtime.reap_drained_cartridges().await;
        assert!(runtime.cartridges[0].shutdown_reason.is_none());
    }

    // TEST1946: an IDLE cartridge is retired immediately (no reason to keep a
    // process nothing routes to), and its reason is RosterRetired so pending
    // work — and the operator-facing log — is attributed to the environment
    // rather than reported as a user cancellation.
    #[tokio::test]
    async fn test1946_roster_retire_kills_an_idle_cartridge_as_retired() {
        let (mut runtime, _dir, _entry) = retire_fixture();
        let (outbound_tx, _outbound_rx) = mpsc::unbounded_channel();

        runtime.sync_registered_roster(vec![], Vec::new(), &outbound_tx).await.unwrap();

        assert!(runtime.cartridges[0].retiring_since.is_none());
        assert_eq!(
            runtime.cartridges[0].shutdown_reason,
            Some(ShutdownReason::RosterRetired)
        );
    }

    // TEST1871: SyncRoster updates the LIVE host inventory in place — the engine
    // sees an added registered-dir cartridge via a fresh RelayNotify without
    // reconnecting, and a subsequent empty sync removes it. This is the
    // macOS-XPC `syncDiscoveryOutcomes` parity path the daemon uses after a
    // registry verdict flips a held cartridge to Listed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test1871_sync_roster_adds_and_removes_registered_dir_live() {
        // A valid registered-dir cartridge (hashable dir + cartridge.json).
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("cartridge.json"),
            r#"{"name":"latejoiner","version":"1.0.0","channel":"release","registry_url":null,"entry":"bin","installed_at":"2026-01-01T00:00:00Z","installed_from":"dev"}"#,
        )
        .unwrap();
        let entry = dir.path().join("bin");
        std::fs::write(&entry, b"#!/bin/sh\n").unwrap();

        let mut runtime = CartridgeHostRuntime::new();
        let handle = runtime.process_handle();

        // Relay pipe pair: the engine side reads RelayNotify frames the host emits.
        let (relay_rt_read, _relay_eng_write) = UnixStream::pair().unwrap();
        let (relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();
        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();
        let (eng_read_half, _) = relay_eng_read.into_split();

        // Engine side: collect the cartridge ids advertised across RelayNotify
        // frames over a short window, sending the two SyncRoster commands between.
        let entry_clone = entry.clone();
        let dir_path = dir.path().to_path_buf();
        let engine_task = tokio::spawn(async move {
            let mut r = FrameReader::new(eng_read_half);

            async fn read_notify_ids(r: &mut FrameReader<impl AsyncRead + Unpin>) -> Vec<String> {
                loop {
                    match r.read().await {
                        Ok(Some(f)) if f.frame_type == FrameType::RelayNotify => {
                            let bytes = f.relay_notify_manifest().unwrap_or_default();
                            let payload: RelayNotifyCapabilitiesPayload =
                                serde_json::from_slice(bytes).unwrap();
                            return payload
                                .installed_cartridges
                                .iter()
                                .map(|c| c.id.clone())
                                .collect();
                        }
                        Ok(Some(_)) => continue,
                        _ => return Vec::new(),
                    }
                }
            }

            // Initial RelayNotify (empty roster).
            let initial = read_notify_ids(&mut r).await;

            // Add the cartridge live.
            handle
                .sync_roster(
                    vec![RegisteredDirSpec {
                        entry_point: entry_clone,
                        version_dir: dir_path,
                        id: "latejoiner".to_string(),
                        channel: crate::bifaci::cartridge_repo::CartridgeChannel::Release,
                        registry_url: None,
                        version: "1.0.0".to_string(),
                        cap_groups: cap_groups_from_urns(&[
                            "cap:in=\"media:void\";late;out=\"media:void\"",
                        ]),
                    }],
                    Vec::new(),
                )
                .unwrap();
            let after_add = read_notify_ids(&mut r).await;

            // Remove it again (empty roster).
            handle.sync_roster(vec![], Vec::new()).unwrap();
            let after_remove = read_notify_ids(&mut r).await;

            (initial, after_add, after_remove)
        });

        // Drive the host until the engine side drops the relay (it returns after
        // collecting the three snapshots, which closes eng_read_half's peer).
        let run_task = tokio::spawn(async move {
            let _ = runtime.run(rt_read_half, rt_write_half, Vec::new).await;
        });

        let (initial, after_add, after_remove) = engine_task.await.unwrap();
        run_task.abort();

        assert!(
            !initial.contains(&"latejoiner".to_string()),
            "cartridge must be absent before the sync; got {initial:?}"
        );
        assert!(
            after_add.contains(&"latejoiner".to_string()),
            "SyncRoster must add the cartridge to the live inventory; got {after_add:?}"
        );
        assert!(
            !after_remove.contains(&"latejoiner".to_string()),
            "an empty SyncRoster must retire the cartridge; got {after_remove:?}"
        );
    }

    // TEST462: An attached cartridge (pre-connected over raw streams, no
    // on-disk anchor) gets a resolvable install identity derived from its
    // HELLO manifest — `installed_cartridge_record_from_manifest`. Identity
    // gates advertisement (`build_installed_cartridge_identities` drops a
    // cartridge with no record), so a `None` here means the cartridge is
    // silently dropped from every RelayNotify and the engine can never route
    // to it. Regression lock for the attached-cartridge identity path (the
    // swift mirror regressed here: its attached cartridges returned `nil` and
    // never reached the engine).
    #[test]
    fn test462_attached_cartridge_identity_from_manifest() {
        let manifest = br#"{"name":"TestCart","version":"1.2.3","channel":"nightly","registry_url":null,"description":"d","cap_groups":[{"name":"g","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"]}]}]}"#;

        let record = installed_cartridge_record_from_manifest(manifest)
            .expect("attached cartridge identity must be derivable from a valid manifest (else it is dropped from advertisement)");
        assert_eq!(record.id, "TestCart", "id comes from manifest name");
        assert_eq!(record.version, "1.2.3");
        assert!(matches!(
            record.channel,
            crate::bifaci::cartridge_repo::CartridgeChannel::Nightly
        ));
        assert_eq!(record.registry_url, None, "dev build → null registry_url");
        assert!(
            !record.sha256.is_empty(),
            "sha256 taken over manifest bytes"
        );
        // Attached ⇒ HELLO + identity verification already succeeded ⇒ operational.
        assert!(matches!(record.lifecycle, CartridgeLifecycle::Operational));

        // An unparseable manifest yields no record (honestly absent, not a
        // fabricated id) — the producer must surface the gap, not hide it.
        assert!(
            installed_cartridge_record_from_manifest(b"{not json").is_none(),
            "unparseable manifest must yield None, not a placeholder identity"
        );
    }

    // TEST8067: A late death notification from a retired process generation
    // cannot tear down its replacement. The current generation's heartbeat
    // timeout performs the complete death transition and preserves its typed
    // terminal for the request that process actually owned.
    // TEST1533: `apply_desired_capacities` is validated HARD against the
    // cartridge's last-known pool map — an unknown cartridge or pool name is
    // refused with the offender named and nothing queued — and a cold
    // cartridge keeps the validated values queued for the attach-time probe.
    #[tokio::test]
    async fn test1533_apply_desired_capacities_validation_and_cold_queue() {
        let mut runtime = CartridgeHostRuntime::new();
        let mut cartridge = ManagedCartridge::new_registered_binary(
            PathBuf::from("/nonexistent/test-cartridge"),
            "test-cartridge".to_string(),
            "1.0.0".to_string(),
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            cap_groups_from_urns(&["cap:in=\"media:void\";test;out=\"media:void\""]),
        );
        // The ctor derives identity by hashing the binary; a nonexistent
        // path honestly yields none. Desired-capacity addressing is BY
        // identity, so stamp the record this fixture stands for.
        cartridge.installed_identity = Some(crate::bifaci::relay_switch::InstalledCartridgeRecord {
            registry_url: None,
            channel: crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            id: "test-cartridge".to_string(),
            version: "1.0.0".to_string(),
            sha256: "00".repeat(32),
            cap_groups: Vec::new(),
            attachment_error: None,
            runtime_stats: None,
            lifecycle: crate::bifaci::relay_switch::CartridgeLifecycle::Operational,
        });
        cartridge.pool_states = crate::bifaci::pools::PoolStates::from([
            (
                "gpu".to_string(),
                crate::bifaci::pools::PoolState::declared(1, Vec::new()),
            ),
            (
                crate::bifaci::pools::POOL_ALL.to_string(),
                crate::bifaci::pools::PoolState::declared(0, Vec::new()),
            ),
        ]);
        runtime.cartridges.push(cartridge);

        let mut unknown_cartridge = crate::bifaci::pools::DesiredCapacities::new();
        unknown_cartridge.insert("gpu".to_string(), 2);
        let error = runtime
            .apply_desired_capacities("ghost-cartridge", &unknown_cartridge)
            .expect_err("an unknown cartridge must refuse");
        assert!(
            format!("{error:?}").contains("ghost-cartridge"),
            "the refusal must name the cartridge: {error:?}"
        );

        let mut unknown_pool = crate::bifaci::pools::DesiredCapacities::new();
        unknown_pool.insert("tpu".to_string(), 2);
        let error = runtime
            .apply_desired_capacities("test-cartridge", &unknown_pool)
            .expect_err("an unknown pool must refuse");
        assert!(
            format!("{error:?}").contains("tpu"),
            "the refusal must name the pool: {error:?}"
        );
        assert!(
            runtime.cartridges[0].pending_desired.is_empty(),
            "a refused batch must queue NOTHING"
        );

        let mut desired = crate::bifaci::pools::DesiredCapacities::new();
        desired.insert("gpu".to_string(), 2);
        runtime
            .apply_desired_capacities("test-cartridge", &desired)
            .expect("a valid batch against a cold cartridge must queue");
        assert_eq!(
            runtime.cartridges[0].pending_desired.get("gpu"),
            Some(&2),
            "a cold cartridge keeps the values queued for the attach-time probe"
        );
    }

    #[tokio::test]
    async fn test8067_heartbeat_timeout_is_generation_safe() {
        let mut runtime = CartridgeHostRuntime::new();
        let mut cartridge = ManagedCartridge::new_registered_binary(
            PathBuf::from("/nonexistent/test-cartridge"),
            "test-cartridge".to_string(),
            "1.0.0".to_string(),
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            cap_groups_from_urns(&["cap:in=\"media:void\";test;out=\"media:void\""]),
        );
        cartridge.running = true;
        cartridge.generation = 7;
        cartridge.pool_states = crate::bifaci::pools::PoolStates::from([(
            crate::bifaci::pools::POOL_ALL.to_string(),
            crate::bifaci::pools::PoolState::declared(1, Vec::new()),
        )]);
        runtime.cartridges.push(cartridge);

        let xid = MessageId::Uint(41);
        let rid = MessageId::Uint(42);
        runtime.incoming_rxids.insert((xid.clone(), rid.clone()), 0);

        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();

        runtime
            .handle_cartridge_death(0, 6, &outbound_tx)
            .await
            .expect("stale death event must be ignored");
        assert!(runtime.cartridges[0].running);
        assert_eq!(runtime.cartridges[0].generation, 7);
        assert!(outbound_rx.try_recv().is_err());

        runtime.cartridges[0].shutdown_reason = Some(ShutdownReason::HeartbeatTimeout);
        runtime
            .handle_cartridge_death(0, 7, &outbound_tx)
            .await
            .expect("current generation must be retired");

        assert!(!runtime.cartridges[0].running);
        assert_eq!(runtime.cartridges[0].generation, 8);
        assert_eq!(runtime.cartridges[0].restart_count, 1);
        assert!(!runtime.incoming_rxids.contains_key(&(xid, rid)));

        let frames: Vec<Frame> = std::iter::from_fn(|| outbound_rx.try_recv().ok()).collect();
        let terminal = frames
            .iter()
            .find(|frame| frame.frame_type == FrameType::Err)
            .expect("heartbeat timeout must emit a terminal ERR");
        assert_eq!(terminal.error_code(), Some("CARTRIDGE_UNHEALTHY"));
        assert_eq!(
            terminal.error_message(),
            Some("Cartridge stopped responding to heartbeats")
        );
    }

    // TEST8116: the terminal-release ring discriminates and stays bounded —
    // released rids classify as benign-straggler material, unknown rids do not,
    // duplicates collapse, and eviction past the cap ages a rid back out.
    #[test]
    fn test8116_released_rid_ring_discriminates_dedupes_and_ages_out() {
        let mut runtime = CartridgeHostRuntime::new();
        let rid = MessageId::Uint(7);

        assert!(!runtime.recently_released_rid(&rid), "nothing released yet");
        runtime.note_released_rid(&rid);
        runtime.note_released_rid(&rid); // duplicate must collapse
        assert!(runtime.recently_released_rid(&rid));
        assert_eq!(
            runtime.recent_released_rids.len(),
            1,
            "duplicate releases collapse to one ring entry"
        );
        assert!(
            !runtime.recently_released_rid(&MessageId::Uint(9999)),
            "a rid never released here is a genuine anomaly"
        );

        for n in 100..(100 + CartridgeHostRuntime::RECENT_RELEASED_RIDS_CAP as u64) {
            runtime.note_released_rid(&MessageId::Uint(n));
        }
        assert!(
            !runtime.recently_released_rid(&rid),
            "eviction past RECENT_RELEASED_RIDS_CAP ends benign-straggler classification"
        );
        assert_eq!(
            runtime.recent_released_rids.len(),
            CartridgeHostRuntime::RECENT_RELEASED_RIDS_CAP,
            "the ring is bounded"
        );
    }

    // TEST8117: an unroutable continuation from the relay is classified by
    // the release ring — a rid a terminal just released is a BENIGN
    // post-terminal straggler (counted per frame type, never a drop);
    // a rid this host never routed is a genuine no_route DROP. Both are
    // counted (L8), never errors and never silent — and never conflated.
    #[tokio::test]
    async fn test8117_unroutable_continuation_classified_by_release_ring() {
        let mut runtime = CartridgeHostRuntime::new();
        let (outbound_tx, _outbound_rx) = mpsc::unbounded_channel();
        let resource_fn = || Vec::new();

        // Unknown rid: no routing entry, nothing released → no_route drop.
        let mut unknown = Frame::new(FrameType::Chunk, MessageId::Uint(41));
        unknown.routing_id = Some(MessageId::Uint(4));
        unknown.stream_id = Some("s".to_string());
        unknown.chunk_index = Some(0);
        unknown.checksum = Some(0);
        runtime
            .handle_relay_frame(unknown, &outbound_tx, &resource_fn)
            .await
            .expect("unroutable frame must not error (L6)");
        assert_eq!(
            runtime.drops.get(crate::bifaci::frame::DropReason::NoRoute),
            1,
            "a rid never routed here is a routing anomaly"
        );
        assert_eq!(
            runtime.stragglers.total(),
            0,
            "a genuine anomaly is never counted as a benign straggler"
        );

        // Released rid: the same frame after a terminal released the route →
        // a benign straggler, counted per frame type; NO drop counter moves.
        runtime.note_released_rid(&MessageId::Uint(42));
        let mut straggler = Frame::new(FrameType::Chunk, MessageId::Uint(42));
        straggler.routing_id = Some(MessageId::Uint(4));
        straggler.stream_id = Some("s".to_string());
        straggler.chunk_index = Some(0);
        straggler.checksum = Some(0);
        runtime
            .handle_relay_frame(straggler, &outbound_tx, &resource_fn)
            .await
            .expect("post-terminal straggler must not error (L6)");
        assert_eq!(
            runtime.stragglers.get(FrameType::Chunk),
            1,
            "a released rid's straggler is the benign teardown race, named by frame type"
        );
        assert_eq!(
            runtime.drops.total(),
            1,
            "the drop counters must not absorb benign teardown races"
        );

        // A LOG with no routing entry follows the same law — counted, never
        // silent: released rid → benign straggler, unknown rid → no_route drop.
        let log_released = Frame::progress(MessageId::Uint(42), 0.5, "late log");
        runtime
            .handle_relay_frame(log_released, &outbound_tx, &resource_fn)
            .await
            .expect("unroutable LOG must not error");
        assert_eq!(runtime.stragglers.get(FrameType::Log), 1);
        assert_eq!(runtime.stragglers.total(), 2);
        let log_unknown = Frame::progress(MessageId::Uint(43), 0.5, "alien log");
        runtime
            .handle_relay_frame(log_unknown, &outbound_tx, &resource_fn)
            .await
            .expect("unroutable LOG must not error");
        assert_eq!(
            runtime.drops.get(crate::bifaci::frame::DropReason::NoRoute),
            2
        );
    }
}

