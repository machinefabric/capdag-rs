//! RelaySwitch — Cap-aware routing multiplexer for multiple RelayMasters
//!
//! The RelaySwitch sits above multiple RelayMasters and provides deterministic
//! request routing based on cap URN matching. It plays the same role for RelayMasters
//! that CartridgeHost plays for cartridges.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────┐
//! │   Test Engine / API Client  │
//! └──────────────┬──────────────┘
//!                │
//! ┌──────────────▼──────────────┐
//! │       RelaySwitch            │
//! │  • Aggregates capabilities   │
//! │  • Routes REQ by cap URN     │
//! │  • Routes frames by req_id   │
//! │  • Tracks peer requests      │
//! └─┬───┬───┬───┬───────────────┘
//!   │   │   │   │
//!   ▼   ▼   ▼   ▼
//!  RM  RM  RM  RM   (RelayMasters)
//!   │   │   │   │
//!   ▼   ▼   ▼   ▼
//!  RS  RS  RS  RS   (RelaySlaves)
//!   │   │   │   │
//!   ▼   ▼   ▼   ▼
//!  PH  PH  PH  PH   (CartridgeHosts)
//! ```
//!
//! ## Routing Rules
//!
//! **Engine → Cartridge**:
//! - REQ: route by cap URN using `is_dispatchable` + closest-specificity matching
//! - Continuation frames: route by req_id
//!
//! **Cartridge → Cartridge** (peer invocations):
//! - REQ from master: route to destination master (may be same or different)
//! - Mark is_peer on the unified request entry (special cleanup semantics)
//! - Response frames: route back to source master
//!
//! **Cleanup**:
//! - Engine-initiated: cartridge's END → cleanup immediately
//! - Peer-initiated: engine's response END → cleanup (wait for final response)
//!
//! ## Concurrency Model
//!
//! RelaySwitch uses interior mutability to allow concurrent DAG executions:
//! - All methods take `&self` (not `&mut self`)
//! - xid_counter: AtomicU64
//! - Routing maps: RwLock<HashMap<...>>
//! - Per-master socket writers: Mutex<FrameWriter<...>>
//! - Each execute_fanin spawns its own pump that cooperatively routes frames

use crate::bifaci::frame::{FlowKey, Frame, FrameType, Limits, MessageId, SeqAssigner};
use crate::bifaci::io::{identity_nonce, CborError, FrameReader, FrameWriter};
use crate::bifaci::local_socket::{OwnedReadHalf, OwnedWriteHalf, UnixStream};
use crate::cap::registry::FabricRegistry;
use crate::planner::live_cap_fab::{LiveCapFab, ReachableTargetInfo, Strand};
use crate::urn::media_urn::MediaUrn;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{BufReader, BufWriter};
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{error, info, warn};

// =============================================================================
// ERROR TYPES
// =============================================================================

/// Errors that can occur in the relay switch.
#[derive(Debug, Clone, thiserror::Error)]
pub enum RelaySwitchError {
    #[error("CBOR error: {0}")]
    Cbor(String),

    #[error("I/O error: {0}")]
    Io(String),

    #[error("No handler found for cap: {0}")]
    NoHandler(String),

    #[error("Unknown request ID: {0:?}")]
    UnknownRequest(MessageId),

    #[error("Protocol violation: {0}")]
    Protocol(String),

    #[error("All masters are unhealthy")]
    AllMastersUnhealthy,

    /// The cartridge that would serve this request is not available: it left
    /// its host's inventory and did not come back within the admission grace
    /// window. Distinct from `Protocol` because nothing violated the protocol —
    /// the deployment changed under a valid request, which is an `environment`
    /// failure, not an engine defect (docs/failure-taxonomy.md).
    #[error("Cartridge unavailable: {0}")]
    CartridgeUnavailable(String),
}

impl From<CborError> for RelaySwitchError {
    fn from(e: CborError) -> Self {
        RelaySwitchError::Cbor(e.to_string())
    }
}

impl From<std::io::Error> for RelaySwitchError {
    fn from(e: std::io::Error) -> Self {
        RelaySwitchError::Io(e.to_string())
    }
}

// =============================================================================
// DATA STRUCTURES
// =============================================================================

// RoutingEntry lives in `request_state` — the unified per-request state module
// shared by every routing runtime (L7).
use crate::bifaci::request_state::{
    AdmissionController, AdmissionKey, AdmissionPermit, FrameDirection, RequestState, RequestTable,
    RoutingEntry, TerminalKind,
};
use crate::bifaci::stats::DropCounters;

/// Health status snapshot for a single master connection.
/// Returned by `RelaySwitch::get_master_health()` for monitoring.
#[derive(Debug, Clone)]
pub struct MasterHealthStatus {
    /// Master index (0-based)
    pub index: usize,
    /// Whether the master is currently healthy
    pub healthy: bool,
    /// Number of capabilities registered by this master
    pub cap_count: usize,
    /// Time when this master was connected (seconds since connection)
    pub connected_seconds: u64,
    /// Last error that caused unhealthy state (if any)
    pub last_error: Option<String>,
}

/// Kinds of attachment failure for a cartridge. Matches the
/// `CartridgeAttachmentErrorKind` enum defined in `cartridge.proto`; this enum
/// is the authoritative, language-neutral domain definition.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum CartridgeAttachmentErrorKind {
    /// Manifest parsed but violates the cartridge schema (missing required
    /// CAP_IDENTITY, min_app_version not met, old-format cap_groups, etc.).
    Incompatible,
    /// cartridge.json or HELLO manifest failed to parse as JSON, or lacked
    /// required top-level fields.
    ManifestInvalid,
    /// HELLO handshake did not complete (timeout, bad frame sequence, I/O).
    HandshakeFailed,
    /// CAP_IDENTITY echo protocol check failed.
    IdentityRejected,
    /// Entry point binary missing or not executable.
    EntryPointMissing,
    /// Cartridge repeatedly crashed the host during discovery; held in quarantine.
    Quarantined,
    /// On-disk install context disagrees with the cartridge.json the
    /// cartridge declares — the slug folder doesn't match the
    /// `slug_for(registry_url)` of the manifest, the channel folder
    /// doesn't match the manifest's `channel`, or the name/version
    /// directory components don't match. The cartridge is structurally
    /// well-formed but cannot be trusted because its placement on
    /// disk does not match what it claims to be. Distinct from
    /// `Quarantined` (host decided after a crash) and from
    /// `ManifestInvalid` (cartridge.json is itself unreadable or
    /// schema-broken). Hosts grace-period the offending directory and
    /// then delete it; the record is surfaced so the operator sees
    /// what landed where before it disappears.
    BadInstallation,
    /// Operator explicitly disabled this cartridge through the host
    /// UI. The cartridge is on disk and would otherwise have attached
    /// cleanly; the host treats it as if the binary were yanked out
    /// of the system — its caps are not registered with the engine,
    /// and any in-flight request the cartridge process was handling
    /// fails hard. Re-enabling is a UI-driven operator action.
    /// Enforced at the host level (machfab-mac's XPC service); the
    /// engine doesn't act on it differently from any other failed
    /// attachment, but preserves the kind so consumers can render the
    /// right reason and offer the right recovery action.
    Disabled,
    /// The cartridge declares a non-null `registry_url`, but the
    /// host could not reach that registry to verify the cartridge is
    /// listed. Distinct from `BadInstallation` (= registry confirmed
    /// the version is missing) — `RegistryUnreachable` means we
    /// don't know. Recovery action is "check network + retry"
    /// rather than "rebuild as dev". The cartridge is held back
    /// from attaching until verification succeeds; the UI shows the
    /// actionable reason.
    ///
    /// Network fetch is performed by the main app (which has
    /// outbound network entitlement) and pushed to the host as a
    /// verdict map; the XPC service is sandboxed and cannot fetch
    /// registries directly.
    RegistryUnreachable,
    /// The cartridge was built against a different fabric registry
    /// manifest version than this engine is pinned to. Both engine
    /// and cartridge bake their fabric manifest version at build
    /// time from `MFR_FABRIC_MANIFEST_VERSION` (sourced from
    /// `fabric/manifest-version.txt`); the engine refuses to load
    /// any cartridge whose baked version does not match its own.
    /// Recovery action is "rebuild the cartridge against the
    /// engine's fabric manifest version" — there is no in-engine
    /// fallback because URN resolution between mismatched versions
    /// is fundamentally unsafe (cap and media definitions may have
    /// changed shape across manifest versions).
    FabricManifestVersionMismatch,
}

/// In-progress lifecycle phases that run BEFORE a cartridge becomes
/// dispatchable. See `machfab-mac/docs/cartridge state machine.md` for
/// the canonical state diagram.
///
/// Mutually exclusive with `attachment_error` on
/// [`InstalledCartridgeRecord`]: when the cartridge has a failed
/// terminal classification, `attachment_error` is `Some` and
/// `lifecycle` is irrelevant (consumers must check the error first).
/// When `attachment_error` is `None`, the cartridge is in one of the
/// in-progress phases or has reached `Operational`; only
/// `Operational` cartridges are dispatchable.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum CartridgeLifecycle {
    /// Discovery scan has found the version directory and is about
    /// to inspect it. Transient — the host normally moves to
    /// `Inspecting` in the same scan tick. Surfaced as a distinct
    /// state so the UI has a first-render badge before hashing
    /// starts.
    Discovered,
    /// Reading `cartridge.json`, computing directory hash,
    /// validating on-disk install context (slug/channel/name/version
    /// folder components vs the manifest). Hashing can take seconds
    /// for large model cartridges; runs on a background queue so
    /// other cartridges' inspections proceed in parallel.
    Inspecting,
    /// Inspection succeeded. The host is awaiting a verdict from
    /// the registry verifier service for the cartridge's
    /// `(registry_url, channel, id, version)` 4-tuple. Skipped for
    /// dev cartridges (`registry_url == None`) and bundle
    /// cartridges (shipped with the .app, presence guaranteed by
    /// build) — those go straight to `Operational`.
    Verifying,
    /// Cleared every gate. Caps are registered with the engine and
    /// dispatch can route requests to this cartridge.
    Operational,
}

impl Default for CartridgeLifecycle {
    /// Default is `Discovered` so a freshly-constructed identity
    /// without an explicit lifecycle is treated as "scan saw it,
    /// nothing further yet" — never as `Operational` (which would
    /// silently expose an un-inspected cartridge for dispatch).
    fn default() -> Self {
        CartridgeLifecycle::Discovered
    }
}

/// Structured per-cartridge attachment failure.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct CartridgeAttachmentError {
    pub kind: CartridgeAttachmentErrorKind,
    pub message: String,
    /// Unix timestamp seconds when the failure was first detected.
    pub detected_at_unix_seconds: i64,
}

/// Live runtime statistics for an attached cartridge.
///
/// All fields are gathered by the `CartridgeHostRuntime` that owns the
/// cartridge process — it is the only component with the authoritative
/// routing tables and process handles. Memory figures are self-reported
/// by the cartridge in its heartbeat reply (using `proc_pid_rusage` on
/// its own pid) so the host never needs to inspect another process's
/// state — this keeps the path sandbox-compatible.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct CartridgeRuntimeStats {
    /// Process is currently running and serving requests.
    pub running: bool,
    /// The cartridge's full concurrency-pool state map (`bifaci::pools`):
    /// declared/configured/available/active/queued per pool — singleton
    /// pools keyed by cap URN, declared shared pools, and `all`. This IS
    /// the capacity surface; there is no scalar.
    pub pools: crate::bifaci::pools::PoolStates,
    /// OS pid of the cartridge process when running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// Number of incoming requests the host is currently routing to this
    /// cartridge (entries in `incoming_rxids` whose value is this
    /// cartridge index).
    pub active_request_count: u64,
    /// Number of outstanding peer invocations this cartridge has issued
    /// (entries in `outgoing_rids` whose value is this cartridge index).
    pub peer_request_count: u64,
    /// Physical memory footprint in MB, self-reported via heartbeat.
    pub memory_footprint_mb: u64,
    /// Resident set size in MB, self-reported via heartbeat.
    pub memory_rss_mb: u64,
    /// Unix timestamp seconds of the last heartbeat response received.
    /// `None` means no heartbeat has completed a round trip yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_unix_seconds: Option<i64>,
    /// Number of times this cartridge has been respawned after death.
    pub restart_count: u64,
    /// Total protocol-level frame drops inside the cartridge runtime
    /// (closed-channel sends, credit violations, …), self-reported as
    /// `drops_total` on every heartbeat response meta (L8: every drop is
    /// countable end-to-end). `None` until the first heartbeat round-trip
    /// delivers a reading. Drops mean something went wrong.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_drops_total: Option<u64>,
    /// Total BENIGN post-terminal stragglers suppressed by the cartridge's
    /// writer gate (late keepalive/emitter frames that crossed their flow's
    /// terminal — the expected teardown race), self-reported as
    /// `stragglers_total` on every heartbeat response meta. Separate from
    /// drops: nothing went wrong. `None` until the first reading.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_stragglers_total: Option<u64>,
    /// Total live-feed OVERRUNS at this cartridge's capture edges
    /// (12.5 §Overrun: real-time items discarded because the consumer
    /// lagged — inherent to live capture, its own category, never a
    /// drop), self-reported as `overruns_total` on heartbeat meta.
    /// `None` until the first reading.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_overruns_total: Option<u64>,
}

impl CartridgeRuntimeStats {
    /// Snapshot of a registered but not-yet-running cartridge.
    pub fn not_running() -> Self {
        Self {
            running: false,
            pools: crate::bifaci::pools::PoolStates::new(),
            pid: None,
            active_request_count: 0,
            peer_request_count: 0,
            memory_footprint_mb: 0,
            memory_rss_mb: 0,
            last_heartbeat_unix_seconds: None,
            restart_count: 0,
            protocol_drops_total: None,
            protocol_stragglers_total: None,
            protocol_overruns_total: None,
        }
    }
}

/// Identity of an installed cartridge as known to the bifaci protocol.
///
/// `(registry_url, channel, id)` is the install's full identity. The
/// same id can independently be installed in multiple registries × both
/// channels with different versions and metadata; each combination is a
/// distinct install that lives in its own subtree on disk
/// (`{registry_slug}/{channel}/{id}/{version}/`).
///
/// `registry_url` is sourced from `cartridge.json:registry_url`
/// (written by the installer when the cartridge was placed on disk)
/// and round-trips out to every consumer that needs to render or act
/// on installed cartridges. `None` ⇔ dev install (the cartridge was
/// built and installed locally without `--registry`); the on-disk
/// folder is the literal `dev`.
///
/// The field is required-but-nullable on the wire — missing
/// `registry_url` is a parse error so a downstream reader can never
/// silently accept an old-schema payload.
/// Order-theoretic note: this struct derives only `PartialEq`/`Eq` —
/// not `PartialOrd`/`Ord` via `derive`. The relay sorts and dedups
/// these for inventory aggregation, and the natural sort key is the
/// install's **identity tuple** `(registry_url, channel, id,
/// version, sha256)` — five flat strings/enums that ARE totally
/// ordered. The `cap_groups` field, on the other hand, contains
/// `Cap` URNs whose semantic order is the triple partial order of
/// `(in, out, y)` with mixed variance (see `docs/02-FORMAL-FOUNDATIONS.md`).
/// Cap URNs intentionally do NOT implement `Ord`: a totally-ordered
/// `Ord::cmp` would either flatten the 3D mixed-variance domain (one
/// of capdag's documented failure modes — §18 of formal foundations)
/// or pick a meaningless lexicographic-of-canonical-form ordering.
///
/// We therefore implement `PartialOrd`/`Ord` *manually*, comparing
/// only the identity tuple and ignoring `cap_groups` for purposes of
/// sortability. Two installs of the same identity but different
/// manifests still compare equal under this ordering — which is the
/// correct semantic for an *inventory* sort: the inventory is keyed
/// by identity. Equality (`PartialEq`/`Eq`) still includes
/// `cap_groups` (manifest changes are real changes the watcher must
/// notice), but we drop `cap_groups` from `Hash` for the same reason
/// — actually we don't derive `Hash`, so this only matters for the
/// `Eq`-implies-`Hash`-consistency rule via the manual impls below.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct InstalledCartridgeRecord {
    /// Registry URL the cartridge was published from. `None` ⇔ dev
    /// install. Compared byte-wise; never normalized.
    pub registry_url: Option<String>,
    pub channel: crate::bifaci::cartridge_repo::CartridgeChannel,
    pub id: String,
    pub version: String,
    pub sha256: String,
    /// Cap groups exactly as the cartridge declared them in its
    /// manifest. Each group bundles a set of caps with the
    /// `adapter_urns` it volunteers to inspect. Empty when the
    /// cartridge failed attachment before its manifest could be parsed.
    /// This is the per-cartridge ground truth; the relay's flat cap
    /// snapshot is computed from these groups, not stored separately
    /// on the wire.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cap_groups: Vec<crate::bifaci::manifest::CapGroup>,
    /// Present when the cartridge failed attachment (manifest, handshake,
    /// identity, etc.). Absent when the cartridge is attached and healthy.
    /// Serialized field name is `attachment_error` so the Swift-side
    /// payload (snake_case JSON) and the engine agree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachment_error: Option<CartridgeAttachmentError>,
    /// Live runtime statistics from the owning host. `None` for cartridges
    /// on hosts that don't track process state (e.g. in-process
    /// cartridges handled directly by the engine).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_stats: Option<CartridgeRuntimeStats>,
    /// Positive lifecycle phase. Mutually exclusive with
    /// `attachment_error`: when the cartridge has a failed terminal
    /// classification, `attachment_error` is `Some` and `lifecycle`
    /// should be ignored. When `attachment_error` is `None`,
    /// `lifecycle` carries the in-progress phase
    /// (`Discovered` → `Inspecting` → `Verifying` → `Operational`)
    /// and the cartridge is dispatchable iff `lifecycle ==
    /// Operational`.
    ///
    /// Defaults to `Discovered` so a freshly-constructed identity
    /// without an explicit lifecycle never accidentally appears as
    /// `Operational` (the safe-default rule). Producers MUST set
    /// the field explicitly; relying on the default is a bug.
    #[serde(default)]
    pub lifecycle: CartridgeLifecycle,
}

impl InstalledCartridgeRecord {
    /// Order key for inventory sort/dedup. Only the five fields that
    /// uniquely identify an install participate in the lexicographic
    /// comparison — `cap_groups` / `attachment_error` / `runtime_stats`
    /// carry content that has no natural total order (cap URNs are
    /// 3D mixed-variance partial orders; runtime stats are observation-
    /// time data), so they're excluded from the sort key.
    ///
    /// This is the same five-field tuple that `dedup_by` collapses
    /// after sorting, so identical identities become adjacent and
    /// dedup regardless of whether their manifests or runtime stats
    /// happened to differ at snapshot time.
    pub fn identity_cmp(&self, other: &Self) -> std::cmp::Ordering {
        (
            &self.registry_url,
            &self.channel,
            &self.id,
            &self.version,
            &self.sha256,
        )
            .cmp(&(
                &other.registry_url,
                &other.channel,
                &other.id,
                &other.version,
                &other.sha256,
            ))
    }
}

impl InstalledCartridgeRecord {
    /// On-disk slug derived from `registry_url`. Dev installs hash to
    /// the literal `dev`; published installs hash to the first 16
    /// hex chars of the URL's SHA-256.
    pub fn registry_slug(&self) -> String {
        crate::bifaci::cartridge_slug::slug_for(self.registry_url.as_deref())
    }

    /// Flat cap-URN view across this cartridge's groups, deduplicated
    /// while preserving the order in which urns first appear. Returned
    /// rather than stored on the wire — `cap_groups` is the canonical
    /// source.
    pub fn cap_urns(&self) -> Vec<String> {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
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
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RelayNotifyCapabilitiesPayload {
    pub installed_cartridges: Vec<InstalledCartridgeRecord>,
    /// Host-level protocol observability (L8): drop counters, routing-table
    /// sizes, GC totals. Refreshed with each stats republish so the engine
    /// can surface the state of communications per host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_protocol_stats: Option<crate::bifaci::host_runtime::HostProtocolStats>,
}

impl RelayNotifyCapabilitiesPayload {
    /// Construct a payload from a list of installed-cartridge identities.
    pub fn new(installed_cartridges: Vec<InstalledCartridgeRecord>) -> Self {
        Self {
            installed_cartridges,
            host_protocol_stats: None,
        }
    }

    /// Attach the host's protocol stats snapshot.
    pub fn with_host_protocol_stats(
        mut self,
        stats: crate::bifaci::host_runtime::HostProtocolStats,
    ) -> Self {
        self.host_protocol_stats = Some(stats);
        self
    }

    /// Flat cap-URN union across every cartridge in the payload,
    /// deduplicated while preserving first-seen order. Computed view —
    /// not stored on the wire.
    pub fn cap_urns(&self) -> Vec<String> {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut out: Vec<String> = Vec::new();
        for cart in &self.installed_cartridges {
            for urn in cart.cap_urns() {
                if seen.insert(urn.clone()) {
                    out.push(urn);
                }
            }
        }
        out
    }
}

/// Connection to a single RelayMaster with its socket I/O.
/// Interior mutability: writer and seq_assigner are behind Mutex.
struct MasterConnection {
    /// Stable identity of this master across (re)connections.
    ///
    /// Each cardinality slot the engine wires up at startup gets a
    /// caller-chosen id (e.g. `"in-process"`, `"bundled-cartridges"`,
    /// `"xpc-service"`). When a master dies and the host
    /// reconnects, [`RelaySwitch::add_master`] reattaches the new
    /// socket to the existing slot whose id matches — the slot
    /// index never changes for the lifetime of the engine.
    /// Re-adding the same id while the slot is still healthy is a
    /// caller bug and is rejected.
    ///
    /// `id` itself never changes after the slot is created, so it
    /// is held as plain `String` (no interior lock needed).
    id: String,
    /// Writer for frames to slave (Mutex for concurrent access).
    /// Replaced wholesale on reattach.
    socket_writer: Mutex<FrameWriter<BufWriter<OwnedWriteHalf>>>,
    /// Seq assigner for frames written to this master.
    /// Reset on reattach (sequence numbering restarts per session).
    seq_assigner: Mutex<SeqAssigner>,
    /// Latest manifest from RelayNotify
    manifest: RwLock<Vec<u8>>,
    /// Latest limits from RelayNotify
    limits: RwLock<Limits>,
    /// Parsed capability URNs from manifest
    caps: RwLock<Vec<String>>,
    /// Installed cartridge identities reported by this master
    installed_cartridges: RwLock<Vec<InstalledCartridgeRecord>>,
    /// Latest per-host protocol stats (drops, routing-table sizes, GC
    /// totals) reported by this master's RelayNotify. `None` until the
    /// first advertisement that carries them. Previously this payload
    /// field was parsed and silently discarded — the L8 surface now
    /// retains it so `protocol_stats()` can name the host behind a drop.
    host_protocol_stats: RwLock<Option<crate::bifaci::host_runtime::HostProtocolStats>>,
    /// Connection health status
    healthy: AtomicBool,
    /// Reader task handle. `Mutex<Option<…>>` so reattach can swap
    /// it under a `read()` lock on the masters Vec — keeping the
    /// hot dispatch path unblocked during a registration. The
    /// previous handle is aborted defensively when replaced.
    reader_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Time when this master was connected. Reset on reattach.
    /// Held in a Mutex so reattach can update under a `read()`
    /// lock on the masters Vec. Uses `std::sync::Mutex` (not
    /// Tokio) because all touches are non-await: a brief lock to
    /// read or write an `Instant`.
    connected_at: std::sync::Mutex<Instant>,
    /// Last error message (if unhealthy)
    last_error: RwLock<Option<String>>,
}

impl std::fmt::Debug for MasterConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MasterConnection")
            .field("healthy", &self.healthy.load(Ordering::SeqCst))
            .finish()
    }
}

/// RelaySwitch — Cap-aware routing multiplexer for multiple RelayMasters.
///
/// Aggregates capabilities from multiple RelayMasters and routes requests
/// based on cap URN matching. Handles both engine→cartridge and cartridge→cartridge
/// (peer) invocations with correct routing semantics.
///
/// Uses interior mutability for concurrent DAG execution support.
/// All public methods take `&self`, not `&mut self`.
pub struct RelaySwitch {
    /// Managed relay master connections
    masters: RwLock<Vec<MasterConnection>>,
    /// Routing: cap_urn → master index
    cap_table: RwLock<Vec<(String, usize)>>,
    /// Unified per-request state (L7): routing, origin, peer markers,
    /// cancel-cascade children, external response channel, per-stream flow
    /// stats, and the rid→xid index — one entry, one registration, one
    /// termination. Replaces the six parallel routing maps.
    requests: RwLock<RequestTable>,
    /// FIFO capacity gate keyed by exact relay-master + cartridge identity.
    admission: AdmissionController,
    /// Dropped-frame accounting (L8): frames lost to something going WRONG
    /// (no routing state, dead channels, credit violations) — counted per
    /// reason × frame type, never silent losses and never protocol errors.
    drops: Arc<DropCounters>,
    /// Benign post-terminal stragglers: flow frames that crossed their
    /// request's terminal in flight — the ordinary, protocol-legal teardown
    /// race (a callee ENDing before draining input, a final credit grant
    /// racing END). Counted per frame type and indicated as benign in every
    /// stats surface — NOT drops, nothing went wrong.
    stragglers: Arc<crate::bifaci::stats::StragglerCounters>,
    /// Aggregate capabilities (union of all masters)
    aggregate_capabilities: RwLock<Vec<u8>>,
    /// Aggregate installed cartridge identities (union of all healthy masters).
    /// Includes both attached-successfully and attachment-failed cartridges;
    /// failed ones carry `attachment_error`.
    aggregate_installed_cartridges: RwLock<Vec<InstalledCartridgeRecord>>,
    /// Watch channel broadcasting the latest `aggregate_installed_cartridges`.
    /// Subscribers (e.g. the Mac gRPC bridge) receive the current value on
    /// subscribe and a fresh value every time `rebuild_capabilities` produces
    /// a different snapshot.
    aggregate_installed_cartridges_tx: tokio::sync::watch::Sender<Vec<InstalledCartridgeRecord>>,
    /// Watch channel broadcasting the latest `aggregate_capabilities` bytes
    /// (the JSON array of *routable* cap URNs — union of healthy masters
    /// only). Subscribers receive the current value on subscribe and a fresh
    /// value every time `rebuild_capabilities` changes the routable set. This
    /// is the signal an engine-facing relay (e.g. the interop router) uses to
    /// re-advertise routability: a cap only appears here once its master is
    /// healthy (identity-verified), so it is the correct readiness signal —
    /// unlike `aggregate_installed_cartridges`, which is the inventory view
    /// and is deliberately NOT health-filtered.
    aggregate_capabilities_tx: tokio::sync::watch::Sender<Vec<u8>>,
    /// Negotiated limits (minimum across all masters)
    negotiated_limits: RwLock<Limits>,
    /// Channel receiver for frames from master reader tasks (Mutex for exclusive receive)
    frame_rx: Mutex<mpsc::UnboundedReceiver<(usize, Result<Frame, CborError>)>>,
    /// Channel sender for spawning new reader tasks (stored for add_master)
    frame_tx: mpsc::UnboundedSender<(usize, Result<Frame, CborError>)>,
    /// XID counter for assigning unique routing IDs (RelaySwitch assigns on first arrival)
    xid_counter: AtomicU64,
    /// Precomputed capability graph for path finding and reachability queries
    live_cap_fab: RwLock<LiveCapFab>,
    /// Unified registry. Used for cap-definition lookups while building
    /// the LiveCapFab and read at every sync to compute the
    /// bookend-eligible URN set (URNs whose stored spec has at least one
    /// file extension). The bookend set is cached inside LiveCapFab and
    /// no traversal hits the registry.
    fabric_registry: Arc<FabricRegistry>,
    /// Number of masters this engine intends to register at startup.
    /// `all_masters_ready` only returns true once `masters.len() >=
    /// expected_master_count` AND every connected master is ready —
    /// this prevents a premature "ready" signal during boot when the
    /// external-cartridges master is still spawning cartridges (and, in
    /// Website, before the XPC-service relay has connected).
    ///
    /// Counts the cardinality slots the engine wires up at startup. The
    /// engine hosts no in-process cartridges, so there is no internal slot:
    ///   - MAS edition (1 slot):
    ///     * `MASTER_ID_EXTERNAL` — engine-spawned external cartridges
    ///       (every cartridge ships embedded as an external cartridge
    ///       in MAS; there is no XPC-service slot)
    ///   - Website edition (2 slots):
    ///     * `MASTER_ID_EXTERNAL`
    ///     * `MASTER_ID_XPC_SERVICE` — the host XPC service
    ///       managing installed cartridges
    ///
    /// Reattach via [`add_master`]'s id-keyed lookup keeps the slot
    /// COUNT stable across reconnects — a dead slot is reattached
    /// in place, never accumulating zombies.
    ///
    /// Set once via `set_expected_master_count` shortly after
    /// construction (RelaySwitch ctor doesn't take it because the
    /// caller decides the count from edition + cartridge discovery).
    /// Atomic so reads from the readiness predicate don't need to
    /// take the masters lock.
    expected_master_count: AtomicUsize,
    /// Stop flag for the persistent background drain pump and the
    /// runtime identity-probe driver. Set by `Drop` so both tasks
    /// exit on their next iteration.
    background_pump_stop: Arc<AtomicBool>,
    /// Handles for the persistent background tasks (frame pump +
    /// identity-probe driver), stored so `Drop` can abort them when
    /// the switch goes away. Empty until `start_background_pump` is
    /// called exactly once after the switch is Arc-wrapped.
    background_pump_handle: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// Queue of master indexes whose advertised cap set transitioned
    /// from empty to non-empty since the last identity probe. The
    /// background pump drains this queue and runs end-to-end identity
    /// probes against each named master, gating cap-table publication
    /// on probe success — the runtime counterpart to the synchronous
    /// `add_master` probe that fires at handshake. We push from
    /// `handle_master_frame`'s RelayNotify-update branch and consume
    /// from a dedicated task in `start_background_pump`.
    pending_identity_probes_tx: mpsc::UnboundedSender<usize>,
    /// Receive end of the probe queue, owned by the relay so we can
    /// hand it to the background pump's verification task at startup.
    /// `Mutex<Option<…>>` so the pump can `take` it exactly once;
    /// re-call attempts are caller-error.
    pending_identity_probes_rx: std::sync::Mutex<Option<mpsc::UnboundedReceiver<usize>>>,
    /// Serialises [`RelaySwitch::add_master`] so concurrent
    /// registrations (in particular the steady-state Mac-app
    /// reconnect path racing engine startup) cannot pick the same
    /// `master_idx` for two different slots, and so the
    /// "find existing slot by id" lookup observes a stable
    /// `masters` Vec across the registration's network I/O.
    /// Tokio Mutex because the held region awaits on socket I/O.
    add_master_lock: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for RelaySwitch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelaySwitch")
            .field("xid_counter", &self.xid_counter.load(Ordering::SeqCst))
            .finish()
    }
}

/// The switch's protocol observability snapshot (L8): live request state,
/// recent terminations, and per-reason drop counters. Serializable; field
/// names are the mirror contract.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RelaySwitchProtocolStats {
    pub requests: crate::bifaci::request_state::RequestTableSnapshot,
    pub drops: crate::bifaci::stats::DropSnapshot,
    /// Benign post-terminal stragglers — the expected teardown crossing,
    /// counted per frame type. Indicated separately from `drops` because
    /// nothing went wrong.
    #[serde(default)]
    pub stragglers: crate::bifaci::stats::StragglerSnapshot,
    /// Per-master host protocol stats, keyed by master id, as reported in
    /// each host's latest RelayNotify. A master that has not yet advertised
    /// host stats is absent (never a zeroed placeholder).
    #[serde(default)]
    pub hosts: std::collections::BTreeMap<String, crate::bifaci::host_runtime::HostProtocolStats>,
}

// =============================================================================
// IMPLEMENTATION
// =============================================================================

impl RelaySwitch {
    /// Create a new RelaySwitch with the given socket streams and unified registry.
    ///
    /// Each UnixStream is split into read/write halves internally.
    /// Performs handshake with all masters and builds initial capability table.
    ///
    /// The fabric registry is used to look up Cap definitions for building
    /// the LiveCapFab and to compute the bookend-eligible media URN set.
    /// Construct a RelaySwitch from a list of `(id, socket)` pairs,
    /// one per cardinality slot the engine wires up at startup.
    ///
    /// Each id is the stable identity of its slot — used by
    /// [`add_master`] on subsequent reconnects to find the slot
    /// to reattach to. Ids must be unique within the list; a
    /// duplicate is a wiring bug and surfaces as a hard error
    /// rather than letting the second slot silently shadow the
    /// first in routing.
    pub async fn new(
        sockets: Vec<(String, UnixStream)>,
        fabric_registry: Arc<FabricRegistry>,
    ) -> Result<Self, RelaySwitchError> {
        // Reject duplicate ids up front. Without this, two slots
        // would be created with the same id; the first reconnect
        // would reattach to whichever slot is found first by the
        // linear scan in `add_master`, leaving the other slot
        // stuck unhealthy forever — the exact bug class we are
        // closing.
        {
            let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
            for (id, _) in &sockets {
                if !seen.insert(id.as_str()) {
                    return Err(RelaySwitchError::Protocol(format!(
                        "RelaySwitch::new: duplicate master id '{}' in cardinality list — \
                         each slot must have a unique stable id",
                        id
                    )));
                }
            }
        }

        let mut masters = Vec::new();
        let (frame_tx, frame_rx) = mpsc::unbounded_channel();
        let xid_counter = AtomicU64::new(0);

        // Phase 1: For each master, read RelayNotify and verify identity.
        // Reader tasks are spawned only after verification succeeds.
        let mut pending_readers: Vec<(usize, FrameReader<BufReader<OwnedReadHalf>>)> = Vec::new();
        // Carry ids alongside the index so we can stamp each slot
        // when we push it.
        let mut slot_ids: Vec<String> = Vec::with_capacity(sockets.len());

        for (master_idx, (id, socket)) in sockets.into_iter().enumerate() {
            slot_ids.push(id);
            let (read_half, write_half) = socket.into_split();
            let mut socket_reader = FrameReader::new(BufReader::new(read_half));
            let mut socket_writer = FrameWriter::new(BufWriter::new(write_half));

            // Read RelayNotify (first frame from each master)
            let notify_frame = socket_reader
                .read()
                .await
                .map_err(|e| RelaySwitchError::Cbor(format!("master {}: {}", master_idx, e)))?
                .ok_or_else(|| {
                    RelaySwitchError::Protocol(format!(
                        "master {}: connection closed before RelayNotify",
                        master_idx
                    ))
                })?;

            if notify_frame.frame_type != FrameType::RelayNotify {
                return Err(RelaySwitchError::Protocol(format!(
                    "master {}: expected RelayNotify, got {:?}",
                    master_idx, notify_frame.frame_type
                )));
            }

            let mut caps_payload = notify_frame
                .relay_notify_manifest()
                .ok_or_else(|| {
                    RelaySwitchError::Protocol(format!(
                        "master {}: RelayNotify has no manifest",
                        master_idx
                    ))
                })?
                .to_vec();

            let mut payload = parse_relay_notify_payload(&caps_payload)?;
            let mut caps = payload.cap_urns();
            let mut limits = notify_frame.relay_notify_limits().unwrap_or_default();

            let mut seq_assigner = SeqAssigner::new();

            // End-to-end identity verification. The probe only makes sense
            // when the host has at least one advertised cap — an empty cap
            // list means "no cartridges attached successfully" and there is
            // no handler chain to test. The master still joins so its
            // `installed_cartridges` attachment errors reach the engine.
            if !caps.is_empty() {
                let xid_val = xid_counter.fetch_add(1, Ordering::SeqCst) + 1;
                let xid = MessageId::Uint(xid_val);

                use crate::standard::caps::CAP_IDENTITY;

                let nonce = identity_nonce();
                let req_id = MessageId::new_uuid();
                let stream_id = "identity-verify".to_string();

                let mut req = Frame::req(req_id.clone(), CAP_IDENTITY, vec![], "application/cbor");
                req.routing_id = Some(xid.clone());
                seq_assigner.assign(&mut req);
                socket_writer.write(&req).await.map_err(|e| {
                    RelaySwitchError::Protocol(format!(
                        "master {}: identity verification send failed: {}",
                        master_idx, e
                    ))
                })?;

                let mut ss = Frame::stream_start(
                    req_id.clone(),
                    stream_id.clone(),
                    "media:".to_string(),
                    None,
                );
                ss.routing_id = Some(xid.clone());
                seq_assigner.assign(&mut ss);
                socket_writer.write(&ss).await.map_err(|e| {
                    RelaySwitchError::Protocol(format!(
                        "master {}: identity verification send failed: {}",
                        master_idx, e
                    ))
                })?;

                let checksum = Frame::compute_checksum(&nonce);
                let mut chunk = Frame::chunk(
                    req_id.clone(),
                    stream_id.clone(),
                    0,
                    nonce.clone(),
                    0,
                    checksum,
                );
                chunk.routing_id = Some(xid.clone());
                seq_assigner.assign(&mut chunk);
                socket_writer.write(&chunk).await.map_err(|e| {
                    RelaySwitchError::Protocol(format!(
                        "master {}: identity verification send failed: {}",
                        master_idx, e
                    ))
                })?;

                let mut se = Frame::stream_end(req_id.clone(), stream_id, 1);
                se.routing_id = Some(xid.clone());
                seq_assigner.assign(&mut se);
                socket_writer.write(&se).await.map_err(|e| {
                    RelaySwitchError::Protocol(format!(
                        "master {}: identity verification send failed: {}",
                        master_idx, e
                    ))
                })?;

                let mut end = Frame::end(req_id.clone(), None);
                end.routing_id = Some(xid.clone());
                seq_assigner.assign(&mut end);
                socket_writer.write(&end).await.map_err(|e| {
                    RelaySwitchError::Protocol(format!(
                        "master {}: identity verification send failed: {}",
                        master_idx, e
                    ))
                })?;

                seq_assigner.remove(&FlowKey {
                    rid: req_id.clone(),
                    xid: Some(xid.clone()),
                });

                // Read response — expect STREAM_START → CHUNK(s) → STREAM_END → END
                let mut accumulated = Vec::new();
                loop {
                    let frame = socket_reader
                        .read()
                        .await
                        .map_err(|e| {
                            RelaySwitchError::Protocol(format!(
                                "master {}: identity verification read failed: {}",
                                master_idx, e
                            ))
                        })?
                        .ok_or_else(|| {
                            RelaySwitchError::Protocol(format!(
                                "master {}: connection closed during identity verification",
                                master_idx
                            ))
                        })?;

                    match frame.frame_type {
                        FrameType::RelayNotify => {
                            // CartridgeHostRuntime sends the full RelayNotify (with all caps)
                            // through RelaySlave during identity verification. Update caps.
                            if let Some(manifest) = frame.relay_notify_manifest() {
                                caps_payload = manifest.to_vec();
                                payload = parse_relay_notify_payload(&caps_payload)?;
                                caps = payload.cap_urns();
                            }
                            if let Some(l) = frame.relay_notify_limits() {
                                limits = l;
                            }
                        }
                        FrameType::StreamStart => {}
                        FrameType::Chunk => {
                            if let Some(payload) = frame.payload {
                                accumulated.extend_from_slice(&payload);
                            }
                        }
                        FrameType::StreamEnd => {}
                        FrameType::End => {
                            if accumulated != nonce {
                                return Err(RelaySwitchError::Protocol(format!(
                                    "master {}: identity verification payload mismatch (expected {} bytes, got {})",
                                    master_idx, nonce.len(), accumulated.len()
                                )));
                            }
                            break;
                        }
                        FrameType::Err => {
                            let code = frame.error_code().ok_or_else(|| {
                                RelaySwitchError::Protocol(
                                    "ERR frame missing required text code".to_string(),
                                )
                            })?;
                            let msg = frame.error_message().ok_or_else(|| {
                                RelaySwitchError::Protocol(
                                    "ERR frame missing required text message".to_string(),
                                )
                            })?;
                            return Err(RelaySwitchError::Protocol(format!(
                                "master {}: identity verification failed: [{code}] {msg}",
                                master_idx
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
                            return Err(RelaySwitchError::Protocol(format!(
                                "master {}: identity verification: unexpected frame type {:?}",
                                master_idx, other
                            )));
                        }
                    }
                }
            }

            // Stash reader for spawning after all masters are verified
            pending_readers.push((master_idx, socket_reader));

            masters.push(MasterConnection {
                id: slot_ids[master_idx].clone(),
                socket_writer: Mutex::new(socket_writer),
                seq_assigner: Mutex::new(seq_assigner),
                manifest: RwLock::new(caps_payload),
                limits: RwLock::new(limits),
                caps: RwLock::new(caps),
                installed_cartridges: RwLock::new(payload.installed_cartridges),
                host_protocol_stats: RwLock::new(payload.host_protocol_stats),
                healthy: AtomicBool::new(true),
                reader_handle: Mutex::new(None), // Spawned in phase 2
                connected_at: std::sync::Mutex::new(Instant::now()),
                last_error: RwLock::new(None),
            });
        }

        // Phase 2: All masters verified — spawn reader tasks
        for (master_idx, socket_reader) in pending_readers {
            let tx = frame_tx.clone();
            let reader_handle = tokio::spawn(async move {
                let mut reader = socket_reader;
                loop {
                    match reader.read().await {
                        Ok(Some(frame)) => {
                            if tx.send((master_idx, Ok(frame))).is_err() {
                                tracing::warn!(
                                    "[RelaySwitch] master {} reader: frame_tx closed",
                                    master_idx
                                );
                                break;
                            }
                        }
                        Ok(None) => {
                            tracing::warn!(
                                "[RelaySwitch] master {} reader: socket closed (EOF)",
                                master_idx
                            );
                            let _ = tx.send((master_idx, Err(CborError::UnexpectedEof)));
                            break;
                        }
                        Err(e) => {
                            tracing::error!(
                                "[RelaySwitch] master {} reader: socket error: {}",
                                master_idx,
                                e
                            );
                            let _ = tx.send((master_idx, Err(e)));
                            break;
                        }
                    }
                }
            });
            // `reader_handle` is `Mutex<Option<…>>`; lock briefly
            // to install the freshly-spawned task. No `await` is
            // crossed while the inner Mutex is held.
            *masters[master_idx].reader_handle.lock().await = Some(reader_handle);
        }

        let (aggregate_installed_cartridges_tx, _) = tokio::sync::watch::channel(Vec::new());
        let (aggregate_capabilities_tx, _) = tokio::sync::watch::channel(
            serde_json::to_vec(&Vec::<String>::new())
                .expect("empty capability snapshot must serialize"),
        );
        let (probes_tx, probes_rx) = mpsc::unbounded_channel::<usize>();
        let switch = Self {
            masters: RwLock::new(masters),
            cap_table: RwLock::new(Vec::new()),
            requests: RwLock::new(RequestTable::new()),
            admission: AdmissionController::default(),
            drops: Arc::new(DropCounters::new()),
            stragglers: Arc::new(crate::bifaci::stats::StragglerCounters::new()),
            aggregate_capabilities: RwLock::new(
                serde_json::to_vec(&Vec::<String>::new())
                    .expect("empty capability snapshot must serialize"),
            ),
            aggregate_installed_cartridges: RwLock::new(Vec::new()),
            aggregate_installed_cartridges_tx,
            aggregate_capabilities_tx,
            negotiated_limits: RwLock::new(Limits::default()),
            frame_rx: Mutex::new(frame_rx),
            frame_tx,
            xid_counter,
            live_cap_fab: RwLock::new(LiveCapFab::new()),
            fabric_registry,
            // Default 0 — readiness predicate returns false until
            // the engine calls `set_expected_master_count` after
            // it knows how many masters it intends to register.
            // Without that explicit declaration we'd have no way to
            // distinguish "still booting, more masters coming" from
            // "no more masters expected; ready".
            expected_master_count: AtomicUsize::new(0),
            background_pump_stop: Arc::new(AtomicBool::new(false)),
            background_pump_handle: std::sync::Mutex::new(Vec::new()),
            pending_identity_probes_tx: probes_tx,
            pending_identity_probes_rx: std::sync::Mutex::new(Some(probes_rx)),
            add_master_lock: tokio::sync::Mutex::new(()),
        };

        let initial_inventories = {
            let masters = switch.masters.read().await;
            let mut inventories = Vec::with_capacity(masters.len());
            for master in masters.iter() {
                inventories.push(master.installed_cartridges.read().await.clone());
            }
            inventories
        };
        for (master_idx, cartridges) in initial_inventories.iter().enumerate() {
            switch.configure_master_admission(master_idx, cartridges)?;
        }

        // Build routing tables from already-populated caps
        switch.rebuild_cap_table().await;
        switch.rebuild_capabilities().await;
        switch.rebuild_limits().await;

        Ok(switch)
    }

    /// Get the aggregate capabilities of all healthy masters.
    pub async fn capabilities(&self) -> Vec<u8> {
        self.aggregate_capabilities.read().await.clone()
    }

    /// Get the aggregate installed cartridges of all healthy masters.
    pub async fn installed_cartridges(&self) -> Vec<InstalledCartridgeRecord> {
        self.aggregate_installed_cartridges.read().await.clone()
    }

    /// Subscribe to per-cartridge attachment-state changes. The returned
    /// receiver yields the current snapshot immediately and a fresh snapshot
    /// every time the aggregate changes.
    pub fn subscribe_installed_cartridges(
        &self,
    ) -> tokio::sync::watch::Receiver<Vec<InstalledCartridgeRecord>> {
        self.aggregate_installed_cartridges_tx.subscribe()
    }

    /// Subscribe to changes in the *routable* capability set. The returned
    /// receiver yields the current `aggregate_capabilities` bytes (a JSON
    /// array of cap URNs) immediately and a fresh snapshot every time the
    /// routable set changes — including when a deferred identity probe
    /// completes and a previously-unhealthy master's caps become routable.
    /// An engine-facing relay uses this to advertise readiness that is tied
    /// to master health, not to mere inventory presence.
    pub fn subscribe_capabilities(&self) -> tokio::sync::watch::Receiver<Vec<u8>> {
        self.aggregate_capabilities_tx.subscribe()
    }

    /// Spawn the persistent background drain pump.
    ///
    /// `frame_rx` accumulates inbound frames from every connected master
    /// (RelayNotify capability updates, peer invocations, responses to
    /// `execute_cap`). Without a running drain the channel fills up and
    /// control frames queue until the next per-execution pump runs —
    /// which means capability updates (e.g. a newly installed cartridge)
    /// are invisible to the engine until the next cap execution happens.
    ///
    /// This pump runs for the switch's lifetime and consumes frames
    /// through `handle_master_frame` — the same dispatch path the
    /// per-execution pumps use. Frames destined for response channels
    /// are routed there; pass-through frames returned by
    /// `handle_master_frame` are discarded because no single consumer
    /// owns them (the lock serializes so there is no data loss).
    ///
    /// Idempotent — a second call is a no-op. Must be called with the
    /// switch already wrapped in `Arc` so the task can outlive the
    /// caller.
    pub fn start_background_pump(self: &Arc<Self>) {
        let mut guard = self
            .background_pump_handle
            .lock()
            .expect("background_pump_handle mutex poisoned");
        if !guard.is_empty() {
            return;
        }

        let weak = Arc::downgrade(self);
        let stop = self.background_pump_stop.clone();
        let handle = tokio::spawn(async move {
            loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let Some(switch) = weak.upgrade() else {
                    break;
                };
                match switch
                    .read_from_masters_timeout(std::time::Duration::from_millis(200))
                    .await
                {
                    Ok(Some(_frame)) => {
                        // Dispatched internally by handle_master_frame via
                        // the unified request table / peer routing. The
                        // returned pass-through frame has no owner in the
                        // background path — drop it.
                    }
                    Ok(None) => {
                        // Timeout or all-masters-closed; loop to re-check
                        // the stop flag before blocking again.
                    }
                    Err(e) => {
                        tracing::warn!(
                            "[RelaySwitch] background pump: relay error (continuing): {}",
                            e
                        );
                    }
                }
            }
        });
        guard.push(handle);

        // Spawn the runtime identity-probe driver (see
        // `spawn_identity_probe_driver`). Separated so a host that drives
        // master reads with its own loop — and therefore must NOT start
        // the background frame pump above — can still service runtime
        // capability transitions by calling that method directly.
        if let Some(probe_handle) = self.spawn_identity_probe_driver() {
            guard.push(probe_handle);
        }

        // Registry definitions can arrive after a RelayNotify advertised
        // their URNs. LiveCapFab intentionally drops caps whose canonical
        // definitions are not yet cached; this listener retries the current
        // aggregate cap set whenever the registry cache warms, without
        // requiring a cartridge reconnect or capability-set change.
        let mut cache_revisions = self.fabric_registry.subscribe_cache_revisions();
        let weak_registry = Arc::downgrade(self);
        let stop_registry = self.background_pump_stop.clone();
        let registry_handle = tokio::spawn(async move {
            loop {
                if stop_registry.load(Ordering::Relaxed) {
                    break;
                }
                if cache_revisions.changed().await.is_err() {
                    break;
                }
                let Some(switch) = weak_registry.upgrade() else {
                    break;
                };
                switch
                    .rebuild_live_cap_fab_from_current_capabilities("registry_cache_revision")
                    .await;
            }
        });
        guard.push(registry_handle);
    }

    /// Spawn the runtime identity-probe driver. It owns the
    /// `pending_identity_probes_rx` receiver (taken once) and serially
    /// probes each master that flipped from empty caps to non-empty caps
    /// in the last RelayNotify update; on success it flips the master
    /// healthy and rebuilds the cap table so its caps become routable, on
    /// failure it keeps the master unhealthy and stamps `last_error`.
    ///
    /// Probes run in their own task so they never block the caller's frame
    /// loop — the probe writes via `write_to_master_idx`, the caller's
    /// master-read loop (the background pump, or a host process's own
    /// multiplexer) still drives master reads, and the response routes back
    /// through the request entry's external response channel that
    /// `run_identity_probe_via_relay` registers.
    ///
    /// `start_background_pump` calls this. A host that runs its OWN
    /// master-read loop (e.g. the interop router process, which forwards
    /// master frames to stdout and so must not start the background pump)
    /// calls this directly, so dynamic capability transitions advertised
    /// via a later RelayNotify still get probed and become routable.
    /// Returns the task handle, or `None` if the receiver was already
    /// taken — idempotent, a second call is a no-op.
    pub fn spawn_identity_probe_driver(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<()>> {
        let probes_rx = self
            .pending_identity_probes_rx
            .lock()
            .expect("pending_identity_probes_rx mutex poisoned")
            .take()?;
        let weak_probe = Arc::downgrade(self);
        let stop_probe = self.background_pump_stop.clone();
        Some(tokio::spawn(async move {
            let mut rx = probes_rx;
            loop {
                if stop_probe.load(Ordering::Relaxed) {
                    break;
                }
                let master_idx = match rx.recv().await {
                    Some(idx) => idx,
                    None => break, // sender dropped — relay torn down
                };
                let Some(switch) = weak_probe.upgrade() else {
                    break;
                };
                match switch.run_identity_probe_via_relay(master_idx).await {
                    Ok(()) => {
                        // Probe passed — flip the master back to healthy and
                        // rebuild the cap table so its caps become routable.
                        // We held the master unhealthy from the moment caps
                        // went non-empty until verification completed; this
                        // is the natural reverse.
                        let masters = switch.masters.read().await;
                        if let Some(master) = masters.get(master_idx) {
                            master.healthy.store(true, Ordering::SeqCst);
                            master.last_error.write().await.take();
                        }
                        drop(masters);
                        switch.rebuild_cap_table().await;
                        switch.rebuild_capabilities().await;
                        info!(
                            target: "relay_switch",
                            master_idx = master_idx,
                            "[RelaySwitch] runtime identity probe passed — master is now healthy"
                        );
                    }
                    Err(detail) => {
                        // Probe failed — keep the master unhealthy and stamp
                        // `last_error` so the inventory surface shows the
                        // reason. The master's caps stay published as-is
                        // (they came from the host's RelayNotify), but
                        // `cap_table` skips unhealthy masters during dispatch
                        // so engine REQs won't route here.
                        tracing::error!(
                            target: "relay_switch",
                            master_idx = master_idx,
                            error = %detail,
                            "[RelaySwitch] runtime identity probe FAILED — master remains unhealthy"
                        );
                        let masters = switch.masters.read().await;
                        if let Some(master) = masters.get(master_idx) {
                            master.healthy.store(false, Ordering::SeqCst);
                            *master.last_error.write().await = Some(detail);
                        }
                        drop(masters);
                        switch.rebuild_cap_table().await;
                        switch.rebuild_capabilities().await;
                    }
                }
            }
        }))
    }

    /// Get the negotiated limits (minimum across all masters).
    pub async fn limits(&self) -> Limits {
        self.negotiated_limits.read().await.clone()
    }

    /// Get all reachable targets from a source media URN.
    ///
    /// Uses the prebuilt LiveCapFab for efficient BFS traversal.
    /// Results are sorted by (min_path_length, display_name).
    pub async fn get_reachable_targets(
        &self,
        source: &MediaUrn,
        is_sequence: bool,
        max_depth: usize,
    ) -> Vec<ReachableTargetInfo> {
        let graph = self.live_cap_fab.read().await;
        graph.get_reachable_targets(source, is_sequence, max_depth)
    }

    /// Find all paths from source to an exact target media URN.
    ///
    /// `is_sequence` is the initial cardinality state from context.
    /// Results are sorted by (total_steps, specificity desc, cap_urns).
    pub async fn find_paths_to_exact_target(
        &self,
        source: &MediaUrn,
        target: &MediaUrn,
        is_sequence: bool,
        max_depth: usize,
        max_paths: usize,
    ) -> Vec<Strand> {
        self.find_paths_to_exact_target_with_step_title_query(
            source,
            target,
            is_sequence,
            max_depth,
            max_paths,
            None,
        )
        .await
    }

    pub async fn find_paths_to_exact_target_with_step_title_query(
        &self,
        source: &MediaUrn,
        target: &MediaUrn,
        is_sequence: bool,
        max_depth: usize,
        max_paths: usize,
        step_title_query: Option<&str>,
    ) -> Vec<Strand> {
        let graph = self.live_cap_fab.read().await;
        graph.find_paths_to_exact_target_with_step_title_query(
            source,
            target,
            is_sequence,
            max_depth,
            max_paths,
            step_title_query,
        )
    }

    /// Plan candidate machines through the unified configurable planner —
    /// THE general entry (multi-source convergence, multi-target divergence,
    /// every knob of `PlanRequest`). The historical calls above are presets
    /// over the same graph: `|sources| == 1` with default policies reproduces
    /// `find_paths_to_exact_target`'s strands exactly.
    pub async fn plan(
        &self,
        request: &crate::planner::PlanRequest,
    ) -> Result<crate::planner::PlanOutcome, crate::planner::PlanError> {
        let graph = self.live_cap_fab.read().await;
        graph.plan(request, &self.fabric_registry)
    }

    /// `plan` with a streaming observer: `on_candidate` fires per NEW
    /// (deduplicated) candidate as it is assembled — the transport behind the
    /// PlanMachinesStream RPC. The returned outcome carries the final ranked
    /// list plus any dead-end sources.
    pub async fn plan_with_observer<F>(
        &self,
        request: &crate::planner::PlanRequest,
        on_candidate: F,
    ) -> Result<crate::planner::PlanOutcome, crate::planner::PlanError>
    where
        F: FnMut(&crate::planner::PlanCandidate),
    {
        let graph = self.live_cap_fab.read().await;
        graph.plan_with_observer(request, &self.fabric_registry, on_candidate)
    }

    /// Discover the reachable targets for a (possibly heterogeneous) source
    /// set: convergent targets tagged with their apex, plus independent (map)
    /// targets, plus any DEAD-END sources (no reachable target at all). The
    /// multi-source generalization of `get_reachable_targets`.
    pub async fn discover_convergent_targets(
        &self,
        sources: &[crate::planner::SourceSpec],
        max_depth: usize,
    ) -> Result<crate::planner::ConvergentTargets, crate::planner::PlanError> {
        let graph = self.live_cap_fab.read().await;
        graph.discover_convergent_targets(sources, max_depth)
    }

    /// Find paths with streaming progress callback.
    pub async fn find_paths_streaming<F>(
        &self,
        source: &MediaUrn,
        target: &MediaUrn,
        is_sequence: bool,
        max_depth: usize,
        max_paths: usize,
        cancelled: &std::sync::atomic::AtomicBool,
        on_event: F,
    ) -> Vec<Strand>
    where
        F: FnMut(crate::planner::PathFindingEvent),
    {
        self.find_paths_streaming_with_step_title_query(
            source,
            target,
            is_sequence,
            max_depth,
            max_paths,
            None,
            cancelled,
            on_event,
        )
        .await
    }

    pub async fn find_paths_streaming_with_step_title_query<F>(
        &self,
        source: &MediaUrn,
        target: &MediaUrn,
        is_sequence: bool,
        max_depth: usize,
        max_paths: usize,
        step_title_query: Option<&str>,
        cancelled: &std::sync::atomic::AtomicBool,
        on_event: F,
    ) -> Vec<Strand>
    where
        F: FnMut(crate::planner::PathFindingEvent),
    {
        let graph = self.live_cap_fab.read().await;
        graph.find_paths_streaming_with_step_title_query(
            source,
            target,
            is_sequence,
            max_depth,
            max_paths,
            step_title_query,
            cancelled,
            on_event,
        )
    }

    /// Get the unified fabric registry used by this switch.
    pub fn fabric_registry(&self) -> &Arc<FabricRegistry> {
        &self.fabric_registry
    }

    /// Get health status of all masters.
    ///
    /// Returns a snapshot of each master's health status including:
    /// - Whether the master is healthy
    /// - Number of capabilities it provides
    /// - How long it has been connected
    /// - Last error message if unhealthy
    pub async fn get_master_health(&self) -> Vec<MasterHealthStatus> {
        let masters = self.masters.read().await;
        let mut result = Vec::with_capacity(masters.len());
        for (idx, master) in masters.iter().enumerate() {
            let healthy = master.healthy.load(Ordering::SeqCst);
            let cap_count = master.caps.read().await.len();
            let connected_seconds = master
                .connected_at
                .lock()
                .expect("connected_at mutex poisoned")
                .elapsed()
                .as_secs();
            let last_error = master.last_error.read().await.clone();
            result.push(MasterHealthStatus {
                index: idx,
                healthy,
                cap_count,
                connected_seconds,
                last_error,
            });
        }
        result
    }

    /// Get the total count of masters (healthy and unhealthy).
    pub async fn master_count(&self) -> usize {
        self.masters.read().await.len()
    }

    /// Get the count of healthy masters.
    pub async fn healthy_master_count(&self) -> usize {
        let masters = self.masters.read().await;
        masters
            .iter()
            .filter(|m| m.healthy.load(Ordering::SeqCst))
            .count()
    }

    /// Declare how many RelayMasters this engine intends to register
    /// at startup. The readiness predicate (`all_masters_ready`) only
    /// returns true once `masters.len() >= expected` AND every
    /// connected master is ready. Without this an engine that has
    /// only finished registering its internal master would falsely
    /// report ready before the external-cartridges master finished
    /// spawning + HELLO + cap-probing its cartridges.
    ///
    /// Both editions expect 2 masters (internal + external/XPC).
    /// Set once at engine boot from the same call site that registers
    /// the cartridges.
    pub fn set_expected_master_count(&self, expected: usize) {
        self.expected_master_count.store(expected, Ordering::SeqCst);
    }

    /// Wait until a master advertises a cap that is dispatchable for
    /// the given request URN, or until `timeout` elapses. Returns the
    /// master index on success, or `None` on timeout.
    ///
    /// This is the synchronization point between async master setup
    /// (where a RelayNotify with real caps lands some time after the
    /// master is added) and a synchronous-feeling caller that wants
    /// to invoke `execute_cap` and have it route to the right master.
    /// Without this wait, callers race the RelayNotify and observe
    /// an empty cap table.
    pub async fn wait_for_cap(&self, cap_urn: &str, timeout: std::time::Duration) -> Option<usize> {
        let started = std::time::Instant::now();
        loop {
            if let Some(idx) = self.find_master_for_cap(cap_urn, None).await {
                return Some(idx);
            }
            if started.elapsed() >= timeout {
                return None;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    /// True when:
    ///   1. The number of connected masters is at least
    ///      `expected_master_count` (declared via
    ///      `set_expected_master_count`), AND
    ///   2. Every connected master is healthy.
    ///
    /// **Cap-set non-emptiness is intentionally NOT required.** A
    /// master can be healthy and connected with zero caps while its
    /// cartridges are still inspecting / verifying — see
    /// `machfab-mac/docs/cartridge state machine.md`. Tying readiness
    /// to caps would mean the splash screen waits for every cartridge
    /// to clear inspection + verification, which can take many seconds
    /// for large model cartridges + slow registry fetches. Caps
    /// register incrementally as cartridges progress to `Operational`;
    /// the dispatch table grows under the engine over time.
    ///
    /// Editions differ only in the expected master count (see
    /// `set_expected_master_count`). The engine hosts no in-process
    /// cartridges, so there is no internal-cartridges slot:
    ///   - WEBSITE: 2 (engine external-cartridges, XPC service).
    ///   - MAS: 1 (engine external-cartridges — no XPC service).
    ///
    /// The host app polls this through the engine's
    /// `HealthStatus.cartridges_ready` field to flip its own
    /// readiness gate from `.configuring` to `.ready`. The name of
    /// that field is historical — what it actually signals is "all
    /// expected masters connected and healthy", which is decoupled
    /// from any specific cartridge's lifecycle.
    pub async fn all_masters_ready(&self) -> bool {
        let expected = self.expected_master_count.load(Ordering::SeqCst);
        if expected == 0 {
            // Engine never declared an expected count — treat as
            // not-yet-configured rather than guess. Caller bug.
            return false;
        }
        let masters = self.masters.read().await;
        if masters.len() < expected {
            return false;
        }
        for master in masters.iter() {
            // An unhealthy master is by definition not ready.
            if !master.healthy.load(Ordering::SeqCst) {
                return false;
            }
        }
        true
    }

    /// Execute a cap and return a receiver for streaming response frames.
    ///
    /// This is the high-level API for calling caps programmatically.
    /// The returned receiver will receive all response frames (STREAM_START, CHUNK, END, ERR, etc.)
    /// until the request completes.
    ///
    /// # Arguments
    /// * `cap_urn` - The capability URN to execute
    /// * `payload` - The request payload bytes
    /// * `content_type` - The content type of the payload (e.g., "application/cbor", "application/json")
    ///
    /// # Returns
    /// A tuple of (request_id, receiver). The request_id can be used with
    /// `send_to_master()` to send streaming continuation frames (STREAM_START,
    /// CHUNK, STREAM_END, END) for this request. The receiver streams response
    /// frames — read from it until END or ERR.
    /// Protocol observability snapshot (L8): every live request's phase, age,
    /// per-stream flow counters, and children; the recently-terminated ring;
    /// and the per-reason drop totals. Poll this to understand the state of
    /// communications and the flow of requests through the switch.
    /// Install a termination observer on the request table (L8): called with
    /// every termination's summary, under the table guard — must be cheap.
    /// Lets an engine accumulate complete per-run history without missing
    /// terminations between stats polls (the ring evicts at 64).
    pub async fn set_terminate_observer(
        &self,
        observer: Box<dyn Fn(&crate::bifaci::request_state::TerminatedSummary) + Send + Sync>,
    ) {
        self.requests.write().await.set_terminate_observer(observer);
    }

    pub async fn protocol_stats(&self) -> RelaySwitchProtocolStats {
        let mut hosts = std::collections::BTreeMap::new();
        {
            let masters = self.masters.read().await;
            for master in masters.iter() {
                if let Some(stats) = master.host_protocol_stats.read().await.clone() {
                    hosts.insert(master.id.clone(), stats);
                }
            }
        }
        RelaySwitchProtocolStats {
            requests: self.requests.read().await.snapshot(),
            drops: self.drops.snapshot(),
            stragglers: self.stragglers.snapshot(),
            hosts,
        }
    }

    /// Account a flow frame that found no routing state, for the narrow case
    /// it actually is: when the terminated ledger vouches the request JUST
    /// terminated, the frame is a benign post-terminal straggler — the
    /// expected teardown crossing, counted per frame type and logged as
    /// benign, never as a drop. Otherwise the RID is one the table never
    /// knew: a genuine routing anomaly, counted as a `no_route` drop and
    /// logged as a warning.
    fn account_unrouted_frame(&self, recently_terminated: bool, frame: &Frame, context: &str) {
        if recently_terminated {
            let total = self.stragglers.record(frame.frame_type);
            tracing::debug!(
                rid = ?frame.id,
                xid = ?frame.routing_id,
                ftype = frame.frame_type.as_str(),
                straggler_total = total,
                "[RelaySwitch] benign post-terminal straggler ({}): frame crossed its \
                 request's terminal in flight — expected teardown race, nothing lost",
                context
            );
        } else {
            let total = self
                .drops
                .record(crate::bifaci::frame::DropReason::NoRoute, frame.frame_type);
            tracing::warn!(
                rid = ?frame.id,
                xid = ?frame.routing_id,
                ftype = frame.frame_type.as_str(),
                no_route_total = total,
                "[RelaySwitch] dropped {} — RID has no routing state and never terminated here (no_route)",
                context
            );
        }
    }

    fn admission_key(master_idx: usize, cartridge: &InstalledCartridgeRecord) -> AdmissionKey {
        AdmissionKey {
            master_idx,
            registry_url: cartridge.registry_url.clone(),
            channel: cartridge.channel.as_str().to_string(),
            id: cartridge.id.clone(),
            version: cartridge.version.clone(),
            sha256: cartridge.sha256.clone(),
        }
    }

    fn configure_master_admission(
        &self,
        master_idx: usize,
        cartridges: &[InstalledCartridgeRecord],
    ) -> Result<(), RelaySwitchError> {
        let mut available = HashSet::with_capacity(cartridges.len());
        for cartridge in cartridges {
            // An INCOMPATIBLE install (attachment_error set) is inventory, not
            // a handler: it was never spawned, so it legitimately has no
            // runtime stats and nothing to admit. It must not cost the master
            // its admission — one broken cartridge.json on disk would
            // otherwise refuse the ENTIRE relay, taking every healthy
            // cartridge on it down with it (exactly the cascade a surfaced
            // incompatible exists to prevent). Cap routing already excludes
            // these records (`cap_admission_target` filters on
            // `attachment_error.is_none()`), so skipping them here admits
            // nothing that could be dispatched to.
            if cartridge.attachment_error.is_some() {
                continue;
            }
            let Some(stats) = cartridge.runtime_stats.as_ref() else {
                return Err(RelaySwitchError::Protocol(format!(
                    "cartridge '{}' on master {} is missing mandatory v4 runtime_stats",
                    cartridge.id, master_idx
                )));
            };
            let key = Self::admission_key(master_idx, cartridge);
            // A host may expose several process instances of the same logical
            // install (the interop multi-cartridge topology does this). They
            // share one admission identity. Preserve the first host-ordered
            // record, matching host dispatch, rather than letting a later
            // duplicate overwrite its effective capacity.
            if available.insert(key.clone()) {
                self.admission
                    .configure_pools(key, &Self::pool_capacities(stats, &cartridge.id)?);
            }
        }
        self.admission.reconcile_master(master_idx, &available);
        Ok(())
    }

    /// The EFFECTIVE capacity of every pool a record advertises, as the
    /// admission slots to configure. A NOT-RUNNING cartridge admits one
    /// probe request total: `all` is clamped to 1 until the process is up
    /// and a fresh map has arrived — the canary rule. This is deliberately
    /// NOT about missing information (declared capacities are known while
    /// cold); it is failure containment: a binary that dies on spawn eats
    /// one request, not a declared-width fan-out, and a cold process cannot
    /// yet self-report the `available` it would declare one heartbeat after
    /// spawning.
    fn pool_capacities(
        stats: &CartridgeRuntimeStats,
        cartridge_id: &str,
    ) -> Result<Vec<(String, usize)>, RelaySwitchError> {
        if stats.pools.is_empty() {
            if stats.running {
                return Err(RelaySwitchError::Protocol(format!(
                    "cartridge '{cartridge_id}' is running but advertises no concurrency pools — the pool map is mandatory once HELLO has completed"
                )));
            }
            // A cold record before its first HELLO legitimately has no pool
            // map yet (registered-dir cartridges spawn on first dispatch).
            // Admit through the canary alone: `all` clamped to 1, so the
            // first body proves the spawn before real capacities exist.
            return Ok(vec![(crate::bifaci::pools::POOL_ALL.to_string(), 1)]);
        }
        let mut capacities = Vec::with_capacity(stats.pools.len());
        for (name, state) in &stats.pools {
            let effective = if !stats.running && name == crate::bifaci::pools::POOL_ALL {
                1
            } else {
                usize::try_from(state.effective()).map_err(|_| {
                    RelaySwitchError::Protocol(format!(
                        "cartridge '{cartridge_id}' pool '{name}' capacity exceeds this host's address space"
                    ))
                })?
            };
            capacities.push((name.clone(), effective));
        }
        Ok(capacities)
    }

    async fn cap_admission_target(
        &self,
        master_idx: usize,
        registered_cap: &str,
    ) -> Result<Vec<(crate::bifaci::request_state::PoolKey, usize)>, RelaySwitchError> {
        let cartridges = {
            let masters = self.masters.read().await;
            let master = masters.get(master_idx).ok_or_else(|| {
                RelaySwitchError::Protocol(format!(
                    "selected master index {} no longer exists",
                    master_idx
                ))
            })?;
            let cartridges = master.installed_cartridges.read().await.clone();
            cartridges
        };
        let mut matches = cartridges.iter().filter(|cartridge| {
            cartridge.attachment_error.is_none()
                && cartridge
                    .cap_urns()
                    .iter()
                    .any(|candidate| candidate == registered_cap)
        });
        let cartridge = matches.next().ok_or_else(|| {
            RelaySwitchError::Protocol(format!(
                "master {} advertises cap '{}' without an installed-cartridge owner",
                master_idx, registered_cap
            ))
        })?;
        let key = Self::admission_key(master_idx, cartridge);
        if matches.any(|candidate| Self::admission_key(master_idx, candidate) != key) {
            return Err(RelaySwitchError::Protocol(format!(
                "master {} has multiple distinct installed cartridges claiming cap '{}'; routing is ambiguous",
                master_idx, registered_cap
            )));
        }
        let stats = cartridge.runtime_stats.as_ref().ok_or_else(|| {
            RelaySwitchError::Protocol(format!(
                "cartridge '{}' on master {} is missing mandatory v4 runtime_stats",
                cartridge.id, master_idx
            ))
        })?;
        self.admission
            .configure_pools(key.clone(), &Self::pool_capacities(stats, &cartridge.id)?);
        Self::admission_chain(&key, stats, registered_cap, &cartridge.id)
    }

    /// The cap's pool CHAIN over one record's advertised pool map — each
    /// admission domain a dispatch of this cap is held against, in order
    /// (singleton, declared pools, `all`), paired with that pool's effective
    /// capacity (0 = unlimited). A cap the pool map does not cover is a
    /// protocol error, never a free pass.
    fn admission_chain(
        install: &AdmissionKey,
        stats: &CartridgeRuntimeStats,
        registered_cap: &str,
        cartridge_id: &str,
    ) -> Result<Vec<(crate::bifaci::request_state::PoolKey, usize)>, RelaySwitchError> {
        if stats.pools.is_empty() {
            if stats.running {
                return Err(RelaySwitchError::Protocol(format!(
                    "cartridge '{cartridge_id}' is running but advertises no concurrency pools — the pool map is mandatory once HELLO has completed"
                )));
            }
            // Cold record before its first HELLO: the whole dispatch is the
            // canary — one body through the clamped `all` pool, which is
            // what triggers the spawn and the real pool map.
            return Ok(vec![(
                crate::bifaci::request_state::PoolKey {
                    install: install.clone(),
                    pool: crate::bifaci::pools::POOL_ALL.to_string(),
                },
                1,
            )]);
        }
        let canonical = crate::urn::cap_urn::CapUrn::from_string(registered_cap)
            .map_err(|e| {
                RelaySwitchError::Protocol(format!(
                    "registered cap '{registered_cap}' is not a valid cap URN: {e}"
                ))
            })?
            .to_string();
        let names = crate::bifaci::pools::chain_from_states(&stats.pools, &canonical);
        if names.first() != Some(&canonical)
            || names.last() != Some(&crate::bifaci::pools::POOL_ALL.to_string())
        {
            return Err(RelaySwitchError::Protocol(format!(
                "cartridge '{cartridge_id}' advertises cap '{canonical}' with no pool coverage — its pool map is missing the cap's singleton or the '{}' pool",
                crate::bifaci::pools::POOL_ALL
            )));
        }
        let mut chain = Vec::with_capacity(names.len());
        for name in names {
            let effective = if !stats.running && name == crate::bifaci::pools::POOL_ALL {
                // The cold-start canary clamp — see `pool_capacities`.
                1
            } else {
                usize::try_from(stats.pools[&name].effective()).map_err(|_| {
                    RelaySwitchError::Protocol(format!(
                        "cartridge '{cartridge_id}' pool '{name}' capacity exceeds this host's address space"
                    ))
                })?
            };
            chain.push((
                crate::bifaci::request_state::PoolKey {
                    install: install.clone(),
                    pool: name,
                },
                effective,
            ));
        }
        Ok(chain)
    }

    async fn acquire_cap_admission(
        &self,
        master_idx: usize,
        registered_cap: &str,
    ) -> Result<AdmissionPermit, RelaySwitchError> {
        let chain = self
            .cap_admission_target(master_idx, registered_cap)
            .await?;
        self.admission
            .acquire(chain.into_iter().map(|(key, _)| key).collect())
            .await
            .map_err(RelaySwitchError::CartridgeUnavailable)
    }

    /// Return the authoritative minimum effective capacity across the pool
    /// chain serving a cap (0 = every pool unlimited). A positive capacity is
    /// an execution boundary: callers must not pre-acquire that request as
    /// part of a multi-cap live pipeline, because the permit represents an
    /// actively owned process slot. Zero means unlimited and permits live
    /// pre-opening.
    pub async fn admission_capacity_for_cap(
        &self,
        cap_urn: &str,
    ) -> Result<usize, RelaySwitchError> {
        let chain = self.admission_target_for_cap(cap_urn).await?;
        Ok(chain
            .into_iter()
            .map(|(_, capacity)| capacity)
            .filter(|&capacity| capacity > 0)
            .min()
            .unwrap_or(0))
    }

    /// The ADMISSION CHAIN serving a cap — each (master, install, pool)
    /// permit domain a dispatch is held against, paired with that pool's
    /// effective capacity (0 = unlimited). One relay slot can aggregate MANY
    /// cartridge processes (machfab's external-cartridges RelaySlave), each
    /// with its own independent pools. Callers deciding whether two caps
    /// contend for the same permits must compare CHAIN MEMBERSHIP: two caps
    /// contend iff their chains share any pool with capacity > 0.
    pub async fn admission_target_for_cap(
        &self,
        cap_urn: &str,
    ) -> Result<Vec<(crate::bifaci::request_state::PoolKey, usize)>, RelaySwitchError> {
        let (master_idx, registered_cap) = self
            .find_route_for_cap(cap_urn, None)
            .await
            .ok_or_else(|| RelaySwitchError::NoHandler(cap_urn.to_string()))?;
        self.cap_admission_target(master_idx, &registered_cap).await
    }

    async fn acquire_cartridge_admission(
        &self,
        master_idx: usize,
        cartridge_id: &str,
    ) -> Result<AdmissionPermit, RelaySwitchError> {
        let cartridge = {
            let masters = self.masters.read().await;
            let master = masters.get(master_idx).ok_or_else(|| {
                RelaySwitchError::Protocol(format!(
                    "selected master index {} no longer exists",
                    master_idx
                ))
            })?;
            let cartridges = master.installed_cartridges.read().await;
            let mut matches = cartridges.iter().filter(|record| record.id == cartridge_id);
            let cartridge = matches.next().cloned().ok_or_else(|| {
                RelaySwitchError::Protocol(format!(
                    "cartridge '{}' is not installed on master {}",
                    cartridge_id, master_idx
                ))
            })?;
            if matches.next().is_some() {
                return Err(RelaySwitchError::Protocol(format!(
                    "cartridge id '{}' is ambiguous on master {}; use the full install identity",
                    cartridge_id, master_idx
                )));
            }
            cartridge
        };
        let stats = cartridge.runtime_stats.as_ref().ok_or_else(|| {
            RelaySwitchError::Protocol(format!(
                "cartridge '{}' on master {} is missing mandatory v4 runtime_stats",
                cartridge.id, master_idx
            ))
        })?;
        if !stats.pools.contains_key(crate::bifaci::pools::POOL_ALL)
            && !(stats.pools.is_empty() && !stats.running)
        {
            return Err(RelaySwitchError::Protocol(format!(
                "cartridge '{}' pool map is missing the mandatory '{}' pool",
                cartridge.id,
                crate::bifaci::pools::POOL_ALL
            )));
        }
        let key = Self::admission_key(master_idx, &cartridge);
        self.admission
            .configure_pools(key.clone(), &Self::pool_capacities(stats, &cartridge.id)?);
        let chain = vec![crate::bifaci::request_state::PoolKey {
            install: key,
            pool: crate::bifaci::pools::POOL_ALL.to_string(),
        }];
        self.admission
            .acquire(chain)
            .await
            .map_err(RelaySwitchError::CartridgeUnavailable)
    }

    /// The live pool CHAIN serving a cap, with FULL pool states — the
    /// display shape (run cap-node capacity meters, step inspector chain
    /// rows): the owning cartridge id plus each chain pool's name and
    /// state, in admission order. Read-only roster truth; refreshed by
    /// every heartbeat republish.
    pub async fn pool_chain_states_for_cap(
        &self,
        cap_urn: &str,
    ) -> Result<(String, Vec<(String, crate::bifaci::pools::PoolState)>), RelaySwitchError> {
        let (master_idx, registered_cap) = self
            .find_route_for_cap(cap_urn, None)
            .await
            .ok_or_else(|| RelaySwitchError::NoHandler(cap_urn.to_string()))?;
        let masters = self.masters.read().await;
        let master = masters.get(master_idx).ok_or_else(|| {
            RelaySwitchError::Protocol(format!(
                "selected master index {master_idx} no longer exists"
            ))
        })?;
        let cartridges = master.installed_cartridges.read().await;
        let record = cartridges
            .iter()
            .find(|record| {
                record.attachment_error.is_none()
                    && record
                        .cap_urns()
                        .iter()
                        .any(|candidate| candidate == &registered_cap)
            })
            .ok_or_else(|| {
                RelaySwitchError::Protocol(format!(
                    "master {master_idx} advertises cap '{registered_cap}' without an installed-cartridge owner"
                ))
            })?;
        let stats = record.runtime_stats.as_ref().ok_or_else(|| {
            RelaySwitchError::Protocol(format!(
                "cartridge '{}' on master {master_idx} is missing mandatory v4 runtime_stats",
                record.id
            ))
        })?;
        let canonical = crate::urn::cap_urn::CapUrn::from_string(&registered_cap)
            .map_err(|e| {
                RelaySwitchError::Protocol(format!(
                    "registered cap '{registered_cap}' is not a valid cap URN: {e}"
                ))
            })?
            .to_string();
        let chain = crate::bifaci::pools::chain_from_states(&stats.pools, &canonical)
            .into_iter()
            .map(|name| {
                let state = stats.pools[&name].clone();
                (name, state)
            })
            .collect();
        Ok((record.id.clone(), chain))
    }

    /// Deliver operator `configured` values for one cartridge's pools to
    /// the master that carries it (the heartbeat is the config channel on
    /// the far side — see `bifaci::pools`). Validated hard against the
    /// roster's last-known pool map: an unknown cartridge or pool name is
    /// refused with the offender named, and nothing is sent. Delivery rides
    /// a RelayState frame (master→slave control plane); the slave's host
    /// applies the values and the refreshed pool map returns on the next
    /// roster republish.
    pub async fn send_desired_capacities(
        &self,
        cartridge_id: &str,
        desired: &crate::bifaci::pools::DesiredCapacities,
    ) -> Result<(), RelaySwitchError> {
        // Resolve the owning master + validate against its advertised map.
        let masters = self.masters.read().await;
        let mut target: Option<usize> = None;
        for (idx, master) in masters.iter().enumerate() {
            let cartridges = master.installed_cartridges.read().await;
            let mut matches = cartridges
                .iter()
                .filter(|record| record.attachment_error.is_none() && record.id == cartridge_id);
            if let Some(record) = matches.next() {
                if target.is_some() {
                    return Err(RelaySwitchError::Protocol(format!(
                        "cartridge id '{cartridge_id}' is carried by multiple masters; addressing is ambiguous"
                    )));
                }
                let stats = record.runtime_stats.as_ref().ok_or_else(|| {
                    RelaySwitchError::Protocol(format!(
                        "cartridge '{cartridge_id}' on master {idx} is missing mandatory v4 runtime_stats"
                    ))
                })?;
                for pool in desired.keys() {
                    if !stats.pools.contains_key(pool) {
                        return Err(RelaySwitchError::Protocol(format!(
                            "desired capacities name pool '{pool}', which cartridge '{cartridge_id}' does not declare"
                        )));
                    }
                }
                target = Some(idx);
            }
        }
        let Some(master_idx) = target else {
            return Err(RelaySwitchError::Protocol(format!(
                "desired capacities address cartridge '{cartridge_id}', which no master carries"
            )));
        };
        let payload = serde_json::to_vec(&serde_json::json!({
            "desired_capacities": {
                "cartridge_id": cartridge_id,
                "capacities": desired,
            }
        }))
        .expect("desired-capacities RelayState payload is always JSON-encodable");
        let frame = Frame::relay_state(&payload);
        let master = &masters[master_idx];
        let mut writer = master.socket_writer.lock().await;
        writer.write(&frame).await.map_err(|e| {
            RelaySwitchError::Protocol(format!(
                "failed to send desired-capacities RelayState to master {master_idx}: {e}"
            ))
        })
    }

    /// Register an externally-originated request (engine / execute_cap
    /// caller): response channel, routing, origin, and rid index land in one
    /// atomic table registration (L7). Duplicate registration is a protocol
    /// violation and fails hard.
    async fn register_external(
        &self,
        key: (MessageId, MessageId),
        dest_idx: usize,
        tx: mpsc::UnboundedSender<Frame>,
        cap_urn: Option<String>,
        permit: Option<AdmissionPermit>,
    ) -> Result<(), RelaySwitchError> {
        let initial_credit = self.master_initial_credit(dest_idx).await;
        let mut state = RequestState::new(
            RoutingEntry {
                source_master_idx: None,
                destination_master_idx: dest_idx,
            },
            None,
            Some(tx),
            false,
            initial_credit,
        )
        .with_cap_urn(cap_urn);
        if let Some(permit) = permit {
            state = state.with_admission_permit(permit);
        }
        self.requests
            .write()
            .await
            .register(key, state)
            .map_err(RelaySwitchError::Protocol)
    }

    /// Forward the first REQ after registration. If the write fails there can
    /// be no terminal response, so remove that exact request immediately;
    /// dropping its state also releases its admission permit.
    async fn write_initial_request(
        &self,
        key: &(MessageId, MessageId),
        dest_idx: usize,
        frame: &mut Frame,
    ) -> Result<(), RelaySwitchError> {
        match self.write_to_master_idx(dest_idx, frame).await {
            Ok(()) => Ok(()),
            Err(error) => {
                self.requests
                    .write()
                    .await
                    .terminate(key, TerminalKind::MasterDied);
                Err(error.into())
            }
        }
    }

    pub async fn execute_cap(
        &self,
        cap_urn: &str,
        payload: Vec<u8>,
        content_type: &str,
    ) -> Result<(MessageId, mpsc::UnboundedReceiver<Frame>), RelaySwitchError> {
        // Generate unique request ID
        let rid = MessageId::Uint(self.xid_counter.fetch_add(1, Ordering::SeqCst) + 1);

        // Build REQ frame
        let req_frame = Frame::req(rid.clone(), cap_urn, payload, content_type);

        // Create response channel
        let (tx, rx) = mpsc::unbounded_channel();

        // Find master that can handle this cap (no preference for internal requests)
        let (dest_idx, registered_cap) = self
            .find_route_for_cap(cap_urn, None)
            .await
            .ok_or_else(|| RelaySwitchError::NoHandler(cap_urn.to_string()))?;
        let permit = self
            .acquire_cap_admission(dest_idx, &registered_cap)
            .await?;

        // Assign XID
        let xid = MessageId::Uint(self.xid_counter.fetch_add(1, Ordering::SeqCst) + 1);
        let key = (xid.clone(), rid.clone());

        // Register response channel + routing + rid index BEFORE sending (L7)
        self.register_external(
            key.clone(),
            dest_idx,
            tx,
            Some(cap_urn.to_string()),
            Some(permit),
        )
        .await?;

        // Build frame with XID
        let mut frame_with_xid = req_frame;
        frame_with_xid.routing_id = Some(xid);

        // Forward to destination
        self.write_initial_request(&key, dest_idx, &mut frame_with_xid)
            .await?;

        Ok((rid, rx))
    }

    /// Register an external request and return the assigned XID and response channel.
    ///
    /// This is used when the caller builds their own frames via `build_request_frames`.
    /// The caller must stamp the returned XID onto all frames (via `frame.routing_id = Some(xid)`)
    /// before sending them via `send_to_master`. The switch will route responses to the
    /// returned channel.
    ///
    /// Unlike `execute_cap`, this doesn't send any frames - it only sets up routing.
    ///
    /// Returns `(xid, response_receiver)` - the caller MUST set `frame.routing_id = Some(xid)`
    /// on all frames before sending them.
    pub async fn register_external_request(
        &self,
        rid: MessageId,
        cap_urn: &str,
        preferred_cap: Option<&str>,
    ) -> Result<(MessageId, mpsc::UnboundedReceiver<Frame>), RelaySwitchError> {
        // Find master that can handle this cap
        let (dest_idx, registered_cap) = self
            .find_route_for_cap(cap_urn, preferred_cap)
            .await
            .ok_or_else(|| RelaySwitchError::NoHandler(cap_urn.to_string()))?;
        let permit = self
            .acquire_cap_admission(dest_idx, &registered_cap)
            .await?;

        // Assign XID
        let xid = MessageId::Uint(self.xid_counter.fetch_add(1, Ordering::SeqCst) + 1);
        let key = (xid.clone(), rid.clone());

        // Create response channel
        let (tx, rx) = mpsc::unbounded_channel();

        // Register response channel + routing + rid index BEFORE sending (L7)
        self.register_external(
            key.clone(),
            dest_idx,
            tx,
            Some(cap_urn.to_string()),
            Some(permit),
        )
        .await?;

        Ok((xid, rx))
    }

    /// Register an external request targeting a specific cartridge by ID.
    ///
    /// Instead of dispatching by cap URN, this finds the master that owns
    /// the specified cartridge and routes directly to it. The REQ frame's
    /// meta map gets a `target_cartridge` field so the CartridgeHostRuntime
    /// on the receiving end routes to the correct cartridge process.
    ///
    /// Returns `(xid, response_receiver)` — same as `register_external_request`.
    ///
    /// Fails descriptively if:
    /// - The cartridge ID is not known to any master
    /// - The master owning the cartridge is unhealthy
    pub async fn register_external_request_for_cartridge(
        &self,
        rid: MessageId,
        cap_urn: &str,
        cartridge_id: &str,
    ) -> Result<(MessageId, mpsc::UnboundedReceiver<Frame>), RelaySwitchError> {
        // Find which master owns this cartridge
        let masters = self.masters.read().await;
        let mut dest_idx: Option<usize> = None;

        for (idx, master) in masters.iter().enumerate() {
            let cartridges = master.installed_cartridges.read().await;
            if cartridges.iter().any(|c| c.id == cartridge_id) {
                dest_idx = Some(idx);
                break;
            }
        }

        let dest_idx = dest_idx.ok_or_else(|| {
            RelaySwitchError::Protocol(format!(
                "Unknown cartridge '{}': not reported by any master. \
                 Cannot route adapter-selection request.",
                cartridge_id
            ))
        })?;

        // Check master health
        if !masters[dest_idx].healthy.load(Ordering::SeqCst) {
            let last_error = masters[dest_idx].last_error.read().await;
            return Err(RelaySwitchError::Protocol(format!(
                "Master for cartridge '{}' is unhealthy: {}",
                cartridge_id,
                last_error.as_deref().unwrap_or("unknown error")
            )));
        }

        drop(masters); // Release read lock before taking write locks

        // Assign XID
        let xid = MessageId::Uint(self.xid_counter.fetch_add(1, Ordering::SeqCst) + 1);
        let key = (xid.clone(), rid.clone());

        // Create response channel
        let (tx, rx) = mpsc::unbounded_channel();

        // Register response channel + routing + rid index BEFORE sending (L7)
        let permit = self
            .acquire_cartridge_admission(dest_idx, cartridge_id)
            .await?;
        self.register_external(
            key.clone(),
            dest_idx,
            tx,
            Some(cap_urn.to_string()),
            Some(permit),
        )
        .await?;

        Ok((xid, rx))
    }

    /// Run an end-to-end identity probe against an already-registered
    /// master, using the relay's normal frame routing (writes via
    /// `write_to_master_idx`, response collected via the same
    /// unified-request-table machinery as `execute_cap`).
    ///
    /// This is the post-registration counterpart to the synchronous
    /// probe `add_master` runs at handshake time. It is required for
    /// any RelayNotify update that transitions a master's cap set
    /// from empty to non-empty: `add_master` skipped the probe at the
    /// initial empty advertisement, so the runtime path must not let
    /// the master start serving caps without proving its handler
    /// chain answers identity end-to-end.
    ///
    /// On success returns `Ok(())`. On failure returns a typed error
    /// string suitable for `MasterConnection.last_error`.
    async fn run_identity_probe_via_relay(&self, master_idx: usize) -> Result<(), String> {
        use crate::standard::caps::CAP_IDENTITY;
        use std::time::Duration;

        const RUNTIME_PROBE_TIMEOUT: Duration = Duration::from_secs(10);

        // Build (xid, rid) and a one-shot response channel keyed by
        // them. The frame loop's request-table channel lookup
        // delivers the master's reply frames here.
        let xid = MessageId::Uint(self.xid_counter.fetch_add(1, Ordering::SeqCst) + 1);
        let rid = MessageId::new_uuid();
        let key = (xid.clone(), rid.clone());

        let (tx, mut rx) = mpsc::unbounded_channel::<Frame>();
        self.register_external(
            key.clone(),
            master_idx,
            tx,
            Some(CAP_IDENTITY.to_string()),
            None,
        )
        .await
        .map_err(|e| format!("identity probe registration failed: {}", e))?;

        let nonce = identity_nonce();
        let stream_id = "identity-verify-runtime".to_string();

        // Inner async block that runs the probe round-trip. Wrapped
        // so the function tail can clean up the registered channel
        // and routing maps regardless of which branch produced the
        // outcome.
        let probe_outcome = async {
            // Build and send the probe frames. All five carry the
            // same (xid, rid) so the master returns its echo on the
            // same flow.
            let mut req = Frame::req(rid.clone(), CAP_IDENTITY, vec![], "application/cbor");
            req.routing_id = Some(xid.clone());
            let mut ss =
                Frame::stream_start(rid.clone(), stream_id.clone(), "media:".to_string(), None);
            ss.routing_id = Some(xid.clone());
            let checksum = Frame::compute_checksum(&nonce);
            let mut chunk = Frame::chunk(
                rid.clone(),
                stream_id.clone(),
                0,
                nonce.clone(),
                0,
                checksum,
            );
            chunk.routing_id = Some(xid.clone());
            let mut se = Frame::stream_end(rid.clone(), stream_id.clone(), 1);
            se.routing_id = Some(xid.clone());
            let mut end = Frame::end(rid.clone(), None);
            end.routing_id = Some(xid.clone());

            for mut frame in [req, ss, chunk, se, end] {
                self.write_to_master_idx(master_idx, &mut frame)
                    .await
                    .map_err(|e| format!("identity probe send failed: {}", e))?;
            }

            // Drain response frames. Cartridge contract: the
            // identity handler echoes the nonce back as STREAM_START
            // + CHUNK(nonce) + STREAM_END + END.
            let started_at = Instant::now();
            let mut accumulated = Vec::new();
            loop {
                let elapsed = started_at.elapsed();
                let remaining = RUNTIME_PROBE_TIMEOUT
                    .checked_sub(elapsed)
                    .unwrap_or(Duration::ZERO);
                if remaining.is_zero() {
                    return Err(format!(
                        "runtime identity probe timed out after {:?}",
                        RUNTIME_PROBE_TIMEOUT
                    ));
                }
                let frame = match tokio::time::timeout(remaining, rx.recv()).await {
                    Ok(Some(f)) => f,
                    Ok(None) => {
                        return Err("runtime identity probe channel closed before END".to_string());
                    }
                    Err(_) => {
                        return Err(format!(
                            "runtime identity probe timed out after {:?}",
                            RUNTIME_PROBE_TIMEOUT
                        ));
                    }
                };
                match frame.frame_type {
                    FrameType::StreamStart => {}
                    FrameType::Chunk => {
                        if let Some(payload) = frame.payload {
                            accumulated.extend_from_slice(&payload);
                        }
                    }
                    FrameType::StreamEnd => {}
                    FrameType::End => {
                        if accumulated != nonce {
                            return Err(format!(
                                "identity probe payload mismatch (expected {} bytes, got {})",
                                nonce.len(),
                                accumulated.len()
                            ));
                        }
                        return Ok(());
                    }
                    FrameType::Err => {
                        let code = frame
                            .error_code()
                            .ok_or_else(|| "ERR frame missing required text code".to_string())?;
                        let msg = frame
                            .error_message()
                            .ok_or_else(|| "ERR frame missing required text message".to_string())?;
                        return Err(format!("identity probe failed: [{}] {}", code, msg));
                    }
                    // Control/side-channel frames are legal ANYWHERE during the
                    // probe (spec 12.4: LOG interleaves without affecting data
                    // flow; CREDIT/HEARTBEAT are the control plane the writer
                    // gate itself exempts, L4). A v4 cartridge crediting its
                    // probe input as it consumes (L10) must not fail identity
                    // verification.
                    FrameType::Log | FrameType::Credit | FrameType::Heartbeat => {}
                    other => {
                        return Err(format!("identity probe: unexpected frame type {:?}", other));
                    }
                }
            }
        }
        .await;

        // Always terminate the request — whether the probe succeeded, failed,
        // or timed out. Leaking the entry would waste memory and confuse
        // introspection over time. Success ends via the probe's END; failure
        // is an Err-kind termination.
        let kind = if probe_outcome.is_ok() {
            TerminalKind::End
        } else {
            TerminalKind::Err
        };
        self.requests.write().await.terminate(&key, kind);

        probe_outcome
    }

    /// Cancel a specific in-flight request by RID.
    ///
    /// 1. Looks up RID → XID → routing destination
    /// 2. Sends Cancel frame to destination master
    /// 3. Terminates the request (Cancelled) — removing ALL its state (L7)
    /// 4. Recursively cancels the child peer calls recorded on the entry
    /// 5. Sends ERR "CANCELLED" to the external response channel if present
    /// STOP a feed-bearing request's live inputs (15.2 §Runs Stop): send the
    /// non-force Cancel FRAME to the request's destination WITHOUT touching
    /// host-side request state. The cartridge runtime closes the request's
    /// open taps and the request then ends NATURALLY — END after the drain —
    /// so routing, forwarding, collection, and every downstream cap stay
    /// live and the run completes WITH its outputs. Contrast
    /// [`Self::cancel_request`], which terminates host state, cascades to
    /// children, and delivers ERR CANCELLED (the abort path). A request
    /// that is unknown (already terminated) is a no-op.
    pub async fn stop_request_feeds(&self, rid: &MessageId) {
        let (xid, destination) = {
            let requests = self.requests.read().await;
            let Some(xid) = requests.xid_for_rid(rid) else {
                return;
            };
            let key = (xid.clone(), rid.clone());
            let Some(state) = requests.get(&key) else {
                return;
            };
            (xid, state.routing.destination_master_idx)
        };
        let mut cancel_frame = Frame::cancel(rid.clone(), false);
        cancel_frame.routing_id = Some(xid);
        let _ = self.write_to_master_idx(destination, &mut cancel_frame).await;
    }

    pub async fn cancel_request(&self, rid: &MessageId, force_kill: bool) {
        // Find XID for this RID
        let xid = {
            let requests = self.requests.read().await;
            match requests.xid_for_rid(rid) {
                Some(xid) => xid,
                None => return,
            }
        };

        let key = (xid.clone(), rid.clone());

        // Terminate FIRST: one atomic removal yields the destination, the
        // children for the cascade, and the external channel for the final
        // ERR. A concurrent terminal for the same key loses the race and
        // becomes a counted no_route drop, never a double-cleanup.
        let Some(state) = self
            .requests
            .write()
            .await
            .terminate(&key, TerminalKind::Cancelled)
        else {
            return;
        };

        // Send Cancel frame to destination
        let mut cancel_frame = Frame::cancel(rid.clone(), force_kill);
        cancel_frame.routing_id = Some(xid.clone());
        let _ = self
            .write_to_master_idx(state.routing.destination_master_idx, &mut cancel_frame)
            .await;

        // Recursively cancel children
        for (_child_xid, child_rid) in &state.children {
            // Use Box::pin for recursive async
            Box::pin(self.cancel_request(child_rid, force_kill)).await;
        }

        // Send ERR "CANCELLED" to external response channel if present
        if let Some(tx) = state.external_channel {
            let mut err_frame = Frame::err(
                rid.clone(),
                "CANCELLED",
                crate::failure::AttributionClass::Internal,
                "Request cancelled",
                None,
            );
            err_frame.routing_id = Some(xid.clone());
            let _ = tx.send(err_frame);
        }
    }

    /// Cancel all external-origin (engine-initiated) in-flight requests.
    ///
    /// Returns the list of cancelled RIDs.
    pub async fn cancel_all_requests(&self, force_kill: bool) -> Vec<MessageId> {
        // Snapshot all external-origin request RIDs (origin = None)
        let rids: Vec<MessageId> = {
            let requests = self.requests.read().await;
            requests
                .keys_where(|s| s.origin.is_none())
                .into_iter()
                .map(|(_, rid)| rid)
                .collect()
        };

        for rid in &rids {
            self.cancel_request(rid, force_kill).await;
        }

        rids
    }

    /// Dynamically add a new master connection to the switch.
    ///
    /// Performs handshake (reads RelayNotify, verifies identity) with the new master,
    /// spawns a reader task, and returns the master index.
    ///
    /// This is used for dynamically connecting new hosts (e.g., Mac client connecting via gRPC).
    pub async fn add_master(
        &self,
        id: impl Into<String>,
        socket: UnixStream,
    ) -> Result<usize, RelaySwitchError> {
        let id: String = id.into();

        // Serialise add_master across the whole RelaySwitch.
        //
        // `master_idx` is the routing key for both the request table
        // and `cap_table`. It MUST be decided once and stay stable
        // for the slot's lifetime. Concurrent `add_master` calls
        // would race on `self.masters.len()` — two appenders could
        // both decide they are slot N. The fix is to take a
        // dedicated mutex for the duration of the call, so an
        // append's index choice (or a reattach's id-lookup) cannot
        // be undermined by another in-flight registration.
        //
        // The mutex covers I/O too (RelayNotify read + identity
        // probe) on purpose: those steps need to see a stable view
        // of `self.masters` for the reattach branch, and the lock
        // is per-RelaySwitch so contention is bounded by the
        // (small) number of cardinality slots.
        let _add_master_guard = self.add_master_lock.lock().await;

        // Each engine cardinality slot is wired up with a
        // caller-chosen stable id (`"in-process"`,
        // `"bundled-cartridges"`, `"xpc-service"` for the website
        // edition). When the host backing a slot dies and
        // reconnects, the new socket reattaches to the SAME slot
        // index — preserving the request-table / cap_table
        // entries that were keyed by index.
        //
        // - Existing slot with same id, currently UNHEALTHY → reattach
        //   in place at that index.
        // - Existing slot with same id, currently HEALTHY → caller
        //   bug; the same master must not be added twice. Surface it
        //   loudly so the wiring mistake is fixed instead of silently
        //   growing zombie slots.
        // - No existing slot with that id → append a fresh slot at
        //   `masters.len()`. The index is committed atomically at
        //   the bottom of the function once the I/O has succeeded.
        let existing_slot: Option<usize> = {
            let masters = self.masters.read().await;
            let mut found = None;
            for (idx, master) in masters.iter().enumerate() {
                if master.id == id {
                    if master.healthy.load(Ordering::SeqCst) {
                        return Err(RelaySwitchError::Protocol(format!(
                            "add_master: id '{}' is already attached to a healthy slot at index {} — \
                             cardinality violation (each id may only be attached once at a time)",
                            id, idx
                        )));
                    }
                    found = Some(idx);
                    break;
                }
            }
            found
        };

        // Final slot index. For the append case this is the current
        // length under the add_master mutex (no concurrent appender
        // can intervene); for the reattach case it is the existing
        // slot index. The reader closure below captures this by
        // value so per-frame routing always carries the right index.
        let master_idx = match existing_slot {
            Some(idx) => idx,
            None => self.masters.read().await.len(),
        };

        let (read_half, write_half) = socket.into_split();
        let mut socket_reader = FrameReader::new(BufReader::new(read_half));
        let mut socket_writer = FrameWriter::new(BufWriter::new(write_half));

        // Read RelayNotify
        let notify_frame = socket_reader
            .read()
            .await
            .map_err(|e| RelaySwitchError::Cbor(format!("new master {}: {}", master_idx, e)))?
            .ok_or_else(|| {
                RelaySwitchError::Protocol(format!(
                    "new master {}: closed before RelayNotify",
                    master_idx
                ))
            })?;

        if notify_frame.frame_type != FrameType::RelayNotify {
            return Err(RelaySwitchError::Protocol(format!(
                "new master {}: expected RelayNotify, got {:?}",
                master_idx, notify_frame.frame_type
            )));
        }

        let caps_payload = notify_frame
            .relay_notify_manifest()
            .ok_or_else(|| {
                RelaySwitchError::Protocol(format!(
                    "new master {}: RelayNotify has no manifest",
                    master_idx
                ))
            })?
            .to_vec();

        // Diagnostic — what did the master announce in its initial
        // RelayNotify? This pins down whether an apparently-empty
        // installed_cartridges aggregate downstream is because the
        // master sent zero, or because we lost the data later.
        let mut caps_payload = caps_payload;
        let mut payload = parse_relay_notify_payload(&caps_payload)?;
        let mut caps = payload.cap_urns();
        let mut limits = notify_frame.relay_notify_limits().unwrap_or_default();

        let mut seq_assigner = SeqAssigner::new();

        // End-to-end identity verification. The engine sends a
        // `cap:effect=none` / CAP_IDENTITY REQ through the relay and expects the nonce
        // echoed back via some cartridge on the host. This probe is only
        // meaningful when the host advertises at least one cap — an empty
        // cap set means "no cartridges attached successfully," so there is
        // no handler chain to probe. The master still joins: its
        // `installed_cartridges` carries attachment-error entries the UI
        // needs to surface.
        //
        // The probe is wrapped in a hard timeout. Cold-starting the
        // first cartridge that handles `cap:effect=none` (identity) — typically
        // ~2-3s for a Rust binary, longer for Swift cartridges with
        // sandbox-deferred init — used to silently extend
        // `add_master` indefinitely, and during that window the
        // master was invisible to `rebuild_capabilities` (it isn't
        // pushed to `self.masters` until after identity returns). The
        // engine's `installed_cartridges` aggregate stayed empty for
        // the whole probe window, the gRPC bridge connected and saw
        // 0 cartridges, and the Mac client's UI reported "no
        // cartridges installed" until something else happened to
        // trigger a rebuild. With the timeout, identity failure / hang
        // surfaces as a hard error with a typed message — exposed,
        // not hidden.
        const IDENTITY_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
        let mut identity_failure: Option<String> = None;
        if !caps.is_empty() {
            let probe_started_at = std::time::Instant::now();
            let xid = MessageId::Uint(self.xid_counter.fetch_add(1, Ordering::SeqCst) + 1);

            use crate::standard::caps::CAP_IDENTITY;

            let nonce = identity_nonce();
            let req_id = MessageId::new_uuid();
            let stream_id = "identity-verify".to_string();

            // Wrap the probe so any failure is captured as a typed
            // attachment-error message rather than aborting master
            // registration. We still log the precise failure mode loudly
            // — silent fallbacks hide the real problem — but the master
            // still joins so its installed_cartridges are visible to the
            // inventory aggregate.
            let probe_result: Result<(), String> = async {
                let mut req = Frame::req(req_id.clone(), CAP_IDENTITY, vec![], "application/cbor");
                req.routing_id = Some(xid.clone());
                seq_assigner.assign(&mut req);
                socket_writer
                    .write(&req)
                    .await
                    .map_err(|e| format!("identity send failed (REQ): {}", e))?;

                let mut ss = Frame::stream_start(
                    req_id.clone(),
                    stream_id.clone(),
                    "media:".to_string(),
                    None,
                );
                ss.routing_id = Some(xid.clone());
                seq_assigner.assign(&mut ss);
                socket_writer
                    .write(&ss)
                    .await
                    .map_err(|e| format!("identity send failed (StreamStart): {}", e))?;

                let checksum = Frame::compute_checksum(&nonce);
                let mut chunk = Frame::chunk(
                    req_id.clone(),
                    stream_id.clone(),
                    0,
                    nonce.clone(),
                    0,
                    checksum,
                );
                chunk.routing_id = Some(xid.clone());
                seq_assigner.assign(&mut chunk);
                socket_writer
                    .write(&chunk)
                    .await
                    .map_err(|e| format!("identity send failed (Chunk): {}", e))?;

                let mut se = Frame::stream_end(req_id.clone(), stream_id.clone(), 1);
                se.routing_id = Some(xid.clone());
                seq_assigner.assign(&mut se);
                socket_writer
                    .write(&se)
                    .await
                    .map_err(|e| format!("identity send failed (StreamEnd): {}", e))?;

                let mut end = Frame::end(req_id.clone(), None);
                end.routing_id = Some(xid.clone());
                seq_assigner.assign(&mut end);
                socket_writer
                    .write(&end)
                    .await
                    .map_err(|e| format!("identity send failed (End): {}", e))?;

                seq_assigner.remove(&FlowKey {
                    rid: req_id.clone(),
                    xid: Some(xid.clone()),
                });

                // Read response — bounded by the global probe timeout
                // so a hung cold-start never blocks master
                // registration.
                let mut accumulated = Vec::new();
                loop {
                    let elapsed = probe_started_at.elapsed();
                    let remaining = IDENTITY_PROBE_TIMEOUT
                        .checked_sub(elapsed)
                        .unwrap_or(std::time::Duration::ZERO);
                    if remaining.is_zero() {
                        return Err(format!(
                            "identity verification timed out after {:?} — host did not echo nonce within the probe window. Cartridge cold-start exceeded the timeout or the identity-handler cap is unresponsive.",
                            IDENTITY_PROBE_TIMEOUT
                        ));
                    }
                    let frame = match tokio::time::timeout(remaining, socket_reader.read()).await {
                        Ok(read_result) => read_result
                            .map_err(|e| format!("identity read failed: {}", e))?
                            .ok_or_else(|| {
                                "closed during identity verification".to_string()
                            })?,
                        Err(_) => {
                            return Err(format!(
                                "identity verification timed out after {:?} waiting for next frame.",
                                IDENTITY_PROBE_TIMEOUT
                            ));
                        }
                    };

                    match frame.frame_type {
                        FrameType::RelayNotify => {
                            if let Some(manifest) = frame.relay_notify_manifest() {
                                caps_payload = manifest.to_vec();
                                payload = parse_relay_notify_payload(&caps_payload)
                                    .map_err(|e| format!("RelayNotify reparse failed: {}", e))?;
                                caps = payload.cap_urns();
                            }
                            if let Some(l) = frame.relay_notify_limits() {
                                limits = l;
                            }
                        }
                        FrameType::StreamStart => {}
                        FrameType::Chunk => {
                            if let Some(payload) = frame.payload {
                                accumulated.extend_from_slice(&payload);
                            }
                        }
                        FrameType::StreamEnd => {}
                        FrameType::End => {
                            if accumulated != nonce {
                                return Err(format!(
                                    "identity payload mismatch (expected {} bytes, got {})",
                                    nonce.len(),
                                    accumulated.len()
                                ));
                            }
                            break;
                        }
                        FrameType::Err => {
                            let code = frame
                                .error_code()
                                .ok_or_else(|| "ERR frame missing required text code".to_string())?;
                            let msg = frame
                                .error_message()
                                .ok_or_else(|| "ERR frame missing required text message".to_string())?;
                            return Err(format!(
                                "identity failed: [{}] {}",
                                code, msg
                            ));
                        }
                        // Control/side-channel frames are legal ANYWHERE during the
                        // probe (spec 12.4: LOG interleaves without affecting data
                        // flow; CREDIT/HEARTBEAT are the control plane the writer
                        // gate itself exempts, L4). A v4 cartridge crediting its
                        // probe input as it consumes (L10) must not fail identity
                        // verification.
                        FrameType::Log | FrameType::Credit | FrameType::Heartbeat => {}
                        other => {
                            return Err(format!(
                                "identity: unexpected frame type {:?}",
                                other
                            ));
                        }
                    }
                }
                Ok(())
            }
            .await;

            match probe_result {
                Ok(()) => {}
                Err(detail) => {
                    let elapsed_ms = probe_started_at.elapsed().as_millis() as u64;
                    let detailed = format!(
                        "new master {}: {} (after {} ms)",
                        master_idx, detail, elapsed_ms
                    );
                    tracing::error!(
                        target: "relay_switch",
                        master_idx = master_idx,
                        elapsed_ms = elapsed_ms,
                        error = %detailed,
                        "[RelaySwitch] add_master: identity verification FAILED — registering master as unhealthy so its installed_cartridges remain visible to the inventory aggregate"
                    );
                    identity_failure = Some(detailed);
                }
            }
        }

        // Spawn reader task
        let tx = self.frame_tx.clone();
        let reader_handle = tokio::spawn(async move {
            let mut reader = socket_reader;
            loop {
                match reader.read().await {
                    Ok(Some(frame)) => {
                        if tx.send((master_idx, Ok(frame))).is_err() {
                            tracing::warn!(
                                "[RelaySwitch] master {} reader: frame_tx closed",
                                master_idx
                            );
                            break;
                        }
                    }
                    Ok(None) => {
                        tracing::warn!(
                            "[RelaySwitch] master {} reader: socket closed (EOF)",
                            master_idx
                        );
                        let _ = tx.send((master_idx, Err(CborError::UnexpectedEof)));
                        break;
                    }
                    Err(e) => {
                        tracing::error!(
                            "[RelaySwitch] master {} reader: socket error: {}",
                            master_idx,
                            e
                        );
                        let _ = tx.send((master_idx, Err(e)));
                        break;
                    }
                }
            }
        });

        let cap_count = caps.len();
        let healthy_at_register = identity_failure.is_none();

        // Commit the connection state into the slot.
        //
        // Append branch: push a brand-new MasterConnection at the
        // index reserved earlier. Under the add_master mutex this
        // is race-free — `master_idx` equals `masters.len()` at
        // the moment of push, so the index the reader closure
        // captured matches the slot it ends up in.
        //
        // Reattach branch: overwrite the existing dead slot's
        // fields in place. The slot index, the id, and the routing
        // entries keyed by either are preserved. The previous
        // reader handle is aborted defensively — by the time
        // reattach happens the old reader has already exited on
        // EOF, but abort is idempotent and rules out any race with
        // a still-running zombie task that happens to have
        // outlived the socket close.
        match existing_slot {
            None => {
                // Append branch. Take the Vec write lock briefly to
                // push a fully-constructed `MasterConnection`. The
                // captured `master_idx` MUST equal the new length;
                // if not, a concurrent appender bypassed
                // `add_master_lock`, which is a protocol violation
                // we surface loudly. The hot dispatch path's
                // `masters.read()` is blocked only for the duration
                // of the push.
                let mut masters = self.masters.write().await;
                if masters.len() != master_idx {
                    return Err(RelaySwitchError::Protocol(format!(
                        "add_master: append-index race for id '{}': reserved {} but masters.len() is now {} \
                         (a concurrent caller bypassed add_master_lock — this is a protocol violation)",
                        id, master_idx, masters.len()
                    )));
                }
                masters.push(MasterConnection {
                    id: id.clone(),
                    socket_writer: Mutex::new(socket_writer),
                    seq_assigner: Mutex::new(seq_assigner),
                    manifest: RwLock::new(caps_payload),
                    limits: RwLock::new(limits),
                    caps: RwLock::new(caps),
                    installed_cartridges: RwLock::new(payload.installed_cartridges),
                    host_protocol_stats: RwLock::new(payload.host_protocol_stats),
                    healthy: AtomicBool::new(healthy_at_register),
                    reader_handle: Mutex::new(Some(reader_handle)),
                    connected_at: std::sync::Mutex::new(Instant::now()),
                    last_error: RwLock::new(identity_failure.clone()),
                });
            }
            Some(slot_idx) => {
                // Reattach branch. Take only a read lock on the Vec
                // (the slot's index doesn't change; only its
                // interior fields do, and every field is interior-
                // mutable). The hot dispatch path can keep dispatching
                // throughout. The previous reader handle is aborted
                // defensively — by the time reattach runs the dead
                // master's reader has already exited on EOF, but
                // abort is idempotent and rules out any race with a
                // still-running zombie task that happens to have
                // outlived the socket close.
                let masters = self.masters.read().await;
                let slot: &MasterConnection = &masters[slot_idx];
                if slot.id != id {
                    return Err(RelaySwitchError::Protocol(format!(
                        "add_master: reattach-id mismatch at index {}: expected '{}' but found '{}' \
                         (masters Vec was reordered concurrently — this is a protocol violation)",
                        slot_idx, id, slot.id
                    )));
                }
                *slot.socket_writer.lock().await = socket_writer;
                *slot.seq_assigner.lock().await = seq_assigner;
                *slot.manifest.write().await = caps_payload;
                *slot.limits.write().await = limits;
                *slot.caps.write().await = caps;
                *slot.installed_cartridges.write().await = payload.installed_cartridges;
                *slot.host_protocol_stats.write().await = payload.host_protocol_stats;
                slot.healthy.store(healthy_at_register, Ordering::SeqCst);
                *slot.last_error.write().await = identity_failure.clone();
                {
                    let mut handle_slot = slot.reader_handle.lock().await;
                    if let Some(prev) = handle_slot.take() {
                        prev.abort();
                    }
                    *handle_slot = Some(reader_handle);
                }
                *slot
                    .connected_at
                    .lock()
                    .expect("connected_at mutex poisoned") = Instant::now();
            }
        }

        let inventory = {
            let masters = self.masters.read().await;
            let inventory = masters[master_idx]
                .installed_cartridges
                .read()
                .await
                .clone();
            inventory
        };
        self.configure_master_admission(master_idx, &inventory)?;

        // Rebuild tables
        self.rebuild_cap_table().await;
        self.rebuild_capabilities().await;
        self.rebuild_limits().await;

        if healthy_at_register {
            info!(
                master_idx = master_idx,
                cap_count = cap_count,
                appending = existing_slot.is_none(),
                "[RelaySwitch] Master connected successfully"
            );
        } else {
            tracing::error!(
                master_idx = master_idx,
                cap_count = cap_count,
                error = %identity_failure.as_deref().unwrap_or(""),
                "[RelaySwitch] Master registered as UNHEALTHY (identity probe failed) — installed_cartridges remain in inventory but the master is not in the routing table"
            );
        }

        Ok(master_idx)
    }

    /// Send a frame to the appropriate master (engine → cartridge direction).
    ///
    /// REQ frames: Assigned XID if absent, routed by cap URN.
    /// Continuation frames: Routed by (XID, RID) pair.
    /// Send a frame to the appropriate master.
    ///
    /// `preferred_cap`: when `Some`, uses `is_dispatchable` routing and prefers
    /// the master whose registered cap is equivalent to this URN.
    /// When `None`, uses standard `is_dispatchable` + closest-specificity routing.
    pub async fn send_to_master(
        &self,
        mut frame: Frame,
        preferred_cap: Option<&str>,
    ) -> Result<(), RelaySwitchError> {
        match frame.frame_type {
            FrameType::Req => {
                let cap_urn = frame.cap.as_ref().ok_or_else(|| {
                    RelaySwitchError::Protocol("REQ frame missing cap URN".to_string())
                })?;

                // Check for target_cartridge in meta — if present, route to that
                // cartridge's master directly instead of using cap-based dispatch
                let target_cartridge_id = frame.meta.as_ref().and_then(|m| {
                    m.get("target_cartridge").and_then(|v| {
                        if let ciborium::Value::Text(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                });

                let (dest_idx, admission_cap, admission_cartridge) =
                    if let Some(ref cartridge_id) = target_cartridge_id {
                        // Direct routing by cartridge ID
                        let masters = self.masters.read().await;
                        let mut found = None;
                        for (idx, master) in masters.iter().enumerate() {
                            let cartridges = master.installed_cartridges.read().await;
                            if cartridges.iter().any(|c| &c.id == cartridge_id) {
                                found = Some(idx);
                                break;
                            }
                        }
                        let dest = found.ok_or_else(|| {
                            RelaySwitchError::Protocol(format!(
                                "Unknown cartridge '{}': not reported by any master",
                                cartridge_id
                            ))
                        })?;
                        (dest, None, Some(cartridge_id.clone()))
                    } else {
                        // Standard cap-based dispatch
                        let (dest, registered_cap) = self
                            .find_route_for_cap(cap_urn, preferred_cap)
                            .await
                            .ok_or_else(|| RelaySwitchError::NoHandler(cap_urn.clone()))?;
                        (dest, Some(registered_cap), None)
                    };

                // A REQ arriving WITH an XID is the pre-registered path:
                // the caller obtained the XID (and its response channel) from
                // `register_external_request*` and is now sending the frames
                // itself. The request is already in the table — registering
                // again would violate L7's register-once. Route to the
                // REGISTERED destination (the registration's dispatch
                // decision is authoritative). An XID-stamped REQ with no
                // registration is a caller bug and fails hard.
                if let Some(existing_xid) = frame.routing_id.clone() {
                    let key = (existing_xid, frame.id.clone());
                    let registered_dest = self
                        .requests
                        .read()
                        .await
                        .get(&key)
                        .map(|state| state.routing.destination_master_idx);
                    let Some(dest) = registered_dest else {
                        return Err(RelaySwitchError::Protocol(format!(
                            "REQ ({}, {}) carries an XID but was never registered —                              obtain the XID from register_external_request (L7)",
                            key.0, key.1
                        )));
                    };
                    self.write_initial_request(&key, dest, &mut frame).await?;
                    return Ok(());
                }

                // No XID: first arrival at the RelaySwitch — assign and
                // register (origin None = external caller via send_to_master,
                // no response channel; responses return via
                // read_from_masters), then forward (L7).
                let xid = MessageId::Uint(self.xid_counter.fetch_add(1, Ordering::SeqCst) + 1);
                frame.routing_id = Some(xid.clone());
                let key = (xid, frame.id.clone());
                let permit = match (admission_cap.as_deref(), admission_cartridge.as_deref()) {
                    (Some(registered_cap), None) => {
                        self.acquire_cap_admission(dest_idx, registered_cap).await?
                    }
                    (None, Some(cartridge_id)) => {
                        self.acquire_cartridge_admission(dest_idx, cartridge_id)
                            .await?
                    }
                    _ => {
                        return Err(RelaySwitchError::Protocol(
                            "request has an invalid admission target".to_string(),
                        ))
                    }
                };

                let initial_credit = self.master_initial_credit(dest_idx).await;
                self.requests
                    .write()
                    .await
                    .register(
                        key.clone(),
                        RequestState::new(
                            RoutingEntry {
                                source_master_idx: None,
                                destination_master_idx: dest_idx,
                            },
                            None,
                            None,
                            false,
                            initial_credit,
                        )
                        .with_cap_urn(frame.cap.clone())
                        .with_admission_permit(permit),
                    )
                    .map_err(RelaySwitchError::Protocol)?;

                // Forward to destination with XID
                self.write_initial_request(&key, dest_idx, &mut frame)
                    .await?;
                Ok(())
            }

            FrameType::StreamStart
            | FrameType::Chunk
            | FrameType::StreamEnd
            | FrameType::End
            | FrameType::Err
            | FrameType::Cancel
            | FrameType::Credit => {
                // Continuation/control frames from engine: look up XID from
                // RID if missing, resolve the destination, and RECORD the
                // outbound frame in the request's flow ledger — one table
                // write. Engine-originated grants and chunks are half of
                // every stream's credit arithmetic; skipping them made the
                // snapshot ledger read healthy streams as deep-negative.
                // Unknown RID is a hard error back to the caller: the engine
                // is a direct API client and must observe that the request no
                // longer exists (already terminated) so it stops sending.
                let (dest_idx, xid) = {
                    let mut requests = self.requests.write().await;
                    let xid = match frame.routing_id.clone() {
                        Some(xid) => xid,
                        None => requests
                            .xid_for_rid(&frame.id)
                            .ok_or_else(|| RelaySwitchError::UnknownRequest(frame.id.clone()))?,
                    };
                    let key = (xid.clone(), frame.id.clone());
                    let dest_idx = requests
                        .get(&key)
                        .map(|entry| entry.routing.destination_master_idx)
                        .ok_or_else(|| RelaySwitchError::UnknownRequest(frame.id.clone()))?;
                    requests.record_frame(&key, FrameDirection::Outbound, &frame);
                    (dest_idx, xid)
                };
                frame.routing_id = Some(xid);

                // Forward to destination
                self.write_to_master_idx(dest_idx, &mut frame).await?;

                Ok(())
            }

            _ => Err(RelaySwitchError::Protocol(format!(
                "Unexpected frame type from engine: {:?}",
                frame.frame_type
            ))),
        }
    }

    /// Read the next frame from any master (cartridge → engine direction).
    ///
    /// Awaits until a frame is available from any master. Returns Ok(None) when all masters have closed.
    /// Peer requests (cartridge → cartridge) are handled internally and not returned.
    pub async fn read_from_masters(&self) -> Result<Option<Frame>, RelaySwitchError> {
        loop {
            // Hold lock through handle_master_frame — see read_from_masters_timeout comment.
            let mut rx = self.frame_rx.lock().await;
            let frame_result = rx.recv().await;

            match frame_result {
                Some((master_idx, Ok(frame))) => {
                    let handle_result = self.handle_master_frame(master_idx, frame).await;
                    drop(rx);
                    if let Some(result_frame) = handle_result? {
                        return Ok(Some(result_frame));
                    }
                }
                Some((master_idx, Err(_e))) => {
                    drop(rx);
                    self.handle_master_death(master_idx).await?;
                }
                None => {
                    drop(rx);
                    return Ok(None);
                }
            }
        }
    }

    /// Process one pending frame from any master, non-blocking.
    ///
    /// Returns Ok(Some(frame)) if a frame was processed and should be returned to caller.
    /// Returns Ok(None) if no frame was available or the frame was handled internally.
    /// Use this in tokio::select! loops for concurrent frame processing.
    pub async fn pump_one(&self) -> Result<Option<Frame>, RelaySwitchError> {
        // Hold lock through handle_master_frame — see read_from_masters_timeout comment.
        let mut rx = self.frame_rx.lock().await;
        let frame_result = rx.try_recv();

        match frame_result {
            Ok((master_idx, Ok(frame))) => {
                let handle_result = self.handle_master_frame(master_idx, frame).await;
                drop(rx);
                if let Some(result_frame) = handle_result? {
                    return Ok(Some(result_frame));
                }
                Ok(None)
            }
            Ok((master_idx, Err(_e))) => {
                drop(rx);
                self.handle_master_death(master_idx).await?;
                Ok(None)
            }
            Err(mpsc::error::TryRecvError::Empty) => {
                // No frames available
                Ok(None)
            }
            Err(mpsc::error::TryRecvError::Disconnected) => {
                // All reader tasks have exited
                Err(RelaySwitchError::Protocol(
                    "All masters disconnected".to_string(),
                ))
            }
        }
    }

    /// Wait for the next frame from any master with timeout.
    ///
    /// Returns Ok(Some(frame)) if a frame arrives, Ok(None) on timeout, Err on error.
    pub async fn read_from_masters_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Option<Frame>, RelaySwitchError> {
        let start = std::time::Instant::now();
        loop {
            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return Ok(None); // Timeout
            }

            // Receive and process under the same lock. Multiple pump tasks
            // call this method concurrently — the lock ensures that a REQ's
            // routing table writes (request registration) complete
            // before the next frame is dequeued. Without this, a continuation
            // frame can be dequeued by a second pump before the first pump
            // finishes inserting the REQ's routing entries.
            let mut rx = self.frame_rx.lock().await;
            let frame_result = tokio::time::timeout(remaining, rx.recv()).await;
            // Extract the frame data, then drop the lock before async I/O
            let action = match frame_result {
                Ok(Some((master_idx, Ok(frame)))) => Some((master_idx, Ok(frame))),
                Ok(Some((master_idx, Err(e)))) => Some((master_idx, Err(e))),
                Ok(None) => {
                    drop(rx);
                    return Err(RelaySwitchError::Protocol(
                        "All masters disconnected".to_string(),
                    ));
                }
                Err(_elapsed) => {
                    drop(rx);
                    return Ok(None); // Timeout
                }
            };
            // Process the frame while still holding the lock — this serializes
            // handle_master_frame so routing table mutations from a REQ are
            // visible before the next recv can return its continuation frames.
            if let Some((master_idx, result)) = action {
                match result {
                    Ok(frame) => {
                        let handle_result = self.handle_master_frame(master_idx, frame).await;
                        drop(rx);
                        if let Some(result_frame) = handle_result? {
                            return Ok(Some(result_frame));
                        }
                    }
                    Err(_e) => {
                        drop(rx);
                        self.handle_master_death(master_idx).await?;
                    }
                }
            }
        }
    }

    // =========================================================================
    // FRAME OUTPUT (all writes to masters go through this)
    // =========================================================================

    /// Low-level frame write that assigns seq and writes to the master transport.
    /// This helper does not perform master retirement on failure.
    async fn write_to_master_idx_raw(
        &self,
        master_idx: usize,
        frame: &mut Frame,
    ) -> Result<(), CborError> {
        let masters = self.masters.read().await;
        // A destination that no longer exists (master detached between route
        // resolution and delivery) is a DELIVERY failure, never a panic — a
        // routed frame must not be able to crash the switch.
        let Some(master) = masters.get(master_idx) else {
            return Err(CborError::Io(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!(
                    "master {master_idx} is not attached (of {}) — destination detached before delivery",
                    masters.len()
                ),
            )));
        };

        // Lock seq_assigner and socket_writer separately to minimize lock contention
        {
            let mut seq = master.seq_assigner.lock().await;
            seq.assign(frame);
        }

        let write_result = {
            let mut writer = master.socket_writer.lock().await;
            writer.write(frame).await
        };

        if matches!(frame.frame_type, FrameType::End | FrameType::Err) {
            let mut seq = master.seq_assigner.lock().await;
            seq.remove(&FlowKey::from_frame(frame));
        }

        write_result
    }

    /// Write a frame to a master, assigning seq via the per-master SeqAssigner.
    /// Cleans up seq tracking on terminal frames (END/ERR).
    async fn write_to_master_idx(
        &self,
        master_idx: usize,
        frame: &mut Frame,
    ) -> Result<(), CborError> {
        let write_result = self.write_to_master_idx_raw(master_idx, frame).await;

        match write_result {
            Ok(()) => Ok(()),
            Err(error) => {
                let reason = format!("Write to master {} failed: {}", master_idx, error);
                if let Err(cleanup_error) = self
                    .handle_master_death_with_reason(master_idx, &reason)
                    .await
                {
                    tracing::error!(
                        master_idx = master_idx,
                        write_error = %error,
                        cleanup_error = %cleanup_error,
                        "[RelaySwitch] Failed to retire dead master after write failure"
                    );
                }
                Err(error)
            }
        }
    }

    // =========================================================================
    // INTERNAL ROUTING
    // =========================================================================

    /// Find which master handles a given cap URN.
    ///
    /// ## Routing semantics
    ///
    /// Uses `is_dispatchable(candidate, request)` to find all masters that can
    /// legally handle the request. A candidate is dispatchable if:
    /// - Its input handling is compatible with the request's input
    /// - Its output guarantees meet the request's output requirements
    /// - Its cap-tags satisfy all explicit request constraints
    ///
    /// Among dispatchable matches, ranking prefers:
    /// 1. Equivalent matches (distance 0)
    /// 2. More specific candidates (positive distance) - refinements
    /// 3. More generic candidates (negative distance) - fallbacks
    ///
    /// ## With preference (`preferred_cap = Some(cap_urn)`)
    ///
    /// Among dispatchable matches, the master whose registered cap is
    /// equivalent to the preferred cap wins. If no equivalent match, falls
    /// back to specificity-based ranking.
    async fn find_master_for_cap(
        &self,
        cap_urn: &str,
        preferred_cap: Option<&str>,
    ) -> Option<usize> {
        self.find_route_for_cap(cap_urn, preferred_cap)
            .await
            .map(|(master_idx, _)| master_idx)
    }

    async fn find_route_for_cap(
        &self,
        cap_urn: &str,
        preferred_cap: Option<&str>,
    ) -> Option<(usize, String)> {
        let request_urn = match crate::CapUrn::from_string(cap_urn) {
            Ok(u) => u,
            Err(_) => return None,
        };

        let request_specificity = request_urn.specificity();

        // Parse preferred cap URN if provided
        let preferred_urn = preferred_cap.and_then(|p| crate::CapUrn::from_string(p).ok());

        // Collect ALL dispatchable masters with their specificity scores.
        let mut matches: Vec<(usize, String, isize, bool)> = Vec::new();

        let cap_table = self.cap_table.read().await;
        for (registered_cap, master_idx) in cap_table.iter() {
            if let Ok(registered_urn) = crate::CapUrn::from_string(registered_cap) {
                let dispatchable = registered_urn.is_dispatchable(&request_urn);
                // Use is_dispatchable: can this candidate handle this request?
                if dispatchable {
                    let specificity = registered_urn.specificity();
                    let signed_distance = specificity as isize - request_specificity as isize;
                    // Check if this registered cap is equivalent to the preferred cap
                    let is_preferred = preferred_urn
                        .as_ref()
                        .map_or(false, |pref| pref.is_equivalent(&registered_urn));
                    matches.push((
                        *master_idx,
                        registered_cap.clone(),
                        signed_distance,
                        is_preferred,
                    ));
                }
            }
        }

        if matches.is_empty() {
            return None;
        }

        // If any match is preferred, pick the first preferred match.
        if let Some((idx, registered_cap, _, _)) = matches.iter().find(|(_, _, _, pref)| *pref) {
            return Some((*idx, registered_cap.clone()));
        }

        // Ranking: prefer equivalent (0), then more specific (+), then more generic (-)
        // Sort by: (is_negative, abs_distance) so positives come before negatives at same abs
        matches.sort_by(|a, b| {
            let (_, _, dist_a, _) = a;
            let (_, _, dist_b, _) = b;

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

        matches
            .first()
            .map(|(idx, registered_cap, _, _)| (*idx, registered_cap.clone()))
    }

    /// Handle a frame arriving from a master (cartridge → engine direction).
    ///
    /// Returns Some(frame) if the frame should be forwarded to the engine.
    /// Returns None if the frame was handled internally (peer request).
    async fn handle_master_frame(
        &self,
        source_idx: usize,
        mut frame: Frame,
    ) -> Result<Option<Frame>, RelaySwitchError> {
        match frame.frame_type {
            FrameType::Req => {
                let cap_urn = frame.cap.as_ref().ok_or_else(|| {
                    RelaySwitchError::Protocol("REQ frame missing cap URN".to_string())
                })?;

                // Validate XID-absence and assign XID FIRST, before any
                // dispatch-failure paths: every frame the switch emits
                // toward a master must carry an XID (the host_runtime's
                // path-C invariant), including the synthetic ERR we may
                // produce below for unhandled caps. Doing the assign
                // up-front means the failure-path ERR can carry the same
                // XID as the (failed) request would have, satisfying that
                // invariant uniformly.
                if frame.routing_id.is_some() {
                    return Err(RelaySwitchError::Protocol(
                        "REQ from cartridge should not have XID".to_string(),
                    ));
                }
                let xid = MessageId::Uint(self.xid_counter.fetch_add(1, Ordering::SeqCst) + 1);
                frame.routing_id = Some(xid.clone());

                // Find destination master (no preference for peer requests)
                let route = self.find_route_for_cap(cap_urn, None).await;
                if route.is_none() {
                    // No handler registered for this cap. Rather than returning
                    // Err(NoHandler) — which the pump logs and discards, leaving
                    // the caller hanging until the 120s activity timeout — send
                    // an ERR frame immediately back to the source master so the
                    // peer call fails fast with a clear error. Stamp the
                    // synthetic XID assigned above so the receiving cartridge
                    // host runtime accepts it (path-C invariant).
                    tracing::warn!(
                        "[RelaySwitch] NO_HANDLER for peer REQ cap='{}' rid={:?} from_master={} — sending ERR to caller",
                        cap_urn, frame.id, source_idx
                    );
                    // No master serving this cap is a deployment/manifest
                    // mismatch — Environment (docs/failure-taxonomy.md).
                    let mut err_frame = Frame::err(
                        frame.id.clone(),
                        "NO_HANDLER",
                        crate::failure::AttributionClass::Environment,
                        &format!("No handler found for cap: {}", cap_urn),
                        None,
                    );
                    err_frame.routing_id = Some(xid.clone());
                    let _ = self
                        .write_to_master_idx_raw(source_idx, &mut err_frame)
                        .await;
                    return Ok(None);
                }
                let (dest_idx, _registered_cap) = route.unwrap();

                let rid = frame.id.clone();
                let key = (xid.clone(), rid.clone());

                // Register the peer request and link it under its parent for
                // the cancel cascade — one table write guard (L7). The ledger
                // seed is read BEFORE taking the guard (it awaits the master
                // table's own lock).
                let initial_credit = self.master_initial_credit(dest_idx).await;
                {
                    let mut requests = self.requests.write().await;
                    requests
                        .register(
                            key.clone(),
                            RequestState::new(
                                RoutingEntry {
                                    source_master_idx: Some(source_idx),
                                    destination_master_idx: dest_idx,
                                },
                                Some(source_idx),
                                None,
                                true,
                                initial_credit,
                            )
                            .with_cap_urn(frame.cap.clone()),
                        )
                        .map_err(RelaySwitchError::Protocol)?;

                    // Track parent→child for cancel cascade
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
                        // Find parent's XID from its RID
                        if let Some(parent_xid) = requests.xid_for_rid(&parent_rid) {
                            let parent_key = (parent_xid, parent_rid);
                            requests.link_child(&parent_key, key.clone());
                        }
                    }
                }

                // Forward to destination with XID
                self.write_initial_request(&key, dest_idx, &mut frame)
                    .await?;

                // Do NOT return to engine (internal routing)
                Ok(None)
            }

            FrameType::StreamStart
            | FrameType::Chunk
            | FrameType::StreamEnd
            | FrameType::End
            | FrameType::Err
            | FrameType::Log
            | FrameType::Credit => {
                // Branch based on XID presence to distinguish request vs response direction
                if frame.routing_id.is_some() {
                    // ========================================
                    // HAS XID = RESPONSE CONTINUATION
                    // ========================================
                    // Frame already has XID, so it's a response flowing back to origin
                    let xid = frame.routing_id.clone().unwrap();
                    let rid = frame.id.clone();
                    let key = (xid.clone(), rid.clone());

                    let is_terminal =
                        frame.frame_type == FrameType::End || frame.frame_type == FrameType::Err;

                    /// Where a response frame must go next.
                    enum RouteBack {
                        External(Option<mpsc::UnboundedSender<Frame>>),
                        Master(usize),
                    }

                    // One table guard: record flow stats, resolve the return
                    // path, and — on terminal — remove the whole entry
                    // atomically (L7). The terminal is forwarded using the
                    // state `terminate` hands back, so routing state release
                    // and terminal delivery cannot disagree (L6). A frame for
                    // a released key is a counted no_route drop, never a
                    // protocol error and never silent (L8).
                    let (route, cap_for_log) = {
                        let mut requests = self.requests.write().await;
                        requests.record_frame(&key, FrameDirection::Inbound, &frame);
                        if is_terminal {
                            let kind = if frame.frame_type == FrameType::End {
                                TerminalKind::End
                            } else {
                                TerminalKind::Err
                            };
                            match requests.terminate(&key, kind) {
                                Some(state) => {
                                    let route = match state.origin {
                                        None => RouteBack::External(state.external_channel),
                                        Some(idx) => RouteBack::Master(idx),
                                    };
                                    (route, state.cap_urn)
                                }
                                None => {
                                    // Classify by the terminated ring: a frame
                                    // for a request that JUST terminated is the
                                    // ordinary teardown race — a benign
                                    // straggler, not a drop; only a RID the
                                    // table never knew is a routing anomaly
                                    // (`no_route`).
                                    self.account_unrouted_frame(
                                        requests.recently_terminated_rid(&rid),
                                        &frame,
                                        "duplicate terminal for released request",
                                    );
                                    return Ok(None);
                                }
                            }
                        } else {
                            match requests.get(&key) {
                                Some(state) => {
                                    let route = match state.origin {
                                        None => RouteBack::External(state.external_channel.clone()),
                                        Some(idx) => RouteBack::Master(idx),
                                    };
                                    (route, state.cap_urn.clone())
                                }
                                None => {
                                    self.account_unrouted_frame(
                                        requests.recently_terminated_rid(&rid),
                                        &frame,
                                        "response frame with no routing state",
                                    );
                                    return Ok(None);
                                }
                            }
                        }
                    };

                    match route {
                        RouteBack::External(Some(tx)) => {
                            // Send to external response channel (keep XID)
                            if tx.send(frame.clone()).is_err() {
                                let total = self.drops.record(
                                    crate::bifaci::frame::DropReason::ChannelClosed,
                                    frame.frame_type,
                                );
                                tracing::warn!(
                                    rid = ?rid,
                                    cap = cap_for_log.as_deref().unwrap_or("?"),
                                    ftype = ?frame.frame_type,
                                    channel_closed_total = total,
                                    "[RelaySwitch] response channel receiver gone (channel_closed)"
                                );
                                // A dead consumer on a LIVE request means the
                                // caller abandoned it (dropped/timed-out
                                // future). Nobody can ever read this response —
                                // cancel upstream so the cartridge stops
                                // producing for a dead channel, instead of
                                // letting the request run to completion and
                                // counting a drop for every remaining frame
                                // (TEST7093). Terminal frames need no cancel:
                                // the entry is already terminated.
                                if !is_terminal {
                                    self.cancel_request(&rid, false).await;
                                }
                            }
                            Ok(None)
                        }
                        RouteBack::External(None) => {
                            // No response channel (sent via send_to_master, not
                            // execute_cap). Strip XID and return to caller.
                            frame.routing_id = None;
                            Ok(Some(frame))
                        }
                        RouteBack::Master(master_idx) => {
                            // Route back to source master — KEEP XID.
                            self.write_to_master_idx(master_idx, &mut frame).await?;
                            Ok(None)
                        }
                    }
                } else {
                    // ========================================
                    // NO XID = REQUEST CONTINUATION
                    // ========================================
                    // Frame has no XID, so it's a request continuation
                    // (peer-call argument streams / grants) flowing to the
                    // destination. An unknown RID means the request already
                    // terminated: counted drop (L6), not an error.
                    let rid = frame.id.clone();
                    let lookup = {
                        let mut requests = self.requests.write().await;
                        let hit = requests.xid_for_rid(&rid).and_then(|xid| {
                            let key = (xid.clone(), rid.clone());
                            requests.record_frame(&key, FrameDirection::Inbound, &frame);
                            requests
                                .get(&key)
                                .map(|state| (xid, state.routing.destination_master_idx))
                        });
                        match hit {
                            Some(found) => Ok(found),
                            // Classify under the same table guard: a
                            // continuation for a request that JUST terminated
                            // is the ordinary teardown race — a benign
                            // straggler (a grant or in-flight frame that
                            // crossed END); a RID the table never knew is a
                            // routing anomaly (`no_route`).
                            None => Err(requests.recently_terminated_rid(&rid)),
                        }
                    };
                    let (xid, dest_idx) = match lookup {
                        Ok(found) => found,
                        Err(recently_terminated) => {
                            self.account_unrouted_frame(
                                recently_terminated,
                                &frame,
                                "continuation with no routing state",
                            );
                            return Ok(None);
                        }
                    };

                    // Add XID to frame for forwarding
                    frame.routing_id = Some(xid.clone());

                    // Forward to destination master (keep XID)
                    self.write_to_master_idx(dest_idx, &mut frame).await?;
                    return Ok(None);
                }
            }

            FrameType::Cancel => {
                // Cancel from cartridge — route to destination like a continuation frame.
                // Cartridge is cancelling its own peer call. Unknown RID means
                // the request already completed: a well-defined no-op.
                let rid = frame.id.clone();
                let lookup = {
                    let requests = self.requests.read().await;
                    let xid = match frame.routing_id.clone() {
                        Some(xid) => Some(xid),
                        None => requests.xid_for_rid(&rid),
                    };
                    xid.and_then(|xid| {
                        requests
                            .get(&(xid.clone(), rid.clone()))
                            .map(|state| (xid, state.routing.destination_master_idx))
                    })
                };
                let Some((xid, dest_idx)) = lookup else {
                    return Ok(None);
                };
                frame.routing_id = Some(xid);
                self.write_to_master_idx(dest_idx, &mut frame).await?;
                Ok(None)
            }

            FrameType::RelayNotify => {
                // Capability update from host — update our cap table.
                let caps_payload = frame.relay_notify_manifest().ok_or_else(|| {
                    RelaySwitchError::Protocol("RelayNotify has no payload".to_string())
                })?;

                let payload = parse_relay_notify_payload(caps_payload)?;
                let new_caps = payload.cap_urns();
                self.configure_master_admission(source_idx, &payload.installed_cartridges)?;

                // Detect transition from empty → non-empty caps. The
                // initial RelayNotify (during `add_master`) skipped
                // the identity probe when caps were empty; if the
                // host now advertises a real handler chain we must
                // probe it end-to-end before letting the new caps
                // become routable. The master is held unhealthy
                // until the probe driver task confirms identity.
                let probe_required = {
                    let masters = self.masters.read().await;
                    let prior_caps_empty = if let Some(master) = masters.get(source_idx) {
                        master.caps.read().await.is_empty()
                    } else {
                        // No master at this index — drop the update
                        // silently, the master's reader will exit on
                        // its own.
                        return Ok(Some(frame));
                    };
                    prior_caps_empty && !new_caps.is_empty()
                };

                // Apply the update. We always write the new
                // installed_cartridges and limits (those are
                // observation-only inventory data the engine wants
                // to surface immediately). Caps are also written so
                // RelayNotify-update lookups stay consistent — but
                // when probe_required is true we mark the master
                // unhealthy below so cap_table rebuild excludes it.
                {
                    let masters = self.masters.read().await;
                    if let Some(master) = masters.get(source_idx) {
                        *master.caps.write().await = new_caps;
                        *master.installed_cartridges.write().await = payload.installed_cartridges;
                        *master.host_protocol_stats.write().await = payload.host_protocol_stats;
                        *master.manifest.write().await = caps_payload.to_vec();
                        if let Some(new_limits) = frame.relay_notify_limits() {
                            *master.limits.write().await = new_limits;
                        }
                        if probe_required {
                            master.healthy.store(false, Ordering::SeqCst);
                            *master.last_error.write().await = Some(
                                "runtime identity probe pending — caps held back from routing"
                                    .to_string(),
                            );
                        }
                    }
                }

                // Rebuild cap_table / aggregate / limits from all
                // masters. cap_table only includes healthy masters,
                // so an unhealthy master's caps don't surface as
                // dispatch targets until the probe driver flips it
                // back to healthy.
                self.rebuild_cap_table().await;
                self.rebuild_capabilities().await;
                self.rebuild_limits().await;

                if probe_required {
                    // Hand off to the probe driver task. Sending on
                    // an unbounded channel cannot block; if the
                    // receiver has been dropped the relay is being
                    // torn down and we silently skip.
                    let _ = self.pending_identity_probes_tx.send(source_idx);
                }

                // Pass through to engine (for visibility)
                Ok(Some(frame))
            }

            _ => {
                // All other frames: pass through to engine
                Ok(Some(frame))
            }
        }
    }

    /// Handle master death: ERR all pending requests, mark unhealthy, rebuild tables.
    async fn handle_master_death(&self, master_idx: usize) -> Result<(), RelaySwitchError> {
        self.handle_master_death_with_reason(master_idx, "Connection closed unexpectedly")
            .await
    }

    /// Handle master death with a specific error reason.
    async fn handle_master_death_with_reason(
        &self,
        master_idx: usize,
        reason: &str,
    ) -> Result<(), RelaySwitchError> {
        // Own the reason string for use across await points
        let reason_owned = reason.to_string();

        // Get master info before marking unhealthy
        let (was_healthy, cap_count, connected_seconds) = {
            let masters = self.masters.read().await;
            if master_idx >= masters.len() {
                let total = masters.len();
                error!(
                    master_idx = master_idx,
                    total_masters = total,
                    "[RelaySwitch] handle_master_death called with invalid master index"
                );
                return Ok(());
            }
            let master = &masters[master_idx];
            let was_healthy = master.healthy.load(Ordering::SeqCst);
            let cap_count = master.caps.read().await.len();
            let connected_seconds = master
                .connected_at
                .lock()
                .expect("connected_at mutex poisoned")
                .elapsed()
                .as_secs();
            (was_healthy, cap_count, connected_seconds)
        };

        if !was_healthy {
            return Ok(()); // Already handled
        }

        // Mark unhealthy and record error
        {
            let masters = self.masters.read().await;
            masters[master_idx].healthy.store(false, Ordering::SeqCst);
            *masters[master_idx].last_error.write().await = Some(reason_owned.clone());
        }
        self.admission.disable_master(master_idx);

        error!(
            master_idx = master_idx,
            cap_count = cap_count,
            connected_seconds = connected_seconds,
            reason = %reason_owned,
            "[RelaySwitch] Master died - marking unhealthy and cleaning up"
        );

        // Find all pending requests routed to this master
        let dead_keys = {
            let requests = self.requests.read().await;
            requests.keys_where(|s| s.routing.destination_master_idx == master_idx)
        };

        if !dead_keys.is_empty() {
            warn!(
                master_idx = master_idx,
                pending_requests = dead_keys.len(),
                "[RelaySwitch] Failing pending requests due to master death"
            );
        }

        // Terminate each pending request (MasterDied) and deliver a synthetic
        // ERR to whoever was waiting on it. terminate() atomically removes ALL
        // state for the key (L7) and hands back the origin + channel needed
        // for delivery.
        for key in dead_keys {
            let Some(state) = self
                .requests
                .write()
                .await
                .terminate(&key, TerminalKind::MasterDied)
            else {
                continue; // raced another terminal — already fully cleaned
            };
            let (xid, rid) = &key;

            // Create ERR frame
            // A dead relay master is a runtime-environment failure —
            // Environment (docs/failure-taxonomy.md).
            let mut err_frame = Frame::err(
                rid.clone(),
                "MASTER_DIED",
                crate::failure::AttributionClass::Environment,
                &format!("Relay master {} connection closed: {}", master_idx, reason),
                None,
            );
            err_frame.routing_id = Some(xid.clone());

            match state.origin {
                None => {
                    // External caller - send to response channel if exists
                    if let Some(tx) = state.external_channel {
                        let _ = tx.send(err_frame);
                    }
                }
                Some(src_master_idx) => {
                    // Send ERR back to source master
                    let is_healthy = {
                        let masters = self.masters.read().await;
                        masters[src_master_idx].healthy.load(Ordering::SeqCst)
                    };
                    if is_healthy {
                        let _ = self
                            .write_to_master_idx_raw(src_master_idx, &mut err_frame)
                            .await;
                    }
                }
            }
        }

        // Rebuild tables
        self.rebuild_cap_table().await;
        self.rebuild_capabilities().await;
        self.rebuild_limits().await;

        // Log remaining healthy masters
        let (healthy_count, total_count) = {
            let masters = self.masters.read().await;
            let healthy = masters
                .iter()
                .filter(|m| m.healthy.load(Ordering::SeqCst))
                .count();
            let total = masters.len();
            (healthy, total)
        };
        info!(
            healthy_masters = healthy_count,
            total_masters = total_count,
            "[RelaySwitch] Master health updated after death"
        );

        Ok(())
    }

    // =========================================================================
    // TABLE MANAGEMENT
    // =========================================================================

    /// Rebuild cap_table from all healthy masters.
    async fn rebuild_cap_table(&self) {
        let mut new_cap_table = Vec::new();
        let masters = self.masters.read().await;
        for (idx, master) in masters.iter().enumerate() {
            if master.healthy.load(Ordering::SeqCst) {
                let caps = master.caps.read().await;
                for cap in caps.iter() {
                    new_cap_table.push((cap.clone(), idx));
                }
            }
        }
        *self.cap_table.write().await = new_cap_table;
    }

    /// Rebuild aggregate capabilities (union of all healthy masters).
    /// Logs changes if the capability set differs from the previous state.
    async fn rebuild_capabilities(&self) {
        // Collect caps per master for detailed logging
        let mut caps_by_master: Vec<(usize, String, bool, Vec<String>)> = Vec::new();
        let mut installed_cartridges_by_master: Vec<(bool, Vec<InstalledCartridgeRecord>)> =
            Vec::new();
        let masters = self.masters.read().await;
        for (idx, master) in masters.iter().enumerate() {
            let healthy = master.healthy.load(Ordering::SeqCst);
            let caps = master.caps.read().await.clone();
            let installed_cartridges = master.installed_cartridges.read().await.clone();
            caps_by_master.push((idx, master.id.clone(), healthy, caps));
            installed_cartridges_by_master.push((healthy, installed_cartridges));
        }
        drop(masters);

        // Collect all caps from healthy masters
        let mut all_caps: Vec<String> = Vec::new();
        for (_, _, healthy, caps) in &caps_by_master {
            if *healthy {
                all_caps.extend(caps.iter().cloned());
            }
        }

        // Deduplicate
        all_caps.sort();
        all_caps.dedup();

        // Compare with previous capabilities
        let old_caps: Vec<String> = {
            let old_bytes = self.aggregate_capabilities.read().await;
            serde_json::from_slice(&old_bytes)
                .expect("aggregate_capabilities must be a JSON array of cap URNs")
        };

        let changed = old_caps != all_caps;

        // Build manifest as JSON array (same format as RelayNotify payloads)
        *self.aggregate_capabilities.write().await =
            serde_json::to_vec(&all_caps).expect("cap URNs must serialize to JSON");
        // Broadcast the routable-cap change to subscribers (the engine-facing
        // relay re-advertises readiness off this). Only fire on an actual
        // change so a deferred probe completing — which flips a master
        // healthy and adds its caps here — wakes the engine, without a notify
        // storm from unrelated rebuilds. Use `send_replace`, NOT `send`:
        // `send` is a no-op (drops the value, leaves the stored snapshot
        // stale) when there are momentarily zero receivers, which is exactly
        // the synchronous-verification case — `new()` rebuilds capabilities
        // before the engine-facing relay has subscribed, so a plain `send`
        // would silently lose the initial routable set and the engine would
        // never see readiness. `send_replace` always stores the new value.
        if changed {
            let _ = self.aggregate_capabilities_tx.send_replace(
                serde_json::to_vec(&all_caps).expect("cap URNs must serialize to JSON"),
            );
        }
        // Installed-cartridges aggregate is the inventory view — what is
        // physically installed and known to any master, regardless of
        // current per-master reachability. We do NOT filter by health
        // here. The reachability story lives in
        // `InstalledCartridgeRecord.runtime_stats.running` (per
        // cartridge), not in whether the parent master happens to be
        // unhealthy at this exact tick. Filtering the inventory by
        // master health caused the "all cartridges disappeared"
        // symptom on every transient master flap (XPC bridge
        // reconnect, in-process master restart, RelayNotify race at
        // startup before the first heartbeat round-trip).
        let mut all_installed_cartridges: Vec<InstalledCartridgeRecord> = Vec::new();
        for (_healthy, installed_cartridges) in installed_cartridges_by_master {
            all_installed_cartridges.extend(installed_cartridges);
        }
        // Sort by the install's identity tuple — see
        // `InstalledCartridgeRecord::identity_cmp` for the
        // rationale (cap URNs aren't totally ordered, so the
        // manifest/runtime-stats fields are excluded from the key).
        all_installed_cartridges.sort_by(InstalledCartridgeRecord::identity_cmp);
        // Identity tuple is `(registry_url, channel, id, version, sha256)`.
        // Two installs of the same id+version from different registries
        // (or different channels) are distinct cartridges with their own
        // attached process and on-disk tree; collapsing them here would
        // make the second one invisible to the engine.
        all_installed_cartridges.dedup_by(|left, right| {
            left.registry_url == right.registry_url
                && left.channel == right.channel
                && left.id == right.id
                && left.version == right.version
                && left.sha256 == right.sha256
        });
        let prior_installed = self.aggregate_installed_cartridges.read().await.clone();
        *self.aggregate_installed_cartridges.write().await = all_installed_cartridges.clone();
        // Publish to subscribers only when the snapshot actually changed —
        // watch::send always wakes receivers so we guard against redundant
        // notify storms from unrelated capability rebuilds.
        if prior_installed != all_installed_cartridges {
            // send_replace is infallible; receivers dropped is fine.
            let _ = self
                .aggregate_installed_cartridges_tx
                .send(all_installed_cartridges);
        }

        // Log only if changed
        if changed {
            info!(
                total_caps = all_caps.len(),
                previous_caps = old_caps.len(),
                "[RelaySwitch] Capabilities changed"
            );

            self.rebuild_live_cap_fab_from_current_capabilities("capabilities_changed")
                .await;
        }
    }

    async fn rebuild_live_cap_fab_from_current_capabilities(&self, reason: &'static str) {
        let all_caps: Vec<String> = {
            let aggregate = self.aggregate_capabilities.read().await;
            serde_json::from_slice(&aggregate)
                .expect("aggregate_capabilities must be a JSON array of cap URNs")
        };

        // The bookend URN set is the registry's own predicate: every
        // URN whose stored spec carries at least one file extension.
        // A media-def cache revision can therefore change both which
        // caps are admissible and which graph nodes are file bookends.
        let bookend_urns = self.fabric_registry.bookend_urns();

        let mut graph = self.live_cap_fab.write().await;
        graph
            .sync_from_cap_urns(&all_caps, &self.fabric_registry, &bookend_urns)
            .await;
        tracing::debug!(
            target: "relay_switch",
            cap_count = all_caps.len(),
            reason = reason,
            "[RelaySwitch] LiveCapFab rebuilt from current capabilities"
        );
    }

    /// The destination master's negotiated initial credit — the ledger seed
    /// for requests routed to it ([`RequestState::initial_credit`]). When the
    /// slot has already detached (a resolve/attach race — the registration
    /// that follows will fail on delivery), the switch-level negotiated
    /// minimum is the correct window bound.
    async fn master_initial_credit(&self, dest_idx: usize) -> u64 {
        let masters = self.masters.read().await;
        match masters.get(dest_idx) {
            Some(master) => master.limits.read().await.initial_credit,
            None => self.negotiated_limits.read().await.initial_credit,
        }
    }

    /// Rebuild negotiated limits (minimum across all healthy masters).
    async fn rebuild_limits(&self) {
        let mut min_max_frame = usize::MAX;
        let mut min_max_chunk = usize::MAX;
        let mut min_initial_credit = u64::MAX;

        let masters = self.masters.read().await;
        for master in masters.iter() {
            if master.healthy.load(Ordering::SeqCst) {
                let limits = master.limits.read().await;
                min_max_frame = min_max_frame.min(limits.max_frame);
                min_max_chunk = min_max_chunk.min(limits.max_chunk);
                min_initial_credit = min_initial_credit.min(limits.initial_credit);
            }
        }

        *self.negotiated_limits.write().await = Limits {
            max_frame: if min_max_frame == usize::MAX {
                crate::bifaci::frame::DEFAULT_MAX_FRAME
            } else {
                min_max_frame
            },
            max_chunk: if min_max_chunk == usize::MAX {
                crate::bifaci::frame::DEFAULT_MAX_CHUNK
            } else {
                min_max_chunk
            },
            initial_credit: if min_initial_credit == u64::MAX {
                crate::bifaci::frame::DEFAULT_INITIAL_CREDIT
            } else {
                min_initial_credit
            },
            ..Limits::default()
        };
    }
}

impl Drop for RelaySwitch {
    fn drop(&mut self) {
        // Signal both background tasks (frame pump + identity probe
        // driver) to exit on their next iteration. The tasks hold
        // `Weak<Self>` so they also drop out when the last Arc goes
        // away, but setting the flag lets them exit before their
        // next blocking call returns.
        self.background_pump_stop.store(true, Ordering::Relaxed);
        if let Ok(mut guard) = self.background_pump_handle.lock() {
            for handle in guard.drain(..) {
                handle.abort();
            }
        }
    }
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/// Parse capabilities payload from RelayNotify.
/// RelayNotify contains a JSON object with capability URNs and installed cartridge identities.
///
/// If the host advertises any caps, CAP_IDENTITY must be among them — this
/// is the contract that makes the engine's end-to-end identity probe
/// meaningful. If the host advertises an empty cap set, this is a valid
/// state meaning "this host has no cartridges that passed the attachment
/// checklist"; the `installed_cartridges` list may still report
/// attachment failures the UI needs to surface.
fn parse_relay_notify_payload(
    notify_payload: &[u8],
) -> Result<RelayNotifyCapabilitiesPayload, RelaySwitchError> {
    use crate::standard::caps::CAP_IDENTITY;
    use crate::urn::cap_urn::CapUrn;

    let payload: RelayNotifyCapabilitiesPayload = serde_json::from_slice(notify_payload)
        .map_err(|e| RelaySwitchError::Protocol(format!("Invalid RelayNotify payload: {}", e)))?;

    let cap_urns = payload.cap_urns();
    if !cap_urns.is_empty() {
        // A non-empty cap set must include CAP_IDENTITY — advertising any cap
        // without the structural identity cap is a broken host. The check
        // walks every cap URN declared across the payload's cap_groups.
        let identity_urn =
            CapUrn::from_string(CAP_IDENTITY).expect("BUG: CAP_IDENTITY constant is invalid");
        let has_identity = cap_urns.iter().any(|cap_str| {
            CapUrn::from_string(cap_str)
                .map(|cap_urn| identity_urn.conforms_to(&cap_urn))
                .unwrap_or(false)
        });
        if !has_identity {
            return Err(RelaySwitchError::Protocol(format!(
                "RelayNotify advertised caps but is missing required CAP_IDENTITY ({})",
                CAP_IDENTITY
            )));
        }
    }

    Ok(payload)
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bifaci::frame::{Frame, SeqAssigner};
    use crate::bifaci::local_socket::UnixStream;
    use crate::standard::caps::CAP_IDENTITY;

    /// Create a test FabricRegistry for use in tests. Tests that need
    /// bookend-eligible URNs should populate via
    /// `insert_cached_media_def_for_test` after construction.
    fn test_fabric_registry() -> Arc<FabricRegistry> {
        Arc::new(FabricRegistry::new_for_test())
    }

    /// Wrap a single test socket in the `(id, socket)` shape that
    /// `RelaySwitch::new` now requires. Tests that don't care about
    /// reattach semantics use this helper to keep the call sites
    /// terse; the synthesised ids are positional (`"test-master-N"`)
    /// so each test's slot count drives unique-id generation
    /// without collision.
    fn wrap_with_test_ids(sockets: Vec<UnixStream>) -> Vec<(String, UnixStream)> {
        sockets
            .into_iter()
            .enumerate()
            .map(|(i, s)| (format!("test-master-{}", i), s))
            .collect()
    }

    fn test_media_def(
        urn: &str,
        title: &str,
        extensions: &[&str],
    ) -> crate::fabric::registry::StoredMediaDef {
        crate::fabric::registry::StoredMediaDef {
            version: 0,
            urn: urn.to_string(),
            media_type: urn.to_string(),
            title: title.to_string(),
            profile_uri: None,
            schema: None,
            description: None,
            documentation: None,
            validation: None,
            metadata: None,
            extensions: extensions.iter().map(|ext| ext.to_string()).collect(),
        }
    }

    // TEST0130: A cartridge can advertise a cap before the registry cache
    // has finished hydrating that cap's canonical definition. LiveCapFab
    // must retry the already-advertised aggregate capability set when the
    // registry cache later warms; otherwise the cap remains absent from
    // machine selection until an unrelated cartridge reconnect occurs.
    #[tokio::test]
    async fn test0130_registry_cache_revision_rebuilds_live_cap_fab_without_capability_change() {
        let registry = test_fabric_registry();
        registry.insert_cached_media_def_for_test(test_media_def(
            "media:enc=utf-8;test-source",
            "Test Source",
            &[],
        ));
        registry.insert_cached_media_def_for_test(test_media_def(
            "media:enc=utf-8;ext=txt;test-output",
            "Test Output",
            &["txt"],
        ));

        let switch = Arc::new(
            RelaySwitch::new(wrap_with_test_ids(vec![]), registry.clone())
                .await
                .expect("empty relay switch must construct"),
        );

        let cap_urn = "cap:test-transform;in=\"media:enc=utf-8;test-source\";out=\"media:enc=utf-8;ext=txt;test-output\"";
        *switch.aggregate_capabilities.write().await =
            serde_json::to_vec(&vec![CAP_IDENTITY.to_string(), cap_urn.to_string()])
                .expect("aggregate cap URNs must serialize");

        let source = MediaUrn::from_string("media:enc=utf-8;test-source")
            .expect("test source media URN must parse");
        let target = MediaUrn::from_string("media:enc=utf-8;ext=txt;test-output")
            .expect("test target media URN must parse");
        assert!(
            switch
                .find_paths_to_exact_target(&source, &target, false, 2, 8)
                .await
                .is_empty(),
            "cap must not be reachable before its registry definition is cached"
        );

        switch.start_background_pump();

        let mut cap = crate::cap::definition::Cap::with_description(
            crate::urn::cap_urn::CapUrn::from_string(cap_urn).expect("test cap URN must parse"),
            "Test Transform".to_string(),
            vec!["test-transform".to_string()],
            "Transforms test source into test output.".to_string(),
        );
        // A cached cap the planner will traverse must declare its main input —
        // `sequence_shape()` reads that arg to decide cardinality.
        cap.args.push(crate::cap::definition::CapArg::new(
            "media:enc=utf-8;test-source".to_string(),
            true,
            vec![crate::cap::definition::ArgSource::Stdin {
                stdin: "media:enc=utf-8;test-source".to_string(),
            }],
        ));
        registry.add_caps_to_cache(vec![cap]);

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let paths = switch
                    .find_paths_to_exact_target(&source, &target, false, 2, 8)
                    .await;
                if !paths.is_empty() {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("registry cache revision must make the advertised cap reachable");
    }

    /// Build a roster pools map for test stats: one at-rest singleton per
    /// cap (canonical URN) plus the mandatory `all` pool at the given
    /// capacity — the pool-map equivalent of the retired scalar
    /// handler_capacity.
    fn test_pool_states_json(
        cap_urns: &[&str],
        all_capacity: u64,
        active: u64,
    ) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        for urn in cap_urns {
            let canonical = crate::urn::cap_urn::CapUrn::from_string(urn)
                .expect("test cap URN must parse")
                .to_string();
            map.insert(
                canonical,
                serde_json::json!({ "declared": 0, "configured": 0, "active": active, "queued": 0 }),
            );
        }
        map.insert(
            crate::bifaci::pools::POOL_ALL.to_string(),
            serde_json::json!({
                "declared": all_capacity,
                "configured": all_capacity,
                "active": active,
                "queued": 0
            }),
        );
        serde_json::Value::Object(map)
    }

    /// Helper: send RelayNotify with given caps/limits, then handle identity verification.
    /// Returns (FrameReader, FrameWriter) ready for further communication.
    ///
    /// `caps_json` is a JSON array of cap-URN strings. The helper wraps
    /// them in a single synthetic installed-cartridge entry so the new
    /// payload schema (cap_groups inside installed_cartridges) is
    /// satisfied without each test having to spell out the wrapping.
    async fn slave_notify_with_identity(
        socket: UnixStream,
        caps_json: &serde_json::Value,
        limits: &Limits,
    ) -> (
        FrameReader<BufReader<crate::bifaci::local_socket::OwnedReadHalf>>,
        FrameWriter<BufWriter<crate::bifaci::local_socket::OwnedWriteHalf>>,
    ) {
        let (read_half, write_half) = socket.into_split();
        let mut reader = FrameReader::new(BufReader::new(read_half));
        let mut writer = FrameWriter::new(BufWriter::new(write_half));

        // Build a single synthetic installed-cartridge whose lone
        // cap_group carries the test's caps. Each cap is rendered with
        // the minimal Cap schema (urn/title/command/args=[]) so the
        // payload deserializes cleanly under the production parser.
        let cap_urns_array = caps_json
            .as_array()
            .expect("caps_json must be a JSON array of cap URN strings");
        let group_caps: Vec<serde_json::Value> = cap_urns_array
            .iter()
            .map(|v| {
                let urn = v.as_str().expect("cap URN must be a string").to_string();
                serde_json::json!({
                    "urn": urn,
                    "title": "test",
                    "aliases": ["test"],
                    "args": [],
                })
            })
            .collect();

        let notify_payload = serde_json::json!({
            "installed_cartridges": [
                {
                    "registry_url": null,
                    "channel": "release",
                    "id": "test-cartridge",
                    "version": "0.0.0",
                    "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                    "runtime_stats": {
                        "running": true,
                        "pools": test_pool_states_json(
                            &cap_urns_array
                                .iter()
                                .map(|v| v.as_str().expect("cap URN must be a string"))
                                .collect::<Vec<_>>(),
                            0,
                            0,
                        ),
                        "active_request_count": 0,
                        "peer_request_count": 0,
                        "memory_footprint_mb": 0,
                        "memory_rss_mb": 0,
                        "restart_count": 0
                    },
                    "cap_groups": [
                        {
                            "name": "test",
                            "caps": group_caps,
                            "adapter_urns": [],
                        }
                    ],
                }
            ],
        });
        let notify = Frame::relay_notify(&serde_json::to_vec(&notify_payload).unwrap(), limits);
        writer.write(&notify).await.unwrap();

        // Handle identity verification REQ
        let req = reader
            .read()
            .await
            .unwrap()
            .expect("expected identity REQ after RelayNotify");
        assert_eq!(
            req.frame_type,
            FrameType::Req,
            "first frame after RelayNotify must be identity REQ"
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

    // TEST7113: a capacity-one cartridge owns only the active body. If that
    // invocation dies, its ERR releases the slot and the next queued body is
    // dispatched; the queued body is not failed as pending work of the dead
    // process.
    #[tokio::test]
    async fn test7113_cartridge_loss_releases_next_engine_invocation() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();
        let (first_seen_tx, first_seen_rx) = tokio::sync::oneshot::channel();
        let (fail_first_tx, fail_first_rx) = tokio::sync::oneshot::channel();
        let (second_seen_tx, second_seen_rx) = tokio::sync::oneshot::channel();

        let slave = tokio::spawn(async move {
            let (mut reader, mut writer) = slave_notify_with_identity(
                slave_sock,
                &serde_json::json!(["cap:effect=none"]),
                &Limits::default(),
            )
            .await;

            let capacity_update = serde_json::json!({
                "installed_cartridges": [{
                    "registry_url": null,
                    "channel": "release",
                    "id": "test-cartridge",
                    "version": "0.0.0",
                    "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                    "runtime_stats": {
                        "running": true,
                        "pools": test_pool_states_json(&["cap:effect=none"], 1, 1),
                        "active_request_count": 1,
                        "peer_request_count": 0,
                        "memory_footprint_mb": 0,
                        "memory_rss_mb": 0,
                        "restart_count": 0
                    },
                    "cap_groups": [{
                        "name": "test",
                        "caps": [{
                            "urn": "cap:effect=none",
                            "title": "test",
                            "aliases": ["test"],
                            "args": []
                        }],
                        "adapter_urns": []
                    }]
                }]
            });
            writer
                .write(&Frame::relay_notify(
                    &serde_json::to_vec(&capacity_update).unwrap(),
                    &Limits::default(),
                ))
                .await
                .unwrap();

            let first = loop {
                let frame = reader.read().await.unwrap().expect("first request");
                if frame.frame_type == FrameType::Req {
                    break frame;
                }
            };
            first_seen_tx.send(()).unwrap();
            fail_first_rx.await.unwrap();
            let mut died = Frame::err(
                first.id,
                "CARTRIDGE_DIED",
                crate::AttributionClass::Environment,
                "active cartridge process exited",
                None,
            );
            died.routing_id = first.routing_id;
            writer.write(&died).await.unwrap();

            let second = loop {
                let frame = reader.read().await.unwrap().expect("second request");
                if frame.frame_type == FrameType::Req {
                    break frame;
                }
            };
            second_seen_tx.send(()).unwrap();
            let mut end = Frame::end(second.id, None);
            end.routing_id = second.routing_id;
            writer.write(&end).await.unwrap();
        });

        let switch = Arc::new(
            RelaySwitch::new(
                wrap_with_test_ids(vec![engine_sock]),
                test_fabric_registry(),
            )
            .await
            .unwrap(),
        );
        // Consume the capacity update before starting the two invocations.
        switch.read_from_masters().await.unwrap();

        let (_first_rid, mut first_rx) = switch
            .execute_cap("cap:effect=none", vec![], "application/cbor")
            .await
            .unwrap();
        first_seen_rx.await.unwrap();

        let queued_switch = switch.clone();
        let second_call = tokio::spawn(async move {
            queued_switch
                .execute_cap("cap:effect=none", vec![], "application/cbor")
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !second_call.is_finished(),
            "the second body must remain outside the capacity-one process"
        );

        fail_first_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                switch.pump_one().await.unwrap();
                if !first_rx.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("active failure must be routed to its body");
        let failed = first_rx
            .recv()
            .await
            .expect("first body receives terminal ERR");
        assert_eq!(failed.frame_type, FrameType::Err);
        assert_eq!(failed.error_code(), Some("CARTRIDGE_DIED"));

        let (_second_rid, mut second_rx) =
            tokio::time::timeout(std::time::Duration::from_secs(1), second_call)
                .await
                .expect("the failed body must release the next admission slot")
                .expect("second invocation task must not fail")
                .expect("second invocation must register");
        second_seen_rx.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                switch.pump_one().await.unwrap();
                if !second_rx.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("replacement request must receive its terminal frame");
        assert_eq!(
            second_rx
                .recv()
                .await
                .expect("replacement request completes")
                .frame_type,
            FrameType::End
        );
        slave.await.unwrap();
    }

    /// Test slave for the DEFERRED runtime-identity-probe path. Sends an
    /// EMPTY initial RelayNotify (so `RelaySwitch::new` skips synchronous
    /// verification and the master joins capless+healthy), then a populated
    /// RelayNotify carrying `caps_json` (the empty→non-empty transition the
    /// relay must re-verify before routing). It then answers the runtime
    /// identity probe: if `succeed`, it echoes the probe's nonce back on the
    /// same flow (probe passes → master flips healthy → caps routable); if
    /// `!succeed`, it replies ERR (probe fails → master stays unhealthy, caps
    /// held back). Mirrors what a real CartridgeHost does when a cartridge
    /// attaches AFTER the host first connected.
    async fn slave_deferred_identity(
        socket: UnixStream,
        caps_json: &serde_json::Value,
        limits: &Limits,
        succeed: bool,
    ) {
        let (read_half, write_half) = socket.into_split();
        let mut reader = FrameReader::new(BufReader::new(read_half));
        let mut writer = FrameWriter::new(BufWriter::new(write_half));

        // 1. Empty initial RelayNotify — new() skips the synchronous probe.
        let empty = serde_json::json!({ "installed_cartridges": [] });
        writer
            .write(&Frame::relay_notify(
                &serde_json::to_vec(&empty).unwrap(),
                limits,
            ))
            .await
            .unwrap();

        // 2. Populated RelayNotify — the empty→non-empty transition.
        let cap_urns_array = caps_json
            .as_array()
            .expect("caps_json must be a JSON array of cap URN strings");
        let group_caps: Vec<serde_json::Value> = cap_urns_array
            .iter()
            .map(|v| {
                let urn = v.as_str().expect("cap URN must be a string").to_string();
                serde_json::json!({ "urn": urn, "title": "test", "aliases": ["test"], "args": [] })
            })
            .collect();
        let notify_payload = serde_json::json!({
            "installed_cartridges": [
                {
                    "registry_url": null,
                    "channel": "release",
                    "id": "test-cartridge",
                    "version": "0.0.0",
                    "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                    "runtime_stats": {
                        "running": true,
                        "pools": test_pool_states_json(
                            &cap_urns_array
                                .iter()
                                .map(|v| v.as_str().expect("cap URN must be a string"))
                                .collect::<Vec<_>>(),
                            0,
                            0,
                        ),
                        "active_request_count": 0,
                        "peer_request_count": 0,
                        "memory_footprint_mb": 0,
                        "memory_rss_mb": 0,
                        "restart_count": 0
                    },
                    "cap_groups": [
                        { "name": "test", "caps": group_caps, "adapter_urns": [] }
                    ],
                }
            ],
        });
        writer
            .write(&Frame::relay_notify(
                &serde_json::to_vec(&notify_payload).unwrap(),
                limits,
            ))
            .await
            .unwrap();

        // 3. Answer the runtime identity probe.
        let mut probe_rid = None;
        let mut probe_xid = None;
        let mut nonce: Vec<u8> = Vec::new();
        loop {
            let f = match reader.read().await {
                Ok(Some(f)) => f,
                _ => return,
            };
            match f.frame_type {
                FrameType::Req => {
                    probe_rid = Some(f.id.clone());
                    probe_xid = f.routing_id.clone();
                    if !succeed {
                        // Broken identity handler: reply ERR on the same flow.
                        let mut err = Frame::err(
                            f.id.clone(),
                            "BROKEN",
                            crate::AttributionClass::Internal,
                            "test cartridge",
                            None,
                        );
                        err.routing_id = f.routing_id.clone();
                        let _ = writer.write(&err).await;
                        return;
                    }
                }
                FrameType::Chunk => nonce.extend(f.payload.unwrap_or_default()),
                FrameType::End => {
                    // Echo the nonce back on the probe's flow → probe passes.
                    let rid = probe_rid.clone().expect("probe REQ must precede END");
                    let stream_id = "identity-echo".to_string();
                    let mut ss = Frame::stream_start(
                        rid.clone(),
                        stream_id.clone(),
                        "media:".to_string(),
                        None,
                    );
                    ss.routing_id = probe_xid.clone();
                    let checksum = Frame::compute_checksum(&nonce);
                    let mut chunk = Frame::chunk(
                        rid.clone(),
                        stream_id.clone(),
                        0,
                        nonce.clone(),
                        0,
                        checksum,
                    );
                    chunk.routing_id = probe_xid.clone();
                    let mut se = Frame::stream_end(rid.clone(), stream_id, 1);
                    se.routing_id = probe_xid.clone();
                    let mut end = Frame::end(rid, None);
                    end.routing_id = probe_xid.clone();
                    for fr in [ss, chunk, se, end] {
                        writer.write(&fr).await.unwrap();
                    }
                    return;
                }
                _ => {}
            }
        }
    }

    // TEST429: Cap routing logic (find_master_for_cap)
    #[tokio::test]
    async fn test429_find_master_for_cap() {
        let (engine_sock1, slave_sock1) = UnixStream::pair().unwrap();
        let (engine_sock2, slave_sock2) = UnixStream::pair().unwrap();

        // Spawn slave 1 (identity cap only)
        tokio::spawn(async move {
            slave_notify_with_identity(
                slave_sock1,
                &serde_json::json!(["cap:effect=none"]),
                &Limits::default(),
            )
            .await;
        });

        // Spawn slave 2 (identity + double cap)
        tokio::spawn(async move {
            slave_notify_with_identity(
                slave_sock2,
                &serde_json::json!([
                    "cap:effect=none",
                    "cap:in=\"media:void\";double;out=\"media:void\""
                ]),
                &Limits::default(),
            )
            .await;
        });

        // Constructor reads RelayNotify + verifies identity for both masters
        let switch = RelaySwitch::new(
            wrap_with_test_ids(vec![engine_sock1, engine_sock2]),
            test_fabric_registry(),
        )
        .await
        .unwrap();

        // Verify routing (caps already populated during construction)
        assert_eq!(
            switch.find_master_for_cap("cap:effect=none", None).await,
            Some(0)
        );
        assert_eq!(
            switch
                .find_master_for_cap("cap:in=\"media:void\";double;out=\"media:void\"", None)
                .await,
            Some(1)
        );
        assert_eq!(
            switch
                .find_master_for_cap("cap:in=\"media:void\";unknown;out=\"media:void\"", None)
                .await,
            None
        );

        // Verify aggregate capabilities (plain JSON array)
        let cap_list: Vec<String> = serde_json::from_slice(&switch.capabilities().await).unwrap();
        assert_eq!(cap_list.len(), 2);
    }

    // TEST426: Single master REQ/response routing
    #[tokio::test]
    async fn test426_single_master_req_response() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(
                slave_sock,
                &serde_json::json!(["cap:effect=none"]),
                &Limits::default(),
            )
            .await;

            // Read REQ frame
            let (req_id, xid) = if let Some(frame) = reader.read().await.unwrap() {
                if frame.frame_type == FrameType::Req {
                    (Some(frame.id.clone()), frame.routing_id.clone())
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            };

            // Read END frame
            if let Some(frame) = reader.read().await.unwrap() {
                if frame.frame_type == FrameType::End && req_id.is_some() {
                    let rid = req_id.unwrap();
                    let xid_clone = xid.clone();
                    let mut response = Frame::end(rid.clone(), Some(vec![42]));
                    response.routing_id = xid;
                    seq.assign(&mut response);
                    writer.write(&response).await.unwrap();
                    seq.remove(&FlowKey {
                        rid: rid.clone(),
                        xid: xid_clone,
                    });
                }
            }
        });

        let switch = RelaySwitch::new(
            wrap_with_test_ids(vec![engine_sock]),
            test_fabric_registry(),
        )
        .await
        .unwrap();

        // Send REQ + END (caps already populated from construction)
        let req_id = MessageId::Uint(1);
        let req = Frame::req(
            req_id.clone(),
            "cap:effect=none",
            vec![1, 2, 3],
            "text/plain",
        );
        switch.send_to_master(req, None).await.unwrap();
        let end = Frame::end(req_id.clone(), None);
        switch.send_to_master(end, None).await.unwrap();

        let response = switch.read_from_masters().await.unwrap().unwrap();
        assert_eq!(response.frame_type, FrameType::End);
        assert_eq!(response.id, MessageId::Uint(1));
        assert_eq!(response.payload, Some(vec![42]));
    }

    // TEST427: Multi-master cap routing
    #[tokio::test]
    async fn test427_multi_master_cap_routing() {
        let (engine_sock1, slave_sock1) = UnixStream::pair().unwrap();
        let (engine_sock2, slave_sock2) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(
                slave_sock1,
                &serde_json::json!(["cap:effect=none"]),
                &Limits::default(),
            )
            .await;
            loop {
                match reader.read().await.unwrap() {
                    Some(frame) if frame.frame_type == FrameType::Req => {
                        let rid = frame.id.clone();
                        let xid = frame.routing_id.clone();
                        let mut response = Frame::end(rid.clone(), Some(vec![1]));
                        response.routing_id = xid.clone();
                        seq.assign(&mut response);
                        writer.write(&response).await.unwrap();
                        seq.remove(&FlowKey {
                            rid: rid.clone(),
                            xid,
                        });
                    }
                    Some(frame) if frame.frame_type == FrameType::End => {}
                    None => break,
                    _ => {}
                }
            }
        });

        tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(
                slave_sock2,
                &serde_json::json!([
                    "cap:effect=none",
                    "cap:in=\"media:void\";double;out=\"media:void\""
                ]),
                &Limits::default(),
            )
            .await;
            loop {
                match reader.read().await.unwrap() {
                    Some(frame) if frame.frame_type == FrameType::Req => {
                        let rid = frame.id.clone();
                        let xid = frame.routing_id.clone();
                        let mut response = Frame::end(rid.clone(), Some(vec![2]));
                        response.routing_id = xid.clone();
                        seq.assign(&mut response);
                        writer.write(&response).await.unwrap();
                        seq.remove(&FlowKey {
                            rid: rid.clone(),
                            xid,
                        });
                    }
                    Some(frame) if frame.frame_type == FrameType::End => {}
                    None => break,
                    _ => {}
                }
            }
        });

        let switch = RelaySwitch::new(
            wrap_with_test_ids(vec![engine_sock1, engine_sock2]),
            test_fabric_registry(),
        )
        .await
        .unwrap();

        // Caps already populated from construction — send requests directly
        let req1_id = MessageId::Uint(1);
        switch
            .send_to_master(
                Frame::req(req1_id.clone(), "cap:effect=none", vec![], "text/plain"),
                None,
            )
            .await
            .unwrap();
        switch
            .send_to_master(Frame::end(req1_id, None), None)
            .await
            .unwrap();
        let resp1 = switch.read_from_masters().await.unwrap().unwrap();
        assert_eq!(resp1.payload, Some(vec![1]));

        let req2_id = MessageId::Uint(2);
        switch
            .send_to_master(
                Frame::req(
                    req2_id.clone(),
                    "cap:in=\"media:void\";double;out=\"media:void\"",
                    vec![],
                    "text/plain",
                ),
                None,
            )
            .await
            .unwrap();
        switch
            .send_to_master(Frame::end(req2_id, None), None)
            .await
            .unwrap();
        let resp2 = switch.read_from_masters().await.unwrap().unwrap();
        assert_eq!(resp2.payload, Some(vec![2]));
    }

    // TEST428: Unknown cap returns error
    #[tokio::test]
    async fn test428_unknown_cap_returns_error() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            slave_notify_with_identity(
                slave_sock,
                &serde_json::json!(["cap:effect=none"]),
                &Limits::default(),
            )
            .await;
        });

        let switch = RelaySwitch::new(
            wrap_with_test_ids(vec![engine_sock]),
            test_fabric_registry(),
        )
        .await
        .unwrap();

        let req = Frame::req(
            MessageId::Uint(1),
            "cap:in=\"media:void\";unknown;out=\"media:void\"",
            vec![],
            "text/plain",
        );
        let result = switch.send_to_master(req, None).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            RelaySwitchError::NoHandler(_)
        ));
    }

    // TEST430: Tie-breaking (same cap on multiple masters - first match wins, routing is consistent)
    #[tokio::test]
    async fn test430_tie_breaking_same_cap_multiple_masters() {
        let same_cap = "cap:effect=none";

        let (engine_sock1, slave_sock1) = UnixStream::pair().unwrap();
        let (engine_sock2, slave_sock2) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(
                slave_sock1,
                &serde_json::json!([same_cap]),
                &Limits::default(),
            )
            .await;
            loop {
                match reader.read().await.unwrap() {
                    Some(frame) if frame.frame_type == FrameType::Req => {
                        let rid = frame.id.clone();
                        let xid = frame.routing_id.clone();
                        let mut response = Frame::end(rid.clone(), Some(vec![1]));
                        response.routing_id = xid.clone();
                        seq.assign(&mut response);
                        writer.write(&response).await.unwrap();
                        seq.remove(&FlowKey {
                            rid: rid.clone(),
                            xid,
                        });
                    }
                    Some(frame) if frame.frame_type == FrameType::End => {}
                    None => break,
                    _ => {}
                }
            }
        });

        tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(
                slave_sock2,
                &serde_json::json!([same_cap]),
                &Limits::default(),
            )
            .await;
            loop {
                match reader.read().await.unwrap() {
                    Some(frame) if frame.frame_type == FrameType::Req => {
                        let rid = frame.id.clone();
                        let xid = frame.routing_id.clone();
                        let mut response = Frame::end(rid.clone(), Some(vec![2]));
                        response.routing_id = xid.clone();
                        seq.assign(&mut response);
                        writer.write(&response).await.unwrap();
                        seq.remove(&FlowKey {
                            rid: rid.clone(),
                            xid,
                        });
                    }
                    Some(frame) if frame.frame_type == FrameType::End => {}
                    None => break,
                    _ => {}
                }
            }
        });

        let switch = RelaySwitch::new(
            wrap_with_test_ids(vec![engine_sock1, engine_sock2]),
            test_fabric_registry(),
        )
        .await
        .unwrap();

        // First request — should go to master 0 (first match)
        let req1_id = MessageId::Uint(1);
        switch
            .send_to_master(
                Frame::req(req1_id.clone(), same_cap, vec![], "text/plain"),
                None,
            )
            .await
            .unwrap();
        switch
            .send_to_master(Frame::end(req1_id, None), None)
            .await
            .unwrap();
        let resp1 = switch.read_from_masters().await.unwrap().unwrap();
        assert_eq!(resp1.payload, Some(vec![1]));

        // Second request — should ALSO go to master 0 (consistent routing)
        let req2_id = MessageId::Uint(2);
        switch
            .send_to_master(
                Frame::req(req2_id.clone(), same_cap, vec![], "text/plain"),
                None,
            )
            .await
            .unwrap();
        switch
            .send_to_master(Frame::end(req2_id, None), None)
            .await
            .unwrap();
        let resp2 = switch.read_from_masters().await.unwrap().unwrap();
        assert_eq!(resp2.payload, Some(vec![1]));
    }

    // TEST431: Continuation frame routing (CHUNK, END follow REQ)
    #[tokio::test]
    async fn test431_continuation_frame_routing() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(
                slave_sock,
                &serde_json::json!([
                    "cap:effect=none",
                    "cap:in=\"media:void\";test;out=\"media:void\""
                ]),
                &Limits::default(),
            )
            .await;

            let req = reader.read().await.unwrap().unwrap();
            assert_eq!(req.frame_type, FrameType::Req);
            let xid = req.routing_id.clone();

            let chunk = reader.read().await.unwrap().unwrap();
            assert_eq!(chunk.frame_type, FrameType::Chunk);
            assert_eq!(chunk.id, req.id);

            let end = reader.read().await.unwrap().unwrap();
            assert_eq!(end.frame_type, FrameType::End);
            assert_eq!(end.id, req.id);

            let rid = req.id.clone();
            let xid_clone = xid.clone();
            let mut response = Frame::end(rid.clone(), Some(vec![42]));
            response.routing_id = xid;
            seq.assign(&mut response);
            writer.write(&response).await.unwrap();
            seq.remove(&FlowKey {
                rid: rid.clone(),
                xid: xid_clone,
            });
        });

        let switch = RelaySwitch::new(
            wrap_with_test_ids(vec![engine_sock]),
            test_fabric_registry(),
        )
        .await
        .unwrap();

        let req_id = MessageId::Uint(1);
        switch
            .send_to_master(
                Frame::req(
                    req_id.clone(),
                    "cap:in=\"media:void\";test;out=\"media:void\"",
                    vec![],
                    "text/plain",
                ),
                None,
            )
            .await
            .unwrap();
        let payload = vec![1, 2, 3];
        let checksum = Frame::compute_checksum(&payload);
        switch
            .send_to_master(
                Frame::chunk(
                    req_id.clone(),
                    "stream1".to_string(),
                    0,
                    payload,
                    0,
                    checksum,
                ),
                None,
            )
            .await
            .unwrap();
        switch
            .send_to_master(Frame::end(req_id.clone(), None), None)
            .await
            .unwrap();

        let response = switch.read_from_masters().await.unwrap().unwrap();
        assert_eq!(response.frame_type, FrameType::End);
        assert_eq!(response.payload, Some(vec![42]));
    }

    // TEST432: Empty masters list creates empty switch, add_master works
    #[tokio::test]
    async fn test432_empty_masters_allowed() {
        let switch = RelaySwitch::new(wrap_with_test_ids(vec![]), test_fabric_registry())
            .await
            .unwrap();

        // Empty switch has no caps
        let caps: Vec<String> = serde_json::from_slice(&switch.capabilities().await).unwrap();
        assert!(caps.is_empty(), "empty switch should have no caps");

        // No handler for any cap
        assert_eq!(
            switch.find_master_for_cap("cap:effect=none", None).await,
            None
        );
    }

    // TEST433: Capability aggregation deduplicates caps
    #[tokio::test]
    async fn test433_capability_aggregation_deduplicates() {
        let (engine_sock1, slave_sock1) = UnixStream::pair().unwrap();
        let (engine_sock2, slave_sock2) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            slave_notify_with_identity(
                slave_sock1,
                &serde_json::json!([
                    "cap:effect=none",
                    "cap:in=\"media:void\";double;out=\"media:void\""
                ]),
                &Limits::default(),
            )
            .await;
        });

        tokio::spawn(async move {
            slave_notify_with_identity(
                slave_sock2,
                &serde_json::json!([
                    "cap:effect=none",
                    "cap:in=\"media:void\";triple;out=\"media:void\""
                ]),
                &Limits::default(),
            )
            .await;
        });

        let switch = RelaySwitch::new(
            wrap_with_test_ids(vec![engine_sock1, engine_sock2]),
            test_fabric_registry(),
        )
        .await
        .unwrap();

        // Caps already populated during construction (plain JSON array
        // of canonical cap URN strings — alphabetical tag order, no
        // unnecessary quoting, `cap:effect=none` for the identity cap).
        let mut cap_list: Vec<String> =
            serde_json::from_slice(&switch.capabilities().await).unwrap();
        cap_list.sort();

        assert_eq!(cap_list.len(), 3);
        assert!(cap_list.contains(&"cap:double;in=media:void;out=media:void".to_string()));
        assert!(cap_list.contains(&crate::standard::caps::CAP_IDENTITY.to_string()));
        assert!(cap_list.contains(&"cap:in=media:void;out=media:void;triple".to_string()));
    }

    // TEST434: Limits negotiation takes minimum
    #[tokio::test]
    async fn test434_limits_negotiation_minimum() {
        let (engine_sock1, slave_sock1) = UnixStream::pair().unwrap();
        let (engine_sock2, slave_sock2) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            slave_notify_with_identity(
                slave_sock1,
                &serde_json::json!(["cap:effect=none"]),
                &Limits {
                    max_frame: 1_000_000,
                    max_chunk: 100_000,
                    ..Limits::default()
                },
            )
            .await;
        });

        tokio::spawn(async move {
            slave_notify_with_identity(
                slave_sock2,
                &serde_json::json!(["cap:effect=none"]),
                &Limits {
                    max_frame: 2_000_000,
                    max_chunk: 50_000,
                    ..Limits::default()
                },
            )
            .await;
        });

        let switch = RelaySwitch::new(
            wrap_with_test_ids(vec![engine_sock1, engine_sock2]),
            test_fabric_registry(),
        )
        .await
        .unwrap();

        // Limits already negotiated during construction
        assert_eq!(switch.limits().await.max_frame, 1_000_000);
        assert_eq!(switch.limits().await.max_chunk, 50_000);
    }

    // TEST435: URN matching (exact vs accepts())
    #[tokio::test]
    async fn test435_urn_matching_exact_and_accepts() {
        let registered_cap = "cap:in=\"media:text;utf8\";process;out=\"media:text;utf8\"";

        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(
                slave_sock,
                &serde_json::json!(["cap:effect=none", registered_cap]),
                &Limits::default(),
            )
            .await;
            loop {
                match reader.read().await.unwrap() {
                    Some(frame) if frame.frame_type == FrameType::Req => {
                        let rid = frame.id.clone();
                        let xid = frame.routing_id.clone();
                        let mut response = Frame::end(rid.clone(), Some(vec![42]));
                        response.routing_id = xid.clone();
                        seq.assign(&mut response);
                        writer.write(&response).await.unwrap();
                        seq.remove(&FlowKey {
                            rid: rid.clone(),
                            xid,
                        });
                    }
                    Some(frame) if frame.frame_type == FrameType::End => {}
                    None => break,
                    _ => {}
                }
            }
        });

        let switch = RelaySwitch::new(
            wrap_with_test_ids(vec![engine_sock]),
            test_fabric_registry(),
        )
        .await
        .unwrap();

        // Exact match should work
        let req1_id = MessageId::Uint(1);
        switch
            .send_to_master(
                Frame::req(req1_id.clone(), registered_cap, vec![], "text/plain"),
                None,
            )
            .await
            .unwrap();
        switch
            .send_to_master(Frame::end(req1_id, None), None)
            .await
            .unwrap();
        let resp1 = switch.read_from_masters().await.unwrap().unwrap();
        assert_eq!(resp1.payload, Some(vec![42]));

        // Request with more specific input and less specific output SHOULD match
        // Input (contravariant): request's `media:text;utf8;normalized` conforms to candidate's `media:text;utf8`
        // Output (covariant): candidate's `media:text;utf8` conforms to request's `media:text`
        let req2_id = MessageId::Uint(2);
        switch
            .send_to_master(
                Frame::req(
                    req2_id.clone(),
                    "cap:in=\"media:text;utf8;normalized\";process;out=\"media:text\"",
                    vec![],
                    "text/plain",
                ),
                None,
            )
            .await
            .unwrap();
        switch
            .send_to_master(Frame::end(req2_id, None), None)
            .await
            .unwrap();
        let resp2 = switch.read_from_masters().await.unwrap().unwrap();
        assert_eq!(resp2.payload, Some(vec![42]));

        // Request with INCOMPATIBLE input should NOT match (different type family)
        let req3 = Frame::req(
            MessageId::Uint(3),
            "cap:in=\"media:ext=png;image\";process;out=\"media:text\"",
            vec![],
            "text/plain",
        );
        let result = switch.send_to_master(req3, None).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            RelaySwitchError::NoHandler(_)
        ));
    }

    // =========================================================================
    // Preferred cap routing tests
    // =========================================================================

    // TEST437: find_master_for_cap with preferred_cap routes to generic handler
    //
    // With is_dispatchable semantics:
    // - Generic candidate (in=media:) CAN dispatch specific request (in="media:ext=pdf")
    //   because media: (wildcard) accepts any input type
    // - Preference routes to preferred among dispatchable candidates
    #[tokio::test]
    async fn test437_preferred_cap_routes_to_generic() {
        let (engine_sock0, slave_sock0) = UnixStream::pair().unwrap();
        let (engine_sock1, slave_sock1) = UnixStream::pair().unwrap();

        // Master 0: generic thumbnail handler (like internal ThumbnailCartridge)
        let generic_cap = "cap:in=media:;generate-thumbnail;out=\"media:ext=png;image;thumbnail\"";
        tokio::spawn(async move {
            slave_notify_with_identity(
                slave_sock0,
                &serde_json::json!(["cap:effect=none", generic_cap]),
                &Limits::default(),
            )
            .await;
        });

        // Master 1: specific thumbnail handler (like pdfcartridge)
        let specific_cap =
            "cap:in=\"media:ext=pdf\";generate-thumbnail;out=\"media:ext=png;image;thumbnail\"";
        tokio::spawn(async move {
            slave_notify_with_identity(
                slave_sock1,
                &serde_json::json!(["cap:effect=none", specific_cap]),
                &Limits::default(),
            )
            .await;
        });

        let switch = RelaySwitch::new(
            wrap_with_test_ids(vec![engine_sock0, engine_sock1]),
            test_fabric_registry(),
        )
        .await
        .unwrap();

        // Specific request for PDF thumbnail
        let request =
            "cap:in=\"media:ext=pdf\";generate-thumbnail;out=\"media:ext=png;image;thumbnail\"";

        // Without preference: routes to master 1 (specific, closest-specificity)
        assert_eq!(switch.find_master_for_cap(request, None).await, Some(1));

        // With preference for generic cap: routes to master 0 (generic, via is_equivalent)
        assert_eq!(
            switch.find_master_for_cap(request, Some(generic_cap)).await,
            Some(0)
        );

        // With preference for specific cap: routes to master 1 (specific, matches preference)
        assert_eq!(
            switch
                .find_master_for_cap(request, Some(specific_cap))
                .await,
            Some(1)
        );
    }

    // TEST438: find_master_for_cap with preference falls back to closest-specificity
    //          when preferred cap is not in the comparable set
    #[tokio::test]
    async fn test438_preferred_cap_falls_back_when_not_comparable() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        // Master 0: only has a specific cap
        let registered =
            "cap:in=\"media:ext=pdf\";generate-thumbnail;out=\"media:ext=png;image;thumbnail\"";
        tokio::spawn(async move {
            slave_notify_with_identity(
                slave_sock,
                &serde_json::json!(["cap:effect=none", registered]),
                &Limits::default(),
            )
            .await;
        });

        let switch = RelaySwitch::new(
            wrap_with_test_ids(vec![engine_sock]),
            test_fabric_registry(),
        )
        .await
        .unwrap();

        let request =
            "cap:in=\"media:ext=pdf\";generate-thumbnail;out=\"media:ext=png;image;thumbnail\"";

        // Preference for an unrelated cap — no equivalent match, falls back to closest-specificity
        let unrelated =
            "cap:in=\"media:enc=utf-8;ext=txt\";generate-thumbnail;out=\"media:ext=png;image;thumbnail\"";
        assert_eq!(
            switch.find_master_for_cap(request, Some(unrelated)).await,
            Some(0)
        );
    }

    // TEST439: Generic candidate CAN dispatch specific request
    //          (but only matches if no more specific candidate exists)
    //
    // With is_dispatchable: generic candidate (in=media:) CAN handle specific
    // request (in="media:ext=pdf") because media: accepts any input type.
    // With preference, can route to generic even when more specific exists.
    #[tokio::test]
    async fn test439_generic_candidate_can_dispatch_specific_request() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        // Master 0: only generic handler (in=media: wildcard)
        let generic_cap = "cap:in=media:;generate-thumbnail;out=\"media:ext=png;image;thumbnail\"";
        tokio::spawn(async move {
            slave_notify_with_identity(
                slave_sock,
                &serde_json::json!(["cap:effect=none", generic_cap]),
                &Limits::default(),
            )
            .await;
        });

        let switch = RelaySwitch::new(
            wrap_with_test_ids(vec![engine_sock]),
            test_fabric_registry(),
        )
        .await
        .unwrap();

        // Specific PDF request — generic handler CAN dispatch it
        // because candidate's wildcard input (media:) accepts any input type
        let request =
            "cap:in=\"media:ext=pdf\";generate-thumbnail;out=\"media:ext=png;image;thumbnail\"";
        assert_eq!(
            switch.find_master_for_cap(request, None).await,
            Some(0),
            "Generic candidate can dispatch specific request as fallback"
        );

        // With preference for generic — routes to master 0
        assert_eq!(
            switch.find_master_for_cap(request, Some(generic_cap)).await,
            Some(0),
            "Preference routes to generic candidate"
        );
    }

    // =========================================================================
    // Identity verification integration tests
    // =========================================================================

    // TEST487: RelaySwitch construction verifies identity through relay chain
    #[tokio::test]
    async fn test487_relay_switch_identity_verification_succeeds() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            slave_notify_with_identity(
                slave_sock,
                &serde_json::json!([
                    "cap:effect=none",
                    "cap:in=\"media:void\";test;out=\"media:void\""
                ]),
                &Limits::default(),
            )
            .await;
        });

        let switch = RelaySwitch::new(
            wrap_with_test_ids(vec![engine_sock]),
            test_fabric_registry(),
        )
        .await
        .unwrap();

        // Construction succeeded — caps are populated
        assert_eq!(
            switch.find_master_for_cap("cap:effect=none", None).await,
            Some(0)
        );
        assert_eq!(
            switch
                .find_master_for_cap("cap:in=\"media:void\";test;out=\"media:void\"", None)
                .await,
            Some(0)
        );
    }

    // TEST488: RelaySwitch construction fails when master's identity verification fails
    #[tokio::test]
    async fn test488_relay_switch_identity_verification_fails() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            let (read_half, write_half) = slave_sock.into_split();
            let mut reader = FrameReader::new(BufReader::new(read_half));
            let mut writer = FrameWriter::new(BufWriter::new(write_half));

            // Send RelayNotify — an installed cartridge whose single
            // cap-group declares CAP_IDENTITY so the host clears the
            // payload-level identity check before the engine probes.
            let caps = serde_json::json!({
                "installed_cartridges": [
                    {
                        "registry_url": null,
                        "channel": "release",
                        "id": "broken-cartridge",
                        "version": "0.0.0",
                        "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                    "runtime_stats": {
                        "running": true,
                        "pools": test_pool_states_json(&["cap:effect=none"], 0, 0),
                        "active_request_count": 0,
                        "peer_request_count": 0,
                        "memory_footprint_mb": 0,
                        "memory_rss_mb": 0,
                        "restart_count": 0
                    },
                        "cap_groups": [
                            {
                                "name": "test",
                                "caps": [
                                    {
                                        "urn": "cap:effect=none",
                                        "title": "Identity",
                                        "aliases": ["identity"],
                                        "args": [],
                                    }
                                ],
                                "adapter_urns": [],
                            }
                        ],
                    }
                ],
            });
            let notify =
                Frame::relay_notify(&serde_json::to_vec(&caps).unwrap(), &Limits::default());
            writer.write(&notify).await.unwrap();

            // Read identity REQ, respond with ERR
            let req = reader.read().await.unwrap().expect("expected identity REQ");
            assert_eq!(req.frame_type, FrameType::Req);
            let err = Frame::err(
                req.id,
                "BROKEN",
                crate::AttributionClass::Internal,
                "identity verification broken",
                None,
            );
            writer.write(&err).await.unwrap();
        });

        let result = RelaySwitch::new(
            wrap_with_test_ids(vec![engine_sock]),
            test_fabric_registry(),
        )
        .await;
        assert!(
            result.is_err(),
            "construction must fail when identity verification fails"
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("identity verification failed"),
            "error must mention identity verification: {}",
            err
        );
    }

    // TEST0131: When a master initially advertises empty caps (so
    // `add_master` skips the identity probe) and later sends a
    // RelayNotify update with non-empty caps, the relay must run an
    // end-to-end identity probe before the new caps become routable.
    // A master that fails to answer the runtime probe with the
    // expected nonce echo must end up unhealthy with `last_error`
    // populated, and its caps must NOT appear in the cap_table.
    //
    // This test guards the wire-protocol regression where the
    // RelayNotify-update path published caps without re-verifying
    // identity end-to-end. Removing the runtime probe re-introduces
    // the hole; this test fails loudly when that happens.
    #[tokio::test]
    async fn test0131_runtime_identity_probe_required_on_empty_to_nonempty_transition() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            let (read_half, write_half) = slave_sock.into_split();
            let mut reader = FrameReader::new(BufReader::new(read_half));
            let mut writer = FrameWriter::new(BufWriter::new(write_half));

            // Initial RelayNotify — empty installed_cartridges so
            // `add_master` skips the synchronous identity probe.
            let initial = serde_json::json!({ "installed_cartridges": [] });
            writer
                .write(&Frame::relay_notify(
                    &serde_json::to_vec(&initial).unwrap(),
                    &Limits::default(),
                ))
                .await
                .unwrap();

            // Send a runtime RelayNotify update with a real cartridge
            // declaring CAP_IDENTITY plus another cap. The relay's
            // RelayNotify-update branch should detect the empty →
            // non-empty transition and queue a runtime identity probe.
            let updated = serde_json::json!({
                "installed_cartridges": [
                    {
                        "registry_url": null,
                        "channel": "release",
                        "id": "test-cartridge",
                        "version": "0.0.0",
                        "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                    "runtime_stats": {
                        "running": true,
                        "pools": test_pool_states_json(&["cap:effect=none", "cap:in=\"media:void\";test;out=\"media:void\""], 0, 0),
                        "active_request_count": 0,
                        "peer_request_count": 0,
                        "memory_footprint_mb": 0,
                        "memory_rss_mb": 0,
                        "restart_count": 0
                    },
                        "cap_groups": [
                            {
                                "name": "test",
                                "caps": [
                                    {
                                        "urn": "cap:effect=none",
                                        "title": "Identity",
                                        "aliases": ["identity"],
                                        "args": [],
                                    },
                                    {
                                        "urn": "cap:in=\"media:void\";test;out=\"media:void\"",
                                        "title": "Test",
                                        "aliases": ["test"],
                                        "args": [],
                                    }
                                ],
                                "adapter_urns": [],
                            }
                        ],
                    }
                ],
            });
            writer
                .write(&Frame::relay_notify(
                    &serde_json::to_vec(&updated).unwrap(),
                    &Limits::default(),
                ))
                .await
                .unwrap();

            // Read the identity REQ from the relay and respond with
            // an ERR — this models a cartridge whose identity
            // handler is broken. The runtime probe driver must
            // observe the ERR and mark the master unhealthy.
            loop {
                let frame = match reader.read().await {
                    Ok(Some(f)) => f,
                    Ok(None) => return,
                    Err(_) => return,
                };
                if frame.frame_type == FrameType::Req {
                    let mut err = Frame::err(
                        frame.id.clone(),
                        "BROKEN",
                        crate::AttributionClass::Internal,
                        "test cartridge",
                        None,
                    );
                    err.routing_id = frame.routing_id.clone();
                    let _ = writer.write(&err).await;
                    return;
                }
            }
        });

        let switch = Arc::new(
            RelaySwitch::new(
                wrap_with_test_ids(vec![engine_sock]),
                test_fabric_registry(),
            )
            .await
            .expect("RelaySwitch construction must succeed for empty-cap initial notify"),
        );
        switch.start_background_pump();

        // Wait for the runtime probe driver to process the failure.
        // The probe times out at 10s; we poll with a generous bound.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        let mut master_unhealthy = false;
        while std::time::Instant::now() < deadline {
            let masters = switch.masters.read().await;
            if let Some(master) = masters.first() {
                if !master.healthy.load(Ordering::SeqCst)
                    && master.last_error.read().await.is_some()
                {
                    master_unhealthy = true;
                    break;
                }
            }
            drop(masters);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(
            master_unhealthy,
            "master must be marked unhealthy after the runtime identity probe fails"
        );

        // The master's caps must NOT appear in the cap_table — even
        // though the host advertised them, the failed probe means
        // the relay refuses to route to this master.
        let cap_table = switch.cap_table.read().await;
        assert!(
            !cap_table.iter().any(|(urn, _)| urn.contains("test")),
            "unverified master's caps must be excluded from cap_table, got: {:?}",
            *cap_table
        );
    }

    // TEST0135: the runtime identity probe SUCCESS path — a master that
    // advertises caps AFTER connecting (empty→non-empty) and then passes the
    // probe must flip healthy and its caps must become routable. This is the
    // companion to test0131 (which covers probe FAILURE → stays unhealthy):
    // together they pin both outcomes of the deferred-probe state machine.
    #[tokio::test]
    async fn test0135_runtime_identity_probe_success_makes_caps_routable() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            slave_deferred_identity(
                slave_sock,
                &serde_json::json!([
                    "cap:effect=none",
                    "cap:in=\"media:void\";test;out=\"media:void\""
                ]),
                &Limits::default(),
                true, // probe succeeds
            )
            .await;
        });

        let switch = Arc::new(
            RelaySwitch::new(
                wrap_with_test_ids(vec![engine_sock]),
                test_fabric_registry(),
            )
            .await
            .expect("RelaySwitch construction must succeed for empty-cap initial notify"),
        );
        switch.start_background_pump();

        // Poll until the probe driver completes the round-trip: the
        // post-init advertised cap becomes routable only AFTER the probe
        // passes (it is held back while the master is unhealthy).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        let mut routable = false;
        while std::time::Instant::now() < deadline {
            if switch
                .find_master_for_cap("cap:in=\"media:void\";test;out=\"media:void\"", None)
                .await
                == Some(0)
            {
                routable = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(
            routable,
            "after a successful runtime identity probe the master's post-init advertised cap must become routable"
        );
        let masters = switch.masters.read().await;
        assert!(
            masters[0].healthy.load(Ordering::SeqCst),
            "master must be marked healthy after a successful runtime identity probe"
        );
    }

    // TEST1897: the installed-cartridge INVENTORY is NOT health-filtered. A
    // master held unhealthy by a failed runtime identity probe still has its
    // cartridges visible in the aggregate inventory (so a transient master
    // flap does not make cartridges "disappear" from the engine's view),
    // even though its caps are excluded from ROUTING. This pins the
    // deliberate asymmetry between the inventory aggregate (unfiltered) and
    // the cap table / routable set (health-filtered).
    #[tokio::test]
    async fn test1897_unhealthy_master_inventory_retained_but_not_routable() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            slave_deferred_identity(
                slave_sock,
                &serde_json::json!([
                    "cap:effect=none",
                    "cap:in=\"media:void\";test;out=\"media:void\""
                ]),
                &Limits::default(),
                false, // probe fails → master stays unhealthy
            )
            .await;
        });

        let switch = Arc::new(
            RelaySwitch::new(
                wrap_with_test_ids(vec![engine_sock]),
                test_fabric_registry(),
            )
            .await
            .expect("RelaySwitch construction must succeed for empty-cap initial notify"),
        );
        switch.start_background_pump();

        // Wait for the probe driver to fail the probe and mark the master
        // unhealthy.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        let mut unhealthy = false;
        while std::time::Instant::now() < deadline {
            {
                let masters = switch.masters.read().await;
                if let Some(master) = masters.first() {
                    if !master.healthy.load(Ordering::SeqCst)
                        && master.last_error.read().await.is_some()
                    {
                        unhealthy = true;
                        break;
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(unhealthy, "master must be unhealthy after the probe fails");

        // ROUTING: the unhealthy master's caps are excluded.
        assert_eq!(
            switch
                .find_master_for_cap("cap:in=\"media:void\";test;out=\"media:void\"", None)
                .await,
            None,
            "an unhealthy master's caps must NOT be routable"
        );

        // INVENTORY: the cartridge is STILL visible — the inventory aggregate
        // is deliberately not health-filtered.
        let inventory = switch.installed_cartridges().await;
        assert!(
            inventory.iter().any(|c| c.id == "test-cartridge"),
            "an unhealthy master's installed cartridges must remain visible in the inventory aggregate, got: {:?}",
            inventory.iter().map(|c| c.id.clone()).collect::<Vec<_>>()
        );
    }

    // TEST1898: the routable-capability watch (subscribe_capabilities). A
    // subscriber must receive the CURRENT routable cap set on subscribe even
    // though it was rebuilt during construction — BEFORE any receiver
    // existed (the channel must persist the value, i.e. use send_replace, not
    // send, which drops the value when there are momentarily zero receivers).
    // The delivered set must be the health-filtered routable cap URNs. This
    // is the engine-readiness signal: a cap appears here only once its master
    // is verified and routable.
    #[tokio::test]
    async fn test1898_subscribe_capabilities_delivers_routable_set() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            slave_notify_with_identity(
                slave_sock,
                &serde_json::json!([
                    "cap:effect=none",
                    "cap:in=\"media:void\";test;out=\"media:void\""
                ]),
                &Limits::default(),
            )
            .await;
        });

        // Capabilities are rebuilt inside new() — before we subscribe.
        let switch = RelaySwitch::new(
            wrap_with_test_ids(vec![engine_sock]),
            test_fabric_registry(),
        )
        .await
        .unwrap();

        let rx = switch.subscribe_capabilities();
        let watched = rx.borrow().clone();

        // The watch must mirror the synchronous getter: identical serialized
        // routable-set snapshot. This is a snapshot-identity check (same
        // bytes from the same source), NOT a URN comparison. It is also what
        // catches the bug this test guards: the snapshot is rebuilt inside
        // new() BEFORE any subscriber exists, so the channel must persist it
        // (send_replace). A plain `send` is a no-op with zero receivers and
        // would leave the watch holding the empty initial value while the
        // getter returns the populated set — making these two diverge.
        assert_eq!(
            watched,
            switch.capabilities().await,
            "the capability watch must deliver the same routable-set snapshot as capabilities()"
        );

        // Prove that snapshot is the live ROUTABLE set the only correct way —
        // via dispatch conformance, not string comparison of URNs. The
        // request cap is the pattern; find_master_for_cap parses it and
        // checks each candidate with is_dispatchable. A healthy, verified
        // master makes its advertised cap dispatchable.
        assert_eq!(
            switch
                .find_master_for_cap("cap:in=\"media:void\";test;out=\"media:void\"", None)
                .await,
            Some(0),
            "the routable set the watch delivers must make the master's advertised cap dispatchable"
        );
    }

    // TEST905: send_to_master + build_request_frames through RelaySwitch → RelaySlave → InProcessCartridgeHost roundtrip
    #[tokio::test]
    async fn test905_send_to_master_build_request_frames_roundtrip() {
        use crate::bifaci::cartridge_runtime::PeerInvoker;
        use crate::bifaci::in_process_host::{
            accumulate_input, FrameHandler, InProcessCartridgeHost, ResponseWriter,
        };
        use crate::bifaci::relay::RelaySlave;
        use crate::cap::caller::CapArgumentValue;
        use crate::cap::definition::Cap;
        use async_trait::async_trait;
        use tokio::sync::mpsc;

        /// Echo handler: accumulates input, echoes raw bytes back.
        #[derive(Debug)]
        struct EchoHandler;

        #[async_trait]
        impl FrameHandler for EchoHandler {
            async fn handle_request(
                &self,
                _cap_urn: &str,
                mut input: mpsc::UnboundedReceiver<Frame>,
                output: ResponseWriter,
                _peer: Arc<dyn PeerInvoker>,
            ) {
                match accumulate_input(&mut input).await {
                    Ok((args, meta)) => {
                        let data: Vec<u8> = args.iter().flat_map(|a| a.value.clone()).collect();
                        output.emit_response_with_meta("media:text", &data, meta);
                    }
                    Err(e) => output.emit_error("ACCUMULATE_ERROR", &e),
                }
            }
        }

        let cap_urn_str = "cap:in=\"media:text\";echo;out=\"media:text\"";
        let cap = Cap {
            urn: crate::CapUrn::from_string(cap_urn_str).unwrap(),
            version: 1,
            title: "echo".to_string(),
            cap_description: None,
            documentation: None,
            metadata: std::collections::HashMap::new(),
            aliases: vec![String::new()],
            is_abstract: false,
            args: Vec::new(),
            output: None,
            metadata_json: None,
            registered_by: None,
            supported_model_types: Vec::new(),
            default_model_spec: None,
        };

        let host = InProcessCartridgeHost::new(
            crate::bifaci::in_process_host::InProcessHostIdentity::for_test("echo-host"),
            vec![(
                "echo".to_string(),
                vec![cap],
                std::sync::Arc::new(EchoHandler) as std::sync::Arc<dyn FrameHandler>,
            )],
        );

        // Create socket pairs (one for host↔slave, one for slave↔switch)
        let (host_sock, slave_local_sock) = UnixStream::pair().unwrap();
        let (slave_sock, switch_sock) = UnixStream::pair().unwrap();

        let (host_read, host_write) = host_sock.into_split();
        let host_task = tokio::spawn(async move {
            host.run(host_read, host_write).await.unwrap();
        });

        let (slave_local_read, slave_local_write) = slave_local_sock.into_split();
        let slave = RelaySlave::new(slave_local_read, slave_local_write);
        let (slave_read, slave_write) = slave_sock.into_split();
        let slave_task = tokio::spawn(async move {
            let socket_reader = FrameReader::new(BufReader::new(slave_read));
            let socket_writer = FrameWriter::new(BufWriter::new(slave_write));
            slave.run(socket_reader, socket_writer, None).await.unwrap();
        });

        let switch = RelaySwitch::new(
            wrap_with_test_ids(vec![switch_sock]),
            test_fabric_registry(),
        )
        .await
        .unwrap();

        // Verify the switch has our echo cap registered
        let caps_json: Vec<String> = serde_json::from_slice(&switch.capabilities().await).unwrap();
        assert!(
            caps_json.iter().any(|c| c.contains("echo")),
            "switch should have echo cap, got: {:?}",
            caps_json
        );

        // Build request frames using the helper
        let rid = MessageId::new_uuid();
        let max_chunk = switch.limits().await.max_chunk;
        let frames = CapArgumentValue::build_request_frames(
            &rid,
            cap_urn_str,
            &[CapArgumentValue::new(
                "media:text",
                b"hello streaming world".to_vec(),
            )],
            max_chunk,
        );

        // Send each frame via send_to_master (no preference)
        for frame in frames {
            switch.send_to_master(frame, None).await.unwrap();
        }

        // Read response frames via read_from_masters_timeout
        // Response chunks are CBOR-encoded (matching emit_response)
        let mut response_data = Vec::new();
        let mut got_end = false;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            match switch
                .read_from_masters_timeout(std::time::Duration::from_millis(200))
                .await
            {
                Ok(Some(frame)) if frame.id == rid => {
                    match frame.frame_type {
                        FrameType::Chunk => {
                            if let Some(payload) = &frame.payload {
                                // CBOR-decode chunk payload to get raw bytes
                                let value: ciborium::Value = ciborium::from_reader(&payload[..])
                                    .expect("response chunk not valid CBOR");
                                match value {
                                    ciborium::Value::Bytes(b) => {
                                        response_data.extend_from_slice(&b)
                                    }
                                    ciborium::Value::Text(s) => {
                                        response_data.extend_from_slice(s.as_bytes())
                                    }
                                    other => {
                                        panic!("unexpected CBOR type in response: {:?}", other)
                                    }
                                }
                            }
                        }
                        FrameType::End => {
                            got_end = true;
                            break;
                        }
                        FrameType::Err => {
                            panic!(
                                "Got ERR: [{:?}] {:?}",
                                frame.error_code(),
                                frame.error_message()
                            );
                        }
                        _ => {} // STREAM_START, STREAM_END — skip
                    }
                }
                Ok(Some(_)) => {} // Frame for different RID (e.g., RelayNotify)
                Ok(None) => {}    // Timeout — retry
                Err(e) => panic!("read_from_masters_timeout error: {}", e),
            }
        }

        assert!(got_end, "should have received END frame");
        assert_eq!(
            response_data, b"hello streaming world",
            "echo handler should return input"
        );

        drop(switch);
        drop(slave_task);
        drop(host_task);
    }

    // TEST0132: add_master dynamically connects new host to running switch
    #[tokio::test]
    async fn test0132_add_master_dynamic() {
        use crate::bifaci::cartridge_runtime::PeerInvoker;
        use crate::bifaci::in_process_host::{
            FrameHandler, InProcessCartridgeHost, ResponseWriter,
        };
        use crate::bifaci::relay::RelaySlave;
        use crate::cap::caller::CapArgumentValue;
        use crate::cap::definition::Cap;
        use async_trait::async_trait;
        use tokio::sync::mpsc;

        /// Handler that returns a constant byte string (ignores input).
        #[derive(Debug)]
        struct ConstHandler(&'static str);

        #[async_trait]
        impl FrameHandler for ConstHandler {
            async fn handle_request(
                &self,
                _cap_urn: &str,
                mut input: mpsc::UnboundedReceiver<Frame>,
                output: ResponseWriter,
                _peer: Arc<dyn PeerInvoker>,
            ) {
                // Drain input
                while let Some(frame) = input.recv().await {
                    if frame.frame_type == FrameType::End {
                        break;
                    }
                }
                output.emit_response("media:", self.0.as_bytes());
            }
        }

        // Helper to wire up host + slave and return switch-side socket + task handles
        async fn wire_host(
            host: InProcessCartridgeHost,
        ) -> (
            UnixStream,
            tokio::task::JoinHandle<()>,
            tokio::task::JoinHandle<()>,
        ) {
            let (host_sock, slave_local_sock) = UnixStream::pair().unwrap();
            let (slave_sock, switch_sock) = UnixStream::pair().unwrap();

            let (host_read, host_write) = host_sock.into_split();
            let host_task = tokio::spawn(async move {
                host.run(host_read, host_write).await.unwrap();
            });

            let (slave_local_read, slave_local_write) = slave_local_sock.into_split();
            let slave = RelaySlave::new(slave_local_read, slave_local_write);
            let (slave_read, slave_write) = slave_sock.into_split();
            let slave_task = tokio::spawn(async move {
                let sr = FrameReader::new(BufReader::new(slave_read));
                let sw = FrameWriter::new(BufWriter::new(slave_write));
                slave.run(sr, sw, None).await.unwrap();
            });

            (switch_sock, host_task, slave_task)
        }

        // Create initial switch with handler A
        let cap_a = "cap:in=\"media:void\";alpha;out=\"media:void\"";
        let host_a = InProcessCartridgeHost::new(
            crate::bifaci::in_process_host::InProcessHostIdentity::for_test("alpha-host"),
            vec![(
                "alpha".to_string(),
                vec![Cap {
                    version: 1,
                    urn: crate::CapUrn::from_string(cap_a).unwrap(),
                    title: "alpha".to_string(),
                    cap_description: None,
                    documentation: None,
                    metadata: std::collections::HashMap::new(),
                    aliases: vec![String::new()],
                    is_abstract: false,
                    args: Vec::new(),
                    output: None,
                    metadata_json: None,
                    registered_by: None,
                    supported_model_types: Vec::new(),
                    default_model_spec: None,
                }],
                std::sync::Arc::new(ConstHandler("alpha")) as std::sync::Arc<dyn FrameHandler>,
            )],
        );

        let (switch_sock_a, ht_a, st_a) = wire_host(host_a).await;
        let switch = RelaySwitch::new(
            wrap_with_test_ids(vec![switch_sock_a]),
            test_fabric_registry(),
        )
        .await
        .unwrap();
        assert_eq!(switch.masters.read().await.len(), 1);

        // Add handler B dynamically
        let cap_b = "cap:in=\"media:void\";beta;out=\"media:void\"";
        let host_b = InProcessCartridgeHost::new(
            crate::bifaci::in_process_host::InProcessHostIdentity::for_test("beta-host"),
            vec![(
                "beta".to_string(),
                vec![Cap {
                    version: 1,
                    urn: crate::CapUrn::from_string(cap_b).unwrap(),
                    title: "beta".to_string(),
                    cap_description: None,
                    documentation: None,
                    metadata: std::collections::HashMap::new(),
                    aliases: vec![String::new()],
                    is_abstract: false,
                    args: Vec::new(),
                    output: None,
                    metadata_json: None,
                    registered_by: None,
                    supported_model_types: Vec::new(),
                    default_model_spec: None,
                }],
                std::sync::Arc::new(ConstHandler("beta")) as std::sync::Arc<dyn FrameHandler>,
            )],
        );

        let (switch_sock_b, ht_b, st_b) = wire_host(host_b).await;
        // Use a distinct id so this test exercises the append branch
        // of `add_master` (the constructor already filled `test-master-0`
        // via `wrap_with_test_ids`; this is a fresh slot at index 1).
        let idx = switch
            .add_master("test-master-1", switch_sock_b)
            .await
            .unwrap();
        assert_eq!(idx, 1);
        assert_eq!(switch.masters.read().await.len(), 2);

        // Verify both caps are in aggregate capabilities
        let caps_json: Vec<String> = serde_json::from_slice(&switch.capabilities().await).unwrap();
        assert!(caps_json.iter().any(|c| c.contains("alpha")));
        assert!(caps_json.iter().any(|c| c.contains("beta")));

        // Execute against beta (dynamically added master) using send_to_master + build_request_frames
        let rid = MessageId::new_uuid();
        let max_chunk = switch.limits().await.max_chunk;
        let frames = CapArgumentValue::build_request_frames(&rid, cap_b, &[], max_chunk);
        for frame in frames {
            switch.send_to_master(frame, None).await.unwrap();
        }

        // Response chunks are CBOR-encoded
        let mut response_data = Vec::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            match switch
                .read_from_masters_timeout(std::time::Duration::from_millis(200))
                .await
            {
                Ok(Some(frame)) if frame.id == rid => match frame.frame_type {
                    FrameType::Chunk => {
                        if let Some(p) = &frame.payload {
                            let value: ciborium::Value = ciborium::from_reader(&p[..])
                                .expect("response chunk not valid CBOR");
                            match value {
                                ciborium::Value::Bytes(b) => response_data.extend_from_slice(&b),
                                other => panic!("unexpected CBOR: {:?}", other),
                            }
                        }
                    }
                    FrameType::End => break,
                    FrameType::Err => panic!("ERR: {:?}", frame.error_message()),
                    _ => {}
                },
                Ok(Some(_)) => {}
                Ok(None) => {}
                Err(e) => panic!("read error: {}", e),
            }
        }

        assert_eq!(response_data, b"beta");

        drop(switch);
        drop(st_a);
        drop(ht_a);
        drop(st_b);
        drop(ht_b);
    }

    // ============================================================
    // Reattach-by-id tests for the cardinality-stable slot model.
    //
    // The core regression these guard against: when a master dies
    // and the host reconnects, the new socket MUST attach to the
    // same slot index that the dead master held — preserving any
    // routing state keyed by index (cap_table, the request
    // table). The engine has at most a handful of cardinality
    // slots (3 in the website edition: in-process, bundled
    // cartridges, XPC-service); accumulating zombie slots on each
    // reconnect was the bug class that left "Master 0 unhealthy"
    // permanently in the engine logs while masters 3, 4, 5, …
    // appeared as fresh slots holding the actual reconnected
    // gguf cartridge caps.

    /// Reattach-by-id keeps the slot index stable.
    ///
    /// After a master at slot index 0 dies, a new socket added with
    /// the same id MUST be placed into slot 0 (not appended at index 1).
    /// Without this, request-table entries keyed by `master_idx=0`
    /// would dangle pointing at a permanently-unhealthy zombie slot
    /// while the live caps came back at slot 1 — exactly the
    /// observed bug.
    #[tokio::test]
    async fn test0133_reattach_by_id_preserves_slot_index() {
        // Phase 1: build a switch with one slot at id "xpc-service",
        // serviced by a freestanding `slave_notify_with_identity`
        // task. We don't use the full `InProcessCartridgeHost` /
        // `RelaySlave` plumbing here because reattach correctness
        // is a pure RelaySwitch property — the slave just needs to
        // satisfy the handshake.
        let initial_caps = serde_json::json!([
            "cap:effect=none",
            "cap:in=\"media:void\";trivial;out=\"media:void\"",
        ]);
        let (engine_sock_1, slave_sock_1) = UnixStream::pair().unwrap();
        let _slave_task_1 = tokio::spawn({
            let initial_caps = initial_caps.clone();
            async move {
                let _kept_open =
                    slave_notify_with_identity(slave_sock_1, &initial_caps, &Limits::default())
                        .await;
                // Hold the (reader, writer) pair open until the
                // engine marks the slot dead — at which point we
                // drop and the socket closes. The test marks the
                // slot dead via `handle_master_death` directly so
                // this task doesn't actually need to do anything
                // after the handshake.
                std::future::pending::<()>().await;
            }
        });
        let switch = Arc::new(
            RelaySwitch::new(
                vec![("xpc-service".to_string(), engine_sock_1)],
                test_fabric_registry(),
            )
            .await
            .unwrap(),
        );
        assert_eq!(switch.masters.read().await.len(), 1);
        assert_eq!(switch.masters.read().await[0].id, "xpc-service");
        assert!(switch.masters.read().await[0]
            .healthy
            .load(Ordering::SeqCst));

        // Phase 2: simulate master death. handle_master_death is
        // the same code path the frame pump invokes on EOF — using
        // it directly keeps the test focused on the reattach
        // contract. The slot stays in `masters` (size unchanged)
        // but its `healthy` flag flips to false.
        switch.handle_master_death(0).await.unwrap();
        assert!(
            !switch.masters.read().await[0]
                .healthy
                .load(Ordering::SeqCst),
            "slot 0 must be unhealthy after handle_master_death"
        );
        assert_eq!(
            switch.masters.read().await.len(),
            1,
            "handle_master_death must NOT remove the slot — reattach depends on it staying in place"
        );

        // Phase 3: reconnect with the SAME id. The new socket
        // should reattach to slot 0, not append a new slot.
        let (engine_sock_2, slave_sock_2) = UnixStream::pair().unwrap();
        let _slave_task_2 = tokio::spawn({
            let initial_caps = initial_caps.clone();
            async move {
                let _kept_open =
                    slave_notify_with_identity(slave_sock_2, &initial_caps, &Limits::default())
                        .await;
                std::future::pending::<()>().await;
            }
        });

        let new_idx = switch
            .add_master("xpc-service", engine_sock_2)
            .await
            .expect("reattach should succeed");
        assert_eq!(
            new_idx, 0,
            "reattach MUST return the same slot index (0), not append a new slot"
        );
        assert_eq!(
            switch.masters.read().await.len(),
            1,
            "reattach MUST NOT grow the slot count — that was the zombie-slot bug"
        );
        assert!(
            switch.masters.read().await[0]
                .healthy
                .load(Ordering::SeqCst),
            "slot must be healthy after reattach"
        );
        assert_eq!(
            switch.masters.read().await[0].id,
            "xpc-service",
            "slot id MUST be preserved across reattach"
        );

        drop(switch);
    }

    /// Adding a master with an id that matches an already-HEALTHY
    /// slot is a wiring bug — the same master must not be
    /// registered twice. The switch surfaces this as a hard
    /// `Protocol` error rather than silently producing a duplicate
    /// slot.
    #[tokio::test]
    async fn test0134_add_master_with_duplicate_healthy_id_errors() {
        let initial_caps = serde_json::json!([
            "cap:effect=none",
            "cap:in=\"media:void\";trivial;out=\"media:void\"",
        ]);
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();
        let _slave_task = tokio::spawn({
            let initial_caps = initial_caps.clone();
            async move {
                let _kept_open =
                    slave_notify_with_identity(slave_sock, &initial_caps, &Limits::default()).await;
                std::future::pending::<()>().await;
            }
        });
        let switch = Arc::new(
            RelaySwitch::new(
                vec![("xpc-service".to_string(), engine_sock)],
                test_fabric_registry(),
            )
            .await
            .unwrap(),
        );
        assert!(
            switch.masters.read().await[0]
                .healthy
                .load(Ordering::SeqCst),
            "slot 0 must be healthy after construction"
        );

        // Try to add a second master with the same id while the
        // existing slot is still healthy. add_master should error
        // BEFORE doing any I/O — there is no need to wire a real
        // slave on the dummy socket; the dispatcher rejects the
        // call at the cardinality check.
        let (_dummy_a, dummy_b) = UnixStream::pair().unwrap();
        let err = switch
            .add_master("xpc-service", dummy_b)
            .await
            .expect_err("re-adding a healthy id must error");
        let msg = format!("{}", err);
        assert!(
            msg.contains("already attached to a healthy slot"),
            "error message should name the cardinality violation: {}",
            msg
        );
        assert_eq!(
            switch.masters.read().await.len(),
            1,
            "no slot should be created when the duplicate-id check fires"
        );

        drop(switch);
    }

    /// `RelaySwitch::new` rejects duplicate ids in its cardinality
    /// list. Without this guard the first reconnect would
    /// reattach to whichever slot is found first by the linear
    /// scan, leaving the other slot stuck unhealthy forever.
    #[tokio::test]
    async fn test6745_relay_switch_new_rejects_duplicate_ids() {
        let (sock_a, _sock_a_other) = UnixStream::pair().unwrap();
        let (sock_b, _sock_b_other) = UnixStream::pair().unwrap();

        let result = RelaySwitch::new(
            vec![
                ("dup-id".to_string(), sock_a),
                ("dup-id".to_string(), sock_b),
            ],
            test_fabric_registry(),
        )
        .await;

        let err = result.expect_err("duplicate ids must be rejected");
        let msg = format!("{}", err);
        assert!(
            msg.contains("duplicate master id 'dup-id'"),
            "error should name the duplicate id: {}",
            msg
        );
    }

    // TEST666: Preferred cap routing - routes to exact equivalent when multiple masters match
    #[tokio::test]
    async fn test666_preferred_cap_routing() {
        use crate::bifaci::cartridge_runtime::PeerInvoker;
        use crate::bifaci::in_process_host::{
            FrameHandler, InProcessCartridgeHost, ResponseWriter,
        };
        use crate::bifaci::relay::RelaySlave;
        use crate::cap::definition::Cap;
        use async_trait::async_trait;
        use tokio::sync::mpsc;

        /// Handler that returns a marker string identifying itself
        #[derive(Debug)]
        struct MarkerHandler(&'static str);

        #[async_trait]
        impl FrameHandler for MarkerHandler {
            async fn handle_request(
                &self,
                _cap_urn: &str,
                mut input: mpsc::UnboundedReceiver<Frame>,
                output: ResponseWriter,
                _peer: Arc<dyn PeerInvoker>,
            ) {
                // Drain input
                while let Some(frame) = input.recv().await {
                    if frame.frame_type == FrameType::End {
                        break;
                    }
                }
                output.emit_response("media:", self.0.as_bytes());
            }
        }

        // Helper to wire up host + slave
        async fn wire_host(
            host: InProcessCartridgeHost,
        ) -> (
            UnixStream,
            tokio::task::JoinHandle<()>,
            tokio::task::JoinHandle<()>,
        ) {
            let (host_sock, slave_local_sock) = UnixStream::pair().unwrap();
            let (slave_sock, switch_sock) = UnixStream::pair().unwrap();

            let (host_read, host_write) = host_sock.into_split();
            let host_task = tokio::spawn(async move {
                host.run(host_read, host_write).await.unwrap();
            });

            let (slave_local_read, slave_local_write) = slave_local_sock.into_split();
            let slave = RelaySlave::new(slave_local_read, slave_local_write);
            let (slave_read, slave_write) = slave_sock.into_split();
            let slave_task = tokio::spawn(async move {
                let sr = FrameReader::new(BufReader::new(slave_read));
                let sw = FrameWriter::new(BufWriter::new(slave_write));
                slave.run(sr, sw, None).await.unwrap();
            });

            (switch_sock, host_task, slave_task)
        }

        // Master 1: Exact-match handler (matches request exactly — closest specificity)
        let cap_exact = "cap:in=\"media:void\";test;out=\"media:void\"";
        let host_exact = InProcessCartridgeHost::new(
            crate::bifaci::in_process_host::InProcessHostIdentity::for_test("exact-host"),
            vec![(
                "exact".to_string(),
                vec![Cap {
                    version: 1,
                    urn: crate::CapUrn::from_string(cap_exact).unwrap(),
                    title: "exact".to_string(),
                    cap_description: None,
                    documentation: None,
                    metadata: std::collections::HashMap::new(),
                    aliases: vec![String::new()],
                    is_abstract: false,
                    args: Vec::new(),
                    output: None,
                    metadata_json: None,
                    registered_by: None,
                    supported_model_types: Vec::new(),
                    default_model_spec: None,
                }],
                std::sync::Arc::new(MarkerHandler("EXACT")) as std::sync::Arc<dyn FrameHandler>,
            )],
        );

        // Master 2: More-specific handler (has extra tag — also matches, but further from request)
        let cap_extra = "cap:in=\"media:void\";test;ext=pdf;out=\"media:void\"";
        let host_extra = InProcessCartridgeHost::new(
            crate::bifaci::in_process_host::InProcessHostIdentity::for_test("extra-host"),
            vec![(
                "extra".to_string(),
                vec![Cap {
                    version: 1,
                    urn: crate::CapUrn::from_string(cap_extra).unwrap(),
                    title: "extra".to_string(),
                    cap_description: None,
                    documentation: None,
                    metadata: std::collections::HashMap::new(),
                    aliases: vec![String::new()],
                    is_abstract: false,
                    args: Vec::new(),
                    output: None,
                    metadata_json: None,
                    registered_by: None,
                    supported_model_types: Vec::new(),
                    default_model_spec: None,
                }],
                std::sync::Arc::new(MarkerHandler("EXTRA")) as std::sync::Arc<dyn FrameHandler>,
            )],
        );

        let (switch_sock_exact, ht_exact, st_exact) = wire_host(host_exact).await;
        let (switch_sock_extra, ht_extra, st_extra) = wire_host(host_extra).await;

        let switch = RelaySwitch::new(
            wrap_with_test_ids(vec![switch_sock_exact, switch_sock_extra]),
            test_fabric_registry(),
        )
        .await
        .unwrap();
        assert_eq!(switch.masters.read().await.len(), 2);

        // Test 1: Without preferred_cap, routes to exact match (closest specificity)
        let req_cap = "cap:in=\"media:void\";test;out=\"media:void\"";
        let req1 = Frame::req(
            MessageId::Uint(1),
            req_cap,
            Vec::new(),
            "application/octet-stream",
        );

        switch.send_to_master(req1.clone(), None).await.unwrap();
        switch
            .send_to_master(Frame::end(MessageId::Uint(1), None), None)
            .await
            .unwrap();

        let mut response_data1 = Vec::new();
        for _ in 0..10 {
            match switch.read_from_masters().await {
                Ok(Some(frame)) => {
                    match frame.frame_type {
                        FrameType::Chunk => {
                            // Chunk payload is CBOR-encoded bytes — decode it
                            let payload = frame.payload.as_ref().unwrap();
                            let val: ciborium::Value =
                                ciborium::from_reader(payload.as_slice()).unwrap();
                            if let ciborium::Value::Bytes(b) = val {
                                response_data1.extend_from_slice(&b);
                            }
                        }
                        FrameType::End => break,
                        FrameType::Err => panic!("ERR: {:?}", frame.error_message()),
                        _ => {}
                    }
                }
                Ok(None) => break,
                Err(e) => panic!("read error: {}", e),
            }
        }

        // Test 2: With preferred_cap = cap_extra, routes to extra handler (preferred override)
        let req2 = Frame::req(
            MessageId::Uint(2),
            req_cap,
            Vec::new(),
            "application/octet-stream",
        );

        switch
            .send_to_master(req2.clone(), Some(cap_extra))
            .await
            .unwrap();
        switch
            .send_to_master(Frame::end(MessageId::Uint(2), None), None)
            .await
            .unwrap();

        let mut response_data2 = Vec::new();
        for _ in 0..10 {
            match switch.read_from_masters().await {
                Ok(Some(frame)) => match frame.frame_type {
                    FrameType::Chunk => {
                        let payload = frame.payload.as_ref().unwrap();
                        let val: ciborium::Value =
                            ciborium::from_reader(payload.as_slice()).unwrap();
                        if let ciborium::Value::Bytes(b) = val {
                            response_data2.extend_from_slice(&b);
                        }
                    }
                    FrameType::End => break,
                    FrameType::Err => panic!("ERR: {:?}", frame.error_message()),
                    _ => {}
                },
                Ok(None) => break,
                Err(e) => panic!("read error: {}", e),
            }
        }

        // Verify routing: without preference routes to exact match (closest), with preference routes to extra (override)
        assert_eq!(
            response_data1, b"EXACT",
            "Without preferred_cap, should route to exact-match handler (closest specificity)"
        );
        assert_eq!(
            response_data2, b"EXTRA",
            "With preferred_cap, should route to extra handler (preferred override)"
        );

        drop(switch);
        drop(st_exact);
        drop(ht_exact);
        drop(st_extra);
        drop(ht_extra);
    }

    // =========================================================================
    // all_masters_ready / set_expected_master_count
    // =========================================================================
    //
    // The host's `.configuring → .ready` advance is gated on this
    // predicate returning true, so its corner cases matter:
    //
    //   - Returns false when expected count is unset (default 0).
    //     This catches the "engine forgot to declare its expected
    //     count at boot" case — better to hang at .configuring than
    //     to advance prematurely.
    //
    //   - Returns false when only some of the expected masters have
    //     connected. This is the bug we hit live: with the internal
    //     master alone connected (4 caps from t=0), the host saw
    //     ready immediately, before external cartridges had spawned.
    //
    //   - Returns true exactly once the expected count is met AND
    //     every connected master is healthy with non-empty caps.

    /// Helper: build a switch whose constructor reads RelayNotify from
    /// `n` slaves, each registering one cap. Returns the switch ready
    /// for `set_expected_master_count` / `all_masters_ready` calls.
    async fn build_switch_with_n_masters(n: usize) -> Arc<RelaySwitch> {
        let mut engine_socks = Vec::with_capacity(n);
        for i in 0..n {
            let (engine_sock, slave_sock) = UnixStream::pair().unwrap();
            engine_socks.push(engine_sock);
            let cap = format!("cap:in=\"media:t{}\";noop;out=\"media:t{}\"", i, i);
            tokio::spawn(async move {
                slave_notify_with_identity(
                    slave_sock,
                    &serde_json::json!(["cap:effect=none", cap]),
                    &Limits::default(),
                )
                .await;
            });
        }
        Arc::new(
            RelaySwitch::new(wrap_with_test_ids(engine_socks), test_fabric_registry())
                .await
                .unwrap(),
        )
    }

    // TEST0136: All masters ready false when expected count unset
    #[tokio::test]
    async fn test0136_all_masters_ready_false_when_expected_count_unset() {
        // Even with a connected, fully-RelayNotify'd master, the
        // predicate must return false until the engine explicitly
        // declares its expected master count via
        // set_expected_master_count. The default-zero policy is the
        // safety net that makes "engine boot forgot to declare its
        // expected count" surface as a hung readiness gate rather
        // than a false-positive ready signal.
        let switch = build_switch_with_n_masters(1).await;
        assert_eq!(
            switch.all_masters_ready().await,
            false,
            "all_masters_ready must return false when expected_master_count is 0"
        );
    }

    // TEST0137: All masters ready false when partially connected
    #[tokio::test]
    async fn test0137_all_masters_ready_false_when_partially_connected() {
        // 1 master connected, 2 expected. This is the live regression
        // we shipped: the internal master had caps from t=0 but the
        // external-cartridges master was still spawning cartridges.
        // The host saw ready immediately and the bidi never started.
        let switch = build_switch_with_n_masters(1).await;
        switch.set_expected_master_count(2);
        assert_eq!(
            switch.all_masters_ready().await,
            false,
            "all_masters_ready must return false until masters.len() reaches expected_master_count"
        );
    }

    // TEST6746: All masters ready true when expectation met
    #[tokio::test]
    async fn test6746_all_masters_ready_true_when_expectation_met() {
        // 2 masters connected, 2 expected, both healthy with caps —
        // the only state where readiness should fire.
        let switch = build_switch_with_n_masters(2).await;
        switch.set_expected_master_count(2);
        assert_eq!(
            switch.all_masters_ready().await,
            true,
            "all_masters_ready must return true when expected count is met and every master has caps"
        );
    }

    /// Helper: build a switch whose masters connect but RelayNotify
    /// an EMPTY cap set. Mirrors the real-world "cartridges still
    /// inspecting / verifying" state where the XPC master has
    /// connected but no cartridge has reached `Operational` yet.
    async fn build_switch_with_n_capless_masters(n: usize) -> Arc<RelaySwitch> {
        let mut engine_socks = Vec::with_capacity(n);
        for _ in 0..n {
            let (engine_sock, slave_sock) = UnixStream::pair().unwrap();
            engine_socks.push(engine_sock);
            tokio::spawn(async move {
                slave_notify_with_identity(slave_sock, &serde_json::json!([]), &Limits::default())
                    .await;
            });
        }
        Arc::new(
            RelaySwitch::new(wrap_with_test_ids(engine_socks), test_fabric_registry())
                .await
                .unwrap(),
        )
    }

    // TEST0139: All masters ready true when masters connected but capless
    #[tokio::test]
    async fn test0139_all_masters_ready_true_when_masters_connected_but_capless() {
        // Cartridges in `.discovered` / `.inspecting` / `.verifying`
        // contribute zero caps to their master's RelayNotify. The
        // engine readiness gate must still fire so the splash screen
        // can unblock — caps register incrementally as cartridges
        // progress to `.operational`. See
        // `machfab-mac/docs/cartridge state machine.md` and the
        // `all_masters_ready` doc comment for the rationale. A
        // regression that re-coupled readiness to cap-set
        // non-emptiness would make this test fail (and would hang
        // the splash screen on every cold start with slow
        // cartridges).
        let switch = build_switch_with_n_capless_masters(2).await;
        switch.set_expected_master_count(2);
        assert_eq!(
            switch.all_masters_ready().await,
            true,
            "all_masters_ready must NOT require master.caps to be non-empty — caps register asynchronously as cartridges progress to Operational"
        );
    }

    // TEST0140: All masters ready does not overshoot
    #[tokio::test]
    async fn test0140_all_masters_ready_does_not_overshoot() {
        // 2 masters connected, 1 expected. The predicate should
        // still report ready — the engine got more masters than it
        // declared, which is fine; "at least expected" is the
        // semantic. (A regression that used `==` instead of `>=`
        // would make this case false and break edition setups where
        // an extra master arrives later.)
        let switch = build_switch_with_n_masters(2).await;
        switch.set_expected_master_count(1);
        assert_eq!(
            switch.all_masters_ready().await,
            true,
            "all_masters_ready uses >= not == against expected_master_count"
        );
    }

    // ============================================================
    // Wire-format tests for `CartridgeAttachmentErrorKind`
    // ============================================================
    //
    // The kind enum crosses three boundaries:
    //   * RelayNotify JSON over the relay socket (Swift host → engine)
    //   * gRPC enum in cartridge.proto (engine → Mac app)
    //   * NSXPC reply dictionaries (XPC service → Mac app)
    //
    // Every variant's serde rename MUST match its proto snake_case
    // name byte-for-byte. The proto mapping in
    // `machfab/src/grpc/service/cartridge_grpc_service.rs` is
    // exhaustive (no wildcard arm), so a missing variant there
    // would fail to compile — but the JSON wire format is strings
    // and silently accepts new variants. These tests are the only
    // thing that catches a rename / typo on the JSON side before
    // it hits a relay-disconnect-with-bad-payload bug in the wild.

    /// TEST1720: Every variant serializes to the snake_case
    /// string the proto and the Swift / Go / Python ports use.
    /// Adding a new variant requires an entry here AND a matching
    /// CARTRIDGE_ATTACHMENT_ERROR_FOO entry in cartridge.proto;
    /// the test fails with a clear "expected X for Y" message
    /// when the two sides drift.
    #[test]
    fn test1720_kind_serde_renames_match_proto_snake_case() {
        use super::CartridgeAttachmentErrorKind;
        let cases = [
            (CartridgeAttachmentErrorKind::Incompatible, "incompatible"),
            (
                CartridgeAttachmentErrorKind::ManifestInvalid,
                "manifest_invalid",
            ),
            (
                CartridgeAttachmentErrorKind::HandshakeFailed,
                "handshake_failed",
            ),
            (
                CartridgeAttachmentErrorKind::IdentityRejected,
                "identity_rejected",
            ),
            (
                CartridgeAttachmentErrorKind::EntryPointMissing,
                "entry_point_missing",
            ),
            (CartridgeAttachmentErrorKind::Quarantined, "quarantined"),
            (
                CartridgeAttachmentErrorKind::BadInstallation,
                "bad_installation",
            ),
            (CartridgeAttachmentErrorKind::Disabled, "disabled"),
            (
                CartridgeAttachmentErrorKind::RegistryUnreachable,
                "registry_unreachable",
            ),
            (
                CartridgeAttachmentErrorKind::FabricManifestVersionMismatch,
                "fabric_manifest_version_mismatch",
            ),
        ];
        for (variant, expected) in cases {
            let json = serde_json::to_string(&variant).expect("variant must serialize");
            // serde_json emits scalar enums as JSON strings:
            // surrounded by quotes. Strip them for the byte
            // comparison so the test message reads naturally.
            let trimmed = json.trim_matches('"');
            assert_eq!(
                trimmed, expected,
                "variant {:?} must serialize as '{}' to match cartridge.proto's CartridgeAttachmentErrorKind (got '{}')",
                variant, expected, trimmed
            );
        }
    }

    /// TEST1721: Wire-format JSON deserializes into the right
    /// variant. This is the engine-receives-from-XPC path: the
    /// machfab-mac side emits `{"kind":"bad_installation",...}`
    /// and the engine must resolve it to `BadInstallation`.
    /// Asserts every variant explicitly so a single-variant typo
    /// in the rename map can't hide behind a passing healthy-case.
    #[test]
    fn test1721_kind_decodes_wire_format_into_expected_variants() {
        use super::CartridgeAttachmentErrorKind;
        let cases: [(&str, CartridgeAttachmentErrorKind); 10] = [
            ("incompatible", CartridgeAttachmentErrorKind::Incompatible),
            (
                "manifest_invalid",
                CartridgeAttachmentErrorKind::ManifestInvalid,
            ),
            (
                "handshake_failed",
                CartridgeAttachmentErrorKind::HandshakeFailed,
            ),
            (
                "identity_rejected",
                CartridgeAttachmentErrorKind::IdentityRejected,
            ),
            (
                "entry_point_missing",
                CartridgeAttachmentErrorKind::EntryPointMissing,
            ),
            ("quarantined", CartridgeAttachmentErrorKind::Quarantined),
            (
                "bad_installation",
                CartridgeAttachmentErrorKind::BadInstallation,
            ),
            ("disabled", CartridgeAttachmentErrorKind::Disabled),
            (
                "registry_unreachable",
                CartridgeAttachmentErrorKind::RegistryUnreachable,
            ),
            (
                "fabric_manifest_version_mismatch",
                CartridgeAttachmentErrorKind::FabricManifestVersionMismatch,
            ),
        ];
        for (raw, expected_variant) in cases {
            let json = format!("\"{}\"", raw);
            let decoded: CartridgeAttachmentErrorKind = serde_json::from_str(&json)
                .unwrap_or_else(|e| panic!("wire kind '{}' must decode: {}", raw, e));
            assert_eq!(
                decoded, expected_variant,
                "wire kind '{}' must decode to {:?}",
                raw, expected_variant
            );
        }
    }

    /// TEST1730: Every `CartridgeLifecycle` variant serializes to
    /// its proto snake_case name byte-for-byte. Adding a variant
    /// requires an entry here AND a `CARTRIDGE_LIFECYCLE_FOO`
    /// constant in `cartridge.proto`. Cross-language drift on this
    /// enum makes lifecycle states silently invisible to one side
    /// of the wire.
    #[test]
    fn test1730_lifecycle_serde_renames_match_proto_snake_case() {
        use super::CartridgeLifecycle;
        let cases = [
            (CartridgeLifecycle::Discovered, "discovered"),
            (CartridgeLifecycle::Inspecting, "inspecting"),
            (CartridgeLifecycle::Verifying, "verifying"),
            (CartridgeLifecycle::Operational, "operational"),
        ];
        for (variant, expected) in cases {
            let json = serde_json::to_string(&variant).expect("serialize");
            let trimmed = json.trim_matches('"');
            assert_eq!(
                trimmed, expected,
                "lifecycle variant {:?} must serialize as '{}' (got '{}')",
                variant, expected, trimmed
            );
        }
    }

    /// TEST1731: `CartridgeLifecycle` defaults to `Discovered`
    /// (the safe sentinel) — never `Operational`. Pins the
    /// safe-default rule the doc explicitly calls out: a
    /// freshly-constructed record without an explicit lifecycle
    /// MUST NOT silently expose an un-inspected cartridge for
    /// dispatch.
    #[test]
    fn test1731_lifecycle_default_is_discovered() {
        use super::CartridgeLifecycle;
        assert_eq!(
            CartridgeLifecycle::default(),
            CartridgeLifecycle::Discovered,
            "CartridgeLifecycle::default() must be Discovered (safe sentinel), not Operational"
        );
    }

    /// TEST1732: An `InstalledCartridgeRecord` deserialized from a
    /// JSON payload that omits the `lifecycle` field defaults to
    /// `Discovered` — never `Operational`. The wire-shape contract
    /// covered by the safe-default rule.
    #[test]
    fn test1732_installed_cartridge_record_lifecycle_defaults_when_missing() {
        use super::CartridgeLifecycle;
        use crate::bifaci::cartridge_repo::CartridgeChannel;
        let json = r#"{
            "registry_url": null,
            "id": "test",
            "channel": "release",
            "version": "0.0.1",
            "sha256": "deadbeef"
        }"#;
        let record: super::InstalledCartridgeRecord = serde_json::from_str(json).expect("decode");
        assert_eq!(
            record.lifecycle,
            CartridgeLifecycle::Discovered,
            "InstalledCartridgeRecord without `lifecycle` field must default to Discovered, not Operational"
        );
        // Also assert other fields landed correctly so the test
        // exposes a regression that drops more than just lifecycle.
        assert_eq!(record.id, "test");
        assert_eq!(record.channel, CartridgeChannel::Release);
    }

    /// TEST1733: `validate_registry_url_scheme` accepts https
    /// unconditionally, rejects non-https in production builds,
    /// and accepts non-https in dev mode. Pins the deepest layer
    /// of the HTTPS rule.
    #[test]
    fn test1733_registry_url_scheme_validator() {
        use super::super::cartridge_json::{validate_registry_url_scheme, RegistryUrlSchemeResult};
        // https always OK.
        assert_eq!(
            validate_registry_url_scheme("https://example.com/manifest", false),
            RegistryUrlSchemeResult::Ok
        );
        assert_eq!(
            validate_registry_url_scheme("https://example.com/manifest", true),
            RegistryUrlSchemeResult::Ok
        );
        // http rejected in production, accepted in dev.
        assert_eq!(
            validate_registry_url_scheme("http://localhost:8080/manifest", false),
            RegistryUrlSchemeResult::NonHttps {
                scheme: "http".to_string()
            }
        );
        assert_eq!(
            validate_registry_url_scheme("http://localhost:8080/manifest", true),
            RegistryUrlSchemeResult::Ok
        );
        // Malformed URL is always NotAUrl, regardless of mode.
        assert_eq!(
            validate_registry_url_scheme("not a url", false),
            RegistryUrlSchemeResult::NotAUrl("not a url".to_string())
        );
        assert_eq!(
            validate_registry_url_scheme("not a url", true),
            RegistryUrlSchemeResult::NotAUrl("not a url".to_string())
        );
        // Empty rest after `://` is also NotAUrl.
        assert_eq!(
            validate_registry_url_scheme("https://", false),
            RegistryUrlSchemeResult::NotAUrl("https://".to_string())
        );
        // Case-insensitive on the scheme — `HTTPS` is still
        // https. RFC 3986 says schemes are case-insensitive.
        assert_eq!(
            validate_registry_url_scheme("HTTPS://example.com", false),
            RegistryUrlSchemeResult::Ok
        );
    }

    /// TEST1722: An unknown wire kind FAILS to decode rather than
    /// silently coercing to a default variant. Older capdag binaries
    /// that don't know `bad_installation` or `disabled` will see
    /// those strings on the wire from a newer Swift side; rejecting
    /// the unknown variant is the correct behaviour because silently
    /// coercing it would hide the version-skew bug. The engine's
    /// per-master JSON parse failure path is what surfaces this to
    /// the operator (the master's manifest fails to parse and the
    /// master is held unhealthy until the version is patched).
    #[test]
    fn test1722_unknown_kind_fails_to_decode() {
        use super::CartridgeAttachmentErrorKind;
        let json = "\"completely_made_up_kind\"";
        let result: Result<CartridgeAttachmentErrorKind, _> = serde_json::from_str(json);
        assert!(
            result.is_err(),
            "unknown wire kind must error rather than silently coerce; got: {:?}",
            result
        );
    }
    // TEST7085: The RelayNotify capabilities payload carries the host's protocol stats snapshot, surviving the wire round-trip.
    #[test]
    fn test7085_relay_notify_carries_host_protocol_stats() {
        let stats = crate::bifaci::host_runtime::HostProtocolStats {
            drops: {
                let counters = crate::bifaci::stats::DropCounters::new();
                counters.record(crate::bifaci::frame::DropReason::NoRoute, FrameType::Chunk);
                counters.record(crate::bifaci::frame::DropReason::NoRoute, FrameType::Credit);
                counters.snapshot()
            },
            stragglers: {
                let counters = crate::bifaci::stats::StragglerCounters::new();
                counters.record(FrameType::Credit);
                counters.snapshot()
            },
            outgoing_rids: 3,
            incoming_rxids: 5,
            incoming_to_peer_rids: 1,
            outgoing_max_seq: 4,
            routing_gc_runs_total: 2,
            routing_gc_evicted_total: 7,
        };
        let payload = RelayNotifyCapabilitiesPayload::new(vec![]).with_host_protocol_stats(stats);
        let bytes = serde_json::to_vec(&payload).unwrap();

        let parsed = parse_relay_notify_payload(&bytes).expect("payload must parse");
        let got = parsed
            .host_protocol_stats
            .expect("host stats must survive the round trip");
        assert_eq!(got.drops.total, 2);
        assert_eq!(got.drops.by_reason.get("no_route"), Some(&2));
        assert_eq!(
            got.drops
                .by_reason_frame_type
                .get("no_route")
                .and_then(|m| m.get("credit")),
            Some(&1),
            "the per-frame-type breakdown survives the wire round-trip"
        );
        assert_eq!(got.stragglers.total, 1);
        assert_eq!(
            got.stragglers.by_frame_type.get("credit"),
            Some(&1),
            "the benign straggler counters survive the wire round-trip"
        );
        assert_eq!(got.incoming_rxids, 5);
        assert_eq!(got.routing_gc_evicted_total, 7);

        // A payload WITHOUT stats (initial capability advertisement) still
        // parses — the field is a per-republish refresh, not a requirement.
        let bare = RelayNotifyCapabilitiesPayload::new(vec![]);
        let bytes = serde_json::to_vec(&bare).unwrap();
        let parsed = parse_relay_notify_payload(&bytes).expect("bare payload parses");
        assert!(parsed.host_protocol_stats.is_none());
    }

    // TEST7093: A response frame for a LIVE request whose external consumer is
    // gone (dropped/timed-out caller future) is a counted channel_closed drop
    // AND cancels the request upstream — the destination receives Cancel, the
    // entry terminates as cancelled, and the cartridge stops producing for a
    // dead channel instead of running to completion against it.
    #[tokio::test]
    async fn test7093_dead_consumer_cancels_upstream() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        let slave = tokio::spawn(async move {
            let (mut reader, mut writer) = slave_notify_with_identity(
                slave_sock,
                &serde_json::json!(["cap:effect=none"]),
                &Limits::default(),
            )
            .await;

            // Serve one REQ: read it, then stream a response frame. The
            // engine-side consumer will already be gone — the switch must
            // answer with Cancel on this connection.
            let req = loop {
                let f = reader.read().await.unwrap().expect("expected REQ");
                if f.frame_type == FrameType::Req {
                    break f;
                }
            };
            let mut log = Frame::log(
                req.id.clone(),
                "info",
                crate::AttributionClass::Internal,
                "first result row",
                None,
            );
            log.routing_id = req.routing_id.clone();
            writer.write(&log).await.unwrap();

            // The switch must now cancel this request (dead consumer).
            loop {
                let f = reader.read().await.unwrap().expect("expected Cancel");
                if f.frame_type == FrameType::Cancel {
                    assert_eq!(f.id, req.id, "cancel targets the abandoned request");
                    return;
                }
            }
        });

        let switch = Arc::new(
            RelaySwitch::new(
                wrap_with_test_ids(vec![engine_sock]),
                test_fabric_registry(),
            )
            .await
            .unwrap(),
        );
        switch.start_background_pump();

        let (rid, rx) = switch
            .execute_cap("cap:effect=none", vec![], "application/cbor")
            .await
            .expect("execute_cap");
        // The caller abandons the request (timed-out/cancelled future).
        drop(rx);
        let _ = rid;

        // The slave streams a frame into the dead channel; the switch must
        // count the drop and cancel upstream (the slave task asserts Cancel).
        tokio::time::timeout(std::time::Duration::from_secs(5), slave)
            .await
            .expect("slave must observe Cancel before timeout")
            .expect("slave task must not panic");

        let stats = switch.protocol_stats().await;
        assert_eq!(
            stats.drops.by_reason.get("channel_closed"),
            Some(&1),
            "the abandoned frame is a counted channel_closed drop"
        );
        assert_eq!(
            stats.requests.terminated_by_kind.get("cancelled"),
            Some(&1),
            "the abandoned request terminates as cancelled — it never lingers"
        );
        assert!(
            stats.requests.active.is_empty(),
            "no state remains for the abandoned request (L7)"
        );
    }

    // TEST7091: Host protocol stats carried by a master's RelayNotify are
    // RETAINED by the switch (not parsed-and-discarded) and surface in
    // `protocol_stats().hosts` keyed by master id; a master that has not yet
    // advertised stats is absent from the map — never a zeroed placeholder.
    #[tokio::test]
    async fn test7091_switch_retains_host_protocol_stats_from_relay_notify() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        let slave = tokio::spawn(async move {
            let (reader, mut writer) = slave_notify_with_identity(
                slave_sock,
                &serde_json::json!(["cap:effect=none"]),
                &Limits::default(),
            )
            .await;

            // Republish the SAME inventory (no cap change → no re-verify),
            // now carrying host protocol stats — the periodic refresh path.
            let notify_payload = serde_json::json!({
                "installed_cartridges": [
                    {
                        "registry_url": null,
                        "channel": "release",
                        "id": "test-cartridge",
                        "version": "0.0.0",
                        "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                    "runtime_stats": {
                        "running": true,
                        "pools": test_pool_states_json(&["cap:effect=none"], 0, 0),
                        "active_request_count": 0,
                        "peer_request_count": 0,
                        "memory_footprint_mb": 0,
                        "memory_rss_mb": 0,
                        "restart_count": 0
                    },
                        "cap_groups": [
                            {
                                "name": "test",
                                "caps": [
                                    { "urn": "cap:effect=none", "title": "test", "aliases": ["test"], "args": [] }
                                ],
                                "adapter_urns": [],
                            }
                        ],
                    }
                ],
                "host_protocol_stats": {
                    "drops": { "total": 1, "by_reason": { "no_route": 1 } },
                    "stragglers": { "total": 2, "by_frame_type": { "credit": 2 } },
                    "outgoing_rids": 4,
                    "incoming_rxids": 6,
                    "incoming_to_peer_rids": 0,
                    "outgoing_max_seq": 2,
                    "routing_gc_runs_total": 1,
                    "routing_gc_evicted_total": 9,
                },
            });
            writer
                .write(&Frame::relay_notify(
                    &serde_json::to_vec(&notify_payload).unwrap(),
                    &Limits::default(),
                ))
                .await
                .unwrap();
            // Keep the connection open until the assertion side finishes.
            (reader, writer)
        });

        let switch = Arc::new(
            RelaySwitch::new(
                wrap_with_test_ids(vec![engine_sock]),
                test_fabric_registry(),
            )
            .await
            .unwrap(),
        );

        // The initial advertisement carried no host stats: absent, not zeroed.
        assert!(
            switch.protocol_stats().await.hosts.is_empty(),
            "no host stats before a RelayNotify carries them"
        );

        switch.start_background_pump();

        let stats = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let stats = switch.protocol_stats().await;
                if let Some(host) = stats.hosts.get("test-master-0") {
                    return host.clone();
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("host stats must surface in protocol_stats().hosts after RelayNotify");

        assert_eq!(stats.drops.total, 1);
        assert_eq!(stats.drops.by_reason.get("no_route"), Some(&1));
        assert_eq!(
            stats.stragglers.by_frame_type.get("credit"),
            Some(&2),
            "benign stragglers surface separately from drops"
        );
        assert_eq!(stats.incoming_rxids, 6);
        assert_eq!(stats.routing_gc_evicted_total, 9);
        drop(slave);
    }

    // TEST7025: A flow frame for a request with no routing state is a counted no_route drop — not a protocol error and not a silent loss — observable in the protocol stats snapshot.
    #[tokio::test]
    async fn test7025_unroutable_flow_frame_is_counted_drop() {
        let registry = test_fabric_registry();
        let switch = RelaySwitch::new(wrap_with_test_ids(vec![]), registry)
            .await
            .expect("empty relay switch must construct");

        // Response continuation (has XID) for a key that was never registered
        // (or already terminated): must be dropped + counted, never an error.
        let mut orphan = Frame::progress(MessageId::new_uuid(), 0.5, "orphan");
        orphan.routing_id = Some(MessageId::Uint(999));
        let result = switch
            .handle_master_frame(0, orphan)
            .await
            .expect("unroutable frame must not surface as an error (L6)");
        assert!(result.is_none(), "nothing to deliver");

        // Request continuation (no XID) for an unknown RID: same law.
        let mut chunk = Frame::new(FrameType::Chunk, MessageId::new_uuid());
        chunk.stream_id = Some("s".to_string());
        chunk.chunk_index = Some(0);
        chunk.checksum = Some(0);
        let result = switch
            .handle_master_frame(0, chunk)
            .await
            .expect("unknown request continuation must not error");
        assert!(result.is_none());

        let stats = switch.protocol_stats().await;
        assert_eq!(
            stats.drops.by_reason.get("no_route"),
            Some(&2),
            "both drops counted, exactly once each (L8): {:?}",
            stats.drops
        );
        assert!(stats.requests.active.is_empty());
    }

    // TEST7035: After END, the switch holds zero state for the request — entry, rid index, and response channel all released atomically, with the terminal delivered and a terminated summary recorded.
    #[tokio::test]
    async fn test7035_end_terminates_and_releases_all_state() {
        let registry = test_fabric_registry();
        let switch = RelaySwitch::new(wrap_with_test_ids(vec![]), registry)
            .await
            .expect("empty relay switch must construct");

        let xid = MessageId::Uint(11);
        let rid = MessageId::new_uuid();
        let key = (xid.clone(), rid.clone());
        let (tx, mut rx) = mpsc::unbounded_channel();
        switch
            .requests
            .write()
            .await
            .register(
                key.clone(),
                RequestState::new(
                    RoutingEntry {
                        source_master_idx: None,
                        destination_master_idx: 0,
                    },
                    None,
                    Some(tx),
                    false,
                    crate::bifaci::frame::DEFAULT_INITIAL_CREDIT,
                ),
            )
            .expect("registration must succeed");
        assert_eq!(switch.protocol_stats().await.requests.active.len(), 1);

        // Terminal END arrives from the master side.
        let mut end = Frame::end_ok_with(rid.clone(), None, Some(1.0), None);
        end.routing_id = Some(xid.clone());
        switch
            .handle_master_frame(0, end)
            .await
            .expect("terminal must route");

        // The terminal was DELIVERED to the waiting channel...
        let delivered = rx
            .recv()
            .await
            .expect("END must reach the response channel");
        assert_eq!(delivered.frame_type, FrameType::End);
        assert_eq!(delivered.final_progress(), Some(1.0));

        // ...and zero state remains (L7), with the lifecycle recorded.
        let stats = switch.protocol_stats().await;
        assert!(stats.requests.active.is_empty(), "no live entry after END");
        assert_eq!(stats.requests.terminated_by_kind.get("end"), Some(&1));
        let summary = stats
            .requests
            .recent_terminated
            .last()
            .expect("terminated summary must be recorded");
        assert_eq!(summary.rid, rid.to_string());
        assert_eq!(
            summary.frames_in, 1,
            "ingress recording captured the terminal frame"
        );

        // A follow-up frame for the released key is a BENIGN post-terminal
        // straggler — the expected teardown race, counted separately and
        // never as a drop; no_route stays reserved for RIDs the table never
        // knew (TEST8114).
        let mut late = Frame::progress(rid.clone(), 1.0, "late");
        late.routing_id = Some(xid);
        switch
            .handle_master_frame(0, late)
            .await
            .expect("post-terminal straggler must not error");
        let stats = switch.protocol_stats().await;
        assert_eq!(stats.stragglers.by_frame_type.get("log"), Some(&1));
        assert_eq!(
            stats.drops.total, 0,
            "a just-terminated request's straggler is benign — never a drop"
        );
    }

    // TEST7036: After ERR, the same total-cleanup invariant holds as after END, with kind err.
    #[tokio::test]
    async fn test7036_err_terminates_and_releases_all_state() {
        let registry = test_fabric_registry();
        let switch = RelaySwitch::new(wrap_with_test_ids(vec![]), registry)
            .await
            .expect("empty relay switch must construct");

        let xid = MessageId::Uint(21);
        let rid = MessageId::new_uuid();
        let (tx, mut rx) = mpsc::unbounded_channel();
        switch
            .requests
            .write()
            .await
            .register(
                (xid.clone(), rid.clone()),
                RequestState::new(
                    RoutingEntry {
                        source_master_idx: None,
                        destination_master_idx: 0,
                    },
                    None,
                    Some(tx),
                    false,
                    crate::bifaci::frame::DEFAULT_INITIAL_CREDIT,
                ),
            )
            .unwrap();

        let mut err = Frame::err(
            rid.clone(),
            "HANDLER_ERROR",
            crate::AttributionClass::Internal,
            "boom",
            None,
        );
        err.routing_id = Some(xid);
        switch.handle_master_frame(0, err).await.unwrap();

        let delivered = rx.recv().await.expect("ERR must reach the channel");
        assert_eq!(delivered.frame_type, FrameType::Err);
        assert_eq!(delivered.error_code(), Some("HANDLER_ERROR"));

        let stats = switch.protocol_stats().await;
        assert!(stats.requests.active.is_empty());
        assert_eq!(stats.requests.terminated_by_kind.get("err"), Some(&1));
    }

    // TEST7037: Cancelling a request terminates it AND its recursively-linked peer children — Cancel frames reach the destination, waiting channels get ERR CANCELLED, and zero state remains for parent or child.
    #[tokio::test]
    async fn test7037_cancel_cascades_to_children_and_cleans_all_state() {
        let (engine_socket, slave_socket) = UnixStream::pair().expect("socket pair");

        let limits = Limits::default();
        let caps = serde_json::json!([CAP_IDENTITY]);
        let slave_task = tokio::spawn(async move {
            let (mut reader, _writer) =
                slave_notify_with_identity(slave_socket, &caps, &limits).await;
            // Collect the Cancel frames the cascade sends us.
            let mut cancels = Vec::new();
            while cancels.len() < 2 {
                match reader.read().await {
                    Ok(Some(f)) if f.frame_type == FrameType::Cancel => cancels.push(f.id),
                    Ok(Some(_)) => {}
                    _ => break,
                }
            }
            cancels
        });

        let switch = RelaySwitch::new(
            wrap_with_test_ids(vec![engine_socket]),
            test_fabric_registry(),
        )
        .await
        .expect("switch with one master must construct");

        // Parent (engine-origin, has a waiting channel) + child peer call.
        let parent_key = (MessageId::Uint(1), MessageId::new_uuid());
        let child_key = (MessageId::Uint(2), MessageId::new_uuid());
        let (ptx, mut prx) = mpsc::unbounded_channel();
        {
            let mut requests = switch.requests.write().await;
            requests
                .register(
                    parent_key.clone(),
                    RequestState::new(
                        RoutingEntry {
                            source_master_idx: None,
                            destination_master_idx: 0,
                        },
                        None,
                        Some(ptx),
                        false,
                    crate::bifaci::frame::DEFAULT_INITIAL_CREDIT,
                    ),
                )
                .unwrap();
            requests
                .register(
                    child_key.clone(),
                    RequestState::new(
                        RoutingEntry {
                            source_master_idx: Some(0),
                            destination_master_idx: 0,
                        },
                        Some(0),
                        None,
                        true,
                    crate::bifaci::frame::DEFAULT_INITIAL_CREDIT,
                    ),
                )
                .unwrap();
            requests.link_child(&parent_key, child_key.clone());
        }

        switch.cancel_request(&parent_key.1, false).await;

        // Parent's waiter observes ERR CANCELLED.
        let delivered = prx.recv().await.expect("parent channel gets ERR");
        assert_eq!(delivered.error_code(), Some("CANCELLED"));

        // Both parent and child are fully released (L7), recorded cancelled.
        let stats = switch.protocol_stats().await;
        assert!(
            stats.requests.active.is_empty(),
            "no state for parent or child remains: {:?}",
            stats.requests.active
        );
        assert_eq!(stats.requests.terminated_by_kind.get("cancelled"), Some(&2));

        // The destination master received Cancel for BOTH rids.
        let cancels = slave_task.await.expect("slave task");
        assert_eq!(cancels.len(), 2, "parent + cascaded child Cancel frames");
        assert!(cancels.contains(&parent_key.1));
        assert!(cancels.contains(&child_key.1));
    }

    // TEST7038: Master death terminates every request routed to it with kind master_died, delivering synthetic MASTER_DIED ERRs to waiting channels and leaving zero state.
    #[tokio::test]
    async fn test7038_master_death_terminates_pending_requests() {
        let (engine_socket, slave_socket) = UnixStream::pair().expect("socket pair");

        let limits = Limits::default();
        let caps = serde_json::json!([CAP_IDENTITY]);
        let slave = tokio::spawn(async move {
            // Keep the connection alive until the test drops it.
            slave_notify_with_identity(slave_socket, &caps, &limits).await
        });

        let switch = RelaySwitch::new(
            wrap_with_test_ids(vec![engine_socket]),
            test_fabric_registry(),
        )
        .await
        .expect("switch with one master must construct");
        let _halves = slave.await.expect("slave handshake");

        let key = (MessageId::Uint(5), MessageId::new_uuid());
        let (tx, mut rx) = mpsc::unbounded_channel();
        switch
            .requests
            .write()
            .await
            .register(
                key.clone(),
                RequestState::new(
                    RoutingEntry {
                        source_master_idx: None,
                        destination_master_idx: 0,
                    },
                    None,
                    Some(tx),
                    false,
                    crate::bifaci::frame::DEFAULT_INITIAL_CREDIT,
                ),
            )
            .unwrap();

        switch
            .handle_master_death(0)
            .await
            .expect("death handling must succeed");

        let delivered = rx.recv().await.expect("synthetic ERR must be delivered");
        assert_eq!(delivered.error_code(), Some("MASTER_DIED"));

        let stats = switch.protocol_stats().await;
        assert!(stats.requests.active.is_empty(), "zero state remains (L7)");
        assert_eq!(
            stats.requests.terminated_by_kind.get("master_died"),
            Some(&1)
        );
        let summary = stats.requests.recent_terminated.last().unwrap();
        assert_eq!(summary.rid, key.1.to_string());
    }

    // TEST8114: A flow frame that CROSSED its request's terminal in flight is
    // a BENIGN straggler — the ordinary teardown race of credit-based flow
    // control must not pollute the routing-anomaly alarm, nor the drop
    // counters at all: the crossing is counted as a straggler, named by
    // frame type. A frame for a RID the table never knew stays a no_route
    // DROP (TEST7025).
// TEST1532: the cold-start canary — a NOT-RUNNING record's `all` pool is
    // clamped to capacity 1 regardless of what the pool map claims, so the
    // first dispatch to a cold cartridge is a single canary body and a
    // spawn-time failure costs one body, not a fleet. Running records keep
    // their advertised effective capacities, and an EMPTY pool map on an
    // operational record is a protocol error, never a free pass.
    #[test]
    fn test1532_not_running_canary_clamps_all_to_one() {
        let mut stats = CartridgeRuntimeStats::not_running();
        stats.pools = crate::bifaci::pools::decode_pool_states(
            test_pool_states_json(&["cap:effect=none"], 8, 0)
                .to_string()
                .as_bytes(),
        )
        .expect("test pool map must decode");

        let capacities = RelaySwitch::pool_capacities(&stats, "coldcart")
            .expect("a populated map must configure");
        let all = capacities
            .iter()
            .find(|(name, _)| name == crate::bifaci::pools::POOL_ALL)
            .expect("the mandatory 'all' pool");
        assert_eq!(all.1, 1, "cold start admits exactly one canary body");

        stats.running = true;
        let capacities = RelaySwitch::pool_capacities(&stats, "warmcart")
            .expect("a populated map must configure");
        let all = capacities
            .iter()
            .find(|(name, _)| name == crate::bifaci::pools::POOL_ALL)
            .expect("the mandatory 'all' pool");
        assert_eq!(all.1, 8, "a running record keeps its advertised capacity");

        stats.pools.clear();
        let error = RelaySwitch::pool_capacities(&stats, "emptycart")
            .expect_err("an empty pool map must refuse, never default");
        assert!(
            format!("{error}").contains("emptycart"),
            "the refusal must name the cartridge: {error}"
        );
    }

        // TEST1515: one INCOMPATIBLE install on a master's roster must not cost
    // the master its admission. An incompatible record (attachment_error set)
    // was never spawned, so it legitimately carries no runtime stats — it is
    // inventory the UI shows, not a handler anything routes to. Before this
    // rule, a single unparseable cartridge.json on disk refused the ENTIRE
    // daemon relay ("missing mandatory v4 runtime_stats", retried forever),
    // taking every healthy cartridge down with it. The mandatory-stats rule
    // still holds where it means something: an OPERATIONAL record without
    // stats refuses the master.
    #[tokio::test]
    async fn test1515_incompatible_roster_record_does_not_refuse_the_master() {
        let registry = test_fabric_registry();
        let switch = RelaySwitch::new(wrap_with_test_ids(vec![]), registry)
            .await
            .expect("empty relay switch must construct");

        let healthy = InstalledCartridgeRecord {
            registry_url: None,
            channel: crate::bifaci::cartridge_repo::CartridgeChannel::Nightly,
            id: "healthycart".to_string(),
            version: "1.0.0".to_string(),
            sha256: "aa".repeat(32),
            cap_groups: Vec::new(),
            attachment_error: None,
            runtime_stats: Some(CartridgeRuntimeStats {
                running: true,
                pools: crate::bifaci::pools::decode_pool_states(
                    test_pool_states_json(&[], 2, 0).to_string().as_bytes(),
                )
                .expect("test pool map must decode"),
                ..CartridgeRuntimeStats::not_running()
            }),
            lifecycle: CartridgeLifecycle::Operational,
        };
        let incompatible = InstalledCartridgeRecord {
            registry_url: Some("https://cartridges-staging.machinefabric.com/v1/manifest".to_string()),
            channel: crate::bifaci::cartridge_repo::CartridgeChannel::Nightly,
            id: "brokencart".to_string(),
            version: "1.2.3".to_string(),
            sha256: "bb".repeat(32),
            cap_groups: Vec::new(),
            attachment_error: Some(CartridgeAttachmentError {
                kind: CartridgeAttachmentErrorKind::ManifestInvalid,
                message: "invalid cartridge.json: unknown variant `x`".to_string(),
                detected_at_unix_seconds: 1_780_000_000,
            }),
            runtime_stats: None,
            lifecycle: CartridgeLifecycle::Discovered,
        };

        switch
            .configure_master_admission(0, &[healthy.clone(), incompatible])
            .expect("an incompatible record is inventory, not an admission failure");

        // The mandatory-stats rule is untouched where it is meaningful: an
        // operational record with no stats is a real protocol violation.
        let stats_less_operational = InstalledCartridgeRecord {
            runtime_stats: None,
            ..healthy
        };
        let err = switch
            .configure_master_admission(0, &[stats_less_operational])
            .expect_err("an operational record must carry v4 runtime stats");
        assert!(
            err.to_string().contains("runtime_stats"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test8114_straggler_for_terminated_request_is_benign_not_a_drop() {
        let registry = test_fabric_registry();
        let switch = RelaySwitch::new(wrap_with_test_ids(vec![]), registry)
            .await
            .expect("empty relay switch must construct");

        let xid = MessageId::Uint(21);
        let rid = MessageId::new_uuid();
        let key = (xid.clone(), rid.clone());
        let (tx, mut rx) = mpsc::unbounded_channel();
        switch
            .requests
            .write()
            .await
            .register(
                key.clone(),
                RequestState::new(
                    RoutingEntry {
                        source_master_idx: None,
                        destination_master_idx: 0,
                    },
                    None,
                    Some(tx),
                    false,
                    crate::bifaci::frame::DEFAULT_INITIAL_CREDIT,
                ),
            )
            .expect("registration must succeed");

        let mut end = Frame::end_ok_with(rid.clone(), None, Some(1.0), None);
        end.routing_id = Some(xid.clone());
        switch
            .handle_master_frame(0, end)
            .await
            .expect("terminal must route");
        assert_eq!(
            rx.recv().await.expect("END delivered").frame_type,
            FrameType::End
        );

        // A response continuation (has XID) that raced the END.
        let mut late = Frame::progress(rid.clone(), 0.9, "late");
        late.routing_id = Some(xid.clone());
        switch
            .handle_master_frame(0, late)
            .await
            .expect("post-terminal straggler must not error (L6)");

        // A request continuation (no XID) for the same terminated RID.
        let mut chunk = Frame::new(FrameType::Chunk, rid.clone());
        chunk.stream_id = Some("s".to_string());
        chunk.chunk_index = Some(0);
        chunk.checksum = Some(0);
        switch
            .handle_master_frame(0, chunk)
            .await
            .expect("post-terminal continuation must not error");

        // A duplicate terminal for the released request.
        let mut dup_end = Frame::end_ok_with(rid.clone(), None, Some(1.0), None);
        dup_end.routing_id = Some(xid.clone());
        switch
            .handle_master_frame(0, dup_end)
            .await
            .expect("duplicate terminal must not error");

        let stats = switch.protocol_stats().await;
        assert_eq!(
            stats.stragglers.total, 3,
            "all three post-terminal frames counted as benign stragglers: {:?}",
            stats.stragglers
        );
        assert_eq!(
            stats.stragglers.by_frame_type.get("log"),
            Some(&1),
            "the late progress LOG is named by frame type"
        );
        assert_eq!(
            stats.stragglers.by_frame_type.get("chunk"),
            Some(&1),
            "the late request continuation CHUNK is named by frame type"
        );
        assert_eq!(
            stats.stragglers.by_frame_type.get("end"),
            Some(&1),
            "the duplicate terminal END is named by frame type"
        );
        assert_eq!(
            stats.drops.total, 0,
            "a terminated request's stragglers are benign — never drops, never routing anomalies: {:?}",
            stats.drops
        );
    }

    // TEST8118: the flow ledger is a WINDOW — seeded with the request's
    // negotiated initial credit, consumed by chunks, replenished by grants —
    // and engine-originated frames sent through send_to_master are recorded
    // as its outbound half. Before this, engine grants and chunks bypassed
    // the ledger entirely, so every healthy stream snapshot read as negative
    // by exactly the un-seeded initial window.
    #[tokio::test]
    async fn test8118_send_to_master_records_outbound_flow_and_window() {
        let registry = test_fabric_registry();
        let switch = RelaySwitch::new(wrap_with_test_ids(vec![]), registry)
            .await
            .expect("empty relay switch must construct");

        let xid = MessageId::Uint(31);
        let rid = MessageId::Uint(310);
        let key = (xid.clone(), rid.clone());
        switch
            .requests
            .write()
            .await
            .register(
                key.clone(),
                RequestState::new(
                    RoutingEntry {
                        source_master_idx: None,
                        destination_master_idx: 0,
                    },
                    None,
                    None,
                    false,
                    5, // negotiated window under test — odd-sized so seed arithmetic is visible
                ),
            )
            .expect("registration must succeed");

        // An outbound chunk and an outbound grant from the engine side. There
        // is no master 0 attached, so delivery fails — but the ledger records
        // BEFORE the write, exactly like the inbound path records before
        // routing, so stats never depend on delivery succeeding.
        let payload = vec![0u8; 7];
        let checksum = Frame::compute_checksum(&payload);
        let mut chunk = Frame::chunk(rid.clone(), "s".to_string(), 0, payload, 0, checksum);
        chunk.routing_id = Some(xid.clone());
        let _ = switch.send_to_master(chunk, None).await;

        let mut credit = Frame::credit(
            rid.clone(),
            Some("s".to_string()),
            3,
            crate::bifaci::frame::CreditDirection::Response,
        );
        credit.routing_id = Some(xid.clone());
        let _ = switch.send_to_master(credit, None).await;

        let stats = switch.protocol_stats().await;
        let req = stats
            .requests
            .active
            .iter()
            .find(|r| r.rid == rid.to_string())
            .expect("request must still be active");
        let stream = req
            .streams
            .iter()
            .find(|s| s.stream_id == Some("s".to_string()))
            .expect("stream ledger must exist for the outbound flow");
        assert_eq!(stream.stats.chunks_out, 1, "outbound chunk recorded");
        assert_eq!(stream.stats.bytes_out, 7, "outbound bytes recorded");
        assert_eq!(
            stream.stats.credit_outstanding,
            5 - 1 + 3,
            "window = seed - chunks + grants, never a bare running delta"
        );
    }
}

