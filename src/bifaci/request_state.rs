//! Unified per-request state for routing runtimes (protocol v4, L7/L8).
//!
//! One `RequestState` per in-flight request replaces the parallel routing maps
//! (routing entry, origin, peer markers, parent→child links, response channel,
//! rid→xid index) that previously had to be mutated consistently by hand.
//! Registration and termination are single operations: a request is registered
//! once and terminated once (End | Err | Cancelled | MasterDied); after
//! `terminate` returns, zero state for the key remains (L7).
//!
//! The table is also the observability substrate: per-stream flow counters,
//! phase tracking, and a bounded ring of recently-terminated summaries feed the
//! protocol stats snapshots (L8) without retaining routing state.

use crate::bifaci::frame::{CancelReason, Frame, FrameType, MessageId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::mpsc;

/// (XID, RID) — the unique key of a routed request.
pub type RequestKey = (MessageId, MessageId);

/// Stable admission identity for one cartridge behind one relay master.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AdmissionKey {
    pub master_idx: usize,
    pub registry_url: Option<String>,
    pub channel: String,
    pub id: String,
    pub version: String,
    pub sha256: String,
}

/// How long a queued request waits for an admission target that has gone
/// unavailable before it is failed.
///
/// A cartridge disappearing from its host's inventory is not, by itself, a
/// reason to fail work that has not started: the process may be respawning, the
/// host may be re-publishing its roster, or a transient registry outage may have
/// briefly retired and then restored the install. 17.2 requires that queued
/// bodies are NOT assigned terminal failure from another body's process loss and
/// that "once a replacement instance advertises capacity, subsequent queued work
/// is admitted to that live instance" — this window is how long we hold the
/// queue open for that replacement to appear.
///
/// It is a bound, not a retry: when it expires the wait fails hard and the
/// failure is classified `environment`, so a target that is genuinely gone
/// surfaces promptly instead of hanging the run.
pub const ADMISSION_UNAVAILABLE_GRACE: std::time::Duration = std::time::Duration::from_secs(60);

/// One pool of one cartridge install behind one master — the unit permits
/// are held against (see `bifaci::pools`: a cap is a pool of one, `all` is
/// the pool of every cap, and a dispatch is admitted through its cap's
/// whole pool CHAIN).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PoolKey {
    pub install: AdmissionKey,
    pub pool: String,
}

#[derive(Debug, Default)]
struct PoolSlot {
    /// EFFECTIVE capacity (min of configured/available, 0 = unlimited),
    /// as advertised by the roster.
    capacity: usize,
    active: usize,
    /// FIFO tickets. Populated only on SINGLETON pool keys (a request
    /// queues on its cap's own pool); shared pools hold no queue — their
    /// waiters are the union of member singleton queues.
    queue: VecDeque<u64>,
}

impl PoolSlot {
    fn has_room(&self) -> bool {
        self.capacity == 0 || self.active < self.capacity
    }
}

/// Install-level availability. Outages are a PROCESS fact (a respawn, a
/// roster republish), so they are tracked per install and inherited by
/// every pool of that install.
#[derive(Debug, Default)]
struct InstallState {
    /// `None` while the target is available; `Some(since)` from the moment it
    /// went unavailable. Kept as an instant rather than a bool so the grace
    /// window measures the OUTAGE, not the arrival time of each waiter — a
    /// request that queues late into an outage does not get a fresh window.
    /// `tokio::time::Instant` so the window is on the same clock as the timeout
    /// that waits it out (and so tests can drive it deterministically).
    unavailable_since: Option<tokio::time::Instant>,
}

impl InstallState {
    fn available(&self) -> bool {
        self.unavailable_since.is_none()
    }

    fn mark_unavailable(&mut self, now: tokio::time::Instant) {
        self.unavailable_since.get_or_insert(now);
    }

    /// Remaining grace for an outage, or `None` when available.
    /// `Some(Duration::ZERO)` means the window has expired.
    fn grace_remaining(
        &self,
        now: tokio::time::Instant,
        grace: std::time::Duration,
    ) -> Option<std::time::Duration> {
        self.unavailable_since
            .map(|since| grace.saturating_sub(now.duration_since(since)))
    }
}

#[derive(Debug)]
struct AdmissionInner {
    slots: HashMap<PoolKey, PoolSlot>,
    installs: HashMap<AdmissionKey, InstallState>,
    /// [`ADMISSION_UNAVAILABLE_GRACE`] in production. Tests shorten it to drive
    /// the expiry path without sleeping through a real minute — the same hook
    /// the Go, Python and ObjC mirrors expose, so the four implementations test
    /// this identically.
    grace: std::time::Duration,
}

impl Default for AdmissionInner {
    fn default() -> Self {
        Self {
            slots: HashMap::new(),
            installs: HashMap::new(),
            grace: ADMISSION_UNAVAILABLE_GRACE,
        }
    }
}

/// FIFO admission shared by every request path in a RelaySwitch.
#[derive(Debug, Clone, Default)]
pub struct AdmissionController {
    inner: Arc<Mutex<AdmissionInner>>,
    notify: Arc<tokio::sync::Notify>,
    tickets: Arc<AtomicU64>,
}

impl AdmissionController {
    /// Advertise one install's full pool map: EFFECTIVE capacity per pool.
    /// A configure is the target advertising itself: it ENDS any outage,
    /// which is what releases waiters queued through a respawn or a roster
    /// round-trip.
    pub fn configure_pools(&self, install: AdmissionKey, pools: &[(String, usize)]) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner
            .installs
            .entry(install.clone())
            .or_default()
            .unavailable_since = None;
        for (pool, capacity) in pools {
            let slot = inner
                .slots
                .entry(PoolKey {
                    install: install.clone(),
                    pool: pool.clone(),
                })
                .or_default();
            slot.capacity = *capacity;
        }
        drop(inner);
        self.notify.notify_waiters();
    }

    pub fn reconcile_master(
        &self,
        master_idx: usize,
        available: &std::collections::HashSet<AdmissionKey>,
    ) {
        let now = tokio::time::Instant::now();
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        for (install, state) in &mut inner.installs {
            if install.master_idx == master_idx && !available.contains(install) {
                state.mark_unavailable(now);
            }
        }
        drop(inner);
        self.notify.notify_waiters();
    }

    pub fn disable_master(&self, master_idx: usize) {
        let now = tokio::time::Instant::now();
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        for (install, state) in &mut inner.installs {
            if install.master_idx == master_idx {
                state.mark_unavailable(now);
            }
        }
        drop(inner);
        self.notify.notify_waiters();
    }

    /// Take a FIFO admission slot across a cap's whole pool CHAIN, waiting
    /// for capacity. The chain's FIRST key is the cap's singleton pool — the
    /// queue the ticket waits in; admission requires EVERY chain pool to
    /// have room, decided in one critical section (no half-admission).
    ///
    /// An UNAVAILABLE target (an install-level fact) does not fail the
    /// caller immediately. The request stays queued for
    /// [`ADMISSION_UNAVAILABLE_GRACE`] measured from the start of the
    /// outage, so a cartridge that is respawning — or that a transient
    /// registry outage briefly retired — resumes serving its queue instead
    /// of terminally failing every body waiting on it (17.2: one body's
    /// process loss must not terminate unrelated queued bodies). Only when
    /// the window expires does the wait fail, and it fails hard.
    pub async fn acquire(&self, chain: Vec<PoolKey>) -> Result<AdmissionPermit, String> {
        let head = chain.first().cloned().ok_or_else(|| {
            "admission chain is empty — a dispatch always has at least its cap's own pool"
                .to_string()
        })?;
        let install = head.install.clone();
        let ticket = self.tickets.fetch_add(1, Ordering::Relaxed);
        {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            for key in &chain {
                if !inner.slots.contains_key(key) {
                    return Err(format!(
                        "cartridge '{}' has no configured admission pool '{}'",
                        key.install.id, key.pool
                    ));
                }
            }
            let slot = inner
                .slots
                .get_mut(&head)
                .expect("checked above");
            // Queue even while unavailable: the loop below owns the grace
            // window, so a request arriving mid-outage gets the same treatment
            // as one that was already waiting when the outage began.
            slot.queue.push_back(ticket);
        }
        let mut waiter = AdmissionWaiter {
            controller: self.clone(),
            key: head.clone(),
            ticket,
            queued: true,
        };
        loop {
            let notified = self.notify.notified();
            let wait_budget = {
                let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                let grace = inner.grace;
                let available = inner
                    .installs
                    .get(&install)
                    .map(|state| state.available())
                    .unwrap_or(false);
                let chain_has_room = chain.iter().all(|key| {
                    inner
                        .slots
                        .get(key)
                        .expect("admission pool disappeared while request was queued")
                        .has_room()
                });
                let is_head = inner
                    .slots
                    .get(&head)
                    .expect("admission pool disappeared while request was queued")
                    .queue
                    .front()
                    == Some(&ticket);
                if available && chain_has_room && is_head {
                    inner
                        .slots
                        .get_mut(&head)
                        .expect("checked above")
                        .queue
                        .pop_front();
                    for key in &chain {
                        inner.slots.get_mut(key).expect("checked above").active += 1;
                    }
                    waiter.queued = false;
                    drop(inner);
                    self.notify.notify_waiters();
                    return Ok(AdmissionPermit {
                        controller: self.clone(),
                        chain: Some(chain),
                    });
                }
                let install_state = inner.installs.get(&install).map(|state| {
                    state.grace_remaining(tokio::time::Instant::now(), grace)
                });
                match install_state {
                    // Available (or never seen — treated as an outage that
                    // just began would hide a real config gap; an install
                    // with slots but no state is unreachable because
                    // configure_pools writes both): wait for capacity.
                    Some(None) => None,
                    // Outage still inside its window: wait, but no longer than
                    // what is left of it.
                    Some(Some(remaining)) if !remaining.is_zero() => Some(remaining),
                    // Outage outlived the window — the target is gone, not slow.
                    Some(Some(_)) => {
                        drop(inner);
                        return Err(format!(
                            "cartridge '{}' was unavailable for longer than {}s while this request \
                             waited for capacity",
                            install.id,
                            grace.as_secs()
                        ));
                    }
                    None => {
                        drop(inner);
                        return Err(format!(
                            "cartridge '{}' has admission pools but no install state — \
                             configure_pools was bypassed",
                            install.id
                        ));
                    }
                }
            };
            match wait_budget {
                None => notified.await,
                // A timeout here is not an error: it means the grace window is
                // up, and the next loop iteration re-reads the slot and decides.
                Some(remaining) => {
                    let _ = tokio::time::timeout(remaining, notified).await;
                }
            }
        }
    }

    /// Shorten the outage window. Tests only: production always uses
    /// [`ADMISSION_UNAVAILABLE_GRACE`].
    #[cfg(test)]
    fn set_grace_for_test(&self, grace: std::time::Duration) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.grace = grace;
    }

    fn cancel_waiter(&self, key: &PoolKey, ticket: u64) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(slot) = inner.slots.get_mut(key) {
            if let Some(position) = slot.queue.iter().position(|queued| *queued == ticket) {
                slot.queue.remove(position);
            }
        }
        drop(inner);
        self.notify.notify_waiters();
    }

    fn release(&self, chain: &[PoolKey]) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        for key in chain {
            let slot = inner
                .slots
                .get_mut(key)
                .expect("admission permit references an unknown pool");
            slot.active = slot
                .active
                .checked_sub(1)
                .expect("admission permit released without an active request");
        }
        drop(inner);
        self.notify.notify_waiters();
    }
}

struct AdmissionWaiter {
    controller: AdmissionController,
    key: PoolKey,
    ticket: u64,
    queued: bool,
}

impl Drop for AdmissionWaiter {
    fn drop(&mut self) {
        if self.queued {
            self.controller.cancel_waiter(&self.key, self.ticket);
        }
    }
}

pub struct AdmissionPermit {
    controller: AdmissionController,
    chain: Option<Vec<PoolKey>>,
}

impl std::fmt::Debug for AdmissionPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmissionPermit")
            .field("chain", &self.chain)
            .finish()
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        if let Some(chain) = self.chain.take() {
            self.controller.release(&chain);
        }
    }
}

/// Where a request came from and where it is going, as master indices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingEntry {
    /// Master the request arrived from (None = external caller / engine).
    pub source_master_idx: Option<usize>,
    /// Master the request was dispatched to.
    pub destination_master_idx: usize,
}

/// How a request's lifecycle ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalKind {
    End,
    Err,
    Cancelled,
    MasterDied,
}

impl TerminalKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            TerminalKind::End => "end",
            TerminalKind::Err => "err",
            TerminalKind::Cancelled => "cancelled",
            TerminalKind::MasterDied => "master_died",
        }
    }
}

/// Live phase of a request. `Terminated` never appears in the active table —
/// termination removes the entry (L7) and leaves a `TerminatedSummary` in the
/// recent ring instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestPhase {
    /// Registered; no flow frames observed yet.
    Created,
    /// At least one flow frame has moved through the runtime.
    Streaming,
}

/// Direction of a recorded frame relative to this runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameDirection {
    Inbound,
    Outbound,
}

/// Per-stream flow accounting. Keyed by stream_id (None = frames not tied to a
/// specific stream: REQ, END, ERR, LOG).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamFlowStats {
    pub frames_in: u64,
    pub frames_out: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub chunks_in: u64,
    pub chunks_out: u64,
    /// The stream's REMAINING credit window as observed by this runtime: the
    /// negotiated initial window, plus credits granted through this runtime,
    /// minus chunks that consumed them (in either direction — a stream's
    /// chunks flow one way and its grants the other). Non-negative in healthy
    /// operation; a negative value means the producer overran its window.
    /// Diagnostic — the endpoints hold the authoritative windows.
    pub credit_outstanding: i64,
    /// Stream announced with unbounded=true (no length promise).
    pub unbounded: bool,
    /// STREAM_END observed.
    pub ended: bool,
}

/// Everything a routing runtime knows about one in-flight request.
#[derive(Debug)]
pub struct RequestState {
    pub routing: RoutingEntry,
    /// Master index the response must return to (None = external caller).
    pub origin: Option<usize>,
    /// Response delivery channel for externally-registered requests.
    pub external_channel: Option<mpsc::UnboundedSender<Frame>>,
    /// Whether this is a cartridge-initiated peer invocation.
    pub is_peer: bool,
    /// Cap URN of the originating REQ, when known at registration — the
    /// request's nameable identity on the L8 surface. Without it a stats
    /// snapshot shows only anonymous rids, making background chatter
    /// indistinguishable from run traffic.
    pub cap_urn: Option<String>,
    /// Capacity slot held until the existing terminal path removes this state.
    pub admission_permit: Option<AdmissionPermit>,
    /// Child peer calls spawned under this request (cancel cascade).
    pub children: Vec<RequestKey>,
    pub phase: RequestPhase,
    /// Per-stream flow stats (None key = non-stream frames).
    pub streams: HashMap<Option<String>, StreamFlowStats>,
    /// The NEGOTIATED initial credit window of this request's destination —
    /// the ledger seed for every stream (see
    /// [`StreamFlowStats::credit_outstanding`]).
    pub initial_credit: u64,
    pub created_at: Instant,
    pub last_activity: Instant,
}

impl RequestState {
    pub fn new(
        routing: RoutingEntry,
        origin: Option<usize>,
        external_channel: Option<mpsc::UnboundedSender<Frame>>,
        is_peer: bool,
        initial_credit: u64,
    ) -> Self {
        let now = Instant::now();
        Self {
            routing,
            origin,
            external_channel,
            is_peer,
            cap_urn: None,
            admission_permit: None,
            children: Vec::new(),
            phase: RequestPhase::Created,
            streams: HashMap::new(),
            initial_credit,
            created_at: now,
            last_activity: now,
        }
    }

    /// Attach the originating REQ's cap URN — the request's nameable
    /// identity in observability surfaces.
    pub fn with_cap_urn(mut self, cap_urn: Option<String>) -> Self {
        self.cap_urn = cap_urn;
        self
    }

    pub fn with_admission_permit(mut self, permit: AdmissionPermit) -> Self {
        self.admission_permit = Some(permit);
        self
    }

    fn record(&mut self, direction: FrameDirection, frame: &Frame) {
        self.last_activity = Instant::now();
        if frame.is_flow_frame() {
            self.phase = RequestPhase::Streaming;
        }
        // A fresh stream starts with the NEGOTIATED initial window (L10): the
        // producer may send that many chunks before any CREDIT frame arrives,
        // so a ledger that starts at zero reads every healthy stream as
        // negative by exactly the initial window.
        let initial_credit = self.initial_credit as i64;
        let stats = self
            .streams
            .entry(frame.stream_id.clone())
            .or_insert_with(|| StreamFlowStats {
                credit_outstanding: initial_credit,
                ..StreamFlowStats::default()
            });
        let bytes = frame.payload.as_ref().map(|p| p.len() as u64).unwrap_or(0);
        match direction {
            FrameDirection::Inbound => {
                stats.frames_in += 1;
                stats.bytes_in += bytes;
                if frame.frame_type == FrameType::Chunk {
                    stats.chunks_in += 1;
                }
            }
            FrameDirection::Outbound => {
                stats.frames_out += 1;
                stats.bytes_out += bytes;
                if frame.frame_type == FrameType::Chunk {
                    stats.chunks_out += 1;
                }
            }
        }
        // A chunk consumes one credit from ITS stream's window regardless of
        // which way it flows past this runtime — a stream's chunks all flow
        // one direction, and its grants flow the other.
        if frame.frame_type == FrameType::Chunk {
            stats.credit_outstanding -= 1;
        }
        match frame.frame_type {
            FrameType::StreamStart if frame.is_unbounded() => stats.unbounded = true,
            FrameType::StreamEnd => stats.ended = true,
            FrameType::Credit => {
                stats.credit_outstanding += frame.credit_count().unwrap_or(0) as i64;
            }
            _ => {}
        }
    }
}

/// Summary of a finished request, retained in a bounded ring for stats.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminatedSummary {
    pub xid: String,
    pub rid: String,
    pub kind: TerminalKind,
    /// WHY a `Cancelled` termination happened — the Cancel's attribution, in
    /// the ERR vocabulary: the terminal code (`CANCELLED` /
    /// `ABORTED_COLLATERAL` / `ABORTED`, always present for a cancelled kind),
    /// the class (`user` for an operator's cancel, the originating failure's
    /// class for collateral, the host's for a host abort; None when the
    /// cancel was unattributed), and the reason. Never present for any other
    /// kind. Surfaces read it to say "aborted — step X failed" instead of
    /// the one word "cancelled".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_class: Option<crate::failure::AttributionClass>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_reason: Option<String>,
    pub is_peer: bool,
    #[serde(default)]
    pub cap_urn: Option<String>,
    pub lifetime_ms: u64,
    pub frames_in: u64,
    pub frames_out: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

/// How many terminated-request summaries the ring retains.
const RECENT_TERMINATED_CAP: usize = 64;

/// The unified request table (L7): one entry per in-flight request, one
/// registration, one termination, plus the rid→xid secondary index and the
/// recently-terminated ring.
#[derive(Default)]
pub struct RequestTable {
    entries: HashMap<RequestKey, RequestState>,
    rid_index: HashMap<MessageId, MessageId>,
    recent_terminated: VecDeque<TerminatedSummary>,
    total_registered: u64,
    terminated_by_kind: BTreeMap<&'static str, u64>,
    /// Called with every termination's summary, synchronously under the
    /// table guard — observers must be cheap and non-blocking (an engine
    /// aggregating per-run history, a test recorder). The bounded ring
    /// serves polling; this hook serves accumulation that must not miss
    /// terminations between polls (the ring evicts at 64).
    terminate_observer: Option<Box<dyn Fn(&TerminatedSummary) + Send + Sync>>,
}

impl std::fmt::Debug for RequestTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestTable")
            .field("entries", &self.entries.len())
            .field("recent_terminated", &self.recent_terminated.len())
            .field("total_registered", &self.total_registered)
            .finish()
    }
}

impl RequestTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a request. A request is registered exactly once (L7):
    /// re-registering a live key, or a RID already indexed to a different
    /// XID, is a protocol violation and is rejected.
    pub fn register(&mut self, key: RequestKey, state: RequestState) -> Result<(), String> {
        if self.entries.contains_key(&key) {
            return Err(format!(
                "request ({}, {}) already registered — a request is registered exactly once (L7)",
                key.0, key.1
            ));
        }
        if let Some(existing_xid) = self.rid_index.get(&key.1) {
            if *existing_xid != key.0 {
                return Err(format!(
                    "rid {} already indexed to xid {} — cannot re-index to xid {} (L7)",
                    key.1, existing_xid, key.0
                ));
            }
        }
        self.rid_index.insert(key.1.clone(), key.0.clone());
        self.entries.insert(key, state);
        self.total_registered += 1;
        Ok(())
    }

    pub fn get(&self, key: &RequestKey) -> Option<&RequestState> {
        self.entries.get(key)
    }

    pub fn get_mut(&mut self, key: &RequestKey) -> Option<&mut RequestState> {
        self.entries.get_mut(key)
    }

    pub fn contains(&self, key: &RequestKey) -> bool {
        self.entries.contains_key(key)
    }

    /// Look up the XID a bare RID belongs to (continuation frames arriving
    /// without routing IDs).
    pub fn xid_for_rid(&self, rid: &MessageId) -> Option<MessageId> {
        self.rid_index.get(rid).cloned()
    }

    /// Terminate a request: remove the entry and its rid index atomically,
    /// record a summary, and return the removed state (children for cancel
    /// cascades, the external channel for final delivery). After this returns,
    /// zero state for the key remains (L7). Returns None if the key is not
    /// live (already terminated — termination happens exactly once).
    ///
    /// `Cancelled` terminations go through [`Self::terminate_cancelled`] —
    /// a cancellation without a cause is not a state this table represents.
    pub fn terminate(&mut self, key: &RequestKey, kind: TerminalKind) -> Option<RequestState> {
        assert!(
            kind != TerminalKind::Cancelled,
            "RequestTable::terminate: Cancelled terminations carry a cause — use terminate_cancelled"
        );
        self.terminate_with(key, kind, None, None, None)
    }

    /// Terminate a request as cancelled, recording WHY (the Cancel frame's
    /// attribution) on its summary. An unattributed reason records the
    /// terminal code `CANCELLED` and no class.
    pub fn terminate_cancelled(
        &mut self,
        key: &RequestKey,
        reason: &CancelReason,
    ) -> Option<RequestState> {
        self.terminate_with(
            key,
            TerminalKind::Cancelled,
            Some(reason.terminal_code().to_string()),
            reason.class,
            reason.message.clone(),
        )
    }

    fn terminate_with(
        &mut self,
        key: &RequestKey,
        kind: TerminalKind,
        cancel_code: Option<String>,
        cancel_class: Option<crate::failure::AttributionClass>,
        cancel_reason: Option<String>,
    ) -> Option<RequestState> {
        let state = self.entries.remove(key)?;
        // Only remove the rid index if it points at THIS xid — a re-used RID
        // under another XID (never valid per register, but defensive against
        // the impossible) must not lose its index.
        if self.rid_index.get(&key.1) == Some(&key.0) {
            self.rid_index.remove(&key.1);
        }

        let totals = state
            .streams
            .values()
            .fold((0u64, 0u64, 0u64, 0u64), |acc, s| {
                (
                    acc.0 + s.frames_in,
                    acc.1 + s.frames_out,
                    acc.2 + s.bytes_in,
                    acc.3 + s.bytes_out,
                )
            });
        if self.recent_terminated.len() == RECENT_TERMINATED_CAP {
            self.recent_terminated.pop_front();
        }
        self.recent_terminated.push_back(TerminatedSummary {
            xid: key.0.to_string(),
            rid: key.1.to_string(),
            kind,
            cancel_code,
            cancel_class,
            cancel_reason,
            is_peer: state.is_peer,
            cap_urn: state.cap_urn.clone(),
            lifetime_ms: state.created_at.elapsed().as_millis() as u64,
            frames_in: totals.0,
            frames_out: totals.1,
            bytes_in: totals.2,
            bytes_out: totals.3,
        });
        *self.terminated_by_kind.entry(kind.as_str()).or_insert(0) += 1;
        if let Some(observer) = &self.terminate_observer {
            observer(
                self.recent_terminated
                    .back()
                    .expect("summary was just pushed"),
            );
        }
        Some(state)
    }

    /// Install the termination observer (see field docs). One observer;
    /// installing replaces any previous one.
    pub fn set_terminate_observer(
        &mut self,
        observer: Box<dyn Fn(&TerminatedSummary) + Send + Sync>,
    ) {
        self.terminate_observer = Some(observer);
    }

    /// Whether this RID belongs to a recently terminated request (the bounded
    /// `recent_terminated` ring).
    ///
    /// This is the discriminator between the two ways a frame can arrive with
    /// no routing state. A hit here means the frame CROSSED its request's
    /// terminal in flight — the ordinary teardown race of credit-based flow
    /// control (a grant or straggler emitted before the sender observed
    /// END/ERR) — which receivers count as a BENIGN post-terminal straggler
    /// (nothing went wrong; never a drop). A miss means the
    /// table has never known the RID within the ring's horizon: a genuine
    /// `no_route` anomaly worth alarming on. The ring holds the last
    /// [`RECENT_TERMINATED_CAP`] terminations; the race window is
    /// milliseconds, so eviction cannot misclassify a real race, only age a
    /// pathologically late frame back into `no_route` — where something that
    /// stale belongs.
    pub fn recently_terminated_rid(&self, rid: &MessageId) -> bool {
        let rid = rid.to_string();
        self.recent_terminated.iter().any(|s| s.rid == rid)
    }

    /// How a recently terminated RID ended (newest summary for the RID), or
    /// None when the RID is live or unknown within the ring's horizon.
    pub fn recent_terminal_of_rid(&self, rid: &MessageId) -> Option<&TerminatedSummary> {
        let rid = rid.to_string();
        self.recent_terminated.iter().rev().find(|s| s.rid == rid)
    }

    /// Record a frame moving through the runtime for this request.
    /// Unknown keys are ignored — the caller decides whether that is a
    /// counted drop (it is, at the routing layer) — recording is accounting,
    /// not routing.
    pub fn record_frame(&mut self, key: &RequestKey, direction: FrameDirection, frame: &Frame) {
        if let Some(state) = self.entries.get_mut(key) {
            state.record(direction, frame);
        }
    }

    /// Register a child peer call under its parent (cancel cascade).
    pub fn link_child(&mut self, parent: &RequestKey, child: RequestKey) {
        if let Some(state) = self.entries.get_mut(parent) {
            state.children.push(child);
        }
    }

    /// Keys of all live requests (for sweeps). Cloned so the caller can
    /// mutate the table while iterating.
    pub fn keys(&self) -> Vec<RequestKey> {
        self.entries.keys().cloned().collect()
    }

    /// Keys of live requests matching a predicate on their state.
    pub fn keys_where(&self, pred: impl Fn(&RequestState) -> bool) -> Vec<RequestKey> {
        self.entries
            .iter()
            .filter(|(_, s)| pred(s))
            .map(|(k, _)| k.clone())
            .collect()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Serializable snapshot of the table: live requests + recent terminations
    /// + lifetime totals. Field names are the mirror contract.
    pub fn snapshot(&self) -> RequestTableSnapshot {
        let mut active: Vec<RequestSnapshot> = self
            .entries
            .iter()
            .map(|(key, s)| RequestSnapshot {
                xid: key.0.to_string(),
                rid: key.1.to_string(),
                phase: s.phase,
                is_peer: s.is_peer,
                cap_urn: s.cap_urn.clone(),
                origin_master: s.origin,
                destination_master: s.routing.destination_master_idx,
                age_ms: s.created_at.elapsed().as_millis() as u64,
                idle_ms: s.last_activity.elapsed().as_millis() as u64,
                children: s.children.len() as u64,
                streams: s
                    .streams
                    .iter()
                    .map(|(id, stats)| StreamSnapshot {
                        stream_id: id.clone(),
                        stats: stats.clone(),
                    })
                    .collect(),
            })
            .collect();
        active.sort_by(|a, b| a.rid.cmp(&b.rid));
        RequestTableSnapshot {
            active,
            recent_terminated: self.recent_terminated.iter().cloned().collect(),
            total_registered: self.total_registered,
            terminated_by_kind: self
                .terminated_by_kind
                .iter()
                .map(|(k, v)| (k.to_string(), *v))
                .collect(),
        }
    }
}

/// One stream's stats in a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamSnapshot {
    pub stream_id: Option<String>,
    #[serde(flatten)]
    pub stats: StreamFlowStats,
}

/// One live request in a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestSnapshot {
    pub xid: String,
    pub rid: String,
    pub phase: RequestPhase,
    pub is_peer: bool,
    #[serde(default)]
    pub cap_urn: Option<String>,
    pub origin_master: Option<usize>,
    pub destination_master: usize,
    pub age_ms: u64,
    pub idle_ms: u64,
    pub children: u64,
    pub streams: Vec<StreamSnapshot>,
}

/// Full table snapshot: the L8 observability surface for request state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestTableSnapshot {
    pub active: Vec<RequestSnapshot>,
    pub recent_terminated: Vec<TerminatedSummary>,
    pub total_registered: u64,
    pub terminated_by_kind: BTreeMap<String, u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admission_key() -> AdmissionKey {
        AdmissionKey {
            master_idx: 0,
            registry_url: Some("https://registry.example".to_string()),
            channel: "stable".to_string(),
            id: "candle".to_string(),
            version: "1.0.0".to_string(),
            sha256: "abc123".to_string(),
        }
    }

    fn pool_key(install: &AdmissionKey, pool: &str) -> PoolKey {
        PoolKey {
            install: install.clone(),
            pool: pool.to_string(),
        }
    }

    /// The minimal chain: a cap addressed through its install's `all` pool.
    fn all_chain(install: &AdmissionKey) -> Vec<PoolKey> {
        vec![pool_key(install, "all")]
    }

    // TEST7110: admission is strict FIFO and a terminal request releases exactly
    // one capacity slot for the next body.
    #[tokio::test]
    async fn test7110_admission_fifo_releases_one_waiter() {
        let controller = AdmissionController::default();
        let key = admission_key();
        controller.configure_pools(key.clone(), &[("all".to_string(), 1)]);
        let first = controller.acquire(all_chain(&key)).await.unwrap();

        let second_controller = controller.clone();
        let second_key = key.clone();
        let second = tokio::spawn(async move { second_controller.acquire(all_chain(&second_key)).await });
        tokio::task::yield_now().await;
        let third_controller = controller.clone();
        let third_key = key.clone();
        let third = tokio::spawn(async move { third_controller.acquire(all_chain(&third_key)).await });
        tokio::task::yield_now().await;
        assert!(!second.is_finished());
        assert!(!third.is_finished());

        drop(first);
        let second_permit = tokio::time::timeout(std::time::Duration::from_secs(1), second)
            .await
            .expect("second FIFO waiter must be admitted")
            .expect("second waiter task must not fail")
            .expect("second waiter must acquire its slot");
        assert!(!third.is_finished(), "one release admits only one waiter");
        drop(second_permit);
        tokio::time::timeout(std::time::Duration::from_secs(1), third)
            .await
            .expect("third FIFO waiter must be admitted next")
            .expect("third waiter task must not fail")
            .expect("third waiter must acquire its slot");
    }

    // TEST7111: cancelling a queued body removes its ticket; it cannot strand
    // later ForEach bodies behind a dead queue head.
    #[tokio::test]
    async fn test7111_cancelled_admission_waiter_cannot_block_queue() {
        let controller = AdmissionController::default();
        let key = admission_key();
        controller.configure_pools(key.clone(), &[("all".to_string(), 1)]);
        let active = controller.acquire(all_chain(&key)).await.unwrap();

        let cancelled_controller = controller.clone();
        let cancelled_key = key.clone();
        let cancelled =
            tokio::spawn(async move { cancelled_controller.acquire(all_chain(&cancelled_key)).await });
        tokio::task::yield_now().await;
        cancelled.abort();
        let _ = cancelled.await;

        let next_controller = controller.clone();
        let next_key = key.clone();
        let next = tokio::spawn(async move { next_controller.acquire(all_chain(&next_key)).await });
        tokio::task::yield_now().await;
        drop(active);
        tokio::time::timeout(std::time::Duration::from_secs(1), next)
            .await
            .expect("later waiter must pass the cancelled ticket")
            .expect("later waiter task must not fail")
            .expect("later waiter must acquire its slot");
    }

    // TEST7112: the post-HELLO capacity update wakes already queued work. This
    // is what changes an unstarted cartridge's one bootstrap slot to its
    // authoritative runtime capacity without waiting for the first body to end.
    #[tokio::test]
    async fn test7112_capacity_reconfiguration_wakes_existing_waiters() {
        let controller = AdmissionController::default();
        let key = admission_key();
        controller.configure_pools(key.clone(), &[("all".to_string(), 1)]);
        let active = controller.acquire(all_chain(&key)).await.unwrap();

        let waiting_controller = controller.clone();
        let waiting_key = key.clone();
        let waiting = tokio::spawn(async move { waiting_controller.acquire(all_chain(&waiting_key)).await });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        controller.configure_pools(key, &[("all".to_string(), 0)]);
        let concurrently_admitted =
            tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
                .await
                .expect("unlimited HELLO capacity must wake queued work")
                .expect("queued waiter task must not fail")
                .expect("queued waiter must acquire its slot");
        drop(concurrently_admitted);
        drop(active);
    }

    // TEST7114: a cartridge that disappears and comes back does NOT terminally
    // fail the work queued behind it. This is 17.2's "queued bodies are not
    // assigned terminal failure from another body's process loss; once a
    // replacement instance advertises capacity, subsequent queued work is
    // admitted to that live instance".
    //
    // The regression this pins: a single failed registry-manifest fetch retired
    // three live cartridges for ~24s, and every queued ForEach body was failed
    // with "became unavailable while waiting for capacity" — 195 bodies lost to
    // an outage that had already healed.
    #[tokio::test]
    async fn test7114_transient_unavailability_does_not_fail_queued_work() {
        let controller = AdmissionController::default();
        let key = admission_key();
        controller.configure_pools(key.clone(), &[("all".to_string(), 1)]);
        let active = controller.acquire(all_chain(&key)).await.unwrap();

        let waiting_controller = controller.clone();
        let waiting_key = key.clone();
        let waiting = tokio::spawn(async move { waiting_controller.acquire(all_chain(&waiting_key)).await });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        // The target vanishes from its host's inventory...
        controller.disable_master(key.master_idx);
        tokio::task::yield_now().await;
        assert!(
            !waiting.is_finished(),
            "an outage inside the grace window must not fail queued work"
        );

        // ...and comes back, which is what must release the queue.
        controller.configure_pools(key.clone(), &[("all".to_string(), 1)]);
        drop(active);
        tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .expect("a restored admission target must admit the work queued on it")
            .expect("queued waiter task must not fail")
            .expect("queued work must acquire the restored target");
    }

    // TEST1943: the grace window is a BOUND, not a hang. A target that stays
    // gone fails its queued work once the window expires, so a cartridge that
    // is genuinely retired surfaces as a failure instead of stalling the run
    // forever.
    #[tokio::test]
    async fn test1943_outage_outliving_the_grace_window_fails_queued_work() {
        let controller = AdmissionController::default();
        // Shorten the window so the expiry path is exercised without sleeping
        // through a real minute. Production uses ADMISSION_UNAVAILABLE_GRACE.
        controller.set_grace_for_test(std::time::Duration::from_millis(150));
        let key = admission_key();
        controller.configure_pools(key.clone(), &[("all".to_string(), 1)]);
        let active = controller.acquire(all_chain(&key)).await.unwrap();

        let waiting_controller = controller.clone();
        let waiting_key = key.clone();
        let waiting = tokio::spawn(async move { waiting_controller.acquire(all_chain(&waiting_key)).await });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        controller.disable_master(key.master_idx);
        let error = tokio::time::timeout(std::time::Duration::from_secs(3), waiting)
            .await
            .expect("an expired grace window must wake queued work")
            .expect("queued waiter task must not fail")
            .expect_err("queued work must not acquire a target that never came back");
        assert!(
            error.contains("unavailable for longer than"),
            "the failure must name the outage, not a generic routing error: {error}"
        );
        drop(active);
    }

    // TEST1524: chain admission is ATOMIC — a request is admitted only when
    // EVERY pool in its chain has room, and holds all of them until release.
    // A free singleton behind a full shared pool waits; releasing the shared
    // pool's holder admits it.
    #[tokio::test]
    async fn test1524_chain_admission_is_atomic_across_pools() {
        let controller = AdmissionController::default();
        let key = admission_key();
        controller.configure_pools(
            key.clone(),
            &[
                ("cap:a".to_string(), 0),
                ("cap:b".to_string(), 0),
                ("gpu".to_string(), 1),
                ("all".to_string(), 0),
            ],
        );
        let chain_a = vec![
            pool_key(&key, "cap:a"),
            pool_key(&key, "gpu"),
            pool_key(&key, "all"),
        ];
        let chain_b = vec![
            pool_key(&key, "cap:b"),
            pool_key(&key, "gpu"),
            pool_key(&key, "all"),
        ];

        let holder = controller.acquire(chain_a).await.unwrap();

        // cap:b's own singleton is free, but the shared "gpu" pool is full —
        // the whole chain must wait.
        let waiting_controller = controller.clone();
        let waiting =
            tokio::spawn(async move { waiting_controller.acquire(chain_b).await });
        tokio::task::yield_now().await;
        assert!(
            !waiting.is_finished(),
            "a full shared pool must block the whole chain"
        );

        drop(holder);
        tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .expect("releasing the shared pool must admit the queued chain")
            .expect("queued chain task must not fail")
            .expect("queued chain must acquire all its pools");
    }

    // TEST1525: pools are ISOLATED — saturating one cap's singleton does not
    // block a different cap whose chain shares only unlimited pools.
    #[tokio::test]
    async fn test1525_disjoint_bounded_pools_admit_independently() {
        let controller = AdmissionController::default();
        let key = admission_key();
        controller.configure_pools(
            key.clone(),
            &[
                ("cap:a".to_string(), 1),
                ("cap:b".to_string(), 1),
                ("all".to_string(), 0),
            ],
        );
        let chain_a = vec![pool_key(&key, "cap:a"), pool_key(&key, "all")];
        let chain_b = vec![pool_key(&key, "cap:b"), pool_key(&key, "all")];

        let _a = controller.acquire(chain_a).await.unwrap();
        // cap:a is saturated; cap:b must admit immediately.
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            controller.acquire(chain_b),
        )
        .await
        .expect("a saturated sibling singleton must not delay this cap")
        .expect("disjoint chain must acquire immediately");
    }

    // TEST1526: acquiring a chain naming a pool the install never advertised
    // fails hard — an unknown pool is a protocol defect, never a free pass.
    #[tokio::test]
    async fn test1526_unknown_pool_in_chain_fails_hard() {
        let controller = AdmissionController::default();
        let key = admission_key();
        controller.configure_pools(key.clone(), &[("all".to_string(), 0)]);
        let error = controller
            .acquire(vec![pool_key(&key, "cap:ghost"), pool_key(&key, "all")])
            .await
            .expect_err("an unadvertised pool must refuse admission");
        assert!(
            error.contains("cap:ghost"),
            "the failure must name the unknown pool: {error}"
        );
    }

    fn key(x: u64, r: u64) -> RequestKey {
        (MessageId::Uint(x), MessageId::Uint(r))
    }

    /// The ledger seed every test request negotiates — deliberately a small
    /// odd-sized window so seed arithmetic is visible in assertions.
    const TEST_INITIAL_CREDIT: u64 = 8;

    fn state(dest: usize, origin: Option<usize>, is_peer: bool) -> RequestState {
        RequestState::new(
            RoutingEntry {
                source_master_idx: origin,
                destination_master_idx: dest,
            },
            origin,
            None,
            is_peer,
            TEST_INITIAL_CREDIT,
        )
    }

    // TEST7087: Protocol stats snapshots serialize with stable field names — the snapshot shape is the mirror contract.
    #[test]
    fn test7092_cap_urn_attribution_survives_lifecycle() {
        // TEST7092: A request registered with its originating REQ's cap URN
        // carries that identity through the ACTIVE snapshot and into the
        // terminated ring — observability surfaces can always NAME a request
        // (background chatter vs run traffic), never just show a bare rid.
        // A request registered without one (pre-attribution mirror, unknown
        // origin) snapshots with cap_urn null — absent, never invented.
        let mut table = RequestTable::new();
        let named = key(1, 9);
        table
            .register(
                named.clone(),
                state(0, Some(1), false).with_cap_urn(Some("cap:effect=none".to_string())),
            )
            .unwrap();
        let anonymous = key(2, 10);
        table
            .register(anonymous.clone(), state(0, Some(1), true))
            .unwrap();

        let snapshot = table.snapshot();
        let by_rid = |rid: &str| snapshot.active.iter().find(|r| r.rid == rid).unwrap();
        assert_eq!(
            by_rid("9").cap_urn.as_deref(),
            Some("cap:effect=none"),
            "active snapshot names the request's cap"
        );
        assert_eq!(by_rid("10").cap_urn, None, "unknown identity stays absent");

        table.terminate(&named, TerminalKind::End).unwrap();
        let snapshot = table.snapshot();
        assert_eq!(
            snapshot.recent_terminated[0].cap_urn.as_deref(),
            Some("cap:effect=none"),
            "the terminated ring keeps the cap identity"
        );
    }

    #[test]
    fn test7087_snapshot_field_names_are_stable() {
        let mut table = RequestTable::new();
        let k = key(1, 9);
        table.register(k.clone(), state(0, Some(1), true)).unwrap();
        let rid = MessageId::Uint(9);
        let ss = Frame::stream_start(
            rid,
            "s".to_string(),
            "media:enc=utf-8".to_string(),
            Some(false),
        );
        table.record_frame(&k, FrameDirection::Inbound, &ss);

        let json = serde_json::to_value(table.snapshot()).unwrap();
        for field in [
            "active",
            "recent_terminated",
            "total_registered",
            "terminated_by_kind",
        ] {
            assert!(
                json.get(field).is_some(),
                "missing top-level field {}",
                field
            );
        }
        let req = &json["active"][0];
        for field in [
            "xid",
            "rid",
            "phase",
            "is_peer",
            "origin_master",
            "destination_master",
            "age_ms",
            "idle_ms",
            "children",
            "streams",
        ] {
            assert!(req.get(field).is_some(), "missing request field {}", field);
        }
        assert_eq!(req["phase"], "streaming", "phase serializes snake_case");
        let stream = &req["streams"][0];
        for field in [
            "stream_id",
            "frames_in",
            "frames_out",
            "bytes_in",
            "bytes_out",
            "chunks_in",
            "chunks_out",
            "credit_outstanding",
            "unbounded",
            "ended",
        ] {
            assert!(
                stream.get(field).is_some(),
                "missing stream field {}",
                field
            );
        }

        table.terminate(&k, TerminalKind::MasterDied).unwrap();
        let json = serde_json::to_value(table.snapshot()).unwrap();
        let summary = &json["recent_terminated"][0];
        for field in [
            "xid",
            "rid",
            "kind",
            "is_peer",
            "lifetime_ms",
            "frames_in",
            "frames_out",
            "bytes_in",
            "bytes_out",
        ] {
            assert!(
                summary.get(field).is_some(),
                "missing summary field {}",
                field
            );
        }
        assert_eq!(summary["kind"], "master_died", "kind serializes snake_case");
    }

    // TEST7088: last_activity is monotonic non-decreasing across a long-lived streaming request — idle time resets on every recorded frame and never runs backwards.
    #[test]
    fn test7088_last_activity_monotonic() {
        let mut table = RequestTable::new();
        let k = key(1, 5);
        table.register(k.clone(), state(0, None, false)).unwrap();
        let rid = MessageId::Uint(5);

        let mut last_activity_points = Vec::new();
        for i in 0..3u64 {
            std::thread::sleep(std::time::Duration::from_millis(15));
            let payload = vec![0u8; 4];
            let checksum = Frame::compute_checksum(&payload);
            let chunk = Frame::chunk(rid.clone(), "s".to_string(), i, payload, i, checksum);
            table.record_frame(&k, FrameDirection::Inbound, &chunk);
            let entry = table.get(&k).unwrap();
            assert!(
                entry.last_activity >= entry.created_at,
                "activity never precedes creation"
            );
            last_activity_points.push(entry.last_activity);
        }
        for pair in last_activity_points.windows(2) {
            assert!(
                pair[1] >= pair[0],
                "last_activity must be monotonic non-decreasing"
            );
        }
        // idle_ms in the snapshot reflects the LAST activity, not the first:
        // it must be (much) smaller than the request's age.
        std::thread::sleep(std::time::Duration::from_millis(15));
        let snap = table.snapshot();
        let req = &snap.active[0];
        assert!(
            req.idle_ms <= req.age_ms,
            "idle {}ms cannot exceed age {}ms",
            req.idle_ms,
            req.age_ms
        );
        assert!(
            req.age_ms >= 45,
            "age accumulates across the request lifetime"
        );
    }

    // TEST7030: A request registers exactly once and terminates exactly once — duplicate registration and double termination are rejected, and after terminate zero state remains for the key.
    #[test]
    fn test7030_register_once_terminate_once() {
        let mut table = RequestTable::new();
        let k = key(1, 100);

        table.register(k.clone(), state(0, None, false)).unwrap();
        assert!(table.contains(&k));
        assert_eq!(
            table.xid_for_rid(&MessageId::Uint(100)),
            Some(MessageId::Uint(1))
        );

        // Duplicate registration of a live key is a protocol violation.
        let err = table
            .register(k.clone(), state(0, None, false))
            .unwrap_err();
        assert!(err.contains("already registered"));

        // Same RID under a different XID is rejected while live.
        let err = table
            .register(key(2, 100), state(0, None, false))
            .unwrap_err();
        assert!(err.contains("already indexed"));

        let removed = table.terminate(&k, TerminalKind::End).expect("live entry");
        assert!(!removed.is_peer);
        assert!(!table.contains(&k), "no entry remains after terminate");
        assert_eq!(
            table.xid_for_rid(&MessageId::Uint(100)),
            None,
            "rid index removed with the entry (L7)"
        );
        assert!(
            table.terminate(&k, TerminalKind::End).is_none(),
            "termination happens exactly once"
        );
    }

    // TEST7031: The rid index and the entry table never disagree across register/terminate cycles, and a terminated rid is immediately reusable.
    #[test]
    fn test7031_rid_index_consistency() {
        let mut table = RequestTable::new();
        for round in 0..3u64 {
            for n in 0..10u64 {
                let k = key(round * 100 + n, n);
                table.register(k, state(0, None, false)).unwrap();
            }
            for n in 0..10u64 {
                let k = key(round * 100 + n, n);
                let xid = table.xid_for_rid(&MessageId::Uint(n)).expect("indexed");
                assert_eq!(xid, k.0, "index resolves to the live entry's xid");
                assert!(table.contains(&(xid, MessageId::Uint(n))));
                table.terminate(&k, TerminalKind::End).unwrap();
                assert_eq!(table.xid_for_rid(&MessageId::Uint(n)), None);
            }
        }
        assert!(table.is_empty());
        assert_eq!(table.snapshot().total_registered, 30);
    }

    // TEST7032: record_frame accumulates per-stream frame/byte/chunk counters by direction, flips phase Created→Streaming on the first flow frame, and tracks unbounded/ended/credit stream markers.
    #[test]
    fn test7032_record_frame_stats_and_phase() {
        let mut table = RequestTable::new();
        let k = key(1, 7);
        table.register(k.clone(), state(0, None, false)).unwrap();
        assert_eq!(table.get(&k).unwrap().phase, RequestPhase::Created);

        let rid = MessageId::Uint(7);
        let ss = Frame::stream_start_unbounded(
            rid.clone(),
            "s1".to_string(),
            "media:enc=utf-8".to_string(),
            None,
        );
        table.record_frame(&k, FrameDirection::Inbound, &ss);
        assert_eq!(table.get(&k).unwrap().phase, RequestPhase::Streaming);

        let payload = vec![0u8; 100];
        let checksum = Frame::compute_checksum(&payload);
        let chunk = Frame::chunk(rid.clone(), "s1".to_string(), 0, payload, 0, checksum);
        table.record_frame(&k, FrameDirection::Inbound, &chunk);
        table.record_frame(&k, FrameDirection::Outbound, &chunk);

        let credit = Frame::credit(
            rid.clone(),
            Some("s1".to_string()),
            4,
            crate::bifaci::frame::CreditDirection::Response,
        );
        table.record_frame(&k, FrameDirection::Outbound, &credit);

        let se = Frame::stream_end_unbounded(rid, "s1".to_string());
        table.record_frame(&k, FrameDirection::Inbound, &se);

        let entry = table.get(&k).unwrap();
        let s1 = entry.streams.get(&Some("s1".to_string())).unwrap();
        assert_eq!(s1.frames_in, 3, "stream_start + chunk + stream_end");
        assert_eq!(s1.frames_out, 2, "chunk + credit");
        assert_eq!(s1.chunks_in, 1);
        assert_eq!(s1.chunks_out, 1);
        assert_eq!(s1.bytes_in, 100);
        assert_eq!(s1.bytes_out, 100);
        assert!(s1.unbounded);
        assert!(s1.ended);
        // The ledger is the REMAINING WINDOW: seeded with the negotiated
        // initial credit, +4 granted, -1 per chunk in EITHER direction (the
        // inbound chunk and the outbound chunk each consumed one).
        assert_eq!(
            s1.credit_outstanding,
            TEST_INITIAL_CREDIT as i64 + 4 - 2,
            "window = seed + grants - chunks"
        );
    }

    // TEST7033: Terminated requests leave a bounded ring of summaries carrying kind, lifetime, and flow totals, and the ring evicts oldest-first at capacity.
    #[test]
    fn test7033_terminated_summaries_ring() {
        let mut table = RequestTable::new();
        for n in 0..(RECENT_TERMINATED_CAP as u64 + 3) {
            let k = key(n, n);
            table.register(k.clone(), state(0, Some(2), true)).unwrap();
            let payload = vec![0u8; 10];
            let checksum = Frame::compute_checksum(&payload);
            let chunk = Frame::chunk(MessageId::Uint(n), "s".to_string(), 0, payload, 0, checksum);
            table.record_frame(&k, FrameDirection::Inbound, &chunk);
            table.terminate_cancelled(&k, &CancelReason::user(false)).unwrap();
        }
        let snap = table.snapshot();
        assert_eq!(snap.recent_terminated.len(), RECENT_TERMINATED_CAP);
        // Oldest evicted: first retained summary is rid "3"
        assert_eq!(
            snap.recent_terminated[0].rid,
            MessageId::Uint(3).to_string()
        );
        let last = snap.recent_terminated.last().unwrap();
        assert_eq!(last.kind, TerminalKind::Cancelled);
        assert_eq!(last.cancel_code.as_deref(), Some("CANCELLED"));
        assert_eq!(last.cancel_class, Some(crate::failure::AttributionClass::User));
        assert!(last.is_peer);
        assert_eq!(last.frames_in, 1);
        assert_eq!(last.bytes_in, 10);
        assert_eq!(
            snap.terminated_by_kind.get("cancelled"),
            Some(&(RECENT_TERMINATED_CAP as u64 + 3))
        );
    }

    // TEST8115: recently_terminated_rid discriminates the teardown race from
    // genuine routing loss: true for a rid whose request just terminated,
    // false for a rid the table never knew, false again once the summary is
    // evicted past the ring's horizon — a pathologically late frame ages back
    // into no_route, where something that stale belongs.
    #[test]
    fn test8115_recently_terminated_rid_discriminates_and_ages_out() {
        let mut table = RequestTable::new();

        let k = key(1, 500);
        table.register(k.clone(), state(0, None, false)).unwrap();
        assert!(
            !table.recently_terminated_rid(&MessageId::Uint(500)),
            "a LIVE request is not recently terminated"
        );
        table.terminate(&k, TerminalKind::End).unwrap();
        assert!(
            table.recently_terminated_rid(&MessageId::Uint(500)),
            "a just-terminated rid must be in the ring"
        );
        assert!(
            !table.recently_terminated_rid(&MessageId::Uint(9999)),
            "an unknown rid is a genuine routing anomaly, never a benign straggler"
        );

        // Push the ring past its horizon: rid 500's summary must age out.
        for n in 1000..(1000 + RECENT_TERMINATED_CAP as u64) {
            let k = key(n, n);
            table.register(k.clone(), state(0, None, false)).unwrap();
            table.terminate(&k, TerminalKind::End).unwrap();
        }
        assert!(
            !table.recently_terminated_rid(&MessageId::Uint(500)),
            "eviction past RECENT_TERMINATED_CAP ends benign-straggler classification"
        );
        assert!(
            table.recently_terminated_rid(&MessageId::Uint(1000 + RECENT_TERMINATED_CAP as u64 - 1)),
            "the newest termination is still in the ring"
        );
    }
}

