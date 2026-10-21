//! Run-flow correlation: joins the relay switch's protocol observability
//! (bifaci v4 L8 — per-request phases, per-stream flow counters, terminated
//! summaries) to the steps of a running plan.
//!
//! The protocol keys everything by request id (`rid`); a run's picture keys
//! everything by step `token_id`. The bridge is built at the only point both
//! are known: the shared segment executor
//! ([`run_dag_on_context`](crate::run_dag_on_context), via the
//! [`FlowObserver`](crate::FlowObserver) hook) registers `(token_id, rid)` the
//! moment each invocation's REQ is minted, under the scope of the run it
//! belongs to. A poll-time snapshot then joins the switch's
//! `protocol_stats()` against the scope's rid set and aggregates per step
//! (ForEach bodies run several concurrent invocations of the same step).
//!
//! Every host that draws a run uses this one correlation: the engine scopes
//! by machine-run id, the CLI by the run it started.

use crate::{RelaySwitchProtocolStats, RequestPhase, StreamSnapshot, TerminalKind};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, RwLock};

/// One stream's flow accounting within a step, aggregated across the step's
/// concurrent invocations. Field names mirror capdag's `StreamFlowStats`
/// (the L8 mirror contract) and the `RunStreamFlow` proto message.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RunStreamFlow {
    pub stream_id: Option<String>,
    pub frames_in: u64,
    pub frames_out: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub chunks_in: u64,
    pub chunks_out: u64,
    pub credit_outstanding: i64,
    pub unbounded: bool,
    pub ended: bool,
}

/// Live protocol state of one strand step, joined at poll time.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RunFlowState {
    pub token_id: String,
    /// `created` | `streaming` while any invocation is live; otherwise the
    /// worst terminal kind among the step's finished invocations
    /// (`err` > `cancelled` > `master_died` > `done`). A finished invocation
    /// whose terminal kind has aged out of the switch's bounded ring counts
    /// as `done` — run-level failures are carried by run status/body
    /// outcomes, not by this field.
    pub phase: String,
    /// WHY the phase is `cancelled` — the Cancel's attribution in the ERR
    /// vocabulary: terminal code (`CANCELLED` / `ABORTED_COLLATERAL` /
    /// `ABORTED`; always present for a cancelled phase), class (`user` for an
    /// operator's cancel, the originating failure's class for collateral, the
    /// host's for a host abort; absent for an unattributed cancel) and reason.
    /// A collateral teardown renders as "aborted — <reason>", never as a
    /// cancel; "Cancelled" is reserved for `user`.
    pub cancel_code: Option<String>,
    pub cancel_class: Option<String>,
    pub cancel_reason: Option<String>,
    pub age_ms: u64,
    pub idle_ms: u64,
    pub active_requests: u32,
    pub completed_requests: u32,
    pub streams: Vec<RunStreamFlow>,
}

/// rid → token_id map for one scope (machine run).
type ScopeMap = HashMap<String, String>;

/// Terminal aggregate for one strand step within a scope: every termination
/// the switch reports for the step's invocations, folded as it happens (the
/// terminated ring is bounded — polling it would miss terminations between
/// polls; this never does). Persisted as the run's durable flow summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepFlowTotals {
    pub invocations: u32,
    pub completed: u32,
    pub failed: u32,
    pub frames_in: u64,
    pub frames_out: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub total_lifetime_ms: u64,
    pub worst_kind: TerminalKind,
    /// The cancel attribution of the worst termination when it is a
    /// cancellation — carried with `worst_kind` so the summary can say why.
    pub cancel_code: Option<String>,
    pub cancel_class: Option<crate::AttributionClass>,
    pub cancel_reason: Option<String>,
}

impl Default for StepFlowTotals {
    fn default() -> Self {
        Self {
            invocations: 0,
            completed: 0,
            failed: 0,
            frames_in: 0,
            frames_out: 0,
            bytes_in: 0,
            bytes_out: 0,
            total_lifetime_ms: 0,
            worst_kind: TerminalKind::End,
            cancel_code: None,
            cancel_class: None,
            cancel_reason: None,
        }
    }
}

/// One scope's live correlation state: the rid→token map fed by the execution
/// path plus the per-token terminal aggregates fed by the switch's
/// termination observer.
#[derive(Debug, Default)]
struct ScopeState {
    rids: ScopeMap,
    /// token_id → terminal aggregate. BTreeMap for a deterministic summary
    /// row order.
    totals: BTreeMap<String, StepFlowTotals>,
}

/// How many retired scopes' terminal aggregates are retained. The pipeline's
/// guard retires a scope the moment it returns; the interpreter reads the
/// summary immediately after — the ring only needs to bridge that gap, and
/// the bound keeps abandoned summaries from accumulating.
const RETIRED_SCOPE_CAP: usize = 16;

/// Registry of live run scopes. Registration happens on the execution path
/// (cheap: one map insert per invocation); terminal aggregation happens on
/// the switch's termination path (cheap: one map fold per termination);
/// snapshots happen on the poll path.
#[derive(Debug, Default)]
pub struct RunFlowRegistry {
    scopes: RwLock<HashMap<String, ScopeState>>,
    /// Recently retired scopes' terminal aggregates, oldest first. A scope's
    /// aggregates move here when the guard deregisters it, so the write point
    /// (which runs after the pipeline — and therefore the guard — returns)
    /// can still read the finished run's summary.
    retired: RwLock<VecDeque<(String, BTreeMap<String, StepFlowTotals>)>>,
    /// FEED-BEARING request ids (their input carries a live reference the
    /// cartridge resolved into an open device tap). The stop-input control
    /// (15.2 §Runs Stop) drains: it takes this set and sends each rid a
    /// non-force Cancel — the runtime closes the taps and the requests end
    /// naturally with the machine's outputs intact.
    feed_bearing: RwLock<Vec<crate::MessageId>>,
}

impl RunFlowRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a FEED-BEARING rid (see `feed_bearing`). Called by the shared
    /// executor through the `FlowObserver` seam at invocation setup.
    pub fn record_feed_bearing(&self, rid: &crate::MessageId) {
        self.feed_bearing
            .write()
            .expect("run-flow registry poisoned")
            .push(rid.clone());
    }

    /// Take (and clear) every recorded feed-bearing rid — the stop-input
    /// control consumes the set exactly once per stop.
    pub fn take_feed_bearing(&self) -> Vec<crate::MessageId> {
        std::mem::take(
            &mut *self
                .feed_bearing
                .write()
                .expect("run-flow registry poisoned"),
        )
    }

    /// Record one invocation: `rid` executes strand step `token_id` within
    /// `scope`. Called at REQ-minting time.
    pub fn record(&self, scope: &str, rid: String, token_id: String) {
        let mut scopes = self.scopes.write().expect("run-flow registry poisoned");
        scopes
            .entry(scope.to_string())
            .or_default()
            .rids
            .insert(rid, token_id);
    }

    /// Drop a scope's correlation state. Called when the run's pipeline
    /// returns (any path) — after this, snapshots for the scope are empty.
    /// The scope's terminal aggregates move to the bounded retired ring so
    /// the run's durable summary can still be taken after the pipeline
    /// returns.
    pub fn deregister_scope(&self, scope: &str) {
        let removed = self
            .scopes
            .write()
            .expect("run-flow registry poisoned")
            .remove(scope);
        let Some(state) = removed else {
            return;
        };
        if state.totals.is_empty() {
            return;
        }
        let mut retired = self.retired.write().expect("run-flow registry poisoned");
        retired.retain(|(retired_scope, _)| retired_scope != scope);
        if retired.len() == RETIRED_SCOPE_CAP {
            retired.pop_front();
        }
        retired.push_back((scope.to_string(), state.totals));
    }

    /// The scope (machine run id) a rid was registered under, if its run's
    /// pipeline is still live. Linear scan over live scopes — a handful at
    /// most, on the poll path only.
    pub fn scope_of(&self, rid: &str) -> Option<String> {
        let scopes = self.scopes.read().expect("run-flow registry poisoned");
        scopes
            .iter()
            .find(|(_, state)| state.rids.contains_key(rid))
            .map(|(scope, _)| scope.clone())
    }

    /// Clone the scope's rid→token map, or `None` if the scope is unknown.
    pub fn scope_map(&self, scope: &str) -> Option<ScopeMap> {
        self.scopes
            .read()
            .expect("run-flow registry poisoned")
            .get(scope)
            .map(|state| state.rids.clone())
    }

    /// Fold one termination into its scope's per-token terminal aggregates.
    /// Installed as the switch's termination observer — runs synchronously
    /// under the request-table guard, so it must stay cheap: one linear scan
    /// over the handful of live scopes, one map fold, no I/O. Terminations
    /// whose rid belongs to no live scope (background chatter, already
    /// deregistered runs) are ignored.
    pub fn accumulate_termination(&self, summary: &crate::TerminatedSummary) {
        let mut scopes = self.scopes.write().expect("run-flow registry poisoned");
        for state in scopes.values_mut() {
            let Some(token_id) = state.rids.get(&summary.rid) else {
                continue;
            };
            let totals = state.totals.entry(token_id.clone()).or_default();
            totals.invocations += 1;
            match summary.kind {
                TerminalKind::End => totals.completed += 1,
                TerminalKind::Err | TerminalKind::Cancelled | TerminalKind::MasterDied => {
                    totals.failed += 1
                }
            }
            totals.frames_in += summary.frames_in;
            totals.frames_out += summary.frames_out;
            totals.bytes_in += summary.bytes_in;
            totals.bytes_out += summary.bytes_out;
            totals.total_lifetime_ms += summary.lifetime_ms;
            if terminal_kind_rank(&summary.kind) > terminal_kind_rank(&totals.worst_kind) {
                totals.worst_kind = summary.kind;
                totals.cancel_code = summary.cancel_code.clone();
                totals.cancel_class = summary.cancel_class;
                totals.cancel_reason = summary.cancel_reason.clone();
            }
            return;
        }
    }

    /// The scope's terminal aggregates as durable `RunFlowState` rows — the
    /// run's flow summary, in the same wire shape as the live flow_states so
    /// the gRPC layer serves history and live state through one field.
    /// A pure read: taking never removes state (the guard deregisters; the
    /// retired ring evicts), so repeated takes return the same rows. Empty
    /// when the scope is unknown or nothing terminated under it.
    pub fn take_flow_summary(&self, scope: &str) -> Vec<RunFlowState> {
        {
            let scopes = self.scopes.read().expect("run-flow registry poisoned");
            if let Some(state) = scopes.get(scope) {
                return totals_to_flow_states(&state.totals);
            }
        }
        self.retired
            .read()
            .expect("run-flow registry poisoned")
            .iter()
            .rev()
            .find(|(retired_scope, _)| retired_scope == scope)
            .map(|(_, totals)| totals_to_flow_states(totals))
            .unwrap_or_default()
    }
}

/// Convert a scope's per-token terminal aggregates into summary
/// `RunFlowState` rows: terminal phase from the worst kind, lifetime as
/// age, and one ended stream carrying the step's frame/byte totals.
fn totals_to_flow_states(totals: &BTreeMap<String, StepFlowTotals>) -> Vec<RunFlowState> {
    totals
        .iter()
        .map(|(token_id, step)| RunFlowState {
            token_id: token_id.clone(),
            phase: terminal_phase(&step.worst_kind).to_string(),
            cancel_code: step.cancel_code.clone(),
            cancel_class: step.cancel_class.map(|c| c.as_str().to_string()),
            cancel_reason: step.cancel_reason.clone(),
            age_ms: step.total_lifetime_ms,
            idle_ms: 0,
            active_requests: 0,
            completed_requests: step.invocations,
            streams: vec![RunStreamFlow {
                stream_id: None,
                frames_in: step.frames_in,
                frames_out: step.frames_out,
                bytes_in: step.bytes_in,
                bytes_out: step.bytes_out,
                chunks_in: 0,
                chunks_out: 0,
                credit_outstanding: 0,
                unbounded: false,
                ended: true,
            }],
        })
        .collect()
}

/// A scope-bound recorder handed to the execution path so it needs neither
/// the scope string nor the registry plumbed separately.
#[derive(Clone)]
pub struct FlowRecorder {
    scope: String,
    registry: Arc<RunFlowRegistry>,
}

impl FlowRecorder {
    pub fn new(scope: String, registry: Arc<RunFlowRegistry>) -> Self {
        Self { scope, registry }
    }

    pub fn record(&self, rid: &crate::MessageId, token_id: &str) {
        self.registry
            .record(&self.scope, rid.to_string(), token_id.to_string());
    }
}

/// The shared segment executor (`crate::run_dag_on_context`) correlates each
/// invocation's `rid` to its strand step through this trait — the one point
/// both ids exist. Every host that observes a run's flow wires one per run.
impl crate::FlowObserver for FlowRecorder {
    fn record(&self, rid: &crate::MessageId, token_id: &str) {
        FlowRecorder::record(self, rid, token_id)
    }

    fn record_feed_bearing(&self, rid: &crate::MessageId) {
        self.registry.record_feed_bearing(rid);
    }
}

/// Scope guard: deregisters the scope when the pipeline returns, on every
/// path (success, error, cancel, panic-unwind).
pub struct FlowScopeGuard {
    scope: String,
    registry: Arc<RunFlowRegistry>,
}

impl FlowScopeGuard {
    pub fn new(scope: String, registry: Arc<RunFlowRegistry>) -> Self {
        Self { scope, registry }
    }

    pub fn recorder(&self) -> FlowRecorder {
        FlowRecorder::new(self.scope.clone(), self.registry.clone())
    }
}

impl Drop for FlowScopeGuard {
    fn drop(&mut self) {
        self.registry.deregister_scope(&self.scope);
    }
}

fn terminal_kind_rank(kind: &TerminalKind) -> u8 {
    match kind {
        TerminalKind::Err => 3,
        TerminalKind::Cancelled => 2,
        TerminalKind::MasterDied => 1,
        TerminalKind::End => 0,
    }
}

fn terminal_phase(kind: &TerminalKind) -> &'static str {
    match kind {
        TerminalKind::Err => "err",
        TerminalKind::Cancelled => "cancelled",
        TerminalKind::MasterDied => "master_died",
        TerminalKind::End => "done",
    }
}

/// Aggregate live stream snapshots per stream id (summing across a step's
/// concurrent invocations). `ended` requires ALL contributors ended;
/// `unbounded` if ANY contributor is unbounded.
fn merge_streams(streams: &[&StreamSnapshot]) -> Vec<RunStreamFlow> {
    let mut merged: BTreeMap<Option<String>, RunStreamFlow> = BTreeMap::new();
    for snapshot in streams {
        let entry = merged
            .entry(snapshot.stream_id.clone())
            .or_insert_with(|| RunStreamFlow {
                stream_id: snapshot.stream_id.clone(),
                frames_in: 0,
                frames_out: 0,
                bytes_in: 0,
                bytes_out: 0,
                chunks_in: 0,
                chunks_out: 0,
                credit_outstanding: 0,
                unbounded: false,
                ended: true,
            });
        entry.frames_in += snapshot.stats.frames_in;
        entry.frames_out += snapshot.stats.frames_out;
        entry.bytes_in += snapshot.stats.bytes_in;
        entry.bytes_out += snapshot.stats.bytes_out;
        entry.chunks_in += snapshot.stats.chunks_in;
        entry.chunks_out += snapshot.stats.chunks_out;
        entry.credit_outstanding += snapshot.stats.credit_outstanding;
        entry.unbounded |= snapshot.stats.unbounded;
        entry.ended &= snapshot.stats.ended;
    }
    merged.into_values().collect()
}

/// Join a scope's rid→token map against a switch protocol snapshot, producing
/// one `RunFlowState` per step that has at least one registered invocation.
/// Pure — unit-testable without a switch.
pub fn join_flow_states(
    scope_map: &ScopeMap,
    stats: &RelaySwitchProtocolStats,
) -> Vec<RunFlowState> {
    // token → collected evidence
    struct StepAccum<'a> {
        live: Vec<&'a crate::RequestSnapshot>,
        terminated: Vec<&'a crate::TerminatedSummary>,
        registered: u32,
    }
    let mut steps: BTreeMap<&str, StepAccum> = BTreeMap::new();
    for token_id in scope_map.values() {
        let accum = steps.entry(token_id.as_str()).or_insert(StepAccum {
            live: Vec::new(),
            terminated: Vec::new(),
            registered: 0,
        });
        accum.registered += 1;
    }
    for request in &stats.requests.active {
        if let Some(token_id) = scope_map.get(&request.rid) {
            steps
                .get_mut(token_id.as_str())
                .expect("token present: built from the same scope map")
                .live
                .push(request);
        }
    }
    for terminated in &stats.requests.recent_terminated {
        if let Some(token_id) = scope_map.get(&terminated.rid) {
            steps
                .get_mut(token_id.as_str())
                .expect("token present: built from the same scope map")
                .terminated
                .push(terminated);
        }
    }

    steps
        .into_iter()
        .map(|(token_id, accum)| {
            let active_requests = accum.live.len() as u32;
            // L7: a registered rid absent from the active table has
            // terminated (register-once/terminate-once) — the ring may have
            // evicted its summary, but the count stays exact.
            let completed_requests = accum.registered.saturating_sub(active_requests);

            let worst_terminated = accum
                .terminated
                .iter()
                .max_by_key(|t| terminal_kind_rank(&t.kind))
                .copied();
            let phase = if let Some(worst_live) = accum
                .live
                .iter()
                .map(|r| match r.phase {
                    RequestPhase::Streaming => 1u8,
                    RequestPhase::Created => 0u8,
                })
                .max()
            {
                if worst_live == 1 { "streaming" } else { "created" }
            } else if completed_requests > 0 {
                worst_terminated
                    .map(|t| terminal_phase(&t.kind))
                    .unwrap_or("done")
            } else {
                // Registered but neither live nor terminated is impossible
                // under L7 (registration precedes table insertion only within
                // execute_cap, which registers atomically). Registered-only
                // entries therefore mean the snapshot raced the REQ: report
                // created, the truthful pre-streaming phase.
                "created"
            }
            .to_string();
            // The cancel attribution travels with the phase it explains:
            // only a terminal `cancelled` phase carries one.
            let (cancel_code, cancel_class, cancel_reason) = match worst_terminated {
                Some(t) if phase == "cancelled" => (
                    t.cancel_code.clone(),
                    t.cancel_class.map(|c| c.as_str().to_string()),
                    t.cancel_reason.clone(),
                ),
                _ => (None, None, None),
            };

            let age_ms = accum
                .live
                .iter()
                .map(|r| r.age_ms)
                .max()
                .or_else(|| accum.terminated.iter().map(|t| t.lifetime_ms).max())
                .unwrap_or(0);
            let idle_ms = accum.live.iter().map(|r| r.idle_ms).min().unwrap_or(0);

            let live_streams: Vec<&StreamSnapshot> =
                accum.live.iter().flat_map(|r| r.streams.iter()).collect();

            RunFlowState {
                token_id: token_id.to_string(),
                phase,
                cancel_code,
                cancel_class,
                cancel_reason,
                age_ms,
                idle_ms,
                active_requests,
                completed_requests,
                streams: merge_streams(&live_streams),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DropCounters, RequestSnapshot, RequestTableSnapshot, StreamFlowStats, TerminatedSummary,
    };

    fn stream(id: Option<&str>, bytes_in: u64, credit: i64, ended: bool) -> StreamSnapshot {
        StreamSnapshot {
            stream_id: id.map(|s| s.to_string()),
            stats: StreamFlowStats {
                frames_in: 2,
                frames_out: 1,
                bytes_in,
                bytes_out: bytes_in / 2,
                chunks_in: 1,
                chunks_out: 1,
                credit_outstanding: credit,
                unbounded: false,
                ended,
            },
        }
    }

    fn live(rid: &str, phase: RequestPhase, age_ms: u64, idle_ms: u64, streams: Vec<StreamSnapshot>) -> RequestSnapshot {
        RequestSnapshot {
            xid: "x1".into(),
            rid: rid.into(),
            cap_urn: None,
            phase,
            is_peer: false,
            origin_master: None,
            destination_master: 0,
            age_ms,
            idle_ms,
            children: 0,
            streams,
        }
    }

    fn terminated(rid: &str, kind: TerminalKind, lifetime_ms: u64) -> TerminatedSummary {
        TerminatedSummary {
            xid: "x1".into(),
            rid: rid.into(),
            cap_urn: None,
            kind,
            cancel_code: None,
            cancel_class: None,
            cancel_reason: None,
            is_peer: false,
            lifetime_ms,
            frames_in: 10,
            frames_out: 4,
            bytes_in: 100,
            bytes_out: 50,
        }
    }

    fn snapshot(
        active: Vec<RequestSnapshot>,
        recent_terminated: Vec<TerminatedSummary>,
    ) -> RelaySwitchProtocolStats {
        RelaySwitchProtocolStats {
            requests: RequestTableSnapshot {
                active,
                recent_terminated,
                total_registered: 0,
                terminated_by_kind: Default::default(),
            },
            drops: DropCounters::new().snapshot(),
            stragglers: Default::default(),
            hosts: Default::default(),
        }
    }

    // TEST13000: join_flow_states aggregates a step's CONCURRENT invocations
    // (ForEach bodies): counts split live/completed, streams sum by stream id,
    // ended only when all contributors ended, and streaming wins over created.
    #[test]
    fn test13000_join_aggregates_concurrent_invocations_per_step() {
        let mut scope = ScopeMap::new();
        scope.insert("r1".into(), "tok-a".into());
        scope.insert("r2".into(), "tok-a".into());
        scope.insert("r3".into(), "tok-a".into());

        let stats = snapshot(
            vec![
                live("r1", RequestPhase::Streaming, 500, 40, vec![stream(Some("in"), 100, 2, false)]),
                live("r2", RequestPhase::Created, 300, 10, vec![stream(Some("in"), 60, 1, true)]),
            ],
            vec![terminated("r3", TerminalKind::End, 900)],
        );

        let states = join_flow_states(&scope, &stats);
        assert_eq!(states.len(), 1);
        let state = &states[0];
        assert_eq!(state.token_id, "tok-a");
        assert_eq!(state.phase, "streaming", "any live streaming invocation wins");
        assert_eq!(state.active_requests, 2);
        assert_eq!(state.completed_requests, 1, "L7: registered − live");
        assert_eq!(state.age_ms, 500, "oldest live invocation");
        assert_eq!(state.idle_ms, 10, "most recent activity across live invocations");
        assert_eq!(state.streams.len(), 1);
        let flow = &state.streams[0];
        assert_eq!(flow.bytes_in, 160, "streams sum across invocations");
        assert_eq!(flow.credit_outstanding, 3);
        assert!(!flow.ended, "ended only when ALL contributors ended");
    }

    // TEST13001: a fully-terminated step reports the WORST terminal kind among
    // its invocations (err > cancelled > master_died > done), a ring-evicted
    // terminal degrades to done (never invented), and rids from other scopes
    // never leak into the join.
    #[test]
    fn test13001_terminal_phase_is_worst_kind_and_scope_bounded() {
        let mut scope = ScopeMap::new();
        scope.insert("r1".into(), "tok-a".into());
        scope.insert("r2".into(), "tok-a".into());
        scope.insert("r4".into(), "tok-b".into());

        let stats = snapshot(
            vec![
                // Foreign rid: same switch, different run — must not leak.
                live("r-foreign", RequestPhase::Streaming, 1, 1, vec![]),
            ],
            vec![
                terminated("r1", TerminalKind::End, 400),
                terminated("r2", TerminalKind::Err, 700),
                // r4 terminated but its summary aged out of the ring.
            ],
        );

        let states = join_flow_states(&scope, &stats);
        assert_eq!(states.len(), 2, "one state per registered step, none for foreign rids");

        let a = states.iter().find(|s| s.token_id == "tok-a").unwrap();
        assert_eq!(a.phase, "err", "worst terminal kind wins");
        assert_eq!(a.active_requests, 0);
        assert_eq!(a.completed_requests, 2);
        assert_eq!(a.age_ms, 700, "longest lifetime among terminated");
        assert!(a.streams.is_empty(), "terminated invocations carry no live streams");

        let b = states.iter().find(|s| s.token_id == "tok-b").unwrap();
        assert_eq!(b.phase, "done", "ring-evicted terminal degrades to done, never invented");
        assert_eq!(b.completed_requests, 1);
    }

    // TEST13002: the registry's scope lifecycle — record/snapshot/deregister —
    // and the guard's drop-based cleanup. After deregistration a scope
    // snapshot is empty (the UI stops receiving flow states, not stale ones).
    #[test]
    fn test13002_registry_scope_lifecycle_and_guard_cleanup() {
        let registry = Arc::new(RunFlowRegistry::new());
        {
            let guard = FlowScopeGuard::new("run-1".into(), registry.clone());
            let recorder = guard.recorder();
            recorder.record(&crate::MessageId::Uint(7), "tok-a");
            let map = registry.scope_map("run-1").expect("scope registered");
            assert_eq!(map.get("7").map(String::as_str), Some("tok-a"));
            assert!(registry.scope_map("run-2").is_none(), "unknown scopes are None, not empty");
        }
        assert!(
            registry.scope_map("run-1").is_none(),
            "guard drop must deregister the scope on every exit path"
        );
    }

    fn terminated_with(
        rid: &str,
        kind: TerminalKind,
        lifetime_ms: u64,
        frames: (u64, u64),
        bytes: (u64, u64),
    ) -> TerminatedSummary {
        TerminatedSummary {
            xid: "x1".into(),
            rid: rid.into(),
            cap_urn: None,
            kind,
            cancel_code: None,
            cancel_class: None,
            cancel_reason: None,
            is_peer: false,
            lifetime_ms,
            frames_in: frames.0,
            frames_out: frames.1,
            bytes_in: bytes.0,
            bytes_out: bytes.1,
        }
    }

    // TEST13003: durable per-run flow summary — accumulate_termination
    // attributes each termination by rid→scope→token (unknown rids are
    // no-ops, foreign scopes untouched), counts completed vs failed, sums
    // frames/bytes/lifetime, and ranks worst_kind (err > cancelled >
    // master_died > done). take_flow_summary renders terminal-phase rows
    // with a single ended stream carrying the totals, is an idempotent read
    // (multiple takes, same rows), and survives the guard's deregistration
    // — the write point runs AFTER the pipeline returns.
    #[test]
    fn test13003_terminal_aggregates_accumulate_and_summarize_durably() {
        let registry = Arc::new(RunFlowRegistry::new());
        registry.record("run-1", "r1".into(), "tok-a".into());
        registry.record("run-1", "r2".into(), "tok-a".into());
        registry.record("run-1", "r3".into(), "tok-b".into());
        registry.record("run-2", "r9".into(), "tok-z".into());

        // Terminations arrive via the switch observer, one per invocation.
        registry.accumulate_termination(&terminated_with(
            "r1",
            TerminalKind::End,
            400,
            (10, 4),
            (100, 50),
        ));
        registry.accumulate_termination(&terminated_with(
            "r2",
            TerminalKind::Cancelled,
            300,
            (6, 2),
            (60, 10),
        ));
        registry.accumulate_termination(&terminated_with(
            "r3",
            TerminalKind::End,
            250,
            (3, 3),
            (30, 30),
        ));
        // Unknown rid: registered under no live scope — must be a no-op.
        registry.accumulate_termination(&terminated_with(
            "r-unknown",
            TerminalKind::Err,
            999,
            (99, 99),
            (999, 999),
        ));

        let summary = registry.take_flow_summary("run-1");
        assert_eq!(summary.len(), 2, "one row per token, unknown rid ignored");

        let a = summary.iter().find(|s| s.token_id == "tok-a").unwrap();
        assert_eq!(
            a.phase, "cancelled",
            "worst terminal kind wins (cancelled outranks done)"
        );
        assert_eq!(a.completed_requests, 2, "completed_requests = invocations");
        assert_eq!(a.active_requests, 0);
        assert_eq!(a.age_ms, 700, "total lifetime across invocations");
        assert_eq!(a.idle_ms, 0);
        assert_eq!(a.streams.len(), 1, "one summary stream per step");
        let flow = &a.streams[0];
        assert_eq!(flow.stream_id, None);
        assert_eq!(flow.frames_in, 16);
        assert_eq!(flow.frames_out, 6);
        assert_eq!(flow.bytes_in, 160, "bytes sum across invocations");
        assert_eq!(flow.bytes_out, 60);
        assert!(flow.ended, "summary streams are terminal");
        assert!(!flow.unbounded);

        let b = summary.iter().find(|s| s.token_id == "tok-b").unwrap();
        assert_eq!(b.phase, "done", "End renders as done");
        assert_eq!(b.completed_requests, 1);

        // err outranks cancelled: a later worse termination upgrades the phase.
        registry.accumulate_termination(&terminated_with(
            "r1",
            TerminalKind::Err,
            100,
            (0, 0),
            (0, 0),
        ));
        let a_phase = registry
            .take_flow_summary("run-1")
            .into_iter()
            .find(|s| s.token_id == "tok-a")
            .unwrap()
            .phase;
        assert_eq!(a_phase, "err", "err outranks every other terminal kind");

        // The foreign scope accumulated nothing from run-1's terminations.
        assert!(
            registry.take_flow_summary("run-2").is_empty(),
            "no terminations under run-2 yet, so no summary rows"
        );

        // Deregistration (the guard's drop) retires the aggregates but the
        // summary must still be takable — the interpreter persists AFTER the
        // pipeline (and guard) returns — and takes stay idempotent.
        registry.deregister_scope("run-1");
        assert!(registry.scope_map("run-1").is_none());
        let first_take = registry.take_flow_summary("run-1");
        assert_eq!(first_take.len(), 2, "summary survives deregistration");
        let second_take = registry.take_flow_summary("run-1");
        assert_eq!(
            first_take, second_take,
            "take_flow_summary is an idempotent read"
        );
    }

    // TEST13004: the cancel ATTRIBUTION travels with the `cancelled` phase —
    // in the durable summary (the worst termination's code, class and reason)
    // and in the live join (the ring entry's) — and is absent for every other
    // phase, so a surface can say "aborted — step X failed" for a collateral
    // teardown and reserve "Cancelled" for a user's cancel.
    #[test]
    fn test13004_cancel_attribution_travels_with_the_cancelled_phase() {
        let registry = Arc::new(RunFlowRegistry::new());
        registry.record("run-1", "r1".into(), "tok-a".into());
        registry.record("run-1", "r2".into(), "tok-b".into());

        let mut collateral = terminated_with("r1", TerminalKind::Cancelled, 10, (1, 1), (1, 1));
        collateral.cancel_code = Some("ABORTED_COLLATERAL".into());
        collateral.cancel_class = Some(crate::AttributionClass::Resource);
        collateral.cancel_reason = Some("step tok-b (cap:gen) failed: GPU_OUT_OF_MEMORY".into());
        registry.accumulate_termination(&collateral);
        registry.accumulate_termination(&terminated_with(
            "r2",
            TerminalKind::Err,
            10,
            (1, 1),
            (1, 1),
        ));

        let summary = registry.take_flow_summary("run-1");
        let a = summary.iter().find(|s| s.token_id == "tok-a").unwrap();
        assert_eq!(a.phase, "cancelled");
        assert_eq!(a.cancel_code.as_deref(), Some("ABORTED_COLLATERAL"));
        assert_eq!(a.cancel_class.as_deref(), Some("resource"));
        assert_eq!(
            a.cancel_reason.as_deref(),
            Some("step tok-b (cap:gen) failed: GPU_OUT_OF_MEMORY")
        );
        let b = summary.iter().find(|s| s.token_id == "tok-b").unwrap();
        assert_eq!(b.phase, "err");
        assert_eq!(b.cancel_code, None);
        assert_eq!(b.cancel_class, None);
        assert_eq!(b.cancel_reason, None);

        // Live join: the ring's cancelled entry carries its attribution; a
        // live step does not.
        let scope_map = registry.scope_map("run-1").unwrap();
        let stats = snapshot(vec![], vec![collateral.clone()]);
        let states = join_flow_states(&scope_map, &stats);
        let a = states.iter().find(|s| s.token_id == "tok-a").unwrap();
        assert_eq!(a.phase, "cancelled");
        assert_eq!(a.cancel_code.as_deref(), Some("ABORTED_COLLATERAL"));
        assert_eq!(a.cancel_class.as_deref(), Some("resource"));
        assert!(a.cancel_reason.is_some());
    }

    // TEST13005: the FEED-BEARING rid registry behind the stop-input control
    // (15.2 §Runs Stop) — recording accumulates, `take` consumes exactly
    // once (a stop must not re-cancel rids a later stop never recorded),
    // and the recorder seam feeds it through `FlowObserver`.
    #[test]
    fn test13005_feed_bearing_rids_record_and_take_once() {
        let registry = Arc::new(RunFlowRegistry::new());
        let rid_a = crate::MessageId::new_uuid();
        let rid_b = crate::MessageId::new_uuid();

        // Through the observer seam, exactly as the shared executor reports.
        let recorder = FlowRecorder::new("scope-a".to_string(), registry.clone());
        crate::FlowObserver::record_feed_bearing(&recorder, &rid_a);
        registry.record_feed_bearing(&rid_b);

        let taken = registry.take_feed_bearing();
        assert_eq!(taken.len(), 2, "both feed-bearing rids are taken");
        assert!(taken.contains(&rid_a) && taken.contains(&rid_b));
        assert!(
            registry.take_feed_bearing().is_empty(),
            "take consumes — a second stop has nothing to re-cancel"
        );
    }
}
