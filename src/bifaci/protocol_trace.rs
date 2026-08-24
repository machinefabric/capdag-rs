//! Per-segment protocol trace sink for the reference runtime.
//!
//! The engine's dev trace (`floom-engine/src/cap/protocol_trace.rs`) samples a
//! LONG-LIVED relay switch every 2s and writes transition-deduped JSONL. The
//! capdag CLI runtime ([`crate::orchestrator::cli_runtime::CliRuntime`]) reuses a
//! long-lived switch too, but the trace is scoped PER SEGMENT: the shared
//! [`EngineRuntime::run_segment`](crate::orchestrator::execute_plan::EngineRuntime)
//! both SAMPLES the switch live during the segment (a 250ms sampler) and captures
//! a final SNAPSHOT at teardown — every line carries the switch's
//! [`RelaySwitchProtocolStats`], the same information the Protocol Health view
//! shows. Live sampling is what makes a HANGING segment observable: the last line
//! written before the harness kills it shows the stalled active request with its
//! per-stream credit/flow counters.
//!
//! Line schema (JSONL, one object per line):
//! ```json
//! { "ts": <unix millis>, "segment": <label>, "stats": <RelaySwitchProtocolStats> }
//! ```
//!
//! Lines are deduped by a transition fingerprint that EXCLUDES ever-advancing
//! clocks (ages/idle/lifetime), so an idle or stalled engine does not spam
//! identical samples — one line per protocol transition, mirroring floom-engine's
//! `trace_fingerprint`.
//!
//! This is diagnostics the user explicitly asked for (a `--trace`/env path):
//! the FINAL snapshot's serialize and I/O errors are HARD errors surfaced to the
//! caller. A LIVE sample's write failure is logged and ignored — a mid-run trace
//! hiccup must never abort execution.

use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::io::AsyncWriteExt;

use crate::bifaci::relay_switch::RelaySwitchProtocolStats;

/// A failure to write a protocol trace line. Both variants are hard errors: the
/// trace was requested, so a write that cannot happen is reported, not dropped.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolTraceError {
    /// The trace file could not be opened or written.
    #[error("protocol trace I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The snapshot could not be serialized to JSON.
    #[error("protocol trace serialize error: {0}")]
    Serialize(#[from] serde_json::Error),
    /// The system clock is before the Unix epoch (cannot timestamp the line).
    #[error("protocol trace clock error: {0}")]
    Clock(#[from] std::time::SystemTimeError),
}

/// One JSONL trace line. A dedicated `Serialize` struct (rather than an ad-hoc
/// `json!`) so a stats-serialization failure surfaces as a real error.
#[derive(serde::Serialize)]
struct TraceLine<'a> {
    /// Capture time, Unix milliseconds.
    ts: u64,
    /// Identifies the segment this snapshot belongs to (e.g. the terminal cap URN).
    segment: &'a str,
    /// The switch's full protocol snapshot for the segment.
    stats: &'a RelaySwitchProtocolStats,
}

/// The sink's mutable state, guarded by ONE mutex so the dedup check and the
/// write are atomic across the concurrent live sampler and the final snapshot.
struct SinkState {
    file: tokio::fs::File,
    /// Fingerprint of the last line actually written; `None` before the first.
    last_fingerprint: Option<String>,
}

/// An append-only JSONL sink for per-segment protocol snapshots. Cheap to share
/// (`Arc`) so the same sink serves both the live sampler and the final snapshot.
pub struct ProtocolTraceSink {
    state: tokio::sync::Mutex<SinkState>,
}

/// Transition fingerprint: everything the snapshot says that MATTERS, EXCLUDING
/// the ever-advancing clocks (a request's `age_ms`/`idle_ms`, a termination's
/// `lifetime_ms`) which change every sample and would defeat dedup. Mirrors
/// floom-engine's `floom-engine/src/cap/protocol_trace.rs::trace_fingerprint` so both
/// traces dedup on the same notion of "a protocol transition".
fn trace_fingerprint(stats: &RelaySwitchProtocolStats) -> String {
    let active: Vec<serde_json::Value> = stats
        .requests
        .active
        .iter()
        .map(|r| {
            serde_json::json!({
                "rid": r.rid,
                "cap": r.cap_urn,
                "phase": r.phase,
                "children": r.children,
                "streams": r.streams.iter().map(|s| {
                    serde_json::json!({
                        "id": s.stream_id,
                        "fi": s.stats.frames_in,
                        "fo": s.stats.frames_out,
                        "bi": s.stats.bytes_in,
                        "bo": s.stats.bytes_out,
                        "credit": s.stats.credit_outstanding,
                        "unbounded": s.stats.unbounded,
                        "ended": s.stats.ended,
                    })
                }).collect::<Vec<_>>(),
            })
        })
        .collect();
    serde_json::json!({
        "total_registered": stats.requests.total_registered,
        "terminated_by_kind": stats.requests.terminated_by_kind,
        "terminated_len": stats.requests.recent_terminated.len(),
        "last_terminated": stats.requests.recent_terminated.last().map(|t| (&t.rid, t.kind.as_str())),
        "drops": stats.drops,
        "stragglers": stats.stragglers,
        "hosts": stats.hosts,
        "active": active,
    })
    .to_string()
}

impl ProtocolTraceSink {
    /// Open `path` for append, creating it if absent. A failure to open (bad
    /// directory, no permission) is a hard error — the caller asked for a trace.
    pub async fn open(path: impl AsRef<Path>) -> Result<Arc<Self>, ProtocolTraceError> {
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.as_ref())
            .await?;
        Ok(Arc::new(Self {
            state: tokio::sync::Mutex::new(SinkState {
                file,
                last_fingerprint: None,
            }),
        }))
    }

    /// Append one JSONL line `{ ts, segment, stats }`, then flush. The trace must
    /// be complete on disk even if the process is killed right after a failing
    /// segment. Caller holds the state lock.
    async fn write_line(
        state: &mut SinkState,
        stats: &RelaySwitchProtocolStats,
        segment_label: &str,
    ) -> Result<(), ProtocolTraceError> {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
        let line = TraceLine {
            ts,
            segment: segment_label,
            stats,
        };
        let mut buf = serde_json::to_vec(&line)?;
        buf.push(b'\n');
        state.file.write_all(&buf).await?;
        state.file.flush().await?;
        Ok(())
    }

    /// Append one line unconditionally (no dedup). Serialize, clock, and I/O
    /// failures are returned to the caller (this is requested diagnostics; a
    /// silently dropped line would hide the very problem the trace exposes).
    pub async fn record(
        &self,
        stats: &RelaySwitchProtocolStats,
        segment_label: &str,
    ) -> Result<(), ProtocolTraceError> {
        let mut state = self.state.lock().await;
        Self::write_line(&mut state, stats, segment_label).await?;
        // Keep the fingerprint coherent so a later `record_deduped` compares
        // against what is actually on disk.
        state.last_fingerprint = Some(trace_fingerprint(stats));
        Ok(())
    }

    /// Append one line ONLY when the protocol state changed since the last line
    /// written — so an idle or stalled engine leaves the trace silent instead of
    /// spamming identical samples. The fingerprint check and the write share one
    /// lock, so concurrent samplers cannot interleave a duplicate.
    pub async fn record_deduped(
        &self,
        stats: &RelaySwitchProtocolStats,
        segment_label: &str,
    ) -> Result<(), ProtocolTraceError> {
        let fingerprint = trace_fingerprint(stats);
        let mut state = self.state.lock().await;
        if state.last_fingerprint.as_deref() == Some(fingerprint.as_str()) {
            return Ok(());
        }
        Self::write_line(&mut state, stats, segment_label).await?;
        state.last_fingerprint = Some(fingerprint);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bifaci::request_state::{
        RequestPhase, RequestSnapshot, RequestTableSnapshot, StreamFlowStats, StreamSnapshot,
    };
    use crate::bifaci::stats::DropCounters;

    fn empty_stats(total_registered: u64) -> RelaySwitchProtocolStats {
        RelaySwitchProtocolStats {
            requests: RequestTableSnapshot {
                active: vec![],
                recent_terminated: vec![],
                total_registered,
                terminated_by_kind: Default::default(),
            },
            drops: DropCounters::new().snapshot(),
            stragglers: Default::default(),
            hosts: Default::default(),
        }
    }

    /// A snapshot with one active request, so age/idle clocks are present to test
    /// that the fingerprint ignores them while flow counters are significant.
    fn active_stats(age_ms: u64, idle_ms: u64, bytes_in: u64) -> RelaySwitchProtocolStats {
        RelaySwitchProtocolStats {
            requests: RequestTableSnapshot {
                active: vec![RequestSnapshot {
                    xid: "1".into(),
                    rid: "9".into(),
                    phase: RequestPhase::Streaming,
                    is_peer: false,
                    cap_urn: Some("cap:effect=none".into()),
                    origin_master: None,
                    destination_master: 0,
                    age_ms,
                    idle_ms,
                    children: 0,
                    streams: vec![StreamSnapshot {
                        stream_id: Some("in".into()),
                        stats: StreamFlowStats {
                            bytes_in,
                            ..Default::default()
                        },
                    }],
                }],
                recent_terminated: vec![],
                total_registered: 1,
                terminated_by_kind: Default::default(),
            },
            drops: DropCounters::new().snapshot(),
            stragglers: Default::default(),
            hosts: Default::default(),
        }
    }

    fn temp_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "capdag-protocol-trace-{}-{}-{}.trace",
            tag,
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    // TEST1312: Two snapshots recorded to a temp file produce exactly two JSONL lines,
    // each carrying ts + segment + a round-tripped stats object (requests/drops).
    #[tokio::test]
    async fn test1312_record_appends_one_json_line_per_snapshot() {
        let path = temp_path("roundtrip");
        let sink = ProtocolTraceSink::open(&path).await.expect("open sink");

        sink.record(&empty_stats(1), "seg-a")
            .await
            .expect("record 1");
        sink.record(&empty_stats(2), "seg-b")
            .await
            .expect("record 2");

        let contents = std::fs::read_to_string(&path).expect("read trace back");
        std::fs::remove_file(&path).ok();

        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2, "one JSONL line per recorded snapshot");

        let first: serde_json::Value = serde_json::from_str(lines[0]).expect("line 1 is JSON");
        assert!(first["ts"].is_u64(), "ts is a unix-millis integer");
        assert_eq!(first["segment"], "seg-a");
        assert_eq!(first["stats"]["requests"]["total_registered"], 1);
        assert!(
            first["stats"]["requests"].is_object() && first["stats"]["drops"].is_object(),
            "stats carries the requests + drops snapshots"
        );

        let second: serde_json::Value = serde_json::from_str(lines[1]).expect("line 2 is JSON");
        assert_eq!(second["segment"], "seg-b");
        assert_eq!(second["stats"]["requests"]["total_registered"], 2);
    }

    // TEST1313: Dedup: recording identical protocol state twice writes ONE line; a real
    // change (a bumped counter, a moved stream byte) writes another. This is what
    // keeps a stalled engine's repeated live samples from spamming the trace.
    #[tokio::test]
    async fn test1313_record_deduped_writes_only_on_change() {
        let path = temp_path("dedup");
        let sink = ProtocolTraceSink::open(&path).await.expect("open sink");

        sink.record_deduped(&empty_stats(1), "seg")
            .await
            .expect("first");
        // Identical state — must NOT write a second line.
        sink.record_deduped(&empty_stats(1), "seg")
            .await
            .expect("dup");
        // Changed counter — must write.
        sink.record_deduped(&empty_stats(2), "seg")
            .await
            .expect("changed");
        // A stream flow-counter change is also a transition.
        sink.record_deduped(&active_stats(10, 0, 512), "seg")
            .await
            .expect("active");

        let contents = std::fs::read_to_string(&path).expect("read trace back");
        std::fs::remove_file(&path).ok();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(
            lines.len(),
            3,
            "identical samples dedup to one line; each real change adds one"
        );
    }

    // The fingerprint EXCLUDES advancing clocks: two snapshots differing only in
    // TEST1314: `age_ms`/`idle_ms` are the same transition, while a flow-counter change is
    // a new one. If dedup keyed on the whole serialized stats, these clocks would
    // defeat it and every sample would write.
    #[test]
    fn test1314_fingerprint_ignores_advancing_clocks() {
        let a = active_stats(1000, 10, 512);
        let b = active_stats(9000, 8010, 512); // only age/idle advanced
        assert_eq!(
            trace_fingerprint(&a),
            trace_fingerprint(&b),
            "age/idle advancement alone is not a transition"
        );

        let c = active_stats(9000, 0, 1024); // bytes moved
        assert_ne!(
            trace_fingerprint(&a),
            trace_fingerprint(&c),
            "a flow-counter change is a transition"
        );
    }

    // TEST1315: Requested diagnostics fail HARD, never silently: a write to an unwritable
    // sink returns Err. `/dev/full` opens fine but every write is ENOSPC — the
    // Linux-standard way to exercise a write failure deterministically.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test1315_record_to_unwritable_path_is_a_hard_error() {
        let sink = ProtocolTraceSink::open("/dev/full")
            .await
            .expect("/dev/full opens for append");
        let err = sink
            .record(&empty_stats(1), "seg")
            .await
            .expect_err("writing to /dev/full must fail, not silently drop");
        assert!(
            matches!(err, ProtocolTraceError::Io(_)),
            "an unwritable trace surfaces as an I/O error, got {err:?}"
        );
    }
}
