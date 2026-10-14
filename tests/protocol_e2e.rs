//! Protocol end-to-end tests (explicit diagnostic attribution,
//! handler-capacity admission, credit-based flow control,
//! bidirectional streaming, terminal metadata on END, pipelined chains).
//!
//! These exercise the FULL stack with a real cartridge process:
//!
//! ```text
//!   test ←→ RelaySwitch ←→ RelaySlave ←→ CartridgeHostRuntime ←→ testcartridge
//! ```
//!
//! by the path a machine takes outside the tests — notation → plan →
//! `execute_plan` on a `CliRuntime` (`support::Machines`) — and, where a test
//! needs the switch itself (protocol stats, negotiated limits, a second
//! master), by running the plan's steps on a context it holds. The registry
//! is the cartridge's own manifest. The test cartridge is built by cargo,
//! against this capdag, once per test binary.
//!
//! # Spec coverage (docs/capdag-improvement/05-parity-test-spec.md,
//! "Remaining as end-to-end scenarios")
//!
//! Implemented in this file:
//! - TEST7054 — input-direction credit through a slow consumer
//! - TEST7056 — bidirectional streaming without deadlock
//! - TEST7059 — no request/stream state leaks after terminal END
//! - TEST7061 — negotiated initial_credit is the element-wise min
//!   (also the cross-process variant of TEST7055: the 48-chunk producer can
//!   only stream past the 32-chunk initial window via consumption grants)
//! - TEST7076 — pipelined chain ordering (downstream consumes before
//!   upstream finishes)
//!
//! Deferred to the capdag-interop suite (see
//! mfab-tests/capdag-interop/README.md, "Protocol scenarios"):
//! - TEST7057 (credit across a relay hop with XID rewriting): the
//!   orchestrator harness has exactly one slave hop, which every test here
//!   exercises implicitly; the multi-hop XID-rewrite assertion needs the
//!   interop matrix's host↔relay↔host topology.
//! - TEST7058 (cancel releases a credit-blocked sender): `run_dag_on_context`
//!   does not expose the request id mid-run, so a test cannot inject Cancel
//!   at a deterministic mid-transfer point at this layer. Covered at the
//!   substrate by TEST7016/TEST7018 (gate close releases waiters); the
//!   cross-process variant lands in interop.
//! - TEST7060 (peer-to-peer credit end-to-end): testcartridge's `test-peer`
//!   uses buffered `collect_value` on small payloads — a streaming
//!   peer-to-peer producer/consumer pair is an interop-suite scenario.
//! - TEST7074/TEST7075 (cancel mid-unbounded): the orchestrator's
//!   materializing collector (`collect_terminal_output`) buffers terminal
//!   output, so unbounded output streams are only reachable through the
//!   incremental `TerminalOutput` consumer / interop harness. The
//!   `test-unbounded-ticker` cap this needs is registered in testcartridge.

mod support;

use capdag::orchestrator::{
    plan_to_resolved_graph, CartridgeManager, ExecutionContext, ResolvedGraph, ResolvedNotation,
};
use capdag::{
    FrameReader, FrameWriter, Limits, PipelineLogFn, RelayNotifyCapabilitiesPayload, RelaySlave,
    DEFAULT_INITIAL_CREDIT,
};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use support::{Given, Machines};
use tokio::io::{BufReader, BufWriter};

const TAG: &str = "ProtocolE2ETest";

// =============================================================================
// Cap URNs under test (must match testcartridge's manifest verbatim)
// =============================================================================

const CAP_STREAM_N_CHUNKS: &str = r#"cap:in="media:enc=utf-8;count-spec";test-stream-n-chunks;out="media:enc=utf-8;chunk-stream""#;
const CAP_SLOW_CONSUME: &str = r#"cap:in="media:enc=utf-8;chunk-stream";test-slow-consume;out="media:enc=utf-8;consume-report""#;
/// Echoes ONE value, chunk by chunk as it arrives.
const CAP_ECHO_STREAM: &str =
    r#"cap:in="media:enc=utf-8;chunk-stream";test-echo-stream;out="media:enc=utf-8;echoed-stream""#;
/// The same handler over a SEQUENCE of items.
const CAP_ECHO_ITEMS: &str =
    r#"cap:in="media:enc=utf-8;chunk-stream";test-echo-items;out="media:enc=utf-8;echoed-items""#;

// =============================================================================
// Payloads
// =============================================================================

/// Chunk payload produced by testcartridge's `test-stream-n-chunks` for
/// index `i` — MUST stay byte-identical to `stream_chunk_payload` in
/// mfab-tests/testcartridge/src/main.rs.
fn stream_chunk_payload(i: usize) -> Vec<u8> {
    let mut s = format!("chunk-{:05}:", i);
    while s.len() < 1024 {
        s.push('x');
    }
    s.into_bytes()
}

/// The full expected output of `test-stream-n-chunks` for `n` chunks.
fn expected_stream_output(n: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(n * 1024);
    for i in 0..n {
        out.extend_from_slice(&stream_chunk_payload(i));
    }
    out
}

/// `n` distinct 1024-byte items. An engine sends a sequence one frame per
/// item, so `n` above the window exercises credit.
fn sequence_items(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            let mut s = format!("item-{:05}:", i);
            while s.len() < 1024 {
                s.push('y');
            }
            s.into_bytes()
        })
        .collect()
}

// =============================================================================
// Running a machine's steps on a context the test holds
// =============================================================================

/// A one-step machine, ready to run on an `ExecutionContext`: its step, the
/// node its input is put at and the node the step's result is read from.
struct OnContext {
    ctx: ExecutionContext,
    graph: ResolvedGraph,
    input: String,
    output: String,
    _dir: tempfile::TempDir,
}

/// Resolve and plan the one-input machine `notation`, and attach the test
/// cartridge to a fresh context — the manual counterpart of what a runtime
/// sets up, for tests that need the switch (protocol stats, negotiated
/// limits, extra masters).
async fn on_context(machines: &Machines, notation: &str, input_is_sequence: bool) -> OnContext {
    let resolved = ResolvedNotation::resolve(notation, machines.registry.clone())
        .await
        .expect("the notation resolves");
    let inputs = resolved.inputs().expect("inputs");
    assert_eq!(inputs.len(), 1, "these machines take one input");
    let sequences: HashSet<String> = if input_is_sequence {
        HashSet::from([inputs[0].name.clone()])
    } else {
        HashSet::new()
    };
    let strand = resolved
        .plan(0, &capdag::orchestrator::RunInputs { sequences, ..Default::default() })
        .await
        .expect("the machine plans");
    let graph = plan_to_resolved_graph(&strand.plan, &machines.registry).expect("the plan converts");
    let input = strand.input_slots[&inputs[0].name].clone();
    // These machines are one step: its node is where its main output is.
    assert_eq!(graph.steps.len(), 1, "these machines are one step");
    let output = graph.steps[0].node.clone();
    let cap_urns: Vec<&str> = graph.steps.iter().map(|step| step.cap_urn.as_str()).collect();

    let dir = tempfile::TempDir::new().expect("Failed to create temp dir");
    let cartridge_dir = dir.path().join("cartridges");
    std::fs::create_dir_all(&cartridge_dir).expect("Failed to create cartridge dir");
    let mut manager = CartridgeManager::new(
        cartridge_dir,
        None,
        capdag::CartridgeChannel::Release,
        capdag::FABRIC_MANIFEST_VERSION,
        vec![support::testcartridge_bin(TAG)],
        None,
        capdag::RegistryConfig::default().registry_base_url,
    );
    manager.init().await.expect("CartridgeManager init failed");
    let cartridges = manager
        .resolve_cartridges(&cap_urns)
        .await
        .expect("resolve_cartridges failed");
    let mut ctx = ExecutionContext::new(machines.registry.clone())
        .await
        .expect("ExecutionContext::new failed");
    ctx.add_cartridge_host(cartridges)
        .await
        .expect("add_cartridge_host failed");
    OnContext {
        ctx,
        graph,
        input,
        output,
        _dir: dir,
    }
}

impl OnContext {
    /// Run the machine's step on the context, which stays the test's: the
    /// switch is live afterwards. Returns what the step produced.
    async fn run(&mut self, limit: Duration) -> Vec<u8> {
        let log_fn: PipelineLogFn = std::sync::Arc::new(|record| {
            eprintln!(
                "[ProtocolE2E][{}] {} {}",
                record.level,
                record.cap_urn.as_deref().unwrap_or("machine"),
                record.message
            );
        });
        let outputs = tokio::time::timeout(
            limit,
            capdag::run_dag_on_context(
                &mut self.ctx,
                &self.graph,
                &HashMap::new(),
                None,
                None,
                Some(&log_fn),
                None,
                None,
                None,
                &HashSet::new(),
                120,
                None,
                // Transient capture is engine-only: this protocol test runs
                // the steps directly, with no run-artifact root and nothing
                // to publish to.
                None,
                None,
                None,
            ),
        )
        .await
        .expect("DEADLOCK: the run did not complete within its limit")
        .expect("Execution failed");
        outputs
            .node_data
            .get(&self.output)
            .unwrap_or_else(|| panic!("nothing was produced at '{}'", self.output))
            .concat()
    }
}

// =============================================================================
// TEST7054 / TEST7056 — credit flow control through a real cartridge
// =============================================================================

// TEST7054: Input-direction credit: a slow handler recv() throttles the
// engine's stream send (observed pause on the engine wire).
//
// E2E form of the law: the wire pause itself is asserted at the substrate
// layer (TEST7050); here the observable contract is that a 100-item
// sequence input — 3x the 32-chunk initial window — flows COMPLETELY and
// CORRECTLY through a deliberately slow consumer. If input-direction
// grants broke, the engine's send gate would stall forever (timeout
// fails the test); if the engine ignored the window, the cartridge would
// terminate with ERR CREDIT_VIOLATION (execution fails); if items were
// dropped, the count/bytes report would differ. The consumer takes a
// sequence, so the hundred items are ONE request — a step that took one
// value would be run a hundred times, each well inside the window.
#[tokio::test]
async fn test7054_slow_consumer_throttles_input_send() {
    let machines = Machines::new(TAG);
    let notation = format!("[slow_consume {CAP_SLOW_CONSUME}]\n[input -> slow_consume -> output]\n");

    // 100 items x 1024 bytes, one CHUNK frame each (> 32-chunk window).
    let items = sequence_items(100);
    let items: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();
    support::testcartridge_bin(TAG);
    let ran = tokio::time::timeout(
        Duration::from_secs(120),
        machines.run(&notation, &[("input", Given::Many(&items))]),
    )
    .await
    .expect(
        "TEST7054 DEADLOCK: >window input through a slow consumer did not \
         complete within 120s — input-direction credit grants are not \
         reaching the engine's send gate (L14)",
    )
    .expect("Execution failed");

    // One value: the slow-consume cap emits a single report.
    assert_eq!(
        ran.text("output"),
        "count=100;bytes=102400",
        "slow consumer must observe every item of the >window input exactly once"
    );
    assert_eq!(
        ran.plan.nodes.values().filter(|node| node.is_cap()).count(),
        1,
        "one request took the whole sequence"
    );

    // Terminal metadata on END (L3/L5): the handler's finish() message must
    // arrive end-to-end as the final progress event.
    let events = machines.logs.lock().unwrap().clone();
    assert!(
        events.iter().any(|(urn, level, msg)| {
            urn.contains("test-slow-consume")
                && level == "progress"
                && msg == "slow-consume-complete"
        }),
        "END terminal metadata (finish message) must reach the progress \
         callback as the final progress event (L3/L5); captured: {:?}",
        events
    );
}

// TEST7056: Bidirectional streaming (handler consumes input while emitting
// output) completes without deadlock at window 2.
//
// The window-2 wire variant is the substrate test; at e2e scale the
// negotiated window is 32 chunks and the input is 100 items (>3x the
// window), so completion is only possible when the engine feeds input
// concurrently with collecting output (L15) and credit flows in BOTH
// directions (L14). A deadlock hangs; the timeout converts it into a
// clear failure. Payload equality proves every item made the round trip
// in order.
#[tokio::test]
async fn test7056_bidirectional_echo_no_deadlock() {
    let machines = Machines::new(TAG);
    let notation = format!("[echo {CAP_ECHO_ITEMS}]\n[input -> echo -> output]\n");

    let items = sequence_items(100);
    let concat = items.concat();
    let items: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();
    support::testcartridge_bin(TAG);
    let ran = tokio::time::timeout(
        Duration::from_secs(120),
        machines.run(&notation, &[("input", Given::Many(&items))]),
    )
    .await
    .expect(
        "TEST7056 DEADLOCK: bidirectional echo of a >window input did not \
         complete within 120s — the engine is not servicing the cap's output \
         concurrently with feeding its input (L15), or credit is not flowing \
         in both directions (L14)",
    )
    .expect("Execution failed");

    // The echo cap writes each input item out as it arrives: one value, the
    // items end to end.
    let (produced, is_sequence) = ran.produced("output");
    assert!(!is_sequence);
    let output = produced.concat();
    assert_eq!(output.len(), concat.len(), "echoed payload length must equal the input length");
    assert!(output == concat, "echoed payload must be byte-identical to the input, in order");

    // The echo handler logs on its first item — proof it consumed
    // incrementally rather than buffering to completion.
    let events = machines.logs.lock().unwrap().clone();
    assert!(
        events.iter().any(|(urn, _level, msg)| {
            urn.contains("test-echo-items") && msg == "echo-stream: first item received"
        }),
        "echo cap's first-item log must be delivered; captured: {:?}",
        events
    );
}

// =============================================================================
// TEST7059 — no leaks after terminal END
// =============================================================================

// TEST7059: Terminal END releases credit waiters and leaks no stream state.
//
// Runs a full bidirectional >window request (real credit gates on both
// directions), then asserts the switch's protocol stats snapshot shows
// ZERO active requests and the terminated-by-kind accounting recorded the
// END. A leaked request entry, response channel, or rid-index row keeps
// `active` non-empty and fails the test (L7/L13).
#[tokio::test]
async fn test7059_terminal_end_releases_credit_and_leaks_no_state() {
    let machines = Machines::new(TAG);
    let notation = format!("[echo {CAP_ECHO_ITEMS}]\n[input -> echo -> output]\n");
    let mut machine = on_context(&machines, &notation, true).await;

    let items = sequence_items(100);
    let concat = items.concat();
    let input = machine.input.clone();
    machine.ctx.set_node_is_sequence(input.clone(), true);
    machine.ctx.set_node_data(
        input,
        capdag::orchestrator::cbor_util::wrap_raw_items_as_cbor_sequence(&items)
            .expect("items wrap as a sequence"),
    );

    // The steps run on the test's own context — so the switch stays live for
    // the post-run leak assertions.
    let output = machine.run(Duration::from_secs(120)).await;

    // The run itself must have been correct — a leak test over a broken run
    // proves nothing.
    assert!(output == concat, "echoed payload must round-trip intact before leak assertions");

    // Post-terminal settle: the terminal frame is forwarded to the response
    // channel before routing state is released (L6), but the identity-probe
    // requests from master attachment finish asynchronously. Poll briefly;
    // fail hard if any request is still registered after the deadline.
    let deadline = Instant::now() + Duration::from_secs(10);
    let stats = loop {
        let stats = machine.ctx.switch().protocol_stats().await;
        if stats.requests.active.is_empty() {
            break stats;
        }
        if Instant::now() > deadline {
            panic!(
                "TEST7059 LEAK: {} request(s) still active in the switch's \
                 request table 10s after the run completed — terminal END \
                 must release ALL state for the (xid,rid) (L7); \
                 terminated_by_kind={:?}, total_registered={}",
                stats.requests.active.len(),
                stats.requests.terminated_by_kind,
                stats.requests.total_registered,
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    // Termination accounting: the request table recorded at least the cap
    // request's END (identity probes add more "end" terminations — never
    // fewer), and every registration is accounted for.
    assert!(
        stats.requests.total_registered >= 1,
        "at least the cap request must have been registered; snapshot: total_registered={}",
        stats.requests.total_registered
    );
    let end_count = stats
        .requests
        .terminated_by_kind
        .get("end")
        .copied()
        .unwrap_or(0);
    assert!(
        end_count >= 1,
        "terminated_by_kind must count the END-terminated cap request; got {:?}",
        stats.requests.terminated_by_kind
    );
    let terminated_total: u64 = stats.requests.terminated_by_kind.values().sum();
    assert_eq!(
        terminated_total, stats.requests.total_registered,
        "every registered request must be terminated exactly once (L7): \
         terminated_by_kind={:?}, total_registered={}",
        stats.requests.terminated_by_kind, stats.requests.total_registered
    );
}

// =============================================================================
// TEST7061 — negotiated initial_credit
// =============================================================================

// TEST7061: The negotiated initial_credit (min of both HELLOs) is the actual
// first-burst size on the wire.
//
// E2E form, in two parts:
//   1. With the real cartridge host attached (both sides propose the
//      default), the switch's negotiated initial_credit is exactly
//      DEFAULT_INITIAL_CREDIT (32) — and a 48-chunk producer completes
//      correctly, which is only possible if the first burst (32) is
//      followed by grant-driven resumption (the cross-process TEST7055
//      variant: consumption grants replenish the producer's window).
//   2. Attaching a second master whose RelayNotify proposes
//      initial_credit=8 drops the switch's negotiated value to the
//      element-wise min, 8 — wire-visible min-negotiation.
#[tokio::test]
async fn test7061_negotiated_initial_credit_is_min_of_proposals() {
    use capdag::bifaci::local_socket::UnixStream;

    let machines = Machines::new(TAG);
    let notation = format!("[producer {CAP_STREAM_N_CHUNKS}]\n[input -> producer -> output]\n");
    let mut machine = on_context(&machines, &notation, false).await;

    // Part 1a: default-vs-default negotiation converges on the default.
    let negotiated = machine.ctx.limits().await;
    assert_eq!(
        negotiated.initial_credit, DEFAULT_INITIAL_CREDIT,
        "with a real cartridge host attached, both sides propose the default \
         initial_credit, so the negotiated min must be exactly the default"
    );

    // Part 1b: a 48-chunk producer (> the 32-chunk window) completes with a
    // byte-perfect payload — the producer's OutputStream can only stream
    // past its initial window via the engine's consumption grants (L9/L10).
    let input = machine.input.clone();
    machine.ctx.set_node_is_sequence(input.clone(), false);
    machine.ctx.set_node_data(input, b"48".to_vec());
    let output = machine.run(Duration::from_secs(120)).await;
    assert!(
        output == expected_stream_output(48),
        "48-chunk producer output must arrive intact and in order past the \
         32-chunk initial window"
    );

    // Part 2: attach a master proposing initial_credit=8. Its RelayNotify
    // carries the limits; the switch renegotiates element-wise min across
    // masters. The master advertises no caps, so it joins without an
    // identity probe and never dispatches — only its limits matter.
    let (switch_sock, slave_ext_sock) = UnixStream::pair().expect("socket pair");
    let (slave_int_sock, host_side_keepalive) = UnixStream::pair().expect("socket pair");
    let (int_read, int_write) = slave_int_sock.into_split();
    let slave = RelaySlave::new(BufReader::new(int_read), BufWriter::new(int_write));
    let (ext_read, ext_write) = slave_ext_sock.into_split();
    let slave_task = tokio::spawn(async move {
        let caps_json = serde_json::to_vec(&RelayNotifyCapabilitiesPayload::new(Vec::new()))
            .expect("serialize empty caps payload");
        let low_limits = Limits {
            initial_credit: 8,
            ..Limits::default()
        };
        let _ = slave
            .run(
                FrameReader::new(BufReader::new(ext_read)),
                FrameWriter::new(BufWriter::new(ext_write)),
                Some((&caps_json, &low_limits)),
            )
            .await;
    });
    machine
        .ctx
        .add_master("low-credit-limits-probe", switch_sock)
        .await
        .expect("add_master(low-credit-limits-probe) failed");

    // The rebuild runs inside add_master, but poll briefly to stay robust
    // against ordering, then assert hard.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let limits = machine.ctx.limits().await;
        if limits.initial_credit == 8 {
            break;
        }
        if Instant::now() > deadline {
            panic!(
                "TEST7061: negotiated initial_credit must drop to the \
                 element-wise min of all masters' proposals (min(32, 8) = 8); \
                 still {} after 5s",
                limits.initial_credit
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Keep the synthetic master's host-side socket alive until the
    // assertions are done, then tear it down.
    drop(host_side_keepalive);
    slave_task.abort();
}

// =============================================================================
// TEST7076 — pipelined chain ordering
// =============================================================================

// TEST7076: Pipelined chain execution: the downstream cap receives its first
// item before the upstream cap emits its last.
//
// [input -> test-stream-n-chunks -> mid -> test-echo-stream -> output] is a
// linear chain, so the run pipelines it: the intermediate node's data
// streams cap-to-cap live and is never materialized. The producer emits 48
// chunks (> the 32-chunk window) with a progress log per chunk; the echo
// cap logs on its first consumed item. With credit flowing per hop, the
// producer CANNOT emit chunk 33+ until the echo cap has consumed — so the
// echo's first-item log must appear in the captured event sequence BEFORE
// the producer's final per-chunk log. A materializing (non-pipelined)
// executor would show the producer finishing all 48 chunks first and fail
// the ordering assertion.
#[tokio::test]
async fn test7076_pipelined_chain_downstream_consumes_before_upstream_finishes() {
    // Surface counted-drop warnings and credit diagnostics on failure.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .try_init();
    let machines = Machines::new(TAG);
    let notation = format!(
        "[producer {CAP_STREAM_N_CHUNKS}][echo {CAP_ECHO_STREAM}]\
         [input -> producer -> mid][mid -> echo -> output]"
    );

    support::testcartridge_bin(TAG);
    // 180s, deliberately LONGER than the 120s activity timeout: on a
    // credit-forwarding stall the runtime's activity warnings fire at 120s
    // carrying the per-stream credit-state dumps (forwarder gate balances,
    // pending grants) — the diagnostics that identify the starved edge.
    // A kill timeout equal to the activity timeout races those dumps and
    // loses them from the failure log.
    let ran = tokio::time::timeout(
        Duration::from_secs(180),
        machines.run(&notation, &[("input", Given::One(b"48"))]),
    )
    .await
    .expect(
        "TEST7076 DEADLOCK: pipelined 2-cap chain did not complete within \
         180s — per-hop credit forwarding is stalled (L9/L10/L11); the \
         credit-state dumps in the 120s activity warnings above name the \
         starved edge",
    )
    .expect("Execution failed");

    // Correctness first: the full 48-chunk payload survived the pipelined
    // hop byte-for-byte.
    let (produced, is_sequence) = ran.produced("output");
    assert!(!is_sequence);
    assert!(
        produced.concat() == expected_stream_output(48),
        "pipelined chain output must equal the producer's full payload"
    );

    // Ordering: the echo cap's first-item log precedes the producer's final
    // per-chunk progress log in the captured sequence.
    let events = machines.logs.lock().unwrap().clone();
    let echo_first_idx = events
        .iter()
        .position(|(urn, _level, msg)| {
            urn.contains("test-echo-stream") && msg == "echo-stream: first item received"
        })
        .expect("TEST7076: echo cap's first-item log must be captured");
    let producer_last_idx = events
        .iter()
        .rposition(|(urn, _level, msg)| {
            urn.contains("test-stream-n-chunks") && msg == "emitted chunk 48/48"
        })
        .expect("TEST7076: producer's final per-chunk progress log must be captured");
    assert!(
        echo_first_idx < producer_last_idx,
        "TEST7076: with 48 chunks > the 32-chunk window, credit guarantees \
         the downstream cap consumed its first item (event index {}) before \
         the upstream cap emitted its last (event index {}) — the chain did \
         not pipeline",
        echo_first_idx,
        producer_last_idx
    );
}
