//! Protocol v4 end-to-end tests (bifaci v4 — explicit diagnostic attribution,
//! handler-capacity admission, credit-based flow control,
//! bidirectional streaming, terminal metadata on END, pipelined chains).
//!
//! These exercise the FULL stack with a real cartridge process:
//!
//! ```text
//!   test ←→ RelaySwitch ←→ RelaySlave ←→ CartridgeHostRuntime ←→ testcartridge
//! ```
//!
//! using the same harness as `orchestrator_integration.rs` (testcartridge is
//! auto-built at test runtime if missing or outdated).
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
//! mfab-tests/capdag-interop/README.md, "Protocol v4 scenarios"):
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

use capdag::cap::definition::{ArgSource, CapArg, CapOutput};
use capdag::orchestrator::{
    execute_dag, parse_machine_to_cap_dag, CapProgressFn, CartridgeManager, ExecutionContext,
    NodeData,
};
use capdag::{
    Cap, CapUrn, FabricRegistry, FrameReader, FrameWriter, Limits, PipelineLogFn,
    RelayNotifyCapabilitiesPayload, RelaySlave, StreamMeta, DEFAULT_INITIAL_CREDIT,
};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::io::{BufReader, BufWriter};

// =============================================================================
// Cap URNs under test (must match testcartridge's manifest verbatim)
// =============================================================================

const CAP_STREAM_N_CHUNKS: &str = r#"cap:in="media:enc=utf-8;count-spec";test-stream-n-chunks;out="media:enc=utf-8;chunk-stream""#;
const CAP_SLOW_CONSUME: &str = r#"cap:in="media:enc=utf-8;chunk-stream";test-slow-consume;out="media:enc=utf-8;consume-report""#;
const CAP_ECHO_STREAM: &str =
    r#"cap:in="media:enc=utf-8;chunk-stream";test-echo-stream;out="media:enc=utf-8;echoed-stream""#;

// =============================================================================
// Log capture — every pipeline log/progress event, in arrival order
// =============================================================================

/// Captured (cap_urn, level, message) triples in the order the engine
/// delivered them. Ordering assertions (TEST7076) key on this sequence.
type CapturedLogs = Arc<Mutex<Vec<(String, String, String)>>>;

fn capturing_log_fn() -> (PipelineLogFn, CapturedLogs) {
    let events: CapturedLogs = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&events);
    let log_fn: PipelineLogFn = Arc::new(move |record| {
        let cap_urn = record.cap_urn.unwrap_or_else(|| "machine".to_string());
        eprintln!(
            "[V4E2ELog][{}] {} {}",
            record.level, cap_urn, record.message
        );
        sink.lock()
            .unwrap()
            .push((cap_urn, record.level, record.message));
    });
    (log_fn, events)
}

fn capturing_progress_fn(events: &CapturedLogs) -> CapProgressFn {
    let sink = Arc::clone(events);
    Arc::new(move |_progress, cap_urn, message| {
        sink.lock().unwrap().push((
            cap_urn.to_string(),
            "progress".to_string(),
            message.to_string(),
        ));
    })
}

// =============================================================================
// Deterministic payloads (kept in sync with testcartridge/src/main.rs)
// =============================================================================

/// The exact ~1KB chunk `test-stream-n-chunks` emits for index `i`.
/// Must stay byte-identical to `stream_chunk_payload` in
/// mfab-tests/testcartridge/src/main.rs.
fn stream_chunk_payload(i: usize) -> Vec<u8> {
    let mut s = format!("chunk-{:05}:", i);
    while s.len() < 1024 {
        s.push('x');
    }
    s.into_bytes()
}

/// Expected concatenated output of `test-stream-n-chunks` for count `n`.
fn expected_stream_output(n: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(n * 1024);
    for i in 0..n {
        out.extend_from_slice(&stream_chunk_payload(i));
    }
    out
}

/// Build a sequence-mode initial input of `n` 1024-byte items.
/// Returns (RFC 8742 CBOR sequence of Bytes items, concatenated raw bytes).
/// In sequence mode each item is sent as ONE CHUNK frame, so n > 32 exceeds
/// the default initial_credit window and forces credit stall/resume.
fn sequence_items(n: usize) -> (Vec<u8>, Vec<u8>) {
    let mut seq = Vec::new();
    let mut concat = Vec::new();
    for i in 0..n {
        let mut s = format!("item-{:05}:", i);
        while s.len() < 1024 {
            s.push('y');
        }
        let bytes = s.into_bytes();
        concat.extend_from_slice(&bytes);
        ciborium::into_writer(&ciborium::Value::Bytes(bytes), &mut seq)
            .expect("CBOR-encode sequence item");
    }
    (seq, concat)
}

// =============================================================================
// Test cap registry (same construction pattern as orchestrator_integration.rs)
// =============================================================================

/// Build a `Cap` from a cap URN string with one stdin arg matching its
/// `in=` spec so the resolver's source-to-cap-arg matching can succeed.
fn build_testcartridge_cap(urn_str: &str) -> Cap {
    let cap_urn = CapUrn::from_string(urn_str).expect("Invalid test cap URN");
    let in_spec = cap_urn.in_spec().to_string();
    let out_spec = cap_urn.out_spec().to_string();
    Cap {
        urn: cap_urn.clone(),
        version: 1,
        title: format!(
            "Test {}",
            cap_urn.get_tag("op").map_or("unknown", |s| s.as_str())
        ),
        cap_description: None,
        documentation: None,
        metadata: HashMap::new(),
        aliases: vec!["testcartridge".to_string()],
        is_abstract: false,
        args: vec![CapArg::new(
            in_spec.clone(),
            true,
            vec![ArgSource::Stdin { stdin: in_spec }],
        )],
        output: Some(CapOutput::new(out_spec, "testcartridge output".to_string())),
        metadata_json: None,
        registered_by: None,
        supported_model_types: Vec::new(),
        default_model_spec: None,
    }
}

/// Unified `FabricRegistry` pre-loaded with the v4 testcartridge caps.
fn create_v4_fabric_registry() -> Arc<FabricRegistry> {
    let registry = FabricRegistry::new_for_test();
    let caps = vec![
        build_testcartridge_cap(CAP_STREAM_N_CHUNKS),
        build_testcartridge_cap(CAP_SLOW_CONSUME),
        build_testcartridge_cap(CAP_ECHO_STREAM),
    ];
    registry.add_caps_to_cache(caps);
    Arc::new(registry)
}

// =============================================================================
// testcartridge binary harness (mirrors orchestrator_integration.rs)
// =============================================================================

/// Get the testcartridge source directory.
///
/// The crate sits at `machinefabric/capdag/capdag-rs`, so the workspace holding
/// `mfab-tests` is two levels up. Missing is a hard error here rather than an
/// opaque spawn failure later: `Command::current_dir` on an absent directory
/// reports only `NotFound`, which reads as a missing `cargo`.
fn testcartridge_dir() -> PathBuf {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let dir = PathBuf::from(&manifest_dir)
        .parent()
        .expect("capdag-rs has a parent (the capdag superrepo)")
        .parent()
        .expect("the capdag superrepo has a parent (the machinefabric workspace)")
        .join("mfab-tests")
        .join("testcartridge");
    assert!(
        dir.is_dir(),
        "testcartridge source not found at {}; these tests need the machinefabric workspace \
         checkout, not a standalone capdag-rs clone",
        dir.display()
    );
    dir
}

/// Check if testcartridge needs rebuilding
fn testcartridge_needs_rebuild(binary_path: &PathBuf) -> bool {
    let binary_mtime = match binary_path.metadata().and_then(|m| m.modified()) {
        Ok(t) => t,
        Err(_) => return true,
    };

    let cart_dir = testcartridge_dir();

    let cargo_toml = cart_dir.join("Cargo.toml");
    if let Ok(meta) = cargo_toml.metadata() {
        if let Ok(mtime) = meta.modified() {
            if mtime > binary_mtime {
                eprintln!("[V4E2ETest] Cargo.toml is newer than binary");
                return true;
            }
        }
    }

    let src_dir = cart_dir.join("src");
    if src_dir.exists() && check_dir_newer(&src_dir, &binary_mtime) {
        eprintln!("[V4E2ETest] src/ has files newer than binary");
        return true;
    }

    false
}

/// Check if any file in directory is newer than reference time
fn check_dir_newer(dir: &PathBuf, reference: &std::time::SystemTime) -> bool {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if check_dir_newer(&path, reference) {
                    return true;
                }
            } else if path.is_file() {
                if let Ok(meta) = path.metadata() {
                    if let Ok(mtime) = meta.modified() {
                        if mtime > *reference {
                            return true;
                        }
                    }
                }
            }
        }
    }
    false
}

/// Build testcartridge in release mode
fn build_testcartridge() {
    let cart_dir = testcartridge_dir();
    let target_dir = testcartridge_target_dir();
    eprintln!("[V4E2ETest] Building testcartridge in release mode...");
    eprintln!("[V4E2ETest]   Directory: {:?}", cart_dir);
    eprintln!("[V4E2ETest]   Target dir: {:?}", target_dir);

    let output = Command::new("cargo")
        .arg("build")
        .arg("--release")
        .env("CARGO_TARGET_DIR", &target_dir)
        .current_dir(&cart_dir)
        .output()
        .expect("Failed to run cargo build for testcartridge");

    let stdout = String::from_utf8_lossy(&output.stdout);
    if !stdout.trim().is_empty() {
        for line in stdout.lines() {
            eprintln!("[V4E2ETest]   {}", line);
        }
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !stderr.trim().is_empty() {
        for line in stderr.lines() {
            eprintln!("[V4E2ETest]   {}", line);
        }
    }

    if !output.status.success() {
        panic!(
            "Failed to build testcartridge (exit code: {:?})",
            output.status.code()
        );
    }

    eprintln!("[V4E2ETest] Successfully built testcartridge");
}

/// Resolve the `CARGO_TARGET_DIR` for the testcartridge build (see
/// orchestrator_integration.rs for the workspace-layout rationale).
fn testcartridge_target_dir() -> PathBuf {
    if let Ok(dir) = env::var("CAPDAG_TESTCARTRIDGE_TARGET_DIR") {
        if !dir.is_empty() {
            return PathBuf::from(dir);
        }
    }
    if let Ok(build_dir) = env::var("CARGO_BUILD_DIR") {
        if !build_dir.is_empty() {
            return PathBuf::from(build_dir).join("testcartridge");
        }
    }
    // No runner env: derive the SAME location from the workspace layout. There
    // is deliberately no in-tree fallback — build output belongs under
    // machinefabric/build, and a `target/` beside the source both violates that
    // and hides the missing variable behind a second 600 MB copy.
    workspace_root()
        .join("build")
        .join("cargo")
        .join("testcartridge")
}

/// The machinefabric workspace root: the crate is `machinefabric/capdag/capdag-rs`.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("capdag-rs has a parent (the capdag superrepo)")
        .parent()
        .expect("the capdag superrepo has a parent (the machinefabric workspace)")
        .to_path_buf()
}

/// Get path to testcartridge binary, building if necessary.
fn testcartridge_bin() -> PathBuf {
    let target_dir = testcartridge_target_dir();
    let bin_path = target_dir.join("release").join("testcartridge");

    let needs_build = if !bin_path.exists() {
        eprintln!("[V4E2ETest] Binary not found at {:?}, will build", bin_path);
        true
    } else {
        testcartridge_needs_rebuild(&bin_path)
    };

    if needs_build {
        build_testcartridge();
    }

    if !bin_path.exists() {
        panic!(
            "testcartridge binary not found at {:?} after build attempt (CARGO_TARGET_DIR={:?})",
            bin_path,
            env::var("CARGO_TARGET_DIR").ok()
        );
    }

    bin_path
}

fn setup_test_env() -> (TempDir, PathBuf, Vec<PathBuf>) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cartridge_dir = temp_dir.path().join("cartridges");
    fs::create_dir_all(&cartridge_dir).expect("Failed to create cartridge dir");
    (temp_dir, cartridge_dir, vec![testcartridge_bin()])
}

/// Build an `ExecutionContext` with the testcartridge attached as a
/// cartridge host — the manual-context counterpart of `execute_dag`'s
/// setup phase, for tests that need direct access to the switch
/// (protocol stats, negotiated limits, extra masters).
async fn setup_execution_context(
    cartridge_dir: PathBuf,
    dev_binaries: Vec<PathBuf>,
    cap_urns: &[&str],
) -> ExecutionContext {
    let mut manager = CartridgeManager::new(
        cartridge_dir,
        None,
        capdag::CartridgeChannel::Release,
        capdag::FABRIC_MANIFEST_VERSION,
        dev_binaries,
        None,
        capdag::RegistryConfig::default().registry_base_url,
    );
    manager.init().await.expect("CartridgeManager init failed");
    let cartridges = manager
        .resolve_cartridges(cap_urns)
        .await
        .expect("resolve_cartridges failed");

    let mut ctx = ExecutionContext::new(create_v4_fabric_registry())
        .await
        .expect("ExecutionContext::new failed");
    ctx.add_cartridge_host(cartridges)
        .await
        .expect("add_cartridge_host failed");
    ctx
}

// =============================================================================
// TEST7054 / TEST7056 — credit flow control through a real cartridge
// =============================================================================

// TEST7054: Input-direction credit: a slow handler recv() throttles the
// engine's stream send (observed pause on the engine wire).
//
// E2E form of the law: the wire pause itself is asserted at the substrate
// layer (TEST7050); here the observable contract is that a 100-chunk
// sequence input — 3x the 32-chunk initial window — flows COMPLETELY and
// CORRECTLY through a deliberately slow consumer. If input-direction
// grants broke, the engine's send gate would stall forever (timeout
// fails the test); if the engine ignored the window, the cartridge would
// terminate with ERR CREDIT_VIOLATION (execution fails); if items were
// dropped, the count/bytes report would differ.
#[tokio::test]
async fn test7054_slow_consumer_throttles_input_send() {
    let registry = create_v4_fabric_registry();
    let (_temp, cartridge_dir, dev_binaries) = setup_test_env();

    let route = format!(
        "\n[slow_consume {}]\n[input -> slow_consume -> output]\n",
        CAP_SLOW_CONSUME
    );
    let graph = parse_machine_to_cap_dag(&route, &*registry)
        .await
        .expect("Parse failed");

    // 100 items x 1024 bytes, one CHUNK frame each (> 32-chunk window).
    let (seq, _concat) = sequence_items(100);
    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("input".to_string(), NodeData::Bytes(seq));
    let mut initial_is_sequence = HashMap::new();
    initial_is_sequence.insert("input".to_string(), true);

    let (log_fn, events) = capturing_log_fn();
    let progress_fn = capturing_progress_fn(&events);
    let outputs = tokio::time::timeout(
        Duration::from_secs(120),
        execute_dag(
            &graph,
            cartridge_dir,
            None,
            capdag::CartridgeChannel::Release,
            capdag::FABRIC_MANIFEST_VERSION,
            initial_inputs,
            initial_is_sequence,
            dev_binaries,
            None,
            create_v4_fabric_registry(),
            Some(&progress_fn),
            &log_fn,
            &HashMap::new(),
            None,
        ),
    )
    .await
    .expect(
        "TEST7054 DEADLOCK: >window input through a slow consumer did not \
         complete within 120s — input-direction credit grants are not \
         reaching the engine's send gate (L14)",
    )
    .expect("Execution failed")
    .node_data;

    // Scalar terminal: the slow-consume cap emits a single report item.
    let output = outputs.get("output").expect("No output node").concat();
    let report = String::from_utf8(output).expect("Invalid UTF-8 report");
    assert_eq!(
        report, "count=100;bytes=102400",
        "slow consumer must observe every item of the >window input exactly once"
    );

    // Terminal metadata on END (L3/L5): the handler's finish() message must
    // arrive end-to-end as the final progress event.
    let events = events.lock().unwrap().clone();
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
// negotiated window is 32 chunks and the input is 100 chunks (>3x the
// window), so completion is only possible when the engine feeds input
// concurrently with collecting output (L15) and credit flows in BOTH
// directions (L14). A deadlock hangs; the timeout converts it into a
// clear failure. Payload equality proves every item made the round trip
// in order.
#[tokio::test]
async fn test7056_bidirectional_echo_no_deadlock() {
    let registry = create_v4_fabric_registry();
    let (_temp, cartridge_dir, dev_binaries) = setup_test_env();

    let route = format!("\n[echo {}]\n[input -> echo -> output]\n", CAP_ECHO_STREAM);
    let graph = parse_machine_to_cap_dag(&route, &*registry)
        .await
        .expect("Parse failed");

    let (seq, concat) = sequence_items(100);
    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("input".to_string(), NodeData::Bytes(seq));
    let mut initial_is_sequence = HashMap::new();
    initial_is_sequence.insert("input".to_string(), true);

    let (log_fn, events) = capturing_log_fn();
    let outputs = tokio::time::timeout(
        Duration::from_secs(120),
        execute_dag(
            &graph,
            cartridge_dir,
            None,
            capdag::CartridgeChannel::Release,
            capdag::FABRIC_MANIFEST_VERSION,
            initial_inputs,
            initial_is_sequence,
            dev_binaries,
            None,
            create_v4_fabric_registry(),
            None,
            &log_fn,
            &HashMap::new(),
            None,
        ),
    )
    .await
    .expect(
        "TEST7056 DEADLOCK: bidirectional echo of a >window input did not \
         complete within 120s — the engine is not servicing the cap's output \
         concurrently with feeding its input (L15), or credit is not flowing \
         in both directions (L14)",
    )
    .expect("Execution failed")
    .node_data;

    // Sequence terminal: the echo cap re-emits each input item; the decoded
    // items concatenate back to the original payload.
    let output = outputs.get("output").expect("No output node").concat();
    assert_eq!(
        output.len(),
        concat.len(),
        "echoed payload length must equal the input length"
    );
    assert_eq!(
        output, concat,
        "echoed payload must be byte-identical to the input, in order"
    );

    // The echo handler logs on its first item — proof it consumed
    // incrementally rather than buffering to completion.
    let events = events.lock().unwrap().clone();
    assert!(
        events.iter().any(|(urn, _level, msg)| {
            urn.contains("test-echo-stream") && msg == "echo-stream: first item received"
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
    let registry = create_v4_fabric_registry();
    let (_temp, cartridge_dir, dev_binaries) = setup_test_env();

    let route = format!("\n[echo {}]\n[input -> echo -> output]\n", CAP_ECHO_STREAM);
    let graph = parse_machine_to_cap_dag(&route, &*registry)
        .await
        .expect("Parse failed");

    let mut ctx = setup_execution_context(cartridge_dir, dev_binaries, &[CAP_ECHO_STREAM]).await;

    let (seq, concat) = sequence_items(100);
    ctx.set_node_is_sequence("input".to_string(), true);
    ctx.set_node_data("input".to_string(), seq);

    let (log_fn, _events) = capturing_log_fn();
    // `run_dag_on_context` is the ONE shared segment executor (the old
    // `execute_fanin` is gone). It borrows the context — so the switch stays
    // live for the post-run leak assertions — and returns the decoded outputs
    // keyed by node rather than storing them back into the context.
    let outputs = tokio::time::timeout(
        Duration::from_secs(120),
        capdag::run_dag_on_context(
            &mut ctx,
            &graph,
            &HashMap::new(),
            None,
            None,
            Some(&log_fn),
            None,
            None,
            None,
            &std::collections::HashSet::new(),
            120,
            None,
            // Transient capture is engine-only: this protocol test runs the DAG
            // directly, with no run-artifact root and nothing to publish to.
            None,
            None,
        ),
    )
    .await
    .expect("TEST7059 DEADLOCK: echo run did not complete within 120s")
    .expect("Execution failed")
    .node_data;

    // The run itself must have been correct — a leak test over a broken run
    // proves nothing. The sequence terminal's decoded items concatenate back
    // to the original payload.
    let output = outputs.get("output").expect("No output node").concat();
    assert_eq!(
        output.as_slice(),
        concat.as_slice(),
        "echoed payload must round-trip intact before leak assertions"
    );

    // Post-terminal settle: the terminal frame is forwarded to the response
    // channel before routing state is released (L6), but the identity-probe
    // requests from master attachment finish asynchronously. Poll briefly;
    // fail hard if any request is still registered after the deadline.
    let deadline = Instant::now() + Duration::from_secs(10);
    let stats = loop {
        let stats = ctx.switch().protocol_stats().await;
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

    let registry = create_v4_fabric_registry();
    let (_temp, cartridge_dir, dev_binaries) = setup_test_env();

    let route = format!(
        "\n[producer {}]\n[input -> producer -> output]\n",
        CAP_STREAM_N_CHUNKS
    );
    let graph = parse_machine_to_cap_dag(&route, &*registry)
        .await
        .expect("Parse failed");

    let mut ctx =
        setup_execution_context(cartridge_dir, dev_binaries, &[CAP_STREAM_N_CHUNKS]).await;

    // Part 1a: default-vs-default negotiation converges on the default.
    let negotiated = ctx.limits().await;
    assert_eq!(
        negotiated.initial_credit, DEFAULT_INITIAL_CREDIT,
        "with a real cartridge host attached, both sides propose the default \
         initial_credit, so the negotiated min must be exactly the default"
    );

    // Part 1b: a 48-chunk producer (> the 32-chunk window) completes with a
    // byte-perfect payload — the producer's OutputStream can only stream
    // past its initial window via the engine's consumption grants (L9/L10).
    ctx.set_node_is_sequence("input".to_string(), false);
    ctx.set_node_data("input".to_string(), b"48".to_vec());
    let (log_fn, _events) = capturing_log_fn();
    // Shared segment executor; borrows the context so `ctx` remains usable for
    // the Part-2 master attachment and limits renegotiation below.
    let outputs = tokio::time::timeout(
        Duration::from_secs(120),
        capdag::run_dag_on_context(
            &mut ctx,
            &graph,
            &HashMap::new(),
            None,
            None,
            Some(&log_fn),
            None,
            None,
            None,
            &std::collections::HashSet::new(),
            120,
            None,
            // Transient capture is engine-only: this protocol test runs the DAG
            // directly, with no run-artifact root and nothing to publish to.
            None,
            None,
        ),
    )
    .await
    .expect(
        "TEST7061 DEADLOCK: 48-chunk producer did not complete within 120s — \
         the engine's consumption grants are not replenishing the \
         cartridge's output window (L10)",
    )
    .expect("Execution failed")
    .node_data;
    let output = outputs.get("output").expect("No output node").concat();
    assert_eq!(
        output.as_slice(),
        expected_stream_output(48).as_slice(),
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
    ctx.add_master("low-credit-limits-probe", switch_sock)
        .await
        .expect("add_master(low-credit-limits-probe) failed");

    // The rebuild runs inside add_master, but poll briefly to stay robust
    // against ordering, then assert hard.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let limits = ctx.limits().await;
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
// linear chain, so `execute_dag` pipelines it: the intermediate node's data
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
    let registry = create_v4_fabric_registry();
    let (_temp, cartridge_dir, dev_binaries) = setup_test_env();

    let route = format!(
        "\n[producer {}]\n[echo {}]\n[input -> producer -> mid]\n[mid -> echo -> output]\n",
        CAP_STREAM_N_CHUNKS, CAP_ECHO_STREAM
    );
    let graph = parse_machine_to_cap_dag(&route, &*registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 2, "chain must resolve to two edges");

    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("input".to_string(), NodeData::Text("48".to_string()));
    let mut initial_is_sequence = HashMap::new();
    initial_is_sequence.insert("input".to_string(), false);

    let (log_fn, events) = capturing_log_fn();
    let progress_fn = capturing_progress_fn(&events);
    // 180s, deliberately LONGER than the 120s activity timeout: on a
    // credit-forwarding stall the runtime's activity warnings fire at 120s
    // carrying the per-stream credit-state dumps (forwarder gate balances,
    // pending grants) — the diagnostics that identify the starved edge.
    // A kill timeout equal to the activity timeout races those dumps and
    // loses them from the failure log.
    let outputs = tokio::time::timeout(
        Duration::from_secs(180),
        execute_dag(
            &graph,
            cartridge_dir,
            None,
            capdag::CartridgeChannel::Release,
            capdag::FABRIC_MANIFEST_VERSION,
            initial_inputs,
            initial_is_sequence,
            dev_binaries,
            None,
            create_v4_fabric_registry(),
            Some(&progress_fn),
            &log_fn,
            &HashMap::new(),
            None,
        ),
    )
    .await
    .expect(
        "TEST7076 DEADLOCK: pipelined 2-cap chain did not complete within \
         180s — per-hop credit forwarding is stalled (L9/L10/L11); the \
         credit-state dumps in the 120s activity warnings above name the \
         starved edge",
    )
    .expect("Execution failed")
    .node_data;

    // Correctness first: the full 48-chunk payload survived the pipelined
    // hop byte-for-byte.
    let output = outputs.get("output").expect("No output node").concat();
    assert_eq!(
        output.as_slice(),
        expected_stream_output(48).as_slice(),
        "pipelined chain output must equal the producer's full payload"
    );

    // The pipelined intermediate node must NOT be materialized in the
    // result map — that is the whole point of pipelining.
    assert!(
        !outputs.contains_key("mid"),
        "pipelined intermediate node 'mid' must never be materialized"
    );

    // Ordering: the echo cap's first-item log precedes the producer's final
    // per-chunk progress log in the captured sequence.
    let events = events.lock().unwrap().clone();
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
