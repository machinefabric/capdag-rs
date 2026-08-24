//! Integration tests for capdag orchestrator using testcartridge
//!
//! These tests verify the orchestrator's ability to:
//! 1. Parse and validate machine notation graphs with Cap URNs
//! 2. Execute DAGs using testcartridge capabilities
//! 3. Handle data flow between nodes
//! 4. Work with CBOR protocol via CartridgeHost
//!
//! testcartridge provides simple, predictable test caps without heavy dependencies
//! The testcartridge binary will be auto-built if missing or outdated

use capdag::cap::definition::{ArgSource, CapArg, CapOutput};
use capdag::orchestrator::{
    execute_dag, parse_machine_to_cap_dag, NodeData, ParseOrchestrationError,
};
use capdag::{Cap, CapUrn, FabricRegistry, PipelineLogFn, StreamMeta};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use tempfile::TempDir;

fn test_pipeline_log_fn() -> PipelineLogFn {
    fn render_meta(meta: Option<&StreamMeta>) -> String {
        let Some(meta) = meta else {
            return String::new();
        };
        if let Some(ciborium::Value::Float(progress)) = meta.get("progress") {
            return format!(" [meta progress={:.1}%]", progress * 100.0);
        }
        if let Some(ciborium::Value::Integer(progress)) = meta.get("progress") {
            let progress: i128 = (*progress).into();
            return format!(" [meta progress={}]", progress);
        }
        format!(" [meta {:?}]", meta)
    }

    Arc::new(|record| {
        let meta_suffix = render_meta(record.meta.as_ref());
        let cap_urn = record.cap_urn.as_deref().unwrap_or("machine");
        match record.body_index {
            Some(index) => eprintln!(
                "[OrchestratorTestLog][{}][body {}]{} {} {}",
                record.level, index, meta_suffix, cap_urn, record.message
            ),
            None => eprintln!(
                "[OrchestratorTestLog][{}]{} {} {}",
                record.level, meta_suffix, cap_urn, record.message
            ),
        }
    })
}

// =============================================================================
// Test Cap Registry for testcartridge Caps
//
// Builds a `FabricRegistry::new_for_test()` populated with the
// testcartridge caps. Each cap declares one stdin arg matching
// its `in=` spec so the resolver's source-to-cap-arg matching
// can succeed. Used by both `parse_machine_to_cap_dag` (for
// resolution) and `execute_dag` (for runtime cap lookup).
// =============================================================================

/// Build a `Cap` from a cap URN string with one stdin arg
/// matching its `in=` spec.
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
        // Empty model-related fields — testcartridge has no model
        // dependency, so it accepts any architecture and has no
        // default model spec. See `Cap` doc-comments in
        // src/cap/definition.rs.
        supported_model_types: Vec::new(),
        default_model_spec: None,
    }
}

/// Build the two-input `test-combine` cap def (matches testcartridge): a MAIN input on
/// stdin (`node2` = `in=`) plus a distinct-URN, NON-stdin second arg (`node3`) fed by a
/// producer. Exercises convergence — two producers routed into one cap by distinct arg
/// URNs. Note RULE3 holds: only the main arg declares a stdin source.
fn build_combine_cap() -> Cap {
    let cap_urn = CapUrn::from_string(
        r#"cap:in="media:enc=utf-8;node2";test-combine;out="media:enc=utf-8;combined""#,
    )
    .expect("Invalid combine URN");
    Cap {
        urn: cap_urn,
        version: 1,
        title: "Test Combine".to_string(),
        cap_description: None,
        documentation: None,
        metadata: HashMap::new(),
        aliases: vec!["testcartridge".to_string()],
        is_abstract: false,
        args: vec![
            CapArg::new(
                "media:enc=utf-8;file-path",
                true,
                vec![ArgSource::Stdin {
                    stdin: "media:enc=utf-8;node2".to_string(),
                }],
            ),
            CapArg::new(
                "media:enc=utf-8;node3",
                true,
                vec![ArgSource::CliFlag {
                    cli_flag: "--second-input".to_string(),
                }],
            ),
        ],
        output: Some(CapOutput::new(
            "media:enc=utf-8;combined".to_string(),
            "combined output".to_string(),
        )),
        metadata_json: None,
        registered_by: None,
        supported_model_types: Vec::new(),
        default_model_spec: None,
    }
}

// =============================================================================
// Test Helpers
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

    // Check Cargo.toml
    let cargo_toml = cart_dir.join("Cargo.toml");
    if let Ok(meta) = cargo_toml.metadata() {
        if let Ok(mtime) = meta.modified() {
            if mtime > binary_mtime {
                eprintln!("[TestcartridgeTest] Cargo.toml is newer than binary");
                return true;
            }
        }
    }

    // Check src/ directory
    let src_dir = cart_dir.join("src");
    if src_dir.exists() {
        if check_dir_newer(&src_dir, &binary_mtime) {
            eprintln!("[TestcartridgeTest] src/ has files newer than binary");
            return true;
        }
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
    eprintln!("[TestcartridgeTest] Building testcartridge in release mode...");
    eprintln!("[TestcartridgeTest]   Directory: {:?}", cart_dir);
    eprintln!("[TestcartridgeTest]   Target dir: {:?}", target_dir);
    eprintln!("[TestcartridgeTest]   Running: cargo build --release");

    let output = Command::new("cargo")
        .arg("build")
        .arg("--release")
        .env("CARGO_TARGET_DIR", &target_dir)
        .current_dir(&cart_dir)
        .output()
        .expect("Failed to run cargo build for testcartridge");

    // Print stdout if any
    let stdout = String::from_utf8_lossy(&output.stdout);
    if !stdout.trim().is_empty() {
        for line in stdout.lines() {
            eprintln!("[TestcartridgeTest]   {}", line);
        }
    }

    // Print stderr (cargo output goes here)
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !stderr.trim().is_empty() {
        for line in stderr.lines() {
            eprintln!("[TestcartridgeTest]   {}", line);
        }
    }

    if !output.status.success() {
        panic!(
            "Failed to build testcartridge (exit code: {:?})",
            output.status.code()
        );
    }

    eprintln!("[TestcartridgeTest] Successfully built testcartridge");
}

/// Resolve the `CARGO_TARGET_DIR` to use for the testcartridge build.
///
/// The workspace test runner builds testcartridge into a per-crate
/// directory (`$CARGO_BUILD_DIR/testcartridge`) but runs the
/// orchestrator integration tests with `CARGO_TARGET_DIR` pointing at
/// capdag's own target dir. Both build phases must agree on which
/// `target` directory holds the testcartridge binary, so we resolve
/// it from the workspace layout rather than the inherited env.
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
        eprintln!(
            "[TestcartridgeTest] Binary not found at {:?}, will build",
            bin_path
        );
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

/// Build the `initial_is_sequence` map that pairs with the
/// caller's `initial_inputs`, declaring every input node as
/// scalar. The orchestrator now requires a 1:1 match between
/// the keys of `initial_inputs` and `initial_is_sequence`
/// (missing or extra entries are a hard error). Every test in
/// this file feeds scalar inputs (single text/bytes/file blob
/// per input node), so this helper covers them all.
fn all_scalar(inputs: &HashMap<String, NodeData>) -> HashMap<String, bool> {
    inputs.keys().map(|k| (k.clone(), false)).collect()
}

/// `execute_dag` returns each node's output as a `Vec<Vec<u8>>` of decoded items:
/// input/intermediate nodes carry their raw bytes as a single item, and a scalar
/// terminal decodes to exactly one item. This helper asserts the single-item
/// (scalar) shape and returns its raw bytes — the successor to the old
/// `NodeData::Bytes(b)` match arm.
fn scalar_bytes(items: &[Vec<u8>]) -> Vec<u8> {
    assert_eq!(
        items.len(),
        1,
        "expected a scalar node (exactly 1 item), got {} items",
        items.len()
    );
    items[0].clone()
}

/// Create an `Arc<FabricRegistry>` with all testcartridge caps.
/// Used by both `parse_machine_to_cap_dag` (which needs the
/// resolver's `args` lists) and `execute_dag` (which looks up
/// the full cap definition at runtime).
/// Build a single unified `FabricRegistry` pre-loaded with the
/// testcartridge synthetic caps the orchestrator integration tests
/// depend on. The merged registry holds caps and media defs together,
/// so callers pass the same Arc to anything that previously took two
/// separate registries.
fn create_test_fabric_registry() -> Arc<FabricRegistry> {
    let registry = FabricRegistry::new_for_test();
    let caps = vec![
        build_testcartridge_cap(
            r#"cap:in="media:enc=utf-8;node1";test-edge1;out="media:enc=utf-8;node2""#,
        ),
        build_testcartridge_cap(
            r#"cap:in="media:enc=utf-8;node2";test-edge2;out="media:enc=utf-8;node3""#,
        ),
        build_testcartridge_cap(
            r#"cap:in="media:enc=utf-8;node3";test-edge3;out="media:enc=utf-8;list;node4""#,
        ),
        build_testcartridge_cap(
            r#"cap:in="media:enc=utf-8;list;node4";test-edge4;out="media:enc=utf-8;node5""#,
        ),
        build_testcartridge_cap(
            r#"cap:in="media:enc=utf-8;node3";test-edge7;out="media:enc=utf-8;node6""#,
        ),
        build_testcartridge_cap(
            r#"cap:in="media:enc=utf-8;node6";test-edge8;out="media:enc=utf-8;node7""#,
        ),
        build_testcartridge_cap(
            r#"cap:in="media:enc=utf-8;node7";test-edge9;out="media:enc=utf-8;node8""#,
        ),
        build_testcartridge_cap(
            r#"cap:in="media:enc=utf-8;node8";test-edge10;out="media:enc=utf-8;node1""#,
        ),
        build_testcartridge_cap(r#"cap:in="media:void";test-large;out="media:""#),
        build_testcartridge_cap(
            r#"cap:in="media:enc=utf-8;node1";test-peer;out="media:enc=utf-8;node3""#,
        ),
        build_testcartridge_cap(
            r#"cap:in="media:enc=utf-8;node1";identity;out="media:enc=utf-8;node1""#,
        ),
        build_combine_cap(),
    ];
    registry.add_caps_to_cache(caps);
    Arc::new(registry)
}

// =============================================================================
// Phase 1: Basic capdag CLI Functionality with testcartridge
// =============================================================================

// TEST919: Parse simple machine notation graph with test-edge1
#[tokio::test]
async fn test919_parse_simple_testcartridge_graph() {
    let registry = create_test_fabric_registry();

    let route = r#"
[test_edge1 cap:in="media:enc=utf-8;node1";test-edge1;out="media:enc=utf-8;node2"]
[A -> test_edge1 -> B]
"#;

    let result = parse_machine_to_cap_dag(route, &*registry).await;
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let graph = result.unwrap();
    assert_eq!(graph.nodes.len(), 2);
    assert_eq!(graph.edges.len(), 1);
    let node_a = capdag::MediaUrn::from_string(graph.nodes.get("A").unwrap()).unwrap();
    let expected_a = capdag::MediaUrn::from_string("media:enc=utf-8;node1").unwrap();
    assert!(node_a.is_equivalent(&expected_a).unwrap());
    let node_b = capdag::MediaUrn::from_string(graph.nodes.get("B").unwrap()).unwrap();
    let expected_b = capdag::MediaUrn::from_string("media:enc=utf-8;node2").unwrap();
    assert!(node_b.is_equivalent(&expected_b).unwrap());
}

// TEST889: Execute single-edge DAG (test-edge1)
#[tokio::test]
async fn test889_execute_single_edge_dag() {
    let registry = create_test_fabric_registry();
    let (_temp, cartridge_dir, dev_binaries) = setup_test_env();

    let route = r#"
[test_edge1 cap:in="media:enc=utf-8;node1";test-edge1;out="media:enc=utf-8;node2"]
[input -> test_edge1 -> output]
"#;

    let graph = parse_machine_to_cap_dag(route, &*registry)
        .await
        .expect("Parse failed");

    // Create initial input
    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("input".to_string(), NodeData::Text("TEST".to_string()));

    // Execute DAG
    let fabric_registry = create_test_fabric_registry();
    let initial_is_sequence = all_scalar(&initial_inputs);
    let result = execute_dag(
        &graph,
        cartridge_dir,
        None, // dev-bin fixtures — no registry
        capdag::CartridgeChannel::Release,
        capdag::FABRIC_MANIFEST_VERSION,
        initial_inputs,
        initial_is_sequence,
        dev_binaries,
        None, // no bundled cartridges in these unit fixtures
        fabric_registry,
        None,
        &test_pipeline_log_fn(),
        &std::collections::HashMap::new(),
        None,
    )
    .await;

    assert!(result.is_ok(), "Execution failed: {:?}", result.err());

    let outputs = result.unwrap().node_data;
    let output_data = outputs.get("output").expect("No output node");

    let b = scalar_bytes(output_data);
    let output_str = String::from_utf8(b).expect("Invalid UTF-8");
    assert_eq!(output_str, "[PREPEND]TEST");
}

// TEST888: Execute two-edge chain (test-edge1 -> test-edge2)
#[tokio::test]
async fn test888_execute_edge1_to_edge2_chain() {
    let registry = create_test_fabric_registry();
    let (_temp, cartridge_dir, dev_binaries) = setup_test_env();

    let route = r#"
[test_edge1 cap:in="media:enc=utf-8;node1";test-edge1;out="media:enc=utf-8;node2"]
[test_edge2 cap:in="media:enc=utf-8;node2";test-edge2;out="media:enc=utf-8;node3"]
[A -> test_edge1 -> B]
[B -> test_edge2 -> C]
"#;

    let graph = parse_machine_to_cap_dag(route, &*registry)
        .await
        .expect("Parse failed");

    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("A".to_string(), NodeData::Text("CHAIN".to_string()));

    let fabric_registry = create_test_fabric_registry();
    let initial_is_sequence = all_scalar(&initial_inputs);
    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        None, // dev-bin fixtures — no registry
        capdag::CartridgeChannel::Release,
        capdag::FABRIC_MANIFEST_VERSION,
        initial_inputs,
        initial_is_sequence,
        dev_binaries,
        None, // no bundled cartridges in these unit fixtures
        fabric_registry,
        None,
        &test_pipeline_log_fn(),
        &std::collections::HashMap::new(),
        None,
    )
    .await
    .expect("Execution failed")
    .node_data;

    let final_output = outputs.get("C").expect("No final output");

    let b = scalar_bytes(final_output);
    let output_str = String::from_utf8(b).expect("Invalid UTF-8");
    // edge1: [PREPEND]CHAIN, edge2: [PREPEND]CHAIN[APPEND]
    assert_eq!(output_str, "[PREPEND]CHAIN[APPEND]");
}

// TEST887: Execute with file-path input
#[tokio::test]
async fn test887_execute_with_file_input() {
    let registry = create_test_fabric_registry();
    let (temp, cartridge_dir, dev_binaries) = setup_test_env();

    let route = r#"
[test_edge1 cap:in="media:enc=utf-8;node1";test-edge1;out="media:enc=utf-8;node2"]
[input -> test_edge1 -> output]
"#;

    let graph = parse_machine_to_cap_dag(route, &*registry)
        .await
        .expect("Parse failed");

    // Create test input file
    let input_file = temp.path().join("input.txt");
    fs::write(&input_file, "FILE_CONTENT").expect("Failed to write file");

    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("input".to_string(), NodeData::FilePath(input_file));

    let initial_is_sequence = all_scalar(&initial_inputs);
    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        None, // dev-bin fixtures — no registry
        capdag::CartridgeChannel::Release,
        capdag::FABRIC_MANIFEST_VERSION,
        initial_inputs,
        initial_is_sequence,
        dev_binaries,
        None, // no bundled cartridges in these unit fixtures
        create_test_fabric_registry(),
        None,
        &test_pipeline_log_fn(),
        &std::collections::HashMap::new(),
        None,
    )
    .await
    .expect("Execution failed")
    .node_data;

    let output = outputs.get("output").expect("No output");

    let b = scalar_bytes(output);
    let output_str = String::from_utf8(b).expect("Invalid UTF-8");
    assert_eq!(output_str, "[PREPEND]FILE_CONTENT");
}

// TEST952: Execute large payload (test-large cap)
#[tokio::test]
async fn test952_execute_large_payload() {
    let registry = create_test_fabric_registry();
    let (_temp, cartridge_dir, dev_binaries) = setup_test_env();

    let route = r#"
[test_large cap:in="media:void";test-large;out="media:"]
[input -> test_large -> output]
"#;

    let graph = parse_machine_to_cap_dag(route, &*registry)
        .await
        .expect("Parse failed");

    // test-large generates payload based on size, but with media:void input
    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("input".to_string(), NodeData::Bytes(vec![]));

    let initial_is_sequence = all_scalar(&initial_inputs);
    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        None, // dev-bin fixtures — no registry
        capdag::CartridgeChannel::Release,
        capdag::FABRIC_MANIFEST_VERSION,
        initial_inputs,
        initial_is_sequence,
        dev_binaries,
        None, // no bundled cartridges in these unit fixtures
        create_test_fabric_registry(),
        None,
        &test_pipeline_log_fn(),
        &std::collections::HashMap::new(),
        None,
    )
    .await
    .expect("Execution failed")
    .node_data;

    let output = outputs.get("output").expect("No output");

    let b = scalar_bytes(output);
    // Default size is 1MB
    assert_eq!(b.len(), 1_048_576);
    // Verify pattern: repeating 0-255
    for (i, &byte) in b.iter().enumerate() {
        assert_eq!(byte, (i % 256) as u8, "Pattern mismatch at byte {}", i);
    }
}

// TEST1316: Convergence — two producers routed into ONE cap via DISTINCT arg URNs.
//
// `A -edge1-> B(node2)`; `B -edge2-> D(node3)`; then `(B, D) -combine-> E`. `B` fans
// out (feeds both edge2 and combine). `combine` has a MAIN input on stdin (node2 = B)
// plus a distinct-URN second arg (node3 = D). This is the legal convergence the old
// two-stdin test951 was not: the resolver matches B→the main arg and D→the node3 arg,
// the plan/parser emit B as the main-input edge and D as a node3 `Arg` edge, and the
// cartridge `require_stream`s both by their distinct URNs. E must carry BOTH.
#[tokio::test]
async fn test1316_convergence_two_producers_distinct_arg_urns() {
    let registry = create_test_fabric_registry();
    let (_temp, cartridge_dir, dev_binaries) = setup_test_env();

    let route = r#"
[test_edge1 cap:in="media:enc=utf-8;node1";test-edge1;out="media:enc=utf-8;node2"]
[test_edge2 cap:in="media:enc=utf-8;node2";test-edge2;out="media:enc=utf-8;node3"]
[test_combine cap:in="media:enc=utf-8;node2";test-combine;out="media:enc=utf-8;combined"]
[A -> test_edge1 -> B]
[B -> test_edge2 -> D]
[(B, D) -> test_combine -> E]
"#;

    let graph = parse_machine_to_cap_dag(route, &*registry)
        .await
        .expect("Parse failed");

    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("A".to_string(), NodeData::Text("hello".to_string()));
    let initial_is_sequence = all_scalar(&initial_inputs);

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        None, // dev-bin fixtures — no registry
        capdag::CartridgeChannel::Release,
        capdag::FABRIC_MANIFEST_VERSION,
        initial_inputs,
        initial_is_sequence,
        dev_binaries,
        None,
        create_test_fabric_registry(),
        None,
        &test_pipeline_log_fn(),
        &std::collections::HashMap::new(),
        None,
    )
    .await
    .expect("Execution failed")
    .node_data;

    // B = edge1(hello) = "[PREPEND]hello"; D = edge2(B) = "[PREPEND]hello[APPEND]";
    // E = combine(B main, D arg) = "B|D". If convergence mis-routed (e.g. D not
    // delivered on its node3 URN), combine's require_stream(node3) would fail and
    // execution would error — so a passing assertion here proves both inputs arrived
    // by their distinct URNs.
    let e = scalar_bytes(outputs.get("E").expect("No E output"));
    let e_str = String::from_utf8(e).expect("Invalid UTF-8");
    assert_eq!(e_str, "[PREPEND]hello|[PREPEND]hello[APPEND]");
}

// TEST950: Validate that cycles are rejected
#[tokio::test]
async fn test950_reject_cycles() {
    let registry = create_test_fabric_registry();

    // Create a self-loop using identity cap
    let route = r#"
[identity cap:in="media:enc=utf-8;node1";identity;out="media:enc=utf-8;node1"]
[A -> identity -> A]
"#;

    let result = parse_machine_to_cap_dag(route, &*registry).await;
    assert!(result.is_err(), "Should reject cycle");

    match result.err() {
        Some(ParseOrchestrationError::NotADag { .. }) => {
            // Expected error
        }
        other => panic!("Expected NotADag error, got: {:?}", other),
    }
}

// TEST943: Two nodes with the same media type but different names are two
// distinct graph positions — NOT a loop. The identity cap has `in = out` by
// type, so its upstream and downstream node carry the same media URN; this
// must not collapse them into a self-loop. Node identity comes from the
// user-written name, not the media URN.
#[tokio::test]
async fn test943_same_media_different_names_is_not_a_cycle() {
    let registry = create_test_fabric_registry();

    let route = r#"
[identity cap:in="media:enc=utf-8;node1";identity;out="media:enc=utf-8;node1"]
[A -> identity -> B]
"#;

    let result = parse_machine_to_cap_dag(route, &*registry).await;
    let graph = result.expect("A -> identity -> B must parse: distinct names, not a cycle");
    assert_eq!(graph.edges.len(), 1, "single edge expected");
    assert_eq!(graph.edges[0].from, "A");
    assert_eq!(graph.edges[0].to, "B");
}

// TEST949: Empty machine notation (no edges)
#[tokio::test]
async fn test949_empty_graph() {
    let registry = create_test_fabric_registry();

    let route = "";

    let result = parse_machine_to_cap_dag(route, &*registry).await;
    assert!(result.is_err(), "Should fail on empty machine notation");

    match result.err() {
        Some(ParseOrchestrationError::MachineSyntaxParseFailed(_)) => {
            // Expected error
        }
        other => panic!("Expected MachineSyntaxParseFailed, got: {:?}", other),
    }
}

// TEST948: Invalid cap URN in machine notation
#[tokio::test]
async fn test948_invalid_cap_urn() {
    let registry = create_test_fabric_registry();

    let route = concat!(r#"[bad cap:INVALID]"#, "[A -> bad -> B]");

    let result = parse_machine_to_cap_dag(route, &*registry).await;
    assert!(result.is_err(), "Should reject invalid cap URN");
}

// TEST947: Cap not found in registry
#[tokio::test]
async fn test947_cap_not_found() {
    let registry = create_test_fabric_registry();

    let route = r#"
[nonexistent cap:in="media:unknown";nonexistent;out="media:unknown"]
[A -> nonexistent -> B]
"#;

    let result = parse_machine_to_cap_dag(route, &*registry).await;
    assert!(result.is_err(), "Should fail when cap not found");

    match result.err() {
        Some(ParseOrchestrationError::MachineSyntaxParseFailed(_)) => {
            // Expected: the parser resolves header caps and wraps lookup failure
        }
        other => panic!("Expected MachineSyntaxParseFailed, got: {:?}", other),
    }
}

// =============================================================================
// Phase 2: Long Chain Tests (4-6 caps)
// =============================================================================

// TEST946: 4-machine: edge1 -> edge2 -> edge7 -> edge8
// node1 -> node2 -> node3 -> node6 -> node7
// "hello" -> "[PREPEND]hello" -> "[PREPEND]hello[APPEND]" -> "[PREPEND]HELLO[APPEND]" -> "]DNEPPA[OLLEH]DNEPERP["
#[tokio::test]
async fn test946_four_machine() {
    let registry = create_test_fabric_registry();
    let (_temp, cartridge_dir, dev_binaries) = setup_test_env();

    let route = r#"
[test_edge1 cap:in="media:enc=utf-8;node1";test-edge1;out="media:enc=utf-8;node2"]
[test_edge2 cap:in="media:enc=utf-8;node2";test-edge2;out="media:enc=utf-8;node3"]
[test_edge7 cap:in="media:enc=utf-8;node3";test-edge7;out="media:enc=utf-8;node6"]
[test_edge8 cap:in="media:enc=utf-8;node6";test-edge8;out="media:enc=utf-8;node7"]
[A -> test_edge1 -> B]
[B -> test_edge2 -> C]
[C -> test_edge7 -> D]
[D -> test_edge8 -> E]
"#;

    let graph = parse_machine_to_cap_dag(route, &*registry)
        .await
        .expect("Parse failed");

    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("A".to_string(), NodeData::Text("hello".to_string()));

    let initial_is_sequence = all_scalar(&initial_inputs);
    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        None, // dev-bin fixtures — no registry
        capdag::CartridgeChannel::Release,
        capdag::FABRIC_MANIFEST_VERSION,
        initial_inputs,
        initial_is_sequence,
        dev_binaries,
        None, // no bundled cartridges in these unit fixtures
        create_test_fabric_registry(),
        None,
        &test_pipeline_log_fn(),
        &std::collections::HashMap::new(),
        None,
    )
    .await
    .expect("Execution failed")
    .node_data;

    let final_output = outputs.get("E").expect("No final output");

    let b = scalar_bytes(final_output);
    let output_str = String::from_utf8(b).expect("Invalid UTF-8");
    // edge1: [PREPEND]hello
    // edge2: [PREPEND]hello[APPEND]
    // edge7 (uppercase): [PREPEND]HELLO[APPEND]
    // edge8 (reverse): ]DNEPPA[OLLEH]DNEPERP[
    assert_eq!(output_str, "]DNEPPA[OLLEH]DNEPERP[");
}

// TEST945: 5-machine: edge1 -> edge2 -> edge7 -> edge8 -> edge9
// node1 -> node2 -> node3 -> node6 -> node7 -> node8
// adds <<...>> wrapping around the reversed string
#[tokio::test]
async fn test945_five_machine() {
    let registry = create_test_fabric_registry();
    let (_temp, cartridge_dir, dev_binaries) = setup_test_env();

    let route = r#"
[test_edge1 cap:in="media:enc=utf-8;node1";test-edge1;out="media:enc=utf-8;node2"]
[test_edge2 cap:in="media:enc=utf-8;node2";test-edge2;out="media:enc=utf-8;node3"]
[test_edge7 cap:in="media:enc=utf-8;node3";test-edge7;out="media:enc=utf-8;node6"]
[test_edge8 cap:in="media:enc=utf-8;node6";test-edge8;out="media:enc=utf-8;node7"]
[test_edge9 cap:in="media:enc=utf-8;node7";test-edge9;out="media:enc=utf-8;node8"]
[A -> test_edge1 -> B]
[B -> test_edge2 -> C]
[C -> test_edge7 -> D]
[D -> test_edge8 -> E]
[E -> test_edge9 -> F]
"#;

    let graph = parse_machine_to_cap_dag(route, &*registry)
        .await
        .expect("Parse failed");

    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("A".to_string(), NodeData::Text("hello".to_string()));

    let initial_is_sequence = all_scalar(&initial_inputs);
    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        None, // dev-bin fixtures — no registry
        capdag::CartridgeChannel::Release,
        capdag::FABRIC_MANIFEST_VERSION,
        initial_inputs,
        initial_is_sequence,
        dev_binaries,
        None, // no bundled cartridges in these unit fixtures
        create_test_fabric_registry(),
        None,
        &test_pipeline_log_fn(),
        &std::collections::HashMap::new(),
        None,
    )
    .await
    .expect("Execution failed")
    .node_data;

    let final_output = outputs.get("F").expect("No final output");

    let b = scalar_bytes(final_output);
    let output_str = String::from_utf8(b).expect("Invalid UTF-8");
    // Previous 4 caps: ]DNEPPA[OLLEH]DNEPERP[
    // edge9 (wrap): <<]DNEPPA[OLLEH]DNEPERP[>>
    assert_eq!(output_str, "<<]DNEPPA[OLLEH]DNEPERP[>>");
}

// TEST944: 6-machine: edge1 -> edge2 -> edge7 -> edge8 -> edge9 -> edge10
// Full cycle: node1 -> node2 -> node3 -> node6 -> node7 -> node8 -> node1
// Completes the round trip: unwrap markers + lowercase
#[tokio::test]
async fn test944_six_machine() {
    let registry = create_test_fabric_registry();
    let (_temp, cartridge_dir, dev_binaries) = setup_test_env();

    let route = r#"
[test_edge1 cap:in="media:enc=utf-8;node1";test-edge1;out="media:enc=utf-8;node2"]
[test_edge2 cap:in="media:enc=utf-8;node2";test-edge2;out="media:enc=utf-8;node3"]
[test_edge7 cap:in="media:enc=utf-8;node3";test-edge7;out="media:enc=utf-8;node6"]
[test_edge8 cap:in="media:enc=utf-8;node6";test-edge8;out="media:enc=utf-8;node7"]
[test_edge9 cap:in="media:enc=utf-8;node7";test-edge9;out="media:enc=utf-8;node8"]
[test_edge10 cap:in="media:enc=utf-8;node8";test-edge10;out="media:enc=utf-8;node1"]
[A -> test_edge1 -> B]
[B -> test_edge2 -> C]
[C -> test_edge7 -> D]
[D -> test_edge8 -> E]
[E -> test_edge9 -> F]
[F -> test_edge10 -> G]
"#;

    let graph = parse_machine_to_cap_dag(route, &*registry)
        .await
        .expect("Parse failed");

    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("A".to_string(), NodeData::Text("hello".to_string()));

    let initial_is_sequence = all_scalar(&initial_inputs);
    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        None, // dev-bin fixtures — no registry
        capdag::CartridgeChannel::Release,
        capdag::FABRIC_MANIFEST_VERSION,
        initial_inputs,
        initial_is_sequence,
        dev_binaries,
        None, // no bundled cartridges in these unit fixtures
        create_test_fabric_registry(),
        None,
        &test_pipeline_log_fn(),
        &std::collections::HashMap::new(),
        None,
    )
    .await
    .expect("Execution failed")
    .node_data;

    let final_output = outputs.get("G").expect("No final output");

    let b = scalar_bytes(final_output);
    let output_str = String::from_utf8(b).expect("Invalid UTF-8");
    // Previous 5 caps: <<]DNEPPA[OLLEH]DNEPERP[>>
    // edge10 (unwrap+lowercase): ]dneppa[olleh]dneperp[
    assert_eq!(output_str, "]dneppa[olleh]dneperp[");

    // v3 pipelining regime: this six-cap machine is one linear-chain segment,
    // so the intermediate nodes stream cap-to-cap and are deliberately NEVER
    // materialized into the result map — the correct terminal value above
    // proves every intermediate transformation ran, in order, through live
    // frame forwarding. Their absence is asserted so a regression back to
    // materialization (which would silently reintroduce the memory barrier)
    // is caught.
    for node in ["B", "C", "D", "E", "F"] {
        assert!(
            !outputs.contains_key(node),
            "pipelined intermediate node {} must not be materialized (L16 pipelining regime)",
            node
        );
    }
}

// =============================================================================
// Phase 3: Peer Invoke Testing (TEST394)
// =============================================================================

// TEST394: Test peer invoke round-trip (testcartridge calls itself)
// Disabled: LocalCartridgeRouter feature not implemented - uses non-existent modules
#[cfg(feature = "__disabled_local_cartridge_router")]
#[tokio::test]
#[ignore]
async fn test394_peer_invoke_roundtrip() {
    use capdag::local_cartridge_router::LocalCartridgeRouter;
    use capdag::{CapArgumentValue, CartridgeHost};
    use std::process::Stdio;
    use std::sync::Arc;
    use tokio::process::Command;

    let testcartridge = testcartridge_bin();

    // Create LocalCartridgeRouter for routing peer invoke requests
    let router = Arc::new(LocalCartridgeRouter::new());
    let router_arc: Arc<dyn capdag::cap_router::CapRouter> = router.clone();

    // Spawn testcartridge
    let mut child = Command::new(&testcartridge)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to spawn testcartridge");

    let stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();

    // Create host with router
    let host = CartridgeHost::new_with_router(stdin, stdout, router_arc)
        .await
        .expect("Failed to create host");

    // Get manifest to discover all caps
    let manifest_bytes = host.cartridge_manifest();
    let manifest: capdag::CapManifest =
        serde_json::from_slice(manifest_bytes).expect("Failed to parse manifest");

    let all_caps = manifest.all_caps();
    eprintln!(
        "[TEST394] Discovered {} caps from testcartridge",
        all_caps.len()
    );

    // Register all caps with the router (pointing to this same host)
    let host_arc = Arc::new(host);
    for cap in &all_caps {
        let cap_urn = cap.urn.to_string();
        eprintln!("[TEST394] Registering cap: {}", cap_urn);
        router
            .register_cartridge(&cap_urn, Arc::clone(&host_arc))
            .await;
    }

    // Now call test-peer, which will peer invoke test-edge1 and test-edge2
    let test_peer_urn = r#"cap:in="media:enc=utf-8;node1";test-peer;out="media:enc=utf-8;node5""#;
    let input_data = b"CHAIN".to_vec();
    let arguments = vec![CapArgumentValue::new("media:enc=utf-8;node1", input_data)];

    eprintln!("[TEST394] Calling test-peer with input: CHAIN");

    let mut response = host_arc
        .request_with_arguments(test_peer_urn, &arguments)
        .await
        .expect("Failed to call test-peer");

    // Collect response chunks
    let mut result_data = Vec::new();
    while let Some(chunk_result) = response.recv().await {
        match chunk_result {
            Ok(chunk) => {
                eprintln!("[TEST394] Received chunk: {} bytes", chunk.payload.len());
                result_data.extend_from_slice(&chunk.payload);
            }
            Err(e) => {
                panic!("Peer invoke failed: {:?}", e);
            }
        }
    }

    // Shutdown host (try_unwrap to get ownership)
    match Arc::try_unwrap(host_arc) {
        Ok(host) => host.shutdown().await,
        Err(_) => eprintln!("[TEST394] Warning: Could not unwrap host Arc, skipping shutdown"),
    }

    // Debug: print raw bytes
    eprintln!(
        "[TEST394] Raw response bytes: {:?}",
        &result_data[..std::cmp::min(result_data.len(), 30)]
    );

    // Decode CBOR response
    let cbor_value: ciborium::Value =
        ciborium::from_reader(&result_data[..]).expect("Failed to decode CBOR response");

    eprintln!("[TEST394] Decoded CBOR value: {:?}", cbor_value);

    // Extract bytes from CBOR value
    let result_bytes = match cbor_value {
        ciborium::Value::Bytes(b) => b,
        _ => panic!("Expected CBOR Bytes, got: {:?}", cbor_value),
    };

    let result_str = String::from_utf8(result_bytes).expect("Invalid UTF-8 in result");

    eprintln!("[TEST394] Final result: {}", result_str);

    // Expected flow:
    // 1. test-peer receives "CHAIN"
    // 2. Calls peer.invoke(test-edge1, "CHAIN") -> "[PREPEND]CHAIN"
    // 3. Calls peer.invoke(test-edge2, "[PREPEND]CHAIN") -> "[PREPEND]CHAIN[APPEND]"
    // 4. Returns final result
    assert_eq!(
        result_str, "[PREPEND]CHAIN[APPEND]",
        "Peer invoke chain should prepend and append correctly"
    );
}

// =============================================================================
// Host-mediated live capture → ForEach region (13.2 §Reference Media)
// =============================================================================

// TEST11011: a live source drives a ForEach region END TO END through the
// REAL stack — the CLI runtime hosts the actual testcartridge process on a
// real relay switch, the HOST opens the built-in `media:live;synthetic`
// feed itself through the same capture dispatch the hardware backends use,
// and one body runs per delivered item while the feed runs. No mocks
// anywhere: real capture bridge, real region driver, real cartridge
// invocations, real persisted body outputs.
#[tokio::test]
async fn test11011_live_synthetic_foreach_region_end_to_end() {
    use capdag::orchestrator::{execute_plan, CliRuntime, EngineRuntime, PlanInput};
    use capdag::planner::{InputCardinality, MachineNode, MachinePlan, MachinePlanEdge};

    let registry = create_test_fabric_registry();
    let (_temp, cartridge_dir, dev_binaries) = setup_test_env();
    let persist_dir = _temp.path().join("outputs");

    // input (live synthetic, sequence) → fe → mapper (test-edge1, one item
    // per body) → out. The planner produces exactly this shape when a
    // scalar-consuming cap is planned over live content.
    let mut plan = MachinePlan::new("live-region-e2e");
    plan.add_node(MachineNode::input_slot(
        "input",
        "input",
        "media:feed-frames",
        InputCardinality::Sequence,
    ));
    plan.add_node(MachineNode::cap(
        "mapper",
        r#"cap:in="media:enc=utf-8;node1";test-edge1;out="media:enc=utf-8;node2""#,
    ));
    plan.add_node(MachineNode::for_each_token(
        "fe",
        "input",
        "mapper",
        "mapper",
        "live-e2e-token".parse().unwrap(),
    ));
    plan.add_node(MachineNode::output("out", "result", "mapper"));
    plan.add_edge(MachinePlanEdge::direct("input", "fe"));
    plan.add_edge(MachinePlanEdge::iteration("fe", "mapper"));
    plan.add_edge(MachinePlanEdge::direct("mapper", "out"));

    let runtime: Arc<dyn EngineRuntime> = Arc::new(CliRuntime::new(
        cartridge_dir,
        None, // no registry URL — dev binaries only, no network
        capdag::CartridgeChannel::Release,
        capdag::FABRIC_MANIFEST_VERSION,
        dev_binaries,
        None,
        registry.clone(),
        None,
        persist_dir,
    ));

    let inputs = HashMap::from([(
        "input".to_string(),
        PlanInput::LiveReference {
            reference_urn: "media:live;synthetic".to_string(),
            selector: br#"{"params":{"items":3,"interval_ms":0,"item_bytes":4}}"#.to_vec(),
        },
    )]);
    let flags = HashMap::from([("input".to_string(), true)]);

    let result = execute_plan(
        &plan,
        runtime,
        inputs,
        flags,
        &capdag::RunArgumentLedger::new(&plan, HashMap::new()).expect("empty ledger"),
        None,
        None,
        Some(&test_pipeline_log_fn()),
        None,
        None,
        None,
    )
    .await
    .expect("a live source must drive the region through the real stack");

    // One persisted body output per delivered feed item, in item order: the
    // CLI runtime persists terminal sinks, so the region terminal carries
    // writer results (one blob per body) instead of in-memory items.
    let terminal = result.terminal("out").expect("region terminal");
    assert!(terminal.is_sequence, "a region terminal is a sequence");
    assert_eq!(
        terminal.writer_results.len(),
        3,
        "one persisted body output per feed item"
    );
    for (i, writer) in terminal.writer_results.iter().enumerate() {
        assert_eq!(
            writer.saved_paths.len(),
            1,
            "body {i} persisted exactly one blob"
        );
        let bytes =
            fs::read(&writer.saved_paths[0]).expect("persisted body output is on disk");
        // The synthetic feed emits [i % 256; item_bytes]; test-edge1 prepends.
        let mut expected = b"[PREPEND]".to_vec();
        expected.extend(std::iter::repeat(i as u8).take(4));
        assert_eq!(
            bytes, expected,
            "body {i} ran on the feed's actual item bytes"
        );
    }
}
