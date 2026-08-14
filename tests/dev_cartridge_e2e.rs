//! End-to-end test of the `capdag` cartridge-development flow:
//! `new` → `dev-install` → run → edit → `dev-install` (update) → run.
//!
//! This drives the REAL `capdag` binary and the REAL scaffolded Python cartridge
//! through the full bifaci host. It is hermetic (no network): a local mock fabric
//! serves an empty manifest, so `capdag <alias>` misses the fabric and falls back
//! to the locally dev-installed cartridge's own manifest (the local-manifest dev
//! path).
//!
//! The scaffolded cartridge does no inference itself — it PEER-CALLS
//! `classify-en`. In a real install that call is answered by a BUNDLED model
//! cartridge sitting beside the capdag binary; a `cargo test` build has no
//! bundled tree, and a real model cartridge would download weights and want a
//! GPU, so neither is usable here. The test supplies a stand-in through
//! `--dev-bins`, the same affordance a developer uses to substitute a local
//! cartridge binary — so the host is assembled exactly as production assembles
//! it, with one participant swapped rather than the routing rules changed.
//!
//! That makes the flow this exercises strictly larger than before: the peer call
//! itself, with arguments addressed by media URN and the peer's progress
//! forwarded through the caller.
//!
//! It REQUIRES a Python runtime with `capdag`, `cbor2`, and `ops` importable —
//! the scaffolded cartridge is launched via its `#!/usr/bin/env python3` shebang,
//! so `python3` on PATH must have them. Under a workspace test run the machinefabric conda
//! env provides all three; the test fails loudly (never silently skips) if they
//! are missing.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const CAPDAG_BIN: &str = env!("CARGO_BIN_EXE_capdag");

/// This crate's own directory. Fixtures under `tests/` are resolved from here
/// rather than from a parent, so moving the crate cannot silently break them.
fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// The capdag superrepo — the crate's parent, holding the sibling mirrors.
fn capdag_root() -> PathBuf {
    crate_dir()
        .parent()
        .expect("capdag-rs has a parent (the capdag superrepo)")
        .to_path_buf()
}

/// Put the in-repo Python mirror on PYTHONPATH so the scaffolded cartridge
/// imports the LIVE `capdag` source.
///
/// It must be named explicitly: `run_capdag` overrides HOME, which takes the
/// user site-packages directory (and any editable install living there) out of
/// the child's sys.path. `tagged_urn`, `ops` and `cbor2` are NOT listed — each
/// lives in its own repository now and is consumed as a published package
/// (`tagged-urn`, `opsx-py`) from the interpreter's real site-packages, so a
/// missing install fails loudly.
fn pythonpath() -> String {
    capdag_root().join("capdag-py/src").display().to_string()
}

/// Spawn a mock fabric HTTP server on an ephemeral port that returns an empty
/// manifest for every request. Returns the base URL. The listener thread is
/// detached and lives for the rest of the process.
fn spawn_mock_fabric() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock fabric");
    let addr = listener.local_addr().expect("mock fabric addr");
    // The manifest's `version` is the version the CLIENT asked for: the
    // registry fetches `manifest-v{N}.json` and refuses a body that reports a
    // different N — a stale mirror serving an old manifest under a new name is
    // exactly the failure that guard exists to catch. So the mock reports the
    // version this build is pinned to rather than a literal, and bumping
    // `fabric/manifest-version.txt` can never leave this test behind.
    let body = format!(
        r#"{{"version":{},"previous":0,"caps":{{}},"media":{{}},"aliases":{{}}}}"#,
        capdag::FABRIC_MANIFEST_VERSION
    );
    std::thread::spawn(move || {
        let body = body;
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut buf = [0u8; 2048];
            let _ = stream.read(&mut buf); // drain the request; we answer every path the same
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes());
        }
    });
    format!("http://{addr}")
}

/// True if `python3` on PATH can import the cartridge runtime dependencies.
fn python_runtime_available(pythonpath: &str) -> bool {
    Command::new("python3")
        .args(["-c", "import capdag, cbor2, tagged_urn; from ops import Op"])
        .env("PYTHONPATH", pythonpath)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Run the capdag binary with `args` (and optional piped stdin), returning
/// `(trimmed stdout, success, stderr)`. PATH is inherited so the cartridge's
/// shebang resolves to the same `python3` the runtime check used.
fn run_capdag(
    home: &Path,
    fabric: &str,
    pythonpath: &str,
    args: &[&str],
    stdin: Option<&str>,
) -> (String, bool, String) {
    let mut cmd = Command::new(CAPDAG_BIN);
    cmd.args(args)
        .env("HOME", home)
        .env("PYTHONPATH", pythonpath)
        .env("CDG_FABRIC_REGISTRY_URL", fabric)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd.stdin(if stdin.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    let mut child = cmd.spawn().expect("spawn capdag");
    if let Some(s) = stdin {
        child
            .stdin
            .take()
            .expect("stdin piped")
            .write_all(s.as_bytes())
            .expect("write stdin");
    }
    let out = child.wait_with_output().expect("capdag output");
    (
        String::from_utf8_lossy(&out.stdout).trim().to_string(),
        out.status.success(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

// TEST8110: the full cartridge-development loop through the real CLI and a real
// Python cartridge — scaffold, install under the dev slug, run the custom cap
// (never published to the fabric), edit its logic, re-install to update, and
// observe the changed behavior.
#[test]
fn test8110_dev_cartridge_create_install_run_update() {
    let pp = pythonpath();
    assert!(
        python_runtime_available(&pp),
        "this e2e requires a Python runtime with capdag + cbor2 + ops importable. \
         `ops` comes from the published `opsx-py` package: pip install opsx-py. \
         PYTHONPATH={pp}"
    );
    let fabric = spawn_mock_fabric();

    let tmp = std::env::temp_dir().join(format!("capdag-dev-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let home = tmp.join("home");
    let projects = tmp.join("projects");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&projects).unwrap();

    let name = "e2e-tagger";

    // The stand-in for the model cartridge that answers `classify-en`. Passed
    // with `--dev-bins` on every run of the tagger: it is standing in for a
    // BUNDLED cartridge, not a dev-installed one, and `--dev-bins` is the
    // affordance that substitutes a local cartridge binary without changing how
    // the host resolves anything else.
    let classifier = crate_dir()
        .join("tests/fixtures/e2e_classify_standin/cartridge.py")
        .display()
        .to_string();
    // `--dev-bins` consumes following non-flag tokens, so the cap alias must not
    // trail it; the alias leads and the flag comes after.
    let run_tagger: Vec<&str> = vec![name, "--dev-bins", &classifier];

    // 1. Scaffold a new Python cartridge.
    let (proj_out, ok, err) = run_capdag(
        &home,
        &fabric,
        &pp,
        &["new", name, "--python", "-o", projects.to_str().unwrap()],
        None,
    );
    assert!(ok, "`new` failed: {err}");
    let proj = PathBuf::from(&proj_out);
    assert!(
        proj.join("cartridge.py").is_file(),
        "scaffold wrote cartridge.py"
    );

    // 2. Install it under the local `dev` slug.
    let (_, ok, err) = run_capdag(
        &home,
        &fabric,
        &pp,
        &["dev-install", proj.to_str().unwrap()],
        None,
    );
    assert!(ok, "`dev-install` failed: {err}");

    // 3. Run the custom cap through the capdag host — it is NOT in the fabric,
    //    so this exercises the local-manifest dev path end to end, including the
    //    peer call out to the classifier.
    let (out, ok, err) = run_capdag(&home, &fabric, &pp, &run_tagger, Some("I love this good great"));
    assert!(ok, "run failed: {err}");
    assert_eq!(out, "positive", "positive input; stderr:\n{err}");

    let (out, _, err) = run_capdag(
        &home,
        &fabric,
        &pp,
        &run_tagger,
        Some("awful terrible bad hate"),
    );
    assert_eq!(out, "negative", "negative input; stderr:\n{err}");

    let (out, _, _) = run_capdag(&home, &fabric, &pp, &run_tagger, Some("the sky is blue"));
    assert_eq!(out, "neutral", "neutral input before the edit");

    // 4. Edit the cartridge — shout the label instead of returning it plain —
    //    then update the install by re-running dev-install.
    //
    //    The edit is on the OUTPUT rather than on the judgment, because the
    //    judgment is the peer's to make: this cartridge owns what it does with
    //    the answer, and that is what a developer edits here.
    let src_path = proj.join("cartridge.py");
    let src = std::fs::read_to_string(&src_path).unwrap();
    let edited = src.replace("emitter.emit_cbor(label)", "emitter.emit_cbor(label.upper())");
    assert_ne!(
        src, edited,
        "the edit anchor `emitter.emit_cbor(label)` is no longer in the scaffolded \
         cartridge — the stub changed, so update this test with it"
    );
    std::fs::write(&src_path, &edited).unwrap();

    let (_, ok, err) = run_capdag(
        &home,
        &fabric,
        &pp,
        &["dev-install", proj.to_str().unwrap()],
        None,
    );
    assert!(ok, "update `dev-install` failed: {err}");

    // 5. The same input now renders differently — the update took effect.
    let (out, _, err) = run_capdag(&home, &fabric, &pp, &run_tagger, Some("the sky is blue"));
    assert_eq!(
        out, "NEUTRAL",
        "update did not take effect; stderr:\n{err}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}
