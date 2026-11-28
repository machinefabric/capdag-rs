//! End-to-end test of the `capdag` cartridge-development flow:
//! `dev new` → `dev install` → `run` → edit → `dev install` (update) → `run`.
//!
//! This drives the REAL `capdag` binary and the REAL scaffolded Python cartridge
//! through the full bifaci host, against the real fabric the run selected
//! (`CDG_FABRIC_REGISTRY_URL`, else the one the build baked). A fabric exists
//! only through its charter, verified against the MachineFabric roots the
//! binary carries, so no local stand-in can be one. The custom cap is in no
//! fabric, so `capdag run <alias>` misses it and falls back to the locally
//! installed dev cartridge's own manifest (the local-manifest dev path).
//!
//! The scaffolded cartridge does no inference itself — it PEER-CALLS
//! `classify-en`. In a real install that call is answered by a BUNDLED model
//! cartridge sitting beside the capdag binary; a `cargo test` build has no
//! bundled tree, and a real model cartridge would download weights and want a
//! GPU, so neither is usable here. The test supplies a stand-in through
//! `--dev-bin`, the same affordance a developer uses to substitute a local
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

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const CAPDAG_BIN: &str = env!("CARGO_BIN_EXE_capdag");

/// This crate's own directory. Fixtures under `tests/` are resolved from here
/// rather than from a parent, so moving the crate cannot silently break them.
fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// True if the interpreter the runtime starts a cartridge with can import the
/// cartridge's dependencies.
///
/// All of them from that interpreter's own site-packages, and nothing from a
/// source tree: capdag-py compiles the program generated from capdag's proved
/// model when it is installed, so its `src/` cannot be imported by path. The
/// workspace's `requirements.txt` installs capdag-py and tagged-urn-py editable,
/// which builds that program and keeps imports reading the LIVE source. An
/// inherited PYTHONPATH is removed so it cannot shadow the installation.
fn python_runtime_available() -> bool {
    // The interpreter the runtime will actually start the cartridge with, asked
    // of the runtime. This named `python3`, which a Windows install does not
    // have — it ships `python.exe` — so the check failed there while the
    // launcher, which knows that, would have run the cartridge perfectly well.
    let (interpreter, _) = capdag::bifaci::launch::launcher(Path::new("cartridge.py"));
    Command::new(interpreter)
        .args(["-c", "import capdag, cbor2, tagged_urn; from ops import Op"])
        .env_remove("PYTHONPATH")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Run the capdag binary with `args` (and optional piped stdin), returning
/// `(trimmed stdout, success, stderr)`. PATH is inherited so the cartridge's
/// launcher resolves the same interpreter the runtime check used.
fn run_capdag(home: &Path, args: &[&str], stdin: Option<&str>) -> (String, bool, String) {
    let mut cmd = Command::new(CAPDAG_BIN);
    cmd.args(args)
        .env("HOME", home)
        .env_remove("PYTHONPATH")
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
    assert!(
        python_runtime_available(),
        "this e2e requires the interpreter the runtime launches cartridges with to have \
         capdag, tagged-urn, cbor2 and ops installed: install the workspace's \
         requirements.txt with it (`python -m pip install -r requirements.txt`)"
    );
    let tmp = std::env::temp_dir().join(format!("capdag-dev-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let home = tmp.join("home");
    let projects = tmp.join("projects");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&projects).unwrap();

    let name = "e2e-tagger";

    // The stand-in for the model cartridge that answers `classify-en`. Passed
    // with `--dev-bin` on every run of the tagger: it is standing in for a
    // BUNDLED cartridge, not a dev cartridge, and `--dev-bin` is the
    // affordance that substitutes a local cartridge binary without changing how
    // the host resolves anything else.
    let classifier = crate_dir()
        .join("tests/fixtures/e2e_classify_standin/cartridge.py")
        .display()
        .to_string();
    // A capability runs as a machine of one step; its input is stdin (`-`).
    let machine = format!("[in -> {name} -> out]");
    let run_tagger: Vec<&str> = vec!["run", &machine, "-", "--dev-bin", &classifier];

    // 1. Scaffold a new Python cartridge.
    let project_path = projects.join(name);
    let (proj_out, ok, err) = run_capdag(
        &home,
        &["dev", "new", project_path.to_str().unwrap(), "--lang", "python"],
        None,
    );
    assert!(ok, "`dev new` failed: {err}");
    let proj = PathBuf::from(&proj_out);
    assert!(
        proj.join("cartridge.py").is_file(),
        "scaffold wrote cartridge.py"
    );

    // 2. Install it under the local `dev` slug.
    let (_, ok, err) = run_capdag(
        &home,
        &["dev", "install", proj.to_str().unwrap()],
        None,
    );
    assert!(ok, "`dev install` failed: {err}");

    // 3. Run the custom cap through the capdag host — it is NOT in the fabric,
    //    so this exercises the local-manifest dev path end to end, including the
    //    peer call out to the classifier.
    let (out, ok, err) = run_capdag(&home, &run_tagger, Some("I love this good great"));
    assert!(ok, "run failed: {err}");
    assert_eq!(out, "positive", "positive input; stderr:\n{err}");

    let (out, _, err) = run_capdag(
        &home,
        &run_tagger,
        Some("awful terrible bad hate"),
    );
    assert_eq!(out, "negative", "negative input; stderr:\n{err}");

    let (out, _, _) = run_capdag(&home, &run_tagger, Some("the sky is blue"));
    assert_eq!(out, "neutral", "neutral input before the edit");

    // 4. Edit the cartridge — shout the label instead of returning it plain —
    //    then update the install by running `dev install` again.
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
        &["dev", "install", proj.to_str().unwrap()],
        None,
    );
    assert!(ok, "update `dev install` failed: {err}");

    // 5. The same input now renders differently — the update took effect.
    let (out, _, err) = run_capdag(&home, &run_tagger, Some("the sky is blue"));
    assert_eq!(
        out, "NEUTRAL",
        "update did not take effect; stderr:\n{err}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

/// The media a `capdag run <file> --dry-run --json` took each file as, from its `input`
/// records, by file.
fn planned_inputs(stdout: &str) -> std::collections::BTreeMap<String, String> {
    stdout
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|record| record["type"] == "input")
        .map(|record| {
            let file = record["file"].as_str().expect("an input record names its file");
            let urn = record["urn"].as_str().expect("an input record names its media URN");
            (Path::new(file).file_name().unwrap().to_string_lossy().into_owned(), urn.to_string())
        })
        .collect()
}

// TEST8107: the capdag command tells what a file is the way the desktop
// clients do: when a file's candidates fall under a cartridge's content
// adapter, it starts that cartridge and asks it. A dev-installed cartridge that
// inspects JSON proves an object is a record and an array of objects a list of
// records; without it nothing does, and every .json file is plain JSON.
#[test]
fn test8107_the_command_asks_the_cartridge_that_inspects_a_file() {
    assert!(
        python_runtime_available(),
        "this e2e requires the interpreter the runtime launches cartridges with to have \
         capdag, tagged-urn, cbor2 and ops installed: install the workspace's \
         requirements.txt with it (`python -m pip install -r requirements.txt`)"
    );
    const JSON: &str = "media:enc=utf8;ext=json;fmt=json";
    const RECORD: &str = "media:enc=utf8;ext=json;fmt=json;record";
    const LIST_OF_RECORDS: &str = "media:enc=utf8;ext=json;fmt=json;list;record";

    let tmp = std::env::temp_dir().join(format!("capdag-inspect-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let home = tmp.join("home");
    let files = tmp.join("files");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&files).unwrap();
    for (name, content) in [
        ("person.json", r#"{"name": "ann"}"#),
        ("people.json", r#"[{"name": "ann"}, {"name": "bo"}]"#),
        ("numbers.json", "[1, 2, 3]"),
    ] {
        std::fs::write(files.join(name), content).unwrap();
    }
    let plan = |file: &str| -> std::collections::BTreeMap<String, String> {
        let path = files.join(file).display().to_string();
        let (out, _, err) = run_capdag(&home, &["--json", "--no-input", "run", &path, "--dry-run"], None);
        let found = planned_inputs(&out);
        assert!(found.contains_key(file), "`capdag run {file} --dry-run` named no input for it; stdout:\n{out}\nstderr:\n{err}");
        found
    };

    // No cartridge inspects JSON: each file is what its extension names.
    for file in ["person.json", "people.json", "numbers.json"] {
        assert_eq!(plan(file)[file], JSON, "{file} with no inspecting cartridge installed");
    }

    // The project directory is named for the cartridge, as `dev install` reads it.
    let project = tmp.join("json-inspector");
    std::fs::create_dir_all(&project).unwrap();
    let fixture = crate_dir().join("tests/fixtures/json-inspector/cartridge.py");
    let entry = project.join("cartridge.py");
    std::fs::copy(&fixture, &entry).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&entry, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let (_, ok, err) = run_capdag(&home, &["dev", "install", project.to_str().unwrap()], None);
    assert!(ok, "`dev install` of the inspecting cartridge failed: {err}");

    // Installed, it is asked, and what it proves is what each file is.
    assert_eq!(plan("person.json")["person.json"], RECORD, "an object, asked of the inspecting cartridge");
    assert_eq!(plan("people.json")["people.json"], LIST_OF_RECORDS, "an array of objects, asked of it");
    assert_eq!(plan("numbers.json")["numbers.json"], JSON, "an array of numbers, which it says is plain JSON");

    let _ = std::fs::remove_dir_all(&tmp);
}

/// The `bundle.json` under a bundled-cartridges root.
fn read_bundle_manifest(root: &Path) -> serde_json::Value {
    let bytes = std::fs::read(root.join("bundle.json")).expect("bundle.json was written");
    serde_json::from_slice(&bytes).expect("bundle.json is JSON")
}

// TEST8056: a build records its bundle manifest before it signs the bundled
// cartridges for their platform, and hashes it again afterwards without
// starting them. On macOS the build phase signs each bundled cartridge into the
// app sandbox, and a cartridge signed so traps when anything but its app starts
// it: asking it for its caps then fails, and the build stopped there. The
// stand-in here is a cartridge whose entry is rewritten, after the record, into
// one that aborts as soon as it is started.
#[test]
fn test8056_a_bundle_signed_for_its_platform_is_hashed_again_without_starting_a_cartridge() {
    assert!(
        python_runtime_available(),
        "this e2e requires the interpreter the runtime launches cartridges with to have \
         capdag, tagged-urn, cbor2 and ops installed: install the workspace's \
         requirements.txt with it (`python -m pip install -r requirements.txt`)"
    );
    let tmp = std::env::temp_dir().join(format!("capdag-bundle-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let home = tmp.join("home");
    std::fs::create_dir_all(&home).unwrap();

    // Staged as a build stages a bundled cartridge:
    // `{slug}/v{n}/{channel}/{name}/{version}/`, its cartridge.json beside it.
    let root = tmp.join("bundled-cartridges");
    let fabric = capdag::FABRIC_MANIFEST_VERSION;
    let dir = root.join(format!("dev/v{fabric}/nightly/json-inspector/0.1.0"));
    std::fs::create_dir_all(&dir).unwrap();
    let entry = dir.join("cartridge.py");
    std::fs::copy(crate_dir().join("tests/fixtures/json-inspector/cartridge.py"), &entry).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&entry, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let record = serde_json::json!({
        "name": "json-inspector", "version": "0.1.0", "channel": "nightly", "registry_url": null,
        "entry": "cartridge.py", "installed_at": "2026-11-16T00:00:00Z", "installed_from": "bundle",
        "fabric_manifest_version": fabric,
    });
    std::fs::write(dir.join("cartridge.json"), serde_json::to_vec_pretty(&record).unwrap()).unwrap();
    let root_arg = root.to_str().unwrap();

    // Recorded as soon as it is staged: the cartridge is started once and its
    // HELLO gives the caps.
    let (_, ok, err) = run_capdag(&home, &["dev", "bundle-manifest", "--root", root_arg, "--environment", "dev"], None);
    assert!(ok, "recording the staged bundle failed: {err}");
    let recorded = read_bundle_manifest(&root);
    let recorded_entry = &recorded["cartridges"][0];
    assert_eq!(recorded_entry["name"], "json-inspector");
    let groups = recorded_entry["cap_groups"].as_array().expect("cap_groups is a list");
    assert_eq!(groups.len(), 1, "the HELLO's one cap group: {recorded_entry}");
    assert_eq!(groups[0]["adapter_urns"][0], "media:enc=utf8;fmt=json", "{recorded_entry}");

    // Signed for the platform: its bytes change, and it no longer starts.
    std::fs::write(&entry, "#!/usr/bin/env python3\nimport os\nos.abort()\n").unwrap();
    let (_, ok, err) = run_capdag(&home, &["dev", "bundle-manifest", "--root", root_arg, "--environment", "dev"], None);
    assert!(!ok, "a cartridge that cannot start cannot be asked for its caps");
    assert!(err.contains("json-inspector 0.1.0 could not be asked for its caps"), "{err}");

    // Hashed again: nothing is started, the caps are the recorded ones, and
    // the hash is the signed bytes'.
    let (_, ok, err) = run_capdag(&home, &["dev", "bundle-rehash", "--root", root_arg, "--environment", "dev"], None);
    assert!(ok, "hashing the signed bundle again failed: {err}");
    let sealed = read_bundle_manifest(&root);
    let sealed_entry = &sealed["cartridges"][0];
    assert_eq!(sealed_entry["cap_groups"], recorded_entry["cap_groups"], "the caps are carried from the record");
    assert_eq!(sealed_entry["fabric_manifest_version"], fabric);
    let (hash, ok, err) = run_capdag(&home, &["dev", "hash-dir", dir.to_str().unwrap()], None);
    assert!(ok, "{err}");
    assert_eq!(sealed_entry["sha256"], hash.as_str(), "the hash is the signed directory's");
    assert_ne!(sealed_entry["sha256"], recorded_entry["sha256"]);
    assert!(!root.join("bundle.json.sig").exists(), "no signature of earlier bytes is left beside it");

    let _ = std::fs::remove_dir_all(&tmp);
}
