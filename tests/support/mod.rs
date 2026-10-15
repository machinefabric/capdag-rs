//! What the end-to-end tests share.

use std::path::{Path, PathBuf};

/// The cargo arguments that build a test cartridge against THIS capdag.
///
/// A cartridge built to test capdag has to be built against the capdag under
/// test. Its manifest names capdag two ways: a path to the working copy, or —
/// as it is committed, and as a release sees it — the last RELEASED tag. Under
/// the second, the suite that decides whether this capdag may be released built
/// its cartridge against the release before, which no longer compiled against
/// the tagged-urn this one requires, and every test that needed the cartridge
/// failed on a build that had nothing to do with it.
///
/// So a capdag taken from a git source is patched to this crate. A manifest
/// that already names a path has nothing to patch.
///
/// The patch alone is not enough where the cartridge has been built before:
/// its `Cargo.lock` holds the released capdag, and cargo keeps a locked entry
/// over a patch it has not resolved — it built the released one again and said
/// only `patch … was not used in the crate graph`. [`resolve_this_capdag`]
/// moves that one entry first.
pub fn against_this_capdag(cartridge_dir: &Path) -> Vec<String> {
    let manifest_path = cartridge_dir.join("Cargo.toml");
    let manifest = std::fs::read_to_string(&manifest_path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", manifest_path.display()));
    let Some(source) = capdag_git_source(&manifest) else {
        return Vec::new();
    };
    vec![
        "--config".to_string(),
        format!("patch.'{source}'.capdag.path='{}'", env!("CARGO_MANIFEST_DIR")),
    ]
}

/// Resolve the cartridge's `capdag` to this crate before it is built: `cargo
/// update capdag` under the same patch, which moves that one lock entry and
/// leaves every other dependency where the lock has it. Nothing is run for a
/// manifest that names a path. `tag` prefixes what is printed.
pub fn resolve_this_capdag(cartridge_dir: &Path, tag: &str) {
    let patch = against_this_capdag(cartridge_dir);
    if patch.is_empty() {
        return;
    }
    let output = std::process::Command::new("cargo")
        .args(["update", "capdag"])
        .args(&patch)
        .current_dir(cartridge_dir)
        .output()
        .expect("Failed to run cargo update for the test cartridge");
    for line in String::from_utf8_lossy(&output.stderr).lines() {
        eprintln!("[{tag}]   {line}");
    }
    assert!(
        output.status.success(),
        "could not resolve the test cartridge against the capdag under test (exit code: {:?})",
        output.status.code()
    );
}

/// Where cargo builds the test cartridge in `cartridge_dir`: cargo's own
/// answer, `target_directory` of `cargo metadata`.
///
/// Asked, never worked out. A target directory is not always `target/` beside
/// the manifest — it follows `CARGO_TARGET_DIR`, Cargo configuration, and a
/// cargo that keeps build output in a directory of its own — and the build
/// below runs under whichever of those this test run has. Two earlier
/// versions of this derived the directory from the checkout's layout, and
/// each looked for the binary somewhere the build had not put it.
pub fn target_dir_of(cartridge_dir: &Path) -> PathBuf {
    let output = std::process::Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .current_dir(cartridge_dir)
        .output()
        .expect("Failed to run cargo metadata for the test cartridge");
    assert!(
        output.status.success(),
        "cargo metadata failed in {}: {}",
        cartridge_dir.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("cargo metadata prints JSON");
    PathBuf::from(
        metadata["target_directory"]
            .as_str()
            .expect("cargo metadata names the target directory"),
    )
}

/// The git URL a manifest takes `capdag` from, if it takes it from one.
fn capdag_git_source(manifest: &str) -> Option<String> {
    let line = manifest
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("capdag") && line.contains('=') && line.contains("git"))?;
    let after = &line[line.find("git")? + 3..];
    let start = after.find('"')? + 1;
    let end = after[start..].find('"')?;
    Some(after[start..start + end].to_string())
}

/// The testcartridge source directory.
///
/// The crate sits at `machinefabric/capdag/capdag-rs`, so the workspace holding
/// `mfab-tests` is two levels up. Missing is a hard error here rather than an
/// opaque spawn failure later: `Command::current_dir` on an absent directory
/// reports only `NotFound`, which reads as a missing `cargo`.
pub fn testcartridge_dir() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
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

/// The testcartridge binary, built against this capdag.
///
/// Built by cargo, once per test binary. Whether an existing binary is stale
/// is cargo's decision, never this harness's: the cartridge links this
/// library, and a check of the cartridge's own sources alone ran a cartridge
/// built against an earlier library — which answers by an earlier protocol.
pub fn testcartridge_bin(tag: &str) -> PathBuf {
    static BUILT: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    BUILT
        .get_or_init(|| {
            let cart_dir = testcartridge_dir();
            resolve_this_capdag(&cart_dir, tag);
            let output = std::process::Command::new("cargo")
                .arg("build")
                .arg("--release")
                .args(against_this_capdag(&cart_dir))
                .current_dir(&cart_dir)
                .output()
                .expect("Failed to run cargo build for testcartridge");
            if !output.status.success() {
                for line in String::from_utf8_lossy(&output.stderr).lines() {
                    eprintln!("[{tag}]   {line}");
                }
                panic!(
                    "Failed to build testcartridge (exit code: {:?})",
                    output.status.code()
                );
            }
            // Where cargo built it, as cargo says: the build ran under the same
            // cargo and the same environment. With the platform's executable
            // suffix: on Windows the build writes `testcartridge.exe`.
            let bin_path = target_dir_of(&cart_dir)
                .join("release")
                .join(format!("testcartridge{}", std::env::consts::EXE_SUFFIX));
            assert!(
                bin_path.exists(),
                "testcartridge binary not found at {:?} after a successful build",
                bin_path
            );
            bin_path
        })
        .clone()
}

// =============================================================================
// Running machines on the test cartridge
// =============================================================================

/// The cap definitions the test cartridge serves: its own manifest, as its
/// binary prints it. The tests resolve machines against exactly what the
/// cartridge they run says of itself — a registry built by hand beside it
/// drifts from it.
#[allow(dead_code)]
pub fn testcartridge_caps(tag: &str) -> Vec<capdag::Cap> {
    let bin = testcartridge_bin(tag);
    let output = std::process::Command::new(&bin)
        .arg("manifest")
        .output()
        .expect("Failed to run the test cartridge for its manifest");
    assert!(
        output.status.success(),
        "`testcartridge manifest` failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let manifest = capdag::CapManifest::from_advertised(&output.stdout)
        .expect("the test cartridge prints a manifest of valid cap definitions");
    manifest.all_caps().into_iter().cloned().collect()
}

/// A fabric registry holding the test cartridge's caps, plus `extra` — and a
/// media definition for every arg of them, which is what a constant written
/// in a machine is held to.
#[allow(dead_code)]
pub fn testcartridge_registry(tag: &str, extra: Vec<capdag::Cap>) -> std::sync::Arc<capdag::FabricRegistry> {
    let registry = capdag::FabricRegistry::new_for_test();
    let mut caps = testcartridge_caps(tag);
    caps.extend(extra);
    for cap in &caps {
        for arg in cap.get_args() {
            registry.insert_cached_media_def_for_test(capdag::StoredMediaDef {
                version: 0,
                urn: arg.media_urn.clone(),
                media_type: "application/octet-stream".to_string(),
                title: arg.name.clone(),
                profile_uri: None,
                schema: None,
                description: None,
                documentation: None,
                validation: None,
                metadata: None,
                extensions: Vec::new(),
            });
        }
    }
    registry.add_caps_to_cache(caps);
    std::sync::Arc::new(registry)
}

/// What a machine input is given.
#[allow(dead_code)]
pub enum Given<'a> {
    One(&'a [u8]),
    Many(&'a [&'a [u8]]),
    Live { reference_urn: &'a str, selector: &'a [u8] },
}

/// What one run of a machine came to.
#[allow(dead_code)]
pub struct Ran {
    pub result: capdag::PipelineResult,
    /// What was produced at each output, by the name the notation gives it:
    /// its items and whether it is a sequence — `None` when it was not
    /// produced.
    pub outputs: std::collections::HashMap<String, Option<(Vec<Vec<u8>>, bool)>>,
    /// The plan that ran.
    pub plan: capdag::planner::MachinePlan,
}

#[allow(dead_code)]
impl Ran {
    /// The one value produced at output `name`, as text.
    pub fn text(&self, name: &str) -> String {
        let (items, is_sequence) = self.produced(name);
        assert!(!is_sequence, "output '{name}' is a sequence: {} items", items.len());
        assert_eq!(items.len(), 1, "output '{name}' holds {} items", items.len());
        String::from_utf8(items[0].clone()).expect("output is text")
    }

    /// The sequence produced at output `name`, as text.
    pub fn texts(&self, name: &str) -> Vec<String> {
        let (items, is_sequence) = self.produced(name);
        assert!(is_sequence, "output '{name}' is one value, not a sequence");
        items
            .into_iter()
            .map(|item| String::from_utf8(item).expect("output is text"))
            .collect()
    }

    pub fn produced(&self, name: &str) -> (Vec<Vec<u8>>, bool) {
        self.outputs
            .get(name)
            .unwrap_or_else(|| panic!("the machine has no output '{name}': {:?}", self.outputs.keys()))
            .clone()
            .unwrap_or_else(|| panic!("output '{name}' was not produced"))
    }

    pub fn was_produced(&self, name: &str) -> bool {
        self.outputs
            .get(name)
            .unwrap_or_else(|| panic!("the machine has no output '{name}'"))
            .is_some()
    }
}

/// Runs machines on the test cartridge by the path a machine takes outside
/// the tests: notation → plan → `execute_plan` on a [`capdag::CliRuntime`]
/// hosting the real cartridge process. Nothing is mocked.
#[allow(dead_code)]
pub struct Machines {
    pub registry: std::sync::Arc<capdag::FabricRegistry>,
    tag: String,
    dir: tempfile::TempDir,
    /// Every log record of every run, as `(cap urn, level, message)`.
    pub logs: std::sync::Arc<std::sync::Mutex<Vec<(String, String, String)>>>,
    /// Every overall progress value of every run.
    pub progress: std::sync::Arc<std::sync::Mutex<Vec<f32>>>,
}

#[allow(dead_code)]
impl Machines {
    pub fn new(tag: &str) -> Self {
        Self::with_caps(tag, Vec::new())
    }

    /// With `extra` caps in the registry beside the cartridge's own.
    pub fn with_caps(tag: &str, extra: Vec<capdag::Cap>) -> Self {
        Self {
            registry: testcartridge_registry(tag, extra),
            tag: tag.to_string(),
            dir: tempfile::TempDir::new().expect("Failed to create temp dir"),
            logs: Default::default(),
            progress: Default::default(),
        }
    }

    /// Run the one-strand machine `notation`, giving its inputs — by the
    /// names the notation wrote — what `inputs` says.
    pub async fn run(
        &self,
        notation: &str,
        inputs: &[(&str, Given<'_>)],
    ) -> Result<Ran, capdag::ExecutionError> {
        self.run_with(notation, inputs, &[]).await
    }

    /// [`run`](Self::run), also giving values to args the machine leaves
    /// unbound: `(cap URN of the step, arg media URN, value)`.
    pub async fn run_with(
        &self,
        notation: &str,
        inputs: &[(&str, Given<'_>)],
        arguments: &[(&str, &str, &[u8])],
    ) -> Result<Ran, capdag::ExecutionError> {
        self.run_from(notation, &capdag::NoMachineSource, inputs, arguments).await
    }

    /// [`run`](Self::run) for a machine that includes others: its machine
    /// headers name machines `machines` holds.
    pub async fn run_including(
        &self,
        notation: &str,
        machines: &dyn capdag::MachineSource,
        inputs: &[(&str, Given<'_>)],
    ) -> Result<Ran, capdag::ExecutionError> {
        self.run_from(notation, machines, inputs, &[]).await
    }

    async fn run_from(
        &self,
        notation: &str,
        machines: &dyn capdag::MachineSource,
        inputs: &[(&str, Given<'_>)],
        arguments: &[(&str, &str, &[u8])],
    ) -> Result<Ran, capdag::ExecutionError> {
        use capdag::orchestrator::{execute_plan, CliRuntime, EngineRuntime, PlanInput, ResolvedNotation};
        use std::collections::{HashMap, HashSet};
        use std::sync::Arc;

        let resolved = ResolvedNotation::resolve_from(notation, self.registry.clone(), machines, None)
            .await
            .unwrap_or_else(|e| panic!("the notation does not resolve: {e}\n{notation}"));
        assert_eq!(resolved.strand_count(), 1, "these tests run one strand");
        let sequences: HashSet<String> = inputs
            .iter()
            .filter(|(_, given)| matches!(given, Given::Many(_) | Given::Live { .. }))
            .map(|(name, _)| name.to_string())
            .collect();
        let strand = resolved
            .plan(0, &capdag::orchestrator::RunInputs { sequences, ..Default::default() })
            .await
            .unwrap_or_else(|e| panic!("the machine does not plan: {e}"));

        let mut initial_inputs: HashMap<String, PlanInput> = HashMap::new();
        let mut flags: HashMap<String, bool> = HashMap::new();
        let input_slots = strand.input_slots();
        for (name, given) in inputs {
            let slot = input_slots
                .get(*name)
                .unwrap_or_else(|| panic!("the machine has no input '{name}': {:?}", input_slots.keys()))
                .clone();
            let (input, is_sequence) = match given {
                Given::One(bytes) => (PlanInput::Bytes(bytes.to_vec()), false),
                Given::Many(items) => {
                    let items: Vec<Vec<u8>> = items.iter().map(|item| item.to_vec()).collect();
                    (
                        PlanInput::Bytes(
                            capdag::orchestrator::cbor_util::wrap_raw_items_as_cbor_sequence(&items)
                                .expect("items wrap as a sequence"),
                        ),
                        true,
                    )
                }
                Given::Live { reference_urn, selector } => (
                    PlanInput::LiveReference {
                        reference_urn: reference_urn.to_string(),
                        selector: selector.to_vec(),
                    },
                    true,
                ),
            };
            initial_inputs.insert(slot.clone(), input);
            flags.insert(slot, is_sequence);
        }

        // Argument values are keyed by the step's plan node: the one step
        // that runs the cap named.
        let mut cap_arguments: HashMap<String, Vec<(String, Vec<u8>)>> = HashMap::new();
        for (cap, arg, value) in arguments {
            let wanted = capdag::CapUrn::from_string(cap).expect("a cap URN");
            let steps: Vec<&String> = strand
                .plan
                .nodes
                .iter()
                .filter(|(_, node)| {
                    node.cap_urn().is_some_and(|urn| {
                        capdag::CapUrn::from_string(urn).unwrap().is_equivalent(&wanted)
                    })
                })
                .map(|(id, _)| id)
                .collect();
            assert_eq!(steps.len(), 1, "expected one step running {cap}, found {}", steps.len());
            cap_arguments
                .entry(steps[0].clone())
                .or_default()
                .push((arg.to_string(), value.to_vec()));
        }

        // A run of its own: its own cartridge directory, its own output
        // directory, its own cartridge process.
        let run_dir = self.dir.path().join(format!("run-{}", uuid::Uuid::new_v4()));
        let cartridge_dir = run_dir.join("cartridges");
        let persist_dir = run_dir.join("outputs");
        std::fs::create_dir_all(&cartridge_dir).expect("Failed to create cartridge dir");
        let runtime: Arc<dyn EngineRuntime> = Arc::new(CliRuntime::new(
            cartridge_dir,
            None, // no registry URL — dev binaries only, no network
            capdag::CartridgeChannel::Release,
            capdag::FABRIC_MANIFEST_VERSION,
            vec![testcartridge_bin(&self.tag)],
            None,
            self.registry.clone(),
            None,
            persist_dir,
        ));

        let logs = self.logs.clone();
        let progress = self.progress.clone();
        let log_progress = self.logs.clone();
        let callbacks = capdag::RunCallbacks {
            progress: Some(Arc::new(move |p: f32, cap_urn: &str, message: &str| {
                progress.lock().unwrap().push(p);
                log_progress.lock().unwrap().push((
                    cap_urn.to_string(),
                    "progress".to_string(),
                    message.to_string(),
                ));
            })),
            log: Some(Arc::new(move |record: capdag::PipelineLogRecord| {
                logs.lock().unwrap().push((
                    record.cap_urn.clone().unwrap_or_default(),
                    record.level.clone(),
                    record.message.clone(),
                ));
            })),
            ..Default::default()
        };
        let ledger = Arc::new(
            capdag::RunArgumentLedger::new(&strand.plan, cap_arguments)
                .unwrap_or_else(|e| panic!("the argument values are refused: {e}")),
        );
        let result =
            execute_plan(&strand.plan, runtime, initial_inputs, flags, ledger, callbacks).await?;

        // What the CLI runtime persisted, read back: one file for a single
        // value, one per item for a sequence.
        let mut outputs = HashMap::new();
        let output_names = strand.output_names();
        for terminal in &result.terminals {
            let name = output_names
                .get(terminal.output_node_id())
                .unwrap_or_else(|| panic!("terminal '{}' has no name", terminal.output_node_id()))
                .clone();
            let value = terminal.produced().map(|produced| {
                let mut items: Vec<Vec<u8>> =
                    produced.items.iter().map(|item| item.data.clone()).collect();
                for writer in &produced.writer_results {
                    for path in &writer.saved_paths {
                        items.push(std::fs::read(path).unwrap_or_else(|e| {
                            panic!("a persisted output cannot be read back ({path}): {e}")
                        }));
                    }
                }
                (items, produced.is_sequence)
            });
            outputs.insert(name, value);
        }
        Ok(Ran {
            result,
            outputs,
            plan: strand.plan,
        })
    }
}
