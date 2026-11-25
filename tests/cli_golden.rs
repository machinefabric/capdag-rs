//! The capdag command, recorded: what each command prints and draws in each of
//! its modes, held to a record kept beside this test (`golden/`).
//!
//! Every case runs the real command line in a process of its own, against a
//! fabric registry holding the test cartridge's caps, with the test cartridge
//! itself as the cartridge that runs them (`--dev-bin`): nothing is mocked but
//! where the fabric comes from. A case runs in one of three modes:
//!
//! - **pipes**: stdin, stdout and stderr are pipes, as in a script or CI; the
//!   record holds stdout and stderr byte for byte.
//! - **inline**: stdin and stderr are a terminal and stdout a pipe, as
//!   `capdag run … > out` is typed at one: the live view and the pickers draw
//!   on stderr, and keys are typed at them.
//! - **full screen**: all three are the terminal, as `capdag view` and
//!   `capdag edit` are typed.
//!
//! What a terminal shows is read through a terminal emulator, the way a person
//! reads it: the record holds the screen at each point a case marks, and once
//! the command has ended. The terminal answers what it is asked (where its
//! cursor is, what it is) as a terminal does.
//!
//! What differs between two runs of the same command is taken out of the
//! record and nothing else: the scratch directories' paths, the test
//! cartridge's path, and how long a run took. `CAPDAG_GOLDEN_UPDATE=1` writes
//! the record afresh from what the commands do now.
#![cfg(unix)]

mod support;

use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const TAG: &str = "cli-golden";

/// Set in a case's own process: the command line it runs, as JSON.
const CHILD_ARGV: &str = "CAPDAG_GOLDEN_ARGV";
/// Set in a case's own process: the test cartridge, whose caps the registry
/// holds.
const CHILD_CARTRIDGE: &str = "CAPDAG_GOLDEN_CARTRIDGE";
/// The descriptor a case's process finds its real stdout on: the test
/// harness writes its own lines on descriptor 1 before the case runs, so the
/// command's stdout is handed over apart and put in place by the case.
const STDOUT_FD: i32 = 3;

/// The terminal a case draws on.
const COLUMNS: u16 = 100;
const ROWS: u16 = 32;

// =============================================================================
// The fabric the commands resolve against
// =============================================================================

/// Aliases a person types for the test cartridge's caps: `test-dag-shout` is
/// `shout`.
const CAP_ALIASES: &[(&str, &str)] = &[
    ("say", "test-dag-say"),
    ("words", "test-dag-words"),
    ("shout", "test-dag-shout"),
    ("join", "test-dag-join"),
    ("pair", "test-dag-pair"),
    ("flaky", "test-dag-flaky"),
];

/// A text file, as the fabric defines it: what `.txt` is detected as.
const TEXT_FILE: &str = "media:enc=utf8;ext=txt";

/// The test cartridge's results the fabric defines as files, by the
/// extension each is saved with: what a machine can be planned to make.
const RESULT_FILES: &[(&str, &str)] = &[
    ("media:dag-said;enc=utf8", "said"),
    ("media:dag-word;enc=utf8", "word"),
    ("media:dag-shout;enc=utf8", "shout"),
    ("media:dag-joined;enc=utf8", "joined"),
];

fn media_def(urn: &str, title: &str, extensions: Vec<String>) -> capdag::StoredMediaDef {
    capdag::StoredMediaDef {
        version: 1,
        urn: urn.to_string(),
        media_type: "text/plain".to_string(),
        title: title.to_string(),
        profile_uri: None,
        schema: None,
        description: None,
        documentation: None,
        dims: Vec::new(),
        validation: None,
        metadata: None,
        extensions,
    }
}

/// The registry every case resolves against: the test cartridge's caps as
/// its manifest declares them, a media definition for every type they take
/// and give, a text file type, and an alias for each cap.
fn golden_registry(cartridge: &Path) -> Arc<capdag::FabricRegistry> {
    let output = Command::new(cartridge)
        .arg("manifest")
        .output()
        .expect("the test cartridge prints its manifest");
    assert!(output.status.success(), "`testcartridge manifest` failed");
    let manifest = capdag::CapManifest::from_advertised(&output.stdout)
        .expect("the test cartridge's manifest is valid cap definitions");
    let caps: Vec<capdag::Cap> = manifest.all_caps().into_iter().cloned().collect();
    let registry = capdag::FabricRegistry::new_for_test();
    let mut urns: Vec<String> = Vec::new();
    for cap in &caps {
        urns.push(cap.urn.in_spec().to_string());
        urns.push(cap.urn.out_spec().to_string());
        urns.extend(cap.get_args().iter().map(|arg| arg.media_urn.clone()));
    }
    urns.sort();
    urns.dedup();
    for urn in urns {
        if urn == "media:" || registry.get_cached_media_def(&urn).is_ok() {
            continue;
        }
        let title = urn.trim_start_matches("media:").to_string();
        let extensions = RESULT_FILES
            .iter()
            .filter(|(result, _)| capdag::MediaUrn::from_string(result).unwrap().is_equivalent(&capdag::MediaUrn::from_string(&urn).unwrap()))
            .map(|(_, extension)| extension.to_string())
            .collect();
        registry.insert_cached_media_def_for_test(media_def(&urn, &title, extensions));
        registry.list_media_in_manifest_for_test(&urn, 1);
    }
    registry.insert_cached_media_def_for_test(media_def(TEXT_FILE, "Text file", vec!["txt".to_string()]));
    registry.list_media_in_manifest_for_test(TEXT_FILE, 1);
    registry.insert_cached_alias_for_test(capdag::StoredAlias {
        name: "txt".to_string(),
        target: TEXT_FILE.to_string(),
        version: 1,
    });
    for (alias, tag) in CAP_ALIASES {
        let cap = caps
            .iter()
            .find(|cap| cap.urn.to_string().contains(tag))
            .unwrap_or_else(|| panic!("the test cartridge serves no cap tagged {tag}"));
        registry.insert_cached_alias_for_test(capdag::StoredAlias {
            name: alias.to_string(),
            target: cap.urn.to_string(),
            version: 1,
        });
    }
    registry.add_caps_to_cache(caps);
    Arc::new(registry)
}

// =============================================================================
// A case's own process
// =============================================================================

/// In a case's own process: run the command line it was given and end with
/// its status. Elsewhere: nothing.
fn run_child_if_asked() {
    let Ok(argv) = std::env::var(CHILD_ARGV) else {
        return;
    };
    let argv: Vec<String> = serde_json::from_str(&argv).expect("the case's command line is JSON");
    let cartridge = PathBuf::from(std::env::var(CHILD_CARTRIDGE).expect("the case names the test cartridge"));
    // SAFETY: descriptor 3 was opened for this process by the case that
    // started it; dup2 replaces descriptor 1 with it.
    unsafe {
        assert_eq!(libc::dup2(STDOUT_FD, libc::STDOUT_FILENO), libc::STDOUT_FILENO);
        libc::close(STDOUT_FD);
    }
    let code = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("a runtime for the command")
        .block_on(async move {
            let registry = golden_registry(&cartridge);
            capdag::cli::run_command_line(argv, Some(registry)).await
        });
    let _ = std::io::stdout().flush();
    let _ = std::io::stderr().flush();
    std::process::exit(code);
}

// =============================================================================
// Cases
// =============================================================================

/// Where a case's streams go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Pipes,
    Inline,
    FullScreen,
}

/// What a case does at a terminal, in order.
#[derive(Debug, Clone, Copy)]
enum Step {
    /// Wait until the screen shows this text.
    Wait(&'static str),
    /// Type these bytes.
    Press(&'static str),
    /// Record the screen as it stands, under this name.
    Look(&'static str),
}

struct Case {
    /// The test's name: its record is `golden/<name>.txt`.
    name: &'static str,
    mode: Mode,
    /// The command line after `capdag`. `{cartridge}` is the test
    /// cartridge's path.
    args: &'static [&'static str],
    /// Bytes piped on stdin (pipes only).
    stdin: Option<&'static str>,
    steps: &'static [Step],
}

impl Case {
    const fn pipes(name: &'static str, args: &'static [&'static str]) -> Self {
        Case { name, mode: Mode::Pipes, args, stdin: None, steps: &[] }
    }
}

/// The files every case starts with, in its working directory.
const FILES: &[(&str, &str)] = &[
    ("in.txt", "hello brave world\n"),
    ("other.txt", "quiet small things\n"),
    ("loud.machine", "[words cap:in=\"media:enc=utf8\";test-dag-words;out=\"media:dag-word;enc=utf8\"]\n[shout cap:in=\"media:enc=utf8\";test-dag-shout;out=\"media:dag-shout;enc=utf8\"]\n[doc -> words -> w]\n[w -> shout -> loud]\n"),
    ("say.machine", "[say cap:in=\"media:void\";test-dag-say;out=\"media:dag-said;enc=utf8\"]\n[words cap:in=\"media:enc=utf8\";test-dag-words;out=\"media:dag-word;enc=utf8\"]\n[(text=\"hello brave world\") -> say -> s]\n[s -> words -> w]\n"),
    ("refuses.machine", "[words cap:in=\"media:enc=utf8\";test-dag-words;out=\"media:dag-word;enc=utf8\"]\n[shout cap:in=\"media:enc=utf8\";test-dag-shout;out=\"media:dag-shout;enc=utf8\"]\n[doc -> words -> w]\n[(w, fail-on=\"brave\") -> shout -> loud]\n"),
    ("brave.txt", "brave"),
    ("refuses-one.machine", "[shout cap:in=\"media:enc=utf8\";test-dag-shout;out=\"media:dag-shout;enc=utf8\"]\n[(doc, fail-on=\"brave\") -> shout -> loud]\n"),
    ("tolerates.machine", "[words cap:in=\"media:enc=utf8\";test-dag-words;out=\"media:dag-word;enc=utf8\"]\n[shout cap:in=\"media:enc=utf8\";test-dag-shout;out=\"media:dag-shout;enc=utf8\"]\n[doc -> words -> w]\n[(w, fail-on=\"brave\") -> shout{on_error=absent} -> loud]\n"),
    ("upper.machine", "[shout cap:in=\"media:enc=utf8\";test-dag-shout;out=\"media:dag-shout;enc=utf8\"]\n[doc -> shout -> loud]\n"),
    ("tolerates-one.machine", "[shout cap:in=\"media:enc=utf8\";test-dag-shout;out=\"media:dag-shout;enc=utf8\"]\n[(doc, fail-on=\"brave\") -> shout{on_error=absent} -> loud]\n"),
    ("slow.machine", "[shout cap:in=\"media:enc=utf8\";test-dag-shout;out=\"media:dag-shout;enc=utf8\"]\n[(doc, delay-ms=4000) -> shout -> loud]\n"),
    ("broken.machine", "[words cap:in=\"media:enc=utf8\";test-dag-words;out=\"media:dag-word;enc=utf8\"]\n[doc -> words -> w\n"),
    ("unknown.machine", "[doc -> nosuch -> w]\n"),
];

const DEV: &str = "--dev-bin";
const CARTRIDGE: &str = "{cartridge}";

/// The keys a terminal sends.
const ENTER: &str = "\r";
const DOWN: &str = "\x1b[B";
const RIGHT: &str = "\x1b[C";
const CTRL_C: &str = "\x03";
const CTRL_S: &str = "\x13";

fn case(name: &str) -> &'static Case {
    CASES
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("no case is named {name}"))
}

static CASES: &[Case] = &[
    // ── The command tree ──
    Case::pipes("bare", &[]),
    Case::pipes("help", &["--help"]),
    Case::pipes("help_run", &["help", "run"]),
    Case::pipes("help_unknown", &["help", "rnu"]),
    Case::pipes("run_help_flag", &["run", "--help"]),
    Case::pipes("unknown_command", &["cpas"]),
    Case::pipes("cap_as_command", &["shout", "in.txt"]),
    Case::pipes("machine_as_command", &["loud.machine"]),
    Case::pipes("unknown_word", &["zzzqqq"]),
    Case::pipes("unknown_flag", &["caps", "--nope"]),
    Case::pipes("fabric_twice", &["--fabric", "https://a.example", "--fabric", "https://b.example", "caps"]),
    Case::pipes("fabric_not_a_url", &["--fabric", "ftp://a.example", "caps"]),
    // ── caps ──
    Case::pipes("caps", &["caps"]),
    Case::pipes("caps_json", &["--json", "caps"]),
    Case::pipes("caps_search", &["caps", "shout"]),
    Case::pipes("caps_search_verbose", &["-v", "caps", "words"]),
    Case::pipes("caps_none", &["caps", "nothingmatches"]),
    Case::pipes("caps_pick_no_input", &["--no-input", "caps", "--pick", "dag"]),
    Case {
        name: "caps_pick_inline",
        mode: Mode::Inline,
        args: &["caps", "--pick", "dag"],
        stdin: None,
        steps: &[Step::Wait("shout"), Step::Look("the picker"), Step::Press(DOWN), Step::Press(ENTER)],
    },
    // ── info ──
    Case::pipes("info_cap", &["info", "shout"]),
    Case::pipes("info_cap_json", &["--json", "info", "shout"]),
    Case::pipes("info_media", &["info", "txt"]),
    Case::pipes("info_unknown", &["info", "nosuch"]),
    // ── check, graph, view, edit ──
    Case::pipes("check", &["check", "loud.machine"]),
    Case::pipes("check_json", &["--json", "check", "loud.machine"]),
    Case::pipes("check_broken", &["check", "broken.machine"]),
    Case::pipes("check_unknown_cap", &["check", "unknown.machine"]),
    Case::pipes("check_missing", &["check", "absent.machine"]),
    Case::pipes("graph", &["graph", "loud.machine"]),
    Case::pipes("graph_width", &["graph", "say.machine", "--width", "60"]),
    Case::pipes("graph_mermaid", &["graph", "loud.machine", "--format", "mermaid"]),
    Case::pipes("graph_dot", &["graph", "loud.machine", "--format", "dot"]),
    Case::pipes("graph_dot_verbose", &["-v", "graph", "loud.machine", "--format", "dot"]),
    Case {
        name: "graph_terminal",
        mode: Mode::FullScreen,
        args: &["graph", "say.machine"],
        stdin: None,
        steps: &[],
    },
    Case::pipes("view_without_terminal", &["view", "loud.machine"]),
    Case::pipes("edit_without_terminal", &["edit", "loud.machine"]),
    Case {
        name: "view_full_screen",
        mode: Mode::FullScreen,
        args: &["view", "loud.machine"],
        stdin: None,
        steps: &[
            Step::Wait("resolves"),
            Step::Look("the machine"),
            Step::Press(RIGHT),
            Step::Look("a node focused"),
            Step::Press(RIGHT),
            Step::Look("the next one focused"),
            Step::Press("q"),
        ],
    },
    Case {
        name: "edit_full_screen",
        mode: Mode::FullScreen,
        args: &["edit", "say.machine"],
        stdin: None,
        steps: &[
            Step::Wait("resolves · 2 steps"),
            Step::Look("the machine"),
            Step::Press("u"),
            Step::Press(CTRL_S),
            Step::Press("q"),
        ],
    },
    // ── run ──
    // A run's progress lines and --json records are recorded of machines of
    // one step. A map's items run side by side on the cartridge, and the
    // order its signals arrive in — an item's outcome, the map's own
    // progress — is the order they happen to end in, so of a map run only
    // what does not depend on it is recorded: its results, its summary and
    // its errors (`--progress none`, `-q`).
    Case::pipes("run_machine", &["run", "upper.machine", "in.txt", DEV, CARTRIDGE]),
    Case::pipes("run_machine_json", &["--json", "run", "upper.machine", "in.txt", DEV, CARTRIDGE]),
    Case::pipes("run_machine_quiet", &["-q", "run", "loud.machine", "in.txt", DEV, CARTRIDGE]),
    Case::pipes("run_machine_two_files", &["run", "loud.machine", "in.txt", "other.txt", "--progress", "none", DEV, CARTRIDGE]),
    Case::pipes("run_machine_progress_plain", &["run", "upper.machine", "in.txt", "--progress", "plain", DEV, CARTRIDGE]),
    Case::pipes("run_machine_refused", &["run", "refuses-one.machine", "brave.txt", DEV, CARTRIDGE]),
    Case::pipes("run_machine_refused_json", &["--json", "run", "refuses-one.machine", "brave.txt", DEV, CARTRIDGE]),
    Case::pipes("run_machine_tolerated", &["run", "tolerates-one.machine", "brave.txt", DEV, CARTRIDGE]),
    Case::pipes("run_machine_map", &["run", "loud.machine", "in.txt", "--progress", "none", DEV, CARTRIDGE]),
    Case::pipes("run_machine_map_refused", &["run", "refuses.machine", "in.txt", "--progress", "none", DEV, CARTRIDGE]),
    Case::pipes("run_machine_map_tolerated", &["run", "tolerates.machine", "in.txt", "--progress", "none", DEV, CARTRIDGE]),
    Case::pipes("run_machine_constant", &["run", "say.machine", DEV, CARTRIDGE]),
    Case::pipes("run_machine_missing_input", &["run", "loud.machine", DEV, CARTRIDGE]),
    Case::pipes("run_machine_bad_option", &["run", "loud.machine", "in.txt", "--shout", DEV, CARTRIDGE]),
    Case::pipes("run_cap", &["run", "shout", "in.txt", DEV, CARTRIDGE]),
    Case::pipes("run_cap_option", &["run", "shout", "in.txt", "--fail-on", "hello brave world\n", DEV, CARTRIDGE]),
    Case::pipes("run_cap_help", &["run", "shout", "--help"]),
    Case::pipes("run_cap_unknown_option", &["run", "shout", "in.txt", "--loudness", "9", DEV, CARTRIDGE]),
    Case::pipes("run_unknown_target", &["run", "shuot", "in.txt"]),
    Case::pipes("run_missing_machine_file", &["run", "absent.machine", "in.txt"]),
    Case {
        name: "run_cap_stdin",
        mode: Mode::Pipes,
        args: &["run", "shout", DEV, CARTRIDGE],
        stdin: Some("from a pipe"),
        steps: &[],
    },
    Case {
        name: "run_machine_inline",
        mode: Mode::Inline,
        args: &["run", "loud.machine", "in.txt", DEV, CARTRIDGE],
        stdin: None,
        steps: &[],
    },
    Case {
        name: "run_machine_inline_refused",
        mode: Mode::Inline,
        args: &["run", "refuses.machine", "in.txt", DEV, CARTRIDGE],
        stdin: None,
        steps: &[],
    },
    Case {
        name: "run_stopped_inline",
        mode: Mode::Inline,
        args: &["run", "slow.machine", "in.txt", "other.txt", DEV, CARTRIDGE],
        stdin: None,
        steps: &[Step::Wait("shout"), Step::Press(CTRL_C)],
    },
    Case {
        name: "run_aborted_inline",
        mode: Mode::Inline,
        args: &["run", "slow.machine", "in.txt", DEV, CARTRIDGE],
        stdin: None,
        steps: &[Step::Wait("shout"), Step::Press(CTRL_C), Step::Wait("Stopping"), Step::Press(CTRL_C)],
    },
    // ── plan ──
    Case::pipes("plan_targets", &["plan", "in.txt"]),
    Case::pipes("plan_targets_json", &["--json", "plan", "in.txt"]),
    Case::pipes("plan_machines", &["plan", "in.txt", "--to", "media:dag-shout;enc=utf8"]),
    Case::pipes("plan_pick_print", &["plan", "in.txt", "--to", "media:dag-shout;enc=utf8", "--pick", "1"]),
    Case::pipes("plan_pick_save", &["plan", "in.txt", "--to", "media:dag-shout;enc=utf8", "--pick", "1", "--save", "made.machine"]),
    Case::pipes("plan_pick_out_of_range", &["plan", "in.txt", "--to", "media:dag-shout;enc=utf8", "--pick", "99"]),
    Case::pipes("plan_pick_json", &["--json", "plan", "in.txt", "--to", "media:dag-shout;enc=utf8", "--pick", "1"]),
    Case::pipes("plan_run", &["plan", "in.txt", "--to", "media:dag-shout;enc=utf8", "--pick", "1", "--run", DEV, CARTRIDGE]),
    Case::pipes("plan_unknown_extension", &["plan", "in.unknown"]),
    Case {
        name: "plan_inline",
        mode: Mode::Inline,
        args: &["plan", "in.txt"],
        stdin: None,
        steps: &[
            Step::Wait("what to make"),
            Step::Look("what to make"),
            Step::Press(ENTER),
            Step::Wait("Choose the machine"),
            Step::Look("the machine"),
            Step::Press(ENTER),
            Step::Wait("what to do with it"),
            Step::Look("what to do with it"),
            Step::Press(DOWN),
            Step::Press(DOWN),
            Step::Press(ENTER),
        ],
    },
    // ── cartridges, models, the cache, development, the command itself ──
    Case::pipes("cartridge_list", &["cartridge", "list"]),
    Case::pipes("cartridge_list_json", &["--json", "cartridge", "list"]),
    Case::pipes("cartridge_install_offline", &["--offline", "cartridge", "install", "nosuch"]),
    Case::pipes("model_list", &["--offline", "model", "list"]),
    Case::pipes("cache_clear", &["cache", "clear"]),
    Case::pipes("cache_clear_json", &["--json", "cache", "clear"]),
    Case::pipes("cache_refresh_offline", &["--offline", "cache", "refresh"]),
    Case::pipes("dev_new", &["dev", "new", "made-cartridge", "--lang", "python"]),
    Case::pipes("self_update_offline", &["--offline", "self", "update"]),
];

// =============================================================================
// Running a case
// =============================================================================

/// What a case did, as its record holds it.
fn record(case: &Case, test: &str) -> String {
    let cartridge = support::testcartridge_bin(TAG);
    let scratch = tempfile::TempDir::new().expect("a scratch directory");
    // Paths are said as the command sees them: through any link in the
    // scratch directory's own path.
    let root = std::fs::canonicalize(scratch.path()).expect("the scratch directory's real path");
    let work = root.join("work");
    let home = root.join("home");
    std::fs::create_dir_all(&work).unwrap();
    std::fs::create_dir_all(&home).unwrap();
    for (name, content) in FILES {
        std::fs::write(work.join(name), content).unwrap();
    }
    let cartridge_path = cartridge.display().to_string();
    let args: Vec<String> = case
        .args
        .iter()
        .map(|arg| arg.replace(CARTRIDGE, &cartridge_path))
        .collect();
    let mut argv = vec!["capdag".to_string()];
    argv.extend(args.iter().cloned());

    let mut command = Command::new(std::env::current_exe().expect("this test's own executable"));
    command
        .args([test, "--exact", "--nocapture", "--test-threads", "1"])
        .current_dir(&work)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", &home)
        .env("TMPDIR", &root)
        .env("LANG", "C.UTF-8")
        .env("LC_ALL", "C.UTF-8")
        // A fabric of its own name, whatever fabric the build is bound to: the
        // commands resolve against the registry the case gives them, and what
        // they say of the fabric names this one.
        .env("CDG_FABRIC_REGISTRY_URL", "https://fabric.golden.invalid")
        // Nothing is fetched: every request goes to a port nothing listens on.
        .env("HTTPS_PROXY", "http://127.0.0.1:9")
        .env("HTTP_PROXY", "http://127.0.0.1:9")
        .env(CHILD_ARGV, serde_json::to_string(&argv).unwrap())
        .env(CHILD_CARTRIDGE, &cartridge);
    if case.mode != Mode::Pipes {
        command.env("TERM", "xterm-256color");
    }
    let mut out = format!("$ {}\n", shown(&argv));
    let (code, stdout, stderr, looks) = match case.mode {
        Mode::Pipes => run_on_pipes(command, case.stdin),
        Mode::Inline | Mode::FullScreen => run_on_terminal(command, case),
    };
    out.push_str(&format!("exit: {}\n", code.map_or("signal".to_string(), |c| c.to_string())));
    out.push_str("--- stdout ---\n");
    out.push_str(&stdout);
    if let Some(stderr) = stderr {
        out.push_str("--- stderr ---\n");
        out.push_str(&stderr);
    }
    for (name, screen) in looks {
        out.push_str(&format!("--- screen: {name} ---\n"));
        out.push_str(&screen);
        if !screen.ends_with('\n') {
            out.push('\n');
        }
    }
    normalised(&out, &root, &cartridge)
}

/// A command line as it is typed.
fn shown(argv: &[String]) -> String {
    argv.iter()
        .map(|word| {
            if word.is_empty() || word.chars().any(|c| c.is_whitespace() || "'\"$`\\|&;<>()*?".contains(c)) {
                format!("'{}'", word.replace('\'', "'\\''").replace('\n', "\\n"))
            } else {
                word.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// The case's record with what differs between runs named instead: the
/// scratch directory, the cartridge's path, and times.
fn normalised(text: &str, root: &Path, cartridge: &Path) -> String {
    let text = text
        .replace(&cartridge.display().to_string(), "<cartridge>")
        .replace(&root.join("work").display().to_string(), "<work>")
        .replace(&root.join("home").display().to_string(), "<home>")
        .replace(&root.display().to_string(), "<tmp>");
    let times = regex::Regex::new(r"\b\d+(\.\d+)? ?(s|ms)\b").unwrap();
    let text = times.replace_all(&text, "<time>");
    let seconds = regex::Regex::new(r#""seconds":\d+(\.\d+)?"#).unwrap();
    let text = seconds.replace_all(&text, r#""seconds":"<time>""#);
    // A step's token is made afresh for each run.
    let tokens = regex::Regex::new(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b").unwrap();
    let text = tokens.replace_all(&text, "<token>");
    // A fetch is made with the time as a cache-buster.
    let busters = regex::Regex::new(r"\bcb=\d+").unwrap();
    let text = busters.replace_all(&text, "cb=<time>");
    let text = canonical_diagrams(&text);
    // A screen's lines end where its text does.
    text.lines().map(str::trim_end).collect::<Vec<_>>().join("\n") + "\n"
}

/// Held while a case's descriptors are made and its process started: cases
/// run side by side, and a process started between the moment a terminal is
/// opened and the moment it is marked close-on-exec would hold that terminal
/// open, so its case would never see it end.
static STARTING: Mutex<()> = Mutex::new(());

/// Diagram source (`graph --format dot|mermaid`) names its nodes
/// `p<plan>_n<k>`, numbered in the order of the plan's node ids, and a step's
/// id is a token made afresh for each run: each node is named by its label
/// instead, and each run of lines naming nodes is put in order.
fn canonical_diagrams(text: &str) -> String {
    let id = regex::Regex::new(r"p\d+_n\d+").unwrap();
    let declared = regex::Regex::new(r#"(p\d+_n\d+)"?\s*(?:\[label=|[\[\(\{]+)"([^"]*)""#).unwrap();
    let labels: std::collections::HashMap<String, String> = declared
        .captures_iter(text)
        .map(|found| (found[1].to_string(), found[2].to_string()))
        .collect();
    if labels.is_empty() {
        return text.to_string();
    }
    let mut lines: Vec<String> = Vec::new();
    let mut naming: Vec<String> = Vec::new();
    for line in text.lines() {
        if id.is_match(line) {
            let named = id.replace_all(line, |found: &regex::Captures| match labels.get(&found[0]) {
                Some(label) => format!("«{label}»"),
                None => found[0].to_string(),
            });
            naming.push(named.into_owned());
        } else {
            naming.sort();
            lines.append(&mut naming);
            lines.push(line.to_string());
        }
    }
    naming.sort();
    lines.append(&mut naming);
    lines.join("\n") + "\n"
}

/// A pipe, closed in every process started from here: its read end and its
/// write end.
fn pipe() -> (OwnedFd, OwnedFd) {
    let (read, write) = std::io::pipe().expect("a pipe");
    (OwnedFd::from(read), OwnedFd::from(write))
}

/// Give the case's process `stdout` as descriptor [`STDOUT_FD`], and
/// nothing on descriptor 1 but where the test harness's own lines are
/// dropped.
fn hand_stdout(command: &mut Command, stdout: &OwnedFd) {
    let fd = stdout.as_raw_fd();
    command.stdout(Stdio::null());
    // SAFETY: only async-signal-safe calls, between fork and exec.
    unsafe {
        command.pre_exec(move || {
            if fd == STDOUT_FD {
                // Already in place: only keep it open across exec.
                if libc::fcntl(fd, libc::F_SETFD, 0) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
            } else if libc::dup2(fd, STDOUT_FD) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

fn read_all(fd: OwnedFd) -> std::thread::JoinHandle<Vec<u8>> {
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = std::fs::File::from(fd).read_to_end(&mut bytes);
        bytes
    })
}

type Ran = (Option<i32>, String, Option<String>, Vec<(String, String)>);

fn run_on_pipes(mut command: Command, stdin: Option<&str>) -> Ran {
    let (stdout_read, stdout_write) = pipe();
    hand_stdout(&mut command, &stdout_write);
    command
        .stdin(if stdin.is_some() { Stdio::piped() } else { Stdio::null() })
        .stderr(Stdio::piped());
    let mut child = {
        let _starting = STARTING.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        command.spawn().expect("start the case")
    };
    drop(command);
    drop(stdout_write);
    let stdout = read_all(stdout_read);
    if let Some(text) = stdin {
        let mut pipe = child.stdin.take().expect("stdin is a pipe");
        pipe.write_all(text.as_bytes()).expect("write the case's stdin");
    }
    let mut stderr = Vec::new();
    child.stderr.take().expect("stderr is a pipe").read_to_end(&mut stderr).unwrap();
    let status = wait(&mut child, Duration::from_secs(120));
    let stdout = stdout.join().expect("stdout was read");
    (
        status,
        String::from_utf8_lossy(&stdout).into_owned(),
        Some(String::from_utf8_lossy(&stderr).into_owned()),
        Vec::new(),
    )
}

fn wait(child: &mut std::process::Child, timeout: Duration) -> Option<i32> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("ask whether the case runs") {
            return status.code();
        }
        if Instant::now() >= deadline {
            // Ended by its own handle: the process this test started.
            let _ = child.kill();
            let _ = child.wait();
            panic!("the case did not end within {timeout:?}");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A new pseudo-terminal, closed in every process started from here: its far
/// end and its near end. Opened while [`STARTING`] is held.
fn open_pty() -> (OwnedFd, OwnedFd) {
    let mut far: libc::c_int = -1;
    let mut near: libc::c_int = -1;
    let mut size = libc::winsize { ws_row: ROWS, ws_col: COLUMNS, ws_xpixel: 0, ws_ypixel: 0 };
    // SAFETY: openpty writes the two descriptors it opens; the size is read.
    let opened = unsafe { libc::openpty(&mut far, &mut near, std::ptr::null_mut(), std::ptr::null_mut(), &mut size) };
    assert_eq!(opened, 0, "openpty: {}", std::io::Error::last_os_error());
    for fd in [far, near] {
        // SAFETY: fd was just opened here; F_SETFD sets its descriptor flags.
        assert_ne!(unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) }, -1, "close-on-exec");
    }
    // SAFETY: both were just opened and are owned here.
    unsafe { (OwnedFd::from_raw_fd(far), OwnedFd::from_raw_fd(near)) }
}

/// The terminal a case draws on, as a person sees it.
struct Screen {
    parser: vt100::Parser,
    /// When something was last drawn.
    drawn_at: Instant,
}

/// Read what is drawn on the terminal into the screen, answering what a
/// terminal is asked: where its cursor is, and what it is (a terminal that
/// speaks the legacy keyboard protocol).
fn read_terminal(far: std::fs::File, mut answers: std::fs::File, screen: Arc<Mutex<Screen>>) {
    let mut far = far;
    let mut buffer = [0u8; 8192];
    let mut pending: Vec<u8> = Vec::new();
    loop {
        let read = match far.read(&mut buffer) {
            Ok(0) | Err(_) => return,
            Ok(read) => read,
        };
        pending.extend_from_slice(&buffer[..read]);
        let mut screen = screen.lock().unwrap();
        // Each question is answered once the bytes before it are drawn, so the
        // cursor it is told is the one those bytes left.
        loop {
            let asked = [b"\x1b[6n".as_slice(), b"\x1b[c".as_slice()]
                .iter()
                .filter_map(|question| find(&pending, question).map(|at| (at, *question)))
                .min_by_key(|(at, _)| *at);
            let Some((at, question)) = asked else { break };
            screen.parser.process(&pending[..at + question.len()]);
            pending.drain(..at + question.len());
            let answer = if question == b"\x1b[6n" {
                let (row, column) = screen.parser.screen().cursor_position();
                format!("\x1b[{};{}R", row + 1, column + 1).into_bytes()
            } else {
                b"\x1b[?1;2c".to_vec()
            };
            if answers.write_all(&answer).is_err() {
                return;
            }
        }
        // A question can arrive split: keep a tail that could begin one.
        let keep = pending.len().min(3);
        let drawable = pending.len() - keep;
        screen.parser.process(&pending[..drawable]);
        pending.drain(..drawable);
        if pending.first() != Some(&0x1b) {
            screen.parser.process(&pending);
            pending.clear();
        }
        screen.drawn_at = Instant::now();
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|window| window == needle)
}

fn contents(screen: &Arc<Mutex<Screen>>) -> String {
    screen.lock().unwrap().parser.screen().contents()
}

/// The screen once nothing has been drawn on it for a while: a frame whole.
fn settled(screen: &Arc<Mutex<Screen>>) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let quiet = screen.lock().unwrap().drawn_at.elapsed();
        if quiet >= Duration::from_millis(400) || Instant::now() >= deadline {
            return contents(screen);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn run_on_terminal(mut command: Command, case: &Case) -> Ran {
    let starting = STARTING.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let (far, near) = open_pty();
    let terminal = |what: &str| -> OwnedFd {
        near.try_clone().unwrap_or_else(|e| panic!("the terminal as {what}: {e}"))
    };
    let (stdout_read, stdout_write) = pipe();
    let stdout_fd = match case.mode {
        Mode::FullScreen => {
            drop(stdout_write);
            terminal("stdout")
        }
        _ => stdout_write,
    };
    hand_stdout(&mut command, &stdout_fd);
    command.stdin(Stdio::from(terminal("stdin"))).stderr(Stdio::from(terminal("stderr")));
    // SAFETY: only async-signal-safe calls, between fork and exec.
    unsafe {
        command.pre_exec(|| {
            // A session of its own, with the terminal as its controlling
            // terminal: what a full-screen surface reads its keys from.
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::ioctl(libc::STDIN_FILENO, libc::TIOCSCTTY as _, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn().expect("start the case on a terminal");
    drop(command);
    drop(near);
    drop(stdout_fd);
    drop(starting);
    let stdout = read_all(stdout_read);
    let far = std::fs::File::from(far);
    let mut keys = far.try_clone().unwrap();
    let answers = far.try_clone().unwrap();
    let screen = Arc::new(Mutex::new(Screen { parser: vt100::Parser::new(ROWS, COLUMNS, 0), drawn_at: Instant::now() }));
    let reader = {
        let screen = screen.clone();
        std::thread::spawn(move || read_terminal(far, answers, screen))
    };
    let mut looks: Vec<(String, String)> = Vec::new();
    for step in case.steps {
        match step {
            Step::Wait(text) => {
                let deadline = Instant::now() + Duration::from_secs(60);
                while !contents(&screen).contains(text) {
                    if let Some(status) = child.try_wait().unwrap() {
                        panic!("{}: the command ended ({status}) before it showed '{text}'\n{}", case.name, contents(&screen));
                    }
                    assert!(Instant::now() < deadline, "{}: '{text}' was never shown\n{}", case.name, contents(&screen));
                    std::thread::sleep(Duration::from_millis(30));
                }
            }
            Step::Press(bytes) => {
                keys.write_all(bytes.as_bytes()).unwrap();
                keys.flush().unwrap();
                std::thread::sleep(Duration::from_millis(150));
            }
            Step::Look(name) => looks.push((name.to_string(), settled(&screen))),
        }
    }
    let code = wait(&mut child, Duration::from_secs(120));
    let grace = Instant::now() + Duration::from_secs(5);
    while !reader.is_finished() && Instant::now() < grace {
        std::thread::sleep(Duration::from_millis(20));
    }
    looks.push(("at the end".to_string(), contents(&screen)));
    let stdout = stdout.join().expect("stdout was read");
    (code, String::from_utf8_lossy(&stdout).into_owned(), None, looks)
}

/// The test `test` of case `name`: in the case's own process, the case's
/// command; otherwise the case held to its record, or the record written
/// afresh.
fn golden(test: &str, name: &str) {
    run_child_if_asked();
    let case = case(name);
    let got = record(case, test);
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden").join(format!("{name}.txt"));
    if std::env::var_os("CAPDAG_GOLDEN_UPDATE").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &got).unwrap();
        return;
    }
    let want = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{name} has no record at {}: {e}; write one with CAPDAG_GOLDEN_UPDATE=1", path.display()));
    if got != want {
        let first = got
            .lines()
            .zip(want.lines())
            .position(|(g, w)| g != w)
            .unwrap_or_else(|| got.lines().count().min(want.lines().count()));
        panic!(
            "{name} differs from its record {} from line {}\n--- recorded ---\n{want}\n--- now ---\n{got}",
            path.display(),
            first + 1
        );
    }
}

// TEST22200: every case has exactly one test, which names itself and its
// case, and every test names a case.
#[test]
fn test22200_every_case_has_one_test() {
    run_child_if_asked();
    let source = include_str!("cli_golden.rs");
    let call = regex::Regex::new(r#"fn (test\d+_\w+)\(\) \{\s*golden\("(test\d+_\w+)", "(\w+)"\)"#).unwrap();
    let mut tested: Vec<&str> = Vec::new();
    for found in call.captures_iter(source) {
        assert_eq!(&found[1], &found[2], "{} names itself as {}", &found[1], &found[2]);
        let name = found.get(3).unwrap().as_str();
        assert!(CASES.iter().any(|case| case.name == name), "{} names no case: {name}", &found[1]);
        assert!(!tested.contains(&name), "case {name} has two tests");
        tested.push(name);
    }
    for case in CASES {
        assert!(tested.contains(&case.name), "case {} has no test", case.name);
    }
}

// TEST22201: `capdag` alone shows the overview of the commands and exits 2.
#[test]
fn test22201_bare() { golden("test22201_bare", "bare") }

// TEST22202: `--help` shows every command, the global flags and where to start.
#[test]
fn test22202_help() { golden("test22202_help", "help") }

// TEST22203: `help run` shows run's long help with its examples.
#[test]
fn test22203_help_run() { golden("test22203_help_run", "help_run") }

// TEST22204: `help` of a command that does not exist names the one meant.
#[test]
fn test22204_help_unknown() { golden("test22204_help_unknown", "help_unknown") }

// TEST22205: `run --help` shows run's own help.
#[test]
fn test22205_run_help_flag() { golden("test22205_run_help_flag", "run_help_flag") }

// TEST22206: a mistyped command is answered with the command it meant.
#[test]
fn test22206_unknown_command() { golden("test22206_unknown_command", "unknown_command") }

// TEST22207: a capability typed as a command says how to run it.
#[test]
fn test22207_cap_as_command() { golden("test22207_cap_as_command", "cap_as_command") }

// TEST22208: a machine file typed as a command says how to run it.
#[test]
fn test22208_machine_as_command() { golden("test22208_machine_as_command", "machine_as_command") }

// TEST22209: a word that is neither a command nor a capability lists the commands.
#[test]
fn test22209_unknown_word() { golden("test22209_unknown_word", "unknown_word") }

// TEST22210: an unknown flag is clap's usage error, exit 2.
#[test]
fn test22210_unknown_flag() { golden("test22210_unknown_flag", "unknown_flag") }

// TEST22211: two different --fabric values are refused.
#[test]
fn test22211_fabric_twice() { golden("test22211_fabric_twice", "fabric_twice") }

// TEST22212: a --fabric that is no http(s) URL is refused.
#[test]
fn test22212_fabric_not_a_url() { golden("test22212_fabric_not_a_url", "fabric_not_a_url") }

// TEST22213: `caps` lists every capability by its alias.
#[test]
fn test22213_caps() { golden("test22213_caps", "caps") }

// TEST22214: `caps --json` gives one record per capability.
#[test]
fn test22214_caps_json() { golden("test22214_caps_json", "caps_json") }

// TEST22215: `caps <word>` lists the capabilities that mention it.
#[test]
fn test22215_caps_search() { golden("test22215_caps_search", "caps_search") }

// TEST22216: `-v caps <word>` shows each capability's URN as well.
#[test]
fn test22216_caps_search_verbose() { golden("test22216_caps_search_verbose", "caps_search_verbose") }

// TEST22217: `caps` of a word nothing mentions says so.
#[test]
fn test22217_caps_none() { golden("test22217_caps_none", "caps_none") }

// TEST22218: `caps --pick` with --no-input and several matches names the flag to choose with.
#[test]
fn test22218_caps_pick_no_input() { golden("test22218_caps_pick_no_input", "caps_pick_no_input") }

// TEST22219: `caps --pick` on a terminal opens the picker inline and prints the alias chosen.
#[test]
fn test22219_caps_pick_inline() { golden("test22219_caps_pick_inline", "caps_pick_inline") }

// TEST22220: `info <cap>` shows what the capability takes, gives and needs.
#[test]
fn test22220_info_cap() { golden("test22220_info_cap", "info_cap") }

// TEST22221: `info <cap> --json` gives the capability's definition.
#[test]
fn test22221_info_cap_json() { golden("test22221_info_cap_json", "info_cap_json") }

// TEST22222: `info <media>` shows the media type and the capabilities that take it.
#[test]
fn test22222_info_media() { golden("test22222_info_media", "info_media") }

// TEST22223: `info` of a name nothing has says so.
#[test]
fn test22223_info_unknown() { golden("test22223_info_unknown", "info_unknown") }

// TEST22224: `check` of a machine names its inputs, steps and outputs.
#[test]
fn test22224_check() { golden("test22224_check", "check") }

// TEST22225: `check --json` gives the machine's record.
#[test]
fn test22225_check_json() { golden("test22225_check_json", "check_json") }

// TEST22226: `check` of a machine that does not parse says where.
#[test]
fn test22226_check_broken() { golden("test22226_check_broken", "check_broken") }

// TEST22227: `check` of a machine naming a cap no header declares says which.
#[test]
fn test22227_check_unknown_cap() { golden("test22227_check_unknown_cap", "check_unknown_cap") }

// TEST22228: `check` of a file that does not exist says so.
#[test]
fn test22228_check_missing() { golden("test22228_check_missing", "check_missing") }

// TEST22229: `graph` prints the machine's picture as text.
#[test]
fn test22229_graph() { golden("test22229_graph", "graph") }

// TEST22230: `graph --width` fits the picture into that many columns.
#[test]
fn test22230_graph_width() { golden("test22230_graph_width", "graph_width") }

// TEST22231: `graph --format mermaid` prints Mermaid source.
#[test]
fn test22231_graph_mermaid() { golden("test22231_graph_mermaid", "graph_mermaid") }

// TEST22232: `graph --format dot` prints Graphviz source by alias.
#[test]
fn test22232_graph_dot() { golden("test22232_graph_dot", "graph_dot") }

// TEST22233: `-v graph --format dot` names caps and media by URN.
#[test]
fn test22233_graph_dot_verbose() { golden("test22233_graph_dot_verbose", "graph_dot_verbose") }

// TEST22234: `graph` on a terminal draws the picture at the terminal's width.
#[test]
fn test22234_graph_terminal() { golden("test22234_graph_terminal", "graph_terminal") }

// TEST22235: `view` without a terminal prints the picture as `graph` does.
#[test]
fn test22235_view_without_terminal() { golden("test22235_view_without_terminal", "view_without_terminal") }

// TEST22236: `edit` without a terminal says to edit the file and check it.
#[test]
fn test22236_edit_without_terminal() { golden("test22236_edit_without_terminal", "edit_without_terminal") }

// TEST22237: `view` on a terminal shows the machine full screen, moves focus with Tab, and quits with q.
#[test]
fn test22237_view_full_screen() { golden("test22237_view_full_screen", "view_full_screen") }

// TEST22238: `edit` on a terminal shows the machine, saves with Ctrl-S and quits with q.
#[test]
fn test22238_edit_full_screen() { golden("test22238_edit_full_screen", "edit_full_screen") }

// TEST22239: `run` of a machine on a file shows the run on stderr and writes its result.
#[test]
fn test22239_run_machine() { golden("test22239_run_machine", "run_machine") }

// TEST22240: `run --json` of a machine gives the run's records on stdout.
#[test]
fn test22240_run_machine_json() { golden("test22240_run_machine_json", "run_machine_json") }

// TEST22241: `-q run` of a map says its run's summary and what failed, and no progress.
#[test]
fn test22241_run_machine_quiet() { golden("test22241_run_machine_quiet", "run_machine_quiet") }

// TEST22242: `run` of a one-input machine on two files is two runs, each with its results and summary.
#[test]
fn test22242_run_machine_two_files() { golden("test22242_run_machine_two_files", "run_machine_two_files") }

// TEST22243: `run --progress plain` says the run in plain lines.
#[test]
fn test22243_run_machine_progress_plain() { golden("test22243_run_machine_progress_plain", "run_machine_progress_plain") }

// TEST22244: a step that refuses its input fails the run, named by the step.
#[test]
fn test22244_run_machine_refused() { golden("test22244_run_machine_refused", "run_machine_refused") }

// TEST22245: a refused input under --json is the run's records and the error's.
#[test]
fn test22245_run_machine_refused_json() { golden("test22245_run_machine_refused_json", "run_machine_refused_json") }

// TEST22246: a machine of constants runs on nothing and gives its result on stdout.
#[test]
fn test22246_run_machine_constant() { golden("test22246_run_machine_constant", "run_machine_constant") }

// TEST22247: a machine given nothing for an input names the input.
#[test]
fn test22247_run_machine_missing_input() { golden("test22247_run_machine_missing_input", "run_machine_missing_input") }

// TEST22248: a machine run given an option it has not is refused.
#[test]
fn test22248_run_machine_bad_option() { golden("test22248_run_machine_bad_option", "run_machine_bad_option") }

// TEST22249: `run <cap> <file>` runs one capability on the file.
#[test]
fn test22249_run_cap() { golden("test22249_run_cap", "run_cap") }

// TEST22250: a cap's own option, by its name, reaches the cap.
#[test]
fn test22250_run_cap_option() { golden("test22250_run_cap_option", "run_cap_option") }

// TEST22251: `run <cap> --help` shows the cap's own interface.
#[test]
fn test22251_run_cap_help() { golden("test22251_run_cap_help", "run_cap_help") }

// TEST22252: a cap given an option it has not is refused, naming where its options are.
#[test]
fn test22252_run_cap_unknown_option() { golden("test22252_run_cap_unknown_option", "run_cap_unknown_option") }

// TEST22253: `run` of a name no cap has offers the names near it.
#[test]
fn test22253_run_unknown_target() { golden("test22253_run_unknown_target", "run_unknown_target") }

// TEST22254: `run` of a machine file that does not exist says so.
#[test]
fn test22254_run_missing_machine_file() { golden("test22254_run_missing_machine_file", "run_missing_machine_file") }

// TEST22255: `run <cap>` reads its input from a pipe.
#[test]
fn test22255_run_cap_stdin() { golden("test22255_run_cap_stdin", "run_cap_stdin") }

// TEST22256: a run on a terminal draws the live view inline and leaves its summary.
#[test]
fn test22256_run_machine_inline() { golden("test22256_run_machine_inline", "run_machine_inline") }

// TEST22257: a failing run on a terminal leaves its summary and the error.
#[test]
fn test22257_run_machine_inline_refused() { golden("test22257_run_machine_inline_refused", "run_machine_inline_refused") }

// TEST22258: Ctrl-C in the live view stops the runs: what started finishes, the rest does not start.
#[test]
fn test22258_run_stopped_inline() { golden("test22258_run_stopped_inline", "run_stopped_inline") }

// TEST22259: Ctrl-C twice in the live view aborts the run.
#[test]
fn test22259_run_aborted_inline() { golden("test22259_run_aborted_inline", "run_aborted_inline") }

// TEST22260: `plan <file>` says what the file can become.
#[test]
fn test22260_plan_targets() { golden("test22260_plan_targets", "plan_targets") }

// TEST22261: `plan --json <file>` gives the file's type and the targets as records.
#[test]
fn test22261_plan_targets_json() { golden("test22261_plan_targets_json", "plan_targets_json") }

// TEST22262: `plan --to` lists the machines that make the target.
#[test]
fn test22262_plan_machines() { golden("test22262_plan_machines", "plan_machines") }

// TEST22263: `plan --pick` prints the machine chosen.
#[test]
fn test22263_plan_pick_print() { golden("test22263_plan_pick_print", "plan_pick_print") }

// TEST22264: `plan --pick --save` writes the machine chosen and says how to run it.
#[test]
fn test22264_plan_pick_save() { golden("test22264_plan_pick_save", "plan_pick_save") }

// TEST22265: `plan --pick` out of range says how many machines there are.
#[test]
fn test22265_plan_pick_out_of_range() { golden("test22265_plan_pick_out_of_range", "plan_pick_out_of_range") }

// TEST22266: `plan --json --pick` gives the machine chosen as a record.
#[test]
fn test22266_plan_pick_json() { golden("test22266_plan_pick_json", "plan_pick_json") }

// TEST22267: `plan --pick --run` runs the machine chosen on the file.
#[test]
fn test22267_plan_run() { golden("test22267_plan_run", "plan_run") }

// TEST22268: `plan` of a file whose extension no type has says so.
#[test]
fn test22268_plan_unknown_extension() { golden("test22268_plan_unknown_extension", "plan_unknown_extension") }

// TEST22269: `plan` on a terminal asks what to make and what to do with the machine.
#[test]
fn test22269_plan_inline() { golden("test22269_plan_inline", "plan_inline") }

// TEST22270: `cartridge list` with none installed says so.
#[test]
fn test22270_cartridge_list() { golden("test22270_cartridge_list", "cartridge_list") }

// TEST22271: `cartridge list --json` gives the cartridges as records.
#[test]
fn test22271_cartridge_list_json() { golden("test22271_cartridge_list_json", "cartridge_list_json") }

// TEST22272: `cartridge install` offline says what it cannot fetch.
#[test]
fn test22272_cartridge_install_offline() { golden("test22272_cartridge_install_offline", "cartridge_install_offline") }

// TEST22273: `model list` with no model cartridge says what is missing.
#[test]
fn test22273_model_list() { golden("test22273_model_list", "model_list") }

// TEST22274: `cache clear` says which cache it cleared.
#[test]
fn test22274_cache_clear() { golden("test22274_cache_clear", "cache_clear") }

// TEST22275: `cache clear --json` gives the cache's record.
#[test]
fn test22275_cache_clear_json() { golden("test22275_cache_clear_json", "cache_clear_json") }

// TEST22276: `cache refresh` offline is refused.
#[test]
fn test22276_cache_refresh_offline() { golden("test22276_cache_refresh_offline", "cache_refresh_offline") }

// TEST22277: `dev new` scaffolds a cartridge project and says what to do next.
#[test]
fn test22277_dev_new() { golden("test22277_dev_new", "dev_new") }

// TEST22278: `self update` offline is refused.
#[test]
fn test22278_self_update_offline() { golden("test22278_self_update_offline", "self_update_offline") }

// TEST22279: a failure its step's policy tolerates completes the run, and the run says so.
#[test]
fn test22279_run_machine_tolerated() { golden("test22279_run_machine_tolerated", "run_machine_tolerated") }

// TEST22281: `run` of a map writes a result for each item and says the run's summary.
#[test]
fn test22281_run_machine_map() { golden("test22281_run_machine_map", "run_machine_map") }

// TEST22282: an item of a map that refuses its input fails the run, named by the step.
#[test]
fn test22282_run_machine_map_refused() { golden("test22282_run_machine_map_refused", "run_machine_map_refused") }

// TEST22283: an item of a map whose failure its step's policy tolerates is left out, and the run completes tolerated.
#[test]
fn test22283_run_machine_map_tolerated() { golden("test22283_run_machine_map_tolerated", "run_machine_map_tolerated") }
