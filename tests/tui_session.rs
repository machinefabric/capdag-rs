//! The terminal run view, end to end.
//!
//! The real session loop runs a real machine on the real test cartridge —
//! hosted by a `CliRuntime` exactly as `capdag run --tui` hosts it — and draws
//! every frame into a headless display. The test reads the frames the way a
//! person reads the screen, presses keys the way a person does, and checks
//! what the session hands back against what the run must have produced.

mod support;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use capdag::orchestrator::{CliRuntime, PlanView, PlannedStrand, ResolvedNotation, RunEnd, RunInputs};
use capdag::tui::app::run::{run_session_on, step_arguments, RunSessionSpec, SessionRun, SessionStrand};
use capdag::tui::app::{HeadlessDisplay, TuiSettings};
use capdag::tui::theme::{Appearance, ColorDepth, GlyphSet};
use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use support::{testcartridge_bin, testcartridge_caps, testcartridge_registry};

const TAG: &str = "tui-session";
const SAY: &str = r#"cap:in="media:void";test-dag-say;out="media:dag-said;enc=utf-8""#;
const WORDS: &str = r#"cap:in="media:enc=utf-8";test-dag-words;out="media:dag-word;enc=utf-8""#;
const SHOUT: &str = r#"cap:in="media:enc=utf-8";test-dag-shout;out="media:dag-shout;enc=utf-8""#;

/// The test cartridge's registry, with a media definition for every type its
/// caps take and give — what the fabric holds for a published cap, and what
/// drawing a plan names each value by.
fn registry() -> Arc<capdag::FabricRegistry> {
    let registry = testcartridge_registry(TAG, Vec::new());
    for cap in testcartridge_caps(TAG) {
        for urn in [cap.urn.in_spec().to_string(), cap.urn.out_spec().to_string()] {
            if registry.get_cached_media_def(&urn).is_some() {
                continue;
            }
            registry.insert_cached_media_def_for_test(capdag::StoredMediaDef {
                version: 0,
                urn: urn.clone(),
                media_type: "application/octet-stream".to_string(),
                title: urn.trim_start_matches("media:").to_string(),
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
    registry
}

fn text_of(buffer: &ratatui::buffer::Buffer) -> String {
    capdag::tui::canvas::to_text(buffer, false)
}

fn key(code: KeyCode) -> std::io::Result<Event> {
    Ok(Event::Key(KeyEvent::new(code, KeyModifiers::NONE)))
}

// TEST13050: a machine whose middle step makes a sequence runs in the run
// view to its end: the view shows the map over the three words the text has,
// every step done, and the output's three files; quitting hands back the
// run's end as completed, and the files hold what the machine makes of the
// text — every word shouted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test13050_run_view_runs_a_machine_to_its_end() {
    let registry = registry();
    let notation = format!(
        "[say {SAY}][words {WORDS}][shout {SHOUT}]\
         [(text=\"hello brave world\") -> say -> s][s -> words -> w][w -> shout -> out]"
    );
    let resolved = ResolvedNotation::resolve(&notation, registry.clone())
        .await
        .expect("the notation resolves against the test cartridge");
    let strand = resolved.plan(0, &RunInputs::default()).await.expect("the machine plans");
    let planned = PlannedStrand {
        strand,
        inputs: HashMap::new(),
        is_sequence: HashMap::new(),
        cap_arguments: HashMap::new(),
    };
    let view = PlanView::of(&registry, &planned.strand.plan, &planned.strand.node_names)
        .expect("the plan draws");
    let arguments = step_arguments(registry.clone(), &planned, &view)
        .await
        .expect("the steps' arguments are known");

    let work = tempfile::TempDir::new().unwrap();
    let out_dir = work.path().join("out");
    let cartridges = work.path().join("cartridges");
    std::fs::create_dir_all(&cartridges).unwrap();
    let runtime = Arc::new(
        CliRuntime::new(
            cartridges,
            None,
            capdag::CartridgeChannel::Release,
            capdag::FABRIC_MANIFEST_VERSION,
            vec![testcartridge_bin(TAG)],
            None,
            registry.clone(),
            None,
            out_dir.clone(),
        )
        .with_transient_root(work.path().join("transient")),
    );
    let spec = RunSessionSpec {
        machine: "shout.machine".to_string(),
        notation: notation.clone(),
        runs: vec![SessionRun {
            label: "shout.machine".to_string(),
            stem: "shout".to_string(),
            strands: vec![SessionStrand {
                planned,
                view,
                arguments,
                sources: BTreeMap::new(),
            }],
        }],
        output_dir: Some(out_dir.clone()),
        force: false,
    };
    let settings = TuiSettings {
        appearance: Appearance::Dark,
        glyphs: GlyphSet::Unicode,
        depth: ColorDepth::None,
        motion: false,
    };

    let mut display = HeadlessDisplay::new(180, 50);
    let frames = display.frames();
    let (keys, input) = tokio::sync::mpsc::unbounded_channel::<std::io::Result<Event>>();
    let input = Box::pin(futures_util::stream::unfold(input, |mut rx| async move {
        rx.recv().await.map(|event| (event, rx))
    }));

    // Watch the screen; when the run reads done, look at it, then quit.
    let watcher = async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
        loop {
            let screen = text_of(&frames.lock().unwrap());
            if screen.contains(" done ") {
                return screen;
            }
            assert!(!screen.contains(" failed "), "the run failed:\n{screen}");
            assert!(tokio::time::Instant::now() < deadline, "the run did not end:\n{screen}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    let session = run_session_on(&mut display, input, runtime, spec, settings);
    tokio::pin!(session);
    let screen = tokio::select! {
        screen = watcher => screen,
        outcome = &mut session => panic!("the session ended before the run did: {:?}", outcome.map(|o| o.ends)),
    };
    // The map over the words of the text: a stack of three, said on its
    // card (the subtitle is cut to the card's width).
    assert!(
        screen.contains("3 items are processed") && screen.contains(" 3 items "),
        "the map's stack counts the text's three words:\n{screen}"
    );
    // The output's stack holds its three files.
    assert!(screen.contains("3 files"), "the output's stack holds three files:\n{screen}");
    keys.send(key(KeyCode::Char('q'))).unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(30), session)
        .await
        .expect("quitting ends the session")
        .expect("the session ends without a fault");

    assert_eq!(outcome.ends, vec![RunEnd::Completed]);
    assert!(!outcome.aborted);
    // In file mode the run's stdout names the files it wrote, one a line.
    let listed: Vec<std::path::PathBuf> = std::str::from_utf8(&outcome.stdout)
        .expect("the listing is text")
        .lines()
        .map(std::path::PathBuf::from)
        .collect();
    assert_eq!(listed.len(), 3, "stdout lists the three files");
    for path in &listed {
        assert!(path.starts_with(out_dir.canonicalize().unwrap()), "{} is in the output directory", path.display());
    }
    let mut written: Vec<String> = std::fs::read_dir(&out_dir)
        .unwrap()
        .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect();
    written.sort();
    assert_eq!(written, vec!["BRAVE", "HELLO", "WORLD"], "every word shouted, one file each");
}

/// Press keys, read the screen: wait until the last frame says `wanted`.
async fn screen_until(
    frames: &std::sync::Arc<std::sync::Mutex<ratatui::buffer::Buffer>>,
    wanted: impl Fn(&str) -> bool,
    what: &str,
) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let screen = text_of(&frames.lock().unwrap());
        if wanted(&screen) {
            return screen;
        }
        assert!(tokio::time::Instant::now() < deadline, "the editor never showed {what}:\n{screen}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn ctrl(c: char) -> std::io::Result<Event> {
    Ok(Event::Key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)))
}

// TEST13064: the editor commits an edit only once the machine it makes
// resolves, rolls back one that does not — the committed machine, the file
// and the history untouched — undoes and redoes, and saves the committed
// machine to the file. A step added after the last node reads that node, and
// the saved machine resolves with three steps.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test13064_editor_commits_resolvable_edits_and_rolls_back_the_rest() {
    use capdag::tui::app::edit::{edit_session_on, CatalogCap, EditorOutcome, EditorSpec};
    let registry = registry();
    let catalog = CatalogCap::from_registry(&registry).await.expect("the registry's caps are read");
    let notation = format!("[say {SAY}]\n[words {WORDS}]\n[(text=\"hello brave world\") -> say -> s]\n[s -> words -> w]\n");
    let work = tempfile::TempDir::new().unwrap();
    let path = work.path().join("words.machine");
    std::fs::write(&path, &notation).unwrap();
    let spec = EditorSpec {
        path: path.clone(),
        text: notation.clone(),
        read_only: false,
        registry: registry.clone(),
        machines: Arc::new(capdag::NoMachineSource),
        machine_id: None,
        catalog,
        inputs_given: false,
    };
    let settings = TuiSettings {
        appearance: Appearance::Dark,
        glyphs: GlyphSet::Unicode,
        depth: ColorDepth::None,
        motion: false,
    };
    let mut display = HeadlessDisplay::new(160, 45);
    let frames = display.frames();
    let (keys, input) = tokio::sync::mpsc::unbounded_channel::<std::io::Result<Event>>();
    let input = Box::pin(futures_util::stream::unfold(input, |mut rx| async move {
        rx.recv().await.map(|event| (event, rx))
    }));
    let session = edit_session_on(&mut display, input, spec, settings);
    tokio::pin!(session);

    let script = async {
        screen_until(&frames, |s| s.contains(" resolves · 2 steps"), "the machine resolving").await;
        // Focus the machine's output: the node no step reads.
        let mut focused = false;
        for _ in 0..8 {
            keys.send(key(KeyCode::Tab)).unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            if text_of(&frames.lock().unwrap()).contains("An output of the machine") {
                focused = true;
                break;
            }
        }
        assert!(focused, "tab reaches the output node");
        // A step after it, chosen by name from the caps that take what it holds.
        keys.send(key(KeyCode::Char('a'))).unwrap();
        for c in "shout".chars() {
            keys.send(key(KeyCode::Char(c))).unwrap();
        }
        keys.send(key(KeyCode::Enter)).unwrap();
        let screen = screen_until(&frames, |s| s.contains(" resolves · 3 steps"), "the added step").await;
        assert!(screen.contains("words.machine ●"), "the change is not saved yet:\n{screen}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), notation, "nothing is written before saving");

        keys.send(key(KeyCode::Char('u'))).unwrap();
        screen_until(&frames, |s| s.contains(" resolves · 2 steps"), "the undone step").await;
        keys.send(key(KeyCode::Char('U'))).unwrap();
        screen_until(&frames, |s| s.contains(" resolves · 3 steps"), "the redone step").await;
        keys.send(ctrl('s')).unwrap();
        screen_until(&frames, |s| s.contains("Saved "), "the save").await;

        // Source that parses but names a cap no header declares: rolled back.
        keys.send(key(KeyCode::Char('e'))).unwrap();
        keys.send(key(KeyCode::PageDown)).unwrap();
        keys.send(key(KeyCode::End)).unwrap();
        for c in "[w -> nosuch -> z]".chars() {
            keys.send(key(KeyCode::Char(c))).unwrap();
        }
        keys.send(ctrl('s')).unwrap();
        let screen = screen_until(&frames, |s| s.contains("Not applied"), "the rolled-back source").await;
        assert!(screen.contains(" source not applied · 3 steps"), "the committed machine stands:\n{screen}");
        keys.send(key(KeyCode::Esc)).unwrap();
        keys.send(key(KeyCode::Char('q'))).unwrap();
        screen_until(&frames, |s| s.contains("The source has changes not applied."), "the quit question").await;
        keys.send(key(KeyCode::Char('y'))).unwrap();
    };
    let outcome = tokio::select! {
        () = script => tokio::time::timeout(Duration::from_secs(10), &mut session).await.expect("quitting ends the editor"),
        outcome = &mut session => panic!("the editor ended before the script did: {outcome:?}"),
    };
    assert_eq!(outcome.expect("the editor ends without a fault"), EditorOutcome::Closed { saved: true });

    let saved = std::fs::read_to_string(&path).unwrap();
    assert!(!saved.contains("nosuch"), "the rolled-back source never reaches the file:\n{saved}");
    let resolved = ResolvedNotation::resolve(&saved, registry.clone())
        .await
        .unwrap_or_else(|e| panic!("the saved machine resolves: {e}\n{saved}"));
    let plan = resolved.plan(0, &RunInputs::default()).await.expect("the saved machine plans");
    let steps: Vec<String> = plan
        .plan
        .nodes
        .values()
        .filter_map(|node| match &node.node_type {
            capdag::planner::ExecutionNodeType::Cap { cap_urn, .. } => Some(cap_urn.to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(steps.len(), 3, "say, words and the added shout:\n{saved}");
    assert!(
        steps.iter().any(|urn| urn.contains("test-dag-shout")),
        "the added step is the shout chosen:\n{saved}"
    );
    assert!(
        saved.contains("w -> ") && saved.contains("shout"),
        "the shout reads the node it was added after:\n{saved}"
    );
}

// TEST13065: the live state of a run is folded from what a real run says.
// Every signal the run gave folds without a fault, in the order it gave
// them; the map holds the text's three words as three items, each run of
// the body succeeded, and every step reached its end; the run's progress
// never goes down. What a run cannot have said is refused, not drawn: a
// step the plan does not have, an item that skips one, an item published
// twice. A cancelled run is shown cancelled — never as a failure, and with
// no step painted as failed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test13065_live_state_folds_a_real_run_and_refuses_what_it_cannot_have_said() {
    use capdag::orchestrator::{spawn_strand, RunKey, RunObserver, RunSignal};
    use capdag::tui::live::{LiveEnd, LiveError, RunFailure, RunLive, RunStatus, StepPhase};
    let registry = registry();
    let notation = format!(
        "[say {SAY}][words {WORDS}][shout {SHOUT}]\
         [(text=\"hello brave world\") -> say -> s][s -> words -> w][w -> shout -> out]"
    );
    let resolved = ResolvedNotation::resolve(&notation, registry.clone()).await.unwrap();
    let strand = resolved.plan(0, &RunInputs::default()).await.unwrap();
    let view = PlanView::of(&registry, &strand.plan, &strand.node_names).unwrap();
    let work = tempfile::TempDir::new().unwrap();
    let cartridges = work.path().join("cartridges");
    std::fs::create_dir_all(&cartridges).unwrap();
    let runtime = Arc::new(CliRuntime::new(
        cartridges,
        None,
        capdag::CartridgeChannel::Release,
        capdag::FABRIC_MANIFEST_VERSION,
        vec![testcartridge_bin(TAG)],
        None,
        registry.clone(),
        None,
        work.path().join("out"),
    ));
    let (observer, mut events) = RunObserver::new();
    let key = RunKey { run: 0, strand: 0 };
    let planned = PlannedStrand {
        strand,
        inputs: HashMap::new(),
        is_sequence: HashMap::new(),
        cap_arguments: HashMap::new(),
    };
    let handle = spawn_strand(&runtime, &observer, key, planned, None).unwrap_or_else(|(_, e)| panic!("{e}"));
    drop(observer);
    let (_, result) = handle.task.await.expect("the run's task ends");
    let result = result.expect("the run completes");
    let mut recorded: Vec<RunSignal> = Vec::new();
    while let Ok(event) = events.try_recv() {
        assert_eq!(event.key, key);
        recorded.push(event.signal);
    }
    assert!(!recorded.is_empty(), "the run said something");

    let started = std::time::Instant::now();
    let mut live = RunLive::new(&view, started);
    let mut progress = 0.0f32;
    for signal in &recorded {
        live.apply(signal, started).unwrap_or_else(|e| panic!("{e}: {signal:?}"));
        assert!(live.progress >= progress, "progress went down at {signal:?}");
        progress = live.progress;
    }
    live.finish(
        LiveEnd::Completed {
            verdict: result.verdict.clone(),
            outputs: Default::default(),
        },
        started,
    )
    .unwrap();
    assert!(matches!(live.status, RunStatus::Completed(_)));
    assert_eq!(live.progress, 1.0);
    let map = live.maps.values().next().expect("the plan's map");
    assert_eq!(map.items.len(), 3, "one item a word");
    assert_eq!(
        map.items.iter().map(|i| i.key.label()).collect::<Vec<_>>(),
        vec!["Item 1", "Item 2", "Item 3"]
    );
    assert!(
        map.items.iter().all(|i| i.outcome.as_ref().is_some_and(|o| o.success)),
        "every body succeeded: {:?}",
        map.items
    );
    // A step outside a map reaches its end by its own progress; one inside
    // reports none and stands as its map's items stand.
    for node in &view.nodes {
        if let capdag::orchestrator::PlanViewKind::Step(step) = &node.kind {
            if node.enclosing_map.is_none() {
                assert_eq!(live.steps[&step.token_id.to_string()].phase, StepPhase::Done, "{} reached its end", node.id);
            }
        }
    }
    assert!(live.failed_tokens().is_empty());
    // What is drawn of it: every step done.
    let sources = BTreeMap::new();
    let drawn = capdag::tui::scene::run_scene(
        &view,
        &HashMap::new(),
        Some(&capdag::tui::scene::RunContext { live: &live, sources: &sources }),
    )
    .expect("the finished run draws");
    let steps: Vec<_> = drawn.nodes.iter().filter(|n| n.role == capdag::tui::scene::NodeRole::Step).collect();
    assert_eq!(steps.len(), 3);
    for node in steps {
        assert_eq!(node.state, capdag::tui::scene::NodeState::Done, "{} is drawn done", node.id);
    }

    // What the run cannot have said.
    let first_step = recorded
        .iter()
        .find_map(|s| match s {
            RunSignal::StepProgress { token_id, .. } if live.step_node(token_id).is_some() => Some(s.clone()),
            _ => None,
        })
        .expect("a step reported progress");
    let RunSignal::StepProgress { cap_urn, progress, .. } = first_step else { unreachable!() };
    let mut fresh = RunLive::new(&view, started);
    assert!(matches!(
        fresh.apply(
            &RunSignal::StepProgress { token_id: "no-such-step".to_string(), cap_urn, progress },
            started
        ),
        Err(LiveError::UnknownToken(_))
    ));
    let items = recorded
        .iter()
        .find(|s| matches!(s, RunSignal::ForEachItems { items, .. } if !items.is_empty()))
        .expect("the map published its items")
        .clone();
    let RunSignal::ForEachItems { map_token, items } = items else { unreachable!() };
    let mut skipping = RunLive::new(&view, started);
    let skipped = RunSignal::ForEachItems {
        map_token: map_token.clone(),
        items: items.iter().skip(1).cloned().collect(),
    };
    if !skipped_is_empty(&skipped) {
        assert!(matches!(skipping.apply(&skipped, started), Err(LiveError::ItemsNotContiguous { .. })));
    }
    let mut twice = RunLive::new(&view, started);
    let once = RunSignal::ForEachItems { map_token: map_token.clone(), items: items.clone() };
    twice.apply(&once, started).unwrap();
    assert!(matches!(twice.apply(&once, started), Err(LiveError::ItemsNotContiguous { .. })));

    // Cancelled by the user: shown cancelled, nothing failed.
    let mut cancelled = RunLive::new(&view, started);
    for signal in &recorded {
        cancelled.apply(signal, started).unwrap();
    }
    // A cancel arrives as the step it stopped, failed with the user's class.
    let stopped = view
        .nodes
        .iter()
        .find_map(|n| match &n.kind {
            capdag::orchestrator::PlanViewKind::Step(step) => Some(step.token_id.clone()),
            _ => None,
        })
        .expect("the plan has steps");
    let cancel = capdag::ExecutionError::StepFailed {
        step_token_id: stopped,
        source: Box::new(capdag::ExecutionError::CartridgeExecutionFailed {
            cap_urn: SHOUT.to_string(),
            code: None,
            class: capdag::AttributionClass::User,
            details: "cancelled by the user".to_string(),
            arg_urn: None,
        }),
    };
    cancelled.finish(LiveEnd::Failed(RunFailure::of(&cancel)), started).unwrap();
    assert!(matches!(cancelled.status, RunStatus::Cancelled(_)), "{:?}", cancelled.status);
    assert!(cancelled.failed_tokens().is_empty());
    let drawn = capdag::tui::scene::run_scene(
        &view,
        &HashMap::new(),
        Some(&capdag::tui::scene::RunContext { live: &cancelled, sources: &sources }),
    )
    .expect("the cancelled run draws");
    assert!(
        drawn.nodes.iter().all(|n| n.state != capdag::tui::scene::NodeState::Failed),
        "nothing of a cancelled run is drawn failed"
    );
}

fn skipped_is_empty(signal: &capdag::orchestrator::RunSignal) -> bool {
    matches!(signal, capdag::orchestrator::RunSignal::ForEachItems { items, .. } if items.is_empty())
}
