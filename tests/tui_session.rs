//! The terminal editor, end to end.
//!
//! The real editor session runs on a machine resolved against the real test
//! cartridge's caps and draws every frame into a headless display. The test
//! reads the frames the way a person reads the screen, presses keys the way a
//! person does, and checks what the session hands back and what it wrote
//! against what the edits must have made.

mod support;

use std::sync::Arc;
use std::time::Duration;

use capdag::orchestrator::{ResolvedNotation, RunInputs};
use capdag::tui::app::edit::{edit_session_on, CatalogCap, EditorOutcome, EditorSpec};
use capdag::tui::app::{HeadlessDisplay, TuiSettings};
use capdag::tui::theme::{Appearance, ColorDepth, GlyphSet};
use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use support::{testcartridge_caps, testcartridge_registry};

const TAG: &str = "tui-session";
const SAY: &str = r#"cap:in="media:void";test-dag-say;out="media:dag-said;enc=utf8""#;
const WORDS: &str = r#"cap:in="media:enc=utf8";test-dag-words;out="media:dag-word;enc=utf8""#;

/// The test cartridge's registry, with a media definition for every type its
/// caps take and give — what the fabric holds for a published cap, and what
/// the editor names each node by.
fn registry() -> Arc<capdag::FabricRegistry> {
    let registry = testcartridge_registry(TAG, Vec::new());
    for cap in testcartridge_caps(TAG) {
        for urn in [cap.urn.in_spec().to_string(), cap.urn.out_spec().to_string()] {
            if registry.get_cached_media_def(&urn).is_ok() {
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
                dims: Vec::new(),
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
