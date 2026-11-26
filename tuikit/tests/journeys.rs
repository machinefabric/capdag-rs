//! The scenarios: every journey, played in every mode it declares by the
//! capdag-tuikit binary on its world, comes to what it should; and every world
//! says only what the engine could.
#![cfg(unix)]

use capdag_tuikit::scenario::check::check;
use capdag_tuikit::scenario::play::{unmet, Player};
use capdag_tuikit::scenario::world::Scenarios;

fn player(scenarios: Scenarios) -> Player {
    // Twenty times as fast as the scripts' waits: what a journey waits for
    // still comes in the order its script gives.
    Player { exe: env!("CARGO_BIN_EXE_capdag-tuikit").into(), scenarios, speed: 20.0 }
}

// TEST22285: every journey, in every mode it declares — full screen, inline,
// plain and --json — comes to what it says: its exit status, what stdout
// holds, what is shown, and what is not.
#[test]
fn test22285_every_journey_comes_to_what_it_should() {
    let scenarios = Scenarios::bundled();
    let player = player(scenarios.clone());
    let ids = scenarios.journeys().expect("the journeys are listed");
    assert!(!ids.is_empty(), "there are journeys");
    let mut failures = Vec::new();
    for id in ids {
        let journey = scenarios.journey(&id).expect("every journey is valid");
        for (mode, leg) in &journey.modes {
            let dir = tempfile::tempdir().expect("a working directory for the journey");
            match player.play(&journey, *mode, dir.path(), None) {
                Ok(played) => {
                    let unmet = unmet(&leg.expect, &played);
                    if !unmet.is_empty() {
                        failures.push(format!(
                            "{id} in {} mode: {}\n--- stdout ---\n{}\n--- shown ---\n{}",
                            mode.name(),
                            unmet.join("; "),
                            played.stdout,
                            played.shown
                        ));
                    }
                }
                Err(e) => failures.push(format!("{id} in {} mode: {e}", mode.name())),
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}

// TEST22286: the scenarios are whole: every world's script, played against
// the plan of the machine it is a run of, says only what a run can, every
// world's cartridges provide caps of its fabric, and every journey names a
// world there is.
#[tokio::test]
async fn test22286_the_scenarios_say_only_what_the_engine_could() {
    let wrong = check(&Scenarios::bundled()).await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

// TEST22287: a script that says what no run could is refused, saying what:
// a report for a step its plan does not hold, and an item outcome for a run
// with no map.
#[tokio::test]
async fn test22287_a_script_no_run_could_play_is_refused() {
    let bundled = Scenarios::bundled();
    let dir = tempfile::tempdir().expect("a scenarios directory");
    for sub in ["worlds", "journeys", "fabric"] {
        std::fs::create_dir_all(dir.path().join(sub)).unwrap();
    }
    let world: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(bundled.dir.join("worlds/ordinary.json")).unwrap()).unwrap();
    let fabric = world["fabric"].as_str().expect("a world names its fabric").to_string();
    std::fs::copy(bundled.dir.join("fabric").join(&fabric), dir.path().join("fabric").join(&fabric)).unwrap();
    let script = |signals: serde_json::Value| {
        serde_json::json!({
            "description": "a world whose one run says what no run can",
            "fabric": fabric,
            "runs": { "disbind-pdf": { "signals": signals, "end": { "completed": { "outputs": { "page-text": ["page one"] } } } } }
        })
    };
    let cases = [
        (serde_json::json!([{ "report": { "step": "nosuch", "outcome": { "skipped": { "absent": [] } } } }]), "no step 'nosuch'"),
        (serde_json::json!([{ "items": { "map": "disbind-pdf", "items": ["a"] } }]), "no map 'disbind-pdf'"),
    ];
    for (signals, said) in cases {
        std::fs::write(dir.path().join("worlds/bad.json"), script(signals).to_string()).unwrap();
        let wrong = check(&Scenarios::at(dir.path())).await;
        assert!(
            wrong.iter().any(|line| line.contains("world bad") && line.contains(said)),
            "the check says {said}: {wrong:?}"
        );
    }
}

// TEST22288: a world that uses what its fabric snapshot lacks is refused,
// saying what: a file of a type the snapshot holds none of, and a machine
// file naming a cap it does not have; and a machine file the world holds to
// be refused must be.
#[tokio::test]
async fn test22288_a_world_using_what_its_snapshot_lacks_is_refused() {
    let bundled = Scenarios::bundled();
    let dir = tempfile::tempdir().expect("a scenarios directory");
    for sub in ["worlds", "journeys", "fabric"] {
        std::fs::create_dir_all(dir.path().join(sub)).unwrap();
    }
    std::fs::copy(bundled.dir.join("fabric/capdag.json"), dir.path().join("fabric/capdag.json")).unwrap();
    let world = |files: serde_json::Value, refused: serde_json::Value| {
        serde_json::json!({
            "description": "a world that uses what its snapshot lacks",
            "fabric": "capdag.json",
            "files": files,
            "refused_machines": refused
        })
    };
    let pages = "[doc -> disbind-pdf -> pages]\n";
    let cases = [
        (world(serde_json::json!({ "scan.qqz": "?" }), serde_json::json!([])), "is of no file type its fabric snapshot capdag.json holds (.qqz)"),
        (
            world(serde_json::json!({ "odd.machine": "[doc -> disbind-qqz -> pages]\n" }), serde_json::json!([])),
            "the machine file 'odd.machine' does not plan against its fabric snapshot",
        ),
        (
            world(serde_json::json!({ "pages.machine": pages }), serde_json::json!(["pages.machine"])),
            "the machine file 'pages.machine' is said to be refused, and it plans",
        ),
    ];
    for (declared, said) in cases {
        std::fs::write(dir.path().join("worlds/bad.json"), declared.to_string()).unwrap();
        let wrong = check(&Scenarios::at(dir.path())).await;
        assert!(
            wrong.iter().any(|line| line.contains("world bad") && line.contains(said)),
            "the check says {said}: {wrong:?}"
        );
    }
    // The same world, with what it uses held by the snapshot, is whole.
    let whole = world(serde_json::json!({ "report.pdf": "%PDF-1.7\n", "pages.machine": pages }), serde_json::json!([]));
    std::fs::write(dir.path().join("worlds/bad.json"), whole.to_string()).unwrap();
    let wrong = check(&Scenarios::at(dir.path())).await;
    assert!(wrong.is_empty(), "{wrong:?}");
}
