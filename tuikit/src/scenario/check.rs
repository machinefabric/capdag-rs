//! What a world may say: only what the engine could.
//!
//! Every script of a world is played, with no waits, against the plan of the
//! machine it is a run of — a machine file of the world, the cap it names run
//! on its own, or the machine it gives — and everything it plays is folded
//! into the run view's model of a run, which refuses what a run cannot do: a
//! report a step in its state cannot have, a signal for a step the plan does
//! not hold, an end a run cannot come to. A world's cartridges provide only
//! caps of its fabric; its files are of file types its fabric snapshot holds,
//! and its machine files plan against the snapshot, save those it holds to be
//! refused; and a journey is played only in a world there is.

use std::collections::BTreeSet;
use std::sync::Arc;

use capdag::orchestrator::cli_cap::synthesize_single_cap_notation;
use capdag::orchestrator::machine_plan::{ResolvedNotation, RunInputs, StrandPlan};
use capdag::orchestrator::observe::{RunKey, RunObserver};
use capdag::orchestrator::run_control::{RunAsk, RunControl, RunOutcome};
use capdag::orchestrator::RunVerdict;
use capdag::FabricRegistry;

use super::host::{run_key, ScenarioHost};
use super::script::{play, Names, RunScript};
use super::world::Scenarios;

/// Everything wrong with the scenarios, each said with where it is; nothing
/// when they are whole.
pub async fn check(scenarios: &Scenarios) -> Vec<String> {
    let mut wrong = Vec::new();
    let worlds = match scenarios.worlds() {
        Ok(worlds) => worlds,
        Err(e) => return vec![e],
    };
    for name in &worlds {
        if let Err(mut found) = check_world(scenarios, name).await {
            wrong.append(&mut found);
        }
    }
    match scenarios.journeys() {
        Ok(ids) => {
            for id in ids {
                match scenarios.journey(&id) {
                    Ok(journey) if !worlds.contains(&journey.world) => {
                        wrong.push(format!("journey {id}: there is no world '{}'", journey.world))
                    }
                    Ok(journey) if journey.modes.is_empty() => wrong.push(format!("journey {id}: it is played in no mode")),
                    Ok(_) => {}
                    Err(e) => wrong.push(e),
                }
            }
        }
        Err(e) => wrong.push(e),
    }
    wrong
}

async fn check_world(scenarios: &Scenarios, name: &str) -> Result<(), Vec<String>> {
    let at = |e: String| vec![format!("world {name}: {e}")];
    let world = scenarios.world(name).map_err(at)?;
    let fabric = scenarios.fabric(&world.fabric).map_err(at)?;
    let host = ScenarioHost::new(name, world.clone(), fabric, std::path::Path::new("/nonexistent"), 1.0).map_err(at)?;
    let registry = host.registry();
    let mut wrong = Vec::new();
    // A world's files with an extension are of file types its snapshot
    // holds: the snapshot keeps only part of a fabric, and a file of a type it
    // left out would be told from its extension as no fabric tells it. A file
    // with none (a cartridge's binary) is told by no extension anywhere.
    for path in world.files.keys().filter(|path| !path.ends_with(".machine")) {
        let Some(extension) = std::path::Path::new(path).extension().and_then(|extension| extension.to_str()) else {
            continue;
        };
        match registry.file_media_for_extension(extension).await {
            Ok(types) if types.is_empty() => wrong.push(format!(
                "world {name}: the file '{path}' is of no file type its fabric snapshot {} holds (.{extension})",
                world.fabric
            )),
            Ok(_) => {}
            Err(e) => wrong.push(format!("world {name}: the type of the file '{path}' cannot be told: {e}")),
        }
    }
    // The strands each script could be a run of: of the world's machine
    // files, of the machines scripts give, and of single caps.
    let mut strands: Vec<(String, StrandPlan)> = Vec::new();
    for path in &world.refused_machines {
        if !world.files.contains_key(path) || !path.ends_with(".machine") {
            wrong.push(format!("world {name}: '{path}' is said to be a refused machine, and it is no machine file of the world"));
        }
    }
    // Every machine file plans against the snapshot — a cap or media type it
    // names that the snapshot lacks is said here, not met as a refusal in a
    // journey — save the ones the world holds to be refused, which do not.
    for (path, text) in world.files.iter().filter(|(path, _)| path.ends_with(".machine")) {
        match (strands_of(text, &registry).await, world.refused_machines.contains(path)) {
            (Ok(planned), false) => strands.extend(planned.into_iter().map(|strand| (path.clone(), strand))),
            (Err(e), false) => wrong.push(format!("world {name}: the machine file '{path}' does not plan against its fabric snapshot: {e}")),
            (Ok(_), true) => wrong.push(format!("world {name}: the machine file '{path}' is said to be refused, and it plans")),
            (Err(_), true) => {}
        }
    }
    for (key, script) in &world.runs {
        if let Some(machine) = &script.machine {
            match strands_of(machine, &registry).await {
                Ok(planned) => strands.extend(planned.into_iter().map(|strand| (format!("the machine of '{key}'"), strand))),
                Err(e) => wrong.push(format!("world {name}: the machine of the run '{key}' does not plan: {e}")),
            }
        }
    }
    let mut scripted: BTreeSet<&String> = BTreeSet::new();
    for (whence, strand) in &strands {
        let key = run_key(strand, &registry);
        if let Some(script) = world.runs.get(&key) {
            scripted.insert(world.runs.get_key_value(&key).expect("just found").0);
            if let Err(e) = simulate(script, strand, &registry).await {
                wrong.push(format!("world {name}: the run '{key}' of {whence}: {e}"));
            }
        }
    }
    for (key, script) in &world.runs {
        if scripted.contains(key) {
            continue;
        }
        // A single cap: one of the fabric's, or one a dev cartridge of the
        // world declares itself, which a run finds among the dev cartridges.
        let cap = match key.contains(" > ") {
            true => None,
            false => match registry.get_cap(key).await.ok() {
                Some(cap) => Some(cap),
                None => match world
                    .installed
                    .iter()
                    .filter(|cartridge| cartridge.dev)
                    .flat_map(|cartridge| &cartridge.own_caps)
                    .find(|own| own.aliases.iter().any(|alias| alias == key))
                {
                    Some(own) => match own.cap() {
                        Ok(cap) => {
                            registry.add_caps_to_cache(vec![cap.clone()]);
                            Some(cap)
                        }
                        Err(e) => {
                            wrong.push(format!("world {name}: the run '{key}' is of the dev cap {e}"));
                            continue;
                        }
                    },
                    None => None,
                },
            },
        };
        let Some(cap) = cap else {
            wrong.push(format!(
                "world {name}: the run '{key}' is of no machine of the world and no single cap; give its machine"
            ));
            continue;
        };
        match synthesize_single_cap_notation(&cap) {
            Ok(notation) => match strands_of(&notation, &registry).await {
                Ok(planned) => {
                    for strand in planned {
                        if let Err(e) = simulate(script, &strand, &registry).await {
                            wrong.push(format!("world {name}: the run '{key}': {e}"));
                        }
                    }
                }
                Err(e) => wrong.push(format!("world {name}: '{key}' run on its own does not plan: {e}")),
            },
            Err(e) => wrong.push(format!("world {name}: '{key}' cannot be run on its own: {e}")),
        }
    }
    if wrong.is_empty() {
        Ok(())
    } else {
        Err(wrong)
    }
}

/// The plan of each strand of the machine `notation`, as a run of it plans.
async fn strands_of(notation: &str, registry: &Arc<FabricRegistry>) -> Result<Vec<StrandPlan>, String> {
    let resolved = ResolvedNotation::resolve(notation, registry.clone()).await.map_err(|e| e.to_string())?;
    let mut strands = Vec::new();
    for index in 0..resolved.strand_count() {
        strands.push(resolved.plan(index, &RunInputs::declared()).await.map_err(|e| e.to_string())?);
    }
    Ok(strands)
}

/// Play `script` on `strand` with no waits, as it runs and as it runs when
/// stopped, folding everything it says into the run view's model of a run.
async fn simulate(script: &RunScript, strand: &StrandPlan, registry: &Arc<FabricRegistry>) -> Result<(), String> {
    Names::of(strand, registry)?.check(script)?;
    run_through(script, strand, registry, false).await?;
    if script.on_stop.is_some() {
        run_through(script, strand, registry, true).await.map_err(|e| format!("stopped: {e}"))?;
    }
    Ok(())
}

async fn run_through(script: &RunScript, strand: &StrandPlan, registry: &Arc<FabricRegistry>, stopped: bool) -> Result<(), String> {
    let key = RunKey { run: 0, strand: 0 };
    let (observer, mut events) = RunObserver::new();
    let control = Arc::new(RunControl::new());
    if stopped {
        control.ask(RunAsk::Interrupt);
    }
    let names = Names::of(strand, registry)?;
    let result = play(script.clone(), names, observer.callbacks(key), control, 1e9).await;
    let mut watch = crate::cli::watch::Watch::new(2);
    watch.add_run(key, String::new(), strand, registry);
    while let Ok(event) = events.try_recv() {
        watch.apply(&event).map_err(|e| format!("it plays what no run can: {e}"))?;
    }
    let outcome = match &result {
        Ok(result) if matches!(result.verdict, RunVerdict::CompletedWithToleratedFailures) => RunOutcome::Tolerated,
        Ok(_) if stopped => RunOutcome::Stopped,
        Ok(_) => RunOutcome::Completed,
        Err(error) => {
            if let Some(token) = error.step_token_id() {
                watch.failed_at(key, token.as_str()).map_err(|e| format!("it ends as no run can: {e}"))?;
            }
            RunOutcome::Failed
        }
    };
    watch.end_run(key, outcome).map_err(|e| format!("it ends as no run can: {e}"))?;
    Ok(())
}
