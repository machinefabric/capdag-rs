//! `capdag-tuikit`: the capdag command on worlds of test data.
//!
//! - `capdag-tuikit list` — the worlds and the journeys.
//! - `capdag-tuikit check` — every world says only what the engine could.
//! - `capdag-tuikit journey <ID> [--mode M] [--speed N] [--step]` — play a
//!   journey on this terminal, as a person would see it.
//! - `capdag-tuikit journey <ID> --check [--mode M]` — play it unwatched, as
//!   the journey tests do, and say what each mode came to.
//! - `capdag-tuikit --world <W> [--speed N] -- <capdag args>` — the capdag
//!   command line, any of it, on a world, in the working directory.
//! - `capdag-tuikit world files <W> <DIR>` — write a world's files into DIR.
//! - `capdag-tuikit world fabric --out <FILE> [--anchor <URL>]` — snapshot a
//!   published fabric for worlds to serve.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use capdag_tuikit::scenario::host::ScenarioHost;
use capdag_tuikit::scenario::journey::Mode;
use capdag_tuikit::scenario::world::{FabricSnapshot, Scenarios};
use clap::{Parser, Subcommand};

/// The capdag command on worlds of test data: every surface, flag and command
/// flow, served from a world instead of the engine.
#[derive(Debug, Parser)]
#[command(name = "capdag-tuikit", disable_help_subcommand = true)]
struct Kit {
    /// The scenarios: worlds, fabric snapshots and journeys (default: the ones
    /// beside this kit's sources)
    #[arg(long, value_name = "DIR", global = true)]
    scenarios: Option<PathBuf>,
    /// Run the capdag command line after `--` on this world
    #[arg(long, value_name = "WORLD")]
    world: Option<String>,
    /// How many times as fast as their scripts' waits runs go
    #[arg(long, value_name = "N", default_value_t = 1.0, global = true)]
    speed: f64,
    #[command(subcommand)]
    command: Option<KitCommand>,
    /// The capdag command line, after `capdag`
    #[arg(last = true, value_name = "CAPDAG ARGS")]
    capdag: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum KitCommand {
    /// The worlds and the journeys
    List,
    /// Check that every world says only what the engine could, and every journey names a world
    Check,
    /// Play a journey on this terminal
    Journey {
        /// The journey, as `list` names it: CJRN-001
        id: String,
        /// The mode to play it in (default: its first): full_screen, inline, plain or json
        #[arg(long, value_name = "MODE")]
        mode: Option<String>,
        /// Wait for Space before each key the journey presses
        #[arg(long, conflicts_with = "check")]
        step: bool,
        /// Play it unwatched, in every mode it declares (or --mode), and say
        /// whether each came to what it should, with what it printed when not
        #[arg(long)]
        check: bool,
    },
    /// Worlds' files and fabric
    #[command(subcommand)]
    World(WorldCommand),
}

#[derive(Debug, Subcommand)]
enum WorldCommand {
    /// Write a world's files into a directory, to run the command there
    Files { world: String, dir: PathBuf },
    /// Snapshot a published fabric — its caps, media types and aliases — for worlds to serve
    Fabric {
        /// Where to write the snapshot
        #[arg(long, value_name = "FILE")]
        out: PathBuf,
        /// The fabric (default: the one this build is bound to)
        #[arg(long, value_name = "URL")]
        anchor: Option<String>,
    },
}

fn main() {
    let kit = Kit::parse();
    let scenarios = kit.scenarios.clone().map(Scenarios::at).unwrap_or_else(Scenarios::bundled);
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("a runtime for the kit");
    let code = match (kit.command, kit.world) {
        (None, Some(world)) => runtime.block_on(run_on_world(&scenarios, &world, kit.speed, kit.capdag)),
        (Some(_), Some(_)) => fail("--world runs a capdag command line, after `--`; it takes no kit command"),
        (None, None) => fail("give a kit command (list, journey, world) or --world <W> -- <capdag args>; --help says more"),
        (Some(command), None) if !kit.capdag.is_empty() => {
            fail(&format!("the capdag command line after `--` is run on a world: give --world, not {command:?}"))
        }
        (Some(KitCommand::List), None) => list(&scenarios),
        (Some(KitCommand::Check), None) => {
            let wrong = runtime.block_on(capdag_tuikit::scenario::check::check(&scenarios));
            for line in &wrong {
                eprintln!("{line}");
            }
            i32::from(!wrong.is_empty())
        }
        (Some(KitCommand::Journey { id, mode, check: true, .. }), None) => {
            check_journey(&scenarios, &id, mode.as_deref(), kit.speed)
        }
        (Some(KitCommand::Journey { id, mode, step, check: false }), None) => {
            journey(&scenarios, &id, mode.as_deref(), kit.speed, step)
        }
        (Some(KitCommand::World(WorldCommand::Files { world, dir })), None) => files(&scenarios, &world, &dir),
        (Some(KitCommand::World(WorldCommand::Fabric { out, anchor })), None) => {
            runtime.block_on(snapshot_fabric(&out, anchor))
        }
    };
    std::process::exit(code);
}

fn fail(message: &str) -> i32 {
    eprintln!("error: {message}");
    2
}

/// The capdag command line `args` on world `name`, in the working directory.
async fn run_on_world(scenarios: &Scenarios, name: &str, speed: f64, args: Vec<String>) -> i32 {
    let host = match scenarios
        .world(name)
        .and_then(|world| scenarios.fabric(&world.fabric).map(|fabric| (world, fabric)))
        .and_then(|(world, fabric)| {
            let root = std::env::current_dir().map_err(|e| e.to_string())?.join(".capdag-world");
            ScenarioHost::new(name, world, fabric, &root, speed)
        }) {
        Ok(host) => host,
        Err(e) => return fail(&e),
    };
    let mut argv = vec!["capdag".to_string()];
    argv.extend(args);
    capdag_tuikit::cli::run_command_line(argv, Arc::new(host)).await
}

fn list(scenarios: &Scenarios) -> i32 {
    let listed = (|| -> Result<(), String> {
        println!("Worlds:");
        for name in scenarios.worlds()? {
            println!("  {name:<16} {}", scenarios.world(&name)?.description);
        }
        println!("\nJourneys:");
        for id in scenarios.journeys()? {
            let journey = scenarios.journey(&id)?;
            let modes: Vec<&str> = journey.modes.keys().map(|mode| mode.name()).collect();
            println!("  {id}  {}  [{}; {}]", journey.title, journey.world, modes.join(", "));
        }
        Ok(())
    })();
    match listed {
        Ok(()) => 0,
        Err(e) => fail(&e),
    }
}

fn files(scenarios: &Scenarios, name: &str, dir: &Path) -> i32 {
    match scenarios.world(name).and_then(|world| {
        std::fs::create_dir_all(dir).map_err(|e| format!("{} could not be made: {e}", dir.display()))?;
        world.write_files(dir)
    }) {
        Ok(()) => 0,
        Err(e) => fail(&e),
    }
}

#[cfg(unix)]
fn journey(scenarios: &Scenarios, id: &str, mode: Option<&str>, speed: f64, step: bool) -> i32 {
    use capdag_tuikit::scenario::play::{unmet, Player, Watching};
    let played = (|| -> Result<(Mode, PathBuf, Vec<String>), String> {
        let journey = scenarios.journey(id)?;
        let mode = match mode {
            Some(name) => Mode::named(name)?,
            None => *journey.modes.keys().next().ok_or_else(|| format!("the journey {id} has no mode"))?,
        };
        let exe = std::env::current_exe().map_err(|e| format!("this kit's own path could not be read: {e}"))?;
        let dir = std::env::temp_dir().join(format!("capdag-tuikit-{id}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).map_err(|e| format!("{} could not be made: {e}", dir.display()))?;
        let player = Player { exe, scenarios: scenarios.clone(), speed };
        let played = player.play(&journey, mode, &dir, Some(Watching { step }))?;
        let unmet = unmet(&journey.modes[&mode].expect, &played, &dir);
        Ok((mode, dir, unmet))
    })();
    match played {
        Ok((mode, dir, unmet)) => {
            eprintln!();
            if unmet.is_empty() {
                eprintln!("{id} in {} mode came to what it should; its files are in {}", mode.name(), dir.display());
                0
            } else {
                eprintln!("{id} in {} mode did not come to what it should; its files are in {}", mode.name(), dir.display());
                for line in unmet {
                    eprintln!("  {line}");
                }
                1
            }
        }
        Err(e) => fail(&e),
    }
}

/// Play journey `id` unwatched in `mode`, or in every mode it declares, as the
/// journey tests do, and say what each came to.
#[cfg(unix)]
fn check_journey(scenarios: &Scenarios, id: &str, mode: Option<&str>, speed: f64) -> i32 {
    use capdag_tuikit::scenario::play::{unmet, Player};
    let checked = (|| -> Result<bool, String> {
        let journey = scenarios.journey(id)?;
        let modes: Vec<Mode> = match mode {
            Some(name) => vec![Mode::named(name)?],
            None => journey.modes.keys().copied().collect(),
        };
        let exe = std::env::current_exe().map_err(|e| format!("this kit's own path could not be read: {e}"))?;
        let player = Player { exe, scenarios: scenarios.clone(), speed };
        let mut whole = true;
        for mode in modes {
            let leg = journey.modes.get(&mode).ok_or_else(|| format!("the journey {id} is not played in {} mode", mode.name()))?;
            let dir = std::env::temp_dir().join(format!("capdag-tuikit-{id}-{}-{}", mode.name(), std::process::id()));
            std::fs::create_dir_all(&dir).map_err(|e| format!("{} could not be made: {e}", dir.display()))?;
            let played = player.play(&journey, mode, &dir, None)?;
            let unmet = unmet(&leg.expect, &played, &dir);
            if unmet.is_empty() {
                println!("{id} in {} mode came to what it should", mode.name());
                std::fs::remove_dir_all(&dir).map_err(|e| format!("{} could not be removed: {e}", dir.display()))?;
            } else {
                whole = false;
                println!("{id} in {} mode did not come to what it should; its files are in {}", mode.name(), dir.display());
                for line in unmet {
                    println!("  {line}");
                }
                println!("--- exit ---\n{:?}\n--- stdout ---\n{}\n--- shown ---\n{}", played.code, played.stdout, played.shown);
            }
        }
        Ok(whole)
    })();
    match checked {
        Ok(whole) => i32::from(!whole),
        Err(e) => fail(&e),
    }
}

#[cfg(not(unix))]
fn journey(_scenarios: &Scenarios, _id: &str, _mode: Option<&str>, _speed: f64, _step: bool) -> i32 {
    fail("journeys are played on a pseudo-terminal, which this platform does not have; run `--world <W> -- <capdag args>` instead")
}

#[cfg(not(unix))]
fn check_journey(_scenarios: &Scenarios, _id: &str, _mode: Option<&str>, _speed: f64) -> i32 {
    fail("journeys are played on a pseudo-terminal, which this platform does not have; run `--world <W> -- <capdag args>` instead")
}

/// Snapshot the fabric at `anchor`: every cap of its manifest, every alias,
/// and the media types the command reads of it — every file type (what a
/// file's extension is told by) and live source, every type a cap's input or
/// output ends on (what planning reads), and every type a cap's arguments and
/// outputs or an alias name. A fabric publishes tens of thousands of media
/// types no cap reads; the snapshot leaves them out, and its manifest names
/// only what it holds.
async fn snapshot_fabric(out: &Path, anchor: Option<String>) -> i32 {
    let taken = async {
        let mut config = capdag::RegistryConfig::new();
        if let Some(anchor) = anchor {
            config = config.with_anchor(anchor);
        }
        let anchor = config.anchor.clone();
        let registry = capdag::FabricRegistry::with_config(config).await.map_err(|e| format!("{anchor}: {e}"))?;
        registry.prefetch_manifest_aliases().await;
        let caps = registry.manifest_caps().await.map_err(|e| e.to_string())?;
        let aliases = registry.cached_aliases().map_err(|e| e.to_string())?;
        let published: Vec<capdag::MediaUrn> =
            registry.manifest_media_urns().map_err(|e| e.to_string())?.into_iter().filter(|urn| !urn.is_top()).collect();
        let in_manifest: std::collections::BTreeSet<String> = published.iter().map(|urn| urn.to_string()).collect();
        let mut read: std::collections::BTreeSet<String> = published
            .iter()
            .filter(|urn| urn.get_tag("ext").is_some() || urn.is_live_feed())
            .map(|urn| urn.to_string())
            .collect();
        let mut named: Vec<String> = Vec::new();
        for alias in &aliases {
            match capdag::classify_alias_target(&alias.target) {
                Some(capdag::AliasTargetKind::Media) => named.push(alias.target.clone()),
                Some(capdag::AliasTargetKind::Cap) => {}
                None => return Err(format!("the alias '{}' names '{}', no cap or media URN", alias.name, alias.target)),
            }
        }
        for cap in caps.iter().filter(|cap| !cap.is_abstract) {
            for node in [cap.urn.in_media_urn(), cap.urn.out_media_urn()] {
                let node = node.map_err(|e| format!("{}: {e}", cap.urn))?;
                if let Some(shapes) = node.ends_on(&published) {
                    read.extend(shapes.iter().map(|shape| shape.to_string()));
                }
            }
            named.extend(cap.args.iter().map(|arg| arg.media_urn.clone()));
            named.extend(cap.outputs.iter().map(|output| output.media_urn.clone()));
        }
        for urn in &named {
            let urn = capdag::MediaUrn::from_string(urn).map_err(|e| format!("{urn}: {e}"))?.to_string();
            if in_manifest.contains(&urn) {
                read.insert(urn);
            }
        }
        let mut media = Vec::new();
        for urn in &read {
            media.push(registry.get_media_def(urn).await.map_err(|e| format!("{urn}: {e}"))?);
        }
        media.sort_by(|a, b| a.urn.cmp(&b.urn));
        let snapshot = FabricSnapshot { anchor, manifest_version: registry.manifest_version(), caps, media, aliases };
        let text = serde_json::to_string_pretty(&snapshot).map_err(|e| e.to_string())?;
        std::fs::write(out, text + "\n").map_err(|e| format!("{} could not be written: {e}", out.display()))?;
        Ok::<_, String>(snapshot)
    };
    match taken.await {
        Ok(snapshot) => {
            eprintln!(
                "{}: manifest {} — {} caps, {} media types, {} aliases",
                out.display(),
                snapshot.manifest_version,
                snapshot.caps.len(),
                snapshot.media.len(),
                snapshot.aliases.len()
            );
            0
        }
        Err(e) => fail(&format!("the fabric could not be snapshot: {e}")),
    }
}
