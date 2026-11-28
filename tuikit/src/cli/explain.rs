//! `capdag run <machine> --explain`: the machine shown — its inputs, its
//! steps in alias form, its outputs and the options it leaves to the run —
//! or what is wrong with it and where. The commands that show a machine
//! (`graph`, `view`, `edit`) name and resolve it the same way.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::ctx::Ctx;
use super::error::{CliError, CliResult, Exit};
use super::formal::{self as f, index, nat};
use super::inspect::Inspection;
use super::line::{Extras, MachineEntry, MachineFacts, PlanningFlags, Resolved, RunState, Tables, World};
use super::names;
use super::term::{emit_record, outln};
use capdag::fabric::registry::FabricRegistry;
use capdag::orchestrator::machine_plan::ResolvedNotation;

/// A machine named on a line that shows it: a file, or written inline when
/// it starts with `[`.
pub fn machine_entry(tables: &mut Tables, given: &str) -> f::Written {
    let inline = given.starts_with('[');
    let entry = if inline { MachineEntry::Inline(given.to_string()) } else { MachineEntry::File(PathBuf::from(given)) };
    f::Written { base: nat(tables.add_machine(entry)), inline }
}

/// A world of one machine named for showing, for `graph`, `view` and
/// `edit`.
pub async fn showing_world<'a>(ctx: &'a Ctx, given: &str) -> CliResult<(World<'a>, f::Written)> {
    let registry = ctx.registry().await?;
    let mut tables = Tables::default();
    let m = machine_entry(&mut tables, given);
    let inspection = Inspection::new(ctx, super::run::inspection_runtime(ctx, &registry)?, registry.clone());
    let world = World::new(ctx, registry, inspection, tables, Extras::default(), PlanningFlags::default());
    Ok((world, m))
}

/// The verdict of a command that shows a machine, said: the machine is not
/// there, or does not resolve.
pub fn showing_error(world: &World<'_>, m: &f::Written, verdict: &f::CliVerdict) -> CliError {
    let base = index(&m.base);
    match verdict {
        f::CliVerdict::Is { v: f::Verdict::Unresolved } => match world.machine(base) {
            Some(MachineFacts::Unresolved(error)) => super::say::with_exit(error.clone(), verdict),
            _ => super::say::with_exit(CliError::failed("the machine does not resolve"), verdict),
        },
        f::CliVerdict::Refused { r: f::CliRefusal::Line { r: f::Refusal::NoSuchMachine } } => super::say::with_exit(
            CliError::usage(format!(
                "{}: {}",
                world.tables.machines[base].shown(),
                f::refusal::says(f::Refusal::NoSuchMachine)
            ))
            .hint(NO_SUCH_MACHINE_HINT),
            verdict,
        ),
        other => super::ends(other)
            .unwrap_or_else(|| CliError::failed("a machine was shown, and said to have failed").hint("report it")),
    }
}

/// What a machine file that is not there is answered with: the line that
/// writes one.
pub const NO_SUCH_MACHINE_HINT: &str = "write one: capdag run <files> --to <type> --dry-run > <file>.machine";

/// `capdag run <machine> --explain`: the line is the machine alone, or it is
/// refused; the machine is shown when it resolves, and what is wrong with it
/// is said when it does not.
pub async fn explain(ctx: &Ctx, world: &mut World<'_>, state: &RunState, planning_flags: bool) -> CliResult<Exit> {
    let line = state.line.clone();
    let cli_world = ctx.cli_world(false);
    let globals = ctx.globals();
    let verdict = world
        .settle(&[], |w| {
            f::cli(
                cli_world.clone(),
                f::Invocation::Command {
                    g: globals.clone(),
                    c: f::Command::Run { l: line.clone(), planning_flags, mode: f::RunMode::Explain, w },
                },
            )
        })
        .await?;
    match (&verdict, &state.line.machine) {
        (f::CliVerdict::Is { v: f::Verdict::Answers }, Some(m)) => {
            let base = index(&m.base);
            let resolved = world.machine(base).and_then(MachineFacts::resolved).expect("a machine shown resolves");
            show(ctx, &world.registry, &world.tables.machines[base], resolved)?;
            Ok(Exit::Ok)
        }
        (f::CliVerdict::Is { v: f::Verdict::Unresolved }, Some(m))
        | (f::CliVerdict::Refused { r: f::CliRefusal::Line { r: f::Refusal::NoSuchMachine } }, Some(m)) => {
            Err(showing_error(world, m, &verdict))
        }
        _ => Err(super::say::line_verdict_error(ctx, world, state, &[], &verdict).await),
    }
}

/// Why a machine does not resolve: a name the fabric does not define (which a
/// dev cartridge may), or anything else.
pub enum Unresolved {
    UndefinedAlias { alias: String, error: CliError },
    Other(CliError),
}

/// Resolve a machine's notation against the fabric, or say what is wrong and
/// where. Machines it includes are read relative to its file, or to the
/// working directory for one written inline.
pub async fn resolve_notation(
    registry: &Arc<FabricRegistry>,
    path: Option<&Path>,
    notation: &str,
) -> Result<ResolvedNotation, Unresolved> {
    let shown = path.map(|p| p.display().to_string()).unwrap_or_else(|| "the machine".to_string());
    let working = std::env::current_dir().map_err(|e| {
        Unresolved::Other(CliError::failed("the working directory could not be read").context(e.to_string()))
    })?;
    let id = match path {
        Some(path) => Some(capdag::FileMachineSource::id_of(path).map_err(|e| {
            Unresolved::Other(CliError::failed(format!("the machine file '{shown}' could not be read")).context(e.to_string()))
        })?),
        None => None,
    };
    let machines = capdag::FileMachineSource::new(working);
    // Parsed first on its own, so a mistake in it is told by what it is — a
    // name that is no capability, a statement that does not parse — not by
    // the message it would be flattened into.
    if let Err(error) = capdag::machine::parse_machine_with_sources_async(notation, registry, &machines, id.as_deref()).await
    {
        return Err(not_a_machine(registry, &shown, &error));
    }
    ResolvedNotation::resolve_from(notation, registry.clone(), &machines, id.as_deref()).await.map_err(|e| {
        Unresolved::Other(CliError::failed(format!("{shown} is not a machine that can run")).context(e.to_string()))
    })
}

/// A machine that does not parse: what is wrong, and — for a name that is no
/// capability — the capability it most likely meant.
fn not_a_machine(registry: &FabricRegistry, shown: &str, error: &capdag::machine::MachineParseError) -> Unresolved {
    use capdag::machine::{MachineParseError, MachineSyntaxError};
    let failed = CliError::failed(format!("{shown} is not a machine that can run")).context(error.to_string());
    match error {
        MachineParseError::Syntax(MachineSyntaxError::UndefinedAlias { alias }) => {
            let aliases: Vec<String> = registry.cached_cap_aliases().into_iter().map(|(alias, _)| alias).collect();
            let error = match super::nearest(alias, aliases.iter().map(String::as_str)) {
                Some(near) => failed.hint(format!("did you mean `{near}`? Find capabilities with: capdag caps {alias}")),
                None => failed.hint(format!("find the capability you meant with: capdag caps {alias}")),
            };
            Unresolved::UndefinedAlias { alias: alias.clone(), error }
        }
        _ => Unresolved::Other(failed.hint(
            "`capdag run <machine> --explain` on a machine that works shows the form; the notation is in the CapDAG docs, §Machine notation",
        )),
    }
}

/// A machine, shown: its inputs, its steps in alias form, its outputs, and
/// the options it leaves to the run, as `--arg` gives them.
pub fn show(ctx: &Ctx, registry: &Arc<FabricRegistry>, entry: &MachineEntry, resolved: &Resolved) -> CliResult<()> {
    let shown = entry.shown();
    let aliased = names::machine(registry, resolved.resolution.machine())
        .map_err(|e| CliError::failed(format!("{shown} could not be written in alias form")).context(e))?;
    let option_names: Vec<String> = (0..resolved.options.len()).map(|j| resolved.flag_name(j)).collect();
    if ctx.term.json {
        emit_record(&serde_json::json!({
            "type": "machine",
            "file": match entry { MachineEntry::File(path) => Some(path.display().to_string()), MachineEntry::Inline(_) => None },
            "notation": aliased,
            "canonical": resolved.resolution.machine().to_machine_notation().ok(),
            "strands": resolved.resolution.strand_count(),
            "steps": resolved.resolution.machine().strands().iter().map(|strand| strand.edges().len()).sum::<usize>(),
            "inputs": resolved.inputs.iter().map(|i| serde_json::json!({
                "name": i.name, "media": names::media(registry, &i.media_urn), "urn": i.media_urn.to_string(), "strand": i.strand,
            })).collect::<Vec<_>>(),
            "outputs": resolved.outputs.iter().map(|o| serde_json::json!({
                "name": o.name, "media": names::media(registry, &o.media_urn), "urn": o.media_urn.to_string(), "strand": o.strand,
            })).collect::<Vec<_>>(),
            "options": resolved.options.iter().zip(&option_names).map(|(o, flag)| serde_json::json!({
                "arg": flag, "step": o.step, "name": o.name, "media": names::media_urn(registry, &o.media_urn),
                "urn": o.media_urn, "values": o.allowed,
            })).collect::<Vec<_>>(),
        }));
        return Ok(());
    }
    let style = &ctx.term.style;
    outln!("{} {}", style.good("✓"), style.name(&shown));
    let anchor = |a: &capdag::orchestrator::machine_plan::MachineAnchor| {
        let mut line = format!("{} ({})", a.name, names::media(registry, &a.media_urn));
        if ctx.term.verbosity >= 1 {
            line.push_str(&format!("  {}", style.dim(&a.media_urn.to_string())));
        }
        line
    };
    let listed = |anchors: &[capdag::orchestrator::machine_plan::MachineAnchor]| {
        if anchors.is_empty() {
            "none".to_string()
        } else {
            anchors.iter().map(anchor).collect::<Vec<_>>().join(", ")
        }
    };
    outln!("  inputs:  {}", listed(&resolved.inputs));
    outln!("  outputs: {}", listed(&resolved.outputs));
    if !resolved.options.is_empty() {
        outln!("  left to the run:");
        for (option, flag) in resolved.options.iter().zip(&option_names) {
            let values = match &option.allowed {
                Some(values) => format!("  one of {}", values.join(", ")),
                None => String::new(),
            };
            outln!("    --arg {flag}=<{}>{values}", names::media_urn(registry, &option.media_urn));
        }
    }
    outln!();
    for line in aliased.lines() {
        outln!("  {line}");
    }
    if ctx.term.verbosity >= 1 {
        if let Ok(canonical) = resolved.resolution.machine().to_machine_notation() {
            outln!("\n  canonical: {}", style.dim(&canonical));
        }
    }
    let example: String = resolved.inputs.iter().map(|i| format!(" <{}>", i.name)).collect();
    outln!("\nRun it: capdag run {}{example}", super::shell_word(&entry.argument()));
    Ok(())
}

/// The state of a line that gives nothing but its machine: what a command
/// that shows a machine says its verdict with.
pub fn showing_state(m: &f::Written) -> super::line::RunState {
    super::line::RunState {
        line: f::RunLine { machine: Some(m.clone()), ..f::no_run_line() },
        texts: Vec::new(),
        arg_values: Vec::new(),
        unknown_inputs: Vec::new(),
        unknown_options: Vec::new(),
    }
}
