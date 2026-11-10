//! `capdag check <machine>`: is the machine one that can run, and what is it.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::ctx::Ctx;
use super::error::{CliError, CliResult, Exit};
use super::names;
use super::term::emit_record;
use crate::fabric::registry::FabricRegistry;
use crate::orchestrator::machine_plan::ResolvedNotation;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// The machine file
    #[arg(value_name = "MACHINE")]
    pub machine: PathBuf,
}

pub async fn check(ctx: &Ctx, args: Args) -> CliResult<Exit> {
    let notation = read_machine(&args.machine)?;
    let registry = ctx.registry().await?;
    show(ctx, &registry, &args.machine, &notation).await
}

/// Read a machine file, or say why not.
pub fn read_machine(path: &Path) -> CliResult<String> {
    if !path.is_file() {
        return Err(CliError::usage(format!("the machine file '{}' does not exist", path.display()))
            .hint("`capdag plan <files> --to <type> --save <file>` writes one"));
    }
    std::fs::read_to_string(path).map_err(|e| {
        CliError::failed(format!("the machine file '{}' could not be read", path.display())).context(e.to_string())
    })
}

/// Resolve a machine file's notation against the fabric, or say what is wrong
/// and where. Machines it includes are read relative to it.
pub async fn resolve(registry: &Arc<FabricRegistry>, path: &Path, notation: &str) -> CliResult<ResolvedNotation> {
    let working = std::env::current_dir()
        .map_err(|e| CliError::failed("the working directory could not be read").context(e.to_string()))?;
    let id = crate::FileMachineSource::id_of(path).map_err(|e| {
        CliError::failed(format!("the machine file '{}' could not be read", path.display())).context(e.to_string())
    })?;
    let machines = crate::FileMachineSource::new(working);
    // Parsed first on its own, so a mistake in it is told by what it is — a
    // name that is no capability, a statement that does not parse — not by
    // the message it would be flattened into.
    if let Err(error) =
        crate::machine::parse_machine_with_sources_async(notation, registry, &machines, Some(&id)).await
    {
        return Err(not_a_machine(registry, path, &error));
    }
    ResolvedNotation::resolve_from(notation, registry.clone(), &machines, Some(&id))
        .await
        .map_err(|e| {
            CliError::failed(format!("{} is not a machine that can run", path.display())).context(e.to_string())
        })
}

/// A machine that does not parse: what is wrong, and — for a name that is no
/// capability — the capability it most likely meant.
fn not_a_machine(registry: &FabricRegistry, path: &Path, error: &crate::machine::MachineParseError) -> CliError {
    use crate::machine::{MachineParseError, MachineSyntaxError};
    let failed = CliError::failed(format!("{} is not a machine that can run", path.display())).context(error.to_string());
    match error {
        MachineParseError::Syntax(MachineSyntaxError::UndefinedAlias { alias }) => {
            let aliases: Vec<String> = registry.cached_cap_aliases().into_iter().map(|(alias, _)| alias).collect();
            match super::nearest(alias, aliases.iter().map(String::as_str)) {
                Some(near) => failed.hint(format!("did you mean `{near}`? Find capabilities with: capdag caps {alias}")),
                None => failed.hint(format!("find the capability you meant with: capdag caps {alias}")),
            }
        }
        _ => failed.hint("`capdag check` on a machine that works shows the form; the notation is in the CapDAG docs, §Machine notation"),
    }
}

/// A machine, shown: its inputs, its steps in alias form, its outputs.
pub async fn show(ctx: &Ctx, registry: &Arc<FabricRegistry>, path: &Path, notation: &str) -> CliResult<Exit> {
    let resolved = resolve(registry, path, notation).await?;
    let failed = |e: crate::orchestrator::ParseOrchestrationError| {
        CliError::failed(format!("{} is not a machine that can run", path.display())).context(e.to_string())
    };
    let inputs = resolved.inputs().map_err(failed)?;
    let outputs = resolved.outputs().map_err(failed)?;
    let aliased = names::machine(registry, resolved.machine())
        .map_err(|e| CliError::failed(format!("{} could not be written in alias form", path.display())).context(e))?;
    if ctx.term.json {
        emit_record(&serde_json::json!({
            "type": "machine",
            "file": path.display().to_string(),
            "notation": aliased,
            "canonical": resolved.machine().to_machine_notation().ok(),
            "strands": resolved.strand_count(),
            "inputs": inputs.iter().map(|i| serde_json::json!({
                "name": i.name, "media": names::media(registry, &i.media_urn), "urn": i.media_urn.to_string(), "strand": i.strand,
            })).collect::<Vec<_>>(),
            "outputs": outputs.iter().map(|o| serde_json::json!({
                "name": o.name, "media": names::media(registry, &o.media_urn), "urn": o.media_urn.to_string(), "strand": o.strand,
            })).collect::<Vec<_>>(),
        }));
        return Ok(Exit::Ok);
    }
    let style = &ctx.term.style;
    println!("{} {}", style.good("✓"), style.name(&path.display().to_string()));
    let anchor = |a: &crate::orchestrator::machine_plan::MachineAnchor| {
        let mut line = format!("{} ({})", a.name, names::media(registry, &a.media_urn));
        if ctx.term.verbosity >= 1 {
            line.push_str(&format!("  {}", style.dim(&a.media_urn.to_string())));
        }
        line
    };
    println!("  inputs:  {}", if inputs.is_empty() { "none".into() } else { inputs.iter().map(anchor).collect::<Vec<_>>().join(", ") });
    println!("  outputs: {}", if outputs.is_empty() { "none".into() } else { outputs.iter().map(anchor).collect::<Vec<_>>().join(", ") });
    println!();
    for line in aliased.lines() {
        println!("  {line}");
    }
    if ctx.term.verbosity >= 1 {
        if let Ok(canonical) = resolved.machine().to_machine_notation() {
            println!("\n  canonical: {}", style.dim(&canonical));
        }
    }
    let example: Vec<String> = inputs.iter().map(|i| format!("<{}>", i.name)).collect();
    println!("\nRun it: capdag run {} {}", path.display(), example.join(" "));
    Ok(Exit::Ok)
}
