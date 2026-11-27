//! `capdag info <name>…`: a capability — what it takes, gives and needs,
//! which cartridge provides it and whether that is here — or a media type.
//!
//! `--json` prints each capability's definition as the fabric publishes it,
//! one record per line: what cartridges' builds read to snapshot the caps they
//! implement (`--no-cache` reads the fabric afresh).

use super::ctx::Ctx;
use super::error::{CliError, CliResult, Exit};
use super::names;
use super::term::emit_record;
use super::formal as f;
use capdag::orchestrator::cli_cap::{render_cap_interface, InterfaceNames};
use capdag::Cap;
use super::term::{out, outln};

#[derive(Debug, clap::Args)]
pub struct Args {
    /// Capabilities or media types, by alias or URN
    #[arg(value_name = "NAME", required = true)]
    pub names: Vec<String>,
    /// Read the fabric afresh rather than from the local cache
    #[arg(long)]
    pub no_cache: bool,
}

/// What a name an `info` line gives is.
enum Named {
    Cap(Cap),
    Media(capdag::MediaUrn),
    Nothing,
}

pub async fn info(ctx: &Ctx, args: Args) -> CliResult<Exit> {
    let registry = if args.no_cache { ctx.fresh_registry().await? } else { ctx.registry().await? };
    let mut named = Vec::with_capacity(args.names.len());
    for name in &args.names {
        // A cap, a media type, or neither.
        named.push(match registry.get_cap(name).await {
            Ok(cap) => Named::Cap(cap),
            Err(e) => {
                super::ctx::nothing_by_that_name(e)?;
                match media_named(&registry, name).await? {
                    Some(media) => Named::Media(media),
                    None => Named::Nothing,
                }
            }
        });
    }
    let all_known = !named.iter().any(|n| matches!(n, Named::Nothing));
    let verdict = ctx.decide(f::Command::Info { all_known, no_cache: args.no_cache }, false);
    if let f::CliVerdict::Refused { r: f::CliRefusal::NoSuchName } = verdict {
        let name = args
            .names
            .iter()
            .zip(&named)
            .find(|(_, n)| matches!(n, Named::Nothing))
            .map(|(name, _)| name.clone())
            .expect("a name nothing has was found above");
        return Err(super::say::with_exit(unknown_name(&registry, &name).await, &verdict));
    }
    if let Some(error) = super::ends(&verdict) {
        return Err(error);
    }
    for (index, found) in named.iter().enumerate() {
        if index > 0 && !ctx.term.json {
            outln!();
        }
        match found {
            Named::Cap(cap) => show_cap(ctx, &registry, cap).await?,
            Named::Media(media) => show_media(ctx, &registry, media).await?,
            Named::Nothing => unreachable!("the model refuses a name nothing has"),
        }
    }
    Ok(Exit::Ok)
}

/// No capability or media type has this name: say which capabilities are
/// near it.
async fn unknown_name(registry: &capdag::FabricRegistry, token: &str) -> CliError {
    let mut error = CliError::usage(format!("'{token}': {}", f::says(&f::CliRefusal::NoSuchName)));
    let aliases: Vec<String> = match registry.manifest_caps().await {
        Ok(caps) => caps.iter().flat_map(|c| c.aliases.clone()).collect(),
        Err(e) => {
            return error
                .context(format!("names near it cannot be offered: {e}"))
                .hint(format!("find capabilities with: capdag caps {token}"))
        }
    };
    let mut near: Vec<(f64, &String)> = aliases
        .iter()
        .map(|a| (strsim::jaro_winkler(token, a), a))
        .filter(|(score, _)| *score > 0.75)
        .collect();
    near.sort_by(|a, b| b.0.total_cmp(&a.0));
    if !near.is_empty() {
        let names: Vec<&str> = near.iter().take(3).map(|(_, a)| a.as_str()).collect();
        error = error.context(format!("did you mean {}?", names.join(", ")));
    }
    error.hint(format!("find capabilities with: capdag caps {token}"))
}

/// The media type `name` names, `None` when the registry has none by that
/// name, and the registry's failure when it could not be asked.
async fn media_named(registry: &capdag::FabricRegistry, name: &str) -> CliResult<Option<capdag::MediaUrn>> {
    let urn = if name.contains(':') {
        name.to_string()
    } else {
        match registry
            .resolve_alias_typed(name, Some(capdag::fabric::alias::AliasTargetKind::Media))
            .await
        {
            Ok(urn) => urn,
            Err(e) => {
                super::ctx::nothing_by_that_name(e)?;
                return Ok(None);
            }
        }
    };
    let Ok(media) = capdag::MediaUrn::from_string(&urn) else {
        return Ok(None);
    };
    match registry.get_media_def(&media.to_string()).await {
        Ok(_) => Ok(Some(media)),
        Err(e) => {
            super::ctx::nothing_by_that_name(e)?;
            Ok(None)
        }
    }
}

async fn show_cap(ctx: &Ctx, registry: &capdag::FabricRegistry, cap: &Cap) -> CliResult<()> {
    let alias = names::cap(registry, cap);
    let bundled = super::cartridge::bundled_providers(ctx, &cap.urn).await?;
    let installed: Vec<String> = super::cartridge::installed(ctx)
        .await?
        .into_iter()
        .filter_map(|found| match found {
            capdag::cartridge_discovery::DiscoveredCartridge::Directory { id, version, cap_groups, .. }
                if cap_groups.iter().any(|g| g.caps.iter().any(|c| c.urn.is_equivalent(&cap.urn))) =>
            {
                Some(format!("{id} {version}"))
            }
            _ => None,
        })
        .collect();
    if ctx.term.json {
        // The definition is written as the fabric publishes it, its fields in
        // their declared order: cartridges' builds embed it as it is.
        let definition = serde_json::to_string(cap)
            .map_err(|e| CliError::failed(format!("{alias} could not be written as JSON")).context(e.to_string()))?;
        let head = serde_json::json!({
            "type": "cap",
            "alias": alias,
            "urn": cap.urn.to_string(),
            "bundled": bundled.iter().map(|p| format!("{} {}", p.id, p.version)).collect::<Vec<_>>(),
            "installed": installed,
        })
        .to_string();
        let record = format!("{},\"definition\":{definition}}}", head.trim_end_matches('}'));
        super::term::emit_line(&record);
        return Ok(());
    }
    let media = |urn: &str| names::media_urn(registry, urn);
    let interface = InterfaceNames { cap: alias.clone(), media: &media, urns: ctx.term.verbosity >= 1 };
    let text = render_cap_interface(cap, &interface)
        .map_err(|e| CliError::failed(format!("{alias} could not be shown")).context(e))?;
    out!("{text}");
    if let Some(description) = cap.cap_description.as_deref().filter(|d| !d.trim().is_empty()) {
        outln!("\n{}", description.trim());
    }
    let default_model = cap
        .default_model_spec()
        .map_err(|e| CliError::failed(format!("{alias} could not be shown")).context(e.to_string()))?;
    if let Some(model) = default_model {
        let option = cap
            .model_spec_arg()
            .map_err(|e| CliError::failed(format!("{alias} could not be shown")).context(e.to_string()))?
            .map(|arg| arg.name.clone())
            .unwrap_or_default();
        outln!("\nRuns a model: {model} by default (--arg {option}=<spec> chooses another)");
    }
    outln!();
    match (bundled.first(), installed.first()) {
        (Some(provider), _) => outln!("Provided by {} {}, bundled with capdag.", provider.id, provider.version),
        (None, Some(cartridge)) => outln!("Provided by {cartridge}, installed."),
        (None, None) if ctx.offline => outln!("Provided by a cartridge that is not installed; it is not looked up offline."),
        (None, None) => match super::cartridge::manager(ctx).await {
            Ok(manager) => {
                let providers = manager.providers_of(&cap.urn.to_string()).await;
                match providers.first() {
                    Some(id) => outln!(
                        "Provided by {id}, which is downloaded the first time a run needs it (or: capdag cartridge install {id})."
                    ),
                    None => outln!("No cartridge in the registry provides it."),
                }
            }
            Err(e) => outln!("Its cartridge could not be looked up: {}", e.problem),
        },
    }
    let machine = names::one_step_machine(registry, cap)
        .map_err(|e| CliError::failed(format!("the machine of {alias}'s one step could not be written")).context(e))?;
    let input = if machine.starts_with("[()") { "" } else { " <input>" };
    outln!("\nRun it as a machine of one step:\n  capdag run {}{input} -o <dir>", super::shell_word(&machine));
    Ok(())
}

async fn show_media(ctx: &Ctx, registry: &capdag::FabricRegistry, media: &capdag::MediaUrn) -> CliResult<()> {
    let def = registry.get_media_def(&media.to_string()).await.map_err(|e| {
        CliError::failed(format!("the media type {} could not be read", names::media(registry, media))).context(e.to_string())
    })?;
    let caps = ctx.fabric_caps(registry).await?;
    let takes: Vec<String> = caps
        .iter()
        .filter(|cap| !cap.aliases.is_empty())
        .filter(|cap| cap.urn.in_media_urn().is_ok_and(|input| media.conforms_to(&input)))
        .map(|cap| names::cap(registry, cap))
        .collect();
    let alias = names::media(registry, media);
    if ctx.term.json {
        emit_record(&serde_json::json!({
            "type": "media",
            "alias": alias,
            "title": def.title,
            "urn": def.urn,
            "extensions": def.extensions,
            "taken_by": takes,
        }));
        return Ok(());
    }
    outln!("{alias} — {}", def.title);
    outln!("{}", ctx.term.style.dim(&def.urn));
    if !def.extensions.is_empty() {
        outln!("Files: .{}", def.extensions.join(", ."));
    }
    if let Some(description) = def.description.as_deref().filter(|d| !d.trim().is_empty()) {
        outln!("\n{}", description.trim());
    }
    if takes.is_empty() {
        outln!("\nNo capability takes it.");
    } else {
        outln!("\nTaken by {} capabilit{}: {}", takes.len(), if takes.len() == 1 { "y" } else { "ies" }, takes.join(", "));
        outln!("\nWhat a file of it can become: capdag plan <file>");
    }
    Ok(())
}
