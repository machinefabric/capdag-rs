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
use capdag::orchestrator::cli_cap::{
    render_cap_interface, synthesize_single_cap_notation, InterfaceNames, SINGLE_CAP_INPUT_NODE,
};
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

pub async fn info(ctx: &Ctx, args: Args) -> CliResult<Exit> {
    let registry = if args.no_cache { ctx.fresh_registry().await? } else { ctx.registry().await? };
    for (index, name) in args.names.iter().enumerate() {
        if index > 0 && !ctx.term.json {
            outln!();
        }
        // A cap, a media type, or neither.
        match registry.get_cap(name).await {
            Ok(cap) => show_cap(ctx, &registry, &cap).await?,
            Err(e) => {
                super::ctx::nothing_by_that_name(e)?;
                match media_named(&registry, name).await? {
                    Some(media) => show_media(ctx, &registry, &media).await?,
                    None => return Err(super::run::unknown_cap(&registry, name, "capability or media type").await),
                }
            }
        }
    }
    Ok(Exit::Ok)
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
        outln!("\nRuns a model: {model} by default (--model-spec chooses another)");
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
    let takes_input = synthesize_single_cap_notation(cap)
        .map(|notation| notation.contains(&format!("[{SINGLE_CAP_INPUT_NODE} -> ")))
        .unwrap_or(true);
    if takes_input {
        outln!("\nExample:\n  capdag run {alias} <input> -o <dir>");
    } else {
        outln!("\nExample:\n  capdag run {alias}");
    }
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
