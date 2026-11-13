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
use crate::orchestrator::cli_cap::{
    render_cap_interface, synthesize_single_cap_notation, InterfaceNames, SINGLE_CAP_INPUT_NODE,
};
use crate::Cap;

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
            println!();
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
async fn media_named(registry: &crate::FabricRegistry, name: &str) -> CliResult<Option<crate::MediaUrn>> {
    let urn = if name.contains(':') {
        name.to_string()
    } else {
        match registry
            .resolve_alias_typed(name, Some(crate::fabric::alias::AliasTargetKind::Media))
            .await
        {
            Ok(urn) => urn,
            Err(e) => {
                super::ctx::nothing_by_that_name(e)?;
                return Ok(None);
            }
        }
    };
    let Ok(media) = crate::MediaUrn::from_string(&urn) else {
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

async fn show_cap(ctx: &Ctx, registry: &crate::FabricRegistry, cap: &Cap) -> CliResult<()> {
    let alias = names::cap(registry, cap);
    let bundled = super::cartridge::bundled_providers(&cap.urn).await?;
    let installed: Vec<String> = super::cartridge::installed()
        .await?
        .into_iter()
        .filter_map(|found| match found {
            crate::cartridge_discovery::DiscoveredCartridge::Directory { id, version, cap_groups, .. }
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
    print!("{text}");
    if let Some(description) = cap.cap_description.as_deref().filter(|d| !d.trim().is_empty()) {
        println!("\n{}", description.trim());
    }
    if let Some(model) = &cap.default_model_spec {
        println!("\nRuns a model: {model} by default (--model-spec chooses another)");
    }
    println!();
    match (bundled.first(), installed.first()) {
        (Some(provider), _) => println!("Provided by {} {}, bundled with capdag.", provider.id, provider.version),
        (None, Some(cartridge)) => println!("Provided by {cartridge}, installed."),
        (None, None) if ctx.offline => println!("Provided by a cartridge that is not installed; it is not looked up offline."),
        (None, None) => match super::cartridge::manager(ctx).await {
            Ok(manager) => {
                let suggestions = manager.suggestions_for_cap(&cap.urn.to_string()).await;
                match suggestions.first() {
                    Some(s) => println!(
                        "Provided by {}, which is downloaded the first time a run needs it (or: capdag cartridge install {}).",
                        s.cartridge_id, s.cartridge_id
                    ),
                    None => println!("No cartridge in the registry provides it."),
                }
            }
            Err(e) => println!("Its cartridge could not be looked up: {}", e.problem),
        },
    }
    let takes_input = synthesize_single_cap_notation(cap)
        .map(|notation| notation.contains(&format!("[{SINGLE_CAP_INPUT_NODE} -> ")))
        .unwrap_or(true);
    if takes_input {
        println!("\nExample:\n  capdag run {alias} <input> -o <dir>");
    } else {
        println!("\nExample:\n  capdag run {alias}");
    }
    Ok(())
}

async fn show_media(ctx: &Ctx, registry: &crate::FabricRegistry, media: &crate::MediaUrn) -> CliResult<()> {
    let def = registry.get_media_def(&media.to_string()).await.map_err(|e| {
        CliError::failed(format!("the media type {} could not be read", names::media(registry, media))).context(e.to_string())
    })?;
    let caps = registry
        .get_cached_caps()
        .await
        .map_err(|e| CliError::failed("the capabilities could not be read").context(e.to_string()))?;
    let takes: Vec<String> = caps
        .iter()
        .filter(|cap| !cap.aliases.is_empty())
        .filter(|cap| cap.urn.in_media_urn().is_ok_and(|input| media.conforms_to(&input).unwrap_or(false)))
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
    println!("{alias} — {}", def.title);
    println!("{}", ctx.term.style.dim(&def.urn));
    if !def.extensions.is_empty() {
        println!("Files: .{}", def.extensions.join(", ."));
    }
    if let Some(description) = def.description.as_deref().filter(|d| !d.trim().is_empty()) {
        println!("\n{}", description.trim());
    }
    if takes.is_empty() {
        println!("\nNo capability takes it.");
    } else {
        println!("\nTaken by {} capabilit{}: {}", takes.len(), if takes.len() == 1 { "y" } else { "ies" }, takes.join(", "));
        println!("\nWhat a file of it can become: capdag plan <file>");
    }
    Ok(())
}
