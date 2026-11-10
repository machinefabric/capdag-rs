//! `capdag cartridge list | install | remove`: the cartridges that provide
//! capabilities.
//!
//! A cartridge is bundled with capdag, installed from the cartridge registry
//! (on demand when a run needs it, or by `install`), or a dev cartridge
//! (`capdag dev install`). `list` says which are here and in what state.

use std::path::PathBuf;

use super::ctx::{self, Ctx};
use super::error::{CliError, CliResult, Exit};
use super::names;
use super::term::emit_record;
use crate::cartridge_discovery::{discover_cartridges, DiscoveredCartridge, DiscoveryIdentity};
use crate::orchestrator::executor::CartridgeManager;
use crate::CapUrn;

#[derive(Debug, clap::Subcommand)]
pub enum Command {
    /// The cartridges bundled with capdag and those installed
    #[command(after_help = "Examples:\n  capdag cartridge list\n  capdag cartridge list --json")]
    List,
    /// Download and verify a cartridge, without running anything
    #[command(after_help = "Examples:\n  \
        capdag cartridge install pdfcartridge     by its id\n  \
        capdag cartridge install disbind-pdf      the cartridge that provides a capability")]
    Install {
        /// A cartridge id, or a capability whose cartridge to install
        #[arg(value_name = "CARTRIDGE|CAP")]
        name: String,
    },
    /// Remove an installed cartridge; a run downloads it again when it needs it
    #[command(after_help = "Examples:\n  capdag cartridge remove pdfcartridge")]
    Remove {
        /// The cartridge's id, as `capdag cartridge list` shows it
        #[arg(value_name = "CARTRIDGE")]
        id: String,
    },
}

pub async fn cartridge(ctx: &Ctx, command: Command) -> CliResult<Exit> {
    match command {
        Command::List => list(ctx).await,
        Command::Install { name } => install(ctx, &name).await,
        Command::Remove { id } => remove(ctx, &id),
    }
}

/// Every cap the bundled cartridges declare.
pub async fn bundled_caps() -> CliResult<Vec<CapUrn>> {
    let Some(dir) = ctx::bundled_cartridges_dir() else {
        return Ok(Vec::new());
    };
    let found = crate::orchestrator::executor::discover_bundled_cartridges(
        &dir,
        ctx::BUILD_CHANNEL,
        ctx::BAKED_REGISTRY_URL,
        crate::FABRIC_MANIFEST_VERSION,
    )
    .await
    .map_err(|e| CliError::failed("the cartridges bundled with capdag could not be read").context(e.to_string()))?;
    Ok(found
        .into_iter()
        .flat_map(|(_, _, groups)| groups.into_iter().flat_map(|g| g.caps.into_iter().map(|c| c.urn)))
        .collect())
}

/// The bundled cartridges that declare a cap.
pub async fn bundled_providers(cap: &CapUrn) -> CliResult<Vec<crate::orchestrator::executor::BundledProvider>> {
    let Some(dir) = ctx::bundled_cartridges_dir() else {
        return Ok(Vec::new());
    };
    crate::orchestrator::executor::bundled_providers_of(
        &dir,
        ctx::BUILD_CHANNEL,
        ctx::BAKED_REGISTRY_URL,
        crate::FABRIC_MANIFEST_VERSION,
        cap,
    )
    .await
    .map_err(|e| CliError::failed("the cartridges bundled with capdag could not be read").context(e.to_string()))
}

/// The cartridges installed under the per-user root (registry and dev).
pub async fn installed() -> CliResult<Vec<DiscoveredCartridge>> {
    let identity = DiscoveryIdentity {
        channel: ctx::BUILD_CHANNEL,
        registry_url: ctx::BAKED_REGISTRY_URL.map(str::to_string),
        fabric_manifest_version: crate::FABRIC_MANIFEST_VERSION,
        cartridge_registry_version: crate::CARTRIDGE_REGISTRY_VERSION,
        signing_environment: crate::SIGNING_ENVIRONMENT.map(str::to_string),
        charter_trust: crate::bifaci::trust::charter_trust(),
        now_unix: mg_srel::unix_now(),
        // Nothing in the user's cartridge directory was put there by a build,
        // so a cartridge there that says it was bundled is refused for that.
        bundle: crate::bifaci::bundle_manifest::BundleProof::none(
            "the installed-cartridges directory is not a build's bundle, so nothing here \
             can vouch for a cartridge that says it was bundled",
        ),
    };
    discover_cartridges(&ctx::user_cartridge_dir()?, &identity)
        .await
        .map_err(|e| CliError::failed("the installed cartridges could not be read").context(e.to_string()))
}

/// A cartridge manager bound to this build's registry, its manifest synced
/// and verified.
pub async fn manager(ctx: &Ctx) -> CliResult<CartridgeManager> {
    if ctx.offline {
        return Err(CliError::usage("the cartridge registry is not read offline").hint("drop --offline"));
    }
    if ctx::BAKED_REGISTRY_URL.is_none() {
        return Err(CliError::failed("this capdag is a development build, bound to no cartridge registry")
            .context("it runs bundled and dev cartridges only")
            .hint("install a release of capdag, or `capdag dev install <project>` a cartridge you build"));
    }
    let mut manager = CartridgeManager::new(
        ctx::user_cartridge_dir()?,
        ctx::BAKED_REGISTRY_URL.map(str::to_string),
        ctx::BUILD_CHANNEL,
        crate::FABRIC_MANIFEST_VERSION,
        Vec::new(),
        crate::bifaci::trust::content_trust(),
        crate::RegistryConfig::default().anchor,
        crate::bifaci::trust::charter_trust(),
    );
    manager.init().await.map_err(|e| {
        CliError::failed("the cartridge registry could not be read")
            .context(e.to_string())
            .hint("check the connection; bundled cartridges run without one (--offline)")
    })?;
    Ok(manager)
}

async fn list(ctx: &Ctx) -> CliResult<Exit> {
    let mut rows: Vec<serde_json::Value> = Vec::new();
    if let Some(dir) = ctx::bundled_cartridges_dir() {
        let registry = ctx.registry().await?;
        let bundled = crate::orchestrator::executor::bundled_cartridge_adapter_groups(
            &dir,
            ctx::BUILD_CHANNEL,
            ctx::BAKED_REGISTRY_URL,
            crate::FABRIC_MANIFEST_VERSION,
            registry,
        )
        .await
        .map_err(|e| CliError::failed("the cartridges bundled with capdag could not be read").context(e.to_string()))?;
        for cartridge in bundled {
            rows.push(serde_json::json!({
                "type": "cartridge",
                "id": cartridge.id,
                "version": cartridge.version,
                "state": "bundled",
                "warnings": cartridge.refused.iter().map(|r| r.statement(&cartridge.id)).collect::<Vec<_>>(),
            }));
        }
    }
    for found in installed().await? {
        rows.push(match found {
            DiscoveredCartridge::Directory { id, version, registry_url, cap_groups, version_dir, .. } => serde_json::json!({
                "type": "cartridge",
                "id": id,
                "version": version,
                "state": if registry_url.is_some() { "installed" } else { "dev" },
                "caps": cap_groups.iter().map(|g| g.caps.len()).sum::<usize>(),
                "dir": version_dir.display().to_string(),
                "warnings": Vec::<String>::new(),
            }),
            DiscoveredCartridge::Incompatible { id, version, error, version_dir, .. } => serde_json::json!({
                "type": "cartridge",
                "id": id,
                "version": version,
                "state": "refused",
                "dir": version_dir.display().to_string(),
                "warnings": [error.message],
            }),
        });
    }
    if ctx.term.json {
        for row in &rows {
            emit_record(row);
        }
        return Ok(Exit::Ok);
    }
    if rows.is_empty() {
        println!("No cartridges are bundled or installed. A run installs the ones it needs.");
        return Ok(Exit::Ok);
    }
    let style = &ctx.term.style;
    for row in &rows {
        let state = row["state"].as_str().unwrap_or("");
        println!(
            "{:<22} {:<12} {}",
            style.name(row["id"].as_str().unwrap_or("")),
            row["version"].as_str().unwrap_or(""),
            match state {
                "refused" => style.bad("refused"),
                other => other.to_string(),
            }
        );
        for warning in row["warnings"].as_array().into_iter().flatten() {
            println!("  {} {}", style.warning("warning:"), warning.as_str().unwrap_or(""));
        }
    }
    Ok(Exit::Ok)
}

async fn install(ctx: &Ctx, name: &str) -> CliResult<Exit> {
    let manager = manager(ctx).await?;
    // A name with ':' is a cap URN; a bare name is a cartridge id when the
    // registry knows one by it, else a cap alias.
    let cartridge_id = if !name.contains(':') && manager.registry_cartridge(name).await.is_some() {
        name.to_string()
    } else {
        let registry = ctx.registry().await?;
        let cap = registry.get_cap(name).await.map_err(|_| {
            CliError::usage(format!("there is no cartridge or capability named '{name}'"))
                .hint("see the cartridges with: capdag cartridge list; find a capability with: capdag caps")
        })?;
        if let Some(provider) = bundled_providers(&cap.urn).await?.first() {
            let alias = names::cap(&registry, &cap);
            if ctx.term.json {
                emit_record(&serde_json::json!({
                    "type": "cartridge", "id": provider.id, "version": provider.version, "state": "bundled",
                    "path": provider.entry_point.display().to_string(),
                }));
            } else {
                eprintln!("{alias} is provided by {} {}, bundled with capdag: nothing to install", provider.id, provider.version);
                // The cartridge's path on stdout, as an install prints the one it installed.
                println!("{}", provider.entry_point.display());
            }
            return Ok(Exit::Ok);
        }
        let suggestions = manager.suggestions_for_cap(&cap.urn.to_string()).await;
        let first = suggestions.first().ok_or_else(|| {
            CliError::failed(format!("no cartridge in the registry provides {}", names::cap(&registry, &cap)))
        })?;
        first.cartridge_id.clone()
    };
    if !ctx.term.json {
        eprintln!("Installing {cartridge_id}…");
    }
    let path = manager.get_cartridge_path(&cartridge_id).await.map_err(|e| {
        CliError::failed(format!("{cartridge_id} could not be installed")).context(e.to_string())
    })?;
    if ctx.term.json {
        emit_record(&serde_json::json!({
            "type": "cartridge", "id": cartridge_id, "state": "installed", "path": path.display().to_string(),
        }));
    } else {
        eprintln!("Installed and verified {cartridge_id}");
        println!("{}", path.display());
    }
    Ok(Exit::Ok)
}

fn remove(ctx: &Ctx, id: &str) -> CliResult<Exit> {
    let root = ctx::user_cartridge_dir()?;
    // `{slug}/v{n}/{channel}/{name}/`: every version of the cartridge, in every
    // registry and the dev slot.
    let removed = cartridge_dirs(&root, id)?;
    for dir in &removed {
        std::fs::remove_dir_all(dir)
            .map_err(|e| CliError::failed(format!("{} could not be removed", dir.display())).context(e.to_string()))?;
    }
    if removed.is_empty() {
        let mut error = CliError::usage(format!("no installed cartridge is called '{id}'"));
        if let Some(bundle) = ctx::bundled_cartridges_dir() {
            if !cartridge_dirs(&bundle, id)?.is_empty() {
                error = error.context(format!("{id} is bundled with capdag, and a bundled cartridge cannot be removed"));
            }
        }
        return Err(error.hint("see the cartridges with: capdag cartridge list"));
    }
    for dir in &removed {
        if ctx.term.json {
            emit_record(&serde_json::json!({ "type": "removed", "id": id, "dir": dir.display().to_string() }));
        } else {
            eprintln!("Removed {}", dir.display());
        }
    }
    Ok(Exit::Ok)
}

/// The directories of cartridge `id` under a cartridge root:
/// `{slug}/v{n}/{channel}/{id}/`.
fn cartridge_dirs(root: &std::path::Path, id: &str) -> CliResult<Vec<PathBuf>> {
    let mut found = Vec::new();
    for slug in subdirs(root)? {
        for version_level in subdirs(&slug)? {
            for channel in subdirs(&version_level)? {
                let dir = channel.join(id);
                if dir.is_dir() {
                    found.push(dir);
                }
            }
        }
    }
    Ok(found)
}

fn subdirs(dir: &std::path::Path) -> CliResult<Vec<PathBuf>> {
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let entries = std::fs::read_dir(dir)
        .map_err(|e| CliError::failed(format!("{} could not be read", dir.display())).context(e.to_string()))?;
    Ok(entries.filter_map(Result::ok).map(|e| e.path()).filter(|p| p.is_dir()).collect())
}
