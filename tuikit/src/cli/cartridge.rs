//! `capdag cartridge list | install | remove`: the cartridges that provide
//! capabilities.
//!
//! A cartridge is bundled with capdag, installed from the cartridge registry
//! (on demand when a run needs it, or by `install`), or a dev cartridge
//! (`capdag dev install`). `list` says which are here and in what state.

use super::ask::{self, Answered, Choice, Question};
use super::ctx::{self, Ctx};
use super::error::{CliError, CliResult, Exit};
use super::formal as f;
use super::host::CartridgeStore;
use super::names;
use super::term::{emit_record, outln};
use capdag::cartridge_discovery::DiscoveredCartridge;
use capdag::CapUrn;

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

/// The command as the model knows it, before its own facts are known.
pub fn placeholder(command: &Command) -> f::Command {
    match command {
        Command::List => f::Command::CartridgeList,
        Command::Install { .. } => f::Command::CartridgeInstall { target: f::InstallTarget::Unknown },
        Command::Remove { .. } => f::Command::CartridgeRemove { installed: false },
    }
}

pub async fn cartridge(ctx: &Ctx, command: Command) -> CliResult<Exit> {
    match command {
        Command::List => list(ctx).await,
        Command::Install { name } => install(ctx, &name).await,
        Command::Remove { id } => remove(ctx, &id).await,
    }
}

/// The bundled cartridges could not be read.
fn bundle_unreadable(error: capdag::ExecutionError) -> CliError {
    CliError::failed("the cartridges bundled with capdag could not be read").context(error.to_string())
}

/// Every bundled cartridge, as the host's discovery accepts it.
pub async fn bundled(ctx: &Ctx) -> CliResult<Vec<capdag::orchestrator::executor::HostableCartridge>> {
    ctx.host.bundled_cartridges().await.map_err(bundle_unreadable)
}

/// Every cap the bundled cartridges declare.
pub async fn bundled_caps(ctx: &Ctx) -> CliResult<Vec<CapUrn>> {
    Ok(bundled(ctx)
        .await?
        .into_iter()
        .flat_map(|cartridge| cartridge.cap_groups.into_iter().flat_map(|g| g.caps.into_iter().map(|c| c.urn)))
        .collect())
}

/// The bundled cartridges that declare a cap.
pub async fn bundled_providers(ctx: &Ctx, cap: &CapUrn) -> CliResult<Vec<capdag::orchestrator::executor::BundledProvider>> {
    ctx.host.bundled_providers(cap).await.map_err(bundle_unreadable)
}

/// The cartridges installed under the per-user root (registry and dev).
pub async fn installed(ctx: &Ctx) -> CliResult<Vec<DiscoveredCartridge>> {
    ctx.host
        .installed_cartridges()
        .await
        .map_err(|e| ctx::host_failure("the installed cartridges could not be read", e))
}

/// The cartridge registry's store, its manifest synced and verified.
pub async fn manager(ctx: &Ctx) -> CliResult<Box<dyn CartridgeStore>> {
    if ctx.host.cartridge_registry().is_none() {
        return Err(CliError::failed("this capdag is a development build, bound to no cartridge registry")
            .context("it runs bundled and dev cartridges only")
            .hint("install a release of capdag, or `capdag dev install <project>` a cartridge you build"));
    }
    ctx.host.cartridge_store().await.map_err(|e| match e {
        e @ super::host::HostError::Engine(_) => ctx::host_failure("the cartridge registry could not be read", e)
            .hint("check the connection; bundled cartridges run without one (--offline)"),
        e => ctx::host_failure("the cartridge registry could not be read", e),
    })
}

async fn list(ctx: &Ctx) -> CliResult<Exit> {
    if let Some(error) = super::ends(&ctx.decide(f::Command::CartridgeList, false)) {
        return Err(error);
    }
    let mut rows: Vec<serde_json::Value> = Vec::new();
    if ctx.host.has_bundle() {
        let registry = ctx.registry().await?;
        let bundled = ctx.host.bundled_adapter_groups(registry).await.map_err(bundle_unreadable)?;
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
    for found in installed(ctx).await? {
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
        outln!("No cartridges are bundled or installed. A run installs the ones it needs.");
        return Ok(Exit::Ok);
    }
    let style = &ctx.term.style;
    for row in &rows {
        let state = row["state"].as_str().unwrap_or("");
        outln!(
            "{:<22} {:<12} {}",
            style.name(row["id"].as_str().unwrap_or("")),
            row["version"].as_str().unwrap_or(""),
            match state {
                "refused" => style.bad("refused"),
                other => other.to_string(),
            }
        );
        for warning in row["warnings"].as_array().into_iter().flatten() {
            outln!("  {} {}", style.warning("warning:"), warning.as_str().unwrap_or(""));
        }
    }
    Ok(Exit::Ok)
}

/// What `capdag cartridge install` was given, as the cartridge registry and
/// the fabric know it.
enum Target {
    Cartridge(String),
    Bundled { alias: String, provider: capdag::orchestrator::executor::BundledProvider },
    Capability { alias: String, providers: Vec<String> },
    Unknown,
}

async fn install(ctx: &Ctx, name: &str) -> CliResult<Exit> {
    let store = manager(ctx).await?;
    // A name with ':' is a cap URN; a bare name is a cartridge id when the
    // registry knows one by it, else a cap alias.
    let target = if !name.contains(':') && store.has(name).await {
        Target::Cartridge(name.to_string())
    } else {
        let registry = ctx.registry().await?;
        match registry.get_cap(name).await {
            Ok(cap) => {
                let alias = names::cap(&registry, &cap);
                match bundled_providers(ctx, &cap.urn).await?.into_iter().next() {
                    Some(provider) => Target::Bundled { alias, provider },
                    None => Target::Capability { alias, providers: store.providers_of(&cap.urn.to_string()).await },
                }
            }
            Err(e) => {
                super::ctx::nothing_by_that_name(e)?;
                Target::Unknown
            }
        }
    };
    let modelled = match &target {
        Target::Cartridge(_) => f::InstallTarget::Cartridge,
        Target::Bundled { .. } => f::InstallTarget::Bundled,
        Target::Capability { providers, .. } => f::InstallTarget::Capability {
            providers: match providers.len() {
                0 => f::Choices::None,
                1 => f::Choices::One,
                _ => f::Choices::Many,
            },
        },
        Target::Unknown => f::InstallTarget::Unknown,
    };
    let verdict = ctx.decide(f::Command::CartridgeInstall { target: modelled }, false);
    let cartridge_id = match (&verdict, &target) {
        (f::CliVerdict::Is { v: f::Verdict::Answers }, Target::Cartridge(id)) => id.clone(),
        (f::CliVerdict::Is { v: f::Verdict::Answers }, Target::Bundled { alias, provider }) => {
            if ctx.term.json {
                emit_record(&serde_json::json!({
                    "type": "cartridge", "id": provider.id, "version": provider.version, "state": "bundled",
                    "path": provider.entry_point.display().to_string(),
                }));
            } else {
                eprintln!("{alias} is provided by {} {}, bundled with capdag: nothing to install", provider.id, provider.version);
                // The cartridge's path on stdout, as an install prints the one it installed.
                outln!("{}", provider.entry_point.display());
            }
            return Ok(Exit::Ok);
        }
        (f::CliVerdict::Is { v: f::Verdict::Answers }, Target::Capability { providers, .. }) => providers[0].clone(),
        (f::CliVerdict::Is { v: f::Verdict::Asks { s: f::Stage::Provider } }, Target::Capability { alias, providers }) => {
            match ask::pick(ctx, &provider_question(alias), &provider_choices(alias, providers)).await? {
                Answered::Given(at) => providers[at].clone(),
                Answered::Declined => return Err(ask::declined()),
            }
        }
        (f::CliVerdict::Is { v: f::Verdict::Names { .. } }, Target::Capability { alias, providers }) => {
            return Err(super::say::with_exit(
                ask::named(ctx, &provider_question(alias), &provider_choices(alias, providers)),
                &verdict,
            ))
        }
        (f::CliVerdict::Is { v: f::Verdict::Nothing { .. } }, Target::Capability { alias, .. }) => {
            return Err(super::say::with_exit(CliError::failed(format!("no cartridge in the registry provides {alias}")), &verdict))
        }
        (f::CliVerdict::Refused { r: f::CliRefusal::NoSuchName }, _) => {
            return Err(super::say::with_exit(
                CliError::usage(format!("'{name}': {}", f::says(&f::CliRefusal::NoSuchName)))
                    .hint("see the cartridges with: capdag cartridge list; find a capability with: capdag caps"),
                &verdict,
            ))
        }
        (other, _) => return Err(super::ends(other).expect("a verdict other than answers ends the command")),
    };
    let path = install_cartridge(ctx, &cartridge_id).await?;
    if ctx.term.json {
        emit_record(&serde_json::json!({
            "type": "cartridge", "id": cartridge_id, "state": "installed", "path": path.display().to_string(),
        }));
    } else {
        outln!("{}", path.display());
    }
    Ok(Exit::Ok)
}

fn provider_question(alias: &str) -> Question {
    Question { what: format!("the cartridge to install for {alias}"), flag: "capdag cartridge install ".into() }
}

fn provider_choices(alias: &str, providers: &[String]) -> Vec<Choice> {
    providers
        .iter()
        .map(|id| Choice { label: id.clone(), detail: format!("provides {alias}"), urn: None, value: id.clone() })
        .collect()
}

/// Download, verify and install cartridge `id` from the registry: answers
/// its entry point.
pub async fn install_cartridge(ctx: &Ctx, id: &str) -> CliResult<std::path::PathBuf> {
    let store = manager(ctx).await?;
    if !ctx.term.json {
        eprintln!("Installing {id}…");
    }
    let path = store.install(id).await.map_err(|e| match e.refused_cartridge() {
        // An install this capdag does not use is replaced only when the
        // person removes it: installing never overwrites one silently.
        Some((refused, dir)) => super::run::refused_cartridge(refused, dir, &e),
        None => CliError::failed(format!("{id} could not be installed")).context(e.to_string()),
    })?;
    if !ctx.term.json {
        eprintln!("Installed and verified {id}");
    }
    Ok(path)
}

async fn remove(ctx: &Ctx, id: &str) -> CliResult<Exit> {
    let present = installed(ctx).await?.iter().any(|found| match found {
        DiscoveredCartridge::Directory { id: have, .. } | DiscoveredCartridge::Incompatible { id: have, .. } => have == id,
    });
    let verdict = ctx.decide(f::Command::CartridgeRemove { installed: present }, false);
    if let f::CliVerdict::Refused { r: f::CliRefusal::NotInstalled } = verdict {
        let mut error = CliError::usage(format!("'{id}': {}", f::says(&f::CliRefusal::NotInstalled)));
        if bundled(ctx).await?.iter().any(|b| b.identity.as_ref().is_some_and(|(name, _, _)| name == id)) {
            error = error.context(format!("{id} is bundled with capdag, and a bundled cartridge cannot be removed"));
        }
        return Err(super::say::with_exit(error, &verdict));
    }
    if let Some(error) = super::ends(&verdict) {
        return Err(error);
    }
    let removal = ctx
        .host
        .remove_cartridge(id)
        .map_err(|e| ctx::host_failure(format!("{id} could not be removed"), e))?;
    if removal.removed.is_empty() {
        return Err(CliError::failed(format!("{id} was found installed, and nothing of it was removed"))
            .hint("see the cartridges with: capdag cartridge list"));
    }
    for dir in &removal.removed {
        if ctx.term.json {
            emit_record(&serde_json::json!({ "type": "removed", "id": id, "dir": dir.display().to_string() }));
        } else {
            eprintln!("Removed {}", dir.display());
        }
    }
    Ok(Exit::Ok)
}
