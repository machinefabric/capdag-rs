//! `capdag dev new | install`: making a cartridge, and running it before it is
//! published. `hash-dir`, `bundle-manifest` and `bundle-rehash` are the build's
//! own tools, hidden from the help.

use std::path::PathBuf;

use super::ctx::{self, Ctx};
use super::error::{CliError, CliResult, Exit};
use super::term::emit_record;

#[derive(Debug, clap::Subcommand)]
pub enum Command {
    /// Start a cartridge project in any language capdag scaffolds
    #[command(after_help = "Examples:\n  \
        capdag dev new sentiment --lang rust       a Rust project in ./sentiment\n  \
        capdag dev new carts/sentiment --lang python")]
    New {
        /// Where to make it; its last part is the cartridge's name
        #[arg(value_name = "PATH")]
        path: PathBuf,
        /// The language to write it in
        #[arg(long, value_name = "LANGUAGE", value_parser = languages())]
        lang: String,
    },
    /// Install a cartridge project for local runs, under the `dev` slot
    #[command(after_help = "Examples:\n  \
        capdag dev install                 the project in the working directory\n  \
        capdag dev install ./sentiment\n\n\
        Run it again after each build: it replaces what it installed.")]
    Install {
        /// The project's directory
        #[arg(value_name = "DIR", default_value = ".")]
        dir: PathBuf,
    },
    /// The content hash of a cartridge version directory (the build uses it)
    #[command(hide = true, after_help = "Examples:\n  capdag dev hash-dir <version-dir>")]
    HashDir {
        #[arg(value_name = "DIR")]
        dir: PathBuf,
    },
    /// Record the manifest a build ships beside its bundled cartridges, before
    /// they are signed for their platform
    #[command(hide = true, after_help = "Examples:\n  capdag dev bundle-manifest --root <dir> --environment <env>")]
    BundleManifest {
        #[arg(long, value_name = "DIR")]
        root: PathBuf,
        #[arg(long, value_name = "ENV")]
        environment: String,
    },
    /// Hash the recorded manifest's cartridges again, after a step rewrote them
    #[command(hide = true, after_help = "Examples:\n  capdag dev bundle-rehash --root <dir> --environment <env>")]
    BundleRehash {
        #[arg(long, value_name = "DIR")]
        root: PathBuf,
        #[arg(long, value_name = "ENV")]
        environment: String,
    },
}

/// The languages a project can be started in, from the vendored stubs.
fn languages() -> clap::builder::PossibleValuesParser {
    clap::builder::PossibleValuesParser::new(capdag::dev::stub_languages().iter().map(|l| l.id))
}

pub async fn dev(ctx: &Ctx, command: Command) -> CliResult<Exit> {
    match command {
        Command::New { path, lang } => new(ctx, &path, &lang),
        Command::Install { dir } => install(ctx, &dir).await,
        Command::HashDir { dir } => {
            let hash = capdag::hash_cartridge_directory(&dir).map_err(|e| {
                CliError::failed(format!("{} could not be hashed", dir.display())).context(e.to_string())
            })?;
            println!("{hash}");
            Ok(Exit::Ok)
        }
        Command::BundleManifest { root, environment } => bundle_manifest(ctx, &root, &environment).await,
        Command::BundleRehash { root, environment } => bundle_rehash(&root, &environment),
    }
}

fn new(ctx: &Ctx, path: &std::path::Path, lang: &str) -> CliResult<Exit> {
    let language = capdag::dev::stub_language(lang).expect("clap admits only the languages the stubs declare");
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .ok_or_else(|| CliError::usage(format!("'{}' names no directory to make", path.display())))?;
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let project = capdag::dev::scaffold_cartridge(&name, language, &parent)
        .map_err(|e| CliError::failed(format!("the project {} could not be made", path.display())).context(e.to_string()))?;
    if ctx.term.json {
        emit_record(&serde_json::json!({ "type": "project", "dir": project.display().to_string(), "language": language.id }));
        return Ok(Exit::Ok);
    }
    eprintln!("Made the {} cartridge '{name}' at {}", language.display, project.display());
    eprintln!("Next:");
    eprintln!("  cd {}", project.display());
    for step in language.build {
        eprintln!("  {}", step.replace(capdag::dev::stubs_generated::STUB_PLACEHOLDER, &name));
    }
    eprintln!("  capdag dev install");
    eprintln!("  echo \"I love this\" | capdag run {name}");
    println!("{}", project.display());
    Ok(Exit::Ok)
}

async fn install(ctx: &Ctx, dir: &std::path::Path) -> CliResult<Exit> {
    let entry = ctx.host.dev_project_entry(dir).map_err(|e| {
        CliError::failed(format!("{} is not a cartridge project that is built", dir.display()))
            .context(e.to_string())
            .hint("build it first, as `capdag dev new` said; then run this again")
    })?;
    let manifest = ctx
        .host
        .dev_entry_manifest(&entry)
        .map_err(|e| CliError::failed("the cartridge's manifest could not be read").context(e.to_string()))?;
    // A dev cartridge may declare caps the fabric does not know, but none of
    // its aliases may mean another cap upstream.
    let registry = ctx.registry().await?;
    for group in &manifest.cap_groups {
        for cap in &group.caps {
            capdag::dev::check_no_fabric_conflict(&registry, cap)
                .await
                .map_err(|e| CliError::failed("the cartridge's caps conflict with the fabric").context(e.to_string()))?;
        }
    }
    let version_dir = ctx
        .host
        .install_dev_cartridge(dir, &manifest)
        .map_err(|e| ctx::host_failure("the cartridge could not be installed", e))?;
    let alias = manifest
        .cap_groups
        .iter()
        .flat_map(|g| g.caps.iter())
        .filter(|c| !c.get_aliases().iter().any(|a| a == "identity"))
        .find_map(|c| c.get_aliases().first().cloned());
    if ctx.term.json {
        emit_record(&serde_json::json!({
            "type": "cartridge", "id": manifest.name, "version": manifest.version, "state": "dev",
            "dir": version_dir.display().to_string(),
        }));
        return Ok(Exit::Ok);
    }
    eprintln!("Installed the dev cartridge '{}' {} at {}", manifest.name, manifest.version, version_dir.display());
    if let Some(alias) = alias {
        eprintln!("Run it: capdag run {alias} <input>");
    }
    println!("{}", version_dir.display());
    Ok(Exit::Ok)
}

/// Record what a build ships: every cartridge staged under `root`, with the
/// hash of its directory, the fabric manifest version it was built against,
/// and the cap groups it advertises, in `bundle.json` there. Each cartridge is
/// started once here, for its HELLO, so that no host has to start every
/// bundled cartridge to learn its caps.
///
/// A build runs this as soon as the tree is staged, before it signs the
/// cartridges for their platform: one signed into the macOS app sandbox traps
/// when anything but its app starts it. After the last step that rewrites a
/// byte, `bundle-rehash` takes the hashes again, and that is what is signed.
async fn bundle_manifest(ctx: &Ctx, root: &std::path::Path, environment: &str) -> CliResult<Exit> {
    let mut asked = std::collections::BTreeMap::new();
    for (key, dir) in staged_dirs(root)? {
        let record = cartridge_record(&dir)?;
        let entry = record.resolve_entry_point(&dir);
        let cap_groups = ctx.host.probe_cap_groups(&entry).await.map_err(|e| {
            CliError::failed(format!("{} {} could not be asked for its caps", record.name, record.version))
                .context(e.to_string())
                .hint("the manifest is recorded before the cartridges are signed for their platform; a cartridge signed into the macOS app sandbox cannot be started outside its app")
        })?;
        asked.insert(key, capdag::AskedCartridge { staged: staged_cartridge(&dir, &record), cap_groups });
    }
    let manifest = capdag::bundle_manifest_for(environment, &asked)
        .map_err(|e| CliError::failed("the bundle manifest could not be made").context(e.to_string()))?;
    write_bundle_manifest(root, &manifest, "recorded")?;
    eprintln!("next: sign the cartridges for their platform, then run `capdag dev bundle-rehash` and sign what it writes");
    Ok(Exit::Ok)
}

/// Take the hashes of the cartridges `root`'s recorded manifest lists again,
/// after a step that rewrote their bytes, keeping the caps the record asked of
/// them. Starts nothing: a cartridge signed for its platform may not start
/// outside its app. The staged tree must be the set the record lists.
fn bundle_rehash(root: &std::path::Path, environment: &str) -> CliResult<Exit> {
    let mut staged = std::collections::BTreeMap::new();
    for (key, dir) in staged_dirs(root)? {
        let record = cartridge_record(&dir)?;
        staged.insert(key, staged_cartridge(&dir, &record));
    }
    let (path, _) = capdag::bundle_manifest_paths(root);
    let bytes = std::fs::read(&path).map_err(|e| {
        CliError::failed(format!("{} could not be read", path.display()))
            .context(e.to_string())
            .hint("record it first, before the cartridges are signed: capdag dev bundle-manifest --root <dir> --environment <env>")
    })?;
    let recorded: capdag::BundleManifest = serde_json::from_slice(&bytes)
        .map_err(|e| CliError::failed(format!("{} is not a bundle manifest", path.display())).context(e.to_string()))?;
    if recorded.format != capdag::BUNDLE_MANIFEST_FORMAT {
        return Err(CliError::failed(format!("{} is not a bundle manifest", path.display())).context(format!(
            "its format is '{}', and this capdag writes '{}'",
            recorded.format,
            capdag::BUNDLE_MANIFEST_FORMAT
        )));
    }
    let manifest = capdag::bundle_manifest_rehashed(&recorded, environment, &staged)
        .map_err(|e| CliError::failed("the bundle manifest could not be hashed again").context(e.to_string()))?;
    write_bundle_manifest(root, &manifest, "rehashed")?;
    eprintln!("next: sign it, and put the envelope at {}", capdag::bundle_manifest_paths(root).1.display());
    Ok(Exit::Ok)
}

/// Write `manifest` as `root`'s bundle manifest, say what it lists, and put its
/// path alone on stdout, for the build to capture what to sign.
fn write_bundle_manifest(root: &std::path::Path, manifest: &capdag::BundleManifest, done: &str) -> CliResult<()> {
    let bytes = manifest
        .to_bytes()
        .map_err(|e| CliError::failed("the bundle manifest could not be written").context(e.to_string()))?;
    let (path, signature) = capdag::bundle_manifest_paths(root);
    std::fs::write(&path, &bytes)
        .map_err(|e| CliError::failed(format!("{} could not be written", path.display())).context(e.to_string()))?;
    // A signature of earlier bytes describes bytes that are gone: left, it
    // would be found, fail verification and read as tampering.
    let _ = std::fs::remove_file(&signature);
    eprintln!(
        "{done} {} — {} cartridge(s), environment {}",
        path.display(),
        manifest.cartridges.len(),
        manifest.environment
    );
    for one in &manifest.cartridges {
        eprintln!("  {} {} [{}] {}", one.name, one.version, one.channel, one.sha256);
    }
    println!("{}", path.display());
    Ok(())
}

/// A staged cartridge's `cartridge.json`.
fn cartridge_record(dir: &std::path::Path) -> CliResult<capdag::CartridgeJson> {
    let path = dir.join("cartridge.json");
    let bytes = std::fs::read(&path)
        .map_err(|e| CliError::failed(format!("{} could not be read", path.display())).context(e.to_string()))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| CliError::failed(format!("{} is not a cartridge.json", path.display())).context(e.to_string()))
}

/// A staged cartridge, as its `cartridge.json` describes it.
fn staged_cartridge(dir: &std::path::Path, record: &capdag::CartridgeJson) -> capdag::StagedCartridge {
    capdag::StagedCartridge { dir: dir.to_path_buf(), fabric_manifest_version: record.fabric_manifest_version }
}

/// Every cartridge staged under a bundled-cartridges root, by what its
/// directory path says it is. A root holding none is refused: a manifest
/// listing nothing would describe a bundle that verifies and hosts nothing.
fn staged_dirs(root: &std::path::Path) -> CliResult<std::collections::BTreeMap<capdag::StagedKey, PathBuf>> {
    if !root.is_dir() {
        return Err(CliError::usage(format!("--root {}: no such directory", root.display())));
    }
    let mut dirs = std::collections::BTreeMap::new();
    collect_staged(root, &mut dirs)?;
    if dirs.is_empty() {
        return Err(CliError::failed(format!("{} holds no staged cartridges", root.display()))
            .context("a manifest listing nothing would describe a bundle that verifies and hosts nothing"));
    }
    Ok(dirs)
}

/// Every `{slug}/v{n}/{channel}/{name}/{version}/` directory under a staged
/// bundled-cartridges root that holds a cartridge.
fn collect_staged(
    root: &std::path::Path,
    found: &mut std::collections::BTreeMap<capdag::StagedKey, PathBuf>,
) -> CliResult<()> {
    let dirs = |at: &std::path::Path| -> CliResult<Vec<PathBuf>> {
        let entries = std::fs::read_dir(at)
            .map_err(|e| CliError::failed(format!("{} could not be read", at.display())).context(e.to_string()))?;
        let mut out: Vec<PathBuf> = entries.filter_map(Result::ok).map(|e| e.path()).filter(|p| p.is_dir()).collect();
        out.sort();
        Ok(out)
    };
    let name_of = |p: &std::path::Path| p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    for slug in dirs(root)? {
        for version_level in dirs(&slug)? {
            for channel in dirs(&version_level)? {
                for name in dirs(&channel)? {
                    for version in dirs(&name)? {
                        if version.join("cartridge.json").is_file() {
                            found.insert((name_of(&name), name_of(&version), name_of(&channel)), version);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
