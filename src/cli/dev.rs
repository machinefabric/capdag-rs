//! `capdag dev new | install`: making a cartridge, and running it before it is
//! published. `hash-dir` and `bundle-manifest` are the build's own tools,
//! hidden from the help.

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
    /// Write the manifest a build ships beside its bundled cartridges
    #[command(hide = true, after_help = "Examples:\n  capdag dev bundle-manifest --root <dir> --environment <env>")]
    BundleManifest {
        #[arg(long, value_name = "DIR")]
        root: PathBuf,
        #[arg(long, value_name = "ENV")]
        environment: String,
    },
}

/// The languages a project can be started in, from the vendored stubs.
fn languages() -> clap::builder::PossibleValuesParser {
    clap::builder::PossibleValuesParser::new(crate::dev::stub_languages().iter().map(|l| l.id))
}

pub async fn dev(ctx: &Ctx, command: Command) -> CliResult<Exit> {
    match command {
        Command::New { path, lang } => new(ctx, &path, &lang),
        Command::Install { dir } => install(ctx, &dir).await,
        Command::HashDir { dir } => {
            let hash = crate::hash_cartridge_directory(&dir).map_err(|e| {
                CliError::failed(format!("{} could not be hashed", dir.display())).context(e.to_string())
            })?;
            println!("{hash}");
            Ok(Exit::Ok)
        }
        Command::BundleManifest { root, environment } => bundle_manifest(&root, &environment),
    }
}

fn new(ctx: &Ctx, path: &std::path::Path, lang: &str) -> CliResult<Exit> {
    let language = crate::dev::stub_language(lang).expect("clap admits only the languages the stubs declare");
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .ok_or_else(|| CliError::usage(format!("'{}' names no directory to make", path.display())))?;
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let project = crate::dev::scaffold_cartridge(&name, language, &parent)
        .map_err(|e| CliError::failed(format!("the project {} could not be made", path.display())).context(e.to_string()))?;
    if ctx.term.json {
        emit_record(&serde_json::json!({ "type": "project", "dir": project.display().to_string(), "language": language.id }));
        return Ok(Exit::Ok);
    }
    eprintln!("Made the {} cartridge '{name}' at {}", language.display, project.display());
    eprintln!("Next:");
    eprintln!("  cd {}", project.display());
    for step in language.build {
        eprintln!("  {}", step.replace(crate::dev::stubs_generated::STUB_PLACEHOLDER, &name));
    }
    eprintln!("  capdag dev install");
    eprintln!("  echo \"I love this\" | capdag run {name}");
    println!("{}", project.display());
    Ok(Exit::Ok)
}

async fn install(ctx: &Ctx, dir: &std::path::Path) -> CliResult<Exit> {
    let entry = crate::dev::project_entry(dir).map_err(|e| {
        CliError::failed(format!("{} is not a cartridge project that is built", dir.display()))
            .context(e.to_string())
            .hint("build it first, as `capdag dev new` said; then run this again")
    })?;
    let manifest = crate::dev::read_entry_manifest(&entry)
        .map_err(|e| CliError::failed("the cartridge's manifest could not be read").context(e.to_string()))?;
    // A dev cartridge may declare caps the fabric does not know, but none of
    // its aliases may mean another cap upstream.
    let registry = ctx.registry().await?;
    for group in &manifest.cap_groups {
        for cap in &group.caps {
            crate::dev::check_no_fabric_conflict(&registry, cap)
                .await
                .map_err(|e| CliError::failed("the cartridge's caps conflict with the fabric").context(e.to_string()))?;
        }
    }
    let version_dir = crate::dev::stage_dev_cartridge(dir, &manifest, &ctx::user_cartridge_dir()?, crate::FABRIC_MANIFEST_VERSION)
        .map_err(|e| CliError::failed("the cartridge could not be installed").context(e.to_string()))?;
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
/// hash of its directory, in `bundle.json` there. The build signs it. A
/// manifest listing nothing is refused: it would describe a bundle that
/// verifies and hosts nothing.
fn bundle_manifest(root: &std::path::Path, environment: &str) -> CliResult<Exit> {
    if !root.is_dir() {
        return Err(CliError::usage(format!("--root {}: no such directory", root.display())));
    }
    let mut staged = std::collections::BTreeMap::new();
    collect_staged(root, &mut staged)?;
    if staged.is_empty() {
        return Err(CliError::failed(format!("{} holds no staged cartridges", root.display()))
            .context("a manifest listing nothing would describe a bundle that verifies and hosts nothing"));
    }
    let manifest = crate::bundle_manifest_for(environment, &staged)
        .map_err(|e| CliError::failed("the bundle manifest could not be made").context(e.to_string()))?;
    let bytes = manifest
        .to_bytes()
        .map_err(|e| CliError::failed("the bundle manifest could not be written").context(e.to_string()))?;
    let (path, signature) = crate::bundle_manifest_paths(root);
    std::fs::write(&path, &bytes)
        .map_err(|e| CliError::failed(format!("{} could not be written", path.display())).context(e.to_string()))?;
    // A signature of an earlier build describes bytes that are gone: left, it
    // would be found, fail verification and read as tampering.
    let _ = std::fs::remove_file(&signature);
    eprintln!("wrote {} — {} cartridge(s), environment {environment}", path.display(), manifest.cartridges.len());
    for one in &manifest.cartridges {
        eprintln!("  {} {} [{}] {}", one.name, one.version, one.channel, one.sha256);
    }
    eprintln!("next: sign it, and put the envelope at {}", signature.display());
    // The path alone on stdout, for the build to capture what to sign.
    println!("{}", path.display());
    Ok(Exit::Ok)
}

/// Every `{slug}/v{n}/{channel}/{name}/{version}/` directory under a staged
/// bundled-cartridges root that holds a cartridge.
fn collect_staged(
    root: &std::path::Path,
    found: &mut std::collections::BTreeMap<(String, String, String), PathBuf>,
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
