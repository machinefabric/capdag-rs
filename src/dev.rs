//! Cartridge-development support for the `capdag` CLI.
//!
//! This module backs three developer commands and the local-manifest run path:
//!
//! - [`scaffold_cartridge`] — `capdag new <name> --<language>`: write a fresh,
//!   runnable cartridge project (one custom cap, one Op that peer-calls a
//!   model, one manifest) into a new directory, in any language the vendored
//!   canonical stubs cover. The stubs are the SAME bytes in every capdag
//!   implementation (see `stubs_generated`), so the project you get does not
//!   depend on which capdag binary you ran.
//! - [`stage_dev_cartridge`] — `capdag dev-install <project-dir>`: read the
//!   project's manifest, then copy it under the per-user cartridge root's
//!   reserved `dev` slug so the capdag host (and any other host pointed at that
//!   root) discovers it. Re-running overwrites the same version directory — the
//!   update step of the edit/reinstall loop.
//! - [`find_dev_cap_by_alias`] + [`check_no_fabric_conflict`] — the local-manifest
//!   run path: when `capdag <alias>` names a cap the fabric does NOT define, we
//!   fall back to a locally dev-installed cartridge's OWN manifest and run that
//!   cap through the full bifaci host — **as long as the cap does not conflict
//!   with the fabric** (no alias of it already means a different cap upstream).
//!   A dev cap never needs to be published to be developed and run locally.
//!
//! The on-disk layout mirrors every other host exactly:
//! `{user_cartridge_dir}/dev/v{CARTRIDGE_REGISTRY_VERSION}/{channel}/{name}/{version}/`.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::bifaci::cartridge_repo::CartridgeChannel;
use crate::bifaci::cartridge_slug::DEV_SLUG;
use crate::bifaci::manifest::CapManifest;
use crate::cap::definition::Cap;
use crate::fabric::registry::FabricRegistry;

pub mod stubs_generated;

/// Errors from the cartridge-development commands. Each variant is actionable —
/// it names the file, entry, or conflicting alias so the developer can fix it.
#[derive(Debug)]
pub enum DevError {
    Io(String),
    InvalidName(String),
    AlreadyExists(PathBuf),
    NoEntry(PathBuf),
    AmbiguousEntry {
        project: PathBuf,
        found: Vec<PathBuf>,
    },
    ManifestSpawn {
        entry: PathBuf,
        source: String,
    },
    ManifestFailed {
        entry: PathBuf,
        code: Option<i32>,
        stderr: String,
    },
    ManifestParse {
        entry: PathBuf,
        source: String,
    },
    NotDev {
        registry_url: String,
    },
    FabricConflict {
        alias: String,
        dev_urn: String,
        fabric_urn: String,
    },
}

impl std::fmt::Display for DevError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DevError::Io(m) => write!(f, "{m}"),
            DevError::InvalidName(n) => write!(
                f,
                "invalid cartridge name '{n}': use a lowercase, path-safe name \
                 matching [a-z0-9] with '-' or '_' separators (e.g. sentiment-tagger)"
            ),
            DevError::AlreadyExists(p) => {
                write!(
                    f,
                    "'{}' already exists — pick a new name or remove it first",
                    p.display()
                )
            }
            DevError::NoEntry(p) => write!(
                f,
                "no cartridge entry found in '{}'. Looked for {}. A compiled cartridge \
                 must be BUILT before it is installed — the host launches the binary, \
                 not the sources. Create the project with `capdag new`.",
                p.display(),
                stub_entry_candidates_description(p)
            ),
            DevError::AmbiguousEntry { project, found } => write!(
                f,
                "'{}' contains more than one cartridge entry ({}) — capdag cannot tell \
                 which one to install. A project is ONE cartridge; remove the build \
                 outputs of the language you are not developing.",
                project.display(),
                found
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            DevError::ManifestSpawn { entry, source } => write!(
                f,
                "could not run the cartridge entry '{}' to read its manifest: {source}. \
                 Make sure it is executable and its dependencies (capdag) are importable.",
                entry.display()
            ),
            DevError::ManifestFailed {
                entry,
                code,
                stderr,
            } => write!(
                f,
                "the cartridge entry '{}' exited with {} when asked for its manifest:\n{}",
                entry.display(),
                code.map(|c| format!("code {c}"))
                    .unwrap_or_else(|| "a signal".to_string()),
                stderr.trim()
            ),
            DevError::ManifestParse { entry, source } => write!(
                f,
                "the cartridge entry '{}' printed a manifest capdag could not parse: {source}",
                entry.display()
            ),
            DevError::NotDev { registry_url } => write!(
                f,
                "this project declares registry_url='{registry_url}' — `dev-install` only \
                 installs DEV cartridges (registry_url must be null). Publish it through the \
                 cartridge registry instead, or set registry_url to null for local development."
            ),
            DevError::FabricConflict {
                alias,
                dev_urn,
                fabric_urn,
            } => write!(
                f,
                "dev cap '{dev_urn}' claims alias '{alias}', but the fabric already maps that \
                 alias to a different cap '{fabric_urn}'. A dev cartridge may declare caps the \
                 fabric does not know, but its aliases must not collide with the fabric. Rename \
                 the dev cap's alias."
            ),
        }
    }
}

impl std::error::Error for DevError {}

fn io_err(context: &str, e: std::io::Error) -> DevError {
    DevError::Io(format!("{context}: {e}"))
}

// ---------------------------------------------------------------------------
// Scaffold — `capdag new <name> --python`
// ---------------------------------------------------------------------------

/// Validate a cartridge project name: a path-safe, lowercase identifier
/// (`[a-z0-9]` with `-`/`_` separators). This is the manifest name, the on-disk
/// folder, and — in the scaffold — the seed for the example cap's alias and
/// URN tags, so it must be a clean slug.
pub fn valid_cartridge_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
}

/// Every language `capdag new` can scaffold, from the vendored canonical stubs.
///
/// The list is the contract's, in its order — a mirror that offered a subset
/// would silently make `capdag new --rust` mean different things depending on
/// which capdag binary you happened to run.
pub fn stub_languages() -> &'static [stubs_generated::StubLanguage] {
    stubs_generated::STUB_LANGUAGES
}

/// Look a language up by its id (`python`) or its flag (`--python`).
///
/// Returns `None` for anything else; the caller turns that into an error that
/// lists what IS available, which is the only useful thing to say.
pub fn stub_language(selector: &str) -> Option<&'static stubs_generated::StubLanguage> {
    stubs_generated::STUB_LANGUAGES
        .iter()
        .find(|l| l.id == selector || l.flag == selector)
}

/// Substitute the project name into a stub's text.
///
/// The placeholder appears in file CONTENTS, in destination PATHS, and in the
/// entry — a compiled cartridge's binary is named after the project — so one
/// function serves all three rather than three call sites each remembering.
fn render(template: &str, name: &str) -> String {
    template.replace(stubs_generated::STUB_PLACEHOLDER, name)
}

/// The executable the host launches for a scaffolded project, relative to the
/// project directory.
pub fn stub_entry(language: &stubs_generated::StubLanguage, name: &str) -> String {
    render(language.entry, name)
}

/// Scaffold a new cartridge project named `name` under `parent_dir`, in
/// `language`. Returns the created project directory.
///
/// Fails hard if the name is not path-safe or the target already exists — never
/// overwrites existing work, and never half-writes: the directory is created
/// first and a failure part-way leaves the error naming the exact file.
pub fn scaffold_cartridge(
    name: &str,
    language: &stubs_generated::StubLanguage,
    parent_dir: &Path,
) -> Result<PathBuf, DevError> {
    if !valid_cartridge_name(name) {
        return Err(DevError::InvalidName(name.to_string()));
    }
    let project_dir = parent_dir.join(name);
    if project_dir.exists() {
        return Err(DevError::AlreadyExists(project_dir));
    }
    fs::create_dir_all(&project_dir).map_err(|e| {
        io_err(
            &format!("creating project dir '{}'", project_dir.display()),
            e,
        )
    })?;

    for file in language.files {
        let dest = project_dir.join(render(file.dest, name));
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| io_err(&format!("creating '{}'", parent.display()), e))?;
        }
        fs::write(&dest, render(file.contents, name))
            .map_err(|e| io_err(&format!("writing '{}'", dest.display()), e))?;
        if file.executable {
            make_executable(&dest)?;
        }
    }

    Ok(project_dir)
}

#[cfg(unix)]
fn make_executable(path: &Path) -> Result<(), DevError> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = fs::metadata(path)
        .map_err(|e| io_err(&format!("stat '{}'", path.display()), e))?
        .permissions();
    perms.set_mode(perms.mode() | 0o755);
    fs::set_permissions(path, perms)
        .map_err(|e| io_err(&format!("chmod +x '{}'", path.display()), e))
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) -> Result<(), DevError> {
    // On Windows the host launches the entry through its file association /
    // launcher; there is no executable bit to set.
    Ok(())
}

// ---------------------------------------------------------------------------
// Manifest reading — run `<entry> manifest` and parse the CapManifest JSON.
// ---------------------------------------------------------------------------

/// Run a cartridge entry's `manifest` subcommand and parse the printed
/// `CapManifest` JSON. Every cartridge (any language) prints the same wire
/// shape, so a Python cartridge's output deserializes into the Rust type.
pub fn read_entry_manifest(entry: &Path) -> Result<CapManifest, DevError> {
    let output =
        Command::new(entry)
            .arg("manifest")
            .output()
            .map_err(|e| DevError::ManifestSpawn {
                entry: entry.to_path_buf(),
                source: e.to_string(),
            })?;
    if !output.status.success() {
        return Err(DevError::ManifestFailed {
            entry: entry.to_path_buf(),
            code: output.status.code(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }
    serde_json::from_slice::<CapManifest>(&output.stdout).map_err(|e| DevError::ManifestParse {
        entry: entry.to_path_buf(),
        source: e.to_string(),
    })
}

/// The project name a scaffolded directory carries: its own directory name.
///
/// `capdag new <name>` creates `<parent>/<name>`, and every rendered path is
/// seeded from that name, so the directory IS the name. Reading it back is how
/// `dev-install` knows what a compiled entry is called without being told.
fn project_name(project_dir: &Path) -> Result<&str, DevError> {
    project_dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| DevError::NoEntry(project_dir.to_path_buf()))
}

/// Describe every entry path that WOULD have been accepted, for the error when
/// none exists. Naming them turns "no entry found" into an instruction.
fn stub_entry_candidates_description(project_dir: &Path) -> String {
    let Ok(name) = project_name(project_dir) else {
        return "the entry of each supported language".to_string();
    };
    stub_languages()
        .iter()
        .map(|l| format!("{} ({})", stub_entry(l, name), l.display))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The project's entry path, discovered across every scaffoldable language and
/// verified to exist.
///
/// A project is ONE cartridge, so finding two entries is an error rather than a
/// silent pick: installing whichever language happened to sort first would be a
/// coin flip the developer never sees.
pub fn project_entry(project_dir: &Path) -> Result<PathBuf, DevError> {
    let name = project_name(project_dir)?;
    let found: Vec<PathBuf> = stub_languages()
        .iter()
        .map(|l| project_dir.join(stub_entry(l, name)))
        .filter(|p| p.is_file())
        .collect();
    match found.len() {
        1 => Ok(found.into_iter().next().expect("length checked")),
        0 => Err(DevError::NoEntry(project_dir.to_path_buf())),
        _ => Err(DevError::AmbiguousEntry {
            project: project_dir.to_path_buf(),
            found,
        }),
    }
}

// ---------------------------------------------------------------------------
// dev-install — stage a project under the `dev` slug.
// ---------------------------------------------------------------------------

/// The install version directory for a dev cartridge under `user_cartridge_dir`:
/// `dev/v{CARTRIDGE_REGISTRY_VERSION}/{channel}/{name}/{version}/`.
pub fn dev_version_dir(
    user_cartridge_dir: &Path,
    channel: CartridgeChannel,
    name: &str,
    version: &str,
) -> PathBuf {
    user_cartridge_dir
        .join(DEV_SLUG)
        .join(format!("v{}", crate::CARTRIDGE_REGISTRY_VERSION))
        .join(channel.as_str())
        .join(name)
        .join(version)
}

/// Copy a dev cartridge project into its `dev`-slug version directory and write
/// its `cartridge.json` install record. Overwrites any existing install of the
/// same `(name, version, channel)` — this is the "update" of the edit/reinstall
/// loop. Returns the version directory the cartridge was installed into.
///
/// `manifest` must have already been read from the project (via
/// [`read_entry_manifest`]) and verified to be a dev cartridge (`registry_url`
/// is `None`); this staging step does not itself re-run the entry.
pub fn stage_dev_cartridge(
    project_dir: &Path,
    manifest: &CapManifest,
    user_cartridge_dir: &Path,
    fabric_manifest_version: u32,
) -> Result<PathBuf, DevError> {
    if let Some(url) = &manifest.registry_url {
        return Err(DevError::NotDev {
            registry_url: url.clone(),
        });
    }
    let version_dir = dev_version_dir(
        user_cartridge_dir,
        manifest.channel,
        &manifest.name,
        &manifest.version,
    );

    // Update semantics: replace the version directory wholesale so a removed
    // file in the project does not linger in a stale install.
    if version_dir.exists() {
        fs::remove_dir_all(&version_dir).map_err(|e| {
            io_err(
                &format!("clearing old install '{}'", version_dir.display()),
                e,
            )
        })?;
    }
    fs::create_dir_all(&version_dir)
        .map_err(|e| io_err(&format!("creating '{}'", version_dir.display()), e))?;

    // The entry is discovered in the PROJECT, then recorded relative to the
    // install — a compiled cartridge's entry lives under its build directory
    // (`target/release/<name>`), which the tree copy preserves, so the two are
    // the same relative path.
    let project_entry_path = project_entry(project_dir)?;
    let relative_entry = project_entry_path
        .strip_prefix(project_dir)
        .expect("project_entry returns a path inside the project")
        .to_str()
        .ok_or_else(|| DevError::NoEntry(project_dir.to_path_buf()))?
        .to_string();

    copy_project_tree(project_dir, &version_dir)?;

    // The entry is copied explicitly because a compiled one lives INSIDE a
    // build tree the walk above deliberately skips. Doing it here rather than
    // exempting the whole tree keeps the install to the sources plus the one
    // binary the host actually launches.
    let installed_entry = version_dir.join(&relative_entry);
    if !installed_entry.is_file() {
        if let Some(parent) = installed_entry.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| io_err(&format!("creating '{}'", parent.display()), e))?;
        }
        fs::copy(&project_entry_path, &installed_entry).map_err(|e| {
            io_err(
                &format!(
                    "copying the cartridge entry '{}' into the install",
                    project_entry_path.display()
                ),
                e,
            )
        })?;
    }
    make_executable(&installed_entry)?;

    let cj = crate::CartridgeJson {
        name: manifest.name.clone(),
        version: manifest.version.clone(),
        channel: manifest.channel,
        registry_url: None,
        entry: relative_entry,
        installed_at: crate::bifaci::cartridge_json::install_timestamp_now(),
        installed_from: Some(crate::CartridgeInstallSource::Dev),
        source_url: String::new(),
        package_sha256: String::new(),
        package_size: 0,
        fabric_manifest_version,
    };
    cj.write_to_dir(&version_dir)
        .map_err(|e| DevError::Io(format!("writing cartridge.json: {e}")))?;

    Ok(version_dir)
}

/// Directory/file names never copied into an install (developer scratch that
/// would bloat or break the install).
fn is_ignored_project_entry(name: &str) -> bool {
    matches!(
        name,
        // Developer scratch.
        ".venv" | "__pycache__" | ".git" | ".pytest_cache" | "cartridge.json"
        // Build trees. A compiled cartridge's intermediates are gigabytes of
        // object files and dependency sources that the host never reads — only
        // the linked entry matters, and `stage_dev_cartridge` copies that
        // explicitly after this walk. Without these, `dev-install` on a Rust
        // project would copy its whole `target/`.
        | "target" | ".build" | ".swiftpm" | "node_modules"
    ) || name.ends_with(".pyc")
}

/// Recursively copy a project tree into `dst`, skipping developer scratch.
fn copy_project_tree(src: &Path, dst: &Path) -> Result<(), DevError> {
    for entry in fs::read_dir(src)
        .map_err(|e| io_err(&format!("reading project dir '{}'", src.display()), e))?
    {
        let entry = entry.map_err(|e| io_err("reading a project entry", e))?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if is_ignored_project_entry(&name_str) {
            continue;
        }
        let from = entry.path();
        let to = dst.join(&name);
        let file_type = entry
            .file_type()
            .map_err(|e| io_err(&format!("stat '{}'", from.display()), e))?;
        if file_type.is_dir() {
            fs::create_dir_all(&to)
                .map_err(|e| io_err(&format!("creating '{}'", to.display()), e))?;
            copy_project_tree(&from, &to)?;
        } else {
            fs::copy(&from, &to)
                .map_err(|e| io_err(&format!("copying '{}'", from.display()), e))?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Local-manifest run path — resolve a dev cap by alias, guard against conflict.
// ---------------------------------------------------------------------------

/// Scan the dev-installed cartridges under `user_cartridge_dir/dev/…` and return
/// the cap whose declared `aliases` contain `alias`, along with the version
/// directory it was found in. Reads each dev cartridge's manifest by running its
/// entry's `manifest` subcommand. Returns `Ok(None)` when no dev cartridge
/// advertises the alias (the caller then reports the normal "unknown cap" error).
///
/// Alias uniqueness makes at most one match meaningful; the first match wins.
pub fn find_dev_cap_by_alias(
    user_cartridge_dir: &Path,
    alias: &str,
) -> Result<Option<(Cap, PathBuf)>, DevError> {
    let dev_root = user_cartridge_dir
        .join(DEV_SLUG)
        .join(format!("v{}", crate::CARTRIDGE_REGISTRY_VERSION));
    if !dev_root.is_dir() {
        return Ok(None);
    }
    // dev/v{N}/{channel}/{name}/{version}/
    for version_dir in walk_version_dirs(&dev_root)? {
        let cj_path = version_dir.join("cartridge.json");
        if !cj_path.is_file() {
            continue;
        }
        let bytes = fs::read(&cj_path)
            .map_err(|e| io_err(&format!("reading '{}'", cj_path.display()), e))?;
        let cj: crate::CartridgeJson = match serde_json::from_slice(&bytes) {
            Ok(cj) => cj,
            Err(_) => continue, // a malformed dev install is surfaced elsewhere; skip here.
        };
        let entry = version_dir.join(&cj.entry);
        if !entry.is_file() {
            continue;
        }
        let manifest = read_entry_manifest(&entry)?;
        for group in &manifest.cap_groups {
            for cap in &group.caps {
                if cap.get_aliases().iter().any(|a| a == alias) {
                    return Ok(Some((cap.clone(), version_dir)));
                }
            }
        }
    }
    Ok(None)
}

/// Collect every `.../{channel}/{name}/{version}/` directory three levels below
/// `dev_root` (channel → name → version).
fn walk_version_dirs(dev_root: &Path) -> Result<Vec<PathBuf>, DevError> {
    let mut out = Vec::new();
    for channel in read_subdirs(dev_root)? {
        for name in read_subdirs(&channel)? {
            for version in read_subdirs(&name)? {
                out.push(version);
            }
        }
    }
    Ok(out)
}

fn read_subdirs(dir: &Path) -> Result<Vec<PathBuf>, DevError> {
    let mut out = Vec::new();
    for entry in
        fs::read_dir(dir).map_err(|e| io_err(&format!("reading '{}'", dir.display()), e))?
    {
        let entry = entry.map_err(|e| io_err("reading a directory entry", e))?;
        if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            out.push(entry.path());
        }
    }
    out.sort();
    Ok(out)
}

/// Verify a dev cap does not conflict with the fabric: none of its aliases may
/// already resolve, in the fabric, to a DIFFERENT cap URN. A dev cartridge is
/// free to declare caps the fabric does not know (that is the whole point of
/// local development); it just may not hijack a name the fabric already owns for
/// something else. An alias the fabric does not define at all is fine.
pub async fn check_no_fabric_conflict(
    registry: &FabricRegistry,
    cap: &Cap,
) -> Result<(), DevError> {
    let dev_urn = cap.urn.to_string();
    for alias in cap.get_aliases() {
        if let Ok(target) = registry.resolve_alias(alias).await {
            // Compare canonical forms — resolve_alias returns the target URN
            // string; a dev cap providing the SAME fabric cap (e.g. identity) is
            // not a conflict.
            let fabric_urn = match crate::CapUrn::from_string(&target) {
                Ok(u) => u.to_string(),
                Err(_) => target.clone(),
            };
            if fabric_urn != dev_urn {
                return Err(DevError::FabricConflict {
                    alias: alias.clone(),
                    dev_urn,
                    fabric_urn,
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cap::definition::Cap;
    use crate::fabric::alias::StoredAlias;
    use crate::urn::cap_urn::CapUrn;

    fn temp_root(tag: &str) -> PathBuf {
        // A unique-per-test dir under the OS temp root. No Date/rand available
        // in the crate's normal build, so key on the test tag + process id.
        let base =
            std::env::temp_dir().join(format!("capdag-dev-test-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&base);
        fs::create_dir_all(&base).unwrap();
        base
    }

    // TEST7154: EVERY vendored language scaffolds a runnable-shaped project —
    // every declared file exists, no placeholder survives anywhere (contents or
    // paths), the manifest/alias/media URNs are seeded from the project name,
    // and the interpreted languages' entries are executable.
    //
    // Iterating the contract rather than testing one language is the point: a
    // newly vendored language is covered the moment it appears, instead of
    // whenever someone remembers to add a test for it.
    #[test]
    fn test7154_scaffold_writes_a_runnable_project_in_every_language() {
        let root = temp_root("scaffold");
        assert!(
            !stub_languages().is_empty(),
            "the vendored contract must declare at least one language"
        );

        for language in stub_languages() {
            let name = format!("mood-tagger-{}", language.id);
            let proj = scaffold_cartridge(&name, language, &root).unwrap();
            assert_eq!(proj, root.join(&name));

            for file in language.files {
                let dest = proj.join(render(file.dest, &name));
                assert!(
                    dest.is_file(),
                    "{}: declared file {} was not written",
                    language.id,
                    dest.display()
                );
                let body = fs::read_to_string(&dest).unwrap();
                assert!(
                    !body.contains(stubs_generated::STUB_PLACEHOLDER),
                    "{}: {} still contains the placeholder",
                    language.id,
                    dest.display()
                );

                #[cfg(unix)]
                if file.executable {
                    use std::os::unix::fs::PermissionsExt;
                    let mode = fs::metadata(&dest).unwrap().permissions().mode();
                    assert!(
                        mode & 0o111 != 0,
                        "{}: {} is declared executable but is not",
                        language.id,
                        dest.display()
                    );
                }
            }

            // The rendered entry path must itself be free of the placeholder —
            // a compiled cartridge's binary is named after the project.
            let entry = stub_entry(language, &name);
            assert!(
                !entry.contains(stubs_generated::STUB_PLACEHOLDER),
                "{}: the entry path was not rendered",
                language.id
            );

            // The project name reaches the cap it declares, in every language.
            let sources: String = language
                .files
                .iter()
                .map(|f| render(f.contents, &name))
                .collect();
            assert!(
                sources.contains(&format!("media:enc=utf-8;{name}-input")),
                "{}: input media URN is not seeded from the project name",
                language.id
            );
            assert!(
                !sources.contains("command="),
                "{}: carries the removed `command=` field",
                language.id
            );
        }
    }

    // TEST7155: scaffolding rejects a bad name and refuses to overwrite.
    #[test]
    fn test7155_scaffold_guards() {
        let root = temp_root("guards");
        let language = &stub_languages()[0];
        assert!(matches!(
            scaffold_cartridge("Bad Name", language, &root),
            Err(DevError::InvalidName(_))
        ));
        scaffold_cartridge("greeter", language, &root).unwrap();
        assert!(matches!(
            scaffold_cartridge("greeter", language, &root),
            Err(DevError::AlreadyExists(_))
        ));
    }

    // TEST7159: a project with two languages' entries is REFUSED, not silently
    // resolved. A project is one cartridge; installing whichever entry sorted
    // first would be a coin flip the developer never sees.
    #[cfg(unix)]
    #[test]
    fn test7159_two_entries_is_ambiguous_not_a_coin_flip() {
        let root = temp_root("ambiguous");
        let proj = root.join("twoheaded");
        fs::create_dir_all(&proj).unwrap();

        let mut written = 0usize;
        for language in stub_languages() {
            let entry = proj.join(stub_entry(language, "twoheaded"));
            if let Some(parent) = entry.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(&entry, "#!/usr/bin/env bash\n").unwrap();
            written += 1;
            if written == 2 {
                break;
            }
        }
        assert_eq!(written, 2, "the contract must cover at least two languages");

        assert!(
            matches!(
                project_entry(&proj),
                Err(DevError::AmbiguousEntry { .. })
            ),
            "two entries must be an error, not a pick"
        );
    }

    /// Write a stub cartridge entry (a bash script) that prints a canned
    /// `CapManifest` JSON on `manifest`. Lets us exercise the capdag-side
    /// staging/parsing/resolution without any language runtime.
    ///
    /// It is written at the PYTHON entry because that is the one language whose
    /// entry is a source file with no build step, so a bash script standing in
    /// for it is discovered by exactly the same path a real project would be.
    #[cfg(unix)]
    fn write_stub_entry(dir: &Path, name: &str, alias: &str, urn: &str) -> PathBuf {
        // The cap URN quotes its media specs; escape those quotes for JSON.
        let urn_json = urn.replace('"', "\\\"");
        let manifest = format!(
            r#"{{"name":"{name}","version":"0.1.0","channel":"nightly","registry_url":null,"description":"stub","cap_groups":[{{"name":"default","caps":[{{"urn":"cap:effect=none","title":"Identity","aliases":["identity"]}},{{"urn":"{urn_json}","title":"{name}","aliases":["{alias}"]}}]}}]}}"#
        );
        let script = format!("#!/usr/bin/env bash\nif [ \"$1\" = manifest ]; then\n  cat <<'EOF'\n{manifest}\nEOF\nfi\n");
        let python = stub_language("python").expect("the contract must cover python");
        let dir_name = dir
            .file_name()
            .and_then(|n| n.to_str())
            .expect("test project dir has a name");
        let path = dir.join(stub_entry(python, dir_name));
        fs::write(&path, script).unwrap();
        make_executable(&path).unwrap();
        path
    }

    // TEST7156: read_entry_manifest + stage_dev_cartridge + find_dev_cap_by_alias
    // round-trip: a stub project installs under dev/v{N}/nightly/<name>/<ver>/,
    // writes a cartridge.json, and its custom cap is resolvable by alias.
    #[cfg(unix)]
    #[test]
    fn test7156_dev_install_and_find_by_alias() {
        let root = temp_root("install");
        let project = root.join("proj");
        fs::create_dir_all(&project).unwrap();
        let urn = "cap:greet;in=\"media:enc=utf-8\";out=\"media:enc=utf-8;greeting\"";
        write_stub_entry(&project, "greeter", "greet", urn);

        let user_dir = root.join("cartridges");
        let entry = project_entry(&project).unwrap();
        let manifest = read_entry_manifest(&entry).unwrap();
        assert_eq!(manifest.name, "greeter");
        assert!(manifest.registry_url.is_none());

        let version_dir = stage_dev_cartridge(&project, &manifest, &user_dir, 7).unwrap();
        assert!(version_dir.ends_with(format!(
            "dev/v{}/nightly/greeter/0.1.0",
            crate::CARTRIDGE_REGISTRY_VERSION
        )));
        assert!(version_dir.join("cartridge.json").is_file());
        assert!(version_dir
            .join(stub_entry(
                stub_language("python").expect("the contract must cover python"),
                "proj"
            ))
            .is_file());

        let found = find_dev_cap_by_alias(&user_dir, "greet").unwrap();
        let (cap, dir) = found.expect("dev cap resolvable by alias");
        assert_eq!(dir, version_dir);
        assert!(cap.get_aliases().iter().any(|a| a == "greet"));
        // An alias no dev cartridge advertises resolves to nothing.
        assert!(find_dev_cap_by_alias(&user_dir, "nope").unwrap().is_none());
    }

    // TEST7157: dev-install refuses a PUBLISHED manifest. `registry_url` non-null
    // means the cartridge belongs to a registry, and staging it under the dev
    // slug would put a published identity in a slot reserved for local work.
    #[cfg(unix)]
    #[test]
    fn test7157_dev_install_rejects_published_manifest() {
        let root = temp_root("nondev");
        let project = root.join("proj");
        fs::create_dir_all(&project).unwrap();
        write_stub_entry(&project, "pub", "pub-cap", "cap:effect=none;pub");
        let entry = project_entry(&project).unwrap();
        let mut manifest = read_entry_manifest(&entry).unwrap();
        manifest.registry_url = Some("https://cartridges.example.com/v1/manifest".to_string());
        assert!(matches!(
            stage_dev_cartridge(&project, &manifest, &root.join("c"), 7),
            Err(DevError::NotDev { .. })
        ));
    }

    // TEST7158: the fabric-conflict guard — a dev cap whose alias the fabric maps
    // to a DIFFERENT cap is rejected; a brand-new alias, and a dev cap that
    // matches an existing fabric cap exactly, are both accepted.
    #[tokio::test]
    async fn test7158_fabric_conflict_guard() {
        let registry = FabricRegistry::new_for_test();
        // Seed the fabric with a cap `alpha` at a known URN, and publish its
        // alias into the warm alias cache (as the real publisher would).
        let alpha = Cap::new(
            CapUrn::from_string("cap:alpha;in=\"media:enc=utf-8\";out=\"media:enc=utf-8;alpha\"")
                .unwrap(),
            "Alpha".to_string(),
            vec!["alpha".to_string()],
        );
        let alpha_urn = alpha.urn.to_string();
        registry.add_caps_to_cache(vec![alpha.clone()]);
        registry.add_aliases_to_cache(vec![StoredAlias {
            name: "alpha".to_string(),
            target: alpha_urn.clone(),
            version: 1,
        }]);

        // A dev cap claiming `alpha` but with a DIFFERENT URN => conflict.
        let clashing = Cap::new(
            CapUrn::from_string("cap:beta;in=\"media:enc=utf-8\";out=\"media:enc=utf-8;beta\"")
                .unwrap(),
            "Clash".to_string(),
            vec!["alpha".to_string()],
        );
        assert!(matches!(
            check_no_fabric_conflict(&registry, &clashing).await,
            Err(DevError::FabricConflict { .. })
        ));

        // A brand-new alias the fabric never heard of => fine.
        let fresh = Cap::new(
            CapUrn::from_string("cap:gamma;in=\"media:enc=utf-8\";out=\"media:enc=utf-8;gamma\"")
                .unwrap(),
            "Fresh".to_string(),
            vec!["gamma".to_string()],
        );
        assert!(check_no_fabric_conflict(&registry, &fresh).await.is_ok());

        // The very same fabric cap (same alias => same URN) => not a conflict.
        assert!(check_no_fabric_conflict(&registry, &alpha).await.is_ok());
    }

    // TEST7160: the vendored stub contract is IDENTICAL to the canonical
    // source.
    //
    // This is the whole promise of `capdag new`: the same command from any
    // capdag binary writes the same project. Every mirror's copy is generated
    // from this one source, so a difference here means the reference itself was
    // vendored from a different commit than the stub repo currently holds —
    // which would ship capdags that disagree about what a cartridge looks like,
    // silently.

    /// A vendored stub file against the canonical bytes.
    ///
    /// Byte equality, with ONE allowance: the capdag version the stub pins may
    /// be OLDER in the vendored copy than in the canonical one. The canonical
    /// stub is rendered from a template that stamps capdag's current version,
    /// and the vendored copies are snapshots taken when someone last vendored
    /// them — so the two disagree from the moment capdag's version moves, which
    /// is every time it is bumped, and the disagreement says nothing about the
    /// stub CONTRACT.
    ///
    /// An older pin is harmless: it names a release that exists, so a cartridge
    /// scaffolded from it resolves. A NEWER pin is not, because it would name a
    /// version this capdag has not reached, so the comparison is an ordering and
    /// not "ignore the version".
    ///
    /// Every other byte still has to match, and a line that differs in anything
    /// besides that version still fails.
    ///
    /// And ONE more: HOW the stub reaches capdag is environment, not contract.
    /// The dependency line of a language manifest (a git tag, a module version,
    /// a SwiftPM `from:` — or a path, were one ever rendered) and the comment
    /// lines that explain it are removed by `strip_capdag_dependency_source`
    /// before comparing, so a render that differs only in how it reaches capdag
    /// is not a contract difference. The VERSION on that line is still read
    /// first, and the ordering rule above still applies to it.
    fn assert_stub_matches(language: &str, dest: &str, vendored: &str, canonical: &str) {
        if vendored == canonical {
            return;
        }
        // The pin is read from the dependency line BEFORE that line is
        // stripped — the ordering rule below must keep seeing it.
        let (vendored_pins, vendored_rest) = split_pin(vendored);
        let (canonical_pins, canonical_rest) = split_pin(canonical);
        let vendored_rest = strip_capdag_dependency_source(dest, &vendored_rest);
        let canonical_rest = strip_capdag_dependency_source(dest, &canonical_rest);
        assert_eq!(
            vendored_rest, canonical_rest,
            "{language}: vendored {dest} differs from the canonical bytes in more than the \
             capdag dependency source and the stamped version pins — re-vendor the stubs"
        );
        assert!(
            !vendored_pins.is_empty() && vendored_pins.len() == canonical_pins.len(),
            "{language}: vendored {dest} differs from the canonical bytes and the two sides \
             do not carry the same version pins to explain it — re-vendor the stubs"
        );
        for (vendored_pin, canonical_pin) in vendored_pins.iter().zip(canonical_pins.iter()) {
            assert!(
                vendored_pin <= canonical_pin,
                "{language}: vendored {dest} pins {} but the canonical stub is at {} — a stub \
                 may lag a release, never precede one",
                join_version(vendored_pin),
                join_version(canonical_pin)
            );
        }
    }

    /// A line whose dotted triple is a STAMPED version, not contract: one
    /// that names capdag (the dependency pin, in any language's syntax —
    /// `tag = "v1.2.3"` (Cargo), `capdag-go v1.2.3` (go.mod), `from: "1.2.3"`
    /// (SwiftPM)), or the stub's own version — a manifest's `version = "…"`
    /// line, a CapManifest `version: "…"` / `version="…"` argument, or a
    /// bare positional `"N.N.N",` (the stub repo's release, stamped by the
    /// templates so a scaffolded cartridge carries an accurate version). All
    /// move on every release and none says anything about the stub.
    fn is_pin_line(line: &str) -> bool {
        if line.contains("capdag") || line.trim().starts_with("version") {
            return true;
        }
        // A bare positional version argument: a quoted dotted triple and
        // nothing else on the line (the Go stub's manifest constructor).
        let bare = line.trim().trim_end_matches(',').trim();
        bare.len() >= 2
            && bare.starts_with('"')
            && bare.ends_with('"')
            && first_triple(bare).is_some_and(|(_, at)| at.start == 1 && at.end == bare.len() - 1)
    }

    /// Split a stub file into its version pins (in order) and everything
    /// else. Rather than teach this several grammars, the first dotted-triple
    /// on every pin line IS a pin.
    fn split_pin(text: &str) -> (Vec<Vec<u64>>, String) {
        let mut pins = Vec::new();
        let mut rest = String::with_capacity(text.len());
        for line in text.lines() {
            let mut kept = line.to_string();
            if is_pin_line(line) {
                if let Some((version, at)) = first_triple(line) {
                    pins.push(version);
                    kept.replace_range(at.clone(), "<pin>");
                }
            }
            rest.push_str(&kept);
            rest.push('\n');
        }
        (pins, rest)
    }

    /// Whether `dest` is a language manifest whose capdag dependency source
    /// may legitimately differ between renders (path vs tag vs version).
    fn is_dependency_manifest(dest: &str) -> bool {
        dest.ends_with("Cargo.toml") || dest.ends_with("go.mod") || dest.ends_with("Package.swift")
    }

    /// Whether a manifest line is the capdag dependency SOURCE: a path, git
    /// tag, module version or SwiftPM `from:` naming capdag.
    fn is_capdag_dependency_source(line: &str) -> bool {
        let t = line.trim();
        t.contains("capdag")
            && (t.contains("path")
                || t.contains("git =")
                || t.contains("tag =")
                || t.contains("url:")
                || t.contains("from:")
                || t.starts_with("require ")
                || t.starts_with("replace "))
    }

    /// Strip the capdag dependency source from a manifest: the dependency
    /// line(s) themselves, the comment lines that explain them, and blank
    /// lines (the templates' conditional blocks differ in spacing). Every
    /// other file is returned untouched — only manifests have a source to
    /// differ in.
    fn strip_capdag_dependency_source(dest: &str, text: &str) -> String {
        if !is_dependency_manifest(dest) {
            return text.to_string();
        }
        text.lines()
            .filter(|line| {
                let t = line.trim();
                !(t.is_empty()
                    || t.starts_with('#')
                    || t.starts_with("//")
                    || is_capdag_dependency_source(t))
            })
            .map(|line| format!("{line}\n"))
            .collect()
    }

    /// The first `N.N.N` in a line, with the range it occupies.
    fn first_triple(line: &str) -> Option<(Vec<u64>, std::ops::Range<usize>)> {
        let bytes = line.as_bytes();
        let mut index = 0;
        while index < bytes.len() {
            if !bytes[index].is_ascii_digit() {
                index += 1;
                continue;
            }
            let start = index;
            while index < bytes.len() && (bytes[index].is_ascii_digit() || bytes[index] == b'.') {
                index += 1;
            }
            let candidate = &line[start..index];
            let parts: Vec<&str> = candidate.split('.').collect();
            if parts.len() == 3 {
                if let Ok(numbers) = parts
                    .iter()
                    .map(|part| part.parse::<u64>())
                    .collect::<Result<Vec<u64>, _>>()
                {
                    return Some((numbers, start..index));
                }
            }
        }
        None
    }

    fn join_version(version: &[u64]) -> String {
        version
            .iter()
            .map(u64::to_string)
            .collect::<Vec<String>>()
            .join(".")
    }

    #[test]
    fn test7160_vendored_stub_contract_matches_the_canonical_source() {
        // Locate the canonical stubs relative to this mirror inside the
        // workspace. Absent (a standalone checkout of capdag), there is nothing
        // to compare against and the vendored copy IS the contract — that is not
        // a skip to hide behind, it is the only meaningful statement available.
        let stub_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("the capdag crate always has a parent directory")
            .join("capdag-stub-cartridges");
        let canonical = stub_root.join("stubs.json");
        let Ok(raw) = fs::read(&canonical) else {
            eprintln!(
                "canonical stubs not present at {} (standalone checkout)",
                canonical.display()
            );
            return;
        };
        let contract: serde_json::Value =
            serde_json::from_slice(&raw).expect("canonical stubs.json does not parse");

        assert_eq!(
            contract["contract_version"].as_u64(),
            Some(u64::from(stubs_generated::STUB_CONTRACT_VERSION)),
            "vendored contract version differs from canonical — re-vendor the stubs"
        );
        assert_eq!(
            contract["placeholder"].as_str(),
            Some(stubs_generated::STUB_PLACEHOLDER)
        );
        let languages = contract["languages"]
            .as_object()
            .expect("canonical `languages` is not an object");
        assert_eq!(
            languages.len(),
            stub_languages().len(),
            "vendored language count differs from canonical — re-vendor the stubs"
        );

        for vendored in stub_languages() {
            let spec = languages.get(vendored.id).unwrap_or_else(|| {
                panic!("vendored language {} is not in the canonical contract", vendored.id)
            });
            assert_eq!(spec["flag"].as_str(), Some(vendored.flag), "{}", vendored.id);
            assert_eq!(spec["entry"].as_str(), Some(vendored.entry), "{}", vendored.id);
            let declared = spec["files"]
                .as_array()
                .unwrap_or_else(|| panic!("{}: canonical `files` is not a list", vendored.id));
            assert_eq!(declared.len(), vendored.files.len(), "{}", vendored.id);
            for (declared, got) in declared.iter().zip(vendored.files.iter()) {
                let source = declared["source"]
                    .as_str()
                    .unwrap_or_else(|| panic!("{}: a canonical file declares no source", vendored.id));
                let want = fs::read_to_string(stub_root.join(source))
                    .unwrap_or_else(|e| panic!("reading canonical {source}: {e}"));
                assert_eq!(declared["dest"].as_str(), Some(got.dest), "{}", vendored.id);
                assert_eq!(declared["executable"].as_bool(), Some(got.executable), "{}", vendored.id);
                assert_stub_matches(vendored.id, got.dest, got.contents, &want);
            }
        }
    }
}
