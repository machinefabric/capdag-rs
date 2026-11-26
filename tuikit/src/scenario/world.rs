//! A world: everything a scenario host serves the capdag command, with no
//! fabric, cartridge or network behind it.
//!
//! Its fabric is a snapshot of a real one (`scenarios/fabric/*.json`, written
//! by `capdag-tuikit world fabric` from what a published fabric holds), so
//! every cap, media type and alias a command shows is one the fabric
//! publishes. A snapshot keeps every cap and alias and only the media types
//! the command reads; its manifest names only what it holds. The rest is the
//! world's own: the cartridges bundled, installed, refused and in the
//! registry, the files in the working directory, and a script for each run
//! the world's machines make.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use capdag::fabric::alias::StoredAlias;
use capdag::{Cap, FabricRegistry, RegistryConfig, StoredMediaDef};
use serde::{Deserialize, Serialize};

use super::script::RunScript;

/// A world, as `scenarios/worlds/<name>.json` declares it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct World {
    /// What the world is for, in a sentence.
    pub description: String,
    /// The fabric snapshot it serves: a file of `scenarios/fabric/`.
    pub fabric: String,
    /// When given, the fabric cannot be reached, for this reason, and nothing
    /// of it is cached.
    #[serde(default)]
    pub unreachable: Option<String>,
    /// The cartridge registry the command is bound to; none for a build bound
    /// to none.
    #[serde(default)]
    pub cartridge_registry: Option<String>,
    /// The cartridges bundled with the command.
    #[serde(default)]
    pub bundled: Vec<WorldCartridge>,
    /// The cartridges installed in the per-user folder.
    #[serde(default)]
    pub installed: Vec<WorldCartridge>,
    /// Installed cartridges discovery refuses.
    #[serde(default)]
    pub refused: Vec<RefusedCartridge>,
    /// The cartridges the registry offers, to install.
    #[serde(default)]
    pub store: Vec<WorldCartridge>,
    /// The files of the working directory, by path: their text.
    #[serde(default)]
    pub files: BTreeMap<String, String>,
    /// The machine files among `files` that are there to be refused — wrong
    /// notation, a cap no fabric has. Every other machine file plans against
    /// the world's fabric snapshot.
    #[serde(default)]
    pub refused_machines: BTreeSet<String>,
    /// How each run goes, by the caps its strand runs, in order: `words >
    /// shout`, or one cap's alias.
    #[serde(default)]
    pub runs: BTreeMap<String, RunScript>,
    /// What `capdag self update` finds.
    #[serde(default)]
    pub update: Option<UpdateScript>,
}

/// A cartridge of a world.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorldCartridge {
    pub id: String,
    pub version: String,
    /// The caps it provides, by alias.
    pub caps: Vec<String>,
    /// A dev cartridge (`capdag dev install`), not the registry's.
    #[serde(default)]
    pub dev: bool,
    /// How long installing it takes, in milliseconds at speed 1.
    #[serde(default)]
    pub install_ms: u64,
}

/// An installed cartridge discovery refuses.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RefusedCartridge {
    pub id: String,
    pub version: String,
    pub reason: String,
}

/// What `capdag self update` finds: a release to update to, or a failure.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum UpdateScript {
    Latest { version: String },
    Fails { reason: String },
}

/// A snapshot of a published fabric: what `capdag-tuikit world fabric`
/// writes, and what a world's registry holds.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FabricSnapshot {
    /// The fabric it was taken of.
    pub anchor: String,
    /// The manifest version it was taken at.
    pub manifest_version: u32,
    pub caps: Vec<Cap>,
    pub media: Vec<StoredMediaDef>,
    pub aliases: Vec<StoredAlias>,
}

impl FabricSnapshot {
    /// A registry holding exactly this snapshot, which fetches nothing.
    pub fn registry(&self) -> Arc<FabricRegistry> {
        let registry = FabricRegistry::new_for_test_with_config_and_version(
            RegistryConfig::new().with_anchor(&self.anchor),
            self.manifest_version,
        );
        for media in &self.media {
            registry.insert_cached_media_def_for_test(media.clone());
            registry.list_media_in_manifest_for_test(&media.urn, media.version);
        }
        registry.add_aliases_to_cache(self.aliases.clone());
        registry.add_caps_to_cache(self.caps.clone());
        registry.set_offline(true);
        Arc::new(registry)
    }

    /// The cap of alias `alias`.
    pub fn cap(&self, alias: &str) -> Option<&Cap> {
        let target = self.aliases.iter().find(|stored| stored.name == alias).map(|stored| stored.target.as_str());
        self.caps
            .iter()
            .find(|cap| cap.aliases.iter().any(|a| a == alias) || Some(cap.urn.to_string().as_str()) == target)
    }
}

/// The scenarios: worlds, their fabric snapshots, and journeys, under one
/// folder.
#[derive(Debug, Clone)]
pub struct Scenarios {
    pub dir: PathBuf,
}

impl Scenarios {
    /// The scenarios kept beside this crate's sources.
    pub fn bundled() -> Self {
        Self { dir: Path::new(env!("CARGO_MANIFEST_DIR")).join("scenarios") }
    }

    pub fn at(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    /// The names of the worlds, in order.
    pub fn worlds(&self) -> Result<Vec<String>, String> {
        names_in(&self.dir.join("worlds"))
    }

    /// The world `name`.
    pub fn world(&self, name: &str) -> Result<World, String> {
        let path = self.dir.join("worlds").join(format!("{name}.json"));
        let text = std::fs::read_to_string(&path).map_err(|e| {
            let known = self.worlds().map(|names| names.join(", ")).unwrap_or_default();
            format!("there is no world '{name}' ({}: {e}); the worlds are {known}", path.display())
        })?;
        serde_json::from_str(&text).map_err(|e| format!("the world '{name}' ({}) is not valid: {e}", path.display()))
    }

    /// The fabric snapshot `file`.
    pub fn fabric(&self, file: &str) -> Result<FabricSnapshot, String> {
        let path = self.dir.join("fabric").join(file);
        let text = std::fs::read_to_string(&path)
            .map_err(|e| format!("the fabric snapshot {} could not be read: {e}", path.display()))?;
        serde_json::from_str(&text).map_err(|e| format!("the fabric snapshot {} is not valid: {e}", path.display()))
    }
}

/// The names of the `.json` files in `dir`, without the extension, in order.
pub fn names_in(dir: &Path) -> Result<Vec<String>, String> {
    let entries = std::fs::read_dir(dir).map_err(|e| format!("{} could not be read: {e}", dir.display()))?;
    let mut names: Vec<String> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            (path.extension().and_then(|e| e.to_str()) == Some("json"))
                .then(|| path.file_stem().map(|stem| stem.to_string_lossy().into_owned()))
                .flatten()
        })
        .collect();
    names.sort();
    Ok(names)
}

impl World {
    /// Write the world's files into `dir`, the working directory of its runs.
    pub fn write_files(&self, dir: &Path) -> Result<(), String> {
        for (path, text) in &self.files {
            let file = dir.join(path);
            if let Some(parent) = file.parent() {
                std::fs::create_dir_all(parent).map_err(|e| format!("{} could not be made: {e}", parent.display()))?;
            }
            std::fs::write(&file, text).map_err(|e| format!("{} could not be written: {e}", file.display()))?;
        }
        Ok(())
    }
}
