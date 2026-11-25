//! The capdag binary's host: the engine itself, bound to what this build was
//! built for — its channel, cartridge registry and fabric.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;

use super::host::{CartridgeStore, Host, HostError, Removal, RunHost, RunSetup, Update};
use crate::bifaci::cartridge_repo::CartridgeChannel;
use crate::bifaci::live_feed::{StopInputsError, StopInputsOutcome};
use crate::cartridge_discovery::{discover_cartridges, DiscoveredCartridge, DiscoveryIdentity};
use crate::fabric::registry::{FabricRegistry, FabricRegistryError, RegistryConfig};
use crate::input_resolver::{CartridgeAdapterInvoker, RelaySwitchAdapterInvoker};
use crate::orchestrator::cli_runtime::CliRuntime;
use crate::orchestrator::cli_session::{finish_strand, spawn_strand, EmitTarget, PlannedStrand, StrandHandle, StrandReport};
use crate::orchestrator::executor::{BundledProvider, CartridgeAdapterGroups, CartridgeManager, HostableCartridge};
use crate::orchestrator::machine_plan::StrandPlan;
use crate::orchestrator::observe::{RunKey, RunObserver};
use crate::orchestrator::run_control::RunControl;
use crate::{Cap, CapGroup, CapManifest, CapUrn, ExecutionError, PipelineResult};

/// Distribution channel of this build. `MFR_CARTRIDGE_CHANNEL` is set by the
/// workspace build for every cargo invocation: a release build orchestrates
/// release cartridges only, a nightly build nightly ones — channels never
/// cross.
pub const BUILD_CHANNEL: CartridgeChannel = CartridgeChannel::from_build_env(env!("MFR_CARTRIDGE_CHANNEL"));

/// The cartridge registry this build is bound to, baked as the engine's is:
/// `None` for a dev build (dev and bundled cartridges only).
pub const BAKED_REGISTRY_URL: Option<&str> =
    crate::registry_url_from_build_env(option_env!("MFR_CARTRIDGE_REGISTRY_URL"));

/// The anchor of the fabric this build resolves caps, media and aliases
/// against, baked from the selected environment. The cartridge and fabric
/// registries move together (build.rs refuses a product build that bakes one
/// without the other).
pub const BAKED_FABRIC_REGISTRY_URL: Option<&str> = option_env!("CDG_FABRIC_REGISTRY_URL");

/// The engine, as this build is bound to it.
pub struct EngineHost {
    /// A registry opened already, which every command resolves against in
    /// place of the fabric: what a test of the commands gives them.
    registry: Option<Arc<FabricRegistry>>,
}

impl EngineHost {
    pub fn new() -> Self {
        Self { registry: None }
    }

    /// An engine whose commands resolve against `registry`, opened already.
    pub fn on_registry(registry: Arc<FabricRegistry>) -> Self {
        Self { registry: Some(registry) }
    }

    fn user_cartridge_dir(&self) -> Result<PathBuf, HostError> {
        user_cartridge_dir().ok_or(HostError::NoHome)
    }
}

impl Default for EngineHost {
    fn default() -> Self {
        Self::new()
    }
}

/// The per-user cartridge install root, `~/.capdag/cartridges`, in the
/// `{registry_slug}/{channel}/{name}/{version}/` tree every host uses.
pub fn user_cartridge_dir() -> Option<PathBuf> {
    dirs::home_dir().map(|home| home.join(".capdag").join("cartridges"))
}

/// The cartridges shipped beside this binary, in a packaged build; `None` for a
/// bare `cargo run`. The executable is canonicalized so a launcher symlink on
/// PATH (`/usr/bin/capdag`, Homebrew's `bin/capdag`) finds the tree beside the
/// real binary.
pub fn bundled_cartridges_dir() -> Option<PathBuf> {
    std::env::current_exe()
        .ok()
        .and_then(|exe| bundled_cartridges_dir_for_exe(&exe))
}

/// The `bundled-cartridges/` tree beside a launcher path, following symlinks.
pub fn bundled_cartridges_dir_for_exe(exe: &Path) -> Option<PathBuf> {
    std::fs::canonicalize(exe)
        .ok()
        .and_then(|real| real.parent().map(|dir| dir.join("bundled-cartridges")))
        .filter(|dir| dir.is_dir())
}

/// The directories of cartridge `id` under a cartridge root:
/// `{slug}/v{n}/{channel}/{id}/`.
fn cartridge_dirs(root: &Path, id: &str) -> Result<Vec<PathBuf>, HostError> {
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

fn subdirs(dir: &Path) -> Result<Vec<PathBuf>, HostError> {
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let entries = std::fs::read_dir(dir).map_err(|e| HostError::File {
        path: dir.to_path_buf(),
        done: "read",
        detail: e.to_string(),
    })?;
    Ok(entries.filter_map(Result::ok).map(|e| e.path()).filter(|p| p.is_dir()).collect())
}

#[async_trait]
impl Host for EngineHost {
    fn bind_fabric(&self, flag: Option<&str>) {
        // The registry reads one value from one place, the environment, so the
        // choice is written there before any registry is made.
        if let Some(url) = flag {
            std::env::set_var("CDG_FABRIC_REGISTRY_URL", url);
        } else if std::env::var_os("CDG_FABRIC_REGISTRY_URL").is_none() {
            if let Some(url) = BAKED_FABRIC_REGISTRY_URL {
                std::env::set_var("CDG_FABRIC_REGISTRY_URL", url);
            }
        }
    }

    fn fabric_anchor(&self) -> String {
        RegistryConfig::default().anchor
    }

    async fn open_fabric(&self, offline: bool, afresh: bool) -> Result<Arc<FabricRegistry>, FabricRegistryError> {
        if let Some(registry) = &self.registry {
            registry.set_offline(offline);
            return Ok(registry.clone());
        }
        let config = RegistryConfig::default().with_bypass_cache(afresh);
        let registry = FabricRegistry::with_config(config).await?;
        registry.set_offline(offline);
        // Machine notation resolves aliases synchronously, from memory: warm
        // them from the pinned manifest. A no-op once they are cached. Caps
        // are not warmed here: a command that needs every cap of the fabric
        // asks for them (`manifest_caps`), and the others fetch what they use.
        if !offline {
            registry.prefetch_manifest_aliases().await;
        }
        Ok(Arc::new(registry))
    }

    async fn clear_fabric_cache(&self) -> Result<PathBuf, FabricRegistryError> {
        // Cleared without opening a registry on it: a cache holding a corrupt
        // entry is one a registry refuses to open, and clearing is what that
        // refusal says to do. The folder is the one every other command reads:
        // the fabric this process is bound to, at the manifest version it is
        // built for.
        FabricRegistry::clear_cache_of(&RegistryConfig::default(), crate::FABRIC_MANIFEST_VERSION).await
    }

    fn cartridge_registry(&self) -> Option<String> {
        BAKED_REGISTRY_URL.map(str::to_string)
    }

    fn has_bundle(&self) -> bool {
        bundled_cartridges_dir().is_some()
    }

    async fn bundled_cartridges(&self) -> Result<Vec<HostableCartridge>, ExecutionError> {
        let Some(dir) = bundled_cartridges_dir() else {
            return Ok(Vec::new());
        };
        crate::orchestrator::executor::discover_bundled_cartridges(
            &dir,
            BUILD_CHANNEL,
            BAKED_REGISTRY_URL,
            crate::FABRIC_MANIFEST_VERSION,
        )
        .await
    }

    async fn bundled_providers(&self, cap: &CapUrn) -> Result<Vec<BundledProvider>, ExecutionError> {
        let Some(dir) = bundled_cartridges_dir() else {
            return Ok(Vec::new());
        };
        crate::orchestrator::executor::bundled_providers_of(
            &dir,
            BUILD_CHANNEL,
            BAKED_REGISTRY_URL,
            crate::FABRIC_MANIFEST_VERSION,
            cap,
        )
        .await
    }

    async fn bundled_adapter_groups(
        &self,
        registry: Arc<FabricRegistry>,
    ) -> Result<Vec<CartridgeAdapterGroups>, ExecutionError> {
        let Some(dir) = bundled_cartridges_dir() else {
            return Ok(Vec::new());
        };
        crate::orchestrator::executor::bundled_cartridge_adapter_groups(
            &dir,
            BUILD_CHANNEL,
            BAKED_REGISTRY_URL,
            crate::FABRIC_MANIFEST_VERSION,
            registry,
        )
        .await
    }

    async fn installed_cartridges(&self) -> Result<Vec<DiscoveredCartridge>, HostError> {
        let identity = DiscoveryIdentity {
            channel: BUILD_CHANNEL,
            registry_url: BAKED_REGISTRY_URL.map(str::to_string),
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
        discover_cartridges(&self.user_cartridge_dir()?, &identity)
            .await
            .map_err(HostError::engine)
    }

    async fn cartridge_store(&self) -> Result<Box<dyn CartridgeStore>, HostError> {
        let mut manager = CartridgeManager::new(
            self.user_cartridge_dir()?,
            BAKED_REGISTRY_URL.map(str::to_string),
            BUILD_CHANNEL,
            crate::FABRIC_MANIFEST_VERSION,
            Vec::new(),
            crate::bifaci::trust::content_trust(),
            RegistryConfig::default().anchor,
            crate::bifaci::trust::charter_trust(),
        );
        manager.init().await.map_err(HostError::engine)?;
        Ok(Box::new(EngineStore { manager }))
    }

    fn remove_cartridge(&self, id: &str) -> Result<Removal, HostError> {
        // `{slug}/v{n}/{channel}/{name}/`: every version of the cartridge, in
        // every registry and the dev slot.
        let removed = cartridge_dirs(&self.user_cartridge_dir()?, id)?;
        for dir in &removed {
            std::fs::remove_dir_all(dir).map_err(|e| HostError::File {
                path: dir.clone(),
                done: "removed",
                detail: e.to_string(),
            })?;
        }
        // Whether it is bundled is asked only of a name nothing was removed of.
        let bundled = match bundled_cartridges_dir() {
            Some(bundle) if removed.is_empty() => !cartridge_dirs(&bundle, id)?.is_empty(),
            _ => false,
        };
        Ok(Removal { removed, bundled })
    }

    fn find_dev_cap(&self, alias: &str) -> Result<Option<(Cap, PathBuf)>, HostError> {
        crate::dev::find_dev_cap_by_alias(&self.user_cartridge_dir()?, alias).map_err(HostError::engine)
    }

    fn dev_project_entry(&self, project: &Path) -> Result<PathBuf, HostError> {
        crate::dev::project_entry(project).map_err(HostError::engine)
    }

    fn dev_entry_manifest(&self, entry: &Path) -> Result<CapManifest, HostError> {
        crate::dev::read_entry_manifest(entry).map_err(HostError::engine)
    }

    fn install_dev_cartridge(&self, project: &Path, manifest: &CapManifest) -> Result<PathBuf, HostError> {
        crate::dev::stage_dev_cartridge(project, manifest, &self.user_cartridge_dir()?, crate::FABRIC_MANIFEST_VERSION)
            .map_err(HostError::engine)
    }

    async fn probe_cap_groups(&self, entry: &Path) -> Result<Vec<CapGroup>, HostError> {
        crate::probe_cartridge_cap_groups(entry).await.map_err(HostError::engine)
    }

    async fn self_update(&self, current: &str) -> Result<Update, HostError> {
        let updated = crate::self_update::update(current).await.map_err(HostError::engine)?;
        Ok(Update { from: updated.from, to: updated.to })
    }

    fn runtime(&self, setup: RunSetup) -> Result<Arc<dyn RunHost>, HostError> {
        let runtime = CliRuntime::new(
            self.user_cartridge_dir()?,
            if setup.offline { None } else { BAKED_REGISTRY_URL.map(str::to_string) },
            BUILD_CHANNEL,
            crate::FABRIC_MANIFEST_VERSION,
            setup.dev_binaries,
            bundled_cartridges_dir(),
            setup.registry,
            setup.trace,
            setup.persist_dir,
        );
        Ok(Arc::new(EngineRuns { runtime: Arc::new(runtime) }))
    }
}

/// The cartridge registry, through a cartridge manager bound to this build.
struct EngineStore {
    manager: CartridgeManager,
}

#[async_trait]
impl CartridgeStore for EngineStore {
    async fn has(&self, id: &str) -> bool {
        self.manager.registry_cartridge(id).await.is_some()
    }

    async fn providers_of(&self, cap_urn: &str) -> Vec<String> {
        self.manager
            .suggestions_for_cap(cap_urn)
            .await
            .into_iter()
            .map(|suggestion| suggestion.cartridge_id)
            .collect()
    }

    async fn install(&self, id: &str) -> Result<PathBuf, ExecutionError> {
        self.manager.get_cartridge_path(id).await
    }
}

/// One command's runs, hosted by the engine's CLI runtime.
struct EngineRuns {
    runtime: Arc<CliRuntime>,
}

#[async_trait]
impl RunHost for EngineRuns {
    fn spawn(
        &self,
        observer: &RunObserver,
        key: RunKey,
        planned: PlannedStrand,
        control: Arc<RunControl>,
    ) -> Result<StrandHandle, (StrandPlan, ExecutionError)> {
        spawn_strand(&self.runtime, observer, key, planned, None, control)
    }

    fn finish(
        &self,
        strand: &StrandPlan,
        result: Result<PipelineResult, ExecutionError>,
        target: &EmitTarget,
        stdout: &mut dyn std::io::Write,
    ) -> StrandReport {
        finish_strand(strand, result, target, stdout, &self.runtime)
    }

    async fn stop_live_inputs(&self) -> Result<StopInputsOutcome, StopInputsError> {
        self.runtime.stop_live_inputs().await
    }

    async fn inspect(&self, id: &str, cartridge: HostableCartridge, file: &Path) -> Result<Option<Vec<String>>, HostError> {
        let switch = self.runtime.host_inspector(id, cartridge).await.map_err(HostError::engine)?;
        RelaySwitchAdapterInvoker::new(switch)
            .invoke_adapter_selection(id, file)
            .await
            .map_err(HostError::engine)
    }
}

#[cfg(test)]
mod packaging_tests {
    //! What the OS packages (deb, rpm, Homebrew) depend on.
    use super::bundled_cartridges_dir_for_exe;
    use std::fs;

    /// TEST1902: a launcher symlink — `/usr/bin/capdag` → `/opt/capdag/capdag`,
    /// Homebrew's `bin/capdag` → `libexec/capdag` — finds the real bundle's
    /// `bundled-cartridges/`, not one beside the symlink.
    #[cfg(unix)]
    #[test]
    fn test1902_cartridges_resolve_through_launcher_symlink() {
        use std::os::unix::fs::symlink;
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("opt/capdag");
        fs::create_dir_all(real.join("bundled-cartridges")).unwrap();
        fs::write(real.join("capdag"), b"binary").unwrap();
        let bindir = tmp.path().join("usr/bin");
        fs::create_dir_all(&bindir).unwrap();
        let link = bindir.join("capdag");
        symlink(real.join("capdag"), &link).unwrap();

        let got = bundled_cartridges_dir_for_exe(&link)
            .expect("bundled-cartridges/ must resolve through the launcher symlink");
        assert_eq!(
            fs::canonicalize(&got).unwrap(),
            fs::canonicalize(real.join("bundled-cartridges")).unwrap(),
        );
    }

    /// TEST1903: no `bundled-cartridges/` beside the binary is `None`: a bare
    /// cargo build, whose discovery skips the bundle.
    #[test]
    fn test1903_no_bundled_cartridges_dir_when_absent() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("capdag"), b"binary").unwrap();
        assert!(bundled_cartridges_dir_for_exe(&tmp.path().join("capdag")).is_none());
    }
}
