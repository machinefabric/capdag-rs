//! The cartridges that inspect a file's content, asked the way the desktop
//! clients ask theirs.
//!
//! A cartridge volunteers to inspect files through the `adapter_urns` of its
//! cap groups. Nothing is read or started until a command detects a file:
//! then the bundled cartridges' groups are read from the build's signed bundle
//! manifest, and each installed cartridge is started once for its manifest, as
//! `capdag cartridge list` starts it. A cartridge is started to inspect a file
//! only when one of the file's candidate media falls under its adapter, and it
//! is started on the run's own switch, so a run that needs it then finds it
//! there.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::OnceCell;

use super::cartridge;
use super::ctx;
use super::error::{CliError, CliResult};
use crate::cartridge_discovery::DiscoveredCartridge;
use crate::fabric::registry::FabricRegistry;
use crate::input_resolver::{
    AdapterSource, CartridgeAdapterInvoker, InputResolverError, MediaAdapterRegistry,
    RelaySwitchAdapterInvoker,
};
use crate::orchestrator::cli_runtime::CliRuntime;
use crate::orchestrator::executor::HostableCartridge;
use crate::MediaUrn;

/// The file inspection of one command: the cartridges that inspect files,
/// known on the first file the command detects, and the runtime they are
/// started on.
pub struct Inspection {
    runtime: Arc<CliRuntime>,
    registry: Arc<FabricRegistry>,
    known: OnceCell<Inspectors>,
}

/// An adapter group that conflicts with one registered before it, and so is
/// not registered: what a file under it cannot be told without.
struct Refused {
    adapter: MediaUrn,
    cartridge_id: String,
    reason: String,
}

/// The inspecting cartridges, by the id the switch knows each by.
struct Inspectors {
    adapters: MediaAdapterRegistry,
    cartridges: HashMap<String, HostableCartridge>,
    refused: Vec<Refused>,
    runtime: Arc<CliRuntime>,
    /// Why asking a cartridge failed, by cartridge: a cartridge that cannot be
    /// asked claims nothing, and the file's detection then fails; this is what
    /// it says why.
    failures: Mutex<Vec<String>>,
}

impl Inspection {
    pub fn new(runtime: Arc<CliRuntime>, registry: Arc<FabricRegistry>) -> Self {
        Self { runtime, registry, known: OnceCell::new() }
    }

    /// The media `file` is: [`crate::detect_file_media`] with the inspecting
    /// cartridges as its adapters.
    pub async fn detect(&self, file: &Path) -> CliResult<crate::FileMedia> {
        let inspectors = self.known.get_or_try_init(|| self.discover()).await?;
        inspectors.refuse_if_under_refused(file, &self.registry).await?;
        // What went wrong asking about an earlier file is not this one's.
        inspectors.failures.lock().expect("the inspection failures' lock is poisoned").clear();
        crate::detect_file_media(
            file,
            &self.registry,
            inspectors,
            inspectors,
            &crate::ValueAdapterRegistry::new(),
        )
        .await
        .map_err(|e| inspectors.explained(file, e))
    }

    /// Every bundled and installed cartridge's adapter groups, registered in
    /// that order: of two that conflict, the second is refused, as the desktop
    /// clients refuse it.
    async fn discover(&self) -> CliResult<Inspectors> {
        let mut found: Vec<(String, HostableCartridge)> = Vec::new();
        if let Some(dir) = ctx::bundled_cartridges_dir() {
            let bundled = crate::orchestrator::executor::discover_bundled_cartridges(
                &dir,
                ctx::BUILD_CHANNEL,
                ctx::BAKED_REGISTRY_URL,
                crate::FABRIC_MANIFEST_VERSION,
            )
            .await
            .map_err(|e| CliError::failed("the cartridges bundled with capdag could not be read").context(e.to_string()))?;
            for hostable in bundled {
                let (id, _, _) = hostable
                    .identity
                    .clone()
                    .expect("bundled discovery names every cartridge it returns");
                found.push((id, hostable));
            }
        }
        for discovered in cartridge::installed().await? {
            // A cartridge discovery refuses is never run, so it inspects nothing;
            // `capdag cartridge list` says why it was refused.
            if let DiscoveredCartridge::Directory { entry_point, id, cap_groups, .. } = discovered {
                found.push((id, HostableCartridge { entry_point, identity: None, cap_groups, bundle_sha256: None }));
            }
        }

        let mut adapters = MediaAdapterRegistry::new(self.registry.clone());
        let mut cartridges = HashMap::new();
        let mut refused = Vec::new();
        for (id, hostable) in found {
            let mut inspects = false;
            for group in &hostable.cap_groups {
                if group.adapter_urns.is_empty() {
                    continue;
                }
                match adapters.register_cap_group(&group.name, &group.adapter_urns, &id) {
                    Ok(()) => inspects = true,
                    Err(error) => {
                        let adapter = MediaUrn::from_string(&error.new_adapter_urn).map_err(|e| {
                            CliError::failed(format!("the cartridge '{id}' declares an adapter that is no media URN"))
                                .context(e.to_string())
                        })?;
                        refused.push(Refused { adapter, cartridge_id: id.clone(), reason: error.to_string() });
                    }
                }
            }
            // The first of a cartridge's copies is the one asked (a cartridge
            // both bundled and installed is the bundle's, as a run takes it).
            if inspects && !cartridges.contains_key(&id) {
                cartridges.insert(id, hostable);
            }
        }
        Ok(Inspectors { adapters, cartridges, refused, runtime: self.runtime.clone(), failures: Mutex::new(Vec::new()) })
    }
}

impl Inspectors {
    /// A file whose candidates fall under a refused adapter group cannot be
    /// told as the cartridges would tell it: an error naming both cartridges.
    async fn refuse_if_under_refused(&self, file: &Path, registry: &FabricRegistry) -> CliResult<()> {
        if self.refused.is_empty() {
            return Ok(());
        }
        let Some(extension) = file.extension().and_then(|e| e.to_str()) else {
            return Ok(());
        };
        let candidates = registry.file_media_for_extension(extension).await.map_err(ctx::registry_failure)?;
        for refused in &self.refused {
            let under = candidates.iter().any(|def| {
                MediaUrn::from_string(&def.urn).is_ok_and(|urn| urn.conforms_to(&refused.adapter).unwrap_or(false))
            });
            if under {
                return Err(CliError::failed(format!(
                    "two cartridges inspect files like '{}', and capdag cannot tell which to ask",
                    file.display()
                ))
                .context(refused.reason.clone())
                .hint(format!("remove one of them: capdag cartridge remove {}", refused.cartridge_id)));
            }
        }
        Ok(())
    }

    /// A detection error, with why each cartridge that could not be asked
    /// could not.
    fn explained(&self, file: &Path, error: InputResolverError) -> CliError {
        let mut failure = super::inputs::detection_failure(file, error);
        for reason in self.failures.lock().expect("the inspection failures' lock is poisoned").iter() {
            failure = failure.context(reason.clone());
        }
        failure
    }
}

impl AdapterSource for Inspectors {
    fn adapters_for(&self, candidates: &[MediaUrn]) -> Vec<(String, MediaUrn)> {
        self.adapters.adapters_for_candidates(candidates)
    }
}

#[async_trait]
impl CartridgeAdapterInvoker for Inspectors {
    async fn invoke_adapter_selection(
        &self,
        cartridge_id: &str,
        file_path: &Path,
    ) -> Result<Option<Vec<String>>, InputResolverError> {
        let hostable = self
            .cartridges
            .get(cartridge_id)
            .cloned()
            .expect("every cartridge an adapter is registered for is known");
        let failed = |reason: String| {
            self.failures
                .lock()
                .expect("the inspection failures' lock is poisoned")
                .push(format!("the cartridge '{cartridge_id}' could not inspect it: {reason}"));
            InputResolverError::InspectionFailed { path: file_path.to_path_buf(), reason }
        };
        let switch = self.runtime.host_inspector(cartridge_id, hostable).await.map_err(|e| failed(e.to_string()))?;
        RelaySwitchAdapterInvoker::new(switch)
            .invoke_adapter_selection(cartridge_id, file_path)
            .await
            .map_err(|e| failed(e.to_string()))
    }
}
