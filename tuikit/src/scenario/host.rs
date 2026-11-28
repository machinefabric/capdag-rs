//! The scenario host: the capdag command's engine, served from a world.
//!
//! Every operation answers what the world says, with the engine's own types:
//! the fabric is the world's snapshot, the cartridges its cartridges, and a
//! run of a machine plays the world's script for the caps the strand runs
//! (`super::script`). A dev project is built or not as the world says, and a
//! staged cartridge asked for its caps answers with those of the world's
//! cartridge of its id and version. Nothing is fetched, started or installed.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use capdag::bifaci::cartridge_repo::CartridgeChannel;
use capdag::bifaci::live_feed::{StopInputsError, StopInputsOutcome};
use capdag::bifaci::relay_switch::{CartridgeAttachmentError, CartridgeAttachmentErrorKind};
use capdag::cartridge_discovery::DiscoveredCartridge;
use capdag::orchestrator::cli_session::{emit_strand, EmitTarget, PlannedStrand, StrandHandle, StrandReport};
use capdag::orchestrator::executor::{BundledProvider, CartridgeAdapterGroups, HostableCartridge};
use capdag::orchestrator::machine_plan::StrandPlan;
use capdag::orchestrator::observe::{RunKey, RunObserver};
use capdag::orchestrator::run_control::RunControl;
use capdag::orchestrator::RunArgumentLedger;
use capdag::planner::ExecutionNodeType;
use capdag::{Cap, CapGroup, CapManifest, CapUrn, ExecutionError, FabricRegistry, FabricRegistryError, PipelineResult};

use super::script::{self, Names, RunScript};
use super::world::{DevProject, FabricSnapshot, UpdateScript, World, WorldCartridge};
use crate::cli::host::{CartridgeStore, Host, HostError, Removal, RunHost, RunSetup, Update};

/// A world, served as the engine.
pub struct ScenarioHost {
    name: String,
    world: World,
    fabric: FabricSnapshot,
    registry: Arc<FabricRegistry>,
    /// Where the world's cartridges are said to be.
    root: PathBuf,
    /// How much faster than the scripts' waits runs go.
    speed: f64,
    /// The installed cartridges removed so far.
    removed: Mutex<Vec<String>>,
}

impl ScenarioHost {
    /// Serve world `name`, its cartridges said to be under `root`, its runs
    /// played `speed` times as fast as their scripts' waits.
    pub fn new(name: &str, world: World, fabric: FabricSnapshot, root: &Path, speed: f64) -> Result<Self, String> {
        if !(speed > 0.0) {
            return Err(format!("a speed is a number above 0, and {speed} is not"));
        }
        let registry = fabric.registry();
        let host = Self {
            name: name.to_string(),
            world,
            fabric,
            registry,
            root: root.to_path_buf(),
            speed,
            removed: Mutex::new(Vec::new()),
        };
        host.check_cartridges()?;
        Ok(host)
    }

    /// Every cap a world's cartridge provides is a cap of its fabric.
    fn check_cartridges(&self) -> Result<(), String> {
        for cartridge in self.world.bundled.iter().chain(&self.world.installed).chain(&self.world.store) {
            for alias in &cartridge.caps {
                if self.fabric.cap(alias).is_none() {
                    return Err(format!(
                        "the world '{}' gives {} the cap '{alias}', which its fabric does not have",
                        self.name, cartridge.id
                    ));
                }
            }
        }
        for (dir, project) in &self.world.dev_projects {
            if let DevProject::Built { caps, own_caps, .. } = project {
                if let Some(alias) = caps.iter().find(|alias| self.fabric.cap(alias).is_none()) {
                    return Err(format!(
                        "the world '{}' gives the project {dir} the cap '{alias}', which its fabric does not have; a cap of its own goes in own_caps",
                        self.name
                    ));
                }
                for own in own_caps {
                    CapUrn::from_string(&own.urn)
                        .map_err(|e| format!("the world '{}' gives the project {dir} the cap '{}': {e}", self.name, own.urn))?;
                }
            }
        }
        Ok(())
    }

    /// The world's project in the directory `project`, as `capdag dev install`
    /// is given it: `.`, `sentiment`, `./sentiment`.
    fn dev_project(&self, project: &Path) -> Result<&DevProject, HostError> {
        let parts: Vec<String> = project
            .components()
            .filter(|part| !matches!(part, std::path::Component::CurDir))
            .map(|part| part.as_os_str().to_string_lossy().into_owned())
            .collect();
        let key = if parts.is_empty() { ".".to_string() } else { parts.join("/") };
        self.world
            .dev_projects
            .get(&key)
            .ok_or_else(|| HostError::Engine(format!("{} holds no cartridge project", project.display())))
    }

    fn cap_groups(&self, cartridge: &WorldCartridge) -> Vec<CapGroup> {
        vec![CapGroup {
            name: cartridge.id.clone(),
            caps: cartridge.caps.iter().filter_map(|alias| self.fabric.cap(alias).cloned()).collect(),
            adapter_urns: Vec::new(),
        }]
    }

    fn cartridge_dir(&self, place: &str, cartridge: &WorldCartridge) -> PathBuf {
        self.root.join(place).join(&cartridge.id).join(&cartridge.version)
    }

    fn installed(&self) -> Vec<&WorldCartridge> {
        let removed = self.removed.lock().expect("the removed cartridges' lock is poisoned");
        self.world.installed.iter().filter(|c| !removed.contains(&c.id)).collect()
    }

    /// The script of a run of the caps `key` names.
    pub fn script(&self, key: &str) -> Option<&RunScript> {
        self.world.runs.get(key)
    }

    /// The registry the world's commands resolve against.
    pub fn registry(&self) -> Arc<FabricRegistry> {
        self.registry.clone()
    }
}

/// What a strand runs: its caps' aliases in the order they run, `a > b`.
pub fn run_key(strand: &StrandPlan, registry: &FabricRegistry) -> String {
    let plan = &strand.plan;
    let order: Vec<String> = match plan.topological_order() {
        Ok(nodes) => nodes.iter().map(|node| node.id.clone()).collect(),
        Err(_) => {
            let mut ids: Vec<String> = plan.nodes.keys().cloned().collect();
            ids.sort();
            ids
        }
    };
    order
        .iter()
        .filter_map(|id| match &plan.nodes[id].node_type {
            ExecutionNodeType::Cap { cap_urn, .. } => Some(crate::cli::names::cap_urn(registry, cap_urn)),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(" > ")
}

#[async_trait]
impl Host for ScenarioHost {
    fn bind_fabric(&self, _flag: Option<&str>) {
        // The world's fabric is the only one it has.
    }

    fn fabric_anchor(&self) -> String {
        self.fabric.anchor.clone()
    }

    async fn open_fabric(&self, _offline: bool, _afresh: bool) -> Result<Arc<FabricRegistry>, FabricRegistryError> {
        match &self.world.unreachable {
            Some(reason) => Err(FabricRegistryError::Charter(format!(
                "cannot reach {}/charter.json: {reason}",
                self.fabric.anchor.trim_end_matches('/')
            ))),
            None => Ok(self.registry.clone()),
        }
    }

    /// A world the network cannot reach has nothing of the fabric cached; a
    /// world it can reach is served from its snapshot, as from a cache.
    fn fabric_cached(&self) -> Result<bool, FabricRegistryError> {
        Ok(self.world.unreachable.is_none())
    }

    async fn clear_fabric_cache(&self) -> Result<PathBuf, FabricRegistryError> {
        Ok(self.root.join("fabric-cache"))
    }

    fn cartridge_registry(&self) -> Option<String> {
        self.world.cartridge_registry.clone()
    }

    fn has_bundle(&self) -> bool {
        !self.world.bundled.is_empty()
    }

    async fn bundled_cartridges(&self) -> Result<Vec<HostableCartridge>, ExecutionError> {
        Ok(self
            .world
            .bundled
            .iter()
            .map(|cartridge| HostableCartridge {
                entry_point: self.cartridge_dir("bundled-cartridges", cartridge).join("cartridge"),
                identity: Some((cartridge.id.clone(), cartridge.version.clone(), CartridgeChannel::Release)),
                cap_groups: self.cap_groups(cartridge),
                bundle_sha256: None,
            })
            .collect())
    }

    async fn bundled_providers(&self, cap: &CapUrn) -> Result<Vec<BundledProvider>, ExecutionError> {
        Ok(self
            .world
            .bundled
            .iter()
            .filter(|cartridge| {
                cartridge
                    .caps
                    .iter()
                    .filter_map(|alias| self.fabric.cap(alias))
                    .any(|provided| provided.urn.is_equivalent(cap))
            })
            .map(|cartridge| BundledProvider {
                id: cartridge.id.clone(),
                version: cartridge.version.clone(),
                entry_point: self.cartridge_dir("bundled-cartridges", cartridge).join("cartridge"),
            })
            .collect())
    }

    async fn bundled_adapter_groups(
        &self,
        _registry: Arc<FabricRegistry>,
    ) -> Result<Vec<CartridgeAdapterGroups>, ExecutionError> {
        Ok(self
            .world
            .bundled
            .iter()
            .map(|cartridge| CartridgeAdapterGroups {
                id: cartridge.id.clone(),
                version: cartridge.version.clone(),
                refused: Vec::new(),
            })
            .collect())
    }

    async fn installed_cartridges(&self) -> Result<Vec<DiscoveredCartridge>, HostError> {
        let mut found: Vec<DiscoveredCartridge> = self
            .installed()
            .into_iter()
            .map(|cartridge| {
                let version_dir = self.cartridge_dir(if cartridge.dev { "dev" } else { "installed" }, cartridge);
                DiscoveredCartridge::Directory {
                    entry_point: version_dir.join("cartridge"),
                    version_dir,
                    id: cartridge.id.clone(),
                    channel: CartridgeChannel::Release,
                    registry_url: if cartridge.dev { None } else { self.world.cartridge_registry.clone() },
                    version: cartridge.version.clone(),
                    cap_groups: self.cap_groups(cartridge),
                    bundle_sha256: None,
                }
            })
            .collect();
        for refused in &self.world.refused {
            found.push(DiscoveredCartridge::Incompatible {
                version_dir: self.root.join("installed").join(&refused.id).join(&refused.version),
                id: refused.id.clone(),
                channel: CartridgeChannel::Release,
                registry_url: self.world.cartridge_registry.clone(),
                version: refused.version.clone(),
                error: CartridgeAttachmentError {
                    kind: CartridgeAttachmentErrorKind::Incompatible,
                    message: refused.reason.clone(),
                    detected_at_unix_seconds: 0,
                },
            });
        }
        Ok(found)
    }

    async fn cartridge_store(&self) -> Result<Box<dyn CartridgeStore>, HostError> {
        Ok(Box::new(ScenarioStore {
            cartridges: self
                .world
                .store
                .iter()
                .map(|cartridge| {
                    let caps = cartridge.caps.iter().filter_map(|alias| self.fabric.cap(alias).map(|cap| cap.urn.clone())).collect();
                    (cartridge.clone(), caps, self.cartridge_dir("installed", cartridge).join("cartridge"))
                })
                .collect(),
            speed: self.speed,
        }))
    }

    fn remove_cartridge(&self, id: &str) -> Result<Removal, HostError> {
        let removed: Vec<PathBuf> = self
            .installed()
            .into_iter()
            .filter(|cartridge| cartridge.id == id)
            .map(|cartridge| self.root.join(if cartridge.dev { "dev" } else { "installed" }).join(&cartridge.id))
            .collect();
        if !removed.is_empty() {
            self.removed.lock().expect("the removed cartridges' lock is poisoned").push(id.to_string());
        }
        let bundled = removed.is_empty() && self.world.bundled.iter().any(|cartridge| cartridge.id == id);
        Ok(Removal { removed, bundled })
    }

    fn find_dev_cap(&self, alias: &str) -> Result<Option<(Cap, PathBuf)>, HostError> {
        for cartridge in self.installed().into_iter().filter(|cartridge| cartridge.dev) {
            if cartridge.caps.iter().any(|provided| provided == alias) {
                if let Some(cap) = self.fabric.cap(alias) {
                    return Ok(Some((cap.clone(), self.cartridge_dir("dev", cartridge))));
                }
            }
        }
        Ok(None)
    }

    fn dev_project_entry(&self, project: &Path) -> Result<PathBuf, HostError> {
        match self.dev_project(project)? {
            DevProject::Built { .. } => Ok(project.join("cartridge")),
            DevProject::Unbuilt(reason) => Err(HostError::Engine(reason.clone())),
        }
    }

    fn dev_entry_manifest(&self, entry: &Path) -> Result<CapManifest, HostError> {
        let project = entry.parent().unwrap_or(Path::new("."));
        let DevProject::Built { name, version, caps, own_caps } = self.dev_project(project)? else {
            return Err(HostError::Engine(format!("{} is not built", project.display())));
        };
        let mut declared: Vec<Cap> = caps.iter().filter_map(|alias| self.fabric.cap(alias).cloned()).collect();
        for own in own_caps {
            let urn = CapUrn::from_string(&own.urn).map_err(|e| HostError::Engine(format!("{}: {e}", own.urn)))?;
            declared.push(Cap::new(urn, own.title.clone(), own.aliases.clone()));
        }
        Ok(CapManifest::new(
            name.clone(),
            version.clone(),
            CartridgeChannel::Release,
            None,
            format!("The {name} cartridge"),
            vec![CapGroup { name: name.clone(), caps: declared, adapter_urns: Vec::new() }],
        ))
    }

    fn install_dev_cartridge(&self, project: &Path, manifest: &CapManifest) -> Result<PathBuf, HostError> {
        self.dev_project(project)?;
        Ok(self.root.join("dev").join(&manifest.name).join(&manifest.version))
    }

    async fn probe_cap_groups(&self, entry: &Path) -> Result<Vec<CapGroup>, HostError> {
        // A staged cartridge's entry is `…/{name}/{version}/{entry}`.
        let version_dir = entry.parent().unwrap_or(Path::new(""));
        let part = |path: &Path| path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
        let (name, version) = (part(version_dir.parent().unwrap_or(Path::new(""))), part(version_dir));
        if let Some(refused) = self.world.unstartable.iter().find(|c| c.id == name && c.version == version) {
            return Err(HostError::Engine(refused.reason.clone()));
        }
        match self
            .world
            .bundled
            .iter()
            .chain(&self.world.installed)
            .chain(&self.world.store)
            .find(|cartridge| cartridge.id == name && cartridge.version == version)
        {
            Some(cartridge) => Ok(self.cap_groups(cartridge)),
            None => Err(HostError::Engine(format!(
                "{} is {name} {version}, which is no cartridge of the world '{}': nothing answers when it is started",
                entry.display(),
                self.name
            ))),
        }
    }

    async fn self_update(&self, current: &str) -> Result<Update, HostError> {
        match &self.world.update {
            Some(UpdateScript::Latest { version }) => Ok(Update { from: current.to_string(), to: version.clone() }),
            Some(UpdateScript::Fails { reason }) => Err(HostError::Engine(reason.clone())),
            None => Err(HostError::Engine(format!("the world '{}' has no release to update to", self.name))),
        }
    }

    fn runtime(&self, setup: RunSetup) -> Result<Arc<dyn RunHost>, HostError> {
        // A world's runs are scripts: no cartridge binary is started and no
        // protocol is spoken, so neither can be given or traced.
        if let Some(binary) = setup.dev_binaries.first() {
            return Err(HostError::Engine(format!(
                "the world '{}' starts no cartridge binary, {} included: --dev-bin runs on the engine only",
                self.name,
                binary.display()
            )));
        }
        if setup.trace.is_some() {
            return Err(HostError::Engine(format!(
                "the world '{}' speaks no protocol to trace: --trace runs on the engine only",
                self.name
            )));
        }
        Ok(Arc::new(ScenarioRuns {
            world: self.name.clone(),
            runs: self.world.runs.clone(),
            registry: setup.registry,
            speed: self.speed,
        }))
    }
}

/// The registry's cartridges in a world.
struct ScenarioStore {
    cartridges: Vec<(WorldCartridge, Vec<CapUrn>, PathBuf)>,
    speed: f64,
}

#[async_trait]
impl CartridgeStore for ScenarioStore {
    async fn has(&self, id: &str) -> bool {
        self.cartridges.iter().any(|(cartridge, _, _)| cartridge.id == id)
    }

    async fn providers_of(&self, cap_urn: &str) -> Vec<String> {
        let Ok(wanted) = CapUrn::from_string(cap_urn) else {
            return Vec::new();
        };
        self.cartridges
            .iter()
            .filter(|(_, caps, _)| caps.iter().any(|cap| cap.is_equivalent(&wanted)))
            .map(|(cartridge, _, _)| cartridge.id.clone())
            .collect()
    }

    async fn install(&self, id: &str) -> Result<PathBuf, ExecutionError> {
        let (cartridge, _, entry) = self
            .cartridges
            .iter()
            .find(|(cartridge, _, _)| cartridge.id == id)
            .ok_or_else(|| ExecutionError::CartridgeDownloadFailed(format!("the registry has no cartridge {id}")))?;
        tokio::time::sleep(Duration::from_secs_f64(cartridge.install_ms as f64 / 1000.0 / self.speed)).await;
        Ok(entry.clone())
    }
}

/// One command's runs in a world: each strand plays the world's script for
/// the caps it runs.
struct ScenarioRuns {
    world: String,
    runs: std::collections::BTreeMap<String, RunScript>,
    registry: Arc<FabricRegistry>,
    speed: f64,
}

/// The first required argument of a step of `strand` that neither the machine
/// binds nor the run gives a value, refused as the step's cartridge refuses
/// it.
async fn missing_argument(
    registry: &Arc<FabricRegistry>,
    strand: &StrandPlan,
    given: &std::collections::HashMap<String, Vec<(String, Vec<u8>)>>,
) -> Result<(), ExecutionError> {
    let requirements = capdag::planner::MachinePlanBuilder::new(registry.clone())
        .analyze_plan_arguments(&strand.plan)
        .await
        .map_err(|e| ExecutionError::HostError(format!("the plan's arguments could not be read: {e}")))?;
    for step in &requirements.steps {
        let node = strand
            .plan
            .nodes
            .iter()
            .find(|(_, node)| matches!(&node.node_type, ExecutionNodeType::Cap { token_id, .. } if token_id == &step.token_id))
            .map(|(id, _)| id.as_str())
            .unwrap_or_default();
        let values = given.get(node).map(Vec::as_slice).unwrap_or_default();
        for slot in &step.slots {
            let open = slot.is_required && slot.resolution == capdag::planner::ArgumentResolution::RequiresUserInput;
            if open && !values.iter().any(|(urn, _)| urn == &slot.media_urn) {
                return Err(ExecutionError::StepFailed {
                    step_token_id: step.token_id.clone(),
                    source: Box::new(ExecutionError::CartridgeExecutionFailed {
                        cap_urn: step.cap_urn.clone(),
                        code: None,
                        class: capdag::failure::AttributionClass::Input,
                        details: format!("Missing required arg: {}", slot.media_urn),
                        arg_urn: Some(slot.media_urn.clone()),
                    }),
                });
            }
        }
    }
    Ok(())
}

#[async_trait]
impl RunHost for ScenarioRuns {
    fn spawn(
        &self,
        observer: &RunObserver,
        key: RunKey,
        planned: PlannedStrand,
        control: Arc<RunControl>,
    ) -> Result<StrandHandle, (StrandPlan, ExecutionError)> {
        let strand = planned.strand;
        let refused = |strand: StrandPlan, message: String| Err((strand, ExecutionError::HostError(message)));
        let run = run_key(&strand, &self.registry);
        let Some(script) = self.runs.get(&run).cloned() else {
            return refused(strand, format!("the world '{}' has no script for a run of {run}", self.world));
        };
        let names = match Names::of(&strand, &self.registry).and_then(|names| names.check(&script).map(|()| names)) {
            Ok(names) => names,
            Err(e) => return refused(strand, format!("the world '{}' scripts a run of {run} its plan cannot make: {e}", self.world)),
        };
        let given = planned.cap_arguments.clone();
        let ledger = match RunArgumentLedger::new(&strand.plan, planned.cap_arguments) {
            Ok(ledger) => Arc::new(ledger),
            Err(e) => return refused(strand, format!("argument values refused: {e}")),
        };
        let callbacks = observer.callbacks(key);
        let observer = observer.clone();
        let speed = self.speed;
        let registry = self.registry.clone();
        let task = tokio::spawn(async move {
            // A cartridge refuses a request that gives a required argument
            // no value: so does the world, before it plays anything.
            if let Err(refused) = missing_argument(&registry, &strand, &given).await {
                return (strand, Err(refused));
            }
            let result: Result<PipelineResult, ExecutionError> = script::play(script, names, callbacks, control, speed).await;
            if observer.lost() {
                return (
                    strand,
                    Err(ExecutionError::HostError(format!(
                        "what run {} strand {} said was lost: its observer went away while it ran",
                        key.run, key.strand
                    ))),
                );
            }
            (strand, result)
        });
        Ok(StrandHandle { key, ledger, task })
    }

    fn finish(
        &self,
        strand: &StrandPlan,
        result: Result<PipelineResult, ExecutionError>,
        target: &EmitTarget,
        stdout: &mut dyn std::io::Write,
    ) -> StrandReport {
        emit_strand(strand, result, target, stdout)
    }

    async fn stop_live_inputs(&self) -> Result<StopInputsOutcome, StopInputsError> {
        Err(StopInputsError::NothingToStop)
    }

    async fn inspect(
        &self,
        id: &str,
        _cartridge: HostableCartridge,
        file: &std::path::Path,
    ) -> Result<Option<Vec<String>>, HostError> {
        Err(HostError::Engine(format!(
            "the world '{}' starts no cartridge to inspect {}, {id} included",
            self.world,
            file.display()
        )))
    }
}
