//! What the capdag command asks of the engine it runs on.
//!
//! The commands decide everything a person sees: which command a line names,
//! what is asked and what is shown, and every word of it. What a command needs
//! done by the engine — the fabric opened, cartridges found, installed, started
//! and run, a release fetched — it asks of a [`Host`], and a run of a machine
//! of the [`RunHost`] the host makes for it. What a host answers are capdag's
//! own types, and a failure is the engine's own error, which the command words.
//!
//! The capdag binary's host is the engine itself (capdag-cli's `EngineHost`).
//! Work on a registry the host has opened — parsing and resolving machine
//! notation, planning, naming caps and media — and on the files a person names
//! (machines, inputs, the results a run writes) is the command's own.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;

use capdag::bifaci::live_feed::{StopInputsError, StopInputsOutcome};
use capdag::cartridge_discovery::DiscoveredCartridge;
use capdag::fabric::registry::{FabricRegistry, FabricRegistryError};
use capdag::orchestrator::cli_session::{EmitTarget, PlannedStrand, StrandHandle, StrandReport};
use capdag::orchestrator::executor::{BundledProvider, CartridgeAdapterGroups, HostableCartridge};
use capdag::orchestrator::machine_plan::StrandPlan;
use capdag::orchestrator::observe::{RunKey, RunObserver};
use capdag::orchestrator::run_control::RunControl;
use capdag::{Cap, CapGroup, CapManifest, CapUrn, ExecutionError, PipelineResult};

/// Why a host could not do what it was asked. The command says what it was
/// doing; this says why it could not.
#[derive(Debug, thiserror::Error)]
pub enum HostError {
    /// There is no home directory, which every cartridge folder is under.
    #[error("there is no home directory to keep cartridges in")]
    NoHome,
    /// A file or directory of the host's could not be read or changed: what
    /// was done to it (`read`, `removed`), and why it failed.
    #[error("{} could not be {done}: {detail}", path.display())]
    File { path: PathBuf, done: &'static str, detail: String },
    /// The engine failed, and said why.
    #[error("{0}")]
    Engine(String),
}

impl HostError {
    pub fn engine(error: impl std::fmt::Display) -> Self {
        HostError::Engine(error.to_string())
    }
}

/// What a cartridge removed from the per-user cartridge folder was.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Removal {
    /// Its directories removed: every version, in every registry and the dev slot.
    pub removed: Vec<PathBuf>,
    /// When nothing was removed: whether a cartridge of its id is bundled with
    /// the command, which cannot be removed.
    pub bundled: bool,
}

/// A replacement of the command by its channel's latest release.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Update {
    pub from: String,
    pub to: String,
}

/// How a run's cartridges are hosted: what a [`RunHost`] is made for.
pub struct RunSetup {
    /// The fabric the run's caps resolve against.
    pub registry: Arc<FabricRegistry>,
    /// `--offline`: no cartridge is fetched from the registry.
    pub offline: bool,
    /// `--dev-bin`: cartridge binaries, and directories of them, to use as well.
    pub dev_binaries: Vec<PathBuf>,
    /// `--trace`: where each segment's protocol trace is written.
    pub trace: Option<Arc<capdag::ProtocolTraceSink>>,
    /// Where the run's results are written.
    pub persist_dir: PathBuf,
}

/// The engine the capdag command runs on.
#[async_trait]
pub trait Host: Send + Sync {
    /// Point this process at a fabric: the one `--fabric` names when given,
    /// else the one the environment names, else the one the host is bound to.
    fn bind_fabric(&self, flag: Option<&str>);

    /// Where the fabric this process is bound to is read from.
    fn fabric_anchor(&self) -> String;

    /// The fabric, opened. `afresh` fetches its manifest and everything it
    /// pins anew; `offline` fetches nothing, and what is not cached is an error
    /// naming it.
    async fn open_fabric(&self, offline: bool, afresh: bool) -> Result<Arc<FabricRegistry>, FabricRegistryError>;

    /// Remove the local copy of the fabric this process reads, without opening
    /// it: answers the folder it was in.
    async fn clear_fabric_cache(&self) -> Result<PathBuf, FabricRegistryError>;

    /// The cartridge registry the host installs from; `None` for a build bound
    /// to none, which runs bundled and dev cartridges only.
    fn cartridge_registry(&self) -> Option<String>;

    /// Whether cartridges are bundled with the command: a packaged build has a
    /// bundle, a bare build none.
    fn has_bundle(&self) -> bool;

    /// The cartridges bundled with the command, each held to the build's signed
    /// bundle manifest.
    async fn bundled_cartridges(&self) -> Result<Vec<HostableCartridge>, ExecutionError>;

    /// The bundled cartridges that declare `cap`.
    async fn bundled_providers(&self, cap: &CapUrn) -> Result<Vec<BundledProvider>, ExecutionError>;

    /// The bundled cartridges with the adapter groups each inspects files by,
    /// and those refused for conflicting with another's.
    async fn bundled_adapter_groups(
        &self,
        registry: Arc<FabricRegistry>,
    ) -> Result<Vec<CartridgeAdapterGroups>, ExecutionError>;

    /// The cartridges installed in the per-user cartridge folder (from the
    /// registry, and dev cartridges), and those discovery refused.
    async fn installed_cartridges(&self) -> Result<Vec<DiscoveredCartridge>, HostError>;

    /// The cartridge registry's store, its manifest synced and verified.
    async fn cartridge_store(&self) -> Result<Box<dyn CartridgeStore>, HostError>;

    /// Remove every installed version of cartridge `id`.
    fn remove_cartridge(&self, id: &str) -> Result<Removal, HostError>;

    /// The dev cartridge whose cap has alias `alias`, and its directory.
    fn find_dev_cap(&self, alias: &str) -> Result<Option<(Cap, PathBuf)>, HostError>;

    /// A cartridge project's built entry point.
    fn dev_project_entry(&self, project: &Path) -> Result<PathBuf, HostError>;

    /// What a built entry point says it provides: it is started and asked.
    fn dev_entry_manifest(&self, entry: &Path) -> Result<CapManifest, HostError>;

    /// Install a built cartridge project under the dev slot, replacing what was
    /// there: answers its version directory.
    fn install_dev_cartridge(&self, project: &Path, manifest: &CapManifest) -> Result<PathBuf, HostError>;

    /// What a staged cartridge advertises: it is started once for its HELLO.
    async fn probe_cap_groups(&self, entry: &Path) -> Result<Vec<CapGroup>, HostError>;

    /// Replace the command with its channel's latest release, installed only
    /// when the release chains to this build's roots.
    async fn self_update(&self, current: &str) -> Result<Update, HostError>;

    /// A host for the cartridges of one command's runs and inspections.
    fn runtime(&self, setup: RunSetup) -> Result<Arc<dyn RunHost>, HostError>;
}

/// The cartridge registry's store: what it holds, and installing from it.
#[async_trait]
pub trait CartridgeStore: Send + Sync {
    /// Whether the registry has a cartridge of this id, in this channel.
    async fn has(&self, id: &str) -> bool;

    /// The registry's cartridges that provide `cap_urn`, the likeliest first.
    async fn providers_of(&self, cap_urn: &str) -> Vec<String>;

    /// Download, verify and install cartridge `id`: answers its entry point.
    async fn install(&self, id: &str) -> Result<PathBuf, ExecutionError>;
}

/// The host of one command's runs: their cartridges started, the strands run
/// on them, and what they produce written.
#[async_trait]
pub trait RunHost: Send + Sync {
    /// Start one strand of one run, every signal it gives reported to
    /// `observer` under `key`, stopped and aborted through `control`.
    fn spawn(
        &self,
        observer: &RunObserver,
        key: RunKey,
        planned: PlannedStrand,
        control: Arc<RunControl>,
    ) -> Result<StrandHandle, (StrandPlan, ExecutionError)>;

    /// A strand that ended: its results written as `target` says (a single
    /// value on `stdout` when the target allows), and how it went.
    fn finish(
        &self,
        strand: &StrandPlan,
        result: Result<PipelineResult, ExecutionError>,
        target: &EmitTarget,
        stdout: &mut dyn std::io::Write,
    ) -> StrandReport;

    /// Close the live inputs of the runs: their feeds stop, and what they gave
    /// is finished.
    async fn stop_live_inputs(&self) -> Result<StopInputsOutcome, StopInputsError>;

    /// Ask cartridge `id`, which inspects files like `file`, what `file` is:
    /// the media it proves, or `None` when it proves none.
    async fn inspect(&self, id: &str, cartridge: HostableCartridge, file: &Path) -> Result<Option<Vec<String>>, HostError>;
}
