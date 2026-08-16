//! DAG Execution Engine
//!
//! Executes a resolved DOT DAG by:
//! 1. Discovering and downloading cartridges that provide the required caps
//! 2. Connecting all cartridges to a single CartridgeHostRuntime
//! 3. Routing cap requests through a RelaySwitch
//! 4. Executing edge groups in topological order, streaming frames between caps
//!
//! Fan-in: multiple edges pointing to the same `(to, cap_urn)` are grouped and
//! executed as ONE cap invocation with multiple input streams. The cartridge handler
//! receives all streams and decides how to handle partial availability — it may
//! wait for all, use whatever arrives, or fail.
//!
//! Architecture:
//! ```text
//!   macino ←→ RelaySwitch ←→ RelaySlave ←→ CartridgeHostRuntime ←→ Cartridge A
//!                                                             ←→ Cartridge B
//!                                                             ←→ Cartridge C
//! ```

use super::stream_io::{PipelineLogFn, PipelineLogRecord, PipelineProgressTracker, TerminalMeta};
use super::types::{ResolvedEdge, ResolvedGraph};
use crate::{
    handshake, Cap, CapManifest, CapUrn, CartridgeHostRuntime, CartridgeRepo, FabricRegistry,
    Frame, FrameReader, FrameType, FrameWriter, Limits, MessageId, RelayNotifyCapabilitiesPayload,
    RelaySlave, RelaySwitch, DEFAULT_MAX_CHUNK,
};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

/// Detect the current platform in the format used by the registry (e.g., "darwin-arm64").
/// Thin alias over the shared [`crate::host_platform`] — the single source of
/// truth for the host {os}-{arch} string.
fn detect_platform() -> String {
    crate::host_platform()
}

fn platform_binary_name(cartridge_id: &str) -> String {
    if cfg!(target_os = "windows") {
        format!("{cartridge_id}.exe")
    } else {
        cartridge_id.to_string()
    }
}

/// Default cap-level activity timeout in seconds.
/// If a cartridge sends no frames (Chunk, Log, progress, peer requests) for this
/// duration, the executor aborts with `ExecutionError::ActivityTimeout`.
const DEFAULT_ACTIVITY_TIMEOUT_SECS: u64 = 120;

/// Cap metadata key for per-cap activity timeout override.
const ACTIVITY_TIMEOUT_METADATA_KEY: &str = "activity_timeout_secs";

/// How long the segment executor polls for a cap to become dispatchable before
/// failing hard. Cap registration is asynchronous — a freshly attached cartridge
/// host's RelayNotify (carrying its real cap_groups) lands some time after
/// `add_master` returns. On a long-lived switch (the engine) the cap is already
/// registered and the first probe succeeds, so this bound only ever elapses for a
/// genuinely unprovided cap.
const CAP_DISPATCH_READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
use crate::bifaci::local_socket::UnixStream;
use crate::planner::StepToken;
use tokio::io::{BufReader, BufWriter};
use tokio::process::Command;

/// Callback for reporting per-cap progress.
/// Parameters: (progress 0.0–1.0, cap URN string, human-readable message)
pub type CapProgressFn = Arc<dyn Fn(f32, &str, &str) + Send + Sync>;

/// Side-channel callback reporting a single cap's OWN completion fraction (the
/// mapper's un-mapped child value, before it is folded into the overall progress).
/// Parameters: (step_progress 0.0–1.0, cap URN string, step token_id). The token_id
/// is the stable per-step identity (`StrandStep.token_id`) of the reporting cap —
/// the unambiguous key a consumer uses to attribute this fraction to the exact
/// strand step (a cap URN alone is ambiguous when the same URN repeats). Attached
/// only to the per-cap group mappers so a consumer can persist each section's own
/// progress distinct from the whole-strand `overall` carried by `CapProgressFn`.
pub type CapStepProgressFn = Arc<dyn Fn(f32, &str, &str) + Send + Sync>;

/// Maps child progress [0.0, 1.0] into a parent range [base, base + weight].
///
/// This is the single progress mapping computation used everywhere:
/// - DAG execution: per-group subdivision
/// - ForEach plans: per-item subdivision
/// - Peer calls: caller's progress range delegation
/// - LLM cartridge client: frame-to-callback mapping
///
/// All child progress values are clamped to [0.0, 1.0] before mapping.
/// The mapped result is `base + child_progress.clamp(0.0, 1.0) * weight`.

/// Map child progress [0.0, 1.0] into parent range [base, base + weight].
///
/// This is the canonical progress mapping formula. Every place in the system
/// that subdivides progress must use this function — no ad-hoc derivations.
#[inline]
pub fn map_progress(child_progress: f32, base: f32, weight: f32) -> f32 {
    base + child_progress.clamp(0.0, 1.0) * weight
}

/// Wraps a `CapProgressFn` with a progress range subdivision.
#[derive(Clone)]
pub struct ProgressMapper {
    base: f32,
    weight: f32,
    parent: CapProgressFn,
    /// When set, every `report` also emits the raw (un-mapped) child value — this
    /// cap's OWN completion fraction — so a per-step consumer sees each section's
    /// progress, paired with the reporting cap's stable per-step identity so the
    /// consumer can attribute the fraction unambiguously. Attached only to the
    /// per-cap group mappers; sub-mappers created via `sub_mapper` deliberately do
    /// NOT inherit it (their child is an intra-cap phase fraction, not the cap's own
    /// progress).
    ///
    /// Sink and token live in ONE option because neither is meaningful alone: a sink
    /// with no step to attribute to has nothing to say, and a token with no sink has
    /// nowhere to say it.
    step_sink: Option<(CapStepProgressFn, StepToken)>,
}

impl ProgressMapper {
    /// Create a mapper that maps child [0.0, 1.0] into parent [base, base + weight].
    pub fn new(parent: &CapProgressFn, base: f32, weight: f32) -> Self {
        Self {
            base,
            weight,
            parent: Arc::clone(parent),
            step_sink: None,
        }
    }

    /// Attach a per-step sink that receives this mapper's raw child value (the cap's
    /// own completion fraction) on every `report`, tagged with `token_id` — the
    /// reporting cap step's stable identity.
    pub fn with_step_sink(mut self, sink: &CapStepProgressFn, token_id: &StepToken) -> Self {
        self.step_sink = Some((Arc::clone(sink), token_id.clone()));
        self
    }

    /// Report child progress. The value is clamped to [0.0, 1.0] and mapped.
    pub fn report(&self, child_progress: f32, cap_urn: &str, msg: &str) {
        let clamped = child_progress.clamp(0.0, 1.0);
        // Emit the cap's own fraction BEFORE the overall so a consumer that reads a
        // shared "latest step" cell inside the parent callback sees the fresh value.
        if let Some((sink, token_id)) = &self.step_sink {
            sink(clamped, cap_urn, token_id);
        }
        let overall = map_progress(clamped, self.base, self.weight);
        (self.parent)(overall, cap_urn, msg);
    }

    /// Convert into a `CapProgressFn` for passing to APIs that expect one.
    pub fn as_cap_progress_fn(&self) -> CapProgressFn {
        let mapper = self.clone();
        Arc::new(move |p: f32, cap_urn: &str, msg: &str| {
            mapper.report(p, cap_urn, msg);
        })
    }

    /// Create a sub-mapper that maps a child range within this mapper's range.
    ///
    /// Example: if this mapper maps to [0.2, 0.8] (base=0.2, weight=0.6),
    /// and you create a sub-mapper with sub_base=0.5, sub_weight=0.5,
    /// the sub-mapper maps to [0.5, 0.8] in the parent's coordinate space.
    ///
    /// The sub-mapper does NOT inherit `step_sink`: it flattens onto this mapper's
    /// `parent` (the grandparent), and its child is an intra-cap phase fraction, not
    /// the cap's own progress. The live per-step value is sourced from the per-cap
    /// group mappers' direct reports, which the machine streaming path drives; this
    /// intra-cap subdivision is used by non-streaming phases only.
    pub fn sub_mapper(&self, sub_base: f32, sub_weight: f32) -> Self {
        Self {
            base: self.base + sub_base * self.weight,
            weight: sub_weight * self.weight,
            parent: Arc::clone(&self.parent),
            step_sink: None,
        }
    }
}

/// Cap URN for the identity capability (always available from any cartridge runtime).
const CAP_IDENTITY: &str = "cap:effect=none";

// =============================================================================
// Error Types
// =============================================================================

#[derive(Debug, Error)]
pub enum ExecutionError {
    /// An execution failure attributed to the exact immutable strand step.
    #[error("Step {step_token_id} failed: {source}")]
    StepFailed {
        step_token_id: StepToken,
        #[source]
        source: Box<ExecutionError>,
    },

    #[error("Cartridge not found for cap: {cap_urn}")]
    CartridgeNotFound { cap_urn: String },

    #[error("Cartridge download failed: {0}")]
    CartridgeDownloadFailed(String),

    /// A cap invocation failed. `code`/`class` carry the failure identity
    /// DECLARED at the emit source (docs/failure-taxonomy.md) — read from the
    /// ERR frame, never re-derived from message text. Engine-detected
    /// failures carry `None` + `Internal`. `arg_urn` is the emit source's
    /// argument attribution (the media URN of the ONE argument the failure
    /// is about), threaded from the ERR frame; `None` when the source did
    /// not attribute — never inferred here.
    #[error("Cartridge execution failed for cap {cap_urn}: {details}")]
    CartridgeExecutionFailed {
        cap_urn: String,
        code: Option<String>,
        class: crate::failure::AttributionClass,
        details: String,
        arg_urn: Option<String>,
    },

    /// A cap emitted an output STREAM_START whose media URN violates its
    /// declared effect contract (`CapUrn::is_conformant_runtime_output`).
    /// The plan's downstream type refinement was built on that promise, so
    /// the violation fails hard at receipt — before the stream is forwarded
    /// or collected — attributed Internal: the cartridge broke its own
    /// declared contract, not a user input problem.
    #[error(
        "Cap '{cap_urn}' violated its effect contract: effect={effect}, runtime input '{runtime_input}', expected output '{expected}', emitted '{actual}'"
    )]
    EffectContractViolation {
        cap_urn: String,
        effect: String,
        runtime_input: String,
        expected: String,
        actual: String,
    },

    #[error("Node {node} has no incoming data")]
    NoIncomingData { node: String },

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Host error: {0}")]
    HostError(String),

    /// The cartridge that would serve this step is not available — it left its
    /// host's inventory and did not return within the admission grace window.
    /// Its own class because the deployment changed under a valid request; the
    /// engine did nothing wrong, so this is not `HostError`/`internal`.
    #[error("Cartridge unavailable for cap '{cap_urn}': {details}")]
    CartridgeUnavailable { cap_urn: String, details: String },

    #[error("Registry error: {0}")]
    FabricRegistryError(String),
}

impl ExecutionError {
    /// The failure class this error DECLARES (docs/failure-taxonomy.md).
    /// `CartridgeExecutionFailed` carries the class the emit source declared;
    /// every other variant declares its class here, at its definition:
    /// missing/undownloadable cartridges and registry failures are deployment
    /// problems (Environment); everything else is ours (Internal).
    pub fn attribution_class(&self) -> crate::failure::AttributionClass {
        use crate::failure::AttributionClass;
        match self {
            ExecutionError::StepFailed { source, .. } => source.attribution_class(),
            ExecutionError::CartridgeExecutionFailed { class, .. } => *class,
            ExecutionError::CartridgeNotFound { .. }
            | ExecutionError::CartridgeDownloadFailed(_)
            | ExecutionError::CartridgeUnavailable { .. }
            | ExecutionError::FabricRegistryError(_) => AttributionClass::Environment,
            ExecutionError::NoIncomingData { .. }
            | ExecutionError::IoError(_)
            | ExecutionError::HostError(_)
            | ExecutionError::EffectContractViolation { .. } => AttributionClass::Internal,
        }
    }

    /// The machine-readable code declared at the emit source, when carried.
    pub fn failure_code(&self) -> Option<&str> {
        match self {
            ExecutionError::StepFailed { source, .. } => source.failure_code(),
            ExecutionError::CartridgeExecutionFailed { code, .. } => code.as_deref(),
            _ => None,
        }
    }

    /// Media URN of the argument the failure is attributed to, when the
    /// emit source declared one on the ERR frame. Every other variant is
    /// not about one argument and returns `None` — never a guess.
    pub fn failure_arg_urn(&self) -> Option<&str> {
        match self {
            ExecutionError::StepFailed { source, .. } => source.failure_arg_urn(),
            ExecutionError::CartridgeExecutionFailed { arg_urn, .. } => arg_urn.as_deref(),
            _ => None,
        }
    }

    /// The LEAF human reason — the emit source's own message for cap
    /// failures, the Display chain otherwise.
    pub fn failure_reason(&self) -> String {
        match self {
            ExecutionError::StepFailed { source, .. } => source.failure_reason(),
            ExecutionError::CartridgeExecutionFailed { details, .. } => details.clone(),
            other => other.to_string(),
        }
    }

    pub fn step_token_id(&self) -> Option<&StepToken> {
        match self {
            ExecutionError::StepFailed { step_token_id, .. } => Some(step_token_id),
            _ => None,
        }
    }

    pub fn failure_cap_urn(&self) -> Option<&str> {
        match self {
            ExecutionError::StepFailed { source, .. } => source.failure_cap_urn(),
            ExecutionError::CartridgeExecutionFailed { cap_urn, .. } => Some(cap_urn),
            ExecutionError::EffectContractViolation { cap_urn, .. } => Some(cap_urn),
            _ => None,
        }
    }

    fn at_step(self, step_token_id: &StepToken) -> Self {
        if matches!(self, ExecutionError::StepFailed { .. }) {
            self
        } else {
            ExecutionError::StepFailed {
                step_token_id: step_token_id.clone(),
                source: Box::new(self),
            }
        }
    }
}

// =============================================================================
// Node Data (public API — resolved to raw bytes internally)
// =============================================================================

/// Runtime data associated with a DAG node.
#[derive(Debug, Clone)]
pub enum NodeData {
    /// Raw binary data
    Bytes(Vec<u8>),
    /// Text data
    Text(String),
    /// File path — read into bytes before execution
    FilePath(PathBuf),
}

impl NodeData {
    /// Resolve to raw bytes. FilePath reads the file, Text converts to UTF-8 bytes.
    async fn into_bytes(self) -> Result<Vec<u8>, ExecutionError> {
        match self {
            NodeData::Bytes(b) => Ok(b),
            NodeData::Text(t) => Ok(t.into_bytes()),
            NodeData::FilePath(path) => tokio::fs::read(&path).await.map_err(|e| {
                ExecutionError::HostError(format!(
                    "Failed to read file '{}': {}",
                    path.display(),
                    e
                ))
            }),
        }
    }
}

// =============================================================================
// Edge Grouping — fan-in detection
// =============================================================================

/// A group of edges that share the same `(to, cap_urn)`.
///
/// Single-edge groups are standard single-input cap invocations.
/// Multi-edge groups are fan-in: all edges' inputs are sent as separate streams
/// in ONE cap invocation so the handler can consume them together.
#[derive(Debug, Clone)]
pub struct EdgeGroup {
    /// Destination node (same for all edges in the group)
    pub to: String,
    /// Cap URN (same for all edges in the group)
    pub cap_urn: String,
    /// Stable per-step identity of this cap invocation, carried from the
    /// originating `StrandStep.token_id`. A group is exactly one cap step, so all
    /// its edges share this identity; the group's `to` node produced it.
    pub token_id: StepToken,
    /// All edges in this group (one or more)
    pub edges: Vec<ResolvedEdge>,
}

/// Group DAG edges by `(to, cap_urn)`.
///
/// Edges that share the same destination node and cap URN form a fan-in group
/// and will be sent as multiple streams in a single cap invocation.
fn build_edge_groups(edges: &[ResolvedEdge]) -> Vec<EdgeGroup> {
    // Preserve insertion order for determinism
    let mut order: Vec<(String, String)> = Vec::new();
    let mut map: HashMap<(String, String), Vec<ResolvedEdge>> = HashMap::new();

    for edge in edges {
        let key = (edge.to.clone(), edge.cap_urn.clone());
        if !map.contains_key(&key) {
            order.push(key.clone());
        }
        map.entry(key).or_default().push(edge.clone());
    }

    order
        .into_iter()
        .map(|key| {
            let edges = map.remove(&key).unwrap();
            // A group is exactly one cap step; every edge in it shares the step's
            // identity. Take the first edge's token_id as the group's identity
            // (insertion order is preserved above, so this is deterministic).
            let token_id = edges
                .first()
                .expect("edge group is built from a non-empty edge list")
                .token_id
                .clone();
            EdgeGroup {
                to: key.0,
                cap_urn: key.1,
                token_id,
                edges,
            }
        })
        .collect()
}

/// Topological sort of edge groups.
///
/// A group can execute when all groups that produce its `from` nodes have completed.
/// Returns group indices in execution order.
fn topological_sort_groups(groups: &[EdgeGroup]) -> Result<Vec<usize>, ExecutionError> {
    let n = groups.len();

    // Map each produced node to the group index that produces it
    let mut produced_by: HashMap<&str, usize> = HashMap::new();
    for (i, g) in groups.iter().enumerate() {
        produced_by.insert(g.to.as_str(), i);
    }

    // Compute in-degree for each group and reverse-dependency map
    let mut in_degree: Vec<usize> = vec![0; n];
    // dependents[i] = set of group indices that depend on group i completing first
    let mut dependents: Vec<HashSet<usize>> = (0..n).map(|_| HashSet::new()).collect();

    for (i, g) in groups.iter().enumerate() {
        let mut seen: HashSet<usize> = HashSet::new();
        for edge in &g.edges {
            if let Some(&dep) = produced_by.get(edge.from.as_str()) {
                if dep != i && seen.insert(dep) {
                    in_degree[i] += 1;
                    dependents[dep].insert(i);
                }
            }
        }
    }

    let mut queue: Vec<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();
    let mut sorted: Vec<usize> = Vec::with_capacity(n);

    while let Some(i) = queue.pop() {
        sorted.push(i);
        for &j in &dependents[i] {
            in_degree[j] -= 1;
            if in_degree[j] == 0 {
                queue.push(j);
            }
        }
    }

    if sorted.len() != n {
        // A cyclic graph reached execution — planner bug, ours: Internal.
        return Err(ExecutionError::CartridgeExecutionFailed {
            cap_urn: String::new(),
            code: None,
            class: crate::failure::AttributionClass::Internal,
            details: "Cycle detected in graph".to_string(),
            arg_urn: None,
        });
    }

    Ok(sorted)
}

// =============================================================================
// Cartridge Manager
// =============================================================================

/// Manages cartridge discovery, download, and caching.
pub struct CartridgeManager {
    cartridge_repo: CartridgeRepo,
    /// Channel-partitioned root: cartridges install under
    /// `{cartridge_dir}/{channel}/{cartridge_id}/{version}/`.
    cartridge_dir: PathBuf,
    /// The cartridge registry this manager installs from. `None` = a build
    /// with no baked registry (dev): only dev binaries and bundled cartridges
    /// exist, and any cap that would need a registry download hard-errors.
    registry_url: Option<String>,
    /// Signing trust anchors (baked roots + environment). Required for every
    /// registry operation: the manifest chain-verifies on sync, and every
    /// downloaded/installed binary verifies under a certificate-authorized
    /// release key. `Some(registry) + None(trust)` cannot happen in a
    /// product build (capdag's build.rs enforces the triple); the runtime
    /// guard below covers library/test misuse.
    trust: Option<crate::bifaci::release_cert::RegistryTrust>,
    /// Channel this manager is operating in. The orchestrator can only
    /// install/run cartridges that match its channel — release builds
    /// never touch nightly artefacts and vice versa.
    channel: crate::bifaci::cartridge_repo::CartridgeChannel,
    /// Fabric registry manifest version stamped into every cartridge.json
    /// this manager writes. Typically the engine's build-time-baked
    /// `capdag::FABRIC_MANIFEST_VERSION`; passed in at construction so
    /// tests can write provenance at arbitrary versions.
    fabric_manifest_version: u32,
    dev_cartridges: HashMap<PathBuf, CapManifest>,
}

impl CartridgeManager {
    pub fn new(
        cartridge_dir: PathBuf,
        registry_url: Option<String>,
        channel: crate::bifaci::cartridge_repo::CartridgeChannel,
        fabric_manifest_version: u32,
        dev_binaries: Vec<PathBuf>,
        trust: Option<crate::bifaci::release_cert::RegistryTrust>,
        // The fabric registry this client resolves caps against. Every cartridge
        // registry fetched must declare this same fabric (enforced in
        // `CartridgeRepoServer::new`) so all registries share one fabric.
        fabric_registry_url: String,
    ) -> Self {
        use crate::bifaci::cartridge_json::CartridgeJson;

        // Resolve dev paths: directories with cartridge.json → resolve entry point.
        // Files → standalone binary. Directories without cartridge.json → each
        // executable file inside is a separate binary cartridge.
        let mut resolved: Vec<PathBuf> = Vec::new();
        // Dev cartridges resolved via this code path always live under
        // the dev tree, so the expected slug is the dev sentinel.
        // Registry-installed cartridges go through CartridgeManager's
        // download path, not here.
        let dev_slug = crate::bifaci::cartridge_slug::DEV_SLUG;
        for p in dev_binaries {
            if p.is_file() {
                resolved.push(p);
            } else if p.is_dir() {
                match CartridgeJson::read_from_dir(&p, dev_slug) {
                    Ok(cj) => {
                        let entry_point = cj.resolve_entry_point(&p);
                        resolved.push(entry_point);
                    }
                    Err(crate::bifaci::cartridge_json::CartridgeJsonError::NotFound(_)) => {
                        // No cartridge.json — treat each executable file as a separate binary cartridge
                        if let Ok(entries) = std::fs::read_dir(&p) {
                            for entry in entries.flatten() {
                                let path = entry.path();
                                if path.is_file() {
                                    #[cfg(unix)]
                                    {
                                        use std::os::unix::fs::PermissionsExt;
                                        if let Ok(meta) = std::fs::metadata(&path) {
                                            if meta.permissions().mode() & 0o111 != 0 {
                                                resolved.push(path);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            "[DevMode] Invalid cartridge.json in {:?}: {} — skipping",
                            p,
                            e
                        );
                    }
                }
            }
        }

        Self {
            cartridge_repo: CartridgeRepo::new(3600, fabric_registry_url),
            cartridge_dir,
            registry_url,
            trust,
            channel,
            fabric_manifest_version,
            dev_cartridges: resolved
                .into_iter()
                .map(|p| {
                    (
                        p,
                        CapManifest::new(
                            String::new(),
                            String::new(),
                            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
                            None,
                            String::new(),
                            Vec::new(),
                        ),
                    )
                })
                .collect(),
        }
    }

    pub async fn init(&mut self) -> Result<(), ExecutionError> {
        fs::create_dir_all(&self.cartridge_dir)?;

        for (bin_path, _) in &self.dev_cartridges.clone() {
            match self.discover_manifest(bin_path).await {
                Ok(manifest) => {
                    self.dev_cartridges.insert(bin_path.clone(), manifest);
                }
                Err(e) => {
                    tracing::error!("[DevMode] Failed: {:?}: {}", bin_path, e);
                    return Err(e);
                }
            }
        }

        if let Some(registry_url) = self.registry_url.clone() {
            // Manifest verification happens inside the fetch when trust is
            // set: the `<url>.sig` sidecar must chain-verify over the exact
            // fetched bytes before the manifest is cached.
            self.cartridge_repo.set_trust(self.trust.clone()).await;
            self.cartridge_repo
                .sync_repos(&[registry_url.clone()])
                .await;
            // This manager's registry is mandatory — a sync failure (network,
            // parse, or SIGNATURE) surfaces here with its real cause instead
            // of a later misleading "cartridge not found".
            if let Some(err) = self.cartridge_repo.sync_error(&registry_url).await {
                return Err(ExecutionError::HostError(format!(
                    "cartridge registry sync failed for '{registry_url}': {err}"
                )));
            }
        }

        Ok(())
    }

    /// The configured registry URL, or the hard error every registry
    /// operation raises in a registry-less (dev) build.
    fn registry_url_required(&self, context: &str) -> Result<&str, ExecutionError> {
        self.registry_url.as_deref().ok_or_else(|| {
            ExecutionError::CartridgeDownloadFailed(format!(
                "{context}: this build bakes no cartridge registry — registry installs and \
                 downloads are disabled in dev builds (use --dev-bins or a bundled cartridge)"
            ))
        })
    }

    /// The signing trust anchors, or the hard error raised when a registry
    /// operation is attempted without them.
    fn trust_required(
        &self,
        context: &str,
    ) -> Result<&crate::bifaci::release_cert::RegistryTrust, ExecutionError> {
        self.trust.as_ref().ok_or_else(|| {
            ExecutionError::CartridgeDownloadFailed(format!(
                "{context}: this build bakes no cartridge signing root keys — registry \
                 downloads are disabled without them (a product build bakes \
                 MFR_CARTRIDGE_ROOT_PUBKEYS beside the registry URL)"
            ))
        })
    }

    async fn discover_manifest(&self, bin_path: &Path) -> Result<CapManifest, ExecutionError> {
        let mut child = Command::new(bin_path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| ExecutionError::CartridgeExecutionFailed {
                cap_urn: "manifest-discovery".to_string(),
                code: None,
                // An unspawnable cartridge binary is a deployment problem.
                class: crate::failure::AttributionClass::Environment,
                details: format!("Failed to spawn cartridge: {}", e),
                arg_urn: None,
            })?;

        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();

        let mut reader = FrameReader::new(stdout);
        let mut writer = FrameWriter::new(stdin);

        let result = handshake(&mut reader, &mut writer)
            .await
            .map_err(|e| ExecutionError::HostError(format!("Handshake failed: {:?}", e)))?;

        let manifest: CapManifest = serde_json::from_slice(&result.manifest)
            .map_err(|e| ExecutionError::HostError(format!("Bad manifest: {}", e)))?;

        let _ = child.kill().await;
        Ok(manifest)
    }

    /// Resolve all cap URNs from the graph to unique (binary_path, cap_groups) pairs.
    ///
    /// For dev cartridges (with discovered manifests), forwards the
    /// cartridge's full `cap_groups` to the host so every cap declared
    /// in the manifest is registered — not just the DAG-edge caps.
    /// This is critical because cartridges send peer requests for caps
    /// that aren't in the DAG (e.g. candlecartridge peer-invokes
    /// modelcartridge's `download-model` cap during ML inference).
    /// Without full cap registration, the `CartridgeHostRuntime` can't
    /// route these peer requests.
    ///
    /// `adapter_urns` declared by the cartridge propagate verbatim
    /// inside the cap groups so the engine can register
    /// content-inspection adapters per cartridge once the host's
    /// RelayNotify reaches the relay.
    pub async fn resolve_cartridges(
        &self,
        cap_urns: &[&str],
    ) -> Result<
        Vec<(
            PathBuf,
            Option<(
                String,
                String,
                crate::bifaci::cartridge_repo::CartridgeChannel,
            )>,
            Vec<crate::bifaci::manifest::CapGroup>,
        )>,
        ExecutionError,
    > {
        // Collect unique cartridge binaries needed for the DAG
        let mut cartridge_paths: HashSet<PathBuf> = HashSet::new();

        for &cap_urn in cap_urns {
            let (bin_path, _cartridge_id) = self.find_cartridge_binary(cap_urn).await?;
            cartridge_paths.insert(bin_path);
        }

        // Also include ALL dev cartridge binaries — they may be needed for peer request
        // routing even if they don't directly appear in the DAG.
        for dev_path in self.dev_cartridges.keys() {
            cartridge_paths.insert(dev_path.clone());
        }

        // For each cartridge, forward the full manifest cap_groups —
        // adapter_urns and all. Non-dev cartridges (registry installs)
        // get a synthetic identity-only group so on-demand spawn can
        // route the identity probe; their real cap_groups arrive via
        // the post-spawn HELLO and overwrite this fallback.
        let result: Vec<(
            PathBuf,
            Option<(
                String,
                String,
                crate::bifaci::cartridge_repo::CartridgeChannel,
            )>,
            Vec<crate::bifaci::manifest::CapGroup>,
        )> = cartridge_paths
            .into_iter()
            .map(|path| {
                if let Some(manifest) = self.dev_cartridges.get(&path) {
                    let identity = Some((
                        manifest.name.clone(),
                        manifest.version.clone(),
                        manifest.channel,
                    ));
                    (path, identity, manifest.cap_groups.clone())
                } else {
                    let groups = vec![crate::bifaci::manifest::CapGroup {
                        name: "identity".to_string(),
                        caps: vec![crate::standard::caps::identity_cap()],
                        adapter_urns: Vec::new(),
                    }];
                    (path, None, groups)
                }
            })
            .collect();

        Ok(result)
    }

    /// Registry suggestions for a cap URN (which cartridges provide it),
    /// resolved from the synced, signature-verified manifest — no download.
    /// Powers `capdag resolve`.
    pub async fn suggestions_for_cap(
        &self,
        cap_urn: &str,
    ) -> Vec<crate::bifaci::cartridge_repo::CartridgeSuggestion> {
        self.cartridge_repo.get_suggestions_for_cap(cap_urn).await
    }

    /// The synced registry's entry for a cartridge id in this manager's
    /// channel, if any. Powers `capdag resolve`'s per-cartridge detail
    /// (version, platforms, signed-binary presence).
    pub async fn registry_cartridge(
        &self,
        cartridge_id: &str,
    ) -> Option<crate::bifaci::cartridge_repo::CartridgeInfo> {
        let registry_url = self.registry_url.as_deref()?;
        self.cartridge_repo
            .get_cartridge(registry_url, self.channel, cartridge_id)
            .await
    }

    /// Find the binary path for a cap URN.
    async fn find_cartridge_binary(
        &self,
        cap_urn: &str,
    ) -> Result<(PathBuf, String), ExecutionError> {
        let requested_urn =
            CapUrn::from_string(cap_urn).map_err(|e| ExecutionError::CartridgeNotFound {
                cap_urn: format!("Invalid URN: {}: {}", cap_urn, e),
            })?;

        // This is RESOLUTION, not dispatch: `requested_urn` is a fully-resolved,
        // concrete cap (an alias resolved to exactly one cap URN), and we must
        // run the cartridge that implements THAT cap — never silently substitute
        // a merely-dispatchable one. So we match with `is_equivalent` (symmetric
        // exact match), the SAME relation the registry path uses in
        // `get_suggestions_for_cap`. Using the looser `is_dispatchable` here
        // would let a dev cartridge declaring a more-general cap short-circuit
        // the exact cap the alias named. See capdag/docs/07-dispatch.md,
        // "Resolution vs. dispatch: which predicate?".
        for (bin_path, manifest) in &self.dev_cartridges {
            for cap in manifest.all_caps() {
                // cap.urn is the declared candidate cap; requested_urn is the
                // resolved cap we must run.
                if cap.urn.is_equivalent(&requested_urn) {
                    return Ok((bin_path.clone(), format!("dev:{}", bin_path.display())));
                }
            }
        }

        // Fall back to registry
        let suggestions = self.cartridge_repo.get_suggestions_for_cap(cap_urn).await;
        if suggestions.is_empty() {
            return Err(ExecutionError::CartridgeNotFound {
                cap_urn: cap_urn.to_string(),
            });
        }

        let cartridge_id = &suggestions[0].cartridge_id;
        let bin_path = self.get_cartridge_path(cartridge_id).await?;
        Ok((bin_path, cartridge_id.clone()))
    }

    pub async fn get_cartridge_path(&self, cartridge_id: &str) -> Result<PathBuf, ExecutionError> {
        if let Some(dev_path) = cartridge_id.strip_prefix("dev:") {
            let path = PathBuf::from(dev_path);
            if !path.exists() {
                // A missing dev binary is a deployment problem — Environment.
                return Err(ExecutionError::CartridgeExecutionFailed {
                    cap_urn: cartridge_id.to_string(),
                    code: None,
                    class: crate::failure::AttributionClass::Environment,
                    details: format!("Dev binary not found: {:?}", path),
                    arg_urn: None,
                });
            }
            return Ok(path);
        }

        // Look for an existing installed cartridge in the registry-partitioned,
        // version-partitioned, channel-partitioned versioned layout:
        // `{cartridge_dir}/{registry_slug}/v{cartridge_registry_version}/{channel}/{cartridge_id}/{version}/cartridge.json`.
        // The orchestrator's manager is bound to a single registry — that's
        // the registry it just fetched the manifest from, so the slug it
        // walks is fixed.
        let registry_url = self.registry_url_required(cartridge_id)?.to_string();
        let registry_slug = crate::bifaci::cartridge_slug::slug_for(Some(registry_url.as_str()));
        let name_dir = self
            .cartridge_dir
            .join(&registry_slug)
            .join(format!("v{}", crate::CARTRIDGE_REGISTRY_VERSION))
            .join(self.channel.as_str())
            .join(cartridge_id);
        if name_dir.is_dir() {
            if let Some(entry_point) =
                self.find_latest_installed_entry_point(&name_dir, &registry_slug, &registry_url)
            {
                // Re-verify the installed binary against the registry's
                // signed manifest on every use — a post-install tamper of
                // the on-disk executable must never run. A FAILED
                // verification is not a dead end, though: whether the local
                // bytes rotted or the registry re-published this version
                // with new bytes (staging does, via --overwrite), the
                // remedy is identical and fully gated — discard the stale
                // install and re-download through the same
                // sha256+size+signature pipeline. Only a fresh download
                // that ALSO fails verification is terminal (the registry
                // itself is inconsistent).
                match self
                    .verify_cartridge_integrity(cartridge_id, &entry_point)
                    .await
                {
                    Ok(()) => return Ok(entry_point),
                    Err(e) => {
                        tracing::warn!(
                            cartridge_id,
                            entry_point = %entry_point.display(),
                            error = %e,
                            "installed cartridge failed integrity verification against the \
                             current signed manifest — discarding the stale install and \
                             re-downloading (every byte re-verifies before it can run)"
                        );
                        if let Some(version_dir) = entry_point.parent() {
                            if let Err(rm) = fs::remove_dir_all(version_dir) {
                                // If the stale install cannot even be removed,
                                // the original verification failure stands —
                                // never run unverified bytes.
                                tracing::error!(
                                    version_dir = %version_dir.display(),
                                    error = %rm,
                                    "failed to remove stale cartridge install"
                                );
                                return Err(e);
                            }
                        }
                    }
                }
            }
        }

        self.download_cartridge(cartridge_id).await
    }

    /// Find the entry point of the latest installed version in a cartridge name directory.
    /// `expected_slug` is the on-disk registry slug the caller reached
    /// through (the slug for `self.registry_url`); the per-version
    /// cartridge.json is validated against it via the three-place
    /// rule.
    fn find_latest_installed_entry_point(
        &self,
        name_dir: &Path,
        expected_slug: &str,
        registry_url: &str,
    ) -> Option<PathBuf> {
        let mut versions: Vec<(String, PathBuf)> = Vec::new();

        for entry in fs::read_dir(name_dir).ok()? {
            let entry = entry.ok()?;
            let version_dir = entry.path();
            if !version_dir.is_dir() {
                continue;
            }
            match crate::bifaci::cartridge_json::CartridgeJson::read_from_dir(
                &version_dir,
                expected_slug,
            ) {
                Ok(cj) => {
                    // Hard mismatch — never run a cartridge from a different
                    // channel even if it landed under our channel's tree.
                    if cj.channel != self.channel {
                        tracing::warn!(
                            "Skipping cartridge at {:?}: cartridge.json channel '{}' \
                             does not match orchestrator channel '{}'",
                            version_dir,
                            cj.channel,
                            self.channel
                        );
                        continue;
                    }
                    // Three-place rule: cartridge.json's registry_url
                    // must match the orchestrator's. The slug check
                    // inside read_from_dir only proves folder ⇔ json
                    // agreement; here we check json ⇔ orchestrator's
                    // configured registry. Mismatches are skipped, not
                    // deleted — a stale install from a previously
                    // configured registry is a user-visible state, not
                    // garbage.
                    if cj.registry_url.as_deref() != Some(registry_url) {
                        tracing::warn!(
                            "Skipping cartridge at {:?}: cartridge.json registry_url={:?} \
                             does not match orchestrator registry_url='{}'",
                            version_dir,
                            cj.registry_url,
                            registry_url
                        );
                        continue;
                    }
                    let entry_point = cj.resolve_entry_point(&version_dir);
                    versions.push((cj.version, entry_point));
                }
                Err(_) => continue,
            }
        }

        if versions.is_empty() {
            return None;
        }

        // Sort by version descending (latest first)
        versions.sort_by(|a, b| {
            let parts_a: Vec<u32> = a.0.split('.').filter_map(|p| p.parse().ok()).collect();
            let parts_b: Vec<u32> = b.0.split('.').filter_map(|p| p.parse().ok()).collect();
            let max_len = parts_a.len().max(parts_b.len());
            for i in 0..max_len {
                let na = parts_a.get(i).copied().unwrap_or(0);
                let nb = parts_b.get(i).copied().unwrap_or(0);
                match nb.cmp(&na) {
                    std::cmp::Ordering::Equal => continue,
                    other => return other,
                }
            }
            std::cmp::Ordering::Equal
        });

        Some(versions.into_iter().next()?.1)
    }

    /// The manifest `binary` entry for a cartridge's host-platform build,
    /// or the hard error for a build that publishes none. There is NO
    /// installer fallback — the pure binary is the only artifact this
    /// execution path runs.
    async fn registry_binary_info(
        &self,
        registry_url: &str,
        cartridge_id: &str,
    ) -> Result<
        (
            crate::bifaci::cartridge_repo::CartridgeInfo,
            crate::bifaci::cartridge_repo::CartridgeBinaryInfo,
        ),
        ExecutionError,
    > {
        let cartridge_info = self
            .cartridge_repo
            .get_cartridge(registry_url, self.channel, cartridge_id)
            .await
            .ok_or_else(|| ExecutionError::CartridgeNotFound {
                cap_urn: format!(
                    "Cartridge {} not found in {} registry",
                    cartridge_id, self.channel
                ),
            })?;

        let platform = detect_platform();
        let build = cartridge_info
            .build_for_platform(&platform)
            .ok_or_else(|| {
                ExecutionError::CartridgeDownloadFailed(format!(
                    "Cartridge {} v{} has no build for platform '{}'. Available: {:?}",
                    cartridge_id,
                    cartridge_info.version,
                    platform,
                    cartridge_info.available_platforms()
                ))
            })?;

        let binary = build.binary.clone().ok_or_else(|| {
            ExecutionError::CartridgeDownloadFailed(format!(
                "Cartridge {} v{} for '{}' publishes no signed pure-binary artifact — \
                 refusing installer fallback. Re-publish the version under the signing \
                 regime: a published release build of cartridge {}.",
                cartridge_id, cartridge_info.version, platform, cartridge_id
            ))
        })?;
        Ok((cartridge_info, binary))
    }

    /// Verify `bytes` against a manifest `binary` entry: sha256, size, and
    /// the minisign signature under a certificate-authorized release key
    /// from the registry's verified manifest sidecar. Every failure is a
    /// hard, named error.
    async fn verify_binary_against_manifest(
        &self,
        registry_url: &str,
        cartridge_id: &str,
        binary: &crate::bifaci::cartridge_repo::CartridgeBinaryInfo,
        bytes: &[u8],
    ) -> Result<(), ExecutionError> {
        // Trust must exist for any registry operation.
        self.trust_required(cartridge_id)?;

        use sha2::{Digest, Sha256};
        let computed = format!("{:x}", Sha256::digest(bytes));
        if computed != binary.sha256 {
            return Err(ExecutionError::CartridgeDownloadFailed(format!(
                "SECURITY: SHA256 mismatch for {}!\n  Expected: {}\n  Computed: {}",
                cartridge_id, binary.sha256, computed
            )));
        }
        if bytes.len() as u64 != binary.size {
            return Err(ExecutionError::CartridgeDownloadFailed(format!(
                "SECURITY: size mismatch for {} (manifest says {} bytes, got {})",
                cartridge_id,
                binary.size,
                bytes.len()
            )));
        }

        // The signature must verify under a release key the manifest's
        // chain-verified certificate list authorizes. The list is populated
        // by the verified sidecar at sync; empty means the sync didn't
        // verify — which init() already turns into a hard error, so this is
        // a defense-in-depth check with its own message.
        let release_keys = self
            .cartridge_repo
            .verified_release_keys(registry_url)
            .await;
        if release_keys.is_empty() {
            return Err(ExecutionError::CartridgeDownloadFailed(format!(
                "SECURITY: no certificate-authorized release keys for registry '{}' — the \
                 manifest signature sidecar was never verified; refusing to trust {}'s \
                 binary signature",
                registry_url, cartridge_id
            )));
        }
        let mut last_error: Option<crate::SignatureError> = None;
        for (_key_id, pubkey) in &release_keys {
            match crate::verify_binary_signature(pubkey, &binary.signature, bytes) {
                Ok(()) => return Ok(()),
                Err(e) => last_error = Some(e),
            }
        }
        Err(ExecutionError::CartridgeDownloadFailed(format!(
            "SECURITY: binary signature for {} does not verify under any of the {} \
             certificate-authorized release key(s) of registry '{}': {}",
            cartridge_id,
            release_keys.len(),
            registry_url,
            last_error.expect("non-empty key list implies at least one error")
        )))
    }

    /// Re-verify an INSTALLED cartridge binary against the registry's signed
    /// manifest: recompute the sha256 of the on-disk executable and check
    /// its manifest signature chain. Runs on every reuse of an installed
    /// registry cartridge — a post-install tamper must never execute.
    async fn verify_cartridge_integrity(
        &self,
        cartridge_id: &str,
        binary_path: &Path,
    ) -> Result<(), ExecutionError> {
        let registry_url = self.registry_url_required(cartridge_id)?.to_string();
        let (_info, binary) = self
            .registry_binary_info(&registry_url, cartridge_id)
            .await?;
        let bytes = fs::read(binary_path).map_err(|e| {
            // An unreadable installed binary is a deployment problem.
            ExecutionError::CartridgeExecutionFailed {
                cap_urn: cartridge_id.to_string(),
                code: None,
                class: crate::failure::AttributionClass::Environment,
                details: format!(
                    "failed to read installed binary {:?} for integrity verification: {}",
                    binary_path, e
                ),
                arg_urn: None,
            }
        })?;
        self.verify_binary_against_manifest(&registry_url, cartridge_id, &binary, &bytes)
            .await
            .map_err(|e| ExecutionError::CartridgeExecutionFailed {
                cap_urn: cartridge_id.to_string(),
                code: None,
                // A tampered/mismatched installed binary is a deployment
                // problem — Environment.
                class: crate::failure::AttributionClass::Environment,
                details: format!(
                    "installed binary at {:?} failed integrity verification: {}",
                    binary_path, e
                ),
                arg_urn: None,
            })
    }

    /// Download a cartridge's signed PURE-BINARY artifact from the registry
    /// and install it into the versioned directory layout:
    /// `{cartridge_dir}/{slug}/{channel}/{id}/{version}/cartridge.json + binary`.
    ///
    /// Nothing touches disk until the bytes pass ALL of: sha256, size, and
    /// the minisign signature under a certificate-authorized release key
    /// (chain: baked roots → release-key certificate from the verified
    /// manifest sidecar → binary signature). Installer packages are never a
    /// fallback.
    async fn download_cartridge(&self, cartridge_id: &str) -> Result<PathBuf, ExecutionError> {
        let registry_url = self.registry_url_required(cartridge_id)?.to_string();
        self.trust_required(cartridge_id)?;
        let (cartridge_info, binary) = self
            .registry_binary_info(&registry_url, cartridge_id)
            .await?;

        // The v5 manifest carries the absolute URL on the binary itself.
        // No URL derivation: if the manifest's URL is wrong, we want to fail
        // hard against the URL the publisher actually committed to. The
        // manifest also pins the artifact's sha256 — appending it as a query
        // param keys any CDN cache to the CONTENT: a republished
        // (byte-different) artifact at the same path gets a new cache entry
        // instead of serving the stale object until TTL expiry.
        let download_url = format!(
            "{}{}sha256={}",
            binary.url,
            if binary.url.contains('?') { "&" } else { "?" },
            binary.sha256
        );
        let download_url = download_url.as_str();

        let response = reqwest::get(download_url).await.map_err(|e| {
            ExecutionError::CartridgeDownloadFailed(format!("Download failed: {}", e))
        })?;

        if !response.status().is_success() {
            return Err(ExecutionError::CartridgeDownloadFailed(format!(
                "HTTP {} from {}",
                response.status(),
                download_url
            )));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| ExecutionError::CartridgeDownloadFailed(format!("Read failed: {}", e)))?
            .to_vec();

        self.verify_binary_against_manifest(&registry_url, cartridge_id, &binary, &bytes)
            .await?;

        // Registry-partitioned, version-partitioned, channel-partitioned
        // layout. The orchestrator only ever installs from its own configured
        // registry, so the slug is fixed for the lifetime of this manager.
        let registry_slug = crate::bifaci::cartridge_slug::slug_for(Some(registry_url.as_str()));
        let version_dir = self
            .cartridge_dir
            .join(&registry_slug)
            .join(format!("v{}", crate::CARTRIDGE_REGISTRY_VERSION))
            .join(self.channel.as_str())
            .join(cartridge_id)
            .join(&cartridge_info.version);
        fs::create_dir_all(&version_dir)?;

        let binary_name = platform_binary_name(cartridge_id);
        let binary_path = version_dir.join(&binary_name);
        fs::write(&binary_path, &bytes)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mut perms = fs::metadata(&binary_path)?.permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&binary_path, perms)?;
        }

        // Write cartridge.json. `registry_url` is verbatim the manager's
        // registry; the cartridge was downloaded from there, so the
        // three-place rule (folder ⇔ provenance ⇔ HELLO) is satisfied by
        // construction. sha/size describe the BINARY artifact.
        let cj = crate::CartridgeJson {
            name: cartridge_id.to_string(),
            version: cartridge_info.version.clone(),
            channel: self.channel,
            registry_url: Some(registry_url.clone()),
            entry: binary_name.to_string(),
            installed_at: crate::bifaci::cartridge_json::install_timestamp_now(),
            installed_from: Some(crate::CartridgeInstallSource::Registry),
            // Provenance records the URL the publisher committed to, not the
            // content-keyed fetch URL.
            source_url: binary.url.clone(),
            package_sha256: binary.sha256.clone(),
            package_size: binary.size,
            fabric_manifest_version: self.fabric_manifest_version,
        };
        cj.write_to_dir(&version_dir).map_err(|e| {
            ExecutionError::CartridgeDownloadFailed(format!(
                "Failed to write cartridge.json: {}",
                e
            ))
        })?;

        Ok(binary_path)
    }
}

// =============================================================================
// Execution Context — Arc<RelaySwitch> for concurrent DAG execution
// =============================================================================

/// Handle for cleanup of a master's associated resources.
struct MasterCleanupHandle {
    /// Task handles to abort after shutdown.
    task_handles: Vec<tokio::task::JoinHandle<()>>,
}

/// Execution context for DAG execution.
///
/// Each `ExecutionContext` is an isolated execution environment that:
/// - Shares the `RelaySwitch` via `Arc` for concurrent access
/// - Owns its own `node_data` HashMap (isolated per execution)
/// - Tracks cleanup handles for managed tasks
///
/// This design enables concurrent DAG executions:
/// - Multiple contexts can share the same switch
/// - Each context has isolated node data
/// - The switch handles concurrent frame routing internally
pub struct ExecutionContext {
    /// Shared relay switch (interior mutability)
    switch: Arc<RelaySwitch>,
    /// Raw bytes at each DAG node. Isolated per execution context.
    node_data: HashMap<String, Vec<u8>>,
    /// Per-node stream metadata. Carries provenance context (e.g. {"title": "page_3"})
    /// through ForEach splits so body caps receive the upstream item's metadata.
    node_meta: HashMap<String, crate::StreamMeta>,
    /// Tracks which nodes hold sequence data (CBOR sequence of items).
    /// When true, the node's data is an RFC 8742 CBOR sequence that should be
    /// sent with is_sequence=true on STREAM_START so the receiver gets
    /// properly framed per-item chunks.
    node_is_sequence: HashMap<String, bool>,
    /// Nodes whose data is a LIVE-FEED REFERENCE selector (13.2 §Reference
    /// Media, live family): node id → reference urn. The send path labels
    /// such a node's outgoing stream with the REFERENCE urn (never the
    /// consuming edge's in_media) so the receiving cartridge's demux
    /// intercepts and resolves capture — the op stays transport-blind.
    node_live_reference: HashMap<String, String>,
    /// Nodes whose data lives in a DISK SPOOL, not in `node_data`: an
    /// UNBOUNDED intermediate at a mandatory chain-split boundary streams to
    /// a temp file (L16 — never unbounded memory) and downstream chain heads
    /// feed from the file (`send_file_stream`). node id → spool path.
    node_spool: HashMap<String, std::path::PathBuf>,
    /// Cached max chunk size from the relay.
    max_chunk: usize,
    /// Cleanup handles for masters added via add_cartridge_host.
    cleanup_handles: Vec<MasterCleanupHandle>,
}

impl ExecutionContext {
    /// Create a new ExecutionContext with an empty RelaySwitch.
    ///
    /// The RelaySwitch starts with no masters. Use `add_master()` or
    /// `add_cartridge_host()` to add masters before executing caps.
    ///
    /// Requires a `FabricRegistry` for the RelaySwitch to use when building
    /// the LiveCapFab for path finding queries. The registry is read at
    /// every LiveCapFab sync to compute the bookend-eligible URN set.
    pub async fn new(fabric_registry: Arc<FabricRegistry>) -> Result<Self, ExecutionError> {
        let switch = RelaySwitch::new(vec![], fabric_registry)
            .await
            .map_err(|e| ExecutionError::HostError(format!("RelaySwitch init: {}", e)))?;

        let max_chunk = switch.limits().await.max_chunk as usize;
        let max_chunk = if max_chunk == 0 {
            DEFAULT_MAX_CHUNK as usize
        } else {
            max_chunk
        };

        let switch = Arc::new(switch);
        // Start the background frame pump so master-side frames (notably
        // RelayNotify capability updates) are continuously dispatched
        // through `handle_master_frame` even while the orchestrator is
        // not actively executing a cap. Without this, `wait_for_cap`
        // polls a master.caps that never updates because no consumer is
        // draining `frame_rx`.
        switch.start_background_pump();

        Ok(Self {
            switch,
            node_data: HashMap::new(),
            node_live_reference: HashMap::new(),
            node_meta: HashMap::new(),
            node_is_sequence: HashMap::new(),
            node_spool: HashMap::new(),
            max_chunk,
            cleanup_handles: Vec::new(),
        })
    }

    /// Create a new ExecutionContext from an existing shared RelaySwitch.
    ///
    /// This is used for concurrent DAG executions that share the same infrastructure.
    /// Each context has its own isolated node_data.
    pub async fn from_switch(switch: Arc<RelaySwitch>) -> Result<Self, ExecutionError> {
        let max_chunk = switch.limits().await.max_chunk as usize;
        let max_chunk = if max_chunk == 0 {
            DEFAULT_MAX_CHUNK as usize
        } else {
            max_chunk
        };

        Ok(Self {
            switch,
            node_data: HashMap::new(),
            node_live_reference: HashMap::new(),
            node_meta: HashMap::new(),
            node_is_sequence: HashMap::new(),
            node_spool: HashMap::new(),
            max_chunk,
            cleanup_handles: Vec::new(),
        })
    }

    /// Get the shared RelaySwitch.
    pub fn switch(&self) -> &Arc<RelaySwitch> {
        &self.switch
    }

    /// Add a master connection from an externally managed socket.
    ///
    /// `id` is the stable identity of the cardinality slot — see
    /// [`crate::bifaci::RelaySwitch::add_master`] for the contract
    /// (reattach on reconnect, cardinality enforcement).
    ///
    /// The caller is responsible for the lifecycle of the connected endpoint
    /// (e.g., an InProcessCartridgeHost or external cartridge connection).
    ///
    /// Returns the master index on success.
    pub async fn add_master(
        &mut self,
        id: impl Into<String>,
        socket: UnixStream,
    ) -> Result<usize, ExecutionError> {
        let idx = self
            .switch
            .add_master(id, socket)
            .await
            .map_err(|e| ExecutionError::HostError(format!("add_master: {}", e)))?;

        self.update_max_chunk().await;
        Ok(idx)
    }

    /// Add a CartridgeHostRuntime as a master, spawning all required infrastructure.
    ///
    /// This creates:
    /// - CartridgeHostRuntime (async, in tokio task)
    /// - RelaySlave (async, in tokio task)
    /// - Socket pairs connecting them to the switch
    ///
    /// The ExecutionContext manages cleanup of these resources.
    pub async fn add_cartridge_host(
        &mut self,
        cartridges: Vec<(
            PathBuf,
            Option<(
                String,
                String,
                crate::bifaci::cartridge_repo::CartridgeChannel,
            )>,
            Vec<crate::bifaci::manifest::CapGroup>,
        )>,
    ) -> Result<usize, ExecutionError> {
        // Create socket pairs:
        //   switch_sock <-> slave_ext_sock (switch to slave)
        //   slave_int_sock <-> host_sock (slave to host runtime)
        let (switch_sock, slave_ext_sock) = UnixStream::pair().map_err(ExecutionError::IoError)?;
        let (slave_int_sock, host_sock) = UnixStream::pair().map_err(ExecutionError::IoError)?;

        // --- CartridgeHostRuntime (async, in tokio task) ---
        // Identity comes from one of two sources:
        //
        //   1. Installed cartridges live at
        //      `.../{registry_slug}/{channel}/{name}/{version}/{entry}`.
        //      The binary's parent dir holds cartridge.json; we read
        //      it and verify the three-place rule (folder slug ⇔
        //      provenance registry_url).
        //
        //   2. Dev binaries live wherever cargo dropped them
        //      (`build/cargo/<name>/release/<name>` or similar) and
        //      have no cartridge.json. We fall back to the
        //      orchestrator's `dev_fallback_channel` and treat the
        //      registry_url as `None` (dev install). The cartridge
        //      itself reports the same via HELLO at attach time.
        //
        // We choose between these at runtime by checking for
        // cartridge.json's presence; the file is absent for dev
        // binaries (no installer wrote it) and present for installed
        // ones. Anywhere else fails hard — we never silently guess.
        let mut host = CartridgeHostRuntime::new();
        for (path, manifest_identity, cap_groups) in &cartridges {
            let version_dir = path.parent().ok_or_else(|| {
                ExecutionError::HostError(format!(
                    "cartridge binary {} has no parent directory",
                    path.display()
                ))
            })?;
            let cartridge_json_path = version_dir.join("cartridge.json");
            if cartridge_json_path.exists() {
                // Installed-cartridge path. Walk up: version → name →
                // channel → v{registry_version} → slug. The slug folder is
                // FOUR levels up from the version dir (the registry-version
                // level sits between slug and channel); pass it through so the
                // three-place rule is enforced inside read_from_dir.
                let expected_slug_owned = version_dir
                    .ancestors()
                    .nth(4)
                    .and_then(|p| p.file_name())
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| {
                        ExecutionError::HostError(format!(
                            "cartridge path {} is not under a valid \
                             {{slug}}/v{{registry_version}}/{{channel}}/{{name}}/{{version}}/ tree",
                            path.display()
                        ))
                    })?;
                let provenance = crate::bifaci::cartridge_json::CartridgeJson::read_from_dir(
                    version_dir,
                    &expected_slug_owned,
                )
                .map_err(|e| {
                    ExecutionError::HostError(format!(
                        "reading cartridge.json for {}: {}",
                        path.display(),
                        e
                    ))
                })?;
                host.register_cartridge(
                    path,
                    &provenance.name,
                    &provenance.version,
                    provenance.channel,
                    provenance.registry_url.as_deref(),
                    cap_groups,
                );
            } else {
                // Dev binary (no cartridge.json on disk). Identity comes from the
                // manifest the cartridge reported during the pre-registration HELLO
                // probe. registry_url is None (dev install = absent registry).
                let (name, version, channel) = manifest_identity.as_ref().ok_or_else(|| {
                    ExecutionError::HostError(format!(
                        "dev binary {} has no manifest identity \
                         (discover_manifest should have populated this)",
                        path.display()
                    ))
                })?;
                host.register_cartridge(path, name, version, *channel, None, cap_groups);
            }
        }

        let (host_read, host_write) = host_sock.into_split();

        let host_handle = tokio::spawn(async move {
            if let Err(e) = host.run(host_read, host_write, || Vec::new()).await {
                tracing::error!("[CartridgeHostRuntime] Fatal: {}", e);
            }
        });

        // --- RelaySlave (async, in tokio task) ---
        let (slave_int_read, slave_int_write) = slave_int_sock.into_split();
        let slave = RelaySlave::new(
            BufReader::new(slave_int_read),
            BufWriter::new(slave_int_write),
        );

        // Initial RelayNotify advertises an empty `installed_cartridges`
        // list — the orchestrator hasn't attached any cartridges to
        // the host yet, so there is nothing real to declare. The
        // relay's `add_master` path treats an empty cap set as
        // "host present, no handler chain to probe yet" and skips
        // identity verification at this point. The RelayNotify the
        // CartridgeHostRuntime sends after spawning each cartridge
        // (with that cartridge's real cap_groups) is what triggers
        // the engine's identity probe, end-to-end through the
        // cartridge process.
        let initial_caps_json =
            serde_json::to_vec(&RelayNotifyCapabilitiesPayload::new(Vec::new()))
                .map_err(|e| ExecutionError::HostError(format!("serialize caps: {}", e)))?;

        let (slave_ext_read, slave_ext_write) = slave_ext_sock.into_split();

        let slave_handle = tokio::spawn(async move {
            if let Err(e) = slave
                .run(
                    FrameReader::new(BufReader::new(slave_ext_read)),
                    FrameWriter::new(BufWriter::new(slave_ext_write)),
                    Some((&initial_caps_json, &Limits::default())),
                )
                .await
            {
                tracing::error!("[RelaySlave] Fatal: {}", e);
            }
        });

        // --- Add to switch ---
        //
        // Synthesise a stable per-host id from the cartridges this
        // host wraps. Each call to `add_cartridge_host` creates a
        // new orchestrator-owned slot; the id distinguishes one
        // host's slot from another in logs / telemetry. For
        // single-cartridge hosts the id is the cartridge binary's
        // path; for multi-cartridge hosts it's the joined sorted
        // path list. Same input → same id, so a reconnect under
        // the same set of cartridges reattaches in place.
        let host_id: String = {
            let mut paths: Vec<String> = cartridges
                .iter()
                .map(|(path, _, _)| path.display().to_string())
                .collect();
            paths.sort();
            format!("cartridge-host:{}", paths.join("|"))
        };
        let master_idx = self
            .switch
            .add_master(host_id, switch_sock)
            .await
            .map_err(|e| ExecutionError::HostError(format!("add_master: {}", e)))?;

        // Store cleanup handles
        self.cleanup_handles.push(MasterCleanupHandle {
            task_handles: vec![host_handle, slave_handle],
        });

        self.update_max_chunk().await;
        Ok(master_idx)
    }

    /// Update max_chunk from current switch limits.
    async fn update_max_chunk(&mut self) {
        let chunk = self.switch.limits().await.max_chunk as usize;
        self.max_chunk = if chunk == 0 {
            DEFAULT_MAX_CHUNK as usize
        } else {
            chunk
        };
    }

    /// Get the current max chunk size.
    pub fn max_chunk(&self) -> usize {
        self.max_chunk
    }

    /// Get the aggregate capabilities of all connected masters.
    pub async fn capabilities(&self) -> Vec<u8> {
        self.switch.capabilities().await
    }

    /// Get the negotiated limits.
    pub async fn limits(&self) -> Limits {
        self.switch.limits().await
    }

    /// Set data for a node.
    pub fn set_node_data(&mut self, node: String, data: Vec<u8>) {
        self.node_data.insert(node, data);
    }

    /// Mark a node as carrying a live-feed reference selector (see
    /// `node_live_reference`).
    pub fn set_node_live_reference(&mut self, node: String, reference_urn: String) {
        self.node_live_reference.insert(node, reference_urn);
    }

    /// Get the full node → live-reference-urn map.
    pub fn node_live_reference(&self) -> &HashMap<String, String> {
        &self.node_live_reference
    }

    /// Mark a node's data as living in a disk spool (unbounded intermediate
    /// at a chain-split boundary); downstream feeds stream from the file.
    pub fn set_node_spool(&mut self, node: String, path: std::path::PathBuf) {
        self.node_spool.insert(node, path);
    }

    /// Get the full node → spool-path map.
    pub fn node_spool(&self) -> &HashMap<String, std::path::PathBuf> {
        &self.node_spool
    }

    /// Set stream metadata for a node (provenance context for ForEach propagation).
    pub fn set_node_meta(&mut self, node: String, meta: crate::StreamMeta) {
        self.node_meta.insert(node, meta);
    }

    /// Get stream metadata for a node.
    pub fn get_node_meta(&self, node: &str) -> Option<&crate::StreamMeta> {
        self.node_meta.get(node)
    }

    /// Get immutable reference to node_meta map.
    pub fn node_meta(&self) -> &HashMap<String, crate::StreamMeta> {
        &self.node_meta
    }

    /// Get mutable reference to node_meta map.
    pub fn node_meta_mut(&mut self) -> &mut HashMap<String, crate::StreamMeta> {
        &mut self.node_meta
    }

    /// Mark a node as holding sequence data.
    pub fn set_node_is_sequence(&mut self, node: String, is_sequence: bool) {
        self.node_is_sequence.insert(node, is_sequence);
    }

    /// Check if a node holds sequence data.
    pub fn is_node_sequence(&self, node: &str) -> bool {
        self.node_is_sequence.get(node).copied().unwrap_or(false)
    }

    /// Get the full node_is_sequence map.
    pub fn node_is_sequence(&self) -> &HashMap<String, bool> {
        &self.node_is_sequence
    }

    /// Get data for a node.
    pub fn get_node_data(&self, node: &str) -> Option<&Vec<u8>> {
        self.node_data.get(node)
    }

    /// Get immutable reference to node_data map.
    pub fn node_data(&self) -> &HashMap<String, Vec<u8>> {
        &self.node_data
    }

    /// Get mutable reference to node_data map.
    pub fn node_data_mut(&mut self) -> &mut HashMap<String, Vec<u8>> {
        &mut self.node_data
    }

    /// Consume and return the node_data map.
    pub fn into_node_data(self) -> HashMap<String, Vec<u8>> {
        // Abort all managed tasks
        for handle in self.cleanup_handles {
            for task in handle.task_handles {
                task.abort();
            }
        }
        self.node_data
    }

    /// Shut down the infrastructure and return accumulated node data.
    ///
    /// This:
    /// 1. Drops the switch reference (may or may not release the switch)
    /// 2. Aborts all managed tasks
    ///
    /// For masters added via `add_master()`, the caller is responsible for
    /// shutting down their endpoints.
    pub fn shutdown(self) -> HashMap<String, Vec<u8>> {
        self.into_node_data()
    }
}

// =============================================================================
// DAG Executor
// =============================================================================

/// Run one resolved DAG (a single ForEach-free linear chain, possibly with a
/// fan-in head) on an already-built [`ExecutionContext`]. This is THE segment
/// executor, shared by the reference [`execute_dag`] (dev-bin cartridges) and
/// the engine's `EngineHostRuntime::run_segment` (relay-switch cartridges).
///
/// All cap invocations open up front; frames stream cap→cap live (downstream
/// consumes upstream chunks before END exists) with credit flowing per hop
/// (L9–L15); the terminal cap's output is collected — to disk via `writer` when
/// present, else accumulated and CBOR-decoded in memory. `observer` correlates
/// each invocation's request id to its strand step for the run's flow snapshots
/// (the reference path passes `None`).
///
/// Returns `(node_data, terminal_is_sequence, writer_result, terminal_meta)`.
/// `node_data` holds each input node's raw bytes as a single-element vec and the
/// terminal node's CBOR-transport-stripped items (empty when a writer persisted
/// them to disk).
#[allow(clippy::too_many_arguments)]
pub async fn run_dag_on_context(
    ctx: &mut ExecutionContext,
    graph: &ResolvedGraph,
    cap_arguments: &HashMap<String, Vec<(String, Vec<u8>)>>,
    progress_fn: Option<&CapProgressFn>,
    step_progress_fn: Option<&CapStepProgressFn>,
    log_fn: Option<&PipelineLogFn>,
    stall_tracker: Option<Arc<PipelineProgressTracker>>,
    writer_factory: Option<&super::stream_io::SegmentWriterFactory>,
    body_coordinate: Option<super::execute_plan::ForEachBodyCoordinate>,
    persist_sinks: &HashSet<String>,
    activity_timeout_secs: u64,
    observer: Option<&dyn super::stream_io::FlowObserver>,
    // Transient-artifact capture (engine only): root + publication hook.
    // Intermediate chain sinks are captured HERE, mid-run, the moment they
    // materialize. ForEach bodies never capture (body guts are excluded by
    // design — region items/outputs have their own surfaces).
    transient_root: Option<&std::path::Path>,
    on_transient: Option<
        &(dyn Fn(&super::transient::TransientArtifact) -> Result<(), ExecutionError> + Sync),
    >,
) -> Result<DagOutput, ExecutionError> {
    let body_index = body_coordinate
        .as_ref()
        .map(|coordinate| coordinate.body_index);
    let groups = build_edge_groups(&graph.edges);
    let group_order = topological_sort_groups(&groups)?;
    let n_groups = group_order.len();

    // Seed the outputs with every already-materialised node (the initial inputs).
    let mut node_data: HashMap<String, Vec<Vec<u8>>> = ctx
        .node_data()
        .iter()
        .map(|(k, v)| (k.clone(), vec![v.clone()]))
        .collect();
    let mut node_is_sequence: HashMap<String, bool> = ctx.node_is_sequence().clone();
    let mut writer_results: HashMap<String, Vec<super::execute_plan::WriterResult>> =
        HashMap::new();
    let mut terminal_meta: HashMap<String, TerminalMeta> = HashMap::new();

    if n_groups == 0 {
        return Ok(DagOutput {
            node_data,
            node_is_sequence,
            writer_results,
            terminal_meta,
            node_spool: HashMap::new(),
        });
    }

    // Decompose the topologically-ordered cap groups into maximal LINEAR chains.
    // A group is a linear continuation of a single producer that feeds ONLY it;
    // every other group heads a chain. Fan-out (a producer feeding >1 group) and
    // fan-in (a group whose stdin/args come from >1 producer group) both break
    // chains, so every non-linear junction is resolved by materialising the
    // producer's output into node_data and feeding the downstream chain head from
    // it. A single linear machine yields exactly one structural chain before the
    // capacity partition below. Persisted terminal sinks each get their own writer
    // from the factory (fan-out ⇒ several); intermediate sinks stay in memory.
    //
    // A materialisation boundary is ALSO forced between two ADJACENT caps
    // that resolve to the SAME capacity-bounded cartridge: opening both
    // invocations before feeding the chain head deadlocks — the first
    // invocation owns the permit but has no input while opening the second
    // waits for that permit before the feed and relay pump exist. That is
    // the ONLY capacity case that must split: bounded caps sharing NO
    // bounded pool contend for nothing, and splitting them anyway would
    // materialise every intermediate — which an UNBOUNDED live pipeline
    // (13.2 §Reference Media) cannot survive (its intermediates must
    // stream; collection refuses unbounded, L16). The permit domain is the
    // POOL CHAIN (master + cartridge identity + pool), NOT the master
    // index: one relay slot can aggregate many cartridge processes
    // (machfab's external-cartridges RelaySlave), each with independent
    // pools — comparing master indices there would split every junction
    // and break live pipelines. Two same-cartridge caps in disjoint
    // bounded pools likewise share no permit and must NOT split.
    let linear_chains = decompose_group_chains(&groups, &group_order);
    let mut cap_admission: HashMap<
        String,
        Vec<(crate::bifaci::request_state::PoolKey, usize)>,
    > = HashMap::new();
    let mut group_chain: Vec<Vec<(crate::bifaci::request_state::PoolKey, usize)>> =
        Vec::with_capacity(groups.len());
    for group in groups.iter() {
        let admission_chain = if let Some(cached) = cap_admission.get(&group.cap_urn) {
            cached.clone()
        } else {
            if ctx
                .switch()
                .wait_for_cap(&group.cap_urn, CAP_DISPATCH_READY_TIMEOUT)
                .await
                .is_none()
            {
                return Err(ExecutionError::HostError(format!(
                    "resolve admission capacity for cap '{}': no master advertised a cap \
                     dispatchable for this request within {}s",
                    group.cap_urn,
                    CAP_DISPATCH_READY_TIMEOUT.as_secs(),
                )));
            }
            let target = ctx
                .switch()
                .admission_target_for_cap(&group.cap_urn)
                .await
                .map_err(|error| {
                    ExecutionError::HostError(format!(
                        "resolve admission capacity for cap '{}': {}",
                        group.cap_urn, error
                    ))
                })?;
            cap_admission.insert(group.cap_urn.clone(), target.clone());
            target
        };
        group_chain.push(admission_chain);
    }
    let chains = split_chains_at_shared_bounded_pool(linear_chains, &group_chain);
    let n_chains = chains.len();

    // Spool files created for unbounded intermediates this segment; consumed
    // by later chains in THIS loop, removed when the segment completes —
    // success or error (Drop), a leftover spool is temp litter, not state.
    struct SpoolCleanup(Vec<std::path::PathBuf>);
    impl Drop for SpoolCleanup {
        fn drop(&mut self) {
            for path in &self.0 {
                let _ = std::fs::remove_file(path);
            }
        }
    }
    let mut spool_files = SpoolCleanup(Vec::new());
    let mut node_spool: HashMap<String, std::path::PathBuf> = HashMap::new();

    for (ci, chain_idxs) in chains.iter().enumerate() {
        let chain_groups: Vec<EdgeGroup> =
            chain_idxs.iter().map(|&gi| groups[gi].clone()).collect();
        let sink = chain_groups
            .last()
            .expect("a chain has at least one group")
            .to
            .clone();

        // This sink persists to disk iff it is a plan terminal AND a writer factory
        // was supplied (the engine persists; the reference/in-memory path does not).
        // The writer is owned here, handed by borrow to the collect step, and
        // finalised into a per-sink `WriterResult` after.
        let mut writer: Option<Box<dyn super::stream_io::IncrementalWriter>> = match writer_factory
        {
            Some(f) if persist_sinks.contains(&sink) => Some(f(&sink, body_coordinate.clone())),
            _ => None,
        };
        let has_writer = writer.is_some();

        // An INTERMEDIATE sink (not a plan terminal) gets a lazy disk spool:
        // the collector engages it only if the stream declares UNBOUNDED, so
        // a mandatory split boundary never buffers unbounded data in memory
        // (L16) and never refuses a legitimate machine. Terminals keep the
        // strict contract: unbounded without a persisted sink is refused.
        let mut spool: Option<super::stream_io::SpoolWriter> = if persist_sinks.contains(&sink) {
            None
        } else {
            Some(super::stream_io::SpoolWriter::new(std::env::temp_dir().join(
                format!(
                    "capdag-spool-{}-{}",
                    std::process::id(),
                    uuid::Uuid::new_v4()
                ),
            )))
        };

        let progress_base = ci as f32 / n_chains as f32;
        let progress_span = 1.0 / n_chains as f32;

        let chain_result = run_group_chain(
            ctx,
            &chain_groups,
            cap_arguments,
            progress_fn,
            progress_base,
            progress_span,
            step_progress_fn,
            log_fn,
            body_index,
            stall_tracker.clone(),
            writer
                .as_mut()
                .map(|w| w.as_mut() as &mut dyn super::stream_io::IncrementalWriter),
            spool.as_mut(),
            activity_timeout_secs,
            observer,
        )
        .await;
        let (items, is_seq, meta) = match chain_result {
            Ok(v) => v,
            Err(e) => {
                // A failed chain's engaged spool is already on disk — remove
                // it here; later chains' spools are handled by the Drop guard.
                if let Some(sp) = &spool {
                    if sp.engaged() {
                        let _ = std::fs::remove_file(sp.path());
                    }
                }
                return Err(e);
            }
        };

        if let Some(w) = writer {
            writer_results
                .entry(sink.clone())
                .or_default()
                .push(w.finish());
        }

        let capture_transients = body_coordinate.is_none();
        let sink_out_media = chain_groups
            .last()
            .expect("a chain has at least one group")
            .edges
            .first()
            .expect("an edge group is non-empty by construction")
            .out_media
            .clone();
        let spool_engaged = spool.as_ref().is_some_and(|s| s.engaged());
        if spool_engaged {
            let sp = spool.take().expect("engaged spool exists");
            let path = sp.path().to_path_buf();
            if let Some(m) = sp.stream_meta() {
                ctx.set_node_meta(sink.clone(), m.clone());
            }
            ctx.set_node_is_sequence(sink.clone(), sp.is_sequence());
            match transient_root {
                Some(root) if capture_transients => {
                    // The spool IS the artifact: adopt it under the transient
                    // root — downstream chains and region drivers read the
                    // adopted path; the TTL reaper owns its lifetime, so it
                    // is NOT registered for segment-end deletion.
                    let artifact = super::transient::adopt_spool_as_transient(
                        root,
                        &sink,
                        &sink_out_media,
                        &path,
                        sp.is_sequence(),
                    )?;
                    ctx.set_node_spool(sink.clone(), artifact.data_path.clone());
                    node_spool.insert(sink.clone(), artifact.data_path.clone());
                    if let Some(publish) = on_transient {
                        publish(&artifact)?;
                    }
                }
                _ => {
                    ctx.set_node_spool(sink.clone(), path.clone());
                    spool_files.0.push(path.clone());
                    node_spool.insert(sink.clone(), path);
                }
            }
        }

        // Materialise this chain's sink so downstream chains' heads can read it (via
        // send_group_input from node_data). When persisted to a writer there are no
        // in-memory items and nothing downstream reads it; a spool-engaged sink's
        // data is on disk and downstream feeds stream from the file. Sequences
        // re-assemble to the CBOR sequence the next head will re-split.
        if !has_writer && !spool_engaged {
            // Sequence node_data is an RFC 8742 CBOR sequence of self-delimiting
            // values; `items` are the raw, unwrapped item bytes from
            // `decode_terminal_output`, so each must be re-encoded as a `CBOR::Bytes`
            // value here (scalar node_data stays raw — `send_one_stream` wraps it at
            // send). Dropping this re-encode is what broke sequence chains: raw PNG /
            // JSON bytes are not themselves CBOR.
            let bytes = if is_seq {
                crate::orchestrator::cbor_util::wrap_raw_items_as_cbor_sequence(&items).map_err(
                    |e| {
                        ExecutionError::HostError(format!("materialise chain output '{sink}': {e}"))
                    },
                )?
            } else {
                items.first().cloned().unwrap_or_default()
            };
            ctx.set_node_data(sink.clone(), bytes);
            ctx.set_node_is_sequence(sink.clone(), is_seq);
            if let (Some(root), true) = (transient_root, capture_transients) {
                // A bounded intermediate: one write, and the mid-strand node
                // becomes inspectable while later chains still run.
                let artifact = super::transient::capture_memory_intermediate(
                    root,
                    &sink,
                    &sink_out_media,
                    &items,
                    is_seq,
                )?;
                if let Some(publish) = on_transient {
                    publish(&artifact)?;
                }
            }
        }

        node_is_sequence.insert(sink.clone(), is_seq);
        terminal_meta.insert(sink.clone(), meta);
        // A spool-engaged sink's data lives ON DISK (`node_spool`) — the
        // in-memory `items` is only the pre-spool remainder. Recording it
        // here would present an (empty) in-memory value for a node that
        // produced a full stream, and any consumer that prefers memory over
        // spool (the ForEach region driver) would silently run on nothing.
        // One node, one truth: memory OR spool, never both.
        if !spool_engaged {
            node_data.insert(sink, items);
        }
    }

    // The segment completed: ownership of the spool files transfers to the
    // caller through `DagOutput.node_spool` (a ForEach region driver may
    // still stream from them) — disarm the error-path guard instead of
    // deleting. The caller (execute_plan) removes them at plan end.
    spool_files.0.clear();
    drop(spool_files);

    Ok(DagOutput {
        node_data,
        node_is_sequence,
        writer_results,
        terminal_meta,
        node_spool,
    })
}

/// Output of running a resolved DAG on a context: every node's decoded output items,
/// each node's cardinality, and — for persisted terminal sinks — the writer results
/// and per-sink terminal metadata. A fan-out DAG produces several sinks; each appears
/// here.
pub struct DagOutput {
    /// Node id → decoded output items (one for a scalar, N for a sequence). Persisted
    /// terminal sinks carry an empty vec (their data is on disk, in `writer_results`).
    pub node_data: HashMap<String, Vec<Vec<u8>>>,
    /// Node id → whether its output is a sequence.
    pub node_is_sequence: HashMap<String, bool>,
    /// Persisted terminal sink node id → its writer result(s).
    pub writer_results: HashMap<String, Vec<super::execute_plan::WriterResult>>,
    /// Terminal sink node id → its terminal metadata (titles, final progress, …).
    pub terminal_meta: HashMap<String, TerminalMeta>,
    /// UNBOUNDED intermediates spooled to disk at chain-split boundaries
    /// (L16): node id → spool path, in the node_data byte form. The caller
    /// owns the files (a ForEach region driver streams items from them) and
    /// removes them when the plan completes.
    pub node_spool: HashMap<String, std::path::PathBuf>,
}

/// Decompose topologically-ordered cap groups into maximal linear chains (lists of
/// indices into `groups`), mirroring `execute_plan::linear_chains` at the edge-group
/// level. A group folds into its producer's chain iff it has exactly one producer
/// group and that producer feeds ONLY it; otherwise it heads a new chain. Chains are
/// returned in head-topological order, so every chain's head inputs are materialised
/// by an earlier chain (a producer feeding a downstream chain head necessarily
/// fans out, so it is that earlier chain's sink).
fn decompose_group_chains(groups: &[EdgeGroup], group_order: &[usize]) -> Vec<Vec<usize>> {
    let n = groups.len();

    // node → the group index that produces it (a group produces its `to` node).
    let mut produced_by: HashMap<&str, usize> = HashMap::new();
    for (i, g) in groups.iter().enumerate() {
        produced_by.insert(g.to.as_str(), i);
    }

    // input_sources[i] = distinct producer groups feeding group i's input edges.
    // consumers[i] = groups that consume group i's output.
    let mut input_sources: Vec<HashSet<usize>> = (0..n).map(|_| HashSet::new()).collect();
    let mut consumers: Vec<HashSet<usize>> = (0..n).map(|_| HashSet::new()).collect();
    for (i, g) in groups.iter().enumerate() {
        for edge in &g.edges {
            if let Some(&src) = produced_by.get(edge.from.as_str()) {
                if src != i {
                    input_sources[i].insert(src);
                    consumers[src].insert(i);
                }
            }
        }
    }

    // A group continues its producer's chain iff it is a SINGLE-edge group with
    // exactly one producer group that feeds only this group. A multi-edge group
    // (fan-in: several args, or a gather of several producers into one sequence
    // arg) always heads a chain — mid-chain streaming forwards exactly one
    // producer stream, so a multi-input invocation must be fed from
    // materialised node_data via `send_group_input`. This also covers the group
    // whose extra input is an InputSlot (not a producer group): counting only
    // producer groups would otherwise misclassify it as a linear continuation
    // and silently drop the slot's stream.
    let continues_producer = |i: usize| -> bool {
        if groups[i].edges.len() == 1 && input_sources[i].len() == 1 {
            let src = *input_sources[i].iter().next().unwrap();
            return consumers[src].len() == 1;
        }
        false
    };

    let mut assigned: HashSet<usize> = HashSet::new();
    let mut chains: Vec<Vec<usize>> = Vec::new();
    for &start in group_order {
        if assigned.contains(&start) || continues_producer(start) {
            continue;
        }
        let mut chain = vec![start];
        assigned.insert(start);
        let mut tail = start;
        // Extend while the tail feeds exactly one consumer that is its linear
        // continuation (a single-edge group whose only producer is the tail —
        // a multi-edge group heads its own chain, see `continues_producer`).
        while consumers[tail].len() == 1 {
            let next = *consumers[tail].iter().next().unwrap();
            if groups[next].edges.len() == 1
                && input_sources[next].len() == 1
                && input_sources[next].contains(&tail)
            {
                chain.push(next);
                assigned.insert(next);
                tail = next;
            } else {
                break;
            }
        }
        chains.push(chain);
    }
    chains
}

/// Split each linear chain at the permit-deadlock boundary: between two
/// ADJACENT groups whose admission chains share a capacity-BOUNDED pool
/// (opening both invocations up front would have the second wait on the
/// pool slot the first holds while neither has input). A bounded
/// invocation owns a concrete pool slot from REQ through terminal
/// response, so it must receive its input and finish before a dependent
/// invocation contending for the same pool is acquired. Groups sharing no
/// bounded pool — different cartridges, or same-cartridge caps in
/// disjoint bounded pools — share no permit and keep streaming in one
/// live chain: the property unbounded (live-feed) pipelines depend on,
/// since only chain SINKS are collected/persisted and unbounded
/// intermediates cannot be materialised (L16).
fn split_chains_at_shared_bounded_pool<K: PartialEq>(
    chains: Vec<Vec<usize>>,
    group_chain: &[Vec<(K, usize)>],
) -> Vec<Vec<usize>> {
    let mut split = Vec::new();
    for chain in chains {
        let mut segment: Vec<usize> = Vec::new();
        for group_idx in chain {
            let deadlock_boundary = segment.last().is_some_and(|&prev| {
                group_chain[prev].iter().any(|(pool, prev_capacity)| {
                    group_chain[group_idx]
                        .iter()
                        .any(|(other, capacity)| {
                            pool == other && (*prev_capacity > 0 || *capacity > 0)
                        })
                })
            });
            if deadlock_boundary {
                split.push(std::mem::take(&mut segment));
            }
            segment.push(group_idx);
        }
        if !segment.is_empty() {
            split.push(segment);
        }
    }
    split
}

/// Execute ONE linear chain of cap groups as a live pipeline: the head's inputs are
/// read from `ctx`'s materialised node_data (a fan-in head sends N streams via
/// [`send_group_input`]), intermediate edges stream cap-to-cap through
/// [`forward_frames`] with per-hop credit, and the sink's output is collected.
/// Returns the sink's decoded items (empty when a `writer` persisted them to disk),
/// its cardinality, and terminal meta. Progress is scaled into
/// `[progress_base, progress_base + progress_span]`.
#[allow(clippy::too_many_arguments)]

/// Send an upstream CREDIT grant, tolerating exactly one failure mode: the
/// request no longer existing because it TERMINATED while the grant was being
/// prepared. Credits for a finished request are moot — failing the body for
/// one would turn the ordinary teardown race of credit-based flow control
/// into a spurious execution failure. Every other send failure is a real
/// host error and stays hard.
async fn send_upstream_credit(
    switch: &std::sync::Arc<crate::bifaci::relay_switch::RelaySwitch>,
    rid: crate::bifaci::frame::MessageId,
    stream_id: String,
    n: u64,
    context: &str,
) -> Result<(), ExecutionError> {
    let credit = crate::bifaci::frame::Frame::credit(
        rid,
        Some(stream_id),
        n,
        crate::bifaci::frame::CreditDirection::Response,
    );
    match switch.send_to_master(credit, None).await {
        Ok(()) => Ok(()),
        Err(crate::bifaci::relay_switch::RelaySwitchError::UnknownRequest(rid)) => {
            tracing::debug!(
                rid = %rid,
                context,
                "upstream grant arrived after the request terminated — moot, not an error"
            );
            Ok(())
        }
        Err(e) => Err(ExecutionError::HostError(format!("{context}: {e}"))),
    }
}

async fn run_group_chain(
    ctx: &ExecutionContext,
    chain: &[EdgeGroup],
    cap_arguments: &HashMap<String, Vec<(String, Vec<u8>)>>,
    progress_fn: Option<&CapProgressFn>,
    progress_base: f32,
    progress_span: f32,
    step_progress_fn: Option<&CapStepProgressFn>,
    log_fn: Option<&PipelineLogFn>,
    body_index: Option<usize>,
    stall_tracker: Option<Arc<PipelineProgressTracker>>,
    writer: Option<&mut dyn super::stream_io::IncrementalWriter>,
    mut spool: Option<&mut super::stream_io::SpoolWriter>,
    activity_timeout_secs: u64,
    observer: Option<&dyn super::stream_io::FlowObserver>,
) -> Result<(Vec<Vec<u8>>, bool, TerminalMeta), ExecutionError> {
    use tokio::sync::mpsc;

    // Whether the terminal is being persisted to disk by the caller's writer. The
    // writer itself is a borrow we hand to the collect step; the caller owns it
    // and finalizes it after we return.
    let has_writer = writer.is_some();

    let n = chain.len();
    let ordered_groups: Vec<&EdgeGroup> = chain.iter().collect();

    // Per-group progress base within this chain, scaled into the chain's span.
    let group_base = |i: usize| progress_base + progress_span * (i as f32 / n as f32);
    let group_weight = progress_span / n as f32;

    let switch = ctx.switch().clone();
    let max_chunk = ctx.max_chunk();
    let initial_credit = switch.limits().await.initial_credit;

    // ── Step 1: Set up all cap invocations ──
    // Each execute_cap sends REQ and returns (rid, response_rx). Each invocation
    // gets a CreditRouter: inbound CREDIT frames on its response channel route to
    // the gates registered by whoever feeds that cap (L14).
    let mut invocations: Vec<(MessageId, mpsc::UnboundedReceiver<Frame>, String)> =
        Vec::with_capacity(n);
    let mut credit_routers: Vec<crate::bifaci::credit::CreditRouter> = Vec::with_capacity(n);
    for group in &ordered_groups {
        let cap_urn = &group.cap_urn;
        // Cap registration is async: a freshly attached cartridge host's
        // RelayNotify (its real cap_groups) arrives some time after `add_master`
        // returns. Poll until some master advertises a cap this request CONFORMS
        // to (`find_master_for_cap` matches by tagged-URN dispatchability, never
        // string equality) — the synchronization point that lets dispatch route
        // correctly. On the engine's long-lived switch the cap is already present
        // and this returns on the first probe; bounded so a genuinely unprovided
        // cap surfaces as a typed error rather than hanging on execute_cap.
        if switch
            .wait_for_cap(cap_urn, CAP_DISPATCH_READY_TIMEOUT)
            .await
            .is_none()
        {
            return Err(ExecutionError::HostError(format!(
                "execute_cap '{}': no master advertised a cap dispatchable for \
                 this request within {}s — RelayNotify never arrived, the \
                 identity probe failed, or no candidate conforms to this cap",
                cap_urn,
                CAP_DISPATCH_READY_TIMEOUT.as_secs(),
            )));
        }
        let (rid, rx) = switch
            .execute_cap(cap_urn, vec![], "application/cbor")
            .await
            .map_err(|e| match e {
                // Preserve the switch's classification instead of flattening
                // every switch failure into `internal`.
                crate::bifaci::relay_switch::RelaySwitchError::CartridgeUnavailable(details) => {
                    ExecutionError::CartridgeUnavailable {
                        cap_urn: cap_urn.clone(),
                        details,
                    }
                }
                other => {
                    ExecutionError::HostError(format!("execute_cap '{}': {}", cap_urn, other))
                }
            })?;
        // Correlate this invocation to its strand step for the run's live flow
        // snapshots (L8): the only point where both ids exist.
        if let Some(obs) = observer {
            obs.record(&rid, &group.token_id);
        }
        invocations.push((rid, rx, cap_urn.clone()));
        credit_routers.push(crate::bifaci::credit::CreditRouter::new());
    }

    // The chain HEAD is FEED-BEARING when any of its inputs is a live
    // reference: the selector we send it resolves into an open device tap in
    // the receiving cartridge. Recorded so the stop-input control can close
    // exactly these taps (non-force Cancel → close-tap → drain, 15.2 §Runs
    // Stop) without touching other requests.
    if let Some(obs) = observer {
        let head_group = ordered_groups[0];
        if head_group
            .edges
            .iter()
            .any(|edge| ctx.node_live_reference().contains_key(&edge.from))
        {
            obs.record_feed_bearing(&invocations[0].0);
        }
    }

    // ── Step 2: Spawn pump task ──
    // Reads from switch.read_from_masters_timeout to route peer requests. Without
    // this, cartridge→cartridge peer calls would deadlock. The pump must exit via
    // the stop flag — never via abort() (aborting can drop a frame mid-route).
    let pump_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let pump_stop_flag = pump_stop.clone();
    let pump_switch = switch.clone();
    let pump_handle = tokio::spawn(async move {
        loop {
            if pump_stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
            match pump_switch
                .read_from_masters_timeout(std::time::Duration::from_millis(200))
                .await
            {
                Ok(Some(_frame)) => {}
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!("[pipeline pump] relay error (continuing): {}", e);
                }
            }
        }
    });

    // ── Step 3: Feed first group's input CONCURRENTLY (L15) ──
    let send_task = {
        let switch = switch.clone();
        let first_rid = invocations[0].0.clone();
        let first_group = (*ordered_groups[0]).clone();
        let cap_arguments = cap_arguments.clone();
        let node_data = ctx.node_data().clone();
        let node_meta = ctx.node_meta().clone();
        let node_is_sequence = ctx.node_is_sequence().clone();
        let node_live_reference = ctx.node_live_reference().clone();
        let node_spool = ctx.node_spool().clone();
        let router = credit_routers[0].clone();
        tokio::spawn(async move {
            send_group_input(
                &switch,
                &first_rid,
                &first_group,
                &cap_arguments,
                &node_data,
                &node_meta,
                &node_is_sequence,
                &node_live_reference,
                &node_spool,
                max_chunk,
                Some((&router, initial_credit)),
            )
            .await
        })
    };

    // ── Step 4: Spawn forwarding tasks for intermediate groups ──
    let mut forwarding_handles: Vec<tokio::task::JoinHandle<Result<(), ExecutionError>>> =
        Vec::new();
    for i in 0..(n - 1) {
        let next_rid = invocations[i + 1].0.clone();
        let next_group = ordered_groups[i + 1];
        let prev_cap_urn = invocations[i].2.clone();

        // The downstream cap demuxes input streams by arg URN equivalence
        // (spec 13.2) — every stream the orchestrator sends is labeled with
        // the edge's declared input media URN (`send_group_input` does this
        // for chain heads). The pipelined hop must do the same: find the
        // edge this forwarding serves so STREAM_START can be relabeled from
        // the producer's (richer) output URN to the declared arg URN.
        let prev_to = &ordered_groups[i].to;
        let mut feeding_edges = next_group.edges.iter().filter(|e| &e.from == prev_to);
        let next_in_media = match (feeding_edges.next(), feeding_edges.next()) {
            (Some(edge), None) => edge.in_media.clone(),
            (Some(a), Some(b)) => {
                return Err(ExecutionError::HostError(format!(
                    "pipelined forward: node '{}' feeds cap '{}' through multiple \
                     edges (args '{}' and '{}') — a single forwarded stream cannot \
                     be split into two args",
                    prev_to, next_group.cap_urn, a.in_media, b.in_media
                )));
            }
            (None, _) => {
                return Err(ExecutionError::HostError(format!(
                    "pipelined forward: chain wiring bug — group for cap '{}' has \
                     no edge from its producer node '{}'",
                    next_group.cap_urn, prev_to
                )));
            }
        };

        let (dummy_tx, dummy_rx) = mpsc::unbounded_channel();
        let taken_rx = std::mem::replace(&mut invocations[i].1, dummy_rx);
        drop(dummy_tx);

        let fwd_switch = switch.clone();
        let extra_args: Vec<(String, Vec<u8>)> = cap_arguments
            .get(&next_group.to)
            .cloned()
            .unwrap_or_default();

        let group_token_id = ordered_groups[i].token_id.clone();
        let fwd_step_token_id = group_token_id.clone();
        let pfn: Option<CapProgressFn> = progress_fn.map(|parent| {
            let base = group_base(i);
            let weight = group_weight;
            let mapper = ProgressMapper::new(parent, base, weight);
            let mapper = match step_progress_fn {
                Some(sink) => mapper.with_step_sink(sink, &group_token_id),
                None => mapper,
            };
            mapper.as_cap_progress_fn()
        });
        let fwd_max_chunk = max_chunk;
        let fwd_log_fn = log_fn.cloned();
        let fwd_body_index = body_index;
        let fwd_stall_tracker = stall_tracker.clone();
        let prev_rid = invocations[i].0.clone();
        let router_up = credit_routers[i].clone();
        let router_down = credit_routers[i + 1].clone();

        forwarding_handles.push(tokio::spawn(async move {
            forward_frames(
                taken_rx,
                &fwd_switch,
                prev_rid,
                next_rid,
                router_up,
                router_down,
                initial_credit,
                &extra_args,
                fwd_max_chunk,
                pfn.as_ref(),
                &prev_cap_urn,
                &fwd_step_token_id,
                &next_in_media,
                fwd_log_fn.as_ref(),
                fwd_body_index,
                fwd_stall_tracker,
                activity_timeout_secs,
            )
            .await
        }));
    }

    // ── Step 5: Collect last group's output ──
    let last_idx = n - 1;
    let last_cap_urn_owned = invocations[last_idx].2.clone();
    let last_cap_urn = last_cap_urn_owned.as_str();
    let last_rid = invocations[last_idx].0.clone();
    let (dummy_tx2, dummy_rx2) = mpsc::unbounded_channel();
    let taken_last_rx = std::mem::replace(&mut invocations[last_idx].1, dummy_rx2);
    drop(dummy_tx2);

    let last_group_token_id = ordered_groups[last_idx].token_id.clone();
    let last_pfn: Option<CapProgressFn> = progress_fn.map(|parent| {
        let base = group_base(last_idx);
        let weight = group_weight;
        let mapper = ProgressMapper::new(parent, base, weight);
        let mapper = match step_progress_fn {
            Some(sink) => mapper.with_step_sink(sink, &last_group_token_id),
            None => mapper,
        };
        mapper.as_cap_progress_fn()
    });

    // Terminal credit plumbing: grant the last cap's output as we consume it, and
    // route its inbound CREDIT frames to the gates feeding it (L10/L14).
    let terminal_plumbing = {
        let grant_switch = switch.clone();
        let grant_rid = last_rid.clone();
        let grant: super::stream_io::CreditGrantFn = Arc::new(move |stream_id, n| {
            let switch = grant_switch.clone();
            let rid = grant_rid.clone();
            tokio::spawn(async move {
                let frame = Frame::credit(
                    rid,
                    stream_id,
                    n,
                    crate::bifaci::frame::CreditDirection::Response,
                );
                match switch.send_to_master(frame, None).await {
                    Ok(()) => {}
                    Err(crate::bifaci::relay_switch::RelaySwitchError::UnknownRequest(rid)) => {
                        // The request terminated while this grant was in
                        // flight — the ordinary teardown race, moot.
                        tracing::debug!(
                            rid = %rid,
                            "[pipeline] terminal grant after termination — moot"
                        );
                    }
                    Err(e) => {
                        // Any OTHER failure is a real host problem; a debug
                        // line would bury it. It cannot propagate from this
                        // spawned task, so it is at least loud.
                        tracing::error!("[pipeline] terminal grant failed: {}", e);
                    }
                }
            });
        });
        super::stream_io::CreditPlumbing {
            router: credit_routers[last_idx].clone(),
            grant,
            batch: (initial_credit / 2).max(1),
        }
    };

    let collect_result = super::stream_io::collect_terminal_output(
        taken_last_rx,
        last_pfn.as_ref(),
        last_cap_urn,
        &last_group_token_id,
        log_fn,
        body_index,
        stall_tracker.as_ref(),
        writer,
        spool
            .as_deref_mut()
            .map(|s| s as &mut dyn super::stream_io::IncrementalWriter),
        activity_timeout_secs,
        Some(&terminal_plumbing),
    )
    .await
    .map_err(|e| match e {
        // The terminal cap failure keeps its declared identity
        // (docs/failure-taxonomy.md) — never flattened into HostError.
        super::stream_io::StreamIoError::Terminal {
            cap_urn,
            code,
            class,
            details,
            arg_urn,
        } => ExecutionError::CartridgeExecutionFailed {
            cap_urn,
            code,
            class,
            details,
            arg_urn,
        }
        .at_step(&last_group_token_id),
        // An effect-contract violation keeps its typed identity (the audit
        // fired at receipt inside collect) — never flattened into HostError.
        super::stream_io::StreamIoError::EffectContract {
            cap_urn,
            effect,
            runtime_input,
            expected,
            actual,
        } => ExecutionError::EffectContractViolation {
            cap_urn,
            effect,
            runtime_input,
            expected,
            actual,
        }
        .at_step(&last_group_token_id),
        other => ExecutionError::HostError(other.to_string()),
    });

    // ── Step 6: Terminal — release credit waiters (L13), stop pump, join ──
    for ((rid, _, _), router) in invocations.iter().zip(credit_routers.iter()) {
        router.close_request(rid, "terminal");
    }
    pump_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = pump_handle.await;

    let mut first_error: Option<ExecutionError> = None;
    match send_task.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            first_error.get_or_insert(e.at_step(&ordered_groups[0].token_id));
        }
        Err(e) => {
            first_error.get_or_insert(
                ExecutionError::HostError(format!("Input send task panicked: {}", e))
                    .at_step(&ordered_groups[0].token_id),
            );
        }
    }
    for (i, handle) in forwarding_handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                first_error.get_or_insert(e.at_step(&ordered_groups[i].token_id));
            }
            Err(e) => {
                first_error.get_or_insert(
                    ExecutionError::HostError(format!("Forwarding task {} panicked: {}", i, e))
                        .at_step(&ordered_groups[i].token_id),
                );
            }
        }
    }

    let (output_bytes, is_sequence, terminal_meta) = match collect_result {
        Ok(ok) => {
            if let Some(err) = first_error {
                // The chain broke upstream — a terminal END is not trustworthy.
                // Cancel the chain and fail hard.
                for (rid, _, _) in &invocations {
                    switch.cancel_request(rid, false).await;
                }
                return Err(err);
            }
            ok
        }
        Err(e) => {
            for (rid, _, _) in &invocations {
                switch.cancel_request(rid, false).await;
            }
            return Err(first_error.unwrap_or(e));
        }
    };

    if let Some(pfn) = &progress_fn {
        pfn(progress_base + progress_span, last_cap_urn, "Completed");
    }

    // ── Step 7: Decode this chain's sink output ──
    let terminal_is_sequence = is_sequence.unwrap_or(false);

    // Writer present (or the intermediate spool engaged) ⇒ data is on disk,
    // no in-memory items. Otherwise decode the accumulated bytes (CBOR
    // transport stripped).
    let spool_engaged = spool.as_ref().is_some_and(|s| s.engaged());
    let decoded_items = if has_writer || spool_engaged {
        vec![]
    } else {
        super::stream_io::decode_terminal_output(&output_bytes, is_sequence)
            .map_err(|e| ExecutionError::HostError(e.to_string()))?
    };

    Ok((decoded_items, terminal_is_sequence, terminal_meta))
}

/// Send a group's input frames from `node_data`: one named STREAM per edge
/// (fan-in head sends N streams into the one cap), plus any extra cap-argument
/// streams, then END. Provenance meta on the source node is forwarded on
/// STREAM_START. Every source node MUST carry a sequence flag — a missing flag
/// is a wiring bug and fails hard.
#[allow(clippy::too_many_arguments)]
async fn send_group_input(
    switch: &Arc<RelaySwitch>,
    rid: &MessageId,
    group: &EdgeGroup,
    cap_arguments: &HashMap<String, Vec<(String, Vec<u8>)>>,
    node_data: &HashMap<String, Vec<u8>>,
    node_meta: &HashMap<String, crate::StreamMeta>,
    node_is_sequence: &HashMap<String, bool>,
    node_live_reference: &HashMap<String, String>,
    node_spool: &HashMap<String, std::path::PathBuf>,
    max_chunk: usize,
    credit: Option<(&crate::bifaci::credit::CreditRouter, u64)>,
) -> Result<(), ExecutionError> {
    // One incoming payload per edge, grouped by arg URN (tagged-URN
    // equivalence). The cartridge demuxes incoming streams by URN, so exactly
    // ONE stream per distinct URN may go out:
    //  - a single-edge group sends as-is (the historical path);
    //  - an N-edge group is a GATHER into a sequence arg (N distinct producers
    //    feeding one arg — the resolver's implicit Collect). Legal ONLY when
    //    the cap's matching arg declares `is_sequence` and every member payload
    //    is scalar; the N scalar payloads then concatenate — in edge order,
    //    which is the resolver's source-declaration order — into one CBOR
    //    sequence sent with is_sequence=true. A scalar arg receiving two
    //    streams (the illegal "two stdins" case) or a sequence member inside a
    //    gather is a malformed graph — fail hard, never silently double-send.
    enum MemberSource<'a> {
        Mem(&'a [u8]),
        /// The member's data lives in a disk spool (unbounded intermediate at
        /// a chain-split boundary) — streamed via `send_file_stream`.
        Spool(&'a std::path::Path),
    }
    struct UrnGroup<'a> {
        urn: crate::MediaUrn,
        urn_str: &'a str,
        members: Vec<(MemberSource<'a>, Option<&'a crate::StreamMeta>, bool)>,
    }
    let mut urn_groups: Vec<UrnGroup> = Vec::new();
    for edge in &group.edges {
        let data: MemberSource = if let Some(path) = node_spool.get(&edge.from) {
            MemberSource::Spool(path.as_path())
        } else {
            MemberSource::Mem(
                node_data
                    .get(&edge.from)
                    .ok_or_else(|| {
                        ExecutionError::HostError(format!(
                            "Missing input data at node '{}' for cap '{}'",
                            edge.from, edge.cap_urn
                        ))
                    })?
                    .as_slice(),
            )
        };
        let meta = node_meta.get(&edge.from);
        let is_seq = *node_is_sequence.get(&edge.from).ok_or_else(|| {
            ExecutionError::HostError(format!(
                "Missing sequence flag at node '{}' for cap '{}'. Either the input \
                 map was constructed without a flag for this node, or an intermediate \
                 cap completed without setting its sequence flag — both are bugs.",
                edge.from, edge.cap_urn,
            ))
        })?;
        // A live-feed reference node sends its selector labeled with the
        // REFERENCE urn — never relabeled to the edge's in_media — so the
        // receiving cartridge's demux intercepts it and resolves capture
        // (13.2 §Reference Media). The reference is ONE record: wire
        // is_sequence=false regardless of the node's content cardinality.
        if let Some(reference_urn) = node_live_reference.get(&edge.from) {
            let urn = crate::MediaUrn::from_string(reference_urn).map_err(|e| {
                ExecutionError::HostError(format!(
                    "live-feed reference URN '{}' at node '{}' is not a valid media URN: {e}",
                    reference_urn, edge.from
                ))
            })?;
            if urn_groups
                .iter()
                .any(|g| g.urn.is_equivalent(&urn).unwrap_or(false))
            {
                return Err(ExecutionError::HostError(format!(
                    "cap '{}' would receive two live-feed reference streams '{}' — a \
                     device capture cannot be fanned into one cap twice",
                    edge.cap_urn, reference_urn
                )));
            }
            urn_groups.push(UrnGroup {
                urn,
                urn_str: reference_urn.as_str(),
                members: vec![(data, meta, false)],
            });
            continue;
        }
        let urn = crate::MediaUrn::from_string(&edge.in_media).map_err(|e| {
            ExecutionError::HostError(format!(
                "input arg URN '{}' for cap '{}' is not a valid media URN: {e}",
                edge.in_media, edge.cap_urn
            ))
        })?;
        match urn_groups
            .iter_mut()
            .find(|g| g.urn.is_equivalent(&urn).unwrap_or(false))
        {
            Some(g) => g.members.push((data, meta, is_seq)),
            None => urn_groups.push(UrnGroup {
                urn,
                urn_str: edge.in_media.as_str(),
                members: vec![(data, meta, is_seq)],
            }),
        }
    }

    let extra_args = cap_arguments
        .get(&group.to)
        .map(|v| v.as_slice())
        .unwrap_or(&[]);

    // Extra-arg streams must not collide with each other or with any input
    // stream URN — they are value-channel deliveries, never gathered.
    {
        let mut seen_urns: Vec<crate::MediaUrn> =
            urn_groups.iter().map(|g| g.urn.clone()).collect();
        for (urn_str, _) in extra_args {
            let urn = crate::MediaUrn::from_string(urn_str).map_err(|e| {
                ExecutionError::HostError(format!(
                    "input arg URN '{urn_str}' for cap '{}' is not a valid media URN: {e}",
                    group.cap_urn
                ))
            })?;
            if seen_urns
                .iter()
                .any(|s| s.is_equivalent(&urn).unwrap_or(false))
            {
                return Err(ExecutionError::HostError(format!(
                    "cap '{}' receives two input streams with the same arg URN '{urn_str}'; \
                     each input must carry a distinct arg URN — a cap has one main input, \
                     and convergence args have distinct URNs",
                    group.cap_urn
                )));
            }
            seen_urns.push(urn);
        }
    }

    // The full cap definition rides on every edge of the group; a group is one
    // cap invocation, so the first edge's definition is the group's.
    let cap_def = &group
        .edges
        .first()
        .expect("an edge group is non-empty by construction")
        .cap;

    for urn_group in &urn_groups {
        if urn_group.members.len() == 1 {
            let (ref data, meta, is_seq) = urn_group.members[0];
            match data {
                MemberSource::Mem(bytes) => {
                    super::stream_io::send_one_stream(
                        switch,
                        rid,
                        urn_group.urn_str,
                        bytes,
                        meta.cloned(),
                        is_seq,
                        max_chunk,
                        credit,
                    )
                    .await
                    .map_err(|e| ExecutionError::HostError(e.to_string()))?;
                }
                MemberSource::Spool(path) => {
                    super::stream_io::send_file_stream(
                        switch,
                        rid,
                        urn_group.urn_str,
                        path,
                        meta.cloned(),
                        is_seq,
                        max_chunk,
                        credit,
                    )
                    .await
                    .map_err(|e| ExecutionError::HostError(e.to_string()))?;
                }
            }
            continue;
        }

        // Gather: verify the receiving arg is a sequence arg.
        let arg_def = cap_def
            .args
            .iter()
            .find(|a| {
                crate::MediaUrn::from_string(a.stream_urn())
                    .map(|u| u.is_equivalent(&urn_group.urn).unwrap_or(false))
                    .unwrap_or(false)
            })
            .ok_or_else(|| {
                ExecutionError::HostError(format!(
                    "cap '{}' receives {} input streams on arg URN '{}', but no cap arg \
                     declares that stream URN — malformed graph",
                    group.cap_urn,
                    urn_group.members.len(),
                    urn_group.urn_str
                ))
            })?;
        if !arg_def.is_sequence {
            return Err(ExecutionError::HostError(format!(
                "cap '{}' receives {} input streams with the same arg URN '{}', but that \
                 arg is not a sequence arg; each scalar input must carry a distinct arg \
                 URN — a cap has one main input, and convergence args have distinct URNs",
                group.cap_urn,
                urn_group.members.len(),
                urn_group.urn_str
            )));
        }
        // Assemble deterministically, in edge (= source-declaration) order: a
        // scalar member contributes one item, a sequence member its items,
        // and a SPOOLED member (an ended unbounded intermediate) streams its
        // data from the file — a gather never re-buffers an unbounded
        // stream into memory.
        let members: Vec<super::stream_io::GatherMember> = urn_group
            .members
            .iter()
            .map(|(data, _, member_is_seq)| match data {
                MemberSource::Mem(bytes) => super::stream_io::GatherMember::Memory {
                    data: bytes.to_vec(),
                    is_sequence: *member_is_seq,
                },
                MemberSource::Spool(path) => super::stream_io::GatherMember::Spooled {
                    path: path.to_path_buf(),
                    is_sequence: *member_is_seq,
                },
            })
            .collect();
        super::stream_io::send_gathered_stream(
            switch,
            rid,
            urn_group.urn_str,
            members,
            max_chunk,
            credit,
        )
        .await
        .map_err(|e| ExecutionError::HostError(e.to_string()))?;
    }
    for (media_urn, data) in extra_args {
        super::stream_io::send_one_stream(
            switch, rid, media_urn, data, None, false, max_chunk, credit,
        )
        .await
        .map_err(|e| ExecutionError::HostError(e.to_string()))?;
    }

    let end_frame = Frame::end(rid.clone(), None);
    switch
        .send_to_master(end_frame, None)
        .await
        .map_err(|e| ExecutionError::HostError(format!("END: {}", e)))?;

    Ok(())
}

/// Forward one cap's response frames live into the next cap's input, re-stamping
/// request/stream ids for wire fidelity. STREAM_START is relabeled from the
/// producer's output URN to `next_in_media` — the downstream edge's declared
/// input arg URN — because the consumer demuxes args by URN equivalence
/// (spec 13.2); the plan proved the producer's output conforms, and that
/// conformance is re-asserted here (a non-conforming stream is a wiring bug,
/// failed hard). Credit flows both ways per hop (L10/L14):
/// forwarded chunks acquire from the downstream cap's window; consumed upstream
/// chunks are granted back. On the upstream END, the next group's extra-arg
/// streams are sent, then END. LOG frames drive progress/log callbacks. Activity
/// silence surfaces as a one-shot warning — never an abort (long caps are legit;
/// cancellation is the user's explicit path).
#[allow(clippy::too_many_arguments)]
async fn forward_frames(
    mut prev_rx: tokio::sync::mpsc::UnboundedReceiver<Frame>,
    switch: &Arc<RelaySwitch>,
    prev_rid: MessageId,
    next_rid: MessageId,
    router_up: crate::bifaci::credit::CreditRouter,
    router_down: crate::bifaci::credit::CreditRouter,
    initial_credit: u64,
    extra_args: &[(String, Vec<u8>)],
    max_chunk: usize,
    progress_fn: Option<&CapProgressFn>,
    prev_cap_urn: &str,
    prev_step_token_id: &StepToken,
    next_in_media: &str,
    log_fn: Option<&PipelineLogFn>,
    body_index: Option<usize>,
    stall_tracker: Option<Arc<PipelineProgressTracker>>,
    activity_timeout_secs: u64,
) -> Result<(), ExecutionError> {
    use crate::bifaci::credit::CreditGate;
    use crate::bifaci::frame::CreditDirection;
    use std::time::Duration;

    let target_arg_urn = crate::MediaUrn::from_string(next_in_media).map_err(|e| {
        ExecutionError::HostError(format!(
            "pipelined forward: downstream arg URN '{}' is not a valid media URN: {}",
            next_in_media, e
        ))
    })?;

    // The producer's emission is audited against its declared effect
    // contract BEFORE the relabel below — the relabel deliberately coarsens
    // the label to the downstream declared arg URN for demux (spec 13.2),
    // so an unaudited relabel would substitute the plan's belief for the
    // cartridge's claim and mask a lying cap.
    let effect_audit =
        super::stream_io::EffectAudit::new(prev_cap_urn).map_err(|e| {
            ExecutionError::HostError(format!("pipelined forward: {}", e))
        })?;

    let mut stream_id_map: HashMap<String, (String, Arc<CreditGate>, u64)> = HashMap::new();
    let grant_batch = (initial_credit / 2).max(1);
    let mut timer = super::stream_io::ActivityTimer::new(activity_timeout_secs);
    let mut activity_warning_logged = false;

    loop {
        let frame = tokio::time::timeout(Duration::from_millis(500), prev_rx.recv()).await;

        match frame {
            Ok(Some(frame)) => {
                if let Some(ref tracker) = stall_tracker {
                    tracker.touch();
                }
                activity_warning_logged = false;
                match frame.frame_type {
                    FrameType::StreamStart => {
                        timer.touch();
                        let prev_sid = frame.stream_id.clone().unwrap_or_default();
                        let new_sid = uuid::Uuid::new_v4().to_string();
                        let gate = Arc::new(CreditGate::new(initial_credit));
                        router_down.register(
                            next_rid.clone(),
                            Some(new_sid.clone()),
                            Arc::clone(&gate),
                        );
                        stream_id_map.insert(prev_sid, (new_sid.clone(), gate, 0));

                        // Effect audit FIRST, on the producer's own claim: the
                        // emission must satisfy the producer cap's declared
                        // effect contract before anything else touches it.
                        effect_audit
                            .audit(frame.media_urn.as_deref())
                            .map_err(|e| match e {
                                super::stream_io::StreamIoError::EffectContract {
                                    cap_urn,
                                    effect,
                                    runtime_input,
                                    expected,
                                    actual,
                                } => ExecutionError::EffectContractViolation {
                                    cap_urn,
                                    effect,
                                    runtime_input,
                                    expected,
                                    actual,
                                },
                                other => ExecutionError::HostError(format!(
                                    "pipelined forward: {}",
                                    other
                                )),
                            })?;

                        // Then relabel to the downstream edge's declared arg
                        // URN: the consumer matches input streams by arg-URN
                        // equivalence (spec 13.2), and the producer's audited
                        // output URN is a refinement the plan already proved
                        // conforms. Re-assert that conformance — a violation
                        // is a wiring/type bug that must surface here, not as
                        // a missing-arg error inside the cartridge. The
                        // comparison itself failing is equally hard: an
                        // unanswerable conformance question is an engine
                        // inconsistency, never a pass.
                        let produced_urn_str = frame.media_urn.as_deref().unwrap_or_default();
                        let produced_urn =
                            crate::MediaUrn::from_string(produced_urn_str).map_err(|e| {
                                ExecutionError::HostError(format!(
                                    "pipelined forward: cap '{}' emitted a stream with \
                                     invalid media URN '{}': {}",
                                    prev_cap_urn, produced_urn_str, e
                                ))
                            })?;
                        let conforms =
                            produced_urn.conforms_to(&target_arg_urn).map_err(|e| {
                                ExecutionError::HostError(format!(
                                    "pipelined forward: cap '{}' output URN '{}' could not \
                                     be compared to downstream arg URN '{}': {}",
                                    prev_cap_urn, produced_urn_str, next_in_media, e
                                ))
                            })?;
                        if !conforms {
                            return Err(ExecutionError::HostError(format!(
                                "pipelined forward: cap '{}' output stream URN '{}' does \
                                 not conform to the downstream declared arg URN '{}'",
                                prev_cap_urn, produced_urn_str, next_in_media
                            )));
                        }

                        let mut new_frame = frame.clone();
                        new_frame.id = next_rid.clone();
                        new_frame.routing_id = None;
                        new_frame.seq = 0;
                        new_frame.stream_id = Some(new_sid);
                        new_frame.media_urn = Some(next_in_media.to_string());
                        switch.send_to_master(new_frame, None).await.map_err(|e| {
                            ExecutionError::HostError(format!("forward STREAM_START: {}", e))
                        })?;
                    }
                    FrameType::Chunk => {
                        timer.touch();
                        let prev_sid = frame.stream_id.clone().unwrap_or_default();
                        let has_window = {
                            let (_, gate, _) = stream_id_map.get(&prev_sid).ok_or_else(|| {
                                ExecutionError::HostError(format!(
                                    "forward CHUNK: unknown stream_id '{}'",
                                    prev_sid
                                ))
                            })?;
                            match gate.try_acquire(1) {
                                Ok(w) => w,
                                // A closed downstream gate means the consumer's
                                // request terminated (Step 6 `close_request`) or was
                                // cancelled. By the credit contract the sender must
                                // stop; the cap's real outcome is carried by its own
                                // END/ERR (collected separately), so stopping here is
                                // success, not a forwarding failure. Turning it into an
                                // ExecutionError is what broke fan-in/fan-out chains
                                // (a cap that ends before draining its input closes the
                                // gate while the upstream is still forwarding).
                                Err(closed) => {
                                    tracing::debug!(
                                        "[pipeline] forwarding stops early: downstream credit gate closed ({})",
                                        closed.reason
                                    );
                                    return Ok(());
                                }
                            }
                        };
                        if !has_window {
                            // Flush-before-block (L10 corollary): the downstream
                            // acquire is about to wait, and the upstream producer
                            // may be stalled on exactly the sub-batch grants we hold.
                            for (flush_sid, (_, _, consumed)) in stream_id_map.iter_mut() {
                                if *consumed > 0 {
                                    let n = *consumed;
                                    *consumed = 0;
                                    send_upstream_credit(
                                        &switch,
                                        prev_rid.clone(),
                                        flush_sid.clone(),
                                        n,
                                        "flush upstream CREDIT",
                                    )
                                    .await?;
                                }
                            }
                            let (_, gate, _) = stream_id_map.get(&prev_sid).ok_or_else(|| {
                                ExecutionError::HostError(format!(
                                    "forward CHUNK: unknown stream_id '{}'",
                                    prev_sid
                                ))
                            })?;
                            // Hard block on downstream credit — but never
                            // silently: this await is outside the main loop's
                            // 500ms heartbeat, so a starved credit edge here
                            // is otherwise invisible (the exact shape of the
                            // rare pipelined-chain deadlock). Surface the
                            // blocked state with its credit balance at the
                            // activity-timeout cadence while continuing to
                            // wait (L8: stalls are observable, never silent).
                            loop {
                                match tokio::time::timeout(
                                    Duration::from_secs(activity_timeout_secs.max(1)),
                                    gate.acquire(1),
                                )
                                .await
                                {
                                    Ok(Ok(())) => break,
                                    Ok(Err(closed)) => {
                                        // Downstream consumer terminated while we
                                        // waited for credit — stop forwarding
                                        // gracefully (see the try_acquire arm
                                        // above for the rationale).
                                        tracing::debug!(
                                            "[pipeline] forwarding stops early: downstream credit gate closed ({})",
                                            closed.reason
                                        );
                                        return Ok(());
                                    }
                                    Err(_elapsed) => {
                                        tracing::warn!(
                                            cap_urn = %prev_cap_urn,
                                            stream_id = %prev_sid,
                                            downstream_available = gate.available(),
                                            gate_closed = gate.is_closed(),
                                            "[pipeline] forwarder blocked on downstream credit for {}s — \
                                             the consumer is not consuming (or its grants are not arriving); \
                                             continuing to wait",
                                            activity_timeout_secs.max(1)
                                        );
                                    }
                                }
                            }
                        }
                        let (new_sid, _, consumed) =
                            stream_id_map.get_mut(&prev_sid).ok_or_else(|| {
                                ExecutionError::HostError(format!(
                                    "forward CHUNK: unknown stream_id '{}'",
                                    prev_sid
                                ))
                            })?;

                        let mut new_frame = frame.clone();
                        new_frame.id = next_rid.clone();
                        new_frame.routing_id = None;
                        new_frame.seq = 0;
                        new_frame.stream_id = Some(new_sid.clone());
                        switch.send_to_master(new_frame, None).await.map_err(|e| {
                            ExecutionError::HostError(format!("forward CHUNK: {}", e))
                        })?;

                        *consumed += 1;
                        if *consumed >= grant_batch {
                            let n = *consumed;
                            *consumed = 0;
                            send_upstream_credit(
                                &switch,
                                prev_rid.clone(),
                                prev_sid.clone(),
                                n,
                                "forward upstream CREDIT",
                            )
                            .await?;
                        }
                    }
                    FrameType::StreamEnd => {
                        timer.touch();
                        let prev_sid = frame.stream_id.as_deref().unwrap_or("");
                        let (new_sid, _, _) = stream_id_map
                            .get(prev_sid)
                            .ok_or_else(|| {
                                ExecutionError::HostError(format!(
                                    "forward STREAM_END: unknown stream_id '{}'",
                                    prev_sid
                                ))
                            })?
                            .clone();

                        let mut new_frame = frame.clone();
                        new_frame.id = next_rid.clone();
                        new_frame.routing_id = None;
                        new_frame.seq = 0;
                        new_frame.stream_id = Some(new_sid);
                        switch.send_to_master(new_frame, None).await.map_err(|e| {
                            ExecutionError::HostError(format!("forward STREAM_END: {}", e))
                        })?;
                    }
                    FrameType::Credit => {
                        router_up.grant(&frame);
                    }
                    FrameType::End => {
                        if frame.exit_code() != Some(0) {
                            let details = format!(
                                "Cap '{}' END without success: exit_code={:?}",
                                prev_cap_urn,
                                frame.exit_code()
                            );
                            if let Some(lfn) = &log_fn {
                                let mut record = PipelineLogRecord::attributed(
                                    prev_step_token_id,
                                    prev_cap_urn,
                                    "error",
                                    crate::failure::AttributionClass::Internal,
                                    &details,
                                );
                                record.body_index = body_index;
                                lfn(record);
                            }
                            return Err(ExecutionError::CartridgeExecutionFailed {
                                cap_urn: prev_cap_urn.to_string(),
                                code: None,
                                class: crate::failure::AttributionClass::Internal,
                                details,
                                arg_urn: None,
                            });
                        }
                        let final_progress = frame.final_progress().unwrap_or(1.0) as f32;
                        if let Some(pfn) = &progress_fn {
                            pfn(
                                final_progress,
                                prev_cap_urn,
                                frame.final_message().unwrap_or(""),
                            );
                        }
                        for (media_urn, data) in extra_args {
                            super::stream_io::send_one_stream(
                                switch,
                                &next_rid,
                                media_urn,
                                data,
                                None,
                                false,
                                max_chunk,
                                Some((&router_down, initial_credit)),
                            )
                            .await
                            .map_err(|e| ExecutionError::HostError(e.to_string()))?;
                        }
                        let end_frame = Frame::end(next_rid.clone(), None);
                        switch.send_to_master(end_frame, None).await.map_err(|e| {
                            ExecutionError::HostError(format!("forward END: {}", e))
                        })?;
                        return Ok(());
                    }
                    FrameType::Log => {
                        let level = frame.log_level().ok_or_else(|| {
                            ExecutionError::HostError(format!(
                                "Cap '{}' emitted a LOG frame without required text level",
                                prev_cap_urn
                            ))
                        })?;
                        timer.handle_log_level(level);

                        if let Some(p) = frame.log_progress() {
                            let msg = frame.log_message().ok_or_else(|| {
                                ExecutionError::HostError(format!(
                                    "Cap '{}' emitted a progress LOG without required text message",
                                    prev_cap_urn
                                ))
                            })?;
                            if let Some(pfn) = &progress_fn {
                                pfn(p, prev_cap_urn, msg);
                            }
                        } else {
                            let msg = frame.log_message().ok_or_else(|| {
                                ExecutionError::HostError(format!(
                                    "Cap '{}' emitted a LOG frame without required text message",
                                    prev_cap_urn
                                ))
                            })?;
                            let class = frame.attribution_class().map_err(|error| {
                                ExecutionError::HostError(format!(
                                    "Cap '{}' emitted an invalid LOG frame: {}",
                                    prev_cap_urn, error
                                ))
                            })?;
                            let arg_urn = frame
                                .attribution_arg_urn()
                                .map_err(|error| {
                                    ExecutionError::HostError(format!(
                                        "Cap '{}' emitted an invalid LOG frame: {}",
                                        prev_cap_urn, error
                                    ))
                                })?
                                .map(str::to_string);
                            if let Some(lfn) = &log_fn {
                                let mut record = PipelineLogRecord::attributed(
                                    prev_step_token_id,
                                    prev_cap_urn,
                                    level,
                                    class,
                                    msg,
                                );
                                record.meta = frame.meta.clone();
                                record.body_index = body_index;
                                record.arg_urn = arg_urn;
                                lfn(record);
                            }
                        }
                    }
                    FrameType::Err => {
                        let class = frame.attribution_class().map_err(|error| {
                            ExecutionError::HostError(format!(
                                "Cap '{}' emitted an invalid ERR frame: {}",
                                prev_cap_urn, error
                            ))
                        })?;
                        let code = frame.error_code().ok_or_else(|| {
                            ExecutionError::HostError(format!(
                                "Cap '{}' emitted an ERR frame without required text code",
                                prev_cap_urn
                            ))
                        })?;
                        let msg = frame
                            .error_message()
                            .ok_or_else(|| {
                                ExecutionError::HostError(format!(
                                    "Cap '{}' emitted an ERR frame without required text message",
                                    prev_cap_urn
                                ))
                            })?
                            .to_string();
                        let arg_urn = frame
                            .attribution_arg_urn()
                            .map_err(|error| {
                                ExecutionError::HostError(format!(
                                    "Cap '{}' emitted an invalid ERR frame: {}",
                                    prev_cap_urn, error
                                ))
                            })?
                            .map(str::to_string);
                        if let Some(lfn) = &log_fn {
                            let mut record = PipelineLogRecord::attributed(
                                prev_step_token_id,
                                prev_cap_urn,
                                "error",
                                class,
                                &msg,
                            );
                            record.body_index = body_index;
                            record.arg_urn = arg_urn.clone();
                            lfn(record);
                        }
                        return Err(ExecutionError::CartridgeExecutionFailed {
                            cap_urn: prev_cap_urn.to_string(),
                            code: Some(code.to_string()),
                            class,
                            details: msg,
                            arg_urn,
                        });
                    }
                    _ => {}
                }
            }
            Ok(None) => {
                let msg = format!("Cap '{}' response channel closed without END", prev_cap_urn);
                if let Some(lfn) = &log_fn {
                    let mut record = PipelineLogRecord::attributed(
                        prev_step_token_id,
                        prev_cap_urn,
                        "error",
                        crate::failure::AttributionClass::Internal,
                        &msg,
                    );
                    record.body_index = body_index;
                    lfn(record);
                }
                return Err(ExecutionError::HostError(msg));
            }
            Err(_timeout) => {
                for (prev_sid, (_, _, consumed)) in stream_id_map.iter_mut() {
                    if *consumed > 0 {
                        let n = *consumed;
                        *consumed = 0;
                        send_upstream_credit(
                            &switch,
                            prev_rid.clone(),
                            prev_sid.clone(),
                            n,
                            "flush upstream CREDIT",
                        )
                        .await?;
                    }
                }
                if timer.is_expired() && !activity_warning_logged {
                    // Credit-state dump (L8): a silent pipelined stall is a
                    // starved credit edge somewhere in the per-hop loop; the
                    // per-stream gate balance and pending upstream grants
                    // name the starved edge directly. `available` is the
                    // downstream window this forwarder can still send into;
                    // `pending_upstream_grants` are consumed-but-ungranted
                    // credits the producer is owed (should be 0 after the
                    // flush above).
                    let credit_state: Vec<String> = stream_id_map
                        .iter()
                        .map(|(prev_sid, (new_sid, gate, consumed))| {
                            format!(
                                "stream {}→{}: downstream_available={} pending_upstream_grants={} gate_closed={}",
                                prev_sid,
                                new_sid,
                                gate.available(),
                                consumed,
                                gate.is_closed(),
                            )
                        })
                        .collect();
                    let msg = format!(
                        "Cap '{}' has had no activity for {}s — continuing to wait. Use Cancel to abort. \
                         Forwarding credit state: [{}]",
                        prev_cap_urn,
                        activity_timeout_secs,
                        credit_state.join("; "),
                    );
                    if let Some(lfn) = &log_fn {
                        let mut record = PipelineLogRecord::attributed(
                            prev_step_token_id,
                            prev_cap_urn,
                            "warn",
                            crate::failure::AttributionClass::Internal,
                            &msg,
                        );
                        record.body_index = body_index;
                        lfn(record);
                    }
                    tracing::warn!(
                        cap_urn = %prev_cap_urn,
                        credit_state = ?credit_state,
                        "[cap] No activity for {}s; continuing to wait for completion or cancel",
                        activity_timeout_secs
                    );
                    activity_warning_logged = true;
                }
            }
        }
    }
}

/// Execute a resolved DAG: discover cartridges, set up infrastructure, run edge groups.
/// Execute a resolved DAG end-to-end.
///
/// `initial_is_sequence` is the per-node sequence-flag map that
/// mirrors machfab's interpreter contract (see
/// `machfab::cap::capdag_service::execute_dag` and
/// `machfab::ops_rs::cap_interpreter::interpreter::resolve_inputs`).
/// For every node in `initial_inputs` there MUST be a matching
/// entry here declaring whether the bytes are a CBOR sequence
/// (`true` — multiple self-delimiting items, dispatched as
/// separate chunks) or a scalar blob (`false` — one chunk,
/// wrapped in `Value::Bytes`).
///
/// Missing or extra entries are not papered over — they're a
/// programmer error and we fail hard so the call site is fixed
/// at the source. A silent default would let a sequence input
/// flow into a scalar-shaped chunk on the wire (or vice-versa)
/// and produce confusing downstream parse errors hours later
/// inside the receiving cap.
///
/// The flag flows through to `send_one_stream`
/// (orchestrator/stream_io.rs) which branches on it: sequence →
/// split self-delimiting CBOR values into per-chunk frames;
/// scalar → wrap raw bytes in `Value::Bytes` and chunk by
/// `max_chunk`.
///
/// `log_fn` is mandatory. Cartridges emit operational log and
/// progress frames during long-running work, and dropping them at
/// the DAG boundary makes failures opaque.
/// The per-item activity timeout for a segment, read from the segment's terminal cap
/// metadata (`activity_timeout_secs`), falling to the documented default only when the
/// key is absent or malformed. Shared by [`execute_dag`] and the CLI runtime so both
/// resolve the timeout identically.
pub(crate) fn segment_activity_timeout(graph: &ResolvedGraph) -> u64 {
    graph
        .edges
        .first()
        .and_then(|e| e.cap.metadata.get(ACTIVITY_TIMEOUT_METADATA_KEY))
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(DEFAULT_ACTIVITY_TIMEOUT_SECS)
}

/// One hostable cartridge: its entry-point binary, optional installed identity
/// `(id, version, channel)` (absent for dev binaries), and the cap groups it serves.
pub(crate) type HostableCartridge = (
    PathBuf,
    Option<(
        String,
        String,
        crate::bifaci::cartridge_repo::CartridgeChannel,
    )>,
    Vec<crate::bifaci::manifest::CapGroup>,
);

/// Discover the host's BUNDLED cartridges (shipped beside the executor, e.g. the capdag
/// CLI's own `bundled-cartridges/` tree) as hostable cartridges. Uses the shared
/// `discover_cartridges`, so they pass the same identity + bundled-hash integrity checks
/// the engine applies. Each `Incompatible` entry is logged and skipped (discovery
/// already surfaced the reason). An absent directory yields an empty list. Shared by
/// [`execute_dag`] and the CLI runtime.
pub(crate) async fn discover_bundled_cartridges(
    bundled_cartridges_dir: &std::path::Path,
    channel: crate::bifaci::cartridge_repo::CartridgeChannel,
    registry_url: Option<&str>,
    fabric_manifest_version: u32,
) -> Result<Vec<HostableCartridge>, ExecutionError> {
    let identity = crate::cartridge_discovery::DiscoveryIdentity {
        channel,
        registry_url: registry_url.map(str::to_string),
        fabric_manifest_version,
        cartridge_registry_version: crate::CARTRIDGE_REGISTRY_VERSION,
    };
    let mut out = Vec::new();
    for discovered in
        crate::cartridge_discovery::discover_cartridges(bundled_cartridges_dir, &identity)
            .await
            .map_err(|e| {
                ExecutionError::HostError(format!("bundled cartridge discovery failed: {e}"))
            })?
    {
        match discovered {
            crate::cartridge_discovery::DiscoveredCartridge::Directory {
                entry_point,
                id,
                channel: cart_channel,
                version,
                cap_groups,
                ..
            } => {
                out.push((entry_point, Some((id, version, cart_channel)), cap_groups));
            }
            crate::cartridge_discovery::DiscoveredCartridge::Incompatible {
                id,
                version,
                error,
                ..
            } => {
                tracing::error!(
                    cartridge = %id, version = %version, reason = %error.message,
                    "bundled cartridge rejected at discovery — not hosted"
                );
            }
        }
    }
    Ok(out)
}

pub async fn execute_dag(
    graph: &ResolvedGraph,
    cartridge_dir: PathBuf,
    registry_url: Option<String>,
    channel: crate::bifaci::cartridge_repo::CartridgeChannel,
    fabric_manifest_version: u32,
    initial_inputs: HashMap<String, NodeData>,
    initial_is_sequence: HashMap<String, bool>,
    dev_binaries: Vec<PathBuf>,
    bundled_cartridges_dir: Option<PathBuf>,
    fabric_registry: Arc<FabricRegistry>,
    progress_fn: Option<&CapProgressFn>,
    log_fn: &PipelineLogFn,
    // Extra argument streams per node: node id → [(arg media URN, raw bytes)]. These
    // are the already-resolved arg-stream bytes (the single format shared with the
    // engine's `run_segment` and `execute_plan`) — not JSON, no serialization here.
    cap_arguments: &HashMap<String, Vec<(String, Vec<u8>)>>,
    // Optional per-segment protocol trace. When present, the switch is sampled
    // LIVE during the segment (a 250ms task) AND snapshotted once at teardown, so
    // both a normal segment's transitions and a HANGING segment's stall point are
    // captured. Owned `Arc` so the sampler task can hold its own reference. `None`
    // on every path that does not trace.
    trace_sink: Option<Arc<crate::bifaci::protocol_trace::ProtocolTraceSink>>,
) -> Result<DagOutput, ExecutionError> {
    // 1. Initialize cartridge manager and discover/download all needed cartridges
    let mut cartridge_manager = CartridgeManager::new(
        cartridge_dir,
        registry_url.clone(),
        channel,
        fabric_manifest_version,
        dev_binaries,
        crate::bifaci::release_cert::RegistryTrust::from_build_constants(),
        crate::fabric::registry::RegistryConfig::default().registry_base_url,
    );
    cartridge_manager.init().await?;

    let cap_urns: Vec<&str> = graph.edges.iter().map(|e| e.cap_urn.as_str()).collect();
    let mut cartridges = cartridge_manager.resolve_cartridges(&cap_urns).await?;

    // 1b. Register the host's BUNDLED cartridges (shipped beside the executor) alongside
    // the dev/registry cartridges. Shared discovery + integrity checks live in
    // `discover_bundled_cartridges`. Absent dir ⇒ no bundled cartridges.
    if let Some(bundled_cartridges_dir) = bundled_cartridges_dir {
        cartridges.extend(
            discover_bundled_cartridges(
                &bundled_cartridges_dir,
                channel,
                registry_url.as_deref(),
                fabric_manifest_version,
            )
            .await?,
        );
    }

    // 2. Create execution context and add cartridge host as master
    let mut ctx = ExecutionContext::new(fabric_registry).await?;
    ctx.add_cartridge_host(cartridges).await?;

    // 3. Resolve initial inputs to raw bytes and set on nodes.
    //    Enforce strict 1:1 correspondence between
    //    `initial_inputs` and `initial_is_sequence`: every input
    //    node has an explicit sequence flag, every flag entry
    //    refers to an input node. Missing or extra entries are a
    //    programmer error, not a silent default.
    let inputs_keys: HashSet<&str> = initial_inputs.keys().map(|s| s.as_str()).collect();
    let flags_keys: HashSet<&str> = initial_is_sequence.keys().map(|s| s.as_str()).collect();
    let missing_flags: Vec<&str> = inputs_keys.difference(&flags_keys).copied().collect();
    if !missing_flags.is_empty() {
        return Err(ExecutionError::HostError(format!(
            "execute_dag: initial_is_sequence is missing entries for input \
             node(s) {:?}. Every entry in `initial_inputs` requires an \
             explicit sequence/scalar flag — see machfab's `resolve_inputs` \
             for the canonical population pattern.",
            missing_flags,
        )));
    }
    let extra_flags: Vec<&str> = flags_keys.difference(&inputs_keys).copied().collect();
    if !extra_flags.is_empty() {
        return Err(ExecutionError::HostError(format!(
            "execute_dag: initial_is_sequence has flag(s) for node(s) \
             {:?} that are not present in `initial_inputs`. Either drop \
             the stale flags or supply the matching input data.",
            extra_flags,
        )));
    }
    for (node, data) in initial_inputs {
        // unwrap is sound: presence checked exhaustively above.
        let is_seq = *initial_is_sequence
            .get(&node)
            .expect("initial_is_sequence key set verified above");
        ctx.set_node_is_sequence(node.clone(), is_seq);
        let bytes = data.into_bytes().await?;
        ctx.set_node_data(node, bytes);
    }
    // Run the single shared segment executor. The reference path has no config
    // service, so the activity timeout comes from the terminal cap's metadata
    // (the same key the engine reads), defaulting when absent. No disk writer and
    // no flow observer — those are engine concerns the reference path does not carry.
    let activity_timeout_secs = segment_activity_timeout(graph);

    // Protocol trace label: the segment's terminal cap URN (its output anchor).
    // Computed once, shared by the live sampler and the final snapshot.
    let trace_label = trace_sink.as_ref().map(|_| {
        graph
            .edges
            .last()
            .map(|e| e.cap_urn.clone())
            .unwrap_or_else(|| "empty-graph".to_string())
    });

    // Live protocol sampler: while the segment runs, sample the switch's L8
    // snapshot every 250ms and append it (deduped) so a segment that HANGS still
    // leaves a line at the stall point — the last line before the harness kills
    // it shows the stalled active request with its per-stream credit/flow
    // counters. A live sample's write failure is logged and swallowed: a mid-run
    // trace hiccup must never abort execution (only the final snapshot is
    // fail-hard). Aborted the moment the segment returns.
    let trace_sampler = match (&trace_sink, &trace_label) {
        (Some(sink), Some(label)) => {
            let switch = ctx.switch().clone();
            let sink = sink.clone();
            let label = label.clone();
            Some(tokio::spawn(async move {
                let mut ticker = tokio::time::interval(std::time::Duration::from_millis(250));
                loop {
                    ticker.tick().await;
                    let stats = switch.protocol_stats().await;
                    if let Err(e) = sink.record_deduped(&stats, &label).await {
                        tracing::debug!(
                            error = %e, segment = %label,
                            "protocol trace live sample failed (continuing)"
                        );
                    }
                }
            }))
        }
        _ => None,
    };

    // Reference/in-memory path: no writer factory (nothing persists to disk) and no
    // persisted sinks, so every terminal is returned as in-memory items. No flow
    // observer — that is an engine concern.
    let out = run_dag_on_context(
        &mut ctx,
        graph,
        cap_arguments,
        progress_fn,
        None,
        Some(log_fn),
        None,
        None,
        None,
        &HashSet::new(),
        activity_timeout_secs,
        None,
        None,
        None,
    )
    .await;

    // The segment is done — stop the live sampler before the final snapshot so
    // they cannot race on the same file. Awaiting the aborted handle yields a
    // cancellation `JoinError`, which is expected and ignored.
    if let Some(handle) = trace_sampler {
        handle.abort();
        let _ = handle.await;
    }

    // Final end-of-segment snapshot: capture the switch's L8 snapshot once more
    // before teardown, on BOTH the success and failure paths, so the terminal
    // state is on disk even if the last transition happened between live samples.
    // Routed through `record_deduped` so it does not duplicate the last sample.
    // Unlike the live path, this one is fail-hard: the user asked for a trace, so
    // a final line that cannot be written is surfaced.
    if let (Some(sink), Some(label)) = (&trace_sink, &trace_label) {
        let stats = ctx.switch().protocol_stats().await;
        if let Err(e) = sink.record_deduped(&stats, label).await {
            match &out {
                // Success path: the user asked for a trace and it could not be
                // written — surface it hard rather than silently dropping the line.
                Ok(_) => {
                    let _ = ctx.shutdown();
                    return Err(ExecutionError::HostError(format!(
                        "protocol trace write failed for segment '{label}': {e}"
                    )));
                }
                // Failure path: the segment already failed loudly; that error is
                // the primary signal and stays the return value, but the trace
                // failure is not swallowed — it is logged.
                Err(_) => {
                    tracing::error!(
                        error = %e, segment = %label,
                        "protocol trace write failed on the segment error path"
                    );
                }
            }
        }
    }

    // Tear down the per-run cartridge hosts regardless of outcome.
    let _ = ctx.shutdown();
    out
}

#[cfg(test)]
mod tests {
    // TEST1448: chains split ONLY at the permit-deadlock boundary — two
    // adjacent groups whose admission chains SHARE a bounded pool (the
    // pool behind the cartridge behind the master, never the master
    // index: one relay slot can aggregate many cartridges). Bounded caps
    // sharing no pool keep streaming in one live chain (the property
    // unbounded live pipelines depend on: only chain sinks are collected,
    // and unbounded intermediates cannot be materialised, L16).
    #[test]
    fn test1448_chain_split_only_at_shared_bounded_pool_adjacency() {
        // encode(audiocartridge) → transcribe(candle) → generate(candle),
        // both candle caps in the shared bounded "gpu" pool: the electron
        // shape — ONE aggregated master slot, THREE caps, TWO cartridges.
        // The only boundary is between the two candle caps;
        // encode|transcribe streams.
        let chains = vec![vec![0, 1, 2]];
        let group_chain = vec![
            vec![("audio/encode", 0), ("audio/all", 0)],
            vec![("candle/transcribe", 0), ("candle/gpu", 1), ("candle/all", 0)],
            vec![("candle/generate", 0), ("candle/gpu", 1), ("candle/all", 0)],
        ];
        let split = super::split_chains_at_shared_bounded_pool(chains, &group_chain);
        assert_eq!(
            split,
            vec![vec![0, 1], vec![2]],
            "split exactly at the shared bounded-pool junction"
        );

        // Different cartridges everywhere — ONE live chain, no
        // materialisation, even with a bounded singleton in the middle.
        let chains = vec![vec![0, 1, 2]];
        let group_chain = vec![
            vec![("a/x", 0), ("a/all", 0)],
            vec![("b/y", 3), ("b/all", 0)],
            vec![("c/z", 0), ("c/all", 0)],
        ];
        let split = super::split_chains_at_shared_bounded_pool(chains, &group_chain);
        assert_eq!(split, vec![vec![0, 1, 2]], "different cartridges never split");

        // Same cartridge but every shared pool UNBOUNDED: no permit to
        // contend — stays one chain.
        let chains = vec![vec![0, 1]];
        let group_chain = vec![
            vec![("d/x", 0), ("d/all", 0)],
            vec![("d/y", 0), ("d/all", 0)],
        ];
        let split = super::split_chains_at_shared_bounded_pool(chains, &group_chain);
        assert_eq!(split, vec![vec![0, 1]], "unbounded same-cartridge streams");

        // Same cartridge, both caps bounded, but in DISJOINT bounded pools
        // (cpu vs gpu) with `all` unbounded: no shared permit — the pool
        // refinement over the old whole-cartridge domain. Streams.
        let chains = vec![vec![0, 1]];
        let group_chain = vec![
            vec![("e/x", 1), ("e/cpu", 2), ("e/all", 0)],
            vec![("e/y", 1), ("e/gpu", 1), ("e/all", 0)],
        ];
        let split = super::split_chains_at_shared_bounded_pool(chains, &group_chain);
        assert_eq!(
            split,
            vec![vec![0, 1]],
            "disjoint bounded pools on one cartridge stream"
        );

        // Same cap twice in a row: the bounded SINGLETON is shared — split.
        let chains = vec![vec![0, 1]];
        let group_chain = vec![
            vec![("f/x", 1), ("f/all", 0)],
            vec![("f/x", 1), ("f/all", 0)],
        ];
        let split = super::split_chains_at_shared_bounded_pool(chains, &group_chain);
        assert_eq!(split, vec![vec![0], vec![1]], "shared bounded singleton splits");
    }

    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Minimal ResolvedEdge for segment-detection tests.
    fn edge(from: &str, to: &str, urn: &str) -> ResolvedEdge {
        let cap_urn = CapUrn::from_string(urn).expect("test cap URN");
        let cap = Cap::new(cap_urn, "t".to_string(), vec!["t".to_string()]);
        ResolvedEdge {
            token_id: format!("{}->{}", from, to).parse().unwrap(),
            from: from.to_string(),
            to: to.to_string(),
            cap_urn: urn.to_string(),
            cap,
            in_media: "media:".to_string(),
            out_media: "media:".to_string(),
        }
    }

    // TEST1433: a multi-edge group (fan-in / gather) ALWAYS heads its own
    // chain — mid-chain streaming forwards exactly one producer stream, so a
    // multi-input invocation must be fed from materialised node_data. A
    // single-edge group with one dedicated producer still chains linearly.
    #[test]
    fn test1433_multi_edge_group_heads_a_chain() {
        // Gather shape: in→A, in2→B, then (A, B) → C on one cap.
        let cap_c = "cap:in=\"media:enc=utf-8\";fold;out=\"media:enc=utf-8;ext=txt\"";
        let edges = vec![
            edge(
                "in",
                "A",
                "cap:in=\"media:ext=pdf\";op-a;out=\"media:enc=utf-8\"",
            ),
            edge(
                "in2",
                "B",
                "cap:in=\"media:ext=md\";op-b;out=\"media:enc=utf-8\"",
            ),
            edge("A", "C", cap_c),
            edge("B", "C", cap_c),
        ];
        let groups = build_edge_groups(&edges);
        assert_eq!(groups.len(), 3, "A, B, and the two-edge C group");
        let order = topological_sort_groups(&groups).expect("acyclic");
        let chains = decompose_group_chains(&groups, &order);
        assert_eq!(
            chains.len(),
            3,
            "the gather group must head its own chain, never continue A's or B's"
        );
        // Every chain containing the C group contains ONLY the C group.
        let c_idx = groups.iter().position(|g| g.to == "C").expect("C group");
        let c_chain = chains
            .iter()
            .find(|ch| ch.contains(&c_idx))
            .expect("C chained");
        assert_eq!(c_chain, &vec![c_idx]);

        // Control: a plain linear chain still fuses.
        let linear = vec![
            edge(
                "in",
                "A",
                "cap:in=\"media:ext=pdf\";op-a;out=\"media:enc=utf-8\"",
            ),
            edge(
                "A",
                "D",
                "cap:in=\"media:enc=utf-8\";op-d;out=\"media:enc=utf-8;ext=txt\"",
            ),
        ];
        let groups = build_edge_groups(&linear);
        let order = topological_sort_groups(&groups).expect("acyclic");
        let chains = decompose_group_chains(&groups, &order);
        assert_eq!(
            chains.len(),
            1,
            "a dedicated single-edge consumer fuses into one chain"
        );
        assert_eq!(chains[0].len(), 2);
    }

    // TEST1125: map_progress clamps child to [0.0, 1.0] and maps to [base, base+weight]
    #[test]
    fn test1125_map_progress_basic_mapping() {
        // Identity mapping: base=0, weight=1
        assert_eq!(map_progress(0.0, 0.0, 1.0), 0.0);
        assert_eq!(map_progress(0.5, 0.0, 1.0), 0.5);
        assert_eq!(map_progress(1.0, 0.0, 1.0), 1.0);

        // Subdivision: base=0.2, weight=0.6 → range [0.2, 0.8]
        assert_eq!(map_progress(0.0, 0.2, 0.6), 0.2);
        assert_eq!(map_progress(0.5, 0.2, 0.6), 0.5);
        assert_eq!(map_progress(1.0, 0.2, 0.6), 0.8);

        // Clamping: values outside [0, 1] are clamped before mapping
        assert_eq!(map_progress(-0.5, 0.2, 0.6), 0.2); // clamp to 0 → base
        assert_eq!(map_progress(1.5, 0.2, 0.6), 0.8); // clamp to 1 → base+weight
    }

    // TEST1126: map_progress is deterministic — same inputs always produce same output
    #[test]
    fn test1126_map_progress_deterministic() {
        for i in 0..100 {
            let p = i as f32 / 100.0;
            let a = map_progress(p, 0.1, 0.8);
            let b = map_progress(p, 0.1, 0.8);
            assert_eq!(a, b, "map_progress must be deterministic for p={}", p);
        }
    }

    // TEST910: map_progress output is monotonic for monotonically increasing input
    #[test]
    fn test910_map_progress_monotonic() {
        let mut prev = map_progress(0.0, 0.1, 0.7);
        for i in 1..=100 {
            let p = i as f32 / 100.0;
            let curr = map_progress(p, 0.1, 0.7);
            assert!(
                curr >= prev,
                "map_progress must be monotonic: p={}, prev={}, curr={}",
                p,
                prev,
                curr
            );
            prev = curr;
        }
    }

    // TEST911: map_progress output is bounded within [base, base+weight]
    #[test]
    fn test911_map_progress_bounded() {
        let base = 0.15;
        let weight = 0.55;
        for i in -10..=110 {
            let p = i as f32 / 100.0;
            let result = map_progress(p, base, weight);
            assert!(
                result >= base && result <= base + weight,
                "map_progress({}, {}, {}) = {} must be in [{}, {}]",
                p,
                base,
                weight,
                result,
                base,
                base + weight
            );
        }
    }

    // TEST912: ProgressMapper correctly maps through a CapProgressFn
    #[test]
    fn test912_progress_mapper_reports_through_parent() {
        let reported = Arc::new(std::sync::Mutex::new(Vec::new()));
        let reported_clone = Arc::clone(&reported);
        let parent: CapProgressFn = Arc::new(move |p: f32, _cap: &str, msg: &str| {
            reported_clone.lock().unwrap().push((p, msg.to_string()));
        });

        let mapper = ProgressMapper::new(&parent, 0.2, 0.6);
        mapper.report(0.0, "", "start");
        mapper.report(0.5, "", "half");
        mapper.report(1.0, "", "done");

        let reports = reported.lock().unwrap();
        assert_eq!(reports.len(), 3);
        assert!((reports[0].0 - 0.2).abs() < 0.001, "0% maps to base=0.2");
        assert!((reports[1].0 - 0.5).abs() < 0.001, "50% maps to 0.5");
        assert!(
            (reports[2].0 - 0.8).abs() < 0.001,
            "100% maps to base+weight=0.8"
        );
    }

    // TEST913: ProgressMapper.as_cap_progress_fn produces same mapping
    #[test]
    fn test913_progress_mapper_as_cap_progress_fn() {
        let reported = Arc::new(std::sync::Mutex::new(Vec::new()));
        let reported_clone = Arc::clone(&reported);
        let parent: CapProgressFn = Arc::new(move |p: f32, _cap: &str, _msg: &str| {
            reported_clone.lock().unwrap().push(p);
        });

        let mapper = ProgressMapper::new(&parent, 0.1, 0.3);
        let pfn = mapper.as_cap_progress_fn();

        pfn(0.0, "", "a");
        pfn(0.5, "", "b");
        pfn(1.0, "", "c");

        let reports = reported.lock().unwrap();
        assert_eq!(reports.len(), 3);
        assert!((reports[0] - 0.1).abs() < 0.001);
        assert!((reports[1] - 0.25).abs() < 0.001);
        assert!((reports[2] - 0.4).abs() < 0.001);
    }

    // TEST914: ProgressMapper.sub_mapper chains correctly
    #[test]
    fn test914_progress_mapper_sub_mapper() {
        let reported = Arc::new(std::sync::Mutex::new(Vec::new()));
        let reported_clone = Arc::clone(&reported);
        let parent: CapProgressFn = Arc::new(move |p: f32, _cap: &str, _msg: &str| {
            reported_clone.lock().unwrap().push(p);
        });

        // Parent maps [0, 1] to [0.2, 0.8] (base=0.2, weight=0.6)
        let mapper = ProgressMapper::new(&parent, 0.2, 0.6);

        // Sub-mapper maps [0, 1] to the second half of parent's range
        // sub_base=0.5, sub_weight=0.5 → [0.2 + 0.5*0.6, 0.2 + (0.5+0.5)*0.6] = [0.5, 0.8]
        let sub = mapper.sub_mapper(0.5, 0.5);
        sub.report(0.0, "", "sub_start");
        sub.report(1.0, "", "sub_end");

        let reports = reported.lock().unwrap();
        assert_eq!(reports.len(), 2);
        assert!((reports[0] - 0.5).abs() < 0.001, "sub 0% maps to 0.5");
        assert!((reports[1] - 0.8).abs() < 0.001, "sub 100% maps to 0.8");
    }

    // TEST914b: ProgressMapper.with_step_sink emits the RAW child (the cap's own
    // fraction) to the step sink while the parent still receives the MAPPED overall.
    #[test]
    fn test914b_progress_mapper_step_sink_reports_raw_child() {
        let overall = Arc::new(std::sync::Mutex::new(Vec::new()));
        let overall_clone = Arc::clone(&overall);
        let parent: CapProgressFn = Arc::new(move |p: f32, _cap: &str, _msg: &str| {
            overall_clone.lock().unwrap().push(p);
        });
        let steps = Arc::new(std::sync::Mutex::new(Vec::new()));
        let steps_clone = Arc::clone(&steps);
        let sink: CapStepProgressFn = Arc::new(move |step: f32, cap: &str, token: &str| {
            steps_clone
                .lock()
                .unwrap()
                .push((step, cap.to_string(), token.to_string()));
        });

        // This cap occupies [0.5, 0.75] of the overall run (base=0.5, weight=0.25).
        let mapper = ProgressMapper::new(&parent, 0.5, 0.25).with_step_sink(&sink, &"tok-cap-x".parse().unwrap());
        mapper.report(0.0, "cap:x", "start");
        mapper.report(0.4, "cap:x", "mid");
        mapper.report(1.0, "cap:x", "end");
        // A sub-mapper of a step-sink mapper does NOT re-fire the sink (intra-cap
        // phase) — its child is not the cap's own progress.
        let sub = mapper.sub_mapper(0.0, 0.5);
        sub.report(1.0, "cap:x", "phase");

        let steps = steps.lock().unwrap();
        let overall = overall.lock().unwrap();
        // The sink saw the cap's OWN progress, unmapped, exactly 3 times (not from the sub).
        assert_eq!(
            steps.len(),
            3,
            "only the group mapper fires the sink; sub_mapper does not"
        );
        assert!((steps[0].0 - 0.0).abs() < 0.001);
        assert!(
            (steps[1].0 - 0.4).abs() < 0.001,
            "sink gets the raw child, not the overall"
        );
        assert!((steps[2].0 - 1.0).abs() < 0.001);
        assert_eq!(steps[1].1, "cap:x");
        // Every step report carries the reporting cap's stable identity — the key
        // that disambiguates a repeated cap URN back to its exact strand step.
        assert_eq!(steps[0].2, "tok-cap-x");
        assert_eq!(steps[1].2, "tok-cap-x");
        assert_eq!(steps[2].2, "tok-cap-x");
        // The parent still saw that child mapped into [0.5, 0.75].
        assert!(
            (overall[1] - (0.5 + 0.4 * 0.25)).abs() < 0.001,
            "parent gets the mapped overall"
        );
    }

    // TEST915: Per-group subdivision produces monotonic, bounded progress for N groups
    //
    // Uses pre-computed boundaries (same pattern as production code) to guarantee
    // monotonicity regardless of f32 rounding.
    #[test]
    fn test915_per_group_subdivision_monotonic_bounded() {
        let all_progress = Arc::new(std::sync::Mutex::new(Vec::new()));
        let all_clone = Arc::clone(&all_progress);
        let parent: CapProgressFn = Arc::new(move |p: f32, _cap: &str, _msg: &str| {
            all_clone.lock().unwrap().push(p);
        });

        let n_groups: usize = 5;
        let boundaries: Vec<f32> = (0..=n_groups).map(|i| i as f32 / n_groups as f32).collect();

        for i in 0..n_groups {
            let base = boundaries[i];
            let weight = boundaries[i + 1] - base;
            let mapper = ProgressMapper::new(&parent, base, weight);

            // Each group reports 0%, 50%, 100%
            mapper.report(0.0, "", "start");
            mapper.report(0.5, "", "half");
            mapper.report(1.0, "", "done");
        }

        let progress = all_progress.lock().unwrap();
        assert_eq!(progress.len(), 15); // 5 groups * 3 reports

        // Verify monotonicity
        for i in 1..progress.len() {
            assert!(
                progress[i] >= progress[i - 1],
                "monotonic violation at index {}: {} < {}",
                i,
                progress[i],
                progress[i - 1]
            );
        }

        // Verify bounded [0.0, 1.0]
        for (i, &p) in progress.iter().enumerate() {
            assert!(
                p >= 0.0 && p <= 1.0,
                "Progress[{}]={} must be in [0.0, 1.0]",
                i,
                p
            );
        }

        // First should be 0.0 (group 0, 0%)
        assert!((progress[0] - 0.0).abs() < 0.001);
        // Last should be 1.0 (group 4, 100%)
        assert!((progress[14] - 1.0).abs() < 0.001);
    }

    // TEST916: ForEach item subdivision produces correct, monotonic ranges
    //
    // Mirrors the production code in interpreter.rs: pre-compute item boundaries
    // from the same formula so the end of item N and the start of item N+1 are
    // the same f32 value (no divergent accumulation paths).
    #[test]
    fn test916_foreach_item_subdivision() {
        let all_progress = Arc::new(std::sync::Mutex::new(Vec::new()));
        let all_clone = Arc::clone(&all_progress);
        let parent: CapProgressFn = Arc::new(move |p: f32, _cap: &str, _msg: &str| {
            all_clone.lock().unwrap().push(p);
        });

        // ForEach: prefix [0.0, 0.05), body [0.05, 0.95), suffix [0.95, 1.0)
        let body_base = 0.05_f32;
        let body_weight = 0.90_f32;
        let item_count: usize = 4;

        // Pre-compute boundaries from a single formula — same as production code
        let item_boundaries: Vec<f32> = (0..=item_count)
            .map(|i| body_base + body_weight * (i as f32 / item_count as f32))
            .collect();

        for i in 0..item_count {
            let item_base = item_boundaries[i];
            let item_weight = item_boundaries[i + 1] - item_base;
            let mapper = ProgressMapper::new(&parent, item_base, item_weight);

            // Each item reports 0% and 100%
            mapper.report(0.0, "", "item_start");
            mapper.report(1.0, "", "item_done");
        }

        let progress = all_progress.lock().unwrap();
        assert_eq!(progress.len(), 8); // 4 items * 2 reports

        // Item 0 start: body_base = 0.05
        assert!(
            (progress[0] - 0.05).abs() < 0.01,
            "item 0 start: got {}",
            progress[0]
        );
        // Item 0 end: boundary[1] = 0.05 + 0.90 * 0.25 = 0.275
        assert!(
            (progress[1] - 0.275).abs() < 0.01,
            "item 0 end: got {}",
            progress[1]
        );
        // Item 3 end: boundary[4] = 0.05 + 0.90 * 1.0 = 0.95
        assert!(
            (progress[7] - 0.95).abs() < 0.01,
            "item 3 end: got {}",
            progress[7]
        );

        // All monotonic — this is the core invariant
        for i in 1..progress.len() {
            assert!(
                progress[i] >= progress[i - 1],
                "monotonic violation at index {}: {} < {}",
                i,
                progress[i],
                progress[i - 1]
            );
        }
    }

    // TEST917: High-frequency progress emission does not violate bounds
    // (Regression test for the deadlock scenario — verifies computation stays bounded)
    #[test]
    fn test917_high_frequency_progress_bounded() {
        let count = Arc::new(AtomicU32::new(0));
        let max_val = Arc::new(std::sync::Mutex::new(f32::MIN));
        let min_val = Arc::new(std::sync::Mutex::new(f32::MAX));

        let count_clone = Arc::clone(&count);
        let max_clone = Arc::clone(&max_val);
        let min_clone = Arc::clone(&min_val);
        let parent: CapProgressFn = Arc::new(move |p: f32, _cap: &str, _msg: &str| {
            count_clone.fetch_add(1, Ordering::Relaxed);
            let mut max = max_clone.lock().unwrap();
            if p > *max {
                *max = p;
            }
            let mut min = min_clone.lock().unwrap();
            if p < *min {
                *min = p;
            }
        });

        let mapper = ProgressMapper::new(&parent, 0.1, 0.8);

        // Simulate 100,000 rapid progress updates (like model download without throttle)
        for i in 0..100_000 {
            let p = i as f32 / 100_000.0;
            mapper.report(p, "", "downloading");
        }

        assert_eq!(count.load(Ordering::Relaxed), 100_000);
        let min = *min_val.lock().unwrap();
        let max = *max_val.lock().unwrap();
        assert!(min >= 0.1, "min {} must be >= base 0.1", min);
        assert!(max <= 0.9, "max {} must be <= base+weight 0.9", max);
    }

    // ActivityTimeout error variants removed: the runtime no longer
    // aborts a cap on activity-silence; long silences surface as
    // warnings only and the user cancels via the explicit
    // cancel-task path. The TEST918 assertion of the
    // `ExecutionError::ActivityTimeout` Display has been deleted
    // along with the variant — not a tautological removal of a real
    // test, but the deletion of an assertion about a code path that
    // no longer exists.
}
