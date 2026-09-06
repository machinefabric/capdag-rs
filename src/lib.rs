//! Cap SDK — URN system, cap definitions, and the Bifaci protocol
//!
//! This library provides:
//!
//! - **URN system** (`urn`): Cap URNs, media URNs, cap matrix
//! - **Cap definitions** (`cap`): Cap types, validation, registry, caller
//! - **Media types** (`media`): Media def resolution, registry, profile schemas
//! - **Bifaci protocol** (`bifaci`): Binary Frame Cap Invocation — cartridge runtime,
//!   host runtime, relay, relay switch, cartridge repo
//! - **Standard** (`standard`): Standard cap and media URN constants
//!
//! ## Architecture
//!
//! ```text
//! Router:      (RelaySwitch + RelayMaster × N)
//! Host × N:    (RelaySlave + CartridgeHostRuntime)
//! Cartridge × N:  (CartridgeRuntime + handler × N)
//! ```
//!
//! ## Protocol Overview
//!
//! Cartridges communicate via length-prefixed CBOR frames over stdin/stdout:
//!
//! 1. Host sends HELLO, cartridge responds with HELLO (negotiate limits)
//! 2. Host sends REQ frames to invoke caps
//! 3. Cartridge responds with STREAM_START/CHUNK/STREAM_END/END frames
//! 4. Cartridge sends END frame when complete, or ERR on error
//! 5. Cartridge can send LOG frames for progress/status
//! 6. Relay-specific: RelayNotify (slave→master) and RelayState (master→slave)

pub mod bifaci;
pub mod cap;
pub mod capture;
pub mod cartridge_discovery;
pub mod cartridge_registry_version;
pub mod dev;
pub mod fabric;
pub mod fabric_manifest_version;
pub mod input_resolver;
pub mod llm;
pub mod machine;
pub mod media;
pub mod net_retry;
pub mod orchestrator;
pub mod pages;
pub mod planner;
pub mod standard;
pub mod urn;

// The failure taxonomy — declared at every error's definition site, carried
// structurally through the ERR frame to the engine (`../docs/17.2-error-handling.md`). Defined in
// the leaf `ops` crate; re-exported here as the cartridge-contract surface.
pub use ops_rs::failure;
pub use ops_rs::failure::AttributionClass;

// URN types
pub use urn::cap_urn::*;
pub use urn::media_urn::*;

// Cap definitions
pub use cap::caller::{CapArgumentValue, CapResult, StdinSource};
pub use cap::definition::*;
pub use cap::response::*;
pub use cap::schema_validation::{
    FileSchemaResolver, SchemaResolver, SchemaValidationError,
    SchemaValidator as JsonSchemaValidator,
};
pub use cap::validation::*;

// Media types
pub use media::profile::{ProfileSchemaError, ProfileSchemaRegistry};
pub use media::spec::*;

// Unified fabric registry — caps + media defs in one type
pub use fabric::alias::{
    classify_alias_target, is_alias_token, normalize_alias_name, token_is_urn, AliasNameError,
    AliasTargetKind, StoredAlias,
};
pub use fabric::registry::{FabricRegistry, FabricRegistryError, RegistryConfig, StoredMediaDef};

// Build-time-baked fabric manifest version (see capdag/build.rs).
pub use cartridge_registry_version::CARTRIDGE_REGISTRY_VERSION;
pub use fabric_manifest_version::FABRIC_MANIFEST_VERSION;

// Cartridge binary + manifest signing (minisign / ed25519): runtime
// verification of registry-downloaded pure-binary artifacts and signed
// manifests against the baked root public keys.
pub use bifaci::bundle_manifest::{
    manifest_for as bundle_manifest_for, manifest_paths as bundle_manifest_paths, BundleError,
    BundleManifest, BundleProof, BundledCartridge, BUNDLE_MANIFEST_FILE, BUNDLE_MANIFEST_FORMAT,
    BUNDLE_MANIFEST_SIG_FILE,
};
pub use bifaci::binary_signing::{
    parse_minisign_public_key, raw_verify, root_pubkeys_from_build_env,
    signing_environment_from_build_env, split_root_pubkeys, verify_binary_signature,
    ParsedPublicKey, SignatureError,
};
pub use bifaci::registry_verdict::{
    ChainFailureReason, RegistryRemedy, RegistryVerdict, RegistryVerdictError,
    RegistryVerdictState,
};
pub use bifaci::release_cert::{
    unix_now, verify_manifest_envelope, CertificateEntry, ChainError, ManifestSigEnvelope,
    ManifestSignature, RegistryTrust, ReleaseKeyCertificate, RootSignature, VerifiedChain,
    MANIFEST_SIG_FORMAT, RELEASE_KEY_CERT_FORMAT, REQUIRED_ROOT_SIGNATURES,
};

/// The comma-separated base64 minisign ROOT public keys this build trusts
/// (Root A, Root B, …), baked at compile time from
/// `MFR_CARTRIDGE_ROOT_PUBKEYS`. Roots sign release-key certificates only;
/// artifacts and manifests are signed by a certificate-authorized release
/// key. `None` = dev build (registry downloads and manifest verification are
/// disabled). `capdag/build.rs` enforces that a build baking a cartridge
/// registry URL also bakes the root set and the environment label — the
/// triple travels together.
pub const CARTRIDGE_ROOT_PUBKEYS: Option<&'static str> =
    root_pubkeys_from_build_env(option_env!("MFR_CARTRIDGE_ROOT_PUBKEYS"));

/// The signing environment label (`prod` / `staging`) this build is bound
/// to, baked from `MFR_SIGNING_ENVIRONMENT`. Release-key certificates carry
/// the environment they were issued for; a certificate for the other
/// environment is rejected even though the root set is shared.
pub const SIGNING_ENVIRONMENT: Option<&'static str> =
    signing_environment_from_build_env(option_env!("MFR_SIGNING_ENVIRONMENT"));

// Standard caps and media
pub use standard::*;

// Bifaci protocol — frames, I/O, runtimes
pub use bifaci::cartridge_runtime::{
    find_stream, find_stream_conforming, find_stream_meta, find_stream_str,
    find_stream_str_conforming, require_stream, require_stream_str, AdapterSelectionOp,
    CartridgeRuntime, CliStreamEmitter, DiscardOp, FinalStatus, FrameSender,
    IdentityOp, InputPackage, InputStream, NoPeerInvoker, OpFactory, OutputStream, PeerCall,
    PoolHandle,
    PeerInvoker, PeerResponse, PeerResponseItem, ProgressSender, Request, RuntimeError,
    StreamError, StreamMeta, StreamSender, WET_KEY_REQUEST,
};
pub use bifaci::credit::{CreditClosed, CreditGate, CreditRouter};
pub use bifaci::decode_chunk_payload;
pub use bifaci::frame::{
    CancelReason, CreditDirection, DropReason, FlowKey, Frame, FrameType, Limits, MessageId, ReorderBuffer,
    SeqAssigner, DEFAULT_INITIAL_CREDIT, DEFAULT_MAX_CHUNK, DEFAULT_MAX_FRAME,
    DEFAULT_MAX_REORDER_BUFFER, PROTOCOL_VERSION,
};
pub use bifaci::io::{
    decode_frame, encode_frame, handshake, handshake_accept, read_frame, verify_identity,
    write_frame, CborError, FrameReader, FrameWriter, HandshakeResult,
};
pub use bifaci::manifest::*;
// Concurrency pools — the one capacity concept (a cap is a pool of one;
// `all` is the pool of every cap; queues lead to pools).
pub use bifaci::pools::{
    chain_from_states, decode_desired, decode_pool_states, effective_capacity, encode_desired,
    encode_pool_states, DesiredCapacities, PoolDeclarations, PoolState, PoolStates,
    CAPACITY_UNLIMITED, META_DESIRED_CAPACITIES, META_POOLS, POOL_ALL,
};
pub use bifaci::request_state::{
    FrameDirection, PoolKey, RequestPhase, RequestSnapshot, RequestState, RequestTable,
    RequestTableSnapshot, RoutingEntry, StreamFlowStats, StreamSnapshot, TerminalKind,
    TerminatedSummary,
};
pub use bifaci::live_feed::{
    stop_feed_requests, LiveFeedHandle, LiveFeedItem, LiveFeedSelector, LiveFeedSink,
    LiveFeedStop, OpenedFeed, OverrunPolicy, StopInputsError, StopInputsOutcome,
    MEDIA_LIVE_FEED, MEDIA_LIVE_SYNTHETIC, STOP_INPUT_DRAIN_TIMEOUT,
};
pub use capture::MEDIA_FEED_FRAMES;
pub use bifaci::stats::{
    DropCounters, DropSnapshot, StragglerCounters, StragglerSnapshot, TerminatedFlows,
};

// Re-export ops crate types used by Op-based handlers
pub use async_trait::async_trait;
pub use bifaci::cartridge_repo::{
    host_platform, CartridgeBinaryInfo, CartridgeBuild, CartridgeChannel, CartridgeChannelEntries,
    CartridgeCompatibilityResolution, CartridgeDistributionInfo, CartridgeInfo, CartridgeRegistry,
    CartridgeRegistryChannels, CartridgeRegistryEntry, CartridgeRegistryResponse, CartridgeRepo,
    CartridgeRepoError, CartridgeSuggestion, CartridgeVersionData, CompatStatus, RegistryArgSource,
    RegistryCap, RegistryCapArg, RegistryCapGroup, RegistryCapOutput,
};
pub use ops_rs::{DryContext, Op, OpError, OpMetadata, OpResult, WetContext};

// CartridgeHost is the primary API for host-side cartridge communication (async/tokio-native)
pub use bifaci::host_runtime::{
    AsyncHostError as HostError, CartridgeHostRuntime as CartridgeHost, CartridgeResponse,
    ResponseChunk, StreamingResponse,
};

// Also export with explicit Async prefix for clarity when needed
pub use bifaci::host_runtime::AsyncHostError;
pub use bifaci::host_runtime::CartridgeHostRuntime;

// Cartridge process monitoring
pub use bifaci::host_runtime::{
    CartridgeHostObserver, CartridgeProcessHandle, CartridgeProcessInfo, HostCommand,
    RegisteredDirSpec,
};

// Cartridge install metadata
pub use bifaci::cartridge_json::{
    hash_cartridge_directory, validate_registry_url_scheme, CartridgeInstallSource, CartridgeJson,
    CartridgeJsonError, RegistryUrlSchemeResult,
};

// Registry slug — deterministic on-disk folder name for a registry URL.
pub use bifaci::cartridge_slug::{is_registry_slug, slug_for, DEV_SLUG};

// Shared cartridge discovery (engine + unifloom-daemon)
pub use cartridge_discovery::{
    discover_cartridges, probe_cartridge_cap_groups, DiscoveredCartridge, DiscoveryIdentity,
};

// Relay exports
pub use bifaci::host_runtime::HostProtocolStats;
pub use bifaci::in_process_host::{
    accumulate_input, FrameHandler, InProcessCartridgeHost, ResponseWriter,
};
pub use bifaci::protocol_trace::{ProtocolTraceError, ProtocolTraceSink};
pub use bifaci::relay::{RelayMaster, RelaySlave};
pub use bifaci::relay_switch::{
    CartridgeAttachmentError, CartridgeAttachmentErrorKind, CartridgeLifecycle,
    InstalledCartridgeRecord, MasterHealthStatus, RelayNotifyCapabilitiesPayload, RelaySwitch,
    RelaySwitchError, RelaySwitchProtocolStats,
};

// Planner — planning, discovery, and execution for machines
pub use planner::{
    // Argument binding
    ArgumentBinding,
    ArgumentBindings,
    ArgumentInfo,
    ArgumentResolution,
    ArgumentResolutionContext,
    ArgumentSource,
    BodyOutcome,
    CapFileMetadata,
    // Collection input
    CapInputCollection,
    CapInputFile,
    CapShapeInfo,
    CardinalityCompatibility,
    CardinalityPattern,
    CollectionFile,
    EdgeType,
    ExecutionNodeType,
    // Shape (cardinality + structure)
    InputCardinality,
    InputStructure,
    // Live capfab (unified path finding)
    LiveCapFab,
    LiveMachinePlanEdge,
    MachineNode,
    // Execution plan
    MachinePlan,
    // Plan builder
    MachinePlanBuilder,
    MachinePlanEdge,
    MachineResult,
    MediaShape,
    MergeStrategy,
    NodeExecutionResult,
    NodeId,
    PathArgumentRequirements,
    PlannerError,
    PlannerResult,
    ReachableTargetInfo,
    ResolvedArgument,
    ShapeCompatibility,
    SourceEntityType,
    StepArgumentRequirements,
    StepToken,
    StepTokenError,
    Strand,
    StrandInput,
    StrandShapeAnalysis,
    StrandStep,
    StructureCompatibility,
};

// Machine notation — typed DAG path identifiers
pub use machine::{
    parse_machine, parse_machine_async, parse_machine_with_node_names,
    parse_machine_with_node_names_async, Machine, MachineAbstractionError, MachineEdge,
    MachineParseError, MachineRun, MachineRunStatus, MachineStrand, MachineSyntaxError,
    NotationFormat, StrandNodeNames,
};

// Orchestrator — machine notation parsing and DAG execution
pub use orchestrator::{
    assemble_cbor_array,
    assemble_cbor_sequence,
    build_plans_from_notation,
    // Stream I/O — shared between orchestrator executor and floom-engine engine
    collect_terminal_output,
    decode_terminal_output,
    execute_dag,
    // Plan execution — the single ForEach/Collect-aware executor, shared by the
    // reference/CLI runtime and the engine.
    execute_plan, PlanInput,
    // Mid-run argument state — the dispatch-journaled ledger execute_plan
    // reads and the engine's UpdateMachineRunArguments RPC writes.
    AppliedArgumentUpdate, ArgumentUpdate, ArgumentUpdateDisposition, ArgumentUpdateOutcome,
    RunArgumentError, RunArgumentLedger,
    map_progress,
    parse_machine_to_cap_dag,
    plan_to_resolved_graph,
    run_dag_on_context,
    send_one_stream,
    split_cbor_array,
    split_cbor_sequence,
    unwrap_cbor_value,
    wrap_raw_items_as_cbor_sequence,
    ActivityTimer,
    BodyOutcomeFn,
    CapProgressFn,
    CapStepProgressFn,
    CartridgeManager,
    CborUtilError,
    CliRuntime,
    CreditGrantFn,
    CreditPlumbing,
    DagOutput,
    EdgeGroup,
    EngineRuntime,
    ExecutionContext,
    ExecutionError,
    FlowObserver,
    ForEachBodyCoordinate,
    ForEachItemSnapshot,
    ForEachItemsFn,
    IncrementalWriter,
    NodeData,
    OutputItem,
    ParseOrchestrationError,
    PipelineItemFn,
    PipelineLogFn,
    PipelineLogRecord,
    PipelineProgressTracker,
    PipelineResult,
    ProgressMapper,
    ResolvedEdge,
    ResolvedGraph,
    SegmentOutput,
    SegmentWriterFactory,
    TransientArtifact,
    StreamIoError,
    TerminalItem,
    TerminalMeta,
    TerminalOutput,
    WriterResult,
    PIPELINE_STALL_TIMEOUT_SECS,
};

// InputResolver — unified input resolution with media detection
pub use input_resolver::{
    detect_file_confirmed, detect_file_discriminated, detect_file_with_fabric_registry,
    discriminate_by_cartridge_handlers, discriminate_candidates_by_validation, discriminate_file,
    filter_by_handler_verdict, refine_survivors, FileDiscrimination,
    resolve_input, resolve_inputs, resolve_inputs_confirmed, resolve_paths, AdapterResult,
    CartridgeAdapterInvoker, ContentStructure, InputItem, InputResolverError, MediaAdapterRegistry,
    ResolvedFile, ResolvedInputSet, ValueAdapter, ValueAdapterRegistry, ValueAdapterResult,
    MAX_CONTENT_INSPECTION_BYTES,
};
