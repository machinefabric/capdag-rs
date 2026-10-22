//! Tests of the plan executor against a runtime that runs no cartridge.
//!
//! Plans are built from machine notation by the real resolver and plan
//! builder, so what is tested is the path a machine takes: notation → plan →
//! `execute_plan`. Only the last hop — running one pipelined chain of steps on
//! cartridges — is scripted ([`Scripted`]): each cap is a closure from what a
//! step was sent to what it emits, how long it takes, and whether it fails.

use super::*;
use crate::cap::definition::{ArgSource, Cap, CapArg, CapOutput};
use crate::machine::{parse_machine_with_node_names_async, NodeId};
use crate::planner::{InputCardinality, MachinePlanBuilder};
use crate::urn::cap_urn::CapUrn;

// ---------------------------------------------------------------------------
// Caps
// ---------------------------------------------------------------------------

const TEXT: &str = "media:enc=utf-8";
const OTHER: &str = "media:enc=utf-8;other";
const HINT: &str = "media:enc=utf-8;hint";
const SAID: &str = "media:enc=utf-8;said";
const REST: &str = "media:enc=utf-8;rest";
const REJECTED: &str = "media:enc=utf-8;rejected";

/// The URN of the test cap `op`: text in, text out (its own flavour of it).
fn urn(op: &str) -> String {
    format!(r#"cap:in="{TEXT}";{op};out="{TEXT};{op}-out""#)
}

fn text_cap(op: &str) -> Cap {
    let cap_urn = CapUrn::from_string(&urn(op)).unwrap();
    let out = cap_urn.out_spec().to_string();
    let mut cap = Cap::new(cap_urn, op.to_string(), vec![op.to_string()]);
    cap.args.push(CapArg::new(
        TEXT.to_string(),
        "input",
        true,
        vec![ArgSource::Stdin {
            stdin: TEXT.to_string(),
        }],
    ));
    cap.outputs = vec![CapOutput::new(out, "output", "out".to_string())];
    cap
}

/// One more arg on `cap`: `media_urn` is its identity, and it takes any text.
fn with_arg(mut cap: Cap, name: &str, media_urn: &str, required: bool) -> Cap {
    let mut arg = CapArg::new(
        media_urn.to_string(),
        name,
        required,
        vec![ArgSource::CliFlag {
            cli_flag: format!("--{name}"),
        }],
    );
    arg.accepts = Some(TEXT.to_string());
    cap.args.push(arg);
    cap
}

fn with_output(mut cap: Cap, name: &str, media_urn: &str, optional: bool) -> Cap {
    let mut output = CapOutput::new(media_urn.to_string(), name, name.to_string());
    output.optional = optional;
    cap.outputs.push(output);
    cap
}

/// The arg of `among` that takes every item.
const ALL: &str = "media:all-of;enc=utf-8";
const NOW: &str = r#"cap:in="media:void";now;out="media:enc=utf-8;now-out""#;
const SAY: &str = r#"cap:in="media:void";say;out="media:enc=utf-8;say-out""#;
const SINK: &str = r#"cap:in="media:enc=utf-8";sink;out="media:void""#;

fn registry() -> Arc<FabricRegistry> {
    let mut split = text_cap("split");
    split.outputs[0].is_sequence = true;
    let mut join = text_cap("join");
    join.args[0].is_sequence = true;
    let mut route = with_output(text_cap("route"), "rejected", REJECTED, true);
    route.outputs[0].optional = true;

    let void_cap = |urn: &str, op: &str| {
        let cap_urn = CapUrn::from_string(urn).unwrap();
        let out = cap_urn.out_spec().to_string();
        let mut cap = Cap::new(cap_urn, op.to_string(), vec![op.to_string()]);
        cap.outputs = vec![CapOutput::new(out, "output", "out".to_string())];
        cap
    };
    let mut say = void_cap(SAY, "say");
    say.args.push(CapArg::new(
        SAID.to_string(),
        "text",
        true,
        vec![ArgSource::CliFlag {
            cli_flag: "--text".to_string(),
        }],
    ));
    let mut sink = Cap::new(
        CapUrn::from_string(SINK).unwrap(),
        "sink".to_string(),
        vec!["sink".to_string()],
    );
    sink.args.push(CapArg::new(
        TEXT.to_string(),
        "input",
        true,
        vec![ArgSource::Stdin {
            stdin: TEXT.to_string(),
        }],
    ));

    let registry = FabricRegistry::new_for_test();
    registry.add_caps_to_cache(vec![
        text_cap("upper"),
        text_cap("exclaim"),
        text_cap("slow"),
        text_cap("slower"),
        text_cap("boom"),
        text_cap("flaky"),
        split,
        join,
        route,
        with_arg(text_cap("pair"), "other", OTHER, true),
        {
            let mut among = with_arg(text_cap("among"), "all", ALL, true);
            among.args[1].is_sequence = true;
            among
        },
        with_arg(text_cap("label"), "hint", HINT, false),
        with_output(text_cap("headrest"), "rest", REST, false),
        void_cap(NOW, "now"),
        say,
        sink,
    ]);
    // A constant is held to its arg's media definition.
    registry.insert_cached_media_def_for_test(crate::fabric::registry::StoredMediaDef {
        dims: Vec::new(),
        version: 0,
        urn: SAID.to_string(),
        media_type: "text/plain".to_string(),
        title: "said".to_string(),
        profile_uri: None,
        schema: None,
        description: None,
        documentation: None,
        validation: None,
        metadata: None,
        extensions: Vec::new(),
    });
    Arc::new(registry)
}

/// The headers that give every test cap its op as an alias.
fn headers() -> String {
    let mut headers = String::new();
    for op in [
        "upper", "exclaim", "slow", "slower", "boom", "flaky", "split", "join", "route", "pair",
        "among", "label", "headrest",
    ] {
        headers.push_str(&format!("[{op} {}]", urn(op)));
    }
    headers.push_str(&format!("[now {NOW}][say {SAY}][sink {SINK}]"));
    headers
}

// ---------------------------------------------------------------------------
// The scripted runtime
// ---------------------------------------------------------------------------

/// One run of one step, as the script sees it.
#[derive(Debug, Clone)]
struct Call {
    op: String,
    /// Per input stream, in the order sent: its URN, its items, whether it
    /// was sent as a sequence.
    inputs: Vec<(String, Vec<Vec<u8>>, bool)>,
    /// The argument values the step was dispatched with.
    args: Vec<(String, Vec<u8>)>,
    body: Option<ForEachBodyCoordinate>,
    /// How many times this step has run before, in this run of its body.
    attempt: usize,
}

impl Call {
    /// The main input's one value, as text.
    fn main(&self) -> String {
        let (_, items, _) = self
            .inputs
            .iter()
            .find(|(stream, _, _)| same_media(stream, TEXT))
            .unwrap_or_else(|| panic!("step '{}' was sent no main input: {:?}", self.op, self.inputs));
        assert_eq!(items.len(), 1, "step '{}' main input: {items:?}", self.op);
        String::from_utf8(items[0].clone()).unwrap()
    }

    /// The items of the stream delivered under `urn`, as text; `None` when
    /// no such stream was sent.
    fn stream(&self, urn: &str) -> Option<Vec<String>> {
        self.inputs
            .iter()
            .find(|(stream, _, _)| same_media(stream, urn))
            .map(|(_, items, _)| items.iter().map(|i| String::from_utf8(i.clone()).unwrap()).collect())
    }
}

fn same_media(a: &str, b: &str) -> bool {
    crate::MediaUrn::from_string(a)
        .unwrap()
        .is_equivalent(&crate::MediaUrn::from_string(b).unwrap())
        .unwrap()
}

enum Emit {
    Value(String),
    Sequence(Vec<String>),
    /// An optional output that does not fire.
    Absent,
    /// An unbounded stream that ended: its items are in a spool file.
    Spooled(std::path::PathBuf),
}

/// What a scripted step does: after `after`, emit on each named output (`""`
/// is the main one), or fail.
struct Reply {
    after: Duration,
    result: Result<Vec<(&'static str, Emit)>, ExecutionError>,
}

fn value(text: impl Into<String>) -> Reply {
    Reply {
        after: Duration::ZERO,
        result: Ok(vec![("", Emit::Value(text.into()))]),
    }
}

fn after(after: Duration, mut reply: Reply) -> Reply {
    reply.after = after;
    reply
}

fn failure(class: AttributionClass, code: &str, message: &str) -> Reply {
    Reply {
        after: Duration::ZERO,
        result: Err(ExecutionError::CartridgeExecutionFailed {
            cap_urn: "cap:scripted".to_string(),
            code: Some(code.to_string()),
            class,
            details: message.to_string(),
            arg_urn: None,
        }),
    }
}

type Behaviour = Arc<dyn Fn(&Call) -> Reply + Send + Sync>;

struct Scripted {
    registry: Arc<FabricRegistry>,
    behaviours: Mutex<HashMap<String, Behaviour>>,
    policy: MapFailurePolicy,
    /// Whether terminals are "persisted": returned as writer results.
    persist: bool,
    transient_root: Option<std::path::PathBuf>,
    /// Every run of every step, in the order they began.
    calls: Mutex<Vec<Call>>,
    /// The steps of each segment handed over, in order.
    segments: Mutex<Vec<Vec<String>>>,
    /// What each segment's head was given: `(node, kind)`.
    given: Mutex<Vec<(String, String)>>,
    /// The ops whose run was cut short by the run's failure signal.
    cancelled: Mutex<Vec<String>>,
    /// How many times each step has run, per run of its body.
    attempts: Mutex<HashMap<(String, Option<Vec<MapRun>>), usize>>,
    taps: std::sync::atomic::AtomicUsize,
}

impl Scripted {
    fn new() -> Arc<Self> {
        Self::with(MapFailurePolicy::Fail, false, None)
    }

    fn with(
        policy: MapFailurePolicy,
        persist: bool,
        transient_root: Option<std::path::PathBuf>,
    ) -> Arc<Self> {
        let runtime = Arc::new(Self {
            registry: registry(),
            behaviours: Mutex::new(HashMap::new()),
            policy,
            persist,
            transient_root,
            calls: Mutex::new(Vec::new()),
            segments: Mutex::new(Vec::new()),
            given: Mutex::new(Vec::new()),
            cancelled: Mutex::new(Vec::new()),
            attempts: Mutex::new(HashMap::new()),
            taps: std::sync::atomic::AtomicUsize::new(0),
        });
        runtime.on("upper", |call| value(call.main().to_uppercase()));
        runtime.on("exclaim", |call| value(format!("{}!", call.main())));
        runtime.on("slow", |call| after(Duration::from_secs(10), value(format!("{}~", call.main()))));
        runtime.on("slower", |call| after(Duration::from_secs(100), value(format!("{}~~", call.main()))));
        runtime.on("boom", |_| after(Duration::from_secs(1), failure(AttributionClass::Input, "E_BOOM", "boom")));
        runtime.on("flaky", |call| value(call.main()));
        runtime.on("split", |call| Reply {
            after: Duration::ZERO,
            result: Ok(vec![(
                "",
                Emit::Sequence(call.main().split(' ').filter(|w| !w.is_empty()).map(str::to_string).collect()),
            )]),
        });
        runtime.on("join", |call| value(call.stream(TEXT).expect("join is sent its sequence").join("+")));
        runtime.on("pair", |call| {
            let other = call.stream(OTHER).expect("pair is sent `other`");
            assert_eq!(other.len(), 1, "`other` takes one value");
            value(format!("{}|{}", call.main(), other[0]))
        });
        runtime.on("among", |call| {
            let all = call.stream(ALL).expect("among is sent `all`");
            value(format!("{} of {}", call.main(), all.join("+")))
        });
        runtime.on("label", |call| {
            let hint = call.stream(HINT).map(|h| h.join(","));
            value(format!("{}#{}", call.main(), hint.unwrap_or_else(|| "none".to_string())))
        });
        runtime.on("route", |call| {
            let text = call.main();
            let rejected = text.starts_with('x');
            Reply {
                after: Duration::ZERO,
                result: Ok(if rejected {
                    vec![("", Emit::Absent), ("rejected", Emit::Value(text))]
                } else {
                    vec![("", Emit::Value(text)), ("rejected", Emit::Absent)]
                }),
            }
        });
        runtime.on("headrest", |call| {
            let text = call.main();
            let (head, rest) = text.split_once(' ').unwrap_or((text.as_str(), ""));
            Reply {
                after: Duration::ZERO,
                result: Ok(vec![
                    ("", Emit::Value(head.to_string())),
                    ("rest", Emit::Value(rest.to_string())),
                ]),
            }
        });
        runtime.on("now", |_| value("T"));
        runtime.on("say", |call| {
            let said = call
                .args
                .iter()
                .find(|(urn, _)| same_media(urn, SAID))
                .map(|(_, bytes)| String::from_utf8(bytes.clone()).unwrap());
            value(said.unwrap_or_else(|| "(nothing)".to_string()))
        });
        runtime.on("sink", |_| Reply {
            after: Duration::ZERO,
            result: Ok(Vec::new()),
        });
        runtime
    }

    fn on(&self, op: &str, behaviour: impl Fn(&Call) -> Reply + Send + Sync + 'static) {
        self.behaviours
            .lock()
            .unwrap()
            .insert(op.to_string(), Arc::new(behaviour));
    }

    /// Every run of `op`, in the order they began.
    fn runs_of(&self, op: &str) -> Vec<Call> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| call.op == op)
            .cloned()
            .collect()
    }

    /// The segment each step of `ops` was handed over in, as ops.
    fn segment_ops(&self) -> Vec<Vec<String>> {
        self.segments.lock().unwrap().clone()
    }
}

/// The op of a test cap: its title.
fn op_of(step: &ResolvedStep) -> String {
    step.cap.title.clone()
}

#[async_trait]
impl EngineRuntime for Scripted {
    async fn segment_switch(
        &self,
        _graph: &ResolvedGraph,
    ) -> Result<Arc<crate::bifaci::relay_switch::RelaySwitch>, ExecutionError> {
        panic!("the scripted runtime overrides run_segment")
    }

    async fn activity_timeout_secs(&self, _graph: &ResolvedGraph) -> Result<u64, ExecutionError> {
        panic!("the scripted runtime overrides run_segment")
    }

    fn fabric_registry(&self) -> Arc<FabricRegistry> {
        self.registry.clone()
    }

    async fn map_failure_policy(&self) -> Result<MapFailurePolicy, ExecutionError> {
        Ok(self.policy)
    }

    fn transient_artifact_root(&self) -> Option<std::path::PathBuf> {
        self.transient_root.clone()
    }

    fn on_host_feed_open(&self, _handle: &crate::bifaci::live_feed::LiveFeedHandle) {
        self.taps.fetch_add(1, Ordering::SeqCst);
    }

    async fn run_segment(
        &self,
        graph: &ResolvedGraph,
        initial_inputs: HashMap<String, PlanInput>,
        initial_is_sequence: HashMap<String, bool>,
        cap_arguments: &HashMap<String, Vec<(String, Vec<u8>)>>,
        progress_fn: Option<&CapProgressFn>,
        _step_progress_fn: Option<&CapStepProgressFn>,
        _log_fn: Option<&PipelineLogFn>,
        body_coordinate: Option<ForEachBodyCoordinate>,
        _stall_tracker: Option<Arc<PipelineProgressTracker>>,
        persist_sinks: &HashSet<String>,
        cancel: &RunCancel,
    ) -> Result<SegmentOutput, ExecutionError> {
        self.segments
            .lock()
            .unwrap()
            .push(graph.steps.iter().map(op_of).collect());

        // What the segment was given, decoded to items.
        enum Value {
            Items(Vec<Vec<u8>>, bool),
            Absent,
            Spooled(std::path::PathBuf),
        }
        let mut held: HashMap<String, Value> = HashMap::new();
        let mut nodes: Vec<&String> = initial_inputs.keys().collect();
        nodes.sort();
        for node in nodes {
            let is_sequence = initial_is_sequence[node];
            let (kind, items) = match &initial_inputs[node] {
                PlanInput::Bytes(bytes) if is_sequence => (
                    "sequence",
                    crate::orchestrator::stream_io::decode_terminal_output(bytes, Some(true)).unwrap(),
                ),
                PlanInput::Bytes(bytes) => ("value", vec![bytes.clone()]),
                PlanInput::Spool { path } => (
                    "spool",
                    crate::orchestrator::stream_io::decode_terminal_output(
                        &std::fs::read(path).expect("the spool is there to read"),
                        Some(true),
                    )
                    .unwrap(),
                ),
                PlanInput::LiveReference { reference_urn, selector } => (
                    "live",
                    vec![format!("{reference_urn} {}", String::from_utf8_lossy(selector)).into_bytes()],
                ),
            };
            self.given.lock().unwrap().push((node.clone(), kind.to_string()));
            held.insert(node.clone(), Value::Items(items, is_sequence));
        }

        let body_path = body_coordinate.as_ref().map(|c| {
            let mut path = c.enclosing.clone();
            path.push(MapRun {
                foreach_token_id: c.foreach_token_id.clone(),
                body_index: c.body_index,
            });
            path
        });
        let total = graph.steps.len();
        for (index, step) in graph.steps.iter().enumerate() {
            let mut inputs = Vec::new();
            for input in &step.inputs {
                let read = |node: &String| match held.get(node) {
                    Some(Value::Items(items, is_sequence)) => (items.clone(), *is_sequence),
                    Some(Value::Spooled(path)) => (
                        crate::orchestrator::stream_io::decode_terminal_output(
                            &std::fs::read(path).unwrap(),
                            Some(true),
                        )
                        .unwrap(),
                        true,
                    ),
                    Some(Value::Absent) => panic!("'{}' was sent absent node '{node}'", op_of(step)),
                    None => panic!("'{}' reads '{node}', which the segment was not given", op_of(step)),
                };
                inputs.push(match &input.source {
                    StepSource::Node(node) => {
                        let (items, is_sequence) = read(node);
                        (input.stream_urn.clone(), items, is_sequence)
                    }
                    StepSource::Gather(nodes) => (
                        input.stream_urn.clone(),
                        nodes.iter().flat_map(|node| read(node).0).collect(),
                        true,
                    ),
                });
            }
            let attempt = {
                let mut attempts = self.attempts.lock().unwrap();
                let count = attempts.entry((step.node.clone(), body_path.clone())).or_insert(0);
                *count += 1;
                *count - 1
            };
            let call = Call {
                op: op_of(step),
                inputs,
                args: cap_arguments.get(&step.node).cloned().unwrap_or_default(),
                body: body_coordinate.clone(),
                attempt,
            };
            self.calls.lock().unwrap().push(call.clone());
            let behaviour = self
                .behaviours
                .lock()
                .unwrap()
                .get(&call.op)
                .cloned()
                .unwrap_or_else(|| panic!("no behaviour for '{}'", call.op));
            let reply = behaviour(&call);
            tokio::select! {
                _ = tokio::time::sleep(reply.after) => {}
                cause = cancel.raised() => {
                    self.cancelled.lock().unwrap().push(call.op.clone());
                    return Err(ExecutionError::Collateral { class: cause.class, detail: cause.detail });
                }
            }
            let emits = reply.result.map_err(|error| error.at_step(&step.token_id))?;
            for (name, emit) in emits {
                // The main output is stored at the step's node; another at
                // the node of that output — when something takes it.
                let node = if name.is_empty() {
                    Some(step.node.clone())
                } else {
                    let declared = step
                        .cap
                        .outputs
                        .iter()
                        .find(|output| output.name == name)
                        .unwrap_or_else(|| panic!("'{}' declares no output '{name}'", call.op));
                    step.outputs
                        .iter()
                        .find(|taken| same_media(&taken.output_media_urn, &declared.media_urn))
                        .map(|taken| taken.node.clone())
                };
                let Some(node) = node else { continue };
                held.insert(
                    node,
                    match emit {
                        Emit::Value(text) => Value::Items(vec![text.into_bytes()], false),
                        Emit::Sequence(items) => {
                            Value::Items(items.into_iter().map(String::into_bytes).collect(), true)
                        }
                        Emit::Absent => Value::Absent,
                        Emit::Spooled(path) => Value::Spooled(path),
                    },
                );
            }
            if let Some(progress) = progress_fn {
                progress((index + 1) as f32 / total as f32, &step.cap_urn, "step done");
            }
        }

        // Only what the LAST step produced is materialised, as in the real
        // segment executor.
        let mut out = SegmentOutput {
            node_data: HashMap::new(),
            node_is_sequence: HashMap::new(),
            writer_results: HashMap::new(),
            terminal_meta: HashMap::new(),
            node_spool: HashMap::new(),
            absent: HashSet::new(),
        };
        let last = graph.steps.last().expect("a segment has a step");
        for node in last.produced_nodes() {
            match held.remove(node) {
                None => {}
                Some(Value::Absent) => {
                    out.absent.insert(node.clone());
                }
                Some(Value::Spooled(path)) => {
                    out.node_is_sequence.insert(node.clone(), true);
                    out.node_spool.insert(node.clone(), path);
                }
                Some(Value::Items(items, is_sequence)) => {
                    out.node_is_sequence.insert(node.clone(), is_sequence);
                    if self.persist && persist_sinks.contains(node) {
                        out.writer_results.insert(
                            node.clone(),
                            vec![WriterResult {
                                is_sequence,
                                media_urn: String::new(),
                                saved_paths: items
                                    .iter()
                                    .map(|item| format!("saved:{}", String::from_utf8_lossy(item)))
                                    .collect(),
                                total_bytes: items.iter().map(Vec::len).sum(),
                                stream_meta: None,
                                item_metas: Vec::new(),
                            }],
                        );
                        out.node_data.insert(node.clone(), Vec::new());
                    } else {
                        out.node_data.insert(node.clone(), items);
                    }
                }
            }
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Building and running
// ---------------------------------------------------------------------------

/// A plan built from notation, with the node names the notation wrote.
struct Built {
    plan: MachinePlan,
    names: HashMap<String, NodeId>,
}

impl Built {
    /// The plan's input slot for the machine input named `name`.
    fn slot(&self, name: &str) -> String {
        format!("input_slot_{}", self.names[name])
    }

    /// The plan `Output` node for the machine output named `name`.
    fn output(&self, name: &str) -> String {
        format!("output_{}", self.names[name])
    }

    /// The step that runs the test cap `op`.
    fn step(&self, op: &str) -> String {
        let found: Vec<&String> = self
            .plan
            .nodes
            .iter()
            .filter(|(_, node)| {
                node.cap_urn().is_some_and(|urn| {
                    let wanted = match op {
                        "now" => NOW.to_string(),
                        "say" => SAY.to_string(),
                        "sink" => SINK.to_string(),
                        op => self::urn(op),
                    };
                    CapUrn::from_string(urn)
                        .unwrap()
                        .is_equivalent(&CapUrn::from_string(&wanted).unwrap())
                })
            })
            .map(|(id, _)| id)
            .collect();
        match found.as_slice() {
            [one] => (*one).clone(),
            other => panic!("expected one step running '{op}', found {other:?}"),
        }
    }
}

/// Build the plan of `wirings` (one strand). `sequences` names the machine
/// inputs this run binds to a sequence.
async fn build(wirings: &str, sequences: &[&str]) -> Built {
    let registry = registry();
    let notation = format!("{}{wirings}", headers());
    let (machine, names) = parse_machine_with_node_names_async(&notation, &registry)
        .await
        .unwrap_or_else(|e| panic!("{wirings}: {e}"));
    assert_eq!(machine.strands().len(), 1, "{wirings}: one strand");
    let strand = &machine.strands()[0];
    let names: HashMap<String, NodeId> = names[0].clone();
    let inputs: HashMap<NodeId, crate::planner::AnchorInput> = strand
        .input_anchor_ids()
        .iter()
        .map(|anchor| {
            let name = names
                .iter()
                .find(|(_, id)| *id == anchor)
                .map(|(name, _)| name.as_str())
                .expect("every input has the name the notation wrote");
            (
                *anchor,
                crate::planner::AnchorInput {
                    cardinality: if sequences.contains(&name) {
                        InputCardinality::Sequence
                    } else {
                        InputCardinality::Single
                    },
                    media: strand.node_urn(*anchor).clone(),
                },
            )
        })
        .collect();
    let plan = MachinePlanBuilder::new(registry)
        .build_plan_from_machine_strand_for_run("test", strand, &inputs)
        .await
        .unwrap_or_else(|e| panic!("{wirings}: {e}"));
    plan.validate().expect("the builder makes a valid plan");
    Built { plan, names }
}

/// What a machine input is given.
enum Given<'a> {
    One(&'a str),
    Many(&'a [&'a str]),
}

async fn run_with(
    built: &Built,
    runtime: &Arc<Scripted>,
    inputs: &[(&str, Given<'_>)],
    callbacks: RunCallbacks,
) -> Result<PipelineResult, ExecutionError> {
    let mut initial_inputs = HashMap::new();
    let mut flags = HashMap::new();
    for (name, given) in inputs {
        let slot = built.slot(name);
        match given {
            Given::One(text) => {
                initial_inputs.insert(slot.clone(), PlanInput::Bytes(text.as_bytes().to_vec()));
                flags.insert(slot, false);
            }
            Given::Many(items) => {
                let items: Vec<Vec<u8>> = items.iter().map(|i| i.as_bytes().to_vec()).collect();
                initial_inputs.insert(
                    slot.clone(),
                    PlanInput::Bytes(
                        crate::orchestrator::cbor_util::wrap_raw_items_as_cbor_sequence(&items).unwrap(),
                    ),
                );
                flags.insert(slot, true);
            }
        }
    }
    let ledger = Arc::new(RunArgumentLedger::new(&built.plan, HashMap::new()).expect("ledger"));
    execute_plan(
        &built.plan,
        runtime.clone() as Arc<dyn EngineRuntime>,
        initial_inputs,
        flags,
        ledger,
        callbacks,
    )
    .await
}

async fn run(
    built: &Built,
    runtime: &Arc<Scripted>,
    inputs: &[(&str, Given<'_>)],
) -> Result<PipelineResult, ExecutionError> {
    run_with(built, runtime, inputs, RunCallbacks::default()).await
}

/// The items produced at the machine output `name`, as text, with whether it
/// is a sequence.
fn produced(result: &PipelineResult, built: &Built, name: &str) -> (Vec<String>, bool) {
    let terminal = result
        .terminal(&built.output(name))
        .unwrap_or_else(|| panic!("no terminal for '{name}'"));
    let produced = terminal
        .produced()
        .unwrap_or_else(|| panic!("'{name}' was not produced"));
    (
        produced
            .items
            .iter()
            .map(|item| String::from_utf8(item.data.clone()).unwrap())
            .collect(),
        produced.is_sequence,
    )
}

fn texts(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

// ---------------------------------------------------------------------------
// Chains, outputs, sources and sinks
// ---------------------------------------------------------------------------

// TEST12770: a linear machine is one pipelined chain, handed over whole; its
// result is the last step's, a single value; the run completes with nothing
// to report, and — having no map — has one body outcome for the whole run.
#[tokio::test]
async fn test12770_a_linear_machine_is_one_chain() {
    let built = build("[doc -> upper -> u][u -> exclaim -> out]", &[]).await;
    let runtime = Scripted::new();
    let result = run(&built, &runtime, &[("doc", Given::One("hello"))]).await.unwrap();
    assert_eq!(produced(&result, &built, "out"), (texts(&["HELLO!"]), false));
    assert_eq!(runtime.segment_ops(), vec![texts(&["upper", "exclaim"])]);
    assert_eq!(result.verdict, RunVerdict::Completed);
    assert!(result.steps.is_empty());
    assert_eq!(result.body_outcomes.len(), 1);
    let whole = &result.body_outcomes[0];
    assert!(whole.success && whole.foreach_token_id.is_none() && whole.body_index == 0);
    assert_eq!(whole.cap_urns.len(), 2);
    // The terminal's media is what the run held there: the declared output.
    assert!(same_media(
        result.terminal(&built.output("out")).unwrap().media_urn(),
        "media:enc=utf-8;exclaim-out"
    ));
}

// TEST12771: a step's outputs go where they were taken: its main output to
// the step wired from it, another output to the step wired from that. Each
// consumer is handed what its producer emitted on that output and nothing
// else, and a step with a second taken output runs as a chain of its own.
#[tokio::test]
async fn test12771_each_output_reaches_the_step_that_takes_it() {
    let built = build(
        "[doc -> headrest -> (h, rest=r)][h -> upper -> a][r -> exclaim -> b]",
        &[],
    )
    .await;
    let runtime = Scripted::new();
    let result = run(&built, &runtime, &[("doc", Given::One("alpha beta gamma"))]).await.unwrap();
    assert_eq!(produced(&result, &built, "a"), (texts(&["ALPHA"]), false));
    assert_eq!(produced(&result, &built, "b"), (texts(&["beta gamma!"]), false));
    let mut segments = runtime.segment_ops();
    segments.sort();
    assert_eq!(
        segments,
        vec![texts(&["exclaim"]), texts(&["headrest"]), texts(&["upper"])],
        "the two-output step is not pipelined into either consumer"
    );

    // An output of the machine that is not a step's main one is produced at
    // its own terminal, beside the main one.
    let built = build("[doc -> headrest -> (h, rest=r)]", &[]).await;
    let result = run(&built, &Scripted::new(), &[("doc", Given::One("alpha beta"))]).await.unwrap();
    assert_eq!(produced(&result, &built, "h"), (texts(&["alpha"]), false));
    assert_eq!(produced(&result, &built, "r"), (texts(&["beta"]), false));
    assert!(same_media(result.terminal(&built.output("r")).unwrap().media_urn(), REST));
}

// TEST12772: a machine may start from a step nothing is wired into and end
// in a step nothing reads: it is given no input, the source is sent no
// stream, and the run has no terminal. A constant the machine writes reaches
// its step as that arg's value.
#[tokio::test]
async fn test12772_a_machine_with_no_input_and_no_output_runs() {
    let built = build("[() -> now -> t][t -> sink -> ()]", &[]).await;
    let runtime = Scripted::new();
    let result = run(&built, &runtime, &[]).await.unwrap();
    assert!(result.terminals.is_empty());
    assert_eq!(result.verdict, RunVerdict::Completed);
    let now = runtime.runs_of("now");
    assert_eq!(now.len(), 1);
    assert!(now[0].inputs.is_empty(), "a source is sent no stream");
    assert_eq!(runtime.runs_of("sink")[0].stream(TEXT), Some(texts(&["T"])));

    let built = build("[(text=\"good morning\") -> say -> out]", &[]).await;
    let runtime = Scripted::new();
    let result = run(&built, &runtime, &[]).await.unwrap();
    assert_eq!(produced(&result, &built, "out"), (texts(&["good morning"]), false));
    let say = &runtime.runs_of("say")[0];
    assert!(say.inputs.is_empty(), "a constant is a value, not a stream wired in");
    assert_eq!(say.args.len(), 1);
}

// TEST12773: every input of the plan is given exactly one value, with its
// cardinality: a missing one, a missing flag, and a value for something the
// plan has no input for are each refused, naming it.
#[tokio::test]
async fn test12773_a_plan_is_given_exactly_its_inputs() {
    let built = build("[doc -> upper -> out]", &[]).await;
    let slot = built.slot("doc");
    let attempt = |inputs: Vec<(&str, PlanInput)>, flags: Vec<(&str, bool)>| {
        let plan = built.plan.clone();
        let inputs: HashMap<String, PlanInput> =
            inputs.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
        let flags: HashMap<String, bool> =
            flags.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
        async move {
            let ledger = Arc::new(RunArgumentLedger::new(&plan, HashMap::new()).unwrap());
            execute_plan(
                &plan,
                Scripted::new() as Arc<dyn EngineRuntime>,
                inputs,
                flags,
                ledger,
                RunCallbacks::default(),
            )
            .await
            .map(|_| ())
            .unwrap_err()
            .to_string()
        }
    };
    let bytes = || PlanInput::Bytes(b"x".to_vec());
    assert!(attempt(vec![], vec![]).await.contains(&format!("input '{slot}' of the plan was given no value")));
    assert!(attempt(vec![(&slot, bytes())], vec![]).await.contains("has no sequence flag"));
    let stray = attempt(vec![(&slot, bytes()), ("elsewhere", bytes())], vec![(&slot, false)]).await;
    assert!(stray.contains("\"elsewhere\"") && stray.contains("no input for"), "{stray}");
    let stray_flag = attempt(vec![(&slot, bytes())], vec![(&slot, false), ("elsewhere", true)]).await;
    assert!(stray_flag.contains("\"elsewhere\""), "{stray_flag}");
}

// ---------------------------------------------------------------------------
// Maps
// ---------------------------------------------------------------------------

// TEST12774: a map runs its body once per item and collects in ITEM order,
// whichever run finished first; a step that takes the sequence is given all
// of it; each run has a body outcome under the map's token, and each item
// was announced before its run, in order.
#[tokio::test(start_paused = true)]
async fn test12774_a_map_collects_in_item_order_whatever_finishes_first() {
    let built = build(
        "[doc -> split -> words][words -> upper -> w][w -> join -> out][w -> exclaim -> each]",
        &[],
    )
    .await;
    let runtime = Scripted::new();
    // The first item takes longest: completion order is the reverse of item order.
    runtime.on("upper", |call| {
        let text = call.main();
        let delay = match text.as_str() {
            "one" => 30,
            "two" => 20,
            _ => 10,
        };
        after(Duration::from_secs(delay), value(text.to_uppercase()))
    });
    let announced: Arc<Mutex<Vec<(usize, Option<String>)>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = announced.clone();
    let callbacks = RunCallbacks {
        foreach_items: Some(Arc::new(move |_token, items| {
            sink.lock().unwrap().extend(
                items.iter().map(|item| (item.body_index, item.item_preview_text.clone())),
            );
            Ok(())
        })),
        ..Default::default()
    };
    let result = run_with(&built, &runtime, &[("doc", Given::One("one two three"))], callbacks)
        .await
        .unwrap();
    assert_eq!(produced(&result, &built, "out"), (texts(&["ONE+TWO+THREE"]), false));
    // A machine output inside the map is the sequence of it over the runs.
    assert_eq!(produced(&result, &built, "each"), (texts(&["ONE!", "TWO!", "THREE!"]), true));
    assert_eq!(runtime.runs_of("join").len(), 1, "the fold runs once, on the collected sequence");

    assert_eq!(
        *announced.lock().unwrap(),
        vec![
            (0, Some("one".to_string())),
            (1, Some("two".to_string())),
            (2, Some("three".to_string()))
        ]
    );
    let mut outcomes: Vec<(usize, bool)> = result
        .body_outcomes
        .iter()
        .map(|outcome| {
            assert!(outcome.foreach_token_id.is_some() && outcome.enclosing.is_empty());
            (outcome.body_index, outcome.success)
        })
        .collect();
    outcomes.sort();
    assert_eq!(outcomes, vec![(0, true), (1, true), (2, true)]);
    // Each run was a segment of its own, inside the map.
    assert!(runtime.runs_of("upper").iter().all(|call| call.body.is_some()));
    assert!(runtime.runs_of("join")[0].body.is_none());
}

// TEST12775: sequences on two args that each take one value are zipped: run
// i is given item i of each. Sequences of different lengths are refused
// before anything is run on them, naming each source and what it gave.
#[tokio::test]
async fn test12775_two_sequences_are_zipped_and_unequal_lengths_refused() {
    let wirings = "[a -> split -> xs][b -> split -> ys][(xs, other=ys) -> pair -> p]";
    let built = build(wirings, &[]).await;
    let runtime = Scripted::new();
    let result = run(
        &built,
        &runtime,
        &[("a", Given::One("1 2 3")), ("b", Given::One("x y z"))],
    )
    .await
    .unwrap();
    assert_eq!(produced(&result, &built, "p"), (texts(&["1|x", "2|y", "3|z"]), true));

    let runtime = Scripted::new();
    let error = run(
        &built,
        &runtime,
        &[("a", Given::One("1 2 3")), ("b", Given::One("x y"))],
    )
    .await
    .unwrap_err();
    assert!(runtime.runs_of("pair").is_empty(), "nothing is run on sequences that cannot be zipped");
    assert_eq!(error.attribution_class(), AttributionClass::Input);
    match &error {
        ExecutionError::StepFailed { source, .. } => match source.as_ref() {
            ExecutionError::ZipLengthMismatch { sources, .. } => {
                let mut lengths: Vec<usize> = sources.iter().map(|(_, n)| *n).collect();
                lengths.sort();
                assert_eq!(lengths, vec![2, 3]);
                // Each source is named by the node that holds it: the two splits.
                assert_eq!(sources.len(), 2);
                assert!(sources.iter().all(|(node, _)| built.plan.nodes[node].is_cap()));
                assert_ne!(sources[0].0, sources[1].0);
            }
            other => panic!("expected ZipLengthMismatch, got {other}"),
        },
        other => panic!("expected the map's step to be named, got {other}"),
    }
    let message = error.to_string();
    assert!(message.contains("gave 3") && message.contains("gave 2"), "{message}");
}

// TEST12776: a sequence produced inside a map and bound to an arg that takes
// one value is mapped inside it. A step that takes a sequence folds what the
// inner map produced once per OUTER item; from the machine, what the inner
// map produced is one flat sequence in order; and a run of the inner map is
// addressed by its own index under the outer run it is in.
#[tokio::test]
async fn test12776_a_map_inside_a_map_flattens_and_a_fold_closes_one_level() {
    let built = build(
        "[docs -> split -> words][words -> upper -> w][w -> join -> line][w -> exclaim -> e]",
        &["docs"],
    )
    .await;
    let runtime = Scripted::new();
    let result = run(&built, &runtime, &[("docs", Given::Many(&["a b", "c", "d e f"]))])
        .await
        .unwrap();
    assert_eq!(produced(&result, &built, "line"), (texts(&["A+B", "C", "D+E+F"]), true));
    assert_eq!(
        produced(&result, &built, "e"),
        (texts(&["A!", "B!", "C!", "D!", "E!", "F!"]), true)
    );
    assert_eq!(runtime.runs_of("split").len(), 3);
    assert_eq!(runtime.runs_of("join").len(), 3);

    // A step of the inner map that is also given the sequence the OUTER map
    // iterates is given the outer run's item: each word beside the document
    // it came from.
    let paired = build("[docs -> split -> words][(words, other=docs) -> pair -> p]", &["docs"]).await;
    let paired_result = run(&paired, &Scripted::new(), &[("docs", Given::Many(&["a b", "c"]))])
        .await
        .unwrap();
    assert_eq!(
        produced(&paired_result, &paired, "p"),
        (texts(&["a|a b", "b|a b", "c|c"]), true)
    );

    // The run "E" was upper-cased in: outer run 2, inner run 1.
    let e = runtime
        .runs_of("upper")
        .into_iter()
        .find(|call| call.main() == "e")
        .unwrap();
    let body = e.body.expect("inside the maps");
    assert_eq!(body.body_index, 1);
    assert_eq!(body.enclosing.len(), 1);
    assert_eq!(body.enclosing[0].body_index, 2);
    assert_ne!(body.enclosing[0].foreach_token_id, body.foreach_token_id);
    // Body outcomes: three outer runs, six inner ones under them.
    let inner: Vec<_> = result.body_outcomes.iter().filter(|o| !o.enclosing.is_empty()).collect();
    assert_eq!(inner.len(), 6);
    assert_eq!(result.body_outcomes.len(), 9);
}

// TEST12814: a step run once per item of a sequence, that also takes the
// whole sequence on an arg of its own, is given both in every run: the item
// and every item. What makes the sequence runs once.
#[tokio::test]
async fn test12814_a_run_of_a_map_is_given_the_item_and_the_whole_sequence() {
    let built = build("[doc -> split -> words][(words, all=words) -> among -> out]", &[]).await;
    let runtime = Scripted::new();
    let result = run(&built, &runtime, &[("doc", Given::One("a b c"))]).await.unwrap();
    assert_eq!(
        produced(&result, &built, "out"),
        (texts(&["a of a+b+c", "b of a+b+c", "c of a+b+c"]), true)
    );
    assert_eq!(runtime.runs_of("split").len(), 1);
    assert_eq!(runtime.runs_of("among").len(), 3);
}

// TEST12828: sequences whose lengths are known and differ are refused when
// the map is set up, before an item of either is read: nothing can have been
// run on them. Equal known lengths are taken.
#[tokio::test]
async fn test12828_known_unequal_lengths_are_refused_before_an_item_is_read() {
    let source = |known: usize| {
        // Nothing is ever sent: a zip that waited for an item would wait for ever.
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        (
            tx,
            RegionItemSource {
                rx,
                known_total: Some(known),
                feed_handle: None,
            },
        )
    };
    let token: StepToken = "zip-known".parse().unwrap();
    let nodes = || vec!["a".to_string(), "b".to_string()];
    let ((_keep_a, a), (_keep_b, b)) = (source(3), source(2));
    match ZippedSources::new(token.clone(), nodes(), vec![a, b]) {
        Err(ExecutionError::ZipLengthMismatch { sources, foreach_token_id }) => {
            assert_eq!(sources, vec![("a".to_string(), 3), ("b".to_string(), 2)]);
            assert_eq!(foreach_token_id, token);
        }
        Err(other) => panic!("expected ZipLengthMismatch, got {other}"),
        Ok(_) => panic!("sequences of 3 and 2 items cannot be zipped"),
    }
    let ((_keep_a, a), (_keep_b, b)) = (source(2), source(2));
    let zipped = ZippedSources::new(token, nodes(), vec![a, b]).expect("equal lengths zip");
    assert_eq!(zipped.known_total(), Some(2));
}

// TEST12829: sequences read as they arrive, with no length known ahead, are
// zipped item by item. When one ends while another still gives an item, that
// is a length mismatch, reported with what each gave; when they end together
// the zip ends.
#[tokio::test]
async fn test12829_a_sequence_that_ends_early_is_a_mismatch_found_while_reading() {
    async fn arriving(items: &[&str]) -> RegionItemSource {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        for item in items {
            tx.send(Ok(RegionItemDelivery {
                bytes: item.as_bytes().to_vec(),
                preview: None,
                byte_count: item.len() as u64,
            }))
            .await
            .unwrap();
        }
        RegionItemSource {
            rx,
            known_total: None,
            feed_handle: None,
        }
    }
    let token: StepToken = "zip-arriving".parse().unwrap();
    let nodes = || vec!["a".to_string(), "b".to_string()];
    let bytes = |items: Vec<RegionItemDelivery>| -> Vec<Vec<u8>> { items.into_iter().map(|d| d.bytes).collect() };

    let mut zipped =
        ZippedSources::new(token.clone(), nodes(), vec![arriving(&["a0", "a1"]).await, arriving(&["b0"]).await])
            .expect("nothing is known ahead");
    assert_eq!(zipped.known_total(), None);
    let first = zipped.next().await.unwrap().expect("item 0 of each");
    assert_eq!(bytes(first), vec![b"a0".to_vec(), b"b0".to_vec()]);
    match zipped.next().await {
        Err(ExecutionError::ZipLengthMismatch { sources, .. }) => {
            assert_eq!(sources, vec![("a".to_string(), 2), ("b".to_string(), 1)]);
        }
        Err(other) => panic!("expected ZipLengthMismatch, got {other}"),
        Ok(_) => panic!("`b` ended while `a` still gave an item"),
    }

    let mut even =
        ZippedSources::new(token, nodes(), vec![arriving(&["a0"]).await, arriving(&["b0"]).await]).unwrap();
    assert!(even.next().await.unwrap().is_some());
    assert!(even.next().await.unwrap().is_none(), "both ended together");
}

// TEST12830: a map over a sequence that is absent runs nothing, and what it
// would have produced is absent — not an empty sequence. The machine's output
// behind it is reported not produced. When the sequence is there, the same
// machine maps it.
#[tokio::test]
async fn test12830_a_map_over_an_absent_sequence_produces_nothing_at_all() {
    let built = build(
        "[doc -> route -> (kept, rejected=no)][no -> split -> words][words -> upper -> out]",
        &[],
    )
    .await;
    // `route` rejects what starts with an 'x': this is kept, so `no` is absent.
    let runtime = Scripted::new();
    let result = run(&built, &runtime, &[("doc", Given::One("plain words"))]).await.unwrap();
    assert!(runtime.runs_of("split").is_empty());
    assert!(runtime.runs_of("upper").is_empty());
    assert!(
        matches!(result.terminal(&built.output("out")).unwrap(), PlanTerminal::NotProduced { .. }),
        "an absent sequence is not an empty one"
    );
    assert_eq!(produced(&result, &built, "kept"), (texts(&["plain words"]), false));

    let runtime = Scripted::new();
    let result = run(&built, &runtime, &[("doc", Given::One("x a b"))]).await.unwrap();
    assert_eq!(produced(&result, &built, "out"), (texts(&["X", "A", "B"]), true));
}

// TEST12777: steps that map over the same sequence run in one map, so a
// step given what each produced is given both FOR THE SAME ITEM — and stays
// so when a run is filtered out: an item whose value is absent runs nothing
// further and contributes nothing, and the others keep their own partners.
#[tokio::test]
async fn test12777_values_of_one_map_stay_paired_when_an_item_is_filtered() {
    let built = build(
        "[doc -> split -> ws][ws -> route -> kept][kept -> upper -> u][(u, other=ws) -> pair -> p]",
        &[],
    )
    .await;
    let runtime = Scripted::new();
    let result = run(&built, &runtime, &[("doc", Given::One("ant xray bee"))]).await.unwrap();
    // "xray" is rejected: its `kept` is absent, so it is not upper-cased and
    // not paired. The others are paired with their OWN word.
    assert_eq!(produced(&result, &built, "p"), (texts(&["ANT|ant", "BEE|bee"]), true));
    assert_eq!(runtime.runs_of("upper").len(), 2);
    assert_eq!(runtime.runs_of("pair").len(), 2);
    // The skipped steps are reported, for the run they were skipped in.
    let skipped: Vec<(&str, usize)> = result
        .steps
        .iter()
        .filter(|report| matches!(report.outcome, StepOutcome::Skipped { .. }))
        .map(|report| {
            let op = if report.cap_urn.contains("upper") { "upper" } else { "pair" };
            (op, report.body.as_ref().expect("in the map").body_index)
        })
        .collect();
    assert_eq!(skipped.len(), 2);
    assert!(skipped.contains(&("upper", 1)) && skipped.contains(&("pair", 1)));
    assert_eq!(result.verdict, RunVerdict::Completed, "a filtered item is not a failure");
}

// ---------------------------------------------------------------------------
// Absent values
// ---------------------------------------------------------------------------

// TEST12778: an output that may not fire and does not leaves its node absent.
// A step with a required arg wired to it is skipped — reported so, naming
// the absent node — and everything it would have produced is absent, down to
// the machine output, which is NOT PRODUCED: not an empty value, not an
// error. The output that did fire runs its own consumers.
#[tokio::test]
async fn test12778_an_output_that_does_not_fire_skips_what_needs_it() {
    let built = build(
        "[doc -> route -> (kept, rejected=no)][kept -> upper -> a][a -> exclaim -> a2][no -> exclaim -> b]",
        &[],
    )
    .await;
    let runtime = Scripted::new();
    let result = run(&built, &runtime, &[("doc", Given::One("xenon"))]).await.unwrap();

    assert!(runtime.runs_of("upper").is_empty());
    assert_eq!(produced(&result, &built, "b"), (texts(&["xenon!"]), false));
    match result.terminal(&built.output("a2")).unwrap() {
        PlanTerminal::NotProduced { media_urn, .. } => {
            assert!(same_media(media_urn, "media:enc=utf-8;exclaim-out"));
        }
        other => panic!("expected NotProduced, got {other:?}"),
    }
    let route = built.step("route");
    let skipped: Vec<&StepReport> = result.steps.iter().collect();
    assert_eq!(skipped.len(), 2, "upper, and the exclaim after it: {skipped:?}");
    assert!(skipped.iter().any(|report| {
        report.cap_urn.contains("upper")
            && report.outcome == StepOutcome::Skipped { absent: vec![route.clone()] }
            && report.body.is_none()
    }));
    assert_eq!(result.verdict, RunVerdict::Completed);

    // The other way: `kept` fires, `rejected` does not.
    let runtime = Scripted::new();
    let result = run(&built, &runtime, &[("doc", Given::One("argon"))]).await.unwrap();
    assert_eq!(produced(&result, &built, "a2"), (texts(&["ARGON!"]), false));
    assert!(result.terminal(&built.output("b")).unwrap().produced().is_none());
}

// TEST12779: a gather sends the members that are present, in order, and an
// empty sequence when none is; an arg that is not required and is wired to an
// absent node is not sent at all. Neither skips the step.
#[tokio::test]
async fn test12779_a_gather_drops_absent_members_and_an_optional_arg_is_not_sent() {
    let built = build(
        "[doc -> route -> (kept, rejected=no)][(kept, no) -> join -> j][(doc, hint=kept) -> label -> l]",
        &[],
    )
    .await;
    let runtime = Scripted::new();
    let result = run(&built, &runtime, &[("doc", Given::One("xenon"))]).await.unwrap();
    // `kept` is absent: the gather holds `no` alone; `label` runs without its hint.
    assert_eq!(produced(&result, &built, "j"), (texts(&["xenon"]), false));
    assert_eq!(produced(&result, &built, "l"), (texts(&["xenon#none"]), false));
    assert!(result.steps.is_empty(), "nothing was skipped");

    let runtime = Scripted::new();
    let result = run(&built, &runtime, &[("doc", Given::One("argon"))]).await.unwrap();
    assert_eq!(produced(&result, &built, "l"), (texts(&["argon#argon"]), false));

    // Every member absent: the step is sent an EMPTY sequence, and runs.
    let built = build("[doc -> route -> (kept, rejected=no)][no -> upper -> u][(kept, u) -> join -> j]", &[]).await;
    let runtime = Scripted::new();
    // A route that fires neither output.
    runtime.on("route", |_| Reply {
        after: Duration::ZERO,
        result: Ok(vec![("", Emit::Absent), ("rejected", Emit::Absent)]),
    });
    let result = run(&built, &runtime, &[("doc", Given::One("argon"))]).await.unwrap();
    let join = runtime.runs_of("join");
    assert_eq!(join.len(), 1, "a gather of nothing still runs its step");
    assert_eq!(join[0].stream(TEXT), Some(Vec::new()));
    assert_eq!(produced(&result, &built, "j"), (texts(&[""]), false));
}

// ---------------------------------------------------------------------------
// Scheduling
// ---------------------------------------------------------------------------

// TEST12780: steps with no path between them run at the same time: two
// ten-second steps and the step that needs both take ten seconds, not twenty.
#[tokio::test(start_paused = true)]
async fn test12780_independent_steps_run_at_the_same_time() {
    let built = build("[a -> slow -> x][b -> slow -> y][(x, other=y) -> pair -> p]", &[]).await;
    // `slow` is one cap used twice: tell the two steps apart by what they are given.
    let runtime = Scripted::new();
    let started = tokio::time::Instant::now();
    let result = run(&built, &runtime, &[("a", Given::One("a")), ("b", Given::One("b"))])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(produced(&result, &built, "p"), (texts(&["a~|b~"]), false));
    assert!(
        elapsed >= Duration::from_secs(10) && elapsed < Duration::from_secs(11),
        "two ten-second steps side by side take ten seconds, took {elapsed:?}"
    );
}

// TEST12781: the first step to fail for good ends the run: every other live
// request is stopped as its collateral, nothing more is started, and the run
// returns THAT failure — its step, its class, its code — not the stop it
// caused elsewhere.
#[tokio::test(start_paused = true)]
async fn test12781_a_failure_ends_its_siblings_and_is_what_the_run_returns() {
    let built = build("[a -> boom -> x][b -> slower -> y][(x, other=y) -> pair -> p]", &[]).await;
    let runtime = Scripted::new();
    let started = tokio::time::Instant::now();
    let error = run(&built, &runtime, &[("a", Given::One("a")), ("b", Given::One("b"))])
        .await
        .unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(2), "the run did not wait out the slow step");
    assert!(!error.is_collateral());
    assert_eq!(error.attribution_class(), AttributionClass::Input);
    assert_eq!(error.failure_code(), Some("E_BOOM"));
    let boom_token = match &built.plan.nodes[&built.step("boom")].node_type {
        ExecutionNodeType::Cap { token_id, .. } => token_id.clone(),
        _ => unreachable!(),
    };
    assert_eq!(error.step_token_id(), Some(&boom_token));
    assert_eq!(*runtime.cancelled.lock().unwrap(), texts(&["slower"]));
    assert!(runtime.runs_of("pair").is_empty());
}

// ---------------------------------------------------------------------------
// Step policy
// ---------------------------------------------------------------------------

/// `flaky` fails `failures` times with `class`, then returns its input.
fn flaky(runtime: &Scripted, failures: usize, class: AttributionClass) {
    runtime.on("flaky", move |call| {
        if call.attempt < failures {
            failure(class, "E_FLAKY", &format!("failed attempt {}", call.attempt + 1))
        } else {
            value(format!("{}:ok", call.main()))
        }
    });
}

// TEST12782: `retry=N` attempts a step again after a failure whose class is
// retryable, up to N further times. Every attempt is sent what the first was;
// a step that then succeeds completes, with its failed attempts on record. A
// step that is out of attempts fails the run with its last failure, and a
// failure that is the input's fault is not attempted again at all.
#[tokio::test(start_paused = true)]
async fn test12782_retry_attempts_a_retryable_failure_again() {
    let built = build("[doc -> flaky{retry=3} -> out]", &[]).await;
    let runtime = Scripted::new();
    flaky(&runtime, 2, AttributionClass::Resource);
    let result = run(&built, &runtime, &[("doc", Given::One("d"))]).await.unwrap();
    assert_eq!(produced(&result, &built, "out"), (texts(&["d:ok"]), false));
    let runs = runtime.runs_of("flaky");
    assert_eq!(runs.len(), 3);
    assert!(runs.iter().all(|call| call.main() == "d"), "every attempt is sent the same input");
    assert_eq!(result.verdict, RunVerdict::Completed, "a step that succeeded did not fail");
    assert_eq!(result.steps.len(), 1);
    match &result.steps[0].outcome {
        StepOutcome::Completed { failed_attempts } => {
            assert_eq!(
                failed_attempts,
                &vec![
                    StepFailure {
                        class: AttributionClass::Resource,
                        code: Some("E_FLAKY".to_string()),
                        message: "failed attempt 1".to_string(),
                    },
                    StepFailure {
                        class: AttributionClass::Resource,
                        code: Some("E_FLAKY".to_string()),
                        message: "failed attempt 2".to_string(),
                    },
                ]
            );
        }
        other => panic!("expected Completed with its attempts, got {other:?}"),
    }

    // One retry is not enough for two failures: the second failure stands.
    let built = build("[doc -> flaky{retry=1} -> out]", &[]).await;
    let runtime = Scripted::new();
    flaky(&runtime, 2, AttributionClass::Environment);
    let error = run(&built, &runtime, &[("doc", Given::One("d"))]).await.unwrap_err();
    assert_eq!(runtime.runs_of("flaky").len(), 2);
    assert_eq!(error.failure_reason(), "failed attempt 2");

    // No retry asked: one attempt.
    let built = build("[doc -> flaky -> out]", &[]).await;
    let runtime = Scripted::new();
    flaky(&runtime, 1, AttributionClass::Resource);
    assert!(run(&built, &runtime, &[("doc", Given::One("d"))]).await.is_err());
    assert_eq!(runtime.runs_of("flaky").len(), 1);

    // The input's fault, and a cancel: never attempted again, whatever was asked.
    for class in [AttributionClass::Input, AttributionClass::User] {
        let built = build("[doc -> flaky{retry=3} -> out]", &[]).await;
        let runtime = Scripted::new();
        flaky(&runtime, 1, class);
        let error = run(&built, &runtime, &[("doc", Given::One("d"))]).await.unwrap_err();
        assert_eq!(runtime.runs_of("flaky").len(), 1, "{class:?} is not retryable");
        assert_eq!(error.attribution_class(), class);
    }
}

// TEST12783: `on_error=absent` tolerates a step that has failed for good: its
// outputs are absent, what needs them is skipped, the run goes on and ends
// COMPLETED WITH TOLERATED FAILURES — never plain completed — with the
// failure on record. It tolerates after the retries it was given, and it
// never tolerates a cancel.
#[tokio::test(start_paused = true)]
async fn test12783_on_error_absent_tolerates_a_failure_and_says_so() {
    let wirings = |policy: &str| {
        format!("[doc -> flaky{policy} -> f][f -> upper -> out][doc -> exclaim -> other]")
    };
    let built = build(&wirings("{on_error=absent}"), &[]).await;
    let runtime = Scripted::new();
    flaky(&runtime, 9, AttributionClass::Input);
    let result = run(&built, &runtime, &[("doc", Given::One("d"))]).await.unwrap();
    assert_eq!(result.verdict, RunVerdict::CompletedWithToleratedFailures);
    assert!(result.terminal(&built.output("out")).unwrap().produced().is_none());
    assert_eq!(produced(&result, &built, "other"), (texts(&["d!"]), false), "the rest of the run went on");
    assert_eq!(runtime.runs_of("flaky").len(), 1);
    assert!(runtime.runs_of("upper").is_empty());
    let tolerated: Vec<&StepReport> = result
        .steps
        .iter()
        .filter(|report| matches!(report.outcome, StepOutcome::FailedTolerated { .. }))
        .collect();
    assert_eq!(tolerated.len(), 1);
    match &tolerated[0].outcome {
        StepOutcome::FailedTolerated { failure, failed_attempts } => {
            assert_eq!(failure.class, AttributionClass::Input);
            assert_eq!(failure.code.as_deref(), Some("E_FLAKY"));
            assert!(failed_attempts.is_empty());
        }
        _ => unreachable!(),
    }
    assert!(result
        .steps
        .iter()
        .any(|report| matches!(report.outcome, StepOutcome::Skipped { .. })));

    // With retries: attempted again first, then tolerated, with every attempt on record.
    let built = build(&wirings("{retry=2, on_error=absent}"), &[]).await;
    let runtime = Scripted::new();
    flaky(&runtime, 9, AttributionClass::Resource);
    let result = run(&built, &runtime, &[("doc", Given::One("d"))]).await.unwrap();
    assert_eq!(runtime.runs_of("flaky").len(), 3);
    match &result.steps.iter().find(|r| matches!(r.outcome, StepOutcome::FailedTolerated { .. })).unwrap().outcome {
        StepOutcome::FailedTolerated { failure, failed_attempts } => {
            assert_eq!(failed_attempts.len(), 2);
            assert_eq!(failure.message, "failed attempt 3");
        }
        _ => unreachable!(),
    }

    // A cancel is never tolerated.
    let built = build(&wirings("{on_error=absent}"), &[]).await;
    let runtime = Scripted::new();
    flaky(&runtime, 9, AttributionClass::User);
    let error = run(&built, &runtime, &[("doc", Given::One("d"))]).await.unwrap_err();
    assert_eq!(error.attribution_class(), AttributionClass::User);

    // A step that succeeds under the policy is a plain completion.
    let built = build(&wirings("{on_error=absent}"), &[]).await;
    let runtime = Scripted::new();
    flaky(&runtime, 0, AttributionClass::Input);
    let result = run(&built, &runtime, &[("doc", Given::One("d"))]).await.unwrap();
    assert_eq!(result.verdict, RunVerdict::Completed);
    assert_eq!(produced(&result, &built, "out"), (texts(&["D:OK"]), false));
}

// TEST12784: a run of a map's body that fails. Under a host that fails the
// run, it does — with that failure, the other runs stopped. Under a host that
// drops the item, the item contributes nothing, its failure is on its body
// outcome, and the run ends completed with tolerated failures — unless every
// run failed, which fails the run. A step that tolerates its own failure
// drops the item under either host.
#[tokio::test(start_paused = true)]
async fn test12784_a_failed_run_of_a_map_follows_the_hosts_policy() {
    let wirings = "[doc -> split -> ws][ws -> flaky -> f][f -> upper -> out]";
    let fail_on = |runtime: &Scripted, bad: &'static [&'static str]| {
        runtime.on("flaky", move |call| {
            let text = call.main();
            if bad.contains(&text.as_str()) {
                failure(AttributionClass::Input, "E_BAD", &format!("bad item {text}"))
            } else {
                after(Duration::from_secs(5), value(text))
            }
        });
    };

    let built = build(wirings, &[]).await;
    let runtime = Scripted::with(MapFailurePolicy::Fail, false, None);
    fail_on(&runtime, &["two"]);
    let error = run(&built, &runtime, &[("doc", Given::One("one two three"))]).await.unwrap_err();
    assert_eq!(error.failure_reason(), "bad item two");
    assert_eq!(error.failure_code(), Some("E_BAD"));
    let mut cancelled = runtime.cancelled.lock().unwrap().clone();
    cancelled.sort();
    assert_eq!(cancelled, texts(&["flaky", "flaky"]), "the two good runs were stopped");
    assert!(runtime.runs_of("upper").is_empty());

    let runtime = Scripted::with(MapFailurePolicy::DropItem, false, None);
    fail_on(&runtime, &["two"]);
    let result = run(&built, &runtime, &[("doc", Given::One("one two three"))]).await.unwrap();
    assert_eq!(produced(&result, &built, "out"), (texts(&["ONE", "THREE"]), true));
    assert_eq!(result.verdict, RunVerdict::CompletedWithToleratedFailures);
    let failed: Vec<_> = result.body_outcomes.iter().filter(|o| !o.success).collect();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].body_index, 1);
    assert!(failed[0].error.as_deref().unwrap().contains("bad item two"));
    assert!(failed[0].failed_token_id.is_some());
    assert!(runtime.cancelled.lock().unwrap().is_empty(), "nothing was stopped");

    let runtime = Scripted::with(MapFailurePolicy::DropItem, false, None);
    fail_on(&runtime, &["one", "two", "three"]);
    let error = run(&built, &runtime, &[("doc", Given::One("one two three"))]).await.unwrap_err();
    assert!(error.to_string().contains("every run of the map failed (3 of 3)"), "{error}");

    // The step tolerates its own failure: the item is dropped even under a
    // host that fails the run, and no body outcome is a failure.
    let built = build("[doc -> split -> ws][ws -> flaky{on_error=absent} -> f][f -> upper -> out]", &[]).await;
    let runtime = Scripted::with(MapFailurePolicy::Fail, false, None);
    fail_on(&runtime, &["two"]);
    let result = run(&built, &runtime, &[("doc", Given::One("one two three"))]).await.unwrap();
    assert_eq!(produced(&result, &built, "out"), (texts(&["ONE", "THREE"]), true));
    assert_eq!(result.verdict, RunVerdict::CompletedWithToleratedFailures);
    assert!(result.body_outcomes.iter().all(|outcome| outcome.success));
}

// ---------------------------------------------------------------------------
// Progress and arguments
// ---------------------------------------------------------------------------

// TEST12785: progress is the sum of what each part has contributed: parts
// move independently and in any order, a part never takes back what it gave,
// and the total never goes down.
#[test]
fn test12785_progress_is_a_sum_of_shares() {
    let seen: Arc<Mutex<Vec<f32>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let progress = SharedProgress::new(
        &[1, 1, 2],
        Arc::new(move |p: f32, _: &str, _: &str| sink.lock().unwrap().push(p)),
    );
    let third = progress.part(2);
    progress.set(0, 1.0, "", "");
    third(0.5, "", "");
    progress.set(1, 0.4, "", "");
    progress.set(1, 0.2, "", ""); // a part does not go back
    progress.set(0, 7.0, "", ""); // nor past its share
    third(1.0, "", "");
    progress.set(1, 1.0, "", "");
    let seen = seen.lock().unwrap().clone();
    let expected = [0.25, 0.5, 0.6, 0.6, 0.6, 0.85, 1.0];
    assert_eq!(seen.len(), expected.len());
    for (got, want) in seen.iter().zip(expected) {
        assert!((got - want).abs() < 1e-6, "{seen:?}");
    }
}

// TEST12786: a run's progress never goes down, ends at one, and counts a
// step that is running beside another: with two of four steps half done the
// run is a quarter done, whichever reported first.
#[tokio::test(start_paused = true)]
async fn test12786_a_runs_progress_never_goes_down_and_ends_at_one() {
    let built = build(
        "[a -> slow -> x][b -> slow -> y][(x, other=y) -> pair -> p][p -> upper -> out]",
        &[],
    )
    .await;
    let runtime = Scripted::new();
    let seen: Arc<Mutex<Vec<f32>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let callbacks = RunCallbacks {
        progress: Some(Arc::new(move |p: f32, _: &str, _: &str| sink.lock().unwrap().push(p))),
        ..Default::default()
    };
    run_with(&built, &runtime, &[("a", Given::One("a")), ("b", Given::One("b"))], callbacks)
        .await
        .unwrap();
    let seen = seen.lock().unwrap().clone();
    assert!(seen.windows(2).all(|pair| pair[0] <= pair[1]), "{seen:?}");
    assert_eq!(*seen.last().unwrap(), 1.0);
    // Four steps: each of the two slow ones is a quarter of the run.
    assert!(seen.iter().any(|p| (p - 0.25).abs() < 1e-6), "{seen:?}");
    assert!(seen.iter().any(|p| (p - 0.5).abs() < 1e-6), "{seen:?}");
}

// TEST7119: a map's progress is reported under the MAP's own token — the
// identity a client renders it by — as the aggregate over its runs: it never
// goes down and ends at one. A host that follows single steps is told this
// whether or not it also follows the run's overall progress.
#[tokio::test(start_paused = true)]
async fn test7119_a_maps_progress_is_reported_under_its_own_token() {
    let built = build("[doc -> split -> ws][ws -> slow -> out]", &[]).await;
    let map_token = built
        .plan
        .nodes
        .values()
        .find_map(|node| match &node.node_type {
            ExecutionNodeType::ForEach { token_id, .. } => Some(token_id.to_string()),
            _ => None,
        })
        .expect("the plan has a map");
    for with_overall in [false, true] {
        let seen: Arc<Mutex<Vec<(f32, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let callbacks = RunCallbacks {
            step_progress: Some(Arc::new(move |p: f32, _cap: &str, token: &str| {
                sink.lock().unwrap().push((p, token.to_string()));
            })),
            progress: with_overall.then(|| Arc::new(|_: f32, _: &str, _: &str| {}) as CapProgressFn),
            ..Default::default()
        };
        run_with(&built, &Scripted::new(), &[("doc", Given::One("a b c d"))], callbacks)
            .await
            .unwrap();
        let seen = seen.lock().unwrap().clone();
        assert!(!seen.is_empty(), "the map reported (overall followed: {with_overall})");
        assert!(seen.iter().all(|(_, token)| *token == map_token), "{seen:?}");
        let values: Vec<f32> = seen.iter().map(|(p, _)| *p).collect();
        assert!(values.windows(2).all(|pair| pair[0] <= pair[1]), "{values:?}");
        assert_eq!(*values.last().unwrap(), 1.0);
        // Four runs: the aggregate passes through a quarter, a half, three quarters.
        for step in [0.25, 0.5, 0.75] {
            assert!(values.iter().any(|p| (p - step).abs() < 1e-6), "{values:?}");
        }
    }
}

// TEST12787: an argument update lands before or after a step's dispatch,
// never during it, and says which. A step that has run reports it was
// already dispatched and keeps what it read; a step of the same run that has
// not started yet reads the new value; inside a map the update says how many
// runs took the old one.
#[tokio::test(start_paused = true)]
async fn test12787_an_argument_update_lands_between_dispatches() {
    use crate::orchestrator::run_arguments::{ArgumentUpdate, ArgumentUpdateDisposition};
    // Three runs of `slow` in a map, and beside them `slower` then `pair` —
    // which has a second input, so it is dispatched only when `slower` ends.
    let built = build(
        "[doc -> split -> ws][ws -> slow -> s][doc -> slower -> t][(t, other=doc) -> pair -> out]",
        &[],
    )
    .await;
    let runtime = Scripted::new();
    let label = built.step("pair");
    let slow = built.step("slow");
    let ledger = Arc::new(
        RunArgumentLedger::new(
            &built.plan,
            HashMap::from([
                (label.clone(), vec![(HINT.to_string(), b"old".to_vec())]),
                (slow.clone(), vec![(HINT.to_string(), b"old".to_vec())]),
            ]),
        )
        .unwrap(),
    );
    let slot = built.slot("doc");
    let plan = built.plan.clone();
    let running = {
        let ledger = ledger.clone();
        let runtime = runtime.clone();
        tokio::spawn(async move {
            execute_plan(
                &plan,
                runtime as Arc<dyn EngineRuntime>,
                HashMap::from([(slot.clone(), PlanInput::Bytes(b"a b c".to_vec()))]),
                HashMap::from([(slot, false)]),
                ledger,
                RunCallbacks::default(),
            )
            .await
        })
    };
    // Five seconds in: the three runs of `slow` are under way; `slower` has
    // ninety-five to go, so `pair` has not been dispatched.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let update = |token: &str| ArgumentUpdate {
        token_id: token.to_string(),
        media_urn: HINT.to_string(),
        value: b"new".to_vec(),
    };
    let applied = ledger.apply(&[update(&label), update(&slow)]).unwrap();
    assert_eq!(applied.outcomes[0].disposition, ArgumentUpdateDisposition::Applied);
    assert_eq!(
        applied.outcomes[1].disposition,
        ArgumentUpdateDisposition::AppliedToRemainingBodies { bodies_dispatched: 3 }
    );
    running.await.unwrap().unwrap();

    let hint_of = |call: &Call| {
        call.args
            .iter()
            .find(|(urn, _)| same_media(urn, HINT))
            .map(|(_, bytes)| String::from_utf8(bytes.clone()).unwrap())
    };
    assert!(runtime.runs_of("slow").iter().all(|call| hint_of(call).as_deref() == Some("old")));
    assert_eq!(hint_of(&runtime.runs_of("pair")[0]).as_deref(), Some("new"));
    // The run is over: every step has been dispatched for the last time.
    let late = ledger.apply(&[update(&label), update(&slow)]).unwrap();
    assert!(late
        .outcomes
        .iter()
        .all(|outcome| outcome.disposition == ArgumentUpdateDisposition::AlreadyDispatched));
}

// ---------------------------------------------------------------------------
// What a segment is given, and what it gives back
// ---------------------------------------------------------------------------

// TEST1449: a live source enters the executor as PlanInput::LiveReference and
// reaches the segment that reads it intact — reference URN and selector —
// when it feeds a step that runs once: the cartridge resolves the capture.
#[tokio::test]
async fn test1449_live_reference_reaches_segment_intact() {
    let built = build("[feed -> upper -> out]", &[]).await;
    let runtime = Scripted::new();
    runtime.on("upper", |call| value(call.main()));
    let slot = built.slot("feed");
    let ledger = Arc::new(RunArgumentLedger::new(&built.plan, HashMap::new()).unwrap());
    let result = execute_plan(
        &built.plan,
        runtime.clone() as Arc<dyn EngineRuntime>,
        HashMap::from([(
            slot.clone(),
            PlanInput::LiveReference {
                reference_urn: "media:live;synthetic".to_string(),
                selector: b"{\"params\":{}}".to_vec(),
            },
        )]),
        HashMap::from([(slot.clone(), false)]),
        ledger,
        RunCallbacks::default(),
    )
    .await
    .unwrap();
    assert_eq!(*runtime.given.lock().unwrap(), vec![(slot, "live".to_string())]);
    assert_eq!(
        produced(&result, &built, "out").0,
        texts(&["media:live;synthetic {\"params\":{}}"])
    );
    assert_eq!(runtime.taps.load(Ordering::SeqCst), 0, "the host opened nothing");
}

// TEST1454: a live source mapped DIRECTLY is resolved BY THE HOST (13.2
// §Reference Media, host resolution): the executor opens the feed through
// the built-in capture dispatch — here the real `media:live;synthetic`
// backend — and dispatches one run per delivered item while the feed runs.
// The tap is registered with the runtime so a run stop can close it.
#[tokio::test]
async fn test1454_live_source_drives_a_map_host_side() {
    let built = build("[frames -> upper -> out]", &["frames"]).await;
    let runtime = Scripted::new();
    runtime.on("upper", |call| {
        let (_, items, _) = &call.inputs[0];
        value(format!("{}x{}", items[0][0], items[0].len()))
    });
    let announced: Arc<Mutex<Vec<(usize, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = announced.clone();
    let slot = built.slot("frames");
    let ledger = Arc::new(RunArgumentLedger::new(&built.plan, HashMap::new()).unwrap());
    let result = execute_plan(
        &built.plan,
        runtime.clone() as Arc<dyn EngineRuntime>,
        HashMap::from([(
            slot.clone(),
            PlanInput::LiveReference {
                reference_urn: "media:live;synthetic".to_string(),
                selector: br#"{"params":{"items":3,"interval_ms":0,"item_bytes":8}}"#.to_vec(),
            },
        )]),
        HashMap::from([(slot, true)]),
        ledger,
        RunCallbacks {
            foreach_items: Some(Arc::new(move |_token, items| {
                sink.lock().unwrap().extend(items.iter().map(|i| (i.body_index, i.item_byte_count)));
                Ok(())
            })),
            ..Default::default()
        },
    )
    .await
    .expect("a live source drives the map host-side");
    // One run per delivered item, with the feed's actual payload bytes (the
    // synthetic backend emits [i % 256; item_bytes]), collected in order.
    assert_eq!(produced(&result, &built, "out"), (texts(&["0x8", "1x8", "2x8"]), true));
    assert_eq!(*announced.lock().unwrap(), vec![(0, 8), (1, 8), (2, 8)]);
    assert_eq!(
        runtime.taps.load(Ordering::SeqCst),
        1,
        "the host must be able to close the feed on a run stop"
    );
}

/// A spool file of three CBOR byte-string items.
fn write_spool(path: &std::path::Path) {
    let mut bytes = Vec::new();
    for i in 0..3u8 {
        ciborium::ser::into_writer(
            &ciborium::Value::Bytes(format!("frame-{i}").into_bytes()),
            &mut bytes,
        )
        .unwrap();
    }
    std::fs::write(path, bytes).unwrap();
}

// TEST1456: an UNBOUNDED stream that ended (a live capture's content) is
// held on disk, never in memory. A map over it runs once per spooled item,
// with the item's real bytes; a step that runs once and reads it is handed
// the file, not its contents; and the run removes the spool when it ends.
#[tokio::test]
async fn test1456_a_spooled_value_drives_a_map_and_is_removed_at_the_end() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("frames.spool");
    write_spool(&spool);
    let built = build("[doc -> split -> frames][frames -> upper -> out][frames -> join -> all]", &[]).await;
    let runtime = Scripted::new();
    let path = spool.clone();
    runtime.on("split", move |_| Reply {
        after: Duration::ZERO,
        result: Ok(vec![("", Emit::Spooled(path.clone()))]),
    });
    let announced: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = announced.clone();
    let result = run_with(
        &built,
        &runtime,
        &[("doc", Given::One("doc"))],
        RunCallbacks {
            foreach_items: Some(Arc::new(move |_token, items| {
                sink.lock().unwrap().extend(items.iter().map(|i| i.item_byte_count));
                Ok(())
            })),
            ..Default::default()
        },
    )
    .await
    .expect("a spooled value drives the map");
    assert_eq!(
        produced(&result, &built, "out"),
        (texts(&["FRAME-0", "FRAME-1", "FRAME-2"]), true)
    );
    assert_eq!(*announced.lock().unwrap(), vec![7, 7, 7]);
    assert_eq!(produced(&result, &built, "all").0, texts(&["frame-0+frame-1+frame-2"]));
    // `join` was handed the spool itself.
    let split = built.step("split");
    assert!(runtime
        .given
        .lock()
        .unwrap()
        .contains(&(split, "spool".to_string())));
    assert!(!spool.exists(), "spool files are removed when the run ends");
}

// TEST1459: with transient capture ON, a spooled value belongs to the
// runtime's transient store — its lifetime is the store's, and the run does
// NOT remove it.
#[tokio::test]
async fn test1459_transient_capture_owns_spooled_values() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("frames.spool");
    write_spool(&spool);
    let built = build("[doc -> split -> frames][frames -> upper -> out]", &[]).await;
    let runtime = Scripted::with(MapFailurePolicy::Fail, false, Some(dir.path().to_path_buf()));
    let path = spool.clone();
    runtime.on("split", move |_| Reply {
        after: Duration::ZERO,
        result: Ok(vec![("", Emit::Spooled(path.clone()))]),
    });
    let result = run(&built, &runtime, &[("doc", Given::One("doc"))]).await.unwrap();
    assert_eq!(produced(&result, &built, "out").0.len(), 3);
    assert!(spool.exists(), "the transient store owns the file");
}

// TEST1460: a segment that records the SAME node both on disk and in memory
// is refused: one node, one truth. (This is the breach that once ran a whole
// capture as zero bodies — the empty in-memory entry was read, the spool
// ignored.)
#[tokio::test]
async fn test1460_a_node_recorded_on_disk_and_in_memory_is_refused() {
    /// The scripted runtime, lying about one segment.
    struct Duplicating(Arc<Scripted>);
    #[async_trait]
    impl EngineRuntime for Duplicating {
        async fn segment_switch(
            &self,
            graph: &ResolvedGraph,
        ) -> Result<Arc<crate::bifaci::relay_switch::RelaySwitch>, ExecutionError> {
            self.0.segment_switch(graph).await
        }
        async fn activity_timeout_secs(&self, graph: &ResolvedGraph) -> Result<u64, ExecutionError> {
            self.0.activity_timeout_secs(graph).await
        }
        fn fabric_registry(&self) -> Arc<FabricRegistry> {
            self.0.fabric_registry()
        }
        async fn map_failure_policy(&self) -> Result<MapFailurePolicy, ExecutionError> {
            self.0.map_failure_policy().await
        }
        async fn run_segment(
            &self,
            graph: &ResolvedGraph,
            initial_inputs: HashMap<String, PlanInput>,
            initial_is_sequence: HashMap<String, bool>,
            cap_arguments: &HashMap<String, Vec<(String, Vec<u8>)>>,
            progress_fn: Option<&CapProgressFn>,
            step_progress_fn: Option<&CapStepProgressFn>,
            log_fn: Option<&PipelineLogFn>,
            body_coordinate: Option<ForEachBodyCoordinate>,
            stall_tracker: Option<Arc<PipelineProgressTracker>>,
            persist_sinks: &HashSet<String>,
            cancel: &RunCancel,
        ) -> Result<SegmentOutput, ExecutionError> {
            let mut out = self
                .0
                .run_segment(
                    graph,
                    initial_inputs,
                    initial_is_sequence,
                    cap_arguments,
                    progress_fn,
                    step_progress_fn,
                    log_fn,
                    body_coordinate,
                    stall_tracker,
                    persist_sinks,
                    cancel,
                )
                .await?;
            for node in out.node_spool.keys() {
                out.node_data.insert(node.clone(), Vec::new());
            }
            Ok(out)
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("frames.spool");
    write_spool(&spool);
    let built = build("[doc -> split -> frames][frames -> upper -> out]", &[]).await;
    let scripted = Scripted::new();
    let path = spool.clone();
    scripted.on("split", move |_| Reply {
        after: Duration::ZERO,
        result: Ok(vec![("", Emit::Spooled(path.clone()))]),
    });
    let slot = built.slot("doc");
    let ledger = Arc::new(RunArgumentLedger::new(&built.plan, HashMap::new()).unwrap());
    let error = execute_plan(
        &built.plan,
        Arc::new(Duplicating(scripted.clone())) as Arc<dyn EngineRuntime>,
        HashMap::from([(slot.clone(), PlanInput::Bytes(b"doc".to_vec()))]),
        HashMap::from([(slot, false)]),
        ledger,
        RunCallbacks::default(),
    )
    .await
    .unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("is recorded both on disk and in memory"),
        "the breach is named: {message}"
    );
    assert!(scripted.runs_of("upper").is_empty(), "nothing ran on either reading");
}

// TEST12788: a runtime that persists terminals is given exactly the plan's
// terminals to persist — a step that is not a machine output is not — and
// what it wrote is what the terminal holds: paths, no items. Inside a map
// every run's files are the terminal's, in item order.
#[tokio::test]
async fn test12788_terminals_are_persisted_and_nothing_else_is() {
    let built = build("[doc -> split -> ws][ws -> upper -> out][doc -> exclaim -> e]", &[]).await;
    let runtime = Scripted::with(MapFailurePolicy::Fail, true, None);
    let result = run(&built, &runtime, &[("doc", Given::One("a b"))]).await.unwrap();
    let saved = |name: &str| {
        let produced = result.terminal(&built.output(name)).unwrap().produced().unwrap();
        assert!(produced.items.is_empty(), "the data is where the writer put it");
        produced
            .writer_results
            .iter()
            .flat_map(|writer| writer.saved_paths.clone())
            .collect::<Vec<String>>()
    };
    assert_eq!(saved("out"), texts(&["saved:A", "saved:B"]));
    assert_eq!(saved("e"), texts(&["saved:a b!"]));
    assert!(result.terminal(&built.output("out")).unwrap().produced().unwrap().is_sequence);
    // The words themselves were read by the map from memory: not a terminal.
    assert_eq!(runtime.runs_of("upper").len(), 2);
    // Each run's saved files are on its body outcome.
    let mut per_run: Vec<(usize, Vec<String>)> = result
        .body_outcomes
        .iter()
        .map(|outcome| (outcome.body_index, outcome.saved_paths.clone()))
        .collect();
    per_run.sort();
    assert_eq!(per_run, vec![(0, texts(&["saved:A"])), (1, texts(&["saved:B"]))]);
}

// TEST12874: a map's persisted files are the terminal's in ITEM order when its
// runs finish in the reverse order. A host lists, titles and numbers a run's
// outputs in the order the terminal gives them ("long · 1", "long · 2", …), so
// a completion-ordered terminal listed page 2 before page 1.
#[tokio::test(start_paused = true)]
async fn test12874_persisted_map_files_are_in_item_order_whatever_finishes_first() {
    let built = build("[doc -> split -> ws][ws -> upper -> out]", &[]).await;
    let runtime = Scripted::with(MapFailurePolicy::Fail, true, None);
    runtime.on("upper", |call| {
        let text = call.main();
        let delay = match text.as_str() {
            "one" => 30,
            "two" => 20,
            _ => 10,
        };
        after(Duration::from_secs(delay), value(text.to_uppercase()))
    });
    let result = run(&built, &runtime, &[("doc", Given::One("one two three"))]).await.unwrap();
    let produced = result.terminal(&built.output("out")).unwrap().produced().unwrap();
    let saved: Vec<String> = produced
        .writer_results
        .iter()
        .flat_map(|writer| writer.saved_paths.clone())
        .collect();
    assert_eq!(saved, texts(&["saved:ONE", "saved:TWO", "saved:THREE"]));
}
