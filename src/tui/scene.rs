//! What is drawn: the nodes and edges of a run, built as the uikit's run
//! scene builder builds them (`_buildRunScene`), so the terminal picture has
//! the same parts as the desktop's.
//!
//! A run is drawn from its plan. Its inputs are stacks of what each was
//! given; each step is a card, with a port for every argument that is not
//! wired; a value one step hands another gets a card of its own (a stack,
//! when the run captured it); a map is a stack of the items its body runs on,
//! and the edges out of it say "For each item"; a gather is a card; each
//! output is a stack of the files it was written as. Edge labels name the
//! argument an edge feeds, or the output of the step it leaves.
//!
//! Live state is laid over that structure — progress, queues, failures, the
//! protocol's flow — and never changes it: what the scene holds is decided by
//! the plan, and every live reading must resolve against it.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;

use crate::orchestrator::{PlanView, PlanViewEdgeKind, PlanViewKind, PlanViewNode};
use crate::tui::live::{ItemLive, OutputEnd, RunLive, RunStatus, StepPhase};
use crate::tui::text;
use crate::tui::theme::{Icon, MediaKind, StatusMark, Tone};
use crate::{AttributionClass, CapUrn};

/// The plan contradicts what a picture of it needs.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SceneError {
    #[error("plan node '{0}' is named by an edge but is not in the plan")]
    MissingNode(String),
    #[error("plan node '{node}' is a {kind}, which holds no value to read")]
    NotReadable { node: String, kind: &'static str },
    #[error("plan edge {index} feeds '{node}', a {kind}, which nothing feeds")]
    FeedsUnfeedable {
        index: usize,
        node: String,
        kind: &'static str,
    },
    #[error("plan edge {index} feeds output '{output}' of '{step}' from '{from}'")]
    OutputFedByOtherStep {
        index: usize,
        output: String,
        step: String,
        from: String,
    },
    #[error("output '{output}' of the run's plan is fed by {count} edges; an output takes exactly one")]
    OutputFeeds { output: String, count: usize },
    #[error("the run's plan has a cycle through '{0}'")]
    Cycle(String),
    #[error("input '{0}' of the run's plan was given no source")]
    InputWithoutSource(String),
    #[error("a source names '{0}', which is no input of the run's plan")]
    SourceForNoInput(String),
    #[error("the run's plan draws nothing")]
    Empty,
    #[error("step '{0}' has arguments recorded, but the plan has no such step")]
    ArgumentsForNoStep(String),
    #[error("cap URN '{urn}' of step '{node}' does not parse: {reason}")]
    InvalidCapUrn {
        node: String,
        urn: String,
        reason: String,
    },
}

/// Where an argument's value comes from (the desktop's origins).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgOrigin {
    /// Wired from an input of the machine.
    Input,
    /// Wired from what another step makes.
    Upstream,
    /// A constant the machine writes.
    Constant,
    /// The cap's own default.
    Default,
    /// Given by whoever ran it.
    User,
}

impl ArgOrigin {
    /// How the step inspector names it.
    pub fn label(&self) -> &'static str {
        match self {
            ArgOrigin::Input => "from input",
            ArgOrigin::Upstream => "from previous step",
            ArgOrigin::Constant => "machine constant",
            ArgOrigin::Default => "default",
            ArgOrigin::User => "user",
        }
    }

    /// Whether the argument is an edge into the step rather than a port on it.
    pub fn is_wired(&self) -> bool {
        matches!(self, ArgOrigin::Input | ArgOrigin::Upstream)
    }
}

/// One argument of one step, as the step ran (or will run) with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepArgument {
    /// The arg's name in its cap definition.
    pub name: String,
    pub media_urn: String,
    pub is_main_input: bool,
    pub is_required: bool,
    pub is_sequence: bool,
    pub origin: ArgOrigin,
    /// The value as text, when it has one to show.
    pub value_text: Option<String>,
    /// The cap's own default, when it declares one.
    pub default_text: Option<String>,
}

/// One source an input of a run was given.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    File(PathBuf),
    /// A value named on the command line (`--input name=value`).
    Value(String),
    /// A live device: what it was asked for, and what it delivers.
    Live {
        reference_urn: String,
        content_urn: String,
    },
}

/// What a node is in the picture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeRole {
    Input,
    Step,
    Value,
    Map,
    Gather,
    Output,
}

/// How a node stands in the run.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NodeState {
    /// Nothing to say: a plan not running, a stack.
    Idle,
    /// Not reached yet.
    Pending,
    /// Waiting for pool capacity.
    Queued,
    /// Running, with its own progress when it reported one.
    Active(Option<f32>),
    /// Done.
    Done,
    /// Failed for good.
    Failed,
    /// Stopped by a cancel — never painted as a failure.
    Cancelled,
    /// Skipped: a required arg of it was absent.
    Skipped,
    /// Completed after retrying.
    Retried,
    /// Failed, and the machine tolerates it.
    Tolerated,
    /// An output that did not fire.
    NotProduced,
}

impl NodeState {
    /// The mark in the card's corner, at spinner frame `frame`.
    pub fn mark(&self, frame: usize) -> Option<StatusMark> {
        match self {
            NodeState::Idle | NodeState::Pending | NodeState::NotProduced => None,
            NodeState::Queued => Some(StatusMark::Queued),
            NodeState::Active(_) => Some(StatusMark::Spinner(frame)),
            NodeState::Done => Some(StatusMark::Done),
            NodeState::Failed => Some(StatusMark::Failed),
            NodeState::Cancelled => Some(StatusMark::Cancelled),
            NodeState::Skipped => Some(StatusMark::Skipped),
            NodeState::Retried => Some(StatusMark::Retried),
            NodeState::Tolerated => Some(StatusMark::Tolerated),
        }
    }

    /// The tone the state draws its mark and border in.
    pub fn tone(&self) -> Option<Tone> {
        match self {
            NodeState::Failed => Some(Tone::Danger),
            NodeState::Done | NodeState::Retried => Some(Tone::Success),
            NodeState::Active(_) => Some(Tone::Accent),
            NodeState::Queued | NodeState::Tolerated | NodeState::Skipped => Some(Tone::Warning),
            NodeState::Cancelled => Some(Tone::Muted),
            NodeState::Idle | NodeState::Pending | NodeState::NotProduced => None,
        }
    }
}

/// An argument of a step drawn on its card.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Port {
    pub name: String,
    pub value: Option<String>,
    pub origin: ArgOrigin,
}

/// A badge over a node's corner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Badge {
    pub text: String,
    pub tone: Tone,
}

/// One node of the picture.
#[derive(Debug, Clone, PartialEq)]
pub struct SceneNode {
    /// The display id — the uikit's: `run-step-<plan node>`, …
    pub id: String,
    pub role: NodeRole,
    pub icon: Icon,
    /// The accent: blue in, green out, neutral between.
    pub tone: Tone,
    pub title: String,
    pub subtitle: String,
    /// What became of it, when that is worth a line.
    pub meta: Option<String>,
    /// A stack's item count; `None` for a card.
    pub stack: Option<usize>,
    pub ports: Vec<Port>,
    /// The token live state resolves by: a step's, or a map's.
    pub token: Option<String>,
    /// The plan node it draws.
    pub plan_node: String,
    pub state: NodeState,
    /// How many of a stack's sources are live devices.
    pub live_lanes: usize,
    /// Above its top-left corner: why it failed.
    pub failure: Option<Badge>,
    /// Above its top-right corner: its pools, `2/4 ⧖1`.
    pub pool: Option<Badge>,
    /// The full identity a hover reveals: the cap or media URN.
    pub full_urn: Option<String>,
}

/// How an edge's protocol flow reads (the uikit's `_classifyFlow`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowClass {
    /// Bytes are moving: slow, medium or fast.
    Flowing(RateBucket),
    /// A stream waits for credit.
    CreditWait,
    /// Streams are open and nothing has moved for a while.
    Stalled,
    Done,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateBucket {
    Slow,
    Medium,
    Fast,
}

/// What an edge carries, which decides its colour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeTone {
    /// An ordinary edge.
    Neutral,
    /// Into an output, or out of a map: the state of the items it carries.
    Items(ItemsState),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemsState {
    /// Some carried item failed.
    Failure,
    /// Every carried item succeeded.
    Success,
    /// Nothing to say yet.
    Open,
}

/// One edge of the picture.
#[derive(Debug, Clone, PartialEq)]
pub struct SceneEdge {
    pub id: String,
    pub from: String,
    pub to: String,
    /// The argument it feeds, the output it leaves by, "For each item".
    pub label: String,
    /// Out of a map, to what runs once per item.
    pub foreach: bool,
    pub tone: EdgeTone,
    /// The protocol's flow on it, when its source step is flowing.
    pub flow: Option<FlowClass>,
    /// What the flow reads: `1.2 MB · 340 KB/s ×2`.
    pub chip: Option<String>,
}

/// A whole picture.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Scene {
    pub nodes: Vec<SceneNode>,
    pub edges: Vec<SceneEdge>,
    /// Nodes that sit in the last column whatever their depth: the outputs.
    pub align_last: BTreeSet<String>,
}

impl Scene {
    pub fn node(&self, id: &str) -> Option<&SceneNode> {
        self.nodes.iter().find(|node| node.id == id)
    }

    /// The node a step's or a map's token resolves to.
    pub fn node_of_token(&self, token: &str) -> Option<&SceneNode> {
        self.nodes
            .iter()
            .find(|node| node.token.as_deref() == Some(token))
    }
}

/// What a run gives the scene beyond its plan.
pub struct RunContext<'a> {
    pub live: &'a RunLive,
    /// What each input was given, by input plan node id.
    pub sources: &'a BTreeMap<String, Vec<Source>>,
}

/// A step's cap, shortly: the tags that name it, without the media it moves
/// between (the uikit's `capShortLabel`), read from the parsed URN.
pub fn cap_short_label(cap_urn: &str) -> Result<String, String> {
    let urn = CapUrn::from_string(cap_urn).map_err(|e| e.to_string())?;
    let named = tagged_urn::TaggedUrn::new("cap".to_string(), urn.tags().clone()).tags_to_string();
    Ok(if named.is_empty() {
        urn.to_string()
    } else {
        named
    })
}

/// What a value is called: what the notation names it, else the name of the
/// input or output it is, else the title of its media.
fn name_of(node: &PlanViewNode) -> String {
    if let Some(label) = &node.label {
        return label.clone();
    }
    match &node.kind {
        PlanViewKind::Input { name, .. } | PlanViewKind::Output { name, .. } => name.clone(),
        _ => media_title(node),
    }
}

fn media_title(node: &PlanViewNode) -> String {
    node.media.as_ref().map(|m| m.title.clone()).unwrap_or_default()
}

fn media_icon(node: &PlanViewNode) -> Icon {
    Icon::Media(
        node.media
            .as_ref()
            .map(|m| MediaKind::of(&m.urn))
            .unwrap_or(MediaKind::Binary),
    )
}

fn kind_name(kind: &PlanViewKind) -> &'static str {
    match kind {
        PlanViewKind::Step(_) => "step",
        PlanViewKind::StepOutput { .. } => "step output",
        PlanViewKind::Map { .. } => "map",
        PlanViewKind::Item { .. } => "item",
        PlanViewKind::Gather { .. } => "gather",
        PlanViewKind::Input { .. } => "input",
        PlanViewKind::Output { .. } => "output",
    }
}

/// The plan's nodes in an order every edge goes forward in: the order the
/// uikit walks a plan in. Ties go by node id.
fn topological_order(plan: &PlanView) -> Result<Vec<&PlanViewNode>, SceneError> {
    let mut indegree: BTreeMap<&str, usize> = plan.nodes.iter().map(|n| (n.id.as_str(), 0)).collect();
    let mut out: HashMap<&str, Vec<&str>> = HashMap::new();
    for edge in &plan.edges {
        if !indegree.contains_key(edge.from_node.as_str()) {
            return Err(SceneError::MissingNode(edge.from_node.clone()));
        }
        let entry = indegree
            .get_mut(edge.to_node.as_str())
            .ok_or_else(|| SceneError::MissingNode(edge.to_node.clone()))?;
        *entry += 1;
        out.entry(edge.from_node.as_str()).or_default().push(edge.to_node.as_str());
    }
    let mut ready: BTreeSet<&str> = indegree
        .iter()
        .filter(|(_, d)| **d == 0)
        .map(|(id, _)| *id)
        .collect();
    let mut order = Vec::with_capacity(plan.nodes.len());
    while let Some(id) = ready.pop_first() {
        order.push(plan.node(id).ok_or_else(|| SceneError::MissingNode(id.to_string()))?);
        for next in out.get(id).cloned().unwrap_or_default() {
            let d = indegree.get_mut(next).expect("counted above");
            *d -= 1;
            if *d == 0 {
                ready.insert(next);
            }
        }
    }
    if order.len() != plan.nodes.len() {
        let stuck = indegree
            .iter()
            .find(|(_, d)| **d > 0)
            .map(|(id, _)| id.to_string())
            .unwrap_or_default();
        return Err(SceneError::Cycle(stuck));
    }
    Ok(order)
}

/// The state the items an edge carries are in.
fn items_state<'a>(states: impl Iterator<Item = Option<bool>> + 'a) -> ItemsState {
    let mut any = false;
    let mut all_done = true;
    for state in states {
        any = true;
        match state {
            Some(false) => return ItemsState::Failure,
            Some(true) => {}
            None => all_done = false,
        }
    }
    if any && all_done {
        ItemsState::Success
    } else {
        ItemsState::Open
    }
}

/// The attribution heading of a failure, as the uikit names it.
pub fn attribution_label(class: AttributionClass) -> &'static str {
    match class {
        AttributionClass::Input => "Input problem",
        AttributionClass::Resource => "Resource shortage",
        AttributionClass::Environment => "Environment failure",
        AttributionClass::Internal => "Internal error",
        AttributionClass::User => "Cancelled by user",
    }
}

/// Build the picture of `plan`. With `run`, the picture of that run of it:
/// its sources, its items, its outputs, its live state.
pub fn run_scene(
    plan: &PlanView,
    arguments: &HashMap<String, Vec<StepArgument>>,
    run: Option<&RunContext<'_>>,
) -> Result<Scene, SceneError> {
    let order = topological_order(plan)?;
    let node = |id: &str| -> Result<&PlanViewNode, SceneError> {
        plan.node(id).ok_or_else(|| SceneError::MissingNode(id.to_string()))
    };
    for token in arguments.keys() {
        let is_step = plan
            .nodes
            .iter()
            .any(|n| matches!(&n.kind, PlanViewKind::Step(step) if step.token_id.as_str() == token));
        if !is_step {
            return Err(SceneError::ArgumentsForNoStep(token.clone()));
        }
    }
    if let Some(run) = run {
        for input in run.sources.keys() {
            if !matches!(plan.node(input).map(|n| &n.kind), Some(PlanViewKind::Input { .. })) {
                return Err(SceneError::SourceForNoInput(input.clone()));
            }
        }
    }

    // ── who reads each value ──
    let mut readers: HashMap<&str, Vec<&str>> = HashMap::new();
    for edge in &plan.edges {
        if edge.kind == PlanViewEdgeKind::Iteration {
            continue;
        }
        readers.entry(edge.from_node.as_str()).or_default().push(edge.to_node.as_str());
    }
    // A value one step hands another gets a card of its own. One only the
    // machine's outputs read goes straight to them, and one a map runs over
    // IS the map's stack: a card before it would draw the same value twice.
    let has_card = |id: &str| -> bool {
        readers.get(id).is_some_and(|targets| {
            targets.iter().any(|target| {
                !matches!(
                    plan.node(target).map(|n| &n.kind),
                    Some(PlanViewKind::Output { .. })
                        | Some(PlanViewKind::StepOutput { .. })
                        | Some(PlanViewKind::Map { .. })
                )
            })
        })
    };
    let input_id = |id: &str| format!("run-input-{id}");
    let step_id = |id: &str| format!("run-step-{id}");
    let value_id = |id: &str| format!("run-value-{id}");
    let map_id = |id: &str| format!("run-map-{id}");
    let gather_id = |id: &str| format!("run-gather-{id}");
    let output_id = |id: &str| format!("run-output-{id}");

    // Where a reader of plan node `id` draws its edge from, and what it says.
    let drawn_source = |id: &str| -> Result<(String, String, bool), SceneError> {
        let source = node(id)?;
        Ok(match &source.kind {
            PlanViewKind::Input { .. } => (input_id(id), String::new(), false),
            PlanViewKind::Step(_) => {
                if has_card(id) {
                    (value_id(id), String::new(), false)
                } else {
                    (step_id(id), String::new(), false)
                }
            }
            PlanViewKind::StepOutput {
                step_node,
                output_name,
                ..
            } => {
                if has_card(id) {
                    (value_id(id), String::new(), false)
                } else {
                    (step_id(step_node), output_name.clone(), false)
                }
            }
            PlanViewKind::Item { map, source: item_source } => {
                let map_node = node(map)?;
                let single = matches!(&map_node.kind, PlanViewKind::Map { sources, .. } if sources.len() == 1);
                let label = if single {
                    "For each item".to_string()
                } else {
                    format!("For each item of {}", name_of(node(item_source)?))
                };
                (map_id(map), label, true)
            }
            PlanViewKind::Gather { .. } => (gather_id(id), String::new(), false),
            other => {
                return Err(SceneError::NotReadable {
                    node: id.to_string(),
                    kind: kind_name(other),
                })
            }
        })
    };

    let live = run.map(|r| r.live);
    let failed_tokens: Vec<String> = live.map(|l| l.failed_tokens()).unwrap_or_default();
    let cancelled = matches!(live.map(|l| &l.status), Some(RunStatus::Cancelled(_)));

    let mut nodes: Vec<SceneNode> = Vec::new();
    let mut align_last = BTreeSet::new();

    for plan_node in &order {
        let id = plan_node.id.as_str();
        match &plan_node.kind {
            PlanViewKind::Input { name, .. } => {
                let media = media_title(plan_node);
                let (subtitle, count, live_lanes) = match run {
                    None => (format!("{media} · input"), None, 0),
                    Some(run) => {
                        let sources = run
                            .sources
                            .get(id)
                            .filter(|s| !s.is_empty())
                            .ok_or_else(|| SceneError::InputWithoutSource(name.clone()))?;
                        let lanes = sources.iter().filter(|s| matches!(s, Source::Live { .. })).count();
                        let values = sources.iter().filter(|s| matches!(s, Source::Value(_))).count();
                        let files = sources.len() - lanes - values;
                        let subtitle = if lanes == 0 && values > 0 && files == 0 {
                            format!("{media} · {}", text::count(values, "value"))
                        } else if lanes == 0 && values > 0 {
                            format!("{media} · {} · {}", text::count(files, "file"), text::count(values, "value"))
                        } else if lanes == 0 {
                            format!("{media} · {}", text::count(files, "file"))
                        } else if files == 0 {
                            format!("{media} · {}", text::count(lanes, "live feed"))
                        } else {
                            format!(
                                "{media} · {} · {}",
                                text::count(files, "file"),
                                text::count(lanes, "live feed")
                            )
                        };
                        (subtitle, Some(sources.len()), lanes)
                    }
                };
                nodes.push(SceneNode {
                    id: input_id(id),
                    role: NodeRole::Input,
                    icon: media_icon(plan_node),
                    tone: Tone::Source,
                    title: plan_node.label.clone().unwrap_or_else(|| name.clone()),
                    subtitle,
                    meta: None,
                    stack: count,
                    ports: Vec::new(),
                    token: None,
                    plan_node: id.to_string(),
                    state: NodeState::Idle,
                    live_lanes,
                    failure: None,
                    pool: None,
                    full_urn: plan_node.media.as_ref().map(|m| m.urn.to_string()),
                });
            }
            PlanViewKind::Step(step) => {
                let token = step.token_id.to_string();
                let short = cap_short_label(&step.cap_urn).map_err(|reason| SceneError::InvalidCapUrn {
                    node: id.to_string(),
                    urn: step.cap_urn.clone(),
                    reason,
                })?;
                let mut subtitle = vec![short];
                if step.retry > 0 {
                    subtitle.push(format!("retry {}", step.retry));
                }
                if step.on_error_absent {
                    subtitle.push("tolerates failure".to_string());
                }
                let in_map = plan_node.enclosing_map.is_some();
                let mut meta: Vec<String> = Vec::new();
                let mut state = NodeState::Idle;
                let mut failure = None;
                let mut pool = None;
                if let Some(live) = live {
                    let step_live = live
                        .steps
                        .get(&token)
                        .expect("RunLive holds every step of the plan it was made for");
                    let (tolerated, skipped, retried) =
                        (step_live.tolerated(), step_live.skipped(), step_live.retried());
                    let runs = |n: usize| -> String { text::count(n, "run") };
                    if tolerated > 0 {
                        meta.push(if in_map {
                            format!("{} failed — tolerated", runs(tolerated))
                        } else {
                            "failed — tolerated".to_string()
                        });
                    }
                    if skipped > 0 {
                        meta.push(if in_map {
                            format!("{} skipped", runs(skipped))
                        } else {
                            "skipped".to_string()
                        });
                    }
                    if retried > 0 {
                        meta.push(if in_map {
                            format!("{} retried", runs(retried))
                        } else {
                            "completed after retrying".to_string()
                        });
                    }
                    state = step_state(live, &token, plan_node, &failed_tokens, cancelled);
                    if state == NodeState::Done || state == NodeState::Idle || state == NodeState::Pending {
                        if tolerated > 0 {
                            state = NodeState::Tolerated;
                        } else if skipped > 0 {
                            state = NodeState::Skipped;
                        } else if retried > 0 {
                            state = NodeState::Retried;
                        }
                    }
                    failure = failure_badge(live, &token);
                    pool = live.pools.get(&token).map(|chain| pool_badge(chain));
                }
                let ports = arguments
                    .get(&token)
                    .map(|args| {
                        args.iter()
                            .filter(|arg| !arg.origin.is_wired())
                            .map(|arg| Port {
                                name: arg.name.clone(),
                                value: arg.value_text.clone(),
                                origin: arg.origin.clone(),
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                nodes.push(SceneNode {
                    id: step_id(id),
                    role: NodeRole::Step,
                    icon: Icon::Operation,
                    tone: Tone::Secondary,
                    title: step.title.clone(),
                    subtitle: subtitle.join(" · "),
                    meta: (!meta.is_empty()).then(|| meta.join(" · ")),
                    stack: None,
                    ports,
                    token: Some(token),
                    plan_node: id.to_string(),
                    state,
                    live_lanes: 0,
                    failure,
                    pool,
                    full_urn: Some(step.cap_urn.clone()),
                });
                if has_card(id) {
                    nodes.push(value_card(value_id(id), plan_node, live)?);
                }
            }
            PlanViewKind::StepOutput { .. } => {
                if has_card(id) {
                    nodes.push(value_card(value_id(id), plan_node, live)?);
                }
            }
            PlanViewKind::Map { token_id, sources, .. } => {
                let title = sources
                    .iter()
                    .map(|source| node(&source.node).map(name_of))
                    .collect::<Result<Vec<_>, _>>()?
                    .join(" × ");
                let zip = sources.len() > 1;
                let token = token_id.to_string();
                let items: &[ItemLive] = live
                    .map(|l| l.maps.get(&token).map(|m| m.items.as_slice()).unwrap_or(&[]))
                    .unwrap_or(&[]);
                let subtitle = match (live, items.len()) {
                    (None, _) => {
                        if zip {
                            "Runs once per item of each".to_string()
                        } else {
                            "Each item is processed individually".to_string()
                        }
                    }
                    (Some(_), 0) => "No items are available.".to_string(),
                    (Some(_), n) if zip => format!("{}, each on one item of each", text::count(n, "run")),
                    (Some(_), 1) => "1 item is processed individually".to_string(),
                    (Some(_), n) => format!("{n} items are processed individually"),
                };
                let state = match live {
                    None => NodeState::Idle,
                    Some(l) => match l.maps.get(&token).and_then(|m| m.progress) {
                        Some(p) if p >= 0.999 => NodeState::Done,
                        Some(p) => NodeState::Active(Some(p)),
                        None => NodeState::Idle,
                    },
                };
                nodes.push(SceneNode {
                    id: map_id(id),
                    role: NodeRole::Map,
                    icon: Icon::Map,
                    tone: Tone::Muted,
                    title,
                    subtitle,
                    meta: None,
                    stack: live.map(|_| items.len()),
                    ports: Vec::new(),
                    token: Some(token),
                    plan_node: id.to_string(),
                    state,
                    live_lanes: 0,
                    failure: None,
                    pool: None,
                    full_urn: None,
                });
            }
            PlanViewKind::Gather { members } => {
                nodes.push(SceneNode {
                    id: gather_id(id),
                    role: NodeRole::Gather,
                    icon: media_icon(plan_node),
                    tone: Tone::Muted,
                    title: plan_node.label.clone().unwrap_or_else(|| media_title(plan_node)),
                    subtitle: format!("Gathered from {}", members.len()),
                    meta: None,
                    stack: None,
                    ports: Vec::new(),
                    token: None,
                    plan_node: id.to_string(),
                    state: NodeState::Idle,
                    live_lanes: 0,
                    failure: None,
                    pool: None,
                    full_urn: plan_node.media.as_ref().map(|m| m.urn.to_string()),
                });
            }
            PlanViewKind::Output { name, .. } => {
                let media = media_title(plan_node);
                let (subtitle, count, state) = match live {
                    None => (format!("{media} · output"), None, NodeState::Idle),
                    Some(live) => match live.outputs.get(id) {
                        Some(OutputEnd::NotProduced) => (
                            format!("{media} · not produced"),
                            Some(0),
                            NodeState::NotProduced,
                        ),
                        Some(OutputEnd::Produced { paths, .. }) => (
                            format!("{media} · {}", text::count(paths.len(), "file")),
                            Some(paths.len()),
                            NodeState::Done,
                        ),
                        Some(OutputEnd::Stdout { bytes }) => (
                            format!("{media} · {} to stdout", text::bytes(*bytes)),
                            Some(1),
                            NodeState::Done,
                        ),
                        // An empty output reads as the run stands: it has
                        // nothing YET while the run can still produce it.
                        None => match &live.status {
                            RunStatus::Pending | RunStatus::Running => {
                                (format!("{media} · nothing yet"), Some(0), NodeState::Pending)
                            }
                            RunStatus::Failed(_) | RunStatus::Cancelled(_) => {
                                (format!("{media} · nothing produced"), Some(0), NodeState::Idle)
                            }
                            RunStatus::Completed(_) => {
                                (format!("{media} · no files"), Some(0), NodeState::Idle)
                            }
                        },
                    },
                };
                align_last.insert(output_id(id));
                nodes.push(SceneNode {
                    id: output_id(id),
                    role: NodeRole::Output,
                    icon: media_icon(plan_node),
                    tone: Tone::Target,
                    title: name.clone(),
                    subtitle,
                    meta: None,
                    stack: count,
                    ports: Vec::new(),
                    token: None,
                    plan_node: id.to_string(),
                    state,
                    live_lanes: 0,
                    failure: None,
                    pool: None,
                    full_urn: plan_node.media.as_ref().map(|m| m.urn.to_string()),
                });
            }
            PlanViewKind::Item { .. } => {}
        }
    }
    if nodes.is_empty() {
        return Err(SceneError::Empty);
    }

    // ── the edges ──
    let mut edges: Vec<SceneEdge> = Vec::new();
    let mut main_edge_of_step: HashMap<String, String> = HashMap::new();
    let connect = |edges: &mut Vec<SceneEdge>, id: String, from: String, to: String, label: String, foreach: bool| {
        edges.push(SceneEdge {
            id,
            from,
            to,
            label,
            foreach,
            tone: EdgeTone::Neutral,
            flow: None,
            chip: None,
        });
    };
    for plan_node in &order {
        let id = plan_node.id.as_str();
        match &plan_node.kind {
            PlanViewKind::Step(_) if has_card(id) => {
                let edge_id = format!("run-edge-main-{id}");
                connect(&mut edges, edge_id.clone(), step_id(id), value_id(id), String::new(), false);
                main_edge_of_step.insert(id.to_string(), edge_id);
            }
            PlanViewKind::StepOutput {
                step_node,
                output_name,
                ..
            } if has_card(id) => {
                connect(
                    &mut edges,
                    format!("run-edge-output-{id}"),
                    step_id(step_node),
                    value_id(id),
                    output_name.clone(),
                    false,
                );
            }
            _ => {}
        }
    }
    for (index, edge) in plan.edges.iter().enumerate() {
        if edge.kind == PlanViewEdgeKind::Iteration {
            continue;
        }
        let target = node(&edge.to_node)?;
        let to = match &target.kind {
            PlanViewKind::StepOutput { step_node, .. } => {
                if step_node != &edge.from_node {
                    return Err(SceneError::OutputFedByOtherStep {
                        index,
                        output: edge.to_node.clone(),
                        step: step_node.clone(),
                        from: edge.from_node.clone(),
                    });
                }
                continue;
            }
            PlanViewKind::Item { .. } | PlanViewKind::Input { .. } => {
                return Err(SceneError::FeedsUnfeedable {
                    index,
                    node: edge.to_node.clone(),
                    kind: kind_name(&target.kind),
                })
            }
            PlanViewKind::Step(_) => step_id(&edge.to_node),
            PlanViewKind::Map { .. } => map_id(&edge.to_node),
            PlanViewKind::Gather { .. } => gather_id(&edge.to_node),
            PlanViewKind::Output { .. } => output_id(&edge.to_node),
        };
        let (from, source_label, foreach) = drawn_source(&edge.from_node)?;
        let label = match &edge.kind {
            PlanViewEdgeKind::Arg { arg_name, .. } => arg_name.clone(),
            _ => source_label,
        };
        let edge_id = format!("run-edge-{index}");
        if matches!(target.kind, PlanViewKind::Output { .. })
            && matches!(node(&edge.from_node)?.kind, PlanViewKind::Step(_))
            && !main_edge_of_step.contains_key(&edge.from_node)
        {
            main_edge_of_step.insert(edge.from_node.clone(), edge_id.clone());
        }
        connect(&mut edges, edge_id, from, to, label, foreach);
    }

    // Every output is fed by exactly one edge.
    for scene_node in nodes.iter().filter(|n| n.role == NodeRole::Output) {
        let count = edges.iter().filter(|e| e.to == scene_node.id).count();
        if count != 1 {
            return Err(SceneError::OutputFeeds {
                output: scene_node.plan_node.clone(),
                count,
            });
        }
    }

    // An edge's colour is the state of the items it carries: into an output,
    // that output's files; out of a map, the items its body ran on.
    if let Some(live) = live {
        for edge in &mut edges {
            let target = nodes.iter().find(|n| n.id == edge.to).expect("connected nodes exist");
            let source = nodes.iter().find(|n| n.id == edge.from).expect("connected nodes exist");
            if target.role == NodeRole::Output {
                edge.tone = EdgeTone::Items(match live.outputs.get(&target.plan_node) {
                    Some(OutputEnd::Produced { .. }) | Some(OutputEnd::Stdout { .. }) => ItemsState::Success,
                    Some(OutputEnd::NotProduced) => ItemsState::Open,
                    None => match &live.status {
                        RunStatus::Failed(_) => ItemsState::Failure,
                        _ => ItemsState::Open,
                    },
                });
            } else if source.role == NodeRole::Map {
                let token = source.token.as_deref().expect("a map's stack carries its token");
                let items = live.maps.get(token).map(|m| m.items.as_slice()).unwrap_or(&[]);
                edge.tone = EdgeTone::Items(items_state(
                    items.iter().map(|item| item.outcome.as_ref().map(|o| o.success)),
                ));
            }
        }

        // The protocol's flow is drawn on the edge a step's own work leaves
        // on: its main output, else the first edge out of it.
        for scene_node in nodes.iter().filter(|n| n.role == NodeRole::Step) {
            let token = scene_node.token.as_deref().expect("a step card carries its token");
            let Some(reading) = live.flow.get(token) else {
                continue;
            };
            let edge_id = main_edge_of_step
                .get(&scene_node.plan_node)
                .cloned()
                .or_else(|| edges.iter().find(|e| e.from == scene_node.id).map(|e| e.id.clone()));
            let Some(edge_id) = edge_id else { continue };
            let (class, chip) = classify_flow(reading);
            let edge = edges.iter_mut().find(|e| e.id == edge_id).expect("found above");
            edge.flow = Some(class);
            edge.chip = Some(chip);
        }
    }

    Ok(Scene {
        nodes,
        edges,
        align_last,
    })
}

/// A value one step hands another: a media card, or — when the run captured
/// it — the stack of what it held.
fn value_card(id: String, plan_node: &PlanViewNode, live: Option<&RunLive>) -> Result<SceneNode, SceneError> {
    let media = media_title(plan_node);
    let title = plan_node.label.clone().unwrap_or_else(|| match &plan_node.kind {
        PlanViewKind::StepOutput { output_name, .. } => output_name.clone(),
        _ => media.clone(),
    });
    let captured = live.and_then(|l| l.transients.get(&plan_node.id));
    let (subtitle, stack) = match captured {
        Some(artifact) => (
            format!(
                "{} · {}",
                text::count(artifact.item_count, "transient item"),
                text::bytes(artifact.byte_count)
            ),
            Some(artifact.item_count),
        ),
        None => (
            if title == media {
                text::truncate(&media, 36)
            } else {
                media.clone()
            },
            None,
        ),
    };
    Ok(SceneNode {
        id,
        role: NodeRole::Value,
        icon: media_icon(plan_node),
        tone: Tone::Muted,
        title,
        subtitle,
        meta: None,
        stack,
        ports: Vec::new(),
        token: None,
        plan_node: plan_node.id.clone(),
        state: NodeState::Idle,
        live_lanes: 0,
        failure: None,
        pool: None,
        full_urn: plan_node.media.as_ref().map(|m| m.urn.to_string()),
    })
}

/// Where a step stands. A step that reported its own progress says so
/// itself; a step inside a map gets no progress of its own, and stands as
/// the runs of its map's body that reached it stand.
fn step_state(
    live: &RunLive,
    token: &str,
    plan_node: &PlanViewNode,
    failed_tokens: &[String],
    cancelled: bool,
) -> NodeState {
    if failed_tokens.iter().any(|t| t == token) {
        return NodeState::Failed;
    }
    let step = &live.steps[token];
    if step.queued > 0 {
        return NodeState::Queued;
    }
    let own = match step.phase {
        StepPhase::Done => Some(NodeState::Done),
        StepPhase::Active => Some(NodeState::Active(step.progress)),
        StepPhase::Pending => None,
    };
    let state = match (own, &plan_node.enclosing_map) {
        (Some(state), _) => state,
        (None, None) => {
            if matches!(live.status, RunStatus::Completed(_)) {
                NodeState::Done
            } else {
                NodeState::Pending
            }
        }
        (None, Some(map_node)) => {
            // The runs of the map this step is in, and how they ended.
            let map = live
                .maps
                .iter()
                .find(|(map_token, _)| live.map_node(map_token) == Some(map_node.as_str()))
                .map(|(_, map)| map);
            match map {
                None => NodeState::Pending,
                Some(map) if map.items.is_empty() => NodeState::Pending,
                Some(map) => {
                    let ended = map.items.iter().filter(|i| i.outcome.is_some()).count();
                    if ended == map.items.len() && !live.status.is_live() {
                        NodeState::Done
                    } else if ended == map.items.len() && matches!(map.progress, Some(p) if p >= 0.999) {
                        NodeState::Done
                    } else {
                        NodeState::Active(None)
                    }
                }
            }
        }
    };
    match state {
        NodeState::Active(_) | NodeState::Pending | NodeState::Queued if cancelled => NodeState::Cancelled,
        other => other,
    }
}

/// Why a step failed, over its card: how many runs of a map failed at it,
/// or the run's own failure, with its attribution and code.
fn failure_badge(live: &RunLive, token: &str) -> Option<Badge> {
    let failed_bodies = live
        .maps
        .values()
        .flat_map(|map| map.items.iter())
        .filter_map(|item| item.outcome.as_ref())
        .filter(|o| !o.success && o.failed_token_id.as_deref() == Some(token))
        .count();
    let own = match &live.status {
        RunStatus::Failed(failure) if failure.step_token.as_deref() == Some(token) && !failure.collateral => {
            Some(failure)
        }
        _ => None,
    };
    if failed_bodies > 0 {
        let code = own
            .and_then(|f| f.code.clone())
            .map(|code| format!(" · {code}"))
            .unwrap_or_default();
        return Some(Badge {
            text: format!(
                "{} failed{code}",
                if failed_bodies == 1 {
                    "1 body".to_string()
                } else {
                    format!("{failed_bodies} bodies")
                }
            ),
            tone: Tone::Danger,
        });
    }
    own.map(|failure| Badge {
        text: match &failure.code {
            Some(code) => format!("{} · {code}", attribution_label(failure.class)),
            None => attribution_label(failure.class).to_string(),
        },
        tone: Tone::Danger,
    })
}

/// A step's pools as the meter chip reads: the tightest bounded pool of its
/// chain, `active/limit` and the queue behind it, or `∞` when nothing bounds
/// it.
fn pool_badge(chain: &crate::tui::live::PoolChain) -> Badge {
    let tightest = chain
        .pools
        .iter()
        .filter_map(|(_, state)| {
            let configured = if state.configured == 0 { None } else { Some(state.configured) };
            let effective = match (configured, state.available) {
                (Some(c), Some(a)) if a > 0 => Some(c.min(a)),
                (Some(c), _) => Some(c),
                (None, Some(a)) if a > 0 => Some(a),
                (None, _) => None,
            };
            effective.map(|limit| (limit, state))
        })
        .min_by_key(|(limit, state)| (*limit as i64 - state.active as i64, *limit));
    match tightest {
        None => Badge {
            text: "∞".to_string(),
            tone: Tone::Muted,
        },
        Some((limit, state)) => {
            let queue = if state.queued > 0 {
                format!(" ⧖{}", state.queued)
            } else {
                String::new()
            };
            let tone = if state.queued > 0 {
                Tone::Warning
            } else if state.active >= limit {
                Tone::Accent
            } else {
                Tone::Muted
            };
            Badge {
                text: format!("{}/{limit}{queue}", state.active),
                tone,
            }
        }
    }
}

/// How the uikit names the cause of a cancelled step's end: only a user's
/// cancel reads "cancelled"; a teardown because another step failed, or a
/// host's abort, reads "aborted".
fn cancel_chip(state: &crate::orchestrator::RunFlowState) -> String {
    match state.cancel_code.as_deref() {
        Some("ABORTED_COLLATERAL") => "aborted — another step failed".to_string(),
        Some("ABORTED") => "aborted by host".to_string(),
        _ => "cancelled".to_string(),
    }
}

/// One step's protocol flow, as the edge it leaves on draws it and as its
/// chip reads (the uikit's `_classifyFlow`). Thresholds come from the
/// protocol's own idle clock: credit-starved after 750ms idle, stalled after
/// 3s with streams open.
pub fn classify_flow(reading: &crate::tui::live::FlowReading) -> (FlowClass, String) {
    let state = &reading.state;
    let mut has_open_stream = false;
    let mut credit_blocked = false;
    let mut unbounded = false;
    for stream in &state.streams {
        if !stream.ended {
            has_open_stream = true;
            if stream.credit_outstanding <= 0 {
                credit_blocked = true;
            }
            if stream.unbounded {
                unbounded = true;
            }
        }
    }
    let total = reading.total_bytes();
    let multi = if state.active_requests > 1 {
        format!(" ×{}", state.active_requests)
    } else {
        String::new()
    };
    if state.active_requests == 0 {
        return match state.phase.as_str() {
            "err" => (FlowClass::Error, "failed".to_string()),
            "cancelled" => (FlowClass::Error, cancel_chip(state)),
            "master_died" => (FlowClass::Error, "host died".to_string()),
            _ => (
                FlowClass::Done,
                if total > 0 {
                    format!("✓ {}", text::bytes(total))
                } else {
                    "✓".to_string()
                },
            ),
        };
    }
    if credit_blocked && state.idle_ms >= 750 {
        return (FlowClass::CreditWait, format!("waiting for credit{multi}"));
    }
    if has_open_stream && state.idle_ms >= 3000 {
        return (
            FlowClass::Stalled,
            format!("no data {}s{multi}", (state.idle_ms as f64 / 1000.0).round() as u64),
        );
    }
    if state.phase == "created" && total == 0 {
        return (FlowClass::Flowing(RateBucket::Slow), format!("starting{multi}"));
    }
    let bps = reading.bytes_per_second;
    let bucket = if bps >= 1024.0 * 1024.0 {
        RateBucket::Fast
    } else if bps >= 8.0 * 1024.0 {
        RateBucket::Medium
    } else {
        RateBucket::Slow
    };
    let rate = if bps >= 512.0 {
        format!(" · {}/s", text::bytes(bps as u64))
    } else {
        String::new()
    };
    let inf = if unbounded { " ∞" } else { "" };
    (
        FlowClass::Flowing(bucket),
        format!("{}{rate}{multi}{inf}", text::bytes(total)),
    )
}

/// What resolving a machine says of its draft: what each node holds and
/// what each cap is called. Empty for a draft that does not resolve yet —
/// it is still drawn, from its text alone.
#[derive(Debug, Clone, Default)]
pub struct DraftInfo {
    /// By node name: the media it holds and that media's title.
    pub media_of: HashMap<String, (crate::MediaUrn, String)>,
    /// By cap URN as the header writes it: the cap's title.
    pub cap_titles: HashMap<String, String>,
}

/// The picture of a machine being edited: a card for every node the text
/// names — inputs blue, outputs green — and one for every step, with its
/// cap, its policy and its constants; an edge from each member a node feeds
/// and into each node a step makes, named for the arg or output it names.
pub fn machine_scene(draft: &crate::tui::edit::MachineDraft, info: &DraftInfo) -> Result<Scene, SceneError> {
    use crate::tui::edit::{DraftHeader, DraftValue};
    let inputs = draft.input_names();
    let outputs = draft.output_names();
    let mut nodes: Vec<SceneNode> = Vec::new();
    let mut align_last = BTreeSet::new();
    for name in draft.node_names() {
        let (role, tone, anchor) = if inputs.contains(&name) && outputs.contains(&name) {
            (NodeRole::Input, Tone::Source, "Input · Output")
        } else if inputs.contains(&name) {
            (NodeRole::Input, Tone::Source, "Input")
        } else if outputs.contains(&name) {
            (NodeRole::Output, Tone::Target, "Output")
        } else {
            (NodeRole::Value, Tone::Muted, "")
        };
        let (icon, subtitle, full_urn) = match info.media_of.get(&name) {
            Some((urn, title)) => (
                Icon::Media(MediaKind::of(urn)),
                if anchor.is_empty() { title.clone() } else { format!("{title} · {anchor}") },
                Some(urn.to_string()),
            ),
            None => (Icon::Media(MediaKind::Binary), anchor.to_string(), None),
        };
        let id = format!("node-{name}");
        if role == NodeRole::Output {
            align_last.insert(id.clone());
        }
        nodes.push(SceneNode {
            id,
            role,
            icon,
            tone,
            title: name.clone(),
            subtitle,
            meta: None,
            stack: None,
            ports: Vec::new(),
            token: None,
            plan_node: name,
            state: NodeState::Idle,
            live_lanes: 0,
            failure: None,
            pool: None,
            full_urn,
        });
    }
    let mut edges: Vec<SceneEdge> = Vec::new();
    for (index, wiring) in draft.wirings.iter().enumerate() {
        let header = draft.headers.iter().find(|h| h.alias() == wiring.cap_alias);
        let (title, subtitle, full_urn) = match header {
            Some(DraftHeader::Cap { alias, cap_urn }) => {
                let short = cap_short_label(cap_urn).unwrap_or_else(|_| alias.clone());
                (
                    info.cap_titles.get(cap_urn).cloned().unwrap_or_else(|| alias.clone()),
                    short,
                    Some(cap_urn.clone()),
                )
            }
            Some(DraftHeader::Machine { alias, reference }) => (alias.clone(), format!("machine {reference}"), None),
            None => (wiring.cap_alias.clone(), "no header names it".to_string(), None),
        };
        let mut subtitle = vec![subtitle];
        let policy = wiring.policy.to_notation();
        if !policy.is_empty() {
            subtitle.push(policy);
        }
        let constants: Vec<String> = wiring
            .sources
            .iter()
            .filter_map(|m| match (&m.name, &m.value) {
                (Some(name), DraftValue::Constant(value)) => Some(format!("{name}={value}")),
                (None, DraftValue::Constant(value)) => Some(value.to_string()),
                _ => None,
            })
            .collect();
        let id = format!("step-{index}");
        nodes.push(SceneNode {
            id: id.clone(),
            role: NodeRole::Step,
            icon: Icon::Operation,
            tone: Tone::Secondary,
            title,
            subtitle: subtitle.join(" · "),
            meta: (!constants.is_empty()).then(|| constants.join(" · ")),
            stack: None,
            ports: Vec::new(),
            token: None,
            plan_node: index.to_string(),
            state: if header.is_none() { NodeState::Failed } else { NodeState::Idle },
            live_lanes: 0,
            failure: None,
            pool: None,
            full_urn,
        });
        for (k, member) in wiring.sources.iter().enumerate() {
            if let DraftValue::Node(node) = &member.value {
                edges.push(SceneEdge {
                    id: format!("in-{index}-{k}"),
                    from: format!("node-{node}"),
                    to: id.clone(),
                    label: member.name.clone().unwrap_or_default(),
                    foreach: false,
                    tone: EdgeTone::Neutral,
                    flow: None,
                    chip: None,
                });
            }
        }
        for (k, target) in wiring.targets.iter().enumerate() {
            edges.push(SceneEdge {
                id: format!("out-{index}-{k}"),
                from: id.clone(),
                to: format!("node-{}", target.node),
                label: target.name.clone().unwrap_or_default(),
                foreach: false,
                tone: EdgeTone::Neutral,
                flow: None,
                chip: None,
            });
        }
    }
    if nodes.is_empty() {
        return Err(SceneError::Empty);
    }
    Ok(Scene {
        nodes,
        edges,
        align_last,
    })
}

/// The picture of each strand of a resolved machine as it is planned to run
/// on the inputs it declares, before anything runs: what `dag-viz --text`
/// prints and the planner previews.
pub async fn planned_scenes(
    resolved: &crate::orchestrator::ResolvedNotation,
    registry: &crate::FabricRegistry,
) -> Result<Vec<Scene>, String> {
    let mut scenes = Vec::with_capacity(resolved.strand_count());
    for strand in 0..resolved.strand_count() {
        let plan = resolved
            .plan(strand, &crate::orchestrator::RunInputs::declared())
            .await
            .map_err(|e| format!("strand {} cannot be planned: {e}", strand + 1))?;
        let view = crate::orchestrator::PlanView::of(registry, &plan.plan, &plan.node_names)
            .map_err(|e| format!("strand {} cannot be drawn: {e}", strand + 1))?;
        let scene = run_scene(&view, &HashMap::new(), None)
            .map_err(|e| format!("strand {} cannot be drawn: {e}", strand + 1))?;
        scenes.push(scene);
    }
    Ok(scenes)
}
