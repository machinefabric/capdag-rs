//! What is drawn: the nodes and edges of a machine, built as the uikit's
//! scene builders build them, so the terminal picture has the same parts as
//! the desktop's.
//!
//! A planned machine is drawn from its plan ([`plan_scene`]). Each input is a
//! card; each step is a card; a value one step hands another gets a card of
//! its own; a map is a card for the items its body runs on, and the edges out
//! of it say "For each item"; a gather is a card; each output is a card.
//! Edge labels name the argument an edge feeds, or the output of the step it
//! leaves. What the scene holds is decided by the plan alone, and a plan that
//! contradicts what a picture of it needs is refused, never drawn around.
//!
//! A machine being edited is drawn from its draft ([`machine_scene`]): a card
//! for every node the text names and one for every step.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::orchestrator::{PlanView, PlanViewEdgeKind, PlanViewKind, PlanViewNode};
use crate::tui::text;
use crate::tui::theme::{GlyphSet, Icon, MediaKind, Tone};

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
    #[error("output '{output}' of the plan is fed by {count} edges; an output takes exactly one")]
    OutputFeeds { output: String, count: usize },
    #[error("the plan has a cycle through '{0}'")]
    Cycle(String),
    #[error("the plan draws nothing")]
    Empty,
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

/// Whether what a node draws holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    /// Nothing to say.
    Idle,
    /// What it draws cannot be: a step of a draft that no header names.
    Failed,
}

impl NodeState {
    /// The mark in the card's corner.
    pub fn mark(&self, glyphs: GlyphSet) -> Option<char> {
        match self {
            NodeState::Idle => None,
            NodeState::Failed => Some(glyphs.failed()),
        }
    }

    /// The tone the state draws its mark and border in.
    pub fn tone(&self) -> Option<Tone> {
        match self {
            NodeState::Idle => None,
            NodeState::Failed => Some(Tone::Danger),
        }
    }
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
    /// A line under the subtitle, when there is more to say: a step's
    /// constants.
    pub meta: Option<String>,
    /// The plan node it draws.
    pub plan_node: String,
    pub state: NodeState,
}

/// One edge of the picture.
#[derive(Debug, Clone, PartialEq)]
pub struct SceneEdge {
    pub id: String,
    pub from: String,
    pub to: String,
    /// The argument it feeds, the output it leaves by, "For each item".
    pub label: String,
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

/// Build the picture of `plan`. A step's card names its cap as `cap_name`
/// names a cap URN: by its alias.
pub fn plan_scene(plan: &PlanView, cap_name: &dyn Fn(&str) -> String) -> Result<Scene, SceneError> {
    let order = topological_order(plan)?;
    let node = |id: &str| -> Result<&PlanViewNode, SceneError> {
        plan.node(id).ok_or_else(|| SceneError::MissingNode(id.to_string()))
    };

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
    // IS the map's card: a card before it would draw the same value twice.
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
    let drawn_source = |id: &str| -> Result<(String, String), SceneError> {
        let source = node(id)?;
        Ok(match &source.kind {
            PlanViewKind::Input { .. } => (input_id(id), String::new()),
            PlanViewKind::Step(_) => {
                if has_card(id) {
                    (value_id(id), String::new())
                } else {
                    (step_id(id), String::new())
                }
            }
            PlanViewKind::StepOutput {
                step_node,
                output_name,
                ..
            } => {
                if has_card(id) {
                    (value_id(id), String::new())
                } else {
                    (step_id(step_node), output_name.clone())
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
                (map_id(map), label)
            }
            PlanViewKind::Gather { .. } => (gather_id(id), String::new()),
            other => {
                return Err(SceneError::NotReadable {
                    node: id.to_string(),
                    kind: kind_name(other),
                })
            }
        })
    };

    let mut nodes: Vec<SceneNode> = Vec::new();
    let mut align_last = BTreeSet::new();

    for plan_node in &order {
        let id = plan_node.id.as_str();
        match &plan_node.kind {
            PlanViewKind::Input { name, .. } => {
                nodes.push(SceneNode {
                    id: input_id(id),
                    role: NodeRole::Input,
                    icon: media_icon(plan_node),
                    tone: Tone::Source,
                    title: plan_node.label.clone().unwrap_or_else(|| name.clone()),
                    subtitle: format!("{} · input", media_title(plan_node)),
                    meta: None,
                    plan_node: id.to_string(),
                    state: NodeState::Idle,
                });
            }
            PlanViewKind::Step(step) => {
                let mut subtitle = vec![cap_name(&step.cap_urn)];
                if step.retry > 0 {
                    subtitle.push(format!("retry {}", step.retry));
                }
                if step.on_error_absent {
                    subtitle.push("tolerates failure".to_string());
                }
                nodes.push(SceneNode {
                    id: step_id(id),
                    role: NodeRole::Step,
                    icon: Icon::Operation,
                    tone: Tone::Secondary,
                    title: step.title.clone(),
                    subtitle: subtitle.join(" · "),
                    meta: None,
                    plan_node: id.to_string(),
                    state: NodeState::Idle,
                });
                if has_card(id) {
                    nodes.push(value_card(value_id(id), plan_node));
                }
            }
            PlanViewKind::StepOutput { .. } => {
                if has_card(id) {
                    nodes.push(value_card(value_id(id), plan_node));
                }
            }
            PlanViewKind::Map { sources, .. } => {
                let title = sources
                    .iter()
                    .map(|source| node(&source.node).map(name_of))
                    .collect::<Result<Vec<_>, _>>()?
                    .join(" × ");
                let subtitle = if sources.len() > 1 {
                    "Runs once per item of each"
                } else {
                    "Each item is processed individually"
                };
                nodes.push(SceneNode {
                    id: map_id(id),
                    role: NodeRole::Map,
                    icon: Icon::Map,
                    tone: Tone::Muted,
                    title,
                    subtitle: subtitle.to_string(),
                    meta: None,
                    plan_node: id.to_string(),
                    state: NodeState::Idle,
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
                    plan_node: id.to_string(),
                    state: NodeState::Idle,
                });
            }
            PlanViewKind::Output { name, .. } => {
                align_last.insert(output_id(id));
                nodes.push(SceneNode {
                    id: output_id(id),
                    role: NodeRole::Output,
                    icon: media_icon(plan_node),
                    tone: Tone::Target,
                    title: name.clone(),
                    subtitle: format!("{} · output", media_title(plan_node)),
                    meta: None,
                    plan_node: id.to_string(),
                    state: NodeState::Idle,
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
    let connect = |edges: &mut Vec<SceneEdge>, id: String, from: String, to: String, label: String| {
        edges.push(SceneEdge { id, from, to, label });
    };
    for plan_node in &order {
        let id = plan_node.id.as_str();
        match &plan_node.kind {
            PlanViewKind::Step(_) if has_card(id) => {
                connect(&mut edges, format!("run-edge-main-{id}"), step_id(id), value_id(id), String::new());
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
        let (from, source_label) = drawn_source(&edge.from_node)?;
        let label = match &edge.kind {
            PlanViewEdgeKind::Arg { arg_name, .. } => arg_name.clone(),
            _ => source_label,
        };
        connect(&mut edges, format!("run-edge-{index}"), from, to, label);
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

    Ok(Scene {
        nodes,
        edges,
        align_last,
    })
}

/// A value one step hands another: a card of its media.
fn value_card(id: String, plan_node: &PlanViewNode) -> SceneNode {
    let media = media_title(plan_node);
    let title = plan_node.label.clone().unwrap_or_else(|| match &plan_node.kind {
        PlanViewKind::StepOutput { output_name, .. } => output_name.clone(),
        _ => media.clone(),
    });
    let subtitle = if title == media {
        text::truncate(&media, 36)
    } else {
        media
    };
    SceneNode {
        id,
        role: NodeRole::Value,
        icon: media_icon(plan_node),
        tone: Tone::Muted,
        title,
        subtitle,
        meta: None,
        plan_node: plan_node.id.clone(),
        state: NodeState::Idle,
    }
}

/// What resolving a machine says of its draft: what each node holds and
/// which cap each step runs. A draft that does not resolve yet knows no
/// node's media — it is still drawn, from its text — but still knows the
/// caps its steps name, each found on its own.
#[derive(Debug, Clone, Default)]
pub struct DraftInfo {
    /// By node name: the media it holds and that media's title.
    pub media_of: HashMap<String, (crate::MediaUrn, String)>,
    /// By the name a step runs it by, as the text writes it — a header's
    /// alias or the fabric's: the cap. A name neither a header nor the
    /// fabric gives is absent.
    pub caps: HashMap<String, DraftCap>,
}

/// A cap a step of a draft runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DraftCap {
    pub urn: String,
    pub title: String,
    /// How a person names it: its fabric alias, else the name the text gives it.
    pub alias: String,
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
        let (icon, subtitle) = match info.media_of.get(&name) {
            Some((urn, title)) => (
                Icon::Media(MediaKind::of(urn)),
                if anchor.is_empty() { title.clone() } else { format!("{title} · {anchor}") },
            ),
            None => (Icon::Media(MediaKind::Binary), anchor.to_string()),
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
            plan_node: name,
            state: NodeState::Idle,
        });
    }
    let mut edges: Vec<SceneEdge> = Vec::new();
    for (index, wiring) in draft.wirings.iter().enumerate() {
        let header = draft.headers.iter().find(|h| h.alias() == wiring.cap_alias);
        let (title, subtitle, known) = match (header, info.caps.get(&wiring.cap_alias)) {
            (Some(DraftHeader::Machine { alias, reference }), _) => (alias.clone(), format!("machine {reference}"), true),
            (_, Some(cap)) => (cap.title.clone(), cap.alias.clone(), true),
            (_, None) => (wiring.cap_alias.clone(), "no cap or machine by this name".to_string(), false),
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
            plan_node: index.to_string(),
            state: if known { NodeState::Idle } else { NodeState::Failed },
        });
        for (k, member) in wiring.sources.iter().enumerate() {
            if let DraftValue::Node(node) = &member.value {
                edges.push(SceneEdge {
                    id: format!("in-{index}-{k}"),
                    from: format!("node-{node}"),
                    to: id.clone(),
                    label: member.name.clone().unwrap_or_default(),
                });
            }
        }
        for (k, target) in wiring.targets.iter().enumerate() {
            edges.push(SceneEdge {
                id: format!("out-{index}-{k}"),
                from: id.clone(),
                to: format!("node-{}", target.node),
                label: target.name.clone().unwrap_or_default(),
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
/// on the inputs it declares, before anything runs: what `capdag graph`
/// prints.
pub async fn planned_scenes(
    resolved: &crate::orchestrator::ResolvedNotation,
    registry: &crate::FabricRegistry,
    cap_name: &dyn Fn(&str) -> String,
) -> Result<Vec<Scene>, String> {
    let mut scenes = Vec::with_capacity(resolved.strand_count());
    for strand in 0..resolved.strand_count() {
        let plan = resolved
            .plan(strand, &crate::orchestrator::RunInputs::declared())
            .await
            .map_err(|e| format!("strand {} cannot be planned: {e}", strand + 1))?;
        let view = crate::orchestrator::PlanView::of(registry, &plan.plan, &plan.node_names)
            .map_err(|e| format!("strand {} cannot be drawn: {e}", strand + 1))?;
        let scene = plan_scene(&view, cap_name).map_err(|e| format!("strand {} cannot be drawn: {e}", strand + 1))?;
        scenes.push(scene);
    }
    Ok(scenes)
}
