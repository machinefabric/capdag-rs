//! A run's plan as a client draws and follows it.
//!
//! A [`MachinePlan`] says what runs; a picture of it also needs what each
//! node holds by NAME — the title of the media it holds, the title of the cap
//! a step runs, the name of the arg an edge feeds, the constants a step was
//! given — and the innermost map each node runs in. [`PlanView`] is that
//! reading, taken once from the fabric registry's cache, so every surface
//! that draws a run (the desktop clients through the engine, the CLI's
//! terminal view) draws the same plan the same way.
//!
//! Nothing here guesses: a media type with no registered definition, a cap
//! the cache does not hold, a constant or an edge naming no arg of its cap —
//! each is a [`PlanViewError`], never a node drawn with a blank name.

use std::collections::{BTreeMap, HashMap};

use crate::cap::registry::FabricRegistry;
use crate::machine::OnError;
use crate::planner::{
    derive_output_media_urn, ArgumentBinding, EdgeType, ExecutionNodeType, ForEachSource,
    InputCardinality, MachinePlan, StepToken,
};
use crate::MediaUrn;

/// Why a plan cannot be drawn.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanViewError {
    /// The registry's cache holds no definition for a media type the plan
    /// holds. The registry must be warmed before a plan is drawn.
    #[error("No registered media def for '{0}'")]
    MissingMediaDef(String),
    /// The registry's cache holds no definition for a cap the plan runs.
    #[error("No cached cap for '{0}'")]
    MissingCap(String),
    /// The type a node holds cannot be derived from the plan.
    #[error("the type plan node '{node}' holds cannot be derived: {reason}")]
    UnderivableMedia { node: String, reason: String },
    /// A derived type is not a media URN.
    #[error("plan node '{node}' holds an invalid media URN '{urn}': {reason}")]
    InvalidMedia {
        node: String,
        urn: String,
        reason: String,
    },
    /// A step binds a constant to something that is no arg of its cap.
    #[error("step '{node}' binds a constant to '{arg_urn}', which is no arg of '{cap_urn}'")]
    ConstantNamesNoArg {
        node: String,
        arg_urn: String,
        cap_urn: String,
    },
    /// An arg edge ends at a node that is no step.
    #[error("an arg edge into '{0}', which is no step")]
    ArgEdgeIntoNonStep(String),
    /// An arg edge feeds something that is no arg of the step's cap.
    #[error("an edge into '{node}' feeds '{arg_urn}', which is no arg of '{cap_urn}'")]
    EdgeFeedsNoArg {
        node: String,
        arg_urn: String,
        cap_urn: String,
    },
}

/// A constant the machine binds to one arg of a step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanViewConstant {
    /// The URN the arg's value is delivered under.
    pub arg_urn: String,
    /// The arg's name in its cap definition.
    pub arg_name: String,
    /// The constant as JSON.
    pub value_json: String,
}

/// A step of the plan: one cap it runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanViewStep {
    pub token_id: StepToken,
    pub cap_urn: String,
    /// The cap's title in its definition.
    pub title: String,
    /// How many further attempts a retryable failure gets.
    pub retry: u32,
    /// Whether a failure that stands leaves the step's outputs absent.
    pub on_error_absent: bool,
    /// The constants the machine binds, ordered by arg name.
    pub constants: Vec<PlanViewConstant>,
}

/// What a node of the plan is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanViewKind {
    Step(PlanViewStep),
    /// An output of a step other than its main one.
    StepOutput {
        step_node: String,
        output_name: String,
        output_media_urn: String,
        carries_input: bool,
    },
    /// A map: its body runs once per item of its sources.
    Map {
        token_id: StepToken,
        sources: Vec<ForEachSource>,
        body: Vec<String>,
    },
    /// The item one run of a map's body is given of one source.
    Item { map: String, source: String },
    /// Several values gathered into one sequence, in order.
    Gather { members: Vec<String> },
    /// An input of the machine.
    Input { name: String, sequence: bool },
    /// An output of the machine.
    Output { name: String, source_node: String },
}

/// The media a node holds: its URN, and the title of its definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanViewMedia {
    pub urn: MediaUrn,
    pub title: String,
}

/// One node of the plan as it is drawn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanViewNode {
    pub id: String,
    /// The name the machine's notation gives the value, when it gives one.
    pub label: Option<String>,
    /// What the node holds. A map holds nothing of its own.
    pub media: Option<PlanViewMedia>,
    /// The innermost map the node runs in.
    pub enclosing_map: Option<String>,
    pub kind: PlanViewKind,
}

/// What an edge of the plan carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanViewEdgeKind {
    /// Into a step: its main input. Into anything else: what it is of.
    Direct,
    /// Into a non-main arg of a step.
    Arg { arg_urn: String, arg_name: String },
    /// From a map to one of its item nodes.
    Iteration,
    /// From a gathered node into its gather.
    Collection,
}

/// One edge of the plan as it is drawn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanViewEdge {
    pub from_node: String,
    pub to_node: String,
    pub kind: PlanViewEdgeKind,
}

/// A plan as a client draws and follows it: every node with what it holds
/// and its name, ordered by node id; every edge with what it carries, in the
/// plan's order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanView {
    pub name: String,
    pub nodes: Vec<PlanViewNode>,
    pub edges: Vec<PlanViewEdge>,
}

/// The media type every node of `plan` holds in a run, by node id. A map
/// holds nothing of its own and is not listed.
pub fn plan_media(plan: &MachinePlan) -> Result<BTreeMap<String, MediaUrn>, PlanViewError> {
    let mut media = BTreeMap::new();
    for (id, node) in &plan.nodes {
        if matches!(node.node_type, ExecutionNodeType::ForEach { .. }) {
            continue;
        }
        let urn = derive_output_media_urn(plan, id).map_err(|e| PlanViewError::UnderivableMedia {
            node: id.clone(),
            reason: e.to_string(),
        })?;
        let parsed = MediaUrn::from_string(&urn).map_err(|e| PlanViewError::InvalidMedia {
            node: id.clone(),
            urn: urn.clone(),
            reason: e.to_string(),
        })?;
        media.insert(id.clone(), parsed);
    }
    Ok(media)
}

impl PlanView {
    /// Read `plan` as it is drawn. `node_names` is what the notation calls
    /// each value ([`StrandPlan::node_names`](super::StrandPlan)); titles come
    /// from the registry's cache, which must hold every cap and media type the
    /// plan names.
    pub fn of(
        registry: &FabricRegistry,
        plan: &MachinePlan,
        node_names: &HashMap<String, String>,
    ) -> Result<Self, PlanViewError> {
        let media = plan_media(plan)?;
        // The innermost map each node runs in: a map's body lists only its
        // own members, never those of maps opened inside it.
        let mut enclosing: HashMap<&str, &str> = HashMap::new();
        for (id, node) in &plan.nodes {
            if let ExecutionNodeType::ForEach { body, .. } = &node.node_type {
                for member in body {
                    enclosing.insert(member.as_str(), id.as_str());
                }
            }
        }
        let title_of = |urn: &str| -> Result<String, PlanViewError> {
            registry
                .get_cached_media_def(urn)
                .map(|def| def.title)
                .ok_or_else(|| PlanViewError::MissingMediaDef(urn.to_string()))
        };
        let cap_of = |urn: &str| -> Result<crate::Cap, PlanViewError> {
            registry
                .get_cached_cap(urn)
                .ok_or_else(|| PlanViewError::MissingCap(urn.to_string()))
        };

        let mut ids: Vec<&String> = plan.nodes.keys().collect();
        ids.sort();
        let mut nodes = Vec::with_capacity(ids.len());
        for id in ids {
            let node = &plan.nodes[id];
            let held = match media.get(id) {
                Some(urn) => Some(PlanViewMedia {
                    title: title_of(&urn.to_string())?,
                    urn: urn.clone(),
                }),
                None => None,
            };
            let kind = match &node.node_type {
                ExecutionNodeType::Cap {
                    token_id,
                    cap_urn,
                    arg_bindings,
                    policy,
                    ..
                } => {
                    let cap = cap_of(cap_urn)?;
                    let mut constants = Vec::new();
                    for (arg_urn, binding) in &arg_bindings.bindings {
                        if let ArgumentBinding::Literal { value } = binding {
                            let arg = cap
                                .get_args()
                                .iter()
                                .find(|arg| arg.stream_urn() == arg_urn || &arg.media_urn == arg_urn)
                                .ok_or_else(|| PlanViewError::ConstantNamesNoArg {
                                    node: id.clone(),
                                    arg_urn: arg_urn.clone(),
                                    cap_urn: cap_urn.clone(),
                                })?;
                            constants.push(PlanViewConstant {
                                arg_urn: arg_urn.clone(),
                                arg_name: arg.name.clone(),
                                value_json: value.to_string(),
                            });
                        }
                    }
                    constants.sort_by(|a, b| a.arg_name.cmp(&b.arg_name));
                    PlanViewKind::Step(PlanViewStep {
                        token_id: token_id.clone(),
                        cap_urn: cap_urn.clone(),
                        title: cap.title.clone(),
                        retry: policy.retry,
                        on_error_absent: policy.on_error == OnError::Absent,
                        constants,
                    })
                }
                ExecutionNodeType::CapOutput {
                    cap_node,
                    output_media_urn,
                    output_name,
                    carries_input,
                } => PlanViewKind::StepOutput {
                    step_node: cap_node.clone(),
                    output_name: output_name.clone(),
                    output_media_urn: output_media_urn.clone(),
                    carries_input: *carries_input,
                },
                ExecutionNodeType::ForEach {
                    token_id,
                    sources,
                    body,
                } => PlanViewKind::Map {
                    token_id: token_id.clone(),
                    sources: sources.clone(),
                    body: body.clone(),
                },
                ExecutionNodeType::Item {
                    foreach, source, ..
                } => PlanViewKind::Item {
                    map: foreach.clone(),
                    source: source.clone(),
                },
                ExecutionNodeType::Collect { input_nodes, .. } => PlanViewKind::Gather {
                    members: input_nodes.clone(),
                },
                ExecutionNodeType::InputSlot {
                    slot_name,
                    cardinality,
                    ..
                } => PlanViewKind::Input {
                    name: slot_name.clone(),
                    sequence: !matches!(cardinality, InputCardinality::Single),
                },
                ExecutionNodeType::Output {
                    output_name,
                    source_node,
                } => PlanViewKind::Output {
                    name: output_name.clone(),
                    source_node: source_node.clone(),
                },
            };
            nodes.push(PlanViewNode {
                id: id.clone(),
                label: node_names.get(id).cloned(),
                media: held,
                enclosing_map: enclosing.get(id.as_str()).map(|map| map.to_string()),
                kind,
            });
        }

        let mut edges = Vec::with_capacity(plan.edges.len());
        for edge in &plan.edges {
            let kind = match &edge.edge_type {
                EdgeType::Direct => PlanViewEdgeKind::Direct,
                EdgeType::Iteration => PlanViewEdgeKind::Iteration,
                EdgeType::Collection => PlanViewEdgeKind::Collection,
                EdgeType::Arg { arg_urn } => {
                    let cap_urn = plan
                        .get_node(&edge.to_node)
                        .and_then(|node| node.cap_urn())
                        .ok_or_else(|| PlanViewError::ArgEdgeIntoNonStep(edge.to_node.clone()))?;
                    let cap = cap_of(cap_urn)?;
                    let arg = cap
                        .get_args()
                        .iter()
                        .find(|arg| arg.stream_urn() == arg_urn)
                        .ok_or_else(|| PlanViewError::EdgeFeedsNoArg {
                            node: edge.to_node.clone(),
                            arg_urn: arg_urn.clone(),
                            cap_urn: cap_urn.to_string(),
                        })?;
                    PlanViewEdgeKind::Arg {
                        arg_urn: arg_urn.clone(),
                        arg_name: arg.name.clone(),
                    }
                }
            };
            edges.push(PlanViewEdge {
                from_node: edge.from_node.clone(),
                to_node: edge.to_node.clone(),
                kind,
            });
        }

        Ok(Self {
            name: plan.name.clone(),
            nodes,
            edges,
        })
    }

    /// The node with id `id`.
    pub fn node(&self, id: &str) -> Option<&PlanViewNode> {
        self.nodes
            .binary_search_by(|node| node.id.as_str().cmp(id))
            .ok()
            .map(|index| &self.nodes[index])
    }
}
