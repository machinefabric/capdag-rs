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
    /// A definition the plan names is in the registry and could not be had
    /// from it: its fetch was refused or got no answer, or it is not fetched
    /// yet. `error` is the registry's account of it.
    #[error("the definition of '{urn}' could not be had from the registry: {error}")]
    DefinitionUnavailable { urn: String, error: String },
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
        // Missing only when the registry holds no such definition; one that
        // could not be had is reported as what kept it.
        let unavailable = |urn: &str, error: crate::FabricRegistryError, absent: PlanViewError| match error {
            crate::FabricRegistryError::NotFound(_) => absent,
            error => PlanViewError::DefinitionUnavailable {
                urn: urn.to_string(),
                error: error.to_string(),
            },
        };
        let title_of = |urn: &str| -> Result<String, PlanViewError> {
            registry
                .get_cached_media_def(urn)
                .map(|def| def.title)
                .map_err(|e| unavailable(urn, e, PlanViewError::MissingMediaDef(urn.to_string())))
        };
        let cap_of = |urn: &str| -> Result<crate::Cap, PlanViewError> {
            registry
                .get_cached_cap(urn)
                .map_err(|e| unavailable(urn, e, PlanViewError::MissingCap(urn.to_string())))
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::fabric::registry::StoredMediaDef;
    use crate::orchestrator::machine_plan::tests::{cap, header, registry, step_of, with_arg};
    use crate::orchestrator::{ResolvedNotation, RunInputs, StrandPlan};
    use crate::Cap;

    const RENDER: &str = r#"cap:in="media:document;ext=pdf";render-page-image;out="media:ext=png;image""#;
    const TO_JPEG: &str = r#"cap:in="media:ext=png;image";convert-image;out="media:ext=jpeg;image""#;
    const A: &str = r#"cap:in="media:document;ext=pdf";a;out="media:enc=utf8""#;
    const B: &str = r#"cap:in="media:ext=md;plain-text";b;out="media:enc=utf8""#;
    const MERGE: &str = r#"cap:in="media:enc=utf8";merge;out="media:enc=utf8;merged""#;
    const OTHER: &str = "media:enc=utf8;other";
    const LABEL: &str = "media:enc=utf8;label";

    /// The media every plan below holds, each with a title of its own.
    const TITLES: &[(&str, &str)] = &[
        ("media:document;ext=pdf", "PDF document"),
        ("media:ext=png;image", "PNG image"),
        ("media:ext=jpeg;image", "JPEG image"),
        ("media:ext=md;plain-text", "Markdown"),
        ("media:enc=utf8", "Text"),
        ("media:enc=utf8;merged", "Merged text"),
        (LABEL, "Label"),
    ];

    fn titled(mut cap: Cap, title: &str) -> Cap {
        cap.title = title.to_string();
        cap
    }

    fn media_def(urn: &str, title: &str) -> StoredMediaDef {
        StoredMediaDef {
            dims: Vec::new(),
            version: 0,
            urn: urn.to_string(),
            media_type: "application/octet-stream".to_string(),
            title: title.to_string(),
            profile_uri: None,
            schema: None,
            description: None,
            documentation: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        }
    }

    /// A registry holding `caps` and every media definition of [`TITLES`]
    /// but those `leaving_out` names.
    fn fabric(caps: Vec<Cap>, leaving_out: &[&str]) -> Arc<FabricRegistry> {
        let reg = registry(caps);
        for (urn, title) in TITLES {
            if !leaving_out.contains(urn) {
                reg.insert_cached_media_def_for_test(media_def(urn, title));
            }
        }
        reg
    }

    async fn strand(notation: &str, reg: Arc<FabricRegistry>) -> StrandPlan {
        let resolved = ResolvedNotation::resolve(notation, reg)
            .await
            .unwrap_or_else(|e| panic!("{notation}: {e}"));
        assert_eq!(resolved.strand_count(), 1, "one connected strand");
        resolved
            .plan(0, &RunInputs::declared())
            .await
            .unwrap_or_else(|e| panic!("{notation} cannot be planned: {e}"))
    }

    fn render_then_convert() -> String {
        format!(
            "{}{}[pages -> render -> thumbnail][thumbnail -> to_jpeg -> picture]",
            header("render", RENDER),
            header("to_jpeg", TO_JPEG)
        )
    }

    fn render_caps() -> Vec<Cap> {
        vec![titled(cap(RENDER, true), "Render pages"), titled(cap(TO_JPEG, false), "Convert to JPEG")]
    }

    fn media_title(view: &PlanView, id: &str) -> Option<String> {
        view.node(id).unwrap_or_else(|| panic!("no node '{id}'")).media.as_ref().map(|m| m.title.clone())
    }

    // TEST22104: a plan is drawn with every node, ordered by id and found by
    // it, holding the title of its media and of its cap, the name the
    // notation gives it, and the innermost map it runs in; every edge is
    // drawn in the plan's order with what it carries.
    #[tokio::test]
    async fn test22104_a_plan_is_drawn_with_the_names_and_maps_of_its_nodes() {
        let reg = fabric(render_caps(), &[]);
        let planned = strand(&render_then_convert(), reg.clone()).await;
        let plan = &planned.plan;
        let view = PlanView::of(&reg, plan, &planned.node_names).expect("every cap and media is known");

        assert_eq!(view.name, plan.name);
        assert_eq!(view.nodes.len(), plan.nodes.len(), "every node is drawn");
        assert!(view.nodes.windows(2).all(|pair| pair[0].id < pair[1].id), "ordered by id");
        for id in plan.nodes.keys() {
            assert_eq!(view.node(id).map(|node| &node.id), Some(id), "'{id}' is found by its id");
        }
        assert!(view.node("no-such-node").is_none());

        let render = step_of(plan, RENDER);
        let to_jpeg = step_of(plan, TO_JPEG);
        match &view.node(&render).unwrap().kind {
            PlanViewKind::Step(step) => {
                assert_eq!(step.title, "Render pages");
                assert_eq!(step.cap_urn, plan.nodes[&render].cap_urn().unwrap());
                assert!(step.constants.is_empty());
            }
            other => panic!("render is drawn as {other:?}"),
        }
        match &view.node(&to_jpeg).unwrap().kind {
            PlanViewKind::Step(step) => assert_eq!(step.title, "Convert to JPEG"),
            other => panic!("to_jpeg is drawn as {other:?}"),
        }
        assert_eq!(media_title(&view, &render).as_deref(), Some("PNG image"));
        assert_eq!(media_title(&view, &to_jpeg).as_deref(), Some("JPEG image"));
        assert_eq!(view.node(&render).unwrap().label.as_deref(), Some("thumbnail"));
        assert_eq!(view.node(&to_jpeg).unwrap().label.as_deref(), Some("picture"));

        // The input, named as the notation names it and holding what render takes.
        let input = view
            .nodes
            .iter()
            .find(|node| matches!(node.kind, PlanViewKind::Input { .. }))
            .expect("the machine's input is drawn");
        assert_eq!(input.kind, PlanViewKind::Input { name: "pages".into(), sequence: false });
        assert_eq!(input.media.as_ref().map(|m| m.title.as_str()), Some("PDF document"));

        // The map: holds nothing, runs to_jpeg, and is what to_jpeg and its
        // item run in; render runs once, in no map.
        let map = view
            .nodes
            .iter()
            .find(|node| matches!(node.kind, PlanViewKind::Map { .. }))
            .expect("the pages are mapped over");
        assert!(map.media.is_none(), "a map holds nothing of its own");
        match &map.kind {
            PlanViewKind::Map { body, .. } => assert!(body.contains(&to_jpeg)),
            _ => unreachable!(),
        }
        assert_eq!(view.node(&to_jpeg).unwrap().enclosing_map.as_deref(), Some(map.id.as_str()));
        assert_eq!(view.node(&render).unwrap().enclosing_map, None);
        let item = view
            .nodes
            .iter()
            .find(|node| matches!(node.kind, PlanViewKind::Item { .. }))
            .expect("one run's item is drawn");
        assert_eq!(item.kind, PlanViewKind::Item { map: map.id.clone(), source: render.clone() });
        assert_eq!(item.media.as_ref().map(|m| m.title.as_str()), Some("PNG image"));

        // The output, of to_jpeg's value.
        let output = view
            .nodes
            .iter()
            .find(|node| matches!(node.kind, PlanViewKind::Output { .. }))
            .expect("the machine's output is drawn");
        assert_eq!(output.kind, PlanViewKind::Output { name: "picture".into(), source_node: to_jpeg.clone() });

        // Every edge, in the plan's order, carrying what the plan says.
        let drawn: Vec<(&str, &str)> =
            view.edges.iter().map(|edge| (edge.from_node.as_str(), edge.to_node.as_str())).collect();
        let planned_edges: Vec<(&str, &str)> =
            plan.edges.iter().map(|edge| (edge.from_node.as_str(), edge.to_node.as_str())).collect();
        assert_eq!(drawn, planned_edges);
        for (edge, drawn) in plan.edges.iter().zip(&view.edges) {
            let expected = match &edge.edge_type {
                EdgeType::Direct => PlanViewEdgeKind::Direct,
                EdgeType::Iteration => PlanViewEdgeKind::Iteration,
                EdgeType::Collection => PlanViewEdgeKind::Collection,
                EdgeType::Arg { .. } => panic!("this machine wires no arg but a main input"),
            };
            assert_eq!(drawn.kind, expected, "{} -> {}", edge.from_node, edge.to_node);
        }
        assert!(
            view.edges.iter().any(|edge| edge.kind == PlanViewEdgeKind::Iteration
                && edge.from_node == map.id
                && edge.to_node == item.id),
            "the map's edge to its item is an iteration"
        );
    }

    // TEST22105: an arg a step is fed by an edge and an arg it is given a
    // constant are drawn by the names their cap gives them.
    #[tokio::test]
    async fn test22105_args_and_constants_are_drawn_by_their_names_in_the_cap() {
        let merge = with_arg(
            with_arg(titled(cap(MERGE, false), "Merge"), "other", OTHER, Some("media:enc=utf8")),
            "label",
            LABEL,
            None,
        );
        let reg = fabric(vec![cap(A, false), cap(B, false), merge], &[]);
        let notation = format!(
            "{}{}{}[doc_a -> a -> x][doc_b -> b -> y][(x, other=y, label=\"weekly\") -> merge -> z]",
            header("a", A),
            header("b", B),
            header("merge", MERGE)
        );
        let planned = strand(&notation, reg.clone()).await;
        let view = PlanView::of(&reg, &planned.plan, &planned.node_names).expect("every cap and media is known");

        let merge = step_of(&planned.plan, MERGE);
        match &view.node(&merge).unwrap().kind {
            PlanViewKind::Step(step) => {
                assert_eq!(step.title, "Merge");
                assert_eq!(
                    step.constants,
                    [PlanViewConstant {
                        arg_urn: LABEL.to_string(),
                        arg_name: "label".to_string(),
                        value_json: "\"weekly\"".to_string(),
                    }]
                );
            }
            other => panic!("merge is drawn as {other:?}"),
        }
        let b = step_of(&planned.plan, B);
        let fed = view
            .edges
            .iter()
            .find(|edge| edge.to_node == merge && edge.from_node == b)
            .expect("b's value is wired into merge");
        assert_eq!(
            fed.kind,
            PlanViewEdgeKind::Arg { arg_urn: OTHER.to_string(), arg_name: "other".to_string() }
        );
        let a = step_of(&planned.plan, A);
        let main = view
            .edges
            .iter()
            .find(|edge| edge.to_node == merge && edge.from_node == a)
            .expect("a's value is merge's main input");
        assert_eq!(main.kind, PlanViewEdgeKind::Direct);
    }

    // TEST22106: a plan names nothing it cannot draw: a media type or a cap
    // the registry's cache does not hold is an error naming it, never a node
    // drawn with a blank title.
    #[tokio::test]
    async fn test22106_a_media_or_cap_the_cache_lacks_is_an_error_naming_it() {
        let planned = strand(&render_then_convert(), fabric(render_caps(), &[])).await;

        let without_jpeg = fabric(render_caps(), &["media:ext=jpeg;image"]);
        match PlanView::of(&without_jpeg, &planned.plan, &planned.node_names) {
            Err(PlanViewError::MissingMediaDef(urn)) => assert!(
                MediaUrn::from_string(&urn)
                    .unwrap()
                    .is_equivalent(&MediaUrn::from_string("media:ext=jpeg;image").unwrap())
                    .unwrap(),
                "the error names the media it lacks, not {urn}"
            ),
            other => panic!("a missing media definition is refused, not {other:?}"),
        }

        let without_convert = fabric(vec![titled(cap(RENDER, true), "Render pages")], &[]);
        match PlanView::of(&without_convert, &planned.plan, &planned.node_names) {
            Err(PlanViewError::MissingCap(urn)) => {
                let to_jpeg = step_of(&planned.plan, TO_JPEG);
                assert_eq!(urn, planned.plan.nodes[&to_jpeg].cap_urn().unwrap(), "the error names the cap it lacks");
            }
            other => panic!("a missing cap is refused, not {other:?}"),
        }
    }
}
