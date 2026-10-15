//! Notation → `MachinePlan`: the one front end from machine notation to what runs.
//!
//! It resolves machine notation into the **same** `MachinePlan` the engine
//! executes, through the one plan builder
//! (`MachinePlanBuilder::build_plan_from_machine_strand`) — no plan is
//! constructed here.
//!
//! Strands are independent connected components, so each becomes its own plan.
//! A strand may have no input (every step of it is started by constants, or
//! by nothing) and may have no output (it ends in steps nothing reads).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::cap::registry::FabricRegistry;
use crate::machine::{
    parse_machine_with_sources_async, Machine, MachineSource, NoMachineSource, NodeId,
    StrandNodeNames,
};
use crate::planner::{
    AnchorInput, ExecutionNodeType, InputCardinality, MachinePlan, MachinePlanBuilder,
};
use crate::MediaUrn;

use super::types::ParseOrchestrationError;

/// Translate a `MachineParseError` from the resolver into the orchestrator's
/// error type: an unknown cap and a cycle keep their own variants, so a caller
/// can tell them from notation that does not parse.
pub(crate) fn translate_machine_parse_error(
    err: crate::machine::MachineParseError,
) -> ParseOrchestrationError {
    use crate::machine::{MachineAbstractionError, MachineParseError};
    match err {
        MachineParseError::Resolution(MachineAbstractionError::UnknownCap { cap_urn }) => {
            ParseOrchestrationError::CapNotFound { cap_urn }
        }
        MachineParseError::Resolution(MachineAbstractionError::CyclicMachineStrand {
            strand_index,
        }) => ParseOrchestrationError::NotADag {
            cycle_nodes: vec![format!("strand {}", strand_index)],
        },
        other => ParseOrchestrationError::MachineSyntaxParseFailed(format!("{}", other)),
    }
}

/// One value a machine takes or gives: an input anchor or an output anchor,
/// under the name the notation gives it. A host addresses a machine's inputs
/// and outputs by these names; the name is a label of the text and plays no
/// part in what the machine is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineAnchor {
    /// The strand (connected component) it belongs to.
    pub strand: usize,
    /// The name the notation wrote for the node.
    pub name: String,
    /// The media the node holds.
    pub media_urn: MediaUrn,
    node: NodeId,
}

/// The plan of one strand for one run, with the names of what it takes and
/// gives.
///
/// It is what a run IS: the host is shown it, keys every argument value by its
/// steps' tokens, and hands it back to be run; a run keeps it, and running it
/// again runs it again. Before a plan that came back is run it is checked to be
/// what the machine resolves to now ([`StrandPlan::check_is_a_resolution_of`]).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StrandPlan {
    /// The strand of the machine this is the plan of. A machine's strands are
    /// independent, and a plan handed back is checked against its own.
    pub strand: usize,
    pub plan: MachinePlan,
    /// The written name of every plan node that holds a value of the
    /// machine, by the node's id: an input slot, a step (its main output),
    /// and another output of a step. What a report names a value by — a plan
    /// node's own id means nothing to who wrote the machine. A node inside
    /// an included machine has the name `alias#use/name`.
    pub node_names: HashMap<String, String>,
}

/// How a plan handed back to be run differs from the plan its machine resolves
/// to now. Said, never repaired: a plan that is not the machine's is not run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanMismatch {
    pub reason: String,
}

impl PlanMismatch {
    fn new(reason: impl Into<String>) -> Self {
        Self { reason: reason.into() }
    }
}

impl std::fmt::Display for PlanMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the plan is not what the machine resolves to now: {}", self.reason)
    }
}

impl std::error::Error for PlanMismatch {}

impl StrandPlan {
    /// The plan's `InputSlot` node for each input, by the input's name. The
    /// slot carries the name; this reads it.
    pub fn input_slots(&self) -> HashMap<String, String> {
        self.plan
            .nodes
            .iter()
            .filter_map(|(id, node)| match &node.node_type {
                ExecutionNodeType::InputSlot { slot_name, .. } => Some((slot_name.clone(), id.clone())),
                _ => None,
            })
            .collect()
    }

    /// The written name of each plan `Output` node, by the node's id. The
    /// node carries the name; this reads it.
    pub fn output_names(&self) -> HashMap<String, String> {
        self.plan
            .nodes
            .iter()
            .filter_map(|(id, node)| match &node.node_type {
                ExecutionNodeType::Output { output_name, .. } => Some((id.clone(), output_name.clone())),
                _ => None,
            })
            .collect()
    }

    /// Check that this plan — one a host was shown and handed back — is
    /// `fresh`, the plan the same machine and run inputs resolve to now, in
    /// every respect but the values of its tokens.
    ///
    /// Tokens are minted each time notation is parsed, so two resolutions of
    /// one machine never share them; nothing else about them may differ. The
    /// steps of the two are paired by the names the notation writes for them
    /// (every step that holds a value has one, and one text writes each once),
    /// a map by its node (named for the step that opens it, not by a token),
    /// and a step that holds no value — a sink — by everything else it is.
    /// With the pairs, this plan's tokens are renamed to `fresh`'s, and the two
    /// must then be equal field for field.
    ///
    /// The pairing exists for this check only. What runs is this plan, with
    /// its own tokens, so every argument value keyed by them reaches the step
    /// it was given for.
    pub fn check_is_a_resolution_of(&self, fresh: &StrandPlan) -> Result<(), PlanMismatch> {
        let (our_steps, our_maps) = tokens_of(&self.plan)?;
        let (fresh_steps, fresh_maps) = tokens_of(&fresh.plan)?;
        if our_steps.len() != fresh_steps.len() {
            return Err(PlanMismatch::new(format!(
                "it has {} steps, and the machine has {}",
                our_steps.len(),
                fresh_steps.len()
            )));
        }

        // Steps that hold a value: by the name the notation writes for them.
        let fresh_by_name: HashMap<&str, &str> = fresh_steps
            .iter()
            .filter_map(|step| fresh.node_names.get(*step).map(|name| (name.as_str(), *step)))
            .collect();
        let mut renamed: HashMap<String, String> = HashMap::new();
        let mut our_sinks = Vec::new();
        for step in &our_steps {
            match self.node_names.get(*step) {
                Some(name) => {
                    let paired = fresh_by_name.get(name.as_str()).ok_or_else(|| {
                        PlanMismatch::new(format!("its step '{name}' is no step of the machine"))
                    })?;
                    renamed.insert((*step).to_string(), (*paired).to_string());
                }
                None => our_sinks.push(*step),
            }
        }
        let fresh_sinks: Vec<&str> = fresh_steps
            .iter()
            .copied()
            .filter(|step| !fresh.node_names.contains_key(*step))
            .collect();

        // Maps: by their node, which the step that opens the map names.
        if our_maps.keys().collect::<Vec<_>>() != fresh_maps.keys().collect::<Vec<_>>() {
            return Err(PlanMismatch::new(format!(
                "its maps are {:?}, and the machine's are {:?}",
                our_maps.keys().collect::<Vec<_>>(),
                fresh_maps.keys().collect::<Vec<_>>()
            )));
        }
        for (map, token) in &our_maps {
            renamed.insert(token.clone(), fresh_maps[map].clone());
        }

        // Sinks: by everything they are, once what they read is renamed. Two
        // sinks alike in everything are interchangeable, and paired in turn.
        if our_sinks.len() != fresh_sinks.len() {
            return Err(PlanMismatch::new(format!(
                "it has {} steps that hold no value, and the machine has {}",
                our_sinks.len(),
                fresh_sinks.len()
            )));
        }
        let mut fresh_sink_keys: Vec<(String, &str)> = fresh_sinks
            .iter()
            .map(|sink| Ok((sink_key(&fresh.plan, sink, &HashMap::new())?, *sink)))
            .collect::<Result<_, PlanMismatch>>()?;
        for sink in our_sinks {
            let key = sink_key(&self.plan, sink, &renamed)?;
            let position = fresh_sink_keys.iter().position(|(fresh_key, _)| *fresh_key == key).ok_or_else(|| {
                PlanMismatch::new(format!("its step '{sink}', which holds no value, is no step of the machine"))
            })?;
            let (_, paired) = fresh_sink_keys.remove(position);
            renamed.insert(sink.to_string(), paired.to_string());
        }

        // One token each way.
        let mut images: HashSet<&str> = HashSet::new();
        for image in renamed.values() {
            if !images.insert(image.as_str()) {
                return Err(PlanMismatch::new(format!("two of its steps pair with the machine's '{image}'")));
            }
        }

        let ours = normalized(rename_tokens(plan_value(self)?, &renamed));
        let theirs = normalized(plan_value(fresh)?);
        match first_difference(&ours, &theirs, "") {
            None => Ok(()),
            Some(at) => Err(PlanMismatch::new(format!("they differ at {at}"))),
        }
    }
}

/// A plan's step tokens (a step's node is its token) and its maps' tokens, by
/// map node. A token held twice addresses two steps at once, and is refused.
fn tokens_of(plan: &MachinePlan) -> Result<(Vec<&str>, std::collections::BTreeMap<String, String>), PlanMismatch> {
    let mut steps = Vec::new();
    let mut maps = std::collections::BTreeMap::new();
    let mut seen: HashSet<String> = HashSet::new();
    for (id, node) in &plan.nodes {
        let token = match &node.node_type {
            ExecutionNodeType::Cap { token_id, .. } => {
                if token_id.as_str() != id {
                    return Err(PlanMismatch::new(format!(
                        "its step node '{id}' carries the token '{token_id}', which is not the node's"
                    )));
                }
                steps.push(id.as_str());
                token_id.to_string()
            }
            ExecutionNodeType::ForEach { token_id, .. } => {
                maps.insert(id.clone(), token_id.to_string());
                token_id.to_string()
            }
            _ => continue,
        };
        if !seen.insert(token.clone()) {
            return Err(PlanMismatch::new(format!("its token '{token}' addresses two of its nodes")));
        }
    }
    steps.sort_unstable();
    Ok((steps, maps))
}

/// What a step that holds no value is, apart from its token: its node with the
/// token left out, and every edge into it — with `renamed` applied, so the
/// steps it reads are named as the fresh plan names them.
fn sink_key(plan: &MachinePlan, sink: &str, renamed: &HashMap<String, String>) -> Result<String, PlanMismatch> {
    let node = plan
        .nodes
        .get(sink)
        .ok_or_else(|| PlanMismatch::new(format!("it has no node '{sink}'")))?;
    let mut value = serde_json::to_value(node)
        .map_err(|e| PlanMismatch::new(format!("its node '{sink}' cannot be read: {e}")))?;
    strip_own_token(&mut value, sink);
    let mut edges: Vec<String> = plan
        .edges
        .iter()
        .filter(|edge| edge.to_node == sink)
        .map(|edge| {
            serde_json::to_value(edge)
                .map(|edge| rename_tokens(edge, renamed))
                .map(|mut edge| {
                    strip_own_token(&mut edge, sink);
                    edge.to_string()
                })
                .map_err(|e| PlanMismatch::new(format!("an edge into '{sink}' cannot be read: {e}")))
        })
        .collect::<Result<_, _>>()?;
    edges.sort_unstable();
    Ok(format!("{}|{}", rename_tokens(value, renamed), edges.join("|")))
}

/// `value` with every string that holds `token` blanked where it does: what a
/// node is when its own identity is set aside.
fn strip_own_token(value: &mut serde_json::Value, token: &str) {
    match value {
        serde_json::Value::String(text) if text.contains(token) => *text = text.replace(token, ""),
        serde_json::Value::Array(items) => items.iter_mut().for_each(|item| strip_own_token(item, token)),
        serde_json::Value::Object(fields) => fields.values_mut().for_each(|field| strip_own_token(field, token)),
        _ => {}
    }
}

/// A plan as a value: the plan with its names.
fn plan_value(plan: &StrandPlan) -> Result<serde_json::Value, PlanMismatch> {
    serde_json::to_value(plan).map_err(|e| PlanMismatch::new(format!("it cannot be read: {e}")))
}

/// `value` with every token `renamed` names replaced by its pair, in keys and
/// in values alike — node ids, edges, bodies and names all hold them. Tokens
/// are UUIDs: one is never part of another, or of anything that is not one.
fn rename_tokens(value: serde_json::Value, renamed: &HashMap<String, String>) -> serde_json::Value {
    let rename = |text: &str| -> String {
        renamed
            .iter()
            .filter(|(old, _)| text.contains(old.as_str()))
            .fold(text.to_string(), |text, (old, new)| text.replace(old.as_str(), new))
    };
    match value {
        serde_json::Value::String(text) => serde_json::Value::String(rename(&text)),
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(|item| rename_tokens(item, renamed)).collect())
        }
        serde_json::Value::Object(fields) => serde_json::Value::Object(
            fields
                .into_iter()
                .map(|(key, field)| (rename(&key), rename_tokens(field, renamed)))
                .collect(),
        ),
        other => other,
    }
}

/// A plan value with the lists whose order means nothing put in one order:
/// the plan's edges, entry nodes and output nodes.
fn normalized(mut value: serde_json::Value) -> serde_json::Value {
    if let Some(plan) = value.get_mut("plan").and_then(|plan| plan.as_object_mut()) {
        for list in ["edges", "entry_nodes", "output_nodes"] {
            if let Some(serde_json::Value::Array(items)) = plan.get_mut(list) {
                items.sort_by_key(|item| item.to_string());
            }
        }
    }
    value
}

/// Where two plan values first differ, as a path, or `None` when they do not.
fn first_difference(a: &serde_json::Value, b: &serde_json::Value, at: &str) -> Option<String> {
    use serde_json::Value;
    match (a, b) {
        (Value::Object(a), Value::Object(b)) => {
            let keys: std::collections::BTreeSet<&String> = a.keys().chain(b.keys()).collect();
            keys.into_iter().find_map(|key| match (a.get(key), b.get(key)) {
                (Some(a), Some(b)) => first_difference(a, b, &format!("{at}/{key}")),
                (Some(_), None) => Some(format!("{at}/{key}, which the machine's plan does not have")),
                (None, _) => Some(format!("{at}/{key}, which it does not have")),
            })
        }
        (Value::Array(a), Value::Array(b)) if a.len() == b.len() => a
            .iter()
            .zip(b)
            .enumerate()
            .find_map(|(index, (a, b))| first_difference(a, b, &format!("{at}/{index}"))),
        (Value::Array(a), Value::Array(b)) => {
            Some(format!("{at}, which holds {} items and the machine's {}", a.len(), b.len()))
        }
        (a, b) if a == b => None,
        (a, b) => Some(format!("{at}: {a} where the machine's plan has {b}")),
    }
}

/// What a run binds to the inputs of a machine, by the names the notation
/// gives them.
#[derive(Debug, Clone, Default)]
pub struct RunInputs {
    /// The inputs bound to a sequence. Every other input is bound to one
    /// value.
    pub sequences: HashSet<String>,
    /// The type of what the run binds, for an input it knows more of than the
    /// machine declares (a file whose type is more specific than the input's).
    /// Every other input holds its declared type. A type that does not
    /// conform to the input's is refused.
    pub media: HashMap<String, MediaUrn>,
}

impl RunInputs {
    /// Every input one value of its declared type.
    pub fn declared() -> Self {
        Self::default()
    }
}

/// Machine notation resolved against a registry: what the machine takes and
/// gives, and — once a run has said which inputs it binds to a sequence —
/// the plan of each of its strands.
///
/// Strands are independent connected components, so each has its own plan. A
/// strand may take nothing (its steps are started by constants, or by
/// nothing) and may give nothing (it ends in steps nothing reads).
pub struct ResolvedNotation {
    machine: Machine,
    names: Vec<StrandNodeNames>,
    registry: Arc<FabricRegistry>,
}

impl ResolvedNotation {
    /// Resolve notation that includes no other machine: a machine header in
    /// it is refused, for want of anything to resolve its reference against.
    pub async fn resolve(
        notation: &str,
        registry: Arc<FabricRegistry>,
    ) -> Result<Self, ParseOrchestrationError> {
        Self::resolve_from(notation, registry, &NoMachineSource, None).await
    }

    /// Resolve notation whose machine headers name machines `source` holds.
    /// `id` is what the source knows this notation by, when it knows it.
    /// What is resolved is the inlined whole: the steps of every included
    /// machine are steps of this one.
    pub async fn resolve_from(
        notation: &str,
        registry: Arc<FabricRegistry>,
        source: &dyn MachineSource,
        id: Option<&str>,
    ) -> Result<Self, ParseOrchestrationError> {
        let (machine, names) = parse_machine_with_sources_async(notation, &registry, source, id)
            .await
            .map_err(translate_machine_parse_error)?;
        if machine.strands().is_empty() {
            return Err(ParseOrchestrationError::InvalidGraph {
                message: "notation resolved to zero strands (no capability steps)".to_string(),
            });
        }
        Ok(Self {
            machine,
            names,
            registry,
        })
    }

    /// A machine already resolved — a path the planner found, made a machine
    /// — under the names its canonical notation gives its nodes. Its steps
    /// keep the tokens they have, so what was addressed to them before it was
    /// a machine (an argument value for a step of the path) is addressed to
    /// them still.
    pub fn from_machine(
        machine: Machine,
        registry: Arc<FabricRegistry>,
    ) -> Result<Self, ParseOrchestrationError> {
        if machine.strands().is_empty() {
            return Err(ParseOrchestrationError::InvalidGraph {
                message: "the machine has zero strands (no capability steps)".to_string(),
            });
        }
        let names = machine.canonical_node_names();
        Ok(Self {
            machine,
            names,
            registry,
        })
    }

    /// The resolved machine: every included machine inlined.
    pub fn machine(&self) -> &Machine {
        &self.machine
    }

    /// How the machine is drawn: its render payload, each node carrying the
    /// name the notation gave it (see `Machine::to_render_payload_json_with_names`).
    pub fn render_payload_json(&self) -> Result<String, ParseOrchestrationError> {
        self.machine
            .to_render_payload_json_with_names(&self.registry, &self.names)
            .map_err(|error| ParseOrchestrationError::InvalidGraph {
                message: format!("the machine's render payload cannot be built: {error}"),
            })
    }

    /// How many strands the machine has.
    pub fn strand_count(&self) -> usize {
        self.machine.strands().len()
    }

    /// The name the notation wrote for node `node` of strand `strand`.
    fn name_of(&self, strand: usize, node: NodeId) -> Result<String, ParseOrchestrationError> {
        self.names
            .get(strand)
            .and_then(|names| names.iter().find(|(_, id)| **id == node))
            .map(|(name, _)| name.clone())
            .ok_or_else(|| ParseOrchestrationError::InvalidGraph {
                message: format!("strand {strand}: node {node} has no name in the notation"),
            })
    }

    fn anchors(
        &self,
        of: impl Fn(&crate::machine::MachineStrand) -> &[NodeId],
    ) -> Result<Vec<MachineAnchor>, ParseOrchestrationError> {
        let mut anchors = Vec::new();
        for (strand, resolved) in self.machine.strands().iter().enumerate() {
            let mut of_strand = Vec::new();
            for &node in of(resolved) {
                of_strand.push(MachineAnchor {
                    strand,
                    name: self.name_of(strand, node)?,
                    media_urn: resolved.node_urn(node).clone(),
                    node,
                });
            }
            of_strand.sort_by(|a, b| a.name.cmp(&b.name));
            anchors.extend(of_strand);
        }
        Ok(anchors)
    }

    /// Everything the machine takes: strand by strand, by name within one.
    pub fn inputs(&self) -> Result<Vec<MachineAnchor>, ParseOrchestrationError> {
        self.anchors(|strand| strand.input_anchor_ids())
    }

    /// Everything the machine gives: strand by strand, by name within one.
    pub fn outputs(&self) -> Result<Vec<MachineAnchor>, ParseOrchestrationError> {
        self.anchors(|strand| strand.output_anchor_ids())
    }

    /// The plan of strand `strand` for a run that binds its inputs as
    /// `inputs` says. A name in `inputs` that is not an input of the strand is
    /// refused.
    pub async fn plan(
        &self,
        strand: usize,
        inputs: &RunInputs,
    ) -> Result<StrandPlan, ParseOrchestrationError> {
        let resolved = self.machine.strands().get(strand).ok_or_else(|| {
            ParseOrchestrationError::InvalidGraph {
                message: format!("the machine has no strand {strand}"),
            }
        })?;
        let run_inputs = inputs;
        let inputs: Vec<MachineAnchor> = self
            .inputs()?
            .into_iter()
            .filter(|input| input.strand == strand)
            .collect();
        for name in run_inputs.sequences.iter().chain(run_inputs.media.keys()) {
            if !inputs.iter().any(|input| &input.name == name) {
                return Err(ParseOrchestrationError::InvalidGraph {
                    message: format!("strand {strand} has no input named '{name}'"),
                });
            }
        }
        let anchor_inputs: HashMap<NodeId, AnchorInput> = inputs
            .iter()
            .map(|input| {
                (
                    input.node,
                    AnchorInput {
                        cardinality: if run_inputs.sequences.contains(&input.name) {
                            InputCardinality::Sequence
                        } else {
                            InputCardinality::Single
                        },
                        media: run_inputs
                            .media
                            .get(&input.name)
                            .cloned()
                            .unwrap_or_else(|| input.media_urn.clone()),
                    },
                )
            })
            .collect();
        let mut plan = MachinePlanBuilder::new(self.registry.clone())
            .build_plan_from_machine_strand_for_run(
                &format!("machine_strand_{strand}"),
                resolved,
                &anchor_inputs,
            )
            .await
            .map_err(|e| ParseOrchestrationError::InvalidGraph {
                message: format!("strand {strand}: plan construction failed: {e}"),
            })?;
        let input_slots: HashMap<String, String> = inputs
            .iter()
            .map(|input| (input.name.clone(), format!("input_slot_{}", input.node)))
            .collect();
        let mut output_names = HashMap::new();
        for output in self.outputs()?.into_iter().filter(|o| o.strand == strand) {
            output_names.insert(format!("output_{}", output.node), output.name);
        }
        // Where each node of the strand is held in the plan: an input in its
        // slot, a step's main output at the step, any other at the output's
        // own node.
        let mut node_names = HashMap::new();
        for input in &inputs {
            node_names.insert(format!("input_slot_{}", input.node), input.name.clone());
        }
        for edge in resolved.edges() {
            let main = edge.main_target();
            for output in &edge.outputs {
                let held_at = if main == Some(output.node) {
                    edge.token_id.to_string()
                } else {
                    format!("{}.{}", edge.token_id, output.output_name)
                };
                node_names.insert(held_at, self.name_of(strand, output.node)?);
            }
        }
        if let Some(unknown) = node_names.keys().find(|id| !plan.nodes.contains_key(*id)) {
            return Err(ParseOrchestrationError::InvalidGraph {
                message: format!("strand {strand}: the plan holds no node '{unknown}' to name"),
            });
        }
        // The plan's own boundary bears the names too: a slot is the input
        // the notation named, and an output the output it named — what a
        // drawing of the plan labels them with.
        let unnamed = |id: &str| ParseOrchestrationError::InvalidGraph {
            message: format!("strand {strand}: the plan has no boundary node '{id}' to name"),
        };
        for (name, slot) in &input_slots {
            match plan.nodes.get_mut(slot) {
                Some(node) => match &mut node.node_type {
                    ExecutionNodeType::InputSlot { slot_name, .. } => {
                        *slot_name = name.clone();
                        node.description = Some(format!("Input: {name}"));
                    }
                    _ => return Err(unnamed(slot)),
                },
                None => return Err(unnamed(slot)),
            }
        }
        for (output, name) in &output_names {
            match plan.nodes.get_mut(output).map(|node| &mut node.node_type) {
                Some(ExecutionNodeType::Output { output_name, .. }) => *output_name = name.clone(),
                _ => return Err(unnamed(output)),
            }
        }
        // And every output of the plan is one the notation named: the node is
        // where its name is kept.
        if let Some(nameless) = plan.output_nodes.iter().find(|output| !output_names.contains_key(*output)) {
            return Err(ParseOrchestrationError::InvalidGraph {
                message: format!("strand {strand}: the plan's output '{nameless}' is no output the notation names"),
            });
        }
        Ok(StrandPlan { strand, plan, node_names })
    }
}

/// Build one [`MachinePlan`] per strand from machine notation, every input of
/// it bound to one value. A run that binds an input to a sequence asks
/// [`ResolvedNotation::plan`] for the plan of that run.
pub async fn build_plans_from_notation(
    notation: &str,
    registry: Arc<FabricRegistry>,
) -> Result<Vec<MachinePlan>, ParseOrchestrationError> {
    build_plans_from_notation_from(notation, registry, &NoMachineSource, None).await
}

/// [`build_plans_from_notation`], for notation whose machine headers name
/// machines `source` holds (see [`ResolvedNotation::resolve_from`]).
pub async fn build_plans_from_notation_from(
    notation: &str,
    registry: Arc<FabricRegistry>,
    source: &dyn MachineSource,
    id: Option<&str>,
) -> Result<Vec<MachinePlan>, ParseOrchestrationError> {
    let resolved = ResolvedNotation::resolve_from(notation, registry, source, id).await?;
    let mut plans = Vec::with_capacity(resolved.strand_count());
    for strand in 0..resolved.strand_count() {
        plans.push(resolved.plan(strand, &RunInputs::declared()).await?.plan);
    }
    Ok(plans)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cap::definition::{ArgSource, Cap, CapArg, CapOutput, OutputCarries};
    use crate::planner::{EdgeType, ExecutionNodeType};
    use crate::urn::cap_urn::CapUrn;

    /// A registry cap. `output_is_sequence` models a cap that emits a sequence
    /// (render-page-image → many pages) — what a map is opened over. A cap
    /// whose `in=` is `media:void` takes no input: it has no main-input arg.
    pub(crate) fn cap(urn: &str, output_is_sequence: bool) -> Cap {
        let cap_urn = CapUrn::from_string(urn).unwrap();
        let in_spec = cap_urn.in_spec().to_string();
        let out_spec = cap_urn.out_spec().to_string();
        let mut output = CapOutput::new(out_spec, "output", "out".to_string());
        output.is_sequence = output_is_sequence;
        let args = if in_spec == "media:void" {
            Vec::new()
        } else {
            vec![CapArg::new(
                in_spec.clone(),
                "input",
                true,
                vec![ArgSource::Stdin { stdin: in_spec }],
            )]
        };
        Cap {
            urn: cap_urn,
            version: 1,
            title: "t".to_string(),
            cap_description: None,
            documentation: None,
            metadata: std::collections::HashMap::new(),
            aliases: vec!["test".to_string()],
            is_abstract: false,
            args,
            outputs: vec![output],
            metadata_json: None,
            registered_by: None,
            supported_model_types: Vec::new(),
            default_model_spec: None,
        }
    }

    /// `cap` with one more arg, named `name`: its identity is `media_urn`, and
    /// it takes what conforms to `accepts` — its own URN when `None`.
    fn with_arg(mut cap: Cap, name: &str, media_urn: &str, accepts: Option<&str>) -> Cap {
        let mut arg = CapArg::new(
            media_urn.to_string(),
            name,
            true,
            vec![ArgSource::CliFlag {
                cli_flag: format!("--{name}"),
            }],
        );
        arg.accepts = accepts.map(str::to_string);
        cap.args.push(arg);
        cap
    }

    /// `cap` with one more output, named `name`.
    fn with_output(mut cap: Cap, name: &str, media_urn: &str, is_sequence: bool) -> Cap {
        let mut output = CapOutput::new(media_urn.to_string(), name, name.to_string());
        output.is_sequence = is_sequence;
        cap.outputs.push(output);
        cap
    }

    pub(crate) fn registry(caps: Vec<Cap>) -> Arc<FabricRegistry> {
        let r = FabricRegistry::new_for_test();
        r.add_caps_to_cache(caps);
        Arc::new(r)
    }

    /// The header that gives `alias` to `urn`.
    fn header(alias: &str, urn: &str) -> String {
        format!("[{alias} {urn}]")
    }

    /// A plan's maps: for each, the nodes it iterates and the steps in its body.
    fn maps(plan: &MachinePlan) -> Vec<(String, Vec<String>, Vec<String>)> {
        let mut maps: Vec<(String, Vec<String>, Vec<String>)> = plan
            .nodes
            .iter()
            .filter_map(|(id, n)| match &n.node_type {
                ExecutionNodeType::ForEach { sources, body, .. } => Some((
                    id.clone(),
                    sources.iter().map(|s| s.node.clone()).collect(),
                    body.iter()
                        .filter(|member| plan.nodes[*member].is_cap())
                        .cloned()
                        .collect(),
                )),
                _ => None,
            })
            .collect();
        maps.sort();
        maps
    }

    /// The node of the one step that runs the cap `cap` (compared as cap
    /// URNs, never as strings).
    fn step_of(plan: &MachinePlan, cap: &str) -> String {
        let wanted = CapUrn::from_string(cap).unwrap();
        let found: Vec<&String> = plan
            .nodes
            .iter()
            .filter(|(_, n)| {
                n.cap_urn()
                    .is_some_and(|urn| CapUrn::from_string(urn).unwrap().is_equivalent(&wanted))
            })
            .map(|(id, _)| id)
            .collect();
        match found.as_slice() {
            [one] => (*one).clone(),
            other => panic!("expected one step running '{cap}', found {other:?}"),
        }
    }

    /// The node wired into `step` on its main input, and on its arg `arg_urn`.
    fn wired(plan: &MachinePlan, step: &str, arg_urn: Option<&str>) -> String {
        let found: Vec<&String> = plan
            .edges
            .iter()
            .filter(|e| e.to_node == step)
            .filter(|e| match (&e.edge_type, arg_urn) {
                (EdgeType::Direct, None) => true,
                (EdgeType::Arg { arg_urn: on }, Some(want)) => on == want,
                _ => false,
            })
            .map(|e| &e.from_node)
            .collect();
        match found.as_slice() {
            [one] => (*one).clone(),
            other => panic!("step '{step}' arg {arg_urn:?}: expected one wire, found {other:?}"),
        }
    }

    async fn one_plan(notation: &str, reg: Arc<FabricRegistry>) -> MachinePlan {
        let mut plans = build_plans_from_notation(notation, reg)
            .await
            .unwrap_or_else(|e| panic!("{notation}: {e}"));
        assert_eq!(plans.len(), 1, "one connected strand");
        let plan = plans.remove(0);
        plan.validate().expect("the builder makes a valid plan");
        plan
    }

    const EXTRACT: &str = r#"cap:in="media:document;ext=pdf";extract;out="media:enc=utf-8;ext=txt""#;
    const SUMMARIZE: &str = r#"cap:in="media:enc=utf-8;ext=txt";summarize;out="media:enc=utf-8;summary""#;
    const RENDER: &str = r#"cap:in="media:document;ext=pdf";render-page-image;out="media:ext=png;image""#;
    const TO_JPEG: &str = r#"cap:in="media:ext=png;image";convert-image;out="media:ext=jpeg;image""#;

    // TEST1303: A linear machine (no sequence output) lowers to a plan with no map.
    #[tokio::test]
    async fn test1303_linear_machine_no_foreach() {
        let reg = registry(vec![cap(EXTRACT, false), cap(SUMMARIZE, false)]);
        let notation = format!(
            "{}{}[doc -> extract -> text][text -> summarize -> summary]",
            header("extract", EXTRACT),
            header("summarize", SUMMARIZE)
        );
        let plan = one_plan(&notation, reg).await;
        assert!(maps(&plan).is_empty(), "a linear machine must not introduce a map");
        assert_eq!(plan.nodes.values().filter(|n| n.is_cap()).count(), 2);
        // The second step's main input is wired from the first.
        assert_eq!(
            wired(&plan, &step_of(&plan, SUMMARIZE), None),
            step_of(&plan, EXTRACT)
        );
    }

    // TEST1304: A sequence-output cap feeding a scalar-input cap lowers to a
    // map whose body is the consumer: the map iterates the producer's node,
    // and the consumer's main input is the map's item, not the sequence.
    #[tokio::test]
    async fn test1304_sequence_into_scalar_wraps_foreach() {
        let reg = registry(vec![cap(RENDER, true), cap(TO_JPEG, false)]);
        let notation = format!(
            "{}{}[input -> render -> thumbnail][thumbnail -> to_jpeg -> output]",
            header("render", RENDER),
            header("to_jpeg", TO_JPEG)
        );
        let plan = one_plan(&notation, reg).await;
        let render = step_of(&plan, RENDER);
        let to_jpeg = step_of(&plan, TO_JPEG);
        let maps = maps(&plan);
        assert_eq!(maps.len(), 1, "exactly one map");
        let (map_id, sources, body) = &maps[0];
        assert_eq!(sources, &vec![render.clone()], "the map iterates the rendered pages");
        assert_eq!(body, &vec![to_jpeg.clone()], "its body is the scalar consumer, not render");
        let item = wired(&plan, &to_jpeg, None);
        assert!(
            matches!(
                &plan.nodes[&item].node_type,
                ExecutionNodeType::Item { foreach, source, .. } if foreach == map_id && source == &render
            ),
            "the consumer is given the map's item of the pages, got {:?}",
            plan.nodes[&item].node_type
        );
        assert!(plan.enclosing_maps().get(render.as_str()).is_none(), "render runs once");
    }

    // TEST1305: A disconnected multi-strand machine yields one plan per strand.
    #[tokio::test]
    async fn test1305_multi_strand_yields_one_plan_each() {
        let reg = registry(vec![cap(RENDER, true), cap(TO_JPEG, false)]);
        let notation = format!(
            "{}{}[a_in -> render -> a_thumb][a_thumb -> to_jpeg -> a_out]\
             [b_in -> render -> b_thumb][b_thumb -> to_jpeg -> b_out]",
            header("render", RENDER),
            header("to_jpeg", TO_JPEG)
        );
        let plans = build_plans_from_notation(&notation, reg).await.expect("build");
        assert_eq!(plans.len(), 2, "two disconnected strands → two plans");
        for plan in &plans {
            assert_eq!(maps(plan).len(), 1, "each strand keeps its own map");
        }
    }

    /// A registry cap whose single stdin arg is a SEQUENCE arg — the gather
    /// consumer shape (e.g. `concat`: sequence of text items → one text).
    fn seq_arg_cap(urn: &str) -> Cap {
        let mut c = cap(urn, false);
        c.args[0].is_sequence = true;
        c
    }

    const OP_A: &str = r#"cap:in="media:document;ext=pdf";op-a;out="media:enc=utf-8;page""#;
    const OP_B: &str = r#"cap:in="media:ext=md";op-b;out="media:enc=utf-8;page""#;
    const CONCAT: &str = r#"cap:in="media:enc=utf-8";concat;out="media:enc=utf-8;ext=txt""#;

    // TEST1307: Fan-in `(x, y) -> concat` where concat's stdin arg is a SEQUENCE
    // arg gathers both producers through a `Collect` node: the plan carries a
    // real `Collect{input_nodes=[a,b]}` feeding concat (never a bare
    // multi-bound arg), in the order the members were written, and no map is
    // introduced (the gathered value IS the sequence the arg consumes).
    #[tokio::test]
    async fn test1307_fan_in_gather_synthesizes_collect() {
        let reg = registry(vec![cap(OP_A, false), cap(OP_B, false), seq_arg_cap(CONCAT)]);
        let notation = format!(
            "{}{}{}[doc_a -> a -> x][doc_b -> b -> y][(y, x) -> cat -> z]",
            header("a", OP_A),
            header("b", OP_B),
            header("cat", CONCAT)
        );
        let plan = one_plan(&notation, reg).await;
        let concat = step_of(&plan, CONCAT);
        let collect = wired(&plan, &concat, None);
        match &plan.nodes[&collect].node_type {
            ExecutionNodeType::Collect {
                input_nodes,
                output_media_urn,
            } => {
                assert_eq!(
                    input_nodes,
                    &vec![step_of(&plan, OP_B), step_of(&plan, OP_A)],
                    "the members in the order they were written"
                );
                assert!(
                    crate::MediaUrn::from_string(output_media_urn)
                        .unwrap()
                        .is_equivalent(&crate::MediaUrn::from_string("media:enc=utf-8;page").unwrap())
                        .unwrap(),
                    "the gathered sequence's element type is the members' join, got {output_media_urn}"
                );
            }
            other => panic!("concat's main input must be a gather, got {other:?}"),
        }
        assert_eq!(
            plan.edges
                .iter()
                .filter(|e| e.to_node == collect && matches!(e.edge_type, EdgeType::Collection))
                .count(),
            2,
            "one Collection edge per gathered producer"
        );
        assert!(maps(&plan).is_empty(), "a gather into a sequence arg opens no map");
    }

    // TEST1309: a SEQUENCE-consuming cap given what a map produces folds it:
    // it runs outside the map, wired from the mapped step — which, read from
    // outside the map, is the sequence of its values over the runs.
    #[tokio::test]
    async fn test1309_fold_after_mapped_region_closes_the_region() {
        const UPSCALE: &str = r#"cap:in="media:ext=png;image";upscale;out="media:ext=png;image;upscaled""#;
        const ANIMATE: &str = r#"cap:in="media:ext=png;image";animate;out="media:ext=gif;image""#;
        let reg = registry(vec![cap(RENDER, true), cap(UPSCALE, false), seq_arg_cap(ANIMATE)]);
        let notation = format!(
            "{}{}{}[doc -> render -> pics][pics -> upscale -> big][big -> animate -> out]",
            header("render", RENDER),
            header("upscale", UPSCALE),
            header("animate", ANIMATE)
        );
        let plan = one_plan(&notation, reg).await;
        let upscale = step_of(&plan, UPSCALE);
        let animate = step_of(&plan, ANIMATE);
        let maps = maps(&plan);
        assert_eq!(maps.len(), 1, "upscale maps per item under one map");
        assert_eq!(maps[0].2, vec![upscale.clone()], "the body is the scalar mapper alone");
        let enclosing = plan.enclosing_maps();
        assert_eq!(enclosing.get(upscale.as_str()), Some(&maps[0].0.as_str()));
        assert_eq!(enclosing.get(animate.as_str()), None, "the fold runs once, outside the map");
        assert_eq!(wired(&plan, &animate, None), upscale, "wired straight from the mapped step");
    }

    // TEST1306: two values need somewhere to go. Two unnamed sources that
    // both fit a cap's one scalar arg are refused — nothing says which is the
    // input. The same two on a cap with an arg for each build, each wired to
    // its own arg; and on a cap whose arg takes a sequence they are a gather.
    #[tokio::test]
    async fn test1306_two_sources_need_an_arg_each_or_a_sequence_arg() {
        const A: &str = r#"cap:in="media:document;ext=pdf";a;out="media:enc=utf-8""#;
        const B: &str = r#"cap:in="media:ext=md";b;out="media:enc=utf-8""#;
        const MERGE: &str = r#"cap:in="media:enc=utf-8";merge;out="media:enc=utf-8;merged""#;
        const OTHER: &str = "media:enc=utf-8;other";
        let wirings = |merge: &str| {
            format!(
                "{}{}{}[doc_a -> a -> x][doc_b -> b -> y][{merge} -> merge -> z]",
                header("a", A),
                header("b", B),
                header("merge", MERGE)
            )
        };
        let caps = |merge: Cap| registry(vec![cap(A, false), cap(B, false), merge]);

        // One scalar arg, two sources.
        match build_plans_from_notation(&wirings("(x, y)"), caps(cap(MERGE, false))).await {
            Err(ParseOrchestrationError::MachineSyntaxParseFailed(message)) => {
                assert!(message.contains("merge"), "the refusal names the cap: {message}");
            }
            other => panic!("expected the wiring to be refused, got {other:?}"),
        }

        // An arg for each: the named one is wired as that arg, the other is the input.
        // `other` is its own arg and takes any text.
        let two_args = || with_arg(cap(MERGE, false), "other", OTHER, Some("media:enc=utf-8"));
        let plan = one_plan(&wirings("(x, other=y)"), caps(two_args())).await;
        let merge = step_of(&plan, MERGE);
        assert_eq!(wired(&plan, &merge, None), step_of(&plan, A));
        assert_eq!(wired(&plan, &merge, Some(OTHER)), step_of(&plan, B));
        // Naming the other way round wires them the other way round.
        let plan = one_plan(&wirings("(y, other=x)"), caps(two_args())).await;
        let merge = step_of(&plan, MERGE);
        assert_eq!(wired(&plan, &merge, None), step_of(&plan, B));
        assert_eq!(wired(&plan, &merge, Some(OTHER)), step_of(&plan, A));

        // A sequence arg gathers them.
        let plan = one_plan(&wirings("(x, y)"), caps(seq_arg_cap(MERGE))).await;
        let collect = wired(&plan, &step_of(&plan, MERGE), None);
        assert!(matches!(plan.nodes[&collect].node_type, ExecutionNodeType::Collect { .. }));
    }

    // TEST1261: a header whose cap the registry cannot resolve is reported as
    // a machine-notation parse failure at the header, naming the cap.
    #[tokio::test]
    async fn test1261_an_unresolvable_header_cap_is_a_parse_failure_at_the_header() {
        let reg = registry(vec![cap(EXTRACT, false)]);
        let notation = format!("{}[doc -> summarize -> s]", header("summarize", SUMMARIZE));
        match build_plans_from_notation(&notation, reg).await {
            Err(ParseOrchestrationError::MachineSyntaxParseFailed(message)) => {
                assert!(message.contains("in header"), "{message}");
                assert!(message.contains("summarize"), "{message}");
            }
            other => panic!("expected a parse failure at the header, got {other:?}"),
        }
    }

    // TEST1262: text that is not machine notation fails as a notation parse
    // failure.
    #[tokio::test]
    async fn test1262_invalid_machine_notation() {
        let reg = registry(vec![cap(EXTRACT, false)]);
        for notation in ["this is not notation", "[doc -> -> text]", ""] {
            match build_plans_from_notation(notation, reg.clone()).await {
                Err(ParseOrchestrationError::MachineSyntaxParseFailed(_)) => {}
                other => panic!("'{notation}': expected a parse failure, got {other:?}"),
            }
        }
    }

    // TEST1263: wirings that wait on one another are refused as not a DAG.
    #[tokio::test]
    async fn test1263_cycle_detection() {
        const STEP: &str = r#"cap:in="media:enc=utf-8";rewrite;out="media:enc=utf-8""#;
        let reg = registry(vec![cap(STEP, false)]);
        let notation = format!(
            "{}[a -> rewrite -> b][b -> rewrite -> c][c -> rewrite -> a]",
            header("rewrite", STEP)
        );
        match build_plans_from_notation(&notation, reg).await {
            Err(ParseOrchestrationError::NotADag { cycle_nodes }) => {
                assert_eq!(cycle_nodes, vec!["strand 0".to_string()]);
            }
            other => panic!("expected NotADag, got {other:?}"),
        }
    }

    const SPLIT: &str = r#"cap:in="media:document;ext=pdf";split;out="media:page;textable""#;
    const OCR: &str = r#"cap:in="media:page;textable";ocr;out="media:enc=utf-8;ocr""#;
    const TITLE: &str = r#"cap:in="media:page;textable";title;out="media:enc=utf-8;title""#;
    const JOIN: &str = r#"cap:in="media:enc=utf-8;ocr";join;out="media:enc=utf-8;joined""#;
    const PAIR: &str = r#"cap:in="media:enc=utf-8;ocr";pair;out="media:enc=utf-8;paired""#;
    const PAIR_TITLE: &str = "media:enc=utf-8;title";

    // TEST12762: two steps that map over the same sequence run in ONE map,
    // and a step given what each produced for an item joins it: it is wired
    // from the two steps directly, with no second map and nothing zipped.
    #[tokio::test]
    async fn test12762_steps_over_one_sequence_share_a_map() {
        let reg = registry(vec![
            cap(SPLIT, true),
            cap(OCR, false),
            cap(TITLE, false),
            with_arg(cap(PAIR, false), "title", PAIR_TITLE, None),
        ]);
        let notation = format!(
            "{}{}{}{}[doc -> split -> pages][pages -> ocr -> text][pages -> title -> t]\
             [(text, title=t) -> pair -> out]",
            header("split", SPLIT),
            header("ocr", OCR),
            header("title", TITLE),
            header("pair", PAIR)
        );
        let plan = one_plan(&notation, reg).await;
        let (ocr, title, pair) = (step_of(&plan, OCR), step_of(&plan, TITLE), step_of(&plan, PAIR));
        let maps = maps(&plan);
        assert_eq!(maps.len(), 1, "one map over the pages, not one per step");
        let mut body = maps[0].2.clone();
        body.sort();
        let mut expected = vec![ocr.clone(), title.clone(), pair.clone()];
        expected.sort();
        assert_eq!(body, expected);
        assert_eq!(wired(&plan, &pair, None), ocr);
        assert_eq!(wired(&plan, &pair, Some(PAIR_TITLE)), title);
        // Both mappers are given the SAME item node.
        assert_eq!(wired(&plan, &ocr, None), wired(&plan, &title, None));
    }

    // TEST12763: two sequences on two args that each take one value are
    // zipped by one map: it iterates both, and the step is given the item of
    // each on the arg it was bound to.
    #[tokio::test]
    async fn test12763_two_sequences_are_zipped_by_one_map() {
        const PAGES_B: &str = r#"cap:in="media:ext=md";titles;out="media:enc=utf-8;title""#;
        const TEXTS: &str = r#"cap:in="media:document;ext=pdf";texts;out="media:enc=utf-8;ocr""#;
        let reg = registry(vec![
            cap(TEXTS, true),
            cap(PAGES_B, true),
            with_arg(cap(PAIR, false), "title", PAIR_TITLE, None),
        ]);
        let notation = format!(
            "{}{}{}[doc -> texts -> text][notes -> titles -> t][(text, title=t) -> pair -> out]",
            header("texts", TEXTS),
            header("titles", PAGES_B),
            header("pair", PAIR)
        );
        let plan = one_plan(&notation, reg).await;
        let (texts, titles, pair) = (step_of(&plan, TEXTS), step_of(&plan, PAGES_B), step_of(&plan, PAIR));
        let maps = maps(&plan);
        assert_eq!(maps.len(), 1);
        let (map_id, sources, body) = &maps[0];
        let mut zipped = sources.clone();
        zipped.sort();
        let mut expected = vec![texts.clone(), titles.clone()];
        expected.sort();
        assert_eq!(zipped, expected, "the map zips both sequences");
        assert_eq!(body, &vec![pair.clone()]);
        let item_of = |node: &str| match &plan.nodes[node].node_type {
            ExecutionNodeType::Item { foreach, source, .. } => {
                assert_eq!(foreach, map_id);
                source.clone()
            }
            other => panic!("expected an item, got {other:?}"),
        };
        assert_eq!(item_of(&wired(&plan, &pair, None)), texts, "the main input is the text's item");
        assert_eq!(item_of(&wired(&plan, &pair, Some(PAIR_TITLE))), titles);
    }

    // TEST12813: a step inside a map may also be given the WHOLE of the
    // sequence the map iterates, on an arg that takes a sequence: every run
    // is wired the item on the arg that takes one value, and the sequence
    // itself on the arg that takes them all. The sequence is iterated once.
    #[tokio::test]
    async fn test12813_a_sequence_arg_inside_a_map_is_given_the_whole_sequence() {
        const TEXTS: &str = r#"cap:in="media:document;ext=pdf";texts;out="media:enc=utf-8;ocr""#;
        const ALL: &str = "media:all-texts;enc=utf-8";
        let mut among = with_arg(cap(PAIR, false), "all", ALL, Some("media:enc=utf-8;ocr"));
        among.args.last_mut().unwrap().is_sequence = true;
        let reg = registry(vec![cap(TEXTS, true), among]);
        let notation = format!(
            "{}{}[doc -> texts -> text][(text, all=text) -> pair -> out]",
            header("texts", TEXTS),
            header("pair", PAIR)
        );
        let plan = one_plan(&notation, reg).await;
        let (texts, pair) = (step_of(&plan, TEXTS), step_of(&plan, PAIR));
        let maps = maps(&plan);
        assert_eq!(maps.len(), 1);
        assert_eq!(maps[0].1, vec![texts.clone()], "one source, whatever else reads it");
        assert_eq!(maps[0].2, vec![pair.clone()]);
        match &plan.nodes[&wired(&plan, &pair, None)].node_type {
            ExecutionNodeType::Item { source, .. } => assert_eq!(source, &texts),
            other => panic!("the main input is the item, got {other:?}"),
        }
        assert_eq!(wired(&plan, &pair, Some(ALL)), texts, "the sequence arg is wired the sequence itself");
    }

    // TEST12764: a sequence produced inside a map and bound to an arg that
    // takes one value opens a map inside that map's body; a step that takes a
    // sequence and is given what the inner map produces runs in the OUTER
    // body — one value per outer item.
    #[tokio::test]
    async fn test12764_a_map_opens_inside_a_map_and_a_fold_closes_one_level() {
        let reg = registry(vec![cap(SPLIT, true), cap(OCR, false), seq_arg_cap(JOIN)]);
        let notation = format!(
            "{}{}{}[docs -> split -> pages][pages -> ocr -> text][text -> join -> doc_text]",
            header("split", SPLIT),
            header("ocr", OCR),
            header("join", JOIN)
        );
        let (machine, _) = crate::machine::parse_machine_with_node_names_async(&notation, &reg)
            .await
            .unwrap();
        let strand = &machine.strands()[0];
        // The run binds the machine's input to a sequence of documents.
        let plan = MachinePlanBuilder::new(reg.clone())
            .build_plan_from_machine_strand("nested", strand, InputCardinality::Sequence)
            .await
            .expect("builds");
        plan.validate().expect("valid");
        let (split, ocr, join) = (step_of(&plan, SPLIT), step_of(&plan, OCR), step_of(&plan, JOIN));
        let enclosing = plan.enclosing_maps();
        let outer = *enclosing.get(split.as_str()).expect("split runs once per document");
        let inner = *enclosing.get(ocr.as_str()).expect("ocr runs once per page");
        assert_ne!(outer, inner);
        assert_eq!(enclosing.get(inner), Some(&outer), "the page map is opened inside the document map");
        assert_eq!(enclosing.get(outer), None);
        assert_eq!(enclosing.get(join.as_str()), Some(&outer), "the fold runs once per document");
        match &plan.nodes[inner].node_type {
            ExecutionNodeType::ForEach { sources, .. } => {
                assert_eq!(sources.len(), 1);
                assert_eq!(sources[0].node, split, "the inner map iterates that document's pages");
            }
            _ => unreachable!(),
        }
        assert_eq!(wired(&plan, &join, None), ocr, "the fold is wired from the inner step");
        let inside_outer = plan.map_members(outer);
        assert!(inside_outer.contains(&ocr) && inside_outer.contains(inner));
    }

    const REGRESS: &str = r#"cap:in="media:enc=utf-8;ext=csv";regress;out="media:fit;record""#;
    const RESIDUALS: &str = "media:residuals;record";
    const PLOT: &str = r#"cap:in="media:residuals;record";plot;out="media:ext=png;image""#;
    const REPORT: &str = r#"cap:in="media:fit;record";report;out="media:enc=utf-8;ext=md""#;

    // TEST12765: every output of a step other than its main one that
    // something takes is a node of its own: what reads it is wired from that
    // node, the main output's reader from the step, and a machine output that
    // is such an output is of that node.
    #[tokio::test]
    async fn test12765_an_output_other_than_the_main_one_is_its_own_node() {
        let reg = registry(vec![
            with_output(cap(REGRESS, false), "residuals", RESIDUALS, false),
            cap(PLOT, false),
            cap(REPORT, false),
        ]);
        let headers = format!(
            "{}{}{}",
            header("regress", REGRESS),
            header("plot", PLOT),
            header("report", REPORT)
        );
        let plan = one_plan(
            &format!("{headers}[obs -> regress -> (fit, residuals=res)][res -> plot -> chart][fit -> report -> r]"),
            reg.clone(),
        )
        .await;
        let regress = step_of(&plan, REGRESS);
        let output = wired(&plan, &step_of(&plan, PLOT), None);
        match &plan.nodes[&output].node_type {
            ExecutionNodeType::CapOutput {
                cap_node,
                output_media_urn,
                output_name,
                carries_input,
            } => {
                assert_eq!(cap_node, &regress);
                assert_eq!(output_media_urn, RESIDUALS);
                assert_eq!(output_name, "residuals");
                assert!(!carries_input);
            }
            other => panic!("plot must read the residuals output's node, got {other:?}"),
        }
        assert_eq!(wired(&plan, &step_of(&plan, REPORT), None), regress);
        assert!(plan
            .edges
            .iter()
            .any(|e| e.from_node == regress && e.to_node == output && matches!(e.edge_type, EdgeType::Direct)));

        // Taken and read by nothing: it is the machine's output, of that node.
        let plan = one_plan(&format!("{headers}[obs -> regress -> (fit, residuals=res)]"), reg.clone()).await;
        let mut sources: Vec<String> = plan
            .nodes
            .values()
            .filter_map(|n| match &n.node_type {
                ExecutionNodeType::Output { source_node, .. } => Some(source_node.clone()),
                _ => None,
            })
            .collect();
        sources.sort();
        let regress = step_of(&plan, REGRESS);
        let mut expected = vec![regress.clone(), format!("{regress}.residuals")];
        expected.sort();
        assert_eq!(sources, expected, "both outputs are outputs of the machine");

        // An output the wiring does not take has no node.
        let plan = one_plan(&format!("{headers}[obs -> regress -> fit]"), reg).await;
        assert!(!plan
            .nodes
            .values()
            .any(|n| matches!(n.node_type, ExecutionNodeType::CapOutput { .. })));
    }

    // TEST12766: an output that carries its step's input holds, in a run,
    // what the step was given — the plan says so on the output's node.
    #[tokio::test]
    async fn test12766_an_output_that_carries_the_input_says_so() {
        const ROUTE: &str = r#"cap:in="media:textable";route;out="media:textable""#;
        let mut route = cap(ROUTE, false);
        route.outputs[0].optional = true;
        let mut rejected = CapOutput::new("media:rejected".to_string(), "rejected", "no".to_string());
        rejected.optional = true;
        rejected.carries = Some(OutputCarries::Input);
        route.outputs.push(rejected);
        let reg = registry(vec![route]);
        let plan = one_plan(
            &format!("{}[page -> route -> (kept, rejected=dropped)]", header("route", ROUTE)),
            reg,
        )
        .await;
        let node = format!("{}.rejected", step_of(&plan, ROUTE));
        assert!(matches!(
            &plan.nodes[&node].node_type,
            ExecutionNodeType::CapOutput { carries_input: true, .. }
        ));
    }

    const NOW: &str = r#"cap:in="media:void";now;out="media:stamp""#;
    const PUBLISH: &str = r#"cap:in="media:stamp";publish;out="media:void""#;
    const FETCH: &str = r#"cap:in="media:void";fetch;out="media:enc=utf-8;series""#;
    const REFERENCE: &str = "media:enc=utf-8;reference";

    // TEST12767: a machine may begin with a step nothing is wired into and
    // end in a step nothing reads. The plan then has no input slot and no
    // output; the source step has no main-input edge; and a constant is the
    // step's binding for its arg, with nothing wired.
    #[tokio::test]
    async fn test12767_a_machine_with_no_input_and_no_output() {
        let mut publish = cap(PUBLISH, false);
        publish.outputs.clear();
        let reg = registry(vec![cap(NOW, false), publish]);
        let plan = one_plan(
            &format!(
                "{}{}[() -> now -> stamp][stamp -> publish -> ()]",
                header("now", NOW),
                header("publish", PUBLISH)
            ),
            reg,
        )
        .await;
        assert!(plan.entry_nodes.is_empty(), "no input slot");
        assert!(plan.output_nodes.is_empty(), "no output");
        let now = step_of(&plan, NOW);
        assert!(!plan.edges.iter().any(|e| e.to_node == now), "nothing is wired into a source");
        assert_eq!(wired(&plan, &step_of(&plan, PUBLISH), None), now);

        // A source started by a constant.
        let reg = registry(vec![with_arg(cap(FETCH, false), "reference", REFERENCE, None)]);
        // A constant is held to its arg's media definition, which must be known.
        reg.insert_cached_media_def_for_test(crate::fabric::registry::StoredMediaDef {
            version: 0,
            urn: REFERENCE.to_string(),
            media_type: "text/plain".to_string(),
            title: "reference".to_string(),
            profile_uri: None,
            schema: None,
            description: None,
            documentation: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        });
        let plan = one_plan(
            &format!("{}[(reference=\"fred/UNRATE\") -> fetch -> series]", header("fetch", FETCH)),
            reg,
        )
        .await;
        let fetch = step_of(&plan, FETCH);
        assert!(!plan.edges.iter().any(|e| e.to_node == fetch));
        match &plan.nodes[&fetch].node_type {
            ExecutionNodeType::Cap { arg_bindings, .. } => {
                match arg_bindings.bindings.get(REFERENCE) {
                    Some(crate::planner::ArgumentBinding::Literal { value }) => {
                        assert_eq!(value, &serde_json::json!("fred/UNRATE"));
                    }
                    other => panic!("the constant is the arg's binding, got {other:?}"),
                }
            }
            _ => unreachable!(),
        }
        assert_eq!(plan.output_nodes.len(), 1);
    }

    // TEST12768: one value bound to an arg that takes a sequence is delivered
    // as a sequence of one: the plan gathers it, alone. A sequence bound to
    // that arg is wired as it is.
    #[tokio::test]
    async fn test12768_one_value_into_a_sequence_arg_is_a_gather_of_one() {
        let reg = registry(vec![cap(OP_A, false), seq_arg_cap(CONCAT)]);
        let plan = one_plan(
            &format!(
                "{}{}[doc -> a -> x][x -> cat -> z]",
                header("a", OP_A),
                header("cat", CONCAT)
            ),
            reg,
        )
        .await;
        let collect = wired(&plan, &step_of(&plan, CONCAT), None);
        match &plan.nodes[&collect].node_type {
            ExecutionNodeType::Collect { input_nodes, .. } => {
                assert_eq!(input_nodes, &vec![step_of(&plan, OP_A)]);
            }
            other => panic!("expected a gather of one, got {other:?}"),
        }

        let reg = registry(vec![cap(OP_A, true), seq_arg_cap(CONCAT)]);
        let plan = one_plan(
            &format!(
                "{}{}[doc -> a -> x][x -> cat -> z]",
                header("a", OP_A),
                header("cat", CONCAT)
            ),
            reg,
        )
        .await;
        assert_eq!(wired(&plan, &step_of(&plan, CONCAT), None), step_of(&plan, OP_A));
    }

    // TEST12791: a machine is addressed by the names its notation wrote: what
    // it takes and what it gives, each with its media, strand by strand. A
    // run that binds an input to a sequence gets the plan of THAT run, with
    // the slot of every input and the name of every output; a name that is
    // not an input is refused.
    #[tokio::test]
    async fn test12791_a_machine_is_addressed_by_its_written_names() {
        let reg = registry(vec![cap(EXTRACT, false), cap(SUMMARIZE, false), cap(RENDER, true)]);
        let notation = format!(
            "{}{}{}[doc -> extract -> text][text -> summarize -> summary][scan -> render -> pages]",
            header("extract", EXTRACT),
            header("summarize", SUMMARIZE),
            header("render", RENDER)
        );
        let resolved = ResolvedNotation::resolve(&notation, reg).await.expect("resolves");
        assert_eq!(resolved.strand_count(), 2);
        let named = |anchors: Vec<MachineAnchor>| -> Vec<(usize, String, String)> {
            anchors
                .into_iter()
                .map(|anchor| (anchor.strand, anchor.name, anchor.media_urn.to_string()))
                .collect()
        };
        let inputs = named(resolved.inputs().unwrap());
        let outputs = named(resolved.outputs().unwrap());
        // Which strand is which is the resolver's to order; within the machine
        // each name appears once, with what it holds.
        let of = |list: &[(usize, String, String)], name: &str| {
            let found: Vec<_> = list.iter().filter(|(_, n, _)| n == name).collect();
            assert_eq!(found.len(), 1, "'{name}' in {list:?}");
            found[0].clone()
        };
        assert_eq!(inputs.len(), 2);
        assert_eq!(outputs.len(), 2);
        let same = |a: &str, b: &str| {
            crate::MediaUrn::from_string(a)
                .unwrap()
                .is_equivalent(&crate::MediaUrn::from_string(b).unwrap())
                .unwrap()
        };
        let (doc_strand, _, doc_media) = of(&inputs, "doc");
        let (scan_strand, _, _) = of(&inputs, "scan");
        assert_ne!(doc_strand, scan_strand);
        assert!(same(&doc_media, "media:document;ext=pdf"));
        let (summary_strand, _, summary_media) = of(&outputs, "summary");
        assert_eq!(summary_strand, doc_strand);
        assert!(same(&summary_media, "media:enc=utf-8;summary"));
        assert_eq!(of(&outputs, "pages").0, scan_strand);
        // `text` is neither: it is produced and consumed inside the machine.
        assert!(!inputs.iter().chain(&outputs).any(|(_, name, _)| name == "text"));

        // One value: no map. The slot and the output are found by name.
        let single = resolved.plan(doc_strand, &RunInputs::declared()).await.unwrap();
        assert!(maps(&single.plan).is_empty());
        let slot = &single.input_slots()["doc"];
        assert!(matches!(
            &single.plan.nodes[slot].node_type,
            ExecutionNodeType::InputSlot { cardinality: InputCardinality::Single, .. }
        ));
        assert_eq!(single.input_slots().len(), 1);
        assert!(matches!(
            &single.plan.nodes[slot].node_type,
            ExecutionNodeType::InputSlot { slot_name, .. } if slot_name == "doc"
        ));
        let output_names = single.output_names();
        let (output_node, name) = output_names.iter().next().unwrap();
        assert_eq!(name, "summary");
        assert!(matches!(
            &single.plan.nodes[output_node].node_type,
            ExecutionNodeType::Output { output_name, .. } if output_name == "summary"
        ));
        // Every value of the strand is named where the plan holds it: the
        // input at its slot, and what each step produces at the step.
        let named_at = |id: &str| single.node_names.get(id).map(String::as_str);
        assert_eq!(named_at(slot), Some("doc"));
        assert_eq!(named_at(&step_of(&single.plan, EXTRACT)), Some("text"));
        assert_eq!(named_at(&step_of(&single.plan, SUMMARIZE)), Some("summary"));
        assert_eq!(single.node_names.len(), 3);
        assert!(matches!(
            &single.plan.nodes[output_node].node_type,
            ExecutionNodeType::Output { source_node, .. } if source_node == &step_of(&single.plan, SUMMARIZE)
        ));

        // A sequence of documents: the same strand, mapped.
        let many = resolved
            .plan(
                doc_strand,
                &RunInputs { sequences: HashSet::from(["doc".to_string()]), ..RunInputs::default() },
            )
            .await
            .unwrap();
        assert_eq!(maps(&many.plan).len(), 1);
        assert!(matches!(
            &many.plan.nodes[&many.input_slots()["doc"]].node_type,
            ExecutionNodeType::InputSlot { cardinality: InputCardinality::Sequence, .. }
        ));

        // Not an input of that strand; not a strand.
        for (strand, name) in [(doc_strand, "scan"), (doc_strand, "text"), (scan_strand, "nowhere")] {
            match resolved.plan(strand, &RunInputs { sequences: HashSet::from([name.to_string()]), ..RunInputs::default() }).await {
                Err(ParseOrchestrationError::InvalidGraph { message }) => {
                    assert!(message.contains(&format!("no input named '{name}'")), "{message}");
                }
                other => panic!("expected a refusal for '{name}', got {:?}", other.map(|p| p.plan.name)),
            }
        }
        assert!(resolved.plan(7, &RunInputs::declared()).await.is_err());
    }

    // TEST12820: what a step gives on an output other than its main one is
    // named where the plan holds it — that output's own node — so a report
    // about that value can say the name the machine wrote for it.
    #[tokio::test]
    async fn test12820_another_output_of_a_step_is_named_at_its_own_node() {
        let reg = registry(vec![
            with_output(cap(REGRESS, false), "residuals", RESIDUALS, false),
            cap(PLOT, false),
            cap(REPORT, false),
        ]);
        let notation = format!(
            "{}{}{}[obs -> regress -> (fit, residuals=res)][res -> plot -> chart][fit -> report -> r]",
            header("regress", REGRESS),
            header("plot", PLOT),
            header("report", REPORT)
        );
        let resolved = ResolvedNotation::resolve(&notation, reg).await.expect("resolves");
        let strand = resolved.plan(0, &RunInputs::declared()).await.expect("plans");
        let regress = step_of(&strand.plan, REGRESS);
        let residuals = wired(&strand.plan, &step_of(&strand.plan, PLOT), None);
        assert_ne!(residuals, regress, "the other output is a node of its own");
        let named_at = |id: &str| strand.node_names.get(id).map(String::as_str);
        assert_eq!(named_at(&residuals), Some("res"));
        assert_eq!(named_at(&regress), Some("fit"));
        assert_eq!(named_at(&step_of(&strand.plan, PLOT)), Some("chart"));
    }

    // TEST12769: a step's policy goes with it into the plan.
    #[tokio::test]
    async fn test12769_a_steps_policy_is_on_its_plan_node() {
        let reg = registry(vec![cap(EXTRACT, false), cap(SUMMARIZE, false)]);
        let plan = one_plan(
            &format!(
                "{}{}[doc -> extract{{retry=3, on_error=absent}} -> text][text -> summarize -> s]",
                header("extract", EXTRACT),
                header("summarize", SUMMARIZE)
            ),
            reg,
        )
        .await;
        let policy_of = |cap: &str| match &plan.nodes[&step_of(&plan, cap)].node_type {
            ExecutionNodeType::Cap { policy, .. } => *policy,
            _ => unreachable!(),
        };
        assert_eq!(
            policy_of(EXTRACT),
            crate::machine::StepPolicy {
                retry: 3,
                on_error: crate::machine::OnError::Absent,
            }
        );
        assert!(policy_of(SUMMARIZE).is_default());
    }

    // ---- What a run of a plan is asked for (MachinePlanBuilder::analyze_plan_arguments)

    /// The requirements of the one plan of `notation`, by step URN.
    async fn requirements(
        notation: &str,
        reg: Arc<FabricRegistry>,
    ) -> (MachinePlan, crate::planner::PlanArgumentRequirements) {
        let plan = one_plan(notation, reg.clone()).await;
        let requirements = MachinePlanBuilder::new(reg)
            .analyze_plan_arguments(&plan)
            .await
            .unwrap_or_else(|e| panic!("{notation}: {e}"));
        (plan, requirements)
    }

    /// The arg named `name` of the one step running `cap`.
    fn arg_of<'a>(
        requirements: &'a crate::planner::PlanArgumentRequirements,
        cap: &str,
        name: &str,
    ) -> &'a crate::planner::ArgumentInfo {
        let wanted = CapUrn::from_string(cap).unwrap();
        let step = requirements
            .steps
            .iter()
            .find(|s| CapUrn::from_string(&s.cap_urn).unwrap().is_equivalent(&wanted))
            .unwrap_or_else(|| panic!("no step runs '{cap}'"));
        step.arguments
            .iter()
            .find(|a| a.name == name)
            .unwrap_or_else(|| panic!("step '{cap}' has no arg named '{name}'"))
    }

    fn slot_names(requirements: &crate::planner::PlanArgumentRequirements) -> Vec<String> {
        let mut names: Vec<String> = requirements
            .steps
            .iter()
            .flat_map(|s| s.slots.iter().map(|a| a.name.clone()))
            .collect();
        names.sort();
        names
    }

    const COMPARE: &str = r#"cap:in="media:enc=utf-8;ext=txt";compare;out="media:comparison;enc=utf-8""#;

    // TEST994: an arg is wired from an input of the machine or from what a
    // step produced, and the analysis says which; neither is asked of the run.
    #[tokio::test]
    async fn test994_wired_args_come_from_an_input_or_a_step_and_are_not_slots() {
        let reg = registry(vec![cap(EXTRACT, false), cap(SUMMARIZE, false)]);
        let notation = format!(
            "{}{}[doc -> extract -> text][text -> summarize -> summary]",
            header("extract", EXTRACT),
            header("summarize", SUMMARIZE)
        );
        let (_, req) = requirements(&notation, reg).await;
        let extract_in = arg_of(&req, EXTRACT, "input");
        assert_eq!(extract_in.resolution, crate::planner::ArgumentResolution::FromInput);
        assert!(extract_in.is_main_input);
        let summarize_in = arg_of(&req, SUMMARIZE, "input");
        assert_eq!(summarize_in.resolution, crate::planner::ArgumentResolution::FromStep);
        assert!(slot_names(&req).is_empty(), "a wired arg is not the run's to give");
        assert!(req.can_execute_without_input);
    }

    // TEST1009: an arg the machine leaves unbound is the run's: a slot,
    // carrying its definition's default, which the run may go without.
    #[tokio::test]
    async fn test1009_an_unbound_arg_with_a_default_is_a_slot_the_run_may_skip() {
        let mut summarize = with_arg(cap(SUMMARIZE, false), "budget", "media:budget;numeric", None);
        let budget = summarize.args.last_mut().unwrap();
        budget.required = false;
        budget.default_value = Some(serde_json::json!(400));
        let reg = registry(vec![summarize]);
        let notation = format!("{}[text -> summarize -> summary]", header("summarize", SUMMARIZE));
        let (_, req) = requirements(&notation, reg).await;
        let budget = arg_of(&req, SUMMARIZE, "budget");
        assert_eq!(budget.resolution, crate::planner::ArgumentResolution::HasDefault);
        assert_eq!(budget.default_value, Some(serde_json::json!(400)));
        assert_eq!(slot_names(&req), vec!["budget"]);
        assert!(req.can_execute_without_input);
    }

    // TEST1012: an unbound required arg with no default must be given by
    // the run: the plan cannot run without the user.
    #[tokio::test]
    async fn test1012_an_unbound_required_arg_without_default_needs_the_user() {
        let reg = registry(vec![with_arg(
            cap(SUMMARIZE, false),
            "instruction",
            "media:enc=utf-8;instruction",
            None,
        )]);
        let notation = format!("{}[text -> summarize -> summary]", header("summarize", SUMMARIZE));
        let (_, req) = requirements(&notation, reg).await;
        assert_eq!(
            arg_of(&req, SUMMARIZE, "instruction").resolution,
            crate::planner::ArgumentResolution::RequiresUserInput
        );
        assert_eq!(slot_names(&req), vec!["instruction"]);
        assert!(!req.can_execute_without_input);
    }

    // TEST1015: an unbound optional arg without a default is a slot the run
    // may leave empty: it does not stop the run going without the user.
    #[tokio::test]
    async fn test1015_an_unbound_optional_arg_without_default_does_not_block_the_run() {
        let mut summarize =
            with_arg(cap(SUMMARIZE, false), "style", "media:enc=utf-8;style", None);
        summarize.args.last_mut().unwrap().required = false;
        let reg = registry(vec![summarize]);
        let notation = format!("{}[text -> summarize -> summary]", header("summarize", SUMMARIZE));
        let (_, req) = requirements(&notation, reg).await;
        assert_eq!(
            arg_of(&req, SUMMARIZE, "style").resolution,
            crate::planner::ArgumentResolution::RequiresUserInput
        );
        assert_eq!(slot_names(&req), vec!["style"]);
        assert!(req.can_execute_without_input);
    }

    // TEST12851: a constant written in the machine binds its arg: the arg is
    // not a slot, and the run needs no value for it though it has no default.
    #[tokio::test]
    async fn test12851_a_constant_binds_its_arg_and_is_not_asked_of_the_run() {
        let reg = registry(vec![with_arg(
            cap(SUMMARIZE, false),
            "instruction",
            "media:enc=utf-8;instruction",
            None,
        )]);
        // A constant is held to its arg's media definition, which says it is
        // a string.
        reg.insert_cached_media_def_for_test(crate::fabric::registry::StoredMediaDef {
            version: 0,
            urn: "media:enc=utf-8;instruction".to_string(),
            media_type: "text/plain".to_string(),
            title: "Instruction".to_string(),
            profile_uri: None,
            schema: Some(serde_json::json!({"type": "string"})),
            description: None,
            documentation: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        });
        let notation = format!(
            "{}[(text, instruction=\"be brief\") -> summarize -> summary]",
            header("summarize", SUMMARIZE)
        );
        let (_, req) = requirements(&notation, reg).await;
        assert_eq!(
            arg_of(&req, SUMMARIZE, "instruction").resolution,
            crate::planner::ArgumentResolution::Constant
        );
        assert!(slot_names(&req).is_empty());
        assert!(req.can_execute_without_input);
    }

    // TEST12852: a source wired to a non-main arg by name binds it: the arg
    // is fed — from an input, or from a step — and is not a slot.
    #[tokio::test]
    async fn test12852_a_named_source_binds_a_non_main_arg() {
        let mut compare = with_arg(
            with_arg(
                cap(COMPARE, false),
                "benchmark",
                "media:benchmark;enc=utf-8",
                Some("media:enc=utf-8;ext=txt"),
            ),
            "threshold",
            "media:numeric;threshold",
            None,
        );
        compare.args.last_mut().unwrap().default_value = Some(serde_json::json!(0.5));
        let reg = registry(vec![cap(EXTRACT, false), compare]);
        let notation = format!(
            "{}{}[doc -> extract -> text][(text, benchmark=reference) -> compare -> result]",
            header("extract", EXTRACT),
            header("compare", COMPARE)
        );
        let (_, req) = requirements(&notation, reg).await;
        assert_eq!(
            arg_of(&req, COMPARE, "input").resolution,
            crate::planner::ArgumentResolution::FromStep
        );
        assert_eq!(
            arg_of(&req, COMPARE, "benchmark").resolution,
            crate::planner::ArgumentResolution::FromInput,
            "`reference` is an input of the machine"
        );
        assert_eq!(
            arg_of(&req, COMPARE, "threshold").resolution,
            crate::planner::ArgumentResolution::HasDefault,
            "the wire into `benchmark` binds that arg, not every other"
        );
        assert_eq!(slot_names(&req), vec!["threshold"]);
    }

    // TEST12854: a run says what type it binds to an input, and the plan
    // follows it: a step whose output is its input unchanged produces the
    // run's type, not the declared one. A type that does not conform to the
    // input's, and a name that is no input, are refused.
    #[tokio::test]
    async fn test12854_a_run_binds_a_more_specific_type_and_the_plan_follows_it() {
        const STRIP: &str = r#"cap:effect=none;in="media:image";strip-metadata;out="media:image""#;
        let reg = registry(vec![cap(STRIP, false)]);
        let notation = format!("{}[img -> strip -> clean]", header("strip", STRIP));
        let resolved = ResolvedNotation::resolve(&notation, reg).await.expect("resolves");
        let output_media = |plan: &MachinePlan| {
            let output = plan.output_nodes.first().expect("one output").clone();
            MediaUrn::from_string(
                &crate::planner::derive_output_media_urn(plan, &output).expect("derives"),
            )
            .unwrap()
        };

        let declared = resolved.plan(0, &RunInputs::declared()).await.expect("plans").plan;
        assert!(output_media(&declared)
            .is_equivalent(&MediaUrn::from_string("media:image").unwrap())
            .unwrap());

        let png = MediaUrn::from_string("media:ext=png;image").unwrap();
        let run = RunInputs {
            media: HashMap::from([("img".to_string(), png.clone())]),
            ..RunInputs::default()
        };
        let bound = resolved.plan(0, &run).await.expect("a png is an image").plan;
        assert!(
            output_media(&bound).is_equivalent(&png).unwrap(),
            "the step passes its input through: a png in is a png out"
        );

        let text = RunInputs {
            media: HashMap::from([(
                "img".to_string(),
                MediaUrn::from_string("media:enc=utf-8;ext=txt").unwrap(),
            )]),
            ..RunInputs::default()
        };
        let refused = resolved.plan(0, &text).await.err().expect("text is not an image");
        assert!(refused.to_string().contains("which takes"), "{refused}");

        let stray = RunInputs {
            media: HashMap::from([("nothing".to_string(), png)]),
            ..RunInputs::default()
        };
        let refused = resolved.plan(0, &stray).await.err().expect("no input is called that");
        assert!(refused.to_string().contains("no input named 'nothing'"), "{refused}");
    }

    // TEST12853: an arg reached through a map's item is fed from where the
    // map's sequence comes from: an input of the machine bound to a sequence
    // feeds each run of the body, and is not asked of the run.
    #[tokio::test]
    async fn test12853_an_item_of_an_input_feeds_from_the_input() {
        let reg = registry(vec![cap(TO_JPEG, false)]);
        let notation = format!("{}[pages -> convert -> jpeg]", header("convert", TO_JPEG));
        let resolved = ResolvedNotation::resolve(&notation, reg.clone()).await.expect("resolves");
        let plan = resolved
            .plan(
                0,
                &RunInputs { sequences: HashSet::from(["pages".to_string()]), ..RunInputs::default() },
            )
            .await
            .expect("plans")
            .plan;
        assert_eq!(maps(&plan).len(), 1, "a sequence into a scalar arg opens a map");
        let req = MachinePlanBuilder::new(reg).analyze_plan_arguments(&plan).await.unwrap();
        assert_eq!(
            arg_of(&req, TO_JPEG, "input").resolution,
            crate::planner::ArgumentResolution::FromInput
        );
        assert!(slot_names(&req).is_empty());
    }

    // TEST7104: the main input is found by tagged-URN equivalence with the
    // cap's `in=`, never by spelling: a stdin source whose URN writes the
    // tags in another order is still the main input, and exactly one arg is.
    #[tokio::test]
    async fn test7104_the_main_input_is_found_by_equivalence_not_spelling() {
        let mut summarize = cap(SUMMARIZE, false);
        summarize.args[0].sources = vec![ArgSource::Stdin {
            stdin: "media:ext=txt;enc=utf-8".to_string(),
        }];
        let mut summarize =
            with_arg(summarize, "instruction", "media:enc=utf-8;instruction", None);
        // Where an arg is written says nothing of what it is: the main input
        // is not the first.
        summarize.args.rotate_right(1);
        assert_eq!(summarize.args[0].name, "instruction");
        let reg = registry(vec![summarize]);
        let notation = format!("{}[text -> summarize -> summary]", header("summarize", SUMMARIZE));
        let (_, req) = requirements(&notation, reg).await;
        let main: Vec<&str> = req.steps[0]
            .arguments
            .iter()
            .filter(|a| a.is_main_input)
            .map(|a| a.name.as_str())
            .collect();
        assert_eq!(main, vec!["input"]);
        assert_eq!(
            arg_of(&req, SUMMARIZE, "input").resolution,
            crate::planner::ArgumentResolution::FromInput
        );
    }

    // TEST1461: requirements are addressed by the plan's own tokens: two
    // steps of one cap carry the two distinct tokens of their cap nodes.
    #[tokio::test]
    async fn test1461_step_requirements_carry_the_plans_own_tokens() {
        let rewrite = r#"cap:in="media:enc=utf-8;ext=txt";rewrite;out="media:enc=utf-8;ext=txt""#;
        let reg = registry(vec![cap(rewrite, false)]);
        let notation = format!(
            "{}[a -> rewrite -> b][b -> rewrite -> c]",
            header("rewrite", rewrite)
        );
        let (plan, req) = requirements(&notation, reg).await;
        let mut plan_tokens: Vec<String> = plan
            .nodes
            .values()
            .filter_map(|n| match &n.node_type {
                ExecutionNodeType::Cap { token_id, .. } => Some(token_id.to_string()),
                _ => None,
            })
            .collect();
        plan_tokens.sort();
        let mut requirement_tokens: Vec<String> =
            req.steps.iter().map(|s| s.token_id.to_string()).collect();
        requirement_tokens.sort();
        assert_eq!(plan_tokens.len(), 2);
        assert_ne!(plan_tokens[0], plan_tokens[1], "two steps, two tokens");
        assert_eq!(requirement_tokens, plan_tokens);
    }

    /// The plan of strand 0 of `notation`, resolved afresh — new tokens each
    /// time, as every resolution mints its own.
    async fn resolved_plan(notation: &str, reg: Arc<FabricRegistry>, inputs: &RunInputs) -> StrandPlan {
        ResolvedNotation::resolve(notation, reg)
            .await
            .unwrap_or_else(|e| panic!("{notation}: {e}"))
            .plan(0, inputs)
            .await
            .unwrap_or_else(|e| panic!("{notation}: {e}"))
    }

    /// The token of every step and map of a plan.
    fn all_tokens(plan: &MachinePlan) -> HashSet<String> {
        plan.nodes
            .values()
            .filter_map(|node| match &node.node_type {
                ExecutionNodeType::Cap { token_id, .. } | ExecutionNodeType::ForEach { token_id, .. } => {
                    Some(token_id.to_string())
                }
                _ => None,
            })
            .collect()
    }

    // TEST12882: a plan a host was shown and hands back is checked against
    // what the machine resolves to now. Two resolutions of one machine share
    // no token, and each is a resolution of the other — through a map, an
    // output that is not the main one, and a step the map runs.
    #[tokio::test]
    async fn test12882_a_plan_handed_back_is_a_resolution_of_its_machine() {
        let reg = registry(vec![
            cap(RENDER, true),
            with_output(cap(TO_JPEG, false), "thumb", "media:ext=png;image;thumbnail", false),
        ]);
        let notation = format!(
            "{}{}[doc -> render -> pages][pages -> to_jpeg -> (jpegs, thumb=small)]",
            header("render", RENDER),
            header("to_jpeg", TO_JPEG)
        );
        let shown = resolved_plan(&notation, reg.clone(), &RunInputs::declared()).await;
        let now = resolved_plan(&notation, reg, &RunInputs::declared()).await;
        assert!(
            all_tokens(&shown.plan).is_disjoint(&all_tokens(&now.plan)),
            "every resolution mints its own tokens"
        );
        assert!(!maps(&shown.plan).is_empty(), "the page sequence is mapped, so a map token is paired too");
        shown.check_is_a_resolution_of(&now).expect("the plan shown is the machine's");
        now.check_is_a_resolution_of(&shown).expect("and the other way round");

        // It survives the wire: a plan read back is still the machine's.
        let carried: StrandPlan = serde_json::from_str(&serde_json::to_string(&shown).unwrap()).unwrap();
        carried.check_is_a_resolution_of(&now).expect("a plan carried as text is the same plan");
    }

    // TEST12883: a plan that is not what the machine resolves to now is
    // refused, saying where it differs — another cap at a step, another
    // policy, a step too many, a step whose node is not its token.
    #[tokio::test]
    async fn test12883_a_plan_that_is_not_the_machines_is_refused() {
        let reg = registry(vec![cap(EXTRACT, false), cap(RENDER, false), cap(SUMMARIZE, false)]);
        let render = format!("{}[doc -> render -> out]", header("render", RENDER));
        let extract = format!("{}[doc -> extract -> out]", header("extract", EXTRACT));
        let other_cap = resolved_plan(&render, reg.clone(), &RunInputs::declared()).await;
        let now = resolved_plan(&extract, reg.clone(), &RunInputs::declared()).await;
        let refused = other_cap.check_is_a_resolution_of(&now).expect_err("another cap at the step 'out'");
        assert!(refused.reason.contains("cap_urn"), "it says where: {refused}");

        let retried = format!("{}[doc -> extract{{retry=2}} -> out]", header("extract", EXTRACT));
        let other_policy = resolved_plan(&retried, reg.clone(), &RunInputs::declared()).await;
        let refused = other_policy.check_is_a_resolution_of(&now).expect_err("another policy");
        assert!(refused.reason.contains("/policy"), "it says where: {refused}");

        let longer = format!(
            "{}{}[doc -> extract -> out][out -> summarize -> s]",
            header("extract", EXTRACT),
            header("summarize", SUMMARIZE)
        );
        let more = resolved_plan(&longer, reg.clone(), &RunInputs::declared()).await;
        let refused = more.check_is_a_resolution_of(&now).expect_err("a step too many");
        assert!(refused.reason.contains("2 steps"), "{refused}");

        // A step whose node and token disagree addresses one step by two names.
        let mut forged = resolved_plan(&extract, reg, &RunInputs::declared()).await;
        let step = step_of(&forged.plan, EXTRACT);
        if let ExecutionNodeType::Cap { token_id, .. } = &mut forged.plan.nodes.get_mut(&step).unwrap().node_type {
            *token_id = crate::planner::StepToken::mint();
        }
        let refused = forged.check_is_a_resolution_of(&now).expect_err("a forged token");
        assert!(refused.reason.contains("is not the node's"), "{refused}");
    }

    // TEST12884: steps that hold no value — sinks — have no name to be paired
    // by; they are paired by everything else they are. Two alike in everything
    // are interchangeable, and a plan with them is still its machine's.
    #[tokio::test]
    async fn test12884_steps_that_hold_no_value_are_paired_by_what_they_are() {
        let mut publish = cap(PUBLISH, false);
        publish.outputs.clear();
        let reg = registry(vec![cap(NOW, false), publish]);
        let notation = format!(
            "{}{}[() -> now -> stamp][stamp -> publish -> ()][stamp -> publish -> ()]",
            header("now", NOW),
            header("publish", PUBLISH)
        );
        let shown = resolved_plan(&notation, reg.clone(), &RunInputs::declared()).await;
        let now = resolved_plan(&notation, reg.clone(), &RunInputs::declared()).await;
        shown.check_is_a_resolution_of(&now).expect("two alike sinks pair either way");

        // One sink fewer is another machine.
        let one = format!(
            "{}{}[() -> now -> stamp][stamp -> publish -> ()]",
            header("now", NOW),
            header("publish", PUBLISH)
        );
        let fewer = resolved_plan(&one, reg, &RunInputs::declared()).await;
        assert!(fewer.check_is_a_resolution_of(&now).is_err());
    }

    // TEST12885: a plan is checked against the plan the SAME run inputs make.
    // The plan of one value through a step is not the plan of a sequence
    // through it (the sequence is mapped), so one is refused as the other.
    #[tokio::test]
    async fn test12885_a_plan_is_the_resolution_for_its_own_run_inputs() {
        let reg = registry(vec![cap(TO_JPEG, false)]);
        let notation = format!("{}[png -> to_jpeg -> jpeg]", header("to_jpeg", TO_JPEG));
        let one = resolved_plan(&notation, reg.clone(), &RunInputs::declared()).await;
        let many_inputs = RunInputs { sequences: HashSet::from(["png".to_string()]), media: HashMap::new() };
        let many = resolved_plan(&notation, reg.clone(), &many_inputs).await;
        let many_again = resolved_plan(&notation, reg, &many_inputs).await;
        many.check_is_a_resolution_of(&many_again).expect("the same run inputs, the same plan");
        assert!(one.check_is_a_resolution_of(&many).is_err(), "one value is not a sequence");
    }
}

#[cfg(test)]
mod anchor_cardinality_tests {
    use super::tests::{cap, registry};
    use crate::machine::parse_machine_with_node_names_async;
    use crate::planner::{AnchorInput, ExecutionNodeType, InputCardinality, MachinePlanBuilder};
    use std::collections::HashMap;

    // TEST1446: per-anchor cardinality — a Sequence anchor feeding a scalar
    // entry cap gets its per-file map; a Single anchor beside it does not.
    #[tokio::test]
    async fn test1446_per_anchor_cardinality_drives_foreach() {
        let mut concat = cap(
            r#"cap:in="media:enc=utf-8";concat;out="media:enc=utf-8;ext=txt""#,
            false,
        );
        concat.args[0].is_sequence = true;
        let reg = registry(vec![
            cap(
                r#"cap:in="media:document;ext=pdf";op-a;out="media:enc=utf-8;page""#,
                false,
            ),
            cap(
                r#"cap:in="media:ext=md";op-b;out="media:enc=utf-8;page""#,
                false,
            ),
            concat,
        ]);
        let notation = concat!(
            r#"[a cap:in="media:document;ext=pdf";op-a;out="media:enc=utf-8;page"]"#,
            r#"[b cap:in="media:ext=md";op-b;out="media:enc=utf-8;page"]"#,
            r#"[cat cap:in="media:enc=utf-8";concat;out="media:enc=utf-8;ext=txt"]"#,
            "[doc_a -> a -> x]",
            "[doc_b -> b -> y]",
            "[(x, y) -> cat -> z]",
        );
        let (machine, _) = parse_machine_with_node_names_async(notation, &reg)
            .await
            .expect("machine parses");
        let strand = &machine.strands()[0];

        // pdf anchor: Sequence (a 3-file batch); md anchor: Single.
        let mut inputs: HashMap<crate::machine::NodeId, AnchorInput> = HashMap::new();
        for &anchor in strand.input_anchor_ids() {
            let urn = strand.node_urn(anchor);
            let cardinality = if urn.to_string().contains("pdf") {
                InputCardinality::Sequence
            } else {
                InputCardinality::Single
            };
            inputs.insert(anchor, AnchorInput { cardinality, media: urn.clone() });
        }

        let builder = MachinePlanBuilder::new(reg.clone());
        let plan = builder
            .build_plan_from_machine_strand_for_run("mixed anchors", strand, &inputs)
            .await
            .expect("mixed-cardinality plan builds");
        plan.validate().expect("valid");

        // Exactly ONE map, over the pdf slot, and its body is op-a.
        let maps: Vec<(&Vec<crate::planner::ForEachSource>, &Vec<String>)> = plan
            .nodes
            .values()
            .filter_map(|n| match &n.node_type {
                ExecutionNodeType::ForEach { sources, body, .. } => Some((sources, body)),
                _ => None,
            })
            .collect();
        assert_eq!(maps.len(), 1, "only the Sequence anchor maps per file");
        let (sources, body) = maps[0];
        assert_eq!(sources.len(), 1);
        assert!(matches!(
            &plan.nodes[&sources[0].node].node_type,
            ExecutionNodeType::InputSlot { expected_media_urn, .. } if expected_media_urn.contains("pdf")
        ));
        let body_caps: Vec<&str> = body
            .iter()
            .filter_map(|member| plan.nodes[member].cap_urn())
            .collect();
        assert_eq!(body_caps.len(), 1);
        assert!(body_caps[0].contains("op-a"), "the pdf leg maps per file, got {}", body_caps[0]);

        // The gather runs once: it reads the mapped leg as the sequence the
        // map collected, beside the single value of the other leg.
        let concat = plan
            .nodes
            .iter()
            .find(|(_, n)| n.cap_urn().is_some_and(|urn| urn.contains("concat")))
            .map(|(id, _)| id.clone())
            .unwrap();
        assert!(plan.enclosing_maps().get(concat.as_str()).is_none());

        // Slot cardinalities recorded per anchor.
        let slot_cardinalities: Vec<(String, InputCardinality)> = plan
            .nodes
            .values()
            .filter_map(|n| match &n.node_type {
                ExecutionNodeType::InputSlot {
                    expected_media_urn,
                    cardinality,
                    ..
                } => Some((expected_media_urn.clone(), *cardinality)),
                _ => None,
            })
            .collect();
        assert_eq!(slot_cardinalities.len(), 2);
        for (urn, cardinality) in slot_cardinalities {
            if urn.contains("pdf") {
                assert_eq!(cardinality, InputCardinality::Sequence);
            } else {
                assert_eq!(cardinality, InputCardinality::Single);
            }
        }
    }

}
