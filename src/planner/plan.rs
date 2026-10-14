//! Cap Execution Plan structures
//!
//! Defines the structured execution plan for machines.
//! This plan describes a DAG of caps to execute, with data flow between them.
//!
//! The execution model supports:
//! - Linear chains of caps
//! - Parallel branches (diamond patterns)
//! - Fan-out (ForEach) for sequence -> single transitions
//! - Fan-in (Collect) for gathering results back into sequences

use crate::machine::StepPolicy;
use super::argument_binding::ArgumentBindings;
use super::cardinality::InputCardinality;
use super::live_cap_fab::{StepToken, Strand, StrandStep, StrandStepType};
use super::PlannerError;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use uuid::Uuid;

/// Unique identifier for a node in the execution plan
pub type NodeId = String;

/// Node type in the execution DAG
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "node_type", rename_all = "snake_case")]
pub enum ExecutionNodeType {
    /// Execute a single cap
    Cap {
        /// Stable per-step identity (the capdag `StrandStep.token_id`). This is the
        /// canonical, alias-free key by which a run's progress updates map back to the
        /// exact graph element the client rendered. It is minted once at the strand's
        /// source and threaded verbatim through the plan → resolved graph → progress.
        token_id: StepToken,
        /// The cap URN to execute
        cap_urn: String,
        /// Argument bindings for this cap
        #[serde(default)]
        arg_bindings: ArgumentBindings,
        /// Optional preferred cap URN for routing.
        /// When set, the RelaySwitch uses `is_comparable` matching and prefers
        /// the master whose registered cap is equivalent to this URN.
        /// When absent, standard `accepts` + closest-specificity routing.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        preferred_cap: Option<String>,
        /// What the step does when it fails: how many further attempts a
        /// retryable failure gets, and whether a failure that stands leaves
        /// the step's outputs absent instead of failing the run. The default
        /// is to fail at once.
        #[serde(default, skip_serializing_if = "StepPolicy::is_default")]
        policy: StepPolicy,
    },

    /// One output of a step other than its main one, as a node of its own.
    ///
    /// A step's main output is the step's node. Every other output something
    /// takes is one of these: what consumes it wires from this node, and its
    /// value in a run is stored under this node's id.
    CapOutput {
        /// The step whose output this is.
        cap_node: NodeId,
        /// The output's media URN in the cap's definition: its identity, and
        /// what its response stream names.
        output_media_urn: String,
        /// The output's name in the cap's definition (a label; unique per cap).
        output_name: String,
        /// The output carries the step's input, type unchanged, rather than
        /// its own URN (a router's outputs).
        #[serde(default)]
        carries_input: bool,
    },

    /// A map: its body runs once per item of its sources, zipped.
    ///
    /// Opened where a sequence is bound to an arg that takes one value
    /// (docs 09 §Iteration). Run `i` of the body is given item `i` of every
    /// source; sources of unequal length fail the run. What a body node
    /// produces is, from outside the map, the sequence of it over the runs in
    /// item order — one flat sequence when the map is inside another.
    ForEach {
        /// Stable identity of the map. The run's aggregate per-item progress is
        /// reported under it, so it maps to the graph element a client renders.
        token_id: StepToken,
        /// The sequences the map zips, each with the node the body is given
        /// its item as. At least one.
        sources: Vec<ForEachSource>,
        /// Every node that runs once per item: the item nodes, the steps, the
        /// outputs and gathers that belong to them, and the maps opened inside
        /// this one (whose own bodies are theirs, not listed here).
        body: Vec<NodeId>,
    },

    /// What one run of a map's body is given of one of the map's sources.
    Item {
        /// The map this is an item of.
        foreach: NodeId,
        /// The source node whose item it is.
        source: NodeId,
        /// The item's media URN: the element type of the source sequence.
        media_urn: String,
    },

    /// A gather: several values delivered to one sequence arg as one
    /// sequence, in order. A single value contributes one item, a sequence its
    /// items; a member that is absent contributes none.
    Collect {
        /// The nodes gathered, in the order they are delivered.
        input_nodes: Vec<NodeId>,
        /// The element type of the gathered sequence.
        output_media_urn: String,
    },

    /// Input slot: entry point for user-provided files
    InputSlot {
        /// Name of this input slot
        slot_name: String,
        /// Expected media URN for inputs
        expected_media_urn: String,
        /// Expected cardinality
        cardinality: InputCardinality,
    },

    /// Output: terminal node marking a result
    Output {
        /// Name of this output
        output_name: String,
        /// Source node for this output
        source_node: NodeId,
    },
}

/// One sequence a map iterates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForEachSource {
    /// The node that holds the sequence, as the map's own scope sees it.
    pub node: NodeId,
    /// The [`ExecutionNodeType::Item`] node the body is given each item as.
    pub item: NodeId,
}

/// A node in the execution DAG
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MachineNode {
    /// Unique identifier for this node
    pub id: NodeId,
    /// The type of node and its specific configuration
    pub node_type: ExecutionNodeType,
    /// Optional description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl MachineNode {
    /// Create a cap execution node.
    ///
    /// Mints a fresh `token_id`: this constructor builds cap nodes for plans that
    /// are NOT derived from a resolved strand (notation-derived demos, tests). A run
    /// whose progress must map back to a client-rendered strand uses
    /// [`MachineNode::cap_with_bindings_token`] to preserve the strand's identity.
    pub fn cap(id: &str, cap_urn: &str) -> Self {
        Self {
            id: id.to_string(),
            node_type: ExecutionNodeType::Cap {
                token_id: StepToken::mint(),
                cap_urn: cap_urn.to_string(),
                arg_bindings: ArgumentBindings::new(),
                preferred_cap: None,
                policy: StepPolicy::default(),
            },
            description: None,
        }
    }

    /// Create a cap node with argument bindings.
    ///
    /// Mints a fresh `token_id`. See [`MachineNode::cap`] for when identity is minted
    /// vs preserved; the run path uses [`MachineNode::cap_with_bindings_token`].
    pub fn cap_with_bindings(id: &str, cap_urn: &str, bindings: ArgumentBindings) -> Self {
        Self {
            id: id.to_string(),
            node_type: ExecutionNodeType::Cap {
                token_id: StepToken::mint(),
                cap_urn: cap_urn.to_string(),
                arg_bindings: bindings,
                preferred_cap: None,
                policy: StepPolicy::default(),
            },
            description: None,
        }
    }

    /// Create a cap node with argument bindings, preserving a strand step's identity.
    ///
    /// Used by the run path (`build_plan_from_path`) so the plan's cap node carries the
    /// exact `token_id` minted on the originating `StrandStep`. This is what makes a
    /// run's live progress updates map deterministically to the client's graph element.
    pub fn cap_with_bindings_token(
        id: &str,
        cap_urn: &str,
        bindings: ArgumentBindings,
        token_id: StepToken,
    ) -> Self {
        Self {
            id: id.to_string(),
            node_type: ExecutionNodeType::Cap {
                token_id,
                cap_urn: cap_urn.to_string(),
                arg_bindings: bindings,
                preferred_cap: None,
                policy: StepPolicy::default(),
            },
            description: None,
        }
    }

    /// Create a cap node with argument bindings and routing preference
    pub fn cap_with_preference(
        id: &str,
        cap_urn: &str,
        bindings: ArgumentBindings,
        preferred_cap: Option<String>,
    ) -> Self {
        Self {
            id: id.to_string(),
            node_type: ExecutionNodeType::Cap {
                token_id: StepToken::mint(),
                cap_urn: cap_urn.to_string(),
                arg_bindings: bindings,
                preferred_cap,
                policy: StepPolicy::default(),
            },
            description: None,
        }
    }

    /// Create a map node. Mints a fresh `token_id`; a run whose progress must
    /// map back to a rendered strand uses [`MachineNode::for_each_token`].
    pub fn for_each(id: &str, sources: Vec<ForEachSource>, body: Vec<NodeId>) -> Self {
        Self::for_each_token(id, sources, body, StepToken::mint())
    }

    /// Create a map node, preserving the originating step's identity.
    pub fn for_each_token(
        id: &str,
        sources: Vec<ForEachSource>,
        body: Vec<NodeId>,
        token_id: StepToken,
    ) -> Self {
        Self {
            id: id.to_string(),
            node_type: ExecutionNodeType::ForEach {
                token_id,
                sources,
                body,
            },
            description: Some("Map: run the body once per item".to_string()),
        }
    }

    /// Create the node a map's body is given one source's item as.
    pub fn item(id: &str, foreach: &str, source: &str, media_urn: &str) -> Self {
        Self {
            id: id.to_string(),
            node_type: ExecutionNodeType::Item {
                foreach: foreach.to_string(),
                source: source.to_string(),
                media_urn: media_urn.to_string(),
            },
            description: Some(format!("Item of {source}")),
        }
    }

    /// Create the node of an output of `cap_node` other than its main one.
    pub fn cap_output(
        id: &str,
        cap_node: &str,
        output_media_urn: &str,
        output_name: &str,
        carries_input: bool,
    ) -> Self {
        Self {
            id: id.to_string(),
            node_type: ExecutionNodeType::CapOutput {
                cap_node: cap_node.to_string(),
                output_media_urn: output_media_urn.to_string(),
                output_name: output_name.to_string(),
                carries_input,
            },
            description: Some(format!("Output: {output_name}")),
        }
    }

    /// Create a gather node.
    pub fn collect(id: &str, input_nodes: Vec<String>, output_media_urn: &str) -> Self {
        Self {
            id: id.to_string(),
            node_type: ExecutionNodeType::Collect {
                input_nodes,
                output_media_urn: output_media_urn.to_string(),
            },
            description: Some("Gather: several values into one sequence".to_string()),
        }
    }

    /// Create an input slot node
    pub fn input_slot(
        id: &str,
        slot_name: &str,
        media_urn: &str,
        cardinality: InputCardinality,
    ) -> Self {
        Self {
            id: id.to_string(),
            node_type: ExecutionNodeType::InputSlot {
                slot_name: slot_name.to_string(),
                expected_media_urn: media_urn.to_string(),
                cardinality,
            },
            description: Some(format!("Input: {}", slot_name)),
        }
    }

    /// Create an output node
    pub fn output(id: &str, output_name: &str, source_node: &str) -> Self {
        Self {
            id: id.to_string(),
            node_type: ExecutionNodeType::Output {
                output_name: output_name.to_string(),
                source_node: source_node.to_string(),
            },
            description: Some(format!("Output: {}", output_name)),
        }
    }

    /// Check if this is a cap execution node
    pub fn is_cap(&self) -> bool {
        matches!(self.node_type, ExecutionNodeType::Cap { .. })
    }

    /// Check if this is a fan-out node
    pub fn is_fan_out(&self) -> bool {
        matches!(self.node_type, ExecutionNodeType::ForEach { .. })
    }

    /// Check if this is a fan-in node
    pub fn is_fan_in(&self) -> bool {
        matches!(self.node_type, ExecutionNodeType::Collect { .. })
    }

    /// Get cap URN if this is a cap node
    pub fn cap_urn(&self) -> Option<&str> {
        match &self.node_type {
            ExecutionNodeType::Cap { cap_urn, .. } => Some(cap_urn),
            _ => None,
        }
    }

    /// Get preferred cap URN if this is a cap node with a routing preference
    pub fn preferred_cap(&self) -> Option<&str> {
        match &self.node_type {
            ExecutionNodeType::Cap { preferred_cap, .. } => preferred_cap.as_deref(),
            _ => None,
        }
    }
}

/// Edge type for execution plans
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeType {
    /// Into a step: its MAIN input. Into any other node: what that node is
    /// of — a map's source, an output node's step, a plan output's source.
    Direct,
    /// Into a specific NON-main argument of the target step, identified by the
    /// URN that argument's stream is delivered under.
    Arg { arg_urn: String },
    /// From a map to one of its item nodes.
    Iteration,
    /// From a gathered node into its gather.
    Collection,
}

impl Default for EdgeType {
    fn default() -> Self {
        Self::Direct
    }
}

/// An edge in the execution plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MachinePlanEdge {
    /// Source node
    pub from_node: NodeId,
    /// Target node
    pub to_node: NodeId,
    /// Type of data flow
    #[serde(default)]
    pub edge_type: EdgeType,
}

impl MachinePlanEdge {
    /// Create a direct edge
    pub fn direct(from: &str, to: &str) -> Self {
        Self {
            from_node: from.to_string(),
            to_node: to.to_string(),
            edge_type: EdgeType::Direct,
        }
    }

    /// Create an iteration edge (ForEach -> body)
    pub fn iteration(from: &str, to: &str) -> Self {
        Self {
            from_node: from.to_string(),
            to_node: to.to_string(),
            edge_type: EdgeType::Iteration,
        }
    }

    /// Create a collection edge (body -> Collect)
    pub fn collection(from: &str, to: &str) -> Self {
        Self {
            from_node: from.to_string(),
            to_node: to.to_string(),
            edge_type: EdgeType::Collection,
        }
    }

    /// Create a convergence edge feeding a specific non-main argument (by its media
    /// URN) of the target cap.
    pub fn arg(from: &str, to: &str, arg_urn: &str) -> Self {
        Self {
            from_node: from.to_string(),
            to_node: to.to_string(),
            edge_type: EdgeType::Arg {
                arg_urn: arg_urn.to_string(),
            },
        }
    }
}

/// The structured execution plan for a machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MachinePlan {
    /// Human-readable name for this execution plan
    pub name: String,

    /// All nodes in the DAG
    pub nodes: HashMap<NodeId, MachineNode>,

    /// Edges describing data flow
    pub edges: Vec<MachinePlanEdge>,

    /// Entry point nodes (InputSlots)
    pub entry_nodes: Vec<NodeId>,

    /// Output nodes
    pub output_nodes: Vec<NodeId>,

    /// Plan metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

impl MachinePlan {
    /// Create an empty plan
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            nodes: HashMap::new(),
            edges: Vec::new(),
            entry_nodes: Vec::new(),
            output_nodes: Vec::new(),
            metadata: None,
        }
    }

    /// Add a node to the plan
    pub fn add_node(&mut self, node: MachineNode) {
        let id = node.id.clone();

        // Track entry/output nodes
        match &node.node_type {
            ExecutionNodeType::InputSlot { .. } => {
                self.entry_nodes.push(id.clone());
            }
            ExecutionNodeType::Output { .. } => {
                self.output_nodes.push(id.clone());
            }
            _ => {}
        }

        self.nodes.insert(id, node);
    }

    /// Add an edge to the plan
    pub fn add_edge(&mut self, edge: MachinePlanEdge) {
        self.edges.push(edge);
    }

    /// Get a node by ID
    pub fn get_node(&self, id: &str) -> Option<&MachineNode> {
        self.nodes.get(id)
    }

    /// Validate the plan structure
    pub fn validate(&self) -> Result<(), PlannerError> {
        // Check all edge references exist
        for edge in &self.edges {
            if !self.nodes.contains_key(&edge.from_node) {
                return Err(PlannerError::Internal(format!(
                    "Edge from_node '{}' not found in plan",
                    edge.from_node
                )));
            }
            if !self.nodes.contains_key(&edge.to_node) {
                return Err(PlannerError::Internal(format!(
                    "Edge to_node '{}' not found in plan",
                    edge.to_node
                )));
            }
        }

        // Check entry nodes exist
        for entry in &self.entry_nodes {
            if !self.nodes.contains_key(entry) {
                return Err(PlannerError::Internal(format!(
                    "Entry node '{}' not found in plan",
                    entry
                )));
            }
        }

        // Check output nodes exist
        for output in &self.output_nodes {
            if !self.nodes.contains_key(output) {
                return Err(PlannerError::Internal(format!(
                    "Output node '{}' not found in plan",
                    output
                )));
            }
        }

        // What a node names must be there, and be what it is named as.
        let known = |owner: &str, what: &str, id: &str| {
            if self.nodes.contains_key(id) {
                Ok(())
            } else {
                Err(PlannerError::Internal(format!(
                    "node '{owner}' names {what} '{id}', which is not in the plan"
                )))
            }
        };
        let mut in_body: HashMap<&str, &str> = HashMap::new();
        // In node-id order, so the same plan is refused the same way every time.
        let mut ids: Vec<&NodeId> = self.nodes.keys().collect();
        ids.sort();
        for id in ids {
            let node = &self.nodes[id];
            match &node.node_type {
                ExecutionNodeType::Cap { .. } | ExecutionNodeType::InputSlot { .. } => {}
                ExecutionNodeType::Output { source_node, .. } => {
                    known(id, "source", source_node)?;
                }
                ExecutionNodeType::Collect { input_nodes, .. } => {
                    for input in input_nodes {
                        known(id, "gathered node", input)?;
                    }
                }
                ExecutionNodeType::CapOutput { cap_node, .. } => {
                    known(id, "step", cap_node)?;
                    if !self.nodes[cap_node].is_cap() {
                        return Err(PlannerError::Internal(format!(
                            "output node '{id}' names '{cap_node}' as its step, which is not a step"
                        )));
                    }
                }
                ExecutionNodeType::Item { foreach, source, .. } => {
                    known(id, "map", foreach)?;
                    let listed = match &self.nodes[foreach].node_type {
                        ExecutionNodeType::ForEach { sources, .. } => sources
                            .iter()
                            .any(|s| &s.item == id && &s.node == source),
                        _ => false,
                    };
                    if !listed {
                        return Err(PlannerError::Internal(format!(
                            "item node '{id}' names map '{foreach}' and source '{source}', and \
                             that map does not list it so"
                        )));
                    }
                }
                ExecutionNodeType::ForEach { sources, body, .. } => {
                    if sources.is_empty() {
                        return Err(PlannerError::Internal(format!(
                            "map '{id}' iterates nothing"
                        )));
                    }
                    for source in sources {
                        known(id, "source", &source.node)?;
                        known(id, "item", &source.item)?;
                        let is_item = matches!(
                            &self.nodes[&source.item].node_type,
                            ExecutionNodeType::Item { foreach, source: of, .. }
                                if foreach == id && of == &source.node
                        );
                        if !is_item {
                            return Err(PlannerError::Internal(format!(
                                "map '{id}' lists '{}' as its item of '{}', which is not that",
                                source.item, source.node
                            )));
                        }
                        if !body.contains(&source.item) {
                            return Err(PlannerError::Internal(format!(
                                "map '{id}' does not list its item '{}' in its body",
                                source.item
                            )));
                        }
                    }
                    for member in body {
                        known(id, "body node", member)?;
                        if member == id {
                            return Err(PlannerError::Internal(format!(
                                "map '{id}' lists itself in its body"
                            )));
                        }
                        if let Some(other) = in_body.insert(member.as_str(), id.as_str()) {
                            return Err(PlannerError::Internal(format!(
                                "node '{member}' is in the body of two maps ('{other}' and '{id}')"
                            )));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// The map each node runs in: node id → the id of the map whose body
    /// lists it. A node of the machine itself has no entry.
    pub fn enclosing_maps(&self) -> HashMap<&str, &str> {
        let mut enclosing: HashMap<&str, &str> = HashMap::new();
        for (id, node) in &self.nodes {
            if let ExecutionNodeType::ForEach { body, .. } = &node.node_type {
                for member in body {
                    enclosing.insert(member.as_str(), id.as_str());
                }
            }
        }
        enclosing
    }

    /// Every node inside map `foreach_id`: its body, and the bodies of the
    /// maps opened inside it.
    pub fn map_members(&self, foreach_id: &str) -> HashSet<String> {
        let mut members: HashSet<String> = HashSet::new();
        let mut pending: Vec<&str> = vec![foreach_id];
        while let Some(id) = pending.pop() {
            if let Some(ExecutionNodeType::ForEach { body, .. }) =
                self.nodes.get(id).map(|n| &n.node_type)
            {
                for member in body {
                    if members.insert(member.clone()) {
                        pending.push(member.as_str());
                    }
                }
            }
        }
        members
    }

    /// Get topological ordering of nodes
    pub fn topological_order(&self) -> Result<Vec<&MachineNode>, PlannerError> {
        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();

        for (id, _) in &self.nodes {
            in_degree.entry(id.as_str()).or_insert(0);
            adj.entry(id.as_str()).or_insert_with(Vec::new);
        }

        for edge in &self.edges {
            *in_degree.entry(edge.to_node.as_str()).or_insert(0) += 1;
            adj.entry(edge.from_node.as_str())
                .or_insert_with(Vec::new)
                .push(edge.to_node.as_str());
        }

        let mut queue: VecDeque<&str> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(&id, _)| id)
            .collect();

        let mut result: Vec<&MachineNode> = Vec::new();

        while let Some(node_id) = queue.pop_front() {
            if let Some(node) = self.nodes.get(node_id) {
                result.push(node);
            }

            if let Some(neighbors) = adj.get(node_id) {
                for &neighbor in neighbors {
                    if let Some(deg) = in_degree.get_mut(neighbor) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(neighbor);
                        }
                    }
                }
            }
        }

        if result.len() != self.nodes.len() {
            return Err(PlannerError::Internal(
                "Cycle detected in execution plan".to_string(),
            ));
        }

        Ok(result)
    }

    /// Return the exact strand this plan executes.
    ///
    /// Structural ForEach steps already present in `strand` must carry the same
    /// identity as the plan. A ForEach introduced solely by run input cardinality
    /// is inserted immediately before its body-entry cap using the identity minted
    /// by plan construction. Persisting this value keeps run provenance and the
    /// rendered path on the same graph-address space.
    pub fn executed_strand(&self, strand: &Strand) -> Result<Strand, PlannerError> {
        let mut cap_positions: HashMap<&str, usize> = HashMap::new();
        for (index, step) in strand.steps.iter().enumerate() {
            if step.is_cap()
                && cap_positions
                    .insert(step.token_id.as_str(), index)
                    .is_some()
            {
                return Err(PlannerError::InvalidPath(format!(
                    "resolved strand contains duplicate cap token_id '{}'",
                    step.token_id
                )));
            }
        }

        let mut boundaries: Vec<(usize, StepToken, StepToken)> = Vec::new();
        for node in self.nodes.values() {
            let ExecutionNodeType::ForEach {
                token_id, sources, ..
            } = &node.node_type
            else {
                continue;
            };
            // The step a map's item is given to is where the map begins on
            // the strand: the earliest such step, when several take it.
            let mut entry: Option<(usize, &StepToken)> = None;
            for edge in &self.edges {
                if !sources.iter().any(|source| source.item == edge.from_node) {
                    continue;
                }
                let Some(ExecutionNodeType::Cap {
                    token_id: cap_token_id,
                    ..
                }) = self.nodes.get(&edge.to_node).map(|n| &n.node_type)
                else {
                    continue;
                };
                let position = *cap_positions.get(cap_token_id.as_str()).ok_or_else(|| {
                    PlannerError::InvalidPath(format!(
                        "ForEach '{}' body-entry cap '{}' is absent from the resolved strand",
                        token_id, cap_token_id
                    ))
                })?;
                if entry.map_or(true, |(best, _)| position < best) {
                    entry = Some((position, cap_token_id));
                }
            }
            let (position, cap_token_id) = entry.ok_or_else(|| {
                PlannerError::InvalidPath(format!(
                    "ForEach '{}' gives its item to no capability step",
                    token_id
                ))
            })?;
            boundaries.push((position, token_id.clone(), cap_token_id.clone()));
        }
        boundaries.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        let plan_boundary_tokens: HashSet<&str> = boundaries
            .iter()
            .map(|(_, token, _)| token.as_str())
            .collect();
        for step in &strand.steps {
            if matches!(step.step_type, StrandStepType::ForEach { .. })
                && !plan_boundary_tokens.contains(step.token_id.as_str())
            {
                // Name BOTH sides of the disagreement. A boundary identity that
                // exists on one side only is almost always a rename, not a
                // deletion, and the operator can only see that if the message
                // carries the identities the plan actually uses.
                let mut plan_tokens: Vec<&str> = plan_boundary_tokens.iter().copied().collect();
                plan_tokens.sort_unstable();
                let plan_tokens = if plan_tokens.is_empty() {
                    "none".to_string()
                } else {
                    plan_tokens.join(", ")
                };
                return Err(PlannerError::InvalidPath(format!(
                    "resolved strand ForEach '{}' is absent from the execution plan (plan boundaries: {})",
                    step.token_id, plan_tokens
                )));
            }
        }

        let mut executed = strand.clone();
        for (_, boundary_token_id, cap_token_id) in boundaries {
            let cap_index = executed
                .steps
                .iter()
                .position(|step| step.is_cap() && step.token_id == cap_token_id)
                .ok_or_else(|| {
                    PlannerError::InvalidPath(format!(
                        "execution cap '{}' disappeared while aligning its ForEach boundary",
                        cap_token_id
                    ))
                })?;

            if let Some(previous) = cap_index.checked_sub(1).and_then(|i| executed.steps.get(i)) {
                if matches!(previous.step_type, StrandStepType::ForEach { .. }) {
                    if previous.token_id != boundary_token_id {
                        return Err(PlannerError::InvalidPath(format!(
                            "cap '{}' is preceded by ForEach '{}' but the execution plan uses '{}'",
                            cap_token_id, previous.token_id, boundary_token_id
                        )));
                    }
                    continue;
                }
            }

            if executed.steps.iter().any(|step| {
                matches!(step.step_type, StrandStepType::ForEach { .. })
                    && step.token_id == boundary_token_id
            }) {
                return Err(PlannerError::InvalidPath(format!(
                    "ForEach '{}' is not adjacent to its body-entry cap '{}'",
                    boundary_token_id, cap_token_id
                )));
            }

            let cap_from = executed.steps[cap_index].from_spec.clone();
            executed.steps.insert(
                cap_index,
                StrandStep {
                    token_id: boundary_token_id,
                    step_type: StrandStepType::ForEach {
                        media_def: cap_from.clone(),
                    },
                    from_spec: cap_from.clone(),
                    to_spec: cap_from,
                },
            );
        }

        executed.total_steps = i32::try_from(executed.steps.len()).map_err(|_| {
            PlannerError::InvalidPath("executed strand has more than i32::MAX steps".to_string())
        })?;
        executed.cap_step_count =
            i32::try_from(executed.steps.iter().filter(|s| s.is_cap()).count()).map_err(|_| {
                PlannerError::InvalidPath(
                    "executed strand has more than i32::MAX capability steps".to_string(),
                )
            })?;
        Ok(executed)
    }

    /// Merge N per-strand plans (disjoint DAGs — one plan per `MachineStrand`, as
    /// built by [`MachinePlanBuilder::build_plan_from_machine_strand`]) into ONE
    /// executable plan, so a multi-strand machine runs as a single task with its
    /// outputs merged.
    ///
    /// Node-id discipline:
    /// - **Cap nodes keep their ids** — a cap node's id IS its strand step's stable
    ///   `token_id`, the identity argument values and progress updates are keyed by.
    ///   Token ids are minted per step and unique across a machine; a collision is a
    ///   hard error, never renamed.
    /// - **Every non-cap node id is namespaced** with an `s{index}_` prefix (input
    ///   slots, outputs, maps and their items, gathers, output nodes): those ids are
    ///   synthesized per strand and WOULD collide across strands. All references
    ///   (edges, output lists, map sources and bodies, gather members, output
    ///   sources) are rewritten consistently.
    ///
    /// A single plan passes through unchanged (only renamed to `name`).
    pub fn merge_disjoint(name: &str, plans: Vec<MachinePlan>) -> Result<Self, PlannerError> {
        if plans.is_empty() {
            return Err(PlannerError::InvalidPath(
                "cannot merge zero strand plans into a machine plan".to_string(),
            ));
        }
        if plans.len() == 1 {
            let mut plan = plans.into_iter().next().expect("checked len == 1");
            plan.name = name.to_string();
            return Ok(plan);
        }

        let mut merged = MachinePlan::new(name);
        for (strand_index, plan) in plans.into_iter().enumerate() {
            // Build this strand's rename map: every non-cap node gets namespaced.
            let mut rename: HashMap<String, String> = HashMap::new();
            for (id, node) in &plan.nodes {
                if !node.is_cap() {
                    rename.insert(id.clone(), format!("s{strand_index}_{id}"));
                }
            }
            let map_id =
                |id: &str| -> NodeId { rename.get(id).cloned().unwrap_or_else(|| id.to_string()) };

            // Nodes, with intra-node references rewritten. `add_node` re-derives the
            // entry/output tracking lists from the node types.
            for (old_id, mut node) in plan.nodes {
                let new_id = map_id(&old_id);
                if merged.nodes.contains_key(&new_id) {
                    return Err(PlannerError::InvalidPath(format!(
                        "strand plan {strand_index}: node id '{new_id}' collides with a node \
                         from an earlier strand — cap token ids must be unique per machine"
                    )));
                }
                node.id = new_id;
                match &mut node.node_type {
                    ExecutionNodeType::ForEach { sources, body, .. } => {
                        for source in sources.iter_mut() {
                            source.node = map_id(&source.node);
                            source.item = map_id(&source.item);
                        }
                        for member in body.iter_mut() {
                            *member = map_id(member);
                        }
                    }
                    ExecutionNodeType::Item {
                        foreach, source, ..
                    } => {
                        *foreach = map_id(foreach);
                        *source = map_id(source);
                    }
                    ExecutionNodeType::Collect { input_nodes, .. } => {
                        for n in input_nodes.iter_mut() {
                            *n = map_id(n);
                        }
                    }
                    ExecutionNodeType::CapOutput { cap_node, .. } => {
                        *cap_node = map_id(cap_node);
                    }
                    ExecutionNodeType::Output { source_node, .. } => {
                        *source_node = map_id(source_node);
                    }
                    ExecutionNodeType::Cap { .. } | ExecutionNodeType::InputSlot { .. } => {}
                }
                merged.add_node(node);
            }

            // Edges, endpoints rewritten.
            for mut edge in plan.edges {
                edge.from_node = map_id(&edge.from_node);
                edge.to_node = map_id(&edge.to_node);
                merged.add_edge(edge);
            }
        }

        // Node maps iterate in hash order; the entry/output lists rebuilt by
        // `add_node` must not inherit that nondeterminism — sort them so the merged
        // plan (and its representative first output) is stable.
        merged.entry_nodes.sort();
        merged.output_nodes.sort();

        merged.validate()?;
        merged.topological_order().map_err(|e| {
            PlannerError::InvalidPath(format!("merged multi-strand plan is cyclic: {e}"))
        })?;
        Ok(merged)
    }

    /// Create a plan for a single cap execution.
    /// `file_path_arg_name` is the name of the argument that receives the input file path.
    pub fn single_cap(
        cap_urn: &str,
        input_media: &str,
        _output_media: &str,
        file_path_arg_name: &str,
    ) -> Self {
        let mut plan = Self::new(&format!("Single cap: {}", cap_urn));

        // Add input slot
        let input_id = "input_slot";
        plan.add_node(MachineNode::input_slot(
            input_id,
            "input",
            input_media,
            InputCardinality::Single,
        ));

        // Add cap node
        let cap_id = "cap_0";
        let mut bindings = ArgumentBindings::new();
        bindings.add_file_path(file_path_arg_name);
        plan.add_node(MachineNode::cap_with_bindings(cap_id, cap_urn, bindings));
        plan.add_edge(MachinePlanEdge::direct(input_id, cap_id));

        // Add output node
        let output_id = "output";
        plan.add_node(MachineNode::output(output_id, "result", cap_id));
        plan.add_edge(MachinePlanEdge::direct(cap_id, output_id));

        plan
    }

    /// Whether this plan contains a map.
    pub fn has_foreach(&self) -> bool {
        self.nodes
            .values()
            .any(|n| matches!(n.node_type, ExecutionNodeType::ForEach { .. }))
    }

    /// Create a linear chain of caps (each output feeds into next input).
    /// `file_path_arg_names` provides the argument name for each cap in the chain.
    pub fn linear_chain(
        cap_urns: &[&str],
        input_media: &str,
        _output_media: &str,
        file_path_arg_names: &[&str],
    ) -> Self {
        let mut plan = Self::new("Linear machine");

        if cap_urns.is_empty() {
            return plan;
        }

        // Add input slot
        let input_id = "input_slot";
        plan.add_node(MachineNode::input_slot(
            input_id,
            "input",
            input_media,
            InputCardinality::Single,
        ));

        let mut prev_id = input_id.to_string();

        // Add cap nodes
        for (i, urn) in cap_urns.iter().enumerate() {
            let cap_id = format!("cap_{}", i);
            let mut bindings = ArgumentBindings::new();
            // Use the corresponding arg name, or skip if not provided
            if let Some(arg_name) = file_path_arg_names.get(i) {
                bindings.add_file_path(arg_name);
            }
            plan.add_node(MachineNode::cap_with_bindings(&cap_id, urn, bindings));
            plan.add_edge(MachinePlanEdge::direct(&prev_id, &cap_id));
            prev_id = cap_id;
        }

        // Add output node
        let output_id = "output";
        plan.add_node(MachineNode::output(output_id, "result", &prev_id));
        plan.add_edge(MachinePlanEdge::direct(&prev_id, output_id));

        plan
    }
}

/// Result of executing a single node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeExecutionResult {
    /// The node that was executed
    pub node_id: NodeId,

    /// Whether execution succeeded
    pub success: bool,

    /// Binary output data (if any).
    /// Used by the standalone executor (capdag). The pipeline executor in floom-engine
    /// writes output incrementally to disk and populates saved_paths instead.
    pub binary_output: Option<Vec<u8>>,

    /// Individual output items when the terminal cap used emit_list_item (is_sequence=true).
    /// Used by the standalone executor. Pipeline executor uses saved_paths.
    #[serde(default)]
    pub binary_items: Option<Vec<Vec<u8>>>,

    /// File paths of output already saved to disk by IncrementalWriter.
    /// Populated by the pipeline executor (floom-engine). Empty for standalone executor.
    /// For blob: single path. For sequence: one path per item.
    #[serde(default)]
    pub saved_paths: Vec<String>,

    /// Whether the output is a sequence (from is_sequence on STREAM_START).
    /// Determines how saved_paths should be interpreted: true = folder of items,
    /// false = single file.
    #[serde(default)]
    pub is_sequence_output: bool,

    /// Total bytes written to disk. 0 when binary_output is used instead.
    #[serde(default)]
    pub total_bytes: usize,

    /// Output media URN (from the terminal cap's STREAM_START or plan derivation).
    #[serde(default)]
    pub media_urn_output: String,

    /// Error message if execution failed
    pub error: Option<String>,

    /// Execution duration in milliseconds
    pub duration_ms: u64,
}

/// One run of a map's body: which map, and which of its runs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MapRun {
    /// Stable identity of the map.
    pub foreach_token_id: StepToken,
    /// Index of the run within the map (0-based, item order).
    pub body_index: usize,
}

/// Outcome of a single ForEach body execution.
///
/// Tracks what happened to each body: which caps were in the pathway,
/// which one failed (if any), what output was produced, and timing.
/// For linear (non-ForEach) pipelines, a single BodyOutcome with body_index=0
/// represents the entire execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "BodyOutcomeWire")]
pub struct BodyOutcome {
    /// Stable StrandStep.token_id of the ForEach boundary this body belongs to.
    /// Null only for the synthetic whole-run outcome of a linear machine.
    #[serde(deserialize_with = "deserialize_required_nullable_string")]
    pub foreach_token_id: Option<StepToken>,
    /// Index of this body within the ForEach (0-based). 0 for linear pipelines.
    pub body_index: usize,
    /// The runs this body is inside, when its map is opened inside another's
    /// body: outermost first. Empty for a map of the machine itself, and for
    /// a linear pipeline. A body index alone does not name a run of a nested
    /// map — it recurs under every run of the map around it.
    #[serde(default)]
    pub enclosing: Vec<MapRun>,
    /// Whether this body completed successfully.
    pub success: bool,
    /// Cap URNs in the body's execution pathway (in execution order).
    pub cap_urns: Vec<String>,
    /// Exact immutable strand-step token that failed.
    #[serde(deserialize_with = "deserialize_required_nullable_string")]
    pub failed_token_id: Option<StepToken>,
    /// Error message if the body failed.
    pub error: Option<String>,
    /// Media URN of the argument attributed by the failure's emit source.
    /// Absent when the source did not name one; downstream layers never infer it.
    #[serde(deserialize_with = "deserialize_required_nullable_string")]
    pub failed_arg_urn: Option<String>,
    /// Human-readable title for this body, from stream metadata.
    /// ForEach: per-item meta "title" from prefix output (e.g. "page_3").
    /// Linear: stream-level meta "title" from STREAM_START.
    #[serde(default)]
    pub title: Option<String>,
    /// File paths saved by this body's IncrementalWriter.
    pub saved_paths: Vec<String>,
    /// Total bytes written by this body.
    pub total_bytes: usize,
    /// Execution duration in milliseconds.
    pub duration_ms: u64,
    /// Bounded UTF-8 preview of the body's INPUT item as it entered the
    /// ForEach split — the intermediate that is deliberately never
    /// materialized to disk under the pipelined regime. `None` for
    /// non-textable items and for linear pipelines (no split).
    #[serde(default)]
    pub item_preview_text: Option<String>,
    /// Byte size of the body's input item at the split (0 for linear).
    #[serde(default)]
    pub item_byte_count: u64,
}

#[derive(Deserialize)]
struct BodyOutcomeWire {
    #[serde(deserialize_with = "deserialize_required_nullable_token")]
    foreach_token_id: Option<StepToken>,
    body_index: usize,
    #[serde(default)]
    enclosing: Vec<MapRun>,
    success: bool,
    cap_urns: Vec<String>,
    #[serde(deserialize_with = "deserialize_required_nullable_token")]
    failed_token_id: Option<StepToken>,
    error: Option<String>,
    #[serde(deserialize_with = "deserialize_required_nullable_string")]
    failed_arg_urn: Option<String>,
    #[serde(default)]
    title: Option<String>,
    saved_paths: Vec<String>,
    total_bytes: usize,
    duration_ms: u64,
    #[serde(default)]
    item_preview_text: Option<String>,
    #[serde(default)]
    item_byte_count: u64,
}

impl TryFrom<BodyOutcomeWire> for BodyOutcome {
    type Error = String;

    fn try_from(wire: BodyOutcomeWire) -> Result<Self, Self::Error> {
        // foreach_token_id and failed_token_id need no emptiness check: a
        // StepToken cannot BE empty, and decoding one from "" already failed
        // before this wire value existed.
        if wire.foreach_token_id.is_none() && wire.body_index != 0 {
            return Err("a linear BodyOutcome must use body_index 0".to_string());
        }
        if wire.foreach_token_id.is_none() && !wire.enclosing.is_empty() {
            return Err("a linear BodyOutcome is inside no map".to_string());
        }
        if wire.failed_arg_urn.as_deref() == Some("") {
            return Err("BodyOutcome.failed_arg_urn must be null or non-empty".to_string());
        }
        Ok(Self {
            foreach_token_id: wire.foreach_token_id,
            body_index: wire.body_index,
            enclosing: wire.enclosing,
            success: wire.success,
            cap_urns: wire.cap_urns,
            failed_token_id: wire.failed_token_id,
            error: wire.error,
            failed_arg_urn: wire.failed_arg_urn,
            title: wire.title,
            saved_paths: wire.saved_paths,
            total_bytes: wire.total_bytes,
            duration_ms: wire.duration_ms,
            item_preview_text: wire.item_preview_text,
            item_byte_count: wire.item_byte_count,
        })
    }
}

fn deserialize_required_nullable_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)
}

/// As [`deserialize_required_nullable_string`], for a step identity: the field
/// must be PRESENT and may be null, and a present non-null value goes through
/// [`StepToken`]'s own validating decode — so `""` is refused here exactly as it
/// is everywhere else a token crosses into the process.
fn deserialize_required_nullable_token<'de, D>(deserializer: D) -> Result<Option<StepToken>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<StepToken>::deserialize(deserializer)
}

/// Overall result of executing a machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MachineResult {
    /// Whether the entire chain executed successfully
    pub success: bool,

    /// Results from each node
    pub node_results: HashMap<NodeId, NodeExecutionResult>,

    /// Outputs from output nodes (structured JSON)
    pub outputs: HashMap<String, serde_json::Value>,

    /// Error message if the chain failed
    pub error: Option<String>,

    /// Total execution duration in milliseconds
    pub total_duration_ms: u64,

    /// Per-body outcomes for ForEach pipelines, or single entry for linear.
    /// Populated by the pipeline executor, empty for standalone executor.
    #[serde(default)]
    pub body_outcomes: Vec<BodyOutcome>,
}

impl MachineResult {
    /// Get the primary output (first output node's result)
    pub fn primary_output(&self) -> Option<&serde_json::Value> {
        self.outputs.values().next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // TEST920: Tests creation of a simple execution plan with a single capability
    // Verifies that single_cap() generates a valid plan with input_slot, cap node, and output node
    #[test]
    fn test920_single_cap_plan() {
        let plan = MachinePlan::single_cap(
            "cap:test",
            "media:document;ext=pdf",
            "media:ext=png;image",
            "input_file", // file-path argument name for this cap
        );
        assert_eq!(plan.nodes.len(), 3); // input_slot, cap, output
        assert_eq!(plan.entry_nodes.len(), 1);
        assert_eq!(plan.output_nodes.len(), 1);
        assert!(plan.validate().is_ok());
    }

    // TEST921: Tests creation of a linear chain of capabilities connected in sequence
    // Verifies that linear_chain() correctly links multiple caps with proper edges and topological order
    #[test]
    fn test921_linear_chain_plan() {
        let plan = MachinePlan::linear_chain(
            &["cap:a", "cap:b", "cap:c"],
            "media:document;ext=pdf",
            "media:ext=png;image",
            &["input_a", "input_b", "input_c"], // file-path argument names for each cap
        );
        assert_eq!(plan.nodes.len(), 5); // input_slot, 3 caps, output
        assert_eq!(plan.edges.len(), 4);
        assert!(plan.validate().is_ok());

        let order = plan.topological_order().unwrap();
        assert_eq!(order.len(), 5);
    }

    // TEST922: Tests creation and validation of an empty execution plan with no nodes
    // Verifies that plans without capabilities are valid and handle zero nodes correctly
    #[test]
    fn test922_empty_plan() {
        let plan = MachinePlan::new("empty");
        assert_eq!(plan.nodes.len(), 0);
        assert!(plan.validate().is_ok());
    }

    // TEST923: Tests storing and retrieving metadata attached to an execution plan
    // Verifies that arbitrary JSON metadata can be associated with a plan for context preservation
    #[test]
    fn test923_plan_with_metadata() {
        let mut plan = MachinePlan::new("test");
        let mut metadata = HashMap::new();
        metadata.insert("source".to_string(), json!("pdf"));
        metadata.insert("version".to_string(), json!(1));
        plan.metadata = Some(metadata);

        assert!(plan.metadata.is_some());
        assert_eq!(
            plan.metadata.as_ref().unwrap().get("source"),
            Some(&json!("pdf"))
        );
    }

    // TEST924: Tests plan validation detects edges pointing to non-existent nodes
    // Verifies that validate() returns an error when an edge references a missing to_node
    #[test]
    fn test924_validate_invalid_edge() {
        let mut plan = MachinePlan::new("invalid");
        plan.nodes
            .insert("node_0".to_string(), MachineNode::cap("node_0", "cap:test"));
        plan.edges
            .push(MachinePlanEdge::direct("node_0", "nonexistent"));

        let result = plan.validate();
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("Edge to_node 'nonexistent' not found"));
    }

    // TEST925: Tests topological sort correctly orders a diamond-shaped DAG (A->B,C->D)
    // Verifies that nodes with multiple paths respect dependency constraints (A first, D last)
    #[test]
    fn test925_topological_order_diamond() {
        // Diamond: A -> B, A -> C, B -> D, C -> D
        let mut plan = MachinePlan::new("diamond");

        plan.nodes
            .insert("A".to_string(), MachineNode::cap("A", "cap:a"));
        plan.nodes
            .insert("B".to_string(), MachineNode::cap("B", "cap:b"));
        plan.nodes
            .insert("C".to_string(), MachineNode::cap("C", "cap:c"));
        plan.nodes
            .insert("D".to_string(), MachineNode::cap("D", "cap:d"));

        plan.edges.push(MachinePlanEdge::direct("A", "B"));
        plan.edges.push(MachinePlanEdge::direct("A", "C"));
        plan.edges.push(MachinePlanEdge::direct("B", "D"));
        plan.edges.push(MachinePlanEdge::direct("C", "D"));

        let order = plan.topological_order().unwrap();
        assert_eq!(order.len(), 4);

        // A must come first
        assert_eq!(order[0].id, "A");
        // D must come last
        assert_eq!(order[3].id, "D");
    }

    // TEST926: Tests topological sort detects and rejects cyclic dependencies (A->B->C->A)
    // Verifies that circular references produce a "Cycle detected" error
    #[test]
    fn test926_topological_order_detects_cycle() {
        // Cycle: A -> B -> C -> A
        let mut plan = MachinePlan::new("cyclic");

        plan.nodes
            .insert("A".to_string(), MachineNode::cap("A", "cap:a"));
        plan.nodes
            .insert("B".to_string(), MachineNode::cap("B", "cap:b"));
        plan.nodes
            .insert("C".to_string(), MachineNode::cap("C", "cap:c"));

        plan.edges.push(MachinePlanEdge::direct("A", "B"));
        plan.edges.push(MachinePlanEdge::direct("B", "C"));
        plan.edges.push(MachinePlanEdge::direct("C", "A"));

        let result = plan.topological_order();
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("Cycle detected"));
    }

    // TEST728: Tests MachineNode helper methods for identifying node types (cap, fan-out, fan-in)
    // Verifies is_cap(), is_fan_out(), is_fan_in(), and cap_urn() correctly classify node types
    #[test]
    fn test728_cap_node_helpers() {
        let cap_node = MachineNode::cap("test", "cap:test");
        assert!(cap_node.is_cap());
        assert!(!cap_node.is_fan_out());
        assert!(!cap_node.is_fan_in());
        assert_eq!(cap_node.cap_urn(), Some("cap:test"));

        let foreach_node = MachineNode::for_each(
            "foreach",
            vec![ForEachSource {
                node: "input".to_string(),
                item: "item".to_string(),
            }],
            vec!["item".to_string(), "body".to_string()],
        );
        assert!(!foreach_node.is_cap());
        assert!(foreach_node.is_fan_out());
        assert!(!foreach_node.is_fan_in());
        assert_eq!(foreach_node.cap_urn(), None);

        let collect_node = MachineNode::collect("collect", vec!["a".to_string()], "media:");
        assert!(!collect_node.is_cap());
        assert!(!collect_node.is_fan_out());
        assert!(collect_node.is_fan_in());
    }

    // TEST729: Tests creation and classification of different edge types (Direct, Iteration, Collection, Arg)
    // Verifies that edge constructors produce correct EdgeType variants
    #[test]
    fn test729_edge_types() {
        let direct = MachinePlanEdge::direct("a", "b");
        assert!(matches!(direct.edge_type, EdgeType::Direct));

        let iteration = MachinePlanEdge::iteration("foreach", "body");
        assert!(matches!(iteration.edge_type, EdgeType::Iteration));

        let collection = MachinePlanEdge::collection("body", "collect");
        assert!(matches!(collection.edge_type, EdgeType::Collection));

        let arg = MachinePlanEdge::arg("a", "b", "media:data");
        assert!(matches!(arg.edge_type, EdgeType::Arg { arg_urn } if arg_urn == "media:data"));
    }

    // TEST927: Tests MachineResult structure for successful execution outcomes
    // Verifies that success status, outputs, and primary_output() accessor work correctly
    #[test]
    fn test927_execution_result() {
        let mut outputs = HashMap::new();
        outputs.insert("output".to_string(), json!({"result": "success"}));

        let result = MachineResult {
            success: true,
            node_results: HashMap::new(),
            outputs,
            error: None,
            total_duration_ms: 100,
            body_outcomes: vec![],
        };

        assert!(result.success);
        assert!(result.primary_output().is_some());
    }

    // TEST928: Tests plan validation detects edges originating from non-existent nodes
    // Verifies that validate() returns an error when an edge references a missing from_node
    #[test]
    fn test928_validate_invalid_from_node() {
        let mut plan = MachinePlan::new("invalid");
        plan.nodes
            .insert("node_0".to_string(), MachineNode::cap("node_0", "cap:test"));
        plan.edges
            .push(MachinePlanEdge::direct("nonexistent", "node_0"));

        let result = plan.validate();
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("Edge from_node 'nonexistent' not found"));
    }

    // TEST929: Tests plan validation detects invalid entry node references
    // Verifies that validate() returns an error when entry_nodes contains a non-existent node ID
    #[test]
    fn test929_validate_invalid_entry_node() {
        let mut plan = MachinePlan::new("invalid_entry");
        plan.nodes
            .insert("cap_0".to_string(), MachineNode::cap("cap_0", "cap:test"));
        // Manually add invalid entry node reference
        plan.entry_nodes.push("nonexistent_entry".to_string());

        let result = plan.validate();
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("Entry node 'nonexistent_entry' not found"));
    }

    // TEST930: Tests plan validation detects invalid output node references
    // Verifies that validate() returns an error when output_nodes contains a non-existent node ID
    #[test]
    fn test930_validate_invalid_output_node() {
        let mut plan = MachinePlan::new("invalid_output");
        plan.nodes
            .insert("cap_0".to_string(), MachineNode::cap("cap_0", "cap:test"));
        // Manually add invalid output node reference
        plan.output_nodes.push("nonexistent_output".to_string());

        let result = plan.validate();
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("Output node 'nonexistent_output' not found"));
    }

    // TEST734: Tests topological sort detects self-referencing cycles (A→A)
    // Verifies that self-loops are recognized as cycles and produce an error
    #[test]
    fn test734_topological_order_self_loop() {
        // Self-loop: A -> A
        let mut plan = MachinePlan::new("self_loop");

        plan.nodes
            .insert("A".to_string(), MachineNode::cap("A", "cap:a"));
        plan.edges.push(MachinePlanEdge::direct("A", "A"));

        let result = plan.topological_order();
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("Cycle detected"));
    }

    // TEST735: Tests topological sort handles graphs with multiple independent starting nodes
    // Verifies that parallel entry points (A→C, B→C) both precede their merge point in ordering
    #[test]
    fn test735_topological_order_multiple_entry_points() {
        // Multiple entry points: A -> C, B -> C, C -> D
        let mut plan = MachinePlan::new("multi_entry");

        plan.nodes
            .insert("A".to_string(), MachineNode::cap("A", "cap:a"));
        plan.nodes
            .insert("B".to_string(), MachineNode::cap("B", "cap:b"));
        plan.nodes
            .insert("C".to_string(), MachineNode::cap("C", "cap:c"));
        plan.nodes
            .insert("D".to_string(), MachineNode::cap("D", "cap:d"));

        plan.edges.push(MachinePlanEdge::direct("A", "C"));
        plan.edges.push(MachinePlanEdge::direct("B", "C"));
        plan.edges.push(MachinePlanEdge::direct("C", "D"));

        let order = plan.topological_order().unwrap();
        assert_eq!(order.len(), 4);

        // A and B must come before C
        let a_pos = order.iter().position(|n| n.id == "A").unwrap();
        let b_pos = order.iter().position(|n| n.id == "B").unwrap();
        let c_pos = order.iter().position(|n| n.id == "C").unwrap();
        let d_pos = order.iter().position(|n| n.id == "D").unwrap();

        assert!(a_pos < c_pos);
        assert!(b_pos < c_pos);
        assert!(c_pos < d_pos);
    }

    // TEST736: Tests topological sort on a complex multi-path DAG with 6 nodes
    // Verifies that all dependency constraints are satisfied in a graph with multiple converging paths
    #[test]
    fn test736_topological_order_complex_dag() {
        // Complex DAG:
        //   A --> B --> D
        //   |     |     |
        //   v     v     v
        //   C --> E --> F
        let mut plan = MachinePlan::new("complex");

        for name in ["A", "B", "C", "D", "E", "F"] {
            plan.nodes.insert(
                name.to_string(),
                MachineNode::cap(name, &format!("cap:{}", name.to_lowercase())),
            );
        }

        plan.edges.push(MachinePlanEdge::direct("A", "B"));
        plan.edges.push(MachinePlanEdge::direct("A", "C"));
        plan.edges.push(MachinePlanEdge::direct("B", "D"));
        plan.edges.push(MachinePlanEdge::direct("B", "E"));
        plan.edges.push(MachinePlanEdge::direct("C", "E"));
        plan.edges.push(MachinePlanEdge::direct("D", "F"));
        plan.edges.push(MachinePlanEdge::direct("E", "F"));

        let order = plan.topological_order().unwrap();
        assert_eq!(order.len(), 6);

        // Verify ordering constraints
        let pos = |name: &str| order.iter().position(|n| n.id == name).unwrap();

        // A must be first
        assert_eq!(pos("A"), 0);
        // F must be last
        assert_eq!(pos("F"), 5);
        // B must come before D and E
        assert!(pos("B") < pos("D"));
        assert!(pos("B") < pos("E"));
        // C must come before E
        assert!(pos("C") < pos("E"));
        // D and E must come before F
        assert!(pos("D") < pos("F"));
        assert!(pos("E") < pos("F"));
    }

    // TEST737: Tests linear_chain() with exactly one capability
    // Verifies that a single-element chain produces a valid plan with input_slot, cap, and output
    #[test]
    fn test737_linear_chain_single_cap() {
        let plan = MachinePlan::linear_chain(
            &["cap:only"],
            "media:document;ext=pdf",
            "media:ext=png;image",
            &["source_file"], // file-path argument name
        );
        assert_eq!(plan.nodes.len(), 3); // input_slot, 1 cap, output
        assert_eq!(plan.edges.len(), 2);
        assert!(plan.validate().is_ok());
    }

    // TEST738: Tests linear_chain() with empty capability list
    // Verifies that an empty chain produces a plan with zero nodes and edges
    #[test]
    fn test738_linear_chain_empty() {
        let plan = MachinePlan::linear_chain(
            &[],
            "media:document;ext=pdf",
            "media:ext=png;image",
            &[], // no caps, no arg names
        );
        assert_eq!(plan.nodes.len(), 0);
        assert_eq!(plan.edges.len(), 0);
    }

    // TEST739: Tests NodeExecutionResult structure for successful node execution
    // Verifies that success status, outputs (binary and text), and error fields work correctly
    #[test]
    fn test739_node_execution_result_success() {
        let result = NodeExecutionResult {
            node_id: "node_0".to_string(),
            success: true,
            binary_output: Some(vec![1, 2, 3]),
            binary_items: None,
            saved_paths: vec![],
            is_sequence_output: false,
            total_bytes: 0,
            media_urn_output: String::new(),
            error: None,
            duration_ms: 50,
        };

        assert!(result.success);
        assert!(result.binary_output.is_some());
        assert_eq!(result.error, None);
    }

    // TEST931: Tests NodeExecutionResult structure for failed node execution
    // Verifies that failure status, error message, and absence of outputs are correctly represented
    #[test]
    fn test931_node_execution_result_failure() {
        let result = NodeExecutionResult {
            node_id: "node_0".to_string(),
            success: false,
            binary_output: None,
            binary_items: None,
            saved_paths: vec![],
            is_sequence_output: false,
            total_bytes: 0,
            media_urn_output: String::new(),
            error: Some("Cap execution failed".to_string()),
            duration_ms: 10,
        };

        assert!(!result.success);
        assert!(result.binary_output.is_none());
        assert_eq!(result.error, Some("Cap execution failed".to_string()));
    }

    // TEST932: Tests MachineResult structure for failed chain execution
    // Verifies that failure status, error message, and absence of outputs are correctly represented
    #[test]
    fn test932_execution_result_failure() {
        let result = MachineResult {
            success: false,
            node_results: HashMap::new(),
            outputs: HashMap::new(),
            error: Some("Chain failed".to_string()),
            total_duration_ms: 100,
            body_outcomes: vec![],
        };

        assert!(!result.success);
        assert_eq!(result.error, Some("Chain failed".to_string()));
        assert!(result.primary_output().is_none());
    }

    // TEST742: Tests EdgeType enum serialization and deserialization to/from JSON
    // Verifies that edge types like Direct and Arg correctly round-trip through serde_json
    #[test]
    fn test742_edge_type_serialization() {
        let direct = EdgeType::Direct;
        let json = serde_json::to_string(&direct).unwrap();
        assert_eq!(json, "\"direct\"");

        let deserialized: EdgeType = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, EdgeType::Direct));

        let arg = EdgeType::Arg {
            arg_urn: "media:data".to_string(),
        };
        let json = serde_json::to_string(&arg).unwrap();
        assert_eq!(json, r#"{"arg":{"arg_urn":"media:data"}}"#);
        assert!(matches!(
            serde_json::from_str::<EdgeType>(&json).unwrap(),
            EdgeType::Arg { arg_urn } if arg_urn == "media:data"
        ));
        // Every kind is written as the reference of every implementation
        // writes it, and read back as itself.
        for (written, edge_type) in [
            (r#""direct""#, EdgeType::Direct),
            (r#""iteration""#, EdgeType::Iteration),
            (r#""collection""#, EdgeType::Collection),
            (r#"{"arg":{"arg_urn":"media:data"}}"#, arg.clone()),
        ] {
            assert_eq!(serde_json::to_string(&edge_type).unwrap(), written);
            let read: EdgeType = serde_json::from_str(written).unwrap();
            assert_eq!(std::mem::discriminant(&read), std::mem::discriminant(&edge_type), "{written}");
            assert_eq!(serde_json::to_string(&read).unwrap(), written);
        }
        // An arg edge that names no argument is not one.
        assert!(serde_json::from_str::<EdgeType>(r#"{"arg":{}}"#).is_err());
        // An edge carries the whole value: a part of it is not something an
        // edge can be asked for, in either of the forms that once said so —
        // and a kind nothing has is no kind, whatever it carries.
        for retired in [
            r#"{"json_field":{"field":"data"}}"#,
            r#"{"json_path":{"path":"a.b"}}"#,
            r#""json_field""#,
            r#""sideways""#,
            r#""arg""#,
            r#"{"sideways":{"arg_urn":"media:data"}}"#,
        ] {
            assert!(serde_json::from_str::<EdgeType>(retired).is_err(), "{retired}");
        }
    }

    // TEST743: Tests ExecutionNodeType enum serialization and deserialization to/from JSON
    // Verifies that node types like Cap and ForEach correctly serialize with their fields
    #[test]
    fn test743_execution_node_type_serialization() {
        let cap_node = ExecutionNodeType::Cap {
            token_id: "tok-cap-test".parse().unwrap(),
            cap_urn: "cap:test".to_string(),
            arg_bindings: ArgumentBindings::new(),
            preferred_cap: None,
            policy: StepPolicy::default(),
        };
        let json = serde_json::to_string(&cap_node).unwrap();
        assert!(json.contains("cap"));
        assert!(json.contains("cap:test"));
        assert!(
            json.contains("tok-cap-test"),
            "Cap node must serialize its token_id identity: {}",
            json
        );

        let foreach_node = ExecutionNodeType::ForEach {
            token_id: "tok-foreach".parse().unwrap(),
            sources: vec![ForEachSource {
                node: "input".to_string(),
                item: "item".to_string(),
            }],
            body: vec!["item".to_string(), "body".to_string()],
        };
        let json: serde_json::Value = serde_json::to_value(&foreach_node).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "node_type": "for_each",
                "token_id": "tok-foreach",
                "sources": [{"node": "input", "item": "item"}],
                "body": ["item", "body"],
            })
        );
        let output_node = ExecutionNodeType::CapOutput {
            cap_node: "tok-cap-test".to_string(),
            output_media_urn: "media:residuals".to_string(),
            output_name: "residuals".to_string(),
            carries_input: false,
        };
        assert_eq!(
            serde_json::to_value(&output_node).unwrap(),
            serde_json::json!({
                "node_type": "cap_output",
                "cap_node": "tok-cap-test",
                "output_media_urn": "media:residuals",
                "output_name": "residuals",
                "carries_input": false,
            })
        );
        let item_node = MachineNode::item("item", "each", "input", "media:page").node_type;
        assert_eq!(
            serde_json::to_value(&item_node).unwrap(),
            serde_json::json!({
                "node_type": "item", "foreach": "each", "source": "input", "media_urn": "media:page",
            })
        );
        let gather = MachineNode::collect("g", vec!["a".to_string(), "b".to_string()], "media:summary").node_type;
        assert_eq!(
            serde_json::to_value(&gather).unwrap(),
            serde_json::json!({
                "node_type": "collect", "input_nodes": ["a", "b"], "output_media_urn": "media:summary",
            })
        );

        // The default policy is not written; one a step wrote is, whole.
        let written = serde_json::to_value(&cap_node).unwrap();
        assert!(written.get("policy").is_none());
        let mut retrying = cap_node.clone();
        if let ExecutionNodeType::Cap { policy, .. } = &mut retrying {
            *policy = StepPolicy {
                retry: 2,
                on_error: crate::machine::OnError::Absent,
            };
        }
        let written = serde_json::to_value(&retrying).unwrap();
        assert_eq!(written["policy"], serde_json::json!({"retry": 2, "on_error": "absent"}));
        match serde_json::from_value::<ExecutionNodeType>(written).unwrap() {
            ExecutionNodeType::Cap { policy, .. } => {
                assert_eq!((policy.retry, policy.on_error), (2, crate::machine::OnError::Absent))
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            serde_json::to_value(StepPolicy { retry: 1, ..StepPolicy::default() }).unwrap(),
            serde_json::json!({"retry": 1, "on_error": "fail"})
        );
        assert_eq!(
            serde_json::from_value::<StepPolicy>(serde_json::json!({"retry": 3})).unwrap(),
            StepPolicy { retry: 3, ..StepPolicy::default() },
            "a field left out is its default"
        );

        // A node type nothing produces is not read, nor is a node missing a
        // field its kind has, nor a map in the shape it once had.
        for refused in [
            serde_json::json!({"node_type": "merge", "input_nodes": ["a"], "merge_strategy": "concat"}),
            serde_json::json!({"node_type": "split", "input_node": "a", "output_count": 2}),
            serde_json::json!({"node_type": "for_each", "input_node": "a", "body_entry": "b", "body_exit": "b"}),
            serde_json::json!({"node_type": "for_each", "token_id": "t", "sources": [{"node": "a", "item": "i"}]}),
            serde_json::json!({"node_type": "collect", "input_nodes": ["a"]}),
            serde_json::json!({"node_type": "cap", "cap_urn": "cap:test"}),
            serde_json::json!({"node_type": "cap", "token_id": "", "cap_urn": "cap:test"}),
            serde_json::json!({"node_type": "cap", "token_id": "t", "cap_urn": "cap:test", "policy": {"on_error": "shrug"}}),
            serde_json::json!({"node_type": "item", "foreach": "each", "source": "input"}),
            serde_json::json!({"cap_urn": "cap:test"}),
        ] {
            assert!(
                serde_json::from_value::<ExecutionNodeType>(refused.clone()).is_err(),
                "{refused}"
            );
        }
    }

    // TEST744: Tests MachinePlan serialization and deserialization to/from JSON
    // Verifies that complete plans with nodes and edges correctly round-trip through JSON
    #[test]
    fn test744_plan_serialization() {
        let plan = MachinePlan::single_cap(
            "cap:test",
            "media:document;ext=pdf",
            "media:ext=png;image",
            "input_file", // file-path argument name
        );

        let json = serde_json::to_string(&plan).unwrap();
        assert!(json.contains("cap:test"));
        assert!(json.contains("input_slot"));
        assert!(json.contains("output"));

        let deserialized: MachinePlan = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.nodes.len(), plan.nodes.len());
        assert_eq!(deserialized.edges.len(), plan.edges.len());
    }

    // TEST746: Tests creation of Output node type that references a source node
    // Verifies that MachineNode::output() correctly constructs an Output node with name and source
    #[test]
    fn test746_cap_node_output() {
        let output = MachineNode::output("out", "result", "source");
        match &output.node_type {
            ExecutionNodeType::Output {
                output_name,
                source_node,
            } => {
                assert_eq!(output_name, "result");
                assert_eq!(source_node, "source");
            }
            _ => panic!("Expected Output node type"),
        }
    }

    // TEST749: Tests get_node() method for looking up nodes by ID in a plan
    // Verifies that existing nodes are found and non-existent nodes return None
    #[test]
    fn test749_get_node() {
        let plan = MachinePlan::single_cap(
            "cap:test",
            "media:document;ext=pdf",
            "media:ext=png;image",
            "doc_path", // file-path argument name
        );

        assert!(plan.get_node("cap_0").is_some());
        assert!(plan.get_node("input_slot").is_some());
        assert!(plan.get_node("output").is_some());
        assert!(plan.get_node("nonexistent").is_none());
    }

    /// A plan with a map and a fold after it:
    /// input_slot → cap_0 (a sequence) → map over it [ body_cap_0 → body_cap_1 ] → cap_post
    /// (takes the sequence of body_cap_1 over the runs) → output.
    fn build_foreach_plan() -> MachinePlan {
        let mut plan = MachinePlan::new("ForEach test plan");
        plan.add_node(MachineNode::input_slot(
            "input_slot",
            "input",
            "media:document;ext=pdf",
            InputCardinality::Single,
        ));
        plan.add_node(MachineNode::cap(
            "cap_0",
            "cap:in=\"media:document;ext=pdf\";out=\"media:pdf-page;list\"",
        ));
        plan.add_node(MachineNode::for_each(
            "foreach_0",
            vec![ForEachSource {
                node: "cap_0".to_string(),
                item: "foreach_0_item_0".to_string(),
            }],
            vec![
                "foreach_0_item_0".to_string(),
                "body_cap_0".to_string(),
                "body_cap_1".to_string(),
            ],
        ));
        plan.add_node(MachineNode::item(
            "foreach_0_item_0",
            "foreach_0",
            "cap_0",
            "media:pdf-page",
        ));
        plan.add_node(MachineNode::cap(
            "body_cap_0",
            "cap:in=media:pdf-page;out=\"media:enc=utf-8;text\"",
        ));
        plan.add_node(MachineNode::cap(
            "body_cap_1",
            "cap:in=\"media:enc=utf-8;text\";out=\"media:decision;fmt=json;record\"",
        ));
        plan.add_node(MachineNode::cap(
            "cap_post",
            "cap:in=\"media:decision;fmt=json;record\";out=\"media:fmt=json\"",
        ));
        plan.add_node(MachineNode::output("output", "result", "cap_post"));

        plan.add_edge(MachinePlanEdge::direct("input_slot", "cap_0"));
        plan.add_edge(MachinePlanEdge::direct("cap_0", "foreach_0"));
        plan.add_edge(MachinePlanEdge::iteration("foreach_0", "foreach_0_item_0"));
        plan.add_edge(MachinePlanEdge::direct("foreach_0_item_0", "body_cap_0"));
        plan.add_edge(MachinePlanEdge::direct("body_cap_0", "body_cap_1"));
        plan.add_edge(MachinePlanEdge::direct("body_cap_1", "cap_post"));
        plan.add_edge(MachinePlanEdge::direct("cap_post", "output"));
        plan
    }

    // TEST936: has_foreach detects a map, and a gather alone is not one.
    #[test]
    fn test936_has_foreach() {
        let foreach_plan = build_foreach_plan();
        foreach_plan.validate().expect("the fixture is a valid plan");
        assert!(foreach_plan.has_foreach());

        let linear_plan = MachinePlan::linear_chain(
            &["cap:a"],
            "media:document;ext=pdf",
            "media:ext=png;image",
            &["input_a"],
        );
        assert!(!linear_plan.has_foreach());

        let mut gather_plan = MachinePlan::new("gather_only");
        gather_plan.add_node(MachineNode::input_slot(
            "input",
            "input",
            "media:enc=utf-8",
            crate::planner::cardinality::InputCardinality::Single,
        ));
        gather_plan.add_node(MachineNode::cap(
            "cap_0",
            "cap:in=\"media:enc=utf-8\";summarize;out=media:summary",
        ));
        gather_plan.add_node(MachineNode::collect(
            "collect_0",
            vec!["cap_0".to_string()],
            "media:summary",
        ));
        gather_plan.add_node(MachineNode::output("output", "result", "collect_0"));
        gather_plan.validate().expect("valid");
        assert!(!gather_plan.has_foreach());
    }

    // TEST12753: a plan says which map each node runs in — the map whose body
    // lists it — and which nodes are inside a map at any depth.
    #[test]
    fn test12753_a_plan_says_which_map_a_node_runs_in() {
        let mut plan = build_foreach_plan();
        let enclosing = plan.enclosing_maps();
        assert_eq!(enclosing.get("body_cap_0"), Some(&"foreach_0"));
        assert_eq!(enclosing.get("body_cap_1"), Some(&"foreach_0"));
        assert_eq!(enclosing.get("foreach_0_item_0"), Some(&"foreach_0"));
        for outside in ["input_slot", "cap_0", "foreach_0", "cap_post", "output"] {
            assert_eq!(enclosing.get(outside), None, "{outside} is the machine's own");
        }

        // A map opened inside the body: its nodes are inside the outer map,
        // and the outer body lists the inner map, not the inner body.
        plan.add_node(MachineNode::for_each(
            "foreach_1",
            vec![ForEachSource {
                node: "body_cap_0".to_string(),
                item: "foreach_1_item_0".to_string(),
            }],
            vec!["foreach_1_item_0".to_string(), "inner_cap".to_string()],
        ));
        plan.add_node(MachineNode::item(
            "foreach_1_item_0",
            "foreach_1",
            "body_cap_0",
            "media:enc=utf-8;text",
        ));
        plan.add_node(MachineNode::cap("inner_cap", "cap:in=\"media:enc=utf-8;text\";out=media:word"));
        if let ExecutionNodeType::ForEach { body, .. } =
            &mut plan.nodes.get_mut("foreach_0").unwrap().node_type
        {
            body.push("foreach_1".to_string());
        }
        plan.validate().expect("valid");
        let enclosing = plan.enclosing_maps();
        assert_eq!(enclosing.get("inner_cap"), Some(&"foreach_1"));
        assert_eq!(enclosing.get("foreach_1"), Some(&"foreach_0"));
        let inside: HashSet<String> = plan.map_members("foreach_0");
        for id in ["body_cap_0", "body_cap_1", "foreach_0_item_0", "foreach_1", "foreach_1_item_0", "inner_cap"] {
            assert!(inside.contains(id), "{id} is inside the outer map");
        }
        assert!(!inside.contains("cap_post") && !inside.contains("foreach_0"));
        assert_eq!(plan.map_members("foreach_1").len(), 2);
        assert!(plan.map_members("cap_0").is_empty(), "a step is no map");
    }

    // TEST12754: a plan whose nodes name what is not there, or name it as what
    // it is not, is refused: a map that iterates nothing, an item its map does
    // not list, a node in two bodies, an output node of something that is not
    // a step, a gather of a missing node.
    #[test]
    fn test12754_a_plan_that_names_what_is_not_there_is_refused() {
        let refused = |change: &dyn Fn(&mut MachinePlan), expect: &str| {
            let mut plan = build_foreach_plan();
            change(&mut plan);
            let error = plan.validate().expect_err(expect).to_string();
            assert!(error.contains(expect), "expected '{expect}' in: {error}");
        };
        refused(
            &|plan| {
                if let ExecutionNodeType::ForEach { sources, .. } =
                    &mut plan.nodes.get_mut("foreach_0").unwrap().node_type
                {
                    sources.clear();
                }
            },
            "map 'foreach_0' iterates nothing",
        );
        refused(
            &|plan| {
                plan.add_node(MachineNode::for_each("lonely", Vec::new(), Vec::new()));
            },
            "map 'lonely' iterates nothing",
        );
        refused(
            &|plan| {
                if let ExecutionNodeType::ForEach { sources, .. } =
                    &mut plan.nodes.get_mut("foreach_0").unwrap().node_type
                {
                    sources[0].item = "body_cap_0".to_string();
                }
            },
            "which is not that",
        );
        fn source(plan: &mut MachinePlan) -> &mut ForEachSource {
            match &mut plan.nodes.get_mut("foreach_0").unwrap().node_type {
                ExecutionNodeType::ForEach { sources, .. } => &mut sources[0],
                _ => unreachable!(),
            }
        }
        refused(
            &|plan| source(plan).node = "nowhere".to_string(),
            "node 'foreach_0' names source 'nowhere', which is not in the plan",
        );
        refused(
            &|plan| source(plan).item = "nowhere".to_string(),
            "node 'foreach_0' names item 'nowhere', which is not in the plan",
        );
        // The item it lists is an item — of another source, or of another map.
        refused(
            &|plan| source(plan).node = "input_slot".to_string(),
            "map 'foreach_0' lists 'foreach_0_item_0' as its item of 'input_slot', which is not that",
        );
        refused(
            &|plan| {
                plan.add_node(MachineNode::for_each(
                    "second",
                    vec![ForEachSource {
                        node: "cap_0".to_string(),
                        item: "foreach_0_item_0".to_string(),
                    }],
                    vec!["foreach_0_item_0".to_string()],
                ));
            },
            "map 'second' lists 'foreach_0_item_0' as its item of 'cap_0', which is not that",
        );
        refused(
            &|plan| {
                if let ExecutionNodeType::ForEach { body, .. } =
                    &mut plan.nodes.get_mut("foreach_0").unwrap().node_type
                {
                    body.retain(|id| id != "foreach_0_item_0");
                }
            },
            "does not list its item 'foreach_0_item_0' in its body",
        );
        fn body(plan: &mut MachinePlan, member: &str) {
            if let ExecutionNodeType::ForEach { body, .. } =
                &mut plan.nodes.get_mut("foreach_0").unwrap().node_type
            {
                body.push(member.to_string());
            }
        }
        refused(&|plan| body(plan, "foreach_0"), "map 'foreach_0' lists itself in its body");
        refused(
            &|plan| body(plan, "nowhere"),
            "node 'foreach_0' names body node 'nowhere', which is not in the plan",
        );
        refused(
            &|plan| {
                plan.add_node(MachineNode::for_each(
                    "second",
                    vec![ForEachSource {
                        node: "cap_0".to_string(),
                        item: "second_item".to_string(),
                    }],
                    vec!["second_item".to_string(), "body_cap_1".to_string()],
                ));
                plan.add_node(MachineNode::item("second_item", "second", "cap_0", "media:pdf-page"));
            },
            "node 'body_cap_1' is in the body of two maps",
        );
        refused(
            &|plan| {
                plan.add_node(MachineNode::cap_output("o", "input_slot", "media:x", "x", false));
            },
            "names 'input_slot' as its step, which is not a step",
        );
        refused(
            &|plan| {
                plan.add_node(MachineNode::cap_output("o", "nowhere", "media:x", "x", false));
            },
            "node 'o' names step 'nowhere', which is not in the plan",
        );
        refused(
            &|plan| {
                plan.add_node(MachineNode::collect("g", vec!["nowhere".to_string()], "media:"));
            },
            "node 'g' names gathered node 'nowhere', which is not in the plan",
        );
        refused(
            &|plan| {
                plan.add_node(MachineNode::output("o2", "result", "nowhere"));
            },
            "node 'o2' names source 'nowhere', which is not in the plan",
        );
        refused(
            &|plan| {
                plan.add_node(MachineNode::item("stray", "foreach_0", "cap_0", "media:pdf-page"));
            },
            "item node 'stray' names map 'foreach_0'",
        );
        refused(
            &|plan| {
                plan.add_node(MachineNode::item("stray", "cap_0", "input_slot", "media:pdf-page"));
            },
            "item node 'stray' names map 'cap_0'",
        );
        refused(
            &|plan| {
                plan.add_node(MachineNode::item("stray", "nowhere", "cap_0", "media:pdf-page"));
            },
            "node 'stray' names map 'nowhere', which is not in the plan",
        );
        // An item its map lists for another source: nodes are checked in the
        // order of their ids, so it is the item — which comes first — that
        // is named.
        refused(
            &|plan| {
                plan.add_node(MachineNode::for_each(
                    "second",
                    vec![ForEachSource {
                        node: "cap_0".to_string(),
                        item: "an_item".to_string(),
                    }],
                    vec!["an_item".to_string()],
                ));
                plan.add_node(MachineNode::item("an_item", "second", "input_slot", "media:pdf-page"));
            },
            "item node 'an_item' names map 'second' and source 'input_slot', and that map does not list it so",
        );

        // A step's other output, of a step: accepted.
        let mut plan = build_foreach_plan();
        plan.add_node(MachineNode::cap_output("o", "cap_0", "media:x", "x", false));
        plan.validate().expect("valid");
    }

    fn one_cap_strand(prefix: Option<&str>) -> Strand {
        let input = crate::MediaUrn::from_string("media:enc=utf-8;page").unwrap();
        let output = crate::MediaUrn::from_string("media:enc=utf-8;ext=txt;plain-text").unwrap();
        let cap = crate::CapUrn::from_string(
            "cap:constrained;in=\"media:enc=utf-8\";language=en;out=\"media:enc=utf-8;ext=txt;plain-text\";summarize",
        )
        .unwrap();
        let cap_step = StrandStep {
            token_id: "tok-cap".parse().unwrap(),
            step_type: StrandStepType::Cap {
                cap_urn: cap,
                title: "Summarize".to_string(),
                specificity: 1,
                input_is_sequence: false,
                output_is_sequence: false,
                output_urn: None,
                inputs: Vec::new(),
            },
            from_spec: input.clone(),
            to_spec: output.clone(),
        };
        let mut steps = Vec::new();
        if let Some(token_id) = prefix {
            steps.push(StrandStep {
                token_id: token_id.parse().unwrap(),
                step_type: StrandStepType::ForEach {
                    media_def: input.clone(),
                },
                from_spec: input.clone(),
                to_spec: input.clone(),
            });
        }
        steps.push(cap_step);
        Strand {
            total_steps: steps.len() as i32,
            cap_step_count: 1,
            steps,
            source_media_urn: input,
            target_media_urn: output,
            description: "one-cap test strand".to_string(),
        }
    }

    /// A map over the plan's one input, giving its item to `cap`.
    fn add_map_over_input(plan: &mut MachinePlan, cap: &str, foreach_token: &str) {
        plan.add_node(MachineNode::input_slot(
            "input_slot_0",
            "input",
            "media:enc=utf-8;page",
            InputCardinality::Sequence,
        ));
        plan.add_node(MachineNode::for_each_token(
            "foreach_0",
            vec![ForEachSource {
                node: "input_slot_0".to_string(),
                item: "foreach_0_item_0".to_string(),
            }],
            vec!["foreach_0_item_0".to_string(), cap.to_string()],
            foreach_token.parse().unwrap(),
        ));
        plan.add_node(MachineNode::item(
            "foreach_0_item_0",
            "foreach_0",
            "input_slot_0",
            "media:enc=utf-8;page",
        ));
        plan.add_edge(MachinePlanEdge::direct("input_slot_0", "foreach_0"));
        plan.add_edge(MachinePlanEdge::iteration("foreach_0", "foreach_0_item_0"));
        plan.add_edge(MachinePlanEdge::direct("foreach_0_item_0", cap));
        plan.validate().expect("valid");
    }

    // TEST7120: A runtime-cardinality ForEach is part of the executed graph, so
    // persistence receives the exact plan token rather than a cap token or a
    // separately minted render-only token.
    #[test]
    fn test7120_executed_strand_inserts_runtime_foreach_identity() {
        let mut plan = MachinePlan::new("runtime foreach");
        plan.add_node(MachineNode::cap_with_bindings_token(
            "tok-cap",
            "cap:constrained;in=\"media:enc=utf-8\";language=en;out=\"media:enc=utf-8;ext=txt;plain-text\";summarize",
            ArgumentBindings::new(),
            "tok-cap".parse().unwrap(),
        ));
        add_map_over_input(&mut plan, "tok-cap", "tok-foreach");

        let executed = plan.executed_strand(&one_cap_strand(None)).unwrap();
        assert_eq!(executed.steps.len(), 2);
        assert!(matches!(
            executed.steps[0].step_type,
            StrandStepType::ForEach { .. }
        ));
        assert_eq!(executed.steps[0].token_id, "tok-foreach");
        assert_eq!(executed.steps[1].token_id, "tok-cap");
        assert_eq!(executed.total_steps, 2);
    }

    // TEST7121: Existing shape identity is never silently rewritten. A run whose
    // strand and plan disagree must fail before it can persist invalid provenance.
    #[test]
    fn test7121_executed_strand_rejects_foreach_identity_mismatch() {
        let mut plan = MachinePlan::new("mismatched foreach");
        plan.add_node(MachineNode::cap_with_bindings_token(
            "tok-cap",
            "cap:test",
            ArgumentBindings::new(),
            "tok-cap".parse().unwrap(),
        ));
        add_map_over_input(&mut plan, "tok-cap", "tok-plan-foreach");

        let error = plan
            .executed_strand(&one_cap_strand(Some("tok-strand-foreach")))
            .expect_err("mismatched boundary identities must fail hard");
        assert!(error.to_string().contains("tok-strand-foreach"));
        assert!(error.to_string().contains("tok-plan-foreach"));
    }

    // TEST6525: Body outcomes serialize an explicit stable failure coordinate
    #[test]
    fn test6525_body_outcome_requires_explicit_failure_coordinate() {
        let outcome = BodyOutcome {
            foreach_token_id: None,
            body_index: 0,
            enclosing: vec![],
            success: true,
            cap_urns: vec![],
            failed_token_id: None,
            error: None,
            failed_arg_urn: None,
            title: None,
            saved_paths: vec![],
            total_bytes: 0,
            duration_ms: 0,
            item_preview_text: None,
            item_byte_count: 0,
        };
        let encoded = serde_json::to_value(&outcome).unwrap();
        assert_eq!(
            encoded.get("foreach_token_id"),
            Some(&serde_json::Value::Null)
        );
        assert_eq!(
            encoded.get("failed_token_id"),
            Some(&serde_json::Value::Null)
        );
        assert_eq!(
            encoded.get("failed_arg_urn"),
            Some(&serde_json::Value::Null)
        );

        let mut missing = encoded.as_object().unwrap().clone();
        missing.remove("foreach_token_id");
        assert!(serde_json::from_value::<BodyOutcome>(serde_json::Value::Object(missing)).is_err());

        let mut missing = encoded.as_object().unwrap().clone();
        missing.remove("failed_token_id");
        assert!(serde_json::from_value::<BodyOutcome>(serde_json::Value::Object(missing)).is_err());

        let mut missing = encoded.as_object().unwrap().clone();
        missing.remove("failed_arg_urn");
        assert!(serde_json::from_value::<BodyOutcome>(serde_json::Value::Object(missing)).is_err());

        let mut nonzero_linear = encoded.as_object().unwrap().clone();
        nonzero_linear.insert("body_index".to_string(), serde_json::json!(1));
        let error =
            serde_json::from_value::<BodyOutcome>(serde_json::Value::Object(nonzero_linear))
                .expect_err("a bare nonzero ordinal is not a linear body coordinate");
        assert!(error
            .to_string()
            .contains("linear BodyOutcome must use body_index 0"));

        let mut foreach = encoded.as_object().unwrap().clone();
        foreach.insert(
            "foreach_token_id".to_string(),
            serde_json::json!("tok-foreach"),
        );
        foreach.insert("body_index".to_string(), serde_json::json!(3));
        let decoded = serde_json::from_value::<BodyOutcome>(serde_json::Value::Object(foreach))
            .expect("a scoped ForEach body coordinate is valid");
        assert_eq!(decoded.foreach_token_id.as_deref(), Some("tok-foreach"));
        assert_eq!(decoded.body_index, 3);
        assert!(decoded.enclosing.is_empty());

        // The runs a body is inside are written as a list, empty for a body
        // of the machine's own map; a linear outcome is inside no map.
        assert_eq!(encoded.get("enclosing"), Some(&serde_json::json!([])));
        let outer = serde_json::json!({"foreach_token_id": "tok-outer", "body_index": 2});
        let mut inside_linear = encoded.as_object().unwrap().clone();
        inside_linear.insert("enclosing".to_string(), serde_json::json!([outer]));
        let error = serde_json::from_value::<BodyOutcome>(serde_json::Value::Object(inside_linear))
            .expect_err("a linear outcome names no enclosing run");
        assert!(error.to_string().contains("a linear BodyOutcome is inside no map"));

        // A body of a map opened inside another names the run it is inside.
        let mut nested = encoded.as_object().unwrap().clone();
        nested.insert("foreach_token_id".to_string(), serde_json::json!("tok-foreach"));
        nested.insert("body_index".to_string(), serde_json::json!(3));
        nested.insert("enclosing".to_string(), serde_json::json!([outer]));
        let decoded = serde_json::from_value::<BodyOutcome>(serde_json::Value::Object(nested)).unwrap();
        assert_eq!(
            decoded.enclosing,
            vec![MapRun {
                foreach_token_id: "tok-outer".parse().unwrap(),
                body_index: 2,
            }]
        );
        assert_eq!(serde_json::to_value(&decoded).unwrap()["enclosing"], serde_json::json!([outer]));
        // A run that names no map is not a run.
        assert!(serde_json::from_value::<MapRun>(serde_json::json!({"body_index": 2})).is_err());
        assert!(serde_json::from_value::<MapRun>(serde_json::json!({"foreach_token_id": "", "body_index": 2})).is_err());
    }
}
