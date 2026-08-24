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
    },

    /// Fan-out: run downstream path for each item in input sequence
    ///
    /// Auto-inserted when sequence feeds into single-input cap.
    /// Creates parallel execution branches for each input item.
    ForEach {
        /// Stable per-step identity (the ForEach `StrandStep.token_id`). The run's
        /// aggregate per-item progress is reported under this identity so it maps to
        /// the exact ForEach graph element the client rendered.
        token_id: StepToken,
        /// Node that provides the input sequence
        input_node: NodeId,
        /// Entry point of the per-item sub-graph
        body_entry: NodeId,
        /// Exit point of the per-item sub-graph (where results are collected)
        body_exit: NodeId,
    },

    /// Fan-in: collect outputs from parallel ForEach branches into sequence
    ///
    /// Auto-inserted to gather results after fan-out.
    Collect {
        /// Nodes whose outputs should be collected
        input_nodes: Vec<NodeId>,
        /// Output media URN for the collected sequence
        #[serde(skip_serializing_if = "Option::is_none")]
        output_media_urn: Option<String>,
    },

    /// Merge: combine outputs from parallel branches
    ///
    /// User-designed node for combining outputs from parallel paths.
    Merge {
        /// Nodes whose outputs should be merged
        input_nodes: Vec<NodeId>,
        /// Strategy for merging
        merge_strategy: MergeStrategy,
    },

    /// Split: route input to multiple downstream paths
    ///
    /// User-designed node for parallel processing.
    Split {
        /// Node that provides the input
        input_node: NodeId,
        /// Number of output branches
        output_count: usize,
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

/// Strategy for merging outputs from parallel branches
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeStrategy {
    /// Concatenate all outputs into a sequence
    Concat,
    /// Zip outputs together (requires same length)
    ZipWith,
    /// Take first successful output
    FirstSuccess,
    /// Take all successful outputs
    AllSuccessful,
}

impl Default for MergeStrategy {
    fn default() -> Self {
        Self::Concat
    }
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
            },
            description: None,
        }
    }

    /// Create a ForEach (fan-out) node.
    ///
    /// Mints a fresh `token_id`. The run path uses
    /// [`MachineNode::for_each_token`] to preserve the originating ForEach step's
    /// identity so its aggregate progress maps to the client's graph element.
    pub fn for_each(id: &str, input_node: &str, body_entry: &str, body_exit: &str) -> Self {
        Self::for_each_token(
            id,
            input_node,
            body_entry,
            body_exit,
            StepToken::mint(),
        )
    }

    /// Create a ForEach (fan-out) node, preserving the originating step's identity.
    pub fn for_each_token(
        id: &str,
        input_node: &str,
        body_entry: &str,
        body_exit: &str,
        token_id: StepToken,
    ) -> Self {
        Self {
            id: id.to_string(),
            node_type: ExecutionNodeType::ForEach {
                token_id,
                input_node: input_node.to_string(),
                body_entry: body_entry.to_string(),
                body_exit: body_exit.to_string(),
            },
            description: Some("Fan-out: process each item in vector".to_string()),
        }
    }

    /// Create a Collect (fan-in) node
    pub fn collect(id: &str, input_nodes: Vec<String>) -> Self {
        Self {
            id: id.to_string(),
            node_type: ExecutionNodeType::Collect {
                input_nodes,
                output_media_urn: None,
            },
            description: Some("Fan-in: collect results into vector".to_string()),
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
    /// Direct data flow into the target cap's MAIN (stdin) input.
    Direct,
    /// Data flow into a specific NON-main argument of the target cap, identified by
    /// that argument's media URN. This is convergence: another cap's output routed
    /// into a named arg. The runtime delivers it as its own stream, keyed by `arg_urn`.
    Arg { arg_urn: String },
    /// Extract field from JSON output
    JsonField { field: String },
    /// Extract via JSONPath
    JsonPath { path: String },
    /// Iteration edge (from ForEach to body)
    Iteration,
    /// Collection edge (from body to Collect)
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

    /// Create a JSON field extraction edge
    pub fn json_field(from: &str, to: &str, field: &str) -> Self {
        Self {
            from_node: from.to_string(),
            to_node: to.to_string(),
            edge_type: EdgeType::JsonField {
                field: field.to_string(),
            },
        }
    }

    /// Create a JSON path extraction edge
    pub fn json_path(from: &str, to: &str, path: &str) -> Self {
        Self {
            from_node: from.to_string(),
            to_node: to.to_string(),
            edge_type: EdgeType::JsonPath {
                path: path.to_string(),
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

        Ok(())
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
                token_id,
                body_entry,
                ..
            } = &node.node_type
            else {
                continue;
            };
            let body_node = self.nodes.get(body_entry).ok_or_else(|| {
                PlannerError::InvalidPath(format!(
                    "ForEach '{}' references missing body-entry node '{}'",
                    token_id, body_entry
                ))
            })?;
            let ExecutionNodeType::Cap {
                token_id: cap_token_id,
                ..
            } = &body_node.node_type
            else {
                return Err(PlannerError::InvalidPath(format!(
                    "ForEach '{}' body-entry '{}' is not a capability step",
                    token_id, body_entry
                )));
            };
            let position = *cap_positions.get(cap_token_id.as_str()).ok_or_else(|| {
                PlannerError::InvalidPath(format!(
                    "ForEach '{}' body-entry cap '{}' is absent from the resolved strand",
                    token_id, cap_token_id
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
    ///   slots, outputs, ForEach, Collect, …): those ids are synthesized per strand
    ///   from strand-local indices and WOULD collide across strands. All references
    ///   (edges, output lists, ForEach body extents, Collect members, Output sources)
    ///   are rewritten consistently.
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
                    ExecutionNodeType::ForEach {
                        input_node,
                        body_entry,
                        body_exit,
                        ..
                    } => {
                        *input_node = map_id(input_node);
                        *body_entry = map_id(body_entry);
                        *body_exit = map_id(body_exit);
                    }
                    ExecutionNodeType::Collect { input_nodes, .. }
                    | ExecutionNodeType::Merge { input_nodes, .. } => {
                        for n in input_nodes.iter_mut() {
                            *n = map_id(n);
                        }
                    }
                    ExecutionNodeType::Split { input_node, .. } => {
                        *input_node = map_id(input_node);
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

    /// Find the first ForEach node in the plan, if any.
    /// Returns the node ID of the ForEach node.
    pub fn find_first_foreach(&self) -> Option<&NodeId> {
        // Use topological order to find the first ForEach node in execution order
        let topo = self.topological_order().ok()?;
        for node in topo {
            if matches!(node.node_type, ExecutionNodeType::ForEach { .. }) {
                return Some(&node.id);
            }
        }
        None
    }

    /// Check whether this plan contains any ForEach nodes (requiring decomposition).
    ///
    /// ForEach nodes require special handling: the plan is decomposed into
    /// prefix/body/suffix, and the body is executed per-item. Standalone Collect
    /// nodes (scalar→list without ForEach) are pass-throughs handled by
    /// plan_converter and do NOT require decomposition.
    pub fn has_foreach(&self) -> bool {
        self.nodes
            .values()
            .any(|n| matches!(n.node_type, ExecutionNodeType::ForEach { .. }))
    }

    /// Check whether this plan contains any Collect nodes paired with ForEach.
    ///
    /// A Collect node following a ForEach marks the re-assembly point.
    /// Standalone Collect nodes (no ForEach) are pass-throughs.
    pub fn has_foreach_collect_pair(&self) -> bool {
        let has_foreach = self
            .nodes
            .values()
            .any(|n| matches!(n.node_type, ExecutionNodeType::ForEach { .. }));
        let has_collect = self
            .nodes
            .values()
            .any(|n| matches!(n.node_type, ExecutionNodeType::Collect { .. }));
        has_foreach && has_collect
    }

    /// Extract a sub-plan containing all nodes from entry points up to (and including)
    /// the specified target node. The target node becomes the source of a synthetic Output node.
    ///
    /// Used to extract the "prefix" before a ForEach node — everything needed to produce the
    /// list that the ForEach will iterate over.
    ///
    /// The resulting plan has the same InputSlot(s) as the original and a new Output node
    /// connected to `target_node_id`.
    pub fn extract_prefix_to(&self, target_node_id: &str) -> Result<Self, PlannerError> {
        if !self.nodes.contains_key(target_node_id) {
            return Err(PlannerError::Internal(format!(
                "Target node '{}' not found in plan",
                target_node_id
            )));
        }

        // BFS backward from target to find all ancestor nodes (including target)
        let mut ancestors = std::collections::HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(target_node_id.to_string());
        ancestors.insert(target_node_id.to_string());

        // Build reverse adjacency: to_node -> [from_node]
        let mut reverse_adj: HashMap<&str, Vec<&str>> = HashMap::new();
        for edge in &self.edges {
            reverse_adj
                .entry(edge.to_node.as_str())
                .or_default()
                .push(edge.from_node.as_str());
        }

        while let Some(node_id) = queue.pop_front() {
            if let Some(parents) = reverse_adj.get(node_id.as_str()) {
                for &parent in parents {
                    if ancestors.insert(parent.to_string()) {
                        queue.push_back(parent.to_string());
                    }
                }
            }
        }

        // Build the sub-plan with only ancestor nodes
        let mut sub_plan =
            MachinePlan::new(&format!("{} [prefix to {}]", self.name, target_node_id));

        for node_id in &ancestors {
            if let Some(node) = self.nodes.get(node_id) {
                // Skip Output nodes from the original plan — we'll add our own
                if matches!(node.node_type, ExecutionNodeType::Output { .. }) {
                    continue;
                }
                sub_plan.add_node(node.clone());
            }
        }

        // Add edges where both endpoints are in the ancestor set and not Output nodes
        for edge in &self.edges {
            if ancestors.contains(&edge.from_node) && ancestors.contains(&edge.to_node) {
                let from_is_output = self.nodes.get(&edge.from_node).map_or(false, |n| {
                    matches!(n.node_type, ExecutionNodeType::Output { .. })
                });
                let to_is_output = self.nodes.get(&edge.to_node).map_or(false, |n| {
                    matches!(n.node_type, ExecutionNodeType::Output { .. })
                });
                if !from_is_output && !to_is_output {
                    sub_plan.add_edge(edge.clone());
                }
            }
        }

        // Add synthetic Output node connected to the target
        let output_id = format!("{}_prefix_output", target_node_id);
        sub_plan.add_node(MachineNode::output(
            &output_id,
            "prefix_result",
            target_node_id,
        ));
        sub_plan.add_edge(MachinePlanEdge::direct(target_node_id, &output_id));

        sub_plan.validate()?;
        Ok(sub_plan)
    }

    /// Extract the ForEach body as a standalone execution plan.
    ///
    /// Creates a new plan with:
    /// - A synthetic InputSlot with `item_media_urn` (the per-item type, without list tag)
    /// - All nodes between `body_entry` and `body_exit` (inclusive)
    /// - A synthetic Output connected to `body_exit`
    ///
    /// The result can be converted to a ResolvedGraph and executed independently per item.
    pub fn extract_foreach_body(
        &self,
        foreach_node_id: &str,
        item_media_urn: &str,
    ) -> Result<Self, PlannerError> {
        let foreach_node = self.nodes.get(foreach_node_id).ok_or_else(|| {
            PlannerError::Internal(format!("ForEach node '{}' not found", foreach_node_id))
        })?;

        let (body_entry, body_exit) = match &foreach_node.node_type {
            ExecutionNodeType::ForEach {
                body_entry,
                body_exit,
                ..
            } => (body_entry.clone(), body_exit.clone()),
            _ => {
                return Err(PlannerError::Internal(format!(
                    "Node '{}' is not a ForEach node",
                    foreach_node_id
                )));
            }
        };

        // BFS forward from body_entry to find all reachable body nodes
        // Stop at: Output nodes, Collect nodes, and nodes outside the body
        let mut body_nodes = std::collections::HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(body_entry.clone());
        body_nodes.insert(body_entry.clone());

        // Build forward adjacency: from_node -> [(to_node, edge)]
        let mut forward_adj: HashMap<&str, Vec<&str>> = HashMap::new();
        for edge in &self.edges {
            forward_adj
                .entry(edge.from_node.as_str())
                .or_default()
                .push(edge.to_node.as_str());
        }

        while let Some(node_id) = queue.pop_front() {
            // Don't traverse past body_exit
            if node_id == body_exit && node_id != body_entry {
                continue;
            }
            if let Some(children) = forward_adj.get(node_id.as_str()) {
                for &child in children {
                    // Don't include Output or Collect nodes from the original plan
                    if let Some(child_node) = self.nodes.get(child) {
                        if matches!(
                            child_node.node_type,
                            ExecutionNodeType::Output { .. } | ExecutionNodeType::Collect { .. }
                        ) {
                            // Include body_exit but stop here
                            if child == body_exit {
                                body_nodes.insert(child.to_string());
                            }
                            continue;
                        }
                    }
                    if body_nodes.insert(child.to_string()) {
                        queue.push_back(child.to_string());
                    }
                }
            }
        }

        // Ensure body_exit is included
        body_nodes.insert(body_exit.clone());

        // Build the body sub-plan
        let mut body_plan =
            MachinePlan::new(&format!("{} [foreach body {}]", self.name, foreach_node_id));

        // Add synthetic InputSlot for the per-item input
        let input_id = format!("{}_body_input", foreach_node_id);
        body_plan.add_node(MachineNode::input_slot(
            &input_id,
            "item_input",
            item_media_urn,
            InputCardinality::Single,
        ));

        // Add body nodes
        for node_id in &body_nodes {
            if let Some(node) = self.nodes.get(node_id) {
                body_plan.add_node(node.clone());
            }
        }

        // Add edge from synthetic input to body_entry
        body_plan.add_edge(MachinePlanEdge::direct(&input_id, &body_entry));

        // Add edges where both endpoints are body nodes
        for edge in &self.edges {
            if body_nodes.contains(&edge.from_node) && body_nodes.contains(&edge.to_node) {
                // Skip iteration/collection edges — they connect to ForEach/Collect, not body internals
                if matches!(edge.edge_type, EdgeType::Iteration | EdgeType::Collection) {
                    continue;
                }
                body_plan.add_edge(edge.clone());
            }
        }

        // Add synthetic Output connected to body_exit
        let output_id = format!("{}_body_output", foreach_node_id);
        body_plan.add_node(MachineNode::output(&output_id, "item_result", &body_exit));
        body_plan.add_edge(MachinePlanEdge::direct(&body_exit, &output_id));

        body_plan.validate()?;
        Ok(body_plan)
    }

    /// Extract a sub-plan from the specified source node to all reachable Output nodes.
    ///
    /// Used to extract the "suffix" after a Collect node — everything needed to process
    /// the collected results into final output.
    ///
    /// The resulting plan has a synthetic InputSlot connected to `source_node_id` and
    /// preserves the original Output nodes.
    pub fn extract_suffix_from(
        &self,
        source_node_id: &str,
        source_media_urn: &str,
    ) -> Result<Self, PlannerError> {
        if !self.nodes.contains_key(source_node_id) {
            return Err(PlannerError::Internal(format!(
                "Source node '{}' not found in plan",
                source_node_id
            )));
        }

        // BFS forward from source to find all descendants (including source)
        let mut descendants = std::collections::HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(source_node_id.to_string());
        descendants.insert(source_node_id.to_string());

        let mut forward_adj: HashMap<&str, Vec<&str>> = HashMap::new();
        for edge in &self.edges {
            forward_adj
                .entry(edge.from_node.as_str())
                .or_default()
                .push(edge.to_node.as_str());
        }

        while let Some(node_id) = queue.pop_front() {
            if let Some(children) = forward_adj.get(node_id.as_str()) {
                for &child in children {
                    if descendants.insert(child.to_string()) {
                        queue.push_back(child.to_string());
                    }
                }
            }
        }

        let mut sub_plan =
            MachinePlan::new(&format!("{} [suffix from {}]", self.name, source_node_id));

        // Add synthetic InputSlot to feed into source_node
        let input_id = format!("{}_suffix_input", source_node_id);
        sub_plan.add_node(MachineNode::input_slot(
            &input_id,
            "collected_input",
            source_media_urn,
            InputCardinality::Single,
        ));

        // Add descendant nodes (skip the source node itself if it's a Collect —
        // it's being replaced by the InputSlot)
        for node_id in &descendants {
            if node_id == source_node_id {
                continue; // replaced by synthetic InputSlot
            }
            if let Some(node) = self.nodes.get(node_id) {
                // Skip InputSlot nodes from the original plan
                if matches!(node.node_type, ExecutionNodeType::InputSlot { .. }) {
                    continue;
                }
                sub_plan.add_node(node.clone());
            }
        }

        // Connect synthetic input to the successors of source_node
        for edge in &self.edges {
            if edge.from_node == source_node_id && descendants.contains(&edge.to_node) {
                sub_plan.add_edge(MachinePlanEdge::direct(&input_id, &edge.to_node));
            } else if descendants.contains(&edge.from_node)
                && descendants.contains(&edge.to_node)
                && edge.from_node != source_node_id
            {
                sub_plan.add_edge(edge.clone());
            }
        }

        sub_plan.validate()?;
        Ok(sub_plan)
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
        if wire.failed_arg_urn.as_deref() == Some("") {
            return Err("BodyOutcome.failed_arg_urn must be null or non-empty".to_string());
        }
        Ok(Self {
            foreach_token_id: wire.foreach_token_id,
            body_index: wire.body_index,
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
            "media:ext=pdf",
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
            "media:ext=pdf",
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

        let foreach_node = MachineNode::for_each("foreach", "input", "body", "body");
        assert!(!foreach_node.is_cap());
        assert!(foreach_node.is_fan_out());
        assert!(!foreach_node.is_fan_in());
        assert_eq!(foreach_node.cap_urn(), None);

        let collect_node = MachineNode::collect("collect", vec!["a".to_string()]);
        assert!(!collect_node.is_cap());
        assert!(!collect_node.is_fan_out());
        assert!(collect_node.is_fan_in());
    }

    // TEST729: Tests creation and classification of different edge types (Direct, Iteration, Collection, JsonField)
    // Verifies that edge constructors produce correct EdgeType variants
    #[test]
    fn test729_edge_types() {
        let direct = MachinePlanEdge::direct("a", "b");
        assert!(matches!(direct.edge_type, EdgeType::Direct));

        let iteration = MachinePlanEdge::iteration("foreach", "body");
        assert!(matches!(iteration.edge_type, EdgeType::Iteration));

        let collection = MachinePlanEdge::collection("body", "collect");
        assert!(matches!(collection.edge_type, EdgeType::Collection));

        let json_field = MachinePlanEdge::json_field("a", "b", "data");
        assert!(matches!(json_field.edge_type, EdgeType::JsonField { field } if field == "data"));
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
            "media:ext=pdf",
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
            "media:ext=pdf",
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
    // Verifies that edge types like Direct and JsonField correctly round-trip through serde_json
    #[test]
    fn test742_edge_type_serialization() {
        let direct = EdgeType::Direct;
        let json = serde_json::to_string(&direct).unwrap();
        assert_eq!(json, "\"direct\"");

        let deserialized: EdgeType = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, EdgeType::Direct));

        let json_field = EdgeType::JsonField {
            field: "data".to_string(),
        };
        let json = serde_json::to_string(&json_field).unwrap();
        assert!(json.contains("json_field"));
        assert!(json.contains("data"));
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
            input_node: "input".to_string(),
            body_entry: "body".to_string(),
            body_exit: "body".to_string(),
        };
        let json = serde_json::to_string(&foreach_node).unwrap();
        assert!(json.contains("for_each"));
    }

    // TEST744: Tests MachinePlan serialization and deserialization to/from JSON
    // Verifies that complete plans with nodes and edges correctly round-trip through JSON
    #[test]
    fn test744_plan_serialization() {
        let plan = MachinePlan::single_cap(
            "cap:test",
            "media:ext=pdf",
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

    // TEST745: Tests MergeStrategy enum serialization to JSON
    // Verifies that merge strategies like Concat and ZipWith serialize to correct string values
    #[test]
    fn test745_merge_strategy_serialization() {
        let concat = MergeStrategy::Concat;
        let json = serde_json::to_string(&concat).unwrap();
        assert_eq!(json, "\"concat\"");

        let zip = MergeStrategy::ZipWith;
        let json = serde_json::to_string(&zip).unwrap();
        assert_eq!(json, "\"zip_with\"");
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

    // TEST747: Tests creation and validation of Merge node that combines multiple inputs
    // Verifies that Merge nodes with multiple input nodes and a strategy can be added to plans
    #[test]
    fn test747_cap_node_merge() {
        let mut plan = MachinePlan::new("merge_test");

        // Create merge node manually
        let merge_node = MachineNode {
            id: "merge".to_string(),
            node_type: ExecutionNodeType::Merge {
                input_nodes: vec!["a".to_string(), "b".to_string()],
                merge_strategy: MergeStrategy::Concat,
            },
            description: Some("Merge outputs".to_string()),
        };

        plan.nodes
            .insert("a".to_string(), MachineNode::cap("a", "cap:a"));
        plan.nodes
            .insert("b".to_string(), MachineNode::cap("b", "cap:b"));
        plan.nodes.insert("merge".to_string(), merge_node);
        plan.edges.push(MachinePlanEdge::direct("a", "merge"));
        plan.edges.push(MachinePlanEdge::direct("b", "merge"));

        assert!(plan.validate().is_ok());
    }

    // TEST748: Tests creation of Split node that distributes input to multiple outputs
    // Verifies that Split nodes correctly specify an input node and output count
    #[test]
    fn test748_cap_node_split() {
        let split_node = MachineNode {
            id: "split".to_string(),
            node_type: ExecutionNodeType::Split {
                input_node: "input".to_string(),
                output_count: 3,
            },
            description: Some("Split input".to_string()),
        };

        match &split_node.node_type {
            ExecutionNodeType::Split {
                input_node,
                output_count,
            } => {
                assert_eq!(input_node, "input");
                assert_eq!(*output_count, 3);
            }
            _ => panic!("Expected Split node type"),
        }
    }

    // TEST749: Tests get_node() method for looking up nodes by ID in a plan
    // Verifies that existing nodes are found and non-existent nodes return None
    #[test]
    fn test749_get_node() {
        let plan = MachinePlan::single_cap(
            "cap:test",
            "media:ext=pdf",
            "media:ext=png;image",
            "doc_path", // file-path argument name
        );

        assert!(plan.get_node("cap_0").is_some());
        assert!(plan.get_node("input_slot").is_some());
        assert!(plan.get_node("output").is_some());
        assert!(plan.get_node("nonexistent").is_none());
    }

    // Helper: build a plan with ForEach (closed with Collect)
    // Topology: input_slot → cap_0(disbind) → foreach → body_cap_0 → body_cap_1 → collect → cap_post → output
    fn build_foreach_plan_with_collect() -> MachinePlan {
        let mut plan = MachinePlan::new("ForEach test plan");

        // input_slot → cap_0 → foreach --iteration--> body_cap_0 → body_cap_1 --collection--> collect → cap_post → output
        plan.add_node(MachineNode::input_slot(
            "input_slot",
            "input",
            "media:ext=pdf",
            InputCardinality::Single,
        ));
        plan.add_node(MachineNode::cap(
            "cap_0",
            "cap:in=\"media:ext=pdf\";out=\"media:pdf-page;list\"",
        )); // disbind
        plan.add_node(MachineNode::for_each(
            "foreach_0",
            "cap_0",
            "body_cap_0",
            "body_cap_1",
        ));
        plan.add_node(MachineNode::cap(
            "body_cap_0",
            "cap:in=media:pdf-page;out=\"media:enc=utf-8;text\"",
        ));
        plan.add_node(MachineNode::cap(
            "body_cap_1",
            "cap:in=\"media:enc=utf-8;text\";out=\"media:decision;fmt=json;record\"",
        ));
        plan.add_node(MachineNode::collect(
            "collect_0",
            vec!["body_cap_1".to_string()],
        ));
        plan.add_node(MachineNode::cap(
            "cap_post",
            "cap:in=\"media:decision;fmt=json;record\";out=\"media:fmt=json\"",
        ));
        plan.add_node(MachineNode::output("output", "result", "cap_post"));

        plan.add_edge(MachinePlanEdge::direct("input_slot", "cap_0"));
        plan.add_edge(MachinePlanEdge::direct("cap_0", "foreach_0"));
        plan.add_edge(MachinePlanEdge::iteration("foreach_0", "body_cap_0"));
        plan.add_edge(MachinePlanEdge::direct("body_cap_0", "body_cap_1"));
        plan.add_edge(MachinePlanEdge::collection("body_cap_1", "collect_0"));
        plan.add_edge(MachinePlanEdge::direct("collect_0", "cap_post"));
        plan.add_edge(MachinePlanEdge::direct("cap_post", "output"));

        plan
    }

    // Helper: build a plan with unclosed ForEach (no Collect)
    // Topology: input_slot → cap_0(disbind) → foreach → body_cap_0 → output
    fn build_foreach_plan_unclosed() -> MachinePlan {
        let mut plan = MachinePlan::new("Unclosed ForEach test plan");

        plan.add_node(MachineNode::input_slot(
            "input_slot",
            "input",
            "media:ext=pdf",
            InputCardinality::Single,
        ));
        plan.add_node(MachineNode::cap(
            "cap_0",
            "cap:in=\"media:ext=pdf\";out=\"media:pdf-page;list\"",
        ));
        plan.add_node(MachineNode::for_each(
            "foreach_0",
            "cap_0",
            "body_cap_0",
            "body_cap_0",
        ));
        plan.add_node(MachineNode::cap(
            "body_cap_0",
            "cap:in=media:pdf-page;out=\"media:decision;fmt=json;record\"",
        ));
        plan.add_node(MachineNode::output("output", "result", "body_cap_0"));

        plan.add_edge(MachinePlanEdge::direct("input_slot", "cap_0"));
        plan.add_edge(MachinePlanEdge::direct("cap_0", "foreach_0"));
        plan.add_edge(MachinePlanEdge::iteration("foreach_0", "body_cap_0"));
        plan.add_edge(MachinePlanEdge::direct("body_cap_0", "output"));

        plan
    }

    // TEST934: find_first_foreach detects ForEach in a plan
    #[test]
    fn test934_find_first_foreach() {
        let plan = build_foreach_plan_with_collect();
        let foreach_id = plan.find_first_foreach();
        assert_eq!(foreach_id, Some(&"foreach_0".to_string()));
    }

    // TEST935: find_first_foreach returns None for linear plans
    #[test]
    fn test935_find_first_foreach_linear() {
        let plan = MachinePlan::linear_chain(
            &["cap:a", "cap:b"],
            "media:ext=pdf",
            "media:ext=png;image",
            &["input_a", "input_b"],
        );
        assert_eq!(plan.find_first_foreach(), None);
    }

    // TEST936: has_foreach detects ForEach nodes
    #[test]
    fn test936_has_foreach() {
        let foreach_plan = build_foreach_plan_with_collect();
        assert!(
            foreach_plan.has_foreach(),
            "Plan with ForEach+Collect should detect ForEach"
        );

        let linear_plan = MachinePlan::linear_chain(
            &["cap:a"],
            "media:ext=pdf",
            "media:ext=png;image",
            &["input_a"],
        );
        assert!(
            !linear_plan.has_foreach(),
            "Linear plan should not detect ForEach"
        );

        // Standalone Collect (no ForEach) should NOT trigger has_foreach
        let mut standalone_collect_plan = MachinePlan::new("collect_only");
        standalone_collect_plan.add_node(MachineNode::input_slot(
            "input",
            "input",
            "media:enc=utf-8",
            crate::planner::cardinality::InputCardinality::Single,
        ));
        standalone_collect_plan.add_node(MachineNode::cap(
            "cap_0",
            "cap:in=\"media:enc=utf-8\";summarize;out=media:summary",
        ));
        let mut collect_node = MachineNode::collect("collect_0", vec!["cap_0".to_string()]);
        collect_node.node_type = ExecutionNodeType::Collect {
            input_nodes: vec!["cap_0".to_string()],
            output_media_urn: Some("media:list;summary".to_string()),
        };
        standalone_collect_plan.add_node(collect_node);
        standalone_collect_plan.add_node(MachineNode::output("output", "result", "collect_0"));
        assert!(
            !standalone_collect_plan.has_foreach(),
            "Plan with standalone Collect (no ForEach) should NOT trigger has_foreach"
        );
    }

    // TEST937: extract_prefix_to extracts input_slot -> cap_0 as a standalone plan
    #[test]
    fn test937_extract_prefix_to() {
        let plan = build_foreach_plan_with_collect();

        // Extract prefix up to cap_0 (the disbind cap that produces the list)
        let prefix = plan.extract_prefix_to("cap_0").unwrap();

        // Should have: input_slot, cap_0, and a synthetic output
        assert_eq!(prefix.nodes.len(), 3);
        assert!(prefix.get_node("input_slot").is_some());
        assert!(prefix.get_node("cap_0").is_some());
        assert!(prefix.get_node("cap_0_prefix_output").is_some());
        assert_eq!(prefix.entry_nodes.len(), 1);
        assert_eq!(prefix.output_nodes.len(), 1);
        assert!(prefix.validate().is_ok());

        // Verify topological order works (no cycles)
        let order = prefix.topological_order().unwrap();
        assert_eq!(order.len(), 3);
    }

    // TEST754: extract_prefix_to with nonexistent node returns error
    #[test]
    fn test754_extract_prefix_nonexistent() {
        let plan = build_foreach_plan_with_collect();
        let result = plan.extract_prefix_to("nonexistent");
        assert!(result.is_err());
    }

    // TEST755: extract_foreach_body extracts body as standalone plan
    #[test]
    fn test755_extract_foreach_body() {
        let plan = build_foreach_plan_with_collect();

        let body = plan
            .extract_foreach_body("foreach_0", "media:pdf-page")
            .unwrap();

        // Should have: synthetic input, body_cap_0, body_cap_1, synthetic output
        assert_eq!(body.nodes.len(), 4);
        assert!(body.get_node("foreach_0_body_input").is_some());
        assert!(body.get_node("body_cap_0").is_some());
        assert!(body.get_node("body_cap_1").is_some());
        assert!(body.get_node("foreach_0_body_output").is_some());
        assert_eq!(body.entry_nodes.len(), 1);
        assert_eq!(body.output_nodes.len(), 1);
        assert!(body.validate().is_ok());

        // Verify it does NOT contain ForEach or Collect nodes
        assert!(!body.has_foreach());

        // Verify the synthetic InputSlot has the item media URN
        if let Some(input_node) = body.get_node("foreach_0_body_input") {
            match &input_node.node_type {
                ExecutionNodeType::InputSlot {
                    expected_media_urn,
                    cardinality,
                    ..
                } => {
                    assert_eq!(expected_media_urn, "media:pdf-page");
                    assert!(matches!(cardinality, InputCardinality::Single));
                }
                _ => panic!("Expected InputSlot node"),
            }
        }

        // Verify topological order
        let order = body.topological_order().unwrap();
        assert_eq!(order.len(), 4);
    }

    // TEST756: extract_foreach_body for unclosed ForEach (single body cap)
    #[test]
    fn test756_extract_foreach_body_unclosed() {
        let plan = build_foreach_plan_unclosed();

        let body = plan
            .extract_foreach_body("foreach_0", "media:pdf-page")
            .unwrap();

        // Should have: synthetic input, body_cap_0, synthetic output
        assert_eq!(body.nodes.len(), 3);
        assert!(body.get_node("foreach_0_body_input").is_some());
        assert!(body.get_node("body_cap_0").is_some());
        assert!(body.get_node("foreach_0_body_output").is_some());
        assert!(body.validate().is_ok());
        assert!(!body.has_foreach());
    }

    // TEST757: extract_foreach_body fails for non-ForEach node
    #[test]
    fn test757_extract_foreach_body_wrong_type() {
        let plan = build_foreach_plan_with_collect();
        let result = plan.extract_foreach_body("cap_0", "media:pdf-page");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not a ForEach node"), "got: {}", err);
    }

    // TEST758: extract_suffix_from extracts collect → cap_post → output
    #[test]
    fn test758_extract_suffix_from() {
        let plan = build_foreach_plan_with_collect();

        let suffix = plan
            .extract_suffix_from("collect_0", "media:decision;fmt=json;record")
            .unwrap();

        // Should have: synthetic input, cap_post, output
        assert_eq!(suffix.nodes.len(), 3);
        assert!(suffix.get_node("collect_0_suffix_input").is_some());
        assert!(suffix.get_node("cap_post").is_some());
        assert!(suffix.get_node("output").is_some());
        assert_eq!(suffix.entry_nodes.len(), 1);
        assert_eq!(suffix.output_nodes.len(), 1);
        assert!(suffix.validate().is_ok());

        // Should not contain ForEach/Collect
        assert!(!suffix.has_foreach());
    }

    // TEST759: extract_suffix_from fails for nonexistent node
    #[test]
    fn test759_extract_suffix_nonexistent() {
        let plan = build_foreach_plan_with_collect();
        let result = plan.extract_suffix_from("nonexistent", "media:whatever");
        assert!(result.is_err());
    }

    // TEST760: Full decomposition roundtrip — prefix + body + suffix cover all cap nodes
    #[test]
    fn test760_decomposition_covers_all_caps() {
        let plan = build_foreach_plan_with_collect();

        // Get all original cap node IDs
        let original_caps: std::collections::HashSet<String> = plan
            .nodes
            .values()
            .filter(|n| n.is_cap())
            .map(|n| n.id.clone())
            .collect();
        assert_eq!(original_caps.len(), 4); // cap_0, body_cap_0, body_cap_1, cap_post

        let prefix = plan.extract_prefix_to("cap_0").unwrap();
        let body = plan
            .extract_foreach_body("foreach_0", "media:pdf-page")
            .unwrap();
        let suffix = plan
            .extract_suffix_from("collect_0", "media:decision;fmt=json;record")
            .unwrap();

        // Collect cap nodes from each sub-plan
        let prefix_caps: std::collections::HashSet<String> = prefix
            .nodes
            .values()
            .filter(|n| n.is_cap())
            .map(|n| n.id.clone())
            .collect();
        let body_caps: std::collections::HashSet<String> = body
            .nodes
            .values()
            .filter(|n| n.is_cap())
            .map(|n| n.id.clone())
            .collect();
        let suffix_caps: std::collections::HashSet<String> = suffix
            .nodes
            .values()
            .filter(|n| n.is_cap())
            .map(|n| n.id.clone())
            .collect();

        // Union of all sub-plan caps should equal original caps
        let mut all_caps = prefix_caps;
        all_caps.extend(body_caps);
        all_caps.extend(suffix_caps);
        assert_eq!(
            all_caps,
            original_caps,
            "Decomposition should cover all cap nodes. Missing: {:?}",
            original_caps.difference(&all_caps).collect::<Vec<_>>()
        );
    }

    // TEST761: Prefix sub-plan can be topologically sorted (is a valid DAG)
    #[test]
    fn test761_prefix_is_dag() {
        let plan = build_foreach_plan_with_collect();
        let prefix = plan.extract_prefix_to("cap_0").unwrap();
        assert!(prefix.topological_order().is_ok());
    }

    // TEST762: Body sub-plan can be topologically sorted (is a valid DAG)
    #[test]
    fn test762_body_is_dag() {
        let plan = build_foreach_plan_with_collect();
        let body = plan
            .extract_foreach_body("foreach_0", "media:pdf-page")
            .unwrap();
        assert!(body.topological_order().is_ok());
    }

    // TEST763: Suffix sub-plan can be topologically sorted (is a valid DAG)
    #[test]
    fn test763_suffix_is_dag() {
        let plan = build_foreach_plan_with_collect();
        let suffix = plan
            .extract_suffix_from("collect_0", "media:decision;fmt=json;record")
            .unwrap();
        assert!(suffix.topological_order().is_ok());
    }

    // TEST764: extract_prefix_to with InputSlot as target (trivial prefix)
    #[test]
    fn test764_extract_prefix_to_input_slot() {
        let plan = build_foreach_plan_with_collect();
        let prefix = plan.extract_prefix_to("input_slot").unwrap();

        // Should have: input_slot + synthetic output
        assert_eq!(prefix.nodes.len(), 2);
        assert!(prefix.validate().is_ok());
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
        plan.add_node(MachineNode::for_each_token(
            "foreach_0",
            "input_slot_0",
            "tok-cap",
            "tok-cap",
            "tok-foreach".parse().unwrap(),
        ));

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
        plan.add_node(MachineNode::for_each_token(
            "foreach_0",
            "input_slot_0",
            "tok-cap",
            "tok-cap",
            "tok-plan-foreach".parse().unwrap(),
        ));

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
    }
}
