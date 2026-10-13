//! Machine notation parser — pest-generated PEG parser plus
//! anchor-realization layer.
//!
//! Parses the machine notation format into a `Machine` using a
//! formal PEG grammar defined in `machine.pest`. The parser is
//! a two-phase pipeline:
//!
//! 1. **Lexical / grammatical** — pest produces an AST of
//!    headers and wirings. Failures here surface as
//!    `MachineSyntaxError`.
//! 2. **Resolution** — the wirings are partitioned into
//!    connected components (one component per maximal set of
//!    wirings sharing node names), each component is fed to
//!    `resolve::resolve_wiring_set`, and the resulting
//!    `MachineStrand`s are assembled into a `Machine` in the
//!    order each component first appears textually. Failures
//!    here surface as `MachineAbstractionError`.
//!
//! The combined result type is `MachineParseError`.
//!
//! ## Grammar
//!
//! The grammar is `machine.pest`; `syntax.rs` is its one reader and yields the
//! statements this module resolves. A wiring is
//! `sources -> step -> targets`: members that may name their arg or be
//! constants, a step with an optional policy, and targets that may name the
//! output they take (`capdag/docs/09-machine-notation.md`).
//!
//! ## Strand boundary discovery
//!
//! Two wirings belong to the same `MachineStrand` iff there
//! exists a path through the wiring set, hopping along shared
//! node-name endpoints, that connects them. Connected
//! components are computed via union-find. The strand list in
//! the resulting `Machine` is in **first-appearance order**:
//! the strand whose earliest wiring appears first in the
//! textual input comes first. A constant is not a node and
//! connects nothing.
//!
//! ## The media URN of a node
//!
//! Every node has one URN, derived from the wirings that touch it; where a
//! member stands in its group never decides it (09 §The URN of a node):
//!
//! - a node some step **produces** has the URN of what that output carries;
//! - a node **nothing produces**, as a **named** member, has the accepted
//!   type of the arg it names;
//! - as an **unnamed** member that nothing else gives a URN, it has the
//!   accepted type of the step's main input — at most one such member per
//!   wiring, and only while no named member bound the main input
//!   (`UntypedMachineInput` otherwise);
//! - URNs given to one node must be on one specialization chain
//!   (`NodeMediaConflict` otherwise); the node has the most specific.

use std::collections::HashMap;

use pest_derive::Parser;

use crate::cap::definition::Cap;
use crate::cap::registry::FabricRegistry;
use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::MediaUrn;
use crate::FabricRegistryError;

use super::error::{MachineAbstractionError, MachineParseError, MachineSyntaxError};
use super::graph::{BindingSource, Machine, MachineStrand, NodeId};
use super::resolve::{resolve_pre_interned, ForEachIdentity, PreInternedWiring};
use super::syntax::{parse_syntax, StepPolicy, SyntaxStatementKind, SyntaxValue};

#[derive(Parser)]
#[grammar = "machine/machine.pest"]
pub struct MachineParser;

/// What a member of a wiring gives its arg.
enum RawValue {
    /// A node, by the name the text gives it.
    Node(String),
    Constant(serde_json::Value),
}

/// One member of a wiring's sources.
struct RawMember {
    /// The arg the member names, when it names one.
    name: Option<String>,
    value: RawValue,
}

/// One wiring as it comes off the syntax tree, with raw alias
/// names. Resolution happens in two more passes after this.
struct RawWiring {
    /// The members, in the order the user wrote them. The order decides
    /// nothing but the item order of a gather.
    sources: Vec<RawMember>,
    /// Cap header alias.
    cap_alias: String,
    policy: StepPolicy,
    /// The targets: the output each names (`None`: the main output) and the
    /// node it goes to.
    targets: Vec<(Option<String>, String)>,
    /// Index of this wiring in the textual input. Used to
    /// order connected components by first appearance.
    position: usize,
}

impl RawWiring {
    /// The name of every node this wiring consumes, in the order written.
    fn source_nodes(&self) -> impl Iterator<Item = &String> {
        self.sources.iter().filter_map(|m| match &m.value {
            RawValue::Node(name) => Some(name),
            RawValue::Constant(_) => None,
        })
    }

    /// The name of every node this wiring touches.
    fn nodes(&self) -> impl Iterator<Item = &String> {
        self.source_nodes().chain(self.targets.iter().map(|(_, node)| node))
    }
}

/// Per-strand mapping from user-written node name to the
/// `NodeId` the parser allocated for that name. Returned by
/// `parse_machine_with_node_names` for callers that need to
/// preserve the user's node-name identity through the
/// resolved-machine layer (the orchestrator's
/// `ResolvedGraph` is keyed on these names).
pub type StrandNodeNames = HashMap<String, NodeId>;

/// Parse machine notation into a `Machine`, discarding the
/// per-strand user node names.
///
/// Two-phase: pest grammar parsing → resolver. Either phase
/// may fail; the combined error type is `MachineParseError`.
/// The cap registry is required by the resolver to look up
/// each cap's `args` list and run source-to-arg matching.
pub fn parse_machine(input: &str, registry: &FabricRegistry) -> Result<Machine, MachineParseError> {
    let (machine, _names) = parse_machine_with_node_names(input, registry)?;
    Ok(machine)
}

/// Parse machine notation into a `Machine` AND a per-strand
/// mapping from user-written node name to the resolved
/// `NodeId`. The strand vec and the names vec are aligned —
/// `names[i]` is the name map for `machine.strands()[i]`.
///
/// Used by callers that need to preserve user-facing node
/// identity through the resolved-machine layer (the
/// orchestrator's `ResolvedGraph`, the `bin/capdag` binary's
/// input-node finder).
pub fn parse_machine_with_node_names(
    input: &str,
    registry: &FabricRegistry,
) -> Result<(Machine, Vec<StrandNodeNames>), MachineParseError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(MachineSyntaxError::Empty.into());
    }

    // Phase 1: the text, read into statements.
    let statements = parse_syntax(input)?;

    // Phase 2: headers and wirings.
    let mut headers: Vec<(String, CapUrn, usize)> = Vec::new();
    let mut wirings: Vec<RawWiring> = Vec::new();
    for statement in statements {
        match statement.kind {
            SyntaxStatementKind::CapHeader { alias, cap_urn } => {
                let parsed = CapUrn::from_string(&cap_urn.value).map_err(|e| {
                    MachineSyntaxError::InvalidCapUrn {
                        alias: alias.value.clone(),
                        details: format!("{}", e),
                    }
                })?;
                headers.push((alias.value, parsed, statement.position));
            }
            SyntaxStatementKind::MachineHeader { alias, reference } => {
                return Err(MachineSyntaxError::MachineReferenceWithoutSource {
                    alias: alias.value,
                    reference: reference.value,
                }
                .into());
            }
            SyntaxStatementKind::Wiring(wiring) => wirings.push(RawWiring {
                sources: wiring
                    .sources
                    .into_iter()
                    .map(|member| RawMember {
                        name: member.name.map(|n| n.value),
                        value: match member.value {
                            SyntaxValue::Node(node) => RawValue::Node(node.value),
                            SyntaxValue::Constant(constant) => RawValue::Constant(constant.value),
                        },
                    })
                    .collect(),
                cap_alias: wiring.step.value,
                policy: wiring.policy,
                targets: wiring
                    .targets
                    .into_iter()
                    .map(|target| (target.name.map(|n| n.value), target.node.value))
                    .collect(),
                position: statement.position,
            }),
        }
    }

    // Phase 3: alias map with duplicate check.
    let mut alias_map: HashMap<String, (CapUrn, usize)> = HashMap::new();
    for (alias, cap_urn, position) in &headers {
        if let Some((_, first_pos)) = alias_map.get(alias) {
            return Err(MachineSyntaxError::DuplicateAlias {
                alias: alias.clone(),
                first_position: *first_pos,
            }
            .into());
        }
        alias_map.insert(alias.clone(), (cap_urn.clone(), *position));
    }

    if wirings.is_empty() && !headers.is_empty() {
        return Err(MachineSyntaxError::NoEdges.into());
    }
    if wirings.is_empty() {
        return Err(MachineSyntaxError::Empty.into());
    }

    // Phase 3b: cap-alias resolution against the fabric registry.
    //
    // A wiring's cap-position name (the alias in cap position) that is NOT defined by
    // a local header is taken to be a fabric **cap alias** and resolved
    // through the registry: an identifier without a local definition is an
    // alias, resolved before we conclude it is undefined. The resolved cap
    // URN is injected into `alias_map` as if a header had defined it, so
    // the rest of the pipeline is unchanged. Media URNs never appear in a
    // wiring (they are implicit), so only cap aliases are resolved here; an
    // alias that points at a media URN in cap position is a hard error.
    //
    // The lookup is synchronous and in-memory (`resolve_alias_cached`); the
    // async wrapper (`parse_machine_with_node_names_async`) pre-warms the
    // alias cache before this runs. A name that resolves to nothing is left
    // alone — phase 4 surfaces it as `UndefinedAlias`.
    {
        // Collect the distinct cap-position names first to avoid mutating
        // `alias_map` while iterating the wirings.
        let mut unresolved_cap_names: Vec<String> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for w in &wirings {
            if !alias_map.contains_key(&w.cap_alias) && seen.insert(w.cap_alias.clone()) {
                unresolved_cap_names.push(w.cap_alias.clone());
            }
        }
        for name in unresolved_cap_names {
            // A token containing ':' is a URN, not an alias — but the
            // grammar's `alias` rule already forbids ':' in a cap-position
            // name, so any name reaching here is alias-shaped. Resolve it.
            let Some(target) = registry.resolve_alias_cached(&name) else {
                continue; // not a known alias → phase 4 yields UndefinedAlias
            };
            let cap_urn =
                CapUrn::from_string(&target).map_err(|_| MachineSyntaxError::AliasNotACap {
                    alias: name.clone(),
                    target: target.clone(),
                })?;
            // Synthetic position past every real statement so duplicate
            // detection (which compares positions) never mistakes an
            // injected alias for a user-written header.
            let synthetic_pos = headers.len() + wirings.len() + alias_map.len();
            alias_map.insert(name, (cap_urn, synthetic_pos));
        }
    }

    // Phase 4: every step is defined, and no node bears a step's name.
    for w in &wirings {
        if !alias_map.contains_key(&w.cap_alias) {
            return Err(MachineSyntaxError::UndefinedAlias {
                alias: w.cap_alias.clone(),
            }
            .into());
        }
        if let Some(node) = w.nodes().find(|node| alias_map.contains_key(*node)) {
            return Err(MachineSyntaxError::NodeAliasCollision {
                name: node.clone(),
                alias: node.clone(),
            }
            .into());
        }
    }

    // Phase 5: connected-components partition by shared node
    // name. Union-find over wiring indices, where two wirings
    // are unioned iff they share at least one node name. A wiring that
    // touches no node at all (`() -> tick -> ()`) is a strand of its own.
    let n = wirings.len();
    let mut union = UnionFind::new(n);

    // Map: node name → index of the first wiring that touched
    // it. As we process wirings in order, any wiring that
    // touches a previously-seen node name is unioned with the
    // earlier wiring.
    let mut node_first_wiring: HashMap<String, usize> = HashMap::new();
    for (w_idx, w) in wirings.iter().enumerate() {
        for node_name in w.nodes() {
            if let Some(&earlier) = node_first_wiring.get(node_name) {
                union.union(earlier, w_idx);
            } else {
                node_first_wiring.insert(node_name.clone(), w_idx);
            }
        }
    }

    // Group wirings by their union-find root. Order roots by
    // the smallest wiring index in each group (= first-
    // appearance order).
    let mut groups: HashMap<usize, Vec<usize>> = HashMap::new();
    for w_idx in 0..n {
        let root = union.find(w_idx);
        groups.entry(root).or_default().push(w_idx);
    }
    let mut group_min_idx: Vec<(usize, usize)> = groups
        .iter()
        .map(|(&root, members)| {
            let min_idx = *members.iter().min().expect("non-empty group");
            (root, min_idx)
        })
        .collect();
    group_min_idx.sort_by_key(|(_, min_idx)| *min_idx);

    // Phase 6: per-component node typing, pre-interning and resolution.
    //
    // For each connected component (= strand), every node is given its URN
    // (`type_nodes`), then `NodeId`s are allocated in the order user node
    // names are encountered (walking the wirings in their textual order).
    // The resolver receives `PreInternedWiring`s that already reference
    // NodeIds, plus the parallel `nodes: Vec<MediaUrn>` table. Two distinct
    // user node names that happen to share a media URN stay distinct NodeIds
    // — that's the parser's identity contract.
    let mut strands: Vec<MachineStrand> = Vec::with_capacity(group_min_idx.len());
    let mut strand_node_names: Vec<StrandNodeNames> = Vec::with_capacity(group_min_idx.len());
    for (strand_index, (root, _)) in group_min_idx.iter().enumerate() {
        let mut member_indices = groups[root].clone();
        member_indices.sort();
        let members: Vec<&RawWiring> = member_indices.iter().map(|i| &wirings[*i]).collect();

        let node_media = type_nodes(&members, &alias_map, registry, strand_index)?;

        let mut nodes: Vec<MediaUrn> = Vec::new();
        let mut name_to_id: StrandNodeNames = HashMap::new();
        let mut intern = |name: &str| -> NodeId {
            if let Some(id) = name_to_id.get(name) {
                return *id;
            }
            let urn = node_media
                .get(name)
                .cloned()
                .expect("every node of the strand was given its URN");
            let id = nodes.len() as NodeId;
            nodes.push(urn);
            name_to_id.insert(name.to_string(), id);
            id
        };

        let mut pre_interned: Vec<PreInternedWiring> = Vec::with_capacity(members.len());
        for w in &members {
            let (cap_urn, _) = alias_map
                .get(&w.cap_alias)
                .expect("cap alias was validated above");

            let mut named_sources: Vec<(String, BindingSource)> = Vec::new();
            let mut source_node_ids: Vec<NodeId> = Vec::new();
            for member in &w.sources {
                match (&member.name, &member.value) {
                    (Some(name), RawValue::Node(node)) => {
                        named_sources.push((name.clone(), BindingSource::Node(intern(node))))
                    }
                    (Some(name), RawValue::Constant(value)) => {
                        named_sources.push((name.clone(), BindingSource::Constant(value.clone())))
                    }
                    (None, RawValue::Node(node)) => source_node_ids.push(intern(node)),
                    (None, RawValue::Constant(_)) => {
                        unreachable!("the syntax refuses a constant that names no arg")
                    }
                }
            }
            let targets: Vec<(Option<String>, NodeId)> = w
                .targets
                .iter()
                .map(|(name, node)| (name.clone(), intern(node)))
                .collect();

            pre_interned.push(PreInternedWiring {
                // The notation (static-machine) path has no resolved-strand step to
                // inherit from, so each parsed wiring's edge gets its own freshly
                // minted stable identity.
                token_id: crate::planner::StepToken::mint(),
                foreach_identity: ForEachIdentity::MintIfRequired,
                cap_urn: cap_urn.clone(),
                named_sources,
                source_node_ids,
                targets,
                policy: w.policy,
            });
        }

        let strand = resolve_pre_interned(nodes, &pre_interned, registry, strand_index)?;
        strands.push(strand);
        strand_node_names.push(name_to_id);
    }

    Ok((Machine::from_resolved_strands(strands), strand_node_names))
}

/// Give `node` the URN `urn`, beside any URN it was already given: the two must
/// be on one specialization chain, and the node holds the more specific.
fn give(
    node_media: &mut HashMap<String, MediaUrn>,
    node: &str,
    urn: MediaUrn,
) -> Result<(), MachineParseError> {
    match node_media.get(node) {
        None => {
            node_media.insert(node.to_string(), urn);
        }
        Some(existing) => {
            if !existing.is_comparable(&urn).unwrap_or(false) {
                return Err(MachineAbstractionError::NodeMediaConflict {
                    node: node.to_string(),
                    first: existing.to_string(),
                    second: urn.to_string(),
                }
                .into());
            }
            if urn.specificity() > existing.specificity() {
                node_media.insert(node.to_string(), urn);
            }
        }
    }
    Ok(())
}

/// Give every node of one strand its media URN (see the module documentation
/// and 09 §The URN of a node). The result does not depend on the order of the
/// wirings: what produces a node and what names it are read first, for every
/// wiring; only then is each wiring asked for the one unnamed machine input
/// its main input can type, against what the first two passes left untyped.
fn type_nodes(
    wirings: &[&RawWiring],
    alias_map: &HashMap<String, (CapUrn, usize)>,
    registry: &FabricRegistry,
    strand_index: usize,
) -> Result<HashMap<String, MediaUrn>, MachineParseError> {
    let cap_of = |w: &RawWiring| -> Result<(CapUrn, Cap), MachineParseError> {
        let (cap_urn, _) = alias_map
            .get(&w.cap_alias)
            .expect("cap alias was validated above");
        let cap = registry.get_cached_cap(&cap_urn.to_string()).ok_or_else(|| {
            MachineAbstractionError::UnknownCap {
                cap_urn: cap_urn.to_string(),
            }
        })?;
        Ok((cap_urn.clone(), cap))
    };
    let media = |urn: &str| {
        MediaUrn::from_string(urn)
            .expect("cap registry invariant: a definition's media URNs are valid")
    };
    // What a cap's main input accepts: its `in=`. `None` when it takes none.
    let main_input_of = |w: &RawWiring, cap_urn: &CapUrn| -> Result<Option<MediaUrn>, MachineParseError> {
        let in_media = cap_urn.in_media_urn().map_err(|e| MachineSyntaxError::InvalidMediaUrn {
            alias: w.cap_alias.clone(),
            details: format!("in= spec: {}", e),
        })?;
        let void = media(crate::urn::media_urn::MEDIA_VOID);
        Ok((!in_media.is_equivalent(&void).unwrap_or(false)).then_some(in_media))
    };

    let mut node_media: HashMap<String, MediaUrn> = HashMap::new();

    // What produces a node, and — for a node nothing produces — what names it.
    let produced: std::collections::HashSet<&String> = wirings
        .iter()
        .flat_map(|w| w.targets.iter().map(|(_, node)| node))
        .collect();
    for w in wirings {
        let (cap_urn, cap) = cap_of(w)?;
        for (name, node) in &w.targets {
            let urn = match name {
                // The main output: the cap URN's `out=`. (A cap with none is
                // refused by the resolver, which holds the wiring to it.)
                None => cap_urn.out_media_urn().map_err(|e| MachineSyntaxError::InvalidMediaUrn {
                    alias: w.cap_alias.clone(),
                    details: format!("out= spec: {}", e),
                })?,
                Some(name) => {
                    let output = cap.output_named(name).ok_or_else(|| {
                        MachineAbstractionError::UnknownOutputName {
                            strand_index,
                            cap_urn: cap_urn.to_string(),
                            name: name.clone(),
                            declared: cap.get_outputs().iter().map(|o| o.name.clone()).collect(),
                        }
                    })?;
                    match (&output.carries, main_input_of(w, &cap_urn)?) {
                        // An output that carries the input holds the input's type.
                        (Some(_), Some(in_media)) => in_media,
                        _ => media(&output.media_urn),
                    }
                }
            };
            give(&mut node_media, node, urn)?;
        }
        for member in &w.sources {
            let (Some(name), RawValue::Node(node)) = (&member.name, &member.value) else {
                continue;
            };
            let arg = cap.arg_named(name).ok_or_else(|| MachineAbstractionError::UnknownArgName {
                strand_index,
                cap_urn: cap_urn.to_string(),
                name: name.clone(),
                declared: cap.args.iter().map(|arg| arg.name.clone()).collect(),
            })?;
            // A node some step produces has that output's type; whether it
            // conforms to the arg it is named into is the resolver's to say
            // (`SourceDoesNotConformToArg`). The name types an input of the
            // machine, which has no other type.
            if !produced.contains(node) {
                give(&mut node_media, node, media(arg.accepted_urn()))?;
            }
        }
    }

    // What a step's MAIN input says of the unnamed member that feeds it. A
    // step has one data input, so where exactly one unnamed member can be it,
    // that member's node is given the type the main input accepts:
    //
    // - a member nothing above gave a URN is an input of the machine whose arg
    //   nothing says: one such member per wiring is the main input, and only
    //   that (`UntypedMachineInput` otherwise);
    // - failing that, the one unnamed member whose URN is on the main input's
    //   specialization chain is refined to it — a consumer that asks for a
    //   tighter type than the producer declared states the type of what flows
    //   there, and the run holds the producer to it.
    //
    // Read against what the passes above left, for every wiring, before any
    // of it is applied: the order of the wirings changes nothing.
    let mut from_main_input: Vec<(String, MediaUrn)> = Vec::new();
    for w in wirings {
        let unnamed: Vec<&String> = w
            .sources
            .iter()
            .filter_map(|m| match (&m.name, &m.value) {
                (None, RawValue::Node(node)) => Some(node),
                _ => None,
            })
            .collect();
        if unnamed.is_empty() {
            continue;
        }
        let (cap_urn, cap) = cap_of(w)?;
        let main_input = main_input_of(w, &cap_urn)?;
        let main_named = main_input.as_ref().is_some_and(|in_media| {
            cap.args
                .iter()
                .find(|arg| arg.is_main_input(in_media))
                .is_some_and(|main| w.sources.iter().any(|m| m.name.as_deref() == Some(main.name.as_str())))
        });
        let untyped: Vec<String> = unnamed
            .iter()
            .filter(|node| !node_media.contains_key(**node))
            .map(|node| (*node).clone())
            .collect();
        match (untyped.as_slice(), &main_input) {
            ([], Some(in_media)) if !main_named => {
                let on_chain: Vec<&String> = unnamed
                    .iter()
                    .filter(|node| node_media[**node].is_comparable(in_media).unwrap_or(false))
                    .copied()
                    .collect();
                if let [node] = on_chain.as_slice() {
                    from_main_input.push(((*node).clone(), in_media.clone()));
                }
            }
            ([], _) => {}
            ([node], Some(in_media)) if !main_named => {
                from_main_input.push((node.clone(), in_media.clone()))
            }
            _ => {
                return Err(MachineAbstractionError::UntypedMachineInput {
                    position: w.position,
                    cap_urn: cap_urn.to_string(),
                    members: untyped,
                }
                .into())
            }
        }
    }
    for (node, urn) in from_main_input {
        give(&mut node_media, &node, urn)?;
    }

    Ok(node_media)
}

/// Run only the pest grammar parse over `input` and return the
/// cap URN strings declared in `[alias cap:...]` headers, in
/// textual order with duplicates suppressed.
///
/// This is a pure syntactic pass: it does not consult the
/// registry, validate cap-URN structure beyond what pest's
/// grammar enforces, or run any of the resolver phases. It
/// exists so the async wrappers below can warm the cap cache
/// before delegating to the existing sync resolver, without
/// duplicating the resolver itself.
fn extract_header_cap_urns(input: &str) -> Result<Vec<String>, MachineParseError> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut urns: Vec<String> = Vec::new();
    for statement in parse_syntax(input.trim())? {
        if let SyntaxStatementKind::CapHeader { cap_urn, .. } = statement.kind {
            if seen.insert(cap_urn.value.clone()) {
                urns.push(cap_urn.value);
            }
        }
    }
    Ok(urns)
}

/// Run only the pest grammar parse over `input` and return, in textual
/// order with duplicates suppressed, the cap-position names in wirings
/// (the alias in cap position) that are NOT defined by a local header. These are the
/// candidate **cap aliases** the async warm-up must hydrate before the
/// synchronous resolver runs.
fn extract_unresolved_cap_alias_names(input: &str) -> Result<Vec<String>, MachineParseError> {
    let statements = parse_syntax(input.trim())?;
    // First pass: collect every header alias (these shadow aliases).
    let header_aliases: std::collections::HashSet<&str> = statements
        .iter()
        .filter_map(|statement| match &statement.kind {
            SyntaxStatementKind::CapHeader { alias, .. }
            | SyntaxStatementKind::MachineHeader { alias, .. } => Some(alias.value.as_str()),
            SyntaxStatementKind::Wiring(_) => None,
        })
        .collect();
    // Second pass: collect cap-position names not in that set.
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut names: Vec<String> = Vec::new();
    for statement in &statements {
        if let SyntaxStatementKind::Wiring(wiring) = &statement.kind {
            let name = wiring.step.value.as_str();
            if !header_aliases.contains(name) && seen.insert(name) {
                names.push(name.to_string());
            }
        }
    }
    Ok(names)
}

/// Async parallel of `parse_machine_with_node_names`.
///
/// The cap definitions the notation names are fetched into the registry's
/// cache first — every header's cap, and every registered cap alias used in
/// step position — so the synchronous resolver finds them there.
pub async fn parse_machine_with_node_names_async(
    input: &str,
    registry: &FabricRegistry,
) -> Result<(Machine, Vec<StrandNodeNames>), MachineParseError> {
    let cap_urns = extract_header_cap_urns(input)?;
    for urn in &cap_urns {
        registry
            .get_cap(urn)
            .await
            .map_err(|e| MachineSyntaxError::InvalidCapUrn {
                alias: urn.clone(),
                details: format!("registry could not resolve cap: {}", e),
            })?;
    }

    // Warm cap aliases used in wirings without a local header. A name that
    // is not a registered alias is left alone (the sync resolver reports it
    // as UndefinedAlias); a name whose alias points at a media URN is a
    // hard error in cap position.
    let alias_names = extract_unresolved_cap_alias_names(input)?;
    for name in &alias_names {
        match registry
            .resolve_alias_typed(name, Some(crate::AliasTargetKind::Cap))
            .await
        {
            Ok(target_cap_urn) => {
                registry.get_cap(&target_cap_urn).await.map_err(|e| {
                    MachineSyntaxError::InvalidCapUrn {
                        alias: name.clone(),
                        details: format!(
                            "alias resolved to cap '{}' but the registry could not load it: {}",
                            target_cap_urn, e
                        ),
                    }
                })?;
            }
            Err(FabricRegistryError::NotFound(_)) => {
                // Not a registered alias — defer to the sync resolver's
                // UndefinedAlias. (A genuinely undefined local name.)
            }
            Err(FabricRegistryError::ValidationError(details)) => {
                // Alias exists but points at a media URN (wrong kind for a
                // cap position), or the name is malformed. Surface now.
                return Err(MachineSyntaxError::InvalidCapUrn {
                    alias: name.clone(),
                    details,
                }
                .into());
            }
            Err(e) => {
                return Err(MachineSyntaxError::InvalidCapUrn {
                    alias: name.clone(),
                    details: format!("alias resolution failed: {}", e),
                }
                .into());
            }
        }
    }

    parse_machine_with_node_names(input, registry)
}

/// Async parallel of `parse_machine`.
///
/// Wraps `parse_machine_with_node_names_async`, discarding the
/// per-strand user node names.
pub async fn parse_machine_async(
    input: &str,
    registry: &FabricRegistry,
) -> Result<Machine, MachineParseError> {
    let (machine, _names) = parse_machine_with_node_names_async(input, registry).await?;
    Ok(machine)
}

impl Machine {
    /// Parse machine notation into a `Machine`.
    ///
    /// Combined lexical / grammatical / resolution parse. The
    /// cap registry is required to resolve each cap's argument
    /// structure during anchor realization.
    pub fn from_string(input: &str, registry: &FabricRegistry) -> Result<Self, MachineParseError> {
        parse_machine(input, registry)
    }
}

/// Tiny union-find used for connected-components partition.
struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<u32>,
}

impl UnionFind {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
            rank: vec![0; n],
        }
    }

    fn find(&mut self, x: usize) -> usize {
        if self.parent[x] != x {
            let root = self.find(self.parent[x]);
            self.parent[x] = root;
        }
        self.parent[x]
    }

    fn union(&mut self, a: usize, b: usize) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb {
            return;
        }
        if self.rank[ra] < self.rank[rb] {
            self.parent[ra] = rb;
        } else if self.rank[ra] > self.rank[rb] {
            self.parent[rb] = ra;
        } else {
            self.parent[rb] = ra;
            self.rank[ra] += 1;
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::parse_machine;
    use crate::cap::registry::FabricRegistry;
    use crate::machine::error::{MachineAbstractionError, MachineParseError, MachineSyntaxError};
    use crate::machine::test_fixtures::{build_cap, registry_with};

    fn pdf_extract_embed_registry() -> FabricRegistry {
        let extract = build_cap(
            "cap:extract;in=\"media:document;ext=pdf\";out=\"media:enc=utf-8;ext=txt\"",
            "extract",
            &["media:document;ext=pdf"],
            "media:enc=utf-8;ext=txt",
        );
        let embed = build_cap(
            "cap:embed;in=\"media:enc=utf-8\";out=\"media:vec;record\"",
            "embed",
            &["media:enc=utf-8"],
            "media:vec;record",
        );
        registry_with(vec![extract, embed])
    }

    // TEST1163: Parsing one connected strand yields a single machine strand with both caps connected by the shared node.
    #[test]
    fn test1163_parse_single_strand_two_caps_connected_via_shared_node() {
        let registry = pdf_extract_embed_registry();
        let notation = "\
[extract cap:extract;in=\"media:document;ext=pdf\";out=\"media:enc=utf-8;ext=txt\"]\
[embed cap:embed;in=\"media:enc=utf-8\";out=\"media:vec;record\"]\
[doc -> extract -> txt]\
[txt -> embed -> vec]";
        let machine = parse_machine(notation, &registry).expect("must parse");
        // Two wirings, one shared node `txt` → ONE connected
        // component → ONE strand.
        assert_eq!(machine.strand_count(), 1);
        let strand = &machine.strands()[0];
        assert_eq!(strand.edges().len(), 2);
        // The intermediate node must be the same NodeId for
        // both edges.
        let extract_target = strand.edges()[0].main_target().expect("the main output is taken");
        let embed_source = strand.edges()[1].assignment[0].node().expect("a node binding");
        assert_eq!(extract_target, embed_source);
    }

    // TEST1164: Parsing two disconnected strand definitions yields two separate machine strands.
    #[test]
    fn test1164_parse_two_disconnected_strands_yields_two_machine_strands() {
        // Two strands sharing no node names. The parser must
        // partition them into two `MachineStrand`s and order
        // them by first appearance in the textual input.
        let convert_a = build_cap(
            "cap:convert-a;in=\"media:fmt=json\";out=\"media:fmt=csv\"",
            "convert_a",
            &["media:fmt=json"],
            "media:fmt=csv",
        );
        let convert_b = build_cap(
            "cap:convert-b;in=\"media:ext=html\";out=\"media:ext=txt\"",
            "convert_b",
            &["media:ext=html"],
            "media:ext=txt",
        );
        let registry = registry_with(vec![convert_a, convert_b]);
        let notation = "\
[ca cap:convert-a;in=\"media:fmt=json\";out=\"media:fmt=csv\"]\
[cb cap:convert-b;in=\"media:ext=html\";out=\"media:ext=txt\"]\
[input_a -> ca -> output_a]\
[input_b -> cb -> output_b]";
        let machine = parse_machine(notation, &registry).expect("must parse");
        assert_eq!(
            machine.strand_count(),
            2,
            "two wirings sharing no nodes must produce two strands"
        );
        // Strand order is first-appearance order. The first
        // wiring `input_a -> ca -> output_a` belongs to strand 0;
        // the second to strand 1.
        assert_eq!(machine.strands()[0].edges().len(), 1);
        assert_eq!(machine.strands()[1].edges().len(), 1);
        // First strand uses convert-a, second uses convert-b. The
        // marker tag in the URN uses hyphens; the cap title is
        // separately stored with underscores but isn't part of the
        // URN serialization.
        assert!(machine.strands()[0].edges()[0]
            .cap_urn
            .to_string()
            .contains("convert-a"));
        assert!(machine.strands()[1].edges()[0]
            .cap_urn
            .to_string()
            .contains("convert-b"));
    }

    // TEST1165: Parsing fails hard when a referenced cap is missing from the registry cache.
    #[test]
    fn test1165_parse_unknown_cap_in_registry_fails_hard() {
        let registry = registry_with(vec![]);
        let notation = "\
[ghost cap:ghost;in=\"media:document;ext=pdf\";out=\"media:enc=utf-8;ext=txt\"]\
[a -> ghost -> b]";
        let err = parse_machine(notation, &registry).unwrap_err();
        match err {
            MachineParseError::Resolution(MachineAbstractionError::UnknownCap { cap_urn }) => {
                assert!(cap_urn.contains("ghost"));
            }
            other => panic!("expected Resolution(UnknownCap), got {:?}", other),
        }
    }

    // TEST1166: Duplicate header aliases are reported as syntax errors.
    #[test]
    fn test1166_parse_duplicate_alias_is_syntax_error() {
        let registry = pdf_extract_embed_registry();
        let notation = "\
[extract cap:extract;in=\"media:document;ext=pdf\";out=\"media:enc=utf-8;ext=txt\"]\
[extract cap:embed;in=\"media:enc=utf-8\";out=\"media:vec;record\"]\
[a -> extract -> b]";
        let err = parse_machine(notation, &registry).unwrap_err();
        assert!(matches!(
            err,
            MachineParseError::Syntax(MachineSyntaxError::DuplicateAlias { .. })
        ));
    }

    // TEST1167: Wiring that references an undefined alias is reported as a syntax error.
    #[test]
    fn test1167_parse_undefined_alias_is_syntax_error() {
        let registry = pdf_extract_embed_registry();
        let notation = "\
[extract cap:extract;in=\"media:document;ext=pdf\";out=\"media:enc=utf-8;ext=txt\"]\
[a -> notDefined -> b]";
        let err = parse_machine(notation, &registry).unwrap_err();
        assert!(matches!(
            err,
            MachineParseError::Syntax(MachineSyntaxError::UndefinedAlias { .. })
        ));
    }

    // TEST1168: Parsing rejects node names that collide with declared cap aliases.
    #[test]
    fn test1168_parse_node_alias_collision_with_header_alias_fails_hard() {
        // The user wrote `extract` as a NODE name in a wiring
        // but `extract` is also a header alias. This is
        // structurally ambiguous: is `extract` the cap or the
        // node? The parser must reject it.
        let registry = pdf_extract_embed_registry();
        let notation = "\
[extract cap:extract;in=\"media:document;ext=pdf\";out=\"media:enc=utf-8;ext=txt\"]\
[extract -> extract -> b]";
        let err = parse_machine(notation, &registry).unwrap_err();
        assert!(matches!(
            err,
            MachineParseError::Syntax(MachineSyntaxError::NodeAliasCollision { .. })
        ));
    }

    // TEST1169: A sequence-output cap feeding a scalar-input cap makes the resolved
    // edge a per-item map (`is_loop`), derived from cardinality — the single rule
    // `Cap::needs_foreach`, which replaces the retired `LOOP` keyword. The
    // scalar→sequence producer edge itself does not loop.
    #[test]
    fn test1169_sequence_into_scalar_cap_derives_is_loop() {
        // Producer: scalar text → SEQUENCE of items.
        let mut splitter = build_cap(
            "cap:in=\"media:enc=utf-8\";split;out=\"media:enc=utf-8;item\"",
            "split",
            &["media:enc=utf-8"],
            "media:enc=utf-8;item",
        );
        splitter.outputs[0].is_sequence = true;
        // Consumer: scalar item → scalar text.
        let texter = build_cap(
            "cap:in=\"media:enc=utf-8;item\";t;out=\"media:enc=utf-8\"",
            "t",
            &["media:enc=utf-8;item"],
            "media:enc=utf-8",
        );
        let registry = registry_with(vec![splitter, texter]);
        let notation = "\
[split cap:in=\"media:enc=utf-8\";split;out=\"media:enc=utf-8;item\"]\
[t cap:in=\"media:enc=utf-8;item\";t;out=\"media:enc=utf-8\"]\
[a -> split -> items]\
[items -> t -> b]";
        let machine = parse_machine(notation, &registry).expect("must parse");
        assert_eq!(machine.strand_count(), 1);
        let strand = &machine.strands()[0];
        assert_eq!(strand.edges().len(), 2);
        let split_edge = strand
            .edges()
            .iter()
            .find(|e| e.cap_urn.to_string().contains("split"))
            .expect("split edge");
        let t_edge = strand
            .edges()
            .iter()
            .find(|e| !e.cap_urn.to_string().contains("split"))
            .expect("t edge");
        assert!(
            !split_edge.is_loop,
            "a scalar source feeding the sequence-producing cap must not map"
        );
        assert!(
            t_edge.is_loop,
            "a sequence feeding a scalar-input cap must map per item (is_loop)"
        );
    }

    // TEST1170: Parsing and then serializing machine notation round-trips to the canonical form.
    #[test]
    fn test1170_parse_then_serialize_round_trips_to_canonical_form() {
        // The user can write any aliases / node names; the
        // parse-then-reserialize cycle normalizes them to
        // edge_<i> / n<i> from the global counters. Round-tripping
        // a serializer-produced notation through parse-then-
        // serialize is a fixed point.
        let registry = pdf_extract_embed_registry();
        let user_input = "\
[user_extract cap:extract;in=\"media:document;ext=pdf\";out=\"media:enc=utf-8;ext=txt\"]\
[user_embed cap:embed;in=\"media:enc=utf-8\";out=\"media:vec;record\"]\
[doc -> user_extract -> txt]\
[txt -> user_embed -> vec]";
        let m1 = parse_machine(user_input, &registry).expect("must parse");
        let canonical = m1.to_machine_notation().expect("must serialize");
        // Canonical form should NOT contain user aliases /
        // node names — they get rewritten to edge_N / nN.
        assert!(!canonical.contains("user_extract"));
        assert!(!canonical.contains("user_embed"));
        assert!(canonical.contains("edge_0"));
        let m2 = parse_machine(&canonical, &registry).expect("canonical must re-parse");
        assert!(m1.is_equivalent(&m2));
        let canonical2 = m2.to_machine_notation().unwrap();
        assert_eq!(canonical, canonical2);
    }

    // TEST1171: Empty machine notation is rejected as a syntax error.
    #[test]
    fn test1171_parse_empty_notation_is_syntax_error() {
        let registry = registry_with(vec![]);
        let err = parse_machine("   ", &registry).unwrap_err();
        assert!(matches!(
            err,
            MachineParseError::Syntax(MachineSyntaxError::Empty)
        ));
    }

    // TEST1136: parse_machine with an undefined cap alias raises MachineParseError wrapping
    // MachineSyntaxError::UndefinedAlias. This pins the error path so an alias lookup failure
    // is always surfaced as a syntax error (not a resolution error or a panic).
    #[test]
    fn test1136_parse_machine_undefined_alias_raises_syntax_error() {
        let registry = registry_with(vec![]);
        let notation = "[doc -> undefined_alias -> text]";
        let err = parse_machine(notation, &registry).unwrap_err();
        assert!(
            matches!(
                err,
                MachineParseError::Syntax(MachineSyntaxError::UndefinedAlias { .. })
            ),
            "undefined alias must produce a MachineParseError::Syntax(UndefinedAlias), got {:?}",
            err
        );
    }

    use crate::StoredAlias;

    // Build a registry with the `extract` cap cached AND a `pdf2text` alias
    // pointing at that cap's URN, so the alias can be used in a wiring
    // without a local header.
    fn extract_with_alias_registry() -> (FabricRegistry, String) {
        let extract_urn = "cap:extract;in=\"media:document;ext=pdf\";out=\"media:enc=utf-8;ext=txt\"";
        let extract = build_cap(
            extract_urn,
            "extract",
            &["media:document;ext=pdf"],
            "media:enc=utf-8;ext=txt",
        );
        let canonical = extract.urn_string();
        let registry = registry_with(vec![extract]);
        registry.insert_cached_alias_for_test(StoredAlias {
            name: "pdf2text".to_string(),
            target: canonical.clone(),
            version: 1,
        });
        (registry, canonical)
    }

    // TEST1883: a cap-position name with no local header is resolved as a
    // fabric cap alias. The wiring uses `pdf2text` where a cap is expected;
    // it must resolve to the aliased cap URN and produce a one-edge strand
    // whose cap URN is the alias target. A broken resolver would either
    // fail (treating it as undefined) or wire the wrong cap.
    #[test]
    fn test1883_cap_position_alias_resolves_to_cap() {
        let (registry, canonical) = extract_with_alias_registry();
        // No header for `pdf2text` — it must resolve via the alias.
        let notation = "[doc -> pdf2text -> txt]";
        let machine = parse_machine(notation, &registry).expect("alias must resolve");
        assert_eq!(machine.strand_count(), 1);
        let strand = &machine.strands()[0];
        assert_eq!(strand.edges().len(), 1);
        assert_eq!(strand.edges()[0].cap_urn.to_string(), canonical);
    }

    // TEST1884: a local header alias shadows a fabric alias of the same
    // name. If `pdf2text` is BOTH a header (bound to one cap) and a
    // registered alias (pointing at another cap), the header wins. This
    // pins the precedence rule: local definitions shadow registry aliases.
    #[test]
    fn test1884_local_header_shadows_cap_alias() {
        let (registry, _alias_target) = extract_with_alias_registry();
        // Add a DIFFERENT cap that the header binds `pdf2text` to.
        let other_urn = "cap:other;in=\"media:document;ext=pdf\";out=\"media:enc=utf-8;ext=txt\"";
        let other = build_cap(
            other_urn,
            "other",
            &["media:document;ext=pdf"],
            "media:enc=utf-8;ext=txt",
        );
        let other_canonical = other.urn_string();
        registry.add_caps_to_cache(vec![other]);
        let notation = format!("[pdf2text {}]\n[doc -> pdf2text -> txt]", other_urn);
        let machine = parse_machine(&notation, &registry).expect("must parse");
        // The header binding (other_urn), not the alias target, must win.
        assert_eq!(
            machine.strands()[0].edges()[0].cap_urn.to_string(),
            other_canonical
        );
    }

    // TEST1885: a cap-position alias that resolves to a MEDIA URN is a hard
    // error — the cap position requires a cap. This proves the type-correct
    // enforcement in notation: a media alias cannot stand in for a cap.
    #[test]
    fn test1885_cap_position_alias_to_media_is_error() {
        let extract = build_cap(
            "cap:extract;in=\"media:document;ext=pdf\";out=\"media:enc=utf-8;ext=txt\"",
            "extract",
            &["media:document;ext=pdf"],
            "media:enc=utf-8;ext=txt",
        );
        let registry = registry_with(vec![extract]);
        // `jsondoc` resolves to a media URN, not a cap.
        registry.insert_cached_alias_for_test(StoredAlias {
            name: "jsondoc".to_string(),
            target: "media:fmt=json;record".to_string(),
            version: 1,
        });
        let notation = "[doc -> jsondoc -> out]";
        let err = parse_machine(notation, &registry).unwrap_err();
        assert!(
            matches!(
                err,
                MachineParseError::Syntax(MachineSyntaxError::AliasNotACap { .. })
            ),
            "a media alias in cap position must raise AliasNotACap, got {:?}",
            err
        );
    }

    // TEST1886: a cap-position name that is neither a local header nor a
    // registered alias still raises UndefinedAlias. The alias mechanism
    // must not mask a genuinely undefined name.
    #[test]
    fn test1886_unregistered_cap_name_is_undefined_alias() {
        let registry = extract_with_alias_registry().0;
        let notation = "[doc -> nosuchalias -> out]";
        let err = parse_machine(notation, &registry).unwrap_err();
        assert!(
            matches!(
                err,
                MachineParseError::Syntax(MachineSyntaxError::UndefinedAlias { .. })
            ),
            "an unregistered cap-position name must raise UndefinedAlias, got {:?}",
            err
        );
    }
}
