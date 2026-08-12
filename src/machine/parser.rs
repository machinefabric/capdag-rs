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
//! ## Grammar (PEG / EBNF)
//!
//! ```ebnf
//! program      = stmt*
//! stmt         = "[" inner "]" | inner
//! inner        = wiring | header
//! header       = alias cap_urn
//! wiring       = source arrow alias arrow alias
//! source       = group | alias
//! group        = "(" alias ("," alias)+ ")"
//! arrow        = "-"+ ">"
//! alias        = (ALPHA | "_") (ALNUM | "_" | "-")*
//! cap_urn      = "cap:" cap_urn_body*
//! cap_urn_body = quoted_value | !("]" | NEWLINE) ANY
//! quoted_value = '"' ('\\"' | '\\\\' | !'"' ANY)* '"'
//! ```
//!
//! ## Strand boundary discovery
//!
//! Two wirings belong to the same `MachineStrand` iff there
//! exists a path through the wiring set, hopping along shared
//! node-name endpoints, that connects them. Connected
//! components are computed via union-find. The strand list in
//! the resulting `Machine` is in **first-appearance order**:
//! the strand whose earliest wiring appears first in the
//! textual input comes first.
//!
//! ## Media URN derivation per node name
//!
//! For each wiring, the parser derives the media URNs that
//! its source node names and target node name are bound to:
//!
//! - **Primary source** (slot 0 in the wiring's source group):
//!   bound to the cap's declared `in=` URN. If the same node
//!   name was already bound to a different URN by an earlier
//!   wiring, the two URNs must be `is_comparable` (on the same
//!   specialization chain).
//! - **Secondary sources** (slots 1+): take whichever URN was
//!   previously bound to that node name. If unbound, default
//!   to `media:` (the wildcard); the resolver / orchestrator
//!   will distinguish concrete arg URNs at run time.
//! - **Target**: bound to the cap's declared `out=` URN, with
//!   the same `is_comparable` check.

use std::collections::HashMap;

use pest::Parser;
use pest_derive::Parser;

use crate::cap::registry::FabricRegistry;
use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::MediaUrn;
use crate::FabricRegistryError;

use super::error::{MachineParseError, MachineSyntaxError};
use super::graph::{Machine, MachineStrand, NodeId};
use super::resolve::{resolve_pre_interned, ForEachIdentity, PreInternedWiring};

#[derive(Parser)]
#[grammar = "machine/machine.pest"]
pub struct MachineParser;

/// One wiring as it comes off the AST walk, with raw alias
/// names. Resolution happens in two more passes after this.
struct RawWiring {
    /// Node-name aliases for the source slots, in the order
    /// the user wrote them. Slot 0 is the primary.
    sources: Vec<String>,
    /// Cap header alias.
    cap_alias: String,
    /// Node-name alias for the target.
    target: String,
    /// Index of this wiring in the textual input. Used to
    /// order connected components by first appearance.
    position: usize,
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

    // Phase 1: pest grammar parse.
    let pairs =
        MachineParser::parse(Rule::program, input).map_err(|e| MachineSyntaxError::ParseError {
            details: format!("{}", e),
        })?;

    // Phase 2: walk AST collecting headers and wirings.
    let mut headers: Vec<(String, CapUrn, usize)> = Vec::new();
    let mut wirings: Vec<RawWiring> = Vec::new();

    let program = pairs
        .into_iter()
        .next()
        .expect("pest produces a program rule");
    for (stmt_idx, pair) in program.into_inner().enumerate() {
        if pair.as_rule() != Rule::stmt {
            continue; // skip EOI
        }
        let inner = pair.into_inner().next().expect("stmt wraps inner");
        let content = inner
            .into_inner()
            .next()
            .expect("inner wraps header or wiring");

        match content.as_rule() {
            Rule::header => {
                let mut inner_pairs = content.into_inner();
                let alias = inner_pairs
                    .next()
                    .expect("header has alias")
                    .as_str()
                    .to_string();
                let cap_urn_str = inner_pairs.next().expect("header has cap_urn").as_str();
                let cap_urn = CapUrn::from_string(cap_urn_str).map_err(|e| {
                    MachineSyntaxError::InvalidCapUrn {
                        alias: alias.clone(),
                        details: format!("{}", e),
                    }
                })?;
                headers.push((alias, cap_urn, stmt_idx));
            }
            Rule::wiring => {
                let mut inner_pairs = content.into_inner();
                let source_pair = inner_pairs.next().expect("wiring has source");
                let sources = parse_source(source_pair);
                inner_pairs.next(); // arrow
                let cap_alias = inner_pairs
                    .next()
                    .expect("wiring has cap alias")
                    .as_str()
                    .to_string();
                inner_pairs.next(); // arrow
                let target = inner_pairs
                    .next()
                    .expect("wiring has target")
                    .as_str()
                    .to_string();
                wirings.push(RawWiring {
                    sources,
                    cap_alias,
                    target,
                    position: stmt_idx,
                });
            }
            _ => unreachable!("grammar guarantees inner is header or wiring"),
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

    // Phase 4: derive node-name → MediaUrn bindings.
    //
    // Walk wirings in textual order. For each wiring:
    //   - Primary source: bind cap.in=
    //   - Secondary sources: bind to whatever they already
    //     hold (or media: wildcard if unbound)
    //   - Target: bind cap.out=
    // Re-binding is allowed iff the new URN is_comparable to
    // the existing one (same specialization chain).
    let mut node_media: HashMap<String, MediaUrn> = HashMap::new();
    let wildcard = MediaUrn::from_string("media:").expect("wildcard media URN parses");

    for w in &wirings {
        let (cap_urn, _) =
            alias_map
                .get(&w.cap_alias)
                .ok_or_else(|| MachineSyntaxError::UndefinedAlias {
                    alias: w.cap_alias.clone(),
                })?;

        for src in &w.sources {
            if alias_map.contains_key(src) {
                return Err(MachineSyntaxError::NodeAliasCollision {
                    name: src.clone(),
                    alias: src.clone(),
                }
                .into());
            }
        }
        if alias_map.contains_key(&w.target) {
            return Err(MachineSyntaxError::NodeAliasCollision {
                name: w.target.clone(),
                alias: w.target.clone(),
            }
            .into());
        }

        let cap_in_media =
            cap_urn
                .in_media_urn()
                .map_err(|e| MachineSyntaxError::InvalidMediaUrn {
                    alias: w.cap_alias.clone(),
                    details: format!("in= spec: {}", e),
                })?;
        let cap_out_media =
            cap_urn
                .out_media_urn()
                .map_err(|e| MachineSyntaxError::InvalidMediaUrn {
                    alias: w.cap_alias.clone(),
                    details: format!("out= spec: {}", e),
                })?;

        // Primary source: bind to cap.in=
        if !w.sources.is_empty() {
            assign_or_check_node(&w.sources[0], &cap_in_media, &mut node_media, w.position)?;
            // Secondaries: bind to wildcard if unbound, leave
            // alone otherwise. The bound value is what
            // resolution will see.
            for src in w.sources.iter().skip(1) {
                if !node_media.contains_key(src) {
                    node_media.insert(src.clone(), wildcard.clone());
                }
            }
        }
        assign_or_check_node(&w.target, &cap_out_media, &mut node_media, w.position)?;
    }

    // Phase 5: connected-components partition by shared node
    // name. Union-find over wiring indices, where two wirings
    // are unioned iff they share at least one node name.
    let n = wirings.len();
    let mut union = UnionFind::new(n);

    // Map: node name → index of the first wiring that touched
    // it. As we process wirings in order, any wiring that
    // touches a previously-seen node name is unioned with the
    // earlier wiring.
    let mut node_first_wiring: HashMap<String, usize> = HashMap::new();
    for (w_idx, w) in wirings.iter().enumerate() {
        let mut node_names: Vec<&String> = Vec::with_capacity(w.sources.len() + 1);
        node_names.extend(w.sources.iter());
        node_names.push(&w.target);
        for node_name in node_names {
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

    // Phase 6: per-component pre-interning + resolution.
    //
    // For each connected component (= strand), allocate
    // `NodeId`s in the order user node names are encountered
    // (walking the wirings in their textual order). The
    // resolver receives `PreInternedWiring`s that already
    // reference NodeIds, plus the parallel `nodes: Vec<MediaUrn>`
    // table. Two distinct user node names that happen to share
    // a media URN stay distinct NodeIds — that's the parser's
    // identity contract.
    let mut strands: Vec<MachineStrand> = Vec::with_capacity(group_min_idx.len());
    let mut strand_node_names: Vec<StrandNodeNames> = Vec::with_capacity(group_min_idx.len());
    for (strand_index, (root, _)) in group_min_idx.iter().enumerate() {
        let mut member_indices = groups[root].clone();
        member_indices.sort();

        let mut nodes: Vec<MediaUrn> = Vec::new();
        let mut name_to_id: StrandNodeNames = HashMap::new();

        // Allocate a NodeId for `name`. If `name` is already
        // bound to a NodeId in this strand, return it.
        // Otherwise allocate a new NodeId, push the name's
        // bound URN onto the nodes table, and return.
        fn intern_named(
            name: &str,
            node_media: &HashMap<String, MediaUrn>,
            nodes: &mut Vec<MediaUrn>,
            name_to_id: &mut StrandNodeNames,
        ) -> NodeId {
            if let Some(id) = name_to_id.get(name) {
                return *id;
            }
            let urn = node_media
                .get(name)
                .cloned()
                .expect("every node name was bound during phase 4");
            let id = nodes.len() as NodeId;
            nodes.push(urn);
            name_to_id.insert(name.to_string(), id);
            id
        }

        let mut pre_interned: Vec<PreInternedWiring> = Vec::with_capacity(member_indices.len());
        for &w_idx in &member_indices {
            let w = &wirings[w_idx];
            let (cap_urn, _) = alias_map
                .get(&w.cap_alias)
                .expect("cap alias was validated above");

            let source_node_ids: Vec<NodeId> = w
                .sources
                .iter()
                .map(|name| intern_named(name, &node_media, &mut nodes, &mut name_to_id))
                .collect();
            let target_node_id = intern_named(&w.target, &node_media, &mut nodes, &mut name_to_id);

            pre_interned.push(PreInternedWiring {
                // The notation (static-machine) path has no resolved-strand step to
                // inherit from, so each parsed wiring's edge gets its own freshly
                // minted stable identity.
                token_id: crate::planner::StepToken::mint(),
                foreach_identity: ForEachIdentity::MintIfRequired,
                cap_urn: cap_urn.clone(),
                source_node_ids,
                target_node_id,
            });
        }

        let strand = resolve_pre_interned(nodes, &pre_interned, registry, strand_index)?;
        strands.push(strand);
        strand_node_names.push(name_to_id);
    }

    Ok((Machine::from_resolved_strands(strands), strand_node_names))
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
    let pairs = MachineParser::parse(Rule::program, input.trim()).map_err(|e| {
        MachineSyntaxError::ParseError {
            details: format!("{}", e),
        }
    })?;

    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut urns: Vec<String> = Vec::new();
    let program = pairs
        .into_iter()
        .next()
        .expect("pest produces a program rule");
    for pair in program.into_inner() {
        if pair.as_rule() != Rule::stmt {
            continue;
        }
        let inner = pair.into_inner().next().expect("stmt wraps inner");
        let content = inner
            .into_inner()
            .next()
            .expect("inner wraps header or wiring");
        if content.as_rule() != Rule::header {
            continue;
        }
        let mut inner_pairs = content.into_inner();
        let _alias = inner_pairs.next();
        let cap_urn_str = inner_pairs
            .next()
            .expect("header has cap_urn")
            .as_str()
            .to_string();
        if seen.insert(cap_urn_str.clone()) {
            urns.push(cap_urn_str);
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
    let pairs = MachineParser::parse(Rule::program, input.trim()).map_err(|e| {
        MachineSyntaxError::ParseError {
            details: format!("{}", e),
        }
    })?;

    let program = pairs
        .into_iter()
        .next()
        .expect("pest produces a program rule");

    // First pass: collect every header alias (these shadow aliases).
    // Second pass: collect cap-position names not in that set.
    let mut header_aliases: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut cap_names: Vec<(String, bool)> = Vec::new(); // (name, is_wiring_cap_position)

    for pair in program.into_inner() {
        if pair.as_rule() != Rule::stmt {
            continue;
        }
        let inner = pair.into_inner().next().expect("stmt wraps inner");
        let content = inner
            .into_inner()
            .next()
            .expect("inner wraps header or wiring");
        match content.as_rule() {
            Rule::header => {
                let mut ip = content.into_inner();
                let alias = ip.next().expect("header has alias").as_str().to_string();
                header_aliases.insert(alias);
            }
            Rule::wiring => {
                let mut ip = content.into_inner();
                let _source = ip.next();
                let _arrow = ip.next();
                let cap_alias = ip
                    .next()
                    .expect("wiring has cap alias")
                    .as_str()
                    .to_string();
                cap_names.push((cap_alias, true));
            }
            _ => {}
        }
    }

    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut out: Vec<String> = Vec::new();
    for (name, _) in cap_names {
        if header_aliases.contains(&name) {
            continue;
        }
        if seen.insert(name.clone()) {
            out.push(name);
        }
    }
    Ok(out)
}

/// Async parallel of `parse_machine_with_node_names`.
///
/// Differs from the sync version only in that it pre-warms the
/// `FabricRegistry`'s caches BEFORE running the existing sync resolver:
///
/// 1. Every cap URN declared in a header is fetched via `get_cap`.
/// 2. Every cap-position name in a wiring that is NOT a local header is
///    treated as a **cap alias**: the alias is resolved (warming the alias
///    cache) and its target cap is fetched (warming the cap cache). An
///    alias that points at a media URN in cap position is a hard error
///    here, matching the synchronous resolver's `AliasNotACap`.
///
/// After the warm-up the resolver's synchronous `get_cached_cap` and
/// `resolve_alias_cached` lookups all hit cache, so the resolver code is
/// unchanged.
///
/// Use this from any `async` context where the registry may need to fetch
/// caps over the network — notably the scenarios integration tests, which
/// run with internet access and do not pre-load any bundled catalog.
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

/// Extract source node names from a source pair (single alias
/// or group).
fn parse_source(pair: pest::iterators::Pair<Rule>) -> Vec<String> {
    let inner = pair.into_inner().next().expect("source has inner");
    match inner.as_rule() {
        Rule::group => inner
            .into_inner()
            .filter(|p| p.as_rule() == Rule::alias)
            .map(|p| p.as_str().to_string())
            .collect(),
        Rule::alias => vec![inner.as_str().to_string()],
        _ => unreachable!("source is group or alias"),
    }
}

/// Bind a media URN to a node, or check that an existing
/// binding is comparable. Two URNs bound to the same node name
/// must be on the same specialization chain (`is_comparable`);
/// the resolver will pick the more-specific one when it runs.
fn assign_or_check_node(
    node: &str,
    media_urn: &MediaUrn,
    node_media: &mut HashMap<String, MediaUrn>,
    position: usize,
) -> Result<(), MachineSyntaxError> {
    if let Some(existing) = node_media.get(node) {
        let compatible = existing.is_comparable(media_urn).unwrap_or(false);
        if !compatible {
            return Err(MachineSyntaxError::InvalidWiring {
                position,
                details: format!(
                    "node '{}' has conflicting media types: existing '{}', new '{}'",
                    node, existing, media_urn
                ),
            });
        }
        // The more-specific URN wins (so a downstream cap with
        // a tighter pattern bound to the same node refines the
        // type at that data position).
        if media_urn.specificity() > existing.specificity() {
            node_media.insert(node.to_string(), media_urn.clone());
        }
    } else {
        node_media.insert(node.to_string(), media_urn.clone());
    }
    Ok(())
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
            "cap:extract;in=\"media:ext=pdf\";out=\"media:enc=utf-8;ext=txt\"",
            "extract",
            &["media:ext=pdf"],
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
[extract cap:extract;in=\"media:ext=pdf\";out=\"media:enc=utf-8;ext=txt\"]\
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
        let extract_target = strand.edges()[0].target;
        let embed_source = strand.edges()[1].assignment[0].source;
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
[ghost cap:ghost;in=\"media:ext=pdf\";out=\"media:enc=utf-8;ext=txt\"]\
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
[extract cap:extract;in=\"media:ext=pdf\";out=\"media:enc=utf-8;ext=txt\"]\
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
[extract cap:extract;in=\"media:ext=pdf\";out=\"media:enc=utf-8;ext=txt\"]\
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
[extract cap:extract;in=\"media:ext=pdf\";out=\"media:enc=utf-8;ext=txt\"]\
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
        splitter.output.as_mut().unwrap().is_sequence = true;
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
[user_extract cap:extract;in=\"media:ext=pdf\";out=\"media:enc=utf-8;ext=txt\"]\
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
        let extract_urn = "cap:extract;in=\"media:ext=pdf\";out=\"media:enc=utf-8;ext=txt\"";
        let extract = build_cap(
            extract_urn,
            "extract",
            &["media:ext=pdf"],
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
        let other_urn = "cap:other;in=\"media:ext=pdf\";out=\"media:enc=utf-8;ext=txt\"";
        let other = build_cap(
            other_urn,
            "other",
            &["media:ext=pdf"],
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
            "cap:extract;in=\"media:ext=pdf\";out=\"media:enc=utf-8;ext=txt\"",
            "extract",
            &["media:ext=pdf"],
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
