//! Machine notation serializer — canonical text encoding of
//! a `Machine`.
//!
//! A `Machine` is a `Vec<MachineStrand>` (see `graph.rs`). The
//! serializer walks the strands in declaration order and emits
//! one notation document covering all of them. Two
//! strictly-equivalent `Machine`s produce byte-identical
//! notation, because both the canonical edge order within each
//! strand and the global alias / node-name allocation are
//! deterministic functions of the resolved DAG structure.
//!
//! ## Layout
//!
//! ```text
//! [<global alias 0> <cap-urn 0>]
//! [<global alias 1> <cap-urn 1>]
//! ...
//! [<source nodes> -> <global alias 0> -> <target node>]
//! [<source nodes> -> <global alias 1> -> <target node>]
//! ...
//! ```
//!
//! All headers come first, then all wirings. Both sections
//! traverse strands in `Machine::strands()` order, and within
//! each strand the resolved canonical edge order from
//! `MachineStrand::edges()`.
//!
//! ## Aliases and node names
//!
//! Aliases and node names are opaque labels — see
//! `09-MACHINE-NOTATION.md` §4 for the rationale. The
//! serializer generates them as:
//!
//! - `edge_<global_index>` for each cap edge in the order it
//!   appears in the global walk (across all strands).
//! - `n<global_index>` for each `NodeId` allocated as the walk
//!   visits new data positions.
//!
//! Strand boundaries are unmarked in the notation. The parser
//! recovers them via connected-components analysis on shared
//! node names — and because the serializer assigns each strand
//! a fresh disjoint range of node names, the parser's
//! connected-components partition matches the serializer's
//! strand list exactly. Round-trip preserves both strand order
//! and intra-strand canonical edge order.
//!
//! ## Failure modes
//!
//! Serialization is infallible for any `Machine` that was built
//! through one of the legitimate constructors (`from_strand`,
//! `from_strands`, `from_string`). The `Machine`'s internal
//! invariants (every `NodeId` referenced is in range, every
//! resolved edge points at valid nodes) are established at
//! construction time and cannot be violated by the serializer.

use std::fmt::Write;

use super::error::MachineAbstractionError;
use super::graph::{BindingSource, Machine, MachineEdge, MachineStrand};
use crate::FabricRegistry;

/// Serialization format for machine notation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotationFormat {
    /// Bracketed: each statement wrapped in `[...]`. The
    /// canonical, single-line form used as a stable
    /// identifier. The default for `Machine::to_machine_notation`.
    Bracketed,
    /// Line-based: one statement per line, no brackets. Used
    /// for human-readable / human-editable display.
    LineBased,
}

/// How a cap header renders its cap identity.
///
/// The notation grammar lets a header name a cap either by its full URN or by
/// a registered alias (the parser's async warm-up resolves the alias back to
/// the URN, so both forms round-trip). These two modes pick which.
#[derive(Clone, Copy)]
enum CapRendering<'a> {
    /// Always emit the canonical cap URN. No registry needed. This is the
    /// alias-independent identity form.
    CanonicalUrn,
    /// Emit the cap's display alias when one is registered (shortest, then
    /// alphabetical), falling back to the canonical URN when none exists.
    /// This is the "store aliased" form used by generation and persistence.
    Aliased(&'a FabricRegistry),
}

impl<'a> CapRendering<'a> {
    /// The cap's display alias for this edge, if this rendering aliases AND a
    /// registered alias exists. `None` for canonical rendering or an
    /// un-aliased cap (the caller then emits a synthetic `edge_N` header).
    ///
    /// An aliased cap is referenced directly in the wiring's cap position; the
    /// grammar only permits an alias (a `:`-free token) there, never in a
    /// header (which requires a `cap:` URN), so aliasing implies header-less.
    fn cap_alias(&self, edge: &MachineEdge) -> Option<String> {
        match self {
            CapRendering::CanonicalUrn => None,
            CapRendering::Aliased(registry) => {
                registry.display_alias_for_urn(&edge.cap_urn.to_string())
            }
        }
    }
}

impl Machine {
    /// Serialize this machine to canonical bracketed machine
    /// notation. Two strictly-equivalent machines produce
    /// byte-identical output. Caps are rendered by their canonical
    /// URN (alias-independent identity form).
    pub fn to_machine_notation(&self) -> Result<String, MachineAbstractionError> {
        self.to_machine_notation_formatted(NotationFormat::Bracketed)
    }

    /// Serialize to multi-line bracketed machine notation —
    /// one statement per line, each wrapped in `[...]`.
    /// Functionally equivalent to the canonical bracketed form
    /// but with newlines between statements for readability.
    pub fn to_machine_notation_multiline(&self) -> Result<String, MachineAbstractionError> {
        let plan = build_serialization_plan(self, CapRendering::CanonicalUrn);
        emit_multiline(self, &plan)
    }

    /// Serialize this machine to machine notation in the
    /// specified format. Two strictly-equivalent machines
    /// produce byte-identical output for a given format. Caps are
    /// rendered by their canonical URN.
    pub fn to_machine_notation_formatted(
        &self,
        format: NotationFormat,
    ) -> Result<String, MachineAbstractionError> {
        if self.is_empty() {
            return Ok(String::new());
        }
        let plan = build_serialization_plan(self, CapRendering::CanonicalUrn);
        match format {
            NotationFormat::Bracketed => emit_bracketed(self, &plan),
            NotationFormat::LineBased => emit_line_based(self, &plan),
        }
    }

    /// Serialize to machine notation rendering each cap by its registered
    /// display alias (shortest name, ties alphabetical) when one exists,
    /// falling back to the canonical URN otherwise. This is the "store
    /// aliased" form: generated and persisted machines use it so the saved
    /// notation reads in terms of aliases. The parser resolves these aliases
    /// back to URNs on load (its async warm-up hydrates the alias cache), so
    /// the form round-trips.
    pub fn to_machine_notation_aliased(
        &self,
        registry: &FabricRegistry,
        format: NotationFormat,
    ) -> Result<String, MachineAbstractionError> {
        if self.is_empty() {
            return Ok(String::new());
        }
        let plan = build_serialization_plan(self, CapRendering::Aliased(registry));
        match format {
            NotationFormat::Bracketed => emit_bracketed(self, &plan),
            NotationFormat::LineBased => emit_line_based(self, &plan),
        }
    }
}

/// Per-machine serialization plan: per-strand alias and node-
/// name allocations, all keyed by global indices.
struct SerializationPlan {
    /// One entry per strand. Each strand contributes its own
    /// edge alias names and node names from the global
    /// counters.
    strands: Vec<StrandPlan>,
}

struct StrandPlan {
    /// Cap-position token for each edge in `MachineStrand::edges()`, in the
    /// strand's canonical edge order. Indexed by edge index within the strand.
    /// In `CanonicalUrn` rendering this is always the synthetic `edge_N` (which
    /// requires a header binding it to the cap URN). In `Aliased` rendering it
    /// is the cap's display alias when one exists (used directly in the wiring,
    /// NO header), or `edge_N` otherwise.
    edge_tokens: Vec<String>,
    /// Whether each edge needs a header (`[edge_N cap:...]`). True for synthetic
    /// `edge_N` tokens; false for cap aliases (the wiring references the
    /// registered alias directly, so a header would be redundant — and the
    /// grammar forbids an alias in the header's cap position anyway).
    needs_header: Vec<bool>,
    /// Node name for each `NodeId` in the strand. Indexed by
    /// `NodeId as usize`.
    node_names: Vec<String>,
}

fn build_serialization_plan(machine: &Machine, rendering: CapRendering<'_>) -> SerializationPlan {
    let mut strand_plans: Vec<StrandPlan> = Vec::with_capacity(machine.strand_count());
    let mut next_alias: usize = 0;
    let mut next_node: usize = 0;

    for strand in machine.strands() {
        let mut edge_tokens: Vec<String> = Vec::with_capacity(strand.edges().len());
        let mut needs_header: Vec<bool> = Vec::with_capacity(strand.edges().len());
        for edge in strand.edges() {
            match rendering.cap_alias(edge) {
                // Cap has a registered alias: use it directly in the wiring
                // (the parser resolves it as a registry cap alias), no header.
                Some(alias) => {
                    edge_tokens.push(alias);
                    needs_header.push(false);
                }
                // No alias (or canonical rendering): synthetic edge token with
                // a header binding it to the cap URN.
                None => {
                    edge_tokens.push(format!("edge_{}", next_alias));
                    needs_header.push(true);
                }
            }
            next_alias += 1;
        }
        // Nodes are named in the order the canonical edge walk first meets
        // them: each edge's sources in assignment order, then its outputs.
        // The name of a node is then a property of the machine, not of the
        // order its nodes happened to be allocated in — two inputs of one type
        // get the same names whichever was written first.
        let mut node_names: Vec<Option<String>> = vec![None; strand.nodes().len()];
        for edge in strand.edges() {
            for node in edge.source_nodes().chain(edge.output_nodes()) {
                if node_names[node as usize].is_none() {
                    node_names[node as usize] = Some(format!("n{}", next_node));
                    next_node += 1;
                }
            }
        }
        let node_names: Vec<String> = node_names
            .into_iter()
            .map(|name| name.expect("every node of a strand is a source or an output of an edge"))
            .collect();
        strand_plans.push(StrandPlan {
            edge_tokens,
            needs_header,
            node_names,
        });
    }

    SerializationPlan {
        strands: strand_plans,
    }
}

/// Emit one wiring statement (without enclosing brackets or
/// trailing newline) for a single edge inside a strand.
///
/// Every binding is written under its arg's name and every taken output under
/// its output's name, in the canonical (URN-sorted) order of the edge. Nothing
/// is left for a reader of the text to match by type, so what is written
/// resolves to the edge it was written from whatever other args the cap has.
/// A gather writes its arg's name once per member, in item order.
fn format_wiring(edge: &MachineEdge, alias: &str, strand_plan: &StrandPlan) -> String {
    let members: Vec<String> = edge
        .assignment
        .iter()
        .map(|b| match &b.source {
            BindingSource::Node(id) => {
                format!("{}={}", b.arg_name, strand_plan.node_names[*id as usize])
            }
            // `serde_json` writes a value as JSON, which is what a constant is.
            BindingSource::Constant(value) => format!("{}={}", b.arg_name, value),
        })
        .collect();
    let targets: Vec<String> = edge
        .outputs
        .iter()
        .map(|o| format!("{}={}", o.output_name, strand_plan.node_names[o.node as usize]))
        .collect();
    // The per-item map (`edge.is_loop`) is NOT emitted into notation text: it is a
    // derived cardinality property, not authored syntax. Re-parsing this notation
    // re-derives the same `is_loop` from the cap shapes, so round-trip stays
    // semantically stable.
    format!(
        "({}) -> {}{} -> ({})",
        members.join(", "),
        alias,
        edge.policy.to_notation(),
        targets.join(", ")
    )
}

fn emit_bracketed(
    machine: &Machine,
    plan: &SerializationPlan,
) -> Result<String, MachineAbstractionError> {
    let mut output = String::new();

    // Headers across all strands — only for edges whose cap-position token is a
    // synthetic `edge_N` (aliased caps are referenced directly in the wiring
    // and need no header).
    for (strand, strand_plan) in machine.strands().iter().zip(plan.strands.iter()) {
        for (edge_idx, edge) in strand.edges().iter().enumerate() {
            if strand_plan.needs_header[edge_idx] {
                write!(
                    output,
                    "[{} {}]",
                    strand_plan.edge_tokens[edge_idx], edge.cap_urn
                )
                .unwrap();
            }
        }
    }

    // Wirings across all strands.
    for (strand, strand_plan) in machine.strands().iter().zip(plan.strands.iter()) {
        for (edge_idx, edge) in strand.edges().iter().enumerate() {
            let wiring = format_wiring(edge, &strand_plan.edge_tokens[edge_idx], strand_plan);
            write!(output, "[{}]", wiring).unwrap();
        }
    }

    Ok(output)
}

fn emit_line_based(
    machine: &Machine,
    plan: &SerializationPlan,
) -> Result<String, MachineAbstractionError> {
    let mut lines: Vec<String> = Vec::new();

    for (strand, strand_plan) in machine.strands().iter().zip(plan.strands.iter()) {
        for (edge_idx, edge) in strand.edges().iter().enumerate() {
            if strand_plan.needs_header[edge_idx] {
                lines.push(format!(
                    "{} {}",
                    strand_plan.edge_tokens[edge_idx], edge.cap_urn
                ));
            }
        }
    }
    for (strand, strand_plan) in machine.strands().iter().zip(plan.strands.iter()) {
        for (edge_idx, edge) in strand.edges().iter().enumerate() {
            lines.push(format_wiring(
                edge,
                &strand_plan.edge_tokens[edge_idx],
                strand_plan,
            ));
        }
    }

    Ok(lines.join("\n"))
}

fn emit_multiline(
    machine: &Machine,
    plan: &SerializationPlan,
) -> Result<String, MachineAbstractionError> {
    let mut lines: Vec<String> = Vec::new();

    for (strand, strand_plan) in machine.strands().iter().zip(plan.strands.iter()) {
        for (edge_idx, edge) in strand.edges().iter().enumerate() {
            if strand_plan.needs_header[edge_idx] {
                lines.push(format!(
                    "[{} {}]",
                    strand_plan.edge_tokens[edge_idx], edge.cap_urn
                ));
            }
        }
    }
    for (strand, strand_plan) in machine.strands().iter().zip(plan.strands.iter()) {
        for (edge_idx, edge) in strand.edges().iter().enumerate() {
            let wiring = format_wiring(edge, &strand_plan.edge_tokens[edge_idx], strand_plan);
            lines.push(format!("[{}]", wiring));
        }
    }

    Ok(lines.join("\n"))
}

// =============================================================================
// Render-payload JSON for the JS renderer
// =============================================================================
//
// The Swift / JS visualization layer no longer reads
// `Machine.abstract_strand` (which has been deleted). Instead
// the gRPC layer ships the canonical machine notation as the
// machine's identity AND a render-payload JSON computed by the
// Rust side, which the JS renderer consumes directly.
//
// The render payload is a list of strands, each with its
// nodes, edges, and anchor sets. The JS renderer iterates the
// strands and draws each as a sub-graph.

impl Machine {
    /// Build the JSON payload the JS strand-graph renderer
    /// consumes. Shape (top-level array of strands):
    ///
    /// ```json
    /// {
    ///   "strands": [
    ///     {
    ///       "nodes": [
    ///         {"id": "n0", "urn": "media:document;ext=pdf", "title": "PDF Document"},
    ///         ...
    ///       ],
    ///       "edges": [
    ///         {
    ///           "alias": "edge_0",
    ///           "cap_urn": "cap:in=...;...;out=...",
    ///           "title": "Extract Text from PDF",
    ///           "is_loop": false,
    ///           "assignment": [
    ///             {
    ///               "cap_arg_media_urn": "media:document;ext=pdf",
    ///               "source_node": "n0"
    ///             }
    ///           ],
    ///           "target_node": "n1"
    ///         },
    ///         ...
    ///       ],
    ///       "input_anchor_nodes": ["n0"],
    ///       "output_anchor_nodes": ["n1"]
    ///     },
    ///     ...
    ///   ]
    /// }
    /// ```
    ///
    /// Each node carries the media-def title and each edge the cap
    /// definition title from the unified `FabricRegistry`. Lookups are
    /// cache-only (no network). A missing cached entry is a hard
    /// failure — we never synthesize a title from a URN string.
    ///
    /// Node names use the same global counter as the canonical
    /// notation, so a notation string and its render payload
    /// share the same node identities.
    pub fn to_render_payload_json(
        &self,
        fabric_registry: &FabricRegistry,
    ) -> Result<String, MachineAbstractionError> {
        if self.is_empty() {
            return Ok("{\"strands\":[]}".to_string());
        }
        // The render payload's `alias` slot is the canonical synthetic `edge_N`
        // identifier (alias-independent). The graph renderer labels each edge by
        // the cap's manifest TITLE (the `title` field below), not by a fabric
        // alias, so the plan uses canonical rendering here.
        let plan = build_serialization_plan(self, CapRendering::CanonicalUrn);
        let mut json = String::new();
        write!(json, "{{\"strands\":[").unwrap();
        for (s_idx, (strand, strand_plan)) in
            self.strands().iter().zip(plan.strands.iter()).enumerate()
        {
            if s_idx > 0 {
                json.push(',');
            }
            emit_strand_json(&mut json, strand, strand_plan, fabric_registry)?;
        }
        write!(json, "]}}").unwrap();
        Ok(json)
    }
}

fn emit_strand_json(
    json: &mut String,
    strand: &MachineStrand,
    plan: &StrandPlan,
    fabric_registry: &FabricRegistry,
) -> Result<(), MachineAbstractionError> {
    write!(json, "{{").unwrap();

    // nodes
    write!(json, "\"nodes\":[").unwrap();
    for (id, urn) in strand.nodes().iter().enumerate() {
        if id > 0 {
            json.push(',');
        }
        let urn_str = urn.to_string();
        let title = fabric_registry
            .get_cached_media_def(&urn_str)
            .map(|spec| spec.title)
            .ok_or_else(|| MachineAbstractionError::UncachedMediaDef {
                media_urn: urn_str.clone(),
            })?;
        write!(
            json,
            "{{\"id\":\"{}\",\"urn\":\"{}\",\"title\":\"{}\"}}",
            plan.node_names[id],
            json_escape(&urn_str),
            json_escape(&title)
        )
        .unwrap();
    }
    write!(json, "],").unwrap();

    // edges
    write!(json, "\"edges\":[").unwrap();
    for (e_idx, edge) in strand.edges().iter().enumerate() {
        if e_idx > 0 {
            json.push(',');
        }
        let cap_urn_str = edge.cap_urn.to_string();
        let cap_title = fabric_registry
            .get_cached_cap(&cap_urn_str)
            .map(|cap| cap.title)
            .ok_or_else(|| MachineAbstractionError::UncachedCap {
                cap_urn: cap_urn_str.clone(),
            })?;
        write!(
            json,
            "{{\"alias\":\"{}\",\"cap_urn\":\"{}\",\"title\":\"{}\",\"is_loop\":{},\"retry\":{},\"on_error\":\"{}\",\"included_as\":[{}],\"assignment\":[",
            plan.edge_tokens[e_idx],
            json_escape(&cap_urn_str),
            json_escape(&cap_title),
            edge.is_loop,
            edge.policy.retry,
            match edge.policy.on_error {
                super::syntax::OnError::Fail => "fail",
                super::syntax::OnError::Absent => "absent",
            },
            // The machine steps the step was included through, outermost
            // first: what a view groups the steps of an included machine by.
            edge.included_as
                .iter()
                .map(|step| format!("\"{}\"", json_escape(step)))
                .collect::<Vec<_>>()
                .join(",")
        )
        .unwrap();
        // Each binding says which arg (its URN and its name) and what feeds
        // it: a node of the strand, or a constant (a JSON value, as written).
        for (b_idx, b) in edge.assignment.iter().enumerate() {
            if b_idx > 0 {
                json.push(',');
            }
            write!(
                json,
                "{{\"cap_arg_media_urn\":\"{}\",\"arg_name\":\"{}\",",
                json_escape(&b.cap_arg_media_urn.to_string()),
                json_escape(&b.arg_name)
            )
            .unwrap();
            match &b.source {
                BindingSource::Node(id) => {
                    write!(json, "\"source_node\":\"{}\"}}", plan.node_names[*id as usize]).unwrap()
                }
                BindingSource::Constant(value) => write!(json, "\"constant\":{}}}", value).unwrap(),
            }
        }
        // Each output the step's wiring takes, and the node it goes to.
        write!(json, "],\"outputs\":[").unwrap();
        for (o_idx, o) in edge.outputs.iter().enumerate() {
            if o_idx > 0 {
                json.push(',');
            }
            write!(
                json,
                "{{\"output_media_urn\":\"{}\",\"output_name\":\"{}\",\"node\":\"{}\"}}",
                json_escape(&o.output_media_urn.to_string()),
                json_escape(&o.output_name),
                plan.node_names[o.node as usize]
            )
            .unwrap();
        }
        write!(json, "]}}").unwrap();
    }
    write!(json, "],").unwrap();

    // input_anchor_nodes
    write!(json, "\"input_anchor_nodes\":[").unwrap();
    for (i, id) in strand.input_anchor_ids().iter().enumerate() {
        if i > 0 {
            json.push(',');
        }
        write!(json, "\"{}\"", plan.node_names[*id as usize]).unwrap();
    }
    write!(json, "],").unwrap();

    // output_anchor_nodes
    write!(json, "\"output_anchor_nodes\":[").unwrap();
    for (i, id) in strand.output_anchor_ids().iter().enumerate() {
        if i > 0 {
            json.push(',');
        }
        write!(json, "\"{}\"", plan.node_names[*id as usize]).unwrap();
    }
    write!(json, "]").unwrap();

    write!(json, "}}").unwrap();
    Ok(())
}

/// Minimal JSON string-escape: only `\` and `"` need escaping
/// here because `MediaUrn::to_string()` and `CapUrn::to_string()`
/// produce ASCII-safe canonical text, and the only metacharacters
/// that can appear are quoted attribute values (which use `"`).
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::NotationFormat;
    use crate::machine::graph::Machine;
    use crate::machine::test_fixtures::{build_cap, cap_step, registry_with, strand_from_steps};

    fn extract_cap_def() -> crate::cap::definition::Cap {
        build_cap(
            "cap:extract;in=\"media:document;ext=pdf\";out=\"media:enc=utf-8;ext=txt\"",
            "extract",
            &["media:document;ext=pdf"],
            "media:enc=utf-8;ext=txt",
        )
    }

    fn embed_cap_def() -> crate::cap::definition::Cap {
        build_cap(
            "cap:embed;in=\"media:enc=utf-8\";out=\"media:vec;record\"",
            "embed",
            &["media:enc=utf-8"],
            "media:vec;record",
        )
    }

    fn pdf_to_vec_strand() -> crate::planner::Strand {
        strand_from_steps(
            vec![
                cap_step(
                    "cap:extract;in=\"media:document;ext=pdf\";out=\"media:enc=utf-8;ext=txt\"",
                    "extract",
                    "media:document;ext=pdf",
                    "media:enc=utf-8;ext=txt",
                ),
                cap_step(
                    "cap:embed;in=\"media:enc=utf-8\";out=\"media:vec;record\"",
                    "embed",
                    "media:enc=utf-8;ext=txt",
                    "media:vec;record",
                ),
            ],
            "pdf to vec",
        )
    }

    // TEST1172: Serializing a two-step strand emits the expected aliases and node names.
    #[test]
    fn test1172_serialize_two_step_strand_emits_global_aliases_and_node_names() {
        let registry = registry_with(vec![extract_cap_def(), embed_cap_def()]);
        let machine = Machine::from_strand(&pdf_to_vec_strand(), &registry).unwrap();
        let notation = machine.to_machine_notation().unwrap();
        // Two header brackets, two wiring brackets — `edge_0`
        // and `edge_1` from the global alias counter, `n0..n2`
        // from the global node counter.
        assert!(
            notation.contains("[edge_0 cap:") && notation.contains("[edge_1 cap:"),
            "headers must use edge_0 / edge_1 aliases, got: {notation}"
        );
        // A wiring is written with every binding under its arg's name and
        // every taken output under its output's name: nothing in the canonical
        // text is left to be matched by type when it is read back.
        assert!(
            notation.contains("[(input=n0) -> edge_0 -> (output=n1)]"),
            "first wiring should be `(input=n0) -> edge_0 -> (output=n1)`, got: {notation}"
        );
        assert!(
            notation.contains("[(input=n1) -> edge_1 -> (output=n2)]"),
            "second wiring should be `(input=n1) -> edge_1 -> (output=n2)`, got: {notation}"
        );
    }

    // TEST1173: Serializing and reparsing a machine preserves strict machine equivalence.
    #[test]
    fn test1173_serialize_then_parse_round_trip_preserves_strict_equivalence() {
        let registry = registry_with(vec![extract_cap_def(), embed_cap_def()]);
        let m1 = Machine::from_strand(&pdf_to_vec_strand(), &registry).unwrap();
        let notation = m1.to_machine_notation().unwrap();
        let m2 = Machine::from_string(&notation, &registry).expect("re-parse must succeed");
        assert!(
            m1.is_equivalent(&m2),
            "machine and its parse-reserialize must be strictly equivalent"
        );
        // And the second-pass notation must be byte-identical
        // to the first — canonical form.
        let notation2 = m2.to_machine_notation().unwrap();
        assert_eq!(
            notation, notation2,
            "canonical notation is a fixed point of parse-then-serialize"
        );
    }

    // TEST1174: The line-based notation format round-trips back to the same machine.
    #[test]
    fn test1174_line_based_format_round_trips_to_same_machine() {
        let registry = registry_with(vec![extract_cap_def(), embed_cap_def()]);
        let m1 = Machine::from_strand(&pdf_to_vec_strand(), &registry).unwrap();
        let line_based = m1
            .to_machine_notation_formatted(NotationFormat::LineBased)
            .unwrap();
        // Should not contain `[` brackets — line-based form
        // is one statement per line, no enclosing brackets.
        assert!(
            !line_based.contains('['),
            "line-based form must not contain brackets, got: {line_based}"
        );
        let m2 = Machine::from_string(&line_based, &registry).expect("line-based form must parse");
        assert!(m1.is_equivalent(&m2));
    }

    // TEST1196: `to_machine_notation_aliased` renders a cap header by its
    // registered display alias (shortest, then alphabetical), falls back to the
    // raw URN for a cap with no alias, and the result round-trips back to the
    // same machine (the parser resolves the alias from the warm cache).
    #[test]
    fn test1196_aliased_serialization_uses_alias_and_round_trips() {
        use crate::fabric::alias::StoredAlias;
        let registry = registry_with(vec![extract_cap_def(), embed_cap_def()]);
        // Two aliases on the extract cap; "ex" is shorter than "extract-pdf".
        registry.insert_cached_alias_for_test(StoredAlias {
            name: "extract-pdf".to_string(),
            target: "cap:extract;in=\"media:document;ext=pdf\";out=\"media:enc=utf-8;ext=txt\"".to_string(),
            version: 1,
        });
        registry.insert_cached_alias_for_test(StoredAlias {
            name: "ex".to_string(),
            target: "cap:extract;in=\"media:document;ext=pdf\";out=\"media:enc=utf-8;ext=txt\"".to_string(),
            version: 1,
        });
        // No alias for the embed cap → it must stay a raw URN.

        let m1 = Machine::from_strand(&pdf_to_vec_strand(), &registry).unwrap();
        let aliased = m1
            .to_machine_notation_aliased(&registry, NotationFormat::Bracketed)
            .unwrap();

        // The extract cap is aliased: referenced directly in the wiring by its
        // SHORTER alias `ex`, with NO header, and the URN must not appear.
        assert!(
            aliased.contains("-> ex ->"),
            "extract cap must be referenced in the wiring by the shortest alias `ex`, got: {aliased}"
        );
        assert!(
            !aliased.contains("cap:extract"),
            "the aliased extract cap URN must not appear, got: {aliased}"
        );
        // No header was emitted for the aliased extract edge (edge_0 is the
        // global counter value for it, but it gets no header).
        // The embed cap has no alias → it keeps its synthetic header + URN.
        assert!(
            aliased.contains("cap:embed"),
            "the un-aliased embed cap must keep its header URN, got: {aliased}"
        );

        // Round-trip: parse the aliased notation back. The alias is already in
        // the warm cache (seeded above), so the sync parser resolves it.
        let m2 = Machine::from_string(&aliased, &registry).expect("aliased notation must re-parse");
        assert!(
            m1.is_equivalent(&m2),
            "aliased serialize → parse must preserve strict equivalence"
        );
    }

    // TEST1175: Serializing an empty machine produces an empty string.
    #[test]
    fn test1175_empty_machine_serializes_to_empty_string() {
        let machine = Machine::from_resolved_strands(vec![]);
        let notation = machine.to_machine_notation().unwrap();
        assert!(notation.is_empty());
    }

    // TEST1176: Rendering payload JSON includes strand anchor metadata for a populated machine.
    #[test]
    fn test1176_render_payload_json_includes_strand_with_anchors() {
        use crate::machine::test_fixtures::seed_media_titles;
        let registry = registry_with(vec![extract_cap_def(), embed_cap_def()]);
        seed_media_titles(
            &registry,
            &[
                "media:document;ext=pdf",
                "media:enc=utf-8;ext=txt",
                "media:vec;record",
            ],
        );
        let machine = Machine::from_strand(&pdf_to_vec_strand(), &registry).unwrap();
        let payload = machine.to_render_payload_json(&registry).unwrap();
        // Should have a `strands` array, containing one strand
        // with `nodes`, `edges`, `input_anchor_nodes`,
        // `output_anchor_nodes`.
        assert!(payload.starts_with("{\"strands\":["));
        assert!(payload.contains("\"nodes\":["));
        assert!(payload.contains("\"edges\":["));
        assert!(payload.contains("\"input_anchor_nodes\":["));
        assert!(payload.contains("\"output_anchor_nodes\":["));
        // The two cap URNs should appear in the payload as
        // edge.cap_urn entries.
        assert!(payload.contains("extract"));
        assert!(payload.contains("embed"));
        // Titles should appear on nodes and edges.
        assert!(payload.contains("\"title\":\"Title for media:document;ext=pdf\""));
        assert!(payload.contains("\"title\":\"extract\""));
        assert!(payload.contains("\"title\":\"embed\""));
    }

    // TEST1177: Rendering payload JSON for an empty machine emits an empty strands array.
    #[test]
    fn test1177_render_payload_for_empty_machine_has_empty_strands_array() {
        let registry = registry_with(Vec::new());
        let machine = Machine::from_resolved_strands(vec![]);
        let payload = machine.to_render_payload_json(&registry).unwrap();
        assert_eq!(payload, "{\"strands\":[]}");
    }

    // TEST1137: A machine built from two independent strands serializes to a non-empty notation
    // string that contains both op tags. Checks that multi-strand serialization doesn't lose or
    // merge strands.
    #[test]
    fn test1137_two_strand_machine_serializes_to_notation_containing_both_ops() {
        let caption_cap = build_cap(
            "cap:in=media:image;caption;out=\"media:enc=utf-8;ext=txt\"",
            "caption",
            &["media:image"],
            "media:enc=utf-8;ext=txt",
        );
        let registry = registry_with(vec![extract_cap_def(), caption_cap]);

        let extract_strand = strand_from_steps(
            vec![cap_step(
                "cap:extract;in=\"media:document;ext=pdf\";out=\"media:enc=utf-8;ext=txt\"",
                "extract",
                "media:document;ext=pdf",
                "media:enc=utf-8;ext=txt",
            )],
            "extract strand",
        );
        let caption_strand = strand_from_steps(
            vec![cap_step(
                "cap:in=media:image;caption;out=\"media:enc=utf-8;ext=txt\"",
                "caption",
                "media:image",
                "media:enc=utf-8;ext=txt",
            )],
            "caption strand",
        );

        let machine = Machine::from_strands(&[extract_strand, caption_strand], &registry).unwrap();
        let notation = machine.to_machine_notation().unwrap();

        assert!(
            !notation.is_empty(),
            "notation must be non-empty for a two-strand machine"
        );
        assert!(
            notation.contains("extract"),
            "notation must contain the 'extract' op tag"
        );
        assert!(
            notation.contains("caption"),
            "notation must contain the 'caption' op tag"
        );
    }
}
