//! Editing a machine structurally: its notation, read as a draft of headers
//! and wirings, changed one operation at a time.
//!
//! The notation is the document. A picture of it carries structure, never
//! coordinates, so an edit made on the picture is a structural one — add a
//! step, remove one, rewire a member, join a node to a step's sources — and
//! every edit is the uikit's (`_machineOp*`), applied to the parsed draft and
//! written back as notation. Whether the result is a machine is not this
//! module's to say: the editor resolves what an operation wrote and keeps it
//! only when it resolves ([`crate::tui::app::edit`]).

use std::collections::BTreeSet;

use crate::machine::syntax::{parse_syntax, SyntaxStatementKind, SyntaxValue};
use crate::machine::{MachineSyntaxError, StepPolicy};
use crate::CapUrn;

/// An operation that cannot be applied to the draft as it stands.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EditError {
    #[error("the machine has no step {0}")]
    NoWiring(usize),
    #[error("step {wiring} has no source {source_index}")]
    NoSource { wiring: usize, source_index: usize },
    #[error("a step is inserted before a member a node feeds, not before a constant")]
    InsertBeforeConstant,
    #[error("removing a step and reconnecting around it takes a step with exactly one input and one output; disconnect it instead")]
    NotReconnectable,
    #[error("a step keeps at least one source — remove the step instead")]
    LastSource,
    #[error("'{0}' is already connected to this step")]
    AlreadyConnected(String),
    #[error("only a member a node feeds is rewired")]
    RewireConstant,
    #[error("this step takes no main output to rewire")]
    NoMainOutput,
    #[error("step {0} runs '{1}', which no header names")]
    MissingHeader(usize, String),
    #[error("'{0}' is not a cap URN: {1}")]
    InvalidCap(String, String),
    #[error("'{0}' is not a node name")]
    BadName(String),
}

/// A header: the cap or the machine an alias names.
#[derive(Debug, Clone, PartialEq)]
pub enum DraftHeader {
    Cap { alias: String, cap_urn: String },
    Machine { alias: String, reference: String },
}

impl DraftHeader {
    pub fn alias(&self) -> &str {
        match self {
            DraftHeader::Cap { alias, .. } | DraftHeader::Machine { alias, .. } => alias,
        }
    }
}

/// What a member of a step's sources gives its arg.
#[derive(Debug, Clone, PartialEq)]
pub enum DraftValue {
    Node(String),
    Constant(serde_json::Value),
}

/// One member of a step's sources.
#[derive(Debug, Clone, PartialEq)]
pub struct DraftMember {
    /// The arg it names, when it names one.
    pub name: Option<String>,
    pub value: DraftValue,
}

/// One output a step's result is taken into.
#[derive(Debug, Clone, PartialEq)]
pub struct DraftTarget {
    /// The output it names; `None` is the step's main output.
    pub name: Option<String>,
    pub node: String,
}

/// One step: what it reads, the cap it runs, what it makes.
#[derive(Debug, Clone, PartialEq)]
pub struct DraftWiring {
    pub sources: Vec<DraftMember>,
    pub cap_alias: String,
    pub policy: StepPolicy,
    pub targets: Vec<DraftTarget>,
}

impl DraftWiring {
    /// The nodes it reads.
    pub fn source_nodes(&self) -> Vec<&str> {
        self.sources
            .iter()
            .filter_map(|m| match &m.value {
                DraftValue::Node(n) => Some(n.as_str()),
                DraftValue::Constant(_) => None,
            })
            .collect()
    }

    fn plain(source: &str, alias: &str, target: &str) -> Self {
        Self {
            sources: vec![DraftMember {
                name: None,
                value: DraftValue::Node(source.to_string()),
            }],
            cap_alias: alias.to_string(),
            policy: StepPolicy::default(),
            targets: vec![DraftTarget {
                name: None,
                node: target.to_string(),
            }],
        }
    }
}

/// A machine as its notation writes it: its headers and its steps.
#[derive(Debug, Clone, PartialEq)]
pub struct MachineDraft {
    pub headers: Vec<DraftHeader>,
    pub wirings: Vec<DraftWiring>,
}

/// A constant as notation writes it: its JSON.
fn constant_text(value: &serde_json::Value) -> String {
    value.to_string()
}

impl MachineDraft {
    /// Read `notation`'s structure. Nothing is resolved: the draft is what
    /// the text says, whether or not it is a machine yet.
    pub fn parse(notation: &str) -> Result<Self, MachineSyntaxError> {
        let mut draft = MachineDraft {
            headers: Vec::new(),
            wirings: Vec::new(),
        };
        for statement in parse_syntax(notation)? {
            match statement.kind {
                SyntaxStatementKind::CapHeader { alias, cap_urn } => draft.headers.push(DraftHeader::Cap {
                    alias: alias.value,
                    cap_urn: cap_urn.value,
                }),
                SyntaxStatementKind::MachineHeader { alias, reference } => draft.headers.push(DraftHeader::Machine {
                    alias: alias.value,
                    reference: reference.value,
                }),
                SyntaxStatementKind::Wiring(wiring) => draft.wirings.push(DraftWiring {
                    sources: wiring
                        .sources
                        .into_iter()
                        .map(|member| DraftMember {
                            name: member.name.map(|n| n.value),
                            value: match member.value {
                                SyntaxValue::Node(node) => DraftValue::Node(node.value),
                                SyntaxValue::Constant(constant) => DraftValue::Constant(constant.value),
                            },
                        })
                        .collect(),
                    cap_alias: wiring.step.value,
                    policy: wiring.policy,
                    targets: wiring
                        .targets
                        .into_iter()
                        .map(|target| DraftTarget {
                            name: target.name.map(|n| n.value),
                            node: target.node.value,
                        })
                        .collect(),
                }),
            }
        }
        Ok(draft)
    }

    /// The draft as notation: one statement a line, headers first. One
    /// unnamed member, or one unnamed output, is written bare; anything else
    /// is a group.
    pub fn to_notation(&self) -> String {
        let mut lines = Vec::with_capacity(self.headers.len() + self.wirings.len());
        for header in &self.headers {
            lines.push(match header {
                DraftHeader::Cap { alias, cap_urn } => format!("[{alias} {cap_urn}]"),
                DraftHeader::Machine { alias, reference } => {
                    format!("[{alias} machine {}]", constant_text(&serde_json::Value::String(reference.clone())))
                }
            });
        }
        let member = |m: &DraftMember| -> String {
            match (&m.name, &m.value) {
                (Some(name), DraftValue::Constant(c)) => format!("{name}={}", constant_text(c)),
                (None, DraftValue::Constant(c)) => constant_text(c),
                (Some(name), DraftValue::Node(n)) => format!("{name}={n}"),
                (None, DraftValue::Node(n)) => n.clone(),
            }
        };
        let taken = |t: &DraftTarget| -> String {
            match &t.name {
                Some(name) => format!("{name}={}", t.node),
                None => t.node.clone(),
            }
        };
        for wiring in &self.wirings {
            let sources = match wiring.sources.as_slice() {
                [only] if only.name.is_none() && matches!(only.value, DraftValue::Node(_)) => member(only),
                many => format!("({})", many.iter().map(member).collect::<Vec<_>>().join(", ")),
            };
            let targets = match wiring.targets.as_slice() {
                [only] if only.name.is_none() => only.node.clone(),
                many => format!("({})", many.iter().map(taken).collect::<Vec<_>>().join(", ")),
            };
            lines.push(format!(
                "[{sources} -> {}{} -> {targets}]",
                wiring.cap_alias,
                wiring.policy.to_notation()
            ));
        }
        let mut text = lines.join("\n");
        text.push('\n');
        text
    }

    /// Every node the steps read or make.
    pub fn node_names(&self) -> BTreeSet<String> {
        let mut names = BTreeSet::new();
        for wiring in &self.wirings {
            for node in wiring.source_nodes() {
                names.insert(node.to_string());
            }
            for target in &wiring.targets {
                names.insert(target.node.clone());
            }
        }
        names
    }

    /// The nodes no step makes: the machine's inputs.
    pub fn input_names(&self) -> BTreeSet<String> {
        let made: BTreeSet<&str> = self
            .wirings
            .iter()
            .flat_map(|w| w.targets.iter().map(|t| t.node.as_str()))
            .collect();
        self.node_names()
            .into_iter()
            .filter(|n| !made.contains(n.as_str()))
            .collect()
    }

    /// The nodes no step reads: the machine's outputs.
    pub fn output_names(&self) -> BTreeSet<String> {
        let read: BTreeSet<&str> = self.wirings.iter().flat_map(|w| w.source_nodes()).collect();
        self.node_names()
            .into_iter()
            .filter(|n| !read.contains(n.as_str()))
            .collect()
    }

    /// A name no node and no alias bears: `prefix` and the first free number.
    pub fn fresh_name(&self, prefix: &str) -> String {
        let mut taken = self.node_names();
        for header in &self.headers {
            taken.insert(header.alias().to_string());
        }
        (0..)
            .map(|i| format!("{prefix}{i}"))
            .find(|name| !taken.contains(name))
            .expect("an unbounded range has a free name")
    }

    /// The cap URN step `wiring` runs, when it runs a cap (not a machine).
    pub fn cap_of(&self, wiring: usize) -> Result<Option<&str>, EditError> {
        let w = self.wirings.get(wiring).ok_or(EditError::NoWiring(wiring))?;
        match self.headers.iter().find(|h| h.alias() == w.cap_alias) {
            Some(DraftHeader::Cap { cap_urn, .. }) => Ok(Some(cap_urn.as_str())),
            Some(DraftHeader::Machine { .. }) => Ok(None),
            None => Err(EditError::MissingHeader(wiring, w.cap_alias.clone())),
        }
    }

    /// The alias of the header naming `cap_urn` — an equivalent cap, read as
    /// a parsed URN — minting one when none does.
    fn ensure_header(&mut self, cap_urn: &str) -> Result<String, EditError> {
        let wanted = CapUrn::from_string(cap_urn).map_err(|e| EditError::InvalidCap(cap_urn.to_string(), e.to_string()))?;
        for header in &self.headers {
            if let DraftHeader::Cap { alias, cap_urn: existing } = header {
                let parsed = CapUrn::from_string(existing)
                    .map_err(|e| EditError::InvalidCap(existing.clone(), e.to_string()))?;
                if parsed.is_equivalent(&wanted) {
                    return Ok(alias.clone());
                }
            }
        }
        let alias = self.fresh_name("edge_");
        self.headers.push(DraftHeader::Cap {
            alias: alias.clone(),
            cap_urn: wanted.to_string(),
        });
        Ok(alias)
    }

    fn drop_orphan_header(&mut self, alias: &str) {
        if !self.wirings.iter().any(|w| w.cap_alias == alias) {
            self.headers.retain(|h| h.alias() != alias);
        }
    }

    fn wiring_mut(&mut self, index: usize) -> Result<&mut DraftWiring, EditError> {
        self.wirings.get_mut(index).ok_or(EditError::NoWiring(index))
    }

    /// A new step reading `source` into a fresh node.
    pub fn connect(&mut self, source: &str, cap_urn: &str) -> Result<(), EditError> {
        let alias = self.ensure_header(cap_urn)?;
        let target = self.fresh_name("n");
        self.wirings.push(DraftWiring::plain(source, &alias, &target));
        Ok(())
    }

    /// A new step reading `source` into `target`.
    pub fn connect_to(&mut self, source: &str, target: &str, cap_urn: &str) -> Result<(), EditError> {
        let alias = self.ensure_header(cap_urn)?;
        self.wirings.push(DraftWiring::plain(source, &alias, target));
        Ok(())
    }

    /// A new step of its own: a fresh input into a fresh output.
    pub fn create_strand(&mut self, cap_urn: &str) -> Result<(), EditError> {
        let alias = self.ensure_header(cap_urn)?;
        let source = self.fresh_name("n");
        let mut with_source = self.clone();
        with_source.wirings.push(DraftWiring::plain(&source, &alias, &source));
        let target = with_source.fresh_name("n");
        self.wirings.push(DraftWiring::plain(&source, &alias, &target));
        Ok(())
    }

    /// Remove a step, without rewiring anything (and its header when no
    /// other step uses it).
    pub fn disconnect(&mut self, wiring: usize) -> Result<(), EditError> {
        if wiring >= self.wirings.len() {
            return Err(EditError::NoWiring(wiring));
        }
        let removed = self.wirings.remove(wiring);
        self.drop_orphan_header(&removed.cap_alias);
        Ok(())
    }

    /// Insert a step before member `source_index` of step `wiring`:
    /// `[src -> new -> mid]`, and the member — still naming the arg it names
    /// — reads `mid`.
    pub fn insert_cap(&mut self, wiring: usize, source_index: usize, cap_urn: &str) -> Result<(), EditError> {
        let mid = self.fresh_name("n");
        let member = self
            .wiring_mut(wiring)?
            .sources
            .get(source_index)
            .ok_or(EditError::NoSource { wiring, source_index })?
            .clone();
        let DraftValue::Node(upstream) = member.value else {
            return Err(EditError::InsertBeforeConstant);
        };
        let alias = self.ensure_header(cap_urn)?;
        self.wiring_mut(wiring)?.sources[source_index].value = DraftValue::Node(mid.clone());
        self.wirings.insert(wiring, DraftWiring::plain(&upstream, &alias, &mid));
        Ok(())
    }

    /// Remove a step, reconnecting around it: a step fed by one node into
    /// one output has its readers read that node instead. Any other step
    /// cannot be reconnected without guessing, and is refused.
    pub fn delete_step(&mut self, wiring: usize) -> Result<(), EditError> {
        let w = self.wirings.get(wiring).ok_or(EditError::NoWiring(wiring))?;
        let fed = w.source_nodes();
        if fed.len() != 1 || w.sources.len() != 1 || w.targets.len() != 1 {
            return Err(EditError::NotReconnectable);
        }
        let source = fed[0].to_string();
        let removed = self.wirings.remove(wiring);
        let produced = removed.targets[0].node.clone();
        for w in &mut self.wirings {
            for member in &mut w.sources {
                if member.value == DraftValue::Node(produced.clone()) {
                    member.value = DraftValue::Node(source.clone());
                }
            }
            // The same node read twice by unnamed members is one member.
            let mut seen: Vec<String> = Vec::new();
            w.sources.retain(|member| match (&member.name, &member.value) {
                (None, DraftValue::Node(n)) => {
                    if seen.contains(n) {
                        false
                    } else {
                        seen.push(n.clone());
                        true
                    }
                }
                _ => true,
            });
        }
        self.drop_orphan_header(&removed.cap_alias);
        Ok(())
    }

    /// A fresh input joins step `wiring`'s sources.
    pub fn add_source(&mut self, wiring: usize) -> Result<(), EditError> {
        let name = self.fresh_name("n");
        self.wiring_mut(wiring)?.sources.push(DraftMember {
            name: None,
            value: DraftValue::Node(name),
        });
        Ok(())
    }

    /// An existing node joins step `wiring`'s sources.
    pub fn add_existing_source(&mut self, wiring: usize, node: &str) -> Result<(), EditError> {
        let w = self.wiring_mut(wiring)?;
        if w.source_nodes().contains(&node) {
            return Err(EditError::AlreadyConnected(node.to_string()));
        }
        w.sources.push(DraftMember {
            name: None,
            value: DraftValue::Node(node.to_string()),
        });
        Ok(())
    }

    /// Remove member `source_index` of step `wiring`.
    pub fn remove_source(&mut self, wiring: usize, source_index: usize) -> Result<(), EditError> {
        let w = self.wiring_mut(wiring)?;
        if source_index >= w.sources.len() {
            return Err(EditError::NoSource { wiring, source_index });
        }
        if w.sources.len() <= 1 {
            return Err(EditError::LastSource);
        }
        w.sources.remove(source_index);
        Ok(())
    }

    /// Member `source_index` of step `wiring` reads `node` instead; it keeps
    /// the arg it names.
    pub fn rewire_source(&mut self, wiring: usize, source_index: usize, node: &str) -> Result<(), EditError> {
        check_name(node)?;
        let w = self.wiring_mut(wiring)?;
        let member = w
            .sources
            .get_mut(source_index)
            .ok_or(EditError::NoSource { wiring, source_index })?;
        if matches!(member.value, DraftValue::Constant(_)) {
            return Err(EditError::RewireConstant);
        }
        member.value = DraftValue::Node(node.to_string());
        Ok(())
    }

    /// Step `wiring`'s main output goes into `node` instead.
    pub fn rewire_target(&mut self, wiring: usize, node: &str) -> Result<(), EditError> {
        check_name(node)?;
        let w = self.wiring_mut(wiring)?;
        let main = w.targets.iter_mut().find(|t| t.name.is_none()).ok_or(EditError::NoMainOutput)?;
        main.node = node.to_string();
        Ok(())
    }

    /// Step `wiring` runs `cap_urn` instead.
    pub fn set_cap(&mut self, wiring: usize, cap_urn: &str) -> Result<(), EditError> {
        let previous = self
            .wirings
            .get(wiring)
            .ok_or(EditError::NoWiring(wiring))?
            .cap_alias
            .clone();
        let alias = self.ensure_header(cap_urn)?;
        self.wirings[wiring].cap_alias = alias;
        self.drop_orphan_header(&previous);
        Ok(())
    }
}

/// A node name as the notation reads one: an alias, and not a constant word.
fn check_name(name: &str) -> Result<(), EditError> {
    let valid = !name.is_empty()
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        && !["true", "false", "null"].contains(&name);
    if valid {
        Ok(())
    } else {
        Err(EditError::BadName(name.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXTRACT: &str = r#"cap:in="media:document;ext=pdf";extract;out="media:enc=utf-8;ext=txt""#;
    const SUMMARIZE: &str = r#"cap:in="media:enc=utf-8";summarize;out="media:enc=utf-8;summary""#;
    const TRANSLATE: &str = r#"cap:in="media:enc=utf-8";translate;out="media:enc=utf-8;translated""#;

    fn draft(text: &str) -> MachineDraft {
        MachineDraft::parse(text).unwrap_or_else(|e| panic!("{e}: {text}"))
    }

    // TEST13060: a draft written back is notation the parser reads as the
    // same draft — headers, named members, constants, groups, policies and
    // named outputs all survive the round trip.
    #[test]
    fn test13060_draft_round_trips_through_notation() {
        let text = format!(
            "[extract {EXTRACT}][summarize {SUMMARIZE}]\
             [doc -> extract{{retry=2}} -> text]\
             [(text, for=purpose, depth=3) -> summarize{{on_error=absent}} -> (summary, notes=extra)]"
        );
        let first = draft(&text);
        let written = first.to_notation();
        assert_eq!(draft(&written), first, "written:\n{written}");
        assert!(written.contains("depth=3"), "{written}");
        assert!(written.contains("{retry=2}"), "{written}");
        assert!(written.contains("(summary, notes=extra)"), "{written}");
    }

    // TEST13061: the uikit's structural operations — a step inserted before
    // a member reads what the member read and feeds it a fresh node; a step
    // removed with reconnection hands its readers its input; a cap set on a
    // step reuses an equivalent header and drops the one nothing uses.
    #[test]
    fn test13061_structural_operations_rewrite_the_draft() {
        let base = format!("[extract {EXTRACT}][summarize {SUMMARIZE}][doc -> extract -> text][text -> summarize -> summary]");
        let mut d = draft(&base);
        d.insert_cap(1, 0, TRANSLATE).unwrap();
        // [doc -> extract -> text][text -> edge_0 -> n0][n0 -> summarize -> summary]
        assert_eq!(d.wirings.len(), 3);
        assert_eq!(d.wirings[1].source_nodes(), vec!["text"]);
        let mid = d.wirings[1].targets[0].node.clone();
        assert_eq!(d.wirings[2].source_nodes(), vec![mid.as_str()]);
        assert!(draft(&d.to_notation()).wirings.len() == 3);

        d.delete_step(1).unwrap();
        assert_eq!(d.wirings[1].source_nodes(), vec!["text"], "summarize reads extract's output again");
        assert!(!d.headers.iter().any(|h| h.alias() == "edge_0"), "the header nothing uses is gone");

        d.set_cap(1, EXTRACT).unwrap();
        assert_eq!(d.wirings[1].cap_alias, "extract", "an equivalent header is reused");
        assert!(!d.headers.iter().any(|h| h.alias() == "summarize"), "the replaced header is dropped");

        let mut fan_in = draft(&format!("[summarize {SUMMARIZE}][(a, b) -> summarize -> s]"));
        assert_eq!(fan_in.delete_step(0), Err(EditError::NotReconnectable));
        assert_eq!(fan_in.remove_source(0, 0), Ok(()));
        assert_eq!(fan_in.remove_source(0, 0), Err(EditError::LastSource));
    }

    // TEST13062: what a machine reads and makes follows from its steps: a
    // node no step makes is an input, one no step reads an output; and a
    // fresh name is never one a node or an alias already bears.
    #[test]
    fn test13062_inputs_outputs_and_fresh_names() {
        let d = draft(&format!("[extract {EXTRACT}][summarize {SUMMARIZE}][n0 -> extract -> n1][n1 -> summarize -> n2]"));
        assert_eq!(d.input_names().into_iter().collect::<Vec<_>>(), vec!["n0"]);
        assert_eq!(d.output_names().into_iter().collect::<Vec<_>>(), vec!["n2"]);
        assert_eq!(d.fresh_name("n"), "n3");
        let aliased = draft(&format!("[edge_0 {EXTRACT}][n0 -> edge_0 -> n1]"));
        assert_eq!(aliased.fresh_name("edge_"), "edge_1", "an alias is a taken name too");
    }
}
