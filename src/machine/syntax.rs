//! The syntax of machine notation: one walk of the pest parse tree into a
//! tree of statements whose every part knows where it stands in the text.
//!
//! This is the only reader of the grammar's rules (`machine.pest`). The
//! resolver's parser (`parser.rs`) consumes the tree for what it says; the
//! editor support (`notation_ast.rs`) consumes it for where each part is.
//! Nothing here knows a cap definition: a name is a name, a node is a node,
//! and whether either exists is the resolver's question.
//!
//! What IS decided here is everything the text alone decides
//! (`capdag/docs/09-machine-notation.md` §Statement kinds):
//!
//! - a constant is always named (`UnnamedConstant`);
//! - `true`, `false` and `null` are constants, never node names
//!   (`ReservedNodeName`);
//! - at most one target of a wiring is unnamed (`SeveralUnnamedTargets`);
//! - a step policy holds known keys, once each, with values from the table
//!   (`InvalidPolicy`).

use pest::iterators::Pair;
use pest::Parser;

use super::error::MachineSyntaxError;
use super::parser::{MachineParser, Rule};

/// A part of the text: its value and the byte range it was read from.
#[derive(Debug, Clone, PartialEq)]
pub struct Spanned<T> {
    pub value: T,
    pub start: usize,
    pub end: usize,
}

impl<T> Spanned<T> {
    fn of(pair: &Pair<'_, Rule>, value: T) -> Self {
        let span = pair.as_span();
        Spanned {
            value,
            start: span.start(),
            end: span.end(),
        }
    }
}

fn text(pair: &Pair<'_, Rule>) -> Spanned<String> {
    Spanned::of(pair, pair.as_str().to_string())
}

/// What a step does when it fails (`capdag/docs/09-machine-notation.md` §Step
/// policy). The default — no policy written — is to fail the run at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize)]
pub struct StepPolicy {
    /// Further attempts after a failure whose class is retryable.
    #[serde(default)]
    pub retry: u32,
    /// What a failure that stands (after its retries) does to the run.
    #[serde(default)]
    pub on_error: OnError,
}

/// What a step's lasting failure does to the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnError {
    /// The run fails.
    #[default]
    Fail,
    /// The step's outputs are absent and the run goes on.
    Absent,
}

impl StepPolicy {
    /// Whether this is the policy of a step that wrote none.
    pub fn is_default(&self) -> bool {
        *self == StepPolicy::default()
    }

    /// The policy as notation writes it after a step alias: `{retry=3}`,
    /// `{on_error=absent}`, both in that order, or nothing for the default.
    pub fn to_notation(&self) -> String {
        let mut entries = Vec::new();
        if self.retry > 0 {
            entries.push(format!("retry={}", self.retry));
        }
        if self.on_error == OnError::Absent {
            entries.push("on_error=absent".to_string());
        }
        if entries.is_empty() {
            String::new()
        } else {
            format!("{{{}}}", entries.join(", "))
        }
    }
}

/// What a member of a source group gives its arg.
#[derive(Debug, Clone, PartialEq)]
pub enum SyntaxValue {
    /// A node: a data position of the machine.
    Node(Spanned<String>),
    /// A constant: a JSON value written in the machine.
    Constant(Spanned<serde_json::Value>),
}

/// One member of a wiring's sources.
#[derive(Debug, Clone, PartialEq)]
pub struct SyntaxMember {
    /// The arg the member names, when it names one.
    pub name: Option<Spanned<String>>,
    pub value: SyntaxValue,
}

/// One target of a wiring.
#[derive(Debug, Clone, PartialEq)]
pub struct SyntaxTarget {
    /// The output the target names. `None`: the step's main output.
    pub name: Option<Spanned<String>>,
    pub node: Spanned<String>,
}

/// A wiring: one invocation.
#[derive(Debug, Clone, PartialEq)]
pub struct SyntaxWiring {
    pub sources: Vec<SyntaxMember>,
    /// The range of the whole sources part, group parentheses included.
    pub sources_span: (usize, usize),
    pub step: Spanned<String>,
    pub policy: StepPolicy,
    /// The range of the policy as written, braces included, when one is.
    pub policy_span: Option<(usize, usize)>,
    /// The ranges of the two arrows.
    pub arrow_spans: [(usize, usize); 2],
    pub targets: Vec<SyntaxTarget>,
    pub targets_span: (usize, usize),
}

/// One statement of a program.
#[derive(Debug, Clone, PartialEq)]
pub enum SyntaxStatementKind {
    /// `alias cap:...`
    CapHeader {
        alias: Spanned<String>,
        cap_urn: Spanned<String>,
    },
    /// `alias machine "<reference>"`
    MachineHeader {
        alias: Spanned<String>,
        /// The reference, unescaped.
        reference: Spanned<String>,
    },
    Wiring(SyntaxWiring),
}

/// A statement and where it stands.
#[derive(Debug, Clone, PartialEq)]
pub struct SyntaxStatement {
    pub kind: SyntaxStatementKind,
    /// The statement's index among the program's statements.
    pub position: usize,
    /// The range of the statement, brackets included when it has them.
    pub start: usize,
    pub end: usize,
}

/// The words the notation reads as constants. A node may not bear one.
const RESERVED: [&str; 3] = ["true", "false", "null"];

fn node(pair: &Pair<'_, Rule>, position: usize) -> Result<Spanned<String>, MachineSyntaxError> {
    let name = text(pair);
    if RESERVED.contains(&name.value.as_str()) {
        return Err(MachineSyntaxError::ReservedNodeName {
            name: name.value,
            position,
        });
    }
    Ok(name)
}

fn constant(pair: &Pair<'_, Rule>) -> Result<Spanned<serde_json::Value>, MachineSyntaxError> {
    // The grammar's `constant` is JSON's value grammar, so what matched is
    // JSON text. What the grammar cannot say is still refused here: a number
    // no float holds (`1e999`), and a string escape that names half a
    // character (`"\ud800"`).
    let value = serde_json::from_str(pair.as_str()).map_err(|error| MachineSyntaxError::ParseError {
        details: format!("{} is not a JSON value: {error}", pair.as_str().trim()),
    })?;
    Ok(Spanned::of(pair, value))
}

fn member(pair: Pair<'_, Rule>, position: usize) -> Result<SyntaxMember, MachineSyntaxError> {
    let inner = pair.into_inner().next().expect("member wraps one form");
    match inner.as_rule() {
        Rule::named_member => {
            let mut parts = inner.into_inner();
            let name = text(&parts.next().expect("named member has a name"));
            let value = parts.next().expect("named member has a value");
            let value = match value.as_rule() {
                Rule::constant => SyntaxValue::Constant(constant(&value)?),
                Rule::alias => SyntaxValue::Node(node(&value, position)?),
                other => unreachable!("a named member's value is a constant or an alias, not {other:?}"),
            };
            Ok(SyntaxMember {
                name: Some(name),
                value,
            })
        }
        // A constant has no type of its own to be matched by: it says which
        // arg it is for, or it is not a member.
        Rule::constant => Err(MachineSyntaxError::UnnamedConstant {
            constant: inner.as_str().to_string(),
            position,
        }),
        Rule::alias => Ok(SyntaxMember {
            name: None,
            value: SyntaxValue::Node(node(&inner, position)?),
        }),
        other => unreachable!("a member is named, a constant or an alias, not {other:?}"),
    }
}

fn policy(pair: Pair<'_, Rule>, step: &str, position: usize) -> Result<StepPolicy, MachineSyntaxError> {
    let invalid = |details: String| MachineSyntaxError::InvalidPolicy {
        step: step.to_string(),
        position,
        details,
    };
    let mut policy = StepPolicy::default();
    let mut seen: Vec<String> = Vec::new();
    for entry in pair.into_inner() {
        let mut parts = entry.into_inner();
        let key = parts.next().expect("policy entry has a key").as_str().to_string();
        let value = parts
            .next()
            .expect("policy entry has a value")
            .into_inner()
            .next()
            .expect("policy value wraps a number or a word");
        if seen.contains(&key) {
            return Err(invalid(format!("'{key}' is given twice")));
        }
        match key.as_str() {
            "retry" => {
                // A count of further attempts: a whole number, not negative.
                policy.retry = (value.as_rule() == Rule::number)
                    .then(|| value.as_str().parse::<u32>().ok())
                    .flatten()
                    .ok_or_else(|| {
                        invalid(format!(
                            "retry takes a whole number of further attempts, not '{}'",
                            value.as_str()
                        ))
                    })?;
            }
            "on_error" => {
                if value.as_str() != "absent" {
                    return Err(invalid(format!(
                        "on_error takes 'absent', not '{}'",
                        value.as_str()
                    )));
                }
                policy.on_error = OnError::Absent;
            }
            other => {
                return Err(invalid(format!(
                    "unknown key '{other}' (a policy takes retry and on_error)"
                )));
            }
        }
        seen.push(key);
    }
    Ok(policy)
}

fn wiring(pair: Pair<'_, Rule>, position: usize) -> Result<SyntaxWiring, MachineSyntaxError> {
    let mut parts = pair.into_inner();

    let sources_pair = parts.next().expect("wiring has sources");
    let sources_span = (sources_pair.as_span().start(), sources_pair.as_span().end());
    let sources_inner = sources_pair.into_inner().next().expect("sources wraps one form");
    let sources = match sources_inner.as_rule() {
        Rule::alias => vec![SyntaxMember {
            name: None,
            value: SyntaxValue::Node(node(&sources_inner, position)?),
        }],
        Rule::source_group => sources_inner
            .into_inner()
            .map(|m| member(m, position))
            .collect::<Result<Vec<_>, _>>()?,
        other => unreachable!("sources is a group or an alias, not {other:?}"),
    };

    let span_of = |pair: Pair<'_, Rule>| (pair.as_span().start(), pair.as_span().end());
    let first_arrow = span_of(parts.next().expect("wiring has a first arrow"));
    let mut step_parts = parts.next().expect("wiring has a step").into_inner();
    let step = text(&step_parts.next().expect("step has an alias"));
    let (policy, policy_span) = match step_parts.next() {
        Some(pair) => {
            let span = (pair.as_span().start(), pair.as_span().end());
            (policy(pair, &step.value, position)?, Some(span))
        }
        None => (StepPolicy::default(), None),
    };

    let second_arrow = span_of(parts.next().expect("wiring has a second arrow"));
    let targets_pair = parts.next().expect("wiring has targets");
    let targets_span = (targets_pair.as_span().start(), targets_pair.as_span().end());
    let targets_inner = targets_pair.into_inner().next().expect("targets wraps one form");
    let targets = match targets_inner.as_rule() {
        Rule::alias => vec![SyntaxTarget {
            name: None,
            node: node(&targets_inner, position)?,
        }],
        Rule::target_group => {
            let mut targets = Vec::new();
            for target in targets_inner.into_inner() {
                let inner = target.into_inner().next().expect("target wraps one form");
                targets.push(match inner.as_rule() {
                    Rule::named_target => {
                        let mut named = inner.into_inner();
                        let name = text(&named.next().expect("named target has a name"));
                        let node = node(&named.next().expect("named target has a node"), position)?;
                        SyntaxTarget {
                            name: Some(name),
                            node,
                        }
                    }
                    Rule::alias => SyntaxTarget {
                        name: None,
                        node: node(&inner, position)?,
                    },
                    other => unreachable!("a target is named or an alias, not {other:?}"),
                });
            }
            targets
        }
        other => unreachable!("targets is a group or an alias, not {other:?}"),
    };
    // The unnamed target is the main output, and a step has one.
    let unnamed: Vec<&str> = targets
        .iter()
        .filter(|t| t.name.is_none())
        .map(|t| t.node.value.as_str())
        .collect();
    if unnamed.len() > 1 {
        return Err(MachineSyntaxError::SeveralUnnamedTargets {
            position,
            targets: unnamed.iter().map(|s| s.to_string()).collect(),
        });
    }

    Ok(SyntaxWiring {
        sources,
        sources_span,
        step,
        policy,
        policy_span,
        arrow_spans: [first_arrow, second_arrow],
        targets,
        targets_span,
    })
}

/// Read `input` into its statements, in the order written.
///
/// Fails on text the grammar does not accept (`ParseError`) and on the rules
/// the text alone decides (see the module documentation). An input with no
/// statement at all is not an error here: what an empty program means is the
/// caller's question.
pub fn parse_syntax(input: &str) -> Result<Vec<SyntaxStatement>, MachineSyntaxError> {
    let program = MachineParser::parse(Rule::program, input)
        .map_err(|e| MachineSyntaxError::ParseError {
            details: format!("{}", e),
        })?
        .next()
        .expect("pest produces a program rule");

    let mut statements = Vec::new();
    for pair in program.into_inner() {
        if pair.as_rule() != Rule::stmt {
            continue; // EOI
        }
        let position = statements.len();
        let (start, end) = (pair.as_span().start(), pair.as_span().end());
        let content = pair
            .into_inner()
            .next()
            .expect("stmt wraps inner")
            .into_inner()
            .next()
            .expect("inner wraps a header or a wiring");
        let kind = match content.as_rule() {
            Rule::header => {
                let mut parts = content.into_inner();
                SyntaxStatementKind::CapHeader {
                    alias: text(&parts.next().expect("header has an alias")),
                    cap_urn: text(&parts.next().expect("header has a cap URN")),
                }
            }
            Rule::machine_header => {
                let mut parts = content.into_inner();
                let alias = text(&parts.next().expect("machine header has an alias"));
                parts.next(); // the `machine` keyword
                let string = parts.next().expect("machine header has a reference");
                // The grammar's `string` is JSON's string grammar, but for
                // what it cannot say: an escape that names half a character.
                let reference: String =
                    serde_json::from_str(string.as_str()).map_err(|error| MachineSyntaxError::ParseError {
                        details: format!("{} is not a JSON string: {error}", string.as_str().trim()),
                    })?;
                SyntaxStatementKind::MachineHeader {
                    alias,
                    reference: Spanned::of(&string, reference),
                }
            }
            Rule::wiring => SyntaxStatementKind::Wiring(wiring(content, position)?),
            other => unreachable!("a statement is a header or a wiring, not {other:?}"),
        };
        statements.push(SyntaxStatement {
            kind,
            position,
            start,
            end,
        });
    }
    Ok(statements)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn wirings(input: &str) -> Vec<SyntaxWiring> {
        parse_syntax(input)
            .unwrap_or_else(|e| panic!("{input:?} must parse: {e}"))
            .into_iter()
            .filter_map(|s| match s.kind {
                SyntaxStatementKind::Wiring(w) => Some(w),
                _ => None,
            })
            .collect()
    }

    fn one(input: &str) -> SyntaxWiring {
        let mut all = wirings(input);
        assert_eq!(all.len(), 1, "{input:?}");
        all.remove(0)
    }

    /// (name, node or constant as text) of every member.
    fn members(w: &SyntaxWiring) -> Vec<(Option<String>, String)> {
        w.sources
            .iter()
            .map(|m| {
                let value = match &m.value {
                    SyntaxValue::Node(n) => n.value.clone(),
                    SyntaxValue::Constant(c) => format!("={}", c.value),
                };
                (m.name.as_ref().map(|n| n.value.clone()), value)
            })
            .collect()
    }

    fn targets(w: &SyntaxWiring) -> Vec<(Option<String>, String)> {
        w.targets
            .iter()
            .map(|t| (t.name.as_ref().map(|n| n.value.clone()), t.node.value.clone()))
            .collect()
    }

    fn named(name: &str, value: &str) -> (Option<String>, String) {
        (Some(name.to_string()), value.to_string())
    }

    fn bare(value: &str) -> (Option<String>, String) {
        (None, value.to_string())
    }

    // TEST12732: every form of a wiring's sources and targets reads as the spec
    // writes it: a bare node, a group of nodes, named members, constants of
    // every JSON kind, an empty group on either side, and several targets with
    // at most one unnamed. Both statement forms read alike.
    #[test]
    fn test12732_the_forms_of_a_wiring() {
        let w = one("doc -> extract -> text");
        assert_eq!(members(&w), vec![bare("doc")]);
        assert_eq!(w.step.value, "extract");
        assert_eq!(targets(&w), vec![bare("text")]);
        assert!(w.policy.is_default());

        let w = one("[(thumbnail, model_spec) -> describe -> description]");
        assert_eq!(members(&w), vec![bare("thumbnail"), bare("model_spec")]);

        let w = one("(path, benchmark=bench) -> compare -> result");
        assert_eq!(members(&w), vec![bare("path"), named("benchmark", "bench")]);

        let w = one(r#"(prices, window=20, statistic="mean") -> rolling -> smooth"#);
        assert_eq!(
            members(&w),
            vec![bare("prices"), named("window", "=20"), named("statistic", "=\"mean\"")]
        );

        // A source fed only by constants, and one fed nothing at all.
        let w = one(r#"(reference="fred/UNRATE") -> fetch -> series"#);
        assert_eq!(members(&w), vec![named("reference", "=\"fred/UNRATE\"")]);
        let w = one("() -> now -> stamp");
        assert!(w.sources.is_empty());
        assert_eq!(targets(&w), vec![bare("stamp")]);

        // A sink, and several outputs.
        let w = one("report -> publish -> ()");
        assert!(w.targets.is_empty());
        let w = one("obs -> regress -> (fit, residuals=res)");
        assert_eq!(targets(&w), vec![bare("fit"), named("residuals", "res")]);
        let w = one("item -> route -> (yes=kept, no=dropped)");
        assert_eq!(targets(&w), vec![named("yes", "kept"), named("no", "dropped")]);

        // A group of one is a group.
        assert_eq!(members(&one("(x) -> f -> y")), vec![bare("x")]);
        assert_eq!(targets(&one("x -> f -> (y)")), vec![bare("y")]);

        // Every JSON kind is a constant, nested ones too, and a string may
        // hold what would end a statement.
        let w = one(r#"[(a=true, b=false, c=null, d=-1.5e3, e=[1, "two", {"k": [3]}], f={"x": {"y": null}}, g="a ] \" \n b") -> f -> y]"#);
        let constants: Vec<serde_json::Value> = w
            .sources
            .iter()
            .map(|m| match &m.value {
                SyntaxValue::Constant(c) => c.value.clone(),
                SyntaxValue::Node(n) => panic!("{} is a node", n.value),
            })
            .collect();
        assert_eq!(
            constants,
            vec![
                json!(true),
                json!(false),
                json!(null),
                json!(-1500.0),
                json!([1, "two", {"k": [3]}]),
                json!({"x": {"y": null}}),
                json!("a ] \" \n b"),
            ]
        );

        // A word that only starts like a constant is a node.
        assert_eq!(members(&one("(a=truely, b=nullable) -> f -> y")), vec![named("a", "truely"), named("b", "nullable")]);

        // Statements need no separator beyond their own shape.
        let all = wirings("a -> f -> b\n(b, k=1) -> g -> (c, d=e)\n[e -> h -> ()] [() -> i -> z]");
        assert_eq!(all.len(), 4);
        assert_eq!(all[1].step.value, "g");
        assert_eq!(all[3].step.value, "i");
    }

    // TEST12733: where each part stands in the text is recorded, so an editor
    // can point at exactly the name, the node or the constant it means.
    #[test]
    fn test12733_every_part_knows_its_place() {
        let input = r#"(prices, window=20) -> rolling{retry=2} -> (smooth, tail=rest)"#;
        let w = one(input);
        let at = |s: &Spanned<String>| &input[s.start..s.end];
        match &w.sources[0].value {
            SyntaxValue::Node(n) => assert_eq!(at(n), "prices"),
            other => panic!("{other:?}"),
        }
        assert_eq!(at(w.sources[1].name.as_ref().unwrap()), "window");
        match &w.sources[1].value {
            SyntaxValue::Constant(c) => assert_eq!(&input[c.start..c.end], "20"),
            other => panic!("{other:?}"),
        }
        assert_eq!(&input[w.sources_span.0..w.sources_span.1], "(prices, window=20)");
        assert_eq!(at(&w.step), "rolling");
        let policy_span = w.policy_span.expect("a policy was written");
        assert_eq!(&input[policy_span.0..policy_span.1], "{retry=2}");
        assert!(w.arrow_spans.iter().all(|(start, end)| &input[*start..*end] == "->"));
        assert!(w.arrow_spans[0].1 <= w.step.start && w.step.end <= w.arrow_spans[1].0);
        assert!(one("a -> f -> b").policy_span.is_none());
        assert_eq!(at(w.targets[1].name.as_ref().unwrap()), "tail");
        assert_eq!(at(&w.targets[1].node), "rest");
        assert_eq!(&input[w.targets_span.0..w.targets_span.1], "(smooth, tail=rest)");

        let statements = parse_syntax("[a -> f -> b]\nc -> g -> d").unwrap();
        assert_eq!((statements[0].position, statements[1].position), (0, 1));
        assert_eq!(&"[a -> f -> b]\nc -> g -> d"[statements[0].start..statements[0].end], "[a -> f -> b]");
        assert_eq!(&"[a -> f -> b]\nc -> g -> d"[statements[1].start..statements[1].end], "c -> g -> d");
    }

    // TEST12734: the two headers. A cap header reads its URN to the end of the
    // statement; a machine header names another machine by an opaque reference,
    // written as a string with JSON's escapes; and the word `machine` is only a
    // keyword where a header puts it.
    #[test]
    fn test12734_cap_headers_and_machine_headers() {
        let statements = parse_syntax(
            "extract cap:in=\"media:document;ext=pdf\";extract;out=\"media:enc=utf-8\"\n\
             clean machine \"lib/clean \\\"v2\\\".machine\"\n\
             [machine cap:in=media:;machine;out=media:]\n\
             machine -> machine -> machinery",
        )
        .unwrap();
        assert_eq!(statements.len(), 4);
        match &statements[0].kind {
            SyntaxStatementKind::CapHeader { alias, cap_urn } => {
                assert_eq!(alias.value, "extract");
                assert_eq!(cap_urn.value, "cap:in=\"media:document;ext=pdf\";extract;out=\"media:enc=utf-8\"");
            }
            other => panic!("{other:?}"),
        }
        match &statements[1].kind {
            SyntaxStatementKind::MachineHeader { alias, reference } => {
                assert_eq!(alias.value, "clean");
                assert_eq!(reference.value, "lib/clean \"v2\".machine", "the reference is unescaped");
            }
            other => panic!("{other:?}"),
        }
        // An alias may be the word `machine`, and so may a node and a step.
        match &statements[2].kind {
            SyntaxStatementKind::CapHeader { alias, .. } => assert_eq!(alias.value, "machine"),
            other => panic!("{other:?}"),
        }
        match &statements[3].kind {
            SyntaxStatementKind::Wiring(w) => {
                assert_eq!(members(w), vec![bare("machine")]);
                assert_eq!(w.step.value, "machine");
                assert_eq!(targets(w), vec![bare("machinery")]);
            }
            other => panic!("{other:?}"),
        }
    }

    // TEST12735: a step's policy. It is read from the braces after the step,
    // either key or both, in any order; and what it writes back is one form.
    #[test]
    fn test12735_a_step_policy() {
        let policy = |input: &str| one(input).policy;
        assert_eq!(policy("a -> f -> b"), StepPolicy::default());
        assert_eq!(policy("a -> f{} -> b"), StepPolicy::default());
        assert_eq!(policy("a -> f{retry=3} -> b"), StepPolicy { retry: 3, on_error: OnError::Fail });
        assert_eq!(policy("a -> f{on_error=absent} -> b"), StepPolicy { retry: 0, on_error: OnError::Absent });
        let both = StepPolicy { retry: 2, on_error: OnError::Absent };
        assert_eq!(policy("a -> f{retry=2, on_error=absent} -> b"), both);
        assert_eq!(policy("a -> f { on_error = absent , retry = 2 } -> b"), both);
        assert_eq!(one("a -> f{retry=2} -> b").step.value, "f", "the policy is not part of the step's name");

        assert_eq!(StepPolicy::default().to_notation(), "");
        assert_eq!(StepPolicy { retry: 3, on_error: OnError::Fail }.to_notation(), "{retry=3}");
        assert_eq!(StepPolicy { retry: 0, on_error: OnError::Absent }.to_notation(), "{on_error=absent}");
        assert_eq!(both.to_notation(), "{retry=2, on_error=absent}");
        // What is written back reads as what it was.
        for p in [StepPolicy::default(), both, StepPolicy { retry: 7, on_error: OnError::Fail }] {
            assert_eq!(policy(&format!("a -> f{} -> b", p.to_notation())), p);
        }
    }

    fn refused(input: &str) -> MachineSyntaxError {
        parse_syntax(input).expect_err(&format!("{input:?} must be refused"))
    }

    // TEST12736: what the text alone decides is refused by name, with the
    // statement it is in: a constant that names no arg, a node named as a
    // constant is, a second unnamed target, and a policy outside its table.
    #[test]
    fn test12736_what_the_text_alone_refuses() {
        for input in ["(prices, 20) -> rolling -> s", r#"("mean") -> rolling -> s"#, "(true) -> f -> y", "(a, [1]) -> f -> y"] {
            assert!(matches!(refused(input), MachineSyntaxError::UnnamedConstant { .. }), "{input}");
        }
        match refused("a -> f -> b\n(prices, 20) -> rolling -> s") {
            MachineSyntaxError::UnnamedConstant { constant, position } => {
                assert_eq!((constant.as_str(), position), ("20", 1));
            }
            other => panic!("{other:?}"),
        }

        for (input, name) in [
            ("true -> f -> y", "true"),
            ("x -> f -> null", "null"),
            ("x -> f -> (out=false)", "false"),
            ("x -> f -> (a, b=null)", "null"),
        ] {
            match refused(input) {
                MachineSyntaxError::ReservedNodeName { name: got, .. } => assert_eq!(got, name, "{input}"),
                other => panic!("{input}: {other:?}"),
            }
        }
        // As a member's value those words are constants, which is what they are.
        assert_eq!(members(&one("(a=null) -> f -> y")), vec![named("a", "=null")]);

        match refused("x -> f -> (a, b, c=d)") {
            MachineSyntaxError::SeveralUnnamedTargets { targets, .. } => assert_eq!(targets, vec!["a", "b"]),
            other => panic!("{other:?}"),
        }

        for (input, word) in [
            ("a -> f{retries=3} -> b", "retries"),
            ("a -> f{retry=-1} -> b", "-1"),
            ("a -> f{retry=1.5} -> b", "1.5"),
            ("a -> f{retry=many} -> b", "many"),
            ("a -> f{retry=4294967296} -> b", "4294967296"),
            ("a -> f{on_error=ignore} -> b", "ignore"),
            ("a -> f{on_error=3} -> b", "3"),
            ("a -> f{retry=1, retry=2} -> b", "twice"),
        ] {
            match refused(input) {
                MachineSyntaxError::InvalidPolicy { step, details, .. } => {
                    assert_eq!(step, "f", "{input}");
                    assert!(details.contains(word), "{input}: {details}");
                }
                other => panic!("{input}: {other:?}"),
            }
        }

        // Both halves of a character, written as their two escapes, are it;
        // and an escaped backslash before a `u` is no escape of a character.
        assert_eq!(
            members(&one(r#"(a="\ud83d\ude00") -> f -> b"#)),
            vec![(Some("a".to_string()), "=\"\u{1f600}\"".to_string())]
        );
        assert_eq!(
            members(&one(r#"(a="\\ud800") -> f -> b"#)),
            vec![(Some("a".to_string()), r#"="\\ud800""#.to_string())]
        );

        // The largest count a policy holds is read.
        assert_eq!(one("a -> f{retry=4294967295} -> b").policy.retry, 4294967295);

        // What the grammar does not accept at all.
        for input in [
            "a -> f", "a -> -> b", "(a,) -> f -> b", "a -> f -> (b", "(a b) -> f -> c", "x machine clean", "(a=) -> f -> b",
            // A string that is not a JSON string: an escape JSON does not
            // have, and a raw line break.
            r#"(a="\q") -> f -> b"#, "(a=\"x\ny\") -> f -> b", r#"x machine "bad \q ref""#,
            // A number no constant can hold.
            "(a=1e999) -> f -> b",
            // An escape that names half a character: text has no such thing.
            r#"(a="\ud800") -> f -> b"#, r#"(a="\udc00x") -> f -> b"#, r#"(a="\ud83d\ud83d") -> f -> b"#,
            r#"(a={"k\ud800": 1}) -> f -> b"#, r#"(a=["\udc00"]) -> f -> b"#, r#"(a={"k": "\ud800"}) -> f -> b"#,
            r#"x machine "\ud800""#,
        ] {
            assert!(matches!(refused(input), MachineSyntaxError::ParseError { .. }), "{input}");
        }
    }

    // TEST12832: a constant has one text, the same in every implementation:
    // an object's keys in order, a whole number without a point, a float with
    // one (or an exponent beyond sixteen digits and below five zeros), and
    // only the escapes JSON requires. A number is read as the nearest float,
    // however many digits it was written with. Two constants are equal as
    // JSON values: `1` and `1.0` are not.
    #[test]
    fn test12832_a_constant_has_one_text() {
        let constant = |written: &str| -> serde_json::Value {
            let wiring = one(&format!("(a={written}) -> f -> y"));
            match &wiring.sources[0].value {
                SyntaxValue::Constant(c) => c.value.clone(),
                other => panic!("{written}: {other:?}"),
            }
        };
        for (written, text) in [
            ("20", "20"),
            ("-7", "-7"),
            ("18446744073709551615", "18446744073709551615"),
            ("18446744073709551616", "1.8446744073709552e+19"),
            ("-9223372036854775809", "-9.223372036854776e+18"),
            ("1.0", "1.0"),
            ("1.50", "1.5"),
            ("-1.5e3", "-1500.0"),
            ("1e3", "1000.0"),
            ("0.0001", "0.0001"),
            ("0.00001", "0.00001"),
            ("0.000001", "1e-6"),
            ("1.25e-7", "1.25e-7"),
            ("1e15", "1000000000000000.0"),
            ("1e16", "1e+16"),
            ("1234567890123456.0", "1234567890123456.0"),
            // Seventeen digits: the nearest float, not the one below it.
            ("12345678901234567.0", "1.2345678901234568e+16"),
            ("1e21", "1e+21"),
            ("5e-324", "5e-324"),
            ("0.0", "0.0"),
            ("-0.0", "-0.0"),
            // A whole number has no sign to keep at zero: `-0` is the float.
            ("0", "0"),
            ("-0", "-0.0"),
            ("-9223372036854775808", "-9223372036854775808"),
            ("1E3", "1000.0"),
            ("2.50e0", "2.5"),
            (r#"{"b": 1, "a": [true, null]}"#, r#"{"a":[true,null],"b":1}"#),
            (r#""a<b>&c é""#, r#""a<b>&c é""#),
            (r#""tab\there \u0001 \" \\ \/""#, r#""tab\there \u0001 \" \\ /""#),
        ] {
            assert_eq!(constant(written).to_string(), text, "{written}");
            // The text reads back as the value it was written from.
            assert_eq!(constant(text), constant(written), "{written}");
        }
        assert_ne!(constant("1"), constant("1.0"));
        for (left, right) in [("true", "1"), ("false", "0"), ("true", "1.0"), ("null", "0"), ("null", "false")] {
            assert_ne!(constant(left), constant(right), "{left} and {right}");
        }
        assert_eq!(constant(r#"{"a":1,"b":[2,"x"]}"#), constant(r#"{"b":[2,"x"],"a":1}"#));
    }
}
