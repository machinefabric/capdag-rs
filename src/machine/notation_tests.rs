//! Tests of machine notation end to end through the parser and the resolver:
//! named members, constants, taken outputs, sources and sinks, step policy,
//! the URN of a node, and the canonical form
//! (`capdag/docs/09-machine-notation.md`).

use std::collections::HashMap;

use serde_json::json;

use crate::cap::definition::{ArgSource, Cap, CapArg, CapOutput};
use crate::cap::registry::FabricRegistry;
use crate::media::spec::MediaValidation;
use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::MediaUrn;
use crate::StoredMediaDef;

use super::error::{MachineAbstractionError, MachineParseError, MachineSyntaxError};
use super::graph::{BindingSource, Machine, MachineEdge};
use super::parser::{parse_machine, parse_machine_with_node_names};
use super::syntax::{OnError, StepPolicy};

pub(super) const SERIES: &str = "media:fmt=json;list;record;time-series";
const BENCHMARK: &str = "media:benchmark;fmt=json;list;record;time-series";
const WINDOW: &str = "media:integer;window";
const LABEL: &str = "media:enc=utf-8;label";
const TAG: &str = "media:enc=utf-8;tag";
pub(super) const COMPARISON: &str = "media:comparison;fmt=json;record";
pub(super) const RESIDUALS: &str = "media:fmt=json;list;record;residual";
pub(super) const NOTE: &str = "media:enc=utf-8;note";
const REFERENCE: &str = "media:enc=utf-8;series-ref";

pub(super) fn definition(urn: &str, title: &str, args: Vec<CapArg>, outputs: Vec<CapOutput>) -> Cap {
    let cap = Cap {
        urn: CapUrn::from_string(urn).unwrap_or_else(|e| panic!("{urn}: {e}")),
        version: 1,
        title: title.to_string(),
        cap_description: None,
        documentation: None,
        metadata: HashMap::new(),
        aliases: vec![title.to_string()],
        is_abstract: false,
        args,
        outputs,
        metadata_json: None,
        registered_by: None,
        supported_model_types: Vec::new(),
        default_model_spec: None,
    };
    crate::cap::validation::validate_cap_definition(&cap)
        .unwrap_or_else(|e| panic!("the fixture '{title}' is not a valid definition: {e}"));
    cap
}

fn flag(media_urn: &str, name: &str) -> CapArg {
    CapArg::new(
        media_urn, name,
        false,
        vec![ArgSource::CliFlag {
            cli_flag: format!("--{name}"),
        }],
    )
}

/// `compare`: a main input and a second arg that accepts the same type, two
/// constants' worth of options, a sequence arg, and three outputs.
fn compare_cap() -> Cap {
    let mut tags = flag(TAG, "tags");
    tags.is_sequence = true;
    let mut residuals = CapOutput::new(RESIDUALS, "residuals", "Residuals");
    residuals.is_sequence = true;
    definition(
        &format!(r#"cap:in="{SERIES}";compare;out="{COMPARISON}""#),
        "compare",
        vec![
            CapArg::new(SERIES, "series", true, vec![ArgSource::Stdin { stdin: SERIES.to_string() }]),
            flag(BENCHMARK, "benchmark").accepting(SERIES),
            flag(WINDOW, "window"),
            flag(LABEL, "label"),
            tags,
        ],
        vec![
            residuals,
            CapOutput::new(COMPARISON, "comparison", "The comparison"),
            CapOutput::new(NOTE, "note", "A note").optional(true),
        ],
    )
}

/// `fetch`: a source. Its `in=` is void; it is given a reference and nothing else.
fn fetch_cap() -> Cap {
    definition(
        &format!(r#"cap:in=media:void;fetch;out="{SERIES}""#),
        "fetch",
        vec![flag(REFERENCE, "reference")],
        vec![CapOutput::new(SERIES, "series", "The series")],
    )
}

/// `publish`: a sink. Its `out=` is void.
fn publish_cap() -> Cap {
    definition(
        &format!(r#"cap:in="{COMPARISON}";publish;out=media:void"#),
        "publish",
        vec![CapArg::new(COMPARISON, "report", true, vec![ArgSource::Stdin { stdin: COMPARISON.to_string() }])],
        vec![],
    )
}

/// `mean`: one value from a sequence of residuals — maps nothing.
fn mean_cap() -> Cap {
    let mut rows = CapArg::new(RESIDUALS, "rows", true, vec![ArgSource::Stdin { stdin: RESIDUALS.to_string() }]);
    rows.is_sequence = true;
    definition(
        &format!(r#"cap:in="{RESIDUALS}";mean;out="media:mean;numeric""#),
        "mean",
        vec![rows],
        vec![CapOutput::new("media:mean;numeric", "mean", "The mean")],
    )
}

/// `score`: one value per residual — a sequence of them maps it.
fn score_cap() -> Cap {
    definition(
        &format!(r#"cap:in="{RESIDUALS}";score;out="media:numeric;score""#),
        "score",
        vec![CapArg::new(RESIDUALS, "row", true, vec![ArgSource::Stdin { stdin: RESIDUALS.to_string() }])],
        vec![CapOutput::new("media:numeric;score", "score", "The score")],
    )
}

fn media_def(urn: &str, validation: Option<MediaValidation>, schema: Option<serde_json::Value>) -> StoredMediaDef {
    StoredMediaDef {
        version: 0,
        urn: urn.to_string(),
        media_type: "application/json".to_string(),
        title: urn.to_string(),
        profile_uri: None,
        schema,
        description: None,
        documentation: None,
        validation,
        metadata: None,
        extensions: Vec::new(),
    }
}

/// A registry with the caps above and the media definitions their constants
/// are held to: a window of 1 to 365, a label that is a string of three
/// allowed words, tags and references that are strings.
pub(super) fn registry() -> FabricRegistry {
    let registry = FabricRegistry::new_for_test();
    registry.add_caps_to_cache(vec![compare_cap(), fetch_cap(), publish_cap(), mean_cap(), score_cap()]);
    registry.insert_cached_media_def_for_test(media_def(
        WINDOW,
        Some(MediaValidation { min: Some(1.0), max: Some(365.0), ..Default::default() }),
        Some(json!({"type": "integer"})),
    ));
    registry.insert_cached_media_def_for_test(media_def(
        LABEL,
        Some(MediaValidation {
            allowed_values: Some(vec!["mean".to_string(), "median".to_string(), "max".to_string()]),
            ..Default::default()
        }),
        Some(json!({"type": "string"})),
    ));
    registry.insert_cached_media_def_for_test(media_def(TAG, None, Some(json!({"type": "string"}))));
    registry.insert_cached_media_def_for_test(media_def(REFERENCE, None, None));
    registry
}

pub(super) const HEADERS: &str = "\
[compare cap:in=\"media:fmt=json;list;record;time-series\";compare;out=\"media:comparison;fmt=json;record\"]\
[fetch cap:in=media:void;fetch;out=\"media:fmt=json;list;record;time-series\"]\
[publish cap:in=\"media:comparison;fmt=json;record\";publish;out=media:void]\
[mean cap:in=\"media:fmt=json;list;record;residual\";mean;out=\"media:mean;numeric\"]\
[score cap:in=\"media:fmt=json;list;record;residual\";score;out=\"media:numeric;score\"]";

fn parse(wirings: &str) -> Result<Machine, MachineParseError> {
    parse_machine(&format!("{HEADERS}{wirings}"), &registry())
}

fn machine(wirings: &str) -> Machine {
    parse(wirings).unwrap_or_else(|e| panic!("{wirings} must resolve: {e}"))
}

fn resolution_error(wirings: &str) -> MachineAbstractionError {
    match parse(wirings) {
        Err(MachineParseError::Resolution(e)) => e,
        Err(other) => panic!("{wirings}: expected a resolution error, got {other}"),
        Ok(_) => panic!("{wirings} must be refused"),
    }
}

/// The one edge of a one-step machine.
fn only_edge(machine: &Machine) -> &MachineEdge {
    assert_eq!(machine.strand_count(), 1);
    assert_eq!(machine.strands()[0].edges().len(), 1);
    &machine.strands()[0].edges()[0]
}

/// What each arg of an edge is bound to, by the arg's name: a node's URN, or a
/// constant.
fn bindings(machine: &Machine, edge: &MachineEdge) -> Vec<(String, String)> {
    let strand = &machine.strands()[0];
    edge.assignment
        .iter()
        .map(|b| {
            let source = match &b.source {
                BindingSource::Node(id) => format!("node {} {}", id, strand.node_urn(*id)),
                BindingSource::Constant(value) => format!("constant {value}"),
            };
            (b.arg_name.clone(), source)
        })
        .collect()
}

fn equivalent(a: &str, b: &str) -> bool {
    machine(a).is_equivalent(&machine(b))
}

// TEST12737: a member that names its arg binds that arg, whatever its type
// could also fit. Two inputs of one type are told apart only so: unnamed they
// are refused, and naming the other one is another machine. A name is the
// definition's label for the arg; the binding records the arg's media URN.
#[test]
fn test12737_a_named_member_binds_the_arg_it_names() {
    let named = machine("[(path, benchmark=bench) -> compare -> result]");
    let edge = only_edge(&named);
    let bound = bindings(&named, edge);
    assert_eq!(bound.len(), 2);
    let by_name: HashMap<&str, &str> = bound.iter().map(|(n, s)| (n.as_str(), s.as_str())).collect();
    // Both are inputs of the machine and both hold a series: the benchmark
    // arg ACCEPTS a series, and is identified by its own URN.
    assert!(by_name["series"].ends_with(SERIES) || by_name["series"].contains("time-series"), "{bound:?}");
    assert!(by_name["benchmark"].contains("time-series"), "{bound:?}");
    assert_ne!(by_name["series"].split(' ').nth(1), by_name["benchmark"].split(' ').nth(1), "two nodes");
    let slot = |name: &str| {
        edge.assignment.iter().find(|b| b.arg_name == name).map(|b| b.cap_arg_media_urn.clone()).unwrap()
    };
    assert!(slot("benchmark").is_equivalent(&MediaUrn::from_string(BENCHMARK).unwrap()).unwrap());
    assert!(slot("series").is_equivalent(&MediaUrn::from_string(SERIES).unwrap()).unwrap());

    // Which node is the benchmark is what the machine says: the other naming
    // binds the same two nodes the other way round. The two are inputs of one
    // type, so the machines are equivalent up to which input is which — and
    // a host addresses them by the names as written.
    let (_, names) = parse_machine_with_node_names(
        &format!("{HEADERS}[(path, benchmark=bench) -> compare -> result]"),
        &registry(),
    )
    .unwrap();
    let node_of = |binding_name: &str| {
        edge.assignment.iter().find(|b| b.arg_name == binding_name).and_then(|b| b.node())
    };
    assert_eq!(node_of("benchmark"), names[0].get("bench").copied());
    assert_eq!(node_of("series"), names[0].get("path").copied());

    // Unnamed, nothing says which of the two series is the benchmark.
    match resolution_error("[(path, bench) -> compare -> result]") {
        MachineAbstractionError::UntypedMachineInput { members, .. } => {
            assert_eq!(members, vec!["path", "bench"]);
        }
        other => panic!("{other}"),
    }
    // The same two, produced by steps: each has a type, and it fits two args.
    let produced = "[(reference=\"a\") -> fetch -> a][(reference=\"b\") -> fetch -> b]";
    assert!(matches!(
        resolution_error(&format!("{produced}[(a, b) -> compare -> result]")),
        MachineAbstractionError::AmbiguousMachineNotation { .. }
    ));
    let said = machine(&format!("{produced}[(a, benchmark=b) -> compare -> result]"));
    assert_eq!(said.strands()[0].edges().len(), 3);
    // An arg a member named is said: an unnamed member is not matched into it
    // as well. Two unnamed series beside a named benchmark have one arg left
    // between them, and that is what the refusal says — not that they tie.
    let third = "[(reference=\"c\") -> fetch -> c]";
    assert!(matches!(
        resolution_error(&format!("{produced}{third}[(a, c, benchmark=b) -> compare -> result]")),
        MachineAbstractionError::UnmatchedSourceInCapArgs { .. }
    ));

    // A name the cap does not declare, with the names it does.
    match resolution_error("[(path, baseline=bench) -> compare -> result]") {
        MachineAbstractionError::UnknownArgName { name, declared, .. } => {
            assert_eq!(name, "baseline");
            assert_eq!(declared, vec!["series", "benchmark", "window", "label", "tags"]);
        }
        other => panic!("{other}"),
    }
    // A name is not read out of a URN: `time-series` is a tag of the arg's
    // type, and no arg is called that.
    assert!(matches!(
        resolution_error("[(time-series=path) -> compare -> result]"),
        MachineAbstractionError::UnknownArgName { .. }
    ));
    // A produced node of another type, named into an arg that does not accept it.
    match resolution_error("[path -> compare -> r][(series=p2, benchmark=r) -> compare -> r2]") {
        MachineAbstractionError::SourceDoesNotConformToArg { name, accepts, source_urn, .. } => {
            assert_eq!(name, "benchmark");
            assert!(accepts.contains("time-series") && source_urn.contains("comparison"), "{accepts} / {source_urn}");
        }
        other => panic!("{other}"),
    }
    // An arg that takes one value, named twice.
    match resolution_error("[(path, benchmark=b1, benchmark=b2) -> compare -> result]") {
        MachineAbstractionError::ArgBoundTwice { name, count, .. } => assert_eq!((name.as_str(), count), ("benchmark", 2)),
        other => panic!("{other}"),
    }
}

// TEST12738: a constant is the arg's value, written in the machine. It is held
// to the validation of the arg's media definition when the machine is
// resolved, and a sequence arg gathers the constants named into it, in order.
#[test]
fn test12738_a_constant_is_bound_and_validated() {
    let m = machine(r#"[(path, window=20, label="mean", tags="a", tags="b") -> compare -> result]"#);
    let bound = bindings(&m, only_edge(&m));
    let of = |name: &str| -> Vec<&str> {
        bound.iter().filter(|(n, _)| n == name).map(|(_, s)| s.as_str()).collect()
    };
    assert_eq!(of("window"), vec!["constant 20"]);
    assert_eq!(of("label"), vec![r#"constant "mean""#]);
    assert_eq!(of("tags"), vec![r#"constant "a""#, r#"constant "b""#], "gathered in the order written");
    // A constant is no node: the machine has one input and one output.
    assert_eq!(m.strands()[0].nodes().len(), 2);
    assert_eq!(m.strands()[0].input_anchor_ids().len(), 1);

    let refused = |wiring: &str| match resolution_error(wiring) {
        MachineAbstractionError::ConstantFailsValidation { name, constant, reason, .. } => (name, constant, reason),
        other => panic!("{wiring}: {other}"),
    };
    // Outside the range the media definition gives.
    let (name, constant, reason) = refused("[(path, window=0) -> compare -> result]");
    assert_eq!((name.as_str(), constant.as_str()), ("window", "0"));
    assert!(reason.contains("minimum value 1"), "{reason}");
    assert!(refused("[(path, window=366) -> compare -> result]").2.contains("maximum value 365"));
    // Inside the range and not the kind of value the schema asks for.
    assert!(refused("[(path, window=2.5) -> compare -> result]").2.contains("schema"));
    assert!(refused(r#"[(path, window="20") -> compare -> result]"#).2.contains("schema"));
    // A word the definition does not allow.
    assert!(refused(r#"[(path, label="mode") -> compare -> result]"#).2.contains("allowed values"));
    // An arg whose media definition is not at hand cannot be given a constant
    // nobody could check.
    let (name, _, reason) = refused(r#"[(path, benchmark=[1, 2]) -> compare -> result]"#);
    assert_eq!(name, "benchmark");
    assert!(reason.contains("not available"), "{reason}");

    // The bounds themselves are allowed.
    machine("[(path, window=1) -> compare -> result]");
    machine("[(path, window=365) -> compare -> result]");

    // A constant is not a node. One input feeding two args, and the same input
    // feeding one of them beside a constant for the other, have the same nodes
    // and are different machines.
    let with_benchmark_def = registry();
    with_benchmark_def.insert_cached_media_def_for_test(media_def(BENCHMARK, None, None));
    let in_registry = |wirings: &str| {
        parse_machine(&format!("{HEADERS}{wirings}"), &with_benchmark_def)
            .unwrap_or_else(|e| panic!("{wirings}: {e}"))
    };
    let twice = in_registry("[(series=p, benchmark=p) -> compare -> r]");
    let once = in_registry("[(series=p, benchmark=[1, 2]) -> compare -> r]");
    assert_eq!(twice.strands()[0].nodes().len(), once.strands()[0].nodes().len());
    assert!(!twice.is_equivalent(&once));
    assert!(!once.is_equivalent(&twice));

    // Two machines that give an arg different constants are different machines.
    assert!(equivalent("[(path, window=20) -> compare -> r]", "[(p, window=20) -> compare -> out]"));
    assert!(!equivalent("[(path, window=20) -> compare -> r]", "[(path, window=21) -> compare -> r]"));
    assert!(!equivalent("[(path, window=20) -> compare -> r]", "[path -> compare -> r]"));
}

// TEST12739: the outputs a wiring takes. An unnamed target takes the step's
// main output, a named one the output of that name; one that is not taken is
// discarded; a sink takes none and a source is given nothing.
#[test]
fn test12739_outputs_sources_and_sinks() {
    let both = machine("[path -> compare -> (result, residuals=res)]");
    let edge = only_edge(&both);
    let taken: Vec<(&str, bool)> = edge
        .outputs
        .iter()
        .map(|o| (o.output_name.as_str(), both.strands()[0].node_is_sequence(o.node)))
        .collect();
    // Sorted by output URN, each node holding what its output is declared to be.
    assert_eq!(taken.len(), 2);
    assert!(taken.contains(&("comparison", false)) && taken.contains(&("residuals", true)), "{taken:?}");
    let main = edge.main_target().expect("the main output is taken");
    assert!(both.strands()[0].node_urn(main).is_equivalent(&MediaUrn::from_string(COMPARISON).unwrap()).unwrap());
    assert_eq!(both.strands()[0].output_anchor_ids().len(), 2);

    // The main output need not be taken, and the one that is, is still named.
    let residuals_only = machine("[path -> compare -> (residuals=res)]");
    assert!(only_edge(&residuals_only).main_target().is_none());
    assert_eq!(only_edge(&residuals_only).outputs.len(), 1);
    assert!(!residuals_only.is_equivalent(&both));
    // Which outputs are taken is not their order in the text.
    assert!(equivalent("[path -> compare -> (result, residuals=res)]", "[p -> compare -> (residuals=r, out)]"));

    match resolution_error("[path -> compare -> (result, errors=e)]") {
        MachineAbstractionError::UnknownOutputName { name, declared, .. } => {
            assert_eq!(name, "errors");
            assert_eq!(declared, vec!["residuals", "comparison", "note"]);
        }
        other => panic!("{other}"),
    }
    assert!(matches!(
        resolution_error("[path -> compare -> (residuals=a, residuals=b)]"),
        MachineAbstractionError::OutputTakenTwice { .. }
    ));
    // The main output taken unnamed and by its name is the same output twice.
    assert!(matches!(
        resolution_error("[path -> compare -> (a, comparison=b)]"),
        MachineAbstractionError::OutputTakenTwice { .. }
    ));

    // A sink: nothing is taken, and its strand has no output.
    let sink = machine("[report -> publish -> ()]");
    assert!(only_edge(&sink).outputs.is_empty());
    assert!(sink.strands()[0].output_anchor_ids().is_empty());
    assert_eq!(sink.strands()[0].input_anchor_ids().len(), 1);
    // A sink has no main output to take.
    assert!(matches!(
        resolution_error("[report -> publish -> done]"),
        MachineAbstractionError::NoMainOutput { .. }
    ));

    // A source: given nothing, or only what its constants say.
    let source = machine("[() -> fetch -> s]");
    assert!(only_edge(&source).assignment.is_empty());
    assert!(source.strands()[0].input_anchor_ids().is_empty());
    let referenced = machine(r#"[(reference="fred/UNRATE") -> fetch -> s]"#);
    assert_eq!(bindings(&referenced, only_edge(&referenced)), vec![("reference".to_string(), r#"constant "fred/UNRATE""#.to_string())]);
    assert!(!referenced.is_equivalent(&source));
    // A source cannot be given an input, and a cap that takes one is given it.
    assert!(matches!(
        resolution_error("[x -> fetch -> s]"),
        MachineAbstractionError::UntypedMachineInput { .. }
    ));
    match resolution_error("[() -> compare -> r]") {
        MachineAbstractionError::MainInputUnbound { main_input, .. } => assert_eq!(main_input, "series"),
        other => panic!("{other}"),
    }
    assert!(matches!(
        resolution_error("[(benchmark=b) -> compare -> r]"),
        MachineAbstractionError::MainInputUnbound { .. }
    ));

    // What a sequence output feeds: a step that takes the sequence runs once,
    // one that takes a single value runs once per item.
    let folded = machine("[path -> compare -> (residuals=res)][res -> mean -> m]");
    let mapped = machine("[path -> compare -> (residuals=res)][res -> score -> s]");
    let step = |m: &Machine, cap: &str| {
        m.strands()[0].edges().iter().find(|e| e.cap_urn.to_string().contains(cap)).unwrap().is_loop
    };
    assert!(!step(&folded, "mean"));
    assert!(step(&mapped, "score"));
    assert!(!step(&mapped, "compare"));
}

// TEST12740: the media URN of a node comes from the wirings that touch it and
// never from where a member stands or which wiring comes first.
#[test]
fn test12740_the_urn_of_a_node_is_position_free() {
    // Members in either order, wirings in either order: one machine.
    let a = "[(path, benchmark=bench, window=20) -> compare -> (r, residuals=res)][res -> score -> s]";
    let b = "[res -> score -> s][(window=20, benchmark=bench, path) -> compare -> (residuals=res, r)]";
    assert!(equivalent(a, b));
    // And one text: the canonical notation names its nodes by what they are
    // for, not by which was written first.
    assert_eq!(
        machine(a).to_machine_notation().unwrap(),
        machine(b).to_machine_notation().unwrap()
    );
    // The two inputs are not interchangeable: swapping which is the benchmark
    // is the same shape with the roles exchanged, and a machine that names a
    // DIFFERENT pairing of its nodes to a second step is another machine.
    assert!(!equivalent(
        "[(path, benchmark=bench) -> compare -> (residuals=r)][(path, benchmark=bench) -> compare -> (note=n)]",
        "[(path, benchmark=bench) -> compare -> (residuals=r)][(bench, benchmark=path) -> compare -> (note=n)]",
    ));

    // An input of the machine named into an arg holds what that arg accepts,
    // and the unnamed one beside it holds what the main input accepts.
    let (m, names) = parse_machine_with_node_names(
        &format!("{HEADERS}[(benchmark=bench, path) -> compare -> r]"),
        &registry(),
    )
    .unwrap();
    let urn_of = |name: &str| m.strands()[0].node_urn(names[0][name]).clone();
    let series = MediaUrn::from_string(SERIES).unwrap();
    assert!(urn_of("bench").is_equivalent(&series).unwrap());
    assert!(urn_of("path").is_equivalent(&series).unwrap());

    // Naming the main input is allowed; then an unnamed input beside it has
    // no arg anything decides.
    machine("[(series=path, benchmark=bench) -> compare -> r]");
    assert!(matches!(
        resolution_error("[(series=path, bench) -> compare -> r]"),
        MachineAbstractionError::UntypedMachineInput { .. }
    ));

    // An input of the machine given two types that are not on one chain: it
    // is named into an arg that accepts a series and into one that accepts a
    // comparison.
    match resolution_error("[(series=x) -> compare -> r][(report=x) -> publish -> ()]") {
        MachineAbstractionError::NodeMediaConflict { node, .. } => assert_eq!(node, "x"),
        other => panic!("{other}"),
    }
    // A node a step produces has that output's type, and a name does not
    // change it: named into an arg that does not accept it, it is refused there.
    assert!(matches!(
        resolution_error("[path -> compare -> r][(series=r) -> compare -> r2]"),
        MachineAbstractionError::SourceDoesNotConformToArg { .. }
    ));
}

// TEST12741: the canonical notation. Every binding and every taken output is
// written under its name, so nothing in it is matched by type; a machine
// written out and read back is the same machine, and the text does not change
// on a second pass. A policy is part of what a machine is.
#[test]
fn test12741_canonical_notation_round_trips() {
    for wirings in [
        "[path -> compare -> result]",
        "[(path, benchmark=bench) -> compare -> result]",
        r#"[(path, window=20, label="max", tags="b", tags="a") -> compare -> (result, residuals=res, note=n)]"#,
        r#"[(reference="fred/UNRATE") -> fetch{retry=3} -> s][s -> compare{retry=1, on_error=absent} -> (residuals=res)][res -> score -> x]"#,
        "[() -> fetch -> s]",
        "[report -> publish{on_error=absent} -> ()]",
    ] {
        let first = machine(wirings);
        let text = first.to_machine_notation().unwrap();
        let second = Machine::from_string(&text, &registry())
            .unwrap_or_else(|e| panic!("canonical text must read back: {e}\n{text}"));
        assert!(first.is_equivalent(&second), "{wirings}\n{text}");
        assert_eq!(second.to_machine_notation().unwrap(), text, "a fixed point");
        let line_based = first.to_machine_notation_formatted(super::serializer::NotationFormat::LineBased).unwrap();
        assert!(first.is_equivalent(&Machine::from_string(&line_based, &registry()).unwrap()), "{line_based}");
    }

    let text = machine(r#"[(path, benchmark=bench, window=20) -> compare{retry=2} -> (result, residuals=res)]"#)
        .to_machine_notation()
        .unwrap();
    // Named throughout, and the policy on the step.
    for part in ["series=n", "benchmark=n", "window=20", "comparison=n", "residuals=n", "edge_0{retry=2}"] {
        assert!(text.contains(part), "{part} is missing from {text}");
    }
    // A gather keeps the order its members were written in.
    let gathered = machine(r#"[(path, tags="b", tags="a") -> compare -> r]"#).to_machine_notation().unwrap();
    assert!(gathered.contains(r#"tags="b", tags="a""#), "{gathered}");

    // The policy is read onto the edge, and two machines that differ in it differ.
    let policy = |wirings: &str| only_edge(&machine(wirings)).policy;
    assert_eq!(policy("[p -> compare -> r]"), StepPolicy::default());
    assert_eq!(policy("[p -> compare{retry=2, on_error=absent} -> r]"), StepPolicy { retry: 2, on_error: OnError::Absent });
    assert!(!equivalent("[p -> compare{retry=2} -> r]", "[p -> compare -> r]"));
    assert!(!equivalent("[p -> compare{retry=2} -> r]", "[p -> compare{retry=3} -> r]"));
    assert!(equivalent("[p -> compare{retry=2} -> r]", "[q -> compare { retry = 2 } -> s]"));
}

// TEST12742: a machine header names another machine. Read with nothing to
// resolve the reference against, it is refused by name rather than ignored.
#[test]
fn test12742_a_machine_reference_needs_a_source() {
    match parse("[clean machine \"lib/clean.machine\"][path -> compare -> r]") {
        Err(MachineParseError::Syntax(MachineSyntaxError::MachineReferenceWithoutSource { alias, reference })) => {
            assert_eq!((alias.as_str(), reference.as_str()), ("clean", "lib/clean.machine"));
        }
        Err(other) => panic!("{other}"),
        Ok(_) => panic!("a machine reference cannot be resolved without a source"),
    }
}

// TEST12743: the render payload carries what the graph view needs of the new
// forms: each binding's arg name and its node or its constant, each taken
// output with its name, and the step's policy.
#[test]
fn test12743_render_payload_carries_names_constants_and_outputs() {
    let registry = registry();
    for urn in [SERIES, COMPARISON, RESIDUALS] {
        if registry.get_cached_media_def(urn).is_none() {
            registry.insert_cached_media_def_for_test(media_def(urn, None, None));
        }
    }
    let m = parse_machine(
        &format!(r#"{HEADERS}[(path, window=20) -> compare{{retry=2, on_error=absent}} -> (result, residuals=res)]"#),
        &registry,
    )
    .unwrap();
    let payload: serde_json::Value =
        serde_json::from_str(&m.to_render_payload_json(&registry).unwrap()).expect("the payload is JSON");
    let edge = &payload["strands"][0]["edges"][0];
    assert_eq!(edge["retry"], json!(2));
    assert_eq!(edge["on_error"], json!("absent"));
    let assignment = edge["assignment"].as_array().unwrap();
    let window = assignment.iter().find(|b| b["arg_name"] == "window").unwrap();
    assert_eq!(window["constant"], json!(20));
    assert!(window.get("source_node").is_none());
    let series = assignment.iter().find(|b| b["arg_name"] == "series").unwrap();
    assert!(series["source_node"].is_string() && series.get("constant").is_none());
    let outputs = edge["outputs"].as_array().unwrap();
    let names: Vec<&str> = outputs.iter().map(|o| o["output_name"].as_str().unwrap()).collect();
    assert_eq!(outputs.len(), 2);
    assert!(names.contains(&"comparison") && names.contains(&"residuals"), "{names:?}");
    assert!(outputs.iter().all(|o| o["node"].is_string()));
    assert!(edge.get("target_node").is_none(), "one target is not the shape any more");
    assert_eq!(edge["included_as"], json!([]), "a step the machine wrote itself");
}
