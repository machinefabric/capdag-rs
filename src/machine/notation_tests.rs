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
        dims: Vec::new(),
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
        dims: Vec::new(),
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
/// allowed words, tags that are strings, and references of at most twelve
/// bytes.
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
    registry.insert_cached_media_def_for_test(media_def(
        REFERENCE,
        Some(MediaValidation { max_length: Some(12), ..Default::default() }),
        None,
    ));
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
    // Two inputs that hold one type come in the order their nodes were made.
    assert_eq!(named.strands()[0].input_anchor_ids(), &[names[0]["path"], names[0]["bench"]]);

    // Unnamed, nothing says which of the two series is the benchmark.
    match resolution_error("[(path, bench) -> compare -> result]") {
        MachineAbstractionError::UntypedMachineInput { members, .. } => {
            assert_eq!(members, vec!["path", "bench"]);
        }
        other => panic!("{other}"),
    }
    // The same two, produced by steps: each has a type, and it fits two args.
    let produced = "[(reference=\"a\") -> fetch -> a][(reference=\"b\") -> fetch -> b]";
    // The refusal says which members to name, by the names the text gave
    // them, and the args each could be for.
    match resolution_error(&format!("{produced}[(a, b) -> compare -> result]")) {
        MachineAbstractionError::AmbiguousMachineNotation { members, .. } => {
            let named: Vec<(Option<&str>, Vec<&str>)> = members
                .iter()
                .map(|m| (m.node.as_deref(), m.could_be.iter().map(String::as_str).collect()))
                .collect();
            assert_eq!(
                named,
                vec![(Some("a"), vec!["series", "benchmark"]), (Some("b"), vec!["series", "benchmark"])]
            );
            assert!(members.iter().all(|m| m.source_urn == MediaUrn::from_string(SERIES).unwrap().to_string()));
        }
        other => panic!("{other}"),
    }
    let said = resolution_error(&format!("{produced}[(a, b) -> compare -> result]")).to_string();
    assert!(said.contains("name them: 'a' (") && said.contains("could be series or benchmark; 'b' ("), "{said}");
    // A member is named by the name the text gave it, wherever it stands in
    // its group and whichever node was written first.
    match resolution_error(&format!("{produced}[(b, a) -> compare -> result]")) {
        MachineAbstractionError::AmbiguousMachineNotation { members, .. } => {
            let named: Vec<Option<&str>> = members.iter().map(|m| m.node.as_deref()).collect();
            assert_eq!(named, vec![Some("b"), Some("a")]);
        }
        other => panic!("{other}"),
    }
    // The args a member could be for are the ones no name took: with the
    // first of three bound by name, the other two are what is left to say.
    let mix_urn = format!(r#"cap:in="{SERIES}";mix;out="{COMPARISON}""#);
    let with_mix = registry();
    with_mix.add_caps_to_cache(vec![definition(
        &mix_urn,
        "mix",
        vec![
            CapArg::new(SERIES, "first", true, vec![ArgSource::Stdin { stdin: SERIES.to_string() }]),
            flag("media:second;fmt=json;list;record;time-series", "second").accepting(SERIES),
            flag("media:third;fmt=json;list;record;time-series", "third").accepting(SERIES),
        ],
        vec![CapOutput::new(COMPARISON, "mixed", "The mix")],
    )]);
    let third = "[(reference=\"c\") -> fetch -> c]";
    match parse_machine(
        &format!("{HEADERS}[mix {mix_urn}]{produced}{third}[(first=c, a, b) -> mix -> result]"),
        &with_mix,
    ) {
        Err(MachineParseError::Resolution(MachineAbstractionError::AmbiguousMachineNotation { members, .. })) => {
            let named: Vec<(Option<&str>, Vec<&str>)> = members
                .iter()
                .map(|m| (m.node.as_deref(), m.could_be.iter().map(String::as_str).collect()))
                .collect();
            assert_eq!(
                named,
                vec![(Some("a"), vec!["second", "third"]), (Some("b"), vec!["second", "third"])]
            );
        }
        other => panic!("{:?}", other.map(|_| ())),
    }
    let said = machine(&format!("{produced}[(a, benchmark=b) -> compare -> result]"));
    assert_eq!(said.strands()[0].edges().len(), 3);
    // An arg a member named is said: an unnamed member is not matched into it
    // as well. Two unnamed series beside a named benchmark have one arg left
    // between them, and that is what the refusal says — not that they tie.
    assert!(matches!(
        resolution_error(&format!("{produced}{third}[(a, c, benchmark=b) -> compare -> result]")),
        MachineAbstractionError::UnmatchedSourceInCapArgs { .. }
    ));

    // A member that fits no arg at all is the one the refusal names, wherever
    // it stands: here the second, a comparison where a series is taken.
    match resolution_error(&format!("{produced}[a -> compare -> made][(b, made) -> compare -> result]")) {
        MachineAbstractionError::UnmatchedSourceInCapArgs { source_urn, .. } => {
            assert_eq!(source_urn, MediaUrn::from_string(COMPARISON).unwrap().to_string());
        }
        other => panic!("{other}"),
    }

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

    // A string's length is its UTF-8 bytes: six two-byte characters fill a
    // reference of twelve, and a seventh breaks it.
    machine(r#"[(reference="éééééé") -> fetch -> s]"#);
    let (_, _, reason) = refused(r#"[(reference="ééééééé") -> fetch -> s]"#);
    assert!(reason.contains("maximum length 12"), "{reason}");

    // A schema's required properties are held: a benchmark written as an
    // object says its points.
    let with_benchmark_schema = registry();
    with_benchmark_schema.insert_cached_media_def_for_test(media_def(
        BENCHMARK,
        None,
        Some(json!({"type": "object", "required": ["points"], "properties": {"points": {"type": "array"}}})),
    ));
    parse_machine(&format!(r#"{HEADERS}[(path, benchmark={{"points": [1, 2]}}) -> compare -> r]"#), &with_benchmark_schema)
        .unwrap_or_else(|e| panic!("a benchmark that says its points: {e}"));
    match parse_machine(&format!(r#"{HEADERS}[(path, benchmark={{"scale": 2}}) -> compare -> r]"#), &with_benchmark_schema) {
        Err(MachineParseError::Resolution(MachineAbstractionError::ConstantFailsValidation { reason, .. })) => {
            assert!(reason.contains("schema"), "{reason}")
        }
        other => panic!("a benchmark that does not say its points must be refused, got {other:?}"),
    }

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
    // Whether a step maps is part of what the machine is: the same text, read
    // where the residuals are one value, is another machine.
    let one_value = registry();
    let mut single = compare_cap();
    single.outputs.iter_mut().find(|output| output.name == "residuals").unwrap().is_sequence = false;
    one_value.add_caps_to_cache(vec![single]);
    let unmapped = parse_machine(
        &format!("{HEADERS}[path -> compare -> (residuals=res)][res -> score -> s]"),
        &one_value,
    )
    .unwrap();
    assert!(!step(&unmapped, "score"));
    assert!(!unmapped.is_equivalent(&mapped));
    assert!(!mapped.is_equivalent(&unmapped));
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
    assert!(!equivalent("[p -> compare{on_error=absent} -> r]", "[p -> compare -> r]"));
    assert!(equivalent("[p -> compare{retry=2} -> r]", "[q -> compare { retry = 2 } -> s]"));

    // Two machines of the same nodes, steps and anchors, whose steps take
    // their outputs differently: which step makes the residuals is part of
    // what each is.
    let first = "[p -> compare -> r1][(p, window=20) -> compare -> (r2, residuals=x)]";
    let second = "[p -> compare -> (r1, residuals=x)][(p, window=20) -> compare -> r2]";
    assert!(!equivalent(first, second));
    assert!(!equivalent(second, first));

    // A machine is not the machine it is the first steps of.
    let one = "[p -> compare -> (residuals=res)]";
    let two = "[p -> compare -> (residuals=res)][res -> score -> s]";
    assert!(!equivalent(one, two));
    assert!(!equivalent(two, one));

    // Two steps of one cap that differ only in what feeds an arg have one
    // order in the canonical text, whichever was written first: the one fed
    // by a node, then the one fed by a constant.
    let with_benchmark_def = registry();
    with_benchmark_def.insert_cached_media_def_for_test(media_def(BENCHMARK, None, None));
    let by_node = "[(series=p, benchmark=q) -> compare -> (note=a)]";
    let by_constant = "[(series=p, benchmark=[1, 2]) -> compare -> (note=b)]";
    let text_of = |wirings: String| {
        parse_machine(&format!("{HEADERS}{wirings}"), &with_benchmark_def)
            .unwrap_or_else(|e| panic!("{wirings}: {e}"))
            .to_machine_notation()
            .unwrap()
    };
    let text = text_of(format!("{by_node}{by_constant}"));
    assert_eq!(text, text_of(format!("{by_constant}{by_node}")));
    assert!(text.find("benchmark=n").unwrap() < text.find("benchmark=[1,2]").unwrap(), "{text}");
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

// TEST12833: an output that carries the input holds the input's type, not the
// URN that names it: a router's branches are typed by what was routed, so a
// step that takes that type can be wired to one. Which branch a wiring takes
// is part of what the machine is, although both hold one type.
#[test]
fn test12833_a_carried_output_holds_the_inputs_type() {
    const VERDICT: &str = "media:enc=utf-8;verdict";
    const KEPT: &str = "media:kept;marker";
    const DROPPED: &str = "media:dropped;marker";
    let route_urn = format!(r#"cap:in="{SERIES}";route;out="{VERDICT}""#);
    let registry = registry();
    registry.add_caps_to_cache(vec![definition(
        &route_urn,
        "route",
        vec![CapArg::new(SERIES, "series", true, vec![ArgSource::Stdin { stdin: SERIES.to_string() }])],
        vec![
            CapOutput::new(VERDICT, "verdict", "The verdict"),
            CapOutput::new(KEPT, "kept", "What was kept").optional(true).carrying_input(),
            CapOutput::new(DROPPED, "dropped", "What was dropped").optional(true).carrying_input(),
        ],
    )]);
    let headers = format!("{HEADERS}[route {route_urn}]");
    let parse = |wirings: &str| {
        parse_machine_with_node_names(&format!("{headers}{wirings}"), &registry)
            .unwrap_or_else(|e| panic!("{wirings}: {e}"))
    };
    let urn = |text: &str| MediaUrn::from_string(text).unwrap().to_string();

    let (m, names) = parse("[s -> route -> (v, kept=k, dropped=d)][k -> compare -> r]");
    let held = |name: &str| m.strands()[0].node_urn(names[0][name]).to_string();
    assert_eq!(held("k"), urn(SERIES), "the branch holds what was routed");
    assert_eq!(held("d"), urn(SERIES));
    assert_eq!(held("v"), urn(VERDICT), "the main output holds its own type");
    // The edge still records which output each is, by the output's own URN.
    let route = m.strands()[0]
        .edges()
        .iter()
        .find(|edge| {
            let cap = edge.cap_urn.to_string();
            cap.trim_start_matches("cap:").split(';').any(|tag| tag == "route")
        })
        .expect("the route step");
    let mut taken: Vec<(String, String)> = route
        .outputs
        .iter()
        .map(|o| (o.output_name.clone(), o.output_media_urn.to_string()))
        .collect();
    taken.sort();
    assert_eq!(
        taken,
        vec![
            ("dropped".to_string(), urn(DROPPED)),
            ("kept".to_string(), urn(KEPT)),
            ("verdict".to_string(), urn(VERDICT)),
        ]
    );

    // An output that carries nothing holds the URN it declares.
    let (plain, plain_names) = parse("[p -> compare -> (note=n)]");
    assert_eq!(plain.strands()[0].node_urn(plain_names[0]["n"]).to_string(), urn(NOTE));

    // The two branches hold one type and are two outputs.
    let kept_only = parse("[s -> route -> (kept=x)]").0;
    let kept_again = parse("[t -> route -> (kept=y)]").0;
    let dropped_only = parse("[s -> route -> (dropped=x)]").0;
    assert!(kept_only.is_equivalent(&kept_again));
    assert!(!kept_only.is_equivalent(&dropped_only));
    assert!(!dropped_only.is_equivalent(&kept_only));
}

// TEST12838: what a machine sorts by URN, it sorts by the URN's parts — the
// prefix, then the tags by key and value; for a cap its input, its output, its
// effect and then its tags — and never by the URN's text. The two orders
// differ wherever one tag key is the beginning of another (`file`,
// `file-path`), and the canonical form is the same in every implementation
// only because all of them use the first.
#[test]
fn test12838_urns_are_ordered_by_their_parts_not_their_text() {
    let media = |text: &str| MediaUrn::from_string(text).unwrap_or_else(|e| panic!("{text}: {e}"));
    let stands_before = |first: &str, second: &str| media(first) < media(second);
    let (sample, path) = ("media:file;sample", "media:file-path");
    assert!(stands_before(sample, path));
    assert!(!stands_before(path, sample));
    assert!(media(path).to_string() < media(sample).to_string(), "the text orders them the other way");
    // A URN that is the beginning of another stands first; a tag with any
    // value before the same tag with one; values in their own order.
    assert!(stands_before("media:file", "media:file;sample"));
    assert!(stands_before("media:enc", "media:enc=ascii"));
    assert!(stands_before("media:enc=ascii", "media:enc=utf-8"));
    assert!(media("media:sample;file") == media(sample));

    let cap = |text: &str| CapUrn::from_string(text).unwrap_or_else(|e| panic!("{text}: {e}"));
    let cap_before = |first: &str, second: &str| cap(first) < cap(second);
    // A cap's input decides before anything else it says.
    assert!(cap_before(
        &format!(r#"cap:in="{sample}";zz;out="media:x""#),
        &format!(r#"cap:in="{path}";aa;out="media:x""#)
    ));
    // Then its output — and only then: an input that stands first wins over
    // an output that stands last.
    assert!(cap_before(
        &format!(r#"cap:in="media:x";zz;out="{sample}""#),
        &format!(r#"cap:in="media:x";aa;out="{path}""#)
    ));
    assert!(cap_before(
        &format!(r#"cap:in="{sample}";op;out="{path}""#),
        &format!(r#"cap:in="{path}";op;out="{sample}""#)
    ));
    // Then its effect, in the order: declared, none, patch, unspecified.
    let effects = [
        r#"cap:in="media:x";op;out="media:x""#,
        r#"cap:effect=none;in="media:x";op;out="media:x""#,
        r#"cap:effect=patch;in="media:x";op;out="media:x""#,
        r#"cap:effect=?;in="media:x";op;out="media:x""#,
    ];
    for pair in effects.windows(2) {
        assert!(cap_before(pair[0], pair[1]), "{} before {}", pair[0], pair[1]);
        assert!(!cap_before(pair[1], pair[0]));
    }
    // Then its other tags — after the effect, whatever they say.
    assert!(cap_before(r#"cap:in="media:x";aa;out="media:x""#, r#"cap:in="media:x";zz;out="media:x""#));
    assert!(cap_before(
        r#"cap:in="media:x";zz;out="media:x""#,
        r#"cap:effect=patch;in="media:x";aa;out="media:x""#
    ));

    // In a machine: the bindings of a step, the outputs it takes, the inputs
    // of the strand and the steps themselves all stand in that order.
    let (result, whereabouts) = ("media:file;result", "media:file-path;written");
    let pair_urn = format!(r#"cap:in="{sample}";pair;out="{result}""#);
    let first_urn = format!(r#"cap:in="{sample}";zz-first;out="media:file;a""#);
    let second_urn = format!(r#"cap:in="{sample}";aa-second;out="{path}""#);
    let stdin = |media_urn: &str, name: &str| {
        CapArg::new(media_urn, name, true, vec![ArgSource::Stdin { stdin: media_urn.to_string() }])
    };
    let output = |media_urn: &str, name: &str| CapOutput::new(media_urn, name, String::new());
    let registry = FabricRegistry::new_for_test();
    registry.add_caps_to_cache(vec![
        definition(
            &pair_urn,
            "pair",
            vec![stdin(sample, "sample"), flag(path, "path")],
            vec![output(result, "result"), output(whereabouts, "where")],
        ),
        definition(&first_urn, "first", vec![stdin(sample, "sample")], vec![output("media:file;a", "made")]),
        definition(&second_urn, "second", vec![stdin(sample, "sample")], vec![output(path, "made")]),
    ]);

    let (machine, names) = parse_machine_with_node_names(
        &format!("[pair {pair_urn}][(path=p, s) -> pair -> (where=w, r)]"),
        &registry,
    )
    .unwrap();
    let edge = only_edge(&machine);
    assert_eq!(edge.assignment.iter().map(|b| b.arg_name.as_str()).collect::<Vec<_>>(), ["sample", "path"]);
    assert_eq!(edge.outputs.iter().map(|o| o.output_name.as_str()).collect::<Vec<_>>(), ["result", "where"]);
    assert_eq!(machine.strands()[0].input_anchor_ids(), &[names[0]["s"], names[0]["p"]]);
    assert_eq!(machine.strands()[0].output_anchor_ids(), &[names[0]["r"], names[0]["w"]]);
    assert!(machine
        .to_machine_notation()
        .unwrap()
        .contains("[(sample=n0, path=n1) -> edge_0 -> (result=n2, where=n3)]"));

    let headers = format!("[first {first_urn}][second {second_urn}]");
    for wirings in ["[x -> first -> a][x -> second -> b]", "[x -> second -> b][x -> first -> a]"] {
        let parsed = parse_machine(&format!("{headers}{wirings}"), &registry).unwrap();
        let steps: Vec<bool> = parsed.strands()[0]
            .edges()
            .iter()
            .map(|step| step.cap_urn.to_string().contains("zz-first"))
            .collect();
        assert_eq!(steps, [true, false], "{wirings}");
    }
}

// TEST12867: the render payload says which binding feeds a step's main input
// and which output is its main one, by the step's definition, so a view can
// draw the main wires plain and name the others.
#[test]
fn test12867_render_payload_marks_the_main_input_and_output() {
    let registry = registry();
    for urn in [SERIES, BENCHMARK, COMPARISON, RESIDUALS] {
        registry.insert_cached_media_def_for_test(media_def(urn, None, None));
    }
    let parsed = parse_machine(
        &format!("{HEADERS}[(path, benchmark=bench) -> compare -> (result, residuals=res)]"),
        &registry,
    )
    .unwrap();
    let payload: serde_json::Value =
        serde_json::from_str(&parsed.to_render_payload_json(&registry).unwrap()).unwrap();
    let edge = &payload["strands"][0]["edges"][0];
    let main_of = |list: &str, key: &str, flag: &str| -> Vec<(String, bool)> {
        let mut entries: Vec<(String, bool)> = edge[list]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| (entry[key].as_str().unwrap().to_string(), entry[flag].as_bool().unwrap()))
            .collect();
        entries.sort();
        entries
    };
    assert_eq!(
        main_of("assignment", "arg_name", "is_main_input"),
        vec![("benchmark".to_string(), false), ("series".to_string(), true)]
    );
    assert_eq!(
        main_of("outputs", "output_name", "is_main_output"),
        vec![("comparison".to_string(), true), ("residuals".to_string(), false)]
    );
}

// TEST12868: a render payload made with the text's names carries, on each
// node, the name the text gave it — whatever order the canonical walk numbers
// the nodes in — so an editor finds the payload's nodes in the text; names that
// do not describe the machine are refused.
#[test]
fn test12868_render_payload_carries_the_texts_node_names() {
    let registry = registry();
    for urn in [SERIES, BENCHMARK, COMPARISON, RESIDUALS, "media:numeric;score"] {
        registry.insert_cached_media_def_for_test(media_def(urn, None, None));
    }
    let text = format!(
        "{HEADERS}[res -> score -> scores][(path, benchmark=bench) -> compare -> (result, residuals=res)]"
    );
    let (parsed, names) = parse_machine_with_node_names(&text, &registry).unwrap();
    let payload: serde_json::Value = serde_json::from_str(
        &parsed.to_render_payload_json_with_names(&registry, &names).unwrap(),
    )
    .unwrap();
    let strand = &payload["strands"][0];
    let name_of: HashMap<&str, &str> = strand["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|node| (node["id"].as_str().unwrap(), node["name"].as_str().unwrap()))
        .collect();
    let mut written: Vec<&str> = name_of.values().copied().collect();
    written.sort();
    assert_eq!(written, ["bench", "path", "res", "result", "scores"]);

    let compare = strand["edges"]
        .as_array()
        .unwrap()
        .iter()
        .find(|edge| edge["title"] == "compare")
        .unwrap();
    let fed: HashMap<&str, &str> = compare["assignment"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| (b["arg_name"].as_str().unwrap(), name_of[b["source_node"].as_str().unwrap()]))
        .collect();
    assert_eq!(fed["benchmark"], "bench", "the benchmark is fed by the node the text calls bench");
    assert_eq!(fed["series"], "path");
    let took: HashMap<&str, &str> = compare["outputs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| (o["output_name"].as_str().unwrap(), name_of[o["node"].as_str().unwrap()]))
        .collect();
    assert_eq!(took["residuals"], "res");
    assert_eq!(took["comparison"], "result");

    // Without the text's names, a node is its canonical id alone.
    let canonical: serde_json::Value =
        serde_json::from_str(&parsed.to_render_payload_json(&registry).unwrap()).unwrap();
    assert!(canonical["strands"][0]["nodes"][0].get("name").is_none());

    let mut doubled = names.clone();
    let (some_name, &node) = doubled[0].iter().next().map(|(n, id)| (n.clone(), id)).unwrap();
    doubled[0].insert(format!("{some_name}_again"), node);
    assert!(matches!(
        parsed.to_render_payload_json_with_names(&registry, &doubled),
        Err(MachineAbstractionError::NodeNamesMismatch { .. })
    ));
    assert!(matches!(
        parsed.to_render_payload_json_with_names(&registry, &[]),
        Err(MachineAbstractionError::NodeNamesMismatch { .. })
    ));
}
