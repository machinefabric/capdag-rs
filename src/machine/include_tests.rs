//! Tests of machines in step position: a header names another machine, and
//! its steps are written into the machine that includes it
//! (`capdag/docs/09-machine-notation.md`, §Machines in step position).

use std::collections::HashSet;

use crate::cap::definition::{ArgSource, CapArg, CapOutput};
use crate::cap::registry::FabricRegistry;
use crate::StoredMediaDef;

use super::error::{MachineAbstractionError, MachineParseError, MachineSyntaxError};
use super::graph::{BindingSource, Machine, MachineEdge, NodeId};
use super::include::{FileMachineSource, MapMachineSource, NoMachineSource};
use super::notation_tests::{definition, registry, COMPARISON, HEADERS, NOTE, RESIDUALS, SERIES};
use super::parser::{
    parse_machine, parse_machine_with_sources, parse_machine_with_sources_async, StrandNodeNames,
};
use super::syntax::{OnError, StepPolicy};

/// `analyse`: two inputs of ONE type — `series`, and `bench` — and two
/// outputs: `cmp`, and `scores`, one per residual. `res` is a node inside.
const ANALYSE: &str =
    "[(series, benchmark=bench, window=20) -> compare -> (cmp, residuals=res)][res -> score -> scores]";
/// `pub`: two inputs of different types, in two strands, and one output.
const PUB: &str = "[report -> publish -> ()][res -> mean -> m]";
/// `win`: an input that feeds an arg a constant can be written for.
const WIN: &str = "[(series, window=w) -> compare -> cmp]";
/// `outer`: a machine that includes `analyse`.
const OUTER: &str =
    "[analyse machine \"analyse\"][(series=x, bench=y) -> analyse -> (cmp=c, scores=s)][c -> publish -> ()]";

const FETCH_TWO: &str = "[(reference=\"a\") -> fetch -> a][(reference=\"b\") -> fetch -> b]";

fn with_headers(wirings: &str) -> String {
    format!("{HEADERS}{wirings}")
}

/// The machines above, each under its name.
fn library() -> MapMachineSource {
    let mut source = MapMachineSource::new();
    for (name, wirings) in [("analyse", ANALYSE), ("pub", PUB), ("win", WIN), ("outer", OUTER)] {
        source.insert(name, with_headers(wirings));
    }
    source
}

fn parse(wirings: &str) -> Result<(Machine, Vec<StrandNodeNames>), MachineParseError> {
    parse_machine_with_sources(&with_headers(wirings), &registry(), &library(), None)
}

fn machine(wirings: &str) -> Machine {
    parse(wirings).unwrap_or_else(|e| panic!("{wirings} must resolve: {e}")).0
}

/// The same wirings with no machine in them, for what an inclusion must equal.
fn flat(wirings: &str) -> Machine {
    parse_machine(&with_headers(wirings), &registry())
        .unwrap_or_else(|e| panic!("{wirings} must resolve: {e}"))
}

fn refusal(wirings: &str) -> MachineAbstractionError {
    match parse(wirings) {
        Err(MachineParseError::Resolution(e)) => e,
        Err(other) => panic!("{wirings}: expected a resolution error, got {other}"),
        Ok(_) => panic!("{wirings} must be refused"),
    }
}

fn edges(machine: &Machine) -> Vec<&MachineEdge> {
    machine.strands().iter().flat_map(|strand| strand.edges()).collect()
}

/// The edges whose cap URN bears the tag `cap`.
fn edges_of<'a>(machine: &'a Machine, cap: &str) -> Vec<&'a MachineEdge> {
    edges(machine)
        .into_iter()
        .filter(|edge| {
            let urn = edge.cap_urn.to_string();
            urn.trim_start_matches("cap:").split(';').any(|tag| tag == cap)
        })
        .collect()
}

/// The names of the nodes `ids` picks out of each strand.
fn names_of(
    machine: &Machine,
    names: &[StrandNodeNames],
    ids: impl Fn(&super::graph::MachineStrand) -> &[NodeId],
) -> HashSet<String> {
    machine
        .strands()
        .iter()
        .zip(names)
        .flat_map(|(strand, names)| {
            let ids: HashSet<NodeId> = ids(strand).iter().copied().collect();
            names
                .iter()
                .filter(move |(_, id)| ids.contains(id))
                .map(|(name, _)| name.clone())
        })
        .collect()
}

fn set(names: &[&str]) -> HashSet<String> {
    names.iter().map(|name| name.to_string()).collect()
}

// TEST12803: a machine in step position is written out as its own steps:
// the including machine is the machine one would have written by hand with
// those steps in it. Its inputs are bound by the names its notation wrote —
// and by nothing else: binding the other way round is another machine. A
// node inside it is no input or output of the whole, and each of its steps
// says which machine step it came through.
#[test]
fn test12803_an_included_machine_is_its_steps_written_in() {
    let including =
        format!("[analyse machine \"analyse\"]{FETCH_TWO}[(series=a, bench=b) -> analyse -> (cmp=c, scores=s)][c -> publish -> ()]");
    let (inlined, names) = parse(&including).unwrap();
    let by_hand = flat(&format!(
        "{FETCH_TWO}[(a, benchmark=b, window=20) -> compare -> (c, residuals=r)][r -> score -> s][c -> publish -> ()]"
    ));
    assert!(inlined.is_equivalent(&by_hand));
    assert_eq!(edges(&inlined).len(), 5);

    let swapped = machine(&format!(
        "[analyse machine \"analyse\"]{FETCH_TWO}[(series=b, bench=a) -> analyse -> (cmp=c, scores=s)][c -> publish -> ()]"
    ));
    assert!(!swapped.is_equivalent(&by_hand), "the names say which input each source is for");

    // What the whole gives and takes is what was written in it.
    assert_eq!(names_of(&inlined, &names, |s| s.output_anchor_ids()), set(&["s"]));
    assert_eq!(names_of(&inlined, &names, |s| s.input_anchor_ids()), set(&[]));
    // The node inside the included machine is a node of the whole, under a
    // name nothing written can bear.
    assert!(names[0].contains_key("analyse#1/res"), "{:?}", names[0].keys());
    assert!(!names[0].contains_key("res"));

    for cap in ["compare", "score"] {
        let [edge] = edges_of(&inlined, cap)[..] else { panic!("one {cap} step") };
        assert_eq!(edge.included_as, vec!["analyse#1".to_string()]);
    }
    for edge in edges_of(&inlined, "fetch").into_iter().chain(edges_of(&inlined, "publish")) {
        assert!(edge.included_as.is_empty(), "a step the machine wrote itself");
    }

    // The graph view is told the same.
    let registry = registry();
    for urn in [SERIES, COMPARISON, RESIDUALS, "media:numeric;score"] {
        if registry.get_cached_media_def(urn).is_none() {
            registry.insert_cached_media_def_for_test(StoredMediaDef {
                dims: Vec::new(),
                version: 0,
                urn: urn.to_string(),
                media_type: "application/json".to_string(),
                title: urn.to_string(),
                profile_uri: None,
                schema: None,
                description: None,
                documentation: None,
                validation: None,
                metadata: None,
                extensions: Vec::new(),
            });
        }
    }
    let payload: serde_json::Value =
        serde_json::from_str(&inlined.to_render_payload_json(&registry).unwrap()).unwrap();
    let came_through: Vec<&serde_json::Value> = payload["strands"][0]["edges"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|edge| edge["included_as"] == serde_json::json!(["analyse#1"]))
        .collect();
    assert_eq!(came_through.len(), 2);
}

// TEST12804: a machine used twice is written in twice. Each use has nodes of
// its own: nothing of one use is a node of the other.
#[test]
fn test12804_each_use_of_a_machine_has_nodes_of_its_own() {
    let (inlined, names) = parse(&format!(
        "[analyse machine \"analyse\"]{FETCH_TWO}\
         [(series=a, bench=b) -> analyse -> (cmp=c1, scores=s1)]\
         [(series=b, bench=a) -> analyse -> (cmp=c2, scores=s2)]"
    ))
    .unwrap();
    assert_eq!(edges(&inlined).len(), 6);
    let first = names[0]["analyse#1/res"];
    let second = names[0]["analyse#2/res"];
    assert_ne!(first, second);
    let through = |use_: &str| {
        edges(&inlined)
            .into_iter()
            .filter(|edge| edge.included_as == vec![use_.to_string()])
            .count()
    };
    assert_eq!((through("analyse#1"), through("analyse#2")), (2, 2));
    // Each use's scoring step reads its own residuals.
    let reads: HashSet<NodeId> = edges_of(&inlined, "score")
        .iter()
        .map(|edge| edge.assignment[0].node().unwrap())
        .collect();
    assert_eq!(reads, HashSet::from([first, second]));
    assert_eq!(
        names_of(&inlined, &names, |s| s.output_anchor_ids()),
        set(&["c1", "s1", "c2", "s2"])
    );

    // Two headers may name one machine: reading it for the first does not
    // make the second a circle, and each alias counts its own uses.
    let (aliased, names) = parse(&format!(
        "[one machine \"analyse\"][two machine \"analyse\"]{FETCH_TWO}\
         [(series=a, bench=b) -> one -> (cmp=c1, scores=s1)]\
         [(series=b, bench=a) -> two -> (cmp=c2, scores=s2)]"
    ))
    .unwrap();
    assert!(aliased.is_equivalent(&inlined));
    assert!(names[0].contains_key("one#1/res") && names[0].contains_key("two#1/res"));
}

// TEST12805: an unnamed member of a machine step is matched to an input by
// type alone, and an unnamed target takes the one output the other targets
// leave. Where the types leave more than one answer, or a member has no type
// to be matched by, or fits nothing, the wiring is refused by name.
#[test]
fn test12805_unnamed_members_of_a_machine_step_are_matched_by_type() {
    let produce = "[(reference=\"a\") -> fetch -> a][a -> compare -> (c, residuals=r)]";
    let unnamed = machine(&format!("[pub machine \"pub\"]{produce}[(c, r) -> pub -> m]"));
    let named = machine(&format!("[pub machine \"pub\"]{produce}[(res=r, report=c) -> pub -> (m=m)]"));
    let by_hand = flat(&format!("{produce}[c -> publish -> ()][r -> mean -> m]"));
    assert!(unnamed.is_equivalent(&named));
    assert!(unnamed.is_equivalent(&by_hand));

    // What one machine step makes is typed for the next: the comparison
    // `analyse` gives is matched to the input of `pub` that takes one.
    let chained = machine(&format!(
        "[analyse machine \"analyse\"][pub machine \"pub\"]{FETCH_TWO}\
         [(series=a, bench=b) -> analyse -> (cmp=c, scores=s)][a -> compare -> (c0, residuals=r)]\
         [(c, r) -> pub -> m]"
    ));
    assert!(chained.is_equivalent(&flat(&format!(
        "{FETCH_TWO}[(a, benchmark=b, window=20) -> compare -> (c, residuals=r1)][r1 -> score -> s]\
         [a -> compare -> (c0, residuals=r)][c -> publish -> ()][r -> mean -> m]"
    ))));

    // `analyse` has two outputs: an unnamed target beside a named one takes
    // the other.
    let one_named = machine(&format!(
        "[analyse machine \"analyse\"]{FETCH_TWO}[(series=a, bench=b) -> analyse -> (c, scores=s)]"
    ));
    let both_named = machine(&format!(
        "[analyse machine \"analyse\"]{FETCH_TWO}[(series=a, bench=b) -> analyse -> (cmp=c, scores=s)]"
    ));
    assert!(one_named.is_equivalent(&both_named));
    // An input a member named is said: the unnamed member beside it has the
    // other input, though both hold one type.
    let one_unnamed = machine(&format!(
        "[analyse machine \"analyse\"]{FETCH_TWO}[(series=a, b) -> analyse -> (cmp=c, scores=s)]"
    ));
    assert!(one_unnamed.is_equivalent(&both_named));
    let other_unnamed = machine(&format!(
        "[analyse machine \"analyse\"]{FETCH_TWO}[(a, series=b) -> analyse -> (cmp=c, scores=s)]"
    ));
    assert!(!other_unnamed.is_equivalent(&both_named));

    // Two sources of the one type its two inputs hold.
    match refusal(&format!(
        "[analyse machine \"analyse\"]{FETCH_TWO}[(a, b) -> analyse -> (cmp=c, scores=s)]"
    )) {
        MachineAbstractionError::AmbiguousMachineMembers { alias, members, .. } => {
            assert_eq!(alias, "analyse");
            // Which members to name, and the inputs each could be.
            let named: Vec<(Option<&str>, Vec<&str>)> = members
                .iter()
                .map(|m| (m.node.as_deref(), m.could_be.iter().map(String::as_str).collect()))
                .collect();
            assert_eq!(named, vec![(Some("a"), vec!["bench", "series"]), (Some("b"), vec!["bench", "series"])]);
        }
        other => panic!("{other}"),
    }
    // An input of the including machine, unnamed: nothing says what it is.
    match refusal(&format!(
        "[analyse machine \"analyse\"]{FETCH_TWO}[(x, bench=b) -> analyse -> (cmp=c, scores=s)]"
    )) {
        MachineAbstractionError::UntypedMachineMember { alias, members, .. } => {
            assert_eq!((alias.as_str(), members), ("analyse", vec!["x".to_string()]));
        }
        other => panic!("{other}"),
    }
    // A comparison, where the input left takes a series.
    match refusal(&format!(
        "[analyse machine \"analyse\"]{produce}[(c, bench=a) -> analyse -> (cmp=c2, scores=s)]"
    )) {
        MachineAbstractionError::UnmatchedMachineMember { alias, source_urn, .. } => {
            assert_eq!(alias, "analyse");
            assert_eq!(source_urn, crate::MediaUrn::from_string(COMPARISON).unwrap().to_string());
        }
        other => panic!("{other}"),
    }
    // The member that fits no input is the one named, wherever it stands.
    match refusal(&format!(
        "[analyse machine \"analyse\"]{produce}[(a, c) -> analyse -> (cmp=c2, scores=s)]"
    )) {
        MachineAbstractionError::UnmatchedMachineMember { source_urn, .. } => {
            assert_eq!(source_urn, crate::MediaUrn::from_string(COMPARISON).unwrap().to_string());
        }
        other => panic!("{other}"),
    }
    // Two outputs left for one unnamed target, and none left for it.
    match refusal(&format!(
        "[analyse machine \"analyse\"]{FETCH_TWO}[(series=a, bench=b) -> analyse -> c]"
    )) {
        MachineAbstractionError::UnnamedMachineTarget { left, .. } => {
            assert_eq!(left, vec!["cmp".to_string(), "scores".to_string()]);
        }
        other => panic!("{other}"),
    }
    match refusal(&format!(
        "[analyse machine \"analyse\"]{FETCH_TWO}[(series=a, bench=b) -> analyse -> (x, cmp=c, scores=s)]"
    )) {
        MachineAbstractionError::UnnamedMachineTarget { left, .. } => assert!(left.is_empty()),
        other => panic!("{other}"),
    }
}

// TEST12806: what a machine step may not be given, each refused by name: a
// name that is no input or no output of it, an input given nothing or given
// twice, an output taken twice, a source of the wrong type, and a policy.
#[test]
fn test12806_a_machine_step_is_held_to_the_machines_inputs_and_outputs() {
    let step = |wiring: &str| {
        refusal(&format!(
            "[analyse machine \"analyse\"]{FETCH_TWO}[a -> compare -> made]{wiring}"
        ))
    };
    match step("[(serie=a, bench=b) -> analyse -> (cmp=c, scores=s)]") {
        MachineAbstractionError::UnknownMachineInput { alias, name, declared, .. } => {
            assert_eq!((alias.as_str(), name.as_str()), ("analyse", "serie"));
            assert_eq!(declared, vec!["bench".to_string(), "series".to_string()]);
        }
        other => panic!("{other}"),
    }
    match step("[(series=a, bench=b) -> analyse -> (cmp=c, score=s)]") {
        MachineAbstractionError::UnknownMachineOutput { name, declared, .. } => {
            assert_eq!(name, "score");
            assert_eq!(declared, vec!["cmp".to_string(), "scores".to_string()]);
        }
        other => panic!("{other}"),
    }
    match step("[(series=a) -> analyse -> (cmp=c, scores=s)]") {
        MachineAbstractionError::MachineInputBinding { input, count, .. } => {
            assert_eq!((input.as_str(), count), ("bench", 0));
        }
        other => panic!("{other}"),
    }
    match step("[(series=a, series=b, bench=b) -> analyse -> (cmp=c, scores=s)]") {
        MachineAbstractionError::MachineInputBinding { input, count, .. } => {
            assert_eq!((input.as_str(), count), ("series", 2));
        }
        other => panic!("{other}"),
    }
    match step("[(series=a, bench=b) -> analyse -> (cmp=c, cmp=d)]") {
        MachineAbstractionError::MachineOutputTakenTwice { output, .. } => assert_eq!(output, "cmp"),
        other => panic!("{other}"),
    }
    match step("[(series=made, bench=b) -> analyse -> (cmp=c, scores=s)]") {
        MachineAbstractionError::SourceDoesNotConformToMachineInput { name, accepts, source_urn, .. } => {
            assert_eq!(name, "series");
            assert_eq!(accepts, crate::MediaUrn::from_string(SERIES).unwrap().to_string());
            assert_eq!(source_urn, crate::MediaUrn::from_string(COMPARISON).unwrap().to_string());
        }
        other => panic!("{other}"),
    }
    match step("[(series=a, bench=b) -> analyse{retry=2} -> (cmp=c, scores=s)]") {
        MachineAbstractionError::PolicyOnMachine { alias, .. } => assert_eq!(alias, "analyse"),
        other => panic!("{other}"),
    }

    // A cap header and a machine header name steps in one space: one alias
    // is declared once, whichever kind declares it first.
    for headers in ["[fetch machine \"analyse\"]", "[twice machine \"analyse\"][twice machine \"pub\"]"] {
        match parse(&format!("{headers}{FETCH_TWO}")) {
            Err(MachineParseError::Syntax(MachineSyntaxError::DuplicateAlias { .. })) => {}
            other => panic!("{headers}: {:?}", other.map(|_| ())),
        }
    }
    // A machine header is a declaration like any other: with no wiring after
    // it, the machine has headers and no steps.
    match parse_machine_with_sources("[analyse machine \"analyse\"]", &registry(), &library(), None) {
        Err(MachineParseError::Syntax(MachineSyntaxError::NoEdges)) => {}
        other => panic!("{:?}", other.map(|_| ())),
    }
}

// TEST12807: a reference that does not resolve, a machine that is not a
// valid machine, and a circle of inclusions are each refused — a header
// nothing uses included. A circle is one failure of the whole, naming every
// machine on the way round, whichever machine the reading started from.
#[test]
fn test12807_a_reference_must_resolve_to_a_valid_machine_and_never_to_itself() {
    match refusal("[lost machine \"nowhere\"][(reference=\"a\") -> fetch -> a]") {
        MachineAbstractionError::UnknownMachineReference { alias, reference, details } => {
            assert_eq!((alias.as_str(), reference.as_str()), ("lost", "nowhere"));
            assert!(details.contains("nowhere"), "{details}");
        }
        other => panic!("{other}"),
    }
    // With nothing to resolve against at all, the refusal says that.
    match parse_machine_with_sources(
        &with_headers("[lost machine \"nowhere\"][(reference=\"a\") -> fetch -> a]"),
        &registry(),
        &NoMachineSource,
        None,
    ) {
        Err(MachineParseError::Syntax(MachineSyntaxError::MachineReferenceWithoutSource { .. })) => {}
        other => panic!("{:?}", other.map(|_| ())),
    }

    let mut source = library();
    source.insert("broken", with_headers("[p -> nothing -> q]"));
    source.insert("a", with_headers("[inner machine \"b\"][(reference=\"x\") -> fetch -> s]"));
    source.insert("b", with_headers("[back machine \"a\"][(reference=\"y\") -> fetch -> t]"));
    let parse_from = |notation: &str, id: Option<&str>| {
        match parse_machine_with_sources(notation, &registry(), &source, id) {
            Err(MachineParseError::Resolution(e)) => e,
            other => panic!("expected a resolution error, got {:?}", other.map(|_| ())),
        }
    };

    match parse_from(&with_headers("[bad machine \"broken\"][(reference=\"a\") -> fetch -> a]"), None) {
        MachineAbstractionError::InvalidIncludedMachine { alias, reference, error } => {
            assert_eq!((alias.as_str(), reference.as_str()), ("bad", "broken"));
            assert!(matches!(
                *error,
                MachineParseError::Syntax(MachineSyntaxError::UndefinedAlias { ref alias }) if alias == "nothing"
            ));
        }
        other => panic!("{other}"),
    }

    let a = with_headers("[inner machine \"b\"][(reference=\"x\") -> fetch -> s]");
    match parse_from(&a, Some("a")) {
        MachineAbstractionError::CyclicMachineReference { chain } => assert_eq!(chain, ["a", "b", "a"]),
        other => panic!("{other}"),
    }
    // Read as notation the source does not know: the circle is found one
    // step later, and is the same circle.
    match parse_from(&a, None) {
        MachineAbstractionError::CyclicMachineReference { chain } => assert_eq!(chain, ["b", "a", "b"]),
        other => panic!("{other}"),
    }
    // A machine that only leads to the circle is not on it.
    let leading = with_headers("[first machine \"a\"][(reference=\"z\") -> fetch -> u]");
    match parse_from(&leading, Some("top")) {
        MachineAbstractionError::CyclicMachineReference { chain } => assert_eq!(chain, ["a", "b", "a"]),
        other => panic!("{other}"),
    }
}

// TEST12808: a constant written for an input of a machine step is the value
// of every arg that input feeds inside, and is held to that arg's validation
// there.
#[test]
fn test12808_a_constant_for_a_machine_input_reaches_the_arg_inside() {
    let fetch = "[(reference=\"a\") -> fetch -> a]";
    let inlined = machine(&format!("[win machine \"win\"]{fetch}[(series=a, w=30) -> win -> c]"));
    assert!(inlined.is_equivalent(&flat(&format!("{fetch}[(a, window=30) -> compare -> c]"))));
    let [compare] = edges_of(&inlined, "compare")[..] else { panic!("one compare step") };
    let window = compare.assignment.iter().find(|b| b.arg_name == "window").unwrap();
    assert_eq!(window.source, BindingSource::Constant(serde_json::json!(30)));

    match refusal(&format!("[win machine \"win\"]{fetch}[(series=a, w=1000) -> win -> c]")) {
        MachineAbstractionError::ConstantFailsValidation { name, constant, .. } => {
            assert_eq!((name.as_str(), constant.as_str()), ("window", "1000"));
        }
        other => panic!("{other}"),
    }
}

// TEST12818: a policy written on a step inside an included machine is that
// step's policy in the whole: the step is attempted again, or its failure
// tolerated, exactly as it would be where it was written.
#[test]
fn test12818_a_step_keeps_its_policy_where_it_is_included() {
    let mut source = library();
    source.insert(
        "careful",
        with_headers("[(series, benchmark=bench) -> compare{retry=2, on_error=absent} -> cmp]"),
    );
    let (inlined, _) = parse_machine_with_sources(
        &with_headers(&format!(
            "[careful machine \"careful\"]{FETCH_TWO}[(series=a, bench=b) -> careful -> c]"
        )),
        &registry(),
        &source,
        None,
    )
    .unwrap();
    let [compare] = edges_of(&inlined, "compare")[..] else { panic!("one compare step") };
    assert_eq!(compare.policy, StepPolicy { retry: 2, on_error: OnError::Absent });
    assert!(inlined.is_equivalent(&flat(&format!(
        "{FETCH_TWO}[(a, benchmark=b) -> compare{{retry=2, on_error=absent}} -> c]"
    ))));
    for fetch in edges_of(&inlined, "fetch") {
        assert_eq!(fetch.policy, StepPolicy::default());
    }
}

// TEST12809: a machine that includes a machine that includes a machine is
// one flat machine: a step says every machine step it came through,
// outermost first. An output of an included machine that nothing takes is
// not an output of the whole — the step that makes it still runs.
#[test]
fn test12809_inclusion_nests_and_an_untaken_output_is_not_the_wholes() {
    let (inlined, names) =
        parse(&format!("[deep machine \"outer\"]{FETCH_TWO}[(x=a, y=b) -> deep -> (s=out)]")).unwrap();
    let by_hand = flat(&format!(
        "{FETCH_TWO}[(a, benchmark=b, window=20) -> compare -> (c, residuals=r)][r -> score -> s][c -> publish -> ()]"
    ));
    assert!(inlined.is_equivalent(&by_hand));
    let [compare] = edges_of(&inlined, "compare")[..] else { panic!("one compare step") };
    assert_eq!(compare.included_as, vec!["deep#1".to_string(), "analyse#1".to_string()]);
    let [publish] = edges_of(&inlined, "publish")[..] else { panic!("one publish step") };
    assert_eq!(publish.included_as, vec!["deep#1".to_string()]);
    assert_eq!(names_of(&inlined, &names, |s| s.output_anchor_ids()), set(&["out"]));

    let (partial, names) = parse(&format!(
        "[analyse machine \"analyse\"]{FETCH_TWO}[(series=a, bench=b) -> analyse -> (cmp=c)][c -> publish -> ()]"
    ))
    .unwrap();
    assert_eq!(names_of(&partial, &names, |s| s.output_anchor_ids()), set(&[]));
    let [score] = edges_of(&partial, "score")[..] else { panic!("the scoring step is still a step") };
    assert!(score.outputs.is_empty(), "what it makes is taken by nothing");
    assert!(partial.is_equivalent(&flat(&format!(
        "{FETCH_TWO}[(a, benchmark=b, window=20) -> compare -> (c, residuals=r)][r -> score -> ()][c -> publish -> ()]"
    ))));
}

const LOOSE: &str = "cap:in=\"media:fmt=json;list;record;time-series\";loose;out=\"media:enc=utf-8\"";
const TIGHT: &str = "cap:in=\"media:enc=utf-8;note\";tight;out=\"media:tightened\"";

/// The registry of the tests above, with a step whose output is declared
/// more loosely than the step after it asks for.
fn registry_with_a_refinement() -> FabricRegistry {
    let registry = registry();
    registry.add_caps_to_cache(vec![
        definition(
            LOOSE,
            "loose",
            vec![CapArg::new(SERIES, "series", true, vec![ArgSource::Stdin { stdin: SERIES.to_string() }])],
            vec![CapOutput::new("media:enc=utf-8", "text", "Some text")],
        ),
        definition(
            TIGHT,
            "tight",
            vec![CapArg::new(NOTE, "note", true, vec![ArgSource::Stdin { stdin: NOTE.to_string() }])],
            vec![CapOutput::new("media:tightened", "tightened", "Tightened")],
        ),
    ]);
    registry
}

// TEST12810: an included machine is what it is alone. Inside `refine`, the
// second step asks for a tighter type than the first declares, which makes
// the node between them hold the tighter type; written into another machine
// that node holds it still, and the machine resolves as it did alone.
#[test]
fn test12810_a_node_of_an_included_machine_holds_the_type_it_held_alone() {
    let registry = registry_with_a_refinement();
    let headers = format!("{HEADERS}[loose {LOOSE}][tight {TIGHT}]");
    let refine = format!("{headers}[s -> loose -> t][t -> tight -> u]");
    let alone = parse_machine(&refine, &registry).expect("the machine resolves alone");
    let note = crate::MediaUrn::from_string(NOTE).unwrap();
    assert!(
        alone.strands()[0].nodes().iter().any(|urn| urn.is_equivalent(&note).unwrap()),
        "the node between the steps holds the tighter type"
    );

    let mut source = MapMachineSource::new();
    source.insert("refine", refine);
    let (inlined, names) = parse_machine_with_sources(
        &format!("{headers}[refine machine \"refine\"][(reference=\"a\") -> fetch -> a][a -> refine -> u]"),
        &registry,
        &source,
        None,
    )
    .expect("the machine resolves written into another");
    let between = names[0]["refine#1/t"];
    assert!(inlined.strands()[0].node_urn(between).is_equivalent(&note).unwrap());
    let by_hand = parse_machine(
        &format!("{headers}[(reference=\"a\") -> fetch -> a][a -> loose -> t][t -> tight -> u]"),
        &registry,
    )
    .unwrap();
    assert!(inlined.is_equivalent(&by_hand));
}

// TEST12811: machines in files. A reference is a path relative to the file
// that wrote it — not to where the reading started — and a machine is known
// by its file, however the path to it is spelled: a file that reaches itself
// through another spelling is a circle.
#[test]
fn test12811_file_references_are_relative_to_the_including_file() {
    let root = tempfile::tempdir().unwrap();
    let lib = root.path().join("lib");
    std::fs::create_dir_all(&lib).unwrap();
    let top = root.path().join("top.machine");
    std::fs::write(
        &top,
        with_headers(&format!(
            "[deep machine \"lib/outer.machine\"]{FETCH_TWO}[(x=a, y=b) -> deep -> (s=out)]"
        )),
    )
    .unwrap();
    // `outer` names `analyse` beside itself, in `lib/`.
    std::fs::write(lib.join("outer.machine"), with_headers(&OUTER.replace("\"analyse\"", "\"analyse.machine\""))).unwrap();
    std::fs::write(lib.join("analyse.machine"), with_headers(ANALYSE)).unwrap();

    let elsewhere = tempfile::tempdir().unwrap();
    let source = FileMachineSource::new(elsewhere.path());
    let id = FileMachineSource::id_of(&top).unwrap();
    let (inlined, _) = parse_machine_with_sources(
        &std::fs::read_to_string(&top).unwrap(),
        &registry(),
        &source,
        Some(&id),
    )
    .expect("every reference is read relative to the file that wrote it");
    assert_eq!(edges(&inlined).len(), 5);

    // Notation from no file reads its references relative to the base.
    let from_base = FileMachineSource::new(root.path());
    parse_machine_with_sources(&std::fs::read_to_string(&top).unwrap(), &registry(), &from_base, None)
        .expect("relative to the base");
    match parse_machine_with_sources(&std::fs::read_to_string(&top).unwrap(), &registry(), &source, None) {
        Err(MachineParseError::Resolution(MachineAbstractionError::UnknownMachineReference {
            reference,
            details,
            ..
        })) => {
            assert_eq!(reference, "lib/outer.machine");
            assert!(details.contains("outer.machine"), "{details}");
        }
        other => panic!("{:?}", other.map(|_| ())),
    }

    let itself = root.path().join("itself.machine");
    std::fs::write(
        &itself,
        with_headers("[again machine \"lib/../itself.machine\"][(reference=\"a\") -> fetch -> a]"),
    )
    .unwrap();
    let id = FileMachineSource::id_of(&itself).unwrap();
    match parse_machine_with_sources(&std::fs::read_to_string(&itself).unwrap(), &registry(), &source, Some(&id)) {
        Err(MachineParseError::Resolution(MachineAbstractionError::CyclicMachineReference { chain })) => {
            assert_eq!(chain, [id.clone(), id]);
        }
        other => panic!("{:?}", other.map(|_| ())),
    }
}

// TEST12812: read asynchronously, a machine that includes others resolves to
// the same machine, and a circle of inclusions ends as one — it is walked
// once, not for ever.
#[tokio::test]
async fn test12812_the_async_reading_follows_inclusions_once() {
    let registry = registry();
    let including = with_headers(&format!(
        "[deep machine \"outer\"]{FETCH_TWO}[(x=a, y=b) -> deep -> (s=out)]"
    ));
    let (asynchronous, _) = parse_machine_with_sources_async(&including, &registry, &library(), None)
        .await
        .unwrap();
    let (synchronous, _) = parse_machine_with_sources(&including, &registry, &library(), None).unwrap();
    assert!(asynchronous.is_equivalent(&synchronous));

    let mut circle = MapMachineSource::new();
    circle.insert("a", with_headers("[inner machine \"b\"][(reference=\"x\") -> fetch -> s]"));
    circle.insert("b", with_headers("[back machine \"a\"][(reference=\"y\") -> fetch -> t]"));
    let read = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        parse_machine_with_sources_async(
            &with_headers("[inner machine \"b\"][(reference=\"x\") -> fetch -> s]"),
            &registry,
            &circle,
            Some("a"),
        ),
    )
    .await
    .expect("a circle of inclusions is walked once");
    match read {
        Err(MachineParseError::Resolution(MachineAbstractionError::CyclicMachineReference { chain })) => {
            assert_eq!(chain, ["a", "b", "a"]);
        }
        other => panic!("{:?}", other.map(|_| ())),
    }
}
