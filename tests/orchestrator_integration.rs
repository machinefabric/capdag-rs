//! End-to-end tests of running machines, on the test cartridge.
//!
//! Every test here takes the path a machine takes outside the tests:
//! notation → plan → `execute_plan` on a `CliRuntime` that hosts the real
//! test cartridge process (`support::Machines`). The registry the notation
//! resolves against is the cartridge's own manifest, so the definitions a
//! machine is planned on are the ones the cartridge that runs it serves.
//!
//! The test cartridge is built by cargo, against this capdag, once per test
//! binary.

mod support;

use capdag::orchestrator::{build_plans_from_notation, ParseOrchestrationError, ResolvedNotation};
use capdag::{AttributionClass, RunVerdict, StepOutcome};
use support::{Given, Machines};

const TAG: &str = "TestcartridgeTest";

const EDGE1: &str = r#"cap:in="media:enc=utf-8;node1";test-edge1;out="media:enc=utf-8;node2""#;
const EDGE2: &str = r#"cap:in="media:enc=utf-8;node2";test-edge2;out="media:enc=utf-8;node3""#;
const EDGE7: &str = r#"cap:in="media:enc=utf-8;node3";test-edge7;out="media:enc=utf-8;node6""#;
const EDGE8: &str = r#"cap:in="media:enc=utf-8;node6";test-edge8;out="media:enc=utf-8;node7""#;
const EDGE9: &str = r#"cap:in="media:enc=utf-8;node7";test-edge9;out="media:enc=utf-8;node8""#;
const EDGE10: &str = r#"cap:in="media:enc=utf-8;node8";test-edge10;out="media:enc=utf-8;node1""#;
const COMBINE: &str = r#"cap:in="media:enc=utf-8;node2";test-combine;out="media:enc=utf-8;combined""#;
const LARGE: &str = r#"cap:in="media:void";test-large;out="media:""#;
const PEER: &str = r#"cap:in="media:enc=utf-8;node1";test-peer;out="media:enc=utf-8;node5""#;
const SPLIT: &str = r#"cap:in="media:enc=utf-8;node1";test-split;out="media:enc=utf-8;node2""#;
const SPLIT_PIECES: &str = "media:numeric;split-pieces";

const SAY: &str = r#"cap:in="media:void";test-dag-say;out="media:dag-said;enc=utf-8""#;
const WORDS: &str = r#"cap:in="media:enc=utf-8";test-dag-words;out="media:dag-word;enc=utf-8""#;
const SHOUT: &str = r#"cap:in="media:enc=utf-8";test-dag-shout;out="media:dag-shout;enc=utf-8""#;
const JOIN: &str = r#"cap:in="media:enc=utf-8";test-dag-join;out="media:dag-joined;enc=utf-8""#;
const PAIR: &str = r#"cap:in="media:enc=utf-8";test-dag-pair;out="media:dag-pair;enc=utf-8""#;
const FLAKY: &str = r#"cap:in="media:enc=utf-8";test-dag-flaky;out="media:dag-flaky;enc=utf-8""#;

/// The headers of every cap the DAG tests wire, each under its short name.
fn dag_headers() -> String {
    format!(
        "[say {SAY}][words {WORDS}][shout {SHOUT}][join {JOIN}][pair {PAIR}][flaky {FLAKY}]\
         [split {SPLIT}][edge2 {EDGE2}]"
    )
}

// =============================================================================
// Resolving notation against the cartridge's own definitions
// =============================================================================

// TEST919: a one-step machine resolves against the cartridge's manifest: it
// takes one input and gives one output, each under the name the notation
// wrote, holding the media the cap declares.
#[tokio::test]
async fn test919_parse_simple_testcartridge_graph() {
    let machines = Machines::new(TAG);
    let notation = format!("[test_edge1 {EDGE1}]\n[A -> test_edge1 -> B]\n");
    let resolved = ResolvedNotation::resolve(&notation, machines.registry.clone())
        .await
        .expect("resolves");
    assert_eq!(resolved.strand_count(), 1);
    let same = |a: &capdag::MediaUrn, b: &str| {
        a.is_equivalent(&capdag::MediaUrn::from_string(b).unwrap()).unwrap()
    };
    let inputs = resolved.inputs().unwrap();
    assert_eq!(inputs.len(), 1);
    assert_eq!(inputs[0].name, "A");
    assert!(same(&inputs[0].media_urn, "media:enc=utf-8;node1"));
    let outputs = resolved.outputs().unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].name, "B");
    assert!(same(&outputs[0].media_urn, "media:enc=utf-8;node2"));
}

// TEST950: Validate that cycles are rejected
#[tokio::test]
async fn test950_reject_cycles() {
    let machines = Machines::new(TAG);
    // The six-step chain ends on the type it starts from: wired back to its
    // own start, every step reads what a later one produces.
    let cycle = format!(
        "[e1 {EDGE1}][e2 {EDGE2}][e7 {EDGE7}][e8 {EDGE8}][e9 {EDGE9}][e10 {EDGE10}]\
         [A -> e1 -> B][B -> e2 -> C][C -> e7 -> D][D -> e8 -> E][E -> e9 -> F][F -> e10 -> A]"
    );
    match build_plans_from_notation(&cycle, machines.registry.clone()).await {
        Err(ParseOrchestrationError::NotADag { .. }) => {}
        other => panic!("Expected NotADag error, got: {:?}", other.map(|plans| plans.len())),
    }
}

// TEST943: Two nodes with the same media type but different names are two
// distinct graph positions — NOT a loop. A cap whose input and output are one
// type has its upstream and downstream node carry the same media URN; that
// must not collapse them into a self-loop. Node identity comes from the
// user-written name, not the media URN.
#[tokio::test]
async fn test943_same_media_different_names_is_not_a_cycle() {
    use capdag::cap::definition::{ArgSource, CapArg, CapOutput};
    const SAME: &str = r#"cap:in="media:enc=utf-8;node1";same-type;out="media:enc=utf-8;node1""#;
    let mut same = capdag::Cap::new(
        capdag::CapUrn::from_string(SAME).unwrap(),
        "Same type".to_string(),
        vec!["same-type".to_string()],
    );
    same.args.push(CapArg::new(
        "media:enc=utf-8;node1",
        "input",
        true,
        vec![ArgSource::Stdin {
            stdin: "media:enc=utf-8;node1".to_string(),
        }],
    ));
    same.outputs = vec![CapOutput::new("media:enc=utf-8;node1", "output", "the same type")];
    let machines = Machines::with_caps(TAG, vec![same]);
    let notation = format!("[same {SAME}][A -> same -> B]");
    let resolved = ResolvedNotation::resolve(&notation, machines.registry.clone())
        .await
        .expect("A -> same -> B must resolve: distinct names, not a cycle");
    assert_eq!(resolved.inputs().unwrap()[0].name, "A");
    assert_eq!(resolved.outputs().unwrap()[0].name, "B");
    let plans = build_plans_from_notation(&notation, machines.registry.clone()).await.unwrap();
    assert_eq!(plans[0].nodes.values().filter(|node| node.is_cap()).count(), 1);
}

// TEST949: Empty machine notation (no edges)
#[tokio::test]
async fn test949_empty_graph() {
    let machines = Machines::new(TAG);
    match build_plans_from_notation("", machines.registry.clone()).await {
        Err(ParseOrchestrationError::MachineSyntaxParseFailed(_)) => {}
        other => panic!("Expected MachineSyntaxParseFailed, got: {:?}", other.map(|plans| plans.len())),
    }
}

// TEST948: Invalid cap URN in machine notation
#[tokio::test]
async fn test948_invalid_cap_urn() {
    let machines = Machines::new(TAG);
    let notation = concat!(r#"[bad cap:INVALID]"#, "[A -> bad -> B]");
    assert!(
        build_plans_from_notation(notation, machines.registry.clone()).await.is_err(),
        "Should reject invalid cap URN"
    );
}

// TEST947: Cap not found in registry
#[tokio::test]
async fn test947_cap_not_found() {
    let machines = Machines::new(TAG);
    let notation = r#"
[nonexistent cap:in="media:unknown";nonexistent;out="media:unknown"]
[A -> nonexistent -> B]
"#;
    match build_plans_from_notation(notation, machines.registry.clone()).await {
        // The parser resolves header caps and wraps the lookup failure.
        Err(ParseOrchestrationError::MachineSyntaxParseFailed(message)) => {
            assert!(message.contains("nonexistent"), "{message}");
        }
        other => panic!("Expected MachineSyntaxParseFailed, got: {:?}", other.map(|plans| plans.len())),
    }
}

// =============================================================================
// Chains
// =============================================================================

// TEST889: Execute a single-step machine (test-edge1)
#[tokio::test]
async fn test889_execute_single_edge_dag() {
    let machines = Machines::new(TAG);
    let ran = machines
        .run(
            &format!("[test_edge1 {EDGE1}]\n[input -> test_edge1 -> output]\n"),
            &[("input", Given::One(b"TEST"))],
        )
        .await
        .expect("Execution failed");
    assert_eq!(ran.text("output"), "[PREPEND]TEST");
    assert_eq!(ran.result.verdict, RunVerdict::Completed);
}

// TEST888: Execute a two-step chain (test-edge1 -> test-edge2)
#[tokio::test]
async fn test888_execute_edge1_to_edge2_chain() {
    let machines = Machines::new(TAG);
    let ran = machines
        .run(
            &format!("[test_edge1 {EDGE1}][test_edge2 {EDGE2}][A -> test_edge1 -> B][B -> test_edge2 -> C]"),
            &[("A", Given::One(b"CHAIN"))],
        )
        .await
        .expect("Execution failed");
    // edge1: [PREPEND]CHAIN, edge2: [PREPEND]CHAIN[APPEND]
    assert_eq!(ran.text("C"), "[PREPEND]CHAIN[APPEND]");
}

/// The chain edge1 → edge2 → edge7 → edge8 [→ edge9 [→ edge10]], run on "hello".
async fn run_chain(steps: usize) -> String {
    let all = [
        ("e1", EDGE1),
        ("e2", EDGE2),
        ("e7", EDGE7),
        ("e8", EDGE8),
        ("e9", EDGE9),
        ("e10", EDGE10),
    ];
    let mut notation = String::new();
    for (alias, urn) in &all[..steps] {
        notation.push_str(&format!("[{alias} {urn}]"));
    }
    for (index, (alias, _)) in all[..steps].iter().enumerate() {
        notation.push_str(&format!("[n{index} -> {alias} -> n{}]", index + 1));
    }
    Machines::new(TAG)
        .run(&notation, &[("n0", Given::One(b"hello"))])
        .await
        .expect("Execution failed")
        .text(&format!("n{steps}"))
}

// TEST946: 4-machine: edge1 -> edge2 -> edge7 -> edge8
// node1 -> node2 -> node3 -> node6 -> node7
// "hello" -> "[PREPEND]hello" -> "[PREPEND]hello[APPEND]" -> "[PREPEND]HELLO[APPEND]" -> "]DNEPPA[OLLEH]DNEPERP["
#[tokio::test]
async fn test946_four_machine() {
    assert_eq!(run_chain(4).await, "]DNEPPA[OLLEH]DNEPERP[");
}

// TEST945: 5-machine: edge1 -> edge2 -> edge7 -> edge8 -> edge9 (wrap)
#[tokio::test]
async fn test945_five_machine() {
    assert_eq!(run_chain(5).await, "<<]DNEPPA[OLLEH]DNEPERP[>>");
}

// TEST944: 6-machine: edge1 -> edge2 -> edge7 -> edge8 -> edge9 -> edge10 (unwrap + lowercase)
#[tokio::test]
async fn test944_six_machine() {
    assert_eq!(run_chain(6).await, "]dneppa[olleh]dneperp[");
}

// =============================================================================
// Sources, sinks and constants
// =============================================================================

// TEST952: a machine that starts from a step nothing is wired into. The
// large-payload cap takes no input: it is given none — no dummy value stands
// in for one — and its megabyte arrives whole. A machine whose last step
// nothing reads has no output, and runs.
#[tokio::test]
async fn test952_execute_large_payload() {
    let machines = Machines::new(TAG);
    let ran = machines
        .run(&format!("[test_large {LARGE}]\n[() -> test_large -> output]\n"), &[])
        .await
        .expect("Execution failed");
    let (items, is_sequence) = ran.produced("output");
    assert!(!is_sequence);
    let bytes = &items[0];
    // Default size is 1MB
    assert_eq!(bytes.len(), 1_048_576);
    // Verify pattern: repeating 0-255
    for (i, &byte) in bytes.iter().enumerate() {
        assert_eq!(byte, (i % 256) as u8, "Pattern mismatch at byte {}", i);
    }

    let ran = machines
        .run(&format!("[test_edge1 {EDGE1}]\n[A -> test_edge1 -> ()]\n"), &[("A", Given::One(b"x"))])
        .await
        .expect("a machine with no output runs");
    assert!(ran.result.terminals.is_empty());
    assert_eq!(ran.result.verdict, RunVerdict::Completed);
}

// TEST12795: a constant written in the machine reaches its step as that
// arg's value, and starts a step that takes no input. An arg so bound is the
// machine's: a value for it from the run is refused, naming the step and the
// arg.
#[tokio::test]
async fn test12795_a_constant_starts_a_source_and_is_not_the_runs_to_change() {
    let machines = Machines::new(TAG);
    let notation = format!(
        "{}[(text=\"hello brave world\") -> say -> s][s -> words -> w][w -> shout -> out]",
        dag_headers()
    );
    let ran = machines.run(&notation, &[]).await.expect("Execution failed");
    assert_eq!(ran.texts("out"), vec!["HELLO", "BRAVE", "WORLD"]);

    let say = ran
        .plan
        .nodes
        .iter()
        .find(|(_, node)| node.cap_urn().is_some_and(|urn| urn.contains("test-dag-say")))
        .map(|(id, _)| id.clone())
        .unwrap();
    let given = std::collections::HashMap::from([(
        say.clone(),
        vec![("media:dag-say-text;enc=utf-8".to_string(), b"something else".to_vec())],
    )]);
    match capdag::RunArgumentLedger::new(&ran.plan, given) {
        Err(capdag::RunArgumentError::ArgumentBound { token_id, media_urn }) => {
            assert_eq!(token_id, say);
            assert_eq!(media_urn, "media:dag-say-text;enc=utf-8");
        }
        other => panic!("expected the bound arg to be refused, got {:?}", other.map(|_| ())),
    }
}

// =============================================================================
// Which arg, and which output
// =============================================================================

// TEST1316: Convergence — two producers routed into ONE cap via DISTINCT arg URNs.
//
// `A -edge1-> B(node2)`; `B -edge2-> D(node3)`; then `(B, D) -combine-> E`. `B` fans
// out (feeds both edge2 and combine). `combine` has a MAIN input on stdin (node2 = B)
// plus a distinct-URN second arg (node3 = D): the resolver matches B to the main arg
// and D to the node3 arg by type, and the cartridge reads both by their URNs. E must
// carry BOTH.
#[tokio::test]
async fn test1316_convergence_two_producers_distinct_arg_urns() {
    let machines = Machines::new(TAG);
    let ran = machines
        .run(
            &format!(
                "[test_edge1 {EDGE1}][test_edge2 {EDGE2}][test_combine {COMBINE}]\
                 [A -> test_edge1 -> B][B -> test_edge2 -> D][(B, D) -> test_combine -> E]"
            ),
            &[("A", Given::One(b"hello"))],
        )
        .await
        .expect("Execution failed");
    // B = edge1(hello) = "[PREPEND]hello"; D = edge2(B) = "[PREPEND]hello[APPEND]";
    // E = combine(B main, D arg) = "B|D".
    assert_eq!(ran.text("E"), "[PREPEND]hello|[PREPEND]hello[APPEND]");
}

// TEST12794: two inputs of ONE type are told apart by the name of the arg
// each is bound to — and by nothing else: swapping the names swaps what the
// step is given. Without a name there is nothing to tell them apart by, and
// the wiring is refused.
#[tokio::test]
async fn test12794_two_inputs_of_one_type_are_told_apart_by_name() {
    let machines = Machines::new(TAG);
    let inputs = [("a", Given::One(b"alpha" as &[u8])), ("b", Given::One(b"beta" as &[u8]))];
    let ran = machines
        .run(&format!("{}[(a, right=b) -> pair -> out]", dag_headers()), &inputs)
        .await
        .expect("Execution failed");
    assert_eq!(ran.text("out"), "alpha|beta");

    let inputs = [("a", Given::One(b"alpha" as &[u8])), ("b", Given::One(b"beta" as &[u8]))];
    let ran = machines
        .run(&format!("{}[(b, right=a) -> pair -> out]", dag_headers()), &inputs)
        .await
        .expect("Execution failed");
    assert_eq!(ran.text("out"), "beta|alpha");

    // Both named: the same.
    let inputs = [("a", Given::One(b"alpha" as &[u8])), ("b", Given::One(b"beta" as &[u8]))];
    let ran = machines
        .run(&format!("{}[(right=a, left=b) -> pair -> out]", dag_headers()), &inputs)
        .await
        .expect("Execution failed");
    assert_eq!(ran.text("out"), "beta|alpha");

    // Neither named: each fits either arg.
    match ResolvedNotation::resolve(
        &format!("{}[(a, b) -> pair -> out]", dag_headers()),
        machines.registry.clone(),
    )
    .await
    {
        Err(ParseOrchestrationError::MachineSyntaxParseFailed(message)) => {
            assert!(message.contains("test-dag-pair"), "the refusal names the cap: {message}");
        }
        other => panic!("expected the unnamed wiring to be refused, got {:?}", other.map(|_| ())),
    }
}

// TEST12728: a cap that answers on several outputs, as the last step of a run.
// What the run takes from it is what the machine took: here its main output,
// the head. The other streams it sends — a required rest, and two optional
// outputs that fire here, one of them carrying the input itself — pass the
// engine's audit and are not mixed into the result, which is what happened
// when every chunk of a response was collected into one buffer.
#[tokio::test]
async fn test12728_several_outputs_the_main_one_is_the_steps_result() {
    let machines = Machines::new(TAG);
    let notation = format!("[test_split {SPLIT}]\n[A -> test_split -> B]\n");

    // Every output fires: head, rest, flagged, original.
    let ran = machines.run(&notation, &[("A", Given::One(b"alpha!?:beta"))]).await.expect("Execution failed");
    assert_eq!(ran.text("B"), "alpha!?");

    // No ':' — the handler writes no rest. The runtime sends the required
    // output empty, and the engine, which holds the response to a stream for
    // every required output, accepts it.
    let ran = machines.run(&notation, &[("A", Given::One(b"alone"))]).await.expect("Execution failed");
    assert_eq!(ran.text("B"), "alone");
}

// TEST12729: the same cap in the middle of a chain. The next step is fed the
// main output and nothing of the others. The rest is padded past the flow-
// control window: its producer is credited for a stream no step reads, so the
// run completes instead of stalling on it.
#[tokio::test]
async fn test12729_several_outputs_the_main_one_feeds_the_next_step() {
    let machines = Machines::new(TAG);
    let notation = format!(
        "[test_split {SPLIT}][test_edge2 {EDGE2}][A -> test_split -> B][B -> test_edge2 -> C]"
    );
    // The cartridge is built before the clock starts: the limit below is on
    // the run, and a build is not a stall.
    support::testcartridge_bin(TAG);
    let ran = tokio::time::timeout(
        std::time::Duration::from_secs(120),
        machines.run_with(&notation, &[("A", Given::One(b"alpha?:beta"))], &[(SPLIT, SPLIT_PIECES, b"80")]),
    )
    .await
    .expect("the run stalled: a stream nobody reads was not credited")
    .expect("Execution failed");
    // edge2 appends to what it was fed: the head, not the rest and not the
    // 80 pieces of padding.
    assert_eq!(ran.text("C"), "alpha?[APPEND]");
}

// TEST12796: each output of a step goes to the step that takes it. The head
// feeds one consumer and the rest another; each is given what was emitted on
// its output and nothing of the other, and both results are the machine's.
#[tokio::test]
async fn test12796_two_outputs_feed_two_consumers() {
    let machines = Machines::new(TAG);
    let ran = machines
        .run(
            &format!("{}[A -> split -> (h, rest=r)][h -> edge2 -> x][r -> shout -> y]", dag_headers()),
            &[("A", Given::One(b"left:right"))],
        )
        .await
        .expect("Execution failed");
    assert_eq!(ran.text("x"), "left[APPEND]");
    assert_eq!(ran.text("y"), "RIGHT");

    // An output the machine takes and no step reads is an output of the machine.
    let ran = machines
        .run(
            &format!("{}[A -> split -> (h, rest=r)]", dag_headers()),
            &[("A", Given::One(b"left:right"))],
        )
        .await
        .expect("Execution failed");
    assert_eq!(ran.text("h"), "left");
    assert_eq!(ran.text("r"), "right");
}

// =============================================================================
// Maps
// =============================================================================

// TEST12797: sequences on two args that each take one value are zipped: run
// i of the step is given item i of each. Sequences of different lengths fail
// the run, naming what each gave, before the step is run on anything.
#[tokio::test]
async fn test12797_two_sequences_are_zipped() {
    let machines = Machines::new(TAG);
    let notation = format!(
        "{}[a -> words -> xs][b -> words -> ys][(xs, right=ys) -> pair -> p]",
        dag_headers()
    );
    let ran = machines
        .run(&notation, &[("a", Given::One(b"one two three")), ("b", Given::One(b"x y z"))])
        .await
        .expect("Execution failed");
    assert_eq!(ran.texts("p"), vec!["one|x", "two|y", "three|z"]);

    // The run above is on the same record: what the next one adds is read
    // from here on.
    let before = machines.logs.lock().unwrap().len();
    let error = machines
        .run(&notation, &[("a", Given::One(b"one two three")), ("b", Given::One(b"x y"))])
        .await
        .err()
        .expect("sequences of different lengths cannot be zipped");
    let message = error.to_string();
    assert!(message.contains("gave 3") && message.contains("gave 2"), "{message}");
    assert_eq!(error.attribution_class(), AttributionClass::Input);
    let logs = machines.logs.lock().unwrap();
    assert!(
        logs[before..].iter().any(|(cap, _, _)| cap.contains("test-dag-words")),
        "the steps before the map ran and were heard from: {:?}",
        &logs[before..]
    );
    assert!(
        !logs[before..].iter().any(|(cap, _, _)| cap.contains("test-dag-pair")),
        "the step was not run on any item: {:?}",
        &logs[before..]
    );
}

// TEST12798: a sequence produced inside a map is mapped inside it, and a
// step that takes a sequence folds what the inner map produced once per
// OUTER item. From the machine, what the inner map produced is one flat
// sequence, in order. A map over nothing produces nothing, and its fold is
// given an empty sequence.
#[tokio::test]
async fn test12798_a_map_inside_a_map_and_a_fold_per_outer_item() {
    let machines = Machines::new(TAG);
    let notation = format!(
        "{}[docs -> words -> w][w -> shout -> s][s -> join -> line][(w, right=w) -> pair -> e]",
        dag_headers()
    );
    let docs: [&[u8]; 3] = [b"a b", b"", b"c d e"];
    let ran = machines
        .run(&notation, &[("docs", Given::Many(&docs))])
        .await
        .expect("Execution failed");
    // One line per document — the empty one included: its words are no
    // items, and the join of no items is the empty text.
    assert_eq!(ran.texts("line"), vec!["A+B", "", "C+D+E"]);
    assert_eq!(ran.texts("e"), vec!["a|a", "b|b", "c|c", "d|d", "e|e"]);
}

// TEST12799: an output that may not fire and does not leaves what needs it
// skipped: the step wired to it does not run and is reported skipped, a
// gather it is a member of holds one member fewer, and a machine output
// behind it is not produced — which is neither empty nor a failure. When it
// does fire, all of them have it.
#[tokio::test]
async fn test12799_an_output_that_does_not_fire_skips_and_shrinks() {
    let machines = Machines::new(TAG);
    let notation = format!(
        "{}[A -> split -> (h, flagged=f)][f -> shout -> loud][(h, f) -> join -> j]",
        dag_headers()
    );

    // No '!': `flagged` does not fire.
    let ran = machines.run(&notation, &[("A", Given::One(b"plain:rest"))]).await.expect("Execution failed");
    assert!(!ran.was_produced("loud"));
    assert_eq!(ran.text("j"), "plain", "the gather holds the head alone");
    assert_eq!(ran.result.verdict, RunVerdict::Completed);
    let skipped: Vec<_> = ran
        .result
        .steps
        .iter()
        .filter(|report| matches!(report.outcome, StepOutcome::Skipped { .. }))
        .collect();
    assert_eq!(skipped.len(), 1);
    assert!(skipped[0].cap_urn.contains("test-dag-shout"));

    // With a '!': it fires, and everything that takes it has it.
    let ran = machines.run(&notation, &[("A", Given::One(b"wow!:rest"))]).await.expect("Execution failed");
    assert_eq!(ran.text("loud"), "FLAGGED");
    assert_eq!(ran.text("j"), "wow!+flagged");
    assert!(ran.result.steps.is_empty());
}

// TEST11011: a live source drives a map END TO END through the REAL stack —
// the CLI runtime hosts the actual testcartridge process on a real relay
// switch, the HOST opens the built-in `media:live;synthetic` feed itself
// through the same capture dispatch the hardware backends use, and one run of
// the body happens per delivered item while the feed runs. No mocks anywhere:
// real capture bridge, real map driver, real cartridge invocations, real
// persisted outputs.
#[tokio::test]
async fn test11011_live_synthetic_foreach_region_end_to_end() {
    let machines = Machines::new(TAG);
    let ran = machines
        .run(
            &format!("[test_edge1 {EDGE1}]\n[frames -> test_edge1 -> out]\n"),
            &[(
                "frames",
                Given::Live {
                    reference_urn: "media:live;synthetic",
                    selector: br#"{"params":{"items":3,"interval_ms":0,"item_bytes":4}}"#,
                },
            )],
        )
        .await
        .expect("a live source must drive the map through the real stack");
    // One persisted output per delivered feed item, in item order. The
    // synthetic feed emits [i % 256; item_bytes]; test-edge1 prepends.
    let (items, is_sequence) = ran.produced("out");
    assert!(is_sequence, "what a map produces is a sequence");
    assert_eq!(items.len(), 3, "one output per feed item");
    for (i, bytes) in items.iter().enumerate() {
        let mut expected = b"[PREPEND]".to_vec();
        expected.extend(std::iter::repeat(i as u8).take(4));
        assert_eq!(bytes, &expected, "run {i} was given the feed's actual item bytes");
    }
}

// =============================================================================
// Machines in step position
// =============================================================================

// TEST12815: a machine used as a step of another runs as its own steps, in
// place: each use is given what was bound to its inputs and gives what its
// outputs made, and two uses of one machine do not share anything. Here
// `loud` — words, each in capitals, joined — is run on two inputs, one use
// named throughout and one taking its only output unnamed, and a step of the
// including machine is given both results.
#[tokio::test]
async fn test12815_a_machine_runs_as_a_step_of_another() {
    let machines = Machines::new(TAG);
    let mut library = capdag::MapMachineSource::new();
    library.insert(
        "loud",
        format!("{}[doc -> words -> w][w -> shout -> s][s -> join -> line]", dag_headers()),
    );
    let ran = machines
        .run_including(
            &format!(
                "{}[loud machine \"loud\"][(doc=a) -> loud -> (line=x)][(doc=b) -> loud -> y][(x, right=y) -> pair -> p]",
                dag_headers()
            ),
            &library,
            &[("a", Given::One(b"one two")), ("b", Given::One(b"three"))],
        )
        .await
        .expect("Execution failed");
    assert_eq!(ran.text("p"), "ONE+TWO|THREE");
    assert_eq!(
        ran.plan.nodes.values().filter(|node| node.is_cap()).count(),
        7,
        "three steps for each use, and the step that takes both"
    );
    assert_eq!(ran.result.verdict, RunVerdict::Completed);
}

// =============================================================================
// Scheduling and step policy
// =============================================================================

// TEST12800: steps with no path between them run at the same time. Two
// steps that each take three seconds, and the step that needs both: the run
// takes about three seconds, not six.
#[tokio::test]
async fn test12800_independent_steps_run_at_the_same_time() {
    let machines = Machines::new(TAG);
    let notation = format!(
        "{}[(a, delay-ms=3000) -> shout -> x][(b, delay-ms=3000) -> shout -> y][(x, right=y) -> pair -> p]",
        dag_headers()
    );
    // The cartridge is built before the clock starts.
    support::testcartridge_bin(TAG);
    let started = std::time::Instant::now();
    let ran = machines
        .run(&notation, &[("a", Given::One(b"a")), ("b", Given::One(b"b"))])
        .await
        .expect("Execution failed");
    let elapsed = started.elapsed();
    assert_eq!(ran.text("p"), "A|B");
    assert!(
        elapsed >= std::time::Duration::from_secs(3),
        "each step takes three seconds: {elapsed:?}"
    );
    assert!(
        elapsed < std::time::Duration::from_millis(5500),
        "two three-second steps side by side took {elapsed:?} — as long as one after the other"
    );
}

// TEST12801: `retry=N` attempts a step again after a failure of a retryable
// class. A step that fails twice and then answers completes under `retry=3`,
// with both failed attempts on record, and fails under `retry=1` with the
// failure the cartridge declared. A failure that is the input's fault is not
// attempted again whatever was asked.
#[tokio::test]
async fn test12801_a_retryable_failure_is_attempted_again() {
    let machines = Machines::new(TAG);
    let flaky = |policy: &str, failures: usize, class: &str| {
        // Failures are counted per key, in the cartridge: each run has its own.
        let key = uuid::Uuid::new_v4();
        format!(
            "{}[(a, key=\"{key}\", failures={failures}, class=\"{class}\") -> flaky{policy} -> out]",
            dag_headers()
        )
    };

    let ran = machines
        .run(&flaky("{retry=3}", 2, "resource"), &[("a", Given::One(b"a"))])
        .await
        .expect("two failures are within three retries");
    assert_eq!(ran.text("out"), "a:attempt-3");
    assert_eq!(ran.result.verdict, RunVerdict::Completed);
    match ran.result.steps.as_slice() {
        [report] => match &report.outcome {
            StepOutcome::Completed { failed_attempts } => {
                assert_eq!(failed_attempts.len(), 2);
                assert!(failed_attempts.iter().all(|failure| {
                    failure.class == AttributionClass::Resource && failure.code.as_deref() == Some("FLAKY")
                }));
                assert!(failed_attempts[0].message.contains("attempt 1"), "{:?}", failed_attempts[0]);
                assert!(failed_attempts[1].message.contains("attempt 2"), "{:?}", failed_attempts[1]);
            }
            other => panic!("expected a completion with its attempts, got {other:?}"),
        },
        other => panic!("expected one step on the report, got {other:?}"),
    }

    let error = machines
        .run(&flaky("{retry=1}", 2, "resource"), &[("a", Given::One(b"a"))])
        .await
        .err()
        .expect("two failures are more than one retry");
    assert_eq!(error.failure_code(), Some("FLAKY"));
    assert_eq!(error.attribution_class(), AttributionClass::Resource);
    assert!(error.failure_reason().contains("attempt 2"), "{error}");

    let error = machines
        .run(&flaky("{retry=3}", 1, "input"), &[("a", Given::One(b"a"))])
        .await
        .err()
        .expect("a failure of the input stands");
    assert_eq!(error.attribution_class(), AttributionClass::Input);
    assert!(error.failure_reason().contains("attempt 1"), "it was asked once: {error}");
}

// TEST12802: `on_error=absent` tolerates a step that failed for good. In a
// map, the item it failed on is dropped and the others are produced; the run
// ends COMPLETED WITH TOLERATED FAILURES, with the failure — its class, its
// code, its message — on record. Without the policy the same failure fails
// the run.
#[tokio::test]
async fn test12802_a_tolerated_failure_drops_its_item_and_is_said() {
    let machines = Machines::new(TAG);
    let notation = |policy: &str| {
        format!(
            "{}[doc -> words -> w][(w, fail-on=\"bad\") -> shout{policy} -> s]",
            dag_headers()
        )
    };

    let ran = machines
        .run(&notation("{on_error=absent}"), &[("doc", Given::One(b"good bad fine"))])
        .await
        .expect("the failure is tolerated");
    assert_eq!(ran.texts("s"), vec!["GOOD", "FINE"]);
    assert_eq!(ran.result.verdict, RunVerdict::CompletedWithToleratedFailures);
    let tolerated: Vec<_> = ran
        .result
        .steps
        .iter()
        .filter_map(|report| match &report.outcome {
            StepOutcome::FailedTolerated { failure, .. } => Some((report, failure)),
            _ => None,
        })
        .collect();
    assert_eq!(tolerated.len(), 1);
    let (report, failure) = tolerated[0];
    assert_eq!(failure.class, AttributionClass::Input);
    assert_eq!(failure.code.as_deref(), Some("SHOUT_REFUSED"));
    assert!(failure.message.contains("'bad'"), "{failure:?}");
    assert_eq!(report.body.as_ref().expect("in the map").body_index, 1);

    let error = machines
        .run(&notation(""), &[("doc", Given::One(b"good bad fine"))])
        .await
        .err()
        .expect("nothing tolerates the failure");
    assert_eq!(error.failure_code(), Some("SHOUT_REFUSED"));
    assert_eq!(error.attribution_class(), AttributionClass::Input);
}

// =============================================================================
// Peer calls
// =============================================================================

// TEST394: a cap that calls other caps while it runs: test-peer invokes
// test-edge1 and then test-edge2 through its host, which routes both back to
// the cartridge. The step's result is what the two calls made of its input.
#[tokio::test]
async fn test394_peer_invoke_roundtrip() {
    let machines = Machines::new(TAG);
    let ran = machines
        .run(&format!("[test_peer {PEER}]\n[A -> test_peer -> B]\n"), &[("A", Given::One(b"CHAIN"))])
        .await
        .expect("Execution failed");
    assert_eq!(ran.text("B"), "[PREPEND]CHAIN[APPEND]");
}
