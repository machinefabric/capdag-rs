//! The runtime's objects, replayed against the proved model's scripts.
//!
//! The decisions these objects make are the model's generated code
//! (`formal/CapDAG/Bifaci`), so this does not test the rules — those are
//! proved. It tests everything around them: that a `CreditGate`, a
//! `CreditWindow`, a `ReorderBuffer`, the writer's gate and a `RequestTable`
//! carry the decisions out as the model means them — their counters, their
//! containers, the order they do things in.
//!
//! `../formal/conformance-bifaci.json` is written by the model
//! (`lake exe conformance_bifaci`): for each machine, short scripts of
//! operations and, for each operation, what the model says happened. The same
//! scripts run in every mirror.

use crate::bifaci::cartridge_runtime::{write_gated, GatedWrite};
use crate::bifaci::credit::{negotiate_initial_credit, CreditGate, CreditRouter, CreditWindow};
use crate::bifaci::frame::{
    CreditDirection, Frame, FrameType, Limits, MessageId, ReorderBuffer, SeqAssigner,
};
use crate::bifaci::request_state::{
    Disposition, FrameDirection, RequestState, RequestTable, RoutingEntry, TerminalKind,
};
use serde_json::Value;
use std::sync::{Arc, OnceLock};

/// The model's table, read once.
pub(crate) fn table() -> &'static Value {
    static TABLE: OnceLock<Value> = OnceLock::new();
    TABLE.get_or_init(|| {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../formal/conformance-bifaci.json");
        serde_json::from_str(&std::fs::read_to_string(path).expect("the model's table"))
            .expect("the model's table is JSON")
    })
}

pub(crate) fn rows(section: &str) -> &'static Vec<Value> {
    table()[section]
        .as_array()
        .unwrap_or_else(|| panic!("the model's table has no '{section}' section"))
}

pub(crate) fn uint(value: &Value) -> u64 {
    value.as_u64().unwrap_or_else(|| panic!("{value} is not a count"))
}

fn text(value: &Value) -> String {
    value.as_str().unwrap_or_else(|| panic!("{value} is not text")).to_string()
}

fn optional_text(value: &Value) -> Option<String> {
    if value.is_null() { None } else { Some(text(value)) }
}

/// Fail naming every script that went wrong, and how many of how many.
pub(crate) fn conclude(what: &str, total: usize, wrong: Vec<String>) {
    assert!(total > 0, "the model's table has no {what} scripts");
    assert!(
        wrong.is_empty(),
        "{} of {total} {what} scripts disagree with the model; first: {}",
        wrong.len(),
        wrong[0]
    );
}

// TEST12375: every frame type is the type the model means by its number, part
// of a flow exactly when the model says, and an end exactly when the model
// says. The runtime's own `FrameType` is mapped onto the model's by hand; a
// type mapped to the wrong one would be numbered, ordered and gated as another.
#[test]
fn test12375_frame_types_are_the_models() {
    let table = rows("frame_types");
    assert_eq!(table.len(), FrameType::ALL.len(), "the model and the runtime name the same types");
    let mut wrong = Vec::new();
    for row in table {
        let code = uint(&row["code"]) as u8;
        let Some(frame_type) = FrameType::from_u8(code) else {
            wrong.push(format!("wire number {code} names no frame type here"));
            continue;
        };
        let model_code = crate::formal::bifaci::flow::frame_type::code(frame_type.model());
        if model_code.to_u64() != Some(u64::from(code)) {
            wrong.push(format!("{frame_type:?} ({code}) is mapped to the model's type {model_code}"));
        }
        if frame_type.is_flow() != row["flow"].as_bool().unwrap() {
            wrong.push(format!("{frame_type:?}: flow is {}", frame_type.is_flow()));
        }
        if frame_type.is_terminal() != row["terminal"].as_bool().unwrap() {
            wrong.push(format!("{frame_type:?}: terminal is {}", frame_type.is_terminal()));
        }
        let ends = TerminalKind::of_frame(frame_type).map(|kind| kind.as_str().to_string());
        if ends != optional_text(&row["ends"]) {
            wrong.push(format!("{frame_type:?}: ends its request as {ends:?}, model {}", row["ends"]));
        }
        if Frame::new(frame_type, MessageId::Uint(1)).is_flow_frame() != frame_type.is_flow() {
            wrong.push(format!("{frame_type:?}: a frame and its type disagree on being of a flow"));
        }
    }
    conclude("frame type", table.len(), wrong);
}

// TEST12376: a credit gate answers every acquire, grant and close as the model
// does, and holds what the model says it holds afterwards.
#[test]
fn test12376_credit_gate_follows_the_model() {
    let scripts = rows("gate");
    let mut wrong = Vec::new();
    for script in scripts {
        let gate = CreditGate::new(uint(&script["window"]));
        let ops = script["ops"].as_array().unwrap();
        let steps = script["steps"].as_array().unwrap();
        for (index, (op, step)) in ops.iter().zip(steps).enumerate() {
            let answer = match op["op"].as_str().unwrap() {
                "acquire" => Some(match gate.try_acquire(uint(&op["n"])) {
                    Ok(true) => "acquired".to_string(),
                    Ok(false) => "wait".to_string(),
                    Err(closed) => format!("closed:{}", closed.reason),
                }),
                "grant" => {
                    gate.grant(uint(&op["n"]));
                    None
                }
                "close" => {
                    gate.close(op["reason"].as_str().unwrap());
                    None
                }
                other => panic!("unknown gate operation '{other}'"),
            };
            let closed = gate.is_closed();
            if answer != optional_text(&step["answer"])
                || gate.available() != uint(&step["available"])
                || closed != !step["closed"].is_null()
            {
                wrong.push(format!(
                    "step {index} of {script}: answered {answer:?}, available {}, closed {closed}",
                    gate.available()
                ));
                break;
            }
        }
    }
    conclude("credit gate", scripts.len(), wrong);
}

// TEST12377: a credit window accepts, refuses and grants as the model does: an
// arriving chunk is a violation exactly when nothing is left of the window, a
// grant is due exactly when a batch has built up, a flush grants what is
// pending, and a continued chunk is granted back at once.
#[test]
fn test12377_credit_window_follows_the_model() {
    let scripts = rows("window");
    let mut wrong = Vec::new();
    for script in scripts {
        let window = CreditWindow::new(uint(&script["window"]));
        let ops = script["ops"].as_array().unwrap();
        let steps = script["steps"].as_array().unwrap();
        for (index, (op, step)) in ops.iter().zip(steps).enumerate() {
            let (violation, grant) = match op.as_str().unwrap() {
                "arrive" => (window.arrive().is_err(), 0),
                "continuation" => match window.arrive() {
                    Ok(()) => (false, window.continued()),
                    Err(_) => (true, 0),
                },
                "consume" => (false, window.consumed().unwrap_or(0)),
                "flush" => (false, window.flush().unwrap_or(0)),
                other => panic!("unknown window operation '{other}'"),
            };
            if violation != step["violation"].as_bool().unwrap()
                || grant != uint(&step["grant"])
                || window.remaining() != uint(&step["remaining"])
                || window.pending() != uint(&step["pending"])
            {
                wrong.push(format!(
                    "step {index} of {script}: violation {violation}, grant {grant}, remaining {}, pending {}",
                    window.remaining(),
                    window.pending()
                ));
                break;
            }
        }
    }
    conclude("credit window", scripts.len(), wrong);
}

// TEST12378: the window two ends start with is the smaller proposal, and a
// proposal of zero is refused — it would deadlock every stream at its first
// chunk.
#[test]
fn test12378_a_zero_window_is_refused() {
    let table = rows("negotiate");
    let mut wrong = Vec::new();
    for row in table {
        let negotiated = negotiate_initial_credit(uint(&row["ours"]), uint(&row["theirs"]));
        let expected = row["window"].as_u64();
        if negotiated != expected {
            wrong.push(format!("{row}: negotiated {negotiated:?}"));
        }
    }
    conclude("negotiation", table.len(), wrong);
}

// TEST12379: a grant credits the stream it names, or — naming none — the
// request's only sending stream; a grant that names a stream the request does
// not have, or names none among several, credits nothing.
#[test]
fn test12379_a_grant_reaches_the_stream_it_is_for() {
    let table = rows("grant_target");
    let mut wrong = Vec::new();
    for row in table {
        let rid = MessageId::Uint(7);
        let router = CreditRouter::new();
        let streams: Vec<Option<String>> =
            row["streams"].as_array().unwrap().iter().map(optional_text).collect();
        let gates: Vec<Arc<CreditGate>> = streams
            .iter()
            .map(|stream| {
                let gate = Arc::new(CreditGate::new(0));
                router.register(rid.clone(), stream.clone(), Arc::clone(&gate));
                gate
            })
            .collect();
        // A gate of ANOTHER request with the same stream ids must never be credited.
        let stranger = Arc::new(CreditGate::new(0));
        router.register(MessageId::Uint(8), optional_text(&row["named"]), Arc::clone(&stranger));

        let named = optional_text(&row["named"]);
        let frame = Frame::credit(rid.clone(), named, 3, CreditDirection::Response);
        let matched = router.grant(&frame);
        let credited: Vec<Option<String>> = streams
            .iter()
            .zip(&gates)
            .filter(|(_, gate)| gate.available() == 3)
            .map(|(stream, _)| stream.clone())
            .collect();
        let expected: Vec<Option<String>> = if row["matched"].as_bool().unwrap() {
            vec![optional_text(&row["target"])]
        } else {
            Vec::new()
        };
        if matched != row["matched"].as_bool().unwrap() || credited != expected || stranger.available() != 0 {
            wrong.push(format!("{row}: matched {matched}, credited {credited:?}"));
        }
    }
    conclude("grant routing", table.len(), wrong);
}

// TEST12380: frames arriving out of order are handed on in the order they were
// written — each arrival delivers exactly what the model says, holds what it
// says, and is refused when it says: a number already handed on, one already
// held, or one more than the buffer may hold.
#[test]
fn test12380_reorder_buffer_follows_the_model() {
    let scripts = rows("reorder");
    let mut wrong = Vec::new();
    for script in scripts {
        let mut buffer = ReorderBuffer::new(uint(&script["limit"]) as usize);
        let arrivals = script["arrivals"].as_array().unwrap();
        let steps = script["steps"].as_array().unwrap();
        for (index, (arrival, step)) in arrivals.iter().zip(steps).enumerate() {
            let mut frame = Frame::new(FrameType::Log, MessageId::Uint(1));
            frame.seq = uint(arrival);
            let outcome = buffer.accept(frame);
            let agrees = match (&outcome, step.get("deliver"), step.get("hold"), step.get("error")) {
                (Ok(delivered), Some(seqs), _, _) => {
                    let got: Vec<u64> = delivered.iter().map(|f| f.seq).collect();
                    let want: Vec<u64> = seqs.as_array().unwrap().iter().map(uint).collect();
                    got == want
                }
                (Ok(delivered), None, Some(_), _) => delivered.is_empty(),
                (Err(error), None, None, Some(kind)) => {
                    let message = error.to_string();
                    match kind.as_str().unwrap() {
                        "stale" => message.contains("stale/duplicate seq: expected"),
                        "duplicate" => message.contains("already buffered"),
                        "overflow" => message.contains("reorder buffer overflow"),
                        other => panic!("unknown reorder refusal '{other}'"),
                    }
                }
                _ => false,
            };
            if !agrees {
                wrong.push(format!("step {index} of {script}: {outcome:?}"));
                break;
            }
        }
    }
    conclude("reorder", scripts.len(), wrong);
}

/// A frame of one type for one request, as a writer is handed it.
fn frame_of(frame_type: FrameType, rid: &MessageId) -> Frame {
    match frame_type {
        FrameType::Chunk => {
            let payload = vec![1u8];
            let checksum = Frame::compute_checksum(&payload);
            Frame::chunk(rid.clone(), "s".to_string(), 0, payload, 0, checksum)
        }
        FrameType::Log => Frame::progress(rid.clone(), 0.5, "working"),
        FrameType::End => Frame::end_ok_with(rid.clone(), None, Some(1.0), None),
        FrameType::Err => Frame::err(
            rid.clone(),
            "FAILED",
            crate::failure::AttributionClass::Internal,
            "it failed",
            None,
        ),
        FrameType::Credit => Frame::credit(rid.clone(), None, 1, CreditDirection::Response),
        other => panic!("the writer scripts hand over no {other:?} frame"),
    }
}

// TEST12381: the writer writes exactly the frames the model says: everything
// until the flow's END or ERR, then nothing of the flow — while credit, which
// is not of the flow, still passes. What reaches the wire has one end and no
// flow frame after it, and the flow's numbers are 0, 1, 2, … without a gap.
#[test]
fn test12381_the_writer_gate_follows_the_model() {
    let scripts = rows("writer");
    let mut wrong = Vec::new();
    for script in scripts {
        let rid = MessageId::Uint(1);
        let limits = Limits::default();
        let mut wire: Vec<u8> = Vec::new();
        let mut seq = SeqAssigner::new();
        let mut terminated = crate::bifaci::stats::TerminatedFlows::new(16);
        let stragglers = crate::bifaci::stats::StragglerCounters::new();
        let frames = script["frames"].as_array().unwrap();
        let expected: Vec<bool> =
            script["written"].as_array().unwrap().iter().map(|w| w.as_bool().unwrap()).collect();
        let mut written = Vec::new();
        for code in frames {
            let frame_type = FrameType::from_u8(uint(code) as u8).unwrap();
            written.push(matches!(
                write_gated(
                    frame_of(frame_type, &rid),
                    &mut wire,
                    &limits,
                    &mut seq,
                    &mut terminated,
                    &stragglers
                ),
                GatedWrite::Written
            ));
        }
        if written != expected {
            wrong.push(format!("{script}: written {written:?}"));
            continue;
        }
        // What is on the wire: the flow's frames are numbered without a gap.
        let on_wire = decode_wire(&wire);
        let flow_seqs: Vec<u64> =
            on_wire.iter().filter(|f| f.is_flow_frame()).map(|f| f.seq).collect();
        if flow_seqs != (0..flow_seqs.len() as u64).collect::<Vec<_>>() {
            wrong.push(format!("{script}: flow frames numbered {flow_seqs:?}"));
        }
    }
    conclude("writer", scripts.len(), wrong);
}

/// Decode every length-prefixed frame from a captured wire buffer.
fn decode_wire(buf: &[u8]) -> Vec<Frame> {
    let mut frames = Vec::new();
    let mut pos = 0;
    while pos + 4 <= buf.len() {
        let len = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        frames.push(
            crate::bifaci::io::decode_frame(&buf[pos..pos + len])
                .expect("wire buffer must hold valid frames"),
        );
        pos += len;
    }
    assert_eq!(pos, buf.len(), "trailing bytes on the wire");
    frames
}

/// The ids a table script names, as message ids.
fn id(name: &str) -> MessageId {
    MessageId::Uint(match name {
        "x1" => 1,
        "x2" => 2,
        "r1" => 101,
        "r2" => 102,
        "r3" => 103,
        other => panic!("the table scripts name no id '{other}'"),
    })
}

// TEST12382: a request table registers a request once, ends it once, keeps no
// state for it afterwards, and tells a frame that crossed a request's end from
// a frame for a request nobody knew — step for step as the model's table does,
// including when the ring of ended requests is full and the oldest is forgotten.
#[test]
fn test12382_request_table_follows_the_model() {
    let scripts = rows("table");
    let mut wrong = Vec::new();
    for script in scripts {
        let mut table = RequestTable::with_recent_capacity(uint(&script["keep"]) as usize);
        let ops = script["ops"].as_array().unwrap();
        let steps = script["steps"].as_array().unwrap();
        let mut agreed = true;
        for (index, (op, step)) in ops.iter().zip(steps).enumerate() {
            let key = (id(op["xid"].as_str().unwrap()), id(op["rid"].as_str().unwrap()));
            let ok = match op["op"].as_str().unwrap() {
                "register" => table
                    .register(
                        key.clone(),
                        RequestState::new(
                            RoutingEntry { source_master_idx: None, destination_master_idx: 0 },
                            None,
                            None,
                            false,
                            32,
                        ),
                    )
                    .is_ok(),
                "terminate" => {
                    let ended = table.terminate(&key, TerminalKind::End).is_some();
                    if ended && (table.contains(&key) || table.xid_for_rid(&key.1).is_some()) {
                        wrong.push(format!("step {index} of {script}: state remains after the end"));
                        agreed = false;
                        break;
                    }
                    ended
                }
                other => panic!("unknown table operation '{other}'"),
            };
            let frames: Vec<&str> = ["r1", "r2", "r3"]
                .iter()
                .map(|rid| match table.disposition(&id(rid)) {
                    Disposition::Route => "route",
                    Disposition::Straggler => "straggler",
                    Disposition::NoRoute => "no_route",
                })
                .collect();
            let expected: Vec<&str> =
                step["frames"].as_array().unwrap().iter().map(|f| f.as_str().unwrap()).collect();
            if ok != step["ok"].as_bool().unwrap() || frames != expected {
                wrong.push(format!("step {index} of {script}: ok {ok}, frames {frames:?}"));
                agreed = false;
                break;
            }
        }
        if agreed
            && (table.len() as u64 != uint(&script["live"])
                || table.total_registered() != uint(&script["registered"]))
        {
            wrong.push(format!(
                "{script}: {} live, {} registered",
                table.len(),
                table.total_registered()
            ));
        }
    }
    conclude("request table", scripts.len(), wrong);

    // The ledger a request keeps of each stream's window moves as the model's
    // does: one less for a chunk, more by a grant, and by nothing else.
    for row in rows("ledger") {
        let rid = MessageId::Uint(9);
        let mut state = RequestState::new(
            RoutingEntry { source_master_idx: None, destination_master_idx: 0 },
            None,
            None,
            false,
            uint(&row["remaining"]),
        );
        let frame_type = FrameType::from_u8(uint(&row["frame"]) as u8).unwrap();
        let mut frame = match frame_type {
            FrameType::Credit => {
                Frame::credit(rid.clone(), None, uint(&row["granted"]), CreditDirection::Response)
            }
            other => Frame::new(other, rid.clone()),
        };
        frame.stream_id = Some("s".to_string());
        let mut table = RequestTable::new();
        let key = (MessageId::Uint(1), rid);
        state.cap_urn = None;
        table.register(key.clone(), state).unwrap();
        table.record_frame(&key, FrameDirection::Inbound, &frame);
        let after = table.get(&key).unwrap().streams[&Some("s".to_string())].credit_outstanding;
        assert_eq!(after, row["after"].as_i64().unwrap(), "the ledger after {row}");
    }
}

// TEST12383: a pool's limit is the smaller of the operator's number and what
// the cartridge reports, with zero meaning no limit in both and in the result;
// and a cartridge that is not running is given one request, through `all`.
#[test]
fn test12383_pool_limits_are_the_models() {
    use crate::bifaci::pools::{advertised_capacity, effective_capacity, PoolState, POOL_ALL};
    let table = rows("effective");
    let mut wrong = Vec::new();
    for row in table {
        let configured = uint(&row["configured"]);
        let available = row["available"].as_u64();
        let state = PoolState { available, ..PoolState::declared(configured, Vec::new()) };
        let effective = effective_capacity(configured, available);
        if effective != uint(&row["effective"])
            || state.effective() != effective
            || advertised_capacity(true, POOL_ALL, &state) != effective
            || advertised_capacity(false, POOL_ALL, &state) != uint(&row["cold_all"])
            || advertised_capacity(false, "gpu", &state) != uint(&row["cold_other"])
        {
            wrong.push(format!("{row}: effective {effective}"));
        }
    }
    conclude("pool limit", table.len(), wrong);
}
