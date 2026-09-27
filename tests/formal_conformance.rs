//! TEST12166: matching, specificity, dispatch and acceptance are the proved model's.
//!
//! The decisions are generated from `../formal`, so this does not test the
//! rules — those are proved — but everything around them: the media and cap
//! URN parsers, the stored-value encoding, and how a `CapUrn` hands itself to
//! the generated code. Every row of `../formal/conformance.json` (written by
//! the model, `lake exe conformance`) is parsed here and must get the model's
//! verdict. The same table runs in every mirror.

use capdag::{CapUrn, MediaUrn};

#[test]
fn test12166_the_implementation_is_the_proved_model() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../formal/conformance.json");
    let table: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("the model's table"))
            .expect("json");
    let text = |v: &serde_json::Value| v.as_str().unwrap().to_string();
    let mut wrong = Vec::new();

    let refines = table["refines"].as_array().unwrap();
    for r in refines {
        let a = MediaUrn::from_string(&text(&r["instance"])).unwrap();
        let b = MediaUrn::from_string(&text(&r["pattern"])).unwrap();
        let got = a.conforms_to(&b).unwrap();
        if got != r["refines"].as_bool().unwrap() {
            wrong.push(format!("{a} ⪯ {b}: model {}, got {got}", r["refines"]));
        }
    }
    let scores = table["scores"].as_array().unwrap();
    for r in scores {
        let u = MediaUrn::from_string(&text(&r["urn"])).unwrap();
        let got = u.inner().specificity() as u64;
        if got != r["score"].as_u64().unwrap() {
            wrong.push(format!("score {u}: model {}, got {got}", r["score"]));
        }
    }
    let dispatch = table["dispatch"].as_array().unwrap();
    for r in dispatch {
        let c = CapUrn::from_string(&text(&r["candidate"])).unwrap();
        let q = CapUrn::from_string(&text(&r["request"])).unwrap();
        let got = c.is_dispatchable(&q);
        if got != r["dispatch"].as_bool().unwrap() {
            wrong.push(format!("{c} serves {q}: model {}, got {got}", r["dispatch"]));
        }
        let got = c.accepts(&q);
        if got != r["accepts"].as_bool().unwrap() {
            wrong.push(format!("{c} accepts {q}: model {}, got {got}", r["accepts"]));
        }
    }
    assert!(
        refines.len() > 4000 && dispatch.len() > 20000,
        "the table is the full one"
    );
    assert!(
        wrong.is_empty(),
        "{} row(s) differ from the model, e.g.\n  {}",
        wrong.len(),
        wrong[..wrong.len().min(8)].join("\n  ")
    );
}
