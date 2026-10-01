//! TEST12166: every answer about media and caps is the proved model's.
//!
//! The decisions are generated from `../formal`, so this does not test the
//! rules — those are proved — but everything around them: the media and cap
//! URN parsers, the stored-value encoding, and how a `CapUrn` or a `CapQuery`
//! hands itself to the generated code. Every row of `../formal/conformance.json`
//! (written by the model, `lake exe conformance`) is parsed here and must get
//! the model's verdict: between media, the guarantee, the possibility and the
//! complete reading; between caps, serving, could-serve and the grade of a
//! request, fitting a pattern, being the same cap, and flowing into one
//! another. The same table runs in every mirror.

use capdag::{CapQuery, CapUrn, MatchGrade, MediaUrn};

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
        for (name, got) in [
            ("meets", a.meets(&b).unwrap()),
            ("satisfies", a.satisfies(&b).unwrap()),
            ("may_satisfy", a.may_satisfy(&b).unwrap()),
        ] {
            if got != r[name].as_bool().unwrap() {
                wrong.push(format!("{a} {name} {b}: model {}, got {got}", r[name]));
            }
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
        let request = CapQuery::from_request(&q);
        let grade = match request.grade(&c) {
            MatchGrade::Exact => "exact",
            MatchGrade::Guaranteed => "guaranteed",
            MatchGrade::Possible => "possible",
            MatchGrade::None => "none",
        };
        if grade != r["grade"].as_str().unwrap() {
            wrong.push(format!("grade of {c} for {q}: model {}, got {grade}", r["grade"]));
        }
        for (name, got) in [
            ("dispatch", c.is_dispatchable(&q)),
            ("dispatch", request.admits(&c)),
            ("may_dispatch", c.may_dispatch(&q)),
            ("may_dispatch", request.may_admit(&c)),
            ("accepts", c.accepts(&q)),
            ("accepts", CapQuery::from_pattern(&c).admits(&q)),
            ("accepts", q.conforms_to(&c)),
            ("equivalent", c.is_equivalent(&q)),
            ("flows", c.flows_into(&q)),
        ] {
            if got != r[name].as_bool().unwrap() {
                wrong.push(format!("{name}: {c} / {q}: model {}, got {got}", r[name]));
            }
        }
    }
    assert!(
        refines.len() > 4000 && dispatch.len() > 30000,
        "the table is the full one"
    );
    assert!(
        wrong.is_empty(),
        "{} row(s) differ from the model, e.g.\n  {}",
        wrong.len(),
        wrong[..wrong.len().min(8)].join("\n  ")
    );
}
