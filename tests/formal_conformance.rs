//! TEST12166: every answer about media and caps is the proved model's.
//!
//! The decisions are generated from `../formal`, so this does not test the
//! rules — those are proved — but everything around them: the media and cap
//! URN parsers, the stored-value encoding, and how a `CapUrn` or a `CapQuery`
//! hands itself to the generated code. Every row of `../formal/conformance.json`
//! (written by the model, `lake exe conformance`) is parsed here and must get
//! the model's verdict: between media, the guarantee, the possibility and the
//! complete reading; between caps, serving, could-serve and the grade of a typed
//! call, fitting a pattern, being the same cap, and flowing into one
//! another. The same table runs in every mirror.

use capdag::input_resolver::detection::{
    primary_media, settle, surviving_candidates, AdapterVerdict, FileMediaCandidate,
};
use capdag::input_resolver::value_adapter_registry::ValueAdapterRegistry;
use capdag::media::spec::MediaValidation;
use capdag::{CapQuery, CapUrn, CapUrnError, MatchGrade, MediaUrn};

fn model_table() -> serde_json::Value {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../formal/conformance.json");
    serde_json::from_str(&std::fs::read_to_string(path).expect("the model's table")).expect("json")
}

fn section<'t>(table: &'t serde_json::Value, name: &str) -> &'t Vec<serde_json::Value> {
    table[name]
        .as_array()
        .unwrap_or_else(|| panic!("the model's table has no '{name}' section"))
}

fn conclude(what: &str, total: usize, wrong: Vec<String>) {
    assert!(total > 0, "the model's table has no {what} rows");
    assert!(
        wrong.is_empty(),
        "{} of {total} {what} row(s) differ from the model, e.g.\n  {}",
        wrong.len(),
        wrong[..wrong.len().min(8)].join("\n  ")
    );
}

// TEST7123: the cap URN parser refuses a cap exactly when the model says it
// cannot be declared, and for the reason the model gives: the bare top form,
// or an effect=none cap whose input is not inside its output. Every other cap
// over every effect — patch included — parses.
#[test]
fn test7123_admissibility_is_the_models() {
    let table = model_table();
    let rows = section(&table, "admissible");
    let mut wrong = Vec::new();
    for r in rows {
        let text = r["cap"].as_str().unwrap();
        let got = CapUrn::from_string(text);
        match (r["illegal"].as_str(), &got) {
            (None, Ok(_)) => {}
            (Some("bare_top"), Err(CapUrnError::IllegalDeclaration(m))) if m.starts_with("illegal bare top cap") => {}
            (Some("none_not_inside"), Err(CapUrnError::IllegalDeclaration(m)))
                if m.starts_with("effect=none requires declared input") => {}
            (model, got) => wrong.push(format!("{text}: model {model:?}, got {got:?}")),
        }
    }
    assert_eq!(rows.len(), 500, "every cap over the directions, cap-tags and effects");
    conclude("admissibility", rows.len(), wrong);
}

// TEST7124: what a file is, decided through the detection module as a host
// runs it: which candidates stand, given what their rules say of the content,
// the consulted adapters and what their cartridges claimed; which of those
// each line of refinement keeps; and which of the kept a host that cannot ask
// takes. The content is one byte, so a rule of at least one byte is met and a
// rule of at least two is not.
#[test]
fn test7124_detection_is_the_models() {
    let table = model_table();
    let rows = section(&table, "detect");
    let content = b"x";
    let media = |v: &serde_json::Value| MediaUrn::from_string(v.as_str().unwrap()).unwrap();
    let media_list = |v: &serde_json::Value| v.as_array().unwrap().iter().map(media).collect::<Vec<_>>();
    let mut wrong = Vec::new();
    for r in rows {
        let candidates: Vec<FileMediaCandidate> = r["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| FileMediaCandidate {
                urn: media(&c["urn"]),
                validation: match c["rules"].as_str().unwrap() {
                    "unruled" => None,
                    "met" => Some(MediaValidation { min_length: Some(1), ..Default::default() }),
                    "unmet" => Some(MediaValidation { min_length: Some(2), ..Default::default() }),
                    other => panic!("the model's table names no rules '{other}'"),
                },
            })
            .collect();
        let verdict = AdapterVerdict { owners: media_list(&r["owners"]), claimed: media_list(&r["claims"]) };
        let survivors = surviving_candidates(content, &candidates, &verdict).unwrap();
        let stands: Vec<bool> = candidates.iter().map(|c| survivors.contains(&c.urn)).collect();
        let expected: Vec<bool> = r["stands"].as_array().unwrap().iter().map(|b| b.as_bool().unwrap()).collect();
        if stands != expected {
            wrong.push(format!("{r}: stands {stands:?}"));
            continue;
        }
        let kept = settle(&survivors, &ValueAdapterRegistry::new(), content).unwrap();
        let most_specific: Vec<bool> = survivors.iter().map(|u| kept.contains(&u.to_string())).collect();
        let expected: Vec<bool> =
            r["most_specific"].as_array().unwrap().iter().map(|b| b.as_bool().unwrap()).collect();
        if most_specific != expected || kept.len() != expected.iter().filter(|k| **k).count() {
            wrong.push(format!("{r}: kept {kept:?}"));
            continue;
        }
        let primary = primary_media(&kept).unwrap();
        let expected = r["primary"].as_u64().map(|i| kept[i as usize].as_str());
        if primary != expected {
            wrong.push(format!("{r}: primary {primary:?}"));
        }
    }
    assert_eq!(rows.len(), (9 + 81) * 2 * 4, "every rule, owner and claim set over both lists");
    conclude("detection", rows.len(), wrong);
}

#[test]
fn test12166_the_implementation_is_the_proved_model() {
    let table = model_table();
    let text = |v: &serde_json::Value| v.as_str().unwrap().to_string();
    let mut wrong = Vec::new();

    let refines = table["refines"].as_array().unwrap();
    for r in refines {
        let a = MediaUrn::from_string(&text(&r["instance"])).unwrap();
        let b = MediaUrn::from_string(&text(&r["pattern"])).unwrap();
        let got = a.conforms_to(&b);
        if got != r["refines"].as_bool().unwrap() {
            wrong.push(format!("{a} ⪯ {b}: model {}, got {got}", r["refines"]));
        }
        for (name, got) in [
            ("meets", a.meets(&b)),
            ("satisfies", a.satisfies(&b)),
            ("may_satisfy", a.may_satisfy(&b)),
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
        let request = CapQuery::from_call(&q);
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
