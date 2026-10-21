//! Plans to draw in tests: the topologies the uikit's own run-scene tests
//! draw (`test/test-run-plan-scene.mjs`, `src/fixtures/index.ts`), built as
//! [`PlanView`]s so they need no registry.

use crate::orchestrator::{
    PlanView, PlanViewConstant, PlanViewEdge, PlanViewEdgeKind, PlanViewKind, PlanViewMedia,
    PlanViewNode, PlanViewStep,
};
use crate::planner::{ForEachSource, StepToken};
use crate::MediaUrn;

fn media(urn: &str, title: &str) -> Option<PlanViewMedia> {
    Some(PlanViewMedia {
        urn: MediaUrn::from_string(urn).expect(urn),
        title: title.to_string(),
    })
}

fn token(raw: &str) -> StepToken {
    StepToken::parse(raw).expect(raw)
}

pub fn input(id: &str, name: &str, urn: &str, title: &str, sequence: bool) -> PlanViewNode {
    PlanViewNode {
        id: id.to_string(),
        label: Some(name.to_string()),
        media: media(urn, title),
        enclosing_map: None,
        kind: PlanViewKind::Input {
            name: name.to_string(),
            sequence,
        },
    }
}

#[allow(clippy::too_many_arguments)]
pub fn step(
    id: &str,
    token_id: &str,
    cap: &str,
    title: &str,
    label: Option<&str>,
    urn: &str,
    display: &str,
    enclosing: Option<&str>,
) -> PlanViewNode {
    PlanViewNode {
        id: id.to_string(),
        label: label.map(str::to_string),
        media: media(urn, display),
        enclosing_map: enclosing.map(str::to_string),
        kind: PlanViewKind::Step(PlanViewStep {
            token_id: token(token_id),
            cap_urn: cap.to_string(),
            title: title.to_string(),
            retry: 0,
            on_error_absent: false,
            constants: Vec::new(),
        }),
    }
}

pub fn step_output(id: &str, step: &str, name: &str, label: &str, urn: &str, display: &str) -> PlanViewNode {
    PlanViewNode {
        id: id.to_string(),
        label: Some(label.to_string()),
        media: media(urn, display),
        enclosing_map: None,
        kind: PlanViewKind::StepOutput {
            step_node: step.to_string(),
            output_name: name.to_string(),
            output_media_urn: urn.to_string(),
            carries_input: false,
        },
    }
}

pub fn map(id: &str, token_id: &str, sources: &[(&str, &str)], body: &[&str], enclosing: Option<&str>) -> PlanViewNode {
    PlanViewNode {
        id: id.to_string(),
        label: None,
        media: None,
        enclosing_map: enclosing.map(str::to_string),
        kind: PlanViewKind::Map {
            token_id: token(token_id),
            sources: sources
                .iter()
                .map(|(node, item)| ForEachSource {
                    node: node.to_string(),
                    item: item.to_string(),
                })
                .collect(),
            body: body.iter().map(|s| s.to_string()).collect(),
        },
    }
}

pub fn item(id: &str, map: &str, source: &str, urn: &str, display: &str) -> PlanViewNode {
    PlanViewNode {
        id: id.to_string(),
        label: None,
        media: media(urn, display),
        enclosing_map: Some(map.to_string()),
        kind: PlanViewKind::Item {
            map: map.to_string(),
            source: source.to_string(),
        },
    }
}

pub fn gather(id: &str, members: &[&str], urn: &str, display: &str) -> PlanViewNode {
    PlanViewNode {
        id: id.to_string(),
        label: None,
        media: media(urn, display),
        enclosing_map: None,
        kind: PlanViewKind::Gather {
            members: members.iter().map(|s| s.to_string()).collect(),
        },
    }
}

pub fn output(id: &str, name: &str, source: &str, urn: &str, display: &str) -> PlanViewNode {
    PlanViewNode {
        id: id.to_string(),
        label: Some(name.to_string()),
        media: media(urn, display),
        enclosing_map: None,
        kind: PlanViewKind::Output {
            name: name.to_string(),
            source_node: source.to_string(),
        },
    }
}

pub fn direct(from: &str, to: &str) -> PlanViewEdge {
    PlanViewEdge {
        from_node: from.to_string(),
        to_node: to.to_string(),
        kind: PlanViewEdgeKind::Direct,
    }
}

pub fn arg(from: &str, to: &str, name: &str, urn: &str) -> PlanViewEdge {
    PlanViewEdge {
        from_node: from.to_string(),
        to_node: to.to_string(),
        kind: PlanViewEdgeKind::Arg {
            arg_urn: urn.to_string(),
            arg_name: name.to_string(),
        },
    }
}

pub fn iteration(from: &str, to: &str) -> PlanViewEdge {
    PlanViewEdge {
        from_node: from.to_string(),
        to_node: to.to_string(),
        kind: PlanViewEdgeKind::Iteration,
    }
}

pub fn collection(from: &str, to: &str) -> PlanViewEdge {
    PlanViewEdge {
        from_node: from.to_string(),
        to_node: to.to_string(),
        kind: PlanViewEdgeKind::Collection,
    }
}

fn plan(name: &str, mut nodes: Vec<PlanViewNode>, edges: Vec<PlanViewEdge>) -> PlanView {
    nodes.sort_by(|a, b| a.id.cmp(&b.id));
    PlanView {
        name: name.to_string(),
        nodes,
        edges,
    }
}

const PDF: &str = "media:document;ext=pdf";
const PAGE_TEXT: &str = "media:enc=utf-8;ext=txt;page;plain-text";
const PLAIN: &str = "media:enc=utf-8;ext=txt;plain-text";
const CRITERION: &str = "media:criterion;enc=utf-8";
const TEXT: &str = "media:enc=utf-8;text";
const REPORT: &str = "media:enc=utf-8;report";
const ROWS: &str = "media:fmt=csv;rows";
const DISBIND: &str = r#"cap:in="media:document;ext=pdf";disbind-pdf;out="media:enc=utf-8;ext=txt;page;plain-text""#;
const SUMMARIZE: &str = r#"cap:in="media:enc=utf-8";summarize-en;out="media:enc=utf-8;ext=txt;plain-text""#;

/// The uikit's run fixture: `[doc -> extract{index-range="1-40"} -> text]
/// [(text, for=purpose) -> summarize -> summary]`, run once per document.
pub fn summarize_documents() -> PlanView {
    let mut extract = step("step_extract", "tok-extract-0001", DISBIND, "Extract text", Some("text"), PAGE_TEXT, "Page text", Some("map_doc"));
    if let PlanViewKind::Step(s) = &mut extract.kind {
        s.constants.push(PlanViewConstant {
            arg_urn: "media:enc=utf-8;index-range".to_string(),
            arg_name: "index-range".to_string(),
            value_json: "\"1-40\"".to_string(),
        });
    }
    let mut summarize = step("step_summarize", "tok-summarize-0002", SUMMARIZE, "Summarize", Some("summary"), PLAIN, "Summary", Some("map_doc"));
    if let PlanViewKind::Step(s) = &mut summarize.kind {
        s.retry = 2;
    }
    plan(
        "summarize documents",
        vec![
            input("in_doc", "doc", PDF, "PDF", true),
            input("in_purpose", "purpose", CRITERION, "Criterion", false),
            map("map_doc", "tok-run-foreach-0003", &[("in_doc", "item_doc")], &["item_doc", "step_extract", "step_summarize"], None),
            item("item_doc", "map_doc", "in_doc", PDF, "PDF"),
            extract,
            summarize,
            output("out_summary", "summary", "step_summarize", PLAIN, "Summary"),
        ],
        vec![
            direct("in_doc", "map_doc"),
            iteration("map_doc", "item_doc"),
            direct("item_doc", "step_extract"),
            direct("step_extract", "step_summarize"),
            arg("in_purpose", "step_summarize", "for", CRITERION),
            direct("step_summarize", "out_summary"),
        ],
    )
}

/// `(path, benchmark=bench) -> compare -> (report, residuals=rows)`,
/// `rows -> tabulate -> table`: two inputs of one type, a named argument, a
/// step's other output read by another step.
pub fn compare() -> PlanView {
    plan(
        "compare",
        vec![
            input("in_path", "path", TEXT, "Text", false),
            input("in_bench", "bench", TEXT, "Text", false),
            step("compare", "tok-compare", "cap:compare", "Compare", Some("report"), REPORT, "Report", None),
            step_output("residuals", "compare", "residuals", "rows", ROWS, "Rows"),
            step("tabulate", "tok-tabulate", "cap:tabulate", "Tabulate", Some("table"), TEXT, "Text", None),
            output("out_report", "report", "compare", REPORT, "Report"),
            output("out_table", "table", "tabulate", TEXT, "Text"),
        ],
        vec![
            direct("in_path", "compare"),
            arg("in_bench", "compare", "benchmark", "media:enc=utf-8;text;benchmark"),
            direct("compare", "residuals"),
            direct("residuals", "tabulate"),
            direct("compare", "out_report"),
            direct("tabulate", "out_table"),
        ],
    )
}

/// A map that zips two sequences, and a gather of both.
pub fn zip() -> PlanView {
    plan(
        "zip",
        vec![
            input("in_a", "left", TEXT, "Text", true),
            input("in_b", "right", TEXT, "Text", true),
            map("map", "tok-zip", &[("in_a", "a"), ("in_b", "b")], &["a", "b", "pair"], None),
            item("a", "map", "in_a", TEXT, "Text"),
            item("b", "map", "in_b", TEXT, "Text"),
            step("pair", "tok-pair", "cap:pair", "Pair", None, TEXT, "Text", Some("map")),
            gather("both", &["in_a", "in_b"], TEXT, "Text"),
            step("count", "tok-count", "cap:count", "Count", None, TEXT, "Text", None),
            output("out_pairs", "pairs", "pair", TEXT, "Text"),
            output("out_count", "count", "count", TEXT, "Text"),
        ],
        vec![
            direct("in_a", "map"),
            direct("in_b", "map"),
            iteration("map", "a"),
            iteration("map", "b"),
            direct("a", "pair"),
            arg("b", "pair", "other", "media:enc=utf-8;text;other"),
            collection("in_a", "both"),
            collection("in_b", "both"),
            direct("both", "count"),
            direct("pair", "out_pairs"),
            direct("count", "out_count"),
        ],
    )
}

/// A map inside a map: documents split into pages, each page rendered.
pub fn nested_pages() -> PlanView {
    plan(
        "pages",
        vec![
            input("in", "docs", "media:pdf", "PDF", true),
            map("outer", "tok-docs", &[("in", "doc")], &["doc", "split", "inner", "page", "render"], None),
            item("doc", "outer", "in", "media:pdf", "PDF"),
            step("split", "tok-split", "cap:split", "Split", None, "media:png", "PNG", Some("outer")),
            map("inner", "tok-pages", &[("split", "page")], &["page", "render"], Some("outer")),
            item("page", "inner", "split", "media:png", "PNG"),
            step("render", "tok-render", "cap:render", "Render", None, "media:jpg", "JPEG", Some("inner")),
            output("out", "pages", "render", "media:jpg", "JPEG"),
        ],
        vec![
            direct("in", "outer"),
            iteration("outer", "doc"),
            direct("doc", "split"),
            direct("split", "inner"),
            iteration("inner", "page"),
            direct("page", "render"),
            direct("render", "out"),
        ],
    )
}

/// PDF → extract → text → summarize → summary: the plainest chain, with a
/// value card between the steps.
pub fn linear() -> PlanView {
    plan(
        "linear",
        vec![
            input("in_pdf", "pdf", PDF, "PDF", false),
            step("extract", "tok-extract", DISBIND, "Extract PDF text", Some("text"), PAGE_TEXT, "Page Text", None),
            step("summarize", "tok-summarize", SUMMARIZE, "Summarize", None, PLAIN, "Summary", None),
            output("out", "summary", "summarize", PLAIN, "Summary"),
        ],
        vec![direct("in_pdf", "extract"), direct("extract", "summarize"), direct("summarize", "out")],
    )
}

/// One value read by two steps, each with its own output: a fan-out.
pub fn diverge() -> PlanView {
    plan(
        "diverge",
        vec![
            input("in_pdf", "pdf", PDF, "PDF", false),
            step("extract", "tok-extract", DISBIND, "Extract PDF text", Some("page_text"), PAGE_TEXT, "Page Text", None),
            step("concat", "tok-concat", "cap:concat-text", "Concatenate", None, PLAIN, "Plain Text", None),
            step("fields", "tok-fields", "cap:extract-en", "Extract fields", None, "media:fmt=json;record;semantic-judgment", "Structured record", None),
            output("out_plain", "plain", "concat", PLAIN, "Plain Text"),
            output("out_record", "record", "fields", "media:fmt=json;record;semantic-judgment", "Structured record"),
        ],
        vec![
            direct("in_pdf", "extract"),
            direct("extract", "concat"),
            direct("extract", "fields"),
            direct("concat", "out_plain"),
            direct("fields", "out_record"),
        ],
    )
}

/// Two chains gathered into one step: a fan-in.
pub fn converge() -> PlanView {
    plan(
        "converge",
        vec![
            input("in_pdf", "pdf", PDF, "PDF", false),
            input("in_png", "png", "media:ext=png;image", "PNG image", false),
            step("extract", "tok-extract", DISBIND, "Extract PDF text", Some("page_text"), PAGE_TEXT, "Page Text", None),
            step("ocr", "tok-ocr", "cap:ocr-gguf", "OCR", Some("digitized"), "media:digitized-text;enc=utf-8;ext=txt;plain-text", "Digitized text", None),
            gather("both", &["extract", "ocr"], TEXT, "Text"),
            step("concat", "tok-concat", "cap:concat-text", "Concatenate", None, PLAIN, "Plain Text", None),
            output("out", "plain", "concat", PLAIN, "Plain Text"),
        ],
        vec![
            direct("in_pdf", "extract"),
            direct("in_png", "ocr"),
            collection("extract", "both"),
            collection("ocr", "both"),
            direct("both", "concat"),
            direct("concat", "out"),
        ],
    )
}

/// Every fixture, by name.
pub fn all() -> Vec<(&'static str, PlanView)> {
    vec![
        ("summarize_documents", summarize_documents()),
        ("compare", compare()),
        ("zip", zip()),
        ("nested_pages", nested_pages()),
        ("linear", linear()),
        ("diverge", diverge()),
        ("converge", converge()),
    ]
}
