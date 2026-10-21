//! The terminal interface's pictures, checked against what the uikit's own
//! tests say a run's picture is, and against the geometry a drawing must
//! keep whatever the scene.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::tui::canvas;
use crate::tui::fixtures;
use crate::tui::layout::{self, Arrangement, Layout, LayoutOptions, Lod};
use crate::tui::scene::{run_scene, Scene};
use crate::tui::theme::{Appearance, ColorDepth, GlyphSet, Theme};

fn planned(plan: &crate::orchestrator::PlanView) -> Scene {
    run_scene(plan, &HashMap::new(), None).expect("fixture plans draw")
}

fn edges_into<'a>(scene: &'a Scene, id: &str) -> Vec<(&'a str, &'a str)> {
    let mut out: Vec<(&str, &str)> = scene
        .edges
        .iter()
        .filter(|e| e.to == id)
        .map(|e| (e.from.as_str(), e.label.as_str()))
        .collect();
    out.sort();
    out
}

// TEST13030: inputs of one type are told apart by name; the edge into an
// argument other than the main input names the argument; a step's other
// output read by another step is a value card of its own, reached by an
// edge named for the output; the main output read only by the machine's
// output has no card (the uikit's "named inputs, argument edges, a step's
// other outputs").
#[test]
fn test13030_compare_plan_draws_named_inputs_args_and_other_outputs() {
    let scene = planned(&fixtures::compare());
    assert_eq!(scene.node("run-input-in_path").unwrap().title, "path");
    assert_eq!(scene.node("run-input-in_bench").unwrap().title, "bench");
    assert_eq!(
        edges_into(&scene, "run-step-compare"),
        vec![("run-input-in_bench", "benchmark"), ("run-input-in_path", "")]
    );
    let residuals = scene.node("run-value-residuals").expect("the residuals output is a card");
    assert_eq!(residuals.title, "rows");
    assert_eq!(edges_into(&scene, "run-value-residuals"), vec![("run-step-compare", "residuals")]);
    assert!(scene.node("run-value-compare").is_none(), "the main output only the machine reads has no card");
    assert_eq!(edges_into(&scene, "run-output-out_report"), vec![("run-step-compare", "")]);

    // Outputs read at the end of the picture, even one whose producer is early.
    let laid = layout::layout(&scene, &options(Lod::Full, Arrangement::Rows)).unwrap();
    let last = laid.nodes.iter().map(|n| n.column).max().unwrap();
    for id in ["run-output-out_report", "run-output-out_table"] {
        assert_eq!(laid.node(id).unwrap().column, last, "{id} sits in the last column");
    }
    assert!(laid.node("run-output-out_report").unwrap().column > laid.node("run-step-tabulate").unwrap().column);
}

// TEST13031: a zip names every sequence it runs over; each item's wire names
// the sequence it is of and an argument's wire names the argument; a gather
// is drawn as the value its members make, gathered from each member.
#[test]
fn test13031_zip_map_and_gather_draw_as_the_uikit_draws_them() {
    let scene = planned(&fixtures::zip());
    let zip = scene.node("run-map-map").unwrap();
    assert_eq!(zip.title, "left × right");
    assert_eq!(
        edges_into(&scene, "run-map-map").iter().map(|e| e.0).collect::<Vec<_>>(),
        vec!["run-input-in_a", "run-input-in_b"]
    );
    let mut labels: Vec<&str> = edges_into(&scene, "run-step-pair").iter().map(|e| e.1).collect();
    labels.sort();
    assert_eq!(labels, vec!["For each item of left", "other"]);
    let gather = scene.node("run-gather-both").unwrap();
    assert_eq!(gather.subtitle, "Gathered from 2");
    assert_eq!(
        edges_into(&scene, "run-gather-both").iter().map(|e| e.0).collect::<Vec<_>>(),
        vec!["run-input-in_a", "run-input-in_b"]
    );
    assert_eq!(scene.node_of_token("tok-zip").unwrap().id, "run-map-map", "a map's token resolves to its stack");
}

// TEST13032: the summarize-documents run draws its input stacks, the map's
// stack with its "For each item" wire, the step cards with their policies,
// and the argument edge from the second input named for the argument.
#[test]
fn test13032_summarize_documents_draws_map_policy_and_argument_edge() {
    let scene = planned(&fixtures::summarize_documents());
    assert_eq!(scene.node("run-map-map_doc").unwrap().title, "doc");
    assert_eq!(
        edges_into(&scene, "run-step-step_extract"),
        vec![("run-map-map_doc", "For each item")]
    );
    let summarize = scene.node("run-step-step_summarize").unwrap();
    assert!(summarize.subtitle.ends_with("retry 2"), "{}", summarize.subtitle);
    assert!(summarize.subtitle.starts_with("summarize-en"), "{}", summarize.subtitle);
    assert_eq!(
        edges_into(&scene, "run-step-step_summarize"),
        vec![("run-input-in_purpose", "for"), ("run-value-step_extract", "")]
    );
}

fn options(lod: Lod, arrangement: Arrangement) -> LayoutOptions {
    LayoutOptions {
        lod,
        arrangement,
        chip_width: 0,
    }
}

fn arrangements() -> Vec<Arrangement> {
    vec![Arrangement::Rows, Arrangement::Wrapped { width: 70 }, Arrangement::Wrapped { width: 30 }]
}

/// Every cell a routed edge covers, with whether it runs across or down
/// there.
fn edge_cells(layout: &Layout) -> Vec<(String, String, (i32, i32), bool, bool)> {
    let mut out = Vec::new();
    for edge in &layout.edges {
        for pair in edge.points.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            let horizontal = a.1 == b.1;
            let (xs, ys) = (a.0.min(b.0)..=a.0.max(b.0), a.1.min(b.1)..=a.1.max(b.1));
            for x in xs.clone() {
                for y in ys.clone() {
                    out.push((edge.id.clone(), edge.from.clone(), (x, y), horizontal, !horizontal));
                }
            }
        }
    }
    out
}

// TEST13033: whatever the plan, level of detail and arrangement, a layout
// keeps its geometry: no two nodes overlap; every edge runs orthogonally from
// the cell right of its source to the cell left of its target, inside the
// target's rows; no edge passes through a node; every edge goes forward a
// column; and the same scene always lays out the same.
#[test]
fn test13033_layout_geometry_holds_for_every_fixture() {
    for (name, plan) in fixtures::all() {
        let scene = planned(&plan);
        for lod in Lod::ALL {
            for arrangement in arrangements() {
                let opts = options(lod, arrangement);
                let laid = layout::layout(&scene, &opts).unwrap();
                let again = layout::layout(&scene, &opts).unwrap();
                assert_eq!(laid, again, "{name} {lod:?} {arrangement:?}: deterministic");
                assert!(
                    layout::overlapping(&laid).is_empty(),
                    "{name} {lod:?} {arrangement:?}: overlapping nodes {:?}",
                    layout::overlapping(&laid)
                );
                for edge in &laid.edges {
                    let from = laid.node(&edge.from).unwrap();
                    let to = laid.node(&edge.to).unwrap();
                    let first = edge.points[0];
                    let last = *edge.points.last().unwrap();
                    assert_eq!(first, (from.card.right(), from.out_row), "{name} {lod:?} {arrangement:?} {}: starts right of its source", edge.id);
                    assert_eq!(last.0, to.card.x - 1, "{name} {lod:?} {arrangement:?} {}: ends left of its target", edge.id);
                    assert!(last.1 >= to.card.y && last.1 < to.card.bottom(), "{name} {lod:?} {arrangement:?} {}: enters within its target's rows", edge.id);
                    assert!(to.column > from.column, "{name}: {} goes forward", edge.id);
                    for pair in edge.points.windows(2) {
                        assert!(pair[0].0 == pair[1].0 || pair[0].1 == pair[1].1, "{name} {lod:?} {}: orthogonal", edge.id);
                    }
                    assert_eq!(edge.nets.len(), edge.points.len() - 1);
                }
                for (id, _, cell, _, _) in edge_cells(&laid) {
                    for node in &laid.nodes {
                        assert!(
                            !node.rect.contains(cell.0, cell.1),
                            "{name} {lod:?} {arrangement:?}: edge {id} passes through {} at {cell:?}",
                            node.id
                        );
                    }
                }
            }
        }
    }
}

// TEST13034: edges from different sources never share a line: where two of
// them meet in a cell, one crosses the other at a right angle. Only the edges
// leaving one node share cells — their common trunk — so a junction is only
// ever where a node's edges branch.
#[test]
fn test13034_edges_of_different_sources_only_cross() {
    for (name, plan) in fixtures::all() {
        let scene = planned(&plan);
        for lod in Lod::ALL {
            for arrangement in arrangements() {
                let laid = layout::layout(&scene, &options(lod, arrangement)).unwrap();
                let mut by_cell: BTreeMap<(i32, i32), Vec<(String, bool, bool)>> = BTreeMap::new();
                for (_, source, cell, across, down) in edge_cells(&laid) {
                    by_cell.entry(cell).or_default().push((source, across, down));
                }
                for (cell, uses) in by_cell {
                    let sources: HashSet<&String> = uses.iter().map(|u| &u.0).collect();
                    if sources.len() < 2 {
                        continue;
                    }
                    // Group the cell's runs by source: each source must run
                    // straight through it in one direction, and the
                    // directions of different sources must differ.
                    let mut direction_of: HashMap<&String, (bool, bool)> = HashMap::new();
                    for (source, across, down) in &uses {
                        let entry = direction_of.entry(source).or_insert((false, false));
                        entry.0 |= across;
                        entry.1 |= down;
                    }
                    let across: Vec<_> = direction_of.values().filter(|d| d.0 && !d.1).collect();
                    let down: Vec<_> = direction_of.values().filter(|d| d.1 && !d.0).collect();
                    assert!(
                        across.len() == 1 && down.len() == 1 && direction_of.len() == 2,
                        "{name} {lod:?} {arrangement:?}: cell {cell:?} is shared by {direction_of:?}"
                    );
                }
            }
        }
    }
}

fn theme() -> Theme {
    Theme::new(Appearance::Dark, ColorDepth::None, GlyphSet::Unicode, false)
}

// TEST13035: the whole picture of every fixture renders at every level of
// detail, and its text names every node's title at the levels that show
// titles — nothing a node is called is lost in the drawing.
#[test]
fn test13035_whole_picture_shows_every_title() {
    for (name, plan) in fixtures::all() {
        let scene = planned(&plan);
        for lod in [Lod::Full, Lod::Compact] {
            let laid = layout::layout(&scene, &options(lod, Arrangement::Rows)).unwrap();
            let picture = canvas::to_text(&canvas::render_whole(&scene, &laid, &theme()), false);
            if std::env::var_os("CAPDAG_TUI_PRINT").is_some() {
                println!("── {name} · {lod:?} ──\n{picture}");
            }
            for node in &scene.nodes {
                let shown = crate::tui::text::truncate(&node.title, 6);
                let stem = shown.trim_end_matches('…');
                assert!(picture.contains(stem), "{name} {lod:?}: {:?} is not in the picture:\n{picture}", node.title);
            }
        }
    }
}

// TEST13036: every line in a drawn picture is continuous: no cell of it is
// a half line (a stub that connects one side only). An edge that broke into
// pieces where it passes from one column's channel to the next, or that did
// not reach the port it leaves by, would show one.
#[test]
fn test13036_drawn_lines_are_continuous() {
    const HALF_LINES: [char; 8] = ['╴', '╶', '╵', '╷', '╸', '╺', '╹', '╻'];
    for (name, plan) in fixtures::all() {
        let scene = planned(&plan);
        for lod in Lod::ALL {
            for arrangement in arrangements() {
                let laid = layout::layout(&scene, &options(lod, arrangement)).unwrap();
                let picture = canvas::to_text(&canvas::render_whole(&scene, &laid, &theme()), false);
                for (row, line) in picture.lines().enumerate() {
                    if let Some(col) = line.chars().position(|c| HALF_LINES.contains(&c)) {
                        panic!("{name} {lod:?} {arrangement:?}: a broken line at row {row}, column {col}:\n{picture}");
                    }
                }
            }
        }
    }
}

// TEST13037: fitting a picture keeps the most detail it can. A machine one
// column too wide for one row of whole cards, in a viewport with room below,
// is drawn as whole cards in bands — never shrunk to lines or dots while the
// bands would fit; one that fits in one row is drawn in one row.
#[test]
fn test13037_fit_wraps_into_bands_before_losing_detail() {
    let mut wrapped_somewhere = false;
    for (name, plan) in fixtures::all() {
        let scene = planned(&plan);
        let row = layout::layout(&scene, &options(Lod::Full, Arrangement::Rows)).unwrap();
        // Room for one row exactly: one row it is.
        let fitted = layout::fit(
            &scene,
            row.width,
            row.height,
            &[Arrangement::Rows, Arrangement::Wrapped { width: row.width }],
            0,
        )
        .unwrap();
        assert_eq!((fitted.lod, fitted.arrangement), (Lod::Full, Arrangement::Rows), "{name}");

        let width = row.width - 1;
        let bands = Arrangement::Wrapped { width };
        let banded = layout::layout(&scene, &options(Lod::Full, bands)).unwrap();
        if banded.width > width {
            // Too narrow to band at all: nothing to say of this one.
            continue;
        }
        wrapped_somewhere = true;
        let fitted = layout::fit(&scene, width, 1000, &[Arrangement::Rows, bands], 0).unwrap();
        assert_eq!(fitted.lod, Lod::Full, "{name}: whole cards fit in bands of {width}");
        assert_eq!(fitted.arrangement, bands, "{name}");
        assert!(fitted.width <= width, "{name}");
    }
    assert!(wrapped_somewhere, "some fixture is wide enough to band");
}
