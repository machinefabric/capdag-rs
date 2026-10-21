//! Where each part of a scene sits, in terminal cells.
//!
//! A layered (Sugiyama) layout, computed on the cell grid:
//!
//! 1. **Columns.** A node's column is the longest path into it — the uikit's
//!    rule (`_layerPlacement`), so a machine has the same columns on the
//!    desktop and in a terminal. The run's outputs sit in the last column.
//! 2. **Long edges** get a one-row slot in every column they pass through,
//!    so they run between nodes, never across them.
//! 3. **Order within columns.** Barycentre sweeps, down then up, keeping the
//!    order with the fewest crossings; ties keep a deterministic order (fewest
//!    edges in, then id), so the same scene always lays out the same.
//! 4. **Rows.** Each element is pulled toward the rows of what it connects
//!    to, keeping its column's order and spacing: an exact, order-preserving
//!    least-squares fit (pool-adjacent-violators), swept left and right.
//! 5. **Routing.** Between two columns is a channel. Everything leaving one
//!    element is one net: a stub out of its right side, a vertical trunk, a
//!    branch into each target. Each net's trunk gets its own track in the
//!    channel (left-edge interval colouring), so no two nets share a line and
//!    a junction is only ever drawn where one net branches.
//!
//! The level of detail decides how big a node is; the layout is a pure
//! function of the scene, the level and the arrangement.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::tui::scene::{NodeRole, Scene, SceneNode};
use crate::tui::text;

/// How much of each node is drawn — the terminal's zoom.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Lod {
    /// One cell per node: the overview.
    Dot,
    /// One line per node: mark, title, state.
    Glyph,
    /// A small card: title and state.
    Compact,
    /// The whole card: title, subtitle, what became of it, its ports.
    Full,
}

impl Lod {
    pub const ALL: [Lod; 4] = [Lod::Full, Lod::Compact, Lod::Glyph, Lod::Dot];

    /// One step more detail, if there is more.
    pub fn more(self) -> Option<Lod> {
        match self {
            Lod::Dot => Some(Lod::Glyph),
            Lod::Glyph => Some(Lod::Compact),
            Lod::Compact => Some(Lod::Full),
            Lod::Full => None,
        }
    }

    /// One step less detail, if there is less.
    pub fn less(self) -> Option<Lod> {
        match self {
            Lod::Full => Some(Lod::Compact),
            Lod::Compact => Some(Lod::Glyph),
            Lod::Glyph => Some(Lod::Dot),
            Lod::Dot => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Lod::Full => "full",
            Lod::Compact => "compact",
            Lod::Glyph => "lines",
            Lod::Dot => "dots",
        }
    }
}

/// How the columns are arranged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Arrangement {
    /// One band: every column side by side.
    Rows,
    /// Columns break into bands below each other where `width` runs out, the
    /// way a paragraph wraps; every edge still runs left to right in a band.
    Wrapped { width: i32 },
}

/// What decides a layout besides the scene.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LayoutOptions {
    pub lod: Lod,
    pub arrangement: Arrangement,
    /// Columns reserved where a step's edge leaves it for its flow chip; 0
    /// when the scene has no flow to show.
    pub chip_width: i32,
}

/// A rectangle of cells.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Rect {
    pub x: i32,
    pub y: i32,
    pub w: i32,
    pub h: i32,
}

impl Rect {
    pub fn right(&self) -> i32 {
        self.x + self.w
    }
    pub fn bottom(&self) -> i32 {
        self.y + self.h
    }
    pub fn contains(&self, x: i32, y: i32) -> bool {
        x >= self.x && x < self.right() && y >= self.y && y < self.bottom()
    }
    pub fn intersects(&self, other: &Rect) -> bool {
        self.x < other.right() && other.x < self.right() && self.y < other.bottom() && other.y < self.bottom()
    }
}

/// A node where it sits. `rect` is the whole footprint, the back cards of a
/// stack included; `card` is the front card.
#[derive(Debug, Clone, PartialEq)]
pub struct PlacedNode {
    pub id: String,
    pub rect: Rect,
    pub card: Rect,
    pub column: usize,
    pub band: usize,
    /// The row edges leave it from.
    pub out_row: i32,
}

/// One edge as it is drawn: the corners of an orthogonal path, from the cell
/// right of its source to the cell left of its target (where the arrowhead
/// is). Consecutive points share a row or a column.
#[derive(Debug, Clone, PartialEq)]
pub struct RoutedEdge {
    pub id: String,
    pub from: String,
    pub to: String,
    pub points: Vec<(i32, i32)>,
    /// The net each stretch of the path belongs to, one per segment
    /// (`points.len() - 1`): edges of one net share cells and meet in
    /// junctions; edges of different nets only ever cross.
    pub nets: Vec<usize>,
    /// Where the label starts, and how many columns it may take.
    pub label_at: Option<(i32, i32, i32)>,
    /// Where the flow chip starts, and how many columns it may take.
    pub chip_at: Option<(i32, i32, i32)>,
}

/// A laid-out scene.
#[derive(Debug, Clone, PartialEq)]
pub struct Layout {
    pub nodes: Vec<PlacedNode>,
    pub edges: Vec<RoutedEdge>,
    pub width: i32,
    pub height: i32,
    pub lod: Lod,
    pub arrangement: Arrangement,
}

impl Layout {
    pub fn node(&self, id: &str) -> Option<&PlacedNode> {
        self.nodes.iter().find(|n| n.id == id)
    }
}

/// The scene cannot be laid out.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LayoutError {
    #[error("edge '{edge}' joins '{node}', which is not in the scene")]
    UnknownNode { edge: String, node: String },
    #[error("edge '{0}' runs backwards against the columns; a run's picture has no cycles")]
    Backward(String),
    #[error("no arrangement was given to fit the scene in")]
    NoArrangement,
}

/// The columns of a card's side borders and padding.
const CARD_CHROME: i32 = 4;

/// How big a node is at `lod`: its front card, and how many rows its edges
/// need on each side.
fn card_size(node: &SceneNode, lod: Lod, ins: usize, outs: usize) -> (i32, i32) {
    let title_width = text::width(&node.title) as i32 + 2; // icon and space
    match lod {
        Lod::Full => {
            let mut inner = title_width + 2; // the status mark
            inner = inner.max(text::width(&node.subtitle) as i32);
            if let Some(meta) = &node.meta {
                inner = inner.max(text::width(meta) as i32);
            }
            for port in &node.ports {
                let value = port.value.as_deref().map(text::width).unwrap_or(1) as i32;
                inner = inner.max(2 + text::width(&port.name) as i32 + 2 + value.min(14));
            }
            let inner = inner.clamp(16, 32);
            let mut h = 2 + 2; // borders, title, subtitle
            if node.meta.is_some() {
                h += 1;
            }
            if !node.ports.is_empty() {
                h += 1 + node.ports.len().min(PORT_ROWS) as i32;
            }
            // Every edge in or out takes its own interior row.
            let needed = ins.max(outs) as i32 + 2;
            (inner + CARD_CHROME, h.max(needed))
        }
        Lod::Compact => {
            let inner = (title_width + 2).clamp(10, 22);
            let needed = ins.max(outs) as i32 + 2;
            (inner + CARD_CHROME, 3.max(needed))
        }
        // A one-line node is as tall as the edges into it need: each enters
        // on a row of its own, so edges from different nodes never merge.
        Lod::Glyph => ((title_width + 4).min(24), (ins as i32).max(1)),
        Lod::Dot => (3, (ins as i32).max(1)),
    }
}

/// How many argument rows a full card shows before it says how many more.
pub const PORT_ROWS: usize = 6;

/// One thing placed in a column: a node, or the slot a long edge passes
/// through.
#[derive(Debug, Clone)]
struct Element {
    /// The node id, or `edge id #column` for a slot.
    key: String,
    node: Option<usize>,
    column: usize,
    w: i32,
    h: i32,
    /// Footprint offset of the front card (a stack's back cards sit up and
    /// to the right of it).
    stack_rise: i32,
    stack_shift: i32,
    y: i32,
}

/// One hop of an edge from one element to the next column's.
#[derive(Debug, Clone, Copy)]
struct Hop {
    edge: usize,
    from: usize,
    to: usize,
}

/// Lay `scene` out.
pub fn layout(scene: &Scene, options: &LayoutOptions) -> Result<Layout, LayoutError> {
    let lod = options.lod;
    let index_of: HashMap<&str, usize> = scene
        .nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (n.id.as_str(), i))
        .collect();
    let mut edges: Vec<(usize, usize)> = Vec::with_capacity(scene.edges.len());
    for edge in &scene.edges {
        let from = *index_of.get(edge.from.as_str()).ok_or_else(|| LayoutError::UnknownNode {
            edge: edge.id.clone(),
            node: edge.from.clone(),
        })?;
        let to = *index_of.get(edge.to.as_str()).ok_or_else(|| LayoutError::UnknownNode {
            edge: edge.id.clone(),
            node: edge.to.clone(),
        })?;
        edges.push((from, to));
    }

    // ── 1. columns: the longest path into each node ──
    let n = scene.nodes.len();
    let mut column = vec![0usize; n];
    for _ in 0..n.max(1) {
        let mut changed = false;
        for &(from, to) in &edges {
            if column[to] < column[from] + 1 {
                column[to] = column[from] + 1;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    let last = column.iter().copied().max().unwrap_or(0);
    for (i, node) in scene.nodes.iter().enumerate() {
        if scene.align_last.contains(&node.id) {
            column[i] = last;
        }
    }
    for (e, &(from, to)) in edges.iter().enumerate() {
        if column[to] <= column[from] {
            return Err(LayoutError::Backward(scene.edges[e].id.clone()));
        }
    }
    let columns = last + 1;

    // ── 2. elements: nodes, and a slot per column a long edge passes ──
    let ins: Vec<usize> = (0..n).map(|i| edges.iter().filter(|e| e.1 == i).count()).collect();
    let outs: Vec<usize> = (0..n)
        .map(|i| if edges.iter().any(|e| e.0 == i) { 1 } else { 0 })
        .collect();
    let mut elements: Vec<Element> = Vec::new();
    let mut element_of_node = vec![0usize; n];
    for (i, node) in scene.nodes.iter().enumerate() {
        let (w, h) = card_size(node, lod, ins[i], outs[i]);
        let stacked = node.stack.is_some() && matches!(lod, Lod::Full | Lod::Compact);
        let (rise, shift) = if stacked { (1, 1) } else { (0, 0) };
        element_of_node[i] = elements.len();
        elements.push(Element {
            key: node.id.clone(),
            node: Some(i),
            column: column[i],
            w: w + shift,
            h: h + rise,
            stack_rise: rise,
            stack_shift: shift,
            y: 0,
        });
    }
    let mut hops: Vec<Hop> = Vec::new();
    for (e, &(from, to)) in edges.iter().enumerate() {
        let mut previous = element_of_node[from];
        for c in column[from] + 1..column[to] {
            let slot = elements.len();
            elements.push(Element {
                key: format!("{}#{c}", scene.edges[e].id),
                node: None,
                column: c,
                w: 1,
                h: 1,
                stack_rise: 0,
                stack_shift: 0,
                y: 0,
            });
            hops.push(Hop {
                edge: e,
                from: previous,
                to: slot,
            });
            previous = slot;
        }
        hops.push(Hop {
            edge: e,
            from: previous,
            to: element_of_node[to],
        });
    }

    // ── 3. order within columns ──
    let mut order: Vec<Vec<usize>> = vec![Vec::new(); columns];
    for (i, element) in elements.iter().enumerate() {
        order[element.column].push(i);
    }
    let incoming_count = |i: usize| hops.iter().filter(|h| h.to == i).count();
    for col in &mut order {
        col.sort_by(|&a, &b| {
            incoming_count(a)
                .cmp(&incoming_count(b))
                .then_with(|| elements[a].key.cmp(&elements[b].key))
        });
    }
    let preds: Vec<Vec<usize>> = (0..elements.len())
        .map(|i| hops.iter().filter(|h| h.to == i).map(|h| h.from).collect())
        .collect();
    let succs: Vec<Vec<usize>> = (0..elements.len())
        .map(|i| hops.iter().filter(|h| h.from == i).map(|h| h.to).collect())
        .collect();
    let position = |order: &Vec<Vec<usize>>| -> Vec<usize> {
        let mut pos = vec![0usize; elements.len()];
        for col in order {
            for (p, &i) in col.iter().enumerate() {
                pos[i] = p;
            }
        }
        pos
    };
    let sweep = |order: &mut Vec<Vec<usize>>, down: bool| {
        let range: Vec<usize> = if down {
            (1..columns).collect()
        } else {
            (0..columns.saturating_sub(1)).rev().collect()
        };
        for c in range {
            let pos = position(order);
            let neighbours = if down { &preds } else { &succs };
            let current = order[c].clone();
            let mut keyed: Vec<(Option<f64>, usize, usize)> = current
                .iter()
                .enumerate()
                .map(|(p, &i)| {
                    let ns = &neighbours[i];
                    let bary = if ns.is_empty() {
                        None
                    } else {
                        Some(ns.iter().map(|&j| pos[j] as f64).sum::<f64>() / ns.len() as f64)
                    };
                    (bary, p, i)
                })
                .collect();
            // Elements with no neighbour on that side keep their place
            // relative to each other; the rest go by barycentre, ties by
            // their previous place (stable and deterministic).
            let fixed: Vec<(usize, usize)> = keyed
                .iter()
                .filter(|k| k.0.is_none())
                .map(|k| (k.1, k.2))
                .collect();
            keyed.retain(|k| k.0.is_some());
            keyed.sort_by(|a, b| {
                a.0.partial_cmp(&b.0)
                    .expect("barycentres are finite")
                    .then(a.1.cmp(&b.1))
            });
            let mut merged: Vec<usize> = Vec::with_capacity(current.len());
            let mut floating = keyed.into_iter().map(|k| k.2);
            let mut fixed_iter = fixed.into_iter().peekable();
            for slot in 0..current.len() {
                if fixed_iter.peek().is_some_and(|(p, _)| *p == slot) {
                    merged.push(fixed_iter.next().expect("peeked").1);
                } else if let Some(next) = floating.next() {
                    merged.push(next);
                } else {
                    merged.push(fixed_iter.next().expect("every element is placed").1);
                }
            }
            order[c] = merged;
        }
    };
    let crossings = |order: &Vec<Vec<usize>>| -> usize {
        let pos = position(order);
        let mut total = 0;
        for c in 0..columns.saturating_sub(1) {
            let between: Vec<(usize, usize)> = hops
                .iter()
                .filter(|h| elements[h.from].column == c)
                .map(|h| (pos[h.from], pos[h.to]))
                .collect();
            for i in 0..between.len() {
                for j in i + 1..between.len() {
                    let (a, b) = (between[i], between[j]);
                    if (a.0 < b.0 && a.1 > b.1) || (a.0 > b.0 && a.1 < b.1) {
                        total += 1;
                    }
                }
            }
        }
        total
    };
    // The uikit's own order: one downward barycentre pass.
    sweep(&mut order, true);
    let mut best = order.clone();
    let mut best_crossings = crossings(&best);
    for round in 0..8 {
        sweep(&mut order, round % 2 == 1);
        let count = crossings(&order);
        if count < best_crossings {
            best = order.clone();
            best_crossings = count;
        }
        if best_crossings == 0 {
            break;
        }
    }
    let order = best;

    // ── 4. rows ──
    let gap = match lod {
        Lod::Full => 2,
        Lod::Compact => 1,
        Lod::Glyph => 1,
        Lod::Dot => 0,
    };
    // Where an element's connections meet it: its out-row offset from its top.
    let anchor = |el: &Element| -> i32 {
        match el.node {
            None => 0,
            Some(_) => el.stack_rise + if el.h - el.stack_rise >= 3 { 1 } else { 0 },
        }
    };
    // Pack every column from the top.
    for col in &order {
        let mut y = 0;
        for &i in col {
            elements[i].y = y;
            y += elements[i].h + gap;
        }
    }
    for round in 0..6 {
        let left_to_right = round % 2 == 0;
        let columns_in_order: Vec<usize> = if left_to_right {
            (1..columns).collect()
        } else {
            (0..columns.saturating_sub(1)).rev().collect()
        };
        for c in columns_in_order {
            let col = &order[c];
            let neighbours = if left_to_right { &preds } else { &succs };
            // Desired row of each element's anchor: the mean of its
            // neighbours' anchors; an element with none stays.
            let desired: Vec<(f64, f64)> = col
                .iter()
                .map(|&i| {
                    let ns = &neighbours[i];
                    if ns.is_empty() {
                        ((elements[i].y + anchor(&elements[i])) as f64, 0.25)
                    } else {
                        let mean = ns
                            .iter()
                            .map(|&j| (elements[j].y + anchor(&elements[j])) as f64)
                            .sum::<f64>()
                            / ns.len() as f64;
                        (mean, 1.0 + ns.len() as f64)
                    }
                })
                .collect();
            let placed = fit_in_order(
                &col.iter().map(|&i| (elements[i].h, anchor(&elements[i]))).collect::<Vec<_>>(),
                &desired,
                gap,
            );
            for (k, &i) in col.iter().enumerate() {
                elements[i].y = placed[k];
            }
        }
    }
    // Leave a row above everything for the badges over nodes.
    let min_y = elements.iter().map(|e| e.y).min().unwrap_or(0);
    let top_margin = if lod == Lod::Full { 1 } else { 0 };
    for e in &mut elements {
        e.y = e.y - min_y + top_margin;
    }

    // ── 5. channels and columns ──
    // Rows each hop enters its target by: a node's incoming hops spread over
    // its left side in the order of the rows they come from.
    let mut in_row: HashMap<usize, i32> = HashMap::new();
    for (t, element) in elements.iter().enumerate() {
        let mut incoming: Vec<usize> = (0..hops.len()).filter(|&h| hops[h].to == t).collect();
        incoming.sort_by_key(|&h| {
            let src = &elements[hops[h].from];
            (src.y + anchor(src), hops[h].edge)
        });
        match element.node {
            None => {
                for h in incoming {
                    in_row.insert(h, element.y);
                }
            }
            Some(_) => {
                let card_top = element.y + element.stack_rise;
                let card_h = element.h - element.stack_rise;
                // A card is entered between its borders; a one-line node on
                // any of its rows.
                let interior: Vec<i32> = if card_h >= 3 && matches!(lod, Lod::Full | Lod::Compact) {
                    (card_top + 1..card_top + card_h - 1).collect()
                } else {
                    (card_top..card_top + card_h).collect()
                };
                if interior.len() >= incoming.len() {
                    // Centre the run of rows on the anchor where it fits.
                    let start = (anchor(element) - element.stack_rise - 1).max(0) as usize;
                    let start = start.min(interior.len() - incoming.len().max(1).min(interior.len()));
                    for (k, h) in incoming.iter().enumerate() {
                        in_row.insert(*h, interior[(start + k).min(interior.len() - 1)]);
                    }
                } else {
                    // A one-line node: everything enters on its line.
                    for h in incoming {
                        in_row.insert(h, interior[0]);
                    }
                }
            }
        }
    }

    // Column widths, and the bands the columns fall in. A band is decided
    // before routing: the columns of one band side by side, a new band where
    // the width runs out.
    let column_width: Vec<i32> = (0..columns)
        .map(|c| order[c].iter().map(|&i| elements[i].w).max().unwrap_or(1))
        .collect();
    let label_width: Vec<i32> = (0..columns)
        .map(|c| {
            // Labels sit on the branch into a node in column c+1.
            hops.iter()
                .filter(|h| elements[h.from].column == c && elements[h.to].node.is_some())
                .map(|h| text::width(&scene.edges[h.edge].label) as i32)
                .max()
                .unwrap_or(0)
                .min(24)
        })
        .collect();
    let chip_room = |c: usize| -> i32 {
        let steps_leave = hops.iter().any(|h| {
            elements[h.from].column == c
                && elements[h.from]
                    .node
                    .is_some_and(|i| scene.nodes[i].role == NodeRole::Step)
        });
        if options.chip_width > 0 && steps_leave {
            options.chip_width + 1
        } else {
            0
        }
    };
    let min_channel = match lod {
        Lod::Dot => 3,
        Lod::Glyph => 4,
        _ => 6,
    };
    let wrapped_width = match options.arrangement {
        Arrangement::Rows => None,
        Arrangement::Wrapped { width } => Some(width),
    };
    let mut band_of_column = vec![0usize; columns];
    {
        let mut x = 0;
        let mut band = 0;
        for c in 0..columns {
            if let Some(width) = wrapped_width {
                if x > 0 && x + column_width[c] > width {
                    band += 1;
                    x = 0;
                }
            }
            band_of_column[c] = band;
            // An estimate of the channel: tracks are not known yet, labels and
            // chips are.
            x += column_width[c] + 2 + chip_room(c) + label_width[c] + 4;
        }
    }
    let bands = band_of_column.iter().copied().max().unwrap_or(0) + 1;
    let crosses = |h: &Hop| band_of_column[elements[h.from].column] != band_of_column[elements[h.to].column];

    // Per channel: nets (one per element leaving into it), their vertical
    // extent within the band, and a track each. A hop to another band leaves
    // the band by a lane of its own, after the tracks.
    struct Net {
        top: i32,
        bottom: i32,
        track: usize,
        /// The row it leaves its element by.
        start: i32,
        /// The rows it enters its targets by.
        branches: Vec<i32>,
    }
    let mut nets_by_channel: Vec<Vec<Net>> = (0..columns).map(|_| Vec::new()).collect();
    let mut net_of_element: HashMap<usize, (usize, usize)> = HashMap::new();
    let mut exit_lane: HashMap<usize, i32> = HashMap::new(); // crossing hop → lane within its channel
    let mut exit_lanes_in_channel = vec![0i32; columns];
    for c in 0..columns.saturating_sub(1) {
        let mut nets: Vec<Net> = Vec::new();
        for &i in &order[c] {
            let leaving: Vec<usize> = (0..hops.len()).filter(|&h| hops[h].from == i).collect();
            if leaving.is_empty() {
                continue;
            }
            let start = elements[i].y + anchor(&elements[i]);
            let mut top = start;
            let mut bottom = start;
            let mut branches = Vec::new();
            for &h in &leaving {
                if crosses(&hops[h]) {
                    exit_lane.insert(h, exit_lanes_in_channel[c]);
                    exit_lanes_in_channel[c] += 1;
                    continue;
                }
                top = top.min(in_row[&h]);
                bottom = bottom.max(in_row[&h]);
                branches.push(in_row[&h]);
            }
            net_of_element.insert(i, (c, nets.len()));
            nets.push(Net {
                top,
                bottom,
                track: 0,
                start,
                branches,
            });
        }
        // Constrained left-edge track assignment. A net's start stub runs
        // from the channel's left to its trunk, and its branches from its
        // trunk to the channel's right. Where one net leaves on a row another
        // net enters a target by, the leaving net's trunk must be left of the
        // entering one's, or the two lines would share that row. Tracks fill
        // from the left: a net goes on the leftmost track where it overlaps
        // no net and every net it must be right of is already left of it.
        // Touching nets would merge into one line, so a track is free only
        // strictly below its last net.
        let count = nets.len();
        let must_be_right_of: Vec<Vec<usize>> = (0..count)
            .map(|a| {
                (0..count)
                    .filter(|&b| b != a && nets[a].branches.contains(&nets[b].start))
                    .collect()
            })
            .collect();
        let mut assigned: Vec<Option<usize>> = vec![None; count];
        let mut by_top: Vec<usize> = (0..count).collect();
        by_top.sort_by_key(|&k| (nets[k].top, nets[k].bottom, k));
        let mut track = 0usize;
        while assigned.iter().any(Option::is_none) {
            let mut last_bottom = i32::MIN;
            let mut placed_any = false;
            for &k in &by_top {
                if assigned[k].is_some() || nets[k].top <= last_bottom {
                    continue;
                }
                let ready = must_be_right_of[k]
                    .iter()
                    .all(|&b| assigned[b].is_some_and(|t| t < track));
                if !ready {
                    continue;
                }
                assigned[k] = Some(track);
                last_bottom = nets[k].bottom;
                placed_any = true;
            }
            if !placed_any {
                // Every remaining net waits on another: a cycle of nets that
                // swap rows. Break it at the net that starts highest; the
                // geometry check reports any line it then shares.
                let k = *by_top
                    .iter()
                    .find(|&&k| assigned[k].is_none())
                    .expect("a net is unassigned");
                assigned[k] = Some(track);
            }
            track += 1;
        }
        for (k, net) in nets.iter_mut().enumerate() {
            net.track = assigned[k].expect("every net is assigned");
        }
        nets_by_channel[c] = nets;
    }
    let tracks_in_channel: Vec<i32> = (0..columns)
        .map(|c| nets_by_channel[c].iter().map(|n| n.track as i32 + 1).max().unwrap_or(0))
        .collect();
    let channel_width: Vec<i32> = (0..columns)
        .map(|c| {
            if c + 1 >= columns {
                return 0;
            }
            let label = if label_width[c] > 0 { label_width[c] + 3 } else { 0 };
            (2 + chip_room(c) + tracks_in_channel[c] + exit_lanes_in_channel[c] + label + 2).max(min_channel)
        })
        .collect();

    // Hops into a later band come in from the left by a gutter lane of their
    // own: every band starts after the gutter.
    let crossing_hops: Vec<usize> = (0..hops.len()).filter(|&h| crosses(&hops[h])).collect();
    let gutter = if crossing_hops.is_empty() {
        0
    } else {
        crossing_hops.len() as i32 + 2
    };
    let mut column_x = vec![0i32; columns];
    {
        let mut x = gutter;
        for c in 0..columns {
            if c > 0 && band_of_column[c] != band_of_column[c - 1] {
                x = gutter;
            }
            column_x[c] = x;
            x += column_width[c] + channel_width[c];
        }
    }
    let gutter_lane: HashMap<usize, i32> = crossing_hops
        .iter()
        .enumerate()
        .map(|(k, &h)| (h, gutter - 2 - k as i32))
        .collect();

    // Each band's top; under each band, a highway row per hop leaving it.
    let band_height: Vec<i32> = (0..bands)
        .map(|b| {
            elements
                .iter()
                .filter(|e| band_of_column[e.column] == b)
                .map(|e| e.y + e.h)
                .max()
                .unwrap_or(0)
        })
        .collect();
    let mut band_y = vec![0i32; bands];
    let mut highway_row: HashMap<usize, i32> = HashMap::new();
    {
        let mut y = 0;
        for b in 0..bands {
            band_y[b] = y;
            y += band_height[b] + 1;
            let leaving: Vec<usize> = crossing_hops
                .iter()
                .copied()
                .filter(|&h| band_of_column[elements[hops[h].from].column] == b)
                .collect();
            for (k, h) in leaving.iter().enumerate() {
                highway_row.insert(*h, y + k as i32);
            }
            y += leaving.len() as i32 + 1;
        }
    }
    let band_shift = |c: usize| band_y[band_of_column[c]];

    // ── placed nodes ──
    let mut placed_nodes: Vec<PlacedNode> = Vec::with_capacity(n);
    for (i, node) in scene.nodes.iter().enumerate() {
        let el = &elements[element_of_node[i]];
        let x = column_x[el.column];
        let y = el.y + band_shift(el.column);
        let rect = Rect { x, y, w: el.w, h: el.h };
        let card = Rect {
            x,
            y: y + el.stack_rise,
            w: el.w - el.stack_shift,
            h: el.h - el.stack_rise,
        };
        placed_nodes.push(PlacedNode {
            id: node.id.clone(),
            rect,
            card,
            column: el.column,
            band: band_of_column[el.column],
            out_row: y + anchor(el),
        });
    }

    // ── routed edges ──
    // A net's id is unique across the layout.
    let net_id = |channel: usize, index: usize| channel * 100_000 + index;
    let mut routed: Vec<RoutedEdge> = Vec::with_capacity(scene.edges.len());
    for (e, edge) in scene.edges.iter().enumerate() {
        let edge_hops: Vec<usize> = (0..hops.len()).filter(|&h| hops[h].edge == e).collect();
        let mut points: Vec<(i32, i32)> = Vec::new();
        let mut nets: Vec<usize> = Vec::new();
        let mut label_at = None;
        let mut chip_at = None;
        let push = |points: &mut Vec<(i32, i32)>, nets: &mut Vec<usize>, p: (i32, i32), net: usize| {
            if let Some(&last) = points.last() {
                if last == p {
                    return;
                }
                nets.push(net);
            }
            points.push(p);
        };
        for (k, &h) in edge_hops.iter().enumerate() {
            let hop = hops[h];
            let from = &elements[hop.from];
            let to = &elements[hop.to];
            let c = from.column;
            let start_y = from.y + anchor(from) + band_shift(c);
            let start_x = column_x[c] + if from.node.is_some() { from.w - from.stack_shift } else { column_width[c] };
            let chip = chip_room(c);
            let channel_start = column_x[c] + column_width[c];
            let (channel, index) = net_of_element[&hop.from];
            // The whole of an edge is one net: past its first channel it runs
            // through slots of its own, still the same line from the same node.
            let id = {
                let (first_channel, first_index) = net_of_element[&hops[edge_hops[0]].from];
                net_id(first_channel, first_index)
            };
            if k == 0 {
                points.push((start_x, start_y));
                if chip > 0 {
                    chip_at = Some((channel_start + 1, start_y - 1, options.chip_width));
                }
            } else if from.node.is_none() {
                // Through a slot: across its column on its row.
                push(&mut points, &mut nets, (column_x[c] + column_width[c] - 1, start_y), id);
            }
            let target_x = column_x[to.column] - 1;
            let target_y = in_row[&h] + band_shift(to.column);
            if crosses(&hop) {
                let exit_x = channel_start + 1 + chip + tracks_in_channel[c] + exit_lane[&h];
                let highway = highway_row[&h];
                let lane_x = gutter_lane[&h];
                push(&mut points, &mut nets, (exit_x, start_y), id);
                push(&mut points, &mut nets, (exit_x, highway), id);
                push(&mut points, &mut nets, (lane_x, highway), id);
                push(&mut points, &mut nets, (lane_x, target_y), id);
                if to.node.is_some() {
                    push(&mut points, &mut nets, (target_x, target_y), id);
                    if !edge.label.is_empty() && target_x - lane_x - 2 > 0 {
                        label_at = Some((lane_x + 2, target_y, target_x - lane_x - 2));
                    }
                } else {
                    push(&mut points, &mut nets, (column_x[to.column], target_y), id);
                }
                continue;
            }
            let track_x = channel_start + 1 + chip + nets_by_channel[channel][index].track as i32;
            push(&mut points, &mut nets, (track_x, start_y), id);
            push(&mut points, &mut nets, (track_x, target_y), id);
            if to.node.is_some() {
                push(&mut points, &mut nets, (target_x, target_y), id);
                if !edge.label.is_empty() && target_x - track_x - 2 > 0 {
                    label_at = Some((track_x + 2, target_y, target_x - track_x - 2));
                }
            } else {
                push(&mut points, &mut nets, (column_x[to.column], target_y), id);
            }
        }
        routed.push(RoutedEdge {
            id: edge.id.clone(),
            from: edge.from.clone(),
            to: edge.to.clone(),
            points,
            nets,
            label_at,
            chip_at,
        });
    }

    let width = (0..columns)
        .map(|c| column_x[c] + column_width[c])
        .max()
        .unwrap_or(0)
        .max(routed.iter().flat_map(|r| r.points.iter().map(|p| p.0 + 1)).max().unwrap_or(0));
    let height = placed_nodes
        .iter()
        .map(|p| p.rect.bottom())
        .chain(routed.iter().flat_map(|r| r.points.iter().map(|p| p.1 + 1)))
        .max()
        .unwrap_or(0);
    Ok(Layout {
        nodes: placed_nodes,
        edges: routed,
        width,
        height,
        lod,
        arrangement: options.arrangement,
    })
}

/// Place the elements of one column, top to bottom in their order, each
/// `(height, anchor offset)`, as close as order and spacing allow to its
/// desired anchor row `(row, weight)`: the weighted least-squares fit of an
/// increasing sequence (pool-adjacent-violators). Exact and deterministic.
fn fit_in_order(elements: &[(i32, i32)], desired: &[(f64, f64)], gap: i32) -> Vec<i32> {
    // y_k = z_k + offset_k, where offset is what the elements above take;
    // order and spacing hold exactly when z is non-decreasing.
    let mut offset = Vec::with_capacity(elements.len());
    let mut running = 0;
    for &(h, _) in elements {
        offset.push(running);
        running += h + gap;
    }
    // Target for z_k: the desired top, less the offset.
    let targets: Vec<(f64, f64)> = elements
        .iter()
        .zip(desired)
        .zip(&offset)
        .map(|((&(_, anchor), &(row, weight)), &off)| (row - anchor as f64 - off as f64, weight))
        .collect();
    // Pool adjacent violators: blocks of (weighted mean, weight, count).
    let mut blocks: Vec<(f64, f64, usize)> = Vec::new();
    for &(value, weight) in &targets {
        blocks.push((value, weight, 1));
        while blocks.len() >= 2 {
            let (v2, w2, c2) = blocks[blocks.len() - 1];
            let (v1, w1, c1) = blocks[blocks.len() - 2];
            if v1 <= v2 {
                break;
            }
            blocks.pop();
            blocks.pop();
            blocks.push(((v1 * w1 + v2 * w2) / (w1 + w2), w1 + w2, c1 + c2));
        }
    }
    let mut z: Vec<f64> = Vec::with_capacity(elements.len());
    for (value, _, count) in blocks {
        for _ in 0..count {
            z.push(value);
        }
    }
    // Round, keeping the order exact after rounding.
    let mut out = Vec::with_capacity(elements.len());
    let mut previous: Option<i32> = None;
    for (k, value) in z.into_iter().enumerate() {
        let mut zk = value.round() as i32;
        if let Some(p) = previous {
            zk = zk.max(p);
        }
        previous = Some(zk);
        out.push(zk + offset[k]);
    }
    out
}

/// The richest level of detail at which the whole scene fits `width` ×
/// `height`, in the first of `arrangements` it fits in at that level, and
/// its layout; the least detailed level in the last arrangement when none
/// fits. More detail is worth more than a preferred arrangement: a machine
/// too wide for one row of cards is wrapped into bands of cards before it
/// is shrunk to lines.
pub fn fit(
    scene: &Scene,
    width: i32,
    height: i32,
    arrangements: &[Arrangement],
    chip_width: i32,
) -> Result<Layout, LayoutError> {
    let mut last = None;
    for lod in Lod::ALL {
        for &arrangement in arrangements {
            let laid = layout(
                scene,
                &LayoutOptions {
                    lod,
                    arrangement,
                    chip_width: if matches!(lod, Lod::Full | Lod::Compact) { chip_width } else { 0 },
                },
            )?;
            if laid.width <= width && laid.height <= height {
                return Ok(laid);
            }
            last = Some(laid);
        }
    }
    last.ok_or(LayoutError::NoArrangement)
}

/// The scene at `lod`, in the first of `arrangements` that fits `width`;
/// in the last when none does.
pub fn fit_width(
    scene: &Scene,
    lod: Lod,
    width: i32,
    arrangements: &[Arrangement],
    chip_width: i32,
) -> Result<Layout, LayoutError> {
    let mut last = None;
    for &arrangement in arrangements {
        let laid = layout(scene, &LayoutOptions { lod, arrangement, chip_width })?;
        if laid.width <= width {
            return Ok(laid);
        }
        last = Some(laid);
    }
    last.ok_or(LayoutError::NoArrangement)
}

/// Every node id in `layout`'s columns, in reading order: column by column,
/// top to bottom — the order Tab walks.
pub fn reading_order(layout: &Layout) -> Vec<String> {
    let mut keyed: BTreeMap<(usize, i32, i32), String> = BTreeMap::new();
    for node in &layout.nodes {
        keyed.insert((node.band * 1000 + node.column, node.rect.y, node.rect.x), node.id.clone());
    }
    keyed.into_values().collect()
}

/// The ids of the nodes whose rects overlap — a layout defect; empty for
/// every correct layout.
pub fn overlapping(layout: &Layout) -> BTreeSet<(String, String)> {
    let mut out = BTreeSet::new();
    for (i, a) in layout.nodes.iter().enumerate() {
        for b in &layout.nodes[i + 1..] {
            if a.rect.intersects(&b.rect) {
                out.insert((a.id.clone(), b.id.clone()));
            }
        }
    }
    out
}
