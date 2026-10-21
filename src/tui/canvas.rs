//! Drawing a laid-out scene into a ratatui buffer.
//!
//! Lines are drawn cell by cell from the directions that meet in each cell:
//! every segment of an edge marks the cells it passes with the sides it
//! leaves them by, and the glyph of a cell is the one that connects exactly
//! those sides ([`GlyphSet::line`]). Directions merge only within one net —
//! the edges leaving one node — so a junction is drawn only where a net
//! branches. Where two nets cross, the horizontal line passes over the
//! vertical one: a crossing is never drawn as a junction.
//!
//! Everything is drawn through a [`Surface`]: a window of the scene (the
//! viewport, scrolled by `origin`) onto an area of the buffer, clipped, so
//! the same code draws the live view and the whole picture for `dag-viz`.

use std::collections::HashMap;

use ratatui::buffer::Buffer;
use ratatui::layout::Rect as Area;
use ratatui::style::{Modifier, Style};

use crate::tui::layout::{Layout, Lod, PlacedNode, Rect, PORT_ROWS};
use crate::tui::scene::{
    EdgeTone, FlowClass, ItemsState, NodeRole, NodeState, RateBucket, Scene, SceneEdge, SceneNode,
};
use crate::tui::text;
use crate::tui::theme::{GlyphSet, Theme, Tone, Weight, EAST, NORTH, SOUTH, WEST};

/// A window of the scene onto an area of a buffer.
pub struct Surface<'a> {
    pub buffer: &'a mut Buffer,
    pub area: Area,
    /// The scene cell drawn at the area's top-left corner.
    pub origin: (i32, i32),
}

impl Surface<'_> {
    fn to_screen(&self, x: i32, y: i32) -> Option<(u16, u16)> {
        let sx = x - self.origin.0;
        let sy = y - self.origin.1;
        if sx < 0 || sy < 0 || sx >= self.area.width as i32 || sy >= self.area.height as i32 {
            return None;
        }
        Some((self.area.x + sx as u16, self.area.y + sy as u16))
    }

    /// Put one character at scene cell `(x, y)`, when it is in the window.
    pub fn put(&mut self, x: i32, y: i32, ch: char, style: Style) {
        if let Some((sx, sy)) = self.to_screen(x, y) {
            let cell = &mut self.buffer[(sx, sy)];
            cell.set_char(ch);
            cell.set_style(style);
        }
    }

    /// Patch the style of scene cell `(x, y)`.
    pub fn style(&mut self, x: i32, y: i32, style: Style) {
        if let Some((sx, sy)) = self.to_screen(x, y) {
            let cell = &mut self.buffer[(sx, sy)];
            cell.set_style(style);
        }
    }

    /// Write `text` from scene cell `(x, y)`, at most `max` columns of it.
    /// A wide character takes two cells, the second left blank, as ratatui
    /// lays them out.
    pub fn text(&mut self, x: i32, y: i32, value: &str, max: i32, style: Style) -> i32 {
        if max <= 0 {
            return 0;
        }
        let cut = text::truncate(value, max as usize);
        let mut cx = x;
        for grapheme in unicode_segmentation::UnicodeSegmentation::graphemes(cut.as_str(), true) {
            let w = text::width(grapheme) as i32;
            if w == 0 {
                continue;
            }
            if let Some((sx, sy)) = self.to_screen(cx, y) {
                // A wide grapheme whose second half falls outside the window
                // is not drawn at all.
                if w == 2 && self.to_screen(cx + 1, y).is_none() {
                    break;
                }
                let cell = &mut self.buffer[(sx, sy)];
                cell.set_symbol(grapheme);
                cell.set_style(style);
                if w == 2 {
                    let (sx2, sy2) = self.to_screen(cx + 1, y).expect("checked above");
                    let next = &mut self.buffer[(sx2, sy2)];
                    next.set_symbol("");
                    next.set_style(style);
                }
            }
            cx += w;
        }
        cx - x
    }

    /// Fill a rectangle of scene cells with spaces in `style`.
    pub fn fill(&mut self, rect: Rect, style: Style) {
        for y in rect.y..rect.bottom() {
            for x in rect.x..rect.right() {
                self.put(x, y, ' ', style);
            }
        }
    }
}

/// What the interactive view adds to a drawing.
#[derive(Debug, Clone, Default)]
pub struct Overlay {
    /// The node with the focus.
    pub focused: Option<String>,
    /// The edge with the focus (the editor's connection actions).
    pub focused_edge: Option<String>,
    /// Animation frame: spinners and moving flow.
    pub frame: usize,
}

/// One line contribution to a cell.
#[derive(Debug, Clone, Copy)]
struct Mark {
    net: usize,
    bits: u8,
    weight: Weight,
    style: Style,
    /// Paint priority among nets: a flowing edge over an idle one.
    priority: u8,
}

/// How an edge is drawn: weight, colour, priority.
fn edge_look(edge: &SceneEdge, theme: &Theme) -> (Weight, Style, u8) {
    match edge.flow {
        Some(FlowClass::Flowing(bucket)) => {
            let tone = match bucket {
                RateBucket::Slow => Tone::Accent,
                RateBucket::Medium => Tone::Accent,
                RateBucket::Fast => Tone::Success,
            };
            (Weight::Heavy, theme.fg(tone), 5)
        }
        Some(FlowClass::CreditWait) => (Weight::Dashed, theme.fg(Tone::Warning), 4),
        Some(FlowClass::Stalled) => (Weight::Light, theme.fg(Tone::Danger), 4),
        Some(FlowClass::Error) => (Weight::Light, theme.fg(Tone::Danger), 3),
        Some(FlowClass::Done) => (Weight::Light, theme.fg(Tone::Success), 2),
        None => match edge.tone {
            EdgeTone::Items(ItemsState::Failure) => (Weight::Light, theme.fg(Tone::Danger), 2),
            EdgeTone::Items(ItemsState::Success) => (Weight::Light, theme.fg(Tone::Success), 2),
            EdgeTone::Items(ItemsState::Open) => (Weight::Light, theme.fg(Tone::Source), 1),
            EdgeTone::Neutral => (Weight::Light, theme.fg(Tone::Muted), 0),
        },
    }
}

/// The cells of an orthogonal path, in order, with the sides each leaves by.
fn path_cells(points: &[(i32, i32)]) -> Vec<((i32, i32), u8)> {
    let mut cells: Vec<((i32, i32), u8)> = Vec::new();
    let add = |cells: &mut Vec<((i32, i32), u8)>, p: (i32, i32), bits: u8| {
        if let Some(last) = cells.last_mut() {
            if last.0 == p {
                last.1 |= bits;
                return;
            }
        }
        cells.push((p, bits));
    };
    for pair in points.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        if a.1 == b.1 {
            let step = if b.0 > a.0 { 1 } else { -1 };
            let (forward, back) = if step > 0 { (EAST, WEST) } else { (WEST, EAST) };
            let mut x = a.0;
            while x != b.0 {
                add(&mut cells, (x, a.1), forward);
                x += step;
                add(&mut cells, (x, a.1), back);
            }
        } else {
            let step = if b.1 > a.1 { 1 } else { -1 };
            let (forward, back) = if step > 0 { (SOUTH, NORTH) } else { (NORTH, SOUTH) };
            let mut y = a.1;
            while y != b.1 {
                add(&mut cells, (a.0, y), forward);
                y += step;
                add(&mut cells, (a.0, y), back);
            }
        }
    }
    cells
}

/// Draw `scene` as laid out by `layout`.
pub fn draw(surface: &mut Surface<'_>, scene: &Scene, layout: &Layout, theme: &Theme, overlay: &Overlay) {
    let glyphs = theme.glyphs;
    let canvas = theme.canvas();
    surface.fill(
        Rect {
            x: surface.origin.0,
            y: surface.origin.1,
            w: surface.area.width as i32,
            h: surface.area.height as i32,
        },
        canvas,
    );

    let edge_by_id: HashMap<&str, &SceneEdge> = scene.edges.iter().map(|e| (e.id.as_str(), e)).collect();

    // ── lines ──
    let mut marks: HashMap<(i32, i32), Vec<Mark>> = HashMap::new();
    let mut flowing_paths: Vec<(Vec<(i32, i32)>, Style)> = Vec::new();
    for routed in &layout.edges {
        let Some(edge) = edge_by_id.get(routed.id.as_str()) else {
            continue;
        };
        let (weight, style, priority) = edge_look(edge, theme);
        let focused = overlay.focused_edge.as_deref() == Some(edge.id.as_str());
        let style = if focused {
            theme.fg(Tone::Accent).add_modifier(Modifier::BOLD)
        } else {
            style
        };
        let weight = if focused { Weight::Heavy } else { weight };
        // A net per segment: the cells of each segment belong to its net.
        let mut segment_cells: Vec<((i32, i32), u8, usize)> = Vec::new();
        for (s, pair) in routed.points.windows(2).enumerate() {
            let net = routed.nets[s];
            for (cell, bits) in path_cells(pair) {
                segment_cells.push((cell, bits, net));
            }
        }
        // The first cell joins the port on its source's border.
        if let Some(first) = segment_cells.first_mut() {
            first.1 |= WEST;
        }
        if matches!(edge.flow, Some(FlowClass::Flowing(_))) {
            flowing_paths.push((path_cells(&routed.points).into_iter().map(|c| c.0).collect(), style));
        }
        for (cell, bits, net) in segment_cells {
            let list = marks.entry(cell).or_default();
            match list.iter_mut().find(|m| m.net == net) {
                Some(mark) => {
                    mark.bits |= bits;
                    if (weight, priority) > (mark.weight, mark.priority) {
                        mark.weight = weight;
                        mark.style = style;
                        mark.priority = priority;
                    }
                }
                None => list.push(Mark {
                    net,
                    bits,
                    weight,
                    style,
                    priority,
                }),
            }
        }
    }
    for ((x, y), list) in &marks {
        let (bits, weight, style) = if list.len() == 1 {
            (list[0].bits, list[0].weight, list[0].style)
        } else {
            // Nets that meet only cross. The horizontal one passes over, so
            // the vertical one reads as going under it.
            let horizontal = list
                .iter()
                .filter(|m| m.bits & (EAST | WEST) != 0 && m.bits & (NORTH | SOUTH) == 0)
                .max_by_key(|m| (m.priority, m.weight));
            match horizontal {
                Some(m) => (m.bits, m.weight, m.style),
                None => {
                    // Two corners in one cell: the layout gives every net its
                    // own track, so only a shared start or end row can do it.
                    let top = list.iter().max_by_key(|m| (m.priority, m.weight)).expect("non-empty");
                    (top.bits, top.weight, top.style)
                }
            }
        };
        let ch = glyphs.line(bits, weight, true);
        surface.put(*x, *y, ch, canvas.patch(style));
    }
    // Flow moves: a bright pulse travels each flowing edge.
    if theme.motion {
        for (cells, style) in &flowing_paths {
            for (i, (x, y)) in cells.iter().enumerate() {
                if (i + overlay.frame) % 6 == 0 {
                    surface.style(*x, *y, style.add_modifier(Modifier::BOLD | Modifier::REVERSED));
                }
            }
        }
    }
    // Arrowheads, labels, chips.
    for routed in &layout.edges {
        let Some(edge) = edge_by_id.get(routed.id.as_str()) else {
            continue;
        };
        let (_, style, _) = edge_look(edge, theme);
        if let Some(&(x, y)) = routed.points.last() {
            if layout.nodes.iter().any(|n| n.id == routed.to && n.card.x == x + 1) {
                surface.put(x, y, glyphs.arrow_right(), canvas.patch(style));
            }
        }
        if let Some((x, y, room)) = routed.label_at {
            let label = text::truncate(&edge.label, (room - 2).max(0) as usize);
            if !label.is_empty() {
                surface.text(x, y, &format!(" {label} "), room, canvas.patch(theme.fg(Tone::Secondary)));
            }
        }
        if let (Some((x, y, room)), Some(chip)) = (routed.chip_at, &edge.chip) {
            surface.text(x, y, chip, room, canvas.patch(style));
        }
    }

    // ── nodes ──
    for placed in &layout.nodes {
        let Some(node) = scene.node(&placed.id) else {
            continue;
        };
        let focused = overlay.focused.as_deref() == Some(node.id.as_str());
        match layout.lod {
            Lod::Full | Lod::Compact => draw_card(surface, node, placed, layout.lod, theme, overlay, focused),
            Lod::Glyph => draw_line_node(surface, node, placed, theme, overlay, focused),
            Lod::Dot => draw_dot(surface, node, placed, theme, overlay, focused),
        }
        // A node with edges out shows where they leave.
        if layout.edges.iter().any(|e| e.from == node.id) && matches!(layout.lod, Lod::Full | Lod::Compact) {
            let x = placed.card.right() - 1;
            surface.put(x, placed.out_row, glyphs.line(NORTH | SOUTH | EAST, Weight::Light, false), canvas.patch(border_style(node, theme, focused)));
        }
    }
}

fn border_style(node: &SceneNode, theme: &Theme, focused: bool) -> Style {
    if focused {
        return theme.fg(Tone::Accent).add_modifier(Modifier::BOLD);
    }
    match node.state.tone() {
        Some(Tone::Danger) => theme.fg(Tone::Danger),
        Some(Tone::Success) if node.role == NodeRole::Step || node.role == NodeRole::Output => {
            theme.fg(Tone::Success)
        }
        Some(Tone::Accent) => theme.fg(Tone::Accent),
        _ => match node.role {
            NodeRole::Step => theme.fg(Tone::BorderStrong),
            NodeRole::Input => theme.fg(Tone::Source),
            NodeRole::Output => theme.fg(Tone::Target),
            _ => theme.fg(Tone::Border),
        },
    }
}

fn draw_box(surface: &mut Surface<'_>, rect: Rect, glyphs: GlyphSet, style: Style, dashed: bool) {
    let weight = if dashed { Weight::Dashed } else { Weight::Light };
    let (x0, y0, x1, y1) = (rect.x, rect.y, rect.right() - 1, rect.bottom() - 1);
    surface.put(x0, y0, glyphs.line(EAST | SOUTH, Weight::Light, true), style);
    surface.put(x1, y0, glyphs.line(SOUTH | WEST, Weight::Light, true), style);
    surface.put(x0, y1, glyphs.line(NORTH | EAST, Weight::Light, true), style);
    surface.put(x1, y1, glyphs.line(NORTH | WEST, Weight::Light, true), style);
    for x in x0 + 1..x1 {
        surface.put(x, y0, glyphs.line(EAST | WEST, weight, false), style);
        surface.put(x, y1, glyphs.line(EAST | WEST, weight, false), style);
    }
    for y in y0 + 1..y1 {
        surface.put(x0, y, glyphs.line(NORTH | SOUTH, weight, false), style);
        surface.put(x1, y, glyphs.line(NORTH | SOUTH, weight, false), style);
    }
}

fn draw_card(
    surface: &mut Surface<'_>,
    node: &SceneNode,
    placed: &PlacedNode,
    lod: Lod,
    theme: &Theme,
    overlay: &Overlay,
    focused: bool,
) {
    let glyphs = theme.glyphs;
    let card = placed.card;
    let border = border_style(node, theme, focused);
    let ground = theme.surface(node.role == NodeRole::Step);
    let dashed = node.state == NodeState::NotProduced;

    // A stack's back card, up and to the right.
    if node.stack.is_some() && placed.rect.w > card.w {
        let back = Rect {
            x: card.x + 1,
            y: card.y - 1,
            w: card.w,
            h: card.h,
        };
        surface.fill(back, ground);
        draw_box(surface, back, glyphs, theme.fg(Tone::Border).patch(ground), dashed);
    }
    surface.fill(card, ground);
    draw_box(surface, card, glyphs, border.patch(ground), dashed);

    let inner_x = card.x + 2;
    let inner_w = card.w - 4;
    // Title row: mark, title, state.
    let receded = matches!(node.state, NodeState::Done) && node.role == NodeRole::Step;
    let title_style = if receded {
        theme.fg(Tone::Secondary)
    } else {
        theme.fg(Tone::Ink).add_modifier(Modifier::BOLD)
    }
    .patch(ground);
    let icon_tone = match node.tone {
        Tone::Source | Tone::Target => node.tone,
        _ => Tone::Secondary,
    };
    surface.put(inner_x, card.y + 1, glyphs.icon(node.icon), theme.fg(icon_tone).patch(ground));
    let mark = node.state.mark(overlay.frame);
    let title_room = inner_w - 2 - if mark.is_some() { 2 } else { 0 };
    surface.text(inner_x + 2, card.y + 1, &node.title, title_room, title_style);
    if let Some(mark) = mark {
        let tone = node.state.tone().unwrap_or(Tone::Muted);
        surface.put(card.right() - 3, card.y + 1, glyphs.status(mark), theme.fg(tone).add_modifier(Modifier::BOLD).patch(ground));
    }
    let mut row = card.y + 2;
    if lod == Lod::Full {
        surface.text(inner_x, row, &node.subtitle, inner_w, theme.fg(Tone::Secondary).patch(ground));
        row += 1;
        if let Some(meta) = &node.meta {
            let tone = match node.state {
                NodeState::Tolerated | NodeState::Skipped => Tone::Warning,
                NodeState::Failed => Tone::Danger,
                _ => Tone::Muted,
            };
            surface.text(inner_x, row, meta, inner_w, theme.fg(tone).patch(ground));
            row += 1;
        }
        if !node.ports.is_empty() {
            // The divider between the card and its arguments.
            let style = theme.fg(Tone::Border).patch(ground);
            surface.put(card.x, row, glyphs.line(NORTH | SOUTH | EAST, Weight::Light, false), border.patch(ground));
            for x in card.x + 1..card.right() - 1 {
                surface.put(x, row, glyphs.line(EAST | WEST, Weight::Light, false), style);
            }
            surface.put(card.right() - 1, row, glyphs.line(NORTH | SOUTH | WEST, Weight::Light, false), border.patch(ground));
            row += 1;
            let shown = node.ports.len().min(PORT_ROWS);
            for (k, port) in node.ports.iter().take(shown).enumerate() {
                if k + 1 == PORT_ROWS && node.ports.len() > PORT_ROWS {
                    surface.text(
                        inner_x,
                        row,
                        &format!("+{} more", node.ports.len() - PORT_ROWS + 1),
                        inner_w,
                        theme.fg(Tone::Muted).patch(ground),
                    );
                    row += 1;
                    break;
                }
                let value = port.value.clone().unwrap_or_else(|| "—".to_string());
                let value = text::truncate(&value, 14);
                let value_w = text::width(&value) as i32;
                let name_w = text::width(&port.name) as i32;
                let name_room = (inner_w - value_w - 2).max(1);
                let dot = match glyphs {
                    GlyphSet::Unicode => '·',
                    GlyphSet::Ascii => '.',
                };
                surface.text(inner_x, row, &port.name, name_room, theme.fg(Tone::Secondary).patch(ground));
                for x in inner_x + name_w.min(name_room) + 1..card.x + 2 + inner_w - value_w - 1 {
                    surface.put(x, row, dot, theme.fg(Tone::Border).patch(ground));
                }
                surface.text(card.x + 2 + inner_w - value_w, row, &value, value_w, theme.fg(Tone::Muted).patch(ground));
                row += 1;
            }
        }
    }
    let _ = row;

    // The bottom border: progress for a running step, the count of a stack.
    let bottom = card.bottom() - 1;
    if let NodeState::Active(Some(progress)) = node.state {
        let span = card.w - 2;
        let eighths = glyphs.bar_eighths();
        let filled = (progress.clamp(0.0, 1.0) * span as f32 * 8.0).round() as i32;
        for k in 0..span {
            let part = (filled - k * 8).clamp(0, 8) as usize;
            if part > 0 {
                surface.put(card.x + 1 + k, bottom, eighths[part], theme.fg(Tone::Accent).patch(ground));
            }
        }
    }
    if let Some(count) = node.stack {
        let pill = format!(" {} ", text::count(count, "item"));
        let w = text::width(&pill) as i32;
        let x = card.right() - 2 - w;
        if x > card.x + 1 {
            let tone = match node.role {
                NodeRole::Input => Tone::Source,
                NodeRole::Output => Tone::Target,
                _ => Tone::Secondary,
            };
            surface.text(x, bottom, &pill, w, theme.fg(tone).add_modifier(Modifier::BOLD).patch(ground));
        }
    }

    // Badges above the card: why it failed (left), its pools (right), LIVE.
    let mut left_x = card.x;
    if let Some(badge) = &node.failure {
        let label = format!(" {} {} ", glyphs.status(crate::tui::theme::StatusMark::Failed), badge.text);
        left_x += surface.text(left_x, placed.rect.y - 1, &label, placed.rect.w.max(20), theme.fg(badge.tone).patch(theme.soft(badge.tone)));
    }
    if node.live_lanes > 0 {
        let live = if theme.motion && overlay.frame % 8 < 4 { " ◉ LIVE " } else { " ○ LIVE " };
        let live = if glyphs == GlyphSet::Ascii { " LIVE " } else { live };
        surface.text(left_x, placed.rect.y - 1, live, 8, theme.fg(Tone::Danger).add_modifier(Modifier::BOLD).patch(theme.soft(Tone::Danger)));
    }
    if let Some(badge) = &node.pool {
        let label = format!(" {} ", badge.text);
        let w = text::width(&label) as i32;
        surface.text(card.right() - w, placed.rect.y - 1, &label, w, theme.fg(badge.tone).patch(theme.soft(badge.tone)));
    }
}

fn draw_line_node(surface: &mut Surface<'_>, node: &SceneNode, placed: &PlacedNode, theme: &Theme, overlay: &Overlay, focused: bool) {
    let glyphs = theme.glyphs;
    let rect = placed.card;
    let border = border_style(node, theme, focused);
    let ground = theme.surface(node.role == NodeRole::Step);
    surface.fill(rect, ground);
    let (open, close) = match (node.role, glyphs) {
        (NodeRole::Step, GlyphSet::Unicode) => ('⟨', '⟩'),
        (_, GlyphSet::Unicode) => ('[', ']'),
        (_, GlyphSet::Ascii) => ('[', ']'),
    };
    surface.put(rect.x, rect.y, open, border.patch(ground));
    surface.put(rect.right() - 1, rect.y, close, border.patch(ground));
    // A node several edges enter is as tall as they need; its sides go on.
    for y in rect.y + 1..rect.bottom() {
        surface.put(rect.x, y, glyphs.line(NORTH | SOUTH, Weight::Light, false), border.patch(ground));
        surface.put(rect.right() - 1, y, glyphs.line(NORTH | SOUTH, Weight::Light, false), border.patch(ground));
    }
    surface.put(rect.x + 1, rect.y, glyphs.icon(node.icon), theme.fg(node.tone).patch(ground));
    let mark = node.state.mark(overlay.frame);
    let room = rect.w - 4 - if mark.is_some() { 2 } else { 0 };
    surface.text(rect.x + 3, rect.y, &node.title, room, theme.fg(Tone::Ink).patch(ground));
    if let Some(mark) = mark {
        surface.put(rect.right() - 2, rect.y, glyphs.status(mark), theme.fg(node.state.tone().unwrap_or(Tone::Muted)).patch(ground));
    }
}

fn draw_dot(surface: &mut Surface<'_>, node: &SceneNode, placed: &PlacedNode, theme: &Theme, overlay: &Overlay, focused: bool) {
    let glyphs = theme.glyphs;
    let rect = placed.card;
    let tone = node.state.tone().unwrap_or(node.tone);
    let style = if focused {
        theme.fg(Tone::Accent).add_modifier(Modifier::REVERSED)
    } else {
        theme.fg(tone)
    };
    let mark = match node.state.mark(overlay.frame) {
        Some(mark) => glyphs.status(mark),
        None => glyphs.icon(node.icon),
    };
    surface.put(rect.x, rect.y, '(', style);
    surface.put(rect.x + 1, rect.y, mark, style.add_modifier(Modifier::BOLD));
    surface.put(rect.x + 2, rect.y, ')', style);
    for y in rect.y + 1..rect.bottom() {
        surface.put(rect.x, y, glyphs.line(NORTH | SOUTH, Weight::Light, false), style);
    }
}

/// The whole picture as text: `ansi` with colour escapes, else plain. Each
/// row is trimmed of trailing blanks.
pub fn to_text(buffer: &Buffer, ansi: bool) -> String {
    use ratatui::style::Color;
    let area = buffer.area;
    let mut out = String::new();
    for y in area.y..area.y + area.height {
        let mut line = String::new();
        let mut current: Option<(Color, Color, Modifier)> = None;
        let mut trailing_plain = 0usize;
        for x in area.x..area.x + area.width {
            let cell = &buffer[(x, y)];
            let symbol = cell.symbol();
            if ansi {
                let key = (cell.fg, cell.bg, cell.modifier);
                if current != Some(key) {
                    line.push_str(&sgr(cell.fg, cell.bg, cell.modifier));
                    current = Some(key);
                }
            }
            line.push_str(symbol);
            if symbol == " " && cell.bg == Color::Reset {
                trailing_plain += 1;
            } else if !symbol.is_empty() {
                trailing_plain = 0;
            }
        }
        if !ansi {
            let trimmed = line.trim_end_matches(' ');
            out.push_str(trimmed);
        } else {
            let _ = trailing_plain;
            out.push_str(&line);
            out.push_str("\x1b[0m");
        }
        out.push('\n');
    }
    out
}

/// The SGR escape that sets a cell's colours and modifiers from scratch.
fn sgr(fg: ratatui::style::Color, bg: ratatui::style::Color, modifier: Modifier) -> String {
    use ratatui::style::Color;
    let mut codes: Vec<String> = vec!["0".to_string()];
    if modifier.contains(Modifier::BOLD) {
        codes.push("1".into());
    }
    if modifier.contains(Modifier::DIM) {
        codes.push("2".into());
    }
    if modifier.contains(Modifier::ITALIC) {
        codes.push("3".into());
    }
    if modifier.contains(Modifier::UNDERLINED) {
        codes.push("4".into());
    }
    if modifier.contains(Modifier::REVERSED) {
        codes.push("7".into());
    }
    let color = |c: Color, base: u8| -> Option<String> {
        match c {
            Color::Reset => None,
            Color::Rgb(r, g, b) => Some(format!("{};2;{r};{g};{b}", base + 8)),
            Color::Indexed(i) => Some(format!("{};5;{i}", base + 8)),
            Color::Black => Some(format!("{}", base)),
            Color::Red => Some(format!("{}", base + 1)),
            Color::Green => Some(format!("{}", base + 2)),
            Color::Yellow => Some(format!("{}", base + 3)),
            Color::Blue => Some(format!("{}", base + 4)),
            Color::Magenta => Some(format!("{}", base + 5)),
            Color::Cyan => Some(format!("{}", base + 6)),
            Color::Gray => Some(format!("{}", base + 7)),
            Color::DarkGray => Some(format!("{}", base + 60)),
            Color::LightRed => Some(format!("{}", base + 61)),
            Color::LightGreen => Some(format!("{}", base + 62)),
            Color::LightYellow => Some(format!("{}", base + 63)),
            Color::LightBlue => Some(format!("{}", base + 64)),
            Color::LightMagenta => Some(format!("{}", base + 65)),
            Color::LightCyan => Some(format!("{}", base + 66)),
            Color::White => Some(format!("{}", base + 67)),
        }
    };
    if let Some(c) = color(fg, 30) {
        codes.push(c);
    }
    if let Some(c) = color(bg, 40) {
        codes.push(c);
    }
    format!("\x1b[{}m", codes.join(";"))
}

/// The whole of `layout` drawn into a buffer of its own size.
pub fn render_whole(scene: &Scene, layout: &Layout, theme: &Theme) -> Buffer {
    let area = Area::new(0, 0, layout.width.max(1) as u16, layout.height.max(1) as u16);
    let mut buffer = Buffer::empty(area);
    let mut surface = Surface {
        buffer: &mut buffer,
        area,
        origin: (0, 0),
    };
    draw(&mut surface, scene, layout, theme, &Overlay::default());
    buffer
}

/// The node drawn at screen cell `(x, y)` of a surface showing `layout` at
/// `origin` on `area`.
pub fn node_at(layout: &Layout, area: Area, origin: (i32, i32), x: u16, y: u16) -> Option<&PlacedNode> {
    if x < area.x || y < area.y || x >= area.x + area.width || y >= area.y + area.height {
        return None;
    }
    let sx = (x - area.x) as i32 + origin.0;
    let sy = (y - area.y) as i32 + origin.1;
    layout.nodes.iter().find(|n| n.rect.contains(sx, sy))
}
