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
//! the same code draws the interactive view and the whole picture
//! `capdag graph` prints.

use std::collections::HashMap;

use ratatui::buffer::Buffer;
use ratatui::layout::Rect as Area;
use ratatui::style::{Modifier, Style};

use crate::tui::layout::{Layout, Lod, PlacedNode, Rect};
use crate::tui::scene::{NodeRole, Scene, SceneEdge, SceneNode};
use crate::tui::text;
use crate::tui::theme::{GlyphSet, Theme, Tone, EAST, NORTH, SOUTH, WEST};

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
}

/// One net's line through a cell: the sides it leaves the cell by.
#[derive(Debug, Clone, Copy)]
struct Mark {
    net: usize,
    bits: u8,
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
    let line_style = canvas.patch(theme.fg(Tone::Muted));

    // ── lines ──
    let mut marks: HashMap<(i32, i32), Vec<Mark>> = HashMap::new();
    for routed in &layout.edges {
        if !edge_by_id.contains_key(routed.id.as_str()) {
            continue;
        }
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
        for (cell, bits, net) in segment_cells {
            let list = marks.entry(cell).or_default();
            match list.iter_mut().find(|m| m.net == net) {
                Some(mark) => mark.bits |= bits,
                None => list.push(Mark { net, bits }),
            }
        }
    }
    for ((x, y), list) in &marks {
        let bits = if list.len() == 1 {
            list[0].bits
        } else {
            // Nets that meet only cross. The horizontal one passes over, so
            // the vertical one reads as going under it.
            let horizontal = list
                .iter()
                .rev()
                .find(|m| m.bits & (EAST | WEST) != 0 && m.bits & (NORTH | SOUTH) == 0);
            match horizontal {
                Some(m) => m.bits,
                // Two corners in one cell: the layout gives every net its
                // own track, so only a shared start or end row can do it.
                None => list.last().expect("non-empty").bits,
            }
        };
        surface.put(*x, *y, glyphs.line(bits, true), line_style);
    }
    // Arrowheads and labels.
    for routed in &layout.edges {
        let Some(edge) = edge_by_id.get(routed.id.as_str()) else {
            continue;
        };
        if let Some(&(x, y)) = routed.points.last() {
            if layout.nodes.iter().any(|n| n.id == routed.to && n.rect.x == x + 1) {
                surface.put(x, y, glyphs.arrow_right(), line_style);
            }
        }
        if let Some((x, y, room)) = routed.label_at {
            let label = text::truncate(&edge.label, (room - 2).max(0) as usize);
            if !label.is_empty() {
                surface.text(x, y, &format!(" {label} "), room, canvas.patch(theme.fg(Tone::Secondary)));
            }
        }
    }

    // ── nodes ──
    for placed in &layout.nodes {
        let Some(node) = scene.node(&placed.id) else {
            continue;
        };
        let focused = overlay.focused.as_deref() == Some(node.id.as_str());
        match layout.lod {
            Lod::Full | Lod::Compact => draw_card(surface, node, placed, layout.lod, theme, focused),
            Lod::Glyph => draw_line_node(surface, node, placed, theme, focused),
            Lod::Dot => draw_dot(surface, node, placed, theme, focused),
        }
        // A node with edges out shows where they leave.
        if layout.edges.iter().any(|e| e.from == node.id) && matches!(layout.lod, Lod::Full | Lod::Compact) {
            let x = placed.rect.right() - 1;
            surface.put(x, placed.out_row, glyphs.line(NORTH | SOUTH | EAST, false), canvas.patch(border_style(node, theme, focused)));
        }
    }
}

fn border_style(node: &SceneNode, theme: &Theme, focused: bool) -> Style {
    if focused {
        return theme.fg(Tone::Accent).add_modifier(Modifier::BOLD);
    }
    match node.state.tone() {
        Some(tone) => theme.fg(tone),
        None => match node.role {
            NodeRole::Step => theme.fg(Tone::BorderStrong),
            NodeRole::Input => theme.fg(Tone::Source),
            NodeRole::Output => theme.fg(Tone::Target),
            _ => theme.fg(Tone::Border),
        },
    }
}

fn draw_box(surface: &mut Surface<'_>, rect: Rect, glyphs: GlyphSet, style: Style) {
    let (x0, y0, x1, y1) = (rect.x, rect.y, rect.right() - 1, rect.bottom() - 1);
    surface.put(x0, y0, glyphs.line(EAST | SOUTH, true), style);
    surface.put(x1, y0, glyphs.line(SOUTH | WEST, true), style);
    surface.put(x0, y1, glyphs.line(NORTH | EAST, true), style);
    surface.put(x1, y1, glyphs.line(NORTH | WEST, true), style);
    for x in x0 + 1..x1 {
        surface.put(x, y0, glyphs.line(EAST | WEST, false), style);
        surface.put(x, y1, glyphs.line(EAST | WEST, false), style);
    }
    for y in y0 + 1..y1 {
        surface.put(x0, y, glyphs.line(NORTH | SOUTH, false), style);
        surface.put(x1, y, glyphs.line(NORTH | SOUTH, false), style);
    }
}

fn draw_card(surface: &mut Surface<'_>, node: &SceneNode, placed: &PlacedNode, lod: Lod, theme: &Theme, focused: bool) {
    let glyphs = theme.glyphs;
    let card = placed.rect;
    let border = border_style(node, theme, focused);
    let ground = theme.surface(node.role == NodeRole::Step);
    surface.fill(card, ground);
    draw_box(surface, card, glyphs, border.patch(ground));

    let inner_x = card.x + 2;
    let inner_w = card.w - 4;
    // Title row: icon, title, mark.
    let title_style = theme.fg(Tone::Ink).add_modifier(Modifier::BOLD).patch(ground);
    let icon_tone = match node.tone {
        Tone::Source | Tone::Target => node.tone,
        _ => Tone::Secondary,
    };
    surface.put(inner_x, card.y + 1, glyphs.icon(node.icon), theme.fg(icon_tone).patch(ground));
    let mark = node.state.mark(glyphs);
    let title_room = inner_w - 2 - if mark.is_some() { 2 } else { 0 };
    surface.text(inner_x + 2, card.y + 1, &node.title, title_room, title_style);
    if let Some(mark) = mark {
        let tone = node.state.tone().unwrap_or(Tone::Muted);
        surface.put(card.right() - 3, card.y + 1, mark, theme.fg(tone).add_modifier(Modifier::BOLD).patch(ground));
    }
    if lod == Lod::Full {
        surface.text(inner_x, card.y + 2, &node.subtitle, inner_w, theme.fg(Tone::Secondary).patch(ground));
        if let Some(meta) = &node.meta {
            let tone = node.state.tone().unwrap_or(Tone::Muted);
            surface.text(inner_x, card.y + 3, meta, inner_w, theme.fg(tone).patch(ground));
        }
    }
}

fn draw_line_node(surface: &mut Surface<'_>, node: &SceneNode, placed: &PlacedNode, theme: &Theme, focused: bool) {
    let glyphs = theme.glyphs;
    let rect = placed.rect;
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
        surface.put(rect.x, y, glyphs.line(NORTH | SOUTH, false), border.patch(ground));
        surface.put(rect.right() - 1, y, glyphs.line(NORTH | SOUTH, false), border.patch(ground));
    }
    surface.put(rect.x + 1, rect.y, glyphs.icon(node.icon), theme.fg(node.tone).patch(ground));
    let mark = node.state.mark(glyphs);
    let room = rect.w - 4 - if mark.is_some() { 2 } else { 0 };
    surface.text(rect.x + 3, rect.y, &node.title, room, theme.fg(Tone::Ink).patch(ground));
    if let Some(mark) = mark {
        surface.put(rect.right() - 2, rect.y, mark, theme.fg(node.state.tone().unwrap_or(Tone::Muted)).patch(ground));
    }
}

fn draw_dot(surface: &mut Surface<'_>, node: &SceneNode, placed: &PlacedNode, theme: &Theme, focused: bool) {
    let glyphs = theme.glyphs;
    let rect = placed.rect;
    let tone = node.state.tone().unwrap_or(node.tone);
    let style = if focused {
        theme.fg(Tone::Accent).add_modifier(Modifier::REVERSED)
    } else {
        theme.fg(tone)
    };
    let mark = node.state.mark(glyphs).unwrap_or_else(|| glyphs.icon(node.icon));
    surface.put(rect.x, rect.y, '(', style);
    surface.put(rect.x + 1, rect.y, mark, style.add_modifier(Modifier::BOLD));
    surface.put(rect.x + 2, rect.y, ')', style);
    for y in rect.y + 1..rect.bottom() {
        surface.put(rect.x, y, glyphs.line(NORTH | SOUTH, false), style);
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
