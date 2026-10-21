//! The graph view: a scene laid out and drawn into a viewport, with focus,
//! panning, levels of detail, a minimap and off-screen rails.
//!
//! Zoom in a terminal is the level of detail ([`Lod`]): whole cards, small
//! cards, one line per node, one cell per node. Unless one is chosen, the
//! view picks the richest level at which the whole scene fits. The viewport
//! follows the run's subject — what failed, else what runs, else the first
//! node — until it is moved by hand; `c` returns to the subject and `f` to
//! fitting.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect as Area;
use ratatui::style::Modifier;

use crate::tui::canvas::{self, Overlay, Surface};
use crate::tui::layout::{self, Arrangement, Layout, LayoutError, Lod};
use crate::tui::scene::{NodeState, Scene};
use crate::tui::theme::{Theme, Tone};

/// A direction focus or the viewport moves in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Left,
    Right,
    Up,
    Down,
}

/// What a key or a click did to the graph view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphAction {
    /// Nothing the view handles.
    Ignored,
    /// The view changed: redraw.
    Changed,
    /// A node was chosen (Enter, a click): inspect it.
    Inspect(String),
}

/// The graph view's state.
#[derive(Debug, Clone)]
pub struct GraphView {
    /// The chosen level of detail; `None` fits the scene to the viewport.
    pub lod: Option<Lod>,
    /// Bands that fit the width (`true`), one row of columns (`false`), or
    /// whichever shows more (`None`): one row when it fits, else bands.
    pub wrapped: Option<bool>,
    pub focus: Option<String>,
    /// The scene cell at the viewport's top-left corner.
    pub origin: (i32, i32),
    /// Whether the viewport keeps the subject in view.
    pub follow: bool,
    pub minimap: bool,
    layout: Option<Layout>,
    area: Area,
    drag: Option<(u16, u16, (i32, i32))>,
}

impl Default for GraphView {
    fn default() -> Self {
        Self {
            lod: None,
            wrapped: None,
            focus: None,
            origin: (0, 0),
            follow: true,
            minimap: false,
            layout: None,
            area: Area::default(),
            drag: None,
        }
    }
}

/// The node the viewport opens on: the first that failed, else the first
/// running, else the first in reading order (the uikit's `_openOnSubject`).
pub fn subject(scene: &Scene, layout: &Layout) -> Option<String> {
    let ordered = layout::reading_order(layout);
    let state_of = |id: &str| scene.node(id).map(|n| n.state);
    ordered
        .iter()
        .find(|id| state_of(id) == Some(NodeState::Failed))
        .or_else(|| {
            ordered
                .iter()
                .find(|id| matches!(state_of(id), Some(NodeState::Active(_)) | Some(NodeState::Queued)))
        })
        .or_else(|| ordered.first())
        .cloned()
}

impl GraphView {
    /// The layout last drawn.
    pub fn layout(&self) -> Option<&Layout> {
        self.layout.as_ref()
    }

    /// The level of detail last drawn at.
    pub fn drawn_lod(&self) -> Option<Lod> {
        self.layout.as_ref().map(|l| l.lod)
    }

    /// The arrangements to lay out in, preferred first.
    fn arrangements(&self, area: Area) -> Vec<Arrangement> {
        let wrapped = Arrangement::Wrapped {
            width: (area.width as i32 - 2).max(20),
        };
        match self.wrapped {
            Some(true) => vec![wrapped],
            Some(false) => vec![Arrangement::Rows],
            None => vec![Arrangement::Rows, wrapped],
        }
    }

    /// Bands, or one row, whichever is not drawn now.
    pub fn toggle_wrap(&mut self) {
        let drawn_wrapped = self
            .layout
            .as_ref()
            .is_some_and(|l| matches!(l.arrangement, Arrangement::Wrapped { .. }));
        self.wrapped = Some(!drawn_wrapped);
        self.follow = true;
    }

    /// Lay out `scene` for `area` and draw it.
    pub fn draw(
        &mut self,
        buffer: &mut Buffer,
        area: Area,
        scene: &Scene,
        theme: &Theme,
        frame: usize,
        chip_width: i32,
    ) -> Result<(), LayoutError> {
        self.area = area;
        let arrangements = self.arrangements(area);
        let laid = match self.lod {
            None => layout::fit(scene, area.width as i32, area.height as i32, &arrangements, chip_width)?,
            // A chosen level is panned around, so only the width decides
            // between the arrangements.
            Some(lod) => layout::fit_width(
                scene,
                lod,
                area.width as i32,
                &arrangements,
                if matches!(lod, Lod::Full | Lod::Compact) { chip_width } else { 0 },
            )?,
        };
        if self.focus.as_ref().is_some_and(|f| laid.node(f).is_none()) {
            self.focus = None;
        }
        if self.follow {
            let target = self.focus.clone().or_else(|| subject(scene, &laid));
            if let Some(target) = target.and_then(|id| laid.node(&id).cloned()) {
                self.origin = centred_on(&laid, area, target.rect);
            }
        }
        self.origin = clamp_origin(&laid, area, self.origin);
        let overlay = Overlay {
            focused: self.focus.clone(),
            focused_edge: None,
            frame,
        };
        let mut surface = Surface {
            buffer,
            area,
            origin: self.origin,
        };
        canvas::draw(&mut surface, scene, &laid, theme, &overlay);
        draw_rails(buffer, area, &laid, self.origin, theme);
        if self.minimap {
            draw_minimap(buffer, area, &laid, scene, self.origin, theme);
        }
        self.layout = Some(laid);
        Ok(())
    }

    fn focused_rect(&self) -> Option<layout::Rect> {
        let laid = self.layout.as_ref()?;
        laid.node(self.focus.as_ref()?).map(|n| n.rect)
    }

    /// Move the focus to the nearest node in `direction`.
    pub fn move_focus(&mut self, direction: Direction) -> bool {
        let Some(laid) = &self.layout else { return false };
        let from = match self.focused_rect() {
            Some(rect) => rect,
            None => {
                self.focus = layout::reading_order(laid).first().cloned();
                self.follow = true;
                return self.focus.is_some();
            }
        };
        let (cx, cy) = (from.x * 2 + from.w, from.y * 2 + from.h);
        let best = laid
            .nodes
            .iter()
            .filter(|n| Some(&n.id) != self.focus.as_ref())
            .filter_map(|n| {
                let (x, y) = (n.rect.x * 2 + n.rect.w, n.rect.y * 2 + n.rect.h);
                let (along, across) = match direction {
                    Direction::Right => (x - cx, (y - cy).abs()),
                    Direction::Left => (cx - x, (y - cy).abs()),
                    Direction::Down => (y - cy, (x - cx).abs()),
                    Direction::Up => (cy - y, (x - cx).abs()),
                };
                (along > 0).then_some((along + 2 * across, n.id.clone()))
            })
            .min();
        match best {
            Some((_, id)) => {
                self.focus = Some(id);
                self.follow = true;
                true
            }
            None => false,
        }
    }

    /// Move the focus along the reading order.
    pub fn cycle_focus(&mut self, forward: bool) -> bool {
        let Some(laid) = &self.layout else { return false };
        let order = layout::reading_order(laid);
        if order.is_empty() {
            return false;
        }
        let at = self.focus.as_ref().and_then(|f| order.iter().position(|id| id == f));
        let next = match (at, forward) {
            (None, _) => 0,
            (Some(i), true) => (i + 1) % order.len(),
            (Some(i), false) => (i + order.len() - 1) % order.len(),
        };
        self.focus = Some(order[next].clone());
        self.follow = true;
        true
    }

    /// Follow an edge out of (or into) the focused node.
    pub fn follow_edge(&mut self, outgoing: bool) -> bool {
        let Some(laid) = &self.layout else { return false };
        let Some(focus) = &self.focus else { return false };
        let mut neighbours: Vec<(&layout::PlacedNode, &String)> = laid
            .edges
            .iter()
            .filter(|e| if outgoing { &e.from == focus } else { &e.to == focus })
            .filter_map(|e| {
                let other = if outgoing { &e.to } else { &e.from };
                laid.node(other).map(|n| (n, other))
            })
            .collect();
        neighbours.sort_by_key(|(n, _)| (n.rect.y, n.rect.x));
        match neighbours.first() {
            Some((_, id)) => {
                self.focus = Some((*id).clone());
                self.follow = true;
                true
            }
            None => false,
        }
    }

    /// Pan the viewport by a step in `direction`; panning by hand stops
    /// following the subject.
    pub fn pan(&mut self, direction: Direction, step: i32) {
        self.follow = false;
        match direction {
            Direction::Left => self.origin.0 -= step,
            Direction::Right => self.origin.0 += step,
            Direction::Up => self.origin.1 -= step,
            Direction::Down => self.origin.1 += step,
        }
        if let Some(laid) = &self.layout {
            self.origin = clamp_origin(laid, self.area, self.origin);
        }
    }

    /// More detail, if there is more.
    pub fn zoom_in(&mut self) {
        let current = self.lod.or(self.drawn_lod()).unwrap_or(Lod::Full);
        self.lod = Some(current.more().unwrap_or(current));
        self.follow = true;
    }

    /// Less detail, if there is less.
    pub fn zoom_out(&mut self) {
        let current = self.lod.or(self.drawn_lod()).unwrap_or(Lod::Full);
        self.lod = Some(current.less().unwrap_or(current));
        self.follow = true;
    }

    /// Fit the scene again, following the subject.
    pub fn fit(&mut self) {
        self.lod = None;
        self.wrapped = None;
        self.follow = true;
    }

    /// Return to the subject.
    pub fn centre(&mut self) {
        self.focus = None;
        self.follow = true;
    }

    /// Handle a key the view understands.
    pub fn on_key(&mut self, key: KeyEvent) -> GraphAction {
        let shift = key.modifiers.contains(KeyModifiers::SHIFT);
        let page_w = (self.area.width as i32 / 2).max(4);
        let page_h = (self.area.height as i32 / 2).max(2);
        let changed = |done: bool| if done { GraphAction::Changed } else { GraphAction::Ignored };
        match key.code {
            KeyCode::Left | KeyCode::Char('h') if !shift => changed(self.move_focus(Direction::Left)),
            KeyCode::Right | KeyCode::Char('l') if !shift => changed(self.move_focus(Direction::Right)),
            KeyCode::Up | KeyCode::Char('k') if !shift => changed(self.move_focus(Direction::Up)),
            KeyCode::Down | KeyCode::Char('j') if !shift => changed(self.move_focus(Direction::Down)),
            KeyCode::Left | KeyCode::Char('H') => {
                self.pan(Direction::Left, page_w);
                GraphAction::Changed
            }
            KeyCode::Right | KeyCode::Char('L') => {
                self.pan(Direction::Right, page_w);
                GraphAction::Changed
            }
            KeyCode::Up | KeyCode::Char('K') => {
                self.pan(Direction::Up, page_h);
                GraphAction::Changed
            }
            KeyCode::Down | KeyCode::Char('J') => {
                self.pan(Direction::Down, page_h);
                GraphAction::Changed
            }
            KeyCode::Tab => changed(self.cycle_focus(true)),
            KeyCode::BackTab => changed(self.cycle_focus(false)),
            KeyCode::Char(']') => changed(self.follow_edge(true)),
            KeyCode::Char('[') => changed(self.follow_edge(false)),
            KeyCode::Char('+') | KeyCode::Char('=') => {
                self.zoom_in();
                GraphAction::Changed
            }
            KeyCode::Char('-') | KeyCode::Char('_') => {
                self.zoom_out();
                GraphAction::Changed
            }
            KeyCode::Char('f') => {
                self.fit();
                GraphAction::Changed
            }
            KeyCode::Char('c') => {
                self.centre();
                GraphAction::Changed
            }
            KeyCode::Char('w') => {
                self.toggle_wrap();
                GraphAction::Changed
            }
            KeyCode::Char('m') => {
                self.minimap = !self.minimap;
                GraphAction::Changed
            }
            KeyCode::Enter => match &self.focus {
                Some(id) => GraphAction::Inspect(id.clone()),
                None => GraphAction::Ignored,
            },
            _ => GraphAction::Ignored,
        }
    }

    /// Handle the mouse over the view: a click focuses and inspects a node,
    /// a drag pans, the wheel pans (with Ctrl, changes the detail).
    pub fn on_mouse(&mut self, mouse: MouseEvent) -> GraphAction {
        let inside = mouse.column >= self.area.x
            && mouse.row >= self.area.y
            && mouse.column < self.area.x + self.area.width
            && mouse.row < self.area.y + self.area.height;
        match mouse.kind {
            MouseEventKind::Down(MouseButton::Left) if inside => {
                if let Some(rail) = self.rail_at(mouse.column, mouse.row) {
                    self.pan(rail, (self.area.width as i32 / 2).max(4));
                    return GraphAction::Changed;
                }
                let hit = self
                    .layout
                    .as_ref()
                    .and_then(|laid| canvas::node_at(laid, self.area, self.origin, mouse.column, mouse.row))
                    .map(|n| n.id.clone());
                match hit {
                    Some(id) => {
                        self.focus = Some(id.clone());
                        GraphAction::Inspect(id)
                    }
                    None => {
                        self.drag = Some((mouse.column, mouse.row, self.origin));
                        GraphAction::Ignored
                    }
                }
            }
            MouseEventKind::Drag(MouseButton::Left) => match self.drag {
                Some((x0, y0, origin)) => {
                    self.follow = false;
                    self.origin = (
                        origin.0 - (mouse.column as i32 - x0 as i32),
                        origin.1 - (mouse.row as i32 - y0 as i32),
                    );
                    if let Some(laid) = &self.layout {
                        self.origin = clamp_origin(laid, self.area, self.origin);
                    }
                    GraphAction::Changed
                }
                None => GraphAction::Ignored,
            },
            MouseEventKind::Up(MouseButton::Left) => {
                self.drag = None;
                GraphAction::Ignored
            }
            MouseEventKind::ScrollDown if inside => {
                if mouse.modifiers.contains(KeyModifiers::CONTROL) {
                    self.zoom_out();
                } else if mouse.modifiers.contains(KeyModifiers::SHIFT) {
                    self.pan(Direction::Right, 4);
                } else {
                    self.pan(Direction::Down, 2);
                }
                GraphAction::Changed
            }
            MouseEventKind::ScrollUp if inside => {
                if mouse.modifiers.contains(KeyModifiers::CONTROL) {
                    self.zoom_in();
                } else if mouse.modifiers.contains(KeyModifiers::SHIFT) {
                    self.pan(Direction::Left, 4);
                } else {
                    self.pan(Direction::Up, 2);
                }
                GraphAction::Changed
            }
            MouseEventKind::ScrollRight if inside => {
                self.pan(Direction::Right, 4);
                GraphAction::Changed
            }
            MouseEventKind::ScrollLeft if inside => {
                self.pan(Direction::Left, 4);
                GraphAction::Changed
            }
            _ => GraphAction::Ignored,
        }
    }

    /// The off-screen rail at a screen cell, if one is drawn there.
    fn rail_at(&self, x: u16, y: u16) -> Option<Direction> {
        let laid = self.layout.as_ref()?;
        let counts = offscreen(laid, self.area, self.origin);
        let area = self.area;
        let mid_y = area.y + area.height / 2;
        let mid_x = area.x + area.width / 2;
        if counts.0 > 0 && x <= area.x + 3 && y == mid_y {
            return Some(Direction::Left);
        }
        if counts.1 > 0 && x + 4 >= area.x + area.width && y == mid_y {
            return Some(Direction::Right);
        }
        if counts.2 > 0 && y == area.y && x.abs_diff(mid_x) <= 3 {
            return Some(Direction::Up);
        }
        if counts.3 > 0 && y + 1 == area.y + area.height && x.abs_diff(mid_x) <= 3 {
            return Some(Direction::Down);
        }
        None
    }
}

/// The origin that centres `rect` in `area`.
fn centred_on(laid: &Layout, area: Area, rect: layout::Rect) -> (i32, i32) {
    let x = rect.x + rect.w / 2 - area.width as i32 / 2;
    let y = rect.y + rect.h / 2 - area.height as i32 / 2;
    clamp_origin(laid, area, (x, y))
}

/// Keep the viewport on the scene: never scrolled past its ends.
fn clamp_origin(laid: &Layout, area: Area, origin: (i32, i32)) -> (i32, i32) {
    let max_x = (laid.width - area.width as i32).max(0);
    let max_y = (laid.height - area.height as i32).max(0);
    (origin.0.clamp(0, max_x), origin.1.clamp(0, max_y))
}

/// How many nodes lie wholly off each side of the viewport: left, right,
/// above, below.
fn offscreen(laid: &Layout, area: Area, origin: (i32, i32)) -> (usize, usize, usize, usize) {
    let (x0, y0) = origin;
    let (x1, y1) = (x0 + area.width as i32, y0 + area.height as i32);
    let mut counts = (0, 0, 0, 0);
    for node in &laid.nodes {
        let r = node.rect;
        if r.right() <= x0 {
            counts.0 += 1;
        } else if r.x >= x1 {
            counts.1 += 1;
        } else if r.bottom() <= y0 {
            counts.2 += 1;
        } else if r.y >= y1 {
            counts.3 += 1;
        }
    }
    counts
}

/// The rails at the viewport's sides: how many nodes lie beyond each.
fn draw_rails(buffer: &mut Buffer, area: Area, laid: &Layout, origin: (i32, i32), theme: &Theme) {
    let (left, right, up, down) = offscreen(laid, area, origin);
    let style = theme.fg(Tone::Accent).add_modifier(Modifier::BOLD).patch(theme.soft(Tone::Accent));
    let arrows = match theme.glyphs {
        crate::tui::theme::GlyphSet::Unicode => ('◀', '▶', '▲', '▼'),
        crate::tui::theme::GlyphSet::Ascii => ('<', '>', '^', 'v'),
    };
    let mid_y = area.y + area.height / 2;
    let mid_x = area.x + area.width / 2;
    if left > 0 {
        buffer.set_string(area.x, mid_y, format!("{}{left}", arrows.0), style);
    }
    if right > 0 {
        let label = format!("{right}{}", arrows.1);
        buffer.set_string(area.x + area.width - label.chars().count() as u16, mid_y, label, style);
    }
    if up > 0 {
        buffer.set_string(mid_x.saturating_sub(1), area.y, format!("{}{up}", arrows.2), style);
    }
    if down > 0 {
        buffer.set_string(mid_x.saturating_sub(1), area.y + area.height - 1, format!("{}{down}", arrows.3), style);
    }
}

/// The minimap: the whole scene in braille dots (two by four per cell) in
/// the viewport's bottom-right corner, each node in its state's colour, and
/// the viewport outlined.
fn draw_minimap(buffer: &mut Buffer, area: Area, laid: &Layout, scene: &Scene, origin: (i32, i32), theme: &Theme) {
    let w = (area.width / 4).clamp(12, 32);
    let h = (area.height / 3).clamp(4, 10);
    if area.width < w + 4 || area.height < h + 2 {
        return;
    }
    let map = Area::new(area.x + area.width - w - 1, area.y + area.height - h - 1, w, h);
    let dots_w = (w as i32) * 2;
    let dots_h = (h as i32) * 4;
    let scale_x = (laid.width.max(1) as f64) / dots_w as f64;
    let scale_y = (laid.height.max(1) as f64) / dots_h as f64;
    let mut cells: Vec<(u8, Option<Tone>)> = vec![(0, None); (w * h) as usize];
    let set = |dx: i32, dy: i32, tone: Option<Tone>, cells: &mut Vec<(u8, Option<Tone>)>| {
        if dx < 0 || dy < 0 || dx >= dots_w || dy >= dots_h {
            return;
        }
        let (cx, cy) = (dx / 2, dy / 4);
        let bit = match (dx % 2, dy % 4) {
            (0, 0) => 0x01,
            (0, 1) => 0x02,
            (0, 2) => 0x04,
            (1, 0) => 0x08,
            (1, 1) => 0x10,
            (1, 2) => 0x20,
            (0, 3) => 0x40,
            _ => 0x80,
        };
        let cell = &mut cells[(cy * w as i32 + cx) as usize];
        cell.0 |= bit;
        if tone.is_some() {
            cell.1 = tone;
        }
    };
    for node in &laid.nodes {
        let tone = scene
            .node(&node.id)
            .and_then(|n| n.state.tone())
            .or(Some(Tone::Secondary));
        let (x0, x1) = ((node.rect.x as f64 / scale_x) as i32, (node.rect.right() as f64 / scale_x).ceil() as i32);
        let (y0, y1) = ((node.rect.y as f64 / scale_y) as i32, (node.rect.bottom() as f64 / scale_y).ceil() as i32);
        for dy in y0..y1.max(y0 + 1) {
            for dx in x0..x1.max(x0 + 1) {
                set(dx, dy, tone, &mut cells);
            }
        }
    }
    // The viewport's outline.
    let (vx0, vy0) = ((origin.0 as f64 / scale_x) as i32, (origin.1 as f64 / scale_y) as i32);
    let (vx1, vy1) = (
        ((origin.0 + area.width as i32) as f64 / scale_x) as i32,
        ((origin.1 + area.height as i32) as f64 / scale_y) as i32,
    );
    for dx in vx0..=vx1.min(dots_w - 1) {
        set(dx, vy0, None, &mut cells);
        set(dx, vy1.min(dots_h - 1), None, &mut cells);
    }
    for dy in vy0..=vy1.min(dots_h - 1) {
        set(vx0, dy, None, &mut cells);
        set(vx1.min(dots_w - 1), dy, None, &mut cells);
    }
    let ground = theme.chrome();
    for cy in 0..h {
        for cx in 0..w {
            let (bits, tone) = cells[(cy * w + cx) as usize];
            let ch = char::from_u32(0x2800 + bits as u32).expect("braille block");
            let style = ground.patch(theme.fg(tone.unwrap_or(Tone::Muted)));
            buffer[(map.x + cx, map.y + cy)].set_char(ch).set_style(style);
        }
    }
}
