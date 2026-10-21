//! How the terminal interface looks: the uikit's palette, the glyphs lines
//! and marks are drawn with, and what kind of thing a medium is.
//!
//! The palette is FabricLoomUIKit's own (`styles/foundations.scss`), both
//! appearances, so a run reads the same on the desktop and in a terminal:
//! blue for what comes in, green for what goes out, amber for a loop, red for
//! a failure. A terminal that does not draw 24-bit colour is given the nearest
//! of its 256; one told `NO_COLOR` is given none, and everything a colour says
//! is also said by a glyph or a word.

use ratatui::style::{Color, Modifier, Style};

use crate::MediaUrn;

/// Which of the uikit's two appearances.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Appearance {
    Light,
    Dark,
}

/// How many colours the terminal draws.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColorDepth {
    /// 24-bit colour.
    TrueColor,
    /// The xterm 256-colour palette.
    Indexed,
    /// None: `NO_COLOR`, or output that is not a terminal.
    None,
}

impl ColorDepth {
    /// What the environment says the terminal draws. `NO_COLOR` (any value,
    /// per no-color.org) wins; `COLORTERM=truecolor|24bit` is how terminals
    /// announce 24-bit colour.
    pub fn from_environment() -> Self {
        if std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty()) {
            return ColorDepth::None;
        }
        match std::env::var("COLORTERM").as_deref() {
            Ok("truecolor") | Ok("24bit") => ColorDepth::TrueColor,
            _ => ColorDepth::Indexed,
        }
    }
}

/// Which glyphs lines and marks are drawn with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GlyphSet {
    /// Box drawing and geometric shapes.
    Unicode,
    /// Seven-bit ASCII only, for a terminal or font without box drawing.
    Ascii,
}

impl GlyphSet {
    /// What the locale says the terminal can show: UTF-8 unless the locale
    /// names another encoding. An explicit choice (`--glyphs`) overrides it.
    pub fn from_environment() -> Self {
        let locale = ["LC_ALL", "LC_CTYPE", "LANG"]
            .iter()
            .find_map(|name| std::env::var(name).ok().filter(|v| !v.is_empty()));
        match locale {
            Some(value) => {
                let upper = value.to_ascii_uppercase();
                if upper.contains("UTF-8") || upper.contains("UTF8") {
                    GlyphSet::Unicode
                } else {
                    // `C`, `POSIX`, or a locale naming another encoding.
                    GlyphSet::Ascii
                }
            }
            // No locale at all: Windows terminals and most macOS ones draw
            // UTF-8 without saying so.
            None => GlyphSet::Unicode,
        }
    }
}

/// A colour of the palette, before the terminal's depth is applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rgb(pub u8, pub u8, pub u8);

/// The uikit's tokens in one appearance.
#[derive(Debug, Clone, Copy)]
struct Palette {
    canvas: Rgb,
    surface: Rgb,
    surface_raised: Rgb,
    surface_muted: Rgb,
    ink: Rgb,
    ink_2: Rgb,
    ink_3: Rgb,
    line: Rgb,
    line_strong: Rgb,
    accent: Rgb,
    accent_soft: Rgb,
    blue: Rgb,
    blue_soft: Rgb,
    green: Rgb,
    green_soft: Rgb,
    amber: Rgb,
    amber_soft: Rgb,
    red: Rgb,
    red_soft: Rgb,
}

const DARK: Palette = Palette {
    canvas: Rgb(0x14, 0x15, 0x13),
    surface: Rgb(0x1b, 0x1c, 0x1a),
    surface_raised: Rgb(0x20, 0x21, 0x1f),
    surface_muted: Rgb(0x25, 0x26, 0x23),
    ink: Rgb(0xf1, 0xf1, 0xec),
    ink_2: Rgb(0xb8, 0xbb, 0xb2),
    ink_3: Rgb(0x89, 0x8d, 0x83),
    line: Rgb(0x31, 0x33, 0x2f),
    line_strong: Rgb(0x41, 0x44, 0x3e),
    accent: Rgb(0x9f, 0xbe, 0x88),
    accent_soft: Rgb(0x27, 0x32, 0x22),
    blue: Rgb(0x8e, 0xb4, 0xd8),
    blue_soft: Rgb(0x20, 0x2e, 0x3a),
    green: Rgb(0x8a, 0xc2, 0x9a),
    green_soft: Rgb(0x20, 0x31, 0x27),
    amber: Rgb(0xd5, 0xb5, 0x68),
    amber_soft: Rgb(0x34, 0x2d, 0x1f),
    red: Rgb(0xdf, 0x8f, 0x8c),
    red_soft: Rgb(0x38, 0x25, 0x23),
};

const LIGHT: Palette = Palette {
    canvas: Rgb(0xf6, 0xf6, 0xf3),
    surface: Rgb(0xff, 0xff, 0xff),
    surface_raised: Rgb(0xff, 0xff, 0xff),
    surface_muted: Rgb(0xf1, 0xf1, 0xed),
    ink: Rgb(0x17, 0x18, 0x16),
    ink_2: Rgb(0x4f, 0x51, 0x4c),
    ink_3: Rgb(0x7d, 0x80, 0x78),
    line: Rgb(0xde, 0xdf, 0xd9),
    line_strong: Rgb(0xc9, 0xcb, 0xc3),
    accent: Rgb(0x44, 0x5d, 0x36),
    accent_soft: Rgb(0xe8, 0xef, 0xe3),
    blue: Rgb(0x41, 0x68, 0x8f),
    blue_soft: Rgb(0xe8, 0xf0, 0xf7),
    green: Rgb(0x38, 0x73, 0x4b),
    green_soft: Rgb(0xe4, 0xf2, 0xe8),
    amber: Rgb(0x83, 0x62, 0x23),
    amber_soft: Rgb(0xf5, 0xed, 0xda),
    red: Rgb(0x9a, 0x3f, 0x3f),
    red_soft: Rgb(0xf8, 0xe6, 0xe4),
};

/// What a mark or a line means, which decides its colour.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tone {
    /// Text that matters most: titles.
    Ink,
    /// Secondary text: subtitles, values.
    Secondary,
    /// Tertiary text and idle lines.
    Muted,
    /// Borders.
    Border,
    /// Strong borders: a step card.
    BorderStrong,
    /// What comes in.
    Source,
    /// What goes out.
    Target,
    /// The system's accent: focus, selection, what is active.
    Accent,
    /// A loop, a wait, a warning.
    Warning,
    Success,
    Danger,
}

/// The look of everything drawn: palette, depth, glyphs, motion.
#[derive(Debug, Clone, Copy)]
pub struct Theme {
    pub appearance: Appearance,
    pub depth: ColorDepth,
    pub glyphs: GlyphSet,
    /// Whether anything moves: spinners, marching edges, pulses.
    pub motion: bool,
    palette: Palette,
}

impl Theme {
    pub fn new(appearance: Appearance, depth: ColorDepth, glyphs: GlyphSet, motion: bool) -> Self {
        Self {
            appearance,
            depth,
            glyphs,
            motion,
            palette: match appearance {
                Appearance::Dark => DARK,
                Appearance::Light => LIGHT,
            },
        }
    }

    fn color(&self, rgb: Rgb) -> Option<Color> {
        match self.depth {
            ColorDepth::TrueColor => Some(Color::Rgb(rgb.0, rgb.1, rgb.2)),
            ColorDepth::Indexed => Some(Color::Indexed(nearest_xterm_256(rgb))),
            ColorDepth::None => None,
        }
    }

    fn tone_rgb(&self, tone: Tone) -> Rgb {
        let p = &self.palette;
        match tone {
            Tone::Ink => p.ink,
            Tone::Secondary => p.ink_2,
            Tone::Muted => p.ink_3,
            Tone::Border => p.line_strong,
            Tone::BorderStrong => p.ink_3,
            Tone::Source => p.blue,
            Tone::Target => p.green,
            Tone::Accent => p.accent,
            Tone::Warning => p.amber,
            Tone::Success => p.green,
            Tone::Danger => p.red,
        }
    }

    /// The foreground of a tone. Without colour, the tones that carry a
    /// state keep a modifier, so a failure is still told from a success.
    pub fn fg(&self, tone: Tone) -> Style {
        match self.color(self.tone_rgb(tone)) {
            Some(color) => Style::default().fg(color),
            None => match tone {
                Tone::Ink | Tone::Danger => Style::default().add_modifier(Modifier::BOLD),
                Tone::Muted | Tone::Border => Style::default().add_modifier(Modifier::DIM),
                Tone::Accent => Style::default().add_modifier(Modifier::REVERSED),
                _ => Style::default(),
            },
        }
    }

    /// The ground the graph is drawn on.
    pub fn canvas(&self) -> Style {
        match self.color(self.palette.canvas) {
            Some(color) => Style::default().bg(color),
            None => Style::default(),
        }
    }

    /// A card's ground.
    pub fn surface(&self, raised: bool) -> Style {
        let rgb = if raised {
            self.palette.surface_raised
        } else {
            self.palette.surface
        };
        match self.color(rgb) {
            Some(color) => Style::default().bg(color),
            None => Style::default(),
        }
    }

    /// The soft ground of a tone: a selected row, a failure badge.
    pub fn soft(&self, tone: Tone) -> Style {
        let p = &self.palette;
        let rgb = match tone {
            Tone::Source => p.blue_soft,
            Tone::Target | Tone::Success => p.green_soft,
            Tone::Warning => p.amber_soft,
            Tone::Danger => p.red_soft,
            Tone::Accent => p.accent_soft,
            _ => p.surface_muted,
        };
        match self.color(rgb) {
            Some(color) => Style::default().bg(color),
            None => Style::default().add_modifier(Modifier::REVERSED),
        }
    }

    /// The ground of chrome: bars, the dock.
    pub fn chrome(&self) -> Style {
        match self.color(self.palette.surface) {
            Some(color) => Style::default().bg(color).fg(self.color(self.palette.ink).expect("same depth")),
            None => Style::default(),
        }
    }

    /// A thin separator line.
    pub fn rule(&self) -> Style {
        match self.color(self.palette.line) {
            Some(color) => Style::default().fg(color),
            None => Style::default().add_modifier(Modifier::DIM),
        }
    }
}

/// The xterm 256-colour index closest to `rgb`: the 6×6×6 cube or the
/// 24-step grey ramp, whichever is nearer.
pub fn nearest_xterm_256(rgb: Rgb) -> u8 {
    const LEVELS: [u8; 6] = [0, 95, 135, 175, 215, 255];
    let nearest_level = |v: u8| -> usize {
        LEVELS
            .iter()
            .enumerate()
            .min_by_key(|(_, level)| (**level as i32 - v as i32).abs())
            .map(|(index, _)| index)
            .expect("six levels")
    };
    let (r, g, b) = (nearest_level(rgb.0), nearest_level(rgb.1), nearest_level(rgb.2));
    let cube = (LEVELS[r], LEVELS[g], LEVELS[b]);
    let cube_index = 16 + 36 * r + 6 * g + b;
    let grey_avg = (rgb.0 as u32 + rgb.1 as u32 + rgb.2 as u32) / 3;
    let grey_step = ((grey_avg as i32 - 8).max(0) / 10).min(23) as u8;
    let grey = 8 + 10 * grey_step;
    let distance = |c: (u8, u8, u8)| -> i32 {
        let dr = c.0 as i32 - rgb.0 as i32;
        let dg = c.1 as i32 - rgb.1 as i32;
        let db = c.2 as i32 - rgb.2 as i32;
        dr * dr + dg * dg + db * db
    };
    if distance((grey, grey, grey)) < distance(cube) {
        232 + grey_step
    } else {
        cube_index as u8
    }
}

/// What kind of thing a medium is — the uikit's classifier
/// (`src/media-kind.js`), read from the parsed URN's tags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MediaKind {
    Image,
    Audio,
    Video,
    Document,
    Text,
    Table,
    Record,
    Numeric,
    Boolean,
    Model,
    Live,
    Urn,
    None,
    Binary,
}

const TEXT_EXTENSIONS: [&str; 8] = ["txt", "md", "rst", "log", "csv", "json", "yaml", "ndjson"];

impl MediaKind {
    /// The kind of `urn`, in the uikit's order of precedence.
    pub fn of(urn: &MediaUrn) -> Self {
        let marker = |tag: &str| urn.has_marker_tag(tag);
        if marker("void") {
            return MediaKind::None;
        }
        if marker("live") {
            return MediaKind::Live;
        }
        if ["cap-urn", "media-urn", "cap-definition", "media-definition"]
            .iter()
            .any(|tag| marker(tag))
        {
            return MediaKind::Urn;
        }
        if ["model-spec", "embeddings", "embedding-vector"].iter().any(|tag| marker(tag)) {
            return MediaKind::Model;
        }
        if marker("audio") {
            return MediaKind::Audio;
        }
        if marker("video") {
            return MediaKind::Video;
        }
        if marker("image") {
            return MediaKind::Image;
        }
        if marker("document") {
            return MediaKind::Document;
        }
        if marker("bool") {
            return MediaKind::Boolean;
        }
        if marker("numeric") || marker("integer") {
            return MediaKind::Numeric;
        }
        let fmt = urn.get_tag("fmt");
        if fmt == Some("csv") || marker("list") {
            return MediaKind::Table;
        }
        if fmt.is_some() || marker("record") {
            return MediaKind::Record;
        }
        if urn
            .get_tag("ext")
            .is_some_and(|ext| TEXT_EXTENSIONS.contains(&ext))
        {
            return MediaKind::Text;
        }
        if urn.get_tag("enc").is_some() {
            return MediaKind::Text;
        }
        MediaKind::Binary
    }

    /// The word for the kind, as the uikit labels it.
    pub fn label(self) -> &'static str {
        match self {
            MediaKind::Image => "Image",
            MediaKind::Audio => "Audio",
            MediaKind::Video => "Video",
            MediaKind::Document => "Document",
            MediaKind::Text => "Text",
            MediaKind::Table => "Table",
            MediaKind::Record => "Record",
            MediaKind::Numeric => "Number",
            MediaKind::Boolean => "Boolean",
            MediaKind::Model => "Model",
            MediaKind::Live => "Live feed",
            MediaKind::Urn => "Identifier",
            MediaKind::None => "Nothing",
            MediaKind::Binary => "Binary",
        }
    }
}

/// A mark a node is drawn with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Icon {
    Media(MediaKind),
    /// A step: a cap.
    Operation,
    /// A map's stack.
    Map,
    /// A gather.
    Gather,
}

/// Where a step stands, as its card's corner says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusMark {
    /// Running: one frame of the spinner.
    Spinner(usize),
    Done,
    Failed,
    Queued,
    Skipped,
    Retried,
    Tolerated,
    /// Stopped by a cancel, never painted as a failure.
    Cancelled,
}

/// One cell's line directions: north, east, south, west.
pub const NORTH: u8 = 0b0001;
pub const EAST: u8 = 0b0010;
pub const SOUTH: u8 = 0b0100;
pub const WEST: u8 = 0b1000;

/// How heavy a line is drawn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Weight {
    /// A dashed light line: a loop, a wait.
    Dashed,
    Light,
    /// What is flowing now.
    Heavy,
}

const SPINNER_UNICODE: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
const SPINNER_ASCII: [char; 4] = ['|', '/', '-', '\\'];

impl GlyphSet {
    /// The glyph for a cell whose line runs in `directions`, at `weight`.
    /// `rounded` turns a light corner into an arc.
    pub fn line(self, directions: u8, weight: Weight, rounded: bool) -> char {
        if self == GlyphSet::Ascii {
            return match directions {
                d if d == NORTH | SOUTH || d == NORTH || d == SOUTH => {
                    if weight == Weight::Dashed { ':' } else { '|' }
                }
                d if d == EAST | WEST || d == EAST || d == WEST => match weight {
                    Weight::Dashed => '.',
                    Weight::Light => '-',
                    Weight::Heavy => '=',
                },
                0 => ' ',
                _ => '+',
            };
        }
        let heavy = weight == Weight::Heavy;
        let dashed = weight == Weight::Dashed;
        match directions {
            0 => ' ',
            d if d == NORTH | SOUTH => {
                if heavy {
                    '┃'
                } else if dashed {
                    '┆'
                } else {
                    '│'
                }
            }
            d if d == EAST | WEST => {
                if heavy {
                    '━'
                } else if dashed {
                    '┄'
                } else {
                    '─'
                }
            }
            d if d == NORTH => {
                if heavy { '╹' } else { '╵' }
            }
            d if d == SOUTH => {
                if heavy { '╻' } else { '╷' }
            }
            d if d == EAST => {
                if heavy { '╺' } else { '╶' }
            }
            d if d == WEST => {
                if heavy { '╸' } else { '╴' }
            }
            d if d == NORTH | EAST => {
                if heavy {
                    '┗'
                } else if rounded {
                    '╰'
                } else {
                    '└'
                }
            }
            d if d == EAST | SOUTH => {
                if heavy {
                    '┏'
                } else if rounded {
                    '╭'
                } else {
                    '┌'
                }
            }
            d if d == SOUTH | WEST => {
                if heavy {
                    '┓'
                } else if rounded {
                    '╮'
                } else {
                    '┐'
                }
            }
            d if d == WEST | NORTH => {
                if heavy {
                    '┛'
                } else if rounded {
                    '╯'
                } else {
                    '┘'
                }
            }
            d if d == NORTH | EAST | SOUTH => {
                if heavy { '┣' } else { '├' }
            }
            d if d == EAST | SOUTH | WEST => {
                if heavy { '┳' } else { '┬' }
            }
            d if d == SOUTH | WEST | NORTH => {
                if heavy { '┫' } else { '┤' }
            }
            d if d == WEST | NORTH | EAST => {
                if heavy { '┻' } else { '┴' }
            }
            _ => {
                if heavy { '╋' } else { '┼' }
            }
        }
    }

    /// The head an edge ends in, entering a node from its left.
    pub fn arrow_right(self) -> char {
        match self {
            GlyphSet::Unicode => '▸',
            GlyphSet::Ascii => '>',
        }
    }

    pub fn icon(self, icon: Icon) -> char {
        if self == GlyphSet::Ascii {
            return match icon {
                Icon::Operation => '*',
                Icon::Map => '%',
                Icon::Gather => '&',
                Icon::Media(kind) => match kind {
                    MediaKind::Image => 'i',
                    MediaKind::Audio => 'a',
                    MediaKind::Video => 'v',
                    MediaKind::Document => 'd',
                    MediaKind::Text => 't',
                    MediaKind::Table => '#',
                    MediaKind::Record => 'r',
                    MediaKind::Numeric => 'n',
                    MediaKind::Boolean => 'b',
                    MediaKind::Model => 'm',
                    MediaKind::Live => 'o',
                    MediaKind::Urn => '@',
                    MediaKind::None => '0',
                    MediaKind::Binary => 'x',
                },
            };
        }
        match icon {
            Icon::Operation => '◇',
            Icon::Map => '∀',
            Icon::Gather => '∑',
            Icon::Media(kind) => match kind {
                MediaKind::Image => '◩',
                MediaKind::Audio => '♫',
                MediaKind::Video => '▣',
                MediaKind::Document => '▤',
                MediaKind::Text => '≡',
                MediaKind::Table => '▦',
                MediaKind::Record => '⁘',
                MediaKind::Numeric => '#',
                MediaKind::Boolean => '◑',
                MediaKind::Model => '◆',
                MediaKind::Live => '◉',
                MediaKind::Urn => '@',
                MediaKind::None => '∅',
                MediaKind::Binary => '▪',
            },
        }
    }

    pub fn status(self, mark: StatusMark) -> char {
        match (self, mark) {
            (GlyphSet::Unicode, StatusMark::Spinner(frame)) => {
                SPINNER_UNICODE[frame % SPINNER_UNICODE.len()]
            }
            (GlyphSet::Ascii, StatusMark::Spinner(frame)) => SPINNER_ASCII[frame % SPINNER_ASCII.len()],
            (GlyphSet::Unicode, StatusMark::Done) => '✓',
            (GlyphSet::Unicode, StatusMark::Failed) => '✗',
            (GlyphSet::Unicode, StatusMark::Queued) => '⋯',
            (GlyphSet::Unicode, StatusMark::Skipped) => '↷',
            (GlyphSet::Unicode, StatusMark::Retried) => '↻',
            (GlyphSet::Unicode, StatusMark::Tolerated) => '△',
            (GlyphSet::Unicode, StatusMark::Cancelled) => '◼',
            (GlyphSet::Ascii, StatusMark::Done) => 'v',
            (GlyphSet::Ascii, StatusMark::Failed) => 'X',
            (GlyphSet::Ascii, StatusMark::Queued) => 'q',
            (GlyphSet::Ascii, StatusMark::Skipped) => 's',
            (GlyphSet::Ascii, StatusMark::Retried) => 'r',
            (GlyphSet::Ascii, StatusMark::Tolerated) => '!',
            (GlyphSet::Ascii, StatusMark::Cancelled) => '#',
        }
    }

    /// Eighth-blocks for a progress bar: index 0 is empty, 8 full.
    pub fn bar_eighths(self) -> [char; 9] {
        match self {
            GlyphSet::Unicode => [' ', '▏', '▎', '▍', '▌', '▋', '▊', '▉', '█'],
            GlyphSet::Ascii => [' ', ' ', ' ', ' ', '=', '=', '=', '=', '='],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST13020: a medium's kind follows the uikit's precedence, read from
    // the parsed URN — an audio file with an extension is audio, a CSV is a
    // table though it is text, JSON is a record, a utf-8 text is text, and
    // bytes with no telling tag are binary.
    #[test]
    fn test13020_media_kind_follows_the_uikit_precedence() {
        let kind = |urn: &str| MediaKind::of(&MediaUrn::from_string(urn).expect(urn));
        assert_eq!(kind("media:audio;ext=wav"), MediaKind::Audio);
        assert_eq!(kind("media:document;ext=pdf"), MediaKind::Document);
        assert_eq!(kind("media:enc=utf-8;ext=csv;fmt=csv"), MediaKind::Table);
        assert_eq!(kind("media:fmt=json;record"), MediaKind::Record);
        assert_eq!(kind("media:enc=utf-8;ext=txt"), MediaKind::Text);
        assert_eq!(kind("media:enc=utf-8"), MediaKind::Text);
        assert_eq!(kind("media:audio;live;microphone"), MediaKind::Live);
        assert_eq!(kind("media:void"), MediaKind::None);
        assert_eq!(kind("media:ext=bin"), MediaKind::Binary);
    }

    // TEST13021: every combination of line directions has a glyph that
    // connects exactly those sides — a junction is never drawn where lines
    // only cross, and a corner is never drawn as a straight line.
    #[test]
    fn test13021_line_glyphs_connect_exactly_their_directions() {
        // The sides each box-drawing glyph connects, read off the glyph.
        let sides = |c: char| -> u8 {
            match c {
                '│' | '┃' | '┆' => NORTH | SOUTH,
                '─' | '━' | '┄' => EAST | WEST,
                '╵' | '╹' => NORTH,
                '╷' | '╻' => SOUTH,
                '╶' | '╺' => EAST,
                '╴' | '╸' => WEST,
                '└' | '╰' | '┗' => NORTH | EAST,
                '┌' | '╭' | '┏' => EAST | SOUTH,
                '┐' | '╮' | '┓' => SOUTH | WEST,
                '┘' | '╯' | '┛' => WEST | NORTH,
                '├' | '┣' => NORTH | EAST | SOUTH,
                '┬' | '┳' => EAST | SOUTH | WEST,
                '┤' | '┫' => SOUTH | WEST | NORTH,
                '┴' | '┻' => WEST | NORTH | EAST,
                '┼' | '╋' => NORTH | EAST | SOUTH | WEST,
                other => panic!("unexpected glyph {other:?}"),
            }
        };
        for directions in 1u8..16 {
            for weight in [Weight::Light, Weight::Heavy] {
                for rounded in [false, true] {
                    let glyph = GlyphSet::Unicode.line(directions, weight, rounded);
                    assert_eq!(
                        sides(glyph),
                        directions,
                        "directions {directions:04b} weight {weight:?} drew {glyph:?}"
                    );
                }
            }
        }
    }

    // TEST13022: the 256-colour fallback finds the palette's greys on the
    // grey ramp and its hues in the cube, never black for a light colour.
    #[test]
    fn test13022_indexed_colour_is_the_nearest_xterm_entry() {
        // The dark canvas is a near-black grey: the grey ramp, dark end.
        let canvas = nearest_xterm_256(Rgb(0x14, 0x15, 0x13));
        assert!((232..=235).contains(&canvas), "canvas -> {canvas}");
        // Pure primaries land on the cube's corners.
        assert_eq!(nearest_xterm_256(Rgb(255, 0, 0)), 196);
        assert_eq!(nearest_xterm_256(Rgb(0, 0, 255)), 21);
        // The dark palette's blue is a light blue, nowhere near black.
        let blue = nearest_xterm_256(Rgb(0x8e, 0xb4, 0xd8));
        assert!(blue >= 16 && blue < 232, "a hue stays in the cube: {blue}");
        assert_ne!(blue, 16);
    }
}
