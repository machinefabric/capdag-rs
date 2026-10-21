//! Previews of what a run produced, captured or was given: text as text, an
//! image in half blocks, anything else as bytes.
//!
//! An image is drawn with the upper half block (`▀`): its foreground is one
//! pixel, its background the pixel below, so each cell shows two pixels —
//! the technique every terminal with colour draws, with no graphics
//! protocol to negotiate. A terminal told to draw no colour is told what the
//! image is instead.

use std::path::Path;

use ratatui::buffer::Buffer;
use ratatui::layout::Rect as Area;
use ratatui::style::{Color, Style};

use crate::tui::text;
use crate::tui::theme::{nearest_xterm_256, ColorDepth, MediaKind, Rgb, Theme, Tone};

/// How much of a file a preview reads.
pub const PREVIEW_BYTES: u64 = 256 * 1024;

/// A preview, ready to draw.
#[derive(Debug, Clone, PartialEq)]
pub enum Preview {
    /// Lines of text; `cut` when the file holds more than was read.
    Text { lines: Vec<String>, cut: bool, size: u64 },
    /// Pixel pairs per cell, top and bottom, row by row.
    Image {
        cells: Vec<Vec<(Rgb, Rgb)>>,
        width: u32,
        height: u32,
        size: u64,
    },
    /// The first bytes, as a hex dump.
    Bytes { lines: Vec<String>, size: u64 },
    /// It could not be read; why.
    Unreadable(String),
}

/// A preview of the file at `path`, of media kind `kind`, to fit `cells`
/// columns by `rows` rows.
pub fn of_file(path: &Path, kind: MediaKind, cells: u16, rows: u16) -> Preview {
    let size = match std::fs::metadata(path) {
        Ok(meta) => meta.len(),
        Err(e) => return Preview::Unreadable(format!("{}: {e}", path.display())),
    };
    if kind == MediaKind::Image {
        return match image::open(path) {
            Ok(decoded) => image_preview(&decoded, cells, rows, size),
            Err(e) => Preview::Unreadable(format!("{} does not decode as an image: {e}", path.display())),
        };
    }
    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(e) => return Preview::Unreadable(format!("{}: {e}", path.display())),
    };
    let mut bytes = Vec::new();
    use std::io::Read;
    if let Err(e) = (&mut file).take(PREVIEW_BYTES).read_to_end(&mut bytes) {
        return Preview::Unreadable(format!("{}: {e}", path.display()));
    }
    of_bytes(&bytes, kind, size)
}

/// A preview of `bytes` that are the start of something `size` bytes long.
pub fn of_bytes(bytes: &[u8], kind: MediaKind, size: u64) -> Preview {
    let texty = matches!(
        kind,
        MediaKind::Text | MediaKind::Record | MediaKind::Table | MediaKind::Numeric | MediaKind::Boolean | MediaKind::Urn
    );
    match std::str::from_utf8(bytes) {
        Ok(text) if texty || !text.contains('\0') => Preview::Text {
            lines: text.lines().map(|l| l.replace('\t', "    ")).collect(),
            cut: (bytes.len() as u64) < size,
            size,
        },
        // Text cut mid-character at the read bound is still text.
        Err(e) if texty && e.valid_up_to() + 4 > bytes.len() => {
            let text = String::from_utf8_lossy(&bytes[..e.valid_up_to()]);
            Preview::Text {
                lines: text.lines().map(|l| l.replace('\t', "    ")).collect(),
                cut: true,
                size,
            }
        }
        _ => Preview::Bytes {
            lines: bytes
                .chunks(16)
                .take(64)
                .enumerate()
                .map(|(i, chunk)| {
                    let hex: Vec<String> = chunk.iter().map(|b| format!("{b:02x}")).collect();
                    let ascii: String = chunk
                        .iter()
                        .map(|&b| if (0x20..0x7f).contains(&b) { b as char } else { '.' })
                        .collect();
                    format!("{:08x}  {:<48} {ascii}", i * 16, hex.join(" "))
                })
                .collect(),
            size,
        },
    }
}

fn image_preview(decoded: &image::DynamicImage, cells: u16, rows: u16, size: u64) -> Preview {
    let (width, height) = (decoded.width(), decoded.height());
    // A cell is two pixels tall: fit `cells` × `rows * 2` keeping the aspect.
    let box_w = cells.max(1) as f64;
    let box_h = (rows.max(1) as f64) * 2.0;
    let scale = (box_w / width.max(1) as f64).min(box_h / height.max(1) as f64).min(1.0);
    let w = ((width as f64 * scale).round() as u32).max(1);
    let h = ((height as f64 * scale).round() as u32).max(2);
    let small = decoded
        .resize_exact(w, h, image::imageops::FilterType::Triangle)
        .to_rgb8();
    let mut cells_out = Vec::with_capacity(h.div_ceil(2) as usize);
    for y in (0..h).step_by(2) {
        let mut row = Vec::with_capacity(w as usize);
        for x in 0..w {
            let top = small.get_pixel(x, y);
            let bottom = if y + 1 < h { *small.get_pixel(x, y + 1) } else { *top };
            row.push((Rgb(top[0], top[1], top[2]), Rgb(bottom[0], bottom[1], bottom[2])));
        }
        cells_out.push(row);
    }
    Preview::Image {
        cells: cells_out,
        width,
        height,
        size,
    }
}

/// Draw `preview` into `area`, scrolled down by `scroll` lines.
pub fn draw(buffer: &mut Buffer, area: Area, preview: &Preview, scroll: usize, theme: &Theme) {
    let ink = theme.fg(Tone::Ink).patch(theme.chrome());
    let muted = theme.fg(Tone::Muted).patch(theme.chrome());
    let put_lines = |buffer: &mut Buffer, lines: &[String], style: Style, footer: Option<String>| {
        let room = area.height as usize - footer.is_some() as usize;
        for (row, line) in lines.iter().skip(scroll).take(room).enumerate() {
            buffer.set_string(area.x, area.y + row as u16, text::truncate(line, area.width as usize), style);
        }
        if let Some(footer) = footer {
            buffer.set_string(
                area.x,
                area.y + area.height - 1,
                text::truncate(&footer, area.width as usize),
                muted,
            );
        }
    };
    match preview {
        Preview::Text { lines, cut, size } => {
            let footer = if *cut {
                Some(format!("… the first {} of {}", text::bytes(PREVIEW_BYTES), text::bytes(*size)))
            } else {
                None
            };
            put_lines(buffer, lines, ink, footer);
        }
        Preview::Bytes { lines, size } => {
            put_lines(buffer, lines, ink, Some(format!("{} of bytes", text::bytes(*size))));
        }
        Preview::Unreadable(why) => {
            buffer.set_string(area.x, area.y, text::truncate(why, area.width as usize), theme.fg(Tone::Danger));
        }
        Preview::Image {
            cells,
            width,
            height,
            size,
        } => {
            let caption = format!("{width}×{height} · {}", text::bytes(*size));
            if theme.depth == ColorDepth::None {
                buffer.set_string(area.x, area.y, format!("An image, {caption}"), ink);
                return;
            }
            let color = |rgb: Rgb| match theme.depth {
                ColorDepth::TrueColor => Color::Rgb(rgb.0, rgb.1, rgb.2),
                _ => Color::Indexed(nearest_xterm_256(rgb)),
            };
            let rows = (area.height as usize).saturating_sub(1);
            for (y, row) in cells.iter().take(rows).enumerate() {
                for (x, (top, bottom)) in row.iter().take(area.width as usize).enumerate() {
                    buffer[(area.x + x as u16, area.y + y as u16)]
                        .set_char('▀')
                        .set_style(Style::default().fg(color(*top)).bg(color(*bottom)));
                }
            }
            buffer.set_string(area.x, area.y + area.height - 1, caption, muted);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST13041: UTF-8 text previews as its lines; text cut mid-character at
    // the read bound is still text, and says it was cut; bytes that are not
    // text are shown as a hex dump with their printable characters.
    #[test]
    fn test13041_previews_tell_text_from_bytes() {
        match of_bytes(b"line one\nline two", MediaKind::Text, 17) {
            Preview::Text { lines, cut, .. } => {
                assert_eq!(lines, vec!["line one", "line two"]);
                assert!(!cut);
            }
            other => panic!("text previews as text: {other:?}"),
        }
        // "é" is two bytes; the read stopped after the first.
        match of_bytes(&[b'c', b'a', b'f', 0xc3], MediaKind::Text, 100) {
            Preview::Text { lines, cut, .. } => {
                assert_eq!(lines, vec!["caf"]);
                assert!(cut);
            }
            other => panic!("cut text is still text: {other:?}"),
        }
        match of_bytes(&[0x89, b'P', b'N', b'G', 0, 1], MediaKind::Binary, 6) {
            Preview::Bytes { lines, .. } => {
                assert!(lines[0].starts_with("00000000  89 50 4e 47 00 01"), "{}", lines[0]);
                assert!(lines[0].ends_with(".PNG.."), "{}", lines[0]);
            }
            other => panic!("bytes preview as hex: {other:?}"),
        }
    }

    // TEST13042: an image is fitted to its box keeping its aspect — a cell
    // being two pixels tall — and never enlarged.
    #[test]
    fn test13042_image_fits_its_box_by_half_blocks() {
        let wide = image::DynamicImage::new_rgb8(400, 100);
        match image_preview(&wide, 40, 20, 1) {
            Preview::Image { cells, .. } => {
                assert_eq!(cells[0].len(), 40, "width-bound: fills the columns");
                assert_eq!(cells.len(), 5, "100/400 of 40 columns is 10 pixels: five cells");
            }
            other => panic!("{other:?}"),
        }
        let small = image::DynamicImage::new_rgb8(8, 4);
        match image_preview(&small, 40, 20, 1) {
            Preview::Image { cells, .. } => {
                assert_eq!(cells[0].len(), 8, "a small image is not enlarged");
                assert_eq!(cells.len(), 2);
            }
            other => panic!("{other:?}"),
        }
    }
}
