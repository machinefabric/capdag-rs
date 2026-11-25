//! Text as the terminal measures it: display width, ellipsis, padding.

use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

/// How many terminal columns `text` takes.
pub fn width(text: &str) -> usize {
    UnicodeWidthStr::width(text)
}

/// `text` cut to at most `max` columns, ending in `…` when anything was cut.
/// Cuts between grapheme clusters, never inside one.
pub fn truncate(text: &str, max: usize) -> String {
    if width(text) <= max {
        return text.to_string();
    }
    if max == 0 {
        return String::new();
    }
    let mut out = String::new();
    let mut used = 0;
    for grapheme in text.graphemes(true) {
        let w = width(grapheme);
        if used + w + 1 > max {
            break;
        }
        out.push_str(grapheme);
        used += w;
    }
    out.push('…');
    out
}

/// `text` cut in the middle to at most `max` columns: the start and the end
/// both stay readable (a path, a URN).
pub fn truncate_middle(text: &str, max: usize) -> String {
    if width(text) <= max {
        return text.to_string();
    }
    if max < 3 {
        return truncate(text, max);
    }
    let graphemes: Vec<&str> = text.graphemes(true).collect();
    let keep = max - 1;
    let head_budget = keep.div_ceil(2);
    let tail_budget = keep - head_budget;
    let mut head = String::new();
    let mut used = 0;
    for g in &graphemes {
        if used + width(g) > head_budget {
            break;
        }
        head.push_str(g);
        used += width(g);
    }
    let mut tail: Vec<&str> = Vec::new();
    let mut used = 0;
    for g in graphemes.iter().rev() {
        if used + width(g) > tail_budget {
            break;
        }
        tail.push(g);
        used += width(g);
    }
    tail.reverse();
    format!("{head}…{}", tail.concat())
}

/// `text` padded with spaces on the right to exactly `columns`, cut first
/// when it is wider.
pub fn pad(text: &str, columns: usize) -> String {
    let cut = truncate(text, columns);
    let w = width(&cut);
    format!("{cut}{}", " ".repeat(columns.saturating_sub(w)))
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST20150: cutting text to a column budget never exceeds the budget,
    // keeps whole grapheme clusters (a wide CJK character takes two columns
    // and is never split), and says it cut with an ellipsis.
    #[test]
    fn test20150_truncate_respects_display_width_and_graphemes() {
        assert_eq!(truncate("Extract text", 20), "Extract text");
        assert_eq!(truncate("Extract text", 8), "Extract…");
        // "日本語" is six columns; five leave room for one character and the
        // ellipsis, never half of a second one.
        assert_eq!(truncate("日本語", 5), "日本…");
        assert_eq!(width(&truncate("日本語", 5)), 5);
        assert_eq!(truncate("日本語", 4), "日…");
        assert_eq!(truncate("abc", 0), "");
    }
}
