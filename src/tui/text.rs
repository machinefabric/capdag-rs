//! Text as the terminal measures it: display width, ellipsis, word wrap, and
//! the human forms of sizes and durations the desktop's run view uses.

use std::time::Duration;

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

/// `text` wrapped at word boundaries into lines of at most `max` columns, at
/// most `lines` of them; the last line ends in `…` when text remains. A word
/// longer than a line is cut.
pub fn wrap(text: &str, max: usize, lines: usize) -> Vec<String> {
    if max == 0 || lines == 0 {
        return Vec::new();
    }
    let mut out: Vec<String> = Vec::new();
    let mut current = String::new();
    let words: Vec<&str> = text.split_whitespace().collect();
    let mut index = 0;
    while index < words.len() {
        let word = words[index];
        let candidate = if current.is_empty() {
            word.to_string()
        } else {
            format!("{current} {word}")
        };
        if width(&candidate) <= max {
            current = candidate;
            index += 1;
            continue;
        }
        if current.is_empty() {
            // A word wider than the line: cut it.
            current = truncate(word, max);
            index += 1;
        }
        if out.len() + 1 == lines {
            let rest: Vec<&str> = std::iter::once(current.as_str())
                .chain(words[index..].iter().copied())
                .collect();
            out.push(truncate(&rest.join(" "), max));
            return out;
        }
        out.push(std::mem::take(&mut current));
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// `text` padded with spaces on the right to exactly `columns`, cut first
/// when it is wider.
pub fn pad(text: &str, columns: usize) -> String {
    let cut = truncate(text, columns);
    let w = width(&cut);
    format!("{cut}{}", " ".repeat(columns.saturating_sub(w)))
}

/// A byte count as the run view writes it: `812 B`, `216.8 KB`, `1.2 MB`.
pub fn bytes(value: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
    let mut n = value as f64;
    let mut unit = 0;
    while n >= 1024.0 && unit < UNITS.len() - 1 {
        n /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", n.round() as u64, UNITS[unit])
    } else {
        format!("{n:.1} {}", UNITS[unit])
    }
}

/// A duration as the run view writes protocol ages: `340ms`, `4.2s`, `38s`,
/// `2m 5s`, `1h 3m`.
pub fn duration(value: Duration) -> String {
    let ms = value.as_millis() as f64;
    if ms < 1000.0 {
        return format!("{}ms", ms.round() as u64);
    }
    let seconds = ms / 1000.0;
    if seconds < 60.0 {
        return if seconds < 10.0 {
            format!("{seconds:.1}s")
        } else {
            format!("{}s", seconds.round() as u64)
        };
    }
    let minutes = seconds / 60.0;
    if minutes < 60.0 {
        return format!("{}m {}s", minutes.floor() as u64, (seconds % 60.0).round() as u64);
    }
    format!("{}h {}m", (minutes / 60.0).floor() as u64, (minutes % 60.0).round() as u64)
}

/// `count` of `noun`, singular or plural: `1 file`, `6 files`.
pub fn count(count: usize, noun: &str) -> String {
    if count == 1 {
        format!("1 {noun}")
    } else {
        format!("{count} {noun}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST13010: cutting text to a column budget never exceeds the budget,
    // keeps whole grapheme clusters (a wide CJK character takes two columns
    // and is never split), and says it cut with an ellipsis.
    #[test]
    fn test13010_truncate_respects_display_width_and_graphemes() {
        assert_eq!(truncate("Extract text", 20), "Extract text");
        assert_eq!(truncate("Extract text", 8), "Extract…");
        // "日本語" is six columns; five leave room for one character and the
        // ellipsis, never half of a second one.
        assert_eq!(truncate("日本語", 5), "日本…");
        assert_eq!(width(&truncate("日本語", 5)), 5);
        assert_eq!(truncate("日本語", 4), "日…");
        assert_eq!(truncate("abc", 0), "");
    }

    // TEST13011: wrapping keeps words whole, stops at the line budget, and the
    // last line says text was left out.
    #[test]
    fn test13011_wrap_at_words_with_ellipsis_on_overflow() {
        assert_eq!(
            wrap("6 items are processed individually", 18, 3),
            vec!["6 items are", "processed", "individually"]
        );
        assert_eq!(
            wrap("6 items are processed individually", 18, 2),
            vec!["6 items are", "processed individ…"]
        );
        for line in wrap("supercalifragilisticexpialidocious and more", 10, 4) {
            assert!(width(&line) <= 10, "{line:?} is wider than 10");
        }
    }

    // TEST13012: sizes and durations read as the desktop's run view reads them.
    #[test]
    fn test13012_bytes_and_durations_read_like_the_run_view() {
        assert_eq!(bytes(812), "812 B");
        assert_eq!(bytes(222_003), "216.8 KB");
        assert_eq!(bytes(1_258_291), "1.2 MB");
        assert_eq!(duration(Duration::from_millis(340)), "340ms");
        assert_eq!(duration(Duration::from_millis(4_200)), "4.2s");
        assert_eq!(duration(Duration::from_secs(38)), "38s");
        assert_eq!(duration(Duration::from_secs(125)), "2m 5s");
        assert_eq!(duration(Duration::from_secs(3_780)), "1h 3m");
        assert_eq!(count(1, "file"), "1 file");
        assert_eq!(count(6, "file"), "6 files");
    }
}
