//! Page/index selection grammar shared by page-producing cartridges
//! (pdfcartridge render/disbind, txtcartridge pagination).
//!
//! Grammar (1-based, inclusive): comma-separated segments, each one of
//! - `N`        — a single page
//! - `A-B`      — a contiguous range
//! - `A-`       — from A to the end of the document
//!
//! Semantics:
//! - Ranges extending PAST the end are clamped to the document ("give
//!   me 1-100" of a 10-page document renders the 10 that exist) — the
//!   common user intent, not an error.
//! - A segment that starts past the end is a hard error naming both
//!   numbers: the user asked for pages that cannot exist at all, and
//!   silently returning nothing would hide it.
//! - Segments emit in the order written; duplicate indices keep their
//!   first occurrence (`5,1,3` is a deliberate ordering).
//! - `None` / empty selects every page.
//!
//! Returned indices are 0-based.

/// Parse a page-selection spec against a document of `total` pages.
pub fn parse_index_range(spec: Option<&str>, total: usize) -> Result<Vec<usize>, String> {
    let spec = match spec.map(str::trim) {
        None | Some("") => return Ok((0..total).collect()),
        Some(s) => s,
    };
    if total == 0 {
        return Err(format!(
            "index range '{}' selects from an empty document (0 pages)",
            spec
        ));
    }

    let mut out: Vec<usize> = Vec::new();
    let mut seen = vec![false; total];

    for segment in spec.split(',') {
        let segment = segment.trim();
        if segment.is_empty() {
            return Err(format!(
                "index range '{}' contains an empty segment (stray comma)",
                spec
            ));
        }

        let (start, end) = parse_segment(segment, spec, total)?;
        for idx in (start - 1)..end {
            if !seen[idx] {
                seen[idx] = true;
                out.push(idx);
            }
        }
    }

    Ok(out)
}

/// Parse one `N` / `A-B` / `A-` segment into a 1-based inclusive
/// `(start, end)` pair, clamped to `total`.
fn parse_segment(segment: &str, spec: &str, total: usize) -> Result<(usize, usize), String> {
    let parse_num = |s: &str, what: &str| -> Result<usize, String> {
        let n: usize = s.trim().parse().map_err(|_| {
            format!(
                "index range '{}': {} '{}' is not a positive number \
                 (grammar: N, A-B, A-, comma-separated)",
                spec, what, s.trim()
            )
        })?;
        if n == 0 {
            return Err(format!(
                "index range '{}': pages are 1-based, 0 is not a valid page",
                spec
            ));
        }
        Ok(n)
    };

    let (start, end) = match segment.split_once('-') {
        None => {
            let n = parse_num(segment, "page")?;
            (n, n)
        }
        Some((a, b)) => {
            let start = parse_num(a, "range start")?;
            let end = if b.trim().is_empty() {
                total
            } else {
                parse_num(b, "range end")?
            };
            (start, end)
        }
    };

    if start > end {
        return Err(format!(
            "index range '{}': segment '{}' runs backwards ({} > {})",
            spec, segment, start, end
        ));
    }
    if start > total {
        return Err(format!(
            "index range '{}': segment '{}' starts at page {} but the document \
             has only {} page{}",
            spec,
            segment,
            start,
            total,
            if total == 1 { "" } else { "s" }
        ));
    }
    // Clamp the end to the document.
    Ok((start, end.min(total)))
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST0060: full grammar — singles, ranges, open ranges, comma lists,
    // written order preserved, duplicates dropped on first occurrence.
    #[test]
    fn test0060_index_range_grammar() {
        assert_eq!(parse_index_range(None, 3).unwrap(), vec![0, 1, 2]);
        assert_eq!(parse_index_range(Some(""), 3).unwrap(), vec![0, 1, 2]);
        assert_eq!(parse_index_range(Some("2"), 5).unwrap(), vec![1]);
        assert_eq!(parse_index_range(Some("2-4"), 5).unwrap(), vec![1, 2, 3]);
        assert_eq!(parse_index_range(Some("3-"), 5).unwrap(), vec![2, 3, 4]);
        assert_eq!(
            parse_index_range(Some("1,3,5-7"), 10).unwrap(),
            vec![0, 2, 4, 5, 6]
        );
        // Written order is preserved; duplicates keep first occurrence.
        assert_eq!(
            parse_index_range(Some("5,1,3,1-2"), 10).unwrap(),
            vec![4, 0, 2, 1]
        );
    }

    // TEST0061: over-long ranges clamp to the document instead of erroring
    // (the old pdf parser hard-errored on `1-100` of a 10-page doc).
    #[test]
    fn test0061_index_range_clamps_past_end() {
        assert_eq!(
            parse_index_range(Some("1-100"), 10).unwrap(),
            (0..10).collect::<Vec<_>>()
        );
        assert_eq!(parse_index_range(Some("8-100"), 10).unwrap(), vec![7, 8, 9]);
        // A single page past the end is a start-past-end error, not a clamp
        // (see TEST0062) — clamping only widens ranges that START in bounds.
    }

    // TEST0062: genuinely impossible selections stay hard errors with
    // actionable messages.
    #[test]
    fn test0062_index_range_hard_errors() {
        // Starts past the end of the document.
        let err = parse_index_range(Some("11-20"), 10).unwrap_err();
        assert!(err.contains("starts at page 11"), "got: {err}");
        assert!(err.contains("10 pages"), "got: {err}");
        // 0 is not a page.
        assert!(parse_index_range(Some("0-3"), 10).is_err());
        // Backwards.
        assert!(parse_index_range(Some("5-2"), 10).is_err());
        // Garbage.
        assert!(parse_index_range(Some("abc"), 10).is_err());
        assert!(parse_index_range(Some("1,,3"), 10).is_err());
        // Any selection from an empty document.
        assert!(parse_index_range(Some("1"), 0).is_err());
    }
}
