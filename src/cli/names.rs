//! What a person is shown for a cap, a media type or a machine: its alias.
//!
//! Every cap and media type the fabric names has an alias a person types —
//! `disbind-pdf`, `pdf`, `page-text` — and every place the command line shows
//! one, it shows that alias (the shortest, ties alphabetical, as the
//! serializer chooses). The URN is shown only on request: `-v`, `capdag info`,
//! and the `urn` field of every `--json` record. A machine is shown in alias
//! form, steps by their caps' aliases and nodes by their media's aliases:
//! `[(pdf) -> disbind -> (page-text)]`.

use crate::fabric::registry::FabricRegistry;
use crate::machine::{Machine, NotationFormat};
use crate::{Cap, MediaUrn};

/// A cap as a person names it: its display alias, else its title.
pub fn cap(registry: &FabricRegistry, cap: &Cap) -> String {
    registry
        .display_alias_for_urn(&cap.urn.to_string())
        .unwrap_or_else(|| {
            if cap.aliases.is_empty() {
                cap.title.clone()
            } else {
                cap.primary_alias().to_string()
            }
        })
}

/// A cap URN as a person names it: the cap's display alias, else the URN.
pub fn cap_urn(registry: &FabricRegistry, urn: &str) -> String {
    registry
        .display_alias_for_urn(urn)
        .unwrap_or_else(|| urn.to_string())
}

/// A media type as a person names it: its display alias, else its title,
/// else the URN.
pub fn media(registry: &FabricRegistry, media: &MediaUrn) -> String {
    let urn = media.to_string();
    registry
        .display_alias_for_urn(&urn)
        // A name is a label: with no title at hand, for whatever reason, the
        // URN itself names the type.
        .or_else(|| registry.get_cached_media_def(&urn).ok().map(|def| def.title))
        .unwrap_or(urn)
}

/// A media URN string as a person names it.
pub fn media_urn(registry: &FabricRegistry, urn: &str) -> String {
    match MediaUrn::from_string(urn) {
        Ok(parsed) => media(registry, &parsed),
        Err(_) => urn.to_string(),
    }
}

/// A machine in alias form, one statement per line.
pub fn machine(registry: &FabricRegistry, machine: &Machine) -> Result<String, String> {
    machine
        .to_machine_notation_aliased(registry, NotationFormat::Bracketed)
        .map(|notation| one_statement_per_line(&notation))
        .map_err(|e| e.to_string())
}

/// Machine notation, canonical or not, in alias form: parsed against the
/// fabric and written again.
pub async fn notation(registry: &FabricRegistry, notation: &str) -> Result<String, String> {
    let parsed = crate::machine::parse_machine_async(notation, registry)
        .await
        .map_err(|e| e.to_string())?;
    machine(registry, &parsed)
}

/// `[a][b][c]` as one bracketed statement per line.
fn one_statement_per_line(notation: &str) -> String {
    let mut out = String::with_capacity(notation.len() + 8);
    let mut depth = 0usize;
    let mut in_quotes = false;
    for c in notation.chars() {
        match c {
            '"' => in_quotes = !in_quotes,
            '[' if !in_quotes => depth += 1,
            ']' if !in_quotes => depth = depth.saturating_sub(1),
            _ => {}
        }
        out.push(c);
        if c == ']' && !in_quotes && depth == 0 {
            out.push('\n');
        }
    }
    out.trim_end().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST20112: alias-form notation is shown one statement per line, quoted
    // brackets left alone.
    #[test]
    fn test20112_statements_are_shown_one_per_line() {
        assert_eq!(
            one_statement_per_line("[(pdf) -> disbind -> (page)][(page) -> save-as-txt -> (txt)]"),
            "[(pdf) -> disbind -> (page)]\n[(page) -> save-as-txt -> (txt)]"
        );
        assert_eq!(
            one_statement_per_line("[e cap:in=\"media:a;x=[1]\";out=\"media:b\"][(a) -> e -> (b)]"),
            "[e cap:in=\"media:a;x=[1]\";out=\"media:b\"]\n[(a) -> e -> (b)]"
        );
    }
}
