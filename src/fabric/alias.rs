//! Fabric aliases — the DNS-analogue translation layer over URNs.
//!
//! An **alias** is a first-class fabric definition (like a cap or a media
//! def): a short, contiguous, case-insensitive name that resolves to
//! exactly one cap or media URN. Aliases are unique by name; many distinct
//! aliases may point at the same target; an alias always points at exactly
//! one target, and that target must be a cap or media URN that exists in
//! the same registry snapshot.
//!
//! On the wire and on disk an alias is stored exactly like caps and media
//! defs: a per-definition object at `aliases/<sha256-of-name>/<defver>.json`
//! referenced from the manifest's `aliases` map (`name -> defver`). The
//! body is a [`StoredAlias`].
//!
//! ## Name rules
//!
//! An alias name is normalized to lowercase and must match
//! `[a-z0-9._-]+`. The crucial invariant is that a name can **never**
//! contain `:` — that is what lets any caller decide, for a single
//! contiguous identifier, whether it is a tagged URN (`prefix:tag;…`,
//! which always contains `:`) or an alias (no `:`). See [`is_alias_token`].

use serde::{Deserialize, Serialize};

/// The kind of thing an alias resolves to. An alias target is always a
/// URN; the kind is determined by the URN prefix (`cap:` vs `media:`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AliasTargetKind {
    Cap,
    Media,
}

impl AliasTargetKind {
    pub fn as_str(self) -> &'static str {
        match self {
            AliasTargetKind::Cap => "cap",
            AliasTargetKind::Media => "media",
        }
    }
}

/// Stored alias definition. Mirrors `fabric/alias.schema.json` on the wire
/// and is the body cached at `aliases/<sha256-of-name>/<defver>.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredAlias {
    /// The normalized (lowercase) alias name. Unique across the snapshot.
    pub name: String,
    /// The canonical URN this alias resolves to (a `cap:` or `media:` URN).
    pub target: String,
    /// Per-definition version. Always >= 1 — aliases are introduced in the
    /// versioned regime; there is no v0 flat-path alias.
    pub version: u32,
}

/// Error raised when an alias name is malformed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AliasNameError {
    #[error("alias name is empty")]
    Empty,
    #[error("alias name '{0}' contains ':' — aliases must never look like a tagged URN")]
    ContainsColon(String),
    #[error("alias name '{0}' contains whitespace")]
    ContainsWhitespace(String),
    #[error(
        "alias name '{name}' contains invalid character {ch:?}; allowed: lowercase letters, digits, '.', '_', '-'"
    )]
    InvalidChar { name: String, ch: char },
}

/// A single contiguous token "looks like a URN" iff it contains a colon.
/// Every tagged URN has the shape `prefix:...`, so the presence of `:` is
/// the unambiguous discriminator between a URN and an alias name. This is
/// the one detection rule shared by every call site (registry, machine
/// notation, CLI).
pub fn token_is_urn(token: &str) -> bool {
    token.contains(':')
}

/// The complement of [`token_is_urn`]: a token with no colon is an alias
/// candidate. (It still has to pass [`normalize_alias_name`] to be a
/// *valid* alias name; this just routes detection.)
pub fn is_alias_token(token: &str) -> bool {
    !token_is_urn(token)
}

/// Normalize and validate an alias name. Lowercases the input, then
/// enforces the lexical rules: non-empty, no `:`, no whitespace, only
/// `[a-z0-9._-]`. Returns the canonical (lowercased) name, or a hard
/// error — there is no lenient path.
pub fn normalize_alias_name(name: &str) -> Result<String, AliasNameError> {
    if name.is_empty() {
        return Err(AliasNameError::Empty);
    }
    // Reject colon and whitespace with specific errors before the generic
    // char-class check, so the message points at the real problem.
    if name.contains(':') {
        return Err(AliasNameError::ContainsColon(name.to_string()));
    }
    if name.chars().any(char::is_whitespace) {
        return Err(AliasNameError::ContainsWhitespace(name.to_string()));
    }
    let lowered = name.to_ascii_lowercase();
    for ch in lowered.chars() {
        let ok = ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-');
        if !ok {
            return Err(AliasNameError::InvalidChar {
                name: name.to_string(),
                ch,
            });
        }
    }
    Ok(lowered)
}

/// Classify an alias target URN by prefix. The target must parse as a cap
/// or media URN; anything else is rejected by the caller as an invalid
/// target. Returns the kind plus the canonical target string.
pub fn classify_alias_target(target: &str) -> Option<AliasTargetKind> {
    if crate::CapUrn::from_string(target).is_ok() {
        return Some(AliasTargetKind::Cap);
    }
    if crate::MediaUrn::from_string(target).is_ok() {
        return Some(AliasTargetKind::Media);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST1880: alias name normalization lowercases and accepts the allowed
    // character class; rejects colon, whitespace, and out-of-class chars
    // with the right error. A broken validator would let a URN-shaped or
    // whitespace name through, or mangle a valid name.
    #[test]
    fn test1880_alias_name_normalization_rules() {
        assert_eq!(normalize_alias_name("JSONDoc").unwrap(), "jsondoc");
        assert_eq!(normalize_alias_name("pdf2text").unwrap(), "pdf2text");
        assert_eq!(
            normalize_alias_name("my.alias-1_x").unwrap(),
            "my.alias-1_x"
        );

        assert!(matches!(
            normalize_alias_name(""),
            Err(AliasNameError::Empty)
        ));
        assert!(matches!(
            normalize_alias_name("pdf:text"),
            Err(AliasNameError::ContainsColon(_))
        ));
        assert!(matches!(
            normalize_alias_name("my alias"),
            Err(AliasNameError::ContainsWhitespace(_))
        ));
        assert!(matches!(
            normalize_alias_name("a/b"),
            Err(AliasNameError::InvalidChar { .. })
        ));
    }

    // TEST1881: URN-vs-alias detection keys purely on the presence of ':'.
    // The whole design rests on this discriminator being exact.
    #[test]
    fn test1881_token_urn_vs_alias_detection() {
        assert!(token_is_urn(
            "cap:in=\"media:document;ext=pdf\";extract;out=\"media:enc=utf-8\""
        ));
        assert!(token_is_urn("media:fmt=json;record"));
        assert!(!token_is_urn("pdf2text"));
        assert!(is_alias_token("pdf2text"));
        assert!(!is_alias_token("media:enc=utf-8"));
    }

    // TEST1882: alias target classification distinguishes cap from media by
    // prefix and rejects a non-URN target. The typed-boundary enforcement
    // in the registry depends on this.
    #[test]
    fn test1882_classify_alias_target_by_prefix() {
        assert_eq!(
            classify_alias_target("media:fmt=json;record"),
            Some(AliasTargetKind::Media)
        );
        assert_eq!(
            classify_alias_target(
                "cap:effect=patch;in=\"media:image\";name;out=\"media:ext=png;image\""
            ),
            Some(AliasTargetKind::Cap)
        );
        assert_eq!(classify_alias_target("not-a-urn"), None);
    }
}
