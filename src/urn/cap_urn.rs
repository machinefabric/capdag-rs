//! Flat Tag-Based Cap Identifier System
//!
//! This module provides a flat, tag-based cap URN system that replaces
//! hierarchical naming with key-value tags to handle cross-cutting concerns and
//! multi-dimensional cap classification.
//!
//! Cap URNs use the tagged URN format with "cap" prefix. Omitted structural
//! coordinates default to `in=media:`, `out=media:`, and `effect=declared`,
//! after which admissibility validation rejects the illegal all-default bare
//! top form.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use tagged_urn::{TaggedUrn, TaggedUrnBuilder, TaggedUrnError};

use crate::urn::media_urn::{MediaUrn, MediaUrnError, MEDIA_IDENTITY, MEDIA_OBJECT, MEDIA_VOID};

/// Functional category of a cap, derived from all four structural axes (`in`,
/// `out`, `effect`, and the remaining tags). The classification is **logical**
/// — the dispatch protocol (specificity, conformance, accepts /
/// conforms_to) does not branch on `CapKind`. The kind is exposed so
/// tools, UIs, planners, and tests can reason about a cap's role in
/// plain terms (`A → B`, generator, discarder, effect, identity
/// passthrough) without re-deriving the rules.
///
/// `media:void` is the **unit type** — the nullary value, no
/// meaningful data. Not "invalid" or "absent". `media:` is the
/// **top type** — the wildcard over every media URN. With those two
/// anchors the five kinds fall out:
///
/// | Kind             | `in`         | `out`        | `effect`      | other tags | Reads as |
/// |------------------|--------------|--------------|---------------|------------|----------|
/// | Identity         | `media:`     | `media:`     | `none`        | none       | `A → A`  |
/// | Source           | `media:void` | not `void`   | any           | any        | `() → B` |
/// | Sink             | not `void`   | `media:void` | any           | any        | `A → ()` |
/// | Effect           | `media:void` | `media:void` | any           | any        | `() → ()`|
/// | Transform        | anything else                                               |
///
/// `Identity` is the explicit `effect=none` cap on every axis: input wide
/// open, output wide open, no operation/metadata tags, and no effect on
/// runtime media/type identity. The canonical form is `cap:effect=none`.
///
/// Examples:
/// - `cap:effect=none` — Identity
/// - `cap:passthrough` — Transform (specifies the operation, even
///   though in/out are unconstrained)
/// - `cap:in=media:void;out=media:model-artifact;warm` — Source
/// - `cap:in="media:fmt=json";out="media:ext=log";void` — Sink
/// - `cap:in=media:void;out=media:void;ping` — Effect (health check)
/// - `cap:in=media:ext=pdf;out="media:enc=utf-8";extract` — Transform
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CapKind {
    /// `media:` → `media:` with `effect=none` and no other tags.
    /// The categorical identity morphism.
    Identity,
    /// `media:void` → non-`void`. A generator: produces a value with
    /// no meaningful input.
    Source,
    /// non-`void` → `media:void`. A consumer: absorbs a value with no
    /// meaningful output.
    Sink,
    /// `media:void` → `media:void`. A nullary effect: side-effect with
    /// no data flow on either end (warm cache, ping, health check).
    Effect,
    /// Anything else: a normal data-processing cap with a non-trivial
    /// in/out signature.
    Transform,
}

impl CapKind {
    /// Stable wire/log/UI label for the kind. Snake_case to match
    /// other capdag enum serializations on the wire (proto, JSON).
    pub fn as_str(&self) -> &'static str {
        match self {
            CapKind::Identity => "identity",
            CapKind::Source => "source",
            CapKind::Sink => "sink",
            CapKind::Effect => "effect",
            CapKind::Transform => "transform",
        }
    }
}

/// Effect on runtime media/type identity carried by a cap URN.
///
/// `Declared` is the default when the `effect` coordinate is omitted.
/// `Any` represents the explicit unconstrained request form (`?effect` or
/// `effect=*`), not the default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapEffect {
    Declared,
    None,
    Patch,
    Any,
}

impl CapEffect {
    pub fn as_str(&self) -> &'static str {
        match self {
            CapEffect::Declared => "declared",
            CapEffect::None => "none",
            CapEffect::Patch => "patch",
            CapEffect::Any => "?",
        }
    }

    pub fn is_unconstrained(&self) -> bool {
        matches!(self, CapEffect::Any)
    }
}

impl fmt::Display for CapKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A cap URN using flat, ordered tags with required direction specifiers
///
/// Direction (in→out) is integral to a cap's identity. The `in_urn` and `out_urn`
/// fields specify the input and output media URNs respectively.
///
/// Examples:
/// - `cap:in="media:binary";generate;out="media:binary";target=thumbnail`
/// - `cap:dimensions;in=media:void;out=media:integer`
/// - `cap:in="media:string";out="media:object";key="Value With Spaces"`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CapUrn {
    /// Input media URN - required (use media:void for caps with no input)
    in_urn: String,
    /// Output media URN - required
    out_urn: String,
    /// Effect on runtime media/type identity. Stored as canonical raw value:
    /// `declared`, `none`, `patch`, or `?` for explicit unconstrained effect.
    effect: String,
    /// Additional tags that define this cap, stored in sorted order for canonical representation
    /// Note: 'in', 'out', and 'effect' are NOT stored here.
    pub tags: BTreeMap<String, String>,
}

// Per-tag truth-table specificity scoring is owned by the
// tagged_urn crate (the same scorer applies uniformly to media-URN
// tags, cap-tag y-axis, and any other Tagged URN dimension). We
// re-use the canonical implementation rather than duplicate it; any
// drift here would be a wire-level inconsistency.
use tagged_urn::score_tag_value;

impl CapUrn {
    /// The required prefix for all cap URNs
    pub const PREFIX: &'static str = "cap";

    fn validate_non_structural_tags(tags: &BTreeMap<String, String>) -> Result<(), CapUrnError> {
        let mut builder = TaggedUrnBuilder::new(Self::PREFIX);
        for (k, v) in tags {
            builder = builder
                .tag(k, v)
                .map_err(CapUrnError::from_tagged_urn_error)?;
        }
        let _ = builder.build_allow_empty();
        Ok(())
    }

    /// Create a new cap URN from direction specs and additional tags
    /// Keys are normalized to lowercase; values are preserved as-is
    /// in_urn and out_urn are required direction specifiers (media URN strings)
    /// Media URNs are parsed and normalized to canonical form for consistent matching.
    pub fn new(
        in_urn: String,
        out_urn: String,
        effect: String,
        tags: BTreeMap<String, String>,
    ) -> Result<Self, CapUrnError> {
        use crate::urn::media_urn::MediaUrn;

        // Normalize in_urn to canonical form
        let in_urn_normalized = if in_urn == "media:" {
            in_urn
        } else {
            MediaUrn::from_string(&in_urn)
                .map_err(|e| {
                    CapUrnError::InvalidInSpec(format!(
                        "Invalid media URN for in spec '{}': {}",
                        in_urn, e
                    ))
                })?
                .to_string()
        };

        // Normalize out_urn to canonical form
        let out_urn_normalized = if out_urn == "media:" {
            out_urn
        } else {
            MediaUrn::from_string(&out_urn)
                .map_err(|e| {
                    CapUrnError::InvalidOutSpec(format!(
                        "Invalid media URN for out spec '{}': {}",
                        out_urn, e
                    ))
                })?
                .to_string()
        };

        let effect_normalized = Self::normalize_effect_value(Some(effect.as_str()))?;

        let normalized_tags: BTreeMap<String, String> = tags
            .into_iter()
            .filter(|(k, _)| {
                let k_lower = k.to_lowercase();
                k_lower != "in" && k_lower != "out" && k_lower != "effect"
            })
            .map(|(k, v)| (k.to_lowercase(), v))
            .collect();
        Self::validate_non_structural_tags(&normalized_tags)?;
        let candidate = Self {
            in_urn: in_urn_normalized,
            out_urn: out_urn_normalized,
            effect: effect_normalized,
            tags: normalized_tags,
        };
        candidate.validate_admissible()?;
        Ok(candidate)
    }

    /// Create a cap URN from a tag map.
    ///
    /// Missing structural coordinates default exactly as they do in string
    /// parsing:
    /// - missing `in`   -> `media:`
    /// - missing `out`  -> `media:`
    /// - missing effect -> `declared`
    pub fn from_tags(mut tags: BTreeMap<String, String>) -> Result<Self, CapUrnError> {
        let in_urn = tags
            .remove("in")
            .unwrap_or_else(|| MEDIA_IDENTITY.to_string());
        let out_urn = tags
            .remove("out")
            .unwrap_or_else(|| MEDIA_IDENTITY.to_string());
        let effect = tags
            .remove("effect")
            .unwrap_or_else(|| "declared".to_string());
        Self::new(in_urn, out_urn, effect, tags)
    }

    /// Create a cap URN from a string representation
    ///
    /// Format: `cap:in="media:...";out="media:...";key1=value1;...`
    /// The "cap:" prefix is mandatory
    ///
    /// **Wildcard expansion for in/out tags:**
    /// - Missing `in` or `out` tag → defaults to `media:`
    /// - `in` or `out` without `=` → becomes `media:` (TaggedUrn treats `tag` as `tag=*`, we replace `*` with `media:`)
    /// - `in=*` or `out=*` → replaced with `media:`
    /// - `cap:` normalizes to `cap:in=media:;out=media:` and is then rejected as the illegal bare top form
    /// - `cap:in` normalizes to `cap:in=media:;out=media:` and is then rejected as the illegal bare top form
    /// - `cap:in=media:;out` normalizes to `cap:in=media:;out=media:` and is then rejected as the illegal bare top form
    ///
    /// Trailing semicolons are optional and ignored
    /// Tags are automatically sorted alphabetically for canonical form
    ///
    /// Case handling (inherited from TaggedUrn):
    /// - Keys: Always normalized to lowercase
    /// - Unquoted values: Normalized to lowercase
    /// - Quoted values: Case preserved exactly as specified
    pub fn from_string(s: &str) -> Result<Self, CapUrnError> {
        // Parse using TaggedUrn
        let tagged = TaggedUrn::from_string(s).map_err(CapUrnError::from_tagged_urn_error)?;

        // Verify cap prefix
        if tagged.prefix != Self::PREFIX {
            return Err(CapUrnError::MissingCapPrefix);
        }

        // Process in and out tags with wildcard expansion
        // Missing tag or tag=* → "media:" (the wildcard)
        let in_urn_raw = Self::process_direction_tag(&tagged, "in")?;
        let out_urn_raw = Self::process_direction_tag(&tagged, "out")?;
        let effect = Self::normalize_effect_value(tagged.tags.get("effect").map(|s| s.as_str()))?;

        // Parse and normalize media URNs to canonical form.
        // This ensures consistent tag ordering (e.g., "record;enc=utf-8" vs "enc=utf-8;record").
        use crate::urn::media_urn::MediaUrn;
        let in_urn = if in_urn_raw == "media:" {
            in_urn_raw
        } else {
            MediaUrn::from_string(&in_urn_raw)
                .map_err(|e| {
                    CapUrnError::InvalidInSpec(format!(
                        "Invalid media URN for in spec '{}': {}",
                        in_urn_raw, e
                    ))
                })?
                .to_string()
        };
        let out_urn = if out_urn_raw == "media:" {
            out_urn_raw
        } else {
            MediaUrn::from_string(&out_urn_raw)
                .map_err(|e| {
                    CapUrnError::InvalidOutSpec(format!(
                        "Invalid media URN for out spec '{}': {}",
                        out_urn_raw, e
                    ))
                })?
                .to_string()
        };

        // Collect remaining tags (excluding in/out)
        let tags: BTreeMap<String, String> = tagged
            .tags
            .into_iter()
            .filter(|(k, _)| k != "in" && k != "out" && k != "effect")
            .collect();
        Self::validate_non_structural_tags(&tags)?;

        let candidate = Self {
            in_urn,
            out_urn,
            effect,
            tags,
        };
        candidate.validate_admissible()?;
        Ok(candidate)
    }

    /// Process a direction tag (in or out) with wildcard expansion
    ///
    /// - Missing tag → "media:" (wildcard)
    /// - tag=* → "media:" (wildcard)
    /// - tag= (empty) → error
    /// - tag=value → value (validated later)
    fn process_direction_tag(tagged: &TaggedUrn, tag_name: &str) -> Result<String, CapUrnError> {
        match tagged.tags.get(tag_name) {
            Some(value) => {
                if value == "*" {
                    // Replace * with media: wildcard
                    Ok("media:".to_string())
                } else if value.is_empty() {
                    // Empty value is not allowed (in= or out= with nothing after =)
                    if tag_name == "in" {
                        Err(CapUrnError::InvalidInSpec(
                            "Empty value for 'in' tag is not allowed".to_string(),
                        ))
                    } else {
                        Err(CapUrnError::InvalidOutSpec(
                            "Empty value for 'out' tag is not allowed".to_string(),
                        ))
                    }
                } else {
                    // Regular value - will be validated as MediaUrn later
                    Ok(value.clone())
                }
            }
            None => {
                // Tag is missing - default to media: wildcard
                Ok("media:".to_string())
            }
        }
    }

    fn normalize_effect_value(raw: Option<&str>) -> Result<String, CapUrnError> {
        match raw {
            None => Ok("declared".to_string()),
            Some("?") | Some("*") => Ok("?".to_string()),
            Some("declared") => Ok("declared".to_string()),
            Some("none") => Ok("none".to_string()),
            Some("patch") => Ok("patch".to_string()),
            Some(value) if value.is_empty() => Err(CapUrnError::InvalidEffect(
                "Empty value for 'effect' tag is not allowed".to_string(),
            )),
            Some(value) => Err(CapUrnError::InvalidEffect(format!(
                "Unsupported effect '{}'. Supported values are declared, none, patch, or explicit unconstrained ?effect/effect=*",
                value
            ))),
        }
    }

    fn validate_admissible(&self) -> Result<(), CapUrnError> {
        let in_media = self.in_media_urn().map_err(|e| {
            CapUrnError::InvalidInSpec(format!(
                "Stored in_urn failed CapUrn admissibility validation: {}",
                e
            ))
        })?;
        let out_media = self.out_media_urn().map_err(|e| {
            CapUrnError::InvalidOutSpec(format!(
                "Stored out_urn failed CapUrn admissibility validation: {}",
                e
            ))
        })?;

        if in_media.is_top()
            && out_media.is_top()
            && self.tags.is_empty()
            && self.effect() == CapEffect::Declared
        {
            return Err(CapUrnError::IllegalDeclaration(
                "illegal bare top cap; use cap:effect=none for identity, or declare a non-vacuous input/output/effect/tag"
                    .to_string(),
            ));
        }

        match self.effect() {
            CapEffect::Declared | CapEffect::Any => Ok(()),
            CapEffect::None => {
                let sound = in_media.conforms_to(&out_media).map_err(|e| {
                    CapUrnError::IllegalDeclaration(format!(
                        "failed to verify effect=none admissibility for in='{}' out='{}': {}",
                        in_media, out_media, e
                    ))
                })?;
                if !sound {
                    return Err(CapUrnError::IllegalDeclaration(format!(
                        "effect=none requires declared input '{}' to conform to declared output '{}'",
                        in_media, out_media
                    )));
                }
                Ok(())
            }
            CapEffect::Patch => {
                let delta = out_media.delta_from(&in_media).map_err(|e| {
                    CapUrnError::IllegalDeclaration(format!(
                        "effect=patch requires a computable declared media delta from '{}' to '{}': {}",
                        in_media, out_media, e
                    ))
                })?;
                let witness = in_media.apply_delta(&delta).map_err(|e| {
                    CapUrnError::IllegalDeclaration(format!(
                        "effect=patch failed to apply declared media delta to input '{}': {}",
                        in_media, e
                    ))
                })?;
                let sound = witness.conforms_to(&out_media).map_err(|e| {
                    CapUrnError::IllegalDeclaration(format!(
                        "failed to verify effect=patch admissibility for witness '{}' against declared output '{}': {}",
                        witness, out_media, e
                    ))
                })?;
                if !sound {
                    return Err(CapUrnError::IllegalDeclaration(format!(
                        "effect=patch witness '{}' does not conform to declared output '{}'",
                        witness, out_media
                    )));
                }
                Ok(())
            }
        }
    }

    /// Get the canonical string representation of this cap URN
    ///
    /// Always includes "cap:" prefix
    /// All tags (including in/out) are sorted alphabetically
    /// No trailing semicolon in canonical form
    /// Values are quoted only when necessary (smart quoting via TaggedUrn)
    /// Build a TaggedUrn representation of this CapUrn (internal helper)
    ///
    /// `in` and `out` segments are emitted only when they refine beyond
    /// the trivial wildcard `media:`. `effect=declared` is omitted because
    /// it is the default on admissible caps.
    ///
    /// `effect=none` is not omitted. The true identity cap canonicalizes to
    /// `cap:effect=none`, never to `cap:`.
    fn build_tagged_urn(&self) -> TaggedUrn {
        let mut builder = TaggedUrnBuilder::new(Self::PREFIX);

        if self.in_urn != crate::urn::media_urn::MEDIA_IDENTITY {
            builder = builder
                .tag("in", &self.in_urn)
                .expect("in_urn guaranteed non-empty");
        }
        if self.out_urn != crate::urn::media_urn::MEDIA_IDENTITY {
            builder = builder
                .tag("out", &self.out_urn)
                .expect("out_urn guaranteed non-empty");
        }
        if self.effect != "declared" {
            builder = builder
                .tag("effect", &self.effect)
                .expect("effect guaranteed valid");
        }

        for (k, v) in &self.tags {
            // Tags are validated at construction time
            builder = builder
                .tag(k, v)
                .expect("tag values validated at construction");
        }

        // Use build_allow_empty which returns TaggedUrn directly
        builder.build_allow_empty()
    }

    /// Serialize just the tags portion (without "cap:" prefix)
    ///
    /// Returns tags in canonical form with proper quoting and sorting.
    pub fn tags_to_string(&self) -> String {
        self.build_tagged_urn().tags_to_string()
    }

    pub fn to_string(&self) -> String {
        self.build_tagged_urn().to_string()
    }

    /// Get a specific tag value
    /// Key is normalized to lowercase for lookup
    /// For 'in' and 'out', returns the direction spec fields
    pub fn get_tag(&self, key: &str) -> Option<&String> {
        let key_lower = key.to_lowercase();
        match key_lower.as_str() {
            "in" => Some(&self.in_urn),
            "out" => Some(&self.out_urn),
            "effect" => Some(&self.effect),
            _ => self.tags.get(&key_lower),
        }
    }

    /// Get the input media URN string
    pub fn in_spec(&self) -> &str {
        &self.in_urn
    }

    /// Get the output media URN string
    pub fn out_spec(&self) -> &str {
        &self.out_urn
    }

    /// Get the effect coordinate in canonical string form.
    pub fn effect_spec(&self) -> &str {
        &self.effect
    }

    /// Get the parsed effect coordinate.
    pub fn effect(&self) -> CapEffect {
        match self.effect.as_str() {
            "declared" => CapEffect::Declared,
            "none" => CapEffect::None,
            "patch" => CapEffect::Patch,
            "?" => CapEffect::Any,
            other => panic!("CapUrn invariant: invalid stored effect '{}'", other),
        }
    }

    /// Get the input as a parsed MediaUrn
    pub fn in_media_urn(&self) -> Result<MediaUrn, MediaUrnError> {
        MediaUrn::from_string(&self.in_urn)
    }

    /// Get the output as a parsed MediaUrn
    pub fn out_media_urn(&self) -> Result<MediaUrn, MediaUrnError> {
        MediaUrn::from_string(&self.out_urn)
    }

    /// Functional category of this cap, derived from all four structural axes:
    /// `in`, `out`, `effect`, and the rest of the tags.
    ///
    /// Source/Sink/Effect are decided by the directional axes
    /// alone (presence of `media:void` on either side), since the
    /// unit-vs-top reading of those slots determines whether data
    /// flows there. Tags refine the operation but don't change
    /// whether one side is the unit.
    ///
    /// See [`CapKind`] for the full taxonomy and the unit-vs-top
    /// reading of `media:void` / `media:`.
    ///
    /// Because this method parses both axes through `MediaUrn`, it
    /// returns an error if either side is somehow not a valid media
    /// URN. In normal use both fields are validated at `CapUrn`
    /// construction time, so this only fails on internally
    /// inconsistent state — a hard signal that something upstream is
    /// broken.
    pub fn kind(&self) -> Result<CapKind, MediaUrnError> {
        let in_media = self.in_media_urn()?;
        let out_media = self.out_media_urn()?;

        let in_void = in_media.is_void();
        let out_void = out_media.is_void();
        let in_top = in_media.is_top();
        let out_top = out_media.is_top();
        let no_extra_tags = self.tags.is_empty();
        let effect = self.effect();

        if in_top && out_top && no_extra_tags && effect == CapEffect::None {
            return Ok(CapKind::Identity);
        }
        if in_void && out_void {
            return Ok(CapKind::Effect);
        }
        if in_void {
            return Ok(CapKind::Source);
        }
        if out_void {
            return Ok(CapKind::Sink);
        }
        Ok(CapKind::Transform)
    }

    /// Check if this cap has a specific tag with a specific value
    /// Key is normalized to lowercase; value comparison is case-sensitive
    /// For 'in' and 'out', checks the direction spec fields
    pub fn has_tag(&self, key: &str, value: &str) -> bool {
        let key_lower = key.to_lowercase();
        match key_lower.as_str() {
            "in" => self.in_urn == value,
            "out" => self.out_urn == value,
            "effect" => self.effect == value,
            _ => self.tags.get(&key_lower).map_or(false, |v| v == value),
        }
    }

    /// Check if a marker tag (solo tag with no value) is present.
    /// A marker tag is stored as key="*" in the cap URN.
    /// Example: `cap:constrained;...` has marker tag "constrained"
    pub fn has_marker_tag(&self, tag_name: &str) -> bool {
        self.tags
            .get(&tag_name.to_lowercase())
            .map_or(false, |v| v == "*")
    }

    /// Add or update a non-structural tag.
    /// Key is normalized to lowercase; value is preserved as-is.
    ///
    /// Reserved structural coordinates (`in`, `out`, `effect`) are not part of
    /// the y-axis and must be changed through dedicated accessors.
    /// Returns error if value is empty (use "*" for wildcard).
    pub fn with_tag(mut self, key: String, value: String) -> Result<Self, CapUrnError> {
        if value.is_empty() {
            return Err(CapUrnError::EmptyValue(key));
        }
        let key_lower = key.to_lowercase();
        if key_lower == "in" || key_lower == "out" || key_lower == "effect" {
            return Err(CapUrnError::InvalidTagFormat(format!(
                "reserved structural key '{}' must be changed via dedicated CapUrn accessors",
                key_lower
            )));
        }
        self.tags.insert(key_lower, value);
        Self::validate_non_structural_tags(&self.tags)?;
        self.validate_admissible()?;
        Ok(self)
    }

    /// Create a new cap URN with a different input spec
    pub fn with_in_spec(mut self, in_urn: String) -> Self {
        self.in_urn = in_urn;
        self.validate_admissible()
            .expect("CapUrn::with_in_spec produced an illegal cap declaration");
        self
    }

    /// Create a new cap URN with a different output spec
    pub fn with_out_spec(mut self, out_urn: String) -> Self {
        self.out_urn = out_urn;
        self.validate_admissible()
            .expect("CapUrn::with_out_spec produced an illegal cap declaration");
        self
    }

    /// Create a new cap URN with a different effect coordinate.
    pub fn with_effect(mut self, effect: CapEffect) -> Self {
        self.effect = effect.as_str().to_string();
        self.validate_admissible()
            .expect("CapUrn::with_effect produced an illegal cap declaration");
        self
    }

    /// Remove a non-structural tag.
    /// Key is normalized to lowercase for case-insensitive removal.
    pub fn without_tag(mut self, key: &str) -> Self {
        let key_lower = key.to_lowercase();
        if key_lower == "in" || key_lower == "out" || key_lower == "effect" {
            panic!(
                "CapUrn::without_tag cannot remove reserved structural key '{}'",
                key_lower
            );
        }
        self.tags.remove(&key_lower);
        self.validate_admissible()
            .expect("CapUrn::without_tag produced an illegal cap declaration");
        self
    }

    /// Check if this cap (pattern/handler) accepts the given request (instance).
    ///
    /// Direction specs use semantic TaggedUrn matching via MediaUrn:
    /// - Input: `cap_in.accepts(request_in)` — does request's data satisfy cap's input requirement?
    /// - Output: `request_out.accepts(cap_out)` — does cap's output satisfy what request expects?
    ///
    /// For other tags: cap satisfies request's tag constraints.
    /// Missing cap tags are wildcards (cap accepts any value for that tag).
    pub fn accepts(&self, request: &CapUrn) -> bool {
        // Input direction: self.in_urn is pattern, request.in_urn is instance
        // "media:" on the PATTERN side means "I accept any input" — skip check.
        // "media:" on the INSTANCE side is just the least specific — still check.
        if self.in_urn != "media:" {
            let cap_in = MediaUrn::from_string(&self.in_urn).unwrap_or_else(|e| {
                panic!(
                    "CU2: cap in_spec '{}' is not a valid MediaUrn: {}",
                    self.in_urn, e
                )
            });
            let request_in = MediaUrn::from_string(&request.in_urn).unwrap_or_else(|e| {
                panic!(
                    "CU2: request in_spec '{}' is not a valid MediaUrn: {}",
                    request.in_urn, e
                )
            });
            if !cap_in
                .accepts(&request_in)
                .expect("CU2: media URN prefix mismatch in direction spec matching")
            {
                return false;
            }
        }

        // Output direction: self.out_urn is pattern, request.out_urn is instance
        // "media:" on the PATTERN side means "I accept any output" — skip check.
        // "media:" on the INSTANCE side is just the least specific — still check.
        if self.out_urn != "media:" {
            let cap_out = MediaUrn::from_string(&self.out_urn).unwrap_or_else(|e| {
                panic!(
                    "CU2: cap out_spec '{}' is not a valid MediaUrn: {}",
                    self.out_urn, e
                )
            });
            let request_out = MediaUrn::from_string(&request.out_urn).unwrap_or_else(|e| {
                panic!(
                    "CU2: request out_spec '{}' is not a valid MediaUrn: {}",
                    request.out_urn, e
                )
            });
            if !cap_out
                .conforms_to(&request_out)
                .expect("CU2: media URN prefix mismatch in direction spec matching")
            {
                return false;
            }
        }

        if self.effect != "?" && self.effect != request.effect {
            return false;
        }

        // Y-axis: every tag's per-key match runs through the six-form
        // truth table (TaggedUrn::values_match). Walk the union of
        // all keys appearing on either side so missing-on-pattern
        // and missing-on-instance cells both get evaluated.
        let all_keys: std::collections::HashSet<&String> =
            self.tags.keys().chain(request.tags.keys()).collect();
        for key in all_keys {
            let patt = self.tags.get(key).map(|s| s.as_str());
            let inst = request.tags.get(key).map(|s| s.as_str());
            if !TaggedUrn::values_match(inst, patt) {
                return false;
            }
        }
        true
    }

    /// Check if this request conforms to (can be handled by) the given cap.
    /// Equivalent to `cap.accepts(self)`.
    pub fn conforms_to(&self, cap: &CapUrn) -> bool {
        cap.accepts(self)
    }

    pub fn accepts_str(&self, request_str: &str) -> Result<bool, CapUrnError> {
        let request = CapUrn::from_string(request_str)?;
        Ok(self.accepts(&request))
    }

    pub fn conforms_to_str(&self, cap_str: &str) -> Result<bool, CapUrnError> {
        let cap = CapUrn::from_string(cap_str)?;
        Ok(self.conforms_to(&cap))
    }

    /// Check if two cap URNs are comparable in the order-theoretic sense.
    ///
    /// Two URNs are comparable if either one accepts (subsumes) the other.
    /// This is the symmetric closure of the accepts relation.
    ///
    /// Use this for routing when you want to find any handler that could
    /// potentially satisfy a request, regardless of which is more specific.
    pub fn is_comparable(&self, other: &CapUrn) -> bool {
        self.accepts(other) || other.accepts(self)
    }

    /// Check if two cap URNs are equivalent in the order-theoretic sense.
    ///
    /// Two URNs are equivalent if each accepts (subsumes) the other.
    /// This means they have the same position in the specificity lattice.
    ///
    /// Use this for exact matching where you need URNs to be interchangeable.
    pub fn is_equivalent(&self, other: &CapUrn) -> bool {
        self.accepts(other) && other.accepts(self)
    }

    /// Check if this candidate can dispatch (handle) the given request.
    ///
    /// This is the PRIMARY predicate for routing/dispatch decisions.
    ///
    /// A candidate is dispatchable for a request iff:
    /// 1. Input axis: candidate can handle request's input (candidate.in same or more specific)
    /// 2. Output axis: candidate meets request's output needs (candidate.out same or more specific)
    /// 3. Cap-tags: candidate satisfies all explicit request tags, may add more
    ///
    /// Key insight: This is NOT symmetric. `candidate.is_dispatchable(&request)` may be true
    /// while `request.is_dispatchable(&candidate)` is false.
    ///
    /// # Arguments
    /// * `request` - The request URN (partial specification, may have wildcards)
    ///
    /// # Returns
    /// * `true` if this candidate can legally handle the request
    /// * `false` if there's a contradiction or incompatibility
    pub fn is_dispatchable(&self, request: &CapUrn) -> bool {
        // Axis 1: Input - candidate must handle at least what request specifies
        if !self.input_dispatchable(request) {
            return false;
        }

        // Axis 2: Output - candidate must produce at least what request needs
        if !self.output_dispatchable(request) {
            return false;
        }

        if !self.effect_dispatchable(request) {
            return false;
        }

        // Axis 3: Cap-tags - candidate must satisfy explicit request constraints
        if !self.cap_tags_dispatchable(request) {
            return false;
        }

        true
    }

    /// Check if candidate's input is dispatchable for request's input.
    ///
    /// Input is CONTRAVARIANT: candidate with looser input constraint can handle
    /// request with stricter input. `media:` is the identity (top) and means
    /// "unconstrained" — vacuously true on either side.
    ///
    /// - Request `in=media:` (unconstrained) + any candidate -> YES (no constraint)
    /// - Candidate `in=media:` (accepts any) + Request `in=media:ext=pdf` -> YES (candidate accepts any)
    /// - Both specific -> request input must conform to candidate's accepted input
    fn input_dispatchable(&self, request: &CapUrn) -> bool {
        // Request wildcard: any candidate input is fine (request doesn't constrain what it sends)
        if request.in_urn == "media:" {
            return true;
        }

        // Candidate wildcard: candidate accepts any input, including request's specific input
        if self.in_urn == "media:" {
            return true;
        }

        // Both specific: request input must conform to candidate's input requirement
        // (request sends something the candidate can handle)
        let req_in = match MediaUrn::from_string(&request.in_urn) {
            Ok(u) => u,
            Err(_) => return false,
        };
        let prov_in = match MediaUrn::from_string(&self.in_urn) {
            Ok(u) => u,
            Err(_) => return false,
        };

        // Request input conforms to candidate input = request sends what candidate can handle
        req_in.conforms_to(&prov_in).unwrap_or(false)
    }

    /// Check if candidate's output is dispatchable for request's output.
    ///
    /// Rules:
    /// - Request wildcard (media:): any candidate output is acceptable
    /// - Otherwise: candidate output must conform to (be same or more specific than) request output
    fn output_dispatchable(&self, request: &CapUrn) -> bool {
        // Request wildcard: any candidate output is fine
        if request.out_urn == "media:" {
            return true;
        }

        // Candidate wildcard: cannot guarantee specific output request needs
        // This is asymmetric with input! Generic output doesn't satisfy specific requirement.
        if self.out_urn == "media:" {
            return false;
        }

        // Both specific: candidate output must conform to request output
        // (candidate can be same or more specific - providing more is OK)
        let req_out = match MediaUrn::from_string(&request.out_urn) {
            Ok(u) => u,
            Err(_) => return false,
        };
        let prov_out = match MediaUrn::from_string(&self.out_urn) {
            Ok(u) => u,
            Err(_) => return false,
        };

        // Candidate output conforms to request output = candidate guarantees at least what request needs
        prov_out.conforms_to(&req_out).unwrap_or(false)
    }

    /// Check if candidate's effect satisfies request's requested effect.
    ///
    /// Omitted `effect` means `declared`, not unconstrained. Unconstrained
    /// matching must be requested explicitly via `?effect` / `effect=*`.
    fn effect_dispatchable(&self, request: &CapUrn) -> bool {
        request.effect == "?" || self.effect == request.effect
    }

    /// Check if candidate's cap-tags are dispatchable for request's cap-tags.
    ///
    /// Rules:
    /// - Every explicit request tag must be satisfied by candidate
    /// - Candidate may have extra tags (refinement is OK)
    /// - Wildcard (*) in request means any value acceptable
    /// - Wildcard (*) in candidate means candidate can handle any value
    fn cap_tags_dispatchable(&self, request: &CapUrn) -> bool {
        // Every per-key match runs through the six-form truth table
        // (TaggedUrn::values_match). For dispatch, the request is the
        // pattern (declares constraints the candidate must satisfy)
        // and the candidate is the instance. Walk the union of keys
        // so absent-on-candidate cells against present-on-request
        // patterns are evaluated correctly.
        let all_keys: std::collections::HashSet<&String> =
            self.tags.keys().chain(request.tags.keys()).collect();
        for key in all_keys {
            let patt = request.tags.get(key).map(|s| s.as_str());
            let inst = self.tags.get(key).map(|s| s.as_str());
            if !TaggedUrn::values_match(inst, patt) {
                return false;
            }
        }
        true
    }

    /// Apply this cap URN to a concrete runtime input media URN.
    ///
    /// This validates that the runtime input conforms to the cap's declared
    /// input and, if so, returns the concrete runtime output media URN after
    /// the cap's effect semantics are applied. The resulting runtime output
    /// must conform to the declared output or this method fails hard.
    pub fn apply_to_runtime_input_media(
        &self,
        runtime_input: &MediaUrn,
    ) -> Result<MediaUrn, CapUrnError> {
        let declared_in = self.in_media_urn().map_err(|e| {
            CapUrnError::InvalidInSpec(format!(
                "Stored in_urn failed to parse during inference: {}",
                e
            ))
        })?;
        let declared_out = self.out_media_urn().map_err(|e| {
            CapUrnError::InvalidOutSpec(format!(
                "Stored out_urn failed to parse during inference: {}",
                e
            ))
        })?;

        if !runtime_input.conforms_to(&declared_in).map_err(|e| {
            CapUrnError::InvalidEffectApplication(format!(
                "Failed to compare runtime input '{}' against declared input '{}': {}",
                runtime_input, declared_in, e
            ))
        })? {
            return Err(CapUrnError::InvalidEffectApplication(format!(
                "Runtime input '{}' does not conform to declared input '{}'",
                runtime_input, declared_in
            )));
        }

        let runtime_out = match self.effect() {
            CapEffect::Declared => declared_out.clone(),
            CapEffect::None => runtime_input.clone(),
            CapEffect::Patch => {
                let delta = declared_out.delta_from(&declared_in).map_err(|e| {
                    CapUrnError::InvalidEffectApplication(format!(
                        "Failed to derive media delta from '{}' to '{}': {}",
                        declared_in, declared_out, e
                    ))
                })?;
                runtime_input.apply_delta(&delta).map_err(|e| {
                    CapUrnError::InvalidEffectApplication(format!(
                        "Failed to apply media delta to runtime input '{}': {}",
                        runtime_input, e
                    ))
                })?
            }
            CapEffect::Any => {
                return Err(CapUrnError::InvalidEffectApplication(
                    "Cannot infer runtime output for an unconstrained effect request".to_string(),
                ));
            }
        };

        if !runtime_out.conforms_to(&declared_out).map_err(|e| {
            CapUrnError::InvalidEffectApplication(format!(
                "Failed to validate runtime output '{}' against declared output '{}': {}",
                runtime_out, declared_out, e
            ))
        })? {
            return Err(CapUrnError::InvalidEffectApplication(format!(
                "Inferred runtime output '{}' does not conform to declared output '{}'",
                runtime_out, declared_out
            )));
        }

        Ok(runtime_out)
    }

    /// Infer the runtime output media URN for a concrete runtime input.
    ///
    /// Alias for `apply_to_runtime_input_media()`. Kept as the runtime-output
    /// terminology used elsewhere in the stack.
    pub fn infer_runtime_output_media(
        &self,
        runtime_input: &MediaUrn,
    ) -> Result<MediaUrn, CapUrnError> {
        self.apply_to_runtime_input_media(runtime_input)
    }

    /// Does an actually-emitted runtime output satisfy this cap's declared
    /// effect contract for the given runtime input?
    ///
    /// This is THE effect-audit predicate: every check of "did the cap emit
    /// what its effect promised" must go through it — never a hand-rolled
    /// combination of `infer_runtime_output_media` with equality or
    /// conformance checks, so the contract has exactly one definition.
    ///
    /// The condition is deliberately asymmetric per effect:
    ///
    /// - `effect=none` and `effect=patch` compute an EXACT runtime output
    ///   type (the input itself / the input with the declared delta
    ///   applied), so the emission must be tag-equivalent to the inference.
    ///   A more specific emission would still be a lie: the effect promises
    ///   the type is fully determined by the input.
    /// - `effect=declared` promises only the declared `out=`, so an
    ///   emission that is MORE specific than the declaration is legal and
    ///   desirable (e.g. declaring `out=media:record` and emitting
    ///   `media:fmt=json;record`); the emission must `conform_to` the
    ///   declared output. A more generic emission breaks every downstream
    ///   plan refinement and fails.
    ///
    /// Errors (rather than returning `false`) when the inference itself is
    /// impossible: the runtime input does not conform to the declared
    /// input, the effect is the unconstrained request form (`?effect`),
    /// or the URN state is internally inconsistent — those are upstream
    /// contract breaks, not emission mismatches, and must surface as such.
    pub fn is_conformant_runtime_output(
        &self,
        runtime_input: &MediaUrn,
        runtime_output: &MediaUrn,
    ) -> Result<bool, CapUrnError> {
        let inferred = self.apply_to_runtime_input_media(runtime_input)?;
        match self.effect() {
            CapEffect::None | CapEffect::Patch => {
                runtime_output.is_equivalent(&inferred).map_err(|e| {
                    CapUrnError::InvalidEffectApplication(format!(
                        "Failed to compare emitted output '{}' against inferred output '{}': {}",
                        runtime_output, inferred, e
                    ))
                })
            }
            CapEffect::Declared => runtime_output.conforms_to(&inferred).map_err(|e| {
                CapUrnError::InvalidEffectApplication(format!(
                    "Failed to compare emitted output '{}' against declared output '{}': {}",
                    runtime_output, inferred, e
                ))
            }),
            CapEffect::Any => Err(CapUrnError::InvalidEffectApplication(
                "Cannot audit an emission against an unconstrained effect request".to_string(),
            )),
        }
    }

    /// Calculate specificity score for cap matching.
    ///
    /// More specific caps have higher scores and are preferred. The
    /// score is a **weighted sum** of the per-tag truth-table score
    /// across the three axes (`out`, `in`, `y`), each axis scored as
    /// a Tagged URN per [`tagged_urn::TaggedUrn::specificity`]:
    ///
    /// | Stored tag value | Form           | Score |
    /// |------------------|----------------|------:|
    /// | `"?"`            | `?x`           |     0 |
    /// | starts with `?=` | `x?=v`         |     1 |
    /// | `"*"`            | `x` (`x=*`)    |     2 |
    /// | starts with `!=` | `x!=v`         |     3 |
    /// | exact value      | `x=v`          |     4 |
    /// | `"!"`            | `!x`           |     5 |
    ///
    /// Axis weighting:
    ///
    /// ```text
    /// spec_C(c) = 10_000 * spec_U(c.out) + 100 * spec_U(c.in) + spec_U(c.y)
    /// ```
    ///
    /// The lexicographic priority `(out, in, y)` reflects the
    /// routing intent: producing different things is the largest
    /// semantic difference between two caps; consuming different
    /// things is next; descriptive y-axis metadata is last. Two
    /// orders of magnitude separate each axis so per-axis scores up
    /// to ~99 stay in their own digit slot, making the integer both
    /// totally ordered and visually decodable (e.g. `40205` reads as
    /// out=4, in=2, y=5).
    ///
    /// Panics if either direction spec is somehow not a valid media
    /// URN — both fields are validated at construction time, so this
    /// only fires on internally inconsistent state.
    pub fn specificity(&self) -> usize {
        let in_media = MediaUrn::from_string(&self.in_urn).unwrap_or_else(|e| {
            panic!(
                "CU2: in_spec '{}' is not a valid MediaUrn: {}",
                self.in_urn, e
            )
        });
        let out_media = MediaUrn::from_string(&self.out_urn).unwrap_or_else(|e| {
            panic!(
                "CU2: out_spec '{}' is not a valid MediaUrn: {}",
                self.out_urn, e
            )
        });

        let in_score = in_media.inner().specificity();
        let out_score = out_media.inner().specificity();
        let y_score: usize = self.tags.values().map(|v| score_tag_value(v)).sum();

        Self::WEIGHT_OUT * out_score + Self::WEIGHT_IN * in_score + y_score
    }

    /// Per-axis weights for cap-URN specificity. Two orders of
    /// magnitude separate each axis to keep them in distinct digit
    /// slots while still folding into a single comparable integer.
    pub const WEIGHT_OUT: usize = 10_000;
    pub const WEIGHT_IN: usize = 100;

    /// Check if this cap is more specific than another
    ///
    /// Compares specificity scores. Only meaningful when both caps
    /// already matched the same request.
    pub fn is_more_specific_than(&self, other: &CapUrn) -> bool {
        self.specificity() > other.specificity()
    }

    /// Create a wildcard version by replacing specific values with wildcards
    /// For 'in' or 'out', sets the corresponding direction spec to wildcard
    pub fn with_wildcard_tag(mut self, key: &str) -> Self {
        let key_lower = key.to_lowercase();
        match key_lower.as_str() {
            "in" => {
                self.in_urn = MEDIA_IDENTITY.to_string();
            }
            "out" => {
                self.out_urn = MEDIA_IDENTITY.to_string();
            }
            "effect" => {
                self.effect = "?".to_string();
            }
            _ => {
                if self.tags.contains_key(&key_lower) {
                    self.tags.insert(key_lower, "*".to_string());
                }
            }
        }
        self.validate_admissible()
            .expect("CapUrn::with_wildcard_tag produced an illegal cap declaration");
        self
    }

    /// Create a subset cap with only specified tags
    /// Note: 'in' and 'out' are always included as they are required
    pub fn subset(&self, keys: &[&str]) -> Self {
        let mut tags = BTreeMap::new();
        for &key in keys {
            let key_lower = key.to_lowercase();
            // Skip in/out as they're handled separately
            if key_lower == "in" || key_lower == "out" {
                continue;
            }
            if let Some(value) = self.tags.get(&key_lower) {
                tags.insert(key_lower, value.clone());
            }
        }
        let subset = Self {
            in_urn: self.in_urn.clone(),
            out_urn: self.out_urn.clone(),
            effect: self.effect.clone(),
            tags,
        };
        subset
            .validate_admissible()
            .expect("CapUrn::subset produced an illegal cap declaration");
        subset
    }

    /// Merge with another cap (other takes precedence for conflicts)
    /// Direction specs from other override this one's
    pub fn merge(&self, other: &CapUrn) -> Self {
        let mut tags = self.tags.clone();
        for (key, value) in &other.tags {
            tags.insert(key.clone(), value.clone());
        }
        let merged = Self {
            in_urn: other.in_urn.clone(),
            out_urn: other.out_urn.clone(),
            effect: other.effect.clone(),
            tags,
        };
        merged
            .validate_admissible()
            .expect("CapUrn::merge produced an illegal cap declaration");
        merged
    }

    pub fn canonical(cap_urn: &str) -> Result<String, CapUrnError> {
        let cap_urn_deserialized = CapUrn::from_string(cap_urn)?;
        Ok(cap_urn_deserialized.to_string())
    }

    pub fn canonical_option(cap_urn: Option<&str>) -> Result<Option<String>, CapUrnError> {
        if let Some(cu) = cap_urn {
            let cap_urn_deserialized = CapUrn::from_string(cu)?;
            Ok(Some(cap_urn_deserialized.to_string()))
        } else {
            Ok(None)
        }
    }
}

/// Errors that can occur when parsing cap URNs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CapUrnError {
    /// Error code 1: Empty or malformed URN
    Empty,
    /// Error code 5: URN does not start with `cap:`
    MissingCapPrefix,
    /// Error code 4: Tag not in key=value format
    InvalidTagFormat(String),
    /// Error code 2: Empty key or value component
    EmptyTagComponent(String),
    /// Error code 3: Disallowed character in key/value
    InvalidCharacter(String),
    /// Error code 6: Same key appears twice
    DuplicateKey(String),
    /// Error code 7: Key is purely numeric
    NumericKey(String),
    /// Error code 8: Quoted value never closed
    UnterminatedQuote(usize),
    /// Error code 9: Invalid escape in quoted value (only \" and \\ allowed)
    InvalidEscapeSequence(usize),
    /// Error code 10: Missing required 'in' tag - caps must declare their input type
    MissingInSpec,
    /// Error code 11: Missing required 'out' tag - caps must declare their output type
    MissingOutSpec,
    /// Error code 12: Empty value provided (use "*" for wildcard)
    EmptyValue(String),
    /// Error code 13: Invalid media URN in 'in' spec
    InvalidInSpec(String),
    /// Error code 14: Invalid media URN in 'out' spec
    InvalidOutSpec(String),
    /// Error code 15: Invalid or unsupported effect coordinate
    InvalidEffect(String),
    /// Error code 16: Runtime effect inference/evaluation failed
    InvalidEffectApplication(String),
    /// Error code 17: Structurally parseable but inadmissible cap declaration
    IllegalDeclaration(String),
}

impl CapUrnError {
    /// Convert from TaggedUrnError to CapUrnError
    fn from_tagged_urn_error(e: TaggedUrnError) -> Self {
        match e {
            TaggedUrnError::Empty => CapUrnError::Empty,
            TaggedUrnError::MissingPrefix => CapUrnError::MissingCapPrefix,
            TaggedUrnError::EmptyPrefix => CapUrnError::MissingCapPrefix,
            TaggedUrnError::InvalidTagFormat(s) => CapUrnError::InvalidTagFormat(s),
            TaggedUrnError::EmptyTagComponent(s) => CapUrnError::EmptyTagComponent(s),
            TaggedUrnError::InvalidCharacter(s) => CapUrnError::InvalidCharacter(s),
            TaggedUrnError::DuplicateKey(s) => CapUrnError::DuplicateKey(s),
            TaggedUrnError::NumericKey(s) => CapUrnError::NumericKey(s),
            TaggedUrnError::UnterminatedQuote(pos) => CapUrnError::UnterminatedQuote(pos),
            TaggedUrnError::InvalidEscapeSequence(pos) => CapUrnError::InvalidEscapeSequence(pos),
            TaggedUrnError::PrefixMismatch { .. } => CapUrnError::MissingCapPrefix,
            TaggedUrnError::WhitespaceInInput(s) => CapUrnError::InvalidCharacter(s),
        }
    }
}

impl fmt::Display for CapUrnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CapUrnError::Empty => {
                write!(f, "Cap identifier cannot be empty")
            }
            CapUrnError::MissingCapPrefix => {
                write!(f, "Cap identifier must start with 'cap:'")
            }
            CapUrnError::InvalidTagFormat(tag) => {
                write!(f, "Invalid tag format (must be key=value): {}", tag)
            }
            CapUrnError::EmptyTagComponent(tag) => {
                write!(f, "Tag key or value cannot be empty: {}", tag)
            }
            CapUrnError::InvalidCharacter(tag) => {
                write!(f, "Invalid character in tag: {}", tag)
            }
            CapUrnError::DuplicateKey(key) => {
                write!(f, "Duplicate tag key: {}", key)
            }
            CapUrnError::NumericKey(key) => {
                write!(f, "Tag key cannot be purely numeric: {}", key)
            }
            CapUrnError::UnterminatedQuote(pos) => {
                write!(f, "Unterminated quote at position {}", pos)
            }
            CapUrnError::InvalidEscapeSequence(pos) => {
                write!(
                    f,
                    "Invalid escape sequence at position {} (only \\\" and \\\\ allowed)",
                    pos
                )
            }
            CapUrnError::MissingInSpec => {
                write!(f, "Cap URN is missing required 'in' tag - caps must declare their input type (use {} for no input)", MEDIA_VOID)
            }
            CapUrnError::MissingOutSpec => {
                write!(
                    f,
                    "Cap URN is missing required 'out' tag - caps must declare their output type"
                )
            }
            CapUrnError::EmptyValue(key) => {
                write!(f, "Empty value for key '{}' (use '*' for wildcard)", key)
            }
            CapUrnError::InvalidInSpec(msg) => {
                write!(f, "Invalid 'in' spec: {}", msg)
            }
            CapUrnError::InvalidOutSpec(msg) => {
                write!(f, "Invalid 'out' spec: {}", msg)
            }
            CapUrnError::InvalidEffect(msg) => {
                write!(f, "Invalid 'effect' spec: {}", msg)
            }
            CapUrnError::InvalidEffectApplication(msg) => {
                write!(f, "Invalid effect application: {}", msg)
            }
            CapUrnError::IllegalDeclaration(msg) => {
                write!(f, "Illegal cap declaration: {}", msg)
            }
        }
    }
}

impl std::error::Error for CapUrnError {}

impl FromStr for CapUrn {
    type Err = CapUrnError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        CapUrn::from_string(s)
    }
}

impl fmt::Display for CapUrn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

/// Structural total order over `CapUrn`.
///
/// The comparison routes through the parsed `MediaUrn` values
/// of `in` / `out` (which use `TaggedUrn`'s structural
/// `(prefix, tags-BTreeMap)` `Ord`) and then the non-direction
/// `tags` `BTreeMap<String, String>` (whose `Ord` is the
/// natural lexicographic order over canonicalized tag keys and
/// values).
///
/// This explicitly avoids flat-string comparison of the
/// whole canonical form — per the URN rules in
/// `../docs/04-predicates.md`, URNs must never be compared as
/// opaque strings, only via their structural components.
///
/// `CapUrn::new` / `CapUrn::from_string` guarantee that the
/// stored `in_urn` / `out_urn` are valid canonical `MediaUrn`
/// serializations, so the `.expect()` on parse here is a
/// hard-fail on a broken invariant, not a runtime recovery
/// path.
impl Ord for CapUrn {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_in = self
            .in_media_urn()
            .expect("CapUrn invariant: in_urn parses as MediaUrn");
        let other_in = other
            .in_media_urn()
            .expect("CapUrn invariant: in_urn parses as MediaUrn");
        match self_in.cmp(&other_in) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }

        let self_out = self
            .out_media_urn()
            .expect("CapUrn invariant: out_urn parses as MediaUrn");
        let other_out = other
            .out_media_urn()
            .expect("CapUrn invariant: out_urn parses as MediaUrn");
        match self_out.cmp(&other_out) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }

        match self.effect().cmp(&other.effect()) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }

        self.tags.cmp(&other.tags)
    }
}

impl PartialOrd for CapUrn {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Serde serialization support
impl Serialize for CapUrn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for CapUrn {
    fn deserialize<D>(deserializer: D) -> Result<CapUrn, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        CapUrn::from_string(&s).map_err(serde::de::Error::custom)
    }
}

/// Cap matching and selection utilities
pub struct CapMatcher;

impl CapMatcher {
    /// Find the most specific cap that accepts a request
    pub fn find_best_match<'a>(caps: &'a [CapUrn], request: &CapUrn) -> Option<&'a CapUrn> {
        caps.iter()
            .filter(|cap| request.accepts(cap))
            .max_by_key(|cap| cap.specificity())
    }

    /// Find all caps that match a request, sorted by specificity
    pub fn find_all_matches<'a>(caps: &'a [CapUrn], request: &CapUrn) -> Vec<&'a CapUrn> {
        let mut matches: Vec<&CapUrn> = caps.iter().filter(|cap| request.accepts(cap)).collect();

        // Sort by specificity (most specific first)
        matches.sort_by_key(|cap| std::cmp::Reverse(cap.specificity()));
        matches
    }

    /// Check if two cap sets overlap (any pair is comparable)
    pub fn are_compatible(caps1: &[CapUrn], caps2: &[CapUrn]) -> bool {
        caps1
            .iter()
            .any(|c1| caps2.iter().any(|c2| c1.is_comparable(c2)))
    }
}

/// Builder for creating cap URNs fluently
/// Direction specs (in/out) are required and must be set before building
pub struct CapUrnBuilder {
    in_urn: Option<String>,
    out_urn: Option<String>,
    effect: Option<String>,
    tags: BTreeMap<String, String>,
}

impl CapUrnBuilder {
    pub fn new() -> Self {
        Self {
            in_urn: None,
            out_urn: None,
            effect: None,
            tags: BTreeMap::new(),
        }
    }

    /// Set the input media URN (required)
    pub fn in_spec(mut self, spec: &str) -> Self {
        self.in_urn = Some(spec.to_string());
        self
    }

    /// Set the output media URN (required)
    pub fn out_spec(mut self, spec: &str) -> Self {
        self.out_urn = Some(spec.to_string());
        self
    }

    /// Set the effect coordinate.
    pub fn effect(mut self, effect: CapEffect) -> Self {
        self.effect = Some(effect.as_str().to_string());
        self
    }

    /// Add a non-structural tag with key (normalized to lowercase) and value
    /// (preserved as-is).
    pub fn tag(mut self, key: &str, value: &str) -> Self {
        let key_lower = key.to_lowercase();
        if key_lower == "in" || key_lower == "out" || key_lower == "effect" {
            panic!(
                "CapUrnBuilder::tag cannot set reserved structural key '{}'; use in_spec/out_spec/effect",
                key_lower
            );
        }
        self.tags.insert(key_lower, value.to_string());
        self
    }

    pub fn marker(mut self, key: &str) -> Self {
        let key_lower = key.to_lowercase();
        if key_lower == "in" || key_lower == "out" || key_lower == "effect" {
            panic!(
                "CapUrnBuilder::marker cannot set reserved structural key '{}'; use in_spec/out_spec/effect",
                key_lower
            );
        }
        self.tags.insert(key_lower, "*".to_string());
        self
    }

    pub fn build(self) -> Result<CapUrn, CapUrnError> {
        let in_urn = self.in_urn.ok_or(CapUrnError::MissingInSpec)?;
        let out_urn = self.out_urn.ok_or(CapUrnError::MissingOutSpec)?;
        let effect = self.effect.unwrap_or_else(|| "declared".to_string());
        CapUrn::new(in_urn, out_urn, effect, self.tags)
    }
}

impl Default for CapUrnBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // All cap URNs now require in and out specs. Use these helpers:
    fn test_urn(tags: &str) -> String {
        if tags.is_empty() {
            format!("cap:in=\"{}\";out=\"{}\"", MEDIA_VOID, MEDIA_OBJECT)
        } else {
            format!(
                "cap:in=\"{}\";out=\"{}\";{}",
                MEDIA_VOID, MEDIA_OBJECT, tags
            )
        }
    }

    fn test_urn_with_io(in_spec: &str, out_spec: &str, tags: &str) -> String {
        if tags.is_empty() {
            format!("cap:in=\"{}\";out=\"{}\"", in_spec, out_spec)
        } else {
            format!("cap:in=\"{}\";out=\"{}\";{}", in_spec, out_spec, tags)
        }
    }

    // TEST001: Test that cap URN is created with tags parsed correctly and direction specs accessible
    #[test]
    fn test001_cap_urn_creation() {
        let cap = CapUrn::from_string(&test_urn("generate;ext=pdf;target=thumbnail")).unwrap();
        assert!(cap.has_marker_tag("generate"));
        assert_eq!(cap.get_tag("target"), Some(&"thumbnail".to_string()));
        assert_eq!(cap.get_tag("ext"), Some(&"pdf".to_string()));
        // Direction specs are required and accessible
        assert_eq!(cap.in_spec(), MEDIA_VOID);
        assert_eq!(cap.out_spec(), MEDIA_OBJECT);
    }

    // TEST002: Test that missing 'in' or 'out' defaults to media: wildcard
    #[test]
    fn test002_direction_specs_default_to_wildcard() {
        // Missing 'in' defaults to media:
        let cap = CapUrn::from_string(&format!("cap:out=\"{}\";test", MEDIA_OBJECT))
            .expect("Missing in should default to media:");
        assert_eq!(cap.in_spec(), "media:");
        assert_eq!(cap.out_spec(), MEDIA_OBJECT);

        // Missing 'out' defaults to media:
        let cap = CapUrn::from_string(&format!("cap:in=\"{}\";test", MEDIA_VOID))
            .expect("Missing out should default to media:");
        assert_eq!(cap.in_spec(), MEDIA_VOID);
        assert_eq!(cap.out_spec(), "media:");

        // Both present should succeed
        let cap = CapUrn::from_string(&format!(
            "cap:in=\"{}\";out=\"{}\";test",
            MEDIA_VOID, MEDIA_OBJECT
        ))
        .expect("Both specs present should succeed");
        assert_eq!(cap.in_spec(), MEDIA_VOID);
        assert_eq!(cap.out_spec(), MEDIA_OBJECT);
    }

    // TEST003: Test that direction specs must match exactly, different in/out types don't match, wildcard matches any
    #[test]
    fn test003_direction_matching() {
        let in_str = "media:string";
        let out_obj = "media:object";
        let in_bin = "media:binary";
        let out_int = "media:integer";

        // Direction specs must match for caps to match
        let cap1 = CapUrn::from_string(&format!("cap:in=\"{}\";test;out=\"{}\"", in_str, out_obj))
            .unwrap();
        let cap2 = CapUrn::from_string(&format!("cap:in=\"{}\";test;out=\"{}\"", in_str, out_obj))
            .unwrap();
        assert!(cap1.accepts(&cap2));

        // Different in_urn should not match
        let cap3 = CapUrn::from_string(&format!("cap:in=\"{}\";test;out=\"{}\"", in_bin, out_obj))
            .unwrap();
        assert!(!cap1.accepts(&cap3));

        // Different out_urn should not match
        let cap4 = CapUrn::from_string(&format!("cap:in=\"{}\";test;out=\"{}\"", in_str, out_int))
            .unwrap();
        assert!(!cap1.accepts(&cap4));

        // Wildcard in=* direction: cap5 has media: for in, specific for out
        let cap5 = CapUrn::from_string(&format!("cap:in=*;test;out=\"{}\"", out_obj)).unwrap();
        // cap1 (specific in) as pattern rejects cap5 (bare media: in) — specific pattern doesn't accept broad instance
        assert!(!cap1.accepts(&cap5));
        // cap5 (wildcard in) as pattern accepts cap1 (specific in) — wildcard pattern accepts anything
        assert!(cap5.accepts(&cap1));
    }

    // TEST004: Test that unquoted keys and values are normalized to lowercase
    #[test]
    fn test004_unquoted_values_lowercased() {
        // Mixed-case keyed tags + a marker. Both keys and unquoted
        // values are lowercased on parse.
        let cap = CapUrn::from_string(&test_urn("Generate;EXT=PDF;Target=Thumbnail")).unwrap();

        assert!(cap.has_marker_tag("generate"));
        assert_eq!(cap.get_tag("ext"), Some(&"pdf".to_string()));
        assert_eq!(cap.get_tag("target"), Some(&"thumbnail".to_string()));

        // Key lookup is case-insensitive: uppercase variants of an
        // existing key resolve to the same keyed tag.
        assert_eq!(cap.get_tag("EXT"), Some(&"pdf".to_string()));
        assert_eq!(cap.get_tag("Ext"), Some(&"pdf".to_string()));

        // Tag order in the source string does not affect canonical
        // form — parsed URNs sort tags alphabetically before
        // serializing.
        let cap2 = CapUrn::from_string(&test_urn("target=thumbnail;ext=pdf;generate")).unwrap();
        assert_eq!(cap.to_string(), cap2.to_string());
        assert_eq!(cap, cap2);
    }

    // TEST005: Test that quoted values preserve case while unquoted are lowercased
    #[test]
    fn test005_quoted_values_preserve_case() {
        // Quoted values preserve their case
        let cap = CapUrn::from_string(&test_urn(r#"key="Value With Spaces""#)).unwrap();
        assert_eq!(cap.get_tag("key"), Some(&"Value With Spaces".to_string()));

        // Key is still lowercase
        let cap2 = CapUrn::from_string(&test_urn(r#"KEY="Value With Spaces""#)).unwrap();
        assert_eq!(cap2.get_tag("key"), Some(&"Value With Spaces".to_string()));

        // Unquoted vs quoted case difference
        let unquoted = CapUrn::from_string(&test_urn("key=UPPERCASE")).unwrap();
        let quoted = CapUrn::from_string(&test_urn(r#"key="UPPERCASE""#)).unwrap();
        assert_eq!(unquoted.get_tag("key"), Some(&"uppercase".to_string())); // lowercase
        assert_eq!(quoted.get_tag("key"), Some(&"UPPERCASE".to_string())); // preserved
        assert_ne!(unquoted, quoted); // NOT equal
    }

    // TEST006: Test that quoted values can contain special characters (semicolons, equals, spaces)
    #[test]
    fn test006_quoted_value_special_chars() {
        // Semicolons in quoted values
        let cap = CapUrn::from_string(&test_urn(r#"key="value;with;semicolons""#)).unwrap();
        assert_eq!(
            cap.get_tag("key"),
            Some(&"value;with;semicolons".to_string())
        );

        // Equals in quoted values
        let cap2 = CapUrn::from_string(&test_urn(r#"key="value=with=equals""#)).unwrap();
        assert_eq!(cap2.get_tag("key"), Some(&"value=with=equals".to_string()));

        // Spaces in quoted values
        let cap3 = CapUrn::from_string(&test_urn(r#"key="hello world""#)).unwrap();
        assert_eq!(cap3.get_tag("key"), Some(&"hello world".to_string()));
    }

    // TEST007: Test that escape sequences in quoted values (\" and \\) are parsed correctly
    #[test]
    fn test007_quoted_value_escape_sequences() {
        // Escaped quotes
        let cap = CapUrn::from_string(&test_urn(r#"key="value\"quoted\"""#)).unwrap();
        assert_eq!(cap.get_tag("key"), Some(&r#"value"quoted""#.to_string()));

        // Escaped backslashes
        let cap2 = CapUrn::from_string(&test_urn(r#"key="path\\file""#)).unwrap();
        assert_eq!(cap2.get_tag("key"), Some(&r#"path\file"#.to_string()));

        // Mixed escapes
        let cap3 = CapUrn::from_string(&test_urn(r#"key="say \"hello\\world\"""#)).unwrap();
        assert_eq!(
            cap3.get_tag("key"),
            Some(&r#"say "hello\world""#.to_string())
        );
    }

    // TEST008: Test that mixed quoted and unquoted values in same URN parse correctly
    #[test]
    fn test008_mixed_quoted_unquoted() {
        let cap = CapUrn::from_string(&test_urn(r#"a="Quoted";b=simple"#)).unwrap();
        assert_eq!(cap.get_tag("a"), Some(&"Quoted".to_string()));
        assert_eq!(cap.get_tag("b"), Some(&"simple".to_string()));
    }

    // TEST009: Test that unterminated quote produces UnterminatedQuote error
    #[test]
    fn test009_unterminated_quote_error() {
        let result = CapUrn::from_string(&test_urn(r#"key="unterminated"#));
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, CapUrnError::UnterminatedQuote(_)));
        }
    }

    // TEST010: Test that invalid escape sequences (like \n, \x) produce InvalidEscapeSequence error
    #[test]
    fn test010_invalid_escape_sequence_error() {
        let result = CapUrn::from_string(&test_urn(r#"key="bad\n""#));
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, CapUrnError::InvalidEscapeSequence(_)));
        }

        // Invalid escape at end
        let result2 = CapUrn::from_string(&test_urn(r#"key="bad\x""#));
        assert!(result2.is_err());
        if let Err(e) = result2 {
            assert!(matches!(e, CapUrnError::InvalidEscapeSequence(_)));
        }
    }

    // TEST011: Test that serialization uses smart quoting (no quotes for simple lowercase, quotes for special chars/uppercase)
    #[test]
    fn test011_serialization_smart_quoting() {
        // Simple lowercase value - no quoting needed
        let cap = CapUrnBuilder::new()
            .in_spec(MEDIA_VOID)
            .out_spec(MEDIA_OBJECT)
            .tag("key", "simple")
            .build()
            .unwrap();
        // The serialized form should contain key=simple (unquoted)
        let s = cap.to_string();
        assert!(s.contains("key=simple"));

        // Value with spaces - needs quoting
        let cap2 = CapUrnBuilder::new()
            .in_spec(MEDIA_VOID)
            .out_spec(MEDIA_OBJECT)
            .tag("key", "has spaces")
            .build()
            .unwrap();
        let s2 = cap2.to_string();
        assert!(s2.contains(r#"key="has spaces""#));

        // Value with uppercase - needs quoting to preserve
        let cap4 = CapUrnBuilder::new()
            .in_spec(MEDIA_VOID)
            .out_spec(MEDIA_OBJECT)
            .tag("key", "HasUpper")
            .build()
            .unwrap();
        let s4 = cap4.to_string();
        assert!(s4.contains(r#"key="HasUpper""#));
    }

    // TEST012: Test that simple cap URN round-trips (parse -> serialize -> parse equals original)
    #[test]
    fn test012_round_trip_simple() {
        let original = test_urn("generate;ext=pdf");
        let cap = CapUrn::from_string(&original).unwrap();
        let serialized = cap.to_string();
        let reparsed = CapUrn::from_string(&serialized).unwrap();
        assert_eq!(cap, reparsed);
    }

    // TEST013: Test that quoted values round-trip preserving case and spaces
    #[test]
    fn test013_round_trip_quoted() {
        let original = test_urn(r#"key="Value With Spaces""#);
        let cap = CapUrn::from_string(&original).unwrap();
        let serialized = cap.to_string();
        let reparsed = CapUrn::from_string(&serialized).unwrap();
        assert_eq!(cap, reparsed);
        assert_eq!(
            reparsed.get_tag("key"),
            Some(&"Value With Spaces".to_string())
        );
    }

    // TEST014: Test that escape sequences round-trip correctly
    #[test]
    fn test014_round_trip_escapes() {
        let original = test_urn(r#"key="value\"with\\escapes""#);
        let cap = CapUrn::from_string(&original).unwrap();
        assert_eq!(
            cap.get_tag("key"),
            Some(&r#"value"with\escapes"#.to_string())
        );
        let serialized = cap.to_string();
        let reparsed = CapUrn::from_string(&serialized).unwrap();
        assert_eq!(cap, reparsed);
    }

    // TEST015: Test that cap: prefix is required and case-insensitive
    #[test]
    fn test015_cap_prefix_required() {
        // Missing cap: prefix should fail
        assert!(CapUrn::from_string(&format!(
            "in=\"{}\";out=\"{}\";generate",
            MEDIA_VOID, MEDIA_OBJECT
        ))
        .is_err());

        // Valid cap: prefix should work
        let cap = CapUrn::from_string(&test_urn("generate;ext=pdf")).unwrap();
        assert!(cap.has_marker_tag("generate"));

        // Case-insensitive prefix
        let cap2 = CapUrn::from_string(&format!(
            "CAP:in=\"{}\";out=\"{}\";generate",
            MEDIA_VOID, MEDIA_OBJECT
        ))
        .unwrap();
        assert!(cap2.has_marker_tag("generate"));
    }

    // TEST016: Test that trailing semicolon is equivalent (same hash, same string, matches)
    #[test]
    fn test016_trailing_semicolon_equivalence() {
        // Both with and without trailing semicolon should be equivalent
        let cap1 = CapUrn::from_string(&test_urn("generate;ext=pdf")).unwrap();
        let cap2 = CapUrn::from_string(&format!("{};", test_urn("generate;ext=pdf"))).unwrap();

        // They should be equal
        assert_eq!(cap1, cap2);

        // They should have same hash
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher1 = DefaultHasher::new();
        cap1.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        cap2.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        assert_eq!(hash1, hash2);

        // They should have same string representation (canonical form)
        assert_eq!(cap1.to_string(), cap2.to_string());

        // They should match each other
        assert!(cap1.accepts(&cap2));
        assert!(cap2.accepts(&cap1));
    }

    // TEST939: The canonical form drops `in=media:` and `out=media:`
    // segments. Every spelling of "the same cap with wildcard in/out"
    // collapses to one byte-identical canonical string. This is the
    // contract that makes registry lookups work: the cap-publisher
    // hashes `<canonical-urn>` to compute the cache key, and every
    // language port (Rust, Go, Python, JS, ObjC) must agree on the
    // canonical form for cross-language lookups to land on the same
    // key. A regression that emitted the wildcard segments would
    // silently move the published cap to a different SHA-256 bucket,
    // 404'ing every reader that hashes the canonical form.
    #[test]
    fn test939_cap_urn_canonical_form_drops_wildcard_in_out() {
        let canonical = "cap:decimate-sequence";
        let variants = [
            "cap:decimate-sequence",
            "cap:decimate-sequence;in=media:;out=media:",
            "cap:in=media:;out=media:;decimate-sequence",
            "cap:in=media:;decimate-sequence;out=media:",
        ];
        for v in variants {
            let parsed = CapUrn::from_string(v).expect("variant must parse");
            assert_eq!(
                parsed.to_string(),
                canonical,
                "input {:?} canonicalized to {:?}, expected {:?} — wildcard \
                 in/out segments must be elided so the registry SHA-256 key \
                 is stable across input spellings",
                v,
                parsed.to_string(),
                canonical
            );
        }
        let generic_top = CapUrn::from_string("cap:in=media:;out=media:");
        assert!(matches!(
            generic_top,
            Err(CapUrnError::IllegalDeclaration(_))
        ));

        let identity = CapUrn::from_string("cap:effect=none").expect("identity cap must parse");
        assert_eq!(identity.to_string(), "cap:effect=none");
        assert_ne!(identity.to_string(), "cap:");
    }

    // TEST017: Test tag matching: exact match, subset match, wildcard match, value mismatch
    #[test]
    fn test017_tag_matching() {
        let cap = CapUrn::from_string(&test_urn("generate;ext=pdf;target=thumbnail")).unwrap();

        // Exact match — both directions accept
        let request1 = CapUrn::from_string(&test_urn("generate;ext=pdf;target=thumbnail")).unwrap();
        assert!(cap.accepts(&request1));
        assert!(request1.accepts(&cap));

        // Routing direction: request(generate) accepts cap(op,ext,target) — request only needs op
        let request2 = CapUrn::from_string(&test_urn("generate")).unwrap();
        assert!(request2.accepts(&cap));
        // Reverse: cap(op,ext,target) as pattern rejects request missing ext,target
        assert!(!cap.accepts(&request2));

        // Routing direction: request(ext=*) accepts cap(ext=pdf) — wildcard matches specific
        let request3 = CapUrn::from_string(&test_urn("ext=*")).unwrap();
        assert!(request3.accepts(&cap));

        // Conflicting value — neither direction accepts
        let request4 = CapUrn::from_string(&test_urn("extract")).unwrap();
        assert!(!cap.accepts(&request4));
        assert!(!request4.accepts(&cap));
    }

    // TEST018: Test that quoted values with different case do NOT match (case-sensitive)
    #[test]
    fn test018_matching_case_sensitive_values() {
        // Values with different case should NOT match
        let cap1 = CapUrn::from_string(&test_urn(r#"key="Value""#)).unwrap();
        let cap2 = CapUrn::from_string(&test_urn(r#"key="value""#)).unwrap();
        assert!(!cap1.accepts(&cap2));
        assert!(!cap2.accepts(&cap1));

        // Same case should match
        let cap3 = CapUrn::from_string(&test_urn(r#"key="Value""#)).unwrap();
        assert!(cap1.accepts(&cap3));
    }

    // TEST019: Missing tag in instance causes rejection — pattern's tags are constraints
    #[test]
    fn test019_missing_tag_handling() {
        let cap = CapUrn::from_string(&test_urn("generate")).unwrap();
        let request1 = CapUrn::from_string(&test_urn("ext=pdf")).unwrap();

        // cap(op) as pattern: instance(ext) missing op → reject
        assert!(!cap.accepts(&request1));
        // request(ext) as pattern: instance(cap) missing ext → reject
        assert!(!request1.accepts(&cap));

        // Routing: request(op) accepts cap(op,ext) — instance has op → match
        let cap2 = CapUrn::from_string(&test_urn("generate;ext=pdf")).unwrap();
        let request2 = CapUrn::from_string(&test_urn("generate")).unwrap();
        assert!(request2.accepts(&cap2));
        // Reverse: cap(op,ext) as pattern rejects request missing ext
        assert!(!cap2.accepts(&request2));
    }

    // TEST020: Specificity is the sum of per-tag truth-table scores
    // across in/out/y. Marker tags (bare segments like `generate`) are
    // must-have-any (score 2), exact `key=value` tags score 3,
    // missing/`?` score 0, `!` scores 1.
    //
    // test_urn() builds "cap:in=media:void;out=media:record;<tags>"
    // so the directional baseline is:
    //   in:  media:void  -> {void=*}     -> 2
    //   out: media:record -> {record=*}  -> 2
    // Total directional baseline: 4.
    #[test]
    fn test020_specificity() {
        // test_urn() prepends in="media:void" (1 marker, score 2) and
        // out="media:record" (1 marker, score 2). Cap-URN spec is
        // 10_000 * spec_U(out) + 100 * spec_U(in) + spec_U(y).
        let cap1 = CapUrn::from_string(&test_urn("type=general")).unwrap();
        let cap2 = CapUrn::from_string(&test_urn("generate")).unwrap();
        let cap3 = CapUrn::from_string(&test_urn("op;ext=pdf")).unwrap();

        // out=2, in=2, y=4 (type=general exact)
        assert_eq!(cap1.specificity(), 10_000 * 2 + 100 * 2 + 4);
        // out=2, in=2, y=2 (generate marker = must-have-any)
        assert_eq!(cap2.specificity(), 10_000 * 2 + 100 * 2 + 2);
        // out=2, in=2, y=2+4 (op marker, ext=pdf exact)
        assert_eq!(cap3.specificity(), 10_000 * 2 + 100 * 2 + 6);

        // Wildcard in direction normalizes to media: (no tags, score 0).
        let cap4 = CapUrn::from_string(&format!("cap:in=*;out=\"{}\";test", MEDIA_OBJECT)).unwrap();
        // out=2 (record=*), in=0 (media: empty), y=2 (test marker)
        assert_eq!(cap4.specificity(), 10_000 * 2 + 100 * 0 + 2);
    }

    // TEST021: Test builder creates cap URN with correct tags and direction specs
    #[test]
    fn test021_builder() {
        let cap = CapUrnBuilder::new()
            .in_spec(MEDIA_VOID)
            .out_spec(MEDIA_OBJECT)
            .marker("generate")
            .tag("target", "thumbnail")
            .tag("ext", "pdf")
            .build()
            .unwrap();

        assert!(cap.has_marker_tag("generate"));
        assert_eq!(cap.in_spec(), MEDIA_VOID);
        assert_eq!(cap.out_spec(), MEDIA_OBJECT);
    }

    // TEST022: Test builder requires both in_spec and out_spec
    #[test]
    fn test022_builder_requires_direction() {
        // Missing in_spec should fail
        let result = CapUrnBuilder::new()
            .out_spec(MEDIA_OBJECT)
            .tag("op", "test")
            .build();
        assert!(result.is_err());

        // Missing out_spec should fail
        let result = CapUrnBuilder::new()
            .in_spec(MEDIA_VOID)
            .tag("op", "test")
            .build();
        assert!(result.is_err());

        // Both present should succeed
        let result = CapUrnBuilder::new()
            .in_spec(MEDIA_VOID)
            .out_spec(MEDIA_OBJECT)
            .build();
        assert!(result.is_ok());

        let invalid_tag = CapUrnBuilder::new()
            .in_spec(MEDIA_VOID)
            .out_spec(MEDIA_OBJECT)
            .tag("123", "value")
            .build();
        assert!(matches!(invalid_tag, Err(CapUrnError::NumericKey(_))));

        let reserved_tag = std::panic::catch_unwind(|| {
            let _ = CapUrnBuilder::new().tag("in", "media:void");
        });
        assert!(reserved_tag.is_err());

        let reserved_marker = std::panic::catch_unwind(|| {
            let _ = CapUrnBuilder::new().marker("effect");
        });
        assert!(reserved_marker.is_err());
    }

    // TEST023: Test builder lowercases keys but preserves value case
    #[test]
    fn test023_builder_preserves_case() {
        let cap = CapUrnBuilder::new()
            .in_spec(MEDIA_VOID)
            .out_spec(MEDIA_OBJECT)
            .tag("KEY", "ValueWithCase")
            .build()
            .unwrap();

        // Key is lowercase
        assert_eq!(cap.get_tag("key"), Some(&"ValueWithCase".to_string()));
    }

    // TEST024: Directional accepts — pattern's tags are constraints, instance must satisfy
    #[test]
    fn test024_directional_accepts() {
        let cap1 = CapUrn::from_string(&test_urn("generate;ext=pdf")).unwrap();
        let cap2 = CapUrn::from_string(&test_urn("generate;format=*")).unwrap();
        let cap3 = CapUrn::from_string(&test_urn("type=image;extract")).unwrap();

        // cap1(op,ext) as pattern: cap2 missing ext → reject
        assert!(!cap1.accepts(&cap2));
        // cap2(op,format) as pattern: cap1 missing format → reject
        assert!(!cap2.accepts(&cap1));
        // op mismatch: neither direction accepts
        assert!(!cap1.accepts(&cap3));
        assert!(!cap3.accepts(&cap1));

        // Routing: general request(op) accepts specific cap(op,ext) — instance has op
        let cap4 = CapUrn::from_string(&test_urn("generate")).unwrap();
        assert!(cap4.accepts(&cap1)); // cap4 only requires op, cap1 has it
                                      // Reverse: specific cap(op,ext) rejects general request missing ext
        assert!(!cap1.accepts(&cap4));

        // Different direction specs: neither accepts the other
        let cap5 = CapUrn::from_string(&format!(
            "cap:in=\"media:ext=pdf\";out=\"{}\";generate",
            MEDIA_OBJECT
        ))
        .unwrap();
        assert!(!cap1.accepts(&cap5));
        assert!(!cap5.accepts(&cap1));
    }

    // TEST025: Test find_best_match returns most specific matching cap
    #[test]
    fn test025_best_match() {
        let caps = vec![
            CapUrn::from_string(&test_urn("op")).unwrap(),
            CapUrn::from_string(&test_urn("generate")).unwrap(),
            CapUrn::from_string(&test_urn("generate;ext=pdf")).unwrap(),
        ];

        let request = CapUrn::from_string(&test_urn("generate")).unwrap();
        let best = CapMatcher::find_best_match(&caps, &request).unwrap();

        // Most specific cap that accepts the request
        assert_eq!(best.get_tag("ext"), Some(&"pdf".to_string()));
    }

    // TEST026: Test merge combines tags from both caps, subset keeps only specified tags
    #[test]
    fn test026_merge_and_subset() {
        let cap1 = CapUrn::from_string(&test_urn("generate")).unwrap();
        let cap2 = CapUrn::from_string(&format!(
            "cap:in=media:;out=media:integer;ext=pdf;output=binary"
        ))
        .unwrap();

        let merged = cap1.merge(&cap2);
        // Merged takes in/out from cap2
        assert_eq!(merged.in_spec(), "media:");
        assert_eq!(merged.out_spec(), "media:integer");
        // Has tags from both
        assert!(merged.has_marker_tag("generate"));
        assert_eq!(merged.get_tag("ext"), Some(&"pdf".to_string()));

        let subset = merged.subset(&["type", "ext"]);
        // subset keeps in/out from merged
        assert_eq!(subset.in_spec(), "media:");
        assert_eq!(subset.get_tag("ext"), Some(&"pdf".to_string()));
        assert_eq!(subset.get_tag("type"), None);
    }

    // TEST027: Test with_wildcard_tag sets tag to wildcard, including in/out
    #[test]
    fn test027_wildcard_tag() {
        let cap = CapUrn::from_string(&test_urn("ext=pdf")).unwrap();
        let wildcarded = cap.clone().with_wildcard_tag("ext");

        assert_eq!(wildcarded.get_tag("ext"), Some(&"*".to_string()));

        // Test wildcarding in/out
        let wildcard_in = cap.clone().with_wildcard_tag("in");
        assert_eq!(wildcard_in.in_spec(), MEDIA_IDENTITY);

        let wildcard_out = cap.clone().with_wildcard_tag("out");
        assert_eq!(wildcard_out.out_spec(), MEDIA_IDENTITY);
    }

    // TEST28: Test empty cap URN is illegal after effect transition
    #[test]
    fn test28_empty_cap_urn_is_illegal() {
        assert!(matches!(
            CapUrn::from_string("cap:"),
            Err(CapUrnError::IllegalDeclaration(_))
        ));
        assert!(matches!(
            CapUrn::from_string("cap:;"),
            Err(CapUrnError::IllegalDeclaration(_))
        ));
    }

    // TEST029: Test minimal valid cap URN has just in and out, empty tags
    #[test]
    fn test029_minimal_cap_urn() {
        // Minimal valid cap URN has just in and out
        let cap = CapUrn::from_string(&format!(
            "cap:in=\"{}\";out=\"{}\"",
            MEDIA_VOID, MEDIA_OBJECT
        ))
        .unwrap();
        assert_eq!(cap.in_spec(), MEDIA_VOID);
        assert_eq!(cap.out_spec(), MEDIA_OBJECT);
        assert!(cap.tags.is_empty());
    }

    // TEST030: Test extended characters (forward slashes, colons) in tag values
    #[test]
    fn test030_extended_character_support() {
        // Test forward slashes and colons in tag components
        let cap =
            CapUrn::from_string(&test_urn("url=https://example_org/api;path=/some/file")).unwrap();
        assert_eq!(
            cap.get_tag("url"),
            Some(&"https://example_org/api".to_string())
        );
        assert_eq!(cap.get_tag("path"), Some(&"/some/file".to_string()));
    }

    // TEST031: Test wildcard rejected in keys but accepted in values
    #[test]
    fn test031_wildcard_restrictions() {
        // Wildcard should be rejected in keys
        assert!(CapUrn::from_string(&test_urn("*=value")).is_err());

        // Wildcard should be accepted in values
        let cap = CapUrn::from_string(&test_urn("key=*")).unwrap();
        assert_eq!(cap.get_tag("key"), Some(&"*".to_string()));
    }

    // TEST032: Test duplicate keys are rejected with DuplicateKey error
    #[test]
    fn test032_duplicate_key_rejection() {
        let result = CapUrn::from_string(&test_urn("key=value1;key=value2"));
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, CapUrnError::DuplicateKey(_)));
        }
    }

    // TEST033: Test pure numeric keys rejected, mixed alphanumeric allowed, numeric values allowed
    #[test]
    fn test033_numeric_key_restriction() {
        // Pure numeric keys should be rejected
        assert!(CapUrn::from_string(&test_urn("123=value")).is_err());

        // Mixed alphanumeric keys should be allowed
        assert!(CapUrn::from_string(&test_urn("key123=value")).is_ok());
        assert!(CapUrn::from_string(&test_urn("123key=value")).is_ok());

        // Pure numeric values should be allowed
        assert!(CapUrn::from_string(&test_urn("key=123")).is_ok());
    }

    // TEST034: Test empty values are rejected
    #[test]
    fn test034_empty_value_error() {
        assert!(CapUrn::from_string(&test_urn("key=")).is_err());
        assert!(CapUrn::from_string(&test_urn("key=;other=value")).is_err());
    }

    // TEST035: Test has_tag is case-sensitive for values, case-insensitive for keys, works for in/out
    #[test]
    fn test035_has_tag_case_sensitive() {
        let cap = CapUrn::from_string(&test_urn(r#"key="Value""#)).unwrap();

        // Exact case match works
        assert!(cap.has_tag("key", "Value"));

        // Different case does not match
        assert!(!cap.has_tag("key", "value"));
        assert!(!cap.has_tag("key", "VALUE"));

        // Key lookup is case-insensitive
        assert!(cap.has_tag("KEY", "Value"));
        assert!(cap.has_tag("Key", "Value"));

        // has_tag works for in/out
        assert!(cap.has_tag("in", MEDIA_VOID));
        assert!(cap.has_tag("out", MEDIA_OBJECT));
    }

    // TEST036: Test with_tag preserves value case
    #[test]
    fn test036_with_tag_preserves_value() -> Result<(), CapUrnError> {
        let cap = CapUrn::new(
            MEDIA_VOID.to_string(),
            MEDIA_OBJECT.to_string(),
            "declared".to_string(),
            BTreeMap::new(),
        )?
        .with_tag("key".to_string(), "ValueWithCase".to_string())?;
        assert_eq!(cap.get_tag("key"), Some(&"ValueWithCase".to_string()));
        Ok(())
    }

    // TEST037: Test with_tag rejects empty value
    #[test]
    fn test037_with_tag_rejects_empty_value() -> Result<(), CapUrnError> {
        let cap = CapUrn::new(
            MEDIA_VOID.to_string(),
            MEDIA_OBJECT.to_string(),
            "declared".to_string(),
            BTreeMap::new(),
        )?;
        let result = cap.with_tag("key".to_string(), "".to_string());
        assert!(result.is_err());
        Ok(())
    }

    // TEST038: Test semantic equivalence of unquoted and quoted simple lowercase values
    #[test]
    fn test038_semantic_equivalence() {
        // Unquoted and quoted simple lowercase values are equivalent
        let unquoted = CapUrn::from_string(&test_urn("key=simple")).unwrap();
        let quoted = CapUrn::from_string(&test_urn(r#"key="simple""#)).unwrap();
        assert_eq!(unquoted, quoted);

        // Both serialize the same way (unquoted for simple values)
        assert!(unquoted.to_string().contains("key=simple"));
        assert!(quoted.to_string().contains("key=simple"));
    }

    // TEST039: Test get_tag returns direction specs (in/out) with case-insensitive lookup
    #[test]
    fn test039_get_tag_returns_direction_specs() {
        let in_str = "media:string";
        let out_int = "media:integer";
        let cap = CapUrn::from_string(&format!("cap:in=\"{}\";test;out=\"{}\"", in_str, out_int))
            .unwrap();

        // get_tag works for in/out
        assert_eq!(cap.get_tag("in"), Some(&in_str.to_string()));
        assert_eq!(cap.get_tag("out"), Some(&out_int.to_string()));
        assert!(cap.has_marker_tag("test"));

        // Case-insensitive lookup for in/out
        assert_eq!(cap.get_tag("IN"), Some(&in_str.to_string()));
        assert_eq!(cap.get_tag("OUT"), Some(&out_int.to_string()));
    }

    // ============================================================================
    // MATCHING SEMANTICS SPECIFICATION TESTS
    // These tests verify the exact matching semantics
    // All implementations (Rust, Go, JS, ObjC) must pass these identically
    // Note: All tests now require in/out direction specs using media URNs
    // ============================================================================

    // TEST040: Matching semantics - exact match succeeds
    #[test]
    fn test040_matching_semantics_test1_exact_match() {
        // Test 1: Exact match
        let cap = CapUrn::from_string(&test_urn("generate;ext=pdf")).unwrap();
        let request = CapUrn::from_string(&test_urn("generate;ext=pdf")).unwrap();
        assert!(cap.accepts(&request), "Test 1: Exact match should succeed");
    }

    // TEST041: Matching semantics - cap missing tag matches (implicit wildcard)
    #[test]
    fn test041_matching_semantics_test2_cap_missing_tag() {
        // Test 2: Cap missing tag (implicit wildcard for other tags, not direction)
        let cap = CapUrn::from_string(&test_urn("generate")).unwrap();
        let request = CapUrn::from_string(&test_urn("generate;ext=pdf")).unwrap();
        assert!(
            cap.accepts(&request),
            "Test 2: Cap missing tag should match (implicit wildcard)"
        );
    }

    // TEST042: Pattern rejects instance missing required tags
    #[test]
    fn test042_matching_semantics_test3_cap_has_extra_tag() {
        let cap = CapUrn::from_string(&test_urn("generate;ext=pdf;version=2")).unwrap();
        let request = CapUrn::from_string(&test_urn("generate;ext=pdf")).unwrap();
        // cap(op,ext,version) as pattern rejects request missing version
        assert!(
            !cap.accepts(&request),
            "Pattern rejects instance missing required tag"
        );
        // Routing: request(op,ext) accepts cap(op,ext,version) — instance has all request needs
        assert!(
            request.accepts(&cap),
            "Request pattern satisfied by more-specific cap"
        );
    }

    // TEST043: Matching semantics - request wildcard matches specific cap value
    #[test]
    fn test043_matching_semantics_test4_request_has_wildcard() {
        // Test 4: Request has wildcard
        let cap = CapUrn::from_string(&test_urn("generate;ext=pdf")).unwrap();
        let request = CapUrn::from_string(&test_urn("generate;ext=*")).unwrap();
        assert!(
            cap.accepts(&request),
            "Test 4: Request wildcard should match"
        );
    }

    // TEST044: Matching semantics - cap wildcard matches specific request value
    #[test]
    fn test044_matching_semantics_test5_cap_has_wildcard() {
        // Test 5: Cap has wildcard
        let cap = CapUrn::from_string(&test_urn("generate;ext=*")).unwrap();
        let request = CapUrn::from_string(&test_urn("generate;ext=pdf")).unwrap();
        assert!(cap.accepts(&request), "Test 5: Cap wildcard should match");
    }

    // TEST045: Matching semantics - value mismatch does not match
    #[test]
    fn test045_matching_semantics_test6_value_mismatch() {
        // Test 6: Value mismatch
        let cap = CapUrn::from_string(&test_urn("generate;ext=pdf")).unwrap();
        let request = CapUrn::from_string(&test_urn("generate;ext=docx")).unwrap();
        assert!(
            !cap.accepts(&request),
            "Test 6: Value mismatch should not match"
        );
    }

    // TEST046: Matching semantics - fallback pattern (cap missing tag = implicit wildcard)
    #[test]
    fn test046_matching_semantics_test7_fallback_pattern() {
        // Test 7: Fallback pattern
        let in_bin = "media:binary";
        let cap = CapUrn::from_string(&format!(
            "cap:in=\"{}\";generate_thumbnail;out=\"{}\"",
            in_bin, in_bin
        ))
        .unwrap();
        let request = CapUrn::from_string(&format!(
            "cap:ext=wav;in=\"{}\";generate_thumbnail;out=\"{}\"",
            in_bin, in_bin
        ))
        .unwrap();
        assert!(
            cap.accepts(&request),
            "Test 7: Fallback pattern should match (cap missing ext = implicit wildcard)"
        );
    }

    // TEST047: Matching semantics - thumbnail fallback with void input
    #[test]
    fn test047_matching_semantics_test7b_thumbnail_void_input() {
        // Test 7b: Thumbnail fallback with void input (real-world scenario)
        let out_bin = "media:binary";
        let cap = CapUrn::from_string(&format!(
            "cap:in=\"{}\";generate_thumbnail;out=\"{}\"",
            MEDIA_VOID, out_bin
        ))
        .unwrap();
        let request = CapUrn::from_string(&format!(
            "cap:ext=wav;in=\"{}\";generate_thumbnail;out=\"{}\"",
            MEDIA_VOID, out_bin
        ))
        .unwrap();
        assert!(
            cap.accepts(&request),
            "Test 7b: Thumbnail fallback with void input should match"
        );
    }

    // TEST048: Matching semantics - wildcard direction matches anything
    #[test]
    fn test048_matching_semantics_test8_wildcard_direction_matches_anything() {
        let cap = CapUrn::from_string("cap:generate").unwrap();
        let request = CapUrn::from_string(&format!(
            "cap:ext=pdf;in=media:string;generate;out=\"{}\"",
            MEDIA_OBJECT
        ))
        .unwrap();
        assert!(
            cap.accepts(&request),
            "Test 8: Generic declared directions should accept a more specific matching request"
        );
    }

    // TEST049: Non-overlapping tags — neither direction accepts
    #[test]
    fn test049_matching_semantics_test9_cross_dimension_independence() {
        let cap = CapUrn::from_string(&test_urn("generate")).unwrap();
        let request = CapUrn::from_string(&test_urn("ext=pdf")).unwrap();
        // cap(op) rejects request missing op; request(ext) rejects cap missing ext
        assert!(
            !cap.accepts(&request),
            "Pattern rejects instance missing required tag"
        );
        assert!(
            !request.accepts(&cap),
            "Reverse also rejects — non-overlapping tags"
        );
    }

    // TEST050: Matching semantics - direction mismatch prevents matching
    #[test]
    fn test050_matching_semantics_test10_direction_mismatch() {
        // Test 10: Direction mismatch prevents matching
        // media:string has tags {enc=utf-8, form:scalar}, media: has no tags (wildcard)
        // Neither can provide input for the other (completely different marker tags)
        let cap = CapUrn::from_string(&format!(
            "cap:in=media:string;generate;out=\"{}\"",
            MEDIA_OBJECT
        ))
        .unwrap();
        let request =
            CapUrn::from_string(&format!("cap:in=media:;generate;out=\"{}\"", MEDIA_OBJECT))
                .unwrap();
        assert!(
            !cap.accepts(&request),
            "Test 10: Direction mismatch should not match"
        );
    }

    // TEST890: Semantic direction matching - generic candidate matches specific request
    #[test]
    fn test890_direction_semantic_matching() {
        // A cap accepting media: (generic wildcard) should match a request with media:ext=pdf (specific)
        // because media:ext=pdf has the media: wildcard pattern (accepts everything)
        let generic_cap = CapUrn::from_string(
            "cap:generate_thumbnail;in=media:;out=\"media:ext=png;image;thumbnail\"",
        )
        .unwrap();
        let pdf_request = CapUrn::from_string(
            "cap:generate_thumbnail;in=\"media:ext=pdf\";out=\"media:ext=png;image;thumbnail\"",
        )
        .unwrap();
        assert!(
            generic_cap.accepts(&pdf_request),
            "Generic wildcard candidate must match specific pdf request"
        );

        // Generic cap also matches epub (any media subtype)
        let epub_request = CapUrn::from_string(
            "cap:generate_thumbnail;in=media:epub;out=\"media:ext=png;image;thumbnail\"",
        )
        .unwrap();
        assert!(
            generic_cap.accepts(&epub_request),
            "Generic wildcard candidate must match epub request"
        );

        // Reverse: specific cap does NOT match generic request
        // A pdf-only handler cannot accept arbitrary bytes
        let pdf_cap = CapUrn::from_string(
            "cap:generate_thumbnail;in=\"media:ext=pdf\";out=\"media:ext=png;image;thumbnail\"",
        )
        .unwrap();
        let generic_request = CapUrn::from_string(
            "cap:generate_thumbnail;in=media:;out=\"media:ext=png;image;thumbnail\"",
        )
        .unwrap();
        assert!(
            !pdf_cap.accepts(&generic_request),
            "Specific pdf cap must NOT match generic wildcard request"
        );

        // Incompatible types: pdf cap does NOT match epub request
        assert!(
            !pdf_cap.accepts(&epub_request),
            "PDF-specific cap must NOT match epub request (epub lacks pdf marker)"
        );

        // Output direction: cap producing more specific output matches less specific request
        let specific_out_cap = CapUrn::from_string(
            "cap:generate_thumbnail;in=media:;out=\"media:ext=png;image;thumbnail\"",
        )
        .unwrap();
        let generic_out_request =
            CapUrn::from_string("cap:generate_thumbnail;in=media:;out=media:image").unwrap();
        assert!(
            specific_out_cap.accepts(&generic_out_request),
            "Cap producing image;png;thumbnail must satisfy request for image"
        );

        // Reverse output: generic output cap does NOT match specific output request
        let generic_out_cap =
            CapUrn::from_string("cap:generate_thumbnail;in=media:;out=media:image").unwrap();
        let specific_out_request = CapUrn::from_string(
            "cap:generate_thumbnail;in=media:;out=\"media:ext=png;image;thumbnail\"",
        )
        .unwrap();
        assert!(
            !generic_out_cap.accepts(&specific_out_request),
            "Cap producing generic image must NOT satisfy request requiring image;png;thumbnail"
        );
    }

    // TEST891: Semantic direction specificity — more constraints in
    // either axis means a higher score under the truth-table-driven
    // sum. media: (top, no tags) scores 0; each marker tag scores 2;
    // each exact tag scores 3.
    #[test]
    fn test891_direction_semantic_specificity() {
        let generic_cap = CapUrn::from_string(
            "cap:generate_thumbnail;in=media:;out=\"media:ext=png;image;thumbnail\"",
        )
        .unwrap();
        let specific_cap = CapUrn::from_string(
            "cap:generate_thumbnail;in=\"media:ext=pdf\";out=\"media:ext=png;image;thumbnail\"",
        )
        .unwrap();

        // generic:
        //   out=media:ext=png;image;thumbnail -> 4 (ext=png exact-value) + 2 + 2 = 8
        //   in=media:                     -> 0
        //   y: generate_thumbnail marker  -> 2
        //   spec_C = 10_000*8 + 100*0 + 2 = 80002
        assert_eq!(generic_cap.specificity(), 10_000 * 8 + 100 * 0 + 2);
        // specific:
        //   out=media:ext=png;image;thumbnail -> 8
        //   in=media:ext=pdf              -> 4 (ext=pdf is an exact-value tag, not a bare marker)
        //   y: generate_thumbnail marker  -> 2
        //   spec_C = 10_000*8 + 100*4 + 2 = 80402
        assert_eq!(specific_cap.specificity(), 10_000 * 8 + 100 * 4 + 2);

        assert!(
            specific_cap.specificity() > generic_cap.specificity(),
            "pdf cap must be more specific than wildcard cap"
        );

        // CapMatcher should prefer the more specific cap when both match
        let pdf_request = CapUrn::from_string(
            "cap:generate_thumbnail;in=\"media:ext=pdf\";out=\"media:ext=png;image;thumbnail\"",
        )
        .unwrap();
        let caps = vec![generic_cap.clone(), specific_cap.clone()];
        let best = CapMatcher::find_best_match(&caps, &pdf_request).unwrap();
        assert_eq!(
            best.in_spec(),
            "media:ext=pdf",
            "CapMatcher must prefer the more specific pdf candidate"
        );
    }
}

// TEST639: bare/default top-to-top declared form is illegal
#[test]
fn test639_wildcard_001_empty_cap_is_illegal() {
    assert!(matches!(
        CapUrn::from_string("cap:"),
        Err(CapUrnError::IllegalDeclaration(_))
    ));
}

// TEST640: cap:in defaults to the same illegal bare top form
#[test]
fn test640_wildcard_002_in_only_is_illegal() {
    assert!(matches!(
        CapUrn::from_string("cap:in"),
        Err(CapUrnError::IllegalDeclaration(_))
    ));
}

// TEST641: cap:out defaults to the same illegal bare top form
#[test]
fn test641_wildcard_003_out_only_is_illegal() {
    assert!(matches!(
        CapUrn::from_string("cap:out"),
        Err(CapUrnError::IllegalDeclaration(_))
    ));
}

// TEST642: cap:in;out becomes the same illegal bare top form
#[test]
fn test642_wildcard_004_in_out_no_values_is_illegal() {
    assert!(matches!(
        CapUrn::from_string("cap:in;out"),
        Err(CapUrnError::IllegalDeclaration(_))
    ));
}

// TEST643: cap:in=*;out=* is the same illegal bare top form
#[test]
fn test643_wildcard_005_explicit_asterisk_is_illegal() {
    assert!(matches!(
        CapUrn::from_string("cap:in=*;out=*"),
        Err(CapUrnError::IllegalDeclaration(_))
    ));
}

// TEST644: cap:in=media:;out=* is the same illegal bare top form
#[test]
fn test644_wildcard_006_specific_in_wildcard_out_is_illegal() {
    assert!(matches!(
        CapUrn::from_string("cap:in=media:;out=*"),
        Err(CapUrnError::IllegalDeclaration(_))
    ));
}

// TEST645: cap:in=*;out=media:text has wildcard in, specific out
#[test]
fn test645_wildcard_007_wildcard_in_specific_out() {
    let cap =
        CapUrn::from_string("cap:in=*;out=media:text").expect("Wildcard in with specific out");
    assert_eq!(cap.in_spec(), "media:");
    assert_eq!(cap.out_spec(), "media:text");
}

// TEST646: cap:in=foo fails (invalid media URN)
#[test]
fn test646_wildcard_008_invalid_in_spec_fails() {
    let result = CapUrn::from_string("cap:in=foo;out=media:");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, CapUrnError::InvalidInSpec(_)));
}

// TEST647: cap:in=media:;out=bar fails (invalid media URN)
#[test]
fn test647_wildcard_009_invalid_out_spec_fails() {
    let result = CapUrn::from_string("cap:in=media:;out=bar");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, CapUrnError::InvalidOutSpec(_)));
}

// TEST648: Wildcard in/out match specific caps
#[test]
fn test648_wildcard_010_wildcard_accepts_specific() {
    let wildcard = CapUrn::from_string("cap:raw").unwrap();
    let specific = CapUrn::from_string("cap:out=media:text;raw").unwrap();

    assert!(
        wildcard.accepts(&specific),
        "Wildcard should accept specific cap"
    );
    assert!(
        specific.conforms_to(&wildcard),
        "Specific should conform to wildcard"
    );
}

// TEST649: Specificity - wildcard has 0, specific has tag count
#[test]
fn test649_wildcard_011_specificity_scoring() {
    let wildcard = CapUrn::from_string("cap:raw").unwrap();
    let specific = CapUrn::from_string("cap:out=media:text;raw").unwrap();

    assert_eq!(
        wildcard.specificity(),
        2,
        "Marker-only wildcard cap should have y-axis specificity only"
    );
    assert!(
        specific.specificity() > 0,
        "Specific cap should have non-zero specificity"
    );
}

// TEST650: cap:in=media:;out=media:;test preserves other tags
#[test]
fn test650_wildcard_012_preserve_other_tags() {
    let cap = CapUrn::from_string("cap:in=media:;out=media:;test").unwrap();
    assert_eq!(cap.in_spec(), "media:");
    assert_eq!(cap.out_spec(), "media:");
    assert!(cap.has_marker_tag("test"));
}

// TEST651: All bare/default top-to-top forms are rejected
#[test]
fn test651_wildcard_013_generic_forms_rejected() {
    let forms = [
        "cap:",
        "cap:in;out",
        "cap:in=*;out=*",
        "cap:in=media:;out=media:",
        "cap:in;out=media:",
        "cap:in=*;out=media:",
        "cap:in=media:;out",
        "cap:in=media:;out=*",
    ];
    for form in &forms {
        assert!(matches!(
            CapUrn::from_string(form),
            Err(CapUrnError::IllegalDeclaration(_))
        ));
    }
}

// TEST652: CAP_IDENTITY constant matches identity caps regardless of string form
#[test]
fn test652_wildcard_014_cap_identity_constant_works() {
    use crate::standard::caps::CAP_IDENTITY;
    let identity = CapUrn::from_string(CAP_IDENTITY).unwrap();
    assert_eq!(identity.to_string(), "cap:effect=none");
    assert_eq!(identity.kind().unwrap(), CapKind::Identity);

    // Identity accepts itself
    assert!(identity.accepts(&identity));

    // Identity parsed from different string forms is equivalent
    let long_form = CapUrn::from_string("cap:in=media:;out=media:;effect=none").unwrap();
    assert!(identity.accepts(&long_form));
    assert!(long_form.accepts(&identity));

    assert!(matches!(
        CapUrn::from_string("cap:"),
        Err(CapUrnError::IllegalDeclaration(_))
    ));
}

// TEST653: invalid effect=none declarations fail at construction
#[test]
fn test653_effect_none_illegal_declaration_rejected() {
    assert!(matches!(
        CapUrn::from_string("cap:in=\"media:ext=pdf\";out=\"media:enc=utf-8\";effect=none"),
        Err(CapUrnError::IllegalDeclaration(_))
    ));
}

// TEST0125: effect=none preserves runtime media identity
#[test]
fn test0125_effect_none_preserves_runtime_media() {
    let decimate = CapUrn::from_string("cap:decimate-sequence;effect=none").unwrap();
    let png = MediaUrn::from_string("media:ext=png;image").unwrap();
    let pdf = MediaUrn::from_string("media:ext=pdf").unwrap();
    assert_eq!(
        decimate
            .infer_runtime_output_media(&png)
            .unwrap()
            .to_string(),
        png.to_string()
    );
    assert_eq!(
        decimate
            .infer_runtime_output_media(&pdf)
            .unwrap()
            .to_string(),
        pdf.to_string()
    );
}

// TEST0126: default effect=declared uses the declared output
#[test]
fn test0126_effect_declared_uses_declared_output() {
    let resize = CapUrn::from_string("cap:in=media:image;out=media:image;resize").unwrap();
    let png = MediaUrn::from_string("media:ext=png;image;width=4000").unwrap();
    assert_eq!(
        resize.infer_runtime_output_media(&png).unwrap().to_string(),
        "media:image"
    );
}

// TEST0127: invalid effect=none declarations fail hard
#[test]
fn test0127_invalid_effect_none_fails_hard() {
    assert!(matches!(
        CapUrn::from_string("cap:in=\"media:ext=pdf\";out=\"media:enc=utf-8\";effect=none"),
        Err(CapUrnError::IllegalDeclaration(_))
    ));
}

// TEST0128: omitted effect means declared; unconstrained effect must be explicit
#[test]
fn test0128_effect_dispatch_requires_explicit_wildcard() {
    let none_candidate = CapUrn::from_string("cap:effect=none").unwrap();
    let declared_request = CapUrn::from_string("cap:raw").unwrap();
    let any_request = CapUrn::from_string("cap:?effect").unwrap();
    assert!(!none_candidate.is_dispatchable(&declared_request));
    assert!(none_candidate.is_dispatchable(&any_request));
}

// TEST8121: is_conformant_runtime_output — effect=declared accepts a more
// specific emission, rejects a more generic one
#[test]
fn test8121_effect_conformance_declared_asymmetry() {
    let cap = CapUrn::from_string("cap:extract;in=\"media:ext=pdf\";out=\"media:record\"").unwrap();
    let input = MediaUrn::from_string("media:ext=pdf").unwrap();

    // Emitting exactly the declared out is conformant.
    let declared = MediaUrn::from_string("media:record").unwrap();
    assert!(cap.is_conformant_runtime_output(&input, &declared).unwrap());

    // Emitting something MORE specific than the declared out is conformant —
    // the declaration is a lower bound, not an exact type.
    let more_specific = MediaUrn::from_string("media:fmt=json;record").unwrap();
    assert!(cap
        .is_conformant_runtime_output(&input, &more_specific)
        .unwrap());

    // Emitting something more GENERIC than the declared out is a contract
    // violation: downstream plan refinement relied on at least `record`.
    let more_generic = MediaUrn::from_string("media:").unwrap();
    assert!(!cap
        .is_conformant_runtime_output(&input, &more_generic)
        .unwrap());

    // Emitting an unrelated type is a violation.
    let unrelated = MediaUrn::from_string("media:ext=png;image").unwrap();
    assert!(!cap.is_conformant_runtime_output(&input, &unrelated).unwrap());
}

// TEST8122: is_conformant_runtime_output — effect=none requires the emission
// to be tag-equivalent to the runtime input; MORE specific is still a lie
#[test]
fn test8122_effect_conformance_none_requires_equivalence() {
    let cap = CapUrn::from_string("cap:decimate-sequence;effect=none").unwrap();
    let input = MediaUrn::from_string("media:ext=png;image").unwrap();

    // Exactly the runtime input type: conformant.
    assert!(cap.is_conformant_runtime_output(&input, &input).unwrap());

    // A MORE specific emission is nonconformant: effect=none promises the
    // output type IS the input type, fully determined — not merely bounded.
    let more_specific = MediaUrn::from_string("media:ext=png;image;width=64").unwrap();
    assert!(!cap
        .is_conformant_runtime_output(&input, &more_specific)
        .unwrap());

    // A more generic emission is nonconformant.
    let more_generic = MediaUrn::from_string("media:image").unwrap();
    assert!(!cap
        .is_conformant_runtime_output(&input, &more_generic)
        .unwrap());

    // A runtime input that does not conform to the declared in= is an
    // upstream contract break, surfaced as an error — not as `false`.
    let strict = CapUrn::from_string("cap:effect=none;in=\"media:image\";out=\"media:image\"")
        .unwrap();
    let non_image = MediaUrn::from_string("media:ext=pdf").unwrap();
    assert!(strict
        .is_conformant_runtime_output(&non_image, &non_image)
        .is_err());
}

// TEST8123: is_conformant_runtime_output — effect=patch requires exactly the
// delta-patched input type
#[test]
fn test8123_effect_conformance_patch_requires_patched_input() {
    let cap = CapUrn::from_string(
        "cap:convert;effect=patch;in=\"media:ext=jpeg;image\";out=\"media:ext=png;image\"",
    )
    .unwrap();
    let input = MediaUrn::from_string("media:ext=jpeg;image;width=64").unwrap();

    // The patched input (ext replaced, other tags preserved) is conformant.
    let patched = MediaUrn::from_string("media:ext=png;image;width=64").unwrap();
    assert!(cap.is_conformant_runtime_output(&input, &patched).unwrap());

    // Emitting the bare declared out= drops the preserved width tag — the
    // patch effect promised it survives, so this is a violation.
    let bare_declared = MediaUrn::from_string("media:ext=png;image").unwrap();
    assert!(!cap
        .is_conformant_runtime_output(&input, &bare_declared)
        .unwrap());

    // Emitting the unpatched input type (ext still jpeg) is a violation.
    assert!(!cap.is_conformant_runtime_output(&input, &input).unwrap());
}

#[cfg(test)]
mod tier_tests {
    use super::*;
    use crate::urn::media_urn::MEDIA_VOID;

    // TEST559: without_tag removes tag, rejects structural keys, case-insensitive for keys
    #[test]
    fn test559_without_tag() {
        let cap = CapUrn::from_string(r#"cap:ext=pdf;in=media:void;out=media:void;test"#).unwrap();
        let removed = cap.clone().without_tag("ext");
        assert_eq!(removed.get_tag("ext"), None);
        assert!(removed.has_marker_tag("test"));

        // Case-insensitive removal
        let removed2 = cap.clone().without_tag("EXT");
        assert_eq!(removed2.get_tag("ext"), None);

        // Removing structural coordinates fails hard
        let remove_in = std::panic::catch_unwind(|| cap.clone().without_tag("in"));
        assert!(remove_in.is_err());
        let remove_out = std::panic::catch_unwind(|| cap.clone().without_tag("out"));
        assert!(remove_out.is_err());
        let remove_effect = std::panic::catch_unwind(|| cap.clone().without_tag("effect"));
        assert!(remove_effect.is_err());

        // Removing non-existent tag is no-op
        let same3 = cap.clone().without_tag("nonexistent");
        assert_eq!(same3, cap);
    }

    // TEST560: with_in_spec and with_out_spec change direction specs
    #[test]
    fn test560_with_in_out_spec() {
        let cap = CapUrn::from_string(r#"cap:in=media:void;out=media:void;test"#).unwrap();

        let changed_in = cap.clone().with_in_spec("media:".to_string());
        assert_eq!(changed_in.in_spec(), "media:");
        assert_eq!(changed_in.out_spec(), MEDIA_VOID);
        assert!(changed_in.has_marker_tag("test"));

        let changed_out = cap.clone().with_out_spec("media:string".to_string());
        assert_eq!(changed_out.in_spec(), MEDIA_VOID);
        assert_eq!(changed_out.out_spec(), "media:string");

        // Chain both
        let changed_both = cap
            .with_in_spec("media:ext=pdf".to_string())
            .with_out_spec("media:enc=utf-8;ext=txt".to_string());
        assert_eq!(changed_both.in_spec(), "media:ext=pdf");
        assert_eq!(changed_both.out_spec(), "media:enc=utf-8;ext=txt");

        let identity = CapUrn::from_string("cap:effect=none").unwrap();
        let illegal =
            std::panic::catch_unwind(|| identity.with_out_spec("media:ext=pdf".to_string()));
        assert!(
            illegal.is_err(),
            "with_out_spec must revalidate admissibility"
        );
    }

    // TEST561: in_media_urn and out_media_urn parse direction specs into MediaUrn
    #[test]
    fn test561_in_out_media_urn() {
        let cap =
            CapUrn::from_string(r#"cap:extract;in="media:ext=pdf";out="media:enc=utf-8;ext=txt""#)
                .unwrap();

        let in_urn = cap
            .in_media_urn()
            .expect("in_spec should parse as MediaUrn");
        // pdf file input declares no text encoding (no enc= tag).
        assert!(in_urn.get_tag("enc").is_none());
        assert!(in_urn.has_tag("ext", "pdf"));

        let out_urn = cap
            .out_media_urn()
            .expect("out_spec should parse as MediaUrn");
        // digitized-text output is a UTF-8 text file (enc=utf-8;ext=txt).
        assert!(out_urn.get_tag("enc").is_some());
        assert!(out_urn.has_tag("ext", "txt"));

        // Wildcard media: fails to parse (no tags, just prefix)
        let wildcard_cap = CapUrn::from_string("cap:raw").unwrap();
        // "media:" is valid but has no tags
        let wildcard_in = wildcard_cap.in_media_urn();
        assert!(
            wildcard_in.is_ok(),
            "bare media: should parse as valid MediaUrn"
        );
    }

    // TEST562: canonical_option returns None for None input, canonical string for Some
    #[test]
    fn test562_canonical_option() {
        // None input → Ok(None)
        let result = CapUrn::canonical_option(None).unwrap();
        assert_eq!(result, None);

        // Some valid input → Ok(Some(canonical))
        let input = r#"cap:in=media:void;out=media:void;test"#;
        let result = CapUrn::canonical_option(Some(input)).unwrap();
        assert!(result.is_some());
        let canonical = result.unwrap();
        // Parse both and verify they represent the same cap
        let original = CapUrn::from_string(input).unwrap();
        let reparsed = CapUrn::from_string(&canonical).unwrap();
        assert_eq!(original, reparsed);

        // Some invalid input → Err
        let result = CapUrn::canonical_option(Some("invalid"));
        assert!(result.is_err());
    }

    // TEST563: CapMatcher::find_all_matches returns all matching caps sorted by specificity
    #[test]
    fn test563_find_all_matches() {
        let caps = vec![
            CapUrn::from_string(r#"cap:in=media:void;out=media:void;test"#).unwrap(),
            CapUrn::from_string(r#"cap:ext=pdf;in=media:void;out=media:void;test"#).unwrap(),
            CapUrn::from_string(r#"cap:different;in=media:void;out=media:void"#).unwrap(),
        ];

        let request = CapUrn::from_string(r#"cap:in=media:void;out=media:void;test"#).unwrap();
        let matches = CapMatcher::find_all_matches(&caps, &request);

        // Should find 2 matches (test and test;ext=pdf), not different
        assert_eq!(matches.len(), 2);
        // Sorted by specificity descending: ext=pdf first (more specific)
        assert!(matches[0].specificity() >= matches[1].specificity());
        assert_eq!(matches[0].get_tag("ext"), Some(&"pdf".to_string()));
    }

    // TEST564: CapMatcher::are_compatible detects bidirectional overlap
    #[test]
    fn test564_are_compatible() {
        let caps1 = vec![CapUrn::from_string(r#"cap:in=media:void;out=media:void;test"#).unwrap()];
        let caps2 =
            vec![CapUrn::from_string(r#"cap:ext=pdf;in=media:void;out=media:void;test"#).unwrap()];
        let caps3 =
            vec![CapUrn::from_string(r#"cap:different;in=media:void;out=media:void"#).unwrap()];

        // caps1 (test) accepts caps2 (test;ext=pdf) → compatible
        assert!(CapMatcher::are_compatible(&caps1, &caps2));

        // caps1 (test) vs caps3 (different) → not compatible
        assert!(!CapMatcher::are_compatible(&caps1, &caps3));

        // Empty sets are not compatible
        assert!(!CapMatcher::are_compatible(&[], &caps1));
        assert!(!CapMatcher::are_compatible(&caps1, &[]));
    }

    // TEST565: tags_to_string returns only tags portion without prefix
    #[test]
    fn test565_tags_to_string() {
        let cap = CapUrn::from_string(r#"cap:in=media:void;out=media:void;test"#).unwrap();
        let tags_str = cap.tags_to_string();
        // Should NOT start with "cap:"
        assert!(!tags_str.starts_with("cap:"));
        // Should contain in, out, op tags
        assert!(tags_str.contains("test"));
    }

    // TEST566: with_tag rejects structural keys
    #[test]
    fn test566_with_tag_rejects_structural_keys() {
        let cap = CapUrn::from_string(r#"cap:in=media:void;out=media:void;test"#).unwrap();
        let same = cap
            .clone()
            .with_tag("in".to_string(), "media:".to_string())
            .unwrap_err();
        assert!(matches!(same, CapUrnError::InvalidTagFormat(_)));

        let same2 = cap
            .clone()
            .with_tag("out".to_string(), "media:".to_string())
            .unwrap_err();
        assert!(matches!(same2, CapUrnError::InvalidTagFormat(_)));

        let same3 = cap
            .with_tag("effect".to_string(), "none".to_string())
            .unwrap_err();
        assert!(matches!(same3, CapUrnError::InvalidTagFormat(_)));
    }

    // TEST567: conforms_to_str and accepts_str work with string arguments
    #[test]
    fn test567_str_variants() {
        let cap = CapUrn::from_string(r#"cap:in=media:void;out=media:void;test"#).unwrap();

        // accepts_str
        assert!(cap
            .accepts_str(r#"cap:ext=pdf;in=media:void;out=media:void;test"#)
            .unwrap());
        assert!(!cap
            .accepts_str(r#"cap:different;in=media:void;out=media:void"#)
            .unwrap());

        // conforms_to_str
        assert!(cap
            .conforms_to_str(r#"cap:in=media:void;out=media:void;test"#)
            .unwrap());

        // Invalid URN string → error
        assert!(cap.accepts_str("invalid").is_err());
        assert!(cap.conforms_to_str("invalid").is_err());
    }

    // TEST568: is_dispatchable with different tag order in output spec
    #[test]
    fn test568_dispatch_output_tag_order() {
        // Candidate has: enc=utf-8;record
        let candidate = CapUrn::from_string(
            r#"cap:in="media:enc=utf-8;model-spec";download-model;out="media:enc=utf-8;download-result;record""#
        ).unwrap();
        // Request has: record;enc=utf-8 (same tags, different order)
        let request = CapUrn::from_string(
            r#"cap:in="media:enc=utf-8;model-spec";download-model;out="media:enc=utf-8;download-result;record""#
        ).unwrap();

        // After parsing, both should be normalized to same canonical form
        assert_eq!(
            candidate.out_spec(),
            request.out_spec(),
            "Output specs should be normalized to same canonical form"
        );

        // And dispatch should work
        assert!(
            candidate.is_dispatchable(&request),
            "Candidate should dispatch request with same tags in different order"
        );
    }

    // TEST823: is_dispatchable — exact match candidate dispatches request
    #[test]
    fn test823_dispatch_exact_match() {
        let candidate =
            CapUrn::from_string(r#"cap:extract;in="media:ext=pdf";out="media:enc=utf-8;record""#)
                .unwrap();
        let request =
            CapUrn::from_string(r#"cap:extract;in="media:ext=pdf";out="media:enc=utf-8;record""#)
                .unwrap();
        assert!(candidate.is_dispatchable(&request));
    }

    // TEST824: is_dispatchable — candidate with broader input handles specific request (contravariance)
    #[test]
    fn test824_dispatch_contravariant_input() {
        let candidate =
            CapUrn::from_string(r#"cap:analyze;in=media:;out="media:enc=utf-8;record""#).unwrap();
        let request =
            CapUrn::from_string(r#"cap:analyze;in="media:ext=pdf";out="media:enc=utf-8;record""#)
                .unwrap();
        assert!(candidate.is_dispatchable(&request));
    }

    // TEST825: is_dispatchable — request with unconstrained input dispatches to specific candidate
    // media: on the request input axis means "unconstrained" — vacuously true
    #[test]
    fn test825_dispatch_request_unconstrained_input() {
        let candidate =
            CapUrn::from_string(r#"cap:analyze;in="media:ext=pdf";out="media:enc=utf-8;record""#)
                .unwrap();
        let request =
            CapUrn::from_string(r#"cap:analyze;in=media:;out="media:enc=utf-8;record""#).unwrap();
        assert!(
            candidate.is_dispatchable(&request),
            "Request in=media: is unconstrained — axis is vacuously true"
        );
    }

    // TEST826: is_dispatchable — candidate output must satisfy request output (covariance)
    #[test]
    fn test826_dispatch_covariant_output() {
        let candidate =
            CapUrn::from_string(r#"cap:extract;in="media:ext=pdf";out="media:enc=utf-8;record""#)
                .unwrap();
        let request =
            CapUrn::from_string(r#"cap:extract;in="media:ext=pdf";out="media:enc=utf-8""#).unwrap();
        assert!(
            candidate.is_dispatchable(&request),
            "Candidate output enc=utf-8;record conforms to request output enc=utf-8"
        );
    }

    // TEST827: is_dispatchable — candidate with generic output cannot satisfy specific request
    #[test]
    fn test827_dispatch_generic_output_fails() {
        let candidate =
            CapUrn::from_string(r#"cap:extract;in="media:ext=pdf";out=media:"#).unwrap();
        let request =
            CapUrn::from_string(r#"cap:extract;in="media:ext=pdf";out="media:enc=utf-8;record""#)
                .unwrap();
        assert!(
            !candidate.is_dispatchable(&request),
            "Candidate out=media: cannot guarantee specific output"
        );
    }

    // TEST828: is_dispatchable — wildcard * tag in request, candidate missing tag → reject
    #[test]
    fn test828_dispatch_wildcard_requires_tag_presence() {
        let candidate = CapUrn::from_string(
            r#"cap:in=media:model-spec;out="media:enc=utf-8;record";run-inference"#,
        )
        .unwrap();
        let request = CapUrn::from_string(
            r#"cap:candle;in=media:model-spec;out="media:enc=utf-8;record";run-inference"#,
        )
        .unwrap();
        assert!(
            !candidate.is_dispatchable(&request),
            "Wildcard * means tag must be present — candidate has no candle tag"
        );
    }

    // TEST829: is_dispatchable — wildcard * tag in request, candidate has tag → accept
    #[test]
    fn test829_dispatch_wildcard_with_tag_present() {
        let candidate = CapUrn::from_string(
            r#"cap:candle=metal;in=media:model-spec;out="media:enc=utf-8;record";run-inference"#,
        )
        .unwrap();
        let request = CapUrn::from_string(
            r#"cap:candle;in=media:model-spec;out="media:enc=utf-8;record";run-inference"#,
        )
        .unwrap();
        assert!(
            candidate.is_dispatchable(&request),
            "Candidate has candle=metal, request has candle=* — tag present, any value OK"
        );
    }

    // TEST830: is_dispatchable — candidate extra tags are refinement, always OK
    #[test]
    fn test830_dispatch_candidate_extra_tags() {
        let candidate = CapUrn::from_string(
            r#"cap:candle=metal;in=media:model-spec;out="media:enc=utf-8;record";run-inference"#,
        )
        .unwrap();
        let request = CapUrn::from_string(
            r#"cap:in=media:model-spec;out="media:enc=utf-8;record";run-inference"#,
        )
        .unwrap();
        assert!(
            candidate.is_dispatchable(&request),
            "Candidate extra tag candle=metal is refinement — always OK"
        );
    }

    // TEST831: is_dispatchable — cross-backend mismatch prevented
    #[test]
    fn test831_dispatch_cross_backend_mismatch() {
        let gguf_candidate = CapUrn::from_string(
            r#"cap:gguf=q4_k_m;in=media:model-spec;out="media:enc=utf-8;record";run-inference"#,
        )
        .unwrap();
        let candle_request = CapUrn::from_string(
            r#"cap:candle;in=media:model-spec;out="media:enc=utf-8;record";run-inference"#,
        )
        .unwrap();
        assert!(
            !gguf_candidate.is_dispatchable(&candle_request),
            "GGUF candidate has no candle tag — cross-backend mismatch"
        );
    }

    // TEST832: is_dispatchable is NOT symmetric
    #[test]
    fn test832_dispatch_asymmetric() {
        let broad =
            CapUrn::from_string(r#"cap:in=media:;out="media:enc=utf-8;record";process"#).unwrap();
        let narrow =
            CapUrn::from_string(r#"cap:in="media:ext=pdf";out="media:enc=utf-8";process"#).unwrap();
        // broad candidate CAN dispatch narrow request:
        //   input:  candidate in=media: accepts anything → OK
        //   output: candidate out=media:enc=utf-8;record conforms to request out=media:enc=utf-8 → OK
        assert!(broad.is_dispatchable(&narrow));
        // narrow candidate CANNOT dispatch broad request:
        //   input:  request in=media: unconstrained → OK
        //   output: candidate out=media:enc=utf-8, request out=media:enc=utf-8;record
        //           enc=utf-8 does NOT conform to enc=utf-8;record → FAIL
        assert!(!narrow.is_dispatchable(&broad));
    }

    // TEST833: is_comparable — both directions checked
    #[test]
    fn test833_comparable_symmetric() {
        let a =
            CapUrn::from_string(r#"cap:extract;in="media:ext=pdf";out="media:enc=utf-8""#).unwrap();
        let b =
            CapUrn::from_string(r#"cap:extract;in="media:ext=pdf";out="media:enc=utf-8;record""#)
                .unwrap();
        assert!(a.is_comparable(&b));
        assert!(b.is_comparable(&a));
    }

    // TEST834: is_comparable — unrelated caps are NOT comparable
    #[test]
    fn test834_comparable_unrelated() {
        let a =
            CapUrn::from_string(r#"cap:extract;in="media:ext=pdf";out="media:enc=utf-8""#).unwrap();
        let b =
            CapUrn::from_string(r#"cap:in=media:audio;out="media:enc=utf-8;record";transcribe"#)
                .unwrap();
        assert!(!a.is_comparable(&b));
        assert!(!b.is_comparable(&a));
    }

    // TEST835: is_equivalent — identical caps
    #[test]
    fn test835_equivalent_identical() {
        let a =
            CapUrn::from_string(r#"cap:extract;in="media:ext=pdf";out="media:enc=utf-8;record""#)
                .unwrap();
        let b =
            CapUrn::from_string(r#"cap:extract;in="media:ext=pdf";out="media:enc=utf-8;record""#)
                .unwrap();
        assert!(a.is_equivalent(&b));
        assert!(b.is_equivalent(&a));
    }

    // TEST836: is_equivalent — non-equivalent comparable caps
    #[test]
    fn test836_equivalent_non_equivalent() {
        let a =
            CapUrn::from_string(r#"cap:extract;in="media:ext=pdf";out="media:enc=utf-8""#).unwrap();
        let b =
            CapUrn::from_string(r#"cap:extract;in="media:ext=pdf";out="media:enc=utf-8;record""#)
                .unwrap();
        assert!(a.is_comparable(&b));
        assert!(!a.is_equivalent(&b));
    }

    // TEST837: is_dispatchable — op tag mismatch rejects
    #[test]
    fn test837_dispatch_op_mismatch() {
        let candidate =
            CapUrn::from_string(r#"cap:extract;in="media:ext=pdf";out="media:enc=utf-8;record""#)
                .unwrap();
        let request =
            CapUrn::from_string(r#"cap:in="media:ext=pdf";out="media:enc=utf-8;record";summarize"#)
                .unwrap();
        assert!(!candidate.is_dispatchable(&request));
    }

    // TEST838: is_dispatchable — request with wildcard output accepts any candidate output
    #[test]
    fn test838_dispatch_request_wildcard_output() {
        let candidate =
            CapUrn::from_string(r#"cap:extract;in="media:ext=pdf";out="media:enc=utf-8;record""#)
                .unwrap();
        let request = CapUrn::from_string(r#"cap:extract;in="media:ext=pdf";out=media:"#).unwrap();
        assert!(
            candidate.is_dispatchable(&request),
            "Request out=media: is unconstrained — any candidate output accepted"
        );
    }

    // -------------------------------------------------------------------
    // CapKind classifier tests (test1800–test1805).
    //
    // These tests are mirrored across every language port (Rust, Go,
    // Python, Swift/ObjC, JS) under the SAME numbers. Any divergence
    // is a wire-level inconsistency — the kind taxonomy is part of
    // the protocol's public surface, not a per-port detail.
    // -------------------------------------------------------------------

    // TEST1800: Identity classifier — and only explicit effect=none
    // qualifies.
    #[test]
    fn test1800_kind_identity_only_for_bare_cap() {
        let identity = CapUrn::from_string("cap:effect=none").unwrap();
        assert_eq!(identity.kind().unwrap(), CapKind::Identity);

        for spelling in &[
            "cap:in=media:;out=media:;effect=none",
            "cap:effect=none;in=*;out=*",
            "cap:effect=none;in=media:",
            "cap:effect=none;out=media:",
        ] {
            let cap = CapUrn::from_string(spelling).unwrap();
            assert_eq!(
                cap.kind().unwrap(),
                CapKind::Identity,
                "{spelling} should classify as Identity"
            );
        }

        assert!(matches!(
            CapUrn::from_string("cap:"),
            Err(CapUrnError::IllegalDeclaration(_))
        ));

        // Any non-directional tag demotes Identity to Transform.
        let with_op = CapUrn::from_string("cap:passthrough").unwrap();
        assert_eq!(
            with_op.kind().unwrap(),
            CapKind::Transform,
            "cap:passthrough specifies the operation axis — not Identity"
        );
    }

    // TEST1801: Source classifier — in=media:void, out non-void.
    // The y dimension may carry any tags; void on the input alone is
    // what matters.
    #[test]
    fn test1801_kind_source_when_input_is_void() {
        let warm =
            CapUrn::from_string(r#"cap:in=media:void;out="media:model-artifact";warm"#).unwrap();
        assert_eq!(warm.kind().unwrap(), CapKind::Source);

        // Output need not be a leaf type — even out=media: counts as
        // non-void, and that pairing with in=void is a Source. (The
        // protocol does not privilege out=media: here; the
        // classifier looks for `void` specifically.)
        let generator = CapUrn::from_string("cap:in=media:void;out=\"media:enc=utf-8\"").unwrap();
        assert_eq!(generator.kind().unwrap(), CapKind::Source);
    }

    // TEST1802: Sink classifier — out=media:void, in non-void.
    #[test]
    fn test1802_kind_sink_when_output_is_void() {
        let discard = CapUrn::from_string("cap:discard;in=media:;out=media:void").unwrap();
        assert_eq!(discard.kind().unwrap(), CapKind::Sink);

        let log = CapUrn::from_string(r#"cap:in="media:fmt=json";log;out=media:void"#).unwrap();
        assert_eq!(log.kind().unwrap(), CapKind::Sink);
    }

    // TEST1803: Effect classifier — both sides void. Reads as `() → ()`.
    #[test]
    fn test1803_kind_effect_when_both_sides_void() {
        let ping = CapUrn::from_string("cap:in=media:void;out=media:void;ping").unwrap();
        assert_eq!(ping.kind().unwrap(), CapKind::Effect);

        // Effect is decided by the directional axes alone — y may be
        // empty (no other tags), but a fully bare `cap:in=void;out=void`
        // is still an Effect. The y axis carries identity, not kind.
        let bare_effect = CapUrn::from_string("cap:in=media:void;out=media:void").unwrap();
        assert_eq!(bare_effect.kind().unwrap(), CapKind::Effect);
    }

    // TEST1804: Transform classifier — at least one side non-void,
    // and the cap is not the bare identity. The default kind for
    // ordinary data-processing caps.
    #[test]
    fn test1804_kind_transform_for_normal_data_processors() {
        let extract =
            CapUrn::from_string(r#"cap:extract;in="media:ext=pdf";out="media:enc=utf-8;record""#)
                .unwrap();
        assert_eq!(extract.kind().unwrap(), CapKind::Transform);

        // Adding any tag to a fully generic shape is also a Transform —
        // op tag is just metadata, but its presence makes y non-empty
        // so the cap is no longer Identity.
        let labeled = CapUrn::from_string("cap:passthrough;in=media:;out=media:").unwrap();
        assert_eq!(labeled.kind().unwrap(), CapKind::Transform);
    }

    // TEST1805: Kind is invariant under canonicalization. The same
    // morphism written in many surface forms must classify the same
    // way once parsed. This pins the rule that kind is a property of
    // the cap as a structured object, not of any particular spelling.
    #[test]
    fn test1805_kind_invariant_under_canonical_spellings() {
        // Each tuple: (spelling_a, spelling_b, expected kind).
        // The two spellings parse to canonically-equal URNs.
        let cases: &[(&str, &str, CapKind)] = &[
            (
                "cap:effect=none",
                "cap:in=media:;out=media:;effect=none",
                CapKind::Identity,
            ),
            // Transform — quoted vs unquoted single-tag media URN.
            (
                "cap:extract;in=\"media:ext=pdf\";out=\"media:enc=utf-8\"",
                r#"cap:extract;in="media:ext=pdf";out="media:enc=utf-8""#,
                CapKind::Transform,
            ),
            // Source — segment order at parse time must not change kind.
            (
                "cap:in=media:void;out=\"media:enc=utf-8\";warm",
                "cap:warm;out=\"media:enc=utf-8\";in=media:void",
                CapKind::Source,
            ),
        ];

        for (a, b, expected) in cases {
            let kind_a = CapUrn::from_string(a).unwrap().kind().unwrap();
            let kind_b = CapUrn::from_string(b).unwrap().kind().unwrap();
            assert_eq!(
                kind_a, *expected,
                "{a} should classify as {expected:?}, got {kind_a:?}"
            );
            assert_eq!(
                kind_b, *expected,
                "{b} should classify as {expected:?}, got {kind_b:?}"
            );
            assert_eq!(
                kind_a, kind_b,
                "{a} and {b} parse to the same cap and must classify identically"
            );
        }
    }

    // -------------------------------------------------------------------
    // Truth-table specificity tests (test1820–test1824).
    //
    // Mirrored across every language port (Rust, Go, Python, Swift/ObjC,
    // JS) under the SAME numbers. Specificity must be the truth-table
    // sum across all three axes, with no axis-specific carve-out:
    //
    //   ? or missing      -> 0   (no constraint)
    //   !                 -> 1   (must-not-have)
    //   * (incl. marker)  -> 2   (must-have-any)
    //   exact key=value   -> 3   (must-have-this-value)
    //
    // Each test exercises one row of the truth table on the y axis,
    // alone, so a regression in any single form fails its dedicated
    // test rather than being absorbed by a sibling. test1824 then
    // composes all forms at once on a single cap.
    // -------------------------------------------------------------------

    // TEST1820: A `?`-valued cap-tag scores 0. Same as missing.
    #[test]
    fn test1820_specificity_question_is_zero() {
        // `?effect` is the legal fully unconstrained request baseline.
        let bare = CapUrn::from_string("cap:?effect").unwrap();
        assert_eq!(bare.specificity(), 0);

        // Adding `?target` (canonical of `target=?`) does NOT change
        // the score: explicit no-constraint contributes 0, exactly
        // like a missing key.
        let with_q = CapUrn::from_string("cap:?target").unwrap();
        assert_eq!(
            with_q.specificity(),
            0,
            "?x must score 0 (explicit no-constraint, same as missing)"
        );
    }

    // TEST1821: A `!`-valued cap-tag scores 5 (top of negative chain).
    #[test]
    fn test1821_specificity_must_not_have_is_five() {
        let cap = CapUrn::from_string("cap:!constrained").unwrap();
        assert_eq!(
            cap.specificity(),
            5,
            "!constrained (must-not-have) must score 5"
        );
    }

    // TEST1822: A `*`-valued cap-tag (including bare markers) scores 2.
    #[test]
    fn test1822_specificity_must_have_any_is_two() {
        // Bare segment.
        let bare_marker = CapUrn::from_string("cap:extract").unwrap();
        assert_eq!(
            bare_marker.specificity(),
            2,
            "bare `extract` parses as extract=* (must-have-any) and scores 2"
        );

        // Explicit key=*.
        let explicit_star = CapUrn::from_string("cap:extract=*").unwrap();
        assert_eq!(
            explicit_star.specificity(),
            2,
            "explicit key=* must score 2 (same as bare marker)"
        );

        // The two forms must agree byte-for-byte on score.
        assert_eq!(
            bare_marker.specificity(),
            explicit_star.specificity(),
            "bare marker and explicit key=* are the same form and must score identically"
        );
    }

    // TEST1823: An exact-valued cap-tag scores 4.
    #[test]
    fn test1823_specificity_exact_value_is_four() {
        let cap = CapUrn::from_string("cap:target=metadata").unwrap();
        assert_eq!(
            cap.specificity(),
            4,
            "target=metadata (exact value) must score 4"
        );
    }

    // TEST1824: All six forms compose additively on a single cap.
    // This pins the truth-table sum across the y axis as a whole.
    #[test]
    fn test1824_specificity_combined_y_axis() {
        // y carries one of each form:
        //   ?target          -> 0  (no constraint)
        //   ver?=draft       -> 1  (absent OR not draft)
        //   extract          -> 2  (bare marker, must-have-any)
        //   stage!=alpha     -> 3  (present and not alpha)
        //   target2=metadata -> 4  (exact)
        //   !constrained     -> 5  (must-not-have)
        // Total y score: 0+1+2+3+4+5 = 15.
        // in=media:(0) + out=media:(0) + y(15) = 15.
        let cap = CapUrn::from_string(
            "cap:!constrained;?target;extract;stage!=alpha;target2=metadata;ver?=draft",
        )
        .unwrap();
        assert_eq!(
            cap.specificity(),
            15,
            "y combining all six forms (0+1+2+3+4+5) must sum to 15"
        );
    }

    // -----------------------------------------------------------------
    // Six-form canonicalization tests (test1830–test1835).
    //
    // Mirrored across every language port (Rust, Go, Python,
    // Swift/ObjC, JS) under the SAME numbers. Each test enumerates
    // the authored aliases for one canonical form and verifies that
    // ALL of them parse to the same canonical serialization.
    // -----------------------------------------------------------------

    // TEST1830: ?x ≡ x? ≡ x=? all canonicalize to ?x.
    #[test]
    fn test1830_canonicalize_no_constraint() {
        let canonical = "cap:?x";
        for input in ["cap:?x", "cap:x?", "cap:x=?"] {
            let cap = CapUrn::from_string(input).unwrap();
            assert_eq!(
                cap.to_string(),
                canonical,
                "input '{}' must canonicalize to '{}'",
                input,
                canonical
            );
        }
    }

    // TEST1831: ?x=v and x?=v both canonicalize to x?=v. The third
    // hypothetical form `x=?v` is NOT recognized as a qualifier — a
    // value starting with `?` is just an exact value beginning with
    // a `?` character.
    #[test]
    fn test1831_canonicalize_absent_or_not_value() {
        let canonical = "cap:x?=foo";
        for input in ["cap:?x=foo", "cap:x?=foo"] {
            let cap = CapUrn::from_string(input).unwrap();
            assert_eq!(
                cap.to_string(),
                canonical,
                "input '{}' must canonicalize to '{}'",
                input,
                canonical
            );
        }

        // `x=?foo` is a plain exact tag whose value is the string
        // `?foo` — NOT a canonicalization alias.
        let exact = CapUrn::from_string("cap:x=?foo").unwrap();
        assert_eq!(exact.to_string(), "cap:x=?foo");
        assert_eq!(exact.get_tag("x"), Some(&"?foo".to_string()));
    }

    // TEST1832: x ≡ x=* both canonicalize to bare x.
    #[test]
    fn test1832_canonicalize_must_have_any() {
        let canonical = "cap:x";
        for input in ["cap:x", "cap:x=*"] {
            let cap = CapUrn::from_string(input).unwrap();
            assert_eq!(
                cap.to_string(),
                canonical,
                "input '{}' must canonicalize to '{}'",
                input,
                canonical
            );
        }
    }

    // TEST1833: !x=v and x!=v both canonicalize to x!=v. The third
    // hypothetical form `x=!v` is NOT recognized as a qualifier — a
    // value starting with `!` is just an exact value beginning with
    // a `!` character.
    #[test]
    fn test1833_canonicalize_present_not_value() {
        let canonical = "cap:x!=foo";
        for input in ["cap:!x=foo", "cap:x!=foo"] {
            let cap = CapUrn::from_string(input).unwrap();
            assert_eq!(
                cap.to_string(),
                canonical,
                "input '{}' must canonicalize to '{}'",
                input,
                canonical
            );
        }

        // `x=!foo` is a plain exact tag whose value is the string
        // `!foo` — NOT a canonicalization alias.
        let exact = CapUrn::from_string("cap:x=!foo").unwrap();
        assert_eq!(exact.to_string(), "cap:x=!foo");
        assert_eq!(exact.get_tag("x"), Some(&"!foo".to_string()));
    }

    // TEST1834: x=v stays as x=v (the lone exact-value form).
    #[test]
    fn test1834_canonicalize_exact_value() {
        let cap = CapUrn::from_string("cap:x=foo").unwrap();
        assert_eq!(cap.to_string(), "cap:x=foo");
    }

    // TEST1835: !x ≡ x! ≡ x=! all canonicalize to !x.
    #[test]
    fn test1835_canonicalize_must_not_have() {
        let canonical = "cap:!x";
        for input in ["cap:!x", "cap:x!", "cap:x=!"] {
            let cap = CapUrn::from_string(input).unwrap();
            assert_eq!(
                cap.to_string(),
                canonical,
                "input '{}' must canonicalize to '{}'",
                input,
                canonical
            );
        }
    }

    // TEST1842: Full 6×6 truth table — every cell must match the
    // matrix in 04-PREDICATES.md §2.5. Treats prefix `cap:` as the
    // host for a single-key URN (key `x`), pairing every instance form
    // with every pattern form.
    #[test]
    fn test1842_truth_table_full_cross_product() {
        // Forms keyed by the URN string used for the single-key URN.
        // "" represents "missing" (no entry for x).
        let forms = [
            "",     // missing
            "?x",   // no-constraint
            "x?=v", // absent-or-not-v
            "x",    // must-have-any
            "x!=v", // present-not-v
            "x=v",  // exact v
            "!x",   // must-not-have
        ];

        // Expected matrix: rows are instances, columns are patterns.
        // Read from 04-PREDICATES.md §2.5.
        // Rows below correspond to forms: missing, ?x, x?=v, x,
        // x!=v, x=v, !x — same order as `forms` above. Block-comment
        // labels are spaced so a leading `!` is not interpreted as
        // an inner doc comment.
        let expected: [[bool; 7]; 7] = [
            //          miss   ?x    x?=v   x      x!=v   x=v    !x
            /* miss */
            [true, true, true, false, false, false, true],
            /* ?x   */ [true, true, true, true, true, true, true],
            /* x?=v */ [true, true, true, false, false, false, true],
            /* x    */ [true, true, true, true, true, true, false],
            /* x!=v */ [true, true, true, true, true, false, false],
            /* x=v  */ [true, true, false, true, false, true, false],
            /* !x   */ [true, true, true, false, false, false, true],
        ];

        for (i, inst_form) in forms.iter().enumerate() {
            for (j, patt_form) in forms.iter().enumerate() {
                let inst_str = if inst_form.is_empty() {
                    "cap:base".to_string()
                } else {
                    format!("cap:base;{}", inst_form)
                };
                let patt_str = if patt_form.is_empty() {
                    "cap:base".to_string()
                } else {
                    format!("cap:base;{}", patt_form)
                };
                let inst = CapUrn::from_string(&inst_str).unwrap();
                let patt = CapUrn::from_string(&patt_str).unwrap();
                let actual = patt.accepts(&inst);
                assert_eq!(
                    actual, expected[i][j],
                    "truth-table cell (inst={}, patt={}) expected {} got {}",
                    inst_form, patt_form, expected[i][j], actual
                );
            }
        }
    }

    // TEST6734: Invalid combinations of qualifiers must be rejected
    // by the parser. These are hard parse errors, not silently
    // accepted shorthands.
    #[test]
    fn test6734_reject_invalid_combinations() {
        // Each input must fail to parse.
        let invalid = [
            "cap:?x?=v", // prefix and infix ? together
            "cap:!x!=v", // prefix and infix ! together
            "cap:?!x",   // mixing ? and ! prefixes
            "cap:!?x",   // mixing ! and ? prefixes
            "cap:?x=*",  // ? qualifier with * sigil value
            "cap:!x=*",  // ! qualifier with * sigil value
            "cap:?x=?",  // ? qualifier with ? sigil value
            "cap:?x=!",  // ? qualifier with ! sigil value
            "cap:!x=?",  // ! qualifier with ? sigil value
            "cap:!x=!",  // ! qualifier with ! sigil value
            "cap:?",     // bare ? with no key
            "cap:!",     // bare ! with no key
        ];
        for input in invalid {
            let result = CapUrn::from_string(input);
            assert!(
                result.is_err(),
                "input '{}' must be rejected by the parser",
                input
            );
        }
    }

    // TEST6735: Cap-URN axis weighting is lexicographic. A cap with
    // greater out-axis specificity always outranks one with greater
    // in-axis specificity, regardless of y-axis.
    #[test]
    fn test6735_axis_weighting_out_dominates() {
        // out=4 (enc=utf-8;record), in=0 (media:), y=0
        let big_out = CapUrn::from_string("cap:in=media:;out=\"media:enc=utf-8;record\"").unwrap();
        // out=2 (record), in=2 (pdf), y=15 (loaded y axis)
        let big_in_and_y = CapUrn::from_string(
            "cap:in=\"media:ext=pdf\";out=media:record;!constrained;?target;extract;stage!=alpha;target2=metadata;ver?=draft",
        )
        .unwrap();
        assert!(
            big_out.specificity() > big_in_and_y.specificity(),
            "out-axis difference must dominate combined in+y differences"
        );
    }

    // TEST1845: With equal out-axis, in-axis dominates over y-axis.
    #[test]
    fn test1845_axis_weighting_in_dominates_y() {
        // out=2 (record), in=2 (pdf), y=0
        let big_in = CapUrn::from_string("cap:in=\"media:ext=pdf\";out=media:record").unwrap();
        // out=2 (record), in=0 (media:), y=15 (loaded y axis)
        let big_y = CapUrn::from_string(
            "cap:in=media:;out=media:record;!constrained;?target;extract;stage!=alpha;target2=metadata;ver?=draft",
        )
        .unwrap();
        assert!(
            big_in.specificity() > big_y.specificity(),
            "in-axis difference must dominate y-axis"
        );
    }

    // TEST6736: Specificity composes uniformly across all three axes.
    // For a fully-loaded cap with one of each form per axis, the
    // total reads as out_score, in_score, y_score in distinct digit
    // slots.
    #[test]
    fn test6736_axis_weighting_decoded_layout() {
        // Construct a cap where every axis has a controlled score.
        //
        // out=media:a;b;c;d (4 markers, score 4*2 = 8)
        // in =media:a;b (2 markers, score 4)
        // y  = one marker (score 2)
        let cap =
            CapUrn::from_string("cap:in=\"media:a;b\";out=\"media:a;b;c;d\";extract").unwrap();
        // 10000*8 + 100*4 + 2 = 80402
        assert_eq!(cap.specificity(), 10_000 * 8 + 100 * 4 + 2);
    }
}
