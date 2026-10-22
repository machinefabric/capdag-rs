//! Fabric dims — the axes definitions are built along.
//!
//! A dim (a language, a file type, a quantisation, …) is a fabric definition
//! like a cap, a media def or an alias: published on its own at
//! `dims/<sha256-of-name>/<defver>.json` and pinned by a manifest's `dims`
//! map. Its body says what the dim is and which values it may take, each with
//! its own documentation.
//!
//! A cap or media definition names the dims it was built from, and the value
//! chosen from each, in its `dims` ([`DimRef`]). Its own `documentation` is
//! only what is specific to it; what a dim and a value mean is read from the
//! dim. A reader shows both.

use serde::{Deserialize, Serialize};

/// One dim a definition was built from, and the value chosen from it. The
/// entries of a definition's `dims`, in the order its source references them.
///
/// Unknown fields are refused, as for arguments and outputs: a key this
/// library does not know would be dropped when the definition is written back
/// out, and the copy would no longer say what the registry said.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DimRef {
    /// The dim's name.
    pub dim: String,
    /// The name of the value chosen from that dim.
    pub value: String,
}

impl DimRef {
    pub fn new(dim: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            dim: dim.into(),
            value: value.into(),
        }
    }
}

/// A tag a dim value contributes to the URN of every definition built with
/// it. `value` is `*` for a marker tag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DimTag {
    pub key: String,
    pub value: String,
}

/// One value a dim may take.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredDimValue {
    /// The value's name, unique within the dim.
    pub name: String,
    /// Display-friendly title of the value.
    pub title: String,
    /// The tags the value contributes. At least one.
    pub tags: Vec<DimTag>,
    /// Markdown documentation of what choosing this value means.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
    /// The MIME media type the value gives the media defs built with it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    /// The file extensions the value gives the media defs built with it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extensions: Vec<String>,
}

/// Stored dim definition. Mirrors `fabric/dim.schema.json` on the wire and is
/// the body cached at `dims/<sha256-of-name>/<defver>.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredDim {
    /// The dim's name, unique across the snapshot.
    pub name: String,
    /// Per-definition version. Always >= 1 — dims exist only in the
    /// versioned regime.
    pub version: u32,
    /// Display-friendly title of the dim.
    pub title: String,
    /// Markdown documentation of what the dim is.
    pub documentation: String,
    /// The values the dim may take, in the order the dim declares them.
    pub values: Vec<StoredDimValue>,
}

/// Why a dim body is not one this library can hold.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DimError {
    #[error("dim name '{0}' is not a dim name (lowercase letters, digits and '-', starting with a letter)")]
    InvalidName(String),
    #[error("dim '{0}' declares no values")]
    NoValues(String),
    #[error("dim '{dim}' declares value '{value}' more than once")]
    DuplicateValue { dim: String, value: String },
    #[error("dim '{dim}' value '{value}' contributes no tag")]
    ValueWithoutTags { dim: String, value: String },
}

/// Whether `name` is a dim name: `^[a-z][a-z0-9-]*$`.
pub fn is_dim_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

impl StoredDim {
    /// The value named `name`, if the dim declares it.
    pub fn value(&self, name: &str) -> Option<&StoredDimValue> {
        self.values.iter().find(|v| v.name == name)
    }

    /// Hold the body to what `dim.schema.json` says beyond its field types:
    /// a dim name, at least one value, value names unique, every value with a
    /// tag.
    pub fn validate(&self) -> Result<(), DimError> {
        if !is_dim_name(&self.name) {
            return Err(DimError::InvalidName(self.name.clone()));
        }
        if self.values.is_empty() {
            return Err(DimError::NoValues(self.name.clone()));
        }
        let mut seen = std::collections::HashSet::new();
        for v in &self.values {
            if !seen.insert(v.name.as_str()) {
                return Err(DimError::DuplicateValue {
                    dim: self.name.clone(),
                    value: v.name.clone(),
                });
            }
            if v.tags.is_empty() {
                return Err(DimError::ValueWithoutTags {
                    dim: self.name.clone(),
                    value: v.name.clone(),
                });
            }
        }
        Ok(())
    }
}

/// A definition's `dims` names each dim at most once, and only by dim and
/// value names. Returns why not, naming the offending entry.
pub fn check_dim_refs(dims: &[DimRef]) -> Result<(), String> {
    let mut seen = std::collections::HashSet::new();
    for r in dims {
        if !is_dim_name(&r.dim) {
            return Err(format!("'{}' is not a dim name", r.dim));
        }
        if r.value.is_empty() {
            return Err(format!("dim '{}' is named without a value", r.dim));
        }
        if !seen.insert(r.dim.as_str()) {
            return Err(format!("dim '{}' is named more than once", r.dim));
        }
    }
    Ok(())
}
