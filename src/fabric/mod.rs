//! Unified fabric registry — caps, media defs, aliases and dims in one type.
//!
//! Both cap definitions and media definitions live in the same online
//! registry (`https://fabric.capdag.com/{caps,media}/<sha256>`). They share
//! the same caching policy, the same background-fetch queue, and the same
//! offline / clear-cache surface. They differ only in their wire payload
//! shape — `Cap` vs. `StoredMediaDef` — and in disk-cache subdirectory
//! (`caps/` vs. `media/`). `FabricRegistry` is the unified type.
//!
//! On miss `get_cached_cap` atomically fetches the cap AND every media URN
//! it references; if any of those media defs cannot be fetched the cap is
//! NOT cached. This keeps the cap cache consistent with its media-def
//! footprint.

pub mod alias;
pub mod dim;
pub mod registry;
