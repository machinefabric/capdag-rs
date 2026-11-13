//! InputResolver — Unified input resolution with pluggable content inspection
//!
//! This module provides two parallel content-inspection systems that solve the
//! same general problem: determining a specific media URN variant from content.
//!
//! ## File Content Resolution
//!
//! Resolves mixed file/directory/glob inputs into files with detected media types.
//! Content inspection is performed by cartridge-provided adapters via the
//! `CartridgeAdapterInvoker` trait.
//!
//! ## Value Content Resolution
//!
//! Inspects string argument values to refine a base media URN with domain markers:
//!
//! ```text
//! ValueAdapterRegistry (string values)
//!   └── (domain-specific adapters registered by consumers)
//!       e.g., ModelSpecValueAdapter: "hf:.../Mistral-7B..." → adds "mistral" tag
//! ```

mod adapter;
mod adapters;
pub mod detection;
mod os_filter;
mod relay_invoker;
pub(crate) mod path_resolver;
mod resolver;
mod types;
pub mod value_adapter;
pub mod value_adapter_registry;

pub use types::{ContentStructure, InputItem, InputResolverError, ResolvedFile, ResolvedInputSet};

pub use adapter::{CartridgeAdapterInvoker, MAX_CONTENT_INSPECTION_BYTES};

pub use detection::{
    detect_file_media, extension_media, primary_media, AdapterSource, FileMedia, NoAdapters,
    NoInvoker,
};

pub use resolver::{resolve_file, resolve_input, resolve_inputs, resolve_paths, Detection};

pub use path_resolver::resolve_directory;

pub use adapters::MediaAdapterRegistry;
pub use relay_invoker::RelaySwitchAdapterInvoker;
pub use value_adapter::{ValueAdapter, ValueAdapterResult};
pub use value_adapter_registry::ValueAdapterRegistry;
