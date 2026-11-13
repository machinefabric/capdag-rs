//! Adapter types for file type detection
//!
//! This module defines the result types used by the adapter system and the
//! `CartridgeAdapterInvoker` trait for invoking cartridge content-inspection
//! adapters over the Bifaci protocol.

use crate::input_resolver::InputResolverError;
use async_trait::async_trait;
use std::path::Path;

/// Maximum bytes of file content sent to a cartridge for adapter
/// (content-inspection) selection.
///
/// This is the single source of truth for the inspection prefix size.
/// All paths that hand bytes to a content-inspection adapter — the
/// host-side adapter invoker (cartridge route) and the engine's
/// extension-based content-analysis path (in-process route) — must
/// read at most this many bytes so cartridge handlers and the
/// engine's pattern validators see exactly the same prefix.
///
/// 100 KiB is generous enough to cover headers, magic-byte regions,
/// JSON top-level structures, and the first few pages of text in any
/// realistic file format, while keeping per-file analysis bounded so
/// dropping a folder of large media doesn't push hundreds of MB
/// through the adapter pipeline.
pub const MAX_CONTENT_INSPECTION_BYTES: usize = 100 * 1024;

/// Trait for invoking the adapter-selection cap on a specific cartridge.
///
/// A host that holds a [`RelaySwitch`](crate::RelaySwitch) asks its
/// cartridges through
/// [`RelaySwitchAdapterInvoker`](crate::input_resolver::RelaySwitchAdapterInvoker):
/// the desktop engine its attached cartridges, the `capdag` CLI the ones it
/// starts to inspect a file.
#[async_trait]
pub trait CartridgeAdapterInvoker: Send + Sync {
    /// Invoke adapter-selection cap on a specific cartridge by ID.
    ///
    /// The cartridge_id is the `InstalledCartridgeRecord.id` string that
    /// uniquely identifies the cartridge across reconnections.
    ///
    /// Returns:
    /// - `Ok(None)` for empty END frame (no match — cartridge doesn't handle this file)
    /// - `Ok(Some(media_urns))` for a successful detection with one or more media URNs
    /// - `Err(...)` for protocol errors, invalid responses, or infrastructure failures
    ///
    /// Invalid responses (stream output that isn't valid `{"media_urns": [...]}`) are
    /// runtime errors — the implementation must fail hard, not return None.
    async fn invoke_adapter_selection(
        &self,
        cartridge_id: &str,
        file_path: &Path,
    ) -> Result<Option<Vec<String>>, InputResolverError>;
}
