//! Adapter types for file type detection
//!
//! This module defines the result types used by the adapter system and the
//! `CartridgeAdapterInvoker` trait for invoking cartridge content-inspection
//! adapters over the Bifaci protocol.

use crate::input_resolver::{ContentStructure, InputResolverError};
use crate::urn::media_urn::MediaUrn;
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

/// Result of adapter detection — a selected media URN and its structure
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterResult {
    /// The selected media URN
    pub media_urn: String,

    /// The detected content structure
    pub content_structure: ContentStructure,
}

/// Trait for invoking the adapter-selection cap on a specific cartridge.
///
/// The implementation lives on the host side (floom-engine) where it has access
/// to the cartridge process/relay infrastructure. capdag defines the trait;
/// the host implements it.
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

/// Filter a validation-survivor set by the verdicts of consulted
/// cartridge adapters. Pure function — extracted from the engine's
/// content-analysis path so the discrimination rule can be unit
/// tested without the gRPC harness.
///
/// Rules:
/// - If a candidate URN has no registered adapter URN above it
///   (`candidate.conforms_to(adapter)` false for every registered
///   adapter), it passes through on the static-validation verdict
///   alone — no cartridge claimed jurisdiction over this URN's
///   territory, so we have nothing to defer to.
/// - If at least one registered adapter URN sits above the candidate,
///   the candidate survives iff at least one cartridge returned a URN
///   `R` such that `R.conforms_to(candidate)` — i.e. the cartridge
///   said "the file is this candidate type or more specific."
/// - Candidates whose URN string fails to parse are kept (no basis
///   for elimination from a malformed URN that still slipped past
///   the validation step).
///
/// All URN comparisons go through `MediaUrn::conforms_to`. No string
/// equality on URNs anywhere in this filter.
pub fn filter_by_handler_verdict(
    validation_survivors: &[String],
    registered_adapter_urns: &[MediaUrn],
    handler_returned: &std::collections::HashSet<String>,
) -> Vec<String> {
    validation_survivors
        .iter()
        .filter(|cand_str| {
            let cand_urn = match MediaUrn::from_string(cand_str) {
                Ok(u) => u,
                Err(_) => return true,
            };
            let has_owning_adapter = registered_adapter_urns
                .iter()
                .any(|adapter| cand_urn.conforms_to(adapter).unwrap_or(false));
            if !has_owning_adapter {
                return true;
            }
            handler_returned.iter().any(|returned_str| {
                let Ok(returned_urn) = MediaUrn::from_string(returned_str) else {
                    return false;
                };
                returned_urn.conforms_to(&cand_urn).unwrap_or(false)
            })
        })
        .cloned()
        .collect()
}

/// Cartridge-handler discrimination — the engine's `analyze_file_content`
/// step 5b, shared so no host re-implements the ask-and-gate loop: every
/// cartridge whose registered adapter URN matched the file's extension is
/// asked "do you recognize this file?", and the validation-survivor set is
/// gated by the collective verdict ([`filter_by_handler_verdict`]).
///
/// Per-cartridge outcomes:
/// - `Ok(Some(urns))` — the cartridge claims the file as those URNs; they
///   join the returned union.
/// - `Ok(None)` — the cartridge declined; its adapter URN's domain stays
///   gated, so candidates under it are dropped unless another cartridge
///   claims them.
/// - `Err(_)` — loud but not fatal: one misbehaving cartridge must not block
///   discrimination across the rest.
///
/// With NO matching adapters there is nothing to defer to and the static
/// validation verdict stands unchanged — which is also why a host with no
/// live cartridges (the capdag CLI) matches engine behavior by skipping this
/// step entirely.
pub async fn discriminate_by_cartridge_handlers(
    validation_survivors: &[String],
    adapter_pairs: &[(String, MediaUrn)],
    invoker: &dyn CartridgeAdapterInvoker,
    file_path: &Path,
) -> Vec<String> {
    if adapter_pairs.is_empty() {
        return validation_survivors.to_vec();
    }

    let mut handler_returned: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    for (cartridge_id, adapter_urn) in adapter_pairs {
        match invoker
            .invoke_adapter_selection(cartridge_id, file_path)
            .await
        {
            Ok(Some(urns)) => {
                for u in urns {
                    handler_returned.insert(u);
                }
            }
            Ok(None) => {
                tracing::debug!(
                    target: "capdag::input_resolver",
                    cartridge_id = %cartridge_id,
                    adapter_urn = %adapter_urn,
                    file = %file_path.display(),
                    "[discriminate_by_cartridge_handlers] cartridge declined adapter selection"
                );
            }
            Err(err) => {
                tracing::error!(
                    target: "capdag::input_resolver",
                    cartridge_id = %cartridge_id,
                    adapter_urn = %adapter_urn,
                    file = %file_path.display(),
                    error = %err,
                    "[discriminate_by_cartridge_handlers] cartridge adapter-selection error"
                );
            }
        }
    }

    let registered_adapter_urns: Vec<MediaUrn> =
        adapter_pairs.iter().map(|(_, urn)| urn.clone()).collect();
    filter_by_handler_verdict(validation_survivors, &registered_adapter_urns, &handler_returned)
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn urns(strs: &[&str]) -> Vec<MediaUrn> {
        strs.iter()
            .map(|s| MediaUrn::from_string(s).unwrap())
            .collect()
    }

    fn returned(strs: &[&str]) -> HashSet<String> {
        strs.iter().map(|s| (*s).to_string()).collect()
    }

    fn candidates(strs: &[&str]) -> Vec<String> {
        strs.iter().map(|s| (*s).to_string()).collect()
    }

    // TEST11004: Drops candidate whose owning handler declined
    #[test]
    fn test11004_drops_candidate_whose_owning_handler_declined() {
        // sample.txt is plain prose. modelcartridge owns the
        // `media:enc=utf-8;model-spec` adapter URN, and its content
        // predicate rejected the file. Every model-spec candidate
        // (including backend-tagged variants) must be dropped.
        let validation_survivors = candidates(&[
            "media:enc=utf-8;ext=txt",
            "media:enc=utf-8;model-spec",
            "media:candle;enc=utf-8;llm;model-spec",
            "media:enc=utf-8;gguf;llm;model-spec",
        ]);
        let registered = urns(&["media:enc=utf-8;model-spec"]);
        // No cartridge claimed the file — empty handler_returned.
        let surviving =
            filter_by_handler_verdict(&validation_survivors, &registered, &HashSet::new());
        assert_eq!(
            surviving,
            vec!["media:enc=utf-8;ext=txt".to_string()],
            "every model-spec variant must be dropped when its owning cartridge declined"
        );
    }

    // TEST11005: Keeps candidate when handler returned supertype
    #[test]
    fn test11005_keeps_candidate_when_handler_returned_supertype() {
        // sample.txt's content is `hf:openai/whisper-base` —
        // modelcartridge's content predicate accepts it and returns
        // the supertype `media:enc=utf-8;model-spec`. The supertype
        // candidate must survive; backend-tagged variants must NOT
        // (modelcartridge cannot tell which backend from the spec
        // string alone, so it doesn't claim them).
        let validation_survivors = candidates(&[
            "media:enc=utf-8;model-spec",
            "media:candle;enc=utf-8;llm;model-spec",
            "media:enc=utf-8;gguf;llm;model-spec",
        ]);
        let registered = urns(&["media:enc=utf-8;model-spec"]);
        let returned_urns = returned(&["media:enc=utf-8;model-spec"]);
        let surviving =
            filter_by_handler_verdict(&validation_survivors, &registered, &returned_urns);
        assert_eq!(
            surviving,
            vec!["media:enc=utf-8;model-spec".to_string()],
            "only the supertype the cartridge actually claimed should survive"
        );
    }

    // TEST11006: Passes candidate through when no cartridge owns its territory
    #[test]
    fn test11006_passes_candidate_through_when_no_cartridge_owns_its_territory() {
        // Some other candidate (not a model-spec) is in the
        // validation-survivor set. No cartridge has registered an
        // adapter URN above it, so it survives on the static
        // validation verdict alone — handler_returned doesn't have
        // to mention it.
        let validation_survivors = candidates(&["media:enc=utf-8;ext=txt"]);
        let registered = urns(&["media:enc=utf-8;model-spec"]);
        let surviving =
            filter_by_handler_verdict(&validation_survivors, &registered, &HashSet::new());
        assert_eq!(
            surviving,
            vec!["media:enc=utf-8;ext=txt".to_string()],
            "candidates outside any registered adapter's domain pass through"
        );
    }

    // TEST11007: Returned supertype does not promote specific candidates
    #[test]
    fn test11007_returned_supertype_does_not_promote_specific_candidates() {
        // Regression guard: with adapter_invoker's filter using
        // `returned.conforms_to(candidate)`, a cartridge returning a
        // generic URN must not silently promote backend-tagged
        // candidates whose extra tags the cartridge cannot infer.
        let validation_survivors = candidates(&["media:candle;enc=utf-8;llm;model-spec"]);
        let registered = urns(&["media:enc=utf-8;model-spec"]);
        let returned_urns = returned(&["media:enc=utf-8;model-spec"]);
        let surviving =
            filter_by_handler_verdict(&validation_survivors, &registered, &returned_urns);
        assert!(
            surviving.is_empty(),
            "a returned supertype must not survive a strictly-more-specific candidate"
        );
    }

    // TEST11008: Returned subtype keeps specific candidate
    #[test]
    fn test11008_returned_subtype_keeps_specific_candidate() {
        // The dual case: a cartridge that DOES return a backend-
        // specific URN (e.g. after inspecting model files on disk)
        // must keep the corresponding tag-specific candidate.
        let validation_survivors = candidates(&[
            "media:enc=utf-8;model-spec",
            "media:candle;enc=utf-8;llm;model-spec",
        ]);
        let registered = urns(&["media:enc=utf-8;model-spec"]);
        let returned_urns = returned(&["media:candle;enc=utf-8;llm;model-spec"]);
        let surviving =
            filter_by_handler_verdict(&validation_survivors, &registered, &returned_urns);
        // Both candidates survive: returned conforms_to itself
        // (specific) AND returned conforms_to the supertype.
        assert!(surviving.contains(&"media:enc=utf-8;model-spec".to_string()));
        assert!(surviving.contains(&"media:candle;enc=utf-8;llm;model-spec".to_string()));
    }

    // TEST11009: Empty handler returned with owning adapter drops everything in domain
    #[test]
    fn test11009_empty_handler_returned_with_owning_adapter_drops_everything_in_domain() {
        // Every adapter that owned the candidate's domain returned
        // empty (declined or errored). The candidate must NOT
        // survive — we have no positive signal for it.
        let validation_survivors = candidates(&["media:enc=utf-8;model-spec", "media:enc=utf-8;ext=txt"]);
        let registered = urns(&["media:enc=utf-8;model-spec"]);
        let surviving =
            filter_by_handler_verdict(&validation_survivors, &registered, &HashSet::new());
        assert_eq!(
            surviving,
            vec!["media:enc=utf-8;ext=txt".to_string()],
            "an in-domain candidate without any positive handler verdict must drop"
        );
    }

    // TEST11010: Malformed candidate urn passes through
    #[test]
    fn test11010_malformed_candidate_urn_passes_through() {
        // The filter is the last line of defence; a malformed URN
        // that survived parsing earlier shouldn't be dropped here on
        // a parse failure. The validation step is the place to drop
        // garbage URNs.
        let validation_survivors = candidates(&["not-a-valid-urn"]);
        let registered = urns(&["media:enc=utf-8;model-spec"]);
        let surviving =
            filter_by_handler_verdict(&validation_survivors, &registered, &HashSet::new());
        assert_eq!(surviving, vec!["not-a-valid-urn".to_string()]);
    }
}
