//! capture — DEVICE-reference transport resolution (13.2 §Reference Media,
//! live family).
//!
//! The device analog of file-path reading: a cap declares an arg whose urn
//! is the live REFERENCE and whose `stdin` source is the CONTENT the feed
//! delivers; the RESOLVING RUNTIME — the cartridge runtime when the
//! consumer is an op, the HOST when the cartridge cannot reach the device
//! or when the host itself consumes the items (engine-side per-item
//! dispatch) — recognizes the reference, opens the device, and delivers
//! the content stream. The op (or the engine's region driver) is
//! transport-blind.
//!
//! This is NOT a plugin system: the set of capture backends is closed and
//! known at compile time, dispatched by reference-URN family in plain code
//! below — exactly as file paths are read by plain code, with no registry
//! and no registration. The hardware backends (microphone, webcam) sit
//! behind the `capture` cargo feature (they carry the vendored ffmpeg
//! device stack); the deterministic synthetic feed is always built. A
//! reference whose family has no backend — or a hardware family in a build
//! without the feature — is a HARD, named error, never a silent empty
//! feed.

pub mod synthetic;

#[cfg(feature = "capture")]
pub mod microphone;
#[cfg(feature = "capture")]
pub mod webcam;

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use crate::bifaci::cartridge_runtime::RuntimeError;
use crate::bifaci::live_feed::{bridge_feed, LiveFeedSelector, OpenedFeed, MEDIA_LIVE_SYNTHETIC};
use crate::urn::media_urn::MediaUrn;

pub use synthetic::MEDIA_FEED_FRAMES;

/// The microphone reference family (stable regardless of the `capture`
/// feature — content pairing must be answerable in every build).
pub const MICROPHONE_REFERENCE: &str = "media:audio;live;microphone";
/// The content a resolved microphone feed delivers.
pub const MICROPHONE_CONTENT: &str = "media:audio;audio-frames;pcm";
/// The webcam reference family.
pub const WEBCAM_REFERENCE: &str = "media:image;live;webcam";
/// The content a resolved webcam feed delivers.
pub const WEBCAM_CONTENT: &str = "media:image;video-frame";

/// The closed set of device families this codebase can resolve.
enum Backend {
    Synthetic,
    Microphone,
    Webcam,
}

fn backend_for(reference: &MediaUrn) -> Option<Backend> {
    let matches = |pattern: &str| {
        MediaUrn::from_string(pattern)
            .expect("BUG: capture family constant is an invalid media URN")
            .accepts(reference)
            .unwrap_or(false)
    };
    if matches(MEDIA_LIVE_SYNTHETIC) {
        Some(Backend::Synthetic)
    } else if matches(MICROPHONE_REFERENCE) {
        Some(Backend::Microphone)
    } else if matches(WEBCAM_REFERENCE) {
        Some(Backend::Webcam)
    } else {
        None
    }
}

/// The CONTENT urn a reference's resolved feed delivers, when the
/// reference belongs to a known device family. Used by main-input
/// resolution: the content urn must conform to the consuming arg's
/// declared urn. Answerable in EVERY build (the pairing is knowledge, the
/// device stack is not needed to state it).
pub fn content_urn_for(reference: &MediaUrn) -> Option<&'static str> {
    match backend_for(reference)? {
        Backend::Synthetic => Some(MEDIA_FEED_FRAMES),
        Backend::Microphone => Some(MICROPHONE_CONTENT),
        Backend::Webcam => Some(WEBCAM_CONTENT),
    }
}

/// Open the device a live reference names: dispatch to the family's
/// backend and bridge capture → bounded delivery (ring, overrun policy,
/// stop conditions — see [`bridge_feed`]). `overruns_total` is the calling
/// runtime's aggregate overrun counter (rides heartbeat meta cartridge-side;
/// the host accounts per plan).
pub fn open(
    reference_urn: &str,
    selector: LiveFeedSelector,
    overruns_total: Arc<AtomicU64>,
) -> Result<OpenedFeed, RuntimeError> {
    let reference = MediaUrn::from_string(reference_urn).map_err(|e| {
        RuntimeError::Handler(format!(
            "live-feed reference URN '{}' is not a valid media URN: {}",
            reference_urn, e
        ))
    })?;
    let backend = backend_for(&reference).ok_or_else(|| {
        RuntimeError::Handler(format!(
            "no capture backend exists for live reference '{}' — it is not a \
             known device family (microphone, webcam, synthetic)",
            reference_urn
        ))
    })?;
    match backend {
        Backend::Synthetic => bridge_feed(selector, overruns_total, synthetic::open),
        #[cfg(feature = "capture")]
        Backend::Microphone => bridge_feed(selector, overruns_total, microphone::open),
        #[cfg(feature = "capture")]
        Backend::Webcam => bridge_feed(selector, overruns_total, webcam::open),
        #[cfg(not(feature = "capture"))]
        Backend::Microphone | Backend::Webcam => Err(RuntimeError::Handler(format!(
            "this build has no device capture (capdag built without the 'capture' \
             feature) — live reference '{}' cannot be resolved here; run it through \
             a capture-capable host or cartridge",
            reference_urn
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST1457: the capture dispatch is a CLOSED set — a live reference
    // outside the known device families is a hard, NAMED error (never a
    // silent empty feed), and the reference→content pairings are stable
    // knowledge answerable in every build.
    #[tokio::test]
    async fn test1457_capture_dispatch_is_closed_and_named() {
        let err = open(
            "media:live;doesnotexist",
            crate::bifaci::live_feed::LiveFeedSelector::default(),
            Arc::new(AtomicU64::new(0)),
        )
        .expect_err("an unknown device family must refuse");
        let msg = err.to_string();
        assert!(
            msg.contains("no capture backend") && msg.contains("media:live;doesnotexist"),
            "the refusal names the cause and the reference: {msg}"
        );

        let urn = |s: &str| MediaUrn::from_string(s).unwrap();
        assert_eq!(
            content_urn_for(&urn(MICROPHONE_REFERENCE)),
            Some(MICROPHONE_CONTENT)
        );
        assert_eq!(content_urn_for(&urn(WEBCAM_REFERENCE)), Some(WEBCAM_CONTENT));
        assert_eq!(
            content_urn_for(&urn(MEDIA_LIVE_SYNTHETIC)),
            Some(MEDIA_FEED_FRAMES)
        );
        assert_eq!(content_urn_for(&urn("media:live;doesnotexist")), None);
    }

    // TEST1458: the synthetic backend delivers through the same bridge every
    // real device uses — deterministic items, feed ends on its own, zero
    // overruns for a keeping-up consumer.
    #[tokio::test]
    async fn test1458_synthetic_backend_bridges_end_to_end() {
        let overruns = Arc::new(AtomicU64::new(0));
        let selector = crate::bifaci::live_feed::LiveFeedSelector::parse(
            br#"{"params":{"items":4,"interval_ms":0,"item_bytes":3}}"#,
        )
        .expect("selector parses");
        let opened = open(MEDIA_LIVE_SYNTHETIC, selector, overruns.clone())
            .expect("synthetic feed opens");
        let mut rx = opened.rx;
        let mut got: Vec<Vec<u8>> = Vec::new();
        while let Some(item) = rx.recv().await {
            let (value, _meta) = item.expect("synthetic items deliver cleanly");
            match value {
                ciborium::Value::Bytes(b) => got.push(b),
                other => panic!("feed items are raw bytes, got {other:?}"),
            }
        }
        assert_eq!(
            got,
            vec![vec![0u8; 3], vec![1u8; 3], vec![2u8; 3], vec![3u8; 3]]
        );
        assert_eq!(opened.handle.overruns(), 0);
        assert_eq!(overruns.load(std::sync::atomic::Ordering::Relaxed), 0);
    }
}
