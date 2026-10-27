//! Tests for the `media:enc=utf-8;ext=txt;plain-text` terminal-binding contract.
//!
//! Background: the wizard's path planner finds candidate terminals by
//! locating caps whose `out` conforms to the requested target media URN,
//! and feeders by locating caps whose `out` conforms to the candidate's
//! `in`. Earlier, `cap:save-as-txt` declared `in = media:enc=utf-8`,
//! making the cap a universal sink — every textable-producing cap
//! (PDF page extractor, JSON-as-text adapter, prompt loader) became a
//! recommended feeder for `.txt` persistence, polluting the planner's
//! recommended-paths surface.
//!
//! The fix introduced `media:enc=utf-8;ext=txt;plain-text` as a narrow
//! concrete terminal type: producers of user-facing prose
//! (LLM text-generation, OCR's digitized text, summarisation) opt in
//! by declaring this URN as their `out`; everything else stays on
//! `media:enc=utf-8` and is therefore not a candidate feeder.
//!
//! These tests pin down the order-theoretic relations the contract
//! depends on. A regression that re-loosens `save-as-txt`'s `in`, or
//! that drops the narrow-marker discipline on producer caps, will
//! flip one of the assertions and surface immediately.

use capdag::MediaUrn;

/// Wraps `MediaUrn::conforms_to` so a parse failure on either side
/// panics loudly. A test URN that fails to parse is a test bug we'd
/// rather see than have masked into `false`.
fn conforms(concrete: &str, pattern: &str) -> bool {
    let c = MediaUrn::from_string(concrete)
        .unwrap_or_else(|e| panic!("BUG: concrete URN {:?} unparseable: {}", concrete, e));
    let p = MediaUrn::from_string(pattern)
        .unwrap_or_else(|e| panic!("BUG: pattern URN {:?} unparseable: {}", pattern, e));
    c.conforms_to(&p).unwrap_or_else(|e| {
        panic!(
            "conforms_to failed for ({:?},{:?}): {}",
            concrete, pattern, e
        )
    })
}

const PLAIN_TEXT: &str = "media:enc=utf-8;ext=txt;plain-text";
const DIGITIZED_TEXT: &str = "media:digitized-text;enc=utf-8;ext=txt;plain-text";
const IMAGE_DESCRIPTION: &str = "media:enc=utf-8;ext=txt;image-description;plain-text";
const DISBIND_PAGE: &str = "media:enc=utf-8;ext=txt;page;plain-text";
const TRANSCRIPTION: &str = "media:fmt=json;record;transcript";
const BARE_TEXTABLE: &str = "media:enc=utf-8";

/// `media:enc=utf-8;ext=txt;plain-text` conforms to itself — basic
/// reflexivity sanity. If this fails the canonical parser is broken.
#[test]
fn test0054_plain_text_conforms_to_itself() {
    assert!(
        conforms(PLAIN_TEXT, PLAIN_TEXT),
        "plain-text must trivially conform to itself"
    );
}

/// A concrete plain-text URN refines bare textable. The relation is
/// directional — refinement, not equivalence — so a generic textable
/// CONSUMER cap declaring `in = media:enc=utf-8` would correctly
/// accept a plain-text producer's output (any subtype). This is fine
/// in the consumer direction; the regression we guard against below
/// is in the producer direction.
#[test]
fn test0055_plain_text_refines_bare_textable() {
    assert!(
        conforms(PLAIN_TEXT, BARE_TEXTABLE),
        "plain-text must refine textable (it adds the plain-text marker and the txt file-type tag)"
    );
}

/// **Core regression guard.** Bare `media:enc=utf-8` does NOT conform
/// to `media:enc=utf-8;ext=txt;plain-text`. This is the relation that
/// keeps `cap:save-as-txt` from accidentally accepting every textable
/// cap's output as a feeder. If this assertion ever flips to `true`,
/// the wizard's recommended-paths surface will fill with bogus chains
/// where any textable producer reaches `.txt` persistence.
#[test]
fn test0056_bare_textable_does_not_conform_to_plain_text() {
    assert!(
        !conforms(BARE_TEXTABLE, PLAIN_TEXT),
        "media:enc=utf-8 must NOT conform to media:enc=utf-8;ext=txt;plain-text — \
         a generic textable producer is not a valid feeder for save-as-txt"
    );
}

/// OCR's narrowed output URN refines plain-text. Both carry the
/// `plain-text` marker, the `textable` coercion, and `file-type=txt`;
/// digitized-text adds the OCR-specific marker on top. Save-as-txt's
/// `in = media:enc=utf-8;ext=txt;plain-text` therefore accepts OCR output
/// without further intermediation.
#[test]
fn test0057_digitized_text_refines_plain_text() {
    assert!(
        conforms(DIGITIZED_TEXT, PLAIN_TEXT),
        "digitized-text (OCR) must refine plain-text so cap:save-as-txt \
         accepts OCR output as a feeder"
    );
}

/// Plain-text does NOT refine digitized-text — digitized-text is a
/// strict subset of plain-text (OCR-marker added). A consumer that
/// asked specifically for OCR output should NOT silently get any
/// other plain-text. This is the order-theoretic complement of the
/// previous test and catches a regression where the markers on
/// digitized-text get dropped.
#[test]
fn test0058_plain_text_does_not_refine_digitized_text() {
    assert!(
        !conforms(PLAIN_TEXT, DIGITIZED_TEXT),
        "plain-text must NOT refine digitized-text — generic plain text \
         is not a substitute for OCR output"
    );
}

/// `media:enc=utf-8;ext=txt` (the dim-anchored composite without the
/// `plain-text` marker) is NOT a substitute for `media:enc=utf-8;ext=txt;plain-text`.
/// This catches a regression where a producer cap drops the
/// `plain-text` marker and only declares the file-type=txt narrowing
/// — that producer would suddenly satisfy save-as-txt's input
/// because the catalog defines `media:enc=utf-8;ext=txt`, but the
/// `plain-text` marker is what actually gates the persistence path.
#[test]
fn test0059_textable_txt_without_plain_text_marker_does_not_satisfy() {
    assert!(
        !conforms("media:enc=utf-8;ext=txt", PLAIN_TEXT),
        "media:enc=utf-8;ext=txt (file-type narrowing only, no plain-text marker) \
         must NOT satisfy media:enc=utf-8;ext=txt;plain-text — the plain-text \
         marker is the explicit opt-in that keeps the save-as-txt path \
         scoped to producers of user-facing prose"
    );
}

/// Vision describe caps emit `media:enc=utf-8;ext=txt;image-description;plain-text`.
/// The composite carries the vision-specific `image-description` marker so
/// downstream caption-aware tools can match on the narrower URN, AND the
/// `plain-text` marker so `cap:save-as-txt` accepts captions as a feeder.
/// This test pins the second relation — drop it and vision output stops
/// being savable to `.txt`, defeating the parity with OCR.
#[test]
fn test0069_image_description_refines_plain_text() {
    assert!(
        conforms(IMAGE_DESCRIPTION, PLAIN_TEXT),
        "image-description must refine plain-text so cap:save-as-txt \
         accepts vision-describe output as a feeder"
    );
}

/// Disbind-pdf emits `media:enc=utf-8;ext=txt;page;plain-text` per page. Each
/// page item must be persistable as `.txt` — that's the user-visible
/// reason for the marker (per-page save in the Finder). If a regression
/// drops the `plain-text` marker from the disbind output URN, this test
/// flips and the Finder loses its save-each-page-as-txt path.
#[test]
fn test0070_disbind_page_refines_plain_text() {
    assert!(
        conforms(DISBIND_PAGE, PLAIN_TEXT),
        "disbind-pdf's per-page output must refine plain-text so each \
         page can be persisted via cap:save-as-txt"
    );
}

/// Transcription is a JSON record (`{text, segments, …}`), NOT finalised
/// plain prose despite carrying the `textable` coercion (the JSON is
/// representable as UTF-8). It must NOT refine plain-text — saving a
/// transcription record to a `.txt` file would dump JSON into a file
/// the user expects to be readable prose. The `record` marker is what
/// distinguishes structured-as-text from finalised-as-text. If this
/// test flips, transcription either acquired `plain-text` (a marker
/// bug) or the conformance relation regressed.
#[test]
fn test0079_transcription_does_not_refine_plain_text() {
    assert!(
        !conforms(TRANSCRIPTION, PLAIN_TEXT),
        "transcription is a JSON record — its textable face must NOT \
         silently satisfy plain-text. A `.txt` save of a transcription \
         record would dump JSON into a user-facing prose file."
    );
}
