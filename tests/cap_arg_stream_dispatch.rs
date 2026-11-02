//! Tests for the cap-arg stream URN dispatch contract used by every
//! cartridge handler that reads multiple streams from a single
//! invocation.
//!
//! Background: cap TOMLs declare cap-arg URNs with rich dim profiles
//! (e.g. `media:inference;limit;max-tokens;numeric;task;user`)
//! that carry catalog-grade semantics. Cartridge handlers, however,
//! think of arguments in their bare functional shape
//! (`media:max-tokens;numeric` — "this is a numeric textable
//! tagged max-tokens"). The two URNs are semantically the same
//! parameter but they do NOT have the same tag set — `is_equivalent`
//! returns false.
//!
//! When handlers used `is_equivalent` to dispatch incoming streams to
//! parameter slots, the rich form would silently miss the parameter
//! branches and fall through to the textable catch-all (used for the
//! prompt body). Whichever conforming stream arrived last would
//! overwrite the prompt — so the model would receive `"512"` (the
//! max-tokens default), `"0.7"` (temperature), or one of the other
//! numeric defaults as its prompt and produce a continuation of that
//! fragment: math/code-style gibberish where coherent prose was
//! expected.
//!
//! The fix is to dispatch via `conforms_to(broad_pattern)`. The rich
//! cap-arg URN conforms to the bare pattern (more tags = more
//! specific); the bare textable catch-all stays last in the
//! if-chain. These tests pin the order-theoretic relations the
//! dispatch logic now depends on — a regression that flips any of
//! them re-opens the gibberish-output bug.

use capdag::MediaUrn;

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

fn equivalent(a: &str, b: &str) -> bool {
    let ua = MediaUrn::from_string(a).unwrap();
    let ub = MediaUrn::from_string(b).unwrap();
    ua.is_equivalent(&ub).unwrap()
}

// --- Rich cap-arg URNs from the LLM text-generation cap TOMLs -----------------

const RICH_MAX_TOKENS: &str = "media:inference;limit;max-tokens;numeric;task;user";
const BARE_MAX_TOKENS: &str = "media:max-tokens;numeric";

const RICH_TEMPERATURE: &str = "media:inference;numeric;sampling;task;temperature;user";
const BARE_TEMPERATURE: &str = "media:numeric;temperature";

const RICH_TOP_P: &str = "media:inference;numeric;sampling;task;top-p;user";
const BARE_TOP_P: &str = "media:numeric;top-p";

const RICH_MAX_CONTEXT: &str = "media:inference;limit;max-context-length;model;numeric;operator";
const BARE_MAX_CONTEXT: &str = "media:max-context-length;numeric";

const RICH_BATCH_SIZE: &str = "media:batch-size;inference;limit;numeric;operator;task";
const BARE_BATCH_SIZE: &str = "media:batch-size;numeric";

const SYSTEM_PROMPT: &str = "media:enc=utf8;system-prompt";
const HF_TOKEN: &str = "media:enc=utf8;hf-token;secret";
const MODEL_SPEC_GGUF_LLM: &str = "media:enc=utf8;gguf;llm;model-spec;tokenizer-embedded-gguf";
const PAGE_TEXT: &str = "media:enc=utf8;ext=txt;page;plain-text";
const BARE_TEXTABLE: &str = "media:enc=utf8";
/// The numeric catch-all. After the textable→fmt/enc redesign there is no single
/// universal "textable" marker shared by numbers and strings: numeric params carry
/// `numeric` (and no `enc=`), text params carry `enc=utf8`. So there are now TWO
/// catch-alls — `media:enc=utf8` for text and `media:numeric` for numbers.
const BARE_NUMERIC: &str = "media:numeric";

// --- Core regression: rich URN does NOT equal bare URN, but DOES conform ----

/// **Core regression guard.** The rich and bare numeric URNs are
/// SEMANTICALLY the same parameter but their tag sets differ. A
/// handler that used `is_equivalent` here would miss the rich form
/// and route the parameter stream to the textable catch-all,
/// overwriting the prompt with `"512"`.
#[test]
fn test0080_rich_max_tokens_does_not_equal_bare_max_tokens() {
    assert!(
        !equivalent(RICH_MAX_TOKENS, BARE_MAX_TOKENS),
        "the rich form has more tags than the bare form so they MUST NOT be equivalent — \
         a handler relying on equality to dispatch this URN would silently miss it",
    );
}

/// The dispatch contract: the rich cap-arg URN MUST conform to the
/// bare handler pattern. This is what makes
/// `stream_urn.conforms_to(&max_tokens_pattern)` correctly route the
/// rich form to the max-tokens branch.
#[test]
fn test0081_rich_max_tokens_conforms_to_bare_max_tokens() {
    assert!(
        conforms(RICH_MAX_TOKENS, BARE_MAX_TOKENS),
        "rich form must conform to bare form (more tags = more specific) — \
         the cartridge handler dispatch depends on this",
    );
}

/// Symmetric: same shape for every numeric parameter the LLM caps
/// declare. Catching one but not the others would mean some params
/// land on the prompt path and others don't.
#[test]
fn test0082_all_rich_numeric_params_conform_to_their_bare_pattern() {
    let pairs: &[(&str, &str)] = &[
        (RICH_MAX_TOKENS, BARE_MAX_TOKENS),
        (RICH_TEMPERATURE, BARE_TEMPERATURE),
        (RICH_TOP_P, BARE_TOP_P),
        (RICH_MAX_CONTEXT, BARE_MAX_CONTEXT),
        (RICH_BATCH_SIZE, BARE_BATCH_SIZE),
    ];
    for (rich, bare) in pairs {
        assert!(
            conforms(rich, bare),
            "rich {} must conform to bare {}",
            rich,
            bare
        );
        assert!(
            !equivalent(rich, bare),
            "rich {} must NOT be equivalent to bare {} (test integrity check — \
             if they were equivalent the conforms_to fix would not be necessary)",
            rich,
            bare
        );
    }
}

// --- Catch-all greediness: text URNs conform to the enc=utf8 catch-all,
//     numeric URNs to the numeric catch-all, and the two do NOT cross ---

/// The prompt-body catch-all uses `conforms_to(&BARE_TEXTABLE)` and the numeric
/// slots use `conforms_to(&BARE_NUMERIC)`. After the textable→fmt/enc redesign
/// these are two SEPARATE greedy catch-alls: every UTF-8 text URN (system prompt,
/// model spec, page text, secrets) conforms to `media:enc=utf8`, and every numeric
/// parameter URN conforms to `media:numeric` — but a numeric does NOT conform to
/// `media:enc=utf8` and a text URN does NOT conform to `media:numeric`. That
/// separation is WHY the cartridge handler's if-chain must still check specific
/// patterns BEFORE either catch-all (each catch-all would otherwise swallow every
/// member of its own class).
#[test]
fn test0083_text_and_numeric_urns_conform_to_their_catch_alls() {
    // Text carriers conform to the enc=utf8 catch-all (and NOT to numeric).
    let text_carriers = [SYSTEM_PROMPT, HF_TOKEN, MODEL_SPEC_GGUF_LLM, PAGE_TEXT];
    for urn in &text_carriers {
        assert!(
            conforms(urn, BARE_TEXTABLE),
            "every UTF-8 text URN must conform to media:enc=utf8 — this is what \
             makes the text catch-all greedy and forces the if-chain order",
        );
        assert!(
            !conforms(urn, BARE_NUMERIC),
            "a text URN must NOT conform to the numeric catch-all (no shared marker)",
        );
    }

    // Numeric carriers conform to the numeric catch-all (and NOT to enc=utf8).
    let numeric_carriers = [RICH_MAX_TOKENS, RICH_TEMPERATURE, RICH_TOP_P];
    for urn in &numeric_carriers {
        assert!(
            conforms(urn, BARE_NUMERIC),
            "every numeric param URN must conform to media:numeric — the numeric \
             catch-all's greediness forces the if-chain order for numeric slots",
        );
        assert!(
            !conforms(urn, BARE_TEXTABLE),
            "a numeric URN must NOT conform to media:enc=utf8 — numerics dropped \
             the textable marker and carry no encoding tag",
        );
    }
}

/// Cross-axis: a rich numeric param does NOT conform to any other
/// rich numeric param's bare pattern. Without this property the
/// dispatch would pick the wrong branch (e.g. send a temperature
/// stream to the max-tokens slot).
#[test]
fn test0084_numeric_params_do_not_cross_match() {
    assert!(!conforms(RICH_TEMPERATURE, BARE_MAX_TOKENS));
    assert!(!conforms(RICH_MAX_TOKENS, BARE_TEMPERATURE));
    assert!(!conforms(RICH_TOP_P, BARE_MAX_TOKENS));
    assert!(!conforms(RICH_MAX_TOKENS, BARE_TOP_P));
    assert!(!conforms(MODEL_SPEC_GGUF_LLM, BARE_MAX_TOKENS));
    assert!(!conforms(SYSTEM_PROMPT, BARE_MAX_TOKENS));
    assert!(!conforms(HF_TOKEN, BARE_MAX_TOKENS));
}

/// The actual upstream prompt URN (PDF page text) does not conform
/// to any specific cap-arg pattern — it falls through to the
/// textable catch-all, where it correctly lands as the prompt body.
#[test]
fn test0085_page_text_only_matches_textable_catch_all() {
    assert!(conforms(PAGE_TEXT, BARE_TEXTABLE));
    assert!(!conforms(PAGE_TEXT, BARE_MAX_TOKENS));
    assert!(!conforms(PAGE_TEXT, BARE_TEMPERATURE));
    assert!(!conforms(PAGE_TEXT, BARE_TOP_P));
    assert!(!conforms(PAGE_TEXT, SYSTEM_PROMPT));
    assert!(!conforms(PAGE_TEXT, HF_TOKEN));
    assert!(!conforms(PAGE_TEXT, MODEL_SPEC_GGUF_LLM));
}

/// The system-prompt URN must be matched BEFORE the textable
/// catch-all; otherwise the prompt body would be the system
/// prompt's content and the actual upstream text would be
/// discarded. This test pins the conformance both ways: system
/// prompt conforms to textable (so the catch-all WOULD swallow it),
/// AND system prompt conforms to its own marker (so the dedicated
/// branch matches when it runs first).
#[test]
fn test0086_system_prompt_must_be_matched_before_textable_catch_all() {
    assert!(
        conforms(SYSTEM_PROMPT, BARE_TEXTABLE),
        "system-prompt conforms to textable — confirms the catch-all would swallow it \
         if the if-chain didn't check the system-prompt branch first",
    );
    assert!(
        conforms(SYSTEM_PROMPT, SYSTEM_PROMPT),
        "system-prompt is its own pattern — the dedicated branch matches it via conforms_to",
    );
}

/// The model-spec URN is rich but has its own dedicated branch
/// because the handler knows the canonical full URN. Verify it
/// doesn't accidentally conform to any of the parameter patterns
/// (which would route the model-spec content into a numeric slot).
#[test]
fn test0087_model_spec_does_not_conform_to_any_numeric_parameter() {
    assert!(!conforms(MODEL_SPEC_GGUF_LLM, BARE_MAX_TOKENS));
    assert!(!conforms(MODEL_SPEC_GGUF_LLM, BARE_TEMPERATURE));
    assert!(!conforms(MODEL_SPEC_GGUF_LLM, BARE_TOP_P));
    assert!(!conforms(MODEL_SPEC_GGUF_LLM, BARE_MAX_CONTEXT));
    assert!(!conforms(MODEL_SPEC_GGUF_LLM, BARE_BATCH_SIZE));
    assert!(!conforms(MODEL_SPEC_GGUF_LLM, SYSTEM_PROMPT));
    assert!(!conforms(MODEL_SPEC_GGUF_LLM, HF_TOKEN));
}
