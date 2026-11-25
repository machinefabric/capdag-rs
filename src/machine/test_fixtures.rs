//! Shared test fixtures for `machine/` unit tests.
//!
//! Provides helpers for building `Cap` definitions, `Strand`s, and
//! a populated `FabricRegistry` so test code in `resolve.rs`,
//! `parser.rs`, `serializer.rs`, and `graph.rs` doesn't have to
//! repeat the boilerplate. Every helper here is registered as
//! `pub(crate)` and only compiled under `#[cfg(test)]`.

use std::collections::HashMap;

use crate::cap::definition::{ArgSource, Cap, CapArg, CapOutput};
use crate::cap::registry::FabricRegistry;
use crate::planner::{ArgSourceRef, CapInput, StepToken, Strand, StrandStep, StrandStepType};
use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::MediaUrn;

/// Build a `Cap` from a string URN, a list of input arg media
/// URNs, and the output media URN. Each arg gets a stdin source
/// pointing at its own URN — slot identity and stdin URN are
/// the same. Use `build_cap_with_slot_stdin_pairs` when you
/// need to test the case where they differ (e.g. file-path
/// auto-conversion).
pub(crate) fn build_cap(
    cap_urn_str: &str,
    title: &str,
    arg_media_urns: &[&str],
    output_media_urn: &str,
) -> Cap {
    let pairs: Vec<(&str, &str)> = arg_media_urns.iter().map(|m| (*m, *m)).collect();
    build_cap_with_slot_stdin_pairs(cap_urn_str, title, &pairs, output_media_urn)
}

/// Build a `Cap` whose args declare distinct **slot identity**
/// and **stdin source URN** per arg. Each tuple is
/// `(slot_media_urn, stdin_media_urn)`. The resolver matches
/// wiring sources against the stdin URN, not the slot identity
/// — this is the regression-test path for caps like
/// `disbind-pdf` where the slot is `media:enc=utf8;file-path`
/// but the stdin source delivers `media:document;ext=pdf`.
pub(crate) fn build_cap_with_slot_stdin_pairs(
    cap_urn_str: &str,
    title: &str,
    args: &[(&str, &str)],
    output_media_urn: &str,
) -> Cap {
    let urn = CapUrn::from_string(cap_urn_str)
        .unwrap_or_else(|e| panic!("test fixture: invalid cap URN {cap_urn_str}: {e}"));
    // Each arg has a name of its own (RULE17): the first is `input`, the
    // rest `arg1`, `arg2`, …
    let arg_values: Vec<CapArg> = args
        .iter()
        .enumerate()
        .map(|(index, (slot, stdin))| {
            CapArg::new(
                slot.to_string(),
                if index == 0 { "input".to_string() } else { format!("arg{index}") },
                true,
                vec![ArgSource::Stdin {
                    stdin: stdin.to_string(),
                }],
            )
        })
        .collect();
    Cap {
        dims: Vec::new(),
        urn,
        version: 1,
        title: title.to_string(),
        cap_description: None,
        documentation: None,
        metadata: HashMap::new(),
        aliases: vec![format!("test-fixture://{title}")],
        is_abstract: false,
        args: arg_values,
        outputs: vec![CapOutput::new(
            output_media_urn.to_string(), "output",
            format!("output of {title}"),
        )],
        metadata_json: None,
        registered_by: None,
        // Test-fixture caps have no model dependency.
        supported_model_types: Vec::new(),
    }
}

/// Build a unified `FabricRegistry` pre-populated with the supplied caps.
pub(crate) fn registry_with(caps: Vec<Cap>) -> FabricRegistry {
    let registry = FabricRegistry::new_for_test();
    registry.add_caps_to_cache(caps);
    registry
}

/// Seed the supplied registry with minimal test specs covering each
/// URN string. Each spec gets a synthetic title of the form
/// `"Title for <urn>"` — enough to let the render-payload serializer
/// find a cached entry without depending on the online registry.
pub(crate) fn seed_media_titles(registry: &FabricRegistry, urns: &[&str]) {
    use crate::StoredMediaDef;
    for urn in urns {
        registry.insert_cached_media_def_for_test(StoredMediaDef {
            dims: Vec::new(),
            version: 0,
            urn: urn.to_string(),
            media_type: "application/octet-stream".to_string(),
            title: format!("Title for {urn}"),
            profile_uri: None,
            schema: None,
            description: None,
            documentation: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        });
    }
}

/// Seed the supplied registry with a dim named `name`, titled `title`, whose
/// values are `(value name, value title, tag key, tag value)`; a marker value's
/// tag value is `*`.
pub(crate) fn seed_dim(registry: &FabricRegistry, name: &str, title: &str, values: &[(&str, &str, &str, &str)]) {
    use crate::{DimTag, StoredDim, StoredDimValue};
    registry.insert_cached_dim_for_test(StoredDim {
        name: name.to_string(),
        version: 1,
        title: title.to_string(),
        documentation: format!("## {title}"),
        values: values
            .iter()
            .map(|(value, value_title, key, tag_value)| StoredDimValue {
                name: value.to_string(),
                title: value_title.to_string(),
                tags: vec![DimTag {
                    key: key.to_string(),
                    value: tag_value.to_string(),
                }],
                documentation: None,
                media_type: None,
                extensions: Vec::new(),
            })
            .collect(),
    });
}

/// Convenience: parse a media URN string. Panics on parse
/// failure with the failing literal in the message.
pub(crate) fn media(urn: &str) -> MediaUrn {
    MediaUrn::from_string(urn)
        .unwrap_or_else(|e| panic!("test fixture: invalid media URN {urn}: {e}"))
}

/// The fabric's JSON saves of a YAML record's content, by their cap URNs:
/// the generic save, which may give any JSON; the value save, which a record
/// does not reach; the record save; and the list-of-records save.
pub(crate) const SAVE_AS_JSON: &str =
    r#"cap:in="media:enc=utf8;fmt=json;!ext";out="media:enc=utf8;ext=json;fmt=json;?list;?record";save-as-json"#;
pub(crate) const SAVE_AS_JSON_VALUE: &str =
    r#"cap:in="media:enc=utf8;fmt=json;!list;!record;!ext";out="media:enc=utf8;ext=json;fmt=json";save-as-json"#;
pub(crate) const SAVE_AS_JSON_RECORD: &str =
    r#"cap:in="media:enc=utf8;fmt=json;!list;record;!ext";out="media:enc=utf8;ext=json;fmt=json;record";save-as-json"#;
pub(crate) const SAVE_AS_JSON_LIST_OF_RECORDS: &str =
    r#"cap:in="media:enc=utf8;fmt=json;list;record;!ext";out="media:enc=utf8;ext=json;fmt=json;list;record";save-as-json"#;

/// The caps a YAML record is planned to a JSON file with, as the fabric
/// declares them: YAML to JSON (a patch, keeping the record), the merge of
/// YAML mappings into a JSON array (taking a sequence), and the four JSON
/// saves; with the file types they read and write, the bookends.
pub(crate) fn json_saves() -> (Vec<Cap>, std::collections::HashSet<MediaUrn>) {
    let save = |urn: &str, title: &str| {
        let parsed = cap(urn);
        let input = parsed.in_spec().to_string();
        let output = parsed.out_spec().to_string();
        build_cap(urn, title, &[input.as_str()], &output)
    };
    let mut merge = build_cap(
        r#"cap:collect-yaml-mappings-to-json-array;in="media:enc=utf8;fmt=yaml;!list;record";out="media:enc=utf8;fmt=json;list;record""#,
        "Merge YAML Mappings into JSON Array",
        &["media:enc=utf8;fmt=yaml;!list;record"],
        "media:enc=utf8;fmt=json;list;record",
    );
    merge.args[0].is_sequence = true;
    let caps = vec![
        build_cap(
            r#"cap:convert-format;effect=patch;in="media:enc=utf8;fmt=yaml";out="media:enc=utf8;fmt=json;!ext";yaml-to-json"#,
            "Convert YAML to JSON",
            &["media:enc=utf8;fmt=yaml"],
            "media:enc=utf8;fmt=json;!ext",
        ),
        merge,
        save(SAVE_AS_JSON, "Save as JSON"),
        save(SAVE_AS_JSON_VALUE, "Save as JSON Value"),
        save(SAVE_AS_JSON_RECORD, "Save as JSON Record"),
        save(SAVE_AS_JSON_LIST_OF_RECORDS, "Save as JSON List of Records"),
    ];
    let bookends = [
        "media:enc=utf8;ext=yaml;fmt=yaml;record",
        "media:enc=utf8;ext=json;fmt=json",
        "media:enc=utf8;ext=json;fmt=json;list",
        "media:enc=utf8;ext=json;fmt=json;record",
        "media:enc=utf8;ext=json;fmt=json;list;record",
    ]
    .into_iter()
    .map(media)
    .collect();
    (caps, bookends)
}

/// Convenience: parse a cap URN string.
pub(crate) fn cap(urn: &str) -> CapUrn {
    CapUrn::from_string(urn).unwrap_or_else(|e| panic!("test fixture: invalid cap URN {urn}: {e}"))
}

/// Build a one-cap `StrandStep`. `from`/`to` are the runtime
/// data URN at this step's input and output positions; in the
/// new regime they should match the cap's declared in/out
/// patterns (or a more-specific URN that conforms).
pub(crate) fn cap_step(cap_urn_str: &str, title: &str, from: &str, to: &str) -> StrandStep {
    StrandStep::new(
        StrandStepType::Cap {
            cap_urn: cap(cap_urn_str),
            title: title.to_string(),
            specificity: 0,
            input_is_sequence: false,
            output_is_sequence: false,
            output_urn: None,
            // Single main input fed by the strand input. Chained fixtures wire the
            // predecessor via `chain_cap_steps` (below).
            inputs: vec![CapInput {
                arg_urn: media(from),
                source: ArgSourceRef::StrandInput,
            }],
        },
        media(from),
        media(to),
    )
}

/// Wire a sequence of steps into a linear chain: each cap step after the first takes
/// its single main input from the immediately preceding cap step's output (ForEach/
/// Collect steps are passed over — they are cardinality transitions, not producers).
/// Under the explicit-inputs model a chained fixture must name its predecessor rather
/// than rely on position, so fixtures that build linear strands wrap their step vec in
/// this before constructing the `Strand`.
pub(crate) fn chain_cap_steps(mut steps: Vec<StrandStep>) -> Vec<StrandStep> {
    let mut prev_cap_token: Option<StepToken> = None;
    for step in &mut steps {
        let token = step.token_id.clone();
        if let StrandStepType::Cap { inputs, .. } = &mut step.step_type {
            if let (Some(prev), Some(first)) = (&prev_cap_token, inputs.first_mut()) {
                first.source = ArgSourceRef::Step {
                    token_id: prev.clone(),
                };
            }
            prev_cap_token = Some(token);
        }
    }
    steps
}

/// Build a `ForEach` strand step.
pub(crate) fn for_each_step(media_urn: &str) -> StrandStep {
    StrandStep::new(
        StrandStepType::ForEach {
            media_def: media(media_urn),
        },
        media(media_urn),
        media(media_urn),
    )
}

/// Build a `Collect` strand step.
pub(crate) fn collect_step(media_urn: &str) -> StrandStep {
    StrandStep::new(
        StrandStepType::Collect {
            media_def: media(media_urn),
        },
        media(media_urn),
        media(media_urn),
    )
}

/// Wrap a list of steps into a `Strand`. Source/target specs
/// are taken from the first step's `from_spec` and the last
/// step's `to_spec`.
pub(crate) fn strand_from_steps(steps: Vec<StrandStep>, description: &str) -> Strand {
    // Fixtures list steps positionally; under the explicit-inputs model each cap names
    // its producer, so wire them into the linear chain these fixtures intend (a
    // straight pipeline: each cap's main input is the previous cap's output).
    let steps = chain_cap_steps(steps);
    let total_steps = steps.len() as i32;
    let cap_step_count = steps.iter().filter(|s| s.is_cap()).count() as i32;
    let source_media_urn = steps.first().expect("non-empty").from_spec.clone();
    let target_media_urn = steps.last().expect("non-empty").to_spec.clone();
    Strand {
        steps,
        source_media_urn,
        target_media_urn,
        total_steps,
        cap_step_count,
        description: description.to_string(),
    }
}
