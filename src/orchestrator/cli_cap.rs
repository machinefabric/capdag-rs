//! Single-cap CLI execution helpers.
//!
//! `capdag <alias-or-cap-urn> …` runs ONE cap as a Unix-style tool, with the
//! cap's OWN declared interface — exactly as if the cartridge were invoked
//! directly:
//!
//! - the stdin-fed arg comes from piped stdin or an input file,
//! - args with an [`ArgSource::CliFlag`] source are passed as native flags
//!   (`--model-spec hf:x/y` or `--model-spec=hf:x/y`),
//! - args with an [`ArgSource::Position`] source are passed positionally, in
//!   declared position order,
//! - any arg can also be addressed explicitly via
//!   `--arg <flag-name-or-media-urn>=<value>` (with `@file` reading bytes).
//!
//! Execution goes through the same ForEach/Collect-aware planner
//! (`build_plans_from_notation` + `execute_plan`) every other front-end uses
//! — never a hand-built plan, which would duplicate planner invariants.
//!
//! These helpers are a lib module (not bin-private) so their contracts are
//! pinned by numbered unit tests: token classification (TEST8043), notation
//! synthesis (TEST8040), and invocation-arg mapping (TEST8041).

use std::collections::HashSet;

use crate::cap::definition::{ArgSource, Cap, CapArg};
use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::MediaUrn;

/// The node names of the synthesized one-edge machine. The executor keys the
/// plan's input slot as `input_slot_{INPUT_NODE}` and extra cap arguments on
/// the edge's destination node (`OUTPUT_NODE`).
pub const SINGLE_CAP_INPUT_NODE: &str = "input";
pub const SINGLE_CAP_STEP_NAME: &str = "step";
pub const SINGLE_CAP_OUTPUT_NODE: &str = "output";

/// How the CLI's first token names a cap.
#[derive(Debug, Clone, PartialEq)]
pub enum CapToken {
    /// Contains `:` — parsed strictly as a cap URN. An invalid URN is a hard
    /// error, never re-interpreted as an alias.
    Urn(String),
    /// No `:` — resolved as a registry alias.
    Alias(String),
}

/// Classify a CLI token as cap URN vs alias. The discriminator is the
/// presence of `:` (a URN always has one; an alias never does — see the
/// alias rules in MACHINEFABRIC.md). A token with `:` that fails to parse
/// as a cap URN is a hard error naming the parse failure — it does NOT fall
/// through to alias resolution, which would mask typos in URNs.
pub fn classify_cap_token(token: &str) -> Result<CapToken, String> {
    if token.contains(':') {
        let parsed = CapUrn::from_string(token).map_err(|e| {
            format!(
                "'{token}' contains ':' so it must be a cap URN, but it does not parse as \
                 one: {e}. (Aliases never contain ':'.)"
            )
        })?;
        Ok(CapToken::Urn(parsed.to_string()))
    } else {
        Ok(CapToken::Alias(token.to_string()))
    }
}

fn has_stdin_source(arg: &CapArg) -> bool {
    arg.sources
        .iter()
        .any(|s| matches!(s, ArgSource::Stdin { .. }))
}

fn flag_names(arg: &CapArg) -> Vec<String> {
    arg.sources
        .iter()
        .filter_map(|s| match s {
            ArgSource::CliFlag { cli_flag } => Some(cli_flag.trim_start_matches('-').to_string()),
            _ => None,
        })
        .collect()
}

fn declared_position(arg: &CapArg) -> Option<usize> {
    arg.sources.iter().find_map(|s| s.position())
}

/// How a non-main arg is passed on the CLI: its first declared flag, else its
/// declared position, else the universal `--arg` form. Shared by the cap
/// interface rendering and the missing-required-options error so both name
/// an option the same way.
fn arg_invocation_form(arg: &CapArg) -> String {
    if let Some(flag) = flag_names(arg).first() {
        format!("--{flag} <value>")
    } else if let Some(position) = declared_position(arg) {
        format!("positional #{position}")
    } else {
        format!("--arg '{}=<value>'", arg.media_urn)
    }
}

/// One rendered option line: how it's passed, its media URN, its description,
/// plus a trailing qualifier (default value / optional / sequence).
fn render_option_line(arg: &CapArg) -> String {
    let mut line = format!("  {}  ({})", arg_invocation_form(arg), arg.media_urn);
    if let Some(description) = arg
        .arg_description
        .as_deref()
        .filter(|d| !d.trim().is_empty())
    {
        line.push_str(&format!(" — {}", description.trim()));
    }
    if arg.is_sequence {
        line.push_str(" [sequence]");
    }
    match &arg.default_value {
        Some(default) => line.push_str(&format!(" [default: {default}]")),
        None if !arg.required => line.push_str(" [optional]"),
        None => {}
    }
    line
}

/// Render a cap's declared interface for humans — the presentation the CLI
/// (and every other front-end) uses, structured by argument ROLE rather than
/// as a flat arg list:
///
/// - **Input** — the MAIN input (the arg whose `Stdin` source URN is the cap
///   URN's `in=` spec, by tagged-URN equivalence — see
///   [`CapArg::is_main_input`]): how the cap is fed, via piped stdin or input
///   file path(s). Validation RULE11 guarantees every non-void cap declares it.
/// - **Required options** — non-main args that are required and defaultless:
///   these MUST be supplied on every invocation.
/// - **Options** — the remaining non-main args, with their default values.
pub fn render_cap_interface(cap: &Cap) -> Result<String, String> {
    let in_spec = MediaUrn::from_string(cap.urn.in_spec()).map_err(|e| {
        format!(
            "cap {} in= URN '{}' is not a valid media URN: {e}",
            cap.urn,
            cap.urn.in_spec()
        )
    })?;

    let mut out = String::new();
    out.push_str(&format!("{}\n", cap.urn));
    if !cap.title.is_empty() {
        out.push_str(&format!("{}\n", cap.title));
    }
    let aliases = cap.get_aliases();
    if !aliases.is_empty() {
        out.push_str(&format!("Aliases: {}\n", aliases.join(", ")));
    }

    let (main_inputs, others): (Vec<&CapArg>, Vec<&CapArg>) = cap
        .get_args()
        .iter()
        .partition(|arg| arg.is_main_input(&in_spec));
    let (required, optional): (Vec<&CapArg>, Vec<&CapArg>) = others
        .into_iter()
        .partition(|arg| arg.required && arg.default_value.is_none());

    out.push_str("\nInput (piped stdin, or input file path(s)):\n");
    for arg in &main_inputs {
        let mut line = format!("  {}", arg.stream_urn());
        if let Some(description) = arg
            .arg_description
            .as_deref()
            .filter(|d| !d.trim().is_empty())
        {
            line.push_str(&format!(" — {}", description.trim()));
        }
        if arg.is_sequence {
            line.push_str(" [sequence]");
        }
        // The main input may ALSO be addressable by flag/position — stdin is
        // the defining route, the rest are conveniences worth surfacing.
        let mut extra_routes = flag_names(arg)
            .into_iter()
            .map(|flag| format!("--{flag}"))
            .collect::<Vec<_>>();
        if let Some(position) = declared_position(arg) {
            extra_routes.push(format!("positional #{position}"));
        }
        if !extra_routes.is_empty() {
            line.push_str(&format!(" (also: {})", extra_routes.join(", ")));
        }
        out.push_str(&line);
        out.push('\n');
    }

    if !required.is_empty() {
        out.push_str("\nRequired options (must be supplied):\n");
        for arg in &required {
            out.push_str(&render_option_line(arg));
            out.push('\n');
        }
    }

    if !optional.is_empty() {
        out.push_str("\nOptions:\n");
        for arg in &optional {
            out.push_str(&render_option_line(arg));
            out.push('\n');
        }
    }

    if let Some(output) = &cap.output {
        out.push_str(&format!(
            "\nOutput: {}{}\n",
            output.media_urn,
            if output.is_sequence {
                " [sequence]"
            } else {
                ""
            }
        ));
    }

    Ok(out)
}

/// Synthesize the one-edge machine notation around a resolved cap:
///
/// ```text
/// [step <full cap urn>]
/// [input -> step -> output]
/// ```
///
/// The header embeds the ALREADY-RESOLVED full cap URN, so no second
/// alias/registry lookup happens inside the parser. Requirements:
/// - exactly ONE stdin-fed arg (the single-cap CLI drives exactly one piped
///   input; zero or several are hard errors naming the cap), and
/// - a declared output.
pub fn synthesize_single_cap_notation(cap: &Cap) -> Result<String, String> {
    let stdin_count = cap
        .get_args()
        .iter()
        .filter(|a| has_stdin_source(a))
        .count();
    match stdin_count {
        1 => {}
        0 => {
            return Err(format!(
                "cap {} takes no piped (stdin) input — the single-cap CLI drives exactly \
                 one input stream. Run it inside a .machine file instead.",
                cap.urn
            ))
        }
        n => {
            return Err(format!(
                "cap {} takes {n} piped inputs — the single-cap CLI drives exactly one. \
                 Run it inside a .machine file instead.",
                cap.urn
            ))
        }
    }
    if cap.output.is_none() {
        return Err(format!(
            "cap {} declares no output — nothing for the CLI to emit.",
            cap.urn
        ));
    }
    Ok(format!(
        "[{SINGLE_CAP_STEP_NAME} {}]\n[{SINGLE_CAP_INPUT_NODE} -> {SINGLE_CAP_STEP_NAME} -> {SINGLE_CAP_OUTPUT_NODE}]",
        cap.urn
    ))
}

/// The fully mapped invocation of a single cap.
#[derive(Debug, Clone, PartialEq)]
pub struct CapInvocation {
    /// Executor arg-stream bytes: `(arg media URN, raw bytes)`.
    pub cap_arguments: Vec<(String, Vec<u8>)>,
    /// Leftover positional tokens = input file paths (for the stdin arg).
    pub input_paths: Vec<String>,
}

/// Map an invocation's remaining tokens + explicit `--arg` pairs onto the
/// cap's declared interface:
///
/// - a token starting with `-` must match a declared `cli_flag` (dashes
///   normalized); its value is the following token, or the `=`-suffix in
///   `--flag=value` form. An unknown flag is a hard error listing the cap's
///   flags and arg media URNs — never silently ignored.
/// - bare tokens first fill the cap's `Position`-declared args in declared
///   position order (that is the cap's OWN interface — exactly as when the
///   cartridge is invoked directly); tokens beyond the declared positions
///   are input file paths for the stdin arg.
/// - explicit `--arg <name-or-media-urn>=<value>` pairs address any
///   non-stdin arg (media URNs compared by equivalence, not string
///   equality); `@<path>` values read the file's bytes (binary args).
///
/// Pre-flight: every REQUIRED non-stdin arg without a default must end up
/// supplied — the error enumerates exactly what is missing and how to pass
/// it, instead of letting the cartridge fail mid-execution.
pub fn map_invocation(
    cap: &Cap,
    tokens: &[String],
    explicit_pairs: &[(String, String)],
) -> Result<CapInvocation, String> {
    let non_stdin_args: Vec<&CapArg> = cap
        .get_args()
        .iter()
        .filter(|arg| !has_stdin_source(arg))
        .collect();

    let mut cap_arguments: Vec<(String, Vec<u8>)> = Vec::new();
    let mut supplied: HashSet<String> = HashSet::new();
    let mut positionals: Vec<String> = Vec::new();

    // Pass 1: native flags + bare tokens.
    let mut idx = 0usize;
    while idx < tokens.len() {
        let token = &tokens[idx];
        if let Some(stripped) = token.strip_prefix('-') {
            let stripped = stripped.trim_start_matches('-');
            let (flag, inline_value) = match stripped.split_once('=') {
                Some((flag, value)) => (flag.to_string(), Some(value.to_string())),
                None => (stripped.to_string(), None),
            };
            let arg = resolve_flag(cap, &non_stdin_args, &flag)?;
            let value = match inline_value {
                Some(value) => value,
                None => {
                    idx += 1;
                    tokens
                        .get(idx)
                        .cloned()
                        .ok_or_else(|| format!("flag '--{flag}' requires a value"))?
                }
            };
            supplied.insert(arg.media_urn.clone());
            cap_arguments.push((arg.media_urn.clone(), value.into_bytes()));
        } else {
            positionals.push(token.clone());
        }
        idx += 1;
    }

    // Pass 2: bare tokens fill the declared positional args in position
    // order; the rest are input paths.
    let mut positional_args: Vec<&CapArg> = non_stdin_args
        .iter()
        .copied()
        .filter(|arg| declared_position(arg).is_some())
        .collect();
    positional_args.sort_by_key(|arg| declared_position(arg).expect("filtered above"));
    let mut input_paths: Vec<String> = Vec::new();
    let mut positional_iter = positionals.into_iter();
    for arg in &positional_args {
        // A positional already satisfied by a flag/--arg keeps its slot open
        // for input paths rather than double-binding.
        if supplied.contains(&arg.media_urn) {
            continue;
        }
        match positional_iter.next() {
            Some(value) => {
                supplied.insert(arg.media_urn.clone());
                cap_arguments.push((arg.media_urn.clone(), value.into_bytes()));
            }
            None => break, // required-check below reports if it mattered
        }
    }
    input_paths.extend(positional_iter);

    // Pass 3: explicit --arg pairs (flag name or media URN, @file values).
    for (name, value) in explicit_pairs {
        let arg = if name.contains(':') {
            resolve_media_urn(cap, &non_stdin_args, name)?
        } else {
            resolve_flag(cap, &non_stdin_args, name.trim_start_matches('-'))?
        };
        let bytes: Vec<u8> = if let Some(path) = value.strip_prefix('@') {
            std::fs::read(path)
                .map_err(|e| format!("--arg {name}=@{path}: failed to read file: {e}"))?
        } else {
            value.as_bytes().to_vec()
        };
        supplied.insert(arg.media_urn.clone());
        cap_arguments.push((arg.media_urn.clone(), bytes));
    }

    // Pre-flight: required, defaultless, unsupplied args are an error now,
    // not a cartridge failure later. Only the REQUIRED OPTIONS (required,
    // defaultless, non-main) are demanded — defaulted options are never
    // mentioned; each missing one is named the way the interface rendering
    // names it: invocation form, media URN, description.
    let missing: Vec<String> = non_stdin_args
        .iter()
        .filter(|arg| {
            arg.required && arg.default_value.is_none() && !supplied.contains(&arg.media_urn)
        })
        .map(|arg| render_option_line(arg))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "cap {} is missing required options:\n{}",
            cap.urn,
            missing.join("\n")
        ));
    }

    Ok(CapInvocation {
        cap_arguments,
        input_paths,
    })
}

fn resolve_flag<'a>(
    cap: &Cap,
    non_stdin_args: &[&'a CapArg],
    wanted: &str,
) -> Result<&'a CapArg, String> {
    let matches: Vec<&&CapArg> = non_stdin_args
        .iter()
        .filter(|arg| flag_names(arg).iter().any(|flag| flag == wanted))
        .collect();
    match matches.as_slice() {
        [single] => Ok(**single),
        [] => {
            let known: Vec<String> = non_stdin_args
                .iter()
                .flat_map(|a| flag_names(a).into_iter().map(|f| format!("--{f}")))
                .collect();
            Err(format!(
                "cap {} has no arg with CLI flag '--{wanted}'. Known flags: [{}]; any arg \
                 can also be addressed as --arg <media-urn>=<value>. Valid media URNs: [{}]",
                cap.urn,
                known.join(", "),
                non_stdin_args
                    .iter()
                    .map(|a| a.media_urn.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        }
        several => Err(format!(
            "flag '--{wanted}' is ambiguous: {} args of cap {} declare it; address the arg \
             by its media URN via --arg instead",
            several.len(),
            cap.urn
        )),
    }
}

fn resolve_media_urn<'a>(
    cap: &Cap,
    non_stdin_args: &[&'a CapArg],
    name: &str,
) -> Result<&'a CapArg, String> {
    let requested = MediaUrn::from_string(name)
        .map_err(|e| format!("--arg name '{name}' is not a valid media URN: {e}"))?;
    let matches: Vec<&&CapArg> = non_stdin_args
        .iter()
        .filter(|arg| {
            MediaUrn::from_string(&arg.media_urn)
                .ok()
                .and_then(|declared| declared.is_equivalent(&requested).ok())
                .unwrap_or(false)
        })
        .collect();
    match matches.as_slice() {
        [single] => Ok(**single),
        [] => Err(format!(
            "cap {} has no non-stdin arg with media URN '{name}'. Valid arg media URNs: {}",
            cap.urn,
            non_stdin_args
                .iter()
                .map(|a| a.media_urn.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )),
        several => Err(format!(
            "--arg '{name}' is ambiguous: {} args of cap {} share that media URN",
            several.len(),
            cap.urn
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cap::definition::CapOutput;

    fn arg(
        media_urn: &str,
        required: bool,
        sources: Vec<ArgSource>,
        default: Option<serde_json::Value>,
    ) -> CapArg {
        CapArg {
            media_urn: media_urn.to_string(),
            required,
            is_sequence: false,
            streaming: false,
            sources,
            arg_description: None,
            default_value: default,
            metadata: None,
        }
    }

    fn test_cap() -> Cap {
        let urn = CapUrn::from_string(
            r#"cap:in="media:ext=pdf";summarize;out="media:enc=utf-8;summary""#,
        )
        .unwrap();
        let mut cap = Cap::new(urn, "Summarize".to_string(), vec!["summarize".to_string()]);
        cap.args = vec![
            arg(
                "media:ext=pdf",
                true,
                vec![ArgSource::Stdin {
                    stdin: "media:ext=pdf".to_string(),
                }],
                None,
            ),
            arg(
                "media:enc=utf-8;model-spec",
                true,
                vec![ArgSource::CliFlag {
                    cli_flag: "--model-spec".to_string(),
                }],
                None,
            ),
            arg(
                "media:budget;numeric",
                false,
                vec![ArgSource::CliFlag {
                    cli_flag: "--budget".to_string(),
                }],
                Some(serde_json::json!(400)),
            ),
            arg(
                "media:criterion;enc=utf-8",
                false,
                vec![ArgSource::Position { position: 0 }],
                None,
            ),
        ];
        cap.output = Some(CapOutput {
            media_urn: "media:enc=utf-8;summary".to_string(),
            output_description: String::new(),
            is_sequence: false,
            streaming: false,
            metadata: None,
        });
        cap
    }

    // TEST8040: notation synthesis wraps a resolved cap in the canonical
    // one-edge machine; a cap with zero stdin args (nothing to pipe) and a
    // cap with no output are hard errors naming the cap.
    #[test]
    fn test8040_single_cap_notation_synthesis() {
        let cap = test_cap();
        let notation = synthesize_single_cap_notation(&cap).expect("one stdin arg + output");
        assert!(
            notation.starts_with(&format!("[{SINGLE_CAP_STEP_NAME} cap:")),
            "header must open with the step label and the resolved URN: {notation}"
        );
        assert!(notation.contains("summarize"), "URN must ride verbatim");
        assert!(notation.ends_with(&format!(
            "[{SINGLE_CAP_INPUT_NODE} -> {SINGLE_CAP_STEP_NAME} -> {SINGLE_CAP_OUTPUT_NODE}]"
        )));

        let mut no_stdin = test_cap();
        no_stdin.args.retain(|a| !has_stdin_source(a));
        let err = synthesize_single_cap_notation(&no_stdin).unwrap_err();
        assert!(err.contains("no piped (stdin) input"), "{err}");

        let mut no_output = test_cap();
        no_output.output = None;
        let err = synthesize_single_cap_notation(&no_output).unwrap_err();
        assert!(err.contains("declares no output"), "{err}");
    }

    // TEST8041: invocation mapping — the cap's OWN interface works exactly
    // as when invoking the cartridge directly: native declared flags (space
    // and `=` forms), declared positional args in position order (leftover
    // bare tokens become input paths), plus the explicit `--arg` addressing
    // by flag name or media URN with `@file` values; unknown flags error
    // listing candidates; a missing required defaultless arg fails the
    // pre-flight with an actionable message.
    #[test]
    fn test8041_cap_invocation_mapping() {
        let cap = test_cap();

        // Native flag (space form) + positional arg + leftover input path.
        let invocation = map_invocation(
            &cap,
            &[
                "--model-spec".to_string(),
                "hf:foo/bar".to_string(),
                "focus on costs".to_string(),
                "doc.pdf".to_string(),
            ],
            &[],
        )
        .unwrap();
        assert_eq!(
            invocation.cap_arguments,
            vec![
                (
                    "media:enc=utf-8;model-spec".to_string(),
                    b"hf:foo/bar".to_vec()
                ),
                (
                    "media:criterion;enc=utf-8".to_string(),
                    b"focus on costs".to_vec()
                ),
            ]
        );
        assert_eq!(invocation.input_paths, vec!["doc.pdf".to_string()]);

        // `--flag=value` form is equivalent.
        let eq_form = map_invocation(
            &cap,
            &[
                "--model-spec=hf:foo/bar".to_string(),
                "focus on costs".to_string(),
                "doc.pdf".to_string(),
            ],
            &[],
        )
        .unwrap();
        assert_eq!(invocation, eq_form);

        // Unknown flag: hard error listing the cap's flags and media URNs.
        let err = map_invocation(&cap, &["--bogus".to_string(), "v".to_string()], &[]).unwrap_err();
        assert!(err.contains("no arg with CLI flag '--bogus'"), "{err}");
        assert!(err.contains("--model-spec"), "{err}");

        // Explicit --arg by media URN (tag order irrelevant — equivalence)
        // and @file bytes.
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("payload.bin");
        std::fs::write(&file, b"\x00\x01binary").unwrap();
        let invocation = map_invocation(
            &cap,
            &[],
            &[(
                "media:model-spec;enc=utf-8".to_string(),
                format!("@{}", file.display()),
            )],
        )
        .unwrap();
        assert_eq!(invocation.cap_arguments[0].0, "media:enc=utf-8;model-spec");
        assert_eq!(invocation.cap_arguments[0].1, b"\x00\x01binary");

        // Missing required defaultless arg fails pre-flight naming the flag;
        // the optional defaulted `--budget` and the optional positional are
        // NOT demanded.
        let err = map_invocation(&cap, &[], &[]).unwrap_err();
        assert!(err.contains("missing required options"), "{err}");
        assert!(err.contains("--model-spec"), "{err}");
        assert!(
            err.contains("media:enc=utf-8;model-spec"),
            "missing options are named with their media URN: {err}"
        );
        assert!(
            !err.contains("budget"),
            "defaulted args not required: {err}"
        );
        assert!(
            !err.contains("criterion"),
            "optional args not required: {err}"
        );

        // A flag missing its value is a hard error, not a silent empty.
        let err = map_invocation(&cap, &["--model-spec".to_string()], &[]).unwrap_err();
        assert!(err.contains("requires a value"), "{err}");
    }

    // TEST8043: token classification — ':' means URN, strictly (an invalid
    // URN with ':' errors rather than falling through to alias); no ':'
    // means alias.
    #[test]
    fn test8043_cap_token_classification() {
        assert_eq!(
            classify_cap_token("pdf2summary").unwrap(),
            CapToken::Alias("pdf2summary".to_string())
        );
        match classify_cap_token(r#"cap:in="media:ext=pdf";summarize;out="media:enc=utf-8""#)
            .unwrap()
        {
            CapToken::Urn(urn) => assert!(urn.contains("summarize")),
            other => panic!("expected Urn, got {other:?}"),
        }
        // ':' present but not a parseable cap URN — hard error, no alias
        // fall-through.
        let err = classify_cap_token("media:ext=pdf").unwrap_err();
        assert!(err.contains("does not parse"), "{err}");
        let err = classify_cap_token("cap:no way this parses\u{7f}").unwrap_err();
        assert!(err.contains("does not parse"), "{err}");
    }
}
