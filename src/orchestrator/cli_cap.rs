//! A capability, as the command line shows and runs it.
//!
//! What the command line runs is always a machine; a capability runs as a
//! machine of one step, `[in -> disbind-pdf -> out]` ([`one_step_machine`]).
//! Its options are the step options a run gives with `--arg <name>=<value>`,
//! by the name its definition gives each, and [`render_cap_interface`] shows
//! them so. A command that drives one cap itself — the model commands drive
//! the model cartridge's caps — wraps it in the canonical one-step machine
//! ([`synthesize_single_cap_notation`]) and runs it through the same planner
//! every other front-end uses, never a hand-built plan.

use crate::cap::definition::{ArgSource, Cap, CapArg};
use crate::urn::media_urn::MediaUrn;

/// The node names of the synthesized one-edge machine. The executor keys the
/// plan's input slot as `input_slot_{INPUT_NODE}` and extra cap arguments on
/// the edge's destination node (`OUTPUT_NODE`).
pub const SINGLE_CAP_INPUT_NODE: &str = "input";
pub const SINGLE_CAP_STEP_NAME: &str = "step";
pub const SINGLE_CAP_OUTPUT_NODE: &str = "output";

fn has_stdin_source(arg: &CapArg) -> bool {
    arg.sources
        .iter()
        .any(|s| matches!(s, ArgSource::Stdin { .. }))
}

/// Whether a cap takes an input: its `in=` is not `media:void`.
fn takes_input(cap: &Cap) -> Result<bool, String> {
    let void = MediaUrn::from_string(crate::urn::media_urn::MEDIA_VOID).expect("MEDIA_VOID is a valid MediaUrn");
    Ok(!cap
        .urn
        .in_media_urn()
        .map_err(|e| format!("cap {} declares an invalid input: {e}", cap.urn))?
        .is_equivalent(&void))
}

/// The machine of one step of a cap, as a person writes it: `[in -> alias ->
/// out]`, with `()` for the input of a cap that takes none and for the output
/// of one that gives none. Its input is named `in` and its output `out`.
pub fn one_step_machine(cap: &Cap, alias: &str) -> Result<String, String> {
    let sources = if takes_input(cap)? { "in" } else { "()" };
    let targets = if cap.main_output().is_some() { "out" } else { "()" };
    Ok(format!("[{sources} -> {alias} -> {targets}]"))
}

/// One rendered option line: how a run gives it, its media, its description,
/// and its default, or that it is optional.
fn render_option_line(arg: &CapArg, names: &InterfaceNames<'_>) -> String {
    let mut line = format!("  --arg {}=<value>  ({})", arg.name, names.media_with_urn(&arg.media_urn));
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

/// How a cap's interface names what it shows: the cap by its alias, media by
/// theirs, and URNs only when asked for.
pub struct InterfaceNames<'a> {
    /// The cap, as a person names it.
    pub cap: String,
    /// A media URN, as a person names it.
    pub media: &'a dyn Fn(&str) -> String,
    /// Show the URNs beside the names.
    pub urns: bool,
}

impl InterfaceNames<'_> {
    fn media_with_urn(&self, urn: &str) -> String {
        let name = (self.media)(urn);
        if self.urns && name != urn {
            format!("{name} = {urn}")
        } else {
            name
        }
    }
}

/// Render a cap's declared interface for a person, by argument role:
///
/// - **Input** — the main input (the arg whose `Stdin` source URN is the cap
///   URN's `in=` spec, by tagged-URN equivalence — see
///   [`CapArg::is_main_input`]): what the step is fed.
/// - **Options it must be given** — the other args that are required and
///   have no default: a run that leaves one open is asked for it, or names it.
/// - **Options** — the rest, with their defaults.
///
/// Each option is shown as the `--arg <name>=<value>` a run gives it by.
pub fn render_cap_interface(cap: &Cap, names: &InterfaceNames<'_>) -> Result<String, String> {
    let in_spec = MediaUrn::from_string(cap.urn.in_spec()).map_err(|e| {
        format!(
            "cap {} in= URN '{}' is not a valid media URN: {e}",
            cap.urn,
            cap.urn.in_spec()
        )
    })?;

    let mut out = String::new();
    if cap.title.is_empty() {
        out.push_str(&format!("{}\n", names.cap));
    } else {
        out.push_str(&format!("{} — {}\n", names.cap, cap.title));
    }
    if names.urns {
        out.push_str(&format!("{}\n", cap.urn));
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

    if !main_inputs.is_empty() {
        out.push_str("\nInput:\n");
    }
    for arg in &main_inputs {
        let mut line = format!("  {}", names.media_with_urn(&arg.stream_urn().to_string()));
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
        out.push_str(&line);
        out.push('\n');
    }

    if !required.is_empty() {
        out.push_str("\nOptions it must be given:\n");
        for arg in &required {
            out.push_str(&render_option_line(arg, names));
            out.push('\n');
        }
    }

    if !optional.is_empty() {
        out.push_str("\nOptions:\n");
        for arg in &optional {
            out.push_str(&render_option_line(arg, names));
            out.push('\n');
        }
    }

    // The main output first; then any others, by the name a machine takes
    // them with.
    let main = cap.main_output();
    for output in main.into_iter().chain(
        cap.get_outputs()
            .iter()
            .filter(|output| main.map_or(true, |m| !std::ptr::eq(*output, m))),
    ) {
        let is_main = main.is_some_and(|m| std::ptr::eq(output, m));
        out.push_str(&format!(
            "\nOutput{}: {}{}{}\n",
            if is_main { String::new() } else { format!(" `{}`", output.name) },
            names.media_with_urn(&output.media_urn),
            if output.is_sequence { " [sequence]" } else { "" },
            if output.optional { " [may not fire]" } else { "" },
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
/// alias/registry lookup happens inside the parser.
///
/// - A cap that takes an input (`in=` is not `media:void`) must have exactly
///   ONE stdin-fed arg: the single-cap CLI drives exactly one piped input,
///   and zero or several are hard errors naming the cap. A cap that takes
///   none is a source: `() -> step -> …`, run with nothing piped in.
/// - A cap that declares an output gives it at `output`; one that declares
///   none is a sink: `… -> step -> ()`, run for what it does.
pub fn synthesize_single_cap_notation(cap: &Cap) -> Result<String, String> {
    let takes_input = takes_input(cap)?;
    let stdin_count = cap
        .get_args()
        .iter()
        .filter(|a| has_stdin_source(a))
        .count();
    let sources = match (takes_input, stdin_count) {
        (true, 1) => SINGLE_CAP_INPUT_NODE.to_string(),
        (true, 0) => {
            return Err(format!(
                "cap {} declares an input but no arg of it takes piped (stdin) input — the \
                 single-cap CLI drives exactly one input stream. Run it inside a .machine \
                 file instead.",
                cap.urn
            ))
        }
        (true, n) => {
            return Err(format!(
                "cap {} takes {n} piped inputs — the single-cap CLI drives exactly one. \
                 Run it inside a .machine file instead.",
                cap.urn
            ))
        }
        (false, 0) => "()".to_string(),
        (false, n) => {
            return Err(format!(
                "cap {} takes no input (`in=media:void`) and yet declares {n} piped (stdin) \
                 arg(s) — its definition contradicts itself",
                cap.urn
            ))
        }
    };
    let targets = if cap.main_output().is_some() {
        SINGLE_CAP_OUTPUT_NODE
    } else {
        "()"
    };
    Ok(format!(
        "[{SINGLE_CAP_STEP_NAME} {}]\n[{sources} -> {SINGLE_CAP_STEP_NAME} -> {targets}]",
        cap.urn
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::urn::cap_urn::CapUrn;
    use crate::cap::definition::CapOutput;

    fn arg(
        media_urn: &str,
        name: &str,
        required: bool,
        sources: Vec<ArgSource>,
        default: Option<serde_json::Value>,
    ) -> CapArg {
        CapArg {
            media_urn: media_urn.to_string(),
            name: name.to_string(),
            accepts: None,
            required,
            is_sequence: false,
            streaming: false,
            sources,
            arg_description: None,
            default_value: default,
            metadata: None,
        }
    }

    /// Names that show every media type by its URN, as a cap with no fabric
    /// to name it by is shown.
    fn urn_names() -> InterfaceNames<'static> {
        fn same(urn: &str) -> String {
            urn.to_string()
        }
        InterfaceNames { cap: "summarize".to_string(), media: &same, urns: false }
    }

    fn test_cap() -> Cap {
        let urn = CapUrn::from_string(
            r#"cap:in="media:document;ext=pdf";summarize;out="media:enc=utf8;summary""#,
        )
        .unwrap();
        let mut cap = Cap::new(urn, "Summarize".to_string(), vec!["summarize".to_string()]);
        cap.args = vec![
            arg(
                "media:document;ext=pdf",
                "document",
                true,
                vec![ArgSource::Stdin {
                    stdin: "media:document;ext=pdf".to_string(),
                }],
                None,
            ),
            arg(
                "media:enc=utf8;model-spec",
                "model-spec",
                true,
                vec![ArgSource::CliFlag {
                    cli_flag: "--model-spec".to_string(),
                }],
                None,
            ),
            arg(
                "media:budget;numeric",
                "budget",
                false,
                vec![ArgSource::CliFlag {
                    cli_flag: "--budget".to_string(),
                }],
                Some(serde_json::json!(400)),
            ),
            arg(
                "media:criterion;enc=utf8",
                "criterion",
                false,
                vec![ArgSource::Position { position: 0 }],
                None,
            ),
        ];
        cap.outputs = vec![CapOutput {
            media_urn: "media:enc=utf8;summary".to_string(),
            name: "output".to_string(),
            optional: false,
            carries: None,
            output_description: String::new(),
            is_sequence: false,
            streaming: false,
            metadata: None,
        }];
        cap
    }

    // TEST8071: a cap runs as a machine of one step, written as a person
    // writes it: its input `in` and output `out`, `()` where it takes or
    // gives nothing.
    #[test]
    fn test8071_one_step_machine() {
        let cap = test_cap();
        assert_eq!(one_step_machine(&cap, "summarize").unwrap(), "[in -> summarize -> out]");
        let mut sink = test_cap();
        sink.outputs.clear();
        assert_eq!(one_step_machine(&sink, "summarize").unwrap(), "[in -> summarize -> ()]");
        let mut source = test_cap();
        source.urn = crate::CapUrn::from_string(&format!(r#"cap:in="media:void";now;out="{}""#, source.urn.out_spec())).unwrap();
        assert_eq!(one_step_machine(&source, "now").unwrap(), "[() -> now -> out]");
    }

    // TEST8072: a cap's options are shown as the `--arg <name>=<value>` a run
    // gives them by: those it must be given apart from those with a default.
    #[test]
    fn test8072_options_are_shown_as_a_run_gives_them() {
        let text = render_cap_interface(&test_cap(), &urn_names()).unwrap();
        let must = text.find("Options it must be given:").expect("a required option is shown apart");
        let rest = text.find("\nOptions:\n").expect("the other options are shown");
        assert!(must < rest, "{text}");
        assert!(!text.contains("positional #"), "a cap's own command line is not shown: {text}");
        for line in text.lines().filter(|l| l.trim_start().starts_with("--")) {
            assert!(line.trim_start().starts_with("--arg "), "every option is given by --arg: {line}");
        }
    }

    // TEST8040: notation synthesis wraps a resolved cap in the canonical
    // one-edge machine. A cap that takes no input is wired as a source and
    // one that gives no output as a sink; a cap that declares an input with
    // no arg to pipe it into, and one whose definition contradicts itself,
    // are hard errors naming the cap.
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

        // A cap that takes an input, with no arg to pipe it into.
        let mut no_stdin = test_cap();
        no_stdin.args.retain(|a| !has_stdin_source(a));
        let err = synthesize_single_cap_notation(&no_stdin).unwrap_err();
        assert!(err.contains("no arg of it takes piped (stdin) input"), "{err}");

        // A cap that gives nothing is run for what it does: a sink.
        let mut no_output = test_cap();
        no_output.outputs.clear();
        let notation = synthesize_single_cap_notation(&no_output).unwrap();
        assert!(
            notation.ends_with(&format!("[{SINGLE_CAP_INPUT_NODE} -> {SINGLE_CAP_STEP_NAME} -> ()]")),
            "{notation}"
        );

        // A cap that takes nothing is a source: nothing is piped into it.
        let mut source = test_cap();
        source.args.retain(|a| !has_stdin_source(a));
        source.urn = crate::CapUrn::from_string(&format!(
            r#"cap:in="media:void";now;out="{}""#,
            source.urn.out_spec()
        ))
        .unwrap();
        let notation = synthesize_single_cap_notation(&source).unwrap();
        assert!(
            notation.ends_with(&format!("[() -> {SINGLE_CAP_STEP_NAME} -> {SINGLE_CAP_OUTPUT_NODE}]")),
            "{notation}"
        );
        // ...and one that says it takes nothing while declaring a piped arg
        // contradicts itself.
        let mut contradictory = test_cap();
        contradictory.urn = source.urn.clone();
        let err = synthesize_single_cap_notation(&contradictory).unwrap_err();
        assert!(err.contains("contradicts itself"), "{err}");
    }
}
