//! `capdag run <cap|machine> [inputs…]`.
//!
//! One command runs a capability or a machine. A cap is run inside a full
//! host with the bundled cartridges beside it, so its peer calls work, through
//! its own declared interface: its input from files or stdin, its options as
//! it names them (`--index-range 1-5`) or `--arg <name>=<value>`. A machine's
//! inputs are addressed by the names its notation gives them: a path goes to
//! the input its media fits, `--input <name>=<value>` says which.
//!
//! Each input file of a cap — and each path given a machine with one input —
//! is a run of its own. While the runs go they are shown ([`super::live`]);
//! Ctrl-C stops them (nothing new starts, what started finishes and is
//! written), Ctrl-C again aborts (the cartridges are cancelled and given
//! [`ABORT_GRACE`] to end), and a third Ctrl-C during the grace ends them at
//! once. What each does is the model's decision ([`RunControl`]).

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use super::ctx::{self, Ctx};
use super::error::{CliError, CliResult, Exit};
use super::inputs;
use super::live::{Key, ProgressChoice, Shower};
use super::names;
use super::watch::Watch;
use crate::fabric::registry::FabricRegistry;
use crate::orchestrator::cli_cap::{
    classify_cap_token, map_invocation, render_cap_interface, synthesize_single_cap_notation,
    CapToken, InterfaceNames, SINGLE_CAP_INPUT_NODE,
};
use crate::orchestrator::cli_runtime::CliRuntime;
use crate::orchestrator::cli_session::{
    finish_strand, spawn_strand, EmitTarget, EmittedOutput, PlannedStrand, StrandFailure, StrandNote,
    StrandReport,
};
use crate::orchestrator::machine_plan::{ResolvedNotation, RunInputs, StrandPlan};
use crate::orchestrator::observe::{RunKey, RunObserver};
use crate::bifaci::live_feed::{StopInputsError, StopInputsOutcome};
use crate::orchestrator::run_control::{join_outcomes, RunAct, RunAsk, RunControl, RunOutcome, ABORT_GRACE};
use crate::orchestrator::PlanInput;
use crate::planner::ExecutionNodeType;
use crate::{Cap, ExecutionError, MediaUrn};

#[derive(Debug, clap::Args)]
pub struct Args {
    /// A capability, by its alias or URN, or a machine file
    #[arg(value_name = "CAP|MACHINE")]
    pub target: String,
    /// Files, directories or globs to run on; for a cap, also its own options
    #[arg(value_name = "INPUTS", last = true)]
    pub rest: Vec<String>,
    /// Write results into this directory (default: the working directory)
    #[arg(short, long, value_name = "DIR")]
    pub output: Option<PathBuf>,
    /// Overwrite result files that exist
    #[arg(long)]
    pub force: bool,
    /// A machine's input by its name in the notation; `@<path>` reads a file (repeatable)
    #[arg(long = "input", value_name = "NAME=VALUE")]
    pub inputs: Vec<String>,
    /// A cap's option by its name or media URN; `@<path>` reads a file (repeatable)
    #[arg(long = "arg", value_name = "NAME=VALUE")]
    pub args: Vec<String>,
    /// For a generic cap such as convert-image: the type to make
    #[arg(long, value_name = "TYPE")]
    pub to: Option<String>,
    /// For a live source: what to capture, as JSON (`{"duration_ms":5000}`)
    #[arg(long, value_name = "JSON")]
    pub selector: Option<String>,
    /// How the run is shown while it goes
    #[arg(long, value_enum, default_value_t = ProgressChoice::Auto, value_name = "HOW")]
    pub progress: ProgressChoice,
    /// Write each segment's protocol trace to this file (JSON Lines)
    #[arg(long, value_name = "FILE")]
    pub trace: Option<PathBuf>,
    /// Use this cartridge binary, or every binary in this directory (repeatable)
    #[arg(long = "dev-bin", value_name = "PATH")]
    pub dev_bins: Vec<PathBuf>,
}

/// Put `run`'s own flags before its target and everything else after `--`.
///
/// A cap is run through its own declared interface: `capdag run disbind-pdf
/// report.pdf --index-range 1-5`. Its options are not `run`'s, and only the
/// cap knows them, so before the command line is parsed every token after
/// `run` that is not one of `run`'s flags (or a global one), nor the target,
/// is moved after `--` in the order given, where the cap's interface — or a
/// machine's input binding — reads it. `--help` after a target is the
/// target's: a cap's interface, a machine's inputs and steps.
///
/// The command is built first: clap settles how many values an argument takes
/// only when it builds, so an unbuilt `-o` reads as a switch and its value
/// would be moved to the cap.
pub fn separate_cap_tokens(mut command: clap::Command, argv: Vec<String>) -> Vec<String> {
    command.build();
    let Some(run_at) = argv.iter().position(|a| a == "run") else {
        return argv;
    };
    // `run` is the subcommand only when everything before it is a global flag.
    let globals: Vec<&clap::Arg> = command.get_arguments().filter(|a| a.is_global_set()).collect();
    if !before_is_global(&argv[1..run_at], &globals) {
        return argv;
    }
    let Some(run) = command.find_subcommand("run") else {
        return argv;
    };
    let flags: Vec<&clap::Arg> = run.get_arguments().chain(globals.iter().copied()).collect();
    let mut kept: Vec<String> = argv[..=run_at].to_vec();
    let mut moved: Vec<String> = Vec::new();
    let mut target_seen = false;
    let mut tokens = argv[run_at + 1..].iter();
    while let Some(token) = tokens.next() {
        if token == "--" {
            moved.extend(tokens.by_ref().cloned());
            break;
        }
        if (token == "--help" || token == "-h") && target_seen {
            moved.push(token.clone());
            continue;
        }
        if let Some(arg) = known_flag(token, &flags) {
            kept.push(token.clone());
            if takes_value(arg) && !token.contains('=') && !is_attached_short(token, arg) {
                if let Some(value) = tokens.next() {
                    kept.push(value.clone());
                }
            }
            continue;
        }
        if !token.starts_with('-') && !target_seen {
            kept.push(token.clone());
            target_seen = true;
            continue;
        }
        moved.push(token.clone());
    }
    if !moved.is_empty() {
        kept.push("--".to_string());
        kept.extend(moved);
    }
    kept
}

fn before_is_global(tokens: &[String], globals: &[&clap::Arg]) -> bool {
    let mut i = 0;
    while i < tokens.len() {
        match known_flag(&tokens[i], globals) {
            Some(arg) => {
                i += if takes_value(arg) && !tokens[i].contains('=') { 2 } else { 1 };
            }
            None => return false,
        }
    }
    true
}

fn known_flag<'a>(token: &str, flags: &[&'a clap::Arg]) -> Option<&'a clap::Arg> {
    if let Some(long) = token.strip_prefix("--") {
        let name = long.split('=').next().unwrap_or(long);
        return flags.iter().copied().find(|a| a.get_long() == Some(name));
    }
    let mut chars = token.strip_prefix('-')?.chars();
    let short = chars.next()?;
    let arg = flags.iter().copied().find(|a| a.get_short() == Some(short))?;
    // `-vv` is two of a counting flag; `-odir` a short with its value attached.
    let rest: String = chars.collect();
    if rest.is_empty() || takes_value(arg) || rest.chars().all(|c| c == short) {
        Some(arg)
    } else {
        None
    }
}

fn takes_value(arg: &clap::Arg) -> bool {
    arg.get_num_args().map(|n| n.takes_values()).unwrap_or(false)
}

fn is_attached_short(token: &str, arg: &clap::Arg) -> bool {
    !token.starts_with("--") && arg.get_short().is_some() && token.chars().count() > 2
}

/// What `run` was given to run.
enum Target {
    Cap { cap: Cap, dev_dir: Option<PathBuf> },
    Machine { path: PathBuf, notation: String },
}

/// One run: what it is of, the name its outputs are written under, and its
/// strands, planned.
struct PlannedRun {
    label: String,
    stem: String,
    strands: Vec<PlannedStrand>,
}

pub async fn run(ctx: &Ctx, args: Args) -> CliResult<Exit> {
    let registry = ctx.registry().await?;
    let target = resolve_target(ctx, &registry, &args.target).await?;
    let wants_help = args.rest.iter().any(|a| a == "--help" || a == "-h");
    let (runs, single_scalar) = match &target {
        Target::Cap { cap, .. } => {
            if wants_help {
                let media = |urn: &str| names::media_urn(&registry, urn);
                let interface = InterfaceNames { cap: names::cap(&registry, cap), media: &media, urns: ctx.term.verbosity >= 1 };
                let text = render_cap_interface(cap, &interface).map_err(|e| {
                    CliError::failed(format!("the interface of '{}' could not be shown", args.target)).context(e)
                })?;
                print!("{text}");
                println!(
                    "\nExample:\n  capdag run {} <input> [options] [-o <dir>]",
                    names::cap(&registry, cap)
                );
                return Ok(Exit::Ok);
            }
            let cap = narrow_if_abstract(ctx, &registry, cap.clone(), &args).await?;
            let runs = plan_cap_runs(&registry, &cap, &args).await?;
            let single = runs.len() == 1;
            (runs, single)
        }
        Target::Machine { path, notation } => {
            if wants_help {
                return super::check::show(ctx, &registry, path, notation).await;
            }
            let runs = plan_machine_runs(&registry, path, notation, &args).await?;
            let single = runs.len() == 1 && runs[0].strands.len() == 1;
            (runs, single)
        }
    };
    let dev_dir = match &target {
        Target::Cap { dev_dir, .. } => dev_dir.clone(),
        Target::Machine { .. } => None,
    };
    if ctx.term.json {
        // Under --json stdout is records only: every result is written as a
        // file, a single value too, and the files are named in each run's
        // `run_finished` record rather than listed.
        return execute(ctx, &registry, &args, runs, dev_dir, false, &mut std::io::sink()).await;
    }
    execute(ctx, &registry, &args, runs, dev_dir, single_scalar, &mut std::io::stdout()).await
}

/// Run one cap on one value, and return what it gave: how the model commands
/// drive the model cartridge's own caps. The run is shown as any run is.
pub async fn cap_value(ctx: &Ctx, alias: &str, input: Option<Vec<u8>>, progress: ProgressChoice) -> CliResult<Vec<u8>> {
    let registry = ctx.registry().await?;
    let cap = registry.get_cap(alias).await.map_err(|e| {
        CliError::failed(format!("the capability {alias} is not in this fabric")).context(e.to_string())
    })?;
    let notation = synthesize_single_cap_notation(&cap)
        .map_err(|e| CliError::failed(format!("'{alias}' cannot be run on its own")).context(e))?;
    let resolved = ResolvedNotation::resolve(&notation, registry.clone())
        .await
        .map_err(|e| CliError::failed(format!("'{alias}' could not be planned")).context(e.to_string()))?;
    let strand = resolved
        .plan(0, &RunInputs::declared())
        .await
        .map_err(|e| CliError::failed(format!("'{alias}' could not be planned")).context(e.to_string()))?;
    let mut plan_inputs = HashMap::new();
    let mut is_sequence = HashMap::new();
    if let (Some(slot), Some(value)) = (strand.input_slots().get(SINGLE_CAP_INPUT_NODE).cloned(), input) {
        plan_inputs.insert(slot.clone(), PlanInput::Bytes(value));
        is_sequence.insert(slot, false);
    }
    let run = PlannedRun {
        label: alias.to_string(),
        stem: inputs::file_safe(alias),
        strands: vec![PlannedStrand { strand, inputs: plan_inputs, is_sequence, cap_arguments: HashMap::new() }],
    };
    let args = Args {
        target: alias.to_string(),
        rest: Vec::new(),
        output: None,
        force: false,
        inputs: Vec::new(),
        args: Vec::new(),
        to: None,
        selector: None,
        progress,
        trace: None,
        dev_bins: Vec::new(),
    };
    let mut value: Vec<u8> = Vec::new();
    match execute(ctx, &registry, &args, vec![run], None, true, &mut value).await? {
        Exit::Run(RunOutcome::Completed) => Ok(value),
        other => Err(CliError { exit: other, ..CliError::failed(format!("{alias} did not complete")) }),
    }
}

/// A target names a machine file when it is a file; otherwise a capability.
async fn resolve_target(ctx: &Ctx, registry: &Arc<FabricRegistry>, token: &str) -> CliResult<Target> {
    let path = Path::new(token);
    if path.is_file() {
        let notation = std::fs::read_to_string(path).map_err(|e| {
            CliError::failed(format!("the machine file '{token}' could not be read")).context(e.to_string())
        })?;
        return Ok(Target::Machine { path: path.to_path_buf(), notation });
    }
    if token.ends_with(".machine") {
        return Err(CliError::usage(format!("the machine file '{token}' does not exist"))
            .hint("check the path; `capdag plan <files> --to <type> --save <file>` writes one"));
    }
    let reference = match classify_cap_token(token) {
        Ok(CapToken::Urn(urn)) => urn,
        Ok(CapToken::Alias(alias)) => alias,
        Err(e) => return Err(CliError::usage(format!("'{token}' is neither a capability nor a file")).context(e)),
    };
    if let Ok(cap) = registry.get_cap(&reference).await {
        return Ok(Target::Cap { cap, dev_dir: None });
    }
    // A cap the fabric does not define may be a dev cartridge's, run by its
    // alias before it is published — when none of its aliases means another
    // cap upstream.
    let cartridges = ctx::user_cartridge_dir()?;
    match crate::dev::find_dev_cap_by_alias(&cartridges, &reference) {
        Ok(Some((cap, dir))) => {
            crate::dev::check_no_fabric_conflict(registry, &cap)
                .await
                .map_err(|e| CliError::failed(e.to_string()))?;
            if ctx.term.verbosity >= 0 && !ctx.term.json {
                eprintln!("'{token}' is not in the fabric: running the dev cartridge at {}", dir.display());
            }
            registry.add_caps_to_cache(vec![cap.clone()]);
            Ok(Target::Cap { cap, dev_dir: Some(dir) })
        }
        Ok(None) => Err(unknown_cap(registry, token, "capability or machine file").await),
        Err(e) => Err(CliError::failed(format!("the dev cartridges could not be read for '{token}'"))
            .context(e.to_string())),
    }
}

/// No capability has this name: say which ones are near it. `what` is what
/// the name was looked for as: "capability or machine file".
pub async fn unknown_cap(registry: &FabricRegistry, token: &str, what: &str) -> CliError {
    let aliases: Vec<String> = registry
        .get_cached_caps()
        .await
        .map(|caps| caps.iter().flat_map(|c| c.aliases.clone()).collect())
        .unwrap_or_default();
    let mut error = CliError::usage(format!("there is no {what} named '{token}'"));
    let mut near: Vec<(f64, &String)> = aliases
        .iter()
        .map(|a| (strsim::jaro_winkler(token, a), a))
        .filter(|(score, _)| *score > 0.75)
        .collect();
    near.sort_by(|a, b| b.0.total_cmp(&a.0));
    if !near.is_empty() {
        let names: Vec<&str> = near.iter().take(3).map(|(_, a)| a.as_str()).collect();
        error = error.context(format!("did you mean {}?", names.join(", ")));
    }
    error.hint(format!("find capabilities with: capdag caps {token}"))
}

/// A generic cap (`convert-image`) is narrowed to the concrete one for its
/// input's type, and `--to`'s.
async fn narrow_if_abstract(ctx: &Ctx, registry: &Arc<FabricRegistry>, cap: Cap, args: &Args) -> CliResult<Cap> {
    let alias = names::cap(registry, &cap);
    if !cap.is_abstract() {
        if args.to.is_some() {
            return Err(CliError::usage(format!("--to is for a generic cap, and '{alias}' is a specific one"))
                .hint("leave out --to"));
        }
        return Ok(cap);
    }
    let input = args
        .rest
        .iter()
        .filter(|t| !t.starts_with('-'))
        .find_map(|t| inputs::expand(t).ok().and_then(|files| files.into_iter().next()))
        .ok_or_else(|| {
            CliError::usage(format!("'{alias}' is generic: it needs an input file to tell which cap to run"))
                .context("piped input has no type to tell it by")
                .hint(format!("capdag run {alias} <file> --to <type>"))
        })?;
    let media = inputs::detect(&input, registry)?;
    let target = match &args.to {
        Some(to) => Some(super::plan::target_media(registry, to).await?),
        None => None,
    };
    let concrete = registry
        .narrow_abstract_cap(&cap.urn, &media, target.as_ref())
        .await
        .map_err(|e| {
            CliError::failed(format!(
                "'{alias}' could not choose the cap to run for {}",
                inputs::describe(&input, &media, registry)
            ))
            .context(e.to_string())
            .hint(format!(
                "say what to make with --to <type>, or see what it can become: capdag plan {}",
                input.display()
            ))
        })?;
    let concrete = registry.get_cap(&concrete.to_string()).await.map_err(|e| {
        CliError::failed(format!("'{alias}' was narrowed to a cap the fabric does not hold")).context(e.to_string())
    })?;
    // What actually runs is said, as the generic name chose it.
    if ctx.term.verbosity >= 0 && !ctx.term.json {
        eprintln!("{alias} → {} (for {})", names::cap(registry, &concrete), inputs::describe(&input, &media, registry));
    }
    Ok(concrete)
}

/// What a cap's input is.
enum CapInput {
    Files(Vec<PathBuf>),
    Stdin(Vec<u8>),
    Live(String),
    Nothing,
}

/// The runs of one cap: one per input file, or one on stdin, a live source,
/// or nothing.
async fn plan_cap_runs(registry: &Arc<FabricRegistry>, cap: &Cap, args: &Args) -> CliResult<Vec<PlannedRun>> {
    let alias = names::cap(registry, cap);
    let notation = synthesize_single_cap_notation(cap)
        .map_err(|e| CliError::failed(format!("'{alias}' cannot be run on its own")).context(e))?;
    let mut explicit: Vec<(String, String)> = Vec::new();
    for given in &args.args {
        let (name, value) = inputs::pair("--arg", given)?;
        explicit.push((name.to_string(), value.to_string()));
    }
    let mut tokens = args.rest.clone();
    // `--input` names a cap's one input as `input`.
    for given in &args.inputs {
        let (name, value) = inputs::pair("--input", given)?;
        if name != SINGLE_CAP_INPUT_NODE {
            return Err(CliError::usage(format!("a cap has one input, '{SINGLE_CAP_INPUT_NODE}', and --input named '{name}'"))
                .hint(format!("give its input as a path, or --input {SINGLE_CAP_INPUT_NODE}=@<path>")));
        }
        match value.strip_prefix('@') {
            Some(path) => tokens.push(path.to_string()),
            None => explicit.push((SINGLE_CAP_INPUT_NODE.to_string(), value.to_string())),
        }
    }
    let media = |urn: &str| names::media_urn(registry, urn);
    let interface = InterfaceNames { cap: alias.clone(), media: &media, urns: false };
    let invocation = map_invocation(cap, &tokens, &explicit, &interface).map_err(|e| {
        CliError::usage(format!("'{alias}' was given an option it does not have"))
            .context(e)
            .hint(format!("its options: capdag run {alias} --help"))
    })?;
    let takes_input = notation.contains(&format!("[{SINGLE_CAP_INPUT_NODE} -> "));
    if !takes_input && !invocation.input_paths.is_empty() {
        return Err(CliError::usage(format!(
            "'{alias}' takes no input, and was given {}",
            invocation.input_paths.join(", ")
        ))
        .hint(format!("run it on nothing: capdag run {alias}")));
    }
    let live: Vec<&String> = invocation
        .input_paths
        .iter()
        .filter(|t| MediaUrn::from_string(t).map(|u| u.is_live_feed()).unwrap_or(false))
        .collect();
    if live.len() > 1 || (!live.is_empty() && invocation.input_paths.len() > live.len()) {
        return Err(CliError::usage("a cap's run takes one live source, and no files beside it"));
    }
    let input = if !takes_input {
        CapInput::Nothing
    } else if let Some(reference) = live.first() {
        CapInput::Live((*reference).clone())
    } else if invocation.input_paths.is_empty() {
        use std::io::IsTerminal;
        if std::io::stdin().is_terminal() {
            return Err(CliError::usage(format!("'{alias}' needs an input"))
                .context(format!("it takes {}", input_media_name(registry, cap)))
                .hint(format!("give a file: capdag run {alias} <file>, or pipe one in: cat <file> | capdag run {alias}")));
        }
        let mut bytes = Vec::new();
        use std::io::Read;
        std::io::stdin()
            .read_to_end(&mut bytes)
            .map_err(|e| CliError::failed("stdin could not be read").context(e.to_string()))?;
        if bytes.is_empty() {
            return Err(CliError::usage(format!("stdin was empty: there is nothing to run '{alias}' on")));
        }
        CapInput::Stdin(bytes)
    } else {
        let mut files = Vec::new();
        for path in &invocation.input_paths {
            files.extend(inputs::expand(path)?);
        }
        files.sort();
        CapInput::Files(files)
    };
    let selector = selector_bytes(args.selector.as_deref(), matches!(input, CapInput::Live(_)))?;

    let resolved = ResolvedNotation::resolve(&notation, registry.clone())
        .await
        .map_err(|e| CliError::failed(format!("'{alias}' could not be planned")).context(e.to_string()))?;
    let run_inputs = match &input {
        CapInput::Live(_) => RunInputs {
            sequences: std::iter::once(SINGLE_CAP_INPUT_NODE.to_string()).collect(),
            ..RunInputs::default()
        },
        _ => RunInputs::declared(),
    };
    let strand = resolved
        .plan(0, &run_inputs)
        .await
        .map_err(|e| CliError::failed(format!("'{alias}' could not be planned")).context(e.to_string()))?;
    let slot = strand.input_slots().get(SINGLE_CAP_INPUT_NODE).cloned();
    let cap_node = strand
        .plan
        .nodes
        .iter()
        .find(|(_, n)| matches!(n.node_type, ExecutionNodeType::Cap { .. }))
        .map(|(id, _)| id.clone())
        .ok_or_else(|| CliError::failed(format!("the plan of '{alias}' has no step")))?;
    let mut cap_arguments: HashMap<String, Vec<(String, Vec<u8>)>> = HashMap::new();
    if !invocation.cap_arguments.is_empty() {
        cap_arguments.insert(cap_node, invocation.cap_arguments.clone());
    }
    let one = |label: String, stem: String, given: Option<(PlanInput, bool)>| -> PlannedRun {
        let mut plan_inputs = HashMap::new();
        let mut is_sequence = HashMap::new();
        if let (Some(slot), Some((value, sequence))) = (&slot, given) {
            plan_inputs.insert(slot.clone(), value);
            is_sequence.insert(slot.clone(), sequence);
        }
        PlannedRun {
            label,
            stem,
            strands: vec![PlannedStrand {
                strand: strand.clone(),
                inputs: plan_inputs,
                is_sequence,
                cap_arguments: cap_arguments.clone(),
            }],
        }
    };
    let runs = match input {
        CapInput::Nothing => vec![one(alias.clone(), inputs::file_safe(&alias), None)],
        CapInput::Stdin(bytes) => vec![one(format!("{alias} on stdin"), "stdin".into(), Some((PlanInput::Bytes(bytes), false)))],
        CapInput::Live(reference) => vec![one(
            format!("{alias} on {reference}"),
            inputs::file_safe(&reference),
            Some((PlanInput::LiveReference { reference_urn: reference.clone(), selector }, true)),
        )],
        CapInput::Files(files) => {
            let mut runs = Vec::with_capacity(files.len());
            for file in files {
                let bytes = inputs::read(&file)?;
                runs.push(one(file.display().to_string(), inputs::stem(&file), Some((PlanInput::Bytes(bytes), false))));
            }
            runs
        }
    };
    Ok(runs)
}

fn input_media_name(registry: &FabricRegistry, cap: &Cap) -> String {
    cap.urn
        .in_media_urn()
        .map(|media| names::media(registry, &media))
        .unwrap_or_else(|_| "an input".to_string())
}

fn selector_bytes(given: Option<&str>, live: bool) -> CliResult<Vec<u8>> {
    match given {
        None => Ok(b"{}".to_vec()),
        Some(_) if !live => Err(CliError::usage("--selector is for a live source, and none was given")
            .hint("give a live source such as media:audio;live;microphone, or leave out --selector")),
        Some(raw) => {
            crate::bifaci::live_feed::LiveFeedSelector::parse(raw.as_bytes()).map_err(|e| {
                CliError::usage("--selector is not a valid live-feed selector")
                    .context(e.to_string())
                    .hint("for example: --selector '{\"duration_ms\":5000}'")
            })?;
            Ok(raw.as_bytes().to_vec())
        }
    }
}

/// What one input of a machine is given for one run.
#[derive(Clone)]
enum Given {
    /// One or more values in the order given: one is the value, several a sequence.
    Values(Vec<Vec<u8>>),
    /// A live source.
    Live(String),
}

/// The runs of a machine: one per path when it has one input given several
/// paths and nothing else, else one in which an input given several paths
/// takes them as a sequence.
async fn plan_machine_runs(
    registry: &Arc<FabricRegistry>,
    path: &Path,
    notation: &str,
    args: &Args,
) -> CliResult<Vec<PlannedRun>> {
    let label = path.display().to_string();
    let resolved = super::check::resolve(registry, path, notation).await?;
    let (machine_inputs, _outputs) = match (resolved.inputs(), resolved.outputs()) {
        (Ok(i), Ok(o)) => (i, o),
        (Err(e), _) | (_, Err(e)) => {
            return Err(CliError::failed(format!("{label} is not a machine that can run")).context(e.to_string()))
        }
    };
    let input_names = || -> String {
        if machine_inputs.is_empty() {
            return "it has none".to_string();
        }
        machine_inputs
            .iter()
            .map(|i| format!("{} ({})", i.name, names::media(registry, &i.media_urn)))
            .collect::<Vec<_>>()
            .join(", ")
    };
    // Partition: live sources, and paths.
    let mut live_sources: Vec<(String, MediaUrn)> = Vec::new();
    let mut files: Vec<PathBuf> = Vec::new();
    for token in &args.rest {
        if token.starts_with('-') {
            return Err(CliError::usage(format!("a machine run has no option '{token}'"))
                .hint("a machine's steps are configured in the machine; see `capdag run --help` for run's own flags"));
        }
        if let Ok(urn) = MediaUrn::from_string(token) {
            if urn.is_live_feed() {
                let content = registry
                    .live_source_content_urn(&urn)
                    .await
                    .map_err(|e| CliError::failed(format!("the live source '{token}' could not be looked up")).context(e.to_string()))?
                    .ok_or_else(|| {
                        CliError::failed(format!("'{token}' is a live reference the fabric defines no source for"))
                            .hint("check the reference; `capdag info <alias>` shows a media type")
                    })?;
                let content = MediaUrn::from_string(&content)
                    .map_err(|e| CliError::failed(format!("the fabric's content for '{token}' is no media URN")).context(e.to_string()))?;
                live_sources.push((urn.to_string(), content));
                continue;
            }
        }
        files.extend(inputs::expand(token)?);
    }
    files.sort();
    let selector = selector_bytes(args.selector.as_deref(), !live_sources.is_empty())?;
    if args.selector.is_some() && live_sources.len() > 1 {
        return Err(CliError::usage(format!("--selector is ambiguous with {} live sources", live_sources.len()))
            .hint("run one live source per command when you configure it"));
    }

    let mut given: HashMap<String, Given> = HashMap::new();
    let mut named: HashSet<String> = HashSet::new();
    for pair in &args.inputs {
        let (name, value) = inputs::pair("--input", pair)?;
        if !machine_inputs.iter().any(|i| i.name == name) {
            return Err(CliError::usage(format!("the machine has no input named '{name}'"))
                .context(format!("its inputs: {}", input_names())));
        }
        let bytes = inputs::value("--input", name, value)?;
        match given.entry(name.to_string()).or_insert_with(|| Given::Values(Vec::new())) {
            Given::Values(values) => values.push(bytes),
            Given::Live(_) => unreachable!("only values are given by name"),
        }
        named.insert(name.to_string());
    }
    // The input a path or live source fits most closely, among those not named.
    // `file`: the file the value is, when it is one — what `plan` can be
    // asked about.
    let closest = |media: &MediaUrn, what: &str, file: Option<&Path>| -> CliResult<String> {
        let mut best: Vec<(&crate::orchestrator::machine_plan::MachineAnchor, i64)> = Vec::new();
        for input in machine_inputs.iter().filter(|i| !named.contains(&i.name)) {
            if !media.conforms_to(&input.media_urn).unwrap_or(false) {
                continue;
            }
            let distance = media.specificity() as i64 - input.media_urn.specificity() as i64;
            match best.first() {
                Some((_, nearest)) if distance > *nearest => {}
                Some((_, nearest)) if distance == *nearest => best.push((input, distance)),
                _ => best = vec![(input, distance)],
            }
        }
        match best.as_slice() {
            [(input, _)] => Ok(input.name.clone()),
            [] => Err({
                let error = CliError::failed(format!("{what} is not what this machine takes"))
                    .context(format!("its inputs: {}", input_names()));
                match file {
                    Some(file) => error.hint(format!("see what it can become: capdag plan {}", file.display())),
                    None => error.hint("give the machine a source of a kind its inputs take"),
                }
            }),
            several => Err(CliError::usage(format!("{what} fits several inputs of this machine"))
                .context(format!(
                    "it fits {}",
                    several.iter().map(|(i, _)| format!("'{}'", i.name)).collect::<Vec<_>>().join(", ")
                ))
                .hint("say which with --input <name>=@<path>")),
        }
    };
    let mut file_inputs: Vec<(String, PathBuf, Vec<u8>)> = Vec::with_capacity(files.len());
    for file in &files {
        let media = inputs::detect(file, registry)?;
        let name = closest(&media, &inputs::describe(file, &media, registry), Some(file))?;
        file_inputs.push((name, file.clone(), inputs::read(file)?));
    }
    let mut live_inputs: Vec<(String, String)> = Vec::new();
    for (reference, content) in &live_sources {
        let name = closest(content, &format!("the live source {reference}"), None)?;
        if file_inputs.iter().any(|(input, _, _)| input == &name) {
            return Err(CliError::usage(format!("input '{name}' was given both files and the live source {reference}")));
        }
        if live_inputs.iter().any(|(input, _)| input == &name) {
            return Err(CliError::usage(format!("input '{name}' was given two live sources")));
        }
        live_inputs.push((name, reference.clone()));
    }

    struct Run {
        label: String,
        stem: String,
        given: HashMap<String, Given>,
    }
    let once_per_path =
        machine_inputs.len() == 1 && args.inputs.is_empty() && live_inputs.is_empty() && file_inputs.len() > 1;
    let runs: Vec<Run> = if once_per_path {
        file_inputs
            .iter()
            .map(|(name, path, bytes)| Run {
                label: path.display().to_string(),
                stem: inputs::stem(path),
                given: HashMap::from([(name.clone(), Given::Values(vec![bytes.clone()]))]),
            })
            .collect()
    } else {
        for (name, _, bytes) in &file_inputs {
            match given.entry(name.clone()).or_insert_with(|| Given::Values(Vec::new())) {
                Given::Values(values) => values.push(bytes.clone()),
                Given::Live(_) => unreachable!("files and a live source on one input were refused above"),
            }
        }
        for (name, reference) in &live_inputs {
            given.insert(name.clone(), Given::Live(reference.clone()));
        }
        let stem = match (file_inputs.as_slice(), live_inputs.as_slice()) {
            ([(_, path, _)], []) => inputs::stem(path),
            ([(_, path, _), ..], _) => format!("{}.combined", inputs::stem(path)),
            ([], [(_, reference), ..]) => inputs::file_safe(reference),
            ([], []) => inputs::stem(path),
        };
        let run_label = match file_inputs.as_slice() {
            [(_, file, _)] => file.display().to_string(),
            _ => label.clone(),
        };
        vec![Run { label: run_label, stem, given }]
    };
    for run in &runs {
        let missing: Vec<String> = machine_inputs
            .iter()
            .filter(|i| !run.given.contains_key(&i.name))
            .map(|i| format!("{} ({})", i.name, names::media(registry, &i.media_urn)))
            .collect();
        if !missing.is_empty() {
            return Err(CliError::usage(format!("the machine's input {} was given nothing", missing.join(", ")))
                .hint("give a path it takes, or --input <name>=<value> (`@<path>` reads a file)"));
        }
    }

    let mut planned = Vec::with_capacity(runs.len());
    for (index, run) in runs.into_iter().enumerate() {
        let sequences: HashSet<String> = run
            .given
            .iter()
            .filter(|(_, g)| match g {
                Given::Values(values) => values.len() > 1,
                Given::Live(_) => true,
            })
            .map(|(name, _)| name.clone())
            .collect();
        let mut strands = Vec::with_capacity(resolved.strand_count());
        for strand_index in 0..resolved.strand_count() {
            let of_strand: HashSet<String> = sequences
                .iter()
                .filter(|name| machine_inputs.iter().any(|i| i.strand == strand_index && &i.name == *name))
                .cloned()
                .collect();
            let strand = resolved
                .plan(strand_index, &RunInputs { sequences: of_strand, ..RunInputs::default() })
                .await
                .map_err(|e| CliError::failed(format!("{label} could not be planned")).context(e.to_string()))?;
            if index == 0 {
                unbound_required_args(registry, &strand).await?;
            }
            let mut plan_inputs = HashMap::new();
            let mut is_sequence = HashMap::new();
            for (name, slot) in &strand.input_slots() {
                let (value, sequence) = match &run.given[name] {
                    Given::Values(values) if values.len() == 1 => (PlanInput::Bytes(values[0].clone()), false),
                    Given::Values(values) => (
                        PlanInput::Bytes(
                            crate::orchestrator::cbor_util::wrap_raw_items_as_cbor_sequence(values).map_err(|e| {
                                CliError::failed(format!("the {} values for input '{name}' could not be put together", values.len()))
                                    .context(e.to_string())
                            })?,
                        ),
                        true,
                    ),
                    Given::Live(reference) => (
                        PlanInput::LiveReference { reference_urn: reference.clone(), selector: selector.clone() },
                        true,
                    ),
                };
                plan_inputs.insert(slot.clone(), value);
                is_sequence.insert(slot.clone(), sequence);
            }
            strands.push(PlannedStrand {
                strand,
                inputs: plan_inputs,
                is_sequence,
                cap_arguments: HashMap::new(),
            });
        }
        planned.push(PlannedRun { label: run.label, stem: run.stem, strands });
    }
    Ok(planned)
}

/// Every required argument of a step the machine leaves unbound, before
/// anything runs.
async fn unbound_required_args(registry: &Arc<FabricRegistry>, strand: &StrandPlan) -> CliResult<()> {
    let requirements = crate::planner::MachinePlanBuilder::new(registry.clone())
        .analyze_plan_arguments(&strand.plan)
        .await
        .map_err(|e| CliError::failed("the machine's steps could not be checked").context(e.to_string()))?;
    let mut error: Option<CliError> = None;
    for step in &requirements.steps {
        for slot in &step.slots {
            if slot.is_required && slot.resolution == crate::planner::ArgumentResolution::RequiresUserInput {
                let line = format!(
                    "step {} needs '{}' ({}), and the machine gives it no value",
                    names::cap_urn(registry, &step.cap_urn),
                    slot.name,
                    names::media_urn(registry, &slot.media_urn),
                );
                error = Some(match error {
                    None => CliError::failed("the machine cannot run as written").context(line),
                    Some(e) => e.context(line),
                });
            }
        }
    }
    match error {
        None => Ok(()),
        Some(e) => Err(e.hint("bind it in the machine: a constant in the wiring (`<arg>=<value>`), or a node given with --input")),
    }
}

/// Run every planned run, one after another, its strands side by side.
async fn execute(
    ctx: &Ctx,
    registry: &Arc<FabricRegistry>,
    args: &Args,
    runs: Vec<PlannedRun>,
    dev_dir: Option<PathBuf>,
    single_scalar: bool,
    out: &mut dyn std::io::Write,
) -> CliResult<Exit> {
    let mut dev_binaries = inputs::dev_binaries(&args.dev_bins)?;
    dev_binaries.extend(dev_dir);
    let trace = match &args.trace {
        Some(path) => Some(crate::ProtocolTraceSink::open(&path.display().to_string()).await.map_err(|e| {
            CliError::failed(format!("the trace file {} could not be opened", path.display())).context(e.to_string())
        })?),
        None => None,
    };
    let output_dir = args.output.clone();
    let runtime = Arc::new(CliRuntime::new(
        ctx::user_cartridge_dir()?,
        if ctx.offline { None } else { ctx::BAKED_REGISTRY_URL.map(str::to_string) },
        ctx::BUILD_CHANNEL,
        crate::FABRIC_MANIFEST_VERSION,
        dev_binaries,
        ctx::bundled_cartridges_dir(),
        registry.clone(),
        trace,
        output_dir.clone().unwrap_or_else(|| PathBuf::from(".")),
    ));
    let control = Arc::new(RunControl::new());
    let (observer, mut events) = RunObserver::new();
    let mut watch = Watch::new(ctx.term.verbosity);
    let mut shower = Shower::new(&ctx.term, args.progress)?;
    // A key in the live view is Ctrl-C; otherwise the signal is.
    let signals_ctrl_c = !shower.reads_keys();
    // Several runs, or several strands, never interleave raw results on stdout.
    let emit_dir = if single_scalar { output_dir.clone() } else { Some(output_dir.clone().unwrap_or_else(|| PathBuf::from("."))) };

    let total = runs.len();
    let mut outcomes: Vec<RunOutcome> = Vec::with_capacity(total);
    let mut not_started: Vec<String> = Vec::new();
    let mut failures: Vec<CliError> = Vec::new();
    let mut grace_deadline: Option<tokio::time::Instant> = None;
    let mut stop_verdict: Option<tokio::task::JoinHandle<Result<StopInputsOutcome, StopInputsError>>> = None;
    let mut killed = false;
    let mut tick = tokio::time::interval(Duration::from_millis(100));

    for (run_index, run) in runs.into_iter().enumerate() {
        if !control.admits() || killed {
            not_started.push(run.label);
            continue;
        }
        let mut aborts: Vec<tokio::task::AbortHandle> = Vec::new();
        let mut joining = futures_util::stream::FuturesUnordered::new();
        for (strand_index, planned) in run.strands.into_iter().enumerate() {
            let key = RunKey { run: run_index, strand: strand_index };
            let label = if strand_index > 0 { format!("{} (part {})", run.label, strand_index + 1) } else { run.label.clone() };
            watch.add_run(key, label, &planned.strand, registry);
            match spawn_strand(&runtime, &observer, key, planned, None, control.clone()) {
                Ok(handle) => {
                    aborts.push(handle.task.abort_handle());
                    joining.push(async move { (handle.key, handle.task.await) });
                }
                Err((strand, error)) => {
                    failures.push(explain(registry, &strand, &error, ctx.term.verbosity >= 1));
                    let _ = watch.end_run(key, RunOutcome::Failed);
                    outcomes.push(RunOutcome::Failed);
                }
            }
        }
        while !joining.is_empty() {
            use futures_util::StreamExt;
            tokio::select! {
                biased;
                Some(event) = events.recv() => {
                    match watch.apply(&event) {
                        Ok(row) => shower.changed(&watch, event.key, row),
                        Err(e) => failures.push(CliError::failed("the run reported something this command cannot show").context(e.to_string())),
                    }
                }
                key = shower.key() => {
                    let ask = match key {
                        Some(Key::Interrupt) => RunAsk::Interrupt,
                        None => continue,
                    };
                    act(&control, &runtime, &mut shower, ask, &mut grace_deadline, &mut stop_verdict, &mut killed, &aborts);
                }
                Ok(()) = tokio::signal::ctrl_c(), if signals_ctrl_c => {
                    act(&control, &runtime, &mut shower, RunAsk::Interrupt, &mut grace_deadline, &mut stop_verdict, &mut killed, &aborts);
                }
                _ = async { tokio::time::sleep_until(grace_deadline.expect("guarded")).await }, if grace_deadline.is_some() => {
                    grace_deadline = None;
                    act(&control, &runtime, &mut shower, RunAsk::GraceOver, &mut grace_deadline, &mut stop_verdict, &mut killed, &aborts);
                }
                verdict = async { stop_verdict.as_mut().expect("guarded").await }, if stop_verdict.is_some() => {
                    stop_verdict = None;
                    say_verdict(&mut shower, verdict);
                }
                _ = tick.tick() => {
                    shower.tick(&watch, control.phase())?;
                }
                Some((key, joined)) = joining.next() => {
                    // Everything the strand said reaches the watch before its end.
                    while let Ok(event) = events.try_recv() {
                        if let Ok(row) = watch.apply(&event) {
                            shower.changed(&watch, event.key, row);
                        }
                    }
                    let (outcome, outputs) = match joined {
                        Ok((strand, result)) => {
                            let failed_at = result.as_ref().err().and_then(|e| e.step_token_id().map(|t| t.as_str().to_string()));
                            let report = finish_strand(
                                &strand,
                                result,
                                &EmitTarget { output_dir: emit_dir.clone(), force: args.force, stem: run.stem.clone() },
                                out,
                                &runtime,
                            );
                            if let Some(token) = failed_at {
                                if let Err(e) = watch.failed_at(key, &token) {
                                    failures.push(CliError::failed("the run reported something this command cannot show").context(e.to_string()));
                                }
                            }
                            let outcome = if killed {
                                RunOutcome::Aborted
                            } else {
                                control.outcome(report.failure.is_some(), report.tolerated)
                            };
                            say_notes(ctx, registry, &mut shower, &report);
                            if let Some(failure) = report_failure(registry, &strand, &report, ctx.term.verbosity >= 1) {
                                if outcome != RunOutcome::Aborted {
                                    failures.push(failure);
                                }
                            }
                            (outcome, written(&report))
                        }
                        Err(join) if join.is_cancelled() => (RunOutcome::Aborted, Vec::new()),
                        Err(join) => {
                            failures.push(CliError::failed("a run ended unexpectedly").context(join.to_string()));
                            (RunOutcome::Failed, Vec::new())
                        }
                    };
                    if let Err(e) = watch.end_run(key, outcome.clone()) {
                        failures.push(CliError::failed("the run reported something this command cannot show").context(e.to_string()));
                    }
                    shower.run_ended(&watch, key, &outputs);
                    outcomes.push(outcome);
                }
            }
        }
    }
    let interrupted = async {
        tokio::select! {
            Some(Key::Interrupt) = shower.key() => {}
            Ok(()) = tokio::signal::ctrl_c(), if signals_ctrl_c => {}
            else => std::future::pending::<()>().await,
        }
    };
    let verdict = last_verdict(stop_verdict.take(), killed, interrupted).await;
    if let Some(verdict) = verdict {
        say_verdict(&mut shower, verdict);
    }
    let _ = control.ask(RunAsk::Drained);
    shower.tick(&watch, control.phase())?;
    if !not_started.is_empty() {
        let line = format!("{} not started: {}", not_started.len(), not_started.join(", "));
        shower.say(&line, serde_json::json!({ "type": "not_started", "runs": not_started }));
        outcomes.push(RunOutcome::Stopped);
    }
    shower.close()?;
    let outcome = join_outcomes(outcomes);
    if ctx.term.json {
        super::term::emit_record(&serde_json::json!({
            "type": "finished",
            "outcome": super::live::outcome_word(&outcome),
            "exit": Exit::Run(outcome.clone()).code(),
        }));
    }
    let style = ctx.term.style;
    let last = failures.pop();
    for failure in failures {
        if ctx.term.json {
            super::term::emit_record(&failure.record());
        }
        eprint!("{}", failure.render(&style));
    }
    match last {
        Some(mut failure) => {
            failure.exit = Exit::Run(outcome);
            Err(failure)
        }
        None => Ok(Exit::Run(outcome)),
    }
}

/// What a person, or the grace running out, asks of the runs: the model
/// decides, and this does what it decided.
#[allow(clippy::too_many_arguments)]
fn act(
    control: &Arc<RunControl>,
    runtime: &Arc<CliRuntime>,
    shower: &mut Shower,
    ask: RunAsk,
    grace_deadline: &mut Option<tokio::time::Instant>,
    stop_verdict: &mut Option<tokio::task::JoinHandle<Result<StopInputsOutcome, StopInputsError>>>,
    killed: &mut bool,
    aborts: &[tokio::task::AbortHandle],
) {
    match control.ask(ask) {
        RunAct::None | RunAct::Finish => {}
        RunAct::Stop => {
            shower.say(
                "Stopping: nothing new starts; what started finishes and is written. Ctrl-C again aborts.",
                serde_json::json!({ "type": "stopping" }),
            );
            // The live feeds are closed and their end awaited off the loop: the
            // run goes on meanwhile, and an abort is not held up by it. The
            // run's admission is already closed (by the ask above).
            let runtime = runtime.clone();
            *stop_verdict = Some(tokio::spawn(async move {
                match runtime.stop_live_inputs().await {
                    Err(StopInputsError::NothingToStop) => Ok(StopInputsOutcome::default()),
                    verdict => verdict,
                }
            }));
        }
        RunAct::Cancel => {
            shower.say(
                &format!(
                    "Aborting: the cartridges are cancelled and have {} s to end. Ctrl-C again ends them now.",
                    ABORT_GRACE.as_secs()
                ),
                serde_json::json!({ "type": "aborting", "grace_seconds": ABORT_GRACE.as_secs() }),
            );
            *grace_deadline = Some(tokio::time::Instant::now() + ABORT_GRACE);
        }
        RunAct::Kill => {
            *grace_deadline = None;
            *killed = true;
            for abort in aborts {
                abort.abort();
            }
            shower.say("Aborted.", serde_json::json!({ "type": "aborted" }));
        }
    }
}

/// The stop's verdict on the live feeds once the runs have ended: they can end
/// before it is in, and it is still said. An aborted run's feeds may never end
/// cleanly, so there is nothing to wait for; and `interrupted` ends the wait.
async fn last_verdict(
    pending: Option<tokio::task::JoinHandle<Result<StopInputsOutcome, StopInputsError>>>,
    killed: bool,
    interrupted: impl std::future::Future<Output = ()>,
) -> Option<Result<Result<StopInputsOutcome, StopInputsError>, tokio::task::JoinError>> {
    let mut pending = pending?;
    if killed {
        pending.abort();
        return None;
    }
    tokio::select! {
        verdict = &mut pending => Some(verdict),
        () = interrupted => {
            pending.abort();
            None
        }
    }
}

/// The stop's verdict on the live feeds, once they have ended.
fn say_verdict(
    shower: &mut Shower,
    verdict: Result<Result<StopInputsOutcome, StopInputsError>, tokio::task::JoinError>,
) {
    match verdict {
        Ok(Ok(outcome)) if outcome == StopInputsOutcome::default() => {}
        Ok(Ok(outcome)) => shower.say(
            &format!(
                "Live input stopped: {} tap(s) closed, {} feed(s) stopped, {} ended.",
                outcome.host_taps_closed, outcome.feed_requests_stopped, outcome.feeds_ended
            ),
            serde_json::json!({
                "type": "stopped_input",
                "taps_closed": outcome.host_taps_closed,
                "feeds_stopped": outcome.feed_requests_stopped,
                "feeds_ended": outcome.feeds_ended,
            }),
        ),
        Ok(Err(error)) => shower.say(
            &format!("The live input did not stop cleanly: {error}"),
            serde_json::json!({ "type": "stop_failed", "message": error.to_string() }),
        ),
        Err(join) => shower.say(
            &format!("The stop did not finish: {join}"),
            serde_json::json!({ "type": "stop_failed", "message": join.to_string() }),
        ),
    }
}

fn written(report: &StrandReport) -> Vec<String> {
    report
        .outputs
        .values()
        .flat_map(|output| match output {
            EmittedOutput::Files(files) => files.iter().map(|f| f.display().to_string()).collect(),
            EmittedOutput::Stdout | EmittedOutput::NotProduced => Vec::new(),
        })
        .collect()
}

/// What a finished strand did other than simply complete, said with names.
fn say_notes(ctx: &Ctx, registry: &FabricRegistry, shower: &mut Shower, report: &StrandReport) {
    for note in &report.notes {
        let item = |item: &Option<usize>| item.map(|i| format!(" (item {})", i + 1)).unwrap_or_default();
        let (line, record) = match note {
            StrandNote::NotProduced { output, media_urn } => (
                format!("not produced: {output} ({}) — the branch that makes it did not fire", names::media_urn(registry, media_urn)),
                serde_json::json!({ "type": "not_produced", "output": output, "urn": media_urn }),
            ),
            StrandNote::Skipped { cap_urn, item: at, absent } => (
                format!("skipped: {}{} — it needs {}, which was not produced", names::cap_urn(registry, cap_urn), item(at), absent.join(", ")),
                serde_json::json!({ "type": "skipped", "step": names::cap_urn(registry, cap_urn), "urn": cap_urn, "item": at, "absent": absent }),
            ),
            StrandNote::Retried { cap_urn, item: at, attempt, failure } => (
                format!(
                    "retried: {}{}, attempt {attempt} failed {}: {}",
                    names::cap_urn(registry, cap_urn),
                    item(at),
                    failure_identity(failure),
                    one_line(&failure.message)
                ),
                serde_json::json!({
                    "type": "retried", "step": names::cap_urn(registry, cap_urn), "urn": cap_urn, "item": at,
                    "attempt": attempt, "class": failure.class.to_string(), "code": failure.code, "message": failure.message,
                }),
            ),
            StrandNote::Tolerated { cap_urn, item: at, failure, earlier_attempts } => (
                format!(
                    "failed, and tolerated: {}{} {} — {}{}",
                    names::cap_urn(registry, cap_urn),
                    item(at),
                    failure_identity(failure),
                    one_line(&failure.message),
                    if *earlier_attempts > 0 { format!(" (after {earlier_attempts} earlier attempts)") } else { String::new() }
                ),
                serde_json::json!({
                    "type": "tolerated", "step": names::cap_urn(registry, cap_urn), "urn": cap_urn, "item": at,
                    "class": failure.class.to_string(), "code": failure.code, "message": failure.message,
                }),
            ),
        };
        if ctx.term.verbosity >= 0 || ctx.term.json {
            shower.say(&line, record);
        }
    }
}

/// The failure a finished strand ended with, as an error a person can act on.
fn report_failure(registry: &FabricRegistry, strand: &StrandPlan, report: &StrandReport, verbose: bool) -> Option<CliError> {
    match report.failure.as_ref()? {
        StrandFailure::Run(error) => Some(explain(registry, strand, error, verbose)),
        StrandFailure::Emit(message) => Some(
            CliError::failed("the run's results could not be written")
                .context(message.clone())
                .hint(if message.contains("--force") {
                    "pass --force to overwrite them, or -o <dir> to write them elsewhere".to_string()
                } else {
                    "check the output directory".to_string()
                }),
        ),
    }
}

/// A run's failure, named by the step's alias and what its cartridge said.
/// With `verbose`, the URNs behind the names are said too.
pub fn explain(registry: &FabricRegistry, strand: &StrandPlan, error: &ExecutionError, verbose: bool) -> CliError {
    if let Some((id, dir)) = error.refused_cartridge() {
        return refused_cartridge(id, dir, error);
    }
    let step = error
        .step_token_id()
        .and_then(|token| {
            strand.plan.nodes.values().find_map(|node| match &node.node_type {
                ExecutionNodeType::Cap { token_id, cap_urn, .. } if token_id == token => Some(cap_urn.clone()),
                _ => None,
            })
        })
        .or_else(|| error.failure_cap_urn().map(str::to_string));
    let name = step.as_deref().map(|urn| names::cap_urn(registry, urn));
    let reason = one_line(&error.failure_reason());
    use crate::failure::AttributionClass;
    let class = error.attribution_class();
    let problem = match (&name, class) {
        (Some(step), AttributionClass::Input) => format!("{step} refused its input"),
        (Some(step), AttributionClass::User) => format!("{step} was cancelled"),
        (Some(step), _) => format!("{step} failed"),
        (None, AttributionClass::User) => "the run was cancelled".to_string(),
        (None, _) => "the run failed".to_string(),
    };
    let mut cli = CliError::failed(problem).context(reason);
    if let Some(arg) = error.failure_arg_urn() {
        cli = cli.context(format!("about its argument {}", names::media_urn(registry, arg)));
        if verbose {
            cli = cli.context(format!("argument: {arg}"));
        }
    }
    if verbose {
        if let Some(urn) = &step {
            cli = cli.context(format!("step: {urn}"));
        }
        if let Some(code) = error.failure_code() {
            cli = cli.context(format!("code: {code} ({})", error.attribution_class()));
        }
    }
    cli = match (class, &name) {
        (AttributionClass::Input, Some(step)) => cli.hint(format!("see what {step} takes: capdag info {step}")),
        (AttributionClass::Environment, Some(step)) => {
            cli.hint(format!("see which cartridge provides {step}, and whether it is installed: capdag info {step}"))
        }
        (AttributionClass::Resource, _) => cli.hint("free memory or disk, or run fewer items at once, and run it again"),
        (AttributionClass::User, _) => cli,
        (_, Some(step)) => cli.hint(format!("run it again with -v to see what {step}'s cartridge said; if it fails the same way, report it")),
        (_, None) => cli.hint("run it again with -v; if it fails the same way, report it"),
    };
    cli
}

/// An installed cartridge the run needs, and does not use: which one, why,
/// and the commands that replace it with the one the registry has for this
/// capdag.
pub fn refused_cartridge(id: &str, dir: &Path, error: &ExecutionError) -> CliError {
    let reason = match error {
        ExecutionError::StepFailed { source, .. } => source.to_string(),
        other => other.to_string(),
    };
    CliError::failed(format!("the installed {id} cannot be used"))
        .context(one_line(&reason))
        .context(format!("it is in {}", dir.display()))
        .hint(format!("replace it with the registry's: capdag cartridge remove {id} && capdag cartridge install {id}"))
}

/// A failure's declared identity: `[class/CODE]`, or `[class]` with no code.
fn failure_identity(failure: &crate::orchestrator::StepFailure) -> String {
    match &failure.code {
        Some(code) => format!("[{}/{code}]", failure.class),
        None => format!("[{}]", failure.class),
    }
}

/// A message on one line: a cartridge's multi-line debug text, flattened.
fn one_line(message: &str) -> String {
    message.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn separated(line: &str) -> Vec<String> {
        let argv: Vec<String> = line.split(' ').map(str::to_string).collect();
        separate_cap_tokens(super::super::Cli::command(), argv)
    }

    fn feeds_stopped() -> StopInputsOutcome {
        StopInputsOutcome { host_taps_closed: 1, feed_requests_stopped: 1, feeds_ended: 1 }
    }

    // TEST20122: the runs end before the stop's verdict on their live feeds is
    // in, and the verdict is still what is said. The verdict is held back on a
    // channel until the runs have ended, so the order is fixed, not timed.
    #[tokio::test]
    async fn test20122_a_stop_verdict_still_out_when_the_runs_end_is_said() {
        let (send, receive) = tokio::sync::oneshot::channel();
        let pending = tokio::spawn(async move { receive.await.expect("the verdict is sent") });
        assert!(!pending.is_finished(), "the verdict is not in when the runs end");
        let (said, ()) = tokio::join!(last_verdict(Some(pending), false, std::future::pending()), async {
            send.send(Ok(feeds_stopped())).expect("the verdict is awaited");
        });
        match said {
            Some(Ok(Ok(outcome))) => assert_eq!(outcome, feeds_stopped()),
            other => panic!("the verdict that came in after the runs ended is said: {other:?}"),
        }
        assert!(last_verdict(None, false, std::future::pending()).await.is_none(), "no stop, no verdict");
    }

    // TEST20123: an aborted run's verdict is not waited for, nor is one an
    // interrupt cuts short: neither is said, and the stop is abandoned.
    #[tokio::test]
    async fn test20123_an_aborted_or_interrupted_wait_says_no_verdict() {
        let never = || tokio::spawn(std::future::pending::<Result<StopInputsOutcome, StopInputsError>>());
        let aborted = never();
        let watched = aborted.abort_handle();
        assert!(last_verdict(Some(aborted), true, std::future::pending()).await.is_none());
        tokio::task::yield_now().await;
        assert!(watched.is_finished(), "the abandoned stop is ended");
        let interrupted = never();
        let watched = interrupted.abort_handle();
        assert!(last_verdict(Some(interrupted), false, std::future::ready(())).await.is_none());
        tokio::task::yield_now().await;
        assert!(watched.is_finished(), "the abandoned stop is ended");
    }

    // TEST20120: a cap's own options and inputs are put after `--`, in order,
    // and run's own flags — wherever they were — stay `run`'s.
    #[test]
    fn test20120_cap_tokens_are_separated_from_runs_flags() {
        assert_eq!(
            separated("capdag run disbind-pdf report.pdf --index-range 1-5 -o pages --force"),
            ["capdag", "run", "disbind-pdf", "-o", "pages", "--force", "--", "report.pdf", "--index-range", "1-5"]
        );
        assert_eq!(
            separated("capdag --offline run -v csv2json people.csv"),
            ["capdag", "--offline", "run", "-v", "csv2json", "--", "people.csv"]
        );
        assert_eq!(
            separated("capdag --fabric https://fabric.example run csv2json people.csv"),
            ["capdag", "--fabric", "https://fabric.example", "run", "csv2json", "--", "people.csv"]
        );
        assert_eq!(
            separated("capdag run tagger --dev-bin stand-in/cartridge.py --dev-bin=other"),
            ["capdag", "run", "tagger", "--dev-bin", "stand-in/cartridge.py", "--dev-bin=other"]
        );
        assert_eq!(separated("capdag run csv2json --help"), ["capdag", "run", "csv2json", "--", "--help"]);
        assert_eq!(separated("capdag run --help"), ["capdag", "run", "--help"]);
        assert_eq!(separated("capdag plan run.pdf"), ["capdag", "plan", "run.pdf"]);
    }
}
