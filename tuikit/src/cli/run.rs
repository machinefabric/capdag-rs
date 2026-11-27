//! `capdag run [MACHINE] [INPUT…]`.
//!
//! What runs is always a machine: written inline (`'[doc -> disbind-pdf ->
//! pages]'`) or in a `.machine` file, or planned from the inputs to what
//! `--to` names. A line runs its machine once; an input given several values
//! takes them as one sequence, and the machine decides, step by step, whether
//! a step runs once per item or takes them all.
//!
//! Whatever the line leaves open — what to make, which machine, which input a
//! file goes to, a value for an input or a step option, which cartridge to
//! install — is decided by the model of the command line ([`super::formal`]):
//! at a terminal a person is asked; in a script the command fails, naming the
//! choices and the flag that gives each. A run that settled anything prints
//! the line that runs the same machine again, asking nothing.
//!
//! While the run goes it is shown ([`super::live`]); Ctrl-C stops it (nothing
//! new starts, what started finishes and is written), Ctrl-C again aborts (the
//! cartridges are cancelled and given [`ABORT_GRACE`] to end), and a third
//! Ctrl-C during the grace ends them at once. What each does is the model's
//! decision ([`RunControl`]).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use super::ask::{self, Answered, Choice, Question};
use super::ctx::{self, Ctx};
use super::error::{CliError, CliResult, Exit};
use super::formal::{self as f, index, nat};
use super::host::{RunHost, RunSetup};
use super::inputs;
use super::inspect::Inspection;
use super::line::{self, Extras, MachineEntry, MachineFacts, PlanningFlags, RunState, Tables, World};
use super::live::{Key, ProgressChoice, Shower};
use super::names;
use super::watch::Watch;
use capdag::bifaci::live_feed::{StopInputsError, StopInputsOutcome};
use capdag::fabric::registry::FabricRegistry;
use capdag::orchestrator::cli_cap::{synthesize_single_cap_notation, SINGLE_CAP_INPUT_NODE};
use capdag::orchestrator::cli_session::{
    EmitTarget, EmittedOutput, PlannedStrand, StrandFailure, StrandNote, StrandReport,
};
use capdag::orchestrator::machine_plan::{ResolvedNotation, RunInputs, StrandPlan};
use capdag::orchestrator::observe::{RunKey, RunObserver};
use capdag::orchestrator::run_control::{join_outcomes, RunAct, RunAsk, RunControl, RunOutcome, ABORT_GRACE};
use capdag::orchestrator::PlanInput;
use capdag::planner::ExecutionNodeType;
use capdag::ExecutionError;
use super::term::emit_record;

#[derive(Debug, clap::Args)]
#[command(disable_help_flag = true)]
pub struct Args {
    /// The machine — written inline, or a .machine file — and then its inputs; or only
    /// inputs, to plan a machine to what --to names. An input is a file, a directory or
    /// glob (its files, sorted), a live source, or - (stdin)
    #[arg(value_name = "MACHINE|INPUT")]
    pub args: Vec<String>,
    /// Inputs: after --, no argument is taken for the machine
    #[arg(last = true, value_name = "INPUT")]
    pub after: Vec<String>,
    /// An input of the machine by its name: @<file|glob|directory>, - (stdin), or text
    /// (==<text> for text that starts with @ or =, or is -) (repeatable)
    #[arg(long = "input", value_name = "NAME=VALUE")]
    pub inputs: Vec<String>,
    /// A live source for an input of the machine, by the input's name (repeatable)
    #[arg(long = "live", value_name = "NAME=URN")]
    pub lives: Vec<String>,
    /// A step option the machine leaves to the run: <option>=<value> when one step has an
    /// option of that name, <step>.<option>=<value> otherwise; @<file> reads a file (repeatable)
    #[arg(long = "arg", value_name = "[STEP.]OPTION=VALUE")]
    pub options: Vec<String>,
    /// What to make, when no machine is given: a media alias, an extension or a media URN;
    /// several name what one machine makes together (repeatable)
    #[arg(long, value_name = "TYPE")]
    pub to: Vec<String>,
    /// What - holds, when no machine is given
    #[arg(long, value_name = "TYPE")]
    pub from: Option<String>,
    /// What a live source captures, as JSON ('{"duration_ms":5000}')
    #[arg(long, value_name = "JSON")]
    pub selector: Option<String>,
    /// Write results into this directory (default: the working directory)
    #[arg(short, long, value_name = "DIR")]
    pub output: Option<PathBuf>,
    /// Overwrite result files that exist
    #[arg(long)]
    pub force: bool,
    /// How the run is shown while it goes
    #[arg(long, value_enum, default_value_t = ProgressChoice::Auto, value_name = "HOW")]
    pub progress: ProgressChoice,
    /// Write each segment's protocol trace to this file (JSON Lines)
    #[arg(long, value_name = "FILE")]
    pub trace: Option<PathBuf>,
    /// Use this cartridge binary, or every binary in this directory (repeatable)
    #[arg(long = "dev-bin", value_name = "PATH")]
    pub dev_bins: Vec<PathBuf>,
    #[command(flatten)]
    pub planning: PlanningFlags,
    /// Print help; with a machine, the machine's own: its inputs, steps and the options it
    /// leaves to the run
    #[arg(short = 'h', long, action = clap::ArgAction::SetTrue)]
    pub help: bool,
}

impl Args {
    /// Whether the line reads stdin as data: `-` by position or by name.
    pub fn reads_stdin(&self) -> bool {
        self.args.iter().chain(self.after.iter()).any(|a| a == "-")
            || self.inputs.iter().any(|given| given.split_once('=').is_some_and(|(_, value)| value == "-"))
    }
}

/// What a run does with what it makes, beyond the machine.
pub struct RunSettings {
    pub output: Option<PathBuf>,
    pub force: bool,
}

/// One run: what it is of, its strands, planned, and what its outputs are
/// named after — what each source it was given is called, what each strand's
/// input slots were given, and the machine's name for an output that reads
/// no input.
struct PlannedRun {
    label: String,
    strands: Vec<PlannedStrand>,
    sources: Vec<String>,
    slots: Vec<Vec<(String, Vec<usize>)>>,
    machine_name: String,
}

/// A line read: the tables of what it names, its state, and its extras.
pub struct ReadLine {
    pub tables: Tables,
    pub state: RunState,
    pub extras: Extras,
}

/// Read a `run` line into the model's terms. The names `--input`, `--live` and
/// `--arg` give are a machine's, so a written machine is resolved first to
/// know them; a name it does not have is a place past its own, for the model
/// to refuse by name.
async fn read_line(world_ctx: &Ctx, registry: &Arc<FabricRegistry>, args: &Args) -> CliResult<(ReadLine, Option<MachineFacts>)> {
    let mut tables = Tables::default();
    let (machine, positionals) = line::read_positionals(&mut tables, &args.args, &args.after)?;
    let facts = match (&machine, args.inputs.is_empty() && args.lives.is_empty() && args.options.is_empty()) {
        (Some(m), false) => Some(line::resolve_entry(world_ctx, registry, &tables.machines[index(&m.base)]).await?),
        _ => None,
    };
    let resolved = facts.as_ref().and_then(MachineFacts::resolved);
    let input_names: Vec<String> = resolved.map(|r| r.inputs.iter().map(|i| i.name.clone()).collect()).unwrap_or_default();
    let mut unknown_inputs: Vec<String> = Vec::new();
    let place = |name: &str, unknown: &mut Vec<String>| -> usize {
        match input_names.iter().position(|known| known == name) {
            Some(j) => j,
            None => {
                let at = match unknown.iter().position(|u| u == name) {
                    Some(at) => at,
                    None => {
                        unknown.push(name.to_string());
                        unknown.len() - 1
                    }
                };
                input_names.len() + at
            }
        }
    };
    let mut named: Vec<(lungo::Nat, f::Value)> = Vec::new();
    let mut texts: Vec<Option<String>> = Vec::new();
    for given in &args.inputs {
        let (name, value) = inputs::pair("--input", given)?;
        let j = place(name, &mut unknown_inputs);
        if value == "-" {
            named.push((nat(j), f::Value::Stdin));
            texts.push(None);
        } else if let Some(text) = value.strip_prefix('=') {
            named.push((nat(j), f::Value::Text));
            texts.push(Some(text.to_string()));
        } else if let Some(path) = value.strip_prefix('@') {
            for p in line::files_of(&mut tables, path)? {
                named.push((nat(j), f::Value::File { p: nat(p) }));
                texts.push(None);
            }
        } else {
            named.push((nat(j), f::Value::Text));
            texts.push(Some(value.to_string()));
        }
    }
    for given in &args.lives {
        let (name, urn) = inputs::pair("--live", given)?;
        let j = place(name, &mut unknown_inputs);
        let r = tables.add_live(urn.to_string());
        named.push((nat(j), f::Value::Live { r: nat(r) }));
        texts.push(None);
    }
    let mut unknown_options: Vec<String> = Vec::new();
    let mut given_args: Vec<f::ArgGiven> = Vec::new();
    let mut arg_values: Vec<String> = Vec::new();
    for given in &args.options {
        let (name, value) = inputs::pair("--arg", given)?;
        let (option, qualified) = match resolved {
            Some(r) => option_place(r, name, &mut unknown_options),
            None => {
                unknown_options.push(name.to_string());
                (unknown_options.len() - 1, name.contains('.'))
            }
        };
        given_args.push(f::ArgGiven { option: nat(option), qualified });
        arg_values.push(value.to_string());
    }
    let selector_reads = match &args.selector {
        Some(raw) => capdag::bifaci::live_feed::LiveFeedSelector::parse(raw.as_bytes()).is_ok(),
        None => true,
    };
    let state = RunState {
        line: f::RunLine {
            machine,
            inputs: f::list(positionals),
            named: f::list(named),
            args: f::list(given_args),
            targets: nat(args.to.len()),
            has_from: args.from.is_some(),
            has_selector: args.selector.is_some(),
            selector_reads,
        },
        texts,
        arg_values,
        unknown_inputs,
        unknown_options,
    };
    let extras = Extras {
        to: args.to.clone(),
        from: args.from.clone(),
        selector: args.selector.clone(),
        output: args.output.clone(),
    };
    Ok((ReadLine { tables, state, extras }, facts))
}

/// Which open option of a resolved machine `--arg <name>` binds, and whether
/// it names the step: `<step>.<option>` when the part before the first dot
/// names a step that leaves an option of the rest's name to the run. A name
/// that is no open option is a place past the machine's own options.
fn option_place(resolved: &line::Resolved, name: &str, unknown: &mut Vec<String>) -> (usize, bool) {
    if let Some((step, option)) = name.split_once('.') {
        if resolved.options.iter().any(|o| o.step == step) {
            return match resolved.options.iter().position(|o| o.step == step && o.name == option) {
                Some(j) => (j, true),
                None => {
                    unknown.push(name.to_string());
                    (resolved.options.len() + unknown.len() - 1, true)
                }
            };
        }
    }
    match resolved.options.iter().position(|o| o.name == name) {
        Some(j) => (j, false),
        None => {
            unknown.push(name.to_string());
            (resolved.options.len() + unknown.len() - 1, false)
        }
    }
}

pub async fn run(ctx: &Ctx, args: Args) -> CliResult<Exit> {
    let registry = ctx.registry().await?;
    let (read, machine_facts) = read_line(ctx, &registry, &args).await?;
    let ReadLine { tables, mut state, extras } = read;
    let inspection = Inspection::new(ctx, inspection_runtime(ctx, &registry)?, registry.clone());
    let mut world = World::new(ctx, registry.clone(), inspection, tables, extras, args.planning.clone());
    if let (Some(facts), Some(m)) = (machine_facts, &state.line.machine) {
        world.know_machine(index(&m.base), facts);
    }
    world.learn_line(&args.dev_bins).await?;
    if args.help {
        return machine_help(ctx, &mut world, &state).await;
    }
    let typed = state.line.clone();
    let positionals: Vec<f::Positional> = state.line.inputs.0.clone();
    let planning_flags = args.planning.given();
    loop {
        let line_now = state.line.clone();
        let cli_world = ctx.cli_world(false);
        let globals = ctx.globals();
        let verdict = world
            .settle(&positionals, |w| {
                f::cli(
                    cli_world.clone(),
                    f::Invocation::Command {
                        g: globals.clone(),
                        c: f::Command::Run { l: line_now.clone(), planning_flags, w },
                    },
                )
            })
            .await?;
        match verdict {
            f::CliVerdict::Is { v: f::Verdict::Runs } => break,
            f::CliVerdict::Is { v: f::Verdict::Asks { s } } => {
                state = answer(ctx, &mut world, &positionals, state, s).await?;
            }
            other => return Err(super::say::line_verdict_error(ctx, &mut world, &state, &positionals, &other).await),
        }
    }
    let settled = state.line.clone();
    let rerun = world.settle(&positionals, |w| f::rerun(w, settled.clone())).await?;
    let prints = world.settle(&positionals, |w| f::prints_rerun(w, typed.clone(), settled.clone())).await?;
    let rerun_state = carried(&world, &state, rerun)?;
    let running_line = {
        let line = rerun_state.line.clone();
        world.settle(&positionals, |w| f::freeze(w, line.clone())).await?
    };
    let running = carried(&world, &rerun_state, running_line)?;
    if !running.line.inputs.is_empty() {
        return Err(CliError::failed("the run was settled with an input given by position")
            .hint("report it: a line that runs gives every value to a named input"));
    }
    if prints {
        say_rerun(ctx, &world, &rerun_state, &args)?;
    }
    let m = index(&running.line.machine.as_ref().expect("a line that runs writes its machine").base);
    let runtime = runtime(ctx, &registry, &args, world.dev_binaries(Some(m))).await?;
    let (run, single_scalar) = planned_run(&world, &running, args.selector.as_deref()).await?;
    let settings = RunSettings { output: args.output.clone(), force: args.force };
    if ctx.term.json {
        // Under --json stdout is records only: every result is written as a
        // file, a single value too, and the files are named in each run's
        // `run_finished` record rather than listed.
        return execute(ctx, &registry, &settings, Some(args.progress), runtime, vec![run], false, Results::Into(&mut std::io::sink())).await;
    }
    execute(ctx, &registry, &settings, Some(args.progress), runtime, vec![run], single_scalar, Results::Stdout).await
}

/// The state a model line carries, from the state it came from: the text of
/// each `named` entry and the value of each `--arg` follow their entries. An
/// entry the line kept keeps its value; an `--arg` the model added for an
/// option with one value takes that value.
fn carried(world: &World<'_>, from: &RunState, line: f::RunLine) -> CliResult<RunState> {
    let mut texts = Vec::with_capacity(line.named.len());
    for (at, entry) in line.named.iter().enumerate() {
        if at < from.line.named.len() && from.line.named[at] == *entry {
            texts.push(from.texts[at].clone());
        } else if matches!(entry.1, f::Value::Text) {
            return Err(CliError::failed("a value of the line was lost settling it")
                .hint("report it: settling a line never adds text it was not given"));
        } else {
            texts.push(None);
        }
    }
    let mut arg_values = Vec::with_capacity(line.args.len());
    for (at, given) in line.args.iter().enumerate() {
        if at < from.line.args.len() && from.line.args[at] == *given {
            arg_values.push(from.arg_values[at].clone());
            continue;
        }
        let m = index(&line.machine.as_ref().expect("a line with --arg writes its machine").base);
        let option = world
            .machine(m)
            .and_then(MachineFacts::resolved)
            .and_then(|r| r.options.get(index(&given.option)))
            .ok_or_else(|| CliError::failed("the model bound an option the machine does not leave to the run"))?;
        match option.allowed.as_deref() {
            Some([only]) => arg_values.push(only.clone()),
            _ => {
                return Err(CliError::failed(format!("the option {} was bound without a value", option.name))
                    .hint("report it: only an option with one value is bound without being given one"))
            }
        }
    }
    Ok(RunState {
        line,
        texts,
        arg_values,
        unknown_inputs: from.unknown_inputs.clone(),
        unknown_options: from.unknown_options.clone(),
    })
}

/// Ask a person the stage the model asks, and give the answer to the line as
/// the flag that gives it.
async fn answer(ctx: &Ctx, world: &mut World<'_>, positionals: &[f::Positional], state: RunState, s: f::Stage) -> CliResult<RunState> {
    let line_now = state.line.clone();
    let (given, text): (f::Answer, Option<String>) = match &s {
        f::Stage::StdinType => {
            let typed = loop {
                match ask::value(ctx, &ask::ValueQuestion::stdin_type()).await? {
                    Answered::Declined => return Err(ask::declined()),
                    Answered::Given(text) => {
                        if line::media_type(&world.registry, &text).await?.is_some() {
                            break text;
                        }
                        ask::say_retry(ctx, &format!("'{text}' names no media type: see capdag caps"));
                    }
                }
            };
            world.extras.from = Some(typed);
            world.learn_types().await?;
            (f::Answer::Pick { k: nat(0) }, None)
        }
        f::Stage::Target => {
            let plan = world.plan(positionals).cloned().expect("what to make is asked once the inputs are planned");
            let choices: Vec<Choice> = plan.targets.iter().map(|t| super::plan::target_choice(&world.registry, t)).collect();
            let k = pick(ctx, &Question { what: "what to make".into(), flag: "--to ".into() }, &choices).await?;
            world.extras.to = vec![choices[k].value.clone()];
            world.learn_types().await?;
            (f::Answer::Pick { k: nat(0) }, None)
        }
        f::Stage::Machine => {
            let plan = world.plan(positionals).cloned().expect("the machine is asked once it is planned");
            let choices: Vec<Choice> = plan
                .described
                .iter()
                .enumerate()
                .map(|(k, d)| Choice {
                    label: d.notation.replace('\n', " "),
                    detail: d.summary(),
                    urn: Some(d.canonical.clone()),
                    value: (k + 1).to_string(),
                })
                .collect();
            let k = pick(ctx, &Question { what: "the machine".into(), flag: "capdag run ".into() }, &choices).await?;
            (f::Answer::Pick { k: nat(k) }, None)
        }
        f::Stage::Path { p } => {
            let (m, written) = running_machine(world, positionals, &line_now).await?;
            let machine = written.machine.clone().expect("the line that runs a machine writes it");
            let candidates: Vec<usize> = world
                .settle(positionals, |w| f::path_candidates(w, machine.clone(), written.clone(), p.clone()))
                .await?
                .iter()
                .map(index)
                .collect();
            let resolved = world.machine(m).and_then(MachineFacts::resolved).expect("asked of a machine that resolves");
            let file = world.tables.files[index(p)].path.display().to_string();
            let choices: Vec<Choice> = candidates
                .iter()
                .map(|&j| Choice {
                    label: resolved.inputs[j].name.clone(),
                    detail: names::media(&world.registry, &resolved.inputs[j].media_urn),
                    urn: Some(resolved.inputs[j].media_urn.to_string()),
                    value: format!("{}=@{file}", resolved.inputs[j].name),
                })
                .collect();
            let at = pick(ctx, &Question { what: format!("the input {file} goes to"), flag: "--input ".into() }, &choices).await?;
            (f::Answer::Pick { k: nat(candidates[at]) }, None)
        }
        f::Stage::Stdin => {
            let (m, written) = running_machine(world, positionals, &line_now).await?;
            let machine = written.machine.clone().expect("the line that runs a machine writes it");
            let candidates: Vec<usize> = world
                .settle(positionals, |w| f::stdin_candidates(w, machine.clone(), written.clone()))
                .await?
                .iter()
                .map(index)
                .collect();
            let resolved = world.machine(m).and_then(MachineFacts::resolved).expect("asked of a machine that resolves");
            let choices: Vec<Choice> = candidates
                .iter()
                .map(|&j| Choice {
                    label: resolved.inputs[j].name.clone(),
                    detail: names::media(&world.registry, &resolved.inputs[j].media_urn),
                    urn: Some(resolved.inputs[j].media_urn.to_string()),
                    value: format!("{}=-", resolved.inputs[j].name),
                })
                .collect();
            let at = pick(ctx, &Question { what: "the input - goes to".into(), flag: "--input ".into() }, &choices).await?;
            (f::Answer::Pick { k: nat(candidates[at]) }, None)
        }
        f::Stage::Input { j } => {
            let (m, _) = running_machine(world, positionals, &line_now).await?;
            let resolved = world.machine(m).and_then(MachineFacts::resolved).expect("asked of a machine that resolves");
            let input = resolved.inputs[index(j)].clone();
            let question = ask::ValueQuestion::input(&input.name, &names::media(&world.registry, &input.media_urn));
            loop {
                let typed = match ask::value(ctx, &question).await? {
                    Answered::Declined => return Err(ask::declined()),
                    Answered::Given(typed) => typed,
                };
                if typed == "-" {
                    break (f::Answer::Value { v: f::Value::Stdin }, None);
                }
                if let Some(text) = typed.strip_prefix('=') {
                    break (f::Answer::Value { v: f::Value::Text }, Some(text.to_string()));
                }
                if let Some(path) = typed.strip_prefix('@') {
                    let path = PathBuf::from(path);
                    if !path.is_file() {
                        ask::say_retry(ctx, &format!("{} is no file: give one file, or several with --input <name>=@<glob>", path.display()));
                        continue;
                    }
                    let p = world.tables.add_file(path, true);
                    break (f::Answer::Value { v: f::Value::File { p: nat(p) } }, None);
                }
                break (f::Answer::Value { v: f::Value::Text }, Some(typed));
            }
        }
        f::Stage::Option { j } => {
            let (m, _) = running_machine(world, positionals, &line_now).await?;
            let resolved = world.machine(m).and_then(MachineFacts::resolved).expect("asked of a machine that resolves");
            let option = resolved.options[index(j)].clone();
            let flag = format!("--arg {}=", resolved.flag_name(index(j)));
            let value = match &option.allowed {
                Some(values) => {
                    let choices: Vec<Choice> = values
                        .iter()
                        .map(|value| Choice { label: value.clone(), detail: String::new(), urn: None, value: value.clone() })
                        .collect();
                    let at = pick(
                        ctx,
                        &Question { what: format!("{} of step {}", option.name, option.step), flag: flag.clone() },
                        &choices,
                    )
                    .await?;
                    values[at].clone()
                }
                None => match ask::value(
                    ctx,
                    &ask::ValueQuestion::option(
                        &option.name,
                        &option.step,
                        &resolved.flag_name(index(j)),
                        &names::media_urn(&world.registry, &option.media_urn),
                    ),
                )
                .await?
                {
                    Answered::Declined => return Err(ask::declined()),
                    Answered::Given(value) => value,
                },
            };
            (f::Answer::Value { v: f::Value::Text }, Some(value))
        }
        f::Stage::Provider => {
            let (m, _) = running_machine(world, positionals, &line_now).await?;
            let needs = world.needs(m).cloned().expect("a provider is asked once what the run needs is known");
            let (cap, providers) = needs
                .missing
                .iter()
                .find(|(_, providers)| providers.len() > 1)
                .cloned()
                .expect("a provider is asked only when a cap has several");
            let cap_name = names::cap_urn(&world.registry, &cap.to_string());
            let choices: Vec<Choice> = providers
                .iter()
                .map(|id| Choice { label: id.clone(), detail: format!("provides {cap_name}"), urn: None, value: id.clone() })
                .collect();
            let at = pick(
                ctx,
                &Question { what: format!("the cartridge to install for {cap_name}"), flag: "capdag cartridge install ".into() },
                &choices,
            )
            .await?;
            super::cartridge::install_cartridge(ctx, &providers[at]).await?;
            world.forget_needs(m);
            return Ok(state);
        }
        f::Stage::Capability | f::Stage::Model => {
            return Err(CliError::failed("a run was asked a question of another command")
                .hint("report it: a run asks only what its line leaves open"))
        }
    };
    let option_answer = matches!(s, f::Stage::Option { .. });
    let given_line = {
        let (line, stage, given) = (line_now.clone(), s.clone(), given.clone());
        world.settle(positionals, |w| f::give(w, line.clone(), stage.clone(), given.clone())).await?
    };
    let mut next = carried_answer(&state, given_line, if option_answer { None } else { text.clone() })?;
    if option_answer {
        next.arg_values.push(text.expect("an option's answer is a value"));
    }
    Ok(next)
}

/// The state after an answer: entries the line kept keep their values; the
/// one entry the answer added that is text holds what was typed.
fn carried_answer(from: &RunState, line: f::RunLine, mut text: Option<String>) -> CliResult<RunState> {
    let mut texts = Vec::with_capacity(line.named.len());
    for (at, entry) in line.named.iter().enumerate() {
        if at < from.line.named.len() && from.line.named[at] == *entry {
            texts.push(from.texts[at].clone());
        } else if matches!(entry.1, f::Value::Text) {
            texts.push(Some(text.take().ok_or_else(|| {
                CliError::failed("an answer added text that was not typed").hint("report it")
            })?));
        } else {
            texts.push(None);
        }
    }
    let kept = line.args.len().min(from.line.args.len());
    if line.args[..kept] != from.line.args[..kept] {
        return Err(CliError::failed("an answer changed an --arg the line gave").hint("report it"));
    }
    let mut arg_values = from.arg_values.clone();
    arg_values.truncate(kept);
    Ok(RunState {
        line,
        texts,
        arg_values,
        unknown_inputs: from.unknown_inputs.clone(),
        unknown_options: from.unknown_options.clone(),
    })
}

/// Pick one of `choices`, or end the command when the person declines.
async fn pick(ctx: &Ctx, question: &Question, choices: &[Choice]) -> CliResult<usize> {
    match ask::pick(ctx, question, choices).await? {
        Answered::Given(at) => Ok(at),
        Answered::Declined => Err(ask::declined()),
    }
}

/// The machine a line runs — the one it writes, or the one planning settled
/// on — and the line that runs it, the way the model reads it
/// (`RunLine.written`). A question about a machine's inputs or options is
/// about this machine.
async fn running_machine(world: &mut World<'_>, positionals: &[f::Positional], line: &f::RunLine) -> CliResult<(usize, f::RunLine)> {
    let written = world.settle(positionals, |w| f::run_line::written(w, line.clone())).await?;
    let m = index(&written.machine.as_ref().expect("the line that runs a machine writes it").base);
    world.resolve_machine(m).await?;
    if world.machine(m).and_then(MachineFacts::resolved).is_none() {
        return Err(CliError::failed("a question was asked about a machine that does not resolve").hint("report it"));
    }
    Ok((m, written))
}

/// The line to run the same machine again, asking nothing, as words a shell
/// reads back: on stderr, and under `--json` as a `rerun` record.
fn say_rerun(ctx: &Ctx, world: &World<'_>, rerun: &RunState, args: &Args) -> CliResult<()> {
    let words = rerun_words(ctx, world, rerun, args)?;
    if ctx.term.json {
        emit_record(&serde_json::json!({ "type": "rerun", "argv": words }));
    } else if ctx.term.verbosity >= 0 {
        eprintln!("Run it again with: {}", super::shell_line(&words));
    }
    Ok(())
}

/// The words of the line to run the same machine again.
pub fn rerun_words(ctx: &Ctx, world: &World<'_>, rerun: &RunState, args: &Args) -> CliResult<Vec<String>> {
    let line = &rerun.line;
    let m = index(&line.machine.as_ref().expect("the line to run again writes its machine").base);
    let resolved = world
        .machine(m)
        .and_then(MachineFacts::resolved)
        .ok_or_else(|| CliError::failed("the machine that ran is not known").hint("report it"))?;
    let mut words: Vec<String> = vec!["capdag".into()];
    words.extend(ctx.global_words());
    words.push("run".into());
    words.push(world.tables.machines[m].argument());
    let mut after: Vec<String> = Vec::new();
    for x in line.inputs.iter() {
        let word = match x {
            f::Positional::Path { p } => world.tables.files[index(p)].path.display().to_string(),
            f::Positional::Stdin => "-".to_string(),
            f::Positional::Live { r } => world.tables.lives[index(r)].clone(),
        };
        // A path that would be read as a flag, the machine or stdin goes
        // after `--`.
        if word.starts_with('-') && word != "-" {
            after.push(word);
        } else {
            words.push(word);
        }
    }
    for ((j, value), text) in line.named.iter().zip(&rerun.texts) {
        let name = &resolved.inputs[index(j)].name;
        match value {
            f::Value::File { p } => {
                words.push("--input".into());
                words.push(format!("{name}=@{}", world.tables.files[index(p)].path.display()));
            }
            f::Value::Stdin => {
                words.push("--input".into());
                words.push(format!("{name}=-"));
            }
            f::Value::Text => {
                let text = text.as_deref().expect("text is carried with its entry");
                let escaped = if text.starts_with('@') || text.starts_with('=') || text == "-" { "=" } else { "" };
                words.push("--input".into());
                words.push(format!("{name}={escaped}{text}"));
            }
            f::Value::Live { r } => {
                words.push("--live".into());
                words.push(format!("{name}={}", world.tables.lives[index(r)]));
            }
        }
    }
    for (given, value) in line.args.iter().zip(&rerun.arg_values) {
        let j = index(&given.option);
        let name = if given.qualified { resolved.options[j].flag_name(true) } else { resolved.options[j].name.clone() };
        words.push("--arg".into());
        words.push(format!("{name}={value}"));
    }
    if let Some(selector) = &args.selector {
        words.push("--selector".into());
        words.push(selector.clone());
    }
    if let Some(dir) = &args.output {
        words.push("-o".into());
        words.push(dir.display().to_string());
    }
    if args.force {
        words.push("--force".into());
    }
    if let Some(trace) = &args.trace {
        words.push("--trace".into());
        words.push(trace.display().to_string());
    }
    for dev in &args.dev_bins {
        words.push("--dev-bin".into());
        words.push(dev.display().to_string());
    }
    if !after.is_empty() {
        words.push("--".into());
        words.extend(after);
    }
    Ok(words)
}

/// `capdag run <machine> --help`: the machine's inputs, steps and the options
/// it leaves to the run, as `capdag check` shows them; `capdag run --help`
/// without a machine: run's own help.
async fn machine_help(ctx: &Ctx, world: &mut World<'_>, state: &RunState) -> CliResult<Exit> {
    let Some(machine) = state.line.machine.clone() else {
        super::print_command_help(&["run"])?;
        return Ok(Exit::Ok);
    };
    let m = index(&machine.base);
    let positionals: Vec<f::Positional> = Vec::new();
    let cli_world = ctx.cli_world(false);
    let verdict = world
        .settle(&positionals, |w| {
            f::cli(cli_world.clone(), f::Invocation::HelpFlag { topic: f::HelpTopic::Machine { m: machine.clone(), w } })
        })
        .await?;
    match verdict {
        f::CliVerdict::Is { v: f::Verdict::Help } => {
            let resolved = world.machine(m).and_then(MachineFacts::resolved).expect("help is shown for a machine that resolves");
            super::check::show(ctx, &world.registry, &world.tables.machines[m], resolved)?;
            Ok(Exit::Ok)
        }
        other => Err(super::check::showing_error(world, &machine, &other)),
    }
}

/// The run of a settled line: every value it gives each input of its machine,
/// in the order given — one value, or several as one sequence — and every
/// option it binds, given to its step.
async fn planned_run(world: &World<'_>, running: &RunState, selector: Option<&str>) -> CliResult<(PlannedRun, bool)> {
    let m = index(&running.line.machine.as_ref().expect("a line that runs writes its machine").base);
    let resolved = world
        .machine(m)
        .and_then(MachineFacts::resolved)
        .ok_or_else(|| CliError::failed("the machine that runs is not known").hint("report it"))?;
    let selector = selector_bytes(selector)?;
    let mut stdin: Option<Vec<u8>> = None;
    let mut values: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
    let mut live: HashMap<String, String> = HashMap::new();
    let mut files_given: Vec<PathBuf> = Vec::new();
    // Each value given is a source of the run, in the order given; what it
    // is called names what descends from it.
    let mut sources: Vec<String> = Vec::new();
    let mut positions: HashMap<String, Vec<usize>> = HashMap::new();
    for ((j, value), text) in running.line.named.iter().zip(&running.texts) {
        let name = resolved.inputs[index(j)].name.clone();
        positions.entry(name.clone()).or_default().push(sources.len());
        match value {
            f::Value::File { p } => {
                let path = &world.tables.files[index(p)].path;
                values.entry(name).or_default().push(inputs::read(path)?);
                sources.push(inputs::stem(path));
                files_given.push(path.clone());
            }
            f::Value::Text => {
                values.entry(name.clone()).or_default().push(text.clone().expect("text is carried with its entry").into_bytes());
                sources.push(name);
            }
            f::Value::Stdin => {
                if stdin.is_none() {
                    stdin = Some(read_stdin()?);
                }
                values.entry(name).or_default().push(stdin.clone().expect("read just above"));
                sources.push("stdin".to_string());
            }
            f::Value::Live { r } => {
                let reference = world.tables.lives[index(r)].clone();
                sources.push(inputs::file_safe(&reference));
                live.insert(name, reference);
            }
        }
    }
    let mut strands = Vec::with_capacity(resolved.resolution.strand_count());
    let mut slots = Vec::with_capacity(resolved.resolution.strand_count());
    for strand_index in 0..resolved.resolution.strand_count() {
        let sequences = resolved
            .inputs
            .iter()
            .filter(|input| input.strand == strand_index)
            .filter(|input| live.contains_key(&input.name) || values.get(&input.name).is_some_and(|v| v.len() > 1))
            .map(|input| input.name.clone())
            .collect();
        let strand = resolved
            .resolution
            .plan(strand_index, &RunInputs { sequences, ..RunInputs::default() })
            .await
            .map_err(|e| CliError::failed("the machine could not be planned").context(e.to_string()))?;
        let mut plan_inputs = HashMap::new();
        let mut is_sequence = HashMap::new();
        slots.push(
            strand
                .input_slots()
                .iter()
                .map(|(name, slot)| (slot.clone(), positions.get(name).cloned().unwrap_or_default()))
                .collect::<Vec<_>>(),
        );
        for (name, slot) in &strand.input_slots() {
            let (value, sequence) = match (values.get(name), live.get(name)) {
                (_, Some(reference)) => (
                    PlanInput::LiveReference { reference_urn: reference.clone(), selector: selector.clone() },
                    true,
                ),
                (Some(given), None) if given.len() == 1 => (PlanInput::Bytes(given[0].clone()), false),
                (Some(given), None) => (
                    PlanInput::Bytes(capdag::orchestrator::cbor_util::wrap_raw_items_as_cbor_sequence(given).map_err(|e| {
                        CliError::failed(format!("the {} values for input '{name}' could not be put together", given.len()))
                            .context(e.to_string())
                    })?),
                    true,
                ),
                (None, None) => {
                    return Err(CliError::failed(format!("the input '{name}' was given nothing"))
                        .hint("report it: a line that runs gives every input of its machine a value"))
                }
            };
            plan_inputs.insert(slot.clone(), value);
            is_sequence.insert(slot.clone(), sequence);
        }
        let mut cap_arguments: HashMap<String, Vec<(String, Vec<u8>)>> = HashMap::new();
        for (given, value) in running.line.args.iter().zip(&running.arg_values) {
            let option = &resolved.options[index(&given.option)];
            if option.strand != strand_index {
                continue;
            }
            let node = strand
                .plan
                .nodes
                .iter()
                .find(|(_, node)| matches!(&node.node_type, ExecutionNodeType::Cap { token_id, .. } if token_id.as_str() == option.token))
                .map(|(id, _)| id.clone())
                .ok_or_else(|| CliError::failed(format!("the step {} is not in the machine's plan", option.step)).hint("report it"))?;
            let bytes = match value.strip_prefix('@') {
                Some(path) => inputs::read(Path::new(path))?,
                None => value.clone().into_bytes(),
            };
            cap_arguments.entry(node).or_default().push((option.media_urn.clone(), bytes));
        }
        strands.push(PlannedStrand { strand, inputs: plan_inputs, is_sequence, cap_arguments });
    }
    let entry = &world.tables.machines[m];
    // What an output that reads no input is called: the machine file's
    // stem, or for a machine written inline, its first step's name.
    let machine_name = match entry {
        MachineEntry::File(path) => inputs::stem(path),
        MachineEntry::Inline(_) => resolved
            .caps
            .first()
            .map(|cap| names::cap_urn(&world.registry, &cap.to_string()))
            .ok_or_else(|| CliError::failed("the machine has no step").hint("report it"))?,
    };
    let label = match files_given.as_slice() {
        [one] => one.display().to_string(),
        _ => entry.shown(),
    };
    let single_scalar = strands.len() == 1;
    Ok((PlannedRun { label, strands, sources, slots, machine_name }, single_scalar))
}

fn read_stdin() -> CliResult<Vec<u8>> {
    use std::io::Read;
    let mut bytes = Vec::new();
    std::io::stdin()
        .read_to_end(&mut bytes)
        .map_err(|e| CliError::failed("stdin could not be read").context(e.to_string()))?;
    Ok(bytes)
}

/// What a live source captures: `--selector`, or everything it gives.
fn selector_bytes(given: Option<&str>) -> CliResult<Vec<u8>> {
    match given {
        None => Ok(b"{}".to_vec()),
        Some(raw) => {
            capdag::bifaci::live_feed::LiveFeedSelector::parse(raw.as_bytes()).map_err(|e| {
                CliError::usage("--selector is not a valid live-feed selector")
                    .context(e.to_string())
                    .hint("for example: --selector '{\"duration_ms\":5000}'")
            })?;
            Ok(raw.as_bytes().to_vec())
        }
    }
}

/// The cartridge-hosting runtime of one command: its dev binaries, its trace,
/// and where it writes what it makes.
async fn runtime(ctx: &Ctx, registry: &Arc<FabricRegistry>, args: &Args, dev_binaries: Vec<PathBuf>) -> CliResult<Arc<dyn RunHost>> {
    let trace = match &args.trace {
        Some(path) => Some(capdag::ProtocolTraceSink::open(&path.display().to_string()).await.map_err(|e| {
            CliError::failed(format!("the trace file {} could not be opened", path.display())).context(e.to_string())
        })?),
        None => None,
    };
    new_runtime(ctx, registry, dev_binaries, trace, args.output.clone().unwrap_or_else(|| PathBuf::from(".")))
}

/// A runtime that only inspects files: it runs nothing, so it has no dev
/// binaries, no trace and nowhere to write.
pub fn inspection_runtime(ctx: &Ctx, registry: &Arc<FabricRegistry>) -> CliResult<Arc<dyn RunHost>> {
    new_runtime(ctx, registry, Vec::new(), None, PathBuf::from("."))
}

fn new_runtime(
    ctx: &Ctx,
    registry: &Arc<FabricRegistry>,
    dev_binaries: Vec<PathBuf>,
    trace: Option<Arc<capdag::ProtocolTraceSink>>,
    persist_dir: PathBuf,
) -> CliResult<Arc<dyn RunHost>> {
    ctx.host
        .runtime(RunSetup { registry: registry.clone(), offline: ctx.offline, dev_binaries, trace, persist_dir })
        .map_err(|e| ctx::host_failure("the cartridges could not be hosted", e))
}

/// Run one cap on one value, and return what it gave: how the model commands
/// drive the model cartridge's own caps. The run is shown as any run is.
pub async fn cap_value(ctx: &Ctx, alias: &str, input: Option<Vec<u8>>, progress: Option<ProgressChoice>) -> CliResult<Vec<u8>> {
    let registry = ctx.registry().await?;
    let cap = registry.get_cap(alias).await.map_err(|e| match e {
        capdag::FabricRegistryError::NotFound(_) => {
            CliError::failed(format!("the capability {alias} is not in this fabric")).context(e.to_string())
        }
        e => ctx::registry_failure(e),
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
    let mut slots = Vec::new();
    if let (Some(slot), Some(value)) = (strand.input_slots().get(SINGLE_CAP_INPUT_NODE).cloned(), input) {
        plan_inputs.insert(slot.clone(), PlanInput::Bytes(value));
        is_sequence.insert(slot.clone(), false);
        slots.push((slot, vec![0]));
    }
    let run = PlannedRun {
        label: alias.to_string(),
        strands: vec![PlannedStrand { strand, inputs: plan_inputs, is_sequence, cap_arguments: HashMap::new() }],
        sources: if slots.is_empty() { Vec::new() } else { vec![inputs::file_safe(alias)] },
        slots: vec![slots],
        machine_name: inputs::file_safe(alias),
    };
    let runtime = new_runtime(ctx, &registry, Vec::new(), None, PathBuf::from("."))?;
    let settings = RunSettings { output: None, force: false };
    let mut value: Vec<u8> = Vec::new();
    match execute(ctx, &registry, &settings, progress, runtime, vec![run], true, Results::Into(&mut value)).await? {
        Exit::Run(RunOutcome::Completed) => Ok(value),
        other => Err(CliError { exit: other, ..CliError::failed(format!("{alias} did not complete")) }),
    }
}

/// Where a run's results go.
enum Results<'a> {
    /// To stdout; on the terminal the live view is drawn on, above the block.
    Stdout,
    /// Into what the command reads them from, never shown: a lookup's value,
    /// or nowhere under `--json`, whose records name the files written.
    Into(&'a mut dyn std::io::Write),
}

/// Run every planned run, one after another, its strands side by side,
/// shown as `showing` says (`None`: a lookup's run, not shown).
#[allow(clippy::too_many_arguments)]
async fn execute(
    ctx: &Ctx,
    registry: &Arc<FabricRegistry>,
    settings: &RunSettings,
    showing: Option<ProgressChoice>,
    runtime: Arc<dyn RunHost>,
    runs: Vec<PlannedRun>,
    single_scalar: bool,
    results: Results<'_>,
) -> CliResult<Exit> {
    let output_dir = settings.output.clone();
    let control = Arc::new(RunControl::new());
    let (observer, mut events) = RunObserver::new();
    let mut watch = Watch::new(ctx.term.verbosity);
    let mut shower = Shower::new(&ctx.term, showing)?;
    // A key in the live view is Ctrl-C; otherwise the signal is.
    let signals_ctrl_c = !shower.reads_keys();
    // On the terminal the live view is drawn on, a run's results are printed
    // above the block rather than into it.
    // Whether what is written for stdout is read on a terminal: binary is
    // not dumped there.
    let results_on_terminal = matches!(results, Results::Stdout) && ctx.term.stdout_is_terminal;
    let mut stdout = std::io::stdout();
    let (holds_results, out): (bool, &mut dyn std::io::Write) = match results {
        Results::Stdout => (shower.holds_results(&ctx.term), &mut stdout),
        Results::Into(into) => (false, into),
    };
    // The summary names the files written only where stdout does not: when
    // stdout is the terminal, they are printed there already.
    let names_outputs = ctx.term.json || !ctx.term.stdout_is_terminal;
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
            match runtime.spawn(&observer, key, planned, control.clone()) {
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
                        Ok(changed) => shower.changed(&watch, event.key, changed),
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
                        if let Ok(changed) = watch.apply(&event) {
                            shower.changed(&watch, event.key, changed);
                        }
                    }
                    let (outcome, outputs) = match joined {
                        Ok((strand, result)) => {
                            let failed_at = result.as_ref().err().and_then(|e| e.step_token_id().map(|t| t.as_str().to_string()));
                            let target = EmitTarget {
                                output_dir: emit_dir.clone(),
                                force: settings.force,
                                slots: run.slots[key.strand].clone(),
                                sources: run.sources.clone(),
                                machine_name: run.machine_name.clone(),
                            };
                            let report = if holds_results {
                                let mut held = Vec::new();
                                let report = runtime.finish(&strand, result, &target, &mut held);
                                shower.results(held);
                                report
                            } else {
                                runtime.finish(&strand, result, &target, out)
                            };
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
                            // What could not be written where it was asked is
                            // given to the person another way, before the
                            // summary and the error that says why.
                            let delivered = match &report.failure {
                                Some(StrandFailure::Emit(refusal)) => {
                                    let sink = if holds_results { Stdout::Held } else { Stdout::Out(&mut *out) };
                                    deliver_unsaved(ctx, refusal, &mut shower, sink, results_on_terminal)
                                }
                                _ => Vec::new(),
                            };
                            say_notes(ctx, registry, &mut shower, &report);
                            if let Some(mut failure) = report_failure(registry, &strand, &report, ctx.term.verbosity >= 1) {
                                for line in delivered {
                                    failure = failure.context(line);
                                }
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
                    shower.run_ended(&watch, key, &outputs, names_outputs);
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
    // A lookup's run is not the command's: its records are what it found.
    if ctx.term.json && showing.is_some() {
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
    runtime: &Arc<dyn RunHost>,
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
            StrandNote::Dropped { cap_urn, item: at, message } => (
                format!(
                    "dropped: item {} — {} failed: {}; the other items were kept",
                    at + 1,
                    names::cap_urn(registry, cap_urn),
                    one_line(message)
                ),
                serde_json::json!({
                    "type": "dropped", "step": names::cap_urn(registry, cap_urn), "urn": cap_urn, "item": at,
                    "message": message,
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
        StrandFailure::Emit(refusal) => Some(
            CliError::failed("the run's results could not be written")
                .context(refusal.reason.clone())
                .hint(if refusal.reason.contains("--force") {
                    "pass --force to overwrite them, or -o <dir> to write them elsewhere".to_string()
                } else {
                    "check the output directory".to_string()
                }),
        ),
    }
}

/// Where what a command writes for stdout goes from a run.
enum Stdout<'a> {
    /// Held by the live view, drawn on the terminal stdout is: printed above
    /// its block.
    Held,
    /// Written here.
    Out(&'a mut dyn std::io::Write),
}

/// Give the person what a run made and could not write where it was asked,
/// and say how each was given, for the error to name. Text is written to
/// stdout, each under its name when stdout is a terminal; anything else is
/// written to stdout when stdout is a pipe or a file, and kept in a directory
/// of its own when stdout is a terminal, where its bytes would be noise.
/// Under `--json` stdout is records only: everything is kept, and each kept
/// file is named in an `unsaved` record.
fn deliver_unsaved(
    ctx: &Ctx,
    refusal: &capdag::orchestrator::EmitRefusal,
    shower: &mut Shower,
    mut stdout: Stdout<'_>,
    to_terminal: bool,
) -> Vec<String> {
    use capdag::orchestrator::{is_text_media, UnsavedContent};
    let mut printed: Vec<String> = Vec::new();
    let mut said: Vec<String> = Vec::new();
    let mut kept_in: Option<PathBuf> = None;
    for item in &refusal.unsaved {
        let printable = !ctx.term.json && (is_text_media(&item.media_urn) || !to_terminal);
        if printable {
            if to_terminal {
                shower.say(&format!("{}, not saved:", item.file_name), serde_json::Value::Null);
            }
            let given = match &item.content {
                UnsavedContent::Bytes(data) => Ok(data.clone()),
                UnsavedContent::Part(part) => std::fs::read(part),
            }
            .and_then(|mut data| {
                // On a terminal the next name starts a line of its own.
                if to_terminal && !data.ends_with(b"\n") {
                    data.push(b'\n');
                }
                match &mut stdout {
                    Stdout::Held => {
                        shower.results(data);
                        Ok(())
                    }
                    Stdout::Out(out) => out.write_all(&data).and_then(|()| out.flush()),
                }
            })
            .and_then(|()| match &item.content {
                UnsavedContent::Part(part) => std::fs::remove_file(part),
                UnsavedContent::Bytes(_) => Ok(()),
            });
            match given {
                Ok(()) => printed.push(item.file_name.clone()),
                Err(e) => said.push(format!("{} could not be written to stdout either: {e}", item.file_name)),
            }
            continue;
        }
        let dir = match &kept_in {
            Some(dir) => dir.clone(),
            None => match tempfile::Builder::new().prefix("capdag-kept-").tempdir() {
                Ok(dir) => {
                    let dir = dir.keep();
                    kept_in = Some(dir.clone());
                    dir
                }
                Err(e) => {
                    said.push(match &item.content {
                        UnsavedContent::Part(part) => {
                            format!("{} could not be kept ({e}); it is at {}", item.file_name, part.display())
                        }
                        UnsavedContent::Bytes(_) => format!("{} could not be kept, and is lost: {e}", item.file_name),
                    });
                    continue;
                }
            },
        };
        let path = dir.join(&item.file_name);
        let kept = match &item.content {
            UnsavedContent::Bytes(data) => std::fs::write(&path, data),
            // Across file systems a rename cannot move it: it is copied.
            UnsavedContent::Part(part) => std::fs::rename(part, &path)
                .or_else(|_| std::fs::copy(part, &path).and_then(|_| std::fs::remove_file(part))),
        };
        match kept {
            Ok(()) => {
                said.push(format!("kept instead: {}", path.display()));
                if ctx.term.json {
                    shower.say(
                        "",
                        serde_json::json!({ "type": "unsaved", "output": item.file_name, "kept": path.display().to_string() }),
                    );
                }
            }
            Err(e) => said.push(match &item.content {
                UnsavedContent::Part(part) => format!("{} could not be kept ({e}); it is at {}", item.file_name, part.display()),
                UnsavedContent::Bytes(_) => format!("{} could not be kept, and is lost: {e}", item.file_name),
            }),
        }
    }
    if !printed.is_empty() {
        said.insert(0, format!("written to stdout instead: {}", printed.join(", ")));
    }
    said
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
    use capdag::failure::AttributionClass;
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
fn failure_identity(failure: &capdag::orchestrator::StepFailure) -> String {
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

}
