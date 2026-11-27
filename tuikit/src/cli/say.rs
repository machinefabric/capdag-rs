//! What a `run` or `plan` line that does not run is told: the model's verdict,
//! said with the file, input, option or machine it is about, and what to type
//! next. The exit status is the model's (`CliVerdict.exit`).

use super::ask::{self, Choice, Question};
use super::ctx::Ctx;
use super::error::{CliError, Exit};
use super::formal::{self as f, index};
use super::line::{MachineFacts, RunState, World};
use super::names;

/// The error a verdict that ends a line ends the command with.
pub async fn line_verdict_error(
    ctx: &Ctx,
    world: &mut World<'_>,
    state: &RunState,
    positionals: &[f::Positional],
    verdict: &f::CliVerdict,
) -> CliError {
    let error = match verdict {
        f::CliVerdict::Refused { r: f::CliRefusal::Line { r } } => line_refusal(world, state, r),
        f::CliVerdict::Refused { r } => cli_refusal(r),
        f::CliVerdict::Is { v } => match v {
            f::Verdict::Unresolved => unresolved(world, state, positionals),
            f::Verdict::OutputUnwritable => {
                let dir = world.extras.output.as_ref().map(|d| d.display().to_string()).unwrap_or_default();
                CliError::failed(format!("the output directory '{dir}' cannot be made or written in"))
                    .hint("give -o a directory you can write to")
            }
            f::Verdict::Names { s } => named(ctx, world, state, positionals, s).await,
            f::Verdict::Extra { s } => extra(world, state, positionals, s).await,
            f::Verdict::Nothing { s } => nothing(world, state, positionals, s),
            f::Verdict::Declined => ask::declined(),
            other => CliError::failed(format!("a line came to {other:?}, which ends nothing"))
                .hint("report it: only a verdict that ends the command is said as one"),
        },
    };
    with_exit(error, verdict)
}

/// An error with the exit status the model gives its verdict.
pub fn with_exit(mut error: CliError, verdict: &f::CliVerdict) -> CliError {
    error.exit = match f::exit_of(verdict) {
        Some(0) => Exit::Ok,
        Some(2) => Exit::Usage,
        Some(130) => Exit::Declined,
        Some(1) | None => Exit::Failed,
        Some(other) => panic!("the model ended a line with exit {other}, which no verdict has"),
    };
    error
}

/// A refusal of the whole command: the model's sentence.
pub fn cli_refusal(refusal: &f::CliRefusal) -> CliError {
    CliError::usage(f::says(refusal))
}

/// The machine a line runs, as far as it is known: the one it writes, or the
/// first one planning found.
fn machine_of(world: &World<'_>, state: &RunState, positionals: &[f::Positional]) -> Option<usize> {
    match &state.line.machine {
        Some(machine) => Some(index(&machine.base)),
        None => world.plan(positionals).and_then(|plan| plan.machines.first().copied()),
    }
}

fn unresolved(world: &World<'_>, state: &RunState, positionals: &[f::Positional]) -> CliError {
    match machine_of(world, state, positionals).and_then(|m| world.machine(m)) {
        Some(MachineFacts::Unresolved(error)) => {
            let mut error = error.clone();
            error.exit = Exit::Failed;
            error
        }
        _ => CliError::failed("the machine does not resolve").hint("check it with: capdag check <machine>"),
    }
}

/// The inputs of a resolved machine, said: `doc (pdf), pages (txt)`.
fn inputs_said(world: &World<'_>, m: usize) -> String {
    match world.machine(m).and_then(MachineFacts::resolved) {
        Some(resolved) if !resolved.inputs.is_empty() => resolved
            .inputs
            .iter()
            .map(|i| format!("{} ({})", i.name, names::media(&world.registry, &i.media_urn)))
            .collect::<Vec<_>>()
            .join(", "),
        _ => "it has none".to_string(),
    }
}

/// The options a resolved machine leaves to the run, as `--arg` names them.
fn options_said(world: &World<'_>, m: usize) -> String {
    match world.machine(m).and_then(MachineFacts::resolved) {
        Some(resolved) if !resolved.options.is_empty() => (0..resolved.options.len())
            .map(|j| format!("--arg {}=<{}>", resolved.flag_name(j), names::media_urn(&world.registry, &resolved.options[j].media_urn)))
            .collect::<Vec<_>>()
            .join(", "),
        _ => "it leaves none".to_string(),
    }
}

fn line_refusal(world: &World<'_>, state: &RunState, refusal: &f::Refusal) -> CliError {
    let says = f::refusal::says(refusal.clone());
    let m = state.line.machine.as_ref().map(|machine| index(&machine.base));
    match refusal {
        f::Refusal::NoSuchType => {
            let unknown: Vec<String> = world
                .extras
                .to
                .iter()
                .map(|t| format!("--to {t}"))
                .chain(world.extras.from.iter().map(|t| format!("--from {t}")))
                .collect();
            CliError::usage(says).context(format!("given: {}", unknown.join(", ")))
        }
        f::Refusal::OutputIsFile => CliError::usage(format!(
            "{}: {says}",
            world.extras.output.as_ref().map(|d| d.display().to_string()).unwrap_or_default()
        )),
        f::Refusal::NoSuchFile { p } => {
            CliError::usage(format!("{}: {says}", world.tables.files[index(p)].path.display()))
                .hint("check the path; inputs are files, directories or globs")
        }
        f::Refusal::NoSuchLive { r } => CliError::usage(format!("{}: {says}", world.tables.lives[index(r)]))
            .hint("`capdag info <type>` shows a media type"),
        f::Refusal::NoSuchMachine => {
            let shown = m.map(|m| world.tables.machines[m].shown()).unwrap_or_default();
            CliError::usage(format!("{shown}: {says}")).hint("`capdag plan <files> --to <type> --save <file>` writes one")
        }
        f::Refusal::NoSuchInput { j } => {
            let known = m
                .and_then(|m| world.machine(m))
                .and_then(MachineFacts::resolved)
                .map(|r| r.inputs.len())
                .unwrap_or(0);
            let name = state.unknown_inputs.get(index(j).saturating_sub(known)).cloned().unwrap_or_default();
            let mut error = CliError::usage(format!("'{name}': {says}"));
            if let Some(m) = m {
                error = error.context(format!("its inputs: {}", inputs_said(world, m)));
            }
            error
        }
        f::Refusal::NoSuchOption { j } => {
            let known = m
                .and_then(|m| world.machine(m))
                .and_then(MachineFacts::resolved)
                .map(|r| r.options.len())
                .unwrap_or(0);
            let name = state.unknown_options.get(index(j).saturating_sub(known)).cloned().unwrap_or_default();
            let mut error = CliError::usage(format!("'{name}': {says}"));
            if let Some(m) = m {
                error = error.context(format!("what it leaves to the run: {}", options_said(world, m)));
            }
            error
        }
        f::Refusal::OptionShared { j } => {
            let mut error = CliError::usage(says);
            if let Some(resolved) = m.and_then(|m| world.machine(m)).and_then(MachineFacts::resolved) {
                let name = &resolved.options[index(j)].name;
                let steps: Vec<String> = resolved
                    .options
                    .iter()
                    .filter(|o| &o.name == name)
                    .map(|o| format!("--arg {}.{}=…", o.step, o.name))
                    .collect();
                error = error.context(format!("'{name}' is an option of: {}", steps.join(", ")));
            }
            error
        }
        f::Refusal::Offline => {
            let mut error = CliError::usage(says);
            if let Some(needs) = m.and_then(|m| world.needs(m)) {
                for (cap, _) in &needs.missing {
                    error = error.context(format!("{} is provided by no cartridge here", names::cap_urn(&world.registry, &cap.to_string())));
                }
            }
            error.context("a cartridge installed from a registry is verified against it before it runs").hint("drop --offline")
        }
        f::Refusal::NothingToRun => {
            CliError::usage(says).hint("capdag run <machine> [inputs], or capdag run <inputs> --to <type>")
        }
        _ => CliError::usage(says),
    }
}

/// What a stage that nobody can be asked is: its choices or the value it
/// needs, and the flag that gives it.
async fn named(ctx: &Ctx, world: &mut World<'_>, state: &RunState, positionals: &[f::Positional], stage: &f::Stage) -> CliError {
    match stage {
        f::Stage::StdinType => {
            ask::named_value(ctx, &ask::ValueQuestion::stdin_type())
        }
        f::Stage::Target => {
            let choices: Vec<Choice> = world
                .plan(positionals)
                .map(|plan| plan.targets.iter().map(|t| super::plan::target_choice(&world.registry, t)).collect())
                .unwrap_or_default();
            ask::named(ctx, &Question { what: "what to make".into(), flag: "--to ".into() }, &choices)
        }
        f::Stage::Machine => {
            let choices: Vec<Choice> = world
                .plan(positionals)
                .map(|plan| {
                    plan.described
                        .iter()
                        .map(|d| Choice {
                            label: d.notation.replace('\n', " "),
                            detail: d.summary(),
                            urn: Some(d.canonical.clone()),
                            value: d.notation.replace('\n', ""),
                        })
                        .collect()
                })
                .unwrap_or_default();
            ask::named(ctx, &Question { what: "the machine".into(), flag: "capdag run ".into() }, &choices)
        }
        f::Stage::Path { p } => match candidates(world, state, positionals, Some(index(p))).await {
            Ok((m, js)) => {
                let file = world.tables.files[index(p)].path.display().to_string();
                let choices = input_choices(world, m, &js, &format!("=@{file}"));
                ask::named(ctx, &Question { what: format!("the input {file} goes to"), flag: "--input ".into() }, &choices)
            }
            Err(error) => error,
        },
        f::Stage::Stdin => match candidates(world, state, positionals, None).await {
            Ok((m, js)) => {
                let choices = input_choices(world, m, &js, "=-");
                ask::named(ctx, &Question { what: "the input - goes to".into(), flag: "--input ".into() }, &choices)
            }
            Err(error) => error,
        },
        f::Stage::Input { j } => match written_machine(world, state, positionals).await {
            Ok(m) => {
                let resolved = world.machine(m).and_then(MachineFacts::resolved).expect("asked of a machine that resolves");
                let input = &resolved.inputs[index(j)];
                ask::named_value(ctx, &ask::ValueQuestion::input(&input.name, &names::media(&world.registry, &input.media_urn)))
            }
            Err(error) => error,
        },
        f::Stage::Option { j } => match written_machine(world, state, positionals).await {
            Ok(m) => {
                let resolved = world.machine(m).and_then(MachineFacts::resolved).expect("asked of a machine that resolves");
                let option = &resolved.options[index(j)];
                let question = Question {
                    what: format!("{} of step {} ({})", option.name, option.step, names::media_urn(&world.registry, &option.media_urn)),
                    flag: format!("--arg {}=", resolved.flag_name(index(j))),
                };
                match &option.allowed {
                    Some(values) => {
                        let choices: Vec<Choice> = values
                            .iter()
                            .map(|value| Choice { label: value.clone(), detail: String::new(), urn: None, value: value.clone() })
                            .collect();
                        ask::named(ctx, &question, &choices)
                    }
                    None => ask::named_value(
                        ctx,
                        &ask::ValueQuestion::option(
                            &option.name,
                            &option.step,
                            &resolved.flag_name(index(j)),
                            &names::media_urn(&world.registry, &option.media_urn),
                        ),
                    ),
                }
            }
            Err(error) => error,
        },
        f::Stage::Provider => match written_machine(world, state, positionals).await {
            Ok(m) => match world.needs(m).and_then(|needs| needs.missing.iter().find(|(_, p)| p.len() > 1).cloned()) {
                Some((cap, providers)) => {
                    let cap_name = names::cap_urn(&world.registry, &cap.to_string());
                    let choices: Vec<Choice> = providers
                        .iter()
                        .map(|id| Choice { label: id.clone(), detail: format!("provides {cap_name}"), urn: None, value: id.clone() })
                        .collect();
                    ask::named(
                        ctx,
                        &Question { what: format!("the cartridge to install for {cap_name}"), flag: "capdag cartridge install ".into() },
                        &choices,
                    )
                }
                None => CliError::failed("a cartridge was to be chosen and none was known").hint("report it"),
            },
            Err(error) => error,
        },
        f::Stage::Capability | f::Stage::Model => {
            CliError::failed("a line named a choice of another command").hint("report it")
        }
    }
}

/// What has nowhere left to go at a stage.
async fn extra(world: &mut World<'_>, state: &RunState, positionals: &[f::Positional], stage: &f::Stage) -> CliError {
    match stage {
        f::Stage::Path { p } => {
            let file = world.tables.files[index(p)].path.display().to_string();
            let mut error = CliError::usage(format!("{file} fits only inputs a live source feeds"));
            if let Ok(m) = written_machine(world, state, positionals).await {
                error = error.context(format!("its inputs: {}", inputs_said(world, m)));
            }
            error.hint(format!("give it by name: --input <name>=@{file}, or leave it out"))
        }
        f::Stage::Stdin => CliError::usage("- has nowhere to go: every input it could go to is given something")
            .hint("give it by name: --input <name>=-, or leave it out"),
        other => CliError::usage(format!("what the line gives at {other:?} has nowhere to go")),
    }
}

/// A stage with no way.
fn nothing(world: &World<'_>, state: &RunState, positionals: &[f::Positional], stage: &f::Stage) -> CliError {
    let what = world.plan(positionals).map(|plan| plan.what.clone()).unwrap_or_default();
    match stage {
        f::Stage::Target => CliError::failed(format!("nothing can be made of {what}"))
            .hint("see which capabilities there are: capdag caps"),
        f::Stage::Machine => {
            if let Some(plan) = world.plan(positionals) {
                for excluded in &plan.excluded {
                    super::plan::say_excluded(world.ctx, excluded);
                }
            }
            let mut error = CliError::failed(format!("no machine makes {} of {what}", world.extras.to.join(", ")));
            if let Some(plan) = world.plan(positionals) {
                if !plan.excluded.is_empty() {
                    error = error.context(match plan.excluded.len() {
                        1 => "a way was found and not offered: it is named above".to_string(),
                        n => format!("{n} ways were found and not offered: each is named above"),
                    });
                }
            }
            error.hint("see what the inputs can become: capdag plan <inputs>")
        }
        f::Stage::Path { p } => {
            let file = &world.tables.files[index(p)].path;
            let kind = world
                .media_of(index(p))
                .map(|media| format!(" ({})", names::media(&world.registry, media)))
                .unwrap_or_default();
            let mut error = CliError::failed(format!("{}{kind} is not what this machine takes", file.display()));
            if let Some(m) = machine_of(world, state, positionals) {
                error = error.context(format!("its inputs: {}", inputs_said(world, m)));
            }
            error.hint(format!("see what it can become: capdag plan {}", super::shell_word(&file.display().to_string())))
        }
        f::Stage::Option { j } => {
            let mut error = CliError::failed("a step option the machine leaves to the run has no value it can take");
            if let Some(resolved) = machine_of(world, state, positionals).and_then(|m| world.machine(m)).and_then(MachineFacts::resolved) {
                if let Some(option) = resolved.options.get(index(j)) {
                    error = CliError::failed(format!("{} of step {} has no value it can take", option.name, option.step));
                }
            }
            error.hint("report it: its type lists no values")
        }
        f::Stage::Provider => {
            let mut error = CliError::failed("the run needs a cartridge no registry provides");
            if let Some(needs) = machine_of(world, state, positionals).and_then(|m| world.needs(m)) {
                for (cap, providers) in &needs.missing {
                    if providers.is_empty() {
                        error = error.context(format!("{} is provided by no cartridge", names::cap_urn(&world.registry, &cap.to_string())));
                    }
                }
            }
            if world.ctx.host.cartridge_registry().is_none() {
                error = error.context("this capdag is a development build, bound to no cartridge registry");
            }
            error.hint("`capdag dev install <project>` installs a cartridge you build; --dev-bin runs one")
        }
        other => CliError::failed(format!("there is no way at {other:?}")),
    }
}

/// The machine the line runs, written as the model reads it.
async fn written_machine(world: &mut World<'_>, state: &RunState, positionals: &[f::Positional]) -> Result<usize, CliError> {
    let line = state.line.clone();
    let written = world.settle(positionals, |w| f::run_line::written(w, line.clone())).await?;
    Ok(index(&written.machine.as_ref().expect("the line that runs a machine writes it").base))
}

/// The inputs a file given by position (`p`), or `-` (`None`), can go to.
async fn candidates(
    world: &mut World<'_>,
    state: &RunState,
    positionals: &[f::Positional],
    p: Option<usize>,
) -> Result<(usize, Vec<usize>), CliError> {
    let line = state.line.clone();
    let written = world.settle(positionals, |w| f::run_line::written(w, line.clone())).await?;
    let machine = written.machine.clone().expect("the line that runs a machine writes it");
    let js = match p {
        Some(p) => {
            world
                .settle(positionals, |w| f::path_candidates(w, machine.clone(), written.clone(), f::nat(p)))
                .await?
        }
        None => world.settle(positionals, |w| f::stdin_candidates(w, machine.clone(), written.clone())).await?,
    };
    Ok((index(&machine.base), js.iter().map(index).collect()))
}

fn input_choices(world: &World<'_>, m: usize, js: &[usize], suffix: &str) -> Vec<Choice> {
    let resolved = world.machine(m).and_then(MachineFacts::resolved).expect("asked of a machine that resolves");
    js.iter()
        .map(|&j| Choice {
            label: resolved.inputs[j].name.clone(),
            detail: names::media(&world.registry, &resolved.inputs[j].media_urn),
            urn: Some(resolved.inputs[j].media_urn.to_string()),
            value: format!("{}{suffix}", resolved.inputs[j].name),
        })
        .collect()
}
