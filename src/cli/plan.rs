//! `capdag plan <files…> [--to <type>]…`: what files can become, and the
//! machines that make it.
//!
//! Without `--to` it says what the files can become. With one it lists the
//! machines that make it — each in alias form, numbered from 1, with its step
//! count and whether a step needs a model — and `--pick <n>` chooses one to
//! `--save` or `--run`. On a terminal, a target or machine not given is chosen
//! from a list, and so is what to do with the machine.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use super::choose::{choose, Choice, Question};
use super::ctx::Ctx;
use super::error::{CliError, CliResult, Exit};
use super::inputs;
use super::live::ProgressChoice;
use super::names;
use super::term::emit_record;
use crate::fabric::registry::FabricRegistry;
use crate::planner as p;
use crate::{CapUrn, MediaUrn};

#[derive(Debug, clap::Args)]
pub struct Args {
    /// The files, directories or globs to plan for
    #[arg(value_name = "FILES", required = true)]
    pub files: Vec<String>,
    /// What to make: a media alias (`txt`, `png`), an extension or a media URN (repeatable)
    #[arg(long, value_name = "TYPE")]
    pub to: Vec<String>,
    /// The machine to use, by its number in the list
    #[arg(long, value_name = "N")]
    pub pick: Option<usize>,
    /// Write the chosen machine to this file
    #[arg(long, value_name = "FILE")]
    pub save: Option<PathBuf>,
    /// Run the chosen machine on the files
    #[arg(long)]
    pub run: bool,
    /// List every machine, not only the first 10
    #[arg(long)]
    pub all: bool,
    /// Where files of several types meet: combine them, or treat each on its own
    #[arg(long, value_name = "HOW", value_parser = ["auto", "combine", "independent"], default_value = "auto")]
    pub converge: String,
    /// Where converging paths meet
    #[arg(long = "where", value_name = "WHERE", default_value = "auto")]
    pub location: String,
    /// How converging paths are joined
    #[arg(long, value_name = "HOW", value_parser = ["any", "generalize", "collect", "merge"], default_value = "any")]
    pub mechanism: String,
    /// The order machines are listed in
    #[arg(long, value_name = "ORDER", value_parser = ["intent", "shortest", "cost"], default_value = "intent")]
    pub rank: String,
    /// How many steps a machine may have at most
    #[arg(long, value_name = "N")]
    pub depth: Option<usize>,
    /// How many paths are searched at most
    #[arg(long, value_name = "N")]
    pub max_paths: Option<usize>,
    /// Where results of --run are written (default: the working directory)
    #[arg(short, long, value_name = "DIR")]
    pub output: Option<PathBuf>,
    /// Overwrite result files that exist (with --run), or the --save file
    #[arg(long)]
    pub force: bool,
    /// How a --run is shown while it goes
    #[arg(long, value_enum, default_value_t = ProgressChoice::Auto, value_name = "HOW")]
    pub progress: ProgressChoice,
    /// Use this cartridge binary, or every binary in this directory (repeatable, with --run)
    #[arg(long = "dev-bin", value_name = "PATH")]
    pub dev_bins: Vec<PathBuf>,
}

/// How many machines are listed without --all.
const LISTED: usize = 10;

pub async fn plan(ctx: &Ctx, args: Args) -> CliResult<Exit> {
    let registry = ctx.registry().await?;
    let mut files: Vec<PathBuf> = Vec::new();
    for given in &args.files {
        files.extend(inputs::expand(given)?);
    }
    files.sort();
    // Files of one type are one sequence; each type is its own source.
    let mut groups: Vec<(MediaUrn, usize)> = Vec::new();
    for file in &files {
        let media = inputs::detect(file, &registry).await?;
        match groups.iter_mut().find(|(m, _)| m.is_equivalent(&media).unwrap_or(false)) {
            Some((_, count)) => *count += 1,
            None => groups.push((media, 1)),
        }
    }
    let sources: Vec<p::SourceSpec> = groups
        .iter()
        .map(|(media, count)| if *count > 1 { p::SourceSpec::sequence(media.clone()) } else { p::SourceSpec::single(media.clone()) })
        .collect();
    let what = groups
        .iter()
        .map(|(media, count)| format!("{count} {}", names::media(&registry, media)))
        .collect::<Vec<_>>()
        .join(" and ");
    let fab = cap_graph(&registry).await?;
    let max_depth = args.depth.unwrap_or(p::PlanRequest::DEFAULT_MAX_DEPTH);
    let discovered = fab
        .discover_convergent_targets(&sources, max_depth)
        .map_err(|e| CliError::failed(format!("what {what} can become could not be worked out")).context(e.to_string()))?;
    if discovered.targets.is_empty() {
        return Err(CliError::failed(format!("no capability takes {what}"))
            .hint("see which capabilities there are: capdag caps"));
    }
    let wants_machine = args.save.is_some() || args.run || args.pick.is_some();

    // ── The target ──
    let targets: Vec<MediaUrn> = if !args.to.is_empty() {
        let mut out = Vec::new();
        for token in &args.to {
            out.push(match_target(&registry, &discovered.targets, token, &what).await?);
        }
        out
    } else if ctx.term.interactive || wants_machine {
        let choices: Vec<Choice> = discovered.targets.iter().map(|t| target_choice(&registry, t)).collect();
        let index = choose(ctx, &Question { what: "what to make", flag: "--to" }, &choices).await?;
        vec![discovered.targets[index].media_def.clone()]
    } else {
        print_targets(ctx, &registry, &discovered.targets, &what, &args.files);
        return Ok(Exit::Ok);
    };

    // ── The machines ──
    let request = request(&args, sources.clone(), targets.clone())?;
    let outcome = fab
        .plan(&request, registry.as_ref())
        .map_err(|e| CliError::failed("no machine could be planned").context(e.to_string()))?;
    let target_names = targets.iter().map(|t| names::media(&registry, t)).collect::<Vec<_>>().join(", ");
    if outcome.candidates.is_empty() {
        return Err(CliError::failed(format!("no machine makes {target_names} of {what}"))
            .hint(format!("see what it can become: capdag plan {}", args.files.join(" "))));
    }
    // Reading the bundled cartridges verifies each of them: once per command,
    // not once per machine.
    let bundled = super::cartridge::bundled_caps().await?;
    let mut machines = Vec::with_capacity(outcome.candidates.len());
    for candidate in &outcome.candidates {
        machines.push(describe(&registry, &bundled, candidate).await?);
    }
    let chosen: Option<usize> = match args.pick {
        Some(n) if n >= 1 && n <= machines.len() => Some(n - 1),
        Some(n) => {
            return Err(CliError::usage(format!("--pick {n}: there are {} machines, numbered from 1", machines.len()))
                .hint(format!("choose one of 1 to {}", machines.len())))
        }
        None if machines.len() == 1 => Some(0),
        None if ctx.term.interactive || wants_machine => {
            let choices: Vec<Choice> = machines.iter().enumerate().map(|(i, m)| m.choice(i)).collect();
            Some(choose(ctx, &Question { what: "the machine", flag: "--pick" }, &choices).await?)
        }
        None => None,
    };
    let Some(chosen) = chosen else {
        print_machines(ctx, &machines, &target_names, &what, args.all, &args.files);
        return Ok(Exit::Ok);
    };
    let machine = &machines[chosen];

    // ── What to do with it ──
    let (save, run) = if args.save.is_some() || args.run {
        (args.save.clone(), args.run)
    } else if ctx.term.interactive {
        let default_file = PathBuf::from(format!(
            "{}-to-{}.machine",
            inputs::stem(&files[0]),
            inputs::file_safe(&names::media(&registry, &targets[0]))
        ));
        let actions = [
            Choice { label: "run".into(), detail: format!("run it on {}", args.files.join(" ")), urn: None, value: "--run".into() },
            Choice { label: "save".into(), detail: format!("save it as {}", default_file.display()), urn: None, value: format!("--save {}", default_file.display()) },
            Choice { label: "print".into(), detail: "print it, and do nothing else".into(), urn: None, value: String::new() },
        ];
        match choose(ctx, &Question { what: "what to do with it", flag: "--run or --save" }, &actions).await? {
            0 => (None, true),
            1 => (Some(default_file), false),
            _ => (None, false),
        }
    } else {
        (None, false)
    };
    if let Some(path) = &save {
        if path.exists() && !args.force {
            return Err(CliError::usage(format!("{} exists", path.display()))
                .hint("pass --force to replace it, or --save another file"));
        }
        std::fs::write(path, format!("{}\n", machine.notation)).map_err(|e| {
            CliError::failed(format!("the machine could not be saved to {}", path.display())).context(e.to_string())
        })?;
        if ctx.term.json {
            emit_record(&serde_json::json!({ "type": "saved", "file": path.display().to_string(), "notation": machine.notation }));
        } else {
            eprintln!("Saved machine {} to {}", chosen + 1, path.display());
            if !run {
                eprintln!("Run it: capdag run {} {}", path.display(), args.files.join(" "));
            }
        }
    }
    if !run {
        if save.is_none() {
            if ctx.term.json {
                emit_record(&machine.record(chosen));
            } else {
                println!("{}", machine.notation);
            }
        }
        return Ok(Exit::Ok);
    }
    // Run it: from the saved file, or from a file of its own, removed after.
    let (file, scratch) = match save {
        Some(path) => (path, None),
        None => {
            let dir = std::env::temp_dir().join(format!("capdag-plan-{}", std::process::id()));
            std::fs::create_dir_all(&dir)
                .map_err(|e| CliError::failed("a place for the planned machine could not be made").context(e.to_string()))?;
            let path = dir.join(format!("{}.machine", inputs::stem(&files[0])));
            std::fs::write(&path, &machine.notation)
                .map_err(|e| CliError::failed("the planned machine could not be written").context(e.to_string()))?;
            (path, Some(dir))
        }
    };
    let run_args = super::run::Args {
        target: file.display().to_string(),
        rest: files.iter().map(|f| f.display().to_string()).collect(),
        output: args.output,
        force: args.force,
        inputs: Vec::new(),
        args: Vec::new(),
        to: None,
        selector: None,
        progress: args.progress,
        trace: None,
        dev_bins: args.dev_bins,
    };
    let ran = super::run::run(ctx, run_args).await;
    if let Some(dir) = scratch {
        let _ = std::fs::remove_dir_all(dir);
    }
    ran
}

/// The live cap graph, from the fabric's cached caps and every media type with
/// a file extension.
async fn cap_graph(registry: &Arc<FabricRegistry>) -> CliResult<p::LiveCapFab> {
    let caps = registry
        .get_cached_caps()
        .await
        .map_err(|e| CliError::failed("the capabilities could not be read from the fabric cache").context(e.to_string()))?;
    let media = registry
        .get_cached_media_defs()
        .await
        .map_err(|e| CliError::failed("the media types could not be read from the fabric cache").context(e.to_string()))?;
    let bookends: HashSet<MediaUrn> = media
        .iter()
        .filter(|d| !d.extensions.is_empty())
        .filter_map(|d| MediaUrn::from_string(&d.urn).ok())
        .collect();
    let mut fab = p::LiveCapFab::new();
    fab.sync_from_caps(&caps, &bookends);
    Ok(fab)
}

fn request(args: &Args, sources: Vec<p::SourceSpec>, targets: Vec<MediaUrn>) -> CliResult<p::PlanRequest> {
    let presence = match args.converge.as_str() {
        "combine" => p::ConvergencePresence::Converged,
        "independent" => p::ConvergencePresence::Independent,
        _ => p::ConvergencePresence::Auto,
    };
    let location = match args.location.as_str() {
        "auto" => p::ConvergenceLocation::Auto,
        "earliest" => p::ConvergenceLocation::Earliest,
        "latest" => p::ConvergenceLocation::Latest,
        "source" => p::ConvergenceLocation::AtSource,
        "target" => p::ConvergenceLocation::AtTarget,
        other => match other.strip_prefix("depth=").map(str::parse::<usize>) {
            Some(Ok(depth)) => p::ConvergenceLocation::AtDepth(depth),
            _ => {
                return Err(CliError::usage(format!("--where '{other}' is not a place converging paths meet"))
                    .context("it is auto, earliest, latest, source, target or depth=<n>"))
            }
        },
    };
    let mechanism = match args.mechanism.as_str() {
        "generalize" => p::ConvergenceMechanism::Generalize,
        "collect" => p::ConvergenceMechanism::Collect,
        "merge" => p::ConvergenceMechanism::Merge,
        _ => p::ConvergenceMechanism::Any,
    };
    let ranking = match args.rank.as_str() {
        "shortest" => p::RankPolicy::Shortest,
        "cost" => p::RankPolicy::Cost,
        _ => p::RankPolicy::Intent,
    };
    let configured = args.converge != "auto" || args.location != "auto" || args.mechanism != "any";
    Ok(p::PlanRequest {
        sources,
        targets: p::TargetSpec::Exact(targets),
        convergence: p::ConvergencePolicy {
            presence,
            location,
            mechanism,
            at_type: None,
            arity: p::ConvergenceArity::Auto,
        },
        divergence: p::DivergencePolicy::default(),
        ranking,
        search: p::SearchDirection::Auto,
        mode: if configured { p::PlanMode::Configured } else { p::PlanMode::Auto },
        max_depth: args.depth.unwrap_or(p::PlanRequest::DEFAULT_MAX_DEPTH),
        max_paths: args.max_paths.unwrap_or(p::PlanRequest::DEFAULT_MAX_PATHS),
        max_candidates: p::PlanRequest::DEFAULT_MAX_CANDIDATES,
    })
}

/// A `--to` token as a media type: a media alias, a URN (with `:`), or an
/// extension (`png` → `media:ext=png`).
pub async fn target_media(registry: &FabricRegistry, token: &str) -> CliResult<MediaUrn> {
    let urn = if token.contains(':') {
        token.to_string()
    } else {
        match registry
            .resolve_alias_typed(token, Some(crate::fabric::alias::AliasTargetKind::Media))
            .await
        {
            Ok(target) => target,
            Err(e) => {
                super::ctx::nothing_by_that_name(e)?;
                format!("media:ext={token}")
            }
        }
    };
    MediaUrn::from_string(&urn).map_err(|e| {
        CliError::usage(format!("--to '{token}' is not a media type"))
            .context(e.to_string())
            .hint("give a media alias such as txt or png, an extension, or a media URN")
    })
}

/// The reachable target a `--to` means: the one it names, or the one reachable
/// target of the kind it names. Several of that kind is a choice to make.
async fn match_target(
    registry: &FabricRegistry,
    reachable: &[p::ConvergentTargetInfo],
    token: &str,
    what: &str,
) -> CliResult<MediaUrn> {
    let wanted = target_media(registry, token).await?;
    if let Some(exact) = reachable.iter().find(|t| t.media_def.is_equivalent(&wanted).unwrap_or(false)) {
        return Ok(exact.media_def.clone());
    }
    let fitting: Vec<&p::ConvergentTargetInfo> = reachable
        .iter()
        .filter(|t| t.media_def.conforms_to(&wanted).unwrap_or(false))
        .collect();
    match fitting.as_slice() {
        [one] => Ok(one.media_def.clone()),
        [] => Err(CliError::failed(format!("{what} cannot become {token}"))
            .context(format!(
                "it can become: {}",
                reachable.iter().take(8).map(|t| names::media(registry, &t.media_def)).collect::<Vec<_>>().join(", ")
            ))
            .hint("see everything it can become: capdag plan <files>")),
        several => {
            let mut error = CliError::usage(format!("--to {token} could mean {} different things", several.len()));
            for t in several.iter().take(8) {
                error = error.context(format!("--to {}  {}", names::media(registry, &t.media_def), t.display_name));
            }
            Err(error.hint("say which with one of the --to values above"))
        }
    }
}

fn target_choice(registry: &FabricRegistry, target: &p::ConvergentTargetInfo) -> Choice {
    let alias = names::media(registry, &target.media_def);
    let title = registry
        .get_cached_media_def(&target.media_def.to_string())
        .map(|d| d.title)
        .unwrap_or_else(|_| target.display_name.clone());
    let how = if target.apex.is_some() { "combine all" } else { "each" };
    Choice {
        label: alias.clone(),
        detail: format!("{title} · {how} · {} step{}", target.min_total_steps, if target.min_total_steps == 1 { "" } else { "s" }),
        urn: Some(target.media_def.to_string()),
        value: alias,
    }
}

fn print_targets(ctx: &Ctx, registry: &FabricRegistry, targets: &[p::ConvergentTargetInfo], what: &str, files: &[String]) {
    let choices: Vec<Choice> = targets.iter().map(|t| target_choice(registry, t)).collect();
    if ctx.term.json {
        for choice in &choices {
            emit_record(&serde_json::json!({
                "type": "target", "alias": choice.label, "detail": choice.detail, "urn": choice.urn,
            }));
        }
        return;
    }
    println!("{what} can become:");
    let width = choices.iter().map(|c| c.label.chars().count()).max().unwrap_or(0).min(28);
    for choice in &choices {
        let urn = if ctx.term.verbosity >= 1 { format!("  {}", choice.urn.as_deref().unwrap_or("")) } else { String::new() };
        println!("  {:<width$}  {}{urn}", choice.label, choice.detail);
    }
    eprintln!("\nFind machines that make one: capdag plan {} --to {}", files.join(" "), choices[0].label);
}

/// A planned machine, as it is shown.
struct Planned {
    notation: String,
    canonical: String,
    steps: usize,
    model: bool,
    downloads: bool,
}

impl Planned {
    fn summary(&self) -> String {
        let mut parts = vec![format!("{} step{}", self.steps, if self.steps == 1 { "" } else { "s" })];
        if self.model {
            parts.push("runs a model".into());
        }
        if self.downloads {
            parts.push("needs a cartridge download".into());
        }
        parts.join(" · ")
    }

    /// The machine as an option: `--pick <its number>`.
    fn choice(&self, index: usize) -> Choice {
        Choice {
            label: self.notation.replace('\n', " "),
            detail: self.summary(),
            urn: Some(self.canonical.clone()),
            value: (index + 1).to_string(),
        }
    }

    fn record(&self, index: usize) -> serde_json::Value {
        serde_json::json!({
            "type": "machine",
            "pick": index + 1,
            "notation": self.notation,
            "canonical": self.canonical,
            "steps": self.steps,
            "model": self.model,
            "downloads": self.downloads,
        })
    }
}

/// A planned machine as it is shown: `bundled` is every cap the cartridges
/// bundled with capdag provide.
async fn describe(registry: &Arc<FabricRegistry>, bundled: &[CapUrn], candidate: &p::PlanCandidate) -> CliResult<Planned> {
    let machine = crate::machine::parse_machine_async(&candidate.notation, registry)
        .await
        .map_err(|e| CliError::failed("a planned machine could not be read back").context(e.to_string()))?;
    let notation = names::machine(registry, &machine)
        .map_err(|e| CliError::failed("a planned machine could not be written in alias form").context(e))?;
    let mut model = false;
    let mut downloads = false;
    for strand in machine.strands() {
        for edge in strand.edges() {
            let cap = registry
                .get_cap(&edge.cap_urn.to_string())
                .await
                .map_err(super::ctx::registry_failure)?;
            model |= cap.default_model_spec.is_some();
            downloads |= !bundled.iter().any(|b| b.is_equivalent(&edge.cap_urn));
        }
    }
    Ok(Planned {
        notation,
        canonical: candidate.notation.clone(),
        steps: candidate.cost.cap_steps,
        model,
        downloads,
    })
}

fn print_machines(ctx: &Ctx, machines: &[Planned], target: &str, what: &str, all: bool, files: &[String]) {
    if ctx.term.json {
        for (index, machine) in machines.iter().enumerate() {
            emit_record(&machine.record(index));
        }
        return;
    }
    println!("{} machine{} make{} {target} of {what}:", machines.len(), if machines.len() == 1 { "" } else { "s" }, if machines.len() == 1 { "s" } else { "" });
    let shown = if all { machines.len() } else { machines.len().min(LISTED) };
    for (index, machine) in machines.iter().take(shown).enumerate() {
        println!("\n {:>2}. {}", index + 1, ctx.term.style.dim(&machine.summary()));
        for line in machine.notation.lines() {
            println!("     {line}");
        }
        if ctx.term.verbosity >= 1 {
            println!("     {}", ctx.term.style.dim(&machine.canonical));
        }
    }
    if shown < machines.len() {
        println!("\n … and {} more: --all lists them", machines.len() - shown);
    }
    eprintln!(
        "\nSave one: capdag plan {} --to <type> --pick <n> --save <file>\nRun one:  capdag plan {} --to <type> --pick <n> --run",
        files.join(" "),
        files.join(" ")
    );
}
