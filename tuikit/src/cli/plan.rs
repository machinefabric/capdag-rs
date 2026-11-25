//! `capdag plan <files…> [--to <type>]…`: what files can become, and the
//! machines that make it.
//!
//! Without `--to` it says what the files can become. With one it lists the
//! machines that make it — every machine whose result is inside it, the
//! nearest to it first — each in alias form, numbered from 1, with its step
//! count and whether a step needs a model — and `--pick <n>` chooses one to
//! `--save` or `--run`. On a terminal, a target or machine not given is chosen
//! from a list, and so is what to do with the machine.

use std::path::PathBuf;
use std::sync::Arc;

use super::choose::{choose, Choice, Question};
use super::ctx::Ctx;
use super::error::{CliError, CliResult, Exit};
use super::inputs;
use super::live::ProgressChoice;
use super::names;
use super::inspect::Inspection;
use super::term::emit_record;
use capdag::fabric::registry::FabricRegistry;
use capdag::planner as p;
use capdag::{CapUrn, MediaUrn};

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
    // Files of one type are one sequence; each type is its own source. The
    // cartridges started to tell what the files are stop when they are told:
    // a plan run with `--run` starts its own.
    let inspection = Inspection::new(ctx, super::run::inspection_runtime(ctx, &registry)?, registry.clone());
    let mut groups: Vec<(MediaUrn, usize)> = Vec::new();
    for file in &files {
        let media = inputs::detect(file, &inspection).await?;
        if ctx.term.json {
            emit_record(&serde_json::json!({
                "type": "input", "file": file.display().to_string(),
                "alias": names::media(&registry, &media), "urn": media.to_string(),
            }));
        }
        match groups.iter_mut().find(|(m, _)| m.is_equivalent(&media)) {
            Some((_, count)) => *count += 1,
            None => groups.push((media, 1)),
        }
    }
    drop(inspection);
    let sources: Vec<p::SourceSpec> = groups
        .iter()
        .map(|(media, count)| if *count > 1 { p::SourceSpec::sequence(media.clone()) } else { p::SourceSpec::single(media.clone()) })
        .collect();
    let what = groups
        .iter()
        .map(|(media, count)| format!("{count} {}", names::media(&registry, media)))
        .collect::<Vec<_>>()
        .join(" and ");
    let fab = cap_graph(ctx, &registry).await?;
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
    // A `--to` is searched as it is given: every route whose output is inside
    // it reaches it, listed nearest to it first.
    let targets: Vec<MediaUrn> = if !args.to.is_empty() {
        let mut out = Vec::new();
        for token in &args.to {
            out.push(target_media(&registry, token).await?);
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
        // Strands were found and none could be written: they are named, with
        // why, rather than reported as no plan.
        for excluded in &outcome.excluded {
            say_excluded(ctx, excluded);
        }
        let mut error = CliError::failed(format!("no machine makes {target_names} of {what}"));
        if !outcome.excluded.is_empty() {
            error = error.context(match outcome.excluded.len() {
                1 => "a way was found and not offered: it is named above".to_string(),
                n => format!("{n} ways were found and not offered: each is named above"),
            });
        }
        return Err(error.hint(format!("see what it can become: capdag plan {}", args.files.join(" "))));
    }
    // Reading the bundled cartridges verifies each of them: once per command,
    // not once per machine.
    let bundled = super::cartridge::bundled_caps(ctx).await?;
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
    // The strands not offered are named under the machines that are.
    let Some(chosen) = chosen else {
        print_machines(ctx, &machines, &target_names, &what, args.all, &args.files);
        for excluded in &outcome.excluded {
            say_excluded(ctx, excluded);
        }
        return Ok(Exit::Ok);
    };
    for excluded in &outcome.excluded {
        say_excluded(ctx, excluded);
    }
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

/// The graph plans are found in: every cap of the fabric, its bookends marked
/// by their media definitions (`LiveCapFab::from_fabric`).
async fn cap_graph(ctx: &Ctx, registry: &FabricRegistry) -> CliResult<p::LiveCapFab> {
    p::LiveCapFab::from_fabric(registry).await.map_err(|e| ctx.caps_unreadable(e))
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
            .resolve_alias_typed(token, Some(capdag::fabric::alias::AliasTargetKind::Media))
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

/// A strand the planner found and no machine could hold: not offered, said
/// so by its steps' titles, and why. `-v` adds the strand's URNs and the
/// resolver's own words.
fn say_excluded(ctx: &Ctx, excluded: &p::ExcludedCandidate) {
    if ctx.term.json {
        let titled = |media: &p::TitledMedia| match &media.title {
            p::MediaTitle::Defined(title) => serde_json::json!({ "urn": media.urn, "title": title }),
            p::MediaTitle::Unavailable { error } => {
                serde_json::json!({ "urn": media.urn, "title_unavailable": error })
            }
        };
        emit_record(&serde_json::json!({
            "type": "excluded",
            "strand": excluded.strand,
            "steps": excluded.steps.iter().map(|step| serde_json::json!({
                "cap_urn": step.cap_urn, "title": step.title,
            })).collect::<Vec<_>>(),
            "refusal": excluded.refusal.as_ref().map(|refusal| serde_json::json!({
                "cap_urn": refusal.cap_urn,
                "cap_title": refusal.cap_title,
                "input": refusal.input.as_ref().map(|input| serde_json::json!({
                    "name": input.name,
                    "accepts": titled(&input.accepts),
                    "differences": match &input.differences {
                        p::DimDifferences::Said { dims, unworded } => serde_json::json!({
                            "dims": dims.iter().map(|d| serde_json::json!({
                                "dim": d.dim,
                                "dim_title": d.dim_title,
                                "takes": dim_state_json(&d.takes),
                                "given": dim_state_json(&d.given),
                            })).collect::<Vec<_>>(),
                            "unworded": unworded,
                        }),
                        p::DimDifferences::Unavailable { error } => serde_json::json!({ "unavailable": error }),
                    },
                })),
                "given": titled(&refusal.given),
            })),
            "reason": excluded.reason,
        }));
        return;
    }
    let steps: Vec<&str> = excluded.steps.iter().map(|step| step.title.as_str()).collect();
    let why = match &excluded.refusal {
        Some(refusal) => refusal_words(refusal),
        None => excluded.reason.clone(),
    };
    eprintln!("not offered: {}: {why}", steps.join(" > "));
    if ctx.term.verbosity >= 1 {
        eprintln!("  {}", excluded.strand);
        if excluded.refusal.is_some() {
            eprintln!("  {}", excluded.reason);
        }
    }
}

/// Where a step refused what it was given, said by titles: what its input
/// takes against what it is given, each with the published dims they differ
/// in. A difference the dims cannot say leaves only the URNs to say it.
fn refusal_words(refusal: &p::StrandRefusal) -> String {
    let cap = &refusal.cap_title;
    let Some(input) = &refusal.input else {
        return format!("{cap} takes no input; it is given {}", media_named(&refusal.given));
    };
    match &input.differences {
        p::DimDifferences::Said { unworded, .. } if !unworded.is_empty() => format!(
            "{cap}'s {} takes {}; it is given {}",
            input.name, input.accepts.urn, refusal.given.urn
        ),
        p::DimDifferences::Said { dims, .. } => {
            let said = |side: fn(&p::DimDifference) -> &p::DimState| {
                if dims.is_empty() {
                    return String::new();
                }
                let each: Vec<String> =
                    dims.iter().map(|d| format!("{}: {}", d.dim_title, dim_state_words(side(d)))).collect();
                format!(" ({})", each.join(", "))
            };
            format!(
                "{cap}'s {} takes {}{}; it is given {}{}",
                input.name,
                media_named(&input.accepts),
                said(|d| &d.takes),
                media_named(&refusal.given),
                said(|d| &d.given)
            )
        }
        p::DimDifferences::Unavailable { error } => format!(
            "{cap}'s {} takes {}; it is given {} (the dims could not be had: {error})",
            input.name,
            media_named(&input.accepts),
            media_named(&refusal.given)
        ),
    }
}

/// What a media says of a dim, in the planner's words; a value by its
/// published title.
fn dim_state_words(state: &p::DimState) -> &str {
    match state {
        p::DimState::Value { value_title, .. } => value_title,
        p::DimState::Any => "any",
        p::DimState::None => "none",
        p::DimState::Open => "not said",
    }
}

fn dim_state_json(state: &p::DimState) -> serde_json::Value {
    match state {
        p::DimState::Value { value, value_title } => {
            serde_json::json!({ "state": "value", "value": value, "value_title": value_title })
        }
        p::DimState::Any => serde_json::json!({ "state": "any" }),
        p::DimState::None => serde_json::json!({ "state": "none" }),
        p::DimState::Open => serde_json::json!({ "state": "open" }),
    }
}

/// A media an excluded way names, by its title; one the registry gives no
/// title is named by its URN, saying why it has no title.
fn media_named(media: &p::TitledMedia) -> String {
    match &media.title {
        p::MediaTitle::Defined(title) => title.clone(),
        p::MediaTitle::Unavailable { error } => format!("{} (no title: {error})", media.urn),
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
    let machine = capdag::machine::parse_machine_async(&candidate.notation, registry)
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
            // A cap that takes a model spec runs a model, default or not.
            model |= cap
                .model_spec_arg()
                .map_err(|e| CliError::failed("a planned machine could not be described").context(e.to_string()))?
                .is_some();
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

#[cfg(test)]
mod tests {
    use super::*;

    /// `capdag plan`'s arguments, as the command line gives them.
    #[derive(clap::Parser)]
    struct Command {
        #[command(flatten)]
        args: Args,
    }

    // TEST8211: plan --to lists the machines of every route whose result is
    // inside the --to, nearest to it first and then with fewer steps: a YAML
    // record --to media:ext=json, which several JSON file types fit, is
    // searched as it is given; the two-step machine giving exactly
    // media:ext=json comes before the one-step machines giving JSON of any
    // shape and then a JSON record
    #[tokio::test]
    async fn test8211_a_to_lists_every_route_inside_it_nearest_first() {
        use capdag::machine::test_fixtures::{build_cap, cap, media, registry_with};
        use clap::Parser;

        const TO_ANY_JSON: &str =
            r#"cap:in="media:enc=utf8;fmt=yaml;record";out="media:enc=utf8;ext=json;fmt=json;?list;?record";to-json"#;
        const TO_JSON_RECORD: &str =
            r#"cap:in="media:enc=utf8;fmt=yaml;record";out="media:enc=utf8;ext=json;fmt=json;record";to-json"#;
        const READ: &str = r#"cap:in="media:enc=utf8;fmt=yaml;record";out="media:enc=utf8;fmt=json;record";read-yaml"#;
        const WRITE: &str = r#"cap:in="media:enc=utf8;fmt=json;record";out="media:ext=json";write-json"#;
        let caps: Vec<_> = [TO_ANY_JSON, TO_JSON_RECORD, READ, WRITE]
            .into_iter()
            .map(|text| {
                let parsed = cap(text);
                build_cap(text, text, &[parsed.in_spec()], parsed.out_spec())
            })
            .collect();
        let source = media("media:enc=utf8;ext=yaml;fmt=yaml;record");
        let bookends = [
            source.clone(),
            media("media:enc=utf8;ext=json;fmt=json;?list;?record"),
            media("media:enc=utf8;ext=json;fmt=json;record"),
            media("media:ext=json"),
        ]
        .into_iter()
        .collect();
        let mut fab = p::LiveCapFab::new();
        fab.sync_from_caps(&caps, &bookends);
        let registry = registry_with(caps);
        let args = Command::try_parse_from(["plan", "rec.yaml", "--to", "media:ext=json"]).unwrap().args;
        let target = target_media(&registry, &args.to[0]).await.unwrap();
        assert!(target.is_equivalent(&media("media:ext=json")), "the --to is the media it gives");

        let outcome = fab
            .plan(&request(&args, vec![p::SourceSpec::single(source)], vec![target]).unwrap(), &registry)
            .unwrap();
        assert!(outcome.excluded.is_empty(), "every route found is offered: {:?}", outcome.excluded);
        let last_caps: Vec<&str> = outcome
            .candidates
            .iter()
            .map(|candidate| {
                *[TO_ANY_JSON, TO_JSON_RECORD, WRITE]
                    .iter()
                    .find(|last| candidate.notation.contains(&cap(last).to_string()))
                    .unwrap_or_else(|| panic!("each machine ends on a JSON file: {}", candidate.notation))
            })
            .collect();
        assert_eq!(last_caps, [WRITE, TO_ANY_JSON, TO_JSON_RECORD], "the nearest first, then fewer steps");
    }

    fn titled(urn: &str, title: &str) -> p::TitledMedia {
        p::TitledMedia { urn: urn.to_string(), title: p::MediaTitle::Defined(title.to_string()) }
    }

    // TEST13151: a refusal is said by titles and the planner's words for what
    // each side says of the dims they differ in: a step taking a JSON record
    // that is no list, given one that does not say, reads "List Marker: none"
    // against "List Marker: not said". A difference no dim can say leaves the
    // URNs to say it, and a step with no input says that.
    #[test]
    fn test13151_a_refusal_reads_by_titles_and_the_dims_it_differs_in() {
        let given = titled("media:enc=utf8;fmt=json;record", "JSON Record");
        let input = |differences| p::RefusingInput {
            name: "input".to_string(),
            accepts: titled("media:enc=utf8;fmt=json;!list;record", "JSON Record"),
            differences,
        };
        let refusal = |input| p::StrandRefusal {
            cap_urn: "cap:to-yaml".to_string(),
            cap_title: "JSON to YAML".to_string(),
            input,
            given: given.clone(),
        };
        let list = p::DimDifference {
            dim: "list-marker".to_string(),
            dim_title: "List Marker".to_string(),
            takes: p::DimState::None,
            given: p::DimState::Open,
        };
        assert_eq!(
            refusal_words(&refusal(Some(input(p::DimDifferences::Said { dims: vec![list], unworded: Vec::new() })))),
            "JSON to YAML's input takes JSON Record (List Marker: none); it is given JSON Record (List Marker: not said)"
        );
        assert_eq!(
            refusal_words(&refusal(Some(input(p::DimDifferences::Said {
                dims: Vec::new(),
                unworded: vec!["list".to_string()],
            })))),
            "JSON to YAML's input takes media:enc=utf8;fmt=json;!list;record; it is given media:enc=utf8;fmt=json;record"
        );
        assert_eq!(
            refusal_words(&refusal(None)),
            "JSON to YAML takes no input; it is given JSON Record"
        );
    }
}
