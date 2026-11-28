//! `capdag run … --dry-run`: everything the run would decide, decided, and
//! nothing run.
//!
//! A dry run settles its line as the run does — asking at a terminal what the
//! run would ask — and installs nothing and runs nothing. It then says what
//! the run would do, as the run decides it: the machine it would run, alone on
//! stdout so it can be kept in a file, with the line that runs it on stderr;
//! or, where nobody can be asked, what is still open — what the inputs can
//! become, the machines that make what `--to` names, or the input or option
//! a value is wanted for — and exits 0. What a line comes to is the model's
//! decision (`dryRun`).

use super::ask::{self, Choice, Question};
use super::ctx::Ctx;
use super::error::{CliError, CliResult, Exit};
use super::formal::{self as f, index};
use super::line::{self, MachineFacts, PlanFacts, RunState, World};
use super::names;
use super::run;
use super::term::{emit_record, outln};
use capdag::fabric::registry::FabricRegistry;
use capdag::planner as p;

/// What a dry run answers once its line is settled as far as it can be here:
/// what the run would do with it, decided as the run decides it.
pub async fn report(
    ctx: &Ctx,
    world: &mut World<'_>,
    state: &RunState,
    positionals: &[f::Positional],
    planning_flags: bool,
    args: &run::Args,
) -> CliResult<Exit> {
    say_inputs(ctx, world, positionals);
    let line = state.line.clone();
    let cli_world = ctx.cli_world(false);
    let globals = ctx.globals();
    let verdict = world
        .settle(positionals, |w| {
            f::cli(
                cli_world.clone(),
                f::Invocation::Command {
                    g: globals.clone(),
                    c: f::Command::Run { l: line.clone(), planning_flags, mode: f::RunMode::Run, w },
                },
            )
        })
        .await?;
    // The inputs as they were typed, for the lines it says to type next: a
    // line that lists what to make or the machines has no machine of its own.
    let typed: Vec<String> = args.args.iter().chain(args.after.iter()).cloned().collect();
    match &verdict {
        f::CliVerdict::Is { v: f::Verdict::Runs } => settled(ctx, world, state, positionals, args).await,
        f::CliVerdict::Is { v: f::Verdict::Asks { s: f::Stage::Provider } }
        | f::CliVerdict::Is { v: f::Verdict::Names { s: f::Stage::Provider } } => {
            let exit = settled(ctx, world, state, positionals, args).await?;
            say_provider(ctx, world, state, positionals).await?;
            Ok(exit)
        }
        f::CliVerdict::Is { v: f::Verdict::Names { s: f::Stage::Target } } => {
            let plan = world.plan(positionals).cloned().expect("what to make is named once the inputs are planned");
            print_targets(ctx, &world.registry, &plan, &typed);
            Ok(Exit::Ok)
        }
        f::CliVerdict::Is { v: f::Verdict::Names { s: f::Stage::Machine } } => {
            let plan = world.plan(positionals).cloned().expect("the machines are named once they are planned");
            for excluded in &plan.excluded {
                say_excluded(ctx, excluded);
            }
            let mut made = Vec::with_capacity(world.extras.to.len());
            for token in &world.extras.to {
                let media = line::media_type(&world.registry, token).await?.expect("the model refuses a --to that is no type");
                made.push(names::media(&world.registry, &media));
            }
            print_machines(ctx, &plan, &made.join(", "), &typed);
            Ok(Exit::Ok)
        }
        f::CliVerdict::Is { v: f::Verdict::Names { .. } } => open(ctx, world, state, positionals, &verdict).await,
        other => Err(CliError::failed(format!("a dry run came to answers, and the run it stands for to {other:?}"))
            .hint("report it: a dry run answers where the run runs, names a choice or asks for a cartridge")),
    }
}

/// The machine the run would run: alone on stdout in alias form (under
/// `--json` a `machine` record), and the line that runs it, asking nothing,
/// on stderr (a `rerun` record).
async fn settled(
    ctx: &Ctx,
    world: &mut World<'_>,
    state: &RunState,
    positionals: &[f::Positional],
    args: &run::Args,
) -> CliResult<Exit> {
    let line = state.line.clone();
    let rerun = world.settle(positionals, |w| f::rerun(w, line.clone())).await?;
    let rerun_state = run::carried(world, state, rerun)?;
    let m = index(&rerun_state.line.machine.as_ref().expect("the line that runs writes its machine").base);
    say_machine(ctx, world, m)?;
    run::say_rerun(ctx, world, &rerun_state, args, "Run it with")?;
    Ok(Exit::Ok)
}

/// A machine the line settled on, on stdout: in alias form, or under
/// `--json` the record `--explain` gives.
fn say_machine(ctx: &Ctx, world: &World<'_>, m: usize) -> CliResult<()> {
    let resolved = world
        .machine(m)
        .and_then(MachineFacts::resolved)
        .ok_or_else(|| CliError::failed("the machine the line settled on is not known").hint("report it"))?;
    let entry = &world.tables.machines[m];
    if ctx.term.json {
        return super::explain::show(ctx, &world.registry, entry, resolved);
    }
    let aliased = names::machine(&world.registry, resolved.resolution.machine())
        .map_err(|e| CliError::failed(format!("{} could not be written in alias form", entry.shown())).context(e))?;
    outln!("{}", aliased.trim_end());
    Ok(())
}

/// Which cartridge the run would ask to install, said: a dry run installs
/// nothing.
async fn say_provider(ctx: &Ctx, world: &mut World<'_>, state: &RunState, positionals: &[f::Positional]) -> CliResult<()> {
    let line = state.line.clone();
    let written = world.settle(positionals, |w| f::run_line::written(w, line.clone())).await?;
    let m = index(&written.machine.as_ref().expect("the line that runs writes its machine").base);
    world.resolve_machine(m).await?;
    let (cap, providers) = world
        .needs(m)
        .and_then(|needs| needs.missing.iter().find(|(_, providers)| providers.len() > 1).cloned())
        .ok_or_else(|| CliError::failed("a cartridge was to be chosen and none was known").hint("report it"))?;
    let cap_name = names::cap_urn(&world.registry, &cap.to_string());
    if ctx.term.json {
        let choices: Vec<Choice> = providers
            .iter()
            .map(|id| Choice { label: id.clone(), detail: format!("provides {cap_name}"), urn: None, value: id.clone() })
            .collect();
        // The records only: what the run would ask is no error of the dry run.
        let _ = ask::named(
            ctx,
            &Question { what: format!("the cartridge to install for {cap_name}"), flag: "capdag cartridge install ".into() },
            &choices,
        );
    } else if ctx.term.verbosity >= 0 {
        eprintln!("The run asks which cartridge to install for {cap_name}: {}", providers.join(", "));
    }
    Ok(())
}

/// What is still open where nobody can be asked: the machine, when the line
/// has settled on one, as a settled one is said; then what it wants a value
/// or a choice for, and the flag that gives it (`choice` records and an
/// `open` record under `--json`).
async fn open(
    ctx: &Ctx,
    world: &mut World<'_>,
    state: &RunState,
    positionals: &[f::Positional],
    verdict: &f::CliVerdict,
) -> CliResult<Exit> {
    let line = state.line.clone();
    let written = world.settle(positionals, |w| f::run_line::written(w, line.clone())).await?;
    if let Some(machine) = &written.machine {
        let m = index(&machine.base);
        world.resolve_machine(m).await?;
        if world.machine(m).and_then(MachineFacts::resolved).is_some() {
            say_machine(ctx, world, m)?;
        }
    }
    let wanted = super::say::line_verdict_error(ctx, world, state, positionals, verdict).await;
    if ctx.term.json {
        emit_record(&serde_json::json!({
            "type": "open", "what": wanted.problem, "context": wanted.context, "hint": wanted.hint,
        }));
    } else if ctx.term.verbosity >= 0 {
        let style = &ctx.term.style;
        let mut said = format!("{} {}\n", style.hint("open:"), wanted.problem);
        for line in &wanted.context {
            said.push_str(&format!("  {line}\n"));
        }
        if let Some(hint) = &wanted.hint {
            said.push_str(&format!("{} {hint}\n", style.hint("hint:")));
        }
        eprint!("{said}");
    }
    Ok(Exit::Ok)
}

/// Under `--json`, what each file planned from was detected as.
fn say_inputs(ctx: &Ctx, world: &World<'_>, inputs: &[f::Positional]) {
    if !ctx.term.json {
        return;
    }
    for x in inputs {
        if let f::Positional::Path { p } = x {
            if let Some(media) = world.media_of(index(p)) {
                emit_record(&serde_json::json!({
                    "type": "input", "file": world.tables.files[index(p)].path.display().to_string(),
                    "alias": names::media(&world.registry, media), "urn": media.to_string(),
                }));
            }
        }
    }
}

fn print_targets(ctx: &Ctx, registry: &FabricRegistry, plan: &PlanFacts, inputs: &[String]) {
    let choices: Vec<Choice> = plan.targets.iter().map(|t| target_choice(registry, t)).collect();
    if ctx.term.json {
        for choice in &choices {
            emit_record(&serde_json::json!({
                "type": "target", "alias": choice.label, "detail": choice.detail, "urn": choice.urn,
            }));
        }
        return;
    }
    outln!("{} can become:", plan.what);
    let width = choices.iter().map(|c| c.label.chars().count()).max().unwrap_or(0).min(28);
    for choice in &choices {
        let urn = if ctx.term.verbosity >= 1 { format!("  {}", choice.urn.as_deref().unwrap_or("")) } else { String::new() };
        outln!("  {:<width$}  {}{urn}", choice.label, choice.detail);
    }
    let given: String = inputs.iter().map(|i| format!(" {}", super::shell_word(i))).collect();
    let to = super::shell_word(&choices[0].value);
    eprintln!("\nThe machines that make one: capdag run{given} --to {to} --dry-run");
    eprintln!("Make one:                   capdag run{given} --to {to}");
}

fn print_machines(ctx: &Ctx, plan: &PlanFacts, target: &str, inputs: &[String]) {
    if ctx.term.json {
        for (k, machine) in plan.described.iter().enumerate() {
            emit_record(&serde_json::json!({
                "type": "machine",
                "index": k + 1,
                "notation": machine.notation,
                "canonical": machine.canonical,
                "steps": machine.steps,
                "model": machine.model,
                "downloads": machine.downloads,
            }));
        }
        return;
    }
    let n = plan.described.len();
    outln!("{n} machine{} make{} {target} of {}:", if n == 1 { "" } else { "s" }, if n == 1 { "s" } else { "" }, plan.what);
    for (k, machine) in plan.described.iter().enumerate() {
        outln!("\n {:>2}. {}", k + 1, ctx.term.style.dim(&machine.summary()));
        for line in machine.notation.lines() {
            outln!("     {line}");
        }
        if ctx.term.verbosity >= 1 {
            outln!("     {}", ctx.term.style.dim(&machine.canonical));
        }
    }
    let given: String = inputs.iter().map(|i| format!(" {}", super::shell_word(i))).collect();
    eprintln!("\nRun one:  capdag run '<machine>'{given}");
    eprintln!("Keep one: echo '<machine>' > <name>.machine");
}

/// A strand the planner found and no machine could hold: not offered, said
/// so by its steps' titles, and why. `-v` adds the strand's URNs and the
/// resolver's own words.
pub fn say_excluded(ctx: &Ctx, excluded: &p::ExcludedCandidate) {
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

pub fn target_choice(registry: &FabricRegistry, target: &p::ConvergentTargetInfo) -> Choice {
    let alias = names::media(registry, &target.media_def);
    let title = registry
        .get_cached_media_def(&target.media_def.to_string())
        .map(|d| d.title)
        .unwrap_or_else(|_| target.display_name.clone());
    let how = if target.apex.is_some() { "combine all" } else { "each" };
    // What --to is given to choose it: its alias, or its URN where it has none.
    let urn = target.media_def.to_string();
    let value = registry.display_alias_for_urn(&urn).unwrap_or_else(|| urn.clone());
    Choice {
        label: alias,
        detail: format!("{title} · {how} · {} step{}", target.min_total_steps, if target.min_total_steps == 1 { "" } else { "s" }),
        urn: Some(urn),
        value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::line::PlanningFlags;

    // TEST8211: plan --to lists the machines of every route whose result is
    // inside the --to, nearest to it first and then with fewer steps: a YAML
    // record --to media:ext=json, which several JSON file types fit, is
    // searched as it is given; the two-step machine giving exactly
    // media:ext=json comes before the one-step machines giving JSON of any
    // shape and then a JSON record
    #[tokio::test]
    async fn test8211_a_to_lists_every_route_inside_it_nearest_first() {
        use capdag::machine::test_fixtures::{build_cap, cap, media, registry_with};

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
        let target = line::media_type(&registry, "media:ext=json").await.unwrap().expect("a URN names a type");
        assert!(target.is_equivalent(&media("media:ext=json")), "the --to is the media it gives");

        let outcome = fab
            .plan(&PlanningFlags::default().request(vec![p::SourceSpec::single(source)], vec![target]).unwrap(), &registry)
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
