//! `capdag plan INPUT… [--from TYPE] [--to TYPE…] [--save FILE [--force]]`:
//! what inputs can become, the machines that make it, and saving one.
//!
//! Without `--to` it says what the inputs can become. With it, it lists the
//! machines that make it — every machine whose result is inside it, the
//! nearest to it first — each in alias form, with its step count and whether
//! a step runs a model. With `--save` it writes the one it settles on: asked
//! at a terminal when several make it, named in a script. A plan never runs
//! anything; a machine is run with `capdag run`. What each line comes to is
//! the model's decision (`planVerdict`).

use std::path::PathBuf;

use super::ask::{self, Answered, Choice, Question};
use super::ctx::Ctx;
use super::error::{CliError, CliResult, Exit};
use super::formal::{self as f, index, list, nat};
use super::inspect::Inspection;
use super::line::{self, Extras, PlanFacts, PlanningFlags, RunState, Tables, World};
use super::names;
use super::term::{emit_record, outln};
use capdag::fabric::registry::FabricRegistry;
use capdag::planner as p;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// What to plan from: files, directories or globs (their files, sorted), live
    /// sources, or - (stdin)
    #[arg(value_name = "INPUT")]
    pub inputs: Vec<String>,
    /// What to make: a media alias (txt, png), an extension or a media URN; several name
    /// what one machine makes together (repeatable)
    #[arg(long, value_name = "TYPE")]
    pub to: Vec<String>,
    /// What - holds
    #[arg(long, value_name = "TYPE")]
    pub from: Option<String>,
    /// Write the machine that makes what --to names to this file
    #[arg(long, value_name = "FILE")]
    pub save: Option<PathBuf>,
    /// Replace the --save file when it is there
    #[arg(long)]
    pub force: bool,
    #[command(flatten)]
    pub planning: PlanningFlags,
}

impl Args {
    /// Whether the line reads stdin as data.
    pub fn reads_stdin(&self) -> bool {
        self.inputs.iter().any(|a| a == "-")
    }
}

/// A `plan` line that gives nothing: the stand-in for a line's own facts.
pub fn no_plan_line() -> f::PlanLine {
    f::PlanLine { inputs: list(Vec::new()), has_from: false, targets: nat(0), save: false, force: false }
}

/// A plan's inputs, read: `-`, live sources, and the files each path names.
fn read_inputs(tables: &mut Tables, given: &[String]) -> CliResult<Vec<f::Positional>> {
    let mut inputs = Vec::new();
    for token in given {
        if token == "-" {
            inputs.push(f::Positional::Stdin);
        } else if line::is_live_urn(token) {
            inputs.push(f::Positional::Live { r: nat(tables.add_live(token.clone())) });
        } else {
            for p in line::files_of(tables, token)? {
                inputs.push(f::Positional::Path { p: nat(p) });
            }
        }
    }
    Ok(inputs)
}

/// The state a plan's verdict is said with: a line of its inputs and no
/// machine.
fn said_state(inputs: &[f::Positional]) -> RunState {
    RunState {
        line: f::RunLine { inputs: list(inputs.to_vec()), ..f::no_run_line() },
        texts: Vec::new(),
        arg_values: Vec::new(),
        unknown_inputs: Vec::new(),
        unknown_options: Vec::new(),
    }
}

pub async fn plan(ctx: &Ctx, args: Args) -> CliResult<Exit> {
    let registry = ctx.registry().await?;
    let mut tables = Tables::default();
    let inputs = read_inputs(&mut tables, &args.inputs)?;
    let extras = Extras { to: args.to.clone(), from: args.from.clone(), selector: None, output: None };
    let inspection = Inspection::new(ctx, super::run::inspection_runtime(ctx, &registry)?, registry.clone());
    let mut world = World::new(ctx, registry.clone(), inspection, tables, extras, args.planning.clone());
    world.learn_line(&[]).await?;
    let save_there = args.save.as_ref().is_some_and(|path| path.exists());
    let mut chosen: Option<usize> = None;
    loop {
        let line = f::PlanLine {
            inputs: list(inputs.clone()),
            has_from: world.extras.from.is_some(),
            targets: nat(world.extras.to.len()),
            save: args.save.is_some(),
            force: args.force,
        };
        let verdict = world
            .settle(&inputs, |w| ctx.decide(f::Command::Plan { l: line.clone(), w, save_there }, false))
            .await?;
        match verdict {
            f::CliVerdict::Is { v: f::Verdict::Answers } => break,
            f::CliVerdict::Is { v: f::Verdict::Asks { s: f::Stage::StdinType } } => {
                let typed = loop {
                    match ask::value(ctx, &ask::ValueQuestion::stdin_type()).await? {
                        Answered::Declined => return Err(ask::declined()),
                        Answered::Given(text) => {
                            if line::media_type(&registry, &text).await?.is_some() {
                                break text;
                            }
                            ask::say_retry(ctx, &format!("'{text}' names no media type: see capdag caps"));
                        }
                    }
                };
                world.extras.from = Some(typed);
                world.learn_types().await?;
            }
            // Which machine to save: once it is chosen, saving it is what is
            // left of the line.
            f::CliVerdict::Is { v: f::Verdict::Asks { s: f::Stage::Machine } } => {
                let plan = world.plan(&inputs).cloned().expect("the machine is asked once it is planned");
                let choices = machine_choices(&plan);
                match ask::pick(ctx, &Question { what: "the machine to save".into(), flag: String::new() }, &choices).await? {
                    Answered::Given(k) => {
                        chosen = Some(k);
                        break;
                    }
                    Answered::Declined => return Err(ask::declined()),
                }
            }
            f::CliVerdict::Is { v: f::Verdict::Names { s: f::Stage::Machine } } => {
                let plan = world.plan(&inputs).cloned().expect("the machines are named once they are planned");
                let error = ask::named(
                    ctx,
                    &Question { what: "the machine to save".into(), flag: String::new() },
                    &machine_choices(&plan),
                );
                return Err(super::say::with_exit(
                    error.hint(format!(
                        "narrow what --to names or the planning flags until one machine makes it, or write one to {}",
                        args.save.as_ref().map(|p| p.display().to_string()).unwrap_or_default()
                    )),
                    &verdict,
                ));
            }
            f::CliVerdict::Refused { r: f::CliRefusal::Line { r: f::Refusal::SaveExists } } => {
                let path = args.save.as_ref().expect("a --save file is there").display().to_string();
                return Err(super::say::with_exit(
                    CliError::usage(format!("{path}: {}", f::refusal::says(f::Refusal::SaveExists))),
                    &verdict,
                ));
            }
            other => {
                return Err(super::say::line_verdict_error(ctx, &mut world, &said_state(&inputs), &inputs, &other).await);
            }
        }
    }
    say_inputs(ctx, &world, &inputs);
    let plan = world.plan(&inputs).cloned();
    if world.extras.to.is_empty() {
        let plan = plan.expect("what the inputs can become is answered once they are planned");
        print_targets(ctx, &registry, &plan, &args.inputs);
        return Ok(Exit::Ok);
    }
    let plan = plan.expect("the machines are listed once they are planned");
    for excluded in &plan.excluded {
        say_excluded(ctx, excluded);
    }
    let Some(save) = &args.save else {
        let mut made = Vec::with_capacity(world.extras.to.len());
        for token in &world.extras.to {
            let media = line::media_type(&registry, token).await?.expect("the model refuses a --to that is no type");
            made.push(names::media(&registry, &media));
        }
        print_machines(ctx, &plan, &made.join(", "), &world.extras.to, &args.inputs);
        return Ok(Exit::Ok);
    };
    let k = chosen.unwrap_or(0);
    let machine = &plan.described[k];
    std::fs::write(save, format!("{}\n", machine.notation)).map_err(|e| {
        CliError::failed(format!("the machine could not be saved to {}", save.display())).context(e.to_string())
    })?;
    if ctx.term.json {
        emit_record(&serde_json::json!({ "type": "saved", "file": save.display().to_string(), "notation": machine.notation }));
    } else {
        eprintln!("Saved the machine to {}", save.display());
        eprintln!(
            "Run it: capdag run {}{}",
            super::shell_word(&save.display().to_string()),
            args.inputs.iter().map(|i| format!(" {}", super::shell_word(i))).collect::<String>()
        );
    }
    Ok(Exit::Ok)
}

/// Each planned machine as a choice: written as it is run.
fn machine_choices(plan: &PlanFacts) -> Vec<Choice> {
    plan.described
        .iter()
        .map(|d| Choice {
            label: d.notation.replace('\n', " "),
            detail: d.summary(),
            urn: Some(d.canonical.clone()),
            value: d.notation.replace('\n', ""),
        })
        .collect()
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
    eprintln!("\nThe machines that make one: capdag plan{given} --to {to}");
    eprintln!("Make one:                   capdag run{given} --to {to}");
}

fn print_machines(ctx: &Ctx, plan: &PlanFacts, target: &str, to: &[String], inputs: &[String]) {
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
    let to: String = to.iter().map(|t| format!(" --to {}", super::shell_word(t))).collect();
    eprintln!("\nRun one:  capdag run '<machine>'{given}");
    eprintln!("Save one: capdag plan{given}{to} --save <file>");
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
