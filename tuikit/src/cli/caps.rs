//! `capdag caps [query] [--in <type>] [--out <type>] [--pick]`: the
//! capabilities there are, found by what they are called and what they take
//! and give.

use super::ask::{self, Answered, Choice, Question};
use super::ctx::Ctx;
use super::error::{CliError, CliResult, Exit};
use super::formal as f;
use super::line;
use super::names;
use super::term::{emit_record, outln};
use capdag::Cap;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// Words a capability's alias, title or media must all contain
    #[arg(value_name = "QUERY")]
    pub query: Vec<String>,
    /// Only capabilities that take this type: a media alias, an extension or a URN
    #[arg(long = "in", value_name = "TYPE")]
    pub input: Option<String>,
    /// Only capabilities that give this type
    #[arg(long = "out", value_name = "TYPE")]
    pub output: Option<String>,
    /// Choose one; the machine of its one step is printed, for `capdag run "$(…)"`
    #[arg(long)]
    pub pick: bool,
}

/// One capability, as it is listed.
pub struct Listed {
    pub alias: String,
    pub title: String,
    pub takes: String,
    pub gives: String,
    pub urn: String,
}

pub async fn caps(ctx: &Ctx, args: Args) -> CliResult<Exit> {
    let registry = ctx.registry().await?;
    let mut all = ctx.fabric_caps(&registry).await?;
    all.retain(|cap| !cap.aliases.is_empty() && cap.primary_alias() != "identity");
    let takes = match &args.input {
        Some(t) => Some(line::media_type(&registry, t).await?),
        None => None,
    };
    let gives = match &args.output {
        Some(t) => Some(line::media_type(&registry, t).await?),
        None => None,
    };
    let types_known = !matches!(takes, Some(None)) && !matches!(gives, Some(None));
    let mut listed: Vec<(Listed, &Cap)> = Vec::new();
    if types_known {
        let (takes, gives) = (takes.flatten(), gives.flatten());
        for cap in &all {
            let input = cap.urn.in_media_urn().ok();
            let output = cap.urn.out_media_urn().ok();
            if let (Some(wanted), Some(input)) = (&takes, &input) {
                if !wanted.conforms_to(input) {
                    continue;
                }
            }
            if let (Some(wanted), Some(output)) = (&gives, &output) {
                if !output.conforms_to(wanted) {
                    continue;
                }
            }
            let entry = listed_cap(&registry, cap, input.as_ref(), output.as_ref());
            if matches_query(&args.query, cap, &entry) {
                listed.push((entry, cap));
            }
        }
    }
    listed.sort_by(|a, b| a.0.alias.cmp(&b.0.alias));
    let found = match listed.len() {
        0 => f::Choices::None,
        1 => f::Choices::One,
        _ => f::Choices::Many,
    };
    // Each capability as the machine of its one step: what --pick prints.
    let mut choices: Vec<Choice> = Vec::with_capacity(listed.len());
    for (l, cap) in &listed {
        choices.push(Choice {
            label: l.alias.clone(),
            detail: format!("{} · {} → {}", l.title, l.takes, l.gives),
            urn: Some(l.urn.clone()),
            value: names::one_step_machine(&registry, cap)
                .map_err(|e| CliError::failed(format!("the machine of {}'s one step could not be written", l.alias)).context(e))?,
        });
    }
    let question = Question { what: "a capability".into(), flag: String::new() };
    let verdict = ctx.decide(f::Command::Caps { pick: args.pick, types_known, found }, false);
    let picked = match &verdict {
        f::CliVerdict::Is { v: f::Verdict::Answers } if args.pick => Some(0),
        f::CliVerdict::Is { v: f::Verdict::Answers } => None,
        f::CliVerdict::Is { v: f::Verdict::Asks { s: f::Stage::Capability } } => match ask::pick(ctx, &question, &choices).await? {
            Answered::Given(at) => Some(at),
            Answered::Declined => return Err(ask::declined()),
        },
        f::CliVerdict::Is { v: f::Verdict::Names { .. } } => {
            return Err(super::say::with_exit(
                ask::named(ctx, &question, &choices).hint("add words to the query until one matches, or leave out --pick to list them"),
                &verdict,
            ))
        }
        f::CliVerdict::Is { v: f::Verdict::Nothing { .. } } => {
            let mut error = CliError::failed("no capability matches");
            if !args.query.is_empty() {
                error = error.context(format!("looked for: {}", args.query.join(" ")));
            }
            return Err(super::say::with_exit(error.hint("fewer words find more; `capdag caps` lists every capability"), &verdict));
        }
        f::CliVerdict::Refused { r: f::CliRefusal::NoSuchType } => {
            let given: Vec<String> = args
                .input
                .iter()
                .map(|t| format!("--in {t}"))
                .chain(args.output.iter().map(|t| format!("--out {t}")))
                .collect();
            return Err(super::say::with_exit(
                CliError::usage(f::says(&f::CliRefusal::NoSuchType))
                    .context(format!("given: {}", given.join(", ")))
                    .hint("give a media alias such as txt or png, an extension, or a media URN"),
                &verdict,
            ));
        }
        other => return Err(super::ends(other).expect("a verdict other than answers ends the command")),
    };
    if let Some(at) = picked {
        let (entry, cap) = &listed[at];
        let machine = names::one_step_machine(&registry, cap)
            .map_err(|e| CliError::failed(format!("the machine of {}'s one step could not be written", entry.alias)).context(e))?;
        if ctx.term.json {
            let mut record = record(entry);
            record["machine"] = serde_json::Value::String(machine);
            emit_record(&record);
        } else {
            outln!("{machine}");
        }
        return Ok(Exit::Ok);
    }
    if ctx.term.json {
        for (l, _) in &listed {
            emit_record(&record(l));
        }
        return Ok(Exit::Ok);
    }
    let width = listed.iter().map(|(l, _)| l.alias.chars().count()).max().unwrap_or(0).min(34);
    for (l, _) in &listed {
        let urn = if ctx.term.verbosity >= 1 { format!("\n{:width$}  {}", "", ctx.term.style.dim(&l.urn)) } else { String::new() };
        outln!(
            "{}  {} {}{urn}",
            ctx.term.style.name(&format!("{:<width$}", l.alias)),
            l.title,
            ctx.term.style.dim(&format!("({} → {})", l.takes, l.gives)),
        );
    }
    eprintln!("\n{} capabilit{}. More about one: capdag info <alias>", listed.len(), if listed.len() == 1 { "y" } else { "ies" });
    Ok(Exit::Ok)
}

fn listed_cap(
    registry: &capdag::FabricRegistry,
    cap: &Cap,
    input: Option<&capdag::MediaUrn>,
    output: Option<&capdag::MediaUrn>,
) -> Listed {
    Listed {
        alias: names::cap(registry, cap),
        title: cap.title.clone(),
        takes: input.map(|m| names::media(registry, m)).unwrap_or_else(|| "nothing".into()),
        gives: output.map(|m| names::media(registry, m)).unwrap_or_else(|| "nothing".into()),
        urn: cap.urn.to_string(),
    }
}

/// Every word appears in an alias, the title, or what it takes or gives.
fn matches_query(query: &[String], cap: &Cap, listed: &Listed) -> bool {
    let haystack = format!(
        "{} {} {} {}",
        cap.aliases.join(" "),
        listed.title,
        listed.takes,
        listed.gives
    )
    .to_lowercase();
    query
        .iter()
        .flat_map(|q| q.split_whitespace())
        .all(|word| haystack.contains(&word.to_lowercase()))
}

fn record(l: &Listed) -> serde_json::Value {
    serde_json::json!({
        "type": "cap",
        "alias": l.alias,
        "title": l.title,
        "takes": l.takes,
        "gives": l.gives,
        "urn": l.urn,
    })
}
