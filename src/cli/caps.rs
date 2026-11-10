//! `capdag caps [query] [--in <type>] [--out <type>] [--pick]`: the
//! capabilities there are, found by what they are called and what they take
//! and give.

use super::choose::{choose, Choice, Question};
use super::ctx::Ctx;
use super::error::{CliError, CliResult, Exit};
use super::names;
use super::term::emit_record;
use crate::Cap;

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
    /// Choose one from a list; its alias is printed, for `capdag run $(…)`
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
    let mut all = registry
        .get_cached_caps()
        .await
        .map_err(|e| CliError::failed("the capabilities could not be read from the fabric cache").context(e.to_string()))?;
    all.retain(|cap| !cap.aliases.is_empty() && cap.primary_alias() != "identity");
    let takes = match &args.input {
        Some(t) => Some(super::plan::target_media(&registry, t).await?),
        None => None,
    };
    let gives = match &args.output {
        Some(t) => Some(super::plan::target_media(&registry, t).await?),
        None => None,
    };
    let mut listed: Vec<Listed> = Vec::new();
    for cap in &all {
        let input = cap.urn.in_media_urn().ok();
        let output = cap.urn.out_media_urn().ok();
        if let (Some(wanted), Some(input)) = (&takes, &input) {
            if !wanted.conforms_to(input).unwrap_or(false) {
                continue;
            }
        }
        if let (Some(wanted), Some(output)) = (&gives, &output) {
            if !output.conforms_to(wanted).unwrap_or(false) {
                continue;
            }
        }
        let entry = listed_cap(&registry, cap, input.as_ref(), output.as_ref());
        if matches_query(&args.query, cap, &entry) {
            listed.push(entry);
        }
    }
    listed.sort_by(|a, b| a.alias.cmp(&b.alias));
    if listed.is_empty() {
        let mut error = CliError::failed("no capability matches");
        if !args.query.is_empty() {
            error = error.context(format!("looked for: {}", args.query.join(" ")));
        }
        return Err(error.hint("fewer words find more; `capdag caps` lists every capability"));
    }
    if args.pick {
        if !ctx.term.interactive && listed.len() > 1 {
            let mut error = CliError::usage(format!("{} capabilities match, and --pick has no terminal to choose on", listed.len()));
            for l in listed.iter().take(8) {
                error = error.context(format!("{}  {}", l.alias, l.title));
            }
            return Err(error.hint("add words to the query until one matches, or leave out --pick to list them"));
        }
        let choices: Vec<Choice> = listed
            .iter()
            .map(|l| Choice {
                label: l.alias.clone(),
                detail: format!("{} · {} → {}", l.title, l.takes, l.gives),
                urn: Some(l.urn.clone()),
                value: l.alias.clone(),
            })
            .collect();
        let index = choose(ctx, &Question { what: "a capability", flag: "QUERY" }, &choices).await?;
        if ctx.term.json {
            emit_record(&record(&listed[index]));
        } else {
            println!("{}", listed[index].alias);
        }
        return Ok(Exit::Ok);
    }
    if ctx.term.json {
        for l in &listed {
            emit_record(&record(l));
        }
        return Ok(Exit::Ok);
    }
    let width = listed.iter().map(|l| l.alias.chars().count()).max().unwrap_or(0).min(34);
    for l in &listed {
        let urn = if ctx.term.verbosity >= 1 { format!("\n{:width$}  {}", "", ctx.term.style.dim(&l.urn)) } else { String::new() };
        println!(
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
    registry: &crate::FabricRegistry,
    cap: &Cap,
    input: Option<&crate::MediaUrn>,
    output: Option<&crate::MediaUrn>,
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
