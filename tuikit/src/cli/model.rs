//! `capdag model list | search | pull | remove`: the models capabilities run.
//!
//! Each is done by the model cartridge bundled with capdag, through its own
//! caps (`model-list`, `search-models`, `model-download`, `model-delete`), so
//! the command line and every cap that downloads a model do it one way.

use super::choose::{choose, Choice, Question};
use super::ctx::Ctx;
use super::error::{CliError, CliResult, Exit};
use super::live::ProgressChoice;
use super::run::cap_value;
use super::term::emit_record;

#[derive(Debug, clap::Subcommand)]
pub enum Command {
    /// The models on this machine
    #[command(after_help = "Examples:\n  capdag model list\n  capdag model list --json")]
    List,
    /// Find models to download
    #[command(after_help = "Examples:\n  \
        capdag model search qwen gguf\n  \
        capdag model search llava --pick          choose one; its spec is printed\n  \
        capdag model pull $(capdag model search qwen --pick)")]
    Search {
        /// Words to search for
        #[arg(value_name = "QUERY", required = true)]
        query: Vec<String>,
        /// How many results at most
        #[arg(long, default_value_t = 20)]
        limit: u32,
        /// Choose one from a list; its spec is printed
        #[arg(long)]
        pick: bool,
    },
    /// Download a model, with its progress
    #[command(after_help = "Examples:\n  \
        capdag model pull 'hf:unsloth/Qwen3-4B-Instruct-2507-GGUF?include=Qwen3-4B-Instruct-2507-Q4_K_M.gguf'")]
    Pull {
        /// The model's spec: hf:<owner>/<repo>[?include=<file>]
        #[arg(value_name = "SPEC")]
        spec: String,
    },
    /// Remove a downloaded model
    #[command(after_help = "Examples:\n  capdag model remove 'hf:unsloth/Qwen3-4B-Instruct-2507-GGUF'")]
    Remove {
        /// The model's spec, as `capdag model list` shows it
        #[arg(value_name = "SPEC")]
        spec: String,
    },
}

pub async fn model(ctx: &Ctx, command: Command) -> CliResult<Exit> {
    match command {
        Command::List => list(ctx).await,
        Command::Search { query, limit, pick } => search(ctx, query.join(" "), limit, pick).await,
        Command::Pull { spec } => pull(ctx, &spec).await,
        Command::Remove { spec } => remove(ctx, &spec).await,
    }
}

fn json(alias: &str, bytes: &[u8]) -> CliResult<serde_json::Value> {
    serde_json::from_slice(bytes).map_err(|e| {
        CliError::failed(format!("{alias} gave something other than JSON")).context(e.to_string())
    })
}

/// A spec the model cartridge can read: `hf:<owner>/<repo>[?…]`.
fn check_spec(spec: &str) -> CliResult<()> {
    let ok = spec
        .strip_prefix("hf:")
        .map(|rest| rest.split('?').next().unwrap_or("").split('/').filter(|p| !p.is_empty()).count() == 2)
        .unwrap_or(false);
    if ok {
        Ok(())
    } else {
        Err(CliError::usage(format!("'{spec}' is not a model spec"))
            .context("a spec is hf:<owner>/<repo>, optionally ?include=<file>")
            .hint("find one with: capdag model search <words>"))
    }
}

async fn list(ctx: &Ctx) -> CliResult<Exit> {
    let bytes = cap_value(ctx, "model-list", Some(b"huggingface".to_vec()), ProgressChoice::None).await?;
    let response = json("model-list", &bytes)?;
    let models = response["models"].as_array().cloned().unwrap_or_default();
    if ctx.term.json {
        for model in &models {
            emit_record(&serde_json::json!({ "type": "model", "model": model }));
        }
        return Ok(Exit::Ok);
    }
    if models.is_empty() {
        println!("No models are downloaded. Find one with: capdag model search <words>");
        return Ok(Exit::Ok);
    }
    for model in &models {
        let size = model["size_bytes"].as_u64().or_else(|| model["total_bytes"].as_u64());
        println!(
            "{}  {}{}",
            ctx.term.style.name(model["model_spec"].as_str().unwrap_or("?")),
            model["state"].as_str().map(str::to_string).unwrap_or_else(|| model["state"].to_string()),
            size.map(|b| format!("  {:.1} GB", b as f64 / 1e9)).unwrap_or_default(),
        );
    }
    Ok(Exit::Ok)
}

async fn search(ctx: &Ctx, query: String, limit: u32, pick: bool) -> CliResult<Exit> {
    let request = serde_json::json!({ "query": query, "limit": limit });
    let bytes = cap_value(ctx, "search-models", Some(request.to_string().into_bytes()), ProgressChoice::None).await?;
    let response = json("search-models", &bytes)?;
    let models = response["models"].as_array().cloned().unwrap_or_default();
    if models.is_empty() {
        return Err(CliError::failed(format!("no model matches '{query}'")).hint("try fewer or other words"));
    }
    let spec_of = |m: &serde_json::Value| format!("hf:{}", m["id"].as_str().unwrap_or(""));
    if pick {
        if !ctx.term.interactive {
            return Err(CliError::usage(format!("{} models match, and --pick has no terminal to choose on", models.len()))
                .hint("leave out --pick to list them, and give the spec you want to `capdag model pull`"));
        }
        let choices: Vec<Choice> = models
            .iter()
            .map(|m| Choice {
                label: spec_of(m),
                detail: format!(
                    "{} downloads · {} likes",
                    m["downloads"].as_u64().unwrap_or(0),
                    m["likes"].as_u64().unwrap_or(0)
                ),
                urn: None,
                value: spec_of(m),
            })
            .collect();
        let index = choose(ctx, &Question { what: "a model", flag: "SPEC" }, &choices).await?;
        println!("{}", spec_of(&models[index]));
        return Ok(Exit::Ok);
    }
    if ctx.term.json {
        for model in &models {
            emit_record(&serde_json::json!({ "type": "model", "spec": spec_of(model), "model": model }));
        }
        return Ok(Exit::Ok);
    }
    for model in &models {
        println!(
            "{}  {}",
            ctx.term.style.name(&spec_of(model)),
            ctx.term.style.dim(&format!(
                "{} downloads · {} likes",
                model["downloads"].as_u64().unwrap_or(0),
                model["likes"].as_u64().unwrap_or(0)
            )),
        );
    }
    eprintln!("\nDownload one: capdag model pull <spec>");
    Ok(Exit::Ok)
}

async fn pull(ctx: &Ctx, spec: &str) -> CliResult<Exit> {
    check_spec(spec)?;
    if ctx.offline {
        return Err(CliError::usage("a model cannot be downloaded offline").hint("drop --offline"));
    }
    let bytes = cap_value(ctx, "model-download", Some(spec.as_bytes().to_vec()), ProgressChoice::Auto).await?;
    let response = json("model-download", &bytes)?;
    if ctx.term.json {
        emit_record(&serde_json::json!({ "type": "model_downloaded", "spec": spec, "result": response }));
    } else {
        eprintln!("Downloaded {spec}");
    }
    Ok(Exit::Ok)
}

async fn remove(ctx: &Ctx, spec: &str) -> CliResult<Exit> {
    check_spec(spec)?;
    let bytes = cap_value(ctx, "model-delete", Some(spec.as_bytes().to_vec()), ProgressChoice::None).await?;
    let response = json("model-delete", &bytes)?;
    if ctx.term.json {
        emit_record(&serde_json::json!({ "type": "model_removed", "spec": spec, "result": response }));
    } else {
        eprintln!("Removed {spec}");
    }
    Ok(Exit::Ok)
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST20121: a spec that is not hf:<owner>/<repo> is a usage error saying
    // what one is, before anything runs.
    #[test]
    fn test20121_a_malformed_model_spec_is_refused_saying_what_one_is() {
        assert!(check_spec("hf:unsloth/Qwen3-4B-Instruct-2507-GGUF?include=x.gguf").is_ok());
        let error = check_spec("garbage").unwrap_err();
        assert_eq!(error.problem, "'garbage' is not a model spec");
        assert_eq!(error.exit, super::super::error::Exit::Usage);
        assert!(check_spec("hf:only-owner").is_err());
    }
}
