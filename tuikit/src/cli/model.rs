//! `capdag model list | search | pull | remove`: the models capabilities run.
//!
//! Each is done by the model cartridge bundled with capdag, through its own
//! caps (`model-list`, `search-models`, `model-download`, `model-delete`), so
//! the command line and every cap that downloads a model do it one way.

use super::ask::{self, Answered, Choice, Question};
use super::ctx::Ctx;
use super::error::{CliError, CliResult, Exit};
use super::formal as f;
use super::live::ProgressChoice;
use super::run::cap_value;
use super::term::{emit_record, outln};

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
        /// How many results are listed at most; --pick chooses among all that are found
        #[arg(long, default_value_t = 20)]
        limit: u32,
        /// Choose one; its spec is printed
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

/// The command as the model knows it, before its own facts are known.
pub fn placeholder(command: &Command) -> f::Command {
    match command {
        Command::List => f::Command::ModelList,
        Command::Search { pick, .. } => f::Command::ModelSearch { pick: *pick, found: f::Choices::None },
        Command::Pull { .. } => f::Command::ModelPull { spec_valid: true },
        Command::Remove { .. } => f::Command::ModelRemove { spec_valid: true, downloaded: true },
    }
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

/// Whether a spec is one the model cartridge can read: `hf:<owner>/<repo>[?…]`.
fn is_spec(spec: &str) -> bool {
    spec.strip_prefix("hf:")
        .map(|rest| rest.split('?').next().unwrap_or("").split('/').filter(|p| !p.is_empty()).count() == 2)
        .unwrap_or(false)
}

/// A spec the model refused as no spec, said with it.
fn not_a_spec(spec: &str, verdict: &f::CliVerdict) -> CliError {
    super::say::with_exit(
        CliError::usage(format!("'{spec}': {}", f::says(&f::CliRefusal::NotAModelSpec)))
            .hint("find one with: capdag model search <words>"),
        verdict,
    )
}

/// The models the model cartridge knows on this machine, as `list-models`
/// lists them: HuggingFace's, the one kind of model repository.
async fn downloaded(ctx: &Ctx) -> CliResult<Vec<serde_json::Value>> {
    let request = serde_json::json!({ "kind": "huggingface" });
    let bytes = cap_value(ctx, "list-models", Some(request.to_string().into_bytes()), Vec::new(), None).await?;
    let response = json("list-models", &bytes)?;
    response["models"]
        .as_array()
        .cloned()
        .ok_or_else(|| CliError::failed("list-models gave no list of models").context(response.to_string()))
}

async fn list(ctx: &Ctx) -> CliResult<Exit> {
    if let Some(error) = super::ends(&ctx.decide(f::Command::ModelList, false)) {
        return Err(error);
    }
    let models = downloaded(ctx).await?;
    if ctx.term.json {
        for model in &models {
            emit_record(&serde_json::json!({ "type": "model", "model": model }));
        }
        return Ok(Exit::Ok);
    }
    if models.is_empty() {
        outln!("No models are downloaded. Find one with: capdag model search <words>");
        return Ok(Exit::Ok);
    }
    for model in &models {
        let size = model["size_bytes"].as_u64().or_else(|| model["total_bytes"].as_u64());
        outln!(
            "{}  {}{}",
            ctx.term.style.name(model["model_spec"].as_str().unwrap_or("?")),
            model["state"].as_str().map(str::to_string).unwrap_or_else(|| model["state"].to_string()),
            size.map(|b| format!("  {:.1} GB", b as f64 / 1e9)).unwrap_or_default(),
        );
    }
    Ok(Exit::Ok)
}

async fn search(ctx: &Ctx, query: String, limit: u32, pick: bool) -> CliResult<Exit> {
    // What --pick chooses among is everything the search finds: --limit
    // limits only what is listed.
    let request = if pick {
        serde_json::json!({ "query": query })
    } else {
        serde_json::json!({ "query": query, "limit": limit })
    };
    let bytes = cap_value(ctx, "search-models", Some(request.to_string().into_bytes()), Vec::new(), None).await?;
    let response = json("search-models", &bytes)?;
    let models = response["models"].as_array().cloned().unwrap_or_default();
    let spec_of = |m: &serde_json::Value| format!("hf:{}", m["id"].as_str().unwrap_or(""));
    let found = match models.len() {
        0 => f::Choices::None,
        1 => f::Choices::One,
        _ => f::Choices::Many,
    };
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
    let question = Question { what: "a model".into(), flag: "capdag model pull ".into() };
    let verdict = ctx.decide(f::Command::ModelSearch { pick, found }, false);
    let picked = match &verdict {
        f::CliVerdict::Is { v: f::Verdict::Answers } if pick => Some(0),
        f::CliVerdict::Is { v: f::Verdict::Answers } => None,
        f::CliVerdict::Is { v: f::Verdict::Asks { s: f::Stage::Model } } => match ask::pick(ctx, &question, &choices).await? {
            Answered::Given(at) => Some(at),
            Answered::Declined => return Err(ask::declined()),
        },
        f::CliVerdict::Is { v: f::Verdict::Names { .. } } => {
            return Err(super::say::with_exit(
                ask::named(ctx, &question, &choices).hint("add words until one model matches, or leave out --pick to list them"),
                &verdict,
            ))
        }
        f::CliVerdict::Is { v: f::Verdict::Nothing { .. } } => {
            return Err(super::say::with_exit(
                CliError::failed(format!("no model matches '{query}'")).hint("try fewer or other words"),
                &verdict,
            ))
        }
        other => return Err(super::ends(other).expect("a verdict other than answers ends the command")),
    };
    if let Some(at) = picked {
        if ctx.term.json {
            emit_record(&serde_json::json!({ "type": "model", "spec": spec_of(&models[at]), "model": models[at] }));
        } else {
            outln!("{}", spec_of(&models[at]));
        }
        return Ok(Exit::Ok);
    }
    if ctx.term.json {
        for model in &models {
            emit_record(&serde_json::json!({ "type": "model", "spec": spec_of(model), "model": model }));
        }
        return Ok(Exit::Ok);
    }
    for choice in &choices {
        outln!("{}  {}", ctx.term.style.name(&choice.label), ctx.term.style.dim(&choice.detail));
    }
    eprintln!("\nDownload one: capdag model pull <spec>");
    Ok(Exit::Ok)
}

async fn pull(ctx: &Ctx, spec: &str) -> CliResult<Exit> {
    let verdict = ctx.decide(f::Command::ModelPull { spec_valid: is_spec(spec) }, false);
    if let f::CliVerdict::Refused { r: f::CliRefusal::NotAModelSpec } = verdict {
        return Err(not_a_spec(spec, &verdict));
    }
    if let Some(error) = super::ends(&verdict) {
        return Err(error);
    }
    // The model a capability downloads is held to the pattern of model it
    // takes; one pulled for no capability is held to none beyond being a
    // model spec.
    let pattern = (capdag::MEDIA_MEDIA_URN.to_string(), capdag::urn::media_urn::MEDIA_MODEL_SPEC.as_bytes().to_vec());
    let bytes =
        cap_value(ctx, "model-download", Some(spec.as_bytes().to_vec()), vec![pattern], Some(ProgressChoice::Auto)).await?;
    let response = json("model-download", &bytes)?;
    if ctx.term.json {
        emit_record(&serde_json::json!({ "type": "model_downloaded", "spec": spec, "result": response }));
    } else {
        eprintln!("Downloaded {spec}");
    }
    Ok(Exit::Ok)
}

async fn remove(ctx: &Ctx, spec: &str) -> CliResult<Exit> {
    let spec_valid = is_spec(spec);
    let present = if spec_valid {
        let wanted = spec.split('?').next().unwrap_or(spec);
        downloaded(ctx).await?.iter().any(|model| {
            model["local_available"].as_bool() == Some(true)
                && model["model_spec"].as_str().is_some_and(|have| have == spec || have.split('?').next() == Some(wanted))
        })
    } else {
        false
    };
    let verdict = ctx.decide(f::Command::ModelRemove { spec_valid, downloaded: present }, false);
    match &verdict {
        f::CliVerdict::Refused { r: f::CliRefusal::NotAModelSpec } => return Err(not_a_spec(spec, &verdict)),
        f::CliVerdict::Refused { r: f::CliRefusal::NotDownloaded } => {
            return Err(super::say::with_exit(
                CliError::usage(format!("'{spec}': {}", f::says(&f::CliRefusal::NotDownloaded))),
                &verdict,
            ))
        }
        other => {
            if let Some(error) = super::ends(other) {
                return Err(error);
            }
        }
    }
    let bytes = cap_value(ctx, "model-delete", Some(spec.as_bytes().to_vec()), Vec::new(), None).await?;
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

    // TEST20121: a spec is hf:<owner>/<repo>, optionally ?include=<file>; the
    // model refuses anything else before anything runs.
    #[test]
    fn test20121_a_model_spec_is_hf_owner_repo() {
        assert!(is_spec("hf:unsloth/Qwen3-4B-Instruct-2507-GGUF?include=x.gguf"));
        assert!(!is_spec("garbage"));
        assert!(!is_spec("hf:only-owner"));
    }
}
