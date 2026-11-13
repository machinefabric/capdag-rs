//! The `capdag` command.
//!
//! Verbs for the daily loop — find a capability, run it, plan and run a
//! machine — and nouns for what is managed — cartridges, models, the fabric
//! cache, cartridge development — as cargo and gh arrange theirs. Every
//! command has help with an example (`capdag help <command>`, `-h` short,
//! `--help` long), takes the same global flags, prints what a person reads on
//! stderr and what a program reads on stdout, and fails with what is wrong and
//! what to type next ([`error`]).

pub mod caps;
pub mod cartridge;
pub mod check;
pub mod choose;
pub mod ctx;
pub mod dev;
pub mod error;
pub mod graph;
pub mod info;
pub mod inputs;
pub mod live;
pub mod model;
pub mod names;
pub mod plan;
pub mod run;
pub mod term;
pub mod watch;

use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};

use self::ctx::Ctx;
use self::error::{CliError, CliResult, Exit};
use self::term::{ColorChoice, Term};

/// Find capabilities, run them on files, and plan and run machines of them.
#[derive(Debug, Parser)]
#[command(
    name = "capdag",
    version = ctx::VERSION,
    disable_help_subcommand = true,
    // A bare `capdag` shows the overview, as git does, and exits 2.
    arg_required_else_help = true,
    propagate_version = true,
    max_term_width = 100,
    after_help = "Start here:\n  \
        capdag caps pdf                                   find capabilities that take or give PDF\n  \
        capdag run disbind-pdf report.pdf -o pages        run one on a file\n  \
        capdag plan report.pdf --to txt                   find machines that make text of it\n  \
        capdag run pages.machine report.pdf               run a machine\n\n\
        `capdag help <command>` shows a command's help with examples."
)]
pub struct Cli {
    #[command(flatten)]
    pub global: Global,
    #[command(subcommand)]
    pub command: Command,
}

/// Flags every command takes.
#[derive(Debug, clap::Args)]
pub struct Global {
    /// Resolve caps, media and aliases against this fabric instead of the built-in one
    /// (env: CDG_FABRIC_REGISTRY_URL)
    #[arg(long, global = true, value_name = "URL", action = clap::ArgAction::Append)]
    pub fabric: Vec<String>,
    /// Use the network for nothing: what is not cached or bundled is an error that names it
    #[arg(long, global = true)]
    pub offline: bool,
    /// Print JSON Lines on stdout — one record per line — instead of text for a person
    #[arg(long, global = true)]
    pub json: bool,
    /// Never ask: a choice not given by a flag is an error naming the flag
    #[arg(long, global = true)]
    pub no_input: bool,
    /// When to colour the output
    #[arg(long, global = true, value_enum, default_value_t = ColorChoice::Auto, value_name = "WHEN")]
    pub color: ColorChoice,
    /// Say more: -v adds the cartridges' info logs and URNs, -vv their debug logs
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    pub verbose: u8,
    /// Say only what failed
    #[arg(short, long, global = true, conflicts_with = "verbose")]
    pub quiet: bool,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// List and search capabilities
    #[command(
        visible_alias = "search",
        after_help = "Examples:\n  \
            capdag caps                       every capability\n  \
            capdag caps pdf                   capabilities whose name, title or media mention pdf\n  \
            capdag caps --in pdf --out png    capabilities that make PNG of PDF\n  \
            capdag caps --pick pdf            choose one; its alias is printed\n  \
            capdag run $(capdag caps --pick pdf) report.pdf"
    )]
    Caps(caps::Args),
    /// Show a capability or a media type: what it takes, gives and needs
    #[command(after_help = "Examples:\n  \
        capdag info disbind-pdf           a capability: input, options, output, cartridge, example\n  \
        capdag info pdf                   a media type: its URN, extensions, the caps that take it\n  \
        capdag info disbind-pdf --json    the capability's definition, as the fabric publishes it")]
    Info(info::Args),
    /// Run a capability or a machine on files
    #[command(after_help = "Examples:\n  \
        capdag run csv2json people.csv                       one cap; the result on stdout\n  \
        cat report.pdf | capdag run disbind-pdf -o pages     piped input; pages written to pages/\n  \
        capdag run disbind-pdf report.pdf --index-range 1-5  the cap's own options, as it declares them\n  \
        capdag run pages.machine report.pdf -o out           a machine\n  \
        capdag run pages.machine --input doc=@report.pdf     an input by its name in the machine\n\n\
        A single result goes to stdout; several results are written as files, whose\n\
        paths are listed on stdout. Progress, logs and errors go to stderr.\n\n\
        Ctrl-C stops the run: nothing new starts, what started finishes and is written.\n\
        Ctrl-C again aborts it.\n\n\
        `capdag run <cap> --help` shows that cap's own options.")]
    Run(run::Args),
    /// Find machines that turn files into a target, and save or run one
    #[command(after_help = "Examples:\n  \
        capdag plan report.pdf                            what report.pdf can become\n  \
        capdag plan report.pdf --to txt                   machines that make text of it\n  \
        capdag plan report.pdf --to txt --pick 1 --save pages.machine\n  \
        capdag plan report.pdf --to txt --pick 1 --run -o out\n\n\
        On a terminal, a target or machine not given by --to or --pick is chosen from a list.")]
    Plan(plan::Args),
    /// Check a machine file without running it
    #[command(after_help = "Examples:\n  \
        capdag check pages.machine        its inputs, steps and outputs, or what is wrong and where")]
    Check(check::Args),
    /// Draw a machine: as text, Mermaid or Graphviz
    #[command(after_help = "Examples:\n  \
        capdag graph pages.machine                    as text, in the terminal\n  \
        capdag graph pages.machine --format mermaid   for a Markdown page\n  \
        capdag graph pages.machine --format dot | dot -Tsvg > pages.svg")]
    Graph(graph::GraphArgs),
    /// Look at a machine in the terminal: its graph, steps and arguments
    #[command(after_help = "Examples:\n  \
        capdag view pages.machine\n\n\
        Needs a terminal; without one it prints the machine as `capdag graph` does.")]
    View(graph::ViewArgs),
    /// Edit a machine in the terminal, checking every change against the fabric
    #[command(after_help = "Examples:\n  \
        capdag edit pages.machine\n\n\
        Needs a terminal. Without one, edit the file in any editor and run `capdag check` on it.")]
    Edit(graph::EditArgs),
    /// The cartridges that provide capabilities: list, install, remove
    #[command(subcommand)]
    Cartridge(cartridge::Command),
    /// The models capabilities run: list, search, download, remove
    #[command(subcommand)]
    Model(model::Command),
    /// The local copy of the fabric: refresh or clear it
    #[command(subcommand)]
    Cache(CacheCommand),
    /// Develop cartridges: start a project, install it for local runs
    #[command(subcommand)]
    Dev(dev::Command),
    /// This capdag itself
    #[command(name = "self", subcommand)]
    SelfCmd(SelfCommand),
    /// Show help for a command
    #[command(after_help = "Examples:\n  capdag help run\n  capdag help cartridge install")]
    Help {
        /// The command, and its subcommand
        command: Vec<String>,
    },
    /// Anything else: answered with what it most likely meant.
    #[command(external_subcommand)]
    Unknown(Vec<String>),
}

#[derive(Debug, Subcommand)]
pub enum CacheCommand {
    /// Fetch the fabric's manifest and everything it pins afresh
    #[command(after_help = "Examples:\n  capdag cache refresh")]
    Refresh,
    /// Remove the local copy; the next command fetches what it needs
    #[command(after_help = "Examples:\n  capdag cache clear")]
    Clear,
}

#[derive(Debug, Subcommand)]
pub enum SelfCommand {
    /// Replace this capdag with its channel's latest release
    #[command(after_help = "Examples:\n  capdag self update")]
    Update,
}

/// Run the command line given, and return the process's exit status.
pub async fn main() -> i32 {
    let argv: Vec<String> = std::env::args().collect();
    let argv = run::separate_cap_tokens(Cli::command(), argv);
    let matches = match Cli::command().try_get_matches_from(&argv) {
        Ok(matches) => matches,
        Err(error) => return clap_error(error),
    };
    let cli = match Cli::from_arg_matches(&matches) {
        Ok(cli) => cli,
        Err(error) => return clap_error(error),
    };
    let term = Term::decide(
        cli.global.json,
        cli.global.no_input,
        cli.global.color,
        cli.global.verbose,
        cli.global.quiet,
    );
    let style = term.style;
    let json = term.json;
    let outcome = match one_fabric(&cli.global.fabric).and_then(ctx::bind_fabric) {
        Ok(()) => {
            let ctx = Ctx::new(term, cli.global.offline);
            dispatch(&ctx, cli.command).await
        }
        Err(error) => Err(error),
    };
    match outcome {
        Ok(exit) => exit.code(),
        Err(error) => {
            if json {
                term::emit_record(&error.record());
            }
            eprint!("{}", error.render(&style));
            error.exit.code()
        }
    }
}

/// The one fabric `--fabric` names: given twice, it must name the same one —
/// a run resolves against exactly one fabric.
fn one_fabric(given: &[String]) -> CliResult<Option<&str>> {
    match given {
        [] => Ok(None),
        [first, rest @ ..] => match rest.iter().find(|other| *other != first) {
            Some(other) => Err(CliError::usage(format!("--fabric was given twice, as '{first}' and '{other}'"))
                .context("a command resolves against exactly one fabric")
                .hint("give --fabric once")),
            None => Ok(Some(first.as_str())),
        },
    }
}

/// clap's own errors — an unknown flag, a missing value, no command at all —
/// exit 2 with clap's message on stderr, which names the problem and the
/// usage; `--help` and `--version` print on stdout and exit 0.
fn clap_error(error: clap::Error) -> i32 {
    let _ = error.print();
    match error.kind() {
        clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion => 0,
        _ => 2,
    }
}

async fn dispatch(ctx: &Ctx, command: Command) -> CliResult<Exit> {
    match command {
        Command::Caps(args) => caps::caps(ctx, args).await,
        Command::Info(args) => info::info(ctx, args).await,
        Command::Run(args) => run::run(ctx, args).await,
        Command::Plan(args) => plan::plan(ctx, args).await,
        Command::Check(args) => check::check(ctx, args).await,
        Command::Graph(args) => graph::graph(ctx, args).await,
        Command::View(args) => graph::view(ctx, args).await,
        Command::Edit(args) => graph::edit(ctx, args).await,
        Command::Cartridge(command) => cartridge::cartridge(ctx, command).await,
        Command::Model(command) => model::model(ctx, command).await,
        Command::Cache(command) => cache(ctx, command).await,
        Command::Dev(command) => dev::dev(ctx, command).await,
        Command::SelfCmd(SelfCommand::Update) => self_update(ctx).await,
        Command::Help { command } => help(&command),
        Command::Unknown(words) => unknown(ctx, words).await,
    }
}

/// `capdag help <command> [<subcommand>]`: that command's long help.
fn help(path: &[String]) -> CliResult<Exit> {
    let mut command = Cli::command();
    command.build();
    let mut at = &mut command;
    for (depth, name) in path.iter().enumerate() {
        let known: Vec<String> = at.get_subcommands().map(|c| c.get_name().to_string()).collect();
        at = match at.find_subcommand_mut(name) {
            Some(sub) => sub,
            None => {
                let so_far = path[..depth].join(" ");
                let mut error = CliError::usage(format!(
                    "there is no command `capdag {}{name}`",
                    if so_far.is_empty() { String::new() } else { format!("{so_far} ") }
                ));
                if let Some(near) = nearest(name, known.iter().map(String::as_str)) {
                    error = error.hint(format!("did you mean `{near}`?"));
                } else {
                    error = error.context(format!("the commands are: {}", known.join(", ")));
                }
                return Err(error);
            }
        };
    }
    let _ = at.print_long_help();
    Ok(Exit::Ok)
}

/// A first word that is no command: a capability, a machine file, or a typo.
async fn unknown(ctx: &Ctx, words: Vec<String>) -> CliResult<Exit> {
    let word = words.first().cloned().unwrap_or_default();
    let rest = shell_words(&words[1..]);
    let commands: Vec<String> = Cli::command()
        .get_subcommands()
        .filter(|c| !c.is_hide_set())
        .map(|c| c.get_name().to_string())
        .collect();
    if word.ends_with(".machine") {
        return Err(CliError::usage(format!("'{word}' is a machine file, not a command"))
            .hint(format!("run it with: capdag run {word}{rest}")));
    }
    if let Some(near) = nearest(&word, commands.iter().map(String::as_str)) {
        return Err(CliError::usage(format!("there is no command `{word}`"))
            .hint(format!("did you mean `capdag {near}`?")));
    }
    // A capability's name: the bare form is not a command. Say how to run it.
    // Whether it names one is the registry's to say: when the registry
    // cannot be asked, that is said, not "no capability".
    let registry = ctx.registry().await?;
    match registry.get_cap(&word).await {
        Ok(cap) => {
            return Err(CliError::usage(format!("'{word}' is a capability, not a command"))
                .context(format!("{} — {}", names::cap(&registry, &cap), cap.title))
                .hint(format!("run it with: capdag run {word}{rest}")));
        }
        Err(e) => ctx::nothing_by_that_name(e)?,
    }
    Err(CliError::usage(format!("there is no command or capability named `{word}`"))
        .context(format!("the commands are: {}", commands.join(", ")))
        .hint(format!("look for a capability with: capdag caps {word}")))
}

/// The command closest to `word`, when one is close enough to be what was meant.
pub fn nearest<'a>(word: &str, candidates: impl Iterator<Item = &'a str>) -> Option<&'a str> {
    candidates
        .map(|candidate| (strsim::jaro_winkler(word, candidate), candidate))
        .filter(|(score, _)| *score > 0.8)
        .max_by(|a, b| a.0.total_cmp(&b.0))
        .map(|(_, candidate)| candidate)
}

/// Arguments as they would be typed again: ` a 'b c'`.
fn shell_words(words: &[String]) -> String {
    words
        .iter()
        .map(|word| {
            if word.is_empty() || word.chars().any(|c| c.is_whitespace() || "'\"$`\\|&;<>()*?".contains(c)) {
                format!(" '{}'", word.replace('\'', "'\\''"))
            } else {
                format!(" {word}")
            }
        })
        .collect()
}

async fn cache(ctx: &Ctx, command: CacheCommand) -> CliResult<Exit> {
    // Cleared without opening a registry on it: a cache holding a corrupt
    // entry is one a registry refuses to open, and clearing is what that
    // refusal says to do. The folder is the one every other command reads: the
    // fabric this process is bound to, at the manifest version it is built for.
    let config = crate::RegistryConfig::default();
    let dir = crate::FabricRegistry::clear_cache_of(&config, crate::FABRIC_MANIFEST_VERSION)
        .await
        .map_err(|e| {
            CliError::failed(format!("the fabric cache of {} could not be cleared", config.anchor))
                .context(e.to_string())
        })?
        .display()
        .to_string();
    let refreshed = match command {
        CacheCommand::Clear => None,
        // The manifest and everything it pins, fetched afresh into the now
        // empty cache, so the renewal is complete rather than lazy.
        CacheCommand::Refresh => Some(ctx.fresh_registry().await?.manifest_version()),
    };
    if ctx.term.json {
        term::emit_record(&serde_json::json!({
            "type": "cache",
            "dir": dir,
            "refreshed": refreshed.is_some(),
            "manifest": refreshed,
        }));
    } else if ctx.term.verbosity >= 0 {
        match refreshed {
            Some(manifest) => eprintln!("Fabric cache refreshed at {dir}: manifest {manifest}"),
            None => eprintln!("Fabric cache cleared at {dir}; the next command fetches what it needs."),
        }
    }
    Ok(Exit::Ok)
}

/// `capdag self update`: replace this capdag, and the cartridges bundled
/// beside it, with its channel's latest release — installed only when the
/// release's manifest chains to this build's roots ([`crate::self_update`]).
async fn self_update(ctx: &Ctx) -> CliResult<Exit> {
    if ctx.offline {
        return Err(CliError::usage("capdag cannot update itself offline").hint("drop --offline"));
    }
    let updated = crate::self_update::update(ctx::VERSION)
        .await
        .map_err(|e| CliError::failed("capdag was not updated").context(e.to_string()))?;
    if ctx.term.json {
        term::emit_record(&serde_json::json!({
            "type": "self_update",
            "from": updated.from,
            "to": updated.to,
        }));
    } else if updated.from == updated.to {
        eprintln!("capdag {} is the latest release", updated.to);
    } else {
        eprintln!("capdag {} -> {}", updated.from, updated.to);
    }
    Ok(Exit::Ok)
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST20113: the command tree is well formed: clap's own checks pass, and
    // every command a person sees has help with at least one example.
    #[test]
    fn test20113_every_command_has_help_with_an_example() {
        let mut command = Cli::command();
        command.build();
        command.clone().debug_assert();
        fn walk(command: &clap::Command, path: &str, missing: &mut Vec<String>) {
            for sub in command.get_subcommands() {
                if sub.is_hide_set() || sub.get_name() == "help" {
                    continue;
                }
                let here = format!("{path} {}", sub.get_name());
                if sub.has_subcommands() {
                    walk(sub, &here, missing);
                    continue;
                }
                let help = sub.get_after_help().map(|h| h.to_string()).unwrap_or_default();
                if sub.get_about().is_none() || !help.contains("capdag ") {
                    missing.push(here);
                }
            }
        }
        let mut missing = Vec::new();
        walk(&command, "capdag", &mut missing);
        assert!(missing.is_empty(), "commands without help and an example: {missing:?}");
    }

    // TEST20114: a mistyped command is answered with the one it meant.
    #[test]
    fn test20114_a_mistyped_command_is_answered_with_the_one_meant() {
        let commands = ["caps", "info", "run", "plan", "check", "graph", "cartridge"];
        assert_eq!(nearest("cpas", commands.iter().copied()), Some("caps"));
        assert_eq!(nearest("plna", commands.iter().copied()), Some("plan"));
        assert_eq!(nearest("summarize-en", commands.iter().copied()), None);
    }
}
