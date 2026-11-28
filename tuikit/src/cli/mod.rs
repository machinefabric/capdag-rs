//! The `capdag` command.
//!
//! Verbs for the daily loop — find a capability, run a machine, see what
//! files can become — and nouns for what is managed — cartridges, models, the
//! fabric cache, cartridge development — as cargo and gh arrange theirs. What
//! every line comes to is the model's decision ([`formal`]): the argument
//! definitions here name exactly the commands and flags its table names, and
//! a test holds them to it. Every command has help with an example
//! (`capdag help <command>`, `-h` short, `--help` long), takes the same global
//! flags, prints what a person reads on stderr and what a program reads on
//! stdout, and fails with what is wrong and what to type next ([`error`]).

pub mod ask;
pub mod caps;
pub mod cartridge;
pub mod ctx;
pub mod dev;
pub mod dry_run;
pub mod error;
pub mod explain;
pub mod formal;
pub mod graph;
pub mod host;
pub mod info;
pub mod inline;
pub mod inputs;
pub mod inspect;
pub mod line;
pub mod live;
pub mod model;
pub mod names;
pub mod run;
pub mod say;
pub mod term;
pub mod watch;

use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};

use self::ctx::Ctx;
use self::error::{CliError, CliResult, Exit};
use self::formal as f;
use self::host::Host;
use self::term::{ColorChoice, Term};

/// Run machines of capabilities on files, find capabilities, and see what files can become.
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
        capdag caps pdf                                        find capabilities that take or give PDF\n  \
        capdag run report.pdf --dry-run                        what report.pdf can become\n  \
        capdag run report.pdf --to txt                         make text of it, choosing a machine\n  \
        capdag run '[doc -> disbind-pdf -> pages]' report.pdf  run a machine of one step\n  \
        capdag run pages.machine report.pdf                    run a machine saved in a file\n\n\
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
    /// Never ask: what a line leaves open is an error naming the choices and the flag that gives each
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
            capdag caps --pick pdf            choose one; the machine of its one step is printed\n  \
            capdag run \"$(capdag caps --pick pdf)\" report.pdf"
    )]
    Caps(caps::Args),
    /// Show a capability or a media type: what it takes, gives and needs
    #[command(after_help = "Examples:\n  \
        capdag info disbind-pdf           a capability: input, options, output, cartridge, example\n  \
        capdag info pdf                   a media type: its URN, extensions, the caps that take it\n  \
        capdag info disbind-pdf --json    the capability's definition, as the fabric publishes it")]
    Info(info::Args),
    /// Run a machine on inputs: one written inline or in a file, or one planned to --to
    #[command(after_help = "Examples:\n  \
        # a machine of one step\n  \
        capdag run '[doc -> disbind-pdf -> pages]' report.pdf -o pages\n  \
        # a step's option, written in the machine\n  \
        capdag run '[(doc, index-range=\"1-5\") -> disbind-pdf -> pages]' report.pdf\n  \
        # a machine saved in a file, and an input given by its name in it\n  \
        capdag run pages.machine report.pdf\n  \
        capdag run pages.machine --input doc=@report.pdf\n  \
        # stdin as an input\n  \
        cat notes.txt | capdag run summary.machine -\n  \
        # plan a machine that makes text of it, and run it\n  \
        capdag run report.pdf --to txt\n  \
        # what it can become, and the machines that make text of it, running nothing\n  \
        capdag run report.pdf --dry-run\n  \
        capdag run report.pdf --to txt --dry-run\n  \
        # keep the machine chosen in a file\n  \
        capdag run report.pdf --to txt --dry-run > pages.machine\n  \
        # a machine's inputs, steps and the options it leaves to the run\n  \
        capdag run pages.machine --explain\n\n\
        What a line leaves open is asked at a terminal, and named with the flag that gives it\n\
        in a script. A run that settled anything prints the line to run it again, asking nothing.\n\n\
        A single result goes to stdout; several results are written as files, whose\n\
        paths are listed on stdout. Progress, logs and errors go to stderr.\n\n\
        Ctrl-C stops the run: nothing new starts, what started finishes and is written.\n\
        Ctrl-C again aborts it.\n\n\
        --dry-run decides what the run would, asking what it would ask, and runs and installs\n\
        nothing: the machine it would run goes to stdout, and the line that runs it to stderr.\n\
        Where nobody can be asked it lists what is still open and exits 0.\n\n\
        `capdag info <cap>` shows a capability's own options.")]
    Run(run::Args),
    /// Draw a machine: as text, Mermaid or Graphviz
    #[command(after_help = "Examples:\n  \
        capdag graph pages.machine                    as text, in the terminal\n  \
        capdag graph pages.machine --format mermaid   for a Markdown page\n  \
        capdag graph pages.machine --format dot | dot -Tsvg > pages.svg")]
    Graph(graph::GraphArgs),
    /// Look at a machine in the terminal: its graph, steps and arguments
    #[command(after_help = "Examples:\n  \
        capdag view pages.machine\n  \
        capdag view pages.machine report.pdf      run it from the view\n\n\
        Needs a terminal; without one it prints the machine as `capdag graph` does.")]
    View(graph::ViewArgs),
    /// Edit a machine file in the terminal, checking every change against the fabric
    #[command(after_help = "Examples:\n  \
        capdag edit pages.machine\n\n\
        Needs a terminal. Without one, edit the file in any editor, then: capdag run pages.machine --explain")]
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

/// Run the command line this process was given on `host`, and return the
/// process's exit status.
pub async fn run(host: std::sync::Arc<dyn Host>) -> i32 {
    run_command_line(std::env::args().collect(), host).await
}

/// Run `argv` as the command line on `host`, and return the process's exit
/// status.
pub async fn run_command_line(argv: Vec<String>, host: std::sync::Arc<dyn Host>) -> i32 {
    let parsed = Cli::command()
        .try_get_matches_from(&argv)
        .and_then(|matches| Cli::from_arg_matches(&matches));
    let cli = match parsed {
        Ok(cli) => cli,
        Err(error) => return parse_outcome(&argv, error),
    };
    let reads_stdin = match &cli.command {
        Command::Run(args) => args.reads_stdin(),
        _ => false,
    };
    let term = Term::decide(
        cli.global.json,
        cli.global.no_input,
        cli.global.color,
        cli.global.verbose,
        cli.global.quiet,
        reads_stdin,
    );
    let style = term.style;
    let json = term.json;
    let outcome = match ctx::bind_fabric(host.as_ref(), cli.global.fabric.first().map(String::as_str)) {
        Ok(()) => {
            let ctx = Ctx::new(term, cli.global.offline, host, cli.global.fabric.clone());
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

/// A line the argument parser did not hand over as a command: help or the
/// version asked for, no command named, or a line it could not read. clap
/// says what, and the model what it comes to.
fn parse_outcome(argv: &[String], error: clap::Error) -> i32 {
    let invocation = invocation_of(argv, error.kind());
    let _ = error.print();
    exit_without_world(invocation)
}

/// What a line the parser did not hand over is, as the model knows it.
fn invocation_of(argv: &[String], kind: clap::error::ErrorKind) -> f::Invocation {
    use clap::error::ErrorKind;
    match kind {
        ErrorKind::DisplayVersion => f::Invocation::Version,
        ErrorKind::DisplayHelp => f::Invocation::HelpFlag {
            topic: if command_words(argv).is_empty() { f::HelpTopic::Overview } else { f::HelpTopic::Command },
        },
        ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand => match command_words(argv).first().map(String::as_str) {
            None => f::Invocation::Bare,
            Some("cartridge") => f::Invocation::Group { g: f::Group::Cartridge },
            Some("model") => f::Invocation::Group { g: f::Group::Model },
            Some("cache") => f::Invocation::Group { g: f::Group::Cache },
            Some("dev") => f::Invocation::Group { g: f::Group::Dev },
            Some("self") => f::Invocation::Group { g: f::Group::Self_ },
            Some(_) => f::Invocation::Unreadable,
        },
        _ => f::Invocation::Unreadable,
    }
}

/// The exit status of a line decided before any command runs: the model
/// reads nothing of the world for help, the version, a line with no command,
/// a help command, or a line the parser could not read.
fn exit_without_world(invocation: f::Invocation) -> i32 {
    f::exit_of(&f::cli(ctx_free_world(), invocation)).expect("help, the version and a line not read each end the command")
}

/// The words of a line that name its command: every word before the first
/// flag, after the program's name, skipping the values of the flags every
/// command takes.
fn command_words(argv: &[String]) -> Vec<String> {
    let mut words = Vec::new();
    let mut tokens = argv.iter().skip(1);
    while let Some(token) = tokens.next() {
        if token == "--fabric" || token == "--color" {
            tokens.next();
            continue;
        }
        if token.starts_with('-') {
            continue;
        }
        words.push(token.clone());
    }
    words
}

async fn dispatch(ctx: &Ctx, command: Command) -> CliResult<Exit> {
    let none = || f::no_world(ctx.term.model());
    let placeholder = match &command {
        Command::Caps(args) => Some(f::Command::Caps { pick: args.pick, types_known: true, found: f::Choices::None }),
        Command::Info(args) => Some(f::Command::Info { all_known: true, no_cache: args.no_cache }),
        Command::Run(_) => {
            Some(f::Command::Run { l: f::no_run_line(), planning_flags: false, mode: f::RunMode::Run, w: none() })
        }
        Command::Graph(_) => {
            Some(f::Command::Graph { m: f::Written { base: f::nat(0), inline: false }, text: true, width: false, w: none() })
        }
        Command::View(_) => {
            Some(f::Command::View { m: f::Written { base: f::nat(0), inline: false }, inputs: f::list(Vec::new()), w: none() })
        }
        Command::Edit(_) => {
            Some(f::Command::Edit { m: f::Written { base: f::nat(0), inline: false }, inputs: f::list(Vec::new()), w: none() })
        }
        Command::Cartridge(command) => Some(cartridge::placeholder(command)),
        Command::Model(command) => Some(model::placeholder(command)),
        Command::Cache(CacheCommand::Refresh) => Some(f::Command::CacheRefresh),
        Command::Cache(CacheCommand::Clear) => Some(f::Command::CacheClear),
        Command::Dev(command) => Some(dev::placeholder(command)),
        Command::SelfCmd(SelfCommand::Update) => Some(f::Command::SelfUpdate),
        Command::Help { .. } | Command::Unknown(_) => None,
    };
    if let Some(placeholder) = placeholder {
        ctx.preflight(placeholder).await?;
    }
    match command {
        Command::Caps(args) => caps::caps(ctx, args).await,
        Command::Info(args) => info::info(ctx, args).await,
        Command::Run(args) => run::run(ctx, args).await,
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

/// What a command's own verdict comes to, when it is not to go on: the error
/// it ends with. `answers` and `opens` go on, and are the caller's.
pub fn ends(verdict: &f::CliVerdict) -> Option<CliError> {
    match verdict {
        f::CliVerdict::Is { v: f::Verdict::Answers } | f::CliVerdict::Is { v: f::Verdict::Opens } => None,
        f::CliVerdict::Refused { r } => Some(say::with_exit(say::cli_refusal(r), verdict)),
        f::CliVerdict::Is { v } => Some(say::with_exit(
            CliError::failed(format!("the command came to {v:?}")).hint("report it: the command says what each verdict means"),
            verdict,
        )),
    }
}

/// The command `path` names, as clap knows it.
fn command_at(path: &[&str]) -> Option<clap::Command> {
    let mut command = Cli::command();
    command.build();
    let mut at = command;
    for name in path {
        at = at.find_subcommand(name)?.clone();
    }
    Some(at)
}

/// A command's long help, on stdout.
pub fn print_command_help(path: &[&str]) -> CliResult<()> {
    let mut command = command_at(path).ok_or_else(|| CliError::failed(format!("there is no command {}", path.join(" "))))?;
    command
        .print_long_help()
        .map_err(|e| CliError::failed("the help could not be written").context(e.to_string()))
}

/// `capdag help <command> [<subcommand>]`: that command's long help.
fn help(path: &[String]) -> CliResult<Exit> {
    let words: Vec<&str> = path.iter().map(String::as_str).collect();
    let known = command_at(&words).is_some();
    let verdict = f::cli(ctx_free_world(), f::Invocation::HelpCommand { known });
    if let f::CliVerdict::Is { v: f::Verdict::Help } = verdict {
        if words.is_empty() {
            Cli::command()
                .print_long_help()
                .map_err(|e| CliError::failed("the help could not be written").context(e.to_string()))?;
        } else {
            print_command_help(&words)?;
        }
        return Ok(Exit::Ok);
    }
    let mut error = ends(&verdict).expect("help names a command, or is refused");
    error.problem = format!("'{}': {}", words.join(" "), error.problem);
    let depth = (1..=words.len()).rev().find(|n| command_at(&words[..n - 1]).is_some()).unwrap_or(1);
    if let Some(parent) = command_at(&words[..depth - 1]) {
        let known: Vec<String> = parent.get_subcommands().filter(|c| !c.is_hide_set()).map(|c| c.get_name().to_string()).collect();
        error = match nearest(words[depth - 1], known.iter().map(String::as_str)) {
            Some(near) => error.hint(format!("did you mean `{near}`?")),
            None => error.context(format!("the commands are: {}", known.join(", "))),
        };
    }
    Err(error)
}

/// A first word that is no command: a machine, a file, a word close to a
/// command, a capability, or nothing.
async fn unknown(ctx: &Ctx, words: Vec<String>) -> CliResult<Exit> {
    let word = words.first().cloned().unwrap_or_default();
    let rest: String = words[1..].iter().map(|w| format!(" {}", shell_word(w))).collect();
    let commands: Vec<String> = Cli::command()
        .get_subcommands()
        .filter(|c| !c.is_hide_set())
        .map(|c| c.get_name().to_string())
        .collect();
    let mut capability: Option<capdag::Cap> = None;
    let kind = if word.ends_with(".machine") {
        f::WordKind::MachineFile
    } else if word.starts_with('[') {
        f::WordKind::InlineMachine
    } else if std::path::Path::new(&word).is_file() {
        f::WordKind::File
    } else if nearest(&word, commands.iter().map(String::as_str)).is_some() {
        f::WordKind::NearCommand
    } else {
        // Whether it names a capability is the registry's to say: when the
        // registry cannot be asked, that is said, not "no capability".
        let registry = ctx.registry().await?;
        match registry.get_cap(&word).await {
            Ok(cap) => {
                capability = Some(cap);
                f::WordKind::Capability
            }
            Err(e) => {
                ctx::nothing_by_that_name(e)?;
                f::WordKind::Unknown
            }
        }
    };
    let verdict = f::cli(ctx_free_world(), f::Invocation::Unknown { kind: kind.clone() });
    let mut error = ends(&verdict).expect("a word that is no command is refused");
    error.problem = format!("'{word}': {}", error.problem);
    let error = match kind {
        f::WordKind::MachineFile => error.hint(format!("capdag run {}{rest}", shell_word(&word))),
        f::WordKind::InlineMachine => error.hint(format!("capdag run {}{rest}", shell_word(&word))),
        f::WordKind::File => {
            error.hint(format!("what it can become: capdag run {} --dry-run", shell_word(&word)))
        }
        f::WordKind::NearCommand => {
            let near = nearest(&word, commands.iter().map(String::as_str)).expect("a near command was found above");
            error.hint(format!("did you mean `capdag {near}`?"))
        }
        f::WordKind::Capability => {
            let cap = capability.expect("a capability was found above");
            let registry = ctx.registry().await?;
            let alias = names::cap(&registry, &cap);
            let machine = names::one_step_machine(&registry, &cap)
                .map_err(|e| CliError::failed(format!("the machine of {alias}'s one step could not be written")).context(e))?;
            error
                .context(format!("{alias} — {}", cap.title))
                .hint(format!("capdag run {}{}", shell_word(&machine), if rest.is_empty() { " <input>".to_string() } else { rest }))
        }
        f::WordKind::Unknown => error
            .context(format!("the commands are: {}", commands.join(", ")))
            .hint(format!("look for a capability with: capdag caps {}", shell_word(&word))),
    };
    Err(error)
}

/// The world the model decides a line on that reads nothing of it: help, the
/// version, a line with no command, one the parser could not read, and a
/// first word that is no command.
fn ctx_free_world() -> f::CliWorld {
    f::CliWorld {
        terminal: f::Terminal { reachable: false, no_input: true, json: false, ci: false, dumb: false },
        screen: false,
        fabric_cached: true,
    }
}

/// The command closest to `word`, when one is close enough to be what was meant.
pub fn nearest<'a>(word: &str, candidates: impl Iterator<Item = &'a str>) -> Option<&'a str> {
    candidates
        .map(|candidate| (strsim::jaro_winkler(word, candidate), candidate))
        .filter(|(score, _)| *score > 0.8)
        .max_by(|a, b| a.0.total_cmp(&b.0))
        .map(|(_, candidate)| candidate)
}

/// A word as a shell reads it back: quoted when it holds anything a shell
/// would read otherwise.
pub fn shell_word(word: &str) -> String {
    if word.is_empty() || word.chars().any(|c| c.is_whitespace() || "'\"$`\\|&;<>()*?[]{}#~!".contains(c)) {
        format!("'{}'", word.replace('\'', "'\\''"))
    } else {
        word.to_string()
    }
}

/// Words as a shell reads them back, on one line.
pub fn shell_line(words: &[String]) -> String {
    words.iter().map(|w| shell_word(w)).collect::<Vec<_>>().join(" ")
}

async fn cache(ctx: &Ctx, command: CacheCommand) -> CliResult<Exit> {
    let decided = match command {
        CacheCommand::Refresh => f::Command::CacheRefresh,
        CacheCommand::Clear => f::Command::CacheClear,
    };
    if let Some(error) = ends(&ctx.decide(decided, false)) {
        return Err(error);
    }
    // Cleared without opening a registry on it: a cache holding a corrupt
    // entry is one a registry refuses to open, and clearing is what that
    // refusal says to do.
    let dir = ctx
        .host
        .clear_fabric_cache()
        .await
        .map_err(|e| {
            CliError::failed(format!("the fabric cache of {} could not be cleared", ctx.host.fabric_anchor()))
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
/// release's manifest chains to this build's roots ([`capdag::self_update`]).
async fn self_update(ctx: &Ctx) -> CliResult<Exit> {
    if let Some(error) = ends(&ctx.decide(f::Command::SelfUpdate, false)) {
        return Err(error);
    }
    let updated = ctx
        .host
        .self_update(ctx::VERSION)
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
    use std::collections::BTreeSet;

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
        let commands = ["caps", "info", "run", "graph", "view", "edit", "cartridge"];
        assert_eq!(nearest("cpas", commands.iter().copied()), Some("caps"));
        assert_eq!(nearest("grpah", commands.iter().copied()), Some("graph"));
        assert_eq!(nearest("summarize-en", commands.iter().copied()), None);
    }

    /// Every command the argument definitions have, by the words that name
    /// it, with the long flags it takes beside the global ones; and whether
    /// it is hidden.
    fn defined() -> (BTreeSet<(Vec<String>, BTreeSet<String>)>, BTreeSet<Vec<String>>, BTreeSet<String>, BTreeSet<(String, String)>, Vec<(String, Vec<String>)>) {
        let mut command = Cli::command();
        command.build();
        let globals: BTreeSet<String> = command
            .get_arguments()
            .filter(|a| a.is_global_set())
            .filter_map(|a| a.get_long().map(|l| format!("--{l}")))
            .chain(["--help".to_string(), "--version".to_string()])
            .collect();
        let mut commands = BTreeSet::new();
        let mut hidden = BTreeSet::new();
        let mut shorts = BTreeSet::new();
        let mut aliases = Vec::new();
        fn walk(
            at: &clap::Command,
            path: Vec<String>,
            globals: &BTreeSet<String>,
            commands: &mut BTreeSet<(Vec<String>, BTreeSet<String>)>,
            hidden: &mut BTreeSet<Vec<String>>,
            shorts: &mut BTreeSet<(String, String)>,
            aliases: &mut Vec<(String, Vec<String>)>,
        ) {
            for arg in at.get_arguments() {
                if let (Some(short), Some(long)) = (arg.get_short(), arg.get_long()) {
                    shorts.insert((format!("-{short}"), format!("--{long}")));
                }
            }
            for sub in at.get_subcommands() {
                let mut here = path.clone();
                here.push(sub.get_name().to_string());
                for alias in sub.get_visible_aliases() {
                    aliases.push((alias.to_string(), here.clone()));
                }
                if sub.is_hide_set() {
                    hidden.insert(here.clone());
                }
                if sub.has_subcommands() {
                    walk(sub, here, globals, commands, hidden, shorts, aliases);
                    continue;
                }
                for arg in sub.get_arguments() {
                    if let (Some(short), Some(long)) = (arg.get_short(), arg.get_long()) {
                        shorts.insert((format!("-{short}"), format!("--{long}")));
                    }
                }
                let flags: BTreeSet<String> = sub
                    .get_arguments()
                    .filter_map(|a| a.get_long().map(|l| format!("--{l}")))
                    .filter(|flag| !globals.contains(flag))
                    .collect();
                commands.insert((here, flags));
            }
        }
        walk(&command, Vec::new(), &globals, &mut commands, &mut hidden, &mut shorts, &mut aliases);
        (commands, hidden, globals, shorts, aliases)
    }

    // TEST20131: the argument definitions name exactly the commands and flags
    // the model's table names: every command by its words with the flags it
    // takes, the flags every command takes, the hidden commands, the aliases,
    // and the short forms. A command or flag the model does not decide is a
    // line the model never saw.
    #[test]
    fn test20131_the_argument_definitions_are_the_models_table() {
        let (commands, hidden, globals, shorts, aliases) = defined();
        let table: BTreeSet<(Vec<String>, BTreeSet<String>)> = f::command_table()
            .0
            .into_iter()
            .map(|(words, flags)| (words.0, flags.0.into_iter().collect()))
            .collect();
        assert_eq!(commands, table, "the commands and their flags");
        let model_hidden: BTreeSet<Vec<String>> = f::hidden_commands().0.into_iter().map(|words| words.0).collect();
        assert_eq!(hidden, model_hidden, "the hidden commands");
        let model_globals: BTreeSet<String> = f::global_flags().0.into_iter().collect();
        assert_eq!(globals, model_globals, "the flags every command takes");
        let model_shorts: BTreeSet<(String, String)> = f::short_flags().0.into_iter().collect();
        assert_eq!(shorts, model_shorts, "the short forms");
        let model_aliases: Vec<(String, Vec<String>)> =
            f::command_aliases().0.into_iter().map(|(alias, words)| (alias, words.0)).collect();
        assert_eq!(aliases, model_aliases, "the aliases");
    }

    // TEST20132: a line the parser hands over as no command ends as the model
    // says: help and the version 0, no command or a group without its command
    // 2, a line it could not read 2.
    #[test]
    fn test20132_lines_the_parser_decides_exit_as_the_model_says() {
        let status = |line: &str| {
            let argv: Vec<String> = line.split(' ').map(str::to_string).collect();
            let error = Cli::command().try_get_matches_from(&argv).expect_err("the parser does not hand it over");
            exit_without_world(invocation_of(&argv, error.kind()))
        };
        assert_eq!(status("capdag --help"), 0);
        assert_eq!(status("capdag run --version"), 0);
        assert_eq!(status("capdag caps --help"), 0);
        assert_eq!(status("capdag"), 2);
        assert_eq!(status("capdag cartridge"), 2);
        assert_eq!(status("capdag caps --bogus"), 2);
        assert_eq!(status("capdag -v -q caps"), 2);
    }
}
