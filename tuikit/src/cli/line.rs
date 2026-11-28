//! A `capdag run` or `capdag plan` line, as the model knows it, and the world
//! it is decided in.
//!
//! The model (`CapdagCli.Line`) names what a line gives by number: files,
//! live sources and machines by their place in the tables here, a machine's
//! inputs and step options by their place in it. This module reads a line
//! into those terms ([`Tables`], [`RunState`]), and builds the model's world
//! from what is known of the files, the fabric, the planner and the
//! cartridges.
//!
//! The world is built on demand. Detecting a file, resolving a machine,
//! planning, and asking the cartridge registry are done only when the model
//! asks for what they answer: each round, the model is asked with what is
//! known so far, every fact it asked for that is not known yet is recorded,
//! those are found, and it is asked again. A round in which it asked for
//! nothing unknown is its answer. So a line the model refuses before it looks
//! at a file never has a file detected, and one it settles without planning
//! is never planned.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::ctx::{self, Ctx};
use super::error::{CliError, CliResult};
use super::formal::{self as f, index, list, nat};
use super::inputs;
use super::inspect::Inspection;
use super::names;
use capdag::fabric::registry::FabricRegistry;
use capdag::orchestrator::machine_plan::{MachineAnchor, ResolvedNotation, RunInputs, StrandPlan};
use capdag::planner as p;
use capdag::{CapUrn, MediaUrn};

// =============================================================================
// What a line names
// =============================================================================

/// A file a line names: by position, or after `@` in `--input`, or typed as
/// an answer.
#[derive(Debug, Clone)]
pub struct FileEntry {
    pub path: PathBuf,
    /// Whether it is there to read.
    pub there: bool,
}

/// A machine a line runs or plans: written in a file, written inline, or
/// planned.
#[derive(Debug, Clone)]
pub enum MachineEntry {
    /// A `.machine` file.
    File(PathBuf),
    /// Notation written on the line, or a planned machine in alias form.
    Inline(String),
}

impl MachineEntry {
    /// How the machine is written on a line that runs it.
    pub fn argument(&self) -> String {
        match self {
            MachineEntry::File(path) => path.display().to_string(),
            MachineEntry::Inline(notation) => notation.clone(),
        }
    }

    /// How a person names it in a message.
    pub fn shown(&self) -> String {
        match self {
            MachineEntry::File(path) => path.display().to_string(),
            MachineEntry::Inline(_) => "the machine".to_string(),
        }
    }
}

/// Everything a line names, by the numbers the model knows it by. Entries
/// are only ever added: a number keeps what it names for the whole command.
#[derive(Debug, Clone, Default)]
pub struct Tables {
    pub files: Vec<FileEntry>,
    /// Live sources, as their URNs were written.
    pub lives: Vec<String>,
    pub machines: Vec<MachineEntry>,
}

impl Tables {
    pub fn add_file(&mut self, path: PathBuf, there: bool) -> usize {
        self.files.push(FileEntry { path, there });
        self.files.len() - 1
    }

    pub fn add_live(&mut self, urn: String) -> usize {
        self.lives.push(urn);
        self.lives.len() - 1
    }

    pub fn add_machine(&mut self, machine: MachineEntry) -> usize {
        self.machines.push(machine);
        self.machines.len() - 1
    }
}

/// The files a path given on a line names: a file, each content file of a
/// directory, or each file a glob matches, sorted. A path that names nothing
/// is one file that is not there, for the model to refuse by name.
pub fn files_of(tables: &mut Tables, given: &str) -> CliResult<Vec<usize>> {
    let path = PathBuf::from(given);
    if given.contains('*') || given.contains('?') || path.is_dir() {
        match inputs::expand(given) {
            Ok(files) => return Ok(files.into_iter().map(|file| tables.add_file(file, true)).collect()),
            Err(error) if error.problem.starts_with("no file matches") => {
                return Ok(vec![tables.add_file(path, false)]);
            }
            Err(error) => return Err(error),
        }
    }
    let there = path.is_file();
    Ok(vec![tables.add_file(path, there)])
}

/// Whether an argument is a live source: a media URN of a live family.
pub fn is_live_urn(token: &str) -> bool {
    token.starts_with("media:") && MediaUrn::from_string(token).is_ok_and(|urn| urn.is_live_feed())
}

/// What a positional argument is spelled as: all that decides whether it is
/// the machine.
pub fn spelling(token: &str) -> f::Spelling {
    if token.starts_with('[') {
        f::Spelling::Bracket
    } else if token.ends_with(".machine") {
        f::Spelling::MachineFile
    } else if token == "-" {
        f::Spelling::Dash
    } else if is_live_urn(token) {
        f::Spelling::LiveUrn
    } else {
        f::Spelling::Other
    }
}

/// Positional arguments, read: the machine when the first is spelled as one
/// and no `--` came before them, and every other as an input. A path is the
/// files it names, each an argument of its own, in the order given.
pub fn read_positionals(
    tables: &mut Tables,
    before: &[String],
    after: &[String],
) -> CliResult<(Option<f::Written>, Vec<f::Positional>)> {
    let separated = before.is_empty() && !after.is_empty();
    let mut args: Vec<f::Arg> = Vec::new();
    for (at, token) in before.iter().chain(after.iter()).enumerate() {
        let spelled = spelling(token);
        match spelled {
            f::Spelling::Bracket | f::Spelling::MachineFile if at == 0 && !separated => {
                let entry = if spelled == f::Spelling::Bracket {
                    MachineEntry::Inline(token.clone())
                } else {
                    MachineEntry::File(PathBuf::from(token))
                };
                args.push(f::Arg { spelling: spelled, names: nat(tables.add_machine(entry)) });
            }
            f::Spelling::Dash => args.push(f::Arg { spelling: spelled, names: nat(0) }),
            f::Spelling::LiveUrn => args.push(f::Arg { spelling: spelled, names: nat(tables.add_live(token.clone())) }),
            // A bracket or `.machine` anywhere else is an input: a path.
            _ => {
                for p in files_of(tables, token)? {
                    args.push(f::Arg { spelling: f::Spelling::Other, names: nat(p) });
                }
            }
        }
    }
    let (machine, positionals) = f::read_args(separated, list(args));
    Ok((machine, positionals.0))
}

// =============================================================================
// A machine, as the world knows it
// =============================================================================

/// A step option a machine leaves to the run: no constant in its notation,
/// no default, no per-cap setting.
#[derive(Debug, Clone)]
pub struct OpenOption {
    /// The step's name: the node its main output goes to, as the notation
    /// writes it.
    pub step: String,
    /// The option's name in its cap's definition.
    pub name: String,
    /// What it takes.
    pub media_urn: String,
    /// The values it can take, when its definition lists them.
    pub allowed: Option<Vec<String>>,
    /// The step's token in each strand's plan: where its value is given.
    pub strand: usize,
    pub token: String,
}

impl OpenOption {
    /// How `--arg` names it: by its name alone when no other step has an
    /// option of that name, and `<step>.<option>` otherwise.
    pub fn flag_name(&self, shared: bool) -> String {
        if shared {
            format!("{}.{}", self.step, self.name)
        } else {
            self.name.clone()
        }
    }
}

/// A machine that resolves: what it takes, gives and leaves to the run.
pub struct Resolved {
    pub resolution: ResolvedNotation,
    pub inputs: Vec<MachineAnchor>,
    pub outputs: Vec<MachineAnchor>,
    pub options: Vec<OpenOption>,
    /// Every cap its steps run.
    pub caps: Vec<CapUrn>,
    /// The dev cartridges its steps need beyond the fabric: their directories.
    pub dev_dirs: Vec<PathBuf>,
}

impl Resolved {
    /// Whether another option of the machine has the name option `j` has.
    pub fn shared(&self, j: usize) -> bool {
        let name = &self.options[j].name;
        self.options.iter().enumerate().any(|(k, other)| k != j && &other.name == name)
    }

    /// How `--arg` names option `j`.
    pub fn flag_name(&self, j: usize) -> String {
        self.options[j].flag_name(self.shared(j))
    }
}

/// What the world knows of one machine: it resolves, or why it does not.
pub enum MachineFacts {
    Resolves(Resolved),
    Unresolved(CliError),
}

impl MachineFacts {
    pub fn resolved(&self) -> Option<&Resolved> {
        match self {
            MachineFacts::Resolves(resolved) => Some(resolved),
            MachineFacts::Unresolved(_) => None,
        }
    }
}

/// What running a machine needs that is not on this machine: for each cap
/// no bundled, installed or dev cartridge provides, the registry's
/// cartridges that provide it.
#[derive(Debug, Clone, Default)]
pub struct Needs {
    pub missing: Vec<(CapUrn, Vec<String>)>,
}

impl Needs {
    pub fn needs_cartridge(&self) -> bool {
        !self.missing.is_empty()
    }

    /// The ways the registry leaves: none when a cap has no provider, several
    /// when one has several, else one.
    pub fn providers(&self) -> f::Choices {
        if self.missing.iter().any(|(_, providers)| providers.is_empty()) {
            f::Choices::None
        } else if self.missing.iter().any(|(_, providers)| providers.len() > 1) {
            f::Choices::Many
        } else {
            f::Choices::One
        }
    }
}

/// What planning found for the line's inputs.
pub struct PlanFacts {
    /// What the inputs can become, when no `--to` says.
    pub targets: Vec<p::ConvergentTargetInfo>,
    /// The machines that make what `--to` names, nearest first: each as the
    /// number of its entry in the tables.
    pub machines: Vec<usize>,
    /// How each machine is shown.
    pub described: Vec<Described>,
    /// Ways the planner found and could not write as machines.
    pub excluded: Vec<p::ExcludedCandidate>,
    /// What the inputs are, said for a person: `2 pdf and 1 txt`.
    pub what: String,
}

/// A planned machine, as it is listed.
#[derive(Debug, Clone)]
pub struct Described {
    pub notation: String,
    pub canonical: String,
    pub steps: usize,
    pub model: bool,
    pub downloads: bool,
}

impl Described {
    pub fn summary(&self) -> String {
        let mut parts = vec![format!("{} step{}", self.steps, if self.steps == 1 { "" } else { "s" })];
        if self.model {
            parts.push("runs a model".into());
        }
        if self.downloads {
            parts.push("needs a cartridge download".into());
        }
        parts.join(" · ")
    }
}

/// The planning flags, as `run` and `plan` take them.
#[derive(Debug, Clone, Default, clap::Args)]
pub struct PlanningFlags {
    /// Where inputs of several types meet: combine them, or treat each on its own
    #[arg(long, value_name = "HOW", value_parser = ["auto", "combine", "independent"])]
    pub converge: Option<String>,
    /// Where converging paths meet: auto, earliest, latest, source, target or depth=<n>
    #[arg(long = "where", value_name = "WHERE")]
    pub location: Option<String>,
    /// How converging paths are joined
    #[arg(long, value_name = "HOW", value_parser = ["any", "generalize", "collect", "merge"])]
    pub mechanism: Option<String>,
    /// The order machines are listed in
    #[arg(long, value_name = "ORDER", value_parser = ["intent", "shortest", "cost"])]
    pub rank: Option<String>,
    /// How many steps a machine may have at most
    #[arg(long, value_name = "N")]
    pub depth: Option<usize>,
    /// How many paths are searched at most
    #[arg(long, value_name = "N")]
    pub max_paths: Option<usize>,
}

impl PlanningFlags {
    /// Whether any was given.
    pub fn given(&self) -> bool {
        self.converge.is_some()
            || self.location.is_some()
            || self.mechanism.is_some()
            || self.rank.is_some()
            || self.depth.is_some()
            || self.max_paths.is_some()
    }

    /// The planner's request for `sources` to make `targets`.
    pub fn request(&self, sources: Vec<p::SourceSpec>, targets: Vec<MediaUrn>) -> CliResult<p::PlanRequest> {
        let converge = self.converge.as_deref().unwrap_or("auto");
        let location = self.location.as_deref().unwrap_or("auto");
        let mechanism = self.mechanism.as_deref().unwrap_or("any");
        let presence = match converge {
            "combine" => p::ConvergencePresence::Converged,
            "independent" => p::ConvergencePresence::Independent,
            _ => p::ConvergencePresence::Auto,
        };
        let where_ = match location {
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
        let how = match mechanism {
            "generalize" => p::ConvergenceMechanism::Generalize,
            "collect" => p::ConvergenceMechanism::Collect,
            "merge" => p::ConvergenceMechanism::Merge,
            _ => p::ConvergenceMechanism::Any,
        };
        let ranking = match self.rank.as_deref().unwrap_or("intent") {
            "shortest" => p::RankPolicy::Shortest,
            "cost" => p::RankPolicy::Cost,
            _ => p::RankPolicy::Intent,
        };
        let configured = converge != "auto" || location != "auto" || mechanism != "any";
        Ok(p::PlanRequest {
            sources,
            targets: p::TargetSpec::Exact(targets),
            convergence: p::ConvergencePolicy {
                presence,
                location: where_,
                mechanism: how,
                at_type: None,
                arity: p::ConvergenceArity::Auto,
            },
            divergence: p::DivergencePolicy::default(),
            ranking,
            search: p::SearchDirection::Auto,
            mode: if configured { p::PlanMode::Configured } else { p::PlanMode::Auto },
            max_depth: self.depth.unwrap_or(p::PlanRequest::DEFAULT_MAX_DEPTH),
            max_paths: self.max_paths.unwrap_or(p::PlanRequest::DEFAULT_MAX_PATHS),
            max_candidates: p::PlanRequest::DEFAULT_MAX_CANDIDATES,
        })
    }
}

/// A type a line names (`--to`, `--from`, `--in`, `--out`): a media alias, a
/// media URN, or an extension a media type of the fabric has. `None` when it
/// names none.
pub async fn media_type(registry: &FabricRegistry, token: &str) -> CliResult<Option<MediaUrn>> {
    if token.contains(':') {
        return Ok(MediaUrn::from_string(token).ok());
    }
    match registry
        .resolve_alias_typed(token, Some(capdag::fabric::alias::AliasTargetKind::Media))
        .await
    {
        Ok(target) => return Ok(MediaUrn::from_string(&target).ok()),
        Err(e) => ctx::nothing_by_that_name(e)?,
    }
    let defined = registry.file_media_for_extension(token).await.map_err(ctx::registry_failure)?;
    if defined.is_empty() {
        return Ok(None);
    }
    Ok(MediaUrn::from_string(&format!("media:ext={token}")).ok())
}

/// The inputs a value fits most closely, by the signed specificity distance
/// between what it is and what each takes. A value — a detected file, a live
/// source's content — is a complete thing: what it does not mention it does
/// not have, so it fits an input it satisfies, and may fit one that says more
/// than it writes (a distance below zero). Several are returned only when
/// they tie.
pub fn nearest_inputs<'a, T>(media: &MediaUrn, inputs: impl IntoIterator<Item = (T, &'a MediaUrn)>) -> Vec<T> {
    let mut best: Vec<T> = Vec::new();
    let mut nearest: Option<i64> = None;
    for (input, takes) in inputs {
        if !media.satisfies(takes) {
            continue;
        }
        let distance = media.specificity() as i64 - takes.specificity() as i64;
        match nearest {
            Some(closest) if distance > closest => {}
            Some(closest) if distance == closest => best.push(input),
            _ => {
                nearest = Some(distance);
                best = vec![input];
            }
        }
    }
    best
}

// =============================================================================
// The world, built on demand
// =============================================================================

/// A fact the model asked for that is not known yet.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Want {
    /// Resolve machine `m`.
    Machine(usize),
    /// Detect what file `p` is.
    Detect(usize),
    /// What live source `r` gives.
    Live(usize),
    /// What running machine `m` needs.
    Needs(usize),
    /// Plan the line's inputs.
    Plan,
}

/// What a line's planning is keyed by: what it plans to and from.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PlanKey {
    to: Vec<String>,
    from: Option<String>,
    inputs: Vec<String>,
}

/// What is known so far.
#[derive(Default)]
struct Known {
    machines: HashMap<usize, Arc<MachineFacts>>,
    media: HashMap<usize, MediaUrn>,
    lives: HashMap<usize, Option<MediaUrn>>,
    needs: HashMap<usize, Needs>,
    plans: HashMap<PlanKey, Arc<PlanFacts>>,
}

/// What a line says beyond the model's terms: the values the model does not
/// carry, and what decides nothing but is used.
#[derive(Debug, Clone, Default)]
pub struct Extras {
    /// `--to`, as written.
    pub to: Vec<String>,
    /// `--from`, as written.
    pub from: Option<String>,
    /// `--selector`, as written.
    pub selector: Option<String>,
    /// Whether `-o` was given, and where it points.
    pub output: Option<PathBuf>,
}

/// A `run` line, as it stands: the model's line, and the values it does not
/// carry. `texts` holds the text of each `named` entry that is text, in
/// step with `named`; `arg_values` the value of each `--arg`, in step with
/// `args`.
#[derive(Debug, Clone)]
pub struct RunState {
    pub line: f::RunLine,
    pub texts: Vec<Option<String>>,
    pub arg_values: Vec<String>,
    /// Names `--input` and `--live` gave that are no input of the machine:
    /// `named` places past the machine's inputs, in order.
    pub unknown_inputs: Vec<String>,
    /// Names `--arg` gave that are no option the machine leaves to the run.
    pub unknown_options: Vec<String>,
}

/// One `capdag run` or `capdag plan` command's view of the world.
pub struct World<'a> {
    pub ctx: &'a Ctx,
    pub registry: Arc<FabricRegistry>,
    pub inspection: Inspection<'a>,
    pub tables: Tables,
    pub extras: Extras,
    pub planning: PlanningFlags,
    /// Whether every `--to` and `--from` names a media type.
    pub types_known: bool,
    /// Where `-o` points.
    pub output: f::OutputDir,
    /// What each live source's URN names, whether the fabric defines it.
    live_defined: Vec<bool>,
    /// `--dev-bin`: the cartridge binaries to use.
    dev_bins: Vec<PathBuf>,
    /// What each `--dev-bin` binary provides, once it was asked: it is
    /// started for it only when what a run needs is asked.
    dev_caps: Option<Vec<CapUrn>>,
    /// What `--from` names.
    from_media: Option<MediaUrn>,
    known: Known,
}

impl<'a> World<'a> {
    pub fn new(
        ctx: &'a Ctx,
        registry: Arc<FabricRegistry>,
        inspection: Inspection<'a>,
        tables: Tables,
        extras: Extras,
        planning: PlanningFlags,
    ) -> Self {
        World {
            ctx,
            registry,
            inspection,
            tables,
            extras,
            planning,
            types_known: true,
            output: f::OutputDir::Ok,
            live_defined: Vec::new(),
            dev_bins: Vec::new(),
            dev_caps: None,
            from_media: None,
            known: Known::default(),
        }
    }

    /// What the line names that is cheap to know: whether its types are
    /// types, where `-o` points, which live sources the fabric defines, and
    /// what the `--dev-bin` binaries provide.
    pub async fn learn_line(&mut self, dev_bins: &[PathBuf]) -> CliResult<()> {
        self.learn_types().await?;
        self.output = match &self.extras.output {
            Some(dir) => output_dir(dir),
            None => f::OutputDir::Ok,
        };
        self.learn_lives().await?;
        self.dev_bins.extend(inputs::dev_binaries(dev_bins)?);
        Ok(())
    }

    /// What the `--dev-bin` binaries provide: each is started once and asked.
    fn dev_caps(&mut self) -> CliResult<Vec<CapUrn>> {
        if let Some(caps) = &self.dev_caps {
            return Ok(caps.clone());
        }
        let mut caps = Vec::new();
        for binary in &self.dev_bins {
            let manifest = self.ctx.host.dev_entry_manifest(binary).map_err(|e| {
                CliError::failed(format!("--dev-bin {}: what it provides could not be read", binary.display()))
                    .context(e.to_string())
            })?;
            caps.extend(manifest.cap_groups.iter().flat_map(|g| g.caps.iter().map(|c| c.urn.clone())));
        }
        self.dev_caps = Some(caps.clone());
        Ok(caps)
    }

    /// Whether every `--to` and `--from` names a media type, as they stand.
    pub async fn learn_types(&mut self) -> CliResult<()> {
        let mut known = true;
        for token in &self.extras.to {
            known &= media_type(&self.registry, token).await?.is_some();
        }
        if let Some(from) = &self.extras.from {
            self.from_media = media_type(&self.registry, from).await?;
            known &= self.from_media.is_some();
        }
        self.types_known = known;
        Ok(())
    }

    /// Which live sources, added since the last time, the fabric defines.
    pub async fn learn_lives(&mut self) -> CliResult<()> {
        while self.live_defined.len() < self.tables.lives.len() {
            let r = self.live_defined.len();
            let defined = match MediaUrn::from_string(&self.tables.lives[r]) {
                Ok(urn) => self
                    .registry
                    .live_source_content_urn(&urn)
                    .await
                    .map_err(|e| {
                        CliError::failed(format!("the live source '{}' could not be looked up", self.tables.lives[r]))
                            .context(e.to_string())
                    })?
                    .is_some(),
                Err(_) => false,
            };
            self.live_defined.push(defined);
        }
        Ok(())
    }

    /// The dev binaries the run is given: `--dev-bin`, and the dev
    /// cartridges of the machines it resolved.
    pub fn dev_binaries(&self, machine: Option<usize>) -> Vec<PathBuf> {
        let mut binaries: Vec<PathBuf> = self.dev_bins.clone();
        if let Some(m) = machine {
            if let Some(resolved) = self.machine(m).and_then(MachineFacts::resolved) {
                binaries.extend(resolved.dev_dirs.iter().cloned());
            }
        }
        binaries
    }

    /// What is known of machine `m`, once it is.
    pub fn machine(&self, m: usize) -> Option<&MachineFacts> {
        self.known.machines.get(&m).map(Arc::as_ref)
    }

    /// Resolve machine `m` now, when it is not known yet.
    pub async fn resolve_machine(&mut self, m: usize) -> CliResult<()> {
        if self.known.machines.contains_key(&m) {
            return Ok(());
        }
        let facts = resolve_entry(self.ctx, &self.registry, &self.tables.machines[m]).await?;
        self.known.machines.insert(m, Arc::new(facts));
        Ok(())
    }

    /// Know machine `m` as it was resolved already.
    pub fn know_machine(&mut self, m: usize, facts: MachineFacts) {
        self.known.machines.insert(m, Arc::new(facts));
    }

    /// What file `p` is, once detected.
    pub fn media_of(&self, p: usize) -> Option<&MediaUrn> {
        self.known.media.get(&p)
    }

    /// What live source `r` gives, once known.
    pub fn live_media(&self, r: usize) -> Option<&MediaUrn> {
        self.known.lives.get(&r).and_then(Option::as_ref)
    }

    /// What running machine `m` needs, once known.
    pub fn needs(&self, m: usize) -> Option<&Needs> {
        self.known.needs.get(&m)
    }

    /// Forget what running machine `m` needs: a cartridge was installed.
    pub fn forget_needs(&mut self, m: usize) {
        self.known.needs.remove(&m);
    }

    fn plan_key(&self, inputs: &[f::Positional]) -> PlanKey {
        PlanKey {
            to: self.extras.to.clone(),
            from: self.extras.from.clone(),
            inputs: inputs.iter().map(|x| format!("{x:?}")).collect(),
        }
    }

    /// What planning found for `inputs`, once it is planned.
    pub fn plan(&self, inputs: &[f::Positional]) -> Option<&Arc<PlanFacts>> {
        self.known.plans.get(&self.plan_key(inputs))
    }

    /// Ask the model `ask` of the world as it is known, finding every fact it
    /// asks for that is not known, until it asks for nothing new. `inputs`
    /// is what the line gives by position, which planning plans from.
    ///
    /// What the planner finds is two fields of the model's world, not
    /// functions it calls, so asking for them cannot be recorded. Until the
    /// line is planned, the model is asked with every value they can take:
    /// an answer that is the same for all of them does not depend on what
    /// planning would find, and the line is not planned.
    pub async fn settle<T: PartialEq>(&mut self, inputs: &[f::Positional], ask: impl Fn(f::World) -> T) -> CliResult<T> {
        loop {
            let round: Arc<Mutex<Round>> = Arc::new(Mutex::new(Round::default()));
            let answer = if self.plan(inputs).is_some() {
                ask(self.model_world(inputs, &round, None))
            } else {
                let mut answers: Vec<T> = Vec::new();
                for targets in [f::Choices::None, f::Choices::Many] {
                    for machines in [f::Choices::None, f::Choices::One, f::Choices::Many] {
                        answers.push(ask(self.model_world(inputs, &round, Some((targets.clone(), machines)))));
                    }
                }
                let first = answers.swap_remove(0);
                if answers.iter().any(|other| other != &first) {
                    round.lock().expect("the round is not poisoned").wants.push(Want::Plan);
                }
                first
            };
            let Round { mut wants, faults } = std::mem::take(&mut *round.lock().expect("the round is not poisoned"));
            if let Some(fault) = faults.into_iter().next() {
                return Err(fault);
            }
            let mut seen = HashSet::new();
            wants.retain(|want| seen.insert(want.clone()));
            if wants.is_empty() {
                return Ok(answer);
            }
            for want in wants {
                self.find(want, inputs).await?;
            }
        }
    }

    async fn find(&mut self, want: Want, inputs: &[f::Positional]) -> CliResult<()> {
        match want {
            Want::Machine(m) => self.resolve_machine(m).await,
            Want::Detect(p) => {
                let media = inputs::detect(&self.tables.files[p].path, &self.inspection).await?;
                self.known.media.insert(p, media);
                Ok(())
            }
            Want::Live(r) => {
                let content = match MediaUrn::from_string(&self.tables.lives[r]) {
                    Ok(urn) => match self.registry.live_source_content_urn(&urn).await.map_err(|e| {
                        CliError::failed(format!("the live source '{}' could not be looked up", self.tables.lives[r]))
                            .context(e.to_string())
                    })? {
                        Some(content) => Some(MediaUrn::from_string(&content).map_err(|e| {
                            CliError::failed(format!("the fabric's content for '{}' is no media URN", self.tables.lives[r]))
                                .context(e.to_string())
                        })?),
                        None => None,
                    },
                    Err(_) => None,
                };
                self.known.lives.insert(r, content);
                Ok(())
            }
            Want::Needs(m) => {
                let needs = self.find_needs(m).await?;
                self.known.needs.insert(m, needs);
                Ok(())
            }
            Want::Plan => {
                let facts = self.find_plan(inputs).await?;
                let key = self.plan_key(inputs);
                self.known.plans.insert(key, Arc::new(facts));
                Ok(())
            }
        }
    }

    /// What running machine `m` needs: each cap of it no bundled cartridge,
    /// `--dev-bin` binary, dev cartridge or installed cartridge provides, and
    /// the registry's cartridges that do. Without the network
    /// (`--offline`), no registry is asked: a cartridge installed from one is
    /// verified against it before it runs, so it cannot run offline either.
    async fn find_needs(&mut self, m: usize) -> CliResult<Needs> {
        self.resolve_machine(m).await?;
        let Some(resolved) = self.machine(m).and_then(MachineFacts::resolved) else {
            return Ok(Needs::default());
        };
        let caps = resolved.caps.clone();
        let dev_dirs = resolved.dev_dirs.clone();
        let mut here: Vec<CapUrn> = super::cartridge::bundled_caps(self.ctx).await?;
        here.extend(self.dev_caps()?);
        for found in super::cartridge::installed(self.ctx).await? {
            if let capdag::cartridge_discovery::DiscoveredCartridge::Directory {
                registry_url, cap_groups, version_dir, ..
            } = found
            {
                let dev = registry_url.is_none();
                if dev && !dev_dirs.iter().any(|dir| version_dir.starts_with(dir) || dir.starts_with(&version_dir)) {
                    continue;
                }
                if !dev && self.ctx.offline {
                    continue;
                }
                here.extend(cap_groups.iter().flat_map(|g| g.caps.iter().map(|c| c.urn.clone())));
            }
        }
        let missing_caps: Vec<CapUrn> =
            caps.into_iter().filter(|cap| !here.iter().any(|provided| provided.is_equivalent(cap))).collect();
        if missing_caps.is_empty() || self.ctx.offline {
            return Ok(Needs { missing: missing_caps.into_iter().map(|cap| (cap, Vec::new())).collect() });
        }
        if self.ctx.host.cartridge_registry().is_none() {
            return Ok(Needs { missing: missing_caps.into_iter().map(|cap| (cap, Vec::new())).collect() });
        }
        let store = super::cartridge::manager(self.ctx).await?;
        let mut missing = Vec::with_capacity(missing_caps.len());
        for cap in missing_caps {
            let providers = store.providers_of(&cap.to_string()).await;
            missing.push((cap, providers));
        }
        Ok(Needs { missing })
    }

    /// The sources planning plans from: files of one type are one sequence;
    /// `-` holds what `--from` says; a live source gives what the fabric says.
    async fn sources(&mut self, inputs: &[f::Positional]) -> CliResult<(Vec<p::SourceSpec>, String)> {
        let mut groups: Vec<(MediaUrn, usize, bool)> = Vec::new();
        for x in inputs {
            let (media, sequence) = match x {
                f::Positional::Path { p } => {
                    let p = index(p);
                    if !self.known.media.contains_key(&p) {
                        let media = inputs::detect(&self.tables.files[p].path, &self.inspection).await?;
                        self.known.media.insert(p, media);
                    }
                    (self.known.media[&p].clone(), false)
                }
                f::Positional::Stdin => match &self.from_media {
                    Some(media) => (media.clone(), false),
                    None => {
                        return Err(CliError::failed("planning from - was reached with no --from")
                            .hint("report it: what - holds is settled before anything is planned"))
                    }
                },
                f::Positional::Live { r } => {
                    let r = index(r);
                    if !self.known.lives.contains_key(&r) {
                        Box::pin(self.find(Want::Live(r), inputs)).await?;
                    }
                    let media = self.live_media(r).cloned().expect("a live source the fabric defines has content");
                    (media, true)
                }
            };
            match groups.iter_mut().find(|(m, _, _)| m.is_equivalent(&media)) {
                Some((_, count, _)) => *count += 1,
                None => groups.push((media, 1, sequence)),
            }
        }
        let what = groups
            .iter()
            .map(|(media, count, _)| format!("{count} {}", names::media(&self.registry, media)))
            .collect::<Vec<_>>()
            .join(" and ");
        let sources = groups
            .into_iter()
            .map(|(media, count, sequence)| {
                if count > 1 || sequence {
                    p::SourceSpec::sequence(media)
                } else {
                    p::SourceSpec::single(media)
                }
            })
            .collect();
        Ok((sources, what))
    }

    async fn find_plan(&mut self, inputs: &[f::Positional]) -> CliResult<PlanFacts> {
        let (sources, what) = self.sources(inputs).await?;
        let fab = p::LiveCapFab::from_fabric(&self.registry).await.map_err(|e| self.ctx.caps_unreadable(e))?;
        if self.extras.to.is_empty() {
            let max_depth = self.planning.depth.unwrap_or(p::PlanRequest::DEFAULT_MAX_DEPTH);
            let discovered = fab.discover_convergent_targets(&sources, max_depth).map_err(|e| {
                CliError::failed(format!("what {what} can become could not be worked out")).context(e.to_string())
            })?;
            return Ok(PlanFacts {
                targets: discovered.targets,
                machines: Vec::new(),
                described: Vec::new(),
                excluded: Vec::new(),
                what,
            });
        }
        let mut targets = Vec::new();
        for token in &self.extras.to {
            targets.push(media_type(&self.registry, token).await?.expect("the model refuses a --to that is no type"));
        }
        let request = self.planning.request(sources, targets)?;
        let outcome = fab
            .plan(&request, self.registry.as_ref())
            .map_err(|e| CliError::failed("no machine could be planned").context(e.to_string()))?;
        let bundled = super::cartridge::bundled_caps(self.ctx).await?;
        let mut machines = Vec::with_capacity(outcome.candidates.len());
        let mut described = Vec::with_capacity(outcome.candidates.len());
        for candidate in &outcome.candidates {
            let shown = describe(&self.registry, &bundled, candidate).await?;
            let m = self.tables.add_machine(MachineEntry::Inline(shown.notation.replace('\n', "")));
            machines.push(m);
            described.push(shown);
        }
        Ok(PlanFacts { targets: Vec::new(), machines, described, excluded: outcome.excluded, what })
    }

    /// The model's world, as it is known: what is not known yet is recorded
    /// in `round` and answered with a stand-in, and the round's answer is not
    /// used. `stand_in` is what planning's two fields are taken to be while
    /// the line is not planned.
    fn model_world(
        &self,
        inputs: &[f::Positional],
        round: &Arc<Mutex<Round>>,
        stand_in: Option<(f::Choices, f::Choices)>,
    ) -> f::World {
        let plan = self.plan(inputs).cloned();
        let (targets, machines) = match (&plan, stand_in) {
            (Some(plan), _) => (
                if plan.targets.is_empty() { f::Choices::None } else { f::Choices::Many },
                match plan.machines.len() {
                    0 => f::Choices::None,
                    1 => f::Choices::One,
                    _ => f::Choices::Many,
                },
            ),
            (None, Some(stand_in)) => stand_in,
            (None, None) => unreachable!("a line not planned is asked with stand-ins"),
        };
        let s = Arc::new(Snapshot {
            machine_count: self.tables.machines.len(),
            files: self.tables.files.iter().map(|file| file.there).collect(),
            machine_files: self
                .tables
                .machines
                .iter()
                .map(|machine| matches!(machine, MachineEntry::File(path) if path.is_file()))
                .collect(),
            live_defined: self.live_defined.clone(),
            machines: self.known.machines.clone(),
            media: self.known.media.clone(),
            lives: self.known.lives.clone(),
            needs: self.known.needs.clone(),
            plan,
            from_media: self.from_media.clone(),
            round: round.clone(),
        });
        let c = s.clone();
        let file_there =
            lungo::LeanClosure::from_fn(move |p: lungo::Nat| c.files.get(index(&p)).copied().unwrap_or(false));
        let c = s.clone();
        let machine_file =
            lungo::LeanClosure::from_fn(move |m: lungo::Nat| c.machine_files.get(index(&m)).copied().unwrap_or(false));
        let c = s.clone();
        let live_there =
            lungo::LeanClosure::from_fn(move |r: lungo::Nat| c.live_defined.get(index(&r)).copied().unwrap_or(false));
        let c = s.clone();
        let resolves = lungo::LeanClosure::from_fn(move |m: lungo::Nat| match c.machine(index(&m)) {
            Some(facts) => facts.resolved().is_some(),
            None => true,
        });
        let c = s.clone();
        let inputs_of =
            lungo::LeanClosure::from_fn(move |m: lungo::Nat| nat(c.resolved(index(&m)).map(|r| r.inputs.len()).unwrap_or(0)));
        let c = s.clone();
        let fit = lungo::LeanClosure::from_fn(move |m: lungo::Nat, p: lungo::Nat| {
            list(c.fit(index(&m), index(&p)).into_iter().map(nat))
        });
        let c = s.clone();
        let live_fit = lungo::LeanClosure::from_fn(move |m: lungo::Nat, r: lungo::Nat| {
            list(c.live_fit(index(&m), index(&r)).into_iter().map(nat))
        });
        let c = s.clone();
        let options =
            lungo::LeanClosure::from_fn(move |m: lungo::Nat| nat(c.resolved(index(&m)).map(|r| r.options.len()).unwrap_or(0)));
        let c = s.clone();
        let option_shared = lungo::LeanClosure::from_fn(move |m: lungo::Nat, j: lungo::Nat| {
            let j = index(&j);
            c.resolved(index(&m)).is_some_and(|r| j < r.options.len() && r.shared(j))
        });
        let c = s.clone();
        let option_choices = lungo::LeanClosure::from_fn(move |m: lungo::Nat, j: lungo::Nat| {
            match c.resolved(index(&m)).and_then(|r| r.options.get(index(&j))) {
                Some(OpenOption { allowed: Some(values), .. }) => match values.len() {
                    0 => f::Choices::None,
                    1 => f::Choices::One,
                    _ => f::Choices::Many,
                },
                _ => f::Choices::Free,
            }
        });
        let c = s.clone();
        let needs_cartridge =
            lungo::LeanClosure::from_fn(move |m: lungo::Nat| c.needs(index(&m)).is_some_and(Needs::needs_cartridge));
        let c = s.clone();
        let providers = lungo::LeanClosure::from_fn(move |m: lungo::Nat| {
            c.needs(index(&m)).map(Needs::providers).unwrap_or(f::Choices::One)
        });
        let c = s.clone();
        let planned = lungo::LeanClosure::from_fn(move |k: lungo::Nat| nat(c.planned(index(&k))));
        let c = s.clone();
        let planned_input =
            lungo::LeanClosure::from_fn(move |k: lungo::Nat, x: f::Positional| nat(c.planned_input(index(&k), &x)));
        f::World {
            terminal: self.ctx.term.model(),
            file_there,
            machine_file,
            live_there,
            types_known: self.types_known,
            output: self.output.clone(),
            resolves,
            targets,
            machines,
            planned,
            planned_input,
            inputs: inputs_of,
            fit,
            live_fit,
            options,
            option_shared,
            option_choices,
            needs_cartridge,
            providers,
        }
    }
}

/// One round of asking the model: the facts it asked for that are not known,
/// and what went wrong finding what it asked.
#[derive(Default)]
struct Round {
    wants: Vec<Want>,
    faults: Vec<CliError>,
}

/// What is known, as one round of asking the model reads it.
struct Snapshot {
    /// How many machines the tables hold: a place past them names none.
    machine_count: usize,
    files: Vec<bool>,
    machine_files: Vec<bool>,
    live_defined: Vec<bool>,
    machines: HashMap<usize, Arc<MachineFacts>>,
    media: HashMap<usize, MediaUrn>,
    lives: HashMap<usize, Option<MediaUrn>>,
    needs: HashMap<usize, Needs>,
    plan: Option<Arc<PlanFacts>>,
    from_media: Option<MediaUrn>,
    round: Arc<Mutex<Round>>,
}

/// A place no table holds: what the model is given for a place asked of a
/// world that does not know it yet, so that it names nothing.
const NOWHERE: usize = usize::MAX >> 1;

impl Snapshot {
    fn want(&self, want: Want) {
        self.round.lock().expect("the round is not poisoned").wants.push(want);
    }

    fn fault(&self, fault: CliError) {
        self.round.lock().expect("the round is not poisoned").faults.push(fault);
    }

    fn machine(&self, m: usize) -> Option<&MachineFacts> {
        if m >= self.machine_count {
            return None;
        }
        match self.machines.get(&m) {
            Some(facts) => Some(facts),
            None => {
                self.want(Want::Machine(m));
                None
            }
        }
    }

    fn resolved(&self, m: usize) -> Option<&Resolved> {
        self.machine(m).and_then(MachineFacts::resolved)
    }

    fn needs(&self, m: usize) -> Option<&Needs> {
        if m >= self.machine_count {
            return None;
        }
        match self.needs.get(&m) {
            Some(needs) => Some(needs),
            None => {
                self.want(Want::Needs(m));
                None
            }
        }
    }

    fn media(&self, p: usize) -> Option<&MediaUrn> {
        if p >= self.files.len() || !self.files[p] {
            return None;
        }
        match self.media.get(&p) {
            Some(media) => Some(media),
            None => {
                self.want(Want::Detect(p));
                None
            }
        }
    }

    /// The inputs of machine `m` file `p` fits most closely.
    fn fit(&self, m: usize, p: usize) -> Vec<usize> {
        let resolved = self.resolved(m);
        let media = self.media(p);
        let (Some(resolved), Some(media)) = (resolved, media) else {
            return Vec::new();
        };
        nearest_inputs(media, resolved.inputs.iter().enumerate().map(|(j, input)| (j, &input.media_urn)))
    }

    /// The inputs of machine `m` what live source `r` gives fits: those the
    /// content the fabric pairs with it satisfies.
    fn live_fit(&self, m: usize, r: usize) -> Vec<usize> {
        let Some(resolved) = self.resolved(m) else {
            return Vec::new();
        };
        let content = match self.lives.get(&r) {
            Some(Some(content)) => content,
            Some(None) => return Vec::new(),
            None => {
                self.want(Want::Live(r));
                return Vec::new();
            }
        };
        resolved
            .inputs
            .iter()
            .enumerate()
            .filter(|(_, input)| content.satisfies(&input.media_urn))
            .map(|(j, _)| j)
            .collect()
    }

    /// The `k`th machine planning found, by its number in the tables.
    fn planned(&self, k: usize) -> usize {
        match &self.plan {
            Some(plan) => plan.machines.get(k).copied().unwrap_or(NOWHERE),
            None => {
                self.want(Want::Plan);
                NOWHERE
            }
        }
    }

    /// The input of the `k`th planned machine the line's input `x` is
    /// planned into: the one input of it what `x` is fits most closely. A
    /// planned machine has one for each source it was planned from; one with
    /// none or several is a planner the placement disagrees with, and is
    /// said so.
    fn planned_input(&self, k: usize, x: &f::Positional) -> usize {
        let m = self.planned(k);
        if m == NOWHERE {
            return NOWHERE;
        }
        let Some(resolved) = self.resolved(m) else {
            return NOWHERE;
        };
        let media = match x {
            f::Positional::Path { p } => self.media(index(p)).cloned(),
            f::Positional::Stdin => self.from_media.clone(),
            f::Positional::Live { r } => {
                let r = index(r);
                match self.lives.get(&r) {
                    Some(content) => content.clone(),
                    None => {
                        self.want(Want::Live(r));
                        None
                    }
                }
            }
        };
        let Some(media) = media else {
            return NOWHERE;
        };
        let found = nearest_inputs(&media, resolved.inputs.iter().enumerate().map(|(j, input)| (j, &input.media_urn)));
        match found.as_slice() {
            [j] => *j,
            _ => {
                self.fault(
                    CliError::failed("a planned machine does not take what it was planned from")
                        .context(format!(
                            "{media} fits {} of its inputs ({})",
                            found.len(),
                            resolved.inputs.iter().map(|i| i.name.as_str()).collect::<Vec<_>>().join(", ")
                        ))
                        .hint("report it: the planner and the machine it wrote disagree"),
                );
                NOWHERE
            }
        }
    }
}

// =============================================================================
// Finding what the world answers
// =============================================================================

/// Where `-o` points: a directory a file can be written in, or a path where
/// one can be made (nothing is made until results are written); a file; or
/// neither.
pub fn output_dir(dir: &Path) -> f::OutputDir {
    if dir.exists() {
        if !dir.is_dir() {
            return f::OutputDir::IsFile;
        }
        return if writable(dir) { f::OutputDir::Ok } else { f::OutputDir::Unwritable };
    }
    // The nearest directory that is there must be one a directory can be
    // made in.
    let mut at = dir.parent();
    while let Some(parent) = at {
        let parent = if parent.as_os_str().is_empty() { Path::new(".") } else { parent };
        if parent.exists() {
            return if parent.is_dir() && writable(parent) { f::OutputDir::Ok } else { f::OutputDir::Unwritable };
        }
        at = parent.parent();
    }
    f::OutputDir::Unwritable
}

fn writable(dir: &Path) -> bool {
    tempfile::Builder::new().prefix(".capdag-check").tempfile_in(dir).is_ok()
}

/// Resolve a machine against the fabric: what it takes, gives and leaves to
/// the run, or why it does not resolve. A step whose name the fabric does not
/// define may be a dev cartridge's, run before it is published, when none of
/// its aliases means another cap upstream.
pub async fn resolve_entry(ctx: &Ctx, registry: &Arc<FabricRegistry>, entry: &MachineEntry) -> CliResult<MachineFacts> {
    let (notation, path) = match entry {
        MachineEntry::File(path) => {
            if !path.is_file() {
                return Ok(MachineFacts::Unresolved(
                    CliError::usage(format!("the machine file '{}' is not there", path.display()))
                        .hint("`capdag plan <files> --to <type> --save <file>` writes one"),
                ));
            }
            let notation = std::fs::read_to_string(path).map_err(|e| {
                CliError::failed(format!("the machine file '{}' could not be read", path.display())).context(e.to_string())
            })?;
            (notation, Some(path.as_path()))
        }
        MachineEntry::Inline(notation) => (notation.clone(), None),
    };
    // A name the fabric does not define may be a dev cartridge's cap. It is
    // declared ahead of the machine by its full URN, under that name
    // (`[shout-words cap:…]`), so the machine names it as it would a cap of
    // the fabric; the cap's definition is added to the registry's cache, where
    // the planner reads it.
    let mut dev_dirs: Vec<PathBuf> = Vec::new();
    let mut declared = String::new();
    let mut found: HashSet<String> = HashSet::new();
    let resolution = loop {
        match super::check::resolve_notation(registry, path, &format!("{declared}{notation}")).await {
            Ok(resolution) => break resolution,
            Err(super::check::Unresolved::UndefinedAlias { alias, error }) => {
                if !found.insert(alias.clone()) {
                    return Err(CliError::failed(format!(
                        "the dev cartridge's cap '{alias}' was declared, and the machine still does not name it"
                    ))
                    .context(error.problem)
                    .hint("report it"));
                }
                match ctx.host.find_dev_cap(&alias) {
                    Ok(Some((cap, dir))) => {
                        capdag::dev::check_no_fabric_conflict(registry, &cap)
                            .await
                            .map_err(|e| CliError::failed(e.to_string()))?;
                        if ctx.term.verbosity >= 0 && !ctx.term.json {
                            eprintln!("'{alias}' is not in the fabric: running the dev cartridge at {}", dir.display());
                        }
                        declared.push_str(&format!("[{alias} {}]\n", cap.urn));
                        registry.add_caps_to_cache(vec![cap]);
                        dev_dirs.push(dir);
                    }
                    Ok(None) => return Ok(MachineFacts::Unresolved(error)),
                    Err(e) => {
                        return Err(ctx::host_failure(format!("the dev cartridges could not be read for '{alias}'"), e))
                    }
                }
            }
            Err(super::check::Unresolved::Other(error)) => return Ok(MachineFacts::Unresolved(error)),
        }
    };
    let shown = entry.shown();
    let unrunnable = |e: capdag::orchestrator::ParseOrchestrationError| {
        CliError::failed(format!("{shown} is not a machine that can run")).context(e.to_string())
    };
    let inputs = resolution.inputs().map_err(unrunnable)?;
    let outputs = resolution.outputs().map_err(unrunnable)?;
    let caps: Vec<CapUrn> = resolution
        .machine()
        .strands()
        .iter()
        .flat_map(|strand| strand.edges().iter().map(|edge| edge.cap_urn.clone()))
        .collect();
    let mut options = Vec::new();
    for strand in 0..resolution.strand_count() {
        let plan = match resolution.plan(strand, &RunInputs::declared()).await {
            Ok(plan) => plan,
            Err(e) => return Ok(MachineFacts::Unresolved(unrunnable(e))),
        };
        options.extend(open_options(registry, &plan).await?);
    }
    Ok(MachineFacts::Resolves(Resolved { resolution, inputs, outputs, options, caps, dev_dirs }))
}

/// The step options a strand's plan leaves to the run that it must be given:
/// required, with no constant in the machine and no default.
async fn open_options(registry: &Arc<FabricRegistry>, plan: &StrandPlan) -> CliResult<Vec<OpenOption>> {
    let requirements = p::MachinePlanBuilder::new(registry.clone())
        .analyze_plan_arguments(&plan.plan)
        .await
        .map_err(|e| CliError::failed("the machine's steps could not be read").context(e.to_string()))?;
    let mut sinks: HashMap<String, usize> = HashMap::new();
    let mut options = Vec::new();
    for step in &requirements.steps {
        let node = plan
            .plan
            .nodes
            .iter()
            .find(|(_, node)| matches!(&node.node_type, p::ExecutionNodeType::Cap { token_id, .. } if token_id == &step.token_id))
            .map(|(id, _)| id.clone())
            .ok_or_else(|| CliError::failed(format!("the machine's plan has no step {}", step.token_id.as_str())))?;
        let open: Vec<&p::ArgumentInfo> = step
            .slots
            .iter()
            .filter(|slot| slot.is_required && slot.resolution == p::ArgumentResolution::RequiresUserInput)
            .collect();
        if open.is_empty() {
            continue;
        }
        // A step is named by the node its main output goes to; a step that
        // gives nothing has no such name, and is named by its cap, numbered
        // among the steps of that cap that give nothing.
        let step_name = match plan.node_names.get(&node) {
            Some(name) => name.clone(),
            None => {
                let alias = names::cap_urn(registry, &step.cap_urn);
                let n = sinks.entry(alias.clone()).or_default();
                *n += 1;
                format!("{alias}#{n}")
            }
        };
        for slot in open {
            let allowed = slot
                .validation
                .as_ref()
                .and_then(|validation| validation.get("allowed_values"))
                .and_then(serde_json::Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .map(|value| value.as_str().map(str::to_string).unwrap_or_else(|| value.to_string()))
                        .collect()
                });
            options.push(OpenOption {
                step: step_name.clone(),
                name: slot.name.clone(),
                media_urn: slot.media_urn.clone(),
                allowed,
                strand: plan.strand,
                token: step.token_id.as_str().to_string(),
            });
        }
    }
    Ok(options)
}

/// A planned machine, as it is listed: in alias form, with its step count and
/// whether a step runs a model or needs a cartridge download. `bundled` is
/// every cap the bundled cartridges provide.
async fn describe(registry: &Arc<FabricRegistry>, bundled: &[CapUrn], candidate: &p::PlanCandidate) -> CliResult<Described> {
    let machine = capdag::machine::parse_machine_async(&candidate.notation, registry)
        .await
        .map_err(|e| CliError::failed("a planned machine could not be read back").context(e.to_string()))?;
    let notation = names::machine(registry, &machine)
        .map_err(|e| CliError::failed("a planned machine could not be written in alias form").context(e))?;
    let mut model = false;
    let mut downloads = false;
    for strand in machine.strands() {
        for edge in strand.edges() {
            let cap = registry.get_cap(&edge.cap_urn.to_string()).await.map_err(ctx::registry_failure)?;
            // A cap that takes a model spec runs a model, default or not.
            model |= cap
                .model_spec_arg()
                .map_err(|e| CliError::failed("a planned machine could not be described").context(e.to_string()))?
                .is_some();
            downloads |= !bundled.iter().any(|b| b.is_equivalent(&edge.cap_urn));
        }
    }
    Ok(Described {
        notation,
        canonical: candidate.notation.clone(),
        steps: candidate.cost.cap_steps,
        model,
        downloads,
    })
}
