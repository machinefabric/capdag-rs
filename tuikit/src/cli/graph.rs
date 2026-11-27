//! A machine drawn: `capdag graph` prints it, `capdag view` shows it in the
//! terminal, `capdag edit` edits it there.
//!
//! The picture is the one the desktop clients draw — inputs, steps with their
//! arguments, maps, gathers, outputs, what each edge carries — laid out in
//! terminal cells ([`crate::tui`]). `view` and `edit` take the whole terminal
//! (the alternate screen), because a graph a person moves around in needs it;
//! without a terminal `view` prints the picture as `graph` does, and `edit`
//! says to edit the file and `capdag check` it.

use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::sync::Arc;

use super::check;
use super::ctx::Ctx;
use super::error::{CliError, CliResult, Exit};
use super::formal::{self as f, index, list};
use super::line::{MachineEntry, MachineFacts, Resolved};
use super::names;
use crate::tui::app::edit::{edit_session, CatalogCap, EditorOutcome, EditorSpec};
use crate::tui::app::TuiSettings;
use crate::tui::layout::{self, Arrangement, LayoutOptions, Lod};
use crate::tui::theme::{Appearance, ColorDepth, GlyphSet, Theme};
use super::term::outln;

/// `capdag graph --format`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Format {
    /// The machine drawn in terminal cells
    Text,
    /// Mermaid flowchart source
    Mermaid,
    /// Graphviz source
    Dot,
}

#[derive(Debug, clap::Args)]
pub struct GraphArgs {
    /// The machine: a .machine file, or written inline
    #[arg(value_name = "MACHINE")]
    pub machine: String,
    /// What to print
    #[arg(long, value_enum, default_value_t = Format::Text)]
    pub format: Format,
    /// Columns to fit the picture in (default: the terminal's width; text only)
    #[arg(long, value_name = "COLUMNS")]
    pub width: Option<u16>,
}

#[derive(Debug, clap::Args)]
pub struct ViewArgs {
    /// The machine: a .machine file, or written inline
    #[arg(value_name = "MACHINE")]
    pub machine: String,
    /// Inputs to run the machine on from the view, as `capdag run` takes them
    #[arg(value_name = "INPUTS")]
    pub inputs: Vec<String>,
    /// Where a run from the view writes its results
    #[arg(short, long, value_name = "DIR")]
    pub output: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
pub struct EditArgs {
    /// The machine file; a new one is made when it is saved
    #[arg(value_name = "MACHINE")]
    pub machine: String,
    /// Inputs to run the machine on from the editor, as `capdag run` takes them
    #[arg(value_name = "INPUTS")]
    pub inputs: Vec<String>,
    /// Where a run from the editor writes its results
    #[arg(short, long, value_name = "DIR")]
    pub output: Option<PathBuf>,
}

pub async fn graph(ctx: &Ctx, args: GraphArgs) -> CliResult<Exit> {
    let (mut world, m) = check::showing_world(ctx, &args.machine).await?;
    let (text, width) = (args.format == Format::Text, args.width.is_some());
    let verdict = world.settle(&[], |w| ctx.decide(f::Command::Graph { m: m.clone(), text, width, w }, false)).await?;
    match &verdict {
        f::CliVerdict::Is { v: f::Verdict::Answers } => {}
        f::CliVerdict::Refused { r: f::CliRefusal::WidthWithoutText } => {
            return Err(super::say::with_exit(
                super::say::cli_refusal(&f::CliRefusal::WidthWithoutText).hint("leave out --width, or draw it as text"),
                &verdict,
            ))
        }
        other => return Err(check::showing_error(&world, &m, other)),
    }
    let resolved = world.machine(index(&m.base)).and_then(MachineFacts::resolved).expect("a machine shown resolves");
    draw(ctx, &world.registry, resolved, args.format, args.width).await
}

/// A machine that resolves, drawn as `format` says.
async fn draw(
    ctx: &Ctx,
    registry: &Arc<capdag::FabricRegistry>,
    resolved: &Resolved,
    format: Format,
    width: Option<u16>,
) -> CliResult<Exit> {
    match format {
        Format::Text => print_picture(ctx, registry, &resolved.resolution, width).await?,
        Format::Mermaid | Format::Dot => {
            let mut plans = Vec::with_capacity(resolved.resolution.strand_count());
            for strand in 0..resolved.resolution.strand_count() {
                plans.push(
                    resolved
                        .resolution
                        .plan(strand, &capdag::orchestrator::machine_plan::RunInputs::declared())
                        .await
                        .map_err(|e| CliError::failed("the machine could not be drawn").context(e.to_string()))?
                        .plan,
                );
            }
            // By alias; by URN with `-v`.
            let urns = ctx.term.verbosity >= 1;
            let cap = |urn: &str| if urns { urn.to_string() } else { names::cap_urn(registry, urn) };
            let media = |urn: &str| if urns { urn.to_string() } else { names::media_urn(registry, urn) };
            let names = capdag::planner::DiagramNames { cap: &cap, media: &media };
            let source = if format == Format::Dot {
                capdag::planner::plans_to_dot(&plans, &names)
            } else {
                capdag::planner::plans_to_mermaid(&plans, &names)
            };
            outln!("{source}");
        }
    }
    Ok(Exit::Ok)
}

/// The picture of every strand of a machine, as text on stdout, in colour
/// when stdout is a terminal that wants it.
async fn print_picture(
    ctx: &Ctx,
    registry: &Arc<capdag::FabricRegistry>,
    resolved: &capdag::orchestrator::machine_plan::ResolvedNotation,
    width: Option<u16>,
) -> CliResult<()> {
    let terminal = std::io::stdout().is_terminal();
    let width = width
        .map(i32::from)
        .or_else(|| terminal.then(|| crossterm::terminal::size().ok().map(|(w, _)| w as i32)).flatten());
    let colour = terminal && ctx.term.style.is_colour();
    let theme = Theme::new(
        Appearance::Dark,
        if colour { ColorDepth::from_environment() } else { ColorDepth::None },
        GlyphSet::from_environment(),
    );
    let arrangement = match width {
        Some(width) => Arrangement::Wrapped { width },
        None => Arrangement::Rows,
    };
    // By alias; by URN with `-v`.
    let urns = ctx.term.verbosity >= 1;
    let cap_name = |urn: &str| if urns { urn.to_string() } else { names::cap_urn(registry, urn) };
    let scenes = crate::tui::scene::planned_scenes(resolved, registry, &cap_name)
        .await
        .map_err(|e| CliError::failed("the machine could not be drawn").context(e.to_string()))?;
    let mut stdout = std::io::stdout();
    for (strand, scene) in scenes.iter().enumerate() {
        let laid = match width {
            Some(width) => layout::fit(scene, width, i32::MAX, &[Arrangement::Rows, arrangement]),
            None => layout::layout(scene, &LayoutOptions { lod: Lod::Full, arrangement }),
        }
        .map_err(|e| CliError::failed("the machine could not be laid out").context(e.to_string()))?;
        if scenes.len() > 1 {
            let _ = writeln!(stdout, "part {}", strand + 1);
        }
        let picture = crate::tui::canvas::to_text(&crate::tui::canvas::render_whole(scene, &laid, &theme), colour);
        stdout
            .write_all(picture.as_bytes())
            .map_err(|e| CliError::failed("the picture could not be written").context(e.to_string()))?;
    }
    Ok(())
}

fn settings(ctx: &Ctx) -> TuiSettings {
    TuiSettings {
        appearance: Appearance::Dark,
        glyphs: GlyphSet::from_environment(),
        depth: if ctx.term.style.is_colour() { ColorDepth::from_environment() } else { ColorDepth::None },
    }
}

/// A machine and the inputs to run it on from a view or editor, as the model
/// knows them.
async fn opened<'a>(ctx: &'a Ctx, machine: &str, inputs: &[String]) -> CliResult<(super::line::World<'a>, f::Written, Vec<f::Positional>)> {
    let (mut world, m) = check::showing_world(ctx, machine).await?;
    let (_, positionals) = super::line::read_positionals(&mut world.tables, &[], inputs)?;
    world.learn_lives().await?;
    Ok((world, m, positionals))
}

pub async fn view(ctx: &Ctx, args: ViewArgs) -> CliResult<Exit> {
    let (mut world, m, inputs) = opened(ctx, &args.machine, &args.inputs).await?;
    let screen = ctx.term.stdout_is_terminal;
    let verdict = world
        .settle(&[], |w| ctx.decide(f::Command::View { m: m.clone(), inputs: list(inputs.clone()), w }, screen))
        .await?;
    match &verdict {
        f::CliVerdict::Is { v: f::Verdict::Opens } => {}
        // Where nothing can be drawn and no inputs are given, the view is the
        // picture; it runs nothing.
        f::CliVerdict::Is { v: f::Verdict::Answers } => {
            let resolved = world.machine(index(&m.base)).and_then(MachineFacts::resolved).expect("a machine shown resolves");
            return draw(ctx, &world.registry, resolved, Format::Text, None).await;
        }
        f::CliVerdict::Refused { r: f::CliRefusal::Line { r } } if !matches!(r, f::Refusal::NoSuchMachine) => {
            let state = super::check::showing_state(&m);
            return Err(super::say::line_verdict_error(ctx, &mut world, &state, &inputs, &verdict).await);
        }
        f::CliVerdict::Refused { r: r @ (f::CliRefusal::AskingOff | f::CliRefusal::ViewInputsWithoutTerminal) } => {
            return Err(super::say::with_exit(super::say::cli_refusal(r).hint(run_hint(&args.machine, &args.inputs)), &verdict));
        }
        other => return Err(check::showing_error(&world, &m, other)),
    }
    let entry = world.tables.machines[index(&m.base)].clone();
    let text = match &entry {
        MachineEntry::File(path) => std::fs::read_to_string(path).map_err(|e| {
            CliError::failed(format!("the machine file '{}' could not be read", path.display())).context(e.to_string())
        })?,
        MachineEntry::Inline(notation) => notation.clone(),
    };
    let path = match &entry {
        MachineEntry::File(path) => path.clone(),
        MachineEntry::Inline(_) => PathBuf::from("machine"),
    };
    let outcome = editor(ctx, path, text, true, !args.inputs.is_empty()).await?;
    match outcome {
        EditorOutcome::Closed { .. } => Ok(Exit::Ok),
        EditorOutcome::Run(_) => run_from_view(ctx, &args.machine, args.inputs, args.output).await,
    }
}

pub async fn edit(ctx: &Ctx, args: EditArgs) -> CliResult<Exit> {
    let (mut world, m, inputs) = opened(ctx, &args.machine, &args.inputs).await?;
    let screen = ctx.term.stdout_is_terminal;
    let verdict = world
        .settle(&[], |w| ctx.decide(f::Command::Edit { m: m.clone(), inputs: list(inputs.clone()), w }, screen))
        .await?;
    match &verdict {
        f::CliVerdict::Is { v: f::Verdict::Opens } => {}
        f::CliVerdict::Refused { r: f::CliRefusal::Line { .. } } => {
            let state = super::check::showing_state(&m);
            return Err(super::say::line_verdict_error(ctx, &mut world, &state, &inputs, &verdict).await);
        }
        f::CliVerdict::Refused { r: r @ (f::CliRefusal::NeedsTerminal | f::CliRefusal::AskingOff) } => {
            return Err(super::say::with_exit(
                super::say::cli_refusal(r).hint(format!(
                    "edit {0} in any editor, then: capdag check {0}",
                    super::shell_word(&args.machine)
                )),
                &verdict,
            ))
        }
        other => return Err(super::ends(other).expect("a verdict other than opens ends the command")),
    }
    let path = PathBuf::from(&args.machine);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => {
            return Err(CliError::failed(format!("the machine file '{}' could not be read", path.display()))
                .context(e.to_string()))
        }
    };
    let outcome = editor(ctx, path.clone(), text, false, !args.inputs.is_empty()).await?;
    match outcome {
        EditorOutcome::Closed { saved } => {
            if saved && !ctx.term.json {
                eprintln!("Saved {}", path.display());
            }
            Ok(Exit::Ok)
        }
        // The editor saved the file before asking: run what was saved.
        EditorOutcome::Run(_) => run_from_view(ctx, &args.machine, args.inputs, args.output).await,
    }
}

/// The line that runs a machine and its inputs, as a hint.
fn run_hint(machine: &str, inputs: &[String]) -> String {
    format!(
        "run it with: capdag run {}{}",
        super::shell_word(machine),
        inputs.iter().map(|i| format!(" {}", super::shell_word(i))).collect::<String>()
    )
}

/// Run a machine on the inputs given, from a view or editor: the `run` line of
/// its machine and inputs.
async fn run_from_view(ctx: &Ctx, machine: &str, inputs: Vec<String>, output: Option<PathBuf>) -> CliResult<Exit> {
    let mut args = vec![machine.to_string()];
    args.extend(inputs);
    super::run::run(
        ctx,
        super::run::Args {
            args,
            after: Vec::new(),
            inputs: Vec::new(),
            lives: Vec::new(),
            options: Vec::new(),
            to: Vec::new(),
            from: None,
            selector: None,
            output,
            force: false,
            progress: super::live::ProgressChoice::Auto,
            trace: None,
            dev_bins: Vec::new(),
            planning: super::line::PlanningFlags::default(),
            help: false,
        },
    )
    .await
}

async fn editor(ctx: &Ctx, path: PathBuf, text: String, read_only: bool, inputs_given: bool) -> CliResult<EditorOutcome> {
    let registry = ctx.registry().await?;
    let working = std::env::current_dir()
        .map_err(|e| CliError::failed("the working directory could not be read").context(e.to_string()))?;
    // A new machine is known by where it will be written, so the machines it
    // includes are read beside it from the start.
    let machine_id = if path.is_file() {
        capdag::FileMachineSource::id_of(&path)
            .map_err(|e| CliError::failed(format!("'{}' could not be read", path.display())).context(e.to_string()))?
    } else {
        let parent = match path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
            _ => PathBuf::from("."),
        };
        let dir = std::fs::canonicalize(&parent).map_err(|e| {
            CliError::failed(format!("the directory of '{}' could not be read", path.display())).context(e.to_string())
        })?;
        let name = path
            .file_name()
            .ok_or_else(|| CliError::usage(format!("'{}' names no file", path.display())))?;
        dir.join(name).to_string_lossy().into_owned()
    };
    let catalog = CatalogCap::from_registry(&registry)
        .await
        .map_err(|e| CliError::failed("the capabilities could not be listed for the editor").context(e.to_string()))?;
    let spec = EditorSpec {
        path,
        text,
        read_only,
        registry,
        machines: Arc::new(capdag::FileMachineSource::new(working)),
        machine_id: Some(machine_id),
        catalog,
        inputs_given,
    };
    edit_session(spec, settings(ctx))
        .await
        .map_err(|e| CliError::failed(if read_only { "the view stopped" } else { "the editor stopped" }).context(e.to_string()))
}
