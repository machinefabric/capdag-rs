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
use super::names;
use crate::tui::app::edit::{edit_session, CatalogCap, EditorOutcome, EditorSpec};
use crate::tui::app::TuiSettings;
use crate::tui::layout::{self, Arrangement, LayoutOptions, Lod};
use crate::tui::theme::{Appearance, ColorDepth, GlyphSet, Theme};

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
    /// The machine file
    #[arg(value_name = "MACHINE")]
    pub machine: PathBuf,
    /// What to print
    #[arg(long, value_enum, default_value_t = Format::Text)]
    pub format: Format,
    /// Columns to fit the picture in (default: the terminal's width; text only)
    #[arg(long, value_name = "COLUMNS")]
    pub width: Option<u16>,
}

#[derive(Debug, clap::Args)]
pub struct ViewArgs {
    /// The machine file
    #[arg(value_name = "MACHINE")]
    pub machine: PathBuf,
    /// Inputs to run the machine on from the view
    #[arg(value_name = "INPUTS")]
    pub inputs: Vec<String>,
    /// Where a run from the view writes its results
    #[arg(short, long, value_name = "DIR")]
    pub output: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
pub struct EditArgs {
    /// The machine file; a new one is created when it is saved
    #[arg(value_name = "MACHINE")]
    pub machine: PathBuf,
    /// Inputs to run the machine on from the editor
    #[arg(value_name = "INPUTS")]
    pub inputs: Vec<String>,
    /// Where a run from the editor writes its results
    #[arg(short, long, value_name = "DIR")]
    pub output: Option<PathBuf>,
}

pub async fn graph(ctx: &Ctx, args: GraphArgs) -> CliResult<Exit> {
    let notation = check::read_machine(&args.machine)?;
    let registry = ctx.registry().await?;
    let resolved = check::resolve(&registry, &args.machine, &notation).await?;
    match args.format {
        Format::Text => print_picture(ctx, &registry, &resolved, args.width).await?,
        Format::Mermaid | Format::Dot => {
            let working = std::env::current_dir()
                .map_err(|e| CliError::failed("the working directory could not be read").context(e.to_string()))?;
            let id = crate::FileMachineSource::id_of(&args.machine)
                .map_err(|e| CliError::failed("the machine file could not be read").context(e.to_string()))?;
            let plans = crate::orchestrator::build_plans_from_notation_from(
                &notation,
                registry.clone(),
                &crate::FileMachineSource::new(working),
                Some(&id),
            )
            .await
            .map_err(|e| CliError::failed(format!("{} could not be drawn", args.machine.display())).context(e.to_string()))?;
            // By alias; by URN with `-v`.
            let urns = ctx.term.verbosity >= 1;
            let cap = |urn: &str| if urns { urn.to_string() } else { names::cap_urn(&registry, urn) };
            let media = |urn: &str| if urns { urn.to_string() } else { names::media_urn(&registry, urn) };
            let names = crate::planner::DiagramNames { cap: &cap, media: &media };
            let source = if args.format == Format::Dot {
                crate::planner::plans_to_dot(&plans, &names)
            } else {
                crate::planner::plans_to_mermaid(&plans, &names)
            };
            println!("{source}");
        }
    }
    Ok(Exit::Ok)
}

/// The picture of every strand of a machine, as text on stdout, in colour
/// when stdout is a terminal that wants it.
async fn print_picture(
    ctx: &Ctx,
    registry: &Arc<crate::FabricRegistry>,
    resolved: &crate::orchestrator::machine_plan::ResolvedNotation,
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

fn full_screen_available() -> bool {
    std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
}

pub async fn view(ctx: &Ctx, args: ViewArgs) -> CliResult<Exit> {
    if !ctx.term.interactive || !full_screen_available() {
        // Without a terminal the view is the picture; it runs nothing.
        return graph(ctx, GraphArgs { machine: args.machine, format: Format::Text, width: None }).await;
    }
    let text = check::read_machine(&args.machine)?;
    let outcome = editor(ctx, args.machine.clone(), text, true, !args.inputs.is_empty()).await?;
    match outcome {
        EditorOutcome::Closed { .. } => Ok(Exit::Ok),
        EditorOutcome::Run(_) => run_file(ctx, args.machine, args.inputs, args.output).await,
    }
}

pub async fn edit(ctx: &Ctx, args: EditArgs) -> CliResult<Exit> {
    if !ctx.term.interactive || !full_screen_available() {
        return Err(CliError::usage("capdag edit needs a terminal")
            .hint(format!("edit {} in any editor, then: capdag check {}", args.machine.display(), args.machine.display())));
    }
    let text = match std::fs::read_to_string(&args.machine) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => {
            return Err(CliError::failed(format!("the machine file '{}' could not be read", args.machine.display()))
                .context(e.to_string()))
        }
    };
    let outcome = editor(ctx, args.machine.clone(), text, false, !args.inputs.is_empty()).await?;
    match outcome {
        EditorOutcome::Closed { saved } => {
            if saved && !ctx.term.json {
                eprintln!("Saved {}", args.machine.display());
            }
            Ok(Exit::Ok)
        }
        // The editor saved the file before asking: run what was saved.
        EditorOutcome::Run(_) => run_file(ctx, args.machine, args.inputs, args.output).await,
    }
}

/// Run a machine file on the inputs given, as `capdag run` does.
async fn run_file(ctx: &Ctx, machine: PathBuf, inputs: Vec<String>, output: Option<PathBuf>) -> CliResult<Exit> {
    super::run::run(
        ctx,
        super::run::Args {
            target: machine.display().to_string(),
            rest: inputs,
            output,
            force: false,
            inputs: Vec::new(),
            args: Vec::new(),
            to: None,
            selector: None,
            progress: super::live::ProgressChoice::Auto,
            trace: None,
            dev_bins: Vec::new(),
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
        crate::FileMachineSource::id_of(&path)
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
        machines: Arc::new(crate::FileMachineSource::new(working)),
        machine_id: Some(machine_id),
        catalog,
        inputs_given,
    };
    edit_session(spec, settings(ctx))
        .await
        .map_err(|e| CliError::failed(if read_only { "the view stopped" } else { "the editor stopped" }).context(e.to_string()))
}
