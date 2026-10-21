//! The machine editor: a machine's notation, drawn and edited structurally,
//! with its source beside it.
//!
//! Every structural edit is a transaction, as in the desktop's editor: it is
//! applied to a copy of the draft ([`MachineDraft`]), written back as
//! notation, and resolved against the fabric. A draft that resolves is
//! committed — onto the undo history, into the picture — and one that does
//! not is rolled back exactly, its diagnostic shown. Nothing partial, nothing
//! silent. The source pane edits the text directly; applying it is one
//! transaction too. Saving writes the file atomically.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use futures_util::StreamExt;
use ratatui::layout::{Constraint, Layout as Split, Margin, Rect as Area};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Widget, Wrap};
use ratatui::Frame;
use tokio::sync::mpsc;

use crate::machine::syntax::{parse_syntax, SyntaxStatementKind, SyntaxValue};
use crate::orchestrator::ResolvedNotation;
use crate::tui::app::graph::{GraphAction, GraphView};
use crate::tui::app::palette::{Command, Palette, PaletteResult};
use crate::tui::app::source::SourceBuffer;
use crate::tui::app::{too_small, Display, Tui, TuiError, TuiSettings};
use crate::tui::edit::{DraftHeader, DraftValue, EditError, MachineDraft};
use crate::tui::scene::{cap_short_label, machine_scene, DraftInfo, SceneError};
use crate::tui::text;
use crate::tui::theme::{Theme, Tone};
use crate::{CapUrn, FabricRegistry, MachineSource, MediaUrn};

/// A cap the palette offers.
#[derive(Debug, Clone)]
pub struct CatalogCap {
    /// Canonical.
    pub urn: String,
    pub title: String,
    /// What its main input takes.
    pub input: MediaUrn,
}

impl CatalogCap {
    /// The caps the registry holds, as the palette offers them.
    pub async fn from_registry(registry: &FabricRegistry) -> Result<Vec<CatalogCap>, String> {
        let caps = registry
            .get_cached_caps()
            .await
            .map_err(|e| format!("the fabric's caps cannot be read: {e}"))?;
        let mut out = Vec::with_capacity(caps.len());
        for cap in caps {
            let input = MediaUrn::from_string(cap.urn.in_spec())
                .map_err(|e| format!("cap '{}' takes an invalid media URN: {e}", cap.urn))?;
            out.push(CatalogCap {
                urn: cap.urn.to_string(),
                title: cap.title.clone(),
                input,
            });
        }
        out.sort_by(|a, b| a.title.cmp(&b.title).then_with(|| a.urn.cmp(&b.urn)));
        Ok(out)
    }
}

/// What the editor is given.
pub struct EditorSpec {
    pub path: PathBuf,
    /// The file's text; empty for a machine not written yet.
    pub text: String,
    /// `view`: nothing is changed or saved.
    pub read_only: bool,
    pub registry: Arc<FabricRegistry>,
    /// Where the machines it includes are read from, and its own id there.
    pub machines: Arc<dyn MachineSource>,
    pub machine_id: Option<String>,
    pub catalog: Vec<CatalogCap>,
    /// Whether the command line gave inputs to run the machine on: a
    /// machine with inputs runs from the editor only when it did.
    pub inputs_given: bool,
}

/// How the editor ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EditorOutcome {
    /// Closed; `saved` when the file was written at least once.
    Closed { saved: bool },
    /// Asked to run (after saving, when editing): the notation to run.
    Run(String),
}

/// What resolving `text` says of it: what each node holds, what each cap is
/// called. An error is the resolver's diagnostic, said as it said it.
async fn resolve(
    text: &str,
    registry: Arc<FabricRegistry>,
    machines: &dyn MachineSource,
    machine_id: Option<&str>,
    catalog: &[CatalogCap],
) -> Result<DraftInfo, String> {
    let resolved = ResolvedNotation::resolve_from(text, registry, machines, machine_id)
        .await
        .map_err(|e| e.to_string())?;
    let payload = resolved.render_payload_json().map_err(|e| e.to_string())?;
    let payload: serde_json::Value = serde_json::from_str(&payload)
        .map_err(|e| format!("the machine's render payload is not JSON: {e}"))?;
    let mut info = DraftInfo::default();
    for strand in payload["strands"].as_array().into_iter().flatten() {
        for node in strand["nodes"].as_array().into_iter().flatten() {
            let (Some(name), Some(urn), Some(title)) = (node["name"].as_str(), node["urn"].as_str(), node["title"].as_str())
            else {
                return Err(format!("the machine's render payload has a node without a name, type or title: {node}"));
            };
            let urn = MediaUrn::from_string(urn).map_err(|e| format!("node '{name}' holds an invalid media URN: {e}"))?;
            info.media_of.insert(name.to_string(), (urn, title.to_string()));
        }
    }
    let draft = MachineDraft::parse(text).map_err(|e| e.to_string())?;
    for header in &draft.headers {
        if let DraftHeader::Cap { cap_urn, .. } = header {
            // The text resolved, so its cap URNs parse.
            let canonical = CapUrn::from_string(cap_urn).map_err(|e| e.to_string())?.to_string();
            if let Some(cap) = catalog.iter().find(|c| c.urn == canonical) {
                info.cap_titles.insert(cap_urn.clone(), cap.title.clone());
            }
        }
    }
    Ok(info)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pane {
    Graph,
    Source,
}

/// What has focus in the graph.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Focused {
    /// A step, by its wiring's index.
    Step(usize),
    /// A node, by name.
    Node(String),
}

/// What a cap chosen from the palette is for.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CapPurpose {
    Connect { from: String },
    InsertBefore { wiring: usize, source: usize },
    SetCap { wiring: usize },
    NewStrand,
}

/// What a node name typed into the prompt is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NamePurpose {
    RewireSource { wiring: usize, source: usize },
    RewireTarget { wiring: usize },
    JoinSource { wiring: usize },
}

impl NamePurpose {
    fn label(self) -> &'static str {
        match self {
            NamePurpose::RewireSource { .. } => "Read the source from node",
            NamePurpose::RewireTarget { .. } => "Write the output into node",
            NamePurpose::JoinSource { .. } => "Read another node",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    AddStep,
    NewStrand,
    DeleteStep,
    Disconnect,
    ReplaceCap,
    NewInput,
    JoinSource,
    RemoveSource,
    InsertBefore,
    RewireSource,
    RewireOutput,
    NextSource,
    PreviousSource,
    Undo,
    Redo,
    Save,
    Run,
    ToggleSource,
    DiscardSource,
    Help,
    Quit,
}

fn commands(read_only: bool) -> Vec<Command<Action>> {
    let c = |name: &str, keys: &str, action: Action| Command {
        name: name.to_string(),
        keys: keys.to_string(),
        action,
    };
    let mut all = vec![
        c("Show or hide the source", "e", Action::ToggleSource),
        c("Select the step's next source", ".", Action::NextSource),
        c("Select the step's previous source", ",", Action::PreviousSource),
        c("Keys", "?", Action::Help),
        c("Quit", "q", Action::Quit),
    ];
    if !read_only {
        all.extend([
            c("Add a step reading the focused node", "a", Action::AddStep),
            c("Add an independent step", "A", Action::NewStrand),
            c("Delete the step, reconnecting around it", "d", Action::DeleteStep),
            c("Disconnect the step", "D", Action::Disconnect),
            c("Replace the step's cap", "r", Action::ReplaceCap),
            c("Give the step a new input", "I", Action::NewInput),
            c("Read another node into the step", "N", Action::JoinSource),
            c("Remove the selected source", "X", Action::RemoveSource),
            c("Insert a step before the selected source", "i", Action::InsertBefore),
            c("Rewire the selected source", "s", Action::RewireSource),
            c("Rewire the step's output", "o", Action::RewireOutput),
            c("Undo", "u ^Z", Action::Undo),
            c("Redo", "U ^Y", Action::Redo),
            c("Save", "^S", Action::Save),
            c("Discard the source's unapplied changes", "", Action::DiscardSource),
        ]);
    }
    all.push(c(if read_only { "Run" } else { "Save and run" }, "x", Action::Run));
    all
}

const KEYS: &[&str] = &[
    "Graph    ←↑↓→ hjkl move · HJKL pan · tab next · ] [ follow edges · +/- detail · f fit · w wrap · m map",
    "Steps    a add a step reading the node · A an independent step · r replace the cap",
    "         d delete (reconnecting around it) · D disconnect",
    "Sources  . , select · I new input · N read another node · X remove",
    "         i insert a step before it · s rewire it · o rewire the step's output",
    "History  u ^Z undo · U ^Y redo",
    "File     ^S save · x save and run · q quit",
    "Source   e show and focus · ^S apply · esc back to the graph",
    "Anywhere : or ^K every command by name · ? these keys",
];

const TOAST_FOR: Duration = Duration::from_secs(6);

const SOURCE_NOT_APPLIED: &str = "The source has changes not applied: apply them (^S) or discard them first.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum History {
    Edit,
    Undo,
    Redo,
}

/// The verdict on a transaction, as the resolving task sends it back.
struct Verdict {
    history: History,
    /// The committed text when the transaction started.
    previous: String,
    text: String,
    result: Result<DraftInfo, String>,
}

struct Editor {
    spec: EditorSpec,
    settings: TuiSettings,
    /// The committed text: what the picture shows.
    text: String,
    saved_text: String,
    saved_once: bool,
    draft: MachineDraft,
    info: DraftInfo,
    /// Why the committed text is not a machine yet — only ever the text the
    /// editor opened, since a commit is a text that resolved.
    diagnostic: Option<String>,
    undo: Vec<String>,
    redo: Vec<String>,
    graph: GraphView,
    pane: Pane,
    source_open: bool,
    source: SourceBuffer,
    /// The source pane's text differs from the committed text.
    source_dirty: bool,
    /// The selected source member of the focused step.
    selected_source: usize,
    cap_palette: Option<(CapPurpose, Palette<String>)>,
    name_prompt: Option<(NamePurpose, String)>,
    command_palette: Option<Palette<Action>>,
    toasts: Vec<(String, Tone, Instant)>,
    confirm_quit: bool,
    help: bool,
    /// A transaction is resolving.
    pending: bool,
    quit: Option<EditorOutcome>,
    results: mpsc::UnboundedSender<Verdict>,
}

impl Editor {
    fn toast(&mut self, text: impl Into<String>, tone: Tone) {
        self.toasts.push((text.into(), tone, Instant::now()));
    }

    fn dirty(&self) -> bool {
        self.text != self.saved_text
    }

    fn focused(&self) -> Option<Focused> {
        let id = self.graph.focus.as_ref()?;
        if let Some(index) = id.strip_prefix("step-") {
            return index.parse().ok().map(Focused::Step);
        }
        id.strip_prefix("node-").map(|name| Focused::Node(name.to_string()))
    }

    fn focused_wiring(&self) -> Option<usize> {
        match self.focused()? {
            Focused::Step(w) => Some(w),
            Focused::Node(_) => None,
        }
    }

    fn media_of(&self, name: &str) -> Option<&MediaUrn> {
        self.info.media_of.get(name).map(|(urn, _)| urn)
    }

    /// The caps whose main input takes `media`; every cap when `media` is
    /// not known (an independent step, a replaced cap, an unresolved node).
    fn caps_accepting(&self, media: Option<&MediaUrn>) -> Vec<Command<String>> {
        self.spec
            .catalog
            .iter()
            .filter(|cap| media.is_none_or(|m| cap.input.accepts(m).unwrap_or(false)))
            .map(|cap| Command {
                name: cap.title.clone(),
                keys: cap_short_label(&cap.urn).unwrap_or_default(),
                action: cap.urn.clone(),
            })
            .collect()
    }

    fn open_cap_palette(&mut self, purpose: CapPurpose) {
        let media = match &purpose {
            CapPurpose::Connect { from } => self.media_of(from).cloned(),
            CapPurpose::InsertBefore { wiring, source } => {
                match self.draft.wirings.get(*wiring).and_then(|w| w.sources.get(*source)).map(|m| &m.value) {
                    Some(DraftValue::Node(node)) => self.media_of(node).cloned(),
                    _ => None,
                }
            }
            CapPurpose::SetCap { .. } | CapPurpose::NewStrand => None,
        };
        let caps = self.caps_accepting(media.as_ref());
        if caps.is_empty() {
            self.toast(
                match media {
                    Some(m) => format!("No cap in the fabric takes {m}."),
                    None => "The fabric holds no caps.".to_string(),
                },
                Tone::Warning,
            );
            return;
        }
        self.cap_palette = Some((purpose, Palette::new(caps)));
    }

    /// Whether a change may start now; says why not when it may not.
    fn can_change(&mut self) -> bool {
        if self.spec.read_only {
            self.toast("This is a view: nothing in it changes (open it with `capdag edit`).", Tone::Warning);
            return false;
        }
        if self.pending {
            self.toast("The last change is still being checked.", Tone::Warning);
            return false;
        }
        true
    }

    /// A structural edit: `op` on a copy of the draft, written out and
    /// resolved before anything is committed.
    fn transact(&mut self, op: impl FnOnce(&mut MachineDraft) -> Result<(), EditError>) {
        if !self.can_change() {
            return;
        }
        if self.source_dirty {
            self.toast(SOURCE_NOT_APPLIED, Tone::Warning);
            return;
        }
        let mut draft = self.draft.clone();
        if let Err(e) = op(&mut draft) {
            self.toast(e.to_string(), Tone::Danger);
            return;
        }
        self.submit(History::Edit, draft.to_notation());
    }

    fn submit(&mut self, history: History, text: String) {
        self.pending = true;
        let registry = self.spec.registry.clone();
        let machines = self.spec.machines.clone();
        let machine_id = self.spec.machine_id.clone();
        let catalog = self.spec.catalog.clone();
        let previous = self.text.clone();
        let results = self.results.clone();
        tokio::spawn(async move {
            let result = resolve(&text, registry, machines.as_ref(), machine_id.as_deref(), &catalog).await;
            // The editor gone, there is no one to tell.
            let _ = results.send(Verdict {
                history,
                previous,
                text,
                result,
            });
        });
    }

    /// Commit a transaction that resolved, or roll back one that did not.
    fn settle(&mut self, verdict: Verdict) -> Result<(), TuiError> {
        self.pending = false;
        let Verdict {
            history,
            previous,
            text,
            result,
        } = verdict;
        match result {
            Ok(info) => {
                let draft = MachineDraft::parse(&text)
                    .map_err(|e| TuiError::Inconsistent(format!("notation that resolved does not parse: {e}")))?;
                match history {
                    History::Edit => {
                        self.undo.push(previous);
                        self.redo.clear();
                    }
                    History::Undo => self.redo.push(previous),
                    History::Redo => self.undo.push(previous),
                }
                self.source = SourceBuffer::new(&text);
                self.source_dirty = false;
                self.text = text;
                self.draft = draft;
                self.info = info;
                self.diagnostic = None;
                if let Some(w) = self.focused_wiring() {
                    let count = self.draft.wirings.get(w).map_or(0, |w| w.sources.len());
                    self.selected_source = self.selected_source.min(count.saturating_sub(1));
                }
            }
            Err(why) => {
                // The history move is rolled back with the text.
                match history {
                    History::Undo => self.undo.push(text),
                    History::Redo => self.redo.push(text),
                    History::Edit => {}
                }
                self.toast(format!("Not applied — {why}"), Tone::Danger);
            }
        }
        Ok(())
    }

    fn apply_source(&mut self) {
        if !self.can_change() {
            return;
        }
        let text = self.source.text();
        if let Err(e) = MachineDraft::parse(&text) {
            self.toast(format!("Not applied — {e}"), Tone::Danger);
            return;
        }
        self.submit(History::Edit, text);
    }

    /// Write the committed text: beside the file first, then renamed over
    /// it, so the file is the old text or the new one, never part of either.
    fn save(&mut self) -> bool {
        if self.spec.read_only {
            self.toast("This is a view: nothing is saved.", Tone::Warning);
            return false;
        }
        let path = self.spec.path.clone();
        let Some(file_name) = path.file_name() else {
            self.toast(format!("{} names no file.", path.display()), Tone::Danger);
            return false;
        };
        let mut staged_name = std::ffi::OsString::from(".");
        staged_name.push(file_name);
        staged_name.push(".saving");
        let staged = path.with_file_name(staged_name);
        let written = std::fs::write(&staged, &self.text).and_then(|_| std::fs::rename(&staged, &path));
        match written {
            Ok(()) => {
                self.saved_text = self.text.clone();
                self.saved_once = true;
                self.toast(format!("Saved {}", path.display()), Tone::Success);
                true
            }
            Err(e) => {
                let _ = std::fs::remove_file(&staged);
                self.toast(format!("Saving {} failed: {e}", path.display()), Tone::Danger);
                false
            }
        }
    }

    fn step_or_say(&mut self) -> Option<usize> {
        let wiring = self.focused_wiring();
        if wiring.is_none() {
            self.toast("Focus a step first.", Tone::Warning);
        }
        wiring
    }

    fn act(&mut self, action: Action) {
        match action {
            Action::AddStep => match self.focused() {
                Some(Focused::Node(from)) => {
                    if self.can_change() {
                        self.open_cap_palette(CapPurpose::Connect { from })
                    }
                }
                _ => self.toast("Focus a node: the new step reads it.", Tone::Warning),
            },
            Action::NewStrand => {
                if self.can_change() {
                    self.open_cap_palette(CapPurpose::NewStrand)
                }
            }
            Action::DeleteStep => {
                if let Some(w) = self.step_or_say() {
                    self.transact(move |d| d.delete_step(w));
                }
            }
            Action::Disconnect => {
                if let Some(w) = self.step_or_say() {
                    self.transact(move |d| d.disconnect(w));
                }
            }
            Action::ReplaceCap => {
                if let Some(w) = self.step_or_say() {
                    if self.can_change() {
                        self.open_cap_palette(CapPurpose::SetCap { wiring: w });
                    }
                }
            }
            Action::NewInput => {
                if let Some(w) = self.step_or_say() {
                    self.transact(move |d| d.add_source(w));
                }
            }
            Action::JoinSource => {
                if let Some(w) = self.step_or_say() {
                    if self.can_change() {
                        self.name_prompt = Some((NamePurpose::JoinSource { wiring: w }, String::new()));
                    }
                }
            }
            Action::RemoveSource => {
                if let Some(w) = self.step_or_say() {
                    let s = self.selected_source;
                    self.transact(move |d| d.remove_source(w, s));
                }
            }
            Action::InsertBefore => {
                if let Some(w) = self.step_or_say() {
                    if self.can_change() {
                        let source = self.selected_source;
                        self.open_cap_palette(CapPurpose::InsertBefore { wiring: w, source });
                    }
                }
            }
            Action::RewireSource => {
                if let Some(w) = self.step_or_say() {
                    if self.can_change() {
                        let source = self.selected_source;
                        self.name_prompt = Some((NamePurpose::RewireSource { wiring: w, source }, String::new()));
                    }
                }
            }
            Action::RewireOutput => {
                if let Some(w) = self.step_or_say() {
                    if self.can_change() {
                        self.name_prompt = Some((NamePurpose::RewireTarget { wiring: w }, String::new()));
                    }
                }
            }
            Action::NextSource | Action::PreviousSource => {
                if let Some(w) = self.step_or_say() {
                    let count = self.draft.wirings.get(w).map_or(0, |w| w.sources.len());
                    if count > 0 {
                        self.selected_source = if action == Action::NextSource {
                            (self.selected_source + 1) % count
                        } else {
                            (self.selected_source + count - 1) % count
                        };
                    }
                }
            }
            Action::Undo | Action::Redo => {
                if !self.can_change() {
                    return;
                }
                if self.source_dirty {
                    self.toast(SOURCE_NOT_APPLIED, Tone::Warning);
                    return;
                }
                let (popped, history, none) = if action == Action::Undo {
                    (self.undo.pop(), History::Undo, "Nothing to undo.")
                } else {
                    (self.redo.pop(), History::Redo, "Nothing to redo.")
                };
                match popped {
                    Some(text) => self.submit(history, text),
                    None => self.toast(none, Tone::Muted),
                }
            }
            Action::Save => {
                if self.pending {
                    self.toast("The last change is still being checked.", Tone::Warning);
                } else {
                    self.save();
                }
            }
            Action::Run => {
                if !self.spec.inputs_given && !self.draft.input_names().is_empty() {
                    self.toast("Give the machine's inputs on the command line to run it from here.", Tone::Warning);
                } else if self.pending {
                    self.toast("The last change is still being checked.", Tone::Warning);
                } else if self.draft.wirings.is_empty() {
                    self.toast("An empty machine has nothing to run.", Tone::Warning);
                } else if self.diagnostic.is_some() {
                    self.toast("Only a machine that resolves runs.", Tone::Warning);
                } else if self.source_dirty {
                    self.toast(SOURCE_NOT_APPLIED, Tone::Warning);
                } else if self.spec.read_only || !self.dirty() || self.save() {
                    self.quit = Some(EditorOutcome::Run(self.text.clone()));
                }
            }
            Action::ToggleSource => {
                if self.source_open && self.pane == Pane::Source {
                    self.source_open = false;
                    self.pane = Pane::Graph;
                } else {
                    self.source_open = true;
                    self.pane = Pane::Source;
                }
            }
            Action::DiscardSource => {
                self.source = SourceBuffer::new(&self.text);
                self.source_dirty = false;
            }
            Action::Help => self.help = !self.help,
            Action::Quit => {
                if !self.spec.read_only && (self.dirty() || self.source_dirty) {
                    self.confirm_quit = true;
                } else {
                    self.quit = Some(EditorOutcome::Closed { saved: self.saved_once });
                }
            }
        }
    }

    fn on_key(&mut self, key: KeyEvent) {
        if key.kind == KeyEventKind::Release {
            return;
        }
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        if ctrl && key.code == KeyCode::Char('c') {
            self.act(Action::Quit);
            return;
        }
        if self.confirm_quit {
            self.confirm_quit = false;
            match key.code {
                KeyCode::Char('y') => self.quit = Some(EditorOutcome::Closed { saved: self.saved_once }),
                KeyCode::Char('s') if !self.source_dirty => {
                    if self.save() {
                        self.quit = Some(EditorOutcome::Closed { saved: true });
                    }
                }
                _ => {}
            }
            return;
        }
        if self.help {
            self.help = false;
            return;
        }
        if let Some((purpose, palette)) = &mut self.cap_palette {
            match palette.on_key(key) {
                PaletteResult::Continue => {}
                PaletteResult::Close => self.cap_palette = None,
                PaletteResult::Run(cap) => {
                    let purpose = purpose.clone();
                    self.cap_palette = None;
                    match purpose {
                        CapPurpose::Connect { from } => self.transact(move |d| d.connect(&from, &cap)),
                        CapPurpose::InsertBefore { wiring, source } => {
                            self.transact(move |d| d.insert_cap(wiring, source, &cap))
                        }
                        CapPurpose::SetCap { wiring } => self.transact(move |d| d.set_cap(wiring, &cap)),
                        CapPurpose::NewStrand => self.transact(move |d| d.create_strand(&cap)),
                    }
                }
            }
            return;
        }
        if let Some((purpose, typed)) = &mut self.name_prompt {
            let purpose = *purpose;
            match key.code {
                KeyCode::Esc => self.name_prompt = None,
                KeyCode::Backspace => {
                    typed.pop();
                }
                KeyCode::Tab => {
                    // Complete to the first name that starts with what is typed.
                    let prefix = typed.clone();
                    if let Some(name) = self.draft.node_names().into_iter().find(|n| n.starts_with(&prefix)) {
                        self.name_prompt = Some((purpose, name));
                    }
                }
                KeyCode::Char(c) if !ctrl => typed.push(c),
                KeyCode::Enter => {
                    let name = typed.clone();
                    self.name_prompt = None;
                    match purpose {
                        NamePurpose::RewireSource { wiring, source } => {
                            self.transact(move |d| d.rewire_source(wiring, source, &name))
                        }
                        NamePurpose::RewireTarget { wiring } => self.transact(move |d| d.rewire_target(wiring, &name)),
                        NamePurpose::JoinSource { wiring } => self.transact(move |d| d.add_existing_source(wiring, &name)),
                    }
                }
                _ => {}
            }
            return;
        }
        if let Some(palette) = &mut self.command_palette {
            match palette.on_key(key) {
                PaletteResult::Continue => {}
                PaletteResult::Close => self.command_palette = None,
                PaletteResult::Run(action) => {
                    self.command_palette = None;
                    self.act(action);
                }
            }
            return;
        }
        if ctrl {
            match key.code {
                KeyCode::Char('s') if self.pane == Pane::Source && self.source_dirty => self.apply_source(),
                KeyCode::Char('s') => self.act(Action::Save),
                KeyCode::Char('z') => self.act(Action::Undo),
                KeyCode::Char('y') => self.act(Action::Redo),
                KeyCode::Char('k') => self.open_commands(),
                _ => {
                    if self.pane == Pane::Source {
                        self.source_key(key);
                    }
                }
            }
            return;
        }
        if self.pane == Pane::Source {
            if key.code == KeyCode::Esc {
                self.pane = Pane::Graph;
            } else {
                self.source_key(key);
            }
            return;
        }
        let action = match key.code {
            KeyCode::Char('q') => Action::Quit,
            KeyCode::Char('?') => Action::Help,
            KeyCode::Char(':') => {
                self.open_commands();
                return;
            }
            KeyCode::Char('e') => Action::ToggleSource,
            KeyCode::Char('a') => Action::AddStep,
            KeyCode::Char('A') => Action::NewStrand,
            KeyCode::Char('d') => Action::DeleteStep,
            KeyCode::Char('D') => Action::Disconnect,
            KeyCode::Char('r') => Action::ReplaceCap,
            KeyCode::Char('I') => Action::NewInput,
            KeyCode::Char('N') => Action::JoinSource,
            KeyCode::Char('X') => Action::RemoveSource,
            KeyCode::Char('i') => Action::InsertBefore,
            KeyCode::Char('s') => Action::RewireSource,
            KeyCode::Char('o') => Action::RewireOutput,
            KeyCode::Char('.') => Action::NextSource,
            KeyCode::Char(',') => Action::PreviousSource,
            KeyCode::Char('u') => Action::Undo,
            KeyCode::Char('U') => Action::Redo,
            KeyCode::Char('x') => Action::Run,
            _ => {
                let before = self.graph.focus.clone();
                if self.graph.on_key(key) != GraphAction::Ignored && self.graph.focus != before {
                    self.selected_source = 0;
                }
                return;
            }
        };
        self.act(action);
    }

    fn open_commands(&mut self) {
        self.command_palette = Some(Palette::new(commands(self.spec.read_only)));
    }

    fn source_key(&mut self, key: KeyEvent) {
        let handled = if self.spec.read_only {
            self.source.move_cursor(key)
        } else {
            self.source.on_key(key)
        };
        if handled {
            self.source_dirty = self.source.text() != self.text;
            self.focus_from_source();
        }
    }

    /// Focus, in the graph, what the source's cursor is in: the node whose
    /// name it is on, else the step whose wiring it is in. The source and
    /// the graph agree only while the source is the committed text.
    fn focus_from_source(&mut self) {
        if self.source_dirty {
            return;
        }
        let byte = self.source.cursor_byte();
        let Ok(statements) = parse_syntax(&self.text) else {
            return;
        };
        let wirings = statements.iter().filter_map(|s| match &s.kind {
            SyntaxStatementKind::Wiring(w) => Some((s, w)),
            _ => None,
        });
        for (index, (statement, wiring)) in wirings.enumerate() {
            if !(statement.start..=statement.end).contains(&byte) {
                continue;
            }
            let on_name = wiring
                .sources
                .iter()
                .filter_map(|m| match &m.value {
                    SyntaxValue::Node(n) => Some(n),
                    _ => None,
                })
                .chain(wiring.targets.iter().map(|t| &t.node))
                .find(|n| (n.start..=n.end).contains(&byte))
                .map(|n| format!("node-{}", n.value));
            let id = on_name.unwrap_or_else(|| format!("step-{index}"));
            if self.graph.focus.as_ref() != Some(&id) {
                self.graph.focus = Some(id);
                self.selected_source = 0;
            }
            self.graph.follow = true;
            return;
        }
    }

    /// The byte ranges of the source that are what the graph has focused.
    fn marked(&self) -> Vec<(usize, usize)> {
        if self.source_dirty {
            return Vec::new();
        }
        let Some(focused) = self.focused() else {
            return Vec::new();
        };
        let Ok(statements) = parse_syntax(&self.text) else {
            return Vec::new();
        };
        let wirings = statements.iter().filter_map(|s| match &s.kind {
            SyntaxStatementKind::Wiring(w) => Some((s, w)),
            _ => None,
        });
        let mut out = Vec::new();
        for (index, (statement, wiring)) in wirings.enumerate() {
            match &focused {
                Focused::Step(step) if *step == index => out.push((statement.start, statement.end)),
                Focused::Step(_) => {}
                Focused::Node(name) => {
                    for m in &wiring.sources {
                        if let SyntaxValue::Node(n) = &m.value {
                            if &n.value == name {
                                out.push((n.start, n.end));
                            }
                        }
                    }
                    for t in &wiring.targets {
                        if &t.node.value == name {
                            out.push((t.node.start, t.node.end));
                        }
                    }
                }
            }
        }
        out
    }

    fn draw(&mut self, frame: &mut Frame) -> Result<(), TuiError> {
        let theme = self.settings.theme();
        let area = frame.area();
        frame.buffer_mut().set_style(area, theme.canvas());
        if too_small(frame, &theme) {
            return Ok(());
        }
        let [top, body, footer] =
            Split::vertical([Constraint::Length(1), Constraint::Min(1), Constraint::Length(1)]).areas(area);
        self.draw_top(frame, top, &theme);
        let wide = body.width >= 120;
        let [graph_area, side] = if wide {
            Split::horizontal([Constraint::Percentage(60), Constraint::Percentage(40)]).areas(body)
        } else {
            Split::vertical([Constraint::Percentage(55), Constraint::Percentage(45)]).areas(body)
        };
        match machine_scene(&self.draft, &self.info) {
            Ok(scene) => self
                .graph
                .draw(frame.buffer_mut(), graph_area, &scene, &theme, 0, 0)
                .map_err(|e| TuiError::Inconsistent(format!("the machine cannot be laid out: {e}")))?,
            Err(SceneError::Empty) => {
                let hint = if self.spec.read_only {
                    "An empty machine."
                } else {
                    "An empty machine. A adds a step."
                };
                Paragraph::new(hint)
                    .style(theme.fg(Tone::Muted))
                    .render(graph_area.inner(Margin::new(2, 1)), frame.buffer_mut());
            }
            Err(e) => return Err(TuiError::Inconsistent(e.to_string())),
        }
        let block = Block::default()
            .borders(if wide { Borders::LEFT } else { Borders::TOP })
            .border_style(theme.rule())
            .style(theme.chrome());
        let inner = block.inner(side);
        block.render(side, frame.buffer_mut());
        if self.source_open {
            let marked = self.marked();
            self.source
                .draw(frame.buffer_mut(), inner, &theme, self.pane == Pane::Source, &marked);
        } else {
            self.draw_inspector(frame, inner, &theme);
        }
        self.draw_footer(frame, footer, &theme);
        if let Some((_, palette)) = &self.cap_palette {
            palette.draw(frame.buffer_mut(), body, &theme);
        }
        if let Some(palette) = &self.command_palette {
            palette.draw(frame.buffer_mut(), body, &theme);
        }
        if let Some((purpose, typed)) = &self.name_prompt {
            let matching: Vec<String> = self
                .draft
                .node_names()
                .into_iter()
                .filter(|n| n.starts_with(typed.as_str()))
                .collect();
            let line = format!(" {}: {typed}▏   {}", purpose.label(), matching.join("  "));
            banner(frame, body, &line, &theme, Tone::Accent);
        }
        if self.confirm_quit {
            let line = if self.source_dirty {
                " The source has changes not applied.  y quit without them · any other key stays"
            } else {
                " The machine has unsaved changes.  s save and quit · y quit without saving · any other key stays"
            };
            banner(frame, body, line, &theme, Tone::Warning);
        }
        if self.help {
            let w = (KEYS.iter().map(|l| text::width(l)).max().unwrap_or(0) as u16 + 4).min(body.width);
            let h = (KEYS.len() as u16 + 2).min(body.height);
            let card = Area::new(body.x + (body.width - w) / 2, body.y + (body.height - h) / 2, w, h);
            Clear.render(card, frame.buffer_mut());
            let block = Block::bordered().border_style(theme.fg(Tone::Border)).style(theme.chrome());
            let inner = block.inner(card);
            block.render(card, frame.buffer_mut());
            let lines: Vec<Line> = KEYS.iter().map(|l| Line::raw(format!(" {l}"))).collect();
            Paragraph::new(lines).render(inner, frame.buffer_mut());
        }
        Ok(())
    }

    fn draw_top(&self, frame: &mut Frame, top: Area, theme: &Theme) {
        let buffer = frame.buffer_mut();
        buffer.set_style(top, theme.chrome());
        let (state, tone) = if self.pending {
            ("checking…", Tone::Accent)
        } else if self.diagnostic.is_some() {
            ("does not resolve", Tone::Danger)
        } else if self.source_dirty {
            ("source not applied", Tone::Warning)
        } else {
            ("resolves", Tone::Success)
        };
        let right = format!(" {state} · {} steps ", self.draft.wirings.len());
        let right_width = text::width(&right) as u16;
        let mode = if self.spec.read_only { "view" } else { "edit" };
        let title = format!(
            " capdag {mode} ▸ {}{}",
            self.spec.path.display(),
            if self.dirty() { " ●" } else { "" }
        );
        let room = top.width.saturating_sub(right_width + 1) as usize;
        buffer.set_string(
            top.x,
            top.y,
            text::truncate_middle(&title, room),
            theme.chrome().patch(theme.fg(Tone::Ink)).add_modifier(Modifier::BOLD),
        );
        buffer.set_string(
            top.x + top.width.saturating_sub(right_width),
            top.y,
            right,
            theme.soft(tone).patch(theme.fg(tone)),
        );
    }

    fn draw_footer(&mut self, frame: &mut Frame, footer: Area, theme: &Theme) {
        let now = Instant::now();
        self.toasts.retain(|(_, _, at)| now.duration_since(*at) < TOAST_FOR);
        let (line, tone) = if let Some((t, tone, _)) = self.toasts.last() {
            (t.clone(), *tone)
        } else {
            let keys = match (self.pane, self.spec.read_only) {
                (Pane::Source, true) => "←↑↓→ move · esc graph · e hide",
                (Pane::Source, false) => "type to edit · ^S apply · esc graph · e hide",
                (Pane::Graph, true) => "←↑↓→ move · . , sources · e source · +/- detail · f fit · : commands · ? keys · q quit",
                (Pane::Graph, false) => {
                    "a add · A new step · d delete · r replace · i insert · s/o rewire · u/U undo · ^S save · x run · e source · ? keys · q quit"
                }
            };
            (keys.to_string(), Tone::Muted)
        };
        let buffer = frame.buffer_mut();
        buffer.set_style(footer, theme.chrome());
        buffer.set_string(
            footer.x + 1,
            footer.y,
            text::truncate(&line, footer.width.saturating_sub(2) as usize),
            theme.chrome().patch(theme.fg(tone)),
        );
    }

    fn draw_inspector(&self, frame: &mut Frame, area: Area, theme: &Theme) {
        let bold = |t: String, tone: Tone| Line::from(Span::styled(t, theme.fg(tone).add_modifier(Modifier::BOLD)));
        let plain = |t: String, tone: Tone| Line::from(Span::styled(t, theme.fg(tone)));
        let list = |names: std::collections::BTreeSet<String>| {
            if names.is_empty() {
                "none".to_string()
            } else {
                names.into_iter().collect::<Vec<_>>().join(", ")
            }
        };
        let mut lines: Vec<Line> = Vec::new();
        if let Some(diagnostic) = &self.diagnostic {
            lines.push(bold("Not a machine yet".to_string(), Tone::Danger));
            lines.push(plain(diagnostic.clone(), Tone::Ink));
            lines.push(Line::raw(""));
        }
        match self.focused() {
            None => {
                lines.push(plain(format!("{} steps", self.draft.wirings.len()), Tone::Secondary));
                lines.push(plain(format!("Inputs: {}", list(self.draft.input_names())), Tone::Source));
                lines.push(plain(format!("Outputs: {}", list(self.draft.output_names())), Tone::Target));
                lines.push(Line::raw(""));
                lines.push(plain("Focus a node or a step to inspect it.".to_string(), Tone::Muted));
            }
            Some(Focused::Step(w)) => {
                let Some(wiring) = self.draft.wirings.get(w) else {
                    return;
                };
                let cap = self.draft.cap_of(w).ok().flatten();
                let title = cap
                    .and_then(|c| self.info.cap_titles.get(c).cloned())
                    .unwrap_or_else(|| wiring.cap_alias.clone());
                lines.push(bold(title, Tone::Ink));
                match cap {
                    Some(cap) => lines.push(plain(cap.to_string(), Tone::Muted)),
                    None => lines.push(plain(format!("'{}' is an included machine", wiring.cap_alias), Tone::Muted)),
                }
                let policy = wiring.policy.to_notation();
                if !policy.is_empty() {
                    lines.push(plain(policy, Tone::Warning));
                }
                lines.push(Line::raw(""));
                lines.push(bold("Sources".to_string(), Tone::Ink));
                for (k, member) in wiring.sources.iter().enumerate() {
                    let value = match &member.value {
                        DraftValue::Node(v) => v.clone(),
                        DraftValue::Constant(c) => c.to_string(),
                    };
                    let shown = match &member.name {
                        Some(n) => format!("{n} = {value}"),
                        None => value,
                    };
                    let selected = k == self.selected_source;
                    lines.push(Line::from(Span::styled(
                        format!("{} {shown}", if selected { '›' } else { ' ' }),
                        if selected {
                            theme.soft(Tone::Accent).patch(theme.fg(Tone::Ink))
                        } else {
                            theme.fg(Tone::Secondary)
                        },
                    )));
                }
                lines.push(Line::raw(""));
                lines.push(bold("Outputs".to_string(), Tone::Ink));
                for target in &wiring.targets {
                    lines.push(plain(
                        match &target.name {
                            Some(n) => format!("  {n} → {}", target.node),
                            None => format!("  {}", target.node),
                        },
                        Tone::Secondary,
                    ));
                }
            }
            Some(Focused::Node(name)) => {
                lines.push(bold(name.clone(), Tone::Ink));
                if let Some((urn, title)) = self.info.media_of.get(&name) {
                    lines.push(plain(title.clone(), Tone::Secondary));
                    lines.push(plain(urn.to_string(), Tone::Muted));
                }
                let makers: Vec<&str> = self
                    .draft
                    .wirings
                    .iter()
                    .filter(|w| w.targets.iter().any(|t| t.node == name))
                    .map(|w| w.cap_alias.as_str())
                    .collect();
                let readers: Vec<&str> = self
                    .draft
                    .wirings
                    .iter()
                    .filter(|w| w.source_nodes().contains(&name.as_str()))
                    .map(|w| w.cap_alias.as_str())
                    .collect();
                lines.push(Line::raw(""));
                lines.push(plain(
                    if makers.is_empty() {
                        "An input of the machine".to_string()
                    } else {
                        format!("Made by {}", makers.join(", "))
                    },
                    Tone::Secondary,
                ));
                lines.push(plain(
                    if readers.is_empty() {
                        "An output of the machine".to_string()
                    } else {
                        format!("Read by {}", readers.join(", "))
                    },
                    Tone::Secondary,
                ));
            }
        }
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .render(area.inner(Margin::new(1, 0)), frame.buffer_mut());
    }
}

/// One line across the middle of `area`, on a tone's soft ground.
fn banner(frame: &mut Frame, area: Area, line: &str, theme: &Theme, tone: Tone) {
    let width = area.width.saturating_sub(2) as usize;
    frame.buffer_mut().set_string(
        area.x + 1,
        area.y + area.height / 2,
        text::pad(&text::truncate(line, width), width),
        theme.soft(tone).patch(theme.fg(Tone::Ink)).add_modifier(Modifier::BOLD),
    );
}

/// Edit (or view) a machine until the user closes the editor.
pub async fn edit_session(spec: EditorSpec, settings: TuiSettings) -> Result<EditorOutcome, TuiError> {
    let mut tui = Tui::enter()?;
    edit_session_on(&mut tui, EventStream::new(), spec, settings).await
}

/// [`edit_session`] on any display, reading events from `input`.
pub async fn edit_session_on<D, S>(
    display: &mut D,
    mut input: S,
    spec: EditorSpec,
    settings: TuiSettings,
) -> Result<EditorOutcome, TuiError>
where
    D: Display,
    S: futures_util::Stream<Item = std::io::Result<Event>> + Unpin,
{
    let text = spec.text.clone();
    let draft = MachineDraft::parse(&text)
        .map_err(|e| TuiError::Inconsistent(format!("{} is not machine notation: {e}", spec.path.display())))?;
    // Text that does not resolve still opens: the editor is where it is
    // made to. An empty machine is a start, with nothing to resolve yet.
    let (info, diagnostic) = if draft.wirings.is_empty() {
        (DraftInfo::default(), None)
    } else {
        match resolve(&text, spec.registry.clone(), spec.machines.as_ref(), spec.machine_id.as_deref(), &spec.catalog).await {
            Ok(info) => (info, None),
            Err(why) => (DraftInfo::default(), Some(why)),
        }
    };
    let (results, mut verdicts) = mpsc::unbounded_channel();
    let mut editor = Editor {
        settings,
        saved_text: text.clone(),
        saved_once: false,
        source: SourceBuffer::new(&text),
        text,
        draft,
        info,
        diagnostic,
        undo: Vec::new(),
        redo: Vec::new(),
        graph: GraphView::default(),
        pane: Pane::Graph,
        source_open: false,
        source_dirty: false,
        selected_source: 0,
        cap_palette: None,
        name_prompt: None,
        command_palette: None,
        toasts: Vec::new(),
        confirm_quit: false,
        help: false,
        pending: false,
        quit: None,
        results,
        spec,
    };
    // Toasts expire on their own: a tick while one shows.
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let mut drawn = Ok(());
        display.draw(|frame| drawn = editor.draw(frame))?;
        drawn?;
        if let Some(outcome) = editor.quit.take() {
            return Ok(outcome);
        }
        tokio::select! {
            event = input.next() => match event {
                Some(Ok(Event::Key(key))) => editor.on_key(key),
                Some(Ok(Event::Mouse(mouse))) => {
                    let before = editor.graph.focus.clone();
                    editor.graph.on_mouse(mouse);
                    if editor.graph.focus != before {
                        editor.selected_source = 0;
                    }
                }
                Some(Ok(Event::Paste(value))) => {
                    if let Some((_, typed)) = &mut editor.name_prompt {
                        typed.push_str(value.trim());
                    } else if editor.pane == Pane::Source && !editor.spec.read_only {
                        editor.source.paste(&value);
                        editor.source_dirty = editor.source.text() != editor.text;
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e.into()),
                // The input ended: nothing more can be asked of the editor.
                None => return Ok(EditorOutcome::Closed { saved: editor.saved_once }),
            },
            Some(verdict) = verdicts.recv() => editor.settle(verdict)?,
            _ = tick.tick(), if !editor.toasts.is_empty() => {}
        }
    }
}
