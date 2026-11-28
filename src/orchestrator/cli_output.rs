//! CLI output emission — the pipe-discipline contract of `capdag run`.
//!
//! - Exactly ONE terminal, scalar, single item, and no `--output` dir given
//!   → the item's raw bytes go to STDOUT (logs/progress are on stderr), so
//!   `capdag run summary.machine doc.pdf | wc -w` behaves like a Unix tool.
//! - Anything else (sequence output, several terminals, or an explicit
//!   `--output`) → each item is written as a FILE in the output directory
//!   (default: the current directory), named `{title}.{node}.{ext}` —
//!   `{title}` what the item descends from, as every front-end names what a
//!   run makes ([`crate::orchestrator::lineage`]): the inputs it was made of,
//!   joined, and for the items of a sequence that descend from one thing,
//!   their position among them (`report · 2`); `{node}` the name the
//!   machine's notation gives the output — and every written absolute path
//!   is printed to stdout, one per line.
//! - An existing file is a hard error unless `--force` — silent overwrites
//!   destroy user data. A refused emission hands back everything it did not
//!   write, with what each item holds ([`EmitRefusal`]): the caller gives it
//!   to the person another way, and nothing a run made is thrown away.
//! - A terminal that was not produced writes nothing here: it is not an empty
//!   file. The caller says so, from the result.
//!
//! The extension derives deterministically from the terminal's media URN —
//! `ext=<v>` wins (the declared file type), else `fmt=<v>` (serialization),
//! else `enc=utf8` ⇒ `txt`, else `bin`. This is the defined naming scheme
//! of the CLI, not a fallback chain: every media URN maps to exactly one
//! name by these rules.

use std::io::Write;
use std::path::{Path, PathBuf};

use crate::orchestrator::execute_plan::{ForEachBodyCoordinate, PipelineResult, ProducedTerminal};
use crate::orchestrator::lineage::{lineage_title, sequence_item_titles, RunLineage};
use crate::planner::MachinePlan;
use crate::urn::media_urn::MediaUrn;

/// Options for [`emit_terminals`].
pub struct EmitOptions {
    /// Where files go. `None` = stdout is allowed for the single-scalar
    /// case and files default to the current directory otherwise.
    pub output_dir: Option<PathBuf>,
    /// Overwrite existing files instead of hard-erroring.
    pub force: bool,
    /// What the produced files are named after: what each descends from.
    pub naming: OutputNaming,
    /// The name the notation gives each output, by plan `Output` node id: the
    /// `{node}` of a produced file's name. Every terminal of the result must
    /// have one — a file is never named after a plan's internal node id,
    /// which changes whenever the machine is edited.
    pub output_names: std::collections::HashMap<String, String>,
}

/// What a run's files are named after: the plan it ran, the sources it gave
/// each input slot, and what each source is called.
#[derive(Debug, Clone)]
pub struct OutputNaming {
    /// The plan the result is of: what each output descends from is read
    /// from it.
    pub plan: MachinePlan,
    /// What the run gave each input slot: positions among `sources`, in the
    /// order given.
    pub slots: Vec<(String, Vec<usize>)>,
    /// What each source of the run is called — a file by its stem — by
    /// position.
    pub sources: Vec<String>,
    /// What a file that reads no input is called: the machine's name.
    pub machine_name: String,
}

/// File extension for a terminal's media URN, per the naming scheme above.
pub fn extension_for_media(media_urn: &str) -> String {
    let Ok(parsed) = MediaUrn::from_string(media_urn) else {
        return "bin".to_string();
    };
    if let Some(ext) = parsed.exact_value("ext") {
        return ext.to_string();
    }
    if let Some(fmt) = parsed.exact_value("fmt") {
        return fmt.to_string();
    }
    if parsed.get_tag("enc").is_some() {
        return "txt".to_string();
    }
    "bin".to_string()
}

/// What a refused emission leaves: why it was refused, what it wrote before,
/// and every item it did not write — the one refused and those after it —
/// with what the item holds, so the caller can give it to the person another
/// way. Nothing a run made is thrown away because it could not be written
/// where it was asked to be.
#[derive(Debug)]
pub struct EmitRefusal {
    /// Why: what could not be done, and to what.
    pub reason: String,
    /// What was written before the refusal: the plan `Output` node id and
    /// the file's absolute path.
    pub written: Vec<(String, PathBuf)>,
    /// What was not written, in the order it would have been.
    pub unsaved: Vec<UnsavedItem>,
}

/// One item a run produced and an emission did not write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsavedItem {
    /// The plan `Output` node id it is an item of.
    pub output_node_id: String,
    /// The name it would have been written under, by the naming scheme.
    pub file_name: String,
    /// What it is.
    pub media_urn: String,
    pub content: UnsavedContent,
}

/// Where an unsaved item's bytes are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnsavedContent {
    /// In memory.
    Bytes(Vec<u8>),
    /// In a part-file the CLI's disk writer persisted, left where it is: the
    /// caller owns it now, and removes it once it has given it on.
    Part(PathBuf),
}

/// Whether an item of `media_urn` is text: it declares its text encoding
/// (`enc=`), so its bytes read as lines a person can be shown.
pub fn is_text_media(media_urn: &str) -> bool {
    MediaUrn::from_string(media_urn).is_ok_and(|parsed| parsed.get_tag("enc").is_some())
}

/// Emit a pipeline result per the pipe-discipline contract. Returns the
/// absolute paths of files written (empty when the result went to stdout).
/// `stdout` is injected so tests can capture it.
pub fn emit_terminals(
    result: &PipelineResult,
    options: &EmitOptions,
    stdout: &mut dyn Write,
) -> Result<Vec<PathBuf>, EmitRefusal> {
    Ok(emit_terminals_by_output(result, options, stdout)?
        .into_iter()
        .map(|(_, path)| path)
        .collect())
}

/// One item to emit: under which name, and where its bytes are.
struct Planned<'a> {
    output_node_id: &'a str,
    file_name: String,
    media_urn: &'a str,
    content: PlannedContent<'a>,
}

enum PlannedContent<'a> {
    Bytes(&'a [u8]),
    Part(&'a str),
}

impl Planned<'_> {
    fn unsaved(&self) -> UnsavedItem {
        UnsavedItem {
            output_node_id: self.output_node_id.to_string(),
            file_name: self.file_name.clone(),
            media_urn: self.media_urn.to_string(),
            content: match self.content {
                PlannedContent::Bytes(data) => UnsavedContent::Bytes(data.to_vec()),
                PlannedContent::Part(part) => UnsavedContent::Part(PathBuf::from(part)),
            },
        }
    }
}

/// [`emit_terminals`], saying which output each written file is of: the
/// plan `Output` node id and the file's absolute path, in the order they were
/// written. Empty when the result went to stdout.
pub fn emit_terminals_by_output(
    result: &PipelineResult,
    options: &EmitOptions,
    stdout: &mut dyn Write,
) -> Result<Vec<(String, PathBuf)>, EmitRefusal> {
    let produced: Vec<&ProducedTerminal> = result
        .terminals
        .iter()
        .filter_map(|terminal| terminal.produced())
        .collect();
    // Every item, under the name the scheme gives it, in the order it is
    // written: in memory, or a persisted part-file (the CLI's disk writer, L16).
    // An output with no name is planned under its node id, and refused
    // below if it is to be a file: a file is never named after a plan's
    // internal node id.
    let source_ids: Vec<String> = (0..options.naming.sources.len()).map(|at| at.to_string()).collect();
    let lineage = RunLineage::new(&options.naming.plan, &options.naming.slots, &source_ids);
    let mut planned: Vec<Planned<'_>> = Vec::new();
    let mut unnamed: Option<&str> = None;
    // What the items descend from that could not be told: the run and its
    // plan disagree, and its items are handed back under their place.
    let mut untold: Option<String> = None;
    for terminal in &produced {
        let ext = extension_for_media(&terminal.media_urn);
        let node = match options.output_names.get(&terminal.output_node_id) {
            Some(node) => Some(node.as_str()),
            None => {
                unnamed.get_or_insert(&terminal.output_node_id);
                None
            }
        };
        let contents: Vec<(PlannedContent<'_>, Option<&ForEachBodyCoordinate>)> = if terminal.writer_results.is_empty() {
            terminal.items.iter().map(|item| (PlannedContent::Bytes(&item.data), None)).collect()
        } else {
            terminal
                .writer_results
                .iter()
                .flat_map(|w| w.saved_paths.iter().map(move |part| (PlannedContent::Part(part), w.body_coordinate.as_ref())))
                .collect()
        };
        let multi_item = terminal.is_sequence || contents.len() > 1;
        let told: Result<Vec<String>, String> = match (&lineage, node) {
            (Ok(lineage), Some(_)) => contents
                .iter()
                .map(|(_, coordinate)| {
                    lineage.sources_of(&terminal.output_node_id, *coordinate).map(|ids| {
                        let titles: Vec<&str> = ids
                            .iter()
                            .map(|id| options.naming.sources[id.parse::<usize>().expect("a source id is its position")].as_str())
                            .collect();
                        lineage_title(&titles, &options.naming.machine_name)
                    })
                })
                .collect(),
            (Err(reason), Some(_)) => Err(reason.clone()),
            (_, None) => Ok(contents.iter().map(|_| terminal.output_node_id.clone()).collect()),
        };
        let titles = match told {
            Ok(titles) if multi_item => sequence_item_titles(&titles),
            Ok(titles) => titles,
            Err(reason) => {
                untold.get_or_insert(reason);
                (0..contents.len()).map(|index| format!("{}.{index}", terminal.output_node_id)).collect()
            }
        };
        let node = node.unwrap_or(terminal.output_node_id.as_str());
        for ((content, _), title) in contents.into_iter().zip(titles) {
            planned.push(Planned {
                output_node_id: &terminal.output_node_id,
                file_name: format!("{title}.{node}.{ext}"),
                media_urn: &terminal.media_urn,
                content,
            });
        }
    }
    let refused = |reason: String, written: Vec<(String, PathBuf)>, rest: &[Planned<'_>]| EmitRefusal {
        reason,
        written,
        unsaved: rest.iter().map(Planned::unsaved).collect(),
    };
    if let Some(reason) = untold {
        return Err(refused(format!("what the results descend from could not be told: {reason}"), Vec::new(), &planned));
    }
    // The stdout fast path: one terminal, scalar, one item, no explicit dir.
    // Persisted, its bytes stream to stdout and the part is removed, so the
    // pipe contract is identical either way.
    if let (None, 1, [terminal], [item]) =
        (&options.output_dir, result.terminals.len(), produced.as_slice(), planned.as_slice())
    {
        if !terminal.is_sequence {
            let streamed = match item.content {
                PlannedContent::Bytes(data) => stdout.write_all(data),
                PlannedContent::Part(part) => {
                    std::fs::File::open(part).and_then(|mut file| std::io::copy(&mut file, stdout).map(|_| ()))
                }
            }
            .and_then(|()| stdout.flush());
            if let Err(e) = streamed {
                return Err(refused(format!("failed to write the result to stdout: {e}"), Vec::new(), &planned));
            }
            if let PlannedContent::Part(part) = item.content {
                std::fs::remove_file(part)
                    .map_err(|e| refused(format!("failed to remove part file '{part}': {e}"), Vec::new(), &[]))?;
            }
            return Ok(Vec::new());
        }
    }

    // A plan's every output is one the notation named. What the named
    // outputs hold is handed back; an unnamed output's items are not, since
    // keeping them would name them after the plan's internal node id.
    if let Some(node) = unnamed {
        let named: Vec<Planned<'_>> = planned
            .into_iter()
            .filter(|item| options.output_names.contains_key(item.output_node_id))
            .collect();
        return Err(refused(format!("output '{node}' has no name to write it under"), Vec::new(), &named));
    }
    let dir = options
        .output_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("."));
    if let Err(e) = std::fs::create_dir_all(&dir) {
        return Err(refused(format!("failed to create output dir {dir:?}: {e}"), Vec::new(), &planned));
    }
    let mut written: Vec<(String, PathBuf)> = Vec::new();
    for (at, item) in planned.iter().enumerate() {
        let path = dir.join(&item.file_name);
        if path.exists() && !options.force {
            return Err(refused(
                format!("refusing to overwrite existing file {path:?} (pass --force to allow)"),
                written,
                &planned[at..],
            ));
        }
        // A persisted part lives in the destination directory: the rename
        // is same-filesystem and never re-buffers the data.
        let moved = match item.content {
            PlannedContent::Bytes(data) => std::fs::write(&path, data).map_err(|e| format!("failed to write {path:?}: {e}")),
            PlannedContent::Part(part) => std::fs::rename(part, &path)
                .map_err(|e| format!("failed to move persisted result '{part}' to {path:?}: {e}")),
        };
        if let Err(reason) = moved {
            return Err(refused(reason, written, &planned[at..]));
        }
        let absolute = absolutize(&path);
        written.push((item.output_node_id.to_string(), absolute.clone()));
        if let Err(e) = writeln!(stdout, "{}", absolute.display()) {
            return Err(refused(format!("failed to report written path: {e}"), written, &planned[at + 1..]));
        }
    }
    if let Err(e) = stdout.flush() {
        return Err(refused(format!("failed to flush stdout: {e}"), written, &[]));
    }
    Ok(written)
}

fn absolutize(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::execute_plan::{OutputItem, PlanTerminal, RunVerdict};

    fn terminal(node: &str, media: &str, is_sequence: bool, items: Vec<&[u8]>) -> PlanTerminal {
        PlanTerminal::Produced(ProducedTerminal {
            output_node_id: node.to_string(),
            items: items
                .into_iter()
                .enumerate()
                .map(|(index, data)| OutputItem {
                    index,
                    data: data.to_vec(),
                })
                .collect(),
            is_sequence,
            media_urn: media.to_string(),
            writer_results: Vec::new(),
        })
    }

    /// The names the test machines give their outputs: each node id here is
    /// its own name.
    fn names() -> std::collections::HashMap<String, String> {
        ["output", "kept", "rejected", "thumb", "text"]
            .into_iter()
            .map(|name| (name.to_string(), name.to_string()))
            .collect()
    }

    /// What files of a run of one source are named after: a plan whose
    /// input slot `in` is given the source, read by one step, whose main
    /// output every output of these tests is.
    fn naming(source: &str) -> OutputNaming {
        use crate::planner::{InputCardinality, MachineNode, MachinePlanEdge};
        let mut plan = MachinePlan::new("test");
        plan.add_node(MachineNode::input_slot("in", "in", "media:", InputCardinality::Single));
        plan.add_node(MachineNode::cap("step", "cap:in=media:;step;out=media:"));
        plan.add_edge(MachinePlanEdge::direct("in", "step"));
        for output in ["output", "kept", "rejected", "thumb", "text", "output_7", "output_3"] {
            plan.add_node(MachineNode::output(output, output, "step"));
            plan.add_edge(MachinePlanEdge::direct("step", output));
        }
        OutputNaming {
            plan,
            slots: vec![("in".to_string(), vec![0])],
            sources: vec![source.to_string()],
            machine_name: "machine".to_string(),
        }
    }

    fn result_of(terminals: Vec<PlanTerminal>) -> PipelineResult {
        PipelineResult {
            terminals,
            body_outcomes: Vec::new(),
            steps: Vec::new(),
            verdict: RunVerdict::Completed,
        }
    }

    // TEST8042: the emission contract — single scalar terminal streams raw
    // bytes to stdout; sequences/multi-terminal results write files with the
    // contract names and list their paths on stdout; an existing file
    // without --force is a hard error and --force overwrites.
    #[test]
    fn test8042_emit_terminals_contract() {
        // 1. Single scalar → raw stdout bytes, no files.
        let result = result_of(vec![terminal(
            "output",
            "media:enc=utf8;summary",
            false,
            vec![b"the summary text"],
        )]);
        let mut stdout: Vec<u8> = Vec::new();
        let written = emit_terminals(
            &result,
            &EmitOptions {
                output_dir: None,
                force: false,
                naming: naming("doc"),
                output_names: names(),
            },
            &mut stdout,
        )
        .unwrap();
        assert!(written.is_empty());
        assert_eq!(stdout, b"the summary text");

        // 2. Sequence → one file per item, named after what it descends
        //    from and its place among the items that share it:
        //    {title · k}.{node}.{ext}; paths listed on stdout.
        let dir = tempfile::tempdir().unwrap();
        let result = result_of(vec![terminal(
            "output",
            "media:ext=png;image",
            true,
            vec![b"png0", b"png1"],
        )]);
        let mut stdout: Vec<u8> = Vec::new();
        let written = emit_terminals(
            &result,
            &EmitOptions {
                output_dir: Some(dir.path().to_path_buf()),
                force: false,
                naming: naming("doc"),
                output_names: names(),
            },
            &mut stdout,
        )
        .unwrap();
        assert_eq!(written.len(), 2);
        assert!(written[0].ends_with("doc · 1.output.png"), "{written:?}");
        assert!(written[1].ends_with("doc · 2.output.png"), "{written:?}");
        assert_eq!(std::fs::read(&written[0]).unwrap(), b"png0");
        let listing = String::from_utf8(stdout).unwrap();
        assert_eq!(listing.lines().count(), 2, "one path per line: {listing}");

        // 3. Multi-terminal (fan-out) → files even for scalars, one per
        //    terminal, extension from each terminal's own media.
        let dir = tempfile::tempdir().unwrap();
        let result = result_of(vec![
            terminal("thumb", "media:ext=png;image", false, vec![b"png"]),
            terminal("text", "media:enc=utf8", false, vec![b"txt"]),
        ]);
        let mut stdout: Vec<u8> = Vec::new();
        let written = emit_terminals(
            &result,
            &EmitOptions {
                output_dir: Some(dir.path().to_path_buf()),
                force: false,
                naming: naming("doc"),
                output_names: names(),
            },
            &mut stdout,
        )
        .unwrap();
        assert_eq!(written.len(), 2);
        assert!(written[0].ends_with("doc.thumb.png"), "{written:?}");
        assert!(written[1].ends_with("doc.text.txt"), "{written:?}");

        // 4. Existing file without --force = hard error; with --force it
        //    overwrites.
        let mut stdout: Vec<u8> = Vec::new();
        let err = emit_terminals(
            &result,
            &EmitOptions {
                output_dir: Some(dir.path().to_path_buf()),
                force: false,
                naming: naming("doc"),
                output_names: names(),
            },
            &mut stdout,
        )
        .unwrap_err();
        assert!(err.reason.contains("refusing to overwrite"), "{}", err.reason);
        let mut stdout: Vec<u8> = Vec::new();
        emit_terminals(
            &result,
            &EmitOptions {
                output_dir: Some(dir.path().to_path_buf()),
                force: true,
                naming: naming("doc"),
                output_names: names(),
            },
            &mut stdout,
        )
        .expect("--force must overwrite");

        // 5. Extension derivation: ext= wins over fmt=; enc= alone ⇒ txt;
        //    no axis ⇒ bin.
        assert_eq!(extension_for_media("media:document;enc=utf8;ext=pdf;fmt=json"), "pdf");
        assert_eq!(extension_for_media("media:enc=utf8;fmt=ndjson;record"), "ndjson");
        assert_eq!(extension_for_media("media:enc=utf8;summary"), "txt");
        assert_eq!(extension_for_media("media:embedding-vector"), "bin");
    }

    use crate::orchestrator::execute_plan::WriterResult;

    fn persisted_terminal(
        node: &str,
        media: &str,
        is_sequence: bool,
        parts: Vec<String>,
    ) -> PlanTerminal {
        PlanTerminal::Produced(ProducedTerminal {
            output_node_id: node.to_string(),
            items: Vec::new(),
            is_sequence,
            media_urn: media.to_string(),
            writer_results: vec![WriterResult {
                is_sequence,
                media_urn: media.to_string(),
                saved_paths: parts,
                total_bytes: 0,
                stream_meta: None,
                item_metas: Vec::new(),
                body_coordinate: None,
            }],
        })
    }

    // TEST8073: a file a run of a map writes is named after the item that run
    // read — each document of a sequence its own file — and the items one run
    // writes from one document after it and their place among them; an output
    // that gathers every document is named after all of them, joined.
    #[test]
    fn test8073_files_of_a_map_are_named_after_the_item_each_run_read() {
        use crate::planner::{ForEachSource, InputCardinality, MachineNode, MachinePlanEdge, MapRun, StepToken};
        let mut plan = MachinePlan::new("docs");
        plan.add_node(MachineNode::input_slot("in_docs", "docs", "media:pdf", InputCardinality::Sequence));
        plan.add_node(MachineNode::for_each_token(
            "map",
            vec![ForEachSource { node: "in_docs".into(), item: "doc".into() }],
            vec!["doc".into(), "split".into()],
            "tok-map".parse::<StepToken>().unwrap(),
        ));
        plan.add_node(MachineNode::item("doc", "map", "in_docs", "media:pdf"));
        plan.add_node(MachineNode::cap("split", "cap:in=media:pdf;split;out=media:txt"));
        plan.add_edge(MachinePlanEdge::direct("doc", "split"));
        plan.add_node(MachineNode::output("out_pages", "pages", "split"));
        plan.add_edge(MachinePlanEdge::direct("split", "out_pages"));
        plan.add_node(MachineNode::collect("gather", vec!["split".into()], "media:enc=utf8"));
        plan.add_node(MachineNode::cap("join", "cap:in=media:txt;join;out=media:txt"));
        plan.add_edge(MachinePlanEdge::direct("gather", "join"));
        plan.add_node(MachineNode::output("out_all", "all", "join"));
        plan.add_edge(MachinePlanEdge::direct("join", "out_all"));
        let naming = OutputNaming {
            plan,
            slots: vec![("in_docs".to_string(), vec![0, 1])],
            sources: vec!["letter".to_string(), "invoice".to_string()],
            machine_name: "docs".to_string(),
        };
        let run = |index: usize| ForEachBodyCoordinate {
            foreach_token_id: "tok-map".parse().unwrap(),
            body_index: index,
            enclosing: Vec::<MapRun>::new(),
        };
        let dir = tempfile::tempdir().unwrap();
        let part = |name: &str| write_part(dir.path(), name, name.as_bytes());
        let pages = |index: usize, parts: Vec<String>| WriterResult {
            is_sequence: true,
            media_urn: "media:enc=utf8".to_string(),
            saved_paths: parts,
            total_bytes: 0,
            stream_meta: None,
            item_metas: Vec::new(),
            body_coordinate: Some(run(index)),
        };
        let result = result_of(vec![
            PlanTerminal::Produced(ProducedTerminal {
                output_node_id: "out_pages".to_string(),
                items: Vec::new(),
                is_sequence: true,
                media_urn: "media:enc=utf8".to_string(),
                writer_results: vec![
                    pages(0, vec![part(".p.0.0"), part(".p.0.1")]),
                    pages(1, vec![part(".p.1.0")]),
                ],
            }),
            persisted_terminal("out_all", "media:enc=utf8", false, vec![part(".p.all")]),
        ]);
        let output_names = [("out_pages", "pages"), ("out_all", "all")]
            .into_iter()
            .map(|(id, name)| (id.to_string(), name.to_string()))
            .collect();
        let written = emit_terminals(
            &result,
            &EmitOptions { output_dir: Some(dir.path().to_path_buf()), force: false, naming, output_names },
            &mut Vec::new(),
        )
        .unwrap();
        let names: Vec<String> = written.iter().map(|p| p.file_name().unwrap().to_string_lossy().into_owned()).collect();
        assert_eq!(
            names,
            vec!["letter · 1.pages.txt", "letter · 2.pages.txt", "invoice.pages.txt", "letter + invoice.all.txt"]
        );
    }

    // TEST12792: a produced file is named for the name the notation gives the
    // output, not for the plan's node id; and a terminal with no name is
    // refused rather than written under an id nobody chose. The refusal hands
    // back what the named outputs hold, and nothing under an unnamed id.
    #[test]
    fn test12792_a_file_is_named_for_the_outputs_written_name() {
        let dir = tempfile::tempdir().unwrap();
        let result = result_of(vec![
            terminal("output_7", "media:enc=utf8;ext=md;plain-text", false, vec![b"# summary"]),
            terminal("output_3", "media:ext=png;image", true, vec![b"p0", b"p1"]),
        ]);
        let options = |output_names: &[(&str, &str)]| EmitOptions {
            output_dir: Some(dir.path().to_path_buf()),
            force: false,
            naming: naming("doc"),
            output_names: output_names
                .iter()
                .map(|(id, name)| (id.to_string(), name.to_string()))
                .collect(),
        };
        let mut stdout: Vec<u8> = Vec::new();
        let written = emit_terminals(
            &result,
            &options(&[("output_7", "summary"), ("output_3", "pages")]),
            &mut stdout,
        )
        .unwrap();
        let mut names: Vec<String> = written
            .iter()
            .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(names, vec!["doc · 1.pages.png", "doc · 2.pages.png", "doc.summary.md"]);

        let other = tempfile::tempdir().unwrap();
        let mut unnamed = options(&[("output_7", "summary")]);
        unnamed.output_dir = Some(other.path().to_path_buf());
        let error = emit_terminals(&result, &unnamed, &mut Vec::new()).unwrap_err();
        assert_eq!(error.reason, "output 'output_3' has no name to write it under");
        assert!(error.written.is_empty(), "nothing is written when an output has no name");
        let handed_back: Vec<&str> = error.unsaved.iter().map(|item| item.file_name.as_str()).collect();
        assert_eq!(handed_back, vec!["doc.summary.md"], "only the named output is handed back");
        assert_eq!(error.unsaved[0].content, UnsavedContent::Bytes(b"# summary".to_vec()));
    }

    // TEST12761: a terminal that was not produced writes nothing — no file,
    // no bytes on stdout — and is not what makes a result "one terminal": a
    // produced terminal beside it still goes to a file, by its contract name.
    #[test]
    fn test12761_a_terminal_not_produced_writes_nothing() {
        let absent = || PlanTerminal::NotProduced {
            output_node_id: "rejected".to_string(),
            media_urn: "media:enc=utf8".to_string(),
        };
        let options = |dir: Option<&Path>| EmitOptions {
            output_dir: dir.map(Path::to_path_buf),
            force: false,
            naming: naming("doc"),
            output_names: names(),
        };

        // Alone: nothing at all.
        let mut stdout: Vec<u8> = Vec::new();
        let written = emit_terminals(&result_of(vec![absent()]), &options(None), &mut stdout).unwrap();
        assert!(written.is_empty());
        assert!(stdout.is_empty(), "nothing was produced, so nothing is printed");

        // Beside a produced scalar: the result has two terminals, so the
        // produced one is a file — not the stdout pipe case.
        let dir = tempfile::tempdir().unwrap();
        let result = result_of(vec![
            terminal("kept", "media:enc=utf8;ext=txt;plain-text", false, vec![b"kept text"]),
            absent(),
        ]);
        let mut stdout: Vec<u8> = Vec::new();
        let written = emit_terminals(&result, &options(Some(dir.path())), &mut stdout).unwrap();
        assert_eq!(written.len(), 1);
        assert_eq!(written[0].file_name().unwrap(), "doc.kept.txt");
        assert_eq!(std::fs::read(&written[0]).unwrap(), b"kept text");
        let listed: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
        assert_eq!(listed.len(), 1, "no file for the terminal that was not produced");
    }

    fn write_part(dir: &Path, name: &str, data: &[u8]) -> String {
        let p = dir.join(name);
        std::fs::write(&p, data).unwrap();
        p.to_string_lossy().into_owned()
    }

    // TEST8144: persisted terminals (the CLI disk writer path, L16) obey the
    // same emission contract as in-memory items — a persisted sequence's
    // part-files are RENAMED to contract names with paths on stdout; a single
    // persisted scalar with no --output streams its BYTES to stdout and the
    // part-file disappears; an overwrite refusal leaves the unrenamed parts
    // where they are and hands them back, under the names they would have had.
    #[test]
    fn test8144_emit_persisted_terminals_contract() {
        // 1. Persisted sequence → renamed contract files, paths on stdout.
        let dir = tempfile::tempdir().unwrap();
        let parts = vec![
            write_part(dir.path(), ".capdag-part.0.cap_1.0", b"png0"),
            write_part(dir.path(), ".capdag-part.0.cap_1.1", b"png1"),
        ];
        let result = result_of(vec![persisted_terminal(
            "output",
            "media:ext=png;image",
            true,
            parts.clone(),
        )]);
        let mut stdout: Vec<u8> = Vec::new();
        let written = emit_terminals(
            &result,
            &EmitOptions {
                output_dir: Some(dir.path().to_path_buf()),
                force: false,
                naming: naming("cam"),
                output_names: names(),
            },
            &mut stdout,
        )
        .unwrap();
        assert_eq!(written.len(), 2);
        assert!(written[0].ends_with("cam · 1.output.png"), "{written:?}");
        assert_eq!(std::fs::read(&written[0]).unwrap(), b"png0");
        assert_eq!(std::fs::read(&written[1]).unwrap(), b"png1");
        assert!(
            !Path::new(&parts[0]).exists() && !Path::new(&parts[1]).exists(),
            "part-files are renamed away, not copied"
        );
        let listing = String::from_utf8(stdout).unwrap();
        assert_eq!(listing.lines().count(), 2, "one path per line: {listing}");

        // 2. Single persisted scalar, no --output → raw bytes to stdout, part
        //    removed (identical pipe contract to the in-memory fast path).
        let dir = tempfile::tempdir().unwrap();
        let part = write_part(dir.path(), ".capdag-part.1.cap_1.blob", b"wav-bytes");
        let result = result_of(vec![persisted_terminal(
            "output",
            "media:audio;ext=wav",
            false,
            vec![part.clone()],
        )]);
        let mut stdout: Vec<u8> = Vec::new();
        let written = emit_terminals(
            &result,
            &EmitOptions {
                output_dir: None,
                force: false,
                naming: naming("mic"),
                output_names: names(),
            },
            &mut stdout,
        )
        .unwrap();
        assert!(written.is_empty());
        assert_eq!(stdout, b"wav-bytes");
        assert!(!Path::new(&part).exists(), "streamed part is removed");

        // 3. Persisted scalar WITH --output → renamed to the scalar contract name.
        let dir = tempfile::tempdir().unwrap();
        let part = write_part(dir.path(), ".capdag-part.2.cap_1.blob", b"wav-bytes");
        let result = result_of(vec![persisted_terminal(
            "output",
            "media:audio;ext=wav",
            false,
            vec![part],
        )]);
        let mut stdout: Vec<u8> = Vec::new();
        let written = emit_terminals(
            &result,
            &EmitOptions {
                output_dir: Some(dir.path().to_path_buf()),
                force: false,
                naming: naming("mic"),
                output_names: names(),
            },
            &mut stdout,
        )
        .unwrap();
        assert_eq!(written.len(), 1);
        assert!(written[0].ends_with("mic.output.wav"), "{written:?}");
        assert_eq!(std::fs::read(&written[0]).unwrap(), b"wav-bytes");

        // 4. An overwrite refusal mid-emit hands back the not-yet-renamed
        //    parts, left where they are, and the error names --force.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("mic · 1.output.wav"), b"precious").unwrap();
        let parts = vec![
            write_part(dir.path(), ".capdag-part.3.cap_1.0", b"a"),
            write_part(dir.path(), ".capdag-part.3.cap_1.1", b"b"),
        ];
        let result = result_of(vec![persisted_terminal(
            "output",
            "media:audio;ext=wav",
            true,
            parts.clone(),
        )]);
        let mut stdout: Vec<u8> = Vec::new();
        let err = emit_terminals(
            &result,
            &EmitOptions {
                output_dir: Some(dir.path().to_path_buf()),
                force: false,
                naming: naming("mic"),
                output_names: names(),
            },
            &mut stdout,
        )
        .unwrap_err();
        assert!(err.reason.contains("refusing to overwrite"), "{}", err.reason);
        assert!(err.written.is_empty());
        assert_eq!(
            err.unsaved,
            vec![
                UnsavedItem {
                    output_node_id: "output".to_string(),
                    file_name: "mic · 1.output.wav".to_string(),
                    media_urn: "media:audio;ext=wav".to_string(),
                    content: UnsavedContent::Part(PathBuf::from(&parts[0])),
                },
                UnsavedItem {
                    output_node_id: "output".to_string(),
                    file_name: "mic · 2.output.wav".to_string(),
                    media_urn: "media:audio;ext=wav".to_string(),
                    content: UnsavedContent::Part(PathBuf::from(&parts[1])),
                },
            ]
        );
        assert!(
            Path::new(&parts[0]).exists() && Path::new(&parts[1]).exists(),
            "the parts handed back are where the run left them"
        );
        assert_eq!(
            std::fs::read(dir.path().join("mic · 1.output.wav")).unwrap(),
            b"precious",
            "the existing file is untouched"
        );
    }

    // TEST22291: a refusal after some items were written says which were
    // written and hands back every item from the refused one on, in memory,
    // under the name it would have had; and an item is text exactly when its
    // media declares a text encoding.
    #[test]
    fn test22291_a_refusal_hands_back_what_it_did_not_write() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("doc · 2.output.txt"), b"an earlier page").unwrap();
        let result = result_of(vec![terminal(
            "output",
            "media:enc=utf8;page-text",
            true,
            vec![b"page one", b"page two", b"page three"],
        )]);
        let mut stdout: Vec<u8> = Vec::new();
        let refusal = emit_terminals(
            &result,
            &EmitOptions {
                output_dir: Some(dir.path().to_path_buf()),
                force: false,
                naming: naming("doc"),
                output_names: names(),
            },
            &mut stdout,
        )
        .unwrap_err();
        assert!(refusal.reason.contains("refusing to overwrite"), "{}", refusal.reason);
        assert_eq!(refusal.written.len(), 1);
        assert!(refusal.written[0].1.ends_with("doc · 1.output.txt"), "{:?}", refusal.written);
        let unsaved: Vec<(&str, &UnsavedContent)> =
            refusal.unsaved.iter().map(|item| (item.file_name.as_str(), &item.content)).collect();
        assert_eq!(
            unsaved,
            vec![
                ("doc · 2.output.txt", &UnsavedContent::Bytes(b"page two".to_vec())),
                ("doc · 3.output.txt", &UnsavedContent::Bytes(b"page three".to_vec())),
            ]
        );
        assert_eq!(std::fs::read(dir.path().join("doc · 2.output.txt")).unwrap(), b"an earlier page");
        assert!(!dir.path().join("doc · 3.output.txt").exists());

        assert!(is_text_media("media:enc=utf8;page-text"));
        assert!(!is_text_media("media:ext=png;image"));
        assert!(!is_text_media("not a media urn"));
    }
}
