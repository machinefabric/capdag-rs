//! CLI output emission — the pipe-discipline contract shared by
//! `capdag run <machine>` and the single-cap mode.
//!
//! - Exactly ONE terminal, scalar, single item, and no `--output` dir given
//!   → the item's raw bytes go to STDOUT (logs/progress are on stderr), so
//!   `capdag pdf2summary doc.pdf | wc -w` behaves like a Unix tool.
//! - Anything else (sequence output, several terminals, or an explicit
//!   `--output`) → each item is written as a FILE in the output directory
//!   (default: the current directory), named
//!   `{input_stem}.{node}[.{index}].{ext}` — `{node}` being the name the
//!   machine's notation gives the output — and every written absolute path
//!   is printed to stdout, one per line.
//! - An existing file is a hard error unless `--force` — silent overwrites
//!   destroy user data.
//! - A terminal that was not produced writes nothing here: it is not an empty
//!   file. The caller says so, from the result.
//!
//! The extension derives deterministically from the terminal's media URN —
//! `ext=<v>` wins (the declared file type), else `fmt=<v>` (serialization),
//! else `enc=utf-8` ⇒ `txt`, else `bin`. This is the defined naming scheme
//! of the CLI, not a fallback chain: every media URN maps to exactly one
//! name by these rules.

use std::io::Write;
use std::path::{Path, PathBuf};

use crate::orchestrator::execute_plan::{PipelineResult, ProducedTerminal};
use crate::urn::media_urn::MediaUrn;

/// Options for [`emit_terminals`].
pub struct EmitOptions {
    /// Where files go. `None` = stdout is allowed for the single-scalar
    /// case and files default to the current directory otherwise.
    pub output_dir: Option<PathBuf>,
    /// Overwrite existing files instead of hard-erroring.
    pub force: bool,
    /// Stem for produced file names — the input file's stem, or `stdin`.
    pub input_stem: String,
    /// The name the notation gives each output, by plan `Output` node id: the
    /// `{node}` of a produced file's name. Every terminal of the result must
    /// have one — a file is never named after a plan's internal node id,
    /// which changes whenever the machine is edited.
    pub output_names: std::collections::HashMap<String, String>,
}

/// File extension for a terminal's media URN, per the naming scheme above.
pub fn extension_for_media(media_urn: &str) -> String {
    let Ok(parsed) = MediaUrn::from_string(media_urn) else {
        return "bin".to_string();
    };
    if let Some(ext) = parsed.get_tag("ext") {
        return ext.to_string();
    }
    if let Some(fmt) = parsed.get_tag("fmt") {
        return fmt.to_string();
    }
    if parsed.get_tag("enc").is_some() {
        return "txt".to_string();
    }
    "bin".to_string()
}

/// Emit a pipeline result per the pipe-discipline contract. Returns the
/// absolute paths of files written (empty when the result went to stdout).
/// `stdout` is injected so tests can capture it.
pub fn emit_terminals(
    result: &PipelineResult,
    options: &EmitOptions,
    stdout: &mut dyn Write,
) -> Result<Vec<PathBuf>, String> {
    Ok(emit_terminals_by_output(result, options, stdout)?
        .into_iter()
        .map(|(_, path)| path)
        .collect())
}

/// [`emit_terminals`], saying which output each written file is of: the
/// plan `Output` node id and the file's absolute path, in the order they were
/// written. Empty when the result went to stdout.
pub fn emit_terminals_by_output(
    result: &PipelineResult,
    options: &EmitOptions,
    stdout: &mut dyn Write,
) -> Result<Vec<(String, PathBuf)>, String> {
    // The stdout fast path: one terminal, scalar, one item, no explicit dir.
    // The item may be in memory or a single persisted part-file (the CLI's disk
    // writer, L16) — persisted, its bytes stream to stdout and the part is
    // removed, so the pipe contract is identical either way.
    let produced: Vec<&ProducedTerminal> = result
        .terminals
        .iter()
        .filter_map(|terminal| terminal.produced())
        .collect();
    if let (None, 1, [terminal]) =
        (&options.output_dir, result.terminals.len(), produced.as_slice())
    {
        if !terminal.is_sequence {
            if terminal.writer_results.is_empty() && terminal.items.len() == 1 {
                stdout
                    .write_all(&terminal.items[0].data)
                    .map_err(|e| format!("failed to write result to stdout: {e}"))?;
                stdout
                    .flush()
                    .map_err(|e| format!("failed to flush stdout: {e}"))?;
                return Ok(Vec::new());
            }
            let parts: Vec<&String> = terminal
                .writer_results
                .iter()
                .flat_map(|w| w.saved_paths.iter())
                .collect();
            if let [part] = parts.as_slice() {
                let mut file = std::fs::File::open(part)
                    .map_err(|e| format!("failed to open persisted result '{part}': {e}"))?;
                std::io::copy(&mut file, stdout)
                    .map_err(|e| format!("failed to stream result to stdout: {e}"))?;
                stdout
                    .flush()
                    .map_err(|e| format!("failed to flush stdout: {e}"))?;
                std::fs::remove_file(part)
                    .map_err(|e| format!("failed to remove part file '{part}': {e}"))?;
                return Ok(Vec::new());
            }
        }
    }

    let dir = options
        .output_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("."));
    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("failed to create output dir {dir:?}: {e}"))?;

    // Every persisted part-file in processing order — so a refusal mid-emit can
    // sweep the not-yet-renamed parts instead of littering the output dir.
    let all_parts: Vec<String> = produced
        .iter()
        .flat_map(|t| t.writer_results.iter())
        .flat_map(|w| w.saved_paths.iter().cloned())
        .collect();
    let mut parts_done = 0usize;
    let refuse = |path: &Path, parts_done: usize| {
        for part in &all_parts[parts_done..] {
            let _ = std::fs::remove_file(part);
        }
        format!("refusing to overwrite existing file {path:?} (pass --force to allow)")
    };

    let mut written: Vec<(String, PathBuf)> = Vec::new();
    for terminal in produced {
        let ext = extension_for_media(&terminal.media_urn);
        let node = options.output_names.get(&terminal.output_node_id).ok_or_else(|| {
            // Nothing was written yet for this terminal; parts of the others
            // not yet renamed are swept like any refusal.
            for part in &all_parts[parts_done..] {
                let _ = std::fs::remove_file(part);
            }
            format!(
                "output '{}' has no name to write it under",
                terminal.output_node_id
            )
        })?;
        if terminal.writer_results.is_empty() {
            let multi_item = terminal.is_sequence || terminal.items.len() > 1;
            for item in &terminal.items {
                let name = if multi_item {
                    format!("{}.{}.{}.{}", options.input_stem, node, item.index, ext)
                } else {
                    format!("{}.{}.{}", options.input_stem, node, ext)
                };
                let path = dir.join(&name);
                if path.exists() && !options.force {
                    return Err(refuse(&path, parts_done));
                }
                std::fs::write(&path, &item.data)
                    .map_err(|e| format!("failed to write {path:?}: {e}"))?;
                let absolute = absolutize(&path);
                writeln!(stdout, "{}", absolute.display())
                    .map_err(|e| format!("failed to report written path: {e}"))?;
                written.push((terminal.output_node_id.clone(), absolute));
            }
        } else {
            // Persisted terminal (CLI disk writer): rename each part-file to its
            // contract name — parts live in the destination directory, so the
            // rename is same-filesystem and never re-buffers the data.
            let parts: Vec<&String> = terminal
                .writer_results
                .iter()
                .flat_map(|w| w.saved_paths.iter())
                .collect();
            let multi_item = terminal.is_sequence || parts.len() > 1;
            for (index, part) in parts.iter().enumerate() {
                let name = if multi_item {
                    format!("{}.{}.{}.{}", options.input_stem, node, index, ext)
                } else {
                    format!("{}.{}.{}", options.input_stem, node, ext)
                };
                let path = dir.join(&name);
                if path.exists() && !options.force {
                    return Err(refuse(&path, parts_done));
                }
                std::fs::rename(part, &path).map_err(|e| {
                    format!("failed to move persisted result '{part}' to {path:?}: {e}")
                })?;
                parts_done += 1;
                let absolute = absolutize(&path);
                writeln!(stdout, "{}", absolute.display())
                    .map_err(|e| format!("failed to report written path: {e}"))?;
                written.push((terminal.output_node_id.clone(), absolute));
            }
        }
    }
    stdout
        .flush()
        .map_err(|e| format!("failed to flush stdout: {e}"))?;
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
            "media:enc=utf-8;summary",
            false,
            vec![b"the summary text"],
        )]);
        let mut stdout: Vec<u8> = Vec::new();
        let written = emit_terminals(
            &result,
            &EmitOptions {
                output_dir: None,
                force: false,
                input_stem: "doc".to_string(),
                output_names: names(),
            },
            &mut stdout,
        )
        .unwrap();
        assert!(written.is_empty());
        assert_eq!(stdout, b"the summary text");

        // 2. Sequence → one file per item, named {stem}.{node}.{index}.{ext},
        //    paths listed on stdout.
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
                input_stem: "doc".to_string(),
                output_names: names(),
            },
            &mut stdout,
        )
        .unwrap();
        assert_eq!(written.len(), 2);
        assert!(written[0].ends_with("doc.output.0.png"), "{written:?}");
        assert!(written[1].ends_with("doc.output.1.png"), "{written:?}");
        assert_eq!(std::fs::read(&written[0]).unwrap(), b"png0");
        let listing = String::from_utf8(stdout).unwrap();
        assert_eq!(listing.lines().count(), 2, "one path per line: {listing}");

        // 3. Multi-terminal (fan-out) → files even for scalars, one per
        //    terminal, extension from each terminal's own media.
        let dir = tempfile::tempdir().unwrap();
        let result = result_of(vec![
            terminal("thumb", "media:ext=png;image", false, vec![b"png"]),
            terminal("text", "media:enc=utf-8", false, vec![b"txt"]),
        ]);
        let mut stdout: Vec<u8> = Vec::new();
        let written = emit_terminals(
            &result,
            &EmitOptions {
                output_dir: Some(dir.path().to_path_buf()),
                force: false,
                input_stem: "doc".to_string(),
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
                input_stem: "doc".to_string(),
                output_names: names(),
            },
            &mut stdout,
        )
        .unwrap_err();
        assert!(err.contains("refusing to overwrite"), "{err}");
        let mut stdout: Vec<u8> = Vec::new();
        emit_terminals(
            &result,
            &EmitOptions {
                output_dir: Some(dir.path().to_path_buf()),
                force: true,
                input_stem: "doc".to_string(),
                output_names: names(),
            },
            &mut stdout,
        )
        .expect("--force must overwrite");

        // 5. Extension derivation: ext= wins over fmt=; enc= alone ⇒ txt;
        //    no axis ⇒ bin.
        assert_eq!(extension_for_media("media:document;ext=pdf;fmt=json"), "pdf");
        assert_eq!(extension_for_media("media:fmt=ndjson;record"), "ndjson");
        assert_eq!(extension_for_media("media:enc=utf-8;summary"), "txt");
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
            }],
        })
    }

    // TEST12792: a produced file is named for the name the notation gives the
    // output, not for the plan's node id; and a terminal with no name is
    // refused rather than written under an id nobody chose.
    #[test]
    fn test12792_a_file_is_named_for_the_outputs_written_name() {
        let dir = tempfile::tempdir().unwrap();
        let result = result_of(vec![
            terminal("output_7", "media:enc=utf-8;ext=md", false, vec![b"# summary"]),
            terminal("output_3", "media:ext=png;image", true, vec![b"p0", b"p1"]),
        ]);
        let options = |output_names: &[(&str, &str)]| EmitOptions {
            output_dir: Some(dir.path().to_path_buf()),
            force: false,
            input_stem: "doc".to_string(),
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
        assert_eq!(names, vec!["doc.pages.0.png", "doc.pages.1.png", "doc.summary.md"]);

        let other = tempfile::tempdir().unwrap();
        let mut unnamed = options(&[("output_7", "summary")]);
        unnamed.output_dir = Some(other.path().to_path_buf());
        let error = emit_terminals(&result, &unnamed, &mut Vec::new()).unwrap_err();
        assert_eq!(error, "output 'output_3' has no name to write it under");
    }

    // TEST12761: a terminal that was not produced writes nothing — no file,
    // no bytes on stdout — and is not what makes a result "one terminal": a
    // produced terminal beside it still goes to a file, by its contract name.
    #[test]
    fn test12761_a_terminal_not_produced_writes_nothing() {
        let absent = || PlanTerminal::NotProduced {
            output_node_id: "rejected".to_string(),
            media_urn: "media:enc=utf-8".to_string(),
        };
        let options = |dir: Option<&Path>| EmitOptions {
            output_dir: dir.map(Path::to_path_buf),
            force: false,
            input_stem: "doc".to_string(),
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
            terminal("kept", "media:enc=utf-8;ext=txt", false, vec![b"kept text"]),
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
    // part-file disappears; an overwrite refusal sweeps the unrenamed parts.
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
                input_stem: "cam".to_string(),
                output_names: names(),
            },
            &mut stdout,
        )
        .unwrap();
        assert_eq!(written.len(), 2);
        assert!(written[0].ends_with("cam.output.0.png"), "{written:?}");
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
                input_stem: "mic".to_string(),
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
                input_stem: "mic".to_string(),
                output_names: names(),
            },
            &mut stdout,
        )
        .unwrap();
        assert_eq!(written.len(), 1);
        assert!(written[0].ends_with("mic.output.wav"), "{written:?}");
        assert_eq!(std::fs::read(&written[0]).unwrap(), b"wav-bytes");

        // 4. Overwrite refusal mid-emit sweeps the not-yet-renamed parts —
        //    no litter in the output dir, and the error names --force.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("mic.output.0.wav"), b"precious").unwrap();
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
                input_stem: "mic".to_string(),
                output_names: names(),
            },
            &mut stdout,
        )
        .unwrap_err();
        assert!(err.contains("refusing to overwrite"), "{err}");
        assert!(
            !Path::new(&parts[0]).exists() && !Path::new(&parts[1]).exists(),
            "refusal sweeps the remaining part-files"
        );
        assert_eq!(
            std::fs::read(dir.path().join("mic.output.0.wav")).unwrap(),
            b"precious",
            "the existing file is untouched"
        );
    }
}
