//! [`CliDiskWriter`] — the CLI's [`IncrementalWriter`].
//!
//! The CLI persists every plan-terminal sink to disk incrementally, exactly like
//! the engine: chunks are appended to hidden part-files inside the emit target
//! directory as they arrive, so an UNBOUNDED terminal (L16) never accumulates in
//! memory. [`emit_terminals`](super::cli_output::emit_terminals) then renames
//! each part-file to its contract name (or streams the single-scalar case to
//! stdout). The parts live in the destination directory itself, so the final
//! rename never crosses a filesystem.
//!
//! Wire semantics mirror the engine's writer:
//! - Blob (`is_sequence=false`): each CHUNK payload is one complete CBOR
//!   Bytes/Text value; its raw bytes are appended to a single part-file.
//! - Sequence (`is_sequence=true`): payloads are raw CBOR fragments
//!   (`emit_list_item` splits values across chunks); fragments accumulate until
//!   a complete self-delimiting value decodes, and each decoded item becomes its
//!   own part-file. Bytes left in the buffer at STREAM_END are a hard error —
//!   a truncated item is corruption, never silently dropped.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;

use super::execute_plan::WriterResult;
use super::stream_io::{unwrap_cbor_value, IncrementalWriter, StreamIoError};
use crate::StreamMeta;

/// Prefix of every part-file the CLI writer creates. Hidden (dot-file) so a
/// run's in-flight parts never collide with, or read as, user outputs.
pub const PART_FILE_PREFIX: &str = ".capdag-part";

enum State {
    Idle,
    Blob {
        file: tokio::fs::File,
        path: PathBuf,
        bytes: usize,
    },
    Sequence {
        saved_paths: Vec<String>,
        total_bytes: usize,
        buf: Vec<u8>,
        pending_item_meta: Option<StreamMeta>,
        item_metas: Vec<Option<StreamMeta>>,
    },
}

/// Streams one persisted terminal sink to part-files in `dir`. `tag` makes the
/// part names unique across sinks, runs, and ForEach bodies.
pub struct CliDiskWriter {
    dir: PathBuf,
    tag: String,
    media_urn: String,
    stream_meta: Option<StreamMeta>,
    state: State,
}

impl CliDiskWriter {
    pub fn new(dir: PathBuf, tag: String) -> Self {
        Self {
            dir,
            tag,
            media_urn: String::new(),
            stream_meta: None,
            state: State::Idle,
        }
    }

    fn part_path(&self, suffix: &str) -> PathBuf {
        self.dir
            .join(format!("{PART_FILE_PREFIX}.{}.{suffix}", self.tag))
    }

    /// Decode every complete CBOR item currently in the sequence buffer into its
    /// own part-file. A decode that fails on a prefix means the item is still
    /// incomplete — more chunks are coming; corruption surfaces at STREAM_END
    /// when leftover bytes remain.
    async fn drain_sequence_items(
        dir: &Path,
        tag: &str,
        saved_paths: &mut Vec<String>,
        total_bytes: &mut usize,
        buf: &mut Vec<u8>,
        pending_item_meta: &mut Option<StreamMeta>,
        item_metas: &mut Vec<Option<StreamMeta>>,
    ) -> Result<(), StreamIoError> {
        loop {
            if buf.is_empty() {
                return Ok(());
            }
            let mut cursor = std::io::Cursor::new(buf.as_slice());
            let value: ciborium::Value = match ciborium::de::from_reader(&mut cursor) {
                Ok(v) => v,
                Err(_) => return Ok(()), // incomplete — wait for more chunks
            };
            let consumed = cursor.position() as usize;
            let index = saved_paths.len();
            let raw = unwrap_cbor_value(value, index)?;
            let path = dir.join(format!("{PART_FILE_PREFIX}.{tag}.{index}"));
            tokio::fs::write(&path, &raw).await.map_err(|e| {
                StreamIoError::Protocol(format!(
                    "failed to persist sequence item {index} to '{}': {e}",
                    path.display()
                ))
            })?;
            *total_bytes += raw.len();
            saved_paths.push(path.to_string_lossy().into_owned());
            item_metas.push(pending_item_meta.take());
            buf.drain(..consumed);
        }
    }
}

#[async_trait]
impl IncrementalWriter for CliDiskWriter {
    async fn on_stream_start(
        &mut self,
        is_sequence: Option<bool>,
        media_urn: &str,
        meta: Option<StreamMeta>,
        _stream_id: Option<String>,
    ) -> Result<(), StreamIoError> {
        if !matches!(self.state, State::Idle) {
            return Err(StreamIoError::Protocol(format!(
                "terminal sink '{}' opened a second stream — a persisted CLI \
                 terminal is exactly one stream",
                self.tag
            )));
        }
        self.media_urn = media_urn.to_string();
        self.stream_meta = meta;
        tokio::fs::create_dir_all(&self.dir).await.map_err(|e| {
            StreamIoError::Protocol(format!(
                "failed to create output dir '{}': {e}",
                self.dir.display()
            ))
        })?;
        if is_sequence == Some(true) {
            self.state = State::Sequence {
                saved_paths: Vec::new(),
                total_bytes: 0,
                buf: Vec::new(),
                pending_item_meta: None,
                item_metas: Vec::new(),
            };
        } else {
            let path = self.part_path("blob");
            let file = tokio::fs::File::create(&path).await.map_err(|e| {
                StreamIoError::Protocol(format!(
                    "failed to create part file '{}': {e}",
                    path.display()
                ))
            })?;
            self.state = State::Blob {
                file,
                path,
                bytes: 0,
            };
        }
        Ok(())
    }

    async fn on_chunk_payload(
        &mut self,
        payload: &[u8],
        meta: Option<StreamMeta>,
    ) -> Result<(), StreamIoError> {
        match &mut self.state {
            State::Idle => Err(StreamIoError::Protocol(
                "CLI terminal writer received a CHUNK before STREAM_START".to_string(),
            )),
            State::Blob { file, path, bytes } => {
                // write() sends one complete CBOR Bytes/Text value per chunk.
                let value: ciborium::Value =
                    ciborium::de::from_reader(payload).map_err(|e| {
                        StreamIoError::CborDecode(format!("terminal blob chunk: {e}"))
                    })?;
                let raw = unwrap_cbor_value(value, 0)?;
                file.write_all(&raw).await.map_err(|e| {
                    StreamIoError::Protocol(format!(
                        "failed to append to part file '{}': {e}",
                        path.display()
                    ))
                })?;
                *bytes += raw.len();
                Ok(())
            }
            State::Sequence {
                saved_paths,
                total_bytes,
                buf,
                pending_item_meta,
                item_metas,
            } => {
                // Per-item meta arrives on the first chunk frame of each item.
                if meta.is_some() {
                    *pending_item_meta = meta;
                }
                buf.extend_from_slice(payload);
                Self::drain_sequence_items(
                    &self.dir,
                    &self.tag,
                    saved_paths,
                    total_bytes,
                    buf,
                    pending_item_meta,
                    item_metas,
                )
                .await
            }
        }
    }

    async fn on_stream_end(&mut self) -> Result<(), StreamIoError> {
        match &mut self.state {
            State::Idle => Ok(()), // empty terminal — nothing was streamed
            State::Blob { file, path, .. } => file.flush().await.map_err(|e| {
                StreamIoError::Protocol(format!(
                    "failed to flush part file '{}': {e}",
                    path.display()
                ))
            }),
            State::Sequence {
                saved_paths,
                total_bytes,
                buf,
                pending_item_meta,
                item_metas,
            } => {
                Self::drain_sequence_items(
                    &self.dir,
                    &self.tag,
                    saved_paths,
                    total_bytes,
                    buf,
                    pending_item_meta,
                    item_metas,
                )
                .await?;
                if !buf.is_empty() {
                    return Err(StreamIoError::Protocol(format!(
                        "{} bytes of an incomplete CBOR item remain at STREAM_END — \
                         truncated terminal sequence",
                        buf.len()
                    )));
                }
                Ok(())
            }
        }
    }

    fn finish(self: Box<Self>) -> WriterResult {
        match self.state {
            State::Idle => WriterResult {
                is_sequence: false,
                media_urn: self.media_urn,
                saved_paths: Vec::new(),
                total_bytes: 0,
                stream_meta: self.stream_meta,
                item_metas: Vec::new(),
            },
            State::Blob { path, bytes, .. } => WriterResult {
                is_sequence: false,
                media_urn: self.media_urn,
                saved_paths: vec![path.to_string_lossy().into_owned()],
                total_bytes: bytes,
                stream_meta: self.stream_meta,
                item_metas: Vec::new(),
            },
            State::Sequence {
                saved_paths,
                total_bytes,
                item_metas,
                ..
            } => WriterResult {
                is_sequence: true,
                media_urn: self.media_urn,
                saved_paths,
                total_bytes,
                stream_meta: None,
                item_metas,
            },
        }
    }

    /// Remove every part-file this writer created: the stream did not end, so
    /// none of them is an output.
    fn abort(self: Box<Self>) {
        match self.state {
            State::Idle => {}
            State::Blob { path, .. } => {
                let _ = std::fs::remove_file(path);
            }
            State::Sequence { saved_paths, .. } => {
                for path in saved_paths {
                    let _ = std::fs::remove_file(path);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cbor_bytes(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        ciborium::ser::into_writer(&ciborium::Value::Bytes(data.to_vec()), &mut out).unwrap();
        out
    }

    // TEST8141: blob mode — each chunk's CBOR-wrapped bytes are appended to ONE
    // part-file as they arrive, and finish() reports the path, byte count and
    // scalar shape. This is the persistence that satisfies L16 for an unbounded
    // blob terminal (e.g. a live WAV recording) in the CLI.
    #[tokio::test]
    async fn test8141_blob_writer_streams_chunks_to_a_part_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Box::new(CliDiskWriter::new(
            dir.path().to_path_buf(),
            "t.cap_1".to_string(),
        ));
        w.on_stream_start(Some(false), "media:audio;ext=wav", None, None)
            .await
            .unwrap();
        w.on_chunk_payload(&cbor_bytes(b"RIFF"), None).await.unwrap();
        w.on_chunk_payload(&cbor_bytes(b"data"), None).await.unwrap();
        w.on_stream_end().await.unwrap();
        let result = (w as Box<dyn IncrementalWriter>).finish();
        assert!(!result.is_sequence);
        assert_eq!(result.total_bytes, 8);
        assert_eq!(result.saved_paths.len(), 1);
        let content = std::fs::read(&result.saved_paths[0]).unwrap();
        assert_eq!(content, b"RIFFdata");
        assert!(result.saved_paths[0].contains(PART_FILE_PREFIX));
    }

    // TEST8142: sequence mode — CBOR items split across chunk payload
    // boundaries are reassembled and each complete item lands in its OWN
    // part-file the moment it decodes (incremental, before STREAM_END); a
    // truncated item left at STREAM_END is a hard error, not a dropped item.
    #[tokio::test]
    async fn test8142_sequence_writer_drains_items_across_chunk_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Box::new(CliDiskWriter::new(
            dir.path().to_path_buf(),
            "t.cap_2".to_string(),
        ));
        w.on_stream_start(Some(true), "media:image;ext=png", None, None)
            .await
            .unwrap();
        // Two items; the second is split mid-value across two payloads.
        let item0 = cbor_bytes(b"frame-zero");
        let item1 = cbor_bytes(b"frame-one");
        w.on_chunk_payload(&item0, None).await.unwrap();
        let (a, b) = item1.split_at(3);
        w.on_chunk_payload(a, None).await.unwrap();
        w.on_chunk_payload(b, None).await.unwrap();
        w.on_stream_end().await.unwrap();
        let result = (w as Box<dyn IncrementalWriter>).finish();
        assert!(result.is_sequence);
        assert_eq!(result.saved_paths.len(), 2);
        assert_eq!(
            std::fs::read(&result.saved_paths[0]).unwrap(),
            b"frame-zero"
        );
        assert_eq!(std::fs::read(&result.saved_paths[1]).unwrap(), b"frame-one");

        // Truncated item at STREAM_END → hard error.
        let mut w2 = Box::new(CliDiskWriter::new(
            dir.path().to_path_buf(),
            "t.cap_3".to_string(),
        ));
        w2.on_stream_start(Some(true), "media:image;ext=png", None, None)
            .await
            .unwrap();
        w2.on_chunk_payload(&item0[..2], None).await.unwrap();
        let err = w2.on_stream_end().await.expect_err("truncated item");
        assert!(err.to_string().contains("incomplete"), "{err}");
    }

    // TEST8143: protocol discipline — a CHUNK before STREAM_START and a second
    // STREAM_START are both refused loudly; an Idle finish() reports an empty
    // scalar result rather than inventing files.
    #[tokio::test]
    async fn test8143_writer_protocol_discipline() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Box::new(CliDiskWriter::new(
            dir.path().to_path_buf(),
            "t.cap_4".to_string(),
        ));
        let err = w
            .on_chunk_payload(&cbor_bytes(b"x"), None)
            .await
            .expect_err("chunk before start");
        assert!(err.to_string().contains("STREAM_START"), "{err}");
        w.on_stream_start(Some(false), "media:enc=utf-8", None, None)
            .await
            .unwrap();
        let err = w
            .on_stream_start(Some(false), "media:enc=utf-8", None, None)
            .await
            .expect_err("second stream");
        assert!(err.to_string().contains("second stream"), "{err}");

        let idle = Box::new(CliDiskWriter::new(
            dir.path().to_path_buf(),
            "t.cap_5".to_string(),
        ));
        let result = (idle as Box<dyn IncrementalWriter>).finish();
        assert!(!result.is_sequence);
        assert!(result.saved_paths.is_empty());
        assert_eq!(result.total_bytes, 0);
    }
}
