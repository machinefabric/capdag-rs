//! Transient run artifacts — INTERMEDIATE node data captured to disk.
//!
//! Mid-strand nodes (the transcription between two model caps, the extracted
//! text between disbind and an LLM) are neither outputs (no listing — they
//! are not things meant to keep) nor messages (too big for memory). They are
//! a third category: TRANSIENT artifacts — written under the host's
//! per-run transient root at the moment the node materializes, self-described
//! by a `provenance.json` sidecar (the output writers' sidecar convention),
//! indexed by the FILESYSTEM alone (no DB rows), readable by anyone who can
//! read the disk cache, and owned by an eager TTL reaper. Structural
//! provenance: the run id is the directory, the node id joins the run's
//! persisted resolved strand, and `run_sources` carries the origins.
//!
//! Storage layout, per node: `{transient_root}/{node_id}/data` plus
//! `{transient_root}/{node_id}/provenance.json`. Sequence data is the
//! canonical RFC 8742 CBOR form (the node_data / spool byte form); the
//! sidecar carries every item's `[offset, len]` so a reader previews item N
//! with one bounded read — never a rescan, never a whole-file load.

use std::path::{Path, PathBuf};

use crate::ExecutionError;

/// Sidecar file name — the same name the output writers use for theirs.
pub const TRANSIENT_SIDECAR: &str = "provenance.json";
/// Data file name inside a node's transient directory.
pub const TRANSIENT_DATA_FILE: &str = "data";

/// One captured transient artifact: an intermediate node's complete data on
/// disk. Handed to [`EngineRuntime::on_transient_artifact`] at capture time
/// (mid-run — the node just materialized) so the host can publish it.
#[derive(Debug, Clone)]
pub struct TransientArtifact {
    pub node_id: String,
    pub media_urn: String,
    pub is_sequence: bool,
    pub item_count: usize,
    pub byte_count: u64,
    /// Absolute path of the data file.
    pub data_path: PathBuf,
}

/// The sidecar record — written at capture, read by hosts serving the
/// transient inspection surface. ONE definition for both directions.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TransientSidecar {
    pub node_id: String,
    pub media_urn: String,
    pub is_sequence: bool,
    pub item_count: usize,
    pub byte_count: u64,
    /// Sequence data only: every item's `[offset, len]` within `data` —
    /// the self-delimiting CBOR value; empty for blobs.
    pub item_offsets: Vec<[u64; 2]>,
    pub created_at_ms: u64,
}

/// Read one node's sidecar from its transient directory.
pub fn read_sidecar(node_dir: &Path) -> Result<TransientSidecar, ExecutionError> {
    let path = node_dir.join(TRANSIENT_SIDECAR);
    let bytes = std::fs::read(&path).map_err(|e| {
        ExecutionError::HostError(format!(
            "transient read: failed to read sidecar '{}': {e}",
            path.display()
        ))
    })?;
    serde_json::from_slice(&bytes).map_err(|e| {
        ExecutionError::HostError(format!(
            "transient read: sidecar '{}' does not parse: {e}",
            path.display()
        ))
    })
}

/// Read ONE item of a captured artifact — the read path the sidecar's offset
/// index exists for. A blob has exactly item 0 (the whole data file); a
/// sequence item is one bounded read at its recorded `[offset, len]`, decoded
/// from its CBOR Bytes wrapper back to the raw item.
pub fn read_transient_item(
    node_dir: &Path,
    sidecar: &TransientSidecar,
    item_index: usize,
) -> Result<Vec<u8>, ExecutionError> {
    let data_path = node_dir.join(TRANSIENT_DATA_FILE);
    if !sidecar.is_sequence {
        if item_index != 0 {
            return Err(ExecutionError::HostError(format!(
                "transient read: blob '{}' has exactly one item, index {item_index} requested",
                sidecar.node_id
            )));
        }
        return std::fs::read(&data_path).map_err(|e| {
            ExecutionError::HostError(format!(
                "transient read: failed to read '{}': {e}",
                data_path.display()
            ))
        });
    }
    let [offset, len] = *sidecar.item_offsets.get(item_index).ok_or_else(|| {
        ExecutionError::HostError(format!(
            "transient read: '{}' has {} items, index {item_index} requested",
            sidecar.node_id,
            sidecar.item_offsets.len()
        ))
    })?;
    use std::io::{Read, Seek};
    let mut file = std::fs::File::open(&data_path).map_err(|e| {
        ExecutionError::HostError(format!(
            "transient read: failed to open '{}': {e}",
            data_path.display()
        ))
    })?;
    file.seek(std::io::SeekFrom::Start(offset)).map_err(|e| {
        ExecutionError::HostError(format!(
            "transient read: failed to seek '{}': {e}",
            data_path.display()
        ))
    })?;
    let mut buf = vec![0u8; len as usize];
    file.read_exact(&mut buf).map_err(|e| {
        ExecutionError::HostError(format!(
            "transient read: failed to read item {item_index} of '{}': {e}",
            data_path.display()
        ))
    })?;
    match ciborium::de::from_reader::<ciborium::Value, _>(buf.as_slice()) {
        Ok(ciborium::Value::Bytes(raw)) => Ok(raw),
        Ok(other) => Err(ExecutionError::HostError(format!(
            "transient read: item {item_index} of '{}' is not a CBOR Bytes value              (got {other:?}) — corrupt capture",
            sidecar.node_id
        ))),
        Err(e) => Err(ExecutionError::HostError(format!(
            "transient read: item {item_index} of '{}' does not decode: {e}",
            sidecar.node_id
        ))),
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Suffix of a node's STAGING directory. Captures build the data + sidecar
/// there and atomically rename to the final name — so a reader's invariant
/// is strict: a published node directory ALWAYS holds its sidecar. A
/// `.tmp` dir is an in-flight (or crashed) capture: invisible to readers,
/// owned by the reaper.
pub const TRANSIENT_STAGING_SUFFIX: &str = ".tmp";

fn node_dir(root: &Path, node_id: &str) -> Result<PathBuf, ExecutionError> {
    // Node ids are plan-internal identifiers; a path separator in one would
    // escape the transient root, and the staging suffix would collide with
    // the atomic-publish protocol — illegal states, refused.
    if node_id.is_empty()
        || node_id.contains('/')
        || node_id.contains('\\')
        || node_id.ends_with(TRANSIENT_STAGING_SUFFIX)
    {
        return Err(ExecutionError::HostError(format!(
            "transient capture: node id '{node_id}' is not a valid directory name"
        )));
    }
    Ok(root.join(node_id))
}

/// Create the node's staging dir (wiping a crashed predecessor), returning
/// (staging, final) paths.
fn staging_dir(root: &Path, node_id: &str) -> Result<(PathBuf, PathBuf), ExecutionError> {
    let final_dir = node_dir(root, node_id)?;
    let staging = root.join(format!("{node_id}{TRANSIENT_STAGING_SUFFIX}"));
    if staging.exists() {
        // A crashed earlier capture: its partial state is worthless.
        std::fs::remove_dir_all(&staging).map_err(|e| {
            ExecutionError::HostError(format!(
                "transient capture: failed to clear stale staging '{}': {e}",
                staging.display()
            ))
        })?;
    }
    std::fs::create_dir_all(&staging).map_err(|e| {
        ExecutionError::HostError(format!(
            "transient capture: failed to create '{}': {e}",
            staging.display()
        ))
    })?;
    Ok((staging, final_dir))
}

/// Atomically publish a fully-written staging dir as the node's directory.
fn publish_staging(staging: &Path, final_dir: &Path) -> Result<(), ExecutionError> {
    if final_dir.exists() {
        // A node materializes once per run; a leftover from a superseded
        // attempt is replaced wholesale.
        std::fs::remove_dir_all(final_dir).map_err(|e| {
            ExecutionError::HostError(format!(
                "transient capture: failed to replace '{}': {e}",
                final_dir.display()
            ))
        })?;
    }
    std::fs::rename(staging, final_dir).map_err(|e| {
        ExecutionError::HostError(format!(
            "transient capture: failed to publish '{}' to '{}': {e}",
            staging.display(),
            final_dir.display()
        ))
    })
}

fn write_sidecar(dir: &Path, sidecar: &TransientSidecar) -> Result<(), ExecutionError> {
    let json = serde_json::to_vec_pretty(sidecar).map_err(|e| {
        ExecutionError::HostError(format!(
            "transient capture: sidecar for '{}' does not serialize: {e}",
            sidecar.node_id
        ))
    })?;
    std::fs::write(dir.join(TRANSIENT_SIDECAR), json).map_err(|e| {
        ExecutionError::HostError(format!(
            "transient capture: failed to write sidecar for '{}': {e}",
            sidecar.node_id
        ))
    })
}

/// Capture a MEMORY-materialized intermediate (bounded chain sink): raw
/// items are written in the canonical byte form — a blob's single item as
/// raw bytes; a sequence as RFC 8742 (each raw item re-encoded as a CBOR
/// Bytes value), offsets recorded while writing. A write failure is a hard
/// error: an inspection surface that silently misses nodes is worse than a
/// failed run.
pub fn capture_memory_intermediate(
    root: &Path,
    node_id: &str,
    media_urn: &str,
    items: &[Vec<u8>],
    is_sequence: bool,
) -> Result<TransientArtifact, ExecutionError> {
    let (staging, final_dir) = staging_dir(root, node_id)?;
    let data_path = staging.join(TRANSIENT_DATA_FILE);
    let mut item_offsets: Vec<[u64; 2]> = Vec::new();
    let bytes: Vec<u8> = if is_sequence {
        let mut buf: Vec<u8> = Vec::new();
        for item in items {
            let start = buf.len() as u64;
            ciborium::ser::into_writer(&ciborium::Value::Bytes(item.clone()), &mut buf).map_err(
                |e| {
                    ExecutionError::HostError(format!(
                        "transient capture: item of '{node_id}' does not encode: {e}"
                    ))
                },
            )?;
            item_offsets.push([start, buf.len() as u64 - start]);
        }
        buf
    } else {
        items.first().cloned().unwrap_or_default()
    };
    std::fs::write(&data_path, &bytes).map_err(|e| {
        ExecutionError::HostError(format!(
            "transient capture: failed to write '{}': {e}",
            data_path.display()
        ))
    })?;
    let artifact = TransientArtifact {
        node_id: node_id.to_string(),
        media_urn: media_urn.to_string(),
        is_sequence,
        item_count: if is_sequence { items.len() } else { 1 },
        byte_count: bytes.len() as u64,
        data_path: final_dir.join(TRANSIENT_DATA_FILE),
    };
    write_sidecar(
        &staging,
        &TransientSidecar {
            node_id: artifact.node_id.clone(),
            media_urn: artifact.media_urn.clone(),
            is_sequence,
            item_count: artifact.item_count,
            byte_count: artifact.byte_count,
            item_offsets,
            created_at_ms: now_ms(),
        },
    )?;
    publish_staging(&staging, &final_dir)?;
    Ok(artifact)
}

/// ADOPT a spooled intermediate (unbounded chain sink whose feed ended) as
/// a transient artifact: the spool file — already the canonical byte form —
/// moves into the node's transient directory (rename, or copy+remove across
/// filesystems: temp dirs are commonly tmpfs), sequence item offsets are
/// scanned once, and the sidecar is written. Returns the artifact; its
/// `data_path` is where downstream consumers (later chains, region drivers)
/// must now read from.
pub fn adopt_spool_as_transient(
    root: &Path,
    node_id: &str,
    media_urn: &str,
    spool_path: &Path,
    is_sequence: bool,
) -> Result<TransientArtifact, ExecutionError> {
    let (staging, final_dir) = staging_dir(root, node_id)?;
    let data_path = staging.join(TRANSIENT_DATA_FILE);
    if std::fs::rename(spool_path, &data_path).is_err() {
        // Different filesystem (tmpfs → artifact disk): copy, then remove.
        std::fs::copy(spool_path, &data_path).map_err(|e| {
            ExecutionError::HostError(format!(
                "transient capture: failed to copy spool '{}' to '{}': {e}",
                spool_path.display(),
                data_path.display()
            ))
        })?;
        std::fs::remove_file(spool_path).map_err(|e| {
            ExecutionError::HostError(format!(
                "transient capture: failed to remove adopted spool '{}': {e}",
                spool_path.display()
            ))
        })?;
    }

    let bytes = std::fs::metadata(&data_path)
        .map_err(|e| {
            ExecutionError::HostError(format!(
                "transient capture: failed to stat '{}': {e}",
                data_path.display()
            ))
        })?
        .len();
    let mut item_offsets: Vec<[u64; 2]> = Vec::new();
    if is_sequence {
        // One bounded-window scan for the item index — the read path then
        // never rescans.
        let data = std::fs::File::open(&data_path).map_err(|e| {
            ExecutionError::HostError(format!(
                "transient capture: failed to open '{}': {e}",
                data_path.display()
            ))
        })?;
        let mut reader = std::io::BufReader::new(data);
        use std::io::Read;
        let mut buf: Vec<u8> = Vec::new();
        let mut window = vec![0u8; 256 * 1024];
        let mut offset: u64 = 0;
        loop {
            let n = reader.read(&mut window).map_err(|e| {
                ExecutionError::HostError(format!(
                    "transient capture: failed to read '{}': {e}",
                    data_path.display()
                ))
            })?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&window[..n]);
            loop {
                if buf.is_empty() {
                    break;
                }
                let mut cursor = std::io::Cursor::new(buf.as_slice());
                if ciborium::de::from_reader::<ciborium::Value, _>(&mut cursor).is_err() {
                    break; // incomplete — read more
                }
                let consumed = cursor.position();
                item_offsets.push([offset, consumed]);
                offset += consumed;
                buf.drain(..consumed as usize);
            }
        }
        if !buf.is_empty() {
            return Err(ExecutionError::HostError(format!(
                "transient capture: {} bytes of an incomplete CBOR item at the end of \
                 '{}' — truncated intermediate",
                buf.len(),
                data_path.display()
            )));
        }
    }

    let artifact = TransientArtifact {
        node_id: node_id.to_string(),
        media_urn: media_urn.to_string(),
        is_sequence,
        item_count: if is_sequence { item_offsets.len() } else { 1 },
        byte_count: bytes,
        data_path: final_dir.join(TRANSIENT_DATA_FILE),
    };
    write_sidecar(
        &staging,
        &TransientSidecar {
            node_id: artifact.node_id.clone(),
            media_urn: artifact.media_urn.clone(),
            is_sequence,
            item_count: artifact.item_count,
            byte_count: artifact.byte_count,
            item_offsets,
            created_at_ms: now_ms(),
        },
    )?;
    publish_staging(&staging, &final_dir)?;
    Ok(artifact)
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST8148: transient capture writes the canonical byte forms with a
    // self-describing sidecar — a memory sequence's items are individually
    // addressable via the recorded offsets, and an adopted spool survives
    // the move with identical bytes and a correct item index. This is what
    // makes "querying = reading the disk cache" true.
    #[test]
    fn test8148_transient_capture_forms_and_sidecars() {
        let root = tempfile::tempdir().unwrap();

        // Memory sequence.
        let items = vec![b"window-one".to_vec(), b"window-two".to_vec()];
        let artifact = capture_memory_intermediate(
            root.path(),
            "cap_2",
            "media:enc=utf8;record",
            &items,
            true,
        )
        .unwrap();
        assert!(artifact.is_sequence);
        assert_eq!(artifact.item_count, 2);
        let sidecar: TransientSidecar = serde_json::from_slice(
            &std::fs::read(root.path().join("cap_2").join(TRANSIENT_SIDECAR)).unwrap(),
        )
        .unwrap();
        assert_eq!(sidecar.item_offsets.len(), 2);
        let data = std::fs::read(&artifact.data_path).unwrap();
        // Item 1 decodes independently from its recorded offsets.
        let [off, len] = sidecar.item_offsets[1];
        let value: ciborium::Value =
            ciborium::de::from_reader(&data[off as usize..(off + len) as usize]).unwrap();
        match value {
            ciborium::Value::Bytes(b) => assert_eq!(b, b"window-two"),
            other => panic!("expected bytes, got {other:?}"),
        }

        // Spool adoption (blob).
        let spool = root.path().join("blob.spool");
        std::fs::write(&spool, b"RIFF-recording-bytes").unwrap();
        let adopted = adopt_spool_as_transient(
            root.path(),
            "cap_1",
            "media:audio;ext=wav",
            &spool,
            false,
        )
        .unwrap();
        assert!(!spool.exists(), "the spool is adopted, not duplicated");
        assert!(
            !root.path().join("cap_1.tmp").exists() && !root.path().join("cap_2.tmp").exists(),
            "captures publish atomically — no staging dirs remain"
        );
        assert_eq!(
            adopted.data_path,
            root.path().join("cap_1").join(TRANSIENT_DATA_FILE),
            "the artifact's data path is the PUBLISHED location"
        );
        assert_eq!(adopted.byte_count, 20);
        assert_eq!(
            std::fs::read(&adopted.data_path).unwrap(),
            b"RIFF-recording-bytes"
        );

        // The read path: sidecar + per-item bounded reads round-trip.
        let read_back = read_sidecar(&root.path().join("cap_2")).unwrap();
        assert_eq!(read_back.item_count, 2);
        assert_eq!(
            read_transient_item(&root.path().join("cap_2"), &read_back, 0).unwrap(),
            b"window-one"
        );
        let blob_sidecar = read_sidecar(&root.path().join("cap_1")).unwrap();
        assert_eq!(
            read_transient_item(&root.path().join("cap_1"), &blob_sidecar, 0).unwrap(),
            b"RIFF-recording-bytes"
        );
        assert!(
            read_transient_item(&root.path().join("cap_1"), &blob_sidecar, 1).is_err(),
            "a blob has exactly one item"
        );

        // Illegal node id → refused, nothing written.
        assert!(capture_memory_intermediate(
            root.path(),
            "../escape",
            "media:",
            &[],
            false
        )
        .is_err());
        assert!(
            capture_memory_intermediate(root.path(), "cap_9.tmp", "media:", &[], false).is_err(),
            "the staging suffix is reserved for the atomic-publish protocol"
        );
    }
}
