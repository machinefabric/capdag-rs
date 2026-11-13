//! The `CartridgeAdapterInvoker` a host that holds a `RelaySwitch` asks its
//! cartridges with: the desktop engine for its attached cartridges, and the
//! capdag command for the cartridges it starts to inspect a file.
//!
//! Drives a cartridge's `cap:in="media:";out="media:adapter-selection;json;record"`
//! handler over the bifaci relay, targeting a specific cartridge by id
//! (via `target_cartridge` REQ meta) so adapter selection never lands
//! on the wrong cartridge when multiple register the cap. Returns the
//! cartridge's verdict — a list of media URN strings — to the caller.
//!
//! Each invocation:
//! 1. Reads the file's content (capped) so the cartridge's adapter op
//!    can inspect the bytes without us re-streaming an enormous file.
//! 2. Builds a REQ + STREAM_START + CHUNK + STREAM_END + END frame
//!    sequence carrying the file path in stream meta and the bytes as
//!    the stream's only chunk.
//! 3. Stamps `target_cartridge` on the REQ meta so the relay's
//!    `register_external_request_for_cartridge` path skips cap-table
//!    routing and goes directly to the named cartridge.
//! 4. Drains the response frames and decodes the END payload's
//!    `{"media_urns": [...]}` JSON envelope. An empty END (no body
//!    chunks) means "this cartridge does not handle this file."

use async_trait::async_trait;
use crate::bifaci::frame::{Frame, FrameType, MessageId};
use crate::bifaci::relay_switch::RelaySwitch;
use crate::input_resolver::{CartridgeAdapterInvoker, InputResolverError};
use crate::standard::caps::CAP_ADAPTER_SELECTION;
use std::path::Path;
use std::sync::Arc;

/// Maximum bytes inspected per file when invoking adapter selection.
/// The one inspection prefix size every host path reads
/// ([`MAX_CONTENT_INSPECTION_BYTES`](crate::input_resolver::MAX_CONTENT_INSPECTION_BYTES)).
const MAX_INSPECTION_BYTES: usize = crate::input_resolver::MAX_CONTENT_INSPECTION_BYTES;

/// Maximum chunk size for the file-content stream sent to the
/// cartridge. Mirrors the default the relay uses everywhere else; if
/// negotiated limits are smaller the bifaci layer fragments
/// transparently.
const STREAM_CHUNK_SIZE: usize = 16 * 1024;

/// Adapter-selection invoker that routes through a `RelaySwitch` and
/// targets a specific cartridge by id.
pub struct RelaySwitchAdapterInvoker {
    switch: Arc<RelaySwitch>,
}

impl RelaySwitchAdapterInvoker {
    pub fn new(switch: Arc<RelaySwitch>) -> Self {
        Self { switch }
    }
}

#[async_trait]
impl CartridgeAdapterInvoker for RelaySwitchAdapterInvoker {
    async fn invoke_adapter_selection(
        &self,
        cartridge_id: &str,
        file_path: &Path,
    ) -> Result<Option<Vec<String>>, InputResolverError> {
        // Read up to MAX_INSPECTION_BYTES of the file so the cartridge's
        // adapter op has a content prefix to inspect. Reading from disk
        // here keeps this invoker host-agnostic — cartridges run in any
        // bifaci-speaking process and don't need a file handle of their
        // own.
        let mut content = Vec::new();
        {
            use std::io::Read;
            let metadata = std::fs::metadata(file_path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    InputResolverError::NotFound(file_path.to_path_buf())
                } else if e.kind() == std::io::ErrorKind::PermissionDenied {
                    InputResolverError::PermissionDenied(file_path.to_path_buf())
                } else {
                    InputResolverError::IoError {
                        path: file_path.to_path_buf(),
                        error: e,
                    }
                }
            })?;
            let read_size = (metadata.len() as usize).min(MAX_INSPECTION_BYTES);
            content.resize(read_size, 0);
            let mut file =
                std::fs::File::open(file_path).map_err(|e| InputResolverError::IoError {
                    path: file_path.to_path_buf(),
                    error: e,
                })?;
            let bytes_read = file
                .read(&mut content)
                .map_err(|e| InputResolverError::IoError {
                    path: file_path.to_path_buf(),
                    error: e,
                })?;
            content.truncate(bytes_read);
        }

        // Build the frame sequence by hand so we can stamp
        // `target_cartridge` on the REQ meta. The standard
        // `CapArgumentValue::build_request_frames` doesn't take meta,
        // and adapter selection is the one place where the meta is
        // load-bearing.
        let rid = MessageId::new_uuid();

        // Construct REQ with `target_cartridge` meta. The relay's
        // `send_to_master` path consults this meta and skips
        // cap-based dispatch in favour of direct cartridge routing.
        let mut req = Frame::req(
            rid.clone(),
            CAP_ADAPTER_SELECTION,
            Vec::new(),
            "application/cbor",
        );
        let mut req_meta: std::collections::BTreeMap<String, ciborium::Value> =
            std::collections::BTreeMap::new();
        req_meta.insert(
            "target_cartridge".to_string(),
            ciborium::Value::Text(cartridge_id.to_string()),
        );
        req.meta = Some(req_meta);

        // STREAM_START — adapter-selection takes a single `media:`
        // wildcard input. The file path goes in stream meta so the
        // cartridge can derive the extension when content-only
        // inspection is ambiguous (txtcartridge does this for files
        // whose extension narrows the candidate set).
        let stream_id = "adapter-input".to_string();
        let mut stream_meta: std::collections::BTreeMap<String, ciborium::Value> =
            std::collections::BTreeMap::new();
        stream_meta.insert(
            "file_path".to_string(),
            ciborium::Value::Text(file_path.display().to_string()),
        );
        let mut ss_frame = Frame::stream_start(
            rid.clone(),
            stream_id.clone(),
            "media:".to_string(),
            Some(false),
        );
        ss_frame.meta = Some(stream_meta);

        // CHUNK(s) — CBOR-encoded `Bytes(content)`, one chunk per
        // STREAM_CHUNK_SIZE slice.
        let mut chunk_frames: Vec<Frame> = Vec::new();
        let chunk_count = if content.is_empty() {
            // Empty input still needs one chunk so the cartridge sees
            // `Bytes(Vec::new())` in its stream — matches the
            // build_request_frames convention.
            let mut cbor = Vec::new();
            ciborium::into_writer(&ciborium::Value::Bytes(Vec::new()), &mut cbor)
                .expect("BUG: cbor encode empty bytes");
            let checksum = Frame::compute_checksum(&cbor);
            chunk_frames.push(Frame::chunk(
                rid.clone(),
                stream_id.clone(),
                0,
                cbor,
                0,
                checksum,
            ));
            1u64
        } else {
            for (i, slice) in content.chunks(STREAM_CHUNK_SIZE).enumerate() {
                let mut cbor = Vec::new();
                ciborium::into_writer(&ciborium::Value::Bytes(slice.to_vec()), &mut cbor)
                    .expect("BUG: cbor encode chunk");
                let checksum = Frame::compute_checksum(&cbor);
                chunk_frames.push(Frame::chunk(
                    rid.clone(),
                    stream_id.clone(),
                    0,
                    cbor,
                    i as u64,
                    checksum,
                ));
            }
            chunk_frames.len() as u64
        };

        let se_frame = Frame::stream_end(rid.clone(), stream_id.clone(), chunk_count);
        let end_frame = Frame::end(rid.clone(), None);

        // Register the external request for the targeted cartridge.
        // The relay returns the assigned XID and the response channel;
        // we stamp the XID onto every frame and forward via send_to_master.
        let (xid, mut rx) = self
            .switch
            .register_external_request_for_cartridge(
                rid.clone(),
                CAP_ADAPTER_SELECTION,
                cartridge_id,
            )
            .await
            .map_err(|e| InputResolverError::InspectionFailed {
                path: file_path.to_path_buf(),
                reason: format!(
                    "register adapter-selection request for cartridge '{}' failed: {}",
                    cartridge_id, e
                ),
            })?;

        // Stamp XID onto every frame and send.
        let mut frames = Vec::with_capacity(3 + chunk_frames.len());
        req.routing_id = Some(xid.clone());
        frames.push(req);
        ss_frame.routing_id = Some(xid.clone());
        frames.push(ss_frame);
        for mut chunk in chunk_frames {
            chunk.routing_id = Some(xid.clone());
            frames.push(chunk);
        }
        let mut se_frame = se_frame;
        se_frame.routing_id = Some(xid.clone());
        frames.push(se_frame);
        let mut end_frame = end_frame;
        end_frame.routing_id = Some(xid.clone());
        frames.push(end_frame);

        for frame in frames {
            self.switch.send_to_master(frame, None).await.map_err(|e| {
                InputResolverError::InspectionFailed {
                    path: file_path.to_path_buf(),
                    reason: format!(
                        "send adapter-selection frame to cartridge '{}' failed: {}",
                        cartridge_id, e
                    ),
                }
            })?;
        }

        // Drain response frames. The cartridge produces either an
        // empty END (no match) or STREAM_START + CHUNK(s) + STREAM_END
        // + END where the chunks carry CBOR-encoded `Bytes(json)`.
        let mut accumulated: Vec<u8> = Vec::new();
        let mut got_stream_end = false;
        while let Some(frame) = rx.recv().await {
            match frame.frame_type {
                FrameType::StreamStart => {}
                FrameType::Chunk => {
                    if let Some(payload) = frame.payload {
                        // Each chunk is a CBOR Bytes value. Decode and
                        // append the inner bytes so we accumulate the
                        // full JSON envelope across multiple chunks.
                        let value: ciborium::Value = ciborium::from_reader(payload.as_slice())
                            .map_err(|e| InputResolverError::InspectionFailed {
                                path: file_path.to_path_buf(),
                                reason: format!(
                                    "cartridge '{}' adapter-selection: invalid CBOR chunk: {}",
                                    cartridge_id, e
                                ),
                            })?;
                        match value {
                            ciborium::Value::Bytes(bytes) => accumulated.extend_from_slice(&bytes),
                            other => {
                                return Err(InputResolverError::InspectionFailed {
                                    path: file_path.to_path_buf(),
                                    reason: format!(
                                        "cartridge '{}' adapter-selection: chunk must be CBOR Bytes, got {:?}",
                                        cartridge_id, other
                                    ),
                                });
                            }
                        }
                    }
                }
                FrameType::StreamEnd => {
                    got_stream_end = true;
                }
                FrameType::End => {
                    if !got_stream_end {
                        // Empty END — cartridge declined this file. The
                        // contract distinguishes "no match" (empty END)
                        // from "match" (STREAM_START + chunks + END).
                        return Ok(None);
                    }
                    break;
                }
                FrameType::Err => {
                    let malformed = |field: &str| InputResolverError::InspectionFailed {
                        path: file_path.to_path_buf(),
                        reason: format!(
                            "cartridge '{}' adapter-selection: ERR frame missing required text {}",
                            cartridge_id, field
                        ),
                    };
                    let code = frame.error_code().ok_or_else(|| malformed("code"))?;
                    let message = frame.error_message().ok_or_else(|| malformed("message"))?;
                    frame.attribution_class().map_err(|error| {
                        InputResolverError::InspectionFailed {
                            path: file_path.to_path_buf(),
                            reason: format!(
                                "cartridge '{}' adapter-selection: {}",
                                cartridge_id, error
                            ),
                        }
                    })?;
                    frame.attribution_arg_urn().map_err(|error| {
                        InputResolverError::InspectionFailed {
                            path: file_path.to_path_buf(),
                            reason: format!(
                                "cartridge '{}' adapter-selection: {}",
                                cartridge_id, error
                            ),
                        }
                    })?;
                    return Err(InputResolverError::InspectionFailed {
                        path: file_path.to_path_buf(),
                        reason: format!(
                            "cartridge '{}' adapter-selection failed: [{}] {}",
                            cartridge_id, code, message
                        ),
                    });
                }
                FrameType::Credit => {
                    // Flow-control grant for this call's request-body streams
                    // (L14). Adapter selection exchanges tiny payloads (far
                    // below the initial window), so nothing here ever waits
                    // on credit — a grant is a well-defined no-op.
                    tracing::trace!(
                        cartridge_id = cartridge_id,
                        "adapter-selection: credit grant (no-op)"
                    );
                }
                other => {
                    return Err(InputResolverError::InspectionFailed {
                        path: file_path.to_path_buf(),
                        reason: format!(
                            "cartridge '{}' adapter-selection: unexpected frame type {:?}",
                            cartridge_id, other
                        ),
                    });
                }
            }
        }

        if accumulated.is_empty() {
            // Stream was opened but produced no body — treat the same
            // as an empty END. A correctly-behaving cartridge that
            // wants to match must emit at least the JSON envelope.
            return Ok(None);
        }

        // Decode the JSON envelope. Schema: `{"media_urns": [...]}`.
        let envelope: serde_json::Value = serde_json::from_slice(&accumulated).map_err(|e| {
            InputResolverError::InspectionFailed {
                path: file_path.to_path_buf(),
                reason: format!(
                    "cartridge '{}' adapter-selection: response is not valid JSON: {}",
                    cartridge_id, e
                ),
            }
        })?;
        let media_urns_value =
            envelope
                .get("media_urns")
                .ok_or_else(|| InputResolverError::InspectionFailed {
                    path: file_path.to_path_buf(),
                    reason: format!(
                        "cartridge '{}' adapter-selection: response missing 'media_urns' field",
                        cartridge_id
                    ),
                })?;
        let media_urns_array =
            media_urns_value
                .as_array()
                .ok_or_else(|| InputResolverError::InspectionFailed {
                    path: file_path.to_path_buf(),
                    reason: format!(
                        "cartridge '{}' adapter-selection: 'media_urns' is not an array",
                        cartridge_id
                    ),
                })?;
        let mut media_urns: Vec<String> = Vec::with_capacity(media_urns_array.len());
        for entry in media_urns_array {
            let urn = entry
                .as_str()
                .ok_or_else(|| InputResolverError::InspectionFailed {
                    path: file_path.to_path_buf(),
                    reason: format!(
                        "cartridge '{}' adapter-selection: 'media_urns' must contain strings",
                        cartridge_id
                    ),
                })?;
            media_urns.push(urn.to_string());
        }

        if media_urns.is_empty() {
            // Explicit empty array — cartridge said "no match" via the
            // envelope rather than via empty END. Treat identically.
            return Ok(None);
        }

        Ok(Some(media_urns))
    }
}
