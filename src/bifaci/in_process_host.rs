//! In-Process Cartridge Host — Direct dispatch to FrameHandler trait objects
//!
//! Sits where CartridgeHostRuntime sits (connected to RelaySlave via local socket pair),
//! but routes requests to `Arc<dyn FrameHandler>` trait objects instead of cartridge binaries.
//!
//! ## Architecture
//!
//! ```text
//! RelaySlave ←→ InProcessCartridgeHost ←→ Handler A (streaming frames)
//!                                   ←→ Handler B (streaming frames)
//!                                   ←→ Handler C (streaming frames)
//! ```
//!
//! ## Design
//!
//! The host does NOT accumulate data. On REQ, it spawns a handler task with
//! channels for frame I/O. All continuation frames (STREAM_START, CHUNK, STREAM_END,
//! END) are forwarded to the handler. The handler processes frames natively —
//! streaming or accumulating as it sees fit.
//!
//! This matches how real cartridges work: CartridgeRuntime forwards frames to handlers,
//! and each handler decides how to consume/produce data.

use crate::bifaci::cartridge_runtime::{
    ChannelFrameSender, FrameSender, PeerCall, PeerInvoker, RuntimeError,
};
use crate::bifaci::frame::{FlowKey, Frame, FrameType, Limits, MessageId, SeqAssigner};
use crate::bifaci::io::{CborError, FrameReader, FrameWriter};
use crate::bifaci::relay_switch::{InstalledCartridgeRecord, RelayNotifyCapabilitiesPayload};
use crate::cap::caller::CapArgumentValue;
use crate::cap::definition::Cap;
use crate::standard::caps::CAP_IDENTITY;
use crate::CapUrn;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

// =============================================================================
// FRAME HANDLER TRAIT
// =============================================================================

/// Handler for streaming frame-based requests.
///
/// Handlers receive input frames (STREAM_START, CHUNK, STREAM_END, END) via a
/// channel and send response frames via a ResponseWriter. The host never
/// accumulates — handlers decide how to process input (stream or accumulate).
///
/// Handlers can invoke other caps via `peer` (a PeerInvoker). This mirrors the
/// peer call mechanism in external cartridges: the handler sends a REQ frame
/// through the host, the relay routes it to the destination cap, and response
/// frames (including LOG frames with queue/progress status) flow back to the
/// handler through the PeerResponse.
///
/// For handlers that don't need streaming, use `accumulate_input()` to collect
/// all input streams into `Vec<CapArgumentValue>`.
#[async_trait]
pub trait FrameHandler: Send + Sync + std::fmt::Debug {
    /// Handle a streaming request.
    ///
    /// Called in a dedicated task for each incoming request. The handler reads
    /// input frames from `input` and sends response frames via `output`.
    /// The handler can invoke other caps via `peer`.
    ///
    /// The REQ frame has already been consumed by the host. `input` receives:
    /// STREAM_START, CHUNK, STREAM_END (per argument stream), then END.
    ///
    /// The handler MUST send a complete response: either response frames
    /// (STREAM_START + CHUNK(s) + STREAM_END + END) or an error (via `output.emit_error()`).
    async fn handle_request(
        &self,
        cap_urn: &str,
        input: mpsc::UnboundedReceiver<Frame>,
        output: ResponseWriter,
        peer: Arc<dyn PeerInvoker>,
    );
}

// =============================================================================
// RESPONSE WRITER
// =============================================================================

/// Wraps an output channel with automatic request_id and routing_id stamping.
///
/// All frames sent via ResponseWriter get the correct request_id and routing_id
/// for relay routing. Seq is left at 0 — the wire writer's SeqAssigner handles it.
pub struct ResponseWriter {
    request_id: MessageId,
    routing_id: Option<MessageId>,
    tx: mpsc::UnboundedSender<Frame>,
    max_chunk: usize,
}

impl ResponseWriter {
    fn new(
        request_id: MessageId,
        routing_id: Option<MessageId>,
        tx: mpsc::UnboundedSender<Frame>,
        max_chunk: usize,
    ) -> Self {
        Self {
            request_id,
            routing_id,
            tx,
            max_chunk,
        }
    }

    /// Send a frame, stamping it with the request_id and routing_id.
    pub fn send(&self, mut frame: Frame) {
        frame.id = self.request_id.clone();
        frame.routing_id = self.routing_id.clone();
        frame.seq = 0; // SeqAssigner handles this
        let _ = self.tx.send(frame);
    }

    /// Max chunk size for this connection.
    pub fn max_chunk(&self) -> usize {
        self.max_chunk
    }

    /// Send a complete data response with metadata on STREAM_START.
    pub fn emit_response_with_meta(
        &self,
        media_urn: &str,
        data: &[u8],
        meta: Option<crate::StreamMeta>,
    ) {
        let stream_id = "result".to_string();

        let mut start = Frame::stream_start(
            MessageId::Uint(0),
            stream_id.clone(),
            media_urn.to_string(),
            None,
        );
        start.meta = meta;
        self.send(start);

        if data.is_empty() {
            let mut cbor_payload = Vec::new();
            ciborium::into_writer(&ciborium::Value::Bytes(Vec::new()), &mut cbor_payload)
                .expect("BUG: CBOR encode empty bytes");
            let checksum = Frame::compute_checksum(&cbor_payload);
            self.send(Frame::chunk(
                MessageId::Uint(0),
                stream_id.clone(),
                0,
                cbor_payload,
                0,
                checksum,
            ));
            self.send(Frame::stream_end(MessageId::Uint(0), stream_id, 1));
        } else {
            let chunks: Vec<&[u8]> = data.chunks(self.max_chunk).collect();
            let chunk_count = chunks.len() as u64;
            for (i, chunk_data) in chunks.iter().enumerate() {
                let mut cbor_payload = Vec::new();
                ciborium::into_writer(
                    &ciborium::Value::Bytes(chunk_data.to_vec()),
                    &mut cbor_payload,
                )
                .expect("BUG: CBOR encode chunk bytes");
                let checksum = Frame::compute_checksum(&cbor_payload);
                self.send(Frame::chunk(
                    MessageId::Uint(0),
                    stream_id.clone(),
                    0,
                    cbor_payload,
                    i as u64,
                    checksum,
                ));
            }
            self.send(Frame::stream_end(
                MessageId::Uint(0),
                stream_id,
                chunk_count,
            ));
        }

        self.send(Frame::end_ok(MessageId::Uint(0), None));
    }

    /// Send a complete data response: STREAM_START + CBOR-encoded CHUNK(s) + STREAM_END + END.
    pub fn emit_response(&self, media_urn: &str, data: &[u8]) {
        self.emit_response_with_meta(media_urn, data, None);
    }

    /// Send a list response: STREAM_START + one CHUNK per item + STREAM_END + END.
    ///
    /// Each item is CBOR-encoded and sent as a raw chunk payload (matching
    /// `OutputStream::emit_list_item` semantics). The receiver concatenates
    /// payloads to produce an RFC 8742 CBOR sequence — one self-delimiting
    /// CBOR value per item.
    ///
    /// This differs from `emit_response`, which wraps each chunk in `Bytes()`.
    /// `emit_list_response` is for list-typed cap outputs where the executor's
    /// list path expects a CBOR sequence without transport wrapping.
    pub fn emit_list_response(&self, media_urn: &str, items: &[ciborium::Value]) {
        self.emit_list_response_with_metas(media_urn, items, &[]);
    }

    /// Emit a sequence response with optional per-item metadata.
    /// Each item in `item_metas` corresponds to the item at the same index.
    /// If `item_metas` is shorter than `items`, remaining items get no meta.
    pub fn emit_list_response_with_metas(
        &self,
        media_urn: &str,
        items: &[ciborium::Value],
        item_metas: &[Option<crate::StreamMeta>],
    ) {
        let stream_id = "result".to_string();

        self.send(Frame::stream_start(
            MessageId::Uint(0),
            stream_id.clone(),
            media_urn.to_string(),
            Some(true),
        ));

        for (i, item) in items.iter().enumerate() {
            let mut cbor_payload = Vec::new();
            ciborium::into_writer(item, &mut cbor_payload).expect("BUG: CBOR encode list item");
            let checksum = Frame::compute_checksum(&cbor_payload);
            let mut chunk = Frame::chunk(
                MessageId::Uint(0),
                stream_id.clone(),
                0,
                cbor_payload,
                i as u64,
                checksum,
            );
            if let Some(Some(meta)) = item_metas.get(i) {
                chunk.meta = Some(meta.clone());
            }
            self.send(chunk);
        }

        self.send(Frame::stream_end(
            MessageId::Uint(0),
            stream_id,
            items.len() as u64,
        ));
        self.send(Frame::end_ok(MessageId::Uint(0), None));
    }

    /// Send an error response.
    pub fn emit_error(&self, code: &str, message: &str) {
        self.send(Frame::err(
            MessageId::Uint(0),
            code,
            crate::failure::AttributionClass::Internal,
            message,
            None,
        ));
    }
}

// =============================================================================
// PEER INVOCATION FOR IN-PROCESS HANDLERS
// =============================================================================

/// Tracks a pending peer request from an in-process handler.
/// The main read loop routes response frames to the sender channel.
struct PendingPeerRequest {
    sender: mpsc::UnboundedSender<Frame>,
    origin_request_id: MessageId,
}

/// PeerInvoker implementation for in-process handlers.
///
/// Sends REQ frames through the host's write channel (same channel used for
/// handler responses). The host's main read loop routes response frames back
/// to the PeerCall's receiver via the pending_peer_requests map.
struct InProcessPeerInvoker {
    write_tx: mpsc::UnboundedSender<Frame>,
    pending_requests: Arc<Mutex<HashMap<MessageId, PendingPeerRequest>>>,
    origin_request_id: MessageId,
    max_chunk: usize,
}

#[async_trait]
impl PeerInvoker for InProcessPeerInvoker {
    fn call(&self, cap_urn: &str) -> Result<PeerCall, RuntimeError> {
        let request_id = MessageId::new_uuid();

        // Create channel for response frames
        let (sender, receiver) = mpsc::unbounded_channel();

        // Register before sending REQ
        {
            let mut pending = self.pending_requests.lock().unwrap();
            pending.insert(
                request_id.clone(),
                PendingPeerRequest {
                    sender,
                    origin_request_id: self.origin_request_id.clone(),
                },
            );
        }

        // Send REQ frame through the host's write channel, stamped with parent_rid for cancel cascade
        let mut req_frame = Frame::req(request_id.clone(), cap_urn, vec![], "application/cbor");
        let mut meta = req_frame.meta.take().unwrap_or_default();
        meta.insert(
            "parent_rid".to_string(),
            match &self.origin_request_id {
                MessageId::Uuid(bytes) => ciborium::Value::Bytes(bytes.to_vec()),
                MessageId::Uint(n) => ciborium::Value::Integer((*n as i64).into()),
            },
        );
        req_frame.meta = Some(meta);
        self.write_tx.send(req_frame).map_err(|_| {
            self.pending_requests.lock().unwrap().remove(&request_id);
            RuntimeError::PeerRequest("Host write channel closed".to_string())
        })?;

        // Create FrameSender for PeerCall's arg OutputStreams
        let sender_arc: Arc<dyn FrameSender> = Arc::new(ChannelFrameSender {
            tx: self.write_tx.clone(),
            drops: Arc::new(crate::bifaci::stats::DropCounters::new()),
        });

        Ok(PeerCall {
            sender: sender_arc,
            request_id,
            max_chunk: self.max_chunk,
            response_rx: Some(receiver),
            // In-process dispatch shares one address space — frames land in
            // handler channels directly, so peer args are uncredited here.
            credit_router: None,
            initial_credit: crate::bifaci::frame::DEFAULT_INITIAL_CREDIT,
        })
    }
}

// =============================================================================
// INPUT ACCUMULATION UTILITY
// =============================================================================

/// Accumulate all input streams from a frame channel into CapArgumentValues.
///
/// Reads frames until END. CBOR-decodes chunk payloads to extract raw bytes.
/// For handlers that don't need streaming — they accumulate all input, process,
/// then emit a response.
///
/// Returns Err on CBOR decode failure (protocol violation).
/// Accumulate input frames into argument values and request-level metadata.
///
/// Returns `(args, meta)` where `meta` is the stream metadata from the first
/// input stream's STREAM_START frame. In a ForEach body, this carries
/// provenance context (e.g., "title": "page_3") from the upstream producer.
pub async fn accumulate_input(
    input: &mut mpsc::UnboundedReceiver<Frame>,
) -> Result<(Vec<CapArgumentValue>, Option<crate::StreamMeta>), String> {
    let mut streams: Vec<(String, String, Vec<u8>)> = Vec::new(); // (stream_id, media_urn, data)
    let mut active: HashMap<String, usize> = HashMap::new();
    let mut request_meta: Option<crate::StreamMeta> = None;

    while let Some(frame) = input.recv().await {
        match frame.frame_type {
            FrameType::StreamStart => {
                let sid = frame.stream_id.clone().unwrap_or_default();
                let media_urn = frame.media_urn.clone().unwrap_or_default();
                // Capture meta from the first input stream
                if request_meta.is_none() {
                    request_meta = frame.meta.clone();
                }
                let idx = streams.len();
                streams.push((sid.clone(), media_urn, Vec::new()));
                active.insert(sid, idx);
            }
            FrameType::Chunk => {
                let sid = frame.stream_id.clone().unwrap_or_default();
                if let Some(&idx) = active.get(&sid) {
                    if let Some(payload) = &frame.payload {
                        // CBOR-decode chunk payload to extract raw bytes
                        let value: ciborium::Value =
                            ciborium::from_reader(&payload[..]).map_err(|e| {
                                format!(
                                    "chunk payload is not valid CBOR (stream={}, {} bytes): {}",
                                    sid,
                                    payload.len(),
                                    e
                                )
                            })?;
                        match value {
                            ciborium::Value::Bytes(b) => streams[idx].2.extend_from_slice(&b),
                            ciborium::Value::Text(s) => {
                                streams[idx].2.extend_from_slice(s.as_bytes())
                            }
                            other => {
                                return Err(format!(
                                    "unexpected CBOR type in chunk payload: {:?}",
                                    other
                                ));
                            }
                        }
                    }
                }
            }
            FrameType::StreamEnd => {} // nothing to do
            FrameType::End => break,
            _ => {} // ignore unexpected frame types
        }
    }

    let args = streams
        .into_iter()
        .map(|(_, media_urn, data)| CapArgumentValue::new(media_urn, data))
        .collect();
    Ok((args, request_meta))
}

// =============================================================================
// BUILT-IN IDENTITY HANDLER
// =============================================================================

/// Identity handler: raw byte passthrough (no CBOR decode/encode).
///
/// Echoes all accumulated chunk payloads back as-is. This is the protocol-level
/// identity verification — it proves the transport works end-to-end.
#[derive(Debug)]
struct IdentityHandler;

#[async_trait]
impl FrameHandler for IdentityHandler {
    async fn handle_request(
        &self,
        _cap_urn: &str,
        mut input: mpsc::UnboundedReceiver<Frame>,
        output: ResponseWriter,
        _peer: Arc<dyn PeerInvoker>,
    ) {
        // Accumulate raw payload bytes (no CBOR decode — identity is raw passthrough)
        let mut data = Vec::new();
        while let Some(frame) = input.recv().await {
            match frame.frame_type {
                FrameType::Chunk => {
                    if let Some(p) = &frame.payload {
                        data.extend_from_slice(p);
                    }
                }
                FrameType::End => break,
                _ => {} // STREAM_START, STREAM_END — skip
            }
        }

        // Echo back as a single stream (raw bytes, no CBOR encode)
        let stream_id = "identity".to_string();
        output.send(Frame::stream_start(
            MessageId::Uint(0),
            stream_id.clone(),
            "media:".to_string(),
            None,
        ));

        let checksum = Frame::compute_checksum(&data);
        output.send(Frame::chunk(
            MessageId::Uint(0),
            stream_id.clone(),
            0,
            data,
            0,
            checksum,
        ));

        output.send(Frame::stream_end(MessageId::Uint(0), stream_id, 1));
        output.send(Frame::end_ok(MessageId::Uint(0), None));
    }
}

// =============================================================================
// IN-PROCESS CARTRIDGE HOST
// =============================================================================

/// Entry for a registered in-process handler.
struct HandlerEntry {
    #[allow(dead_code)]
    name: String,
    caps: Vec<Cap>,
    handler: Arc<dyn FrameHandler>,
}

/// Cap table entry: (cap_urn_string, handler_index).
type CapTable = Vec<(String, usize)>;

/// Identity values an `InProcessCartridgeHost` advertises in its
/// RelayNotify payload. The host has no on-disk cartridge directory,
/// so the embedding application must supply the same four-tuple identity
/// (`registry_url`, `channel`, `id`, `version`) it would have read
/// from a cartridge.json — plus a content-derived `sha256` so the
/// engine treats the in-process cartridge indistinguishably from any
/// other installed cartridge.
///
/// The fields propagate verbatim into the RelayNotify
/// `installed_cartridges[*]` entry. Engine-side consumers (e.g.
/// `CapService::get_installed_cartridges`) that key by id-version and
/// require a non-empty sha256 will then accept the in-process entry
/// without special cases.
#[derive(Debug, Clone)]
pub struct InProcessHostIdentity {
    /// Registry URL the embedding binary was built for. `None` ⇔ dev
    /// build. Pass through verbatim from the binary's compile-time
    /// `option_env!("MFR_CARTRIDGE_REGISTRY_URL")` (or equivalent).
    pub registry_url: Option<String>,
    /// Channel the embedding binary was built for. Comes from the
    /// same compile-time constant the embedding binary uses to
    /// validate sibling-cartridge channels.
    pub channel: crate::bifaci::cartridge_repo::CartridgeChannel,
    /// Stable id for this host. Should match `CARGO_PKG_NAME`-style
    /// canonical lowercase identity that is unique per binary.
    pub id: String,
    /// Version of the embedding binary. Pass through verbatim from
    /// `env!("CARGO_PKG_VERSION")` or equivalent.
    pub version: String,
    /// Content-derived hash of the host. The engine asserts this is
    /// non-empty; supply the binary's content sha256 (or any
    /// stable-per-build digest) so the in-process entry has a real
    /// identity hash like an on-disk cartridge.
    pub sha256: String,
}

impl InProcessHostIdentity {
    /// Identity values for unit/integration tests. Carries a
    /// fixed-bytes sha256 so the engine's non-empty-hash assertion
    /// passes; channel and version are stable test defaults. Tests
    /// that need to distinguish multiple in-process hosts pass
    /// distinct ids via this builder.
    pub fn for_test(id: impl Into<String>) -> Self {
        Self {
            registry_url: None,
            channel: crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            id: id.into(),
            version: "0.0.0-test".to_string(),
            sha256: "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        }
    }
}

/// A cartridge host that dispatches to in-process FrameHandler implementations.
///
/// Speaks the Frame protocol to a RelaySlave, but routes requests to
/// `Arc<dyn FrameHandler>` trait objects via frame channels — no accumulation
/// at the host level, handlers own the streaming.
pub struct InProcessCartridgeHost {
    identity: InProcessHostIdentity,
    handlers: Vec<HandlerEntry>,
}

impl std::fmt::Debug for InProcessCartridgeHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcessCartridgeHost")
            .field("identity_id", &self.identity.id)
            .field("identity_version", &self.identity.version)
            .field("handler_count", &self.handlers.len())
            .finish()
    }
}

impl InProcessCartridgeHost {
    /// Create a new in-process cartridge host with the given handlers.
    ///
    /// `identity` declares the host's on-the-wire identity (the
    /// embedding binary supplies it; see `InProcessHostIdentity`).
    /// Each handler is a tuple of (name, caps, handler).
    pub fn new(
        identity: InProcessHostIdentity,
        handlers: Vec<(String, Vec<Cap>, Arc<dyn FrameHandler>)>,
    ) -> Self {
        let handlers = handlers
            .into_iter()
            .map(|(name, caps, handler)| HandlerEntry {
                name,
                caps,
                handler,
            })
            .collect();
        Self { identity, handlers }
    }

    /// Build the aggregate RelayNotify manifest payload.
    ///
    /// The host has no on-disk cartridge directory, but the embedder
    /// supplies an `InProcessHostIdentity` that mirrors a real
    /// cartridge install's identity tuple. We assemble one
    /// `InstalledCartridgeRecord` from that and put every
    /// handler-contributed cap into its lone cap group. The wire
    /// format is symmetric with out-of-process hosts: the engine
    /// reads `cap_groups` from `installed_cartridges` and derives the
    /// flat cap list itself.
    fn build_manifest(&self) -> Vec<u8> {
        use crate::bifaci::manifest::CapGroup as ManifestCapGroup;
        use crate::bifaci::relay_switch::InstalledCartridgeRecord;

        // Collect all handler caps; prepend CAP_IDENTITY exactly once.
        let mut caps: Vec<Cap> = vec![crate::standard::caps::identity_cap()];
        for entry in &self.handlers {
            for cap in &entry.caps {
                if cap.urn.to_string() != CAP_IDENTITY {
                    caps.push(cap.clone());
                }
            }
        }

        let cap_urn_list: Vec<String> = caps.iter().map(|c| c.urn.to_string()).collect();
        let cartridge = InstalledCartridgeRecord {
            registry_url: self.identity.registry_url.clone(),
            channel: self.identity.channel,
            id: self.identity.id.clone(),
            version: self.identity.version.clone(),
            sha256: self.identity.sha256.clone(),
            cap_groups: vec![ManifestCapGroup {
                name: self.identity.id.clone(),
                caps,
                adapter_urns: Vec::new(),
            }],
            attachment_error: None,
            runtime_stats: Some(crate::bifaci::relay_switch::CartridgeRuntimeStats {
                running: true,
                // In-process handlers are task-backed and have no fixed
                // concurrency ceiling: every pool is unlimited, at rest at
                // manifest-build time.
                pools: Self::pool_states_snapshot(&cap_urn_list, &HashMap::new()),
                pid: Some(std::process::id()),
                active_request_count: 0,
                peer_request_count: 0,
                memory_footprint_mb: 0,
                memory_rss_mb: 0,
                last_heartbeat_unix_seconds: None,
                restart_count: 0,
                protocol_drops_total: None,
                protocol_stragglers_total: None,
                protocol_overruns_total: None,
            }),
            // In-process cartridges have no on-disk presence to
            // inspect and no registry to verify against — the
            // embedder constructed them directly. They are
            // operational from the moment the host advertises them.
            lifecycle: crate::bifaci::relay_switch::CartridgeLifecycle::Operational,
        };

        let payload = RelayNotifyCapabilitiesPayload::new(vec![cartridge]);
        serde_json::to_vec(&payload)
            .expect("BUG: InProcessCartridgeHost RelayNotify payload must serialize")
    }

    /// Build the cap table for routing: flat list of (cap_urn, handler_idx).
    fn build_cap_table(handlers: &[HandlerEntry]) -> CapTable {
        let mut table = Vec::new();
        for (idx, entry) in handlers.iter().enumerate() {
            for cap in &entry.caps {
                table.push((cap.urn.to_string(), idx));
            }
        }
        table
    }

    /// Find the best handler for a cap URN.
    ///
    /// Uses `is_dispatchable(candidate, request)` to find handlers that can
    /// legally handle the request, then ranks by specificity.
    ///
    /// Ranking prefers:
    /// 1. Equivalent matches (distance 0)
    /// 2. More specific candidates (positive distance) - refinements
    /// 3. More generic candidates (negative distance) - fallbacks
    fn find_handler_for_cap(cap_table: &CapTable, cap_urn: &str) -> Option<(usize, String)> {
        let request_urn = match CapUrn::from_string(cap_urn) {
            Ok(u) => u,
            Err(_) => return None,
        };

        let request_specificity = request_urn.specificity();
        // (handler_idx, registered cap, signed_distance)
        let mut matches: Vec<(usize, String, isize)> = Vec::new();

        for (registered_cap, handler_idx) in cap_table {
            if let Ok(registered_urn) = CapUrn::from_string(registered_cap) {
                // Use is_dispatchable: can this candidate handle this request?
                if registered_urn.is_dispatchable(&request_urn) {
                    let specificity = registered_urn.specificity();
                    let signed_distance = specificity as isize - request_specificity as isize;
                    matches.push((*handler_idx, registered_urn.to_string(), signed_distance));
                }
            }
        }

        if matches.is_empty() {
            return None;
        }

        // Ranking: prefer equivalent (0), then more specific (+), then more generic (-)
        matches.sort_by(|a, b| {
            let (_, _, dist_a) = a;
            let (_, _, dist_b) = b;

            // First: non-negative distances before negative
            match (dist_a >= &0, dist_b >= &0) {
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                _ => {
                    // Same sign: prefer smaller absolute distance
                    dist_a.unsigned_abs().cmp(&dist_b.unsigned_abs())
                }
            }
        });

        matches
            .into_iter()
            .next()
            .map(|(idx, registered, _)| (idx, registered))
    }

    /// The host's full pool-state map: one at-rest singleton per advertised
    /// cap plus the mandatory `all` pool, with live per-cap active counts.
    /// In-process handlers are task-backed with no fixed concurrency
    /// ceiling, so every capacity is 0 (unlimited) and nothing ever queues.
    fn pool_states_snapshot(
        cap_urns: &[String],
        serving: &HashMap<MessageId, String>,
    ) -> crate::bifaci::pools::PoolStates {
        let mut states = crate::bifaci::pools::PoolStates::new();
        let mut all_active: u64 = 0;
        for cap in cap_urns {
            let active = serving.values().filter(|c| *c == cap).count() as u64;
            all_active += active;
            states.insert(
                cap.clone(),
                crate::bifaci::pools::PoolState {
                    declared: 0,
                    configured: 0,
                    available: None,
                    active,
                    queued: 0,
                    caps: Vec::new(),
                },
            );
        }
        states.insert(
            crate::bifaci::pools::POOL_ALL.to_string(),
            crate::bifaci::pools::PoolState {
                declared: 0,
                configured: 0,
                available: None,
                active: all_active,
                queued: 0,
                caps: cap_urns.to_vec(),
            },
        );
        states
    }

    /// Run the host. Returns when the local connection closes.
    ///
    /// `local_read` / `local_write` connect to the RelaySlave's local side.
    pub async fn run<
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    >(
        self,
        local_read: R,
        local_write: W,
    ) -> Result<(), CborError> {
        let mut reader = FrameReader::new(local_read);

        // Writer runs in a separate task with SeqAssigner
        let (write_tx, mut write_rx) = mpsc::unbounded_channel::<Frame>();
        let writer_task = tokio::spawn(async move {
            let mut writer = FrameWriter::new(local_write);
            let mut seq_assigner = SeqAssigner::new();

            while let Some(mut frame) = write_rx.recv().await {
                seq_assigner.assign(&mut frame);
                if let Err(e) = writer.write(&frame).await {
                    tracing::error!("[InProcessCartridgeHost] writer error: {}", e);
                    break;
                }
                if matches!(frame.frame_type, FrameType::End | FrameType::Err) {
                    seq_assigner.remove(&FlowKey::from_frame(&frame));
                }
            }
        });

        // Send initial RelayNotify with aggregate caps
        let manifest = self.build_manifest();
        let notify = Frame::relay_notify(&manifest, &Limits::default());
        write_tx
            .send(notify)
            .map_err(|_| CborError::Protocol("writer channel closed on startup".into()))?;

        // Move handlers to Arc for sharing with handler tasks
        let handlers = Arc::new(self.handlers);
        let cap_table = Self::build_cap_table(&handlers);

        // Advertised caps (canonical URNs) — the singleton pool roster.
        let mut advertised_caps: Vec<String> = vec![CAP_IDENTITY.to_string()];
        for entry in handlers.iter() {
            for cap in &entry.caps {
                let urn = cap.urn.to_string();
                if !advertised_caps.contains(&urn) {
                    advertised_caps.push(urn);
                }
            }
        }
        // Live per-request cap attribution for pool `active` counts:
        // request_id → registered cap URN, removed when the handler task
        // completes (or the request is cancelled).
        let serving: Arc<Mutex<HashMap<MessageId, String>>> =
            Arc::new(Mutex::new(HashMap::new()));
        // Operator `configured` overlay delivered via heartbeat probes.
        // The in-process host enforces nothing itself (handlers are
        // task-backed); reporting the configured values back in the pool
        // map is what makes the ENGINE's admission enforce them.
        let mut configured_overlay: crate::bifaci::pools::DesiredCapacities =
            crate::bifaci::pools::DesiredCapacities::new();

        // Active request channels: request_id → input_tx for forwarding frames to handler
        let mut active: HashMap<MessageId, mpsc::UnboundedSender<Frame>> = HashMap::new();
        // Handler JoinHandles for per-request abort on Cancel
        let mut handler_handles: HashMap<MessageId, JoinHandle<()>> = HashMap::new();

        // Pending peer requests: peer_rid → sender for routing response frames back
        // Shared with InProcessPeerInvoker instances (handlers insert, main loop routes)
        let pending_peer_requests: Arc<Mutex<HashMap<MessageId, PendingPeerRequest>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Built-in identity handler
        let identity_handler: Arc<dyn FrameHandler> = Arc::new(IdentityHandler);

        let max_chunk = Limits::default().max_chunk;

        // Main read loop — forward frames to handlers or peer response channels
        loop {
            let frame = match reader.read().await {
                Ok(Some(f)) => f,
                Ok(None) => {
                    break;
                }
                Err(e) => {
                    tracing::error!("[InProcessCartridgeHost] read error: {}", e);
                    break;
                }
            };

            match frame.frame_type {
                FrameType::Req => {
                    let rid = frame.id.clone();
                    let xid = frame.routing_id.clone();
                    let cap_urn = match &frame.cap {
                        Some(c) => c.clone(),
                        None => {
                            let mut err = Frame::err(
                                rid,
                                "PROTOCOL_ERROR",
                                crate::failure::AttributionClass::Internal,
                                "REQ missing cap URN",
                                None,
                            );
                            err.routing_id = xid;
                            let _ = write_tx.send(err);
                            continue;
                        }
                    };

                    // Identity cap is CAP_IDENTITY / `cap:effect=none` —
                    // exact string match, NOT conforms_to.
                    let is_identity = cap_urn == CAP_IDENTITY;

                    let (handler, registered_cap): (Arc<dyn FrameHandler>, String) = if is_identity
                    {
                        (Arc::clone(&identity_handler), CAP_IDENTITY.to_string())
                    } else {
                        match Self::find_handler_for_cap(&cap_table, &cap_urn) {
                            Some((idx, registered)) => {
                                (Arc::clone(&handlers[idx].handler), registered)
                            }
                            None => {
                                // No registered handler for a dispatched cap
                                // is a deployment mismatch — Environment.
                                let mut err = Frame::err(
                                    rid,
                                    "NO_HANDLER",
                                    crate::failure::AttributionClass::Environment,
                                    &format!("no handler for cap: {}", cap_urn),
                                    None,
                                );
                                err.routing_id = xid;
                                let _ = write_tx.send(err);
                                continue;
                            }
                        }
                    };

                    // Create channel for forwarding frames to handler
                    let (input_tx, input_rx) = mpsc::unbounded_channel::<Frame>();
                    active.insert(rid.clone(), input_tx);

                    // Create peer invoker for this handler
                    let peer: Arc<dyn PeerInvoker> = Arc::new(InProcessPeerInvoker {
                        write_tx: write_tx.clone(),
                        pending_requests: pending_peer_requests.clone(),
                        origin_request_id: rid.clone(),
                        max_chunk,
                    });

                    // Spawn handler task
                    let output =
                        ResponseWriter::new(rid.clone(), xid.clone(), write_tx.clone(), max_chunk);
                    let cap_urn_owned = cap_urn.clone();
                    let handler_rid = rid.clone();
                    serving
                        .lock()
                        .expect("serving mutex poisoned")
                        .insert(rid.clone(), registered_cap);
                    let serving_done = serving.clone();
                    let done_rid = rid.clone();
                    let handle = tokio::spawn(async move {
                        handler
                            .handle_request(&cap_urn_owned, input_rx, output, peer)
                            .await;
                        serving_done
                            .lock()
                            .expect("serving mutex poisoned")
                            .remove(&done_rid);
                    });
                    handler_handles.insert(handler_rid, handle);
                }

                // Continuation frames: forward to active request or peer response
                FrameType::StreamStart
                | FrameType::Chunk
                | FrameType::StreamEnd
                | FrameType::Log => {
                    // Try active request first (incoming request continuation)
                    if let Some(tx) = active.get(&frame.id) {
                        let _ = tx.send(frame);
                        continue;
                    }

                    // Try peer response (response to handler's peer call)
                    let pending = pending_peer_requests.lock().unwrap();
                    if let Some(pr) = pending.get(&frame.id) {
                        let _ = pr.sender.send(frame);
                    } else {
                        tracing::warn!("[InProcessCartridgeHost] {:?} rid={:?} not found in active or pending_peer_requests", frame.frame_type, frame.id);
                    }
                    drop(pending);
                }

                FrameType::End => {
                    // Try active request first — send END then remove
                    if let Some(tx) = active.remove(&frame.id) {
                        let rid = frame.id.clone();
                        let _ = tx.send(frame);
                        // Clean up handler handle (handler will exit naturally after receiving END)
                        handler_handles.remove(&rid);
                        continue;
                    }

                    // Try peer response — send END then remove
                    let mut pending = pending_peer_requests.lock().unwrap();
                    if let Some(pr) = pending.remove(&frame.id) {
                        let _ = pr.sender.send(frame);
                    }
                    drop(pending);
                }

                FrameType::Err => {
                    tracing::error!(
                        "[InProcessCartridgeHost] ERR received: rid={:?} code={:?} msg={:?}",
                        frame.id,
                        frame.error_code(),
                        frame.error_message()
                    );
                    // Try active request first — forward ERR then remove
                    if let Some(tx) = active.remove(&frame.id) {
                        let rid = frame.id.clone();
                        let _ = tx.send(frame);
                        handler_handles.remove(&rid);
                        continue;
                    }

                    // Try peer response
                    let mut pending = pending_peer_requests.lock().unwrap();
                    if let Some(pr) = pending.remove(&frame.id) {
                        let _ = pr.sender.send(frame);
                    }
                    drop(pending);
                }

                FrameType::CloseStream => {
                    // An in-process handler holds no live feed taps: a
                    // CloseStream has nothing to close and never aborts the
                    // request — it is logged and the request continues.
                    tracing::warn!(
                        target: "in_process_host",
                        rid = ?frame.id,
                        "[InProcessHost] CloseStream for a request with no live feed — nothing to close, request continues"
                    );
                    continue;
                }

                FrameType::Cancel => {
                    let target_rid = frame.id.clone();
                    let xid = frame.routing_id.clone();
                    // The attribution rides in meta like an ERR's; an
                    // unattributed Cancel is still a cancel.
                    let reason = frame
                        .cancel_reason()
                        .expect("a Cancel frame always yields a reason");

                    // Drop active sender → handler's input recv() returns None
                    active.remove(&target_rid);
                    serving
                        .lock()
                        .expect("serving mutex poisoned")
                        .remove(&target_rid);

                    // Abort handler JoinHandle
                    if let Some(handle) = handler_handles.remove(&target_rid) {
                        handle.abort();
                    }

                    // Cancel peer calls originating from this request
                    {
                        let mut pending = pending_peer_requests.lock().unwrap();
                        let peer_rids_to_cancel: Vec<MessageId> = pending
                            .iter()
                            .filter(|(_, pr)| pr.origin_request_id == target_rid)
                            .map(|(rid, _)| rid.clone())
                            .collect();
                        for peer_rid in &peer_rids_to_cancel {
                            pending.remove(peer_rid);
                            let cancel = Frame::cancel(peer_rid.clone(), &reason);
                            let _ = write_tx.send(cancel);
                        }
                    }

                    // Terminal ERR under the cancel's own cause
                    let mut err = Frame::err(
                        target_rid,
                        reason.terminal_code(),
                        reason.terminal_class(),
                        &reason.terminal_message(),
                        None,
                    );
                    err.routing_id = xid;
                    let _ = write_tx.send(err);
                }

                FrameType::Heartbeat => {
                    // The heartbeat is the capacity CONFIG channel (see
                    // cartridge_runtime): a probe may carry the operator's
                    // desired `configured` values. Validate the whole batch
                    // against the pool roster first — an unknown pool
                    // refuses it all with an ERR naming it.
                    if let Some(bytes) = frame.desired_capacity_bytes() {
                        let desired = match crate::bifaci::pools::decode_desired(bytes) {
                            Ok(desired) => desired,
                            Err(reason) => {
                                let err = Frame::err(
                                    frame.id,
                                    "UNKNOWN_POOL",
                                    crate::failure::AttributionClass::Internal,
                                    &format!("desired capacities refused: {reason}"),
                                    None,
                                );
                                let _ = write_tx.send(err);
                                continue;
                            }
                        };
                        if let Some(unknown) = desired.keys().find(|name| {
                            name.as_str() != crate::bifaci::pools::POOL_ALL
                                && !advertised_caps.contains(name)
                        }) {
                            let err = Frame::err(
                                frame.id,
                                "UNKNOWN_POOL",
                                crate::failure::AttributionClass::Internal,
                                &format!(
                                    "desired capacities refused: unknown pool '{unknown}'"
                                ),
                                None,
                            );
                            let _ = write_tx.send(err);
                            continue;
                        }
                        configured_overlay.extend(desired);
                    }
                    let mut states = Self::pool_states_snapshot(
                        &advertised_caps,
                        &serving.lock().expect("serving mutex poisoned"),
                    );
                    for (name, configured) in &configured_overlay {
                        states
                            .get_mut(name)
                            .expect("overlay pools validated on receipt")
                            .configured = *configured;
                    }
                    let mut response = Frame::heartbeat(frame.id.clone());
                    response.meta.get_or_insert_default().insert(
                        crate::bifaci::pools::META_POOLS.to_string(),
                        ciborium::Value::Bytes(crate::bifaci::pools::encode_pool_states(
                            &states,
                        )),
                    );
                    let _ = write_tx.send(response);
                }

                _ => {
                    // RelayNotify, RelayState, etc. — not expected from relay side
                }
            }
        }

        // Drop all active channels to signal handlers to exit
        active.clear();

        // Abort any remaining handler tasks
        for (_, handle) in handler_handles {
            handle.abort();
        }

        drop(write_tx);
        let _ = writer_task.await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bifaci::frame::CancelReason;
    use crate::bifaci::decode_chunk_payload;
    use crate::bifaci::io::{FrameReader, FrameWriter};
    use crate::bifaci::local_socket::UnixStream;
    use crate::Cap;
    use tokio::io::{BufReader, BufWriter};

    /// Echo handler: accumulates input, echoes raw bytes back.
    #[derive(Debug)]
    struct EchoHandler;

    #[async_trait]
    impl FrameHandler for EchoHandler {
        async fn handle_request(
            &self,
            _cap_urn: &str,
            mut input: mpsc::UnboundedReceiver<Frame>,
            output: ResponseWriter,
            _peer: Arc<dyn PeerInvoker>,
        ) {
            match accumulate_input(&mut input).await {
                Ok((args, meta)) => {
                    let data: Vec<u8> = args.iter().flat_map(|a| a.value.clone()).collect();
                    output.emit_response_with_meta("media:", &data, meta);
                }
                Err(e) => {
                    output.emit_error("ACCUMULATE_ERROR", &e);
                }
            }
        }
    }

    fn make_test_cap(urn_str: &str) -> Cap {
        Cap {
            urn: CapUrn::from_string(urn_str).unwrap(),
            version: 1,
            title: "test".to_string(),
            cap_description: None,
            documentation: None,
            metadata: HashMap::new(),
            aliases: vec![String::new()],
            is_abstract: false,
            args: Vec::new(),
            output: None,
            metadata_json: None,
            registered_by: None,
            supported_model_types: Vec::new(),
            default_model_spec: None,
        }
    }

    /// Build a CBOR-encoded chunk payload from raw bytes (matching build_request_frames).
    fn cbor_bytes_payload(data: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        ciborium::into_writer(&ciborium::Value::Bytes(data.to_vec()), &mut buf)
            .expect("BUG: CBOR encode");
        buf
    }

    // TEST6748: InProcessCartridgeHost routes REQ to matching handler and returns response
    #[tokio::test]
    async fn test6748_routes_req_to_handler() {
        let cap_urn = "cap:in=\"media:text\";echo;out=\"media:text\"";
        let cap = make_test_cap(cap_urn);
        let handlers = vec![(
            "echo".to_string(),
            vec![cap],
            Arc::new(EchoHandler) as Arc<dyn FrameHandler>,
        )];

        let host = InProcessCartridgeHost::new(
            InProcessHostIdentity::for_test("in-process-test"),
            handlers,
        );

        let (host_sock, test_sock) = UnixStream::pair().unwrap();
        let (host_read, host_write) = host_sock.into_split();
        let (test_read, test_write) = test_sock.into_split();

        let host_task = tokio::spawn(async move { host.run(host_read, host_write).await });

        let mut reader = FrameReader::new(BufReader::new(test_read));
        let mut writer = FrameWriter::new(BufWriter::new(test_write));

        // First frame should be RelayNotify with manifest
        let notify = reader.read().await.unwrap().unwrap();
        assert_eq!(notify.frame_type, FrameType::RelayNotify);
        let manifest = notify.relay_notify_manifest().unwrap();
        let payload: RelayNotifyCapabilitiesPayload = serde_json::from_slice(manifest).unwrap();
        let caps = payload.cap_urns();
        assert!(caps.len() >= 2); // identity + echo cap
        assert_eq!(caps[0], CAP_IDENTITY);
        // The InProcessCartridgeHost wraps its handlers in one synthetic
        // installed-cartridge entry so the wire schema (cap_groups inside
        // installed_cartridges) is satisfied.
        assert_eq!(payload.installed_cartridges.len(), 1);
        // The in-process host now declares whatever identity its
        // embedder supplied (we use `for_test("in-process-test")`
        // / `for_test("thumb-host")` etc above) — the manifest
        // round-trips that id verbatim, no synthetic placeholder.
        assert_eq!(payload.installed_cartridges[0].id, "in-process-test");

        // Send a REQ + STREAM_START + CHUNK (CBOR-encoded) + STREAM_END + END
        let rid = MessageId::new_uuid();
        let mut req = Frame::req(rid.clone(), cap_urn, vec![], "application/cbor");
        req.routing_id = Some(MessageId::Uint(1));
        writer.write(&req).await.unwrap();

        let ss = Frame::stream_start(
            rid.clone(),
            "arg0".to_string(),
            "media:text".to_string(),
            None,
        );
        writer.write(&ss).await.unwrap();

        let payload = cbor_bytes_payload(b"hello world");
        let checksum = Frame::compute_checksum(&payload);
        let chunk = Frame::chunk(rid.clone(), "arg0".to_string(), 0, payload, 0, checksum);
        writer.write(&chunk).await.unwrap();

        let se = Frame::stream_end(rid.clone(), "arg0".to_string(), 1);
        writer.write(&se).await.unwrap();

        let end = Frame::end(rid.clone(), None);
        writer.write(&end).await.unwrap();

        // Read response: STREAM_START + CHUNK (CBOR-encoded) + STREAM_END + END
        let resp_ss = reader.read().await.unwrap().unwrap();
        assert_eq!(resp_ss.frame_type, FrameType::StreamStart);
        assert_eq!(resp_ss.id, rid);
        assert_eq!(resp_ss.stream_id.as_deref(), Some("result"));

        let resp_chunk = reader.read().await.unwrap().unwrap();
        assert_eq!(resp_chunk.frame_type, FrameType::Chunk);
        let resp_data = decode_chunk_payload(resp_chunk.payload.as_deref().unwrap()).unwrap();
        assert_eq!(resp_data, b"hello world");

        let resp_se = reader.read().await.unwrap().unwrap();
        assert_eq!(resp_se.frame_type, FrameType::StreamEnd);

        let resp_end = reader.read().await.unwrap().unwrap();
        assert_eq!(resp_end.frame_type, FrameType::End);

        drop(writer);
        drop(reader);
        host_task.await.unwrap().unwrap();
    }

    // TEST1961: the in-process host answers a Cancel in the cancel's OWN
    // attribution — ERR ABORTED/resource for a host abort (message carries
    // the reason), ERR ABORTED_COLLATERAL with the originating failure's
    // class for collateral, ERR CANCELLED/user for an operator's cancel, and
    // ERR CANCELLED/internal for an UNATTRIBUTED cancel, which still
    // cancels. A CloseStream is a no-op for a handler with no live feed: the
    // request continues and completes normally with END.
    #[tokio::test]
    async fn test1961_cancel_terminal_carries_its_attribution() {
        use crate::failure::AttributionClass;
        let cap_urn = "cap:in=\"media:text\";echo;out=\"media:text\"";
        let run = |control: Frame| async move {
            let cap = make_test_cap(cap_urn);
            let handlers = vec![(
                "echo".to_string(),
                vec![cap],
                Arc::new(EchoHandler) as Arc<dyn FrameHandler>,
            )];
            let host = InProcessCartridgeHost::new(
                InProcessHostIdentity::for_test("in-process-test"),
                handlers,
            );
            let (host_sock, test_sock) = UnixStream::pair().unwrap();
            let (host_read, host_write) = host_sock.into_split();
            let (test_read, test_write) = test_sock.into_split();
            let host_task = tokio::spawn(async move { host.run(host_read, host_write).await });
            let mut reader = FrameReader::new(BufReader::new(test_read));
            let mut writer = FrameWriter::new(BufWriter::new(test_write));
            let notify = reader.read().await.unwrap().unwrap();
            assert_eq!(notify.frame_type, FrameType::RelayNotify);

            // Open the request and its input stream, but do not END it —
            // the handler is active when the control frame arrives.
            let rid = MessageId::new_uuid();
            let mut req = Frame::req(rid.clone(), cap_urn, vec![], "application/cbor");
            req.routing_id = Some(MessageId::Uint(1));
            writer.write(&req).await.unwrap();
            let ss = Frame::stream_start(
                rid.clone(),
                "arg0".to_string(),
                "media:text".to_string(),
                None,
            );
            writer.write(&ss).await.unwrap();

            let is_close = control.frame_type == FrameType::CloseStream;
            let mut control = control;
            control.id = rid.clone();
            control.routing_id = Some(MessageId::Uint(1));
            writer.write(&control).await.unwrap();

            let outcome = if is_close {
                // Finish the request: it was never cancelled.
                let payload = cbor_bytes_payload(b"still here");
                let checksum = Frame::compute_checksum(&payload);
                writer
                    .write(&Frame::chunk(rid.clone(), "arg0".to_string(), 0, payload, 0, checksum))
                    .await
                    .unwrap();
                writer
                    .write(&Frame::stream_end(rid.clone(), "arg0".to_string(), 1))
                    .await
                    .unwrap();
                writer.write(&Frame::end(rid.clone(), None)).await.unwrap();
                let mut last = None;
                loop {
                    let frame = reader.read().await.unwrap().unwrap();
                    assert_eq!(frame.id, rid);
                    assert_ne!(frame.frame_type, FrameType::Err, "a CloseStream never aborts");
                    let done = frame.frame_type == FrameType::End;
                    last = Some(frame);
                    if done {
                        break;
                    }
                }
                last.unwrap()
            } else {
                let frame = reader.read().await.unwrap().unwrap();
                assert_eq!(frame.id, rid);
                frame
            };
            drop(writer);
            drop(reader);
            host_task.await.unwrap().unwrap();
            outcome
        };
        let cancel = |reason: CancelReason| Frame::cancel(MessageId::Uint(0), &reason);

        let err = run(cancel(CancelReason::host(AttributionClass::Resource, "memory pressure relief", false))).await;
        assert_eq!(err.frame_type, FrameType::Err);
        assert_eq!(err.error_code(), Some("ABORTED"));
        assert_eq!(err.attribution_class().unwrap(), AttributionClass::Resource);
        assert!(err.error_message().unwrap_or("").contains("memory pressure relief"));

        let err = run(cancel(CancelReason::collateral(AttributionClass::Input, "step s1 failed"))).await;
        assert_eq!(err.error_code(), Some("ABORTED_COLLATERAL"));
        assert_eq!(err.attribution_class().unwrap(), AttributionClass::Input);

        let err = run(cancel(CancelReason::user(false))).await;
        assert_eq!(err.error_code(), Some("CANCELLED"));
        assert_eq!(err.attribution_class().unwrap(), AttributionClass::User);

        let err = run(cancel(CancelReason::unattributed(false))).await;
        assert_eq!(err.error_code(), Some("CANCELLED"), "an unattributed Cancel still cancels");
        assert_eq!(err.attribution_class().unwrap(), AttributionClass::Internal);

        let end = run(Frame::close_stream(MessageId::Uint(0), None)).await;
        assert_eq!(end.frame_type, FrameType::End);
    }

    // TEST6749: InProcessCartridgeHost handles identity verification (echo nonce)
    #[tokio::test]
    async fn test6749_identity_verification() {
        let host =
            InProcessCartridgeHost::new(InProcessHostIdentity::for_test("in-process-test"), vec![]);

        let (host_sock, test_sock) = UnixStream::pair().unwrap();
        let (host_read, host_write) = host_sock.into_split();
        let (test_read, test_write) = test_sock.into_split();

        let host_task = tokio::spawn(async move { host.run(host_read, host_write).await });

        let mut reader = FrameReader::new(BufReader::new(test_read));
        let mut writer = FrameWriter::new(BufWriter::new(test_write));

        // Skip RelayNotify
        let _notify = reader.read().await.unwrap().unwrap();

        // Send identity verification
        let rid = MessageId::new_uuid();
        let mut req = Frame::req(rid.clone(), CAP_IDENTITY, vec![], "application/cbor");
        req.routing_id = Some(MessageId::Uint(0));
        writer.write(&req).await.unwrap();

        // Send nonce via stream (already CBOR-encoded by identity_nonce)
        let nonce = crate::bifaci::io::identity_nonce();
        let ss = Frame::stream_start(
            rid.clone(),
            "identity-verify".to_string(),
            "media:".to_string(),
            None,
        );
        writer.write(&ss).await.unwrap();

        let checksum = Frame::compute_checksum(&nonce);
        let chunk = Frame::chunk(
            rid.clone(),
            "identity-verify".to_string(),
            0,
            nonce.clone(),
            0,
            checksum,
        );
        writer.write(&chunk).await.unwrap();

        let se = Frame::stream_end(rid.clone(), "identity-verify".to_string(), 1);
        writer.write(&se).await.unwrap();

        let end = Frame::end(rid.clone(), None);
        writer.write(&end).await.unwrap();

        // Read echoed response — identity echoes raw bytes (no CBOR decode/encode)
        let resp_ss = reader.read().await.unwrap().unwrap();
        assert_eq!(resp_ss.frame_type, FrameType::StreamStart);

        let resp_chunk = reader.read().await.unwrap().unwrap();
        assert_eq!(resp_chunk.frame_type, FrameType::Chunk);
        assert_eq!(resp_chunk.payload.as_deref(), Some(nonce.as_slice()));

        let resp_se = reader.read().await.unwrap().unwrap();
        assert_eq!(resp_se.frame_type, FrameType::StreamEnd);

        let resp_end = reader.read().await.unwrap().unwrap();
        assert_eq!(resp_end.frame_type, FrameType::End);

        drop(writer);
        drop(reader);
        host_task.await.unwrap().unwrap();
    }

    // TEST6750: InProcessCartridgeHost returns NO_HANDLER for unregistered cap
    #[tokio::test]
    async fn test6750_no_handler_returns_err() {
        let host =
            InProcessCartridgeHost::new(InProcessHostIdentity::for_test("in-process-test"), vec![]);

        let (host_sock, test_sock) = UnixStream::pair().unwrap();
        let (host_read, host_write) = host_sock.into_split();
        let (test_read, test_write) = test_sock.into_split();

        let host_task = tokio::spawn(async move { host.run(host_read, host_write).await });

        let mut reader = FrameReader::new(BufReader::new(test_read));
        let mut writer = FrameWriter::new(BufWriter::new(test_write));

        // Skip RelayNotify
        let _notify = reader.read().await.unwrap().unwrap();

        let rid = MessageId::new_uuid();
        let mut req = Frame::req(
            rid.clone(),
            "cap:in=\"media:ext=pdf\";unknown;out=\"media:text\"",
            vec![],
            "application/cbor",
        );
        req.routing_id = Some(MessageId::Uint(1));
        writer.write(&req).await.unwrap();

        // Should get ERR back
        let err_frame = reader.read().await.unwrap().unwrap();
        assert_eq!(err_frame.frame_type, FrameType::Err);
        assert_eq!(err_frame.id, rid);
        assert_eq!(err_frame.error_code(), Some("NO_HANDLER"));

        drop(writer);
        drop(reader);
        host_task.await.unwrap().unwrap();
    }

    // TEST6751: InProcessCartridgeHost manifest includes identity cap and handler caps
    #[test]
    fn test6751_manifest_includes_all_caps() {
        let cap_urn = "cap:in=\"media:ext=pdf\";thumbnail;out=\"media:ext=png;image\"";
        let cap = make_test_cap(cap_urn);
        let host = InProcessCartridgeHost::new(
            InProcessHostIdentity::for_test("thumb-host"),
            vec![(
                "thumb".to_string(),
                vec![cap],
                Arc::new(EchoHandler) as Arc<dyn FrameHandler>,
            )],
        );

        let manifest = host.build_manifest();
        let payload: RelayNotifyCapabilitiesPayload = serde_json::from_slice(&manifest).unwrap();
        let caps = payload.cap_urns();
        assert_eq!(caps[0], CAP_IDENTITY);
        assert!(caps.iter().any(|u| u.contains("thumbnail")));
        // The InProcessCartridgeHost wraps the handlers in a single
        // installed-cartridge entry whose identity comes from the
        // `InProcessHostIdentity` the test passed at construction
        // (here, `for_test("thumb-host")`).
        assert_eq!(payload.installed_cartridges.len(), 1);
        assert_eq!(payload.installed_cartridges[0].id, "thumb-host");
        assert_eq!(payload.installed_cartridges[0].cap_groups.len(), 1);
        let stats = payload.installed_cartridges[0]
            .runtime_stats
            .as_ref()
            .unwrap();
        assert!(stats.running);
        // The pool map is the capacity surface: one at-rest unlimited
        // singleton per advertised cap plus the mandatory `all` pool.
        let all = stats
            .pools
            .get(crate::bifaci::pools::POOL_ALL)
            .expect("mandatory 'all' pool");
        assert_eq!(all.configured, 0, "in-process hosts are unlimited");
        assert_eq!(all.caps.len(), 2, "identity + thumbnail singletons");
        assert!(stats.pools.contains_key(CAP_IDENTITY));
        assert_eq!(stats.pools.len(), 3);
    }

    // TEST658: InProcessCartridgeHost handles heartbeat by echoing same ID
    #[tokio::test]
    async fn test658_heartbeat_response() {
        let host =
            InProcessCartridgeHost::new(InProcessHostIdentity::for_test("in-process-test"), vec![]);

        let (host_sock, test_sock) = UnixStream::pair().unwrap();
        let (host_read, host_write) = host_sock.into_split();
        let (test_read, test_write) = test_sock.into_split();

        let host_task = tokio::spawn(async move { host.run(host_read, host_write).await });

        let mut reader = FrameReader::new(BufReader::new(test_read));
        let mut writer = FrameWriter::new(BufWriter::new(test_write));

        // Skip RelayNotify
        let _notify = reader.read().await.unwrap().unwrap();

        let hb_id = MessageId::new_uuid();
        let hb = Frame::heartbeat(hb_id.clone());
        writer.write(&hb).await.unwrap();

        let resp = reader.read().await.unwrap().unwrap();
        assert_eq!(resp.frame_type, FrameType::Heartbeat);
        assert_eq!(resp.id, hb_id);
        // The heartbeat reply's mandatory pool map replaces the retired
        // scalar handler_capacity meta.
        let states = crate::bifaci::pools::decode_pool_states(
            resp.pool_state_bytes()
                .expect("heartbeat reply must carry the pool map"),
        )
        .expect("heartbeat pool map must decode");
        let all = states
            .get(crate::bifaci::pools::POOL_ALL)
            .expect("mandatory 'all' pool");
        assert_eq!(all.configured, 0, "in-process hosts are unlimited");
        assert_eq!(all.active, 0);

        drop(writer);
        drop(reader);
        host_task.await.unwrap().unwrap();
    }

    // TEST659: InProcessCartridgeHost handler error returns ERR frame
    #[tokio::test]
    async fn test659_handler_error_returns_err_frame() {
        /// Handler that always fails.
        #[derive(Debug)]
        struct FailHandler;

        #[async_trait]
        impl FrameHandler for FailHandler {
            async fn handle_request(
                &self,
                _cap_urn: &str,
                mut input: mpsc::UnboundedReceiver<Frame>,
                output: ResponseWriter,
                _peer: Arc<dyn PeerInvoker>,
            ) {
                // Drain input
                while let Some(frame) = input.recv().await {
                    if frame.frame_type == FrameType::End {
                        break;
                    }
                }
                output.emit_error("CARTRIDGE_ERROR", "cartridge crashed");
            }
        }

        let cap_urn = "cap:in=\"media:void\";fail;out=\"media:void\"";
        let cap = make_test_cap(cap_urn);
        let host = InProcessCartridgeHost::new(
            InProcessHostIdentity::for_test("fail-host"),
            vec![(
                "fail".to_string(),
                vec![cap],
                Arc::new(FailHandler) as Arc<dyn FrameHandler>,
            )],
        );

        let (host_sock, test_sock) = UnixStream::pair().unwrap();
        let (host_read, host_write) = host_sock.into_split();
        let (test_read, test_write) = test_sock.into_split();

        let host_task = tokio::spawn(async move { host.run(host_read, host_write).await });

        let mut reader = FrameReader::new(BufReader::new(test_read));
        let mut writer = FrameWriter::new(BufWriter::new(test_write));

        // Skip RelayNotify
        let _notify = reader.read().await.unwrap().unwrap();

        // Send REQ + END (no streams, void input)
        let rid = MessageId::new_uuid();
        let mut req = Frame::req(rid.clone(), cap_urn, vec![], "application/cbor");
        req.routing_id = Some(MessageId::Uint(1));
        writer.write(&req).await.unwrap();

        let end = Frame::end(rid.clone(), None);
        writer.write(&end).await.unwrap();

        // Should get ERR frame
        let err_frame = reader.read().await.unwrap().unwrap();
        assert_eq!(err_frame.frame_type, FrameType::Err);
        assert_eq!(err_frame.id, rid);
        assert_eq!(err_frame.error_code(), Some("CARTRIDGE_ERROR"));
        assert!(err_frame
            .error_message()
            .unwrap()
            .contains("cartridge crashed"));

        drop(writer);
        drop(reader);
        host_task.await.unwrap().unwrap();
    }

    // TEST660: InProcessCartridgeHost closest-specificity routing prefers specific over identity
    #[test]
    fn test660_closest_specificity_routing() {
        let specific_urn = "cap:in=\"media:ext=pdf\";thumbnail;out=\"media:ext=png;image\"";
        let generic_urn = "cap:in=\"media:image\";thumbnail;out=\"media:ext=png;image\"";

        let specific_cap = make_test_cap(specific_urn);
        let generic_cap = make_test_cap(generic_urn);

        /// Handler that tags its output with its name.
        #[derive(Debug)]
        struct TaggedHandler(String);

        #[async_trait]
        impl FrameHandler for TaggedHandler {
            async fn handle_request(
                &self,
                _cap_urn: &str,
                mut input: mpsc::UnboundedReceiver<Frame>,
                output: ResponseWriter,
                _peer: Arc<dyn PeerInvoker>,
            ) {
                // Drain input
                while let Some(frame) = input.recv().await {
                    if frame.frame_type == FrameType::End {
                        break;
                    }
                }
                output.emit_response("media:text", self.0.as_bytes());
            }
        }

        let handlers = vec![
            (
                "generic".to_string(),
                vec![generic_cap],
                Arc::new(TaggedHandler("generic".into())) as Arc<dyn FrameHandler>,
            ),
            (
                "specific".to_string(),
                vec![specific_cap],
                Arc::new(TaggedHandler("specific".into())) as Arc<dyn FrameHandler>,
            ),
        ];

        let host = InProcessCartridgeHost::new(
            InProcessHostIdentity::for_test("in-process-test"),
            handlers,
        );
        let cap_table = InProcessCartridgeHost::build_cap_table(&host.handlers);

        // Request for pdf thumbnail should match specific (pdf, specificity 3) over generic (image, specificity 2)
        let result = InProcessCartridgeHost::find_handler_for_cap(
            &cap_table,
            "cap:in=\"media:ext=pdf\";thumbnail;out=\"media:ext=png;image\"",
        );
        let (idx, registered) = result.expect("specific handler must match");
        assert_eq!(idx, 1); // specific handler
        assert!(
            registered.contains("ext=pdf"),
            "the winning registered pattern is the specific one: {registered}"
        );
    }
}
