//! CborRelay — Transparent CBOR frame relay with two relay-specific frame types.
//!
//! The relay is a byte-stream bridge between an engine (master) and a cartridge host runtime
//! (slave). Two relay-specific frame types are intercepted and never leaked through:
//!
//! - **RelayNotify** (slave → master): Capability advertisement from the slave's cartridge host runtime.
//! - **RelayState** (master → slave): Host system resources + cap demands from the engine.
//!
//! All other frames pass through transparently in both directions.

use crate::bifaci::frame::{FlowKey, Frame, FrameType, Limits, ReorderBuffer};
use crate::bifaci::io::{CborError, FrameReader, FrameWriter};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncWrite};

// =============================================================================
// RELAY TYPES
// =============================================================================

/// Slave endpoint of the relay. Sits inside the cartridge host process (e.g., XPC service).
///
/// - Reads frames from the socket (from master): RelayState → store; others → forward to local side
/// - Reads frames from local side (from CartridgeHostRuntime): forward to socket
/// - Can inject RelayNotify frames into the socket stream on demand
pub struct RelaySlave<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> {
    /// Read from CartridgeHostRuntime
    local_reader: FrameReader<R>,
    /// Write to CartridgeHostRuntime
    local_writer: FrameWriter<W>,
    /// Latest RelayState payload from master
    resource_state: Arc<Mutex<Vec<u8>>>,
    /// Invoked on every RelayState received from the master, with the raw
    /// payload — the master→slave CONTROL channel (e.g. the engine
    /// delivering desired pool capacities; see `bifaci::pools`). The stored
    /// `resource_state` snapshot is kept regardless.
    on_relay_state: Option<Arc<dyn Fn(Vec<u8>) + Send + Sync>>,
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> RelaySlave<R, W> {
    /// Create a new relay slave with local I/O streams (to/from CartridgeHostRuntime).
    pub fn new(local_read: R, local_write: W) -> Self {
        Self {
            local_reader: FrameReader::new(local_read),
            local_writer: FrameWriter::new(local_write),
            resource_state: Arc::new(Mutex::new(Vec::new())),
            on_relay_state: None,
        }
    }

    /// Register a handler invoked with every RelayState payload received
    /// from the master (the master→slave control channel). Call before
    /// `run`.
    pub fn set_relay_state_handler(
        &mut self,
        handler: Arc<dyn Fn(Vec<u8>) + Send + Sync>,
    ) {
        self.on_relay_state = Some(handler);
    }

    /// Get the latest resource state payload received from the master.
    pub fn resource_state(&self) -> Vec<u8> {
        self.resource_state.lock().unwrap().clone()
    }

    /// Get a cloneable handle to the resource state for sharing with the host runtime.
    pub fn resource_state_handle(&self) -> Arc<Mutex<Vec<u8>>> {
        Arc::clone(&self.resource_state)
    }

    /// Run the relay bidirectionally using two tasks.
    ///
    /// - Task 1 (socket → local): Reads from socket, intercepts RelayState, forwards others to local
    /// - Task 2 (local → socket): Reads from local, drops only RelayState; forwards RelayNotify (host capability updates) and all other frames to socket
    ///
    /// Runs until one side closes or an error occurs.
    /// Consumes self to split local reader/writer across tasks.
    pub async fn run<
        SR: AsyncRead + Unpin + Send + 'static,
        SW: AsyncWrite + Unpin + Send + 'static,
    >(
        self,
        mut socket_read: FrameReader<SR>,
        mut socket_write: FrameWriter<SW>,
        initial_notify: Option<(&[u8], &Limits)>,
    ) -> Result<(), CborError>
    where
        R: Send + 'static,
        W: Send + 'static,
    {
        // Send initial RelayNotify if provided
        let max_reorder = if let Some((manifest, limits)) = initial_notify {
            let notify = Frame::relay_notify(manifest, limits);
            socket_write.write(&notify).await?;
            limits.max_reorder_buffer
        } else {
            crate::bifaci::frame::DEFAULT_MAX_REORDER_BUFFER
        };

        let resource_state = self.resource_state;
        let mut local_writer = self.local_writer;
        let mut local_reader = self.local_reader;

        let first_error: Arc<tokio::sync::Mutex<Option<CborError>>> =
            Arc::new(tokio::sync::Mutex::new(None));

        // Task 1: socket → local (master sends frames to slave's CartridgeHost)
        let err1 = Arc::clone(&first_error);
        let rs1 = Arc::clone(&resource_state);
        let on_relay_state = self.on_relay_state.clone();
        let t1 = tokio::spawn(async move {
            let mut reorder = ReorderBuffer::new(max_reorder);
            loop {
                match socket_read.read().await {
                    Ok(Some(frame)) => {
                        if frame.frame_type == FrameType::RelayState {
                            if let Some(payload) = frame.payload {
                                *rs1.lock().unwrap() = payload.clone();
                                if let Some(handler) = &on_relay_state {
                                    handler(payload);
                                }
                            }
                        } else if frame.frame_type == FrameType::RelayNotify {
                            // RelayNotify from master — ignore
                        } else {
                            let ready_frames = match reorder.accept(frame) {
                                Ok(frames) => frames,
                                Err(e) => {
                                    tracing::error!("[RelaySlave] t1 reorder error: {}", e);
                                    let mut guard = err1.lock().await;
                                    if guard.is_none() {
                                        *guard = Some(e);
                                    }
                                    return;
                                }
                            };
                            for f in &ready_frames {
                                if matches!(f.frame_type, FrameType::End | FrameType::Err) {
                                    reorder.cleanup_flow(&FlowKey::from_frame(f));
                                }
                            }
                            for f in ready_frames {
                                if let Err(e) = local_writer.write(&f).await {
                                    tracing::error!("[RelaySlave] t1 local_writer error: {}", e);
                                    let mut guard = err1.lock().await;
                                    if guard.is_none() {
                                        *guard = Some(e);
                                    }
                                    return;
                                }
                            }
                        }
                    }
                    Ok(None) => return,
                    Err(e) => {
                        tracing::error!("[RelaySlave] t1 socket read error: {}", e);
                        let mut guard = err1.lock().await;
                        if guard.is_none() {
                            *guard = Some(e);
                        }
                        return;
                    }
                }
            }
        });

        // Task 2: local → socket (CartridgeHost sends frames to master)
        let err2 = Arc::clone(&first_error);
        let t2 = tokio::spawn(async move {
            let mut reorder = ReorderBuffer::new(max_reorder);
            loop {
                match local_reader.read().await {
                    Ok(Some(frame)) => {
                        // Forward all frames, including RelayNotify (capability updates)
                        // RelayState is still dropped (deprecated/unused)
                        if frame.frame_type == FrameType::RelayState {
                            // Drop RelayState frames
                        } else {
                            let ready_frames = match reorder.accept(frame) {
                                Ok(frames) => frames,
                                Err(e) => {
                                    tracing::error!("[RelaySlave] t2 reorder error: {}", e);
                                    let mut guard = err2.lock().await;
                                    if guard.is_none() {
                                        *guard = Some(e);
                                    }
                                    return;
                                }
                            };
                            for f in &ready_frames {
                                if matches!(f.frame_type, FrameType::End | FrameType::Err) {
                                    reorder.cleanup_flow(&FlowKey::from_frame(f));
                                }
                            }
                            for f in ready_frames {
                                if let Err(e) = socket_write.write(&f).await {
                                    tracing::error!("[RelaySlave] t2 socket_write error: {}", e);
                                    let mut guard = err2.lock().await;
                                    if guard.is_none() {
                                        *guard = Some(e);
                                    }
                                    return;
                                }
                            }
                        }
                    }
                    Ok(None) => return,
                    Err(e) => {
                        tracing::error!("[RelaySlave] t2 local read error: {}", e);
                        let mut guard = err2.lock().await;
                        if guard.is_none() {
                            *guard = Some(e);
                        }
                        return;
                    }
                }
            }
        });

        let _ = t1.await;
        let _ = t2.await;

        let err = first_error.lock().await.take();
        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Send a RelayNotify frame directly to the socket writer.
    /// Used when capabilities change (cartridge discovered, cartridge died).
    pub async fn send_notify<SW: AsyncWrite + Unpin>(
        socket_write: &mut FrameWriter<SW>,
        manifest: &[u8],
        limits: &Limits,
    ) -> Result<(), CborError> {
        let notify = Frame::relay_notify(manifest, limits);
        socket_write.write(&notify).await
    }
}

/// Master endpoint of the relay. Sits in the engine process.
///
/// - Reads frames from the socket (from slave): RelayNotify → callback + store; others → forward to caller
/// - Writes frames to the socket (from caller): pass through
/// - Can send RelayState frames to the slave
pub struct RelayMaster {
    /// Latest manifest from slave's RelayNotify
    manifest: Vec<u8>,
    /// Latest limits from slave's RelayNotify
    limits: Limits,
    /// Reorder buffer for frames read from the socket
    reorder: ReorderBuffer,
    /// Ready queue of frames that passed through the reorder buffer
    ready_queue: std::collections::VecDeque<Frame>,
}

impl RelayMaster {
    /// Connect to a relay slave by reading the initial RelayNotify frame.
    ///
    /// The slave MUST send a RelayNotify as its first frame after connection.
    /// This extracts the manifest and limits from that frame.
    pub async fn connect<SR: AsyncRead + Unpin>(
        socket_read: &mut FrameReader<SR>,
    ) -> Result<Self, CborError> {
        let frame = socket_read.read().await?.ok_or_else(|| {
            CborError::Handshake("relay connection closed before receiving RelayNotify".to_string())
        })?;

        if frame.frame_type != FrameType::RelayNotify {
            return Err(CborError::Protocol(format!(
                "expected RelayNotify, got {:?}",
                frame.frame_type
            )));
        }

        let manifest = frame
            .relay_notify_manifest()
            .ok_or_else(|| CborError::Protocol("RelayNotify missing manifest".to_string()))?
            .to_vec();

        let limits = frame
            .relay_notify_limits()
            .ok_or_else(|| CborError::Protocol("RelayNotify missing limits".to_string()))?;

        let reorder = ReorderBuffer::new(limits.max_reorder_buffer);
        Ok(Self {
            manifest,
            limits,
            reorder,
            ready_queue: std::collections::VecDeque::new(),
        })
    }

    /// Get the aggregate manifest from the slave.
    pub fn manifest(&self) -> &[u8] {
        &self.manifest
    }

    /// Get the negotiated limits from the slave.
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// Send a RelayState frame to the slave with host system resource info.
    pub async fn send_state<SW: AsyncWrite + Unpin>(
        socket_write: &mut FrameWriter<SW>,
        resources: &[u8],
    ) -> Result<(), CborError> {
        let frame = Frame::relay_state(resources);
        socket_write.write(&frame).await
    }

    /// Read the next non-relay frame from the socket.
    ///
    /// RelayNotify frames are intercepted: manifest and limits are updated.
    /// All other frames pass through the reorder buffer before delivery.
    /// Returns Ok(None) on EOF.
    pub async fn read_frame<SR: AsyncRead + Unpin>(
        &mut self,
        socket_read: &mut FrameReader<SR>,
    ) -> Result<Option<Frame>, CborError> {
        loop {
            // Drain ready queue first
            if let Some(frame) = self.ready_queue.pop_front() {
                return Ok(Some(frame));
            }

            match socket_read.read().await? {
                Some(frame) => {
                    if frame.frame_type == FrameType::RelayNotify {
                        if let Some(manifest) = frame.relay_notify_manifest() {
                            self.manifest = manifest.to_vec();
                        }
                        if let Some(limits) = frame.relay_notify_limits() {
                            self.limits = limits;
                        }
                        continue;
                    } else if frame.frame_type == FrameType::RelayState {
                        continue;
                    }
                    let ready = self.reorder.accept(frame)?;
                    for f in &ready {
                        if matches!(f.frame_type, FrameType::End | FrameType::Err) {
                            self.reorder.cleanup_flow(&FlowKey::from_frame(f));
                        }
                    }
                    self.ready_queue.extend(ready);
                }
                None => return Ok(None),
            }
        }
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bifaci::frame::{FlowKey, Frame, FrameType, Limits, MessageId, SeqAssigner};
    use crate::bifaci::io::{FrameReader, FrameWriter};
    use crate::bifaci::local_socket::UnixStream;
    use tokio::io::{BufReader, BufWriter};

    /// Create an async pipe pair using Unix sockets.
    /// Returns (read_end, write_end) for each direction.
    fn create_pipe_pair() -> (UnixStream, UnixStream) {
        UnixStream::pair().expect("Failed to create pipe pair")
    }

    // TEST404: Slave sends RelayNotify on connect (initial_notify parameter)
    #[tokio::test]
    async fn test404_slave_sends_relay_notify_on_connect() {
        let manifest = b"{\"caps\":[\"cap:in=\"media:void\";test;out=\"media:void\"\"]}";
        let limits = Limits::default();

        // Socket: slave writes → master reads
        let (master_read_stream, slave_write_stream) = create_pipe_pair();

        // Slave sends initial notify through socket_write
        let slave_handle = tokio::spawn(async move {
            let mut socket_writer = FrameWriter::new(BufWriter::new(slave_write_stream));
            RelaySlave::<tokio::io::Empty, Vec<u8>>::send_notify(
                &mut socket_writer,
                manifest,
                &limits,
            )
            .await
            .unwrap();
        });

        // Master reads it
        let mut socket_reader = FrameReader::new(BufReader::new(master_read_stream));
        let frame = socket_reader
            .read()
            .await
            .unwrap()
            .expect("should have frame");

        assert_eq!(frame.frame_type, FrameType::RelayNotify);
        assert_eq!(frame.relay_notify_manifest(), Some(manifest.as_slice()));
        let extracted = frame.relay_notify_limits().unwrap();
        assert_eq!(extracted.max_frame, limits.max_frame);
        assert_eq!(extracted.max_chunk, limits.max_chunk);

        slave_handle.await.unwrap();
    }

    // TEST405: Master reads RelayNotify and extracts manifest + limits
    #[tokio::test]
    async fn test405_master_reads_relay_notify() {
        let manifest = b"{\"caps\":[\"cap:in=\"media:void\";convert;out=\"media:void\"\"]}";
        let limits = Limits {
            max_frame: 1_000_000,
            max_chunk: 64_000,
            ..Limits::default()
        };

        let (master_read_stream, slave_write_stream) = create_pipe_pair();

        // Slave sends RelayNotify
        let slave_handle = tokio::spawn(async move {
            let mut writer = FrameWriter::new(BufWriter::new(slave_write_stream));
            let notify = Frame::relay_notify(manifest, &limits);
            writer.write(&notify).await.unwrap();
        });

        // Master connects
        let mut reader = FrameReader::new(BufReader::new(master_read_stream));
        let master = RelayMaster::connect(&mut reader).await.unwrap();

        assert_eq!(master.manifest(), manifest);
        assert_eq!(master.limits().max_frame, 1_000_000);
        assert_eq!(master.limits().max_chunk, 64_000);

        slave_handle.await.unwrap();
    }

    // TEST406: Slave stores RelayState from master
    #[tokio::test]
    async fn test406_slave_stores_relay_state() {
        let resources = b"{\"memory_mb\":4096}";

        // Socket: master writes → slave reads
        let (slave_socket_read, master_socket_write) = create_pipe_pair();
        // Local: slave writes → (nobody reads in this test)
        let (local_read_end, local_write_end) = create_pipe_pair();

        let resource_state = Arc::new(Mutex::new(Vec::new()));
        let resource_state_clone = Arc::clone(&resource_state);

        // Master sends RelayState
        let master_handle = tokio::spawn(async move {
            let mut writer = FrameWriter::new(BufWriter::new(master_socket_write));
            RelayMaster::send_state(&mut writer, resources)
                .await
                .unwrap();
            drop(writer); // Close to signal EOF
        });

        // Slave reads from socket
        let slave_handle = tokio::spawn(async move {
            let mut slave = RelaySlave::new(local_read_end, local_write_end);
            // Override resource_state with our shared handle
            slave.resource_state = resource_state_clone;

            let mut socket_reader = FrameReader::new(BufReader::new(slave_socket_read));

            // Read one frame — should be RelayState
            let frame = socket_reader
                .read()
                .await
                .unwrap()
                .expect("should have frame");
            assert_eq!(frame.frame_type, FrameType::RelayState);
            if let Some(payload) = frame.payload {
                *slave.resource_state.lock().unwrap() = payload;
            }

            slave.resource_state()
        });

        master_handle.await.unwrap();
        let stored = slave_handle.await.unwrap();
        assert_eq!(stored, resources);
    }

    // TEST407: Protocol frames pass through slave transparently (both directions)
    #[tokio::test]
    async fn test407_protocol_frames_pass_through() {
        // Socket pair: master ↔ slave
        let (slave_socket_read, master_socket_write) = create_pipe_pair();
        let (master_socket_read, slave_socket_write) = create_pipe_pair();
        // Local pair: slave ↔ host runtime
        let (runtime_read_from_slave, slave_local_write) = create_pipe_pair();
        let (slave_local_read, runtime_write_to_slave) = create_pipe_pair();

        let req_id = MessageId::new_uuid();
        let req_id_clone = req_id.clone();

        // Master sends a REQ frame through the socket
        let master_write_handle = tokio::spawn(async move {
            let mut writer = FrameWriter::new(BufWriter::new(master_socket_write));
            let mut seq = SeqAssigner::new();
            let mut req = Frame::req(
                req_id_clone,
                "cap:in=\"media:void\";test;out=\"media:void\"",
                b"hello".to_vec(),
                "text/plain",
            );
            seq.assign(&mut req);
            writer.write(&req).await.unwrap();
            drop(writer);
        });

        // Runtime sends a CHUNK frame through the local write
        let chunk_id = MessageId::new_uuid();
        let chunk_id_clone = chunk_id.clone();
        let runtime_write_handle = tokio::spawn(async move {
            let mut writer = FrameWriter::new(BufWriter::new(runtime_write_to_slave));
            let mut seq = SeqAssigner::new();
            let payload = b"response".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(
                chunk_id_clone,
                "stream-1".to_string(),
                0,
                payload,
                0,
                checksum,
            );
            seq.assign(&mut chunk);
            writer.write(&chunk).await.unwrap();
            drop(writer);
        });

        // Slave relay: read from socket, write to local; read from local, write to socket
        // We do this manually (one frame each direction) to avoid the blocking run() loop
        let slave_handle = tokio::spawn(async move {
            let mut socket_reader = FrameReader::new(BufReader::new(slave_socket_read));
            let mut socket_writer = FrameWriter::new(BufWriter::new(slave_socket_write));
            let mut local_reader = FrameReader::new(BufReader::new(slave_local_read));
            let mut local_writer = FrameWriter::new(BufWriter::new(slave_local_write));

            // Socket → local: read REQ, forward to local
            let from_socket = socket_reader
                .read()
                .await
                .unwrap()
                .expect("should have frame");
            assert_eq!(from_socket.frame_type, FrameType::Req);
            local_writer.write(&from_socket).await.unwrap();

            // Local → socket: read CHUNK, forward to socket
            let from_local = local_reader
                .read()
                .await
                .unwrap()
                .expect("should have frame");
            assert_eq!(from_local.frame_type, FrameType::Chunk);
            socket_writer.write(&from_local).await.unwrap();
        });

        // Runtime reads the forwarded REQ
        let runtime_read_handle = tokio::spawn(async move {
            let mut reader = FrameReader::new(BufReader::new(runtime_read_from_slave));
            let frame = reader.read().await.unwrap().expect("should have frame");
            assert_eq!(frame.frame_type, FrameType::Req);
            assert_eq!(
                frame.cap.as_deref(),
                Some("cap:in=\"media:void\";test;out=\"media:void\"")
            );
            assert_eq!(frame.payload, Some(b"hello".to_vec()));
        });

        // Master reads the forwarded CHUNK
        let master_read_handle = tokio::spawn(async move {
            let mut reader = FrameReader::new(BufReader::new(master_socket_read));
            let frame = reader.read().await.unwrap().expect("should have frame");
            assert_eq!(frame.frame_type, FrameType::Chunk);
            assert_eq!(frame.payload, Some(b"response".to_vec()));
        });

        master_write_handle.await.unwrap();
        runtime_write_handle.await.unwrap();
        slave_handle.await.unwrap();
        runtime_read_handle.await.unwrap();
        master_read_handle.await.unwrap();
    }

    // TEST408: RelayNotify/RelayState are NOT forwarded through relay
    #[tokio::test]
    async fn test408_relay_frames_not_forwarded() {
        // Master sends RelayState — slave should NOT forward it to local
        let (slave_socket_read, master_socket_write) = create_pipe_pair();
        let (_runtime_read, slave_local_write) = create_pipe_pair();

        let master_handle = tokio::spawn(async move {
            let mut writer = FrameWriter::new(BufWriter::new(master_socket_write));
            let mut seq = SeqAssigner::new();
            // Send RelayState
            let state = Frame::relay_state(b"{\"memory\":1024}");
            writer.write(&state).await.unwrap();
            // Then send a normal REQ to verify the slave still works
            let mut req = Frame::req(
                MessageId::new_uuid(),
                "cap:in=\"media:void\";test;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            seq.assign(&mut req);
            writer.write(&req).await.unwrap();
            drop(writer);
        });

        let slave_handle = tokio::spawn(async move {
            let mut socket_reader = FrameReader::new(BufReader::new(slave_socket_read));
            let mut local_writer = FrameWriter::new(BufWriter::new(slave_local_write));
            let resource_state: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));

            // Read first frame — RelayState, should NOT be forwarded
            let frame1 = socket_reader
                .read()
                .await
                .unwrap()
                .expect("should have frame");
            assert_eq!(frame1.frame_type, FrameType::RelayState);
            if let Some(payload) = frame1.payload {
                *resource_state.lock().unwrap() = payload;
            }
            // Do NOT forward to local_writer

            // Read second frame — REQ, should be forwarded
            let frame2 = socket_reader
                .read()
                .await
                .unwrap()
                .expect("should have frame");
            assert_eq!(frame2.frame_type, FrameType::Req);
            local_writer.write(&frame2).await.unwrap();

            // Verify resource state was stored
            let state = resource_state.lock().unwrap().clone();
            assert_eq!(state, b"{\"memory\":1024}");
        });

        master_handle.await.unwrap();
        slave_handle.await.unwrap();
    }

    // TEST409: Slave can inject RelayNotify mid-stream (cap change)
    #[tokio::test]
    async fn test409_slave_injects_relay_notify_midstream() {
        let (master_socket_read, slave_socket_write) = create_pipe_pair();

        let slave_handle = tokio::spawn(async move {
            let mut socket_writer = FrameWriter::new(BufWriter::new(slave_socket_write));
            let mut seq = SeqAssigner::new();
            let limits = Limits::default();

            // First: send initial RelayNotify
            let initial = b"{\"caps\":[\"cap:in=\"media:void\";test;out=\"media:void\"\"]}";
            RelaySlave::<tokio::io::Empty, Vec<u8>>::send_notify(
                &mut socket_writer,
                initial,
                &limits,
            )
            .await
            .unwrap();

            // Then: forward a normal CHUNK frame
            let payload = b"data".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(
                MessageId::new_uuid(),
                "stream-1".to_string(),
                0,
                payload,
                0,
                checksum,
            );
            seq.assign(&mut chunk);
            socket_writer.write(&chunk).await.unwrap();

            // Then: inject updated RelayNotify (new cap discovered)
            let updated = b"{\"caps\":[\"cap:in=\"media:void\";test;out=\"media:void\"\",\"cap:in=\"media:void\";convert;out=\"media:void\"\"]}";
            RelaySlave::<tokio::io::Empty, Vec<u8>>::send_notify(
                &mut socket_writer,
                updated,
                &limits,
            )
            .await
            .unwrap();

            drop(socket_writer);
        });

        let master_handle = tokio::spawn(async move {
            let mut reader = FrameReader::new(BufReader::new(master_socket_read));

            // Read initial RelayNotify
            let f1 = reader.read().await.unwrap().expect("frame 1");
            assert_eq!(f1.frame_type, FrameType::RelayNotify);
            assert_eq!(
                f1.relay_notify_manifest(),
                Some(b"{\"caps\":[\"cap:in=\"media:void\";test;out=\"media:void\"\"]}".as_slice())
            );

            // Read CHUNK (passed through)
            let f2 = reader.read().await.unwrap().expect("frame 2");
            assert_eq!(f2.frame_type, FrameType::Chunk);

            // Read updated RelayNotify
            let f3 = reader.read().await.unwrap().expect("frame 3");
            assert_eq!(f3.frame_type, FrameType::RelayNotify);
            assert_eq!(
                f3.relay_notify_manifest(),
                Some(b"{\"caps\":[\"cap:in=\"media:void\";test;out=\"media:void\"\",\"cap:in=\"media:void\";convert;out=\"media:void\"\"]}".as_slice())
            );
        });

        slave_handle.await.unwrap();
        master_handle.await.unwrap();
    }

    // TEST410: Master receives updated RelayNotify (cap change callback via read_frame)
    #[tokio::test]
    async fn test410_master_receives_updated_relay_notify() {
        let (master_socket_read, slave_socket_write) = create_pipe_pair();

        let limits = Limits {
            max_frame: 2_000_000,
            max_chunk: 100_000,
            ..Limits::default()
        };

        let slave_handle = tokio::spawn(async move {
            let mut writer = FrameWriter::new(BufWriter::new(slave_socket_write));
            let mut seq = SeqAssigner::new();

            // Initial RelayNotify
            let initial = Frame::relay_notify(
                b"{\"caps\":[\"cap:in=\"media:void\";a;out=\"media:void\"\"]}",
                &limits,
            );
            writer.write(&initial).await.unwrap();

            // Normal frame
            let end_id = MessageId::new_uuid();
            let mut end = Frame::end(end_id.clone(), None);
            seq.assign(&mut end);
            writer.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: end_id,
                xid: None,
            });

            // Updated RelayNotify
            let updated_limits = Limits {
                max_frame: 3_000_000,
                max_chunk: 200_000,
                ..Limits::default()
            };
            let updated = Frame::relay_notify(b"{\"caps\":[\"cap:in=\"media:void\";a;out=\"media:void\"\",\"cap:in=\"media:void\";b;out=\"media:void\"\"]}", &updated_limits);
            writer.write(&updated).await.unwrap();

            // Another normal frame to prove master continues
            let end2_id = MessageId::new_uuid();
            let mut end2 = Frame::end(end2_id.clone(), None);
            seq.assign(&mut end2);
            writer.write(&end2).await.unwrap();
            seq.remove(&FlowKey {
                rid: end2_id,
                xid: None,
            });

            drop(writer);
        });

        let master_handle = tokio::spawn(async move {
            let mut reader = FrameReader::new(BufReader::new(master_socket_read));
            let mut master = RelayMaster::connect(&mut reader).await.unwrap();

            // Initial state
            assert_eq!(
                master.manifest(),
                b"{\"caps\":[\"cap:in=\"media:void\";a;out=\"media:void\"\"]}"
            );
            assert_eq!(master.limits().max_frame, 2_000_000);

            // First non-relay frame
            let f1 = master
                .read_frame(&mut reader)
                .await
                .unwrap()
                .expect("frame 1");
            assert_eq!(f1.frame_type, FrameType::End);

            // read_frame should have intercepted the updated RelayNotify
            let f2 = master
                .read_frame(&mut reader)
                .await
                .unwrap()
                .expect("frame 2");
            assert_eq!(f2.frame_type, FrameType::End);

            // Manifest and limits should be updated
            assert_eq!(master.manifest(), b"{\"caps\":[\"cap:in=\"media:void\";a;out=\"media:void\"\",\"cap:in=\"media:void\";b;out=\"media:void\"\"]}");
            assert_eq!(master.limits().max_frame, 3_000_000);
            assert_eq!(master.limits().max_chunk, 200_000);
        });

        slave_handle.await.unwrap();
        master_handle.await.unwrap();
    }

    // TEST411: Socket close detection (both directions)
    #[tokio::test]
    async fn test411_socket_close_detection() {
        // Master → slave direction: master closes, slave detects
        let (slave_socket_read, master_socket_write) = create_pipe_pair();

        let master_handle = tokio::spawn(async move {
            drop(master_socket_write); // Close immediately
        });

        let slave_handle = tokio::spawn(async move {
            let mut reader = FrameReader::new(BufReader::new(slave_socket_read));
            let result = reader.read().await.unwrap();
            assert!(result.is_none(), "closed socket must return None");
        });

        master_handle.await.unwrap();
        slave_handle.await.unwrap();

        // Slave → master direction: slave closes, master detects
        let (master_socket_read, slave_socket_write) = create_pipe_pair();

        let slave_handle2 = tokio::spawn(async move {
            let mut writer = FrameWriter::new(BufWriter::new(slave_socket_write));
            // Send RelayNotify then close
            let notify = Frame::relay_notify(b"[]", &Limits::default());
            writer.write(&notify).await.unwrap();
            drop(writer);
        });

        let master_handle2 = tokio::spawn(async move {
            let mut reader = FrameReader::new(BufReader::new(master_socket_read));
            let mut master = RelayMaster::connect(&mut reader).await.unwrap();
            let result = master.read_frame(&mut reader).await.unwrap();
            assert!(result.is_none(), "closed socket must return None");
        });

        slave_handle2.await.unwrap();
        master_handle2.await.unwrap();
    }

    // TEST412: Bidirectional concurrent frame flow through relay
    #[tokio::test]
    async fn test412_bidirectional_concurrent_flow() {
        // Full relay setup: master ↔ socket ↔ slave ↔ local ↔ runtime
        let (slave_socket_read, master_socket_write) = create_pipe_pair();
        let (master_socket_read, slave_socket_write) = create_pipe_pair();
        let (runtime_reads_from_slave, slave_local_write) = create_pipe_pair();
        let (slave_local_read, runtime_writes_to_slave) = create_pipe_pair();

        let req_id1 = MessageId::new_uuid();
        let req_id2 = MessageId::new_uuid();
        let req_id1_clone = req_id1.clone();
        let req_id2_clone = req_id2.clone();
        let req_id1_verify = req_id1.clone();
        let req_id2_verify = req_id2.clone();

        // Master writes REQ frames
        let master_write = tokio::spawn(async move {
            let mut writer = FrameWriter::new(BufWriter::new(master_socket_write));
            let mut seq = SeqAssigner::new();
            let mut req1 = Frame::req(
                req_id1_clone,
                "cap:in=\"media:void\";a;out=\"media:void\"",
                b"data-a".to_vec(),
                "text/plain",
            );
            let mut req2 = Frame::req(
                req_id2_clone,
                "cap:in=\"media:void\";b;out=\"media:void\"",
                b"data-b".to_vec(),
                "text/plain",
            );
            seq.assign(&mut req1);
            writer.write(&req1).await.unwrap();
            seq.assign(&mut req2);
            writer.write(&req2).await.unwrap();
            drop(writer);
        });

        // Runtime writes response chunks
        let resp_id1 = MessageId::new_uuid();
        let resp_id1_clone = resp_id1.clone();
        let runtime_write = tokio::spawn(async move {
            let mut writer = FrameWriter::new(BufWriter::new(runtime_writes_to_slave));
            let mut seq = SeqAssigner::new();
            let payload = b"resp-a".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(resp_id1_clone, "s1".to_string(), 0, payload, 0, checksum);
            seq.assign(&mut chunk);
            writer.write(&chunk).await.unwrap();
            let mut end = Frame::end(resp_id1.clone(), None);
            seq.assign(&mut end);
            writer.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: resp_id1,
                xid: None,
            });
            drop(writer);
        });

        // Slave relay: manually forward frames both directions
        let slave = tokio::spawn(async move {
            let mut sock_r = FrameReader::new(BufReader::new(slave_socket_read));
            let mut sock_w = FrameWriter::new(BufWriter::new(slave_socket_write));
            let mut local_r = FrameReader::new(BufReader::new(slave_local_read));
            let mut local_w = FrameWriter::new(BufWriter::new(slave_local_write));

            // Forward 2 frames from socket to local
            for _ in 0..2 {
                let f = sock_r.read().await.unwrap().expect("socket frame");
                local_w.write(&f).await.unwrap();
            }
            // Forward 2 frames from local to socket
            for _ in 0..2 {
                let f = local_r.read().await.unwrap().expect("local frame");
                sock_w.write(&f).await.unwrap();
            }
        });

        // Runtime reads forwarded REQs
        let runtime_read = tokio::spawn(async move {
            let mut reader = FrameReader::new(BufReader::new(runtime_reads_from_slave));
            let f1 = reader.read().await.unwrap().expect("frame 1");
            let f2 = reader.read().await.unwrap().expect("frame 2");
            assert_eq!(f1.frame_type, FrameType::Req);
            assert_eq!(f2.frame_type, FrameType::Req);
            assert_eq!(f1.id, req_id1_verify);
            assert_eq!(f2.id, req_id2_verify);
        });

        // Master reads forwarded responses
        let master_read = tokio::spawn(async move {
            let mut reader = FrameReader::new(BufReader::new(master_socket_read));
            let f1 = reader.read().await.unwrap().expect("frame 1");
            assert_eq!(f1.frame_type, FrameType::Chunk);
            assert_eq!(f1.payload, Some(b"resp-a".to_vec()));
            let f2 = reader.read().await.unwrap().expect("frame 2");
            assert_eq!(f2.frame_type, FrameType::End);
        });

        master_write.await.unwrap();
        runtime_write.await.unwrap();
        slave.await.unwrap();
        runtime_read.await.unwrap();
        master_read.await.unwrap();
    }

    // TEST414: RelaySlave forwards a host-originated RelayNotify (local→socket),
    // dropping only RelayState. The CartridgeHostRuntime publishes capability
    // updates — the installed-cartridge inventory the engine routes by — as
    // RelayNotify frames through the slave's local→socket path; the slave MUST
    // forward them. Regression lock for the drift (reproduced in the go/swift
    // mirrors) where Task 2 dropped RelayNotify alongside RelayState, stranding
    // the host's inventory so the engine never learned the cartridge existed.
    #[tokio::test]
    async fn test414_relay_slave_forwards_host_relay_notify() {
        use tokio::io::split;

        // Socket side: slave ↔ master(router).
        let (slave_socket, router) = create_pipe_pair();
        let (slave_sock_r, slave_sock_w) = split(slave_socket);
        let (router_r, _router_w) = split(router);

        // Local side: host(CartridgeHostRuntime) ↔ slave.
        let (slave_local, host) = create_pipe_pair();
        let (slave_local_r, slave_local_w) = split(slave_local);
        let (_host_r, host_w) = split(host);

        let slave = RelaySlave::new(slave_local_r, slave_local_w);

        let slave_handle = tokio::spawn(async move {
            let socket_read = FrameReader::new(BufReader::new(slave_sock_r));
            let socket_write = FrameWriter::new(BufWriter::new(slave_sock_w));
            let initial = b"{\"installed_cartridges\":[]}".to_vec();
            let limits = Limits::default();
            let _ = slave
                .run(socket_read, socket_write, Some((&initial, &limits)))
                .await;
        });

        let mut router_reader = FrameReader::new(BufReader::new(router_r));

        // Frame 1: the initial RelayNotify the slave emits on start.
        let f1 = router_reader
            .read()
            .await
            .unwrap()
            .expect("initial RelayNotify");
        assert_eq!(f1.frame_type, FrameType::RelayNotify);

        // Host writes a RelayState (must be DROPPED), then a populated
        // RelayNotify (must be FORWARDED).
        let limits = Limits::default();
        let mut host_writer = FrameWriter::new(BufWriter::new(host_w));
        host_writer
            .write(&Frame::relay_state(b"{\"memory\":1}"))
            .await
            .unwrap();
        let manifest = b"{\"installed_cartridges\":[{\"id\":\"CartA\"}]}";
        host_writer
            .write(&Frame::relay_notify(manifest, &limits))
            .await
            .unwrap();

        // The next frame the router sees MUST be the RelayNotify — proving the
        // RelayState was dropped and the RelayNotify forwarded (not the reverse).
        let f2 =
            router_reader.read().await.unwrap().expect(
                "forwarded RelayNotify (RelayState must be dropped, RelayNotify forwarded)",
            );
        assert_eq!(f2.frame_type, FrameType::RelayNotify);
        assert_eq!(f2.relay_notify_manifest(), Some(manifest.as_slice()));

        slave_handle.abort();
    }
}
