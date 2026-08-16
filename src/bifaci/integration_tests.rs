//! Integration tests for CBOR cartridge communication protocol.
//!
//! These tests verify the full path: engine → relay → runtime → cartridge → response back.
//! The CartridgeHostRuntime manages multiple cartridges and routes frames between a relay
//! connection and individual cartridge processes.

#[cfg(test)]
mod tests {
    use crate::bifaci::cartridge_runtime::CartridgeRuntime;
    use crate::bifaci::frame::{FlowKey, Frame, FrameType, MessageId, SeqAssigner};
    use crate::bifaci::io::{handshake, handshake_accept, FrameReader, FrameWriter};
    use crate::standard::caps::CAP_IDENTITY;
    use tokio::io::{BufReader, BufWriter};

    /// Test manifest JSON - cartridges MUST include manifest in HELLO response.
    /// CAP_IDENTITY is mandatory in every manifest.
    const TEST_MANIFEST: &str = r#"{"name":"TestCartridge","version":"1.0.0","channel":"release","registry_url":null,"description":"Test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";test;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}]}]}"#;

    // TEST293: Test CartridgeRuntime Op registration and lookup by exact and non-existent cap URN
    #[test]
    fn test293_cartridge_runtime_handler_registration() {
        use crate::bifaci::cartridge_runtime::{IdentityOp, Request, WET_KEY_REQUEST};
        use async_trait::async_trait;
        use ops_rs::{DryContext, Op, OpError, OpMetadata, OpResult, WetContext};
        use std::sync::Arc;

        /// Test Op: serializes JSON input to CBOR bytes and emits
        #[derive(Default)]
        struct JsonEchoOp;
        #[async_trait]
        impl Op<()> for JsonEchoOp {
            async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
                let req: Arc<Request> = wet
                    .get_required(WET_KEY_REQUEST)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let input = req
                    .take_input()
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let bytes = input
                    .collect_all_bytes()
                    .await
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                req.output()
                    .emit_cbor(&ciborium::Value::Bytes(bytes))
                    .await
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                Ok(())
            }
            fn metadata(&self) -> OpMetadata {
                OpMetadata::builder("JsonEchoOp").build()
            }
        }

        /// Test Op: emits fixed "transformed" bytes
        #[derive(Default)]
        struct TransformOp;
        #[async_trait]
        impl Op<()> for TransformOp {
            async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
                let req: Arc<Request> = wet
                    .get_required(WET_KEY_REQUEST)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let _input = req
                    .take_input()
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                req.output()
                    .emit_cbor(&ciborium::Value::Bytes(b"transformed".to_vec()))
                    .await
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                Ok(())
            }
            fn metadata(&self) -> OpMetadata {
                OpMetadata::builder("TransformOp").build()
            }
        }

        let mut runtime = CartridgeRuntime::new(TEST_MANIFEST.as_bytes());
        runtime.register_op_type::<JsonEchoOp>(CAP_IDENTITY);
        runtime
            .register_op_type::<TransformOp>("cap:in=\"media:void\";transform;out=\"media:void\"");

        // Exact match
        assert!(runtime.find_handler(CAP_IDENTITY).is_some());
        assert!(runtime
            .find_handler("cap:in=\"media:void\";transform;out=\"media:void\"")
            .is_some());

        // Non-existent
        assert!(runtime
            .find_handler("cap:in=\"media:void\";unknown;out=\"media:void\"")
            .is_none());
    }

    /// Helper: create async socket pairs for relay (engine↔runtime).
    fn create_relay_pair() -> (
        crate::bifaci::local_socket::UnixStream,
        crate::bifaci::local_socket::UnixStream,
        crate::bifaci::local_socket::UnixStream,
        crate::bifaci::local_socket::UnixStream,
    ) {
        let (rt_read, eng_write) = crate::bifaci::local_socket::UnixStream::pair().unwrap();
        let (eng_read, rt_write) = crate::bifaci::local_socket::UnixStream::pair().unwrap();

        (rt_read, rt_write, eng_write, eng_read)
    }

    /// Helper: create async socket pairs for cartridge↔runtime.
    fn create_cartridge_pair() -> (
        crate::bifaci::local_socket::UnixStream,
        crate::bifaci::local_socket::UnixStream,
        crate::bifaci::local_socket::UnixStream,
        crate::bifaci::local_socket::UnixStream,
    ) {
        let (c_to_rt, rt_from_c) = crate::bifaci::local_socket::UnixStream::pair().unwrap();
        let (rt_to_c, c_from_rt) = crate::bifaci::local_socket::UnixStream::pair().unwrap();
        (rt_from_c, rt_to_c, c_from_rt, c_to_rt)
    }

    /// Helper: do handshake only on cartridge side (for raw frame tests using `handshake()`).
    async fn cartridge_handshake(
        from_runtime: crate::bifaci::local_socket::UnixStream,
        to_runtime: crate::bifaci::local_socket::UnixStream,
        manifest: &[u8],
    ) -> (
        FrameReader<BufReader<crate::bifaci::local_socket::UnixStream>>,
        FrameWriter<BufWriter<crate::bifaci::local_socket::UnixStream>>,
    ) {
        let mut reader = FrameReader::new(BufReader::new(from_runtime));
        let mut writer = FrameWriter::new(BufWriter::new(to_runtime));
        let limits = handshake_accept(&mut reader, &mut writer, manifest, &crate::bifaci::pools::PoolStates::new())
            .await
            .unwrap();
        reader.set_limits(limits);
        writer.set_limits(limits);
        (reader, writer)
    }

    /// Helper: do handshake + handle identity verification (for tests using `attach_cartridge()`).
    async fn cartridge_handshake_with_identity(
        from_runtime: crate::bifaci::local_socket::UnixStream,
        to_runtime: crate::bifaci::local_socket::UnixStream,
        manifest: &[u8],
    ) -> (
        FrameReader<BufReader<crate::bifaci::local_socket::UnixStream>>,
        FrameWriter<BufWriter<crate::bifaci::local_socket::UnixStream>>,
    ) {
        let (mut reader, mut writer) =
            cartridge_handshake(from_runtime, to_runtime, manifest).await;

        // Handle identity verification REQ
        let req = reader
            .read()
            .await
            .unwrap()
            .expect("expected identity REQ after handshake");
        assert_eq!(
            req.frame_type,
            FrameType::Req,
            "first frame after handshake must be identity REQ"
        );
        let mut payload = Vec::new();
        loop {
            let f = reader.read().await.unwrap().expect("expected frame");
            match f.frame_type {
                FrameType::StreamStart => {}
                FrameType::Chunk => payload.extend(f.payload.unwrap_or_default()),
                FrameType::StreamEnd => {}
                FrameType::End => break,
                other => panic!(
                    "unexpected frame type during identity verification: {:?}",
                    other
                ),
            }
        }
        let stream_id = "identity-echo".to_string();
        let ss = Frame::stream_start(
            req.id.clone(),
            stream_id.clone(),
            "media:".to_string(),
            None,
        );
        writer.write(&ss).await.unwrap();
        let checksum = Frame::compute_checksum(&payload);
        let chunk = Frame::chunk(req.id.clone(), stream_id.clone(), 0, payload, 0, checksum);
        writer.write(&chunk).await.unwrap();
        let se = Frame::stream_end(req.id.clone(), stream_id, 1);
        writer.write(&se).await.unwrap();
        let end = Frame::end(req.id, None);
        writer.write(&end).await.unwrap();

        (reader, writer)
    }

    // TEST1122: Full path: engine REQ → runtime → cartridge → response back through relay
    #[tokio::test]
    async fn test1122_full_path_engine_req_to_cartridge_response() {
        use crate::bifaci::host_runtime::CartridgeHostRuntime;

        let manifest = r#"{"name":"EchoCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Echo test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Test","aliases": ["test"],"args":[]}]}]}"#;

        let (c_read, c_write, c_from_rt, c_to_rt) = create_cartridge_pair();
        let (rt_relay_read, rt_relay_write, eng_write, eng_read) = create_relay_pair();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (mut reader, mut writer) =
                cartridge_handshake_with_identity(c_from_rt, c_to_rt, &m).await;

            let req = reader.read().await.unwrap().expect("Expected REQ");
            assert_eq!(req.frame_type, FrameType::Req);
            assert_eq!(req.cap.as_deref(), Some(CAP_IDENTITY));

            let mut arg_data = Vec::new();
            loop {
                let f = reader.read().await.unwrap().expect("Expected frame");
                match f.frame_type {
                    FrameType::Chunk => arg_data.extend(f.payload.unwrap_or_default()),
                    FrameType::End => break,
                    _ => {}
                }
            }

            let mut seq = SeqAssigner::new();
            let sid = "resp".to_string();
            let mut start =
                Frame::stream_start(req.id.clone(), sid.clone(), "media:".to_string(), None);
            seq.assign(&mut start);
            writer.write(&start).await.unwrap();
            let checksum = Frame::compute_checksum(&arg_data);
            let mut chunk = Frame::chunk(req.id.clone(), sid.clone(), 0, arg_data, 0, checksum);
            seq.assign(&mut chunk);
            writer.write(&chunk).await.unwrap();
            let mut stream_end = Frame::stream_end(req.id.clone(), sid, 1);
            seq.assign(&mut stream_end);
            writer.write(&stream_end).await.unwrap();
            let mut end = Frame::end(req.id, None);
            seq.assign(&mut end);
            writer.write(&end).await.unwrap();
            seq.remove(&FlowKey::from_frame(&end));
            drop(writer);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(c_read, c_write).await.unwrap();

        // Engine task: send request, wait for response, THEN close relay
        let req_id = MessageId::new_uuid();
        let engine_task = tokio::spawn(async move {
            let mut w = FrameWriter::new(eng_write);
            let mut r = FrameReader::new(eng_read);

            let mut seq = SeqAssigner::new();
            let sid = uuid::Uuid::new_v4().to_string();
            let xid = MessageId::Uint(1);
            let mut req_frame = Frame::req(req_id.clone(), CAP_IDENTITY, vec![], "text/plain");
            req_frame.routing_id = Some(xid.clone());
            seq.assign(&mut req_frame);
            w.write(&req_frame).await.unwrap();
            let mut stream_start =
                Frame::stream_start(req_id.clone(), sid.clone(), "media:".to_string(), None);
            stream_start.routing_id = Some(xid.clone());
            seq.assign(&mut stream_start);
            w.write(&stream_start).await.unwrap();
            let payload = b"hello world".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req_id.clone(), sid.clone(), 0, payload, 0, checksum);
            chunk.routing_id = Some(xid.clone());
            seq.assign(&mut chunk);
            w.write(&chunk).await.unwrap();
            let mut stream_end = Frame::stream_end(req_id.clone(), sid, 1);
            stream_end.routing_id = Some(xid.clone());
            seq.assign(&mut stream_end);
            w.write(&stream_end).await.unwrap();
            let mut end = Frame::end(req_id.clone(), None);
            end.routing_id = Some(xid.clone());
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey::from_frame(&end));

            // Read response
            let mut payload = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk {
                            payload.extend(f.payload.unwrap_or_default());
                        }
                        if f.frame_type == FrameType::End {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            drop(w); // Close relay AFTER response received
            payload
        });

        let result = runtime.run(rt_relay_read, rt_relay_write, || vec![]).await;
        assert!(result.is_ok(), "Runtime should exit cleanly: {:?}", result);

        let response = engine_task.await.unwrap();
        assert_eq!(
            response, b"hello world",
            "Cartridge should echo back the argument data"
        );

        cartridge_handle.await.unwrap();
    }

    // TEST1123: Cartridge ERR frame flows back to engine through relay
    #[tokio::test]
    async fn test1123_cartridge_error_flows_to_engine() {
        use crate::bifaci::host_runtime::CartridgeHostRuntime;

        let manifest = r#"{"name":"ErrCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Error test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";fail;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}]}]}"#;

        let (c_read, c_write, c_from_rt, c_to_rt) = create_cartridge_pair();
        let (rt_relay_read, rt_relay_write, eng_write, eng_read) = create_relay_pair();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (mut reader, mut writer) =
                cartridge_handshake_with_identity(c_from_rt, c_to_rt, &m).await;

            let req = reader.read().await.unwrap().expect("Expected REQ");
            let mut seq = SeqAssigner::new();
            let mut err = Frame::err(
                req.id,
                "FAIL_CODE",
                crate::AttributionClass::Internal,
                "Something went wrong",
                None,
            );
            seq.assign(&mut err);
            writer.write(&err).await.unwrap();
            seq.remove(&FlowKey::from_frame(&err));
            drop(writer);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(c_read, c_write).await.unwrap();

        let req_id = MessageId::new_uuid();
        let engine_task = tokio::spawn(async move {
            let mut w = FrameWriter::new(eng_write);
            let mut r = FrameReader::new(eng_read);

            let mut seq = SeqAssigner::new();
            let xid = MessageId::Uint(1);
            let mut req = Frame::req(
                req_id.clone(),
                "cap:in=\"media:void\";fail;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let mut end = Frame::end(req_id.clone(), None);
            end.routing_id = Some(xid.clone());
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey::from_frame(&end));

            let mut err_code = String::new();
            let mut err_msg = String::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Err {
                            err_code = f.error_code().unwrap_or("").to_string();
                            err_msg = f.error_message().unwrap_or("").to_string();
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            drop(w);
            (err_code, err_msg)
        });

        let _ = runtime.run(rt_relay_read, rt_relay_write, || vec![]).await;

        let (code, msg) = engine_task.await.unwrap();
        assert_eq!(code, "FAIL_CODE");
        assert_eq!(msg, "Something went wrong");

        cartridge_handle.await.unwrap();
    }

    // TEST898: Binary data integrity through full relay path (256 byte values)
    #[tokio::test]
    async fn test898_binary_integrity_through_relay() {
        use crate::bifaci::host_runtime::CartridgeHostRuntime;

        let manifest = r#"{"name":"BinCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Binary test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";binary;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}]}]}"#;

        let (c_read, c_write, c_from_rt, c_to_rt) = create_cartridge_pair();
        let (rt_relay_read, rt_relay_write, eng_write, eng_read) = create_relay_pair();

        let binary_data: Vec<u8> = (0u16..=255).map(|i| i as u8).collect();
        let binary_clone = binary_data.clone();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (mut reader, mut writer) =
                cartridge_handshake_with_identity(c_from_rt, c_to_rt, &m).await;

            let req = reader.read().await.unwrap().expect("Expected REQ");

            let mut received = Vec::new();
            loop {
                let f = reader.read().await.unwrap().expect("frame");
                match f.frame_type {
                    FrameType::Chunk => received.extend(f.payload.unwrap_or_default()),
                    FrameType::End => break,
                    _ => {}
                }
            }

            assert_eq!(received.len(), 256, "Must receive all 256 bytes");
            for (i, &b) in received.iter().enumerate() {
                assert_eq!(b, i as u8, "Byte mismatch at position {}", i);
            }

            let mut seq = SeqAssigner::new();
            let sid = "resp".to_string();
            let mut start =
                Frame::stream_start(req.id.clone(), sid.clone(), "media:".to_string(), None);
            seq.assign(&mut start);
            writer.write(&start).await.unwrap();
            let checksum = Frame::compute_checksum(&received);
            let mut chunk = Frame::chunk(req.id.clone(), sid.clone(), 0, received, 0, checksum);
            seq.assign(&mut chunk);
            writer.write(&chunk).await.unwrap();
            let mut stream_end = Frame::stream_end(req.id.clone(), sid, 1);
            seq.assign(&mut stream_end);
            writer.write(&stream_end).await.unwrap();
            let mut end = Frame::end(req.id, None);
            seq.assign(&mut end);
            writer.write(&end).await.unwrap();
            seq.remove(&FlowKey::from_frame(&end));
            drop(writer);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(c_read, c_write).await.unwrap();

        let req_id = MessageId::new_uuid();
        let engine_task = tokio::spawn(async move {
            let mut w = FrameWriter::new(eng_write);
            let mut r = FrameReader::new(eng_read);

            let mut seq = SeqAssigner::new();
            let xid = MessageId::Uint(1);
            let sid = uuid::Uuid::new_v4().to_string();
            let mut req = Frame::req(
                req_id.clone(),
                "cap:in=\"media:void\";binary;out=\"media:void\"",
                vec![],
                "application/octet-stream",
            );
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let mut stream_start =
                Frame::stream_start(req_id.clone(), sid.clone(), "media:".to_string(), None);
            stream_start.routing_id = Some(xid.clone());
            seq.assign(&mut stream_start);
            w.write(&stream_start).await.unwrap();
            let checksum = Frame::compute_checksum(&binary_clone);
            let mut chunk = Frame::chunk(req_id.clone(), sid.clone(), 0, binary_clone, 0, checksum);
            chunk.routing_id = Some(xid.clone());
            seq.assign(&mut chunk);
            w.write(&chunk).await.unwrap();
            let mut stream_end = Frame::stream_end(req_id.clone(), sid, 1);
            stream_end.routing_id = Some(xid.clone());
            seq.assign(&mut stream_end);
            w.write(&stream_end).await.unwrap();
            let mut end = Frame::end(req_id.clone(), None);
            end.routing_id = Some(xid.clone());
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey::from_frame(&end));

            let mut payload = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk {
                            payload.extend(f.payload.unwrap_or_default());
                        }
                        if f.frame_type == FrameType::End {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            drop(w);
            payload
        });

        let _ = runtime.run(rt_relay_read, rt_relay_write, || vec![]).await;

        let response = engine_task.await.unwrap();
        assert_eq!(response.len(), 256);
        for (i, &b) in response.iter().enumerate() {
            assert_eq!(b, i as u8, "Response byte mismatch at position {}", i);
        }

        cartridge_handle.await.unwrap();
    }

    // TEST899: Streaming chunks flow through relay without accumulation
    #[tokio::test]
    async fn test899_streaming_chunks_through_relay() {
        use crate::bifaci::host_runtime::CartridgeHostRuntime;

        let manifest = r#"{"name":"StreamCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Streaming test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";stream;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}]}]}"#;

        let (c_read, c_write, c_from_rt, c_to_rt) = create_cartridge_pair();
        let (rt_relay_read, rt_relay_write, eng_write, eng_read) = create_relay_pair();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (mut reader, mut writer) =
                cartridge_handshake_with_identity(c_from_rt, c_to_rt, &m).await;

            let req = reader.read().await.unwrap().expect("Expected REQ");

            loop {
                let f = reader.read().await.unwrap().expect("frame");
                if f.frame_type == FrameType::End {
                    break;
                }
            }

            let sid = "resp".to_string();
            let mut seq = SeqAssigner::new();
            let mut start =
                Frame::stream_start(req.id.clone(), sid.clone(), "media:".to_string(), None);
            seq.assign(&mut start);
            writer.write(&start).await.unwrap();
            for idx in 0u64..5 {
                let data = format!("chunk{}", idx).into_bytes();
                let checksum = Frame::compute_checksum(&data);
                let mut chunk = Frame::chunk(req.id.clone(), sid.clone(), 0, data, idx, checksum);
                seq.assign(&mut chunk);
                writer.write(&chunk).await.unwrap();
            }
            let mut stream_end = Frame::stream_end(req.id.clone(), sid, 5);
            seq.assign(&mut stream_end);
            writer.write(&stream_end).await.unwrap();
            let mut end = Frame::end(req.id, None);
            seq.assign(&mut end);
            writer.write(&end).await.unwrap();
            drop(writer);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(c_read, c_write).await.unwrap();

        let req_id = MessageId::new_uuid();
        let engine_task = tokio::spawn(async move {
            let mut w = FrameWriter::new(eng_write);
            let mut r = FrameReader::new(eng_read);

            let mut seq = SeqAssigner::new();
            let xid = MessageId::Uint(1);
            let mut req = Frame::req(
                req_id.clone(),
                "cap:in=\"media:void\";stream;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let mut end = Frame::end(req_id.clone(), None);
            end.routing_id = Some(xid.clone());
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey::from_frame(&end));

            let mut chunks = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk {
                            chunks.push((f.seq, f.payload.unwrap_or_default()));
                        }
                        if f.frame_type == FrameType::End {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            drop(w);
            chunks
        });

        let _ = runtime.run(rt_relay_read, rt_relay_write, || vec![]).await;

        let chunks = engine_task.await.unwrap();
        assert_eq!(chunks.len(), 5, "All 5 chunks must arrive");
        for (i, (seq, data)) in chunks.iter().enumerate() {
            assert_eq!(
                *seq,
                (i + 1) as u64,
                "Chunk seq must be contiguous from 1 (StreamStart takes seq 0)"
            );
            assert_eq!(
                data,
                &format!("chunk{}", i).into_bytes(),
                "Chunk data must match"
            );
        }

        cartridge_handle.await.unwrap();
    }

    // TEST430: REMOVED - outdated test that doesn't represent real architecture
    // Real system requires RelaySwitch to assign XIDs to peer requests.
    // Peer invoke functionality is tested in bidirectional_interop tests with full relay stack.

    // TEST900: Two cartridges routed independently by cap_urn
    #[tokio::test]
    async fn test900_two_cartridges_routed_independently() {
        use crate::bifaci::host_runtime::CartridgeHostRuntime;

        let manifest_a = r#"{"name":"CartridgeA","version":"1.0","channel":"release","registry_url":null,"description":"Cartridge A","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";alpha;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}]}]}"#;
        let manifest_b = r#"{"name":"CartridgeB","version":"1.0","channel":"release","registry_url":null,"description":"Cartridge B","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";beta;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}]}]}"#;

        let (ca_read, ca_write, ca_from_rt, ca_to_rt) = create_cartridge_pair();
        let (cb_read, cb_write, cb_from_rt, cb_to_rt) = create_cartridge_pair();
        let (rt_relay_read, rt_relay_write, eng_write, eng_read) = create_relay_pair();

        let ma = manifest_a.as_bytes().to_vec();
        let cartridge_a = tokio::spawn(async move {
            let (mut reader, mut writer) =
                cartridge_handshake_with_identity(ca_from_rt, ca_to_rt, &ma).await;
            let req = reader.read().await.unwrap().expect("Expected REQ");
            assert_eq!(
                req.cap.as_deref(),
                Some("cap:in=\"media:void\";alpha;out=\"media:void\""),
                "Cartridge A must receive alpha REQ"
            );
            loop {
                let f = reader.read().await.unwrap().expect("f");
                if f.frame_type == FrameType::End {
                    break;
                }
            }
            let mut seq = SeqAssigner::new();
            let sid = "a".to_string();
            let mut start =
                Frame::stream_start(req.id.clone(), sid.clone(), "media:".to_string(), None);
            seq.assign(&mut start);
            writer.write(&start).await.unwrap();
            let payload = b"from-alpha".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req.id.clone(), sid.clone(), 0, payload, 0, checksum);
            seq.assign(&mut chunk);
            writer.write(&chunk).await.unwrap();
            let mut stream_end = Frame::stream_end(req.id.clone(), sid, 1);
            seq.assign(&mut stream_end);
            writer.write(&stream_end).await.unwrap();
            let mut end = Frame::end(req.id, None);
            seq.assign(&mut end);
            writer.write(&end).await.unwrap();
            seq.remove(&FlowKey::from_frame(&end));
            drop(writer);
        });

        let mb = manifest_b.as_bytes().to_vec();
        let cartridge_b = tokio::spawn(async move {
            let (mut reader, mut writer) =
                cartridge_handshake_with_identity(cb_from_rt, cb_to_rt, &mb).await;
            let req = reader.read().await.unwrap().expect("Expected REQ");
            assert_eq!(
                req.cap.as_deref(),
                Some("cap:in=\"media:void\";beta;out=\"media:void\""),
                "Cartridge B must receive beta REQ"
            );
            loop {
                let f = reader.read().await.unwrap().expect("f");
                if f.frame_type == FrameType::End {
                    break;
                }
            }
            let mut seq = SeqAssigner::new();
            let sid = "b".to_string();
            let mut start =
                Frame::stream_start(req.id.clone(), sid.clone(), "media:".to_string(), None);
            seq.assign(&mut start);
            writer.write(&start).await.unwrap();
            let payload = b"from-beta".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req.id.clone(), sid.clone(), 0, payload, 0, checksum);
            seq.assign(&mut chunk);
            writer.write(&chunk).await.unwrap();
            let mut stream_end = Frame::stream_end(req.id.clone(), sid, 1);
            seq.assign(&mut stream_end);
            writer.write(&stream_end).await.unwrap();
            let mut end = Frame::end(req.id, None);
            seq.assign(&mut end);
            writer.write(&end).await.unwrap();
            seq.remove(&FlowKey::from_frame(&end));
            drop(writer);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(ca_read, ca_write).await.unwrap();
        runtime.attach_cartridge(cb_read, cb_write).await.unwrap();

        let alpha_id = MessageId::new_uuid();
        let beta_id = MessageId::new_uuid();
        let alpha_id_c = alpha_id.clone();
        let beta_id_c = beta_id.clone();

        let engine_task = tokio::spawn(async move {
            let mut w = FrameWriter::new(eng_write);
            let mut r = FrameReader::new(eng_read);

            let mut seq = SeqAssigner::new();
            let xid_alpha = MessageId::Uint(1);
            let xid_beta = MessageId::Uint(2);
            let mut req_alpha = Frame::req(
                alpha_id_c.clone(),
                "cap:in=\"media:void\";alpha;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req_alpha.routing_id = Some(xid_alpha.clone());
            seq.assign(&mut req_alpha);
            w.write(&req_alpha).await.unwrap();
            let mut end_alpha = Frame::end(alpha_id_c.clone(), None);
            end_alpha.routing_id = Some(xid_alpha.clone());
            seq.assign(&mut end_alpha);
            w.write(&end_alpha).await.unwrap();
            seq.remove(&FlowKey::from_frame(&end_alpha));
            let mut req_beta = Frame::req(
                beta_id_c.clone(),
                "cap:in=\"media:void\";beta;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req_beta.routing_id = Some(xid_beta.clone());
            seq.assign(&mut req_beta);
            w.write(&req_beta).await.unwrap();
            let mut end_beta = Frame::end(beta_id_c.clone(), None);
            end_beta.routing_id = Some(xid_beta.clone());
            seq.assign(&mut end_beta);
            w.write(&end_beta).await.unwrap();
            seq.remove(&FlowKey::from_frame(&end_beta));

            let mut alpha_data = Vec::new();
            let mut beta_data = Vec::new();
            let mut ends_received = 0;
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk {
                            if f.id == alpha_id_c {
                                alpha_data.extend(f.payload.unwrap_or_default());
                            } else if f.id == beta_id_c {
                                beta_data.extend(f.payload.unwrap_or_default());
                            }
                        }
                        if f.frame_type == FrameType::End {
                            ends_received += 1;
                            if ends_received >= 2 {
                                break;
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            drop(w);
            (alpha_data, beta_data)
        });

        let _ = runtime.run(rt_relay_read, rt_relay_write, || vec![]).await;

        let (alpha_data, beta_data) = engine_task.await.unwrap();
        assert_eq!(
            alpha_data, b"from-alpha",
            "Alpha response must come from Cartridge A"
        );
        assert_eq!(
            beta_data, b"from-beta",
            "Beta response must come from Cartridge B"
        );

        cartridge_a.await.unwrap();
        cartridge_b.await.unwrap();
    }

    // TEST901: REQ for unknown cap returns ERR frame (not fatal)
    #[tokio::test]
    async fn test901_req_for_unknown_cap_returns_err_frame() {
        use crate::bifaci::host_runtime::CartridgeHostRuntime;

        let manifest = r#"{"name":"OneCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Known cap cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";known;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}]}]}"#;

        let (c_read, c_write, c_from_rt, c_to_rt) = create_cartridge_pair();
        let (rt_relay_read, rt_relay_write, eng_write, eng_read) = create_relay_pair();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            tracing::info!("[TEST/cartridge] Starting cartridge thread");
            let (mut reader, _writer) =
                cartridge_handshake_with_identity(c_from_rt, c_to_rt, &m).await;
            tracing::info!("[TEST/cartridge] Handshake complete, waiting for EOF...");
            // Cartridge waits for EOF — no REQ should arrive since cap is unknown
            match reader.read().await {
                Ok(None) => {
                    tracing::info!("[TEST/cartridge] Got EOF, cartridge exiting normally");
                }
                Ok(Some(f)) => {
                    tracing::error!(
                        "[TEST/cartridge] ERROR: Got frame {:?}, expected EOF!",
                        f.frame_type
                    );
                    panic!(
                        "Cartridge should not receive frames for unknown cap, got {:?}",
                        f.frame_type
                    )
                }
                Err(e) => {
                    tracing::error!("[TEST/cartridge] Got error: {:?}, treating as EOF", e);
                }
            }
            tracing::info!("[TEST/cartridge] Cartridge thread completing");
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(c_read, c_write).await.unwrap();

        let req_id = MessageId::new_uuid();
        let req_id_clone = req_id.clone();
        let engine_send = tokio::spawn(async move {
            let mut w = FrameWriter::new(eng_write);
            let mut seq = SeqAssigner::new();
            let xid = MessageId::Uint(1);
            let mut req = Frame::req(
                req_id_clone.clone(),
                "cap:in=\"media:void\";unknown;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let mut end = Frame::end(req_id_clone, None);
            end.routing_id = Some(xid.clone());
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey::from_frame(&end));
        });

        // Read ERR frame from the host on the engine side
        let engine_recv = tokio::spawn(async move {
            let mut r = FrameReader::new(eng_read);
            // Skip RelayNotify (initial capabilities notification)
            tracing::info!("[TEST/engine_recv] Starting, attempting first read...");
            let mut frame = r.read().await.unwrap().expect("Expected first frame");
            tracing::info!("[TEST/engine_recv] First frame: {:?}", frame.frame_type);
            if frame.frame_type == FrameType::RelayNotify {
                tracing::info!("[TEST/engine_recv] Got RelayNotify, reading second frame...");
                frame = r
                    .read()
                    .await
                    .unwrap()
                    .expect("Expected ERR frame after RelayNotify");
                tracing::info!("[TEST/engine_recv] Second frame: {:?}", frame.frame_type);
            }
            tracing::info!("[TEST/engine_recv] Asserting frame is ERR...");
            assert_eq!(
                frame.frame_type,
                FrameType::Err,
                "Should get ERR for unknown cap"
            );
            assert_eq!(
                frame.id, req_id,
                "ERR should reference the original request ID"
            );
            let meta = frame.meta.as_ref().expect("ERR should have meta");
            let code = meta.get("code").and_then(|v| v.as_text()).unwrap_or("");
            assert_eq!(
                code, "NO_HANDLER",
                "Error code should be NO_HANDLER, got: {}",
                code
            );
            tracing::info!("[TEST/engine_recv] All assertions passed, task completing!");
        });

        // Host run should NOT return an error — it sends ERR frame and continues
        let run_handle =
            tokio::spawn(
                async move { runtime.run(rt_relay_read, rt_relay_write, || vec![]).await },
            );

        tracing::info!("[TEST] Waiting for engine_send to complete...");
        engine_send.await.unwrap();
        tracing::info!("[TEST] engine_send completed, waiting for engine_recv...");
        engine_recv.await.unwrap();
        tracing::info!("[TEST] engine_recv completed, test done!");

        // Host and cartridge are still running. Just drop them - they'll clean up when test ends.
        drop(run_handle);
        drop(cartridge_handle);
    }

    // =============================================================================
    // Low-level Frame-based Integration Tests (TEST284-299)
    // Ported from Go integration_test.go
    // =============================================================================

    // TEST284: Handshake exchanges HELLO frames, negotiates limits
    #[tokio::test]
    async fn test284_handshake_host_cartridge() {
        use crate::bifaci::io::{handshake, handshake_accept};

        // Single bidirectional socket pair - each end can read and write
        let (host_sock, cartridge_sock) = crate::bifaci::local_socket::UnixStream::pair().unwrap();

        // Split each socket into read/write halves
        let (host_read, host_write) = host_sock.into_split();
        let (cartridge_read, cartridge_write) = cartridge_sock.into_split();

        let manifest = TEST_MANIFEST.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let mut reader = FrameReader::new(BufReader::new(cartridge_read));
            let mut writer = FrameWriter::new(BufWriter::new(cartridge_write));
            let limits = handshake_accept(&mut reader, &mut writer, &manifest, &crate::bifaci::pools::PoolStates::new())
                .await
                .unwrap();
            assert!(limits.max_frame > 0);
            assert!(limits.max_chunk > 0);
            limits
        });

        let mut reader = FrameReader::new(BufReader::new(host_read));
        let mut writer = FrameWriter::new(BufWriter::new(host_write));
        let handshake_result = handshake(&mut reader, &mut writer).await.unwrap();
        let received_manifest = handshake_result.manifest;
        let host_limits = handshake_result.limits;

        assert_eq!(received_manifest, TEST_MANIFEST.as_bytes());

        let cartridge_limits = cartridge_handle.await.unwrap();
        assert_eq!(host_limits.max_frame, cartridge_limits.max_frame);
        assert_eq!(host_limits.max_chunk, cartridge_limits.max_chunk);
    }

    // TEST285: Simple request-response flow (REQ → END with payload)
    #[tokio::test]
    async fn test285_request_response_simple() {
        let (cartridge_from_host, host_to_cartridge) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();
        let (host_from_cartridge, cartridge_to_host) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();

        let manifest = TEST_MANIFEST.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (mut reader, mut writer) =
                cartridge_handshake(cartridge_from_host, cartridge_to_host, &manifest).await;

            let frame = reader.read().await.unwrap().unwrap();
            assert_eq!(frame.frame_type, FrameType::Req);
            assert_eq!(frame.cap.as_deref(), Some(CAP_IDENTITY));
            assert_eq!(frame.payload.as_deref(), Some(b"hello".as_ref()));

            let mut seq = SeqAssigner::new();
            let mut end = Frame::end(frame.id, Some(b"hello back".to_vec()));
            seq.assign(&mut end);
            writer.write(&end).await.unwrap();
            seq.remove(&FlowKey::from_frame(&end));
        });

        let mut reader = FrameReader::new(BufReader::new(host_from_cartridge));
        let mut writer = FrameWriter::new(BufWriter::new(host_to_cartridge));
        let handshake_result = handshake(&mut reader, &mut writer).await.unwrap();
        let limits = handshake_result.limits;
        reader.set_limits(limits);
        writer.set_limits(limits);

        let mut seq = SeqAssigner::new();
        let request_id = MessageId::new_uuid();
        let mut req = Frame::req(
            request_id.clone(),
            CAP_IDENTITY,
            b"hello".to_vec(),
            "application/json",
        );
        seq.assign(&mut req);
        writer.write(&req).await.unwrap();

        let response = reader.read().await.unwrap().unwrap();
        assert_eq!(response.frame_type, FrameType::End);
        assert_eq!(response.payload.as_deref(), Some(b"hello back".as_ref()));

        cartridge_handle.await.unwrap();
    }

    // TEST286: Streaming response with multiple CHUNK frames
    #[tokio::test]
    async fn test286_streaming_chunks() {
        let (cartridge_from_host, host_to_cartridge) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();
        let (host_from_cartridge, cartridge_to_host) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();

        let manifest = TEST_MANIFEST.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (mut reader, mut writer) =
                cartridge_handshake(cartridge_from_host, cartridge_to_host, &manifest).await;

            let frame = reader.read().await.unwrap().unwrap();
            let request_id = frame.id.clone();

            let sid = "response".to_string();
            let mut seq = SeqAssigner::new();
            let mut start =
                Frame::stream_start(request_id.clone(), sid.clone(), "media:".to_string(), None);
            seq.assign(&mut start);
            writer.write(&start).await.unwrap();
            for (idx, data) in [b"chunk1", b"chunk2", b"chunk3"].iter().enumerate() {
                let payload = data.to_vec();
                let checksum = Frame::compute_checksum(&payload);
                let mut chunk = Frame::chunk(
                    request_id.clone(),
                    sid.clone(),
                    0,
                    payload,
                    idx as u64,
                    checksum,
                );
                seq.assign(&mut chunk);
                writer.write(&chunk).await.unwrap();
            }
            let mut stream_end = Frame::stream_end(request_id.clone(), sid, 3);
            seq.assign(&mut stream_end);
            writer.write(&stream_end).await.unwrap();
            let mut end = Frame::end(request_id, None);
            seq.assign(&mut end);
            writer.write(&end).await.unwrap();
        });

        let mut reader = FrameReader::new(BufReader::new(host_from_cartridge));
        let mut writer = FrameWriter::new(BufWriter::new(host_to_cartridge));
        let handshake_result = handshake(&mut reader, &mut writer).await.unwrap();
        let limits = handshake_result.limits;
        reader.set_limits(limits);
        writer.set_limits(limits);

        let mut seq = SeqAssigner::new();
        let request_id = MessageId::new_uuid();
        let mut req = Frame::req(
            request_id.clone(),
            "cap:in=\"media:void\";stream;out=\"media:void\"",
            b"go".to_vec(),
            "application/json",
        );
        seq.assign(&mut req);
        writer.write(&req).await.unwrap();

        // Collect chunks
        let mut chunks = Vec::new();
        loop {
            let frame = reader.read().await.unwrap().unwrap();
            if frame.frame_type == FrameType::Chunk {
                chunks.push(frame.payload.unwrap_or_default());
            }
            if frame.frame_type == FrameType::End {
                break;
            }
        }

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], b"chunk1");
        assert_eq!(chunks[1], b"chunk2");
        assert_eq!(chunks[2], b"chunk3");

        cartridge_handle.await.unwrap();
    }

    // TEST287: Host-initiated heartbeat
    #[tokio::test]
    async fn test287_heartbeat_from_host() {
        let (cartridge_from_host, host_to_cartridge) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();
        let (host_from_cartridge, cartridge_to_host) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();

        let manifest = TEST_MANIFEST.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (mut reader, mut writer) =
                cartridge_handshake(cartridge_from_host, cartridge_to_host, &manifest).await;

            let frame = reader.read().await.unwrap().unwrap();
            assert_eq!(frame.frame_type, FrameType::Heartbeat);

            let mut seq = SeqAssigner::new();
            let mut hb = Frame::heartbeat(frame.id);
            seq.assign(&mut hb);
            writer.write(&hb).await.unwrap();
        });

        let mut reader = FrameReader::new(BufReader::new(host_from_cartridge));
        let mut writer = FrameWriter::new(BufWriter::new(host_to_cartridge));
        let handshake_result = handshake(&mut reader, &mut writer).await.unwrap();
        let limits = handshake_result.limits;
        reader.set_limits(limits);
        writer.set_limits(limits);

        let mut seq = SeqAssigner::new();
        let heartbeat_id = MessageId::new_uuid();
        let mut hb = Frame::heartbeat(heartbeat_id.clone());
        seq.assign(&mut hb);
        writer.write(&hb).await.unwrap();

        let response = reader.read().await.unwrap().unwrap();
        assert_eq!(response.frame_type, FrameType::Heartbeat);
        assert_eq!(response.id, heartbeat_id);

        cartridge_handle.await.unwrap();
    }

    // TEST290: Limit negotiation picks minimum
    #[tokio::test]
    async fn test290_limits_negotiation() {
        use crate::bifaci::io::{handshake, handshake_accept};

        let (cartridge_from_host, host_to_cartridge) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();
        let (host_from_cartridge, cartridge_to_host) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();

        let manifest = TEST_MANIFEST.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let mut reader = FrameReader::new(BufReader::new(cartridge_from_host));
            let mut writer = FrameWriter::new(BufWriter::new(cartridge_to_host));
            handshake_accept(&mut reader, &mut writer, &manifest, &crate::bifaci::pools::PoolStates::new())
                .await
                .unwrap()
        });

        let mut reader = FrameReader::new(BufReader::new(host_from_cartridge));
        let mut writer = FrameWriter::new(BufWriter::new(host_to_cartridge));
        let handshake_result = handshake(&mut reader, &mut writer).await.unwrap();
        let host_limits = handshake_result.limits;

        let cartridge_limits = cartridge_handle.await.unwrap();

        assert_eq!(host_limits.max_frame, cartridge_limits.max_frame);
        assert_eq!(host_limits.max_chunk, cartridge_limits.max_chunk);
        assert!(host_limits.max_frame > 0);
        assert!(host_limits.max_chunk > 0);
    }

    // TEST291: Binary payload roundtrip (all 256 byte values)
    #[tokio::test]
    async fn test291_binary_payload_roundtrip() {
        let (cartridge_from_host, host_to_cartridge) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();
        let (host_from_cartridge, cartridge_to_host) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();

        let binary_data: Vec<u8> = (0u16..=255).map(|i| i as u8).collect();
        let binary_clone = binary_data.clone();

        let manifest = TEST_MANIFEST.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (mut reader, mut writer) =
                cartridge_handshake(cartridge_from_host, cartridge_to_host, &manifest).await;

            let frame = reader.read().await.unwrap().unwrap();
            let payload = frame.payload.unwrap();

            assert_eq!(payload.len(), 256);
            for (i, &byte) in payload.iter().enumerate() {
                assert_eq!(byte, i as u8, "Byte mismatch at position {}", i);
            }

            let mut seq = SeqAssigner::new();
            let mut end = Frame::end(frame.id, Some(payload));
            seq.assign(&mut end);
            writer.write(&end).await.unwrap();
            seq.remove(&FlowKey::from_frame(&end));
        });

        let mut reader = FrameReader::new(BufReader::new(host_from_cartridge));
        let mut writer = FrameWriter::new(BufWriter::new(host_to_cartridge));
        let handshake_result = handshake(&mut reader, &mut writer).await.unwrap();
        let limits = handshake_result.limits;
        reader.set_limits(limits);
        writer.set_limits(limits);

        let mut seq = SeqAssigner::new();
        let request_id = MessageId::new_uuid();
        let mut req = Frame::req(
            request_id.clone(),
            "cap:in=\"media:void\";binary;out=\"media:void\"",
            binary_clone,
            "application/octet-stream",
        );
        seq.assign(&mut req);
        writer.write(&req).await.unwrap();

        let response = reader.read().await.unwrap().unwrap();
        let result = response.payload.unwrap();

        assert_eq!(result.len(), 256);
        for (i, &byte) in result.iter().enumerate() {
            assert_eq!(byte, i as u8, "Response byte mismatch at position {}", i);
        }

        cartridge_handle.await.unwrap();
    }

    // TEST292: Sequential requests get distinct MessageIds
    #[tokio::test]
    async fn test292_message_id_uniqueness() {
        use std::sync::{Arc, Mutex};

        let (cartridge_from_host, host_to_cartridge) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();
        let (host_from_cartridge, cartridge_to_host) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();

        let received_ids = Arc::new(Mutex::new(Vec::new()));
        let received_ids_clone = Arc::clone(&received_ids);

        let manifest = TEST_MANIFEST.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (mut reader, mut writer) =
                cartridge_handshake(cartridge_from_host, cartridge_to_host, &manifest).await;

            let mut seq = SeqAssigner::new();
            for _ in 0..3 {
                let frame = reader.read().await.unwrap().unwrap();
                received_ids_clone.lock().unwrap().push(frame.id.clone());
                let mut end = Frame::end(frame.id, Some(b"ok".to_vec()));
                seq.assign(&mut end);
                writer.write(&end).await.unwrap();
                seq.remove(&FlowKey::from_frame(&end));
            }
        });

        let mut reader = FrameReader::new(BufReader::new(host_from_cartridge));
        let mut writer = FrameWriter::new(BufWriter::new(host_to_cartridge));
        let handshake_result = handshake(&mut reader, &mut writer).await.unwrap();
        let limits = handshake_result.limits;
        reader.set_limits(limits);
        writer.set_limits(limits);

        let mut seq = SeqAssigner::new();
        for _ in 0..3 {
            let request_id = MessageId::new_uuid();
            let mut req = Frame::req(
                request_id.clone(),
                "cap:in=\"media:void\";test;out=\"media:void\"",
                vec![],
                "application/json",
            );
            seq.assign(&mut req);
            writer.write(&req).await.unwrap();
            reader.read().await.unwrap().unwrap();
        }

        cartridge_handle.await.unwrap();

        let ids = received_ids.lock().unwrap();
        assert_eq!(ids.len(), 3);
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                assert_ne!(ids[i], ids[j], "IDs should be unique");
            }
        }
    }

    // TEST299: Empty payload request/response roundtrip
    #[tokio::test]
    async fn test299_empty_payload_roundtrip() {
        let (cartridge_from_host, host_to_cartridge) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();
        let (host_from_cartridge, cartridge_to_host) =
            crate::bifaci::local_socket::UnixStream::pair().unwrap();

        let manifest = TEST_MANIFEST.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (mut reader, mut writer) =
                cartridge_handshake(cartridge_from_host, cartridge_to_host, &manifest).await;

            let frame = reader.read().await.unwrap().unwrap();
            assert!(
                frame.payload.is_none() || frame.payload.as_ref().unwrap().is_empty(),
                "empty payload must arrive empty"
            );

            let mut seq = SeqAssigner::new();
            let mut end = Frame::end(frame.id, Some(vec![]));
            seq.assign(&mut end);
            writer.write(&end).await.unwrap();
            seq.remove(&FlowKey::from_frame(&end));
        });

        let mut reader = FrameReader::new(BufReader::new(host_from_cartridge));
        let mut writer = FrameWriter::new(BufWriter::new(host_to_cartridge));
        let handshake_result = handshake(&mut reader, &mut writer).await.unwrap();
        let limits = handshake_result.limits;
        reader.set_limits(limits);
        writer.set_limits(limits);

        let mut seq = SeqAssigner::new();
        let request_id = MessageId::new_uuid();
        let mut req = Frame::req(
            request_id.clone(),
            "cap:in=\"media:void\";empty;out=\"media:void\"",
            vec![],
            "application/json",
        );
        seq.assign(&mut req);
        writer.write(&req).await.unwrap();

        let response = reader.read().await.unwrap().unwrap();
        assert!(response.payload.is_none() || response.payload.as_ref().unwrap().is_empty());

        cartridge_handle.await.unwrap();
    }

    // =========================================================================
    // Identity verification end-to-end tests
    // =========================================================================

    // TEST489: Full path identity verification: engine → host (attach_cartridge) → cartridge
    //
    // This verifies that attach_cartridge completes identity verification end-to-end
    // and the cartridge is ready to handle subsequent requests.
    #[tokio::test]
    async fn test489_full_path_identity_verification() {
        use crate::bifaci::host_runtime::CartridgeHostRuntime;

        let manifest = r#"{"name":"IdentityE2E","version":"1.0","channel":"release","registry_url":null,"description":"Identity test","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";test;out=\"media:void\"","title":"Test","aliases": ["test"],"args":[]}]}]}"#;

        let (c_read, c_write, c_from_rt, c_to_rt) = create_cartridge_pair();
        let (rt_relay_read, rt_relay_write, eng_write, eng_read) = create_relay_pair();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (mut reader, mut writer) =
                cartridge_handshake_with_identity(c_from_rt, c_to_rt, &m).await;

            // After identity verification, handle a real request
            let req = reader
                .read()
                .await
                .unwrap()
                .expect("Expected REQ after identity verification");
            assert_eq!(
                req.frame_type,
                FrameType::Req,
                "Must receive real REQ after identity handshake"
            );

            // Consume request body
            loop {
                let f = reader.read().await.unwrap().expect("Expected frame");
                if f.frame_type == FrameType::End {
                    break;
                }
            }

            // Send response
            let mut seq = SeqAssigner::new();
            let sid = "resp".to_string();
            let mut ss =
                Frame::stream_start(req.id.clone(), sid.clone(), "media:".to_string(), None);
            seq.assign(&mut ss);
            writer.write(&ss).await.unwrap();
            let payload = b"verified-and-working".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req.id.clone(), sid.clone(), 0, payload, 0, checksum);
            seq.assign(&mut chunk);
            writer.write(&chunk).await.unwrap();
            let mut se = Frame::stream_end(req.id.clone(), sid, 1);
            seq.assign(&mut se);
            writer.write(&se).await.unwrap();
            let mut end = Frame::end(req.id, None);
            seq.assign(&mut end);
            writer.write(&end).await.unwrap();
        });

        let (c_read_half, _) = c_read.into_split();
        let (_, c_write_half) = c_write.into_split();

        let mut runtime = CartridgeHostRuntime::new();
        runtime
            .attach_cartridge(c_read_half, c_write_half)
            .await
            .unwrap();

        let (rt_read_half, _) = rt_relay_read.into_split();
        let (_, rt_write_half) = rt_relay_write.into_split();
        let (_, eng_write_half) = eng_write.into_split();
        let (eng_read_half, _) = eng_read.into_split();

        let req_id = MessageId::new_uuid();
        let engine_task = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let mut w = FrameWriter::new(eng_write_half);
            let mut r = FrameReader::new(eng_read_half);

            let xid = MessageId::Uint(1);
            let sid = uuid::Uuid::new_v4().to_string();

            let mut req = Frame::req(
                req_id.clone(),
                "cap:in=\"media:void\";test;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();

            let mut ss =
                Frame::stream_start(req_id.clone(), sid.clone(), "media:".to_string(), None);
            ss.routing_id = Some(xid.clone());
            seq.assign(&mut ss);
            w.write(&ss).await.unwrap();

            let payload = b"test-data".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req_id.clone(), sid.clone(), 0, payload, 0, checksum);
            chunk.routing_id = Some(xid.clone());
            seq.assign(&mut chunk);
            w.write(&chunk).await.unwrap();

            let mut se = Frame::stream_end(req_id.clone(), sid, 1);
            se.routing_id = Some(xid.clone());
            seq.assign(&mut se);
            w.write(&se).await.unwrap();

            let mut end = Frame::end(req_id.clone(), None);
            end.routing_id = Some(xid);
            seq.assign(&mut end);
            w.write(&end).await.unwrap();

            // Read response
            let mut payload = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk {
                            payload.extend(f.payload.unwrap_or_default());
                        }
                        if f.frame_type == FrameType::End {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            drop(w);
            payload
        });

        let result = runtime.run(rt_read_half, rt_write_half, || vec![]).await;
        assert!(result.is_ok(), "Runtime should exit cleanly: {:?}", result);

        let response = engine_task.await.unwrap();
        assert_eq!(
            response, b"verified-and-working",
            "Cartridge must respond after identity verification"
        );

        cartridge_handle.await.unwrap();
    }

    // TEST490: Identity verification with multiple cartridges through single relay
    //
    // Both cartridges must pass identity verification independently before any
    // real requests are routed.
    #[tokio::test]
    async fn test490_identity_verification_multiple_cartridges() {
        use crate::bifaci::host_runtime::CartridgeHostRuntime;

        let manifest_a = r#"{"name":"CartridgeA","version":"1.0","channel":"release","registry_url":null,"description":"Cartridge A","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";alpha;out=\"media:void\"","title":"Alpha","aliases": ["alpha"],"args":[]}]}]}"#;
        let manifest_b = r#"{"name":"CartridgeB","version":"1.0","channel":"release","registry_url":null,"description":"Cartridge B","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases": ["identity"],"args":[]},{"urn":"cap:in=\"media:void\";beta;out=\"media:void\"","title":"Beta","aliases": ["beta"],"args":[]}]}]}"#;

        let (ca_read, ca_write, ca_from_rt, ca_to_rt) = create_cartridge_pair();
        let (cb_read, cb_write, cb_from_rt, cb_to_rt) = create_cartridge_pair();
        let (rt_relay_read, rt_relay_write, eng_write, eng_read) = create_relay_pair();

        let ma = manifest_a.as_bytes().to_vec();
        let pa_handle = tokio::spawn(async move {
            let (mut reader, mut writer) =
                cartridge_handshake_with_identity(ca_from_rt, ca_to_rt, &ma).await;
            let req = reader
                .read()
                .await
                .unwrap()
                .expect("Expected REQ for alpha");
            assert_eq!(
                req.cap.as_deref(),
                Some("cap:in=\"media:void\";alpha;out=\"media:void\"")
            );
            loop {
                let f = reader.read().await.unwrap().expect("f");
                if f.frame_type == FrameType::End {
                    break;
                }
            }
            let mut seq = SeqAssigner::new();
            let sid = "a".to_string();
            let mut ss =
                Frame::stream_start(req.id.clone(), sid.clone(), "media:".to_string(), None);
            seq.assign(&mut ss);
            writer.write(&ss).await.unwrap();
            let payload = b"from-alpha".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req.id.clone(), sid.clone(), 0, payload, 0, checksum);
            seq.assign(&mut chunk);
            writer.write(&chunk).await.unwrap();
            let mut se = Frame::stream_end(req.id.clone(), sid, 1);
            seq.assign(&mut se);
            writer.write(&se).await.unwrap();
            let mut end = Frame::end(req.id.clone(), None);
            seq.assign(&mut end);
            writer.write(&end).await.unwrap();
        });

        let mb = manifest_b.as_bytes().to_vec();
        let pb_handle = tokio::spawn(async move {
            let (mut reader, mut writer) =
                cartridge_handshake_with_identity(cb_from_rt, cb_to_rt, &mb).await;
            let req = reader.read().await.unwrap().expect("Expected REQ for beta");
            assert_eq!(
                req.cap.as_deref(),
                Some("cap:in=\"media:void\";beta;out=\"media:void\"")
            );
            loop {
                let f = reader.read().await.unwrap().expect("f");
                if f.frame_type == FrameType::End {
                    break;
                }
            }
            let mut seq = SeqAssigner::new();
            let sid = "b".to_string();
            let mut ss =
                Frame::stream_start(req.id.clone(), sid.clone(), "media:".to_string(), None);
            seq.assign(&mut ss);
            writer.write(&ss).await.unwrap();
            let payload = b"from-beta".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req.id.clone(), sid.clone(), 0, payload, 0, checksum);
            seq.assign(&mut chunk);
            writer.write(&chunk).await.unwrap();
            let mut se = Frame::stream_end(req.id.clone(), sid, 1);
            seq.assign(&mut se);
            writer.write(&se).await.unwrap();
            let mut end = Frame::end(req.id.clone(), None);
            seq.assign(&mut end);
            writer.write(&end).await.unwrap();
        });

        let (ca_read_half, _) = ca_read.into_split();
        let (_, ca_write_half) = ca_write.into_split();
        let (cb_read_half, _) = cb_read.into_split();
        let (_, cb_write_half) = cb_write.into_split();

        let mut runtime = CartridgeHostRuntime::new();
        runtime
            .attach_cartridge(ca_read_half, ca_write_half)
            .await
            .unwrap();
        runtime
            .attach_cartridge(cb_read_half, cb_write_half)
            .await
            .unwrap();

        let (rt_read_half, _) = rt_relay_read.into_split();
        let (_, rt_write_half) = rt_relay_write.into_split();
        let (_, eng_write_half) = eng_write.into_split();
        let (eng_read_half, _) = eng_read.into_split();

        let engine_task = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let mut w = FrameWriter::new(eng_write_half);
            let mut r = FrameReader::new(eng_read_half);
            let xid = MessageId::Uint(1);

            // Send alpha request
            let req_id = MessageId::new_uuid();
            let sid = uuid::Uuid::new_v4().to_string();
            let mut req = Frame::req(
                req_id.clone(),
                "cap:in=\"media:void\";alpha;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let mut ss =
                Frame::stream_start(req_id.clone(), sid.clone(), "media:".to_string(), None);
            ss.routing_id = Some(xid.clone());
            seq.assign(&mut ss);
            w.write(&ss).await.unwrap();
            let payload_a = b"alpha-data".to_vec();
            let checksum = Frame::compute_checksum(&payload_a);
            let mut chunk = Frame::chunk(req_id.clone(), sid.clone(), 0, payload_a, 0, checksum);
            chunk.routing_id = Some(xid.clone());
            seq.assign(&mut chunk);
            w.write(&chunk).await.unwrap();
            let mut se = Frame::stream_end(req_id.clone(), sid, 1);
            se.routing_id = Some(xid.clone());
            seq.assign(&mut se);
            w.write(&se).await.unwrap();
            let mut end = Frame::end(req_id.clone(), None);
            end.routing_id = Some(xid.clone());
            seq.assign(&mut end);
            w.write(&end).await.unwrap();

            let mut payload = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk {
                            payload.extend(f.payload.unwrap_or_default());
                        }
                        if f.frame_type == FrameType::End {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            // Send beta request
            let req_id2 = MessageId::new_uuid();
            let xid2 = MessageId::Uint(2);
            let sid2 = uuid::Uuid::new_v4().to_string();
            let mut req2 = Frame::req(
                req_id2.clone(),
                "cap:in=\"media:void\";beta;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req2.routing_id = Some(xid2.clone());
            seq.assign(&mut req2);
            w.write(&req2).await.unwrap();
            let mut ss2 =
                Frame::stream_start(req_id2.clone(), sid2.clone(), "media:".to_string(), None);
            ss2.routing_id = Some(xid2.clone());
            seq.assign(&mut ss2);
            w.write(&ss2).await.unwrap();
            let payload_b = b"beta-data".to_vec();
            let checksum2 = Frame::compute_checksum(&payload_b);
            let mut chunk2 =
                Frame::chunk(req_id2.clone(), sid2.clone(), 0, payload_b, 0, checksum2);
            chunk2.routing_id = Some(xid2.clone());
            seq.assign(&mut chunk2);
            w.write(&chunk2).await.unwrap();
            let mut se2 = Frame::stream_end(req_id2.clone(), sid2, 1);
            se2.routing_id = Some(xid2.clone());
            seq.assign(&mut se2);
            w.write(&se2).await.unwrap();
            let mut end2 = Frame::end(req_id2.clone(), None);
            end2.routing_id = Some(xid2);
            seq.assign(&mut end2);
            w.write(&end2).await.unwrap();

            let mut payload2 = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk {
                            payload2.extend(f.payload.unwrap_or_default());
                        }
                        if f.frame_type == FrameType::End {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            drop(w);
            (payload, payload2)
        });

        let result = runtime.run(rt_read_half, rt_write_half, || vec![]).await;
        assert!(result.is_ok(), "Runtime should exit cleanly: {:?}", result);

        let (resp_alpha, resp_beta) = engine_task.await.unwrap();
        assert_eq!(
            resp_alpha, b"from-alpha",
            "Alpha cartridge must respond correctly after identity verification"
        );
        assert_eq!(
            resp_beta, b"from-beta",
            "Beta cartridge must respond correctly after identity verification"
        );

        pa_handle.await.unwrap();
        pb_handle.await.unwrap();
    }
}
