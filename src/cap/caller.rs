//! Argument/result/stdin-source data types used by in-process cap handlers.
//!
//! Previously this file also housed a `CapCaller` + `CapSet` trait stack for
//! an in-process direct-dispatch execution model. That stack is gone: cap
//! invocation now goes through the bifaci relay (`RelaySwitch::execute_cap`)
//! for out-of-process cartridges and through in-process `FrameHandler`
//! implementations for any engine-built cartridges. The remaining types below
//! are the argument/return shape both paths share.

use crate::bifaci::frame::{Frame, MessageId};
use anyhow::Result;

/// Source for stdin data - either raw bytes or a file reference.
///
/// For cartridges (via gRPC/XPC), using FileReference avoids the 4MB gRPC limit
/// by letting the Swift/XPC side read the file locally instead of sending
/// bytes over the wire.
#[derive(Debug, Clone)]
pub enum StdinSource {
    /// Raw byte data - used for cartridges (in-process) or small inline data
    Data(Vec<u8>),
    /// File reference - used for cartridges to read files locally on Mac side
    FileReference {
        tracked_file_id: String,
        original_path: String,
        security_bookmark: Vec<u8>,
        media_urn: String,
    },
}

/// Unified argument type - arguments are identified by media_urn.
/// The cap definition's sources specify how to extract values (stdin, position, cli_flag).
#[derive(Debug, Clone)]
pub struct CapArgumentValue {
    /// Semantic identifier, e.g., "media:enc=utf-8;model-spec"
    pub media_urn: String,
    /// Value bytes (UTF-8 for text, raw for binary)
    pub value: Vec<u8>,
}

impl CapArgumentValue {
    /// Create a new CapArgumentValue
    pub fn new(media_urn: impl Into<String>, value: Vec<u8>) -> Self {
        Self {
            media_urn: media_urn.into(),
            value,
        }
    }

    /// Create a new CapArgumentValue from a string value
    pub fn from_str(media_urn: impl Into<String>, value: &str) -> Self {
        Self {
            media_urn: media_urn.into(),
            value: value.as_bytes().to_vec(),
        }
    }

    /// Get the value as a UTF-8 string (may fail for binary data)
    pub fn value_as_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(&self.value)
    }

    /// Build the sequence of frames for a cap request with streaming arguments.
    ///
    /// Produces: REQ(empty payload) → for each arg: STREAM_START + CHUNK(s) + STREAM_END → END.
    /// The caller sends these frames one by one via `send_to_master()`.
    pub fn build_request_frames(
        rid: &MessageId,
        cap_urn: &str,
        args: &[Self],
        max_chunk: usize,
    ) -> Vec<Frame> {
        let mut frames = Vec::new();

        // REQ with empty payload (arguments follow as streams)
        frames.push(Frame::req(rid.clone(), cap_urn, vec![], "application/cbor"));

        // Each argument as a named stream
        for (arg_idx, arg) in args.iter().enumerate() {
            let stream_id = format!("arg{}", arg_idx);

            // STREAM_START
            frames.push(Frame::stream_start(
                rid.clone(),
                stream_id.clone(),
                arg.media_urn.clone(),
                None,
            ));

            // CHUNKs — payload must be CBOR-encoded (matching StreamEmitter::send_chunk)
            let data = &arg.value;
            if data.is_empty() {
                let mut cbor_payload = Vec::new();
                ciborium::into_writer(&ciborium::Value::Bytes(Vec::new()), &mut cbor_payload)
                    .expect("BUG: failed to CBOR-encode empty bytes");
                let checksum = Frame::compute_checksum(&cbor_payload);
                frames.push(Frame::chunk(
                    rid.clone(),
                    stream_id.clone(),
                    0,
                    cbor_payload,
                    0,
                    checksum,
                ));
            } else {
                for (i, chunk_data) in data.chunks(max_chunk).enumerate() {
                    let mut cbor_payload = Vec::new();
                    ciborium::into_writer(
                        &ciborium::Value::Bytes(chunk_data.to_vec()),
                        &mut cbor_payload,
                    )
                    .expect("BUG: failed to CBOR-encode chunk");
                    let checksum = Frame::compute_checksum(&cbor_payload);
                    frames.push(Frame::chunk(
                        rid.clone(),
                        stream_id.clone(),
                        0, // seq assigned at output stage
                        cbor_payload,
                        i as u64,
                        checksum,
                    ));
                }
            }

            // STREAM_END
            let chunk_count = if data.is_empty() {
                1
            } else {
                (data.len() + max_chunk - 1) / max_chunk
            } as u64;
            frames.push(Frame::stream_end(rid.clone(), stream_id, chunk_count));
        }

        // END
        frames.push(Frame::end(rid.clone(), None));

        frames
    }
}

/// Result from a cap execution.
///
/// Scalar outputs carry raw materialized bytes (e.g. UTF-8 text, raw binary).
/// The bifaci transport wraps these in `Bytes()` chunks via `emit_response`.
///
/// List outputs carry individual CBOR values, one per list item. The bifaci
/// transport sends each as a separate chunk via `emit_list_response`, producing
/// an RFC 8742 CBOR sequence that `execute_fanin`'s list path stores directly.
#[derive(Debug, Clone)]
pub enum CapResult {
    /// Raw materialized bytes (scalar output).
    Scalar(Vec<u8>),
    /// Individual CBOR values (list output). Each value represents one list item.
    List(Vec<ciborium::Value>),
    /// No output (void cap).
    Empty,
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST156: Test creating StdinSource Data variant with byte vector
    #[test]
    fn test156_stdin_source_data_creation() {
        let data = vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]; // "Hello"
        let source = StdinSource::Data(data.clone());

        match source {
            StdinSource::Data(d) => assert_eq!(d, data),
            StdinSource::FileReference { .. } => panic!("Expected Data variant"),
        }
    }

    // TEST157: Test creating StdinSource FileReference variant with all required fields
    #[test]
    fn test157_stdin_source_file_reference_creation() {
        let tracked_file_id = "tracked-file-123".to_string();
        let original_path = "/path/to/original.pdf".to_string();
        let security_bookmark = vec![0x62, 0x6f, 0x6f, 0x6b]; // "book"
        let media_urn = "media:document;ext=pdf".to_string();

        let source = StdinSource::FileReference {
            tracked_file_id: tracked_file_id.clone(),
            original_path: original_path.clone(),
            security_bookmark: security_bookmark.clone(),
            media_urn: media_urn.clone(),
        };

        match source {
            StdinSource::FileReference {
                tracked_file_id: tid,
                original_path: op,
                security_bookmark: sb,
                media_urn: mu,
            } => {
                assert_eq!(tid, tracked_file_id);
                assert_eq!(op, original_path);
                assert_eq!(sb, security_bookmark);
                assert_eq!(mu, media_urn);
            }
            StdinSource::Data(_) => panic!("Expected FileReference variant"),
        }
    }

    // TEST158: Test StdinSource Data with empty vector stores and retrieves correctly
    #[test]
    fn test158_stdin_source_empty_data() {
        let source = StdinSource::Data(vec![]);

        match source {
            StdinSource::Data(d) => assert!(d.is_empty()),
            StdinSource::FileReference { .. } => panic!("Expected Data variant"),
        }
    }

    // TEST159: Test StdinSource Data with binary content like PNG header bytes
    #[test]
    fn test159_stdin_source_binary_content() {
        // PNG header bytes
        let png_header = vec![0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
        let source = StdinSource::Data(png_header.clone());

        match source {
            StdinSource::Data(d) => {
                assert_eq!(d.len(), 8);
                assert_eq!(d[0], 0x89);
                assert_eq!(d[1], 0x50); // 'P'
                assert_eq!(d, png_header);
            }
            StdinSource::FileReference { .. } => panic!("Expected Data variant"),
        }
    }

    // TEST160: Test StdinSource Data clone creates independent copy with same data
    #[test]
    fn test160_stdin_source_clone() {
        let data = vec![1, 2, 3, 4, 5];
        let source = StdinSource::Data(data.clone());
        let cloned = source.clone();

        match (source, cloned) {
            (StdinSource::Data(d1), StdinSource::Data(d2)) => assert_eq!(d1, d2),
            _ => panic!("Expected both to be Data variants"),
        }
    }

    // TEST161: Test StdinSource FileReference clone creates independent copy with same fields
    #[test]
    fn test161_stdin_source_file_reference_clone() {
        let source = StdinSource::FileReference {
            tracked_file_id: "test-id".to_string(),
            original_path: "/test/path.pdf".to_string(),
            security_bookmark: vec![1, 2, 3],
            media_urn: "media:document;ext=pdf".to_string(),
        };
        let cloned = source.clone();

        match (source, cloned) {
            (
                StdinSource::FileReference {
                    tracked_file_id: tid1,
                    original_path: op1,
                    security_bookmark: sb1,
                    media_urn: mu1,
                },
                StdinSource::FileReference {
                    tracked_file_id: tid2,
                    original_path: op2,
                    security_bookmark: sb2,
                    media_urn: mu2,
                },
            ) => {
                assert_eq!(tid1, tid2);
                assert_eq!(op1, op2);
                assert_eq!(sb1, sb2);
                assert_eq!(mu1, mu2);
            }
            _ => panic!("Expected both to be FileReference variants"),
        }
    }

    // TEST162: Test StdinSource Debug format displays variant type and relevant fields
    #[test]
    fn test162_stdin_source_debug() {
        let data_source = StdinSource::Data(vec![1, 2, 3]);
        let debug_str = format!("{:?}", data_source);
        assert!(debug_str.contains("Data"));

        let file_source = StdinSource::FileReference {
            tracked_file_id: "test-id".to_string(),
            original_path: "/test/path.pdf".to_string(),
            security_bookmark: vec![],
            media_urn: "media:document;ext=pdf".to_string(),
        };
        let debug_str = format!("{:?}", file_source);
        assert!(debug_str.contains("FileReference"));
        assert!(debug_str.contains("test-id"));
        assert!(debug_str.contains("/test/path.pdf"));
    }

    // TEST274: Test CapArgumentValue::new stores media_urn and raw byte value
    #[test]
    fn test274_cap_argument_value_new() {
        let arg = CapArgumentValue::new("media:enc=utf-8;model-spec", b"gpt-4".to_vec());
        assert_eq!(arg.media_urn, "media:enc=utf-8;model-spec");
        assert_eq!(arg.value, b"gpt-4");
    }

    // TEST275: Test CapArgumentValue::from_str converts string to UTF-8 bytes
    #[test]
    fn test275_cap_argument_value_from_str() {
        let arg = CapArgumentValue::from_str("media:enc=utf-8;string", "hello world");
        assert_eq!(arg.media_urn, "media:enc=utf-8;string");
        assert_eq!(arg.value, b"hello world");
    }

    // TEST276: Test CapArgumentValue::value_as_str succeeds for UTF-8 data
    #[test]
    fn test276_cap_argument_value_as_str_valid() {
        let arg = CapArgumentValue::from_str("media:string", "test");
        assert_eq!(arg.value_as_str().unwrap(), "test");
    }

    // TEST277: Test CapArgumentValue::value_as_str fails for non-UTF-8 binary data
    #[test]
    fn test277_cap_argument_value_as_str_invalid_utf8() {
        let arg = CapArgumentValue::new("media:document;ext=pdf", vec![0xFF, 0xFE, 0x80]);
        assert!(arg.value_as_str().is_err(), "non-UTF-8 data must fail");
    }

    // TEST278: Test CapArgumentValue::new with empty value stores empty vec
    #[test]
    fn test278_cap_argument_value_empty() {
        let arg = CapArgumentValue::new("media:void", vec![]);
        assert!(arg.value.is_empty());
        assert_eq!(arg.value_as_str().unwrap(), "");
    }

    // TEST279: Test CapArgumentValue Clone produces independent copy with same data
    #[test]
    fn test279_cap_argument_value_clone() {
        let arg = CapArgumentValue::new("media:test", b"data".to_vec());
        let cloned = arg.clone();
        assert_eq!(arg.media_urn, cloned.media_urn);
        assert_eq!(arg.value, cloned.value);
    }

    // TEST280: Test CapArgumentValue Debug format includes media_urn and value
    #[test]
    fn test280_cap_argument_value_debug() {
        let arg = CapArgumentValue::from_str("media:test", "val");
        let debug = format!("{:?}", arg);
        assert!(debug.contains("media:test"), "debug must include media_urn");
    }

    // TEST281: Test CapArgumentValue::new accepts Into<String> for media_urn (String and &str)
    #[test]
    fn test281_cap_argument_value_into_string() {
        let s = String::from("media:owned");
        let arg1 = CapArgumentValue::new(s, vec![]);
        assert_eq!(arg1.media_urn, "media:owned");

        let arg2 = CapArgumentValue::new("media:borrowed", vec![]);
        assert_eq!(arg2.media_urn, "media:borrowed");
    }

    // TEST282: Test CapArgumentValue::from_str with Unicode string preserves all characters
    #[test]
    fn test282_cap_argument_value_unicode() {
        let arg = CapArgumentValue::from_str("media:string", "hello 世界 🌍");
        assert_eq!(arg.value_as_str().unwrap(), "hello 世界 🌍");
    }

    // TEST283: Test CapArgumentValue with large binary payload preserves all bytes
    #[test]
    fn test283_cap_argument_value_large_binary() {
        let data: Vec<u8> = (0u8..=255).cycle().take(10000).collect();
        let arg = CapArgumentValue::new("media:document;ext=pdf", data.clone());
        assert_eq!(arg.value.len(), 10000);
        assert_eq!(arg.value, data);
    }

    // TEST675: build_request_frames with full media URN preserves it in STREAM_START frame
    #[test]
    fn test675_build_request_frames_preserves_media_urn_in_stream_start() {
        use crate::bifaci::frame::FrameType;
        use crate::MessageId;

        let full_urn = "media:fmt=json;llm-generation-request;record";
        let arg = CapArgumentValue::new(full_urn, b"{\"prompt\":\"test\"}".to_vec());
        let rid = MessageId::new_uuid();
        let frames = CapArgumentValue::build_request_frames(&rid, "cap:test", &[arg], 32768);

        // Find the STREAM_START frame
        let stream_start = frames
            .iter()
            .find(|f| f.frame_type == FrameType::StreamStart)
            .expect("Must have STREAM_START frame");

        assert_eq!(
            stream_start.media_urn.as_deref(),
            Some(full_urn),
            "STREAM_START must carry the exact media URN from CapArgumentValue"
        );
    }

    // TEST676: Full round-trip: build_request_frames → extract streams → find_stream succeeds
    #[test]
    fn test676_build_request_frames_round_trip_find_stream_succeeds() {
        use crate::bifaci::frame::FrameType;
        use crate::{find_stream, MessageId};

        let full_urn = "media:fmt=json;llm-generation-request;record";
        let payload = b"{\"prompt\":\"hello\",\"model_spec\":\"test\"}";
        let arg = CapArgumentValue::new(full_urn, payload.to_vec());
        let rid = MessageId::new_uuid();
        let frames = CapArgumentValue::build_request_frames(&rid, "cap:test", &[arg], 32768);

        // Simulate cartridge-side: extract streams from frames (like collect_streams does)
        let mut streams: Vec<(
            String,
            Vec<u8>,
            Option<std::collections::BTreeMap<String, ciborium::Value>>,
        )> = Vec::new();
        let mut active: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

        for frame in &frames {
            match frame.frame_type {
                FrameType::StreamStart => {
                    let sid = frame.stream_id.clone().unwrap_or_default();
                    let media = frame.media_urn.clone().unwrap_or_default();
                    let idx = streams.len();
                    streams.push((media, Vec::new(), frame.meta.clone()));
                    active.insert(sid, idx);
                }
                FrameType::Chunk => {
                    let sid = frame.stream_id.clone().unwrap_or_default();
                    if let Some(&idx) = active.get(&sid) {
                        if let Some(ref p) = frame.payload {
                            let value: ciborium::Value = ciborium::from_reader(&p[..])
                                .expect("CHUNK payload must be valid CBOR");
                            match value {
                                ciborium::Value::Bytes(b) => streams[idx].1.extend_from_slice(&b),
                                ciborium::Value::Text(s) => {
                                    streams[idx].1.extend_from_slice(s.as_bytes())
                                }
                                other => panic!("Unexpected CBOR type: {:?}", other),
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        // Now find_stream should succeed with the full URN
        let found = find_stream(&streams, full_urn);
        assert!(
            found.is_some(),
            "find_stream must find the stream by full media URN"
        );
        assert_eq!(
            found.unwrap(),
            payload.as_slice(),
            "Round-tripped bytes must match original"
        );
    }

    // TEST677: build_request_frames with BASE URN → find_stream with FULL URN FAILS
    // This documents the root cause of the cartridge_client.rs bug:
    // sender used "media:llm-generation-request" (base), receiver looked for
    // "media:fmt=json;llm-generation-request;record" (full). is_equivalent requires
    // exact tag set match, so base != full.
    #[test]
    fn test677_base_urn_does_not_match_full_urn_in_find_stream() {
        use crate::bifaci::frame::FrameType;
        use crate::{find_stream, MessageId};

        // Sender uses BASE URN (the bug)
        let base_urn = "media:llm-generation-request";
        let full_urn = "media:fmt=json;llm-generation-request;record";
        let arg = CapArgumentValue::new(base_urn, b"{}".to_vec());
        let rid = MessageId::new_uuid();
        let frames = CapArgumentValue::build_request_frames(&rid, "cap:test", &[arg], 32768);

        // Extract streams (same as above)
        let mut streams: Vec<(
            String,
            Vec<u8>,
            Option<std::collections::BTreeMap<String, ciborium::Value>>,
        )> = Vec::new();
        let mut active: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

        for frame in &frames {
            match frame.frame_type {
                FrameType::StreamStart => {
                    let sid = frame.stream_id.clone().unwrap_or_default();
                    let media = frame.media_urn.clone().unwrap_or_default();
                    let idx = streams.len();
                    streams.push((media, Vec::new(), frame.meta.clone()));
                    active.insert(sid, idx);
                }
                FrameType::Chunk => {
                    let sid = frame.stream_id.clone().unwrap_or_default();
                    if let Some(&idx) = active.get(&sid) {
                        if let Some(ref p) = frame.payload {
                            let value: ciborium::Value = ciborium::from_reader(&p[..]).unwrap();
                            match value {
                                ciborium::Value::Bytes(b) => streams[idx].1.extend_from_slice(&b),
                                _ => {}
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        // STREAM_START carries the base URN
        assert_eq!(streams[0].0, base_urn);

        // find_stream with FULL URN must FAIL — base URN is not equivalent to full URN
        let found = find_stream(&streams, full_urn);
        assert!(
            found.is_none(),
            "Base URN '{}' must NOT match full URN '{}' — is_equivalent requires exact tag set",
            base_urn,
            full_urn
        );
    }
}
