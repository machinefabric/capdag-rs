//! A peer request no master serves, held while the switch's provider brings
//! the cartridge that serves it (`peer_hold`, 14.2 §Peer requests no master
//! serves).
//!
//! Every test runs real hosts behind a real switch: a caller cartridge whose
//! handler makes a peer call, and — when the provider brings it — a second
//! host serving the peer cap, attached to the running switch. The engine side
//! drives the switch's inbound stream the way a host's own loop does.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{BufReader, BufWriter};
use tokio::sync::mpsc;

use crate::bifaci::cartridge_runtime::{PeerInvoker, StreamError};
use crate::bifaci::frame::{CancelReason, Frame, FrameType, MessageId};
use crate::bifaci::in_process_host::{
    FrameHandler, InProcessCartridgeHost, InProcessHostIdentity, ResponseWriter,
};
use crate::bifaci::io::{FrameReader, FrameWriter};
use crate::bifaci::local_socket::UnixStream;
use crate::bifaci::peer_hold::{
    PeerHoldBound, PeerProvider, PeerProvision, PeerRefusal, PEER_HOLD_OVERFLOW,
};
use crate::bifaci::relay::RelaySlave;
use crate::bifaci::relay_switch::RelaySwitch;
use crate::cap::caller::CapArgumentValue;
use crate::cap::definition::Cap;
use crate::cap::registry::FabricRegistry;

/// The cap the engine runs: its handler peer-calls [`PROVIDED_CAP`].
const CALLER_CAP: &str = r#"cap:in="media:enc=utf8";peer-caller;out="media:enc=utf8""#;
/// The cap the caller's peer call names, served by no master at first.
const PROVIDED_CAP: &str = r#"cap:in="media:enc=utf8";peer-provided;out="media:enc=utf8""#;
/// A cap nothing asks for.
const UNRELATED_CAP: &str = r#"cap:in="media:enc=utf8";peer-unrelated;out="media:enc=utf8""#;
const TEXT: &str = "media:enc=utf8";

fn cap(urn: &str) -> Cap {
    Cap {
        dims: Vec::new(),
        version: 1,
        urn: crate::CapUrn::from_string(urn).unwrap(),
        title: urn.to_string(),
        cap_description: None,
        documentation: None,
        metadata: std::collections::HashMap::new(),
        aliases: vec![String::new()],
        is_abstract: false,
        args: Vec::new(),
        outputs: Vec::new(),
        metadata_json: None,
        registered_by: None,
        supported_model_types: Vec::new(),
        default_model_spec: None,
    }
}

/// The bytes of a request's argument stream, up to its END.
async fn read_argument(input: &mut mpsc::UnboundedReceiver<Frame>) -> Vec<u8> {
    let mut bytes = Vec::new();
    while let Some(frame) = input.recv().await {
        match frame.frame_type {
            FrameType::Chunk => {
                let payload = frame.payload.as_deref().expect("a chunk carries a payload");
                bytes.extend(crate::bifaci::decode_chunk_payload(payload).expect("a CBOR chunk"));
            }
            FrameType::End => break,
            _ => {}
        }
    }
    bytes
}

/// Peer-calls [`PROVIDED_CAP`] with what it was given, `copies` times over,
/// and answers `ok:<the peer's answer>`, or `err:<CODE>:<message>` with the
/// ERR the peer call ended with.
#[derive(Debug)]
struct Caller {
    copies: usize,
}

#[async_trait]
impl FrameHandler for Caller {
    async fn handle_request(
        &self,
        _cap_urn: &str,
        mut input: mpsc::UnboundedReceiver<Frame>,
        output: ResponseWriter,
        peer: Arc<dyn PeerInvoker>,
        _network: crate::bifaci::policy::NetworkAccess,
    ) {
        let argument = read_argument(&mut input).await.repeat(self.copies);
        let reply = match peer.call_with_bytes(PROVIDED_CAP, &[(TEXT, &argument)]).await {
            Err(e) => format!("call:{e}"),
            Ok(response) => match response.collect_bytes().await {
                Ok(answer) => format!("ok:{}", String::from_utf8_lossy(&answer)),
                Err(StreamError::RemoteError { code, message, .. }) => format!("err:{code}:{message}"),
                Err(e) => format!("stream:{e}"),
            },
        };
        output.emit_response(TEXT, reply.as_bytes());
    }
}

/// Peer-calls [`PROVIDED_CAP`], then cancels the call before any answer, and
/// answers with the ERR the cancelled call ended with.
#[derive(Debug)]
struct CancellingCaller;

#[async_trait]
impl FrameHandler for CancellingCaller {
    async fn handle_request(
        &self,
        _cap_urn: &str,
        mut input: mpsc::UnboundedReceiver<Frame>,
        output: ResponseWriter,
        peer: Arc<dyn PeerInvoker>,
        _network: crate::bifaci::policy::NetworkAccess,
    ) {
        let argument = read_argument(&mut input).await;
        let call = peer.call(PROVIDED_CAP).expect("the peer call starts");
        let arg = call.arg(TEXT);
        arg.start(false, None).expect("its argument starts");
        arg.write(&argument).await.expect("its argument is written");
        arg.close().await.expect("its argument ends");
        let rid = call.request_id.clone();
        let sender = call.sender.clone();
        let mut response = call.finish().await.expect("the call is sent");
        // The caller gives up on the call: a CANCEL for it, from this
        // cartridge, on the connection its peer call went out on.
        sender
            .send(&Frame::cancel(rid, &CancelReason::user(false)))
            .expect("the cancel is sent");
        let reply = match response.recv().await {
            Some(crate::bifaci::cartridge_runtime::PeerResponseItem::Data(Err(StreamError::RemoteError { code, .. }), _)) => {
                format!("err:{code}")
            }
            other => format!("unexpected:{other:?}"),
        };
        output.emit_response(TEXT, reply.as_bytes());
    }
}

/// Answers with what it was given, upper-cased, and counts the requests it
/// served.
#[derive(Debug, Default)]
struct Upper {
    served: AtomicUsize,
}

#[async_trait]
impl FrameHandler for Upper {
    async fn handle_request(
        &self,
        _cap_urn: &str,
        mut input: mpsc::UnboundedReceiver<Frame>,
        output: ResponseWriter,
        _peer: Arc<dyn PeerInvoker>,
        _network: crate::bifaci::policy::NetworkAccess,
    ) {
        let argument = read_argument(&mut input).await;
        self.served.fetch_add(1, Ordering::SeqCst);
        output.emit_response(TEXT, String::from_utf8_lossy(&argument).to_uppercase().as_bytes());
    }
}

/// A host serving `urn` with `handler`, wired as a relay slave: the socket
/// the switch attaches.
fn wire_host(name: &str, urn: &str, handler: Arc<dyn FrameHandler>) -> UnixStream {
    let host = InProcessCartridgeHost::new(
        InProcessHostIdentity::for_test(name),
        vec![(name.to_string(), vec![cap(urn)], handler)],
    );
    let (host_sock, slave_local_sock) = UnixStream::pair().unwrap();
    let (slave_sock, switch_sock) = UnixStream::pair().unwrap();
    let (host_read, host_write) = host_sock.into_split();
    tokio::spawn(async move {
        let _ = host.run(host_read, host_write).await;
    });
    let (slave_local_read, slave_local_write) = slave_local_sock.into_split();
    let slave = RelaySlave::new(slave_local_read, slave_local_write);
    let (slave_read, slave_write) = slave_sock.into_split();
    tokio::spawn(async move {
        let _ = slave
            .run(
                FrameReader::new(BufReader::new(slave_read)),
                FrameWriter::new(BufWriter::new(slave_write)),
                None,
            )
            .await;
    });
    switch_sock
}

/// What the test provider does when asked.
#[derive(Clone)]
enum Bring {
    /// Attach a host serving `urn` after `delay`.
    Host { urn: &'static str, delay: Duration },
    /// Refuse, with this.
    Refuse(PeerRefusal),
}

/// A provider that brings a host the way the command line's brings a
/// cartridge: it attaches a master to the switch it was set on.
struct TestProvider {
    switch: StdMutex<Weak<RelaySwitch>>,
    bring: Bring,
    upper: Arc<Upper>,
    asked: AtomicUsize,
}

fn expiry() -> PeerRefusal {
    PeerRefusal {
        code: "TEST_PROVIDER_TIMEOUT".to_string(),
        class: crate::failure::AttributionClass::Environment,
        message: "testcartridge, which provides peer-provided, was not started in time".to_string(),
    }
}

#[async_trait]
impl PeerProvider for TestProvider {
    async fn provision(&self, cap_urn: &str) -> Result<PeerProvision, PeerRefusal> {
        assert_eq!(cap_urn, PROVIDED_CAP, "the provider is asked for the cap the peer call named");
        self.asked.fetch_add(1, Ordering::SeqCst);
        let (urn, delay) = match &self.bring {
            Bring::Refuse(refusal) => return Err(refusal.clone()),
            Bring::Host { urn, delay } => (*urn, *delay),
        };
        let switch = self.switch.lock().unwrap().clone();
        let upper: Arc<dyn FrameHandler> = self.upper.clone();
        Ok(PeerProvision {
            ready: Box::pin(async move {
                tokio::time::sleep(delay).await;
                let switch = switch.upgrade().expect("the switch outlives its provider's work");
                switch
                    .add_master("provided-host", wire_host("provided", urn, upper))
                    .await
                    .map(|_| ())
                    .map_err(|e| PeerRefusal {
                        code: "TEST_ATTACH_FAILED".to_string(),
                        class: crate::failure::AttributionClass::Internal,
                        message: e.to_string(),
                    })
            }),
            on_expiry: expiry(),
        })
    }
}

const ROOMY: PeerHoldBound = PeerHoldBound {
    frames: 64,
    bytes: 1 << 20,
    deadline: Duration::from_secs(10),
};

/// A switch with one master — the caller, with `handler` — and, when given,
/// `bring` as its peer provider within `bound`.
async fn switch_with(
    handler: Arc<dyn FrameHandler>,
    bring: Option<Bring>,
    bound: PeerHoldBound,
) -> (Arc<RelaySwitch>, Option<Arc<TestProvider>>) {
    let switch = Arc::new(
        RelaySwitch::new(
            vec![("caller-host".to_string(), wire_host("caller", CALLER_CAP, handler))],
            Arc::new(FabricRegistry::new_for_test()),
            crate::bifaci::policy::HostPolicy::new(crate::bifaci::policy::NetworkPolicy::Full),
        )
        .await
        .unwrap(),
    );
    let provider = bring.map(|bring| {
        let provider = Arc::new(TestProvider {
            switch: StdMutex::new(Arc::downgrade(&switch)),
            bring,
            upper: Arc::new(Upper::default()),
            asked: AtomicUsize::new(0),
        });
        switch.set_peer_provider(provider.clone(), bound).unwrap();
        provider
    });
    (switch, provider)
}

/// Run [`CALLER_CAP`] on `input` and return its answer, driving the switch's
/// inbound stream as a host's loop does.
async fn run_caller(switch: &RelaySwitch, input: &str) -> String {
    let rid = MessageId::new_uuid();
    let max_chunk = switch.limits().await.max_chunk;
    let frames = CapArgumentValue::build_request_frames(
        &rid,
        CALLER_CAP,
        &[CapArgumentValue::from_str(TEXT, input)],
        max_chunk,
    );
    for frame in frames {
        switch.send_to_master(frame, None).await.unwrap();
    }
    let mut answer = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        match switch.read_from_masters_timeout(Duration::from_millis(100)).await.unwrap() {
            Some(frame) if frame.id == rid => match frame.frame_type {
                FrameType::Chunk => {
                    let payload = frame.payload.as_deref().unwrap();
                    answer.extend(crate::bifaci::decode_chunk_payload(payload).unwrap());
                }
                FrameType::End => return String::from_utf8(answer).unwrap(),
                FrameType::Err => panic!("the caller itself failed: {:?}", frame.error_message()),
                _ => {}
            },
            _ => {}
        }
    }
    panic!("the caller did not answer within 30 s");
}

// TEST463: A peer call to a cap no master serves is held while the switch's
// provider brings a master that serves it, and is then answered by that
// master, with every argument byte the caller sent while it was held.
#[tokio::test]
async fn test463_held_peer_call_is_answered_once_its_provider_is_brought() {
    let (switch, provider) = switch_with(
        Arc::new(Caller { copies: 1 }),
        Some(Bring::Host { urn: PROVIDED_CAP, delay: Duration::from_millis(300) }),
        ROOMY,
    )
    .await;
    let provider = provider.unwrap();

    assert_eq!(run_caller(&switch, "hello peer").await, "ok:HELLO PEER");
    assert_eq!(provider.asked.load(Ordering::SeqCst), 1);
    assert_eq!(provider.upper.served.load(Ordering::SeqCst), 1);

    // Brought once: the next call is routed to it at once.
    assert_eq!(run_caller(&switch, "again").await, "ok:AGAIN");
    assert_eq!(provider.asked.load(Ordering::SeqCst), 1, "a served cap is not provided again");
}

// TEST464: A provider that cannot bring what serves the cap ends the held
// call with its own ERR — its code and its message, which name what is
// missing and what to do.
#[tokio::test]
async fn test464_refused_provision_ends_the_call_with_the_providers_err() {
    let refusal = PeerRefusal {
        code: "CARTRIDGE_INSTALL_FAILED".to_string(),
        class: crate::failure::AttributionClass::Environment,
        message: "testcartridge provides peer-provided and is not installed; \
                  install it with: capdag cartridge install testcartridge"
            .to_string(),
    };
    let (switch, _provider) =
        switch_with(Arc::new(Caller { copies: 1 }), Some(Bring::Refuse(refusal.clone())), ROOMY).await;

    assert_eq!(
        run_caller(&switch, "hello").await,
        format!("err:{}:{}", refusal.code, refusal.message)
    );
}

// TEST465: A held call waits no longer than its deadline: it ends with the
// provider's own expiry ERR. The provider's work goes on, so what it brought
// late serves the next call.
#[tokio::test]
async fn test465_held_call_ends_at_its_deadline_and_the_late_provider_serves_the_next() {
    let bound = PeerHoldBound { deadline: Duration::from_millis(300), ..ROOMY };
    let (switch, provider) = switch_with(
        Arc::new(Caller { copies: 1 }),
        Some(Bring::Host { urn: PROVIDED_CAP, delay: Duration::from_millis(1500) }),
        bound,
    )
    .await;
    let provider = provider.unwrap();

    let started = std::time::Instant::now();
    let expired = expiry();
    assert_eq!(
        run_caller(&switch, "hello").await,
        format!("err:{}:{}", expired.code, expired.message)
    );
    assert!(
        started.elapsed() < Duration::from_millis(1500),
        "the call ended at its deadline, not when the provider finished"
    );
    assert_eq!(provider.upper.served.load(Ordering::SeqCst), 0, "the expired call was not replayed");

    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(run_caller(&switch, "later").await, "ok:LATER");
    assert_eq!(provider.asked.load(Ordering::SeqCst), 1);
}

// TEST466: A hold keeps no more frames than its bound: a call that sends more
// while it is held ends with PEER_HOLD_OVERFLOW, and is never replayed.
#[tokio::test]
async fn test466_held_call_past_its_frame_bound_ends() {
    // REQ, STREAM_START, CHUNK, STREAM_END and END are five frames.
    let bound = PeerHoldBound { frames: 4, ..ROOMY };
    let (switch, provider) = switch_with(
        Arc::new(Caller { copies: 1 }),
        Some(Bring::Host { urn: PROVIDED_CAP, delay: Duration::from_millis(300) }),
        bound,
    )
    .await;

    let answer = run_caller(&switch, "hello").await;
    assert!(answer.starts_with(&format!("err:{PEER_HOLD_OVERFLOW}:")), "{answer}");
    assert!(answer.contains("more than 4 frames"), "{answer}");
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(provider.unwrap().upper.served.load(Ordering::SeqCst), 0);
}

// TEST467: A hold keeps no more payload bytes than its bound.
#[tokio::test]
async fn test467_held_call_past_its_byte_bound_ends() {
    let bound = PeerHoldBound { bytes: 1024, ..ROOMY };
    let (switch, provider) = switch_with(
        Arc::new(Caller { copies: 400 }),
        Some(Bring::Host { urn: PROVIDED_CAP, delay: Duration::from_millis(300) }),
        bound,
    )
    .await;

    // 400 copies of "hello" is 2000 bytes, over the 1024 a hold keeps.
    let answer = run_caller(&switch, "hello").await;
    assert!(answer.starts_with(&format!("err:{PEER_HOLD_OVERFLOW}:")), "{answer}");
    assert!(answer.contains("1024 bytes"), "{answer}");
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(provider.unwrap().upper.served.load(Ordering::SeqCst), 0);
}

// TEST468: A caller that cancels its held call is answered with its cancel at
// once, and the call is not replayed to the provider brought after.
#[tokio::test]
async fn test468_cancelled_held_call_ends_at_once_and_is_not_replayed() {
    let (switch, provider) = switch_with(
        Arc::new(CancellingCaller),
        Some(Bring::Host { urn: PROVIDED_CAP, delay: Duration::from_millis(800) }),
        ROOMY,
    )
    .await;

    let started = std::time::Instant::now();
    assert_eq!(run_caller(&switch, "hello").await, "err:CANCELLED");
    assert!(started.elapsed() < Duration::from_millis(800), "the cancel was answered before the provider was there");
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(provider.unwrap().upper.served.load(Ordering::SeqCst), 0);
}

// TEST469: A provider whose master does not serve the cap leaves the call
// held until its deadline, which ends it with the provider's expiry ERR; the
// provider is asked once.
#[tokio::test]
async fn test469_provider_that_serves_something_else_ends_the_call_at_its_deadline() {
    let bound = PeerHoldBound { deadline: Duration::from_millis(800), ..ROOMY };
    let (switch, provider) = switch_with(
        Arc::new(Caller { copies: 1 }),
        Some(Bring::Host { urn: UNRELATED_CAP, delay: Duration::from_millis(50) }),
        bound,
    )
    .await;
    let expired = expiry();
    assert_eq!(
        run_caller(&switch, "hello").await,
        format!("err:{}:{}", expired.code, expired.message)
    );
    assert_eq!(provider.unwrap().asked.load(Ordering::SeqCst), 1);
}

// TEST470: A switch without a provider refuses a peer call to a cap no master
// serves at once, with NO_HANDLER naming the cap.
#[tokio::test]
async fn test470_without_a_provider_the_call_is_refused_with_no_handler() {
    let (switch, _) = switch_with(Arc::new(Caller { copies: 1 }), None, ROOMY).await;
    let answer = run_caller(&switch, "hello").await;
    assert!(answer.starts_with("err:NO_HANDLER:No handler found for cap:"), "{answer}");
    assert!(answer.contains("peer-provided"), "{answer}");
}

// TEST471: A switch takes one peer provider.
#[tokio::test]
async fn test471_a_second_peer_provider_is_refused() {
    let (switch, provider) = switch_with(
        Arc::new(Caller { copies: 1 }),
        Some(Bring::Refuse(expiry())),
        ROOMY,
    )
    .await;
    let again = switch.set_peer_provider(provider.unwrap(), ROOMY);
    assert!(again.unwrap_err().to_string().contains("already has a peer provider"));
}
