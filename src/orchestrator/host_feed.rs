//! Host resolution of a cartridge's own live family (13.2 §Reference Media).
//!
//! A live source feeding a map directly is opened by the HOST: the host
//! iterates the items, so it holds the feed. A device family the host opens
//! itself (`crate::capture`). A family a cartridge resolves, the host cannot:
//! it asks the cartridge. The family's fabric definition names the cap that
//! opens it (`metadata.opened_by`); the host runs that cap as a one-step
//! segment fed the live reference, and takes each item of its output as an
//! item of the feed while the map runs.
//!
//! The request is an ordinary one, fed a live reference, so the executor
//! records it as feed-bearing through the runtime's flow observer: a stop is
//! a CloseStream to it, the cartridge's feed ends, the cap's output ends, and
//! the host's feed drains — exactly the stop of a cartridge-resolved feed
//! anywhere else in a machine. The host side is bridged like a device
//! ([`bridge_feed`]), so the selector's ring, overrun policy and stop
//! conditions hold on this side too.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::bifaci::cartridge_runtime::{try_decode_sequence_item, StreamMeta};
use crate::bifaci::live_feed::{bridge_feed, LiveFeedItem, LiveFeedSelector, LiveFeedSink, OpenedFeed};
use crate::machine::StepPolicy;
use crate::orchestrator::execute_plan::{EngineRuntime, WriterResult};
use crate::orchestrator::executor::{run_dag_on_context, ExecutionContext, RunCancel};
use crate::orchestrator::stream_io::{IncrementalWriter, StreamIoError};
use crate::orchestrator::types::{ResolvedGraph, ResolvedStep, StepInput, StepSource};
use crate::planner::StepToken;
use crate::{Cap, ExecutionError};

/// The node the live reference is given at.
const SOURCE: &str = "live_source";
/// The node the opening cap's output — the feed's items — is taken at.
const FEED: &str = "live_feed";

/// Open a live reference of a cartridge's family by asking the cartridge:
/// run `opener` (the family's `metadata.opened_by` cap) fed the reference, and
/// deliver its output items as the feed. `cancel` is the run's: a failed run
/// cancels the request with it.
pub(crate) fn open_through_cartridge(
    runtime: Arc<dyn EngineRuntime>,
    reference_urn: &str,
    selector_bytes: Vec<u8>,
    opener: Cap,
    cancel: RunCancel,
) -> Result<OpenedFeed, ExecutionError> {
    let selector = LiveFeedSelector::parse(&selector_bytes).map_err(|e| {
        ExecutionError::HostError(format!("live source '{reference_urn}': {e}"))
    })?;
    let reference_urn = reference_urn.to_string();
    bridge_feed(selector, Arc::new(std::sync::atomic::AtomicU64::new(0)), move |_selector, sink| {
        let graph = opening_graph(&reference_urn, &opener);
        tokio::spawn(async move {
            let failure = sink.clone();
            if let Err(e) = run_opening(runtime, graph, reference_urn.clone(), selector_bytes, sink, cancel).await {
                failure.fail(format!(
                    "the cartridge that opens '{reference_urn}' failed while feeding the map: {e}"
                ));
            }
        });
        Ok(None)
    })
    .map_err(|e| ExecutionError::HostError(e.to_string()))
}

/// The one-step graph: the live reference into the opening cap's main input,
/// its main output taken as the feed.
fn opening_graph(reference_urn: &str, opener: &Cap) -> ResolvedGraph {
    let out_media = opener.urn.out_spec().to_string();
    ResolvedGraph {
        nodes: HashMap::from([
            (SOURCE.to_string(), reference_urn.to_string()),
            (FEED.to_string(), out_media.clone()),
        ]),
        steps: vec![ResolvedStep {
            token_id: StepToken::mint(),
            node: FEED.to_string(),
            cap_urn: opener.urn.to_string(),
            cap: opener.clone(),
            inputs: vec![StepInput {
                stream_urn: opener.urn.in_spec().to_string(),
                source: StepSource::Node(SOURCE.to_string()),
            }],
            outputs: Vec::new(),
            out_media,
            policy: StepPolicy::default(),
        }],
        graph_name: Some(format!("open {reference_urn}")),
    }
}

async fn run_opening(
    runtime: Arc<dyn EngineRuntime>,
    graph: ResolvedGraph,
    reference_urn: String,
    selector_bytes: Vec<u8>,
    sink: LiveFeedSink,
    cancel: RunCancel,
) -> Result<(), ExecutionError> {
    let activity_timeout_secs = runtime.activity_timeout_secs(&graph).await?;
    let switch = runtime.segment_switch(&graph).await?;
    let mut ctx = ExecutionContext::from_switch(switch).await?;
    ctx.set_node_is_sequence(SOURCE.to_string(), false);
    ctx.set_node_live_reference(SOURCE.to_string(), reference_urn);
    ctx.set_node_data(SOURCE.to_string(), selector_bytes);

    let cap_urn = graph.steps[0].cap_urn.clone();
    let slot = Arc::new(Mutex::new(Some(sink)));
    let factory = move |_node: &str, _body: Option<crate::orchestrator::execute_plan::ForEachBodyCoordinate>| {
        let sink = slot
            .lock()
            .expect("the feed's sink slot")
            .take()
            .expect("the opening segment persists one sink, so one writer is made");
        Box::new(FeedWriter::new(sink, cap_urn.clone())) as Box<dyn IncrementalWriter>
    };
    run_dag_on_context(
        &mut ctx,
        &graph,
        &HashMap::new(),
        None,
        None,
        None,
        None,
        Some(&factory),
        None,
        &HashSet::from([FEED.to_string()]),
        activity_timeout_secs,
        runtime.flow_observer(),
        None,
        None,
        Some(&cancel),
    )
    .await
    .map(|_| ())
}

/// Takes the opening cap's output and pushes each item into the host's feed.
/// An item is raw bytes, as a device's items are, carrying the feed's own
/// `pts_us` and `capture_ts_us` on its first fragment: an opening cap passes
/// its feed's items on as they came.
struct FeedWriter {
    sink: LiveFeedSink,
    cap_urn: String,
    buf: Vec<u8>,
    item_meta: Option<StreamMeta>,
}

impl FeedWriter {
    fn new(sink: LiveFeedSink, cap_urn: String) -> Self {
        Self { sink, cap_urn, buf: Vec::new(), item_meta: None }
    }

    fn contract(&self, what: String) -> StreamIoError {
        StreamIoError::Protocol(format!("cap '{}' opening a live feed {what}", self.cap_urn))
    }

    fn timestamp(&self, meta: &StreamMeta, key: &str) -> Result<u64, StreamIoError> {
        match meta.get(key) {
            Some(ciborium::Value::Integer(value)) => u64::try_from(*value)
                .map_err(|_| self.contract(format!("gave an item whose '{key}' is negative"))),
            _ => Err(self.contract(format!("gave an item without its feed's '{key}'"))),
        }
    }
}

#[async_trait]
impl IncrementalWriter for FeedWriter {
    async fn on_stream_start(
        &mut self,
        is_sequence: Option<bool>,
        _media_urn: &str,
        _meta: Option<StreamMeta>,
        _stream_id: Option<String>,
    ) -> Result<(), StreamIoError> {
        if is_sequence != Some(true) {
            return Err(self.contract("gives no sequence: a feed is a sequence of items".to_string()));
        }
        Ok(())
    }

    async fn on_chunk_payload(&mut self, payload: &[u8], meta: Option<StreamMeta>) -> Result<(), StreamIoError> {
        if meta.is_some() {
            self.item_meta = meta;
        }
        self.buf.extend_from_slice(payload);
        while let Some((value, consumed)) =
            try_decode_sequence_item(&self.buf).map_err(|e| self.contract(format!("gave bytes that are no CBOR item: {e}")))?
        {
            self.buf.drain(..consumed);
            let meta = self
                .item_meta
                .take()
                .ok_or_else(|| self.contract("gave an item without its feed's metadata".to_string()))?;
            let (pts_us, capture_ts_us) = (self.timestamp(&meta, "pts_us")?, self.timestamp(&meta, "capture_ts_us")?);
            let ciborium::Value::Bytes(payload) = value else {
                return Err(self.contract(format!("gave a non-bytes item ({value:?}): a feed's items are raw bytes")));
            };
            // A closed feed (stopped, or its stop condition reached) takes no
            // more: the rest of the output drains as the request ends.
            self.sink.push(LiveFeedItem { payload, pts_us, capture_ts_us });
        }
        Ok(())
    }

    async fn on_stream_end(&mut self) -> Result<(), StreamIoError> {
        if !self.buf.is_empty() {
            return Err(self.contract(format!("ended inside an item ({} bytes of it)", self.buf.len())));
        }
        self.sink.finish();
        Ok(())
    }

    fn finish(self: Box<Self>) -> WriterResult {
        WriterResult {
            is_sequence: true,
            media_urn: String::new(),
            saved_paths: Vec::new(),
            total_bytes: 0,
            stream_meta: None,
            item_metas: Vec::new(),
        }
    }

    fn abort(self: Box<Self>) {
        self.sink.fail(format!("cap '{}' opening a live feed did not end in success", self.cap_urn));
    }
}
