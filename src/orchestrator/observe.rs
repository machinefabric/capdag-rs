//! Watching runs: every signal [`execute_plan`](super::execute_plan) gives a
//! host, as one ordered stream.
//!
//! [`RunCallbacks`] are plain closures, called on whatever task the executor
//! is on. A host that draws runs — or prints them — wants the opposite: one
//! place where every signal of every run it started arrives, in the order the
//! executor gave them, tagged with the run it is of. A [`RunObserver`] makes
//! the callbacks of each run push [`RunEvent`]s onto one channel; the host
//! reads them from the receiver at its own pace.
//!
//! The channel is unbounded: the executor never waits on whoever watches, and
//! nothing a run says is dropped. A watcher that goes away while a run it
//! started still speaks is an illegal state, never silently absorbed: the
//! callbacks that can fail fail the run with it, and the ones that cannot
//! mark the observer [lost](RunObserver::lost), which the host checks when
//! the run returns.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::orchestrator::execute_plan::{ForEachItemSnapshot, RunCallbacks, StepReport};
use crate::orchestrator::executor::ExecutionError;
use crate::orchestrator::stream_io::PipelineLogRecord;
use crate::planner::BodyOutcome;

/// Which run a signal is of: the run of a machine (a machine given several
/// paths for its one input runs once per path) and the strand of it (the
/// disconnected parts of a machine run side by side, each its own plan).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RunKey {
    pub run: usize,
    pub strand: usize,
}

impl RunKey {
    /// The scope the run's protocol flow is recorded under
    /// ([`RunFlowRegistry`](super::RunFlowRegistry)).
    pub fn flow_scope(&self) -> String {
        format!("run-{}/strand-{}", self.run, self.strand)
    }
}

/// One signal of a running plan, exactly as its callback gave it.
#[derive(Debug, Clone)]
pub enum RunSignal {
    /// The run's overall progress: never goes down.
    Progress {
        progress: f32,
        cap_urn: String,
        message: String,
    },
    /// One step's own progress — a map's aggregate over its runs, under the
    /// map's token.
    StepProgress {
        token_id: String,
        cap_urn: String,
        progress: f32,
    },
    /// A diagnostic: a log, a status, a step waiting on a pool.
    Log(PipelineLogRecord),
    /// The running set of the outcomes of map bodies so far.
    BodyOutcomes(Vec<BodyOutcome>),
    /// The next items a map's body is about to run on, append-only.
    ForEachItems {
        map_token: String,
        items: Vec<ForEachItemSnapshot>,
    },
    /// What became of a step that did not simply run once and complete.
    StepReport(StepReport),
}

/// A signal and the run it is of.
#[derive(Debug, Clone)]
pub struct RunEvent {
    pub key: RunKey,
    pub signal: RunSignal,
}

/// Makes the callbacks of every run a host starts speak onto one channel.
#[derive(Clone)]
pub struct RunObserver {
    sender: mpsc::UnboundedSender<RunEvent>,
    lost: Arc<AtomicBool>,
}

impl RunObserver {
    /// An observer and the receiver its runs' events arrive on.
    pub fn new() -> (Self, mpsc::UnboundedReceiver<RunEvent>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (
            Self {
                sender,
                lost: Arc::new(AtomicBool::new(false)),
            },
            receiver,
        )
    }

    /// Whether a signal could not be delivered because the receiver was
    /// gone. A host that dropped its receiver while a run still spoke has
    /// lost what that run said, and must treat the run as unobserved.
    pub fn lost(&self) -> bool {
        self.lost.load(Ordering::SeqCst)
    }

    /// The callbacks for run `key`: every one of them reports onto this
    /// observer's channel.
    pub fn callbacks(&self, key: RunKey) -> RunCallbacks {
        // Deliver one signal. A gone receiver marks the observer lost and
        // says so to the callbacks that can fail.
        let deliver = {
            let sender = self.sender.clone();
            let lost = self.lost.clone();
            Arc::new(move |signal: RunSignal| -> Result<(), ExecutionError> {
                sender.send(RunEvent { key, signal }).map_err(|_| {
                    lost.store(true, Ordering::SeqCst);
                    ExecutionError::HostError(format!(
                        "the observer of run {} strand {} is gone while the run still speaks",
                        key.run, key.strand
                    ))
                })
            })
        };

        let progress = {
            let deliver = deliver.clone();
            Arc::new(move |progress: f32, cap_urn: &str, message: &str| {
                // The failure is recorded as `lost`; a progress callback has
                // no way to fail the run itself.
                let _ = deliver(RunSignal::Progress {
                    progress,
                    cap_urn: cap_urn.to_string(),
                    message: message.to_string(),
                });
            }) as crate::CapProgressFn
        };
        let step_progress = {
            let deliver = deliver.clone();
            Arc::new(move |progress: f32, cap_urn: &str, token_id: &str| {
                let _ = deliver(RunSignal::StepProgress {
                    token_id: token_id.to_string(),
                    cap_urn: cap_urn.to_string(),
                    progress,
                });
            }) as crate::CapStepProgressFn
        };
        let log = {
            let deliver = deliver.clone();
            Arc::new(move |record: PipelineLogRecord| {
                let _ = deliver(RunSignal::Log(record));
            }) as crate::PipelineLogFn
        };
        let body_outcome = {
            let deliver = deliver.clone();
            Arc::new(move |outcomes: &[BodyOutcome]| {
                deliver(RunSignal::BodyOutcomes(outcomes.to_vec()))
            }) as super::BodyOutcomeFn
        };
        let foreach_items = {
            let deliver = deliver.clone();
            Arc::new(move |map_token: &str, items: &[ForEachItemSnapshot]| {
                deliver(RunSignal::ForEachItems {
                    map_token: map_token.to_string(),
                    items: items.to_vec(),
                })
            }) as super::ForEachItemsFn
        };
        let step_report = {
            let deliver = deliver.clone();
            Arc::new(move |report: &StepReport| {
                let _ = deliver(RunSignal::StepReport(report.clone()));
            }) as super::StepReportFn
        };

        RunCallbacks {
            progress: Some(progress),
            step_progress: Some(step_progress),
            log: Some(log),
            body_outcome: Some(body_outcome),
            foreach_items: Some(foreach_items),
            step_report: Some(step_report),
        }
    }
}
