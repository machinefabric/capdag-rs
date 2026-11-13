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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::execute_plan::StepOutcome;
    use crate::planner::StepToken;
    use crate::AttributionClass;

    fn drain(receiver: &mut mpsc::UnboundedReceiver<RunEvent>) -> Vec<RunEvent> {
        let mut events = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            events.push(event);
        }
        events
    }

    // TEST22102: every signal of every run a host starts arrives on the one
    // receiver, in the order the callbacks gave them, tagged with the run and
    // strand it is of, and carrying exactly what its callback was handed.
    #[test]
    fn test22102_signals_of_every_run_arrive_in_order_tagged_with_their_run() {
        let (observer, mut receiver) = RunObserver::new();
        let first = RunKey { run: 0, strand: 1 };
        let second = RunKey { run: 3, strand: 0 };
        let a = observer.callbacks(first);
        let b = observer.callbacks(second);
        let step = StepToken::mint();
        let item = ForEachItemSnapshot {
            foreach_token_id: step.clone(),
            body_index: 4,
            enclosing: Vec::new(),
            item_preview_text: Some("page 5".into()),
            item_byte_count: 17,
        };

        (a.progress.as_ref().unwrap())(0.25, "cap:a", "starting");
        (b.step_progress.as_ref().unwrap())(0.5, "cap:b", "map-b");
        (a.log.as_ref().unwrap())(PipelineLogRecord::attributed(
            &step,
            "cap:a",
            "warn",
            AttributionClass::Input,
            "a page was empty",
        ));
        (b.foreach_items.as_ref().unwrap())("map-b", std::slice::from_ref(&item)).unwrap();
        (a.body_outcome.as_ref().unwrap())(&[]).unwrap();
        (b.step_report.as_ref().unwrap())(&StepReport {
            step_token_id: step.clone(),
            cap_urn: "cap:b".into(),
            body: None,
            outcome: StepOutcome::Skipped { absent: vec!["node-7".into()] },
        });

        let events = drain(&mut receiver);
        let keys: Vec<RunKey> = events.iter().map(|event| event.key).collect();
        assert_eq!(keys, [first, second, first, second, first, second]);
        match &events[0].signal {
            RunSignal::Progress { progress, cap_urn, message } => {
                assert_eq!((*progress, cap_urn.as_str(), message.as_str()), (0.25, "cap:a", "starting"));
            }
            other => panic!("first signal is {other:?}"),
        }
        match &events[1].signal {
            RunSignal::StepProgress { token_id, cap_urn, progress } => {
                assert_eq!((token_id.as_str(), cap_urn.as_str(), *progress), ("map-b", "cap:b", 0.5));
            }
            other => panic!("second signal is {other:?}"),
        }
        match &events[2].signal {
            RunSignal::Log(record) => {
                assert_eq!(record.message, "a page was empty");
                assert_eq!(record.level, "warn");
                assert_eq!(record.step_token_id.as_ref(), Some(&step));
            }
            other => panic!("third signal is {other:?}"),
        }
        match &events[3].signal {
            RunSignal::ForEachItems { map_token, items } => {
                assert_eq!(map_token, "map-b");
                assert_eq!(items.len(), 1);
                assert_eq!((items[0].body_index, items[0].item_byte_count), (4, 17));
                assert_eq!(items[0].item_preview_text.as_deref(), Some("page 5"));
            }
            other => panic!("fourth signal is {other:?}"),
        }
        match &events[4].signal {
            RunSignal::BodyOutcomes(outcomes) => assert!(outcomes.is_empty()),
            other => panic!("fifth signal is {other:?}"),
        }
        match &events[5].signal {
            RunSignal::StepReport(report) => {
                assert_eq!(report.cap_urn, "cap:b");
                assert_eq!(report.outcome, StepOutcome::Skipped { absent: vec!["node-7".into()] });
            }
            other => panic!("sixth signal is {other:?}"),
        }
        assert!(!observer.lost(), "every signal was delivered");
    }

    // TEST22103: a receiver dropped while a run still speaks is never absorbed:
    // the callbacks that can fail fail the run, naming it, and the ones that
    // cannot mark the observer lost for the host to see when the run returns.
    #[test]
    fn test22103_a_gone_receiver_fails_the_run_or_marks_the_observer_lost() {
        let (observer, receiver) = RunObserver::new();
        let callbacks = observer.callbacks(RunKey { run: 2, strand: 5 });
        drop(receiver);

        (callbacks.progress.as_ref().unwrap())(0.1, "cap:a", "starting");
        assert!(observer.lost(), "an undeliverable progress signal marks the observer lost");

        let (observer, receiver) = RunObserver::new();
        let callbacks = observer.callbacks(RunKey { run: 2, strand: 5 });
        drop(receiver);
        match (callbacks.body_outcome.as_ref().unwrap())(&[]) {
            Err(ExecutionError::HostError(message)) => {
                assert!(message.contains("run 2 strand 5"), "the failure names its run: {message}");
            }
            other => panic!("an undeliverable body outcome fails the run, not {other:?}"),
        }
        assert!(observer.lost());
        assert!(matches!(
            (callbacks.foreach_items.as_ref().unwrap())("map", &[]),
            Err(ExecutionError::HostError(_))
        ));
    }
}
