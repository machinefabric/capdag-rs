//! [`RunControl`] — stopping and aborting a run (15.2 §Runs stop).
//!
//! A run admits work — an item into a map, and where a host runs a machine
//! once per input, the run for the next input — until it is stopped. A stop
//! admits nothing new and closes the run's live feeds; what was admitted
//! drains to the outputs. An abort cancels every live request of the run as
//! the user's own cancel, and gives the cartridges [`ABORT_GRACE`] to end them.
//!
//! What a run does when it is asked something is the model's decision
//! (`CapDAG.Run.Control.next`, `capdag/formal`): this keeps the run's phase,
//! carries out the parts of the decision that are the run's own — closing
//! admission, raising the cancel the executor tears every chain down on — and
//! hands the rest back to the caller as the [`RunAct`] it must do: close the
//! live feeds on a stop, start the grace on a cancel, end the cartridges at
//! once on a kill, report the run on a finish. A caller keeps one per run and
//! hands it to [`execute_plan`](super::execute_plan::execute_plan).

use std::sync::Mutex;
use std::time::Duration;

use crate::bifaci::live_feed::{StopInputsError, StopInputsOutcome};
use crate::formal::run::control as model;
use crate::orchestrator::executor::{CollateralCause, RunCancel};

pub use model::{RunAct, RunAsk, RunOutcome, RunPhase};

/// How long an aborted run's cartridges are given to end the requests that
/// were cancelled, before what is left is ended at once.
pub const ABORT_GRACE: Duration = Duration::from_secs(5);

/// One run's control: its phase, whether it admits work, and the cancel every
/// chain of it tears down on.
pub struct RunControl {
    phase: Mutex<RunPhase>,
    admitting: tokio::sync::watch::Sender<bool>,
    cancel: RunCancel,
}

impl Default for RunControl {
    fn default() -> Self {
        Self::new()
    }
}

impl RunControl {
    /// A run that is running and admits work.
    pub fn new() -> Self {
        Self {
            phase: Mutex::new(RunPhase::Running),
            admitting: tokio::sync::watch::channel(true).0,
            cancel: RunCancel::new(),
        }
    }

    /// Ask the run something, and get back what the caller must do about it.
    ///
    /// The run's own part is done here: admission closes when the run stops
    /// admitting, and a cancel raises the run's cancel, attributed to the user,
    /// which ends every chain of the run.
    pub fn ask(&self, ask: RunAsk) -> RunAct {
        let next = {
            let mut phase = self.phase.lock().expect("run control phase mutex poisoned");
            let next = model::next(phase.clone(), ask);
            *phase = next.phase.clone();
            next
        };
        if !model::admits(next.phase.clone()) {
            self.admitting.send_if_modified(|admitting| std::mem::replace(admitting, false));
        }
        if next.act == RunAct::Cancel {
            self.cancel.raise(CollateralCause::aborted());
        }
        next.act
    }

    /// Where the run is.
    pub fn phase(&self) -> RunPhase {
        self.phase.lock().expect("run control phase mutex poisoned").clone()
    }

    /// Whether the run takes new work now.
    pub fn admits(&self) -> bool {
        *self.admitting.borrow()
    }

    /// Resolves once the run admits no new work. Never resolves while it does.
    pub async fn admission_closed(&self) {
        let mut rx = self.admitting.subscribe();
        // The sender lives as long as `self`, so `wait_for` ends only by the
        // value turning false.
        let _ = rx.wait_for(|admitting| !*admitting).await;
    }

    /// The cancel the run's chains are torn down on: raised by an abort, and
    /// by a failure that ends the run.
    pub fn cancel(&self) -> &RunCancel {
        &self.cancel
    }

    /// How the run ended, from the phase it ended in and what its steps did.
    /// `failed`: a failure ended it. `tolerated`: a step's policy tolerated a
    /// failure.
    pub fn outcome(&self, failed: bool, tolerated: bool) -> RunOutcome {
        model::outcome(self.phase(), failed, tolerated)
    }
}

/// Stop runs (15.2 §Runs stop): each admits no new work, and their live feeds
/// are closed by `close_feeds`, whose verdict is the stop's.
///
/// A stop is never refused for finding no feed: a run that was running is
/// stopped all the same, with all three counts zero. It is refused
/// ([`StopInputsError::NotRunning`]) only when no run was running and no feed
/// was open, which is a stop that could do nothing.
pub async fn stop_runs<'a, F>(
    controls: impl IntoIterator<Item = &'a RunControl>,
    close_feeds: F,
) -> Result<StopInputsOutcome, StopInputsError>
where
    F: std::future::Future<Output = Result<StopInputsOutcome, StopInputsError>>,
{
    let mut stopped = false;
    for control in controls {
        if control.ask(RunAsk::Stop) == RunAct::Stop {
            stopped = true;
        }
    }
    match close_feeds.await {
        Err(StopInputsError::NothingToStop) if stopped => Ok(StopInputsOutcome::default()),
        Err(StopInputsError::NothingToStop) => Err(StopInputsError::NotRunning),
        verdict => verdict,
    }
}

/// The exit status a command reports for an outcome: 0 completed, 1 failed,
/// 3 completed with tolerated failures, 130 stopped or aborted by a person.
pub fn exit_status(outcome: RunOutcome) -> i32 {
    let status = model::exit_status(outcome)
        .to_u64()
        .expect("the model's exit statuses are small");
    i32::try_from(status).expect("the model's exit statuses are small")
}

/// Several runs reported as one: the heaviest of them.
pub fn join_outcomes(outcomes: impl IntoIterator<Item = RunOutcome>) -> RunOutcome {
    outcomes
        .into_iter()
        .fold(RunOutcome::Completed, model::join_outcomes)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The run's own objects reproduce every script of the model's table
    /// (`capdag/formal/conformance-run.json`, written by `lake exe
    /// conformance_run`): the phase after each ask, what the caller is told
    /// to do, and whether the run still admits work.
    #[test]
    fn test20100_run_control_replays_the_models_scripts() {
        let table = conformance_table();
        let scripts = table["control"].as_array().expect("control scripts");
        assert!(!scripts.is_empty());
        for script in scripts {
            let control = RunControl::new();
            let asks = script["asks"].as_array().expect("asks");
            let steps = script["steps"].as_array().expect("steps");
            for (ask, step) in asks.iter().zip(steps) {
                let act = control.ask(ask_named(ask.as_str().expect("ask name")));
                assert_eq!(act_name(&act), step["act"], "script {script}");
                assert_eq!(phase_name(&control.phase()), step["phase"], "script {script}");
                assert_eq!(control.admits(), step["admits"].as_bool().expect("admits"), "script {script}");
            }
        }
    }

    /// Every way a run can end is reported and exits as the model says, and
    /// runs reported together are reported as the model joins them.
    #[test]
    fn test20101_outcomes_and_exit_statuses_are_the_models() {
        let table = conformance_table();
        for row in table["outcome"].as_array().expect("outcome rows") {
            let phase = phase_named(row["phase"].as_str().expect("phase"));
            let outcome = model::outcome(
                phase,
                row["failed"].as_bool().expect("failed"),
                row["tolerated"].as_bool().expect("tolerated"),
            );
            assert_eq!(outcome_name(&outcome), row["outcome"], "{row}");
            assert_eq!(exit_status(outcome) as u64, row["exit_status"].as_u64().expect("status"), "{row}");
        }
        for row in table["join"].as_array().expect("join rows") {
            let joined = join_outcomes([
                outcome_named(row["a"].as_str().expect("a")),
                outcome_named(row["b"].as_str().expect("b")),
            ]);
            assert_eq!(outcome_name(&joined), row["joined"], "{row}");
        }
    }

    /// A stop closes admission and raises no cancel; the abort after it raises
    /// the run's cancel as the user's, which every chain reads.
    #[tokio::test]
    async fn test20102_stop_closes_admission_and_abort_raises_the_users_cancel() {
        let control = RunControl::new();
        assert!(control.admits());
        assert_eq!(control.ask(RunAsk::Interrupt), RunAct::Stop);
        assert!(!control.admits());
        // Admission is closed now; a waiter is told at once.
        tokio::time::timeout(Duration::from_secs(1), control.admission_closed())
            .await
            .expect("admission_closed resolves once admission is closed");
        assert!(control.cancel().cause().is_none(), "a stop never cancels");
        assert_eq!(control.ask(RunAsk::Interrupt), RunAct::Cancel);
        let cause = control.cancel().cause().expect("the abort raised the run's cancel");
        assert_eq!(cause.class, crate::failure::AttributionClass::User);
    }

    /// A run with no live feed is stopped all the same, with nothing to close;
    /// a second stop, with the run already stopping and nothing open, is
    /// refused as a stop that could do nothing.
    #[tokio::test]
    async fn test20103_a_run_without_a_feed_is_stopped_and_a_second_stop_is_refused() {
        let control = RunControl::new();
        let nothing_open = || async { Err(StopInputsError::NothingToStop) };
        let verdict = stop_runs([&control], nothing_open()).await;
        assert_eq!(verdict, Ok(StopInputsOutcome::default()));
        assert!(!control.admits());
        let again = stop_runs([&control], nothing_open()).await;
        assert_eq!(again, Err(StopInputsError::NotRunning));
    }

    fn conformance_table() -> serde_json::Value {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../formal/conformance-run.json");
        let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        serde_json::from_str(&text).expect("conformance-run.json is JSON")
    }

    fn ask_named(name: &str) -> RunAsk {
        match name {
            "interrupt" => RunAsk::Interrupt,
            "stop" => RunAsk::Stop,
            "abort" => RunAsk::Abort,
            "drained" => RunAsk::Drained,
            "grace_over" => RunAsk::GraceOver,
            other => panic!("the table names an ask the model has not: {other}"),
        }
    }

    fn phase_named(name: &str) -> RunPhase {
        match name {
            "running" => RunPhase::Running,
            "stopping" => RunPhase::Stopping,
            "aborting" => RunPhase::Aborting,
            "ended" => RunPhase::Ended,
            other => panic!("the table names a phase the model has not: {other}"),
        }
    }

    fn phase_name(phase: &RunPhase) -> &'static str {
        match phase {
            RunPhase::Running => "running",
            RunPhase::Stopping => "stopping",
            RunPhase::Aborting => "aborting",
            RunPhase::Ended => "ended",
        }
    }

    fn act_name(act: &RunAct) -> &'static str {
        match act {
            RunAct::None => "none",
            RunAct::Stop => "stop",
            RunAct::Cancel => "cancel",
            RunAct::Kill => "kill",
            RunAct::Finish => "finish",
        }
    }

    fn outcome_named(name: &str) -> RunOutcome {
        match name {
            "completed" => RunOutcome::Completed,
            "tolerated" => RunOutcome::Tolerated,
            "stopped" => RunOutcome::Stopped,
            "failed" => RunOutcome::Failed,
            "aborted" => RunOutcome::Aborted,
            other => panic!("the table names an outcome the model has not: {other}"),
        }
    }

    fn outcome_name(outcome: &RunOutcome) -> &'static str {
        match outcome {
            RunOutcome::Completed => "completed",
            RunOutcome::Tolerated => "tolerated",
            RunOutcome::Stopped => "stopped",
            RunOutcome::Failed => "failed",
            RunOutcome::Aborted => "aborted",
        }
    }
}
