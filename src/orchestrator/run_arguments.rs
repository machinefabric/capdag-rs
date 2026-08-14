//! The executing plan's mutable argument state, journaled by dispatch.
//!
//! `execute_plan` used to take an immutable argument map, which made "change an
//! argument of a run that is already executing" unrepresentable. The ledger
//! replaces that map: it holds the run's authoritative per-step argument
//! values AND the dispatch journal that says which work has already read them.
//!
//! The ordering rule for the dispatch/update race lives here, in one mutex:
//! a dispatch (a segment invocation being built, a ForEach body being spawned)
//! and an update serialize on the ledger's lock, and the journal decides which
//! came first. An update's per-step outcome is computed inside the same
//! critical section that applies it, so the report can never disagree with
//! what the executor actually delivered:
//!
//! - a step whose only dispatch is behind it reports **already dispatched** and
//!   its value is left untouched — pretending otherwise would falsify the
//!   persisted argument record of work that already ran;
//! - a step inside a ForEach region reports exactly how many bodies took the
//!   old value; every later body reads the new one;
//! - a step not yet reached reports **applied** — every invocation will read
//!   the new value.
//!
//! Keys are the plan's cap node ids, which ARE the strand steps' minted
//! `StepToken` ids for the one plan being executed. Tokens are minted per
//! plan: an update addressed with a token from any other plan (a re-plan, a
//! different run) names no node here and is refused as a whole.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crate::planner::plan::{ExecutionNodeType, MachinePlan};
use crate::MediaUrn;

/// One requested argument change, addressed by the executing plan's own step
/// token (== cap node id) and the argument's media URN.
#[derive(Debug, Clone)]
pub struct ArgumentUpdate {
    pub token_id: String,
    pub media_urn: String,
    pub value: Vec<u8>,
}

/// What one update actually reached, decided under the dispatch lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgumentUpdateDisposition {
    /// No dispatch of the step has happened — every invocation reads the new
    /// value.
    Applied,
    /// The step runs once per ForEach body; `bodies_dispatched` bodies had
    /// already read the old value, every later body reads the new one.
    AppliedToRemainingBodies { bodies_dispatched: u64 },
    /// Every dispatch of the step is behind us — the value was left untouched.
    AlreadyDispatched,
}

/// Per-update outcome, in request order.
#[derive(Debug, Clone)]
pub struct ArgumentUpdateOutcome {
    pub token_id: String,
    pub media_urn: String,
    pub disposition: ArgumentUpdateDisposition,
}

/// The result of one applied update batch.
#[derive(Debug, Clone)]
pub struct AppliedArgumentUpdate {
    pub outcomes: Vec<ArgumentUpdateOutcome>,
    /// Monotone revision of the ledger after this batch (0 = as created).
    pub revision: u64,
}

/// A refused update batch. The batch is transactional: any invalid entry
/// refuses the whole batch and no value changes.
#[derive(Debug, thiserror::Error)]
pub enum RunArgumentError {
    #[error(
        "argument update is addressed to step token '{token_id}', which is not a cap node of \
         the executing plan — step tokens are minted per plan, so this token belongs to a \
         different plan generation and the update cannot be delivered"
    )]
    UnknownStep { token_id: String },
    #[error("argument update for step '{token_id}' names invalid media URN '{media_urn}': {detail}")]
    InvalidMediaUrn {
        token_id: String,
        media_urn: String,
        detail: String,
    },
    #[error(
        "initial argument value is keyed to '{token_id}', which is not a cap node of the plan"
    )]
    UnknownInitialStep { token_id: String },
}

#[derive(Debug, Default)]
struct StepJournal {
    /// The step's single whole-segment dispatch happened (trunk or
    /// post-region segment invocation built).
    dispatched: bool,
    /// ForEach-contained steps: bodies that have read this step's arguments.
    bodies_dispatched: u64,
    /// No further dispatch of this step will ever happen (segment dispatched,
    /// or the region's item source is exhausted).
    exhausted: bool,
}

#[derive(Debug)]
struct Inner {
    /// step token → (argument media URN, value bytes), the values the NEXT
    /// dispatch of each step reads.
    values: HashMap<String, Vec<(String, Vec<u8>)>>,
    /// Every cap node of the plan has a journal entry from birth, so an
    /// unknown token is distinguishable from an untouched step.
    steps: HashMap<String, StepJournal>,
    revision: u64,
}

/// Mutable, dispatch-journaled argument state of one executing plan.
#[derive(Debug)]
pub struct RunArgumentLedger {
    inner: Mutex<Inner>,
}

impl RunArgumentLedger {
    /// Build the ledger for one plan from the creation-time argument map.
    /// Every value key must name a cap node of THIS plan.
    pub fn new(
        plan: &MachinePlan,
        values: HashMap<String, Vec<(String, Vec<u8>)>>,
    ) -> Result<Self, RunArgumentError> {
        let steps: HashMap<String, StepJournal> = plan
            .nodes
            .iter()
            .filter(|(_, node)| matches!(node.node_type, ExecutionNodeType::Cap { .. }))
            .map(|(id, _)| (id.clone(), StepJournal::default()))
            .collect();
        for token_id in values.keys() {
            if !steps.contains_key(token_id) {
                return Err(RunArgumentError::UnknownInitialStep {
                    token_id: token_id.clone(),
                });
            }
        }
        Ok(Self {
            inner: Mutex::new(Inner {
                values,
                steps,
                revision: 0,
            }),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("run argument ledger mutex poisoned")
    }

    /// A whole segment is being dispatched: journal every one of the
    /// subplan's cap nodes as dispatched-and-exhausted and return the values
    /// its invocations read. One atomic step — an update can land entirely
    /// before or entirely after, never between a segment's caps.
    pub fn snapshot_for_segment(
        &self,
        segment: &MachinePlan,
    ) -> HashMap<String, Vec<(String, Vec<u8>)>> {
        let mut inner = self.lock();
        for (id, node) in &segment.nodes {
            if !matches!(node.node_type, ExecutionNodeType::Cap { .. }) {
                continue;
            }
            let journal = inner
                .steps
                .get_mut(id)
                .unwrap_or_else(|| panic!("segment cap node '{id}' is not a cap node of the plan"));
            journal.dispatched = true;
            journal.exhausted = true;
        }
        inner.values.clone()
    }

    /// One ForEach body is being spawned: journal the body subplan's cap nodes
    /// as having dispatched `body_index + 1` bodies and return the values this
    /// body reads.
    pub fn snapshot_for_body(
        &self,
        body: &MachinePlan,
        body_index: usize,
    ) -> HashMap<String, Vec<(String, Vec<u8>)>> {
        let mut inner = self.lock();
        let dispatched = (body_index as u64) + 1;
        for (id, node) in &body.nodes {
            if !matches!(node.node_type, ExecutionNodeType::Cap { .. }) {
                continue;
            }
            let journal = inner
                .steps
                .get_mut(id)
                .unwrap_or_else(|| panic!("body cap node '{id}' is not a cap node of the plan"));
            journal.bodies_dispatched = journal.bodies_dispatched.max(dispatched);
        }
        inner.values.clone()
    }

    /// The region's item source is exhausted: no further body will dispatch,
    /// so its steps report **already dispatched** from here on.
    pub fn exhaust_bodies(&self, body: &MachinePlan) {
        let mut inner = self.lock();
        for (id, node) in &body.nodes {
            if !matches!(node.node_type, ExecutionNodeType::Cap { .. }) {
                continue;
            }
            let journal = inner
                .steps
                .get_mut(id)
                .unwrap_or_else(|| panic!("body cap node '{id}' is not a cap node of the plan"));
            journal.exhausted = true;
        }
    }

    /// Apply a batch of updates transactionally. Validation (every token names
    /// a cap node, every URN parses) happens before any value changes; the
    /// per-step disposition is decided and the value written under the same
    /// lock a dispatch takes, so the reported outcome IS what the executor
    /// delivers.
    pub fn apply(
        &self,
        updates: &[ArgumentUpdate],
    ) -> Result<AppliedArgumentUpdate, RunArgumentError> {
        let mut inner = self.lock();
        // Validate the whole batch first — a refused batch changes nothing.
        let mut parsed: Vec<MediaUrn> = Vec::with_capacity(updates.len());
        for update in updates {
            if !inner.steps.contains_key(&update.token_id) {
                return Err(RunArgumentError::UnknownStep {
                    token_id: update.token_id.clone(),
                });
            }
            let urn = MediaUrn::from_string(&update.media_urn).map_err(|e| {
                RunArgumentError::InvalidMediaUrn {
                    token_id: update.token_id.clone(),
                    media_urn: update.media_urn.clone(),
                    detail: e.to_string(),
                }
            })?;
            parsed.push(urn);
        }

        let mut outcomes = Vec::with_capacity(updates.len());
        let mut changed = false;
        for (update, urn) in updates.iter().zip(parsed.into_iter()) {
            let journal = inner
                .steps
                .get(&update.token_id)
                .expect("validated above");
            let disposition = if journal.exhausted || journal.dispatched {
                ArgumentUpdateDisposition::AlreadyDispatched
            } else if journal.bodies_dispatched > 0 {
                ArgumentUpdateDisposition::AppliedToRemainingBodies {
                    bodies_dispatched: journal.bodies_dispatched,
                }
            } else {
                ArgumentUpdateDisposition::Applied
            };
            if disposition != ArgumentUpdateDisposition::AlreadyDispatched {
                let entries = inner.values.entry(update.token_id.clone()).or_default();
                // Replace by media-URN equivalence (URNs are opaque — never
                // compared as strings), else append: an argument that rode on
                // its cap-side default until now gains its first explicit
                // value.
                let existing = entries.iter_mut().find(|(stored, _)| {
                    MediaUrn::from_string(stored)
                        .map(|s| s.is_equivalent(&urn).unwrap_or(false))
                        .unwrap_or(false)
                });
                match existing {
                    Some(entry) => entry.1 = update.value.clone(),
                    None => entries.push((update.media_urn.clone(), update.value.clone())),
                }
                changed = true;
            }
            outcomes.push(ArgumentUpdateOutcome {
                token_id: update.token_id.clone(),
                media_urn: update.media_urn.clone(),
                disposition,
            });
        }
        if changed {
            inner.revision += 1;
        }
        Ok(AppliedArgumentUpdate {
            outcomes,
            revision: inner.revision,
        })
    }

    /// The current revision (0 until the first applied change).
    pub fn revision(&self) -> u64 {
        self.lock().revision
    }

    /// The step tokens this ledger journals (the plan's cap nodes).
    pub fn step_tokens(&self) -> HashSet<String> {
        self.lock().steps.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::plan::{MachineNode, MachinePlanEdge};
    use crate::planner::InputCardinality;

    fn cap_node(id: &str) -> MachineNode {
        MachineNode::cap(id, "cap:test")
    }

    fn plan_linear(tokens: &[&str]) -> MachinePlan {
        let mut plan = MachinePlan::new("test");
        plan.add_node(MachineNode::input_slot(
            "input_0",
            "in",
            "media:",
            InputCardinality::Single,
        ));
        let mut prev = "input_0".to_string();
        for token in tokens {
            plan.add_node(cap_node(token));
            plan.add_edge(MachinePlanEdge::direct(&prev, token));
            prev = token.to_string();
        }
        plan.add_node(MachineNode::output("output_0", "out", &prev));
        plan.add_edge(MachinePlanEdge::direct(&prev, "output_0"));
        plan
    }

    fn value(urn: &str, bytes: &[u8]) -> (String, Vec<u8>) {
        (urn.to_string(), bytes.to_vec())
    }

    // TEST1470: an update to an undisputed (never-dispatched) step applies and
    // the next segment snapshot delivers the new value.
    #[test]
    fn test1470_update_before_dispatch_applies() {
        let plan = plan_linear(&["tok_a"]);
        let mut initial = HashMap::new();
        initial.insert("tok_a".to_string(), vec![value("media:numeric;width", b"3")]);
        let ledger = RunArgumentLedger::new(&plan, initial).unwrap();

        let applied = ledger
            .apply(&[ArgumentUpdate {
                token_id: "tok_a".to_string(),
                media_urn: "media:numeric;width".to_string(),
                value: b"7".to_vec(),
            }])
            .unwrap();
        assert_eq!(applied.outcomes.len(), 1);
        assert_eq!(
            applied.outcomes[0].disposition,
            ArgumentUpdateDisposition::Applied
        );
        assert_eq!(applied.revision, 1);

        let snapshot = ledger.snapshot_for_segment(&plan);
        assert_eq!(snapshot["tok_a"], vec![value("media:numeric;width", b"7")]);
    }

    // TEST1471: after a segment dispatched a step, an update reports
    // already-dispatched and leaves the delivered value untouched.
    #[test]
    fn test1471_update_after_segment_dispatch_is_already_dispatched() {
        let plan = plan_linear(&["tok_a"]);
        let mut initial = HashMap::new();
        initial.insert("tok_a".to_string(), vec![value("media:numeric;width", b"3")]);
        let ledger = RunArgumentLedger::new(&plan, initial).unwrap();

        let first = ledger.snapshot_for_segment(&plan);
        assert_eq!(first["tok_a"], vec![value("media:numeric;width", b"3")]);

        let applied = ledger
            .apply(&[ArgumentUpdate {
                token_id: "tok_a".to_string(),
                media_urn: "media:numeric;width".to_string(),
                value: b"7".to_vec(),
            }])
            .unwrap();
        assert_eq!(
            applied.outcomes[0].disposition,
            ArgumentUpdateDisposition::AlreadyDispatched
        );
        // Nothing changed, so the revision holds.
        assert_eq!(applied.revision, 0);
        assert_eq!(
            ledger.snapshot_for_segment(&plan)["tok_a"],
            vec![value("media:numeric;width", b"3")]
        );
    }

    // TEST1472: a ForEach-contained step reports how many bodies took the old
    // value; the next body's snapshot reads the new one; exhausting the source
    // flips later updates to already-dispatched.
    #[test]
    fn test1472_body_dispatch_race_semantics() {
        let plan = plan_linear(&["tok_body"]);
        let mut initial = HashMap::new();
        initial.insert(
            "tok_body".to_string(),
            vec![value("media:enc=utf-8;question", b"old")],
        );
        let ledger = RunArgumentLedger::new(&plan, initial).unwrap();

        // Bodies 0 and 1 dispatch with the old value.
        assert_eq!(
            ledger.snapshot_for_body(&plan, 0)["tok_body"],
            vec![value("media:enc=utf-8;question", b"old")]
        );
        let second = ledger.snapshot_for_body(&plan, 1);
        assert_eq!(
            second["tok_body"],
            vec![value("media:enc=utf-8;question", b"old")]
        );

        let applied = ledger
            .apply(&[ArgumentUpdate {
                token_id: "tok_body".to_string(),
                media_urn: "media:enc=utf-8;question".to_string(),
                value: b"new".to_vec(),
            }])
            .unwrap();
        assert_eq!(
            applied.outcomes[0].disposition,
            ArgumentUpdateDisposition::AppliedToRemainingBodies {
                bodies_dispatched: 2
            }
        );

        // Body 2 reads the new value.
        assert_eq!(
            ledger.snapshot_for_body(&plan, 2)["tok_body"],
            vec![value("media:enc=utf-8;question", b"new")]
        );

        // Source exhausted: no more bodies — later updates are honest about it.
        ledger.exhaust_bodies(&plan);
        let late = ledger
            .apply(&[ArgumentUpdate {
                token_id: "tok_body".to_string(),
                media_urn: "media:enc=utf-8;question".to_string(),
                value: b"too-late".to_vec(),
            }])
            .unwrap();
        assert_eq!(
            late.outcomes[0].disposition,
            ArgumentUpdateDisposition::AlreadyDispatched
        );
    }

    // TEST1473: a batch with one unknown step token refuses the WHOLE batch —
    // no partial application. Tokens are minted per plan, so a foreign token
    // is a foreign plan.
    #[test]
    fn test1473_unknown_token_refuses_whole_batch() {
        let plan = plan_linear(&["tok_a"]);
        let ledger = RunArgumentLedger::new(&plan, HashMap::new()).unwrap();
        let err = ledger
            .apply(&[
                ArgumentUpdate {
                    token_id: "tok_a".to_string(),
                    media_urn: "media:numeric;width".to_string(),
                    value: b"7".to_vec(),
                },
                ArgumentUpdate {
                    token_id: "tok_from_another_plan".to_string(),
                    media_urn: "media:numeric;width".to_string(),
                    value: b"9".to_vec(),
                },
            ])
            .unwrap_err();
        assert!(matches!(err, RunArgumentError::UnknownStep { .. }));
        // The valid half of the batch did not apply.
        assert_eq!(ledger.revision(), 0);
        let snapshot = ledger.snapshot_for_segment(&plan);
        assert!(snapshot.get("tok_a").is_none());
    }

    // TEST1474: an update for an argument with no creation-time binding (it
    // rode on its cap default) gains its first explicit value.
    #[test]
    fn test1474_update_creates_first_binding() {
        let plan = plan_linear(&["tok_a"]);
        let ledger = RunArgumentLedger::new(&plan, HashMap::new()).unwrap();
        let applied = ledger
            .apply(&[ArgumentUpdate {
                token_id: "tok_a".to_string(),
                media_urn: "media:numeric;width".to_string(),
                value: b"7".to_vec(),
            }])
            .unwrap();
        assert_eq!(
            applied.outcomes[0].disposition,
            ArgumentUpdateDisposition::Applied
        );
        assert_eq!(
            ledger.snapshot_for_segment(&plan)["tok_a"],
            vec![value("media:numeric;width", b"7")]
        );
    }

    // TEST1475: initial values keyed to a token that names no cap node are
    // refused at construction.
    #[test]
    fn test1475_initial_values_must_name_plan_steps() {
        let plan = plan_linear(&["tok_a"]);
        let mut initial = HashMap::new();
        initial.insert("tok_stale".to_string(), vec![value("media:numeric;width", b"3")]);
        let err = RunArgumentLedger::new(&plan, initial).unwrap_err();
        assert!(matches!(err, RunArgumentError::UnknownInitialStep { .. }));
    }
}
