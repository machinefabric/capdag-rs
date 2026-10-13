//! The executing plan's mutable argument state, journaled by dispatch.
//!
//! `execute_plan` used to take an immutable argument map, which made "change an
//! argument of a run that is already executing" unrepresentable. The ledger
//! replaces that map: it holds the run's authoritative per-step argument
//! values AND the dispatch journal that says which work has already read them.
//!
//! The ordering rule for the dispatch/update race lives here, in one mutex:
//! a dispatch (a chain of steps being opened — once, or once more inside a
//! map) and an update serialize on the ledger's lock, and the journal decides which
//! came first. An update's per-step outcome is computed inside the same
//! critical section that applies it, so the report can never disagree with
//! what the executor actually delivered:
//!
//! - a step whose only dispatch is behind it reports **already dispatched** and
//!   its value is left untouched — pretending otherwise would falsify the
//!   persisted argument record of work that already ran;
//! - a step inside a map reports exactly how many runs of it took the old
//!   value; every later run reads the new one;
//! - a step not yet reached reports **applied** — every invocation will read
//!   the new value.
//!
//! ## Who owns an argument's value
//!
//! An argument the machine itself binds — to a constant it writes, or to a
//! node another step produces or the run supplies as an input — is **bound**:
//! the machine says what feeds it, and a value for it from the run (at
//! creation or mid-run) is refused by name, the whole batch with it. A
//! constant is in the ledger from the start: it is the step's value for that
//! argument, read by every dispatch. An argument the machine leaves unbound
//! takes what the run gives it here, and failing that what the cap's
//! definition defaults to.
//!
//! Keys are the plan's cap node ids, which ARE the strand steps' minted
//! `StepToken` ids for the one plan being executed. Tokens are minted per
//! plan: an update addressed with a token from any other plan (a re-plan, a
//! different run) names no node here and is refused as a whole.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crate::planner::argument_binding::{json_value_to_bytes, ArgumentBinding};
use crate::planner::plan::{EdgeType, ExecutionNodeType, MachinePlan};
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
    #[error(
        "step '{token_id}' has its argument '{media_urn}' bound by the machine — a machine that \
         says what feeds an argument does not take a value for it from the run"
    )]
    ArgumentBound { token_id: String, media_urn: String },
    #[error("the plan's step '{token_id}' names invalid media URN '{media_urn}': {detail}")]
    InvalidPlanMediaUrn {
        token_id: String,
        media_urn: String,
        detail: String,
    },
    #[error("the plan's step '{token_id}' runs '{cap_urn}', which is not a cap URN: {detail}")]
    InvalidPlanCapUrn {
        token_id: String,
        cap_urn: String,
        detail: String,
    },
}

#[derive(Debug, Default)]
struct StepJournal {
    /// The step's single dispatch happened (a step outside any map).
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
    /// step token → the arguments the machine binds for that step (a
    /// constant, or a node wired into it), by the URN the argument is
    /// delivered under. A run-time value for one of these is refused.
    bound: HashMap<String, Vec<MediaUrn>>,
    revision: u64,
}

impl Inner {
    /// Whether the machine binds the argument `urn` of step `token_id`.
    fn is_bound(&self, token_id: &str, urn: &MediaUrn) -> bool {
        self.bound.get(token_id).is_some_and(|bound| {
            bound.iter().any(|b| b.is_equivalent(urn).unwrap_or(false))
        })
    }
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

        // What the machine binds, per step: the constants its cap nodes carry,
        // and every argument a plan edge wires a node into. The constants are
        // the step's values from the start.
        let media = |token_id: &str, urn: &str| {
            MediaUrn::from_string(urn).map_err(|e| RunArgumentError::InvalidPlanMediaUrn {
                token_id: token_id.to_string(),
                media_urn: urn.to_string(),
                detail: e.to_string(),
            })
        };
        let mut bound: HashMap<String, Vec<MediaUrn>> = HashMap::new();
        let mut constants: HashMap<String, Vec<(String, Vec<u8>)>> = HashMap::new();
        for (token_id, node) in &plan.nodes {
            let ExecutionNodeType::Cap {
                cap_urn,
                arg_bindings,
                ..
            } = &node.node_type
            else {
                continue;
            };
            let step_bound = bound.entry(token_id.clone()).or_default();
            // Deterministic order: the bindings are a map.
            let mut literals: Vec<(&String, &serde_json::Value)> = arg_bindings
                .bindings
                .iter()
                .filter_map(|(arg_urn, binding)| match binding {
                    ArgumentBinding::Literal { value } => Some((arg_urn, value)),
                    _ => None,
                })
                .collect();
            literals.sort_by(|a, b| a.0.cmp(b.0));
            for (arg_urn, value) in literals {
                step_bound.push(media(token_id, arg_urn)?);
                constants
                    .entry(token_id.clone())
                    .or_default()
                    .push((arg_urn.clone(), json_value_to_bytes(value)));
            }
            // A node wired into the step: its main input (delivered under the
            // cap's `in=`) or the argument an `Arg` edge names.
            for edge in plan.edges.iter().filter(|edge| &edge.to_node == token_id) {
                match &edge.edge_type {
                    EdgeType::Arg { arg_urn } => step_bound.push(media(token_id, arg_urn)?),
                    // Into a step, a `Direct` edge is its main input, delivered
                    // under the cap's `in=`.
                    EdgeType::Direct => {
                        let cap = crate::CapUrn::from_string(cap_urn).map_err(|e| {
                            RunArgumentError::InvalidPlanCapUrn {
                                token_id: token_id.clone(),
                                cap_urn: cap_urn.clone(),
                                detail: e.to_string(),
                            }
                        })?;
                        step_bound.push(media(token_id, cap.in_spec())?);
                    }
                    EdgeType::Iteration | EdgeType::Collection => {}
                }
            }
        }

        let mut inner = Inner {
            values: constants,
            steps,
            bound,
            revision: 0,
        };
        // What the run supplies goes beside the constants — and never onto an
        // argument the machine binds.
        for (token_id, entries) in values {
            for (media_urn, bytes) in entries {
                let urn = MediaUrn::from_string(&media_urn).map_err(|e| {
                    RunArgumentError::InvalidMediaUrn {
                        token_id: token_id.clone(),
                        media_urn: media_urn.clone(),
                        detail: e.to_string(),
                    }
                })?;
                if inner.is_bound(&token_id, &urn) {
                    return Err(RunArgumentError::ArgumentBound { token_id, media_urn });
                }
                inner
                    .values
                    .entry(token_id.clone())
                    .or_default()
                    .push((media_urn, bytes));
            }
        }
        Ok(Self {
            inner: Mutex::new(inner),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("run argument ledger mutex poisoned")
    }

    /// The values the named steps read, and nothing else's.
    fn values_of(inner: &Inner, steps: &[&str]) -> HashMap<String, Vec<(String, Vec<u8>)>> {
        steps
            .iter()
            .filter_map(|step| {
                inner
                    .values
                    .get(*step)
                    .map(|values| (step.to_string(), values.clone()))
            })
            .collect()
    }

    /// `steps` are being dispatched NOW, and none of them will be dispatched
    /// again: steps that run once (not inside a map). Journals each as
    /// dispatched and returns the values their invocations read — one atomic
    /// step, so an update lands entirely before or entirely after a step's
    /// dispatch, never during it. A step attempted again reads what its first
    /// attempt read: it is dispatched once.
    pub fn dispatch_steps(&self, steps: &[&str]) -> HashMap<String, Vec<(String, Vec<u8>)>> {
        let mut inner = self.lock();
        for step in steps {
            let journal = inner
                .steps
                .get_mut(*step)
                .unwrap_or_else(|| panic!("dispatched step '{step}' is not a cap node of the plan"));
            journal.dispatched = true;
            journal.exhausted = true;
        }
        Self::values_of(&inner, steps)
    }

    /// One more run of `steps` is being dispatched: steps inside a map, each
    /// dispatched once per run of the map's body (once per run of every map
    /// it is inside). Journals the run and returns the values it reads.
    pub fn dispatch_steps_in_map(
        &self,
        steps: &[&str],
    ) -> HashMap<String, Vec<(String, Vec<u8>)>> {
        let mut inner = self.lock();
        for step in steps {
            let journal = inner
                .steps
                .get_mut(*step)
                .unwrap_or_else(|| panic!("dispatched step '{step}' is not a cap node of the plan"));
            journal.bodies_dispatched += 1;
        }
        Self::values_of(&inner, steps)
    }

    /// No further run of `steps` will ever be dispatched: the outermost map
    /// they are inside has ended. They report **already dispatched** from
    /// here on.
    pub fn exhaust_steps(&self, steps: &[&str]) {
        let mut inner = self.lock();
        for step in steps {
            let journal = inner
                .steps
                .get_mut(*step)
                .unwrap_or_else(|| panic!("exhausted step '{step}' is not a cap node of the plan"));
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
            // An argument the machine binds is not the run's to change.
            if inner.is_bound(&update.token_id, &urn) {
                return Err(RunArgumentError::ArgumentBound {
                    token_id: update.token_id.clone(),
                    media_urn: update.media_urn.clone(),
                });
            }
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

    /// A two-step plan as a machine with a constant and a wired argument makes
    /// it: `tok_a` is fed the input and has `width` bound to the constant 20;
    /// `tok_b` is fed `tok_a`'s output on its main input and the input on its
    /// `mask` argument. Both leave `height` to the run.
    fn plan_with_bound_arguments() -> MachinePlan {
        use crate::planner::argument_binding::ArgumentBindings;
        let cap = r#"cap:in="media:image";scale;out="media:image""#;
        let mut plan = MachinePlan::new("bound");
        plan.add_node(MachineNode::input_slot("input_0", "in", "media:image", InputCardinality::Single));
        let mut bindings = ArgumentBindings::new();
        bindings.add(
            "media:numeric;width".to_string(),
            ArgumentBinding::Literal { value: serde_json::json!(20) },
        );
        bindings.add(
            "media:enc=utf-8;label".to_string(),
            ArgumentBinding::Literal { value: serde_json::json!("thumb") },
        );
        plan.add_node(MachineNode::cap_with_bindings("tok_a", cap, bindings));
        plan.add_node(MachineNode::cap("tok_b", cap));
        plan.add_edge(MachinePlanEdge::direct("input_0", "tok_a"));
        plan.add_edge(MachinePlanEdge::direct("tok_a", "tok_b"));
        plan.add_edge(MachinePlanEdge::arg("input_0", "tok_b", "media:image;mask"));
        plan.add_node(MachineNode::output("output_0", "out", "tok_b"));
        plan.add_edge(MachinePlanEdge::direct("tok_b", "output_0"));
        plan
    }

    // TEST12744: a constant a machine writes is the step's value for that
    // argument from the start, read by its dispatch beside what the run
    // supplies for the arguments the machine left unbound.
    #[test]
    fn test12744_constants_are_in_the_ledger_from_the_start() {
        let plan = plan_with_bound_arguments();
        let mut initial = HashMap::new();
        initial.insert("tok_a".to_string(), vec![value("media:numeric;height", b"9")]);
        let ledger = RunArgumentLedger::new(&plan, initial.clone()).unwrap();
        let snapshot = ledger.dispatch_steps(&["tok_a", "tok_b"]);
        let mut a = snapshot["tok_a"].clone();
        a.sort();
        // A number is delivered as its digits, a string as its text.
        assert_eq!(
            a,
            vec![
                value("media:enc=utf-8;label", b"thumb"),
                value("media:numeric;height", b"9"),
                value("media:numeric;width", b"20"),
            ]
        );
        assert!(snapshot.get("tok_b").map_or(true, |v| v.is_empty()), "tok_b has no constant");

        // A dispatch returns the values of the steps it dispatches, and no
        // other step's: what `tok_a` holds is not handed to `tok_b`'s chain.
        let ledger = RunArgumentLedger::new(&plan, initial).unwrap();
        let of_b = ledger.dispatch_steps(&["tok_b"]);
        assert!(!of_b.contains_key("tok_a"), "{of_b:?}");
        assert_eq!(ledger.dispatch_steps(&["tok_a"])["tok_a"].len(), 3);
    }

    // TEST12745: an argument the machine binds takes no value from the run —
    // not when the run is created and not while it runs, whether the machine
    // bound it to a constant, to its main input, or to a node wired into a
    // named argument. The refusal names the step and the argument, and a
    // refused batch changes nothing. An unbound argument is still the run's.
    #[test]
    fn test12745_a_bound_argument_refuses_a_run_time_value() {
        let plan = plan_with_bound_arguments();
        let refused_at_creation = |token: &str, urn: &str| {
            let mut initial = HashMap::new();
            initial.insert(token.to_string(), vec![value(urn, b"x")]);
            match RunArgumentLedger::new(&plan, initial) {
                Err(RunArgumentError::ArgumentBound { token_id, media_urn }) => {
                    assert_eq!((token_id.as_str(), media_urn.as_str()), (token, urn));
                }
                other => panic!("{token} {urn}: expected ArgumentBound, got {other:?}"),
            }
        };
        refused_at_creation("tok_a", "media:numeric;width"); // a constant
        refused_at_creation("tok_a", "media:width;numeric"); // the same argument, tags in another order
        refused_at_creation("tok_a", "media:image"); // its main input, wired from the input slot
        refused_at_creation("tok_b", "media:image;mask"); // a node wired into a named argument
        refused_at_creation("tok_b", "media:image"); // its main input, wired from tok_a

        let ledger = RunArgumentLedger::new(&plan, HashMap::new()).unwrap();
        let update = |token: &str, urn: &str, bytes: &[u8]| ArgumentUpdate {
            token_id: token.to_string(),
            media_urn: urn.to_string(),
            value: bytes.to_vec(),
        };
        // One bound argument refuses the whole batch: the unbound one beside
        // it is not applied either.
        let err = ledger
            .apply(&[
                update("tok_a", "media:numeric;height", b"5"),
                update("tok_a", "media:numeric;width", b"99"),
            ])
            .unwrap_err();
        assert!(matches!(err, RunArgumentError::ArgumentBound { .. }), "{err}");
        assert_eq!(ledger.revision(), 0);
        assert!(matches!(
            ledger.apply(&[update("tok_b", "media:image;mask", b"m")]),
            Err(RunArgumentError::ArgumentBound { .. })
        ));

        // What the machine left unbound is the run's to give and to change.
        let applied = ledger.apply(&[update("tok_a", "media:numeric;height", b"5")]).unwrap();
        assert_eq!(applied.outcomes[0].disposition, ArgumentUpdateDisposition::Applied);
        let snapshot = ledger.dispatch_steps(&["tok_a", "tok_b"]);
        assert!(snapshot["tok_a"].contains(&value("media:numeric;height", b"5")));
        // The constant is what it was.
        assert!(snapshot["tok_a"].contains(&value("media:numeric;width", b"20")));
        assert!(!snapshot["tok_a"].iter().any(|(_, bytes)| bytes == b"99"));
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

        let snapshot = ledger.dispatch_steps(&["tok_a"]);
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

        let first = ledger.dispatch_steps(&["tok_a"]);
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
            ledger.dispatch_steps(&["tok_a"])["tok_a"],
            vec![value("media:numeric;width", b"3")]
        );
    }

    // TEST1472: a step inside a map reports how many runs of it took the old
    // value; the next run reads the new one; once no further run can be
    // dispatched, later updates are already-dispatched.
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
            ledger.dispatch_steps_in_map(&["tok_body"])["tok_body"],
            vec![value("media:enc=utf-8;question", b"old")]
        );
        let second = ledger.dispatch_steps_in_map(&["tok_body"]);
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
            ledger.dispatch_steps_in_map(&["tok_body"])["tok_body"],
            vec![value("media:enc=utf-8;question", b"new")]
        );

        // The map has ended: no more runs — later updates are honest about it.
        ledger.exhaust_steps(&["tok_body"]);
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
        let snapshot = ledger.dispatch_steps(&["tok_a"]);
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
            ledger.dispatch_steps(&["tok_a"])["tok_a"],
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
