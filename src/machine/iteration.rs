//! Which steps of a machine run once per item, and inside which map.
//!
//! Notation never writes iteration: it follows from cardinality (docs 09
//! §Iteration). This module is the one place that rule is computed. The
//! resolver asks it which steps map; the plan builder asks it where every
//! step runs. Both give it the same shapes and differ only in what they know
//! about the machine's inputs: notation has no run, so its inputs are single
//! values, while a run says which of them it binds a sequence to.
//!
//! ## The rule
//!
//! Every node lives in a **scope**: the machine itself, or a map. A node in a
//! map has one value per run of that map. Seen from outside the map it is the
//! sequence of those values in item order — and when the map is itself inside
//! another, seen from outside both it is one flat sequence.
//!
//! A **map** is what it iterates: the sequences it zips, in the scope it is
//! opened in. Two steps that map over the same sequences in the same scope
//! run in the same map, so what each produces for an item can be given to a
//! third step for that item.
//!
//! A step is first **placed** in the deepest scope its members ask for, when
//! those scopes lie on one line of nesting; when they do not, in the scope
//! that encloses them all. What a member asks for:
//!
//! - bound to an arg that takes one value: the scope the node lives in;
//! - a sequence bound to an arg that takes a sequence: the scope it lives in —
//!   the step is given that sequence whole;
//! - a single value bound to an arg that takes a sequence: the scope that
//!   **encloses** the node's map — the step folds what the map collected. (A
//!   single value of the machine itself asks for the machine.)
//!
//! From its placement, each member bound to an arg that takes one value is
//! one of:
//!
//! - a single value there: given as it is;
//! - the sequence a map the placement is inside iterates: given as that map's
//!   item — the step **joins** the map;
//! - a sequence that lives in the placement, or was collected into it:
//!   **mapped** — the step runs in the map over it, opened inside the
//!   placement. Several mapped members are zipped;
//! - a sequence from outside that no enclosing map iterates. Mapping it here
//!   would run every item of it for every item of the enclosing map: a cross
//!   product nobody wrote. The step is placed further out instead — where
//!   that sequence lives — and what it was given from the inner map is
//!   collected and zipped with it.

use std::collections::{HashMap, HashSet};

use super::graph::NodeId;

/// A scope of iteration: the machine itself ([`ROOT`]) or one map.
pub type ScopeId = usize;

/// The machine itself: what is not inside any map.
pub const ROOT: ScopeId = 0;

/// One node a step is given, and what the arg it is bound to takes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberShape {
    pub node: NodeId,
    /// The arg takes a sequence (a member alone on it is given whole; several
    /// on it are gathered).
    pub arg_is_sequence: bool,
}

/// A step, as far as iteration is concerned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepShape {
    /// The nodes the step is given. A constant is not a member: it is one
    /// value wherever the step runs.
    pub members: Vec<MemberShape>,
    /// Each node the step produces, and whether the output that produces it
    /// is declared a sequence.
    pub outputs: Vec<(NodeId, bool)>,
}

/// Where a step runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepPlacement {
    /// The scope the step is placed in: where it is given its members.
    pub placed_in: ScopeId,
    /// The map the step runs in, when it maps: opened inside its placement.
    pub map: Option<ScopeId>,
    /// The members the step maps over, zipped: distinct nodes, in the order
    /// the step lists them. Empty exactly when `map` is `None`.
    pub mapped: Vec<NodeId>,
}

impl StepPlacement {
    /// The scope the step's invocations run in, and its outputs live in: the
    /// map it maps in, or its placement.
    pub fn scope(&self) -> ScopeId {
        self.map.unwrap_or(self.placed_in)
    }
}

/// The steps a machine cannot place because they wait on one another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CyclicSteps(pub Vec<usize>);

#[derive(Debug, Clone)]
struct Scope {
    /// The scope that encloses this one; the machine encloses itself.
    parent: ScopeId,
    /// The sequences a map zips, sorted; empty for the machine.
    sources: Vec<NodeId>,
    /// The steps that map in it, by index.
    mappers: Vec<usize>,
}

/// The scopes of a machine and the place of every step and node in them.
#[derive(Debug, Clone)]
pub struct IterationStructure {
    scopes: Vec<Scope>,
    steps: Vec<StepPlacement>,
    node_scope: HashMap<NodeId, ScopeId>,
    node_is_sequence: HashMap<NodeId, bool>,
}

impl IterationStructure {
    /// Place every step. `input_is_sequence` says, for a node no step
    /// produces, whether it holds a sequence.
    pub fn of(
        steps: &[StepShape],
        input_is_sequence: impl Fn(NodeId) -> bool,
    ) -> Result<Self, CyclicSteps> {
        let produced: HashSet<NodeId> = steps
            .iter()
            .flat_map(|step| step.outputs.iter().map(|(node, _)| *node))
            .collect();

        let mut structure = Self {
            scopes: vec![Scope {
                parent: ROOT,
                sources: Vec::new(),
                mappers: Vec::new(),
            }],
            steps: Vec::new(),
            node_scope: HashMap::new(),
            node_is_sequence: HashMap::new(),
        };
        // An input of the machine lives in the machine.
        for step in steps {
            for member in &step.members {
                if !produced.contains(&member.node) {
                    structure.node_scope.insert(member.node, ROOT);
                    structure
                        .node_is_sequence
                        .insert(member.node, input_is_sequence(member.node));
                }
            }
        }

        // A step is placed once every node it is given has a place. The order
        // steps are placed in does not change where any of them lands: a
        // placement reads only its members, and a map is found by what it
        // iterates.
        let mut placed: Vec<Option<StepPlacement>> = vec![None; steps.len()];
        let mut remaining = steps.len();
        while remaining > 0 {
            let before = remaining;
            for (index, step) in steps.iter().enumerate() {
                if placed[index].is_some()
                    || !step
                        .members
                        .iter()
                        .all(|member| structure.node_scope.contains_key(&member.node))
                {
                    continue;
                }
                let placement = structure.place(index, step);
                for (node, declared_sequence) in &step.outputs {
                    structure.node_scope.insert(*node, placement.scope());
                    structure.node_is_sequence.insert(*node, *declared_sequence);
                }
                placed[index] = Some(placement);
                remaining -= 1;
            }
            if remaining == before {
                return Err(CyclicSteps(
                    (0..steps.len()).filter(|index| placed[*index].is_none()).collect(),
                ));
            }
        }
        structure.steps = placed.into_iter().map(|p| p.expect("every step placed")).collect();
        for scope in &mut structure.scopes {
            scope.mappers.sort_unstable();
        }
        Ok(structure)
    }

    fn place(&mut self, index: usize, step: &StepShape) -> StepPlacement {
        // What each member asks for.
        let asked: Vec<ScopeId> = step
            .members
            .iter()
            .map(|member| {
                let scope = self.node_scope[&member.node];
                if member.arg_is_sequence && !self.node_is_sequence[&member.node] {
                    self.scopes[scope].parent
                } else {
                    scope
                }
            })
            .collect();
        let deepest = asked
            .iter()
            .copied()
            .max_by_key(|scope| self.depth(*scope))
            .unwrap_or(ROOT);
        let mut placed_in = if asked.iter().all(|scope| self.encloses(*scope, deepest)) {
            deepest
        } else {
            asked
                .iter()
                .copied()
                .reduce(|a, b| self.common_scope(a, b))
                .unwrap_or(ROOT)
        };

        // What each member bound to an arg that takes one value is from
        // there. A sequence from outside that no enclosing map iterates moves
        // the placement out to where it lives, and the members are read again.
        let mapped = loop {
            let mut mapped: Vec<NodeId> = Vec::new();
            let mut move_out_to: Option<ScopeId> = None;
            for member in step.members.iter().filter(|member| !member.arg_is_sequence) {
                let lives_in = self.node_scope[&member.node];
                if self.encloses(lives_in, placed_in) {
                    if !self.node_is_sequence[&member.node] {
                        continue; // one value here
                    }
                    if lives_in == placed_in {
                        // A sequence that lives here: mapped.
                    } else if self.iterated_between(member.node, placed_in, lives_in) {
                        continue; // the item of a map the step is inside
                    } else {
                        move_out_to = Some(lives_in);
                        break;
                    }
                }
                // Otherwise the node lives in a map the placement is not
                // inside, and the placement encloses that map (it encloses
                // every scope a member asked for): the node is seen here as
                // what the map collected — a sequence, mapped.
                if !mapped.contains(&member.node) {
                    mapped.push(member.node);
                }
            }
            match move_out_to {
                Some(scope) => placed_in = scope,
                None => break mapped,
            }
        };

        let map = if mapped.is_empty() {
            None
        } else {
            let mut sources = mapped.clone();
            sources.sort_unstable();
            let existing = self
                .scopes
                .iter()
                .position(|scope| scope.parent == placed_in && scope.sources == sources);
            let id = existing.unwrap_or_else(|| {
                self.scopes.push(Scope {
                    parent: placed_in,
                    sources,
                    mappers: Vec::new(),
                });
                self.scopes.len() - 1
            });
            self.scopes[id].mappers.push(index);
            Some(id)
        };
        StepPlacement {
            placed_in,
            map,
            mapped,
        }
    }

    /// Whether a map from `inner` outward, short of `outer`, iterates `node`.
    fn iterated_between(&self, node: NodeId, inner: ScopeId, outer: ScopeId) -> bool {
        let mut at = inner;
        while at != outer {
            if self.scopes[at].sources.contains(&node) {
                return true;
            }
            at = self.scopes[at].parent;
        }
        false
    }

    /// Where each step runs, in the order the steps were given.
    pub fn steps(&self) -> &[StepPlacement] {
        &self.steps
    }

    /// How many scopes there are, the machine included.
    pub fn scope_count(&self) -> usize {
        self.scopes.len()
    }

    /// The scope that encloses `scope`. The machine encloses itself.
    pub fn parent(&self, scope: ScopeId) -> ScopeId {
        self.scopes[scope].parent
    }

    /// The sequences map `scope` zips, in a fixed order; none for the machine.
    pub fn sources(&self, scope: ScopeId) -> &[NodeId] {
        &self.scopes[scope].sources
    }

    /// The first step that maps in `scope`: the one the map is opened by.
    /// `None` for the machine.
    pub fn opened_by(&self, scope: ScopeId) -> Option<usize> {
        self.scopes[scope].mappers.first().copied()
    }

    /// The map that gives a step running in `from` the ITEM of `node`: the
    /// innermost map at or around `from` that iterates it. `None` when no map
    /// the step is inside iterates the node — the step is then given it whole.
    pub fn item_map(&self, node: NodeId, from: ScopeId) -> Option<ScopeId> {
        let mut at = from;
        while at != ROOT {
            if self.scopes[at].sources.contains(&node) {
                return Some(at);
            }
            at = self.scopes[at].parent;
        }
        None
    }

    /// The scope a node lives in.
    pub fn node_scope(&self, node: NodeId) -> ScopeId {
        self.node_scope[&node]
    }

    /// How many maps enclose `scope`, itself included.
    pub fn depth(&self, scope: ScopeId) -> usize {
        let mut depth = 0;
        let mut at = scope;
        while at != ROOT {
            at = self.scopes[at].parent;
            depth += 1;
        }
        depth
    }

    /// Whether `outer` is `inner` or encloses it.
    pub fn encloses(&self, outer: ScopeId, inner: ScopeId) -> bool {
        let mut at = inner;
        loop {
            if at == outer {
                return true;
            }
            if at == ROOT {
                return false;
            }
            at = self.scopes[at].parent;
        }
    }

    /// The deepest scope that encloses both.
    pub fn common_scope(&self, a: ScopeId, b: ScopeId) -> ScopeId {
        let mut at = a;
        while !self.encloses(at, b) {
            at = self.scopes[at].parent;
        }
        at
    }

    /// Whether `node` is a sequence as a step placed in `from` is given it:
    /// what it is in its own scope when that scope encloses `from`, and the
    /// collected sequence otherwise.
    pub fn is_sequence_from(&self, node: NodeId, from: ScopeId) -> bool {
        if self.encloses(self.node_scope[&node], from) {
            self.node_is_sequence[&node]
        } else {
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(node: NodeId, arg_is_sequence: bool) -> MemberShape {
        MemberShape {
            node,
            arg_is_sequence,
        }
    }

    fn step(members: Vec<MemberShape>, outputs: Vec<(NodeId, bool)>) -> StepShape {
        StepShape { members, outputs }
    }

    fn placed(steps: &[StepShape], sequences: &[NodeId]) -> IterationStructure {
        IterationStructure::of(steps, |node| sequences.contains(&node)).expect("acyclic")
    }

    fn once(scope: ScopeId) -> StepPlacement {
        StepPlacement {
            placed_in: scope,
            map: None,
            mapped: vec![],
        }
    }

    // TEST12746: a step given single values runs once, in the machine; a step
    // given a sequence on an arg that takes one value maps over it, and what
    // it produces lives in that map. A sequence on an arg that takes a
    // sequence is given whole and maps nothing.
    #[test]
    fn test12746_a_sequence_on_a_scalar_arg_opens_a_map() {
        // 0 -> a -> 1 ; 1 is a declared sequence ; 1 -> b -> 2 ; 1 -> count(seq arg) -> 3
        let steps = [
            step(vec![member(0, false)], vec![(1, true)]),
            step(vec![member(1, false)], vec![(2, false)]),
            step(vec![member(1, true)], vec![(3, false)]),
        ];
        let s = placed(&steps, &[]);
        assert_eq!(s.steps()[0], once(ROOT));
        let map = s.steps()[1].map.expect("the step maps");
        assert_eq!(s.steps()[1].placed_in, ROOT);
        assert_eq!(s.steps()[1].mapped, vec![1]);
        assert_eq!(s.parent(map), ROOT);
        assert_eq!(s.sources(map), &[1]);
        assert_eq!(s.opened_by(map), Some(1));
        assert_eq!(s.node_scope(2), map);
        assert_eq!(s.node_scope(1), ROOT);
        assert_eq!(s.steps()[2], once(ROOT));
        assert_eq!(s.scope_count(), 2);

        // The same machine with a single value where the sequence was: no map.
        let steps = [
            step(vec![member(0, false)], vec![(1, false)]),
            step(vec![member(1, false)], vec![(2, false)]),
        ];
        let s = placed(&steps, &[]);
        assert!(s.steps().iter().all(|p| *p == once(ROOT)));
        // And a run that binds the machine's input to a sequence maps the
        // first step over it, the second joining that map.
        let s = placed(&steps, &[0]);
        let map = s.steps()[0].map.expect("the input is mapped");
        assert_eq!(s.steps()[0].mapped, vec![0]);
        assert_eq!(s.steps()[1], once(map));
        assert_eq!(s.node_scope(2), map);
    }

    // TEST12747: a step given a value a map produces joins that map; a step
    // that takes a sequence and is given such a value folds what the map
    // collected, in the scope that encloses the map. Where a step lands does
    // not depend on the order the steps are given in.
    #[test]
    fn test12747_a_step_joins_the_map_its_value_is_produced_in_and_a_fold_closes_it() {
        // 0 (sequence) -> ocr -> 1 ; 1 -> summarize -> 2 ; 2 -> join(seq arg) -> 3 ; 3 -> title -> 4
        let steps = [
            step(vec![member(0, false)], vec![(1, false)]),
            step(vec![member(1, false)], vec![(2, false)]),
            step(vec![member(2, true)], vec![(3, false)]),
            step(vec![member(3, false)], vec![(4, false)]),
        ];
        let s = placed(&steps, &[0]);
        let map = s.steps()[0].map.expect("mapped over the sequence");
        assert_eq!(s.steps()[1], once(map));
        assert_eq!(s.steps()[2], once(ROOT));
        assert_eq!(s.steps()[3], once(ROOT));
        assert_eq!(s.node_scope(3), ROOT);

        let reordered = [steps[3].clone(), steps[2].clone(), steps[1].clone(), steps[0].clone()];
        let r = placed(&reordered, &[0]);
        let map = r.steps()[3].map.expect("mapped over the sequence");
        assert_eq!(r.steps()[2], once(map));
        assert_eq!(r.steps()[1], once(ROOT));
        assert_eq!(r.steps()[0], once(ROOT));
        assert_eq!(r.opened_by(map), Some(3));
    }

    // TEST12748: several sequences on args that take one value are zipped by
    // one map; a single value beside them is given whole and does not move
    // the step. The same node on two such args is one zipped member.
    #[test]
    fn test12748_several_mapped_members_open_one_map() {
        // 0, 1 sequences; 2 a single value. (1, b=0, c=2) -> compare -> 3
        let steps = [step(
            vec![member(1, false), member(0, false), member(2, false)],
            vec![(3, false)],
        )];
        let s = placed(&steps, &[0, 1]);
        assert_eq!(s.steps()[0].placed_in, ROOT);
        assert_eq!(s.steps()[0].mapped, vec![1, 0], "in the order the step lists them");
        let map = s.steps()[0].map.unwrap();
        assert_eq!(s.sources(map), &[0, 1], "a map's sources have one fixed order");
        assert_eq!(s.scope_count(), 2);

        let twice = [step(vec![member(0, false), member(0, false)], vec![(1, false)])];
        assert_eq!(placed(&twice, &[0]).steps()[0].mapped, vec![0]);
    }

    // TEST12749: a sequence produced inside a map and bound to an arg that
    // takes one value opens a map inside that map. A fold of what the inner
    // map produces is placed in the outer map: one value per outer item. From
    // the machine the inner value is one flat sequence.
    #[test]
    fn test12749_a_map_nests_and_a_fold_closes_one_level() {
        // 0 docs (sequence) -> split -> 1 pages (declared sequence, per doc)
        // 1 -> ocr -> 2 text ; 2 -> join(seq arg) -> 3 doc text ; 3 -> all(seq arg) -> 4
        let steps = [
            step(vec![member(0, false)], vec![(1, true)]),
            step(vec![member(1, false)], vec![(2, false)]),
            step(vec![member(2, true)], vec![(3, false)]),
            step(vec![member(3, true)], vec![(4, false)]),
        ];
        let s = placed(&steps, &[0]);
        let outer = s.steps()[0].map.expect("mapped over docs");
        let inner = s.steps()[1].map.expect("mapped over each doc's pages");
        assert_eq!(s.steps()[1].placed_in, outer);
        assert_eq!(s.parent(inner), outer);
        assert_eq!(s.depth(inner), 2);
        assert_eq!(s.steps()[2], once(outer));
        assert_eq!(s.steps()[3], once(ROOT));
        assert!(s.is_sequence_from(2, ROOT));
        assert!(s.is_sequence_from(2, outer));
        assert!(!s.is_sequence_from(2, inner));

        // A step of the inner map that is also given the sequence the OUTER
        // map iterates takes the outer item — the doc its page came from —
        // from inside the inner map, two levels down.
        // (1, source=0) -> stamp -> 5
        let mut steps = steps.to_vec();
        steps.push(step(vec![member(1, false), member(0, false)], vec![(5, false)]));
        let s = placed(&steps, &[0]);
        let outer = s.steps()[0].map.unwrap();
        let inner = s.steps()[1].map.unwrap();
        assert_eq!(s.steps()[4].placed_in, outer);
        assert_eq!(s.steps()[4].map, Some(inner), "it maps over the same pages: the same inner map");
        assert_eq!(s.steps()[4].mapped, vec![1]);
        assert_eq!(s.item_map(0, inner), Some(outer), "the outer map's item, seen from the inner one");
        assert_eq!(s.item_map(1, inner), Some(inner));
        assert_eq!(s.item_map(1, outer), None, "from the outer map the pages are a sequence, given whole");
    }

    // TEST12750: two steps that map over the same sequence run in one map, so
    // a third step given what each produced is given both for the same item —
    // it joins the map and zips nothing. A step given a value of the map and
    // the sequence the map iterates joins it too, taking the item.
    #[test]
    fn test12750_steps_over_one_sequence_share_a_map() {
        // 0 docs (sequence). 0 -> text -> 1 ; 0 -> meta -> 2 ; (1, meta=2) -> index -> 3
        // (1, original=0) -> compare -> 4
        let steps = [
            step(vec![member(0, false)], vec![(1, false)]),
            step(vec![member(0, false)], vec![(2, false)]),
            step(vec![member(1, false), member(2, false)], vec![(3, false)]),
            step(vec![member(1, false), member(0, false)], vec![(4, false)]),
        ];
        let s = placed(&steps, &[0]);
        let map = s.steps()[0].map.unwrap();
        assert_eq!(s.steps()[1].map, Some(map), "the same map, not a second one over docs");
        assert_eq!(s.opened_by(map), Some(0));
        assert_eq!(s.scope_count(), 2);
        assert_eq!(s.steps()[2], once(map));
        assert_eq!(s.steps()[3], once(map));
        // Inside the map the sequence it iterates is its item; a value the map
        // produces is not an item of anything, and from the machine the
        // sequence is given whole.
        assert_eq!(s.item_map(0, map), Some(map));
        assert_eq!(s.item_map(1, map), None);
        assert_eq!(s.item_map(0, ROOT), None);
    }

    // TEST12751: there is no cross product. A step given a value of a map and
    // a sequence the map does not iterate is not run for every pair: it is
    // placed where that sequence lives, and the map's values are collected
    // and zipped with it. Values of two maps that are not nested are zipped
    // the same way, where both are seen.
    #[test]
    fn test12751_an_outside_sequence_is_zipped_never_crossed() {
        // 0 files, 5 keywords: sequences. 0 -> ocr -> 1 ; (1, k=5) -> tag -> 2
        let steps = [
            step(vec![member(0, false)], vec![(1, false)]),
            step(vec![member(1, false), member(5, false)], vec![(2, false)]),
        ];
        let s = placed(&steps, &[0, 5]);
        let first = s.steps()[0].map.unwrap();
        let zip = s.steps()[1].map.expect("the collected values and the keywords are zipped");
        assert_eq!(s.steps()[1].placed_in, ROOT, "not inside the map over files");
        assert_eq!(s.steps()[1].mapped, vec![1, 5]);
        assert_eq!(s.parent(zip), ROOT);
        assert_ne!(zip, first);

        // 0, 1 sequences. 0 -> a -> 2 ; 1 -> b -> 3 ; (2, other=3) -> compare -> 4
        let steps = [
            step(vec![member(0, false)], vec![(2, false)]),
            step(vec![member(1, false)], vec![(3, false)]),
            step(vec![member(2, false), member(3, false)], vec![(4, false)]),
        ];
        let s = placed(&steps, &[0, 1]);
        let a = s.steps()[0].map.unwrap();
        let b = s.steps()[1].map.unwrap();
        assert_ne!(a, b);
        assert_eq!(s.steps()[2].placed_in, ROOT);
        assert_eq!(s.steps()[2].mapped, vec![2, 3]);
        assert_eq!(s.common_scope(a, b), ROOT);

        // Inside a nested map the same holds one level down: a page's text
        // and a sequence of the machine are zipped in the machine, not run
        // once per page per item.
        // 0 docs (sequence), 9 stamps (sequence). 0 -> split -> 1 (declared sequence)
        // 1 -> ocr -> 2 ; (2, stamp=9) -> mark -> 3
        let steps = [
            step(vec![member(0, false)], vec![(1, true)]),
            step(vec![member(1, false)], vec![(2, false)]),
            step(vec![member(2, false), member(9, false)], vec![(3, false)]),
        ];
        let s = placed(&steps, &[0, 9]);
        assert_eq!(s.steps()[2].placed_in, ROOT);
        assert_eq!(s.steps()[2].mapped, vec![2, 9]);
    }

    // TEST12793: where a step lands does not depend on the order its members
    // are listed in. A value of one map bound to an arg that takes one value,
    // beside a sequence of an unrelated map bound to an arg that takes a
    // sequence: neither map contains the other, so the step is placed where
    // both are seen — mapping over the first, given the second whole —
    // whichever is listed first.
    #[test]
    fn test12793_placement_does_not_depend_on_member_order() {
        // 0, 1 sequences. 0 -> a -> 2 (one value per item of 0)
        // 1 -> b -> 3 (a declared sequence per item of 1)
        // (2, all=3) -> t -> 4, and the same with its members swapped.
        let producers = [
            step(vec![member(0, false)], vec![(2, false)]),
            step(vec![member(1, false)], vec![(3, true)]),
        ];
        for members in [
            vec![member(2, false), member(3, true)],
            vec![member(3, true), member(2, false)],
        ] {
            let mut steps = producers.to_vec();
            steps.push(step(members, vec![(4, false)]));
            let s = placed(&steps, &[0, 1]);
            assert_eq!(s.steps()[2].placed_in, ROOT, "{:?}", steps[2].members);
            assert_eq!(s.steps()[2].mapped, vec![2]);
            let map = s.steps()[2].map.unwrap();
            assert_ne!(Some(map), s.steps()[0].map, "not the map the value was produced in");
            assert_eq!(s.parent(map), ROOT);
        }
    }

    // TEST12752: steps that wait on one another cannot be placed, and the
    // refusal names them.
    #[test]
    fn test12752_steps_on_a_cycle_are_named() {
        let steps = [
            step(vec![member(0, false)], vec![(1, false)]),
            step(vec![member(3, false)], vec![(2, false)]),
            step(vec![member(2, false)], vec![(3, false)]),
        ];
        assert_eq!(
            IterationStructure::of(&steps, |_| false).unwrap_err(),
            CyclicSteps(vec![1, 2])
        );
    }
}
