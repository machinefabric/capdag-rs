//! Which of a run's sources each file of its outputs descends from, and what
//! the file is called for it.
//!
//! A run binds its sources to the inputs of its plan; an output descends from
//! the sources its path through the plan reads. Inside a map, one run of the
//! body reads one item of each sequence it maps over: a file written by run
//! `i` descends from item `i` of an input the map reads directly, and from the
//! whole of every value the body reads from outside the map. A file is called
//! after what it descends from ([`lineage_title`]), and the items of a
//! sequence that descend from one thing are told apart by their position
//! among them ([`sequence_item_titles`]). Every front-end names what a run
//! makes this one way.

use crate::orchestrator::execute_plan::ForEachBodyCoordinate;
use crate::planner::{ExecutionNodeType, MachinePlan};
use std::collections::{HashMap, HashSet};

/// The run sources of one run of a plan, as its inputs were given them.
pub struct RunLineage<'a> {
    plan: &'a MachinePlan,
    /// Each input slot's sources, in the order the run gives them to it.
    slot_sources: HashMap<&'a str, Vec<&'a str>>,
    /// Every source of the run, in realization order.
    order: HashMap<&'a str, usize>,
    /// The map node each map token names.
    map_by_token: HashMap<String, &'a str>,
}

impl<'a> RunLineage<'a> {
    /// `input_slots` is what the run gives each input slot, as positions in
    /// the run's sources; `run_source_ids` are those sources, by position.
    pub fn new(
        plan: &'a MachinePlan,
        input_slots: &'a [(String, Vec<usize>)],
        run_source_ids: &'a [String],
    ) -> Result<Self, String> {
        let mut slot_sources: HashMap<&str, Vec<&str>> = HashMap::new();
        for (slot, positions) in input_slots {
            match plan.get_node(slot).map(|node| &node.node_type) {
                Some(ExecutionNodeType::InputSlot { .. }) => {}
                _ => {
                    return Err(format!(
                        "the run gives sources to '{}', which is not an input of plan '{}'",
                        slot, plan.name
                    ))
                }
            }
            let sources = positions
                .iter()
                .map(|&position| {
                    run_source_ids.get(position).map(String::as_str).ok_or_else(|| {
                        format!(
                            "input '{}' is given source {} of a run that realized {}",
                            slot,
                            position,
                            run_source_ids.len()
                        )
                    })
                })
                .collect::<Result<Vec<&str>, String>>()?;
            if slot_sources.insert(slot.as_str(), sources).is_some() {
                return Err(format!("the run gives input '{}' sources twice", slot));
            }
        }
        let map_by_token = plan
            .nodes
            .iter()
            .filter_map(|(id, node)| match &node.node_type {
                ExecutionNodeType::ForEach { token_id, .. } => {
                    Some((token_id.to_string(), id.as_str()))
                }
                _ => None,
            })
            .collect();
        let order = run_source_ids
            .iter()
            .enumerate()
            .map(|(position, id)| (id.as_str(), position))
            .collect();
        Ok(Self { plan, slot_sources, order, map_by_token })
    }

    /// The sources a file of output `output_node_id` descends from, in
    /// realization order: none for an output that reads no input.
    /// `coordinate` is the run of a map's body that wrote the file.
    pub fn sources_of(
        &self,
        output_node_id: &str,
        coordinate: Option<&ForEachBodyCoordinate>,
    ) -> Result<Vec<String>, String> {
        let mut runs: HashMap<&str, usize> = HashMap::new();
        if let Some(coordinate) = coordinate {
            let path = coordinate.enclosing.iter().map(|run| (&run.foreach_token_id, run.body_index)).chain(
                std::iter::once((&coordinate.foreach_token_id, coordinate.body_index)),
            );
            for (token, index) in path {
                let map = self.map_by_token.get(token.as_str()).ok_or_else(|| {
                    format!(
                        "a file of output '{}' was written by run {} of map '{}', which plan \
                         '{}' does not have",
                        output_node_id, index, token, self.plan.name
                    )
                })?;
                runs.insert(map, index);
            }
        }

        let mut sources: HashSet<&str> = HashSet::new();
        let mut seen: HashSet<&str> = HashSet::new();
        let mut pending: Vec<&str> = vec![output_node_id];
        while let Some(node_id) = pending.pop() {
            if !seen.insert(node_id) {
                continue;
            }
            let node = self.plan.get_node(node_id).ok_or_else(|| {
                format!("plan '{}' has no node '{}'", self.plan.name, node_id)
            })?;
            match &node.node_type {
                ExecutionNodeType::InputSlot { .. } => {
                    sources.extend(self.given(node_id)?.iter().copied());
                }
                ExecutionNodeType::Item { foreach, source, .. } => {
                    let source_is_input = matches!(
                        self.plan.get_node(source).map(|n| &n.node_type),
                        Some(ExecutionNodeType::InputSlot { .. })
                    );
                    match runs.get(foreach.as_str()) {
                        Some(&index) if source_is_input => {
                            sources.insert(self.item_source(source, index)?);
                        }
                        // An item of a value the machine made, or an item
                        // read outside one run of its map: the whole value.
                        _ => pending.push(source),
                    }
                }
                ExecutionNodeType::Cap { .. } => {
                    pending.extend(
                        self.plan
                            .edges
                            .iter()
                            .filter(|edge| edge.to_node == node_id)
                            .map(|edge| edge.from_node.as_str()),
                    );
                }
                ExecutionNodeType::CapOutput { cap_node, .. } => pending.push(cap_node),
                ExecutionNodeType::Collect { input_nodes, .. } => {
                    pending.extend(input_nodes.iter().map(String::as_str));
                }
                ExecutionNodeType::Output { source_node, .. } => pending.push(source_node),
                ExecutionNodeType::ForEach { sources: mapped, .. } => {
                    pending.extend(mapped.iter().map(|source| source.node.as_str()));
                }
            }
        }

        let mut ordered: Vec<&str> = sources.into_iter().collect();
        ordered.sort_by_key(|id| self.order[id]);
        Ok(ordered.into_iter().map(str::to_string).collect())
    }

    fn given(&self, slot: &str) -> Result<&Vec<&'a str>, String> {
        self.slot_sources.get(slot).ok_or_else(|| {
            format!("input '{}' of plan '{}' is given nothing by the run", slot, self.plan.name)
        })
    }

    /// The source item `index` of an input is read from: one source holds
    /// every item it is given (a file read as a sequence, a live capture);
    /// several are one item each.
    fn item_source(&self, slot: &str, index: usize) -> Result<&'a str, String> {
        match self.given(slot)?.as_slice() {
            [only] => Ok(only),
            many => many.get(index).copied().ok_or_else(|| {
                format!(
                    "a run of a map read item {} of input '{}', which was given {} sources",
                    index,
                    slot,
                    many.len()
                )
            }),
        }
    }
}

/// What an output file is called, from the titles of the sources it
/// descends from: their names joined, or the machine's codename for a file
/// that reads no input.
pub fn lineage_title(source_titles: &[&str], machine_codename: &str) -> String {
    if source_titles.is_empty() {
        machine_codename.to_string()
    } else {
        source_titles.join(" + ")
    }
}

/// What each item of a sequence output is called, given what its lineage
/// calls it. An item whose lineage no other item shares is called that; items
/// that share one — the pages of one document — are told apart by their
/// position among them.
pub fn sequence_item_titles(lineage_titles: &[String]) -> Vec<String> {
    let mut shared: HashMap<&str, usize> = HashMap::new();
    for title in lineage_titles {
        *shared.entry(title.as_str()).or_default() += 1;
    }
    let mut position: HashMap<&str, usize> = HashMap::new();
    lineage_titles
        .iter()
        .map(|title| {
            if shared[title.as_str()] == 1 {
                return title.clone();
            }
            let k = position.entry(title.as_str()).or_default();
            *k += 1;
            format!("{} · {}", title, k)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{ForEachSource, InputCardinality, MachineNode, MachinePlanEdge, MapRun, StepToken};

    fn token(name: &str) -> StepToken {
        name.parse().unwrap()
    }

    fn run_of(map: &str, index: usize, enclosing: Vec<(&str, usize)>) -> ForEachBodyCoordinate {
        ForEachBodyCoordinate {
            foreach_token_id: token(map),
            body_index: index,
            enclosing: enclosing
                .into_iter()
                .map(|(map, index)| MapRun { foreach_token_id: token(map), body_index: index })
                .collect(),
        }
    }

    /// `(docs, reference=ref)`: every doc is diffed against the reference in a
    /// map; the diffs are gathered into a report; a stamp reads no input. Each
    /// doc is split into pages in a map nested in the first, each page
    /// rendered.
    fn compare_plan() -> MachinePlan {
        let mut plan = MachinePlan::new("compare");
        plan.add_node(MachineNode::input_slot("in_docs", "docs", "media:pdf", InputCardinality::Sequence));
        plan.add_node(MachineNode::input_slot("in_ref", "reference", "media:pdf", InputCardinality::Single));
        plan.add_node(MachineNode::for_each_token(
            "map_docs",
            vec![ForEachSource { node: "in_docs".into(), item: "doc".into() }],
            vec!["doc".into(), "diff".into(), "split".into(), "map_pages".into(), "page".into(), "render".into()],
            token("tok-docs"),
        ));
        plan.add_node(MachineNode::item("doc", "map_docs", "in_docs", "media:pdf"));
        plan.add_node(MachineNode::cap("diff", "cap:in=media:pdf;diff;out=media:txt"));
        plan.add_edge(MachinePlanEdge::direct("doc", "diff"));
        plan.add_edge(MachinePlanEdge::arg("in_ref", "diff", "media:pdf;reference"));
        plan.add_node(MachineNode::output("out_diffs", "diffs", "diff"));
        plan.add_edge(MachinePlanEdge::direct("diff", "out_diffs"));

        plan.add_node(MachineNode::cap("split", "cap:in=media:pdf;split;out=media:png"));
        plan.add_edge(MachinePlanEdge::direct("doc", "split"));
        plan.add_node(MachineNode::for_each_token(
            "map_pages",
            vec![ForEachSource { node: "split".into(), item: "page".into() }],
            vec!["page".into(), "render".into()],
            token("tok-pages"),
        ));
        plan.add_node(MachineNode::item("page", "map_pages", "split", "media:png"));
        plan.add_node(MachineNode::cap("render", "cap:in=media:png;render;out=media:jpg"));
        plan.add_edge(MachinePlanEdge::direct("page", "render"));
        plan.add_node(MachineNode::output("out_pages", "pages", "render"));
        plan.add_edge(MachinePlanEdge::direct("render", "out_pages"));

        plan.add_node(MachineNode::collect("gather", vec!["diff".into()], "media:txt"));
        plan.add_edge(MachinePlanEdge::collection("diff", "gather"));
        plan.add_node(MachineNode::cap("report", "cap:in=media:txt;report;out=media:md"));
        plan.add_edge(MachinePlanEdge::direct("gather", "report"));
        plan.add_node(MachineNode::output("out_report", "report", "report"));
        plan.add_edge(MachinePlanEdge::direct("report", "out_report"));

        plan.add_node(MachineNode::cap("stamp", "cap:in=media:void;stamp;out=media:txt"));
        plan.add_node(MachineNode::output("out_stamp", "stamp", "stamp"));
        plan.add_edge(MachinePlanEdge::direct("stamp", "out_stamp"));
        plan
    }

    // The reference is realized second, so a doc's position among the run's
    // sources is not its position among the docs.
    fn bound() -> (Vec<(String, Vec<usize>)>, Vec<String>) {
        (
            vec![("in_docs".into(), vec![0, 2, 3]), ("in_ref".into(), vec![1])],
            ["s0", "s1", "s2", "s3"].iter().map(|s| s.to_string()).collect(),
        )
    }

    // TEST12861: a file written by one run of a map descends from the item
    // that run read and from what the body reads from outside the map — the
    // doc at that position among the docs, not among all the run's sources,
    // and the reference every run reads.
    #[test]
    fn test12861_a_file_of_a_map_run_descends_from_its_item_and_what_the_body_shares() {
        let plan = compare_plan();
        let (slots, sources) = bound();
        let lineage = RunLineage::new(&plan, &slots, &sources).unwrap();

        assert_eq!(
            lineage.sources_of("out_diffs", Some(&run_of("tok-docs", 0, vec![]))).unwrap(),
            vec!["s0", "s1"]
        );
        assert_eq!(
            lineage.sources_of("out_diffs", Some(&run_of("tok-docs", 1, vec![]))).unwrap(),
            vec!["s1", "s2"]
        );
        assert_eq!(
            lineage.sources_of("out_diffs", Some(&run_of("tok-docs", 2, vec![]))).unwrap(),
            vec!["s1", "s3"]
        );
    }

    // TEST12862: a file of a nested map descends through the value its map
    // reads (the pages of one doc) to the doc the run around it read; a
    // gathered output descends from every doc and the reference; an output
    // that reads no input descends from no source.
    #[test]
    fn test12862_nested_gathered_and_inputless_outputs_descend_from_what_they_read() {
        let plan = compare_plan();
        let (slots, sources) = bound();
        let lineage = RunLineage::new(&plan, &slots, &sources).unwrap();

        assert_eq!(
            lineage
                .sources_of("out_pages", Some(&run_of("tok-pages", 4, vec![("tok-docs", 1)])))
                .unwrap(),
            vec!["s2"]
        );
        assert_eq!(lineage.sources_of("out_report", None).unwrap(), vec!["s0", "s1", "s2", "s3"]);
        assert!(lineage.sources_of("out_stamp", None).unwrap().is_empty());
    }

    // TEST12863: what cannot be told is refused, not guessed: a run of a map
    // the plan does not have, an item past what its input was given, and a
    // run that gives an input of another plan; one source holds every item it
    // is given (a live capture).
    #[test]
    fn test12863_lineage_refuses_what_the_plan_and_binding_cannot_say() {
        let plan = compare_plan();
        let (slots, sources) = bound();
        let lineage = RunLineage::new(&plan, &slots, &sources).unwrap();

        let unknown = lineage
            .sources_of("out_diffs", Some(&run_of("tok-elsewhere", 0, vec![])))
            .unwrap_err();
        assert!(unknown.contains("map 'tok-elsewhere'"), "{unknown}");
        let past = lineage.sources_of("out_diffs", Some(&run_of("tok-docs", 3, vec![]))).unwrap_err();
        assert!(past.contains("item 3 of input 'in_docs', which was given 3 sources"), "{past}");

        let foreign = vec![("in_other".to_string(), vec![0])];
        let refused = RunLineage::new(&plan, &foreign, &sources).err().unwrap();
        assert!(refused.contains("'in_other', which is not an input"), "{refused}");

        let live = vec![("in_docs".to_string(), vec![0]), ("in_ref".to_string(), vec![1])];
        let lineage = RunLineage::new(&plan, &live, &sources).unwrap();
        assert_eq!(
            lineage.sources_of("out_diffs", Some(&run_of("tok-docs", 7, vec![]))).unwrap(),
            vec!["s0", "s1"]
        );
    }

    // TEST12864: an output file is called what it descends from; items that
    // descend from the same thing — the pages of one document — are told
    // apart by their position among them, and only among them.
    #[test]
    fn test12864_output_titles_follow_lineage() {
        assert_eq!(lineage_title(&[], "brisk-otter"), "brisk-otter");
        assert_eq!(lineage_title(&["letter", "template"], "brisk-otter"), "letter + template");

        let titles = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        assert_eq!(
            sequence_item_titles(&titles(&["letter", "invoice"])),
            titles(&["letter", "invoice"])
        );
        assert_eq!(
            sequence_item_titles(&titles(&["story", "story"])),
            titles(&["story · 1", "story · 2"])
        );
        assert_eq!(
            sequence_item_titles(&titles(&["a", "b", "a"])),
            titles(&["a · 1", "b", "a · 2"])
        );
    }
}
