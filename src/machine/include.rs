//! Machines in step position: where another machine's notation comes from,
//! and what a machine looks like to the machine that includes it.
//!
//! A header `alias machine "<reference>"` names another machine. The
//! reference is an opaque string: the host resolves it through one interface,
//! [`MachineSource`]. The parser reads the referenced notation, resolves it
//! on its own, and writes its steps into the including machine with node
//! positions of their own (`capdag/docs/09-machine-notation.md`, §Machines in
//! step position).

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::MediaUrn;

use super::graph::{BindingSource, Machine, NodeId};
use super::parser::StrandNodeNames;
use super::syntax::StepPolicy;

/// The notation of one machine, as a [`MachineSource`] gives it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedMachine {
    /// What the source knows this machine by. Two references that resolve to
    /// one machine yield one id: it is what a circle of inclusions is
    /// recognized by, and what a reference written inside this machine is
    /// resolved relative to.
    pub id: String,
    pub notation: String,
}

/// Why a reference did not yield a machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MachineSourceError {
    /// The notation is being read with nothing to resolve references against.
    NoSource,
    /// The source holds no machine under the reference.
    NotFound(String),
}

impl std::fmt::Display for MachineSourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MachineSourceError::NoSource => write!(f, "there is nothing to resolve machine references against"),
            MachineSourceError::NotFound(details) => write!(f, "{details}"),
        }
    }
}

impl std::error::Error for MachineSourceError {}

/// Where the machines named in machine headers come from.
///
/// The `capdag` CLI reads a reference as a path relative to the including
/// file ([`FileMachineSource`]); an engine reads it from its machine store.
pub trait MachineSource: Send + Sync {
    /// The machine `reference` names. `including` is the id of the machine
    /// whose header wrote it — `None` when that machine has none (notation
    /// that came from no place the source knows).
    fn load(
        &self,
        reference: &str,
        including: Option<&str>,
    ) -> Result<LoadedMachine, MachineSourceError>;
}

/// The source of a host that resolves no references. A machine header read
/// against it is refused.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoMachineSource;

impl MachineSource for NoMachineSource {
    fn load(&self, _: &str, _: Option<&str>) -> Result<LoadedMachine, MachineSourceError> {
        Err(MachineSourceError::NoSource)
    }
}

/// Machines in files. A reference is a path: relative to the directory of
/// the including file, or to `base` when the including machine came from no
/// file. A machine's id is its file's canonical path.
#[derive(Debug, Clone)]
pub struct FileMachineSource {
    base: PathBuf,
}

impl FileMachineSource {
    /// References written in notation that came from no file are read
    /// relative to `base`.
    pub fn new(base: impl Into<PathBuf>) -> Self {
        Self { base: base.into() }
    }

    /// The id this source gives the machine in the file at `path`: what to
    /// name the including machine by, so that its own references are read
    /// relative to it and an inclusion of it is recognized.
    pub fn id_of(path: &Path) -> Result<String, MachineSourceError> {
        let canonical = std::fs::canonicalize(path)
            .map_err(|e| MachineSourceError::NotFound(format!("{}: {e}", path.display())))?;
        canonical
            .to_str()
            .map(str::to_string)
            .ok_or_else(|| MachineSourceError::NotFound(format!("{} is not UTF-8", path.display())))
    }
}

impl MachineSource for FileMachineSource {
    fn load(
        &self,
        reference: &str,
        including: Option<&str>,
    ) -> Result<LoadedMachine, MachineSourceError> {
        let directory = match including {
            Some(id) => Path::new(id)
                .parent()
                .map(Path::to_path_buf)
                .ok_or_else(|| MachineSourceError::NotFound(format!("'{id}' is in no directory")))?,
            None => self.base.clone(),
        };
        let path = directory.join(reference);
        let id = Self::id_of(&path)?;
        let notation = std::fs::read_to_string(&path)
            .map_err(|e| MachineSourceError::NotFound(format!("{}: {e}", path.display())))?;
        Ok(LoadedMachine { id, notation })
    }
}

/// Machines held in memory, each under the reference that names it. A
/// reference names the same machine wherever it is written. For a host that
/// loads the machines a notation refers to before it parses (an engine whose
/// store is read asynchronously), and for tests.
#[derive(Debug, Clone, Default)]
pub struct MapMachineSource {
    machines: HashMap<String, String>,
}

impl MapMachineSource {
    pub fn new() -> Self {
        Self::default()
    }

    /// Hold `notation` under `reference`.
    pub fn insert(&mut self, reference: impl Into<String>, notation: impl Into<String>) {
        self.machines.insert(reference.into(), notation.into());
    }
}

impl MachineSource for MapMachineSource {
    fn load(&self, reference: &str, _: Option<&str>) -> Result<LoadedMachine, MachineSourceError> {
        self.machines
            .get(reference)
            .map(|notation| LoadedMachine {
                id: reference.to_string(),
                notation: notation.clone(),
            })
            .ok_or_else(|| MachineSourceError::NotFound(format!("no machine is held under \"{reference}\"")))
    }
}

/// What a member of an included step gives its arg: a node of the included
/// machine, by the name its notation wrote, or a constant.
#[derive(Debug, Clone)]
pub(crate) enum IncludedValue {
    Node(String),
    Constant(serde_json::Value),
}

/// One step of an included machine, with every member under the name of the
/// arg its own resolution bound it to and every target under the name of the
/// output it takes. Written into another machine, it binds what it bound
/// alone: nothing about where it is included can change it.
#[derive(Debug, Clone)]
pub(crate) struct IncludedStep {
    pub cap_urn: CapUrn,
    pub policy: StepPolicy,
    pub members: Vec<(String, IncludedValue)>,
    pub targets: Vec<(String, String)>,
    /// The machine steps this step was already included through inside the
    /// included machine, outermost first.
    pub included_as: Vec<String>,
}

/// A machine as the machine that includes it sees it: its inputs and
/// outputs, by the names its notation wrote, and its steps.
#[derive(Debug, Clone)]
pub(crate) struct IncludedMachine {
    /// Every input anchor, by name, with the media it holds. In the order
    /// of the machine's strands, by name within one.
    pub inputs: Vec<(String, MediaUrn)>,
    pub outputs: Vec<(String, MediaUrn)>,
    /// Every node, by name, with the media it holds in the machine as it
    /// resolved alone.
    pub node_media: Vec<(String, MediaUrn)>,
    pub steps: Vec<IncludedStep>,
}

impl IncludedMachine {
    /// Read a resolved machine, with the names its notation wrote.
    pub fn of(machine: &Machine, names: &[StrandNodeNames]) -> Self {
        let mut inputs = Vec::new();
        let mut outputs = Vec::new();
        let mut node_media = Vec::new();
        let mut steps = Vec::new();
        for (strand, names) in machine.strands().iter().zip(names) {
            let name_of: HashMap<NodeId, &String> =
                names.iter().map(|(name, id)| (*id, name)).collect();
            let name = |id: NodeId| -> String {
                name_of
                    .get(&id)
                    .map(|name| (*name).clone())
                    .expect("the parser names every node of a strand it resolved")
            };
            let anchors = |ids: &[NodeId]| -> Vec<(String, MediaUrn)> {
                let mut anchors: Vec<(String, MediaUrn)> = ids
                    .iter()
                    .map(|id| (name(*id), strand.nodes()[*id as usize].clone()))
                    .collect();
                anchors.sort_by(|a, b| a.0.cmp(&b.0));
                anchors
            };
            let every: Vec<NodeId> = (0..strand.nodes().len() as NodeId).collect();
            node_media.extend(anchors(&every));
            inputs.extend(anchors(strand.input_anchor_ids()));
            outputs.extend(anchors(strand.output_anchor_ids()));
            for edge in strand.edges() {
                steps.push(IncludedStep {
                    cap_urn: edge.cap_urn.clone(),
                    policy: edge.policy,
                    members: edge
                        .assignment
                        .iter()
                        .map(|binding| {
                            let value = match &binding.source {
                                BindingSource::Node(id) => IncludedValue::Node(name(*id)),
                                BindingSource::Constant(value) => IncludedValue::Constant(value.clone()),
                            };
                            (binding.arg_name.clone(), value)
                        })
                        .collect(),
                    targets: edge
                        .outputs
                        .iter()
                        .map(|output| (output.output_name.clone(), name(output.node)))
                        .collect(),
                    included_as: edge.included_as.clone(),
                });
            }
        }
        Self {
            inputs,
            outputs,
            node_media,
            steps,
        }
    }

    pub fn input_names(&self) -> Vec<String> {
        self.inputs.iter().map(|(name, _)| name.clone()).collect()
    }

    pub fn output_names(&self) -> Vec<String> {
        self.outputs.iter().map(|(name, _)| name.clone()).collect()
    }

    pub fn input(&self, name: &str) -> Option<&MediaUrn> {
        self.inputs.iter().find(|(n, _)| n == name).map(|(_, media)| media)
    }

    pub fn output(&self, name: &str) -> Option<&MediaUrn> {
        self.outputs.iter().find(|(n, _)| n == name).map(|(_, media)| media)
    }
}
