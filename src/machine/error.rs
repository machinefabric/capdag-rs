//! Error types for machine notation parsing, resolution, and serialization.

use thiserror::Error;

use crate::planner::StepToken;

/// A member of a wiring whose arg the types alone do not decide.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndecidedMember {
    /// The node, as the notation named it. `None` where no notation is at
    /// hand (a strand resolved from a planner path): the member is then
    /// known by its type alone.
    pub node: Option<String>,
    /// The media URN the member holds.
    pub source_urn: String,
    /// The args — or, for a machine in step position, the inputs — it could
    /// go to, by name, in the order the definition declares them.
    pub could_be: Vec<String>,
}

impl std::fmt::Display for UndecidedMember {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.node {
            Some(node) => write!(f, "'{node}' ({})", self.source_urn)?,
            None => write!(f, "a member of type '{}'", self.source_urn)?,
        }
        write!(f, " could be {}", self.could_be.join(" or "))
    }
}

/// Errors raised when building or resolving a `Machine`.
///
/// These cover anchor-realization (computing each `MachineStrand`'s
/// resolved DAG via the source-to-arg matching algorithm) and
/// downstream invariants on the resulting machine. They are
/// distinct from `MachineSyntaxError`, which covers lexical and
/// grammatical failures of the notation parser.
#[derive(Debug, Error)]
pub enum MachineAbstractionError {
    /// The strand or wiring set contains no Cap step (no edge to
    /// resolve). A machine must declare at least one capability.
    #[error("strand or wiring set contains no capability steps")]
    NoCapabilitySteps,

    /// A cap URN referenced by a strand or a wiring could not be
    /// found in the cap registry's in-memory cache. Resolution
    /// requires the cap definition (specifically its `args` list)
    /// to compute source-to-arg assignment.
    #[error("cap URN '{cap_urn}' is not in the cap registry cache")]
    UnknownCap { cap_urn: String },

    /// A source URN does not conform to any of the cap's input
    /// argument media URNs. The source has no valid slot to be
    /// assigned to.
    #[error(
        "in strand {strand_index}, cap '{cap_urn}': source URN '{source_urn}' does not conform to any of the cap's input arguments"
    )]
    UnmatchedSourceInCapArgs {
        strand_index: usize,
        cap_urn: String,
        source_urn: String,
    },

    /// The minimum-cost assignment of the unnamed members to the cap's args
    /// is not unique: the types do not say which arg some member is for.
    /// The notation cannot be resolved deterministically, and where a member
    /// stands is never a tiebreak. `members` are the ones to name, each with
    /// the args it could go to.
    #[error(
        "in strand {strand_index}, cap '{cap_urn}': the types do not say which arg these members are for — name them: {}",
        members.iter().map(|member| member.to_string()).collect::<Vec<_>>().join("; ")
    )]
    AmbiguousMachineNotation {
        strand_index: usize,
        cap_urn: String,
        members: Vec<UndecidedMember>,
    },

    /// The resolved data-flow graph of a strand contains a cycle.
    /// A planner-produced strand cannot trigger this; only
    /// programmatic misconstruction or notation that wires a cap's
    /// output back into one of its own ancestors can.
    #[error("strand {strand_index}: resolved data-flow graph contains a cycle")]
    CyclicMachineStrand { strand_index: usize },

    /// A media URN appearing as a node in the resolved strand has
    /// no cached entry in the media registry. Render-payload
    /// emission must have a display title for every node and we
    /// never synthesize titles from URN strings.
    #[error("media URN '{media_urn}' has no cached spec — cannot emit a display title for render payload")]
    UncachedMediaDef { media_urn: String },

    /// A cap URN referenced by an edge has no cached entry in the
    /// cap registry. Render-payload emission must have a display
    /// title for every edge.
    #[error("cap URN '{cap_urn}' has no cached definition — cannot emit a display title for render payload")]
    UncachedCap { cap_urn: String },

    /// A cap could not be applied to the runtime input media flowing into it while
    /// realizing a strand — the declared input/output specs are incompatible with the
    /// concrete upstream media. Realization cannot invent a valid data type, so it
    /// fails hard rather than guessing.
    #[error("strand {strand_index}: cap '{cap_urn}' cannot be applied to runtime input '{runtime_input}': {reason}")]
    RuntimeMediaInference {
        strand_index: usize,
        cap_urn: String,
        runtime_input: String,
        reason: String,
    },

    /// A member names an arg the step's cap does not declare. A name is the
    /// label the definition gives an arg; nothing else is one.
    #[error("strand {strand_index}: cap '{cap_urn}' declares no arg named '{name}' (it declares: {})", declared.join(", "))]
    UnknownArgName {
        strand_index: usize,
        cap_urn: String,
        name: String,
        declared: Vec<String>,
    },

    /// A target names an output the step's cap does not declare.
    #[error("strand {strand_index}: cap '{cap_urn}' declares no output named '{name}' (it declares: {})", declared.join(", "))]
    UnknownOutputName {
        strand_index: usize,
        cap_urn: String,
        name: String,
        declared: Vec<String>,
    },

    /// A strand step takes an output, by its media URN, that the step's cap
    /// does not declare as one of its other outputs.
    #[error("strand {strand_index}: cap '{cap_urn}' declares no other output '{output_urn}' (it declares: {})", declared.join(", "))]
    UnknownStrandOutput {
        strand_index: usize,
        cap_urn: String,
        output_urn: String,
        declared: Vec<String>,
    },

    /// A named member's URN does not conform to the type that arg accepts.
    #[error("strand {strand_index}: cap '{cap_urn}' arg '{name}' accepts '{accepts}', and its source is '{source_urn}'")]
    SourceDoesNotConformToArg {
        strand_index: usize,
        cap_urn: String,
        name: String,
        accepts: String,
        source_urn: String,
    },

    /// A constant does not satisfy the validation of the arg it is bound to.
    #[error("strand {strand_index}: cap '{cap_urn}' arg '{name}' refuses the constant {constant}: {reason}")]
    ConstantFailsValidation {
        strand_index: usize,
        cap_urn: String,
        name: String,
        constant: String,
        reason: String,
    },

    /// An arg that takes a single value is named by two members. Only an arg
    /// that takes a sequence gathers several.
    #[error("strand {strand_index}: cap '{cap_urn}' arg '{name}' takes one value and is named by {count} members")]
    ArgBoundTwice {
        strand_index: usize,
        cap_urn: String,
        name: String,
        count: usize,
    },

    /// A step whose cap takes an input is given no member for it.
    #[error("strand {strand_index}: cap '{cap_urn}' takes an input ('{main_input}') and no member of the wiring is bound to it")]
    MainInputUnbound {
        strand_index: usize,
        cap_urn: String,
        main_input: String,
    },

    /// Two targets of one wiring take the same output.
    #[error("strand {strand_index}: cap '{cap_urn}' output '{output}' is taken twice in one wiring")]
    OutputTakenTwice {
        strand_index: usize,
        cap_urn: String,
        output: String,
    },

    /// An unnamed target takes the main output of a step whose cap has none
    /// (`out=media:void`).
    #[error("strand {strand_index}: cap '{cap_urn}' has no main output to take (its out= is media:void)")]
    NoMainOutput {
        strand_index: usize,
        cap_urn: String,
    },

    /// A wiring holds an unnamed member that nothing produces and whose arg
    /// nothing decides: a second such member, or one beside a named member
    /// that bound the main input. Nothing says which arg an unnamed input of
    /// the machine is for, except that a step's one data input is its main one.
    #[error("statement {position}: cap '{cap_urn}' is given unnamed machine inputs whose arg nothing decides ({}) — name them", members.join(", "))]
    UntypedMachineInput {
        position: usize,
        cap_urn: String,
        members: Vec<String>,
    },

    /// A machine header's reference does not resolve: the host's machine
    /// source holds no machine under it.
    #[error("machine header '{alias}' refers to \"{reference}\", which does not resolve: {details}")]
    UnknownMachineReference {
        alias: String,
        reference: String,
        details: String,
    },

    /// A machine includes itself, directly or through others. `chain` is
    /// every machine on the way round, from the first to the one that closes
    /// the circle.
    #[error("a machine includes itself: {}", chain.join(" -> "))]
    CyclicMachineReference { chain: Vec<String> },

    /// A machine named in a header is not itself a valid machine.
    #[error("machine '{alias}' (\"{reference}\") is not a valid machine: {error}")]
    InvalidIncludedMachine {
        alias: String,
        reference: String,
        error: Box<MachineParseError>,
    },

    /// A policy is written on a step that is another machine. It would have
    /// to mean one thing for every step inside.
    #[error("statement {position}: '{alias}' is a machine, and a policy is written on it — write it on the steps inside")]
    PolicyOnMachine { position: usize, alias: String },

    /// A member names an input the machine in step position does not have.
    #[error("statement {position}: machine '{alias}' has no input named '{name}' (its inputs: {})", declared.join(", "))]
    UnknownMachineInput {
        position: usize,
        alias: String,
        name: String,
        declared: Vec<String>,
    },

    /// A target names an output the machine in step position does not have.
    #[error("statement {position}: machine '{alias}' has no output named '{name}' (its outputs: {})", declared.join(", "))]
    UnknownMachineOutput {
        position: usize,
        alias: String,
        name: String,
        declared: Vec<String>,
    },

    /// An input of a machine in step position is not bound by exactly one
    /// member: nothing is given to it, or more than one member is.
    #[error("statement {position}: input '{input}' of machine '{alias}' is given {count} members — every input of a machine takes exactly one")]
    MachineInputBinding {
        position: usize,
        alias: String,
        input: String,
        count: usize,
    },

    /// Two targets of one wiring take the same output of a machine.
    #[error("statement {position}: output '{output}' of machine '{alias}' is taken twice in one wiring")]
    MachineOutputTakenTwice {
        position: usize,
        alias: String,
        output: String,
    },

    /// An unnamed target of a machine in step position, where the wiring's
    /// other targets leave more than one output, or none. A machine has no
    /// main output: an unnamed target takes the one output left.
    #[error("statement {position}: an unnamed target of machine '{alias}' has {} output(s) left to take ({}) — name it", left.len(), left.join(", "))]
    UnnamedMachineTarget {
        position: usize,
        alias: String,
        left: Vec<String>,
    },

    /// An unnamed member of a machine in step position that nothing gives a
    /// type. A machine has no main input, so its unnamed members are matched
    /// by type alone, and this one has none to be matched by.
    #[error("statement {position}: machine '{alias}' is given unnamed members that have no type to be matched by ({}) — name them", members.join(", "))]
    UntypedMachineMember {
        position: usize,
        alias: String,
        members: Vec<String>,
    },

    /// An unnamed member of a machine in step position conforms to no input
    /// left for it.
    #[error("statement {position}: a member of type '{source_urn}' fits no input left of machine '{alias}'")]
    UnmatchedMachineMember {
        position: usize,
        alias: String,
        source_urn: String,
    },

    /// The unnamed members of a machine in step position fit its inputs in
    /// more than one way. `members` are the ones to name, each with the
    /// inputs it could go to.
    #[error(
        "statement {position}: the types do not say which input of machine '{alias}' these members are for — name them: {}",
        members.iter().map(|member| member.to_string()).collect::<Vec<_>>().join("; ")
    )]
    AmbiguousMachineMembers {
        position: usize,
        alias: String,
        members: Vec<UndecidedMember>,
    },

    /// A named member's URN does not conform to the type of the machine
    /// input it names.
    #[error("statement {position}: input '{name}' of machine '{alias}' holds '{accepts}', and its source is '{source_urn}'")]
    SourceDoesNotConformToMachineInput {
        position: usize,
        alias: String,
        name: String,
        accepts: String,
        source_urn: String,
    },

    /// The wirings that touch a node give it URNs that are not on one
    /// specialization chain.
    #[error("node '{node}' is given media URNs that are not on one specialization chain: '{first}' and '{second}'")]
    NodeMediaConflict {
        node: String,
        first: String,
        second: String,
    },

    /// A cap does not declare its input: no argument declares a `Stdin` source whose
    /// URN is the cap's `in=`. The main input is the value piped in on stdin, so the
    /// main arg always declares a stdin source carrying `in=` (its declared slot URN
    /// may differ — e.g. a file-path slot whose piped content is `in=`). A cap without
    /// such an arg cannot receive its input to thread the strand's runtime media.
    #[error("strand {strand_index}: cap '{cap_urn}' does not declare its input (no argument declares a stdin source whose URN is its in=)")]
    CapDoesNotDeclareInput {
        strand_index: usize,
        cap_urn: String,
    },

    /// A strand's edges do not form a data-flow graph whose every source is reachable
    /// (an unreachable edge, or a source whose producer never becomes available).
    #[error("strand {strand_index}: edges do not form a connected data-flow graph")]
    DisconnectedStrand { strand_index: usize },

    /// The explicit cardinality boundary carried by a resolved `Strand` does not
    /// agree with the cardinality derived from the fabric definitions. A boundary
    /// identity may neither be discarded nor attached to a scalar edge.
    #[error("strand {strand_index}: cap '{cap_urn}' has inconsistent ForEach shape (explicit boundary: {has_explicit_boundary}, derived loop: {is_loop})")]
    ForEachShapeMismatch {
        strand_index: usize,
        cap_urn: String,
        has_explicit_boundary: bool,
        is_loop: bool,
    },

    /// A cardinality transition was not followed by the cap it qualifies.
    #[error(
        "strand {strand_index}: ForEach step '{token_id}' is not followed by a capability step"
    )]
    DanglingForEach {
        strand_index: usize,
        token_id: StepToken,
    },

    /// Two ForEach boundaries cannot qualify the same cap step.
    #[error("strand {strand_index}: ForEach step '{token_id}' follows unresolved ForEach step '{previous_token_id}'")]
    ConsecutiveForEach {
        strand_index: usize,
        previous_token_id: StepToken,
        token_id: StepToken,
    },

    /// Anchor binding: the number of run-supplied sources does not equal the number
    /// of input anchors. A run binds exactly one source per input anchor — no
    /// positional guessing, no partial binding.
    #[error("strand {strand_index}: {sources} source(s) supplied for {anchors} input anchor(s) — a run binds exactly one source per input anchor")]
    SourceAnchorCountMismatch {
        strand_index: usize,
        sources: usize,
        anchors: usize,
    },

    /// Anchor binding: a source cannot be bound — either it conforms to no input
    /// anchor, or no assignment exists in which every source claims a distinct
    /// conforming anchor.
    #[error("strand {strand_index}: source '{source_urn}' cannot be bound to a distinct conforming input anchor")]
    UnbindableAnchorSource {
        strand_index: usize,
        source_urn: String,
    },

    /// Anchor binding: the minimum-cost source-to-input-anchor assignment is not
    /// unique. Two distinct assignments tie; binding cannot be decided
    /// deterministically and source order is never used as a tiebreaker.
    #[error("strand {strand_index}: source-to-input-anchor binding is ambiguous (multiple minimum-cost assignments exist)")]
    AmbiguousAnchorBinding { strand_index: usize },

    /// Realization: an input anchor has no bound source. Every input anchor must
    /// receive exactly one concrete source media before the strand can realize.
    #[error(
        "strand {strand_index}: input anchor {anchor_id} ('{anchor_urn}') has no bound source"
    )]
    MissingAnchorSource {
        strand_index: usize,
        anchor_id: u32,
        anchor_urn: String,
    },

    /// Realization: a source was bound to a node that is not an input anchor of the
    /// strand — a caller bug, exposed hard.
    #[error("strand {strand_index}: a source is bound to node {node_id}, which is not an input anchor of the strand")]
    SourceBoundToNonAnchor { strand_index: usize, node_id: u32 },

    /// Realization: a bound source does not conform to its input anchor's declared
    /// media URN. Realization never coerces — the binding is invalid.
    #[error("strand {strand_index}: bound source '{source_urn}' does not conform to input anchor '{anchor_urn}'")]
    AnchorSourceMismatch {
        strand_index: usize,
        anchor_urn: String,
        source_urn: String,
    },
}

/// Errors raised during lexical / grammatical parsing of machine
/// notation.
///
/// These represent failures BEFORE the parser hands the wiring
/// set to the resolver. Resolution-level failures (cap not in
/// registry, ambiguous matching, cyclic strand, …) are reported
/// as `MachineAbstractionError`.
#[derive(Debug, Error)]
pub enum MachineSyntaxError {
    /// Input string is empty or contains only whitespace.
    #[error("machine notation is empty")]
    Empty,

    /// A cap URN in a header statement failed to parse.
    #[error("invalid cap URN in header '{alias}': {details}")]
    InvalidCapUrn { alias: String, details: String },

    /// A wiring statement references a name that is neither a local
    /// header alias nor a registered cap alias in the fabric registry.
    #[error("wiring references undefined alias '{alias}' (not a local header and not a registered cap alias)")]
    UndefinedAlias { alias: String },

    /// A wiring's cap-position name resolved to a fabric alias, but that
    /// alias points at a media URN rather than a cap URN. The cap position
    /// requires a cap.
    #[error("alias '{alias}' in cap position resolves to a media URN ('{target}'), but a cap is required there")]
    AliasNotACap { alias: String, target: String },

    /// Two header statements define the same alias.
    #[error("duplicate alias '{alias}' (first defined at statement {first_position})")]
    DuplicateAlias {
        alias: String,
        first_position: usize,
    },

    /// A media URN referenced in a header failed to parse.
    #[error("invalid media URN in cap '{alias}': {details}")]
    InvalidMediaUrn { alias: String, details: String },

    /// The parsed machine has headers but no wirings.
    #[error("machine has headers but no wirings — define at least one edge")]
    NoEdges,

    /// A wiring references an alias used as a node name that
    /// collides with a header alias.
    #[error("node name '{name}' collides with cap alias '{alias}'")]
    NodeAliasCollision { name: String, alias: String },

    /// PEG parse error from the pest grammar.
    #[error("parse error: {details}")]
    ParseError { details: String },

    /// A machine header names another machine, and the notation is being read
    /// with nothing to resolve the reference against.
    #[error("machine header '{alias}' refers to \"{reference}\", and no machine source was given to resolve it")]
    MachineReferenceWithoutSource { alias: String, reference: String },

    /// A constant appears as a member without a name. A constant has no type
    /// to be matched by: it says which arg it is for.
    #[error("constant {constant} in statement {position} names no arg — write it as name={constant}")]
    UnnamedConstant { constant: String, position: usize },

    /// A node is named `true`, `false` or `null`: the words the notation
    /// reads as constants.
    #[error("'{name}' in statement {position} is a constant, not a node name")]
    ReservedNodeName { name: String, position: usize },

    /// More than one target of a wiring is unnamed. The unnamed target takes
    /// the step's main output, and a step has one.
    #[error("statement {position} has several unnamed targets ({}) — only the main output is taken without a name", targets.join(", "))]
    SeveralUnnamedTargets {
        position: usize,
        targets: Vec<String>,
    },

    /// A step policy has an unknown key, a repeated key, or a value outside
    /// the table.
    #[error("invalid policy on step '{step}' in statement {position}: {details}")]
    InvalidPolicy {
        step: String,
        position: usize,
        details: String,
    },
}

/// Combined error returned by `Machine::from_string`.
///
/// Notation parsing has two phases: lexical/grammatical (yields
/// `MachineSyntaxError`) and resolution (yields
/// `MachineAbstractionError`). This enum is the union returned
/// from `Machine::from_string` and `parse_machine` so callers can
/// branch on either kind of failure.
#[derive(Debug, Error)]
pub enum MachineParseError {
    #[error(transparent)]
    Syntax(#[from] MachineSyntaxError),

    #[error(transparent)]
    Resolution(#[from] MachineAbstractionError),
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST1134: All MachineAbstractionError variants are of type MachineAbstractionError and
    // are convertible to MachineParseError::Resolution. This pins the error hierarchy so a
    // refactor that accidentally changes the type relationship is caught immediately.
    #[test]
    fn test1134_all_abstraction_error_variants_are_machine_abstraction_error() {
        let variants: Vec<MachineAbstractionError> = vec![
            MachineAbstractionError::NoCapabilitySteps,
            MachineAbstractionError::UnknownCap {
                cap_urn: "cap:x".to_string(),
            },
            MachineAbstractionError::UnmatchedSourceInCapArgs {
                strand_index: 0,
                cap_urn: "cap:x".to_string(),
                source_urn: "media:document;ext=pdf".to_string(),
            },
            MachineAbstractionError::AmbiguousMachineNotation {
                strand_index: 1,
                cap_urn: "cap:y".to_string(),
                members: Vec::new(),
            },
            MachineAbstractionError::CyclicMachineStrand { strand_index: 2 },
        ];

        for variant in variants {
            let parse_error: MachineParseError = variant.into();
            assert!(
                matches!(parse_error, MachineParseError::Resolution(_)),
                "every MachineAbstractionError must convert to MachineParseError::Resolution"
            );
        }
    }

    // TEST1147: MachineSyntaxError Display includes position and detail for each variant
    #[test]
    fn test1147_machine_syntax_error_display_is_specific() {
        let err = MachineSyntaxError::InvalidPolicy {
            step: "fetch".to_string(),
            position: 7,
            details: "unknown key 'tries'".to_string(),
        };

        assert_eq!(
            err.to_string(),
            "invalid policy on step 'fetch' in statement 7: unknown key 'tries'"
        );
    }

    // TEST1148: MachineParseError::from(MachineSyntaxError) preserves the syntax error variant
    #[test]
    fn test1148_machine_parse_error_from_syntax_preserves_variant() {
        let parse_error: MachineParseError = MachineSyntaxError::UndefinedAlias {
            alias: "extract".to_string(),
        }
        .into();

        match parse_error {
            MachineParseError::Syntax(MachineSyntaxError::UndefinedAlias { alias }) => {
                assert_eq!(alias, "extract");
            }
            other => panic!("expected syntax undefined alias, got {other:?}"),
        }
    }

    // TEST1149: MachineParseError::from(MachineAbstractionError) preserves the resolution error variant
    #[test]
    fn test1149_machine_parse_error_from_resolution_preserves_variant() {
        let undecided = |node: Option<&str>| UndecidedMember {
            node: node.map(str::to_string),
            source_urn: "media:enc=utf-8".to_string(),
            could_be: vec!["left".to_string(), "right".to_string()],
        };
        let parse_error: MachineParseError = MachineAbstractionError::AmbiguousMachineNotation {
            strand_index: 2,
            cap_urn: "cap:in=\"media:document;ext=pdf\";out=media:text".to_string(),
            members: vec![undecided(Some("a")), undecided(None)],
        }
        .into();
        // What it says is which members to name, and what each could be.
        let said = parse_error.to_string();
        assert!(
            said.ends_with(
                "name them: 'a' (media:enc=utf-8) could be left or right; \
                 a member of type 'media:enc=utf-8' could be left or right"
            ),
            "{said}"
        );

        match parse_error {
            MachineParseError::Resolution(MachineAbstractionError::AmbiguousMachineNotation {
                strand_index,
                cap_urn,
                members,
            }) => {
                assert_eq!(strand_index, 2);
                assert_eq!(cap_urn, "cap:in=\"media:document;ext=pdf\";out=media:text");
                assert_eq!(members.len(), 2);
            }
            other => panic!("expected ambiguous resolution error, got {other:?}"),
        }
    }
}
