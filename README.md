# CapDAG

CapDAG is the reference implementation of MachineFabric’s capability-addressed
planning and execution protocol. It defines capability and media URNs, dispatch,
machine notation, the bifaci frame protocol, cartridge hosting, orchestration,
and the `capdag` command-line product.

This repository is public. It documents public protocol and product behavior;
private MachineFabric release operations and signing-key custody are not part of
this repository.

## Choose documentation by what you need

### Learn cartridge development

[Build and run a cartridge](https://github.com/machinefabric/capdag/blob/main/docs/18.2-getting-started-cartridge-development.md)
is a guided create, install, run, and edit journey using the canonical starter
projects.

### Complete a task

- [Develop a cartridge](https://github.com/machinefabric/capdag/blob/main/docs/18.2-getting-started-cartridge-development.md)
- [Run one capability or a machine](https://github.com/machinefabric/capdag/blob/main/docs/18.1-cli-reference.md)
- [Contribute capability and media definitions](https://github.com/machinefabric/capdag/blob/main/docs/99-contributing.md)

### Look up exact behavior

- [Specification map and terminology](https://github.com/machinefabric/capdag/blob/main/docs/01-overview.md)
- [Tagged URN domain](https://github.com/machinefabric/capdag/blob/main/docs/03-tagged-urn-domain.md)
- [Capability URN structure](https://github.com/machinefabric/capdag/blob/main/docs/06-cap-urn-structure.md)
- [Dispatch](https://github.com/machinefabric/capdag/blob/main/docs/07-dispatch.md)
- [Machine notation](https://github.com/machinefabric/capdag/blob/main/docs/09-machine-notation.md)
- [Bifaci protocol](https://github.com/machinefabric/capdag/blob/main/docs/12.1-architecture.md)
- [Cartridge runtime](https://github.com/machinefabric/capdag/blob/main/docs/13.1-cartridge-runtime.md)
- [Planner and execution](https://github.com/machinefabric/capdag/blob/main/docs/15.4-planner.md)
- [`capdag` CLI](https://github.com/machinefabric/capdag/blob/main/docs/18.1-cli-reference.md)

### Understand the design

- [Formal foundations](https://github.com/machinefabric/capdag/blob/main/docs/02-formal-foundations.md)
- [Specificity and ranking](https://github.com/machinefabric/capdag/blob/main/docs/05-specificity.md)
- [Relay topology](https://github.com/machinefabric/capdag/blob/main/docs/14.3-relay-topology.md)
- [Rust and Swift implementation differences](https://github.com/machinefabric/capdag/blob/main/docs/16.5-rust-vs-swift.md)

## What CapDAG provides

- Parsed tagged, media, and capability URN types with normalization and
  matching predicates.
- Manifest-aware fabric resolution for versioned capabilities, media
  definitions, and aliases.
- Machine notation parsing, graph resolution, planning, and unified execution.
- Bifaci v4 framing, multiplexed streams, credit-based flow control, diagnostic
  attribution, cancellation, and handler-capacity advertisement.
- Cartridge and host runtimes, relay components, discovery, and integrity
  verification.
- The `capdag` CLI for running one capability, planning and running machines,
  inspecting the fabric, warming the cartridge cache, and scaffolding local
  cartridge projects.

## Language family

Rust is the reference implementation. Go, Python, JavaScript, and
Swift/Objective-C mirrors implement the portions applicable to their role.
Shared numbered tests use the same number for the same behavior in every mirror
that implements it. Numbers `0001`–`7999` are shared; `8000` and above are
implementation-specific.

JavaScript intentionally stops at the planner and notation surface; it does not
provide a cartridge runtime, host, or relay. This is a defined difference in
scope, not a parity defect.

## Use CapDAG as a Rust dependency

CapDAG is resolved from a release tag rather than crates.io. Pin an explicit
published tag:

```toml
[dependencies]
capdag = { git = "https://github.com/machinefabric/capdag-rs", tag = "v<version>" }
```

Builds that resolve fabric or cartridge registries require explicit registry
version and trust inputs. Product and workspace build systems supply those
inputs; `build.rs` refuses an ambiguous build instead of selecting defaults.

## Contract summary

- URNs are opaque parsed values. Use their predicates; do not split or compare
  their strings for routing.
- `in` and `out` are the directional capability coordinates. Other capability
  tags are descriptive constraints; `effect` defines the output media-identity
  transformation.
- `media:` is the top media type and `media:void` is the atomic unit type.
- File type, serialization format, and character encoding use `ext=`, `fmt=`,
  and `enc=` respectively.
- Stream cardinality is carried by `is_sequence`; structural tags such as
  `list` do not encode cardinality.
- Abstract capabilities are dispatch umbrellas. They are not runnable graph
  edges and must narrow to a concrete specialization.
- Protocol violations and missing registry definitions fail explicitly. There
  is no compatibility decoder for earlier bifaci wire versions.

The normative details and conformance conditions are in the
[specification](https://github.com/machinefabric/capdag/blob/main/docs/01-overview.md).

## License

MIT License.
