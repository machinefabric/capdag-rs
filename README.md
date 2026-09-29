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

[Build and run a cartridge](https://capdag.com/docs/18.2-getting-started-cartridge-development/)
is a guided create, install, run, and edit journey using the canonical starter
projects.

### Complete a task

- [Develop a cartridge](https://capdag.com/docs/18.2-getting-started-cartridge-development/)
- [Run one capability or a machine](https://capdag.com/docs/18.1-cli-reference/)
- [Contribute capability and media definitions](https://capdag.com/docs/99-contributing/)

### Look up exact behavior

- [Specification map and terminology](https://capdag.com/docs/01-overview/)
- [Tagged URN domain](https://capdag.com/docs/03-tagged-urn-domain/)
- [Capability URN structure](https://capdag.com/docs/06-cap-urn-structure/)
- [Dispatch](https://capdag.com/docs/07-dispatch/)
- [Machine notation](https://capdag.com/docs/09-machine-notation/)
- [Bifaci protocol](https://capdag.com/docs/12.1-architecture/)
- [Cartridge runtime](https://capdag.com/docs/13.1-cartridge-runtime/)
- [Planner and execution](https://capdag.com/docs/15.4-planner/)
- [`capdag` CLI](https://capdag.com/docs/18.1-cli-reference/)

### Understand the design

- [Formal foundations](https://capdag.com/docs/02-formal-foundations/)
- [Specificity and ranking](https://capdag.com/docs/05-specificity/)
- [Relay topology](https://capdag.com/docs/14.3-relay-topology/)
- [Rust and Swift implementation differences](https://capdag.com/docs/16.5-rust-vs-swift/)

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
[specification](https://capdag.com/docs/01-overview/).

## License

MIT License.
