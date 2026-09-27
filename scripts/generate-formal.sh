#!/bin/bash
#
# Regenerate everything this crate takes from the proved model in ../formal:
# the Lean is checked (every proof, including tagged-urn's it builds on), the
# conformance table is written, and lungo turns the model's executable surface
# into src/generated.
#
# Needs elan (the toolchain ../formal/lean-toolchain pins) and cargo-lungo at
# the version Cargo.toml pins for `lungo`. Nothing else in this crate does:
# src/generated is checked in, and a crate compiling this one needs neither.
#
# Run it after changing ../formal, and commit what it writes.

set -euo pipefail

crate="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
(
    cd "$crate/../formal"
    lake build
    lake exe conformance > conformance.json
)
cd "$crate"
cargo lungo build
