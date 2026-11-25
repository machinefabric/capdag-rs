//! Bake the capdag release version (`capdag-rs/version.txt`) as the
//! `CAPDAG_VERSION` compile-time env, so `capdag --version` reports the
//! published release. `version.txt` is the single source of truth for the
//! shipped version; it is read straight from the repository, so no workspace
//! env plumbing is required.

use std::env;
use std::path::Path;

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by cargo");
    let path = Path::new(&manifest_dir).join("..").join("version.txt");
    println!("cargo:rerun-if-changed={}", path.display());
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e));
    let version = raw.trim();
    if version.is_empty() {
        panic!("{} is empty — it must hold the capdag release version", path.display());
    }
    println!("cargo:rustc-env=CAPDAG_VERSION={version}");
}
