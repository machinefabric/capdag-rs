//! What the end-to-end tests share.

use std::path::Path;

/// The cargo arguments that build a test cartridge against THIS capdag.
///
/// A cartridge built to test capdag has to be built against the capdag under
/// test. Its manifest names capdag two ways: a path to the working copy, or —
/// as it is committed, and as a release sees it — the last RELEASED tag. Under
/// the second, the suite that decides whether this capdag may be released built
/// its cartridge against the release before, which no longer compiled against
/// the tagged-urn this one requires, and every test that needed the cartridge
/// failed on a build that had nothing to do with it.
///
/// So a capdag taken from a git source is patched to this crate. A manifest
/// that already names a path has nothing to patch.
///
/// The patch alone is not enough where the cartridge has been built before:
/// its `Cargo.lock` holds the released capdag, and cargo keeps a locked entry
/// over a patch it has not resolved — it built the released one again and said
/// only `patch … was not used in the crate graph`. [`resolve_this_capdag`]
/// moves that one entry first.
pub fn against_this_capdag(cartridge_dir: &Path) -> Vec<String> {
    let manifest_path = cartridge_dir.join("Cargo.toml");
    let manifest = std::fs::read_to_string(&manifest_path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", manifest_path.display()));
    let Some(source) = capdag_git_source(&manifest) else {
        return Vec::new();
    };
    vec![
        "--config".to_string(),
        format!("patch.'{source}'.capdag.path='{}'", env!("CARGO_MANIFEST_DIR")),
    ]
}

/// Resolve the cartridge's `capdag` to this crate before it is built: `cargo
/// update capdag` under the same patch, which moves that one lock entry and
/// leaves every other dependency where the lock has it. Nothing is run for a
/// manifest that names a path. `tag` prefixes what is printed.
pub fn resolve_this_capdag(cartridge_dir: &Path, target_dir: &Path, tag: &str) {
    let patch = against_this_capdag(cartridge_dir);
    if patch.is_empty() {
        return;
    }
    let output = std::process::Command::new("cargo")
        .args(["update", "capdag"])
        .args(&patch)
        .env("CARGO_TARGET_DIR", target_dir)
        .current_dir(cartridge_dir)
        .output()
        .expect("Failed to run cargo update for the test cartridge");
    for line in String::from_utf8_lossy(&output.stderr).lines() {
        eprintln!("[{tag}]   {line}");
    }
    assert!(
        output.status.success(),
        "could not resolve the test cartridge against the capdag under test (exit code: {:?})",
        output.status.code()
    );
}

/// The git URL a manifest takes `capdag` from, if it takes it from one.
fn capdag_git_source(manifest: &str) -> Option<String> {
    let line = manifest
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("capdag") && line.contains('=') && line.contains("git"))?;
    let after = &line[line.find("git")? + 3..];
    let start = after.find('"')? + 1;
    let end = after[start..].find('"')?;
    Some(after[start..start + end].to_string())
}
