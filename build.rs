//! Build-time bake of the fabric registry's pinned manifest version.
//!
//! Every Rust binary in the workspace that consumes the fabric registry
//! (engine, anything that resolves URNs, anything that writes
//! `cartridge.json`) needs to know which manifest version of the
//! registry it's tied to. The version is a single workspace-wide value
//! sourced from `fabric/manifest-version.txt`. The workspace build pipeline
//! reads that file and exports it as `MFR_FABRIC_MANIFEST_VERSION` for
//! every `cargo` invocation it shells.
//!
//! This build script reads that env var and writes a generated
//! `fabric_manifest_version.rs` into `OUT_DIR`, which the crate
//! `include!`s. Two safety properties this guarantees:
//!
//!   1. A raw `cargo build` without the env var **fails the build**
//!      with a descriptive message. There is no implicit default — if
//!      a developer is building outside the workspace build tool, that's an unsupported
//!      path and must be opted-into explicitly by exporting the var.
//!   2. The value is a `pub const u32` known at compile time, so every
//!      consumer can rely on it in `const` contexts (signatures of
//!      `FabricRegistry::new`, default fields on `CartridgeJson`, etc.).

use std::env;
use std::path::Path;

fn main() {
    let out_dir = env::var("OUT_DIR").expect("OUT_DIR is set by cargo");

    bake_registry_version(
        &out_dir,
        "MFR_FABRIC_MANIFEST_VERSION",
        "fabric_manifest_version.rs",
        "FABRIC_MANIFEST_VERSION",
        "Fabric registry manifest version this build is pinned to. Sourced from",
        "fabric/manifest-version.txt",
    );
    // The cartridge registry is versioned by the SAME mechanism as the fabric
    // registry (a breaking cap-shape change — `command` → `aliases` — forced a
    // new registry regime). v0 is the implicit legacy state served to
    // already-shipped builds at the un-versioned path; this build speaks only
    // v >= 1 at the versioned path.
    bake_registry_version(
        &out_dir,
        "MFR_CARTRIDGE_REGISTRY_VERSION",
        "cartridge_registry_version.rs",
        "CARTRIDGE_REGISTRY_VERSION",
        "Cartridge registry version this build is pinned to. Sourced from",
        "schemas/cartridge-registry/registry-version.txt",
    );

    bake_capdag_version();
    generate_bundled_cartridge_hashes(&out_dir);
    enforce_signing_pubkey_pairing();
}

/// Bake `capdag/version.txt` as the `CAPDAG_VERSION` compile-time env so
/// `capdag --version` reports the published RELEASE version. `version.txt` is the
/// single source of truth for the shipped version (Cargo.toml's crate `version`
/// is an unrelated, un-bumped value); read it straight from the crate dir so no
/// workspace env plumbing is required.
fn bake_capdag_version() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by cargo");
    let path = Path::new(&manifest_dir).join("version.txt");
    println!("cargo:rerun-if-changed={}", path.display());
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e));
    let version = raw.trim();
    if version.is_empty() {
        panic!(
            "{} is empty — it must hold the capdag release version",
            path.display()
        );
    }
    println!("cargo:rustc-env=CAPDAG_VERSION={version}");
}

/// Bake a `pub const <const_name>: u32` into `<out_file>` in OUT_DIR from the
/// `<env_var>` build environment variable. Shared by the fabric-manifest and
/// cartridge-registry version bakes — identical contract: the var is mandatory
/// (a raw `cargo build` without it fails the build with a descriptive message,
/// no implicit default), must parse to a non-negative integer, and must be
/// >= 1 (v0 is the implicit pre-versioning legacy state, never a build target).
fn bake_registry_version(
    out_dir: &str,
    env_var: &str,
    out_file: &str,
    const_name: &str,
    doc_lead: &str,
    source_file: &str,
) {
    println!("cargo:rerun-if-env-changed={env_var}");

    let raw = env::var(env_var).unwrap_or_else(|_| {
        panic!(
            "{env_var} is not set. Every cargo invocation against the MachineFabric \
             workspace must export this variable, sourced from {source_file}. Run \
             builds and tests through the workspace build tool (which exports it for you) instead of \
             invoking cargo directly."
        );
    });

    let trimmed = raw.trim();
    let version: u32 = trimmed.parse().unwrap_or_else(|e| {
        panic!("{env_var} must be a non-negative integer (got {trimmed:?}): {e}");
    });
    // 0 is reserved for legacy v0 already in the wild and is never a valid bake
    // target — the workspace builds only at v >= 1.
    if version < 1 {
        panic!(
            "{env_var} must be >= 1 (got {version}). v0 is the implicit \
             pre-versioning state for legacy consumers and is not a build target."
        );
    }

    let dest = Path::new(out_dir).join(out_file);
    let body = format!(
        "/// {doc_lead}\n\
         /// `{source_file}` at build time via `{env_var}`.\n\
         pub const {const_name}: u32 = {version};\n"
    );
    std::fs::write(&dest, body)
        .unwrap_or_else(|e| panic!("failed to write {}: {}", dest.display(), e));
}

/// A build that bakes a cartridge registry identity
/// (`MFR_CARTRIDGE_REGISTRY_URL`) MUST also bake the signing trust triple:
/// the ROOT public key set (`MFR_CARTRIDGE_ROOT_PUBKEYS`, comma-separated —
/// roots sign release-key certificates, which in turn authorize the release
/// key that signs binaries and manifests) and the environment label
/// (`MFR_SIGNING_ENVIRONMENT`, `prod`/`staging`, which certificates are bound
/// to). The registry URL says where artifacts come from; the roots + label
/// say what they must chain to. A registry-baking build missing either would
/// compile a binary whose registry downloads can never verify — a
/// misconfiguration that must fail here, not at a user's first download.
///
/// Valid states:
/// - none set                          => dev build (no registry, no downloads).
/// - all three set, every key decodable, label valid => published build.
/// - roots/label set without registry  => allowed (e.g. test builds).
///
/// Invalid states (hard build failure):
/// - registry set, roots absent/empty or label absent/empty.
/// - any root key not a decodable minisign public key (`RW` + base64,
///   56 chars decoding to 42 bytes).
/// - label not `prod` or `staging`.
fn enforce_signing_pubkey_pairing() {
    println!("cargo:rerun-if-env-changed=MFR_CARTRIDGE_REGISTRY_URL");
    println!("cargo:rerun-if-env-changed=MFR_CARTRIDGE_ROOT_PUBKEYS");
    println!("cargo:rerun-if-env-changed=MFR_SIGNING_ENVIRONMENT");
    println!("cargo:rerun-if-env-changed=CDG_FABRIC_REGISTRY_URL");
    println!("cargo:rerun-if-env-changed=CDG_SCHEMA_BASE_URL");

    let registry = env::var("MFR_CARTRIDGE_REGISTRY_URL")
        .ok()
        .filter(|v| !v.trim().is_empty());
    let roots = env::var("MFR_CARTRIDGE_ROOT_PUBKEYS")
        .ok()
        .filter(|v| !v.trim().is_empty());
    let environment = env::var("MFR_SIGNING_ENVIRONMENT")
        .ok()
        .filter(|v| !v.trim().is_empty());
    let fabric = env::var("CDG_FABRIC_REGISTRY_URL")
        .ok()
        .filter(|v| !v.trim().is_empty());

    if let Some(url) = &registry {
        if roots.is_none() {
            panic!(
                "MFR_CARTRIDGE_REGISTRY_URL is set ({url:?}) but MFR_CARTRIDGE_ROOT_PUBKEYS \
                 is absent or empty. A build baked with a cartridge registry must bake the \
                 root public key set its downloads verify against. Set MFR_CARTRIDGE_ROOT_PUBKEYS \
                 (comma-separated base64 minisign root public keys) in the build environment, or \
                 unset the registry URL for a dev build."
            );
        }
        if environment.is_none() {
            panic!(
                "MFR_CARTRIDGE_REGISTRY_URL is set ({url:?}) but MFR_SIGNING_ENVIRONMENT is \
                 absent or empty. A registry-baking build must carry its signing environment \
                 label ('prod' or 'staging') so release-key certificates bind to it."
            );
        }
        if fabric.is_none() {
            panic!(
                "MFR_CARTRIDGE_REGISTRY_URL is set ({url:?}) but CDG_FABRIC_REGISTRY_URL is \
                 absent or empty. A product build binds the cartridge registry AND the fabric \
                 registry (caps/media/aliases) together — otherwise the shipped CLI silently \
                 resolves caps and aliases against the prod fabric default even in a staging \
                 build. Set CDG_FABRIC_REGISTRY_URL (the workspace build tool exports it when it selects a fabric target: \
                 https://fabric.capdag.com for prod, https://fabric-staging.capdag.com for \
                 staging), or unset the cartridge registry URL for a dev build."
            );
        }
    }
    if let Some(keys) = &roots {
        let mut count = 0usize;
        for key in keys.split(',') {
            let key = key.trim();
            if key.is_empty() {
                panic!(
                    "MFR_CARTRIDGE_ROOT_PUBKEYS contains an empty entry: {keys:?}. Provide a \
                     comma-separated list of base64 minisign root public keys."
                );
            }
            validate_minisign_pubkey(key);
            count += 1;
        }
        if count == 0 {
            panic!("MFR_CARTRIDGE_ROOT_PUBKEYS decodes to zero keys: {keys:?}");
        }
        // Release-key certificates require two distinct trusted roots to verify
        // (2-of-3). A published build must therefore bake at least three roots,
        // so any single root can be lost without making a certificate
        // unverifiable.
        if count < 3 {
            panic!(
                "MFR_CARTRIDGE_ROOT_PUBKEYS has {count} key(s); at least 3 are required \
                 (release-key certificates verify under 2 of 3 trusted roots): {keys:?}"
            );
        }
    }
    if let Some(label) = &environment {
        let label = label.trim();
        if label != "prod" && label != "staging" {
            panic!("MFR_SIGNING_ENVIRONMENT must be 'prod' or 'staging', got {label:?}.");
        }
    }
}

/// Structural validation of a base64 minisign public key: 56 base64 chars
/// decoding to 42 bytes whose first two are the `Ed` signature-algorithm tag.
/// Hand-rolled base64 (no new build-deps) — this is a build-time sanity gate;
/// full cryptographic validation happens in `minisign-verify` at runtime.
fn validate_minisign_pubkey(key: &str) {
    const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let fail = |why: &str| -> ! {
        panic!(
            "MFR_CARTRIDGE_ROOT_PUBKEYS entry is not a valid base64 minisign public key \
             ({why}): {key:?}. Expected the second line of a minisign .pub file (starts \
             with \"RW\")."
        )
    };
    if key.len() != 56 {
        fail("must be 56 base64 characters");
    }
    let mut bytes: Vec<u8> = Vec::with_capacity(42);
    let mut buffer: u32 = 0;
    let mut bits = 0u32;
    for ch in key.bytes() {
        let Some(value) = B64.iter().position(|b| *b == ch) else {
            fail("contains a non-base64 character");
        };
        buffer = (buffer << 6) | value as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            bytes.push((buffer >> bits) as u8);
        }
    }
    if bytes.len() != 42 {
        fail("does not decode to 42 bytes");
    }
    if &bytes[0..2] != b"Ed" {
        fail("does not carry the ed25519 'Ed' algorithm tag");
    }
}

/// Bake the expected content hashes of the build's BUNDLED cartridges
/// (datacartridge / fetchcartridge / modelcartridge — shipped inside the
/// engine/daemon/capdag-CLI binary) into a compile-time constant the discovery
/// path verifies against. Same mechanism as `MFR_FABRIC_MANIFEST_VERSION`: the
/// build pipeline computes the hashes (after building the cartridge binaries,
/// before compiling this crate's consumers) and exports them in
/// `MFR_BUNDLED_CARTRIDGE_HASHES` as a JSON object `{ "<name>": { "<version>":
/// "<sha256>" } }`.
///
/// ABSENT var ⇒ empty set (a build with no bundled cartridges — e.g. plain
/// `cargo test` of capdag — is valid). MALFORMED var ⇒ hard build failure (a
/// pipeline that sets it must set it correctly; a silent empty set would
/// disable integrity checking without anyone noticing).
fn generate_bundled_cartridge_hashes(out_dir: &str) {
    println!("cargo:rerun-if-env-changed=MFR_BUNDLED_CARTRIDGE_HASHES");

    let entries: Vec<(String, String, String)> = match env::var("MFR_BUNDLED_CARTRIDGE_HASHES") {
        Err(_) => Vec::new(),
        Ok(raw) if raw.trim().is_empty() => Vec::new(),
        Ok(raw) => parse_bundled_cartridge_hashes(&raw),
    };

    let mut body = String::from(
        "/// Expected content hashes of this build's bundled cartridges, baked from\n\
         /// `MFR_BUNDLED_CARTRIDGE_HASHES` at build time. `(name, version, sha256)`.\n\
         /// Empty when no cartridges were bundled. Discovery verifies any cartridge\n\
         /// marked `installed_from: bundle` against this set.\n\
         pub const BUNDLED_CARTRIDGE_HASHES: &[(&str, &str, &str)] = &[\n",
    );
    for (name, version, sha256) in &entries {
        // Values are validated below to be hex/identifier-safe, but emit via
        // escaped string literals regardless so codegen can never break.
        body.push_str(&format!("    ({:?}, {:?}, {:?}),\n", name, version, sha256));
    }
    body.push_str("];\n");

    let dest = Path::new(out_dir).join("bundled_cartridge_hashes.rs");
    std::fs::write(&dest, body)
        .unwrap_or_else(|e| panic!("failed to write {}: {}", dest.display(), e));
}

/// Parse `{ "<name>": { "<version>": "<sha256>" } }` into a flat, sorted
/// `(name, version, sha256)` list. Minimal hand-rolled validation (no serde
/// dep in build.rs): every leaf must be a 64-char lowercase hex string. Any
/// structural or value error panics the build.
fn parse_bundled_cartridge_hashes(raw: &str) -> Vec<(String, String, String)> {
    let value: serde_json::Value = serde_json::from_str(raw).unwrap_or_else(|e| {
        panic!("MFR_BUNDLED_CARTRIDGE_HASHES is not valid JSON: {e}");
    });
    let obj = value.as_object().unwrap_or_else(|| {
        panic!("MFR_BUNDLED_CARTRIDGE_HASHES must be a JSON object {{name: {{version: sha256}}}}");
    });
    let mut out: Vec<(String, String, String)> = Vec::new();
    for (name, versions) in obj {
        let versions = versions.as_object().unwrap_or_else(|| {
            panic!("MFR_BUNDLED_CARTRIDGE_HASHES['{name}'] must be an object {{version: sha256}}");
        });
        for (version, sha) in versions {
            let sha = sha.as_str().unwrap_or_else(|| {
                panic!(
                    "MFR_BUNDLED_CARTRIDGE_HASHES['{name}']['{version}'] must be a string sha256"
                );
            });
            let is_hex64 = sha.len() == 64
                && sha
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
            if !is_hex64 {
                panic!(
                    "MFR_BUNDLED_CARTRIDGE_HASHES['{name}']['{version}'] must be a 64-char lowercase hex sha256, got {sha:?}"
                );
            }
            out.push((name.clone(), version.clone(), sha.to_string()));
        }
    }
    out.sort();
    out
}
