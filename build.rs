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
        if label != "prod" && label != "staging" && label != DEV_ENVIRONMENT {
            panic!(
                "MFR_SIGNING_ENVIRONMENT must be 'prod', 'staging' or '{DEV_ENVIRONMENT}', \
                 got {label:?}."
            );
        }
        // ── The one-environment guard ────────────────────────────────────────
        //
        // A build bakes exactly one environment's roots, and this is what makes
        // the `dev` environment safe to have at all.
        //
        // The dev signing keys live on developer machines and in CI, because
        // that is the whole point of them — a local build has to be able to
        // sign its own bundled cartridges. They are harmless because nothing
        // released trusts them: a prod build bakes prod roots, and a
        // dev-signed bundle fails its chain there.
        //
        // That containment is one mistake deep. A build that baked BOTH would
        // turn a laptop-resident key into a production forgery key, and the
        // mistake would be invisible — everything would verify, which is
        // exactly what it must not do. So the environment label decides the
        // roots, there is no way to bake a second set, and a published build
        // may never carry the dev label.
        if label == DEV_ENVIRONMENT {
            let published = env::var("MFR_PUBLISHED_BUILD")
                .ok()
                .filter(|v| !v.trim().is_empty() && v.trim() != "0");
            if let Some(marker) = published {
                panic!(
                    "MFR_SIGNING_ENVIRONMENT is '{DEV_ENVIRONMENT}' and this build is marked \
                     published (MFR_PUBLISHED_BUILD={marker:?}). The dev signing keys live on \
                     developer machines and in CI; a released build that trusted them would \
                     accept anything any of those machines signed. Build with \
                     MFR_SIGNING_ENVIRONMENT=prod or staging."
                );
            }
        }
    }
}

/// The signing environment a local or CI build uses.
///
/// A real environment, with its own roots and its own certificate, rather than
/// a switch that turns verification off. That is what lets one rule — "a
/// bundled cartridge is proven by this build's signed bundle manifest" — hold
/// on every build of every platform, instead of holding only where a release
/// key was available.
pub(crate) const DEV_ENVIRONMENT: &str = "dev";

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
