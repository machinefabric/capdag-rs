//! What proves a bundled cartridge, and what refuses to.
//!
//! Two halves, tested against two different things, deliberately.
//!
//! The LOADER is tested against a real committed chain — a certificate two
//! roots signed and a release key's signature over the exact manifest bytes,
//! generated once and checked in under `tests/fixtures/`. capdag holds no
//! signing code, so a test that built its own signature would be testing a
//! signature it also invented.
//!
//! The CHECK is tested against a directory the test lays down and hashes
//! itself. It needs no signature — holding a directory to a recorded hash is
//! not a cryptographic act — and doing it this way means neither half is ever
//! measured against a stub of the other.

use super::*;
use std::fs;
use tempfile::tempdir;

const ROOT_A: &str = include_str!("../../tests/fixtures/nocommit/signing/root_a.pubkey");
const ROOT_B: &str = include_str!("../../tests/fixtures/nocommit/signing/root_b.pubkey");
const ROOT_C: &str = include_str!("../../tests/fixtures/nocommit/signing/root_c.pubkey");
const BUNDLE: &[u8] = include_bytes!("../../tests/fixtures/nocommit/signing/bundle.json");
const BUNDLE_SIG: &str = include_str!("../../tests/fixtures/nocommit/signing/bundle.json.sig");
const META: &str = include_str!("../../tests/fixtures/nocommit/signing/meta.json");

#[derive(serde::Deserialize)]
struct Meta {
    issued_at: u64,
    not_after: u64,
    environment: String,
}

fn meta() -> Meta {
    serde_json::from_str(META).expect("meta.json must parse")
}

/// A moment the fixture certificate is valid at.
fn mid_validity() -> u64 {
    let m = meta();
    (m.issued_at + m.not_after) / 2
}

fn trust(environment: &str) -> crate::bifaci::release_cert::RegistryTrust {
    crate::bifaci::release_cert::RegistryTrust {
        root_pubkeys: vec![
            ROOT_A.to_string(),
            ROOT_B.to_string(),
            ROOT_C.to_string(),
        ],
        environment: environment.to_string(),
    }
}

/// A bundled-cartridges root holding the committed manifest and its signature.
fn bundle_root() -> tempfile::TempDir {
    let dir = tempdir().expect("a scratch directory");
    fs::write(dir.path().join(BUNDLE_MANIFEST_FILE), BUNDLE).expect("write the manifest");
    fs::write(dir.path().join(BUNDLE_MANIFEST_SIG_FILE), BUNDLE_SIG).expect("write the signature");
    dir
}

/// TEST1922: a bundle manifest is believed only when a real 2-of-3 chain says
/// so, and every way of breaking that chain refuses it.
///
/// This is the whole of what replaced "macOS trusts Gatekeeper". If any of
/// these refusals were a pass, a bundled cartridge would be hosted on the say-so
/// of whoever last wrote the file — which is the state the old platform split
/// left macOS in.
#[test]
fn test1922_a_bundle_manifest_is_only_believed_under_a_real_chain() {
    let dir = bundle_root();
    let environment = meta().environment;

    let manifest = load_verified(dir.path(), Some(&trust(&environment)), mid_validity())
        .expect("the committed chain must verify");
    assert_eq!(manifest.format, BUNDLE_MANIFEST_FORMAT);
    assert_eq!(manifest.environment, environment);
    assert_eq!(manifest.cartridges.len(), 1);
    assert_eq!(manifest.cartridges[0].name, "fixturecartridge");

    // A build that trusts nothing can prove nothing. Not a pass with a warning
    // — a bundled cartridge in such a build does not run.
    assert!(matches!(
        load_verified(dir.path(), None, mid_validity()),
        Err(BundleError::NoTrust)
    ));

    // One byte of the manifest. The signature binds exact bytes, so this is
    // the tamper the whole mechanism exists to catch.
    let tampered = tempdir().expect("a scratch directory");
    let mut bytes = BUNDLE.to_vec();
    let at = bytes.len() / 2;
    bytes[at] ^= 0x01;
    fs::write(tampered.path().join(BUNDLE_MANIFEST_FILE), &bytes).expect("write");
    fs::write(tampered.path().join(BUNDLE_MANIFEST_SIG_FILE), BUNDLE_SIG).expect("write");
    assert!(
        matches!(
            load_verified(tampered.path(), Some(&trust(&environment)), mid_validity()),
            Err(BundleError::Signature(_))
        ),
        "a tampered manifest was accepted"
    );

    // Roots that did not sign it. Two of the three are needed and none of
    // these is one of them.
    let stranger = crate::bifaci::release_cert::RegistryTrust {
        root_pubkeys: vec![
            include_str!("../../tests/fixtures/nocommit/signing/wrong.pubkey").to_string(),
        ],
        environment: environment.clone(),
    };
    assert!(
        matches!(
            load_verified(dir.path(), Some(&stranger), mid_validity()),
            Err(BundleError::Signature(_))
        ),
        "a manifest verified under roots that never signed it"
    );

    // The other environment. The certificate is bound to one, so a staging
    // build cannot be handed a prod bundle even with every signature intact.
    let other = if environment == "prod" { "staging" } else { "prod" };
    assert!(
        matches!(
            load_verified(dir.path(), Some(&trust(other)), mid_validity()),
            Err(BundleError::Signature(_))
        ),
        "a bundle for another environment was accepted"
    );

    // After the certificate expires. A bundle that verified forever would
    // outlive the key that vouched for it.
    assert!(
        matches!(
            load_verified(dir.path(), Some(&trust(&environment)), meta().not_after + 1),
            Err(BundleError::Signature(_))
        ),
        "an expired certificate still authorized the bundle"
    );
}

/// TEST1923: an unsigned or absent manifest proves nothing, and says which.
///
/// The two states are told apart because the remedies differ: a build that
/// never wrote a manifest is a broken build, and a manifest whose signature is
/// missing is a build that stopped half way through releasing.
#[test]
fn test1923_an_unsigned_or_absent_bundle_manifest_is_refused() {
    let environment = meta().environment;

    let empty = tempdir().expect("a scratch directory");
    assert!(matches!(
        load_verified(empty.path(), Some(&trust(&environment)), mid_validity()),
        Err(BundleError::Missing(_))
    ));

    let unsigned = tempdir().expect("a scratch directory");
    fs::write(unsigned.path().join(BUNDLE_MANIFEST_FILE), BUNDLE).expect("write");
    assert!(
        matches!(
            load_verified(unsigned.path(), Some(&trust(&environment)), mid_validity()),
            Err(BundleError::Unsigned(_))
        ),
        "a manifest with no signature beside it was accepted"
    );
}

/// Lay down a cartridge directory with `contents`, and answer its path.
fn cartridge_dir(root: &std::path::Path, contents: &[(&str, &str)]) -> std::path::PathBuf {
    let dir = root.join("cart/1.0.0");
    fs::create_dir_all(&dir).expect("create the cartridge directory");
    // Excluded from the hash by `hash_cartridge_directory`, and present because
    // a real cartridge has one.
    fs::write(dir.join("cartridge.json"), r#"{"name":"cart"}"#).expect("write cartridge.json");
    for (name, body) in contents {
        fs::write(dir.join(name), body).expect("write a file");
    }
    dir
}

/// TEST1924: a bundled cartridge is held to the bytes the manifest recorded.
///
/// The manifest is built here rather than loaded, because what is under test is
/// the CHECK. Three ways to fail it, and each is a different thing that went
/// wrong: the cartridge was changed after the build recorded it, the build
/// shipped something it never recorded, or a stale copy of another version was
/// left behind.
#[test]
fn test1924_a_bundled_cartridge_must_match_what_the_manifest_recorded() {
    let root = tempdir().expect("a scratch directory");
    let dir = cartridge_dir(root.path(), &[("cart", "the entry point")]);
    let recorded = crate::bifaci::cartridge_json::hash_cartridge_directory(&dir).expect("hash it");

    let manifest = BundleManifest::new(
        "prod",
        vec![BundledCartridge {
            name: "cart".to_string(),
            version: "1.0.0".to_string(),
            channel: "release".to_string(),
            sha256: recorded.clone(),
        }],
    );
    verify_cartridge(&manifest, "cart", "1.0.0", &dir).expect("an unchanged cartridge passes");

    // Changed after the build recorded it.
    fs::write(dir.join("cart"), "something else").expect("tamper");
    let why = verify_cartridge(&manifest, "cart", "1.0.0", &dir)
        .expect_err("a changed cartridge must be refused");
    assert!(
        matches!(why, BundleError::ContentMismatch { ref expected, .. } if *expected == recorded),
        "the refusal must name what was recorded, said: {why}"
    );

    // Shipped but never recorded.
    let why = verify_cartridge(&manifest, "other", "1.0.0", &dir)
        .expect_err("a cartridge the manifest does not list must be refused");
    assert!(matches!(why, BundleError::NotListed { .. }), "{why}");

    // The right cartridge at the wrong version — a stale copy from an earlier
    // build, which a bare per-binary signature would have accepted because it
    // was validly signed once.
    let why = verify_cartridge(&manifest, "cart", "0.9.0", &dir)
        .expect_err("a version the manifest does not list must be refused");
    assert!(matches!(why, BundleError::NotListed { .. }), "{why}");
}

/// TEST1925: `cartridge.json` is outside the hash, so writing the manifest
/// cannot change what the manifest attests.
///
/// Not a detail. The build stages a cartridge, hashes it, and records the
/// result; if the hash covered the metadata file the build also writes, the
/// recorded value would describe a directory that no longer exists by the time
/// anyone checks it.
#[test]
fn test1925_the_recorded_hash_does_not_cover_the_metadata_file() {
    let root = tempdir().expect("a scratch directory");
    let dir = cartridge_dir(root.path(), &[("cart", "the entry point")]);
    let before = crate::bifaci::cartridge_json::hash_cartridge_directory(&dir).expect("hash it");

    fs::write(dir.join("cartridge.json"), r#"{"name":"cart","version":"1.0.0"}"#)
        .expect("rewrite the metadata");
    let after = crate::bifaci::cartridge_json::hash_cartridge_directory(&dir).expect("hash it again");
    assert_eq!(
        before, after,
        "rewriting cartridge.json changed the hash, so a build cannot record what it ships"
    );
}

/// TEST1926: a manifest is written in a stable order, so the same tree signs
/// to the same bytes.
///
/// A manifest that reordered itself would make every build produce a different
/// document and therefore a different signature — and a diff of the file would
/// show a change where nothing changed.
#[test]
fn test1926_a_manifest_is_written_in_a_stable_order() {
    let one = BundleManifest::new(
        "prod",
        vec![
            BundledCartridge {
                name: "zeta".into(),
                version: "1.0.0".into(),
                channel: "release".into(),
                sha256: "a".repeat(64),
            },
            BundledCartridge {
                name: "alpha".into(),
                version: "2.0.0".into(),
                channel: "release".into(),
                sha256: "b".repeat(64),
            },
        ],
    );
    let other = BundleManifest::new(
        "prod",
        vec![
            BundledCartridge {
                name: "alpha".into(),
                version: "2.0.0".into(),
                channel: "release".into(),
                sha256: "b".repeat(64),
            },
            BundledCartridge {
                name: "zeta".into(),
                version: "1.0.0".into(),
                channel: "release".into(),
                sha256: "a".repeat(64),
            },
        ],
    );
    assert_eq!(one.to_bytes().unwrap(), other.to_bytes().unwrap());
    assert_eq!(one.cartridges[0].name, "alpha");
}

/// TEST1927: the manifest and its signature are not unmanaged files.
///
/// Discovery reports anything in a cartridges root that does not belong there.
/// These two belong there, and a warning about them on every startup is how an
/// operator learns to ignore the one that means something.
#[test]
fn test1927_the_manifest_files_are_recognised_as_managed() {
    assert!(is_manifest_file(BUNDLE_MANIFEST_FILE));
    assert!(is_manifest_file(BUNDLE_MANIFEST_SIG_FILE));
    assert!(!is_manifest_file("something-else.json"));
    assert!(!is_manifest_file("bundle.json.bak"));
}
