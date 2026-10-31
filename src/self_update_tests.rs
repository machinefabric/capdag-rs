//! `capdag self update`, against a registry the tests serve from memory and a
//! release line they sign themselves.
//!
//! The certificate and signature formats are written out here, not taken from
//! the verifier: a test that asked the code under test what a certificate
//! looks like would agree with it whatever it said.

use super::*;
use ed25519_dalek::{Signer, SigningKey};
use serde_json::json;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Mutex;

const REGISTRY: &str = "https://release.example";
const PLATFORM: &str = "linux-x86_64";
const ISSUED_AT: u64 = 1_700_000_000;
const NOT_AFTER: u64 = ISSUED_AT + 365 * 86_400;
const NOW: u64 = ISSUED_AT + 86_400;

/// An ed25519 key with a minisign key id.
struct Key(SigningKey, [u8; 8]);

impl Key {
    fn new(seed: u8) -> Key {
        Key(SigningKey::from_bytes(&[seed; 32]), [seed; 8])
    }

    fn id(&self) -> String {
        self.1.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// The public key in minisign's form: `Ed`, the key id, the ed25519 key.
    fn public(&self) -> String {
        let mut bytes = b"Ed".to_vec();
        bytes.extend_from_slice(&self.1);
        bytes.extend_from_slice(self.0.verifying_key().as_bytes());
        base64(&bytes)
    }

    fn sign(&self, bytes: &[u8]) -> String {
        base64(&self.0.sign(bytes).to_bytes())
    }
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n = (chunk[0] as u32) << 16
            | (*chunk.get(1).unwrap_or(&0) as u32) << 8
            | *chunk.get(2).unwrap_or(&0) as u32;
        for (i, shift) in [18, 12, 6, 0].into_iter().enumerate() {
            if i <= chunk.len() {
                out.push(ALPHABET[(n >> shift) as usize & 63] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// A release line: three roots and a release key two of them certify.
struct Line {
    roots: [Key; 3],
    release: Key,
}

impl Line {
    fn new() -> Line {
        Line {
            roots: [Key::new(1), Key::new(2), Key::new(3)],
            release: Key::new(9),
        }
    }

    fn roots(&self) -> Vec<String> {
        self.roots.iter().map(Key::public).collect()
    }

    fn trust(&self) -> Trust {
        let roots = self.roots();
        let roots: Vec<&str> = roots.iter().map(String::as_str).collect();
        Trust::new(&roots, "prod", crate::bifaci::trust::content_formats()).expect("three distinct roots")
    }

    /// The same roots, as the charter trust — MachineFabric's roots sign both
    /// a prod line's certificates and every charter's.
    fn charter_trust(&self) -> CharterTrust {
        let roots = self.roots();
        let roots: Vec<&str> = roots.iter().map(String::as_str).collect();
        crate::bifaci::trust::charter_trust_from(&roots).expect("three distinct roots")
    }

    /// The prod certificate of this line's release key, with two root
    /// signatures, as `(certificate, root_signatures)`.
    fn certificate(&self) -> (String, Vec<serde_json::Value>) {
        let certificate = json!({
            "format": "machinefabric-release-key-cert/1",
            "release_pubkey": self.release.public(),
            "key_id": self.release.id(),
            "environment": "prod",
            "issued_at": ISSUED_AT,
            "not_after": NOT_AFTER,
        })
        .to_string();
        let root_signatures: Vec<_> = self.roots[..2]
            .iter()
            .map(|root| json!({ "root_key_id": root.id(), "signature": root.sign(certificate.as_bytes()) }))
            .collect();
        (certificate, root_signatures)
    }

    /// The signature file of `manifest`, as a publish writes it.
    fn sign(&self, manifest: &[u8]) -> Vec<u8> {
        let (certificate, root_signatures) = self.certificate();
        json!({
            "format": "machinefabric-manifest-sig/1",
            "certificates": [{ "certificate": certificate, "root_signatures": root_signatures }],
            "manifest_signature": { "key_id": self.release.id(), "signature": self.release.sign(manifest) },
        })
        .to_string()
        .into_bytes()
    }

    /// The release registry's charter at [`REGISTRY`], for `environment`, with
    /// its manifests at `manifest_template`, and its signature file — as
    /// `sdx charter issue` writes them.
    fn charter(&self, environment: &str, manifest_template: &str) -> (Vec<u8>, Vec<u8>) {
        let body = json!({
            "format": "machinefabric-registry-charter/1",
            "anchor": REGISTRY,
            "kind": "releases",
            "slug": "release.example",
            "environment": environment,
            "locations": { "manifest": manifest_template },
            "issued_at": ISSUED_AT,
            "not_after": null,
        })
        .to_string()
        .into_bytes();
        let (certificate, root_signatures) = self.certificate();
        let signature = json!({
            "format": "machinefabric-charter-sig/1",
            "certificates": [{ "certificate": certificate, "root_signatures": root_signatures }],
            "charter_signature": { "key_id": self.release.id(), "signature": self.release.sign(&body) },
        })
        .to_string()
        .into_bytes();
        (body, signature)
    }
}

/// Where a release registry's charter and its signature are.
fn charter_urls() -> [String; 2] {
    [format!("{REGISTRY}/charter.json"), format!("{REGISTRY}/charter.json.sig")]
}

/// The command's manifest with `version` on the release channel for
/// [`PLATFORM`], and nothing on the nightly channel.
fn manifest(version: &str, url: &str, sha256: &str, size: u64) -> Vec<u8> {
    json!({
        "schemaVersion": "5.0",
        "appId": "capdag-cli",
        "name": "capdag",
        "lastUpdated": "2026-10-13T00:00:00Z",
        "channels": {
            "release": { "latestVersion": version, "versions": { version: {
                "publishedAt": "2026-10-13T00:00:00Z",
                "platforms": { PLATFORM: { "label": "Linux", "installers": [
                    { "type": "targz", "label": "tar.gz", "name": url.rsplit('/').next().unwrap(), "path": url, "sha256": sha256, "size": size }
                ] } },
            } } },
            "nightly": { "latestVersion": "", "versions": {} },
        },
    })
    .to_string()
    .into_bytes()
}

/// An executable that says it is `capdag <version>`.
fn executable(path: &Path, version: &str) {
    std::fs::write(path, format!("#!/bin/sh\necho 'capdag {version}'\n")).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// A release archive: `<root>/capdag` saying `version`, and
/// `<root>/bundled-cartridges/` holding one file that names `version`.
fn archive(scratch: &Path, root: &str, version: &str, with_cartridges: bool) -> Vec<u8> {
    let tree = scratch.join(format!("tree-{root}-{version}-{with_cartridges}"));
    let bundle = tree.join(root);
    std::fs::create_dir_all(&bundle).unwrap();
    executable(&bundle.join("capdag"), version);
    if with_cartridges {
        std::fs::create_dir_all(bundle.join(CARTRIDGES).join("txtcartridge")).unwrap();
        std::fs::write(bundle.join(CARTRIDGES).join("txtcartridge").join("of"), version).unwrap();
    }
    let out = tree.join("bundle.tar.gz");
    let made = crate::bifaci::launch::command(Path::new("tar"))
        .arg("-czf")
        .arg(&out)
        .arg("-C")
        .arg(&tree)
        .arg(root)
        .output()
        .unwrap();
    assert!(made.status.success(), "{}", String::from_utf8_lossy(&made.stderr));
    std::fs::read(&out).unwrap()
}

/// A registry served from memory, recording what was asked of it.
#[derive(Default)]
struct Served {
    files: BTreeMap<String, Vec<u8>>,
    asked: Mutex<Vec<String>>,
}

impl Served {
    /// A registry publishing `version` as `archive`, signed by `line`.
    fn publishing(line: &Line, version: &str, archive: &[u8]) -> Served {
        let url = Served::archive_url(version);
        let digest: String = Sha256::digest(archive).iter().map(|b| format!("{b:02x}")).collect();
        let manifest = manifest(version, &url, &digest, archive.len() as u64);
        let mut served = Served::chartered(line);
        served.files.insert(format!("{REGISTRY}/capdag-cli/manifest.sig"), line.sign(&manifest));
        served.files.insert(format!("{REGISTRY}/capdag-cli/manifest"), manifest);
        served.files.insert(url, archive.to_vec());
        served
    }

    /// A release registry with its prod charter and nothing published.
    fn chartered(line: &Line) -> Served {
        let (body, signature) = line.charter("prod", &format!("{REGISTRY}/{{app_id}}/manifest"));
        let [charter, charter_sig] = charter_urls();
        let mut served = Served::default();
        served.files.insert(charter, body);
        served.files.insert(charter_sig, signature);
        served
    }

    fn archive_url(version: &str) -> String {
        format!("{REGISTRY}/capdag-cli/release/{version}/capdag-{version}-{PLATFORM}.tar.gz")
    }

    fn asked(&self) -> Vec<String> {
        self.asked.lock().unwrap().clone()
    }

    async fn update(&self, line: &Line, installed: &Path, current: &str) -> Result<Updated, UpdateError> {
        let trust = line.trust();
        update_from(
            &Installation {
                registry: REGISTRY,
                channel: "release",
                trust: &trust,
                charter_trust: &line.charter_trust(),
                now: NOW,
                installed,
                current,
                platform: PLATFORM,
            },
            |url| async move {
                self.asked.lock().unwrap().push(url.clone());
                Ok(self.files.get(&url).cloned())
            },
        )
        .await
    }
}

/// An installed bundle of `version`: the executable, its cartridges, and a
/// file of the user's own beside them.
fn installed(scratch: &Path, version: &str) -> PathBuf {
    let bundle = scratch.join("opt");
    std::fs::create_dir_all(bundle.join(CARTRIDGES).join("txtcartridge")).unwrap();
    std::fs::write(bundle.join(CARTRIDGES).join("txtcartridge").join("of"), version).unwrap();
    std::fs::write(bundle.join("notes.txt"), "mine").unwrap();
    executable(&bundle.join("capdag"), version);
    bundle.join("capdag")
}

fn says(executable: &Path) -> String {
    let out = crate::bifaci::launch::command(executable).arg("--version").output().unwrap();
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// The version the installed cartridges are of.
fn cartridges_of(executable: &Path) -> String {
    std::fs::read_to_string(executable.parent().unwrap().join(CARTRIDGES).join("txtcartridge").join("of")).unwrap()
}

/// What is in the bundle's directory.
fn beside(executable: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(executable.parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// The installed bundle is exactly the one of `version`, with nothing of an
/// update left beside it.
#[track_caller]
fn assert_installed(executable: &Path, version: &str) {
    assert_eq!(says(executable), format!("capdag {version}"));
    assert_eq!(cartridges_of(executable), version);
    assert_eq!(beside(executable), [CARTRIDGES, "capdag", "notes.txt"]);
}

/// TEST12690: a newer release replaces the bundle where it is
#[tokio::test]
async fn test12690_a_newer_release_replaces_the_bundle_where_it_is() {
    let scratch = tempfile::tempdir().unwrap();
    let line = Line::new();
    let capdag = installed(scratch.path(), "1.100.0");
    let served = Served::publishing(&line, "1.200.0", &archive(scratch.path(), ARCHIVE_ROOT, "1.200.0", true));
    let updated = served.update(&line, &capdag, "1.100.0").await.unwrap();
    assert_eq!(updated, Updated { from: "1.100.0".into(), to: "1.200.0".into() });
    // The executable and its cartridges are the new release's, together; the
    // file that was not the bundle's is where it was.
    assert_installed(&capdag, "1.200.0");
    assert_eq!(std::fs::read_to_string(capdag.with_file_name("notes.txt")).unwrap(), "mine");
}

/// TEST12691: the latest release is left as it is and nothing is downloaded
#[tokio::test]
async fn test12691_the_latest_release_is_left_as_it_is_and_nothing_is_downloaded() {
    let scratch = tempfile::tempdir().unwrap();
    let line = Line::new();
    let bundle = archive(scratch.path(), ARCHIVE_ROOT, "1.200.0", true);
    // The same, a later patch, and — compared as text, the lesser — 1.1000.0.
    for current in ["1.200.0", "1.200.1", "1.1000.0"] {
        let capdag = installed(scratch.path(), current);
        let served = Served::publishing(&line, "1.200.0", &bundle);
        let updated = served.update(&line, &capdag, current).await.unwrap();
        assert_eq!(updated, Updated { from: current.into(), to: current.into() });
        assert_installed(&capdag, current);
        let [charter, charter_sig] = charter_urls();
        assert_eq!(
            served.asked(),
            [charter, charter_sig, format!("{REGISTRY}/capdag-cli/manifest"), format!("{REGISTRY}/capdag-cli/manifest.sig")],
            "only the charter, the manifest and their signatures were read"
        );
    }
}

/// TEST12692: a manifest this capdag does not trust changes nothing
#[tokio::test]
async fn test12692_a_manifest_this_capdag_does_not_trust_changes_nothing() {
    let scratch = tempfile::tempdir().unwrap();
    let line = Line::new();
    let capdag = installed(scratch.path(), "1.100.0");
    let mut served = Served::publishing(&line, "1.200.0", &archive(scratch.path(), ARCHIVE_ROOT, "1.200.0", true));
    let manifest_url = format!("{REGISTRY}/capdag-cli/manifest");
    let manifest = served.files[&manifest_url].clone();

    // Signed by a line whose roots this capdag was not built with.
    let other = Line { roots: [Key::new(4), Key::new(5), Key::new(6)], release: Key::new(9) };
    served.files.insert(format!("{manifest_url}.sig"), other.sign(&manifest));
    let e = served.update(&line, &capdag, "1.100.0").await.unwrap_err();
    assert!(matches!(e, UpdateError::Untrusted(_)), "{e}");
    assert!(e.to_string().contains("not one this capdag trusts"), "{e}");
    assert_eq!(served.asked().len(), 4, "no bundle is fetched on the word of an untrusted manifest");

    // Signed by this line, and then changed by one byte.
    served.files.insert(format!("{manifest_url}.sig"), line.sign(&manifest));
    let mut changed = manifest.clone();
    let at = changed.iter().position(|b| *b == b'2').unwrap();
    changed[at] = b'3';
    served.files.insert(manifest_url.clone(), changed);
    let e = served.update(&line, &capdag, "1.100.0").await.unwrap_err();
    assert!(matches!(e, UpdateError::Untrusted(_)), "{e}");

    // Published with no signature at all.
    served.files.remove(&format!("{manifest_url}.sig"));
    let e = served.update(&line, &capdag, "1.100.0").await.unwrap_err();
    assert!(matches!(e, UpdateError::Untrusted(_)), "{e}");
    assert!(e.to_string().contains("published without a signature"), "{e}");

    assert_installed(&capdag, "1.100.0");
}

/// TEST12693: a bundle that is not the one the manifest describes is discarded
#[tokio::test]
async fn test12693_a_bundle_that_is_not_the_one_the_manifest_describes_is_discarded() {
    let scratch = tempfile::tempdir().unwrap();
    let line = Line::new();
    let capdag = installed(scratch.path(), "1.100.0");
    let honest = archive(scratch.path(), ARCHIVE_ROOT, "1.200.0", true);
    let url = Served::archive_url("1.200.0");

    // Another file of the same size.
    let mut served = Served::publishing(&line, "1.200.0", &honest);
    let mut swapped = honest.clone();
    let middle = swapped.len() / 2;
    swapped[middle] ^= 1;
    served.files.insert(url.clone(), swapped);
    let e = served.update(&line, &capdag, "1.100.0").await.unwrap_err();
    assert!(matches!(e, UpdateError::Untrusted(_)) && e.to_string().contains("has SHA-256"), "{e}");

    // One of another size.
    let mut served = Served::publishing(&line, "1.200.0", &honest);
    served.files.insert(url.clone(), honest[..honest.len() - 1].to_vec());
    let e = served.update(&line, &capdag, "1.100.0").await.unwrap_err();
    assert!(e.to_string().contains("bytes; the release manifest says"), "{e}");

    // One the manifest lists and the registry does not have.
    let mut served = Served::publishing(&line, "1.200.0", &honest);
    served.files.remove(&url);
    let e = served.update(&line, &capdag, "1.100.0").await.unwrap_err();
    assert!(matches!(e, UpdateError::Unavailable(_)), "{e}");

    assert_installed(&capdag, "1.100.0");
}

/// TEST12694: a bundle that is not the release it was published as is not installed
#[tokio::test]
async fn test12694_a_bundle_that_is_not_the_release_it_was_published_as_is_not_installed() {
    let scratch = tempfile::tempdir().unwrap();
    let line = Line::new();
    let capdag = installed(scratch.path(), "1.100.0");

    // Signed, intact, and holding another version's executable.
    let served = Served::publishing(&line, "1.200.0", &archive(scratch.path(), ARCHIVE_ROOT, "1.150.0", true));
    let e = served.update(&line, &capdag, "1.100.0").await.unwrap_err();
    assert!(matches!(e, UpdateError::Untrusted(_)), "{e}");
    assert!(e.to_string().contains("says \"capdag 1.150.0\", not \"capdag 1.200.0\""), "{e}");
    assert_installed(&capdag, "1.100.0");

    // An executable with no cartridges is not a bundle; nor is an archive of
    // something else.
    for not_a_bundle in [
        archive(scratch.path(), ARCHIVE_ROOT, "1.200.0", false),
        archive(scratch.path(), "other", "1.200.0", true),
    ] {
        let served = Served::publishing(&line, "1.200.0", &not_a_bundle);
        let e = served.update(&line, &capdag, "1.100.0").await.unwrap_err();
        assert!(matches!(e, UpdateError::Untrusted(_)), "{e}");
        assert!(e.to_string().contains("is not a capdag bundle"), "{e}");
        assert_installed(&capdag, "1.100.0");
    }
}

/// TEST12695: a package managers capdag is left to the package manager
#[tokio::test]
async fn test12695_a_package_managers_capdag_is_left_to_the_package_manager() {
    let scratch = tempfile::tempdir().unwrap();
    let line = Line::new();
    let capdag = installed(scratch.path(), "1.100.0");
    let served = Served::publishing(&line, "1.200.0", &archive(scratch.path(), ARCHIVE_ROOT, "1.200.0", true));
    let record = capdag.with_file_name(INSTALLED_BY);
    for (manager, command) in [
        ("Homebrew\n", "`brew upgrade machinefabric/tap/capdag`"),
        ("apt\n", "`sudo apt update && sudo apt install --only-upgrade capdag`"),
        ("dnf", "`sudo dnf upgrade capdag`"),
        ("pacman\n", "update the capdag package with pacman"),
    ] {
        std::fs::write(&record, manager).unwrap();
        let e = served.update(&line, &capdag, "1.100.0").await.unwrap_err();
        assert!(matches!(e, UpdateError::Unavailable(_)), "{e}");
        let text = e.to_string();
        assert!(text.contains(&format!("installed by {}", manager.trim())) && text.contains(command), "{text}");
    }
    // A record that names nobody is still a package's.
    std::fs::write(&record, "\n").unwrap();
    let e = served.update(&line, &capdag, "1.100.0").await.unwrap_err();
    assert!(e.to_string().contains("does not name the package manager"), "{e}");

    assert_eq!(says(&capdag), "capdag 1.100.0");
    assert_eq!(cartridges_of(&capdag), "1.100.0");
    assert!(served.asked().is_empty(), "the registry is not asked about a bundle that is not replaced");
}

/// TEST12696: an update that stopped with the cartridges moved aside is put back first
#[tokio::test]
async fn test12696_an_update_that_stopped_with_the_cartridges_moved_aside_is_put_back_first() {
    let scratch = tempfile::tempdir().unwrap();
    let line = Line::new();
    let capdag = installed(scratch.path(), "1.200.0");
    let bundle = capdag.parent().unwrap();
    // Where an update leaves a bundle when it is killed between moving the
    // installed cartridges aside and putting the new ones in.
    let staging = bundle.join(STAGING);
    std::fs::create_dir_all(staging.join(ARCHIVE_ROOT)).unwrap();
    std::fs::rename(bundle.join(CARTRIDGES), staging.join(PREVIOUS)).unwrap();
    std::fs::write(staging.join("bundle"), b"a download").unwrap();

    // The next update finds nothing newer — and the bundle is whole again.
    let served = Served::publishing(&line, "1.200.0", &archive(scratch.path(), ARCHIVE_ROOT, "1.200.0", true));
    let updated = served.update(&line, &capdag, "1.200.0").await.unwrap();
    assert_eq!(updated, Updated { from: "1.200.0".into(), to: "1.200.0".into() });
    assert_installed(&capdag, "1.200.0");

    // A staging directory left beside cartridges that are in place is a
    // download nobody installed: it is cleared, and they are not touched.
    std::fs::create_dir_all(staging.join(PREVIOUS)).unwrap();
    std::fs::write(staging.join(PREVIOUS).join("stale"), b"").unwrap();
    served.update(&line, &capdag, "1.200.0").await.unwrap();
    assert_installed(&capdag, "1.200.0");
}

/// TEST12697: an executable that cannot be replaced leaves the installed bundle whole
#[tokio::test]
async fn test12697_an_executable_that_cannot_be_replaced_leaves_the_installed_bundle_whole() {
    let scratch = tempfile::tempdir().unwrap();
    let line = Line::new();
    let capdag = installed(scratch.path(), "1.100.0");
    // In the executable's place, something a file cannot be renamed over: the
    // last step of an install fails after the cartridges were swapped.
    std::fs::remove_file(&capdag).unwrap();
    std::fs::create_dir(&capdag).unwrap();
    std::fs::write(capdag.join("occupied"), b"").unwrap();

    let served = Served::publishing(&line, "1.200.0", &archive(scratch.path(), ARCHIVE_ROOT, "1.200.0", true));
    let e = served.update(&line, &capdag, "1.100.0").await.unwrap_err();
    assert!(matches!(e, UpdateError::Io { .. }), "{e}");
    assert!(e.to_string().contains("cannot replace"), "{e}");
    // The cartridges that were installed are back, and nothing of the update
    // is left.
    assert_eq!(cartridges_of(&capdag), "1.100.0");
    assert_eq!(beside(&capdag), [CARTRIDGES, "capdag", "notes.txt"]);
}

/// TEST12698: a bare executable is given the cartridges of its release
#[tokio::test]
async fn test12698_a_bare_executable_is_given_the_cartridges_of_its_release() {
    let scratch = tempfile::tempdir().unwrap();
    let line = Line::new();
    let capdag = installed(scratch.path(), "1.100.0");
    // The executable alone, as someone who copied it out of its bundle has it.
    std::fs::remove_dir_all(capdag.with_file_name(CARTRIDGES)).unwrap();
    let served = Served::publishing(&line, "1.200.0", &archive(scratch.path(), ARCHIVE_ROOT, "1.200.0", true));
    served.update(&line, &capdag, "1.100.0").await.unwrap();
    assert_installed(&capdag, "1.200.0");
}

/// TEST12699: versions are compared as numbers and nothing else is a version
#[test]
fn test12699_versions_are_compared_as_numbers_and_nothing_else_is_a_version() {
    assert!(version("1.99.0").unwrap() < version("1.100.0").unwrap());
    assert!(version("1.100.9").unwrap() < version("1.100.10").unwrap());
    for refused in ["1.2", "1.2.3.4", "1.2.x", "1..3", "v1.2.3", "1.2.3-rc1", "1.2.+3", ""] {
        assert!(version(refused).is_err(), "{refused:?}");
    }
}

/// TEST12700: a channel or a registry that has published nothing is said
#[tokio::test]
async fn test12700_a_channel_or_a_registry_that_has_published_nothing_is_said() {
    let scratch = tempfile::tempdir().unwrap();
    let line = Line::new();
    let capdag = installed(scratch.path(), "1.100.0");
    let served = Served::publishing(&line, "1.200.0", &archive(scratch.path(), ARCHIVE_ROOT, "1.200.0", true));
    let trust = line.trust();
    let served = &served;
    let charter_trust = line.charter_trust();
    let nightly = Installation {
        registry: REGISTRY,
        channel: "nightly",
        trust: &trust,
        charter_trust: &charter_trust,
        now: NOW,
        installed: &capdag,
        current: "1.100.0",
        platform: PLATFORM,
    };
    let e = update_from(&nightly, |url| async move { Ok(served.files.get(&url).cloned()) }).await.unwrap_err();
    assert!(matches!(e, UpdateError::Unavailable(_)), "{e}");
    assert!(e.to_string().contains("no capdag release is published on the nightly channel"), "{e}");

    let e = Served::chartered(&line).update(&line, &capdag, "1.100.0").await.unwrap_err();
    assert!(e.to_string().contains("no capdag release is published at https://release.example yet"), "{e}");

    // A platform the release was not built for is said, with the ones it was.
    let elsewhere = Installation { channel: "release", platform: "darwin-arm64", ..nightly };
    let e = update_from(&elsewhere, |url| async move { Ok(served.files.get(&url).cloned()) }).await.unwrap_err();
    assert!(matches!(e, UpdateError::Unavailable(_)), "{e}");
    assert!(e.to_string().contains("not published for darwin-arm64 (it is for linux-x86_64)"), "{e}");
    assert_installed(&capdag, "1.100.0");
}

/// TEST12701: the release line is read back as it was baked
#[test]
fn test12701_the_release_line_is_read_back_as_it_was_baked() {
    assert_eq!(
        parse_release_line("https://release.example nightly"),
        ReleaseLine { registry: "https://release.example".into(), channel: "nightly".into() }
    );
}

/// TEST12924: an update reads the release registry's charter before anything
/// else, and takes the manifest's address from it: no charter, or a charter
/// for another environment, and nothing more is asked; a charter that puts the
/// manifest elsewhere is followed there.
#[tokio::test]
async fn test12924_an_update_goes_through_the_release_registrys_charter() {
    let scratch = tempfile::tempdir().unwrap();
    let line = Line::new();
    let capdag = installed(scratch.path(), "1.100.0");
    let bundle = archive(scratch.path(), ARCHIVE_ROOT, "1.200.0", true);
    let [charter, charter_sig] = charter_urls();

    // No charter: not a registry, and nothing past the charter is fetched.
    let mut served = Served::publishing(&line, "1.200.0", &bundle);
    served.files.remove(&charter);
    let e = served.update(&line, &capdag, "1.100.0").await.unwrap_err();
    assert!(matches!(e, UpdateError::Untrusted(_)) && e.to_string().contains("has no charter"), "{e}");
    assert_eq!(served.asked(), [charter.clone()]);

    // A staging registry's charter, for a prod capdag: refused at the charter.
    let mut served = Served::publishing(&line, "1.200.0", &bundle);
    let (body, signature) = line.charter("staging", &format!("{REGISTRY}/{{app_id}}/manifest"));
    served.files.insert(charter.clone(), body);
    served.files.insert(charter_sig.clone(), signature);
    let e = served.update(&line, &capdag, "1.100.0").await.unwrap_err();
    assert!(e.to_string().contains("signed under 'staging'"), "{e}");
    assert_eq!(served.asked().len(), 2, "no manifest is read on the word of a refused charter");

    // A charter that puts the manifests under /apps/: they are read there.
    let mut served = Served::publishing(&line, "1.200.0", &bundle);
    let (body, signature) = line.charter("prod", &format!("{REGISTRY}/apps/{{app_id}}/manifest"));
    served.files.insert(charter.clone(), body);
    served.files.insert(charter_sig.clone(), signature);
    for name in ["manifest", "manifest.sig"] {
        let bytes = served.files.remove(&format!("{REGISTRY}/capdag-cli/{name}")).unwrap();
        served.files.insert(format!("{REGISTRY}/apps/capdag-cli/{name}"), bytes);
    }
    let updated = served.update(&line, &capdag, "1.100.0").await.unwrap();
    assert_eq!(updated, Updated { from: "1.100.0".into(), to: "1.200.0".into() });
    assert!(served.asked().contains(&format!("{REGISTRY}/apps/capdag-cli/manifest")));
    assert_installed(&capdag, "1.200.0");
}

/// TEST12702: another products manifest and an archive off https are refused
#[test]
fn test12702_another_products_manifest_and_an_archive_off_https_are_refused() {
    let line = Line::new();
    let sha = "b74a72297e9404fc35470af7fc77f157e2899577d2907bcdffe90397f89c14a5";
    let read = |manifest: Vec<u8>| latest(&manifest, &line.sign(&manifest), &line.trust(), "release", PLATFORM, NOW);

    let honest = manifest("1.200.0", &Served::archive_url("1.200.0"), sha, 9);
    assert_eq!(
        read(honest.clone()).unwrap(),
        Some(Release { version: "1.200.0".into(), url: Served::archive_url("1.200.0"), sha256: sha.into(), size: 9 })
    );

    // Signed by this line and not the command's.
    let theirs = String::from_utf8(honest).unwrap().replace("\"capdag-cli\"", "\"quickrust\"");
    let e = read(theirs.into_bytes()).unwrap_err();
    assert!(e.to_string().contains("quickrust's, not capdag-cli's"), "{e}");

    let plain = manifest("1.200.0", &Served::archive_url("1.200.0").replace("https://", "http://"), sha, 9);
    assert!(read(plain).unwrap_err().to_string().contains("not HTTPS"));
    let short = manifest("1.200.0", &Served::archive_url("1.200.0"), "abc", 9);
    assert!(read(short).unwrap_err().to_string().contains("as its SHA-256"));
}
