//! `capdag self update`: this `capdag` replaced by the latest release of its
//! channel.
//!
//! The command is published in a release registry as a bundle — the `capdag`
//! executable and the `bundled-cartridges/` tree it runs beside — with one
//! manifest, at the location the registry's charter gives for `capdag-cli`,
//! and a signature beside it at the same URL with `.sig` appended. An update
//! believes nothing it has not checked:
//!
//! 0. the registry, only through its charter: signed by MachineFabric's prod
//!    release key, for this anchor, as a release registry of this build's
//!    environment ([`crate::bifaci::charter`]);
//! 1. the manifest, only through its signature: a release-key certificate
//!    signed by two of the roots this build was given, for this build's
//!    signing environment and inside its validity, and the manifest signed by
//!    that release key over the exact bytes fetched
//!    ([`crate::bifaci::trust`]);
//! 2. the archive, only as the file the manifest describes: its size and its
//!    SHA-256 digest;
//! 3. the command in it, only once it runs and says it is the version it was
//!    published as.
//!
//! Then the bundle replaces the installed one where it is: the cartridges
//! first, the executable last. A published executable accepts any bundle its
//! release line signed, so the step in between — the old executable beside the
//! new cartridges — is a bundle that runs, and an update that stops there has
//! not broken anything. What it leaves in its staging directory is put back or
//! cleared by the next update before it does anything else.
//!
//! Two kinds of `capdag` are not replaced this way, and each is told what does
//! update it: one that is not a published release has no release line, and so
//! nothing it could trust; one a package manager installed is that package
//! manager's, and says so with a file named `installed-by` beside the
//! executable.

use crate::bifaci::trust::RegistryKind;
use signed_release::{verify_manifest, CharterExpectation, CharterTrust, Trust};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
#[cfg(windows)]
use std::path::PathBuf;

/// The command's name in the release registry.
pub const APP_ID: &str = "capdag-cli";
/// The one directory every release archive unpacks to.
pub const ARCHIVE_ROOT: &str = "capdag-bundle";
/// The cartridges a bundle carries, beside its executable.
pub const CARTRIDGES: &str = "bundled-cartridges";
/// The file a package leaves beside the executable it installs, naming the
/// package manager.
pub const INSTALLED_BY: &str = "installed-by";
/// Where an update unpacks and swaps, beside the executable: on the same file
/// system, so that putting a part in place is a rename.
const STAGING: &str = ".capdag-update";
/// The installed cartridges, inside [`STAGING`], while the new ones take
/// their place.
const PREVIOUS: &str = "previous-cartridges";
const MANIFEST_SCHEMA_MAJOR: &str = "5";

/// Why an update did not happen.
#[derive(Debug, thiserror::Error)]
pub enum UpdateError {
    /// This `capdag` has no release to update from, or is not the one to
    /// replace itself.
    #[error("{0}")]
    Unavailable(String),
    /// What the registry served is not a release this `capdag` trusts.
    #[error("{0}")]
    Untrusted(String),
    /// The installed bundle could not be read or written.
    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
}

fn io(context: impl Into<String>) -> impl FnOnce(std::io::Error) -> UpdateError {
    let context = context.into();
    move |source| UpdateError::Io { context, source }
}

/// Where a published `capdag` looks for its next release.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseLine {
    /// The release registry this `capdag` was published in.
    pub registry: String,
    /// The channel it was published on: `release` or `nightly`.
    pub channel: String,
}

/// This build's release line, or `None` for a build that is not a published
/// release. Baked by `build.rs`, which checked both parts.
pub fn release_line() -> Option<ReleaseLine> {
    option_env!("CAPDAG_BAKED_RELEASE_LINE").map(parse_release_line)
}

fn parse_release_line(baked: &str) -> ReleaseLine {
    let Some((registry, channel)) = baked.split_once(' ') else {
        panic!("this capdag's baked release line is not `<registry> <channel>`: {baked:?}");
    };
    ReleaseLine {
        registry: registry.to_string(),
        channel: channel.to_string(),
    }
}

/// The registry's name for the platform this `capdag` runs on, or `None`
/// where the command is not published.
pub fn platform() -> Option<&'static str> {
    Some(match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => "darwin-arm64",
        ("macos", "x86_64") => "darwin-x86_64",
        ("linux", "x86_64") => "linux-x86_64",
        ("linux", "aarch64") => "linux-arm64",
        ("windows", "x86_64") => "windows-x86_64",
        _ => return None,
    })
}

/// One published bundle of the command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Release {
    pub version: String,
    pub url: String,
    /// Lower-case hexadecimal.
    pub sha256: String,
    pub size: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Manifest {
    schema_version: String,
    app_id: String,
    channels: BTreeMap<String, Channel>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Channel {
    latest_version: Option<String>,
    #[serde(default)]
    versions: BTreeMap<String, Version>,
}

#[derive(Deserialize)]
struct Version {
    platforms: BTreeMap<String, Platform>,
}

#[derive(Deserialize)]
struct Platform {
    installers: Vec<Installer>,
}

#[derive(Deserialize)]
struct Installer {
    #[serde(rename = "type")]
    kind: String,
    path: String,
    sha256: String,
    size: u64,
}

/// The installer type a platform's bundle is published as: a `.zip` for
/// Windows, a `.tar.gz` for every other platform.
fn archive_type(platform: &str) -> &'static str {
    if platform.starts_with("windows-") {
        "zip"
    } else {
        "targz"
    }
}


/// Verifies `manifest` against `envelope` (the bytes of `manifest.sig`) under
/// `trust` at `now`, and returns the latest release of `channel` for
/// `platform` — `None` when the channel has published nothing yet.
pub fn latest(
    manifest: &[u8],
    envelope: &[u8],
    trust: &Trust,
    channel: &str,
    platform: &str,
    now: u64,
) -> Result<Option<Release>, UpdateError> {
    let untrusted = UpdateError::Untrusted;
    verify_manifest(envelope, manifest, trust, now).map_err(|e| {
        untrusted(format!(
            "the release manifest is not one this capdag trusts: {e}"
        ))
    })?;

    let manifest: Manifest = serde_json::from_slice(manifest)
        .map_err(|e| untrusted(format!("the release manifest cannot be read: {e}")))?;
    if manifest.schema_version.split('.').next() != Some(MANIFEST_SCHEMA_MAJOR) {
        return Err(untrusted(format!(
            "the release manifest is of schema {}; this capdag reads schema {MANIFEST_SCHEMA_MAJOR}",
            manifest.schema_version
        )));
    }
    if manifest.app_id != APP_ID {
        return Err(untrusted(format!(
            "the release manifest is {}'s, not {APP_ID}'s",
            manifest.app_id
        )));
    }
    let Some(releases) = manifest.channels.get(channel) else {
        return Err(untrusted(format!(
            "the release manifest has no {channel} channel"
        )));
    };
    let Some(version) = releases
        .latest_version
        .as_deref()
        .filter(|version| !version.is_empty())
    else {
        return Ok(None);
    };
    let Some(release) = releases.versions.get(version) else {
        return Err(untrusted(format!(
            "the release manifest names {version} as the latest {channel} version and does not list it"
        )));
    };
    let Some(for_platform) = release.platforms.get(platform) else {
        let published: Vec<&str> = release.platforms.keys().map(String::as_str).collect();
        return Err(UpdateError::Unavailable(format!(
            "capdag {version} is not published for {platform} (it is for {})",
            published.join(", ")
        )));
    };
    let kind = archive_type(platform);
    let Some(archive) = for_platform.installers.iter().find(|i| i.kind == kind) else {
        return Err(UpdateError::Unavailable(format!(
            "capdag {version} for {platform} is published without a {kind} bundle"
        )));
    };
    if archive.sha256.len() != 64 || !archive.sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(untrusted(format!(
            "capdag {version} for {platform} carries {:?} as its SHA-256",
            archive.sha256
        )));
    }
    if !archive.path.starts_with("https://") {
        return Err(untrusted(format!(
            "capdag {version} for {platform} is listed at {:?}, which is not HTTPS",
            archive.path
        )));
    }
    Ok(Some(Release {
        version: version.to_string(),
        url: archive.path.clone(),
        sha256: archive.sha256.to_ascii_lowercase(),
        size: archive.size,
    }))
}

/// A release's version: three numbers. Anything else is said, not compared.
fn version(text: &str) -> Result<(u64, u64, u64), String> {
    let number = |part: &str| -> Option<u64> {
        (!part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
            .then(|| part.parse().ok())
            .flatten()
    };
    let parts: Vec<Option<u64>> = text.split('.').map(number).collect();
    match parts[..] {
        [Some(major), Some(minor), Some(patch)] => Ok((major, minor, patch)),
        _ => Err(format!("{text:?}, which is not three numbers")),
    }
}

/// The package manager that installed the bundle in `bundle`, as its package
/// recorded it.
fn package_manager(bundle: &Path) -> Result<Option<String>, UpdateError> {
    let record = bundle.join(INSTALLED_BY);
    match std::fs::read_to_string(&record) {
        Ok(text) => {
            let name = text.trim();
            if name.is_empty() || name.lines().count() != 1 {
                // A record that names nobody is still a package's: the bundle
                // is not replaced on a guess of whose it is.
                return Err(UpdateError::Unavailable(format!(
                    "{} marks this capdag as a package's and does not name the package manager",
                    record.display()
                )));
            }
            Ok(Some(name.to_string()))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io(format!("cannot read {}", record.display()))(e)),
    }
}

/// What updates a `capdag` that `manager` installed.
fn upgrade_with(manager: &str) -> String {
    match manager {
        "Homebrew" => "`brew upgrade machinefabric/tap/capdag`".to_string(),
        "apt" => "`sudo apt update && sudo apt install --only-upgrade capdag`".to_string(),
        "dnf" => "`sudo dnf upgrade capdag`".to_string(),
        other => format!("update the capdag package with {other}"),
    }
}

/// Everything an update is decided by: what a build bakes in, the clock, and
/// where the bundle is.
pub struct Installation<'a> {
    /// The release registry's anchor.
    pub registry: &'a str,
    pub channel: &'a str,
    pub trust: &'a Trust,
    /// The roots the registry's charter is verified against.
    pub charter_trust: &'a CharterTrust,
    /// Seconds since 1970.
    pub now: u64,
    /// The executable to replace: a file, not a link to one. Its directory is
    /// the bundle.
    pub installed: &'a Path,
    /// The version of the executable at `installed`.
    pub current: &'a str,
    pub platform: &'a str,
}

/// An update's outcome: the version before and the one now (equal when this
/// `capdag` was the latest release already).
#[derive(Debug, PartialEq, Eq)]
pub struct Updated {
    pub from: String,
    pub to: String,
}

/// `capdag self update`, for the `capdag` this process is, of version
/// `current`.
pub async fn update(current: &str) -> Result<Updated, UpdateError> {
    let line = release_line().ok_or_else(|| {
        UpdateError::Unavailable(
            "this capdag is not a published release, so it has no release line to update from: \
             build the newer source, or install a release"
                .to_string(),
        )
    })?;
    // A build with a release line has its roots: build.rs refuses the one
    // without the other.
    let trust = crate::bifaci::trust::content_trust().ok_or_else(|| {
        UpdateError::Unavailable(
            "this capdag has a release line and no root keys to verify a release with".to_string(),
        )
    })?;
    let platform = platform().ok_or_else(|| {
        UpdateError::Unavailable(
            "the capdag command is not published for this platform: build it from source"
                .to_string(),
        )
    })?;
    let running = std::env::current_exe().map_err(io("cannot tell where this capdag is"))?;
    // Where the executable is, not a link to it: a link on PATH into a
    // package's directory must find the package's `installed-by`, and the
    // cartridges are beside the executable, not beside the link.
    let installed = std::fs::canonicalize(&running)
        .map_err(io(format!("cannot resolve {}", running.display())))?;
    let client = reqwest::Client::new();
    update_from(
        &Installation {
            registry: &line.registry,
            channel: &line.channel,
            trust: &trust,
            charter_trust: &crate::bifaci::trust::charter_trust(),
            now: signed_release::unix_now(),
            installed: &installed,
            current,
            platform,
        },
        |url| fetch(&client, url),
    )
    .await
}

/// The bytes at `url`, or `None` where nothing is published there.
async fn fetch(client: &reqwest::Client, url: String) -> Result<Option<Vec<u8>>, UpdateError> {
    let unreachable =
        |e: reqwest::Error| UpdateError::Unavailable(format!("cannot download {url}: {e}"));
    let response = crate::net_retry::retry_request(
        &crate::net_retry::RetryPolicy::download(),
        "download a capdag release",
        // Never from a cache: the manifest and its signature are replaced
        // together, and a stale one of either beside a fresh one of the other
        // reads as a forgery.
        || client.get(&url).header("Cache-Control", "no-cache").send(),
    )
    .await
    .map_err(unreachable)?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let response = response.error_for_status().map_err(unreachable)?;
    Ok(Some(response.bytes().await.map_err(unreachable)?.to_vec()))
}

/// [`update`] with everything it reads from the build and the machine passed
/// explicitly, and the registry read through `get`: the bytes at a URL, or
/// `None` where nothing is published.
pub async fn update_from<F, Fut>(at: &Installation<'_>, get: F) -> Result<Updated, UpdateError>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<Option<Vec<u8>>, UpdateError>>,
{
    let bundle = at.installed.parent().ok_or_else(|| {
        UpdateError::Unavailable(format!(
            "{} is not a file in a directory",
            at.installed.display()
        ))
    })?;
    if let Some(manager) = package_manager(bundle)? {
        return Err(UpdateError::Unavailable(format!(
            "this capdag was installed by {manager}, which is what updates it: {}",
            upgrade_with(&manager)
        )));
    }
    // An update that stopped half way is put back before anything else is
    // decided: what is installed is then what it was.
    recover(bundle)?;

    // The registry first: nothing at an anchor MachineFabric did not charter
    // is read, and where the manifest is comes from the charter.
    let charter_url = format!("{}/{}", at.registry, signed_release::CHARTER_FILE);
    let Some(charter_body) = get(charter_url.clone()).await? else {
        return Err(UpdateError::Untrusted(format!(
            "{} has no charter ({charter_url} is missing), so it is not a release registry \
             MachineFabric chartered",
            at.registry
        )));
    };
    let charter_sig_url = format!("{}/{}", at.registry, signed_release::CHARTER_SIG_FILE);
    let Some(charter_sig) = get(charter_sig_url.clone()).await? else {
        return Err(UpdateError::Untrusted(format!(
            "{} has a charter and no signature for it ({charter_sig_url} is missing)",
            at.registry
        )));
    };
    let charter = crate::bifaci::charter::verify(
        &charter_body,
        &charter_sig,
        at.charter_trust,
        &CharterExpectation {
            anchor: Some(at.registry),
            kind: RegistryKind::Releases.wire_name(),
            environment: Some(&at.trust.label),
        },
        at.now,
    )
    .map_err(|e| UpdateError::Untrusted(format!("the charter of {} was refused: {e}", at.registry)))?;
    let url = charter
        .charter
        .location("manifest", &[("app_id", APP_ID)])
        .map_err(|e| UpdateError::Untrusted(format!("the charter of {}: {e}", at.registry)))?;
    let Some(manifest) = get(url.clone()).await? else {
        return Err(UpdateError::Unavailable(format!(
            "no capdag release is published at {} yet",
            at.registry
        )));
    };
    let signature_url = format!("{url}.sig");
    let Some(envelope) = get(signature_url.clone()).await? else {
        return Err(UpdateError::Untrusted(format!(
            "{url} is published without a signature ({signature_url} is missing), so nothing in it is trusted"
        )));
    };
    let Some(release) = latest(
        &manifest,
        &envelope,
        at.trust,
        at.channel,
        at.platform,
        at.now,
    )?
    else {
        return Err(UpdateError::Unavailable(format!(
            "no capdag release is published on the {} channel at {} yet",
            at.channel, at.registry
        )));
    };
    let newest = version(&release.version).map_err(|why| {
        UpdateError::Untrusted(format!("the release manifest's latest version is {why}"))
    })?;
    let current = version(at.current)
        .map_err(|why| UpdateError::Unavailable(format!("this capdag's version is {why}")))?;
    if newest <= current {
        return Ok(Updated {
            from: at.current.to_string(),
            to: at.current.to_string(),
        });
    }

    let Some(archive) = get(release.url.clone()).await? else {
        return Err(UpdateError::Unavailable(format!(
            "{} is listed by the release manifest and is not there",
            release.url
        )));
    };
    if archive.len() as u64 != release.size {
        return Err(UpdateError::Untrusted(format!(
            "{} is {} bytes; the release manifest says {}. The download was discarded",
            release.url,
            archive.len(),
            release.size
        )));
    }
    let digest: String = Sha256::digest(&archive)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    if digest != release.sha256 {
        return Err(UpdateError::Untrusted(format!(
            "{} has SHA-256 {digest}, but the release manifest lists {}. The download was discarded",
            release.url, release.sha256
        )));
    }

    let staging = bundle.join(STAGING);
    std::fs::create_dir(&staging).map_err(io(format!(
        "cannot write in {}, where this capdag is installed, so it cannot be replaced there \
         (run the update with the rights that installed it)",
        bundle.display()
    )))?;
    let outcome = install(&archive, &release, at.installed, bundle, &staging);
    let cleared = std::fs::remove_dir_all(&staging);
    outcome?;
    cleared.map_err(io(format!("cannot remove {}", staging.display())))?;
    Ok(Updated {
        from: at.current.to_string(),
        to: release.version,
    })
}

/// Puts back what an update that stopped left in its staging directory, and
/// clears the directory.
///
/// The one state that needs putting back is the installed cartridges moved
/// aside with nothing in their place: the executable would run, and find no
/// bundled cartridge. Every other leftover is a download or an unpacked
/// bundle that was never installed.
fn recover(bundle: &Path) -> Result<(), UpdateError> {
    let staging = bundle.join(STAGING);
    if !staging.exists() {
        return Ok(());
    }
    let (previous, cartridges) = (staging.join(PREVIOUS), bundle.join(CARTRIDGES));
    if previous.is_dir() && !cartridges.exists() {
        std::fs::rename(&previous, &cartridges).map_err(io(format!(
            "an earlier update stopped with this capdag's cartridges moved to {}, and they cannot \
             be put back at {}",
            previous.display(),
            cartridges.display()
        )))?;
    }
    std::fs::remove_dir_all(&staging).map_err(io(format!(
        "cannot clear {}, which an earlier update left",
        staging.display()
    )))
}

/// Unpacks `archive` (already held to the manifest's size and digest) in
/// `staging`, checks the command in it, and puts the bundle in place.
fn install(
    archive: &[u8],
    release: &Release,
    installed: &Path,
    bundle: &Path,
    staging: &Path,
) -> Result<(), UpdateError> {
    let file = staging.join("bundle");
    std::fs::write(&file, archive).map_err(io(format!("cannot write {}", file.display())))?;
    // The system's tar reads both forms a bundle is published in (a .tar.gz,
    // and on Windows a .zip), and what it unpacks here has just been held to
    // the digest a signed manifest lists.
    // Started the way every process of this crate is: through the launcher.
    let unpacked = crate::bifaci::launch::command(Path::new("tar"))
        .arg("-xf")
        .arg(&file)
        .arg("-C")
        .arg(staging)
        .output()
        .map_err(io("cannot run tar to unpack the release"))?;
    if !unpacked.status.success() {
        return Err(UpdateError::Untrusted(format!(
            "{} cannot be unpacked: {}",
            release.url,
            String::from_utf8_lossy(&unpacked.stderr).trim()
        )));
    }
    let name = format!("capdag{}", std::env::consts::EXE_SUFFIX);
    let (new, new_cartridges) = (
        staging.join(ARCHIVE_ROOT).join(&name),
        staging.join(ARCHIVE_ROOT).join(CARTRIDGES),
    );
    if !new.is_file() || !new_cartridges.is_dir() {
        return Err(UpdateError::Untrusted(format!(
            "{} does not hold {ARCHIVE_ROOT}/{name} and {ARCHIVE_ROOT}/{CARTRIDGES}, so it is not \
             a capdag bundle",
            release.url
        )));
    }
    // It must run, and be the release it was published as.
    let said = crate::bifaci::launch::command(&new)
        .arg("--version")
        .output()
        .map_err(|e| {
            UpdateError::Untrusted(format!(
                "the capdag in {} does not run on this machine: {e}",
                release.url
            ))
        })?;
    let said = String::from_utf8_lossy(&said.stdout).trim().to_string();
    if said != format!("capdag {}", release.version) {
        return Err(UpdateError::Untrusted(format!(
            "the capdag in {} says {said:?}, not \"capdag {}\"",
            release.url, release.version
        )));
    }

    // The cartridges first. An executable of this release line accepts any
    // bundle the line signed, so the installed executable beside the new
    // cartridges runs; the new executable beside the old ones is the pair
    // that was never published together, and is never made.
    let (cartridges, previous) = (bundle.join(CARTRIDGES), staging.join(PREVIOUS));
    let had_cartridges = cartridges.exists();
    if had_cartridges {
        std::fs::rename(&cartridges, &previous)
            .map_err(io(format!("cannot move {} aside", cartridges.display())))?;
    }
    let put_back = |why: UpdateError| -> UpdateError {
        // Whatever of the new bundle is in place goes, and what was there
        // returns. A failure here is reported with the one that caused it.
        let undone = (|| -> std::io::Result<()> {
            if cartridges.exists() {
                std::fs::remove_dir_all(&cartridges)?;
            }
            if had_cartridges {
                std::fs::rename(&previous, &cartridges)?;
            }
            Ok(())
        })();
        match undone {
            Ok(()) => why,
            Err(e) => UpdateError::Io {
                context: format!(
                    "{why}; and the cartridges that were installed could not be put back from {}",
                    previous.display()
                ),
                source: e,
            },
        }
    };
    if let Err(e) = std::fs::rename(&new_cartridges, &cartridges) {
        return Err(put_back(io(format!(
            "cannot install {}",
            cartridges.display()
        ))(e)));
    }
    replace(installed, &new).map_err(put_back)
}

/// Puts `new` where `installed` is.
#[cfg(not(windows))]
fn replace(installed: &Path, new: &Path) -> Result<(), UpdateError> {
    // One rename: a process that starts `capdag` meanwhile runs the old
    // executable or the new one.
    std::fs::rename(new, installed).map_err(io(format!("cannot replace {}", installed.display())))
}

/// Puts `new` where `installed` is. Windows does not let a running program's
/// file be replaced or removed, and does let it be renamed: the running
/// executable is moved aside as `capdag.exe.old` and the new one takes its
/// name. The file moved aside stays until the next update removes it.
#[cfg(windows)]
fn replace(installed: &Path, new: &Path) -> Result<(), UpdateError> {
    let mut name = installed
        .file_name()
        .expect("an executable is a file")
        .to_os_string();
    name.push(".old");
    let aside: PathBuf = installed.with_file_name(name);
    match std::fs::remove_file(&aside) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(io(format!("cannot remove {}", aside.display()))(e)),
    }
    std::fs::rename(installed, &aside)
        .map_err(io(format!("cannot move {} aside", installed.display())))?;
    if let Err(e) = std::fs::rename(new, installed) {
        // Back where it was: a failed update leaves the executable that was
        // there.
        return match std::fs::rename(&aside, installed) {
            Ok(()) => Err(io(format!("cannot replace {}", installed.display()))(e)),
            Err(back) => Err(io(format!(
                "cannot replace {} ({e}), and the executable moved aside could not be put back: \
                 it is at {}",
                installed.display(),
                aside.display()
            ))(back)),
        };
    }
    Ok(())
}

#[cfg(all(test, unix))]
#[path = "self_update_tests.rs"]
mod tests;
