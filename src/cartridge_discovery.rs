//! Shared cartridge discovery.
//!
//! The on-disk scan + identity validation + HELLO probe that classifies each
//! installed cartridge version directory as attachable (`Directory`) or
//! `Incompatible`. This is the single source of truth used by BOTH:
//!
//! - the engine, for the bundled `bundled-cartridges/` tree next to its binary, and
//! - `unifloom-daemon`, for the user-installed cartridge tree.
//!
//! Keeping one implementation guarantees the two hosts accept exactly the same
//! cartridges and reject the rest with byte-identical verdicts. The host's
//! identity (channel / registry URL / fabric manifest version) is passed in via
//! [`DiscoveryIdentity`] rather than read from a compile-time constant, so the
//! same code serves a host built for any channel/registry.
//!
//! Managed layout (relative to the root passed to [`discover_cartridges`]):
//! `{root}/{slug}/v{cartridge_registry_version}/{channel}/{name}/{version}/cartridge.json`.

use crate::bifaci::cartridge_json::{
    validate_registry_url_scheme, CartridgeJson, RegistryUrlSchemeResult,
};
use crate::bifaci::cartridge_repo::CartridgeChannel;
use crate::bifaci::cartridge_slug::slug_for;
use crate::bifaci::manifest::CapManifest;
use crate::bifaci::relay_switch::{CartridgeAttachmentError, CartridgeAttachmentErrorKind};
use crate::CapGroup;
use std::path::{Path, PathBuf};
use tracing::{error, warn};

/// The identity a host accepts cartridges for. A cartridge whose `cartridge.json`
/// diverges from this on channel, registry URL, registry scheme, or fabric
/// manifest version is surfaced as `Incompatible` — never hosted.
#[derive(Debug, Clone)]
pub struct DiscoveryIdentity {
    pub channel: CartridgeChannel,
    /// `Some(url)` for release/nightly hosts, `None` for dev hosts (cartridges
    /// then live under the reserved dev slug and any registry scheme is allowed).
    pub registry_url: Option<String>,
    pub fabric_manifest_version: u32,
    /// Cartridge registry regime version this host speaks (the baked
    /// [`crate::CARTRIDGE_REGISTRY_VERSION`]). It is an on-disk PATH level —
    /// cartridges live under `{slug}/v{cartridge_registry_version}/{channel}/…`
    /// — pinned like the channel, so a v1 host never scans a v2 cartridge tree.
    pub cartridge_registry_version: u32,
    /// What proves the bundled cartridges under the root being scanned.
    ///
    /// Carried rather than looked up, because verification is one act per
    /// discovery and because the caller is the only thing that knows what this
    /// root IS. A build's own `bundled-cartridges/` tree carries
    /// [`BundleProof::load`]'s answer; the operator's installed-cartridges
    /// directory carries [`BundleProof::none`], so a cartridge claiming to be
    /// bundled while sitting there is refused for exactly that reason instead
    /// of being hosted because nobody asked.
    pub bundle: crate::bifaci::bundle_manifest::BundleProof,
}

impl DiscoveryIdentity {
    /// On-disk top-level slug for THIS host's own baked registry (`dev` when
    /// `registry_url` is None). Discovery no longer restricts scanning to this
    /// slug — it enumerates every slug folder on disk (full macOS parity) and
    /// validates each cartridge against the folder it sits under. Retained as a
    /// public helper for callers that need the host's own slug (e.g. to locate
    /// where this build's bundled cartridges were staged).
    pub fn slug(&self) -> String {
        slug_for(self.registry_url.as_deref())
    }
}

/// A discovered cartridge version directory, classified.
///
/// - `Directory` — passed every identity check and its HELLO probe succeeded.
///   Its caps will be registered for dispatch.
/// - `Incompatible` — found on disk but failed a check. NOT spawned, caps never
///   enter the dispatch graph; surfaced with a structured `attachment_error` so
///   the UI can render the reason. This is the uniform surface for every
///   discovery-time rejection — no silent log-and-skip.
#[derive(Debug, Clone)]
pub enum DiscoveredCartridge {
    Directory {
        entry_point: PathBuf,
        version_dir: PathBuf,
        id: String,
        channel: CartridgeChannel,
        registry_url: Option<String>,
        version: String,
        cap_groups: Vec<CapGroup>,
    },
    Incompatible {
        version_dir: PathBuf,
        id: String,
        channel: CartridgeChannel,
        registry_url: Option<String>,
        version: String,
        error: CartridgeAttachmentError,
    },
}

/// Current wall-clock time as Unix seconds, for stamping
/// `CartridgeAttachmentError.detected_at_unix_seconds`. A pre-epoch clock
/// returns 0 (display-ordering only).
fn unix_seconds_now() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Probe a cartridge binary for its capability surface.
///
/// Spawns the binary, performs the bifaci HELLO handshake, parses the manifest,
/// returns its full `cap_groups` (caps + adapter_urns), then kills the process.
/// A binary that fails to spawn, fails HELLO, or returns an unparseable manifest
/// is an error — the caller surfaces it as `HandshakeFailed`.
pub async fn probe_cartridge_cap_groups(path: &Path) -> anyhow::Result<Vec<CapGroup>> {
    use crate::{handshake, FrameReader, FrameWriter};
    use tokio::io::{BufReader, BufWriter};

    // Through the launcher, because a dev cartridge's entry may be a script
    // and Windows cannot execute one. See `bifaci::launch`.
    let mut child = crate::bifaci::launch::tokio_command(path)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .map_err(|e| anyhow::anyhow!("Failed to spawn cartridge {:?}: {}", path, e))?;

    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("cartridge {:?} stdin pipe missing", path))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("cartridge {:?} stdout pipe missing", path))?;

    let mut reader = FrameReader::new(BufReader::new(stdout));
    let mut writer = FrameWriter::new(BufWriter::new(stdin));

    let result = handshake(&mut reader, &mut writer)
        .await
        .map_err(|e| anyhow::anyhow!("cartridge {:?} HELLO failed: {}", path, e))?;

    // SIGKILL immediately — we have the manifest and don't wait for a clean exit.
    if let Err(e) = child.start_kill() {
        warn!(path = %path.display(), error = %e, "probe_cartridge_cap_groups: start_kill failed (process may have already exited)");
    }

    let manifest: CapManifest = serde_json::from_slice(&result.manifest).map_err(|e| {
        let preview = String::from_utf8_lossy(&result.manifest[..result.manifest.len().min(500)]);
        anyhow::anyhow!("cartridge {:?} invalid manifest ({}): {}", path, e, preview)
    })?;

    Ok(manifest.cap_groups)
}

/// Discover every cartridge under `{cartridges_root}/{slug}/{channel}/`, where
/// slug+channel come from `identity`. Each cartridge name directory's newest
/// version is validated against `identity` and probed; the result is the full
/// classified roster (attachable + incompatible). An empty/absent scan root is
/// not an error — it yields an empty roster. A real IO failure reading an
/// existing scan root IS an error (it would otherwise masquerade as "no
/// cartridges installed").
pub async fn discover_cartridges(
    cartridges_root: &Path,
    identity: &DiscoveryIdentity,
) -> anyhow::Result<Vec<DiscoveredCartridge>> {
    let mut discovered: Vec<DiscoveredCartridge> = Vec::new();
    if !cartridges_root.is_dir() {
        return Ok(discovered);
    }

    // Scan EVERY slug folder present on disk — full macOS parity. The host's
    // baked `identity.registry_url` does NOT restrict which slugs are scanned;
    // each cartridge is instead validated in place against the slug folder it
    // sits under (the three-place rule in `read_from_dir`), so a registry-
    // installed cartridge (under its registry's slug), the reserved `dev/` slot
    // (unpublished user cartridges, null registry_url), and the engine's bundled
    // cartridges (under the build's registry slug, `installed_from: "bundle"`,
    // proven against this build's signed bundle manifest) all coexist and load
    // together. The
    // channel folder IS still pinned to the host's channel — release and nightly
    // artefacts never mix. Registry-listing validation (is this version listed
    // upstream?) is the verdict layer's job, applied after discovery.
    let slug_entries = std::fs::read_dir(cartridges_root)
        .map_err(|e| anyhow::anyhow!("read_dir({}): {}", cartridges_root.display(), e))?;

    for slug_entry in slug_entries {
        let slug_entry = slug_entry.map_err(|e| {
            anyhow::anyhow!("read_dir entry in {}: {}", cartridges_root.display(), e)
        })?;
        let slug_dir = slug_entry.path();
        if !slug_dir.is_dir() {
            let file_name = slug_dir.file_name().unwrap_or_default().to_string_lossy();
            // The bundle manifest and its signature live here by design; a
            // warning about them on every startup would train an operator to
            // ignore the one that means something.
            if file_name != ".DS_Store"
                && !crate::bifaci::bundle_manifest::is_manifest_file(&file_name)
            {
                error!(path = %slug_dir.display(), "Unmanaged file in cartridges root — only registry-slug / dev directories belong here");
            }
            continue;
        }
        let expected_slug = slug_dir
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        // `{slug}/v{cartridge_registry_version}/{channel}/…` — the registry
        // regime version is an on-disk level pinned to the host's version (like
        // the channel), so a v1 host never scans a v2 cartridge tree.
        let scan_root = slug_dir
            .join(format!("v{}", identity.cartridge_registry_version))
            .join(identity.channel.as_str());
        if !scan_root.is_dir() {
            // This slug has no subtree for the host's (version, channel) — skip.
            continue;
        }
        scan_channel_root(&scan_root, &expected_slug, identity, &mut discovered).await?;
    }

    Ok(discovered)
}

/// Scan one `{slug}/{channel}/` root: classify each cartridge name directory's
/// newest version against the host identity and the slug folder it sits under.
/// `expected_slug` is the on-disk slug folder name — passed to
/// `read_from_dir` so the three-place rule (folder slug ⇔ `slug_for(registry_url)`)
/// is enforced per cartridge. Appends results to `discovered`.
async fn scan_channel_root(
    scan_root: &Path,
    expected_slug: &str,
    identity: &DiscoveryIdentity,
    discovered: &mut Vec<DiscoveredCartridge>,
) -> anyhow::Result<()> {
    let name_entries = std::fs::read_dir(scan_root)
        .map_err(|e| anyhow::anyhow!("read_dir({}): {}", scan_root.display(), e))?;

    for entry in name_entries {
        let entry = entry
            .map_err(|e| anyhow::anyhow!("read_dir entry in {}: {}", scan_root.display(), e))?;
        let name_dir = entry.path();

        if !name_dir.is_dir() {
            let file_name = name_dir.file_name().unwrap_or_default().to_string_lossy();
            if file_name != ".DS_Store" {
                error!(path = %name_dir.display(), "Unmanaged file in {{slug}}/{{channel}}/ — only cartridge name directories belong here");
            }
            continue;
        }

        let sub_entries = match std::fs::read_dir(&name_dir) {
            Ok(e) => e,
            Err(e) => {
                error!(dir = %name_dir.display(), error = %e, "Cannot read cartridge name directory");
                continue;
            }
        };

        let mut version_dirs: Vec<PathBuf> = Vec::new();
        for sub_entry in sub_entries.flatten() {
            let sub_path = sub_entry.path();
            if sub_path.is_dir() {
                version_dirs.push(sub_path);
            } else {
                let file_name = sub_path.file_name().unwrap_or_default().to_string_lossy();
                if file_name != ".DS_Store" {
                    error!(path = %sub_path.display(), "Unmanaged file inside cartridge name directory — only version directories belong here");
                }
            }
        }

        if version_dirs.is_empty() {
            error!(dir = %name_dir.display(), "Cartridge name directory contains no version subdirectories");
            continue;
        }

        // Prefer the newest version (lexical-descending on the version folder name).
        version_dirs.sort_by(|a, b| {
            let va = a
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            let vb = b
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            vb.cmp(&va)
        });
        let version_dir = &version_dirs[0];

        let path_derived_name = name_dir
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "unknown".to_string());
        let path_derived_version = version_dir
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "unknown".to_string());

        let detected_at = unix_seconds_now();

        // `read_from_dir` enforces the three-place rule against the ACTUAL slug
        // folder (`expected_slug`): the cartridge's declared `registry_url` must
        // hash to it. A non-null registry_url under `dev/` (or any slug≠
        // slug_for(registry_url)) fails here as a slug mismatch — surfaced
        // incompatible and logged, never hosted. A null registry_url is valid
        // only under the reserved `dev/` slot.
        let cj = match CartridgeJson::read_from_dir(version_dir, expected_slug) {
            Ok(cj) => cj,
            Err(e) => {
                // A slug mismatch (declared registry_url doesn't hash to this
                // folder — e.g. a registry-defined url placed under `dev/`, or a
                // cartridge hand-copied between registry slugs) is a bad
                // INSTALL CONTEXT, distinct from an unreadable/garbage
                // cartridge.json (ManifestInvalid). Both are surfaced + logged,
                // never hosted.
                let kind = match &e {
                    crate::bifaci::cartridge_json::CartridgeJsonError::RegistrySlugMismatch {
                        ..
                    } => CartridgeAttachmentErrorKind::Misplaced,
                    _ => CartridgeAttachmentErrorKind::ManifestInvalid,
                };
                error!(dir = %version_dir.display(), slug = %expected_slug, error = %e, "cartridge.json invalid or mis-placed — surfacing as incompatible");
                discovered.push(DiscoveredCartridge::Incompatible {
                    version_dir: version_dir.clone(),
                    id: path_derived_name.clone(),
                    channel: identity.channel,
                    registry_url: identity.registry_url.clone(),
                    version: path_derived_version.clone(),
                    error: CartridgeAttachmentError {
                        kind,
                        message: format!(
                            "cartridge.json failed to load under slug '{}': {}",
                            expected_slug, e
                        ),
                        detected_at_unix_seconds: detected_at,
                    },
                });
                continue;
            }
        };

        if cj.channel != identity.channel {
            discovered.push(DiscoveredCartridge::Incompatible {
                version_dir: version_dir.clone(),
                id: cj.name.clone(),
                channel: cj.channel,
                registry_url: cj.registry_url.clone(),
                version: cj.version.clone(),
                error: CartridgeAttachmentError {
                    kind: CartridgeAttachmentErrorKind::Misplaced,
                    message: format!(
                        "Channel mismatch: cartridge declares '{}' but host is pinned to '{}'. Release and nightly artefacts must not mix.",
                        cj.channel, identity.channel
                    ),
                    detected_at_unix_seconds: detected_at,
                },
            });
            continue;
        }

        // NO registry pin: the host's baked registry does NOT restrict which
        // registries' cartridges are discovered. A self-consistent cartridge
        // (its registry_url hashes to its slug folder, validated above) from any
        // registry present on disk is accepted; whether its version is actually
        // LISTED upstream is the verdict layer's call, applied after discovery.

        // Scheme check is per-cartridge: a dev cartridge (null registry_url)
        // never reaches here; a registry cartridge must use https (dev_mode=false
        // for the scheme relaxation, which only ever applied to null-registry
        // dev cartridges).
        if let Some(url) = cj.registry_url.as_deref() {
            match validate_registry_url_scheme(url, false) {
                RegistryUrlSchemeResult::Ok => {}
                RegistryUrlSchemeResult::NonHttps { scheme } => {
                    discovered.push(DiscoveredCartridge::Incompatible {
                        version_dir: version_dir.clone(),
                        id: cj.name.clone(),
                        channel: cj.channel,
                        registry_url: cj.registry_url.clone(),
                        version: cj.version.clone(),
                        error: CartridgeAttachmentError {
                            kind: CartridgeAttachmentErrorKind::Incompatible,
                            message: format!(
                                "registry_url uses '{}' scheme, must be https in non-dev builds. Rebuild the cartridge with an https registry URL.",
                                scheme
                            ),
                            detected_at_unix_seconds: detected_at,
                        },
                    });
                    continue;
                }
                RegistryUrlSchemeResult::NotAUrl(bad) => {
                    discovered.push(DiscoveredCartridge::Incompatible {
                        version_dir: version_dir.clone(),
                        id: cj.name.clone(),
                        channel: cj.channel,
                        registry_url: cj.registry_url.clone(),
                        version: cj.version.clone(),
                        error: CartridgeAttachmentError {
                            kind: CartridgeAttachmentErrorKind::Incompatible,
                            message: format!("registry_url '{}' is not a well-formed URL.", bad),
                            detected_at_unix_seconds: detected_at,
                        },
                    });
                    continue;
                }
            }
        }

        if cj.fabric_manifest_version != identity.fabric_manifest_version {
            discovered.push(DiscoveredCartridge::Incompatible {
                version_dir: version_dir.clone(),
                id: cj.name.clone(),
                channel: cj.channel,
                registry_url: cj.registry_url.clone(),
                version: cj.version.clone(),
                error: CartridgeAttachmentError {
                    kind: CartridgeAttachmentErrorKind::FabricManifestVersionMismatch,
                    message: format!(
                        "Cartridge built against fabric manifest version {}, but host is pinned to {}. Rebuild the cartridge with MFR_FABRIC_MANIFEST_VERSION={}.",
                        cj.fabric_manifest_version, identity.fabric_manifest_version, identity.fabric_manifest_version
                    ),
                    detected_at_unix_seconds: detected_at,
                },
            });
            continue;
        }

        // Bundled-cartridge integrity. A cartridge marked `installed_from: bundle`
        // is shipped INSIDE this build (the engine/daemon/capdag-CLI's own
        // bundled-cartridges/ tree), not user-installed, and has no upstream
        // registry to verify against — so it needs its own integrity proof.
        //
        // ONE mechanism, on every platform: the build's signed bundle manifest,
        // verified against the roots this build bakes, by the same chain
        // verifier a registry manifest goes through. The proof was established
        // once when this discovery started (see `BundleProof`); here it is
        // applied to the cartridge in hand.
        //
        // This used to be platform-split, and macOS had no check of ours at
        // all — the manifest is produced at the END of a build, after every
        // platform signing step, which is what removed the ordering problem
        // that split existed for. Apple's signature still matters, and it is
        // what stops the operating system warning a user; it is not what
        // decides whether code runs here.
        if cj.installed_from == Some(crate::bifaci::cartridge_json::CartridgeInstallSource::Bundle)
        {
            if let Err(reason) = identity.bundle.check(&cj.name, &cj.version, version_dir) {
                error!(cartridge = %version_dir.display(), name = %cj.name, version = %cj.version, reason = %reason, "bundled cartridge is not proven by this build's signed bundle manifest — surfacing as incompatible");
                discovered.push(DiscoveredCartridge::Incompatible {
                    version_dir: version_dir.clone(),
                    id: cj.name.clone(),
                    channel: cj.channel,
                    registry_url: cj.registry_url.clone(),
                    version: cj.version.clone(),
                    error: CartridgeAttachmentError {
                        kind: CartridgeAttachmentErrorKind::Misplaced,
                        message: format!("bundled cartridge integrity check failed: {reason}"),
                        detected_at_unix_seconds: detected_at,
                    },
                });
                continue;
            }
        }

        let entry_point = cj.resolve_entry_point(version_dir);
        match probe_cartridge_cap_groups(&entry_point).await {
            Ok(cap_groups) => {
                discovered.push(DiscoveredCartridge::Directory {
                    entry_point,
                    version_dir: version_dir.clone(),
                    id: cj.name,
                    channel: cj.channel,
                    registry_url: cj.registry_url,
                    version: cj.version,
                    cap_groups,
                });
            }
            Err(e) => {
                error!(cartridge = %version_dir.display(), error = %e, "Failed to probe cartridge entry point — surfacing as incompatible");
                discovered.push(DiscoveredCartridge::Incompatible {
                    version_dir: version_dir.clone(),
                    id: cj.name,
                    channel: cj.channel,
                    registry_url: cj.registry_url,
                    version: cj.version,
                    error: CartridgeAttachmentError {
                        kind: CartridgeAttachmentErrorKind::HandshakeFailed,
                        message: format!("HELLO handshake / cap discovery probe failed: {}", e),
                        detected_at_unix_seconds: detected_at,
                    },
                });
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::tempdir;

    fn nightly_dev_identity() -> DiscoveryIdentity {
        DiscoveryIdentity {
            channel: CartridgeChannel::Nightly,
            registry_url: None,
            fabric_manifest_version: 1,
            cartridge_registry_version: crate::CARTRIDGE_REGISTRY_VERSION,
            // A root that ships no bundle. Every test below that is not ABOUT
            // the bundle scans a tree nothing built, so a cartridge claiming to
            // be bundled there is in the wrong place — which is what this says.
            bundle: crate::bifaci::bundle_manifest::BundleProof::none(
                "this directory is not a build's bundle",
            ),
        }
    }

    /// An identity whose bundled cartridges are proven by `manifest`.
    ///
    /// The manifest is built in memory rather than signed, because what is
    /// under test here is what discovery DOES with a proof. That the proof is
    /// only ever obtained by verifying a real signature is
    /// `bundle_manifest`'s own tests' business, against a real committed chain
    /// — splitting it that way is what keeps either half from being tested
    /// against a stub of the other.
    fn bundled_identity(
        manifest: crate::bifaci::bundle_manifest::BundleManifest,
    ) -> DiscoveryIdentity {
        DiscoveryIdentity {
            bundle: crate::bifaci::bundle_manifest::BundleProof::Verified(Box::new(manifest)),
            ..nightly_dev_identity()
        }
    }

    /// Lay down `{root}/{slug}/v{CARTRIDGE_REGISTRY_VERSION}/{channel_folder}/{name}/{version}/`
    /// — the version level pins to the host build's registry version, exactly
    /// where `discover_cartridges` scans. When `cartridge_json` is `Some`, also
    /// write it plus an executable `entry` binary so `read_from_dir` accepts the
    /// directory and discovery reaches its own identity checks.
    fn install_fixture(
        root: &Path,
        slug: &str,
        channel_folder: &str,
        name: &str,
        version: &str,
        cartridge_json: Option<&str>,
        entry: &str,
    ) {
        let dir = root
            .join(slug)
            .join(format!("v{}", crate::CARTRIDGE_REGISTRY_VERSION))
            .join(channel_folder)
            .join(name)
            .join(version);
        fs::create_dir_all(&dir).unwrap();
        if let Some(json) = cartridge_json {
            fs::write(dir.join("cartridge.json"), json).unwrap();
            let entry_path = dir.join(entry);
            fs::write(&entry_path, b"#!/bin/sh\nexit 0\n").unwrap();
            fs::set_permissions(&entry_path, fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    fn dev_cartridge_json(channel: &str, fabric_manifest_version: u32) -> String {
        format!(
            r#"{{"name":"cart","version":"1.0.0","channel":"{channel}","registry_url":null,"entry":"cart","installed_at":"2024-01-01T00:00:00Z","fabric_manifest_version":{fabric_manifest_version}}}"#
        )
    }

    fn expect_incompatible(out: &[DiscoveredCartridge], kind: CartridgeAttachmentErrorKind) {
        assert_eq!(out.len(), 1, "expected exactly one discovered entry");
        match &out[0] {
            DiscoveredCartridge::Incompatible { error, .. } => {
                assert_eq!(
                    error.kind, kind,
                    "wrong attachment-error kind: {}",
                    error.message
                );
            }
            other => panic!("expected Incompatible({kind:?}), got {other:?}"),
        }
    }

    // TEST0090: Absent scan root yields empty roster
    #[tokio::test]
    async fn test0090_absent_scan_root_yields_empty_roster() {
        let root = tempdir().unwrap();
        let out = discover_cartridges(root.path(), &nightly_dev_identity())
            .await
            .unwrap();
        assert!(
            out.is_empty(),
            "no install tree must be an empty roster, not an error"
        );
    }

    // TEST0091: Missing cartridge json is manifest invalid
    #[tokio::test]
    async fn test0091_missing_cartridge_json_is_manifest_invalid() {
        let root = tempdir().unwrap();
        install_fixture(root.path(), "dev", "nightly", "cart", "1.0.0", None, "cart");
        let out = discover_cartridges(root.path(), &nightly_dev_identity())
            .await
            .unwrap();
        expect_incompatible(&out, CartridgeAttachmentErrorKind::ManifestInvalid);
    }

    // TEST0092: Channel mismatch is bad installation
    #[tokio::test]
    async fn test0092_channel_mismatch_is_misplaced() {
        let root = tempdir().unwrap();
        // Declares release but lives under nightly/ — host is nightly.
        let json = dev_cartridge_json("release", 1);
        install_fixture(
            root.path(),
            "dev",
            "nightly",
            "cart",
            "1.0.0",
            Some(&json),
            "cart",
        );
        let out = discover_cartridges(root.path(), &nightly_dev_identity())
            .await
            .unwrap();
        expect_incompatible(&out, CartridgeAttachmentErrorKind::Misplaced);
    }

    // TEST0094: Fabric manifest mismatch is flagged
    #[tokio::test]
    async fn test0094_fabric_manifest_mismatch_is_flagged() {
        let root = tempdir().unwrap();
        let json = dev_cartridge_json("nightly", 999);
        install_fixture(
            root.path(),
            "dev",
            "nightly",
            "cart",
            "1.0.0",
            Some(&json),
            "cart",
        );
        let out = discover_cartridges(root.path(), &nightly_dev_identity())
            .await
            .unwrap();
        expect_incompatible(
            &out,
            CartridgeAttachmentErrorKind::FabricManifestVersionMismatch,
        );
    }

    // TEST0120: Registry url under dev slug is rejected
    #[tokio::test]
    async fn test0120_registry_url_under_dev_slug_is_rejected() {
        let root = tempdir().unwrap();
        // A non-null registry_url placed under the reserved dev slug violates the
        // three-place rule — read_from_dir rejects it as a bad install context
        // (BadInstallation), surfaced + logged, never hosted. This is the
        // "registry-defined url under dev/ is invalid" rule.
        let json = r#"{"name":"cart","version":"1.0.0","channel":"nightly","registry_url":"https://cartridges.example.com/manifest","entry":"cart","installed_at":"2024-01-01T00:00:00Z","fabric_manifest_version":1}"#;
        install_fixture(
            root.path(),
            "dev",
            "nightly",
            "cart",
            "1.0.0",
            Some(json),
            "cart",
        );
        let out = discover_cartridges(root.path(), &nightly_dev_identity())
            .await
            .unwrap();
        expect_incompatible(&out, CartridgeAttachmentErrorKind::Misplaced);
    }

    // The registry slug for a fixed URL, so tests can place a registry cartridge
    // under the folder that matches its declared registry_url (three-place rule).
    fn registry_slug_for(url: &str) -> String {
        crate::bifaci::cartridge_slug::slug_for(Some(url))
    }

    fn registry_cartridge_json(url: &str, channel: &str, fmv: u32) -> String {
        format!(
            r#"{{"name":"cart","version":"1.0.0","channel":"{channel}","registry_url":"{url}","entry":"cart","installed_at":"2024-01-01T00:00:00Z","fabric_manifest_version":{fmv}}}"#
        )
    }

    // TEST1875: scan-all — a registry slug folder AND the dev slot present on
    // disk are BOTH scanned, regardless of the host's own baked registry. The
    // dev cartridge (null registry under dev/) and the registry cartridge (its
    // url hashing to its slug folder) each reach their probe. Both fixtures lack
    // a real bifaci binary, so both end at HandshakeFailed — proving discovery
    // REACHED them (was not filtered out by a registry pin), which is the
    // behavior under test. A registry-pin rejection would instead surface
    // BadInstallation and never probe.
    #[tokio::test]
    async fn test1875_scan_all_reaches_both_dev_and_registry_slugs() {
        let root = tempdir().unwrap();
        let url = "https://cartridges.example.com/manifest";
        let rslug = registry_slug_for(url);
        // Host baked for a DIFFERENT registry than the on-disk registry cartridge.
        let host = DiscoveryIdentity {
            registry_url: Some("https://other.example.com/manifest".to_string()),
            ..nightly_dev_identity()
        };
        install_fixture(
            root.path(),
            "dev",
            "nightly",
            "devcart",
            "1.0.0",
            Some(&dev_cartridge_json("nightly", 1)),
            "cart",
        );
        install_fixture(
            root.path(),
            &rslug,
            "nightly",
            "regcart",
            "1.0.0",
            Some(&registry_cartridge_json(url, "nightly", 1)),
            "cart",
        );
        let out = discover_cartridges(root.path(), &host).await.unwrap();
        assert_eq!(out.len(), 2, "both slugs must be scanned, got: {out:?}");
        for c in &out {
            match c {
                DiscoveredCartridge::Incompatible { error, .. } => {
                    assert_eq!(
                        error.kind,
                        CartridgeAttachmentErrorKind::HandshakeFailed,
                        "both reached the probe (not registry-pin-rejected): {}",
                        error.message
                    );
                }
                other => panic!("expected probe-stage Incompatible, got {other:?}"),
            }
        }
    }

    // TEST1876: only the host's channel subtree is scanned. A cartridge under a
    // slug's `release/` folder is invisible to a nightly host even though the
    // slug folder is present (its `nightly/` subtree is absent).
    #[tokio::test]
    async fn test1876_other_channel_subtree_is_skipped() {
        let root = tempdir().unwrap();
        let url = "https://cartridges.example.com/manifest";
        let rslug = registry_slug_for(url);
        install_fixture(
            root.path(),
            &rslug,
            "release",
            "regcart",
            "1.0.0",
            Some(&registry_cartridge_json(url, "release", 1)),
            "cart",
        );
        let out = discover_cartridges(root.path(), &nightly_dev_identity())
            .await
            .unwrap();
        assert!(
            out.is_empty(),
            "a release-only slug must be invisible to a nightly host, got: {out:?}"
        );
    }

    // TEST1879: only the host's registry-VERSION subtree is scanned. A cartridge
    // installed under `{slug}/v{N+1}/nightly/…` (a different registry regime) is
    // invisible to a host that speaks v{N} — the version level is pinned exactly
    // like the channel, so v1 and v2 cartridges of the same registry never mix.
    #[tokio::test]
    async fn test1879_other_registry_version_subtree_is_skipped() {
        let root = tempdir().unwrap();
        let url = "https://cartridges.example.com/manifest";
        let rslug = registry_slug_for(url);
        // Hand-place a cartridge under the NEXT registry version's subtree
        // (install_fixture always writes the host's version, so compose directly).
        let other_version = crate::CARTRIDGE_REGISTRY_VERSION + 1;
        let dir = root
            .path()
            .join(&rslug)
            .join(format!("v{other_version}"))
            .join("nightly")
            .join("regcart")
            .join("1.0.0");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("cartridge.json"),
            registry_cartridge_json(url, "nightly", 1),
        )
        .unwrap();
        let entry = dir.join("cart");
        std::fs::write(&entry, "#!/bin/sh\nexit 0\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&entry, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let out = discover_cartridges(root.path(), &nightly_dev_identity())
            .await
            .unwrap();
        assert!(
            out.is_empty(),
            "a cartridge under a different registry-version subtree must be invisible, got: {out:?}"
        );
    }

    // TEST1877: a registry cartridge hand-copied under the WRONG registry slug
    // folder fails the three-place rule (BadInstallation) — scan-all does not
    // mean "accept anywhere", placement must still be self-consistent.
    #[tokio::test]
    async fn test1877_registry_cartridge_under_wrong_slug_is_bad_install() {
        let root = tempdir().unwrap();
        let url = "https://cartridges.example.com/manifest";
        let wrong_slug = registry_slug_for("https://somewhere-else.example.com/manifest");
        let json = registry_cartridge_json(url, "nightly", 1);
        install_fixture(
            root.path(),
            &wrong_slug,
            "nightly",
            "cart",
            "1.0.0",
            Some(&json),
            "cart",
        );
        let out = discover_cartridges(root.path(), &nightly_dev_identity())
            .await
            .unwrap();
        expect_incompatible(&out, CartridgeAttachmentErrorKind::Misplaced);
    }

    /// The `cartridge.json` of a bundled cartridge in the dev slot.
    ///
    /// Placement is self-consistent (null registry → dev slug), so it passes
    /// every earlier check and reaches the bundled-integrity gate, which is
    /// what these tests are about.
    fn bundled_cartridge_json() -> &'static str {
        r#"{"name":"cart","version":"1.0.0","channel":"nightly","registry_url":null,"entry":"cart","installed_at":"2024-01-01T00:00:00Z","installed_from":"bundle","fabric_manifest_version":1}"#
    }

    // TEST1878: a bundled cartridge in a root that proves nothing is refused —
    // on every platform.
    //
    // This is the check macOS did not have. The old rule was platform-split:
    // Linux and Windows verified a baked content hash and macOS verified
    // nothing of ours, trusting Gatekeeper instead. So this test was
    // `#[cfg(not(target_os = "macos"))]` — it asserted that the guard existed
    // on two platforms out of three. It is unconditional now because the guard
    // is.
    #[tokio::test]
    async fn test1878_a_bundled_cartridge_is_refused_where_nothing_proves_it() {
        let root = tempdir().unwrap();
        install_fixture(
            root.path(),
            "dev",
            "nightly",
            "cart",
            "1.0.0",
            Some(bundled_cartridge_json()),
            "cart",
        );
        // `nightly_dev_identity` carries `BundleProof::none` — a root nothing
        // built, which is what the operator's installed-cartridges directory
        // is. A cartridge claiming to be bundled there is in the wrong place.
        let out = discover_cartridges(root.path(), &nightly_dev_identity())
            .await
            .unwrap();
        expect_incompatible(&out, CartridgeAttachmentErrorKind::Misplaced);
        if let DiscoveredCartridge::Incompatible { error, .. } = &out[0] {
            assert!(
                error.message.contains("bundled cartridge integrity"),
                "message should name the bundled-integrity failure: {}",
                error.message
            );
        }
    }

    // TEST1928: a bundled cartridge the manifest records passes, and one it
    // records differently does not.
    //
    // The other half of TEST1878, and the one that proves the gate is a real
    // check rather than a refusal of everything: the same tree, the same
    // cartridge, and the only difference is what the build recorded about it.
    // A gate that always said no would pass TEST1878 alone.
    #[tokio::test]
    async fn test1928_a_bundled_cartridge_passes_exactly_when_the_manifest_records_it() {
        use crate::bifaci::bundle_manifest::{BundleManifest, BundledCartridge};

        let root = tempdir().unwrap();
        install_fixture(
            root.path(),
            "dev",
            "nightly",
            "cart",
            "1.0.0",
            Some(bundled_cartridge_json()),
            "cart",
        );
        let version_dir = root
            .path()
            .join("dev")
            .join(format!("v{}", crate::CARTRIDGE_REGISTRY_VERSION))
            .join("nightly")
            .join("cart")
            .join("1.0.0");
        let recorded =
            crate::bifaci::cartridge_json::hash_cartridge_directory(&version_dir).unwrap();

        let listed = |sha256: String| {
            bundled_identity(BundleManifest::new(
                "dev",
                vec![BundledCartridge {
                    name: "cart".to_string(),
                    version: "1.0.0".to_string(),
                    channel: "nightly".to_string(),
                    sha256,
                }],
            ))
        };

        // Recorded as it is on disk: it gets past the gate. It still ends at
        // the HELLO probe, because the fixture's "entry point" is not a
        // cartridge — what matters is that the failure is no longer the
        // integrity one.
        let out = discover_cartridges(root.path(), &listed(recorded.clone()))
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        if let DiscoveredCartridge::Incompatible { error, .. } = &out[0] {
            assert!(
                !error.message.contains("bundled cartridge integrity"),
                "a cartridge the manifest records must get past the integrity gate: {}",
                error.message
            );
        }

        // Recorded as something else — the cartridge was changed after the
        // build recorded it.
        let out = discover_cartridges(root.path(), &listed("f".repeat(64)))
            .await
            .unwrap();
        expect_incompatible(&out, CartridgeAttachmentErrorKind::Misplaced);
        if let DiscoveredCartridge::Incompatible { error, .. } = &out[0] {
            assert!(
                error.message.contains("bundled cartridge integrity"),
                "a cartridge that differs from the manifest must be refused: {}",
                error.message
            );
        }
    }
}
