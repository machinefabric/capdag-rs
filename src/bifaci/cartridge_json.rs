//! CartridgeJson — install-context metadata for installed cartridges.
//!
//! Every installed cartridge version directory contains a `cartridge.json` file
//! that records how the cartridge was installed and where its entry point is.
//! This is analogous to `provenance.json` for run artifacts.
//!
//! Layout:
//! ```text
//! cartridges/{name}/{version}/
//!   cartridge.json       ← this file
//!   <entry_point_binary>
//!   <supporting_files>
//! ```

use crate::bifaci::cartridge_repo::CartridgeChannel;
use crate::bifaci::cartridge_slug::{slug_for, DEV_SLUG};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// How a cartridge was installed. Pure metadata — not consulted for
/// any host or engine routing decision. Kept around so downstream
/// telemetry / audit / inventory tooling can record install
/// provenance without growing a parallel data structure.
///
/// New variants may be added freely; the deserializer rejects unknown
/// values rather than silently coercing them so a corrupted /
/// future-version installer surfaces immediately. Within the codebase
/// nothing should branch on the variant — see the field's doc comment
/// on [`CartridgeJson::installed_from`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CartridgeInstallSource {
    /// Pulled from a remote cartridge registry (a workspace cartridge install from a registry).
    Registry,
    /// Built locally from the developer's source tree (a workspace cartridge install with no registry).
    Dev,
    /// Shipped pre-installed under the engine's `bundled-cartridges/` tree at app build time.
    Bundle,
    /// Written by the macOS .pkg installer's release path
    /// (`scripts/commands/release.sh`). Distinct from `Bundle`
    /// (engine-bundled cartridges) and `Registry` (remote-pulled).
    AppInstaller,
}

/// Install-context metadata stored in `cartridge.json` inside each cartridge
/// version directory.
///
/// Identity tuple: `(registry_url, channel, name, version)`. A `v1.0.0`
/// of cartridge X published to two different registries are two
/// distinct installs that coexist on disk under different top-level
/// folders. Dev cartridges (built locally without a registry URL)
/// carry `registry_url == None` and live exclusively under the
/// reserved `dev/` folder.
///
/// All four identity fields are required-but-`registry_url` may be
/// null. A missing `registry_url` field in the JSON is a parse error
/// (forces the new schema across every install path); only `null`
/// means dev. The `Deserialize` impl is manual to enforce this
/// stricter "key must be present, value may be null" contract —
/// stock serde collapses absent and explicit-null for `Option<T>`.
#[derive(Debug, Clone, Serialize)]
pub struct CartridgeJson {
    /// Cartridge name (e.g., "pdfcartridge").
    pub name: String,
    /// Version string (e.g., "0.168.411").
    pub version: String,
    /// Distribution channel. The .pkg installer (pkg.sh) writes this
    /// based on which channel was passed at publish time. Required —
    /// no default; reading a cartridge.json without `channel` is a
    /// publish-pipeline bug we want to surface.
    pub channel: CartridgeChannel,
    /// Registry the cartridge was published from, recorded as the
    /// exact URL byte-string the operator/installer used. `None` ⇔
    /// dev install (the cartridge was built locally and installed
    /// without `--registry`). The on-disk top-level folder is
    /// `cartridge_slug::slug_for(registry_url.as_deref())`; the host
    /// validates the folder name against the slug at scan time.
    ///
    /// The field is required-but-nullable: missing `registry_url` in
    /// the JSON is a parse error so an old-schema cartridge.json
    /// surfaces immediately. Only `null` means dev.
    pub registry_url: Option<String>,
    /// Relative path from the version directory to the executable entry point.
    /// For single-binary cartridges this is just the binary filename.
    /// For directory cartridges it may be a nested path.
    pub entry: String,
    /// RFC3339 timestamp of when the cartridge was installed.
    pub installed_at: String,
    /// Optional install-provenance hint. **Not consulted for any
    /// routing or attachment decision** — the dev-vs-not-dev signal
    /// the host actually uses is `registry_url` (null ⇔ dev). This
    /// field is kept around as opaque metadata for future telemetry
    /// or audit tooling. May be omitted entirely from on-disk
    /// `cartridge.json` files; absence is not a parse error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub installed_from: Option<CartridgeInstallSource>,
    /// URL the package was downloaded from (empty for dev/bundle installs).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source_url: String,
    /// SHA256 hash of the original package (tarball or binary).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub package_sha256: String,
    /// Size in bytes of the original package.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub package_size: u64,
    /// Fabric registry manifest version this cartridge was built against.
    ///
    /// 0 (the default for absent / legacy cartridge.json files) means
    /// the cartridge predates the fabric registry's versioning protocol
    /// and only understands the frozen flat-path v0 registry layout.
    /// >= 1 means the cartridge was built against manifest version N
    /// of the fabric registry and resolves URNs via the versioned-path
    /// layout strictly pinned at N.
    ///
    /// The host engine pins to its own baked-in `FABRIC_MANIFEST_VERSION`
    /// at build time and only attaches cartridges whose `fabric_manifest_version`
    /// equals its own. A mismatch surfaces the cartridge as
    /// "discovered but incompatible" without loading it.
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub fabric_manifest_version: u32,
}

fn is_zero(v: &u64) -> bool {
    *v == 0
}

fn is_zero_u32(v: &u32) -> bool {
    *v == 0
}

/// Render `installed_at` for a cartridge being installed right now.
///
/// The field is declared RFC3339 and every reader (and every fixture) treats
/// it as one, so every producer goes through this one function — a bare epoch
/// count with a `Z` stuck on the end parses as neither a number nor a
/// timestamp, and would leave the field readable only by a human who already
/// knew what it was.
///
/// Implemented against `SystemTime` rather than a date crate: this is the only
/// place capdag formats a calendar date, and a dependency for it would be paid
/// by every consumer of the library.
pub fn install_timestamp_now() -> String {
    use std::time::SystemTime;
    let secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system clock before the Unix epoch")
        .as_secs();
    format_rfc3339_utc(secs)
}

/// Format Unix seconds as `YYYY-MM-DDTHH:MM:SSZ`.
///
/// Uses Howard Hinnant's `civil_from_days`, which is exact for the whole
/// proleptic Gregorian calendar — no leap-year special-casing to get wrong.
pub(crate) fn format_rfc3339_utc(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let time_of_day = secs % 86_400;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year,
        month,
        day,
        time_of_day / 3600,
        (time_of_day % 3600) / 60,
        time_of_day % 60
    )
}

/// Days since 1970-01-01 → (year, month, day), civil Gregorian.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11], March-based
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

impl<'de> Deserialize<'de> for CartridgeJson {
    /// Manual deserializer that enforces "required-but-nullable" for
    /// `registry_url` — the JSON key MUST be present (otherwise parse
    /// fails); the value MAY be null (dev install) or a string
    /// (registry install). Stock `derive(Deserialize)` would treat
    /// missing-key and explicit-null identically for `Option<T>`,
    /// silently accepting old-schema files. Going through
    /// `serde_json::Value` once lets us check key presence directly.
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;

        let value = serde_json::Value::deserialize(deserializer)?;
        let obj = value
            .as_object()
            .ok_or_else(|| D::Error::custom("cartridge.json must be a JSON object"))?;
        if !obj.contains_key("registry_url") {
            return Err(D::Error::missing_field("registry_url"));
        }

        // Re-deserialize via a derive-like inner type. Mirroring the
        // public struct's fields keeps the field defaults and
        // skip_serializing_if on the public type while letting us
        // do the upstream key-presence check first.
        #[derive(Deserialize)]
        struct CartridgeJsonInner {
            name: String,
            version: String,
            channel: CartridgeChannel,
            registry_url: Option<String>,
            entry: String,
            installed_at: String,
            #[serde(default)]
            installed_from: Option<CartridgeInstallSource>,
            #[serde(default)]
            source_url: String,
            #[serde(default)]
            package_sha256: String,
            #[serde(default)]
            package_size: u64,
            #[serde(default)]
            fabric_manifest_version: u32,
        }
        let inner =
            serde_json::from_value::<CartridgeJsonInner>(value).map_err(D::Error::custom)?;
        Ok(CartridgeJson {
            name: inner.name,
            version: inner.version,
            channel: inner.channel,
            registry_url: inner.registry_url,
            entry: inner.entry,
            installed_at: inner.installed_at,
            installed_from: inner.installed_from,
            source_url: inner.source_url,
            package_sha256: inner.package_sha256,
            package_size: inner.package_size,
            fabric_manifest_version: inner.fabric_manifest_version,
        })
    }
}

/// Verdict from `validate_registry_url_scheme`. Distinguishes "OK"
/// from each rejection reason so callers can render an actionable
/// message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryUrlSchemeResult {
    /// URL is acceptable (either dev-mode allows it, or scheme is
    /// `https`). Carries the original URL string so the caller can
    /// keep using it without re-borrowing.
    Ok,
    /// URL string didn't parse as a valid URL.
    NotAUrl(String),
    /// URL parsed but scheme is not `https` and dev-mode is off.
    /// Carries the offending scheme so the caller can include it in
    /// the operator-facing message.
    NonHttps { scheme: String },
}

/// Validate that a non-null `registry_url` uses the `https` scheme
/// — UNLESS `dev_mode` is set, in which case any well-formed URL is
/// accepted (so developers can point at `http://localhost:port` and
/// similar during integration testing).
///
/// The rule lives at the deepest layer (this crate, used by every
/// consumer that loads a `cartridge.json` or a HELLO manifest) so a
/// caller can never bypass it by parsing the URL out of band. Each
/// loader in the engine and the XPC service threads its own
/// `dev_mode` value through — typically derived from
/// `option_env!("MFR_DEV_MODE")` at the binary's build time, so
/// release / nightly engines reject non-https URLs unconditionally.
///
/// Dev cartridges (`registry_url == None`) never go through this
/// validator — they have no URL to check.
pub fn validate_registry_url_scheme(url: &str, dev_mode: bool) -> RegistryUrlSchemeResult {
    // Cheap parse: split once on `://`. We don't pull in a URL
    // crate just for this check; the rule is "scheme must be the
    // literal bytes `https`", and full URL validation is the
    // caller's job (the registry fetcher will fail loudly on a
    // syntactically-invalid URL).
    let (scheme, rest) = match url.split_once("://") {
        Some(parts) => parts,
        None => return RegistryUrlSchemeResult::NotAUrl(url.to_string()),
    };
    if rest.is_empty() {
        return RegistryUrlSchemeResult::NotAUrl(url.to_string());
    }
    if dev_mode {
        // Dev mode: accept any well-formed scheme. We still require
        // SOME scheme to be present (caught above) — an empty
        // scheme is malformed regardless of mode.
        return RegistryUrlSchemeResult::Ok;
    }
    if scheme.eq_ignore_ascii_case("https") {
        RegistryUrlSchemeResult::Ok
    } else {
        RegistryUrlSchemeResult::NonHttps {
            scheme: scheme.to_string(),
        }
    }
}

/// Errors when reading or validating a `cartridge.json`.
#[derive(Debug, thiserror::Error)]
pub enum CartridgeJsonError {
    #[error("cartridge.json not found at {0}")]
    NotFound(PathBuf),
    #[error("failed to read cartridge.json at {path}: {source}")]
    ReadFailed {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid cartridge.json at {path}: {source}")]
    InvalidJson {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("cartridge.json at {path}: entry point '{entry}' does not exist")]
    EntryPointMissing { path: PathBuf, entry: String },
    #[error("cartridge.json at {path}: entry point '{entry}' is not executable")]
    EntryPointNotExecutable { path: PathBuf, entry: String },
    #[error("cartridge.json at {path}: entry path '{entry}' escapes version directory")]
    EntryPathEscape { path: PathBuf, entry: String },
    #[error("failed to write cartridge.json at {path}: {source}")]
    WriteFailed {
        path: PathBuf,
        source: std::io::Error,
    },
    /// The folder the cartridge.json was loaded from doesn't match the
    /// slug derived from its declared `registry_url`. This is the
    /// three-place consistency check: the top-level folder, the
    /// provenance's `registry_url`, and the cartridge's own HELLO
    /// manifest must all agree. A failure here means the cartridge
    /// was either copied between registries by hand or the installer
    /// is buggy — either way the engine refuses to attach it.
    #[error(
        "cartridge.json at {path}: registry slug mismatch — registry_url={registry_url:?} \
         hashes to slug='{expected_slug}' but the directory tree placed it under '{actual_slug}'"
    )]
    RegistrySlugMismatch {
        path: PathBuf,
        registry_url: Option<String>,
        expected_slug: String,
        actual_slug: String,
    },
}

impl CartridgeJson {
    /// Read and validate a `cartridge.json` from a version directory.
    ///
    /// `expected_slug` is the registry slug the host reached the
    /// version directory through — i.e. the second-to-top-level
    /// folder name in the canonical layout
    /// `{root}/{slug}/{channel}/{name}/{version}/`. The host knows
    /// the slug because it walked there; passing it in lets us
    /// enforce the three-place rule (folder slug ⇔ provenance
    /// `registry_url` ⇔ cartridge HELLO manifest's `registry_url`)
    /// inside the parser instead of leaving it to every caller to
    /// remember.
    ///
    /// Validates:
    /// - File exists and is valid JSON
    /// - Entry point path does not escape the version directory
    /// - Entry point binary exists and is executable
    /// - `slug_for(registry_url) == expected_slug` (third place — HELLO
    ///   manifest comparison — is checked by the host once the
    ///   cartridge process responds, since that data isn't on disk).
    pub fn read_from_dir(
        version_dir: &Path,
        expected_slug: &str,
    ) -> Result<Self, CartridgeJsonError> {
        let json_path = version_dir.join("cartridge.json");

        if !json_path.exists() {
            return Err(CartridgeJsonError::NotFound(json_path));
        }

        let contents =
            std::fs::read_to_string(&json_path).map_err(|e| CartridgeJsonError::ReadFailed {
                path: json_path.clone(),
                source: e,
            })?;

        let cartridge_json: CartridgeJson =
            serde_json::from_str(&contents).map_err(|e| CartridgeJsonError::InvalidJson {
                path: json_path.clone(),
                source: e,
            })?;

        // Three-place consistency rule (places 1 + 2): the folder slug
        // must match the slug derived from the provenance's
        // registry_url. None+`dev` and Some(url)+slug(url) are the
        // only valid pairings; any other combination — including a
        // null registry_url under a non-dev folder, or a non-null
        // registry_url under the dev folder — is an installer bug or
        // a tampered tree.
        let derived_slug = slug_for(cartridge_json.registry_url.as_deref());
        if derived_slug != expected_slug {
            return Err(CartridgeJsonError::RegistrySlugMismatch {
                path: json_path,
                registry_url: cartridge_json.registry_url.clone(),
                expected_slug: derived_slug,
                actual_slug: expected_slug.to_string(),
            });
        }

        // Validate entry point exists
        let entry_path = version_dir.join(&cartridge_json.entry);
        if !entry_path.exists() {
            return Err(CartridgeJsonError::EntryPointMissing {
                path: json_path,
                entry: cartridge_json.entry,
            });
        }

        // Validate entry path does not escape version directory
        let canonical_dir = version_dir
            .canonicalize()
            .unwrap_or_else(|_| version_dir.to_path_buf());
        let canonical_entry = entry_path
            .canonicalize()
            .unwrap_or_else(|_| entry_path.clone());

        if !canonical_entry.starts_with(&canonical_dir) {
            return Err(CartridgeJsonError::EntryPathEscape {
                path: json_path,
                entry: cartridge_json.entry,
            });
        }

        // Validate entry point is executable
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let meta =
                std::fs::metadata(&entry_path).map_err(|e| CartridgeJsonError::ReadFailed {
                    path: json_path.clone(),
                    source: e,
                })?;
            if meta.permissions().mode() & 0o111 == 0 {
                return Err(CartridgeJsonError::EntryPointNotExecutable {
                    path: json_path,
                    entry: cartridge_json.entry,
                });
            }
        }

        Ok(cartridge_json)
    }

    /// Returns true when this cartridge was installed as a dev build
    /// (no registry URL). Convenience for hosts that need to gate
    /// dev-only behavior — callers must still verify the on-disk
    /// folder name is `cartridge_slug::DEV_SLUG`; this method only
    /// reports the provenance side.
    pub fn is_dev_install(&self) -> bool {
        self.registry_url.is_none()
    }

    /// Compute the on-disk slug this provenance must live under.
    /// Equivalent to `slug_for(self.registry_url.as_deref())`.
    pub fn registry_slug(&self) -> String {
        slug_for(self.registry_url.as_deref())
    }

    /// Resolve the absolute path to the entry point binary.
    pub fn resolve_entry_point(&self, version_dir: &Path) -> PathBuf {
        version_dir.join(&self.entry)
    }

    /// Write this `cartridge.json` to a version directory.
    pub fn write_to_dir(&self, version_dir: &Path) -> Result<(), CartridgeJsonError> {
        let json_path = version_dir.join("cartridge.json");
        let contents =
            serde_json::to_string_pretty(self).expect("CartridgeJson serialization cannot fail");
        std::fs::write(&json_path, contents.as_bytes()).map_err(|e| {
            CartridgeJsonError::WriteFailed {
                path: json_path,
                source: e,
            }
        })
    }
}

/// Compute a deterministic SHA256 hash of a directory tree.
///
/// Walks all files in the directory recursively, sorts them by relative path,
/// then hashes each file's relative path (UTF-8 bytes) followed by its contents.
/// This produces a stable identity hash regardless of filesystem ordering.
///
/// Symbolic links are followed (their targets are hashed, not the links).
/// `cartridge.json` itself is excluded from the hash — it contains install-time
/// metadata (like `installed_at`) that changes between installs of the same content.
pub fn hash_cartridge_directory(dir: &Path) -> Result<String, std::io::Error> {
    use sha2::{Digest, Sha256};

    let mut files: Vec<(String, PathBuf)> = Vec::new();
    collect_files(dir, dir, &mut files)?;
    files.sort_by(|a, b| a.0.cmp(&b.0));

    let mut hasher = Sha256::new();
    for (relative_path, full_path) in &files {
        hasher.update(relative_path.as_bytes());
        let contents = std::fs::read(full_path)?;
        hasher.update(&contents);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

/// Recursively collect all files in a directory with their relative paths.
fn collect_files(
    base: &Path,
    current: &Path,
    out: &mut Vec<(String, PathBuf)>,
) -> Result<(), std::io::Error> {
    for entry in std::fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;

        if file_type.is_dir() {
            collect_files(base, &path, out)?;
        } else if file_type.is_file() || file_type.is_symlink() {
            let relative = path
                .strip_prefix(base)
                .expect("BUG: path must be under base");
            let relative_str = relative.to_string_lossy().to_string();

            // Exclude cartridge.json from identity hash — it contains
            // install-time metadata that varies between installs of identical content.
            if relative_str == "cartridge.json" {
                continue;
            }

            out.push((relative_str, path));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bifaci::cartridge_slug::{slug_for, DEV_SLUG};
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    const TEST_REGISTRY: &str = "https://cartridges.machinefabric.com/manifest";

    fn registry_slug() -> String {
        slug_for(Some(TEST_REGISTRY))
    }

    // TEST1243: Cartridge JSON round-trips through serde without losing required fields.
    #[test]
    fn test1243_roundtrip_serialize_deserialize() {
        let cj = CartridgeJson {
            name: "pdfcartridge".to_string(),
            version: "0.168.411".to_string(),
            entry: "pdfcartridge".to_string(),
            installed_at: "2026-04-12T10:00:00Z".to_string(),
            installed_from: Some(CartridgeInstallSource::Registry),
            channel: CartridgeChannel::Release,
            registry_url: Some(TEST_REGISTRY.to_string()),
            source_url:
                "https://cartridges.machinefabric.com/release/pdfcartridge/0.168.411/pdfcartridge-0.168.411.pkg"
                    .to_string(),
            package_sha256: "abc123".to_string(),
            package_size: 12345,
            fabric_manifest_version: 0,
        };

        let json = serde_json::to_string_pretty(&cj).unwrap();
        let parsed: CartridgeJson = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.name, "pdfcartridge");
        assert_eq!(parsed.version, "0.168.411");
        assert_eq!(parsed.entry, "pdfcartridge");
        assert_eq!(
            parsed.installed_from,
            Some(CartridgeInstallSource::Registry)
        );
        assert_eq!(parsed.channel, CartridgeChannel::Release);
        assert_eq!(parsed.registry_url.as_deref(), Some(TEST_REGISTRY));
    }

    // TEST1506: Channel round-trips correctly. A nightly cartridge.json
    // must deserialize back to the Nightly variant — channels are
    // independent namespaces, conflating them would be a real bug.
    #[test]
    fn test1506_channel_roundtrip_nightly() {
        let cj = CartridgeJson {
            name: "pdfcartridge".to_string(),
            version: "0.168.411".to_string(),
            entry: "pdfcartridge".to_string(),
            installed_at: "2026-04-12T10:00:00Z".to_string(),
            installed_from: Some(CartridgeInstallSource::Registry),
            channel: CartridgeChannel::Nightly,
            registry_url: Some(TEST_REGISTRY.to_string()),
            source_url: "https://cartridges.machinefabric.com/nightly/pdfcartridge/0.168.411/pdfcartridge-0.168.411.pkg".to_string(),
            package_sha256: "abc123".to_string(),
            package_size: 12345,
            fabric_manifest_version: 0,
        };
        let json = serde_json::to_string(&cj).unwrap();
        // Wire form is lowercase (matches CartridgeChannel's
        // serde rename_all = "lowercase"). Verify the literal is in
        // there so the .pkg installer's jq output is compatible.
        assert!(
            json.contains("\"channel\":\"nightly\""),
            "expected channel='nightly' in serialized form, got: {}",
            json
        );
        let parsed: CartridgeJson = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.channel, CartridgeChannel::Nightly);
    }

    // TEST1507: Reading a cartridge.json without `channel` is a hard
    // error. We never assume a default — that would let an
    // unrecognized install silently masquerade as release.
    #[test]
    fn test1507_missing_channel_fails_to_parse() {
        let json = r#"{
            "name": "pdfcartridge",
            "version": "0.168.411",
            "entry": "pdfcartridge",
            "installed_at": "2026-04-12T10:00:00Z",
            "installed_from": "registry",
            "registry_url": null
        }"#;
        let result: Result<CartridgeJson, _> = serde_json::from_str(json);
        assert!(
            result.is_err(),
            "cartridge.json without `channel` must fail to parse, got: {:?}",
            result
        );
    }

    // TEST1508: Reading a cartridge.json without `registry_url` is a
    // hard error too. The field is required-but-nullable; absence
    // means the file was written by a pre-registry-aware installer
    // and we can't tell whether it was meant to be dev or registry —
    // both cases fail the three-place check, so we surface the
    // schema gap immediately.
    #[test]
    fn test1508_missing_registry_url_fails_to_parse() {
        let json = r#"{
            "name": "pdfcartridge",
            "version": "0.168.411",
            "channel": "nightly",
            "entry": "pdfcartridge",
            "installed_at": "2026-04-12T10:00:00Z",
            "installed_from": "registry"
        }"#;
        let result: Result<CartridgeJson, _> = serde_json::from_str(json);
        assert!(
            result.is_err(),
            "cartridge.json without `registry_url` must fail to parse, got: {:?}",
            result
        );
    }

    // TEST1244: Dev-installed cartridge metadata omits registry-only package fields when serialized.
    #[test]
    fn test1244_dev_install_omits_optional_fields() {
        let cj = CartridgeJson {
            name: "testcartridge".to_string(),
            version: "0.1.0".to_string(),
            entry: "testcartridge".to_string(),
            installed_at: "2026-04-12T10:00:00Z".to_string(),
            installed_from: Some(CartridgeInstallSource::Dev),
            channel: CartridgeChannel::Nightly,
            registry_url: None,
            source_url: String::new(),
            package_sha256: String::new(),
            package_size: 0,
            fabric_manifest_version: 0,
        };

        let json = serde_json::to_string(&cj).unwrap();
        assert!(!json.contains("source_url"));
        assert!(!json.contains("package_sha256"));
        assert!(!json.contains("package_size"));
        // `registry_url` MUST be present-and-null on the wire even
        // for dev installs — its absence triggers a parse error
        // (test1243d) so a downstream reader can't accidentally
        // treat a missing field as "dev".
        assert!(
            json.contains("\"registry_url\":null"),
            "dev install must serialize registry_url=null explicitly, got: {}",
            json
        );
    }

    // TEST1513: `installed_from` is opaque optional metadata. A
    // cartridge.json that omits the field MUST parse cleanly with
    // `installed_from == None`, and a CartridgeJson with
    // `installed_from == None` MUST NOT emit the key on serialize.
    // Pins the new contract that nothing in the host or engine
    // branches on this field — it's audit/telemetry hint only and
    // the absence-vs-Some distinction must round-trip.
    #[test]
    fn test1513_installed_from_optional_round_trip() {
        // Parse: missing key → None.
        let json_without = r#"{
            "name": "pdfcartridge",
            "version": "0.168.411",
            "channel": "nightly",
            "registry_url": null,
            "entry": "pdfcartridge",
            "installed_at": "2026-04-12T10:00:00Z"
        }"#;
        let parsed: CartridgeJson = serde_json::from_str(json_without)
            .expect("cartridge.json without installed_from must parse");
        assert!(
            parsed.installed_from.is_none(),
            "missing installed_from must deserialize to None, got: {:?}",
            parsed.installed_from
        );

        // Serialize: None must NOT emit the key. Without
        // `skip_serializing_if = "Option::is_none"` we'd write
        // `"installed_from":null`, which would re-introduce a
        // distinction between absent-and-null on the wire that
        // nothing in the codebase cares about and that downstream
        // tooling could misread.
        let cj = CartridgeJson {
            name: "x".to_string(),
            version: "0.1.0".to_string(),
            channel: CartridgeChannel::Nightly,
            registry_url: None,
            entry: "x".to_string(),
            installed_at: "2026-01-01T00:00:00Z".to_string(),
            installed_from: None,
            source_url: String::new(),
            package_sha256: String::new(),
            package_size: 0,
            fabric_manifest_version: 0,
        };
        let serialized = serde_json::to_string(&cj).unwrap();
        assert!(
            !serialized.contains("installed_from"),
            "None installed_from must not appear in JSON, got: {}",
            serialized
        );

        // Round-trip: Some(variant) → JSON → Some(same variant).
        let cj_some = CartridgeJson {
            installed_from: Some(CartridgeInstallSource::Bundle),
            ..cj
        };
        let s2 = serde_json::to_string(&cj_some).unwrap();
        let p2: CartridgeJson = serde_json::from_str(&s2).unwrap();
        assert_eq!(p2.installed_from, Some(CartridgeInstallSource::Bundle));
    }

    // TEST1245: Reading cartridge metadata fails when the declared entry binary is missing.
    #[test]
    fn test1245_read_from_dir_validates_entry_exists() {
        let dir = tempfile::tempdir().unwrap();
        let cj = CartridgeJson {
            name: "test".to_string(),
            version: "1.0".to_string(),
            entry: "nonexistent_binary".to_string(),
            installed_at: "2026-04-12T10:00:00Z".to_string(),
            installed_from: Some(CartridgeInstallSource::Dev),
            channel: CartridgeChannel::Nightly,
            registry_url: None,
            source_url: String::new(),
            package_sha256: String::new(),
            package_size: 0,
            fabric_manifest_version: 0,
        };
        let json = serde_json::to_string_pretty(&cj).unwrap();
        std::fs::write(dir.path().join("cartridge.json"), &json).unwrap();

        let err = CartridgeJson::read_from_dir(dir.path(), DEV_SLUG).unwrap_err();
        assert!(matches!(err, CartridgeJsonError::EntryPointMissing { .. }));
    }

    // TEST1246: Cartridge entry points cannot escape the cartridge directory with relative paths.
    #[test]
    #[cfg(unix)]
    fn test1246_read_from_dir_rejects_path_escape() {
        let dir = tempfile::tempdir().unwrap();

        // Create a binary outside the version dir
        let outside = dir.path().parent().unwrap().join("escaped_binary");
        std::fs::write(&outside, b"#!/bin/sh").unwrap();
        std::fs::set_permissions(&outside, std::fs::Permissions::from_mode(0o755)).unwrap();

        let cj = CartridgeJson {
            name: "test".to_string(),
            version: "1.0".to_string(),
            entry: "../escaped_binary".to_string(),
            installed_at: "2026-04-12T10:00:00Z".to_string(),
            installed_from: Some(CartridgeInstallSource::Dev),
            channel: CartridgeChannel::Nightly,
            registry_url: None,
            source_url: String::new(),
            package_sha256: String::new(),
            package_size: 0,
            fabric_manifest_version: 0,
        };
        let json = serde_json::to_string_pretty(&cj).unwrap();
        std::fs::write(dir.path().join("cartridge.json"), &json).unwrap();

        let err = CartridgeJson::read_from_dir(dir.path(), DEV_SLUG).unwrap_err();
        assert!(matches!(err, CartridgeJsonError::EntryPathEscape { .. }));

        // Cleanup
        let _ = std::fs::remove_file(&outside);
    }

    // TEST1247: Valid cartridge directories load successfully and resolve their entry point.
    #[test]
    #[cfg(unix)]
    fn test1247_read_from_dir_succeeds_with_valid_cartridge() {
        let dir = tempfile::tempdir().unwrap();
        let binary_path = dir.path().join("mycartridge");
        std::fs::write(&binary_path, b"#!/bin/sh\necho hello").unwrap();
        std::fs::set_permissions(&binary_path, std::fs::Permissions::from_mode(0o755)).unwrap();

        let cj = CartridgeJson {
            name: "mycartridge".to_string(),
            version: "1.0.0".to_string(),
            entry: "mycartridge".to_string(),
            installed_at: "2026-04-12T10:00:00Z".to_string(),
            installed_from: Some(CartridgeInstallSource::Bundle),
            channel: CartridgeChannel::Release,
            registry_url: Some(TEST_REGISTRY.to_string()),
            source_url: String::new(),
            package_sha256: String::new(),
            package_size: 0,
            fabric_manifest_version: 0,
        };
        cj.write_to_dir(dir.path()).unwrap();

        let loaded = CartridgeJson::read_from_dir(dir.path(), &registry_slug()).unwrap();
        assert_eq!(loaded.name, "mycartridge");
        assert_eq!(loaded.version, "1.0.0");
        assert_eq!(loaded.resolve_entry_point(dir.path()), binary_path);
    }

    // TEST1509: Three-place rule — a registry cartridge whose folder
    // slug doesn't match `slug_for(registry_url)` is rejected. This
    // catches the case where a cartridge tree was hand-copied
    // between registry roots, or a buggy installer wrote the wrong
    // slug. The error names both slugs so the operator can tell at
    // a glance which side is wrong.
    #[test]
    #[cfg(unix)]
    fn test1509_read_from_dir_rejects_slug_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let binary_path = dir.path().join("mycartridge");
        std::fs::write(&binary_path, b"#!/bin/sh\necho hello").unwrap();
        std::fs::set_permissions(&binary_path, std::fs::Permissions::from_mode(0o755)).unwrap();

        let cj = CartridgeJson {
            name: "mycartridge".to_string(),
            version: "1.0.0".to_string(),
            entry: "mycartridge".to_string(),
            installed_at: "2026-04-12T10:00:00Z".to_string(),
            installed_from: Some(CartridgeInstallSource::Registry),
            channel: CartridgeChannel::Release,
            registry_url: Some(TEST_REGISTRY.to_string()),
            source_url: String::new(),
            package_sha256: String::new(),
            package_size: 0,
            fabric_manifest_version: 0,
        };
        cj.write_to_dir(dir.path()).unwrap();

        // Pretend the host walked through a folder whose name does
        // not match the cartridge's registry_url. This mimics
        // "someone copied the registry tree into the dev folder" or
        // "installer wrote into the wrong slug".
        let err = CartridgeJson::read_from_dir(dir.path(), "dead00beefcafe00").unwrap_err();
        match err {
            CartridgeJsonError::RegistrySlugMismatch {
                expected_slug,
                actual_slug,
                ..
            } => {
                assert_eq!(expected_slug, registry_slug());
                assert_eq!(actual_slug, "dead00beefcafe00");
            }
            other => panic!("expected RegistrySlugMismatch, got: {:?}", other),
        }
    }

    // TEST1510: Three-place rule — a dev cartridge.json under a
    // non-dev folder is rejected. Equivalent to a dev-built
    // cartridge being moved into a registry's folder; the host
    // refuses because the binary was never built/signed for that
    // registry.
    #[test]
    #[cfg(unix)]
    fn test1510_read_from_dir_rejects_dev_in_registry_folder() {
        let dir = tempfile::tempdir().unwrap();
        let binary_path = dir.path().join("mycartridge");
        std::fs::write(&binary_path, b"#!/bin/sh\necho hello").unwrap();
        std::fs::set_permissions(&binary_path, std::fs::Permissions::from_mode(0o755)).unwrap();

        let cj = CartridgeJson {
            name: "mycartridge".to_string(),
            version: "1.0.0".to_string(),
            entry: "mycartridge".to_string(),
            installed_at: "2026-04-12T10:00:00Z".to_string(),
            installed_from: Some(CartridgeInstallSource::Dev),
            channel: CartridgeChannel::Nightly,
            registry_url: None, // dev provenance
            source_url: String::new(),
            package_sha256: String::new(),
            package_size: 0,
            fabric_manifest_version: 0,
        };
        cj.write_to_dir(dir.path()).unwrap();

        let err = CartridgeJson::read_from_dir(dir.path(), &registry_slug()).unwrap_err();
        assert!(
            matches!(err, CartridgeJsonError::RegistrySlugMismatch { .. }),
            "expected RegistrySlugMismatch for dev provenance under registry folder, got: {:?}",
            err
        );
    }

    // TEST1511: Three-place rule — a registry cartridge.json under
    // the dev folder is rejected. Equivalent to a published
    // cartridge being dropped into the dev tree; the dev tree is
    // explicitly the only place a null `registry_url` is allowed,
    // so a non-null one here means the layout is corrupted.
    #[test]
    #[cfg(unix)]
    fn test1511_read_from_dir_rejects_registry_in_dev_folder() {
        let dir = tempfile::tempdir().unwrap();
        let binary_path = dir.path().join("mycartridge");
        std::fs::write(&binary_path, b"#!/bin/sh\necho hello").unwrap();
        std::fs::set_permissions(&binary_path, std::fs::Permissions::from_mode(0o755)).unwrap();

        let cj = CartridgeJson {
            name: "mycartridge".to_string(),
            version: "1.0.0".to_string(),
            entry: "mycartridge".to_string(),
            installed_at: "2026-04-12T10:00:00Z".to_string(),
            installed_from: Some(CartridgeInstallSource::Registry),
            channel: CartridgeChannel::Release,
            registry_url: Some(TEST_REGISTRY.to_string()),
            source_url: String::new(),
            package_sha256: String::new(),
            package_size: 0,
            fabric_manifest_version: 0,
        };
        cj.write_to_dir(dir.path()).unwrap();

        let err = CartridgeJson::read_from_dir(dir.path(), DEV_SLUG).unwrap_err();
        assert!(
            matches!(err, CartridgeJsonError::RegistrySlugMismatch { .. }),
            "expected RegistrySlugMismatch for registry provenance under dev folder, got: {:?}",
            err
        );
    }

    // TEST1512: A dev cartridge.json under the dev folder is
    // accepted. This is the only path that ends with a successful
    // dev install; together with 1510/1511 it pins the rule that
    // dev provenance and the dev folder are an inseparable pair.
    #[test]
    #[cfg(unix)]
    fn test1512_read_from_dir_accepts_dev_in_dev_folder() {
        let dir = tempfile::tempdir().unwrap();
        let binary_path = dir.path().join("mycartridge");
        std::fs::write(&binary_path, b"#!/bin/sh\necho hello").unwrap();
        std::fs::set_permissions(&binary_path, std::fs::Permissions::from_mode(0o755)).unwrap();

        let cj = CartridgeJson {
            name: "mycartridge".to_string(),
            version: "1.0.0".to_string(),
            entry: "mycartridge".to_string(),
            installed_at: "2026-04-12T10:00:00Z".to_string(),
            installed_from: Some(CartridgeInstallSource::Dev),
            channel: CartridgeChannel::Nightly,
            registry_url: None,
            source_url: String::new(),
            package_sha256: String::new(),
            package_size: 0,
            fabric_manifest_version: 0,
        };
        cj.write_to_dir(dir.path()).unwrap();

        let loaded = CartridgeJson::read_from_dir(dir.path(), DEV_SLUG).unwrap();
        assert!(loaded.is_dev_install());
        assert_eq!(loaded.registry_slug(), DEV_SLUG);
    }

    // TEST1248: Cartridge directory hashes stay stable across metadata changes and change on content edits.
    #[test]
    fn test1248_hash_cartridge_directory_is_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("binary"), b"executable content").unwrap();
        std::fs::write(dir.path().join("data.bin"), b"some data").unwrap();
        std::fs::create_dir(dir.path().join("subdir")).unwrap();
        std::fs::write(dir.path().join("subdir/nested.txt"), b"nested file").unwrap();

        // Write cartridge.json — should be excluded from hash
        std::fs::write(dir.path().join("cartridge.json"), b"{}").unwrap();

        let hash1 = hash_cartridge_directory(dir.path()).unwrap();
        let hash2 = hash_cartridge_directory(dir.path()).unwrap();
        assert_eq!(hash1, hash2);

        // Changing cartridge.json should NOT change the hash
        std::fs::write(dir.path().join("cartridge.json"), b"{\"different\": true}").unwrap();
        let hash3 = hash_cartridge_directory(dir.path()).unwrap();
        assert_eq!(hash1, hash3);

        // Changing actual content SHOULD change the hash
        std::fs::write(dir.path().join("binary"), b"different content").unwrap();
        let hash4 = hash_cartridge_directory(dir.path()).unwrap();
        assert_ne!(hash1, hash4);
    }

    // TEST1249: A flat single-binary cartridge directory still produces a SHA-256 content hash.
    #[test]
    fn test1249_hash_single_binary_matches_flat_layout() {
        // A directory with just one binary should hash consistently
        let dir = tempfile::tempdir().unwrap();
        let content = b"binary content here";
        std::fs::write(dir.path().join("pdfcartridge"), content).unwrap();

        let hash = hash_cartridge_directory(dir.path()).unwrap();
        assert!(!hash.is_empty());
        assert_eq!(hash.len(), 64); // SHA256 hex length
    }

    // TEST7153: `installed_at` is a real RFC3339 UTC timestamp, at known
    // epoch instants and at the instants that break naive date arithmetic —
    // a leap day, the day after one, and a century year that is NOT a leap
    // year. Emitting a bare epoch count with a `Z` appended would satisfy
    // "some string ending in Z" and satisfy nothing else; every reader and
    // every fixture in the tree treats this field as a parseable timestamp.
    #[test]
    fn test7153_install_timestamp_is_rfc3339_utc() {
        for (secs, want) in [
            (0u64, "1970-01-01T00:00:00Z"),
            (1_000_000_000, "2001-09-09T01:46:40Z"),
            // 2024-02-29 — a leap day in a leap year divisible by 4.
            (1_709_164_800, "2024-02-29T00:00:00Z"),
            // The instant after it: the rollover a naive +1 gets wrong.
            (1_709_251_199, "2024-02-29T23:59:59Z"),
            // 2100-03-01 — 2100 is divisible by 100 but not 400, so it has
            // NO Feb 29. A leap rule of "divisible by 4" lands a day early.
            (4_107_542_400, "2100-03-01T00:00:00Z"),
        ] {
            assert_eq!(format_rfc3339_utc(secs), want, "epoch {secs}");
        }

        // The live producer emits the same shape, and a plausible present
        // instant — a broken epoch-to-civil conversion typically lands in
        // 1970 or the far future rather than producing a malformed string.
        let now = install_timestamp_now();
        assert_eq!(now.len(), 20, "not RFC3339-shaped: {now}");
        assert!(now.ends_with('Z'), "not UTC-marked: {now}");
        let year: i64 = now[..4].parse().expect("year is not numeric");
        assert!(
            (2020..2200).contains(&year),
            "the current year came out as {year}: {now}"
        );
    }
}
