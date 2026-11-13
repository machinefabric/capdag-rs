//! What every command works with: the fabric it resolves against, where
//! cartridges live, what this build is bound to, and what is attached.

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::OnceCell;

use super::error::{CliError, CliResult};
use super::term::Term;
use crate::bifaci::cartridge_repo::CartridgeChannel;
use crate::fabric::registry::{FabricRegistry, FabricRegistryError, FetchCause, RegistryConfig};

/// Distribution channel of this build. `MFR_CARTRIDGE_CHANNEL` is set by the
/// workspace build for every cargo invocation: a release build orchestrates
/// release cartridges only, a nightly build nightly ones — channels never
/// cross.
pub const BUILD_CHANNEL: CartridgeChannel =
    CartridgeChannel::from_build_env(env!("MFR_CARTRIDGE_CHANNEL"));

/// The cartridge registry this build is bound to, baked as the engine's is:
/// `None` for a dev build (dev and bundled cartridges only).
pub const BAKED_REGISTRY_URL: Option<&str> =
    crate::registry_url_from_build_env(option_env!("MFR_CARTRIDGE_REGISTRY_URL"));

/// The anchor of the fabric this build resolves caps, media and aliases
/// against, baked from the selected environment. The cartridge and fabric
/// registries move together (build.rs refuses a product build that bakes one
/// without the other).
pub const BAKED_FABRIC_REGISTRY_URL: Option<&str> = option_env!("CDG_FABRIC_REGISTRY_URL");

/// This release's version (capdag/version.txt), baked by build.rs: what the OS
/// packages and Homebrew's test assert, not the crate's version.
pub const VERSION: &str = env!("CAPDAG_VERSION");

/// Everything a command needs, made once.
pub struct Ctx {
    pub term: Term,
    /// `--offline`: nothing is fetched; what is not cached or bundled is an
    /// error naming it.
    pub offline: bool,
    registry: OnceCell<Arc<FabricRegistry>>,
}

impl Ctx {
    pub fn new(term: Term, offline: bool) -> Self {
        Self {
            term,
            offline,
            registry: OnceCell::new(),
        }
    }

    /// The fabric registry, built on first use.
    pub async fn registry(&self) -> CliResult<Arc<FabricRegistry>> {
        self.registry
            .get_or_try_init(|| async { self.open_registry(false).await })
            .await
            .cloned()
    }

    /// A registry that fetches everything afresh, for `capdag cache refresh`
    /// and `capdag info --no-cache`.
    pub async fn fresh_registry(&self) -> CliResult<Arc<FabricRegistry>> {
        if self.offline {
            return Err(CliError::usage("--offline cannot fetch the fabric afresh")
                .hint("drop --offline, or leave out --no-cache"));
        }
        self.open_registry(true).await
    }

    async fn open_registry(&self, bypass_cache: bool) -> CliResult<Arc<FabricRegistry>> {
        let config = RegistryConfig::default().with_bypass_cache(bypass_cache);
        let anchor = config.anchor.clone();
        let registry = FabricRegistry::with_config(config)
            .await
            .map_err(|e| fabric_unreachable(&anchor, &e, self.offline))?;
        registry.set_offline(self.offline);
        // Planning, listing and extension detection read the cached sets: warm
        // them from the pinned manifest, as the engine does at startup. A
        // no-op once everything is cached.
        if !self.offline {
            registry.prefetch_manifest_caps().await;
            registry.prefetch_manifest_aliases().await;
        }
        Ok(Arc::new(registry))
    }
}

/// A registry lookup, by a name the user gave, that did not give its
/// definition: `Ok` when the registry has nothing of that name and kind (it
/// does not hold it, or the name is not one such a definition can have) — the
/// caller says what that means where it asked — and the registry's own
/// failure, as itself, when it could not be asked: a refused fetch, one that
/// got no answer, a lookup made offline.
pub fn nothing_by_that_name(error: FabricRegistryError) -> CliResult<()> {
    match error {
        FabricRegistryError::NotFound(_)
        | FabricRegistryError::ValidationError(_)
        | FabricRegistryError::ParseError(_) => Ok(()),
        error => Err(registry_failure(error)),
    }
}

/// The registry could not give a definition: said as itself — for a failed
/// fetch, what was fetched, the URL, the status or what happened, and the
/// fabric manifest version.
pub fn registry_failure(error: FabricRegistryError) -> CliError {
    let hint = match &error {
        FabricRegistryError::FetchFailed(failure) => match failure.cause {
            FetchCause::Status(_) => "the registry refused the request: run the command again; if it keeps refusing, report it",
            FetchCause::TimedOut | FetchCause::DeadlineElapsed(_) | FetchCause::Transport(_) => {
                "check the connection, then run the command again"
            }
        },
        FabricRegistryError::NetworkBlocked(_) => "it is not cached: run the command without --offline once",
        _ => "run the command again; if it keeps failing, report it",
    };
    CliError::failed(error.to_string()).hint(hint)
}

/// The fabric's host, for saying where it is.
fn host_of(anchor: &str) -> &str {
    anchor
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/')
}

/// The fabric could not be opened.
fn fabric_unreachable(anchor: &str, error: &FabricRegistryError, offline: bool) -> CliError {
    let host = host_of(anchor);
    match error {
        // The registry refused the manifest, or did not answer: say so as it
        // is, with the URL, the status and the manifest version.
        FabricRegistryError::FetchFailed(failure) => match &failure.cause {
            FetchCause::Status(_) => CliError::failed(failure.to_string())
                .context(format!("capdag reads its capabilities from {anchor}"))
                .hint("the registry refused the request: run the command again; if it keeps refusing, report it"),
            FetchCause::TimedOut | FetchCause::DeadlineElapsed(_) | FetchCause::Transport(_) => {
                CliError::failed(failure.to_string())
                    .context(format!("cannot reach {host}, and the fabric is not cached yet"))
                    .hint(if offline {
                        "connect once so capdag can cache the fabric; after that, --offline runs what is cached and bundled"
                            .to_string()
                    } else {
                        "check the connection; once the fabric is cached, `capdag --offline` works without one"
                            .to_string()
                    })
            }
        },
        FabricRegistryError::Charter(detail)
            if detail.contains("cannot reach") || detail.contains("error sending request") =>
        {
            CliError::failed(format!("cannot reach {host}, and the fabric is not cached yet"))
                .context(format!("capdag reads its capabilities from {anchor}"))
                .hint(if offline {
                    "connect once so capdag can cache the fabric; after that, --offline runs what is cached and bundled"
                        .to_string()
                } else {
                    "check the connection; once the fabric is cached, `capdag --offline` works without one"
                        .to_string()
                })
        }
        FabricRegistryError::Charter(detail) => {
            CliError::failed(format!("the fabric at {host} could not be trusted"))
                .context(detail.clone())
                .hint("if this is a staging fabric, name it with --fabric <url>; otherwise report it")
        }
        other => CliError::failed(format!("the fabric at {host} could not be opened"))
            .context(other.to_string())
            .hint("try `capdag cache clear`, then run the command again"),
    }
}

/// The per-user cartridge install root, `~/.capdag/cartridges`, in the
/// `{registry_slug}/{channel}/{name}/{version}/` tree every host uses.
pub fn user_cartridge_dir() -> CliResult<PathBuf> {
    let home = dirs::home_dir()
        .ok_or_else(|| CliError::failed("there is no home directory to keep cartridges in"))?;
    Ok(home.join(".capdag").join("cartridges"))
}

/// The cartridges shipped beside this binary, in a packaged build; `None` for a
/// bare `cargo run`. The executable is canonicalized so a launcher symlink on
/// PATH (`/usr/bin/capdag`, Homebrew's `bin/capdag`) finds the tree beside the
/// real binary.
pub fn bundled_cartridges_dir() -> Option<PathBuf> {
    std::env::current_exe()
        .ok()
        .and_then(|exe| bundled_cartridges_dir_for_exe(&exe))
}

/// The `bundled-cartridges/` tree beside a launcher path, following symlinks.
pub fn bundled_cartridges_dir_for_exe(exe: &std::path::Path) -> Option<PathBuf> {
    std::fs::canonicalize(exe)
        .ok()
        .and_then(|real| real.parent().map(|dir| dir.join("bundled-cartridges")))
        .filter(|dir| dir.is_dir())
}

/// Point this process at a fabric: `--fabric` first, then the environment,
/// then the baked anchor. The registry reads one value from one place, the
/// environment, so the choice is written there before any registry is made.
pub fn bind_fabric(flag: Option<&str>) -> CliResult<()> {
    if let Some(url) = flag {
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(CliError::usage(format!("--fabric '{url}' is not an http(s) URL"))
                .context("a fabric is an origin")
                .hint("for example: --fabric https://fabric-staging.capdag.com"));
        }
        std::env::set_var("CDG_FABRIC_REGISTRY_URL", url);
    } else if std::env::var_os("CDG_FABRIC_REGISTRY_URL").is_none() {
        if let Some(url) = BAKED_FABRIC_REGISTRY_URL {
            std::env::set_var("CDG_FABRIC_REGISTRY_URL", url);
        }
    }
    Ok(())
}

#[cfg(test)]
mod packaging_tests {
    //! What the OS packages (deb, rpm, Homebrew) depend on.
    use super::bundled_cartridges_dir_for_exe;
    use std::fs;

    /// TEST1902: a launcher symlink — `/usr/bin/capdag` → `/opt/capdag/capdag`,
    /// Homebrew's `bin/capdag` → `libexec/capdag` — finds the real bundle's
    /// `bundled-cartridges/`, not one beside the symlink.
    #[cfg(unix)]
    #[test]
    fn test1902_cartridges_resolve_through_launcher_symlink() {
        use std::os::unix::fs::symlink;
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("opt/capdag");
        fs::create_dir_all(real.join("bundled-cartridges")).unwrap();
        fs::write(real.join("capdag"), b"binary").unwrap();
        let bindir = tmp.path().join("usr/bin");
        fs::create_dir_all(&bindir).unwrap();
        let link = bindir.join("capdag");
        symlink(real.join("capdag"), &link).unwrap();

        let got = bundled_cartridges_dir_for_exe(&link)
            .expect("bundled-cartridges/ must resolve through the launcher symlink");
        assert_eq!(
            fs::canonicalize(&got).unwrap(),
            fs::canonicalize(real.join("bundled-cartridges")).unwrap(),
        );
    }

    /// TEST1903: no `bundled-cartridges/` beside the binary is `None`: a bare
    /// cargo build, whose discovery skips the bundle.
    #[test]
    fn test1903_no_bundled_cartridges_dir_when_absent() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("capdag"), b"binary").unwrap();
        assert!(bundled_cartridges_dir_for_exe(&tmp.path().join("capdag")).is_none());
    }
}
