//! What every command works with: the host it runs on, the fabric it resolves
//! against, and what is attached.

use std::sync::Arc;

use tokio::sync::OnceCell;

use super::error::{CliError, CliResult};
use super::host::{Host, HostError};
use super::term::Term;
use crate::fabric::registry::{FabricRegistry, FabricRegistryError, FetchCause};

/// This release's version (capdag/version.txt), baked by build.rs: what the OS
/// packages and Homebrew's test assert, not the crate's version.
pub const VERSION: &str = env!("CAPDAG_VERSION");

/// Everything a command needs, made once.
pub struct Ctx {
    pub term: Term,
    /// `--offline`: nothing is fetched; what is not cached or bundled is an
    /// error naming it.
    pub offline: bool,
    /// The engine the command runs on.
    pub host: Arc<dyn Host>,
    registry: OnceCell<Arc<FabricRegistry>>,
}

impl Ctx {
    pub fn new(term: Term, offline: bool, host: Arc<dyn Host>) -> Self {
        Self {
            term,
            offline,
            host,
            registry: OnceCell::new(),
        }
    }

    /// The fabric registry, opened on first use.
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

    async fn open_registry(&self, afresh: bool) -> CliResult<Arc<FabricRegistry>> {
        self.host
            .open_fabric(self.offline, afresh)
            .await
            .map_err(|e| fabric_unreachable(&self.host.fabric_anchor(), &e, self.offline))
    }

    /// Every cap of the fabric (`FabricRegistry::manifest_caps`): those not
    /// cached are fetched; one that cannot be had is an error naming it.
    pub async fn fabric_caps(&self, registry: &FabricRegistry) -> CliResult<Vec<crate::Cap>> {
        registry.manifest_caps().await.map_err(|e| self.caps_unreadable(e))
    }

    /// The fabric's caps could not all be read: what went wrong, and what to do.
    pub fn caps_unreadable(&self, error: FabricRegistryError) -> CliError {
        CliError::failed("the fabric's capabilities could not be read")
            .context(error.to_string())
            .hint(if self.offline {
                "they are not all cached: run the command without --offline once"
            } else {
                "check the connection, then run the command again; if it keeps failing, report it"
            })
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

/// What the host could not do, said as what the command was doing (`what`)
/// and why.
pub fn host_failure(what: impl Into<String>, error: HostError) -> CliError {
    match error {
        HostError::NoHome => CliError::failed(error.to_string()),
        HostError::File { path, done, detail } => {
            CliError::failed(format!("{} could not be {done}", path.display())).context(detail)
        }
        HostError::Engine(detail) => CliError::failed(what).context(detail),
    }
}

/// Point this process at a fabric: `--fabric` first, then the environment,
/// then the one the host is bound to. A fabric is an origin.
pub fn bind_fabric(host: &dyn Host, flag: Option<&str>) -> CliResult<()> {
    if let Some(url) = flag {
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(CliError::usage(format!("--fabric '{url}' is not an http(s) URL"))
                .context("a fabric is an origin")
                .hint("for example: --fabric https://fabric-staging.capdag.com"));
        }
    }
    host.bind_fabric(flag);
    Ok(())
}
