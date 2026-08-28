//! What a consumer concluded about a cartridge registry, as a closed
//! vocabulary shared by every implementation.
//!
//! A REGISTRY IS NOT A CARTRIDGE. A registry verdict is one fact per registry
//! URL, shared by every cartridge that claims provenance from it; a cartridge
//! attachment error is one fact per cartridge. Squeezing the first through the
//! second is how a signature that failed verification came to be reported as a
//! network outage, with "check your connection" as the remedy.
//!
//! The vocabulary separates the two things a consumer can conclude:
//!
//! * **It could not get an answer.** [`RegistryVerdictState::Offline`],
//!   [`Unreachable`](RegistryVerdictState::Unreachable),
//!   [`HttpError`](RegistryVerdictState::HttpError),
//!   [`Malformed`](RegistryVerdictState::Malformed). We do not know what the
//!   registry says. Retrying, or changing a setting, may change the answer.
//! * **It got an answer and refused it.** [`Unsigned`](RegistryVerdictState::Unsigned),
//!   [`Untrusted`](RegistryVerdictState::Untrusted),
//!   [`Unverifiable`](RegistryVerdictState::Unverifiable). We know what the
//!   registry says and we will not act on it. Retrying changes nothing.
//!
//! Those two groups have opposite remedies, which is the whole reason the
//! distinction exists.
//!
//! [`Untrusted`](RegistryVerdictState::Untrusted) and
//! [`Unverifiable`](RegistryVerdictState::Unverifiable) are likewise kept
//! apart, because one is the registry's problem and the other is ours:
//! *untrusted* means the signature chain was evaluated and rejected — a key
//! nobody vouches for, an expired certificate, a signature that does not
//! verify. *Unverifiable* means the chain could not be evaluated at all: an
//! envelope in a format this build does not implement. Reporting the second as
//! the first tells an operator their registry is compromised when in fact their
//! client is out of date, which is exactly the wrong instruction.

use serde::{Deserialize, Serialize};

/// What a consumer concluded about a registry.
///
/// The wire form is the snake_case name; every mirror encodes the same
/// strings, and an unknown string is a hard decode failure rather than a
/// silently-tolerated future value.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum RegistryVerdictState {
    /// The manifest was fetched, its signature chain verified, and its body
    /// parsed. The only state in which a cartridge from this registry may
    /// attach.
    Verified,
    /// No verdict has been reached yet — the first check has not run, or is in
    /// flight. NOT a failure: a consumer that renders this as an error tells
    /// every operator their registry is broken for the first seconds of every
    /// launch.
    Pending,
    /// The consumer's own network policy forbade the request. Nothing was
    /// attempted. The remedy is a setting, not the network, which is why this
    /// is not [`Unreachable`](Self::Unreachable).
    Offline,
    /// The request could not be completed: DNS failure, connection refused,
    /// timeout, TLS failure. The only state for which "check your connection"
    /// is sound advice.
    Unreachable,
    /// The registry answered, and the answer was an HTTP error. Carries the
    /// status, because 404 (wrong URL, or nothing published) and 5xx (the
    /// registry is broken) are different situations for the operator.
    HttpError,
    /// The registry answered with a body this build cannot read as a manifest:
    /// not JSON, or not the manifest schema.
    Malformed,
    /// The registry served no signature sidecar where one is required. An
    /// unsigned registry is refused rather than trusted.
    Unsigned,
    /// The signature chain was evaluated and REJECTED: a certificate no baked
    /// root vouches for, too few root signatures, an expired or not-yet-valid
    /// certificate, a certificate bound to another environment, or a manifest
    /// signature that does not verify. The registry's problem.
    Untrusted,
    /// The signature chain could NOT be evaluated: an envelope or certificate
    /// in a format this build does not implement, or one malformed beyond
    /// parsing. This build's problem — most often a client older or newer than
    /// the publisher.
    Unverifiable,
    /// This build bakes no trust anchors, so there is no regime to verify
    /// against and the manifest was accepted without proof. A development
    /// build, and only ever that.
    ///
    /// It permits attachment — a dev build has to work — and it is a SEPARATE
    /// state rather than being reported as `Verified`, because "we checked and
    /// it passed" and "we did not check" are different facts and a consumer
    /// that cannot tell them apart will one day ship the second believing the
    /// first.
    Unenforced,
}

impl RegistryVerdictState {
    /// The wire name. Mirrors encode exactly these strings.
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::Pending => "pending",
            Self::Offline => "offline",
            Self::Unreachable => "unreachable",
            Self::HttpError => "http_error",
            Self::Malformed => "malformed",
            Self::Unsigned => "unsigned",
            Self::Untrusted => "untrusted",
            Self::Unverifiable => "unverifiable",
            Self::Unenforced => "unenforced",
        }
    }

    /// Parse a wire name. An unrecognised string is an error, never a default:
    /// guessing here would let a state nobody implemented pass for one that
    /// was.
    pub fn from_wire_name(name: &str) -> Result<Self, RegistryVerdictError> {
        match name {
            "verified" => Ok(Self::Verified),
            "pending" => Ok(Self::Pending),
            "offline" => Ok(Self::Offline),
            "unreachable" => Ok(Self::Unreachable),
            "http_error" => Ok(Self::HttpError),
            "malformed" => Ok(Self::Malformed),
            "unsigned" => Ok(Self::Unsigned),
            "untrusted" => Ok(Self::Untrusted),
            "unverifiable" => Ok(Self::Unverifiable),
            "unenforced" => Ok(Self::Unenforced),
            other => Err(RegistryVerdictError::UnknownState(other.to_string())),
        }
    }

    /// Whether a cartridge claiming provenance from this registry may attach.
    /// True for [`Verified`](Self::Verified) alone: every other state, the
    /// hopeful ones included, means the claim is unconfirmed.
    pub fn permits_attachment(self) -> bool {
        matches!(self, Self::Verified | Self::Unenforced)
    }

    /// Whether this state is a refusal of an answer we DID get, as opposed to
    /// not having got one. A refusal will not change on retry.
    pub fn is_trust_failure(self) -> bool {
        matches!(self, Self::Unsigned | Self::Untrusted | Self::Unverifiable)
    }

    /// Whether trying again, unattended, could plausibly reach a different
    /// verdict. A trust failure never can; neither does a policy that forbids
    /// the request, until the policy changes.
    pub fn is_transient(self) -> bool {
        matches!(
            self,
            Self::Pending | Self::Unreachable | Self::HttpError | Self::Malformed
        )
    }

    /// Every state, in declaration order — for exhaustive tests and for
    /// mirrors' round-trip checks.
    pub const ALL: [Self; 10] = [
        Self::Verified,
        Self::Pending,
        Self::Offline,
        Self::Unreachable,
        Self::HttpError,
        Self::Malformed,
        Self::Unsigned,
        Self::Untrusted,
        Self::Unverifiable,
        Self::Unenforced,
    ];
}

/// WHAT TO DO ABOUT A REGISTRY IN A GIVEN STATE.
///
/// The remedy follows from the state and nothing else. It used to be a sentence
/// glued onto the failure message at the point the record was built — "Check
/// the network connection and try again." — appended whatever the cause, so a
/// signature this build could not read sent operators to their router. A remedy
/// asserted as fact regardless of what failed is worse than none.
///
/// This is the ACTION, not its wording: a CLI prints a line, a desktop client
/// offers a control. Both derive them from here, so neither can invent a remedy
/// the state does not warrant.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum RegistryRemedy {
    /// Nothing to do — the registry verified.
    None,
    /// Wait: a check is in flight and will answer on its own.
    Wait,
    /// The machine cannot reach the registry. Check the connection.
    CheckNetwork,
    /// This build was told not to go out. Change the network policy.
    ChangeNetworkPolicy,
    /// The registry answered badly. It is the registry's side to fix; trying
    /// again later is all a consumer can do.
    RetryLater,
    /// This build cannot read the registry's signature format. Update the
    /// client — the registry is not at fault and the network is not involved.
    UpdateClient,
    /// The registry's answer was rejected. Do not proceed; nothing a consumer
    /// does locally makes this artefact trustworthy.
    DoNotProceed,
}

impl RegistryRemedy {
    /// The wire name.
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Wait => "wait",
            Self::CheckNetwork => "check_network",
            Self::ChangeNetworkPolicy => "change_network_policy",
            Self::RetryLater => "retry_later",
            Self::UpdateClient => "update_client",
            Self::DoNotProceed => "do_not_proceed",
        }
    }
}

impl RegistryVerdictState {
    /// The one thing to do about a registry in this state.
    ///
    /// Exhaustive by construction: adding a state without deciding its remedy
    /// does not compile, which is the point — a state whose remedy nobody
    /// chose would get whatever sentence was nearest.
    pub fn remedy(self) -> RegistryRemedy {
        match self {
            Self::Verified | Self::Unenforced => RegistryRemedy::None,
            Self::Pending => RegistryRemedy::Wait,
            Self::Offline => RegistryRemedy::ChangeNetworkPolicy,
            Self::Unreachable => RegistryRemedy::CheckNetwork,
            Self::HttpError | Self::Malformed => RegistryRemedy::RetryLater,
            Self::Unverifiable => RegistryRemedy::UpdateClient,
            Self::Unsigned | Self::Untrusted => RegistryRemedy::DoNotProceed,
        }
    }
}

/// Why a signature chain failed, as a closed vocabulary.
///
/// Every implementation that verifies a manifest — this crate's
/// [`verify_manifest_envelope`](super::release_cert::verify_manifest_envelope),
/// the desktop clients' in-process verifiers — reports one of these, and
/// [`RegistryVerdictState::for_chain_failure`] turns it into a verdict. That is
/// what keeps "unsupported format" from being classified as a network problem
/// in one implementation and a trust problem in another.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ChainFailureReason {
    /// The envelope is not parseable as a signature envelope at all.
    MalformedEnvelope,
    /// The envelope declares a format discriminator this build does not
    /// implement.
    UnsupportedEnvelopeFormat,
    /// A certificate inside the envelope is not parseable.
    MalformedCertificate,
    /// A certificate declares a format discriminator this build does not
    /// implement.
    UnsupportedCertificateFormat,
    /// The envelope carries no certificates at all.
    EmptyCertificateList,
    /// Fewer distinct baked roots signed the certificate than the threshold
    /// requires.
    InsufficientRootSignatures,
    /// The certificate's validity window has passed.
    ExpiredCertificate,
    /// The certificate is issued in the future.
    NotYetValidCertificate,
    /// The certificate is bound to a different environment than this build.
    EnvironmentMismatch,
    /// The certificate's stated key id disagrees with its own public key.
    KeyIdMismatch,
    /// No chain-valid certificate authorizes the key that signed the manifest.
    NoAuthorizingCertificate,
    /// The manifest signature does not verify over the fetched bytes.
    ManifestSignatureInvalid,
}

impl ChainFailureReason {
    /// The wire name.
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::MalformedEnvelope => "malformed_envelope",
            Self::UnsupportedEnvelopeFormat => "unsupported_envelope_format",
            Self::MalformedCertificate => "malformed_certificate",
            Self::UnsupportedCertificateFormat => "unsupported_certificate_format",
            Self::EmptyCertificateList => "empty_certificate_list",
            Self::InsufficientRootSignatures => "insufficient_root_signatures",
            Self::ExpiredCertificate => "expired_certificate",
            Self::NotYetValidCertificate => "not_yet_valid_certificate",
            Self::EnvironmentMismatch => "environment_mismatch",
            Self::KeyIdMismatch => "key_id_mismatch",
            Self::NoAuthorizingCertificate => "no_authorizing_certificate",
            Self::ManifestSignatureInvalid => "manifest_signature_invalid",
        }
    }

    /// Parse a wire name; unknown is an error, never a default.
    pub fn from_wire_name(name: &str) -> Result<Self, RegistryVerdictError> {
        match name {
            "malformed_envelope" => Ok(Self::MalformedEnvelope),
            "unsupported_envelope_format" => Ok(Self::UnsupportedEnvelopeFormat),
            "malformed_certificate" => Ok(Self::MalformedCertificate),
            "unsupported_certificate_format" => Ok(Self::UnsupportedCertificateFormat),
            "empty_certificate_list" => Ok(Self::EmptyCertificateList),
            "insufficient_root_signatures" => Ok(Self::InsufficientRootSignatures),
            "expired_certificate" => Ok(Self::ExpiredCertificate),
            "not_yet_valid_certificate" => Ok(Self::NotYetValidCertificate),
            "environment_mismatch" => Ok(Self::EnvironmentMismatch),
            "key_id_mismatch" => Ok(Self::KeyIdMismatch),
            "no_authorizing_certificate" => Ok(Self::NoAuthorizingCertificate),
            "manifest_signature_invalid" => Ok(Self::ManifestSignatureInvalid),
            other => Err(RegistryVerdictError::UnknownChainFailureReason(
                other.to_string(),
            )),
        }
    }

    /// Every reason, in declaration order.
    pub const ALL: [Self; 12] = [
        Self::MalformedEnvelope,
        Self::UnsupportedEnvelopeFormat,
        Self::MalformedCertificate,
        Self::UnsupportedCertificateFormat,
        Self::EmptyCertificateList,
        Self::InsufficientRootSignatures,
        Self::ExpiredCertificate,
        Self::NotYetValidCertificate,
        Self::EnvironmentMismatch,
        Self::KeyIdMismatch,
        Self::NoAuthorizingCertificate,
        Self::ManifestSignatureInvalid,
    ];
}

impl RegistryVerdictState {
    /// The verdict a chain failure produces.
    ///
    /// COULD THE CHAIN BE EVALUATED AT ALL? A format this build does not
    /// implement, or bytes it cannot parse, means no judgement was reached —
    /// [`Unverifiable`](Self::Unverifiable), remedied by updating the client.
    /// Everything else means the chain WAS judged and found wanting —
    /// [`Untrusted`](Self::Untrusted), remedied by not proceeding.
    pub fn for_chain_failure(reason: ChainFailureReason) -> Self {
        match reason {
            ChainFailureReason::MalformedEnvelope
            | ChainFailureReason::UnsupportedEnvelopeFormat
            | ChainFailureReason::MalformedCertificate
            | ChainFailureReason::UnsupportedCertificateFormat
            | ChainFailureReason::EmptyCertificateList => Self::Unverifiable,
            ChainFailureReason::InsufficientRootSignatures
            | ChainFailureReason::ExpiredCertificate
            | ChainFailureReason::NotYetValidCertificate
            | ChainFailureReason::EnvironmentMismatch
            | ChainFailureReason::KeyIdMismatch
            | ChainFailureReason::NoAuthorizingCertificate
            | ChainFailureReason::ManifestSignatureInvalid => Self::Untrusted,
        }
    }
}

/// A verdict that does not describe a possible situation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegistryVerdictError {
    #[error("unknown registry verdict state '{0}'")]
    UnknownState(String),
    #[error("unknown chain failure reason '{0}'")]
    UnknownChainFailureReason(String),
    #[error("a registry verdict must name the registry it is about")]
    MissingRegistryUrl,
    #[error("a '{0}' verdict must carry the detail that explains it")]
    MissingDetail(&'static str),
    #[error("a 'verified' verdict states no failure, so it carries no detail (got {0:?})")]
    VerifiedWithDetail(String),
    #[error("a 'pending' verdict states no failure, so it carries no detail (got {0:?})")]
    PendingWithDetail(String),
    #[error("only an 'http_error' verdict carries an HTTP status (got one on '{0}')")]
    UnexpectedHttpStatus(&'static str),
    #[error("an 'http_error' verdict must carry the status the registry answered with")]
    MissingHttpStatus,
    #[error("only a trust failure carries a chain failure reason (got one on '{0}')")]
    UnexpectedChainFailure(&'static str),
    #[error("an '{0}' verdict must carry the chain failure reason that produced it")]
    MissingChainFailure(&'static str),
}

/// What a consumer concluded about one registry, and why.
///
/// Illegal combinations are unrepresentable: the constructors are the only way
/// to build one, each takes exactly what its state requires, and
/// [`RegistryVerdict::from_wire`] re-checks every invariant on the way in. A
/// verdict that says "http_error" without a status, or "verified" with a
/// failure detail, is a bug in the producer and is refused at the boundary
/// rather than rendered as a contradiction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistryVerdict {
    /// The registry this verdict is about — the verbatim URL a cartridge
    /// declares, which is what consumers join on.
    pub registry_url: String,
    pub state: RegistryVerdictState,
    /// One operator-visible line saying what happened. Empty exactly when the
    /// state states no failure ([`Verified`](RegistryVerdictState::Verified),
    /// [`Pending`](RegistryVerdictState::Pending)).
    pub detail: String,
    /// The HTTP status the registry answered with. Present exactly on
    /// [`HttpError`](RegistryVerdictState::HttpError).
    pub http_status: Option<u16>,
    /// Which chain check failed. Present exactly on
    /// [`Untrusted`](RegistryVerdictState::Untrusted) and
    /// [`Unverifiable`](RegistryVerdictState::Unverifiable) — never on
    /// [`Unsigned`](RegistryVerdictState::Unsigned), where there was no chain
    /// to check.
    pub chain_failure: Option<ChainFailureReason>,
    /// When this verdict was reached, unix seconds.
    pub checked_at_unix_seconds: i64,
}

impl RegistryVerdict {
    /// WHETHER TWO VERDICTS SAY THE SAME THING ABOUT THE REGISTRY.
    ///
    /// Not `==`. Equality includes `checked_at_unix_seconds`, which is
    /// provenance about the CHECK and not about the registry — so a consumer
    /// asking "did this change?" with `==` is told yes on every re-check,
    /// forever.
    ///
    /// Both desktop clients asked exactly that to decide whether to re-run
    /// cartridge discovery, and both wrote a comment saying they were doing it
    /// to avoid a feedback loop. The comparison they used could not: discovery
    /// finished, the verifier re-checked, the identical answer came back with a
    /// newer timestamp, "the verdicts changed" re-ran discovery, and the engine
    /// never reached ready. This is the comparison that question wants.
    pub fn states_the_same_as(&self, other: &Self) -> bool {
        self.registry_url == other.registry_url
            && self.state == other.state
            && self.detail == other.detail
            && self.http_status == other.http_status
            && self.chain_failure == other.chain_failure
    }

    /// The registry answered, verified and parsed.
    pub fn verified(registry_url: impl Into<String>, checked_at_unix_seconds: i64) -> Self {
        Self {
            registry_url: registry_url.into(),
            state: RegistryVerdictState::Verified,
            detail: String::new(),
            http_status: None,
            chain_failure: None,
            checked_at_unix_seconds,
        }
    }

    /// This build bakes no trust anchors: the manifest was accepted without
    /// proof, and says so rather than claiming it verified.
    pub fn unenforced(registry_url: impl Into<String>, checked_at_unix_seconds: i64) -> Self {
        Self {
            registry_url: registry_url.into(),
            state: RegistryVerdictState::Unenforced,
            detail: String::new(),
            http_status: None,
            chain_failure: None,
            checked_at_unix_seconds,
        }
    }

    /// No verdict yet. Carries no time, because nothing has been checked.
    pub fn pending(registry_url: impl Into<String>) -> Self {
        Self {
            registry_url: registry_url.into(),
            state: RegistryVerdictState::Pending,
            detail: String::new(),
            http_status: None,
            chain_failure: None,
            checked_at_unix_seconds: 0,
        }
    }

    /// A state that carries only a detail line: `Offline`, `Unreachable`,
    /// `Malformed`, `Unsigned`. The other states have their own constructors
    /// because they require more, and this refuses them rather than letting a
    /// caller build a verdict missing what it needs.
    pub fn stated(
        registry_url: impl Into<String>,
        state: RegistryVerdictState,
        detail: impl Into<String>,
        checked_at_unix_seconds: i64,
    ) -> Result<Self, RegistryVerdictError> {
        match state {
            RegistryVerdictState::Offline
            | RegistryVerdictState::Unreachable
            | RegistryVerdictState::Malformed
            | RegistryVerdictState::Unsigned => {}
            RegistryVerdictState::Verified
            | RegistryVerdictState::Pending
            | RegistryVerdictState::Unenforced => {
                return Err(RegistryVerdictError::VerifiedWithDetail(detail.into()))
            }
            RegistryVerdictState::HttpError => return Err(RegistryVerdictError::MissingHttpStatus),
            RegistryVerdictState::Untrusted | RegistryVerdictState::Unverifiable => {
                return Err(RegistryVerdictError::MissingChainFailure(state.wire_name()))
            }
        }
        let verdict = Self {
            registry_url: registry_url.into(),
            state,
            detail: detail.into(),
            http_status: None,
            chain_failure: None,
            checked_at_unix_seconds,
        };
        verdict.validate()?;
        Ok(verdict)
    }

    /// The registry answered with an HTTP error.
    pub fn http_error(
        registry_url: impl Into<String>,
        status: u16,
        detail: impl Into<String>,
        checked_at_unix_seconds: i64,
    ) -> Result<Self, RegistryVerdictError> {
        let verdict = Self {
            registry_url: registry_url.into(),
            state: RegistryVerdictState::HttpError,
            detail: detail.into(),
            http_status: Some(status),
            chain_failure: None,
            checked_at_unix_seconds,
        };
        verdict.validate()?;
        Ok(verdict)
    }

    /// A signature chain that failed. The state follows from the reason —
    /// [`RegistryVerdictState::for_chain_failure`] — so a caller cannot file an
    /// unreadable format as a rejected key or the other way round.
    pub fn chain_failed(
        registry_url: impl Into<String>,
        reason: ChainFailureReason,
        detail: impl Into<String>,
        checked_at_unix_seconds: i64,
    ) -> Result<Self, RegistryVerdictError> {
        let verdict = Self {
            registry_url: registry_url.into(),
            state: RegistryVerdictState::for_chain_failure(reason),
            detail: detail.into(),
            http_status: None,
            chain_failure: Some(reason),
            checked_at_unix_seconds,
        };
        verdict.validate()?;
        Ok(verdict)
    }

    /// Every invariant this type promises, checked. Used by the constructors
    /// and by [`from_wire`](Self::from_wire); a verdict that fails this has no
    /// meaning and must not travel.
    pub fn validate(&self) -> Result<(), RegistryVerdictError> {
        if self.registry_url.is_empty() {
            return Err(RegistryVerdictError::MissingRegistryUrl);
        }
        match self.state {
            RegistryVerdictState::Verified | RegistryVerdictState::Unenforced => {
                if !self.detail.is_empty() {
                    return Err(RegistryVerdictError::VerifiedWithDetail(self.detail.clone()));
                }
            }
            RegistryVerdictState::Pending => {
                if !self.detail.is_empty() {
                    return Err(RegistryVerdictError::PendingWithDetail(self.detail.clone()));
                }
            }
            other => {
                if self.detail.is_empty() {
                    return Err(RegistryVerdictError::MissingDetail(other.wire_name()));
                }
            }
        }
        match (self.state, self.http_status) {
            (RegistryVerdictState::HttpError, None) => {
                return Err(RegistryVerdictError::MissingHttpStatus)
            }
            (RegistryVerdictState::HttpError, Some(_)) => {}
            (state, Some(_)) => {
                return Err(RegistryVerdictError::UnexpectedHttpStatus(state.wire_name()))
            }
            (_, None) => {}
        }
        match (self.state, self.chain_failure) {
            (RegistryVerdictState::Untrusted, None) | (RegistryVerdictState::Unverifiable, None) => {
                return Err(RegistryVerdictError::MissingChainFailure(
                    self.state.wire_name(),
                ))
            }
            (RegistryVerdictState::Untrusted, Some(reason))
            | (RegistryVerdictState::Unverifiable, Some(reason)) => {
                // The reason must be one that produces THIS state, or the
                // verdict contradicts itself.
                if RegistryVerdictState::for_chain_failure(reason) != self.state {
                    return Err(RegistryVerdictError::UnexpectedChainFailure(
                        self.state.wire_name(),
                    ));
                }
            }
            (state, Some(_)) => {
                return Err(RegistryVerdictError::UnexpectedChainFailure(
                    state.wire_name(),
                ))
            }
            (_, None) => {}
        }
        Ok(())
    }

    /// Decode from the flat wire form, checking every invariant. The mirrors
    /// encode and decode exactly these keys.
    pub fn from_wire(
        registry_url: &str,
        state: &str,
        detail: &str,
        http_status: Option<u16>,
        chain_failure: Option<&str>,
        checked_at_unix_seconds: i64,
    ) -> Result<Self, RegistryVerdictError> {
        let verdict = Self {
            registry_url: registry_url.to_string(),
            state: RegistryVerdictState::from_wire_name(state)?,
            detail: detail.to_string(),
            http_status,
            chain_failure: chain_failure
                .map(ChainFailureReason::from_wire_name)
                .transpose()?,
            checked_at_unix_seconds,
        };
        verdict.validate()?;
        Ok(verdict)
    }

    /// Whether a cartridge from this registry may attach.
    pub fn permits_attachment(&self) -> bool {
        self.state.permits_attachment()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TEST8150: the wire vocabulary is closed and round-trips. A mirror that
    /// renames a state silently stops understanding its own producers — which
    /// is the failure this whole vocabulary exists to make impossible.
    #[test]
    fn states_round_trip_through_their_wire_names() {
        for state in RegistryVerdictState::ALL {
            assert_eq!(
                RegistryVerdictState::from_wire_name(state.wire_name()).expect("round trip"),
                state,
                "state {state:?} must survive its own wire name"
            );
            let json = serde_json::to_string(&state).expect("serialize");
            assert_eq!(
                json.trim_matches('"'),
                state.wire_name(),
                "serde and wire_name must agree for {state:?}"
            );
        }
        assert!(matches!(
            RegistryVerdictState::from_wire_name("network_error"),
            Err(RegistryVerdictError::UnknownState(_))
        ));
    }

    #[test]
    fn chain_failure_reasons_round_trip() {
        for reason in ChainFailureReason::ALL {
            assert_eq!(
                ChainFailureReason::from_wire_name(reason.wire_name()).expect("round trip"),
                reason
            );
            let json = serde_json::to_string(&reason).expect("serialize");
            assert_eq!(json.trim_matches('"'), reason.wire_name());
        }
        assert!(matches!(
            ChainFailureReason::from_wire_name("bad_signature"),
            Err(RegistryVerdictError::UnknownChainFailureReason(_))
        ));
    }

    /// TEST8151: the distinction the vocabulary exists for. A format this build
    /// cannot read is OUR limitation and must never be reported as the
    /// registry being untrustworthy.
    #[test]
    fn an_unreadable_format_is_unverifiable_and_a_rejected_key_is_untrusted() {
        assert_eq!(
            RegistryVerdictState::for_chain_failure(
                ChainFailureReason::UnsupportedEnvelopeFormat
            ),
            RegistryVerdictState::Unverifiable
        );
        assert_eq!(
            RegistryVerdictState::for_chain_failure(
                ChainFailureReason::UnsupportedCertificateFormat
            ),
            RegistryVerdictState::Unverifiable
        );
        assert_eq!(
            RegistryVerdictState::for_chain_failure(ChainFailureReason::MalformedEnvelope),
            RegistryVerdictState::Unverifiable
        );
        for judged in [
            ChainFailureReason::InsufficientRootSignatures,
            ChainFailureReason::ExpiredCertificate,
            ChainFailureReason::NotYetValidCertificate,
            ChainFailureReason::EnvironmentMismatch,
            ChainFailureReason::KeyIdMismatch,
            ChainFailureReason::NoAuthorizingCertificate,
            ChainFailureReason::ManifestSignatureInvalid,
        ] {
            assert_eq!(
                RegistryVerdictState::for_chain_failure(judged),
                RegistryVerdictState::Untrusted,
                "{judged:?} is a judgement that was reached, not one that could not be"
            );
        }
    }

    /// TEST8152: only a verified registry lets a cartridge attach — the hopeful
    /// states included. `pending` must not read as permission.
    #[test]
    fn only_verified_permits_attachment() {
        for state in RegistryVerdictState::ALL {
            assert_eq!(
                state.permits_attachment(),
                state == RegistryVerdictState::Verified
                    || state == RegistryVerdictState::Unenforced,
                "{state:?}"
            );
        }
        // A DEV BUILD HAS TO WORK, and it says which of the two it is: "we
        // checked and it passed" and "we did not check" are different facts,
        // and a consumer that cannot tell them apart will one day ship the
        // second believing the first.
        assert!(RegistryVerdictState::Unenforced.permits_attachment());
        assert_ne!(RegistryVerdictState::Unenforced, RegistryVerdictState::Verified);
        assert!(!RegistryVerdictState::Unenforced.is_trust_failure());
        assert!(!RegistryVerdictState::Unenforced.is_transient());
        assert_eq!(RegistryVerdictState::Unenforced.remedy(), RegistryRemedy::None);
    }

    /// TEST8153: a trust failure never resolves itself, so nothing may present
    /// it as worth retrying.
    #[test]
    fn trust_failures_are_never_transient() {
        for state in RegistryVerdictState::ALL {
            assert!(
                !(state.is_trust_failure() && state.is_transient()),
                "{state:?} cannot be both a refusal and something a retry could fix"
            );
        }
        assert!(RegistryVerdictState::Unverifiable.is_trust_failure());
        assert!(RegistryVerdictState::Untrusted.is_trust_failure());
        assert!(RegistryVerdictState::Unsigned.is_trust_failure());
        assert!(RegistryVerdictState::Unreachable.is_transient());
        assert!(RegistryVerdictState::Pending.is_transient());
        // Policy is not transient: it stays until an operator changes it.
        assert!(!RegistryVerdictState::Offline.is_transient());
        assert!(!RegistryVerdictState::Offline.is_trust_failure());
    }

    /// TEST8159: the remedy follows from the state, and "check the network" is
    /// reachable from exactly one state.
    ///
    /// The sentence "Check the network connection and try again." used to be
    /// appended to every held-cartridge message whatever the cause, which is
    /// how a signature format this build could not read sent operators to
    /// their router.
    #[test]
    fn test8159_the_remedy_follows_from_the_state() {
        let network: Vec<RegistryVerdictState> = RegistryVerdictState::ALL
            .into_iter()
            .filter(|state| state.remedy() == RegistryRemedy::CheckNetwork)
            .collect();
        assert_eq!(
            network,
            vec![RegistryVerdictState::Unreachable],
            "only a registry we could not reach is a network problem"
        );
        // A trust failure is never something a retry or a router fixes.
        for state in RegistryVerdictState::ALL {
            if !state.is_trust_failure() {
                continue;
            }
            let remedy = state.remedy();
            assert!(
                remedy == RegistryRemedy::DoNotProceed || remedy == RegistryRemedy::UpdateClient,
                "{state:?} is a refusal; its remedy must not be a retry ({remedy:?})"
            );
        }
        // The one that was misclassified: our limitation, so update the client
        // — never distrust the registry, never touch the network.
        assert_eq!(
            RegistryVerdictState::Unverifiable.remedy(),
            RegistryRemedy::UpdateClient
        );
        assert_eq!(
            RegistryVerdictState::Untrusted.remedy(),
            RegistryRemedy::DoNotProceed
        );
        assert_eq!(RegistryVerdictState::Verified.remedy(), RegistryRemedy::None);
        assert_eq!(RegistryVerdictState::Pending.remedy(), RegistryRemedy::Wait);
        // Policy is the operator's setting, not their router.
        assert_eq!(
            RegistryVerdictState::Offline.remedy(),
            RegistryRemedy::ChangeNetworkPolicy
        );
    }

    /// TEST8154: illegal states are unrepresentable — every contradiction is
    /// refused at construction and again at the wire boundary.
    #[test]
    fn contradictory_verdicts_are_refused() {
        let now = 1_700_000_000;
        // A failure with nothing said about it.
        assert!(matches!(
            RegistryVerdict::stated("https://r.example", RegistryVerdictState::Unreachable, "", now),
            Err(RegistryVerdictError::MissingDetail(_))
        ));
        // Success carrying a failure detail.
        assert!(matches!(
            RegistryVerdict::stated(
                "https://r.example",
                RegistryVerdictState::Verified,
                "all good",
                now
            ),
            Err(RegistryVerdictError::VerifiedWithDetail(_))
        ));
        // An HTTP error with no status, built through the general constructor.
        assert!(matches!(
            RegistryVerdict::stated(
                "https://r.example",
                RegistryVerdictState::HttpError,
                "500",
                now
            ),
            Err(RegistryVerdictError::MissingHttpStatus)
        ));
        // A trust failure with no reason.
        assert!(matches!(
            RegistryVerdict::stated(
                "https://r.example",
                RegistryVerdictState::Untrusted,
                "nope",
                now
            ),
            Err(RegistryVerdictError::MissingChainFailure(_))
        ));
        // A verdict about no registry at all.
        assert!(matches!(
            RegistryVerdict::stated("", RegistryVerdictState::Unreachable, "timeout", now),
            Err(RegistryVerdictError::MissingRegistryUrl)
        ));
        // A status on a state that cannot have answered.
        let mut smuggled = RegistryVerdict::pending("https://r.example");
        smuggled.http_status = Some(404);
        assert!(matches!(
            smuggled.validate(),
            Err(RegistryVerdictError::UnexpectedHttpStatus(_))
        ));
        // A reason that contradicts the state it is filed under.
        let mut contradiction = RegistryVerdict::chain_failed(
            "https://r.example",
            ChainFailureReason::ExpiredCertificate,
            "expired",
            now,
        )
        .expect("valid");
        contradiction.chain_failure = Some(ChainFailureReason::UnsupportedEnvelopeFormat);
        assert!(matches!(
            contradiction.validate(),
            Err(RegistryVerdictError::UnexpectedChainFailure(_))
        ));
    }

    /// TEST8162: WHEN IS A VERDICT NEWS? Both desktop clients re-verify their
    /// registries after every discovery round and re-run discovery when the
    /// verdicts "changed". Change had to mean "the registry said something
    /// different", and `==` cannot mean that: it includes the moment of the
    /// check, so the same answer taken a second later is a different value.
    /// That is the loop that left an engine discovering cartridges forever.
    #[test]
    fn test8162_a_verdict_says_the_same_thing_at_a_different_time() {
        let earlier = RegistryVerdict::verified("https://r.example", 1_756_000_000);
        let later = RegistryVerdict::verified("https://r.example", 1_756_000_931);
        assert_ne!(earlier, later, "they are not the same VALUE — one is a later check");
        assert!(
            earlier.states_the_same_as(&later),
            "but they say the same thing about the registry, which is the question a consumer asks"
        );

        // Everything the registry actually said is news when it differs.
        let differing = [
            RegistryVerdict::stated("https://r.example", RegistryVerdictState::Unreachable, "connection timed out", 1_756_000_000).unwrap(),
            RegistryVerdict::http_error("https://r.example", 503, "the registry answered HTTP 503", 1_756_000_000).unwrap(),
            RegistryVerdict::chain_failed("https://r.example", ChainFailureReason::ManifestSignatureInvalid, "signature does not verify", 1_756_000_000).unwrap(),
            RegistryVerdict::verified("https://other.example/manifest", 1_756_000_000),
        ];
        for verdict in &differing {
            assert!(
                !earlier.states_the_same_as(verdict),
                "{} is a different statement about the registry",
                verdict.state.wire_name()
            );
        }

        // Two http errors with different statuses are different statements: 404
        // and 503 are different situations with different remedies.
        let not_found = RegistryVerdict::http_error("https://r.example", 404, "the registry answered HTTP 404", 1_756_000_000).unwrap();
        let unavailable = RegistryVerdict::http_error("https://r.example", 503, "the registry answered HTTP 503", 1_756_000_000).unwrap();
        assert!(!not_found.states_the_same_as(&unavailable));
    }

    /// TEST8155: the wire form survives a round trip with its invariants, and a
    /// producer that omits what a state requires is refused on the way in.
    #[test]
    fn wire_round_trip_and_refusals() {
        let now = 1_700_000_000;
        let verdict = RegistryVerdict::chain_failed(
            "https://r.example/v1/manifest",
            ChainFailureReason::UnsupportedEnvelopeFormat,
            "envelope format 'x/1' is not implemented by this build",
            now,
        )
        .expect("valid");
        let decoded = RegistryVerdict::from_wire(
            &verdict.registry_url,
            verdict.state.wire_name(),
            &verdict.detail,
            verdict.http_status,
            verdict.chain_failure.map(ChainFailureReason::wire_name),
            verdict.checked_at_unix_seconds,
        )
        .expect("round trip");
        assert_eq!(decoded, verdict);
        assert_eq!(decoded.state, RegistryVerdictState::Unverifiable);

        let http = RegistryVerdict::http_error(
            "https://r.example/v1/manifest",
            503,
            "registry answered 503",
            now,
        )
        .expect("valid");
        assert_eq!(http.http_status, Some(503));
        assert!(!http.permits_attachment());

        assert!(matches!(
            RegistryVerdict::from_wire(
                "https://r.example",
                "http_error",
                "answered badly",
                None,
                None,
                now
            ),
            Err(RegistryVerdictError::MissingHttpStatus)
        ));
        assert!(matches!(
            RegistryVerdict::from_wire("https://r.example", "flaky", "hm", None, None, now),
            Err(RegistryVerdictError::UnknownState(_))
        ));
    }
}
