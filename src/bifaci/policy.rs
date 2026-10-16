//! What the host allows the cartridges it serves to do (12.2 §Policy).
//!
//! The engine holds the live policy and gives it to the RelaySwitch; a Policy
//! frame carries it through every relay to each CartridgeHostRuntime, which
//! keeps the latest and sends it to each of its cartridges after the
//! handshake and on every change. A cartridge runtime keeps the latest in a
//! [`LivePolicy`], and a handler reads it through [`NetworkAccess`] before each
//! call it makes to the network — never a copy taken when the request was
//! dispatched, so a change to `offline` stops a running download or stream at
//! its next call.
//!
//! Today a policy says one thing: how far a cartridge may reach on the
//! network.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use ops_rs::failure::AttributionClass;

/// The wire key a Policy frame's `meta` names the network policy by.
pub const POLICY_META_NETWORK: &str = "network";

/// The code a call the policy does not allow fails with.
pub const NETWORK_POLICY_CODE: &str = "NETWORK_POLICY";

/// How far a cartridge may reach on the network. The operator's setting, in
/// the same three words both apps and the engine use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NetworkPolicy {
    /// Nothing beyond this machine.
    Offline,
    /// Third-party services and MachineFabric's own, never analytics.
    NoTelemetry,
    /// The same, and analytics.
    Full,
}

impl NetworkPolicy {
    /// The policy's wire word.
    pub fn as_str(self) -> &'static str {
        match self {
            NetworkPolicy::Offline => "offline",
            NetworkPolicy::NoTelemetry => "no_telemetry",
            NetworkPolicy::Full => "full",
        }
    }

    /// The policy a wire word names; None for any other word. A word nobody
    /// defined is never read as one of these.
    pub fn from_wire(word: &str) -> Option<Self> {
        match word {
            "offline" => Some(NetworkPolicy::Offline),
            "no_telemetry" => Some(NetworkPolicy::NoTelemetry),
            "full" => Some(NetworkPolicy::Full),
            _ => None,
        }
    }

    /// Whether this policy lets a call of this reach be made.
    pub fn allows(self, reach: Reach) -> bool {
        match (self, reach) {
            (NetworkPolicy::Offline, _) => false,
            (NetworkPolicy::NoTelemetry, Reach::Telemetry) => false,
            (NetworkPolicy::NoTelemetry, Reach::ThirdParty | Reach::OurServices) => true,
            (NetworkPolicy::Full, _) => true,
        }
    }

    fn code(self) -> u8 {
        match self {
            NetworkPolicy::Offline => 1,
            NetworkPolicy::NoTelemetry => 2,
            NetworkPolicy::Full => 3,
        }
    }

    fn of_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(NetworkPolicy::Offline),
            2 => Some(NetworkPolicy::NoTelemetry),
            3 => Some(NetworkPolicy::Full),
            _ => None,
        }
    }
}

impl std::fmt::Display for NetworkPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a call to the network reaches, which is what the policy decides on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Reach {
    /// A service MachineFabric does not run: a model hub, a data source, a
    /// web page.
    ThirdParty,
    /// A service MachineFabric runs for the product to work: registries,
    /// updates.
    OurServices,
    /// Analytics and crash reports.
    Telemetry,
}

impl Reach {
    fn describe(self) -> &'static str {
        match self {
            Reach::ThirdParty => "an outside service",
            Reach::OurServices => "MachineFabric's services",
            Reach::Telemetry => "analytics",
        }
    }
}

/// Everything a Policy frame says.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HostPolicy {
    pub network: NetworkPolicy,
}

impl HostPolicy {
    pub fn new(network: NetworkPolicy) -> Self {
        Self { network }
    }
}

/// A call that was not made.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NetworkRefused {
    /// The operator's policy does not allow it.
    #[error("the network policy is '{policy}', which does not allow reaching {}", .reach.describe())]
    Policy { policy: NetworkPolicy, reach: Reach },
    /// No policy has been given. A runtime dispatches no routed request
    /// before its first policy, so a handler meeting this is running outside
    /// that guarantee: a defect, not the operator's decision.
    #[error("no network policy has been given to this cartridge, so nothing may reach {}", .reach.describe())]
    NoPolicy { reach: Reach },
}

impl NetworkRefused {
    /// The code the handler ends with.
    pub fn code(&self) -> &'static str {
        match self {
            NetworkRefused::Policy { .. } => NETWORK_POLICY_CODE,
            NetworkRefused::NoPolicy { .. } => "NETWORK_POLICY_UNSET",
        }
    }

    /// The operator's own setting refusing the work is `user`: never
    /// retried, never tolerated (17.2). A missing policy is `internal`.
    pub fn class(&self) -> AttributionClass {
        match self {
            NetworkRefused::Policy { .. } => AttributionClass::User,
            NetworkRefused::NoPolicy { .. } => AttributionClass::Internal,
        }
    }
}

impl From<NetworkRefused> for ops_rs::OpError {
    fn from(refused: NetworkRefused) -> Self {
        ops_rs::OpError::Classified {
            code: refused.code().to_string(),
            class: refused.class(),
            message: refused.to_string(),
            arg_urn: None,
        }
    }
}

/// The latest policy a cartridge runtime was given, shared with every
/// handler it runs. Empty until the first Policy frame arrives; a runtime
/// dispatches no routed request while it is empty.
#[derive(Debug, Default)]
pub struct LivePolicy {
    network: AtomicU8,
}

impl LivePolicy {
    /// A store that has not been given a policy yet.
    pub fn unset() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// A store holding `policy` from the start: a cartridge run from a
    /// command line (under `full`), or a host built with its policy.
    pub fn with(policy: HostPolicy) -> Arc<Self> {
        let live = Self::default();
        live.set(policy);
        Arc::new(live)
    }

    /// Replace the policy. Every reader sees the new one at its next read.
    pub fn set(&self, policy: HostPolicy) {
        self.network.store(policy.network.code(), Ordering::SeqCst);
    }

    /// The current policy, or None before the first one was given.
    pub fn current(&self) -> Option<HostPolicy> {
        NetworkPolicy::of_code(self.network.load(Ordering::SeqCst)).map(HostPolicy::new)
    }
}

/// A handler's view of the live network policy: cheap to clone, safe to hand
/// to the code that opens connections, which asks it before every call.
#[derive(Debug, Clone)]
pub struct NetworkAccess {
    live: Arc<LivePolicy>,
}

impl NetworkAccess {
    pub fn new(live: Arc<LivePolicy>) -> Self {
        Self { live }
    }

    /// Whether a call of this reach may be made now, under the policy in
    /// force at this moment.
    pub fn allow(&self, reach: Reach) -> Result<(), NetworkRefused> {
        match self.live.current() {
            None => Err(NetworkRefused::NoPolicy { reach }),
            Some(policy) if policy.network.allows(reach) => Ok(()),
            Some(policy) => Err(NetworkRefused::Policy { policy: policy.network, reach }),
        }
    }

    /// The policy in force now, None before the first one was given.
    pub fn policy(&self) -> Option<NetworkPolicy> {
        self.live.current().map(|p| p.network)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST12900: what each policy lets through is the product's three modes: offline reaches
    // nothing, no_telemetry everything but analytics, full everything
    #[test]
    fn test12900_policy_allows_by_reach() {
        use NetworkPolicy::*;
        use Reach::*;
        let table = [
            (Offline, ThirdParty, false),
            (Offline, OurServices, false),
            (Offline, Telemetry, false),
            (NoTelemetry, ThirdParty, true),
            (NoTelemetry, OurServices, true),
            (NoTelemetry, Telemetry, false),
            (Full, ThirdParty, true),
            (Full, OurServices, true),
            (Full, Telemetry, true),
        ];
        for (policy, reach, allowed) in table {
            assert_eq!(policy.allows(reach), allowed, "{policy} / {reach:?}");
        }
    }

    // TEST12901: the wire words round-trip and no other word is read as a policy
    #[test]
    fn test12901_wire_words() {
        for p in [NetworkPolicy::Offline, NetworkPolicy::NoTelemetry, NetworkPolicy::Full] {
            assert_eq!(NetworkPolicy::from_wire(p.as_str()), Some(p));
        }
        for word in ["", "Offline", "online", "no-telemetry", "none"] {
            assert_eq!(NetworkPolicy::from_wire(word), None, "{word:?}");
        }
    }

    // TEST12902: a handle reads the store live — a change made after it was handed out is
    // what its next check sees
    #[test]
    fn test12902_access_reads_the_live_policy() {
        let live = LivePolicy::with(HostPolicy::new(NetworkPolicy::Full));
        let access = NetworkAccess::new(live.clone());
        assert!(access.allow(Reach::ThirdParty).is_ok());
        live.set(HostPolicy::new(NetworkPolicy::Offline));
        let refused = access.allow(Reach::ThirdParty).unwrap_err();
        assert_eq!(refused, NetworkRefused::Policy { policy: NetworkPolicy::Offline, reach: Reach::ThirdParty });
        assert_eq!(refused.code(), "NETWORK_POLICY");
        assert_eq!(refused.class(), AttributionClass::User);
        assert!(refused.to_string().contains("'offline'"), "{refused}");
        live.set(HostPolicy::new(NetworkPolicy::NoTelemetry));
        assert!(access.allow(Reach::ThirdParty).is_ok());
        assert!(access.allow(Reach::Telemetry).is_err());
    }

    // TEST12903: a store never given a policy says so, and a handle over it lets nothing
    // out — as a defect of its own, not as the operator's offline
    #[test]
    fn test12903_unset_store_allows_nothing() {
        let live = LivePolicy::unset();
        assert_eq!(live.current(), None);
        let access = NetworkAccess::new(live);
        assert_eq!(access.policy(), None);
        for reach in [Reach::ThirdParty, Reach::OurServices, Reach::Telemetry] {
            let refused = access.allow(reach).unwrap_err();
            assert_eq!(refused, NetworkRefused::NoPolicy { reach });
            assert_eq!(refused.code(), "NETWORK_POLICY_UNSET");
            assert_eq!(refused.class(), AttributionClass::Internal);
        }
    }
}
