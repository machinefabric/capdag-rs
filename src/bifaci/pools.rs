//! Concurrency pools — the ONE capacity concept of the protocol.
//!
//! A pool is a named concurrency domain on a cartridge process. Every
//! registered cap IS a pool of one, named by its canonical cap URN; the
//! reserved pool [`POOL_ALL`] contains every cap (it is what the deleted
//! scalar `handler_capacity` used to be); the manifest may declare further
//! named pools over subsets of caps. A cap's POOL CHAIN — its own singleton
//! pool, every declared pool containing it, then `all` — is the set of
//! domains a dispatch must be admitted through. Queues lead to pools: each
//! request waits in its cap's singleton-pool queue; shared pools own no
//! queue of their own.
//!
//! Three numbers per pool, one effective value:
//! - `declared`   — the manifest's shipped default (the cartridge's).
//! - `configured` — the operator's number (starts = declared; persisted by
//!                  the engine's cartridge configuration store).
//! - `available`  — OPTIONAL cartridge self-report: what the process can
//!                  serve right now from its OWN state (a sandboxed process
//!                  can inspect itself and nothing else, so this is
//!                  self-state — "loading a model" — never host sensing).
//!                  Absent means static: the normal, fully-supported case.
//!
//! `effective = min(configured, available)` with 0-as-unlimited treated as
//! infinity inside the min and absent `available` treated as infinity.
//!
//! On the wire the pool-state map rides as JSON bytes in frame meta —
//! exactly the transport the manifest itself uses — under the
//! [`META_POOLS`] key (HELLO and every heartbeat reply) and, host→cartridge,
//! the [`META_DESIRED_CAPACITIES`] key on a heartbeat probe (the operator's
//! configured values being delivered). The roster's `runtime_stats` carries
//! the same map, so admission, the engine's protocol-health surface, and
//! the clients' cartridge views all read one shape.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::urn::cap_urn::CapUrn;

/// The reserved pool containing every cap. Always exists; default
/// capacity 0 (unlimited). The exact replacement of the deleted scalar
/// `handler_capacity`.
pub const POOL_ALL: &str = "all";

/// Frame-meta key carrying a JSON-encoded [`PoolStates`] map (HELLO reply,
/// every heartbeat reply, and the roster's runtime stats).
pub const META_POOLS: &str = "pools";

/// Frame-meta key on a heartbeat PROBE carrying a JSON-encoded
/// `BTreeMap<String, u64>` of pool name → desired `configured` value.
pub const META_DESIRED_CAPACITIES: &str = "desired_capacities";

/// Unlimited, as a capacity value. Everywhere a capacity is read, 0 means
/// "no limit" — never "zero slots".
pub const CAPACITY_UNLIMITED: u64 = 0;

/// One pool's full state. The same shape everywhere: manifest-derived
/// declarations, heartbeat replies, roster stats, the engine's run-runtime
/// resource, and both clients' cartridge views.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PoolState {
    /// The manifest's shipped default. 0 = unlimited.
    pub declared: u64,
    /// The operator's number. Starts equal to `declared`. 0 = unlimited.
    pub configured: u64,
    /// The cartridge's self-reported current limit. Absent = static (the
    /// cartridge never self-adjusts this pool) and is treated as unlimited
    /// inside `effective`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available: Option<u64>,
    /// Requests currently being served in this pool.
    pub active: u64,
    /// Requests currently queued against this pool. For shared pools this
    /// counts waiters whose OWN pool has room but this pool does not.
    pub queued: u64,
    /// Member caps (canonical URNs). Singleton pools omit the list — the
    /// pool's name IS its one member.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caps: Vec<String>,
}

impl PoolState {
    /// A declared pool at rest: configured = declared, nothing active,
    /// no self-report.
    pub fn declared(declared: u64, caps: Vec<String>) -> Self {
        Self {
            declared,
            configured: declared,
            available: None,
            active: 0,
            queued: 0,
            caps,
        }
    }

    /// The effective admission bound: `min(configured, available)` with 0
    /// meaning unlimited on either input and on the output, and an absent
    /// `available` treated as unlimited.
    pub fn effective(&self) -> u64 {
        effective_capacity(self.configured, self.available)
    }
}

/// `min(configured, available)` under the 0-as-unlimited convention.
pub fn effective_capacity(configured: u64, available: Option<u64>) -> u64 {
    let configured = if configured == CAPACITY_UNLIMITED {
        u64::MAX
    } else {
        configured
    };
    let available = match available {
        None => u64::MAX,
        Some(CAPACITY_UNLIMITED) => u64::MAX,
        Some(n) => n,
    };
    let effective = configured.min(available);
    if effective == u64::MAX {
        CAPACITY_UNLIMITED
    } else {
        effective
    }
}

/// The full pool-state map of one cartridge process, keyed by pool name
/// (a canonical cap URN for singletons, a declared pool name, or `all`).
pub type PoolStates = BTreeMap<String, PoolState>;

/// The host→cartridge desired-configured map delivered on a heartbeat probe.
pub type DesiredCapacities = BTreeMap<String, u64>;

/// The manifest's pool DECLARATIONS: shared-pool memberships plus a
/// capacities map whose keys are pool names uniformly (a canonical cap URN,
/// a declared pool name, or `all`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolDeclarations {
    /// Declared shared pools: name → member caps (canonical URNs).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub pools: BTreeMap<String, Vec<String>>,
    /// Declared capacities by pool name. Absent = 0 = unlimited.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub capacities: BTreeMap<String, u64>,
}

impl PoolDeclarations {
    pub fn is_empty(&self) -> bool {
        self.pools.is_empty() && self.capacities.is_empty()
    }

    /// Validate the declarations against the set of declared caps and
    /// canonicalize every cap reference. Hard errors, never coercion:
    /// - a shared pool named `all` or parsing as a cap URN;
    /// - a pool member or capacity key that names no declared cap
    ///   (capacity keys may also name a declared pool or `all`);
    /// - a cap listed twice in one pool.
    ///
    /// Returns the canonicalized declarations (cap references rewritten to
    /// canonical URN form so every later lookup is by identity).
    pub fn validated(&self, declared_caps: &[CapUrn]) -> Result<PoolDeclarations, String> {
        let canonical: Vec<String> = declared_caps.iter().map(|c| c.to_string()).collect();
        let canonicalize = |raw: &str| -> Result<String, String> {
            let urn = CapUrn::from_string(raw)
                .map_err(|e| format!("pool cap reference '{raw}' is not a valid cap URN: {e}"))?;
            let canon = urn.to_string();
            if !canonical.iter().any(|c| c == &canon) {
                return Err(format!(
                    "pool cap reference '{raw}' names no cap declared by this manifest"
                ));
            }
            Ok(canon)
        };

        let mut pools = BTreeMap::new();
        for (name, members) in &self.pools {
            if name == POOL_ALL {
                return Err(format!(
                    "pool name '{POOL_ALL}' is reserved for the implicit all-caps pool"
                ));
            }
            if CapUrn::from_string(name).is_ok() {
                return Err(format!(
                    "pool name '{name}' parses as a cap URN — cap URNs name the implicit \
                     singleton pools and cannot be redeclared"
                ));
            }
            if members.is_empty() {
                return Err(format!("pool '{name}' declares no member caps"));
            }
            let mut seen = Vec::new();
            let mut canon_members = Vec::new();
            for member in members {
                let canon = canonicalize(member)?;
                if seen.contains(&canon) {
                    return Err(format!(
                        "pool '{name}' lists cap '{canon}' more than once"
                    ));
                }
                seen.push(canon.clone());
                canon_members.push(canon);
            }
            pools.insert(name.clone(), canon_members);
        }

        let mut capacities = BTreeMap::new();
        for (key, value) in &self.capacities {
            let canon_key = if key == POOL_ALL || pools.contains_key(key) {
                key.clone()
            } else {
                canonicalize(key).map_err(|e| {
                    format!(
                        "capacity key '{key}' is neither '{POOL_ALL}', a declared pool, nor a \
                         declared cap: {e}"
                    )
                })?
            };
            if capacities.insert(canon_key.clone(), *value).is_some() {
                return Err(format!(
                    "capacity for pool '{canon_key}' is declared more than once \
                     (two spellings of one cap URN?)"
                ));
            }
        }

        Ok(PoolDeclarations { pools, capacities })
    }

    /// Materialize the full declared pool-state map for a cap set: one
    /// singleton pool per cap, every declared shared pool, and `all`.
    /// `self` must already be validated against the same cap set.
    pub fn declared_states(&self, declared_caps: &[CapUrn]) -> PoolStates {
        let mut states = PoolStates::new();
        let all_members: Vec<String> = declared_caps.iter().map(|c| c.to_string()).collect();
        for cap in &all_members {
            let declared = self.capacities.get(cap).copied().unwrap_or(CAPACITY_UNLIMITED);
            states.insert(cap.clone(), PoolState::declared(declared, Vec::new()));
        }
        for (name, members) in &self.pools {
            let declared = self
                .capacities
                .get(name)
                .copied()
                .unwrap_or(CAPACITY_UNLIMITED);
            states.insert(name.clone(), PoolState::declared(declared, members.clone()));
        }
        let all_declared = self
            .capacities
            .get(POOL_ALL)
            .copied()
            .unwrap_or(CAPACITY_UNLIMITED);
        states.insert(
            POOL_ALL.to_string(),
            PoolState::declared(all_declared, all_members),
        );
        states
    }

    /// The pool CHAIN of one cap, in admission order: its singleton pool,
    /// every declared pool containing it, then `all`. `cap` must be the
    /// canonical URN string.
    pub fn chain_for(&self, cap: &str) -> Vec<String> {
        let mut chain = vec![cap.to_string()];
        for (name, members) in &self.pools {
            if members.iter().any(|m| m == cap) {
                chain.push(name.clone());
            }
        }
        chain.push(POOL_ALL.to_string());
        chain
    }
}

/// The chain of one cap over a MATERIALIZED state map (roster / heartbeat
/// truth): the singleton pool, every pool listing the cap as a member,
/// then `all`. Order: singleton, declared pools in map order, `all`.
pub fn chain_from_states(states: &PoolStates, cap: &str) -> Vec<String> {
    let mut chain = Vec::new();
    if states.contains_key(cap) {
        chain.push(cap.to_string());
    }
    for (name, state) in states {
        if name != POOL_ALL && name != cap && state.caps.iter().any(|m| m == cap) {
            chain.push(name.clone());
        }
    }
    if states.contains_key(POOL_ALL) {
        chain.push(POOL_ALL.to_string());
    }
    chain
}

/// Encode a pool-state map for frame meta (JSON bytes — the manifest's own
/// transport).
pub fn encode_pool_states(states: &PoolStates) -> Vec<u8> {
    serde_json::to_vec(states).expect("pool states are always JSON-encodable")
}

/// Decode a pool-state map from frame meta. A malformed map is a protocol
/// error at the caller's boundary — never partially read.
pub fn decode_pool_states(bytes: &[u8]) -> Result<PoolStates, String> {
    serde_json::from_slice(bytes).map_err(|e| format!("malformed pool-state map: {e}"))
}

/// Encode the desired-configured map for a heartbeat probe.
pub fn encode_desired(desired: &DesiredCapacities) -> Vec<u8> {
    serde_json::to_vec(desired).expect("desired capacities are always JSON-encodable")
}

/// Decode the desired-configured map from a heartbeat probe.
pub fn decode_desired(bytes: &[u8]) -> Result<DesiredCapacities, String> {
    serde_json::from_slice(bytes).map_err(|e| format!("malformed desired-capacities map: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caps(urns: &[&str]) -> Vec<CapUrn> {
        urns.iter()
            .map(|u| CapUrn::from_string(u).expect("test cap urn"))
            .collect()
    }

    // TEST1520: effective capacity is min(configured, available) under the
    // 0-as-unlimited convention, with absent available treated as unlimited
    // — the one formula every admission decision reduces to.
    #[test]
    fn test1520_effective_capacity_min_semantics() {
        assert_eq!(effective_capacity(0, None), 0, "unlimited stays unlimited");
        assert_eq!(effective_capacity(4, None), 4, "absent available is a free pass");
        assert_eq!(effective_capacity(4, Some(0)), 4, "available 0 is unlimited, not zero slots");
        assert_eq!(effective_capacity(0, Some(2)), 2, "self-limit binds an unlimited configured");
        assert_eq!(effective_capacity(4, Some(1)), 1, "the smaller bound wins");
        assert_eq!(effective_capacity(1, Some(4)), 1, "in either direction");
    }

    // TEST1521: a cap is a pool of one and `all` always exists — the
    // declared state map materializes every singleton, every declared pool,
    // and `all`, with capacities resolved by pool name uniformly.
    #[test]
    fn test1521_declared_states_materialize_every_pool() {
        let declared_caps = caps(&[
            "cap:generate;in=\"media:enc=utf-8\";out=\"media:enc=utf-8\"",
            "cap:embed;in=\"media:enc=utf-8\";out=\"media:embeddings\"",
        ]);
        let generate = declared_caps[0].to_string();
        let embed = declared_caps[1].to_string();
        let declarations = PoolDeclarations {
            pools: BTreeMap::from([("gpu".to_string(), vec![generate.clone(), embed.clone()])]),
            capacities: BTreeMap::from([
                (generate.clone(), 1),
                ("gpu".to_string(), 1),
                (POOL_ALL.to_string(), 8),
            ]),
        };
        let declarations = declarations.validated(&declared_caps).expect("valid");
        let states = declarations.declared_states(&declared_caps);

        assert_eq!(states.len(), 4, "two singletons + gpu + all");
        assert_eq!(states[&generate].declared, 1);
        assert_eq!(states[&generate].configured, 1, "configured starts at declared");
        assert_eq!(states[&embed].declared, 0, "undeclared singleton is unlimited");
        assert_eq!(states["gpu"].declared, 1);
        assert_eq!(states["gpu"].caps, vec![generate.clone(), embed.clone()]);
        assert_eq!(states[POOL_ALL].declared, 8);
        assert_eq!(states[POOL_ALL].caps.len(), 2, "all contains every cap");

        // The chain: singleton, declared pools containing the cap, all.
        assert_eq!(
            declarations.chain_for(&generate),
            vec![generate.clone(), "gpu".to_string(), POOL_ALL.to_string()]
        );
        // And the same chain derived from the materialized states.
        assert_eq!(
            chain_from_states(&states, &generate),
            vec![generate, "gpu".to_string(), POOL_ALL.to_string()]
        );
    }

    // TEST1522: pool declarations are validated hard — reserved name, a
    // pool named like a cap URN, an unknown member, a duplicate member, and
    // an unknown capacity key are each refused with the offender named.
    #[test]
    fn test1522_pool_declaration_validation_refuses_illegal_shapes() {
        let declared_caps = caps(&["cap:generate;in=\"media:enc=utf-8\";out=\"media:enc=utf-8\""]);
        let generate = declared_caps[0].to_string();

        let reserved = PoolDeclarations {
            pools: BTreeMap::from([(POOL_ALL.to_string(), vec![generate.clone()])]),
            capacities: BTreeMap::new(),
        };
        assert!(reserved.validated(&declared_caps).unwrap_err().contains("reserved"));

        let cap_named = PoolDeclarations {
            pools: BTreeMap::from([(generate.clone(), vec![generate.clone()])]),
            capacities: BTreeMap::new(),
        };
        assert!(cap_named
            .validated(&declared_caps)
            .unwrap_err()
            .contains("parses as a cap URN"));

        let unknown_member = PoolDeclarations {
            pools: BTreeMap::from([(
                "gpu".to_string(),
                vec!["cap:absent;in=\"media:\";out=\"media:\"".to_string()],
            )]),
            capacities: BTreeMap::new(),
        };
        assert!(unknown_member
            .validated(&declared_caps)
            .unwrap_err()
            .contains("names no cap"));

        let duplicate = PoolDeclarations {
            pools: BTreeMap::from([("gpu".to_string(), vec![generate.clone(), generate.clone()])]),
            capacities: BTreeMap::new(),
        };
        assert!(duplicate
            .validated(&declared_caps)
            .unwrap_err()
            .contains("more than once"));

        let unknown_capacity = PoolDeclarations {
            pools: BTreeMap::new(),
            capacities: BTreeMap::from([("warp".to_string(), 3)]),
        };
        assert!(unknown_capacity
            .validated(&declared_caps)
            .unwrap_err()
            .contains("neither"));
    }

    // TEST1523: the wire codec round-trips the full map — including the
    // absent-vs-present distinction on `available`, which is the
    // static-vs-self-limited distinction and must never collapse.
    #[test]
    fn test1523_pool_state_wire_round_trip() {
        let mut states = PoolStates::new();
        states.insert(
            "cap:x;in=\"media:\";out=\"media:\"".to_string(),
            PoolState {
                declared: 2,
                configured: 4,
                available: Some(1),
                active: 1,
                queued: 3,
                caps: Vec::new(),
            },
        );
        states.insert(POOL_ALL.to_string(), PoolState::declared(0, vec!["cap:x;in=\"media:\";out=\"media:\"".to_string()]));

        let bytes = encode_pool_states(&states);
        let decoded = decode_pool_states(&bytes).expect("round-trip");
        assert_eq!(decoded, states);
        assert_eq!(decoded["cap:x;in=\"media:\";out=\"media:\""].effective(), 1);
        assert!(decoded[POOL_ALL].available.is_none(), "static stays static");

        assert!(decode_pool_states(b"not json").is_err());

        let desired = DesiredCapacities::from([("all".to_string(), 6u64)]);
        assert_eq!(
            decode_desired(&encode_desired(&desired)).expect("desired round-trip"),
            desired
        );
    }
}
