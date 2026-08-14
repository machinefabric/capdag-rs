//! Cartridge registry slug — deterministic, HUMAN-READABLE mapping from a
//! registry URL to a top-level folder name under the cartridges install root.
//!
//! A registry is identified by its **authority** (host, plus `:port` if
//! present) — NOT the full URL. The manifest path (e.g. `/v1/manifest`) and any
//! query/trailing slash are IGNORED, so every version of the same registry
//! shares one slug and the on-disk version level (`v<N>`) sits beneath it. The
//! slug is a path-safe transform of that authority: lowercased, with every
//! character outside `[a-z0-9.-]` replaced by `-` (so a port `:` becomes `-`).
//! No hashing — domains are already unique and readable, and a readable folder
//! name is far easier to reason about on disk.
//!
//! The literal string `"dev"` is reserved for dev cartridges that have no
//! registry. The mapping is one-way: folder → URL is recovered from each
//! installed cartridge's own `cartridge.json:registry_url`, and the
//! installer/host validates `slug_for(cartridge_json.registry_url) ==
//! folder_name` at parse time.

use serde::{Deserialize, Deserializer};

/// Required-but-nullable `Option<String>` for serde wire formats.
///
/// Stock serde treats an absent key and an explicit `null` the same
/// way for `Option<T>`. We need stricter semantics: the key MUST be
/// present in the JSON object; the value MAY be `null`. This rejects
/// old-schema payloads where the key is absent entirely, instead of
/// silently treating them as dev installs.
///
/// Use as the field type and add `#[serde(deserialize_with = "deserialize_required_nullable_string")]`
/// — wait, serde's `deserialize_with` doesn't see absence. The real
/// path is to use the `must_have_field` pattern via a manual
/// `Deserialize` on the parent struct. We expose this helper for
/// callers that already have a manual impl and want a single place
/// to centralize the "decode Option<String>, but the caller has
/// already verified presence" decode step.
///
/// The mirror-compatible enforcement lives in
/// `CartridgeJson::deserialize` / `CapManifest::deserialize` — they
/// build a `serde_json::Value` first, check `obj.contains_key("registry_url")`,
/// then re-deserialize. This helper is for tests and any future
/// types that follow the same pattern.
pub fn deserialize_option_string<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)
}

/// Reserved folder name for cartridges with no registry (developer-built
/// cartridges installed via a workspace cartridge install without `--registry`).
/// A real registry authority is never the literal `dev`, so the two namespaces
/// never overlap.
pub const DEV_SLUG: &str = "dev";

/// Extract the authority (host, plus `:port` if present) from a registry URL:
/// the substring after `://` up to the next `/`, `?`, or `#`. If there is no
/// `://`, the whole string up to those delimiters is taken. The manifest path,
/// version segment, query, and fragment are all discarded — the slug is
/// version- and path-independent by construction.
fn authority_of(url: &str) -> &str {
    let after_scheme = match url.find("://") {
        Some(i) => &url[i + 3..],
        None => url,
    };
    let end = after_scheme
        .find(['/', '?', '#'])
        .unwrap_or(after_scheme.len());
    &after_scheme[..end]
}

/// Compute the on-disk slug for a registry URL.
///
/// `None` (a dev cartridge) → the literal [`DEV_SLUG`].
/// `Some(url)` → a path-safe transform of the URL's authority: lowercased,
/// with every character outside `[a-z0-9.-]` replaced by `-` (so a port `:`
/// becomes `-`). The slug depends ONLY on the authority — path (including the
/// `/v<N>/manifest` version segment), query, trailing slash, and host case do
/// not change it. This is the identity of the registry as a network location;
/// the registry regime version is a separate on-disk level below the slug.
pub fn slug_for(registry_url: Option<&str>) -> String {
    match registry_url {
        None => DEV_SLUG.to_string(),
        Some(url) => authority_of(url)
            .chars()
            .map(|c| {
                let c = c.to_ascii_lowercase();
                if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-' {
                    c
                } else {
                    '-'
                }
            })
            .collect(),
    }
}

/// True if `s` could be a valid slug for a non-dev registry: a non-empty
/// path-safe authority string (`[a-z0-9.-]+`) that is not the reserved dev
/// sentinel. Used by host scanners to distinguish dev folders from registry
/// folders before they read any cartridge.json.
pub fn is_registry_slug(s: &str) -> bool {
    s != DEV_SLUG
        && !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-')
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TEST1500: The central registry's URL maps to its readable authority.
    /// The slug is the host verbatim (lowercased, path-safe) — no hash. If this
    /// ever changes silently every installed cartridge lands in the wrong
    /// directory and stops being discovered, so the value is pinned as a literal.
    #[test]
    fn test1500_slug_for_central_registry_is_the_host() {
        assert_eq!(
            slug_for(Some("https://cartridges.machinefabric.com/v1/manifest")),
            "cartridges.machinefabric.com"
        );
        assert_eq!(
            slug_for(Some(
                "https://cartridges-staging.machinefabric.com/v1/manifest"
            )),
            "cartridges-staging.machinefabric.com"
        );
        assert!(is_registry_slug("cartridges.machinefabric.com"));
    }

    /// TEST1501: `None` (dev cartridge) maps to the literal `dev`, which is not
    /// classified as a registry slug.
    #[test]
    fn test1501_slug_for_none_is_dev() {
        assert_eq!(slug_for(None), DEV_SLUG);
        assert!(!is_registry_slug(DEV_SLUG));
    }

    /// TEST1502: The slug depends ONLY on the authority. The manifest path (incl.
    /// the `/v<N>/` version segment), a trailing slash, and a query string are
    /// all discarded, and the host is compared case-insensitively — so every
    /// version and spelling of the same registry shares one slug. Two DIFFERENT
    /// hosts, or the same host on different ports, are distinct registries.
    #[test]
    fn test1502_slug_is_authority_only() {
        let base = slug_for(Some("https://cartridges.machinefabric.com/manifest"));
        // Path / version / query / trailing slash / host case do NOT change it.
        assert_eq!(
            base,
            slug_for(Some("https://cartridges.machinefabric.com/v1/manifest"))
        );
        assert_eq!(
            base,
            slug_for(Some("https://cartridges.machinefabric.com/v2/manifest"))
        );
        assert_eq!(
            base,
            slug_for(Some("https://cartridges.machinefabric.com/manifest/"))
        );
        assert_eq!(
            base,
            slug_for(Some("https://cartridges.machinefabric.com/manifest?v=1"))
        );
        assert_eq!(
            base,
            slug_for(Some("https://CARTRIDGES.machinefabric.com/manifest"))
        );
        // A different host is a different registry.
        assert_ne!(
            base,
            slug_for(Some("https://other.machinefabric.com/manifest"))
        );
        // Port is part of the authority: different ports → different slugs, and
        // the port ':' becomes a path-safe '-'.
        assert_eq!(
            slug_for(Some("http://localhost:8080/manifest")),
            "localhost-8080"
        );
        assert_ne!(
            slug_for(Some("http://localhost:8080/manifest")),
            slug_for(Some("http://localhost:9090/manifest"))
        );
    }

    /// TEST1503: `slug_for` is deterministic — same URL, same slug every time.
    #[test]
    fn test1503_slug_is_deterministic() {
        let url = "https://example.com/some/registry/path?token=abc";
        let s1 = slug_for(Some(url));
        let s2 = slug_for(Some(url));
        assert_eq!(s1, s2);
        assert_eq!(s1, "example.com");
    }

    /// TEST1504: A registry slug never equals the reserved `dev` sentinel for
    /// any real registry URL — that invariant is what lets the folder name be a
    /// dev-vs-registry discriminator without reading any file inside.
    #[test]
    fn test1504_dev_never_collides_with_registry_slug() {
        let probes = [
            "https://a.test",
            "https://b.test/manifest",
            "https://localhost:8080/manifest",
            "https://cartridges.machinefabric.com/v1/manifest",
        ];
        for p in probes {
            let s = slug_for(Some(p));
            assert_ne!(s, DEV_SLUG);
            assert!(is_registry_slug(&s));
        }
    }

    /// TEST1505: `is_registry_slug` rejects the dev sentinel and non-path-safe
    /// strings, accepts a lowercase authority. Used by the XPC service and
    /// engine to distinguish dev folders from registry folders during the scan.
    #[test]
    fn test1505_is_registry_slug_classification() {
        assert!(!is_registry_slug(DEV_SLUG));
        assert!(!is_registry_slug(""));
        assert!(!is_registry_slug("Has Space"));
        assert!(!is_registry_slug("UPPER.example.com")); // uppercase rejected
        assert!(!is_registry_slug("has/slash"));
        assert!(is_registry_slug("cartridges.machinefabric.com"));
        assert!(is_registry_slug("localhost-8080"));
        assert!(is_registry_slug(&slug_for(Some(
            "https://cartridges.machinefabric.com/v1/manifest"
        ))));
    }
}
