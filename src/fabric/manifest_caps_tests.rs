//! The fabric's caps as a whole — `FabricRegistry::manifest_caps` — and the
//! graph plans are found in, made of them (`LiveCapFab::from_fabric`), against
//! a registry whose manifest names caps its cache does not hold.
//!
//! The fixture (`tests/fixtures/fabric13_plan_caps.json`) is five caps of
//! fabric manifest 13 as published — render-page-image-txt, png2jpeg,
//! csv2json, json2csv and disbind-pdf — with every media definition and dim
//! they name. A stub registry serves them at the publisher's layout; the
//! registry under test starts with nothing of them in memory and keeps what
//! it fetches in a cache folder of its own.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::*;
use crate::fabric::stub_registry::{Answer, StubRegistry};
use crate::planner::{LiveCapFab, PlanRequest, SourceSpec, TargetSpec};

const FIXTURE: &str = include_str!("../../tests/fixtures/fabric13_plan_caps.json");

/// The fixture's definitions, each as the registry serves it.
struct Fabric {
    version: u32,
    caps: BTreeMap<String, serde_json::Value>,
    media: BTreeMap<String, serde_json::Value>,
    dims: BTreeMap<String, serde_json::Value>,
}

fn fabric() -> Fabric {
    #[derive(Deserialize)]
    struct Fixture {
        manifest_version: u32,
        caps: BTreeMap<String, serde_json::Value>,
        media: BTreeMap<String, serde_json::Value>,
        dims: BTreeMap<String, serde_json::Value>,
    }
    let fixture: Fixture = serde_json::from_str(FIXTURE).expect("the fixture parses");
    Fabric {
        version: fixture.manifest_version,
        caps: fixture.caps.into_iter().map(|(urn, def)| (normalize_cap_urn(&urn).unwrap(), def)).collect(),
        media: fixture.media.into_iter().map(|(urn, def)| (normalize_media_urn(&urn).unwrap(), def)).collect(),
        dims: fixture.dims,
    }
}

fn defver(def: &serde_json::Value) -> u32 {
    def["version"].as_u64().expect("a published definition has a version") as u32
}

/// The path a definition is served at in the publisher's layout.
fn path_of(kind: &str, key: &str, version: u32) -> String {
    let url = layout_locations("http://stub").definition(kind, &cache::key_hash(key), version);
    url.strip_prefix("http://stub").unwrap().to_string()
}

impl Fabric {
    /// Every definition's path and body.
    fn bodies(&self) -> BTreeMap<String, Vec<u8>> {
        let mut bodies = BTreeMap::new();
        for (kind, defs) in [("caps", &self.caps), ("media", &self.media), ("dims", &self.dims)] {
            for (key, def) in defs {
                bodies.insert(path_of(kind, key, defver(def)), serde_json::to_vec(def).unwrap());
            }
        }
        bodies
    }

    fn cap_path(&self, urn: &str) -> String {
        path_of("caps", urn, defver(&self.caps[urn]))
    }
}

/// A stub serving the fixture: each path in `refused` answered 403, every
/// other definition with its body, anything else 404.
async fn serve(fabric: &Fabric, refused: &[String]) -> StubRegistry {
    let bodies = Arc::new(fabric.bodies());
    let refused: Vec<String> = refused.to_vec();
    StubRegistry::start(move |path| {
        if refused.iter().any(|r| r == path) {
            return Answer::Status(403);
        }
        match bodies.get(path) {
            Some(body) => Answer::Body(body.clone()),
            None => Answer::Status(404),
        }
    })
    .await
}

/// A registry pinned at the fixture's manifest version whose manifest names
/// every fixture definition, none of them in memory, caching into `cache`.
fn registry(stub: &StubRegistry, fabric: &Fabric, cache: &tempfile::TempDir) -> FabricRegistry {
    let mut registry = FabricRegistry::new_for_test_with_config_and_version(
        RegistryConfig::new().with_anchor(stub.anchor.clone()),
        fabric.version,
    );
    registry.cache_dir = cache.path().to_path_buf();
    for kind in cache::KIND_FOLDERS {
        fs::create_dir_all(registry.cache_dir.join(kind)).unwrap();
    }
    {
        let mut manifest = registry.manifest.lock().unwrap();
        for (urn, def) in &fabric.caps {
            manifest.caps.insert(urn.clone(), defver(def));
        }
        for (urn, def) in &fabric.media {
            manifest.media.insert(urn.clone(), defver(def));
        }
        for (name, def) in &fabric.dims {
            manifest.dims.insert(name.clone(), defver(def));
        }
    }
    registry
}

fn identity() -> String {
    normalize_cap_urn(crate::standard::caps::CAP_IDENTITY).unwrap()
}

// TEST8207: on a cold cache, the fabric's caps are every cap its manifest
// names, fetched from the registry, each once, with the media and dims they
// name; a second question fetches nothing
#[tokio::test]
async fn test8207_the_fabrics_caps_are_every_cap_the_manifest_names_on_a_cold_cache() {
    let fabric = fabric();
    let stub = serve(&fabric, &[]).await;
    let cache = tempfile::tempdir().unwrap();
    let registry = registry(&stub, &fabric, &cache);
    assert!(
        registry.get_cached_caps().await.unwrap().iter().all(|c| c.urn.to_string() == identity()),
        "nothing of the fabric is in memory yet"
    );

    let caps = registry.manifest_caps().await.unwrap();
    let mut got: Vec<String> = caps.iter().map(|c| c.urn.to_string()).collect();
    let mut expected: Vec<String> = fabric.caps.keys().cloned().chain([identity()]).collect();
    got.sort();
    expected.sort();
    assert_eq!(got, expected);
    for urn in fabric.caps.keys() {
        assert_eq!(stub.requests_for(&fabric.cap_path(urn)), 1, "{urn} is fetched once");
    }

    let fetched = stub.total_requests();
    assert_eq!(registry.manifest_caps().await.unwrap().len(), expected.len());
    assert_eq!(stub.total_requests(), fetched, "a second question fetches nothing");
}

// TEST8208: offline with a cold cache, the fabric's caps cannot be had: the
// question fails, naming every cap it could not load and why, and never
// answers with the part of the fabric that is in memory
#[tokio::test]
async fn test8208_offline_on_a_cold_cache_the_caps_fail_naming_each_missing_cap() {
    let fabric = fabric();
    let stub = serve(&fabric, &[]).await;
    let cache = tempfile::tempdir().unwrap();
    let registry = registry(&stub, &fabric, &cache);
    registry.set_offline(true);

    let error = registry.manifest_caps().await.unwrap_err();
    assert!(matches!(error, FabricRegistryError::Caps(_)), "{error:?}");
    let message = error.to_string();
    assert!(
        message.starts_with("5 of the 6 caps of manifest v13 could not be loaded"),
        "{message}"
    );
    for urn in fabric.caps.keys() {
        assert!(message.contains(&format!("{urn}: Network access blocked")), "{urn} is not named: {message}");
    }
    assert_eq!(stub.total_requests(), 0);

    registry.set_offline(false);
    assert_eq!(registry.manifest_caps().await.unwrap().len(), 6);
}

// TEST8209: a cap the registry refuses fails the question, naming that cap
// and the refusal: the fabric's caps are never answered without it
#[tokio::test]
async fn test8209_a_refused_cap_fails_the_fabrics_caps_naming_it() {
    let fabric = fabric();
    let refused = normalize_cap_urn(
        r#"cap:convert-format;in="media:enc=utf8;fmt=csv;list;record";out="media:enc=utf8;fmt=json;list;record""#,
    )
    .unwrap();
    let stub = serve(&fabric, &[fabric.cap_path(&refused)]).await;
    let cache = tempfile::tempdir().unwrap();
    let registry = registry(&stub, &fabric, &cache);

    let message = registry.manifest_caps().await.unwrap_err().to_string();
    assert!(message.starts_with("1 of the 6 caps of manifest v13 could not be loaded"), "{message}");
    assert!(message.contains(&refused) && message.contains("403"), "{message}");
}

// TEST8210: a file of a type the fabric's caps take is planned on a cold
// cache: its type is told, the graph is every cap the manifest names, and
// what it can become and the machines that make it are found — a .txt can
// become a PNG (render-page-image) and a JPEG (then png2jpeg)
#[tokio::test]
async fn test8210_a_file_is_planned_against_caps_no_cache_holds() {
    let fabric = fabric();
    let stub = serve(&fabric, &[]).await;
    let cache = tempfile::tempdir().unwrap();
    let registry = registry(&stub, &fabric, &cache);
    let files = tempfile::tempdir().unwrap();
    let file = files.path().join("notes.txt");
    fs::write(&file, "A line of text.\n").unwrap();

    let media = crate::detect_file_media(
        &file,
        &registry,
        &crate::NoAdapters,
        &crate::NoInvoker,
        &crate::ValueAdapterRegistry::new(),
    )
    .await
    .unwrap();
    let txt = crate::MediaUrn::from_string(media.primary()).unwrap();
    assert_eq!(txt.to_string(), "media:enc=utf8;ext=txt;plain-text");

    let fab = LiveCapFab::from_fabric(&registry).await.unwrap();
    let sources = vec![SourceSpec::single(txt)];
    let targets: Vec<String> = fab
        .discover_convergent_targets(&sources, PlanRequest::DEFAULT_MAX_DEPTH)
        .unwrap()
        .targets
        .into_iter()
        .map(|t| t.media_def.to_string())
        .collect();
    for wanted in ["media:ext=png;image", "media:ext=jpeg;image"] {
        assert!(targets.iter().any(|t| t == wanted), "{wanted} is not among {targets:?}");
    }

    let png = crate::MediaUrn::from_string("media:ext=png;image").unwrap();
    let outcome = fab
        .plan(&PlanRequest::auto(sources, TargetSpec::Exact(vec![png])), &registry)
        .unwrap();
    assert!(
        outcome.candidates.iter().any(|c| c.notation.contains("render-page-image")),
        "{:?}",
        outcome.candidates.iter().map(|c| &c.notation).collect::<Vec<_>>()
    );
}
