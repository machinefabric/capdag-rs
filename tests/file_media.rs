//! The file media types a file's extension names, from the whole manifest,
//! and what a file is.
//!
//! A registry fixture serves, at its `definition` location, every media
//! definition of fabric manifest 13 that carries the `ext=` file-type tag,
//! and the JSON records that list an extension without one, as published
//! (`fixtures/fabric13_file_media.json`). The registry's manifest names them
//! all; what its cache holds is up to each test.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use capdag::{FabricRegistry, FabricRegistryError, MediaUrn, RegistryConfig, StoredMediaDef};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const FIXTURE: &str = include_str!("fixtures/fabric13_file_media.json");
const MANIFEST_VERSION: u32 = 13;

fn definitions() -> BTreeMap<String, StoredMediaDef> {
    #[derive(serde::Deserialize)]
    struct Fixture {
        definitions: BTreeMap<String, StoredMediaDef>,
    }
    serde_json::from_str::<Fixture>(FIXTURE).expect("the fixture parses").definitions
}

fn is_file_type(urn: &str) -> bool {
    MediaUrn::from_string(urn).unwrap().get_tag("ext").is_some()
}

/// The fixture registry: serves `/media/<sha256 of URN>/<defver>.json` for
/// every definition, 404 for anything else, and counts what it served.
async fn serve(defs: &BTreeMap<String, StoredMediaDef>) -> (String, Arc<AtomicUsize>) {
    let mut bodies: BTreeMap<String, String> = BTreeMap::new();
    for (urn, def) in defs {
        let hash = format!("{:x}", Sha256::digest(urn.as_bytes()));
        bodies.insert(
            format!("/media/{hash}/{}.json", def.version),
            serde_json::to_string(def).unwrap(),
        );
    }
    let bodies = Arc::new(bodies);
    let served = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let counter = Arc::clone(&served);
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else { return };
            let bodies = Arc::clone(&bodies);
            let counter = Arc::clone(&counter);
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buf = [0u8; 2048];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => request.extend_from_slice(&buf[..n]),
                    }
                }
                let line = String::from_utf8_lossy(&request);
                let path = line.split_whitespace().nth(1).unwrap_or("").split('?').next().unwrap_or("");
                let response = match bodies.get(path) {
                    Some(body) => {
                        counter.fetch_add(1, Ordering::SeqCst);
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                    }
                    None => "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string(),
                };
                let _ = sock.write_all(response.as_bytes()).await;
                let _ = sock.flush().await;
            });
        }
    });
    (format!("http://{addr}"), served)
}

/// A registry pinned at manifest 13 whose manifest names every fixture
/// definition, fetching from `anchor`; the definitions in `cached` are in
/// its cache already.
fn registry(anchor: &str, cached: &[&str]) -> FabricRegistry {
    let registry = FabricRegistry::new_for_test_with_config_and_version(
        RegistryConfig::new().with_anchor(anchor),
        MANIFEST_VERSION,
    );
    let defs = definitions();
    for (urn, def) in &defs {
        if cached.contains(&urn.as_str()) {
            registry.insert_cached_media_def_for_test(def.clone());
        } else {
            registry.list_media_in_manifest_for_test(urn, def.version);
        }
    }
    registry
}

async fn urns(registry: &FabricRegistry, extension: &str) -> Vec<String> {
    registry
        .file_media_for_extension(extension)
        .await
        .unwrap_or_else(|e| panic!(".{extension}: {e}"))
        .into_iter()
        .map(|d| d.urn)
        .collect()
}

// TEST7177: on a cold cache, a file's extension names every file media type
// the manifest declares for it, fetched from the registry: .csv and .yaml
// are known although nothing has cached them, and the content-only JSON
// records a machine has cached (a model download result) are no file types
#[tokio::test]
async fn test7177_extensions_name_the_manifests_file_types_on_a_cold_cache() {
    let defs = definitions();
    let (anchor, served) = serve(&defs).await;
    let registry = registry(&anchor, &["media:download-result;enc=utf8;fmt=json;record"]);

    assert_eq!(urns(&registry, "csv").await, vec!["media:enc=utf8;ext=csv;fmt=csv;list;record"]);
    let yaml = vec![
        "media:enc=utf8;ext=yaml;fmt=yaml",
        "media:enc=utf8;ext=yaml;fmt=yaml;list",
        "media:enc=utf8;ext=yaml;fmt=yaml;list;record",
        "media:enc=utf8;ext=yaml;fmt=yaml;record",
    ];
    assert_eq!(urns(&registry, "yaml").await, yaml);
    assert_eq!(urns(&registry, "YML").await, yaml);
    assert_eq!(
        urns(&registry, "json").await,
        vec![
            "media:enc=utf8;ext=json;fmt=json",
            "media:enc=utf8;ext=json;fmt=json;list",
            "media:enc=utf8;ext=json;fmt=json;list;record",
            "media:enc=utf8;ext=json;fmt=json;record",
        ]
    );
    assert_eq!(
        urns(&registry, "txt").await,
        vec![
            "media:digitized;enc=utf8;ext=txt;plain-text",
            "media:enc=utf8;ext=requirements",
            "media:enc=utf8;ext=txt;image-description;plain-text",
            "media:enc=utf8;ext=txt;page;plain-text",
            "media:enc=utf8;ext=txt;plain-text",
        ]
    );

    let file_types = defs.keys().filter(|u| is_file_type(u)).count();
    assert_eq!(file_types, 156, "manifest 13 has 156 file media types");
    assert_eq!(
        served.load(Ordering::SeqCst),
        file_types,
        "every file media type is fetched once, and nothing else"
    );
}

// TEST7178: the file media types load only when a file's type is first
// asked, once: making the registry and reading other definitions fetches
// none of them, and a second question fetches nothing more
#[tokio::test]
async fn test7178_file_media_types_load_lazily_and_once() {
    let defs = definitions();
    let (anchor, served) = serve(&defs).await;
    let registry = registry(&anchor, &[]);

    registry.get_media_def("media:enc=utf8;fmt=json;model-list;record").await.unwrap();
    assert_eq!(served.load(Ordering::SeqCst), 1, "only the definition asked for is fetched");

    urns(&registry, "pdf").await;
    let after_first = served.load(Ordering::SeqCst);
    assert_eq!(after_first, 1 + 156);
    urns(&registry, "csv").await;
    urns(&registry, "md").await;
    assert_eq!(served.load(Ordering::SeqCst), after_first, "the index is loaded once");
}

// TEST7179: file media types already cached are not fetched again
#[tokio::test]
async fn test7179_cached_file_media_types_are_not_fetched() {
    let defs = definitions();
    let (anchor, served) = serve(&defs).await;
    let all: Vec<&str> = defs.keys().map(String::as_str).collect();
    let registry = registry(&anchor, &all);
    assert_eq!(urns(&registry, "csv").await, vec!["media:enc=utf8;ext=csv;fmt=csv;list;record"]);
    assert_eq!(served.load(Ordering::SeqCst), 0);
}

// TEST7180: offline with a cold cache, a file's type cannot be told: the
// question fails, naming every file media type it could not load and why,
// and never answers from the part of the fabric that is cached
#[tokio::test]
async fn test7180_offline_on_a_cold_cache_fails_naming_what_it_could_not_load() {
    let defs = definitions();
    let (anchor, served) = serve(&defs).await;
    let registry = registry(&anchor, &["media:enc=utf8;ext=txt;plain-text", "media:document;ext=pdf"]);
    registry.set_offline(true);

    let err = registry.file_media_for_extension("txt").await.unwrap_err();
    assert!(matches!(err, FabricRegistryError::FileTypes(_)), "{err:?}");
    let message = err.to_string();
    assert!(message.starts_with("154 of the 156 file media types of manifest v13 could not be loaded"), "{message}");
    assert!(message.contains("media:enc=utf8;ext=csv;fmt=csv;list;record: Network access blocked"), "{message}");
    assert!(!message.contains("media:enc=utf8;ext=txt;plain-text:"), "a cached type is not reported: {message}");
    assert_eq!(served.load(Ordering::SeqCst), 0);

    registry.set_offline(false);
    assert_eq!(urns(&registry, "csv").await, vec!["media:enc=utf8;ext=csv;fmt=csv;list;record"]);
}

// ---------------------------------------------------------------------------
// What a file is
// ---------------------------------------------------------------------------

use capdag::{
    detect_file_media, extension_media, CartridgeAdapterInvoker, InputResolverError,
    MediaAdapterRegistry, NoAdapters, NoInvoker, ValueAdapterRegistry,
};
use std::path::{Path, PathBuf};

fn write(dir: &tempfile::TempDir, name: &str, content: &str) -> PathBuf {
    let path = dir.path().join(name);
    std::fs::write(&path, content).unwrap();
    path
}

/// A registry over the fixture whose cache holds what a machine that has run
/// a model download and an OCR cap holds.
async fn warm_machine_registry() -> FabricRegistry {
    let (anchor, _) = serve(&definitions()).await;
    registry(
        &anchor,
        &[
            "media:download-result;enc=utf8;fmt=json;record",
            "media:digitized;enc=utf8;ext=txt;plain-text",
        ],
    )
}

/// One file of each common kind, its content, and the media a host with no
/// cartridge to ask takes it as.
const NO_ADAPTER_TABLE: &[(&str, &str, &str)] = &[
    ("notes.txt", "A line of text.\n", "media:enc=utf8;ext=txt;plain-text"),
    ("people.csv", "name,age\nann,30\n", "media:enc=utf8;ext=csv;fmt=csv;list;record"),
    ("rows.tsv", "name\tage\nann\t30\n", "media:enc=utf8;ext=tsv;fmt=tsv;list;record"),
    ("config.yaml", "name: ann\nage: 30\n", "media:enc=utf8;ext=yaml;fmt=yaml"),
    ("config.yml", "name: ann\n", "media:enc=utf8;ext=yaml;fmt=yaml"),
    ("a.json", "{\"name\": \"ann\"}", "media:enc=utf8;ext=json;fmt=json"),
    ("events.ndjson", "{\"a\":1}\n{\"a\":2}\n", "media:enc=utf8;ext=ndjson;fmt=ndjson"),
    ("events.jsonl", "{\"a\":1}\n", "media:enc=utf8;ext=ndjson;fmt=ndjson"),
    ("data.xml", "<a><b/></a>", "media:enc=utf8;ext=xml;fmt=xml"),
    ("config.toml", "a = 1\n", "media:enc=utf8;ext=toml"),
    ("README.md", "# Title\n", "media:enc=utf8;ext=md;plain-text"),
    ("run.log", "started\n", "media:enc=utf8;ext=log;plain-text"),
    ("page.html", "<html></html>", "media:enc=utf8;ext=html"),
    ("notebook.ipynb", "{\"cells\": []}", "media:enc=utf8;ext=jupyter;record"),
    ("report.pdf", "%PDF-1.4\n", "media:document;ext=pdf"),
    ("letter.docx", "PK", "media:document;ext=docx"),
    ("sheet.xlsx", "PK", "media:document;ext=xlsx"),
    ("photo.png", "\u{89}PNG", "media:ext=png;image"),
    ("photo.jpg", "\u{ff}\u{d8}", "media:ext=jpeg;image"),
    ("song.mp3", "ID3", "media:audio;ext=mp3"),
    ("clip.mp4", "ftyp", "media:ext=mp4;video"),
    ("bundle.zip", "PK", "media:archive;ext=zip"),
];

async fn primary_without_adapters(registry: &FabricRegistry, path: &Path) -> String {
    detect_file_media(path, registry, &NoAdapters, &NoInvoker, &ValueAdapterRegistry::new())
        .await
        .map(|found| found.primary().to_string())
        .unwrap_or_else(|e| format!("error: {e}"))
}

// TEST7181: with no cartridge to ask (the capdag CLI, a host before its
// cartridges attach), each common extension is the file type the fabric
// declares for it: a .txt is plain text, not digitized text; a .json is
// JSON, not a model download result; .csv, .tsv, .yaml and .yml are known;
// a refinement nothing proves (a JSON record, digitized text, a filing
// document) is never claimed
#[tokio::test]
async fn test7181_common_extensions_are_their_file_types_without_adapters() {
    let registry = warm_machine_registry().await;
    let dir = tempfile::tempdir().unwrap();
    let mut wrong = Vec::new();
    for (name, content, expected) in NO_ADAPTER_TABLE {
        let path = write(&dir, name, content);
        let got = primary_without_adapters(&registry, &path).await;
        if got != *expected {
            wrong.push(format!("{name}: {got}, expected {expected}"));
        }
    }
    assert!(wrong.is_empty(), "wrongly identified:\n  {}", wrong.join("\n  "));

    let unknown = write(&dir, "thing.zzzz", "x");
    let err = detect_file_media(&unknown, &registry, &NoAdapters, &NoInvoker, &ValueAdapterRegistry::new())
        .await
        .unwrap_err();
    assert!(
        matches!(&err, InputResolverError::UnknownExtension { extension, .. } if extension == "zzzz"),
        "{err}"
    );
    let bare = write(&dir, "Makefile", "all:\n");
    let err = detect_file_media(&bare, &registry, &NoAdapters, &NoInvoker, &ValueAdapterRegistry::new())
        .await
        .unwrap_err();
    assert!(matches!(err, InputResolverError::NoExtension(_)), "{err}");
}

/// The adapter URNs datacartridge and txtcartridge declare, and what each
/// claims the table's files are (their adapters' answers for that content).
struct BundledAdapters;

#[async_trait::async_trait]
impl CartridgeAdapterInvoker for BundledAdapters {
    async fn invoke_adapter_selection(
        &self,
        cartridge_id: &str,
        file_path: &Path,
    ) -> Result<Option<Vec<String>>, InputResolverError> {
        let name = file_path.file_name().unwrap().to_str().unwrap();
        let claims: &[&str] = match (cartridge_id, name) {
            ("datacartridge", "record.json") => {
                &["media:enc=utf8;ext=json;fmt=json;record", "media:enc=utf8;ext=json;fmt=json"]
            }
            ("datacartridge", "rows.json") => &[
                "media:enc=utf8;ext=json;fmt=json;list;record",
                "media:enc=utf8;ext=json;fmt=json;list",
                "media:enc=utf8;ext=json;fmt=json",
            ],
            ("datacartridge", "people.csv") => &["media:enc=utf8;ext=csv;fmt=csv;list;record"],
            ("txtcartridge", "notes.txt") => &["media:enc=utf8;ext=txt;plain-text"],
            (cartridge, file) => panic!("{cartridge} was not expected to be asked about {file}"),
        };
        Ok(Some(claims.iter().map(|c| c.to_string()).collect()))
    }
}

fn bundled_adapter_registry(fabric: Arc<FabricRegistry>) -> MediaAdapterRegistry {
    let mut adapters = MediaAdapterRegistry::new(fabric);
    let data: Vec<String> = [
        "media:enc=utf8;fmt=json",
        "media:enc=utf8;fmt=ndjson",
        "media:enc=utf8;fmt=csv",
        "media:ext=tsv",
        "media:ext=psv",
        "media:enc=utf8;fmt=yaml",
        "media:ext=xml",
        "media:ext=toml",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    adapters.register_cap_group("data-formats", &data, "datacartridge").unwrap();
    let text: Vec<String> = ["media:plain-text", "media:ext=html"].iter().map(|s| s.to_string()).collect();
    adapters.register_cap_group("text-formats", &text, "txtcartridge").unwrap();
    adapters
}

// TEST7182: with the bundled cartridges' adapters (a desktop client), what a
// cartridge claims proves a refinement: a JSON object is a JSON record, an
// array of objects a list of records, and a .txt the cartridge reads as
// plain text is plain text, never digitized text
#[tokio::test]
async fn test7182_cartridge_claims_prove_refinements() {
    let fabric = Arc::new(warm_machine_registry().await);
    let adapters = bundled_adapter_registry(Arc::clone(&fabric));
    let dir = tempfile::tempdir().unwrap();
    let detect = |path: PathBuf| {
        let fabric = Arc::clone(&fabric);
        let adapters = &adapters;
        async move {
            detect_file_media(&path, &fabric, adapters, &BundledAdapters, &ValueAdapterRegistry::new())
                .await
                .unwrap()
        }
    };

    let found = detect(write(&dir, "record.json", "{\"name\": \"ann\"}")).await;
    assert_eq!(found.media, vec!["media:enc=utf8;ext=json;fmt=json;record"]);

    let found = detect(write(&dir, "rows.json", "[{\"name\": \"ann\"}]")).await;
    assert_eq!(found.media, vec!["media:enc=utf8;ext=json;fmt=json;list;record"]);

    let found = detect(write(&dir, "people.csv", "name\nann\n")).await;
    assert_eq!(found.media, vec!["media:enc=utf8;ext=csv;fmt=csv;list;record"]);

    // pip's requirements files claim .txt too, and no cartridge speaks for
    // them, so a .txt is offered as either; a host that cannot ask takes the
    // plain text, the more specific.
    let found = detect(write(&dir, "notes.txt", "A line of text.\n")).await;
    assert_eq!(
        found.media,
        vec!["media:enc=utf8;ext=requirements", "media:enc=utf8;ext=txt;plain-text"]
    );
    assert_eq!(found.primary(), "media:enc=utf8;ext=txt;plain-text");
}

// TEST7183: what a file is from its name alone is its extension's base
// types, never a refinement
#[tokio::test]
async fn test7183_a_files_media_by_name_alone_is_its_extensions_base_types() {
    let registry = warm_machine_registry().await;
    assert_eq!(
        extension_media("json", &registry).await.unwrap(),
        vec!["media:enc=utf8;ext=json;fmt=json"]
    );
    let txt = extension_media("TXT", &registry).await.unwrap();
    assert_eq!(txt, vec!["media:enc=utf8;ext=requirements", "media:enc=utf8;ext=txt;plain-text"]);
    assert_eq!(capdag::primary_media(&txt).unwrap(), Some("media:enc=utf8;ext=txt;plain-text"));
    assert!(matches!(
        extension_media("zzzz", &registry).await,
        Err(InputResolverError::UnknownExtension { .. })
    ));
}

// TEST7184: a refinement whose declared validation rules the content meets
// stands without a cartridge; one whose rules it fails falls; a definition
// with an invalid pattern is an error naming it
#[tokio::test]
async fn test7184_validation_rules_prove_or_disprove_a_refinement() {
    let script = |urn: &str, pattern: &str| {
        serde_json::from_value::<StoredMediaDef>(serde_json::json!({
            "urn": urn, "version": 1, "media_type": "text/plain", "title": "t", "extensions": ["txt"],
            "validation": {"pattern": pattern}
        }))
        .unwrap()
    };
    let base: StoredMediaDef = serde_json::from_value(serde_json::json!({
        "urn": "media:enc=utf8;ext=txt;plain-text", "version": 1, "media_type": "text/plain",
        "title": "Plain Text", "extensions": ["txt"]
    }))
    .unwrap();
    let registry = FabricRegistry::new_for_test();
    registry.insert_cached_media_def_for_test(base);
    registry.insert_cached_media_def_for_test(script("media:enc=utf8;ext=txt;plain-text;shebang", "^#!"));
    let dir = tempfile::tempdir().unwrap();
    let media = |name: &str, content: &str| {
        let path = write(&dir, name, content);
        let registry = &registry;
        async move {
            detect_file_media(&path, registry, &NoAdapters, &NoInvoker, &ValueAdapterRegistry::new())
                .await
                .map(|f| f.media)
        }
    };
    assert_eq!(
        media("run.txt", "#!/bin/sh\n").await.unwrap(),
        vec!["media:enc=utf8;ext=txt;plain-text;shebang"]
    );
    assert_eq!(media("prose.txt", "hello\n").await.unwrap(), vec!["media:enc=utf8;ext=txt;plain-text"]);

    let broken = FabricRegistry::new_for_test();
    broken.insert_cached_media_def_for_test(script("media:broken;enc=utf8;ext=txt;plain-text", "(("));
    let path = write(&dir, "x.txt", "x");
    let err = detect_file_media(&path, &broken, &NoAdapters, &NoInvoker, &ValueAdapterRegistry::new())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("media:broken;enc=utf8;ext=txt;plain-text"), "{err}");
}
