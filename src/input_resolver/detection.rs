//! What a file is: the one detection every host runs.
//!
//! A file's extension names the file media types the fabric declares for it
//! (its candidates: every definition in the pinned manifest that carries the
//! `ext=` file-type tag and lists the extension,
//! [`FabricRegistry::file_media_for_extension`]). Its content decides between
//! them, by the proved model (`capdag/formal/CapDAG/Detect.lean`, generated
//! into `crate::formal::detect`):
//!
//! - a candidate whose definition declares validation rules the content fails
//!   never stands;
//! - a candidate under a cartridge's content-inspection adapter stands only if
//!   a consulted cartridge claims the file as it or as something more specific
//!   (the adapter speaks for its whole domain);
//! - any other candidate stands if the content met rules it declares, or if
//!   it is a base type of the extension: no other candidate is more general.
//!   A refinement (digitized text of a `.txt`, a JSON record of a `.json`) is
//!   a claim about the content, and an unproven claim is never made.
//!
//! What stands is refined by the value adapters; each line of refinement
//! then keeps only its most specific member. A `list` tag stays: it says
//! what the content is (a CSV file is a list of records), never how many
//! values are in transit, which is the planner's `is_sequence`. What is left is what the file is; more than one
//! is a genuine alternative a host may offer. [`primary_media`] picks the one
//! a host that cannot ask uses.
//!
//! What this module does itself is the I/O the model is handed: reading the
//! content, evaluating each definition's rules against it, and asking the
//! cartridges.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::formal::detect as model;
use crate::input_resolver::adapter::{CartridgeAdapterInvoker, MAX_CONTENT_INSPECTION_BYTES};
use crate::input_resolver::adapters::MediaAdapterRegistry;
use crate::input_resolver::value_adapter_registry::ValueAdapterRegistry;
use crate::input_resolver::InputResolverError;
use crate::media::registry::FabricRegistry;
use crate::media::spec::MediaValidation;
use crate::urn::media_urn::MediaUrn;

/// One file media type a file's extension names, with the validation rules
/// its definition declares.
#[derive(Debug, Clone)]
pub struct FileMediaCandidate {
    pub urn: MediaUrn,
    pub validation: Option<MediaValidation>,
}

/// The cartridges whose content-inspection adapters cover some of a file's
/// candidates, and what each of them claims the file is.
#[derive(Debug, Clone, Default)]
pub struct AdapterVerdict {
    /// The adapter URNs of every cartridge consulted.
    pub owners: Vec<MediaUrn>,
    /// Every media URN a consulted cartridge claimed the file as.
    pub claimed: Vec<MediaUrn>,
}

/// Where a host's content-inspection adapters come from: which cartridges
/// to consult for a file whose candidates these are, as
/// `(cartridge id, adapter URN)` pairs.
pub trait AdapterSource: Send + Sync {
    fn adapters_for(&self, candidates: &[MediaUrn]) -> Vec<(String, MediaUrn)>;
}

/// A host with no content-inspection adapters.
pub struct NoAdapters;

impl AdapterSource for NoAdapters {
    fn adapters_for(&self, _candidates: &[MediaUrn]) -> Vec<(String, MediaUrn)> {
        Vec::new()
    }
}

impl AdapterSource for MediaAdapterRegistry {
    fn adapters_for(&self, candidates: &[MediaUrn]) -> Vec<(String, MediaUrn)> {
        self.adapters_for_candidates(candidates)
    }
}

impl AdapterSource for std::sync::RwLock<MediaAdapterRegistry> {
    fn adapters_for(&self, candidates: &[MediaUrn]) -> Vec<(String, MediaUrn)> {
        self.read()
            .expect("the adapter registry's lock is poisoned")
            .adapters_for_candidates(candidates)
    }
}

/// A host that asks no cartridge: it has none to ask.
pub struct NoInvoker;

#[async_trait::async_trait]
impl CartridgeAdapterInvoker for NoInvoker {
    async fn invoke_adapter_selection(
        &self,
        cartridge_id: &str,
        _file_path: &Path,
    ) -> Result<Option<Vec<String>>, InputResolverError> {
        unreachable!("a host with no adapters was asked to consult cartridge '{cartridge_id}'")
    }
}

/// What a file is.
#[derive(Debug, Clone)]
pub struct FileMedia {
    pub path: PathBuf,
    pub size_bytes: u64,
    /// The file's extension, lowercased.
    pub extension: String,
    /// Every file media type the fabric declares for the extension.
    pub candidates: Vec<String>,
    /// What the file is: one entry, or several genuine alternatives, in the
    /// candidates' order.
    pub media: Vec<String>,
}

impl FileMedia {
    /// The one media a host that cannot ask uses ([`primary_media`]).
    pub fn primary(&self) -> &str {
        primary_media(&self.media)
            .expect("every media of a FileMedia is a media URN it parsed")
            .expect("a FileMedia always holds at least one media")
    }
}

/// Does the content meet the rules? An invalid pattern is a defect of the
/// definition and an error naming it.
fn meets(content: &[u8], urn: &MediaUrn, rules: &MediaValidation) -> Result<bool, String> {
    let text = std::str::from_utf8(content).ok();
    if let Some(pattern) = &rules.pattern {
        let re = regex::Regex::new(pattern).map_err(|e| {
            format!("the media definition '{urn}' declares an invalid validation pattern '{pattern}': {e}")
        })?;
        match text {
            Some(t) if re.is_match(t) => {}
            _ => return Ok(false),
        }
    }
    if let Some(min) = rules.min_length {
        if content.len() < min {
            return Ok(false);
        }
    }
    if let Some(max) = rules.max_length {
        if content.len() > max {
            return Ok(false);
        }
    }
    if let Some(allowed) = &rules.allowed_values {
        match text {
            Some(t) if allowed.iter().any(|v| v == t.trim()) => {}
            _ => return Ok(false),
        }
    }
    if rules.min.is_some() || rules.max.is_some() {
        let Some(value) = text.and_then(|t| t.trim().parse::<f64>().ok()) else {
            return Ok(false);
        };
        if rules.min.is_some_and(|min| value < min) || rules.max.is_some_and(|max| value > max) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn declares_rules(rules: &Option<MediaValidation>) -> bool {
    rules.as_ref().is_some_and(|r| {
        r.pattern.is_some()
            || r.min_length.is_some()
            || r.max_length.is_some()
            || r.allowed_values.is_some()
            || r.min.is_some()
            || r.max.is_some()
    })
}

/// What a candidate's rules say of the content, as the model takes it.
fn rules_verdict(content: &[u8], c: &FileMediaCandidate) -> Result<model::Rules, String> {
    match c.validation.as_ref().filter(|_| declares_rules(&c.validation)) {
        None => Ok(model::Rules::Unruled),
        Some(rules) => Ok(if meets(content, &c.urn, rules)? {
            model::Rules::Met
        } else {
            model::Rules::Unmet
        }),
    }
}

fn formal(urn: &MediaUrn) -> ::tagged_urn::FormalUrn {
    urn.inner().formal().clone()
}

/// The candidates that stand (the module's rule, decided by the model's
/// `stands`), in the candidates' order. An invalid validation pattern is an
/// error naming its definition.
pub fn surviving_candidates(
    content: &[u8],
    candidates: &[FileMediaCandidate],
    verdict: &AdapterVerdict,
) -> Result<Vec<MediaUrn>, String> {
    let mut modelled: Vec<model::Candidate> = Vec::with_capacity(candidates.len());
    for c in candidates {
        modelled.push(model::Candidate { urn: formal(&c.urn), rules: rules_verdict(content, c)? });
    }
    let all: lungo::List<model::Candidate> = modelled.iter().cloned().collect();
    let owners: lungo::List<::tagged_urn::FormalUrn> = verdict.owners.iter().map(formal).collect();
    let claims: lungo::List<::tagged_urn::FormalUrn> = verdict.claimed.iter().map(formal).collect();
    Ok(candidates
        .iter()
        .zip(modelled)
        .filter(|(_, m)| model::stands(all.clone(), owners.clone(), claims.clone(), m.clone()))
        .map(|(c, _)| c.urn.clone())
        .collect())
}

/// Refine what stands through the value adapters and keep only the most
/// specific member of each line of refinement (the model's
/// `mostSpecific`), in order.
pub fn settle(
    survivors: &[MediaUrn],
    value_adapters: &ValueAdapterRegistry,
    content: &[u8],
) -> Result<Vec<String>, String> {
    let text = std::str::from_utf8(content).unwrap_or("");
    let mut refined: Vec<MediaUrn> = Vec::new();
    for urn in survivors {
        let urn_str = value_adapters.refine_media_urn(&urn.to_string(), text);
        let urn = MediaUrn::from_string(&urn_str).map_err(|e| {
            format!("a value adapter refined a file's media to '{urn_str}', which is no media URN: {e}")
        })?;
        if !refined.iter().any(|seen| seen.is_equivalent(&urn).unwrap_or(false)) {
            refined.push(urn);
        }
    }
    let all: lungo::List<::tagged_urn::FormalUrn> = refined.iter().map(formal).collect();
    Ok(refined
        .iter()
        .filter(|u| model::most_specific(all.clone(), formal(u)))
        .map(|u| u.to_string())
        .collect())
}

/// The one media a host that cannot ask uses: the most specific, the first
/// of them on a tie (the model's `primary`). `None` only for an empty list;
/// a member that is no media URN is an error naming it.
pub fn primary_media(media: &[String]) -> Result<Option<&str>, String> {
    let mut all: Vec<::tagged_urn::FormalUrn> = Vec::with_capacity(media.len());
    for m in media {
        let urn = MediaUrn::from_string(m).map_err(|e| format!("'{m}' is no media URN: {e}"))?;
        all.push(formal(&urn));
    }
    Ok(model::primary(all.into_iter().collect()).map(|i| {
        let i = i.to_u64().expect("a position of the list fits in u64") as usize;
        media[i].as_str()
    }))
}

fn lowercase_extension(path: &Path) -> String {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_lowercase())
        .unwrap_or_default()
}

/// The file media types a file with this extension can be, with their
/// validation rules: the fabric's answer, loaded when first asked.
async fn candidates_for(
    path: &Path,
    extension: &str,
    registry: &FabricRegistry,
) -> Result<Vec<FileMediaCandidate>, InputResolverError> {
    if extension.is_empty() {
        return Err(InputResolverError::NoExtension(path.to_path_buf()));
    }
    let defs = registry
        .file_media_for_extension(extension)
        .await
        .map_err(|e| InputResolverError::FabricUnavailable {
            path: path.to_path_buf(),
            reason: e.to_string(),
        })?;
    if defs.is_empty() {
        return Err(InputResolverError::UnknownExtension {
            path: path.to_path_buf(),
            extension: extension.to_string(),
        });
    }
    defs.into_iter()
        .map(|def| {
            let urn = MediaUrn::from_string(&def.urn).map_err(|e| InputResolverError::InspectionFailed {
                path: path.to_path_buf(),
                reason: format!("the fabric's media definition '{}' has no valid URN: {e}", def.urn),
            })?;
            Ok(FileMediaCandidate { urn, validation: def.validation })
        })
        .collect()
}

fn read_prefix(path: &Path) -> Result<(u64, Vec<u8>), InputResolverError> {
    use std::io::Read;
    let io_error = |e: std::io::Error| match e.kind() {
        std::io::ErrorKind::NotFound => InputResolverError::NotFound(path.to_path_buf()),
        std::io::ErrorKind::PermissionDenied => InputResolverError::PermissionDenied(path.to_path_buf()),
        _ => InputResolverError::IoError { path: path.to_path_buf(), error: e },
    };
    let size = std::fs::metadata(path).map_err(io_error)?.len();
    let mut buf = Vec::with_capacity((size as usize).min(MAX_CONTENT_INSPECTION_BYTES));
    std::fs::File::open(path)
        .map_err(io_error)?
        .take(MAX_CONTENT_INSPECTION_BYTES as u64)
        .read_to_end(&mut buf)
        .map_err(io_error)?;
    Ok((size, buf))
}

/// Ask every cartridge whose adapter covers a candidate what the file is.
/// A cartridge that declines, or fails, claims nothing; its domain stays
/// covered, so its candidates fall.
async fn consult(
    path: &Path,
    candidates: &[FileMediaCandidate],
    adapters: &dyn AdapterSource,
    invoker: &dyn CartridgeAdapterInvoker,
) -> AdapterVerdict {
    let urns: Vec<MediaUrn> = candidates.iter().map(|c| c.urn.clone()).collect();
    let pairs = adapters.adapters_for(&urns);
    let mut verdict = AdapterVerdict::default();
    let mut asked: HashSet<String> = HashSet::new();
    for (cartridge_id, adapter_urn) in pairs {
        verdict.owners.push(adapter_urn.clone());
        if !asked.insert(cartridge_id.clone()) {
            continue;
        }
        match invoker.invoke_adapter_selection(&cartridge_id, path).await {
            Ok(Some(claims)) => {
                for claim in claims {
                    match MediaUrn::from_string(&claim) {
                        Ok(urn) => verdict.claimed.push(urn),
                        Err(e) => tracing::error!(
                            target: "capdag::input_resolver",
                            cartridge_id = %cartridge_id,
                            claim = %claim,
                            error = %e,
                            "a cartridge claimed a file as something that is no media URN"
                        ),
                    }
                }
            }
            Ok(None) => {}
            Err(e) => tracing::error!(
                target: "capdag::input_resolver",
                cartridge_id = %cartridge_id,
                adapter_urn = %adapter_urn,
                file = %path.display(),
                error = %e,
                "a cartridge's content inspection failed"
            ),
        }
    }
    verdict
}

/// What a file is, by its extension and its content: the one detection
/// every host runs ([module docs](self)). A host with no cartridges passes
/// [`NoAdapters`] and [`NoInvoker`].
pub async fn detect_file_media(
    path: &Path,
    registry: &FabricRegistry,
    adapters: &dyn AdapterSource,
    invoker: &dyn CartridgeAdapterInvoker,
    value_adapters: &ValueAdapterRegistry,
) -> Result<FileMedia, InputResolverError> {
    let extension = lowercase_extension(path);
    let candidates = candidates_for(path, &extension, registry).await?;
    let (size_bytes, content) = read_prefix(path)?;
    let verdict = consult(path, &candidates, adapters, invoker).await;
    let failed = |reason: String| InputResolverError::InspectionFailed { path: path.to_path_buf(), reason };
    let survivors = surviving_candidates(&content, &candidates, &verdict).map_err(failed)?;
    if survivors.is_empty() {
        return Err(failed(format!(
            "its content is none of the {} media types the fabric declares for .{extension}",
            candidates.len()
        )));
    }
    let media = settle(&survivors, value_adapters, &content).map_err(failed)?;
    Ok(FileMedia {
        path: path.to_path_buf(),
        size_bytes,
        extension,
        candidates: candidates.iter().map(|c| c.urn.to_string()).collect(),
        media,
    })
}

/// What a file with this extension is before its content is read: the
/// extension's base types (no candidate is more general), in the
/// candidates' order — the model's rule with no rules read and no cartridge
/// asked. Hosts that name a file's media from its name alone use this, and
/// [`primary_media`] when they need one.
pub async fn extension_media(
    extension: &str,
    registry: &FabricRegistry,
) -> Result<Vec<String>, InputResolverError> {
    let path = PathBuf::from(format!("*.{extension}"));
    let candidates = candidates_for(&path, &extension.to_lowercase(), registry).await?;
    let unread: Vec<FileMediaCandidate> = candidates
        .into_iter()
        .map(|c| FileMediaCandidate { urn: c.urn, validation: None })
        .collect();
    let bases = surviving_candidates(&[], &unread, &AdapterVerdict::default())
        .expect("candidates with no rules are decided without reading a pattern");
    Ok(bases.iter().map(|u| u.to_string()).collect())
}
