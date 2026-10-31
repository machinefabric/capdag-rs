//! Charters, verified as a client does, against real signed fixtures.
//!
//! The fixtures are produced by sdx (`capdag-signing-fixtures`): charters of
//! each kind signed by a release key that roots A and B certified for `prod`,
//! a lab charter with an expiry of its own, and the cartridges charter signed
//! again under a `staging` certificate from the same roots. Every refusal
//! below starts from a document that verifies, changes one thing, and checks
//! that the one thing is what is refused.

use super::*;
use crate::bifaci::registry_verdict::ChainFailureReason;
use crate::bifaci::trust::{charter_rules, charter_trust_from, CHARTER_ENVELOPE_FORMAT, CHARTER_LABEL};
use signed_release::{store, ChainError, Charter, CharterError};

const ROOT_A: &str = include_str!("../../tests/fixtures/nocommit/signing/root_a.pubkey");
const ROOT_B: &str = include_str!("../../tests/fixtures/nocommit/signing/root_b.pubkey");
const ROOT_C: &str = include_str!("../../tests/fixtures/nocommit/signing/root_c.pubkey");
const META: &str = include_str!("../../tests/fixtures/nocommit/signing/meta.json");
const CARTRIDGES: &[u8] = include_bytes!("../../tests/fixtures/nocommit/signing/charter-cartridges.json");
const CARTRIDGES_SIG: &[u8] = include_bytes!("../../tests/fixtures/nocommit/signing/charter-cartridges.json.sig");
const CARTRIDGES_STAGING_SIG: &[u8] = include_bytes!("../../tests/fixtures/nocommit/signing/charter-cartridges.staging-cert.json.sig");
const FABRIC: &[u8] = include_bytes!("../../tests/fixtures/nocommit/signing/charter-fabric.json");
const FABRIC_SIG: &[u8] = include_bytes!("../../tests/fixtures/nocommit/signing/charter-fabric.json.sig");
const LAB: &[u8] = include_bytes!("../../tests/fixtures/nocommit/signing/charter-lab.json");
const LAB_SIG: &[u8] = include_bytes!("../../tests/fixtures/nocommit/signing/charter-lab.json.sig");
const MANIFEST: &[u8] = include_bytes!("../../tests/fixtures/nocommit/signing/manifest.json");
const MANIFEST_SIG: &[u8] = include_bytes!("../../tests/fixtures/nocommit/signing/manifest.json.sig");

const CARTRIDGES_ANCHOR: &str = "https://registry.example/cartridges";
const LAB_ANCHOR: &str = "https://lab.example/exp/t1/cartridges";

#[derive(serde::Deserialize)]
struct Meta {
    issued_at: u64,
    not_after: u64,
    lab_charter_not_after: u64,
}

fn meta() -> Meta {
    serde_json::from_str(META).expect("meta.json parses")
}

/// A moment inside every fixture's validity: after issue, before the lab
/// charter's own expiry (which is the earliest).
fn now() -> u64 {
    meta().issued_at + 86_400
}

fn trust() -> CharterTrust {
    charter_trust_from(&[ROOT_A, ROOT_B, ROOT_C]).expect("the fixture roots are keys")
}

fn expect<'a>(anchor: &'a str, kind: RegistryKind, environment: Option<&'a str>) -> CharterExpectation<'a> {
    CharterExpectation { anchor: Some(anchor), kind: kind.wire_name(), environment }
}

/// TEST12910: a charter signed by the prod key verifies, and everything a
/// client needs — the slug, the manifest's location, the fabric — is read
/// from it.
#[test]
fn test12910_a_prod_signed_charter_verifies_and_states_the_registry() {
    let verified = verify(
        CARTRIDGES,
        CARTRIDGES_SIG,
        &trust(),
        &expect(CARTRIDGES_ANCHOR, RegistryKind::Cartridges, Some("prod")),
        now(),
    )
    .expect("the cartridges charter verifies");
    let charter = &verified.charter;
    assert_eq!(charter.slug, "example-cartridges");
    assert_eq!(fabric_of(charter), Some("https://registry.example/fabric"));
    assert_eq!(
        charter.location("manifest", &[("registry_version", "4")]).unwrap(),
        "https://registry.example/cartridges/v4/manifest"
    );
    assert_eq!(verified.body, CARTRIDGES, "the exact bytes are kept, to be stored and re-verified");
}

/// TEST12911: one byte of the charter changed and it is refused at its
/// signature, before anything in it is believed.
#[test]
fn test12911_a_tampered_charter_is_refused_at_its_signature() {
    let tampered = String::from_utf8(CARTRIDGES.to_vec())
        .unwrap()
        .replace("example-cartridges", "examplf-cartridges");
    assert_ne!(tampered.as_bytes(), CARTRIDGES);
    let err = verify(
        tampered.as_bytes(),
        CARTRIDGES_SIG,
        &trust(),
        &expect(CARTRIDGES_ANCHOR, RegistryKind::Cartridges, Some("prod")),
        now(),
    )
    .unwrap_err();
    assert!(matches!(err, CharterError::SignatureInvalid(_)), "{err}");
    assert_eq!(ChainFailureReason::from(err.reason()), ChainFailureReason::CharterSignatureInvalid);
}

/// TEST12912: a valid charter served under another anchor is refused — a
/// registry cannot borrow another's charter.
#[test]
fn test12912_a_charter_for_another_anchor_is_refused() {
    let err = verify(
        CARTRIDGES,
        CARTRIDGES_SIG,
        &trust(),
        &expect("https://registry.example/elsewhere", RegistryKind::Cartridges, Some("prod")),
        now(),
    )
    .unwrap_err();
    assert!(matches!(err, CharterError::AnchorMismatch { .. }), "{err}");
}

/// TEST12913: a charter of one kind does not stand for another kind.
#[test]
fn test12913_a_charter_of_another_kind_is_refused() {
    let err = verify(
        FABRIC,
        FABRIC_SIG,
        &trust(),
        &expect("https://registry.example/fabric", RegistryKind::Cartridges, None),
        now(),
    )
    .unwrap_err();
    assert!(matches!(err, CharterError::KindMismatch { .. }), "{err}");
}

/// TEST12914: a charter whose certificate is for staging is refused even
/// though the same roots signed it: only prod charters registries.
#[test]
fn test12914_only_a_prod_certificate_can_sign_a_charter() {
    let err = verify(
        CARTRIDGES,
        CARTRIDGES_STAGING_SIG,
        &trust(),
        &expect(CARTRIDGES_ANCHOR, RegistryKind::Cartridges, Some("prod")),
        now(),
    )
    .unwrap_err();
    match err {
        CharterError::Chain(ChainError::LabelMismatch { certificate, trusted, .. }) => {
            assert_eq!(certificate, "staging");
            assert_eq!(trusted, CHARTER_LABEL);
        }
        other => panic!("expected the certificate's label to be refused, got {other}"),
    }
}

/// TEST12915: once the prod certificate has expired, a charter it signed no
/// longer verifies — which is why every charter is re-signed at rotation.
#[test]
fn test12915_a_charter_dies_with_its_certificate() {
    let err = verify(
        FABRIC,
        FABRIC_SIG,
        &trust(),
        &expect("https://registry.example/fabric", RegistryKind::Fabric, None),
        meta().not_after + 1,
    )
    .unwrap_err();
    assert!(matches!(err, CharterError::Chain(ChainError::ExpiredCertificate { .. })), "{err}");
}

/// TEST12916: a lab charter carries its own expiry, earlier than its
/// certificate's: valid the moment before, refused the moment after.
#[test]
fn test12916_a_lab_charter_expires_on_its_own() {
    let m = meta();
    assert!(m.lab_charter_not_after < m.not_after, "the fixture must separate the two expiries");
    let at = |moment| {
        verify(LAB, LAB_SIG, &trust(), &expect(LAB_ANCHOR, RegistryKind::Cartridges, Some("lab")), moment)
    };
    let verified = at(m.lab_charter_not_after).expect("valid up to its own not_after");
    assert_eq!(verified.charter.experiment.as_deref(), Some("t1"));
    let err = at(m.lab_charter_not_after + 1).unwrap_err();
    assert!(matches!(err, CharterError::Expired { .. }), "{err}");
}

/// TEST12917: a registry whose contents are signed under another environment
/// is refused at its charter — a prod build never takes a lab registry.
#[test]
fn test12917_a_registry_of_another_environment_is_refused_at_its_charter() {
    let err = verify(LAB, LAB_SIG, &trust(), &expect(LAB_ANCHOR, RegistryKind::Cartridges, Some("prod")), now())
        .unwrap_err();
    assert!(matches!(err, CharterError::EnvironmentMismatch { .. }), "{err}");
    assert_eq!(ChainFailureReason::from(err.reason()), ChainFailureReason::CharterEnvironmentMismatch);
}

/// TEST12918: a prod-signed MANIFEST cannot be passed off as a charter: its
/// envelope is refused by format, and re-labelled as a charter's, its body is
/// refused by its own format.
#[test]
fn test12918_a_signed_manifest_is_not_a_charter() {
    let expectation = expect(CARTRIDGES_ANCHOR, RegistryKind::Cartridges, Some("prod"));
    let err = verify(MANIFEST, MANIFEST_SIG, &trust(), &expectation, now()).unwrap_err();
    assert!(matches!(err, CharterError::Chain(ChainError::UnsupportedEnvelopeFormat { .. })), "{err}");

    let mut relabelled: serde_json::Value = serde_json::from_slice(MANIFEST_SIG).unwrap();
    let object = relabelled.as_object_mut().unwrap();
    object.insert("format".into(), CHARTER_ENVELOPE_FORMAT.into());
    let signature = object.remove("manifest_signature").unwrap();
    object.insert("charter_signature".into(), signature);
    let err = verify(MANIFEST, relabelled.to_string().as_bytes(), &trust(), &expectation, now()).unwrap_err();
    assert!(
        matches!(err, CharterError::UnsupportedFormat { .. } | CharterError::Malformed(_)),
        "the signature holds over the manifest's bytes, so the body itself must refuse: {err}"
    );
}

/// TEST12919: a location is filled with exactly the placeholders it takes —
/// one missing, or one it does not take, is refused rather than guessed.
#[test]
fn test12919_a_location_takes_exactly_its_placeholders() {
    let fabric = verify(
        FABRIC,
        FABRIC_SIG,
        &trust(),
        &expect("https://registry.example/fabric", RegistryKind::Fabric, None),
        now(),
    )
    .unwrap()
    .charter;
    assert_eq!(
        fabric.location("definition", &[("kind", "caps"), ("urn_hash", "ab12"), ("version", "3")]).unwrap(),
        "https://registry.example/fabric/caps/ab12/3.json"
    );
    assert!(fabric.location("definition", &[("kind", "caps"), ("urn_hash", "ab12")]).is_err());
    assert!(fabric.location("latest", &[("version", "3")]).is_err());
    assert!(fabric.location("schema", &[]).is_err(), "a fabric charter has no schema location");
}

/// TEST12920: the shape every charter must have. Each case is the fixture's
/// body with one thing wrong; the unchanged body passes, so each refusal is
/// about the one change.
#[test]
fn test12920_a_charter_out_of_shape_is_malformed() {
    let base = Charter::parse(CARTRIDGES, charter_rules()).unwrap();
    base.validate(charter_rules()).expect("the fixture is in shape");
    let refused = |change: &dyn Fn(&mut Charter), why: &str| {
        let mut c = base.clone();
        change(&mut c);
        assert!(c.validate(charter_rules()).is_err(), "{why} was accepted");
    };
    refused(&|c| c.anchor = "http://registry.example/cartridges".into(), "an http anchor");
    refused(&|c| c.anchor.push('/'), "an anchor ending in /");
    refused(&|c| c.slug = DEV_SLUG.into(), "the reserved dev slug");
    refused(&|c| c.slug = "Has_Upper".into(), "a slug outside [a-z0-9.-]");
    refused(&|c| { c.locations.clear(); }, "no manifest location");
    refused(
        &|c| { c.locations.insert("manifest".into(), "https://r.example/manifest".into()); },
        "a manifest location without {registry_version}",
    );
    refused(
        &|c| { c.locations.insert("manifest".into(), "https://r.example/v{registry_version}/{app_id}".into()); },
        "a placeholder the location does not take",
    );
    refused(&|c| { c.locations.insert("latest".into(), "https://r.example/latest".into()); }, "a location of another kind");
    refused(&|c| { c.fields.insert("fabric".into(), None); }, "a cartridges registry with no fabric");
    refused(&|c| c.experiment = Some("Not An Id".into()), "an experiment that is not an id");
    refused(&|c| c.not_after = Some(c.issued_at), "a not_after that is not after issue");
}

/// TEST12921: a stored charter proves its folder only under its own slug: a
/// folder renamed after install is refused, though the charter verifies.
#[test]
fn test12921_a_stored_charter_holds_its_folder_to_its_slug() {
    let verified = verify(
        CARTRIDGES,
        CARTRIDGES_SIG,
        &trust(),
        &expect(CARTRIDGES_ANCHOR, RegistryKind::Cartridges, Some("prod")),
        now(),
    )
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let folder = root.path().join("example-cartridges");
    store(&verified, &folder).unwrap();
    let opened = verify_stored(&folder, &trust(), RegistryKind::Cartridges, Some("prod"), now())
        .expect("a stored charter re-verifies offline");
    assert_eq!(opened.charter.anchor, CARTRIDGES_ANCHOR);

    let renamed = root.path().join("cartridges.machinefabric.com");
    std::fs::rename(&folder, &renamed).unwrap();
    let err = verify_stored(&renamed, &trust(), RegistryKind::Cartridges, Some("prod"), now()).unwrap_err();
    assert!(
        matches!(err, StoredCharterError::Refused(CharterError::FolderMismatch { .. })),
        "{err}"
    );
}

/// TEST12922: a registry folder is opened through its charter, and found by
/// its anchor — never by its name. `dev` is the one folder with no charter.
#[test]
fn test12922_registry_folders_are_found_by_their_charters() {
    let root = tempfile::tempdir().unwrap();
    for (body, sig, anchor, env) in [
        (CARTRIDGES, CARTRIDGES_SIG, CARTRIDGES_ANCHOR, "prod"),
        (LAB, LAB_SIG, LAB_ANCHOR, "lab"),
    ] {
        let verified =
            verify(body, sig, &trust(), &expect(anchor, RegistryKind::Cartridges, Some(env)), now()).unwrap();
        store(&verified, &root.path().join(&verified.charter.slug)).unwrap();
    }
    std::fs::create_dir_all(root.path().join(DEV_SLUG)).unwrap();

    let (folder, found) = find_registry_folder(
        root.path(),
        CARTRIDGES_ANCHOR,
        &trust(),
        RegistryKind::Cartridges,
        Some("prod"),
        now(),
    )
    .expect("the prod registry's folder is found by its anchor");
    assert_eq!(folder, root.path().join("example-cartridges"));
    assert_eq!(found.charter.slug, "example-cartridges");
    assert!(
        find_registry_folder(root.path(), LAB_ANCHOR, &trust(), RegistryKind::Cartridges, Some("prod"), now()).is_none(),
        "a lab registry's folder is not a prod build's"
    );

    let dev = RegistryFolder::open(&root.path().join(DEV_SLUG), &trust(), Some("prod"), now()).unwrap();
    assert!(matches!(dev, RegistryFolder::Dev));
    assert_eq!(dev.anchor(), None);
    let chartered =
        RegistryFolder::open(&root.path().join("example-cartridges"), &trust(), Some("prod"), now()).unwrap();
    assert_eq!(chartered.anchor(), Some(CARTRIDGES_ANCHOR));
}
