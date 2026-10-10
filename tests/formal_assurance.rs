//! TEST12597: every function of the proved model this crate calls carries a proved claim.
//!
//! The model's package carries what is proved of each function it exports (its assurance
//! document, generated from ../formal): each one decides, equals or keeps what its claim says,
//! and none rests on an assumption about the host — the model needs none. A function exported
//! without a proved claim, or a claim resting on `sorry`, fails here and in `lungo generate`.

use capdag::formal::__meta;

#[test]
fn test12597_every_model_function_carries_a_proved_claim() {
    let a = __meta::assurance();
    assert!(a.facilities.is_empty() && a.assumptions.is_empty(), "the model assumes nothing of the host");
    let declarations = __meta::declarations();
    assert!(!declarations.is_empty());
    for d in declarations {
        assert!(!d.assurance.claims.is_empty(), "{} carries no claim", d.lean_name);
        assert!(d.assurance.assumptions.is_empty(), "{} rests on {:?}", d.lean_name, d.assurance.assumptions);
        for name in d.assurance.claims {
            let claim = a.claim(name).unwrap();
            assert_eq!(claim.status, lungo::ClaimStatus::Proved, "{name}");
            assert!(claim.subjects.contains(&d.lean_name), "{name} is about {}", d.lean_name);
        }
    }
    // Routing is decided exactly as the model specifies it.
    let dispatch = a.claim("CapDAG.Exec.dispatch_decides").unwrap();
    assert_eq!((dispatch.relation, dispatch.specifications), ("lungo.decides", &["CapDAG.serves"][..]));
}
