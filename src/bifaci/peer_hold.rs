//! A peer request held while its provider is brought (14.2 §Peer requests no
//! master serves).
//!
//! A cartridge's peer request names a cap. When no master behind the switch
//! serves it, a switch that has a [`PeerProvider`] holds the request instead of
//! refusing it: the REQ and every frame the caller sends for it after are kept,
//! in the order they came, while the provider finds the cartridge that serves
//! the cap, installs it if it is not installed, and hosts it. When a master
//! serves the cap, the held frames are replayed in that order; when the
//! provider cannot be brought, or not in time, the request ends with the ERR
//! the provider words, naming the cartridge and what to do.
//!
//! Every decision about a held request — whether to hold it, what each frame
//! does to the hold, how the hold ends — is the proved model's
//! (`formal/CapDAG/Bifaci/Hold.lean`). This module keeps the held frames and
//! asks it.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::bifaci::frame::{Frame, FrameType, MessageId};

/// Why a held request ends without its provider: the ERR its caller is
/// answered with, in the vocabulary of every ERR (docs/failure-taxonomy.md).
/// The message is for the person who reads the run's failure: it names what is
/// missing and the command that brings it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerRefusal {
    pub code: String,
    pub class: crate::failure::AttributionClass,
    pub message: String,
}

impl PeerRefusal {
    /// The ERR frame answering request `rid` (routing id `xid`) with this
    /// refusal.
    pub(crate) fn err_frame(&self, rid: &MessageId, xid: &MessageId) -> Frame {
        let mut frame = Frame::err(rid.clone(), &self.code, self.class, &self.message, None);
        frame.routing_id = Some(xid.clone());
        frame
    }
}

/// Bringing a provider, once [`PeerProvider::provision`] has found it.
pub struct PeerProvision {
    /// Done when a master behind the switch serves the cap: the cartridge is
    /// installed and its host attached. It runs to its end even when the hold
    /// it was started for does not wait that long, so a provider that took
    /// longer than one request could wait is there for the next.
    pub ready: Pin<Box<dyn Future<Output = Result<(), PeerRefusal>> + Send>>,
    /// What the caller is told when the hold's deadline passes before
    /// `ready` is done: the provider names the cartridge and the command that
    /// installs it outside a run.
    pub on_expiry: PeerRefusal,
}

/// What brings the master that serves a cap no master serves yet. The capdag
/// command line's is the rule a run of a cap follows for its own cartridge:
/// a dev cartridge that declares it, else the registry's, installed when it is
/// not installed (`orchestrator::cli_runtime`).
#[async_trait::async_trait]
pub trait PeerProvider: Send + Sync {
    /// Find what provides `cap_urn` — the cap a cartridge's peer request
    /// named — and start bringing it; or say why nothing can.
    async fn provision(&self, cap_urn: &str) -> Result<PeerProvision, PeerRefusal>;
}

/// How much a hold may keep, and for how long.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerHoldBound {
    /// Frames kept, the REQ among them.
    pub frames: u64,
    /// Payload bytes kept.
    pub bytes: u64,
    /// How long a request is held before it ends with the provider's
    /// `on_expiry`, or a refusal saying nothing was found in time.
    pub deadline: Duration,
}

impl PeerHoldBound {
    fn model(&self) -> crate::formal::bifaci::hold::Bound {
        crate::formal::bifaci::hold::Bound {
            frames: lungo::Nat::from(self.frames),
            bytes: lungo::Nat::from(self.bytes),
        }
    }
}

/// The model's word on a peer request no master serves.
pub(crate) fn holds(can_provide: bool, replayed: bool) -> bool {
    use crate::formal::bifaci::hold::Unrouted;
    match crate::formal::bifaci::hold::unrouted(can_provide, replayed) {
        Unrouted::Hold => true,
        Unrouted::Refuse => false,
    }
}

/// One held request: who asked, the routing id its answers carry, its cap, and
/// its frames in the order they came — the REQ first.
pub(crate) struct HeldRequest {
    pub source_idx: usize,
    pub xid: MessageId,
    pub cap_urn: String,
    pub frames: Vec<Frame>,
    held: crate::formal::bifaci::hold::Held,
}

/// What a frame arriving for a held request did.
pub(crate) enum Arrived {
    /// It was kept.
    Kept,
    /// The hold could not keep it: the request ends with this ERR.
    Overflowed(HeldRequest, PeerRefusal),
    /// The caller cancelled the request: it ends with this ERR.
    Cancelled(HeldRequest, Frame),
}

/// How bringing a held request's provider ended.
pub(crate) enum Settlement {
    Provided,
    Refused(PeerRefusal),
    Expired(PeerRefusal),
}

/// What becomes of a held request when its provider was brought or was not.
pub(crate) enum Released {
    /// Replay these frames, in order: the REQ first.
    Replay(HeldRequest),
    /// Answer its caller with this ERR.
    Fail(HeldRequest, Frame),
}

/// The held requests, by request id.
pub(crate) struct Holds {
    bound: PeerHoldBound,
    held: HashMap<MessageId, HeldRequest>,
}

fn payload_size(frame: &Frame) -> lungo::Nat {
    lungo::Nat::from(frame.payload.as_ref().map_or(0, |p| p.len()) as u64)
}

impl Holds {
    pub fn new(bound: PeerHoldBound) -> Self {
        Self { bound, held: HashMap::new() }
    }

    pub fn bound(&self) -> PeerHoldBound {
        self.bound
    }

    /// Hold `req` — a peer REQ from master `source_idx`, already given its
    /// routing id. `Err` when the REQ alone is more than a hold may keep: the
    /// request ends with that ERR instead.
    pub fn hold(&mut self, source_idx: usize, req: Frame) -> Result<(), Frame> {
        use crate::formal::bifaci::hold::{arrive, held, Arrival};
        let xid = req.routing_id.clone().expect("a held REQ has been given its routing id");
        let cap_urn = req.cap.clone().expect("a REQ names its cap");
        match arrive(self.bound.model(), held::empty(), FrameType::Req.model(), payload_size(&req)) {
            Arrival::Keep { held } => {
                self.held.insert(
                    req.id.clone(),
                    HeldRequest { source_idx, xid, cap_urn, frames: vec![req], held },
                );
                Ok(())
            }
            Arrival::Overflow => Err(self.overflow_refusal(&cap_urn).err_frame(&req.id, &xid)),
            Arrival::Cancel => unreachable!("the model cancels only on a CANCEL frame, and this is a REQ"),
        }
    }

    /// Whether `rid` from master `source_idx` is a held request.
    pub fn is_held(&self, source_idx: usize, rid: &MessageId) -> bool {
        self.held.get(rid).map_or(false, |h| h.source_idx == source_idx)
    }

    /// A frame the caller sent for a held request.
    pub fn arrive(&mut self, frame: Frame) -> Arrived {
        use crate::formal::bifaci::hold::{arrive, Arrival};
        let rid = frame.id.clone();
        let entry = self.held.get_mut(&rid).expect("arrive is asked only of a held request");
        match arrive(self.bound.model(), entry.held.clone(), frame.frame_type.model(), payload_size(&frame)) {
            Arrival::Keep { held } => {
                entry.held = held;
                entry.frames.push(frame);
                Arrived::Kept
            }
            Arrival::Overflow => {
                let request = self.held.remove(&rid).expect("present above");
                let refusal = self.overflow_refusal(&request.cap_urn);
                Arrived::Overflowed(request, refusal)
            }
            Arrival::Cancel => {
                let request = self.held.remove(&rid).expect("present above");
                let reason = frame.cancel_reason().unwrap_or_default();
                let mut err = Frame::err(
                    rid.clone(),
                    reason.terminal_code(),
                    reason.terminal_class(),
                    &reason.terminal_message(),
                    None,
                );
                err.routing_id = Some(request.xid.clone());
                Arrived::Cancelled(request, err)
            }
        }
    }

    /// The end of bringing the provider of held request `rid`. `None` when it
    /// is no longer held: it was cancelled, it overflowed, or its caller's
    /// master died.
    pub fn settle(&mut self, rid: &MessageId, settlement: Settlement) -> Option<Released> {
        use crate::formal::bifaci::hold::{release, Outcome, Release};
        let request = self.held.remove(rid)?;
        let (outcome, refusal) = match settlement {
            Settlement::Provided => (Outcome::Provided, None),
            Settlement::Refused(refusal) => (Outcome::Refused, Some(refusal)),
            Settlement::Expired(refusal) => (Outcome::Expired, Some(refusal)),
        };
        Some(match release(outcome) {
            Release::Replay => Released::Replay(request),
            Release::Fail => {
                let refusal = refusal.expect("the model fails only a hold that was refused or expired");
                let err = refusal.err_frame(rid, &request.xid);
                Released::Fail(request, err)
            }
        })
    }

    /// Every request master `source_idx` asked for: it is gone, and so are
    /// they — nobody is left to answer.
    pub fn forget_source(&mut self, source_idx: usize) -> usize {
        let before = self.held.len();
        self.held.retain(|_, h| h.source_idx != source_idx);
        before - self.held.len()
    }

    fn overflow_refusal(&self, cap_urn: &str) -> PeerRefusal {
        PeerRefusal {
            code: PEER_HOLD_OVERFLOW.to_string(),
            class: crate::failure::AttributionClass::Resource,
            message: format!(
                "the peer call to {cap_urn} sent more than {} frames or {} bytes while the \
                 cartridge that provides it was being installed",
                self.bound.frames, self.bound.bytes
            ),
        }
    }
}

/// The ERR code of a held request that sent more than its hold keeps.
pub const PEER_HOLD_OVERFLOW: &str = "PEER_HOLD_OVERFLOW";
/// The ERR code of a held request whose provider was not there in time and
/// whose provider had not said what it was bringing.
pub const PEER_PROVIDER_TIMEOUT: &str = "PEER_PROVIDER_TIMEOUT";
