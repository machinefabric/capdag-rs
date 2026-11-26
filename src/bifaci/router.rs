//! Cap Router - Pluggable routing for peer invoke requests
//!
//! When a cartridge sends a peer invoke REQ (calling another cap), the host needs to route
//! that request to an appropriate handler. This module provides a trait-based abstraction
//! for different routing strategies.
//!
//! The router receives frames (REQ, STREAM_START, CHUNK, STREAM_END, END) and delegates
//! them to the appropriate target cartridge, then forwards responses back.

use crate::bifaci::frame::Frame;
use crate::bifaci::host_runtime::{AsyncHostError, ResponseChunk};
use crossbeam_channel::Receiver;
use std::sync::Arc;

/// Handle for an active peer invoke request.
///
/// The CartridgeHostRuntime creates this by calling router.begin_request(), then forwards
/// incoming frames (STREAM_START, CHUNK, STREAM_END, END) to the handle. The handle
/// provides a receiver for response chunks.
pub trait PeerRequestHandle: Send {
    /// Forward an incoming frame (STREAM_START, CHUNK, STREAM_END, or END) to the target.
    /// The router forwards these directly to the target cartridge.
    fn forward_frame(&mut self, frame: Frame);

    /// Get a receiver for response chunks from the target cartridge.
    /// The host reads from this and forwards responses back to the requesting cartridge.
    fn response_receiver(&self) -> Receiver<Result<ResponseChunk, AsyncHostError>>;
}

/// Trait for routing cap invocation requests to appropriate handlers.
///
/// When a cartridge issues a peer invoke, the host receives a REQ frame and calls begin_request().
/// The router returns a handle that the host uses to forward incoming argument streams and
/// receive responses.
///
/// # Example Flow
/// ```ignore
/// // 1. Cartridge sends REQ frame
/// let handle = router.begin_request(cap_urn, req_id)?;
///
/// // 2. Host forwards argument streams to handle
/// handle.forward_frame(stream_start_frame);
/// handle.forward_frame(chunk_frame);
/// handle.forward_frame(stream_end_frame);
/// handle.forward_frame(end_frame);
///
/// // 3. Host reads responses from handle and forwards back to cartridge
/// for chunk_result in handle.response_receiver().iter() {
///     let chunk = chunk_result?;
///     send_to_cartridge(chunk);
/// }
/// ```
pub trait CapRouter: Send + Sync {
    /// Begin routing a peer invoke request.
    ///
    /// # Arguments
    /// * `cap_urn` - The cap URN being requested
    /// * `req_id` - The request ID from the REQ frame
    ///
    /// # Returns
    /// A handle for forwarding frames and receiving responses.
    ///
    /// # Errors
    /// - `NoHandler` - No cartridge provides the requested cap
    /// - `CartridgeSpawnFailed` - Failed to download/start a cartridge
    fn begin_request(
        &self,
        cap_urn: &str,
        req_id: &[u8; 16],
    ) -> Result<Box<dyn PeerRequestHandle>, AsyncHostError>;
}

/// No-op router that rejects all peer invoke requests.
pub struct NoPeerRouter;

impl CapRouter for NoPeerRouter {
    fn begin_request(
        &self,
        cap_urn: &str,
        _req_id: &[u8; 16],
    ) -> Result<Box<dyn PeerRequestHandle>, AsyncHostError> {
        Err(AsyncHostError::PeerInvokeNotSupported(cap_urn.to_string()))
    }
}

/// Arc wrapper for trait objects to enable cloning.
pub type ArcCapRouter = Arc<dyn CapRouter>;

#[cfg(test)]
mod tests {
    use super::*;

    // TEST638: Verify NoPeerRouter rejects all requests with PeerInvokeNotSupported
    #[test]
    fn test638_no_peer_router_rejects_all() {
        let router = NoPeerRouter;
        let req_id = [0u8; 16];
        let result = router.begin_request("cap:in=\"media:void\";test;out=\"media:void\"", &req_id);

        assert!(result.is_err());
        match result {
            Err(AsyncHostError::PeerInvokeNotSupported(urn)) => {
                assert!(urn.contains("test"));
            }
            _ => panic!("Expected PeerInvokeNotSupported error"),
        }
    }
}
