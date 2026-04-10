//! Blockyard wire protocol — message definitions and serialization.
//!
//! Defines the on-wire format for client-to-node and node-to-node
//! communication, including version negotiation.

pub mod messages;
pub mod server;
pub mod version;

pub use messages::{
    CURRENT_PROTOCOL_VERSION, ErrorCode, HandshakeRequest, HandshakeResponse, MIN_PROTOCOL_VERSION,
    ProtocolMessage, ProtocolVersion, ReadExtentRequest, ReadExtentResponse, WriteExtentRequest,
    WriteExtentResponse,
};
pub use server::{DataPlaneHandler, DataPlaneServer, ServerError};
pub use version::{
    NegotiationResult, is_version_supported, negotiate_version, negotiate_version_with_auth,
};
