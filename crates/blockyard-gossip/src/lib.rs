//! Blockyard gossip — SWIM-based cluster membership protocol.
//!
//! Handles peer discovery, failure detection, and membership dissemination
//! using a SWIM-family protocol.

pub mod member;
pub mod protocol;
pub mod service;
pub mod swim;
pub mod transport;

#[cfg(test)]
pub(crate) mod testutil;

pub use member::{Member, MemberList};
pub use protocol::{GossipMessage, MemberState, MembershipUpdate};
pub use service::GossipService;
pub use swim::{SwimConfig, SwimProtocol};
pub use transport::{GossipTransport, TransportError, UdpTransport};
