//! Protocol version negotiation (§7, P2.2).
//!
//! Validates that client and server share a compatible protocol version
//! during connection handshake.

use blockyard_common::NodeId;

use crate::messages::{
    CURRENT_PROTOCOL_VERSION, HandshakeRequest, HandshakeResponse, MIN_PROTOCOL_VERSION,
    ProtocolVersion,
};

/// Result of version negotiation.
#[derive(Debug, Clone)]
pub struct NegotiationResult {
    pub negotiated_version: ProtocolVersion,
    pub accepted: bool,
    pub message: Option<String>,
}

/// Negotiate protocol version from a handshake request.
///
/// Accepts if the client's version is within our supported range.
/// Returns the minimum of client and server versions.
pub fn negotiate_version(request: &HandshakeRequest, local_node_id: NodeId) -> HandshakeResponse {
    let client_version = request.protocol_version;

    if client_version < MIN_PROTOCOL_VERSION {
        return HandshakeResponse {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            node_id: local_node_id,
            accepted: false,
            message: Some(format!(
                "client protocol version {} is below minimum supported version {}",
                client_version, MIN_PROTOCOL_VERSION
            )),
            supported_features: vec![],
        };
    }

    let negotiated = client_version.min(CURRENT_PROTOCOL_VERSION);

    HandshakeResponse {
        protocol_version: negotiated,
        node_id: local_node_id,
        accepted: true,
        message: None,
        supported_features: vec![],
    }
}

/// Check whether a protocol version is supported.
pub fn is_version_supported(version: ProtocolVersion) -> bool {
    version >= MIN_PROTOCOL_VERSION && version <= CURRENT_PROTOCOL_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_handshake(version: ProtocolVersion) -> HandshakeRequest {
        HandshakeRequest {
            protocol_version: version,
            node_id: None,
            session_id: None,
            features: vec![],
        }
    }

    #[test]
    fn test_negotiate_current_version() {
        let req = make_handshake(CURRENT_PROTOCOL_VERSION);
        let resp = negotiate_version(&req, NodeId::generate());
        assert!(resp.accepted);
        assert_eq!(resp.protocol_version, CURRENT_PROTOCOL_VERSION);
    }

    #[test]
    fn test_negotiate_minimum_version() {
        let req = make_handshake(MIN_PROTOCOL_VERSION);
        let resp = negotiate_version(&req, NodeId::generate());
        assert!(resp.accepted);
        assert_eq!(resp.protocol_version, MIN_PROTOCOL_VERSION);
    }

    #[test]
    fn test_negotiate_future_version() {
        let req = make_handshake(CURRENT_PROTOCOL_VERSION + 10);
        let resp = negotiate_version(&req, NodeId::generate());
        assert!(resp.accepted);
        assert_eq!(resp.protocol_version, CURRENT_PROTOCOL_VERSION);
    }

    #[test]
    fn test_negotiate_too_old_version() {
        if MIN_PROTOCOL_VERSION > 0 {
            let req = make_handshake(0);
            let resp = negotiate_version(&req, NodeId::generate());
            assert!(!resp.accepted);
            assert!(resp.message.is_some());
        }
    }

    #[test]
    fn test_negotiate_returns_node_id() {
        let node_id = NodeId::generate();
        let req = make_handshake(CURRENT_PROTOCOL_VERSION);
        let resp = negotiate_version(&req, node_id);
        assert_eq!(resp.node_id, node_id);
    }

    #[test]
    fn test_is_version_supported_current() {
        assert!(is_version_supported(CURRENT_PROTOCOL_VERSION));
    }

    #[test]
    fn test_is_version_supported_min() {
        assert!(is_version_supported(MIN_PROTOCOL_VERSION));
    }

    #[test]
    fn test_is_version_not_supported_zero() {
        if MIN_PROTOCOL_VERSION > 0 {
            assert!(!is_version_supported(0));
        }
    }

    #[test]
    fn test_is_version_not_supported_future() {
        assert!(!is_version_supported(CURRENT_PROTOCOL_VERSION + 1));
    }
}
