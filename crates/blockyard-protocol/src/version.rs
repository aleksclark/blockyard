//! Protocol version negotiation (§7, P2.2) and authenticated handshake (P6.3, P6.4).
//!
//! Validates that client and server share a compatible protocol version
//! during connection handshake. Optionally validates an auth token for
//! client (P6.3) and node-to-node (P6.4) authentication.

use blockyard_common::{AuthProvider, NodeId, PeerIdentity};

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
            authenticated: false,
        };
    }

    let negotiated = client_version.min(CURRENT_PROTOCOL_VERSION);

    HandshakeResponse {
        protocol_version: negotiated,
        node_id: local_node_id,
        accepted: true,
        message: None,
        supported_features: vec![],
        authenticated: false,
    }
}

/// Negotiate protocol version and authenticate the peer (P6.3, P6.4).
///
/// If `auth_provider` is `Some`, the handshake token is validated. On failure
/// the handshake is rejected with an error message and `authenticated = false`.
/// If no provider is given, authentication is skipped (backwards compat).
pub fn negotiate_version_with_auth(
    request: &HandshakeRequest,
    local_node_id: NodeId,
    auth_provider: Option<&dyn AuthProvider>,
) -> (HandshakeResponse, Option<PeerIdentity>) {
    let mut response = negotiate_version(request, local_node_id);
    if !response.accepted {
        return (response, None);
    }

    let provider = match auth_provider {
        Some(p) => p,
        None => return (response, None),
    };

    let token = match &request.auth_token {
        Some(t) => t,
        None => {
            response.accepted = false;
            response.authenticated = false;
            response.message = Some("authentication required but no token provided".into());
            return (response, None);
        }
    };

    match provider.validate_token(token) {
        Ok(identity) => {
            response.authenticated = true;
            (response, Some(identity))
        }
        Err(e) => {
            response.accepted = false;
            response.authenticated = false;
            response.message = Some(format!("authentication failed: {e}"));
            (response, None)
        }
    }
}

/// Check whether a protocol version is supported.
pub fn is_version_supported(version: ProtocolVersion) -> bool {
    version >= MIN_PROTOCOL_VERSION && version <= CURRENT_PROTOCOL_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockyard_common::{AuthToken, SessionId, SharedSecretAuth};

    fn make_handshake(version: ProtocolVersion) -> HandshakeRequest {
        HandshakeRequest {
            protocol_version: version,
            node_id: None,
            session_id: None,
            features: vec![],
            auth_token: None,
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

    #[test]
    fn test_negotiate_with_auth_no_provider() {
        let req = make_handshake(CURRENT_PROTOCOL_VERSION);
        let (resp, identity) = negotiate_version_with_auth(&req, NodeId::generate(), None);
        assert!(resp.accepted);
        assert!(!resp.authenticated);
        assert!(identity.is_none());
    }

    #[test]
    fn test_negotiate_with_auth_valid_client_token() {
        let auth = SharedSecretAuth::new("test-handshake-secret").unwrap();
        let sid = SessionId::generate();
        let peer = PeerIdentity::Client(sid);
        let token = auth.create_token(&peer, 300_000).unwrap();

        let req = HandshakeRequest {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            node_id: None,
            session_id: Some(sid),
            features: vec![],
            auth_token: Some(token),
        };

        let (resp, identity) = negotiate_version_with_auth(&req, NodeId::generate(), Some(&auth));
        assert!(resp.accepted);
        assert!(resp.authenticated);
        assert_eq!(identity.unwrap(), peer);
    }

    #[test]
    fn test_negotiate_with_auth_valid_node_token() {
        let auth = SharedSecretAuth::new("node-handshake-secret").unwrap();
        let nid = NodeId::generate();
        let peer = PeerIdentity::Node(nid);
        let token = auth.create_token(&peer, 300_000).unwrap();

        let req = HandshakeRequest {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            node_id: Some(nid),
            session_id: None,
            features: vec![],
            auth_token: Some(token),
        };

        let (resp, identity) = negotiate_version_with_auth(&req, NodeId::generate(), Some(&auth));
        assert!(resp.accepted);
        assert!(resp.authenticated);
        assert_eq!(identity.unwrap(), peer);
    }

    #[test]
    fn test_negotiate_with_auth_missing_token() {
        let auth = SharedSecretAuth::new("missing-token-secret").unwrap();
        let req = make_handshake(CURRENT_PROTOCOL_VERSION);
        let (resp, identity) = negotiate_version_with_auth(&req, NodeId::generate(), Some(&auth));
        assert!(!resp.accepted);
        assert!(!resp.authenticated);
        assert!(resp.message.unwrap().contains("no token"));
        assert!(identity.is_none());
    }

    #[test]
    fn test_negotiate_with_auth_invalid_token() {
        let auth = SharedSecretAuth::new("invalid-token-secret").unwrap();
        let bad_token = AuthToken {
            identity: "client:00000000-0000-0000-0000-000000000000".into(),
            issued_at_ms: 0,
            expires_at_ms: 1,
            signature: "bad-sig".into(),
        };

        let req = HandshakeRequest {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            node_id: None,
            session_id: None,
            features: vec![],
            auth_token: Some(bad_token),
        };

        let (resp, identity) = negotiate_version_with_auth(&req, NodeId::generate(), Some(&auth));
        assert!(!resp.accepted);
        assert!(!resp.authenticated);
        assert!(resp.message.unwrap().contains("authentication failed"));
        assert!(identity.is_none());
    }

    #[test]
    fn test_negotiate_with_auth_version_rejected_first() {
        if MIN_PROTOCOL_VERSION > 0 {
            let auth = SharedSecretAuth::new("version-first-secret").unwrap();
            let mut req = make_handshake(0);
            let peer = PeerIdentity::Client(SessionId::generate());
            req.auth_token = Some(auth.create_token(&peer, 300_000).unwrap());

            let (resp, identity) =
                negotiate_version_with_auth(&req, NodeId::generate(), Some(&auth));
            assert!(!resp.accepted);
            assert!(!resp.authenticated);
            assert!(identity.is_none());
        }
    }

    #[test]
    fn test_negotiate_unauthenticated_is_false_by_default() {
        let req = make_handshake(CURRENT_PROTOCOL_VERSION);
        let resp = negotiate_version(&req, NodeId::generate());
        assert!(!resp.authenticated);
    }

    #[test]
    fn test_negotiation_result_debug() {
        let result = NegotiationResult {
            negotiated_version: 1,
            accepted: true,
            message: None,
        };
        let debug = format!("{result:?}");
        assert!(debug.contains("NegotiationResult"));
    }
}
