//! Authentication and authorization for Blockyard (P6.3–P6.5, §8).
//!
//! Provides:
//! - [`AuthProvider`] trait for pluggable authentication backends.
//! - [`SharedSecretAuth`] implementing HMAC-SHA256 token-based auth for both
//!   client-to-node (P6.3) and node-to-node (P6.4) authentication.
//! - [`VolumeAcl`] for per-volume read/write authorization (P6.5).
//! - [`AuthToken`] as the wire-format bearer token.

use std::collections::HashMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::error::Error;
use crate::id::{NodeId, SessionId, VolumeId};

type HmacSha256 = Hmac<Sha256>;

/// Bearer token exchanged during handshake and included in requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthToken {
    pub identity: String,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
    pub signature: String,
}

impl AuthToken {
    pub fn is_expired(&self, now_ms: u64) -> bool {
        now_ms >= self.expires_at_ms
    }
}

impl fmt::Display for AuthToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AuthToken(identity={}, expires={})",
            self.identity, self.expires_at_ms
        )
    }
}

/// Identity type distinguishing clients from peer nodes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PeerIdentity {
    Client(SessionId),
    Node(NodeId),
}

impl fmt::Display for PeerIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerIdentity::Client(sid) => write!(f, "client:{sid}"),
            PeerIdentity::Node(nid) => write!(f, "node:{nid}"),
        }
    }
}

/// Per-volume access control entry (P6.5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumePermission {
    pub read: bool,
    pub write: bool,
}

impl VolumePermission {
    pub fn read_only() -> Self {
        Self {
            read: true,
            write: false,
        }
    }

    pub fn read_write() -> Self {
        Self {
            read: true,
            write: true,
        }
    }

    pub fn none() -> Self {
        Self {
            read: false,
            write: false,
        }
    }
}

/// Per-volume access control list (P6.5).
///
/// Maps identities to per-volume permissions. Nodes are implicitly granted
/// full access for replication. Clients must be explicitly authorized.
#[derive(Debug)]
pub struct VolumeAcl {
    acls: RwLock<HashMap<VolumeId, HashMap<String, VolumePermission>>>,
}

impl VolumeAcl {
    pub fn new() -> Self {
        Self {
            acls: RwLock::new(HashMap::new()),
        }
    }

    pub fn grant(&self, volume_id: VolumeId, identity: &str, permission: VolumePermission) {
        let mut acls = self.acls.write();
        acls.entry(volume_id)
            .or_default()
            .insert(identity.to_string(), permission);
    }

    pub fn revoke(&self, volume_id: VolumeId, identity: &str) {
        let mut acls = self.acls.write();
        if let Some(vol_acl) = acls.get_mut(&volume_id) {
            vol_acl.remove(identity);
            if vol_acl.is_empty() {
                acls.remove(&volume_id);
            }
        }
    }

    pub fn check_read(&self, volume_id: &VolumeId, identity: &PeerIdentity) -> bool {
        if matches!(identity, PeerIdentity::Node(_)) {
            return true;
        }
        let acls = self.acls.read();
        acls.get(volume_id)
            .and_then(|vol_acl| vol_acl.get(&identity.to_string()))
            .is_some_and(|perm| perm.read)
    }

    pub fn check_write(&self, volume_id: &VolumeId, identity: &PeerIdentity) -> bool {
        if matches!(identity, PeerIdentity::Node(_)) {
            return true;
        }
        let acls = self.acls.read();
        acls.get(volume_id)
            .and_then(|vol_acl| vol_acl.get(&identity.to_string()))
            .is_some_and(|perm| perm.write)
    }

    pub fn list_permissions(&self, volume_id: &VolumeId) -> HashMap<String, VolumePermission> {
        let acls = self.acls.read();
        acls.get(volume_id).cloned().unwrap_or_default()
    }

    pub fn remove_volume(&self, volume_id: &VolumeId) {
        self.acls.write().remove(volume_id);
    }
}

impl Default for VolumeAcl {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for pluggable authentication providers.
pub trait AuthProvider: Send + Sync + fmt::Debug {
    fn create_token(&self, identity: &PeerIdentity, ttl_ms: u64) -> Result<AuthToken, Error>;

    fn validate_token(&self, token: &AuthToken) -> Result<PeerIdentity, Error>;
}

/// Default token TTL: 5 minutes.
pub const DEFAULT_TOKEN_TTL_MS: u64 = 300_000;

/// HMAC-SHA256 shared-secret authentication (P6.3, P6.4).
///
/// Both client-to-node and node-to-node connections authenticate by presenting
/// a token signed with the cluster shared secret. The token contains the
/// caller identity, issue time, and expiry.
#[derive(Debug)]
pub struct SharedSecretAuth {
    secret: Vec<u8>,
}

impl SharedSecretAuth {
    pub fn new(secret: &str) -> Result<Self, Error> {
        if secret.len() < 8 {
            return Err(Error::Auth(
                "shared secret must be at least 8 characters".into(),
            ));
        }
        Ok(Self {
            secret: secret.as_bytes().to_vec(),
        })
    }

    fn compute_signature(&self, identity: &str, issued_at_ms: u64, expires_at_ms: u64) -> String {
        let mut mac =
            HmacSha256::new_from_slice(&self.secret).expect("HMAC accepts any key length");
        mac.update(identity.as_bytes());
        mac.update(&issued_at_ms.to_le_bytes());
        mac.update(&expires_at_ms.to_le_bytes());
        let result = mac.finalize();
        hex::encode(result.into_bytes())
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

impl AuthProvider for SharedSecretAuth {
    fn create_token(&self, identity: &PeerIdentity, ttl_ms: u64) -> Result<AuthToken, Error> {
        let now = Self::now_ms();
        let expires = now.saturating_add(ttl_ms);
        let identity_str = identity.to_string();
        let signature = self.compute_signature(&identity_str, now, expires);

        Ok(AuthToken {
            identity: identity_str,
            issued_at_ms: now,
            expires_at_ms: expires,
            signature,
        })
    }

    fn validate_token(&self, token: &AuthToken) -> Result<PeerIdentity, Error> {
        let now = Self::now_ms();
        if token.is_expired(now) {
            return Err(Error::Auth(format!(
                "token expired at {}, current time {}",
                token.expires_at_ms, now
            )));
        }

        let expected_sig =
            self.compute_signature(&token.identity, token.issued_at_ms, token.expires_at_ms);
        if token.signature != expected_sig {
            return Err(Error::Auth("invalid token signature".into()));
        }

        parse_identity(&token.identity)
    }
}

/// Parse a `PeerIdentity` from its string representation.
fn parse_identity(s: &str) -> Result<PeerIdentity, Error> {
    if let Some(rest) = s.strip_prefix("client:") {
        let sid: SessionId = rest
            .parse()
            .map_err(|e| Error::Auth(format!("invalid session id in token: {e}")))?;
        Ok(PeerIdentity::Client(sid))
    } else if let Some(rest) = s.strip_prefix("node:") {
        let nid: NodeId = rest
            .parse()
            .map_err(|e| Error::Auth(format!("invalid node id in token: {e}")))?;
        Ok(PeerIdentity::Node(nid))
    } else {
        Err(Error::Auth(format!("unrecognized identity format: {s}")))
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

mod hex {
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        super::hex_encode(bytes.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{NodeId, SessionId};

    #[test]
    fn test_shared_secret_auth_create_and_validate_client_token() {
        let auth = SharedSecretAuth::new("super-secret-key-123").unwrap();
        let sid = SessionId::generate();
        let identity = PeerIdentity::Client(sid);

        let token = auth.create_token(&identity, 60_000).unwrap();
        assert!(token.identity.starts_with("client:"));
        assert!(!token.signature.is_empty());

        let validated = auth.validate_token(&token).unwrap();
        assert_eq!(validated, identity);
    }

    #[test]
    fn test_shared_secret_auth_create_and_validate_node_token() {
        let auth = SharedSecretAuth::new("node-secret-key-456").unwrap();
        let nid = NodeId::generate();
        let identity = PeerIdentity::Node(nid);

        let token = auth.create_token(&identity, 60_000).unwrap();
        assert!(token.identity.starts_with("node:"));

        let validated = auth.validate_token(&token).unwrap();
        assert_eq!(validated, identity);
    }

    #[test]
    fn test_shared_secret_auth_reject_short_secret() {
        let result = SharedSecretAuth::new("short");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("at least 8"));
    }

    #[test]
    fn test_shared_secret_auth_reject_expired_token() {
        let auth = SharedSecretAuth::new("test-secret-key-789").unwrap();
        let identity = PeerIdentity::Client(SessionId::generate());

        let token = AuthToken {
            identity: identity.to_string(),
            issued_at_ms: 0,
            expires_at_ms: 1,
            signature: auth.compute_signature(&identity.to_string(), 0, 1),
        };

        let result = auth.validate_token(&token);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("expired"));
    }

    #[test]
    fn test_shared_secret_auth_reject_tampered_signature() {
        let auth = SharedSecretAuth::new("tamper-test-secret-key").unwrap();
        let identity = PeerIdentity::Client(SessionId::generate());

        let mut token = auth.create_token(&identity, 300_000).unwrap();
        token.signature = "deadbeef00000000".into();

        let result = auth.validate_token(&token);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid token signature")
        );
    }

    #[test]
    fn test_shared_secret_auth_reject_tampered_identity() {
        let auth = SharedSecretAuth::new("tamper-identity-test-key").unwrap();
        let identity = PeerIdentity::Client(SessionId::generate());

        let mut token = auth.create_token(&identity, 300_000).unwrap();
        let other_sid = SessionId::generate();
        token.identity = format!("client:{other_sid}");

        let result = auth.validate_token(&token);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid token signature")
        );
    }

    #[test]
    fn test_shared_secret_auth_different_secrets_incompatible() {
        let auth1 = SharedSecretAuth::new("first-secret-key-aaa").unwrap();
        let auth2 = SharedSecretAuth::new("second-secret-key-bbb").unwrap();
        let identity = PeerIdentity::Node(NodeId::generate());

        let token = auth1.create_token(&identity, 300_000).unwrap();
        let result = auth2.validate_token(&token);
        assert!(result.is_err());
    }

    #[test]
    fn test_auth_token_is_expired() {
        let token = AuthToken {
            identity: "test".into(),
            issued_at_ms: 1000,
            expires_at_ms: 2000,
            signature: String::new(),
        };
        assert!(!token.is_expired(1000));
        assert!(!token.is_expired(1999));
        assert!(token.is_expired(2000));
        assert!(token.is_expired(3000));
    }

    #[test]
    fn test_auth_token_display() {
        let token = AuthToken {
            identity: "client:abc".into(),
            issued_at_ms: 1000,
            expires_at_ms: 2000,
            signature: "sig".into(),
        };
        let display = format!("{token}");
        assert!(display.contains("client:abc"));
        assert!(display.contains("2000"));
    }

    #[test]
    fn test_auth_token_serde_roundtrip() {
        let auth = SharedSecretAuth::new("serde-roundtrip-key-123").unwrap();
        let identity = PeerIdentity::Client(SessionId::generate());
        let token = auth.create_token(&identity, 60_000).unwrap();

        let json = serde_json::to_string(&token).unwrap();
        let parsed: AuthToken = serde_json::from_str(&json).unwrap();
        assert_eq!(token, parsed);
    }

    #[test]
    fn test_peer_identity_display_client() {
        let sid = SessionId::generate();
        let id = PeerIdentity::Client(sid);
        assert!(id.to_string().starts_with("client:"));
        assert!(id.to_string().contains(&sid.to_string()));
    }

    #[test]
    fn test_peer_identity_display_node() {
        let nid = NodeId::generate();
        let id = PeerIdentity::Node(nid);
        assert!(id.to_string().starts_with("node:"));
        assert!(id.to_string().contains(&nid.to_string()));
    }

    #[test]
    fn test_peer_identity_serde_roundtrip() {
        let identities = vec![
            PeerIdentity::Client(SessionId::generate()),
            PeerIdentity::Node(NodeId::generate()),
        ];
        for id in &identities {
            let json = serde_json::to_string(id).unwrap();
            let parsed: PeerIdentity = serde_json::from_str(&json).unwrap();
            assert_eq!(*id, parsed);
        }
    }

    #[test]
    fn test_peer_identity_hash() {
        use std::collections::HashSet;
        let sid = SessionId::generate();
        let mut set = HashSet::new();
        set.insert(PeerIdentity::Client(sid));
        set.insert(PeerIdentity::Client(sid));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_parse_identity_client() {
        let sid = SessionId::generate();
        let result = parse_identity(&format!("client:{sid}")).unwrap();
        assert_eq!(result, PeerIdentity::Client(sid));
    }

    #[test]
    fn test_parse_identity_node() {
        let nid = NodeId::generate();
        let result = parse_identity(&format!("node:{nid}")).unwrap();
        assert_eq!(result, PeerIdentity::Node(nid));
    }

    #[test]
    fn test_parse_identity_invalid_format() {
        let result = parse_identity("unknown:abc");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unrecognized"));
    }

    #[test]
    fn test_parse_identity_invalid_uuid() {
        let result = parse_identity("client:not-a-uuid");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_identity_invalid_node_uuid() {
        let result = parse_identity("node:not-a-uuid");
        assert!(result.is_err());
    }

    #[test]
    fn test_volume_permission_read_only() {
        let perm = VolumePermission::read_only();
        assert!(perm.read);
        assert!(!perm.write);
    }

    #[test]
    fn test_volume_permission_read_write() {
        let perm = VolumePermission::read_write();
        assert!(perm.read);
        assert!(perm.write);
    }

    #[test]
    fn test_volume_permission_none() {
        let perm = VolumePermission::none();
        assert!(!perm.read);
        assert!(!perm.write);
    }

    #[test]
    fn test_volume_permission_serde_roundtrip() {
        let perms = vec![
            VolumePermission::read_only(),
            VolumePermission::read_write(),
            VolumePermission::none(),
        ];
        for perm in &perms {
            let json = serde_json::to_string(perm).unwrap();
            let parsed: VolumePermission = serde_json::from_str(&json).unwrap();
            assert_eq!(*perm, parsed);
        }
    }

    #[test]
    fn test_volume_acl_grant_and_check_read() {
        let acl = VolumeAcl::new();
        let vid = VolumeId::generate();
        let sid = SessionId::generate();
        let identity = PeerIdentity::Client(sid);

        assert!(!acl.check_read(&vid, &identity));

        acl.grant(vid, &identity.to_string(), VolumePermission::read_only());
        assert!(acl.check_read(&vid, &identity));
        assert!(!acl.check_write(&vid, &identity));
    }

    #[test]
    fn test_volume_acl_grant_and_check_write() {
        let acl = VolumeAcl::new();
        let vid = VolumeId::generate();
        let sid = SessionId::generate();
        let identity = PeerIdentity::Client(sid);

        acl.grant(vid, &identity.to_string(), VolumePermission::read_write());
        assert!(acl.check_read(&vid, &identity));
        assert!(acl.check_write(&vid, &identity));
    }

    #[test]
    fn test_volume_acl_node_always_authorized() {
        let acl = VolumeAcl::new();
        let vid = VolumeId::generate();
        let nid = NodeId::generate();
        let identity = PeerIdentity::Node(nid);

        assert!(acl.check_read(&vid, &identity));
        assert!(acl.check_write(&vid, &identity));
    }

    #[test]
    fn test_volume_acl_revoke() {
        let acl = VolumeAcl::new();
        let vid = VolumeId::generate();
        let sid = SessionId::generate();
        let identity = PeerIdentity::Client(sid);

        acl.grant(vid, &identity.to_string(), VolumePermission::read_write());
        assert!(acl.check_write(&vid, &identity));

        acl.revoke(vid, &identity.to_string());
        assert!(!acl.check_write(&vid, &identity));
        assert!(!acl.check_read(&vid, &identity));
    }

    #[test]
    fn test_volume_acl_revoke_cleans_empty_volume() {
        let acl = VolumeAcl::new();
        let vid = VolumeId::generate();
        let sid = SessionId::generate();
        let identity = PeerIdentity::Client(sid);

        acl.grant(vid, &identity.to_string(), VolumePermission::read_only());
        acl.revoke(vid, &identity.to_string());

        let perms = acl.list_permissions(&vid);
        assert!(perms.is_empty());
    }

    #[test]
    fn test_volume_acl_revoke_nonexistent() {
        let acl = VolumeAcl::new();
        let vid = VolumeId::generate();
        acl.revoke(vid, "nonexistent");
    }

    #[test]
    fn test_volume_acl_list_permissions() {
        let acl = VolumeAcl::new();
        let vid = VolumeId::generate();
        let sid1 = SessionId::generate();
        let sid2 = SessionId::generate();
        let id1 = PeerIdentity::Client(sid1);
        let id2 = PeerIdentity::Client(sid2);

        acl.grant(vid, &id1.to_string(), VolumePermission::read_only());
        acl.grant(vid, &id2.to_string(), VolumePermission::read_write());

        let perms = acl.list_permissions(&vid);
        assert_eq!(perms.len(), 2);
        assert_eq!(perms[&id1.to_string()], VolumePermission::read_only());
        assert_eq!(perms[&id2.to_string()], VolumePermission::read_write());
    }

    #[test]
    fn test_volume_acl_list_permissions_empty() {
        let acl = VolumeAcl::new();
        let vid = VolumeId::generate();
        let perms = acl.list_permissions(&vid);
        assert!(perms.is_empty());
    }

    #[test]
    fn test_volume_acl_remove_volume() {
        let acl = VolumeAcl::new();
        let vid = VolumeId::generate();
        let sid = SessionId::generate();
        let identity = PeerIdentity::Client(sid);

        acl.grant(vid, &identity.to_string(), VolumePermission::read_write());
        assert!(acl.check_read(&vid, &identity));

        acl.remove_volume(&vid);
        assert!(!acl.check_read(&vid, &identity));
    }

    #[test]
    fn test_volume_acl_multiple_volumes() {
        let acl = VolumeAcl::new();
        let vid1 = VolumeId::generate();
        let vid2 = VolumeId::generate();
        let sid = SessionId::generate();
        let identity = PeerIdentity::Client(sid);

        acl.grant(vid1, &identity.to_string(), VolumePermission::read_only());
        acl.grant(vid2, &identity.to_string(), VolumePermission::read_write());

        assert!(acl.check_read(&vid1, &identity));
        assert!(!acl.check_write(&vid1, &identity));
        assert!(acl.check_read(&vid2, &identity));
        assert!(acl.check_write(&vid2, &identity));
    }

    #[test]
    fn test_volume_acl_overwrite_permission() {
        let acl = VolumeAcl::new();
        let vid = VolumeId::generate();
        let sid = SessionId::generate();
        let identity = PeerIdentity::Client(sid);

        acl.grant(vid, &identity.to_string(), VolumePermission::read_only());
        assert!(!acl.check_write(&vid, &identity));

        acl.grant(vid, &identity.to_string(), VolumePermission::read_write());
        assert!(acl.check_write(&vid, &identity));
    }

    #[test]
    fn test_volume_acl_default() {
        let acl = VolumeAcl::default();
        let vid = VolumeId::generate();
        let identity = PeerIdentity::Client(SessionId::generate());
        assert!(!acl.check_read(&vid, &identity));
    }

    #[test]
    fn test_volume_acl_none_permission_denies_access() {
        let acl = VolumeAcl::new();
        let vid = VolumeId::generate();
        let sid = SessionId::generate();
        let identity = PeerIdentity::Client(sid);

        acl.grant(vid, &identity.to_string(), VolumePermission::none());
        assert!(!acl.check_read(&vid, &identity));
        assert!(!acl.check_write(&vid, &identity));
    }

    #[test]
    fn test_shared_secret_auth_debug() {
        let auth = SharedSecretAuth::new("debug-test-secret").unwrap();
        let debug = format!("{auth:?}");
        assert!(debug.contains("SharedSecretAuth"));
    }

    #[test]
    fn test_volume_acl_debug() {
        let acl = VolumeAcl::new();
        let debug = format!("{acl:?}");
        assert!(debug.contains("VolumeAcl"));
    }

    #[test]
    fn test_peer_identity_debug() {
        let id = PeerIdentity::Client(SessionId::generate());
        let debug = format!("{id:?}");
        assert!(debug.contains("Client"));
    }

    #[test]
    fn test_auth_token_debug() {
        let token = AuthToken {
            identity: "test".into(),
            issued_at_ms: 0,
            expires_at_ms: 1000,
            signature: "sig".into(),
        };
        let debug = format!("{token:?}");
        assert!(debug.contains("AuthToken"));
    }

    #[test]
    fn test_volume_permission_debug() {
        let perm = VolumePermission::read_write();
        let debug = format!("{perm:?}");
        assert!(debug.contains("VolumePermission"));
    }

    #[test]
    fn test_volume_permission_clone() {
        let perm = VolumePermission::read_write();
        let cloned = perm.clone();
        assert_eq!(perm, cloned);
    }

    #[test]
    fn test_peer_identity_clone() {
        let id = PeerIdentity::Node(NodeId::generate());
        let cloned = id.clone();
        assert_eq!(id, cloned);
    }

    #[test]
    fn test_auth_token_clone() {
        let token = AuthToken {
            identity: "test".into(),
            issued_at_ms: 0,
            expires_at_ms: 1000,
            signature: "sig".into(),
        };
        let cloned = token.clone();
        assert_eq!(token, cloned);
    }

    #[test]
    fn test_default_token_ttl() {
        assert_eq!(DEFAULT_TOKEN_TTL_MS, 300_000);
    }

    #[test]
    fn test_compute_signature_deterministic() {
        let auth = SharedSecretAuth::new("determinism-test-key").unwrap();
        let sig1 = auth.compute_signature("test-identity", 1000, 2000);
        let sig2 = auth.compute_signature("test-identity", 1000, 2000);
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_compute_signature_varies_with_identity() {
        let auth = SharedSecretAuth::new("variance-test-key-abc").unwrap();
        let sig1 = auth.compute_signature("identity-a", 1000, 2000);
        let sig2 = auth.compute_signature("identity-b", 1000, 2000);
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn test_compute_signature_varies_with_times() {
        let auth = SharedSecretAuth::new("time-variance-key-xyz").unwrap();
        let sig1 = auth.compute_signature("same-identity", 1000, 2000);
        let sig2 = auth.compute_signature("same-identity", 1000, 3000);
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(&[0xff, 0x00, 0xab]), "ff00ab");
        assert_eq!(hex_encode(&[]), "");
    }

    #[test]
    fn test_auth_provider_trait_object() {
        let auth = SharedSecretAuth::new("trait-object-test-key").unwrap();
        let provider: Box<dyn AuthProvider> = Box::new(auth);
        let identity = PeerIdentity::Client(SessionId::generate());
        let token = provider.create_token(&identity, 60_000).unwrap();
        let validated = provider.validate_token(&token).unwrap();
        assert_eq!(validated, identity);
    }

    #[test]
    fn test_shared_secret_minimum_length() {
        assert!(SharedSecretAuth::new("12345678").is_ok());
        assert!(SharedSecretAuth::new("1234567").is_err());
    }

    #[test]
    fn test_auth_full_flow_with_volume_acl() {
        let auth = SharedSecretAuth::new("full-flow-integration-key").unwrap();
        let acl = VolumeAcl::new();

        let vol1 = VolumeId::generate();
        let vol2 = VolumeId::generate();
        let session = SessionId::generate();
        let identity = PeerIdentity::Client(session);

        let token = auth.create_token(&identity, 300_000).unwrap();

        let validated_identity = auth.validate_token(&token).expect("token should be valid");
        assert_eq!(validated_identity, identity);

        acl.grant(
            vol1,
            &validated_identity.to_string(),
            VolumePermission::read_write(),
        );
        acl.grant(
            vol2,
            &validated_identity.to_string(),
            VolumePermission::read_only(),
        );

        assert!(acl.check_read(&vol1, &validated_identity));
        assert!(acl.check_write(&vol1, &validated_identity));

        assert!(acl.check_read(&vol2, &validated_identity));
        assert!(!acl.check_write(&vol2, &validated_identity));

        let other_vol = VolumeId::generate();
        assert!(
            !acl.check_read(&other_vol, &validated_identity),
            "should not have access to ungranted volume"
        );

        let other_auth = SharedSecretAuth::new("different-secret-key-999").unwrap();
        let result = other_auth.validate_token(&token);
        assert!(
            result.is_err(),
            "token from different auth provider should be rejected"
        );
    }
}
