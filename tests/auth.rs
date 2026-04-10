//! Authentication and authorization integration tests — verify the full auth
//! flow from token generation through validation and volume ACL checks.

use blockyard_common::{
    AuthProvider, AuthToken, PeerIdentity, SessionId, SharedSecretAuth, VolumeAcl, VolumeId,
    VolumePermission,
};

// ===========================================================================
// Test 7: SharedSecretAuth generate + validate, including expiry rejection
// ===========================================================================

#[test]
fn test_shared_secret_auth_generate_validate() {
    let auth = SharedSecretAuth::new("integration-test-secret-key").unwrap();
    let session_id = SessionId::generate();
    let identity = PeerIdentity::Client(session_id);

    let token = auth.create_token(&identity, 60_000).unwrap();
    assert!(token.identity.starts_with("client:"));
    assert!(!token.signature.is_empty());
    assert!(token.expires_at_ms > token.issued_at_ms);

    let validated = auth.validate_token(&token).unwrap();
    assert_eq!(validated, identity);

    let expired_token = AuthToken {
        identity: identity.to_string(),
        issued_at_ms: 0,
        expires_at_ms: 1,
        signature: "dummy-sig".to_string(),
    };
    let result = auth.validate_token(&expired_token);
    assert!(result.is_err(), "expired token must be rejected");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("expired"),
        "error should mention expiry: {err_msg}"
    );

    let mut tampered = auth.create_token(&identity, 300_000).unwrap();
    tampered.signature = "deadbeefdeadbeef".to_string();
    let result = auth.validate_token(&tampered);
    assert!(result.is_err(), "tampered signature must be rejected");
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("invalid token signature")
    );
}

// ===========================================================================
// Test 8: VolumeAcl grant/deny for read-write vs read-only vs unknown
// ===========================================================================

#[test]
fn test_volume_acl_grants_denies() {
    let acl = VolumeAcl::new();
    let vol = VolumeId::generate();

    let rw_session = SessionId::generate();
    let rw_identity = PeerIdentity::Client(rw_session);

    let ro_session = SessionId::generate();
    let ro_identity = PeerIdentity::Client(ro_session);

    let unknown_session = SessionId::generate();
    let unknown_identity = PeerIdentity::Client(unknown_session);

    acl.grant(
        vol,
        &rw_identity.to_string(),
        VolumePermission::read_write(),
    );
    acl.grant(vol, &ro_identity.to_string(), VolumePermission::read_only());

    assert!(
        acl.check_read(&vol, &rw_identity),
        "read-write identity should have read access"
    );
    assert!(
        acl.check_write(&vol, &rw_identity),
        "read-write identity should have write access"
    );

    assert!(
        acl.check_read(&vol, &ro_identity),
        "read-only identity should have read access"
    );
    assert!(
        !acl.check_write(&vol, &ro_identity),
        "read-only identity should NOT have write access"
    );

    assert!(
        !acl.check_read(&vol, &unknown_identity),
        "unknown identity should NOT have read access"
    );
    assert!(
        !acl.check_write(&vol, &unknown_identity),
        "unknown identity should NOT have write access"
    );

    let node_identity = PeerIdentity::Node(blockyard_common::NodeId::generate());
    assert!(
        acl.check_read(&vol, &node_identity),
        "node identity should always have read access"
    );
    assert!(
        acl.check_write(&vol, &node_identity),
        "node identity should always have write access"
    );

    let perms = acl.list_permissions(&vol);
    assert_eq!(perms.len(), 2);
}

// ===========================================================================
// Test 9: VolumeAcl revoke removes access
// ===========================================================================

#[test]
fn test_volume_acl_revoke() {
    let acl = VolumeAcl::new();
    let vol = VolumeId::generate();
    let session = SessionId::generate();
    let identity = PeerIdentity::Client(session);

    acl.grant(vol, &identity.to_string(), VolumePermission::read_write());
    assert!(acl.check_read(&vol, &identity));
    assert!(acl.check_write(&vol, &identity));

    acl.revoke(vol, &identity.to_string());

    assert!(
        !acl.check_read(&vol, &identity),
        "read should be denied after revoke"
    );
    assert!(
        !acl.check_write(&vol, &identity),
        "write should be denied after revoke"
    );

    let perms = acl.list_permissions(&vol);
    assert!(
        perms.is_empty(),
        "permissions list should be empty after revoking the only entry"
    );
}

// ===========================================================================
// Test 10: Full auth flow — generate token, validate, then check volume ACL
// ===========================================================================

#[test]
fn test_auth_with_service_context() {
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
