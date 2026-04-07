//! Volume write lease types (P6.1, §4.8).
//!
//! A [`VolumeLease`] grants exclusive write access to a single client session
//! for a given volume. Leases are stored in the Raft state machine, acquired
//! via proposal, and must be renewed before expiry to remain valid.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{SessionId, VolumeId};

/// Default lease time-to-live.
pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);

/// Monotonically increasing lease version for fencing.
pub type LeaseVersion = u64;

/// An exclusive write lease on a volume (P6.1).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VolumeLease {
    pub volume_id: VolumeId,
    pub holder: SessionId,
    pub granted_at_ms: u64,
    pub expires_at_ms: u64,
    pub lease_version: LeaseVersion,
}

impl VolumeLease {
    pub fn is_expired(&self, now_ms: u64) -> bool {
        now_ms >= self.expires_at_ms
    }

    pub fn is_held_by(&self, session_id: SessionId) -> bool {
        self.holder == session_id
    }

    pub fn remaining_ms(&self, now_ms: u64) -> u64 {
        self.expires_at_ms.saturating_sub(now_ms)
    }
}

/// Request to acquire, renew, or release a volume write lease.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LeaseRequest {
    Acquire {
        volume_id: VolumeId,
        session_id: SessionId,
        now_ms: u64,
        ttl_ms: u64,
    },
    Renew {
        volume_id: VolumeId,
        session_id: SessionId,
        now_ms: u64,
        ttl_ms: u64,
    },
    Release {
        volume_id: VolumeId,
        session_id: SessionId,
    },
}

/// Response to a lease request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LeaseResponse {
    Granted {
        lease_version: LeaseVersion,
        expires_at_ms: u64,
    },
    Renewed {
        lease_version: LeaseVersion,
        expires_at_ms: u64,
    },
    Released,
    Denied {
        reason: String,
    },
}

impl LeaseResponse {
    pub fn is_success(&self) -> bool {
        !matches!(self, LeaseResponse::Denied { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_lease(now_ms: u64, ttl_ms: u64) -> VolumeLease {
        VolumeLease {
            volume_id: VolumeId::generate(),
            holder: SessionId::generate(),
            granted_at_ms: now_ms,
            expires_at_ms: now_ms + ttl_ms,
            lease_version: 1,
        }
    }

    #[test]
    fn test_volume_lease_not_expired() {
        let lease = make_lease(1000, 30_000);
        assert!(!lease.is_expired(1000));
        assert!(!lease.is_expired(15_000));
        assert!(!lease.is_expired(30_999));
    }

    #[test]
    fn test_volume_lease_expired_at_boundary() {
        let lease = make_lease(1000, 30_000);
        assert!(lease.is_expired(31_000));
    }

    #[test]
    fn test_volume_lease_expired_after() {
        let lease = make_lease(1000, 30_000);
        assert!(lease.is_expired(50_000));
    }

    #[test]
    fn test_volume_lease_is_held_by() {
        let sid = SessionId::generate();
        let lease = VolumeLease {
            volume_id: VolumeId::generate(),
            holder: sid,
            granted_at_ms: 0,
            expires_at_ms: 30_000,
            lease_version: 1,
        };
        assert!(lease.is_held_by(sid));
        assert!(!lease.is_held_by(SessionId::generate()));
    }

    #[test]
    fn test_volume_lease_remaining_ms() {
        let lease = make_lease(1000, 30_000);
        assert_eq!(lease.remaining_ms(1000), 30_000);
        assert_eq!(lease.remaining_ms(16_000), 15_000);
        assert_eq!(lease.remaining_ms(31_000), 0);
        assert_eq!(lease.remaining_ms(50_000), 0);
    }

    #[test]
    fn test_lease_response_is_success_granted() {
        let resp = LeaseResponse::Granted {
            lease_version: 1,
            expires_at_ms: 30_000,
        };
        assert!(resp.is_success());
    }

    #[test]
    fn test_lease_response_is_success_renewed() {
        let resp = LeaseResponse::Renewed {
            lease_version: 2,
            expires_at_ms: 60_000,
        };
        assert!(resp.is_success());
    }

    #[test]
    fn test_lease_response_is_success_released() {
        let resp = LeaseResponse::Released;
        assert!(resp.is_success());
    }

    #[test]
    fn test_lease_response_denied() {
        let resp = LeaseResponse::Denied {
            reason: "held by another session".into(),
        };
        assert!(!resp.is_success());
    }

    #[test]
    fn test_volume_lease_serde_roundtrip() {
        let lease = make_lease(5000, 30_000);
        let json = serde_json::to_string(&lease).unwrap();
        let parsed: VolumeLease = serde_json::from_str(&json).unwrap();
        assert_eq!(lease, parsed);
    }

    #[test]
    fn test_lease_request_acquire_serde() {
        let req = LeaseRequest::Acquire {
            volume_id: VolumeId::generate(),
            session_id: SessionId::generate(),
            now_ms: 1000,
            ttl_ms: 30_000,
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: LeaseRequest = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, LeaseRequest::Acquire { .. }));
    }

    #[test]
    fn test_lease_request_renew_serde() {
        let req = LeaseRequest::Renew {
            volume_id: VolumeId::generate(),
            session_id: SessionId::generate(),
            now_ms: 15_000,
            ttl_ms: 30_000,
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: LeaseRequest = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, LeaseRequest::Renew { .. }));
    }

    #[test]
    fn test_lease_request_release_serde() {
        let req = LeaseRequest::Release {
            volume_id: VolumeId::generate(),
            session_id: SessionId::generate(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: LeaseRequest = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, LeaseRequest::Release { .. }));
    }

    #[test]
    fn test_lease_response_serde_roundtrip() {
        let responses = vec![
            LeaseResponse::Granted {
                lease_version: 1,
                expires_at_ms: 30_000,
            },
            LeaseResponse::Renewed {
                lease_version: 2,
                expires_at_ms: 60_000,
            },
            LeaseResponse::Released,
            LeaseResponse::Denied {
                reason: "test".into(),
            },
        ];
        for resp in &responses {
            let json = serde_json::to_string(resp).unwrap();
            let parsed: LeaseResponse = serde_json::from_str(&json).unwrap();
            assert_eq!(*resp, parsed);
        }
    }

    #[test]
    fn test_volume_lease_debug() {
        let lease = make_lease(0, 30_000);
        let debug = format!("{:?}", lease);
        assert!(debug.contains("VolumeLease"));
    }

    #[test]
    fn test_volume_lease_clone() {
        let lease = make_lease(0, 30_000);
        let cloned = lease.clone();
        assert_eq!(lease, cloned);
    }

    #[test]
    fn test_lease_request_debug() {
        let req = LeaseRequest::Acquire {
            volume_id: VolumeId::generate(),
            session_id: SessionId::generate(),
            now_ms: 0,
            ttl_ms: 30_000,
        };
        let debug = format!("{:?}", req);
        assert!(debug.contains("Acquire"));
    }

    #[test]
    fn test_lease_request_clone() {
        let req = LeaseRequest::Release {
            volume_id: VolumeId::generate(),
            session_id: SessionId::generate(),
        };
        let cloned = req.clone();
        assert!(matches!(cloned, LeaseRequest::Release { .. }));
    }

    #[test]
    fn test_default_lease_ttl() {
        assert_eq!(DEFAULT_LEASE_TTL, Duration::from_secs(30));
    }
}
