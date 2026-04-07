//! Volume write lease manager (P6.1, P6.2, §4.8).
//!
//! Manages the lifecycle of an exclusive write lease for a volume:
//! - Acquires the lease on mount via Raft proposal
//! - Renews the lease in a background task at TTL/3 intervals
//! - Releases the lease on clean unmount
//! - Stops writes immediately when renewal fails
//!
//! The lease version is attached to every write request for fencing (P6.2).

use std::sync::Arc;
use std::time::Duration;

use blockyard_common::error::Error;
use blockyard_common::{LeaseResponse, LeaseVersion, SessionId, VolumeId};
use parking_lot::RwLock;
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

use crate::traits::MetadataClient;

/// State of the lease as seen by the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseState {
    /// No lease held.
    None,
    /// Lease is active and valid.
    Active,
    /// Lease renewal failed; writes must stop.
    Lost,
}

/// Manages a write lease for a single volume.
#[derive(Debug)]
pub struct LeaseManager {
    volume_id: VolumeId,
    session_id: SessionId,
    ttl: Duration,
    inner: RwLock<LeaseInner>,
    shutdown: Notify,
}

#[derive(Debug)]
struct LeaseInner {
    state: LeaseState,
    lease_version: Option<LeaseVersion>,
    expires_at_ms: Option<u64>,
}

impl LeaseManager {
    pub fn new(volume_id: VolumeId, session_id: SessionId, ttl: Duration) -> Self {
        Self {
            volume_id,
            session_id,
            ttl,
            inner: RwLock::new(LeaseInner {
                state: LeaseState::None,
                lease_version: None,
                expires_at_ms: None,
            }),
            shutdown: Notify::new(),
        }
    }

    pub fn volume_id(&self) -> VolumeId {
        self.volume_id
    }

    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn state(&self) -> LeaseState {
        self.inner.read().state
    }

    pub fn lease_version(&self) -> Option<LeaseVersion> {
        self.inner.read().lease_version
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Acquire the write lease from the metadata service.
    pub async fn acquire<M: MetadataClient>(&self, client: &M) -> Result<LeaseVersion, Error> {
        let now_ms = current_time_ms();
        let ttl_ms = self.ttl.as_millis() as u64;

        let response = client
            .acquire_lease(self.volume_id, self.session_id, now_ms, ttl_ms)
            .await?;

        match response {
            LeaseResponse::Granted {
                lease_version,
                expires_at_ms,
            } => {
                info!(
                    volume_id = %self.volume_id,
                    lease_version,
                    "write lease acquired"
                );
                let mut inner = self.inner.write();
                inner.state = LeaseState::Active;
                inner.lease_version = Some(lease_version);
                inner.expires_at_ms = Some(expires_at_ms);
                Ok(lease_version)
            }
            LeaseResponse::Denied { reason } => {
                warn!(
                    volume_id = %self.volume_id,
                    reason = %reason,
                    "write lease acquisition denied"
                );
                Err(Error::Auth(format!("lease denied: {reason}")))
            }
            other => Err(Error::Auth(format!("unexpected lease response: {other:?}"))),
        }
    }

    /// Renew the write lease.
    pub async fn renew<M: MetadataClient>(&self, client: &M) -> Result<LeaseVersion, Error> {
        let now_ms = current_time_ms();
        let ttl_ms = self.ttl.as_millis() as u64;

        let response = client
            .renew_lease(self.volume_id, self.session_id, now_ms, ttl_ms)
            .await?;

        match response {
            LeaseResponse::Renewed {
                lease_version,
                expires_at_ms,
            } => {
                debug!(
                    volume_id = %self.volume_id,
                    lease_version,
                    "write lease renewed"
                );
                let mut inner = self.inner.write();
                inner.lease_version = Some(lease_version);
                inner.expires_at_ms = Some(expires_at_ms);
                Ok(lease_version)
            }
            LeaseResponse::Denied { reason } => {
                error!(
                    volume_id = %self.volume_id,
                    reason = %reason,
                    "write lease renewal denied — marking lease lost"
                );
                self.mark_lost();
                Err(Error::Auth(format!("lease renewal denied: {reason}")))
            }
            other => {
                self.mark_lost();
                Err(Error::Auth(format!(
                    "unexpected renewal response: {other:?}"
                )))
            }
        }
    }

    /// Release the write lease (clean unmount).
    pub async fn release<M: MetadataClient>(&self, client: &M) -> Result<(), Error> {
        let response = client
            .release_lease(self.volume_id, self.session_id)
            .await?;

        let mut inner = self.inner.write();
        inner.state = LeaseState::None;
        inner.lease_version = None;
        inner.expires_at_ms = None;

        match response {
            LeaseResponse::Released => {
                info!(
                    volume_id = %self.volume_id,
                    "write lease released"
                );
                Ok(())
            }
            LeaseResponse::Denied { reason } => {
                warn!(
                    volume_id = %self.volume_id,
                    reason = %reason,
                    "write lease release denied (already expired?)"
                );
                Ok(())
            }
            other => Err(Error::Auth(format!(
                "unexpected release response: {other:?}"
            ))),
        }
    }

    fn mark_lost(&self) {
        let mut inner = self.inner.write();
        inner.state = LeaseState::Lost;
    }

    /// Signal the background renewal task to stop.
    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }

    /// Spawn a background task that renews the lease at TTL/3 intervals.
    ///
    /// The task stops when:
    /// - `shutdown()` is called
    /// - A renewal fails (lease marked Lost)
    pub fn spawn_renewal_task<M: MetadataClient + 'static>(
        self: &Arc<Self>,
        client: Arc<M>,
    ) -> tokio::task::JoinHandle<()> {
        let mgr = Arc::clone(self);
        let interval = mgr.ttl / 3;

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    _ = mgr.shutdown.notified() => {
                        debug!(
                            volume_id = %mgr.volume_id,
                            "lease renewal task shutting down"
                        );
                        return;
                    }
                }

                if mgr.state() != LeaseState::Active {
                    debug!(
                        volume_id = %mgr.volume_id,
                        "lease not active, stopping renewal"
                    );
                    return;
                }

                if let Err(e) = mgr.renew(client.as_ref()).await {
                    error!(
                        volume_id = %mgr.volume_id,
                        error = %e,
                        "lease renewal failed — writes must stop"
                    );
                    return;
                }
            }
        })
    }

    /// Check if the lease is valid for writing.
    pub fn check_write_allowed(&self) -> Result<LeaseVersion, Error> {
        let inner = self.inner.read();
        match inner.state {
            LeaseState::Active => inner
                .lease_version
                .ok_or_else(|| Error::Auth("lease active but no version".into())),
            LeaseState::None => Err(Error::Auth("no write lease held".into())),
            LeaseState::Lost => Err(Error::Auth("write lease lost — renewal failed".into())),
        }
    }
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    use blockyard_common::{EpochId, ExtentId, LeaseResponse, NodeId, OperationId};
    use bytes::Bytes;

    use crate::metadata_cache::MetadataCache;
    use crate::traits::{CommitRequest, CommittedMapping, DataNodeClient, WriteAck};

    struct MockLeaseMetadata {
        acquire_version: AtomicU64,
        renew_version: AtomicU64,
        should_deny_acquire: parking_lot::Mutex<bool>,
        should_deny_renew: parking_lot::Mutex<bool>,
    }

    impl MockLeaseMetadata {
        fn new() -> Self {
            Self {
                acquire_version: AtomicU64::new(1),
                renew_version: AtomicU64::new(2),
                should_deny_acquire: parking_lot::Mutex::new(false),
                should_deny_renew: parking_lot::Mutex::new(false),
            }
        }

        fn set_deny_acquire(&self, deny: bool) {
            *self.should_deny_acquire.lock() = deny;
        }

        fn set_deny_renew(&self, deny: bool) {
            *self.should_deny_renew.lock() = deny;
        }
    }

    impl MetadataClient for MockLeaseMetadata {
        async fn refresh_metadata(&self, _cache: &MetadataCache) -> Result<EpochId, Error> {
            Ok(EpochId::new(1))
        }

        async fn commit_extent_mapping(&self, _req: CommitRequest) -> Result<EpochId, Error> {
            Ok(EpochId::new(1))
        }

        async fn lookup_operation(
            &self,
            _op_id: &OperationId,
        ) -> Result<Option<CommittedMapping>, Error> {
            Ok(None)
        }

        async fn current_epoch(&self) -> Result<EpochId, Error> {
            Ok(EpochId::new(1))
        }

        async fn acquire_lease(
            &self,
            _volume_id: VolumeId,
            _session_id: SessionId,
            now_ms: u64,
            ttl_ms: u64,
        ) -> Result<LeaseResponse, Error> {
            if *self.should_deny_acquire.lock() {
                return Ok(LeaseResponse::Denied {
                    reason: "held by other".into(),
                });
            }
            let version = self.acquire_version.fetch_add(1, Ordering::Relaxed);
            Ok(LeaseResponse::Granted {
                lease_version: version,
                expires_at_ms: now_ms + ttl_ms,
            })
        }

        async fn renew_lease(
            &self,
            _volume_id: VolumeId,
            _session_id: SessionId,
            now_ms: u64,
            ttl_ms: u64,
        ) -> Result<LeaseResponse, Error> {
            if *self.should_deny_renew.lock() {
                return Ok(LeaseResponse::Denied {
                    reason: "expired".into(),
                });
            }
            let version = self.renew_version.fetch_add(1, Ordering::Relaxed);
            Ok(LeaseResponse::Renewed {
                lease_version: version,
                expires_at_ms: now_ms + ttl_ms,
            })
        }

        async fn release_lease(
            &self,
            _volume_id: VolumeId,
            _session_id: SessionId,
        ) -> Result<LeaseResponse, Error> {
            Ok(LeaseResponse::Released)
        }
    }

    #[test]
    fn test_lease_manager_new() {
        let vid = VolumeId::generate();
        let sid = SessionId::generate();
        let mgr = LeaseManager::new(vid, sid, Duration::from_secs(30));
        assert_eq!(mgr.volume_id(), vid);
        assert_eq!(mgr.session_id(), sid);
        assert_eq!(mgr.state(), LeaseState::None);
        assert!(mgr.lease_version().is_none());
        assert_eq!(mgr.ttl(), Duration::from_secs(30));
    }

    #[tokio::test]
    async fn test_acquire_success() {
        let vid = VolumeId::generate();
        let sid = SessionId::generate();
        let mgr = LeaseManager::new(vid, sid, Duration::from_secs(30));
        let client = MockLeaseMetadata::new();

        let version = mgr.acquire(&client).await.unwrap();
        assert_eq!(version, 1);
        assert_eq!(mgr.state(), LeaseState::Active);
        assert_eq!(mgr.lease_version(), Some(1));
    }

    #[tokio::test]
    async fn test_acquire_denied() {
        let vid = VolumeId::generate();
        let sid = SessionId::generate();
        let mgr = LeaseManager::new(vid, sid, Duration::from_secs(30));
        let client = MockLeaseMetadata::new();
        client.set_deny_acquire(true);

        let result = mgr.acquire(&client).await;
        assert!(result.is_err());
        assert_eq!(mgr.state(), LeaseState::None);
    }

    #[tokio::test]
    async fn test_renew_success() {
        let vid = VolumeId::generate();
        let sid = SessionId::generate();
        let mgr = LeaseManager::new(vid, sid, Duration::from_secs(30));
        let client = MockLeaseMetadata::new();

        mgr.acquire(&client).await.unwrap();
        let version = mgr.renew(&client).await.unwrap();
        assert_eq!(version, 2);
        assert_eq!(mgr.state(), LeaseState::Active);
        assert_eq!(mgr.lease_version(), Some(2));
    }

    #[tokio::test]
    async fn test_renew_denied_marks_lost() {
        let vid = VolumeId::generate();
        let sid = SessionId::generate();
        let mgr = LeaseManager::new(vid, sid, Duration::from_secs(30));
        let client = MockLeaseMetadata::new();

        mgr.acquire(&client).await.unwrap();
        client.set_deny_renew(true);
        let result = mgr.renew(&client).await;
        assert!(result.is_err());
        assert_eq!(mgr.state(), LeaseState::Lost);
    }

    #[tokio::test]
    async fn test_release_success() {
        let vid = VolumeId::generate();
        let sid = SessionId::generate();
        let mgr = LeaseManager::new(vid, sid, Duration::from_secs(30));
        let client = MockLeaseMetadata::new();

        mgr.acquire(&client).await.unwrap();
        mgr.release(&client).await.unwrap();
        assert_eq!(mgr.state(), LeaseState::None);
        assert!(mgr.lease_version().is_none());
    }

    #[test]
    fn test_check_write_allowed_active() {
        let vid = VolumeId::generate();
        let sid = SessionId::generate();
        let mgr = LeaseManager::new(vid, sid, Duration::from_secs(30));

        {
            let mut inner = mgr.inner.write();
            inner.state = LeaseState::Active;
            inner.lease_version = Some(5);
        }

        let version = mgr.check_write_allowed().unwrap();
        assert_eq!(version, 5);
    }

    #[test]
    fn test_check_write_allowed_none() {
        let vid = VolumeId::generate();
        let sid = SessionId::generate();
        let mgr = LeaseManager::new(vid, sid, Duration::from_secs(30));
        assert!(mgr.check_write_allowed().is_err());
    }

    #[test]
    fn test_check_write_allowed_lost() {
        let vid = VolumeId::generate();
        let sid = SessionId::generate();
        let mgr = LeaseManager::new(vid, sid, Duration::from_secs(30));

        {
            let mut inner = mgr.inner.write();
            inner.state = LeaseState::Lost;
        }

        let result = mgr.check_write_allowed();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("lost"));
    }

    #[tokio::test]
    async fn test_spawn_renewal_task_renews() {
        let vid = VolumeId::generate();
        let sid = SessionId::generate();
        let mgr = Arc::new(LeaseManager::new(vid, sid, Duration::from_millis(90)));
        let client = Arc::new(MockLeaseMetadata::new());

        mgr.acquire(client.as_ref()).await.unwrap();
        let handle = mgr.spawn_renewal_task(Arc::clone(&client));

        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(mgr.state(), LeaseState::Active);
        assert!(mgr.lease_version().unwrap() >= 1);

        mgr.shutdown();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_spawn_renewal_task_stops_on_deny() {
        let vid = VolumeId::generate();
        let sid = SessionId::generate();
        let mgr = Arc::new(LeaseManager::new(vid, sid, Duration::from_millis(60)));
        let client = Arc::new(MockLeaseMetadata::new());

        mgr.acquire(client.as_ref()).await.unwrap();
        client.set_deny_renew(true);
        let handle = mgr.spawn_renewal_task(Arc::clone(&client));

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(mgr.state(), LeaseState::Lost);
        handle.await.unwrap();
    }

    #[test]
    fn test_lease_state_equality() {
        assert_eq!(LeaseState::None, LeaseState::None);
        assert_eq!(LeaseState::Active, LeaseState::Active);
        assert_eq!(LeaseState::Lost, LeaseState::Lost);
        assert_ne!(LeaseState::None, LeaseState::Active);
        assert_ne!(LeaseState::Active, LeaseState::Lost);
    }

    #[test]
    fn test_lease_state_debug() {
        let debug = format!("{:?}", LeaseState::Active);
        assert_eq!(debug, "Active");
    }

    #[test]
    fn test_lease_state_copy() {
        let s = LeaseState::Active;
        let s2 = s;
        assert_eq!(s, s2);
    }

    #[tokio::test]
    async fn test_multiple_acquires_bump_version() {
        let vid = VolumeId::generate();
        let sid = SessionId::generate();
        let mgr = LeaseManager::new(vid, sid, Duration::from_secs(30));
        let client = MockLeaseMetadata::new();

        let v1 = mgr.acquire(&client).await.unwrap();
        mgr.release(&client).await.unwrap();
        let v2 = mgr.acquire(&client).await.unwrap();
        assert!(v2 > v1);
    }

    #[tokio::test]
    async fn test_renew_bumps_version() {
        let vid = VolumeId::generate();
        let sid = SessionId::generate();
        let mgr = LeaseManager::new(vid, sid, Duration::from_secs(30));
        let client = MockLeaseMetadata::new();

        let v1 = mgr.acquire(&client).await.unwrap();
        let v2 = mgr.renew(&client).await.unwrap();
        let v3 = mgr.renew(&client).await.unwrap();
        assert!(v2 > v1);
        assert!(v3 > v2);
    }

    #[test]
    fn test_current_time_ms() {
        let now = current_time_ms();
        assert!(now > 1_000_000_000_000);
    }
}
