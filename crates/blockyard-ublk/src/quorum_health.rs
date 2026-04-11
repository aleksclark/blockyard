//! Metadata quorum unavailable handling (P5.5, §4.9.1, §6.4).
//!
//! When the metadata quorum is unavailable (e.g., minority partition):
//! - Block new write acks — writes cannot be committed without metadata
//! - Allow reads only when policy and watermark permit
//! - Monitor and detect quorum loss
//! - Resume writes when quorum is restored

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tracing::{info, warn};

use crate::metadata_cache::MetadataCache;
use crate::traits::MetadataClient;
use crate::watermark::WriteWatermark;

/// Policy for reads when quorum is unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuorumLossReadPolicy {
    AllowCachedReads,
    BlockAllReads,
}

/// Current quorum health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuorumStatus {
    Healthy,
    Degraded,
    Unavailable,
}

/// Handles metadata quorum unavailability (§4.9.1, §6.4).
///
/// When the metadata service is unreachable or in a minority partition:
/// - Writes are blocked (no metadata commit possible, invariant 1)
/// - Reads may continue if the cached data satisfies the watermark
/// - Periodic health checks detect when quorum is restored
#[derive(Debug)]
pub struct QuorumHealthMonitor {
    writes_blocked: AtomicBool,
    status: parking_lot::RwLock<QuorumStatus>,
    read_policy: QuorumLossReadPolicy,
    consecutive_failures: AtomicU64,
    failure_threshold: u64,
    recovery_count: AtomicU64,
}

impl QuorumHealthMonitor {
    pub fn new(read_policy: QuorumLossReadPolicy) -> Self {
        Self {
            writes_blocked: AtomicBool::new(false),
            status: parking_lot::RwLock::new(QuorumStatus::Healthy),
            read_policy,
            consecutive_failures: AtomicU64::new(0),
            failure_threshold: 3,
            recovery_count: AtomicU64::new(0),
        }
    }

    pub fn with_failure_threshold(mut self, threshold: u64) -> Self {
        self.failure_threshold = threshold;
        self
    }

    /// Whether writes are currently blocked due to quorum loss.
    pub fn writes_blocked(&self) -> bool {
        self.writes_blocked.load(Ordering::Acquire)
    }

    /// Whether reads are allowed under the current quorum status.
    pub fn reads_allowed(&self) -> bool {
        let status = *self.status.read();
        match status {
            QuorumStatus::Healthy | QuorumStatus::Degraded => true,
            QuorumStatus::Unavailable => self.read_policy == QuorumLossReadPolicy::AllowCachedReads,
        }
    }

    /// Get the current quorum status.
    pub fn status(&self) -> QuorumStatus {
        *self.status.read()
    }

    /// Get the configured read policy.
    pub fn read_policy(&self) -> QuorumLossReadPolicy {
        self.read_policy
    }

    /// Record a successful metadata operation, indicating quorum is healthy.
    pub fn record_success(&self) {
        let prev_failures = self.consecutive_failures.swap(0, Ordering::Relaxed);
        let was_blocked = self.writes_blocked.swap(false, Ordering::Release);

        let mut status = self.status.write();
        if *status != QuorumStatus::Healthy {
            info!(
                previous_status = ?*status,
                previous_failures = prev_failures,
                "metadata quorum restored"
            );
            *status = QuorumStatus::Healthy;
            if was_blocked {
                self.recovery_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Record a failed metadata operation.
    ///
    /// After `failure_threshold` consecutive failures, declares quorum
    /// unavailable and blocks writes.
    pub fn record_failure(&self) {
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;

        if failures >= self.failure_threshold {
            self.writes_blocked.store(true, Ordering::Release);
            let mut status = self.status.write();
            if *status != QuorumStatus::Unavailable {
                warn!(
                    consecutive_failures = failures,
                    "metadata quorum declared unavailable — blocking writes"
                );
                *status = QuorumStatus::Unavailable;
            }
        } else {
            let mut status = self.status.write();
            if *status == QuorumStatus::Healthy {
                *status = QuorumStatus::Degraded;
            }
        }
    }

    /// Check whether a read is allowed given the current watermark state.
    ///
    /// Even when quorum is unavailable, reads may proceed if:
    /// - The read policy allows cached reads
    /// - The cached data is fresh enough relative to the watermark
    pub fn check_read_allowed(&self, watermark: &WriteWatermark, cache: &MetadataCache) -> bool {
        if !self.reads_allowed() {
            return false;
        }

        let status = *self.status.read();
        if status == QuorumStatus::Unavailable {
            let cache_epoch = cache.current_epoch();
            let required = watermark.current();
            cache_epoch.as_u64() >= required.as_u64()
        } else {
            true
        }
    }

    /// Perform a health check against the metadata service.
    ///
    /// Returns true if quorum is available.
    pub async fn health_check<M: MetadataClient>(&self, client: &M) -> bool {
        match client.current_epoch().await {
            Ok(_) => {
                self.record_success();
                true
            }
            Err(e) => {
                warn!(error = %e, "metadata health check failed");
                self.record_failure();
                false
            }
        }
    }

    /// Number of times quorum has been recovered after being unavailable.
    pub fn recovery_count(&self) -> u64 {
        self.recovery_count.load(Ordering::Relaxed)
    }

    /// Number of consecutive failures.
    pub fn consecutive_failures(&self) -> u64 {
        self.consecutive_failures.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_cache::MetadataCache;
    use crate::traits::CommitRequest;
    use crate::watermark::WriteWatermark;
    use blockyard_common::EpochId;
    use blockyard_common::error::Error;

    struct HealthyMetadata;
    impl crate::traits::MetadataClient for HealthyMetadata {
        async fn refresh_metadata(&self, _cache: &MetadataCache) -> Result<EpochId, Error> {
            Ok(EpochId::new(1))
        }
        async fn commit_extent_mapping(
            &self,
            _req: crate::traits::CommitRequest,
        ) -> Result<EpochId, Error> {
            Ok(EpochId::new(1))
        }
        async fn lookup_operation(
            &self,
            _op: &blockyard_common::OperationId,
        ) -> Result<Option<crate::traits::CommittedMapping>, Error> {
            Ok(None)
        }
        async fn current_epoch(&self) -> Result<EpochId, Error> {
            Ok(EpochId::new(1))
        }

        fn commit_extent_mappings_batch(
            &self,
            requests: Vec<CommitRequest>,
        ) -> impl std::future::Future<Output = Result<EpochId, Error>> + Send {
            async move {
                let _ = requests;
                Ok(EpochId::new(1))
            }
        }

        async fn acquire_lease(
            &self,
            _: blockyard_common::VolumeId,
            _: blockyard_common::SessionId,
            _: u64,
            _: u64,
        ) -> Result<blockyard_common::LeaseResponse, Error> {
            Ok(blockyard_common::LeaseResponse::Denied {
                reason: "mock".into(),
            })
        }

        async fn renew_lease(
            &self,
            _: blockyard_common::VolumeId,
            _: blockyard_common::SessionId,
            _: u64,
            _: u64,
        ) -> Result<blockyard_common::LeaseResponse, Error> {
            Ok(blockyard_common::LeaseResponse::Denied {
                reason: "mock".into(),
            })
        }

        async fn release_lease(
            &self,
            _: blockyard_common::VolumeId,
            _: blockyard_common::SessionId,
        ) -> Result<blockyard_common::LeaseResponse, Error> {
            Ok(blockyard_common::LeaseResponse::Released)
        }
    }

    struct UnhealthyMetadata;
    impl crate::traits::MetadataClient for UnhealthyMetadata {
        async fn refresh_metadata(&self, _cache: &MetadataCache) -> Result<EpochId, Error> {
            Err(Error::Raft("no quorum".into()))
        }
        async fn commit_extent_mapping(
            &self,
            _req: crate::traits::CommitRequest,
        ) -> Result<EpochId, Error> {
            Err(Error::Raft("no quorum".into()))
        }
        async fn lookup_operation(
            &self,
            _op: &blockyard_common::OperationId,
        ) -> Result<Option<crate::traits::CommittedMapping>, Error> {
            Err(Error::Raft("no quorum".into()))
        }
        async fn current_epoch(&self) -> Result<EpochId, Error> {
            Err(Error::Raft("no quorum".into()))
        }

        fn commit_extent_mappings_batch(
            &self,
            requests: Vec<CommitRequest>,
        ) -> impl std::future::Future<Output = Result<EpochId, Error>> + Send {
            async move {
                let _ = requests;
                Err(Error::Raft("no quorum".into()))
            }
        }

        async fn acquire_lease(
            &self,
            _: blockyard_common::VolumeId,
            _: blockyard_common::SessionId,
            _: u64,
            _: u64,
        ) -> Result<blockyard_common::LeaseResponse, Error> {
            Ok(blockyard_common::LeaseResponse::Denied {
                reason: "mock".into(),
            })
        }

        async fn renew_lease(
            &self,
            _: blockyard_common::VolumeId,
            _: blockyard_common::SessionId,
            _: u64,
            _: u64,
        ) -> Result<blockyard_common::LeaseResponse, Error> {
            Ok(blockyard_common::LeaseResponse::Denied {
                reason: "mock".into(),
            })
        }

        async fn release_lease(
            &self,
            _: blockyard_common::VolumeId,
            _: blockyard_common::SessionId,
        ) -> Result<blockyard_common::LeaseResponse, Error> {
            Ok(blockyard_common::LeaseResponse::Released)
        }
    }

    #[test]
    fn test_new_monitor_healthy() {
        let m = QuorumHealthMonitor::new(QuorumLossReadPolicy::AllowCachedReads);
        assert_eq!(m.status(), QuorumStatus::Healthy);
        assert!(!m.writes_blocked());
        assert!(m.reads_allowed());
    }

    #[test]
    fn test_single_failure_degrades() {
        let m = QuorumHealthMonitor::new(QuorumLossReadPolicy::AllowCachedReads);
        m.record_failure();
        assert_eq!(m.status(), QuorumStatus::Degraded);
        assert!(!m.writes_blocked());
    }

    #[test]
    fn test_threshold_failures_blocks_writes() {
        let m = QuorumHealthMonitor::new(QuorumLossReadPolicy::AllowCachedReads)
            .with_failure_threshold(3);
        m.record_failure();
        m.record_failure();
        assert_eq!(m.status(), QuorumStatus::Degraded);
        assert!(!m.writes_blocked());

        m.record_failure();
        assert_eq!(m.status(), QuorumStatus::Unavailable);
        assert!(m.writes_blocked());
    }

    #[test]
    fn test_success_after_failure_recovers() {
        let m = QuorumHealthMonitor::new(QuorumLossReadPolicy::AllowCachedReads)
            .with_failure_threshold(2);
        m.record_failure();
        m.record_failure();
        assert_eq!(m.status(), QuorumStatus::Unavailable);
        assert!(m.writes_blocked());

        m.record_success();
        assert_eq!(m.status(), QuorumStatus::Healthy);
        assert!(!m.writes_blocked());
        assert_eq!(m.recovery_count(), 1);
    }

    #[test]
    fn test_reads_blocked_when_policy_blocks() {
        let m =
            QuorumHealthMonitor::new(QuorumLossReadPolicy::BlockAllReads).with_failure_threshold(1);
        m.record_failure();
        assert!(!m.reads_allowed());
    }

    #[test]
    fn test_reads_allowed_when_policy_allows_cached() {
        let m = QuorumHealthMonitor::new(QuorumLossReadPolicy::AllowCachedReads)
            .with_failure_threshold(1);
        m.record_failure();
        assert!(m.reads_allowed());
    }

    #[test]
    fn test_check_read_allowed_fresh_cache() {
        let m = QuorumHealthMonitor::new(QuorumLossReadPolicy::AllowCachedReads)
            .with_failure_threshold(1);
        m.record_failure();

        let wm = WriteWatermark::new();
        wm.advance(EpochId::new(5));
        let cache = MetadataCache::new();
        cache.set_epoch(EpochId::new(5));

        assert!(m.check_read_allowed(&wm, &cache));
    }

    #[test]
    fn test_check_read_allowed_stale_cache() {
        let m = QuorumHealthMonitor::new(QuorumLossReadPolicy::AllowCachedReads)
            .with_failure_threshold(1);
        m.record_failure();

        let wm = WriteWatermark::new();
        wm.advance(EpochId::new(10));
        let cache = MetadataCache::new();
        cache.set_epoch(EpochId::new(5));

        assert!(!m.check_read_allowed(&wm, &cache));
    }

    #[test]
    fn test_check_read_allowed_block_policy_unavailable() {
        let m =
            QuorumHealthMonitor::new(QuorumLossReadPolicy::BlockAllReads).with_failure_threshold(1);
        m.record_failure();

        let wm = WriteWatermark::new();
        let cache = MetadataCache::new();
        assert!(!m.check_read_allowed(&wm, &cache));
    }

    #[test]
    fn test_check_read_allowed_healthy() {
        let m = QuorumHealthMonitor::new(QuorumLossReadPolicy::BlockAllReads);
        let wm = WriteWatermark::new();
        let cache = MetadataCache::new();
        assert!(m.check_read_allowed(&wm, &cache));
    }

    #[tokio::test]
    async fn test_health_check_healthy() {
        let m = QuorumHealthMonitor::new(QuorumLossReadPolicy::AllowCachedReads);
        let client = HealthyMetadata;
        assert!(m.health_check(&client).await);
        assert_eq!(m.status(), QuorumStatus::Healthy);
    }

    #[tokio::test]
    async fn test_health_check_unhealthy() {
        let m = QuorumHealthMonitor::new(QuorumLossReadPolicy::AllowCachedReads)
            .with_failure_threshold(1);
        let client = UnhealthyMetadata;
        assert!(!m.health_check(&client).await);
        assert_eq!(m.status(), QuorumStatus::Unavailable);
    }

    #[tokio::test]
    async fn test_health_check_recovery() {
        let m = QuorumHealthMonitor::new(QuorumLossReadPolicy::AllowCachedReads)
            .with_failure_threshold(1);

        let unhealthy = UnhealthyMetadata;
        m.health_check(&unhealthy).await;
        assert_eq!(m.status(), QuorumStatus::Unavailable);

        let healthy = HealthyMetadata;
        m.health_check(&healthy).await;
        assert_eq!(m.status(), QuorumStatus::Healthy);
        assert_eq!(m.recovery_count(), 1);
    }

    #[test]
    fn test_consecutive_failures_count() {
        let m = QuorumHealthMonitor::new(QuorumLossReadPolicy::AllowCachedReads);
        assert_eq!(m.consecutive_failures(), 0);
        m.record_failure();
        assert_eq!(m.consecutive_failures(), 1);
        m.record_failure();
        assert_eq!(m.consecutive_failures(), 2);
        m.record_success();
        assert_eq!(m.consecutive_failures(), 0);
    }

    #[test]
    fn test_quorum_status_debug() {
        assert!(format!("{:?}", QuorumStatus::Healthy).contains("Healthy"));
        assert!(format!("{:?}", QuorumStatus::Degraded).contains("Degraded"));
        assert!(format!("{:?}", QuorumStatus::Unavailable).contains("Unavailable"));
    }

    #[test]
    fn test_quorum_status_clone_eq() {
        let s = QuorumStatus::Degraded;
        assert_eq!(s, s.clone());
    }

    #[test]
    fn test_read_policy_debug() {
        let p = QuorumLossReadPolicy::AllowCachedReads;
        assert!(format!("{:?}", p).contains("AllowCachedReads"));
    }

    #[test]
    fn test_read_policy_clone_eq() {
        let p = QuorumLossReadPolicy::BlockAllReads;
        assert_eq!(p, p.clone());
    }

    #[test]
    fn test_read_policy_accessor() {
        let m = QuorumHealthMonitor::new(QuorumLossReadPolicy::BlockAllReads);
        assert_eq!(m.read_policy(), QuorumLossReadPolicy::BlockAllReads);
    }

    #[test]
    fn test_multiple_recoveries() {
        let m = QuorumHealthMonitor::new(QuorumLossReadPolicy::AllowCachedReads)
            .with_failure_threshold(1);

        m.record_failure();
        assert_eq!(m.status(), QuorumStatus::Unavailable);
        m.record_success();
        assert_eq!(m.recovery_count(), 1);

        m.record_failure();
        assert_eq!(m.status(), QuorumStatus::Unavailable);
        m.record_success();
        assert_eq!(m.recovery_count(), 2);
    }

    #[test]
    fn test_success_when_already_healthy() {
        let m = QuorumHealthMonitor::new(QuorumLossReadPolicy::AllowCachedReads);
        m.record_success();
        assert_eq!(m.status(), QuorumStatus::Healthy);
        assert_eq!(m.recovery_count(), 0);
    }
}
