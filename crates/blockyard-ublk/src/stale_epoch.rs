//! Stale epoch refresh and retry logic (P4A.6, §4.7).
//!
//! When a data node rejects an operation with `StaleEpoch`, the handler:
//! 1. Stops issuing new writes with the stale epoch
//! 2. Refreshes the cluster map and placement via [`MetadataClient`]
//! 3. Updates the [`MetadataCache`]
//! 4. Re-resolves targets under the new epoch

use blockyard_common::EpochId;
use blockyard_common::error::Error;

use crate::metadata_cache::MetadataCache;
use crate::traits::MetadataClient;

/// Handles stale epoch detection and refresh (§4.7).
///
/// When a `StaleEpoch` error is received, the handler coordinates:
/// - Pausing writes
/// - Refreshing metadata from the metadata service
/// - Updating the cache
/// - Signaling readiness to retry
///
/// Uses a `tokio::sync::Mutex` to deduplicate concurrent refresh calls —
/// only the first caller performs the actual refresh; subsequent callers
/// wait and reuse the result.
#[derive(Debug)]
pub struct StaleEpochHandler {
    paused: std::sync::atomic::AtomicBool,
    refresh_count: std::sync::atomic::AtomicU64,
    refresh_lock: tokio::sync::Mutex<()>,
}

impl StaleEpochHandler {
    pub fn new() -> Self {
        Self {
            paused: std::sync::atomic::AtomicBool::new(false),
            refresh_count: std::sync::atomic::AtomicU64::new(0),
            refresh_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Whether writes are currently paused due to a stale epoch.
    pub fn is_paused(&self) -> bool {
        self.paused.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Manually set the paused state (for testing).
    pub fn set_paused(&self, paused: bool) {
        self.paused
            .store(paused, std::sync::atomic::Ordering::Release);
    }

    /// Number of refreshes performed.
    pub fn refresh_count(&self) -> u64 {
        self.refresh_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Handle a stale epoch error: pause writes, refresh metadata, unpause.
    ///
    /// Returns the new epoch after refresh.
    pub async fn handle_stale_epoch<M: MetadataClient>(
        &self,
        cache: &MetadataCache,
        client: &M,
        stale_epoch: EpochId,
    ) -> Result<EpochId, Error> {
        // Acquire lock to deduplicate concurrent refresh calls (I8).
        let _guard = self.refresh_lock.lock().await;

        self.paused
            .store(true, std::sync::atomic::Ordering::Release);

        let current_cached = cache.current_epoch();
        if current_cached.as_u64() > stale_epoch.as_u64() {
            self.paused
                .store(false, std::sync::atomic::Ordering::Release);
            return Ok(current_cached);
        }

        let result = client.refresh_metadata(cache).await;

        self.refresh_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.paused
            .store(false, std::sync::atomic::Ordering::Release);

        match result {
            Ok(new_epoch) => {
                tracing::info!(
                    old_epoch = stale_epoch.as_u64(),
                    new_epoch = new_epoch.as_u64(),
                    "refreshed metadata after stale epoch"
                );
                Ok(new_epoch)
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to refresh metadata after stale epoch");
                Err(e)
            }
        }
    }
}

impl Default for StaleEpochHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_cache::MetadataCache;
    use crate::traits::{CommitRequest, MetadataClient};

    struct MockMetadataClient {
        new_epoch: EpochId,
        should_fail: bool,
    }

    impl MetadataClient for MockMetadataClient {
        async fn refresh_metadata(&self, cache: &MetadataCache) -> Result<EpochId, Error> {
            if self.should_fail {
                return Err(Error::Raft("metadata unavailable".into()));
            }
            cache.set_epoch(self.new_epoch);
            Ok(self.new_epoch)
        }

        async fn commit_extent_mapping(
            &self,
            _request: crate::traits::CommitRequest,
        ) -> Result<EpochId, Error> {
            Ok(self.new_epoch)
        }

        async fn lookup_operation(
            &self,
            _operation_id: &blockyard_common::OperationId,
        ) -> Result<Option<crate::traits::CommittedMapping>, Error> {
            Ok(None)
        }

        async fn current_epoch(&self) -> Result<EpochId, Error> {
            Ok(self.new_epoch)
        }

        fn commit_extent_mappings_batch(
            &self,
            requests: Vec<CommitRequest>,
        ) -> impl std::future::Future<Output = Result<EpochId, Error>> + Send {
            let epoch = self.new_epoch;
            async move {
                let _ = requests;
                Ok(epoch)
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
    fn test_handler_new() {
        let handler = StaleEpochHandler::new();
        assert!(!handler.is_paused());
        assert_eq!(handler.refresh_count(), 0);
    }

    #[test]
    fn test_handler_default() {
        let handler = StaleEpochHandler::default();
        assert!(!handler.is_paused());
    }

    #[tokio::test]
    async fn test_handler_refresh_success() {
        let handler = StaleEpochHandler::new();
        let cache = MetadataCache::new();
        cache.set_epoch(EpochId::new(3));
        let client = MockMetadataClient {
            new_epoch: EpochId::new(5),
            should_fail: false,
        };

        let result = handler
            .handle_stale_epoch(&cache, &client, EpochId::new(3))
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), EpochId::new(5));
        assert_eq!(cache.current_epoch(), EpochId::new(5));
        assert!(!handler.is_paused());
        assert_eq!(handler.refresh_count(), 1);
    }

    #[tokio::test]
    async fn test_handler_refresh_failure() {
        let handler = StaleEpochHandler::new();
        let cache = MetadataCache::new();
        cache.set_epoch(EpochId::new(3));
        let client = MockMetadataClient {
            new_epoch: EpochId::new(5),
            should_fail: true,
        };

        let result = handler
            .handle_stale_epoch(&cache, &client, EpochId::new(3))
            .await;
        assert!(result.is_err());
        assert!(!handler.is_paused());
        assert_eq!(handler.refresh_count(), 1);
    }

    #[tokio::test]
    async fn test_handler_skip_if_already_refreshed() {
        let handler = StaleEpochHandler::new();
        let cache = MetadataCache::new();
        cache.set_epoch(EpochId::new(5));
        let client = MockMetadataClient {
            new_epoch: EpochId::new(10),
            should_fail: false,
        };

        let result = handler
            .handle_stale_epoch(&cache, &client, EpochId::new(3))
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), EpochId::new(5));
        assert_eq!(handler.refresh_count(), 0);
    }

    #[tokio::test]
    async fn test_handler_multiple_refreshes() {
        let handler = StaleEpochHandler::new();
        let cache = MetadataCache::new();

        let client1 = MockMetadataClient {
            new_epoch: EpochId::new(2),
            should_fail: false,
        };
        handler
            .handle_stale_epoch(&cache, &client1, EpochId::new(0))
            .await
            .unwrap();

        let client2 = MockMetadataClient {
            new_epoch: EpochId::new(5),
            should_fail: false,
        };
        handler
            .handle_stale_epoch(&cache, &client2, EpochId::new(2))
            .await
            .unwrap();

        assert_eq!(handler.refresh_count(), 2);
        assert_eq!(cache.current_epoch(), EpochId::new(5));
    }

    #[test]
    fn test_handler_debug() {
        let handler = StaleEpochHandler::new();
        let debug = format!("{:?}", handler);
        assert!(debug.contains("StaleEpochHandler"));
    }

    #[tokio::test]
    async fn test_stale_epoch_handler_triggers_refresh_from_old_epoch() {
        let handler = StaleEpochHandler::new();
        let cache = MetadataCache::new();
        cache.set_epoch(EpochId::new(1));
        let client = MockMetadataClient {
            new_epoch: EpochId::new(5),
            should_fail: false,
        };

        assert_eq!(handler.refresh_count(), 0);

        let new_epoch = handler
            .handle_stale_epoch(&cache, &client, EpochId::new(1))
            .await
            .expect("refresh should succeed");

        assert_eq!(new_epoch, EpochId::new(5));
        assert_eq!(cache.current_epoch(), EpochId::new(5));
        assert_eq!(handler.refresh_count(), 1);
    }
}
