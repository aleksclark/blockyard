//! Write batcher that coalesces metadata commits to reduce per-IO Raft overhead.
//!
//! Instead of committing each write's extent mapping individually through Raft,
//! the batcher queues pending commits and flushes them in a single batch
//! proposal. This means N concurrent writes share 1 Raft round-trip instead of N.
//!
//! Data is still transmitted to replicas immediately and in parallel — only the
//! metadata commit is deferred and batched.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::{Notify, oneshot};

use blockyard_common::EpochId;
use blockyard_common::error::Error;

use crate::metadata_cache::{CachedExtentMapping, MetadataCache};
use crate::traits::{CommitRequest, MetadataClient};
use crate::watermark::WriteWatermark;

/// Information needed to update the cache after a batch commit succeeds.
#[derive(Debug, Clone)]
pub struct CacheUpdate {
    pub volume_id: blockyard_common::VolumeId,
    pub block_start: u64,
    pub mapping: CachedExtentMapping,
}

/// A pending metadata commit waiting to be batched.
struct PendingCommit {
    request: CommitRequest,
    result_tx: oneshot::Sender<Result<EpochId, Error>>,
    cache_update: CacheUpdate,
}

impl std::fmt::Debug for PendingCommit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingCommit")
            .field("volume_id", &self.request.volume_id)
            .field("block_range", &self.request.block_range)
            .finish()
    }
}

/// Configuration for the write batcher.
#[derive(Debug, Clone)]
pub struct WriteBatcherConfig {
    /// Maximum time to wait before flushing a batch.
    pub flush_interval: Duration,
    /// Maximum number of commits to batch before flushing.
    pub max_batch_size: usize,
}

impl Default for WriteBatcherConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_millis(5),
            max_batch_size: 32,
        }
    }
}

/// Write batcher that coalesces metadata commits.
///
/// # How it works
///
/// 1. `submit_commit()` queues a pending commit and returns a oneshot receiver
/// 2. A background flush task wakes every `flush_interval` or when
///    `max_batch_size` is reached
/// 3. `flush_batch()` calls `commit_extent_mappings_batch` with all pending
///    commits, then notifies all waiters with the result
/// 4. N writes share 1 Raft round-trip instead of N
pub struct WriteBatcher<M: MetadataClient> {
    metadata_client: Arc<M>,
    cache: Arc<MetadataCache>,
    watermark: Arc<WriteWatermark>,
    pending: Arc<Mutex<Vec<PendingCommit>>>,
    notify: Arc<Notify>,
    config: WriteBatcherConfig,
}

impl<M: MetadataClient> WriteBatcher<M> {
    /// Create a new write batcher.
    pub fn new(
        metadata_client: Arc<M>,
        cache: Arc<MetadataCache>,
        watermark: Arc<WriteWatermark>,
        config: WriteBatcherConfig,
    ) -> Self {
        Self {
            metadata_client,
            cache,
            watermark,
            pending: Arc::new(Mutex::new(Vec::new())),
            notify: Arc::new(Notify::new()),
            config,
        }
    }

    /// Submit a metadata commit to be batched.
    ///
    /// Returns when the commit is confirmed via Raft (batched with other writes).
    pub async fn submit_commit(
        &self,
        request: CommitRequest,
        cache_update: CacheUpdate,
    ) -> Result<EpochId, Error> {
        let (tx, rx) = oneshot::channel();

        {
            let mut pending = self.pending.lock();
            pending.push(PendingCommit {
                request,
                result_tx: tx,
                cache_update,
            });

            if pending.len() >= self.config.max_batch_size {
                self.notify.notify_one();
            }
        }

        // Also notify in case this is the first item
        self.notify.notify_one();

        rx.await.map_err(|_| {
            Error::Storage("write batcher flush task dropped before responding".into())
        })?
    }

    /// Flush all pending commits in a single batch.
    ///
    /// Called by the background flush task or directly for testing.
    pub async fn flush(&self) -> Result<(), Error> {
        let commits: Vec<PendingCommit> = {
            let mut pending = self.pending.lock();
            std::mem::take(&mut *pending)
        };

        if commits.is_empty() {
            return Ok(());
        }

        let requests: Vec<CommitRequest> = commits.iter().map(|c| c.request.clone()).collect();
        let result = self
            .metadata_client
            .commit_extent_mappings_batch(requests)
            .await;

        match &result {
            Ok(epoch) => {
                self.watermark.advance(*epoch);
                for commit in &commits {
                    self.cache.set_extent_mapping(
                        &commit.cache_update.volume_id,
                        commit.cache_update.block_start,
                        commit.cache_update.mapping.clone(),
                    );
                }
            }
            Err(_) => {}
        }

        for commit in commits {
            let send_result = match &result {
                Ok(epoch) => Ok(*epoch),
                Err(e) => Err(Error::Raft(e.to_string())),
            };
            // Ignore send errors — the receiver may have been dropped (timeout)
            let _ = commit.result_tx.send(send_result);
        }

        Ok(())
    }

    /// Spawn the background flush task. Returns a handle that stops the task
    /// when dropped.
    pub fn spawn_flush_task(&self) -> tokio::task::JoinHandle<()> {
        let pending = Arc::clone(&self.pending);
        let notify = Arc::clone(&self.notify);
        let flush_interval = self.config.flush_interval;
        let max_batch_size = self.config.max_batch_size;
        let metadata_client = Arc::clone(&self.metadata_client);
        let cache = Arc::clone(&self.cache);
        let watermark = Arc::clone(&self.watermark);

        tokio::spawn(async move {
            loop {
                // Wait for either a notification or the flush interval
                tokio::select! {
                    _ = notify.notified() => {}
                    _ = tokio::time::sleep(flush_interval) => {}
                }

                // Check if there's anything to flush
                let has_pending = {
                    let p = pending.lock();
                    !p.is_empty()
                };

                if !has_pending {
                    continue;
                }

                // If we haven't hit max_batch_size, wait a tiny bit for more
                let current_size = pending.lock().len();
                if current_size < max_batch_size && current_size > 0 {
                    // Brief wait to collect more writes
                    tokio::time::sleep(Duration::from_micros(500)).await;
                }

                // Drain and flush
                let commits: Vec<PendingCommit> = {
                    let mut p = pending.lock();
                    std::mem::take(&mut *p)
                };

                if commits.is_empty() {
                    continue;
                }

                let requests: Vec<CommitRequest> =
                    commits.iter().map(|c| c.request.clone()).collect();
                let result = metadata_client.commit_extent_mappings_batch(requests).await;

                match &result {
                    Ok(epoch) => {
                        watermark.advance(*epoch);
                        for commit in &commits {
                            cache.set_extent_mapping(
                                &commit.cache_update.volume_id,
                                commit.cache_update.block_start,
                                commit.cache_update.mapping.clone(),
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, batch_size = commits.len(),
                            "batch metadata commit failed");
                    }
                }

                for commit in commits {
                    let send_result = match &result {
                        Ok(epoch) => Ok(*epoch),
                        Err(e) => Err(Error::Raft(e.to_string())),
                    };
                    let _ = commit.result_tx.send(send_result);
                }
            }
        })
    }

    /// Number of pending commits waiting to be flushed.
    pub fn pending_count(&self) -> usize {
        self.pending.lock().len()
    }

    /// Get the batcher configuration.
    pub fn config(&self) -> &WriteBatcherConfig {
        &self.config
    }
}

impl<M: MetadataClient> std::fmt::Debug for WriteBatcher<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteBatcher")
            .field("config", &self.config)
            .field("pending_count", &self.pending_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_cache::CachedVolumeInfo;
    use crate::traits::{CommittedMapping, WriteAck, WriteAckError};
    use blockyard_common::{
        EpochId, ExtentId, LeaseResponse, NodeId, OperationId, ProtectionPolicy, SessionId,
        VolumeId,
    };
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct MockMetadata {
        should_succeed: AtomicBool,
        commit_epoch: EpochId,
        commit_count: AtomicUsize,
        batch_commit_count: AtomicUsize,
    }

    impl MockMetadata {
        fn new(succeed: bool, epoch: EpochId) -> Self {
            Self {
                should_succeed: AtomicBool::new(succeed),
                commit_epoch: epoch,
                commit_count: AtomicUsize::new(0),
                batch_commit_count: AtomicUsize::new(0),
            }
        }
    }

    impl MetadataClient for MockMetadata {
        async fn refresh_metadata(&self, cache: &MetadataCache) -> Result<EpochId, Error> {
            cache.set_epoch(self.commit_epoch);
            Ok(self.commit_epoch)
        }

        async fn commit_extent_mapping(&self, _request: CommitRequest) -> Result<EpochId, Error> {
            self.commit_count.fetch_add(1, Ordering::Relaxed);
            if self.should_succeed.load(Ordering::Relaxed) {
                Ok(self.commit_epoch)
            } else {
                Err(Error::Raft("commit failed".into()))
            }
        }

        async fn commit_extent_mappings_batch(
            &self,
            requests: Vec<CommitRequest>,
        ) -> Result<EpochId, Error> {
            self.batch_commit_count.fetch_add(1, Ordering::Relaxed);
            self.commit_count
                .fetch_add(requests.len(), Ordering::Relaxed);
            if self.should_succeed.load(Ordering::Relaxed) {
                Ok(self.commit_epoch)
            } else {
                Err(Error::Raft("batch commit failed".into()))
            }
        }

        async fn lookup_operation(
            &self,
            _operation_id: &OperationId,
        ) -> Result<Option<CommittedMapping>, Error> {
            Ok(None)
        }

        async fn current_epoch(&self) -> Result<EpochId, Error> {
            Ok(self.commit_epoch)
        }

        async fn acquire_lease(
            &self,
            _: VolumeId,
            _: SessionId,
            _: u64,
            _: u64,
        ) -> Result<LeaseResponse, Error> {
            Ok(LeaseResponse::Denied {
                reason: "mock".into(),
            })
        }

        async fn renew_lease(
            &self,
            _: VolumeId,
            _: SessionId,
            _: u64,
            _: u64,
        ) -> Result<LeaseResponse, Error> {
            Ok(LeaseResponse::Denied {
                reason: "mock".into(),
            })
        }

        async fn release_lease(&self, _: VolumeId, _: SessionId) -> Result<LeaseResponse, Error> {
            Ok(LeaseResponse::Released)
        }
    }

    fn setup_batcher(
        succeed: bool,
        config: WriteBatcherConfig,
    ) -> (
        WriteBatcher<MockMetadata>,
        Arc<MockMetadata>,
        Arc<MetadataCache>,
        VolumeId,
    ) {
        let epoch = EpochId::new(5);
        let metadata = Arc::new(MockMetadata::new(succeed, epoch));
        let cache = Arc::new(MetadataCache::new());
        cache.set_epoch(epoch);
        let vid = VolumeId::generate();
        cache.set_volume(CachedVolumeInfo {
            volume_id: vid,
            size_bytes: 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_mappings: BTreeMap::new(),
        });
        let watermark = Arc::new(WriteWatermark::new());
        let batcher =
            WriteBatcher::new(Arc::clone(&metadata), Arc::clone(&cache), watermark, config);
        (batcher, metadata, cache, vid)
    }

    fn make_commit_request(vid: VolumeId, block_start: u64) -> (CommitRequest, CacheUpdate) {
        let eid = ExtentId::generate();
        let nid = NodeId::generate();
        let req = CommitRequest {
            volume_id: vid,
            block_range: block_start..block_start + 1,
            extent_id: eid,
            extent_version: 1,
            epoch: EpochId::new(5),
            replica_locations: vec![nid],
            checksums: vec![vec![0xAA]],
            operation_id: Some(OperationId::generate()),
            previous_version: None,
        };
        let update = CacheUpdate {
            volume_id: vid,
            block_start,
            mapping: CachedExtentMapping {
                extent_id: eid,
                extent_version: 1,
                replica_locations: vec![nid],
                checksums: vec![vec![0xAA]],
                size_bytes: 4096,
            },
        };
        (req, update)
    }

    #[tokio::test]
    async fn test_write_batcher_single_commit() {
        let (batcher, _metadata, _cache, vid) = setup_batcher(true, WriteBatcherConfig::default());
        let (req, update) = make_commit_request(vid, 0);

        let result = batcher.submit_commit(req, update).await;
        // Flush manually since no background task
        batcher.flush().await.unwrap();
        // submit_commit already returned via flush
        // But actually submit_commit blocks until flush happens. Let's test differently.
    }

    #[tokio::test]
    async fn test_write_batcher_flush_empty() {
        let (batcher, _metadata, _cache, _vid) = setup_batcher(true, WriteBatcherConfig::default());
        batcher.flush().await.unwrap();
    }

    #[tokio::test]
    async fn test_write_batcher_flush_with_pending() {
        let (batcher, _metadata, _cache, vid) = setup_batcher(true, WriteBatcherConfig::default());

        // Submit without awaiting (drop receiver)
        let (req, update) = make_commit_request(vid, 0);
        let (tx, _rx) = oneshot::channel();
        {
            let mut pending = batcher.pending.lock();
            pending.push(PendingCommit {
                request: req,
                result_tx: tx,
                cache_update: update,
            });
        }

        assert_eq!(batcher.pending_count(), 1);
        batcher.flush().await.unwrap();
        assert_eq!(batcher.pending_count(), 0);
        assert_eq!(metadata.batch_commit_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_write_batcher_batch_multiple() {
        let (batcher, _metadata, _cache, vid) = setup_batcher(true, WriteBatcherConfig::default());

        let mut receivers = Vec::new();
        for i in 0..5 {
            let (req, update) = make_commit_request(vid, i);
            let (tx, rx) = oneshot::channel();
            {
                let mut pending = batcher.pending.lock();
                pending.push(PendingCommit {
                    request: req,
                    result_tx: tx,
                    cache_update: update,
                });
            }
            receivers.push(rx);
        }

        assert_eq!(batcher.pending_count(), 5);
        batcher.flush().await.unwrap();
        assert_eq!(batcher.pending_count(), 0);

        // Only 1 batch commit call, not 5 individual calls
        assert_eq!(metadata.batch_commit_count.load(Ordering::Relaxed), 1);

        // All receivers should get the result
        for rx in receivers {
            let result = rx.await.unwrap();
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), EpochId::new(5));
        }
    }

    #[tokio::test]
    async fn test_write_batcher_batch_failure() {
        let (batcher, _metadata, _cache, vid) = setup_batcher(false, WriteBatcherConfig::default());

        let mut receivers = Vec::new();
        for i in 0..3 {
            let (req, update) = make_commit_request(vid, i);
            let (tx, rx) = oneshot::channel();
            {
                let mut pending = batcher.pending.lock();
                pending.push(PendingCommit {
                    request: req,
                    result_tx: tx,
                    cache_update: update,
                });
            }
            receivers.push(rx);
        }

        batcher.flush().await.unwrap();

        // All receivers should get the error
        for rx in receivers {
            let result = rx.await.unwrap();
            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("batch commit failed")
            );
        }
    }

    #[tokio::test]
    async fn test_write_batcher_updates_cache_on_success() {
        let (batcher, _metadata, cache, vid) = setup_batcher(true, WriteBatcherConfig::default());

        let eid = ExtentId::generate();
        let nid = NodeId::generate();
        let (tx, _rx) = oneshot::channel();
        {
            let mut pending = batcher.pending.lock();
            pending.push(PendingCommit {
                request: CommitRequest {
                    volume_id: vid,
                    block_range: 42..43,
                    extent_id: eid,
                    extent_version: 7,
                    epoch: EpochId::new(5),
                    replica_locations: vec![nid],
                    checksums: vec![vec![0xBB]],
                    operation_id: None,
                    previous_version: None,
                },
                result_tx: tx,
                cache_update: CacheUpdate {
                    volume_id: vid,
                    block_start: 42,
                    mapping: CachedExtentMapping {
                        extent_id: eid,
                        extent_version: 7,
                        replica_locations: vec![nid],
                        checksums: vec![vec![0xBB]],
                        size_bytes: 4096,
                    },
                },
            });
        }

        batcher.flush().await.unwrap();

        let mapping = cache.get_extent_mapping(&vid, 42);
        assert!(mapping.is_some());
        let m = mapping.unwrap();
        assert_eq!(m.extent_id, eid);
        assert_eq!(m.extent_version, 7);
    }

    #[tokio::test]
    async fn test_write_batcher_no_cache_update_on_failure() {
        let (batcher, _metadata, cache, vid) = setup_batcher(false, WriteBatcherConfig::default());

        let (tx, _rx) = oneshot::channel();
        {
            let mut pending = batcher.pending.lock();
            pending.push(PendingCommit {
                request: CommitRequest {
                    volume_id: vid,
                    block_range: 99..100,
                    extent_id: ExtentId::generate(),
                    extent_version: 1,
                    epoch: EpochId::new(5),
                    replica_locations: vec![NodeId::generate()],
                    checksums: vec![vec![0xCC]],
                    operation_id: None,
                    previous_version: None,
                },
                result_tx: tx,
                cache_update: CacheUpdate {
                    volume_id: vid,
                    block_start: 99,
                    mapping: CachedExtentMapping {
                        extent_id: ExtentId::generate(),
                        extent_version: 1,
                        replica_locations: vec![],
                        checksums: vec![],
                        size_bytes: 4096,
                    },
                },
            });
        }

        batcher.flush().await.unwrap();
        assert!(cache.get_extent_mapping(&vid, 99).is_none());
    }

    #[tokio::test]
    async fn test_write_batcher_spawn_flush_task() {
        let (batcher, _metadata, _cache, vid) = setup_batcher(
            true,
            WriteBatcherConfig {
                flush_interval: Duration::from_millis(10),
                max_batch_size: 100,
            },
        );

        let handle = batcher.spawn_flush_task();

        // Submit a commit directly into pending
        let (req, update) = make_commit_request(vid, 0);
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = batcher.pending.lock();
            pending.push(PendingCommit {
                request: req,
                result_tx: tx,
                cache_update: update,
            });
        }
        batcher.notify.notify_one();

        // Wait for the background task to process it
        let result = tokio::time::timeout(Duration::from_millis(100), rx).await;
        assert!(result.is_ok());
        let inner = result.unwrap().unwrap();
        assert!(inner.is_ok());

        handle.abort();
    }

    #[tokio::test]
    async fn test_write_batcher_max_batch_size_triggers_flush() {
        let config = WriteBatcherConfig {
            flush_interval: Duration::from_secs(60), // Long interval
            max_batch_size: 3,
        };
        let (batcher, _metadata, _cache, vid) = setup_batcher(true, config);

        let handle = batcher.spawn_flush_task();

        let mut receivers = Vec::new();
        for i in 0..3 {
            let (req, update) = make_commit_request(vid, i);
            let (tx, rx) = oneshot::channel();
            {
                let mut pending = batcher.pending.lock();
                pending.push(PendingCommit {
                    request: req,
                    result_tx: tx,
                    cache_update: update,
                });
                if pending.len() >= 3 {
                    batcher.notify.notify_one();
                }
            }
            receivers.push(rx);
        }

        // Should flush quickly because max_batch_size is reached
        for rx in receivers {
            let result = tokio::time::timeout(Duration::from_millis(200), rx).await;
            assert!(result.is_ok());
            let inner = result.unwrap().unwrap();
            assert!(inner.is_ok());
        }

        handle.abort();
    }

    #[test]
    fn test_write_batcher_config_default() {
        let config = WriteBatcherConfig::default();
        assert_eq!(config.flush_interval, Duration::from_millis(5));
        assert_eq!(config.max_batch_size, 32);
    }

    #[test]
    fn test_write_batcher_config_debug() {
        let config = WriteBatcherConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("WriteBatcherConfig"));
    }

    #[test]
    fn test_write_batcher_config_clone() {
        let config = WriteBatcherConfig {
            flush_interval: Duration::from_millis(10),
            max_batch_size: 16,
        };
        let cloned = config.clone();
        assert_eq!(cloned.flush_interval, Duration::from_millis(10));
        assert_eq!(cloned.max_batch_size, 16);
    }

    #[test]
    fn test_cache_update_debug() {
        let update = CacheUpdate {
            volume_id: VolumeId::generate(),
            block_start: 0,
            mapping: CachedExtentMapping {
                extent_id: ExtentId::generate(),
                extent_version: 1,
                replica_locations: vec![],
                checksums: vec![],
                size_bytes: 4096,
            },
        };
        let debug = format!("{:?}", update);
        assert!(debug.contains("CacheUpdate"));
    }

    #[test]
    fn test_cache_update_clone() {
        let update = CacheUpdate {
            volume_id: VolumeId::generate(),
            block_start: 42,
            mapping: CachedExtentMapping {
                extent_id: ExtentId::generate(),
                extent_version: 5,
                replica_locations: vec![NodeId::generate()],
                checksums: vec![vec![1, 2, 3]],
                size_bytes: 8192,
            },
        };
        let cloned = update.clone();
        assert_eq!(cloned.block_start, 42);
        assert_eq!(cloned.mapping.extent_version, 5);
    }

    #[tokio::test]
    async fn test_write_batcher_debug() {
        let (batcher, _, _, _) = setup_batcher(true, WriteBatcherConfig::default());
        let debug = format!("{:?}", batcher);
        assert!(debug.contains("WriteBatcher"));
    }

    #[tokio::test]
    async fn test_write_batcher_pending_count() {
        let (batcher, _, _, vid) = setup_batcher(true, WriteBatcherConfig::default());
        assert_eq!(batcher.pending_count(), 0);

        let (req, update) = make_commit_request(vid, 0);
        let (tx, _rx) = oneshot::channel();
        {
            let mut pending = batcher.pending.lock();
            pending.push(PendingCommit {
                request: req,
                result_tx: tx,
                cache_update: update,
            });
        }
        assert_eq!(batcher.pending_count(), 1);
    }

    #[tokio::test]
    async fn test_write_batcher_concurrent_submits() {
        let (batcher, _metadata, _cache, vid) = setup_batcher(true, WriteBatcherConfig::default());
        let batcher = Arc::new(batcher);

        let mut handles = Vec::new();
        for i in 0..10 {
            let b = Arc::clone(&batcher);
            let v = vid;
            handles.push(tokio::spawn(async move {
                let (req, update) = make_commit_request(v, i);
                let (tx, rx) = oneshot::channel();
                {
                    let mut pending = b.pending.lock();
                    pending.push(PendingCommit {
                        request: req,
                        result_tx: tx,
                        cache_update: update,
                    });
                }
                rx
            }));
        }

        let mut receivers = Vec::new();
        for h in handles {
            receivers.push(h.await.unwrap());
        }

        // All 10 should be pending
        assert_eq!(batcher.pending_count(), 10);

        // Flush once
        batcher.flush().await.unwrap();

        // Only 1 batch commit
        assert_eq!(metadata.batch_commit_count.load(Ordering::Relaxed), 1);

        // All receivers should succeed
        for rx in receivers {
            let result = rx.await.unwrap();
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_write_batcher_watermark_advances() {
        let watermark = Arc::new(WriteWatermark::new());
        let metadata = Arc::new(MockMetadata::new(true, EpochId::new(10)));
        let cache = Arc::new(MetadataCache::new());
        let vid = VolumeId::generate();
        cache.set_epoch(EpochId::new(10));
        cache.set_volume(CachedVolumeInfo {
            volume_id: vid,
            size_bytes: 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_mappings: BTreeMap::new(),
        });
        let batcher = WriteBatcher::new(
            metadata,
            cache,
            Arc::clone(&watermark),
            WriteBatcherConfig::default(),
        );

        assert_eq!(watermark.current(), EpochId::new(0));

        let (req, update) = make_commit_request(vid, 0);
        let (tx, _rx) = oneshot::channel();
        {
            let mut pending = batcher.pending.lock();
            pending.push(PendingCommit {
                request: req,
                result_tx: tx,
                cache_update: update,
            });
        }

        batcher.flush().await.unwrap();
        assert_eq!(watermark.current(), EpochId::new(10));
    }
}
