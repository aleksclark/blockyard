//! Client crash recovery (P5.1, §6.1).
//!
//! If a client crashes after transmitting data but before receiving a write
//! ack, the write outcome is ambiguous — it may or may not have been committed.
//!
//! After restart the system resolves correctness via committed metadata state:
//! - Uncommitted local extent files MUST NOT become visible to reads.
//! - The client queries the metadata service to determine which operations
//!   were actually committed before the crash.
//! - Only committed extent mappings are re-populated into the metadata cache.

use std::collections::HashSet;
use std::sync::Arc;

use blockyard_common::error::Error;
use blockyard_common::{EpochId, OperationId, VolumeId};
use tracing::{debug, info};

use crate::metadata_cache::{CachedExtentMapping, MetadataCache};
use crate::traits::{CommittedMapping, MetadataClient};
use crate::watermark::WriteWatermark;

/// State recovered after a client crash and restart.
#[derive(Debug, Clone)]
pub struct RecoveryResult {
    pub recovered_epoch: EpochId,
    pub committed_operations: usize,
    pub uncommitted_operations: usize,
}

/// Performs client crash recovery (P5.1).
///
/// After a client restart, this resolver:
/// 1. Queries the metadata service for the current epoch
/// 2. For each in-flight operation ID from the previous session, queries
///    the metadata to determine if it was committed
/// 3. Rebuilds the metadata cache with only committed mappings
/// 4. Sets the watermark to the recovered epoch
/// 5. Returns a summary of what was recovered
///
/// Uncommitted extents that may exist on data nodes are invisible to reads
/// because they have no metadata mapping. They will be cleaned up by the
/// data node's orphaned extent cleanup (§6.9).
#[derive(Debug)]
pub struct CrashRecoveryResolver<M: MetadataClient> {
    metadata_client: Arc<M>,
    cache: Arc<MetadataCache>,
    watermark: Arc<WriteWatermark>,
}

impl<M: MetadataClient> CrashRecoveryResolver<M> {
    pub fn new(
        metadata_client: Arc<M>,
        cache: Arc<MetadataCache>,
        watermark: Arc<WriteWatermark>,
    ) -> Self {
        Self {
            metadata_client,
            cache,
            watermark,
        }
    }

    /// Recover client state after a crash.
    ///
    /// `inflight_op_ids` are the operation IDs that were in-flight at the
    /// time of the crash (from a durable session log, if available).
    pub async fn recover(
        &self,
        volume_id: VolumeId,
        inflight_op_ids: &[OperationId],
    ) -> Result<RecoveryResult, Error> {
        info!(
            volume_id = %volume_id,
            inflight_count = inflight_op_ids.len(),
            "starting client crash recovery"
        );

        let current_epoch = self.metadata_client.current_epoch().await?;
        self.cache.set_epoch(current_epoch);

        let mut committed_count = 0;
        let mut uncommitted_count = 0;
        let mut committed_ops = HashSet::new();

        for op_id in inflight_op_ids {
            match self.metadata_client.lookup_operation(op_id).await? {
                Some(mapping) => {
                    debug!(
                        operation_id = %op_id,
                        extent_id = %mapping.extent_id,
                        "in-flight operation was committed"
                    );

                    self.cache.set_extent_mapping(
                        &volume_id,
                        mapping.block_range.start,
                        CachedExtentMapping {
                            extent_id: mapping.extent_id,
                            extent_version: mapping.extent_version,
                            replica_locations: mapping.replica_locations.clone(),
                            checksums: mapping.checksums.clone(),
                        },
                    );

                    committed_ops.insert(*op_id);
                    committed_count += 1;
                }
                None => {
                    debug!(
                        operation_id = %op_id,
                        "in-flight operation was NOT committed — invisible to reads"
                    );
                    uncommitted_count += 1;
                }
            }
        }

        self.watermark.advance(current_epoch);

        info!(
            volume_id = %volume_id,
            recovered_epoch = current_epoch.as_u64(),
            committed = committed_count,
            uncommitted = uncommitted_count,
            "client crash recovery complete"
        );

        Ok(RecoveryResult {
            recovered_epoch: current_epoch,
            committed_operations: committed_count,
            uncommitted_operations: uncommitted_count,
        })
    }

    /// Verify that a specific operation was committed.
    ///
    /// Used when the client encounters an extent it cannot verify
    /// through the cache alone.
    pub async fn verify_operation_committed(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<CommittedMapping>, Error> {
        self.metadata_client.lookup_operation(operation_id).await
    }

    /// Refresh the full metadata state after recovery.
    ///
    /// Called after individual operation resolution to bring the cache
    /// up to date with the full cluster state.
    pub async fn refresh_full_state(&self) -> Result<EpochId, Error> {
        let new_epoch = self.metadata_client.refresh_metadata(&self.cache).await?;
        self.watermark.advance(new_epoch);
        Ok(new_epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_cache::{CachedVolumeInfo, MetadataCache};
    use crate::traits::{CommitRequest, CommittedMapping, MetadataClient};
    use crate::watermark::WriteWatermark;
    use blockyard_common::{EpochId, ExtentId, NodeId, OperationId, ProtectionPolicy, VolumeId};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    struct MockRecoveryMetadata {
        committed: parking_lot::Mutex<std::collections::HashMap<String, CommittedMapping>>,
        epoch: EpochId,
    }

    impl MockRecoveryMetadata {
        fn new(epoch: EpochId) -> Self {
            Self {
                committed: parking_lot::Mutex::new(std::collections::HashMap::new()),
                epoch,
            }
        }

        fn add_committed(&self, op_id: OperationId, mapping: CommittedMapping) {
            self.committed.lock().insert(op_id.to_string(), mapping);
        }
    }

    impl MetadataClient for MockRecoveryMetadata {
        async fn refresh_metadata(&self, cache: &MetadataCache) -> Result<EpochId, Error> {
            cache.set_epoch(self.epoch);
            Ok(self.epoch)
        }

        async fn commit_extent_mapping(&self, _request: CommitRequest) -> Result<EpochId, Error> {
            Ok(self.epoch)
        }

        async fn lookup_operation(
            &self,
            operation_id: &OperationId,
        ) -> Result<Option<CommittedMapping>, Error> {
            Ok(self
                .committed
                .lock()
                .get(&operation_id.to_string())
                .cloned())
        }

        async fn current_epoch(&self) -> Result<EpochId, Error> {
            Ok(self.epoch)
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

    fn setup_cache(vid: VolumeId) -> MetadataCache {
        let cache = MetadataCache::new();
        cache.set_volume(CachedVolumeInfo {
            volume_id: vid,
            size_bytes: 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_mappings: BTreeMap::new(),
        });
        cache
    }

    #[tokio::test]
    async fn test_recovery_no_inflight_ops() {
        let vid = VolumeId::generate();
        let cache = Arc::new(setup_cache(vid));
        let wm = Arc::new(WriteWatermark::new());
        let meta = Arc::new(MockRecoveryMetadata::new(EpochId::new(5)));

        let resolver = CrashRecoveryResolver::new(meta, cache.clone(), wm.clone());
        let result = resolver.recover(vid, &[]).await.unwrap();

        assert_eq!(result.recovered_epoch, EpochId::new(5));
        assert_eq!(result.committed_operations, 0);
        assert_eq!(result.uncommitted_operations, 0);
        assert_eq!(wm.current(), EpochId::new(5));
        assert_eq!(cache.current_epoch(), EpochId::new(5));
    }

    #[tokio::test]
    async fn test_recovery_all_committed() {
        let vid = VolumeId::generate();
        let cache = Arc::new(setup_cache(vid));
        let wm = Arc::new(WriteWatermark::new());
        let meta = Arc::new(MockRecoveryMetadata::new(EpochId::new(3)));

        let op1 = OperationId::generate();
        let op2 = OperationId::generate();
        let eid1 = ExtentId::generate();
        let eid2 = ExtentId::generate();
        let nid = NodeId::generate();

        meta.add_committed(
            op1,
            CommittedMapping {
                extent_id: eid1,
                extent_version: 1,
                epoch: EpochId::new(3),
                block_range: 0..64,
                replica_locations: vec![nid],
                checksums: vec![vec![0xAA]],
            },
        );
        meta.add_committed(
            op2,
            CommittedMapping {
                extent_id: eid2,
                extent_version: 2,
                epoch: EpochId::new(3),
                block_range: 64..128,
                replica_locations: vec![nid],
                checksums: vec![vec![0xBB]],
            },
        );

        let resolver = CrashRecoveryResolver::new(meta, cache.clone(), wm.clone());
        let result = resolver.recover(vid, &[op1, op2]).await.unwrap();

        assert_eq!(result.committed_operations, 2);
        assert_eq!(result.uncommitted_operations, 0);
        assert!(cache.get_extent_mapping(&vid, 0).is_some());
        assert!(cache.get_extent_mapping(&vid, 64).is_some());
    }

    #[tokio::test]
    async fn test_recovery_mixed_committed_and_uncommitted() {
        let vid = VolumeId::generate();
        let cache = Arc::new(setup_cache(vid));
        let wm = Arc::new(WriteWatermark::new());
        let meta = Arc::new(MockRecoveryMetadata::new(EpochId::new(7)));

        let op_committed = OperationId::generate();
        let op_uncommitted = OperationId::generate();
        let eid = ExtentId::generate();

        meta.add_committed(
            op_committed,
            CommittedMapping {
                extent_id: eid,
                extent_version: 5,
                epoch: EpochId::new(7),
                block_range: 0..64,
                replica_locations: vec![],
                checksums: vec![],
            },
        );

        let resolver = CrashRecoveryResolver::new(meta, cache.clone(), wm.clone());
        let result = resolver
            .recover(vid, &[op_committed, op_uncommitted])
            .await
            .unwrap();

        assert_eq!(result.committed_operations, 1);
        assert_eq!(result.uncommitted_operations, 1);
        assert!(cache.get_extent_mapping(&vid, 0).is_some());
    }

    #[tokio::test]
    async fn test_recovery_all_uncommitted() {
        let vid = VolumeId::generate();
        let cache = Arc::new(setup_cache(vid));
        let wm = Arc::new(WriteWatermark::new());
        let meta = Arc::new(MockRecoveryMetadata::new(EpochId::new(1)));

        let op1 = OperationId::generate();
        let op2 = OperationId::generate();

        let resolver = CrashRecoveryResolver::new(meta, cache.clone(), wm.clone());
        let result = resolver.recover(vid, &[op1, op2]).await.unwrap();

        assert_eq!(result.committed_operations, 0);
        assert_eq!(result.uncommitted_operations, 2);
    }

    #[tokio::test]
    async fn test_verify_operation_committed() {
        let vid = VolumeId::generate();
        let cache = Arc::new(setup_cache(vid));
        let wm = Arc::new(WriteWatermark::new());
        let meta = Arc::new(MockRecoveryMetadata::new(EpochId::new(1)));

        let op = OperationId::generate();
        meta.add_committed(
            op,
            CommittedMapping {
                extent_id: ExtentId::generate(),
                extent_version: 1,
                epoch: EpochId::new(1),
                block_range: 0..64,
                replica_locations: vec![],
                checksums: vec![],
            },
        );

        let resolver = CrashRecoveryResolver::new(meta, cache, wm);
        assert!(
            resolver
                .verify_operation_committed(&op)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn test_verify_operation_not_committed() {
        let vid = VolumeId::generate();
        let cache = Arc::new(setup_cache(vid));
        let wm = Arc::new(WriteWatermark::new());
        let meta = Arc::new(MockRecoveryMetadata::new(EpochId::new(1)));

        let resolver = CrashRecoveryResolver::new(meta, cache, wm);
        let op = OperationId::generate();
        assert!(
            resolver
                .verify_operation_committed(&op)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_refresh_full_state() {
        let vid = VolumeId::generate();
        let cache = Arc::new(setup_cache(vid));
        let wm = Arc::new(WriteWatermark::new());
        let meta = Arc::new(MockRecoveryMetadata::new(EpochId::new(10)));

        let resolver = CrashRecoveryResolver::new(meta, cache.clone(), wm.clone());
        let epoch = resolver.refresh_full_state().await.unwrap();

        assert_eq!(epoch, EpochId::new(10));
        assert_eq!(cache.current_epoch(), EpochId::new(10));
        assert_eq!(wm.current(), EpochId::new(10));
    }

    #[tokio::test]
    async fn test_recovery_watermark_advances() {
        let vid = VolumeId::generate();
        let cache = Arc::new(setup_cache(vid));
        let wm = Arc::new(WriteWatermark::new());
        assert_eq!(wm.current(), EpochId::new(0));

        let meta = Arc::new(MockRecoveryMetadata::new(EpochId::new(42)));
        let resolver = CrashRecoveryResolver::new(meta, cache, wm.clone());
        resolver.recover(vid, &[]).await.unwrap();

        assert_eq!(wm.current(), EpochId::new(42));
    }

    #[test]
    fn test_recovery_result_debug() {
        let r = RecoveryResult {
            recovered_epoch: EpochId::new(1),
            committed_operations: 3,
            uncommitted_operations: 1,
        };
        let debug = format!("{:?}", r);
        assert!(debug.contains("RecoveryResult"));
    }

    #[test]
    fn test_recovery_result_clone() {
        let r = RecoveryResult {
            recovered_epoch: EpochId::new(5),
            committed_operations: 2,
            uncommitted_operations: 0,
        };
        let cloned = r.clone();
        assert_eq!(cloned.recovered_epoch, EpochId::new(5));
        assert_eq!(cloned.committed_operations, 2);
    }
}
