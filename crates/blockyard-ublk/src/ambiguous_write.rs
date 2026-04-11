//! Ambiguous write resolution (P5.4, §4.9.2).
//!
//! When a client receives an ambiguous write result (timeout, disconnect,
//! or crash recovery), it queries the metadata service to determine whether
//! the operation was committed before deciding to retry.
//!
//! Flow:
//! 1. Client has an OperationId whose outcome is unknown.
//! 2. Query metadata for that OperationId → get committed mapping or None.
//! 3. If committed: populate cache, advance watermark, return success.
//! 4. If not committed: safe to retry with the same OperationId (idempotent).

use std::sync::Arc;

use blockyard_common::error::Error;
use blockyard_common::{EpochId, OperationId, VolumeId};
use tracing::{debug, info};

use crate::metadata_cache::{CachedExtentMapping, MetadataCache};
use crate::traits::MetadataClient;
use crate::watermark::WriteWatermark;

/// Result of resolving an ambiguous write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AmbiguousWriteOutcome {
    Committed { epoch: EpochId, extent_version: u64 },
    NotCommitted,
}

/// Resolves ambiguous write outcomes by querying the metadata service.
///
/// When a write operation's outcome is unknown (e.g., timeout, client crash),
/// this resolver determines whether the operation was actually committed
/// through the metadata service's operation log (§4.9.2).
#[derive(Debug)]
pub struct AmbiguousWriteResolver<M: MetadataClient> {
    metadata_client: Arc<M>,
    cache: Arc<MetadataCache>,
    watermark: Arc<WriteWatermark>,
}

impl<M: MetadataClient> AmbiguousWriteResolver<M> {
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

    /// Resolve an ambiguous write operation.
    ///
    /// Queries the metadata service for the given OperationId. If committed,
    /// updates the cache and watermark. If not committed, the caller can
    /// safely retry with the same OperationId for idempotency.
    pub async fn resolve(
        &self,
        volume_id: VolumeId,
        operation_id: OperationId,
    ) -> Result<AmbiguousWriteOutcome, Error> {
        debug!(
            operation_id = %operation_id,
            volume_id = %volume_id,
            "resolving ambiguous write"
        );

        match self.metadata_client.lookup_operation(&operation_id).await? {
            Some(mapping) => {
                info!(
                    operation_id = %operation_id,
                    extent_id = %mapping.extent_id,
                    extent_version = mapping.extent_version,
                    "ambiguous write was committed"
                );

                self.cache.set_extent_mapping(
                    &volume_id,
                    mapping.block_range.start,
                    CachedExtentMapping {
                        extent_id: mapping.extent_id,
                        extent_version: mapping.extent_version,
                        replica_locations: mapping.replica_locations.clone(),
                        checksums: mapping.checksums.clone(),
                        size_bytes: 0,
                    },
                );

                self.watermark.advance(mapping.epoch);

                Ok(AmbiguousWriteOutcome::Committed {
                    epoch: mapping.epoch,
                    extent_version: mapping.extent_version,
                })
            }
            None => {
                debug!(
                    operation_id = %operation_id,
                    "ambiguous write was NOT committed — safe to retry"
                );
                Ok(AmbiguousWriteOutcome::NotCommitted)
            }
        }
    }

    /// Resolve multiple ambiguous writes in batch.
    ///
    /// Returns a mapping of OperationId to outcome.
    pub async fn resolve_batch(
        &self,
        volume_id: VolumeId,
        operation_ids: &[OperationId],
    ) -> Result<Vec<(OperationId, AmbiguousWriteOutcome)>, Error> {
        let mut results = Vec::with_capacity(operation_ids.len());

        for &op_id in operation_ids {
            let outcome = self.resolve(volume_id, op_id).await?;
            results.push((op_id, outcome));
        }

        let committed = results
            .iter()
            .filter(|(_, o)| matches!(o, AmbiguousWriteOutcome::Committed { .. }))
            .count();
        let not_committed = results.len() - committed;

        info!(
            volume_id = %volume_id,
            total = results.len(),
            committed = committed,
            not_committed = not_committed,
            "batch ambiguous write resolution complete"
        );

        Ok(results)
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

    struct MockAmbiguousMetadata {
        committed: parking_lot::Mutex<std::collections::HashMap<String, CommittedMapping>>,
        epoch: EpochId,
    }

    impl MockAmbiguousMetadata {
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

    impl MetadataClient for MockAmbiguousMetadata {
        async fn refresh_metadata(&self, cache: &MetadataCache) -> Result<EpochId, Error> {
            cache.set_epoch(self.epoch);
            Ok(self.epoch)
        }

        async fn commit_extent_mapping(&self, _req: CommitRequest) -> Result<EpochId, Error> {
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

        fn commit_extent_mappings_batch(
            &self,
            requests: Vec<CommitRequest>,
        ) -> impl std::future::Future<Output = Result<EpochId, Error>> + Send {
            let epoch = self.epoch;
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

    fn setup(
        vid: VolumeId,
        epoch: u64,
    ) -> (
        Arc<MockAmbiguousMetadata>,
        Arc<MetadataCache>,
        Arc<WriteWatermark>,
    ) {
        let cache = MetadataCache::new();
        cache.set_volume(CachedVolumeInfo {
            volume_id: vid,
            size_bytes: 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_mappings: BTreeMap::new(),
        });
        let meta = Arc::new(MockAmbiguousMetadata::new(EpochId::new(epoch)));
        let wm = Arc::new(WriteWatermark::new());
        (meta, Arc::new(cache), wm)
    }

    #[tokio::test]
    async fn test_resolve_committed() {
        let vid = VolumeId::generate();
        let (meta, cache, wm) = setup(vid, 5);
        let op = OperationId::generate();
        let eid = ExtentId::generate();

        meta.add_committed(
            op,
            CommittedMapping {
                extent_id: eid,
                extent_version: 10,
                epoch: EpochId::new(5),
                block_range: 0..64,
                replica_locations: vec![NodeId::generate()],
                checksums: vec![vec![0xAA]],
            },
        );

        let resolver = AmbiguousWriteResolver::new(meta, cache.clone(), wm.clone());
        let outcome = resolver.resolve(vid, op).await.unwrap();

        assert_eq!(
            outcome,
            AmbiguousWriteOutcome::Committed {
                epoch: EpochId::new(5),
                extent_version: 10,
            }
        );
        assert!(cache.get_extent_mapping(&vid, 0).is_some());
        assert_eq!(wm.current(), EpochId::new(5));
    }

    #[tokio::test]
    async fn test_resolve_not_committed() {
        let vid = VolumeId::generate();
        let (meta, cache, wm) = setup(vid, 5);
        let op = OperationId::generate();

        let resolver = AmbiguousWriteResolver::new(meta, cache, wm.clone());
        let outcome = resolver.resolve(vid, op).await.unwrap();

        assert_eq!(outcome, AmbiguousWriteOutcome::NotCommitted);
        assert_eq!(wm.current(), EpochId::new(0));
    }

    #[tokio::test]
    async fn test_resolve_batch_mixed() {
        let vid = VolumeId::generate();
        let (meta, cache, wm) = setup(vid, 3);

        let op1 = OperationId::generate();
        let op2 = OperationId::generate();
        let op3 = OperationId::generate();

        meta.add_committed(
            op1,
            CommittedMapping {
                extent_id: ExtentId::generate(),
                extent_version: 1,
                epoch: EpochId::new(3),
                block_range: 0..64,
                replica_locations: vec![],
                checksums: vec![],
            },
        );
        meta.add_committed(
            op3,
            CommittedMapping {
                extent_id: ExtentId::generate(),
                extent_version: 3,
                epoch: EpochId::new(3),
                block_range: 128..192,
                replica_locations: vec![],
                checksums: vec![],
            },
        );

        let resolver = AmbiguousWriteResolver::new(meta, cache, wm);
        let results = resolver.resolve_batch(vid, &[op1, op2, op3]).await.unwrap();

        assert_eq!(results.len(), 3);
        assert!(matches!(
            results[0].1,
            AmbiguousWriteOutcome::Committed { .. }
        ));
        assert_eq!(results[1].1, AmbiguousWriteOutcome::NotCommitted);
        assert!(matches!(
            results[2].1,
            AmbiguousWriteOutcome::Committed { .. }
        ));
    }

    #[tokio::test]
    async fn test_resolve_batch_empty() {
        let vid = VolumeId::generate();
        let (meta, cache, wm) = setup(vid, 1);

        let resolver = AmbiguousWriteResolver::new(meta, cache, wm);
        let results = resolver.resolve_batch(vid, &[]).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_resolve_watermark_advances_on_commit() {
        let vid = VolumeId::generate();
        let (meta, cache, wm) = setup(vid, 1);
        let op = OperationId::generate();

        meta.add_committed(
            op,
            CommittedMapping {
                extent_id: ExtentId::generate(),
                extent_version: 1,
                epoch: EpochId::new(10),
                block_range: 0..64,
                replica_locations: vec![],
                checksums: vec![],
            },
        );

        let resolver = AmbiguousWriteResolver::new(meta, cache, wm.clone());
        resolver.resolve(vid, op).await.unwrap();
        assert_eq!(wm.current(), EpochId::new(10));
    }

    #[test]
    fn test_outcome_debug() {
        let o = AmbiguousWriteOutcome::NotCommitted;
        let debug = format!("{:?}", o);
        assert!(debug.contains("NotCommitted"));
    }

    #[test]
    fn test_outcome_eq() {
        assert_eq!(
            AmbiguousWriteOutcome::NotCommitted,
            AmbiguousWriteOutcome::NotCommitted
        );
        assert_ne!(
            AmbiguousWriteOutcome::NotCommitted,
            AmbiguousWriteOutcome::Committed {
                epoch: EpochId::new(1),
                extent_version: 1,
            }
        );
    }

    #[test]
    fn test_outcome_clone() {
        let o = AmbiguousWriteOutcome::Committed {
            epoch: EpochId::new(5),
            extent_version: 42,
        };
        let cloned = o.clone();
        assert_eq!(o, cloned);
    }
}
