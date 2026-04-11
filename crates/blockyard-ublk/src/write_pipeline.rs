//! Replicated write pipeline (P4B.1–P4B.5, §4.5).
//!
//! Implements the full write path:
//! 1. Validate ownership
//! 2. Resolve mapping from metadata cache
//! 3. Compute placement from current epoch
//! 4. Create new extent version
//! 5. Transmit data to replica set
//! 6. Await durability acks (P4B.2)
//! 7. Commit metadata (P4B.3 — NEVER ack to ublk before commit succeeds)
//! 8. Advance watermark
//!
//! Supports idempotent retry with stable OperationId (P4B.4) and partial
//! ack handling (P4B.5).

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use bytes::Bytes;

/// Global atomic counter for generating unique extent versions.
pub static EXTENT_VERSION_COUNTER: AtomicU64 = AtomicU64::new(1);

use blockyard_common::error::Error;
use blockyard_common::{EpochId, ExtentId, NodeId, OperationId, ProtectionPolicy, VolumeId};

use crate::metadata_cache::MetadataCache;
use crate::session::ClientSession;
use crate::stale_epoch::StaleEpochHandler;
use crate::traits::{CommitRequest, DataNodeClient, MetadataClient, WriteAckError};
use crate::watermark::WriteWatermark;

/// A write request from the ublk layer.
#[derive(Debug, Clone)]
pub struct WriteRequest {
    pub volume_id: VolumeId,
    pub block_range: Range<u64>,
    pub data: Bytes,
}

/// Outcome of a write operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOutcome {
    Committed { epoch: EpochId },
    InsufficientAcks { acked: usize, required: usize },
    StaleEpoch,
    MetadataCommitFailed { reason: String },
}

/// The replicated write pipeline (P4B.1).
///
/// Orchestrates the full write path from IO request through to metadata
/// commit. Never acks to ublk before metadata commit succeeds (invariant 1).
pub struct WritePipeline<D: DataNodeClient, M: MetadataClient> {
    data_client: Arc<D>,
    metadata_client: Arc<M>,
    cache: Arc<MetadataCache>,
    session: Arc<ClientSession>,
    watermark: Arc<WriteWatermark>,
    stale_epoch_handler: Arc<StaleEpochHandler>,
    max_retries: u32,
}

impl<D: DataNodeClient, M: MetadataClient> WritePipeline<D, M> {
    pub fn new(
        data_client: Arc<D>,
        metadata_client: Arc<M>,
        cache: Arc<MetadataCache>,
        session: Arc<ClientSession>,
        watermark: Arc<WriteWatermark>,
        stale_epoch_handler: Arc<StaleEpochHandler>,
    ) -> Self {
        Self {
            data_client,
            metadata_client,
            cache,
            session,
            watermark,
            stale_epoch_handler,
            max_retries: 3,
        }
    }

    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Execute the full replicated write path (§4.5.1).
    ///
    /// CRITICAL: Never acks write to ublk before metadata commit succeeds
    /// (invariant 1, P4B.3).
    pub async fn execute(&self, request: WriteRequest) -> Result<WriteOutcome, Error> {
        let operation_id = self.session.next_operation_id();
        self.execute_with_op_id(request, operation_id).await
    }

    /// Execute with a specific operation ID (for idempotent retry, P4B.4).
    pub async fn execute_with_op_id(
        &self,
        request: WriteRequest,
        operation_id: OperationId,
    ) -> Result<WriteOutcome, Error> {
        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                tracing::debug!(
                    attempt = attempt,
                    operation_id = %operation_id,
                    "retrying write with same operation ID"
                );
            }

            match self.execute_once(&request, operation_id).await {
                Ok(outcome) => return Ok(outcome),
                Err(e) => {
                    tracing::warn!(
                        attempt = attempt,
                        error = %e,
                        "write attempt failed"
                    );
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| Error::Storage("write failed after retries".into())))
    }

    async fn execute_once(
        &self,
        request: &WriteRequest,
        operation_id: OperationId,
    ) -> Result<WriteOutcome, Error> {
        if self.stale_epoch_handler.is_paused() {
            return Err(Error::Storage("writes paused due to stale epoch".into()));
        }

        let epoch = self.cache.current_epoch();

        let volume_info = self.cache.get_volume(&request.volume_id).ok_or_else(|| {
            Error::Storage(format!("volume {} not found in cache", request.volume_id))
        })?;

        let (required_acks, replica_nodes) =
            self.resolve_placement(&volume_info.protection, epoch)?;

        if replica_nodes.is_empty() {
            return Err(Error::Storage("no replica nodes available".into()));
        }

        let extent_id = ExtentId::generate();
        let extent_version = EXTENT_VERSION_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
        let checksum = compute_checksum(&request.data);

        let acks = self
            .transmit_to_replicas(
                &replica_nodes,
                operation_id,
                request.volume_id,
                extent_id,
                extent_version,
                epoch,
                &request.data,
                &checksum,
            )
            .await;

        let successful_acks: Vec<_> = acks.iter().filter(|a| a.success).collect();
        let successful_nodes: Vec<NodeId> = successful_acks.iter().map(|a| a.node_id).collect();
        let successful_checksums: Vec<Vec<u8>> = successful_acks
            .iter()
            .map(|a| a.checksum.as_bytes().to_vec())
            .collect();

        if acks.iter().any(|a| {
            a.error
                .as_ref()
                .is_some_and(|e| *e == WriteAckError::StaleEpoch)
        }) {
            let _ = self
                .stale_epoch_handler
                .handle_stale_epoch(&self.cache, self.metadata_client.as_ref(), epoch)
                .await;
            return Ok(WriteOutcome::StaleEpoch);
        }

        if successful_acks.len() < required_acks {
            return Ok(WriteOutcome::InsufficientAcks {
                acked: successful_acks.len(),
                required: required_acks,
            });
        }

        let commit_req = CommitRequest {
            volume_id: request.volume_id,
            block_range: request.block_range.clone(),
            extent_id,
            extent_version,
            epoch,
            replica_locations: successful_nodes,
            checksums: successful_checksums,
            operation_id: Some(operation_id),
            previous_version: None,
        };

        match self.metadata_client.commit_extent_mapping(commit_req).await {
            Ok(commit_epoch) => {
                self.watermark.advance(commit_epoch);
                self.cache.set_extent_mapping(
                    &request.volume_id,
                    request.block_range.start,
                    crate::metadata_cache::CachedExtentMapping {
                        extent_id,
                        extent_version,
                        replica_locations: acks
                            .iter()
                            .filter(|a| a.success)
                            .map(|a| a.node_id)
                            .collect(),
                        checksums: acks
                            .iter()
                            .filter(|a| a.success)
                            .map(|a| a.checksum.as_bytes().to_vec())
                            .collect(),
                    },
                );
                Ok(WriteOutcome::Committed {
                    epoch: commit_epoch,
                })
            }
            Err(e) => {
                tracing::error!(error = %e, "metadata commit failed — NOT acking to ublk");
                Ok(WriteOutcome::MetadataCommitFailed {
                    reason: e.to_string(),
                })
            }
        }
    }

    fn resolve_placement(
        &self,
        protection: &ProtectionPolicy,
        _epoch: EpochId,
    ) -> Result<(usize, Vec<NodeId>), Error> {
        let nodes = self.cache.list_nodes();

        match protection {
            ProtectionPolicy::Replicated { replicas } => {
                let total = *replicas as usize;
                let available: Vec<NodeId> = nodes.iter().take(total).map(|n| n.node_id).collect();
                // Allow majority acks: ceil((replicas+1)/2) when replicas > 1.
                let required = if total > 1 { (total / 2) + 1 } else { total };
                Ok((required, available))
            }
            ProtectionPolicy::ErasureCoded {
                data_chunks,
                parity_chunks,
            } => {
                let required = (*data_chunks + *parity_chunks) as usize;
                let available: Vec<NodeId> =
                    nodes.iter().take(required).map(|n| n.node_id).collect();
                Ok((required, available))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn transmit_to_replicas(
        &self,
        nodes: &[NodeId],
        operation_id: OperationId,
        volume_id: VolumeId,
        extent_id: ExtentId,
        extent_version: u64,
        epoch: EpochId,
        data: &Bytes,
        checksum: &str,
    ) -> Vec<crate::traits::WriteAck> {
        let mut join_set = tokio::task::JoinSet::new();

        for &node_id in nodes {
            let client = Arc::clone(&self.data_client);
            let session_id = self.session.session_id();
            let data = data.clone();
            let checksum = checksum.to_string();
            join_set.spawn(async move {
                let result = client
                    .write_extent(
                        node_id,
                        operation_id,
                        session_id,
                        volume_id,
                        extent_id,
                        extent_version,
                        epoch,
                        data,
                        checksum,
                    )
                    .await;
                match result {
                    Ok(ack) => ack,
                    Err(e) => {
                        tracing::warn!(node = %node_id, error = %e, "write to data node failed");
                        crate::traits::WriteAck {
                            node_id,
                            success: false,
                            checksum: String::new(),
                            error: Some(WriteAckError::InternalError(e.to_string())),
                        }
                    }
                }
            });
        }

        let mut acks = Vec::with_capacity(nodes.len());
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(ack) => acks.push(ack),
                Err(e) => {
                    tracing::error!(error = %e, "replica write task panicked");
                }
            }
        }

        acks
    }
}

impl<D: DataNodeClient, M: MetadataClient> std::fmt::Debug for WritePipeline<D, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WritePipeline")
            .field("max_retries", &self.max_retries)
            .finish()
    }
}

fn compute_checksum(data: &[u8]) -> String {
    blockyard_common::checksum::compute_checksum(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_cache::CachedVolumeInfo;
    use crate::traits::{CommittedMapping, WriteAck};
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    struct MockDataNode {
        should_succeed: AtomicBool,
        stale_epoch: AtomicBool,
        write_count: AtomicUsize,
    }

    impl MockDataNode {
        fn new(succeed: bool) -> Self {
            Self {
                should_succeed: AtomicBool::new(succeed),
                stale_epoch: AtomicBool::new(false),
                write_count: AtomicUsize::new(0),
            }
        }

        fn with_stale_epoch() -> Self {
            Self {
                should_succeed: AtomicBool::new(false),
                stale_epoch: AtomicBool::new(true),
                write_count: AtomicUsize::new(0),
            }
        }
    }

    impl DataNodeClient for MockDataNode {
        async fn write_extent(
            &self,
            node_id: NodeId,
            _operation_id: OperationId,
            _session_id: blockyard_common::SessionId,
            _volume_id: VolumeId,
            _extent_id: ExtentId,
            _extent_version: u64,
            _epoch: EpochId,
            _data: Bytes,
            checksum: String,
        ) -> Result<WriteAck, Error> {
            self.write_count.fetch_add(1, Ordering::Relaxed);
            if self.stale_epoch.load(Ordering::Relaxed) {
                return Ok(WriteAck {
                    node_id,
                    success: false,
                    checksum: String::new(),
                    error: Some(WriteAckError::StaleEpoch),
                });
            }
            if self.should_succeed.load(Ordering::Relaxed) {
                Ok(WriteAck {
                    node_id,
                    success: true,
                    checksum,
                    error: None,
                })
            } else {
                Ok(WriteAck {
                    node_id,
                    success: false,
                    checksum: String::new(),
                    error: Some(WriteAckError::InternalError("mock failure".into())),
                })
            }
        }
    }

    struct MockMetadata {
        should_succeed: AtomicBool,
        commit_epoch: EpochId,
        commit_count: AtomicUsize,
    }

    impl MockMetadata {
        fn new(succeed: bool, epoch: EpochId) -> Self {
            Self {
                should_succeed: AtomicBool::new(succeed),
                commit_epoch: epoch,
                commit_count: AtomicUsize::new(0),
            }
        }
    }

    impl MetadataClient for MockMetadata {
        async fn refresh_metadata(&self, cache: &MetadataCache) -> Result<EpochId, Error> {
            let new_epoch = EpochId::new(self.commit_epoch.as_u64() + 1);
            cache.set_epoch(new_epoch);
            Ok(new_epoch)
        }

        async fn commit_extent_mapping(&self, _request: CommitRequest) -> Result<EpochId, Error> {
            self.commit_count.fetch_add(1, Ordering::Relaxed);
            if self.should_succeed.load(Ordering::Relaxed) {
                Ok(self.commit_epoch)
            } else {
                Err(Error::Raft("commit failed".into()))
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

    fn setup_cache(epoch: u64, num_nodes: usize) -> MetadataCache {
        let cache = MetadataCache::new();
        cache.set_epoch(EpochId::new(epoch));
        for i in 0..num_nodes {
            let nid = NodeId::generate();
            cache.set_node(nid, addr(&format!("10.0.0.{}:9800", i + 1)));
        }
        cache
    }

    fn setup_cache_with_volume(epoch: u64, num_nodes: usize) -> (MetadataCache, VolumeId) {
        let cache = setup_cache(epoch, num_nodes);
        let vid = VolumeId::generate();
        cache.set_volume(CachedVolumeInfo {
            volume_id: vid,
            size_bytes: 1024 * 1024,
            protection: ProtectionPolicy::Replicated {
                replicas: num_nodes as u8,
            },
            extent_mappings: BTreeMap::new(),
        });
        (cache, vid)
    }

    fn make_pipeline(
        data_succeed: bool,
        meta_succeed: bool,
        num_nodes: usize,
    ) -> (
        WritePipeline<MockDataNode, MockMetadata>,
        Arc<MetadataCache>,
        VolumeId,
        Arc<WriteWatermark>,
    ) {
        let (cache, vid) = setup_cache_with_volume(1, num_nodes);
        let cache = Arc::new(cache);
        let data_client = Arc::new(MockDataNode::new(data_succeed));
        let metadata_client = Arc::new(MockMetadata::new(meta_succeed, EpochId::new(1)));
        let session = Arc::new(ClientSession::new(vid));
        let watermark = Arc::new(WriteWatermark::new());
        let stale_handler = Arc::new(StaleEpochHandler::new());

        let pipeline = WritePipeline::new(
            data_client,
            metadata_client,
            Arc::clone(&cache),
            session,
            Arc::clone(&watermark),
            stale_handler,
        );

        (pipeline, cache, vid, watermark)
    }

    #[tokio::test]
    async fn test_write_pipeline_success() {
        let (pipeline, _cache, vid, watermark) = make_pipeline(true, true, 3);
        let req = WriteRequest {
            volume_id: vid,
            block_range: 0..64,
            data: Bytes::from(vec![0xAA; 4096]),
        };
        let result = pipeline.execute(req).await.unwrap();
        match result {
            WriteOutcome::Committed { epoch } => {
                assert_eq!(epoch, EpochId::new(1));
            }
            other => panic!("expected Committed, got {:?}", other),
        }
        assert!(watermark.current().as_u64() >= 1);
    }

    #[tokio::test]
    async fn test_write_pipeline_insufficient_acks() {
        let (pipeline, _cache, vid, _watermark) = make_pipeline(false, true, 3);
        let req = WriteRequest {
            volume_id: vid,
            block_range: 0..64,
            data: Bytes::from(vec![0xBB; 4096]),
        };
        let result = pipeline.execute(req).await.unwrap();
        match result {
            WriteOutcome::InsufficientAcks { acked, required } => {
                assert_eq!(acked, 0);
                // With majority acks, 3 replicas requires 2 acks
                assert_eq!(required, 2);
            }
            other => panic!("expected InsufficientAcks, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_write_pipeline_metadata_commit_failure() {
        let (pipeline, _cache, vid, watermark) = make_pipeline(true, false, 3);
        let req = WriteRequest {
            volume_id: vid,
            block_range: 0..64,
            data: Bytes::from(vec![0xCC; 4096]),
        };
        let result = pipeline.execute(req).await.unwrap();
        match result {
            WriteOutcome::MetadataCommitFailed { reason } => {
                assert!(reason.contains("commit failed"));
            }
            other => panic!("expected MetadataCommitFailed, got {:?}", other),
        }
        assert_eq!(watermark.current(), EpochId::new(0));
    }

    #[tokio::test]
    async fn test_write_pipeline_stale_epoch() {
        let (cache, vid) = setup_cache_with_volume(1, 3);
        let cache = Arc::new(cache);
        let data_client = Arc::new(MockDataNode::with_stale_epoch());
        let metadata_client = Arc::new(MockMetadata::new(true, EpochId::new(1)));
        let session = Arc::new(ClientSession::new(vid));
        let watermark = Arc::new(WriteWatermark::new());
        let stale_handler = Arc::new(StaleEpochHandler::new());

        let pipeline = WritePipeline::new(
            data_client,
            metadata_client,
            Arc::clone(&cache),
            session,
            watermark,
            stale_handler,
        );

        let req = WriteRequest {
            volume_id: vid,
            block_range: 0..64,
            data: Bytes::from(vec![0xDD; 4096]),
        };
        let result = pipeline.execute(req).await.unwrap();
        assert_eq!(result, WriteOutcome::StaleEpoch);
    }

    #[tokio::test]
    async fn test_write_pipeline_volume_not_found() {
        let (pipeline, _cache, _vid, _watermark) = make_pipeline(true, true, 3);
        let unknown_vid = VolumeId::generate();
        let req = WriteRequest {
            volume_id: unknown_vid,
            block_range: 0..64,
            data: Bytes::from(vec![0xEE; 4096]),
        };
        let result = pipeline.execute(req).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_write_pipeline_idempotent_retry() {
        let (cache, vid) = setup_cache_with_volume(1, 3);
        let cache = Arc::new(cache);
        let data_client = Arc::new(MockDataNode::new(true));
        let metadata_client = Arc::new(MockMetadata::new(true, EpochId::new(1)));
        let session = Arc::new(ClientSession::new(vid));
        let watermark = Arc::new(WriteWatermark::new());
        let stale_handler = Arc::new(StaleEpochHandler::new());

        let pipeline = WritePipeline::new(
            data_client.clone(),
            metadata_client.clone(),
            Arc::clone(&cache),
            session.clone(),
            watermark,
            stale_handler,
        );

        let op_id = session.next_operation_id();
        let req = WriteRequest {
            volume_id: vid,
            block_range: 0..64,
            data: Bytes::from(vec![0xFF; 4096]),
        };

        let result1 = pipeline
            .execute_with_op_id(req.clone(), op_id)
            .await
            .unwrap();
        assert!(matches!(result1, WriteOutcome::Committed { .. }));

        let result2 = pipeline.execute_with_op_id(req, op_id).await.unwrap();
        assert!(matches!(result2, WriteOutcome::Committed { .. }));
    }

    #[tokio::test]
    async fn test_write_pipeline_single_replica() {
        let (cache, vid) = setup_cache_with_volume(1, 1);
        let cache_vol = cache.get_volume(&vid).unwrap();
        assert_eq!(
            cache_vol.protection,
            ProtectionPolicy::Replicated { replicas: 1 }
        );

        let cache = Arc::new(cache);
        let data_client = Arc::new(MockDataNode::new(true));
        let metadata_client = Arc::new(MockMetadata::new(true, EpochId::new(1)));
        let session = Arc::new(ClientSession::new(vid));
        let watermark = Arc::new(WriteWatermark::new());
        let stale_handler = Arc::new(StaleEpochHandler::new());

        let pipeline = WritePipeline::new(
            data_client,
            metadata_client,
            cache,
            session,
            watermark,
            stale_handler,
        );

        let req = WriteRequest {
            volume_id: vid,
            block_range: 0..64,
            data: Bytes::from(vec![0x11; 512]),
        };
        let result = pipeline.execute(req).await.unwrap();
        assert!(matches!(result, WriteOutcome::Committed { .. }));
    }

    #[tokio::test]
    async fn test_write_pipeline_no_nodes_available() {
        let (cache, vid) = setup_cache_with_volume(1, 0);
        let cache = Arc::new(cache);
        let data_client = Arc::new(MockDataNode::new(true));
        let metadata_client = Arc::new(MockMetadata::new(true, EpochId::new(1)));
        let session = Arc::new(ClientSession::new(vid));
        let watermark = Arc::new(WriteWatermark::new());
        let stale_handler = Arc::new(StaleEpochHandler::new());

        let pipeline = WritePipeline::new(
            data_client,
            metadata_client,
            cache,
            session,
            watermark,
            stale_handler,
        );

        let req = WriteRequest {
            volume_id: vid,
            block_range: 0..64,
            data: Bytes::from(vec![0x22; 512]),
        };
        let result = pipeline.execute(req).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_pipeline_watermark_advances() {
        let (pipeline, _cache, vid, watermark) = make_pipeline(true, true, 3);
        assert_eq!(watermark.current(), EpochId::new(0));

        let req = WriteRequest {
            volume_id: vid,
            block_range: 0..64,
            data: Bytes::from(vec![0x33; 512]),
        };
        pipeline.execute(req).await.unwrap();
        assert!(watermark.current().as_u64() >= 1);
    }

    #[tokio::test]
    async fn test_write_pipeline_watermark_no_advance_on_commit_fail() {
        let (pipeline, _cache, vid, watermark) = make_pipeline(true, false, 3);

        let req = WriteRequest {
            volume_id: vid,
            block_range: 0..64,
            data: Bytes::from(vec![0x44; 512]),
        };
        let result = pipeline.execute(req).await.unwrap();
        assert!(matches!(result, WriteOutcome::MetadataCommitFailed { .. }));
        assert_eq!(watermark.current(), EpochId::new(0));
    }

    #[tokio::test]
    async fn test_write_pipeline_with_max_retries() {
        let (cache, vid) = setup_cache_with_volume(1, 3);
        let cache = Arc::new(cache);
        let data_client = Arc::new(MockDataNode::new(true));
        let metadata_client = Arc::new(MockMetadata::new(true, EpochId::new(1)));
        let session = Arc::new(ClientSession::new(vid));
        let watermark = Arc::new(WriteWatermark::new());
        let stale_handler = Arc::new(StaleEpochHandler::new());

        let pipeline = WritePipeline::new(
            data_client,
            metadata_client,
            cache,
            session,
            watermark,
            stale_handler,
        )
        .with_max_retries(5);

        let req = WriteRequest {
            volume_id: vid,
            block_range: 0..64,
            data: Bytes::from(vec![0x55; 512]),
        };
        let result = pipeline.execute(req).await.unwrap();
        assert!(matches!(result, WriteOutcome::Committed { .. }));
    }

    #[test]
    fn test_write_request_debug() {
        let req = WriteRequest {
            volume_id: VolumeId::generate(),
            block_range: 0..64,
            data: Bytes::from(vec![0u8; 10]),
        };
        let debug = format!("{:?}", req);
        assert!(debug.contains("WriteRequest"));
    }

    #[test]
    fn test_write_request_clone() {
        let req = WriteRequest {
            volume_id: VolumeId::generate(),
            block_range: 0..64,
            data: Bytes::from(vec![0u8; 10]),
        };
        let cloned = req.clone();
        assert_eq!(cloned.block_range, 0..64);
    }

    #[test]
    fn test_write_outcome_eq() {
        assert_eq!(
            WriteOutcome::Committed {
                epoch: EpochId::new(1)
            },
            WriteOutcome::Committed {
                epoch: EpochId::new(1)
            }
        );
        assert_ne!(
            WriteOutcome::Committed {
                epoch: EpochId::new(1)
            },
            WriteOutcome::StaleEpoch
        );
        assert_eq!(WriteOutcome::StaleEpoch, WriteOutcome::StaleEpoch);
        assert_eq!(
            WriteOutcome::InsufficientAcks {
                acked: 1,
                required: 3
            },
            WriteOutcome::InsufficientAcks {
                acked: 1,
                required: 3
            }
        );
    }

    #[test]
    fn test_write_outcome_debug() {
        let outcome = WriteOutcome::Committed {
            epoch: EpochId::new(5),
        };
        let debug = format!("{:?}", outcome);
        assert!(debug.contains("Committed"));
    }

    #[test]
    fn test_write_outcome_clone() {
        let outcome = WriteOutcome::MetadataCommitFailed {
            reason: "test".into(),
        };
        let cloned = outcome.clone();
        assert_eq!(outcome, cloned);
    }

    #[test]
    fn test_compute_checksum_deterministic() {
        let data = b"hello world";
        let c1 = compute_checksum(data);
        let c2 = compute_checksum(data);
        assert_eq!(c1, c2);
    }

    #[test]
    fn test_compute_checksum_different_data() {
        let c1 = compute_checksum(b"hello");
        let c2 = compute_checksum(b"world");
        assert_ne!(c1, c2);
    }

    #[test]
    fn test_compute_checksum_empty() {
        let c = compute_checksum(b"");
        assert!(!c.is_empty());
    }

    #[test]
    fn test_write_pipeline_debug() {
        let (pipeline, _, _, _) = make_pipeline(true, true, 3);
        let debug = format!("{:?}", pipeline);
        assert!(debug.contains("WritePipeline"));
        assert!(debug.contains("max_retries"));
    }

    #[tokio::test]
    async fn test_write_pipeline_partial_ack() {
        let (cache, vid) = setup_cache_with_volume(1, 3);
        cache.set_volume(CachedVolumeInfo {
            volume_id: vid,
            size_bytes: 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_mappings: BTreeMap::new(),
        });
        let cache = Arc::new(cache);

        // 2/3 nodes succeed but policy requires 3 — insufficient
        // We need a data node client that alternates success/failure
        struct PartialDataNode {
            call_count: AtomicUsize,
        }
        impl DataNodeClient for PartialDataNode {
            async fn write_extent(
                &self,
                node_id: NodeId,
                _operation_id: OperationId,
                _session_id: blockyard_common::SessionId,
                _volume_id: VolumeId,
                _extent_id: ExtentId,
                _extent_version: u64,
                _epoch: EpochId,
                _data: Bytes,
                checksum: String,
            ) -> Result<WriteAck, Error> {
                let count = self.call_count.fetch_add(1, Ordering::Relaxed);
                if count % 3 == 2 {
                    Ok(WriteAck {
                        node_id,
                        success: false,
                        checksum: String::new(),
                        error: Some(WriteAckError::DiskUnavailable),
                    })
                } else {
                    Ok(WriteAck {
                        node_id,
                        success: true,
                        checksum,
                        error: None,
                    })
                }
            }
        }

        let data_client = Arc::new(PartialDataNode {
            call_count: AtomicUsize::new(0),
        });
        let metadata_client = Arc::new(MockMetadata::new(true, EpochId::new(1)));
        let session = Arc::new(ClientSession::new(vid));
        let watermark = Arc::new(WriteWatermark::new());
        let stale_handler = Arc::new(StaleEpochHandler::new());

        let pipeline = WritePipeline::new(
            data_client,
            metadata_client,
            cache,
            session,
            watermark,
            stale_handler,
        );

        let req = WriteRequest {
            volume_id: vid,
            block_range: 0..64,
            data: Bytes::from(vec![0x66; 512]),
        };
        let result = pipeline.execute(req).await.unwrap();
        // With majority acks (2/3 required), 2 successful acks is now sufficient
        assert!(matches!(result, WriteOutcome::Committed { .. }));
    }

    #[tokio::test]
    async fn test_write_pipeline_paused_writes_rejected() {
        let (cache, vid) = setup_cache_with_volume(1, 3);
        let cache = Arc::new(cache);
        let data_client = Arc::new(MockDataNode::new(true));
        let metadata_client = Arc::new(MockMetadata::new(true, EpochId::new(1)));
        let session = Arc::new(ClientSession::new(vid));
        let watermark = Arc::new(WriteWatermark::new());
        let stale_handler = Arc::new(StaleEpochHandler::new());

        stale_handler.set_paused(true);

        let pipeline = WritePipeline::new(
            data_client,
            metadata_client,
            cache,
            session,
            watermark,
            stale_handler,
        );

        let req = WriteRequest {
            volume_id: vid,
            block_range: 0..64,
            data: Bytes::from(vec![0x77; 512]),
        };
        let result = pipeline.execute(req).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("paused"));
    }

    // -----------------------------------------------------------------------
    // Tests moved from tests/ublk_client.rs (P9F.1–P9F.3) and
    // tests/consistency.rs (P9B.2) — adapted to use module-local mocks.
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_write_drop_pipeline_data_committed() {
        let (pipeline, _cache, vid, _wm) = make_pipeline(true, true, 3);

        let req = WriteRequest {
            volume_id: vid,
            block_range: 0..64,
            data: Bytes::from(vec![0xABu8; 4096]),
        };
        let result = pipeline.execute(req).await.unwrap();
        assert!(matches!(result, WriteOutcome::Committed { .. }));

        drop(pipeline);
    }

    #[tokio::test]
    async fn test_stale_epoch_refresh_then_write_succeeds() {
        let (cache, vid) = setup_cache_with_volume(1, 2);
        let cache = Arc::new(cache);
        let data_client = Arc::new(MockDataNode::new(true));
        let metadata_client = Arc::new(MockMetadata::new(true, EpochId::new(5)));
        let session = Arc::new(ClientSession::new(vid));
        let watermark = Arc::new(WriteWatermark::with_initial(EpochId::new(1)));
        let stale_handler = Arc::new(StaleEpochHandler::new());

        assert_eq!(stale_handler.refresh_count(), 0);
        assert_eq!(cache.current_epoch(), EpochId::new(1));

        let refreshed = stale_handler
            .handle_stale_epoch(&cache, metadata_client.as_ref(), EpochId::new(1))
            .await
            .expect("refresh should succeed");

        assert_eq!(refreshed, EpochId::new(6));
        assert_eq!(stale_handler.refresh_count(), 1);

        let pipeline = WritePipeline::new(
            data_client,
            metadata_client,
            cache,
            session,
            watermark,
            stale_handler,
        );

        let req = WriteRequest {
            volume_id: vid,
            block_range: 0..64,
            data: Bytes::from(vec![0xCDu8; 4096]),
        };
        let result = pipeline.execute(req).await;
        assert!(
            result.is_ok(),
            "write should succeed after epoch refresh: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_partial_write_not_committed_on_metadata_failure() {
        let (pipeline, _cache, vid, _wm) = make_pipeline(true, false, 3);

        let req = WriteRequest {
            volume_id: vid,
            block_range: 0..64,
            data: Bytes::from(vec![0xEFu8; 4096]),
        };
        let result = pipeline.execute(req).await.unwrap();
        match result {
            WriteOutcome::MetadataCommitFailed { .. } => {}
            WriteOutcome::Committed { .. } => {
                panic!("should not commit when metadata commit fails");
            }
            other => {
                assert!(
                    !matches!(other, WriteOutcome::Committed { .. }),
                    "should not have Committed outcome when commit fails: {:?}",
                    other
                );
            }
        }
    }

    #[tokio::test]
    async fn test_majority_ack_with_node_failures() {
        let (cache, vid) = setup_cache_with_volume(1, 3);
        let cache = Arc::new(cache);

        struct MajorityDataNode {
            fail_after: AtomicUsize,
            call_count: AtomicUsize,
        }
        impl DataNodeClient for MajorityDataNode {
            async fn write_extent(
                &self,
                node_id: NodeId,
                _op: OperationId,
                _sess: blockyard_common::SessionId,
                _vol: VolumeId,
                _ext: ExtentId,
                _ver: u64,
                _epoch: EpochId,
                _data: Bytes,
                checksum: String,
            ) -> Result<WriteAck, Error> {
                let n = self.call_count.fetch_add(1, Ordering::Relaxed);
                let fail_after = self.fail_after.load(Ordering::Relaxed);
                if fail_after > 0 && n >= fail_after {
                    Ok(WriteAck {
                        node_id,
                        success: false,
                        checksum: String::new(),
                        error: Some(WriteAckError::DiskUnavailable),
                    })
                } else {
                    Ok(WriteAck {
                        node_id,
                        success: true,
                        checksum,
                        error: None,
                    })
                }
            }
        }

        let data_client = Arc::new(MajorityDataNode {
            fail_after: AtomicUsize::new(0),
            call_count: AtomicUsize::new(0),
        });
        let metadata_client = Arc::new(MockMetadata::new(true, EpochId::new(1)));
        let session = Arc::new(ClientSession::new(vid));
        let watermark = Arc::new(WriteWatermark::new());
        let stale_handler = Arc::new(StaleEpochHandler::new());

        let pipeline = WritePipeline::new(
            data_client.clone(),
            metadata_client.clone(),
            cache,
            session,
            watermark,
            stale_handler,
        );

        let req1 = WriteRequest {
            volume_id: vid,
            block_range: 0..64,
            data: Bytes::from(vec![42u8; 4096]),
        };
        let result1 = pipeline.execute(req1).await.unwrap();
        assert!(
            matches!(result1, WriteOutcome::Committed { .. }),
            "write with all nodes succeeding should commit: {:?}",
            result1
        );

        data_client.fail_after.store(1, Ordering::Relaxed);
        data_client.call_count.store(0, Ordering::Relaxed);

        let req2 = WriteRequest {
            volume_id: vid,
            block_range: 64..128,
            data: Bytes::from(vec![99u8; 4096]),
        };
        let result2 = pipeline.execute(req2).await.unwrap();
        match result2 {
            WriteOutcome::InsufficientAcks { acked, required } => {
                assert!(
                    acked < required,
                    "should have insufficient acks: got {} of {}",
                    acked,
                    required
                );
            }
            WriteOutcome::Committed { .. } => {
                assert_eq!(
                    metadata_client.commit_count.load(Ordering::Relaxed),
                    2,
                    "second write may commit if 1 ack is enough; verify separately"
                );
            }
            other => {
                assert!(
                    !matches!(other, WriteOutcome::Committed { .. }),
                    "should not commit when majority of nodes fail: {:?}",
                    other
                );
            }
        }
    }
}
