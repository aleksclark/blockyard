//! Cluster-aware block handler (P4A.1, §4.5, §4.6).
//!
//! [`ClusterBlockHandler`] translates kernel block IO requests into
//! cluster operations via the write and read pipelines. It holds
//! all dependencies needed to serve a single mounted volume.

use std::sync::Arc;

use bytes::Bytes;

use blockyard_common::error::Error;
use blockyard_common::{ProtectionPolicy, VolumeId};

use blockyard_client::traits::DataNodeReader;

use crate::ec_write_pipeline::EcWritePipeline;
use crate::lease_manager::{LeaseManager, LeaseState};
use crate::metadata_cache::MetadataCache;
use crate::session::ClientSession;
use crate::stale_epoch::StaleEpochHandler;
use crate::traits::{DataNodeClient, MetadataClient};
use crate::ublk::{BlockHandler, IoOperation, IoRequest};
use crate::watermark::WriteWatermark;
use crate::write_pipeline::{WriteOutcome, WritePipeline, WriteRequest};

/// Volume metadata used by the handler to dispatch IO correctly.
#[derive(Debug, Clone)]
pub struct VolumeConfig {
    pub volume_id: VolumeId,
    pub size_bytes: u64,
    pub block_size: u32,
    pub protection: ProtectionPolicy,
}

/// A [`BlockHandler`] that dispatches IO through the cluster pipelines.
///
/// - Write → [`WritePipeline`] (replicated) or [`EcWritePipeline`] (EC)
/// - Read → returns data from cache/pipeline (see below)
/// - Flush → ensures pending writes are committed
/// - Discard → no-op
///
/// The handler checks the write lease before every write and refuses
/// IO if the lease is lost.
pub struct ClusterBlockHandler<D: DataNodeClient + DataNodeReader, M: MetadataClient> {
    volume_config: VolumeConfig,
    write_pipeline: Arc<WritePipeline<D, M>>,
    ec_write_pipeline: Arc<EcWritePipeline<D, M>>,
    lease_manager: Arc<LeaseManager>,
    session: Arc<ClientSession>,
    metadata_cache: Arc<MetadataCache>,
    watermark: Arc<WriteWatermark>,
    data_client: Arc<D>,
    metadata_client: Arc<M>,
}

impl<D: DataNodeClient + DataNodeReader, M: MetadataClient> ClusterBlockHandler<D, M> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        volume_config: VolumeConfig,
        data_client: Arc<D>,
        metadata_client: Arc<M>,
        lease_manager: Arc<LeaseManager>,
        session: Arc<ClientSession>,
        metadata_cache: Arc<MetadataCache>,
        watermark: Arc<WriteWatermark>,
        stale_epoch_handler: Arc<StaleEpochHandler>,
    ) -> Self {
        let write_pipeline = Arc::new(WritePipeline::new(
            Arc::clone(&data_client),
            Arc::clone(&metadata_client),
            Arc::clone(&metadata_cache),
            Arc::clone(&session),
            Arc::clone(&watermark),
            Arc::clone(&stale_epoch_handler),
        ));

        let ec_write_pipeline = Arc::new(EcWritePipeline::new(
            Arc::clone(&data_client),
            Arc::clone(&metadata_client),
            Arc::clone(&metadata_cache),
            Arc::clone(&session),
            Arc::clone(&watermark),
            stale_epoch_handler,
        ));

        Self {
            volume_config,
            write_pipeline,
            ec_write_pipeline,
            lease_manager,
            session,
            metadata_cache,
            watermark,
            data_client,
            metadata_client,
        }
    }

    pub fn volume_config(&self) -> &VolumeConfig {
        &self.volume_config
    }

    pub fn session(&self) -> &ClientSession {
        &self.session
    }

    pub fn lease_manager(&self) -> &LeaseManager {
        &self.lease_manager
    }

    pub fn metadata_cache(&self) -> &MetadataCache {
        &self.metadata_cache
    }

    pub fn watermark(&self) -> &WriteWatermark {
        &self.watermark
    }

    pub fn data_client(&self) -> &D {
        &self.data_client
    }

    pub fn metadata_client(&self) -> &M {
        &self.metadata_client
    }

    async fn handle_write(&self, request: IoRequest) -> Result<Option<Bytes>, Error> {
        if self.lease_manager.state() != LeaseState::Active {
            return Err(Error::Auth("write lease not active".into()));
        }
        self.lease_manager.check_write_allowed()?;

        let block_range = request.block_range(self.volume_config.block_size as u64);

        let data = request
            .data
            .ok_or_else(|| Error::Storage("write request missing data".into()))?;

        let outcome = match self.volume_config.protection {
            ProtectionPolicy::Replicated { .. } => {
                let write_req = WriteRequest {
                    volume_id: self.volume_config.volume_id,
                    block_range,
                    data,
                };
                self.write_pipeline.execute(write_req).await?
            }
            ProtectionPolicy::ErasureCoded { .. } => {
                self.ec_write_pipeline
                    .execute(self.volume_config.volume_id, block_range, data)
                    .await?
            }
        };

        match outcome {
            WriteOutcome::Committed { .. } => Ok(None),
            WriteOutcome::StaleEpoch => Err(Error::Storage("write rejected: stale epoch".into())),
            WriteOutcome::InsufficientAcks { acked, required } => Err(Error::Storage(format!(
                "write failed: insufficient acks ({acked}/{required})"
            ))),
            WriteOutcome::MetadataCommitFailed { reason } => Err(Error::Storage(format!(
                "write failed: metadata commit failed: {reason}"
            ))),
        }
    }

    async fn handle_read(&self, request: IoRequest) -> Result<Option<Bytes>, Error> {
        let block_size = self.volume_config.block_size as u64;
        let block_range = request.block_range(block_size);
        let total_length = request.length_bytes as usize;

        // For multi-block reads, read each block separately and concatenate.
        // Each block may map to a different extent (write pipeline creates one
        // extent per write, and writes are typically single-block).
        let num_blocks = (block_range.end - block_range.start) as usize;

        if num_blocks <= 1 {
            // Single-block read — fast path
            return self.read_single_block(&request, block_range.start, block_size).await;
        }

        // Multi-block read: stitch together data from individual extents
        let mut result_buf = Vec::with_capacity(total_length);
        for block_num in block_range.start..block_range.end {
            let block_data = self.read_single_block_data(block_num, block_size).await?;
            result_buf.extend_from_slice(&block_data);
        }

        // Trim to exact requested length (in case of rounding)
        result_buf.truncate(total_length);
        Ok(Some(Bytes::from(result_buf)))
    }

    async fn read_single_block(
        &self,
        request: &IoRequest,
        block_num: u64,
        block_size: u64,
    ) -> Result<Option<Bytes>, Error> {
        let block_range = block_num..block_num + 1;
        let mapping = self
            .metadata_cache
            .find_extent_for_range(&self.volume_config.volume_id, &block_range);

        let Some((_block_start, extent_mapping)) = mapping else {
            let length = request.length_bytes as usize;
            return Ok(Some(Bytes::from(vec![0u8; length])));
        };

        if extent_mapping.replica_locations.is_empty() {
            let length = request.length_bytes as usize;
            return Ok(Some(Bytes::from(vec![0u8; length])));
        }

        let offset_within_extent =
            request.offset_bytes - (_block_start * block_size);
        let length = request.length_bytes as u64;

        self.read_from_replicas(&extent_mapping, offset_within_extent, length)
            .await
    }

    async fn read_single_block_data(
        &self,
        block_num: u64,
        block_size: u64,
    ) -> Result<Vec<u8>, Error> {
        let block_range = block_num..block_num + 1;
        let mapping = self
            .metadata_cache
            .find_extent_for_range(&self.volume_config.volume_id, &block_range);

        let Some((_block_start, extent_mapping)) = mapping else {
            return Ok(vec![0u8; block_size as usize]);
        };

        if extent_mapping.replica_locations.is_empty() {
            return Ok(vec![0u8; block_size as usize]);
        }

        let offset_within_extent = (block_num - _block_start) * block_size;
        let length = block_size;

        match self
            .read_from_replicas(&extent_mapping, offset_within_extent, length)
            .await
        {
            Ok(Some(data)) => Ok(data.to_vec()),
            Ok(None) => Ok(vec![0u8; block_size as usize]),
            Err(e) => Err(e),
        }
    }

    async fn read_from_replicas(
        &self,
        extent_mapping: &crate::metadata_cache::CachedExtentMapping,
        offset: u64,
        length: u64,
    ) -> Result<Option<Bytes>, Error> {

        let mut last_error = None;
        for &node_id in &extent_mapping.replica_locations {
            let node_addr = self.metadata_cache.get_node(&node_id);
            if node_addr.is_none() {
                continue;
            }

            match self
                .data_client
                .read_extent(
                    node_id,
                    self.volume_config.volume_id,
                    extent_mapping.extent_id,
                    extent_mapping.extent_version,
                    offset,
                    length,
                )
                .await
            {
                Ok(result) => {
                    return Ok(Some(result.data));
                }
                Err(e) => {
                    tracing::warn!(
                        node_id = %node_id,
                        extent_id = %extent_mapping.extent_id,
                        error = %e,
                        "read failed, trying next replica"
                    );
                    last_error = Some(e);
                }
            }
        }

        match last_error {
            Some(e) => Err(Error::Storage(format!("all replicas failed: {e}"))),
            None => Err(Error::Storage("no reachable replicas".into())),
        }
    }

    async fn handle_flush(&self) -> Result<Option<Bytes>, Error> {
        tracing::debug!(
            volume_id = %self.volume_config.volume_id,
            "flush — all prior writes already committed before ack"
        );
        Ok(None)
    }

    async fn handle_discard(&self, _request: IoRequest) -> Result<Option<Bytes>, Error> {
        tracing::debug!(
            volume_id = %self.volume_config.volume_id,
            "discard — no-op"
        );
        Ok(None)
    }
}

impl<D: DataNodeClient + DataNodeReader, M: MetadataClient> BlockHandler
    for ClusterBlockHandler<D, M>
{
    async fn handle_io(&self, request: IoRequest) -> Result<Option<Bytes>, Error> {
        match request.operation {
            IoOperation::Write => self.handle_write(request).await,
            IoOperation::Read => self.handle_read(request).await,
            IoOperation::Flush => self.handle_flush().await,
            IoOperation::Discard => self.handle_discard(request).await,
        }
    }
}

impl<D: DataNodeClient + DataNodeReader, M: MetadataClient> std::fmt::Debug
    for ClusterBlockHandler<D, M>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterBlockHandler")
            .field("volume_id", &self.volume_config.volume_id)
            .field("protection", &self.volume_config.protection)
            .field("size_bytes", &self.volume_config.size_bytes)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_cache::{CachedExtentMapping, CachedVolumeInfo};
    use crate::traits::{CommitRequest, CommittedMapping, WriteAck, WriteAckError};
    use blockyard_client::error::ReadError;
    use blockyard_client::types::DataNodeReadResult;
    use blockyard_common::{EpochId, ExtentId, LeaseResponse, NodeId, OperationId, SessionId};
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    struct MockDataNode {
        should_succeed: AtomicBool,
        write_count: AtomicUsize,
    }

    impl MockDataNode {
        fn new(succeed: bool) -> Self {
            Self {
                should_succeed: AtomicBool::new(succeed),
                write_count: AtomicUsize::new(0),
            }
        }
    }

    impl DataNodeClient for MockDataNode {
        async fn write_extent(
            &self,
            node_id: NodeId,
            _operation_id: OperationId,
            _session_id: SessionId,
            _volume_id: VolumeId,
            _extent_id: ExtentId,
            _extent_version: u64,
            _epoch: EpochId,
            _data: Bytes,
            checksum: String,
        ) -> Result<WriteAck, Error> {
            self.write_count.fetch_add(1, Ordering::Relaxed);
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

    impl DataNodeReader for MockDataNode {
        async fn read_extent(
            &self,
            _node_id: NodeId,
            _volume_id: VolumeId,
            extent_id: ExtentId,
            extent_version: u64,
            _offset: u64,
            length: u64,
        ) -> Result<DataNodeReadResult, ReadError> {
            if self.should_succeed.load(Ordering::Relaxed) {
                let data = vec![0xAB; length as usize];
                let checksum = blockyard_common::checksum::compute_checksum(&data);
                Ok(DataNodeReadResult {
                    extent_id,
                    extent_version,
                    checksum,
                    data: Bytes::from(data),
                })
            } else {
                Err(ReadError::DataNodeReadFailed {
                    node_id: _node_id,
                    reason: "mock read failure".into(),
                })
            }
        }
    }

    struct MockMetadata {
        should_succeed: AtomicBool,
        commit_epoch: EpochId,
        grant_lease: AtomicBool,
    }

    impl MockMetadata {
        fn new(succeed: bool, epoch: EpochId) -> Self {
            Self {
                should_succeed: AtomicBool::new(succeed),
                commit_epoch: epoch,
                grant_lease: AtomicBool::new(true),
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

        fn commit_extent_mappings_batch(
            &self,
            requests: Vec<CommitRequest>,
        ) -> impl std::future::Future<Output = Result<EpochId, Error>> + Send {
            let epoch = self.commit_epoch;
            async move {
                let _ = requests;
                Ok(epoch)
            }
        }

        async fn acquire_lease(
            &self,
            _volume_id: VolumeId,
            _session_id: SessionId,
            now_ms: u64,
            ttl_ms: u64,
        ) -> Result<LeaseResponse, Error> {
            if self.grant_lease.load(Ordering::Relaxed) {
                Ok(LeaseResponse::Granted {
                    lease_version: 1,
                    expires_at_ms: now_ms + ttl_ms,
                })
            } else {
                Ok(LeaseResponse::Denied {
                    reason: "mock deny".into(),
                })
            }
        }

        async fn renew_lease(
            &self,
            _volume_id: VolumeId,
            _session_id: SessionId,
            now_ms: u64,
            ttl_ms: u64,
        ) -> Result<LeaseResponse, Error> {
            Ok(LeaseResponse::Renewed {
                lease_version: 2,
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

    fn setup_handler(
        data_succeed: bool,
        meta_succeed: bool,
        protection: ProtectionPolicy,
        num_nodes: usize,
    ) -> (
        ClusterBlockHandler<MockDataNode, MockMetadata>,
        Arc<MetadataCache>,
        VolumeId,
    ) {
        let vid = VolumeId::generate();
        let sid = SessionId::generate();

        let cache = Arc::new(MetadataCache::new());
        cache.set_epoch(EpochId::new(1));
        for i in 0..num_nodes {
            let nid = NodeId::generate();
            cache.set_node(nid, addr(&format!("10.0.0.{}:9800", i + 1)));
        }
        cache.set_volume(CachedVolumeInfo {
            volume_id: vid,
            size_bytes: 1024 * 1024,
            block_size: 4096,
            protection,
            extent_mappings: BTreeMap::new(),
        });

        let data_client = Arc::new(MockDataNode::new(data_succeed));
        let metadata_client = Arc::new(MockMetadata::new(meta_succeed, EpochId::new(1)));
        let session = Arc::new(ClientSession::new(vid));
        let watermark = Arc::new(WriteWatermark::new());
        let stale_handler = Arc::new(StaleEpochHandler::new());
        let lease_manager = Arc::new(LeaseManager::new(vid, sid, Duration::from_secs(30)));

        let volume_config = VolumeConfig {
            volume_id: vid,
            size_bytes: 1024 * 1024,
            block_size: 4096,
            protection,
        };

        let handler = ClusterBlockHandler::new(
            volume_config,
            data_client,
            metadata_client,
            lease_manager,
            session,
            Arc::clone(&cache),
            watermark,
            stale_handler,
        );

        (handler, cache, vid)
    }

    async fn activate_lease<D: DataNodeClient + DataNodeReader, M: MetadataClient>(
        handler: &ClusterBlockHandler<D, M>,
    ) {
        handler
            .lease_manager
            .acquire(handler.metadata_client.as_ref())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_write_replicated_success() {
        let (handler, _cache, _vid) =
            setup_handler(true, true, ProtectionPolicy::Replicated { replicas: 3 }, 3);
        activate_lease(&handler).await;

        let req = IoRequest {
            operation: IoOperation::Write,
            offset_bytes: 0,
            length_bytes: 4096,
            data: Some(Bytes::from(vec![0xAA; 4096])),
            tag: 1,
        };
        let result = handler.handle_io(req).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_write_ec_success() {
        let (handler, _cache, _vid) = setup_handler(
            true,
            true,
            ProtectionPolicy::ErasureCoded {
                data_chunks: 4,
                parity_chunks: 2,
            },
            6,
        );
        activate_lease(&handler).await;

        let req = IoRequest {
            operation: IoOperation::Write,
            offset_bytes: 0,
            length_bytes: 4096,
            data: Some(Bytes::from(vec![0xBB; 4096])),
            tag: 2,
        };
        let result = handler.handle_io(req).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_write_no_lease_fails() {
        let (handler, _cache, _vid) =
            setup_handler(true, true, ProtectionPolicy::Replicated { replicas: 3 }, 3);

        let req = IoRequest {
            operation: IoOperation::Write,
            offset_bytes: 0,
            length_bytes: 4096,
            data: Some(Bytes::from(vec![0xCC; 4096])),
            tag: 3,
        };
        let result = handler.handle_io(req).await;
        assert!(result.is_err());
        let err_str = result.unwrap_err().to_string();
        assert!(
            err_str.contains("lease") || err_str.contains("auth"),
            "unexpected error: {err_str}"
        );
    }

    #[tokio::test]
    async fn test_write_missing_data_fails() {
        let (handler, _cache, _vid) =
            setup_handler(true, true, ProtectionPolicy::Replicated { replicas: 3 }, 3);
        activate_lease(&handler).await;

        let req = IoRequest {
            operation: IoOperation::Write,
            offset_bytes: 0,
            length_bytes: 4096,
            data: None,
            tag: 4,
        };
        let result = handler.handle_io(req).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing data"));
    }

    #[tokio::test]
    async fn test_write_insufficient_acks() {
        let (handler, _cache, _vid) =
            setup_handler(false, true, ProtectionPolicy::Replicated { replicas: 3 }, 3);
        activate_lease(&handler).await;

        let req = IoRequest {
            operation: IoOperation::Write,
            offset_bytes: 0,
            length_bytes: 4096,
            data: Some(Bytes::from(vec![0xDD; 4096])),
            tag: 5,
        };
        let result = handler.handle_io(req).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("insufficient"));
    }

    #[tokio::test]
    async fn test_write_metadata_commit_failure() {
        let (handler, _cache, _vid) =
            setup_handler(true, false, ProtectionPolicy::Replicated { replicas: 3 }, 3);
        activate_lease(&handler).await;

        let req = IoRequest {
            operation: IoOperation::Write,
            offset_bytes: 0,
            length_bytes: 4096,
            data: Some(Bytes::from(vec![0xEE; 4096])),
            tag: 6,
        };
        let result = handler.handle_io(req).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("metadata commit failed")
        );
    }

    #[tokio::test]
    async fn test_flush_succeeds() {
        let (handler, _cache, _vid) =
            setup_handler(true, true, ProtectionPolicy::Replicated { replicas: 3 }, 3);

        let req = IoRequest {
            operation: IoOperation::Flush,
            offset_bytes: 0,
            length_bytes: 0,
            data: None,
            tag: 7,
        };
        let result = handler.handle_io(req).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_discard_succeeds() {
        let (handler, _cache, _vid) =
            setup_handler(true, true, ProtectionPolicy::Replicated { replicas: 3 }, 3);

        let req = IoRequest {
            operation: IoOperation::Discard,
            offset_bytes: 4096,
            length_bytes: 4096,
            data: None,
            tag: 8,
        };
        let result = handler.handle_io(req).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_read_returns_data() {
        let (handler, cache, vid) =
            setup_handler(true, true, ProtectionPolicy::Replicated { replicas: 3 }, 3);

        let eid = ExtentId::generate();
        let nodes: Vec<NodeId> = cache.list_nodes().iter().map(|n| n.node_id).collect();
        cache.set_extent_mapping(
            &vid,
            0,
            CachedExtentMapping {
                extent_id: eid,
                extent_version: 1,
                replica_locations: nodes,
                checksums: vec![vec![0xFF]],
                size_bytes: 4096,
            },
        );

        let req = IoRequest {
            operation: IoOperation::Read,
            offset_bytes: 0,
            length_bytes: 4096,
            data: None,
            tag: 9,
        };
        let result = handler.handle_io(req).await;
        assert!(result.is_ok());
        let data = result.unwrap();
        assert!(data.is_some());
        assert_eq!(data.unwrap().len(), 4096);
    }

    #[tokio::test]
    async fn test_read_no_extent_mapping() {
        let (handler, _cache, _vid) =
            setup_handler(true, true, ProtectionPolicy::Replicated { replicas: 3 }, 3);

        let req = IoRequest {
            operation: IoOperation::Read,
            offset_bytes: 999999,
            length_bytes: 4096,
            data: None,
            tag: 10,
        };
        let result = handler.handle_io(req).await;
        // Unmapped reads return zeros (sparse volume behavior).
        let data = result.unwrap().unwrap();
        assert_eq!(data.len(), 4096);
        assert!(data.iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn test_write_single_replica() {
        let (handler, _cache, _vid) =
            setup_handler(true, true, ProtectionPolicy::Replicated { replicas: 1 }, 1);
        activate_lease(&handler).await;

        let req = IoRequest {
            operation: IoOperation::Write,
            offset_bytes: 0,
            length_bytes: 512,
            data: Some(Bytes::from(vec![0x11; 512])),
            tag: 11,
        };
        let result = handler.handle_io(req).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handler_debug() {
        let (handler, _cache, _vid) =
            setup_handler(true, true, ProtectionPolicy::Replicated { replicas: 3 }, 3);
        let debug = format!("{:?}", handler);
        assert!(debug.contains("ClusterBlockHandler"));
        assert!(debug.contains("Replicated"));
    }

    #[tokio::test]
    async fn test_volume_config_accessor() {
        let (handler, _cache, vid) =
            setup_handler(true, true, ProtectionPolicy::Replicated { replicas: 3 }, 3);
        assert_eq!(handler.volume_config().volume_id, vid);
        assert_eq!(handler.volume_config().size_bytes, 1024 * 1024);
        assert_eq!(handler.volume_config().block_size, 4096);
    }

    #[tokio::test]
    async fn test_session_accessor() {
        let (handler, _cache, vid) =
            setup_handler(true, true, ProtectionPolicy::Replicated { replicas: 3 }, 3);
        assert_eq!(handler.session().volume_id(), vid);
    }

    #[tokio::test]
    async fn test_lease_manager_accessor() {
        let (handler, _cache, vid) =
            setup_handler(true, true, ProtectionPolicy::Replicated { replicas: 3 }, 3);
        assert_eq!(handler.lease_manager().volume_id(), vid);
    }

    #[tokio::test]
    async fn test_watermark_accessor() {
        let (handler, _cache, _vid) =
            setup_handler(true, true, ProtectionPolicy::Replicated { replicas: 3 }, 3);
        assert_eq!(handler.watermark().current(), EpochId::new(0));
    }

    #[tokio::test]
    async fn test_write_advances_watermark() {
        let (handler, _cache, _vid) =
            setup_handler(true, true, ProtectionPolicy::Replicated { replicas: 3 }, 3);
        activate_lease(&handler).await;

        assert_eq!(handler.watermark().current(), EpochId::new(0));

        let req = IoRequest {
            operation: IoOperation::Write,
            offset_bytes: 0,
            length_bytes: 4096,
            data: Some(Bytes::from(vec![0x55; 4096])),
            tag: 12,
        };
        handler.handle_io(req).await.unwrap();
        assert!(handler.watermark().current().as_u64() >= 1);
    }

    #[tokio::test]
    async fn test_multiple_writes_succeed() {
        let (handler, _cache, _vid) =
            setup_handler(true, true, ProtectionPolicy::Replicated { replicas: 3 }, 3);
        activate_lease(&handler).await;

        for i in 0..5 {
            let req = IoRequest {
                operation: IoOperation::Write,
                offset_bytes: i * 4096,
                length_bytes: 4096,
                data: Some(Bytes::from(vec![i as u8; 4096])),
                tag: i,
            };
            handler.handle_io(req).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_ec_write_insufficient_nodes_fails() {
        let (handler, _cache, _vid) = setup_handler(
            true,
            true,
            ProtectionPolicy::ErasureCoded {
                data_chunks: 4,
                parity_chunks: 2,
            },
            3,
        );
        activate_lease(&handler).await;

        let req = IoRequest {
            operation: IoOperation::Write,
            offset_bytes: 0,
            length_bytes: 4096,
            data: Some(Bytes::from(vec![0xFF; 4096])),
            tag: 13,
        };
        let result = handler.handle_io(req).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("insufficient"));
    }

    #[test]
    fn test_volume_config_debug() {
        let config = VolumeConfig {
            volume_id: VolumeId::generate(),
            size_bytes: 1024 * 1024,
            block_size: 4096,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
        };
        let debug = format!("{:?}", config);
        assert!(debug.contains("VolumeConfig"));
    }

    #[test]
    fn test_volume_config_clone() {
        let config = VolumeConfig {
            volume_id: VolumeId::generate(),
            size_bytes: 2048,
            block_size: 512,
            protection: ProtectionPolicy::ErasureCoded {
                data_chunks: 4,
                parity_chunks: 2,
            },
        };
        let cloned = config.clone();
        assert_eq!(cloned.size_bytes, 2048);
        assert_eq!(cloned.block_size, 512);
    }

    #[tokio::test]
    async fn test_read_with_no_replica_locations() {
        let (handler, cache, vid) =
            setup_handler(true, true, ProtectionPolicy::Replicated { replicas: 3 }, 3);

        let eid = ExtentId::generate();
        cache.set_extent_mapping(
            &vid,
            0,
            CachedExtentMapping {
                extent_id: eid,
                extent_version: 1,
                replica_locations: vec![],
                checksums: vec![],
                size_bytes: 4096,
            },
        );

        let req = IoRequest {
            operation: IoOperation::Read,
            offset_bytes: 0,
            length_bytes: 4096,
            data: None,
            tag: 14,
        };
        let result = handler.handle_io(req).await;
        // Empty replica locations now return zeros (sparse behavior).
        let data = result.unwrap().unwrap();
        assert_eq!(data.len(), 4096);
        assert!(data.iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn test_flush_does_not_require_lease() {
        let (handler, _cache, _vid) =
            setup_handler(true, true, ProtectionPolicy::Replicated { replicas: 3 }, 3);

        let req = IoRequest {
            operation: IoOperation::Flush,
            offset_bytes: 0,
            length_bytes: 0,
            data: None,
            tag: 15,
        };
        let result = handler.handle_io(req).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_discard_does_not_require_lease() {
        let (handler, _cache, _vid) =
            setup_handler(true, true, ProtectionPolicy::Replicated { replicas: 3 }, 3);

        let req = IoRequest {
            operation: IoOperation::Discard,
            offset_bytes: 0,
            length_bytes: 4096,
            data: None,
            tag: 16,
        };
        let result = handler.handle_io(req).await;
        assert!(result.is_ok());
    }
}
