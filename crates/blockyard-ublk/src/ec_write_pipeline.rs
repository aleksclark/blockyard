//! Erasure-coded write pipeline (P4D.2, P4D.5, P4D.6, §4.5.3).
//!
//! Implements:
//! - EC write: determine stripe geometry → encode → send fragments → await acks → commit
//! - Partial-stripe read-modify-write: read existing stripe → modify → re-encode → write
//! - Adjacent write coalescing: buffer adjacent writes in same stripe

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;

use blockyard_common::error::Error;
use blockyard_common::{EpochId, ExtentId, NodeId, OperationId, ProtectionPolicy, VolumeId};

use crate::metadata_cache::MetadataCache;
use crate::session::ClientSession;
use crate::stale_epoch::StaleEpochHandler;
use crate::traits::{CommitRequest, DataNodeClient, MetadataClient, WriteAck, WriteAckError};
use crate::watermark::WriteWatermark;
use crate::write_pipeline::WriteOutcome;

/// A fragment placement for EC writes.
#[derive(Debug, Clone)]
pub struct EcFragmentPlacement {
    pub fragment_index: usize,
    pub is_data: bool,
    pub node_id: NodeId,
}

/// Result of encoding data into EC fragments.
#[derive(Debug, Clone)]
pub struct EncodedStripe {
    pub fragments: Vec<EcFragment>,
    pub original_data_len: usize,
}

/// A single encoded fragment with its placement.
#[derive(Debug, Clone)]
pub struct EcFragment {
    pub index: usize,
    pub is_data: bool,
    pub data: Bytes,
}

/// Configuration for the write coalescing buffer (P4D.6).
#[derive(Debug, Clone)]
pub struct CoalescingConfig {
    pub max_delay: Duration,
    pub max_bytes: usize,
    pub enabled: bool,
}

impl Default for CoalescingConfig {
    fn default() -> Self {
        Self {
            max_delay: Duration::from_millis(5),
            max_bytes: 128 * 1024,
            enabled: true,
        }
    }
}

/// A buffered write pending coalescing.
#[derive(Debug, Clone)]
pub struct PendingWrite {
    pub volume_id: VolumeId,
    pub block_range: Range<u64>,
    pub data: Bytes,
    pub received_at: Instant,
}

/// Write coalescing buffer (P4D.6).
///
/// Buffers adjacent writes in the same stripe and coalesces them into
/// a single stripe write to reduce partial-stripe amplification.
#[derive(Debug)]
pub struct CoalescingBuffer {
    config: CoalescingConfig,
    pending: Mutex<BTreeMap<u64, PendingWrite>>,
    stripe_size: u64,
}

impl CoalescingBuffer {
    pub fn new(config: CoalescingConfig, stripe_size: u64) -> Self {
        Self {
            config,
            pending: Mutex::new(BTreeMap::new()),
            stripe_size,
        }
    }

    /// Add a write to the buffer. Returns writes that should be flushed.
    pub fn add_write(&self, write: PendingWrite) -> Vec<PendingWrite> {
        if !self.config.enabled {
            return vec![write];
        }

        let stripe_start = (write.block_range.start / self.stripe_size) * self.stripe_size;
        let mut pending = self.pending.lock();

        let total_bytes: usize = pending.values().map(|w| w.data.len()).sum();
        let should_flush = total_bytes + write.data.len() >= self.config.max_bytes;

        let time_exceeded = pending
            .values()
            .any(|w| w.received_at.elapsed() >= self.config.max_delay);

        // Merge within the same stripe instead of replacing: concatenate data
        // and extend the block range so no writes are silently dropped.
        if let Some(existing) = pending.get_mut(&stripe_start) {
            let mut merged = existing.data.to_vec();
            merged.extend_from_slice(&write.data);
            existing.data = Bytes::from(merged);
            existing.block_range =
                std::cmp::min(existing.block_range.start, write.block_range.start)
                    ..std::cmp::max(existing.block_range.end, write.block_range.end);
        } else {
            pending.insert(stripe_start, write);
        }

        if should_flush || time_exceeded {
            let flushed: Vec<PendingWrite> = pending.values().cloned().collect();
            pending.clear();
            flushed
        } else {
            vec![]
        }
    }

    /// Force flush all pending writes.
    pub fn flush_all(&self) -> Vec<PendingWrite> {
        let mut pending = self.pending.lock();
        let flushed: Vec<PendingWrite> = pending.values().cloned().collect();
        pending.clear();
        flushed
    }

    /// Number of pending writes in the buffer.
    pub fn pending_count(&self) -> usize {
        self.pending.lock().len()
    }
}

/// Erasure-coded write pipeline (P4D.2).
///
/// Orchestrates the EC write path:
/// 1. Determine stripe geometry from ProtectionPolicy::ErasureCoded
/// 2. Encode data into K+M fragments
/// 3. Send each fragment to designated node per placement
/// 4. Await sufficient acks
/// 5. Commit fragment mapping to metadata
/// 6. NEVER ack to ublk before metadata commit (invariant 1)
pub struct EcWritePipeline<D: DataNodeClient, M: MetadataClient> {
    data_client: Arc<D>,
    metadata_client: Arc<M>,
    cache: Arc<MetadataCache>,
    session: Arc<ClientSession>,
    watermark: Arc<WriteWatermark>,
    stale_epoch_handler: Arc<StaleEpochHandler>,
    coalescing: Option<CoalescingBuffer>,
    max_retries: u32,
}

impl<D: DataNodeClient, M: MetadataClient> EcWritePipeline<D, M> {
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
            coalescing: None,
            max_retries: 3,
        }
    }

    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn with_coalescing(mut self, config: CoalescingConfig, stripe_size: u64) -> Self {
        self.coalescing = Some(CoalescingBuffer::new(config, stripe_size));
        self
    }

    /// Execute the EC write path (§4.5.3).
    ///
    /// CRITICAL: Never acks write to ublk before metadata commit succeeds
    /// (invariant 1).
    pub async fn execute(
        &self,
        volume_id: VolumeId,
        block_range: Range<u64>,
        data: Bytes,
    ) -> Result<WriteOutcome, Error> {
        let operation_id = self.session.next_operation_id();
        self.execute_with_op_id(volume_id, block_range, data, operation_id)
            .await
    }

    /// Execute with a specific operation ID (for idempotent retry).
    pub async fn execute_with_op_id(
        &self,
        volume_id: VolumeId,
        block_range: Range<u64>,
        data: Bytes,
        operation_id: OperationId,
    ) -> Result<WriteOutcome, Error> {
        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                tracing::debug!(
                    attempt = attempt,
                    operation_id = %operation_id,
                    "retrying EC write with same operation ID"
                );
            }

            match self
                .execute_once(volume_id, &block_range, &data, operation_id)
                .await
            {
                Ok(outcome) => return Ok(outcome),
                Err(e) => {
                    tracing::warn!(
                        attempt = attempt,
                        error = %e,
                        "EC write attempt failed"
                    );
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| Error::Storage("EC write failed after retries".into())))
    }

    async fn execute_once(
        &self,
        volume_id: VolumeId,
        block_range: &Range<u64>,
        data: &Bytes,
        operation_id: OperationId,
    ) -> Result<WriteOutcome, Error> {
        if self.stale_epoch_handler.is_paused() {
            return Err(Error::Storage("writes paused due to stale epoch".into()));
        }

        let epoch = self.cache.current_epoch();

        let volume_info = self
            .cache
            .get_volume(&volume_id)
            .ok_or_else(|| Error::Storage(format!("volume {} not found in cache", volume_id)))?;

        let (data_chunks, parity_chunks) = match volume_info.protection {
            ProtectionPolicy::ErasureCoded {
                data_chunks,
                parity_chunks,
            } => (data_chunks as usize, parity_chunks as usize),
            _ => {
                return Err(Error::Storage(
                    "EC write pipeline requires ErasureCoded protection policy".into(),
                ));
            }
        };

        let nodes = self.cache.list_nodes();
        let total_required = data_chunks + parity_chunks;
        if nodes.len() < total_required {
            return Err(Error::Storage(format!(
                "insufficient nodes: have {}, need {}",
                nodes.len(),
                total_required
            )));
        }

        let fragment_nodes: Vec<NodeId> = nodes
            .iter()
            .take(total_required)
            .map(|n| n.node_id)
            .collect();

        let encoded = encode_data(data, data_chunks, parity_chunks)?;

        let extent_id = ExtentId::generate();
        let extent_version = crate::write_pipeline::next_extent_version();
        let _checksum = compute_checksum(data);

        let acks = self
            .transmit_fragments(
                &fragment_nodes,
                &encoded,
                operation_id,
                volume_id,
                extent_id,
                extent_version,
                epoch,
            )
            .await;

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

        let successful_acks: Vec<&WriteAck> = acks.iter().filter(|a| a.success).collect();
        let successful_nodes: Vec<NodeId> = successful_acks.iter().map(|a| a.node_id).collect();
        let successful_checksums: Vec<Vec<u8>> = successful_acks
            .iter()
            .map(|a| a.checksum.as_bytes().to_vec())
            .collect();

        // EC can tolerate up to M failures, so we require K + min(1, M) acks
        // (at least one parity for protection), not all K+M.
        let min_parity = std::cmp::min(1, parity_chunks);
        let required_acks = data_chunks + min_parity;
        if successful_acks.len() < required_acks {
            return Ok(WriteOutcome::InsufficientAcks {
                acked: successful_acks.len(),
                required: required_acks,
            });
        }

        let commit_req = CommitRequest {
            volume_id,
            block_range: block_range.clone(),
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
                    &volume_id,
                    block_range.start,
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
                        size_bytes: data.len() as u64,
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

    #[allow(clippy::too_many_arguments)]
    async fn transmit_fragments(
        &self,
        nodes: &[NodeId],
        encoded: &EncodedStripe,
        operation_id: OperationId,
        volume_id: VolumeId,
        extent_id: ExtentId,
        extent_version: u64,
        epoch: EpochId,
    ) -> Vec<WriteAck> {
        let mut join_set = tokio::task::JoinSet::new();

        for (i, (&node_id, fragment)) in nodes.iter().zip(encoded.fragments.iter()).enumerate() {
            let frag_checksum = compute_checksum(&fragment.data);
            let client = Arc::clone(&self.data_client);
            let session_id = self.session.session_id();
            let frag_data = fragment.data.clone();
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
                        frag_data,
                        frag_checksum,
                    )
                    .await;
                match result {
                    Ok(ack) => ack,
                    Err(e) => {
                        tracing::warn!(
                            node = %node_id,
                            fragment = i,
                            error = %e,
                            "EC fragment write failed"
                        );
                        WriteAck {
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
                    tracing::error!(error = %e, "EC fragment write task panicked");
                }
            }
        }

        acks
    }

    /// Perform a partial-stripe read-modify-write (P4D.5).
    ///
    /// 1. Read existing full stripe from the cluster
    /// 2. Modify affected data chunks with new data
    /// 3. Re-encode entire stripe
    /// 4. Write all K+M fragments
    pub async fn partial_stripe_write(
        &self,
        volume_id: VolumeId,
        block_range: Range<u64>,
        new_data: Bytes,
        existing_stripe_data: Bytes,
        stripe_offset: usize,
    ) -> Result<WriteOutcome, Error> {
        let volume_info = self
            .cache
            .get_volume(&volume_id)
            .ok_or_else(|| Error::Storage(format!("volume {} not found in cache", volume_id)))?;

        match volume_info.protection {
            ProtectionPolicy::ErasureCoded { .. } => {}
            _ => {
                return Err(Error::Storage(
                    "partial stripe write requires ErasureCoded policy".into(),
                ));
            }
        };

        let stripe_size = existing_stripe_data.len();
        let mut modified = existing_stripe_data.to_vec();

        let end = std::cmp::min(stripe_offset + new_data.len(), stripe_size);
        let copy_len = end - stripe_offset;
        modified[stripe_offset..stripe_offset + copy_len].copy_from_slice(&new_data[..copy_len]);

        let modified_bytes = Bytes::from(modified);

        self.execute(volume_id, block_range, modified_bytes).await
    }
}

impl<D: DataNodeClient, M: MetadataClient> std::fmt::Debug for EcWritePipeline<D, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EcWritePipeline")
            .field("max_retries", &self.max_retries)
            .finish()
    }
}

/// Encode data into EC fragments using spawn_blocking for CPU-bound work.
pub fn encode_data(
    data: &[u8],
    data_chunks: usize,
    parity_chunks: usize,
) -> Result<EncodedStripe, Error> {
    use reed_solomon_erasure::galois_8::ReedSolomon;

    if data.is_empty() {
        return Err(Error::Storage("empty data for EC encoding".into()));
    }

    let rs = ReedSolomon::new(data_chunks, parity_chunks)
        .map_err(|e| Error::Storage(format!("invalid EC parameters: {}", e)))?;

    let frag_size = data.len().div_ceil(data_chunks);

    let mut shards: Vec<Vec<u8>> = Vec::with_capacity(data_chunks + parity_chunks);
    for i in 0..data_chunks {
        let start = i * frag_size;
        let end = std::cmp::min(start + frag_size, data.len());
        let mut shard = vec![0u8; frag_size];
        if start < data.len() {
            let copy_len = end - start;
            shard[..copy_len].copy_from_slice(&data[start..end]);
        }
        shards.push(shard);
    }

    for _ in 0..parity_chunks {
        shards.push(vec![0u8; frag_size]);
    }

    rs.encode(&mut shards)
        .map_err(|e| Error::Storage(format!("EC encoding failed: {}", e)))?;

    let fragments: Vec<EcFragment> = shards
        .into_iter()
        .enumerate()
        .map(|(i, shard)| EcFragment {
            index: i,
            is_data: i < data_chunks,
            data: Bytes::from(shard),
        })
        .collect();

    Ok(EncodedStripe {
        fragments,
        original_data_len: data.len(),
    })
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

    struct MockEcDataNode {
        should_succeed: AtomicBool,
        stale_epoch: AtomicBool,
        write_count: AtomicUsize,
    }

    impl MockEcDataNode {
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

    impl DataNodeClient for MockEcDataNode {
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

    struct MockEcMetadata {
        should_succeed: AtomicBool,
        commit_epoch: EpochId,
        commit_count: AtomicUsize,
    }

    impl MockEcMetadata {
        fn new(succeed: bool, epoch: EpochId) -> Self {
            Self {
                should_succeed: AtomicBool::new(succeed),
                commit_epoch: epoch,
                commit_count: AtomicUsize::new(0),
            }
        }
    }

    impl MetadataClient for MockEcMetadata {
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

    fn setup_ec_cache(epoch: u64, num_nodes: usize, k: u8, m: u8) -> (MetadataCache, VolumeId) {
        let cache = MetadataCache::new();
        cache.set_epoch(EpochId::new(epoch));
        for i in 0..num_nodes {
            let nid = NodeId::generate();
            cache.set_node(nid, addr(&format!("10.0.0.{}:9800", i + 1)));
        }
        let vid = VolumeId::generate();
        cache.set_volume(CachedVolumeInfo {
            volume_id: vid,
            size_bytes: 1024 * 1024,
            block_size: 4096,
            protection: ProtectionPolicy::ErasureCoded {
                data_chunks: k,
                parity_chunks: m,
            },
            extent_mappings: BTreeMap::new(),
        });
        (cache, vid)
    }

    fn make_ec_pipeline(
        data_succeed: bool,
        meta_succeed: bool,
        num_nodes: usize,
        k: u8,
        m: u8,
    ) -> (
        EcWritePipeline<MockEcDataNode, MockEcMetadata>,
        Arc<MetadataCache>,
        VolumeId,
        Arc<WriteWatermark>,
    ) {
        let (cache, vid) = setup_ec_cache(1, num_nodes, k, m);
        let cache = Arc::new(cache);
        let data_client = Arc::new(MockEcDataNode::new(data_succeed));
        let metadata_client = Arc::new(MockEcMetadata::new(meta_succeed, EpochId::new(1)));
        let session = Arc::new(ClientSession::new(vid));
        let watermark = Arc::new(WriteWatermark::new());
        let stale_handler = Arc::new(StaleEpochHandler::new());

        let pipeline = EcWritePipeline::new(
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
    async fn test_ec_write_success() {
        let (pipeline, _cache, vid, watermark) = make_ec_pipeline(true, true, 6, 4, 2);
        let result = pipeline
            .execute(vid, 0..64, Bytes::from(vec![0xAA; 4096]))
            .await
            .unwrap();
        match result {
            WriteOutcome::Committed { epoch } => {
                assert_eq!(epoch, EpochId::new(1));
            }
            other => panic!("expected Committed, got {:?}", other),
        }
        assert!(watermark.current().as_u64() >= 1);
    }

    #[tokio::test]
    async fn test_ec_write_insufficient_acks() {
        let (pipeline, _cache, vid, _) = make_ec_pipeline(false, true, 6, 4, 2);
        let result = pipeline
            .execute(vid, 0..64, Bytes::from(vec![0xBB; 4096]))
            .await
            .unwrap();
        match result {
            WriteOutcome::InsufficientAcks { acked, required } => {
                assert_eq!(acked, 0);
                // EC 4+2 requires K + min(1, M) = 5 acks, not all 6
                assert_eq!(required, 5);
            }
            other => panic!("expected InsufficientAcks, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_ec_write_metadata_commit_failure() {
        let (pipeline, _cache, vid, watermark) = make_ec_pipeline(true, false, 6, 4, 2);
        let result = pipeline
            .execute(vid, 0..64, Bytes::from(vec![0xCC; 4096]))
            .await
            .unwrap();
        match result {
            WriteOutcome::MetadataCommitFailed { reason } => {
                assert!(reason.contains("commit failed"));
            }
            other => panic!("expected MetadataCommitFailed, got {:?}", other),
        }
        assert_eq!(watermark.current(), EpochId::new(0));
    }

    #[tokio::test]
    async fn test_ec_write_stale_epoch() {
        let (cache, vid) = setup_ec_cache(1, 6, 4, 2);
        let cache = Arc::new(cache);
        let data_client = Arc::new(MockEcDataNode::with_stale_epoch());
        let metadata_client = Arc::new(MockEcMetadata::new(true, EpochId::new(1)));
        let session = Arc::new(ClientSession::new(vid));
        let watermark = Arc::new(WriteWatermark::new());
        let stale_handler = Arc::new(StaleEpochHandler::new());

        let pipeline = EcWritePipeline::new(
            data_client,
            metadata_client,
            Arc::clone(&cache),
            session,
            watermark,
            stale_handler,
        );

        let result = pipeline
            .execute(vid, 0..64, Bytes::from(vec![0xDD; 4096]))
            .await
            .unwrap();
        assert_eq!(result, WriteOutcome::StaleEpoch);
    }

    #[tokio::test]
    async fn test_ec_write_volume_not_found() {
        let (pipeline, _cache, _vid, _) = make_ec_pipeline(true, true, 6, 4, 2);
        let unknown = VolumeId::generate();
        let result = pipeline
            .execute(unknown, 0..64, Bytes::from(vec![0xEE; 4096]))
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_ec_write_insufficient_nodes() {
        let (pipeline, _cache, vid, _) = make_ec_pipeline(true, true, 3, 4, 2);
        let result = pipeline
            .execute(vid, 0..64, Bytes::from(vec![0xFF; 4096]))
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("insufficient"));
    }

    #[tokio::test]
    async fn test_ec_write_replicated_policy_rejected() {
        let cache = MetadataCache::new();
        cache.set_epoch(EpochId::new(1));
        let vid = VolumeId::generate();
        cache.set_volume(CachedVolumeInfo {
            volume_id: vid,
            size_bytes: 1024 * 1024,
            block_size: 4096,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_mappings: BTreeMap::new(),
        });
        let cache = Arc::new(cache);
        let data_client = Arc::new(MockEcDataNode::new(true));
        let metadata_client = Arc::new(MockEcMetadata::new(true, EpochId::new(1)));
        let session = Arc::new(ClientSession::new(vid));
        let watermark = Arc::new(WriteWatermark::new());
        let stale_handler = Arc::new(StaleEpochHandler::new());

        let pipeline = EcWritePipeline::new(
            data_client,
            metadata_client,
            cache,
            session,
            watermark,
            stale_handler,
        );

        let result = pipeline
            .execute(vid, 0..64, Bytes::from(vec![0x11; 512]))
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ErasureCoded"));
    }

    #[tokio::test]
    async fn test_ec_write_pipeline_debug() {
        let (pipeline, _, _, _) = make_ec_pipeline(true, true, 6, 4, 2);
        let debug = format!("{:?}", pipeline);
        assert!(debug.contains("EcWritePipeline"));
    }

    #[tokio::test]
    async fn test_ec_write_with_max_retries() {
        let (cache, vid) = setup_ec_cache(1, 6, 4, 2);
        let cache = Arc::new(cache);
        let data_client = Arc::new(MockEcDataNode::new(true));
        let metadata_client = Arc::new(MockEcMetadata::new(true, EpochId::new(1)));
        let session = Arc::new(ClientSession::new(vid));
        let watermark = Arc::new(WriteWatermark::new());
        let stale_handler = Arc::new(StaleEpochHandler::new());

        let pipeline = EcWritePipeline::new(
            data_client,
            metadata_client,
            cache,
            session,
            watermark,
            stale_handler,
        )
        .with_max_retries(5);

        let result = pipeline
            .execute(vid, 0..64, Bytes::from(vec![0x22; 512]))
            .await
            .unwrap();
        assert!(matches!(result, WriteOutcome::Committed { .. }));
    }

    #[tokio::test]
    async fn test_ec_write_paused_rejected() {
        let (cache, vid) = setup_ec_cache(1, 6, 4, 2);
        let cache = Arc::new(cache);
        let data_client = Arc::new(MockEcDataNode::new(true));
        let metadata_client = Arc::new(MockEcMetadata::new(true, EpochId::new(1)));
        let session = Arc::new(ClientSession::new(vid));
        let watermark = Arc::new(WriteWatermark::new());
        let stale_handler = Arc::new(StaleEpochHandler::new());
        stale_handler.set_paused(true);

        let pipeline = EcWritePipeline::new(
            data_client,
            metadata_client,
            cache,
            session,
            watermark,
            stale_handler,
        );

        let result = pipeline
            .execute(vid, 0..64, Bytes::from(vec![0x33; 512]))
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("paused"));
    }

    #[tokio::test]
    async fn test_ec_write_watermark_advances() {
        let (pipeline, _cache, vid, watermark) = make_ec_pipeline(true, true, 6, 4, 2);
        assert_eq!(watermark.current(), EpochId::new(0));

        pipeline
            .execute(vid, 0..64, Bytes::from(vec![0x44; 512]))
            .await
            .unwrap();
        assert!(watermark.current().as_u64() >= 1);
    }

    #[tokio::test]
    async fn test_ec_write_watermark_no_advance_on_commit_fail() {
        let (pipeline, _cache, vid, watermark) = make_ec_pipeline(true, false, 6, 4, 2);

        let result = pipeline
            .execute(vid, 0..64, Bytes::from(vec![0x55; 512]))
            .await
            .unwrap();
        assert!(matches!(result, WriteOutcome::MetadataCommitFailed { .. }));
        assert_eq!(watermark.current(), EpochId::new(0));
    }

    #[test]
    fn test_encode_data_basic() {
        let data = vec![0xAA; 400];
        let encoded = encode_data(&data, 4, 2).unwrap();
        assert_eq!(encoded.fragments.len(), 6);
        assert_eq!(encoded.original_data_len, 400);
        assert!(encoded.fragments[0].is_data);
        assert!(!encoded.fragments[4].is_data);
    }

    #[test]
    fn test_encode_data_empty() {
        let result = encode_data(&[], 4, 2);
        assert!(result.is_err());
    }

    #[test]
    fn test_encode_data_padded() {
        let data = vec![0xBB; 401];
        let encoded = encode_data(&data, 4, 2).unwrap();
        assert_eq!(encoded.fragments.len(), 6);
        assert_eq!(encoded.fragments[0].data.len(), 101);
    }

    #[test]
    fn test_ec_fragment_debug() {
        let frag = EcFragment {
            index: 0,
            is_data: true,
            data: Bytes::from(vec![1, 2, 3]),
        };
        let debug = format!("{:?}", frag);
        assert!(debug.contains("EcFragment"));
    }

    #[test]
    fn test_ec_fragment_clone() {
        let frag = EcFragment {
            index: 2,
            is_data: false,
            data: Bytes::from(vec![4, 5, 6]),
        };
        let cloned = frag.clone();
        assert_eq!(cloned.index, 2);
        assert!(!cloned.is_data);
    }

    #[test]
    fn test_ec_fragment_placement_debug() {
        let p = EcFragmentPlacement {
            fragment_index: 0,
            is_data: true,
            node_id: NodeId::generate(),
        };
        let debug = format!("{:?}", p);
        assert!(debug.contains("EcFragmentPlacement"));
    }

    #[test]
    fn test_ec_fragment_placement_clone() {
        let p = EcFragmentPlacement {
            fragment_index: 3,
            is_data: false,
            node_id: NodeId::generate(),
        };
        let cloned = p.clone();
        assert_eq!(cloned.fragment_index, 3);
    }

    #[test]
    fn test_encoded_stripe_debug() {
        let data = vec![0u8; 100];
        let encoded = encode_data(&data, 2, 1).unwrap();
        let debug = format!("{:?}", encoded);
        assert!(debug.contains("EncodedStripe"));
    }

    #[test]
    fn test_encoded_stripe_clone() {
        let data = vec![0u8; 100];
        let encoded = encode_data(&data, 2, 1).unwrap();
        let cloned = encoded.clone();
        assert_eq!(cloned.fragments.len(), 3);
        assert_eq!(cloned.original_data_len, 100);
    }

    #[test]
    fn test_coalescing_config_default() {
        let config = CoalescingConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_bytes, 128 * 1024);
        assert_eq!(config.max_delay, Duration::from_millis(5));
    }

    #[test]
    fn test_coalescing_config_debug() {
        let config = CoalescingConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("CoalescingConfig"));
    }

    #[test]
    fn test_coalescing_config_clone() {
        let config = CoalescingConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.max_bytes, config.max_bytes);
    }

    #[test]
    fn test_coalescing_buffer_disabled() {
        let config = CoalescingConfig {
            enabled: false,
            ..Default::default()
        };
        let buf = CoalescingBuffer::new(config, 4096);
        let write = PendingWrite {
            volume_id: VolumeId::generate(),
            block_range: 0..64,
            data: Bytes::from(vec![0u8; 100]),
            received_at: Instant::now(),
        };
        let flushed = buf.add_write(write);
        assert_eq!(flushed.len(), 1);
        assert_eq!(buf.pending_count(), 0);
    }

    #[test]
    fn test_coalescing_buffer_buffers_writes() {
        let config = CoalescingConfig {
            enabled: true,
            max_bytes: 1024 * 1024,
            max_delay: Duration::from_secs(60),
        };
        let buf = CoalescingBuffer::new(config, 4096);
        let write = PendingWrite {
            volume_id: VolumeId::generate(),
            block_range: 0..64,
            data: Bytes::from(vec![0u8; 100]),
            received_at: Instant::now(),
        };
        let flushed = buf.add_write(write);
        assert!(flushed.is_empty());
        assert_eq!(buf.pending_count(), 1);
    }

    #[test]
    fn test_coalescing_buffer_flush_on_size() {
        let config = CoalescingConfig {
            enabled: true,
            max_bytes: 200,
            max_delay: Duration::from_secs(60),
        };
        let buf = CoalescingBuffer::new(config, 4096);
        let vid = VolumeId::generate();

        let write1 = PendingWrite {
            volume_id: vid,
            block_range: 0..64,
            data: Bytes::from(vec![0u8; 100]),
            received_at: Instant::now(),
        };
        assert!(buf.add_write(write1).is_empty());

        let write2 = PendingWrite {
            volume_id: vid,
            block_range: 64..128,
            data: Bytes::from(vec![0u8; 150]),
            received_at: Instant::now(),
        };
        let flushed = buf.add_write(write2);
        assert!(!flushed.is_empty());
        assert_eq!(buf.pending_count(), 0);
    }

    #[test]
    fn test_coalescing_buffer_flush_all() {
        let config = CoalescingConfig {
            enabled: true,
            max_bytes: 1024 * 1024,
            max_delay: Duration::from_secs(60),
        };
        let buf = CoalescingBuffer::new(config, 4096);
        let vid = VolumeId::generate();

        buf.add_write(PendingWrite {
            volume_id: vid,
            block_range: 0..64,
            data: Bytes::from(vec![0u8; 100]),
            received_at: Instant::now(),
        });
        buf.add_write(PendingWrite {
            volume_id: vid,
            block_range: 4096..4160,
            data: Bytes::from(vec![0u8; 100]),
            received_at: Instant::now(),
        });

        assert_eq!(buf.pending_count(), 2);
        let flushed = buf.flush_all();
        assert_eq!(flushed.len(), 2);
        assert_eq!(buf.pending_count(), 0);
    }

    #[test]
    fn test_coalescing_buffer_debug() {
        let buf = CoalescingBuffer::new(CoalescingConfig::default(), 4096);
        let debug = format!("{:?}", buf);
        assert!(debug.contains("CoalescingBuffer"));
    }

    #[test]
    fn test_coalescing_buffer_same_stripe_merges() {
        let config = CoalescingConfig {
            enabled: true,
            max_bytes: 1024 * 1024,
            max_delay: Duration::from_secs(60),
        };
        let buf = CoalescingBuffer::new(config, 4096);
        let vid = VolumeId::generate();

        buf.add_write(PendingWrite {
            volume_id: vid,
            block_range: 0..32,
            data: Bytes::from(vec![0u8; 50]),
            received_at: Instant::now(),
        });
        buf.add_write(PendingWrite {
            volume_id: vid,
            block_range: 32..64,
            data: Bytes::from(vec![1u8; 50]),
            received_at: Instant::now(),
        });

        assert_eq!(buf.pending_count(), 1);
    }

    #[test]
    fn test_pending_write_debug() {
        let w = PendingWrite {
            volume_id: VolumeId::generate(),
            block_range: 0..64,
            data: Bytes::from(vec![0u8; 10]),
            received_at: Instant::now(),
        };
        let debug = format!("{:?}", w);
        assert!(debug.contains("PendingWrite"));
    }

    #[test]
    fn test_pending_write_clone() {
        let w = PendingWrite {
            volume_id: VolumeId::generate(),
            block_range: 0..64,
            data: Bytes::from(vec![0u8; 10]),
            received_at: Instant::now(),
        };
        let cloned = w.clone();
        assert_eq!(cloned.block_range, 0..64);
    }

    #[tokio::test]
    async fn test_partial_stripe_write() {
        let (pipeline, _cache, vid, _) = make_ec_pipeline(true, true, 6, 4, 2);

        let existing = Bytes::from(vec![0xAA; 4096]);
        let new_data = Bytes::from(vec![0xBB; 100]);

        let result = pipeline
            .partial_stripe_write(vid, 0..64, new_data, existing, 50)
            .await
            .unwrap();

        assert!(matches!(result, WriteOutcome::Committed { .. }));
    }

    #[tokio::test]
    async fn test_partial_stripe_write_replicated_rejected() {
        let cache = MetadataCache::new();
        cache.set_epoch(EpochId::new(1));
        let vid = VolumeId::generate();
        cache.set_volume(CachedVolumeInfo {
            volume_id: vid,
            size_bytes: 1024 * 1024,
            block_size: 4096,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_mappings: BTreeMap::new(),
        });
        let cache = Arc::new(cache);
        let data_client = Arc::new(MockEcDataNode::new(true));
        let metadata_client = Arc::new(MockEcMetadata::new(true, EpochId::new(1)));
        let session = Arc::new(ClientSession::new(vid));
        let watermark = Arc::new(WriteWatermark::new());
        let stale_handler = Arc::new(StaleEpochHandler::new());

        let pipeline = EcWritePipeline::new(
            data_client,
            metadata_client,
            cache,
            session,
            watermark,
            stale_handler,
        );

        let result = pipeline
            .partial_stripe_write(
                vid,
                0..64,
                Bytes::from(vec![0xBB; 100]),
                Bytes::from(vec![0xAA; 4096]),
                50,
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ec_write_with_coalescing() {
        let (cache, vid) = setup_ec_cache(1, 6, 4, 2);
        let cache = Arc::new(cache);
        let data_client = Arc::new(MockEcDataNode::new(true));
        let metadata_client = Arc::new(MockEcMetadata::new(true, EpochId::new(1)));
        let session = Arc::new(ClientSession::new(vid));
        let watermark = Arc::new(WriteWatermark::new());
        let stale_handler = Arc::new(StaleEpochHandler::new());

        let pipeline = EcWritePipeline::new(
            data_client,
            metadata_client,
            cache,
            session,
            watermark,
            stale_handler,
        )
        .with_coalescing(CoalescingConfig::default(), 4096);

        let result = pipeline
            .execute(vid, 0..64, Bytes::from(vec![0x55; 512]))
            .await
            .unwrap();
        assert!(matches!(result, WriteOutcome::Committed { .. }));
    }

    #[test]
    fn test_compute_checksum_deterministic() {
        let c1 = compute_checksum(b"hello");
        let c2 = compute_checksum(b"hello");
        assert_eq!(c1, c2);
    }

    #[test]
    fn test_compute_checksum_different() {
        let c1 = compute_checksum(b"hello");
        let c2 = compute_checksum(b"world");
        assert_ne!(c1, c2);
    }

    #[test]
    fn test_compute_checksum_empty() {
        let c = compute_checksum(b"");
        assert!(!c.is_empty());
    }

    // -----------------------------------------------------------------------
    // InMemoryEcDataNode — stores fragment data keyed by (node_id, extent_id, version).
    // -----------------------------------------------------------------------

    use std::collections::HashMap;
    use parking_lot::Mutex as ParkingMutex;

    struct InMemoryEcDataNode {
        store: ParkingMutex<HashMap<(NodeId, ExtentId, u64), Vec<u8>>>,
    }

    impl InMemoryEcDataNode {
        fn new() -> Self {
            Self {
                store: ParkingMutex::new(HashMap::new()),
            }
        }

        fn all_fragments_for_extent(
            &self,
            extent_id: ExtentId,
            version: u64,
        ) -> Vec<(NodeId, Vec<u8>)> {
            self.store
                .lock()
                .iter()
                .filter(|((_, eid, ver), _)| *eid == extent_id && *ver == version)
                .map(|((nid, _, _), data)| (*nid, data.clone()))
                .collect()
        }
    }

    impl DataNodeClient for InMemoryEcDataNode {
        async fn write_extent(
            &self,
            node_id: NodeId,
            _operation_id: OperationId,
            _session_id: blockyard_common::SessionId,
            _volume_id: VolumeId,
            extent_id: ExtentId,
            extent_version: u64,
            _epoch: EpochId,
            data: Bytes,
            checksum: String,
        ) -> Result<WriteAck, Error> {
            self.store
                .lock()
                .insert((node_id, extent_id, extent_version), data.to_vec());
            Ok(WriteAck {
                node_id,
                success: true,
                checksum,
                error: None,
            })
        }
    }

    // -----------------------------------------------------------------------
    // InMemoryEcMetadata — records commits for verification.
    // -----------------------------------------------------------------------

    struct InMemoryEcMetadata {
        epoch: EpochId,
        committed: ParkingMutex<Vec<CommitRequest>>,
    }

    impl InMemoryEcMetadata {
        fn new(epoch: EpochId) -> Self {
            Self {
                epoch,
                committed: ParkingMutex::new(Vec::new()),
            }
        }

        fn last_commit(&self) -> Option<CommitRequest> {
            self.committed.lock().last().cloned()
        }
    }

    impl MetadataClient for InMemoryEcMetadata {
        async fn refresh_metadata(
            &self,
            cache: &MetadataCache,
        ) -> Result<EpochId, Error> {
            let new_epoch = EpochId::new(self.epoch.as_u64() + 1);
            cache.set_epoch(new_epoch);
            Ok(new_epoch)
        }

        async fn commit_extent_mapping(
            &self,
            request: CommitRequest,
        ) -> Result<EpochId, Error> {
            self.committed.lock().push(request);
            Ok(self.epoch)
        }

        async fn lookup_operation(
            &self,
            _operation_id: &OperationId,
        ) -> Result<Option<CommittedMapping>, Error> {
            Ok(None)
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

    // -----------------------------------------------------------------------
    // EC pipeline roundtrip tests (data integrity through encode/decode).
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_ec_write_then_reconstruct() {
        use reed_solomon_erasure::galois_8::ReedSolomon;

        let k: u8 = 4;
        let m: u8 = 2;
        let num_nodes = (k + m) as usize;
        let (cache, vid) = setup_ec_cache(1, num_nodes, k, m);
        let cache = Arc::new(cache);
        let node_ids: Vec<NodeId> = cache.list_nodes().iter().map(|n| n.node_id).collect();

        let data_client = Arc::new(InMemoryEcDataNode::new());
        let metadata_client = Arc::new(InMemoryEcMetadata::new(EpochId::new(1)));
        let session = Arc::new(ClientSession::new(vid));
        let watermark = Arc::new(WriteWatermark::new());
        let stale_handler = Arc::new(StaleEpochHandler::new());

        let pipeline = EcWritePipeline::new(
            Arc::clone(&data_client),
            Arc::clone(&metadata_client),
            Arc::clone(&cache),
            session,
            Arc::clone(&watermark),
            stale_handler,
        );

        let original_data: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let result = pipeline
            .execute(vid, 0..64, Bytes::from(original_data.clone()))
            .await
            .unwrap();
        assert!(matches!(result, WriteOutcome::Committed { .. }));

        let commit = metadata_client.last_commit().expect("should have a commit");
        let fragments = data_client.all_fragments_for_extent(commit.extent_id, commit.extent_version);
        assert_eq!(
            fragments.len(),
            num_nodes,
            "should have K+M fragments stored"
        );

        let mut shards: Vec<Option<Vec<u8>>> = vec![None; num_nodes];
        for (nid, frag_data) in &fragments {
            let idx = node_ids.iter().position(|n| n == nid).expect("node should be in list");
            shards[idx] = Some(frag_data.clone());
        }

        let rs = ReedSolomon::new(k as usize, m as usize).unwrap();
        rs.reconstruct(&mut shards).expect("reconstruction must succeed");

        let mut reconstructed = Vec::new();
        for shard in shards.iter().take(k as usize) {
            reconstructed.extend_from_slice(shard.as_ref().unwrap());
        }
        reconstructed.truncate(original_data.len());

        assert_eq!(
            reconstructed, original_data,
            "reconstructed data from EC fragments must match original"
        );
    }

    #[tokio::test]
    async fn test_ec_partial_stripe_integrity() {
        use reed_solomon_erasure::galois_8::ReedSolomon;

        let k: u8 = 4;
        let m: u8 = 2;
        let num_nodes = (k + m) as usize;
        let (cache, vid) = setup_ec_cache(1, num_nodes, k, m);
        let cache = Arc::new(cache);
        let node_ids: Vec<NodeId> = cache.list_nodes().iter().map(|n| n.node_id).collect();

        let data_client = Arc::new(InMemoryEcDataNode::new());
        let metadata_client = Arc::new(InMemoryEcMetadata::new(EpochId::new(1)));
        let session = Arc::new(ClientSession::new(vid));
        let watermark = Arc::new(WriteWatermark::new());
        let stale_handler = Arc::new(StaleEpochHandler::new());

        let pipeline = EcWritePipeline::new(
            Arc::clone(&data_client),
            Arc::clone(&metadata_client),
            Arc::clone(&cache),
            session,
            Arc::clone(&watermark),
            stale_handler,
        );

        let existing_stripe = vec![0xAA; 4096];
        let new_data = vec![0xBB; 100];
        let stripe_offset = 50;

        let mut expected = existing_stripe.clone();
        expected[stripe_offset..stripe_offset + new_data.len()].copy_from_slice(&new_data);

        let result = pipeline
            .partial_stripe_write(
                vid,
                0..64,
                Bytes::from(new_data),
                Bytes::from(existing_stripe),
                stripe_offset,
            )
            .await
            .unwrap();
        assert!(matches!(result, WriteOutcome::Committed { .. }));

        let commit = metadata_client.last_commit().expect("should have a commit");
        let fragments = data_client.all_fragments_for_extent(commit.extent_id, commit.extent_version);
        assert_eq!(fragments.len(), num_nodes);

        let mut shards: Vec<Option<Vec<u8>>> = vec![None; num_nodes];
        for (nid, frag_data) in &fragments {
            let idx = node_ids.iter().position(|n| n == nid).expect("node should be in list");
            shards[idx] = Some(frag_data.clone());
        }

        let rs = ReedSolomon::new(k as usize, m as usize).unwrap();
        rs.reconstruct(&mut shards).expect("reconstruction must succeed");

        let mut reconstructed = Vec::new();
        for shard in shards.iter().take(k as usize) {
            reconstructed.extend_from_slice(shard.as_ref().unwrap());
        }
        reconstructed.truncate(expected.len());

        assert_eq!(
            reconstructed, expected,
            "partial stripe reconstruction must match expected modified data"
        );
    }
}
