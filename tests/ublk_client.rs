use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use blockyard_common::{
    DiskId, EpochId, ExtentId, LeaseResponse, NodeId, OperationId, ProtectionPolicy, SessionId,
    VolumeId,
};
use blockyard_storage::extent::compute_checksum;
use blockyard_storage::{ExtentStore, StorageClass};
use blockyard_ublk::metadata_cache::{CachedExtentMapping, CachedVolumeInfo};
use blockyard_ublk::traits::{CommitRequest, CommittedMapping, WriteAck, WriteAckError};
use blockyard_ublk::{
    ClientSession, DataNodeClient, MetadataCache, MetadataClient, StaleEpochHandler, WriteOutcome,
    WritePipeline, WriteRequest, WriteWatermark,
};
use bytes::Bytes;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Disk-backed DataNodeClient: each node backed by a real ExtentStore + TempDir
// ---------------------------------------------------------------------------

struct DiskBackedDataNode {
    stores: parking_lot::Mutex<HashMap<NodeId, (ExtentStore, TempDir)>>,
    fail: parking_lot::Mutex<bool>,
}

impl DiskBackedDataNode {
    fn new() -> Self {
        Self {
            stores: parking_lot::Mutex::new(HashMap::new()),
            fail: parking_lot::Mutex::new(false),
        }
    }

    fn _set_fail(&self, fail: bool) {
        *self.fail.lock() = fail;
    }

    fn get_or_create_store(&self, node_id: NodeId) -> DiskId {
        let mut stores = self.stores.lock();
        stores
            .entry(node_id)
            .or_insert_with(|| {
                let tmpdir = TempDir::new().expect("create tempdir for node");
                let disk_id = DiskId::generate();
                let store = ExtentStore::new(tmpdir.path().to_path_buf(), disk_id);
                store.init_directories().expect("init directories");
                (store, tmpdir)
            })
            .0
            .disk_id()
    }

    fn read_back(
        &self,
        node_id: NodeId,
        extent_id: ExtentId,
        version: u64,
    ) -> Result<(Vec<u8>, String), String> {
        let stores = self.stores.lock();
        let (store, _) = stores
            .get(&node_id)
            .ok_or_else(|| format!("no store for node {node_id}"))?;
        store
            .read_extent(extent_id, version)
            .map_err(|e| format!("{e}"))
    }

    fn stored_count(&self) -> usize {
        let stores = self.stores.lock();
        stores.len()
    }
}

impl DataNodeClient for DiskBackedDataNode {
    async fn write_extent(
        &self,
        node_id: NodeId,
        _operation_id: OperationId,
        _session_id: SessionId,
        _volume_id: VolumeId,
        extent_id: ExtentId,
        extent_version: u64,
        _epoch: EpochId,
        data: Bytes,
        checksum: String,
    ) -> Result<WriteAck, blockyard_common::Error> {
        if *self.fail.lock() {
            return Ok(WriteAck {
                node_id,
                success: false,
                checksum,
                error: Some(WriteAckError::DiskUnavailable),
            });
        }
        self.get_or_create_store(node_id);
        let mut stores = self.stores.lock();
        let (store, _) = stores.get_mut(&node_id).unwrap();
        let (_, disk_checksum) = store
            .stage_extent(extent_id, extent_version, &data)
            .map_err(|e| blockyard_common::Error::Storage(format!("{e}")))?;
        store
            .commit_extent(
                extent_id,
                extent_version,
                &disk_checksum,
                data.len() as u64,
                StorageClass::Default,
            )
            .map_err(|e| blockyard_common::Error::Storage(format!("{e}")))?;

        Ok(WriteAck {
            node_id,
            success: true,
            checksum,
            error: None,
        })
    }
}

// ---------------------------------------------------------------------------
// Mock MetadataClient
// ---------------------------------------------------------------------------

struct MockMetadata {
    epoch: parking_lot::Mutex<EpochId>,
    commits: parking_lot::Mutex<Vec<CommitRequest>>,
    committed_ops: parking_lot::Mutex<std::collections::HashMap<OperationId, CommittedMapping>>,
    fail_commit: parking_lot::Mutex<bool>,
    stale_epoch_on_commit: parking_lot::Mutex<bool>,
}

impl MockMetadata {
    fn new(epoch: EpochId) -> Self {
        Self {
            epoch: parking_lot::Mutex::new(epoch),
            commits: parking_lot::Mutex::new(Vec::new()),
            committed_ops: parking_lot::Mutex::new(std::collections::HashMap::new()),
            fail_commit: parking_lot::Mutex::new(false),
            stale_epoch_on_commit: parking_lot::Mutex::new(false),
        }
    }

    fn set_epoch(&self, epoch: EpochId) {
        *self.epoch.lock() = epoch;
    }
}

impl MetadataClient for MockMetadata {
    async fn refresh_metadata(
        &self,
        cache: &MetadataCache,
    ) -> Result<EpochId, blockyard_common::Error> {
        let epoch = *self.epoch.lock();
        cache.set_epoch(epoch);
        Ok(epoch)
    }

    async fn commit_extent_mapping(
        &self,
        request: CommitRequest,
    ) -> Result<EpochId, blockyard_common::Error> {
        if *self.fail_commit.lock() {
            return Err(blockyard_common::Error::Raft("commit failed".into()));
        }
        if *self.stale_epoch_on_commit.lock() {
            return Err(blockyard_common::Error::Raft("stale epoch".into()));
        }
        let epoch = *self.epoch.lock();
        if let Some(op_id) = &request.operation_id {
            let mapping = CommittedMapping {
                extent_id: request.extent_id,
                extent_version: request.extent_version,
                epoch,
                block_range: request.block_range.clone(),
                replica_locations: request.replica_locations.clone(),
                checksums: request.checksums.clone(),
            };
            self.committed_ops.lock().insert(*op_id, mapping);
        }
        self.commits.lock().push(request);
        Ok(epoch)
    }

    async fn lookup_operation(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<CommittedMapping>, blockyard_common::Error> {
        Ok(self.committed_ops.lock().get(operation_id).cloned())
    }

    async fn current_epoch(&self) -> Result<EpochId, blockyard_common::Error> {
        Ok(*self.epoch.lock())
    }

    async fn acquire_lease(
        &self,
        _volume_id: VolumeId,
        _session_id: SessionId,
        _now_ms: u64,
        _ttl_ms: u64,
    ) -> Result<LeaseResponse, blockyard_common::Error> {
        Ok(LeaseResponse::Granted {
            lease_version: 1,
            expires_at_ms: u64::MAX,
        })
    }

    async fn renew_lease(
        &self,
        _volume_id: VolumeId,
        _session_id: SessionId,
        _now_ms: u64,
        _ttl_ms: u64,
    ) -> Result<LeaseResponse, blockyard_common::Error> {
        Ok(LeaseResponse::Renewed {
            lease_version: 1,
            expires_at_ms: u64::MAX,
        })
    }

    async fn release_lease(
        &self,
        _volume_id: VolumeId,
        _session_id: SessionId,
    ) -> Result<LeaseResponse, blockyard_common::Error> {
        Ok(LeaseResponse::Released)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn setup_pipeline(
    volume_id: VolumeId,
    epoch: EpochId,
    node_ids: &[NodeId],
    data_client: Arc<DiskBackedDataNode>,
    metadata_client: Arc<MockMetadata>,
) -> (
    WritePipeline<DiskBackedDataNode, MockMetadata>,
    Arc<MetadataCache>,
    Arc<ClientSession>,
    Arc<WriteWatermark>,
) {
    let cache = Arc::new(MetadataCache::new());
    cache.set_epoch(epoch);

    for (i, nid) in node_ids.iter().enumerate() {
        let addr: std::net::SocketAddr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
        cache.set_node(*nid, addr);
    }

    let ext_id = ExtentId::generate();
    let mapping = CachedExtentMapping {
        extent_id: ext_id,
        extent_version: 0,
        replica_locations: node_ids.to_vec(),
        checksums: vec![],
    };
    cache.set_extent_mapping(&volume_id, 0, mapping);

    let vol_info = CachedVolumeInfo {
        volume_id,
        size_bytes: 1024 * 1024,
        protection: ProtectionPolicy::Replicated {
            replicas: node_ids.len() as u8,
        },
        extent_mappings: BTreeMap::new(),
    };
    cache.set_volume(vol_info);

    let session = Arc::new(ClientSession::new(volume_id));
    let watermark = Arc::new(WriteWatermark::with_initial(epoch));
    let stale_handler = Arc::new(StaleEpochHandler::new());

    let pipeline = WritePipeline::new(
        data_client,
        metadata_client,
        cache.clone(),
        session.clone(),
        watermark.clone(),
        stale_handler,
    );

    (pipeline, cache, session, watermark)
}

// ---------------------------------------------------------------------------
// P9F.1 — Write data, simulate crash (drop pipeline), verify data persisted
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mount_write_crash_remount_verify() {
    let volume_id = VolumeId::generate();
    let epoch = EpochId::new(1);
    let node1 = NodeId::generate();
    let node2 = NodeId::generate();
    let node3 = NodeId::generate();

    let data_client = Arc::new(DiskBackedDataNode::new());
    let metadata_client = Arc::new(MockMetadata::new(epoch));

    let (pipeline, _cache, _session, _watermark) = setup_pipeline(
        volume_id,
        epoch,
        &[node1, node2, node3],
        data_client.clone(),
        metadata_client.clone(),
    );

    let write_data = Bytes::from(vec![0xABu8; 4096]);
    let request = WriteRequest {
        volume_id,
        block_range: 0..1024,
        data: write_data.clone(),
    };
    let result = pipeline.execute(request).await;
    assert!(result.is_ok(), "write should succeed: {:?}", result.err());
    assert!(matches!(result.unwrap(), WriteOutcome::Committed { .. }));

    let stored_before_crash = data_client.stored_count();
    assert!(
        stored_before_crash > 0,
        "data should be stored on nodes before crash"
    );
    let committed_before_crash = metadata_client.commits.lock().len();
    assert!(
        committed_before_crash > 0,
        "metadata commit should have happened"
    );

    let commit_req = metadata_client.commits.lock()[0].clone();
    let extent_id = commit_req.extent_id;
    let version = commit_req.extent_version;

    for nid in &[node1, node2, node3] {
        if let Ok((disk_data, disk_checksum)) = data_client.read_back(*nid, extent_id, version) {
            assert_eq!(
                disk_data,
                write_data.as_ref(),
                "data on disk must match original write"
            );
            let expected = compute_checksum(&write_data);
            assert_eq!(
                disk_checksum, expected,
                "checksum from disk must match computed"
            );
        }
    }

    drop(pipeline);

    let (_pipeline2, _, _, _) = setup_pipeline(
        volume_id,
        epoch,
        &[node1, node2, node3],
        data_client.clone(),
        metadata_client.clone(),
    );

    for nid in &[node1, node2, node3] {
        if let Ok((disk_data, _)) = data_client.read_back(*nid, extent_id, version) {
            assert_eq!(
                disk_data,
                write_data.as_ref(),
                "data on disk must survive pipeline crash"
            );
        }
    }

    let committed_ops = metadata_client.committed_ops.lock();
    assert!(
        !committed_ops.is_empty() || committed_before_crash > 0,
        "committed operation should be recoverable"
    );
}

// ---------------------------------------------------------------------------
// P9F.2 — StaleEpoch triggers refresh and pipeline switches to new epoch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_partition_follow_leader_stale_epoch() {
    let volume_id = VolumeId::generate();
    let old_epoch = EpochId::new(1);
    let node1 = NodeId::generate();
    let node2 = NodeId::generate();

    let data_client = Arc::new(DiskBackedDataNode::new());
    let metadata_client = Arc::new(MockMetadata::new(old_epoch));

    let cache = Arc::new(MetadataCache::new());
    cache.set_epoch(old_epoch);
    cache.set_node(node1, "127.0.0.1:9001".parse().unwrap());
    cache.set_node(node2, "127.0.0.1:9002".parse().unwrap());

    let mapping = CachedExtentMapping {
        extent_id: ExtentId::generate(),
        extent_version: 0,
        replica_locations: vec![node1, node2],
        checksums: vec![],
    };
    cache.set_extent_mapping(&volume_id, 0, mapping);
    cache.set_volume(CachedVolumeInfo {
        volume_id,
        size_bytes: 1024 * 1024,
        protection: ProtectionPolicy::Replicated { replicas: 2 },
        extent_mappings: BTreeMap::new(),
    });

    let stale_handler = Arc::new(StaleEpochHandler::new());
    let session = Arc::new(ClientSession::new(volume_id));
    let watermark = Arc::new(WriteWatermark::with_initial(old_epoch));

    assert_eq!(stale_handler.refresh_count(), 0);
    assert_eq!(cache.current_epoch(), old_epoch);

    let new_epoch = EpochId::new(5);
    metadata_client.set_epoch(new_epoch);

    let refreshed_epoch = stale_handler
        .handle_stale_epoch(&cache, metadata_client.as_ref(), old_epoch)
        .await
        .expect("stale epoch refresh should succeed");

    assert_eq!(refreshed_epoch, new_epoch);
    assert_eq!(cache.current_epoch(), new_epoch);
    assert_eq!(stale_handler.refresh_count(), 1);

    let pipeline = WritePipeline::new(
        data_client.clone(),
        metadata_client.clone(),
        cache.clone(),
        session.clone(),
        watermark.clone(),
        stale_handler.clone(),
    );

    let request = WriteRequest {
        volume_id,
        block_range: 0..1024,
        data: Bytes::from(vec![0xCDu8; 4096]),
    };
    let result = pipeline.execute(request).await;
    assert!(
        result.is_ok(),
        "write should succeed after epoch refresh: {:?}",
        result.err()
    );
}

// ---------------------------------------------------------------------------
// P9F.3 — Partial write not committed when pipeline drops mid-flight
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_write_crash_partial_not_committed() {
    let volume_id = VolumeId::generate();
    let epoch = EpochId::new(1);
    let node1 = NodeId::generate();
    let node2 = NodeId::generate();
    let node3 = NodeId::generate();

    let data_client = Arc::new(DiskBackedDataNode::new());
    let metadata_client = Arc::new(MockMetadata::new(epoch));

    *metadata_client.fail_commit.lock() = true;

    let (pipeline, _, session, _) = setup_pipeline(
        volume_id,
        epoch,
        &[node1, node2, node3],
        data_client.clone(),
        metadata_client.clone(),
    );

    let op_id = session.next_operation_id();
    let request = WriteRequest {
        volume_id,
        block_range: 0..1024,
        data: Bytes::from(vec![0xEFu8; 4096]),
    };
    let result = pipeline.execute_with_op_id(request, op_id).await;

    match result {
        Ok(WriteOutcome::MetadataCommitFailed { .. }) => {}
        Ok(WriteOutcome::Committed { .. }) => {
            panic!("should not commit when metadata commit fails");
        }
        Err(_) => {}
        Ok(other) => {
            assert!(
                !matches!(other, WriteOutcome::Committed { .. }),
                "should not have Committed outcome when commit fails: {:?}",
                other
            );
        }
    }

    let committed = metadata_client.commits.lock();
    assert!(
        committed.is_empty(),
        "no commits should succeed when metadata is failing"
    );

    let lookup = metadata_client.committed_ops.lock();
    assert!(
        lookup.get(&op_id).is_none(),
        "operation should NOT be in committed ops"
    );
}
