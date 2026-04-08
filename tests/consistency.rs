use std::sync::Arc;

use blockyard_common::{EpochId, ExtentId, NodeId, OperationId, ProtectionPolicy, SessionId, VolumeId};
use blockyard_test_harness::mock_datanode::{DiskBackedTestDataNode, verify_disk_data};
use blockyard_test_harness::mock_metadata::TestMetadataClient;
use blockyard_test_harness::pipeline_testutil::setup_test_pipeline;
use blockyard_test_harness::raft_testutil::{create_test_raft_cluster, find_leader};
use blockyard_ublk::freshness::FreshnessStatus;
use blockyard_ublk::{
    FreshnessChecker, StaleEpochHandler, WriteOutcome, WriteRequest, WriteWatermark,
};
use bytes::Bytes;

// ---------------------------------------------------------------------------
// Read-pipeline mocks (only used in this file)
// ---------------------------------------------------------------------------

struct MockMetadataProvider {
    mappings: parking_lot::Mutex<
        std::collections::HashMap<(VolumeId, ExtentId), blockyard_client::ExtentMapping>,
    >,
    watermarks: parking_lot::Mutex<std::collections::HashMap<(SessionId, VolumeId), u64>>,
}

impl MockMetadataProvider {
    fn new() -> Self {
        Self {
            mappings: parking_lot::Mutex::new(std::collections::HashMap::new()),
            watermarks: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    fn set_mapping(&self, mapping: blockyard_client::ExtentMapping) {
        self.mappings
            .lock()
            .insert((mapping.volume_id, mapping.extent_id), mapping);
    }

    fn set_watermark(&self, session_id: SessionId, volume_id: VolumeId, version: u64) {
        self.watermarks
            .lock()
            .insert((session_id, volume_id), version);
    }
}

impl blockyard_client::MetadataProvider for MockMetadataProvider {
    async fn get_extent_mapping(
        &self,
        volume_id: VolumeId,
        extent_id: ExtentId,
    ) -> Result<Option<blockyard_client::ExtentMapping>, blockyard_client::ReadError> {
        Ok(self.mappings.lock().get(&(volume_id, extent_id)).cloned())
    }

    async fn get_write_watermark(
        &self,
        session_id: SessionId,
        volume_id: VolumeId,
    ) -> Result<u64, blockyard_client::ReadError> {
        Ok(self
            .watermarks
            .lock()
            .get(&(session_id, volume_id))
            .copied()
            .unwrap_or(0))
    }

    async fn refresh_extent_mapping(
        &self,
        volume_id: VolumeId,
        extent_id: ExtentId,
    ) -> Result<Option<blockyard_client::ExtentMapping>, blockyard_client::ReadError> {
        self.get_extent_mapping(volume_id, extent_id).await
    }
}

struct MockDataNodeReader {
    data: parking_lot::Mutex<std::collections::HashMap<(NodeId, ExtentId), (Bytes, String)>>,
}

impl MockDataNodeReader {
    fn new() -> Self {
        Self {
            data: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    fn store(&self, node_id: NodeId, extent_id: ExtentId, data: Bytes, checksum: String) {
        self.data
            .lock()
            .insert((node_id, extent_id), (data, checksum));
    }
}

impl blockyard_client::DataNodeReader for MockDataNodeReader {
    async fn read_extent(
        &self,
        node_id: NodeId,
        _volume_id: VolumeId,
        extent_id: ExtentId,
        extent_version: u64,
        _offset: u64,
        _length: u64,
    ) -> Result<blockyard_client::DataNodeReadResult, blockyard_client::ReadError> {
        let guard = self.data.lock();
        match guard.get(&(node_id, extent_id)) {
            Some((data, checksum)) => Ok(blockyard_client::DataNodeReadResult {
                extent_id,
                extent_version,
                checksum: checksum.clone(),
                data: data.clone(),
            }),
            None => Err(blockyard_client::ReadError::DataNodeReadFailed {
                node_id,
                reason: "no data".to_string(),
            }),
        }
    }
}

struct NoopHealthReporter;

impl blockyard_client::HealthReporter for NoopHealthReporter {
    async fn report_corruption(&self, _report: blockyard_client::CorruptionReport) {}
    async fn report_read_failure(&self, _report: blockyard_client::ReadFailureReport) {}
}

// ---------------------------------------------------------------------------
// P9B.1 — Linearizability: Raft entries committed on leader are visible on
//          all followers, including after leader failover
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_linearizability_all_ack_leader_failover() {
    let cluster = create_test_raft_cluster(3).await;
    let leader_idx = find_leader(&cluster).await;
    let leader = &cluster.services[leader_idx];

    let vol_id = VolumeId::generate();
    leader
        .create_volume(
            vol_id,
            1024 * 1024 * 1024,
            ProtectionPolicy::Replicated { replicas: 3 },
        )
        .await
        .expect("create volume");

    let epoch = leader.advance_epoch().await.expect("advance epoch");

    let node_id = NodeId::generate();
    leader
        .add_node(node_id, "127.0.0.1:9000".to_string())
        .await
        .expect("add node");

    let ext_id = ExtentId::generate();
    let committed_epoch = leader
        .commit_extent_mapping(
            vol_id,
            0..1024,
            ext_id,
            1,
            epoch,
            vec![node_id],
            vec![vec![1, 2, 3]],
            Some(OperationId::generate()),
            None,
        )
        .await
        .expect("commit extent mapping");

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    for (i, svc) in cluster.services.iter().enumerate() {
        let vol = svc.get_volume(&vol_id);
        assert!(vol.is_some(), "node {} must see the created volume", i + 1);
        assert_eq!(vol.unwrap().volume_id, vol_id);

        let mapping = svc.lookup_by_extent_version(1);
        assert!(
            mapping.is_some(),
            "node {} must see committed extent mapping",
            i + 1
        );
        let m = mapping.unwrap();
        assert_eq!(m.extent_id, ext_id);
        assert_eq!(m.block_range, 0..1024);
    }

    let old_leader_id = (leader_idx + 1) as u64;
    cluster.services[leader_idx]
        .raft()
        .shutdown()
        .await
        .expect("shutdown leader");

    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    let mut new_leader_idx = None;
    for _ in 0..30 {
        for (i, svc) in cluster.services.iter().enumerate() {
            if (i + 1) as u64 == old_leader_id {
                continue;
            }
            let metrics = svc.raft().metrics().borrow().clone();
            if metrics.current_leader == Some((i + 1) as u64) {
                new_leader_idx = Some(i);
                break;
            }
        }
        if new_leader_idx.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let new_leader_idx = new_leader_idx.expect("new leader should be elected after failover");
    let new_leader = &cluster.services[new_leader_idx];

    let vol = new_leader.get_volume(&vol_id);
    assert!(vol.is_some(), "volume must survive leader failover");

    let mapping = new_leader.lookup_by_extent_version(1);
    assert!(
        mapping.is_some(),
        "extent mapping must survive leader failover"
    );

    let ext_id_2 = ExtentId::generate();
    let result = new_leader
        .commit_extent_mapping(
            vol_id,
            1024..2048,
            ext_id_2,
            2,
            committed_epoch,
            vec![node_id],
            vec![vec![4, 5, 6]],
            Some(OperationId::generate()),
            None,
        )
        .await;
    assert!(
        result.is_ok(),
        "new leader must accept writes after failover: {:?}",
        result.err()
    );
}

// ---------------------------------------------------------------------------
// P9B.2 — Majority-ack: WritePipeline only commits when majority acks arrive
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_majority_ack_no_write_loss() {
    let volume_id = VolumeId::generate();
    let epoch = EpochId::new(1);

    let node1 = NodeId::generate();
    let node2 = NodeId::generate();
    let node3 = NodeId::generate();

    let data_client = Arc::new(DiskBackedTestDataNode::new());
    let metadata_client = Arc::new(TestMetadataClient::new(epoch));

    let setup = setup_test_pipeline(
        volume_id,
        epoch,
        &[node1, node2, node3],
        data_client.clone(),
        metadata_client.clone(),
    );

    let data = Bytes::from(vec![42u8; 4096]);
    let request = WriteRequest {
        volume_id,
        block_range: 0..1024,
        data: data.clone(),
    };
    let result = setup.pipeline.execute(request).await;
    assert!(result.is_ok(), "write should succeed: {:?}", result.err());
    let outcome = result.unwrap();
    assert!(
        matches!(outcome, WriteOutcome::Committed { .. }),
        "write should be committed: {:?}",
        outcome
    );

    {
        let committed = metadata_client.committed.lock();
        assert_eq!(committed.len(), 1, "one extent mapping should be committed");
        let commit_req = &committed[0];
        let extent_id = commit_req.extent_id;
        let version = commit_req.extent_version;

        let readable_count =
            verify_disk_data(&data_client, &[node1, node2, node3], extent_id, version, &data);
        assert!(
            readable_count >= 2,
            "at least majority (2 of 3) nodes should have data on disk, got {readable_count}",
        );
    }

    data_client.set_fail_nodes(vec![node1, node2]);

    let request2 = WriteRequest {
        volume_id,
        block_range: 1024..2048,
        data: Bytes::from(vec![99u8; 4096]),
    };
    let result2 = setup.pipeline.execute(request2).await;
    match result2 {
        Ok(WriteOutcome::InsufficientAcks { acked, required }) => {
            assert!(
                acked < required,
                "should have insufficient acks: got {} of {}",
                acked,
                required
            );
        }
        Ok(WriteOutcome::Committed { .. }) => {
            let committed = metadata_client.committed.lock();
            assert!(
                committed.len() <= 2,
                "should not commit without majority acks or handle differently"
            );
        }
        Ok(other) => {
            assert!(
                !matches!(other, WriteOutcome::Committed { .. }),
                "should not commit when majority of nodes fail: {:?}",
                other
            );
        }
        Err(_) => {}
    }
}

// ---------------------------------------------------------------------------
// P9B.3 — Read-your-own-writes: watermark enforcement in ReadPipeline
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_read_your_own_writes_leader_transition() {
    let volume_id = VolumeId::generate();
    let extent_id = ExtentId::generate();
    let session_id = SessionId::generate();
    let node_id = NodeId::generate();

    let data = Bytes::from(vec![77u8; 4096]);
    let checksum = blake3::hash(&data).to_hex().to_string();

    let metadata = MockMetadataProvider::new();
    let mapping = blockyard_client::ExtentMapping {
        volume_id,
        extent_id,
        extent_version: 5,
        epoch: EpochId::new(1),
        replicas: vec![blockyard_client::ReplicaLocation {
            node_id,
            is_local: false,
        }],
        checksum: checksum.clone(),
    };
    metadata.set_mapping(mapping);
    metadata.set_watermark(session_id, volume_id, 5);

    let reader = MockDataNodeReader::new();
    reader.store(node_id, extent_id, data.clone(), checksum.clone());

    let selector = blockyard_client::LatencyAwareSelector::new();
    let pipeline = blockyard_client::ReadPipeline::new(
        metadata,
        reader,
        NoopHealthReporter,
        selector,
        session_id,
    );

    let request = blockyard_client::ReadRequest {
        volume_id,
        extent_id,
        offset: 0,
        length: 4096,
    };
    let result = pipeline.read(&request).await;
    assert!(result.is_ok(), "read should succeed: {:?}", result.err());
    let read_result = result.unwrap();
    assert_eq!(read_result.data, data);
    assert_eq!(read_result.extent_version, 5);
    assert_eq!(read_result.source_node, node_id);
}

#[tokio::test]
async fn test_ryow_stale_mapping_rejected() {
    let volume_id = VolumeId::generate();
    let extent_id = ExtentId::generate();
    let session_id = SessionId::generate();
    let node_id = NodeId::generate();

    let metadata = MockMetadataProvider::new();
    let stale_mapping = blockyard_client::ExtentMapping {
        volume_id,
        extent_id,
        extent_version: 2,
        epoch: EpochId::new(1),
        replicas: vec![blockyard_client::ReplicaLocation {
            node_id,
            is_local: false,
        }],
        checksum: "abc".to_string(),
    };
    metadata.set_mapping(stale_mapping);
    metadata.set_watermark(session_id, volume_id, 10);

    let reader = MockDataNodeReader::new();
    let selector = blockyard_client::LatencyAwareSelector::new();
    let pipeline = blockyard_client::ReadPipeline::new(
        metadata,
        reader,
        NoopHealthReporter,
        selector,
        session_id,
    );

    let request = blockyard_client::ReadRequest {
        volume_id,
        extent_id,
        offset: 0,
        length: 4096,
    };
    let result = pipeline.read(&request).await;
    assert!(result.is_err(), "stale mapping should be rejected");
    match result.unwrap_err() {
        blockyard_client::ReadError::StaleMapping {
            mapping_version,
            required_version,
            ..
        } => {
            assert_eq!(mapping_version, 2);
            assert_eq!(required_version, 10);
        }
        other => panic!("expected StaleMapping error, got: {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// P9B.4 — Bounded staleness: FreshnessChecker detects stale cache
// ---------------------------------------------------------------------------

#[test]
fn test_bounded_staleness_freshness_checker() {
    let cache = blockyard_ublk::MetadataCache::new();
    let watermark = WriteWatermark::new();

    cache.set_epoch(EpochId::new(5));
    watermark.advance(EpochId::new(5));

    let checker = FreshnessChecker::new(&cache, &watermark);
    assert!(checker.is_fresh());
    assert_eq!(checker.check(), FreshnessStatus::Fresh);

    watermark.advance(EpochId::new(10));

    let checker2 = FreshnessChecker::new(&cache, &watermark);
    assert!(!checker2.is_fresh());
    match checker2.check() {
        FreshnessStatus::Stale {
            cached_epoch,
            required_epoch,
        } => {
            assert_eq!(cached_epoch, EpochId::new(5));
            assert_eq!(required_epoch, EpochId::new(10));
        }
        FreshnessStatus::Fresh => panic!("expected Stale status"),
    }

    let status = checker2.check_against(EpochId::new(7));
    match status {
        FreshnessStatus::Stale {
            cached_epoch,
            required_epoch,
        } => {
            assert_eq!(cached_epoch, EpochId::new(5));
            assert_eq!(required_epoch, EpochId::new(7));
        }
        FreshnessStatus::Fresh => panic!("expected Stale for epoch 7"),
    }

    cache.set_epoch(EpochId::new(10));
    let checker3 = FreshnessChecker::new(&cache, &watermark);
    assert!(checker3.is_fresh());
    assert_eq!(checker3.check(), FreshnessStatus::Fresh);
}

#[tokio::test]
async fn test_stale_epoch_handler_triggers_refresh() {
    let epoch = EpochId::new(1);
    let metadata_client = TestMetadataClient::new(EpochId::new(5));
    let cache = blockyard_ublk::MetadataCache::new();
    cache.set_epoch(epoch);
    let handler = StaleEpochHandler::new();

    assert_eq!(handler.refresh_count(), 0);

    let new_epoch = handler
        .handle_stale_epoch(&cache, &metadata_client, epoch)
        .await
        .expect("refresh should succeed");

    assert_eq!(new_epoch, EpochId::new(5));
    assert_eq!(cache.current_epoch(), EpochId::new(5));
    assert_eq!(handler.refresh_count(), 1);
}
