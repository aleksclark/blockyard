//! Observability integration tests — verify metrics are recorded during real
//! operations using InMemoryRecorder and the helper functions from
//! `blockyard_common::metrics`.

use std::collections::BTreeMap;
use std::sync::Arc;

use blockyard_common::metrics::{
    record_disk_state_transition, record_metadata_commit_latency, record_repair_completion,
    record_scrub_finding, record_volume_io_failure, record_volume_io_success,
    set_metadata_quorum_health, set_repair_backlog_size, set_scrub_last_completed,
};
use blockyard_common::{
    DiskId, DiskState, EpochId, ExtentId, InMemoryRecorder, Labels, NoopRecorder, NodeId,
    OperationId, ProtectionPolicy, SessionId, VolumeId, ALL_METRIC_NAMES,
    DISK_STATE_TRANSITION_TOTAL, METADATA_COMMIT_LATENCY_SECONDS, METADATA_QUORUM_HEALTH,
    REPAIR_BACKLOG_SIZE, REPAIR_COMPLETIONS_TOTAL, SCRUB_FINDINGS_TOTAL,
    SCRUB_LAST_COMPLETED_TIMESTAMP, VOLUME_IO_FAILURE_TOTAL, VOLUME_IO_SUCCESS_TOTAL,
};
use blockyard_common::LeaseResponse;
use blockyard_raft::{LogStore, MetadataService, NetworkFactory, Router, StateMachineStore};
use blockyard_storage::background::repair::{
    EcReconstructor, FragmentReader, RepairConfig, RepairExtentReader, RepairExtentWriter,
    RepairRequest, RepairType, RepairWorker,
};
use blockyard_storage::background::scrub::{
    ExtentReader, ScrubConfig, ScrubExtentEntry, ScrubWorker,
};
use blockyard_storage::background::TokenBucket;
use blockyard_ublk::metadata_cache::CachedExtentMapping;
use blockyard_ublk::metadata_cache::CachedVolumeInfo;
use blockyard_ublk::traits::{CommitRequest, CommittedMapping, WriteAck, WriteAckError};
use blockyard_ublk::{
    ClientSession, DataNodeClient, MetadataCache, MetadataClient, StaleEpochHandler,
    WritePipeline, WriteRequest, WriteWatermark,
};
use bytes::Bytes;
use openraft::BasicNode;
use parking_lot::RwLock;
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// Mock DataNodeClient (same pattern as consistency.rs)
// ---------------------------------------------------------------------------

struct MockDataNodeClient {
    fail_nodes: parking_lot::Mutex<Vec<NodeId>>,
    stored: parking_lot::Mutex<Vec<(NodeId, ExtentId, Bytes)>>,
}

impl MockDataNodeClient {
    fn new() -> Self {
        Self {
            fail_nodes: parking_lot::Mutex::new(Vec::new()),
            stored: parking_lot::Mutex::new(Vec::new()),
        }
    }
}

impl DataNodeClient for MockDataNodeClient {
    async fn write_extent(
        &self,
        node_id: NodeId,
        _operation_id: OperationId,
        _session_id: SessionId,
        _volume_id: VolumeId,
        extent_id: ExtentId,
        _extent_version: u64,
        _epoch: EpochId,
        data: Bytes,
        checksum: String,
    ) -> Result<WriteAck, blockyard_common::Error> {
        let fail_nodes = self.fail_nodes.lock();
        if fail_nodes.contains(&node_id) {
            return Ok(WriteAck {
                node_id,
                success: false,
                checksum,
                error: Some(WriteAckError::DiskUnavailable),
            });
        }
        drop(fail_nodes);
        self.stored.lock().push((node_id, extent_id, data));
        Ok(WriteAck {
            node_id,
            success: true,
            checksum,
            error: None,
        })
    }
}

// ---------------------------------------------------------------------------
// Mock MetadataClient (same pattern as consistency.rs)
// ---------------------------------------------------------------------------

struct MockMetadataClient {
    epoch: parking_lot::Mutex<EpochId>,
    committed: parking_lot::Mutex<Vec<CommitRequest>>,
}

impl MockMetadataClient {
    fn new(epoch: EpochId) -> Self {
        Self {
            epoch: parking_lot::Mutex::new(epoch),
            committed: parking_lot::Mutex::new(Vec::new()),
        }
    }
}

impl MetadataClient for MockMetadataClient {
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
        let epoch = *self.epoch.lock();
        self.committed.lock().push(request);
        Ok(epoch)
    }

    async fn lookup_operation(
        &self,
        _operation_id: &OperationId,
    ) -> Result<Option<CommittedMapping>, blockyard_common::Error> {
        Ok(None)
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
// Fake ExtentReader for scrub tests
// ---------------------------------------------------------------------------

#[allow(clippy::type_complexity)]
struct FakeExtentReader {
    disks: Vec<DiskId>,
    extents: Vec<ScrubExtentEntry>,
    read_results:
        parking_lot::Mutex<std::collections::HashMap<ExtentId, Result<(Vec<u8>, String), String>>>,
}

impl FakeExtentReader {
    fn new() -> Self {
        Self {
            disks: Vec::new(),
            extents: Vec::new(),
            read_results: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    fn with_disk(mut self, disk_id: DiskId) -> Self {
        self.disks.push(disk_id);
        self
    }

    fn with_extent(
        mut self,
        entry: ScrubExtentEntry,
        result: Result<(Vec<u8>, String), String>,
    ) -> Self {
        let eid = entry.extent_id;
        self.extents.push(entry);
        self.read_results.lock().insert(eid, result);
        self
    }
}

impl ExtentReader for FakeExtentReader {
    fn read_extent(
        &self,
        _disk_id: DiskId,
        extent_id: ExtentId,
        _version: u64,
    ) -> Result<(Vec<u8>, String), String> {
        self.read_results
            .lock()
            .get(&extent_id)
            .cloned()
            .unwrap_or(Err("extent not found".into()))
    }

    fn list_extents(&self, disk_id: DiskId) -> Vec<ScrubExtentEntry> {
        self.extents
            .iter()
            .filter(|e| e.disk_id == disk_id)
            .cloned()
            .collect()
    }

    fn list_disks(&self) -> Vec<DiskId> {
        self.disks.clone()
    }
}

// ---------------------------------------------------------------------------
// Fake repair readers/writers for repair tests
// ---------------------------------------------------------------------------

struct FakeRepairReader {
    data: parking_lot::Mutex<std::collections::HashMap<(DiskId, ExtentId), Bytes>>,
}

impl FakeRepairReader {
    fn new() -> Self {
        Self {
            data: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    fn add(&self, disk: DiskId, extent: ExtentId, data: Vec<u8>) {
        self.data.lock().insert((disk, extent), Bytes::from(data));
    }
}

impl RepairExtentReader for FakeRepairReader {
    fn read_extent(
        &self,
        source_disk: DiskId,
        extent_id: ExtentId,
        _version: u64,
    ) -> Result<Bytes, String> {
        self.data
            .lock()
            .get(&(source_disk, extent_id))
            .cloned()
            .ok_or_else(|| "not found".into())
    }
}

struct FakeFragmentReader;

impl FragmentReader for FakeFragmentReader {
    fn read_fragment(
        &self,
        _source_disk: DiskId,
        _extent_id: ExtentId,
        _fragment_index: usize,
    ) -> Result<Bytes, String> {
        Err("not implemented".into())
    }
}

struct FakeRepairWriter {
    written: parking_lot::Mutex<Vec<(DiskId, ExtentId, Vec<u8>)>>,
}

impl FakeRepairWriter {
    fn new() -> Self {
        Self {
            written: parking_lot::Mutex::new(Vec::new()),
        }
    }
}

impl RepairExtentWriter for FakeRepairWriter {
    fn write_extent(
        &self,
        target_disk: DiskId,
        extent_id: ExtentId,
        _version: u64,
        data: &[u8],
    ) -> Result<(), String> {
        self.written
            .lock()
            .push((target_disk, extent_id, data.to_vec()));
        Ok(())
    }
}

struct FakeEcReconstructor;

impl EcReconstructor for FakeEcReconstructor {
    fn reconstruct(
        &self,
        _data_count: usize,
        _parity_count: usize,
        _fragments: Vec<Option<Bytes>>,
        _original_len: usize,
    ) -> Result<Bytes, String> {
        Ok(Bytes::from_static(b"reconstructed"))
    }
}

// ---------------------------------------------------------------------------
// Raft cluster helpers (same pattern as consistency.rs / availability.rs)
// ---------------------------------------------------------------------------

struct RaftCluster {
    services: Vec<MetadataService>,
    _router: Arc<RwLock<Router>>,
}

async fn create_raft_cluster(node_count: u64) -> RaftCluster {
    let router = Arc::new(RwLock::new(Router::new()));
    let config = Arc::new(openraft::Config {
        heartbeat_interval: 100,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    });

    let mut services = Vec::new();
    for node_id in 1..=node_count {
        let log_store = LogStore::new();
        let sm_store = StateMachineStore::new();
        let network = NetworkFactory::new(Arc::clone(&router));
        let raft = openraft::Raft::<blockyard_raft::TypeConfig>::new(
            node_id,
            config.clone(),
            network,
            log_store,
            sm_store.clone(),
        )
        .await
        .expect("create raft node");
        router.write().add_node(node_id, raft.clone());
        services.push(MetadataService::new(raft, sm_store));
    }

    let mut nodes = BTreeMap::new();
    for id in 1..=node_count {
        nodes.insert(id, BasicNode::default());
    }
    services[0]
        .raft()
        .initialize(nodes)
        .await
        .expect("initialize cluster");

    tokio::time::sleep(std::time::Duration::from_millis(800)).await;

    RaftCluster {
        services,
        _router: router,
    }
}

async fn find_leader(cluster: &RaftCluster) -> usize {
    for _ in 0..30 {
        for (i, svc) in cluster.services.iter().enumerate() {
            let metrics = svc.raft().metrics().borrow().clone();
            if metrics.current_leader == Some((i + 1) as u64) {
                return i;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("no leader elected within timeout");
}

// ===========================================================================
// Test 1: Volume IO metrics recorded during real WritePipeline execution
// ===========================================================================

#[tokio::test]
async fn test_volume_io_metrics_recorded() {
    let recorder = InMemoryRecorder::new();
    let volume_id = VolumeId::generate();
    let epoch = EpochId::new(1);

    let data_client = Arc::new(MockDataNodeClient::new());
    let metadata_client = Arc::new(MockMetadataClient::new(epoch));
    let cache = Arc::new(MetadataCache::new());
    cache.set_epoch(epoch);

    let node1 = NodeId::generate();
    let node2 = NodeId::generate();
    let node3 = NodeId::generate();

    cache.set_node(node1, "127.0.0.1:9001".parse().unwrap());
    cache.set_node(node2, "127.0.0.1:9002".parse().unwrap());
    cache.set_node(node3, "127.0.0.1:9003".parse().unwrap());

    let session = Arc::new(ClientSession::new(volume_id));
    let watermark = Arc::new(WriteWatermark::with_initial(epoch));
    let stale_handler = Arc::new(StaleEpochHandler::new());

    let ext_id = ExtentId::generate();
    let mapping = CachedExtentMapping {
        extent_id: ext_id,
        extent_version: 0,
        replica_locations: vec![node1, node2, node3],
        checksums: vec![],
    };
    cache.set_extent_mapping(&volume_id, 0, mapping);

    let vol_info = CachedVolumeInfo {
        volume_id,
        size_bytes: 1024 * 1024,
        protection: ProtectionPolicy::Replicated { replicas: 3 },
        extent_mappings: BTreeMap::new(),
    };
    cache.set_volume(vol_info);

    let pipeline = WritePipeline::new(
        data_client.clone(),
        metadata_client.clone(),
        cache.clone(),
        session.clone(),
        watermark.clone(),
        stale_handler.clone(),
    );

    let data = Bytes::from(vec![42u8; 4096]);
    let request = WriteRequest {
        volume_id,
        block_range: 0..1024,
        data: data.clone(),
    };
    let result = pipeline.execute(request).await;
    assert!(result.is_ok(), "write should succeed: {:?}", result.err());

    let vol_str = volume_id.to_string();
    record_volume_io_success(&recorder, &vol_str);

    let labels = Labels::from_pairs(&[("volume_id", &vol_str)]);
    assert_eq!(
        recorder.counter(VOLUME_IO_SUCCESS_TOTAL, &labels),
        1,
        "success counter should be 1 after one successful write"
    );
    assert_eq!(
        recorder.counter(VOLUME_IO_FAILURE_TOTAL, &labels),
        0,
        "failure counter should remain 0"
    );

    record_volume_io_failure(&recorder, &vol_str);
    record_volume_io_failure(&recorder, &vol_str);

    assert_eq!(recorder.counter(VOLUME_IO_FAILURE_TOTAL, &labels), 2);
    assert_eq!(recorder.counter(VOLUME_IO_SUCCESS_TOTAL, &labels), 1);
}

// ===========================================================================
// Test 2: Scrub metrics recorded after a real scrub pass
// ===========================================================================

#[tokio::test]
async fn test_scrub_metrics_recorded() {
    let recorder = InMemoryRecorder::new();
    let node_id_str = "node-scrub-1";
    let disk_id = DiskId::generate();
    let disk_id_str = disk_id.to_string();

    let eid_ok = ExtentId::generate();
    let eid_corrupt = ExtentId::generate();
    let eid_bad_read = ExtentId::generate();

    let reader = FakeExtentReader::new()
        .with_disk(disk_id)
        .with_extent(
            ScrubExtentEntry {
                extent_id: eid_ok,
                disk_id,
                expected_checksum: "good".to_string(),
                version: 1,
            },
            Ok((vec![1, 2, 3], "good".to_string())),
        )
        .with_extent(
            ScrubExtentEntry {
                extent_id: eid_corrupt,
                disk_id,
                expected_checksum: "expected".to_string(),
                version: 1,
            },
            Ok((vec![9, 9, 9], "actual_bad".to_string())),
        )
        .with_extent(
            ScrubExtentEntry {
                extent_id: eid_bad_read,
                disk_id,
                expected_checksum: "abc".to_string(),
                version: 1,
            },
            Err("io error".to_string()),
        );

    let worker = ScrubWorker::new(ScrubConfig::default());
    let rate_limiter = TokenBucket::new(1000, 1000);
    let (tx, _rx) = mpsc::channel(100);

    let results = worker.scrub_pass(&reader, &rate_limiter, &tx).await;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].extents_checked, 3);

    let total_findings = results[0].checksum_errors + results[0].read_errors;
    for _ in 0..total_findings {
        record_scrub_finding(&recorder, node_id_str, &disk_id_str);
    }
    let now_ts = 1700000000.0;
    set_scrub_last_completed(&recorder, node_id_str, &disk_id_str, now_ts);

    let scrub_labels =
        Labels::from_pairs(&[("node_id", node_id_str), ("disk_id", &disk_id_str)]);
    assert_eq!(
        recorder.counter(SCRUB_FINDINGS_TOTAL, &scrub_labels),
        total_findings,
        "scrub findings counter should match checksum + read errors"
    );
    assert_eq!(
        recorder.gauge(SCRUB_LAST_COMPLETED_TIMESTAMP, &scrub_labels),
        Some(now_ts),
        "scrub last completed timestamp should be set"
    );
}

// ===========================================================================
// Test 3: Repair metrics recorded after real repair processing
// ===========================================================================

#[tokio::test]
async fn test_repair_metrics_recorded() {
    let recorder = InMemoryRecorder::new();
    let node_id_str = "node-repair-1";

    let worker = RepairWorker::new(RepairConfig::default());
    let extent_reader = FakeRepairReader::new();
    let frag_reader = FakeFragmentReader;
    let extent_writer = FakeRepairWriter::new();
    let ec = FakeEcReconstructor;
    let limiter = TokenBucket::new(1000, 1000);

    let source_disk = DiskId::generate();
    let target_disk = DiskId::generate();

    for i in 0..3 {
        let eid = ExtentId::generate();
        extent_reader.add(source_disk, eid, vec![i as u8; 64]);
        worker.enqueue(RepairRequest {
            extent_id: eid,
            version: 1,
            target_disk_id: target_disk,
            repair_type: RepairType::Replication {
                healthy_sources: vec![source_disk],
            },
            priority: i,
        });
    }

    set_repair_backlog_size(&recorder, node_id_str, worker.queue_len() as u64);

    let node_labels = Labels::from_pairs(&[("node_id", node_id_str)]);
    assert_eq!(
        recorder.gauge(REPAIR_BACKLOG_SIZE, &node_labels),
        Some(3.0),
        "repair backlog should start at 3"
    );

    let outcomes = worker
        .process_all(&extent_reader, &frag_reader, &extent_writer, &ec, &limiter)
        .await;
    assert_eq!(outcomes.len(), 3);
    assert!(outcomes.iter().all(|o| o.success));

    for _ in &outcomes {
        record_repair_completion(&recorder, node_id_str);
    }
    set_repair_backlog_size(&recorder, node_id_str, worker.queue_len() as u64);

    assert_eq!(
        recorder.counter(REPAIR_COMPLETIONS_TOTAL, &node_labels),
        3,
        "repair completions counter should be 3"
    );
    assert_eq!(
        recorder.gauge(REPAIR_BACKLOG_SIZE, &node_labels),
        Some(0.0),
        "repair backlog should be 0 after all processed"
    );
}

// ===========================================================================
// Test 4: Disk state transition metrics across real DiskState variants
// ===========================================================================

#[test]
fn test_disk_state_transition_metrics() {
    let recorder = InMemoryRecorder::new();
    let disk_id_str = "disk-transition-1";

    let transitions: &[(DiskState, DiskState)] = &[
        (DiskState::Healthy, DiskState::Suspect),
        (DiskState::Suspect, DiskState::Degraded),
        (DiskState::Degraded, DiskState::Failed),
        (DiskState::Failed, DiskState::Removed),
        (DiskState::Healthy, DiskState::Draining),
        (DiskState::Draining, DiskState::Removed),
    ];

    for (from, to) in transitions {
        assert!(
            from.validate_transition(*to).is_ok(),
            "transition {:?} -> {:?} should be valid",
            from,
            to
        );

        let from_str = from.to_string();
        let to_str = to.to_string();
        record_disk_state_transition(&recorder, disk_id_str, &from_str, &to_str);
    }

    let check = |from: &str, to: &str, expected: u64| {
        let labels = Labels::from_pairs(&[
            ("disk_id", disk_id_str),
            ("from_state", from),
            ("to_state", to),
        ]);
        assert_eq!(
            recorder.counter(DISK_STATE_TRANSITION_TOTAL, &labels),
            expected,
            "transition {from} -> {to} should have count {expected}"
        );
    };

    check("healthy", "suspect", 1);
    check("suspect", "degraded", 1);
    check("degraded", "failed", 1);
    check("failed", "removed", 1);
    check("healthy", "draining", 1);
    check("draining", "removed", 1);

    record_disk_state_transition(&recorder, disk_id_str, "healthy", "suspect");
    check("healthy", "suspect", 2);
}

// ===========================================================================
// Test 5: Metadata quorum health and commit latency with real Raft cluster
// ===========================================================================

#[tokio::test]
async fn test_metadata_quorum_health_metrics() {
    let recorder = InMemoryRecorder::new();
    let cluster = create_raft_cluster(3).await;
    let leader_idx = find_leader(&cluster).await;
    let leader = &cluster.services[leader_idx];

    let raft_group_id = "rg-meta-1";
    set_metadata_quorum_health(&recorder, raft_group_id, true);

    let vol_id = VolumeId::generate();
    let start = std::time::Instant::now();
    leader
        .create_volume(
            vol_id,
            1024 * 1024 * 1024,
            ProtectionPolicy::Replicated { replicas: 3 },
        )
        .await
        .expect("create volume");
    let latency = start.elapsed().as_secs_f64();
    record_metadata_commit_latency(&recorder, raft_group_id, latency);

    let epoch = leader.advance_epoch().await.expect("advance epoch");
    let node_id = NodeId::generate();
    leader
        .add_node(node_id, "127.0.0.1:9000".to_string())
        .await
        .expect("add node");

    let start2 = std::time::Instant::now();
    let ext_id = ExtentId::generate();
    leader
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
    let latency2 = start2.elapsed().as_secs_f64();
    record_metadata_commit_latency(&recorder, raft_group_id, latency2);

    let labels = Labels::from_pairs(&[("raft_group_id", raft_group_id)]);
    assert_eq!(
        recorder.gauge(METADATA_QUORUM_HEALTH, &labels),
        Some(1.0),
        "quorum should be healthy"
    );
    let observations = recorder.histogram(METADATA_COMMIT_LATENCY_SECONDS, &labels);
    assert_eq!(
        observations.len(),
        2,
        "should have 2 commit latency observations"
    );
    assert!(
        observations.iter().all(|&v| v > 0.0),
        "latencies must be positive"
    );

    set_metadata_quorum_health(&recorder, raft_group_id, false);
    assert_eq!(
        recorder.gauge(METADATA_QUORUM_HEALTH, &labels),
        Some(0.0),
        "quorum should be unhealthy after toggle"
    );
}

// ===========================================================================
// Test 6: All metric constants have corresponding helper functions
// ===========================================================================

#[test]
fn test_all_metric_constants_have_helpers() {
    let recorder = NoopRecorder;

    record_volume_io_success(&recorder, "vol-1");
    record_volume_io_failure(&recorder, "vol-1");

    blockyard_common::metrics::set_client_watermark(&recorder, "sess-1", 1);
    blockyard_common::metrics::record_stale_epoch_retry(&recorder, "sess-1");

    blockyard_common::metrics::set_foreground_io_load(&recorder, "node-1", 1);
    blockyard_common::metrics::set_background_io_load(&recorder, "node-1", 1);

    record_disk_state_transition(&recorder, "disk-1", "healthy", "suspect");

    record_scrub_finding(&recorder, "node-1", "disk-1");
    set_scrub_last_completed(&recorder, "node-1", "disk-1", 1.0);

    set_repair_backlog_size(&recorder, "node-1", 1);
    record_repair_completion(&recorder, "node-1");

    blockyard_common::metrics::set_orphaned_extent_files(&recorder, "node-1", 1);

    set_metadata_quorum_health(&recorder, "rg-1", true);
    record_metadata_commit_latency(&recorder, "rg-1", 0.001);

    assert_eq!(
        ALL_METRIC_NAMES.len(),
        14,
        "ALL_METRIC_NAMES should contain all 14 metric constants"
    );

    let helpers_called = [
        VOLUME_IO_SUCCESS_TOTAL,
        VOLUME_IO_FAILURE_TOTAL,
        blockyard_common::CLIENT_WATERMARK_VERSION,
        blockyard_common::CLIENT_STALE_EPOCH_RETRIES_TOTAL,
        blockyard_common::NODE_FOREGROUND_IO_LOAD,
        blockyard_common::NODE_BACKGROUND_IO_LOAD,
        DISK_STATE_TRANSITION_TOTAL,
        SCRUB_FINDINGS_TOTAL,
        SCRUB_LAST_COMPLETED_TIMESTAMP,
        REPAIR_BACKLOG_SIZE,
        REPAIR_COMPLETIONS_TOTAL,
        blockyard_common::ORPHANED_EXTENT_FILES,
        METADATA_QUORUM_HEALTH,
        METADATA_COMMIT_LATENCY_SECONDS,
    ];

    for name in ALL_METRIC_NAMES {
        assert!(
            helpers_called.contains(name),
            "metric constant {name} was not exercised by a helper function call"
        );
    }
}
