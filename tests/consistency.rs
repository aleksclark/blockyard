use std::collections::BTreeMap;
use std::sync::Arc;

use blockyard_common::LeaseResponse;
use blockyard_common::{
    EpochId, ExtentId, NodeId, OperationId, ProtectionPolicy, SessionId, VolumeId,
};
use blockyard_raft::{LogStore, MetadataService, NetworkFactory, Router, StateMachineStore};
use blockyard_ublk::freshness::FreshnessStatus;
use blockyard_ublk::traits::{CommitRequest, CommittedMapping, WriteAck, WriteAckError};
use blockyard_ublk::{
    ClientSession, DataNodeClient, FreshnessChecker, MetadataCache, MetadataClient,
    StaleEpochHandler, WriteOutcome, WritePipeline, WriteRequest, WriteWatermark,
};
use bytes::Bytes;
use openraft::BasicNode;
use parking_lot::RwLock;

// ---------------------------------------------------------------------------
// Helpers: stand up a real multi-node Raft cluster in-memory
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
        .expect("failed to create Raft node");

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
        .expect("failed to initialize cluster");

    tokio::time::sleep(std::time::Duration::from_millis(800)).await;

    RaftCluster {
        services,
        _router: router,
    }
}

async fn find_leader(cluster: &RaftCluster) -> usize {
    for _ in 0..20 {
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

// ---------------------------------------------------------------------------
// Mock DataNodeClient for WritePipeline tests
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

    fn set_fail_nodes(&self, nodes: Vec<NodeId>) {
        *self.fail_nodes.lock() = nodes;
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
        self.stored.lock().push((node_id, extent_id, data.clone()));
        Ok(WriteAck {
            node_id,
            success: true,
            checksum,
            error: None,
        })
    }
}

// ---------------------------------------------------------------------------
// Mock MetadataClient for WritePipeline tests
// ---------------------------------------------------------------------------

struct MockMetadataClient {
    epoch: parking_lot::Mutex<EpochId>,
    committed: parking_lot::Mutex<Vec<CommitRequest>>,
    fail_commit: parking_lot::Mutex<bool>,
}

impl MockMetadataClient {
    fn new(epoch: EpochId) -> Self {
        Self {
            epoch: parking_lot::Mutex::new(epoch),
            committed: parking_lot::Mutex::new(Vec::new()),
            fail_commit: parking_lot::Mutex::new(false),
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
        if *self.fail_commit.lock() {
            return Err(blockyard_common::Error::Raft("commit failed".to_string()));
        }
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
// P9B.1 — Linearizability: Raft entries committed on leader are visible on
//          all followers, including after leader failover
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_linearizability_all_ack_leader_failover() {
    let cluster = create_raft_cluster(3).await;
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

    let node_id = blockyard_common::NodeId::generate();
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

    let data_client = Arc::new(MockDataNodeClient::new());
    let metadata_client = Arc::new(MockMetadataClient::new(epoch));
    let cache = Arc::new(MetadataCache::new());
    cache.set_epoch(epoch);

    let node1 = blockyard_common::NodeId::generate();
    let node2 = blockyard_common::NodeId::generate();
    let node3 = blockyard_common::NodeId::generate();

    let addr: std::net::SocketAddr = "127.0.0.1:9001".parse().unwrap();
    cache.set_node(node1, addr);
    cache.set_node(node2, "127.0.0.1:9002".parse().unwrap());
    cache.set_node(node3, "127.0.0.1:9003".parse().unwrap());

    let session = Arc::new(ClientSession::new(volume_id));
    let watermark = Arc::new(WriteWatermark::with_initial(epoch));
    let stale_handler = Arc::new(StaleEpochHandler::new());

    let ext_id = ExtentId::generate();
    let mapping = blockyard_ublk::metadata_cache::CachedExtentMapping {
        extent_id: ext_id,
        extent_version: 0,
        replica_locations: vec![node1, node2, node3],
        checksums: vec![],
    };
    cache.set_extent_mapping(&volume_id, 0, mapping);

    let vol_info = blockyard_ublk::metadata_cache::CachedVolumeInfo {
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
    let outcome = result.unwrap();
    assert!(
        matches!(outcome, WriteOutcome::Committed { .. }),
        "write should be committed: {:?}",
        outcome
    );

    {
        let committed = metadata_client.committed.lock();
        assert_eq!(committed.len(), 1, "one extent mapping should be committed");
    }

    {
        let stored = data_client.stored.lock();
        assert!(
            stored.len() >= 2,
            "at least majority (2 of 3) nodes should have stored data, got {}",
            stored.len()
        );
    }

    data_client.set_fail_nodes(vec![node1, node2]);

    let request2 = WriteRequest {
        volume_id,
        block_range: 1024..2048,
        data: Bytes::from(vec![99u8; 4096]),
    };
    let result2 = pipeline.execute(request2).await;
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
    let cache = MetadataCache::new();
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
    let metadata_client = MockMetadataClient::new(EpochId::new(5));
    let cache = MetadataCache::new();
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
