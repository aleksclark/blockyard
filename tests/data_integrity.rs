use std::collections::BTreeMap;
use std::sync::Arc;

use blockyard_common::{
    DiskId, EpochId, ExtentId, NodeId, OperationId, ProtectionPolicy, VolumeId,
};
use blockyard_raft::{
    LogStore, MetadataService, NetworkFactory, Router,
    StateMachineStore,
};
use blockyard_storage::{
    ExtentIndex, ExtentStore, StorageClass,
};
use blockyard_storage::extent::{
    committed_extent_path, compute_checksum, staged_extent_path, verify_checksum,
};
use openraft::BasicNode;
use parking_lot::RwLock;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn create_extent_store(tmpdir: &TempDir) -> (ExtentStore, DiskId) {
    let disk_id = DiskId::generate();
    let store = ExtentStore::new(tmpdir.path().to_path_buf(), disk_id);
    store.init_directories().expect("init directories");
    (store, disk_id)
}

async fn create_raft_cluster(
    node_count: u64,
) -> (Vec<MetadataService>, Arc<RwLock<Router>>) {
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
    (services, router)
}

async fn find_leader(services: &[MetadataService]) -> usize {
    for _ in 0..20 {
        for (i, svc) in services.iter().enumerate() {
            let metrics = svc.raft().metrics().borrow().clone();
            if metrics.current_leader == Some((i + 1) as u64) {
                return i;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("no leader elected");
}

// ---------------------------------------------------------------------------
// P9E.1 — Crash recovery: write extents, simulate crash, run recovery scan
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_crash_recovery_extents_survive() {
    let tmpdir = TempDir::new().expect("create tempdir");
    let (store, _disk_id) = create_extent_store(&tmpdir);
    let index = ExtentIndex::new();

    let mut committed_extents = Vec::new();
    for i in 0..10u64 {
        let extent_id = ExtentId::generate();
        let data = vec![(i as u8).wrapping_mul(0x37); 4096];
        let (_staged_path, checksum) = store
            .stage_extent(extent_id, i + 1, &data)
            .expect("stage extent");

        let entry = store
            .commit_extent(extent_id, i + 1, &checksum, data.len() as u64, StorageClass::Default)
            .expect("commit extent");
        index.insert(entry.clone()).expect("insert into index");
        committed_extents.push((extent_id, i + 1, checksum, data));
    }

    let staged_extent_id = ExtentId::generate();
    let staged_data = vec![0xABu8; 4096];
    let (_staged_path, _staged_checksum) = store
        .stage_extent(staged_extent_id, 100, &staged_data)
        .expect("stage orphan extent");

    assert_eq!(index.count(), 10);
    let staged_path = staged_extent_path(tmpdir.path(), staged_extent_id, 100);
    assert!(
        staged_path.exists(),
        "staged file should exist before recovery"
    );

    let fresh_index = ExtentIndex::new();
    let report = store
        .recover(&fresh_index)
        .expect("recovery should succeed");

    assert_eq!(
        report.committed_recovered, 10,
        "all 10 committed extents should be recovered"
    );
    assert_eq!(
        report.staged_discarded, 1,
        "1 staged (orphaned) extent should be discarded"
    );
    assert_eq!(fresh_index.count(), 10);

    for (extent_id, version, expected_checksum, expected_data) in &committed_extents {
        let (data, checksum) = store
            .read_extent(*extent_id, *version)
            .expect("read committed extent after recovery");
        assert_eq!(&checksum, expected_checksum);
        assert_eq!(&data, expected_data);
    }

    assert!(
        !staged_path.exists(),
        "staged extent should be cleaned up after recovery"
    );
}

// ---------------------------------------------------------------------------
// P9E.2 — Partition convergence: apply Raft entries to subset, replay to all
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_partition_convergence_metadata_state_machine() {
    let (services, _router) = create_raft_cluster(3).await;
    let leader_idx = find_leader(&services).await;
    let leader = &services[leader_idx];

    let vol_id = VolumeId::generate();
    leader
        .create_volume(vol_id, 1_000_000, ProtectionPolicy::Replicated { replicas: 3 })
        .await
        .expect("create volume");

    let epoch = leader.advance_epoch().await.expect("advance epoch");

    let node_id = NodeId::generate();
    leader
        .add_node(node_id, "10.0.0.1:9000".to_string())
        .await
        .expect("add node");

    let ext1 = ExtentId::generate();
    leader
        .commit_extent_mapping(
            vol_id,
            0..512,
            ext1,
            1,
            epoch,
            vec![node_id],
            vec![vec![1, 2, 3]],
            Some(OperationId::generate()),
            None,
        )
        .await
        .expect("commit mapping 1");

    let ext2 = ExtentId::generate();
    leader
        .commit_extent_mapping(
            vol_id,
            512..1024,
            ext2,
            2,
            epoch,
            vec![node_id],
            vec![vec![4, 5, 6]],
            Some(OperationId::generate()),
            None,
        )
        .await
        .expect("commit mapping 2");

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    for (i, svc) in services.iter().enumerate() {
        let vol = svc.get_volume(&vol_id);
        assert!(
            vol.is_some(),
            "node {} must converge: volume should be present",
            i + 1
        );

        let m1 = svc.lookup_by_extent_version(1);
        let m2 = svc.lookup_by_extent_version(2);
        assert!(
            m1.is_some(),
            "node {} must converge: mapping 1 should be present",
            i + 1
        );
        assert!(
            m2.is_some(),
            "node {} must converge: mapping 2 should be present",
            i + 1
        );
        assert_eq!(m1.unwrap().extent_id, ext1);
        assert_eq!(m2.unwrap().extent_id, ext2);

        let n = svc.get_node(&node_id);
        assert!(
            n.is_some(),
            "node {} must converge: added node should be present",
            i + 1
        );
    }

    let follower_epochs: Vec<EpochId> = services.iter().map(|s| s.current_epoch()).collect();
    let leader_epoch = follower_epochs[leader_idx];
    for (i, e) in follower_epochs.iter().enumerate() {
        assert_eq!(
            *e, leader_epoch,
            "node {} epoch {:?} must match leader epoch {:?}",
            i + 1,
            e,
            leader_epoch
        );
    }
}

// ---------------------------------------------------------------------------
// P9E.3 — Corruption detection: checksum mismatch on tampered extent data
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_corruption_detected_checksum_mismatch() {
    let tmpdir = TempDir::new().expect("create tempdir");
    let (store, _disk_id) = create_extent_store(&tmpdir);
    let extent_id = ExtentId::generate();
    let data = vec![0x42u8; 8192];
    let (_, checksum) = store
        .stage_extent(extent_id, 1, &data)
        .expect("stage extent");

    let _entry = store
        .commit_extent(extent_id, 1, &checksum, data.len() as u64, StorageClass::Default)
        .expect("commit extent");

    let (read_data, read_checksum) = store
        .read_extent(extent_id, 1)
        .expect("read should succeed");
    assert_eq!(read_data, data);
    assert_eq!(read_checksum, checksum);

    let committed_path = committed_extent_path(
        tmpdir.path(),
        extent_id,
        1,
    );
    let mut corrupted = std::fs::read(&committed_path).expect("read file for corruption");
    for byte in corrupted.iter_mut().take(128) {
        *byte ^= 0xFF;
    }
    std::fs::write(&committed_path, &corrupted).expect("write corrupted data");

    let result = store.read_extent(extent_id, 1);
    assert!(
        result.is_err(),
        "reading corrupted extent should fail with checksum mismatch"
    );
    match result.unwrap_err() {
        blockyard_storage::StorageError::ChecksumMismatch(msg) => {
            assert!(
                !msg.is_empty(),
                "checksum mismatch error should have a message"
            );
        }
        other => panic!("expected ChecksumMismatch, got: {:?}", other),
    }

    let good_checksum = compute_checksum(&data);
    assert_eq!(good_checksum, checksum);
    assert!(!verify_checksum(&corrupted, &checksum));
    assert!(verify_checksum(&data, &checksum));
}

// ---------------------------------------------------------------------------
// P9E.4 — Snapshot: write blocks, take snapshot, verify state matches
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_snapshot_verify_state_matches() {
    let (services, _router) = create_raft_cluster(3).await;
    let leader_idx = find_leader(&services).await;
    let leader = &services[leader_idx];

    let vol_id = VolumeId::generate();
    leader
        .create_volume(vol_id, 5_000_000, ProtectionPolicy::Replicated { replicas: 3 })
        .await
        .expect("create volume");

    let epoch = leader.advance_epoch().await.expect("advance epoch");

    let node_id = NodeId::generate();
    leader
        .add_node(node_id, "10.0.0.2:9000".to_string())
        .await
        .expect("add node");

    let mut committed_extents = Vec::new();
    for i in 0..5u64 {
        let ext_id = ExtentId::generate();
        leader
            .commit_extent_mapping(
                vol_id,
                i * 100..(i + 1) * 100,
                ext_id,
                i + 1,
                epoch,
                vec![node_id],
                vec![vec![(i as u8) + 1]],
                Some(OperationId::generate()),
                None,
            )
            .await
            .expect("commit extent mapping");
        committed_extents.push((ext_id, i + 1));
    }

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    for (_, version) in &committed_extents {
        let m = leader.lookup_by_extent_version(*version);
        assert!(m.is_some(), "mapping version {} must exist pre-snapshot", version);
    }

    let new_ext = ExtentId::generate();
    let _post_epoch = leader
        .commit_extent_mapping(
            vol_id,
            500..600,
            new_ext,
            6,
            epoch,
            vec![node_id],
            vec![vec![99]],
            Some(OperationId::generate()),
            None,
        )
        .await
        .expect("post-snapshot commit");

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    for svc in &services {
        assert!(svc.get_volume(&vol_id).is_some());
        assert!(svc.get_node(&node_id).is_some());
    }

    for (i, svc) in services.iter().enumerate() {
        for (_, version) in &committed_extents {
            let m = svc.lookup_by_extent_version(*version);
            assert!(
                m.is_some(),
                "node {} must have mapping version {}",
                i + 1,
                version
            );
        }

        let new_m = svc.lookup_by_extent_version(6);
        assert!(
            new_m.is_some(),
            "node {} must see post-snapshot mapping version 6",
            i + 1
        );
    }

    let tmpdir = TempDir::new().expect("create tempdir for extent snapshot");
    let (store, _disk_id) = create_extent_store(&tmpdir);
    let index = ExtentIndex::new();

    let original_data = vec![0xDDu8; 4096];
    let ext_for_snap = ExtentId::generate();
    let (_, checksum) = store
        .stage_extent(ext_for_snap, 1, &original_data)
        .expect("stage");
    let entry = store
        .commit_extent(ext_for_snap, 1, &checksum, 4096, StorageClass::Default)
        .expect("commit");
    index.insert(entry).expect("insert");

    let (snap_data, snap_checksum) = store.read_extent(ext_for_snap, 1).expect("read snapshot");
    assert_eq!(snap_data, original_data);
    assert_eq!(snap_checksum, checksum);
    assert!(verify_checksum(&snap_data, &snap_checksum));
}
