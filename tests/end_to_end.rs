//! End-to-end integration tests that chain all major components through REAL
//! filesystem I/O.  Every test uses `TempDir` + `ExtentStore` for data storage.

use std::sync::Arc;

use blockyard_common::{DiskId, ExtentId, NodeId, OperationId, ProtectionPolicy, VolumeId};
use blockyard_storage::background::drain::{
    DrainConfig, DrainExtentEntry, DrainInventory, DrainWorker,
};
use blockyard_storage::background::rate_limit::TokenBucket;
use blockyard_storage::background::repair::{RepairConfig, RepairType, RepairWorker};
use blockyard_storage::background::scrub::{CorruptionNotification, ScrubConfig, ScrubWorker};
use blockyard_storage::extent::{committed_extent_path, compute_checksum};
use blockyard_storage::{ExtentStore, StorageClass};
use blockyard_test_harness::mock_datanode::{
    create_test_extent_store, deterministic_data, write_test_extent,
};
use blockyard_test_harness::raft_testutil::{create_test_raft_cluster, find_leader};
use blockyard_test_harness::repair_testutil::{
    StubEcReconstructor, StubFragmentReader, TestExtentReader, TestRepairReader, TestRepairWriter,
};
use bytes::Bytes;
use tempfile::TempDir;
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// DrainInventory backed by real store
// ---------------------------------------------------------------------------

struct RealDrainInventory {
    source_disk: DiskId,
    target_disk: DiskId,
    entries: Vec<DrainExtentEntry>,
}

impl DrainInventory for RealDrainInventory {
    fn list_extents_on_disk(&self, _disk_id: DiskId) -> Vec<DrainExtentEntry> {
        self.entries.clone()
    }

    fn select_target_disk(&self, _exclude: DiskId) -> Result<DiskId, String> {
        Ok(self.target_disk)
    }

    fn transition_to_removed(&self, _disk_id: DiskId) -> Result<(), String> {
        Ok(())
    }

    fn is_draining(&self, disk_id: DiskId) -> bool {
        disk_id == self.source_disk
    }
}

// ===========================================================================
// Test 1: write → scrub → corrupt → scrub detects → repair → scrub clean
// ===========================================================================

#[tokio::test]
async fn test_write_scrub_corrupt_repair_chain() {
    let tmp1 = TempDir::new().expect("tmp1");
    let tmp2 = TempDir::new().expect("tmp2");
    let tmp3 = TempDir::new().expect("tmp3");

    let (store1, disk1) = create_test_extent_store(&tmp1);
    let (store2, disk2) = create_test_extent_store(&tmp2);
    let (store3, disk3) = create_test_extent_store(&tmp3);

    let mut all_entries: Vec<(DiskId, ExtentId, u64, String, Vec<u8>)> = Vec::new();

    let stores_and_disks = [(&store1, disk1), (&store2, disk2), (&store3, disk3)];
    for (store_idx, (store, disk_id)) in stores_and_disks.iter().enumerate() {
        for i in 0..10 {
            let global_idx = store_idx * 10 + i;
            let (eid, ver, checksum, data) = write_test_extent(store, global_idx, 1);
            all_entries.push((*disk_id, eid, ver, checksum, data));
        }
    }

    let mut reader = TestExtentReader::new(vec![
        (disk1, ExtentStore::new(tmp1.path().to_path_buf(), disk1)),
        (disk2, ExtentStore::new(tmp2.path().to_path_buf(), disk2)),
        (disk3, ExtentStore::new(tmp3.path().to_path_buf(), disk3)),
    ]);
    for (disk_id, eid, ver, checksum, _) in &all_entries {
        reader.register(*disk_id, *eid, *ver, checksum.clone());
    }

    let scrub = ScrubWorker::new(ScrubConfig::default());
    let limiter = TokenBucket::new(10000, 10000);
    let (tx, mut rx) = mpsc::channel::<CorruptionNotification>(100);

    let results = scrub.scrub_pass(&reader, &limiter, &tx).await;
    assert_eq!(
        results.iter().map(|r| r.extents_checked).sum::<u64>(),
        30,
        "should check all 30 extents"
    );
    assert_eq!(
        results
            .iter()
            .map(|r| r.checksum_errors + r.read_errors + r.metadata_errors)
            .sum::<u64>(),
        0,
        "should find 0 errors on clean data"
    );
    assert!(
        rx.try_recv().is_err(),
        "no corruption notifications expected"
    );

    let disk1_entries: Vec<_> = all_entries
        .iter()
        .filter(|(d, _, _, _, _)| *d == disk1)
        .collect();
    let corrupt_indices = [0usize, 3, 7];
    let mut corrupted_eids = Vec::new();
    for &ci in &corrupt_indices {
        let (_, eid, ver, _, _) = &disk1_entries[ci];
        let path = committed_extent_path(tmp1.path(), *eid, *ver);
        let mut data = std::fs::read(&path).expect("read for corruption");
        for byte in data.iter_mut().take(64) {
            *byte ^= 0xFF;
        }
        std::fs::write(&path, &data).expect("write corrupted data");
        corrupted_eids.push(*eid);
    }

    let (tx2, mut rx2) = mpsc::channel::<CorruptionNotification>(100);
    let results2 = scrub.scrub_pass(&reader, &limiter, &tx2).await;
    let total_checksum_errors: u64 = results2.iter().map(|r| r.checksum_errors).sum();
    assert_eq!(
        total_checksum_errors, 3,
        "should detect 3 corrupted extents"
    );

    let mut notifications = Vec::new();
    while let Ok(n) = rx2.try_recv() {
        notifications.push(n);
    }
    assert_eq!(
        notifications.len(),
        3,
        "should have 3 corruption notifications"
    );
    for n in &notifications {
        assert!(
            corrupted_eids.contains(&n.extent_id),
            "notification should reference a corrupted extent"
        );
    }

    // Repair writes back to disk1's original path so the final scrub sees repaired data
    struct InPlaceRepairWriter {
        stores: Vec<(DiskId, ExtentStore)>,
    }
    impl blockyard_storage::background::repair::RepairExtentWriter for InPlaceRepairWriter {
        fn write_extent(
            &self,
            target_disk: DiskId,
            extent_id: ExtentId,
            version: u64,
            data: &[u8],
        ) -> Result<(), String> {
            let store = self
                .stores
                .iter()
                .find(|(d, _)| *d == target_disk)
                .map(|(_, s)| s)
                .ok_or("target disk not found")?;
            let checksum = compute_checksum(data);
            let committed_path = committed_extent_path(store.mount_path(), extent_id, version);
            if committed_path.exists() {
                std::fs::remove_file(&committed_path).map_err(|e| format!("{e}"))?;
                let meta_path = blockyard_storage::extent::extent_meta_path(
                    store.mount_path(),
                    extent_id,
                    version,
                );
                if meta_path.exists() {
                    let _ = std::fs::remove_file(&meta_path);
                }
            }
            store
                .stage_extent(extent_id, version, data)
                .map_err(|e| format!("{e}"))?;
            store
                .commit_extent(
                    extent_id,
                    version,
                    &checksum,
                    data.len() as u64,
                    StorageClass::Default,
                )
                .map_err(|e| format!("{e}"))?;
            Ok(())
        }
    }

    let repair_writer = InPlaceRepairWriter {
        stores: vec![(disk1, ExtentStore::new(tmp1.path().to_path_buf(), disk1))],
    };

    let repair_worker = RepairWorker::new(RepairConfig {
        max_concurrent: 4,
        tokens_per_repair: 1,
    });

    struct OriginalDataReader {
        entries: Vec<(DiskId, ExtentId, u64, String, Vec<u8>)>,
    }
    impl blockyard_storage::background::repair::RepairExtentReader for OriginalDataReader {
        fn read_extent(
            &self,
            _source_disk: DiskId,
            extent_id: ExtentId,
            _version: u64,
        ) -> Result<Bytes, String> {
            self.entries
                .iter()
                .find(|(_, e, _, _, _)| *e == extent_id)
                .map(|(_, _, _, _, data)| Bytes::from(data.clone()))
                .ok_or_else(|| "extent not found in original data".into())
        }
    }

    let original_reader = OriginalDataReader {
        entries: all_entries.clone(),
    };

    for n in &notifications {
        repair_worker.enqueue(blockyard_storage::background::repair::RepairRequest {
            extent_id: n.extent_id,
            version: 1,
            target_disk_id: disk1,
            repair_type: RepairType::Replication {
                healthy_sources: vec![disk2],
            },
            priority: 0,
        });
    }

    let outcomes = repair_worker
        .process_all(
            &original_reader,
            &StubFragmentReader,
            &repair_writer,
            &StubEcReconstructor,
            &limiter,
        )
        .await;
    assert_eq!(outcomes.len(), 3);
    for outcome in &outcomes {
        assert!(
            outcome.success,
            "repair should succeed: {:?}",
            outcome.error
        );
    }

    let verify_store = ExtentStore::new(tmp1.path().to_path_buf(), disk1);
    for &eid in &corrupted_eids {
        let (_, _, _, _, original_data) = all_entries
            .iter()
            .find(|(_, e, _, _, _)| *e == eid)
            .expect("find original");
        let (read_data, _) = verify_store
            .read_extent(eid, 1)
            .expect("read repaired extent");
        assert_eq!(
            &read_data, original_data,
            "repaired data should match original"
        );
    }

    let mut final_reader = TestExtentReader::new(vec![
        (disk1, ExtentStore::new(tmp1.path().to_path_buf(), disk1)),
        (disk2, ExtentStore::new(tmp2.path().to_path_buf(), disk2)),
        (disk3, ExtentStore::new(tmp3.path().to_path_buf(), disk3)),
    ]);
    for (disk_id, eid, ver, checksum, _) in &all_entries {
        final_reader.register(*disk_id, *eid, *ver, checksum.clone());
    }

    let (tx3, _rx3) = mpsc::channel::<CorruptionNotification>(100);
    let final_results = scrub.scrub_pass(&final_reader, &limiter, &tx3).await;
    let final_errors: u64 = final_results
        .iter()
        .map(|r| r.checksum_errors + r.read_errors + r.metadata_errors)
        .sum();
    assert_eq!(
        final_errors, 0,
        "final scrub should show 0 errors after repair"
    );
}

// ===========================================================================
// Test 2: Raft metadata commit + extent write + leader failover
// ===========================================================================

#[tokio::test]
async fn test_metadata_commit_then_extent_write() {
    let cluster = create_test_raft_cluster(3).await;
    let leader_idx = find_leader(&cluster).await;
    let leader = &cluster.services[leader_idx];

    let tmpdir = TempDir::new().expect("tmpdir");
    let (store, _disk_id) = create_test_extent_store(&tmpdir);

    let vol_id = VolumeId::generate();
    leader
        .create_volume(
            vol_id,
            10_000_000,
            ProtectionPolicy::Replicated { replicas: 3 },
        )
        .await
        .expect("create volume");

    let epoch = leader.advance_epoch().await.expect("advance epoch");
    let node_id = NodeId::generate();
    leader
        .add_node(node_id, "10.0.0.1:9000".to_string())
        .await
        .expect("add node");

    let extent_id = ExtentId::generate();
    let data = deterministic_data(42, 8192);
    let (_staged, checksum) = store
        .stage_extent(extent_id, 1, &data)
        .expect("stage extent");
    store
        .commit_extent(
            extent_id,
            1,
            &checksum,
            data.len() as u64,
            StorageClass::Default,
        )
        .expect("commit extent");

    let committed_epoch = leader
        .commit_extent_mapping(
            vol_id,
            0..1024,
            extent_id,
            1,
            epoch,
            vec![node_id],
            vec![checksum.as_bytes().to_vec()],
            Some(OperationId::generate()),
            None,
        )
        .await
        .expect("commit mapping");

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let (read_data, read_checksum) = store.read_extent(extent_id, 1).expect("read extent");
    assert_eq!(read_data, data);
    assert_eq!(read_checksum, checksum);

    let mapping = leader.lookup_by_extent_version(1);
    assert!(mapping.is_some(), "mapping should be committed");
    assert_eq!(mapping.as_ref().unwrap().extent_id, extent_id);

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
    let new_leader_idx = new_leader_idx.expect("new leader should be elected");
    let new_leader = &cluster.services[new_leader_idx];

    let mapping = new_leader.lookup_by_extent_version(1);
    assert!(mapping.is_some(), "mapping must survive leader failover");
    assert_eq!(mapping.unwrap().extent_id, extent_id);

    let vol = new_leader.get_volume(&vol_id);
    assert!(vol.is_some(), "volume must survive leader failover");

    let (final_data, final_checksum) = store
        .read_extent(extent_id, 1)
        .expect("read after failover");
    assert_eq!(final_data, data);
    assert_eq!(final_checksum, checksum);

    let ext2 = ExtentId::generate();
    let result = new_leader
        .commit_extent_mapping(
            vol_id,
            1024..2048,
            ext2,
            2,
            committed_epoch,
            vec![node_id],
            vec![vec![7, 8, 9]],
            Some(OperationId::generate()),
            None,
        )
        .await;
    assert!(
        result.is_ok(),
        "new leader must accept writes: {:?}",
        result.err()
    );
}

// ===========================================================================
// Test 3: drain relocates real extents from disk1 to disk2
// ===========================================================================

#[tokio::test]
async fn test_drain_relocates_real_extents() {
    let tmp1 = TempDir::new().expect("tmp1");
    let tmp2 = TempDir::new().expect("tmp2");

    let (store1, disk1) = create_test_extent_store(&tmp1);
    let (_store2, disk2) = create_test_extent_store(&tmp2);

    let mut written_extents: Vec<(ExtentId, u64, String, Vec<u8>)> = Vec::new();
    for i in 0..5 {
        let (eid, ver, checksum, data) = write_test_extent(&store1, i, 1);
        written_extents.push((eid, ver, checksum, data));
    }

    let drain_entries: Vec<DrainExtentEntry> = written_extents
        .iter()
        .map(|(eid, ver, _, _)| DrainExtentEntry {
            extent_id: *eid,
            version: *ver,
            healthy_sources: vec![disk1],
        })
        .collect();

    let inventory = RealDrainInventory {
        source_disk: disk1,
        target_disk: disk2,
        entries: drain_entries,
    };

    let drain_worker = DrainWorker::new(DrainConfig {
        tokens_per_relocate: 1,
        inter_relocate_delay_ms: 0,
    });
    let repair_worker = RepairWorker::new(RepairConfig {
        max_concurrent: 4,
        tokens_per_repair: 1,
    });
    let limiter = TokenBucket::new(10000, 10000);

    let progress = drain_worker
        .drain_disk(disk1, &inventory, &repair_worker, &limiter)
        .await;
    assert_eq!(progress.total_extents, 5);
    assert_eq!(progress.relocated, 5);
    assert!(progress.complete);

    let mut repair_reader = TestRepairReader::new();
    repair_reader.add_store(
        disk1,
        ExtentStore::new(tmp1.path().to_path_buf(), disk1),
        tmp1,
    );
    let mut repair_writer = TestRepairWriter::new();
    repair_writer.add_store(
        disk2,
        ExtentStore::new(tmp2.path().to_path_buf(), disk2),
        tmp2,
    );

    let outcomes = repair_worker
        .process_all(
            &repair_reader,
            &StubFragmentReader,
            &repair_writer,
            &StubEcReconstructor,
            &limiter,
        )
        .await;
    assert_eq!(outcomes.len(), 5);
    for outcome in &outcomes {
        assert!(
            outcome.success,
            "drain repair should succeed: {:?}",
            outcome.error
        );
    }

    for (eid, ver, original_checksum, original_data) in &written_extents {
        let (data, checksum) = repair_writer
            .read_back(disk2, *eid, *ver)
            .expect("read drained extent from disk2");
        assert_eq!(&data, original_data, "data should match original");
        assert_eq!(
            &checksum, original_checksum,
            "checksum should match original"
        );
    }
}

// ===========================================================================
// Test 4: concurrent writes + periodic scrub
// ===========================================================================

#[tokio::test]
async fn test_concurrent_writes_and_scrub() {
    let tmpdir = TempDir::new().expect("tmpdir");
    let disk_id = DiskId::generate();

    let mount_path = tmpdir.path().to_path_buf();
    {
        let init_store = ExtentStore::new(mount_path.clone(), disk_id);
        init_store.init_directories().expect("init dirs");
    }

    let extent_infos: Vec<(ExtentId, Vec<u8>)> = (0..100)
        .map(|i| {
            let eid = ExtentId::generate();
            let data = deterministic_data(i, 4096);
            (eid, data)
        })
        .collect();

    let extent_infos = Arc::new(extent_infos);
    let checksums: Arc<parking_lot::Mutex<Vec<(ExtentId, u64, String)>>> =
        Arc::new(parking_lot::Mutex::new(Vec::new()));

    let mut handles = Vec::new();
    for i in 0..100 {
        let mp = mount_path.clone();
        let infos = Arc::clone(&extent_infos);
        let cs = Arc::clone(&checksums);

        handles.push(tokio::spawn(async move {
            let store = ExtentStore::new(mp, disk_id);
            let (eid, ref data) = infos[i];
            let version = (i as u64) + 1;

            let (_staged, checksum) = store
                .stage_extent(eid, version, data)
                .expect("stage in concurrent write");
            store
                .commit_extent(
                    eid,
                    version,
                    &checksum,
                    data.len() as u64,
                    StorageClass::Default,
                )
                .expect("commit in concurrent write");

            cs.lock().push((eid, version, checksum));
        }));
    }

    let scrub_mp = mount_path.clone();
    let scrub_cs = Arc::clone(&checksums);
    let scrub_handle = tokio::spawn(async move {
        use blockyard_storage::background::scrub::{ExtentReader, ScrubExtentEntry};

        let scrub_worker = ScrubWorker::new(ScrubConfig::default());
        let limiter = TokenBucket::new(100000, 100000);
        let mut total_scrubbed = 0u64;

        for _ in 0..3 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;

            let current_entries: Vec<(ExtentId, u64, String)> = scrub_cs.lock().clone();
            if current_entries.is_empty() {
                continue;
            }

            struct SnapshotReader {
                disk_id: DiskId,
                mount_path: std::path::PathBuf,
                entries: Vec<(ExtentId, u64, String)>,
            }

            impl ExtentReader for SnapshotReader {
                fn read_extent(
                    &self,
                    _disk_id: DiskId,
                    extent_id: ExtentId,
                    version: u64,
                ) -> Result<(Vec<u8>, String), String> {
                    let path = committed_extent_path(&self.mount_path, extent_id, version);
                    let data = std::fs::read(&path).map_err(|e| format!("{e}"))?;
                    let cs = compute_checksum(&data);
                    Ok((data, cs))
                }

                fn list_extents(&self, _disk_id: DiskId) -> Vec<ScrubExtentEntry> {
                    self.entries
                        .iter()
                        .map(|(eid, ver, cs)| ScrubExtentEntry {
                            extent_id: *eid,
                            disk_id: self.disk_id,
                            expected_checksum: cs.clone(),
                            version: *ver,
                        })
                        .collect()
                }

                fn list_disks(&self) -> Vec<DiskId> {
                    vec![self.disk_id]
                }
            }

            let snapshot_reader = SnapshotReader {
                disk_id,
                mount_path: scrub_mp.clone(),
                entries: current_entries,
            };

            let (tx, _rx) = mpsc::channel(1000);
            let results = scrub_worker
                .scrub_pass(&snapshot_reader, &limiter, &tx)
                .await;
            for r in &results {
                total_scrubbed += r.extents_checked;
                assert_eq!(
                    r.checksum_errors, 0,
                    "no checksum errors expected during concurrent writes"
                );
            }
        }
        total_scrubbed
    });

    for h in handles {
        h.await.expect("write task should not panic");
    }

    let total_scrubbed = scrub_handle.await.expect("scrub task should not panic");
    assert!(total_scrubbed > 0, "scrub should have checked some extents");

    let final_store = ExtentStore::new(mount_path, disk_id);
    let final_entries = checksums.lock().clone();
    assert_eq!(
        final_entries.len(),
        100,
        "all 100 extents should be committed"
    );

    for (i, (eid, ver, expected_checksum)) in final_entries.iter().enumerate() {
        let (data, actual_checksum) = final_store
            .read_extent(*eid, *ver)
            .unwrap_or_else(|e| panic!("read extent {i} failed: {e}"));
        assert_eq!(
            &actual_checksum, expected_checksum,
            "checksum mismatch for extent {i}"
        );
        let expected_data = deterministic_data(i, 4096);
        assert_eq!(data, expected_data, "data mismatch for extent {i}");
    }
}
