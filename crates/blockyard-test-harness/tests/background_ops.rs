//! Cross-component background operations integration tests.

use blockyard_common::{DiskId, ExtentId};
use blockyard_storage::background::drain::DrainConfig;
use blockyard_storage::background::rate_limit::TokenBucket;
use blockyard_storage::background::rebalance::RebalanceConfig;
use blockyard_storage::background::repair::{
    RepairConfig, RepairRequest, RepairType, RepairWorker,
};
use blockyard_storage::background::scheduler::{BackgroundScheduler, SchedulerConfig};
use blockyard_storage::background::scrub::{
    CorruptionNotification, CorruptionReason, ScrubConfig, ScrubExtentEntry, ScrubWorker,
};
use blockyard_storage::extent::{committed_extent_path, compute_checksum};
use blockyard_storage::{ExtentStore, StorageClass};
use blockyard_test_harness::repair_testutil::{
    StubEcReconstructor, StubFragmentReader, TestRepairReader, TestRepairWriter,
};
use tempfile::TempDir;
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// Disk-backed ExtentReader (scrub-specific, not shared)
// ---------------------------------------------------------------------------

struct DiskBackedExtentReader {
    stores: Vec<(DiskId, ExtentStore, TempDir)>,
    extents: Vec<ScrubExtentEntry>,
}

impl DiskBackedExtentReader {
    fn new() -> Self {
        Self {
            stores: Vec::new(),
            extents: Vec::new(),
        }
    }

    fn add_disk(&mut self) -> (DiskId, &ExtentStore) {
        let tmpdir = TempDir::new().expect("create tempdir");
        let disk_id = DiskId::generate();
        let store = ExtentStore::new(tmpdir.path().to_path_buf(), disk_id);
        store.init_directories().expect("init directories");
        self.stores.push((disk_id, store, tmpdir));
        let entry = self.stores.last().unwrap();
        (entry.0, &entry.1)
    }

    fn register_extent(&mut self, entry: ScrubExtentEntry) {
        self.extents.push(entry);
    }

    fn tmpdir_for_disk(&self, disk_id: DiskId) -> Option<&TempDir> {
        self.stores
            .iter()
            .find(|(did, _, _)| *did == disk_id)
            .map(|(_, _, t)| t)
    }
}

impl blockyard_storage::background::scrub::ExtentReader for DiskBackedExtentReader {
    fn read_extent(
        &self,
        disk_id: DiskId,
        extent_id: ExtentId,
        version: u64,
    ) -> Result<(Vec<u8>, String), String> {
        let tmpdir = self
            .tmpdir_for_disk(disk_id)
            .ok_or_else(|| format!("no store for disk {disk_id}"))?;
        let path = committed_extent_path(tmpdir.path(), extent_id, version);
        let data = std::fs::read(&path).map_err(|e| format!("{e}"))?;
        let checksum = compute_checksum(&data);
        Ok((data, checksum))
    }

    fn list_extents(&self, disk_id: DiskId) -> Vec<ScrubExtentEntry> {
        self.extents
            .iter()
            .filter(|e| e.disk_id == disk_id)
            .cloned()
            .collect()
    }

    fn list_disks(&self) -> Vec<DiskId> {
        self.stores.iter().map(|(did, _, _)| *did).collect()
    }
}

// ---------------------------------------------------------------------------
// test_scrub_detects_corruption_triggers_repair
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_scrub_detects_corruption_triggers_repair() {
    let corrupt_eid = ExtentId::generate();
    let healthy_eid = ExtentId::generate();
    let corrupt_data = vec![0x42u8; 4096];
    let healthy_data = vec![0xBBu8; 4096];

    let mut reader = DiskBackedExtentReader::new();
    let (disk_id, store) = reader.add_disk();

    let (_, corrupt_checksum) = store
        .stage_extent(corrupt_eid, 1, &corrupt_data)
        .expect("stage corrupt extent");
    store
        .commit_extent(
            corrupt_eid,
            1,
            &corrupt_checksum,
            corrupt_data.len() as u64,
            StorageClass::Default,
        )
        .expect("commit corrupt extent");

    let (_, healthy_checksum) = store
        .stage_extent(healthy_eid, 1, &healthy_data)
        .expect("stage healthy extent");
    store
        .commit_extent(
            healthy_eid,
            1,
            &healthy_checksum,
            healthy_data.len() as u64,
            StorageClass::Default,
        )
        .expect("commit healthy extent");

    let tmpdir = reader.tmpdir_for_disk(disk_id).unwrap();
    let corrupt_path = committed_extent_path(tmpdir.path(), corrupt_eid, 1);
    let mut file_data = std::fs::read(&corrupt_path).expect("read committed file");
    for byte in file_data.iter_mut().take(128) {
        *byte ^= 0xFF;
    }
    std::fs::write(&corrupt_path, &file_data).expect("write corrupted data");

    reader.register_extent(ScrubExtentEntry {
        extent_id: corrupt_eid,
        disk_id,
        expected_checksum: corrupt_checksum.clone(),
        version: 1,
    });
    reader.register_extent(ScrubExtentEntry {
        extent_id: healthy_eid,
        disk_id,
        expected_checksum: healthy_checksum,
        version: 1,
    });

    let scrub_worker = ScrubWorker::new(ScrubConfig {
        interval_secs: 3600,
        tokens_per_extent: 1,
    });
    let rate_limiter = TokenBucket::new(1000, 1000);
    let (corruption_tx, mut corruption_rx) = mpsc::channel::<CorruptionNotification>(100);

    let results = scrub_worker
        .scrub_pass(&reader, &rate_limiter, &corruption_tx)
        .await;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].extents_checked, 2);
    assert_eq!(results[0].checksum_errors, 1);
    assert_eq!(results[0].read_errors, 0);

    let notification = corruption_rx
        .try_recv()
        .expect("should receive corruption notification");
    assert_eq!(notification.extent_id, corrupt_eid);
    assert_eq!(notification.disk_id, disk_id);

    match &notification.reason {
        CorruptionReason::ChecksumMismatch { expected, actual } => {
            assert_eq!(expected, &corrupt_checksum);
            let corrupted_checksum = compute_checksum(&file_data);
            assert_eq!(actual, &corrupted_checksum);
        }
        other => panic!("expected ChecksumMismatch, got {other:?}"),
    }

    assert!(
        corruption_rx.try_recv().is_err(),
        "healthy extent should not generate notification"
    );

    let healthy_source_tmpdir = TempDir::new().expect("create healthy source tmpdir");
    let healthy_source_id = DiskId::generate();
    let healthy_source_store =
        ExtentStore::new(healthy_source_tmpdir.path().to_path_buf(), healthy_source_id);
    healthy_source_store
        .init_directories()
        .expect("init healthy source dirs");
    let (_, src_checksum) = healthy_source_store
        .stage_extent(corrupt_eid, 1, &corrupt_data)
        .expect("stage on healthy source");
    healthy_source_store
        .commit_extent(
            corrupt_eid,
            1,
            &src_checksum,
            corrupt_data.len() as u64,
            StorageClass::Default,
        )
        .expect("commit on healthy source");

    let target_tmpdir = TempDir::new().expect("create target tmpdir");
    let target_disk_id = DiskId::generate();
    let target_store = ExtentStore::new(target_tmpdir.path().to_path_buf(), target_disk_id);
    target_store
        .init_directories()
        .expect("init target dirs");

    let repair_worker = RepairWorker::new(RepairConfig::default());

    repair_worker.enqueue(RepairRequest {
        extent_id: notification.extent_id,
        version: 1,
        target_disk_id,
        repair_type: RepairType::Replication {
            healthy_sources: vec![healthy_source_id],
        },
        priority: 50,
    });

    assert_eq!(repair_worker.queue_len(), 1);

    let mut repair_reader = TestRepairReader::new();
    repair_reader.add_store(healthy_source_id, healthy_source_store, healthy_source_tmpdir);
    let mut repair_writer = TestRepairWriter::new();
    repair_writer.add_store(target_disk_id, target_store, target_tmpdir);

    let outcome = repair_worker
        .process_next(&repair_reader, &StubFragmentReader, &repair_writer, &StubEcReconstructor, &rate_limiter)
        .await
        .expect("should process repair request");

    assert!(outcome.success, "repair should succeed");
    assert_eq!(outcome.extent_id, corrupt_eid);
    assert_eq!(outcome.target_disk_id, target_disk_id);

    let (repaired_data, repaired_checksum) = repair_writer
        .read_back(target_disk_id, corrupt_eid, 1)
        .expect("repaired extent should be readable from disk");
    assert_eq!(repaired_data, corrupt_data);
    assert_eq!(repaired_checksum, corrupt_checksum);

    assert_eq!(repair_worker.queue_len(), 0);
    assert_eq!(repair_worker.completed().len(), 1);
    assert!(repair_worker.completed()[0].success);
}

// ---------------------------------------------------------------------------
// test_scheduler_coordinates_workers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_scheduler_coordinates_workers() {
    let config = SchedulerConfig {
        total_budget_tokens_per_sec: 500,
        burst_capacity: 1000,
        scrub: ScrubConfig {
            interval_secs: 7200,
            tokens_per_extent: 2,
        },
        repair: RepairConfig {
            max_concurrent: 8,
            tokens_per_repair: 15,
        },
        rebalance: RebalanceConfig {
            imbalance_threshold: 0.15,
            max_moves_per_pass: 50,
            tokens_per_move: 10,
            check_interval_secs: 600,
        },
        drain: DrainConfig {
            tokens_per_relocate: 8,
            inter_relocate_delay_ms: 50,
        },
    };

    let scheduler = BackgroundScheduler::new(config);

    assert_eq!(scheduler.config().total_budget_tokens_per_sec, 500);
    assert_eq!(scheduler.config().burst_capacity, 1000);
    assert_eq!(scheduler.rate_limiter().capacity(), 1000);
    assert_eq!(scheduler.rate_limiter().refill_rate(), 500);

    let initial_tokens = scheduler.available_budget();
    assert!(
        initial_tokens > 0,
        "scheduler should start with available tokens"
    );

    assert_eq!(scheduler.scrub_worker().config().interval_secs, 7200);
    assert_eq!(scheduler.scrub_worker().config().tokens_per_extent, 2);
    assert!(scheduler.scrub_worker().last_results().is_empty());

    assert_eq!(scheduler.repair_worker().config().max_concurrent, 8);
    assert_eq!(scheduler.repair_worker().config().tokens_per_repair, 15);
    assert_eq!(scheduler.repair_worker().queue_len(), 0);

    assert!(
        (scheduler.rebalance_worker().config().imbalance_threshold - 0.15).abs() < f64::EPSILON
    );
    assert_eq!(scheduler.rebalance_worker().config().max_moves_per_pass, 50);
    assert!(scheduler.rebalance_worker().last_plan().is_none());

    assert_eq!(scheduler.drain_worker().config().tokens_per_relocate, 8);
    assert_eq!(scheduler.drain_worker().config().inter_relocate_delay_ms, 50);
    assert!(scheduler.drain_worker().progress().is_none());

    let status = scheduler.status();
    assert_eq!(status.repair_backlog, 0);
    assert!(!status.drain_active);
    assert_eq!(status.scrub_results_count, 0);
    assert!(!status.rebalance_planned);
    assert!(status.available_tokens > 0);

    scheduler.repair_worker().enqueue(RepairRequest {
        extent_id: ExtentId::generate(),
        version: 1,
        target_disk_id: DiskId::generate(),
        repair_type: RepairType::Replication {
            healthy_sources: vec![DiskId::generate()],
        },
        priority: 10,
    });
    scheduler.repair_worker().enqueue(RepairRequest {
        extent_id: ExtentId::generate(),
        version: 1,
        target_disk_id: DiskId::generate(),
        repair_type: RepairType::Replication {
            healthy_sources: vec![],
        },
        priority: 5,
    });

    let status2 = scheduler.status();
    assert_eq!(status2.repair_backlog, 2);

    assert!(scheduler.rate_limiter().try_acquire(100));
    let after_acquire = scheduler.available_budget();
    assert!(
        after_acquire < initial_tokens,
        "available budget should decrease after acquiring tokens"
    );

    let debug_str = format!("{:?}", scheduler);
    assert!(debug_str.contains("BackgroundScheduler"));

    let status_debug = format!("{:?}", status2);
    assert!(status_debug.contains("SchedulerStatus"));
}
