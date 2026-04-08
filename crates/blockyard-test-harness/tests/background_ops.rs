//! Cross-component background operations integration tests.
//!
//! Tests that exercise interactions between scrub, repair, and the
//! scheduler using real types from `blockyard_storage::background`.

use std::collections::HashMap;

use blockyard_common::{DiskId, ExtentId};
use blockyard_storage::background::drain::DrainConfig;
use blockyard_storage::background::rate_limit::TokenBucket;
use blockyard_storage::background::rebalance::RebalanceConfig;
use blockyard_storage::background::repair::{
    EcReconstructor, FragmentReader, RepairConfig, RepairExtentReader, RepairExtentWriter,
    RepairRequest, RepairType, RepairWorker,
};
use blockyard_storage::background::scheduler::{BackgroundScheduler, SchedulerConfig};
use blockyard_storage::background::scrub::{
    CorruptionNotification, CorruptionReason, ExtentReader, ScrubConfig, ScrubExtentEntry,
    ScrubWorker,
};
use bytes::Bytes;
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// Mock implementations
// ---------------------------------------------------------------------------

type ExtentReadResult = Result<(Vec<u8>, String), String>;

struct MockExtentReader {
    disks: Vec<DiskId>,
    extents: Vec<ScrubExtentEntry>,
    read_results: parking_lot::Mutex<HashMap<ExtentId, ExtentReadResult>>,
}

impl MockExtentReader {
    fn new() -> Self {
        Self {
            disks: Vec::new(),
            extents: Vec::new(),
            read_results: parking_lot::Mutex::new(HashMap::new()),
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

impl ExtentReader for MockExtentReader {
    fn read_extent(
        &self,
        _disk_id: DiskId,
        extent_id: ExtentId,
        _version: u64,
    ) -> Result<(Vec<u8>, String), String> {
        let results = self.read_results.lock();
        results
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

struct MockRepairReader {
    data: parking_lot::Mutex<HashMap<(DiskId, ExtentId), Bytes>>,
}

impl MockRepairReader {
    fn new() -> Self {
        Self {
            data: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    fn add(&self, disk: DiskId, extent: ExtentId, data: Vec<u8>) {
        self.data.lock().insert((disk, extent), Bytes::from(data));
    }
}

impl RepairExtentReader for MockRepairReader {
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

struct MockRepairWriter {
    written: parking_lot::Mutex<Vec<(DiskId, ExtentId, Vec<u8>)>>,
}

impl MockRepairWriter {
    fn new() -> Self {
        Self {
            written: parking_lot::Mutex::new(Vec::new()),
        }
    }

    fn written(&self) -> Vec<(DiskId, ExtentId, Vec<u8>)> {
        self.written.lock().clone()
    }
}

impl RepairExtentWriter for MockRepairWriter {
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

struct MockFragmentReader;

impl FragmentReader for MockFragmentReader {
    fn read_fragment(
        &self,
        _source_disk: DiskId,
        _extent_id: ExtentId,
        _fragment_index: usize,
    ) -> Result<Bytes, String> {
        Err("not used".into())
    }
}

struct MockEcReconstructor;

impl EcReconstructor for MockEcReconstructor {
    fn reconstruct(
        &self,
        _data_count: usize,
        _parity_count: usize,
        _fragments: Vec<Option<Bytes>>,
        _original_len: usize,
    ) -> Result<Bytes, String> {
        Err("not used".into())
    }
}

// ---------------------------------------------------------------------------
// test_scrub_detects_corruption_triggers_repair
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_scrub_detects_corruption_triggers_repair() {
    let disk_id = DiskId::generate();
    let healthy_source = DiskId::generate();
    let target_disk = DiskId::generate();
    let corrupt_eid = ExtentId::generate();
    let healthy_eid = ExtentId::generate();

    let reader = MockExtentReader::new()
        .with_disk(disk_id)
        .with_extent(
            ScrubExtentEntry {
                extent_id: corrupt_eid,
                disk_id,
                expected_checksum: "expected_abc".to_string(),
                version: 1,
            },
            Ok((vec![1, 2, 3], "actual_xyz".to_string())),
        )
        .with_extent(
            ScrubExtentEntry {
                extent_id: healthy_eid,
                disk_id,
                expected_checksum: "good_hash".to_string(),
                version: 1,
            },
            Ok((vec![4, 5, 6], "good_hash".to_string())),
        );

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
            assert_eq!(expected, "expected_abc");
            assert_eq!(actual, "actual_xyz");
        }
        other => panic!("expected ChecksumMismatch, got {other:?}"),
    }

    assert!(
        corruption_rx.try_recv().is_err(),
        "healthy extent should not generate notification"
    );

    let repair_worker = RepairWorker::new(RepairConfig::default());

    repair_worker.enqueue(RepairRequest {
        extent_id: notification.extent_id,
        version: 1,
        target_disk_id: target_disk,
        repair_type: RepairType::Replication {
            healthy_sources: vec![healthy_source],
        },
        priority: 50,
    });

    assert_eq!(repair_worker.queue_len(), 1);

    let repair_reader = MockRepairReader::new();
    repair_reader.add(healthy_source, notification.extent_id, vec![1, 2, 3]);
    let repair_writer = MockRepairWriter::new();
    let frag_reader = MockFragmentReader;
    let ec = MockEcReconstructor;

    let outcome = repair_worker
        .process_next(&repair_reader, &frag_reader, &repair_writer, &ec, &rate_limiter)
        .await
        .expect("should process repair request");

    assert!(outcome.success, "repair should succeed");
    assert_eq!(outcome.extent_id, corrupt_eid);
    assert_eq!(outcome.target_disk_id, target_disk);

    let written = repair_writer.written();
    assert_eq!(written.len(), 1);
    assert_eq!(written[0].0, target_disk);
    assert_eq!(written[0].1, corrupt_eid);
    assert_eq!(written[0].2, vec![1, 2, 3]);

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
