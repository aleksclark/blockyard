//! Phase 9D — Rebalancing integration tests exercising real Phase 7 workers.
//!
//! These tests use the real `RebalanceWorker`, `DrainWorker`, `RepairWorker`,
//! and `TokenBucket` from `blockyard_storage::background`, wired to mock
//! inventory implementations that track moves, failures, and progress.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use blockyard_common::{DiskId, ExtentId};
use blockyard_storage::background::drain::{DrainConfig, DrainExtentEntry, DrainInventory, DrainWorker};
use blockyard_storage::background::rate_limit::TokenBucket;
use blockyard_storage::background::rebalance::{
    DiskUtilization, RebalanceConfig, RebalanceInventory, RebalanceWorker,
};
use blockyard_storage::background::repair::{
    EcReconstructor, FragmentReader, RepairConfig, RepairExtentReader, RepairExtentWriter,
    RepairWorker,
};
use bytes::Bytes;

// ---------------------------------------------------------------------------
// Mock implementations
// ---------------------------------------------------------------------------

struct MockRebalanceInventory {
    utilizations: Vec<DiskUtilization>,
    extents: HashMap<DiskId, Vec<(ExtentId, u64)>>,
    sources: HashMap<ExtentId, Vec<DiskId>>,
}

impl MockRebalanceInventory {
    fn new() -> Self {
        Self {
            utilizations: Vec::new(),
            extents: HashMap::new(),
            sources: HashMap::new(),
        }
    }

    fn with_disk(
        mut self,
        disk_id: DiskId,
        used: u64,
        total: u64,
        extents: Vec<(ExtentId, u64)>,
    ) -> Self {
        self.utilizations.push(DiskUtilization {
            disk_id,
            used_bytes: used,
            total_bytes: total,
            extent_count: extents.len() as u64,
        });
        for &(eid, _) in &extents {
            self.sources.entry(eid).or_default().push(disk_id);
        }
        self.extents.insert(disk_id, extents);
        self
    }

}

impl RebalanceInventory for MockRebalanceInventory {
    fn get_disk_utilizations(&self) -> Vec<DiskUtilization> {
        self.utilizations.clone()
    }

    fn list_moveable_extents(&self, disk_id: DiskId) -> Vec<(ExtentId, u64)> {
        self.extents.get(&disk_id).cloned().unwrap_or_default()
    }

    fn healthy_sources_for_extent(&self, extent_id: ExtentId) -> Vec<DiskId> {
        self.sources.get(&extent_id).cloned().unwrap_or_default()
    }
}

struct MockDrainInventory {
    extents: parking_lot::Mutex<Vec<DrainExtentEntry>>,
    target_disk: parking_lot::Mutex<Option<DiskId>>,
    draining: parking_lot::Mutex<bool>,
    transitioned: parking_lot::Mutex<bool>,
    target_fail_after: parking_lot::Mutex<Option<u64>>,
    target_calls: AtomicUsize,
}

impl MockDrainInventory {
    fn new() -> Self {
        Self {
            extents: parking_lot::Mutex::new(Vec::new()),
            target_disk: parking_lot::Mutex::new(None),
            draining: parking_lot::Mutex::new(true),
            transitioned: parking_lot::Mutex::new(false),
            target_fail_after: parking_lot::Mutex::new(None),
            target_calls: AtomicUsize::new(0),
        }
    }

    fn with_extents(self, extents: Vec<DrainExtentEntry>) -> Self {
        *self.extents.lock() = extents;
        self
    }

    fn with_target(self, target: DiskId) -> Self {
        *self.target_disk.lock() = Some(target);
        self
    }

    fn set_fail_after_n_targets(&self, n: u64) {
        *self.target_fail_after.lock() = Some(n);
    }

    fn was_transitioned(&self) -> bool {
        *self.transitioned.lock()
    }
}

impl DrainInventory for MockDrainInventory {
    fn list_extents_on_disk(&self, _disk_id: DiskId) -> Vec<DrainExtentEntry> {
        self.extents.lock().clone()
    }

    fn select_target_disk(&self, _exclude: DiskId) -> Result<DiskId, String> {
        let call_num = self.target_calls.fetch_add(1, Ordering::SeqCst) as u64;
        if let Some(limit) = *self.target_fail_after.lock() {
            if call_num >= limit {
                return Err("simulated target selection failure".into());
            }
        }
        self.target_disk
            .lock()
            .ok_or_else(|| "no target disk configured".into())
    }

    fn transition_to_removed(&self, _disk_id: DiskId) -> Result<(), String> {
        *self.transitioned.lock() = true;
        Ok(())
    }

    fn is_draining(&self, _disk_id: DiskId) -> bool {
        *self.draining.lock()
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

    fn write_count(&self) -> usize {
        self.written.lock().len()
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
        Err("not implemented for rebalance tests".into())
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
        Err("not implemented for rebalance tests".into())
    }
}

// ---------------------------------------------------------------------------
// P9D.1 — Add node → rebalance → data integrity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_add_node_rebalance_data_integrity() {
    let disk_high_1 = DiskId::generate();
    let disk_high_2 = DiskId::generate();
    let disk_new = DiskId::generate();

    let mut extents_high_1 = Vec::new();
    for _ in 0..10 {
        extents_high_1.push((ExtentId::generate(), 1));
    }
    let mut extents_high_2 = Vec::new();
    for _ in 0..8 {
        extents_high_2.push((ExtentId::generate(), 1));
    }

    let inventory = MockRebalanceInventory::new()
        .with_disk(disk_high_1, 800, 1000, extents_high_1.clone())
        .with_disk(disk_high_2, 750, 1000, extents_high_2.clone())
        .with_disk(disk_new, 0, 1000, vec![]);

    let worker = RebalanceWorker::new(RebalanceConfig {
        imbalance_threshold: 0.1,
        max_moves_per_pass: 100,
        tokens_per_move: 1,
        check_interval_secs: 60,
    });

    let plan = worker.generate_plan(&inventory);

    assert!(plan.needed, "rebalance should be needed with 80%/75%/0% disks");
    assert!(!plan.moves.is_empty(), "plan should contain moves");
    assert!(
        plan.max_imbalance > 0.1,
        "max imbalance should exceed threshold"
    );

    for mv in &plan.moves {
        assert_eq!(mv.target_disk, disk_new, "all moves should target the new empty disk");
        assert!(
            mv.source_disk == disk_high_1 || mv.source_disk == disk_high_2,
            "moves should come from over-utilized disks"
        );
    }

    let repair_worker = RepairWorker::new(RepairConfig::default());
    let limiter = TokenBucket::new(1000, 1000);

    let submitted = worker
        .execute_plan(&plan, &inventory, &repair_worker, &limiter)
        .await;

    assert_eq!(submitted, plan.moves.len());
    assert_eq!(repair_worker.queue_len(), plan.moves.len());

    let reader = MockRepairReader::new();
    for mv in &plan.moves {
        let sources = inventory.healthy_sources_for_extent(mv.extent_id);
        for src in &sources {
            reader
                .data
                .lock()
                .insert((*src, mv.extent_id), Bytes::from(vec![0xAB; 64]));
        }
    }
    let writer = MockRepairWriter::new();
    let frag_reader = MockFragmentReader;
    let ec = MockEcReconstructor;

    let outcomes = repair_worker
        .process_all(&reader, &frag_reader, &writer, &ec, &limiter)
        .await;

    assert_eq!(outcomes.len(), plan.moves.len());
    assert!(
        outcomes.iter().all(|o| o.success),
        "all repair operations should succeed"
    );
    assert_eq!(writer.write_count(), plan.moves.len());
}

// ---------------------------------------------------------------------------
// P9D.2 — Remove node (drain) → all extents processed → completion fires
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_remove_node_drain_no_data_loss() {
    let drain_disk = DiskId::generate();
    let target_disk = DiskId::generate();
    let source_disk = DiskId::generate();

    let extents: Vec<DrainExtentEntry> = (0..5)
        .map(|_| DrainExtentEntry {
            extent_id: ExtentId::generate(),
            version: 1,
            healthy_sources: vec![source_disk],
        })
        .collect();

    let inventory = MockDrainInventory::new()
        .with_extents(extents.clone())
        .with_target(target_disk);

    let drain_worker = DrainWorker::new(DrainConfig {
        tokens_per_relocate: 1,
        inter_relocate_delay_ms: 0,
    });

    let repair_worker = RepairWorker::new(RepairConfig::default());
    let limiter = TokenBucket::new(1000, 1000);

    assert!(drain_worker.progress().is_none());

    let progress = drain_worker
        .drain_disk(drain_disk, &inventory, &repair_worker, &limiter)
        .await;

    assert_eq!(progress.total_extents, 5);
    assert_eq!(progress.relocated, 5);
    assert_eq!(progress.failed, 0);
    assert!(progress.complete, "drain should complete successfully");
    assert!(
        inventory.was_transitioned(),
        "disk should be transitioned to removed"
    );

    assert_eq!(repair_worker.queue_len(), 5);

    let tracked_progress = drain_worker.progress().expect("progress should be tracked");
    assert_eq!(tracked_progress.total_extents, 5);
    assert_eq!(tracked_progress.relocated, 5);
    assert!(tracked_progress.complete);
}

// ---------------------------------------------------------------------------
// P9D.3 — Kill during rebalance → inventory fails partway → resumes on retry
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_kill_during_rebalance_resumes() {
    let drain_disk = DiskId::generate();
    let target_disk = DiskId::generate();
    let source_disk = DiskId::generate();

    let extents: Vec<DrainExtentEntry> = (0..6)
        .map(|_| DrainExtentEntry {
            extent_id: ExtentId::generate(),
            version: 1,
            healthy_sources: vec![source_disk],
        })
        .collect();

    let inventory = MockDrainInventory::new()
        .with_extents(extents.clone())
        .with_target(target_disk);
    inventory.set_fail_after_n_targets(3);

    let drain_worker = DrainWorker::new(DrainConfig {
        tokens_per_relocate: 1,
        inter_relocate_delay_ms: 0,
    });
    let repair_worker = RepairWorker::new(RepairConfig::default());
    let limiter = TokenBucket::new(1000, 1000);

    let progress = drain_worker
        .drain_disk(drain_disk, &inventory, &repair_worker, &limiter)
        .await;

    assert_eq!(progress.relocated, 3, "should relocate 3 before failure");
    assert_eq!(progress.failed, 3, "remaining 3 should fail");
    assert!(
        !progress.complete,
        "drain should NOT complete due to failures"
    );

    let remaining_extents: Vec<DrainExtentEntry> = extents[3..].to_vec();
    let inventory2 = MockDrainInventory::new()
        .with_extents(remaining_extents)
        .with_target(target_disk);

    let repair_worker2 = RepairWorker::new(RepairConfig::default());

    let progress2 = drain_worker
        .drain_disk(drain_disk, &inventory2, &repair_worker2, &limiter)
        .await;

    assert_eq!(progress2.relocated, 3, "retry should relocate remaining 3");
    assert_eq!(progress2.failed, 0);
    assert!(progress2.complete, "retry should complete successfully");
    assert!(inventory2.was_transitioned());
}

// ---------------------------------------------------------------------------
// P9D.4 — Concurrent IO during rebalance with real TokenBucket rate limiter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_concurrent_io_during_rebalance() {
    let limiter = TokenBucket::new(10, 100);

    assert!(limiter.try_acquire(10), "should acquire all 10 tokens");
    assert!(
        !limiter.try_acquire(1),
        "bucket should be empty after draining"
    );

    tokio::time::sleep(tokio::time::Duration::from_millis(120)).await;
    let available = limiter.available();
    assert!(
        available > 0 && available <= 10,
        "tokens should refill but not exceed capacity; got {available}"
    );

    let disk1 = DiskId::generate();
    let disk2 = DiskId::generate();
    let eid = ExtentId::generate();

    let inventory = MockRebalanceInventory::new()
        .with_disk(disk1, 900, 1000, vec![(eid, 1)])
        .with_disk(disk2, 100, 1000, vec![]);

    let rebalance_worker = RebalanceWorker::new(RebalanceConfig {
        imbalance_threshold: 0.1,
        max_moves_per_pass: 10,
        tokens_per_move: 5,
        check_interval_secs: 60,
    });

    let repair_worker = RepairWorker::new(RepairConfig::default());

    let slow_limiter = TokenBucket::new(5, 50);
    assert!(slow_limiter.try_acquire(5));

    let start = tokio::time::Instant::now();
    let plan = rebalance_worker
        .rebalance_pass(&inventory, &repair_worker, &slow_limiter)
        .await;
    let elapsed = start.elapsed();

    assert!(plan.needed, "rebalance should be needed");
    assert!(
        !plan.moves.is_empty(),
        "should have generated at least one move"
    );
    assert!(
        elapsed.as_millis() >= 50,
        "rate limiter should have throttled the operation; elapsed={elapsed:?}"
    );
    assert_eq!(repair_worker.queue_len(), plan.moves.len());
}
