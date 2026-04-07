//! Capacity rebalancing across disks (P7.6, §5.13).
//!
//! Monitors disk utilization, generates a rebalance plan, and moves
//! extents gradually to equalize usage.

use blockyard_common::{DiskId, ExtentId};
use tracing::{debug, info};

use crate::background::rate_limit::TokenBucket;
use crate::background::repair::{RepairRequest, RepairType, RepairWorker};

/// Configuration for the rebalance worker.
#[derive(Debug, Clone)]
pub struct RebalanceConfig {
    /// Maximum utilization difference (0.0-1.0) before triggering rebalance.
    pub imbalance_threshold: f64,
    /// Maximum number of extents to move per rebalance pass.
    pub max_moves_per_pass: usize,
    /// Tokens consumed per extent move.
    pub tokens_per_move: u64,
    /// Interval between rebalance checks in seconds.
    pub check_interval_secs: u64,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            imbalance_threshold: 0.1,
            max_moves_per_pass: 100,
            tokens_per_move: 5,
            check_interval_secs: 300,
        }
    }
}

/// Disk utilization information.
#[derive(Debug, Clone)]
pub struct DiskUtilization {
    pub disk_id: DiskId,
    /// Used space in bytes.
    pub used_bytes: u64,
    /// Total space in bytes.
    pub total_bytes: u64,
    /// Number of extents on this disk.
    pub extent_count: u64,
}

impl DiskUtilization {
    /// Utilization fraction (0.0 to 1.0).
    pub fn utilization(&self) -> f64 {
        if self.total_bytes == 0 {
            return 0.0;
        }
        self.used_bytes as f64 / self.total_bytes as f64
    }
}

/// An extent to be moved during rebalance.
#[derive(Debug, Clone)]
pub struct RebalanceMove {
    pub extent_id: ExtentId,
    pub version: u64,
    pub source_disk: DiskId,
    pub target_disk: DiskId,
}

/// A plan for rebalancing extents across disks.
#[derive(Debug, Clone)]
pub struct RebalancePlan {
    /// Moves to be executed.
    pub moves: Vec<RebalanceMove>,
    /// Whether rebalance is needed.
    pub needed: bool,
    /// Maximum utilization difference observed.
    pub max_imbalance: f64,
}

/// Trait for querying disk utilization and extents (testability).
pub trait RebalanceInventory: Send + Sync {
    /// Get utilization for all disks.
    fn get_disk_utilizations(&self) -> Vec<DiskUtilization>;

    /// List moveable extents on a disk (returns extent_id, version pairs).
    fn list_moveable_extents(&self, disk_id: DiskId) -> Vec<(ExtentId, u64)>;

    /// Get healthy source disks for an extent (for replication repair).
    fn healthy_sources_for_extent(&self, extent_id: ExtentId) -> Vec<DiskId>;
}

/// Background rebalance worker (P7.6).
///
/// Monitors disk utilization and moves extents from over-utilized
/// to under-utilized disks.
#[derive(Debug)]
pub struct RebalanceWorker {
    config: RebalanceConfig,
    last_plan: parking_lot::Mutex<Option<RebalancePlan>>,
}

impl RebalanceWorker {
    /// Create a new rebalance worker.
    pub fn new(config: RebalanceConfig) -> Self {
        Self {
            config,
            last_plan: parking_lot::Mutex::new(None),
        }
    }

    /// Get configuration.
    pub fn config(&self) -> &RebalanceConfig {
        &self.config
    }

    /// Get the last generated plan.
    pub fn last_plan(&self) -> Option<RebalancePlan> {
        self.last_plan.lock().clone()
    }

    /// Generate a rebalance plan based on current disk utilization.
    pub fn generate_plan(&self, inventory: &dyn RebalanceInventory) -> RebalancePlan {
        let utilizations = inventory.get_disk_utilizations();

        if utilizations.is_empty() {
            let plan = RebalancePlan {
                moves: vec![],
                needed: false,
                max_imbalance: 0.0,
            };
            *self.last_plan.lock() = Some(plan.clone());
            return plan;
        }

        let avg_utilization: f64 = {
            let total_used: f64 = utilizations.iter().map(|d| d.used_bytes as f64).sum();
            let total_capacity: f64 = utilizations.iter().map(|d| d.total_bytes as f64).sum();
            if total_capacity == 0.0 {
                0.0
            } else {
                total_used / total_capacity
            }
        };

        // Find max imbalance
        let max_imbalance = utilizations
            .iter()
            .map(|d| (d.utilization() - avg_utilization).abs())
            .fold(0.0f64, f64::max);

        if max_imbalance < self.config.imbalance_threshold {
            debug!(
                max_imbalance = max_imbalance,
                threshold = self.config.imbalance_threshold,
                "no rebalance needed"
            );
            let plan = RebalancePlan {
                moves: vec![],
                needed: false,
                max_imbalance,
            };
            *self.last_plan.lock() = Some(plan.clone());
            return plan;
        }

        // Sort disks by utilization: over-utilized first (sources), under-utilized last (targets)
        let mut by_util: Vec<_> = utilizations.clone();
        by_util.sort_by(|a, b| {
            b.utilization()
                .partial_cmp(&a.utilization())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Build a map of utilization diffs from average
        let mut over_utilized: Vec<&DiskUtilization> = Vec::new();
        let mut under_utilized: Vec<&DiskUtilization> = Vec::new();

        for d in &by_util {
            let diff = d.utilization() - avg_utilization;
            if diff > self.config.imbalance_threshold / 2.0 {
                over_utilized.push(d);
            } else if diff < -self.config.imbalance_threshold / 2.0 {
                under_utilized.push(d);
            }
        }

        let mut moves = Vec::new();
        let mut remaining_moves = self.config.max_moves_per_pass;

        for source in &over_utilized {
            if remaining_moves == 0 {
                break;
            }

            let extent_list = inventory.list_moveable_extents(source.disk_id);

            for (eid, version) in &extent_list {
                if remaining_moves == 0 {
                    break;
                }

                if let Some(target) = under_utilized.first() {
                    moves.push(RebalanceMove {
                        extent_id: *eid,
                        version: *version,
                        source_disk: source.disk_id,
                        target_disk: target.disk_id,
                    });
                    remaining_moves -= 1;
                }
            }
        }

        let needed = !moves.is_empty();
        let plan = RebalancePlan {
            moves,
            needed,
            max_imbalance,
        };

        info!(
            move_count = plan.moves.len(),
            max_imbalance = max_imbalance,
            avg_utilization = avg_utilization,
            "rebalance plan generated"
        );

        *self.last_plan.lock() = Some(plan.clone());
        plan
    }

    /// Execute a rebalance plan by submitting moves to the repair worker.
    pub async fn execute_plan(
        &self,
        plan: &RebalancePlan,
        inventory: &dyn RebalanceInventory,
        repair_worker: &RepairWorker,
        rate_limiter: &TokenBucket,
    ) -> usize {
        let mut submitted = 0;

        for mv in &plan.moves {
            rate_limiter.acquire(self.config.tokens_per_move).await;

            let sources = inventory.healthy_sources_for_extent(mv.extent_id);

            repair_worker.enqueue(RepairRequest {
                extent_id: mv.extent_id,
                version: mv.version,
                target_disk_id: mv.target_disk,
                repair_type: RepairType::Replication {
                    healthy_sources: sources,
                },
                priority: 200, // Low priority — rebalance is less urgent
            });

            submitted += 1;

            debug!(
                extent_id = %mv.extent_id,
                source = %mv.source_disk,
                target = %mv.target_disk,
                "rebalance move submitted"
            );
        }

        info!(submitted = submitted, "rebalance plan execution complete");
        submitted
    }

    /// Run a single rebalance pass: generate plan and execute.
    pub async fn rebalance_pass(
        &self,
        inventory: &dyn RebalanceInventory,
        repair_worker: &RepairWorker,
        rate_limiter: &TokenBucket,
    ) -> RebalancePlan {
        let plan = self.generate_plan(inventory);
        if plan.needed {
            self.execute_plan(&plan, inventory, repair_worker, rate_limiter)
                .await;
        }
        plan
    }

    /// Run the rebalance worker in a loop until cancellation.
    pub async fn run(
        &self,
        inventory: &dyn RebalanceInventory,
        repair_worker: &RepairWorker,
        rate_limiter: &TokenBucket,
        mut cancel: tokio::sync::watch::Receiver<bool>,
    ) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(self.config.check_interval_secs)) => {
                    self.rebalance_pass(inventory, repair_worker, rate_limiter).await;
                }
                _ = cancel.changed() => {
                    if *cancel.borrow() {
                        info!("rebalance worker shutting down");
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct FakeRebalanceInventory {
        utilizations: Vec<DiskUtilization>,
        extents: HashMap<DiskId, Vec<(ExtentId, u64)>>,
        sources: HashMap<ExtentId, Vec<DiskId>>,
    }

    impl FakeRebalanceInventory {
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

    impl RebalanceInventory for FakeRebalanceInventory {
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

    #[test]
    fn test_rebalance_config_default() {
        let config = RebalanceConfig::default();
        assert!((config.imbalance_threshold - 0.1).abs() < f64::EPSILON);
        assert_eq!(config.max_moves_per_pass, 100);
        assert_eq!(config.tokens_per_move, 5);
        assert_eq!(config.check_interval_secs, 300);
    }

    #[test]
    fn test_rebalance_config_clone() {
        let config = RebalanceConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.max_moves_per_pass, config.max_moves_per_pass);
    }

    #[test]
    fn test_disk_utilization() {
        let d = DiskUtilization {
            disk_id: DiskId::generate(),
            used_bytes: 500,
            total_bytes: 1000,
            extent_count: 10,
        };
        assert!((d.utilization() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_disk_utilization_zero_total() {
        let d = DiskUtilization {
            disk_id: DiskId::generate(),
            used_bytes: 0,
            total_bytes: 0,
            extent_count: 0,
        };
        assert!((d.utilization()).abs() < f64::EPSILON);
    }

    #[test]
    fn test_disk_utilization_full() {
        let d = DiskUtilization {
            disk_id: DiskId::generate(),
            used_bytes: 1000,
            total_bytes: 1000,
            extent_count: 5,
        };
        assert!((d.utilization() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_disk_utilization_debug() {
        let d = DiskUtilization {
            disk_id: DiskId::generate(),
            used_bytes: 100,
            total_bytes: 200,
            extent_count: 3,
        };
        let debug = format!("{:?}", d);
        assert!(debug.contains("DiskUtilization"));
    }

    #[test]
    fn test_rebalance_move_debug() {
        let mv = RebalanceMove {
            extent_id: ExtentId::generate(),
            version: 1,
            source_disk: DiskId::generate(),
            target_disk: DiskId::generate(),
        };
        let debug = format!("{:?}", mv);
        assert!(debug.contains("RebalanceMove"));
    }

    #[test]
    fn test_rebalance_plan_debug() {
        let plan = RebalancePlan {
            moves: vec![],
            needed: false,
            max_imbalance: 0.05,
        };
        let debug = format!("{:?}", plan);
        assert!(debug.contains("RebalancePlan"));
    }

    #[test]
    fn test_rebalance_worker_new() {
        let worker = RebalanceWorker::new(RebalanceConfig::default());
        assert!(worker.last_plan().is_none());
    }

    #[test]
    fn test_rebalance_worker_debug() {
        let worker = RebalanceWorker::new(RebalanceConfig::default());
        let debug = format!("{:?}", worker);
        assert!(debug.contains("RebalanceWorker"));
    }

    #[test]
    fn test_generate_plan_no_disks() {
        let worker = RebalanceWorker::new(RebalanceConfig::default());
        let inventory = FakeRebalanceInventory::new();
        let plan = worker.generate_plan(&inventory);
        assert!(!plan.needed);
        assert!(plan.moves.is_empty());
    }

    #[test]
    fn test_generate_plan_balanced() {
        let disk1 = DiskId::generate();
        let disk2 = DiskId::generate();

        let worker = RebalanceWorker::new(RebalanceConfig::default());
        let inventory = FakeRebalanceInventory::new()
            .with_disk(disk1, 500, 1000, vec![])
            .with_disk(disk2, 500, 1000, vec![]);

        let plan = worker.generate_plan(&inventory);
        assert!(!plan.needed);
    }

    #[test]
    fn test_generate_plan_imbalanced() {
        let disk1 = DiskId::generate();
        let disk2 = DiskId::generate();
        let eid = ExtentId::generate();

        let worker = RebalanceWorker::new(RebalanceConfig {
            imbalance_threshold: 0.1,
            max_moves_per_pass: 100,
            tokens_per_move: 1,
            check_interval_secs: 60,
        });

        let inventory = FakeRebalanceInventory::new()
            .with_disk(disk1, 900, 1000, vec![(eid, 1)])
            .with_disk(disk2, 100, 1000, vec![]);

        let plan = worker.generate_plan(&inventory);
        assert!(plan.needed);
        assert!(!plan.moves.is_empty());
        assert!(plan.max_imbalance > 0.1);
    }

    #[test]
    fn test_generate_plan_max_moves_limit() {
        let disk1 = DiskId::generate();
        let disk2 = DiskId::generate();

        let mut extents = Vec::new();
        for _ in 0..50 {
            extents.push((ExtentId::generate(), 1));
        }

        let worker = RebalanceWorker::new(RebalanceConfig {
            imbalance_threshold: 0.1,
            max_moves_per_pass: 5,
            tokens_per_move: 1,
            check_interval_secs: 60,
        });

        let inventory = FakeRebalanceInventory::new()
            .with_disk(disk1, 900, 1000, extents)
            .with_disk(disk2, 100, 1000, vec![]);

        let plan = worker.generate_plan(&inventory);
        assert!(plan.moves.len() <= 5);
    }

    #[test]
    fn test_generate_plan_stored() {
        let worker = RebalanceWorker::new(RebalanceConfig::default());
        let inventory = FakeRebalanceInventory::new();

        assert!(worker.last_plan().is_none());
        worker.generate_plan(&inventory);
        assert!(worker.last_plan().is_some());
    }

    #[tokio::test]
    async fn test_execute_plan_empty() {
        let worker = RebalanceWorker::new(RebalanceConfig::default());
        let inventory = FakeRebalanceInventory::new();
        let repair = RepairWorker::new(crate::background::repair::RepairConfig::default());
        let limiter = TokenBucket::new(1000, 1000);

        let plan = RebalancePlan {
            moves: vec![],
            needed: false,
            max_imbalance: 0.0,
        };

        let submitted = worker
            .execute_plan(&plan, &inventory, &repair, &limiter)
            .await;
        assert_eq!(submitted, 0);
    }

    #[tokio::test]
    async fn test_execute_plan_with_moves() {
        let disk1 = DiskId::generate();
        let disk2 = DiskId::generate();
        let eid = ExtentId::generate();

        let worker = RebalanceWorker::new(RebalanceConfig::default());
        let inventory = FakeRebalanceInventory::new()
            .with_disk(disk1, 500, 1000, vec![(eid, 1)]);
        let repair = RepairWorker::new(crate::background::repair::RepairConfig::default());
        let limiter = TokenBucket::new(1000, 1000);

        let plan = RebalancePlan {
            moves: vec![RebalanceMove {
                extent_id: eid,
                version: 1,
                source_disk: disk1,
                target_disk: disk2,
            }],
            needed: true,
            max_imbalance: 0.3,
        };

        let submitted = worker
            .execute_plan(&plan, &inventory, &repair, &limiter)
            .await;
        assert_eq!(submitted, 1);
        assert_eq!(repair.queue_len(), 1);
    }

    #[tokio::test]
    async fn test_rebalance_pass_no_rebalance_needed() {
        let disk1 = DiskId::generate();
        let disk2 = DiskId::generate();

        let worker = RebalanceWorker::new(RebalanceConfig::default());
        let inventory = FakeRebalanceInventory::new()
            .with_disk(disk1, 500, 1000, vec![])
            .with_disk(disk2, 500, 1000, vec![]);
        let repair = RepairWorker::new(crate::background::repair::RepairConfig::default());
        let limiter = TokenBucket::new(1000, 1000);

        let plan = worker
            .rebalance_pass(&inventory, &repair, &limiter)
            .await;
        assert!(!plan.needed);
        assert_eq!(repair.queue_len(), 0);
    }

    #[tokio::test]
    async fn test_rebalance_pass_with_rebalance() {
        let disk1 = DiskId::generate();
        let disk2 = DiskId::generate();
        let eid = ExtentId::generate();

        let worker = RebalanceWorker::new(RebalanceConfig {
            imbalance_threshold: 0.1,
            max_moves_per_pass: 100,
            tokens_per_move: 1,
            check_interval_secs: 60,
        });

        let inventory = FakeRebalanceInventory::new()
            .with_disk(disk1, 900, 1000, vec![(eid, 1)])
            .with_disk(disk2, 100, 1000, vec![]);
        let repair = RepairWorker::new(crate::background::repair::RepairConfig::default());
        let limiter = TokenBucket::new(1000, 1000);

        let plan = worker
            .rebalance_pass(&inventory, &repair, &limiter)
            .await;
        assert!(plan.needed);
        assert!(repair.queue_len() > 0);
    }

    #[tokio::test]
    async fn test_rebalance_run_cancellation() {
        let worker = RebalanceWorker::new(RebalanceConfig {
            check_interval_secs: 3600,
            ..Default::default()
        });
        let inventory = FakeRebalanceInventory::new();
        let repair = RepairWorker::new(crate::background::repair::RepairConfig::default());
        let limiter = TokenBucket::new(1000, 1000);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        let handle = tokio::spawn(async move {
            worker
                .run(&inventory, &repair, &limiter, cancel_rx)
                .await;
        });

        cancel_tx.send(true).unwrap_or(());
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        let result = tokio::time::timeout(tokio::time::Duration::from_secs(1), handle).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_rebalance_plan_clone() {
        let plan = RebalancePlan {
            moves: vec![RebalanceMove {
                extent_id: ExtentId::generate(),
                version: 1,
                source_disk: DiskId::generate(),
                target_disk: DiskId::generate(),
            }],
            needed: true,
            max_imbalance: 0.3,
        };
        let cloned = plan.clone();
        assert_eq!(cloned.moves.len(), plan.moves.len());
        assert_eq!(cloned.needed, plan.needed);
    }

    #[test]
    fn test_rebalance_move_clone() {
        let mv = RebalanceMove {
            extent_id: ExtentId::generate(),
            version: 1,
            source_disk: DiskId::generate(),
            target_disk: DiskId::generate(),
        };
        let cloned = mv.clone();
        assert_eq!(cloned.extent_id, mv.extent_id);
    }

    #[test]
    fn test_disk_utilization_clone() {
        let d = DiskUtilization {
            disk_id: DiskId::generate(),
            used_bytes: 100,
            total_bytes: 200,
            extent_count: 5,
        };
        let cloned = d.clone();
        assert_eq!(cloned.used_bytes, d.used_bytes);
    }
}
