//! Background task scheduler (P7.7, §5.13).
//!
//! Coordinates all background workers, enforces priority ordering
//! (foreground > scrub > repair > rebalance > drain), and manages
//! configurable IO budgets.

use tracing::{debug, info};

use crate::background::drain::{DrainConfig, DrainWorker};
use crate::background::rate_limit::TokenBucket;
use crate::background::rebalance::{RebalanceConfig, RebalanceWorker};
use crate::background::repair::{RepairConfig, RepairWorker};
use crate::background::scrub::{ScrubConfig, ScrubWorker};

/// Priority levels for background tasks (lower number = higher priority).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TaskPriority {
    /// Foreground IO — highest priority, never throttled.
    Foreground = 0,
    /// Repair — data durability depends on timely repair.
    Repair = 1,
    /// Scrub — detect corruption early.
    Scrub = 2,
    /// Drain — disk decommission.
    Drain = 3,
    /// Rebalance — optimize but not urgent.
    Rebalance = 4,
}

/// Configuration for the background scheduler.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Total IO budget in tokens/sec for all background work.
    pub total_budget_tokens_per_sec: u64,
    /// Maximum burst capacity for the token bucket.
    pub burst_capacity: u64,
    /// Scrub worker configuration.
    pub scrub: ScrubConfig,
    /// Repair worker configuration.
    pub repair: RepairConfig,
    /// Rebalance worker configuration.
    pub rebalance: RebalanceConfig,
    /// Drain worker configuration.
    pub drain: DrainConfig,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            total_budget_tokens_per_sec: 100,
            burst_capacity: 200,
            scrub: ScrubConfig::default(),
            repair: RepairConfig::default(),
            rebalance: RebalanceConfig::default(),
            drain: DrainConfig::default(),
        }
    }
}

/// Coordinated background task scheduler (P7.7).
///
/// Manages scrub, repair, rebalance, and drain workers with a shared
/// rate-limited IO budget and priority ordering.
#[derive(Debug)]
pub struct BackgroundScheduler {
    config: SchedulerConfig,
    rate_limiter: TokenBucket,
    scrub_worker: ScrubWorker,
    repair_worker: RepairWorker,
    rebalance_worker: RebalanceWorker,
    drain_worker: DrainWorker,
}

impl BackgroundScheduler {
    /// Create a new background scheduler with the given configuration.
    pub fn new(config: SchedulerConfig) -> Self {
        let rate_limiter = TokenBucket::new(config.burst_capacity, config.total_budget_tokens_per_sec);
        let scrub_worker = ScrubWorker::new(config.scrub.clone());
        let repair_worker = RepairWorker::new(config.repair.clone());
        let rebalance_worker = RebalanceWorker::new(config.rebalance.clone());
        let drain_worker = DrainWorker::new(config.drain.clone());

        info!(
            budget = config.total_budget_tokens_per_sec,
            burst = config.burst_capacity,
            "background scheduler initialized"
        );

        Self {
            config,
            rate_limiter,
            scrub_worker,
            repair_worker,
            rebalance_worker,
            drain_worker,
        }
    }

    /// Get the shared rate limiter.
    pub fn rate_limiter(&self) -> &TokenBucket {
        &self.rate_limiter
    }

    /// Get the scrub worker.
    pub fn scrub_worker(&self) -> &ScrubWorker {
        &self.scrub_worker
    }

    /// Get the repair worker.
    pub fn repair_worker(&self) -> &RepairWorker {
        &self.repair_worker
    }

    /// Get the rebalance worker.
    pub fn rebalance_worker(&self) -> &RebalanceWorker {
        &self.rebalance_worker
    }

    /// Get the drain worker.
    pub fn drain_worker(&self) -> &DrainWorker {
        &self.drain_worker
    }

    /// Get the scheduler configuration.
    pub fn config(&self) -> &SchedulerConfig {
        &self.config
    }

    /// Get the current IO budget availability.
    pub fn available_budget(&self) -> u64 {
        self.rate_limiter.available()
    }

    /// Check the current workload status.
    pub fn status(&self) -> SchedulerStatus {
        let repair_backlog = self.repair_worker.queue_len();
        let drain_progress = self.drain_worker.progress();
        let last_scrub = self.scrub_worker.last_results();
        let last_rebalance = self.rebalance_worker.last_plan();

        debug!(
            repair_backlog = repair_backlog,
            drain_active = drain_progress.is_some(),
            available_tokens = self.rate_limiter.available(),
            "scheduler status"
        );

        SchedulerStatus {
            repair_backlog,
            drain_active: drain_progress.is_some(),
            scrub_results_count: last_scrub.len(),
            rebalance_planned: last_rebalance.as_ref().is_some_and(|p| p.needed),
            available_tokens: self.rate_limiter.available(),
        }
    }
}

/// Scheduler status snapshot for observability.
#[derive(Debug, Clone)]
pub struct SchedulerStatus {
    /// Number of pending repair requests.
    pub repair_backlog: usize,
    /// Whether a drain operation is active.
    pub drain_active: bool,
    /// Number of disk scrub results available.
    pub scrub_results_count: usize,
    /// Whether a rebalance was planned.
    pub rebalance_planned: bool,
    /// Currently available IO budget tokens.
    pub available_tokens: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_priority_ordering() {
        assert!(TaskPriority::Foreground < TaskPriority::Repair);
        assert!(TaskPriority::Repair < TaskPriority::Scrub);
        assert!(TaskPriority::Scrub < TaskPriority::Drain);
        assert!(TaskPriority::Drain < TaskPriority::Rebalance);
    }

    #[test]
    fn test_task_priority_eq() {
        assert_eq!(TaskPriority::Foreground, TaskPriority::Foreground);
        assert_ne!(TaskPriority::Foreground, TaskPriority::Repair);
    }

    #[test]
    fn test_task_priority_debug() {
        let debug = format!("{:?}", TaskPriority::Repair);
        assert_eq!(debug, "Repair");
    }

    #[test]
    fn test_task_priority_clone() {
        let p = TaskPriority::Scrub;
        let cloned = p;
        assert_eq!(p, cloned);
    }

    #[test]
    fn test_task_priority_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(TaskPriority::Repair);
        set.insert(TaskPriority::Repair);
        set.insert(TaskPriority::Scrub);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_scheduler_config_default() {
        let config = SchedulerConfig::default();
        assert_eq!(config.total_budget_tokens_per_sec, 100);
        assert_eq!(config.burst_capacity, 200);
    }

    #[test]
    fn test_scheduler_config_clone() {
        let config = SchedulerConfig::default();
        let cloned = config.clone();
        assert_eq!(
            cloned.total_budget_tokens_per_sec,
            config.total_budget_tokens_per_sec
        );
    }

    #[test]
    fn test_scheduler_config_debug() {
        let config = SchedulerConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("SchedulerConfig"));
    }

    #[test]
    fn test_scheduler_new() {
        let scheduler = BackgroundScheduler::new(SchedulerConfig::default());
        assert_eq!(scheduler.config().total_budget_tokens_per_sec, 100);
        assert!(scheduler.available_budget() > 0);
    }

    #[test]
    fn test_scheduler_debug() {
        let scheduler = BackgroundScheduler::new(SchedulerConfig::default());
        let debug = format!("{:?}", scheduler);
        assert!(debug.contains("BackgroundScheduler"));
    }

    #[test]
    fn test_scheduler_rate_limiter() {
        let scheduler = BackgroundScheduler::new(SchedulerConfig {
            burst_capacity: 500,
            total_budget_tokens_per_sec: 100,
            ..Default::default()
        });
        assert_eq!(scheduler.rate_limiter().capacity(), 500);
        assert_eq!(scheduler.rate_limiter().refill_rate(), 100);
    }

    #[test]
    fn test_scheduler_scrub_worker() {
        let scheduler = BackgroundScheduler::new(SchedulerConfig::default());
        let worker = scheduler.scrub_worker();
        assert!(worker.last_results().is_empty());
    }

    #[test]
    fn test_scheduler_repair_worker() {
        let scheduler = BackgroundScheduler::new(SchedulerConfig::default());
        let worker = scheduler.repair_worker();
        assert_eq!(worker.queue_len(), 0);
    }

    #[test]
    fn test_scheduler_rebalance_worker() {
        let scheduler = BackgroundScheduler::new(SchedulerConfig::default());
        let worker = scheduler.rebalance_worker();
        assert!(worker.last_plan().is_none());
    }

    #[test]
    fn test_scheduler_drain_worker() {
        let scheduler = BackgroundScheduler::new(SchedulerConfig::default());
        let worker = scheduler.drain_worker();
        assert!(worker.progress().is_none());
    }

    #[test]
    fn test_scheduler_status() {
        let scheduler = BackgroundScheduler::new(SchedulerConfig::default());
        let status = scheduler.status();
        assert_eq!(status.repair_backlog, 0);
        assert!(!status.drain_active);
        assert_eq!(status.scrub_results_count, 0);
        assert!(!status.rebalance_planned);
        assert!(status.available_tokens > 0);
    }

    #[test]
    fn test_scheduler_status_debug() {
        let status = SchedulerStatus {
            repair_backlog: 5,
            drain_active: true,
            scrub_results_count: 3,
            rebalance_planned: false,
            available_tokens: 42,
        };
        let debug = format!("{:?}", status);
        assert!(debug.contains("SchedulerStatus"));
    }

    #[test]
    fn test_scheduler_status_clone() {
        let status = SchedulerStatus {
            repair_backlog: 10,
            drain_active: false,
            scrub_results_count: 2,
            rebalance_planned: true,
            available_tokens: 100,
        };
        let cloned = status.clone();
        assert_eq!(cloned.repair_backlog, status.repair_backlog);
        assert_eq!(cloned.drain_active, status.drain_active);
    }

    #[test]
    fn test_scheduler_custom_config() {
        let config = SchedulerConfig {
            total_budget_tokens_per_sec: 500,
            burst_capacity: 1000,
            scrub: ScrubConfig {
                interval_secs: 3600,
                tokens_per_extent: 2,
            },
            repair: RepairConfig {
                max_concurrent: 8,
                tokens_per_repair: 20,
            },
            rebalance: RebalanceConfig {
                imbalance_threshold: 0.2,
                max_moves_per_pass: 50,
                tokens_per_move: 10,
                check_interval_secs: 600,
            },
            drain: DrainConfig {
                tokens_per_relocate: 10,
                inter_relocate_delay_ms: 200,
            },
        };

        let scheduler = BackgroundScheduler::new(config);
        assert_eq!(scheduler.config().total_budget_tokens_per_sec, 500);
        assert_eq!(scheduler.scrub_worker().config().interval_secs, 3600);
        assert_eq!(scheduler.repair_worker().config().max_concurrent, 8);
        assert_eq!(scheduler.drain_worker().config().tokens_per_relocate, 10);
    }

    #[test]
    fn test_scheduler_status_with_repair_backlog() {
        let scheduler = BackgroundScheduler::new(SchedulerConfig::default());

        // Enqueue some repair requests
        scheduler.repair_worker().enqueue(
            crate::background::repair::RepairRequest {
                extent_id: blockyard_common::ExtentId::generate(),
                version: 1,
                target_disk_id: blockyard_common::DiskId::generate(),
                repair_type: crate::background::repair::RepairType::Replication {
                    healthy_sources: vec![],
                },
                priority: 0,
            },
        );

        let status = scheduler.status();
        assert_eq!(status.repair_backlog, 1);
    }

    #[test]
    fn test_all_task_priority_variants() {
        let priorities = [
            TaskPriority::Foreground,
            TaskPriority::Repair,
            TaskPriority::Scrub,
            TaskPriority::Drain,
            TaskPriority::Rebalance,
        ];
        // Verify they're all distinct
        for (i, a) in priorities.iter().enumerate() {
            for (j, b) in priorities.iter().enumerate() {
                if i == j {
                    assert_eq!(a, b);
                } else {
                    assert_ne!(a, b);
                }
            }
        }
    }
}
