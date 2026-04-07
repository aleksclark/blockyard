//! Background operations for data node maintenance (Phase 7).
//!
//! Provides workers for scrubbing, repair, drain, rebalance, and
//! coordinated scheduling with rate-limited IO budgets.
//!
//! - [`scrub`] — periodic extent file scanning and checksum verification (P7.1)
//! - [`repair`] — re-replication and erasure-code rebuild (P7.2–P7.4)
//! - [`drain`] — disk drain workflow (P7.5)
//! - [`rebalance`] — capacity rebalancing across disks (P7.6)
//! - [`rate_limit`] — token-bucket IO rate limiter (P7.7)
//! - [`scheduler`] — coordinated background task scheduler (P7.7)

pub mod drain;
pub mod rate_limit;
pub mod rebalance;
pub mod repair;
pub mod scheduler;
pub mod scrub;

pub use drain::{DrainConfig, DrainWorker};
pub use rate_limit::TokenBucket;
pub use rebalance::{RebalanceConfig, RebalancePlan, RebalanceWorker};
pub use repair::{RepairConfig, RepairRequest, RepairType, RepairWorker};
pub use scheduler::{BackgroundScheduler, SchedulerConfig, TaskPriority};
pub use scrub::{ScrubConfig, ScrubResult, ScrubWorker};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_exports() {
        // Verify all public types are accessible
        let _: fn() -> TokenBucket = || TokenBucket::new(100, 100);
        let _ = TaskPriority::Foreground;
        let _ = TaskPriority::Repair;
        let _ = TaskPriority::Scrub;
        let _ = TaskPriority::Rebalance;
        let _ = TaskPriority::Drain;
    }
}
