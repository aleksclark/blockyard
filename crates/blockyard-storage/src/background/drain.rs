//! Disk drain workflow (P7.5, §5.11).
//!
//! Enumerates live extents on a draining disk, relocates each via
//! the repair subsystem, and transitions the disk to `Removed` when done.

use blockyard_common::{DiskId, ExtentId};
use tracing::{debug, info, warn};

use crate::background::rate_limit::TokenBucket;
use crate::background::repair::{RepairRequest, RepairType, RepairWorker};

/// Configuration for the drain worker.
#[derive(Debug, Clone)]
pub struct DrainConfig {
    /// Tokens consumed per extent relocation.
    pub tokens_per_relocate: u64,
    /// Delay between relocations (ms) to avoid overwhelming the cluster.
    pub inter_relocate_delay_ms: u64,
}

impl Default for DrainConfig {
    fn default() -> Self {
        Self {
            tokens_per_relocate: 5,
            inter_relocate_delay_ms: 100,
        }
    }
}

/// Entry for an extent that needs draining.
#[derive(Debug, Clone)]
pub struct DrainExtentEntry {
    pub extent_id: ExtentId,
    pub version: u64,
    pub healthy_sources: Vec<DiskId>,
}

/// Trait for enumerating extents on a disk and managing disk state (testability).
pub trait DrainInventory: Send + Sync {
    /// List all extent entries on the draining disk.
    fn list_extents_on_disk(&self, disk_id: DiskId) -> Vec<DrainExtentEntry>;

    /// Select a target disk for relocation.
    fn select_target_disk(&self, exclude: DiskId) -> Result<DiskId, String>;

    /// Transition disk to removed state.
    fn transition_to_removed(&self, disk_id: DiskId) -> Result<(), String>;

    /// Check if disk is still in draining state.
    fn is_draining(&self, disk_id: DiskId) -> bool;
}

/// Progress of a drain operation.
#[derive(Debug, Clone)]
pub struct DrainProgress {
    /// Disk being drained.
    pub disk_id: DiskId,
    /// Total extents to relocate.
    pub total_extents: u64,
    /// Extents relocated so far.
    pub relocated: u64,
    /// Extents that failed relocation.
    pub failed: u64,
    /// Whether the drain is complete.
    pub complete: bool,
}

/// Background drain worker (P7.5).
///
/// Enumerates extents on a draining disk, submits repair requests for
/// each extent, and transitions the disk to removed when all extents
/// have been relocated.
#[derive(Debug)]
pub struct DrainWorker {
    config: DrainConfig,
    progress: parking_lot::Mutex<Option<DrainProgress>>,
}

impl DrainWorker {
    /// Create a new drain worker.
    pub fn new(config: DrainConfig) -> Self {
        Self {
            config,
            progress: parking_lot::Mutex::new(None),
        }
    }

    /// Get configuration.
    pub fn config(&self) -> &DrainConfig {
        &self.config
    }

    /// Get current drain progress.
    pub fn progress(&self) -> Option<DrainProgress> {
        self.progress.lock().clone()
    }

    /// Execute drain for a single disk.
    ///
    /// Enumerates all extents, submits repair requests via the repair worker,
    /// and transitions the disk to removed on completion.
    pub async fn drain_disk(
        &self,
        disk_id: DiskId,
        inventory: &dyn DrainInventory,
        repair_worker: &RepairWorker,
        rate_limiter: &TokenBucket,
    ) -> DrainProgress {
        let extents = inventory.list_extents_on_disk(disk_id);
        let total = extents.len() as u64;

        let mut progress = DrainProgress {
            disk_id,
            total_extents: total,
            relocated: 0,
            failed: 0,
            complete: false,
        };

        *self.progress.lock() = Some(progress.clone());

        info!(
            %disk_id,
            total_extents = total,
            "starting disk drain"
        );

        for entry in &extents {
            if !inventory.is_draining(disk_id) {
                warn!(%disk_id, "disk no longer in draining state, aborting drain");
                break;
            }

            rate_limiter.acquire(self.config.tokens_per_relocate).await;

            let target = match inventory.select_target_disk(disk_id) {
                Ok(t) => t,
                Err(e) => {
                    warn!(
                        %disk_id,
                        extent_id = %entry.extent_id,
                        error = %e,
                        "failed to select target disk for relocation"
                    );
                    progress.failed += 1;
                    *self.progress.lock() = Some(progress.clone());
                    continue;
                }
            };

            repair_worker.enqueue(RepairRequest {
                extent_id: entry.extent_id,
                version: entry.version,
                target_disk_id: target,
                repair_type: RepairType::Replication {
                    healthy_sources: entry.healthy_sources.clone(),
                },
                priority: 100,
            });

            progress.relocated += 1;
            *self.progress.lock() = Some(progress.clone());

            debug!(
                %disk_id,
                extent_id = %entry.extent_id,
                target = %target,
                relocated = progress.relocated,
                total = total,
                "extent relocation enqueued"
            );

            if self.config.inter_relocate_delay_ms > 0 {
                tokio::time::sleep(tokio::time::Duration::from_millis(
                    self.config.inter_relocate_delay_ms,
                ))
                .await;
            }
        }

        if progress.relocated == total && progress.failed == 0 {
            match inventory.transition_to_removed(disk_id) {
                Ok(()) => {
                    info!(%disk_id, "drain complete — disk transitioned to removed");
                    progress.complete = true;
                }
                Err(e) => {
                    warn!(
                        %disk_id,
                        error = %e,
                        "failed to transition disk to removed after drain"
                    );
                }
            }
        } else {
            info!(
                %disk_id,
                relocated = progress.relocated,
                failed = progress.failed,
                total = total,
                "drain incomplete"
            );
        }

        *self.progress.lock() = Some(progress.clone());
        progress
    }

    /// Run the drain worker for a specific disk until completion or cancellation.
    pub async fn run(
        &self,
        disk_id: DiskId,
        inventory: &dyn DrainInventory,
        repair_worker: &RepairWorker,
        rate_limiter: &TokenBucket,
        mut cancel: tokio::sync::watch::Receiver<bool>,
    ) -> DrainProgress {
        tokio::select! {
            progress = self.drain_disk(disk_id, inventory, repair_worker, rate_limiter) => {
                progress
            }
            _ = cancel.changed() => {
                info!(%disk_id, "drain worker cancelled");
                self.progress().unwrap_or(DrainProgress {
                    disk_id,
                    total_extents: 0,
                    relocated: 0,
                    failed: 0,
                    complete: false,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeDrainInventory {
        extents: parking_lot::Mutex<Vec<DrainExtentEntry>>,
        target_disk: parking_lot::Mutex<Option<DiskId>>,
        draining: parking_lot::Mutex<bool>,
        transition_fail: parking_lot::Mutex<bool>,
        target_fail: parking_lot::Mutex<bool>,
    }

    impl FakeDrainInventory {
        fn new() -> Self {
            Self {
                extents: parking_lot::Mutex::new(Vec::new()),
                target_disk: parking_lot::Mutex::new(None),
                draining: parking_lot::Mutex::new(true),
                transition_fail: parking_lot::Mutex::new(false),
                target_fail: parking_lot::Mutex::new(false),
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

        fn set_draining(&self, draining: bool) {
            *self.draining.lock() = draining;
        }

        fn set_transition_fail(&self, fail: bool) {
            *self.transition_fail.lock() = fail;
        }

        fn set_target_fail(&self, fail: bool) {
            *self.target_fail.lock() = fail;
        }
    }

    impl DrainInventory for FakeDrainInventory {
        fn list_extents_on_disk(&self, _disk_id: DiskId) -> Vec<DrainExtentEntry> {
            self.extents.lock().clone()
        }

        fn select_target_disk(&self, _exclude: DiskId) -> Result<DiskId, String> {
            if *self.target_fail.lock() {
                return Err("no target available".into());
            }
            self.target_disk
                .lock()
                .ok_or_else(|| "no target disk configured".into())
        }

        fn transition_to_removed(&self, _disk_id: DiskId) -> Result<(), String> {
            if *self.transition_fail.lock() {
                return Err("transition failed".into());
            }
            Ok(())
        }

        fn is_draining(&self, _disk_id: DiskId) -> bool {
            *self.draining.lock()
        }
    }

    #[test]
    fn test_drain_config_default() {
        let config = DrainConfig::default();
        assert_eq!(config.tokens_per_relocate, 5);
        assert_eq!(config.inter_relocate_delay_ms, 100);
    }

    #[test]
    fn test_drain_config_clone() {
        let config = DrainConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.tokens_per_relocate, config.tokens_per_relocate);
    }

    #[test]
    fn test_drain_progress_debug() {
        let progress = DrainProgress {
            disk_id: DiskId::generate(),
            total_extents: 100,
            relocated: 50,
            failed: 2,
            complete: false,
        };
        let debug = format!("{:?}", progress);
        assert!(debug.contains("DrainProgress"));
    }

    #[test]
    fn test_drain_progress_clone() {
        let progress = DrainProgress {
            disk_id: DiskId::generate(),
            total_extents: 10,
            relocated: 5,
            failed: 0,
            complete: false,
        };
        let cloned = progress.clone();
        assert_eq!(cloned.relocated, progress.relocated);
    }

    #[test]
    fn test_drain_worker_new() {
        let worker = DrainWorker::new(DrainConfig::default());
        assert!(worker.progress().is_none());
        assert_eq!(worker.config().tokens_per_relocate, 5);
    }

    #[test]
    fn test_drain_worker_debug() {
        let worker = DrainWorker::new(DrainConfig::default());
        let debug = format!("{:?}", worker);
        assert!(debug.contains("DrainWorker"));
    }

    #[test]
    fn test_drain_extent_entry_clone() {
        let entry = DrainExtentEntry {
            extent_id: ExtentId::generate(),
            version: 1,
            healthy_sources: vec![DiskId::generate()],
        };
        let cloned = entry.clone();
        assert_eq!(cloned.extent_id, entry.extent_id);
    }

    #[tokio::test]
    async fn test_drain_disk_empty() {
        let worker = DrainWorker::new(DrainConfig {
            tokens_per_relocate: 1,
            inter_relocate_delay_ms: 0,
        });
        let disk_id = DiskId::generate();
        let inventory = FakeDrainInventory::new();
        let repair = RepairWorker::new(crate::background::repair::RepairConfig::default());
        let limiter = TokenBucket::new(1000, 1000);

        let progress = worker
            .drain_disk(disk_id, &inventory, &repair, &limiter)
            .await;
        assert_eq!(progress.total_extents, 0);
        assert_eq!(progress.relocated, 0);
        assert!(progress.complete);
    }

    #[tokio::test]
    async fn test_drain_disk_with_extents() {
        let worker = DrainWorker::new(DrainConfig {
            tokens_per_relocate: 1,
            inter_relocate_delay_ms: 0,
        });
        let disk_id = DiskId::generate();
        let target = DiskId::generate();
        let source = DiskId::generate();

        let extents = vec![
            DrainExtentEntry {
                extent_id: ExtentId::generate(),
                version: 1,
                healthy_sources: vec![source],
            },
            DrainExtentEntry {
                extent_id: ExtentId::generate(),
                version: 1,
                healthy_sources: vec![source],
            },
        ];

        let inventory = FakeDrainInventory::new()
            .with_extents(extents)
            .with_target(target);
        let repair = RepairWorker::new(crate::background::repair::RepairConfig::default());
        let limiter = TokenBucket::new(1000, 1000);

        let progress = worker
            .drain_disk(disk_id, &inventory, &repair, &limiter)
            .await;
        assert_eq!(progress.total_extents, 2);
        assert_eq!(progress.relocated, 2);
        assert!(progress.complete);
        assert_eq!(repair.queue_len(), 2);
    }

    #[tokio::test]
    async fn test_drain_disk_target_selection_failure() {
        let worker = DrainWorker::new(DrainConfig {
            tokens_per_relocate: 1,
            inter_relocate_delay_ms: 0,
        });
        let disk_id = DiskId::generate();

        let extents = vec![DrainExtentEntry {
            extent_id: ExtentId::generate(),
            version: 1,
            healthy_sources: vec![DiskId::generate()],
        }];

        let inventory = FakeDrainInventory::new().with_extents(extents);
        inventory.set_target_fail(true);
        let repair = RepairWorker::new(crate::background::repair::RepairConfig::default());
        let limiter = TokenBucket::new(1000, 1000);

        let progress = worker
            .drain_disk(disk_id, &inventory, &repair, &limiter)
            .await;
        assert_eq!(progress.failed, 1);
        assert!(!progress.complete);
    }

    #[tokio::test]
    async fn test_drain_disk_not_draining_abort() {
        let worker = DrainWorker::new(DrainConfig {
            tokens_per_relocate: 1,
            inter_relocate_delay_ms: 0,
        });
        let disk_id = DiskId::generate();
        let target = DiskId::generate();

        let extents = vec![
            DrainExtentEntry {
                extent_id: ExtentId::generate(),
                version: 1,
                healthy_sources: vec![DiskId::generate()],
            },
            DrainExtentEntry {
                extent_id: ExtentId::generate(),
                version: 1,
                healthy_sources: vec![DiskId::generate()],
            },
        ];

        let inventory = FakeDrainInventory::new()
            .with_extents(extents)
            .with_target(target);
        inventory.set_draining(false);

        let repair = RepairWorker::new(crate::background::repair::RepairConfig::default());
        let limiter = TokenBucket::new(1000, 1000);

        let progress = worker
            .drain_disk(disk_id, &inventory, &repair, &limiter)
            .await;
        // Should abort without relocating
        assert_eq!(progress.relocated, 0);
        assert!(!progress.complete);
    }

    #[tokio::test]
    async fn test_drain_disk_transition_failure() {
        let worker = DrainWorker::new(DrainConfig {
            tokens_per_relocate: 1,
            inter_relocate_delay_ms: 0,
        });
        let disk_id = DiskId::generate();
        let target = DiskId::generate();

        let extents = vec![DrainExtentEntry {
            extent_id: ExtentId::generate(),
            version: 1,
            healthy_sources: vec![DiskId::generate()],
        }];

        let inventory = FakeDrainInventory::new()
            .with_extents(extents)
            .with_target(target);
        inventory.set_transition_fail(true);

        let repair = RepairWorker::new(crate::background::repair::RepairConfig::default());
        let limiter = TokenBucket::new(1000, 1000);

        let progress = worker
            .drain_disk(disk_id, &inventory, &repair, &limiter)
            .await;
        assert_eq!(progress.relocated, 1);
        // complete should be false because transition failed
        assert!(!progress.complete);
    }

    #[tokio::test]
    async fn test_drain_run_cancellation() {
        let worker = DrainWorker::new(DrainConfig {
            tokens_per_relocate: 1,
            inter_relocate_delay_ms: 5000, // Long delay so we can cancel
        });
        let disk_id = DiskId::generate();
        let target = DiskId::generate();

        // Many extents with long delay between = cancellable
        let mut extents = Vec::new();
        for _ in 0..100 {
            extents.push(DrainExtentEntry {
                extent_id: ExtentId::generate(),
                version: 1,
                healthy_sources: vec![DiskId::generate()],
            });
        }

        let inventory = FakeDrainInventory::new()
            .with_extents(extents)
            .with_target(target);
        let repair = RepairWorker::new(crate::background::repair::RepairConfig::default());
        let limiter = TokenBucket::new(1000, 1000);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        let handle = tokio::spawn(async move {
            worker
                .run(disk_id, &inventory, &repair, &limiter, cancel_rx)
                .await
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        cancel_tx.send(true).unwrap_or(());

        let result = tokio::time::timeout(tokio::time::Duration::from_secs(2), handle).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_drain_progress_tracked() {
        let worker = DrainWorker::new(DrainConfig {
            tokens_per_relocate: 1,
            inter_relocate_delay_ms: 0,
        });
        let disk_id = DiskId::generate();
        let target = DiskId::generate();

        let extents = vec![DrainExtentEntry {
            extent_id: ExtentId::generate(),
            version: 1,
            healthy_sources: vec![DiskId::generate()],
        }];

        let inventory = FakeDrainInventory::new()
            .with_extents(extents)
            .with_target(target);
        let repair = RepairWorker::new(crate::background::repair::RepairConfig::default());
        let limiter = TokenBucket::new(1000, 1000);

        assert!(worker.progress().is_none());
        worker
            .drain_disk(disk_id, &inventory, &repair, &limiter)
            .await;
        let progress = worker.progress().expect("should have progress after drain");
        assert_eq!(progress.total_extents, 1);
        assert_eq!(progress.relocated, 1);
        assert!(progress.complete);
    }
}
