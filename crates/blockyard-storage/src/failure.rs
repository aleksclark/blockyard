//! Failure handling for data node crashes and disk failures (P5.2, P5.3, P5.6, P5.7, §6).
//!
//! Implements:
//! - **P5.2** Data node crash after local ack: preserve dedup state or allow
//!   metadata interrogation for duplicate suppression.
//! - **P5.3** Data node crash before local durability: never claim success for
//!   incomplete writes. Staged files are discarded on recovery.
//! - **P5.6** Disk failure: transition to `failed`, stop IO, report extent set
//!   for repair, exclude from placement.
//! - **P5.7** Node startup ordering: local recovery → serve committed → hide
//!   staged → rejoin metadata.

use std::collections::HashMap;
use std::path::Path;

use blockyard_common::{DiskId, DiskState, ExtentId, OperationId};
use tracing::{debug, info, warn};

use crate::disk::DiskInventory;
use crate::error::StorageError;
use crate::extent::{ExtentIndex, ExtentStore, LocalExtentEntry};
use crate::service::OperationRecord;

/// Result of a disk failure event.
#[derive(Debug, Clone)]
pub struct DiskFailureReport {
    pub disk_id: DiskId,
    pub previous_state: DiskState,
    pub affected_extents: Vec<ExtentId>,
    pub reason: String,
}

/// Result of node startup recovery.
#[derive(Debug, Default)]
pub struct NodeRecoveryReport {
    pub disks_recovered: usize,
    pub committed_extents: usize,
    pub staged_discarded: usize,
    pub recovery_errors: usize,
    pub disk_reports: Vec<DiskRecoveryDetail>,
}

/// Recovery detail for a single disk.
#[derive(Debug, Clone)]
pub struct DiskRecoveryDetail {
    pub disk_id: DiskId,
    pub committed: usize,
    pub staged_discarded: usize,
    pub errors: usize,
}

/// Handles disk failure events (P5.6, §6.6).
///
/// When a disk fails:
/// 1. Transition disk to `Failed` state
/// 2. Stop all reads and writes to that disk
/// 3. Report the set of affected extents for cluster repair
/// 4. Exclude the disk from future placement
pub fn handle_disk_failure(
    inventory: &DiskInventory,
    index: &ExtentIndex,
    disk_id: DiskId,
    reason: &str,
) -> Result<DiskFailureReport, StorageError> {
    let previous_state = inventory.get_state(disk_id)?;

    if previous_state == DiskState::Failed || previous_state == DiskState::Removed {
        return Ok(DiskFailureReport {
            disk_id,
            previous_state,
            affected_extents: vec![],
            reason: format!("disk already in {} state", previous_state),
        });
    }

    inventory.transition_state(disk_id, DiskState::Failed)?;

    let affected: Vec<ExtentId> = index
        .list_for_disk(disk_id)
        .into_iter()
        .map(|e| e.extent_id)
        .collect();

    warn!(
        %disk_id,
        affected_count = affected.len(),
        previous_state = %previous_state,
        reason = reason,
        "disk failure handled — transitioned to failed"
    );

    Ok(DiskFailureReport {
        disk_id,
        previous_state,
        affected_extents: affected,
        reason: reason.to_string(),
    })
}

/// Check whether an operation can be recovered from the dedup log
/// after a data node crash (P5.2, §6.2).
///
/// On recovery, the data node must either:
/// - Preserve local state for duplicate suppression, or
/// - Allow the client to determine committed state through metadata interrogation
///
/// This function checks the durable operation log for a previous record.
pub fn check_dedup_after_crash(
    durable_log: &HashMap<OperationId, OperationRecord>,
    operation_id: OperationId,
) -> DedupCheckResult {
    match durable_log.get(&operation_id) {
        Some(record) if record.success => DedupCheckResult::PreviouslyCompleted {
            extent_id: record.extent_id,
            extent_version: record.extent_version,
            checksum: record.checksum.clone(),
        },
        Some(_) => DedupCheckResult::PreviouslyFailed,
        None => DedupCheckResult::Unknown,
    }
}

/// Result of checking dedup state after a crash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DedupCheckResult {
    PreviouslyCompleted {
        extent_id: ExtentId,
        extent_version: u64,
        checksum: String,
    },
    PreviouslyFailed,
    Unknown,
}

/// Validate that a staged extent file is incomplete and must not be
/// treated as a valid local extent (P5.3, §6.3).
///
/// Staged files remaining after crash are by definition incomplete:
/// they survived because the process crashed before atomic promotion.
/// They MUST NOT be exposed as valid extents.
pub fn validate_staged_not_durable(staged_path: &Path) -> bool {
    if staged_path.exists() {
        debug!(
            path = %staged_path.display(),
            "staged file exists after crash — NOT durable, will be discarded"
        );
        true
    } else {
        false
    }
}

/// Perform node startup recovery (P5.7, §6.10).
///
/// Recovery ordering:
/// 1. Local recovery: restore committed extents, rebuild index
/// 2. Discard incomplete staged files (P5.3)
/// 3. Serve committed extents
/// 4. Hide staged files (they are invisible to reads)
/// 5. Rejoin metadata participation after local state is valid
///
/// A node MUST NOT advertise itself as fully writable before local
/// recovery has completed.
pub fn recover_node(
    inventory: &DiskInventory,
    stores: &HashMap<DiskId, ExtentStore>,
    index: &ExtentIndex,
) -> Result<NodeRecoveryReport, StorageError> {
    let mut report = NodeRecoveryReport::default();

    let disk_ids = inventory.list_disks();

    for disk_id in &disk_ids {
        let store = match stores.get(disk_id) {
            Some(s) => s,
            None => {
                warn!(%disk_id, "no extent store registered for disk during recovery");
                report.recovery_errors += 1;
                continue;
            }
        };

        match store.recover(index) {
            Ok(disk_report) => {
                let detail = DiskRecoveryDetail {
                    disk_id: *disk_id,
                    committed: disk_report.committed_recovered,
                    staged_discarded: disk_report.staged_discarded,
                    errors: disk_report.errors,
                };

                report.committed_extents += disk_report.committed_recovered;
                report.staged_discarded += disk_report.staged_discarded;
                report.recovery_errors += disk_report.errors;
                report.disk_reports.push(detail);
                report.disks_recovered += 1;

                info!(
                    %disk_id,
                    committed = disk_report.committed_recovered,
                    staged_discarded = disk_report.staged_discarded,
                    "disk recovered"
                );
            }
            Err(e) => {
                warn!(%disk_id, error = %e, "disk recovery failed — marking disk failed");
                inventory.transition_state(*disk_id, DiskState::Failed).ok();
                report.recovery_errors += 1;
            }
        }
    }

    info!(
        disks = report.disks_recovered,
        committed = report.committed_extents,
        staged_discarded = report.staged_discarded,
        errors = report.recovery_errors,
        "node recovery complete"
    );

    Ok(report)
}

/// Determine the set of extents that need repair after a disk failure.
///
/// Returns extent IDs from the failed disk that should be re-replicated
/// or reconstructed by the cluster repair subsystem.
pub fn extents_needing_repair(index: &ExtentIndex, failed_disk: DiskId) -> Vec<LocalExtentEntry> {
    index.list_for_disk(failed_disk)
}

/// Check if a node is ready to advertise itself as writable.
///
/// A node must complete local recovery before accepting writes (§6.10).
pub fn node_ready_for_writes(inventory: &DiskInventory, _index: &ExtentIndex) -> bool {
    let disks = inventory.list_disks();
    if disks.is_empty() {
        return false;
    }

    disks
        .iter()
        .any(|disk_id| inventory.allows_allocation(*disk_id).unwrap_or(false))
}

/// Check if a node is ready to serve reads from committed extents.
///
/// A node can serve reads as soon as local recovery has rebuilt the
/// extent index from committed files. It does not need to wait for
/// metadata rejoin.
pub fn node_ready_for_reads(inventory: &DiskInventory, _index: &ExtentIndex) -> bool {
    let disks = inventory.list_disks();
    if disks.is_empty() {
        return false;
    }

    disks
        .iter()
        .any(|disk_id| inventory.allows_reads(*disk_id).unwrap_or(false))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::DiskInventory;
    use crate::extent::{ExtentIndex, ExtentStore, LocalExtentEntry, StorageClass};
    use blockyard_common::{DiskId, ExtentId, OperationId};
    use std::path::PathBuf;

    fn setup_disk_dir() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        std::fs::write(path.join(".blockyard_xfs_ok"), "").unwrap();
        (dir, path)
    }

    fn make_index_entry(disk_id: DiskId, extent_id: ExtentId) -> LocalExtentEntry {
        LocalExtentEntry {
            extent_id,
            disk_id,
            path: PathBuf::from("/test"),
            version: 1,
            checksum: "abc".into(),
            size: 100,
            storage_class: StorageClass::Default,
        }
    }

    #[test]
    fn test_handle_disk_failure_basic() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        let disk_id = ids[0];

        let index = ExtentIndex::new();
        let eid = ExtentId::generate();
        index.insert(make_index_entry(disk_id, eid)).unwrap();

        let report = handle_disk_failure(&inventory, &index, disk_id, "test failure").unwrap();

        assert_eq!(report.disk_id, disk_id);
        assert_eq!(report.previous_state, DiskState::Healthy);
        assert_eq!(report.affected_extents.len(), 1);
        assert_eq!(report.affected_extents[0], eid);
        assert_eq!(inventory.get_state(disk_id).unwrap(), DiskState::Failed);
    }

    #[test]
    fn test_handle_disk_failure_already_failed() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        let disk_id = ids[0];

        inventory
            .transition_state(disk_id, DiskState::Failed)
            .unwrap();

        let index = ExtentIndex::new();
        let report = handle_disk_failure(&inventory, &index, disk_id, "already failed").unwrap();

        assert_eq!(report.previous_state, DiskState::Failed);
        assert!(report.affected_extents.is_empty());
    }

    #[test]
    fn test_handle_disk_failure_multiple_extents() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        let disk_id = ids[0];

        let index = ExtentIndex::new();
        for _ in 0..5 {
            index
                .insert(make_index_entry(disk_id, ExtentId::generate()))
                .unwrap();
        }

        let report =
            handle_disk_failure(&inventory, &index, disk_id, "multi extent failure").unwrap();
        assert_eq!(report.affected_extents.len(), 5);
    }

    #[test]
    fn test_check_dedup_previously_completed() {
        let mut log = HashMap::new();
        let op_id = OperationId::generate();
        let eid = ExtentId::generate();
        log.insert(
            op_id,
            OperationRecord {
                operation_id: op_id,
                extent_id: eid,
                extent_version: 5,
                disk_id: DiskId::generate(),
                checksum: "abc123".into(),
                success: true,
            },
        );

        let result = check_dedup_after_crash(&log, op_id);
        match result {
            DedupCheckResult::PreviouslyCompleted {
                extent_id,
                extent_version,
                checksum,
            } => {
                assert_eq!(extent_id, eid);
                assert_eq!(extent_version, 5);
                assert_eq!(checksum, "abc123");
            }
            _ => panic!("expected PreviouslyCompleted"),
        }
    }

    #[test]
    fn test_check_dedup_previously_failed() {
        let mut log = HashMap::new();
        let op_id = OperationId::generate();
        log.insert(
            op_id,
            OperationRecord {
                operation_id: op_id,
                extent_id: ExtentId::generate(),
                extent_version: 1,
                disk_id: DiskId::generate(),
                checksum: String::new(),
                success: false,
            },
        );

        assert_eq!(
            check_dedup_after_crash(&log, op_id),
            DedupCheckResult::PreviouslyFailed
        );
    }

    #[test]
    fn test_check_dedup_unknown() {
        let log = HashMap::new();
        let op_id = OperationId::generate();
        assert_eq!(
            check_dedup_after_crash(&log, op_id),
            DedupCheckResult::Unknown
        );
    }

    #[test]
    fn test_validate_staged_not_durable_exists() {
        let dir = tempfile::tempdir().unwrap();
        let staged = dir.path().join("test.staging");
        std::fs::write(&staged, "incomplete").unwrap();
        assert!(validate_staged_not_durable(&staged));
    }

    #[test]
    fn test_validate_staged_not_durable_missing() {
        assert!(!validate_staged_not_durable(Path::new("/nonexistent/path")));
    }

    #[test]
    fn test_recover_node_basic() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path.clone()], false).unwrap();
        let disk_id = ids[0];

        let store = ExtentStore::new(path, disk_id);
        store.init_directories().unwrap();

        let eid = ExtentId::generate();
        let data = b"committed data";
        let (_, checksum) = store.stage_extent(eid, 1, data).unwrap();
        store
            .commit_extent(eid, 1, &checksum, data.len() as u64, StorageClass::Default)
            .unwrap();

        store
            .stage_extent(ExtentId::generate(), 1, b"orphan")
            .unwrap();

        let mut stores = HashMap::new();
        stores.insert(
            disk_id,
            ExtentStore::new(store.mount_path().to_path_buf(), disk_id),
        );
        stores.get(&disk_id).unwrap().init_directories().unwrap();

        let index = ExtentIndex::new();

        let fresh_store = ExtentStore::new(store.mount_path().to_path_buf(), disk_id);
        let mut fresh_stores = HashMap::new();
        fresh_stores.insert(disk_id, fresh_store);

        let report = recover_node(&inventory, &fresh_stores, &index).unwrap();

        assert_eq!(report.disks_recovered, 1);
        assert_eq!(report.committed_extents, 1);
        assert_eq!(report.staged_discarded, 1);
    }

    #[test]
    fn test_recover_node_no_stores() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        inventory.discover_disks(&[path], false).unwrap();

        let stores = HashMap::new();
        let index = ExtentIndex::new();

        let report = recover_node(&inventory, &stores, &index).unwrap();
        assert_eq!(report.disks_recovered, 0);
        assert_eq!(report.recovery_errors, 1);
    }

    #[test]
    fn test_extents_needing_repair() {
        let index = ExtentIndex::new();
        let disk_id = DiskId::generate();

        for _ in 0..3 {
            index
                .insert(make_index_entry(disk_id, ExtentId::generate()))
                .unwrap();
        }
        index
            .insert(make_index_entry(DiskId::generate(), ExtentId::generate()))
            .unwrap();

        let repair = extents_needing_repair(&index, disk_id);
        assert_eq!(repair.len(), 3);
    }

    #[test]
    fn test_node_ready_for_writes() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        inventory.discover_disks(&[path], false).unwrap();
        let index = ExtentIndex::new();

        assert!(node_ready_for_writes(&inventory, &index));
    }

    #[test]
    fn test_node_ready_for_writes_no_disks() {
        let inventory = DiskInventory::new();
        let index = ExtentIndex::new();
        assert!(!node_ready_for_writes(&inventory, &index));
    }

    #[test]
    fn test_node_ready_for_writes_all_failed() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        inventory
            .transition_state(ids[0], DiskState::Failed)
            .unwrap();
        let index = ExtentIndex::new();

        assert!(!node_ready_for_writes(&inventory, &index));
    }

    #[test]
    fn test_node_ready_for_reads() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        inventory.discover_disks(&[path], false).unwrap();
        let index = ExtentIndex::new();

        assert!(node_ready_for_reads(&inventory, &index));
    }

    #[test]
    fn test_node_ready_for_reads_draining_allowed() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        inventory
            .transition_state(ids[0], DiskState::Draining)
            .unwrap();
        let index = ExtentIndex::new();

        assert!(node_ready_for_reads(&inventory, &index));
    }

    #[test]
    fn test_node_ready_for_reads_all_failed() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        inventory
            .transition_state(ids[0], DiskState::Failed)
            .unwrap();
        let index = ExtentIndex::new();

        assert!(!node_ready_for_reads(&inventory, &index));
    }

    #[test]
    fn test_disk_failure_report_debug() {
        let r = DiskFailureReport {
            disk_id: DiskId::generate(),
            previous_state: DiskState::Healthy,
            affected_extents: vec![],
            reason: "test".into(),
        };
        let debug = format!("{:?}", r);
        assert!(debug.contains("DiskFailureReport"));
    }

    #[test]
    fn test_disk_failure_report_clone() {
        let r = DiskFailureReport {
            disk_id: DiskId::generate(),
            previous_state: DiskState::Suspect,
            affected_extents: vec![ExtentId::generate()],
            reason: "clone test".into(),
        };
        let cloned = r.clone();
        assert_eq!(cloned.previous_state, DiskState::Suspect);
    }

    #[test]
    fn test_node_recovery_report_default() {
        let r = NodeRecoveryReport::default();
        assert_eq!(r.disks_recovered, 0);
        assert_eq!(r.committed_extents, 0);
        assert_eq!(r.staged_discarded, 0);
        assert_eq!(r.recovery_errors, 0);
    }

    #[test]
    fn test_disk_recovery_detail_debug() {
        let d = DiskRecoveryDetail {
            disk_id: DiskId::generate(),
            committed: 5,
            staged_discarded: 2,
            errors: 0,
        };
        let debug = format!("{:?}", d);
        assert!(debug.contains("DiskRecoveryDetail"));
    }

    #[test]
    fn test_disk_recovery_detail_clone() {
        let d = DiskRecoveryDetail {
            disk_id: DiskId::generate(),
            committed: 10,
            staged_discarded: 3,
            errors: 1,
        };
        let cloned = d.clone();
        assert_eq!(cloned.committed, 10);
    }

    #[test]
    fn test_dedup_check_result_debug() {
        let r = DedupCheckResult::Unknown;
        assert!(format!("{:?}", r).contains("Unknown"));
    }

    #[test]
    fn test_dedup_check_result_eq() {
        assert_eq!(DedupCheckResult::Unknown, DedupCheckResult::Unknown);
        assert_eq!(
            DedupCheckResult::PreviouslyFailed,
            DedupCheckResult::PreviouslyFailed
        );
        assert_ne!(
            DedupCheckResult::Unknown,
            DedupCheckResult::PreviouslyFailed
        );
    }

    #[test]
    fn test_dedup_check_result_clone() {
        let r = DedupCheckResult::PreviouslyCompleted {
            extent_id: ExtentId::generate(),
            extent_version: 42,
            checksum: "xyz".into(),
        };
        let cloned = r.clone();
        assert_eq!(r, cloned);
    }

    #[test]
    fn test_recover_node_multiple_disks() {
        let (_d1, p1) = setup_disk_dir();
        let (_d2, p2) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory
            .discover_disks(&[p1.clone(), p2.clone()], false)
            .unwrap();

        let mut stores = HashMap::new();
        for (i, &disk_id) in ids.iter().enumerate() {
            let path = if i == 0 { &p1 } else { &p2 };
            let store = ExtentStore::new(path.clone(), disk_id);
            store.init_directories().unwrap();
            stores.insert(disk_id, store);
        }

        let index = ExtentIndex::new();
        let report = recover_node(&inventory, &stores, &index).unwrap();
        assert_eq!(report.disks_recovered, 2);
    }

    #[test]
    fn test_handle_disk_failure_from_suspect() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        let disk_id = ids[0];
        inventory
            .transition_state(disk_id, DiskState::Suspect)
            .unwrap();

        let index = ExtentIndex::new();
        let report = handle_disk_failure(&inventory, &index, disk_id, "from suspect").unwrap();
        assert_eq!(report.previous_state, DiskState::Suspect);
        assert_eq!(inventory.get_state(disk_id).unwrap(), DiskState::Failed);
    }
}
