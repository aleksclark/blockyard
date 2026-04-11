//! Disk inventory, discovery, and persistent identity (§5.2, §5.10).
//!
//! Detects physical disks, assigns/recovers persistent [`DiskId`]s, and
//! validates that each disk hosts exactly one dedicated XFS filesystem.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use blockyard_common::{DiskId, DiskState};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::error::StorageError;
use crate::health::{DiskHealthTracker, DiskTelemetry};
use crate::region::BadRegionMap;

const DISK_ID_FILENAME: &str = ".blockyard_disk_id";

/// Persistent metadata stored on each managed disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskMetadata {
    pub disk_id: DiskId,
    pub node_id: Option<blockyard_common::NodeId>,
}

/// Qualification state for newly added disks (§5.10.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QualificationState {
    Pending,
    BurnInRunning { started_at: std::time::Instant },
    Qualified,
}

/// Runtime information for a single managed disk.
#[derive(Debug)]
pub struct ManagedDisk {
    pub disk_id: DiskId,
    pub mount_path: PathBuf,
    pub state: DiskState,
    pub qualification: QualificationState,
    pub health: DiskHealthTracker,
    pub bad_regions: BadRegionMap,
}

impl ManagedDisk {
    fn new(disk_id: DiskId, mount_path: PathBuf) -> Self {
        Self {
            disk_id,
            mount_path,
            state: DiskState::Healthy,
            qualification: QualificationState::Qualified,
            health: DiskHealthTracker::new(disk_id),
            bad_regions: BadRegionMap::new(disk_id),
        }
    }

    fn new_qualifying(disk_id: DiskId, mount_path: PathBuf) -> Self {
        Self {
            disk_id,
            mount_path,
            state: DiskState::Suspect,
            qualification: QualificationState::Pending,
            health: DiskHealthTracker::new(disk_id),
            bad_regions: BadRegionMap::new(disk_id),
        }
    }
}

/// Central disk inventory tracking all managed disks.
#[derive(Debug)]
pub struct DiskInventory {
    disks: RwLock<HashMap<DiskId, ManagedDisk>>,
}

impl DiskInventory {
    pub fn new() -> Self {
        Self {
            disks: RwLock::new(HashMap::new()),
        }
    }

    /// Discover and initialize disks from configured mount paths.
    ///
    /// For each path:
    /// 1. Validate XFS filesystem presence
    /// 2. Read or assign a persistent DiskId
    /// 3. Register the disk in the inventory
    pub fn discover_disks(
        &self,
        mount_paths: &[PathBuf],
        require_qualification: bool,
    ) -> Result<Vec<DiskId>, StorageError> {
        self.discover_disks_for_node(mount_paths, require_qualification, None)
    }

    /// Discover and initialize disks, binding them to the given node.
    ///
    /// Sets `node_id` in the on-disk metadata to prevent another node
    /// from claiming the same disk (§5.10.2).
    pub fn discover_disks_for_node(
        &self,
        mount_paths: &[PathBuf],
        require_qualification: bool,
        node_id: Option<blockyard_common::NodeId>,
    ) -> Result<Vec<DiskId>, StorageError> {
        let mut discovered = Vec::new();

        for path in mount_paths {
            match self.discover_single_disk(path, require_qualification, node_id) {
                Ok(disk_id) => {
                    info!(%disk_id, path = %path.display(), "discovered disk");
                    discovered.push(disk_id);
                }
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "failed to discover disk");
                    return Err(e);
                }
            }
        }

        Ok(discovered)
    }

    fn discover_single_disk(
        &self,
        mount_path: &Path,
        require_qualification: bool,
        node_id: Option<blockyard_common::NodeId>,
    ) -> Result<DiskId, StorageError> {
        if !mount_path.is_dir() {
            return Err(StorageError::DiskNotFound(format!(
                "mount path does not exist or is not a directory: {}",
                mount_path.display()
            )));
        }

        validate_xfs(mount_path)?;

        let disk_id = read_or_assign_disk_id(mount_path, node_id)?;

        let managed = if require_qualification {
            ManagedDisk::new_qualifying(disk_id, mount_path.to_path_buf())
        } else {
            ManagedDisk::new(disk_id, mount_path.to_path_buf())
        };

        let mut disks = self.disks.write();
        if disks.contains_key(&disk_id) {
            return Err(StorageError::DuplicateDisk(format!(
                "disk {} already registered",
                disk_id
            )));
        }
        disks.insert(disk_id, managed);
        Ok(disk_id)
    }

    /// Transition a disk's state, validating the transition is legal.
    pub fn transition_state(
        &self,
        disk_id: DiskId,
        new_state: DiskState,
    ) -> Result<DiskState, StorageError> {
        let mut disks = self.disks.write();
        let disk = disks
            .get_mut(&disk_id)
            .ok_or_else(|| StorageError::DiskNotFound(format!("unknown disk: {disk_id}")))?;

        let old_state = disk.state;
        old_state
            .validate_transition(new_state)
            .map_err(|e| StorageError::InvalidTransition(e.to_string()))?;

        info!(%disk_id, %old_state, %new_state, "disk state transition");
        disk.state = new_state;
        Ok(old_state)
    }

    /// Check whether a disk allows new extent allocations (§5.2, invariant 6).
    pub fn allows_allocation(&self, disk_id: DiskId) -> Result<bool, StorageError> {
        let disks = self.disks.read();
        let disk = disks
            .get(&disk_id)
            .ok_or_else(|| StorageError::DiskNotFound(format!("unknown disk: {disk_id}")))?;

        let state_ok = disk.state.allows_allocation();
        let qualified = disk.qualification == QualificationState::Qualified;
        Ok(state_ok && qualified)
    }

    /// Check whether a disk allows reads.
    pub fn allows_reads(&self, disk_id: DiskId) -> Result<bool, StorageError> {
        let disks = self.disks.read();
        let disk = disks
            .get(&disk_id)
            .ok_or_else(|| StorageError::DiskNotFound(format!("unknown disk: {disk_id}")))?;
        Ok(disk.state.allows_reads())
    }

    /// Get the current state of a disk.
    pub fn get_state(&self, disk_id: DiskId) -> Result<DiskState, StorageError> {
        let disks = self.disks.read();
        let disk = disks
            .get(&disk_id)
            .ok_or_else(|| StorageError::DiskNotFound(format!("unknown disk: {disk_id}")))?;
        Ok(disk.state)
    }

    /// Get the mount path for a disk.
    pub fn get_mount_path(&self, disk_id: DiskId) -> Result<PathBuf, StorageError> {
        let disks = self.disks.read();
        let disk = disks
            .get(&disk_id)
            .ok_or_else(|| StorageError::DiskNotFound(format!("unknown disk: {disk_id}")))?;
        Ok(disk.mount_path.clone())
    }

    /// List all known disk IDs.
    pub fn list_disks(&self) -> Vec<DiskId> {
        let disks = self.disks.read();
        disks.keys().copied().collect()
    }

    /// Record telemetry event for a disk and potentially update its state.
    pub fn record_telemetry(
        &self,
        disk_id: DiskId,
        telemetry: &DiskTelemetry,
    ) -> Result<(), StorageError> {
        let mut disks = self.disks.write();
        let disk = disks
            .get_mut(&disk_id)
            .ok_or_else(|| StorageError::DiskNotFound(format!("unknown disk: {disk_id}")))?;

        disk.health.record(telemetry);

        if let Some(derived_state) = disk.health.derive_state() {
            if derived_state != disk.state && disk.state.validate_transition(derived_state).is_ok()
            {
                debug!(%disk_id, old = %disk.state, new = %derived_state, "telemetry-derived state change");
                disk.state = derived_state;
            }
        }

        Ok(())
    }

    /// Complete qualification (burn-in) for a disk, transitioning it to healthy.
    pub fn complete_qualification(&self, disk_id: DiskId) -> Result<(), StorageError> {
        let mut disks = self.disks.write();
        let disk = disks
            .get_mut(&disk_id)
            .ok_or_else(|| StorageError::DiskNotFound(format!("unknown disk: {disk_id}")))?;

        if disk.qualification == QualificationState::Qualified {
            return Ok(());
        }

        disk.qualification = QualificationState::Qualified;

        if disk.state == DiskState::Suspect {
            disk.state = DiskState::Healthy;
            info!(%disk_id, "disk qualified and promoted to healthy");
        }

        Ok(())
    }

    /// Start burn-in for a disk that is in Pending qualification state.
    pub fn start_burn_in(&self, disk_id: DiskId) -> Result<(), StorageError> {
        let mut disks = self.disks.write();
        let disk = disks
            .get_mut(&disk_id)
            .ok_or_else(|| StorageError::DiskNotFound(format!("unknown disk: {disk_id}")))?;

        if disk.qualification != QualificationState::Pending {
            return Err(StorageError::InvalidTransition(format!(
                "disk {disk_id} not in pending qualification state"
            )));
        }

        disk.qualification = QualificationState::BurnInRunning {
            started_at: std::time::Instant::now(),
        };
        info!(%disk_id, "burn-in started");
        Ok(())
    }

    /// Get the qualification state for a disk.
    pub fn get_qualification(&self, disk_id: DiskId) -> Result<QualificationState, StorageError> {
        let disks = self.disks.read();
        let disk = disks
            .get(&disk_id)
            .ok_or_else(|| StorageError::DiskNotFound(format!("unknown disk: {disk_id}")))?;
        Ok(disk.qualification)
    }

    /// Report bad regions for a disk.
    pub fn report_bad_region(
        &self,
        disk_id: DiskId,
        offset: u64,
        length: u64,
    ) -> Result<(), StorageError> {
        let mut disks = self.disks.write();
        let disk = disks
            .get_mut(&disk_id)
            .ok_or_else(|| StorageError::DiskNotFound(format!("unknown disk: {disk_id}")))?;
        disk.bad_regions.add_region(offset, length);
        Ok(())
    }

    /// Check whether a region on a disk is quarantined.
    pub fn is_region_quarantined(
        &self,
        disk_id: DiskId,
        offset: u64,
        length: u64,
    ) -> Result<bool, StorageError> {
        let disks = self.disks.read();
        let disk = disks
            .get(&disk_id)
            .ok_or_else(|| StorageError::DiskNotFound(format!("unknown disk: {disk_id}")))?;
        Ok(disk.bad_regions.overlaps(offset, length))
    }

    /// Check whether a disk has any quarantined bad regions.
    pub fn has_bad_regions(&self, disk_id: DiskId) -> Result<bool, StorageError> {
        let disks = self.disks.read();
        let disk = disks
            .get(&disk_id)
            .ok_or_else(|| StorageError::DiskNotFound(format!("unknown disk: {disk_id}")))?;
        Ok(disk.bad_regions.count() > 0)
    }
}

impl Default for DiskInventory {
    fn default() -> Self {
        Self::new()
    }
}

/// Validate that the given path is on an XFS filesystem (§3.3, §5.10.3, invariant 8).
///
/// Returns an error if the filesystem is not XFS. Override mechanisms:
/// - Marker file `.blockyard_xfs_ok` in the mount path (for dev/testing)
/// - Environment variable `BLOCKYARD_SKIP_XFS_CHECK=1`
fn validate_xfs(path: &Path) -> Result<(), StorageError> {
    if std::env::var("BLOCKYARD_SKIP_XFS_CHECK").map_or(false, |v| v == "1") {
        debug!(path = %path.display(), "XFS validation: skipped via BLOCKYARD_SKIP_XFS_CHECK");
        return Ok(());
    }

    if path.join(".blockyard_xfs_ok").exists() {
        debug!(path = %path.display(), "XFS validation: accepted via marker file");
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    {
        let _ = std::fs::metadata(path).map_err(|e| {
            StorageError::XfsValidation(format!("cannot stat {}: {e}", path.display()))
        })?;

        if let Ok(output) = std::process::Command::new("stat")
            .args(["-f", "-c", "%T"])
            .arg(path)
            .output()
        {
            let fs_type = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if fs_type == "xfs" {
                return Ok(());
            }
            return Err(StorageError::XfsValidation(format!(
                "{} is not an XFS filesystem (detected: '{}'). \
                 Use .blockyard_xfs_ok marker file or BLOCKYARD_SKIP_XFS_CHECK=1 to override.",
                path.display(),
                fs_type
            )));
        }

        Err(StorageError::XfsValidation(format!(
            "could not determine filesystem type for {}. \
             Use .blockyard_xfs_ok marker file or BLOCKYARD_SKIP_XFS_CHECK=1 to override.",
            path.display()
        )))
    }

    #[cfg(not(target_os = "linux"))]
    {
        Err(StorageError::XfsValidation(format!(
            "{} cannot be validated as XFS on non-Linux. \
             Use .blockyard_xfs_ok marker file or BLOCKYARD_SKIP_XFS_CHECK=1 to override.",
            path.display()
        )))
    }
}

/// Read or assign a persistent DiskId for the given mount path.
fn read_or_assign_disk_id(
    mount_path: &Path,
    node_id: Option<blockyard_common::NodeId>,
) -> Result<DiskId, StorageError> {
    let id_path = mount_path.join(DISK_ID_FILENAME);

    if id_path.exists() {
        let contents = std::fs::read_to_string(&id_path).map_err(|e| {
            StorageError::DiskIdentity(format!(
                "failed to read disk ID from {}: {e}",
                id_path.display()
            ))
        })?;

        let metadata: DiskMetadata = serde_json::from_str(contents.trim()).map_err(|e| {
            StorageError::DiskIdentity(format!(
                "corrupt disk ID file at {}: {e}",
                id_path.display()
            ))
        })?;

        debug!(disk_id = %metadata.disk_id, path = %mount_path.display(), "recovered existing disk ID");
        Ok(metadata.disk_id)
    } else {
        let disk_id = DiskId::generate();
        let metadata = DiskMetadata {
            disk_id,
            node_id,
        };

        let json = serde_json::to_string_pretty(&metadata).map_err(|e| {
            StorageError::DiskIdentity(format!("failed to serialize disk metadata: {e}"))
        })?;

        std::fs::write(&id_path, json).map_err(|e| {
            StorageError::DiskIdentity(format!(
                "failed to write disk ID to {}: {e}",
                id_path.display()
            ))
        })?;

        info!(%disk_id, path = %mount_path.display(), "assigned new disk ID");
        Ok(disk_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_disk_dir() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        std::fs::write(path.join(".blockyard_xfs_ok"), "").unwrap();
        (dir, path)
    }

    #[test]
    fn test_discover_single_disk() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        assert_eq!(ids.len(), 1);
    }

    #[test]
    fn test_discover_recovers_existing_id() {
        let (_dir, path) = setup_disk_dir();

        let id1 = {
            let inventory = DiskInventory::new();
            let ids = inventory.discover_disks(&[path.clone()], false).unwrap();
            ids[0]
        };

        let id2 = {
            let inventory = DiskInventory::new();
            let ids = inventory.discover_disks(&[path], false).unwrap();
            ids[0]
        };

        assert_eq!(id1, id2);
    }

    #[test]
    fn test_discover_nonexistent_path() {
        let inventory = DiskInventory::new();
        let result = inventory.discover_disks(&[PathBuf::from("/nonexistent/path")], false);
        assert!(result.is_err());
    }

    #[test]
    fn test_discover_multiple_disks() {
        let (_d1, p1) = setup_disk_dir();
        let (_d2, p2) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[p1, p2], false).unwrap();
        assert_eq!(ids.len(), 2);
        assert_ne!(ids[0], ids[1]);
    }

    #[test]
    fn test_duplicate_disk_rejected() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        inventory.discover_disks(&[path.clone()], false).unwrap();
        let result = inventory.discover_disks(&[path], false);
        assert!(result.is_err());
    }

    #[test]
    fn test_transition_state() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        let disk_id = ids[0];

        let old = inventory
            .transition_state(disk_id, DiskState::Suspect)
            .unwrap();
        assert_eq!(old, DiskState::Healthy);
        assert_eq!(inventory.get_state(disk_id).unwrap(), DiskState::Suspect);
    }

    #[test]
    fn test_transition_state_illegal() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        let disk_id = ids[0];

        let result = inventory.transition_state(disk_id, DiskState::Removed);
        assert!(result.is_err());
    }

    #[test]
    fn test_transition_state_unknown_disk() {
        let inventory = DiskInventory::new();
        let result = inventory.transition_state(DiskId::generate(), DiskState::Failed);
        assert!(result.is_err());
    }

    #[test]
    fn test_allows_allocation_healthy() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        assert!(inventory.allows_allocation(ids[0]).unwrap());
    }

    #[test]
    fn test_denies_allocation_degraded() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        let disk_id = ids[0];

        inventory
            .transition_state(disk_id, DiskState::Degraded)
            .unwrap();
        assert!(!inventory.allows_allocation(disk_id).unwrap());
    }

    #[test]
    fn test_denies_allocation_draining() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        let disk_id = ids[0];

        inventory
            .transition_state(disk_id, DiskState::Draining)
            .unwrap();
        assert!(!inventory.allows_allocation(disk_id).unwrap());
    }

    #[test]
    fn test_denies_allocation_failed() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        let disk_id = ids[0];

        inventory
            .transition_state(disk_id, DiskState::Failed)
            .unwrap();
        assert!(!inventory.allows_allocation(disk_id).unwrap());
    }

    #[test]
    fn test_denies_allocation_removed() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        let disk_id = ids[0];

        inventory
            .transition_state(disk_id, DiskState::Failed)
            .unwrap();
        inventory
            .transition_state(disk_id, DiskState::Removed)
            .unwrap();
        assert!(!inventory.allows_allocation(disk_id).unwrap());
    }

    #[test]
    fn test_allows_reads_healthy() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        assert!(inventory.allows_reads(ids[0]).unwrap());
    }

    #[test]
    fn test_denies_reads_failed() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        let disk_id = ids[0];

        inventory
            .transition_state(disk_id, DiskState::Failed)
            .unwrap();
        assert!(!inventory.allows_reads(disk_id).unwrap());
    }

    #[test]
    fn test_allows_reads_draining() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        let disk_id = ids[0];

        inventory
            .transition_state(disk_id, DiskState::Draining)
            .unwrap();
        assert!(inventory.allows_reads(disk_id).unwrap());
    }

    #[test]
    fn test_list_disks() {
        let (_d1, p1) = setup_disk_dir();
        let (_d2, p2) = setup_disk_dir();
        let inventory = DiskInventory::new();
        inventory.discover_disks(&[p1, p2], false).unwrap();
        assert_eq!(inventory.list_disks().len(), 2);
    }

    #[test]
    fn test_get_mount_path() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path.clone()], false).unwrap();
        assert_eq!(inventory.get_mount_path(ids[0]).unwrap(), path);
    }

    #[test]
    fn test_get_mount_path_unknown() {
        let inventory = DiskInventory::new();
        assert!(inventory.get_mount_path(DiskId::generate()).is_err());
    }

    #[test]
    fn test_qualification_pending_denies_allocation() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], true).unwrap();
        let disk_id = ids[0];

        assert!(!inventory.allows_allocation(disk_id).unwrap());
        assert_eq!(
            inventory.get_qualification(disk_id).unwrap(),
            QualificationState::Pending
        );
    }

    #[test]
    fn test_qualification_complete_allows_allocation() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], true).unwrap();
        let disk_id = ids[0];

        inventory.complete_qualification(disk_id).unwrap();
        assert!(inventory.allows_allocation(disk_id).unwrap());
        assert_eq!(inventory.get_state(disk_id).unwrap(), DiskState::Healthy);
    }

    #[test]
    fn test_burn_in_start() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], true).unwrap();
        let disk_id = ids[0];

        inventory.start_burn_in(disk_id).unwrap();
        let q = inventory.get_qualification(disk_id).unwrap();
        assert!(matches!(q, QualificationState::BurnInRunning { .. }));
    }

    #[test]
    fn test_burn_in_already_qualified() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        let disk_id = ids[0];

        let result = inventory.start_burn_in(disk_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_complete_qualification_already_qualified() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        let disk_id = ids[0];

        assert!(inventory.complete_qualification(disk_id).is_ok());
    }

    #[test]
    fn test_bad_region_report_and_check() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        let disk_id = ids[0];

        assert!(!inventory.is_region_quarantined(disk_id, 100, 50).unwrap());

        inventory.report_bad_region(disk_id, 100, 50).unwrap();

        assert!(inventory.is_region_quarantined(disk_id, 120, 10).unwrap());
        assert!(!inventory.is_region_quarantined(disk_id, 200, 10).unwrap());
    }

    #[test]
    fn test_bad_region_unknown_disk() {
        let inventory = DiskInventory::new();
        assert!(
            inventory
                .report_bad_region(DiskId::generate(), 0, 100)
                .is_err()
        );
    }

    #[test]
    fn test_record_telemetry() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        let disk_id = ids[0];

        let tel = DiskTelemetry {
            read_errors: 0,
            write_errors: 0,
            checksum_mismatches: 0,
            media_errors: 0,
            timeouts: 0,
            temperature_celsius: Some(35),
            latency_p99_us: Some(500),
        };
        assert!(inventory.record_telemetry(disk_id, &tel).is_ok());
    }

    #[test]
    fn test_record_telemetry_triggers_degraded() {
        let (_dir, path) = setup_disk_dir();
        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path], false).unwrap();
        let disk_id = ids[0];

        let tel = DiskTelemetry {
            read_errors: 100,
            write_errors: 100,
            checksum_mismatches: 50,
            media_errors: 20,
            timeouts: 0,
            temperature_celsius: None,
            latency_p99_us: None,
        };
        inventory.record_telemetry(disk_id, &tel).unwrap();

        let state = inventory.get_state(disk_id).unwrap();
        assert!(
            state == DiskState::Suspect
                || state == DiskState::Degraded
                || state == DiskState::Failed
        );
    }

    #[test]
    fn test_record_telemetry_unknown_disk() {
        let inventory = DiskInventory::new();
        let tel = DiskTelemetry::default();
        assert!(
            inventory
                .record_telemetry(DiskId::generate(), &tel)
                .is_err()
        );
    }

    #[test]
    fn test_validate_xfs_marker_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        std::fs::write(path.join(".blockyard_xfs_ok"), "").unwrap();
        assert!(validate_xfs(path).is_ok());
    }

    #[test]
    fn test_validate_xfs_rejects_non_xfs_without_marker() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        let result = validate_xfs(path);
        assert!(result.is_err(), "should reject directory without XFS or marker");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("XFS") || err.contains("xfs"),
            "error should mention XFS: {err}"
        );
    }

    #[test]
    fn test_validate_xfs_skip_env_var() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        // SAFETY: test is single-threaded for this env var; no other test reads
        // BLOCKYARD_SKIP_XFS_CHECK concurrently.
        unsafe { std::env::set_var("BLOCKYARD_SKIP_XFS_CHECK", "1") };
        let result = validate_xfs(path);
        unsafe { std::env::remove_var("BLOCKYARD_SKIP_XFS_CHECK") };
        assert!(result.is_ok(), "should accept when env var is set");
    }

    #[test]
    fn test_validate_xfs_env_var_not_1_still_validates() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        // SAFETY: test is single-threaded for this env var; no other test reads
        // BLOCKYARD_SKIP_XFS_CHECK concurrently.
        unsafe { std::env::set_var("BLOCKYARD_SKIP_XFS_CHECK", "0") };
        let result = validate_xfs(path);
        unsafe { std::env::remove_var("BLOCKYARD_SKIP_XFS_CHECK") };
        assert!(result.is_err(), "env var value '0' should not skip validation");
    }

    #[test]
    fn test_read_or_assign_disk_id_new() {
        let dir = tempfile::tempdir().unwrap();
        let id = read_or_assign_disk_id(dir.path(), None).unwrap();
        assert!(dir.path().join(DISK_ID_FILENAME).exists());

        let id2 = read_or_assign_disk_id(dir.path(), None).unwrap();
        assert_eq!(id, id2);
    }

    #[test]
    fn test_read_disk_id_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(DISK_ID_FILENAME), "not json").unwrap();
        assert!(read_or_assign_disk_id(dir.path(), None).is_err());
    }

    #[test]
    fn test_disk_metadata_serde() {
        let meta = DiskMetadata {
            disk_id: DiskId::generate(),
            node_id: None,
        };
        let json = serde_json::to_string(&meta).unwrap();
        let parsed: DiskMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(meta.disk_id, parsed.disk_id);
    }

    #[test]
    fn test_read_or_assign_disk_id_sets_node_id() {
        let dir = tempfile::tempdir().unwrap();
        let nid = blockyard_common::NodeId::generate();
        let _id = read_or_assign_disk_id(dir.path(), Some(nid)).unwrap();

        let contents =
            std::fs::read_to_string(dir.path().join(DISK_ID_FILENAME)).unwrap();
        let meta: DiskMetadata = serde_json::from_str(contents.trim()).unwrap();
        assert_eq!(meta.node_id, Some(nid));
    }

    #[test]
    fn test_discover_disks_for_node_binds_node_id() {
        let (_dir, path) = setup_disk_dir();
        let nid = blockyard_common::NodeId::generate();
        let inventory = DiskInventory::new();
        let ids = inventory
            .discover_disks_for_node(&[path.clone()], false, Some(nid))
            .unwrap();
        assert_eq!(ids.len(), 1);

        let contents =
            std::fs::read_to_string(path.join(DISK_ID_FILENAME)).unwrap();
        let meta: DiskMetadata = serde_json::from_str(contents.trim()).unwrap();
        assert_eq!(meta.node_id, Some(nid));
    }

    #[test]
    fn test_default_inventory() {
        let inventory = DiskInventory::default();
        assert!(inventory.list_disks().is_empty());
    }
}
