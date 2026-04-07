//! Data node read/write service (§5.5, §5.6, P2.3–P2.8).
//!
//! Handles write reception, read service, duplicate operation suppression,
//! stale-epoch rejection, checksum mismatch handling, and XFS error handling.

use std::collections::HashMap;

use blockyard_common::{DiskId, DiskState, EpochId, ExtentId, OperationId};
use blockyard_protocol::{
    ReadExtentRequest, ReadExtentResponse, WriteExtentRequest, WriteExtentResponse,
};
use parking_lot::RwLock;
use tracing::{debug, info, warn};

use crate::disk::DiskInventory;
use crate::error::StorageError;
use crate::extent::{ExtentIndex, ExtentStore, ExtentVersion, StorageClass, compute_checksum};

/// Record of a completed write operation for dedup (P2.4, §5.5.5).
#[derive(Debug, Clone)]
pub struct OperationRecord {
    pub operation_id: OperationId,
    pub extent_id: ExtentId,
    pub extent_version: ExtentVersion,
    pub disk_id: DiskId,
    pub checksum: String,
    pub success: bool,
}

/// Data node service managing read/write operations.
#[derive(Debug)]
pub struct DataNodeService {
    current_epoch: RwLock<EpochId>,
    operation_log: RwLock<HashMap<OperationId, OperationRecord>>,
    stores: RwLock<HashMap<DiskId, ExtentStore>>,
    inventory: DiskInventory,
    index: ExtentIndex,
}

impl DataNodeService {
    pub fn new(inventory: DiskInventory, index: ExtentIndex, epoch: EpochId) -> Self {
        Self {
            current_epoch: RwLock::new(epoch),
            operation_log: RwLock::new(HashMap::new()),
            stores: RwLock::new(HashMap::new()),
            inventory,
            index,
        }
    }

    /// Register an ExtentStore for a disk.
    pub fn register_store(&self, disk_id: DiskId, store: ExtentStore) {
        let mut stores = self.stores.write();
        stores.insert(disk_id, store);
    }

    /// Update the current epoch.
    pub fn set_epoch(&self, epoch: EpochId) {
        let mut current = self.current_epoch.write();
        *current = epoch;
    }

    /// Get the current epoch.
    pub fn current_epoch(&self) -> EpochId {
        *self.current_epoch.read()
    }

    /// Handle a write extent request (P2.3: epoch validation → disk eligibility → stage → persist → record → ack).
    pub fn handle_write(
        &self,
        request: &WriteExtentRequest,
        payload: &[u8],
    ) -> WriteExtentResponse {
        let op_id = request.operation_id;

        if let Some(record) = self.check_duplicate(op_id) {
            debug!(%op_id, "duplicate write operation detected");
            return WriteExtentResponse {
                operation_id: op_id,
                extent_id: record.extent_id,
                extent_version: record.extent_version,
                disk_id: record.disk_id,
                success: record.success,
                checksum: record.checksum,
                error: if record.success {
                    None
                } else {
                    Some("previous attempt failed".into())
                },
            };
        }

        if let Err(msg) = self.validate_epoch(request.epoch) {
            return self.write_error(request, msg);
        }

        let disk_id = match self.select_write_disk(request.target_disk_id) {
            Ok(id) => id,
            Err(e) => return self.write_error(request, e.to_string()),
        };

        let payload_checksum = compute_checksum(payload);
        if request.checksum != payload_checksum {
            return self.write_error(
                request,
                format!(
                    "payload checksum mismatch: expected {}, got {}",
                    request.checksum, payload_checksum
                ),
            );
        }

        let stores = self.stores.read();
        let store = match stores.get(&disk_id) {
            Some(s) => s,
            None => {
                return self.write_error(request, format!("no extent store for disk {disk_id}"));
            }
        };

        let staged = match store.stage_extent(request.extent_id, request.extent_version, payload) {
            Ok(s) => s,
            Err(e) => {
                self.handle_xfs_error(disk_id, &e);
                return self.write_error(request, format!("staging failed: {e}"));
            }
        };

        let (_staged_path, checksum) = staged;

        let entry = match store.commit_extent(
            request.extent_id,
            request.extent_version,
            &checksum,
            payload.len() as u64,
            StorageClass::Default,
        ) {
            Ok(e) => e,
            Err(e) => {
                self.handle_xfs_error(disk_id, &e);
                return self.write_error(request, format!("commit failed: {e}"));
            }
        };

        if let Err(e) = self.index.insert(entry) {
            warn!(%op_id, error = %e, "failed to insert into extent index");
        }

        let record = OperationRecord {
            operation_id: op_id,
            extent_id: request.extent_id,
            extent_version: request.extent_version,
            disk_id,
            checksum: checksum.clone(),
            success: true,
        };
        self.record_operation(record);

        info!(
            %op_id,
            extent_id = %request.extent_id,
            version = request.extent_version,
            %disk_id,
            "write completed successfully"
        );

        WriteExtentResponse {
            operation_id: op_id,
            extent_id: request.extent_id,
            extent_version: request.extent_version,
            disk_id,
            success: true,
            checksum,
            error: None,
        }
    }

    /// Handle a read extent request (P2.5: locate → verify readable → read → checksum → return).
    pub fn handle_read(
        &self,
        request: &ReadExtentRequest,
    ) -> (ReadExtentResponse, Option<Vec<u8>>) {
        let op_id = request.operation_id;

        if let Err(msg) = self.validate_epoch_for_read(request.epoch) {
            return (self.read_error(request, msg), None);
        }

        let entry = match self.index.get(request.extent_id) {
            Some(e) => e,
            None => {
                return (
                    self.read_error(request, format!("extent {} not found", request.extent_id)),
                    None,
                );
            }
        };

        if entry.version != request.extent_version {
            return (
                self.read_error(
                    request,
                    format!(
                        "version mismatch: requested {}, have {}",
                        request.extent_version, entry.version
                    ),
                ),
                None,
            );
        }

        if let Ok(false) = self.inventory.allows_reads(entry.disk_id) {
            self.inventory
                .transition_state(entry.disk_id, DiskState::Failed)
                .ok();
            return (
                self.read_error(request, format!("disk {} not readable", entry.disk_id)),
                None,
            );
        }

        let stores = self.stores.read();
        let store = match stores.get(&entry.disk_id) {
            Some(s) => s,
            None => {
                return (
                    self.read_error(
                        request,
                        format!("no extent store for disk {}", entry.disk_id),
                    ),
                    None,
                );
            }
        };

        let (data, read_checksum) =
            match store.read_extent(request.extent_id, request.extent_version) {
                Ok(r) => r,
                Err(e) => {
                    self.handle_read_error(entry.disk_id, &e);
                    return (self.read_error(request, format!("read failed: {e}")), None);
                }
            };

        if read_checksum != entry.checksum {
            warn!(
                extent_id = %request.extent_id,
                expected = %entry.checksum,
                got = %read_checksum,
                disk_id = %entry.disk_id,
                "checksum mismatch on read — marking disk suspect"
            );
            self.inventory
                .transition_state(entry.disk_id, DiskState::Suspect)
                .ok();

            return (
                self.read_error(
                    request,
                    format!(
                        "checksum mismatch: expected {}, got {read_checksum}",
                        entry.checksum
                    ),
                ),
                None,
            );
        }

        let payload_data = if request.offset == 0 && request.length == 0 {
            data
        } else {
            let start = request.offset as usize;
            let end = (request.offset + request.length) as usize;
            if end > data.len() {
                return (
                    self.read_error(
                        request,
                        format!(
                            "read range [{start}, {end}) exceeds extent size {}",
                            data.len()
                        ),
                    ),
                    None,
                );
            }
            data[start..end].to_vec()
        };

        let response = ReadExtentResponse {
            operation_id: op_id,
            extent_id: request.extent_id,
            extent_version: request.extent_version,
            success: true,
            checksum: read_checksum,
            payload_size: payload_data.len() as u64,
            error: None,
        };

        (response, Some(payload_data))
    }

    /// Validate epoch for write operations (P2.6: stale-epoch rejection).
    fn validate_epoch(&self, request_epoch: EpochId) -> Result<(), String> {
        let current = *self.current_epoch.read();
        if request_epoch.as_u64() < current.as_u64() {
            Err(format!(
                "stale epoch: request epoch {} < current {}",
                request_epoch, current
            ))
        } else {
            Ok(())
        }
    }

    /// Validate epoch for read operations (P2.6: conditional stale-epoch reads).
    fn validate_epoch_for_read(&self, request_epoch: EpochId) -> Result<(), String> {
        let current = *self.current_epoch.read();
        if request_epoch.as_u64() + 1 < current.as_u64() {
            Err(format!(
                "stale epoch for read: request epoch {} < current {} (tolerance exceeded)",
                request_epoch, current
            ))
        } else {
            Ok(())
        }
    }

    /// Check for duplicate operation (P2.4, §5.5.5).
    fn check_duplicate(&self, op_id: OperationId) -> Option<OperationRecord> {
        let log = self.operation_log.read();
        log.get(&op_id).cloned()
    }

    /// Record a completed operation for dedup.
    fn record_operation(&self, record: OperationRecord) {
        let mut log = self.operation_log.write();
        log.insert(record.operation_id, record);
    }

    /// Select a disk for writing, validating eligibility (P2.3 step 2).
    fn select_write_disk(&self, preferred: Option<DiskId>) -> Result<DiskId, StorageError> {
        if let Some(disk_id) = preferred {
            if self.inventory.allows_allocation(disk_id)? {
                return Ok(disk_id);
            }
            return Err(StorageError::AllocationDenied(format!(
                "preferred disk {disk_id} does not allow allocation"
            )));
        }

        let disks = self.inventory.list_disks();
        for disk_id in &disks {
            if self.inventory.allows_allocation(*disk_id).unwrap_or(false) {
                return Ok(*disk_id);
            }
        }

        Err(StorageError::AllocationDenied(
            "no eligible disks for allocation".into(),
        ))
    }

    /// Handle XFS errors on write (P2.8).
    fn handle_xfs_error(&self, disk_id: DiskId, error: &StorageError) {
        let error_str = error.to_string();
        if error_str.contains("No space left")
            || error_str.contains("read-only")
            || error_str.contains("Structure needs cleaning")
        {
            warn!(%disk_id, %error, "XFS filesystem error — transitioning disk to failed");
            self.inventory
                .transition_state(disk_id, DiskState::Failed)
                .ok();
        } else if error_str.contains("Input/output error") {
            warn!(%disk_id, %error, "XFS I/O error — transitioning disk to degraded");
            self.inventory
                .transition_state(disk_id, DiskState::Degraded)
                .ok();
        }
    }

    /// Handle read errors (P2.7: checksum mismatch → mark suspect).
    fn handle_read_error(&self, disk_id: DiskId, error: &StorageError) {
        match error {
            StorageError::ChecksumMismatch(_) => {
                warn!(%disk_id, %error, "checksum mismatch on read — marking disk suspect");
                self.inventory
                    .transition_state(disk_id, DiskState::Suspect)
                    .ok();
            }
            StorageError::Io(_) => {
                warn!(%disk_id, %error, "I/O error on read — marking disk degraded");
                self.inventory
                    .transition_state(disk_id, DiskState::Degraded)
                    .ok();
            }
            _ => {}
        }
    }

    fn write_error(&self, request: &WriteExtentRequest, message: String) -> WriteExtentResponse {
        let record = OperationRecord {
            operation_id: request.operation_id,
            extent_id: request.extent_id,
            extent_version: request.extent_version,
            disk_id: request.target_disk_id.unwrap_or_else(DiskId::generate),
            checksum: String::new(),
            success: false,
        };
        self.record_operation(record);

        WriteExtentResponse {
            operation_id: request.operation_id,
            extent_id: request.extent_id,
            extent_version: request.extent_version,
            disk_id: request.target_disk_id.unwrap_or_else(DiskId::generate),
            success: false,
            checksum: String::new(),
            error: Some(message),
        }
    }

    fn read_error(&self, request: &ReadExtentRequest, message: String) -> ReadExtentResponse {
        ReadExtentResponse {
            operation_id: request.operation_id,
            extent_id: request.extent_id,
            extent_version: request.extent_version,
            success: false,
            checksum: String::new(),
            payload_size: 0,
            error: Some(message),
        }
    }

    /// Get a reference to the extent index.
    pub fn index(&self) -> &ExtentIndex {
        &self.index
    }

    /// Get a reference to the disk inventory.
    pub fn inventory(&self) -> &DiskInventory {
        &self.inventory
    }

    /// Get operation count (for observability).
    pub fn operation_count(&self) -> usize {
        self.operation_log.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockyard_common::{SessionId, VolumeId};

    fn setup_service() -> (tempfile::TempDir, DataNodeService, DiskId) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        std::fs::write(path.join(".blockyard_xfs_ok"), "").unwrap();

        let inventory = DiskInventory::new();
        let ids = inventory.discover_disks(&[path.clone()], false).unwrap();
        let disk_id = ids[0];

        let index = ExtentIndex::new();
        let service = DataNodeService::new(inventory, index, EpochId::new(1));

        let store = ExtentStore::new(path, disk_id);
        store.init_directories().unwrap();
        service.register_store(disk_id, store);

        (dir, service, disk_id)
    }

    fn make_write_request(
        extent_id: ExtentId,
        version: u64,
        epoch: EpochId,
        disk_id: Option<DiskId>,
        checksum: &str,
        payload_size: u64,
    ) -> WriteExtentRequest {
        WriteExtentRequest {
            operation_id: OperationId::generate(),
            session_id: SessionId::generate(),
            volume_id: VolumeId::generate(),
            extent_id,
            extent_version: version,
            epoch,
            target_disk_id: disk_id,
            checksum: checksum.into(),
            payload_size,
        }
    }

    fn make_read_request(extent_id: ExtentId, version: u64, epoch: EpochId) -> ReadExtentRequest {
        ReadExtentRequest {
            operation_id: OperationId::generate(),
            session_id: SessionId::generate(),
            volume_id: VolumeId::generate(),
            extent_id,
            extent_version: version,
            epoch,
            offset: 0,
            length: 0,
        }
    }

    #[test]
    fn test_write_and_read_success() {
        let (_dir, service, disk_id) = setup_service();
        let eid = ExtentId::generate();
        let data = b"hello storage";
        let checksum = compute_checksum(data);

        let req = make_write_request(
            eid,
            1,
            EpochId::new(1),
            Some(disk_id),
            &checksum,
            data.len() as u64,
        );
        let resp = service.handle_write(&req, data);
        assert!(resp.success, "write failed: {:?}", resp.error);
        assert_eq!(resp.checksum, checksum);

        let read_req = make_read_request(eid, 1, EpochId::new(1));
        let (read_resp, payload) = service.handle_read(&read_req);
        assert!(read_resp.success, "read failed: {:?}", read_resp.error);
        assert_eq!(payload.unwrap(), data.to_vec());
    }

    #[test]
    fn test_write_stale_epoch_rejected() {
        let (_dir, service, disk_id) = setup_service();
        service.set_epoch(EpochId::new(10));

        let eid = ExtentId::generate();
        let data = b"stale";
        let checksum = compute_checksum(data);
        let req = make_write_request(
            eid,
            1,
            EpochId::new(5),
            Some(disk_id),
            &checksum,
            data.len() as u64,
        );

        let resp = service.handle_write(&req, data);
        assert!(!resp.success);
        assert!(resp.error.unwrap().contains("stale epoch"));
    }

    #[test]
    fn test_write_current_epoch_accepted() {
        let (_dir, service, disk_id) = setup_service();
        let eid = ExtentId::generate();
        let data = b"current";
        let checksum = compute_checksum(data);
        let req = make_write_request(
            eid,
            1,
            EpochId::new(1),
            Some(disk_id),
            &checksum,
            data.len() as u64,
        );

        let resp = service.handle_write(&req, data);
        assert!(resp.success);
    }

    #[test]
    fn test_duplicate_operation_suppression() {
        let (_dir, service, disk_id) = setup_service();
        let eid = ExtentId::generate();
        let data = b"dedup test";
        let checksum = compute_checksum(data);

        let mut req = make_write_request(
            eid,
            1,
            EpochId::new(1),
            Some(disk_id),
            &checksum,
            data.len() as u64,
        );
        let op_id = req.operation_id;

        let resp1 = service.handle_write(&req, data);
        assert!(resp1.success);

        req.operation_id = op_id;
        let resp2 = service.handle_write(&req, data);
        assert!(resp2.success);
        assert_eq!(resp2.operation_id, op_id);
    }

    #[test]
    fn test_read_nonexistent_extent() {
        let (_dir, service, _) = setup_service();
        let req = make_read_request(ExtentId::generate(), 1, EpochId::new(1));
        let (resp, payload) = service.handle_read(&req);
        assert!(!resp.success);
        assert!(payload.is_none());
    }

    #[test]
    fn test_read_version_mismatch() {
        let (_dir, service, disk_id) = setup_service();
        let eid = ExtentId::generate();
        let data = b"version test";
        let checksum = compute_checksum(data);

        let write_req = make_write_request(
            eid,
            1,
            EpochId::new(1),
            Some(disk_id),
            &checksum,
            data.len() as u64,
        );
        service.handle_write(&write_req, data);

        let read_req = make_read_request(eid, 2, EpochId::new(1));
        let (resp, _) = service.handle_read(&read_req);
        assert!(!resp.success);
        assert!(resp.error.unwrap().contains("version mismatch"));
    }

    #[test]
    fn test_read_stale_epoch_tolerated() {
        let (_dir, service, disk_id) = setup_service();
        let eid = ExtentId::generate();
        let data = b"read test";
        let checksum = compute_checksum(data);

        let write_req = make_write_request(
            eid,
            1,
            EpochId::new(1),
            Some(disk_id),
            &checksum,
            data.len() as u64,
        );
        service.handle_write(&write_req, data);

        service.set_epoch(EpochId::new(2));
        let read_req = make_read_request(eid, 1, EpochId::new(1));
        let (resp, payload) = service.handle_read(&read_req);
        assert!(resp.success, "read should tolerate epoch being 1 behind");
        assert!(payload.is_some());
    }

    #[test]
    fn test_read_very_stale_epoch_rejected() {
        let (_dir, service, disk_id) = setup_service();
        let eid = ExtentId::generate();
        let data = b"stale read";
        let checksum = compute_checksum(data);

        let write_req = make_write_request(
            eid,
            1,
            EpochId::new(1),
            Some(disk_id),
            &checksum,
            data.len() as u64,
        );
        service.handle_write(&write_req, data);

        service.set_epoch(EpochId::new(10));
        let read_req = make_read_request(eid, 1, EpochId::new(1));
        let (resp, _) = service.handle_read(&read_req);
        assert!(!resp.success);
        assert!(resp.error.unwrap().contains("stale epoch"));
    }

    #[test]
    fn test_write_checksum_mismatch() {
        let (_dir, service, disk_id) = setup_service();
        let eid = ExtentId::generate();
        let data = b"checksum test";

        let req = make_write_request(
            eid,
            1,
            EpochId::new(1),
            Some(disk_id),
            "wrong_checksum",
            data.len() as u64,
        );
        let resp = service.handle_write(&req, data);
        assert!(!resp.success);
        assert!(resp.error.unwrap().contains("checksum mismatch"));
    }

    #[test]
    fn test_write_allocation_denied_on_degraded_disk() {
        let (_dir, service, disk_id) = setup_service();
        service
            .inventory()
            .transition_state(disk_id, DiskState::Degraded)
            .unwrap();

        let eid = ExtentId::generate();
        let data = b"denied";
        let checksum = compute_checksum(data);
        let req = make_write_request(
            eid,
            1,
            EpochId::new(1),
            Some(disk_id),
            &checksum,
            data.len() as u64,
        );

        let resp = service.handle_write(&req, data);
        assert!(!resp.success);
        assert!(resp.error.unwrap().contains("allocation"));
    }

    #[test]
    fn test_write_no_eligible_disks() {
        let (_dir, service, disk_id) = setup_service();
        service
            .inventory()
            .transition_state(disk_id, DiskState::Failed)
            .unwrap();

        let eid = ExtentId::generate();
        let data = b"no disk";
        let checksum = compute_checksum(data);
        let req = make_write_request(eid, 1, EpochId::new(1), None, &checksum, data.len() as u64);

        let resp = service.handle_write(&req, data);
        assert!(!resp.success);
    }

    #[test]
    fn test_read_with_range() {
        let (_dir, service, disk_id) = setup_service();
        let eid = ExtentId::generate();
        let data = b"0123456789abcdef";
        let checksum = compute_checksum(data);

        let write_req = make_write_request(
            eid,
            1,
            EpochId::new(1),
            Some(disk_id),
            &checksum,
            data.len() as u64,
        );
        service.handle_write(&write_req, data);

        let mut read_req = make_read_request(eid, 1, EpochId::new(1));
        read_req.offset = 4;
        read_req.length = 4;
        let (resp, payload) = service.handle_read(&read_req);
        assert!(resp.success);
        assert_eq!(payload.unwrap(), b"4567".to_vec());
    }

    #[test]
    fn test_read_range_out_of_bounds() {
        let (_dir, service, disk_id) = setup_service();
        let eid = ExtentId::generate();
        let data = b"short";
        let checksum = compute_checksum(data);

        let write_req = make_write_request(
            eid,
            1,
            EpochId::new(1),
            Some(disk_id),
            &checksum,
            data.len() as u64,
        );
        service.handle_write(&write_req, data);

        let mut read_req = make_read_request(eid, 1, EpochId::new(1));
        read_req.offset = 0;
        read_req.length = 1000;
        let (resp, _) = service.handle_read(&read_req);
        assert!(!resp.success);
        assert!(resp.error.unwrap().contains("exceeds extent size"));
    }

    #[test]
    fn test_set_and_get_epoch() {
        let (_dir, service, _) = setup_service();
        assert_eq!(service.current_epoch(), EpochId::new(1));
        service.set_epoch(EpochId::new(42));
        assert_eq!(service.current_epoch(), EpochId::new(42));
    }

    #[test]
    fn test_operation_count() {
        let (_dir, service, disk_id) = setup_service();
        assert_eq!(service.operation_count(), 0);

        let eid = ExtentId::generate();
        let data = b"count test";
        let checksum = compute_checksum(data);
        let req = make_write_request(
            eid,
            1,
            EpochId::new(1),
            Some(disk_id),
            &checksum,
            data.len() as u64,
        );
        service.handle_write(&req, data);
        assert_eq!(service.operation_count(), 1);
    }

    #[test]
    fn test_read_from_failed_disk_rejected() {
        let (_dir, service, disk_id) = setup_service();
        let eid = ExtentId::generate();
        let data = b"will fail";
        let checksum = compute_checksum(data);

        let write_req = make_write_request(
            eid,
            1,
            EpochId::new(1),
            Some(disk_id),
            &checksum,
            data.len() as u64,
        );
        let resp = service.handle_write(&write_req, data);
        assert!(resp.success);

        service
            .inventory()
            .transition_state(disk_id, DiskState::Failed)
            .unwrap();

        let read_req = make_read_request(eid, 1, EpochId::new(1));
        let (resp, _) = service.handle_read(&read_req);
        assert!(!resp.success);
        assert!(resp.error.unwrap().contains("not readable"));
    }

    #[test]
    fn test_write_auto_selects_disk() {
        let (_dir, service, _disk_id) = setup_service();
        let eid = ExtentId::generate();
        let data = b"auto select";
        let checksum = compute_checksum(data);

        let req = make_write_request(eid, 1, EpochId::new(1), None, &checksum, data.len() as u64);
        let resp = service.handle_write(&req, data);
        assert!(resp.success);
    }

    #[test]
    fn test_multiple_writes_different_extents() {
        let (_dir, service, disk_id) = setup_service();

        for i in 0..5 {
            let eid = ExtentId::generate();
            let data = format!("extent data {i}");
            let checksum = compute_checksum(data.as_bytes());
            let req = make_write_request(
                eid,
                1,
                EpochId::new(1),
                Some(disk_id),
                &checksum,
                data.len() as u64,
            );
            let resp = service.handle_write(&req, data.as_bytes());
            assert!(resp.success, "write {i} failed: {:?}", resp.error);
        }

        assert_eq!(service.operation_count(), 5);
        assert_eq!(service.index().count(), 5);
    }

    #[test]
    fn test_read_detects_corruption_marks_suspect() {
        let (_dir, service, disk_id) = setup_service();
        let eid = ExtentId::generate();
        let data = b"corruption test";
        let checksum = compute_checksum(data);

        let write_req = make_write_request(
            eid,
            1,
            EpochId::new(1),
            Some(disk_id),
            &checksum,
            data.len() as u64,
        );
        let resp = service.handle_write(&write_req, data);
        assert!(resp.success);

        let mount_path = service.inventory().get_mount_path(disk_id).unwrap();
        let committed_path = crate::extent::committed_extent_path(&mount_path, eid, 1);
        std::fs::write(&committed_path, b"corrupted data").unwrap();

        let read_req = make_read_request(eid, 1, EpochId::new(1));
        let (resp, _) = service.handle_read(&read_req);
        assert!(!resp.success);

        let state = service.inventory().get_state(disk_id).unwrap();
        assert_eq!(state, DiskState::Suspect);
    }
}
