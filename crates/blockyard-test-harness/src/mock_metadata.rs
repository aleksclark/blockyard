use std::collections::HashMap;

use blockyard_common::{EpochId, LeaseResponse, OperationId, SessionId, VolumeId};
use blockyard_ublk::traits::{CommitRequest, CommittedMapping};
use blockyard_ublk::{MetadataCache, MetadataClient};
use parking_lot::Mutex;

pub struct TestMetadataClient {
    pub epoch: Mutex<EpochId>,
    pub committed: Mutex<Vec<CommitRequest>>,
    pub committed_ops: Mutex<HashMap<OperationId, CommittedMapping>>,
    pub fail_commit: Mutex<bool>,
    pub stale_epoch_on_commit: Mutex<bool>,
}

impl TestMetadataClient {
    pub fn new(epoch: EpochId) -> Self {
        Self {
            epoch: Mutex::new(epoch),
            committed: Mutex::new(Vec::new()),
            committed_ops: Mutex::new(HashMap::new()),
            fail_commit: Mutex::new(false),
            stale_epoch_on_commit: Mutex::new(false),
        }
    }

    pub fn set_epoch(&self, epoch: EpochId) {
        *self.epoch.lock() = epoch;
    }
}

impl MetadataClient for TestMetadataClient {
    async fn refresh_metadata(
        &self,
        cache: &MetadataCache,
    ) -> Result<EpochId, blockyard_common::Error> {
        let epoch = *self.epoch.lock();
        cache.set_epoch(epoch);
        Ok(epoch)
    }

    async fn commit_extent_mapping(
        &self,
        request: CommitRequest,
    ) -> Result<EpochId, blockyard_common::Error> {
        if *self.fail_commit.lock() {
            return Err(blockyard_common::Error::Raft("commit failed".to_string()));
        }
        if *self.stale_epoch_on_commit.lock() {
            return Err(blockyard_common::Error::Raft("stale epoch".to_string()));
        }
        let epoch = *self.epoch.lock();
        if let Some(op_id) = &request.operation_id {
            let mapping = CommittedMapping {
                extent_id: request.extent_id,
                extent_version: request.extent_version,
                epoch,
                block_range: request.block_range.clone(),
                replica_locations: request.replica_locations.clone(),
                checksums: request.checksums.clone(),
            };
            self.committed_ops.lock().insert(*op_id, mapping);
        }
        self.committed.lock().push(request);
        Ok(epoch)
    }

    fn commit_extent_mappings_batch(
        &self,
        requests: Vec<CommitRequest>,
    ) -> impl std::future::Future<Output = Result<EpochId, blockyard_common::Error>> + Send {
        let fail = *self.fail_commit.lock();
        let stale = *self.stale_epoch_on_commit.lock();
        let epoch = *self.epoch.lock();
        if !fail && !stale {
            for request in &requests {
                if let Some(op_id) = &request.operation_id {
                    let mapping = CommittedMapping {
                        extent_id: request.extent_id,
                        extent_version: request.extent_version,
                        epoch,
                        block_range: request.block_range.clone(),
                        replica_locations: request.replica_locations.clone(),
                        checksums: request.checksums.clone(),
                    };
                    self.committed_ops.lock().insert(*op_id, mapping);
                }
                self.committed.lock().push(request.clone());
            }
        }
        async move {
            if fail {
                return Err(blockyard_common::Error::Raft("commit failed".to_string()));
            }
            if stale {
                return Err(blockyard_common::Error::Raft("stale epoch".to_string()));
            }
            Ok(epoch)
        }
    }

    async fn lookup_operation(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<CommittedMapping>, blockyard_common::Error> {
        Ok(self.committed_ops.lock().get(operation_id).cloned())
    }

    async fn current_epoch(&self) -> Result<EpochId, blockyard_common::Error> {
        Ok(*self.epoch.lock())
    }

    async fn acquire_lease(
        &self,
        _volume_id: VolumeId,
        _session_id: SessionId,
        _now_ms: u64,
        _ttl_ms: u64,
    ) -> Result<LeaseResponse, blockyard_common::Error> {
        Ok(LeaseResponse::Granted {
            lease_version: 1,
            expires_at_ms: u64::MAX,
        })
    }

    async fn renew_lease(
        &self,
        _volume_id: VolumeId,
        _session_id: SessionId,
        _now_ms: u64,
        _ttl_ms: u64,
    ) -> Result<LeaseResponse, blockyard_common::Error> {
        Ok(LeaseResponse::Renewed {
            lease_version: 1,
            expires_at_ms: u64::MAX,
        })
    }

    async fn release_lease(
        &self,
        _volume_id: VolumeId,
        _session_id: SessionId,
    ) -> Result<LeaseResponse, blockyard_common::Error> {
        Ok(LeaseResponse::Released)
    }
}
