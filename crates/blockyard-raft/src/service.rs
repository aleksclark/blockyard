//! High-level metadata service API.
//!
//! [`MetadataService`] wraps a Raft node to provide the strongly consistent
//! metadata operations required by the Blockyard spec:
//!
//! - **P3.4** Metadata commit path: validate → apply via Raft → return commit version
//! - **P3.6** Committed state query: lookup by operation ID or extent version
//! - **P3.8** Quorum partition handling: minority nodes refuse new commits
//!
//! Quorum enforcement (P3.8, §6.4, invariant 10) is provided by Raft itself:
//! `client_write` will return `ForwardToLeader` or timeout if the node is in
//! a minority partition, which the caller must handle.

use openraft::Raft;
use openraft::error::{ClientWriteError, RaftError};

use blockyard_common::error::Error;
use blockyard_common::{EpochId, ExtentId, NodeId, OperationId, ProtectionPolicy, VolumeId};

use crate::request::MetadataRequest;
use crate::response::MetadataResponse;
use crate::state_machine::{ClusterNode, ExtentMapping, VolumeMetadata};
use crate::store::StateMachineStore;
use crate::typ::TypeConfig;

/// The metadata service: a strongly consistent replicated state machine
/// accessed through Raft consensus.
#[derive(Clone)]
pub struct MetadataService {
    raft: Raft<TypeConfig>,
    sm: StateMachineStore,
}

impl MetadataService {
    pub fn new(raft: Raft<TypeConfig>, sm: StateMachineStore) -> Self {
        Self { raft, sm }
    }

    /// Submit a metadata request through Raft consensus.
    ///
    /// Returns `Error::Raft` if the node is not the leader or if the quorum
    /// is unavailable (P3.8 — minority partitions cannot commit).
    async fn commit(&self, req: MetadataRequest) -> Result<MetadataResponse, Error> {
        let result = self
            .raft
            .client_write(req)
            .await
            .map_err(map_client_write_error)?;
        Ok(result.data)
    }

    /// Add or update a node in the cluster membership (P3.2).
    pub async fn add_node(&self, node_id: NodeId, addr: String) -> Result<(), Error> {
        let resp = self
            .commit(MetadataRequest::AddNode { node_id, addr })
            .await?;
        check_response(resp)
    }

    /// Remove a node from the cluster membership (P3.2).
    pub async fn remove_node(&self, node_id: NodeId) -> Result<(), Error> {
        let resp = self.commit(MetadataRequest::RemoveNode { node_id }).await?;
        check_response(resp)
    }

    /// Create a new volume (P3.2).
    pub async fn create_volume(
        &self,
        volume_id: VolumeId,
        size_bytes: u64,
        protection: ProtectionPolicy,
    ) -> Result<(), Error> {
        let resp = self
            .commit(MetadataRequest::CreateVolume {
                volume_id,
                size_bytes,
                protection,
            })
            .await?;
        check_response(resp)
    }

    /// Delete a volume and all its extent mappings (P3.2).
    pub async fn delete_volume(&self, volume_id: VolumeId) -> Result<(), Error> {
        let resp = self
            .commit(MetadataRequest::DeleteVolume { volume_id })
            .await?;
        check_response(resp)
    }

    /// Advance the placement epoch (P3.3). Returns the new epoch.
    pub async fn advance_epoch(&self) -> Result<EpochId, Error> {
        let resp = self.commit(MetadataRequest::AdvanceEpoch).await?;
        match resp {
            MetadataResponse::Epoch(e) => Ok(e),
            MetadataResponse::Error(msg) => Err(Error::Raft(msg)),
            MetadataResponse::Ok => Err(Error::Raft(
                "unexpected Ok response from advance_epoch".into(),
            )),
        }
    }

    /// Commit an extent mapping (P3.4, P3.5).
    ///
    /// Validates the epoch matches the current epoch. Supports optional
    /// compare-and-swap via `previous_version`.
    #[allow(clippy::too_many_arguments)]
    pub async fn commit_extent_mapping(
        &self,
        volume_id: VolumeId,
        block_range: std::ops::Range<u64>,
        extent_id: ExtentId,
        extent_version: u64,
        epoch: EpochId,
        replica_locations: Vec<NodeId>,
        checksums: Vec<Vec<u8>>,
        operation_id: Option<OperationId>,
        previous_version: Option<u64>,
    ) -> Result<EpochId, Error> {
        let resp = self
            .commit(MetadataRequest::CommitExtentMapping {
                volume_id,
                block_range,
                extent_id,
                extent_version,
                epoch,
                replica_locations,
                checksums,
                operation_id,
                previous_version,
            })
            .await?;
        match resp {
            MetadataResponse::Epoch(e) => Ok(e),
            MetadataResponse::Error(msg) => Err(Error::Raft(msg)),
            MetadataResponse::Ok => Err(Error::Raft(
                "unexpected Ok response from commit_extent_mapping".into(),
            )),
        }
    }

    /// Query committed state by operation ID (P3.6).
    pub fn lookup_by_operation_id(&self, op_id: &OperationId) -> Option<ExtentMapping> {
        self.sm.data().lookup_by_operation_id(op_id).cloned()
    }

    /// Query committed state by extent version (P3.6).
    pub fn lookup_by_extent_version(&self, version: u64) -> Option<ExtentMapping> {
        self.sm.data().lookup_by_extent_version(version).cloned()
    }

    /// Get volume metadata (read from local committed state).
    pub fn get_volume(&self, volume_id: &VolumeId) -> Option<VolumeMetadata> {
        self.sm.data().get_volume(volume_id).cloned()
    }

    /// List all volumes.
    pub fn list_volumes(&self) -> Vec<VolumeMetadata> {
        self.sm.data().list_volumes().into_iter().cloned().collect()
    }

    /// Get a cluster node.
    pub fn get_node(&self, node_id: &NodeId) -> Option<ClusterNode> {
        self.sm.data().get_node(node_id).cloned()
    }

    /// List all cluster nodes.
    pub fn list_nodes(&self) -> Vec<ClusterNode> {
        self.sm.data().list_nodes().into_iter().cloned().collect()
    }

    /// Get the current placement epoch (P3.3).
    pub fn current_epoch(&self) -> EpochId {
        self.sm.data().current_epoch()
    }

    /// Get a reference to the underlying Raft instance.
    pub fn raft(&self) -> &Raft<TypeConfig> {
        &self.raft
    }
}

fn check_response(resp: MetadataResponse) -> Result<(), Error> {
    match resp {
        MetadataResponse::Ok | MetadataResponse::Epoch(_) => Ok(()),
        MetadataResponse::Error(msg) => Err(Error::Raft(msg)),
    }
}

fn map_client_write_error(e: RaftError<u64, ClientWriteError<u64, openraft::BasicNode>>) -> Error {
    Error::Raft(format!("{e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_response_ok() {
        assert!(check_response(MetadataResponse::Ok).is_ok());
    }

    #[test]
    fn test_check_response_epoch() {
        assert!(check_response(MetadataResponse::Epoch(EpochId::new(1))).is_ok());
    }

    #[test]
    fn test_check_response_error() {
        let result = check_response(MetadataResponse::Error("fail".into()));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("fail"));
    }
}
