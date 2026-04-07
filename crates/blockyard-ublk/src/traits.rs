//! Traits for network communication with data nodes and metadata service.
//!
//! These traits abstract the network layer so that tests can use mock
//! implementations without requiring real cluster connectivity.

use std::ops::Range;

use bytes::Bytes;

use blockyard_common::error::Error;
use blockyard_common::{EpochId, ExtentId, NodeId, OperationId, SessionId, VolumeId};

use crate::metadata_cache::MetadataCache;

/// Result of a data node write acknowledgment.
#[derive(Debug, Clone)]
pub struct WriteAck {
    pub node_id: NodeId,
    pub success: bool,
    pub checksum: String,
    pub error: Option<WriteAckError>,
}

/// Error type within a write ack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteAckError {
    StaleEpoch,
    DiskUnavailable,
    DuplicateOperation,
    InternalError(String),
}

/// Request to commit an extent mapping to the metadata service.
#[derive(Debug, Clone)]
pub struct CommitRequest {
    pub volume_id: VolumeId,
    pub block_range: Range<u64>,
    pub extent_id: ExtentId,
    pub extent_version: u64,
    pub epoch: EpochId,
    pub replica_locations: Vec<NodeId>,
    pub checksums: Vec<Vec<u8>>,
    pub operation_id: Option<OperationId>,
    pub previous_version: Option<u64>,
}

/// A committed extent mapping returned from metadata queries.
#[derive(Debug, Clone)]
pub struct CommittedMapping {
    pub extent_id: ExtentId,
    pub extent_version: u64,
    pub epoch: EpochId,
    pub block_range: Range<u64>,
    pub replica_locations: Vec<NodeId>,
    pub checksums: Vec<Vec<u8>>,
}

/// Trait for sending write data to individual data nodes.
///
/// Abstracts the data plane so tests can mock network communication.
#[allow(async_fn_in_trait)]
pub trait DataNodeClient: Send + Sync + 'static {
    /// Send extent data to a specific data node for local storage.
    #[allow(clippy::too_many_arguments)]
    async fn write_extent(
        &self,
        node_id: NodeId,
        operation_id: OperationId,
        session_id: SessionId,
        volume_id: VolumeId,
        extent_id: ExtentId,
        extent_version: u64,
        epoch: EpochId,
        data: Bytes,
        checksum: String,
    ) -> Result<WriteAck, Error>;
}

/// Trait for interacting with the metadata service.
///
/// Abstracts the metadata plane so tests can mock consensus operations.
#[allow(async_fn_in_trait)]
pub trait MetadataClient: Send + Sync + 'static {
    /// Refresh the metadata cache with current cluster state.
    /// Returns the new epoch.
    async fn refresh_metadata(&self, cache: &MetadataCache) -> Result<EpochId, Error>;

    /// Commit an extent mapping through Raft consensus.
    /// Returns the commit epoch.
    async fn commit_extent_mapping(&self, request: CommitRequest) -> Result<EpochId, Error>;

    /// Look up a previously committed operation by its ID (for ambiguous write resolution).
    async fn lookup_operation(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<CommittedMapping>, Error>;

    /// Get the current placement epoch from the metadata service.
    async fn current_epoch(&self) -> Result<EpochId, Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_ack_debug() {
        let ack = WriteAck {
            node_id: NodeId::generate(),
            success: true,
            checksum: "abc".into(),
            error: None,
        };
        let debug = format!("{:?}", ack);
        assert!(debug.contains("WriteAck"));
    }

    #[test]
    fn test_write_ack_error_variants() {
        assert_eq!(WriteAckError::StaleEpoch, WriteAckError::StaleEpoch);
        assert_ne!(WriteAckError::StaleEpoch, WriteAckError::DiskUnavailable);
        assert_ne!(
            WriteAckError::DuplicateOperation,
            WriteAckError::InternalError("x".into())
        );
    }

    #[test]
    fn test_write_ack_clone() {
        let ack = WriteAck {
            node_id: NodeId::generate(),
            success: false,
            checksum: "xyz".into(),
            error: Some(WriteAckError::StaleEpoch),
        };
        let cloned = ack.clone();
        assert_eq!(cloned.success, false);
        assert_eq!(cloned.error, Some(WriteAckError::StaleEpoch));
    }

    #[test]
    fn test_commit_request_debug() {
        let req = CommitRequest {
            volume_id: VolumeId::generate(),
            block_range: 0..64,
            extent_id: ExtentId::generate(),
            extent_version: 1,
            epoch: EpochId::new(1),
            replica_locations: vec![NodeId::generate()],
            checksums: vec![vec![0xFF]],
            operation_id: Some(OperationId::generate()),
            previous_version: None,
        };
        let debug = format!("{:?}", req);
        assert!(debug.contains("CommitRequest"));
    }

    #[test]
    fn test_commit_request_clone() {
        let req = CommitRequest {
            volume_id: VolumeId::generate(),
            block_range: 0..64,
            extent_id: ExtentId::generate(),
            extent_version: 1,
            epoch: EpochId::new(1),
            replica_locations: vec![],
            checksums: vec![],
            operation_id: None,
            previous_version: Some(0),
        };
        let cloned = req.clone();
        assert_eq!(cloned.extent_version, 1);
        assert_eq!(cloned.previous_version, Some(0));
    }

    #[test]
    fn test_committed_mapping_debug() {
        let m = CommittedMapping {
            extent_id: ExtentId::generate(),
            extent_version: 2,
            epoch: EpochId::new(3),
            block_range: 0..128,
            replica_locations: vec![],
            checksums: vec![],
        };
        let debug = format!("{:?}", m);
        assert!(debug.contains("CommittedMapping"));
    }

    #[test]
    fn test_committed_mapping_clone() {
        let m = CommittedMapping {
            extent_id: ExtentId::generate(),
            extent_version: 42,
            epoch: EpochId::new(7),
            block_range: 64..128,
            replica_locations: vec![NodeId::generate()],
            checksums: vec![vec![1, 2]],
        };
        let cloned = m.clone();
        assert_eq!(cloned.extent_version, 42);
    }

    #[test]
    fn test_write_ack_error_debug() {
        let e = WriteAckError::InternalError("disk failure".into());
        let debug = format!("{:?}", e);
        assert!(debug.contains("InternalError"));
        assert!(debug.contains("disk failure"));
    }

    #[test]
    fn test_write_ack_error_clone() {
        let e = WriteAckError::DiskUnavailable;
        let cloned = e.clone();
        assert_eq!(e, cloned);
    }

    #[test]
    fn test_write_ack_with_all_error_variants() {
        let errors = vec![
            WriteAckError::StaleEpoch,
            WriteAckError::DiskUnavailable,
            WriteAckError::DuplicateOperation,
            WriteAckError::InternalError("test".into()),
        ];
        for err in &errors {
            let ack = WriteAck {
                node_id: NodeId::generate(),
                success: false,
                checksum: String::new(),
                error: Some(err.clone()),
            };
            assert!(!ack.success);
        }
    }
}
