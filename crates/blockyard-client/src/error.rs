//! Client read path error types.

use blockyard_common::{ExtentId, NodeId, VolumeId};

/// Errors that can occur during read path operations.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("extent not found: volume={volume_id}, extent={extent_id}")]
    ExtentNotFound {
        volume_id: VolumeId,
        extent_id: ExtentId,
    },

    #[error("no healthy replicas available for extent {extent_id}")]
    NoHealthyReplicas { extent_id: ExtentId },

    #[error("all replicas failed for extent {extent_id}")]
    AllReplicasFailed { extent_id: ExtentId },

    #[error("all replicas returned corrupt data for extent {extent_id}")]
    AllReplicasCorrupt { extent_id: ExtentId },

    #[error("checksum mismatch on node {node_id} for extent {extent_id}: expected={expected}, got={actual}")]
    ChecksumMismatch {
        node_id: NodeId,
        extent_id: ExtentId,
        expected: String,
        actual: String,
    },

    #[error("stale mapping for extent {extent_id}: mapping_version={mapping_version}, required={required_version}")]
    StaleMapping {
        extent_id: ExtentId,
        mapping_version: u64,
        required_version: u64,
    },

    #[error("metadata refresh failed: {0}")]
    MetadataRefreshFailed(String),

    #[error("data node read failed on node {node_id}: {reason}")]
    DataNodeReadFailed {
        node_id: NodeId,
        reason: String,
    },

    #[error("invalid read request: {0}")]
    InvalidRequest(String),
}
