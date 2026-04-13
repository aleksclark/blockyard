//! Metadata service request types applied to the Raft state machine.

use std::collections::BTreeMap;
use std::ops::Range;

use serde::{Deserialize, Serialize};

use blockyard_common::{
    DiskId, EpochId, ExtentId, LeaseRequest, NodeId, OperationId, ProtectionPolicy, VolumeId,
};

/// A single extent mapping entry used in both individual and batch commit requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtentMappingEntry {
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

/// A request to be applied to the metadata state machine via Raft consensus.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataRequest {
    /// Add or update a node in the cluster membership.
    AddNode { node_id: NodeId, addr: String },

    /// Remove a node from the cluster membership.
    RemoveNode { node_id: NodeId },

    /// Create a new volume with the given protection policy.
    CreateVolume {
        volume_id: VolumeId,
        size_bytes: u64,
        protection: ProtectionPolicy,
        /// Extent size in bytes. Multiple blocks are grouped into one extent.
        /// Default: 524288 (512KB = 128 blocks at 4096 block_size).
        extent_size: u64,
    },

    /// Delete a volume and all its extent mappings.
    DeleteVolume { volume_id: VolumeId },

    /// Advance the placement epoch, returning the new epoch value.
    AdvanceEpoch,

    /// Commit an extent mapping (§4.5.2).
    CommitExtentMapping {
        volume_id: VolumeId,
        block_range: Range<u64>,
        extent_id: ExtentId,
        extent_version: u64,
        epoch: EpochId,
        replica_locations: Vec<NodeId>,
        checksums: Vec<Vec<u8>>,
        operation_id: Option<OperationId>,
        /// Previous mapping version for compare-and-swap.
        previous_version: Option<u64>,
    },

    /// Commit multiple extent mappings in a single Raft proposal (batch optimization).
    /// All mappings are applied atomically — either all succeed or none do.
    CommitExtentMappingBatch { mappings: Vec<ExtentMappingEntry> },

    /// Update the placement map for a set of nodes.
    UpdatePlacementMap {
        assignments: BTreeMap<String, Vec<NodeId>>,
    },

    /// Acquire, renew, or release a volume write lease (P6.1).
    Lease(LeaseRequest),

    /// Register a node and assign it a raft u64 ID.
    /// Returns [`MetadataResponse::NodeRegistered`] with the assigned raft ID.
    RegisterNode { node_id: NodeId, addr: String },

    /// Register a disk in the cluster metadata.
    RegisterDisk {
        disk_id: DiskId,
        node_id: NodeId,
        capacity_bytes: u64,
    },

    /// Update the used bytes for a registered disk.
    UpdateDiskUsage { disk_id: DiskId, used_bytes: u64 },

    /// Deregister a disk from the cluster metadata.
    DeregisterDisk { disk_id: DiskId },

    /// Remove all expired leases from the state machine.
    CleanupExpiredLeases { now_ms: u64 },
}
