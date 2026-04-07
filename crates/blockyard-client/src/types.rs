//! Types used by the client read path.

use blockyard_common::{EpochId, ExtentId, NodeId, VolumeId};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Describes where an extent's replicas live and the current version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtentMapping {
    pub volume_id: VolumeId,
    pub extent_id: ExtentId,
    pub extent_version: u64,
    pub epoch: EpochId,
    pub replicas: Vec<ReplicaLocation>,
    pub checksum: String,
}

/// A single replica's location.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaLocation {
    pub node_id: NodeId,
    pub is_local: bool,
}

/// A request to read a logical block range from a volume.
#[derive(Debug, Clone)]
pub struct ReadRequest {
    pub volume_id: VolumeId,
    pub extent_id: ExtentId,
    pub offset: u64,
    pub length: u64,
}

/// Successful read result returned to the caller.
#[derive(Debug, Clone)]
pub struct ReadResult {
    pub extent_id: ExtentId,
    pub extent_version: u64,
    pub data: Bytes,
    pub source_node: NodeId,
}

/// Data returned from a data node read.
#[derive(Debug, Clone)]
pub struct DataNodeReadResult {
    pub extent_id: ExtentId,
    pub extent_version: u64,
    pub checksum: String,
    pub data: Bytes,
}

/// Health status of a replica, used for source selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaHealth {
    Healthy,
    Suspect,
    Failed,
}

/// Snapshot of a replica's measured quality for selection.
#[derive(Debug, Clone)]
pub struct ReplicaStats {
    pub node_id: NodeId,
    pub health: ReplicaHealth,
    pub latency_us: u64,
    pub is_local: bool,
    pub failure_count: u32,
}

/// A corruption event to report to the health subsystem.
#[derive(Debug, Clone)]
pub struct CorruptionReport {
    pub node_id: NodeId,
    pub extent_id: ExtentId,
    pub extent_version: u64,
    pub expected_checksum: String,
    pub actual_checksum: String,
}

/// A read failure event to report to the health subsystem.
#[derive(Debug, Clone)]
pub struct ReadFailureReport {
    pub node_id: NodeId,
    pub extent_id: ExtentId,
    pub reason: String,
}
