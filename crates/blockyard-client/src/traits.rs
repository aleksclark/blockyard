//! Traits defining the dependencies of the read pipeline.
//!
//! The read pipeline is generic over these traits, allowing mock
//! implementations for testing and real implementations for production.

use std::future::Future;

use blockyard_common::{ExtentId, NodeId, SessionId, VolumeId};

use crate::error::ReadError;
use crate::types::{
    CorruptionReport, DataNodeReadResult, ExtentMapping, ReadFailureReport, ReplicaLocation,
    ReplicaStats,
};

/// Provides extent mappings and session watermark information.
///
/// The metadata provider is the client's view of the metadata service.
/// It caches extent mappings and tracks the session write watermark
/// for read-your-own-writes enforcement (§4.4, §4.6).
pub trait MetadataProvider: Send + Sync {
    /// Returns the extent mapping for the given volume and extent.
    fn get_extent_mapping(
        &self,
        volume_id: VolumeId,
        extent_id: ExtentId,
    ) -> impl Future<Output = Result<Option<ExtentMapping>, ReadError>> + Send;

    /// Returns the session write watermark — the minimum extent version
    /// that reads must see to satisfy read-your-own-writes (§4.4).
    fn get_write_watermark(
        &self,
        session_id: SessionId,
        volume_id: VolumeId,
    ) -> impl Future<Output = Result<u64, ReadError>> + Send;

    /// Forces a metadata refresh for the given extent, bypassing cache.
    /// Called when the cached mapping is stale relative to the watermark.
    fn refresh_extent_mapping(
        &self,
        volume_id: VolumeId,
        extent_id: ExtentId,
    ) -> impl Future<Output = Result<Option<ExtentMapping>, ReadError>> + Send;
}

/// Reads extent data from a data node.
///
/// Abstracts the network call to a data node's read service (§5.6).
pub trait DataNodeReader: Send + Sync {
    /// Read extent data from the specified node.
    fn read_extent(
        &self,
        node_id: NodeId,
        volume_id: VolumeId,
        extent_id: ExtentId,
        extent_version: u64,
        offset: u64,
        length: u64,
    ) -> impl Future<Output = Result<DataNodeReadResult, ReadError>> + Send;
}

/// Reports corruption and read failures to the health subsystem.
///
/// When a checksum mismatch or read failure is detected, the read pipeline
/// reports it so the cluster can schedule repair (§4.9.4).
pub trait HealthReporter: Send + Sync {
    /// Report a checksum corruption event.
    fn report_corruption(&self, report: CorruptionReport) -> impl Future<Output = ()> + Send;

    /// Report a read failure (non-corruption) event.
    fn report_read_failure(&self, report: ReadFailureReport) -> impl Future<Output = ()> + Send;
}

/// Selects the best source replica for a read operation.
///
/// Prefers local replicas, then selects by lowest latency.
/// Tracks replica health and failure counts.
pub trait ReplicaSelector: Send + Sync {
    /// Order replicas by preference for a read. Returns an ordered list
    /// of node IDs, best candidate first.
    fn select_replicas(&self, replicas: &[ReplicaLocation]) -> Vec<NodeId>;

    /// Get current stats for a replica.
    fn get_stats(&self, node_id: NodeId) -> Option<ReplicaStats>;

    /// Record a successful read and its latency.
    fn record_success(&self, node_id: NodeId, latency_us: u64);

    /// Record a read failure.
    fn record_failure(&self, node_id: NodeId);

    /// Mark a replica as suspect (e.g., after corruption detection).
    fn mark_suspect(&self, node_id: NodeId);
}
