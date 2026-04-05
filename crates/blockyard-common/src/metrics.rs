//! Prometheus-compatible metrics definitions for Blockyard.
//!
//! All metric names are defined as constants so that dashboards and alerts can
//! reference them without risk of typos. The [`MetricsRecorder`] struct
//! provides convenience methods that record observations through the global
//! `metrics` facade.

use metrics::{counter, gauge, histogram};

// ---------------------------------------------------------------------------
// Metric name constants
// ---------------------------------------------------------------------------

// Cluster-level
pub const CLUSTER_NODES_TOTAL: &str = "blockyard_cluster_nodes_total";
pub const CLUSTER_NODES_ZFS_DEGRADED_TOTAL: &str = "blockyard_cluster_nodes_zfs_degraded_total";

// Volume I/O
pub const VOLUME_READ_IOPS: &str = "blockyard_volume_read_iops";
pub const VOLUME_WRITE_IOPS: &str = "blockyard_volume_write_iops";
pub const VOLUME_READ_BYTES_TOTAL: &str = "blockyard_volume_read_bytes_total";
pub const VOLUME_WRITE_BYTES_TOTAL: &str = "blockyard_volume_write_bytes_total";
pub const VOLUME_READ_LATENCY_SECONDS: &str = "blockyard_volume_read_latency_seconds";
pub const VOLUME_WRITE_LATENCY_SECONDS: &str = "blockyard_volume_write_latency_seconds";

// Volume replicas
pub const VOLUME_REPLICAS_ONLINE: &str = "blockyard_volume_replicas_online";
pub const VOLUME_REPLICAS_TOTAL: &str = "blockyard_volume_replicas_total";

// Node-level
pub const NODE_ZFS_CAPACITY_BYTES: &str = "blockyard_node_zfs_capacity_bytes";
pub const NODE_ZFS_USED_BYTES: &str = "blockyard_node_zfs_used_bytes";
pub const NODE_RAFT_GROUPS_TOTAL: &str = "blockyard_node_raft_groups_total";
pub const NODE_RAFT_LEADER_GROUPS: &str = "blockyard_node_raft_leader_groups";
pub const NODE_REBALANCE_BYTES_REMAINING: &str = "blockyard_node_rebalance_bytes_remaining";
pub const NODE_ZFS_STATE: &str = "blockyard_node_zfs_state";
pub const NODE_ZFS_CHECKSUM_ERRORS: &str = "blockyard_node_zfs_checksum_errors";

// ---------------------------------------------------------------------------
// MetricsRecorder
// ---------------------------------------------------------------------------

/// Convenience wrapper that records Blockyard-specific metrics through the
/// global `metrics` facade.
///
/// All methods are cheap to call and safe to invoke from async contexts.
#[derive(Debug, Clone, Default)]
pub struct MetricsRecorder;

impl MetricsRecorder {
    /// Create a new recorder instance. This is a zero-cost operation — the
    /// real work happens when the global `metrics` recorder is installed
    /// (e.g. via `metrics-exporter-prometheus`).
    pub fn new() -> Self {
        Self
    }

    // -- Volume I/O --------------------------------------------------------

    /// Record a completed write operation for `volume`.
    pub fn record_write(&self, volume: &str, bytes: u64, latency_secs: f64) {
        counter!(VOLUME_WRITE_IOPS, "volume" => volume.to_owned()).increment(1);
        counter!(VOLUME_WRITE_BYTES_TOTAL, "volume" => volume.to_owned()).increment(bytes);
        histogram!(VOLUME_WRITE_LATENCY_SECONDS, "volume" => volume.to_owned())
            .record(latency_secs);
    }

    /// Record a completed read operation for `volume`.
    pub fn record_read(&self, volume: &str, bytes: u64, latency_secs: f64) {
        counter!(VOLUME_READ_IOPS, "volume" => volume.to_owned()).increment(1);
        counter!(VOLUME_READ_BYTES_TOTAL, "volume" => volume.to_owned()).increment(bytes);
        histogram!(VOLUME_READ_LATENCY_SECONDS, "volume" => volume.to_owned())
            .record(latency_secs);
    }

    // -- Cluster -----------------------------------------------------------

    /// Set the total number of nodes in a given `state` (e.g. "healthy",
    /// "suspect", "failed").
    pub fn set_node_count(&self, state: &str, count: f64) {
        gauge!(CLUSTER_NODES_TOTAL, "state" => state.to_owned()).set(count);
    }

    /// Set the number of nodes whose ZFS pool is in a degraded state.
    pub fn set_zfs_degraded_total(&self, count: f64) {
        gauge!(CLUSTER_NODES_ZFS_DEGRADED_TOTAL).set(count);
    }

    // -- Volume replicas ---------------------------------------------------

    /// Set the number of online replicas for `volume`.
    pub fn set_volume_replicas_online(&self, volume: &str, count: f64) {
        gauge!(VOLUME_REPLICAS_ONLINE, "volume" => volume.to_owned()).set(count);
    }

    /// Set the total (desired) replica count for `volume`.
    pub fn set_volume_replicas_total(&self, volume: &str, count: f64) {
        gauge!(VOLUME_REPLICAS_TOTAL, "volume" => volume.to_owned()).set(count);
    }

    // -- Node ZFS health ---------------------------------------------------

    /// Set ZFS pool health metrics for a given `pool`.
    pub fn set_zfs_health(&self, pool: &str, state: &str) {
        gauge!(NODE_ZFS_STATE, "pool" => pool.to_owned(), "state" => state.to_owned()).set(1.0);
    }

    /// Set ZFS capacity in bytes for a given `pool`.
    pub fn set_zfs_capacity(&self, pool: &str, bytes: f64) {
        gauge!(NODE_ZFS_CAPACITY_BYTES, "pool" => pool.to_owned()).set(bytes);
    }

    /// Set ZFS used bytes for a given `pool`.
    pub fn set_zfs_used(&self, pool: &str, bytes: f64) {
        gauge!(NODE_ZFS_USED_BYTES, "pool" => pool.to_owned()).set(bytes);
    }

    /// Set ZFS checksum error count for a given `pool`.
    pub fn set_zfs_checksum_errors(&self, pool: &str, errors: f64) {
        gauge!(NODE_ZFS_CHECKSUM_ERRORS, "pool" => pool.to_owned()).set(errors);
    }

    // -- Raft --------------------------------------------------------------

    /// Set the total number of Raft groups hosted on this node.
    pub fn set_raft_groups_total(&self, count: f64) {
        gauge!(NODE_RAFT_GROUPS_TOTAL).set(count);
    }

    /// Set the number of Raft groups for which this node is the leader.
    pub fn set_raft_leader_groups(&self, count: f64) {
        gauge!(NODE_RAFT_LEADER_GROUPS).set(count);
    }

    // -- Rebalance ---------------------------------------------------------

    /// Set the remaining bytes to be rebalanced on this node.
    pub fn set_rebalance_bytes_remaining(&self, bytes: f64) {
        gauge!(NODE_REBALANCE_BYTES_REMAINING).set(bytes);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // The `metrics` crate macros work against a global recorder. In unit tests
    // we can exercise the API even without an installed recorder — the calls
    // are no-ops but must not panic.

    #[test]
    fn test_record_write() {
        let m = MetricsRecorder::new();
        m.record_write("test-vol", 4096, 0.001);
    }

    #[test]
    fn test_record_read() {
        let m = MetricsRecorder::new();
        m.record_read("test-vol", 8192, 0.0005);
    }

    #[test]
    fn test_set_node_count() {
        let m = MetricsRecorder::new();
        m.set_node_count("healthy", 3.0);
        m.set_node_count("suspect", 0.0);
    }

    #[test]
    fn test_set_zfs_degraded_total() {
        let m = MetricsRecorder::new();
        m.set_zfs_degraded_total(1.0);
    }

    #[test]
    fn test_set_volume_replicas() {
        let m = MetricsRecorder::new();
        m.set_volume_replicas_online("vol-1", 2.0);
        m.set_volume_replicas_total("vol-1", 3.0);
    }

    #[test]
    fn test_set_zfs_health() {
        let m = MetricsRecorder::new();
        m.set_zfs_health("blockyard", "online");
    }

    #[test]
    fn test_set_zfs_capacity_and_used() {
        let m = MetricsRecorder::new();
        m.set_zfs_capacity("blockyard", 1_000_000_000.0);
        m.set_zfs_used("blockyard", 500_000_000.0);
    }

    #[test]
    fn test_set_zfs_checksum_errors() {
        let m = MetricsRecorder::new();
        m.set_zfs_checksum_errors("blockyard", 0.0);
    }

    #[test]
    fn test_set_raft_groups() {
        let m = MetricsRecorder::new();
        m.set_raft_groups_total(5.0);
        m.set_raft_leader_groups(2.0);
    }

    #[test]
    fn test_set_rebalance_bytes_remaining() {
        let m = MetricsRecorder::new();
        m.set_rebalance_bytes_remaining(1_073_741_824.0);
    }

    #[test]
    fn test_metrics_recorder_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MetricsRecorder>();
    }

    #[test]
    fn test_metric_name_constants() {
        // Smoke-check that all constants are non-empty and start with
        // "blockyard_".
        let names: &[&str] = &[
            CLUSTER_NODES_TOTAL,
            CLUSTER_NODES_ZFS_DEGRADED_TOTAL,
            VOLUME_READ_IOPS,
            VOLUME_WRITE_IOPS,
            VOLUME_READ_BYTES_TOTAL,
            VOLUME_WRITE_BYTES_TOTAL,
            VOLUME_READ_LATENCY_SECONDS,
            VOLUME_WRITE_LATENCY_SECONDS,
            VOLUME_REPLICAS_ONLINE,
            VOLUME_REPLICAS_TOTAL,
            NODE_ZFS_CAPACITY_BYTES,
            NODE_ZFS_USED_BYTES,
            NODE_RAFT_GROUPS_TOTAL,
            NODE_RAFT_LEADER_GROUPS,
            NODE_REBALANCE_BYTES_REMAINING,
            NODE_ZFS_STATE,
            NODE_ZFS_CHECKSUM_ERRORS,
        ];
        for name in names {
            assert!(!name.is_empty(), "metric name must not be empty");
            assert!(
                name.starts_with("blockyard_"),
                "metric '{name}' should start with blockyard_"
            );
        }
    }
}
