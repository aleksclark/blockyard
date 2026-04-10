//! Observability and metrics for Blockyard (Phase 8, §9).
//!
//! Provides a [`MetricsRecorder`] trait for recording counters, gauges, and
//! histograms, plus an [`InMemoryRecorder`] implementation for testing.
//! All metric names are defined as constants with stable string identifiers
//! to ensure consistency across the codebase.

use std::collections::HashMap;

use parking_lot::Mutex;

// ---------------------------------------------------------------------------
// Metric name constants
// ---------------------------------------------------------------------------

// P8.1: Per-volume IO success/failure rate counters
/// Counter: total successful IO operations for a volume.
/// Labels: `volume_id`.
pub const VOLUME_IO_SUCCESS_TOTAL: &str = "blockyard_volume_io_success_total";
/// Counter: total failed IO operations for a volume.
/// Labels: `volume_id`.
pub const VOLUME_IO_FAILURE_TOTAL: &str = "blockyard_volume_io_failure_total";

// P8.2: Client watermark version + stale-epoch retry counters
/// Gauge: current client session watermark version.
/// Labels: `session_id`.
pub const CLIENT_WATERMARK_VERSION: &str = "blockyard_client_watermark_version";
/// Counter: total stale-epoch retries by a client session.
/// Labels: `session_id`.
pub const CLIENT_STALE_EPOCH_RETRIES_TOTAL: &str = "blockyard_client_stale_epoch_retries_total";

// P8.3: Per-node foreground and background IO load gauges
/// Gauge: current number of in-flight foreground IO operations on a node.
/// Labels: `node_id`.
pub const NODE_FOREGROUND_IO_LOAD: &str = "blockyard_node_foreground_io_load";
/// Gauge: current number of in-flight background IO operations on a node.
/// Labels: `node_id`.
pub const NODE_BACKGROUND_IO_LOAD: &str = "blockyard_node_background_io_load";

// P8.4: Per-disk health state transition counters (with stable disk IDs)
/// Counter: total disk health state transitions.
/// Labels: `disk_id`, `from_state`, `to_state`.
pub const DISK_STATE_TRANSITION_TOTAL: &str = "blockyard_disk_state_transition_total";

// P8.5: Scrub findings counter + last scrub timestamp gauge
/// Counter: total scrub findings (corruption, errors).
/// Labels: `node_id`, `disk_id`.
pub const SCRUB_FINDINGS_TOTAL: &str = "blockyard_scrub_findings_total";
/// Gauge: unix timestamp of last completed scrub.
/// Labels: `node_id`, `disk_id`.
pub const SCRUB_LAST_COMPLETED_TIMESTAMP: &str = "blockyard_scrub_last_completed_timestamp";

// P8.6: Repair backlog size gauge + repair completion rate counter
/// Gauge: number of extents awaiting repair.
/// Labels: `node_id`.
pub const REPAIR_BACKLOG_SIZE: &str = "blockyard_repair_backlog_size";
/// Counter: total repair completions.
/// Labels: `node_id`.
pub const REPAIR_COMPLETIONS_TOTAL: &str = "blockyard_repair_completions_total";

// P8.7: Orphaned extent file count gauge
/// Gauge: number of orphaned extent files on a node.
/// Labels: `node_id`.
pub const ORPHANED_EXTENT_FILES: &str = "blockyard_orphaned_extent_files";

// P8.8: Metadata quorum health gauge + commit latency histogram
/// Gauge: metadata quorum health (1 = healthy, 0 = unhealthy).
/// Labels: `raft_group_id`.
pub const METADATA_QUORUM_HEALTH: &str = "blockyard_metadata_quorum_health";
/// Histogram: metadata commit latency in seconds.
/// Labels: `raft_group_id`.
pub const METADATA_COMMIT_LATENCY_SECONDS: &str = "blockyard_metadata_commit_latency_seconds";

/// All metric name constants, useful for validation and enumeration.
pub const ALL_METRIC_NAMES: &[&str] = &[
    VOLUME_IO_SUCCESS_TOTAL,
    VOLUME_IO_FAILURE_TOTAL,
    CLIENT_WATERMARK_VERSION,
    CLIENT_STALE_EPOCH_RETRIES_TOTAL,
    NODE_FOREGROUND_IO_LOAD,
    NODE_BACKGROUND_IO_LOAD,
    DISK_STATE_TRANSITION_TOTAL,
    SCRUB_FINDINGS_TOTAL,
    SCRUB_LAST_COMPLETED_TIMESTAMP,
    REPAIR_BACKLOG_SIZE,
    REPAIR_COMPLETIONS_TOTAL,
    ORPHANED_EXTENT_FILES,
    METADATA_QUORUM_HEALTH,
    METADATA_COMMIT_LATENCY_SECONDS,
];

// ---------------------------------------------------------------------------
// Label type
// ---------------------------------------------------------------------------

/// A set of key-value label pairs attached to a metric observation.
///
/// Labels provide dimensional context (e.g., `volume_id`, `disk_id`).
/// The pairs are sorted by key before being used as map keys so that
/// `[("a","1"),("b","2")]` and `[("b","2"),("a","1")]` resolve to the
/// same time series.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Labels(Vec<(String, String)>);

impl Labels {
    /// Create an empty label set.
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Create a label set from key-value pairs. Keys are sorted for
    /// deterministic identity.
    pub fn from_pairs(pairs: &[(&str, &str)]) -> Self {
        let mut v: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        Self(v)
    }

    /// Return the inner pairs slice.
    pub fn pairs(&self) -> &[(String, String)] {
        &self.0
    }

    /// Return the number of label pairs.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Return whether the label set is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Default for Labels {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// MetricsRecorder trait
// ---------------------------------------------------------------------------

/// Trait for recording observability metrics.
///
/// Implementations may emit to Prometheus, StatsD, an in-memory store for
/// tests, or any other backend. All methods take `&self` and are expected
/// to be internally synchronized (e.g., via `Mutex` or atomics).
pub trait MetricsRecorder: Send + Sync + std::fmt::Debug {
    /// Increment a counter by `value`.
    ///
    /// Counters are monotonically increasing (within a process lifetime).
    fn increment_counter(&self, name: &str, labels: &Labels, value: u64);

    /// Set a gauge to `value`.
    ///
    /// Gauges represent point-in-time values that can go up or down.
    fn set_gauge(&self, name: &str, labels: &Labels, value: f64);

    /// Record a histogram observation.
    ///
    /// Histograms track the distribution of values (e.g., latencies).
    fn record_histogram(&self, name: &str, labels: &Labels, value: f64);
}

// ---------------------------------------------------------------------------
// NoopRecorder
// ---------------------------------------------------------------------------

/// A no-op recorder that discards all metrics. Useful as a default when
/// no observability backend is configured.
#[derive(Debug, Clone, Copy)]
pub struct NoopRecorder;

impl MetricsRecorder for NoopRecorder {
    fn increment_counter(&self, _name: &str, _labels: &Labels, _value: u64) {}
    fn set_gauge(&self, _name: &str, _labels: &Labels, _value: f64) {}
    fn record_histogram(&self, _name: &str, _labels: &Labels, _value: f64) {}
}

// ---------------------------------------------------------------------------
// InMemoryRecorder
// ---------------------------------------------------------------------------

/// Composite key for looking up metric values: `(name, labels)`.
type MetricKey = (String, Labels);

/// In-memory metrics recorder for unit and integration testing.
///
/// Stores counters, gauges, and histogram observations so tests can assert
/// on the exact values emitted by instrumented code paths.
#[derive(Debug)]
pub struct InMemoryRecorder {
    counters: Mutex<HashMap<MetricKey, u64>>,
    gauges: Mutex<HashMap<MetricKey, f64>>,
    histograms: Mutex<HashMap<MetricKey, Vec<f64>>>,
}

impl InMemoryRecorder {
    /// Create a new empty recorder.
    pub fn new() -> Self {
        Self {
            counters: Mutex::new(HashMap::new()),
            gauges: Mutex::new(HashMap::new()),
            histograms: Mutex::new(HashMap::new()),
        }
    }

    /// Read the current value of a counter. Returns 0 if never incremented.
    pub fn counter(&self, name: &str, labels: &Labels) -> u64 {
        let key = (name.to_owned(), labels.clone());
        self.counters.lock().get(&key).copied().unwrap_or(0)
    }

    /// Read the current value of a gauge. Returns `None` if never set.
    pub fn gauge(&self, name: &str, labels: &Labels) -> Option<f64> {
        let key = (name.to_owned(), labels.clone());
        self.gauges.lock().get(&key).copied()
    }

    /// Read all histogram observations for a metric. Returns an empty vec
    /// if no observations have been recorded.
    pub fn histogram(&self, name: &str, labels: &Labels) -> Vec<f64> {
        let key = (name.to_owned(), labels.clone());
        self.histograms
            .lock()
            .get(&key)
            .cloned()
            .unwrap_or_default()
    }

    /// Return the number of distinct counter series recorded.
    pub fn counter_count(&self) -> usize {
        self.counters.lock().len()
    }

    /// Return the number of distinct gauge series recorded.
    pub fn gauge_count(&self) -> usize {
        self.gauges.lock().len()
    }

    /// Return the number of distinct histogram series recorded.
    pub fn histogram_count(&self) -> usize {
        self.histograms.lock().len()
    }

    /// Reset all recorded data.
    pub fn reset(&self) {
        self.counters.lock().clear();
        self.gauges.lock().clear();
        self.histograms.lock().clear();
    }
}

impl Default for InMemoryRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsRecorder for InMemoryRecorder {
    fn increment_counter(&self, name: &str, labels: &Labels, value: u64) {
        let key = (name.to_owned(), labels.clone());
        let mut counters = self.counters.lock();
        let entry = counters.entry(key).or_insert(0);
        *entry = entry.saturating_add(value);
    }

    fn set_gauge(&self, name: &str, labels: &Labels, value: f64) {
        let key = (name.to_owned(), labels.clone());
        self.gauges.lock().insert(key, value);
    }

    fn record_histogram(&self, name: &str, labels: &Labels, value: f64) {
        let key = (name.to_owned(), labels.clone());
        self.histograms.lock().entry(key).or_default().push(value);
    }
}

// ---------------------------------------------------------------------------
// Helper functions for common recording patterns
// ---------------------------------------------------------------------------

/// Record a volume IO success.
pub fn record_volume_io_success(recorder: &dyn MetricsRecorder, volume_id: &str) {
    let labels = Labels::from_pairs(&[("volume_id", volume_id)]);
    recorder.increment_counter(VOLUME_IO_SUCCESS_TOTAL, &labels, 1);
}

/// Record a volume IO failure.
pub fn record_volume_io_failure(recorder: &dyn MetricsRecorder, volume_id: &str) {
    let labels = Labels::from_pairs(&[("volume_id", volume_id)]);
    recorder.increment_counter(VOLUME_IO_FAILURE_TOTAL, &labels, 1);
}

/// Update the client watermark version gauge.
pub fn set_client_watermark(recorder: &dyn MetricsRecorder, session_id: &str, version: u64) {
    let labels = Labels::from_pairs(&[("session_id", session_id)]);
    recorder.set_gauge(CLIENT_WATERMARK_VERSION, &labels, version as f64);
}

/// Record a stale-epoch retry.
pub fn record_stale_epoch_retry(recorder: &dyn MetricsRecorder, session_id: &str) {
    let labels = Labels::from_pairs(&[("session_id", session_id)]);
    recorder.increment_counter(CLIENT_STALE_EPOCH_RETRIES_TOTAL, &labels, 1);
}

/// Set the foreground IO load gauge for a node.
pub fn set_foreground_io_load(recorder: &dyn MetricsRecorder, node_id: &str, load: u64) {
    let labels = Labels::from_pairs(&[("node_id", node_id)]);
    recorder.set_gauge(NODE_FOREGROUND_IO_LOAD, &labels, load as f64);
}

/// Set the background IO load gauge for a node.
pub fn set_background_io_load(recorder: &dyn MetricsRecorder, node_id: &str, load: u64) {
    let labels = Labels::from_pairs(&[("node_id", node_id)]);
    recorder.set_gauge(NODE_BACKGROUND_IO_LOAD, &labels, load as f64);
}

/// Record a disk health state transition.
pub fn record_disk_state_transition(
    recorder: &dyn MetricsRecorder,
    disk_id: &str,
    from_state: &str,
    to_state: &str,
) {
    let labels = Labels::from_pairs(&[
        ("disk_id", disk_id),
        ("from_state", from_state),
        ("to_state", to_state),
    ]);
    recorder.increment_counter(DISK_STATE_TRANSITION_TOTAL, &labels, 1);
}

/// Record a scrub finding.
pub fn record_scrub_finding(recorder: &dyn MetricsRecorder, node_id: &str, disk_id: &str) {
    let labels = Labels::from_pairs(&[("node_id", node_id), ("disk_id", disk_id)]);
    recorder.increment_counter(SCRUB_FINDINGS_TOTAL, &labels, 1);
}

/// Set the last-completed scrub timestamp gauge.
pub fn set_scrub_last_completed(
    recorder: &dyn MetricsRecorder,
    node_id: &str,
    disk_id: &str,
    timestamp: f64,
) {
    let labels = Labels::from_pairs(&[("node_id", node_id), ("disk_id", disk_id)]);
    recorder.set_gauge(SCRUB_LAST_COMPLETED_TIMESTAMP, &labels, timestamp);
}

/// Set the repair backlog size gauge for a node.
pub fn set_repair_backlog_size(recorder: &dyn MetricsRecorder, node_id: &str, size: u64) {
    let labels = Labels::from_pairs(&[("node_id", node_id)]);
    recorder.set_gauge(REPAIR_BACKLOG_SIZE, &labels, size as f64);
}

/// Record a repair completion.
pub fn record_repair_completion(recorder: &dyn MetricsRecorder, node_id: &str) {
    let labels = Labels::from_pairs(&[("node_id", node_id)]);
    recorder.increment_counter(REPAIR_COMPLETIONS_TOTAL, &labels, 1);
}

/// Set the orphaned extent file count gauge.
pub fn set_orphaned_extent_files(recorder: &dyn MetricsRecorder, node_id: &str, count: u64) {
    let labels = Labels::from_pairs(&[("node_id", node_id)]);
    recorder.set_gauge(ORPHANED_EXTENT_FILES, &labels, count as f64);
}

/// Set the metadata quorum health gauge.
pub fn set_metadata_quorum_health(
    recorder: &dyn MetricsRecorder,
    raft_group_id: &str,
    healthy: bool,
) {
    let labels = Labels::from_pairs(&[("raft_group_id", raft_group_id)]);
    recorder.set_gauge(
        METADATA_QUORUM_HEALTH,
        &labels,
        if healthy { 1.0 } else { 0.0 },
    );
}

/// Record a metadata commit latency observation.
pub fn record_metadata_commit_latency(
    recorder: &dyn MetricsRecorder,
    raft_group_id: &str,
    latency_seconds: f64,
) {
    let labels = Labels::from_pairs(&[("raft_group_id", raft_group_id)]);
    recorder.record_histogram(METADATA_COMMIT_LATENCY_SECONDS, &labels, latency_seconds);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Labels tests -------------------------------------------------------

    #[test]
    fn test_labels_new_is_empty() {
        let labels = Labels::new();
        assert!(labels.is_empty());
        assert_eq!(labels.len(), 0);
        assert!(labels.pairs().is_empty());
    }

    #[test]
    fn test_labels_default_is_empty() {
        let labels = Labels::default();
        assert!(labels.is_empty());
    }

    #[test]
    fn test_labels_from_pairs() {
        let labels = Labels::from_pairs(&[("volume_id", "v1"), ("node_id", "n1")]);
        assert_eq!(labels.len(), 2);
        assert!(!labels.is_empty());
    }

    #[test]
    fn test_labels_sorted_by_key() {
        let labels = Labels::from_pairs(&[("z_key", "z"), ("a_key", "a"), ("m_key", "m")]);
        let pairs = labels.pairs();
        assert_eq!(pairs[0].0, "a_key");
        assert_eq!(pairs[1].0, "m_key");
        assert_eq!(pairs[2].0, "z_key");
    }

    #[test]
    fn test_labels_order_independent_equality() {
        let a = Labels::from_pairs(&[("x", "1"), ("y", "2")]);
        let b = Labels::from_pairs(&[("y", "2"), ("x", "1")]);
        assert_eq!(a, b);
    }

    #[test]
    fn test_labels_different_values_not_equal() {
        let a = Labels::from_pairs(&[("k", "v1")]);
        let b = Labels::from_pairs(&[("k", "v2")]);
        assert_ne!(a, b);
    }

    #[test]
    fn test_labels_clone() {
        let a = Labels::from_pairs(&[("k", "v")]);
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_labels_debug() {
        let labels = Labels::from_pairs(&[("k", "v")]);
        let debug = format!("{:?}", labels);
        assert!(debug.contains("Labels"));
    }

    #[test]
    fn test_labels_hash_consistency() {
        use std::collections::HashSet;
        let a = Labels::from_pairs(&[("x", "1"), ("y", "2")]);
        let b = Labels::from_pairs(&[("y", "2"), ("x", "1")]);
        let mut set = HashSet::new();
        set.insert(a);
        set.insert(b);
        assert_eq!(set.len(), 1);
    }

    // -- Metric name constants tests ----------------------------------------

    #[test]
    fn test_all_metric_names_count() {
        assert_eq!(ALL_METRIC_NAMES.len(), 14);
    }

    #[test]
    fn test_all_metric_names_unique() {
        use std::collections::HashSet;
        let set: HashSet<&str> = ALL_METRIC_NAMES.iter().copied().collect();
        assert_eq!(set.len(), ALL_METRIC_NAMES.len());
    }

    #[test]
    fn test_all_metric_names_prefixed() {
        for name in ALL_METRIC_NAMES {
            assert!(
                name.starts_with("blockyard_"),
                "metric name must start with blockyard_ prefix: {name}",
            );
        }
    }

    #[test]
    fn test_metric_name_p8_1_success() {
        assert_eq!(VOLUME_IO_SUCCESS_TOTAL, "blockyard_volume_io_success_total");
    }

    #[test]
    fn test_metric_name_p8_1_failure() {
        assert_eq!(VOLUME_IO_FAILURE_TOTAL, "blockyard_volume_io_failure_total");
    }

    #[test]
    fn test_metric_name_p8_2_watermark() {
        assert_eq!(
            CLIENT_WATERMARK_VERSION,
            "blockyard_client_watermark_version"
        );
    }

    #[test]
    fn test_metric_name_p8_2_stale_epoch() {
        assert_eq!(
            CLIENT_STALE_EPOCH_RETRIES_TOTAL,
            "blockyard_client_stale_epoch_retries_total"
        );
    }

    #[test]
    fn test_metric_name_p8_3_foreground() {
        assert_eq!(NODE_FOREGROUND_IO_LOAD, "blockyard_node_foreground_io_load");
    }

    #[test]
    fn test_metric_name_p8_3_background() {
        assert_eq!(NODE_BACKGROUND_IO_LOAD, "blockyard_node_background_io_load");
    }

    #[test]
    fn test_metric_name_p8_4_disk_transition() {
        assert_eq!(
            DISK_STATE_TRANSITION_TOTAL,
            "blockyard_disk_state_transition_total"
        );
    }

    #[test]
    fn test_metric_name_p8_5_scrub_findings() {
        assert_eq!(SCRUB_FINDINGS_TOTAL, "blockyard_scrub_findings_total");
    }

    #[test]
    fn test_metric_name_p8_5_scrub_timestamp() {
        assert_eq!(
            SCRUB_LAST_COMPLETED_TIMESTAMP,
            "blockyard_scrub_last_completed_timestamp"
        );
    }

    #[test]
    fn test_metric_name_p8_6_repair_backlog() {
        assert_eq!(REPAIR_BACKLOG_SIZE, "blockyard_repair_backlog_size");
    }

    #[test]
    fn test_metric_name_p8_6_repair_completions() {
        assert_eq!(
            REPAIR_COMPLETIONS_TOTAL,
            "blockyard_repair_completions_total"
        );
    }

    #[test]
    fn test_metric_name_p8_7_orphaned() {
        assert_eq!(ORPHANED_EXTENT_FILES, "blockyard_orphaned_extent_files");
    }

    #[test]
    fn test_metric_name_p8_8_quorum_health() {
        assert_eq!(METADATA_QUORUM_HEALTH, "blockyard_metadata_quorum_health");
    }

    #[test]
    fn test_metric_name_p8_8_commit_latency() {
        assert_eq!(
            METADATA_COMMIT_LATENCY_SECONDS,
            "blockyard_metadata_commit_latency_seconds"
        );
    }

    // -- NoopRecorder tests -------------------------------------------------

    #[test]
    fn test_noop_recorder_increment_counter() {
        let r = NoopRecorder;
        let labels = Labels::new();
        // Should not panic.
        r.increment_counter("test", &labels, 1);
    }

    #[test]
    fn test_noop_recorder_set_gauge() {
        let r = NoopRecorder;
        let labels = Labels::new();
        r.set_gauge("test", &labels, 42.0);
    }

    #[test]
    fn test_noop_recorder_record_histogram() {
        let r = NoopRecorder;
        let labels = Labels::new();
        r.record_histogram("test", &labels, 0.5);
    }

    #[test]
    fn test_noop_recorder_debug() {
        let r = NoopRecorder;
        let debug = format!("{:?}", r);
        assert!(debug.contains("NoopRecorder"));
    }

    #[test]
    fn test_noop_recorder_clone() {
        let r = NoopRecorder;
        let r2 = r;
        let _ = format!("{:?}", r2);
    }

    #[test]
    fn test_noop_recorder_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<NoopRecorder>();
    }

    // -- InMemoryRecorder tests ---------------------------------------------

    #[test]
    fn test_in_memory_recorder_new() {
        let r = InMemoryRecorder::new();
        assert_eq!(r.counter_count(), 0);
        assert_eq!(r.gauge_count(), 0);
        assert_eq!(r.histogram_count(), 0);
    }

    #[test]
    fn test_in_memory_recorder_default() {
        let r = InMemoryRecorder::default();
        assert_eq!(r.counter_count(), 0);
    }

    #[test]
    fn test_in_memory_recorder_debug() {
        let r = InMemoryRecorder::new();
        let debug = format!("{:?}", r);
        assert!(debug.contains("InMemoryRecorder"));
    }

    #[test]
    fn test_in_memory_recorder_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<InMemoryRecorder>();
    }

    #[test]
    fn test_in_memory_counter_initial_zero() {
        let r = InMemoryRecorder::new();
        let labels = Labels::new();
        assert_eq!(r.counter("nonexistent", &labels), 0);
    }

    #[test]
    fn test_in_memory_counter_increment() {
        let r = InMemoryRecorder::new();
        let labels = Labels::new();
        r.increment_counter("test_counter", &labels, 5);
        assert_eq!(r.counter("test_counter", &labels), 5);
    }

    #[test]
    fn test_in_memory_counter_accumulates() {
        let r = InMemoryRecorder::new();
        let labels = Labels::new();
        r.increment_counter("c", &labels, 3);
        r.increment_counter("c", &labels, 7);
        assert_eq!(r.counter("c", &labels), 10);
    }

    #[test]
    fn test_in_memory_counter_saturating_add() {
        let r = InMemoryRecorder::new();
        let labels = Labels::new();
        r.increment_counter("c", &labels, u64::MAX);
        r.increment_counter("c", &labels, 1);
        assert_eq!(r.counter("c", &labels), u64::MAX);
    }

    #[test]
    fn test_in_memory_counter_distinct_labels() {
        let r = InMemoryRecorder::new();
        let l1 = Labels::from_pairs(&[("id", "a")]);
        let l2 = Labels::from_pairs(&[("id", "b")]);
        r.increment_counter("c", &l1, 1);
        r.increment_counter("c", &l2, 2);
        assert_eq!(r.counter("c", &l1), 1);
        assert_eq!(r.counter("c", &l2), 2);
        assert_eq!(r.counter_count(), 2);
    }

    #[test]
    fn test_in_memory_gauge_initial_none() {
        let r = InMemoryRecorder::new();
        let labels = Labels::new();
        assert_eq!(r.gauge("nonexistent", &labels), None);
    }

    #[test]
    fn test_in_memory_gauge_set() {
        let r = InMemoryRecorder::new();
        let labels = Labels::new();
        r.set_gauge("g", &labels, 42.0);
        assert_eq!(r.gauge("g", &labels), Some(42.0));
    }

    #[test]
    fn test_in_memory_gauge_overwrite() {
        let r = InMemoryRecorder::new();
        let labels = Labels::new();
        r.set_gauge("g", &labels, 1.0);
        r.set_gauge("g", &labels, 2.0);
        assert_eq!(r.gauge("g", &labels), Some(2.0));
        assert_eq!(r.gauge_count(), 1);
    }

    #[test]
    fn test_in_memory_gauge_distinct_labels() {
        let r = InMemoryRecorder::new();
        let l1 = Labels::from_pairs(&[("id", "a")]);
        let l2 = Labels::from_pairs(&[("id", "b")]);
        r.set_gauge("g", &l1, 10.0);
        r.set_gauge("g", &l2, 20.0);
        assert_eq!(r.gauge("g", &l1), Some(10.0));
        assert_eq!(r.gauge("g", &l2), Some(20.0));
        assert_eq!(r.gauge_count(), 2);
    }

    #[test]
    fn test_in_memory_histogram_initial_empty() {
        let r = InMemoryRecorder::new();
        let labels = Labels::new();
        assert!(r.histogram("nonexistent", &labels).is_empty());
    }

    #[test]
    fn test_in_memory_histogram_record() {
        let r = InMemoryRecorder::new();
        let labels = Labels::new();
        r.record_histogram("h", &labels, 0.5);
        assert_eq!(r.histogram("h", &labels), vec![0.5]);
    }

    #[test]
    fn test_in_memory_histogram_multiple_observations() {
        let r = InMemoryRecorder::new();
        let labels = Labels::new();
        r.record_histogram("h", &labels, 0.1);
        r.record_histogram("h", &labels, 0.2);
        r.record_histogram("h", &labels, 0.3);
        let obs = r.histogram("h", &labels);
        assert_eq!(obs, vec![0.1, 0.2, 0.3]);
        assert_eq!(r.histogram_count(), 1);
    }

    #[test]
    fn test_in_memory_histogram_distinct_labels() {
        let r = InMemoryRecorder::new();
        let l1 = Labels::from_pairs(&[("id", "a")]);
        let l2 = Labels::from_pairs(&[("id", "b")]);
        r.record_histogram("h", &l1, 1.0);
        r.record_histogram("h", &l2, 2.0);
        assert_eq!(r.histogram("h", &l1), vec![1.0]);
        assert_eq!(r.histogram("h", &l2), vec![2.0]);
        assert_eq!(r.histogram_count(), 2);
    }

    #[test]
    fn test_in_memory_reset() {
        let r = InMemoryRecorder::new();
        let labels = Labels::new();
        r.increment_counter("c", &labels, 1);
        r.set_gauge("g", &labels, 1.0);
        r.record_histogram("h", &labels, 1.0);
        assert_eq!(r.counter_count(), 1);
        assert_eq!(r.gauge_count(), 1);
        assert_eq!(r.histogram_count(), 1);

        r.reset();
        assert_eq!(r.counter_count(), 0);
        assert_eq!(r.gauge_count(), 0);
        assert_eq!(r.histogram_count(), 0);
        assert_eq!(r.counter("c", &labels), 0);
        assert_eq!(r.gauge("g", &labels), None);
        assert!(r.histogram("h", &labels).is_empty());
    }

    // -- Helper function tests (P8.1) ---------------------------------------

    #[test]
    fn test_record_volume_io_success() {
        let r = InMemoryRecorder::new();
        record_volume_io_success(&r, "vol-1");
        record_volume_io_success(&r, "vol-1");
        record_volume_io_success(&r, "vol-2");
        let l1 = Labels::from_pairs(&[("volume_id", "vol-1")]);
        let l2 = Labels::from_pairs(&[("volume_id", "vol-2")]);
        assert_eq!(r.counter(VOLUME_IO_SUCCESS_TOTAL, &l1), 2);
        assert_eq!(r.counter(VOLUME_IO_SUCCESS_TOTAL, &l2), 1);
    }

    #[test]
    fn test_record_volume_io_failure() {
        let r = InMemoryRecorder::new();
        record_volume_io_failure(&r, "vol-1");
        let labels = Labels::from_pairs(&[("volume_id", "vol-1")]);
        assert_eq!(r.counter(VOLUME_IO_FAILURE_TOTAL, &labels), 1);
    }

    // -- Helper function tests (P8.2) ---------------------------------------

    #[test]
    fn test_set_client_watermark() {
        let r = InMemoryRecorder::new();
        set_client_watermark(&r, "sess-1", 42);
        let labels = Labels::from_pairs(&[("session_id", "sess-1")]);
        assert_eq!(r.gauge(CLIENT_WATERMARK_VERSION, &labels), Some(42.0));
    }

    #[test]
    fn test_set_client_watermark_update() {
        let r = InMemoryRecorder::new();
        set_client_watermark(&r, "sess-1", 10);
        set_client_watermark(&r, "sess-1", 20);
        let labels = Labels::from_pairs(&[("session_id", "sess-1")]);
        assert_eq!(r.gauge(CLIENT_WATERMARK_VERSION, &labels), Some(20.0));
    }

    #[test]
    fn test_record_stale_epoch_retry() {
        let r = InMemoryRecorder::new();
        record_stale_epoch_retry(&r, "sess-1");
        record_stale_epoch_retry(&r, "sess-1");
        record_stale_epoch_retry(&r, "sess-2");
        let l1 = Labels::from_pairs(&[("session_id", "sess-1")]);
        let l2 = Labels::from_pairs(&[("session_id", "sess-2")]);
        assert_eq!(r.counter(CLIENT_STALE_EPOCH_RETRIES_TOTAL, &l1), 2);
        assert_eq!(r.counter(CLIENT_STALE_EPOCH_RETRIES_TOTAL, &l2), 1);
    }

    // -- Helper function tests (P8.3) ---------------------------------------

    #[test]
    fn test_set_foreground_io_load() {
        let r = InMemoryRecorder::new();
        set_foreground_io_load(&r, "node-1", 5);
        let labels = Labels::from_pairs(&[("node_id", "node-1")]);
        assert_eq!(r.gauge(NODE_FOREGROUND_IO_LOAD, &labels), Some(5.0));
    }

    #[test]
    fn test_set_foreground_io_load_update() {
        let r = InMemoryRecorder::new();
        set_foreground_io_load(&r, "node-1", 5);
        set_foreground_io_load(&r, "node-1", 3);
        let labels = Labels::from_pairs(&[("node_id", "node-1")]);
        assert_eq!(r.gauge(NODE_FOREGROUND_IO_LOAD, &labels), Some(3.0));
    }

    #[test]
    fn test_set_background_io_load() {
        let r = InMemoryRecorder::new();
        set_background_io_load(&r, "node-1", 2);
        let labels = Labels::from_pairs(&[("node_id", "node-1")]);
        assert_eq!(r.gauge(NODE_BACKGROUND_IO_LOAD, &labels), Some(2.0));
    }

    #[test]
    fn test_set_background_io_load_update() {
        let r = InMemoryRecorder::new();
        set_background_io_load(&r, "node-1", 10);
        set_background_io_load(&r, "node-1", 0);
        let labels = Labels::from_pairs(&[("node_id", "node-1")]);
        assert_eq!(r.gauge(NODE_BACKGROUND_IO_LOAD, &labels), Some(0.0));
    }

    // -- Helper function tests (P8.4) ---------------------------------------

    #[test]
    fn test_record_disk_state_transition() {
        let r = InMemoryRecorder::new();
        record_disk_state_transition(&r, "disk-abc", "healthy", "suspect");
        let labels = Labels::from_pairs(&[
            ("disk_id", "disk-abc"),
            ("from_state", "healthy"),
            ("to_state", "suspect"),
        ]);
        assert_eq!(r.counter(DISK_STATE_TRANSITION_TOTAL, &labels), 1);
    }

    #[test]
    fn test_record_disk_state_transition_accumulates() {
        let r = InMemoryRecorder::new();
        record_disk_state_transition(&r, "disk-1", "healthy", "suspect");
        record_disk_state_transition(&r, "disk-1", "healthy", "suspect");
        let labels = Labels::from_pairs(&[
            ("disk_id", "disk-1"),
            ("from_state", "healthy"),
            ("to_state", "suspect"),
        ]);
        assert_eq!(r.counter(DISK_STATE_TRANSITION_TOTAL, &labels), 2);
    }

    #[test]
    fn test_record_disk_state_transition_different_transitions() {
        let r = InMemoryRecorder::new();
        record_disk_state_transition(&r, "disk-1", "healthy", "suspect");
        record_disk_state_transition(&r, "disk-1", "suspect", "degraded");
        let l1 = Labels::from_pairs(&[
            ("disk_id", "disk-1"),
            ("from_state", "healthy"),
            ("to_state", "suspect"),
        ]);
        let l2 = Labels::from_pairs(&[
            ("disk_id", "disk-1"),
            ("from_state", "suspect"),
            ("to_state", "degraded"),
        ]);
        assert_eq!(r.counter(DISK_STATE_TRANSITION_TOTAL, &l1), 1);
        assert_eq!(r.counter(DISK_STATE_TRANSITION_TOTAL, &l2), 1);
    }

    // -- Helper function tests (P8.5) ---------------------------------------

    #[test]
    fn test_record_scrub_finding() {
        let r = InMemoryRecorder::new();
        record_scrub_finding(&r, "node-1", "disk-a");
        record_scrub_finding(&r, "node-1", "disk-a");
        let labels = Labels::from_pairs(&[("node_id", "node-1"), ("disk_id", "disk-a")]);
        assert_eq!(r.counter(SCRUB_FINDINGS_TOTAL, &labels), 2);
    }

    #[test]
    fn test_record_scrub_finding_different_disks() {
        let r = InMemoryRecorder::new();
        record_scrub_finding(&r, "node-1", "disk-a");
        record_scrub_finding(&r, "node-1", "disk-b");
        let la = Labels::from_pairs(&[("node_id", "node-1"), ("disk_id", "disk-a")]);
        let lb = Labels::from_pairs(&[("node_id", "node-1"), ("disk_id", "disk-b")]);
        assert_eq!(r.counter(SCRUB_FINDINGS_TOTAL, &la), 1);
        assert_eq!(r.counter(SCRUB_FINDINGS_TOTAL, &lb), 1);
    }

    #[test]
    fn test_set_scrub_last_completed() {
        let r = InMemoryRecorder::new();
        set_scrub_last_completed(&r, "node-1", "disk-a", 1700000000.0);
        let labels = Labels::from_pairs(&[("node_id", "node-1"), ("disk_id", "disk-a")]);
        assert_eq!(
            r.gauge(SCRUB_LAST_COMPLETED_TIMESTAMP, &labels),
            Some(1700000000.0)
        );
    }

    #[test]
    fn test_set_scrub_last_completed_update() {
        let r = InMemoryRecorder::new();
        set_scrub_last_completed(&r, "node-1", "disk-a", 100.0);
        set_scrub_last_completed(&r, "node-1", "disk-a", 200.0);
        let labels = Labels::from_pairs(&[("node_id", "node-1"), ("disk_id", "disk-a")]);
        assert_eq!(
            r.gauge(SCRUB_LAST_COMPLETED_TIMESTAMP, &labels),
            Some(200.0)
        );
    }

    // -- Helper function tests (P8.6) ---------------------------------------

    #[test]
    fn test_set_repair_backlog_size() {
        let r = InMemoryRecorder::new();
        set_repair_backlog_size(&r, "node-1", 42);
        let labels = Labels::from_pairs(&[("node_id", "node-1")]);
        assert_eq!(r.gauge(REPAIR_BACKLOG_SIZE, &labels), Some(42.0));
    }

    #[test]
    fn test_set_repair_backlog_size_decrease() {
        let r = InMemoryRecorder::new();
        set_repair_backlog_size(&r, "node-1", 100);
        set_repair_backlog_size(&r, "node-1", 50);
        let labels = Labels::from_pairs(&[("node_id", "node-1")]);
        assert_eq!(r.gauge(REPAIR_BACKLOG_SIZE, &labels), Some(50.0));
    }

    #[test]
    fn test_record_repair_completion() {
        let r = InMemoryRecorder::new();
        record_repair_completion(&r, "node-1");
        record_repair_completion(&r, "node-1");
        record_repair_completion(&r, "node-2");
        let l1 = Labels::from_pairs(&[("node_id", "node-1")]);
        let l2 = Labels::from_pairs(&[("node_id", "node-2")]);
        assert_eq!(r.counter(REPAIR_COMPLETIONS_TOTAL, &l1), 2);
        assert_eq!(r.counter(REPAIR_COMPLETIONS_TOTAL, &l2), 1);
    }

    // -- Helper function tests (P8.7) ---------------------------------------

    #[test]
    fn test_set_orphaned_extent_files() {
        let r = InMemoryRecorder::new();
        set_orphaned_extent_files(&r, "node-1", 7);
        let labels = Labels::from_pairs(&[("node_id", "node-1")]);
        assert_eq!(r.gauge(ORPHANED_EXTENT_FILES, &labels), Some(7.0));
    }

    #[test]
    fn test_set_orphaned_extent_files_update() {
        let r = InMemoryRecorder::new();
        set_orphaned_extent_files(&r, "node-1", 10);
        set_orphaned_extent_files(&r, "node-1", 3);
        let labels = Labels::from_pairs(&[("node_id", "node-1")]);
        assert_eq!(r.gauge(ORPHANED_EXTENT_FILES, &labels), Some(3.0));
    }

    // -- Helper function tests (P8.8) ---------------------------------------

    #[test]
    fn test_set_metadata_quorum_health_healthy() {
        let r = InMemoryRecorder::new();
        set_metadata_quorum_health(&r, "rg-1", true);
        let labels = Labels::from_pairs(&[("raft_group_id", "rg-1")]);
        assert_eq!(r.gauge(METADATA_QUORUM_HEALTH, &labels), Some(1.0));
    }

    #[test]
    fn test_set_metadata_quorum_health_unhealthy() {
        let r = InMemoryRecorder::new();
        set_metadata_quorum_health(&r, "rg-1", false);
        let labels = Labels::from_pairs(&[("raft_group_id", "rg-1")]);
        assert_eq!(r.gauge(METADATA_QUORUM_HEALTH, &labels), Some(0.0));
    }

    #[test]
    fn test_set_metadata_quorum_health_toggle() {
        let r = InMemoryRecorder::new();
        set_metadata_quorum_health(&r, "rg-1", true);
        set_metadata_quorum_health(&r, "rg-1", false);
        let labels = Labels::from_pairs(&[("raft_group_id", "rg-1")]);
        assert_eq!(r.gauge(METADATA_QUORUM_HEALTH, &labels), Some(0.0));
    }

    #[test]
    fn test_record_metadata_commit_latency() {
        let r = InMemoryRecorder::new();
        record_metadata_commit_latency(&r, "rg-1", 0.005);
        record_metadata_commit_latency(&r, "rg-1", 0.010);
        record_metadata_commit_latency(&r, "rg-1", 0.015);
        let labels = Labels::from_pairs(&[("raft_group_id", "rg-1")]);
        let obs = r.histogram(METADATA_COMMIT_LATENCY_SECONDS, &labels);
        assert_eq!(obs, vec![0.005, 0.010, 0.015]);
    }

    #[test]
    fn test_record_metadata_commit_latency_distinct_groups() {
        let r = InMemoryRecorder::new();
        record_metadata_commit_latency(&r, "rg-1", 0.001);
        record_metadata_commit_latency(&r, "rg-2", 0.100);
        let l1 = Labels::from_pairs(&[("raft_group_id", "rg-1")]);
        let l2 = Labels::from_pairs(&[("raft_group_id", "rg-2")]);
        assert_eq!(
            r.histogram(METADATA_COMMIT_LATENCY_SECONDS, &l1),
            vec![0.001]
        );
        assert_eq!(
            r.histogram(METADATA_COMMIT_LATENCY_SECONDS, &l2),
            vec![0.100]
        );
    }

    // -- Trait object tests -------------------------------------------------

    #[test]
    fn test_metrics_recorder_as_trait_object() {
        let r: Box<dyn MetricsRecorder> = Box::new(InMemoryRecorder::new());
        let labels = Labels::new();
        r.increment_counter("c", &labels, 1);
        r.set_gauge("g", &labels, 1.0);
        r.record_histogram("h", &labels, 0.5);
    }

    #[test]
    fn test_noop_recorder_as_trait_object() {
        let r: Box<dyn MetricsRecorder> = Box::new(NoopRecorder);
        let labels = Labels::new();
        r.increment_counter("c", &labels, 1);
        r.set_gauge("g", &labels, 1.0);
        r.record_histogram("h", &labels, 0.5);
    }

    // -- End-to-end scenario tests ------------------------------------------

    #[test]
    fn test_scenario_volume_io_mixed() {
        let r = InMemoryRecorder::new();
        let vol = "vol-abc";
        for _ in 0..10 {
            record_volume_io_success(&r, vol);
        }
        for _ in 0..3 {
            record_volume_io_failure(&r, vol);
        }
        let labels = Labels::from_pairs(&[("volume_id", vol)]);
        assert_eq!(r.counter(VOLUME_IO_SUCCESS_TOTAL, &labels), 10);
        assert_eq!(r.counter(VOLUME_IO_FAILURE_TOTAL, &labels), 3);
    }

    #[test]
    fn test_scenario_disk_lifecycle() {
        let r = InMemoryRecorder::new();
        let disk = "disk-xyz";
        record_disk_state_transition(&r, disk, "healthy", "suspect");
        record_disk_state_transition(&r, disk, "suspect", "degraded");
        record_disk_state_transition(&r, disk, "degraded", "failed");
        record_disk_state_transition(&r, disk, "failed", "removed");

        let check = |from: &str, to: &str, expected: u64| {
            let labels =
                Labels::from_pairs(&[("disk_id", disk), ("from_state", from), ("to_state", to)]);
            assert_eq!(r.counter(DISK_STATE_TRANSITION_TOTAL, &labels), expected);
        };
        check("healthy", "suspect", 1);
        check("suspect", "degraded", 1);
        check("degraded", "failed", 1);
        check("failed", "removed", 1);
    }

    #[test]
    fn test_scenario_scrub_and_repair() {
        let r = InMemoryRecorder::new();
        let node = "node-1";
        let disk = "disk-a";

        // Scrub finds 3 issues
        for _ in 0..3 {
            record_scrub_finding(&r, node, disk);
        }
        set_scrub_last_completed(&r, node, disk, 1700000000.0);

        // Repair processes them
        set_repair_backlog_size(&r, node, 3);
        record_repair_completion(&r, node);
        set_repair_backlog_size(&r, node, 2);
        record_repair_completion(&r, node);
        set_repair_backlog_size(&r, node, 1);
        record_repair_completion(&r, node);
        set_repair_backlog_size(&r, node, 0);

        let scrub_labels = Labels::from_pairs(&[("node_id", node), ("disk_id", disk)]);
        assert_eq!(r.counter(SCRUB_FINDINGS_TOTAL, &scrub_labels), 3);
        assert_eq!(
            r.gauge(SCRUB_LAST_COMPLETED_TIMESTAMP, &scrub_labels),
            Some(1700000000.0)
        );

        let node_labels = Labels::from_pairs(&[("node_id", node)]);
        assert_eq!(r.gauge(REPAIR_BACKLOG_SIZE, &node_labels), Some(0.0));
        assert_eq!(r.counter(REPAIR_COMPLETIONS_TOTAL, &node_labels), 3);
    }

    #[test]
    fn test_scenario_metadata_quorum() {
        let r = InMemoryRecorder::new();
        let rg = "rg-1";

        // Initial healthy quorum
        set_metadata_quorum_health(&r, rg, true);

        // Record some commits
        record_metadata_commit_latency(&r, rg, 0.002);
        record_metadata_commit_latency(&r, rg, 0.005);
        record_metadata_commit_latency(&r, rg, 0.003);

        // Quorum lost
        set_metadata_quorum_health(&r, rg, false);

        let labels = Labels::from_pairs(&[("raft_group_id", rg)]);
        assert_eq!(r.gauge(METADATA_QUORUM_HEALTH, &labels), Some(0.0));
        let obs = r.histogram(METADATA_COMMIT_LATENCY_SECONDS, &labels);
        assert_eq!(obs.len(), 3);
    }

    #[test]
    fn test_scenario_node_io_load() {
        let r = InMemoryRecorder::new();
        let node = "node-1";

        set_foreground_io_load(&r, node, 0);
        set_background_io_load(&r, node, 0);

        // Simulate workload increase
        set_foreground_io_load(&r, node, 10);
        set_background_io_load(&r, node, 3);

        // Simulate workload decrease
        set_foreground_io_load(&r, node, 2);
        set_background_io_load(&r, node, 1);

        let labels = Labels::from_pairs(&[("node_id", node)]);
        assert_eq!(r.gauge(NODE_FOREGROUND_IO_LOAD, &labels), Some(2.0));
        assert_eq!(r.gauge(NODE_BACKGROUND_IO_LOAD, &labels), Some(1.0));
    }

    #[test]
    fn test_scenario_orphaned_extents() {
        let r = InMemoryRecorder::new();
        set_orphaned_extent_files(&r, "node-1", 15);
        set_orphaned_extent_files(&r, "node-2", 0);

        let l1 = Labels::from_pairs(&[("node_id", "node-1")]);
        let l2 = Labels::from_pairs(&[("node_id", "node-2")]);
        assert_eq!(r.gauge(ORPHANED_EXTENT_FILES, &l1), Some(15.0));
        assert_eq!(r.gauge(ORPHANED_EXTENT_FILES, &l2), Some(0.0));
    }
}
