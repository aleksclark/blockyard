//! Observability metrics for Blockyard (§9).
//!
//! Defines the [`MetricsRecorder`] trait and an [`InMemoryRecorder`] implementation
//! that covers every Phase 8 metric:
//!
//! - **P8.1** Per-volume IO success/failure rates
//! - **P8.2** Client watermark and stale-epoch retry counts
//! - **P8.3** Per-node foreground and background IO load
//! - **P8.4** Per-disk health state transitions
//! - **P8.5** Scrub findings
//! - **P8.6** Repair backlog
//! - **P8.7** Orphaned extent file counts
//! - **P8.8** Metadata quorum health and commit latency

use std::collections::HashMap;
use std::time::Duration;

use parking_lot::RwLock;

use crate::disk_state::DiskState;
use crate::id::{DiskId, NodeId, VolumeId};

/// Outcome of an IO operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IoOutcome {
    Success,
    Failure,
}

/// Category of IO load for P8.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IoCategory {
    Foreground,
    Background,
}

/// Severity of a scrub finding for P8.5.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScrubFindingKind {
    ChecksumMismatch,
    ReadError,
    MissingExtent,
    MetadataCorruption,
}

/// Quorum health status for P8.8.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum QuorumHealth {
    #[default]
    Healthy,
    Degraded,
    Lost,
}

/// Trait defining all observability recording operations.
///
/// Implementations must be `Send + Sync` so they can be shared across async tasks.
pub trait MetricsRecorder: Send + Sync {
    // -- P8.1: Per-volume IO success/failure rates --

    fn record_volume_io(&self, volume: VolumeId, outcome: IoOutcome);

    // -- P8.2: Client watermark and stale-epoch retry counts --

    fn record_watermark_update(&self, volume: VolumeId, watermark: u64);

    fn record_stale_epoch_retry(&self, volume: VolumeId);

    // -- P8.3: Per-node foreground and background IO load --

    fn record_node_io(&self, node: NodeId, category: IoCategory);

    // -- P8.4: Per-disk health state transitions --

    fn record_disk_state_transition(&self, disk: DiskId, from: DiskState, to: DiskState);

    // -- P8.5: Scrub findings --

    fn record_scrub_finding(&self, disk: DiskId, kind: ScrubFindingKind);

    fn record_scrub_completion(&self, disk: DiskId, extents_checked: u64);

    // -- P8.6: Repair backlog --

    fn set_repair_backlog(&self, count: u64);

    fn record_repair_completion(&self);

    // -- P8.7: Orphaned extent file counts --

    fn set_orphaned_extents(&self, count: u64);

    // -- P8.8: Metadata quorum health and commit latency --

    fn set_quorum_health(&self, health: QuorumHealth);

    fn record_commit_latency(&self, latency: Duration);
}

/// Per-volume IO counters for P8.1.
#[derive(Debug, Clone, Default)]
pub struct VolumeIoCounters {
    pub success: u64,
    pub failure: u64,
}

/// Per-volume watermark/retry state for P8.2.
#[derive(Debug, Clone, Default)]
pub struct WatermarkState {
    pub current_watermark: u64,
    pub stale_epoch_retries: u64,
}

/// Per-node IO load counters for P8.3.
#[derive(Debug, Clone, Default)]
pub struct NodeIoLoad {
    pub foreground: u64,
    pub background: u64,
}

/// A single disk state transition event for P8.4.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskTransitionEvent {
    pub disk: DiskId,
    pub from: DiskState,
    pub to: DiskState,
}

/// Per-disk scrub summary for P8.5.
#[derive(Debug, Clone, Default)]
pub struct ScrubSummary {
    pub checksum_mismatches: u64,
    pub read_errors: u64,
    pub missing_extents: u64,
    pub metadata_corruptions: u64,
    pub extents_checked: u64,
    pub completions: u64,
}

/// Commit latency statistics for P8.8.
#[derive(Debug, Clone)]
pub struct CommitLatencyStats {
    pub count: u64,
    pub total: Duration,
    pub min: Duration,
    pub max: Duration,
}

impl Default for CommitLatencyStats {
    fn default() -> Self {
        Self {
            count: 0,
            total: Duration::ZERO,
            min: Duration::MAX,
            max: Duration::ZERO,
        }
    }
}

impl CommitLatencyStats {
    pub fn mean(&self) -> Duration {
        if self.count == 0 {
            return Duration::ZERO;
        }
        self.total / self.count as u32
    }
}

/// In-memory metrics recorder for testing and lightweight deployments.
///
/// All state is protected by `parking_lot::RwLock` for concurrent access.
#[derive(Debug, Default)]
pub struct InMemoryRecorder {
    volume_io: RwLock<HashMap<VolumeId, VolumeIoCounters>>,
    watermarks: RwLock<HashMap<VolumeId, WatermarkState>>,
    node_io: RwLock<HashMap<NodeId, NodeIoLoad>>,
    disk_transitions: RwLock<Vec<DiskTransitionEvent>>,
    scrub_summaries: RwLock<HashMap<DiskId, ScrubSummary>>,
    repair_backlog: RwLock<u64>,
    repair_completions: RwLock<u64>,
    orphaned_extents: RwLock<u64>,
    quorum_health: RwLock<QuorumHealth>,
    commit_latency: RwLock<CommitLatencyStats>,
}

impl InMemoryRecorder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn volume_io(&self, volume: &VolumeId) -> Option<VolumeIoCounters> {
        self.volume_io.read().get(volume).cloned()
    }

    pub fn all_volume_io(&self) -> HashMap<VolumeId, VolumeIoCounters> {
        self.volume_io.read().clone()
    }

    pub fn watermark_state(&self, volume: &VolumeId) -> Option<WatermarkState> {
        self.watermarks.read().get(volume).cloned()
    }

    pub fn all_watermark_states(&self) -> HashMap<VolumeId, WatermarkState> {
        self.watermarks.read().clone()
    }

    pub fn node_io(&self, node: &NodeId) -> Option<NodeIoLoad> {
        self.node_io.read().get(node).cloned()
    }

    pub fn all_node_io(&self) -> HashMap<NodeId, NodeIoLoad> {
        self.node_io.read().clone()
    }

    pub fn disk_transitions(&self) -> Vec<DiskTransitionEvent> {
        self.disk_transitions.read().clone()
    }

    pub fn scrub_summary(&self, disk: &DiskId) -> Option<ScrubSummary> {
        self.scrub_summaries.read().get(disk).cloned()
    }

    pub fn all_scrub_summaries(&self) -> HashMap<DiskId, ScrubSummary> {
        self.scrub_summaries.read().clone()
    }

    pub fn repair_backlog(&self) -> u64 {
        *self.repair_backlog.read()
    }

    pub fn repair_completions(&self) -> u64 {
        *self.repair_completions.read()
    }

    pub fn orphaned_extents(&self) -> u64 {
        *self.orphaned_extents.read()
    }

    pub fn quorum_health(&self) -> QuorumHealth {
        *self.quorum_health.read()
    }

    pub fn commit_latency_stats(&self) -> CommitLatencyStats {
        self.commit_latency.read().clone()
    }

    pub fn reset(&self) {
        self.volume_io.write().clear();
        self.watermarks.write().clear();
        self.node_io.write().clear();
        self.disk_transitions.write().clear();
        self.scrub_summaries.write().clear();
        *self.repair_backlog.write() = 0;
        *self.repair_completions.write() = 0;
        *self.orphaned_extents.write() = 0;
        *self.quorum_health.write() = QuorumHealth::Healthy;
        *self.commit_latency.write() = CommitLatencyStats::default();
    }
}

impl MetricsRecorder for InMemoryRecorder {
    fn record_volume_io(&self, volume: VolumeId, outcome: IoOutcome) {
        let mut map = self.volume_io.write();
        let counters = map.entry(volume).or_default();
        match outcome {
            IoOutcome::Success => counters.success += 1,
            IoOutcome::Failure => counters.failure += 1,
        }
    }

    fn record_watermark_update(&self, volume: VolumeId, watermark: u64) {
        let mut map = self.watermarks.write();
        let state = map.entry(volume).or_default();
        state.current_watermark = watermark;
    }

    fn record_stale_epoch_retry(&self, volume: VolumeId) {
        let mut map = self.watermarks.write();
        let state = map.entry(volume).or_default();
        state.stale_epoch_retries += 1;
    }

    fn record_node_io(&self, node: NodeId, category: IoCategory) {
        let mut map = self.node_io.write();
        let load = map.entry(node).or_default();
        match category {
            IoCategory::Foreground => load.foreground += 1,
            IoCategory::Background => load.background += 1,
        }
    }

    fn record_disk_state_transition(&self, disk: DiskId, from: DiskState, to: DiskState) {
        self.disk_transitions.write().push(DiskTransitionEvent {
            disk,
            from,
            to,
        });
    }

    fn record_scrub_finding(&self, disk: DiskId, kind: ScrubFindingKind) {
        let mut map = self.scrub_summaries.write();
        let summary = map.entry(disk).or_default();
        match kind {
            ScrubFindingKind::ChecksumMismatch => summary.checksum_mismatches += 1,
            ScrubFindingKind::ReadError => summary.read_errors += 1,
            ScrubFindingKind::MissingExtent => summary.missing_extents += 1,
            ScrubFindingKind::MetadataCorruption => summary.metadata_corruptions += 1,
        }
    }

    fn record_scrub_completion(&self, disk: DiskId, extents_checked: u64) {
        let mut map = self.scrub_summaries.write();
        let summary = map.entry(disk).or_default();
        summary.extents_checked += extents_checked;
        summary.completions += 1;
    }

    fn set_repair_backlog(&self, count: u64) {
        *self.repair_backlog.write() = count;
    }

    fn record_repair_completion(&self) {
        *self.repair_completions.write() += 1;
    }

    fn set_orphaned_extents(&self, count: u64) {
        *self.orphaned_extents.write() = count;
    }

    fn set_quorum_health(&self, health: QuorumHealth) {
        *self.quorum_health.write() = health;
    }

    fn record_commit_latency(&self, latency: Duration) {
        let mut stats = self.commit_latency.write();
        stats.count += 1;
        stats.total += latency;
        if latency < stats.min {
            stats.min = latency;
        }
        if latency > stats.max {
            stats.max = latency;
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{DiskId, NodeId, VolumeId};

    fn recorder() -> InMemoryRecorder {
        InMemoryRecorder::new()
    }

    // -- P8.1: Per-volume IO rates --

    #[test]
    fn test_volume_io_success() {
        let r = recorder();
        let v = VolumeId::generate();
        r.record_volume_io(v, IoOutcome::Success);
        let c = r.volume_io(&v).unwrap();
        assert_eq!(c.success, 1);
        assert_eq!(c.failure, 0);
    }

    #[test]
    fn test_volume_io_failure() {
        let r = recorder();
        let v = VolumeId::generate();
        r.record_volume_io(v, IoOutcome::Failure);
        let c = r.volume_io(&v).unwrap();
        assert_eq!(c.success, 0);
        assert_eq!(c.failure, 1);
    }

    #[test]
    fn test_volume_io_mixed() {
        let r = recorder();
        let v = VolumeId::generate();
        r.record_volume_io(v, IoOutcome::Success);
        r.record_volume_io(v, IoOutcome::Success);
        r.record_volume_io(v, IoOutcome::Failure);
        let c = r.volume_io(&v).unwrap();
        assert_eq!(c.success, 2);
        assert_eq!(c.failure, 1);
    }

    #[test]
    fn test_volume_io_multiple_volumes() {
        let r = recorder();
        let v1 = VolumeId::generate();
        let v2 = VolumeId::generate();
        r.record_volume_io(v1, IoOutcome::Success);
        r.record_volume_io(v2, IoOutcome::Failure);
        assert_eq!(r.volume_io(&v1).unwrap().success, 1);
        assert_eq!(r.volume_io(&v2).unwrap().failure, 1);
    }

    #[test]
    fn test_volume_io_unknown_volume() {
        let r = recorder();
        let v = VolumeId::generate();
        assert!(r.volume_io(&v).is_none());
    }

    #[test]
    fn test_all_volume_io() {
        let r = recorder();
        let v1 = VolumeId::generate();
        let v2 = VolumeId::generate();
        r.record_volume_io(v1, IoOutcome::Success);
        r.record_volume_io(v2, IoOutcome::Failure);
        let all = r.all_volume_io();
        assert_eq!(all.len(), 2);
    }

    // -- P8.2: Watermark and stale-epoch retries --

    #[test]
    fn test_watermark_update() {
        let r = recorder();
        let v = VolumeId::generate();
        r.record_watermark_update(v, 42);
        let s = r.watermark_state(&v).unwrap();
        assert_eq!(s.current_watermark, 42);
        assert_eq!(s.stale_epoch_retries, 0);
    }

    #[test]
    fn test_watermark_advances() {
        let r = recorder();
        let v = VolumeId::generate();
        r.record_watermark_update(v, 10);
        r.record_watermark_update(v, 20);
        assert_eq!(r.watermark_state(&v).unwrap().current_watermark, 20);
    }

    #[test]
    fn test_stale_epoch_retry() {
        let r = recorder();
        let v = VolumeId::generate();
        r.record_stale_epoch_retry(v);
        r.record_stale_epoch_retry(v);
        let s = r.watermark_state(&v).unwrap();
        assert_eq!(s.stale_epoch_retries, 2);
    }

    #[test]
    fn test_watermark_unknown_volume() {
        let r = recorder();
        assert!(r.watermark_state(&VolumeId::generate()).is_none());
    }

    #[test]
    fn test_all_watermark_states() {
        let r = recorder();
        let v1 = VolumeId::generate();
        let v2 = VolumeId::generate();
        r.record_watermark_update(v1, 5);
        r.record_stale_epoch_retry(v2);
        let all = r.all_watermark_states();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_watermark_and_retry_same_volume() {
        let r = recorder();
        let v = VolumeId::generate();
        r.record_watermark_update(v, 100);
        r.record_stale_epoch_retry(v);
        let s = r.watermark_state(&v).unwrap();
        assert_eq!(s.current_watermark, 100);
        assert_eq!(s.stale_epoch_retries, 1);
    }

    // -- P8.3: Foreground/background IO load --

    #[test]
    fn test_node_io_foreground() {
        let r = recorder();
        let n = NodeId::generate();
        r.record_node_io(n, IoCategory::Foreground);
        let l = r.node_io(&n).unwrap();
        assert_eq!(l.foreground, 1);
        assert_eq!(l.background, 0);
    }

    #[test]
    fn test_node_io_background() {
        let r = recorder();
        let n = NodeId::generate();
        r.record_node_io(n, IoCategory::Background);
        let l = r.node_io(&n).unwrap();
        assert_eq!(l.foreground, 0);
        assert_eq!(l.background, 1);
    }

    #[test]
    fn test_node_io_mixed() {
        let r = recorder();
        let n = NodeId::generate();
        r.record_node_io(n, IoCategory::Foreground);
        r.record_node_io(n, IoCategory::Foreground);
        r.record_node_io(n, IoCategory::Background);
        let l = r.node_io(&n).unwrap();
        assert_eq!(l.foreground, 2);
        assert_eq!(l.background, 1);
    }

    #[test]
    fn test_node_io_multiple_nodes() {
        let r = recorder();
        let n1 = NodeId::generate();
        let n2 = NodeId::generate();
        r.record_node_io(n1, IoCategory::Foreground);
        r.record_node_io(n2, IoCategory::Background);
        assert_eq!(r.node_io(&n1).unwrap().foreground, 1);
        assert_eq!(r.node_io(&n2).unwrap().background, 1);
    }

    #[test]
    fn test_node_io_unknown_node() {
        let r = recorder();
        assert!(r.node_io(&NodeId::generate()).is_none());
    }

    #[test]
    fn test_all_node_io() {
        let r = recorder();
        let n1 = NodeId::generate();
        let n2 = NodeId::generate();
        r.record_node_io(n1, IoCategory::Foreground);
        r.record_node_io(n2, IoCategory::Background);
        assert_eq!(r.all_node_io().len(), 2);
    }

    // -- P8.4: Disk health state transitions --

    #[test]
    fn test_disk_state_transition() {
        let r = recorder();
        let d = DiskId::generate();
        r.record_disk_state_transition(d, DiskState::Healthy, DiskState::Suspect);
        let events = r.disk_transitions();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].disk, d);
        assert_eq!(events[0].from, DiskState::Healthy);
        assert_eq!(events[0].to, DiskState::Suspect);
    }

    #[test]
    fn test_disk_state_multiple_transitions() {
        let r = recorder();
        let d = DiskId::generate();
        r.record_disk_state_transition(d, DiskState::Healthy, DiskState::Suspect);
        r.record_disk_state_transition(d, DiskState::Suspect, DiskState::Degraded);
        r.record_disk_state_transition(d, DiskState::Degraded, DiskState::Failed);
        let events = r.disk_transitions();
        assert_eq!(events.len(), 3);
        assert_eq!(events[2].to, DiskState::Failed);
    }

    #[test]
    fn test_disk_state_transitions_multiple_disks() {
        let r = recorder();
        let d1 = DiskId::generate();
        let d2 = DiskId::generate();
        r.record_disk_state_transition(d1, DiskState::Healthy, DiskState::Failed);
        r.record_disk_state_transition(d2, DiskState::Healthy, DiskState::Draining);
        let events = r.disk_transitions();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_disk_transitions_empty() {
        let r = recorder();
        assert!(r.disk_transitions().is_empty());
    }

    #[test]
    fn test_disk_transition_event_eq() {
        let d = DiskId::generate();
        let e1 = DiskTransitionEvent {
            disk: d,
            from: DiskState::Healthy,
            to: DiskState::Suspect,
        };
        let e2 = DiskTransitionEvent {
            disk: d,
            from: DiskState::Healthy,
            to: DiskState::Suspect,
        };
        assert_eq!(e1, e2);
    }

    // -- P8.5: Scrub findings --

    #[test]
    fn test_scrub_finding_checksum_mismatch() {
        let r = recorder();
        let d = DiskId::generate();
        r.record_scrub_finding(d, ScrubFindingKind::ChecksumMismatch);
        let s = r.scrub_summary(&d).unwrap();
        assert_eq!(s.checksum_mismatches, 1);
        assert_eq!(s.read_errors, 0);
    }

    #[test]
    fn test_scrub_finding_read_error() {
        let r = recorder();
        let d = DiskId::generate();
        r.record_scrub_finding(d, ScrubFindingKind::ReadError);
        assert_eq!(r.scrub_summary(&d).unwrap().read_errors, 1);
    }

    #[test]
    fn test_scrub_finding_missing_extent() {
        let r = recorder();
        let d = DiskId::generate();
        r.record_scrub_finding(d, ScrubFindingKind::MissingExtent);
        assert_eq!(r.scrub_summary(&d).unwrap().missing_extents, 1);
    }

    #[test]
    fn test_scrub_finding_metadata_corruption() {
        let r = recorder();
        let d = DiskId::generate();
        r.record_scrub_finding(d, ScrubFindingKind::MetadataCorruption);
        assert_eq!(r.scrub_summary(&d).unwrap().metadata_corruptions, 1);
    }

    #[test]
    fn test_scrub_completion() {
        let r = recorder();
        let d = DiskId::generate();
        r.record_scrub_completion(d, 500);
        let s = r.scrub_summary(&d).unwrap();
        assert_eq!(s.extents_checked, 500);
        assert_eq!(s.completions, 1);
    }

    #[test]
    fn test_scrub_completion_accumulates() {
        let r = recorder();
        let d = DiskId::generate();
        r.record_scrub_completion(d, 100);
        r.record_scrub_completion(d, 200);
        let s = r.scrub_summary(&d).unwrap();
        assert_eq!(s.extents_checked, 300);
        assert_eq!(s.completions, 2);
    }

    #[test]
    fn test_scrub_findings_mixed() {
        let r = recorder();
        let d = DiskId::generate();
        r.record_scrub_finding(d, ScrubFindingKind::ChecksumMismatch);
        r.record_scrub_finding(d, ScrubFindingKind::ChecksumMismatch);
        r.record_scrub_finding(d, ScrubFindingKind::ReadError);
        r.record_scrub_completion(d, 1000);
        let s = r.scrub_summary(&d).unwrap();
        assert_eq!(s.checksum_mismatches, 2);
        assert_eq!(s.read_errors, 1);
        assert_eq!(s.extents_checked, 1000);
    }

    #[test]
    fn test_scrub_unknown_disk() {
        let r = recorder();
        assert!(r.scrub_summary(&DiskId::generate()).is_none());
    }

    #[test]
    fn test_all_scrub_summaries() {
        let r = recorder();
        let d1 = DiskId::generate();
        let d2 = DiskId::generate();
        r.record_scrub_finding(d1, ScrubFindingKind::ReadError);
        r.record_scrub_completion(d2, 50);
        assert_eq!(r.all_scrub_summaries().len(), 2);
    }

    // -- P8.6: Repair backlog --

    #[test]
    fn test_repair_backlog_set() {
        let r = recorder();
        r.set_repair_backlog(42);
        assert_eq!(r.repair_backlog(), 42);
    }

    #[test]
    fn test_repair_backlog_overwrite() {
        let r = recorder();
        r.set_repair_backlog(100);
        r.set_repair_backlog(50);
        assert_eq!(r.repair_backlog(), 50);
    }

    #[test]
    fn test_repair_completion() {
        let r = recorder();
        r.record_repair_completion();
        r.record_repair_completion();
        assert_eq!(r.repair_completions(), 2);
    }

    #[test]
    fn test_repair_backlog_default() {
        let r = recorder();
        assert_eq!(r.repair_backlog(), 0);
    }

    #[test]
    fn test_repair_completions_default() {
        let r = recorder();
        assert_eq!(r.repair_completions(), 0);
    }

    // -- P8.7: Orphaned extent file counts --

    #[test]
    fn test_orphaned_extents_set() {
        let r = recorder();
        r.set_orphaned_extents(10);
        assert_eq!(r.orphaned_extents(), 10);
    }

    #[test]
    fn test_orphaned_extents_overwrite() {
        let r = recorder();
        r.set_orphaned_extents(10);
        r.set_orphaned_extents(5);
        assert_eq!(r.orphaned_extents(), 5);
    }

    #[test]
    fn test_orphaned_extents_zero() {
        let r = recorder();
        r.set_orphaned_extents(10);
        r.set_orphaned_extents(0);
        assert_eq!(r.orphaned_extents(), 0);
    }

    #[test]
    fn test_orphaned_extents_default() {
        let r = recorder();
        assert_eq!(r.orphaned_extents(), 0);
    }

    // -- P8.8: Quorum health and commit latency --

    #[test]
    fn test_quorum_health_default() {
        let r = recorder();
        assert_eq!(r.quorum_health(), QuorumHealth::Healthy);
    }

    #[test]
    fn test_quorum_health_degraded() {
        let r = recorder();
        r.set_quorum_health(QuorumHealth::Degraded);
        assert_eq!(r.quorum_health(), QuorumHealth::Degraded);
    }

    #[test]
    fn test_quorum_health_lost() {
        let r = recorder();
        r.set_quorum_health(QuorumHealth::Lost);
        assert_eq!(r.quorum_health(), QuorumHealth::Lost);
    }

    #[test]
    fn test_quorum_health_recovery() {
        let r = recorder();
        r.set_quorum_health(QuorumHealth::Lost);
        r.set_quorum_health(QuorumHealth::Healthy);
        assert_eq!(r.quorum_health(), QuorumHealth::Healthy);
    }

    #[test]
    fn test_commit_latency_single() {
        let r = recorder();
        r.record_commit_latency(Duration::from_millis(10));
        let stats = r.commit_latency_stats();
        assert_eq!(stats.count, 1);
        assert_eq!(stats.total, Duration::from_millis(10));
        assert_eq!(stats.min, Duration::from_millis(10));
        assert_eq!(stats.max, Duration::from_millis(10));
    }

    #[test]
    fn test_commit_latency_multiple() {
        let r = recorder();
        r.record_commit_latency(Duration::from_millis(10));
        r.record_commit_latency(Duration::from_millis(30));
        r.record_commit_latency(Duration::from_millis(20));
        let stats = r.commit_latency_stats();
        assert_eq!(stats.count, 3);
        assert_eq!(stats.total, Duration::from_millis(60));
        assert_eq!(stats.min, Duration::from_millis(10));
        assert_eq!(stats.max, Duration::from_millis(30));
    }

    #[test]
    fn test_commit_latency_mean() {
        let r = recorder();
        r.record_commit_latency(Duration::from_millis(10));
        r.record_commit_latency(Duration::from_millis(30));
        let stats = r.commit_latency_stats();
        assert_eq!(stats.mean(), Duration::from_millis(20));
    }

    #[test]
    fn test_commit_latency_mean_empty() {
        let stats = CommitLatencyStats::default();
        assert_eq!(stats.mean(), Duration::ZERO);
    }

    #[test]
    fn test_commit_latency_default() {
        let r = recorder();
        let stats = r.commit_latency_stats();
        assert_eq!(stats.count, 0);
        assert_eq!(stats.total, Duration::ZERO);
        assert_eq!(stats.min, Duration::MAX);
        assert_eq!(stats.max, Duration::ZERO);
    }

    // -- Reset --

    #[test]
    fn test_reset_clears_all() {
        let r = recorder();
        let v = VolumeId::generate();
        let n = NodeId::generate();
        let d = DiskId::generate();

        r.record_volume_io(v, IoOutcome::Success);
        r.record_watermark_update(v, 10);
        r.record_stale_epoch_retry(v);
        r.record_node_io(n, IoCategory::Foreground);
        r.record_disk_state_transition(d, DiskState::Healthy, DiskState::Failed);
        r.record_scrub_finding(d, ScrubFindingKind::ReadError);
        r.set_repair_backlog(5);
        r.record_repair_completion();
        r.set_orphaned_extents(3);
        r.set_quorum_health(QuorumHealth::Lost);
        r.record_commit_latency(Duration::from_millis(50));

        r.reset();

        assert!(r.volume_io(&v).is_none());
        assert!(r.watermark_state(&v).is_none());
        assert!(r.node_io(&n).is_none());
        assert!(r.disk_transitions().is_empty());
        assert!(r.scrub_summary(&d).is_none());
        assert_eq!(r.repair_backlog(), 0);
        assert_eq!(r.repair_completions(), 0);
        assert_eq!(r.orphaned_extents(), 0);
        assert_eq!(r.quorum_health(), QuorumHealth::Healthy);
        assert_eq!(r.commit_latency_stats().count, 0);
    }

    // -- Trait object safety --

    #[test]
    fn test_recorder_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<InMemoryRecorder>();
    }

    #[test]
    fn test_trait_object() {
        let r: Box<dyn MetricsRecorder> = Box::new(InMemoryRecorder::new());
        let v = VolumeId::generate();
        r.record_volume_io(v, IoOutcome::Success);
    }

    // -- Debug impls --

    #[test]
    fn test_io_outcome_debug() {
        assert_eq!(format!("{:?}", IoOutcome::Success), "Success");
        assert_eq!(format!("{:?}", IoOutcome::Failure), "Failure");
    }

    #[test]
    fn test_io_category_debug() {
        assert_eq!(format!("{:?}", IoCategory::Foreground), "Foreground");
        assert_eq!(format!("{:?}", IoCategory::Background), "Background");
    }

    #[test]
    fn test_scrub_finding_kind_debug() {
        assert_eq!(
            format!("{:?}", ScrubFindingKind::ChecksumMismatch),
            "ChecksumMismatch"
        );
        assert_eq!(format!("{:?}", ScrubFindingKind::ReadError), "ReadError");
        assert_eq!(
            format!("{:?}", ScrubFindingKind::MissingExtent),
            "MissingExtent"
        );
        assert_eq!(
            format!("{:?}", ScrubFindingKind::MetadataCorruption),
            "MetadataCorruption"
        );
    }

    #[test]
    fn test_quorum_health_debug() {
        assert_eq!(format!("{:?}", QuorumHealth::Healthy), "Healthy");
        assert_eq!(format!("{:?}", QuorumHealth::Degraded), "Degraded");
        assert_eq!(format!("{:?}", QuorumHealth::Lost), "Lost");
    }

    #[test]
    fn test_volume_io_counters_debug() {
        let c = VolumeIoCounters {
            success: 1,
            failure: 2,
        };
        let debug = format!("{:?}", c);
        assert!(debug.contains("success: 1"));
        assert!(debug.contains("failure: 2"));
    }

    #[test]
    fn test_watermark_state_debug() {
        let s = WatermarkState {
            current_watermark: 42,
            stale_epoch_retries: 3,
        };
        let debug = format!("{:?}", s);
        assert!(debug.contains("42"));
        assert!(debug.contains("3"));
    }

    #[test]
    fn test_node_io_load_debug() {
        let l = NodeIoLoad {
            foreground: 10,
            background: 5,
        };
        let debug = format!("{:?}", l);
        assert!(debug.contains("10"));
        assert!(debug.contains("5"));
    }

    #[test]
    fn test_disk_transition_event_debug() {
        let d = DiskId::generate();
        let e = DiskTransitionEvent {
            disk: d,
            from: DiskState::Healthy,
            to: DiskState::Failed,
        };
        let debug = format!("{:?}", e);
        assert!(debug.contains("Healthy"));
        assert!(debug.contains("Failed"));
    }

    #[test]
    fn test_scrub_summary_debug() {
        let s = ScrubSummary {
            checksum_mismatches: 1,
            read_errors: 2,
            missing_extents: 3,
            metadata_corruptions: 4,
            extents_checked: 100,
            completions: 5,
        };
        let debug = format!("{:?}", s);
        assert!(debug.contains("checksum_mismatches: 1"));
    }

    #[test]
    fn test_commit_latency_stats_debug() {
        let s = CommitLatencyStats::default();
        let debug = format!("{:?}", s);
        assert!(debug.contains("count: 0"));
    }

    #[test]
    fn test_in_memory_recorder_debug() {
        let r = InMemoryRecorder::new();
        let debug = format!("{:?}", r);
        assert!(debug.contains("InMemoryRecorder"));
    }

    // -- Clone impls --

    #[test]
    fn test_io_outcome_clone() {
        let a = IoOutcome::Success;
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn test_io_category_clone() {
        let a = IoCategory::Foreground;
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn test_scrub_finding_kind_clone() {
        let a = ScrubFindingKind::ReadError;
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn test_quorum_health_clone() {
        let a = QuorumHealth::Degraded;
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn test_volume_io_counters_default() {
        let c = VolumeIoCounters::default();
        assert_eq!(c.success, 0);
        assert_eq!(c.failure, 0);
    }

    #[test]
    fn test_watermark_state_default() {
        let s = WatermarkState::default();
        assert_eq!(s.current_watermark, 0);
        assert_eq!(s.stale_epoch_retries, 0);
    }

    #[test]
    fn test_node_io_load_default() {
        let l = NodeIoLoad::default();
        assert_eq!(l.foreground, 0);
        assert_eq!(l.background, 0);
    }

    #[test]
    fn test_scrub_summary_default() {
        let s = ScrubSummary::default();
        assert_eq!(s.checksum_mismatches, 0);
        assert_eq!(s.read_errors, 0);
        assert_eq!(s.missing_extents, 0);
        assert_eq!(s.metadata_corruptions, 0);
        assert_eq!(s.extents_checked, 0);
        assert_eq!(s.completions, 0);
    }

    // -- Hash impls --

    #[test]
    fn test_io_outcome_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(IoOutcome::Success);
        set.insert(IoOutcome::Failure);
        set.insert(IoOutcome::Success);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_io_category_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(IoCategory::Foreground);
        set.insert(IoCategory::Background);
        set.insert(IoCategory::Foreground);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_scrub_finding_kind_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(ScrubFindingKind::ChecksumMismatch);
        set.insert(ScrubFindingKind::ReadError);
        set.insert(ScrubFindingKind::MissingExtent);
        set.insert(ScrubFindingKind::MetadataCorruption);
        assert_eq!(set.len(), 4);
    }

    #[test]
    fn test_quorum_health_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(QuorumHealth::Healthy);
        set.insert(QuorumHealth::Degraded);
        set.insert(QuorumHealth::Lost);
        assert_eq!(set.len(), 3);
    }

    // -- Concurrent access --

    #[test]
    fn test_concurrent_volume_io() {
        use std::sync::Arc;
        let r = Arc::new(InMemoryRecorder::new());
        let v = VolumeId::generate();
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let r = Arc::clone(&r);
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        r.record_volume_io(v, IoOutcome::Success);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(r.volume_io(&v).unwrap().success, 1000);
    }

    #[test]
    fn test_concurrent_commit_latency() {
        use std::sync::Arc;
        let r = Arc::new(InMemoryRecorder::new());
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let r = Arc::clone(&r);
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        r.record_commit_latency(Duration::from_millis(1));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(r.commit_latency_stats().count, 1000);
    }

    // -- Eq impls --

    #[test]
    fn test_io_outcome_eq() {
        assert_eq!(IoOutcome::Success, IoOutcome::Success);
        assert_ne!(IoOutcome::Success, IoOutcome::Failure);
    }

    #[test]
    fn test_io_category_eq() {
        assert_eq!(IoCategory::Foreground, IoCategory::Foreground);
        assert_ne!(IoCategory::Foreground, IoCategory::Background);
    }

    #[test]
    fn test_quorum_health_eq() {
        assert_eq!(QuorumHealth::Healthy, QuorumHealth::Healthy);
        assert_ne!(QuorumHealth::Healthy, QuorumHealth::Degraded);
        assert_ne!(QuorumHealth::Degraded, QuorumHealth::Lost);
    }

    #[test]
    fn test_quorum_health_default_impl() {
        let h = QuorumHealth::default();
        assert_eq!(h, QuorumHealth::Healthy);
    }

    // -- Volume IO counters clone --

    #[test]
    fn test_volume_io_counters_clone() {
        let c = VolumeIoCounters {
            success: 5,
            failure: 3,
        };
        let c2 = c.clone();
        assert_eq!(c2.success, 5);
        assert_eq!(c2.failure, 3);
    }

    #[test]
    fn test_watermark_state_clone() {
        let s = WatermarkState {
            current_watermark: 99,
            stale_epoch_retries: 7,
        };
        let s2 = s.clone();
        assert_eq!(s2.current_watermark, 99);
        assert_eq!(s2.stale_epoch_retries, 7);
    }

    #[test]
    fn test_node_io_load_clone() {
        let l = NodeIoLoad {
            foreground: 10,
            background: 20,
        };
        let l2 = l.clone();
        assert_eq!(l2.foreground, 10);
        assert_eq!(l2.background, 20);
    }

    #[test]
    fn test_scrub_summary_clone() {
        let s = ScrubSummary {
            checksum_mismatches: 1,
            read_errors: 2,
            missing_extents: 3,
            metadata_corruptions: 4,
            extents_checked: 500,
            completions: 6,
        };
        let s2 = s.clone();
        assert_eq!(s2.checksum_mismatches, 1);
        assert_eq!(s2.completions, 6);
    }

    #[test]
    fn test_commit_latency_stats_clone() {
        let s = CommitLatencyStats {
            count: 10,
            total: Duration::from_millis(100),
            min: Duration::from_millis(5),
            max: Duration::from_millis(20),
        };
        let s2 = s.clone();
        assert_eq!(s2.count, 10);
        assert_eq!(s2.min, Duration::from_millis(5));
    }

    #[test]
    fn test_disk_transition_event_clone() {
        let d = DiskId::generate();
        let e = DiskTransitionEvent {
            disk: d,
            from: DiskState::Healthy,
            to: DiskState::Failed,
        };
        let e2 = e.clone();
        assert_eq!(e, e2);
    }
}
