//! Latency-aware replica selector implementation.
//!
//! Selects read sources by preferring local replicas, then ordering by
//! lowest measured latency. Tracks health and failure counts per node.

use parking_lot::RwLock;
use std::collections::HashMap;

use blockyard_common::NodeId;

use crate::traits::ReplicaSelector;
use crate::types::{ReplicaHealth, ReplicaLocation, ReplicaStats};

const FAILURE_THRESHOLD: u32 = 3;
const DEFAULT_LATENCY_US: u64 = 1_000;

/// Latency-tracking replica selector.
///
/// Maintains per-node statistics and uses them to order replicas
/// for read source selection. Local replicas are always preferred
/// over remote ones.
#[derive(Debug)]
pub struct LatencyAwareSelector {
    stats: RwLock<HashMap<NodeId, ReplicaStats>>,
}

impl LatencyAwareSelector {
    pub fn new() -> Self {
        Self {
            stats: RwLock::new(HashMap::new()),
        }
    }

    fn get_or_default(&self, node_id: NodeId) -> ReplicaStats {
        let stats = self.stats.read();
        stats.get(&node_id).cloned().unwrap_or(ReplicaStats {
            node_id,
            health: ReplicaHealth::Healthy,
            latency_us: DEFAULT_LATENCY_US,
            is_local: false,
            failure_count: 0,
        })
    }
}

impl Default for LatencyAwareSelector {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicaSelector for LatencyAwareSelector {
    fn select_replicas(&self, replicas: &[ReplicaLocation]) -> Vec<NodeId> {
        let mut candidates: Vec<(NodeId, bool, ReplicaHealth, u64)> = replicas
            .iter()
            .map(|r| {
                let stats = self.get_or_default(r.node_id);
                (r.node_id, r.is_local, stats.health, stats.latency_us)
            })
            .collect();

        candidates.sort_by(|a, b| {
            let a_failed = a.2 == ReplicaHealth::Failed;
            let b_failed = b.2 == ReplicaHealth::Failed;
            a_failed
                .cmp(&b_failed)
                .then_with(|| b.1.cmp(&a.1))
                .then_with(|| health_priority(a.2).cmp(&health_priority(b.2)))
                .then_with(|| a.3.cmp(&b.3))
        });

        candidates.into_iter().map(|(id, ..)| id).collect()
    }

    fn get_stats(&self, node_id: NodeId) -> Option<ReplicaStats> {
        self.stats.read().get(&node_id).cloned()
    }

    fn record_success(&self, node_id: NodeId, latency_us: u64) {
        let mut stats = self.stats.write();
        let entry = stats.entry(node_id).or_insert_with(|| ReplicaStats {
            node_id,
            health: ReplicaHealth::Healthy,
            latency_us: DEFAULT_LATENCY_US,
            is_local: false,
            failure_count: 0,
        });
        entry.latency_us = (entry.latency_us + latency_us) / 2;
        if entry.health == ReplicaHealth::Suspect {
            entry.failure_count = entry.failure_count.saturating_sub(1);
            if entry.failure_count == 0 {
                entry.health = ReplicaHealth::Healthy;
            }
        }
    }

    fn record_failure(&self, node_id: NodeId) {
        let mut stats = self.stats.write();
        let entry = stats.entry(node_id).or_insert_with(|| ReplicaStats {
            node_id,
            health: ReplicaHealth::Healthy,
            latency_us: DEFAULT_LATENCY_US,
            is_local: false,
            failure_count: 0,
        });
        entry.failure_count += 1;
        if entry.failure_count >= FAILURE_THRESHOLD {
            entry.health = ReplicaHealth::Failed;
        } else {
            entry.health = ReplicaHealth::Suspect;
        }
    }

    fn mark_suspect(&self, node_id: NodeId) {
        let mut stats = self.stats.write();
        let entry = stats.entry(node_id).or_insert_with(|| ReplicaStats {
            node_id,
            health: ReplicaHealth::Healthy,
            latency_us: DEFAULT_LATENCY_US,
            is_local: false,
            failure_count: 0,
        });
        entry.health = ReplicaHealth::Suspect;
    }
}

fn health_priority(h: ReplicaHealth) -> u8 {
    match h {
        ReplicaHealth::Healthy => 0,
        ReplicaHealth::Suspect => 1,
        ReplicaHealth::Failed => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(n: u8) -> NodeId {
        use uuid::Uuid;
        NodeId::new(Uuid::from_bytes([n; 16]))
    }

    fn replicas(nodes: &[(u8, bool)]) -> Vec<ReplicaLocation> {
        nodes
            .iter()
            .map(|&(n, local)| ReplicaLocation {
                node_id: node(n),
                is_local: local,
            })
            .collect()
    }

    #[test]
    fn test_select_replicas_prefers_local() {
        let selector = LatencyAwareSelector::new();
        let reps = replicas(&[(1, false), (2, true), (3, false)]);
        let selected = selector.select_replicas(&reps);
        assert_eq!(selected[0], node(2));
    }

    #[test]
    fn test_select_replicas_orders_by_latency() {
        let selector = LatencyAwareSelector::new();
        selector.record_success(node(1), 500);
        selector.record_success(node(2), 100);
        selector.record_success(node(3), 300);

        let reps = replicas(&[(1, false), (2, false), (3, false)]);
        let selected = selector.select_replicas(&reps);
        assert_eq!(selected[0], node(2));
        assert_eq!(selected[1], node(3));
        assert_eq!(selected[2], node(1));
    }

    #[test]
    fn test_select_replicas_failed_last() {
        let selector = LatencyAwareSelector::new();
        for _ in 0..FAILURE_THRESHOLD {
            selector.record_failure(node(1));
        }
        let reps = replicas(&[(1, true), (2, false)]);
        let selected = selector.select_replicas(&reps);
        assert_eq!(selected[0], node(2));
        assert_eq!(selected[1], node(1));
    }

    #[test]
    fn test_record_success_recovers_suspect() {
        let selector = LatencyAwareSelector::new();
        selector.record_failure(node(1));
        assert_eq!(
            selector.get_stats(node(1)).unwrap().health,
            ReplicaHealth::Suspect
        );
        selector.record_success(node(1), 100);
        assert_eq!(
            selector.get_stats(node(1)).unwrap().health,
            ReplicaHealth::Healthy
        );
    }

    #[test]
    fn test_record_failure_escalates_to_failed() {
        let selector = LatencyAwareSelector::new();
        for i in 0..FAILURE_THRESHOLD {
            selector.record_failure(node(1));
            if i < FAILURE_THRESHOLD - 1 {
                assert_eq!(
                    selector.get_stats(node(1)).unwrap().health,
                    ReplicaHealth::Suspect
                );
            }
        }
        assert_eq!(
            selector.get_stats(node(1)).unwrap().health,
            ReplicaHealth::Failed
        );
    }

    #[test]
    fn test_mark_suspect() {
        let selector = LatencyAwareSelector::new();
        selector.record_success(node(1), 100);
        assert_eq!(
            selector.get_stats(node(1)).unwrap().health,
            ReplicaHealth::Healthy
        );
        selector.mark_suspect(node(1));
        assert_eq!(
            selector.get_stats(node(1)).unwrap().health,
            ReplicaHealth::Suspect
        );
    }

    #[test]
    fn test_get_stats_unknown_node() {
        let selector = LatencyAwareSelector::new();
        assert!(selector.get_stats(node(99)).is_none());
    }

    #[test]
    fn test_default() {
        let selector = LatencyAwareSelector::default();
        assert!(selector.get_stats(node(1)).is_none());
    }

    #[test]
    fn test_select_replicas_empty() {
        let selector = LatencyAwareSelector::new();
        let selected = selector.select_replicas(&[]);
        assert!(selected.is_empty());
    }

    #[test]
    fn test_select_replicas_suspect_between_healthy_and_failed() {
        let selector = LatencyAwareSelector::new();
        selector.mark_suspect(node(2));
        for _ in 0..FAILURE_THRESHOLD {
            selector.record_failure(node(3));
        }

        let reps = replicas(&[(1, false), (2, false), (3, false)]);
        let selected = selector.select_replicas(&reps);
        assert_eq!(selected[0], node(1));
        assert_eq!(selected[1], node(2));
        assert_eq!(selected[2], node(3));
    }

    #[test]
    fn test_latency_ewma() {
        let selector = LatencyAwareSelector::new();
        selector.record_success(node(1), 1000);
        let stats = selector.get_stats(node(1)).unwrap();
        assert_eq!(stats.latency_us, (DEFAULT_LATENCY_US + 1000) / 2);

        selector.record_success(node(1), 200);
        let stats = selector.get_stats(node(1)).unwrap();
        let prev = (DEFAULT_LATENCY_US + 1000) / 2;
        assert_eq!(stats.latency_us, (prev + 200) / 2);
    }

    #[test]
    fn test_select_replicas_local_over_lower_latency_remote() {
        let selector = LatencyAwareSelector::new();
        selector.record_success(node(1), 50);
        selector.record_success(node(2), 10000);

        let reps = replicas(&[(1, false), (2, true)]);
        let selected = selector.select_replicas(&reps);
        assert_eq!(selected[0], node(2));
    }
}
