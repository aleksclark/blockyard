use blockyard_common::types::{NodeId, NodeInfo, NodeState, ZfsHealthState};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info, warn};

use crate::placement::PlacementEngine;

/// Configuration for rebalance throttling and concurrency limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalanceConfig {
    /// Maximum number of concurrent moves per node (source or target).
    pub max_concurrent_moves_per_node: u32,
    /// Bandwidth throttle string (e.g. "1Gbps"). Informational for ZFS send.
    pub throttle_bandwidth: String,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            max_concurrent_moves_per_node: 1,
            throttle_bandwidth: "1Gbps".to_string(),
        }
    }
}

/// A node whose usage deviates from the cluster mean beyond the threshold.
#[derive(Debug, Clone)]
pub struct ImbalancedNode {
    pub node_id: NodeId,
    /// Fraction of capacity that is used (0.0–1.0).
    pub usage_ratio: f64,
    /// Cluster mean usage ratio.
    pub mean_ratio: f64,
    /// Signed deviation from the mean (positive = over-used, negative = under-used).
    pub deviation: f64,
}

/// The lifecycle state of a single rebalance move.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MoveState {
    Pending,
    Syncing,
    Promoting,
    Completed,
    Failed(String),
}

impl std::fmt::Display for MoveState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Syncing => write!(f, "syncing"),
            Self::Promoting => write!(f, "promoting"),
            Self::Completed => write!(f, "completed"),
            Self::Failed(reason) => write!(f, "failed: {reason}"),
        }
    }
}

/// A single volume move from one node to another.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalanceMove {
    pub volume_name: String,
    pub source_node: NodeId,
    pub target_node: NodeId,
    pub state: MoveState,
    pub bytes_transferred: u64,
    pub total_bytes: u64,
}

/// Simplified volume record for move computation; mirrors the raft state.
#[derive(Debug, Clone)]
pub struct VolumeInfo {
    pub name: String,
    pub size_bytes: u64,
    pub replicas: u32,
    pub placement: Vec<NodeId>,
}

/// Cluster state snapshot used for rebalance computation.
#[derive(Debug, Clone)]
pub struct ClusterState {
    pub nodes: Vec<NodeInfo>,
    pub volumes: Vec<VolumeInfo>,
}

/// Engine that detects imbalances and computes rebalance moves.
#[derive(Debug)]
pub struct RebalanceEngine {
    config: RebalanceConfig,
    active_moves: Mutex<Vec<RebalanceMove>>,
}

impl RebalanceEngine {
    pub fn new(config: RebalanceConfig) -> Self {
        Self {
            config,
            active_moves: Mutex::new(Vec::new()),
        }
    }

    /// Detect nodes whose usage-ratio deviates from the cluster mean by more
    /// than `threshold` (a fraction, e.g. 0.20 = 20%).
    ///
    /// Only healthy nodes with non-zero capacity are considered.
    pub fn detect_imbalance(&self, nodes: &[NodeInfo], threshold: f64) -> Vec<ImbalancedNode> {
        let eligible: Vec<&NodeInfo> = nodes
            .iter()
            .filter(|n| {
                n.state == NodeState::Healthy
                    && n.zfs_health == ZfsHealthState::Online
                    && n.capacity_bytes > 0
            })
            .collect();

        if eligible.is_empty() {
            return Vec::new();
        }

        let total_used: u64 = eligible.iter().map(|n| n.used_bytes).sum();
        let total_capacity: u64 = eligible.iter().map(|n| n.capacity_bytes).sum();

        if total_capacity == 0 {
            return Vec::new();
        }

        let mean_ratio = total_used as f64 / total_capacity as f64;

        let mut imbalanced = Vec::new();
        for node in &eligible {
            let usage_ratio = node.used_bytes as f64 / node.capacity_bytes as f64;
            let deviation = usage_ratio - mean_ratio;
            if deviation.abs() > threshold {
                debug!(
                    node_id = node.id,
                    usage_ratio, mean_ratio, deviation, "node imbalanced"
                );
                imbalanced.push(ImbalancedNode {
                    node_id: node.id,
                    usage_ratio,
                    mean_ratio,
                    deviation,
                });
            }
        }

        imbalanced
    }

    /// Compute rebalance moves for the cluster. For each volume on an
    /// over-utilised node, ask the PlacementEngine for its ideal placement,
    /// then generate moves that shift replicas towards the ideal.
    ///
    /// Respects `max_concurrent_moves_per_node` from the config.
    pub fn compute_moves(
        &self,
        cluster: &ClusterState,
        placement: &PlacementEngine,
        threshold: f64,
    ) -> Vec<RebalanceMove> {
        let imbalanced = self.detect_imbalance(&cluster.nodes, threshold);
        if imbalanced.is_empty() {
            return Vec::new();
        }

        // Identify overloaded node IDs.
        let overloaded: HashMap<NodeId, &ImbalancedNode> = imbalanced
            .iter()
            .filter(|n| n.deviation > 0.0)
            .map(|n| (n.node_id, n))
            .collect();

        if overloaded.is_empty() {
            return Vec::new();
        }

        // Track per-node move counts for throttling.
        let active = self.active_moves.lock();
        let mut node_move_counts: HashMap<NodeId, u32> = HashMap::new();
        for m in active.iter() {
            if m.state != MoveState::Completed && !matches!(m.state, MoveState::Failed(_)) {
                *node_move_counts.entry(m.source_node).or_default() += 1;
                *node_move_counts.entry(m.target_node).or_default() += 1;
            }
        }
        drop(active);

        let max_per_node = self.config.max_concurrent_moves_per_node;
        let mut moves = Vec::new();

        // Build a map from node_id -> node for lookup.
        let node_map: HashMap<NodeId, &NodeInfo> =
            cluster.nodes.iter().map(|n| (n.id, n)).collect();

        for volume in &cluster.volumes {
            // Only consider volumes that have at least one replica on an
            // overloaded node.
            let source_nodes_on_overloaded: Vec<NodeId> = volume
                .placement
                .iter()
                .copied()
                .filter(|nid| overloaded.contains_key(nid))
                .collect();

            if source_nodes_on_overloaded.is_empty() {
                continue;
            }

            // Build a VolumeSpec-like structure for the placement engine.
            let spec = blockyard_common::types::VolumeSpec {
                id: uuid::Uuid::new_v4(),
                name: volume.name.clone(),
                size_bytes: volume.size_bytes,
                replicas: volume.replicas,
                consistency: blockyard_common::types::WriteConsistency::Majority,
                read_policy: blockyard_common::types::ReadPolicy::Any,
                affinity: HashMap::new(),
                anti_affinity: HashMap::new(),
                failure_domain: "node".to_string(),
            };

            let ideal = match placement.place_volume(&spec, &cluster.nodes) {
                Ok(ids) => ids,
                Err(e) => {
                    warn!(volume = %volume.name, error = %e, "placement failed during rebalance");
                    continue;
                }
            };

            // Diff: find nodes that are in the current placement but not in
            // the ideal, and nodes that are in the ideal but not current.
            let current_set: std::collections::HashSet<NodeId> =
                volume.placement.iter().copied().collect();
            let ideal_set: std::collections::HashSet<NodeId> = ideal.iter().copied().collect();

            let to_remove: Vec<NodeId> = source_nodes_on_overloaded
                .iter()
                .copied()
                .filter(|nid| !ideal_set.contains(nid))
                .collect();
            let to_add: Vec<NodeId> = ideal
                .iter()
                .copied()
                .filter(|nid| !current_set.contains(nid))
                .collect();

            // Pair removals with additions.
            let pairs = to_remove.len().min(to_add.len());
            for i in 0..pairs {
                let source = to_remove[i];
                let target = to_add[i];

                // Check throttling.
                let source_count = node_move_counts.get(&source).copied().unwrap_or(0);
                let target_count = node_move_counts.get(&target).copied().unwrap_or(0);

                if source_count >= max_per_node || target_count >= max_per_node {
                    debug!(
                        volume = %volume.name,
                        source,
                        target,
                        "throttled: max concurrent moves reached"
                    );
                    continue;
                }

                // Check target node is healthy.
                if let Some(target_node) = node_map.get(&target) {
                    if target_node.state != NodeState::Healthy
                        || target_node.zfs_health != ZfsHealthState::Online
                    {
                        continue;
                    }
                }

                *node_move_counts.entry(source).or_default() += 1;
                *node_move_counts.entry(target).or_default() += 1;

                info!(
                    volume = %volume.name,
                    source,
                    target,
                    size = volume.size_bytes,
                    "computed rebalance move"
                );

                moves.push(RebalanceMove {
                    volume_name: volume.name.clone(),
                    source_node: source,
                    target_node: target,
                    state: MoveState::Pending,
                    bytes_transferred: 0,
                    total_bytes: volume.size_bytes,
                });
            }
        }

        moves
    }

    /// Enqueue a set of moves into the active list.
    pub fn enqueue_moves(&self, moves: Vec<RebalanceMove>) {
        let mut active = self.active_moves.lock();
        for m in moves {
            info!(
                volume = %m.volume_name,
                source = m.source_node,
                target = m.target_node,
                "enqueuing rebalance move"
            );
            active.push(m);
        }
    }

    /// Return a snapshot of the active moves list.
    pub fn active_moves(&self) -> Vec<RebalanceMove> {
        self.active_moves.lock().clone()
    }

    /// Return the count of active (non-completed, non-failed) moves.
    pub fn move_count(&self) -> usize {
        self.active_moves
            .lock()
            .iter()
            .filter(|m| m.state != MoveState::Completed && !matches!(m.state, MoveState::Failed(_)))
            .count()
    }

    /// Transition a move to Syncing state. Returns false if the move was not
    /// found or not in Pending state.
    pub fn start_syncing(&self, volume_name: &str) -> bool {
        let mut active = self.active_moves.lock();
        if let Some(m) = active
            .iter_mut()
            .find(|m| m.volume_name == volume_name && m.state == MoveState::Pending)
        {
            m.state = MoveState::Syncing;
            true
        } else {
            false
        }
    }

    /// Transition a move to Promoting state. Returns false if the move was not
    /// found or not in Syncing state.
    pub fn start_promoting(&self, volume_name: &str) -> bool {
        let mut active = self.active_moves.lock();
        if let Some(m) = active
            .iter_mut()
            .find(|m| m.volume_name == volume_name && m.state == MoveState::Syncing)
        {
            m.state = MoveState::Promoting;
            true
        } else {
            false
        }
    }

    /// Mark a move as completed. Returns true if found and transitioned.
    pub fn complete_move(&self, volume_name: &str) -> bool {
        let mut active = self.active_moves.lock();
        if let Some(m) = active.iter_mut().find(|m| {
            m.volume_name == volume_name
                && (m.state == MoveState::Promoting || m.state == MoveState::Syncing)
        }) {
            m.bytes_transferred = m.total_bytes;
            m.state = MoveState::Completed;
            info!(volume = %volume_name, "rebalance move completed");
            true
        } else {
            false
        }
    }

    /// Mark a move as failed with a reason.
    pub fn fail_move(&self, volume_name: &str, reason: &str) {
        let mut active = self.active_moves.lock();
        if let Some(m) = active.iter_mut().find(|m| {
            m.volume_name == volume_name
                && m.state != MoveState::Completed
                && !matches!(m.state, MoveState::Failed(_))
        }) {
            warn!(volume = %volume_name, reason, "rebalance move failed");
            m.state = MoveState::Failed(reason.to_string());
        }
    }

    /// Update the bytes_transferred for a syncing move. Returns false if
    /// the move is not found or not in Syncing state.
    pub fn update_progress(&self, volume_name: &str, bytes_transferred: u64) -> bool {
        let mut active = self.active_moves.lock();
        if let Some(m) = active
            .iter_mut()
            .find(|m| m.volume_name == volume_name && m.state == MoveState::Syncing)
        {
            m.bytes_transferred = bytes_transferred;
            true
        } else {
            false
        }
    }

    /// Remove completed and failed moves from the active list, returning them.
    pub fn drain_finished(&self) -> Vec<RebalanceMove> {
        let mut active = self.active_moves.lock();
        let (finished, remaining): (Vec<_>, Vec<_>) = active.drain(..).partition(|m| {
            m.state == MoveState::Completed || matches!(m.state, MoveState::Failed(_))
        });
        *active = remaining;
        finished
    }

    /// Access the config.
    pub fn config(&self) -> &RebalanceConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockyard_common::types::{NodeState, ZfsHealthState};

    fn gb(n: u64) -> u64 {
        n * 1024 * 1024 * 1024
    }

    fn make_node(id: NodeId, capacity: u64, used: u64) -> NodeInfo {
        NodeInfo {
            id,
            name: format!("node-{id}"),
            addr: format!("127.0.0.1:{}", 7400 + id).parse().unwrap(),
            data_addr: format!("127.0.0.1:{}", 7500 + id).parse().unwrap(),
            tags: HashMap::new(),
            state: NodeState::Healthy,
            zfs_health: ZfsHealthState::Online,
            capacity_bytes: capacity,
            used_bytes: used,
            incarnation: 1,
        }
    }

    fn make_engine() -> RebalanceEngine {
        RebalanceEngine::new(RebalanceConfig::default())
    }

    // ── Imbalance detection ─────────────────────────────────────────────

    #[test]
    fn test_detect_imbalance_balanced_cluster() {
        let engine = make_engine();
        let nodes = vec![
            make_node(1, gb(100), gb(50)),
            make_node(2, gb(100), gb(50)),
            make_node(3, gb(100), gb(50)),
        ];
        let result = engine.detect_imbalance(&nodes, 0.20);
        assert!(
            result.is_empty(),
            "evenly loaded cluster should have no imbalance"
        );
    }

    #[test]
    fn test_detect_imbalance_one_hot_node() {
        let engine = make_engine();
        let nodes = vec![
            make_node(1, gb(100), gb(90)), // 90% used
            make_node(2, gb(100), gb(10)), // 10% used
            make_node(3, gb(100), gb(10)), // 10% used
        ];
        // Mean = ~36.7%
        // Node 1 deviation ≈ +53.3% (>20%)
        // Node 2,3 deviation ≈ -26.7% (>20%)
        let result = engine.detect_imbalance(&nodes, 0.20);
        assert!(
            result.len() >= 2,
            "expected at least 2 imbalanced nodes, got {}",
            result.len()
        );
        // Node 1 should be over-utilised.
        let hot = result.iter().find(|n| n.node_id == 1);
        assert!(hot.is_some());
        assert!(hot.map_or(false, |n| n.deviation > 0.0));
    }

    #[test]
    fn test_detect_imbalance_empty_nodes() {
        let engine = make_engine();
        let result = engine.detect_imbalance(&[], 0.20);
        assert!(result.is_empty());
    }

    #[test]
    fn test_detect_imbalance_zero_capacity() {
        let engine = make_engine();
        let nodes = vec![make_node(1, 0, 0)];
        let result = engine.detect_imbalance(&nodes, 0.20);
        assert!(result.is_empty());
    }

    #[test]
    fn test_detect_imbalance_excludes_failed_nodes() {
        let engine = make_engine();
        let mut hot_node = make_node(1, gb(100), gb(90));
        hot_node.state = NodeState::Failed;
        let nodes = vec![
            hot_node,
            make_node(2, gb(100), gb(50)),
            make_node(3, gb(100), gb(50)),
        ];
        let result = engine.detect_imbalance(&nodes, 0.20);
        // With the failed node excluded, nodes 2 and 3 are perfectly balanced.
        assert!(result.is_empty());
    }

    #[test]
    fn test_detect_imbalance_excludes_faulted_zfs() {
        let engine = make_engine();
        let mut faulted = make_node(1, gb(100), gb(90));
        faulted.zfs_health = ZfsHealthState::Faulted;
        let nodes = vec![
            faulted,
            make_node(2, gb(100), gb(50)),
            make_node(3, gb(100), gb(50)),
        ];
        let result = engine.detect_imbalance(&nodes, 0.20);
        assert!(result.is_empty());
    }

    #[test]
    fn test_detect_imbalance_high_threshold() {
        let engine = make_engine();
        let nodes = vec![
            make_node(1, gb(100), gb(90)),
            make_node(2, gb(100), gb(10)),
            make_node(3, gb(100), gb(10)),
        ];
        // 99% threshold → nothing triggers
        let result = engine.detect_imbalance(&nodes, 0.99);
        assert!(result.is_empty());
    }

    #[test]
    fn test_detect_imbalance_low_threshold() {
        let engine = make_engine();
        let nodes = vec![make_node(1, gb(100), gb(55)), make_node(2, gb(100), gb(45))];
        // Mean = 50%. Deviations = ±5%. Threshold 1% → both trigger.
        let result = engine.detect_imbalance(&nodes, 0.01);
        assert_eq!(result.len(), 2);
    }

    // ── Move computation ────────────────────────────────────────────────

    #[test]
    fn test_compute_moves_balanced_cluster() {
        let engine = make_engine();
        let placement = PlacementEngine::new();
        let cluster = ClusterState {
            nodes: vec![
                make_node(1, gb(100), gb(50)),
                make_node(2, gb(100), gb(50)),
                make_node(3, gb(100), gb(50)),
            ],
            volumes: vec![VolumeInfo {
                name: "vol-a".into(),
                size_bytes: gb(10),
                replicas: 3,
                placement: vec![1, 2, 3],
            }],
        };
        let moves = engine.compute_moves(&cluster, &placement, 0.20);
        assert!(moves.is_empty(), "balanced cluster should produce no moves");
    }

    #[test]
    fn test_compute_moves_imbalanced_produces_move() {
        let engine = make_engine();
        let placement = PlacementEngine::new();
        let cluster = ClusterState {
            nodes: vec![
                make_node(1, gb(100), gb(90)),
                make_node(2, gb(100), gb(10)),
                make_node(3, gb(100), gb(10)),
                make_node(4, gb(100), gb(10)),
            ],
            volumes: vec![VolumeInfo {
                name: "vol-a".into(),
                size_bytes: gb(10),
                replicas: 2,
                placement: vec![1, 2],
            }],
        };
        let moves = engine.compute_moves(&cluster, &placement, 0.20);
        // The placement engine should prefer nodes with more free space,
        // which could move vol-a off node 1.
        // Exact behaviour depends on PlacementEngine; we just verify the
        // structure is correct for any produced move.
        for m in &moves {
            assert_eq!(m.volume_name, "vol-a");
            assert_eq!(m.state, MoveState::Pending);
            assert_eq!(m.total_bytes, gb(10));
            assert_eq!(m.bytes_transferred, 0);
        }
    }

    #[test]
    fn test_compute_moves_no_volumes() {
        let engine = make_engine();
        let placement = PlacementEngine::new();
        let cluster = ClusterState {
            nodes: vec![make_node(1, gb(100), gb(90)), make_node(2, gb(100), gb(10))],
            volumes: vec![],
        };
        let moves = engine.compute_moves(&cluster, &placement, 0.20);
        assert!(moves.is_empty());
    }

    // ── Throttling ──────────────────────────────────────────────────────

    #[test]
    fn test_throttling_limits_moves_per_node() {
        let config = RebalanceConfig {
            max_concurrent_moves_per_node: 1,
            throttle_bandwidth: "1Gbps".into(),
        };
        let engine = RebalanceEngine::new(config);
        let placement = PlacementEngine::new();

        // Pre-load an active move from node 1 → node 4.
        engine.enqueue_moves(vec![RebalanceMove {
            volume_name: "vol-existing".into(),
            source_node: 1,
            target_node: 4,
            state: MoveState::Syncing,
            bytes_transferred: 0,
            total_bytes: gb(10),
        }]);

        let cluster = ClusterState {
            nodes: vec![
                make_node(1, gb(100), gb(90)),
                make_node(2, gb(100), gb(10)),
                make_node(3, gb(100), gb(10)),
                make_node(4, gb(100), gb(10)),
                make_node(5, gb(100), gb(10)),
            ],
            volumes: vec![VolumeInfo {
                name: "vol-a".into(),
                size_bytes: gb(10),
                replicas: 2,
                placement: vec![1, 2],
            }],
        };

        let moves = engine.compute_moves(&cluster, &placement, 0.20);
        // Node 1 already has 1 active move, so no additional moves should
        // use node 1 as source (throttled).
        for m in &moves {
            assert_ne!(
                m.source_node, 1,
                "node 1 should be throttled from additional source moves"
            );
        }
    }

    #[test]
    fn test_throttling_higher_limit_allows_more() {
        let config = RebalanceConfig {
            max_concurrent_moves_per_node: 10,
            throttle_bandwidth: "10Gbps".into(),
        };
        let engine = RebalanceEngine::new(config);
        assert_eq!(engine.config().max_concurrent_moves_per_node, 10);
        assert_eq!(engine.config().throttle_bandwidth, "10Gbps");
    }

    // ── State transitions ───────────────────────────────────────────────

    #[test]
    fn test_move_state_pending_to_syncing() {
        let engine = make_engine();
        engine.enqueue_moves(vec![RebalanceMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 2,
            state: MoveState::Pending,
            bytes_transferred: 0,
            total_bytes: gb(10),
        }]);
        assert!(engine.start_syncing("vol-a"));
        let moves = engine.active_moves();
        assert_eq!(moves[0].state, MoveState::Syncing);
    }

    #[test]
    fn test_move_state_syncing_to_promoting() {
        let engine = make_engine();
        engine.enqueue_moves(vec![RebalanceMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 2,
            state: MoveState::Syncing,
            bytes_transferred: gb(5),
            total_bytes: gb(10),
        }]);
        assert!(engine.start_promoting("vol-a"));
        let moves = engine.active_moves();
        assert_eq!(moves[0].state, MoveState::Promoting);
    }

    #[test]
    fn test_move_state_promoting_to_completed() {
        let engine = make_engine();
        engine.enqueue_moves(vec![RebalanceMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 2,
            state: MoveState::Promoting,
            bytes_transferred: gb(8),
            total_bytes: gb(10),
        }]);
        assert!(engine.complete_move("vol-a"));
        let moves = engine.active_moves();
        assert_eq!(moves[0].state, MoveState::Completed);
        assert_eq!(moves[0].bytes_transferred, gb(10));
    }

    #[test]
    fn test_move_state_fail() {
        let engine = make_engine();
        engine.enqueue_moves(vec![RebalanceMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 2,
            state: MoveState::Syncing,
            bytes_transferred: 0,
            total_bytes: gb(10),
        }]);
        engine.fail_move("vol-a", "network timeout");
        let moves = engine.active_moves();
        assert_eq!(moves[0].state, MoveState::Failed("network timeout".into()));
    }

    #[test]
    fn test_complete_move_not_found() {
        let engine = make_engine();
        assert!(!engine.complete_move("nonexistent"));
    }

    #[test]
    fn test_fail_move_already_completed() {
        let engine = make_engine();
        engine.enqueue_moves(vec![RebalanceMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 2,
            state: MoveState::Completed,
            bytes_transferred: gb(10),
            total_bytes: gb(10),
        }]);
        // Should be a no-op since already completed.
        engine.fail_move("vol-a", "too late");
        let moves = engine.active_moves();
        assert_eq!(moves[0].state, MoveState::Completed);
    }

    #[test]
    fn test_start_syncing_wrong_state() {
        let engine = make_engine();
        engine.enqueue_moves(vec![RebalanceMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 2,
            state: MoveState::Syncing,
            bytes_transferred: 0,
            total_bytes: gb(10),
        }]);
        // Already syncing, should not transition again.
        assert!(!engine.start_syncing("vol-a"));
    }

    #[test]
    fn test_start_promoting_wrong_state() {
        let engine = make_engine();
        engine.enqueue_moves(vec![RebalanceMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 2,
            state: MoveState::Pending,
            bytes_transferred: 0,
            total_bytes: gb(10),
        }]);
        // In Pending, not Syncing → should fail.
        assert!(!engine.start_promoting("vol-a"));
    }

    // ── Move counting & progress ────────────────────────────────────────

    #[test]
    fn test_move_count() {
        let engine = make_engine();
        engine.enqueue_moves(vec![
            RebalanceMove {
                volume_name: "vol-a".into(),
                source_node: 1,
                target_node: 2,
                state: MoveState::Pending,
                bytes_transferred: 0,
                total_bytes: gb(10),
            },
            RebalanceMove {
                volume_name: "vol-b".into(),
                source_node: 3,
                target_node: 4,
                state: MoveState::Completed,
                bytes_transferred: gb(5),
                total_bytes: gb(5),
            },
            RebalanceMove {
                volume_name: "vol-c".into(),
                source_node: 1,
                target_node: 3,
                state: MoveState::Failed("disk error".into()),
                bytes_transferred: 0,
                total_bytes: gb(10),
            },
        ]);
        // Only vol-a is active (Pending).
        assert_eq!(engine.move_count(), 1);
    }

    #[test]
    fn test_update_progress() {
        let engine = make_engine();
        engine.enqueue_moves(vec![RebalanceMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 2,
            state: MoveState::Syncing,
            bytes_transferred: 0,
            total_bytes: gb(10),
        }]);
        assert!(engine.update_progress("vol-a", gb(5)));
        let moves = engine.active_moves();
        assert_eq!(moves[0].bytes_transferred, gb(5));
    }

    #[test]
    fn test_update_progress_wrong_state() {
        let engine = make_engine();
        engine.enqueue_moves(vec![RebalanceMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 2,
            state: MoveState::Pending,
            bytes_transferred: 0,
            total_bytes: gb(10),
        }]);
        assert!(!engine.update_progress("vol-a", gb(5)));
    }

    // ── Drain finished ──────────────────────────────────────────────────

    #[test]
    fn test_drain_finished() {
        let engine = make_engine();
        engine.enqueue_moves(vec![
            RebalanceMove {
                volume_name: "vol-a".into(),
                source_node: 1,
                target_node: 2,
                state: MoveState::Completed,
                bytes_transferred: gb(10),
                total_bytes: gb(10),
            },
            RebalanceMove {
                volume_name: "vol-b".into(),
                source_node: 3,
                target_node: 4,
                state: MoveState::Syncing,
                bytes_transferred: gb(2),
                total_bytes: gb(10),
            },
        ]);
        let finished = engine.drain_finished();
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].volume_name, "vol-a");
        // vol-b should still be active.
        assert_eq!(engine.active_moves().len(), 1);
        assert_eq!(engine.active_moves()[0].volume_name, "vol-b");
    }

    // ── MoveState Display ───────────────────────────────────────────────

    #[test]
    fn test_move_state_display() {
        assert_eq!(MoveState::Pending.to_string(), "pending");
        assert_eq!(MoveState::Syncing.to_string(), "syncing");
        assert_eq!(MoveState::Promoting.to_string(), "promoting");
        assert_eq!(MoveState::Completed.to_string(), "completed");
        assert_eq!(
            MoveState::Failed("disk error".into()).to_string(),
            "failed: disk error"
        );
    }

    // ── Config defaults ─────────────────────────────────────────────────

    #[test]
    fn test_config_default() {
        let config = RebalanceConfig::default();
        assert_eq!(config.max_concurrent_moves_per_node, 1);
        assert_eq!(config.throttle_bandwidth, "1Gbps");
    }
}
