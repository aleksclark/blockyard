use blockyard_common::types::{NodeId, NodeInfo, NodeState};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::placement::PlacementEngine;

/// Simplified volume record for drain move computation.
#[derive(Debug, Clone)]
pub struct VolumeInfo {
    pub name: String,
    pub size_bytes: u64,
    pub replicas: u32,
    pub placement: Vec<NodeId>,
}

/// The lifecycle state of a single drain move.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DrainMoveState {
    Pending,
    Migrating,
    Completed,
    Failed(String),
}

impl std::fmt::Display for DrainMoveState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Migrating => write!(f, "migrating"),
            Self::Completed => write!(f, "completed"),
            Self::Failed(reason) => write!(f, "failed: {reason}"),
        }
    }
}

/// A single volume move generated during a node drain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrainMove {
    /// Name of the volume being moved.
    pub volume_name: String,
    /// Node being drained (source).
    pub source_node: NodeId,
    /// Replacement node chosen by the placement engine.
    pub target_node: NodeId,
    /// Current lifecycle state of this move.
    pub state: DrainMoveState,
    /// Bytes transferred so far.
    pub bytes_transferred: u64,
    /// Total bytes to transfer.
    pub total_bytes: u64,
}

/// Summary of overall drain progress for a node.
#[derive(Debug, Clone)]
pub struct DrainProgress {
    /// The node being drained.
    pub node_id: NodeId,
    /// Total number of volumes to move.
    pub total_moves: usize,
    /// Number of completed moves.
    pub completed: usize,
    /// Number of failed moves.
    pub failed: usize,
    /// Number of in-progress moves (pending + migrating).
    pub in_progress: usize,
    /// Whether the drain is fully complete.
    pub is_complete: bool,
}

/// Engine that manages draining a single node.
///
/// Given a draining node, it discovers all volumes placed on that node,
/// computes replacement nodes using `PlacementEngine`, and tracks the
/// migration lifecycle of each volume.
#[derive(Debug)]
pub struct DrainEngine {
    node_id: NodeId,
    moves: Mutex<Vec<DrainMove>>,
}

impl DrainEngine {
    /// Create a new drain engine targeting `node_id`.
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            moves: Mutex::new(Vec::new()),
        }
    }

    /// Return the node being drained.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Discover all volumes that have a replica on the draining node.
    pub fn volumes_on_node(node_id: NodeId, volumes: &[VolumeInfo]) -> Vec<VolumeInfo> {
        volumes
            .iter()
            .filter(|v| v.placement.contains(&node_id))
            .cloned()
            .collect()
    }

    /// Compute drain moves: for each volume on the draining node, use the
    /// placement engine to find a replacement among eligible nodes.
    ///
    /// Only healthy nodes that are not already hosting the volume and are not
    /// the draining node are considered as targets.
    pub fn compute_moves(
        &self,
        volumes: &[VolumeInfo],
        nodes: &[NodeInfo],
        placement: &PlacementEngine,
    ) -> Vec<DrainMove> {
        let on_node = Self::volumes_on_node(self.node_id, volumes);

        if on_node.is_empty() {
            debug!(node_id = self.node_id, "no volumes on draining node");
            return Vec::new();
        }

        let mut drain_moves = Vec::new();

        for volume in &on_node {
            // Build eligible candidates: healthy, not draining, not already in placement.
            let candidates: Vec<NodeInfo> = nodes
                .iter()
                .filter(|n| {
                    n.id != self.node_id
                        && n.state == NodeState::Healthy
                        && !volume.placement.contains(&n.id)
                })
                .cloned()
                .collect();

            if candidates.is_empty() {
                warn!(
                    volume = %volume.name,
                    node_id = self.node_id,
                    "no eligible replacement node for drain"
                );
                drain_moves.push(DrainMove {
                    volume_name: volume.name.clone(),
                    source_node: self.node_id,
                    target_node: 0,
                    state: DrainMoveState::Failed("no eligible replacement node".to_string()),
                    bytes_transferred: 0,
                    total_bytes: volume.size_bytes,
                });
                continue;
            }

            let spec = blockyard_common::types::VolumeSpec {
                id: uuid::Uuid::new_v4(),
                name: volume.name.clone(),
                size_bytes: volume.size_bytes,
                replicas: 1, // We need exactly 1 replacement.
                consistency: blockyard_common::types::WriteConsistency::Majority,
                read_policy: blockyard_common::types::ReadPolicy::Any,
                affinity: std::collections::HashMap::new(),
                anti_affinity: std::collections::HashMap::new(),
                failure_domain: "node".to_string(),
            };

            match placement.place_volume(&spec, &candidates) {
                Ok(selected) if !selected.is_empty() => {
                    let target = selected[0];
                    info!(
                        volume = %volume.name,
                        source = self.node_id,
                        target,
                        "computed drain move"
                    );
                    drain_moves.push(DrainMove {
                        volume_name: volume.name.clone(),
                        source_node: self.node_id,
                        target_node: target,
                        state: DrainMoveState::Pending,
                        bytes_transferred: 0,
                        total_bytes: volume.size_bytes,
                    });
                }
                Ok(_) => {
                    warn!(
                        volume = %volume.name,
                        "placement returned empty set for drain"
                    );
                    drain_moves.push(DrainMove {
                        volume_name: volume.name.clone(),
                        source_node: self.node_id,
                        target_node: 0,
                        state: DrainMoveState::Failed("placement returned empty set".to_string()),
                        bytes_transferred: 0,
                        total_bytes: volume.size_bytes,
                    });
                }
                Err(e) => {
                    warn!(
                        volume = %volume.name,
                        error = %e,
                        "placement failed during drain"
                    );
                    drain_moves.push(DrainMove {
                        volume_name: volume.name.clone(),
                        source_node: self.node_id,
                        target_node: 0,
                        state: DrainMoveState::Failed(format!("placement error: {e}")),
                        bytes_transferred: 0,
                        total_bytes: volume.size_bytes,
                    });
                }
            }
        }

        drain_moves
    }

    /// Enqueue computed moves into the engine for tracking.
    pub fn enqueue_moves(&self, moves: Vec<DrainMove>) {
        let mut active = self.moves.lock();
        for m in moves {
            info!(
                volume = %m.volume_name,
                source = m.source_node,
                target = m.target_node,
                "enqueuing drain move"
            );
            active.push(m);
        }
    }

    /// Return a snapshot of the current moves list.
    pub fn moves(&self) -> Vec<DrainMove> {
        self.moves.lock().clone()
    }

    /// Transition a move from Pending to Migrating. Returns false if not found
    /// or not in Pending state.
    pub fn start_migrating(&self, volume_name: &str) -> bool {
        let mut moves = self.moves.lock();
        if let Some(m) = moves
            .iter_mut()
            .find(|m| m.volume_name == volume_name && m.state == DrainMoveState::Pending)
        {
            m.state = DrainMoveState::Migrating;
            true
        } else {
            false
        }
    }

    /// Mark a move as completed. Returns true if the transition happened.
    pub fn complete_move(&self, volume_name: &str) -> bool {
        let mut moves = self.moves.lock();
        if let Some(m) = moves
            .iter_mut()
            .find(|m| m.volume_name == volume_name && m.state == DrainMoveState::Migrating)
        {
            m.bytes_transferred = m.total_bytes;
            m.state = DrainMoveState::Completed;
            info!(volume = %volume_name, "drain move completed");
            true
        } else {
            false
        }
    }

    /// Mark a move as failed with a reason.
    pub fn fail_move(&self, volume_name: &str, reason: &str) {
        let mut moves = self.moves.lock();
        if let Some(m) = moves.iter_mut().find(|m| {
            m.volume_name == volume_name
                && m.state != DrainMoveState::Completed
                && !matches!(m.state, DrainMoveState::Failed(_))
        }) {
            warn!(volume = %volume_name, reason, "drain move failed");
            m.state = DrainMoveState::Failed(reason.to_string());
        }
    }

    /// Update bytes transferred for a migrating move. Returns false if not
    /// found or not in Migrating state.
    pub fn update_progress(&self, volume_name: &str, bytes_transferred: u64) -> bool {
        let mut moves = self.moves.lock();
        if let Some(m) = moves
            .iter_mut()
            .find(|m| m.volume_name == volume_name && m.state == DrainMoveState::Migrating)
        {
            m.bytes_transferred = bytes_transferred;
            true
        } else {
            false
        }
    }

    /// Compute a summary of drain progress.
    pub fn progress(&self) -> DrainProgress {
        let moves = self.moves.lock();
        let total_moves = moves.len();
        let completed = moves
            .iter()
            .filter(|m| m.state == DrainMoveState::Completed)
            .count();
        let failed = moves
            .iter()
            .filter(|m| matches!(m.state, DrainMoveState::Failed(_)))
            .count();
        let in_progress = total_moves - completed - failed;
        let is_complete = total_moves > 0 && in_progress == 0;

        DrainProgress {
            node_id: self.node_id,
            total_moves,
            completed,
            failed,
            in_progress,
            is_complete,
        }
    }

    /// Check whether all drain moves are finished (completed or failed).
    pub fn is_drain_complete(&self) -> bool {
        let moves = self.moves.lock();
        if moves.is_empty() {
            return true;
        }
        moves.iter().all(|m| {
            m.state == DrainMoveState::Completed || matches!(m.state, DrainMoveState::Failed(_))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn gb(n: u64) -> u64 {
        n * 1024 * 1024 * 1024
    }

    fn make_node(id: NodeId) -> NodeInfo {
        NodeInfo {
            id,
            name: format!("node-{id}"),
            addr: format!("127.0.0.1:{}", 7400 + id).parse().unwrap(),
            data_addr: format!("127.0.0.1:{}", 7500 + id).parse().unwrap(),
            tags: HashMap::new(),
            state: NodeState::Healthy,
        }
    }

    fn make_volume(name: &str, size: u64, placement: Vec<NodeId>) -> VolumeInfo {
        VolumeInfo {
            name: name.to_string(),
            size_bytes: size,
            replicas: placement.len() as u32,
            placement,
        }
    }

    // ── volumes_on_node ─────────────────────────────────────────────────

    #[test]
    fn test_volumes_on_node_found() {
        let volumes = vec![
            make_volume("vol-a", gb(10), vec![1, 2, 3]),
            make_volume("vol-b", gb(5), vec![2, 3, 4]),
            make_volume("vol-c", gb(20), vec![1, 4, 5]),
        ];
        let result = DrainEngine::volumes_on_node(1, &volumes);
        assert_eq!(result.len(), 2);
        let names: Vec<&str> = result.iter().map(|v| v.name.as_str()).collect();
        assert!(names.contains(&"vol-a"));
        assert!(names.contains(&"vol-c"));
    }

    #[test]
    fn test_volumes_on_node_none() {
        let volumes = vec![
            make_volume("vol-a", gb(10), vec![2, 3, 4]),
            make_volume("vol-b", gb(5), vec![3, 4, 5]),
        ];
        let result = DrainEngine::volumes_on_node(1, &volumes);
        assert!(result.is_empty());
    }

    #[test]
    fn test_volumes_on_node_empty_volumes() {
        let result = DrainEngine::volumes_on_node(1, &[]);
        assert!(result.is_empty());
    }

    // ── compute_moves ───────────────────────────────────────────────────

    #[test]
    fn test_compute_moves_basic() {
        let engine = DrainEngine::new(1);
        let placement = PlacementEngine::new();
        let volumes = vec![make_volume("vol-a", gb(10), vec![1, 2, 3])];
        let nodes = vec![make_node(1), make_node(2), make_node(3), make_node(4)];
        let moves = engine.compute_moves(&volumes, &nodes, &placement);
        assert_eq!(moves.len(), 1);
        assert_eq!(moves[0].volume_name, "vol-a");
        assert_eq!(moves[0].source_node, 1);
        // Target should be node 4 (only node not in current placement).
        assert_eq!(moves[0].target_node, 4);
        assert_eq!(moves[0].state, DrainMoveState::Pending);
        assert_eq!(moves[0].total_bytes, gb(10));
        assert_eq!(moves[0].bytes_transferred, 0);
    }

    #[test]
    fn test_compute_moves_no_volumes() {
        let engine = DrainEngine::new(1);
        let placement = PlacementEngine::new();
        let volumes: Vec<VolumeInfo> = vec![];
        let nodes = vec![make_node(1), make_node(2)];
        let moves = engine.compute_moves(&volumes, &nodes, &placement);
        assert!(moves.is_empty());
    }

    #[test]
    fn test_compute_moves_no_eligible_replacement() {
        let engine = DrainEngine::new(1);
        let placement = PlacementEngine::new();
        // Volume on all nodes; no spare node for replacement.
        let volumes = vec![make_volume("vol-a", gb(10), vec![1, 2, 3])];
        let nodes = vec![make_node(1), make_node(2), make_node(3)];
        let moves = engine.compute_moves(&volumes, &nodes, &placement);
        assert_eq!(moves.len(), 1);
        assert!(matches!(moves[0].state, DrainMoveState::Failed(_)));
    }

    #[test]
    fn test_compute_moves_excludes_failed_nodes() {
        let engine = DrainEngine::new(1);
        let placement = PlacementEngine::new();
        let volumes = vec![make_volume("vol-a", gb(10), vec![1, 2])];
        let mut failed_node = make_node(3);
        failed_node.state = NodeState::Failed;
        let nodes = vec![make_node(1), make_node(2), failed_node, make_node(4)];
        let moves = engine.compute_moves(&volumes, &nodes, &placement);
        assert_eq!(moves.len(), 1);
        // Should pick node 4, not node 3 (failed).
        assert_eq!(moves[0].target_node, 4);
        assert_eq!(moves[0].state, DrainMoveState::Pending);
    }

    #[test]
    fn test_compute_moves_multiple_volumes() {
        let engine = DrainEngine::new(1);
        let placement = PlacementEngine::new();
        let volumes = vec![
            make_volume("vol-a", gb(10), vec![1, 2]),
            make_volume("vol-b", gb(5), vec![1, 3]),
        ];
        let nodes = vec![
            make_node(1),
            make_node(2),
            make_node(3),
            make_node(4),
            make_node(5),
        ];
        let moves = engine.compute_moves(&volumes, &nodes, &placement);
        assert_eq!(moves.len(), 2);
        let names: Vec<&str> = moves.iter().map(|m| m.volume_name.as_str()).collect();
        assert!(names.contains(&"vol-a"));
        assert!(names.contains(&"vol-b"));
    }

    // ── State transitions ───────────────────────────────────────────────

    #[test]
    fn test_enqueue_and_list_moves() {
        let engine = DrainEngine::new(1);
        let moves = vec![DrainMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 4,
            state: DrainMoveState::Pending,
            bytes_transferred: 0,
            total_bytes: gb(10),
        }];
        engine.enqueue_moves(moves);
        assert_eq!(engine.moves().len(), 1);
    }

    #[test]
    fn test_start_migrating() {
        let engine = DrainEngine::new(1);
        engine.enqueue_moves(vec![DrainMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 4,
            state: DrainMoveState::Pending,
            bytes_transferred: 0,
            total_bytes: gb(10),
        }]);
        assert!(engine.start_migrating("vol-a"));
        assert_eq!(engine.moves()[0].state, DrainMoveState::Migrating);
    }

    #[test]
    fn test_start_migrating_wrong_state() {
        let engine = DrainEngine::new(1);
        engine.enqueue_moves(vec![DrainMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 4,
            state: DrainMoveState::Migrating,
            bytes_transferred: 0,
            total_bytes: gb(10),
        }]);
        // Already migrating, should return false.
        assert!(!engine.start_migrating("vol-a"));
    }

    #[test]
    fn test_start_migrating_not_found() {
        let engine = DrainEngine::new(1);
        assert!(!engine.start_migrating("nonexistent"));
    }

    #[test]
    fn test_complete_move() {
        let engine = DrainEngine::new(1);
        engine.enqueue_moves(vec![DrainMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 4,
            state: DrainMoveState::Migrating,
            bytes_transferred: gb(5),
            total_bytes: gb(10),
        }]);
        assert!(engine.complete_move("vol-a"));
        let moves = engine.moves();
        assert_eq!(moves[0].state, DrainMoveState::Completed);
        assert_eq!(moves[0].bytes_transferred, gb(10));
    }

    #[test]
    fn test_complete_move_wrong_state() {
        let engine = DrainEngine::new(1);
        engine.enqueue_moves(vec![DrainMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 4,
            state: DrainMoveState::Pending,
            bytes_transferred: 0,
            total_bytes: gb(10),
        }]);
        // Not in Migrating state.
        assert!(!engine.complete_move("vol-a"));
    }

    #[test]
    fn test_complete_move_not_found() {
        let engine = DrainEngine::new(1);
        assert!(!engine.complete_move("nonexistent"));
    }

    #[test]
    fn test_fail_move() {
        let engine = DrainEngine::new(1);
        engine.enqueue_moves(vec![DrainMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 4,
            state: DrainMoveState::Migrating,
            bytes_transferred: 0,
            total_bytes: gb(10),
        }]);
        engine.fail_move("vol-a", "network error");
        let moves = engine.moves();
        assert_eq!(
            moves[0].state,
            DrainMoveState::Failed("network error".into())
        );
    }

    #[test]
    fn test_fail_move_already_completed() {
        let engine = DrainEngine::new(1);
        engine.enqueue_moves(vec![DrainMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 4,
            state: DrainMoveState::Completed,
            bytes_transferred: gb(10),
            total_bytes: gb(10),
        }]);
        engine.fail_move("vol-a", "too late");
        // Should remain completed.
        assert_eq!(engine.moves()[0].state, DrainMoveState::Completed);
    }

    #[test]
    fn test_fail_move_pending() {
        let engine = DrainEngine::new(1);
        engine.enqueue_moves(vec![DrainMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 4,
            state: DrainMoveState::Pending,
            bytes_transferred: 0,
            total_bytes: gb(10),
        }]);
        engine.fail_move("vol-a", "cancelled");
        assert!(matches!(engine.moves()[0].state, DrainMoveState::Failed(_)));
    }

    // ── Progress tracking ───────────────────────────────────────────────

    #[test]
    fn test_update_progress() {
        let engine = DrainEngine::new(1);
        engine.enqueue_moves(vec![DrainMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 4,
            state: DrainMoveState::Migrating,
            bytes_transferred: 0,
            total_bytes: gb(10),
        }]);
        assert!(engine.update_progress("vol-a", gb(5)));
        assert_eq!(engine.moves()[0].bytes_transferred, gb(5));
    }

    #[test]
    fn test_update_progress_wrong_state() {
        let engine = DrainEngine::new(1);
        engine.enqueue_moves(vec![DrainMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 4,
            state: DrainMoveState::Pending,
            bytes_transferred: 0,
            total_bytes: gb(10),
        }]);
        assert!(!engine.update_progress("vol-a", gb(5)));
    }

    #[test]
    fn test_update_progress_not_found() {
        let engine = DrainEngine::new(1);
        assert!(!engine.update_progress("nonexistent", gb(5)));
    }

    #[test]
    fn test_progress_summary() {
        let engine = DrainEngine::new(1);
        engine.enqueue_moves(vec![
            DrainMove {
                volume_name: "vol-a".into(),
                source_node: 1,
                target_node: 4,
                state: DrainMoveState::Completed,
                bytes_transferred: gb(10),
                total_bytes: gb(10),
            },
            DrainMove {
                volume_name: "vol-b".into(),
                source_node: 1,
                target_node: 5,
                state: DrainMoveState::Migrating,
                bytes_transferred: gb(3),
                total_bytes: gb(5),
            },
            DrainMove {
                volume_name: "vol-c".into(),
                source_node: 1,
                target_node: 6,
                state: DrainMoveState::Failed("disk error".into()),
                bytes_transferred: 0,
                total_bytes: gb(20),
            },
        ]);
        let p = engine.progress();
        assert_eq!(p.node_id, 1);
        assert_eq!(p.total_moves, 3);
        assert_eq!(p.completed, 1);
        assert_eq!(p.failed, 1);
        assert_eq!(p.in_progress, 1);
        assert!(!p.is_complete);
    }

    #[test]
    fn test_progress_summary_all_completed() {
        let engine = DrainEngine::new(1);
        engine.enqueue_moves(vec![DrainMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 4,
            state: DrainMoveState::Completed,
            bytes_transferred: gb(10),
            total_bytes: gb(10),
        }]);
        let p = engine.progress();
        assert!(p.is_complete);
        assert_eq!(p.completed, 1);
        assert_eq!(p.in_progress, 0);
    }

    #[test]
    fn test_progress_summary_empty() {
        let engine = DrainEngine::new(1);
        let p = engine.progress();
        assert_eq!(p.total_moves, 0);
        assert_eq!(p.completed, 0);
        assert!(!p.is_complete);
    }

    // ── is_drain_complete ───────────────────────────────────────────────

    #[test]
    fn test_is_drain_complete_empty() {
        let engine = DrainEngine::new(1);
        assert!(engine.is_drain_complete());
    }

    #[test]
    fn test_is_drain_complete_all_done() {
        let engine = DrainEngine::new(1);
        engine.enqueue_moves(vec![
            DrainMove {
                volume_name: "vol-a".into(),
                source_node: 1,
                target_node: 4,
                state: DrainMoveState::Completed,
                bytes_transferred: gb(10),
                total_bytes: gb(10),
            },
            DrainMove {
                volume_name: "vol-b".into(),
                source_node: 1,
                target_node: 5,
                state: DrainMoveState::Failed("err".into()),
                bytes_transferred: 0,
                total_bytes: gb(5),
            },
        ]);
        assert!(engine.is_drain_complete());
    }

    #[test]
    fn test_is_drain_complete_still_pending() {
        let engine = DrainEngine::new(1);
        engine.enqueue_moves(vec![DrainMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 4,
            state: DrainMoveState::Pending,
            bytes_transferred: 0,
            total_bytes: gb(10),
        }]);
        assert!(!engine.is_drain_complete());
    }

    #[test]
    fn test_is_drain_complete_still_migrating() {
        let engine = DrainEngine::new(1);
        engine.enqueue_moves(vec![DrainMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 4,
            state: DrainMoveState::Migrating,
            bytes_transferred: gb(5),
            total_bytes: gb(10),
        }]);
        assert!(!engine.is_drain_complete());
    }

    // ── node_id accessor ────────────────────────────────────────────────

    #[test]
    fn test_node_id() {
        let engine = DrainEngine::new(42);
        assert_eq!(engine.node_id(), 42);
    }

    // ── DrainMoveState display ──────────────────────────────────────────

    #[test]
    fn test_drain_move_state_display() {
        assert_eq!(DrainMoveState::Pending.to_string(), "pending");
        assert_eq!(DrainMoveState::Migrating.to_string(), "migrating");
        assert_eq!(DrainMoveState::Completed.to_string(), "completed");
        assert_eq!(
            DrainMoveState::Failed("disk error".into()).to_string(),
            "failed: disk error"
        );
    }

    // ── DrainMoveState serialization ────────────────────────────────────

    #[test]
    fn test_drain_move_state_serialization() {
        let states = vec![
            DrainMoveState::Pending,
            DrainMoveState::Migrating,
            DrainMoveState::Completed,
            DrainMoveState::Failed("timeout".into()),
        ];
        for s in &states {
            let json = serde_json::to_string(s).unwrap();
            let decoded: DrainMoveState = serde_json::from_str(&json).unwrap();
            assert_eq!(&decoded, s);
        }
    }

    // ── DrainMove serialization ─────────────────────────────────────────

    #[test]
    fn test_drain_move_serialization() {
        let m = DrainMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 4,
            state: DrainMoveState::Pending,
            bytes_transferred: 0,
            total_bytes: gb(10),
        };
        let json = serde_json::to_string(&m).unwrap();
        let decoded: DrainMove = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.volume_name, "vol-a");
        assert_eq!(decoded.source_node, 1);
        assert_eq!(decoded.target_node, 4);
        assert_eq!(decoded.state, DrainMoveState::Pending);
    }

    // ── Full drain lifecycle ────────────────────────────────────────────

    #[test]
    fn test_drain_lifecycle_pending_to_migrating_to_completed() {
        let engine = DrainEngine::new(1);
        engine.enqueue_moves(vec![DrainMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 4,
            state: DrainMoveState::Pending,
            bytes_transferred: 0,
            total_bytes: gb(10),
        }]);

        assert!(!engine.is_drain_complete());

        assert!(engine.start_migrating("vol-a"));
        assert_eq!(engine.moves()[0].state, DrainMoveState::Migrating);
        assert!(!engine.is_drain_complete());

        assert!(engine.update_progress("vol-a", gb(5)));
        assert_eq!(engine.moves()[0].bytes_transferred, gb(5));

        assert!(engine.complete_move("vol-a"));
        assert_eq!(engine.moves()[0].state, DrainMoveState::Completed);
        assert!(engine.is_drain_complete());
    }

    #[test]
    fn test_drain_lifecycle_pending_to_migrating_to_failed() {
        let engine = DrainEngine::new(1);
        engine.enqueue_moves(vec![DrainMove {
            volume_name: "vol-a".into(),
            source_node: 1,
            target_node: 4,
            state: DrainMoveState::Pending,
            bytes_transferred: 0,
            total_bytes: gb(10),
        }]);

        assert!(engine.start_migrating("vol-a"));
        engine.fail_move("vol-a", "network timeout");
        assert!(matches!(engine.moves()[0].state, DrainMoveState::Failed(_)));
        assert!(engine.is_drain_complete());
    }
}
