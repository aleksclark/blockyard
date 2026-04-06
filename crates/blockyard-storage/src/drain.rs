use blockyard_common::types::NodeId;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DrainMoveState {
    Pending,
    Migrating,
    Completed,
    Failed,
}

impl std::fmt::Display for DrainMoveState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Migrating => write!(f, "migrating"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DrainMove {
    pub volume_name: String,
    pub source_node: NodeId,
    pub target_node: NodeId,
    pub state: DrainMoveState,
}

#[derive(Debug, Clone)]
pub struct DrainProgress {
    pub node_id: NodeId,
    pub total_volumes: usize,
    pub completed: usize,
    pub failed: usize,
    pub in_progress: usize,
}

pub struct DrainEngine {
    moves: Arc<Mutex<Vec<DrainMove>>>,
}

impl DrainEngine {
    pub fn new() -> Self {
        Self {
            moves: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn enqueue_move(&self, volume_name: String, source_node: NodeId, target_node: NodeId) {
        self.moves.lock().push(DrainMove {
            volume_name,
            source_node,
            target_node,
            state: DrainMoveState::Pending,
        });
    }

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

    pub fn complete_move(&self, volume_name: &str) -> bool {
        let mut moves = self.moves.lock();
        if let Some(m) = moves
            .iter_mut()
            .find(|m| m.volume_name == volume_name && m.state == DrainMoveState::Migrating)
        {
            m.state = DrainMoveState::Completed;
            info!(volume = %volume_name, "drain move completed");
            true
        } else {
            false
        }
    }

    pub fn fail_move(&self, volume_name: &str) -> bool {
        let mut moves = self.moves.lock();
        if let Some(m) = moves
            .iter_mut()
            .find(|m| m.volume_name == volume_name && m.state == DrainMoveState::Migrating)
        {
            m.state = DrainMoveState::Failed;
            true
        } else {
            false
        }
    }

    pub fn progress(&self, node_id: NodeId) -> DrainProgress {
        let moves = self.moves.lock();
        let node_moves: Vec<&DrainMove> =
            moves.iter().filter(|m| m.source_node == node_id).collect();
        DrainProgress {
            node_id,
            total_volumes: node_moves.len(),
            completed: node_moves
                .iter()
                .filter(|m| m.state == DrainMoveState::Completed)
                .count(),
            failed: node_moves
                .iter()
                .filter(|m| m.state == DrainMoveState::Failed)
                .count(),
            in_progress: node_moves
                .iter()
                .filter(|m| m.state == DrainMoveState::Migrating)
                .count(),
        }
    }

    pub fn is_drain_complete(&self, node_id: NodeId) -> bool {
        let progress = self.progress(node_id);
        progress.total_volumes > 0 && progress.completed + progress.failed == progress.total_volumes
    }

    pub fn all_moves(&self) -> Vec<DrainMove> {
        self.moves.lock().clone()
    }

    pub fn move_count(&self) -> usize {
        self.moves.lock().len()
    }
}

impl Default for DrainEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_drain_engine_new() {
        let engine = DrainEngine::new();
        assert_eq!(engine.move_count(), 0);
    }

    #[test]
    fn test_drain_engine_default() {
        let engine = DrainEngine::default();
        assert_eq!(engine.move_count(), 0);
    }

    #[test]
    fn test_enqueue_move() {
        let engine = DrainEngine::new();
        engine.enqueue_move("vol-1".into(), 1, 2);
        assert_eq!(engine.move_count(), 1);
        let moves = engine.all_moves();
        assert_eq!(moves[0].state, DrainMoveState::Pending);
    }

    #[test]
    fn test_start_migrating() {
        let engine = DrainEngine::new();
        engine.enqueue_move("vol-1".into(), 1, 2);
        assert!(engine.start_migrating("vol-1"));
        let moves = engine.all_moves();
        assert_eq!(moves[0].state, DrainMoveState::Migrating);
    }

    #[test]
    fn test_start_migrating_wrong_volume() {
        let engine = DrainEngine::new();
        engine.enqueue_move("vol-1".into(), 1, 2);
        assert!(!engine.start_migrating("vol-999"));
    }

    #[test]
    fn test_complete_move() {
        let engine = DrainEngine::new();
        engine.enqueue_move("vol-1".into(), 1, 2);
        engine.start_migrating("vol-1");
        assert!(engine.complete_move("vol-1"));
        let moves = engine.all_moves();
        assert_eq!(moves[0].state, DrainMoveState::Completed);
    }

    #[test]
    fn test_fail_move() {
        let engine = DrainEngine::new();
        engine.enqueue_move("vol-1".into(), 1, 2);
        engine.start_migrating("vol-1");
        assert!(engine.fail_move("vol-1"));
        let moves = engine.all_moves();
        assert_eq!(moves[0].state, DrainMoveState::Failed);
    }

    #[test]
    fn test_progress() {
        let engine = DrainEngine::new();
        engine.enqueue_move("vol-1".into(), 1, 2);
        engine.enqueue_move("vol-2".into(), 1, 3);
        engine.start_migrating("vol-1");
        engine.complete_move("vol-1");

        let progress = engine.progress(1);
        assert_eq!(progress.total_volumes, 2);
        assert_eq!(progress.completed, 1);
        assert_eq!(progress.in_progress, 0);
    }

    #[test]
    fn test_is_drain_complete() {
        let engine = DrainEngine::new();
        engine.enqueue_move("vol-1".into(), 1, 2);
        assert!(!engine.is_drain_complete(1));

        engine.start_migrating("vol-1");
        assert!(!engine.is_drain_complete(1));

        engine.complete_move("vol-1");
        assert!(engine.is_drain_complete(1));
    }

    #[test]
    fn test_is_drain_complete_with_failure() {
        let engine = DrainEngine::new();
        engine.enqueue_move("vol-1".into(), 1, 2);
        engine.start_migrating("vol-1");
        engine.fail_move("vol-1");
        assert!(engine.is_drain_complete(1));
    }

    #[test]
    fn test_drain_move_state_display() {
        assert_eq!(DrainMoveState::Pending.to_string(), "pending");
        assert_eq!(DrainMoveState::Migrating.to_string(), "migrating");
        assert_eq!(DrainMoveState::Completed.to_string(), "completed");
        assert_eq!(DrainMoveState::Failed.to_string(), "failed");
    }

    #[test]
    fn test_multiple_volumes_drain() {
        let engine = DrainEngine::new();
        for i in 0..5 {
            engine.enqueue_move(format!("vol-{i}"), 1, 2 + i);
        }
        assert_eq!(engine.move_count(), 5);

        for i in 0..5 {
            engine.start_migrating(&format!("vol-{i}"));
            engine.complete_move(&format!("vol-{i}"));
        }
        assert!(engine.is_drain_complete(1));
        let progress = engine.progress(1);
        assert_eq!(progress.completed, 5);
    }
}
