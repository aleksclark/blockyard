use crate::types::{RaftRequest, RaftResponse};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Tracks the state of an in-progress rebalance operation for a volume.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RebalanceState {
    /// Node the volume replica is moving from.
    pub source: u64,
    /// Node the volume replica is moving to.
    pub target: u64,
    /// Current phase of the rebalance.
    pub phase: RebalancePhase,
}

/// Lifecycle phases of a volume rebalance tracked in the state machine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RebalancePhase {
    Syncing,
    Completed,
    Failed(String),
}

impl std::fmt::Display for RebalancePhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Syncing => write!(f, "syncing"),
            Self::Completed => write!(f, "completed"),
            Self::Failed(reason) => write!(f, "failed: {reason}"),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppState {
    pub volumes: HashMap<String, VolumeRecord>,
    pub nodes: HashMap<u64, NodeRecord>,
    pub applied_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeRecord {
    pub name: String,
    pub size_bytes: u64,
    pub replicas: u32,
    pub placement: Vec<u64>,
    /// Tracks an ongoing rebalance operation, if any.
    #[serde(default)]
    pub rebalance_state: Option<RebalanceState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRecord {
    pub node_id: u64,
    pub addr: String,
}

#[derive(Debug, Clone)]
pub struct StateMachine {
    inner: Arc<Mutex<AppState>>,
}

impl StateMachine {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(AppState::default())),
        }
    }

    pub fn state(&self) -> AppState {
        self.inner.lock().clone()
    }

    pub fn apply(&self, index: u64, req: &RaftRequest) -> RaftResponse {
        let mut state = self.inner.lock();
        state.applied_index = index;

        match req {
            RaftRequest::VolumeCreate { name, size_bytes, replicas } => {
                state.volumes.insert(
                    name.clone(),
                    VolumeRecord {
                        name: name.clone(),
                        size_bytes: *size_bytes,
                        replicas: *replicas,
                        placement: Vec::new(),
                        rebalance_state: None,
                    },
                );
                RaftResponse::Ok
            }
            RaftRequest::VolumeDelete { name } => {
                state.volumes.remove(name);
                RaftResponse::Ok
            }
            RaftRequest::VolumeResize { name, new_size } => {
                if let Some(vol) = state.volumes.get_mut(name) {
                    vol.size_bytes = *new_size;
                    RaftResponse::Ok
                } else {
                    RaftResponse::Error(format!("volume not found: {name}"))
                }
            }
            RaftRequest::PlacementUpdate { volume_name, nodes } => {
                if let Some(vol) = state.volumes.get_mut(volume_name) {
                    vol.placement = nodes.clone();
                    RaftResponse::Ok
                } else {
                    RaftResponse::Error(format!("volume not found: {volume_name}"))
                }
            }
            RaftRequest::NodeRegister { node_id, addr } => {
                state.nodes.insert(
                    *node_id,
                    NodeRecord {
                        node_id: *node_id,
                        addr: addr.clone(),
                    },
                );
                RaftResponse::Ok
            }
            RaftRequest::NodeDeregister { node_id } => {
                state.nodes.remove(node_id);
                RaftResponse::Ok
            }
            RaftRequest::Write { .. } => RaftResponse::Ok,
            RaftRequest::RebalanceStart { volume_name, source, target } => {
                if let Some(vol) = state.volumes.get_mut(volume_name) {
                    vol.rebalance_state = Some(RebalanceState {
                        source: *source,
                        target: *target,
                        phase: RebalancePhase::Syncing,
                    });
                    RaftResponse::Ok
                } else {
                    RaftResponse::Error(format!("volume not found: {volume_name}"))
                }
            }
            RaftRequest::RebalanceComplete { volume_name } => {
                if let Some(vol) = state.volumes.get_mut(volume_name) {
                    if let Some(ref mut rs) = vol.rebalance_state {
                        // Update placement: replace source with target.
                        if let Some(pos) = vol.placement.iter().position(|&n| n == rs.source) {
                            vol.placement[pos] = rs.target;
                        }
                        rs.phase = RebalancePhase::Completed;
                    }
                    RaftResponse::Ok
                } else {
                    RaftResponse::Error(format!("volume not found: {volume_name}"))
                }
            }
            RaftRequest::RebalanceFail { volume_name, reason } => {
                if let Some(vol) = state.volumes.get_mut(volume_name) {
                    if let Some(ref mut rs) = vol.rebalance_state {
                        rs.phase = RebalancePhase::Failed(reason.clone());
                    }
                    RaftResponse::Ok
                } else {
                    RaftResponse::Error(format!("volume not found: {volume_name}"))
                }
            }
        }
    }

    pub fn snapshot(&self) -> Vec<u8> {
        let state = self.inner.lock();
        serde_json::to_vec(&*state).unwrap_or_default()
    }

    pub fn restore(&self, data: &[u8]) -> blockyard_common::Result<()> {
        let state: AppState = serde_json::from_slice(data)
            .map_err(|e| blockyard_common::Error::Raft(format!("snapshot restore: {e}")))?;
        *self.inner.lock() = state;
        Ok(())
    }
}

impl Default for StateMachine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_machine_new() {
        let sm = StateMachine::new();
        let state = sm.state();
        assert!(state.volumes.is_empty());
        assert!(state.nodes.is_empty());
        assert_eq!(state.applied_index, 0);
    }

    #[test]
    fn test_state_machine_default() {
        let sm = StateMachine::default();
        assert!(sm.state().volumes.is_empty());
    }

    #[test]
    fn test_apply_volume_create() {
        let sm = StateMachine::new();
        let req = RaftRequest::VolumeCreate {
            name: "vol-1".into(),
            size_bytes: 1024,
            replicas: 3,
        };
        let resp = sm.apply(1, &req);
        assert_eq!(resp, RaftResponse::Ok);
        let state = sm.state();
        assert!(state.volumes.contains_key("vol-1"));
        assert_eq!(state.volumes["vol-1"].replicas, 3);
        assert!(state.volumes["vol-1"].rebalance_state.is_none());
        assert_eq!(state.applied_index, 1);
    }

    #[test]
    fn test_apply_volume_delete() {
        let sm = StateMachine::new();
        sm.apply(
            1,
            &RaftRequest::VolumeCreate {
                name: "vol-1".into(),
                size_bytes: 1024,
                replicas: 3,
            },
        );
        let resp = sm.apply(2, &RaftRequest::VolumeDelete { name: "vol-1".into() });
        assert_eq!(resp, RaftResponse::Ok);
        assert!(!sm.state().volumes.contains_key("vol-1"));
    }

    #[test]
    fn test_apply_volume_resize() {
        let sm = StateMachine::new();
        sm.apply(
            1,
            &RaftRequest::VolumeCreate {
                name: "vol-1".into(),
                size_bytes: 1024,
                replicas: 3,
            },
        );
        let resp = sm.apply(
            2,
            &RaftRequest::VolumeResize { name: "vol-1".into(), new_size: 2048 },
        );
        assert_eq!(resp, RaftResponse::Ok);
        assert_eq!(sm.state().volumes["vol-1"].size_bytes, 2048);
    }

    #[test]
    fn test_apply_volume_resize_not_found() {
        let sm = StateMachine::new();
        let resp = sm.apply(
            1,
            &RaftRequest::VolumeResize { name: "vol-1".into(), new_size: 2048 },
        );
        assert!(matches!(resp, RaftResponse::Error(_)));
    }

    #[test]
    fn test_apply_placement_update() {
        let sm = StateMachine::new();
        sm.apply(
            1,
            &RaftRequest::VolumeCreate {
                name: "vol-1".into(),
                size_bytes: 1024,
                replicas: 3,
            },
        );
        let resp = sm.apply(
            2,
            &RaftRequest::PlacementUpdate {
                volume_name: "vol-1".into(),
                nodes: vec![1, 2, 3],
            },
        );
        assert_eq!(resp, RaftResponse::Ok);
        assert_eq!(sm.state().volumes["vol-1"].placement, vec![1, 2, 3]);
    }

    #[test]
    fn test_apply_placement_update_not_found() {
        let sm = StateMachine::new();
        let resp = sm.apply(
            1,
            &RaftRequest::PlacementUpdate {
                volume_name: "nope".into(),
                nodes: vec![1],
            },
        );
        assert!(matches!(resp, RaftResponse::Error(_)));
    }

    #[test]
    fn test_apply_node_register() {
        let sm = StateMachine::new();
        let resp = sm.apply(
            1,
            &RaftRequest::NodeRegister { node_id: 1, addr: "10.0.0.1:7400".into() },
        );
        assert_eq!(resp, RaftResponse::Ok);
        assert!(sm.state().nodes.contains_key(&1));
        assert_eq!(sm.state().nodes[&1].addr, "10.0.0.1:7400");
    }

    #[test]
    fn test_apply_node_deregister() {
        let sm = StateMachine::new();
        sm.apply(
            1,
            &RaftRequest::NodeRegister { node_id: 1, addr: "a".into() },
        );
        let resp = sm.apply(2, &RaftRequest::NodeDeregister { node_id: 1 });
        assert_eq!(resp, RaftResponse::Ok);
        assert!(!sm.state().nodes.contains_key(&1));
    }

    #[test]
    fn test_apply_write() {
        let sm = StateMachine::new();
        let resp = sm.apply(
            1,
            &RaftRequest::Write { volume_id: 1, offset: 0, data: vec![1, 2, 3] },
        );
        assert_eq!(resp, RaftResponse::Ok);
    }

    #[test]
    fn test_snapshot_and_restore() {
        let sm = StateMachine::new();
        sm.apply(
            1,
            &RaftRequest::VolumeCreate {
                name: "vol-1".into(),
                size_bytes: 1024,
                replicas: 3,
            },
        );
        sm.apply(
            2,
            &RaftRequest::NodeRegister { node_id: 1, addr: "a".into() },
        );

        let snap = sm.snapshot();
        assert!(!snap.is_empty());

        let sm2 = StateMachine::new();
        sm2.restore(&snap).unwrap();
        let state2 = sm2.state();
        assert!(state2.volumes.contains_key("vol-1"));
        assert!(state2.nodes.contains_key(&1));
        assert_eq!(state2.applied_index, 2);
    }

    #[test]
    fn test_restore_invalid_data() {
        let sm = StateMachine::new();
        let result = sm.restore(b"not json");
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_applies() {
        let sm = StateMachine::new();
        for i in 0..10 {
            sm.apply(
                i + 1,
                &RaftRequest::VolumeCreate {
                    name: format!("vol-{i}"),
                    size_bytes: 1024,
                    replicas: 1,
                },
            );
        }
        assert_eq!(sm.state().volumes.len(), 10);
        assert_eq!(sm.state().applied_index, 10);
    }

    // ── Rebalance state machine tests ───────────────────────────────────

    #[test]
    fn test_apply_rebalance_start() {
        let sm = StateMachine::new();
        sm.apply(
            1,
            &RaftRequest::VolumeCreate {
                name: "vol-1".into(),
                size_bytes: 1024,
                replicas: 3,
            },
        );
        sm.apply(
            2,
            &RaftRequest::PlacementUpdate {
                volume_name: "vol-1".into(),
                nodes: vec![1, 2, 3],
            },
        );
        let resp = sm.apply(
            3,
            &RaftRequest::RebalanceStart {
                volume_name: "vol-1".into(),
                source: 1,
                target: 4,
            },
        );
        assert_eq!(resp, RaftResponse::Ok);
        let vol = &sm.state().volumes["vol-1"];
        let rs = vol.rebalance_state.as_ref().unwrap();
        assert_eq!(rs.source, 1);
        assert_eq!(rs.target, 4);
        assert_eq!(rs.phase, RebalancePhase::Syncing);
    }

    #[test]
    fn test_apply_rebalance_start_not_found() {
        let sm = StateMachine::new();
        let resp = sm.apply(
            1,
            &RaftRequest::RebalanceStart {
                volume_name: "nonexistent".into(),
                source: 1,
                target: 2,
            },
        );
        assert!(matches!(resp, RaftResponse::Error(_)));
    }

    #[test]
    fn test_apply_rebalance_complete() {
        let sm = StateMachine::new();
        sm.apply(
            1,
            &RaftRequest::VolumeCreate {
                name: "vol-1".into(),
                size_bytes: 1024,
                replicas: 3,
            },
        );
        sm.apply(
            2,
            &RaftRequest::PlacementUpdate {
                volume_name: "vol-1".into(),
                nodes: vec![1, 2, 3],
            },
        );
        sm.apply(
            3,
            &RaftRequest::RebalanceStart {
                volume_name: "vol-1".into(),
                source: 1,
                target: 4,
            },
        );
        let resp = sm.apply(
            4,
            &RaftRequest::RebalanceComplete {
                volume_name: "vol-1".into(),
            },
        );
        assert_eq!(resp, RaftResponse::Ok);
        let vol = &sm.state().volumes["vol-1"];
        // Placement should have 4 replacing 1.
        assert!(vol.placement.contains(&4));
        assert!(!vol.placement.contains(&1));
        assert_eq!(vol.placement.len(), 3);
        let rs = vol.rebalance_state.as_ref().unwrap();
        assert_eq!(rs.phase, RebalancePhase::Completed);
    }

    #[test]
    fn test_apply_rebalance_complete_not_found() {
        let sm = StateMachine::new();
        let resp = sm.apply(
            1,
            &RaftRequest::RebalanceComplete {
                volume_name: "nonexistent".into(),
            },
        );
        assert!(matches!(resp, RaftResponse::Error(_)));
    }

    #[test]
    fn test_apply_rebalance_fail() {
        let sm = StateMachine::new();
        sm.apply(
            1,
            &RaftRequest::VolumeCreate {
                name: "vol-1".into(),
                size_bytes: 1024,
                replicas: 3,
            },
        );
        sm.apply(
            2,
            &RaftRequest::RebalanceStart {
                volume_name: "vol-1".into(),
                source: 1,
                target: 2,
            },
        );
        let resp = sm.apply(
            3,
            &RaftRequest::RebalanceFail {
                volume_name: "vol-1".into(),
                reason: "disk error".into(),
            },
        );
        assert_eq!(resp, RaftResponse::Ok);
        let vol = &sm.state().volumes["vol-1"];
        let rs = vol.rebalance_state.as_ref().unwrap();
        assert_eq!(rs.phase, RebalancePhase::Failed("disk error".into()));
    }

    #[test]
    fn test_apply_rebalance_fail_not_found() {
        let sm = StateMachine::new();
        let resp = sm.apply(
            1,
            &RaftRequest::RebalanceFail {
                volume_name: "nonexistent".into(),
                reason: "nope".into(),
            },
        );
        assert!(matches!(resp, RaftResponse::Error(_)));
    }

    #[test]
    fn test_rebalance_state_snapshot_restore() {
        let sm = StateMachine::new();
        sm.apply(
            1,
            &RaftRequest::VolumeCreate {
                name: "vol-1".into(),
                size_bytes: 1024,
                replicas: 2,
            },
        );
        sm.apply(
            2,
            &RaftRequest::PlacementUpdate {
                volume_name: "vol-1".into(),
                nodes: vec![1, 2],
            },
        );
        sm.apply(
            3,
            &RaftRequest::RebalanceStart {
                volume_name: "vol-1".into(),
                source: 1,
                target: 3,
            },
        );

        let snap = sm.snapshot();
        let sm2 = StateMachine::new();
        sm2.restore(&snap).unwrap();
        let vol = &sm2.state().volumes["vol-1"];
        let rs = vol.rebalance_state.as_ref().unwrap();
        assert_eq!(rs.source, 1);
        assert_eq!(rs.target, 3);
        assert_eq!(rs.phase, RebalancePhase::Syncing);
    }

    #[test]
    fn test_rebalance_phase_display() {
        assert_eq!(RebalancePhase::Syncing.to_string(), "syncing");
        assert_eq!(RebalancePhase::Completed.to_string(), "completed");
        assert_eq!(
            RebalancePhase::Failed("timeout".into()).to_string(),
            "failed: timeout"
        );
    }
}
