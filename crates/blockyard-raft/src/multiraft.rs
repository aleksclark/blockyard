use crate::state_machine::{AppState, StateMachine};
use crate::types::RaftRequest;
use blockyard_common::types::RaftGroupId;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

pub struct RaftGroup {
    pub state_machine: StateMachine,
    next_index: std::sync::atomic::AtomicU64,
}

impl RaftGroup {
    fn new() -> Self {
        Self {
            state_machine: StateMachine::new(),
            next_index: std::sync::atomic::AtomicU64::new(1),
        }
    }

    pub fn propose(&self, req: &RaftRequest) -> crate::types::RaftResponse {
        let idx = self
            .next_index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.state_machine.apply(idx, req)
    }
}

pub struct MultiRaft {
    groups: Arc<RwLock<HashMap<RaftGroupId, RaftGroup>>>,
    node_id: u64,
}

impl std::fmt::Debug for MultiRaft {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiRaft")
            .field("node_id", &self.node_id)
            .field("group_count", &self.groups.read().len())
            .finish()
    }
}

impl MultiRaft {
    pub fn new(node_id: u64) -> Self {
        Self {
            groups: Arc::new(RwLock::new(HashMap::new())),
            node_id,
        }
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn create_group(&self, group_id: RaftGroupId) -> blockyard_common::Result<()> {
        let mut groups = self.groups.write();
        if groups.contains_key(&group_id) {
            return Err(blockyard_common::Error::Raft(format!(
                "group already exists: {group_id}"
            )));
        }
        groups.insert(group_id, RaftGroup::new());
        info!(group_id, node_id = self.node_id, "created raft group");
        Ok(())
    }

    pub fn propose(
        &self,
        group_id: RaftGroupId,
        req: &RaftRequest,
    ) -> blockyard_common::Result<crate::types::RaftResponse> {
        let groups = self.groups.read();
        let group = groups.get(&group_id).ok_or_else(|| {
            blockyard_common::Error::Raft(format!("group not found: {group_id}"))
        })?;
        Ok(group.propose(req))
    }

    pub fn group_count(&self) -> usize {
        self.groups.read().len()
    }

    pub fn has_group(&self, group_id: RaftGroupId) -> bool {
        self.groups.read().contains_key(&group_id)
    }

    pub fn get_state(&self, group_id: RaftGroupId) -> Option<AppState> {
        self.groups
            .read()
            .get(&group_id)
            .map(|g| g.state_machine.state())
    }

    pub fn remove_group(&self, group_id: RaftGroupId) -> bool {
        let removed = self.groups.write().remove(&group_id);
        if removed.is_some() {
            info!(group_id, "removed raft group");
        }
        removed.is_some()
    }

    pub async fn start(&self) -> blockyard_common::Result<()> {
        info!(node_id = self.node_id, "initializing Multi-Raft engine");
        Ok(())
    }
}

impl Default for MultiRaft {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multiraft_new() {
        let mr = MultiRaft::new(1);
        assert_eq!(mr.node_id(), 1);
        assert_eq!(mr.group_count(), 0);
    }

    #[test]
    fn test_multiraft_default() {
        let mr = MultiRaft::default();
        assert_eq!(mr.node_id(), 0);
    }

    #[test]
    fn test_multiraft_create_group() {
        let mr = MultiRaft::new(1);
        mr.create_group(100).unwrap();
        assert_eq!(mr.group_count(), 1);
        assert!(mr.has_group(100));
        assert!(!mr.has_group(200));
    }

    #[test]
    fn test_multiraft_create_duplicate_group() {
        let mr = MultiRaft::new(1);
        mr.create_group(100).unwrap();
        assert!(mr.create_group(100).is_err());
    }

    #[test]
    fn test_multiraft_propose() {
        let mr = MultiRaft::new(1);
        mr.create_group(100).unwrap();
        let resp = mr.propose(
            100,
            &RaftRequest::VolumeCreate {
                name: "vol-1".into(),
                size_bytes: 1024,
                replicas: 3,
            },
        );
        assert!(resp.is_ok());
        let state = mr.get_state(100).unwrap();
        assert!(state.volumes.contains_key("vol-1"));
    }

    #[test]
    fn test_multiraft_propose_missing_group() {
        let mr = MultiRaft::new(1);
        let resp = mr.propose(
            999,
            &RaftRequest::VolumeCreate {
                name: "vol-1".into(),
                size_bytes: 1024,
                replicas: 3,
            },
        );
        assert!(resp.is_err());
    }

    #[test]
    fn test_multiraft_get_state() {
        let mr = MultiRaft::new(1);
        mr.create_group(100).unwrap();
        let state = mr.get_state(100).unwrap();
        assert!(state.volumes.is_empty());
    }

    #[test]
    fn test_multiraft_get_state_missing() {
        let mr = MultiRaft::new(1);
        assert!(mr.get_state(999).is_none());
    }

    #[test]
    fn test_multiraft_remove_group() {
        let mr = MultiRaft::new(1);
        mr.create_group(100).unwrap();
        assert!(mr.remove_group(100));
        assert_eq!(mr.group_count(), 0);
    }

    #[test]
    fn test_multiraft_remove_missing() {
        let mr = MultiRaft::new(1);
        assert!(!mr.remove_group(999));
    }

    #[test]
    fn test_multiraft_multiple_groups() {
        let mr = MultiRaft::new(1);
        mr.create_group(1).unwrap();
        mr.create_group(2).unwrap();
        mr.create_group(3).unwrap();
        assert_eq!(mr.group_count(), 3);
    }

    #[tokio::test]
    async fn test_multiraft_start() {
        let mr = MultiRaft::new(1);
        mr.start().await.unwrap();
    }

    #[test]
    fn test_raft_group_propose_increments_index() {
        let mr = MultiRaft::new(1);
        mr.create_group(1).unwrap();
        for i in 0..5 {
            mr.propose(
                1,
                &RaftRequest::VolumeCreate {
                    name: format!("vol-{i}"),
                    size_bytes: 1024,
                    replicas: 1,
                },
            )
            .unwrap();
        }
        let state = mr.get_state(1).unwrap();
        assert_eq!(state.volumes.len(), 5);
        assert_eq!(state.applied_index, 5);
    }
}
