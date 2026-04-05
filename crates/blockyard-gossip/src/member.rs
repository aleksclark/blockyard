use blockyard_common::types::{NodeId, NodeInfo, NodeState, ZfsHealthState};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use crate::protocol::GossipUpdate;

#[derive(Debug, Clone)]
pub struct MemberList {
    inner: Arc<RwLock<HashMap<NodeId, NodeInfo>>>,
    pending_updates: Arc<RwLock<Vec<GossipUpdate>>>,
}

impl MemberList {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            pending_updates: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn upsert(&self, info: NodeInfo) {
        let mut map = self.inner.write();
        let should_update = match map.get(&info.id) {
            Some(existing) => info.incarnation >= existing.incarnation,
            None => true,
        };
        if should_update {
            let update = GossipUpdate::NodeAlive(info.clone());
            map.insert(info.id, info);
            self.pending_updates.write().push(update);
        }
    }

    pub fn get(&self, id: NodeId) -> Option<NodeInfo> {
        self.inner.read().get(&id).cloned()
    }

    pub fn healthy_nodes(&self) -> Vec<NodeInfo> {
        self.inner
            .read()
            .values()
            .filter(|n| n.state == NodeState::Healthy)
            .cloned()
            .collect()
    }

    pub fn all_nodes(&self) -> Vec<NodeInfo> {
        self.inner.read().values().cloned().collect()
    }

    pub fn mark_state(&self, id: NodeId, state: NodeState) {
        if let Some(node) = self.inner.write().get_mut(&id) {
            let old_state = node.state;
            node.state = state;
            if old_state != state {
                let update = match state {
                    NodeState::Suspect => GossipUpdate::NodeSuspect {
                        node: id,
                        incarnation: node.incarnation,
                    },
                    NodeState::Failed => GossipUpdate::NodeDead {
                        node: id,
                        incarnation: node.incarnation,
                    },
                    NodeState::Left => GossipUpdate::NodeLeft { node: id },
                    _ => GossipUpdate::NodeAlive(node.clone()),
                };
                self.pending_updates.write().push(update);
            }
        }
    }

    pub fn mark_zfs_health(&self, id: NodeId, health: ZfsHealthState) {
        if let Some(node) = self.inner.write().get_mut(&id) {
            node.zfs_health = health;
            self.pending_updates.write().push(GossipUpdate::ZfsHealth {
                node: id,
                state: health,
            });
        }
    }

    pub fn apply_update(&self, update: &GossipUpdate) {
        match update {
            GossipUpdate::NodeAlive(info) => {
                self.upsert(info.clone());
            }
            GossipUpdate::NodeSuspect { node, incarnation } => {
                let mut map = self.inner.write();
                if let Some(n) = map.get_mut(node) {
                    if *incarnation >= n.incarnation && n.state == NodeState::Healthy {
                        n.state = NodeState::Suspect;
                    }
                }
            }
            GossipUpdate::NodeDead { node, incarnation } => {
                let mut map = self.inner.write();
                if let Some(n) = map.get_mut(node) {
                    if *incarnation >= n.incarnation {
                        n.state = NodeState::Failed;
                    }
                }
            }
            GossipUpdate::NodeLeft { node } => {
                let mut map = self.inner.write();
                if let Some(n) = map.get_mut(node) {
                    n.state = NodeState::Left;
                }
            }
            GossipUpdate::ZfsHealth { node, state } => {
                let mut map = self.inner.write();
                if let Some(n) = map.get_mut(node) {
                    n.zfs_health = *state;
                }
            }
        }
    }

    pub fn drain_pending_updates(&self, max: usize) -> Vec<GossipUpdate> {
        let mut pending = self.pending_updates.write();
        let count = max.min(pending.len());
        pending.drain(..count).collect()
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn remove(&self, id: NodeId) -> Option<NodeInfo> {
        self.inner.write().remove(&id)
    }

    pub fn nodes_with_healthy_zfs(&self) -> Vec<NodeInfo> {
        self.inner
            .read()
            .values()
            .filter(|n| n.state == NodeState::Healthy && n.zfs_health == ZfsHealthState::Online)
            .cloned()
            .collect()
    }
}

impl Default for MemberList {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_node(id: NodeId, incarnation: u64) -> NodeInfo {
        NodeInfo {
            id,
            name: format!("node-{id}"),
            addr: format!("127.0.0.1:{}", 7400 + id).parse().unwrap(),
            data_addr: format!("127.0.0.1:{}", 7500 + id).parse().unwrap(),
            tags: HashMap::new(),
            state: NodeState::Healthy,
            zfs_health: ZfsHealthState::Online,
            capacity_bytes: 1024 * 1024 * 1024,
            used_bytes: 0,
            incarnation,
        }
    }

    #[test]
    fn test_new_empty() {
        let ml = MemberList::new();
        assert!(ml.is_empty());
        assert_eq!(ml.len(), 0);
    }

    #[test]
    fn test_default_empty() {
        let ml = MemberList::default();
        assert!(ml.is_empty());
    }

    #[test]
    fn test_upsert_and_get() {
        let ml = MemberList::new();
        let node = make_node(1, 1);
        ml.upsert(node.clone());
        let fetched = ml.get(1).unwrap();
        assert_eq!(fetched.id, 1);
        assert_eq!(fetched.name, "node-1");
    }

    #[test]
    fn test_get_missing() {
        let ml = MemberList::new();
        assert!(ml.get(999).is_none());
    }

    #[test]
    fn test_upsert_respects_incarnation() {
        let ml = MemberList::new();

        let mut node_v1 = make_node(1, 1);
        node_v1.name = "old-name".into();
        ml.upsert(node_v1);

        let mut node_v2 = make_node(1, 2);
        node_v2.name = "new-name".into();
        ml.upsert(node_v2);
        assert_eq!(ml.get(1).unwrap().name, "new-name");

        let mut node_v0 = make_node(1, 0);
        node_v0.name = "stale-name".into();
        ml.upsert(node_v0);
        assert_eq!(ml.get(1).unwrap().name, "new-name");
    }

    #[test]
    fn test_healthy_nodes() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        ml.upsert(make_node(2, 1));

        let mut failed = make_node(3, 1);
        failed.state = NodeState::Failed;
        ml.upsert(failed);

        let healthy = ml.healthy_nodes();
        assert_eq!(healthy.len(), 2);
    }

    #[test]
    fn test_all_nodes() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        ml.upsert(make_node(2, 1));
        assert_eq!(ml.all_nodes().len(), 2);
    }

    #[test]
    fn test_mark_state() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        ml.mark_state(1, NodeState::Suspect);
        assert_eq!(ml.get(1).unwrap().state, NodeState::Suspect);
    }

    #[test]
    fn test_mark_state_unknown_node() {
        let ml = MemberList::new();
        ml.mark_state(999, NodeState::Failed);
        assert!(ml.get(999).is_none());
    }

    #[test]
    fn test_mark_zfs_health() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        ml.mark_zfs_health(1, ZfsHealthState::Degraded);
        assert_eq!(ml.get(1).unwrap().zfs_health, ZfsHealthState::Degraded);
    }

    #[test]
    fn test_mark_zfs_health_unknown_node() {
        let ml = MemberList::new();
        ml.mark_zfs_health(999, ZfsHealthState::Faulted);
    }

    #[test]
    fn test_remove() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        let removed = ml.remove(1);
        assert!(removed.is_some());
        assert!(ml.get(1).is_none());
        assert_eq!(ml.len(), 0);
    }

    #[test]
    fn test_remove_missing() {
        let ml = MemberList::new();
        assert!(ml.remove(999).is_none());
    }

    #[test]
    fn test_drain_pending_updates() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        ml.upsert(make_node(2, 1));
        ml.mark_state(1, NodeState::Suspect);

        let updates = ml.drain_pending_updates(10);
        assert_eq!(updates.len(), 3);

        let remaining = ml.drain_pending_updates(10);
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_drain_pending_updates_limited() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        ml.upsert(make_node(2, 1));
        ml.upsert(make_node(3, 1));

        let updates = ml.drain_pending_updates(2);
        assert_eq!(updates.len(), 2);

        let remaining = ml.drain_pending_updates(10);
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn test_apply_update_node_alive() {
        let ml = MemberList::new();
        let node = make_node(5, 1);
        ml.apply_update(&GossipUpdate::NodeAlive(node));
        assert!(ml.get(5).is_some());
    }

    #[test]
    fn test_apply_update_node_suspect() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        ml.apply_update(&GossipUpdate::NodeSuspect {
            node: 1,
            incarnation: 1,
        });
        assert_eq!(ml.get(1).unwrap().state, NodeState::Suspect);
    }

    #[test]
    fn test_apply_update_node_suspect_stale_incarnation() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 5));
        ml.apply_update(&GossipUpdate::NodeSuspect {
            node: 1,
            incarnation: 3,
        });
        assert_eq!(ml.get(1).unwrap().state, NodeState::Healthy);
    }

    #[test]
    fn test_apply_update_node_dead() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        ml.apply_update(&GossipUpdate::NodeDead {
            node: 1,
            incarnation: 1,
        });
        assert_eq!(ml.get(1).unwrap().state, NodeState::Failed);
    }

    #[test]
    fn test_apply_update_node_left() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        ml.apply_update(&GossipUpdate::NodeLeft { node: 1 });
        assert_eq!(ml.get(1).unwrap().state, NodeState::Left);
    }

    #[test]
    fn test_apply_update_zfs_health() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        ml.apply_update(&GossipUpdate::ZfsHealth {
            node: 1,
            state: ZfsHealthState::Faulted,
        });
        assert_eq!(ml.get(1).unwrap().zfs_health, ZfsHealthState::Faulted);
    }

    #[test]
    fn test_nodes_with_healthy_zfs() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        ml.upsert(make_node(2, 1));
        ml.mark_zfs_health(2, ZfsHealthState::Degraded);

        let healthy = ml.nodes_with_healthy_zfs();
        assert_eq!(healthy.len(), 1);
        assert_eq!(healthy[0].id, 1);
    }

    #[test]
    fn test_mark_state_generates_update() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        ml.drain_pending_updates(100);

        ml.mark_state(1, NodeState::Failed);
        let updates = ml.drain_pending_updates(100);
        assert_eq!(updates.len(), 1);
        assert!(matches!(updates[0], GossipUpdate::NodeDead { .. }));
    }

    #[test]
    fn test_mark_state_same_state_no_update() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        ml.drain_pending_updates(100);

        ml.mark_state(1, NodeState::Healthy);
        let updates = ml.drain_pending_updates(100);
        assert!(updates.is_empty());
    }

    #[test]
    fn test_mark_state_left_generates_node_left() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        ml.drain_pending_updates(100);

        ml.mark_state(1, NodeState::Left);
        let updates = ml.drain_pending_updates(100);
        assert_eq!(updates.len(), 1);
        assert!(matches!(updates[0], GossipUpdate::NodeLeft { .. }));
    }
}
