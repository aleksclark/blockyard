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
            // Don't transition a maintenance node to Suspect or Failed.
            if node.state == NodeState::Maintenance
                && (state == NodeState::Suspect || state == NodeState::Failed)
            {
                return;
            }
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
                    // Don't mark maintenance nodes as suspect.
                    if n.state == NodeState::Maintenance {
                        return;
                    }
                    if *incarnation >= n.incarnation && n.state == NodeState::Healthy {
                        n.state = NodeState::Suspect;
                    }
                }
            }
            GossipUpdate::NodeDead { node, incarnation } => {
                let mut map = self.inner.write();
                if let Some(n) = map.get_mut(node) {
                    // Don't mark maintenance nodes as failed.
                    if n.state == NodeState::Maintenance {
                        return;
                    }
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

    /// Returns `true` if the node with the given ID is in maintenance state.
    pub fn is_in_maintenance(&self, id: NodeId) -> bool {
        self.inner
            .read()
            .get(&id)
            .is_some_and(|n| n.state == NodeState::Maintenance)
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
            pools: Vec::new(),
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

    // ── Maintenance mode tests ────────────────────────────────────────────

    #[test]
    fn test_maintenance_node_not_marked_suspect() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        ml.mark_state(1, NodeState::Maintenance);
        assert_eq!(ml.get(1).unwrap().state, NodeState::Maintenance);
        ml.drain_pending_updates(100);

        // Attempting to mark as suspect should be ignored.
        ml.mark_state(1, NodeState::Suspect);
        assert_eq!(ml.get(1).unwrap().state, NodeState::Maintenance);
        let updates = ml.drain_pending_updates(100);
        assert!(updates.is_empty());
    }

    #[test]
    fn test_maintenance_node_not_marked_failed() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        ml.mark_state(1, NodeState::Maintenance);
        ml.drain_pending_updates(100);

        // Attempting to mark as failed should be ignored.
        ml.mark_state(1, NodeState::Failed);
        assert_eq!(ml.get(1).unwrap().state, NodeState::Maintenance);
        let updates = ml.drain_pending_updates(100);
        assert!(updates.is_empty());
    }

    #[test]
    fn test_maintenance_node_gossip_suspect_ignored() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        ml.mark_state(1, NodeState::Maintenance);

        // Apply a gossip NodeSuspect update — should be ignored.
        ml.apply_update(&GossipUpdate::NodeSuspect {
            node: 1,
            incarnation: 1,
        });
        assert_eq!(ml.get(1).unwrap().state, NodeState::Maintenance);
    }

    #[test]
    fn test_maintenance_node_gossip_dead_ignored() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        ml.mark_state(1, NodeState::Maintenance);

        // Apply a gossip NodeDead update — should be ignored.
        ml.apply_update(&GossipUpdate::NodeDead {
            node: 1,
            incarnation: 1,
        });
        assert_eq!(ml.get(1).unwrap().state, NodeState::Maintenance);
    }

    #[test]
    fn test_is_in_maintenance() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        assert!(!ml.is_in_maintenance(1));

        ml.mark_state(1, NodeState::Maintenance);
        assert!(ml.is_in_maintenance(1));

        // Non-existent node.
        assert!(!ml.is_in_maintenance(999));
    }

    #[test]
    fn test_maintenance_node_can_leave() {
        let ml = MemberList::new();
        ml.upsert(make_node(1, 1));
        ml.mark_state(1, NodeState::Maintenance);

        // A maintenance node can still be explicitly marked as Left.
        ml.mark_state(1, NodeState::Left);
        assert_eq!(ml.get(1).unwrap().state, NodeState::Left);
    }
}
