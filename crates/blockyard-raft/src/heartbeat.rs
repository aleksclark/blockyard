use blockyard_common::types::{NodeId, RaftGroupId};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::debug;

type GroupPairMap = HashMap<(NodeId, NodeId), HashSet<RaftGroupId>>;

pub struct HeartbeatConsolidator {
    shared_groups: Arc<RwLock<GroupPairMap>>,
    last_heartbeat: Arc<RwLock<HashMap<NodeId, Instant>>>,
    interval: Duration,
}

#[derive(Debug, Clone)]
pub struct ConsolidatedHeartbeat {
    pub from: NodeId,
    pub to: NodeId,
    pub groups: Vec<RaftGroupId>,
}

impl HeartbeatConsolidator {
    pub fn new(interval: Duration) -> Self {
        Self {
            shared_groups: Arc::new(RwLock::new(HashMap::new())),
            last_heartbeat: Arc::new(RwLock::new(HashMap::new())),
            interval,
        }
    }

    pub fn register_group(&self, group_id: RaftGroupId, members: &[NodeId]) {
        let mut shared = self.shared_groups.write();
        for &a in members {
            for &b in members {
                if a != b {
                    shared.entry((a, b)).or_default().insert(group_id);
                }
            }
        }
    }

    pub fn unregister_group(&self, group_id: RaftGroupId) {
        let mut shared = self.shared_groups.write();
        for groups in shared.values_mut() {
            groups.remove(&group_id);
        }
        shared.retain(|_, groups| !groups.is_empty());
    }

    pub fn groups_between(&self, from: NodeId, to: NodeId) -> Vec<RaftGroupId> {
        self.shared_groups
            .read()
            .get(&(from, to))
            .map(|gs| gs.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn peers_for(&self, node: NodeId) -> Vec<NodeId> {
        let shared = self.shared_groups.read();
        let mut peers = HashSet::new();
        for &(from, to) in shared.keys() {
            if from == node {
                peers.insert(to);
            }
        }
        peers.into_iter().collect()
    }

    pub fn should_heartbeat(&self, to: NodeId) -> bool {
        let last = self.last_heartbeat.read();
        match last.get(&to) {
            Some(t) => t.elapsed() >= self.interval,
            None => true,
        }
    }

    pub fn mark_heartbeat_sent(&self, to: NodeId) {
        self.last_heartbeat.write().insert(to, Instant::now());
    }

    pub fn generate_heartbeats(&self, local_id: NodeId) -> Vec<ConsolidatedHeartbeat> {
        let peers = self.peers_for(local_id);
        let mut heartbeats = Vec::new();

        for peer in peers {
            if self.should_heartbeat(peer) {
                let groups = self.groups_between(local_id, peer);
                if !groups.is_empty() {
                    heartbeats.push(ConsolidatedHeartbeat {
                        from: local_id,
                        to: peer,
                        groups,
                    });
                    self.mark_heartbeat_sent(peer);
                }
            }
        }

        debug!(
            node = local_id,
            count = heartbeats.len(),
            "generated consolidated heartbeats"
        );
        heartbeats
    }

    pub fn total_pairs(&self) -> usize {
        self.shared_groups.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heartbeat_consolidator_new() {
        let hc = HeartbeatConsolidator::new(Duration::from_millis(200));
        assert_eq!(hc.total_pairs(), 0);
    }

    #[test]
    fn test_register_group() {
        let hc = HeartbeatConsolidator::new(Duration::from_millis(200));
        hc.register_group(1, &[10, 20, 30]);
        let groups = hc.groups_between(10, 20);
        assert_eq!(groups, vec![1]);
        let groups = hc.groups_between(20, 10);
        assert_eq!(groups, vec![1]);
    }

    #[test]
    fn test_register_multiple_groups_same_pair() {
        let hc = HeartbeatConsolidator::new(Duration::from_millis(200));
        hc.register_group(1, &[10, 20]);
        hc.register_group(2, &[10, 20]);
        let groups = hc.groups_between(10, 20);
        assert_eq!(groups.len(), 2);
        assert!(groups.contains(&1));
        assert!(groups.contains(&2));
    }

    #[test]
    fn test_unregister_group() {
        let hc = HeartbeatConsolidator::new(Duration::from_millis(200));
        hc.register_group(1, &[10, 20]);
        hc.register_group(2, &[10, 20]);
        hc.unregister_group(1);
        let groups = hc.groups_between(10, 20);
        assert_eq!(groups, vec![2]);
    }

    #[test]
    fn test_unregister_last_group_removes_pair() {
        let hc = HeartbeatConsolidator::new(Duration::from_millis(200));
        hc.register_group(1, &[10, 20]);
        hc.unregister_group(1);
        assert_eq!(hc.total_pairs(), 0);
    }

    #[test]
    fn test_peers_for() {
        let hc = HeartbeatConsolidator::new(Duration::from_millis(200));
        hc.register_group(1, &[10, 20, 30]);
        let mut peers = hc.peers_for(10);
        peers.sort();
        assert_eq!(peers, vec![20, 30]);
    }

    #[test]
    fn test_peers_for_no_groups() {
        let hc = HeartbeatConsolidator::new(Duration::from_millis(200));
        assert!(hc.peers_for(10).is_empty());
    }

    #[test]
    fn test_should_heartbeat_first_time() {
        let hc = HeartbeatConsolidator::new(Duration::from_millis(200));
        assert!(hc.should_heartbeat(20));
    }

    #[test]
    fn test_should_heartbeat_after_send() {
        let hc = HeartbeatConsolidator::new(Duration::from_secs(60));
        hc.mark_heartbeat_sent(20);
        assert!(!hc.should_heartbeat(20));
    }

    #[test]
    fn test_should_heartbeat_after_interval() {
        let hc = HeartbeatConsolidator::new(Duration::from_millis(0));
        hc.mark_heartbeat_sent(20);
        std::thread::sleep(Duration::from_millis(1));
        assert!(hc.should_heartbeat(20));
    }

    #[test]
    fn test_generate_heartbeats() {
        let hc = HeartbeatConsolidator::new(Duration::from_millis(0));
        hc.register_group(1, &[10, 20, 30]);
        hc.register_group(2, &[10, 20]);

        let hbs = hc.generate_heartbeats(10);
        assert_eq!(hbs.len(), 2);

        let to_20 = hbs.iter().find(|h| h.to == 20).unwrap();
        assert_eq!(to_20.from, 10);
        assert_eq!(to_20.groups.len(), 2);

        let to_30 = hbs.iter().find(|h| h.to == 30).unwrap();
        assert_eq!(to_30.groups.len(), 1);
    }

    #[test]
    fn test_generate_heartbeats_respects_interval() {
        let hc = HeartbeatConsolidator::new(Duration::from_secs(60));
        hc.register_group(1, &[10, 20]);

        let hbs1 = hc.generate_heartbeats(10);
        assert_eq!(hbs1.len(), 1);

        let hbs2 = hc.generate_heartbeats(10);
        assert_eq!(hbs2.len(), 0);
    }

    #[test]
    fn test_total_pairs() {
        let hc = HeartbeatConsolidator::new(Duration::from_millis(200));
        hc.register_group(1, &[10, 20, 30]);
        assert_eq!(hc.total_pairs(), 6);
    }

    #[test]
    fn test_groups_between_no_relationship() {
        let hc = HeartbeatConsolidator::new(Duration::from_millis(200));
        hc.register_group(1, &[10, 20]);
        assert!(hc.groups_between(10, 99).is_empty());
    }
}
