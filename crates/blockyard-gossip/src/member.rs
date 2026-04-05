use blockyard_common::types::{NodeId, NodeInfo, NodeState};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MemberList {
    inner: Arc<RwLock<HashMap<NodeId, NodeInfo>>>,
}

impl MemberList {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn upsert(&self, info: NodeInfo) {
        self.inner.write().insert(info.id, info);
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
            node.state = state;
        }
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for MemberList {
    fn default() -> Self {
        Self::new()
    }
}
