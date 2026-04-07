//! Raft network transport for metadata consensus.
//!
//! Implements [`RaftNetworkFactory`] and [`RaftNetwork`] for inter-node
//! communication. This module provides an in-process router for testing
//! and a trait-based abstraction for production transport.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError};
use openraft::network::RPCOption;
use openraft::network::{RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use parking_lot::RwLock;

use crate::typ::TypeConfig;

type NodeId = u64;

/// Factory that creates network connections to Raft peers.
///
/// In production, this wraps a real TCP/gRPC transport.
/// For testing, it uses a shared in-memory router.
#[derive(Debug, Clone)]
pub struct NetworkFactory {
    router: Arc<RwLock<Router>>,
}

/// In-memory router for testing: maps node IDs to Raft instances.
#[derive(Default)]
pub struct Router {
    targets: BTreeMap<NodeId, openraft::Raft<TypeConfig>>,
}

impl std::fmt::Debug for Router {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Router")
            .field("node_count", &self.targets.len())
            .finish()
    }
}

impl Router {
    pub fn new() -> Self {
        Self {
            targets: BTreeMap::new(),
        }
    }

    pub fn add_node(&mut self, id: NodeId, raft: openraft::Raft<TypeConfig>) {
        self.targets.insert(id, raft);
    }

    pub fn remove_node(&mut self, id: NodeId) {
        self.targets.remove(&id);
    }

    pub fn get(&self, id: NodeId) -> Option<openraft::Raft<TypeConfig>> {
        self.targets.get(&id).cloned()
    }
}

impl NetworkFactory {
    pub fn new(router: Arc<RwLock<Router>>) -> Self {
        Self { router }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        NetworkConnection {
            target,
            router: Arc::clone(&self.router),
        }
    }
}

/// A single connection to a target Raft node.
#[derive(Debug)]
pub struct NetworkConnection {
    target: NodeId,
    router: Arc<RwLock<Router>>,
}

impl NetworkConnection {
    #[allow(clippy::result_large_err)]
    fn get_target(
        &self,
    ) -> Result<openraft::Raft<TypeConfig>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.router.read().get(self.target).ok_or_else(|| {
            RPCError::Network(openraft::error::NetworkError::new(&io::Error::new(
                io::ErrorKind::NotFound,
                format!("node {} not found in router", self.target),
            )))
        })
    }
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let raft = self.get_target()?;
        raft.append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let raft = self.get_target().map_err(|e| match e {
            RPCError::Network(n) => RPCError::Network(n),
            RPCError::RemoteError(r) => RPCError::RemoteError(RemoteError::new(
                r.target,
                RaftError::Fatal(openraft::error::Fatal::Stopped),
            )),
            RPCError::PayloadTooLarge(p) => RPCError::PayloadTooLarge(p),
            RPCError::Unreachable(u) => RPCError::Unreachable(u),
            RPCError::Timeout(t) => RPCError::Timeout(t),
        })?;
        raft.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let raft = self.get_target()?;
        raft.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_router_add_remove() {
        let mut router = Router::new();
        assert!(router.get(1).is_none());
        router.remove_node(1);
    }

    #[test]
    fn test_router_default() {
        let router = Router::default();
        assert!(router.targets.is_empty());
    }

    #[test]
    fn test_network_factory_creation() {
        let router = Arc::new(RwLock::new(Router::new()));
        let _factory = NetworkFactory::new(router);
    }

    #[tokio::test]
    async fn test_network_connection_target_not_found() {
        let router = Arc::new(RwLock::new(Router::new()));
        let conn = NetworkConnection { target: 99, router };
        let result = conn.get_target();
        assert!(result.is_err());
    }
}
