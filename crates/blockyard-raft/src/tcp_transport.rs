//! TCP transport for Raft RPCs.
//!
//! [`TcpNetworkFactory`] implements [`RaftNetworkFactory`] to create
//! [`TcpNetworkConnection`] instances that serialize Raft RPCs as
//! length-prefixed JSON over TCP.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::RPCOption;
use openraft::network::{RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use parking_lot::RwLock;
use tokio::io::{BufReader, BufWriter};
use tokio::net::TcpStream;
use tracing::{debug, warn};

use crate::rpc::{RaftRpc, RaftRpcResponse, read_frame, write_frame};
use crate::typ::TypeConfig;

type NodeId = u64;

/// Maps Raft NodeId → SocketAddr for peer discovery.
#[derive(Debug, Clone, Default)]
pub struct PeerRegistry {
    peers: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,
}

impl PeerRegistry {
    pub fn new() -> Self {
        Self {
            peers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn register(&self, node_id: NodeId, addr: SocketAddr) {
        self.peers.write().insert(node_id, addr);
    }

    pub fn unregister(&self, node_id: NodeId) {
        self.peers.write().remove(&node_id);
    }

    pub fn get(&self, node_id: NodeId) -> Option<SocketAddr> {
        self.peers.read().get(&node_id).copied()
    }

    pub fn list(&self) -> Vec<(NodeId, SocketAddr)> {
        self.peers.read().iter().map(|(&k, &v)| (k, v)).collect()
    }
}

/// Configuration for TCP transport timeouts.
#[derive(Debug, Clone)]
pub struct TcpTransportConfig {
    pub connect_timeout: Duration,
    pub rpc_timeout: Duration,
}

impl Default for TcpTransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            rpc_timeout: Duration::from_secs(5),
        }
    }
}

/// Factory that creates TCP-backed network connections to Raft peers.
#[derive(Debug, Clone)]
pub struct TcpNetworkFactory {
    peers: PeerRegistry,
    config: TcpTransportConfig,
}

impl TcpNetworkFactory {
    pub fn new(peers: PeerRegistry, config: TcpTransportConfig) -> Self {
        Self { peers, config }
    }
}

impl RaftNetworkFactory<TypeConfig> for TcpNetworkFactory {
    type Network = TcpNetworkConnection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        TcpNetworkConnection {
            target,
            peers: self.peers.clone(),
            config: self.config.clone(),
        }
    }
}

/// A single connection to a target Raft node over TCP.
///
/// Each RPC opens a fresh TCP connection (simple and robust).
/// Connection pooling can be added later as an optimization.
#[derive(Debug)]
pub struct TcpNetworkConnection {
    pub(crate) target: NodeId,
    pub(crate) peers: PeerRegistry,
    pub(crate) config: TcpTransportConfig,
}

impl TcpNetworkConnection {
    async fn send_rpc(&self, rpc: RaftRpc) -> std::io::Result<RaftRpcResponse> {
        let addr = self.peers.get(self.target).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no address registered for node {}", self.target),
            )
        })?;

        let stream = tokio::time::timeout(self.config.connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("connect timeout to node {} at {}", self.target, addr),
                )
            })??;

        stream.set_nodelay(true)?;

        let (read_half, write_half) = stream.into_split();
        let mut writer = BufWriter::new(write_half);
        let mut reader = BufReader::new(read_half);

        tokio::time::timeout(self.config.rpc_timeout, async {
            write_frame(&mut writer, &rpc).await?;
            read_frame(&mut reader).await
        })
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("RPC timeout to node {} at {}", self.target, addr),
            )
        })?
    }

    fn io_to_network_err<E: std::error::Error>(
        &self,
        err: std::io::Error,
    ) -> RPCError<NodeId, BasicNode, RaftError<NodeId, E>> {
        warn!(target = self.target, err = %err, "TCP RPC failed");
        RPCError::Network(openraft::error::NetworkError::new(&err))
    }
}

impl RaftNetwork<TypeConfig> for TcpNetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        debug!(target_node = self.target, "sending AppendEntries");
        let resp = self
            .send_rpc(RaftRpc::AppendEntries(rpc))
            .await
            .map_err(|e| self.io_to_network_err(e))?;
        match resp {
            RaftRpcResponse::AppendEntries(r) => Ok(r),
            other => Err(RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unexpected response variant: {other:?}"),
                ),
            ))),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        debug!(target_node = self.target, "sending InstallSnapshot");
        let resp = self
            .send_rpc(RaftRpc::InstallSnapshot(rpc))
            .await
            .map_err(|e| self.io_to_network_err(e))?;
        match resp {
            RaftRpcResponse::InstallSnapshot(r) => Ok(r),
            other => Err(RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unexpected response variant: {other:?}"),
                ),
            ))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        debug!(target_node = self.target, "sending Vote");
        let resp = self
            .send_rpc(RaftRpc::Vote(rpc))
            .await
            .map_err(|e| self.io_to_network_err(e))?;
        match resp {
            RaftRpcResponse::Vote(r) => Ok(r),
            other => Err(RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unexpected response variant: {other:?}"),
                ),
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_registry_register_get() {
        let reg = PeerRegistry::new();
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        reg.register(1, addr);
        assert_eq!(reg.get(1), Some(addr));
        assert_eq!(reg.get(2), None);
    }

    #[test]
    fn test_peer_registry_unregister() {
        let reg = PeerRegistry::new();
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        reg.register(1, addr);
        reg.unregister(1);
        assert_eq!(reg.get(1), None);
    }

    #[test]
    fn test_peer_registry_list() {
        let reg = PeerRegistry::new();
        let a1: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let a2: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        reg.register(1, a1);
        reg.register(2, a2);
        let list = reg.list();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_peer_registry_default() {
        let reg = PeerRegistry::default();
        assert!(reg.list().is_empty());
    }

    #[test]
    fn test_tcp_transport_config_default() {
        let cfg = TcpTransportConfig::default();
        assert_eq!(cfg.connect_timeout, Duration::from_secs(5));
        assert_eq!(cfg.rpc_timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_tcp_network_factory_debug() {
        let peers = PeerRegistry::new();
        let factory = TcpNetworkFactory::new(peers, TcpTransportConfig::default());
        let debug = format!("{factory:?}");
        assert!(debug.contains("TcpNetworkFactory"));
    }

    #[tokio::test]
    async fn test_tcp_network_factory_new_client() {
        let peers = PeerRegistry::new();
        let mut factory = TcpNetworkFactory::new(peers, TcpTransportConfig::default());
        let conn = factory.new_client(1, &BasicNode::default()).await;
        assert_eq!(conn.target, 1);
    }

    #[tokio::test]
    async fn test_tcp_connection_send_rpc_no_peer() {
        let peers = PeerRegistry::new();
        let conn = TcpNetworkConnection {
            target: 99,
            peers,
            config: TcpTransportConfig::default(),
        };
        let vote = openraft::Vote::new(1, 1);
        let rpc = RaftRpc::Vote(VoteRequest {
            vote,
            last_log_id: None,
        });
        let result = conn.send_rpc(rpc).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn test_tcp_connection_send_rpc_connect_refused() {
        let peers = PeerRegistry::new();
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        peers.register(1, addr);
        let conn = TcpNetworkConnection {
            target: 1,
            peers,
            config: TcpTransportConfig {
                connect_timeout: Duration::from_millis(100),
                rpc_timeout: Duration::from_millis(100),
            },
        };
        let vote = openraft::Vote::new(1, 1);
        let rpc = RaftRpc::Vote(VoteRequest {
            vote,
            last_log_id: None,
        });
        let result = conn.send_rpc(rpc).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_tcp_connection_vote_no_peer() {
        let peers = PeerRegistry::new();
        let mut conn = TcpNetworkConnection {
            target: 99,
            peers,
            config: TcpTransportConfig::default(),
        };
        let vote = openraft::Vote::new(1, 1);
        let rpc = VoteRequest {
            vote,
            last_log_id: None,
        };
        let result =
            RaftNetwork::<TypeConfig>::vote(&mut conn, rpc, RPCOption::new(Duration::from_secs(1)))
                .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_tcp_connection_append_entries_no_peer() {
        let peers = PeerRegistry::new();
        let mut conn = TcpNetworkConnection {
            target: 99,
            peers,
            config: TcpTransportConfig::default(),
        };
        let vote = openraft::Vote::new(1, 1);
        let rpc = AppendEntriesRequest::<TypeConfig> {
            vote,
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        };
        let result = RaftNetwork::<TypeConfig>::append_entries(
            &mut conn,
            rpc,
            RPCOption::new(Duration::from_secs(1)),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_tcp_connection_install_snapshot_no_peer() {
        let peers = PeerRegistry::new();
        let mut conn = TcpNetworkConnection {
            target: 99,
            peers,
            config: TcpTransportConfig::default(),
        };
        let vote = openraft::Vote::new(1, 1);
        let meta = openraft::SnapshotMeta {
            last_log_id: None,
            last_membership: openraft::StoredMembership::new(
                None,
                openraft::Membership::new(
                    vec![],
                    std::collections::BTreeMap::<u64, openraft::BasicNode>::new(),
                ),
            ),
            snapshot_id: "snap-1".into(),
        };
        let rpc = InstallSnapshotRequest::<TypeConfig> {
            vote,
            meta,
            offset: 0,
            data: vec![],
            done: true,
        };
        let result = RaftNetwork::<TypeConfig>::install_snapshot(
            &mut conn,
            rpc,
            RPCOption::new(Duration::from_secs(1)),
        )
        .await;
        assert!(result.is_err());
    }

    #[test]
    fn test_peer_registry_overwrite() {
        let reg = PeerRegistry::new();
        let a1: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let a2: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        reg.register(1, a1);
        reg.register(1, a2);
        assert_eq!(reg.get(1), Some(a2));
    }

    #[test]
    fn test_peer_registry_unregister_nonexistent() {
        let reg = PeerRegistry::new();
        reg.unregister(42);
        assert!(reg.list().is_empty());
    }
}
