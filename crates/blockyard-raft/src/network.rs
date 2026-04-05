//! gRPC client for sending Raft RPCs, consolidated heartbeats, and cluster
//! operations to remote peers.
//!
//! [`RaftNetwork`] maintains a pool of lazily-connected gRPC channels keyed by
//! `NodeId`. All three proto services (Raft, Heartbeat, Cluster) share the same
//! underlying HTTP/2 connection to a given peer.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use tonic::transport::Channel;
use tracing::{debug, instrument, warn};

use blockyard_common::types::NodeId;

use crate::proto;

// ---------------------------------------------------------------------------
// Connection pool
// ---------------------------------------------------------------------------

/// Maps a `NodeId` → gRPC `Channel` (lazily connected).
#[derive(Debug, Clone, Default)]
struct ConnectionPool {
    channels: Arc<RwLock<HashMap<NodeId, Channel>>>,
}

impl ConnectionPool {
    /// Return an existing channel or connect to `addr` (e.g. `http://10.0.0.2:7400`).
    async fn get_or_connect(
        &self,
        node_id: NodeId,
        addr: &str,
    ) -> blockyard_common::Result<Channel> {
        // Fast path: already connected.
        if let Some(ch) = self.channels.read().get(&node_id).cloned() {
            return Ok(ch);
        }

        // Slow path: create a new channel.
        let endpoint = Channel::from_shared(addr.to_owned()).map_err(|e| {
            blockyard_common::Error::Raft(format!("invalid endpoint for node {node_id}: {e}"))
        })?;

        let channel = endpoint.connect().await.map_err(|e| {
            blockyard_common::Error::Raft(format!(
                "failed to connect to node {node_id} at {addr}: {e}"
            ))
        })?;

        self.channels.write().insert(node_id, channel.clone());
        debug!(node_id, addr, "established gRPC channel");
        Ok(channel)
    }

    /// Explicitly remove a channel (e.g. when a node leaves the cluster).
    fn remove(&self, node_id: NodeId) {
        self.channels.write().remove(&node_id);
    }
}

// ---------------------------------------------------------------------------
// RaftNetwork — the public client API
// ---------------------------------------------------------------------------

/// Network transport that sends Raft RPCs over gRPC.
///
/// Holds a connection pool so that a single `RaftNetwork` can talk to every
/// peer in the cluster. It is `Clone + Send + Sync` and intended to be shared
/// across all Raft groups running on a node.
#[derive(Debug, Clone)]
pub struct RaftNetwork {
    pool: ConnectionPool,
    /// Maps NodeId → gRPC address string. Updated externally when membership
    /// changes (node join / leave).
    peer_addrs: Arc<RwLock<HashMap<NodeId, String>>>,
}

impl RaftNetwork {
    /// Create a new, empty `RaftNetwork`.
    pub fn new() -> Self {
        Self {
            pool: ConnectionPool::default(),
            peer_addrs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register (or update) the gRPC address for a peer node.
    pub fn add_peer(&self, node_id: NodeId, addr: String) {
        debug!(node_id, addr = %addr, "registered peer address");
        self.peer_addrs.write().insert(node_id, addr);
    }

    /// Deregister a peer and drop its cached connection.
    pub fn remove_peer(&self, node_id: NodeId) {
        self.peer_addrs.write().remove(&node_id);
        self.pool.remove(node_id);
        debug!(node_id, "removed peer");
    }

    /// Resolve and connect (or reuse) a channel for the given node.
    async fn channel_for(&self, node_id: NodeId) -> blockyard_common::Result<Channel> {
        let addr = self
            .peer_addrs
            .read()
            .get(&node_id)
            .cloned()
            .ok_or_else(|| {
                blockyard_common::Error::Raft(format!("no address registered for node {node_id}"))
            })?;
        self.pool.get_or_connect(node_id, &addr).await
    }

    // ------------------------------------------------------------------
    // Raft RPCs
    // ------------------------------------------------------------------

    /// Send an `AppendEntries` RPC to a peer.
    #[instrument(skip(self, request), fields(group_id = request.group_id, target = target))]
    pub async fn send_append_entries(
        &self,
        target: NodeId,
        request: proto::AppendEntriesRequest,
    ) -> blockyard_common::Result<proto::AppendEntriesResponse> {
        let channel = self.channel_for(target).await?;
        let mut client = proto::raft_service_client::RaftServiceClient::new(channel);

        let resp = client.append_entries(request).await.map_err(|e| {
            warn!(target, error = %e, "append_entries RPC failed");
            blockyard_common::Error::Raft(format!("append_entries to node {target}: {e}"))
        })?;

        Ok(resp.into_inner())
    }

    /// Send an `InstallSnapshot` RPC to a peer.
    #[instrument(skip(self, request), fields(group_id = request.group_id, target = target))]
    pub async fn send_install_snapshot(
        &self,
        target: NodeId,
        request: proto::InstallSnapshotRequest,
    ) -> blockyard_common::Result<proto::InstallSnapshotResponse> {
        let channel = self.channel_for(target).await?;
        let mut client = proto::raft_service_client::RaftServiceClient::new(channel);

        let resp = client.install_snapshot(request).await.map_err(|e| {
            warn!(target, error = %e, "install_snapshot RPC failed");
            blockyard_common::Error::Raft(format!("install_snapshot to node {target}: {e}"))
        })?;

        Ok(resp.into_inner())
    }

    /// Send a `RequestVote` RPC to a peer.
    #[instrument(skip(self, request), fields(group_id = request.group_id, target = target))]
    pub async fn send_vote(
        &self,
        target: NodeId,
        request: proto::VoteRequest,
    ) -> blockyard_common::Result<proto::VoteResponse> {
        let channel = self.channel_for(target).await?;
        let mut client = proto::raft_service_client::RaftServiceClient::new(channel);

        let resp = client.request_vote(request).await.map_err(|e| {
            warn!(target, error = %e, "request_vote RPC failed");
            blockyard_common::Error::Raft(format!("request_vote to node {target}: {e}"))
        })?;

        Ok(resp.into_inner())
    }

    // ------------------------------------------------------------------
    // Consolidated heartbeat
    // ------------------------------------------------------------------

    /// Send a consolidated heartbeat covering multiple Raft groups to one peer.
    #[instrument(skip(self, request), fields(target = target, groups = request.heartbeats.len()))]
    pub async fn send_heartbeat(
        &self,
        target: NodeId,
        request: proto::ConsolidatedHeartbeatRequest,
    ) -> blockyard_common::Result<proto::ConsolidatedHeartbeatResponse> {
        let channel = self.channel_for(target).await?;
        let mut client = proto::heartbeat_service_client::HeartbeatServiceClient::new(channel);

        let resp = client.consolidated_heartbeat(request).await.map_err(|e| {
            warn!(target, error = %e, "consolidated_heartbeat RPC failed");
            blockyard_common::Error::Raft(format!("consolidated_heartbeat to node {target}: {e}"))
        })?;

        Ok(resp.into_inner())
    }

    // ------------------------------------------------------------------
    // Cluster operations (client → leader forwarding)
    // ------------------------------------------------------------------

    /// Forward a write proposal to the leader of `group_id` at `target`.
    #[instrument(skip(self, request), fields(group_id = request.group_id, target = target))]
    pub async fn send_forward_proposal(
        &self,
        target: NodeId,
        request: proto::ForwardProposalRequest,
    ) -> blockyard_common::Result<proto::ForwardProposalResponse> {
        let channel = self.channel_for(target).await?;
        let mut client = proto::cluster_service_client::ClusterServiceClient::new(channel);

        let resp = client.forward_proposal(request).await.map_err(|e| {
            warn!(target, error = %e, "forward_proposal RPC failed");
            blockyard_common::Error::Raft(format!("forward_proposal to node {target}: {e}"))
        })?;

        Ok(resp.into_inner())
    }

    /// Query the state machine of `group_id` on `target`.
    #[instrument(skip(self, request), fields(group_id = request.group_id, target = target))]
    pub async fn send_get_state(
        &self,
        target: NodeId,
        request: proto::GetStateRequest,
    ) -> blockyard_common::Result<proto::GetStateResponse> {
        let channel = self.channel_for(target).await?;
        let mut client = proto::cluster_service_client::ClusterServiceClient::new(channel);

        let resp = client.get_state(request).await.map_err(|e| {
            warn!(target, error = %e, "get_state RPC failed");
            blockyard_common::Error::Raft(format!("get_state to node {target}: {e}"))
        })?;

        Ok(resp.into_inner())
    }
}

impl Default for RaftNetwork {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto;

    // -- Unit tests for RaftNetwork peer management -----------------------

    #[test]
    fn test_raft_network_new() {
        let net = RaftNetwork::new();
        assert!(net.peer_addrs.read().is_empty());
    }

    #[test]
    fn test_add_and_remove_peer() {
        let net = RaftNetwork::new();
        net.add_peer(1, "http://10.0.0.1:7400".into());
        net.add_peer(2, "http://10.0.0.2:7400".into());
        assert_eq!(net.peer_addrs.read().len(), 2);

        net.remove_peer(1);
        assert_eq!(net.peer_addrs.read().len(), 1);
        assert!(!net.peer_addrs.read().contains_key(&1));
        assert!(net.peer_addrs.read().contains_key(&2));
    }

    #[test]
    fn test_update_peer_addr() {
        let net = RaftNetwork::new();
        net.add_peer(1, "http://10.0.0.1:7400".into());
        net.add_peer(1, "http://10.0.0.1:8400".into());
        assert_eq!(
            net.peer_addrs.read().get(&1).unwrap(),
            "http://10.0.0.1:8400"
        );
    }

    #[tokio::test]
    async fn test_channel_for_unknown_peer() {
        let net = RaftNetwork::new();
        let result = net.channel_for(42).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("no address registered"), "got: {err}");
    }

    // -- Proto message roundtrip tests -----------------------------------

    #[test]
    fn test_append_entries_request_roundtrip() {
        use prost::Message;

        let entry = proto::LogEntry {
            term: 3,
            index: 7,
            data: serde_json::to_vec(&crate::types::RaftRequest::VolumeDelete {
                name: "vol-x".into(),
            })
            .unwrap(),
        };

        let req = proto::AppendEntriesRequest {
            group_id: 42,
            leader_id: 1,
            term: 3,
            prev_log_index: 6,
            prev_log_term: 2,
            leader_commit: 5,
            entries: vec![entry],
        };

        let bytes = req.encode_to_vec();
        let decoded = proto::AppendEntriesRequest::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded, req);

        // Verify the inner RaftRequest survives the trip.
        let inner: crate::types::RaftRequest =
            serde_json::from_slice(&decoded.entries[0].data).unwrap();
        assert_eq!(
            inner,
            crate::types::RaftRequest::VolumeDelete {
                name: "vol-x".into()
            }
        );
    }

    #[test]
    fn test_install_snapshot_roundtrip() {
        use prost::Message;

        let snap_data = serde_json::to_vec(&crate::state_machine::AppState::default()).unwrap();
        let req = proto::InstallSnapshotRequest {
            group_id: 1,
            leader_id: 2,
            term: 5,
            last_included_index: 100,
            last_included_term: 4,
            data: snap_data.clone(),
        };

        let bytes = req.encode_to_vec();
        let decoded = proto::InstallSnapshotRequest::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded, req);

        let state: crate::state_machine::AppState = serde_json::from_slice(&decoded.data).unwrap();
        assert!(state.volumes.is_empty());
    }

    #[test]
    fn test_vote_request_roundtrip() {
        use prost::Message;

        let req = proto::VoteRequest {
            group_id: 10,
            candidate_id: 3,
            term: 7,
            last_log_index: 50,
            last_log_term: 6,
            pre_vote: true,
        };

        let bytes = req.encode_to_vec();
        let decoded = proto::VoteRequest::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn test_consolidated_heartbeat_roundtrip() {
        use prost::Message;

        let req = proto::ConsolidatedHeartbeatRequest {
            from_node: 1,
            to_node: 2,
            heartbeats: vec![
                proto::GroupHeartbeat {
                    group_id: 10,
                    term: 3,
                    commit_index: 100,
                },
                proto::GroupHeartbeat {
                    group_id: 20,
                    term: 5,
                    commit_index: 42,
                },
            ],
        };

        let bytes = req.encode_to_vec();
        let decoded = proto::ConsolidatedHeartbeatRequest::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded, req);
        assert_eq!(decoded.heartbeats.len(), 2);
    }

    #[test]
    fn test_forward_proposal_roundtrip() {
        use prost::Message;

        let payload = serde_json::to_vec(&crate::types::RaftRequest::VolumeCreate {
            name: "vol-1".into(),
            size_bytes: 1024,
            replicas: 3,
        })
        .unwrap();

        let req = proto::ForwardProposalRequest {
            group_id: 1,
            payload: payload.clone(),
        };

        let bytes = req.encode_to_vec();
        let decoded = proto::ForwardProposalRequest::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded, req);

        let inner: crate::types::RaftRequest = serde_json::from_slice(&decoded.payload).unwrap();
        assert!(matches!(
            inner,
            crate::types::RaftRequest::VolumeCreate { .. }
        ));
    }

    #[test]
    fn test_get_state_response_roundtrip() {
        use prost::Message;

        let mut state = crate::state_machine::AppState::default();
        state.applied_index = 42;

        let resp = proto::GetStateResponse {
            success: true,
            error: String::new(),
            state: serde_json::to_vec(&state).unwrap(),
        };

        let bytes = resp.encode_to_vec();
        let decoded = proto::GetStateResponse::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded, resp);

        let s: crate::state_machine::AppState = serde_json::from_slice(&decoded.state).unwrap();
        assert_eq!(s.applied_index, 42);
    }

    // -- Integration: real client ↔ server roundtrip ---------------------

    #[tokio::test]
    async fn test_client_server_append_entries() {
        use crate::grpc_server::BlockyardGrpcServer;
        use crate::multiraft::MultiRaft;
        use std::net::SocketAddr;

        let multiraft = Arc::new(MultiRaft::new(1));
        multiraft.create_group(100).unwrap();

        let server = BlockyardGrpcServer::new(multiraft);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let bound_addr = listener.local_addr().unwrap();

        // Start server in background.
        let server_handle = tokio::spawn(async move {
            server.serve_with_listener(listener).await.unwrap();
        });

        // Give the server a moment to start.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let net = RaftNetwork::new();
        net.add_peer(1, format!("http://{bound_addr}"));

        let req = proto::AppendEntriesRequest {
            group_id: 100,
            leader_id: 1,
            term: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: 0,
            entries: vec![],
        };

        let resp = net.send_append_entries(1, req).await.unwrap();
        assert!(resp.success);

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_client_server_vote() {
        use crate::grpc_server::BlockyardGrpcServer;
        use crate::multiraft::MultiRaft;
        use std::net::SocketAddr;

        let multiraft = Arc::new(MultiRaft::new(1));
        multiraft.create_group(100).unwrap();

        let server = BlockyardGrpcServer::new(multiraft);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let bound_addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            server.serve_with_listener(listener).await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let net = RaftNetwork::new();
        net.add_peer(1, format!("http://{bound_addr}"));

        let req = proto::VoteRequest {
            group_id: 100,
            candidate_id: 2,
            term: 2,
            last_log_index: 0,
            last_log_term: 0,
            pre_vote: false,
        };

        let resp = net.send_vote(1, req).await.unwrap();
        // The basic server implementation grants votes.
        assert!(resp.vote_granted);

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_client_server_heartbeat() {
        use crate::grpc_server::BlockyardGrpcServer;
        use crate::multiraft::MultiRaft;
        use std::net::SocketAddr;

        let multiraft = Arc::new(MultiRaft::new(1));
        multiraft.create_group(10).unwrap();

        let server = BlockyardGrpcServer::new(multiraft);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let bound_addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            server.serve_with_listener(listener).await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let net = RaftNetwork::new();
        net.add_peer(1, format!("http://{bound_addr}"));

        let req = proto::ConsolidatedHeartbeatRequest {
            from_node: 2,
            to_node: 1,
            heartbeats: vec![proto::GroupHeartbeat {
                group_id: 10,
                term: 1,
                commit_index: 0,
            }],
        };

        let resp = net.send_heartbeat(1, req).await.unwrap();
        assert!(resp.success);

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_client_server_forward_proposal() {
        use crate::grpc_server::BlockyardGrpcServer;
        use crate::multiraft::MultiRaft;
        use std::net::SocketAddr;

        let multiraft = Arc::new(MultiRaft::new(1));
        multiraft.create_group(100).unwrap();

        let server = BlockyardGrpcServer::new(multiraft);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let bound_addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            server.serve_with_listener(listener).await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let net = RaftNetwork::new();
        net.add_peer(1, format!("http://{bound_addr}"));

        let payload = serde_json::to_vec(&crate::types::RaftRequest::VolumeCreate {
            name: "test-vol".into(),
            size_bytes: 2048,
            replicas: 3,
        })
        .unwrap();

        let req = proto::ForwardProposalRequest {
            group_id: 100,
            payload,
        };

        let resp = net.send_forward_proposal(1, req).await.unwrap();
        assert!(resp.success, "error: {}", resp.error);

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_client_server_get_state() {
        use crate::grpc_server::BlockyardGrpcServer;
        use crate::multiraft::MultiRaft;
        use std::net::SocketAddr;

        let multiraft = Arc::new(MultiRaft::new(1));
        multiraft.create_group(100).unwrap();

        // Apply something so state is non-empty.
        multiraft
            .propose(
                100,
                &crate::types::RaftRequest::NodeRegister {
                    node_id: 99,
                    addr: "10.0.0.99:7400".into(),
                },
            )
            .unwrap();

        let server = BlockyardGrpcServer::new(multiraft);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let bound_addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            server.serve_with_listener(listener).await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let net = RaftNetwork::new();
        net.add_peer(1, format!("http://{bound_addr}"));

        let req = proto::GetStateRequest { group_id: 100 };
        let resp = net.send_get_state(1, req).await.unwrap();
        assert!(resp.success, "error: {}", resp.error);

        let state: crate::state_machine::AppState = serde_json::from_slice(&resp.state).unwrap();
        assert!(state.nodes.contains_key(&99));

        server_handle.abort();
    }
}
