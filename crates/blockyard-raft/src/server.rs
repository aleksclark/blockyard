//! TCP server that accepts incoming Raft RPCs and dispatches them to the
//! local Raft node.
//!
//! [`RaftRpcServer`] binds to a TCP port, accepts connections, reads
//! length-prefixed JSON frames ([`RaftRpc`]), dispatches them to the local
//! [`Raft`] instance, and writes back [`RaftRpcResponse`] frames.

use std::net::SocketAddr;
use std::sync::Arc;

use openraft::Raft;
use tokio::io::{BufReader, BufWriter};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::rpc::{RaftRpc, RaftRpcResponse, read_frame, write_frame};
use crate::typ::TypeConfig;

/// Handle used to stop a running [`RaftRpcServer`].
#[derive(Debug, Clone)]
pub struct RaftRpcServerHandle {
    shutdown_tx: watch::Sender<bool>,
}

impl RaftRpcServerHandle {
    /// Signal the server to shut down.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

/// A TCP server that accepts Raft RPC connections and dispatches them
/// to the local Raft node.
pub struct RaftRpcServer {
    raft: Arc<Raft<TypeConfig>>,
    listener: TcpListener,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl std::fmt::Debug for RaftRpcServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftRpcServer")
            .field("listener", &self.listener)
            .finish_non_exhaustive()
    }
}

impl RaftRpcServer {
    /// Bind to the given address and prepare to serve RPCs.
    pub async fn bind(raft: Arc<Raft<TypeConfig>>, addr: SocketAddr) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        info!(addr = %listener.local_addr()?, "Raft RPC server listening");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Ok(Self {
            raft,
            listener,
            shutdown_tx,
            shutdown_rx,
        })
    }

    /// Create from an existing listener (useful for tests).
    pub fn from_listener(raft: Arc<Raft<TypeConfig>>, listener: TcpListener) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            raft,
            listener,
            shutdown_tx,
            shutdown_rx,
        }
    }

    /// Get the local address the server is bound to.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Get a handle that can be used to shut down the server.
    pub fn handle(&self) -> RaftRpcServerHandle {
        RaftRpcServerHandle {
            shutdown_tx: self.shutdown_tx.clone(),
        }
    }

    /// Run the server loop, accepting connections until shutdown is signalled.
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                _ = self.shutdown_rx.changed() => {
                    info!("Raft RPC server shutting down");
                    break;
                }
                accept_result = self.listener.accept() => {
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            debug!(peer = %peer_addr, "accepted Raft RPC connection");
                            let raft = Arc::clone(&self.raft);
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(raft, stream).await {
                                    if e.kind() != std::io::ErrorKind::UnexpectedEof {
                                        warn!(peer = %peer_addr, err = %e, "RPC connection error");
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            error!(err = %e, "failed to accept connection");
                        }
                    }
                }
            }
        }
    }
}

async fn handle_connection(
    raft: Arc<Raft<TypeConfig>>,
    stream: tokio::net::TcpStream,
) -> std::io::Result<()> {
    let _ = stream.set_nodelay(true);
    let (read_half, write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut writer = BufWriter::new(write_half);

    let rpc: RaftRpc = read_frame(&mut reader).await?;
    let response = dispatch_rpc(&raft, rpc).await;
    write_frame(&mut writer, &response).await?;
    Ok(())
}

async fn dispatch_rpc(raft: &Raft<TypeConfig>, rpc: RaftRpc) -> RaftRpcResponse {
    match rpc {
        RaftRpc::AppendEntries(req) => match raft.append_entries(req).await {
            Ok(resp) => RaftRpcResponse::AppendEntries(resp),
            Err(e) => {
                warn!(err = %e, "AppendEntries dispatch failed");
                RaftRpcResponse::AppendEntries(openraft::raft::AppendEntriesResponse::HigherVote(
                    openraft::Vote::new(0, 0),
                ))
            }
        },
        RaftRpc::Vote(req) => match raft.vote(req).await {
            Ok(resp) => RaftRpcResponse::Vote(resp),
            Err(e) => {
                warn!(err = %e, "Vote dispatch failed");
                RaftRpcResponse::Vote(openraft::raft::VoteResponse {
                    vote: openraft::Vote::new(0, 0),
                    vote_granted: false,
                    last_log_id: None,
                })
            }
        },
        RaftRpc::InstallSnapshot(req) => match raft.install_snapshot(req).await {
            Ok(resp) => RaftRpcResponse::InstallSnapshot(resp),
            Err(e) => {
                warn!(err = %e, "InstallSnapshot dispatch failed");
                RaftRpcResponse::InstallSnapshot(openraft::raft::InstallSnapshotResponse {
                    vote: openraft::Vote::new(0, 0),
                })
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::{NetworkFactory, Router};
    use crate::rpc::RaftRpc;
    use crate::store::{LogStore, StateMachineStore};
    use crate::tcp_transport::{PeerRegistry, TcpNetworkConnection, TcpTransportConfig};
    use openraft::network::{RPCOption, RaftNetwork};
    use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
    use openraft::{BasicNode, Config};
    use std::sync::Arc;
    use std::time::Duration;

    async fn start_raft_node(node_id: u64) -> (Arc<Raft<TypeConfig>>, StateMachineStore) {
        let config = Arc::new(Config {
            election_timeout_min: 500,
            election_timeout_max: 1000,
            heartbeat_interval: 200,
            ..Default::default()
        });
        let router = Arc::new(parking_lot::RwLock::new(Router::new()));
        let network = NetworkFactory::new(router);
        let log_store = LogStore::new();
        let sm = StateMachineStore::new();
        let raft = Raft::new(node_id, config, network, log_store, sm.clone())
            .await
            .unwrap();
        (Arc::new(raft), sm)
    }

    #[tokio::test]
    async fn test_server_bind_and_local_addr() {
        let (raft, _sm) = start_raft_node(1).await;
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = RaftRpcServer::bind(raft, addr).await.unwrap();
        let local = server.local_addr().unwrap();
        assert_ne!(local.port(), 0);
    }

    #[tokio::test]
    async fn test_server_handle_shutdown() {
        let (raft, _sm) = start_raft_node(1).await;
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = RaftRpcServer::bind(raft, addr).await.unwrap();
        let handle = server.handle();
        let join = tokio::spawn(server.run());
        handle.shutdown();
        join.await.unwrap();
    }

    #[tokio::test]
    async fn test_server_vote_rpc_roundtrip() {
        let (raft, _sm) = start_raft_node(1).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        let server = RaftRpcServer::from_listener(Arc::clone(&raft), listener);
        let handle = server.handle();

        let join = tokio::spawn(server.run());

        let peers = PeerRegistry::new();
        peers.register(1, server_addr);
        let mut conn = TcpNetworkConnection {
            target: 1,
            peers,
            config: TcpTransportConfig {
                connect_timeout: Duration::from_secs(2),
                rpc_timeout: Duration::from_secs(2),
            },
        };

        let vote = openraft::Vote::new(1, 1);
        let req = VoteRequest {
            vote,
            last_log_id: None,
        };
        let result =
            RaftNetwork::<TypeConfig>::vote(&mut conn, req, RPCOption::new(Duration::from_secs(2)))
                .await;
        assert!(result.is_ok());

        handle.shutdown();
        join.await.unwrap();
    }

    #[tokio::test]
    async fn test_server_append_entries_rpc_roundtrip() {
        let (raft, _sm) = start_raft_node(1).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        let server = RaftRpcServer::from_listener(Arc::clone(&raft), listener);
        let handle = server.handle();

        let join = tokio::spawn(server.run());

        let peers = PeerRegistry::new();
        peers.register(1, server_addr);
        let mut conn = TcpNetworkConnection {
            target: 1,
            peers,
            config: TcpTransportConfig {
                connect_timeout: Duration::from_secs(2),
                rpc_timeout: Duration::from_secs(2),
            },
        };

        let vote = openraft::Vote::new(1, 1);
        let req = AppendEntriesRequest::<TypeConfig> {
            vote,
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        };
        let result = RaftNetwork::<TypeConfig>::append_entries(
            &mut conn,
            req,
            RPCOption::new(Duration::from_secs(2)),
        )
        .await;
        assert!(result.is_ok());

        handle.shutdown();
        join.await.unwrap();
    }

    #[tokio::test]
    async fn test_server_install_snapshot_rpc_roundtrip() {
        let (raft, _sm) = start_raft_node(1).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        let server = RaftRpcServer::from_listener(Arc::clone(&raft), listener);
        let handle = server.handle();

        let join = tokio::spawn(server.run());

        let peers = PeerRegistry::new();
        peers.register(1, server_addr);
        let mut conn = TcpNetworkConnection {
            target: 1,
            peers,
            config: TcpTransportConfig {
                connect_timeout: Duration::from_secs(2),
                rpc_timeout: Duration::from_secs(2),
            },
        };

        let vote = openraft::Vote::new(1, 1);
        let meta = openraft::SnapshotMeta {
            last_log_id: None,
            last_membership: openraft::StoredMembership::new(
                None,
                openraft::Membership::new(
                    vec![],
                    std::collections::BTreeMap::<u64, BasicNode>::new(),
                ),
            ),
            snapshot_id: "snap-1".into(),
        };
        let req = InstallSnapshotRequest::<TypeConfig> {
            vote,
            meta,
            offset: 0,
            data: vec![],
            done: true,
        };
        let result = RaftNetwork::<TypeConfig>::install_snapshot(
            &mut conn,
            req,
            RPCOption::new(Duration::from_secs(2)),
        )
        .await;
        assert!(result.is_ok());

        handle.shutdown();
        join.await.unwrap();
    }

    #[tokio::test]
    async fn test_dispatch_rpc_vote() {
        let (raft, _sm) = start_raft_node(1).await;
        let vote = openraft::Vote::new(1, 1);
        let rpc = RaftRpc::Vote(VoteRequest {
            vote,
            last_log_id: None,
        });
        let resp = dispatch_rpc(&raft, rpc).await;
        match resp {
            RaftRpcResponse::Vote(_) => {}
            _ => panic!("expected Vote response"),
        }
    }

    #[tokio::test]
    async fn test_dispatch_rpc_append_entries() {
        let (raft, _sm) = start_raft_node(1).await;
        let vote = openraft::Vote::new(1, 1);
        let rpc = RaftRpc::AppendEntries(AppendEntriesRequest {
            vote,
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        });
        let resp = dispatch_rpc(&raft, rpc).await;
        match resp {
            RaftRpcResponse::AppendEntries(_) => {}
            _ => panic!("expected AppendEntries response"),
        }
    }

    #[tokio::test]
    async fn test_dispatch_rpc_install_snapshot() {
        let (raft, _sm) = start_raft_node(1).await;
        let vote = openraft::Vote::new(1, 1);
        let meta = openraft::SnapshotMeta {
            last_log_id: None,
            last_membership: openraft::StoredMembership::new(
                None,
                openraft::Membership::new(
                    vec![],
                    std::collections::BTreeMap::<u64, BasicNode>::new(),
                ),
            ),
            snapshot_id: "snap-1".into(),
        };
        let rpc = RaftRpc::InstallSnapshot(InstallSnapshotRequest {
            vote,
            meta,
            offset: 0,
            data: vec![],
            done: true,
        });
        let resp = dispatch_rpc(&raft, rpc).await;
        match resp {
            RaftRpcResponse::InstallSnapshot(_) => {}
            _ => panic!("expected InstallSnapshot response"),
        }
    }

    #[tokio::test]
    async fn test_server_from_listener() {
        let (raft, _sm) = start_raft_node(1).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = RaftRpcServer::from_listener(raft, listener);
        assert_eq!(server.local_addr().unwrap(), addr);
    }

    #[tokio::test]
    async fn test_handle_connection_invalid_data() {
        let (raft, _sm) = start_raft_node(1).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let raft_clone = Arc::clone(&raft);
        let accept_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_connection(raft_clone, stream).await
        });

        let mut stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
        use tokio::io::AsyncWriteExt;
        let garbage = b"not json";
        let len = garbage.len() as u32;
        stream.write_all(&len.to_be_bytes()).await.unwrap();
        stream.write_all(garbage).await.unwrap();
        stream.flush().await.unwrap();
        drop(stream);

        let result = accept_handle.await.unwrap();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_server_handle_debug() {
        let (raft, _sm) = start_raft_node(1).await;
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = RaftRpcServer::bind(raft, addr).await.unwrap();
        let handle = server.handle();
        let debug = format!("{handle:?}");
        assert!(debug.contains("RaftRpcServerHandle"));
        handle.shutdown();
    }

    #[tokio::test]
    async fn test_server_handle_clone() {
        let (raft, _sm) = start_raft_node(1).await;
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = RaftRpcServer::bind(raft, addr).await.unwrap();
        let h1 = server.handle();
        let h2 = h1.clone();
        let join = tokio::spawn(server.run());
        h2.shutdown();
        join.await.unwrap();
    }
}
