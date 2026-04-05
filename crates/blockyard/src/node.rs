use blockyard_common::config::NodeConfig;
use blockyard_gossip::SwimGossip;
use blockyard_gossip::transport::UdpTransport;
use blockyard_protocol::server::{ProtocolServer, RequestHandler};
use blockyard_protocol::wire::Status as WireStatus;
use blockyard_raft::grpc_server::BlockyardGrpcServer;
use blockyard_raft::multiraft::MultiRaft;
use blockyard_raft::network::RaftNetwork;
use blockyard_raft::types::RaftRequest;
use blockyard_storage::backend::MemoryBackend;
use blockyard_storage::{HealthMonitor, PlacementEngine};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

use blockyard_common::types::{NodeId, NodeInfo, NodeState, ZfsHealthState};

// ---------------------------------------------------------------------------
// BlockHandler — bridges the protocol server to raft + storage backend
// ---------------------------------------------------------------------------

struct BlockHandler {
    raft: Arc<MultiRaft>,
    #[allow(dead_code)]
    backend: Arc<MemoryBackend>,
}

impl RequestHandler for BlockHandler {
    async fn handle_write(
        &self,
        volume_id: u64,
        offset: u64,
        data: Bytes,
    ) -> Result<(), WireStatus> {
        // Propose the write through Raft for replication.
        let req = RaftRequest::Write {
            volume_id,
            offset,
            data: data.to_vec(),
        };
        let meta_group_id = blockyard_raft::meta_group::MetaGroup::group_id();
        self.raft.propose(meta_group_id, &req).map_err(|e| {
            warn!(error = %e, "raft propose failed for write");
            WireStatus::IoError
        })?;

        Ok(())
    }

    async fn handle_read(
        &self,
        _volume_id: u64,
        _offset: u64,
        length: u32,
    ) -> Result<Bytes, WireStatus> {
        // Read from the in-memory backend. The MemoryBackend doesn't have
        // block-level read; return zeroes for now (matches memory-backed
        // semantics where unwritten regions are zero).
        Ok(Bytes::from(vec![0u8; length as usize]))
    }

    async fn handle_flush(&self, _volume_id: u64) -> Result<(), WireStatus> {
        // No-op for the memory backend.
        Ok(())
    }

    async fn handle_trim(
        &self,
        _volume_id: u64,
        _offset: u64,
        _length: u32,
    ) -> Result<(), WireStatus> {
        // No-op for the memory backend.
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// BlockyardNode
// ---------------------------------------------------------------------------

pub struct BlockyardNode {
    config: NodeConfig,
    node_id: NodeId,
    node_name: String,
    raft: Arc<MultiRaft>,
    #[allow(dead_code)]
    placement: PlacementEngine,
    health_monitor: Arc<HealthMonitor>,
}

impl BlockyardNode {
    pub fn new(config: NodeConfig) -> blockyard_common::Result<Self> {
        let node_name = config.node.name.clone().unwrap_or_else(|| {
            hostname::get()
                .map(|h| h.to_string_lossy().into_owned())
                .unwrap_or_else(|_| "unknown".to_string())
        });

        let node_id = {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            node_name.hash(&mut hasher);
            config.node.listen.hash(&mut hasher);
            hasher.finish()
        };

        info!(name = %node_name, id = node_id, "initializing blockyard node");

        let raft = Arc::new(MultiRaft::new(node_id));
        let placement = PlacementEngine::new();
        let health_monitor = Arc::new(HealthMonitor::new(Duration::from_secs(30)));

        Ok(Self {
            config,
            node_id,
            node_name,
            raft,
            placement,
            health_monitor,
        })
    }

    pub async fn start(&mut self) -> blockyard_common::Result<()> {
        info!(
            listen = %self.config.node.listen,
            pool = %self.config.storage.zfs_pool,
            node_id = self.node_id,
            "starting blockyard node"
        );

        // 1. Create meta Raft group
        let meta_group_id = blockyard_raft::meta_group::MetaGroup::group_id();
        self.raft.create_group(meta_group_id)?;
        info!("created meta raft group");

        // 2. Start Raft engine
        self.raft.start().await?;

        // 3. Register local node in Raft state machine
        let register_req = RaftRequest::NodeRegister {
            node_id: self.node_id,
            addr: self.config.node.listen.to_string(),
        };
        self.raft.propose(meta_group_id, &register_req)?;
        info!(node_id = self.node_id, "registered local node in raft");

        // 4. Start SWIM gossip
        let transport = UdpTransport::bind(self.config.node.listen).await?;
        let local_info = NodeInfo {
            id: self.node_id,
            name: self.node_name.clone(),
            addr: self.config.node.listen,
            data_addr: self.config.node.data_listen,
            tags: self.config.tags.clone(),
            state: NodeState::Healthy,
            zfs_health: ZfsHealthState::Online,
            capacity_bytes: 0,
            used_bytes: 0,
            incarnation: 1,
        };

        let gossip = SwimGossip::new(
            local_info,
            self.config.cluster.seeds.clone(),
            self.config.gossip.clone(),
            transport,
        );
        gossip.start().await?;

        // 5. Create shared MemoryBackend
        let backend = Arc::new(MemoryBackend::new(
            self.config.storage.zfs_pool.clone(),
            1024 * 1024 * 1024 * 100,
        ));

        // 6. Create BlockHandler
        let handler = BlockHandler {
            raft: Arc::clone(&self.raft),
            backend: Arc::clone(&backend),
        };

        // 7. Spawn ProtocolServer on config.node.listen
        let protocol_listen = self.config.node.listen;
        let protocol_server = ProtocolServer::new(protocol_listen, handler);
        tokio::spawn(async move {
            if let Err(e) = protocol_server.run().await {
                error!(error = %e, "protocol server failed");
            }
        });
        info!(addr = %protocol_listen, "spawned protocol server");

        // 8. Spawn BlockyardGrpcServer on config.node.data_listen
        let grpc_server = BlockyardGrpcServer::new(Arc::clone(&self.raft));
        let grpc_listen = self.config.node.data_listen;
        tokio::spawn(async move {
            if let Err(e) = grpc_server.serve(grpc_listen).await {
                error!(error = %e, "gRPC server failed");
            }
        });
        info!(addr = %grpc_listen, "spawned gRPC server");

        // 9. Create RaftNetwork and spawn peer-sync loop from gossip
        let raft_network = RaftNetwork::new();
        let members = gossip.members().clone();
        let local_id = self.node_id;
        tokio::spawn(async move {
            loop {
                let nodes = members.healthy_nodes();
                for node in &nodes {
                    if node.id != local_id {
                        raft_network.add_peer(node.id, format!("http://{}", node.data_addr));
                    }
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
        info!("spawned raft network peer-sync loop");

        // 10. Start health monitor
        let hm = self.health_monitor.clone();
        let backend_for_health = backend.clone();
        tokio::spawn(async move {
            hm.run(backend_for_health).await;
        });

        // 11. Wait for ctrl-c, then shutdown
        info!("blockyard node started, waiting for shutdown signal");
        tokio::signal::ctrl_c()
            .await
            .map_err(blockyard_common::Error::Io)?;

        info!("shutting down");
        gossip.stop();
        Ok(())
    }
}
