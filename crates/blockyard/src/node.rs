use blockyard_common::config::NodeConfig;
use blockyard_gossip::SwimGossip;
use blockyard_gossip::transport::UdpTransport;
use blockyard_protocol::server::{ProtocolServer, RequestHandler};
use blockyard_protocol::wire::Status as WireStatus;
use blockyard_raft::grpc_server::BlockyardGrpcServer;
use blockyard_raft::multiraft::MultiRaft;
use blockyard_raft::network::RaftNetwork;
use blockyard_raft::types::RaftRequest;
use blockyard_storage::backend::{MemoryBackend, StorageBackend};
use blockyard_storage::{HealthMonitor, PlacementEngine, ZfsBackend};
use blockyard_ublk::nbd::MemBlockStore;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

use blockyard_common::types::{NodeId, NodeInfo, NodeState, PoolInfo, ZfsHealthState};

// ---------------------------------------------------------------------------
// BlockHandler — bridges the protocol server to raft + storage backend
// ---------------------------------------------------------------------------

struct BlockHandler {
    raft: Arc<MultiRaft>,
    store: Arc<MemBlockStore>,
}

impl RequestHandler for BlockHandler {
    async fn handle_write(
        &self,
        volume_id: u64,
        offset: u64,
        data: Bytes,
    ) -> Result<(), WireStatus> {
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

        self.store.write(offset, &data);
        Ok(())
    }

    async fn handle_read(
        &self,
        _volume_id: u64,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, WireStatus> {
        let data = self.store.read(offset, length);
        Ok(Bytes::from(data))
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
        let all_pools = self.config.storage.all_pools();
        info!(
            listen = %self.config.node.listen,
            pools = ?all_pools,
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

        // 4. Probe all configured pools and build PoolInfo list
        let mut pool_infos: Vec<PoolInfo> = Vec::new();
        let mut total_capacity: u64 = 0;
        let mut total_used: u64 = 0;
        let mut any_zfs_available = false;

        for pool_name in &all_pools {
            let zfs_test = ZfsBackend::new(pool_name.clone());
            match zfs_test.pool_capacity().await {
                Ok((cap, used)) if cap > 0 => {
                    info!(pool = %pool_name, capacity = cap, used = used, "probed ZFS pool");
                    pool_infos.push(PoolInfo {
                        name: pool_name.clone(),
                        capacity_bytes: cap,
                        used_bytes: used,
                        health: ZfsHealthState::Online,
                    });
                    total_capacity += cap;
                    total_used += used;
                    any_zfs_available = true;
                }
                _ => {
                    info!(pool = %pool_name, "ZFS pool not available");
                }
            }
        }

        // 5. Start SWIM gossip
        let transport = UdpTransport::bind(self.config.node.listen).await?;
        let local_info = NodeInfo {
            id: self.node_id,
            name: self.node_name.clone(),
            addr: self.config.node.listen,
            data_addr: self.config.node.data_listen,
            tags: self.config.tags.clone(),
            state: NodeState::Healthy,
            zfs_health: ZfsHealthState::Online,
            capacity_bytes: total_capacity,
            used_bytes: total_used,
            incarnation: 1,
            pools: pool_infos,
        };

        let gossip = SwimGossip::new(
            local_info,
            self.config.cluster.seeds.clone(),
            self.config.gossip.clone(),
            transport,
        );
        gossip.start().await?;

        // 6. Create storage backend — use ZFS if any pool exists, else MemoryBackend
        let primary_pool = all_pools.first().cloned().unwrap_or_default();
        if any_zfs_available {
            info!(pools = ?all_pools, "using ZFS storage backend");
        } else {
            info!(pools = ?all_pools, "no ZFS pools available, using in-memory backend");
        }

        // 7. Create BlockHandler with in-memory block store (10GB)
        let block_store = Arc::new(MemBlockStore::new(10 * 1024 * 1024 * 1024, 4096));
        let handler = BlockHandler {
            raft: Arc::clone(&self.raft),
            store: block_store,
        };

        // 8. Spawn ProtocolServer on config.node.listen
        let protocol_listen = self.config.node.listen;
        let protocol_server = ProtocolServer::new(protocol_listen, handler);
        tokio::spawn(async move {
            if let Err(e) = protocol_server.run().await {
                error!(error = %e, "protocol server failed");
            }
        });
        info!(addr = %protocol_listen, "spawned protocol server");

        // 9. Spawn BlockyardGrpcServer on config.node.data_listen
        let grpc_server = BlockyardGrpcServer::new(Arc::clone(&self.raft));
        let grpc_listen = self.config.node.data_listen;
        tokio::spawn(async move {
            if let Err(e) = grpc_server.serve(grpc_listen).await {
                error!(error = %e, "gRPC server failed");
            }
        });
        info!(addr = %grpc_listen, "spawned gRPC server");

        // 10. Create RaftNetwork and spawn peer-sync loop from gossip
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

        // 11. Start health monitor — check all configured pools
        let hm = self.health_monitor.clone();
        let pool_for_health = primary_pool.clone();
        let zfs_available = any_zfs_available;
        tokio::spawn(async move {
            if zfs_available {
                let backend = Arc::new(ZfsBackend::new(pool_for_health));
                hm.run(backend).await;
            } else {
                let backend = Arc::new(MemoryBackend::new(
                    pool_for_health,
                    1024 * 1024 * 1024 * 100,
                ));
                hm.run(backend).await;
            }
        });

        // 12. Wait for ctrl-c, then shutdown
        info!("blockyard node started, waiting for shutdown signal");
        tokio::signal::ctrl_c()
            .await
            .map_err(blockyard_common::Error::Io)?;

        info!("shutting down");
        gossip.stop();
        Ok(())
    }
}
