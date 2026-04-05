use blockyard_common::config::NodeConfig;
use blockyard_gossip::SwimGossip;
use blockyard_gossip::transport::UdpTransport;
use blockyard_protocol::server::{ProtocolServer, RequestHandler};
use blockyard_protocol::wire::Status as WireStatus;
use blockyard_raft::HeartbeatConsolidator;
use blockyard_raft::MultiRaft;
use blockyard_raft::grpc_server::BlockyardGrpcServer;
use blockyard_raft::network::RaftNetwork;
use blockyard_raft::types::RaftRequest;
use blockyard_storage::backend::MemoryBackend;
use blockyard_storage::{HealthMonitor, PlacementEngine};
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::{info, warn};

use blockyard_common::types::{NodeId, NodeInfo, NodeState, ZfsHealthState};

// ---------------------------------------------------------------------------
// RequestHandler implementation — routes block I/O through Raft
// ---------------------------------------------------------------------------

struct BlockHandler {
    raft: Arc<MultiRaft>,
    meta_group_id: u64,
    backend: Arc<MemoryBackend>,
    writes: AtomicU64,
    reads: AtomicU64,
}

impl RequestHandler for BlockHandler {
    async fn handle_read(
        &self,
        volume_id: u64,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, WireStatus> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        // Read from local state machine — no Raft round-trip needed.
        let _state = self
            .raft
            .get_state(self.meta_group_id)
            .ok_or(WireStatus::IoError)?;
        // For now, return zeroes sized to the requested length. A real
        // implementation would read from the backend extent store keyed by
        // (volume_id, offset).
        let _ = (volume_id, offset);
        Ok(Bytes::from(vec![0u8; length as usize]))
    }

    async fn handle_write(
        &self,
        volume_id: u64,
        offset: u64,
        data: Bytes,
    ) -> Result<(), WireStatus> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        // Propose the write through the meta Raft group so it is replicated.
        let req = RaftRequest::Write {
            volume_id,
            offset,
            data: data.to_vec(),
        };
        self.raft
            .propose(self.meta_group_id, &req)
            .map_err(|_| WireStatus::IoError)?;
        // Backend persist is handled by the state-machine apply; nothing
        // extra to do here yet.
        let _ = &self.backend;
        Ok(())
    }

    async fn handle_flush(&self, _volume_id: u64) -> Result<(), WireStatus> {
        // No-op for now.
        Ok(())
    }

    async fn handle_trim(
        &self,
        _volume_id: u64,
        _offset: u64,
        _length: u32,
    ) -> Result<(), WireStatus> {
        // No-op for now.
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
    placement: PlacementEngine,
    health_monitor: Arc<HealthMonitor>,
    network: RaftNetwork,
    heartbeat: HeartbeatConsolidator,
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
        let network = RaftNetwork::new();
        let heartbeat = HeartbeatConsolidator::new(config.raft.heartbeat_interval);

        Ok(Self {
            config,
            node_id,
            node_name,
            raft,
            placement,
            health_monitor,
            network,
            heartbeat,
        })
    }

    pub async fn start(&mut self) -> blockyard_common::Result<()> {
        info!(
            listen = %self.config.node.listen,
            data_listen = %self.config.node.data_listen,
            pool = %self.config.storage.zfs_pool,
            node_id = self.node_id,
            "starting blockyard node"
        );

        // ------------------------------------------------------------------
        // 1. Create and start the meta Raft group
        // ------------------------------------------------------------------
        let meta_group_id = blockyard_raft::meta_group::MetaGroup::group_id();
        self.raft.create_group(meta_group_id)?;
        info!("created meta raft group");
        self.raft.start().await?;

        // Register ourselves in the Raft state machine.
        self.raft.propose(
            meta_group_id,
            &RaftRequest::NodeRegister {
                node_id: self.node_id,
                addr: self.config.node.data_listen.to_string(),
            },
        )?;

        // ------------------------------------------------------------------
        // 2. Gossip
        // ------------------------------------------------------------------
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

        // ------------------------------------------------------------------
        // 3. Storage backend + health monitor
        // ------------------------------------------------------------------
        let backend = Arc::new(MemoryBackend::new(
            self.config.storage.zfs_pool.clone(),
            1024 * 1024 * 1024 * 100,
        ));
        let hm = self.health_monitor.clone();
        let backend_for_health = backend.clone();
        tokio::spawn(async move {
            hm.run(backend_for_health).await;
        });

        // ------------------------------------------------------------------
        // 4. gRPC server (Raft RPCs, heartbeats, cluster ops)
        // ------------------------------------------------------------------
        let grpc_server = BlockyardGrpcServer::new(Arc::clone(&self.raft));
        let grpc_addr = self.config.node.data_listen;
        let grpc_handle = tokio::spawn(async move {
            if let Err(e) = grpc_server.serve(grpc_addr).await {
                warn!(error = %e, "gRPC server exited with error");
            }
        });

        // ------------------------------------------------------------------
        // 5. Block protocol server (separate tokio task)
        // ------------------------------------------------------------------
        let handler = BlockHandler {
            raft: Arc::clone(&self.raft),
            meta_group_id,
            backend: backend.clone(),
            writes: AtomicU64::new(0),
            reads: AtomicU64::new(0),
        };
        let protocol_addr = self.config.node.listen;
        let protocol_server = ProtocolServer::new(protocol_addr, handler);
        let proto_handle = tokio::spawn(async move {
            if let Err(e) = protocol_server.run().await {
                warn!(error = %e, "protocol server exited with error");
            }
        });

        info!("blockyard node started, waiting for shutdown signal");

        // ------------------------------------------------------------------
        // 6. Background tasks (peer-sync, probe loop, heartbeat loop)
        // ------------------------------------------------------------------
        let probe_interval = self.config.gossip.probe_interval;
        let heartbeat_interval = self.config.raft.heartbeat_interval;
        let node_id = self.node_id;
        let members = gossip.members().clone();

        // Use the placement engine to decide if nodes are eligible, keeping
        // the field alive and intentionally read.
        let placement = &self.placement;

        // Peer-sync: update RaftNetwork with gossip membership periodically.
        let network_for_sync = self.network.clone();
        let members_for_sync = members.clone();
        let peer_sync_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                for node in members_for_sync.healthy_nodes() {
                    if node.id != node_id {
                        network_for_sync.add_peer(node.id, format!("http://{}", node.data_addr));
                    }
                }
            }
        });

        // Clone network & heartbeat data for the select! block.
        let network = self.network.clone();
        let heartbeat = &self.heartbeat;

        tokio::select! {
            // Gossip probe loop
            _ = async {
                let mut tick = tokio::time::interval(probe_interval);
                loop {
                    tick.tick().await;
                    gossip.run_probe_cycle().await;
                }
            } => {}

            // Health monitor: log placement-engine exclusion decisions
            _ = async {
                let mut rx = self.health_monitor.subscribe();
                loop {
                    if rx.changed().await.is_err() {
                        break;
                    }
                    let health = rx.borrow().clone();
                    // Build a synthetic NodeInfo to ask the placement engine
                    // whether this node should be excluded.
                    let probe = NodeInfo {
                        id: node_id,
                        name: String::new(),
                        addr: self.config.node.listen,
                        data_addr: self.config.node.data_listen,
                        tags: Default::default(),
                        state: NodeState::Healthy,
                        zfs_health: health.state,
                        capacity_bytes: health.capacity_bytes,
                        used_bytes: health.used_bytes,
                        incarnation: 0,
                    };
                    if placement.should_exclude_node(&probe) {
                        warn!(
                            pool = %health.pool_name,
                            zfs_state = ?health.state,
                            "local node excluded from placement"
                        );
                    }
                }
            } => {}

            // Heartbeat generation loop
            _ = async {
                let mut tick = tokio::time::interval(heartbeat_interval);
                loop {
                    tick.tick().await;
                    let beats = heartbeat.generate_heartbeats(node_id);
                    for hb in beats {
                        let req = blockyard_raft::proto::ConsolidatedHeartbeatRequest {
                            from_node: hb.from,
                            to_node: hb.to,
                            heartbeats: hb
                                .groups
                                .iter()
                                .map(|&g| blockyard_raft::proto::GroupHeartbeat {
                                    group_id: g,
                                    term: 0,
                                    commit_index: 0,
                                })
                                .collect(),
                        };
                        if let Err(e) = network.send_heartbeat(hb.to, req).await {
                            warn!(peer = hb.to, error = %e, "heartbeat send failed");
                        }
                    }
                }
            } => {}

            // Ctrl-C
            _ = tokio::signal::ctrl_c() => {
                info!("received shutdown signal");
            }
        }

        // ------------------------------------------------------------------
        // 7. Graceful shutdown + summary
        // ------------------------------------------------------------------
        gossip.stop();
        grpc_handle.abort();
        proto_handle.abort();
        peer_sync_handle.abort();

        let state = self.raft.get_state(meta_group_id).unwrap_or_default();
        info!(
            node_id = self.node_id,
            node_name = %self.node_name,
            volumes = state.volumes.len(),
            registered_nodes = state.nodes.len(),
            raft_groups = self.raft.group_count(),
            "blockyard node shut down"
        );

        Ok(())
    }
}
