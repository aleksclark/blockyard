//! BlockyardNode — multi-node cluster formation and lifecycle.
//!
//! Integrates gossip peer discovery with Raft cluster formation:
//! - First node bootstraps a single-node cluster with raft_id=1
//! - Subsequent nodes join via HTTP management API on a seed node
//! - Gossip discovers peers and registers them in the Raft PeerRegistry
//! - TCP transport is used for Raft RPCs in production (not in-memory Router)

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use blockyard_common::{EpochId, NodeConfig, NodeId};
use blockyard_gossip::GossipService;
use blockyard_protocol::{
    DataPlaneHandler, ReadExtentRequest, ReadExtentResponse, WriteExtentRequest,
    WriteExtentResponse,
};
use blockyard_raft::{
    MetadataService, PeerRegistry, PersistentLogStore, PersistentStateMachineStore,
    RaftRpcServer, RaftRpcServerHandle, TcpNetworkFactory, TcpTransportConfig, TypeConfig,
};
use blockyard_storage::background::{BackgroundScheduler, SchedulerConfig};
use blockyard_storage::{DataNodeService, DiskInventory, ExtentIndex, ExtentStore};
use openraft::{BasicNode, Raft};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Newtype wrapper around `DataNodeService` that implements [`DataPlaneHandler`].
///
/// Required to satisfy the orphan rule (both trait and type are foreign to this crate).
#[derive(Debug)]
pub struct DataNodeHandler(pub DataNodeService);

impl DataPlaneHandler for DataNodeHandler {
    fn handle_write(&self, request: &WriteExtentRequest, payload: &[u8]) -> WriteExtentResponse {
        self.0.handle_write(request, payload)
    }

    fn handle_read(&self, request: &ReadExtentRequest) -> (ReadExtentResponse, Option<Vec<u8>>) {
        self.0.handle_read(request)
    }
}

/// A running Blockyard node.
#[allow(dead_code)]
pub struct BlockyardNode {
    config: NodeConfig,
    node_id: NodeId,
    raft_id: u64,
    metadata: MetadataService,
    data_service: Arc<DataNodeHandler>,
    gossip: Arc<GossipService>,
    peer_registry: PeerRegistry,
    raft_rpc_handle: Option<RaftRpcServerHandle>,
    _scheduler: BackgroundScheduler,
    shutdown: CancellationToken,
}

/// Derive the raft bind address from config.
///
/// If `raft.bind_addr` is explicitly set, use it.
/// Otherwise, use the same IP as `listen_addr` with port + 10.
pub fn raft_bind_addr(config: &NodeConfig) -> SocketAddr {
    config.raft.bind_addr.unwrap_or_else(|| {
        let mut addr = config.listen_addr;
        addr.set_port(addr.port() + 10);
        addr
    })
}

/// Response from the cluster join endpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct JoinResponse {
    pub raft_id: u64,
}

/// Request body for the cluster join endpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct JoinRequest {
    pub node_id: NodeId,
    pub raft_addr: String,
    pub data_addr: String,
}

#[allow(dead_code)]
impl BlockyardNode {
    /// Start a Blockyard node with full cluster formation logic.
    ///
    /// Bootstrap sequence:
    /// 1. Load persistent node_id
    /// 2. Open persistent raft stores
    /// 3. Check raft state:
    ///    a. Has existing state → recover (restart)
    ///    b. No state + no seeds → bootstrap as single-node cluster
    ///    c. No state + has seeds → join existing cluster via HTTP
    /// 4. Start gossip service
    /// 5. Register gossip callbacks for peer registry
    pub async fn start(config: NodeConfig) -> anyhow::Result<Self> {
        let node_id = NodeId::load_or_create(&config.data_dir)?;
        let shutdown = CancellationToken::new();

        info!(%node_id, "starting blockyard node");

        // Step 1: Discover disks
        let inventory = DiskInventory::new();
        let disk_ids = inventory.discover_disks(&config.storage.disk_paths, false)?;
        info!(disk_count = disk_ids.len(), "discovered disks");

        // Step 2: Create ExtentIndex
        let index = ExtentIndex::new();

        // Step 3: Create ExtentStore per disk + run recovery
        let mut stores = Vec::new();
        for &disk_id in &disk_ids {
            let mount_path = inventory.get_mount_path(disk_id)?;
            let store = ExtentStore::new(mount_path, disk_id);
            let report = store.recover(&index)?;
            info!(
                %disk_id,
                committed = report.committed_recovered,
                staged_discarded = report.staged_discarded,
                errors = report.errors,
                "disk recovery complete"
            );
            stores.push((disk_id, store));
        }

        // Step 4: Create DataNodeService, wrap in handler
        let service = DataNodeService::new(inventory, index, EpochId::new(1));

        // Step 5: Register stores
        for (disk_id, store) in stores {
            service.register_store(disk_id, store);
        }

        let data_service = Arc::new(DataNodeHandler(service));

        // Step 6: Open persistent raft stores
        let log_store = PersistentLogStore::new(&config.data_dir.join("raft.db"))
            .map_err(|e| anyhow::anyhow!("failed to open raft log store: {e}"))?;
        let sm_store = PersistentStateMachineStore::new(&config.data_dir.join("raft-sm.db"))
            .map_err(|e| anyhow::anyhow!("failed to open raft state machine store: {e}"))?;

        let raft_addr = raft_bind_addr(&config);
        let peer_registry = PeerRegistry::new();
        let sm_data = sm_store.data_arc().clone();

        // Step 7: Determine cluster mode
        let has_existing_state = sm_store.data().last_applied.is_some();
        let has_seeds = !config.gossip.seed_nodes.is_empty();

        let (raft, raft_id, raft_rpc_handle) = if has_existing_state {
            // Recovery: node is restarting with existing raft state
            let existing_raft_id = sm_data.read().get_raft_id(&node_id).unwrap_or(1);
            info!(raft_id = existing_raft_id, "recovering existing raft cluster state");

            // Populate peer registry from known cluster nodes
            {
                let data = sm_data.read();
                for node_entry in data.nodes.values() {
                    if let Some(raft_nid) = data.node_raft_map.get(&node_entry.node_id) {
                        if *raft_nid != existing_raft_id {
                            if let Ok(addr) = node_entry.addr.parse::<SocketAddr>() {
                                peer_registry.register(*raft_nid, addr);
                            }
                        }
                    }
                }
            }

            let network = TcpNetworkFactory::new(peer_registry.clone(), TcpTransportConfig::default());
            let raft_config = openraft::Config {
                election_timeout_min: config.raft.election_timeout_min_ms,
                election_timeout_max: config.raft.election_timeout_max_ms,
                heartbeat_interval: config.raft.heartbeat_interval_ms,
                ..Default::default()
            };

            let raft = Raft::<TypeConfig>::new(
                existing_raft_id,
                Arc::new(raft_config),
                network,
                log_store,
                sm_store.clone(),
            )
            .await?;

            // Start RPC server
            let raft_arc = Arc::new(raft.clone());
            let rpc_server = RaftRpcServer::bind(raft_arc, raft_addr).await?;
            let rpc_handle = rpc_server.handle();
            tokio::spawn(rpc_server.run());

            (raft, existing_raft_id, Some(rpc_handle))
        } else if !has_seeds {
            // Bootstrap: first node, single-node cluster with raft_id=1
            let raft_id = 1u64;
            info!(raft_id, "bootstrapping new single-node cluster");

            peer_registry.register(raft_id, raft_addr);

            let network = TcpNetworkFactory::new(peer_registry.clone(), TcpTransportConfig::default());
            let raft_config = openraft::Config {
                election_timeout_min: config.raft.election_timeout_min_ms,
                election_timeout_max: config.raft.election_timeout_max_ms,
                heartbeat_interval: config.raft.heartbeat_interval_ms,
                ..Default::default()
            };

            let raft = Raft::<TypeConfig>::new(
                raft_id,
                Arc::new(raft_config),
                network,
                log_store,
                sm_store.clone(),
            )
            .await?;

            // Start RPC server
            let raft_arc = Arc::new(raft.clone());
            let rpc_server = RaftRpcServer::bind(raft_arc, raft_addr).await?;
            let rpc_handle = rpc_server.handle();
            tokio::spawn(rpc_server.run());

            // Initialize single-node cluster
            let mut members = BTreeMap::new();
            members.insert(raft_id, BasicNode::default());
            raft.initialize(members).await?;
            info!("raft cluster initialized (single-node)");

            // Register self in state machine via raft commit
            let metadata_tmp = MetadataService::new(raft.clone(), sm_data.clone());
            let registered_id = metadata_tmp
                .register_node(node_id, raft_addr.to_string())
                .await?;
            info!(raft_id = registered_id, "registered self in state machine");

            (raft, raft_id, Some(rpc_handle))
        } else {
            // Join: contact a seed node to get a raft_id and be added to the cluster
            info!("joining existing cluster via seed nodes");

            let join_req = JoinRequest {
                node_id,
                raft_addr: raft_addr.to_string(),
                data_addr: config.listen_addr.to_string(),
            };

            let mut raft_id = None;
            let client = reqwest::Client::new();
            for seed in &config.gossip.seed_nodes {
                // Derive the seed's mgmt_addr from the gossip seed.
                // Convention: mgmt API runs on same host, port from config.protocol.mgmt_addr
                // But we use the seed directly as the gossip addr. The mgmt endpoint
                // is at the default management port. We'll try the seed's IP with the
                // default mgmt port.
                let mgmt_addr = format!("http://{}:{}", seed.ip(), config.protocol.mgmt_addr.port());
                let url = format!("{}/api/v1/cluster/join", mgmt_addr);

                match client.post(&url).json(&join_req).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        if let Ok(join_resp) = resp.json::<JoinResponse>().await {
                            info!(raft_id = join_resp.raft_id, seed = %seed, "joined cluster");
                            raft_id = Some(join_resp.raft_id);
                            break;
                        }
                    }
                    Ok(resp) => {
                        warn!(seed = %seed, status = %resp.status(), "join request rejected");
                    }
                    Err(e) => {
                        warn!(seed = %seed, error = %e, "failed to contact seed for join");
                    }
                }
            }

            let raft_id = raft_id.ok_or_else(|| {
                anyhow::anyhow!("failed to join cluster: no seed node responded successfully")
            })?;

            let network = TcpNetworkFactory::new(peer_registry.clone(), TcpTransportConfig::default());
            let raft_config = openraft::Config {
                election_timeout_min: config.raft.election_timeout_min_ms,
                election_timeout_max: config.raft.election_timeout_max_ms,
                heartbeat_interval: config.raft.heartbeat_interval_ms,
                ..Default::default()
            };

            let raft = Raft::<TypeConfig>::new(
                raft_id,
                Arc::new(raft_config),
                network,
                log_store,
                sm_store.clone(),
            )
            .await?;

            // Start RPC server
            let raft_arc = Arc::new(raft.clone());
            let rpc_server = RaftRpcServer::bind(raft_arc, raft_addr).await?;
            let rpc_handle = rpc_server.handle();
            tokio::spawn(rpc_server.run());

            (raft, raft_id, Some(rpc_handle))
        };

        // Step 8: Create MetadataService
        let metadata = MetadataService::new(raft, sm_data);

        // Step 9: Start BackgroundScheduler
        let scheduler = BackgroundScheduler::new(SchedulerConfig::default());

        // Step 10: Start GossipService
        let gossip = Arc::new(GossipService::new(node_id, config.gossip.clone()));

        // Register gossip callbacks to keep PeerRegistry updated
        let peer_reg_join = peer_registry.clone();
        let sm_ref_join = metadata.sm_data().clone();
        gossip.on_member_join(Box::new(move |member_node_id, member_addr| {
            // Look up raft_id for this member from state machine
            let data = sm_ref_join.read();
            if let Some(&raft_nid) = data.node_raft_map.get(&member_node_id) {
                peer_reg_join.register(raft_nid, member_addr);
                tracing::debug!(
                    node_id = %member_node_id,
                    raft_id = raft_nid,
                    addr = %member_addr,
                    "registered peer in PeerRegistry via gossip"
                );
            }
        }));

        let peer_reg_leave = peer_registry.clone();
        let sm_ref_leave = metadata.sm_data().clone();
        gossip.on_member_leave(Box::new(move |member_node_id, _addr| {
            let data = sm_ref_leave.read();
            if let Some(&raft_nid) = data.node_raft_map.get(&member_node_id) {
                peer_reg_leave.unregister(raft_nid);
                tracing::debug!(
                    node_id = %member_node_id,
                    raft_id = raft_nid,
                    "unregistered peer from PeerRegistry via gossip"
                );
            }
        }));

        // Start gossip (best-effort; don't fail node startup if gossip bind fails)
        if let Err(e) = gossip.start().await {
            warn!(error = %e, "gossip service failed to start, continuing without gossip");
        } else {
            info!("gossip service started");
        }

        info!(%node_id, raft_id, "blockyard node started");

        Ok(Self {
            config,
            node_id,
            raft_id,
            metadata,
            data_service,
            gossip,
            peer_registry,
            raft_rpc_handle,
            _scheduler: scheduler,
            shutdown,
        })
    }

    /// Graceful shutdown.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        info!(node_id = %self.node_id, "shutting down blockyard node");
        self.shutdown.cancel();

        // Stop gossip
        self.gossip.stop().await;

        // Stop raft RPC server
        if let Some(handle) = &self.raft_rpc_handle {
            handle.shutdown();
        }

        Ok(())
    }

    /// Get a reference to the metadata service.
    pub fn metadata(&self) -> &MetadataService {
        &self.metadata
    }

    /// Get a reference to the data node service (wrapped).
    pub fn data_service(&self) -> &Arc<DataNodeHandler> {
        &self.data_service
    }

    /// Get the node ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get the raft node ID (u64).
    pub fn raft_id(&self) -> u64 {
        self.raft_id
    }

    /// Get the cancellation token for coordinated shutdown.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Get a reference to the config.
    pub fn config(&self) -> &NodeConfig {
        &self.config
    }

    /// Get a reference to the gossip service.
    pub fn gossip(&self) -> &Arc<GossipService> {
        &self.gossip
    }

    /// Get a reference to the peer registry.
    pub fn peer_registry(&self) -> &PeerRegistry {
        &self.peer_registry
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockyard_raft::MetadataStateMachineData;

    #[test]
    fn test_raft_bind_addr_default_derivation() {
        let config = test_config(9800, None);
        let addr = raft_bind_addr(&config);
        assert_eq!(addr.port(), 9810);
        assert_eq!(addr.ip(), config.listen_addr.ip());
    }

    #[test]
    fn test_raft_bind_addr_explicit() {
        let explicit: SocketAddr = "10.0.0.1:5555".parse().unwrap();
        let config = test_config(9800, Some(explicit));
        let addr = raft_bind_addr(&config);
        assert_eq!(addr, explicit);
    }

    #[test]
    fn test_join_request_serde_roundtrip() {
        let req = JoinRequest {
            node_id: NodeId::generate(),
            raft_addr: "127.0.0.1:9810".into(),
            data_addr: "127.0.0.1:9800".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: JoinRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.node_id, req.node_id);
        assert_eq!(parsed.raft_addr, req.raft_addr);
        assert_eq!(parsed.data_addr, req.data_addr);
    }

    #[test]
    fn test_join_response_serde_roundtrip() {
        let resp = JoinResponse { raft_id: 42 };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: JoinResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.raft_id, 42);
    }

    #[test]
    fn test_state_machine_register_node_assigns_sequential_ids() {
        let mut sm = MetadataStateMachineData::new();
        let node1 = NodeId::generate();
        let node2 = NodeId::generate();
        let node3 = NodeId::generate();

        let resp1 = sm.apply_request(&blockyard_raft::MetadataRequest::RegisterNode {
            node_id: node1,
            addr: "10.0.0.1:9810".into(),
        });
        let resp2 = sm.apply_request(&blockyard_raft::MetadataRequest::RegisterNode {
            node_id: node2,
            addr: "10.0.0.2:9810".into(),
        });
        let resp3 = sm.apply_request(&blockyard_raft::MetadataRequest::RegisterNode {
            node_id: node3,
            addr: "10.0.0.3:9810".into(),
        });

        match resp1 {
            blockyard_raft::MetadataResponse::NodeRegistered(id) => assert_eq!(id, 1),
            other => panic!("expected NodeRegistered, got {:?}", other),
        }
        match resp2 {
            blockyard_raft::MetadataResponse::NodeRegistered(id) => assert_eq!(id, 2),
            other => panic!("expected NodeRegistered, got {:?}", other),
        }
        match resp3 {
            blockyard_raft::MetadataResponse::NodeRegistered(id) => assert_eq!(id, 3),
            other => panic!("expected NodeRegistered, got {:?}", other),
        }
    }

    #[test]
    fn test_state_machine_register_node_idempotent() {
        let mut sm = MetadataStateMachineData::new();
        let node1 = NodeId::generate();

        let resp1 = sm.apply_request(&blockyard_raft::MetadataRequest::RegisterNode {
            node_id: node1,
            addr: "10.0.0.1:9810".into(),
        });
        let resp2 = sm.apply_request(&blockyard_raft::MetadataRequest::RegisterNode {
            node_id: node1,
            addr: "10.0.0.1:9811".into(), // different addr
        });

        match (&resp1, &resp2) {
            (
                blockyard_raft::MetadataResponse::NodeRegistered(id1),
                blockyard_raft::MetadataResponse::NodeRegistered(id2),
            ) => {
                assert_eq!(*id1, 1);
                assert_eq!(*id2, 1); // same node should get same raft_id
            }
            _ => panic!("expected NodeRegistered for both"),
        }

        // Counter should not have advanced for the re-registration
        assert_eq!(sm.raft_id_counter(), 1);
    }

    #[test]
    fn test_state_machine_node_raft_map_lookup() {
        let mut sm = MetadataStateMachineData::new();
        let node1 = NodeId::generate();
        let node2 = NodeId::generate();

        sm.apply_request(&blockyard_raft::MetadataRequest::RegisterNode {
            node_id: node1,
            addr: "10.0.0.1:9810".into(),
        });
        sm.apply_request(&blockyard_raft::MetadataRequest::RegisterNode {
            node_id: node2,
            addr: "10.0.0.2:9810".into(),
        });

        assert_eq!(sm.get_raft_id(&node1), Some(1));
        assert_eq!(sm.get_raft_id(&node2), Some(2));
        assert_eq!(sm.get_raft_id(&NodeId::generate()), None);
    }

    #[test]
    fn test_state_machine_reverse_raft_id_lookup() {
        let mut sm = MetadataStateMachineData::new();
        let node1 = NodeId::generate();

        sm.apply_request(&blockyard_raft::MetadataRequest::RegisterNode {
            node_id: node1,
            addr: "10.0.0.1:9810".into(),
        });

        assert_eq!(sm.get_node_id_by_raft_id(1), Some(node1));
        assert_eq!(sm.get_node_id_by_raft_id(999), None);
    }

    #[test]
    fn test_state_machine_register_also_adds_cluster_node() {
        let mut sm = MetadataStateMachineData::new();
        let node1 = NodeId::generate();

        sm.apply_request(&blockyard_raft::MetadataRequest::RegisterNode {
            node_id: node1,
            addr: "10.0.0.1:9810".into(),
        });

        let cluster_node = sm.get_node(&node1);
        assert!(cluster_node.is_some());
        assert_eq!(cluster_node.unwrap().addr, "10.0.0.1:9810");
    }

    #[test]
    fn test_state_machine_register_updates_addr_on_re_register() {
        let mut sm = MetadataStateMachineData::new();
        let node1 = NodeId::generate();

        sm.apply_request(&blockyard_raft::MetadataRequest::RegisterNode {
            node_id: node1,
            addr: "10.0.0.1:9810".into(),
        });
        sm.apply_request(&blockyard_raft::MetadataRequest::RegisterNode {
            node_id: node1,
            addr: "10.0.0.1:9999".into(),
        });

        let cluster_node = sm.get_node(&node1);
        assert!(cluster_node.is_some());
        assert_eq!(cluster_node.unwrap().addr, "10.0.0.1:9999");
    }

    #[test]
    fn test_state_machine_raft_id_counter_persists_through_serde() {
        let mut sm = MetadataStateMachineData::new();
        let node1 = NodeId::generate();

        sm.apply_request(&blockyard_raft::MetadataRequest::RegisterNode {
            node_id: node1,
            addr: "10.0.0.1:9810".into(),
        });

        let json = serde_json::to_string(&sm).unwrap();
        let restored: MetadataStateMachineData = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.raft_id_counter(), 1);
        assert_eq!(restored.get_raft_id(&node1), Some(1));
    }

    #[test]
    fn test_metadata_response_node_registered_variant() {
        let resp = blockyard_raft::MetadataResponse::NodeRegistered(42);
        assert!(!resp.is_error());
        match resp {
            blockyard_raft::MetadataResponse::NodeRegistered(id) => assert_eq!(id, 42),
            _ => panic!("expected NodeRegistered"),
        }
    }

    #[test]
    fn test_peer_registry_gossip_integration() {
        // Simulate what the gossip callbacks do: map a node_id to raft_id
        // and register/unregister in PeerRegistry.
        let mut sm = MetadataStateMachineData::new();
        let node1 = NodeId::generate();
        let addr: SocketAddr = "10.0.0.1:9810".parse().unwrap();

        sm.apply_request(&blockyard_raft::MetadataRequest::RegisterNode {
            node_id: node1,
            addr: addr.to_string(),
        });

        let registry = PeerRegistry::new();

        // Simulate gossip on_member_join callback
        if let Some(raft_nid) = sm.get_raft_id(&node1) {
            registry.register(raft_nid, addr);
        }

        assert_eq!(registry.get(1), Some(addr));

        // Simulate gossip on_member_leave callback
        if let Some(raft_nid) = sm.get_raft_id(&node1) {
            registry.unregister(raft_nid);
        }

        assert_eq!(registry.get(1), None);
    }

    #[test]
    fn test_data_node_handler_debug() {
        // Just ensure Debug is derived; can't easily construct without real disks.
        // This test ensures the struct compiles with Debug.
        let _: fn(&DataNodeHandler) -> String = |h| format!("{:?}", h);
    }

    /// Helper to create a test NodeConfig.
    fn test_config(port: u16, raft_addr: Option<SocketAddr>) -> NodeConfig {
        NodeConfig {
            name: None,
            listen_addr: format!("127.0.0.1:{}", port).parse().unwrap(),
            data_dir: "/tmp/blockyard-test".into(),
            storage: blockyard_common::StorageSection {
                disk_paths: vec!["/tmp/disk0".into()],
                max_background_io: 4,
                scrub_interval_secs: 86400,
            },
            raft: blockyard_common::RaftSection {
                election_timeout_min_ms: 150,
                election_timeout_max_ms: 300,
                heartbeat_interval_ms: 50,
                max_entries_per_batch: 64,
                snapshot_threshold: 10000,
                bind_addr: raft_addr,
            },
            gossip: blockyard_common::GossipSection {
                bind_addr: format!("127.0.0.1:{}", port + 1).parse().unwrap(),
                seed_nodes: vec![],
                gossip_interval_ms: 1000,
                suspicion_mult: 4,
            },
            protocol: blockyard_common::ProtocolSection {
                max_message_size: 64 * 1024 * 1024,
                connect_timeout_ms: 5000,
                request_timeout_ms: 30000,
                mgmt_addr: "127.0.0.1:9801".parse().unwrap(),
            },
            tls: None,
            auth: None,
        }
    }
}
