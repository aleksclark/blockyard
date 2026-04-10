//! BlockyardNode — single-node initialization and lifecycle.

use std::collections::BTreeMap;
use std::sync::Arc;

use blockyard_common::{EpochId, NodeConfig, NodeId};
use blockyard_protocol::{
    DataPlaneHandler, ReadExtentRequest, ReadExtentResponse, WriteExtentRequest,
    WriteExtentResponse,
};
use blockyard_raft::{
    MetadataService, NetworkFactory, PersistentLogStore, PersistentStateMachineStore, Router,
    TypeConfig,
};
use blockyard_storage::{
    DataNodeService, DiskInventory, ExtentIndex, ExtentStore,
};
use blockyard_storage::background::{BackgroundScheduler, SchedulerConfig};
use openraft::{BasicNode, Raft};
use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::info;

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
    metadata: MetadataService,
    data_service: Arc<DataNodeHandler>,
    _scheduler: BackgroundScheduler,
    shutdown: CancellationToken,
}

#[allow(dead_code)]
impl BlockyardNode {
    /// Start a single Blockyard node from the given configuration.
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

        // Step 6: Create single-node Raft with persistent storage
        let log_store = PersistentLogStore::new(&config.data_dir.join("raft.db"))
            .map_err(|e| anyhow::anyhow!("failed to open raft log store: {e}"))?;
        let sm_store = PersistentStateMachineStore::new(&config.data_dir.join("raft-sm.db"))
            .map_err(|e| anyhow::anyhow!("failed to open raft state machine store: {e}"))?;
        let router = Arc::new(RwLock::new(Router::new()));
        let network = NetworkFactory::new(router.clone());

        let raft_config = openraft::Config {
            election_timeout_min: config.raft.election_timeout_min_ms,
            election_timeout_max: config.raft.election_timeout_max_ms,
            heartbeat_interval: config.raft.heartbeat_interval_ms,
            ..Default::default()
        };

        let sm_data = sm_store.data_arc().clone();
        let raft = Raft::<TypeConfig>::new(
            1, // node ID for Raft (u64)
            Arc::new(raft_config),
            network,
            log_store,
            sm_store.clone(),
        )
        .await?;

        // Register in router
        router.write().add_node(1, raft.clone());

        // Step 7: Initialize single-node cluster
        let mut members = BTreeMap::new();
        members.insert(1u64, BasicNode::default());
        raft.initialize(members).await?;

        info!("raft cluster initialized (single-node)");

        // Step 8: Create MetadataService
        let metadata = MetadataService::new(raft, sm_data);

        // Step 9: Start BackgroundScheduler
        let scheduler = BackgroundScheduler::new(SchedulerConfig::default());

        info!(%node_id, "blockyard node started");

        Ok(Self {
            config,
            node_id,
            metadata,
            data_service,
            _scheduler: scheduler,
            shutdown,
        })
    }

    /// Graceful shutdown.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        info!(node_id = %self.node_id, "shutting down blockyard node");
        self.shutdown.cancel();
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

    /// Get the cancellation token for coordinated shutdown.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Get a reference to the config.
    pub fn config(&self) -> &NodeConfig {
        &self.config
    }
}
