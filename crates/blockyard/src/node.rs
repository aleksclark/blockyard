use blockyard_common::config::NodeConfig;
use blockyard_gossip::SwimGossip;
use blockyard_gossip::transport::UdpTransport;
use blockyard_raft::MultiRaft;
use blockyard_storage::backend::MemoryBackend;
use blockyard_storage::{HealthMonitor, PlacementEngine};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

use blockyard_common::types::{NodeId, NodeInfo, NodeState, ZfsHealthState};

pub struct BlockyardNode {
    config: NodeConfig,
    node_id: NodeId,
    node_name: String,
    raft: MultiRaft,
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

        let raft = MultiRaft::new(node_id);
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

        let meta_group_id = blockyard_raft::meta_group::MetaGroup::group_id();
        self.raft.create_group(meta_group_id)?;
        info!("created meta raft group");

        self.raft.start().await?;

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

        let backend = Arc::new(MemoryBackend::new(
            self.config.storage.zfs_pool.clone(),
            1024 * 1024 * 1024 * 100,
        ));
        let hm = self.health_monitor.clone();
        let backend_for_health = backend.clone();
        tokio::spawn(async move {
            hm.run(backend_for_health).await;
        });

        info!("blockyard node started, waiting for shutdown signal");
        tokio::signal::ctrl_c()
            .await
            .map_err(blockyard_common::Error::Io)?;

        info!("shutting down");
        gossip.stop();
        Ok(())
    }
}
