use blockyard_common::config::NodeConfig;
use blockyard_gossip::SwimGossip;
use blockyard_raft::MultiRaft;
use blockyard_storage::ZfsBackend;
use tracing::info;

pub struct BlockyardNode {
    config: NodeConfig,
    gossip: SwimGossip,
    raft: MultiRaft,
    storage: ZfsBackend,
}

impl BlockyardNode {
    pub fn new(config: NodeConfig) -> blockyard_common::Result<Self> {
        let node_name = config.node.name.clone().unwrap_or_else(|| {
            hostname::get()
                .map(|h| h.to_string_lossy().into_owned())
                .unwrap_or_else(|_| "unknown".to_string())
        });

        info!(name = %node_name, "initializing blockyard node");

        let gossip = SwimGossip::new(
            0, // node ID assigned during cluster join
            config.node.listen,
            config.cluster.seeds.clone(),
            config.gossip.clone(),
        );

        let raft = MultiRaft::new();
        let storage = ZfsBackend::new(config.storage.zfs_pool.clone());

        Ok(Self {
            config,
            gossip,
            raft,
            storage,
        })
    }

    pub async fn start(&mut self) -> blockyard_common::Result<()> {
        info!(
            listen = %self.config.node.listen,
            pool = %self.storage.pool_name(),
            "starting blockyard node"
        );

        self.gossip.start().await?;
        self.raft.start().await?;

        info!("blockyard node started, waiting for shutdown signal");
        tokio::signal::ctrl_c()
            .await
            .map_err(|e| blockyard_common::Error::Io(e))?;

        info!("shutting down");
        Ok(())
    }
}
