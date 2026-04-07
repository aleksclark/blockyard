use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use tracing::{info, warn};

use crate::network::{NetworkConfig, PortAllocator};
use crate::vm::{Node, NodeConfig, NodeId, NodeState};

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub node_count: u32,
    pub binary_path: PathBuf,
    pub base_data_dir: PathBuf,
    pub network: NetworkConfig,
}

impl ClusterConfig {
    pub fn new(node_count: u32, binary_path: PathBuf, base_data_dir: PathBuf) -> Self {
        Self {
            node_count,
            binary_path,
            base_data_dir,
            network: NetworkConfig::default(),
        }
    }

    pub fn with_network(mut self, network: NetworkConfig) -> Self {
        self.network = network;
        self
    }
}

pub trait Cluster: Send + Sync {
    fn start_all(&self) -> anyhow::Result<()>;
    fn stop_all(&self) -> anyhow::Result<()>;
    fn node(&self, id: NodeId) -> Option<&Node>;
    fn node_ids(&self) -> Vec<NodeId>;
    fn node_count(&self) -> usize;
    fn running_nodes(&self) -> Vec<NodeId>;
    fn seed_addrs(&self) -> Vec<std::net::SocketAddr>;
}

pub struct ProcessCluster {
    config: ClusterConfig,
    nodes: HashMap<NodeId, Node>,
    port_allocator: PortAllocator,
    node_order: Vec<NodeId>,
}

impl std::fmt::Debug for ProcessCluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessCluster")
            .field("config", &self.config)
            .field("node_count", &self.nodes.len())
            .field("node_order", &self.node_order)
            .finish()
    }
}

impl ProcessCluster {
    pub fn new(config: ClusterConfig) -> Self {
        let port_allocator = PortAllocator::new(config.network.clone());
        let mut nodes = HashMap::new();
        let mut node_order = Vec::new();

        for i in 0..config.node_count {
            let id = NodeId(i);
            let address = port_allocator.allocate();
            let data_dir = config.base_data_dir.join(format!("node-{i}"));

            let node_config = NodeConfig {
                id,
                binary_path: config.binary_path.clone(),
                data_dir,
                address,
                extra_args: vec![],
                env_vars: vec![],
            };

            nodes.insert(id, Node::new(node_config));
            node_order.push(id);
        }

        Self {
            config,
            nodes,
            port_allocator,
            node_order,
        }
    }

    pub fn add_node(&mut self) -> NodeId {
        let id = NodeId(self.node_order.len() as u32);
        let address = self.port_allocator.allocate();
        let data_dir = self.config.base_data_dir.join(format!("node-{}", id.0));

        let node_config = NodeConfig {
            id,
            binary_path: self.config.binary_path.clone(),
            data_dir,
            address,
            extra_args: vec![],
            env_vars: vec![],
        };

        self.nodes.insert(id, Node::new(node_config));
        self.node_order.push(id);
        info!("added {} to cluster", id);
        id
    }

    pub fn remove_node(&mut self, id: NodeId) -> anyhow::Result<()> {
        if let Some(node) = self.nodes.get(&id) {
            if node.state() == NodeState::Running || node.state() == NodeState::Paused {
                node.stop()?;
            }
        }
        self.nodes
            .remove(&id)
            .ok_or_else(|| anyhow::anyhow!("node {} not found", id))?;
        self.node_order.retain(|n| *n != id);
        info!("removed {} from cluster", id);
        Ok(())
    }

    pub fn port_allocator(&self) -> &PortAllocator {
        &self.port_allocator
    }
}

impl Cluster for ProcessCluster {
    fn start_all(&self) -> anyhow::Result<()> {
        info!("starting all {} nodes", self.nodes.len());
        for id in &self.node_order {
            if let Some(node) = self.nodes.get(id) {
                node.start()?;
            }
        }
        Ok(())
    }

    fn stop_all(&self) -> anyhow::Result<()> {
        info!("stopping all {} nodes", self.nodes.len());
        let mut errors = Vec::new();
        for id in self.node_order.iter().rev() {
            if let Some(node) = self.nodes.get(id) {
                if let Err(e) = node.stop() {
                    warn!("failed to stop {}: {}", id, e);
                    errors.push(e);
                }
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "failed to stop {} nodes",
                errors.len()
            ))
        }
    }

    fn node(&self, id: NodeId) -> Option<&Node> {
        self.nodes.get(&id)
    }

    fn node_ids(&self) -> Vec<NodeId> {
        self.node_order.clone()
    }

    fn node_count(&self) -> usize {
        self.nodes.len()
    }

    fn running_nodes(&self) -> Vec<NodeId> {
        self.node_order
            .iter()
            .filter(|id| {
                self.nodes
                    .get(id)
                    .is_some_and(|n| n.state() == NodeState::Running)
            })
            .copied()
            .collect()
    }

    fn seed_addrs(&self) -> Vec<std::net::SocketAddr> {
        self.port_allocator.seed_addrs()
    }
}

impl Drop for ProcessCluster {
    fn drop(&mut self) {
        let _ = self.stop_all();
    }
}

pub async fn poll_for<F>(
    timeout: Duration,
    interval: Duration,
    mut predicate: F,
) -> bool
where
    F: FnMut() -> bool,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if predicate() {
            return true;
        }
        tokio::time::sleep(interval).await;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn test_cluster_config(node_count: u32, base_port: u16) -> ClusterConfig {
        ClusterConfig {
            node_count,
            binary_path: PathBuf::from("/usr/bin/false"),
            base_data_dir: PathBuf::from("/tmp/blockyard-cluster-test"),
            network: NetworkConfig {
                base_listen_port: base_port,
                base_gossip_port: base_port + 1000,
                host: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            },
        }
    }

    #[test]
    fn test_cluster_config_new() {
        let config = ClusterConfig::new(
            5,
            PathBuf::from("/usr/bin/blockyard"),
            PathBuf::from("/tmp/test"),
        );
        assert_eq!(config.node_count, 5);
        assert_eq!(config.binary_path, PathBuf::from("/usr/bin/blockyard"));
    }

    #[test]
    fn test_cluster_config_with_network() {
        let config = ClusterConfig::new(3, PathBuf::from("/bin/test"), PathBuf::from("/tmp/t"))
            .with_network(NetworkConfig {
                base_listen_port: 50000,
                base_gossip_port: 51000,
                host: "192.168.1.1".parse().unwrap(),
            });
        assert_eq!(config.network.base_listen_port, 50000);
    }

    #[test]
    fn test_process_cluster_creation() {
        let config = test_cluster_config(5, 22000);
        let cluster = ProcessCluster::new(config);

        assert_eq!(cluster.node_count(), 5);
        assert_eq!(cluster.node_ids().len(), 5);
        for i in 0..5 {
            let id = NodeId(i);
            assert!(cluster.node(id).is_some());
            assert_eq!(cluster.node(id).unwrap().state(), NodeState::Stopped);
        }
    }

    #[test]
    fn test_process_cluster_node_ids_ordered() {
        let config = test_cluster_config(3, 23000);
        let cluster = ProcessCluster::new(config);

        let ids = cluster.node_ids();
        assert_eq!(ids, vec![NodeId(0), NodeId(1), NodeId(2)]);
    }

    #[test]
    fn test_process_cluster_running_nodes_empty() {
        let config = test_cluster_config(3, 24000);
        let cluster = ProcessCluster::new(config);
        assert!(cluster.running_nodes().is_empty());
    }

    #[test]
    fn test_process_cluster_seed_addrs() {
        let config = test_cluster_config(3, 25000);
        let cluster = ProcessCluster::new(config);
        let seeds = cluster.seed_addrs();
        assert_eq!(seeds.len(), 3);
    }

    #[test]
    fn test_process_cluster_add_node() {
        let config = test_cluster_config(2, 26000);
        let mut cluster = ProcessCluster::new(config);

        assert_eq!(cluster.node_count(), 2);
        let new_id = cluster.add_node();
        assert_eq!(new_id, NodeId(2));
        assert_eq!(cluster.node_count(), 3);
        assert!(cluster.node(new_id).is_some());
    }

    #[test]
    fn test_process_cluster_remove_node() {
        let config = test_cluster_config(3, 27000);
        let mut cluster = ProcessCluster::new(config);

        assert_eq!(cluster.node_count(), 3);
        cluster.remove_node(NodeId(1)).unwrap();
        assert_eq!(cluster.node_count(), 2);
        assert!(cluster.node(NodeId(1)).is_none());
        assert_eq!(cluster.node_ids(), vec![NodeId(0), NodeId(2)]);
    }

    #[test]
    fn test_process_cluster_remove_nonexistent_node() {
        let config = test_cluster_config(2, 28000);
        let mut cluster = ProcessCluster::new(config);

        let result = cluster.remove_node(NodeId(99));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_process_cluster_nonexistent_node() {
        let config = test_cluster_config(2, 29000);
        let cluster = ProcessCluster::new(config);
        assert!(cluster.node(NodeId(99)).is_none());
    }

    #[test]
    fn test_process_cluster_with_real_processes() {
        let dir = tempfile::tempdir().unwrap();
        let config = ClusterConfig {
            node_count: 3,
            binary_path: PathBuf::from("/bin/sleep"),
            base_data_dir: dir.path().to_path_buf(),
            network: NetworkConfig {
                base_listen_port: 29100,
                base_gossip_port: 29200,
                host: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            },
        };

        for i in 0..3 {
            let node_dir = dir.path().join(format!("node-{i}"));
            std::fs::create_dir_all(&node_dir).unwrap();
        }

        let cluster = ProcessCluster::new(config);

        for id in cluster.node_ids() {
            let node = cluster.node(id).unwrap();
            let mut cfg = node.config().clone();
            cfg.extra_args = vec!["60".to_string()];
        }

        assert_eq!(cluster.running_nodes().len(), 0);
    }

    #[tokio::test]
    async fn test_poll_for_immediate_success() {
        let result = poll_for(
            Duration::from_secs(1),
            Duration::from_millis(10),
            || true,
        )
        .await;
        assert!(result);
    }

    #[tokio::test]
    async fn test_poll_for_timeout() {
        let result = poll_for(
            Duration::from_millis(50),
            Duration::from_millis(10),
            || false,
        )
        .await;
        assert!(!result);
    }

    #[tokio::test]
    async fn test_poll_for_eventual_success() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let c = counter.clone();
        let result = poll_for(
            Duration::from_secs(1),
            Duration::from_millis(10),
            move || {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst) >= 3
            },
        )
        .await;
        assert!(result);
    }
}
