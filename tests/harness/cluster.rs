use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::time::sleep;

pub const DEFAULT_NODE_COUNT: usize = 5;
pub const SSH_PORT_BASE: u16 = 2200;
pub const BLOCKYARD_PORT_BASE: u16 = 7400;
pub const QEMU_MONITOR_PORT_BASE: u16 = 4440;

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub node_count: usize,
    pub base_image: PathBuf,
    pub blockyard_binary: PathBuf,
    pub work_dir: PathBuf,
    pub ram_mb: u32,
    pub cpus: u32,
    pub disk_size_gb: u32,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
        let base = PathBuf::from(&manifest_dir);
        Self {
            node_count: DEFAULT_NODE_COUNT,
            base_image: base.join("images/ubuntu-noble.img"),
            blockyard_binary: PathBuf::from(
                std::env::var("CARGO_TARGET_DIR")
                    .unwrap_or_else(|_| format!("{manifest_dir}/../target")),
            )
            .join("release/blockyard"),
            work_dir: base.join(".work"),
            ram_mb: 1024,
            cpus: 2,
            disk_size_gb: 10,
        }
    }
}

#[derive(Debug)]
pub struct TestCluster {
    config: ClusterConfig,
    nodes: HashMap<usize, TestNode>,
}

#[derive(Debug)]
pub struct TestNode {
    pub id: usize,
    pub name: String,
    pub ssh_port: u16,
    pub blockyard_port: u16,
    pub data_port: u16,
    pub monitor_port: u16,
    pub qemu_process: Option<Child>,
    pub disk_path: PathBuf,
    pub state: NodeTestState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeTestState {
    Stopped,
    Running,
    Paused,
    Crashed,
}

impl TestCluster {
    pub fn new(config: ClusterConfig) -> Self {
        let mut nodes = HashMap::new();
        for i in 0..config.node_count {
            nodes.insert(
                i,
                TestNode {
                    id: i,
                    name: format!("node-{i}"),
                    ssh_port: SSH_PORT_BASE + i as u16,
                    blockyard_port: BLOCKYARD_PORT_BASE + (i as u16 * 10),
                    data_port: BLOCKYARD_PORT_BASE + (i as u16 * 10) + 1,
                    monitor_port: QEMU_MONITOR_PORT_BASE + i as u16,
                    qemu_process: None,
                    disk_path: config.work_dir.join(format!("node-{i}-disk.qcow2")),
                    state: NodeTestState::Stopped,
                },
            );
        }
        Self { config, nodes }
    }

    pub fn assume_running(config: ClusterConfig) -> Self {
        let mut cluster = Self::new(config);
        for node in cluster.nodes.values_mut() {
            node.state = NodeTestState::Running;
        }
        cluster
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn node(&self, id: usize) -> Option<&TestNode> {
        self.nodes.get(&id)
    }

    pub fn node_mut(&mut self, id: usize) -> Option<&mut TestNode> {
        self.nodes.get_mut(&id)
    }

    pub fn running_nodes(&self) -> Vec<&TestNode> {
        self.nodes
            .values()
            .filter(|n| n.state == NodeTestState::Running)
            .collect()
    }

    pub fn seed_addrs(&self) -> Vec<SocketAddr> {
        self.nodes
            .values()
            .take(3)
            .map(|n| format!("127.0.0.1:{}", n.blockyard_port).parse().unwrap())
            .collect()
    }

    pub async fn provision(&mut self) -> anyhow::Result<()> {
        std::fs::create_dir_all(&self.config.work_dir)?;

        for node in self.nodes.values() {
            if !node.disk_path.exists() {
                let status = Command::new("qemu-img")
                    .args([
                        "create",
                        "-f",
                        "qcow2",
                        "-b",
                        self.config.base_image.to_str().unwrap(),
                        "-F",
                        "qcow2",
                        node.disk_path.to_str().unwrap(),
                        &format!("{}G", self.config.disk_size_gb),
                    ])
                    .status()
                    .await?;
                if !status.success() {
                    anyhow::bail!("qemu-img create failed for {}", node.name);
                }
            }
        }
        Ok(())
    }

    pub async fn start_node(&mut self, id: usize) -> anyhow::Result<()> {
        let node = self
            .nodes
            .get_mut(&id)
            .ok_or_else(|| anyhow::anyhow!("node {id} not found"))?;

        if node.state == NodeTestState::Running {
            return Ok(());
        }

        let child = Command::new("qemu-system-x86_64")
            .args([
                "-m",
                &format!("{}M", self.config.ram_mb),
                "-smp",
                &format!("{}", self.config.cpus),
                "-drive",
                &format!("file={},format=qcow2", node.disk_path.display()),
                "-drive",
                &format!(
                    "file={}/node-{id}-zfs.qcow2,format=qcow2",
                    self.config.work_dir.display()
                ),
                "-netdev",
                &format!(
                    "user,id=net0,hostfwd=tcp::{}-:22,hostfwd=tcp::{}-:7400,hostfwd=tcp::{}-:7401",
                    node.ssh_port, node.blockyard_port, node.data_port,
                ),
                "-device",
                "virtio-net-pci,netdev=net0",
                "-monitor",
                &format!("tcp:127.0.0.1:{},server,nowait", node.monitor_port),
                "-nographic",
                "-enable-kvm",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        node.qemu_process = Some(child);
        node.state = NodeTestState::Running;
        Ok(())
    }

    pub async fn start_all(&mut self) -> anyhow::Result<()> {
        let ids: Vec<usize> = self.nodes.keys().copied().collect();
        for id in ids {
            self.start_node(id).await?;
        }
        Ok(())
    }

    pub async fn stop_node(&mut self, id: usize) -> anyhow::Result<()> {
        if let Some(node) = self.nodes.get_mut(&id) {
            if let Some(mut proc) = node.qemu_process.take() {
                proc.kill().await?;
            }
            node.state = NodeTestState::Stopped;
        }
        Ok(())
    }

    pub async fn stop_all(&mut self) -> anyhow::Result<()> {
        let ids: Vec<usize> = self.nodes.keys().copied().collect();
        for id in ids {
            self.stop_node(id).await?;
        }
        Ok(())
    }

    pub async fn ssh_exec(&self, node_id: usize, cmd: &str) -> anyhow::Result<String> {
        let node = self
            .nodes
            .get(&node_id)
            .ok_or_else(|| anyhow::anyhow!("node {node_id} not found"))?;

        let ssh_key = self.config.work_dir.join("id_test");
        let output = Command::new("ssh")
            .args([
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "ConnectTimeout=5",
                "-i",
                ssh_key.to_str().unwrap_or("tests/.work/id_test"),
                "-p",
                &node.ssh_port.to_string(),
                "root@127.0.0.1",
                cmd,
            ])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("ssh exec on {} failed: {}", node.name, stderr);
        }

        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    pub async fn scp_to(&self, node_id: usize, local: &Path, remote: &str) -> anyhow::Result<()> {
        let node = self
            .nodes
            .get(&node_id)
            .ok_or_else(|| anyhow::anyhow!("node {node_id} not found"))?;

        let ssh_key = self.config.work_dir.join("id_test");
        let status = Command::new("scp")
            .args([
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-i",
                ssh_key.to_str().unwrap_or("tests/.work/id_test"),
                "-P",
                &node.ssh_port.to_string(),
                local.to_str().unwrap(),
                &format!("root@127.0.0.1:{remote}"),
            ])
            .status()
            .await?;

        if !status.success() {
            anyhow::bail!("scp to {} failed", node.name);
        }
        Ok(())
    }

    pub async fn wait_for_ssh(&self, node_id: usize, timeout: Duration) -> anyhow::Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if tokio::time::Instant::now() > deadline {
                anyhow::bail!("timeout waiting for SSH on node {node_id}");
            }
            match self.ssh_exec(node_id, "echo ready").await {
                Ok(out) if out.trim() == "ready" => return Ok(()),
                _ => sleep(Duration::from_secs(2)).await,
            }
        }
    }

    pub async fn deploy_blockyard(&self, node_id: usize) -> anyhow::Result<()> {
        self.scp_to(
            node_id,
            &self.config.blockyard_binary,
            "/usr/local/bin/blockyard",
        )
        .await?;
        self.ssh_exec(node_id, "chmod +x /usr/local/bin/blockyard")
            .await?;
        Ok(())
    }

    pub async fn start_blockyard(&self, node_id: usize) -> anyhow::Result<()> {
        let node = self
            .nodes
            .get(&node_id)
            .ok_or_else(|| anyhow::anyhow!("node {node_id} not found"))?;

        let seeds: Vec<String> = self
            .seed_addrs()
            .iter()
            .map(|a| format!("\"{}\"", a))
            .collect();
        let seeds_str = seeds.join(", ");

        let config = format!(
            r#"
[node]
name = "{name}"
listen = "0.0.0.0:7400"
data_listen = "0.0.0.0:7401"
metrics_listen = "0.0.0.0:7402"

[cluster]
seeds = [{seeds}]

[storage]
zfs_pool = "blockyard"
"#,
            name = node.name,
            seeds = seeds_str,
        );

        self.ssh_exec(
            node_id,
            &format!(
                "mkdir -p /etc/blockyard && cat > /etc/blockyard/config.toml << 'CFGEOF'\n{config}\nCFGEOF"
            ),
        )
        .await?;

        self.ssh_exec(
            node_id,
            "nohup blockyard start --config /etc/blockyard/config.toml > /var/log/blockyard.log 2>&1 &",
        )
        .await?;

        sleep(Duration::from_secs(1)).await;
        Ok(())
    }

    pub async fn kill_blockyard(&self, node_id: usize) -> anyhow::Result<()> {
        self.ssh_exec(node_id, "pkill -9 blockyard || true").await?;
        Ok(())
    }

    pub async fn pause_blockyard(&self, node_id: usize) -> anyhow::Result<()> {
        self.ssh_exec(node_id, "pkill -STOP blockyard || true")
            .await?;
        Ok(())
    }

    pub async fn resume_blockyard(&self, node_id: usize) -> anyhow::Result<()> {
        self.ssh_exec(node_id, "pkill -CONT blockyard || true")
            .await?;
        Ok(())
    }
}

impl TestNode {
    pub fn ssh_addr(&self) -> String {
        format!("127.0.0.1:{}", self.ssh_port)
    }

    pub fn blockyard_addr(&self) -> SocketAddr {
        format!("127.0.0.1:{}", self.blockyard_port)
            .parse()
            .unwrap()
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        for node in self.nodes.values_mut() {
            if let Some(mut proc) = node.qemu_process.take() {
                let _ = proc.start_kill();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_config_default() {
        let config = ClusterConfig::default();
        assert_eq!(config.node_count, 5);
        assert_eq!(config.ram_mb, 1024);
        assert_eq!(config.cpus, 2);
    }

    #[test]
    fn test_cluster_new() {
        let cluster = TestCluster::new(ClusterConfig::default());
        assert_eq!(cluster.node_count(), 5);
        assert!(cluster.node(0).is_some());
        assert!(cluster.node(4).is_some());
        assert!(cluster.node(5).is_none());
    }

    #[test]
    fn test_cluster_seed_addrs() {
        let cluster = TestCluster::new(ClusterConfig::default());
        let seeds = cluster.seed_addrs();
        assert_eq!(seeds.len(), 3);
    }

    #[test]
    fn test_node_initial_state() {
        let cluster = TestCluster::new(ClusterConfig::default());
        let node = cluster.node(0).unwrap();
        assert_eq!(node.state, NodeTestState::Stopped);
        assert_eq!(node.name, "node-0");
        assert_eq!(node.ssh_port, SSH_PORT_BASE);
    }

    #[test]
    fn test_node_ssh_addr() {
        let cluster = TestCluster::new(ClusterConfig::default());
        let node = cluster.node(0).unwrap();
        assert_eq!(node.ssh_addr(), format!("127.0.0.1:{SSH_PORT_BASE}"));
    }

    #[test]
    fn test_node_blockyard_addr() {
        let cluster = TestCluster::new(ClusterConfig::default());
        let node = cluster.node(0).unwrap();
        let addr = node.blockyard_addr();
        assert_eq!(addr.port(), BLOCKYARD_PORT_BASE);
    }

    #[test]
    fn test_running_nodes_initially_empty() {
        let cluster = TestCluster::new(ClusterConfig::default());
        assert!(cluster.running_nodes().is_empty());
    }

    #[test]
    fn test_cluster_unique_ports() {
        let cluster = TestCluster::new(ClusterConfig::default());
        let mut ssh_ports = std::collections::HashSet::new();
        let mut by_ports = std::collections::HashSet::new();
        for node in cluster.nodes.values() {
            assert!(ssh_ports.insert(node.ssh_port));
            assert!(by_ports.insert(node.blockyard_port));
        }
    }
}
