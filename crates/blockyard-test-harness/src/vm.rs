use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Child;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tracing::{debug, info, warn};

use crate::network::NodeAddress;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u32);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node-{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeState {
    Stopped,
    Starting,
    Running,
    Paused,
    Crashed,
}

impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeState::Stopped => write!(f, "stopped"),
            NodeState::Starting => write!(f, "starting"),
            NodeState::Running => write!(f, "running"),
            NodeState::Paused => write!(f, "paused"),
            NodeState::Crashed => write!(f, "crashed"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub id: NodeId,
    pub binary_path: PathBuf,
    pub data_dir: PathBuf,
    pub address: NodeAddress,
    pub extra_args: Vec<String>,
    pub env_vars: Vec<(String, String)>,
}

pub struct Node {
    config: NodeConfig,
    state: RwLock<NodeState>,
    process: RwLock<Option<Child>>,
    started_at: RwLock<Option<Instant>>,
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("config", &self.config)
            .field("state", &*self.state.read())
            .field("has_process", &self.process.read().is_some())
            .finish()
    }
}

impl Node {
    pub fn new(config: NodeConfig) -> Self {
        Self {
            config,
            state: RwLock::new(NodeState::Stopped),
            process: RwLock::new(None),
            started_at: RwLock::new(None),
        }
    }

    pub fn id(&self) -> NodeId {
        self.config.id
    }

    pub fn config(&self) -> &NodeConfig {
        &self.config
    }

    pub fn state(&self) -> NodeState {
        *self.state.read()
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.config.address.listen_addr
    }

    pub fn gossip_addr(&self) -> SocketAddr {
        self.config.address.gossip_addr
    }

    pub fn pid(&self) -> Option<u32> {
        self.process.read().as_ref().map(|c| c.id())
    }

    pub fn uptime(&self) -> Option<Duration> {
        self.started_at.read().map(|t| t.elapsed())
    }

    pub fn start(&self) -> anyhow::Result<()> {
        let current_state = *self.state.read();
        match current_state {
            NodeState::Running => {
                warn!("{}: already running", self.config.id);
                return Ok(());
            }
            NodeState::Paused => {
                anyhow::bail!("{}: cannot start a paused node, use resume()", self.config.id);
            }
            _ => {}
        }

        *self.state.write() = NodeState::Starting;
        info!("{}: starting process", self.config.id);

        std::fs::create_dir_all(&self.config.data_dir)?;

        let config_path = self.config.data_dir.join("node.toml");
        let config_toml = self.generate_config_toml();
        std::fs::write(&config_path, config_toml)?;

        let mut cmd = std::process::Command::new(&self.config.binary_path);
        cmd.arg("--config")
            .arg(&config_path)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        for arg in &self.config.extra_args {
            cmd.arg(arg);
        }

        for (key, value) in &self.config.env_vars {
            cmd.env(key, value);
        }

        let child = cmd.spawn()?;
        info!("{}: spawned pid={}", self.config.id, child.id());

        *self.process.write() = Some(child);
        *self.state.write() = NodeState::Running;
        *self.started_at.write() = Some(Instant::now());

        Ok(())
    }

    pub fn stop(&self) -> anyhow::Result<()> {
        let current_state = *self.state.read();
        if current_state == NodeState::Stopped {
            debug!("{}: already stopped", self.config.id);
            return Ok(());
        }

        info!("{}: stopping gracefully", self.config.id);

        if let Some(ref mut child) = *self.process.write() {
            let _ = child.kill();
            let _ = child.wait();
        }
        *self.process.write() = None;
        *self.state.write() = NodeState::Stopped;
        *self.started_at.write() = None;

        Ok(())
    }

    pub fn kill(&self) -> anyhow::Result<()> {
        info!("{}: sending SIGKILL", self.config.id);

        let pid = self.pid();
        if let Some(pid) = pid {
            let pid = nix::unistd::Pid::from_raw(pid as i32);
            let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGKILL);
        }

        if let Some(ref mut child) = *self.process.write() {
            let _ = child.wait();
        }
        *self.process.write() = None;
        *self.state.write() = NodeState::Crashed;
        *self.started_at.write() = None;

        Ok(())
    }

    pub fn pause(&self) -> anyhow::Result<()> {
        let current_state = *self.state.read();
        if current_state != NodeState::Running {
            anyhow::bail!("{}: can only pause a running node, current={}", self.config.id, current_state);
        }

        info!("{}: sending SIGSTOP", self.config.id);

        if let Some(pid) = self.pid() {
            let pid = nix::unistd::Pid::from_raw(pid as i32);
            nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGSTOP)?;
        }
        *self.state.write() = NodeState::Paused;

        Ok(())
    }

    pub fn resume(&self) -> anyhow::Result<()> {
        let current_state = *self.state.read();
        if current_state != NodeState::Paused {
            anyhow::bail!("{}: can only resume a paused node, current={}", self.config.id, current_state);
        }

        info!("{}: sending SIGCONT", self.config.id);

        if let Some(pid) = self.pid() {
            let pid = nix::unistd::Pid::from_raw(pid as i32);
            nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGCONT)?;
        }
        *self.state.write() = NodeState::Running;

        Ok(())
    }

    pub fn is_alive(&self) -> bool {
        if let Some(pid) = self.pid() {
            let pid = nix::unistd::Pid::from_raw(pid as i32);
            nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGCONT).is_ok()
        } else {
            false
        }
    }

    fn generate_config_toml(&self) -> String {
        let listen = self.config.address.listen_addr;
        let gossip = self.config.address.gossip_addr;
        let disk_path = self.config.data_dir.join("disk0");

        format!(
            r#"listen_addr = "{listen}"

[storage]
disk_paths = ["{disk_path}"]
max_background_io = 4
scrub_interval_secs = 86400

[raft]
election_timeout_min_ms = 150
election_timeout_max_ms = 300
heartbeat_interval_ms = 50
max_entries_per_batch = 64
snapshot_threshold = 10000

[gossip]
bind_addr = "{gossip}"
seed_nodes = []
gossip_interval_ms = 1000
suspicion_mult = 4

[protocol]
max_message_size = 67108864
connect_timeout_ms = 5000
request_timeout_ms = 30000
"#,
            disk_path = disk_path.display(),
        )
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(ref mut child) = *self.process.write() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    fn test_node_config(id: u32) -> NodeConfig {
        let listen_port = 10000 + id as u16 * 10;
        let gossip_port = listen_port + 1;
        NodeConfig {
            id: NodeId(id),
            binary_path: PathBuf::from("/usr/bin/false"),
            data_dir: PathBuf::from(format!("/tmp/blockyard-test-{id}")),
            address: NodeAddress {
                listen_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, listen_port)),
                gossip_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, gossip_port)),
            },
            extra_args: vec![],
            env_vars: vec![],
        }
    }

    #[test]
    fn test_node_id_display() {
        let id = NodeId(3);
        assert_eq!(format!("{id}"), "node-3");
    }

    #[test]
    fn test_node_state_display() {
        assert_eq!(format!("{}", NodeState::Running), "running");
        assert_eq!(format!("{}", NodeState::Stopped), "stopped");
        assert_eq!(format!("{}", NodeState::Starting), "starting");
        assert_eq!(format!("{}", NodeState::Paused), "paused");
        assert_eq!(format!("{}", NodeState::Crashed), "crashed");
    }

    #[test]
    fn test_node_new_starts_stopped() {
        let config = test_node_config(1);
        let node = Node::new(config);
        assert_eq!(node.state(), NodeState::Stopped);
        assert!(node.pid().is_none());
        assert!(node.uptime().is_none());
    }

    #[test]
    fn test_node_stop_when_already_stopped() {
        let config = test_node_config(2);
        let node = Node::new(config);
        assert!(node.stop().is_ok());
        assert_eq!(node.state(), NodeState::Stopped);
    }

    #[test]
    fn test_node_kill_when_stopped() {
        let config = test_node_config(3);
        let node = Node::new(config);
        assert!(node.kill().is_ok());
        assert_eq!(node.state(), NodeState::Crashed);
    }

    #[test]
    fn test_node_pause_when_not_running() {
        let config = test_node_config(4);
        let node = Node::new(config);
        let result = node.pause();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("can only pause a running node"));
    }

    #[test]
    fn test_node_resume_when_not_paused() {
        let config = test_node_config(5);
        let node = Node::new(config);
        let result = node.resume();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("can only resume a paused node"));
    }

    #[test]
    fn test_node_addresses() {
        let config = test_node_config(6);
        let listen = config.address.listen_addr;
        let gossip = config.address.gossip_addr;
        let node = Node::new(config);
        assert_eq!(node.listen_addr(), listen);
        assert_eq!(node.gossip_addr(), gossip);
    }

    #[test]
    fn test_node_is_alive_when_stopped() {
        let config = test_node_config(7);
        let node = Node::new(config);
        assert!(!node.is_alive());
    }

    #[test]
    fn test_node_start_with_real_process() {
        let dir = tempfile::tempdir().unwrap();
        let config = NodeConfig {
            id: NodeId(100),
            binary_path: PathBuf::from("/bin/sleep"),
            data_dir: dir.path().to_path_buf(),
            address: NodeAddress {
                listen_addr: "127.0.0.1:19100".parse().unwrap(),
                gossip_addr: "127.0.0.1:19101".parse().unwrap(),
            },
            extra_args: vec!["60".to_string()],
            env_vars: vec![],
        };
        let node = Node::new(config);

        node.start().unwrap();
        assert_eq!(node.state(), NodeState::Running);
        assert!(node.pid().is_some());
        assert!(node.is_alive());
        assert!(node.uptime().is_some());

        node.stop().unwrap();
        assert_eq!(node.state(), NodeState::Stopped);
        assert!(node.pid().is_none());
    }

    #[test]
    fn test_node_kill_running_process() {
        let dir = tempfile::tempdir().unwrap();
        let config = NodeConfig {
            id: NodeId(101),
            binary_path: PathBuf::from("/bin/sleep"),
            data_dir: dir.path().to_path_buf(),
            address: NodeAddress {
                listen_addr: "127.0.0.1:19110".parse().unwrap(),
                gossip_addr: "127.0.0.1:19111".parse().unwrap(),
            },
            extra_args: vec!["60".to_string()],
            env_vars: vec![],
        };
        let node = Node::new(config);

        node.start().unwrap();
        assert_eq!(node.state(), NodeState::Running);

        node.kill().unwrap();
        assert_eq!(node.state(), NodeState::Crashed);
        assert!(node.pid().is_none());
    }

    #[test]
    fn test_node_pause_resume() {
        let dir = tempfile::tempdir().unwrap();
        let config = NodeConfig {
            id: NodeId(102),
            binary_path: PathBuf::from("/bin/sleep"),
            data_dir: dir.path().to_path_buf(),
            address: NodeAddress {
                listen_addr: "127.0.0.1:19120".parse().unwrap(),
                gossip_addr: "127.0.0.1:19121".parse().unwrap(),
            },
            extra_args: vec!["60".to_string()],
            env_vars: vec![],
        };
        let node = Node::new(config);

        node.start().unwrap();
        assert_eq!(node.state(), NodeState::Running);

        node.pause().unwrap();
        assert_eq!(node.state(), NodeState::Paused);

        node.resume().unwrap();
        assert_eq!(node.state(), NodeState::Running);

        node.stop().unwrap();
    }

    #[test]
    fn test_generate_config_toml() {
        let config = test_node_config(8);
        let node = Node::new(config);
        let toml = node.generate_config_toml();
        assert!(toml.contains("listen_addr"));
        assert!(toml.contains("[storage]"));
        assert!(toml.contains("[raft]"));
        assert!(toml.contains("[gossip]"));
        assert!(toml.contains("[protocol]"));
    }

    #[test]
    fn test_node_start_already_running() {
        let dir = tempfile::tempdir().unwrap();
        let config = NodeConfig {
            id: NodeId(103),
            binary_path: PathBuf::from("/bin/sleep"),
            data_dir: dir.path().to_path_buf(),
            address: NodeAddress {
                listen_addr: "127.0.0.1:19130".parse().unwrap(),
                gossip_addr: "127.0.0.1:19131".parse().unwrap(),
            },
            extra_args: vec!["60".to_string()],
            env_vars: vec![],
        };
        let node = Node::new(config);
        node.start().unwrap();
        assert!(node.start().is_ok());
        node.stop().unwrap();
    }

    #[test]
    fn test_node_start_paused_fails() {
        let dir = tempfile::tempdir().unwrap();
        let config = NodeConfig {
            id: NodeId(104),
            binary_path: PathBuf::from("/bin/sleep"),
            data_dir: dir.path().to_path_buf(),
            address: NodeAddress {
                listen_addr: "127.0.0.1:19140".parse().unwrap(),
                gossip_addr: "127.0.0.1:19141".parse().unwrap(),
            },
            extra_args: vec!["60".to_string()],
            env_vars: vec![],
        };
        let node = Node::new(config);
        node.start().unwrap();
        node.pause().unwrap();
        let result = node.start();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot start a paused node"));

        node.resume().unwrap();
        node.stop().unwrap();
    }

    #[test]
    fn test_node_drop_cleans_up() {
        let dir = tempfile::tempdir().unwrap();
        let config = NodeConfig {
            id: NodeId(105),
            binary_path: PathBuf::from("/bin/sleep"),
            data_dir: dir.path().to_path_buf(),
            address: NodeAddress {
                listen_addr: "127.0.0.1:19150".parse().unwrap(),
                gossip_addr: "127.0.0.1:19151".parse().unwrap(),
            },
            extra_args: vec!["60".to_string()],
            env_vars: vec![],
        };
        let pid;
        {
            let node = Node::new(config);
            node.start().unwrap();
            pid = node.pid().unwrap();
        }
        std::thread::sleep(Duration::from_millis(50));
        let result = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid as i32),
            None,
        );
        assert!(result.is_err());
    }
}
