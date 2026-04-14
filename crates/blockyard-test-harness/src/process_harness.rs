use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tempfile::TempDir;
use tracing::{debug, info};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessNodeState {
    Stopped,
    Running,
    Paused,
    Crashed,
}

pub struct ProcessNode {
    index: usize,
    data_addr: SocketAddr,
    gossip_addr: SocketAddr,
    mgmt_addr: SocketAddr,
    raft_addr: SocketAddr,
    data_dir: PathBuf,
    _temp_dir: Option<TempDir>,
    binary_path: PathBuf,
    seed_nodes: Vec<SocketAddr>,
    process: RwLock<Option<Child>>,
    state: RwLock<ProcessNodeState>,
}

impl std::fmt::Debug for ProcessNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessNode")
            .field("index", &self.index)
            .field("data_addr", &self.data_addr)
            .field("mgmt_addr", &self.mgmt_addr)
            .field("state", &*self.state.read())
            .finish()
    }
}

impl ProcessNode {
    pub fn new(
        index: usize,
        base_port: u16,
        binary_path: PathBuf,
        seed_nodes: Vec<SocketAddr>,
    ) -> Self {
        let offset = index as u16 * 100;
        let data_addr: SocketAddr = format!("127.0.0.1:{}", base_port + offset).parse().unwrap();
        // Gossip (UDP) and mgmt API (TCP) share the same port number — no conflict.
        let gossip_addr: SocketAddr = format!("127.0.0.1:{}", base_port + offset + 1)
            .parse()
            .unwrap();
        let mgmt_addr: SocketAddr = format!("127.0.0.1:{}", base_port + offset + 1)
            .parse()
            .unwrap();
        let raft_addr: SocketAddr = format!("127.0.0.1:{}", base_port + offset + 10)
            .parse()
            .unwrap();

        let temp_dir = TempDir::new().expect("create temp dir");
        let data_dir = temp_dir.path().to_path_buf();

        Self {
            index,
            data_addr,
            gossip_addr,
            mgmt_addr,
            raft_addr,
            data_dir,
            _temp_dir: Some(temp_dir),
            binary_path,
            seed_nodes,
            process: RwLock::new(None),
            state: RwLock::new(ProcessNodeState::Stopped),
        }
    }

    pub fn with_existing_data_dir(
        index: usize,
        base_port: u16,
        binary_path: PathBuf,
        seed_nodes: Vec<SocketAddr>,
        data_dir: PathBuf,
    ) -> Self {
        let offset = index as u16 * 100;
        let data_addr: SocketAddr = format!("127.0.0.1:{}", base_port + offset).parse().unwrap();
        let gossip_addr: SocketAddr = format!("127.0.0.1:{}", base_port + offset + 1)
            .parse()
            .unwrap();
        let mgmt_addr: SocketAddr = format!("127.0.0.1:{}", base_port + offset + 1)
            .parse()
            .unwrap();
        let raft_addr: SocketAddr = format!("127.0.0.1:{}", base_port + offset + 10)
            .parse()
            .unwrap();

        Self {
            index,
            data_addr,
            gossip_addr,
            mgmt_addr,
            raft_addr,
            data_dir,
            _temp_dir: None,
            binary_path,
            seed_nodes,
            process: RwLock::new(None),
            state: RwLock::new(ProcessNodeState::Stopped),
        }
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn data_addr(&self) -> SocketAddr {
        self.data_addr
    }

    pub fn gossip_addr(&self) -> SocketAddr {
        self.gossip_addr
    }

    pub fn mgmt_addr(&self) -> SocketAddr {
        self.mgmt_addr
    }

    pub fn raft_addr(&self) -> SocketAddr {
        self.raft_addr
    }

    pub fn mgmt_url(&self) -> String {
        format!("http://{}", self.mgmt_addr)
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn state(&self) -> ProcessNodeState {
        *self.state.read()
    }

    pub fn pid(&self) -> Option<u32> {
        self.process.read().as_ref().map(|c| c.id())
    }

    pub fn start(&self) -> anyhow::Result<()> {
        let current = *self.state.read();
        if current == ProcessNodeState::Running {
            return Ok(());
        }
        if current == ProcessNodeState::Paused {
            anyhow::bail!("node {} is paused, use resume()", self.index);
        }

        std::fs::create_dir_all(&self.data_dir)?;
        let disk_path = self.data_dir.join("disk0");
        std::fs::create_dir_all(&disk_path)?;
        // Write the XFS marker so disk validation passes in test environments
        // (tests use tmpfs, not real XFS)
        std::fs::write(disk_path.join(".blockyard_xfs_ok"), "")?;

        let config_path = self.data_dir.join("config.toml");
        let config_toml = self.generate_config();
        std::fs::write(&config_path, &config_toml)?;

        let log_path = self.data_dir.join("node.log");

        let log_file = std::fs::File::create(&log_path)?;
        let stderr_file = log_file.try_clone()?;

        let child = Command::new(&self.binary_path)
            .arg("--config")
            .arg(&config_path)
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(stderr_file))
            .env("RUST_LOG", "info")
            .spawn()?;

        info!(
            "node {} started pid={} data_addr={} mgmt_addr={}",
            self.index,
            child.id(),
            self.data_addr,
            self.mgmt_addr
        );

        *self.process.write() = Some(child);
        *self.state.write() = ProcessNodeState::Running;
        Ok(())
    }

    pub fn kill(&self) -> anyhow::Result<()> {
        info!("killing node {} (SIGKILL)", self.index);
        if let Some(pid) = self.pid() {
            let pid = nix::unistd::Pid::from_raw(pid as i32);
            let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGKILL);
        }
        if let Some(ref mut child) = *self.process.write() {
            let _ = child.wait();
        }
        *self.process.write() = None;
        *self.state.write() = ProcessNodeState::Crashed;
        Ok(())
    }

    pub fn stop(&self) -> anyhow::Result<()> {
        info!("sending SIGSTOP to node {}", self.index);
        if let Some(pid) = self.pid() {
            let pid = nix::unistd::Pid::from_raw(pid as i32);
            nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGSTOP)?;
        }
        *self.state.write() = ProcessNodeState::Paused;
        Ok(())
    }

    pub fn resume(&self) -> anyhow::Result<()> {
        info!("sending SIGCONT to node {}", self.index);
        if let Some(pid) = self.pid() {
            let pid = nix::unistd::Pid::from_raw(pid as i32);
            nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGCONT)?;
        }
        *self.state.write() = ProcessNodeState::Running;
        Ok(())
    }

    pub async fn wait_ready(&self, timeout: Duration) -> anyhow::Result<()> {
        let start = Instant::now();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()?;
        let url = format!("{}/api/v1/cluster/status", self.mgmt_url());

        while start.elapsed() < timeout {
            match client.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    info!("node {} is ready", self.index);
                    return Ok(());
                }
                _ => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
        anyhow::bail!(
            "node {} did not become ready within {:?}",
            self.index,
            timeout
        )
    }

    fn generate_config(&self) -> String {
        let disk_path = self.data_dir.join("disk0");
        let seed_nodes_str = if self.seed_nodes.is_empty() {
            "[]".to_string()
        } else {
            let seeds: Vec<String> = self
                .seed_nodes
                .iter()
                .map(|s| format!("\"{}\"", s))
                .collect();
            format!("[{}]", seeds.join(", "))
        };

        format!(
            r#"name = "node-{index}"
listen_addr = "{data_addr}"
data_dir = "{data_dir}"

[storage]
disk_paths = ["{disk_path}"]
max_background_io = 4
scrub_interval_secs = 86400

[raft]
election_timeout_min_ms = 300
election_timeout_max_ms = 600
heartbeat_interval_ms = 100
max_entries_per_batch = 64
snapshot_threshold = 10000
bind_addr = "{raft_addr}"

[gossip]
bind_addr = "{gossip_addr}"
seed_nodes = {seed_nodes}
gossip_interval_ms = 500
suspicion_mult = 4

[protocol]
max_message_size = 67108864
connect_timeout_ms = 5000
request_timeout_ms = 30000
mgmt_addr = "{mgmt_addr}"
"#,
            index = self.index,
            data_addr = self.data_addr,
            data_dir = self.data_dir.display(),
            disk_path = disk_path.display(),
            raft_addr = self.raft_addr,
            gossip_addr = self.gossip_addr,
            seed_nodes = seed_nodes_str,
            mgmt_addr = self.mgmt_addr,
        )
    }
}

impl Drop for ProcessNode {
    fn drop(&mut self) {
        if let Some(ref mut child) = *self.process.write() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

pub struct RealProcessCluster {
    nodes: Vec<ProcessNode>,
    binary_path: PathBuf,
    base_port: u16,
}

impl std::fmt::Debug for RealProcessCluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealProcessCluster")
            .field("node_count", &self.nodes.len())
            .field("base_port", &self.base_port)
            .finish()
    }
}

impl RealProcessCluster {
    pub fn new(node_count: usize, binary_path: PathBuf, base_port: u16) -> Self {
        let mut nodes = Vec::new();

        for i in 0..node_count {
            let seed_nodes = if i == 0 {
                vec![]
            } else {
                vec![
                    format!("127.0.0.1:{}", base_port + 1)
                        .parse::<SocketAddr>()
                        .unwrap(),
                ]
            };
            nodes.push(ProcessNode::new(
                i,
                base_port,
                binary_path.clone(),
                seed_nodes,
            ));
        }

        Self {
            nodes,
            binary_path,
            base_port,
        }
    }

    pub async fn start_all(&self) -> anyhow::Result<()> {
        self.nodes[0].start()?;
        self.nodes[0].wait_ready(Duration::from_secs(60)).await?;

        for i in 1..self.nodes.len() {
            self.nodes[i].start()?;
            self.nodes[i].wait_ready(Duration::from_secs(60)).await?;
            // Give raft voter promotion time to settle before starting next node
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        info!("all {} nodes started and ready", self.nodes.len());
        Ok(())
    }

    pub async fn wait_cluster_healthy(&self, timeout: Duration) -> anyhow::Result<()> {
        let start = Instant::now();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()?;
        let expected_nodes = self.nodes.len() as u32;

        while start.elapsed() < timeout {
            let url = format!("{}/api/v1/cluster/status", self.nodes[0].mgmt_url());
            if let Ok(resp) = client.get(&url).send().await {
                if let Ok(body) = resp.json::<serde_json::Value>().await {
                    if let Some(count) = body.get("node_count").and_then(|v| v.as_u64()) {
                        if count >= expected_nodes as u64 {
                            info!("cluster healthy with {} nodes", count);
                            return Ok(());
                        }
                        debug!("cluster has {} of {} nodes", count, expected_nodes);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        anyhow::bail!("cluster did not become healthy within {:?}", timeout)
    }

    pub fn node(&self, index: usize) -> &ProcessNode {
        &self.nodes[index]
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn kill_node(&self, index: usize) -> anyhow::Result<()> {
        self.nodes[index].kill()
    }

    pub fn stop_node(&self, index: usize) -> anyhow::Result<()> {
        self.nodes[index].stop()
    }

    pub fn resume_node(&self, index: usize) -> anyhow::Result<()> {
        self.nodes[index].resume()
    }

    pub async fn restart_node(&mut self, index: usize) -> anyhow::Result<()> {
        let old_data_dir = self.nodes[index].data_dir().to_path_buf();
        let seed_nodes = if index == 0 {
            vec![]
        } else {
            vec![
                format!("127.0.0.1:{}", self.base_port + 1)
                    .parse::<SocketAddr>()
                    .unwrap(),
            ]
        };

        let _ = self.nodes[index].kill();

        let new_node = ProcessNode::with_existing_data_dir(
            index,
            self.base_port,
            self.binary_path.clone(),
            seed_nodes,
            old_data_dir,
        );
        self.nodes[index] = new_node;
        self.nodes[index].start()?;
        self.nodes[index]
            .wait_ready(Duration::from_secs(15))
            .await?;
        Ok(())
    }

    pub async fn create_volume(
        &self,
        name: &str,
        size_bytes: u64,
        protection: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let client = reqwest::Client::new();
        let url = format!("{}/api/v1/volumes", self.nodes[0].mgmt_url());
        let body = serde_json::json!({
            "name": name,
            "size_bytes": size_bytes,
            "protection": protection,
        });

        let resp = client.post(&url).json(&body).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("create_volume failed: {} {}", status, text);
        }
        let vol: serde_json::Value = resp.json().await?;
        Ok(vol)
    }

    pub async fn cluster_status(&self) -> anyhow::Result<serde_json::Value> {
        let client = reqwest::Client::new();
        let url = format!("{}/api/v1/cluster/status", self.nodes[0].mgmt_url());
        let resp = client.get(&url).send().await?;
        let body: serde_json::Value = resp.json().await?;
        Ok(body)
    }

    pub async fn list_nodes_api(&self) -> anyhow::Result<serde_json::Value> {
        let client = reqwest::Client::new();
        let url = format!("{}/api/v1/nodes", self.nodes[0].mgmt_url());
        let resp = client.get(&url).send().await?;
        let body: serde_json::Value = resp.json().await?;
        Ok(body)
    }

    pub async fn acquire_lease(
        &self,
        volume_id: &str,
        session_id: &str,
    ) -> anyhow::Result<reqwest::Response> {
        let client = reqwest::Client::new();
        let url = format!("{}/api/v1/leases/acquire", self.nodes[0].mgmt_url());
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let body = serde_json::json!({
            "volume_id": volume_id,
            "session_id": session_id,
            "now_ms": now_ms,
            "ttl_ms": 30000,
        });
        let resp = client.post(&url).json(&body).send().await?;
        Ok(resp)
    }

    pub fn corrupt_extent_files(&self, node_index: usize) -> anyhow::Result<usize> {
        let disk_dir = self.nodes[node_index].data_dir().join("disk0");
        let mut corrupted = 0;
        if disk_dir.exists() {
            for entry in walkdir(&disk_dir)? {
                let path = entry;
                if path.extension().and_then(|e| e.to_str()) == Some("extent") {
                    if let Ok(mut data) = std::fs::read(&path) {
                        if !data.is_empty() {
                            data[0] ^= 0xFF;
                            if data.len() > 1 {
                                data[1] ^= 0xFF;
                            }
                            std::fs::write(&path, &data)?;
                            corrupted += 1;
                            info!("corrupted extent file: {}", path.display());
                        }
                    }
                }
            }
        }
        Ok(corrupted)
    }

    pub fn binary_path(&self) -> &Path {
        &self.binary_path
    }

    pub fn base_port(&self) -> u16 {
        self.base_port
    }

    /// Wipe raft state on a node while preserving data files (disk0/committed/).
    ///
    /// This simulates a scenario where raft metadata is lost but extent data
    /// remains on disk — forcing the node through the recovery code path.
    pub fn wipe_raft_state(&self, node_index: usize) -> anyhow::Result<()> {
        let data_dir = self.nodes[node_index].data_dir();
        let raft_db = data_dir.join("raft.db");
        let raft_sm_db = data_dir.join("raft-sm.db");
        for path in [&raft_db, &raft_sm_db] {
            if path.exists() {
                std::fs::remove_file(path)?;
                info!("wiped raft state: {}", path.display());
            }
        }
        // Also remove WAL/SHM files for SQLite-backed stores
        for suffix in &["-wal", "-shm"] {
            for base in &[&raft_db, &raft_sm_db] {
                let wal_path = PathBuf::from(format!("{}{}", base.display(), suffix));
                if wal_path.exists() {
                    std::fs::remove_file(&wal_path)?;
                }
            }
        }
        Ok(())
    }

    /// Pre-seed committed extent files on a node's disk to simulate stale data.
    ///
    /// Creates fake committed extent files that will conflict with deterministic
    /// placement when a volume writes to the same block offsets. This tests that
    /// the ExtentStore handles overwrites of pre-existing committed extents
    /// (the "immutability violation" bug).
    ///
    /// Returns the number of files created.
    pub fn pre_seed_extent_files(
        &self,
        node_index: usize,
        extent_ids: &[&str],
        version: u64,
        data: &[u8],
    ) -> anyhow::Result<usize> {
        let disk_dir = self.nodes[node_index].data_dir().join("disk0");
        let committed_dir = disk_dir.join("committed");
        let mut count = 0;
        for id_str in extent_ids {
            let prefix = &id_str[..8.min(id_str.len())];
            let dir = committed_dir.join(prefix);
            std::fs::create_dir_all(&dir)?;
            let file_path = dir.join(format!("{}_v{}", id_str, version));
            std::fs::write(&file_path, data)?;
            info!("seeded extent file: {}", file_path.display());

            // Also write a minimal .meta sidecar so recovery picks it up
            let meta_path = dir.join(format!("{}_v{}.meta", id_str, version));
            let meta = serde_json::json!({
                "extent_id": id_str,
                "disk_id": "00000000-0000-0000-0000-000000000000",
                "version": version,
                "checksum": "stale-seed-checksum",
                "size": data.len(),
                "storage_class": "Default",
                "committed_at": 0
            });
            std::fs::write(&meta_path, serde_json::to_string_pretty(&meta)?)?;
            count += 1;
        }
        Ok(count)
    }

    /// Count committed extent files across all disks on a node.
    pub fn count_committed_extents(&self, node_index: usize) -> anyhow::Result<usize> {
        let disk_dir = self.nodes[node_index].data_dir().join("disk0");
        let committed_dir = disk_dir.join("committed");
        if !committed_dir.exists() {
            return Ok(0);
        }
        let files = walkdir(&committed_dir)?;
        // Count only extent data files (not .meta sidecars)
        Ok(files.iter().filter(|p| {
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            name.contains("_v") && !name.ends_with(".meta")
        }).count())
    }

    /// Restart all nodes in the cluster. Kills each node, then starts them
    /// in sequence (node 0 first since it's the bootstrap/leader).
    ///
    /// Preserves data directories (extent files + raft state) across restarts,
    /// exercising the raft recovery code path.
    pub async fn restart_all_nodes(&mut self) -> anyhow::Result<()> {
        // Kill all nodes first (reverse order — followers before leader)
        for i in (0..self.nodes.len()).rev() {
            let _ = self.nodes[i].kill();
        }
        // Small delay to let ports free up
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Restart with preserved data dirs (node 0 first — it's the bootstrap node)
        for i in 0..self.nodes.len() {
            let old_data_dir = self.nodes[i].data_dir().to_path_buf();
            let seed_nodes = if i == 0 {
                vec![]
            } else {
                vec![
                    format!("127.0.0.1:{}", self.base_port + 1)
                        .parse::<SocketAddr>()
                        .unwrap(),
                ]
            };
            self.nodes[i] = ProcessNode::with_existing_data_dir(
                i,
                self.base_port,
                self.binary_path.clone(),
                seed_nodes,
                old_data_dir,
            );
            self.nodes[i].start()?;
            self.nodes[i]
                .wait_ready(Duration::from_secs(30))
                .await?;
            // Give raft time to settle between node starts
            if i < self.nodes.len() - 1 {
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
        Ok(())
    }
}

fn walkdir(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if dir.is_dir() {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                files.extend(walkdir(&path)?);
            } else {
                files.push(path);
            }
        }
    }
    Ok(files)
}

impl Drop for RealProcessCluster {
    fn drop(&mut self) {
        for node in &self.nodes {
            if node.state() == ProcessNodeState::Running || node.state() == ProcessNodeState::Paused
            {
                let _ = node.kill();
            }
        }
    }
}

pub async fn build_binary() -> anyhow::Result<PathBuf> {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");

    let binary_path = workspace_root.join("target/release/blockyard");

    if binary_path.exists() {
        info!("using existing binary: {}", binary_path.display());
        return Ok(binary_path);
    }

    info!("building blockyard binary (release)...");
    let status = Command::new("cargo")
        .arg("build")
        .arg("--release")
        .arg("-p")
        .arg("blockyard")
        .arg("--bin")
        .arg("blockyard")
        .current_dir(workspace_root)
        .status()?;

    if !status.success() {
        anyhow::bail!("cargo build --release failed");
    }

    Ok(binary_path)
}

pub fn unique_base_port() -> u16 {
    use std::io::{Read, Seek, Write};
    // Cross-process port allocation using a shared lock file to prevent
    // collisions between concurrent test binaries.
    let lock_path = std::env::temp_dir().join("blockyard-test-port.lock");
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .expect("open port lock file");

    // Acquire exclusive file lock (blocks until available)
    use std::os::unix::io::AsRawFd;
    let fd = file.as_raw_fd();
    unsafe {
        nix::libc::flock(fd, nix::libc::LOCK_EX);
    }

    // Read current port from file (or start fresh)
    let mut contents = String::new();
    let _ = file.read_to_string(&mut contents);
    let mut port: u16 = contents.trim().parse().unwrap_or(20000);
    if !(20000..=60000).contains(&port) {
        port = 20000;
    }
    let result = port;

    // Write the next port value
    let next = port + 500;
    file.seek(std::io::SeekFrom::Start(0)).unwrap();
    file.set_len(0).unwrap();
    write!(file, "{}", next).unwrap();

    // Release lock (automatically on drop, but be explicit)
    unsafe {
        nix::libc::flock(fd, nix::libc::LOCK_UN);
    }

    result
}

pub struct TcpDataClient {
    stream: tokio::net::TcpStream,
}

impl TcpDataClient {
    pub async fn connect(addr: SocketAddr) -> anyhow::Result<Self> {
        let stream = tokio::net::TcpStream::connect(addr).await?;
        let mut client = Self { stream };
        client.handshake().await?;
        Ok(client)
    }

    async fn handshake(&mut self) -> anyhow::Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let req = serde_json::json!({
            "protocol_version": 1,
            "node_id": null,
            "session_id": null,
            "features": [],
            "auth_token": null,
        });
        let req_bytes = serde_json::to_vec(&req)?;
        self.stream.write_u32(req_bytes.len() as u32).await?;
        self.stream.write_all(&req_bytes).await?;
        self.stream.flush().await?;

        let len = self.stream.read_u32().await?;
        let mut buf = vec![0u8; len as usize];
        self.stream.read_exact(&mut buf).await?;
        let resp: serde_json::Value = serde_json::from_slice(&buf)?;
        if resp.get("accepted").and_then(|v| v.as_bool()) != Some(true) {
            anyhow::bail!("handshake rejected: {:?}", resp);
        }
        Ok(())
    }

    pub async fn write_extent(
        &mut self,
        volume_id: &str,
        extent_id: &str,
        version: u64,
        data: &[u8],
    ) -> anyhow::Result<serde_json::Value> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let checksum = blockyard_common::checksum::compute_checksum(data);
        let op_id = uuid::Uuid::new_v4().to_string();
        let session_id = uuid::Uuid::new_v4().to_string();

        let req = serde_json::json!({
            "WriteReq": {
                "operation_id": op_id,
                "session_id": session_id,
                "volume_id": volume_id,
                "extent_id": extent_id,
                "extent_version": version,
                "epoch": 1,
                "target_disk_id": null,
                "checksum": checksum,
                "payload_size": data.len(),
                "lease_version": null,
            }
        });
        let req_bytes = serde_json::to_vec(&req)?;
        self.stream.write_u32(req_bytes.len() as u32).await?;
        self.stream.write_all(&req_bytes).await?;
        self.stream.write_all(data).await?;
        self.stream.flush().await?;

        let len = self.stream.read_u32().await?;
        let mut buf = vec![0u8; len as usize];
        self.stream.read_exact(&mut buf).await?;
        let resp: serde_json::Value = serde_json::from_slice(&buf)?;
        Ok(resp)
    }

    pub async fn read_extent(
        &mut self,
        volume_id: &str,
        extent_id: &str,
        version: u64,
    ) -> anyhow::Result<(serde_json::Value, Vec<u8>)> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let op_id = uuid::Uuid::new_v4().to_string();
        let session_id = uuid::Uuid::new_v4().to_string();

        let req = serde_json::json!({
            "ReadReq": {
                "operation_id": op_id,
                "session_id": session_id,
                "volume_id": volume_id,
                "extent_id": extent_id,
                "extent_version": version,
                "epoch": 1,
                "offset": 0,
                "length": 0,
            }
        });
        let req_bytes = serde_json::to_vec(&req)?;
        self.stream.write_u32(req_bytes.len() as u32).await?;
        self.stream.write_all(&req_bytes).await?;
        self.stream.flush().await?;

        let len = self.stream.read_u32().await?;
        let mut buf = vec![0u8; len as usize];
        self.stream.read_exact(&mut buf).await?;
        let resp: serde_json::Value = serde_json::from_slice(&buf)?;

        let payload_size = resp
            .get("ReadResp")
            .and_then(|r| r.get("payload_size"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        let mut payload = vec![0u8; payload_size as usize];
        if payload_size > 0 {
            self.stream.read_exact(&mut payload).await?;
        }

        Ok((resp, payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_node_addresses() {
        let node = ProcessNode::new(0, 19800, PathBuf::from("/bin/false"), vec![]);
        assert_eq!(node.data_addr().port(), 19800);
        assert_eq!(node.gossip_addr().port(), 19801);
        assert_eq!(node.mgmt_addr().port(), 19801); // same as gossip (UDP vs TCP)
        assert_eq!(node.raft_addr().port(), 19810);
        assert_eq!(node.state(), ProcessNodeState::Stopped);
        assert!(node.pid().is_none());
    }

    #[test]
    fn test_process_node_addresses_second_node() {
        let node = ProcessNode::new(1, 19800, PathBuf::from("/bin/false"), vec![]);
        assert_eq!(node.data_addr().port(), 19900);
        assert_eq!(node.gossip_addr().port(), 19901);
        assert_eq!(node.mgmt_addr().port(), 19901); // same as gossip
        assert_eq!(node.raft_addr().port(), 19910);
    }

    #[test]
    fn test_process_node_config_generation() {
        let node = ProcessNode::new(
            0,
            19800,
            PathBuf::from("/bin/false"),
            vec!["127.0.0.1:19801".parse().unwrap()],
        );
        let config = node.generate_config();
        assert!(config.contains("listen_addr = \"127.0.0.1:19800\""));
        assert!(config.contains("mgmt_addr = \"127.0.0.1:19801\""));
        assert!(config.contains("bind_addr = \"127.0.0.1:19810\""));
        assert!(config.contains("\"127.0.0.1:19801\""));
    }

    #[test]
    fn test_process_node_bootstrap_config() {
        let node = ProcessNode::new(0, 19800, PathBuf::from("/bin/false"), vec![]);
        let config = node.generate_config();
        assert!(config.contains("seed_nodes = []"));
    }

    #[test]
    fn test_unique_base_port_different() {
        let p1 = unique_base_port();
        let p2 = unique_base_port();
        assert_ne!(p1, p2, "ports should be different");
        assert!(p1 >= 20000, "port should be >= 20000, got {}", p1);
        assert!(p2 >= 20000, "port should be >= 20000, got {}", p2);
    }

    #[test]
    fn test_real_process_cluster_creation() {
        let cluster = RealProcessCluster::new(3, PathBuf::from("/bin/false"), 30000);
        assert_eq!(cluster.node_count(), 3);
        assert_eq!(cluster.node(0).data_addr().port(), 30000);
        assert_eq!(cluster.node(1).data_addr().port(), 30100);
        assert_eq!(cluster.node(2).data_addr().port(), 30200);
    }

    #[test]
    fn test_walkdir_empty() {
        let dir = TempDir::new().unwrap();
        let files = walkdir(dir.path()).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn test_walkdir_with_files() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("a.extent"), b"data").unwrap();
        std::fs::create_dir_all(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub/b.extent"), b"data2").unwrap();
        let files = walkdir(dir.path()).unwrap();
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn test_process_node_with_existing_data_dir() {
        let dir = TempDir::new().unwrap();
        let node = ProcessNode::with_existing_data_dir(
            0,
            19800,
            PathBuf::from("/bin/false"),
            vec![],
            dir.path().to_path_buf(),
        );
        assert_eq!(node.data_dir(), dir.path());
    }
}
