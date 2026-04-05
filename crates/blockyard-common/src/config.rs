use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::error::{Error, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub node: NodeSection,
    pub cluster: ClusterSection,
    pub storage: StorageSection,
    #[serde(default)]
    pub raft: RaftSection,
    #[serde(default)]
    pub gossip: GossipSection,
    #[serde(default)]
    pub rebalance: RebalanceSection,
    pub tls: Option<TlsSection>,
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSection {
    pub name: Option<String>,
    #[serde(default = "default_listen")]
    pub listen: SocketAddr,
    #[serde(default = "default_data_listen")]
    pub data_listen: SocketAddr,
    #[serde(default = "default_metrics_listen")]
    pub metrics_listen: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSection {
    pub seeds: Vec<SocketAddr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSection {
    pub zfs_pool: String,
    #[serde(default = "default_extent_size")]
    pub extent_size: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftSection {
    #[serde(
        default = "default_heartbeat_interval",
        with = "humantime_serde_compat"
    )]
    pub heartbeat_interval: Duration,
    #[serde(
        default = "default_election_timeout_min",
        with = "humantime_serde_compat"
    )]
    pub election_timeout_min: Duration,
    #[serde(
        default = "default_election_timeout_max",
        with = "humantime_serde_compat"
    )]
    pub election_timeout_max: Duration,
    #[serde(default = "default_snapshot_interval")]
    pub snapshot_interval: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipSection {
    #[serde(default = "default_probe_interval", with = "humantime_serde_compat")]
    pub probe_interval: Duration,
    #[serde(default = "default_suspect_timeout", with = "humantime_serde_compat")]
    pub suspect_timeout: Duration,
    #[serde(default = "default_probe_timeout", with = "humantime_serde_compat")]
    pub probe_timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalanceSection {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_threshold")]
    pub threshold: f64,
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: u32,
    #[serde(default = "default_throttle_bandwidth")]
    pub throttle_bandwidth: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsSection {
    pub ca_cert: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
}

impl NodeConfig {
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            Error::Config(format!(
                "failed to read config file {}: {}",
                path.display(),
                e
            ))
        })?;
        toml::from_str(&content).map_err(|e| Error::Config(format!("failed to parse config: {e}")))
    }
}

fn default_listen() -> SocketAddr {
    "0.0.0.0:7400".parse().unwrap()
}
fn default_data_listen() -> SocketAddr {
    "0.0.0.0:7401".parse().unwrap()
}
fn default_metrics_listen() -> SocketAddr {
    "0.0.0.0:7402".parse().unwrap()
}
fn default_extent_size() -> String {
    "4MB".into()
}
fn default_heartbeat_interval() -> Duration {
    Duration::from_millis(200)
}
fn default_election_timeout_min() -> Duration {
    Duration::from_millis(1000)
}
fn default_election_timeout_max() -> Duration {
    Duration::from_millis(2000)
}
fn default_snapshot_interval() -> u64 {
    10_000
}
fn default_probe_interval() -> Duration {
    Duration::from_millis(500)
}
fn default_suspect_timeout() -> Duration {
    Duration::from_secs(3)
}
fn default_probe_timeout() -> Duration {
    Duration::from_millis(200)
}
fn default_true() -> bool {
    true
}
fn default_threshold() -> f64 {
    0.20
}
fn default_max_concurrent() -> u32 {
    1
}
fn default_throttle_bandwidth() -> String {
    "1Gbps".into()
}

impl Default for RaftSection {
    fn default() -> Self {
        Self {
            heartbeat_interval: default_heartbeat_interval(),
            election_timeout_min: default_election_timeout_min(),
            election_timeout_max: default_election_timeout_max(),
            snapshot_interval: default_snapshot_interval(),
        }
    }
}

impl Default for GossipSection {
    fn default() -> Self {
        Self {
            probe_interval: default_probe_interval(),
            suspect_timeout: default_suspect_timeout(),
            probe_timeout: default_probe_timeout(),
        }
    }
}

impl Default for RebalanceSection {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            threshold: default_threshold(),
            max_concurrent: default_max_concurrent(),
            throttle_bandwidth: default_throttle_bandwidth(),
        }
    }
}

mod humantime_serde_compat {
    use serde::{self, Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}ms", duration.as_millis());
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_duration(&s).map_err(serde::de::Error::custom)
    }

    fn parse_duration(s: &str) -> Result<Duration, String> {
        let s = s.trim();
        if let Some(ms) = s.strip_suffix("ms") {
            ms.trim()
                .parse::<u64>()
                .map(Duration::from_millis)
                .map_err(|e| e.to_string())
        } else if let Some(secs) = s.strip_suffix('s') {
            secs.trim()
                .parse::<u64>()
                .map(Duration::from_secs)
                .map_err(|e| e.to_string())
        } else {
            Err(format!("unsupported duration format: {s}"))
        }
    }
}
