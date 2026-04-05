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
        Self::from_toml(&content)
    }

    pub fn from_toml(content: &str) -> Result<Self> {
        toml::from_str(content).map_err(|e| Error::Config(format!("failed to parse config: {e}")))
    }
}

fn default_listen() -> SocketAddr {
    // SAFETY: this is a valid socket address literal
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

pub(crate) mod humantime_serde_compat {
    use serde::{self, Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let ms = duration.as_millis();
        if ms >= 1000 && ms % 1000 == 0 {
            serializer.serialize_str(&format!("{}s", ms / 1000))
        } else {
            serializer.serialize_str(&format!("{ms}ms"))
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_duration(&s).map_err(serde::de::Error::custom)
    }

    pub fn parse_duration(s: &str) -> Result<Duration, String> {
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

#[cfg(test)]
mod tests {
    use super::humantime_serde_compat::parse_duration;
    use super::*;

    const MINIMAL_CONFIG: &str = r#"
[node]
listen = "0.0.0.0:7400"

[cluster]
seeds = ["10.0.1.1:7400"]

[storage]
zfs_pool = "blockyard"
"#;

    const FULL_CONFIG: &str = r#"
[node]
name = "node-a"
listen = "0.0.0.0:7400"
data_listen = "0.0.0.0:7401"
metrics_listen = "0.0.0.0:7402"

[cluster]
seeds = ["10.0.1.1:7400", "10.0.1.2:7400"]

[storage]
zfs_pool = "blockyard"
extent_size = "4MB"

[raft]
heartbeat_interval = "200ms"
election_timeout_min = "1000ms"
election_timeout_max = "2000ms"
snapshot_interval = 10000

[gossip]
probe_interval = "500ms"
suspect_timeout = "3s"
probe_timeout = "200ms"

[rebalance]
enabled = true
threshold = 0.20
max_concurrent = 1
throttle_bandwidth = "1Gbps"

[tags]
storage_class = "ssd"
rack = "rack-1"
"#;

    #[test]
    fn test_from_str_minimal() {
        let cfg = NodeConfig::from_toml(MINIMAL_CONFIG).unwrap();
        assert_eq!(cfg.storage.zfs_pool, "blockyard");
        assert_eq!(cfg.cluster.seeds.len(), 1);
        assert!(cfg.node.name.is_none());
    }

    #[test]
    fn test_from_str_full() {
        let cfg = NodeConfig::from_toml(FULL_CONFIG).unwrap();
        assert_eq!(cfg.node.name.as_deref(), Some("node-a"));
        assert_eq!(cfg.cluster.seeds.len(), 2);
        assert_eq!(cfg.storage.extent_size, "4MB");
        assert_eq!(cfg.raft.heartbeat_interval, Duration::from_millis(200));
        assert_eq!(cfg.raft.election_timeout_min, Duration::from_millis(1000));
        assert_eq!(cfg.raft.snapshot_interval, 10_000);
        assert_eq!(cfg.gossip.probe_interval, Duration::from_millis(500));
        assert_eq!(cfg.gossip.suspect_timeout, Duration::from_secs(3));
        assert!(cfg.rebalance.enabled);
        assert!((cfg.rebalance.threshold - 0.20).abs() < f64::EPSILON);
        assert_eq!(cfg.tags.get("storage_class").unwrap(), "ssd");
        assert_eq!(cfg.tags.get("rack").unwrap(), "rack-1");
    }

    #[test]
    fn test_from_str_defaults() {
        let cfg = NodeConfig::from_toml(MINIMAL_CONFIG).unwrap();
        assert_eq!(cfg.raft.heartbeat_interval, Duration::from_millis(200));
        assert_eq!(cfg.raft.election_timeout_min, Duration::from_millis(1000));
        assert_eq!(cfg.raft.election_timeout_max, Duration::from_millis(2000));
        assert_eq!(cfg.raft.snapshot_interval, 10_000);
        assert_eq!(cfg.gossip.probe_interval, Duration::from_millis(500));
        assert_eq!(cfg.gossip.suspect_timeout, Duration::from_secs(3));
        assert_eq!(cfg.gossip.probe_timeout, Duration::from_millis(200));
        assert!(cfg.rebalance.enabled);
        assert_eq!(cfg.rebalance.max_concurrent, 1);
        assert_eq!(cfg.storage.extent_size, "4MB");
        assert!(cfg.tls.is_none());
    }

    #[test]
    fn test_from_str_invalid() {
        assert!(NodeConfig::from_toml("this is not toml [[[").is_err());
    }

    #[test]
    fn test_from_str_missing_required() {
        let r = NodeConfig::from_toml("[node]\n[cluster]\nseeds = []\n");
        assert!(r.is_err());
    }

    #[test]
    fn test_from_file_not_found() {
        let r = NodeConfig::from_file(std::path::Path::new("/nonexistent/config.toml"));
        assert!(r.is_err());
        let err = r.unwrap_err();
        assert!(err.to_string().contains("configuration error"));
    }

    #[test]
    fn test_raft_section_default() {
        let r = RaftSection::default();
        assert_eq!(r.heartbeat_interval, Duration::from_millis(200));
        assert_eq!(r.snapshot_interval, 10_000);
    }

    #[test]
    fn test_gossip_section_default() {
        let g = GossipSection::default();
        assert_eq!(g.probe_interval, Duration::from_millis(500));
        assert_eq!(g.suspect_timeout, Duration::from_secs(3));
    }

    #[test]
    fn test_rebalance_section_default() {
        let r = RebalanceSection::default();
        assert!(r.enabled);
        assert!((r.threshold - 0.20).abs() < f64::EPSILON);
        assert_eq!(r.max_concurrent, 1);
    }

    #[test]
    fn test_parse_duration_ms() {
        assert_eq!(parse_duration("200ms").unwrap(), Duration::from_millis(200));
        assert_eq!(parse_duration("0ms").unwrap(), Duration::from_millis(0));
    }

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration("3s").unwrap(), Duration::from_secs(3));
        assert_eq!(parse_duration("0s").unwrap(), Duration::from_secs(0));
    }

    #[test]
    fn test_parse_duration_whitespace() {
        assert_eq!(
            parse_duration("  200ms  ").unwrap(),
            Duration::from_millis(200)
        );
    }

    #[test]
    fn test_parse_duration_invalid() {
        assert!(parse_duration("200").is_err());
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("ms").is_err());
    }

    #[test]
    fn test_config_with_tls() {
        let cfg_str = r#"
[node]
listen = "0.0.0.0:7400"

[cluster]
seeds = ["10.0.1.1:7400"]

[storage]
zfs_pool = "blockyard"

[tls]
ca_cert = "/etc/blockyard/ca.pem"
cert = "/etc/blockyard/node.pem"
key = "/etc/blockyard/node-key.pem"
"#;
        let cfg = NodeConfig::from_toml(cfg_str).unwrap();
        let tls = cfg.tls.unwrap();
        assert_eq!(tls.ca_cert, PathBuf::from("/etc/blockyard/ca.pem"));
        assert_eq!(tls.cert, PathBuf::from("/etc/blockyard/node.pem"));
        assert_eq!(tls.key, PathBuf::from("/etc/blockyard/node-key.pem"));
    }

    #[test]
    fn test_config_roundtrip() {
        let cfg = NodeConfig::from_toml(FULL_CONFIG).unwrap();
        let toml_str = toml::to_string(&cfg).unwrap();
        let cfg2 = NodeConfig::from_toml(&toml_str).unwrap();
        assert_eq!(cfg2.storage.zfs_pool, cfg.storage.zfs_pool);
        assert_eq!(cfg2.raft.heartbeat_interval, cfg.raft.heartbeat_interval);
    }
}
