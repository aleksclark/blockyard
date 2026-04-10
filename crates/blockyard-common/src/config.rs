//! Configuration structs with TOML deserialization and validation.
//!
//! [`NodeConfig`] is the root configuration for a Blockyard node, composed of
//! typed sections for each subsystem.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::Error;

/// Root configuration for a Blockyard node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Human-readable node name (optional).
    #[serde(default)]
    pub name: Option<String>,

    /// Bind address for the data plane.
    pub listen_addr: SocketAddr,

    /// Data directory for metadata and state.
    pub data_dir: PathBuf,

    pub storage: StorageSection,
    pub raft: RaftSection,
    pub gossip: GossipSection,
    pub protocol: ProtocolSection,

    #[serde(default)]
    pub tls: Option<TlsSection>,

    #[serde(default)]
    pub auth: Option<AuthSection>,
}

/// Local disk storage configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSection {
    /// Paths to XFS-formatted disk mount points dedicated to Blockyard.
    pub disk_paths: Vec<PathBuf>,

    /// Maximum concurrent background IO operations per disk.
    #[serde(default = "default_max_background_io")]
    pub max_background_io: u32,

    /// Scrub interval in seconds.
    #[serde(default = "default_scrub_interval_secs")]
    pub scrub_interval_secs: u64,
}

/// Raft consensus configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftSection {
    /// Election timeout range lower bound in milliseconds.
    #[serde(default = "default_election_timeout_min_ms")]
    pub election_timeout_min_ms: u64,

    /// Election timeout range upper bound in milliseconds.
    #[serde(default = "default_election_timeout_max_ms")]
    pub election_timeout_max_ms: u64,

    /// Heartbeat interval in milliseconds.
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,

    /// Maximum entries per append-entries batch.
    #[serde(default = "default_max_entries_per_batch")]
    pub max_entries_per_batch: u64,

    /// Snapshot threshold (number of log entries before compaction).
    #[serde(default = "default_snapshot_threshold")]
    pub snapshot_threshold: u64,

    /// Bind address for the Raft RPC TCP transport.
    /// Defaults to `None`, which means derive from `listen_addr` with port + 10.
    #[serde(default)]
    pub bind_addr: Option<SocketAddr>,
}

/// SWIM gossip protocol configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipSection {
    /// Bind address for gossip protocol.
    pub bind_addr: SocketAddr,

    /// Seed nodes for initial cluster join.
    #[serde(default)]
    pub seed_nodes: Vec<SocketAddr>,

    /// Gossip protocol period in milliseconds.
    #[serde(default = "default_gossip_interval_ms")]
    pub gossip_interval_ms: u64,

    /// Suspicion timeout multiplier (number of protocol periods).
    #[serde(default = "default_suspicion_mult")]
    pub suspicion_mult: u32,
}

/// Wire protocol configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolSection {
    /// Maximum message size in bytes.
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,

    /// Connection timeout in milliseconds.
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,

    /// Request timeout in milliseconds.
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,

    /// Bind address for the management (REST) API.
    #[serde(default = "default_mgmt_addr")]
    pub mgmt_addr: SocketAddr,
}

/// TLS configuration for node-to-node and client-to-node communication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsSection {
    /// Path to the TLS certificate file (PEM).
    pub cert_path: PathBuf,

    /// Path to the TLS private key file (PEM).
    pub key_path: PathBuf,

    /// Path to the CA certificate file for peer verification (PEM).
    pub ca_path: PathBuf,

    /// Whether to require client certificates.
    #[serde(default)]
    pub require_client_cert: bool,
}

/// Authentication configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthSection {
    /// Shared secret or token for cluster authentication.
    pub cluster_secret: String,
}

fn default_max_background_io() -> u32 {
    4
}
fn default_scrub_interval_secs() -> u64 {
    86400
}
fn default_election_timeout_min_ms() -> u64 {
    150
}
fn default_election_timeout_max_ms() -> u64 {
    300
}
fn default_heartbeat_interval_ms() -> u64 {
    50
}
fn default_max_entries_per_batch() -> u64 {
    64
}
fn default_snapshot_threshold() -> u64 {
    10000
}
fn default_gossip_interval_ms() -> u64 {
    1000
}
fn default_suspicion_mult() -> u32 {
    4
}
fn default_max_message_size() -> usize {
    64 * 1024 * 1024
}
fn default_connect_timeout_ms() -> u64 {
    5000
}
fn default_request_timeout_ms() -> u64 {
    30000
}
fn default_mgmt_addr() -> SocketAddr {
    "127.0.0.1:9801".parse().expect("valid default mgmt_addr")
}

impl NodeConfig {
    /// Parse a [`NodeConfig`] from a TOML string.
    pub fn from_toml(s: &str) -> Result<Self, Error> {
        let config: NodeConfig =
            toml::from_str(s).map_err(|e| Error::Config(format!("TOML parse error: {e}")))?;
        config.validate()?;
        Ok(config)
    }

    /// Parse a [`NodeConfig`] from a TOML file.
    pub fn from_file(path: &Path) -> Result<Self, Error> {
        let contents = std::fs::read_to_string(path)?;
        Self::from_toml(&contents)
    }

    /// Validate configuration values, returning helpful error messages.
    pub fn validate(&self) -> Result<(), Error> {
        if self.storage.disk_paths.is_empty() {
            return Err(Error::Config(
                "storage.disk_paths must contain at least one path".into(),
            ));
        }

        if self.raft.election_timeout_min_ms >= self.raft.election_timeout_max_ms {
            return Err(Error::Config(
                "raft.election_timeout_min_ms must be less than election_timeout_max_ms".into(),
            ));
        }

        if self.raft.heartbeat_interval_ms >= self.raft.election_timeout_min_ms {
            return Err(Error::Config(
                "raft.heartbeat_interval_ms must be less than election_timeout_min_ms".into(),
            ));
        }

        if self.protocol.max_message_size == 0 {
            return Err(Error::Config(
                "protocol.max_message_size must be greater than 0".into(),
            ));
        }

        if let Some(ref tls) = self.tls {
            if tls.cert_path.as_os_str().is_empty() {
                return Err(Error::Config("tls.cert_path must not be empty".into()));
            }
            if tls.key_path.as_os_str().is_empty() {
                return Err(Error::Config("tls.key_path must not be empty".into()));
            }
            if tls.ca_path.as_os_str().is_empty() {
                return Err(Error::Config("tls.ca_path must not be empty".into()));
            }
        }

        Ok(())
    }

    /// Generate an example TOML configuration string.
    pub fn example_toml() -> String {
        r#"# Blockyard Node Configuration

name = "node-1"
listen_addr = "0.0.0.0:9800"
data_dir = "/var/lib/blockyard"

[storage]
disk_paths = ["/mnt/disk0", "/mnt/disk1"]
max_background_io = 4
scrub_interval_secs = 86400

[raft]
election_timeout_min_ms = 150
election_timeout_max_ms = 300
heartbeat_interval_ms = 50
max_entries_per_batch = 64
snapshot_threshold = 10000

[gossip]
bind_addr = "0.0.0.0:9801"
seed_nodes = ["10.0.0.2:9801", "10.0.0.3:9801"]
gossip_interval_ms = 1000
suspicion_mult = 4

[protocol]
max_message_size = 67108864
connect_timeout_ms = 5000
request_timeout_ms = 30000
mgmt_addr = "127.0.0.1:9801"

# [tls]
# cert_path = "/etc/blockyard/tls/node.crt"
# key_path = "/etc/blockyard/tls/node.key"
# ca_path = "/etc/blockyard/tls/ca.crt"
# require_client_cert = false

# [auth]
# cluster_secret = "change-me"
"#
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_toml() -> String {
        r#"
listen_addr = "127.0.0.1:9800"
data_dir = "/tmp/blockyard"

[storage]
disk_paths = ["/mnt/disk0"]

[raft]
election_timeout_min_ms = 150
election_timeout_max_ms = 300
heartbeat_interval_ms = 50

[gossip]
bind_addr = "127.0.0.1:9801"

[protocol]
"#
        .to_string()
    }

    #[test]
    fn test_parse_minimal_config() {
        let config = NodeConfig::from_toml(&minimal_toml()).unwrap();
        assert_eq!(
            config.listen_addr,
            "127.0.0.1:9800".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(config.storage.disk_paths.len(), 1);
    }

    #[test]
    fn test_parse_with_name() {
        let toml = format!("name = \"test-node\"\n{}", minimal_toml());
        let config = NodeConfig::from_toml(&toml).unwrap();
        assert_eq!(config.name.as_deref(), Some("test-node"));
    }

    #[test]
    fn test_parse_defaults() {
        let config = NodeConfig::from_toml(&minimal_toml()).unwrap();
        assert_eq!(config.storage.max_background_io, 4);
        assert_eq!(config.storage.scrub_interval_secs, 86400);
        assert_eq!(config.raft.max_entries_per_batch, 64);
        assert_eq!(config.raft.snapshot_threshold, 10000);
        assert_eq!(config.gossip.gossip_interval_ms, 1000);
        assert_eq!(config.gossip.suspicion_mult, 4);
        assert_eq!(config.protocol.max_message_size, 64 * 1024 * 1024);
        assert_eq!(config.protocol.connect_timeout_ms, 5000);
        assert_eq!(config.protocol.request_timeout_ms, 30000);
    }

    #[test]
    fn test_validate_empty_disk_paths() {
        let toml = r#"
listen_addr = "127.0.0.1:9800"
data_dir = "/tmp/blockyard"

[storage]
disk_paths = []

[raft]
election_timeout_min_ms = 150
election_timeout_max_ms = 300
heartbeat_interval_ms = 50

[gossip]
bind_addr = "127.0.0.1:9801"

[protocol]
"#;
        let err = NodeConfig::from_toml(toml).unwrap_err();
        assert!(err.to_string().contains("disk_paths"));
    }

    #[test]
    fn test_validate_election_timeout_order() {
        let toml = r#"
listen_addr = "127.0.0.1:9800"
data_dir = "/tmp/blockyard"

[storage]
disk_paths = ["/mnt/disk0"]

[raft]
election_timeout_min_ms = 300
election_timeout_max_ms = 150
heartbeat_interval_ms = 50

[gossip]
bind_addr = "127.0.0.1:9801"

[protocol]
"#;
        let err = NodeConfig::from_toml(toml).unwrap_err();
        assert!(err.to_string().contains("election_timeout"));
    }

    #[test]
    fn test_validate_heartbeat_vs_election() {
        let toml = r#"
listen_addr = "127.0.0.1:9800"
data_dir = "/tmp/blockyard"

[storage]
disk_paths = ["/mnt/disk0"]

[raft]
election_timeout_min_ms = 150
election_timeout_max_ms = 300
heartbeat_interval_ms = 200

[gossip]
bind_addr = "127.0.0.1:9801"

[protocol]
"#;
        let err = NodeConfig::from_toml(toml).unwrap_err();
        assert!(err.to_string().contains("heartbeat"));
    }

    #[test]
    fn test_validate_zero_message_size() {
        let toml = r#"
listen_addr = "127.0.0.1:9800"
data_dir = "/tmp/blockyard"

[storage]
disk_paths = ["/mnt/disk0"]

[raft]
election_timeout_min_ms = 150
election_timeout_max_ms = 300
heartbeat_interval_ms = 50

[gossip]
bind_addr = "127.0.0.1:9801"

[protocol]
max_message_size = 0
"#;
        let err = NodeConfig::from_toml(toml).unwrap_err();
        assert!(err.to_string().contains("max_message_size"));
    }

    #[test]
    fn test_parse_with_tls() {
        let toml = format!(
            r#"{}
[tls]
cert_path = "/etc/certs/node.crt"
key_path = "/etc/certs/node.key"
ca_path = "/etc/certs/ca.crt"
require_client_cert = true
"#,
            minimal_toml()
        );
        let config = NodeConfig::from_toml(&toml).unwrap();
        let tls = config.tls.unwrap();
        assert!(tls.require_client_cert);
        assert_eq!(tls.cert_path, PathBuf::from("/etc/certs/node.crt"));
    }

    #[test]
    fn test_validate_tls_empty_cert() {
        let toml = format!(
            r#"{}
[tls]
cert_path = ""
key_path = "/etc/certs/node.key"
ca_path = "/etc/certs/ca.crt"
"#,
            minimal_toml()
        );
        let err = NodeConfig::from_toml(&toml).unwrap_err();
        assert!(err.to_string().contains("cert_path"));
    }

    #[test]
    fn test_validate_tls_empty_key() {
        let toml = format!(
            r#"{}
[tls]
cert_path = "/etc/certs/node.crt"
key_path = ""
ca_path = "/etc/certs/ca.crt"
"#,
            minimal_toml()
        );
        let err = NodeConfig::from_toml(&toml).unwrap_err();
        assert!(err.to_string().contains("key_path"));
    }

    #[test]
    fn test_validate_tls_empty_ca() {
        let toml = format!(
            r#"{}
[tls]
cert_path = "/etc/certs/node.crt"
key_path = "/etc/certs/node.key"
ca_path = ""
"#,
            minimal_toml()
        );
        let err = NodeConfig::from_toml(&toml).unwrap_err();
        assert!(err.to_string().contains("ca_path"));
    }

    #[test]
    fn test_parse_with_auth() {
        let toml = format!(
            r#"{}
[auth]
cluster_secret = "my-secret"
"#,
            minimal_toml()
        );
        let config = NodeConfig::from_toml(&toml).unwrap();
        let auth = config.auth.unwrap();
        assert_eq!(auth.cluster_secret, "my-secret");
    }

    #[test]
    fn test_parse_with_seed_nodes() {
        let toml = r#"
listen_addr = "127.0.0.1:9800"
data_dir = "/tmp/blockyard"

[storage]
disk_paths = ["/mnt/disk0"]

[raft]
election_timeout_min_ms = 150
election_timeout_max_ms = 300
heartbeat_interval_ms = 50

[gossip]
bind_addr = "127.0.0.1:9801"
seed_nodes = ["10.0.0.2:9801", "10.0.0.3:9801"]

[protocol]
"#;
        let config = NodeConfig::from_toml(toml).unwrap();
        assert_eq!(config.gossip.seed_nodes.len(), 2);
    }

    #[test]
    fn test_invalid_toml_syntax() {
        let err = NodeConfig::from_toml("not valid toml [[[").unwrap_err();
        assert!(err.to_string().contains("TOML parse error"));
    }

    #[test]
    fn test_example_toml_parses() {
        let example = NodeConfig::example_toml();
        let config = NodeConfig::from_toml(&example).unwrap();
        assert_eq!(config.name.as_deref(), Some("node-1"));
        assert_eq!(config.storage.disk_paths.len(), 2);
    }

    #[test]
    fn test_from_file_missing() {
        let result = NodeConfig::from_file(Path::new("/nonexistent/config.toml"));
        assert!(result.is_err());
    }

    #[test]
    fn test_from_file_valid() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, minimal_toml()).unwrap();
        let config = NodeConfig::from_file(&path).unwrap();
        assert_eq!(config.storage.disk_paths.len(), 1);
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = NodeConfig::from_toml(&minimal_toml()).unwrap();
        let serialized = toml::to_string(&config).unwrap();
        let reparsed: NodeConfig = toml::from_str(&serialized).unwrap();
        assert_eq!(config.listen_addr, reparsed.listen_addr);
    }

    #[test]
    fn test_config_debug() {
        let config = NodeConfig::from_toml(&minimal_toml()).unwrap();
        let debug = format!("{:?}", config);
        assert!(debug.contains("NodeConfig"));
    }

    #[test]
    fn test_storage_section_debug() {
        let config = NodeConfig::from_toml(&minimal_toml()).unwrap();
        let debug = format!("{:?}", config.storage);
        assert!(debug.contains("StorageSection"));
    }

    #[test]
    fn test_raft_section_debug() {
        let config = NodeConfig::from_toml(&minimal_toml()).unwrap();
        let debug = format!("{:?}", config.raft);
        assert!(debug.contains("RaftSection"));
    }

    #[test]
    fn test_raft_bind_addr_default_is_none() {
        let config = NodeConfig::from_toml(&minimal_toml()).unwrap();
        assert!(config.raft.bind_addr.is_none());
    }

    #[test]
    fn test_raft_bind_addr_explicit() {
        let toml = r#"
listen_addr = "127.0.0.1:9800"
data_dir = "/tmp/blockyard"

[storage]
disk_paths = ["/mnt/disk0"]

[raft]
election_timeout_min_ms = 150
election_timeout_max_ms = 300
heartbeat_interval_ms = 50
bind_addr = "10.0.0.1:5555"

[gossip]
bind_addr = "127.0.0.1:9801"

[protocol]
"#;
        let config = NodeConfig::from_toml(toml).unwrap();
        assert_eq!(
            config.raft.bind_addr,
            Some("10.0.0.1:5555".parse().unwrap())
        );
    }
}
