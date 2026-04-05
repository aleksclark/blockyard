use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use uuid::Uuid;

pub type NodeId = u64;
pub type VolumeId = Uuid;
pub type ExtentId = u64;
pub type RaftGroupId = u64;
pub type Term = u64;
pub type LogIndex = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: NodeId,
    pub name: String,
    pub addr: SocketAddr,
    pub data_addr: SocketAddr,
    pub tags: HashMap<String, String>,
    pub state: NodeState,
    pub zfs_health: ZfsHealthState,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub incarnation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Healthy,
    Suspect,
    Failed,
    Draining,
    Left,
}

impl std::fmt::Display for NodeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Healthy => write!(f, "healthy"),
            Self::Suspect => write!(f, "suspect"),
            Self::Failed => write!(f, "failed"),
            Self::Draining => write!(f, "draining"),
            Self::Left => write!(f, "left"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ZfsHealthState {
    Online,
    Degraded,
    Faulted,
    Unknown,
}

impl Default for ZfsHealthState {
    fn default() -> Self {
        Self::Unknown
    }
}

impl std::fmt::Display for ZfsHealthState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Online => write!(f, "online"),
            Self::Degraded => write!(f, "degraded"),
            Self::Faulted => write!(f, "faulted"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZfsPoolHealth {
    pub pool_name: String,
    pub state: ZfsHealthState,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub fragmentation_pct: u8,
    pub checksum_errors: u64,
    pub read_errors: u64,
    pub write_errors: u64,
    pub scrub_errors: u64,
    pub last_scrub_timestamp: Option<u64>,
    pub vdevs: Vec<VdevHealth>,
}

impl Default for ZfsPoolHealth {
    fn default() -> Self {
        Self {
            pool_name: String::new(),
            state: ZfsHealthState::Unknown,
            capacity_bytes: 0,
            used_bytes: 0,
            free_bytes: 0,
            fragmentation_pct: 0,
            checksum_errors: 0,
            read_errors: 0,
            write_errors: 0,
            scrub_errors: 0,
            last_scrub_timestamp: None,
            vdevs: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VdevHealth {
    pub name: String,
    pub state: ZfsHealthState,
    pub read_errors: u64,
    pub write_errors: u64,
    pub checksum_errors: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeSpec {
    pub id: VolumeId,
    pub name: String,
    pub size_bytes: u64,
    pub replicas: u32,
    pub consistency: WriteConsistency,
    pub read_policy: ReadPolicy,
    pub affinity: HashMap<String, String>,
    pub anti_affinity: HashMap<String, String>,
    pub failure_domain: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteConsistency {
    All,
    Majority,
    Single,
}

impl std::fmt::Display for WriteConsistency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::All => write!(f, "all"),
            Self::Majority => write!(f, "majority"),
            Self::Single => write!(f, "single"),
        }
    }
}

impl std::str::FromStr for WriteConsistency {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "all" => Ok(Self::All),
            "majority" => Ok(Self::Majority),
            "single" => Ok(Self::Single),
            _ => Err(format!("unknown write consistency: {s}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadPolicy {
    Leader,
    Any,
    Local,
}

impl std::fmt::Display for ReadPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Leader => write!(f, "leader"),
            Self::Any => write!(f, "any"),
            Self::Local => write!(f, "local"),
        }
    }
}

impl std::str::FromStr for ReadPolicy {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "leader" => Ok(Self::Leader),
            "any" => Ok(Self::Any),
            "local" => Ok(Self::Local),
            _ => Err(format!("unknown read policy: {s}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VolumeState {
    Creating,
    Healthy,
    Degraded,
    Unavailable,
    Deleting,
}

impl std::fmt::Display for VolumeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Creating => write!(f, "creating"),
            Self::Healthy => write!(f, "healthy"),
            Self::Degraded => write!(f, "degraded"),
            Self::Unavailable => write!(f, "unavailable"),
            Self::Deleting => write!(f, "deleting"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumePlacement {
    pub volume_id: VolumeId,
    pub leader: NodeId,
    pub replicas: Vec<NodeId>,
    pub state: VolumeState,
}

pub fn parse_size(s: &str) -> std::result::Result<u64, String> {
    let s = s.trim();
    let (num_str, multiplier) = if let Some(n) = s.strip_suffix("TB") {
        (n.trim(), 1024u64 * 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("GB") {
        (n.trim(), 1024u64 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("MB") {
        (n.trim(), 1024u64 * 1024)
    } else if let Some(n) = s.strip_suffix("KB") {
        (n.trim(), 1024u64)
    } else if let Some(n) = s.strip_suffix("T") {
        (n.trim(), 1024u64 * 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("G") {
        (n.trim(), 1024u64 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("M") {
        (n.trim(), 1024u64 * 1024)
    } else if let Some(n) = s.strip_suffix("K") {
        (n.trim(), 1024u64)
    } else if let Some(n) = s.strip_suffix('B') {
        (n.trim(), 1u64)
    } else {
        (s, 1u64)
    };
    let num: f64 = num_str
        .parse()
        .map_err(|e| format!("invalid size number '{num_str}': {e}"))?;
    Ok((num * multiplier as f64) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size_bytes() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
        assert_eq!(parse_size("0").unwrap(), 0);
    }

    #[test]
    fn test_parse_size_kilobytes() {
        assert_eq!(parse_size("1KB").unwrap(), 1024);
        assert_eq!(parse_size("4K").unwrap(), 4096);
    }

    #[test]
    fn test_parse_size_megabytes() {
        assert_eq!(parse_size("4MB").unwrap(), 4 * 1024 * 1024);
        assert_eq!(parse_size("1M").unwrap(), 1024 * 1024);
    }

    #[test]
    fn test_parse_size_gigabytes() {
        assert_eq!(parse_size("100GB").unwrap(), 100 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("1G").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_terabytes() {
        assert_eq!(parse_size("2TB").unwrap(), 2 * 1024u64 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("1T").unwrap(), 1024u64 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_with_b_suffix() {
        assert_eq!(parse_size("512B").unwrap(), 512);
    }

    #[test]
    fn test_parse_size_whitespace() {
        assert_eq!(parse_size("  100GB  ").unwrap(), 100 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_invalid() {
        assert!(parse_size("abc").is_err());
        assert!(parse_size("GB").is_err());
    }

    #[test]
    fn test_parse_size_fractional() {
        assert_eq!(parse_size("1.5GB").unwrap(), (1.5 * 1024.0 * 1024.0 * 1024.0) as u64);
    }

    #[test]
    fn test_node_state_display() {
        assert_eq!(NodeState::Healthy.to_string(), "healthy");
        assert_eq!(NodeState::Suspect.to_string(), "suspect");
        assert_eq!(NodeState::Failed.to_string(), "failed");
        assert_eq!(NodeState::Draining.to_string(), "draining");
        assert_eq!(NodeState::Left.to_string(), "left");
    }

    #[test]
    fn test_zfs_health_state_display() {
        assert_eq!(ZfsHealthState::Online.to_string(), "online");
        assert_eq!(ZfsHealthState::Degraded.to_string(), "degraded");
        assert_eq!(ZfsHealthState::Faulted.to_string(), "faulted");
        assert_eq!(ZfsHealthState::Unknown.to_string(), "unknown");
    }

    #[test]
    fn test_zfs_health_state_default() {
        assert_eq!(ZfsHealthState::default(), ZfsHealthState::Unknown);
    }

    #[test]
    fn test_write_consistency_from_str() {
        assert_eq!("all".parse::<WriteConsistency>().unwrap(), WriteConsistency::All);
        assert_eq!("majority".parse::<WriteConsistency>().unwrap(), WriteConsistency::Majority);
        assert_eq!("single".parse::<WriteConsistency>().unwrap(), WriteConsistency::Single);
        assert_eq!("ALL".parse::<WriteConsistency>().unwrap(), WriteConsistency::All);
        assert!("bad".parse::<WriteConsistency>().is_err());
    }

    #[test]
    fn test_write_consistency_display() {
        assert_eq!(WriteConsistency::All.to_string(), "all");
        assert_eq!(WriteConsistency::Majority.to_string(), "majority");
        assert_eq!(WriteConsistency::Single.to_string(), "single");
    }

    #[test]
    fn test_read_policy_from_str() {
        assert_eq!("leader".parse::<ReadPolicy>().unwrap(), ReadPolicy::Leader);
        assert_eq!("any".parse::<ReadPolicy>().unwrap(), ReadPolicy::Any);
        assert_eq!("local".parse::<ReadPolicy>().unwrap(), ReadPolicy::Local);
        assert_eq!("LOCAL".parse::<ReadPolicy>().unwrap(), ReadPolicy::Local);
        assert!("bad".parse::<ReadPolicy>().is_err());
    }

    #[test]
    fn test_read_policy_display() {
        assert_eq!(ReadPolicy::Leader.to_string(), "leader");
        assert_eq!(ReadPolicy::Any.to_string(), "any");
        assert_eq!(ReadPolicy::Local.to_string(), "local");
    }

    #[test]
    fn test_volume_state_display() {
        assert_eq!(VolumeState::Creating.to_string(), "creating");
        assert_eq!(VolumeState::Healthy.to_string(), "healthy");
        assert_eq!(VolumeState::Degraded.to_string(), "degraded");
        assert_eq!(VolumeState::Unavailable.to_string(), "unavailable");
        assert_eq!(VolumeState::Deleting.to_string(), "deleting");
    }

    #[test]
    fn test_zfs_pool_health_default() {
        let h = ZfsPoolHealth::default();
        assert_eq!(h.state, ZfsHealthState::Unknown);
        assert_eq!(h.capacity_bytes, 0);
        assert!(h.vdevs.is_empty());
        assert!(h.last_scrub_timestamp.is_none());
    }

    #[test]
    fn test_node_info_serialization() {
        let info = NodeInfo {
            id: 1,
            name: "test-node".to_string(),
            addr: "127.0.0.1:7400".parse().unwrap(),
            data_addr: "127.0.0.1:7401".parse().unwrap(),
            tags: HashMap::from([("rack".to_string(), "r1".to_string())]),
            state: NodeState::Healthy,
            zfs_health: ZfsHealthState::Online,
            capacity_bytes: 1024,
            used_bytes: 512,
            incarnation: 1,
        };
        let json = serde_json::to_string(&info).unwrap();
        let deser: NodeInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.id, info.id);
        assert_eq!(deser.state, info.state);
        assert_eq!(deser.zfs_health, info.zfs_health);
    }

    #[test]
    fn test_volume_spec_serialization() {
        let spec = VolumeSpec {
            id: VolumeId::new_v4(),
            name: "test-vol".to_string(),
            size_bytes: parse_size("100GB").unwrap(),
            replicas: 3,
            consistency: WriteConsistency::Majority,
            read_policy: ReadPolicy::Any,
            affinity: HashMap::new(),
            anti_affinity: HashMap::new(),
            failure_domain: "node".to_string(),
        };
        let json = serde_json::to_string(&spec).unwrap();
        let deser: VolumeSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.name, "test-vol");
        assert_eq!(deser.replicas, 3);
    }

    #[test]
    fn test_volume_placement_serialization() {
        let p = VolumePlacement {
            volume_id: VolumeId::new_v4(),
            leader: 1,
            replicas: vec![1, 2, 3],
            state: VolumeState::Healthy,
        };
        let json = serde_json::to_string(&p).unwrap();
        let deser: VolumePlacement = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.replicas.len(), 3);
        assert_eq!(deser.state, VolumeState::Healthy);
    }
}
