//! CLI-specific response types returned by the client trait.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use blockyard_common::{DiskId, DiskState, EpochId, NodeId, ProtectionPolicy, VolumeId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VolumeState {
    Healthy,
    Degraded,
    Rebuilding,
    Unavailable,
}

impl std::fmt::Display for VolumeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VolumeState::Healthy => write!(f, "healthy"),
            VolumeState::Degraded => write!(f, "degraded"),
            VolumeState::Rebuilding => write!(f, "rebuilding"),
            VolumeState::Unavailable => write!(f, "unavailable"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeInfo {
    pub id: VolumeId,
    pub name: String,
    pub size_bytes: u64,
    pub protection: ProtectionPolicy,
    pub state: VolumeState,
    pub replica_nodes: Vec<NodeId>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskInfo {
    pub id: DiskId,
    pub node_id: NodeId,
    pub path: String,
    pub state: DiskState,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub extent_count: u64,
    pub error_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Online,
    Offline,
    Decommissioning,
}

impl std::fmt::Display for NodeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeState::Online => write!(f, "online"),
            NodeState::Offline => write!(f, "offline"),
            NodeState::Decommissioning => write!(f, "decommissioning"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: NodeId,
    pub address: String,
    pub state: NodeState,
    pub disk_count: u32,
    pub volume_count: u32,
    pub uptime_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuorumHealth {
    Healthy,
    Degraded,
    Lost,
}

impl std::fmt::Display for QuorumHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QuorumHealth::Healthy => write!(f, "healthy"),
            QuorumHealth::Degraded => write!(f, "degraded"),
            QuorumHealth::Lost => write!(f, "lost"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStatus {
    pub node_count: u32,
    pub nodes_online: u32,
    pub volume_count: u32,
    pub disk_count: u32,
    pub placement_epoch: EpochId,
    pub quorum_health: QuorumHealth,
    pub total_capacity_bytes: u64,
    pub used_capacity_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountInfo {
    pub volume_id: VolumeId,
    pub device_path: String,
    pub mount_point: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeCreateParams {
    pub name: String,
    pub size_bytes: u64,
    pub protection: ProtectionPolicy,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_volume_state_display() {
        assert_eq!(VolumeState::Healthy.to_string(), "healthy");
        assert_eq!(VolumeState::Degraded.to_string(), "degraded");
        assert_eq!(VolumeState::Rebuilding.to_string(), "rebuilding");
        assert_eq!(VolumeState::Unavailable.to_string(), "unavailable");
    }

    #[test]
    fn test_node_state_display() {
        assert_eq!(NodeState::Online.to_string(), "online");
        assert_eq!(NodeState::Offline.to_string(), "offline");
        assert_eq!(NodeState::Decommissioning.to_string(), "decommissioning");
    }

    #[test]
    fn test_quorum_health_display() {
        assert_eq!(QuorumHealth::Healthy.to_string(), "healthy");
        assert_eq!(QuorumHealth::Degraded.to_string(), "degraded");
        assert_eq!(QuorumHealth::Lost.to_string(), "lost");
    }

    #[test]
    fn test_volume_state_serde_roundtrip() {
        for state in [
            VolumeState::Healthy,
            VolumeState::Degraded,
            VolumeState::Rebuilding,
            VolumeState::Unavailable,
        ] {
            let json = serde_json::to_string(&state).unwrap();
            let parsed: VolumeState = serde_json::from_str(&json).unwrap();
            assert_eq!(state, parsed);
        }
    }

    #[test]
    fn test_node_state_serde_roundtrip() {
        for state in [
            NodeState::Online,
            NodeState::Offline,
            NodeState::Decommissioning,
        ] {
            let json = serde_json::to_string(&state).unwrap();
            let parsed: NodeState = serde_json::from_str(&json).unwrap();
            assert_eq!(state, parsed);
        }
    }

    #[test]
    fn test_quorum_health_serde_roundtrip() {
        for health in [
            QuorumHealth::Healthy,
            QuorumHealth::Degraded,
            QuorumHealth::Lost,
        ] {
            let json = serde_json::to_string(&health).unwrap();
            let parsed: QuorumHealth = serde_json::from_str(&json).unwrap();
            assert_eq!(health, parsed);
        }
    }

    #[test]
    fn test_volume_info_serde_roundtrip() {
        let info = VolumeInfo {
            id: VolumeId::generate(),
            name: "test-vol".into(),
            size_bytes: 1024 * 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            state: VolumeState::Healthy,
            replica_nodes: vec![NodeId::generate(), NodeId::generate()],
            created_at: Utc::now(),
        };
        let json = serde_json::to_string(&info).unwrap();
        let parsed: VolumeInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(info.id, parsed.id);
        assert_eq!(info.name, parsed.name);
        assert_eq!(info.size_bytes, parsed.size_bytes);
    }

    #[test]
    fn test_disk_info_serde_roundtrip() {
        let info = DiskInfo {
            id: DiskId::generate(),
            node_id: NodeId::generate(),
            path: "/dev/sda".into(),
            state: DiskState::Healthy,
            total_bytes: 1_000_000_000_000,
            used_bytes: 500_000_000_000,
            extent_count: 42,
            error_count: 0,
        };
        let json = serde_json::to_string(&info).unwrap();
        let parsed: DiskInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(info.id, parsed.id);
        assert_eq!(info.path, parsed.path);
    }

    #[test]
    fn test_node_info_serde_roundtrip() {
        let info = NodeInfo {
            id: NodeId::generate(),
            address: "10.0.0.1:9800".into(),
            state: NodeState::Online,
            disk_count: 4,
            volume_count: 10,
            uptime_seconds: 86400,
        };
        let json = serde_json::to_string(&info).unwrap();
        let parsed: NodeInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(info.id, parsed.id);
        assert_eq!(info.address, parsed.address);
    }

    #[test]
    fn test_cluster_status_serde_roundtrip() {
        let status = ClusterStatus {
            node_count: 5,
            nodes_online: 5,
            volume_count: 20,
            disk_count: 20,
            placement_epoch: EpochId::new(42),
            quorum_health: QuorumHealth::Healthy,
            total_capacity_bytes: 5_000_000_000_000,
            used_capacity_bytes: 2_000_000_000_000,
        };
        let json = serde_json::to_string(&status).unwrap();
        let parsed: ClusterStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(status.node_count, parsed.node_count);
        assert_eq!(status.placement_epoch, parsed.placement_epoch);
    }

    #[test]
    fn test_mount_info_serde_roundtrip() {
        let info = MountInfo {
            volume_id: VolumeId::generate(),
            device_path: "/dev/ublk0".into(),
            mount_point: Some("/mnt/data".into()),
        };
        let json = serde_json::to_string(&info).unwrap();
        let parsed: MountInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(info.volume_id, parsed.volume_id);
        assert_eq!(info.device_path, parsed.device_path);
        assert_eq!(info.mount_point, parsed.mount_point);
    }

    #[test]
    fn test_mount_info_no_mount_point() {
        let info = MountInfo {
            volume_id: VolumeId::generate(),
            device_path: "/dev/ublk0".into(),
            mount_point: None,
        };
        let json = serde_json::to_string(&info).unwrap();
        let parsed: MountInfo = serde_json::from_str(&json).unwrap();
        assert!(parsed.mount_point.is_none());
    }

    #[test]
    fn test_volume_create_params_serde() {
        let params = VolumeCreateParams {
            name: "test-vol".into(),
            size_bytes: 1024 * 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
        };
        let json = serde_json::to_string(&params).unwrap();
        let parsed: VolumeCreateParams = serde_json::from_str(&json).unwrap();
        assert_eq!(params.name, parsed.name);
        assert_eq!(params.size_bytes, parsed.size_bytes);
    }

    #[test]
    fn test_volume_state_equality() {
        assert_eq!(VolumeState::Healthy, VolumeState::Healthy);
        assert_ne!(VolumeState::Healthy, VolumeState::Degraded);
    }

    #[test]
    fn test_node_state_equality() {
        assert_eq!(NodeState::Online, NodeState::Online);
        assert_ne!(NodeState::Online, NodeState::Offline);
    }

    #[test]
    fn test_quorum_health_equality() {
        assert_eq!(QuorumHealth::Healthy, QuorumHealth::Healthy);
        assert_ne!(QuorumHealth::Healthy, QuorumHealth::Lost);
    }

    #[test]
    fn test_volume_state_debug() {
        let debug = format!("{:?}", VolumeState::Healthy);
        assert_eq!(debug, "Healthy");
    }

    #[test]
    fn test_node_state_debug() {
        let debug = format!("{:?}", NodeState::Online);
        assert_eq!(debug, "Online");
    }

    #[test]
    fn test_quorum_health_debug() {
        let debug = format!("{:?}", QuorumHealth::Healthy);
        assert_eq!(debug, "Healthy");
    }

    #[test]
    fn test_volume_state_clone() {
        let state = VolumeState::Rebuilding;
        let cloned = state.clone();
        assert_eq!(state, cloned);
    }

    #[test]
    fn test_node_state_clone() {
        let state = NodeState::Decommissioning;
        let cloned = state.clone();
        assert_eq!(state, cloned);
    }

    #[test]
    fn test_quorum_health_clone() {
        let health = QuorumHealth::Degraded;
        let cloned = health.clone();
        assert_eq!(health, cloned);
    }
}
