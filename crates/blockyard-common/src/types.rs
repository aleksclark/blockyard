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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Healthy,
    Suspect,
    Failed,
    Draining,
    Left,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadPolicy {
    Leader,
    Any,
    Local,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VolumeState {
    Creating,
    Healthy,
    Degraded,
    Unavailable,
    Deleting,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumePlacement {
    pub volume_id: VolumeId,
    pub leader: NodeId,
    pub replicas: Vec<NodeId>,
    pub state: VolumeState,
}
