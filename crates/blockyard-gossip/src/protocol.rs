use serde::{Deserialize, Serialize};
use blockyard_common::types::{NodeId, NodeInfo};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipMessage {
    Ping { from: NodeId, seq: u64 },
    PingReq { from: NodeId, target: NodeId, seq: u64 },
    Ack { from: NodeId, seq: u64 },
    Alive(NodeInfo),
    Suspect { node: NodeId, incarnation: u64 },
    Dead { node: NodeId, incarnation: u64 },
    Join(NodeInfo),
}
