use blockyard_common::types::{NodeId, NodeInfo, ZfsHealthState};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipMessage {
    Ping {
        from: NodeId,
        seq: u64,
    },
    PingReq {
        from: NodeId,
        target: NodeId,
        seq: u64,
    },
    Ack {
        from: NodeId,
        seq: u64,
    },
    Alive(NodeInfo),
    Suspect {
        node: NodeId,
        incarnation: u64,
    },
    Dead {
        node: NodeId,
        incarnation: u64,
    },
    Join(NodeInfo),
    Compound {
        primary: Box<GossipMessage>,
        piggyback: Vec<GossipUpdate>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipUpdate {
    NodeAlive(NodeInfo),
    NodeSuspect { node: NodeId, incarnation: u64 },
    NodeDead { node: NodeId, incarnation: u64 },
    NodeLeft { node: NodeId },
    ZfsHealth { node: NodeId, state: ZfsHealthState },
}

impl GossipMessage {
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("gossip message serialization should not fail")
    }

    pub fn decode(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

impl GossipUpdate {
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("gossip update serialization should not fail")
    }

    pub fn decode(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping_encode_decode() {
        let msg = GossipMessage::Ping { from: 1, seq: 42 };
        let data = msg.encode();
        let decoded = GossipMessage::decode(&data).unwrap();
        match decoded {
            GossipMessage::Ping { from, seq } => {
                assert_eq!(from, 1);
                assert_eq!(seq, 42);
            }
            _ => panic!("expected Ping"),
        }
    }

    #[test]
    fn test_ping_req_encode_decode() {
        let msg = GossipMessage::PingReq {
            from: 1,
            target: 2,
            seq: 10,
        };
        let data = msg.encode();
        let decoded = GossipMessage::decode(&data).unwrap();
        match decoded {
            GossipMessage::PingReq { from, target, seq } => {
                assert_eq!(from, 1);
                assert_eq!(target, 2);
                assert_eq!(seq, 10);
            }
            _ => panic!("expected PingReq"),
        }
    }

    #[test]
    fn test_ack_encode_decode() {
        let msg = GossipMessage::Ack { from: 5, seq: 99 };
        let data = msg.encode();
        let decoded = GossipMessage::decode(&data).unwrap();
        match decoded {
            GossipMessage::Ack { from, seq } => {
                assert_eq!(from, 5);
                assert_eq!(seq, 99);
            }
            _ => panic!("expected Ack"),
        }
    }

    #[test]
    fn test_suspect_encode_decode() {
        let msg = GossipMessage::Suspect {
            node: 3,
            incarnation: 7,
        };
        let data = msg.encode();
        let decoded = GossipMessage::decode(&data).unwrap();
        match decoded {
            GossipMessage::Suspect { node, incarnation } => {
                assert_eq!(node, 3);
                assert_eq!(incarnation, 7);
            }
            _ => panic!("expected Suspect"),
        }
    }

    #[test]
    fn test_dead_encode_decode() {
        let msg = GossipMessage::Dead {
            node: 4,
            incarnation: 2,
        };
        let data = msg.encode();
        let decoded = GossipMessage::decode(&data).unwrap();
        match decoded {
            GossipMessage::Dead { node, incarnation } => {
                assert_eq!(node, 4);
                assert_eq!(incarnation, 2);
            }
            _ => panic!("expected Dead"),
        }
    }

    #[test]
    fn test_compound_encode_decode() {
        let msg = GossipMessage::Compound {
            primary: Box::new(GossipMessage::Ping { from: 1, seq: 1 }),
            piggyback: vec![
                GossipUpdate::NodeSuspect {
                    node: 5,
                    incarnation: 1,
                },
                GossipUpdate::ZfsHealth {
                    node: 2,
                    state: ZfsHealthState::Degraded,
                },
            ],
        };
        let data = msg.encode();
        let decoded = GossipMessage::decode(&data).unwrap();
        match decoded {
            GossipMessage::Compound { primary, piggyback } => {
                assert!(matches!(*primary, GossipMessage::Ping { .. }));
                assert_eq!(piggyback.len(), 2);
            }
            _ => panic!("expected Compound"),
        }
    }

    #[test]
    fn test_gossip_update_node_alive() {
        use std::collections::HashMap;
        let info = NodeInfo {
            id: 1,
            name: "n1".into(),
            addr: "127.0.0.1:7400".parse().unwrap(),
            data_addr: "127.0.0.1:7401".parse().unwrap(),
            tags: HashMap::new(),
            state: blockyard_common::types::NodeState::Healthy,
            zfs_health: ZfsHealthState::Online,
            capacity_bytes: 0,
            used_bytes: 0,
            incarnation: 1,
            pools: Vec::new(),
        };
        let update = GossipUpdate::NodeAlive(info);
        let data = update.encode();
        let decoded = GossipUpdate::decode(&data).unwrap();
        match decoded {
            GossipUpdate::NodeAlive(n) => assert_eq!(n.id, 1),
            _ => panic!("expected NodeAlive"),
        }
    }

    #[test]
    fn test_gossip_update_node_left() {
        let update = GossipUpdate::NodeLeft { node: 9 };
        let data = update.encode();
        let decoded = GossipUpdate::decode(&data).unwrap();
        match decoded {
            GossipUpdate::NodeLeft { node } => assert_eq!(node, 9),
            _ => panic!("expected NodeLeft"),
        }
    }

    #[test]
    fn test_decode_invalid_data() {
        let result = GossipMessage::decode(b"not json");
        assert!(result.is_err());
    }
}
