//! Gossip protocol message types and serialization.
//!
//! All SWIM protocol messages are represented as variants of [`GossipMessage`].
//! Messages are serialized with JSON for wire transmission, with membership
//! updates piggybacked on every outgoing message.

use std::net::SocketAddr;

use blockyard_common::NodeId;
use serde::{Deserialize, Serialize};

/// State of a cluster member as disseminated through gossip.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MemberState {
    Alive,
    Suspect,
    Dead,
}

/// A single membership update piggybacked on protocol messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipUpdate {
    pub node_id: NodeId,
    pub addr: SocketAddr,
    pub state: MemberState,
    pub incarnation: u64,
}

/// SWIM protocol messages exchanged between gossip nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GossipMessage {
    /// Direct probe — expects an Ack response.
    Ping {
        from: NodeId,
        from_addr: SocketAddr,
        seq: u64,
        updates: Vec<MembershipUpdate>,
    },
    /// Indirect probe request — asks a relay node to ping the target.
    PingReq {
        from: NodeId,
        from_addr: SocketAddr,
        target: NodeId,
        target_addr: SocketAddr,
        seq: u64,
        updates: Vec<MembershipUpdate>,
    },
    /// Acknowledgement of a Ping or PingReq.
    Ack {
        from: NodeId,
        from_addr: SocketAddr,
        seq: u64,
        updates: Vec<MembershipUpdate>,
    },
    /// Join request sent to seed nodes.
    Join { node_id: NodeId, addr: SocketAddr },
    /// Alive declaration — refutes suspicion with a higher incarnation.
    Alive {
        node_id: NodeId,
        addr: SocketAddr,
        incarnation: u64,
    },
    /// Suspicion declaration for a node that missed probes.
    Suspect {
        node_id: NodeId,
        addr: SocketAddr,
        incarnation: u64,
    },
    /// Dead declaration — node has been confirmed dead.
    Dead {
        node_id: NodeId,
        addr: SocketAddr,
        incarnation: u64,
    },
}

impl GossipMessage {
    /// Serialize this message to JSON bytes.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserialize a message from JSON bytes.
    pub fn decode(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data)
    }

    /// Extract piggybacked membership updates from a message.
    pub fn updates(&self) -> &[MembershipUpdate] {
        match self {
            GossipMessage::Ping { updates, .. }
            | GossipMessage::PingReq { updates, .. }
            | GossipMessage::Ack { updates, .. } => updates,
            _ => &[],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_node_id() -> NodeId {
        NodeId::generate()
    }

    fn test_addr() -> SocketAddr {
        "127.0.0.1:9000".parse().unwrap()
    }

    fn test_update() -> MembershipUpdate {
        MembershipUpdate {
            node_id: test_node_id(),
            addr: test_addr(),
            state: MemberState::Alive,
            incarnation: 1,
        }
    }

    #[test]
    fn test_ping_encode_decode() {
        let msg = GossipMessage::Ping {
            from: test_node_id(),
            from_addr: test_addr(),
            seq: 42,
            updates: vec![test_update()],
        };
        let bytes = msg.encode().unwrap();
        let decoded = GossipMessage::decode(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_ping_req_encode_decode() {
        let msg = GossipMessage::PingReq {
            from: test_node_id(),
            from_addr: test_addr(),
            target: test_node_id(),
            target_addr: "127.0.0.1:9001".parse().unwrap(),
            seq: 7,
            updates: vec![],
        };
        let bytes = msg.encode().unwrap();
        let decoded = GossipMessage::decode(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_ack_encode_decode() {
        let msg = GossipMessage::Ack {
            from: test_node_id(),
            from_addr: test_addr(),
            seq: 1,
            updates: vec![test_update(), test_update()],
        };
        let bytes = msg.encode().unwrap();
        let decoded = GossipMessage::decode(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_join_encode_decode() {
        let msg = GossipMessage::Join {
            node_id: test_node_id(),
            addr: test_addr(),
        };
        let bytes = msg.encode().unwrap();
        let decoded = GossipMessage::decode(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_alive_encode_decode() {
        let msg = GossipMessage::Alive {
            node_id: test_node_id(),
            addr: test_addr(),
            incarnation: 5,
        };
        let bytes = msg.encode().unwrap();
        let decoded = GossipMessage::decode(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_suspect_encode_decode() {
        let msg = GossipMessage::Suspect {
            node_id: test_node_id(),
            addr: test_addr(),
            incarnation: 3,
        };
        let bytes = msg.encode().unwrap();
        let decoded = GossipMessage::decode(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_dead_encode_decode() {
        let msg = GossipMessage::Dead {
            node_id: test_node_id(),
            addr: test_addr(),
            incarnation: 10,
        };
        let bytes = msg.encode().unwrap();
        let decoded = GossipMessage::decode(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_decode_invalid_bytes() {
        let result = GossipMessage::decode(b"not json");
        assert!(result.is_err());
    }

    #[test]
    fn test_updates_from_ping() {
        let upd = test_update();
        let msg = GossipMessage::Ping {
            from: test_node_id(),
            from_addr: test_addr(),
            seq: 1,
            updates: vec![upd.clone()],
        };
        assert_eq!(msg.updates().len(), 1);
        assert_eq!(msg.updates()[0], upd);
    }

    #[test]
    fn test_updates_from_ping_req() {
        let upd = test_update();
        let msg = GossipMessage::PingReq {
            from: test_node_id(),
            from_addr: test_addr(),
            target: test_node_id(),
            target_addr: test_addr(),
            seq: 1,
            updates: vec![upd.clone()],
        };
        assert_eq!(msg.updates().len(), 1);
        assert_eq!(msg.updates()[0], upd);
    }

    #[test]
    fn test_updates_from_ack() {
        let msg = GossipMessage::Ack {
            from: test_node_id(),
            from_addr: test_addr(),
            seq: 1,
            updates: vec![],
        };
        assert!(msg.updates().is_empty());
    }

    #[test]
    fn test_updates_from_join() {
        let msg = GossipMessage::Join {
            node_id: test_node_id(),
            addr: test_addr(),
        };
        assert!(msg.updates().is_empty());
    }

    #[test]
    fn test_updates_from_alive() {
        let msg = GossipMessage::Alive {
            node_id: test_node_id(),
            addr: test_addr(),
            incarnation: 1,
        };
        assert!(msg.updates().is_empty());
    }

    #[test]
    fn test_updates_from_suspect() {
        let msg = GossipMessage::Suspect {
            node_id: test_node_id(),
            addr: test_addr(),
            incarnation: 1,
        };
        assert!(msg.updates().is_empty());
    }

    #[test]
    fn test_updates_from_dead() {
        let msg = GossipMessage::Dead {
            node_id: test_node_id(),
            addr: test_addr(),
            incarnation: 1,
        };
        assert!(msg.updates().is_empty());
    }

    #[test]
    fn test_member_state_serde() {
        for state in [MemberState::Alive, MemberState::Suspect, MemberState::Dead] {
            let json = serde_json::to_string(&state).unwrap();
            let parsed: MemberState = serde_json::from_str(&json).unwrap();
            assert_eq!(state, parsed);
        }
    }

    #[test]
    fn test_membership_update_serde() {
        let upd = test_update();
        let json = serde_json::to_string(&upd).unwrap();
        let parsed: MembershipUpdate = serde_json::from_str(&json).unwrap();
        assert_eq!(upd, parsed);
    }

    #[test]
    fn test_member_state_debug() {
        assert!(format!("{:?}", MemberState::Alive).contains("Alive"));
        assert!(format!("{:?}", MemberState::Suspect).contains("Suspect"));
        assert!(format!("{:?}", MemberState::Dead).contains("Dead"));
    }

    #[test]
    fn test_member_state_clone() {
        let s = MemberState::Suspect;
        let cloned = s;
        assert_eq!(s, cloned);
    }

    #[test]
    fn test_membership_update_clone() {
        let upd = test_update();
        let cloned = upd.clone();
        assert_eq!(upd, cloned);
    }

    #[test]
    fn test_gossip_message_clone() {
        let msg = GossipMessage::Ping {
            from: test_node_id(),
            from_addr: test_addr(),
            seq: 1,
            updates: vec![],
        };
        let cloned = msg.clone();
        assert_eq!(msg, cloned);
    }

    #[test]
    fn test_gossip_message_debug() {
        let msg = GossipMessage::Join {
            node_id: test_node_id(),
            addr: test_addr(),
        };
        let debug = format!("{:?}", msg);
        assert!(debug.contains("Join"));
    }

    #[test]
    fn test_multiple_updates_roundtrip() {
        let updates = vec![
            MembershipUpdate {
                node_id: test_node_id(),
                addr: "127.0.0.1:9000".parse().unwrap(),
                state: MemberState::Alive,
                incarnation: 1,
            },
            MembershipUpdate {
                node_id: test_node_id(),
                addr: "127.0.0.1:9001".parse().unwrap(),
                state: MemberState::Suspect,
                incarnation: 3,
            },
            MembershipUpdate {
                node_id: test_node_id(),
                addr: "127.0.0.1:9002".parse().unwrap(),
                state: MemberState::Dead,
                incarnation: 7,
            },
        ];
        let msg = GossipMessage::Ping {
            from: test_node_id(),
            from_addr: test_addr(),
            seq: 99,
            updates,
        };
        let bytes = msg.encode().unwrap();
        let decoded = GossipMessage::decode(&bytes).unwrap();
        assert_eq!(msg, decoded);
        assert_eq!(decoded.updates().len(), 3);
    }

    #[test]
    fn test_member_state_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(MemberState::Alive);
        set.insert(MemberState::Suspect);
        set.insert(MemberState::Dead);
        set.insert(MemberState::Alive);
        assert_eq!(set.len(), 3);
    }
}
