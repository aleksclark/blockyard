use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RaftRequest {
    VolumeCreate {
        name: String,
        size_bytes: u64,
        replicas: u32,
    },
    VolumeDelete {
        name: String,
    },
    VolumeResize {
        name: String,
        new_size: u64,
    },
    /// Online volume expansion through Raft consensus.
    /// After Raft commit, each replica node applies `zfs set volsize`.
    VolumeExpand {
        name: String,
        new_size: u64,
    },
    /// Change replication factor for a volume.
    VolumeSetReplicas {
        name: String,
        replicas: u32,
    },
    /// Change consistency mode for a volume.
    VolumeSetConsistency {
        name: String,
        consistency: String,
    },
    /// Change read policy for a volume.
    VolumeSetReadPolicy {
        name: String,
        read_policy: String,
    },
    PlacementUpdate {
        volume_name: String,
        nodes: Vec<u64>,
    },
    NodeRegister {
        node_id: u64,
        addr: String,
    },
    NodeDeregister {
        node_id: u64,
    },
    /// Initiate draining a node: mark it as Draining and prevent new placements.
    NodeDrain {
        node_id: u64,
    },
    /// Mark a node drain as fully complete: node transitions to Drained.
    NodeDrainComplete {
        node_id: u64,
    },
    Write {
        volume_id: u64,
        offset: u64,
        data: Vec<u8>,
    },
    RebalanceStart {
        volume_name: String,
        source: u64,
        target: u64,
    },
    RebalanceComplete {
        volume_name: String,
    },
    RebalanceFail {
        volume_name: String,
        reason: String,
    },
}

impl std::fmt::Display for RaftRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::VolumeCreate { name, .. } => write!(f, "VolumeCreate({name})"),
            Self::VolumeDelete { name } => write!(f, "VolumeDelete({name})"),
            Self::VolumeResize { name, new_size } => {
                write!(f, "VolumeResize({name}, {new_size})")
            }
            Self::VolumeExpand { name, new_size } => {
                write!(f, "VolumeExpand({name}, {new_size})")
            }
            Self::VolumeSetReplicas { name, replicas } => {
                write!(f, "VolumeSetReplicas({name}, {replicas})")
            }
            Self::VolumeSetConsistency {
                name, consistency, ..
            } => {
                write!(f, "VolumeSetConsistency({name}, {consistency})")
            }
            Self::VolumeSetReadPolicy {
                name, read_policy, ..
            } => {
                write!(f, "VolumeSetReadPolicy({name}, {read_policy})")
            }
            Self::PlacementUpdate { volume_name, .. } => {
                write!(f, "PlacementUpdate({volume_name})")
            }
            Self::NodeRegister { node_id, .. } => write!(f, "NodeRegister({node_id})"),
            Self::NodeDeregister { node_id } => write!(f, "NodeDeregister({node_id})"),
            Self::NodeDrain { node_id } => write!(f, "NodeDrain({node_id})"),
            Self::NodeDrainComplete { node_id } => write!(f, "NodeDrainComplete({node_id})"),
            Self::Write {
                volume_id, offset, ..
            } => {
                write!(f, "Write(vol={volume_id}, off={offset})")
            }
            Self::RebalanceStart {
                volume_name,
                source,
                target,
            } => {
                write!(f, "RebalanceStart({volume_name}, {source}->{target})")
            }
            Self::RebalanceComplete { volume_name } => {
                write!(f, "RebalanceComplete({volume_name})")
            }
            Self::RebalanceFail {
                volume_name,
                reason,
            } => {
                write!(f, "RebalanceFail({volume_name}, {reason})")
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RaftResponse {
    Ok,
    Error(String),
    Data(Vec<u8>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_request_serialization() {
        let req = RaftRequest::VolumeCreate {
            name: "vol-1".into(),
            size_bytes: 1024,
            replicas: 3,
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: RaftRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn test_raft_response_serialization() {
        let resp = RaftResponse::Ok;
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: RaftResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, resp);
    }

    #[test]
    fn test_raft_response_error() {
        let resp = RaftResponse::Error("something went wrong".into());
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: RaftResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, resp);
    }

    #[test]
    fn test_raft_response_data() {
        let resp = RaftResponse::Data(vec![1, 2, 3]);
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: RaftResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, resp);
    }

    #[test]
    fn test_raft_request_display() {
        let cases = vec![
            (
                RaftRequest::VolumeCreate {
                    name: "v".into(),
                    size_bytes: 1,
                    replicas: 1,
                },
                "VolumeCreate(v)",
            ),
            (
                RaftRequest::VolumeDelete { name: "v".into() },
                "VolumeDelete(v)",
            ),
            (
                RaftRequest::VolumeResize {
                    name: "v".into(),
                    new_size: 2,
                },
                "VolumeResize(v, 2)",
            ),
            (
                RaftRequest::VolumeExpand {
                    name: "v".into(),
                    new_size: 4,
                },
                "VolumeExpand(v, 4)",
            ),
            (
                RaftRequest::VolumeSetReplicas {
                    name: "v".into(),
                    replicas: 5,
                },
                "VolumeSetReplicas(v, 5)",
            ),
            (
                RaftRequest::VolumeSetConsistency {
                    name: "v".into(),
                    consistency: "all".into(),
                },
                "VolumeSetConsistency(v, all)",
            ),
            (
                RaftRequest::VolumeSetReadPolicy {
                    name: "v".into(),
                    read_policy: "leader".into(),
                },
                "VolumeSetReadPolicy(v, leader)",
            ),
            (
                RaftRequest::PlacementUpdate {
                    volume_name: "v".into(),
                    nodes: vec![1],
                },
                "PlacementUpdate(v)",
            ),
            (
                RaftRequest::NodeRegister {
                    node_id: 1,
                    addr: "a".into(),
                },
                "NodeRegister(1)",
            ),
            (
                RaftRequest::NodeDeregister { node_id: 1 },
                "NodeDeregister(1)",
            ),
            (RaftRequest::NodeDrain { node_id: 1 }, "NodeDrain(1)"),
            (
                RaftRequest::NodeDrainComplete { node_id: 1 },
                "NodeDrainComplete(1)",
            ),
            (
                RaftRequest::Write {
                    volume_id: 1,
                    offset: 0,
                    data: vec![0],
                },
                "Write(vol=1, off=0)",
            ),
            (
                RaftRequest::RebalanceStart {
                    volume_name: "v".into(),
                    source: 1,
                    target: 2,
                },
                "RebalanceStart(v, 1->2)",
            ),
            (
                RaftRequest::RebalanceComplete {
                    volume_name: "v".into(),
                },
                "RebalanceComplete(v)",
            ),
            (
                RaftRequest::RebalanceFail {
                    volume_name: "v".into(),
                    reason: "err".into(),
                },
                "RebalanceFail(v, err)",
            ),
        ];
        for (req, expected) in cases {
            assert_eq!(req.to_string(), expected);
        }
    }

    #[test]
    fn test_raft_request_all_variants_roundtrip() {
        let variants: Vec<RaftRequest> = vec![
            RaftRequest::VolumeCreate {
                name: "v".into(),
                size_bytes: 1,
                replicas: 1,
            },
            RaftRequest::VolumeDelete { name: "v".into() },
            RaftRequest::VolumeResize {
                name: "v".into(),
                new_size: 2,
            },
            RaftRequest::VolumeExpand {
                name: "v".into(),
                new_size: 4,
            },
            RaftRequest::VolumeSetReplicas {
                name: "v".into(),
                replicas: 5,
            },
            RaftRequest::VolumeSetConsistency {
                name: "v".into(),
                consistency: "all".into(),
            },
            RaftRequest::VolumeSetReadPolicy {
                name: "v".into(),
                read_policy: "leader".into(),
            },
            RaftRequest::PlacementUpdate {
                volume_name: "v".into(),
                nodes: vec![1],
            },
            RaftRequest::NodeRegister {
                node_id: 1,
                addr: "a".into(),
            },
            RaftRequest::NodeDeregister { node_id: 1 },
            RaftRequest::NodeDrain { node_id: 1 },
            RaftRequest::NodeDrainComplete { node_id: 1 },
            RaftRequest::Write {
                volume_id: 1,
                offset: 0,
                data: vec![0],
            },
            RaftRequest::RebalanceStart {
                volume_name: "v".into(),
                source: 1,
                target: 2,
            },
            RaftRequest::RebalanceComplete {
                volume_name: "v".into(),
            },
            RaftRequest::RebalanceFail {
                volume_name: "v".into(),
                reason: "disk full".into(),
            },
        ];
        for v in &variants {
            let json = serde_json::to_string(v).unwrap();
            let decoded: RaftRequest = serde_json::from_str(&json).unwrap();
            assert_eq!(&decoded, v);
        }
    }

    #[test]
    fn test_rebalance_start_serialization() {
        let req = RaftRequest::RebalanceStart {
            volume_name: "vol-1".into(),
            source: 10,
            target: 20,
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: RaftRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn test_rebalance_complete_serialization() {
        let req = RaftRequest::RebalanceComplete {
            volume_name: "vol-1".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: RaftRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn test_rebalance_fail_serialization() {
        let req = RaftRequest::RebalanceFail {
            volume_name: "vol-1".into(),
            reason: "network timeout".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: RaftRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, req);
    }

    // ── New variant serialization tests ─────────────────────────────────

    #[test]
    fn test_volume_expand_serialization() {
        let req = RaftRequest::VolumeExpand {
            name: "vol-1".into(),
            new_size: 2048,
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: RaftRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn test_volume_set_replicas_serialization() {
        let req = RaftRequest::VolumeSetReplicas {
            name: "vol-1".into(),
            replicas: 5,
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: RaftRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn test_volume_set_consistency_serialization() {
        let req = RaftRequest::VolumeSetConsistency {
            name: "vol-1".into(),
            consistency: "all".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: RaftRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn test_volume_set_read_policy_serialization() {
        let req = RaftRequest::VolumeSetReadPolicy {
            name: "vol-1".into(),
            read_policy: "leader".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: RaftRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn test_node_drain_serialization() {
        let req = RaftRequest::NodeDrain { node_id: 42 };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: RaftRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn test_node_drain_complete_serialization() {
        let req = RaftRequest::NodeDrainComplete { node_id: 42 };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: RaftRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, req);
    }
}
