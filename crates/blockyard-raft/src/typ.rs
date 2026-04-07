//! Raft type configuration for Blockyard metadata consensus.

use std::io::Cursor;

use openraft::BasicNode;
use openraft::TokioRuntime;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = crate::request::MetadataRequest,
        R = crate::response::MetadataResponse,
        NodeId = u64,
        Node = BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = TokioRuntime,
);

pub type LogId = openraft::LogId<u64>;
pub type Vote = openraft::Vote<u64>;
pub type SnapshotMeta = openraft::SnapshotMeta<u64, BasicNode>;
pub type Snapshot = openraft::Snapshot<TypeConfig>;
pub type StoredMembership = openraft::StoredMembership<u64, BasicNode>;
pub type Entry = openraft::Entry<TypeConfig>;

pub type Raft = openraft::Raft<TypeConfig>;
