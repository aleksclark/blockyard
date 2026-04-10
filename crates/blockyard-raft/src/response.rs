//! Metadata service response types returned from state machine application.

use serde::{Deserialize, Serialize};

use blockyard_common::{EpochId, LeaseResponse};

/// Response from applying a [`MetadataRequest`](crate::request::MetadataRequest) to the state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataResponse {
    /// No meaningful return value.
    Ok,

    /// The current placement epoch after the operation.
    Epoch(EpochId),

    /// An application-level error (e.g., CAS failure, volume not found).
    Error(String),

    /// Response to a lease request (P6.1).
    Lease(LeaseResponse),

    /// A raft node ID was assigned for a newly registered node.
    NodeRegistered(u64),

    /// A disk was registered (or updated) in the cluster metadata.
    DiskRegistered,
}

impl MetadataResponse {
    pub fn ok() -> Self {
        MetadataResponse::Ok
    }

    pub fn epoch(epoch: EpochId) -> Self {
        MetadataResponse::Epoch(epoch)
    }

    pub fn error(msg: impl Into<String>) -> Self {
        MetadataResponse::Error(msg.into())
    }

    pub fn is_error(&self) -> bool {
        matches!(self, MetadataResponse::Error(_))
    }
}
