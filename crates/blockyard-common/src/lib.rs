//! Blockyard common types, configuration, and error definitions.
//!
//! This crate provides the shared vocabulary used by all Blockyard components:
//! core identifier types, protection policies, disk state management,
//! configuration, and error types.

pub mod auth;
pub mod checksum;
pub mod config;
pub mod disk_state;
pub mod error;
pub mod id;
pub mod lease;
pub mod protection;

pub use auth::{
    AuthProvider, AuthToken, DEFAULT_TOKEN_TTL_MS, PeerIdentity, SharedSecretAuth, VolumeAcl,
    VolumePermission,
};
pub use config::{
    AuthSection, GossipSection, NodeConfig, ProtocolSection, RaftSection, StorageSection,
    TlsSection,
};
pub use disk_state::DiskState;
pub use error::Error;
pub use id::{DiskId, EpochId, ExtentId, NodeId, OperationId, RaftGroupId, SessionId, VolumeId};
pub use lease::{DEFAULT_LEASE_TTL, LeaseRequest, LeaseResponse, LeaseVersion, VolumeLease};
pub use protection::ProtectionPolicy;
