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
pub mod metrics;
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
pub use metrics::{
    ALL_METRIC_NAMES, CLIENT_STALE_EPOCH_RETRIES_TOTAL, CLIENT_WATERMARK_VERSION,
    DISK_STATE_TRANSITION_TOTAL, InMemoryRecorder, Labels, METADATA_COMMIT_LATENCY_SECONDS,
    METADATA_QUORUM_HEALTH, MetricsRecorder, NODE_BACKGROUND_IO_LOAD, NODE_FOREGROUND_IO_LOAD,
    NoopRecorder, ORPHANED_EXTENT_FILES, REPAIR_BACKLOG_SIZE, REPAIR_COMPLETIONS_TOTAL,
    SCRUB_FINDINGS_TOTAL, SCRUB_LAST_COMPLETED_TIMESTAMP, VOLUME_IO_FAILURE_TOTAL,
    VOLUME_IO_SUCCESS_TOTAL,
};
pub use protection::ProtectionPolicy;
