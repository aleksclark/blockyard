//! Blockyard storage engine — extent files, disk management, and placement.
//!
//! Manages per-disk XFS filesystems, extent file lifecycle (staging, commit,
//! immutability), local extent index, and background scrub/repair.
//!
//! ## Failure Handling (Phase 5)
//!
//! - [`failure`] — Data node crash recovery, disk failure handling, and
//!   node startup ordering (P5.2, P5.3, P5.6, P5.7)
//!
//! ## Background Operations (Phase 7)
//!
//! - [`background`] — Scrub, repair, drain, rebalance, rate limiting,
//!   and coordinated scheduling (P7.1–P7.7)

pub mod background;
pub mod disk;
pub mod error;
pub mod extent;
pub mod failure;
pub mod health;
pub mod region;
pub mod service;

pub use disk::{DiskInventory, DiskMetadata, ManagedDisk, QualificationState};
pub use error::StorageError;
pub use extent::{
    ExtentIndex, ExtentMeta, ExtentStore, ExtentVersion, LocalExtentEntry, RecoveryReport,
    StorageClass,
};
pub use failure::{DedupCheckResult, DiskFailureReport, DiskRecoveryDetail, NodeRecoveryReport};
pub use health::{DiskHealthTracker, DiskTelemetry, HealthPolicy};
pub use region::{BadRegion, BadRegionMap};
pub use service::{CachedLease, DataNodeService, OperationRecord};
