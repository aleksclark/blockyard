//! Blockyard storage engine — extent files, disk management, and placement.
//!
//! Manages per-disk XFS filesystems, extent file lifecycle (staging, commit,
//! immutability), local extent index, and background scrub/repair.

pub mod disk;
pub mod error;
pub mod extent;
pub mod health;
pub mod region;

pub use disk::{DiskInventory, DiskMetadata, ManagedDisk, QualificationState};
pub use error::StorageError;
pub use extent::{
    ExtentIndex, ExtentMeta, ExtentStore, ExtentVersion, LocalExtentEntry, RecoveryReport,
    StorageClass,
};
pub use health::{DiskHealthTracker, DiskTelemetry, HealthPolicy};
pub use region::{BadRegion, BadRegionMap};
