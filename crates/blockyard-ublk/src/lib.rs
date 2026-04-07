//! Blockyard UBLK client — block device serving, session management,
//! metadata cache, replicated write pipeline, and erasure-coded write pipeline.
//!
//! # Architecture
//!
//! - [`ublk`] — UBLK device driver integration via `libublk` (P4A.1)
//! - [`session`] — Client session with stable `SessionId` and `OperationId` generation (P4A.2)
//! - [`metadata_cache`] — Cached placement epoch, cluster map, volume metadata (P4A.3)
//! - [`watermark`] — Session write watermark tracking (P4A.4)
//! - [`freshness`] — Metadata freshness checks against watermark (P4A.5)
//! - [`stale_epoch`] — Stale epoch refresh and retry logic (P4A.6)
//! - [`traits`] — `DataNodeClient` and `MetadataClient` traits for testability
//! - [`write_pipeline`] — Replicated write path (P4B.1–P4B.5)
//! - [`ec_write_pipeline`] — Erasure-coded write path (P4D.2, P4D.5, P4D.6)
//! - [`crash_recovery`] — Client crash recovery (P5.1, §6.1)
//! - [`ambiguous_write`] — Ambiguous write resolution (P5.4, §4.9.2)
//! - [`quorum_health`] — Metadata quorum unavailable handling (P5.5, §4.9.1)

pub mod ambiguous_write;
pub mod crash_recovery;
pub mod ec_write_pipeline;
pub mod freshness;
pub mod metadata_cache;
pub mod quorum_health;
pub mod session;
pub mod stale_epoch;
pub mod traits;
pub mod ublk;
pub mod watermark;
pub mod write_pipeline;

pub use ambiguous_write::{AmbiguousWriteOutcome, AmbiguousWriteResolver};
pub use crash_recovery::{CrashRecoveryResolver, RecoveryResult};
pub use ec_write_pipeline::{
    CoalescingBuffer, CoalescingConfig, EcFragmentPlacement, EcWritePipeline, EncodedStripe,
    PendingWrite,
};
pub use freshness::FreshnessChecker;
pub use metadata_cache::MetadataCache;
pub use quorum_health::{QuorumHealthMonitor, QuorumLossReadPolicy, QuorumStatus};
pub use session::ClientSession;
pub use stale_epoch::StaleEpochHandler;
pub use traits::{DataNodeClient, MetadataClient};
pub use ublk::{BlockHandler, IoOperation, IoRequest, UblkDevice};
pub use watermark::WriteWatermark;
pub use write_pipeline::{WriteOutcome, WritePipeline, WriteRequest};
