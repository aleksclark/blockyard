//! Blockyard UBLK client — block device serving, session management,
//! metadata cache, and replicated write pipeline.
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

pub mod freshness;
pub mod metadata_cache;
pub mod session;
pub mod stale_epoch;
pub mod traits;
pub mod ublk;
pub mod watermark;
pub mod write_pipeline;

pub use freshness::FreshnessChecker;
pub use metadata_cache::MetadataCache;
pub use session::ClientSession;
pub use stale_epoch::StaleEpochHandler;
pub use traits::{DataNodeClient, MetadataClient};
pub use ublk::{BlockHandler, IoOperation, IoRequest, UblkDevice};
pub use watermark::WriteWatermark;
pub use write_pipeline::{WriteOutcome, WritePipeline, WriteRequest};
