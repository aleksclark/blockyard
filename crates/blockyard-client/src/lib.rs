//! Blockyard client read path.
//!
//! Implements the client-side read pipeline for the Blockyard distributed
//! block storage system (§4.6, P4C.1–P4C.4).
//!
//! # Architecture
//!
//! The read pipeline is generic over four trait dependencies:
//!
//! - [`MetadataProvider`] — extent mapping lookup and session watermark
//! - [`DataNodeReader`] — reading extent data from data nodes
//! - [`HealthReporter`] — corruption and failure reporting
//! - [`ReplicaSelector`] — replica source selection with latency tracking
//!
//! # Read-Your-Own-Writes
//!
//! The pipeline enforces read-your-own-writes by checking the session write
//! watermark before serving any read. If the cached extent mapping is older
//! than the watermark, metadata is refreshed before proceeding (§4.4, §4.6).
//!
//! # Replica Fallback
//!
//! On source failure or checksum mismatch, the pipeline tries the next
//! replica. Corruption is reported to the health subsystem and the source
//! is marked as suspect (§4.9.4).

pub mod error;
pub mod pipeline;
pub mod selector;
#[cfg(test)]
pub(crate) mod testutil;
pub mod traits;
pub mod types;

pub use error::ReadError;
pub use pipeline::ReadPipeline;
pub use selector::LatencyAwareSelector;
pub use traits::{DataNodeReader, HealthReporter, MetadataProvider, ReplicaSelector};
pub use types::{
    CorruptionReport, DataNodeReadResult, ExtentMapping, ReadFailureReport, ReadRequest,
    ReadResult, ReplicaHealth, ReplicaLocation, ReplicaStats,
};
