//! Blockyard multi-raft consensus engine.
//!
//! Manages Raft-based metadata replication, providing a strongly consistent
//! state machine for cluster membership, placement maps, volume metadata,
//! extent mappings, and protection policies.
//!
//! # Architecture
//!
//! - [`typ`] — openraft type configuration (`TypeConfig`)
//! - [`request`] / [`response`] — state machine request and response types
//! - [`state_machine`] — deterministic state transition logic
//! - [`store`] — `RaftLogStorage` + `RaftStateMachine` implementations
//! - [`network`] — Raft RPC transport (in-memory router for tests)
//! - [`service`] — high-level [`MetadataService`] API wrapping Raft

pub mod network;
pub mod request;
pub mod response;
pub mod service;
pub mod state_machine;
pub mod store;
pub mod typ;

pub use network::{NetworkFactory, Router};
pub use request::MetadataRequest;
pub use response::MetadataResponse;
pub use service::MetadataService;
pub use state_machine::{ClusterNode, ExtentMapping, MetadataStateMachineData, VolumeMetadata};
pub use store::{LogStore, StateMachineStore};
pub use typ::TypeConfig;
