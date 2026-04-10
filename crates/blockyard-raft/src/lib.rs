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
//! - [`rpc`] — wire-format RPC message types for TCP transport
//! - [`tcp_transport`] — TCP-based `RaftNetworkFactory` for production use
//! - [`server`] — TCP server that dispatches incoming RPCs to the local Raft node
//! - [`service`] — high-level [`MetadataService`] API wrapping Raft

pub mod network;
pub mod persistent_store;
pub mod request;
pub mod response;
pub mod rpc;
pub mod server;
pub mod service;
pub mod state_machine;
pub mod store;
pub mod tcp_transport;
pub mod typ;

pub use network::{NetworkFactory, Router};
pub use persistent_store::{PersistentLogStore, PersistentStateMachineStore};
pub use request::MetadataRequest;
pub use response::MetadataResponse;
pub use rpc::{RaftRpc, RaftRpcResponse};
pub use server::{RaftRpcServer, RaftRpcServerHandle};
pub use service::MetadataService;
pub use state_machine::{
    ClusterDisk, ClusterNode, ExtentMapping, MetadataStateMachineData, VolumeMetadata,
};
pub use store::{LogStore, StateMachineStore};
pub use tcp_transport::{PeerRegistry, TcpNetworkFactory, TcpTransportConfig};
pub use typ::TypeConfig;
