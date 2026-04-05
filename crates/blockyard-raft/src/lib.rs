pub mod grpc_server;
pub mod heartbeat;
pub mod meta_group;
pub mod multiraft;
pub mod network;
pub mod state_machine;
pub mod types;
pub mod volume_group;

/// Generated protobuf / gRPC types from `proto/blockyard.proto`.
pub mod proto {
    tonic::include_proto!("blockyard");
}

pub use heartbeat::HeartbeatConsolidator;
pub use multiraft::MultiRaft;
pub use network::RaftNetwork;
