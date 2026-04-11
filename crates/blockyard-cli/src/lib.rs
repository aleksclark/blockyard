//! Blockyard CLI library (byard) — operator tools for cluster management.
//!
//! Provides volume, disk, node, cluster, and mount/unmount commands
//! with table and JSON output modes.

pub mod cli;
pub mod client;
pub mod commands;
pub mod http_client;
pub mod mount;
pub mod output;
pub mod types;
