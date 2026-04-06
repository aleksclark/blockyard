mod node;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

use blockyard_raft::network::RaftNetwork;
use blockyard_raft::proto;
use blockyard_raft::state_machine::AppState;
use blockyard_raft::types::RaftRequest;

/// Timeout applied to every CLI → cluster gRPC call.
const GRPC_TIMEOUT: Duration = Duration::from_secs(5);

/// Meta-group Raft group ID (group 0 stores cluster-wide metadata).
const META_GROUP: u64 = 0;

#[derive(Parser)]
#[command(name = "blockyard", about = "Distributed block storage system")]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:7401", global = true)]
    endpoint: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Start {
        #[arg(short, long, default_value = "/etc/blockyard/config.toml")]
        config: PathBuf,
    },
    #[command(subcommand)]
    Volume(VolumeCommand),
    #[command(subcommand)]
    Node(NodeCommand),
    #[command(subcommand)]
    Rebalance(RebalanceCommand),
    Mount {
        name: String,
        #[arg(long)]
        device: Option<String>,
        #[arg(long, default_value = "ublk")]
        backend: String,
    },
    Status,
}

#[derive(Subcommand)]
enum VolumeCommand {
    Create {
        #[arg(long)]
        name: String,
        #[arg(long)]
        size: String,
        #[arg(long, default_value = "3")]
        replicas: u32,
        #[arg(long, default_value = "majority")]
        consistency: String,
        #[arg(long)]
        affinity: Option<String>,
        #[arg(long, default_value = "node")]
        failure_domain: String,
        /// Erasure coding configuration in "k+m" format (e.g., "4+2" for
        /// RS(4,2)).  When set, the volume uses erasure coding instead of
        /// replication.
        #[arg(long)]
        erasure_coding: Option<String>,
    },
    Delete {
        name: String,
    },
    List,
    Status {
        name: String,
    },
    Resize {
        name: String,
        #[arg(long)]
        size: String,
    },
    Set {
        name: String,
        #[arg(long)]
        replicas: Option<u32>,
        #[arg(long)]
        consistency: Option<String>,
        #[arg(long)]
        read_policy: Option<String>,
    },
    Snapshot {
        name: String,
        #[arg(long)]
        snap: String,
    },
    Snapshots {
        name: String,
    },
    SnapshotDelete {
        name: String,
        #[arg(long)]
        snap: String,
    },
}

#[derive(Subcommand)]
enum NodeCommand {
    List,
    Status { name: String },
    Drain { name: String },
}

#[derive(Subcommand)]
enum RebalanceCommand {
    /// Show active and recent rebalance moves with progress
    Status,
}

// ---------------------------------------------------------------------------
// gRPC helpers
// ---------------------------------------------------------------------------

/// Build a [`RaftNetwork`] that points at the given cluster endpoint.
async fn connect_to_cluster(endpoint: &str) -> anyhow::Result<RaftNetwork> {
    let network = RaftNetwork::new();
    // RaftNetwork.add_peer expects the full address with http:// scheme.
    let addr = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };
    network.add_peer(0, addr);
    Ok(network)
}

/// Send a mutation proposal to the meta-group via gRPC and return the
/// response payload (if any).
async fn propose(
    network: &RaftNetwork,
    endpoint: &str,
    request: &RaftRequest,
) -> anyhow::Result<proto::ForwardProposalResponse> {
    let payload = serde_json::to_vec(request)?;
    let req = proto::ForwardProposalRequest {
        group_id: META_GROUP,
        payload,
    };

    let resp = tokio::time::timeout(GRPC_TIMEOUT, network.send_forward_proposal(0, req))
        .await
        .map_err(|_| anyhow::anyhow!("Error: cannot connect to cluster at {endpoint}"))?
        .map_err(|e| anyhow::anyhow!("Error: cannot connect to cluster at {endpoint}: {e}"))?;

    if !resp.success && !resp.error.is_empty() {
        anyhow::bail!("{}", resp.error);
    }
    Ok(resp)
}

/// Query the committed state of the meta-group via gRPC.
async fn get_state(network: &RaftNetwork, endpoint: &str) -> anyhow::Result<AppState> {
    let req = proto::GetStateRequest {
        group_id: META_GROUP,
    };

    let resp = tokio::time::timeout(GRPC_TIMEOUT, network.send_get_state(0, req))
        .await
        .map_err(|_| anyhow::anyhow!("Error: cannot connect to cluster at {endpoint}"))?
        .map_err(|e| anyhow::anyhow!("Error: cannot connect to cluster at {endpoint}: {e}"))?;

    if !resp.success {
        anyhow::bail!("{}", resp.error);
    }
    let state: AppState = serde_json::from_slice(&resp.state)?;
    Ok(state)
}

// ---------------------------------------------------------------------------
// Entrypoint
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        // ── Start (unchanged) ─────────────────────────────────────────
        Command::Start { config } => {
            let cfg = blockyard_common::NodeConfig::from_file(&config)?;
            let mut node = node::BlockyardNode::new(cfg)?;
            node.start().await?;
        }

        // ── Volume commands ───────────────────────────────────────────
        Command::Volume(cmd) => {
            let network = connect_to_cluster(&cli.endpoint).await?;

            match cmd {
                VolumeCommand::Create {
                    name,
                    size,
                    replicas,
                    erasure_coding,
                    ..
                } => {
                    let size_bytes =
                        blockyard_common::parse_size(&size).map_err(|e| anyhow::anyhow!("{e}"))?;

                    if let Some(ref ec) = erasure_coding {
                        let (k, m) = parse_ec_spec(ec)?;
                        let req = RaftRequest::VolumeCreateEc {
                            name: name.clone(),
                            size_bytes,
                            data_shards: k,
                            parity_shards: m,
                        };
                        propose(&network, &cli.endpoint, &req).await?;
                        println!("Created EC({k}+{m}) volume '{name}'");
                    } else {
                        let req = RaftRequest::VolumeCreate {
                            name: name.clone(),
                            size_bytes,
                            replicas,
                        };
                        propose(&network, &cli.endpoint, &req).await?;
                        println!("Created volume '{name}'");
                    }
                }
                VolumeCommand::Delete { name } => {
                    let req = RaftRequest::VolumeDelete { name: name.clone() };
                    propose(&network, &cli.endpoint, &req).await?;
                    println!("Deleted volume '{name}'");
                }
                VolumeCommand::List => {
                    let state = get_state(&network, &cli.endpoint).await?;
                    if state.volumes.is_empty() {
                        println!("No volumes.");
                    } else {
                        println!("{:<20} {:>10} {:>8} NODES", "NAME", "SIZE", "REPLICAS");
                        for vol in state.volumes.values() {
                            let size_str = format_size(vol.size_bytes);
                            let nodes_str = if vol.placement.is_empty() {
                                "unplaced".to_string()
                            } else {
                                vol.placement
                                    .iter()
                                    .map(|n| n.to_string())
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            };
                            println!(
                                "{:<20} {:>10} {:>8} {}",
                                vol.name, size_str, vol.replicas, nodes_str
                            );
                        }
                    }
                }
                VolumeCommand::Status { name } => {
                    let state = get_state(&network, &cli.endpoint).await?;
                    match state.volumes.get(&name) {
                        Some(vol) => {
                            println!("Volume:   {}", vol.name);
                            println!("Size:     {}", format_size(vol.size_bytes));
                            println!("Replicas: {}", vol.replicas);
                            println!(
                                "Nodes:    {}",
                                if vol.placement.is_empty() {
                                    "unplaced".to_string()
                                } else {
                                    vol.placement
                                        .iter()
                                        .map(|n| n.to_string())
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                }
                            );
                        }
                        None => println!("Volume '{name}' not found"),
                    }
                }
                VolumeCommand::Resize { name, size } => {
                    let new_size =
                        blockyard_common::parse_size(&size).map_err(|e| anyhow::anyhow!("{e}"))?;
                    let req = RaftRequest::VolumeResize {
                        name: name.clone(),
                        new_size,
                    };
                    propose(&network, &cli.endpoint, &req).await?;
                    println!("Resized volume '{name}'");
                }
                VolumeCommand::Set {
                    name,
                    replicas,
                    consistency,
                    read_policy,
                } => {
                    if let Some(r) = replicas {
                        let req = RaftRequest::VolumeSetReplicas {
                            name: name.clone(),
                            replicas: r,
                        };
                        propose(&network, &cli.endpoint, &req).await?;
                        println!("Set replicas={r} for volume '{name}'");
                    }
                    if let Some(c) = consistency {
                        let req = RaftRequest::VolumeSetConsistency {
                            name: name.clone(),
                            consistency: c.clone(),
                        };
                        propose(&network, &cli.endpoint, &req).await?;
                        println!("Set consistency={c} for volume '{name}'");
                    }
                    if let Some(rp) = read_policy {
                        let req = RaftRequest::VolumeSetReadPolicy {
                            name: name.clone(),
                            read_policy: rp.clone(),
                        };
                        propose(&network, &cli.endpoint, &req).await?;
                        println!("Set read_policy={rp} for volume '{name}'");
                    }
                }
                VolumeCommand::Snapshot { name, snap } => {
                    let req = RaftRequest::VolumeSnapshot {
                        name: name.clone(),
                        snap_name: snap.clone(),
                    };
                    propose(&network, &cli.endpoint, &req).await?;
                    println!("Created snapshot '{snap}' of volume '{name}'");
                }
                VolumeCommand::Snapshots { name } => {
                    let req = RaftRequest::VolumeSnapshotList { name: name.clone() };
                    let resp = propose(&network, &cli.endpoint, &req).await?;
                    let snaps: Vec<String> = serde_json::from_slice(&resp.data).unwrap_or_default();
                    if snaps.is_empty() {
                        println!("No snapshots for volume '{name}'.");
                    } else {
                        println!("Snapshots for volume '{name}':");
                        for s in &snaps {
                            println!("  {s}");
                        }
                    }
                }
                VolumeCommand::SnapshotDelete { name, snap } => {
                    let req = RaftRequest::VolumeSnapshotDelete {
                        name: name.clone(),
                        snap_name: snap.clone(),
                    };
                    propose(&network, &cli.endpoint, &req).await?;
                    println!("Deleted snapshot '{snap}' of volume '{name}'");
                }
            }
        }

        // ── Node commands ─────────────────────────────────────────────
        Command::Node(cmd) => {
            let network = connect_to_cluster(&cli.endpoint).await?;

            match cmd {
                NodeCommand::List => {
                    let state = get_state(&network, &cli.endpoint).await?;
                    if state.nodes.is_empty() {
                        println!("No nodes registered.");
                    } else {
                        println!("{:<10} {:<20} STATUS", "ID", "ADDRESS");
                        for node in state.nodes.values() {
                            println!(
                                "{:<10} {:<20} {}",
                                node.node_id, node.addr, node.drain_state
                            );
                        }
                    }
                }
                NodeCommand::Status { name } => {
                    let state = get_state(&network, &cli.endpoint).await?;
                    let node = state
                        .nodes
                        .values()
                        .find(|n| n.addr.contains(&name) || n.node_id.to_string() == name);
                    match node {
                        Some(n) => {
                            println!("Node:   {}", n.node_id);
                            println!("Addr:   {}", n.addr);
                            println!("State:  {}", n.drain_state);
                        }
                        None => println!("Node '{name}' not found"),
                    }
                }
                NodeCommand::Drain { name } => {
                    let state = get_state(&network, &cli.endpoint).await?;
                    let node = state
                        .nodes
                        .values()
                        .find(|n| n.addr.contains(&name) || n.node_id.to_string() == name);
                    match node {
                        Some(n) => {
                            let req = RaftRequest::NodeDrain { node_id: n.node_id };
                            propose(&network, &cli.endpoint, &req).await?;
                            println!("Draining node '{}' (id={})", name, n.node_id);
                        }
                        None => println!("Node '{name}' not found"),
                    }
                }
            }
        }

        // ── Rebalance commands ────────────────────────────────────────
        Command::Rebalance(cmd) => match cmd {
            RebalanceCommand::Status => {
                let network = connect_to_cluster(&cli.endpoint).await?;
                let state = get_state(&network, &cli.endpoint).await?;

                let rebalancing: Vec<_> = state
                    .volumes
                    .values()
                    .filter(|v| v.rebalance_state.is_some())
                    .collect();

                if rebalancing.is_empty() {
                    println!("No active rebalance operations.");
                } else {
                    println!(
                        "{:<20} {:>8} {:>8} {:>10}",
                        "VOLUME", "SOURCE", "TARGET", "PHASE"
                    );
                    for vol in rebalancing {
                        if let Some(ref rs) = vol.rebalance_state {
                            println!(
                                "{:<20} {:>8} {:>8} {:>10}",
                                vol.name, rs.source, rs.target, rs.phase
                            );
                        }
                    }
                }
            }
        },

        // ── Mount ─────────────────────────────────────────────────────
        Command::Mount {
            name,
            device,
            backend,
        } => {
            let be: blockyard_ublk::client::MountBackend = backend
                .parse()
                .map_err(|e: String| anyhow::anyhow!("{e}"))?;
            println!("Mounting volume '{name}' via {be}...");
            let mut client = blockyard_ublk::UblkClient::new(name).with_backend(be);
            let dev = client.mount(device.as_deref()).await?;
            println!("Mounted at {dev}");
            println!("Press Ctrl-C to unmount");
            tokio::signal::ctrl_c().await?;
            println!("Unmounting...");
            client.unmount().await?;
        }

        // ── Status ────────────────────────────────────────────────────
        Command::Status => {
            let network = connect_to_cluster(&cli.endpoint).await?;
            match get_state(&network, &cli.endpoint).await {
                Ok(state) => {
                    println!("Cluster: connected to {}", cli.endpoint);
                    println!("Nodes:   {}", state.nodes.len());
                    println!("Volumes: {}", state.volumes.len());
                }
                Err(e) => {
                    println!("Error: cannot connect to cluster at {}: {e}", cli.endpoint);
                }
            }
        }
    }

    Ok(())
}

/// Parse an erasure-coding spec like "4+2" into (k, m) as (u32, u32).
fn parse_ec_spec(spec: &str) -> anyhow::Result<(u32, u32)> {
    let parts: Vec<&str> = spec.split('+').collect();
    if parts.len() != 2 {
        anyhow::bail!(
            "invalid erasure-coding spec '{spec}': expected format 'k+m' (e.g., '4+2')"
        );
    }
    let k: u32 = parts[0]
        .trim()
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid data shards in '{spec}': {e}"))?;
    let m: u32 = parts[1]
        .trim()
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid parity shards in '{spec}': {e}"))?;
    if k < 1 || m < 1 {
        anyhow::bail!("data shards and parity shards must both be >= 1, got {k}+{m}");
    }
    Ok((k, m))
}

fn format_size(bytes: u64) -> String {
    const TB: u64 = 1024 * 1024 * 1024 * 1024;
    const GB: u64 = 1024 * 1024 * 1024;
    const MB: u64 = 1024 * 1024;
    const KB: u64 = 1024;

    if bytes >= TB {
        format!("{:.1}TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.1}GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1}MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1}KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes}B")
    }
}
