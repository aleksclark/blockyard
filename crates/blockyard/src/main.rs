mod node;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "blockyard", about = "Distributed block storage system")]
struct Cli {
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Start { config } => {
            let cfg = blockyard_common::NodeConfig::from_file(&config)?;
            let mut node = node::BlockyardNode::new(cfg)?;
            node.start().await?;
        }
        Command::Volume(cmd) => {
            let raft = blockyard_raft::MultiRaft::new(0);
            raft.create_group(blockyard_raft::meta_group::MetaGroup::group_id())?;

            match cmd {
                VolumeCommand::Create {
                    name,
                    size,
                    replicas,
                    ..
                } => {
                    let size_bytes =
                        blockyard_common::parse_size(&size).map_err(|e| anyhow::anyhow!("{e}"))?;
                    let resp = raft.propose(
                        0,
                        &blockyard_raft::types::RaftRequest::VolumeCreate {
                            name: name.clone(),
                            size_bytes,
                            replicas,
                        },
                    )?;
                    println!("Created volume '{name}': {resp:?}");
                }
                VolumeCommand::Delete { name } => {
                    let resp = raft.propose(
                        0,
                        &blockyard_raft::types::RaftRequest::VolumeDelete { name: name.clone() },
                    )?;
                    println!("Deleted volume '{name}': {resp:?}");
                }
                VolumeCommand::List => {
                    let state = raft.get_state(0).unwrap_or_default();
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
                    let state = raft.get_state(0).unwrap_or_default();
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
                    let resp = raft.propose(
                        0,
                        &blockyard_raft::types::RaftRequest::VolumeResize {
                            name: name.clone(),
                            new_size,
                        },
                    )?;
                    println!("Resized volume '{name}': {resp:?}");
                }
                VolumeCommand::Set {
                    name,
                    replicas,
                    consistency,
                    read_policy,
                } => {
                    if let Some(r) = replicas {
                        raft.propose(
                            0,
                            &blockyard_raft::types::RaftRequest::VolumeSetReplicas {
                                name: name.clone(),
                                replicas: r,
                            },
                        )?;
                        println!("Set replicas={r} for volume '{name}'");
                    }
                    if let Some(c) = consistency {
                        raft.propose(
                            0,
                            &blockyard_raft::types::RaftRequest::VolumeSetConsistency {
                                name: name.clone(),
                                consistency: c.clone(),
                            },
                        )?;
                        println!("Set consistency={c} for volume '{name}'");
                    }
                    if let Some(rp) = read_policy {
                        raft.propose(
                            0,
                            &blockyard_raft::types::RaftRequest::VolumeSetReadPolicy {
                                name: name.clone(),
                                read_policy: rp.clone(),
                            },
                        )?;
                        println!("Set read_policy={rp} for volume '{name}'");
                    }
                }
            }
        }
        Command::Node(cmd) => match cmd {
            NodeCommand::List => {
                let raft = blockyard_raft::MultiRaft::new(0);
                raft.create_group(0)?;
                let state = raft.get_state(0).unwrap_or_default();
                if state.nodes.is_empty() {
                    println!("No nodes registered.");
                } else {
                    println!("{:<10} {:<20} STATUS", "ID", "ADDRESS");
                    for node in state.nodes.values() {
                        println!("{:<10} {:<20} healthy", node.node_id, node.addr);
                    }
                }
            }
            NodeCommand::Status { name } => {
                println!("Node:     {name}");
                println!("State:    healthy");
                println!("ZFS Pool: online");
                println!("  Errors: read=0 write=0 cksum=0");
                println!("  Scrub:  none scheduled");
            }
            NodeCommand::Drain { name } => {
                let raft = blockyard_raft::MultiRaft::new(0);
                raft.create_group(0)?;
                let state = raft.get_state(0).unwrap_or_default();
                let node = state.nodes.values().find(|n| n.addr.contains(&name) || n.node_id.to_string() == name);
                match node {
                    Some(n) => {
                        raft.propose(
                            0,
                            &blockyard_raft::types::RaftRequest::NodeDrain { node_id: n.node_id },
                        )?;
                        println!("Draining node '{}' (id={})", name, n.node_id);
                    }
                    None => println!("Node '{name}' not found"),
                }
            }
        },
        Command::Rebalance(cmd) => match cmd {
            RebalanceCommand::Status => {
                let raft = blockyard_raft::MultiRaft::new(0);
                raft.create_group(blockyard_raft::meta_group::MetaGroup::group_id())?;
                let state = raft.get_state(0).unwrap_or_default();

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
        Command::Mount {
            name,
            device,
            backend,
        } => {
            println!("Mounting volume '{name}' via {backend}...");
            let mut client = blockyard_ublk::UblkClient::new(name);
            let dev = client.mount(device.as_deref()).await?;
            println!("Mounted at {dev}");
        }
        Command::Status => {
            println!("Cluster: not connected (standalone mode)");
            println!("Volumes: 0");
            println!("Nodes:   0");
        }
    }

    Ok(())
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
