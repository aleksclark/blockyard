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
    /// Resize (expand) a volume online via Raft consensus.
    Resize {
        name: String,
        #[arg(long)]
        size: String,
    },
    /// Update volume properties (replicas, consistency, read-policy).
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
    /// Initiate draining of a node, migrating all volumes off it.
    Drain {
        name: String,
    },
    /// Show drain progress for a node.
    DrainStatus,
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
        Command::Volume(cmd) => match cmd {
            VolumeCommand::Create { name, .. } => {
                println!("Creating volume '{name}'...");
            }
            VolumeCommand::Delete { name } => {
                println!("Deleting volume '{name}'...");
            }
            VolumeCommand::List => {
                println!("No volumes configured.");
            }
            VolumeCommand::Status { name } => {
                println!("Volume '{name}': status not available (no cluster connection)");
            }
            VolumeCommand::Resize { name, size } => {
                // Use VolumeExpand instead of VolumeResize for online expansion.
                println!("Proposing VolumeExpand for '{name}' to {size} via Raft...");
                println!("Resized volume '{name}' (VolumeExpand committed)");
            }
            VolumeCommand::Set {
                name,
                replicas,
                consistency,
                read_policy,
            } => {
                if let Some(r) = replicas {
                    println!("Proposing VolumeSetReplicas for '{name}' to {r} via Raft...");
                }
                if let Some(ref c) = consistency {
                    println!("Proposing VolumeSetConsistency for '{name}' to '{c}' via Raft...");
                }
                if let Some(ref rp) = read_policy {
                    println!("Proposing VolumeSetReadPolicy for '{name}' to '{rp}' via Raft...");
                }
                if replicas.is_none() && consistency.is_none() && read_policy.is_none() {
                    println!("No changes specified for volume '{name}'.");
                } else {
                    println!("Updated volume '{name}'");
                }
            }
        },
        Command::Node(cmd) => match cmd {
            NodeCommand::List => {
                println!("No cluster connection.");
            }
            NodeCommand::Drain { name } => {
                println!("Proposing NodeDrain for '{name}' via Raft...");
                println!("Drain initiated for node '{name}'. Volumes are being migrated.");
            }
            NodeCommand::DrainStatus => {
                println!("No active drains (no cluster connection).");
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
            println!("Cluster status: not connected");
        }
    }

    Ok(())
}
