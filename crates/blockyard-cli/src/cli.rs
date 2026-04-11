//! CLI argument definitions using clap derive.

use clap::{Parser, Subcommand, ValueEnum};

use blockyard_common::{DiskId, NodeId, VolumeId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputMode {
    Table,
    Json,
}

#[derive(Debug, Parser)]
#[command(
    name = "byard",
    about = "Blockyard distributed block storage — operator CLI",
    version,
    propagate_version = true
)]
pub struct Cli {
    #[arg(
        long,
        short = 'o',
        global = true,
        default_value = "table",
        help = "Output format"
    )]
    pub output: OutputMode,

    #[arg(
        long,
        global = true,
        default_value = "http://127.0.0.1:9801",
        help = "Cluster endpoint URL"
    )]
    pub endpoint: String,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    #[command(subcommand, about = "Volume management")]
    Volume(VolumeCommand),

    #[command(subcommand, about = "Disk management")]
    Disk(DiskCommand),

    #[command(subcommand, about = "Node management")]
    Node(NodeCommand),

    #[command(about = "Show cluster status")]
    Cluster(ClusterArgs),

    #[command(about = "Mount a volume as a UBLK block device")]
    Mount(MountArgs),

    #[command(about = "Unmount a volume (sends SIGTERM to the mount process)")]
    Unmount(UnmountArgs),
}

#[derive(Debug, Subcommand)]
pub enum VolumeCommand {
    #[command(about = "Create a new volume")]
    Create(VolumeCreateArgs),

    #[command(about = "Delete a volume")]
    Delete(VolumeDeleteArgs),

    #[command(about = "List all volumes")]
    List,

    #[command(about = "Inspect a volume")]
    Inspect(VolumeInspectArgs),
}

#[derive(Debug, clap::Args)]
pub struct VolumeCreateArgs {
    #[arg(help = "Volume name")]
    pub name: String,

    #[arg(long, help = "Volume size (e.g., 10G, 500M, 1T)")]
    pub size: String,

    #[arg(long, default_value = "3", help = "Replication factor")]
    pub replicas: u8,

    #[arg(long, help = "Erasure-coding data chunks (use with --parity)")]
    pub data_chunks: Option<u8>,

    #[arg(long, help = "Erasure-coding parity chunks (use with --data-chunks)")]
    pub parity: Option<u8>,
}

#[derive(Debug, clap::Args)]
pub struct VolumeDeleteArgs {
    #[arg(help = "Volume ID")]
    pub id: VolumeId,

    #[arg(long, help = "Skip confirmation")]
    pub force: bool,
}

#[derive(Debug, clap::Args)]
pub struct VolumeInspectArgs {
    #[arg(help = "Volume ID")]
    pub id: VolumeId,
}

#[derive(Debug, Subcommand)]
pub enum DiskCommand {
    #[command(about = "List all disks")]
    List,

    #[command(about = "Inspect a disk")]
    Inspect(DiskInspectArgs),

    #[command(about = "Drain a disk (migrate data off)")]
    Drain(DiskDrainArgs),

    #[command(about = "Remove a disk from the cluster")]
    Remove(DiskRemoveArgs),
}

#[derive(Debug, clap::Args)]
pub struct DiskInspectArgs {
    #[arg(help = "Disk ID")]
    pub id: DiskId,
}

#[derive(Debug, clap::Args)]
pub struct DiskDrainArgs {
    #[arg(help = "Disk ID")]
    pub id: DiskId,
}

#[derive(Debug, clap::Args)]
pub struct DiskRemoveArgs {
    #[arg(help = "Disk ID")]
    pub id: DiskId,

    #[arg(long, help = "Skip confirmation")]
    pub force: bool,
}

#[derive(Debug, Subcommand)]
pub enum NodeCommand {
    #[command(about = "List all nodes")]
    List,

    #[command(about = "Inspect a node")]
    Inspect(NodeInspectArgs),

    #[command(about = "Decommission a node")]
    Decommission(NodeDecommissionArgs),
}

#[derive(Debug, clap::Args)]
pub struct NodeInspectArgs {
    #[arg(help = "Node ID")]
    pub id: NodeId,
}

#[derive(Debug, clap::Args)]
pub struct NodeDecommissionArgs {
    #[arg(help = "Node ID")]
    pub id: NodeId,

    #[arg(long, help = "Skip confirmation")]
    pub force: bool,
}

#[derive(Debug, clap::Args)]
pub struct ClusterArgs {
    #[command(subcommand)]
    pub subcommand: Option<ClusterCommand>,
}

#[derive(Debug, Subcommand)]
pub enum ClusterCommand {
    #[command(about = "Show cluster status")]
    Status,
}

#[derive(Debug, clap::Args)]
pub struct MountArgs {
    #[arg(help = "Volume ID to mount")]
    pub volume_id: VolumeId,

    #[arg(long, help = "UBLK device path (auto-assigned if omitted)")]
    pub device: Option<String>,
}

#[derive(Debug, clap::Args)]
pub struct UnmountArgs {
    #[arg(help = "Volume ID to unmount")]
    pub volume_id: VolumeId,
}

pub fn parse_size(s: &str) -> anyhow::Result<u64> {
    let s = s.trim();
    if s.is_empty() {
        anyhow::bail!("empty size string");
    }

    let (num_part, suffix) = if s.ends_with(|c: char| c.is_ascii_alphabetic()) {
        let split = s.len()
            - s.chars()
                .rev()
                .take_while(|c| c.is_ascii_alphabetic())
                .count();
        (&s[..split], s[split..].to_uppercase())
    } else {
        (s, String::new())
    };

    let num: f64 = num_part
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid number: {}", num_part))?;

    if num < 0.0 {
        anyhow::bail!("size cannot be negative");
    }

    let multiplier: u64 = match suffix.as_str() {
        "" | "B" => 1,
        "K" | "KB" | "KIB" => 1024,
        "M" | "MB" | "MIB" => 1024 * 1024,
        "G" | "GB" | "GIB" => 1024 * 1024 * 1024,
        "T" | "TB" | "TIB" => 1024 * 1024 * 1024 * 1024,
        other => anyhow::bail!("unknown size suffix: {}", other),
    };

    Ok((num * multiplier as f64) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn test_cli_parse_volume_create() {
        let args = Cli::parse_from([
            "byard",
            "volume",
            "create",
            "my-vol",
            "--size",
            "10G",
            "--replicas",
            "3",
        ]);
        match args.command {
            Command::Volume(VolumeCommand::Create(ref a)) => {
                assert_eq!(a.name, "my-vol");
                assert_eq!(a.size, "10G");
                assert_eq!(a.replicas, 3);
            }
            _ => panic!("expected volume create"),
        }
    }

    #[test]
    fn test_cli_parse_volume_create_ec() {
        let args = Cli::parse_from([
            "byard",
            "volume",
            "create",
            "ec-vol",
            "--size",
            "1T",
            "--data-chunks",
            "4",
            "--parity",
            "2",
        ]);
        match args.command {
            Command::Volume(VolumeCommand::Create(ref a)) => {
                assert_eq!(a.name, "ec-vol");
                assert_eq!(a.data_chunks, Some(4));
                assert_eq!(a.parity, Some(2));
            }
            _ => panic!("expected volume create"),
        }
    }

    #[test]
    fn test_cli_parse_volume_delete() {
        let id = VolumeId::generate();
        let args = Cli::parse_from(["byard", "volume", "delete", &id.to_string(), "--force"]);
        match args.command {
            Command::Volume(VolumeCommand::Delete(ref a)) => {
                assert_eq!(a.id, id);
                assert!(a.force);
            }
            _ => panic!("expected volume delete"),
        }
    }

    #[test]
    fn test_cli_parse_volume_list() {
        let args = Cli::parse_from(["byard", "volume", "list"]);
        assert!(matches!(args.command, Command::Volume(VolumeCommand::List)));
    }

    #[test]
    fn test_cli_parse_volume_inspect() {
        let id = VolumeId::generate();
        let args = Cli::parse_from(["byard", "volume", "inspect", &id.to_string()]);
        match args.command {
            Command::Volume(VolumeCommand::Inspect(ref a)) => {
                assert_eq!(a.id, id);
            }
            _ => panic!("expected volume inspect"),
        }
    }

    #[test]
    fn test_cli_parse_disk_list() {
        let args = Cli::parse_from(["byard", "disk", "list"]);
        assert!(matches!(args.command, Command::Disk(DiskCommand::List)));
    }

    #[test]
    fn test_cli_parse_disk_inspect() {
        let id = DiskId::generate();
        let args = Cli::parse_from(["byard", "disk", "inspect", &id.to_string()]);
        match args.command {
            Command::Disk(DiskCommand::Inspect(ref a)) => {
                assert_eq!(a.id, id);
            }
            _ => panic!("expected disk inspect"),
        }
    }

    #[test]
    fn test_cli_parse_disk_drain() {
        let id = DiskId::generate();
        let args = Cli::parse_from(["byard", "disk", "drain", &id.to_string()]);
        match args.command {
            Command::Disk(DiskCommand::Drain(ref a)) => {
                assert_eq!(a.id, id);
            }
            _ => panic!("expected disk drain"),
        }
    }

    #[test]
    fn test_cli_parse_disk_remove() {
        let id = DiskId::generate();
        let args = Cli::parse_from(["byard", "disk", "remove", &id.to_string(), "--force"]);
        match args.command {
            Command::Disk(DiskCommand::Remove(ref a)) => {
                assert_eq!(a.id, id);
                assert!(a.force);
            }
            _ => panic!("expected disk remove"),
        }
    }

    #[test]
    fn test_cli_parse_node_list() {
        let args = Cli::parse_from(["byard", "node", "list"]);
        assert!(matches!(args.command, Command::Node(NodeCommand::List)));
    }

    #[test]
    fn test_cli_parse_node_inspect() {
        let id = NodeId::generate();
        let args = Cli::parse_from(["byard", "node", "inspect", &id.to_string()]);
        match args.command {
            Command::Node(NodeCommand::Inspect(ref a)) => {
                assert_eq!(a.id, id);
            }
            _ => panic!("expected node inspect"),
        }
    }

    #[test]
    fn test_cli_parse_node_decommission() {
        let id = NodeId::generate();
        let args = Cli::parse_from(["byard", "node", "decommission", &id.to_string(), "--force"]);
        match args.command {
            Command::Node(NodeCommand::Decommission(ref a)) => {
                assert_eq!(a.id, id);
                assert!(a.force);
            }
            _ => panic!("expected node decommission"),
        }
    }

    #[test]
    fn test_cli_parse_cluster_status() {
        let args = Cli::parse_from(["byard", "cluster"]);
        assert!(matches!(args.command, Command::Cluster(_)));
    }

    #[test]
    fn test_cli_parse_cluster_status_subcommand() {
        let args = Cli::parse_from(["byard", "cluster", "status"]);
        match args.command {
            Command::Cluster(ref a) => {
                assert!(matches!(a.subcommand, Some(ClusterCommand::Status)));
            }
            _ => panic!("expected cluster"),
        }
    }

    #[test]
    fn test_cli_parse_mount() {
        let id = VolumeId::generate();
        let args = Cli::parse_from(["byard", "mount", &id.to_string()]);
        match args.command {
            Command::Mount(ref a) => {
                assert_eq!(a.volume_id, id);
                assert!(a.device.is_none());
            }
            _ => panic!("expected mount"),
        }
    }

    #[test]
    fn test_cli_parse_mount_with_device() {
        let id = VolumeId::generate();
        let args = Cli::parse_from(["byard", "mount", &id.to_string(), "--device", "/dev/ublk5"]);
        match args.command {
            Command::Mount(ref a) => {
                assert_eq!(a.volume_id, id);
                assert_eq!(a.device.as_deref(), Some("/dev/ublk5"));
            }
            _ => panic!("expected mount"),
        }
    }

    #[test]
    fn test_cli_parse_unmount() {
        let id = VolumeId::generate();
        let args = Cli::parse_from(["byard", "unmount", &id.to_string()]);
        match args.command {
            Command::Unmount(ref a) => {
                assert_eq!(a.volume_id, id);
            }
            _ => panic!("expected unmount"),
        }
    }

    #[test]
    fn test_cli_output_json() {
        let args = Cli::parse_from(["byard", "-o", "json", "volume", "list"]);
        assert_eq!(args.output, OutputMode::Json);
    }

    #[test]
    fn test_cli_output_table() {
        let args = Cli::parse_from(["byard", "-o", "table", "volume", "list"]);
        assert_eq!(args.output, OutputMode::Table);
    }

    #[test]
    fn test_cli_default_output() {
        let args = Cli::parse_from(["byard", "volume", "list"]);
        assert_eq!(args.output, OutputMode::Table);
    }

    #[test]
    fn test_cli_default_endpoint() {
        let args = Cli::parse_from(["byard", "volume", "list"]);
        assert_eq!(args.endpoint, "http://127.0.0.1:9801");
    }

    #[test]
    fn test_cli_custom_endpoint() {
        let args = Cli::parse_from([
            "byard",
            "--endpoint",
            "http://10.0.0.1:9801",
            "volume",
            "list",
        ]);
        assert_eq!(args.endpoint, "http://10.0.0.1:9801");
    }

    #[test]
    fn test_cli_verify_app() {
        Cli::command().debug_assert();
    }

    #[test]
    fn test_parse_size_bytes() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
        assert_eq!(parse_size("0").unwrap(), 0);
    }

    #[test]
    fn test_parse_size_bytes_suffix() {
        assert_eq!(parse_size("512B").unwrap(), 512);
    }

    #[test]
    fn test_parse_size_kib() {
        assert_eq!(parse_size("1K").unwrap(), 1024);
        assert_eq!(parse_size("1KB").unwrap(), 1024);
        assert_eq!(parse_size("1KiB").unwrap(), 1024);
    }

    #[test]
    fn test_parse_size_mib() {
        assert_eq!(parse_size("1M").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("1MB").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("1MiB").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("500M").unwrap(), 500 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_gib() {
        assert_eq!(parse_size("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("1GiB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("10G").unwrap(), 10 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_tib() {
        assert_eq!(parse_size("1T").unwrap(), 1024u64 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("1TB").unwrap(), 1024u64 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("1TiB").unwrap(), 1024u64 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_fractional() {
        assert_eq!(
            parse_size("1.5G").unwrap(),
            (1.5 * 1024.0 * 1024.0 * 1024.0) as u64
        );
    }

    #[test]
    fn test_parse_size_whitespace() {
        assert_eq!(parse_size("  1G  ").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_empty() {
        assert!(parse_size("").is_err());
    }

    #[test]
    fn test_parse_size_invalid_number() {
        assert!(parse_size("abc").is_err());
    }

    #[test]
    fn test_parse_size_unknown_suffix() {
        assert!(parse_size("1X").is_err());
    }

    #[test]
    fn test_parse_size_negative() {
        assert!(parse_size("-1G").is_err());
    }

    #[test]
    fn test_parse_size_case_insensitive() {
        assert_eq!(parse_size("1g").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("1m").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("1t").unwrap(), 1024u64 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_output_mode_debug() {
        assert_eq!(format!("{:?}", OutputMode::Table), "Table");
        assert_eq!(format!("{:?}", OutputMode::Json), "Json");
    }

    #[test]
    fn test_volume_create_args_default_replicas() {
        let args = Cli::parse_from(["byard", "volume", "create", "vol1", "--size", "1G"]);
        match args.command {
            Command::Volume(VolumeCommand::Create(ref a)) => {
                assert_eq!(a.replicas, 3);
                assert!(a.data_chunks.is_none());
                assert!(a.parity.is_none());
            }
            _ => panic!("expected volume create"),
        }
    }

    #[test]
    fn test_volume_delete_no_force() {
        let id = VolumeId::generate();
        let args = Cli::parse_from(["byard", "volume", "delete", &id.to_string()]);
        match args.command {
            Command::Volume(VolumeCommand::Delete(ref a)) => {
                assert!(!a.force);
            }
            _ => panic!("expected volume delete"),
        }
    }

    #[test]
    fn test_disk_remove_no_force() {
        let id = DiskId::generate();
        let args = Cli::parse_from(["byard", "disk", "remove", &id.to_string()]);
        match args.command {
            Command::Disk(DiskCommand::Remove(ref a)) => {
                assert!(!a.force);
            }
            _ => panic!("expected disk remove"),
        }
    }

    #[test]
    fn test_node_decommission_no_force() {
        let id = NodeId::generate();
        let args = Cli::parse_from(["byard", "node", "decommission", &id.to_string()]);
        match args.command {
            Command::Node(NodeCommand::Decommission(ref a)) => {
                assert!(!a.force);
            }
            _ => panic!("expected node decommission"),
        }
    }
}
