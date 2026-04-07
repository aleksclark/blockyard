//! Command execution — dispatches parsed CLI args to the client.

use anyhow::Result;

use blockyard_common::ProtectionPolicy;

use crate::cli::{
    Cli, ClusterCommand, Command, DiskCommand, NodeCommand, OutputMode, VolumeCommand,
};
use crate::client::BlockyardClient;
use crate::output::{
    self, OutputFormat, format_cluster_status, format_disk_detail, format_disk_list,
    format_mount_info, format_node_detail, format_node_list, format_volume_detail,
    format_volume_list,
};
use crate::types::VolumeCreateParams;

fn to_output_format(mode: OutputMode) -> OutputFormat {
    match mode {
        OutputMode::Table => OutputFormat::Table,
        OutputMode::Json => OutputFormat::Json,
    }
}

pub async fn execute(cli: &Cli, client: &impl BlockyardClient) -> Result<String> {
    let fmt = to_output_format(cli.output);

    match &cli.command {
        Command::Volume(cmd) => execute_volume(cmd, client, fmt).await,
        Command::Disk(cmd) => execute_disk(cmd, client, fmt).await,
        Command::Node(cmd) => execute_node(cmd, client, fmt).await,
        Command::Cluster(args) => execute_cluster(&args.subcommand, client, fmt).await,
        Command::Mount(args) => execute_mount(args, client, fmt).await,
        Command::Unmount(args) => execute_unmount(args, client, fmt).await,
    }
}

async fn execute_volume(
    cmd: &VolumeCommand,
    client: &impl BlockyardClient,
    fmt: OutputFormat,
) -> Result<String> {
    match cmd {
        VolumeCommand::Create(args) => {
            let size_bytes = crate::cli::parse_size(&args.size)?;

            let protection = if let (Some(data), Some(parity)) = (args.data_chunks, args.parity) {
                ProtectionPolicy::ErasureCoded {
                    data_chunks: data,
                    parity_chunks: parity,
                }
            } else {
                ProtectionPolicy::Replicated {
                    replicas: args.replicas,
                }
            };
            protection.validate()?;

            let params = VolumeCreateParams {
                name: args.name.clone(),
                size_bytes,
                protection,
            };
            let vol = client.volume_create(params).await?;
            format_volume_detail(&vol, fmt)
        }
        VolumeCommand::Delete(args) => {
            client.volume_delete(args.id).await?;
            match fmt {
                OutputFormat::Json => {
                    output::print_json(&serde_json::json!({"deleted": args.id.to_string()}))
                }
                OutputFormat::Table => Ok(format!("Volume {} deleted.", args.id)),
            }
        }
        VolumeCommand::List => {
            let volumes = client.volume_list().await?;
            format_volume_list(&volumes, fmt)
        }
        VolumeCommand::Inspect(args) => {
            let vol = client.volume_inspect(args.id).await?;
            format_volume_detail(&vol, fmt)
        }
    }
}

async fn execute_disk(
    cmd: &DiskCommand,
    client: &impl BlockyardClient,
    fmt: OutputFormat,
) -> Result<String> {
    match cmd {
        DiskCommand::List => {
            let disks = client.disk_list().await?;
            format_disk_list(&disks, fmt)
        }
        DiskCommand::Inspect(args) => {
            let disk = client.disk_inspect(args.id).await?;
            format_disk_detail(&disk, fmt)
        }
        DiskCommand::Drain(args) => {
            client.disk_drain(args.id).await?;
            match fmt {
                OutputFormat::Json => {
                    output::print_json(&serde_json::json!({"draining": args.id.to_string()}))
                }
                OutputFormat::Table => Ok(format!("Disk {} drain initiated.", args.id)),
            }
        }
        DiskCommand::Remove(args) => {
            client.disk_remove(args.id).await?;
            match fmt {
                OutputFormat::Json => {
                    output::print_json(&serde_json::json!({"removed": args.id.to_string()}))
                }
                OutputFormat::Table => Ok(format!("Disk {} removed.", args.id)),
            }
        }
    }
}

async fn execute_node(
    cmd: &NodeCommand,
    client: &impl BlockyardClient,
    fmt: OutputFormat,
) -> Result<String> {
    match cmd {
        NodeCommand::List => {
            let nodes = client.node_list().await?;
            format_node_list(&nodes, fmt)
        }
        NodeCommand::Inspect(args) => {
            let node = client.node_inspect(args.id).await?;
            format_node_detail(&node, fmt)
        }
        NodeCommand::Decommission(args) => {
            client.node_decommission(args.id).await?;
            match fmt {
                OutputFormat::Json => {
                    output::print_json(&serde_json::json!({"decommissioning": args.id.to_string()}))
                }
                OutputFormat::Table => Ok(format!("Node {} decommission initiated.", args.id)),
            }
        }
    }
}

async fn execute_cluster(
    subcmd: &Option<ClusterCommand>,
    client: &impl BlockyardClient,
    fmt: OutputFormat,
) -> Result<String> {
    match subcmd {
        None | Some(ClusterCommand::Status) => {
            let status = client.cluster_status().await?;
            format_cluster_status(&status, fmt)
        }
    }
}

async fn execute_mount(
    args: &crate::cli::MountArgs,
    client: &impl BlockyardClient,
    fmt: OutputFormat,
) -> Result<String> {
    let info = client.mount(args.volume_id, args.device.clone()).await?;
    format_mount_info(&info, fmt)
}

async fn execute_unmount(
    args: &crate::cli::UnmountArgs,
    client: &impl BlockyardClient,
    fmt: OutputFormat,
) -> Result<String> {
    client.unmount(args.volume_id).await?;
    match fmt {
        OutputFormat::Json => {
            output::print_json(&serde_json::json!({"unmounted": args.volume_id.to_string()}))
        }
        OutputFormat::Table => Ok(format!("Volume {} unmounted.", args.volume_id)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::Cli;
    use crate::client::mock::MockClient;
    use clap::Parser;

    async fn run(args: &[&str]) -> Result<String> {
        let client = MockClient::with_sample_data();
        let cli = Cli::parse_from(args);
        execute(&cli, &client).await
    }

    async fn run_with_client(args: &[&str], client: &MockClient) -> Result<String> {
        let cli = Cli::parse_from(args);
        execute(&cli, client).await
    }

    #[tokio::test]
    async fn test_volume_list_table() {
        let output = run(&["byard", "volume", "list"]).await.unwrap();
        assert!(output.contains("test-volume"));
    }

    #[tokio::test]
    async fn test_volume_list_json() {
        let output = run(&["byard", "-o", "json", "volume", "list"])
            .await
            .unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&output).unwrap();
        assert!(!parsed.is_empty());
    }

    #[tokio::test]
    async fn test_volume_list_empty() {
        let client = MockClient::new();
        let output = run_with_client(&["byard", "volume", "list"], &client)
            .await
            .unwrap();
        assert!(output.contains("No volumes found"));
    }

    #[tokio::test]
    async fn test_volume_create_replicated() {
        let client = MockClient::new();
        let output = run_with_client(
            &[
                "byard",
                "volume",
                "create",
                "new-vol",
                "--size",
                "10G",
                "--replicas",
                "3",
            ],
            &client,
        )
        .await
        .unwrap();
        assert!(output.contains("new-vol"));
        assert!(output.contains("replicated(3)"));
    }

    #[tokio::test]
    async fn test_volume_create_ec() {
        let client = MockClient::new();
        let output = run_with_client(
            &[
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
            ],
            &client,
        )
        .await
        .unwrap();
        assert!(output.contains("ec-vol"));
        assert!(output.contains("ec(4+2)"));
    }

    #[tokio::test]
    async fn test_volume_create_json() {
        let client = MockClient::new();
        let output = run_with_client(
            &[
                "byard", "-o", "json", "volume", "create", "json-vol", "--size", "1G",
            ],
            &client,
        )
        .await
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed["name"], "json-vol");
    }

    #[tokio::test]
    async fn test_volume_create_invalid_size() {
        let client = MockClient::new();
        let result = run_with_client(
            &["byard", "volume", "create", "bad-vol", "--size", "abc"],
            &client,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_volume_create_zero_replicas() {
        let client = MockClient::new();
        let result = run_with_client(
            &[
                "byard",
                "volume",
                "create",
                "bad-vol",
                "--size",
                "1G",
                "--replicas",
                "0",
            ],
            &client,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_volume_inspect_table() {
        let client = MockClient::with_sample_data();
        let vols = client.volume_list().await.unwrap();
        let id = vols[0].id;
        let output = run_with_client(&["byard", "volume", "inspect", &id.to_string()], &client)
            .await
            .unwrap();
        assert!(output.contains("test-volume"));
        assert!(output.contains("Volume:"));
    }

    #[tokio::test]
    async fn test_volume_inspect_json() {
        let client = MockClient::with_sample_data();
        let vols = client.volume_list().await.unwrap();
        let id = vols[0].id;
        let output = run_with_client(
            &["byard", "-o", "json", "volume", "inspect", &id.to_string()],
            &client,
        )
        .await
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed["name"], "test-volume");
    }

    #[tokio::test]
    async fn test_volume_inspect_not_found() {
        let client = MockClient::new();
        let id = blockyard_common::VolumeId::generate();
        let result =
            run_with_client(&["byard", "volume", "inspect", &id.to_string()], &client).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_volume_delete_table() {
        let client = MockClient::with_sample_data();
        let vols = client.volume_list().await.unwrap();
        let id = vols[0].id;
        let output = run_with_client(
            &["byard", "volume", "delete", &id.to_string(), "--force"],
            &client,
        )
        .await
        .unwrap();
        assert!(output.contains("deleted"));
    }

    #[tokio::test]
    async fn test_volume_delete_json() {
        let client = MockClient::with_sample_data();
        let vols = client.volume_list().await.unwrap();
        let id = vols[0].id;
        let output = run_with_client(
            &[
                "byard",
                "-o",
                "json",
                "volume",
                "delete",
                &id.to_string(),
                "--force",
            ],
            &client,
        )
        .await
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert!(parsed["deleted"].is_string());
    }

    #[tokio::test]
    async fn test_disk_list_table() {
        let output = run(&["byard", "disk", "list"]).await.unwrap();
        assert!(output.contains("/dev/sda"));
    }

    #[tokio::test]
    async fn test_disk_list_json() {
        let output = run(&["byard", "-o", "json", "disk", "list"]).await.unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed.len(), 2);
    }

    #[tokio::test]
    async fn test_disk_inspect_table() {
        let client = MockClient::with_sample_data();
        let disks = client.disk_list().await.unwrap();
        let id = disks[0].id;
        let output = run_with_client(&["byard", "disk", "inspect", &id.to_string()], &client)
            .await
            .unwrap();
        assert!(output.contains("Disk:"));
        assert!(output.contains("/dev/sda"));
    }

    #[tokio::test]
    async fn test_disk_drain_table() {
        let client = MockClient::with_sample_data();
        let disks = client.disk_list().await.unwrap();
        let id = disks[0].id;
        let output = run_with_client(&["byard", "disk", "drain", &id.to_string()], &client)
            .await
            .unwrap();
        assert!(output.contains("drain initiated"));
    }

    #[tokio::test]
    async fn test_disk_drain_json() {
        let client = MockClient::with_sample_data();
        let disks = client.disk_list().await.unwrap();
        let id = disks[0].id;
        let output = run_with_client(
            &["byard", "-o", "json", "disk", "drain", &id.to_string()],
            &client,
        )
        .await
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert!(parsed["draining"].is_string());
    }

    #[tokio::test]
    async fn test_disk_remove_table() {
        let client = MockClient::with_sample_data();
        let disks = client.disk_list().await.unwrap();
        let id = disks[0].id;
        let output = run_with_client(
            &["byard", "disk", "remove", &id.to_string(), "--force"],
            &client,
        )
        .await
        .unwrap();
        assert!(output.contains("removed"));
    }

    #[tokio::test]
    async fn test_disk_remove_json() {
        let client = MockClient::with_sample_data();
        let disks = client.disk_list().await.unwrap();
        let id = disks[0].id;
        let output = run_with_client(
            &[
                "byard",
                "-o",
                "json",
                "disk",
                "remove",
                &id.to_string(),
                "--force",
            ],
            &client,
        )
        .await
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert!(parsed["removed"].is_string());
    }

    #[tokio::test]
    async fn test_node_list_table() {
        let output = run(&["byard", "node", "list"]).await.unwrap();
        assert!(output.contains("10.0.0.1:9800"));
        assert!(output.contains("online"));
    }

    #[tokio::test]
    async fn test_node_list_json() {
        let output = run(&["byard", "-o", "json", "node", "list"]).await.unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed.len(), 3);
    }

    #[tokio::test]
    async fn test_node_inspect_table() {
        let client = MockClient::with_sample_data();
        let nodes = client.node_list().await.unwrap();
        let id = nodes[0].id;
        let output = run_with_client(&["byard", "node", "inspect", &id.to_string()], &client)
            .await
            .unwrap();
        assert!(output.contains("Node:"));
        assert!(output.contains("10.0.0.1:9800"));
    }

    #[tokio::test]
    async fn test_node_decommission_table() {
        let client = MockClient::with_sample_data();
        let nodes = client.node_list().await.unwrap();
        let id = nodes[0].id;
        let output = run_with_client(
            &["byard", "node", "decommission", &id.to_string(), "--force"],
            &client,
        )
        .await
        .unwrap();
        assert!(output.contains("decommission initiated"));
    }

    #[tokio::test]
    async fn test_node_decommission_json() {
        let client = MockClient::with_sample_data();
        let nodes = client.node_list().await.unwrap();
        let id = nodes[0].id;
        let output = run_with_client(
            &[
                "byard",
                "-o",
                "json",
                "node",
                "decommission",
                &id.to_string(),
                "--force",
            ],
            &client,
        )
        .await
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert!(parsed["decommissioning"].is_string());
    }

    #[tokio::test]
    async fn test_cluster_status_table() {
        let output = run(&["byard", "cluster"]).await.unwrap();
        assert!(output.contains("Cluster Status"));
        assert!(output.contains("3/3 online"));
        assert!(output.contains("healthy"));
    }

    #[tokio::test]
    async fn test_cluster_status_json() {
        let output = run(&["byard", "-o", "json", "cluster"]).await.unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed["node_count"], 3);
    }

    #[tokio::test]
    async fn test_cluster_status_subcommand() {
        let output = run(&["byard", "cluster", "status"]).await.unwrap();
        assert!(output.contains("Cluster Status"));
    }

    #[tokio::test]
    async fn test_mount_table() {
        let client = MockClient::with_sample_data();
        let vols = client.volume_list().await.unwrap();
        let id = vols[0].id;
        let output = run_with_client(&["byard", "mount", &id.to_string()], &client)
            .await
            .unwrap();
        assert!(output.contains("Volume:"));
        assert!(output.contains("/dev/ublk0"));
    }

    #[tokio::test]
    async fn test_mount_json() {
        let client = MockClient::with_sample_data();
        let vols = client.volume_list().await.unwrap();
        let id = vols[0].id;
        let output = run_with_client(&["byard", "-o", "json", "mount", &id.to_string()], &client)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed["device_path"], "/dev/ublk0");
    }

    #[tokio::test]
    async fn test_mount_with_device() {
        let client = MockClient::with_sample_data();
        let vols = client.volume_list().await.unwrap();
        let id = vols[0].id;
        let output = run_with_client(
            &["byard", "mount", &id.to_string(), "--device", "/dev/ublk5"],
            &client,
        )
        .await
        .unwrap();
        assert!(output.contains("/dev/ublk5"));
    }

    #[tokio::test]
    async fn test_unmount_table() {
        let client = MockClient::with_sample_data();
        let vols = client.volume_list().await.unwrap();
        let id = vols[0].id;
        client.mount(id, None).await.unwrap();
        let output = run_with_client(&["byard", "unmount", &id.to_string()], &client)
            .await
            .unwrap();
        assert!(output.contains("unmounted"));
    }

    #[tokio::test]
    async fn test_unmount_json() {
        let client = MockClient::with_sample_data();
        let vols = client.volume_list().await.unwrap();
        let id = vols[0].id;
        client.mount(id, None).await.unwrap();
        let output = run_with_client(
            &["byard", "-o", "json", "unmount", &id.to_string()],
            &client,
        )
        .await
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert!(parsed["unmounted"].is_string());
    }

    #[tokio::test]
    async fn test_unmount_not_mounted() {
        let client = MockClient::with_sample_data();
        let vols = client.volume_list().await.unwrap();
        let id = vols[0].id;
        let result = run_with_client(&["byard", "unmount", &id.to_string()], &client).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_client_error_propagates() {
        let client = MockClient::with_sample_data();
        client.set_fail_next("connection refused");
        let result = run_with_client(&["byard", "volume", "list"], &client).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("connection refused")
        );
    }

    #[tokio::test]
    async fn test_to_output_format_table() {
        assert_eq!(to_output_format(OutputMode::Table), OutputFormat::Table);
    }

    #[tokio::test]
    async fn test_to_output_format_json() {
        assert_eq!(to_output_format(OutputMode::Json), OutputFormat::Json);
    }
}
