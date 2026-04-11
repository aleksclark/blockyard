use blockyard_cli::cli::{Cli, Command, OutputMode, VolumeCommand};
use blockyard_cli::client::BlockyardClient;
use blockyard_cli::client::mock::MockClient;
use blockyard_cli::commands::execute;

use blockyard_common::{DiskState, NodeId, VolumeId};
use clap::Parser;

fn parse(args: &[&str]) -> Cli {
    Cli::parse_from(args)
}

async fn run(args: &[&str]) -> anyhow::Result<String> {
    let client = MockClient::with_sample_data();
    let cli = parse(args);
    execute(&cli, &client).await
}

async fn run_with(args: &[&str], client: &MockClient) -> anyhow::Result<String> {
    let cli = parse(args);
    execute(&cli, client).await
}

// ---------------------------------------------------------------------------
// 1. test_volume_create_parse
// ---------------------------------------------------------------------------

#[test]
fn test_volume_create_parse() {
    let cli = parse(&[
        "byard",
        "volume",
        "create",
        "test",
        "--size",
        "1G",
        "--replicas",
        "3",
    ]);

    assert_eq!(cli.output, OutputMode::Table);
    assert_eq!(cli.endpoint, "http://127.0.0.1:9801");

    match &cli.command {
        Command::Volume(VolumeCommand::Create(args)) => {
            assert_eq!(args.name, "test");
            assert_eq!(args.size, "1G");
            assert_eq!(args.replicas, 3);
            assert!(args.data_chunks.is_none());
            assert!(args.parity.is_none());
        }
        other => panic!("expected Volume(Create), got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// 2. test_volume_list_json_output
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_volume_list_json_output() {
    let output = run(&["byard", "-o", "json", "volume", "list"])
        .await
        .unwrap();

    let parsed: Vec<serde_json::Value> = serde_json::from_str(&output).unwrap();
    assert_eq!(parsed.len(), 1);

    let vol = &parsed[0];
    assert!(vol.get("id").is_some());
    assert!(vol.get("name").is_some());
    assert!(vol.get("size_bytes").is_some());
    assert!(vol.get("protection").is_some());
    assert!(vol.get("state").is_some());
    assert!(vol.get("replica_nodes").is_some());
    assert!(vol.get("created_at").is_some());

    assert_eq!(vol["name"], "test-volume");
    assert_eq!(vol["size_bytes"], 10 * 1024 * 1024 * 1024_u64);
}

// ---------------------------------------------------------------------------
// 3. test_volume_list_table_output
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_volume_list_table_output() {
    let output = run(&["byard", "volume", "list"]).await.unwrap();

    assert!(output.contains("ID"));
    assert!(output.contains("NAME"));
    assert!(output.contains("SIZE"));
    assert!(output.contains("PROTECTION"));
    assert!(output.contains("STATE"));
    assert!(output.contains("REPLICAS"));

    assert!(output.contains("test-volume"));

    let data_lines: Vec<&str> = output
        .lines()
        .filter(|l| l.contains("test-volume"))
        .collect();
    assert_eq!(data_lines.len(), 1);
}

// ---------------------------------------------------------------------------
// 4. test_disk_list_with_status_filter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_disk_list_with_status_filter() {
    let client = MockClient::with_sample_data();

    {
        let mut disks = client.disks.lock();
        disks[1].state = DiskState::Draining;
    }

    let output = run_with(&["byard", "-o", "json", "disk", "list"], &client)
        .await
        .unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&output).unwrap();
    assert_eq!(parsed.len(), 2);

    let healthy: Vec<&serde_json::Value> =
        parsed.iter().filter(|d| d["state"] == "Healthy").collect();
    assert_eq!(healthy.len(), 1);

    let draining: Vec<&serde_json::Value> =
        parsed.iter().filter(|d| d["state"] == "Draining").collect();
    assert_eq!(draining.len(), 1);
}

// ---------------------------------------------------------------------------
// 5. test_node_inspect_output
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_node_inspect_output() {
    let client = MockClient::with_sample_data();
    let nodes = client.node_list().await.unwrap();
    let node = &nodes[0];

    let output = run_with(&["byard", "node", "inspect", &node.id.to_string()], &client)
        .await
        .unwrap();

    assert!(output.contains("Node:"));
    assert!(output.contains(&node.id.to_string()));
    assert!(output.contains("Address:"));
    assert!(output.contains(&node.address));
    assert!(output.contains("State:"));
    assert!(output.contains("online"));
    assert!(output.contains("Disks:"));
    assert!(output.contains("Volumes:"));
    assert!(output.contains("Uptime:"));
}

// ---------------------------------------------------------------------------
// 6. test_cluster_status_output
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_cluster_status_output() {
    let output = run(&["byard", "cluster"]).await.unwrap();

    assert!(output.contains("Epoch:"));
    assert!(output.contains("3/3 online") || output.contains("Nodes:"));
    assert!(output.contains("Quorum:"));
    assert!(output.contains("healthy"));
}

#[tokio::test]
async fn test_cluster_status_json_output() {
    let output = run(&["byard", "-o", "json", "cluster"]).await.unwrap();

    let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert!(parsed.get("node_count").is_some());
    assert!(parsed.get("nodes_online").is_some());
    assert!(parsed.get("placement_epoch").is_some());
    assert!(parsed.get("quorum_health").is_some());
    assert_eq!(parsed["node_count"], 3);
    assert_eq!(parsed["nodes_online"], 3);
}

// ---------------------------------------------------------------------------
// 7. test_invalid_command_error
// ---------------------------------------------------------------------------

#[test]
fn test_invalid_command_error() {
    let result = Cli::try_parse_from(["byard", "nonexistent"]);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("invalid") || err.contains("unrecognized") || err.contains("not found"),
        "error should be helpful, got: {}",
        err
    );
}

#[test]
fn test_missing_required_args_error() {
    let result = Cli::try_parse_from(["byard", "volume", "create"]);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("required") || err.contains("Usage"),
        "error should mention required args, got: {}",
        err
    );
}

#[test]
fn test_invalid_output_mode_error() {
    let result = Cli::try_parse_from(["byard", "-o", "xml", "volume", "list"]);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("invalid value") || err.contains("possible values"),
        "error should mention valid output modes, got: {}",
        err
    );
}

// ---------------------------------------------------------------------------
// 8. test_json_vs_table_output_modes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_json_vs_table_output_modes() {
    let client = MockClient::with_sample_data();

    let table_output = run_with(&["byard", "volume", "list"], &client)
        .await
        .unwrap();
    let json_output = run_with(&["byard", "-o", "json", "volume", "list"], &client)
        .await
        .unwrap();

    assert_ne!(table_output, json_output);

    let json_parsed: serde_json::Result<Vec<serde_json::Value>> =
        serde_json::from_str(&json_output);
    assert!(json_parsed.is_ok());

    let table_parsed: serde_json::Result<serde_json::Value> = serde_json::from_str(&table_output);
    assert!(table_parsed.is_err());

    assert!(table_output.contains("NAME"));
    assert!(table_output.contains("SIZE"));
}

#[tokio::test]
async fn test_json_vs_table_node_list() {
    let client = MockClient::with_sample_data();

    let table_output = run_with(&["byard", "node", "list"], &client).await.unwrap();
    let json_output = run_with(&["byard", "-o", "json", "node", "list"], &client)
        .await
        .unwrap();

    assert_ne!(table_output, json_output);

    let parsed: Vec<serde_json::Value> = serde_json::from_str(&json_output).unwrap();
    assert_eq!(parsed.len(), 3);

    assert!(table_output.contains("ADDRESS"));
    assert!(table_output.contains("STATE"));
}

// ---------------------------------------------------------------------------
// 9. test_volume_delete_confirmation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_volume_delete_confirmation() {
    let client = MockClient::with_sample_data();
    let vols = client.volume_list().await.unwrap();
    let id = vols[0].id;

    let cli = parse(&["byard", "volume", "delete", &id.to_string(), "--force"]);
    match &cli.command {
        Command::Volume(VolumeCommand::Delete(args)) => {
            assert!(args.force);
            assert_eq!(args.id, id);
        }
        other => panic!("expected Volume(Delete), got {:?}", other),
    }

    let output = run_with(
        &["byard", "volume", "delete", &id.to_string(), "--force"],
        &client,
    )
    .await
    .unwrap();
    assert!(output.contains("deleted"));

    let remaining = client.volume_list().await.unwrap();
    assert!(remaining.is_empty());
}

#[tokio::test]
async fn test_volume_delete_no_force_parses() {
    let id = VolumeId::generate();
    let cli = parse(&["byard", "volume", "delete", &id.to_string()]);
    match &cli.command {
        Command::Volume(VolumeCommand::Delete(args)) => {
            assert!(!args.force);
        }
        other => panic!("expected Volume(Delete), got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// 10. test_all_subcommands_parse
// ---------------------------------------------------------------------------

#[test]
fn test_all_subcommands_parse() {
    let vol_id = VolumeId::generate().to_string();
    let node_id = NodeId::generate().to_string();
    let disk_id = blockyard_common::DiskId::generate().to_string();

    macro_rules! assert_parses {
        ($desc:expr, $($arg:expr),+ $(,)?) => {
            let result = Cli::try_parse_from([$($arg),+]);
            assert!(
                result.is_ok(),
                "subcommand '{}' failed to parse: {:?}",
                $desc,
                result.unwrap_err()
            );
        };
    }

    assert_parses!("volume list", "byard", "volume", "list");
    assert_parses!(
        "volume create",
        "byard",
        "volume",
        "create",
        "v",
        "--size",
        "1G"
    );
    assert_parses!("volume inspect", "byard", "volume", "inspect", &vol_id);
    assert_parses!(
        "volume delete",
        "byard",
        "volume",
        "delete",
        &vol_id,
        "--force"
    );
    assert_parses!("disk list", "byard", "disk", "list");
    assert_parses!("disk inspect", "byard", "disk", "inspect", &disk_id);
    assert_parses!("disk drain", "byard", "disk", "drain", &disk_id);
    assert_parses!(
        "disk remove",
        "byard",
        "disk",
        "remove",
        &disk_id,
        "--force"
    );
    assert_parses!("node list", "byard", "node", "list");
    assert_parses!("node inspect", "byard", "node", "inspect", &node_id);
    assert_parses!(
        "node decommission",
        "byard",
        "node",
        "decommission",
        &node_id,
        "--force"
    );
    assert_parses!("cluster", "byard", "cluster");
    assert_parses!("cluster status", "byard", "cluster", "status");
    assert_parses!("mount", "byard", "mount", &vol_id);
    assert_parses!(
        "mount with device",
        "byard",
        "mount",
        &vol_id,
        "--device",
        "/dev/ublk0"
    );
    assert_parses!("unmount", "byard", "unmount", &vol_id);
}

#[test]
fn test_all_subcommands_parse_with_json_flag() {
    let vol_id = VolumeId::generate().to_string();

    macro_rules! assert_json {
        ($($arg:expr),+ $(,)?) => {
            let cli = Cli::try_parse_from([$($arg),+]).unwrap();
            assert_eq!(cli.output, OutputMode::Json);
        };
    }

    assert_json!("byard", "-o", "json", "volume", "list");
    assert_json!("byard", "-o", "json", "disk", "list");
    assert_json!("byard", "-o", "json", "node", "list");
    assert_json!("byard", "-o", "json", "cluster");
    assert_json!("byard", "-o", "json", "mount", &vol_id);
    assert_json!("byard", "-o", "json", "unmount", &vol_id);
}

// ---------------------------------------------------------------------------
// Additional edge cases for completeness
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_disk_list_json_fields() {
    let output = run(&["byard", "-o", "json", "disk", "list"]).await.unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&output).unwrap();
    assert_eq!(parsed.len(), 2);

    let disk = &parsed[0];
    assert!(disk.get("id").is_some());
    assert!(disk.get("node_id").is_some());
    assert!(disk.get("path").is_some());
    assert!(disk.get("state").is_some());
    assert!(disk.get("total_bytes").is_some());
    assert!(disk.get("used_bytes").is_some());
    assert!(disk.get("extent_count").is_some());
    assert!(disk.get("error_count").is_some());
}

#[tokio::test]
async fn test_node_inspect_json_output() {
    let client = MockClient::with_sample_data();
    let nodes = client.node_list().await.unwrap();
    let node = &nodes[0];

    let output = run_with(
        &[
            "byard",
            "-o",
            "json",
            "node",
            "inspect",
            &node.id.to_string(),
        ],
        &client,
    )
    .await
    .unwrap();

    let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(parsed["address"], "10.0.0.1:9800");
    assert_eq!(parsed["state"], "online");
    assert_eq!(parsed["disk_count"], 4);
    assert_eq!(parsed["volume_count"], 10);
}

#[tokio::test]
async fn test_volume_create_execute_with_mock() {
    let client = MockClient::new();
    let output = run_with(
        &[
            "byard",
            "-o",
            "json",
            "volume",
            "create",
            "integration-vol",
            "--size",
            "500M",
            "--replicas",
            "2",
        ],
        &client,
    )
    .await
    .unwrap();

    let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(parsed["name"], "integration-vol");
    assert_eq!(parsed["size_bytes"], 500 * 1024 * 1024);

    let vols = client.volume_list().await.unwrap();
    assert_eq!(vols.len(), 1);
}
