//! Output formatting — table and JSON modes.

use anyhow::Result;
use comfy_table::{Cell, ContentArrangement, Table};
use serde::Serialize;

use crate::types::{ClusterStatus, DiskInfo, MountInfo, NodeInfo, VolumeInfo};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Table,
    Json,
}

pub fn print_json<T: Serialize + ?Sized>(value: &T) -> Result<String> {
    Ok(serde_json::to_string_pretty(value)?)
}

pub fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;
    const TIB: u64 = GIB * 1024;

    if bytes >= TIB {
        format!("{:.1} TiB", bytes as f64 / TIB as f64)
    } else if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{} B", bytes)
    }
}

pub fn format_uptime(seconds: u64) -> String {
    let days = seconds / 86400;
    let hours = (seconds % 86400) / 3600;
    let mins = (seconds % 3600) / 60;

    if days > 0 {
        format!("{}d {}h {}m", days, hours, mins)
    } else if hours > 0 {
        format!("{}h {}m", hours, mins)
    } else {
        format!("{}m", mins)
    }
}

fn short_id(id: &str) -> &str {
    if id.len() > 8 { &id[..8] } else { id }
}

pub fn format_volume_list(volumes: &[VolumeInfo], format: OutputFormat) -> Result<String> {
    match format {
        OutputFormat::Json => print_json(volumes),
        OutputFormat::Table => {
            if volumes.is_empty() {
                return Ok("No volumes found.".into());
            }
            let mut table = Table::new();
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(vec![
                "ID",
                "NAME",
                "SIZE",
                "PROTECTION",
                "STATE",
                "REPLICAS",
            ]);

            for vol in volumes {
                table.add_row(vec![
                    Cell::new(short_id(&vol.id.to_string())),
                    Cell::new(&vol.name),
                    Cell::new(format_bytes(vol.size_bytes)),
                    Cell::new(vol.protection.to_string()),
                    Cell::new(vol.state.to_string()),
                    Cell::new(vol.replica_nodes.len()),
                ]);
            }
            Ok(table.to_string())
        }
    }
}

pub fn format_volume_detail(vol: &VolumeInfo, format: OutputFormat) -> Result<String> {
    match format {
        OutputFormat::Json => print_json(vol),
        OutputFormat::Table => {
            let mut lines = Vec::new();
            lines.push(format!("Volume:     {}", vol.id));
            lines.push(format!("Name:       {}", vol.name));
            lines.push(format!("Size:       {}", format_bytes(vol.size_bytes)));
            lines.push(format!("Protection: {}", vol.protection));
            lines.push(format!("State:      {}", vol.state));
            lines.push(format!(
                "Created:    {}",
                vol.created_at.format("%Y-%m-%d %H:%M:%S UTC")
            ));
            lines.push(format!("Replicas:   {}", vol.replica_nodes.len()));
            for (i, node) in vol.replica_nodes.iter().enumerate() {
                lines.push(format!("  [{}] {}", i, node));
            }
            Ok(lines.join("\n"))
        }
    }
}

pub fn format_disk_list(disks: &[DiskInfo], format: OutputFormat) -> Result<String> {
    match format {
        OutputFormat::Json => print_json(disks),
        OutputFormat::Table => {
            if disks.is_empty() {
                return Ok("No disks found.".into());
            }
            let mut table = Table::new();
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(vec![
                "ID", "NODE", "PATH", "STATE", "USED", "TOTAL", "EXTENTS", "ERRORS",
            ]);

            for disk in disks {
                table.add_row(vec![
                    Cell::new(short_id(&disk.id.to_string())),
                    Cell::new(short_id(&disk.node_id.to_string())),
                    Cell::new(&disk.path),
                    Cell::new(disk.state.to_string()),
                    Cell::new(format_bytes(disk.used_bytes)),
                    Cell::new(format_bytes(disk.total_bytes)),
                    Cell::new(disk.extent_count),
                    Cell::new(disk.error_count),
                ]);
            }
            Ok(table.to_string())
        }
    }
}

pub fn format_disk_detail(disk: &DiskInfo, format: OutputFormat) -> Result<String> {
    match format {
        OutputFormat::Json => print_json(disk),
        OutputFormat::Table => {
            let used_pct = if disk.total_bytes > 0 {
                (disk.used_bytes as f64 / disk.total_bytes as f64) * 100.0
            } else {
                0.0
            };
            let mut lines = Vec::new();
            lines.push(format!("Disk:     {}", disk.id));
            lines.push(format!("Node:     {}", disk.node_id));
            lines.push(format!("Path:     {}", disk.path));
            lines.push(format!("State:    {}", disk.state));
            lines.push(format!("Total:    {}", format_bytes(disk.total_bytes)));
            lines.push(format!(
                "Used:     {} ({:.1}%)",
                format_bytes(disk.used_bytes),
                used_pct
            ));
            lines.push(format!("Extents:  {}", disk.extent_count));
            lines.push(format!("Errors:   {}", disk.error_count));
            Ok(lines.join("\n"))
        }
    }
}

pub fn format_node_list(nodes: &[NodeInfo], format: OutputFormat) -> Result<String> {
    match format {
        OutputFormat::Json => print_json(nodes),
        OutputFormat::Table => {
            if nodes.is_empty() {
                return Ok("No nodes found.".into());
            }
            let mut table = Table::new();
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(vec!["ID", "ADDRESS", "STATE", "DISKS", "VOLUMES", "UPTIME"]);

            for node in nodes {
                table.add_row(vec![
                    Cell::new(short_id(&node.id.to_string())),
                    Cell::new(&node.address),
                    Cell::new(node.state.to_string()),
                    Cell::new(node.disk_count),
                    Cell::new(node.volume_count),
                    Cell::new(format_uptime(node.uptime_seconds)),
                ]);
            }
            Ok(table.to_string())
        }
    }
}

pub fn format_node_detail(node: &NodeInfo, format: OutputFormat) -> Result<String> {
    match format {
        OutputFormat::Json => print_json(node),
        OutputFormat::Table => {
            let mut lines = Vec::new();
            lines.push(format!("Node:     {}", node.id));
            lines.push(format!("Address:  {}", node.address));
            lines.push(format!("State:    {}", node.state));
            lines.push(format!("Disks:    {}", node.disk_count));
            lines.push(format!("Volumes:  {}", node.volume_count));
            lines.push(format!("Uptime:   {}", format_uptime(node.uptime_seconds)));
            Ok(lines.join("\n"))
        }
    }
}

pub fn format_cluster_status(status: &ClusterStatus, format: OutputFormat) -> Result<String> {
    match format {
        OutputFormat::Json => print_json(status),
        OutputFormat::Table => {
            let used_pct = if status.total_capacity_bytes > 0 {
                (status.used_capacity_bytes as f64 / status.total_capacity_bytes as f64) * 100.0
            } else {
                0.0
            };
            let mut lines = Vec::new();
            lines.push("Cluster Status".to_string());
            lines.push("──────────────────────────────".to_string());
            lines.push(format!(
                "Nodes:     {}/{} online",
                status.nodes_online, status.node_count
            ));
            lines.push(format!("Volumes:   {}", status.volume_count));
            lines.push(format!("Disks:     {}", status.disk_count));
            lines.push(format!("Epoch:     {}", status.placement_epoch));
            lines.push(format!("Quorum:    {}", status.quorum_health));
            lines.push(format!(
                "Capacity:  {} / {} ({:.1}%)",
                format_bytes(status.used_capacity_bytes),
                format_bytes(status.total_capacity_bytes),
                used_pct
            ));
            Ok(lines.join("\n"))
        }
    }
}

pub fn format_mount_info(info: &MountInfo, format: OutputFormat) -> Result<String> {
    match format {
        OutputFormat::Json => print_json(info),
        OutputFormat::Table => {
            let mut lines = Vec::new();
            lines.push(format!("Volume:  {}", info.volume_id));
            lines.push(format!("Device:  {}", info.device_path));
            if let Some(ref mp) = info.mount_point {
                lines.push(format!("Mount:   {}", mp));
            }
            Ok(lines.join("\n"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;
    use blockyard_common::{DiskId, DiskState, EpochId, NodeId, ProtectionPolicy, VolumeId};
    use chrono::Utc;

    #[test]
    fn test_format_bytes_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1023), "1023 B");
    }

    #[test]
    fn test_format_bytes_kib() {
        assert_eq!(format_bytes(1024), "1.0 KiB");
        assert_eq!(format_bytes(1536), "1.5 KiB");
    }

    #[test]
    fn test_format_bytes_mib() {
        assert_eq!(format_bytes(1024 * 1024), "1.0 MiB");
        assert_eq!(format_bytes(5 * 1024 * 1024), "5.0 MiB");
    }

    #[test]
    fn test_format_bytes_gib() {
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GiB");
        assert_eq!(format_bytes(10 * 1024 * 1024 * 1024), "10.0 GiB");
    }

    #[test]
    fn test_format_bytes_tib() {
        assert_eq!(format_bytes(1024u64 * 1024 * 1024 * 1024), "1.0 TiB");
    }

    #[test]
    fn test_format_uptime_minutes() {
        assert_eq!(format_uptime(0), "0m");
        assert_eq!(format_uptime(59), "0m");
        assert_eq!(format_uptime(300), "5m");
    }

    #[test]
    fn test_format_uptime_hours() {
        assert_eq!(format_uptime(3600), "1h 0m");
        assert_eq!(format_uptime(5400), "1h 30m");
    }

    #[test]
    fn test_format_uptime_days() {
        assert_eq!(format_uptime(86400), "1d 0h 0m");
        assert_eq!(format_uptime(90000), "1d 1h 0m");
        assert_eq!(format_uptime(172800), "2d 0h 0m");
    }

    #[test]
    fn test_short_id_long() {
        let id = "abcdef01-2345-6789-abcd-ef0123456789";
        assert_eq!(short_id(id), "abcdef01");
    }

    #[test]
    fn test_short_id_short() {
        let id = "abc";
        assert_eq!(short_id(id), "abc");
    }

    #[test]
    fn test_short_id_exact() {
        let id = "12345678";
        assert_eq!(short_id(id), "12345678");
    }

    #[test]
    fn test_print_json_simple() {
        let val = vec![1, 2, 3];
        let json = print_json(&val).unwrap();
        assert!(json.contains("1"));
        assert!(json.contains("2"));
        assert!(json.contains("3"));
    }

    #[test]
    fn test_format_volume_list_empty_table() {
        let output = format_volume_list(&[], OutputFormat::Table).unwrap();
        assert_eq!(output, "No volumes found.");
    }

    #[test]
    fn test_format_volume_list_empty_json() {
        let output = format_volume_list(&[], OutputFormat::Json).unwrap();
        assert_eq!(output, "[]");
    }

    #[test]
    fn test_format_volume_list_table() {
        let vols = vec![sample_volume()];
        let output = format_volume_list(&vols, OutputFormat::Table).unwrap();
        assert!(output.contains("test-vol"));
        assert!(output.contains("healthy"));
        assert!(output.contains("replicated(3)"));
    }

    #[test]
    fn test_format_volume_list_json() {
        let vols = vec![sample_volume()];
        let output = format_volume_list(&vols, OutputFormat::Json).unwrap();
        let parsed: Vec<VolumeInfo> = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].name, "test-vol");
    }

    #[test]
    fn test_format_volume_detail_table() {
        let vol = sample_volume();
        let output = format_volume_detail(&vol, OutputFormat::Table).unwrap();
        assert!(output.contains("test-vol"));
        assert!(output.contains("Volume:"));
        assert!(output.contains("Protection:"));
        assert!(output.contains("replicated(3)"));
    }

    #[test]
    fn test_format_volume_detail_json() {
        let vol = sample_volume();
        let output = format_volume_detail(&vol, OutputFormat::Json).unwrap();
        let parsed: VolumeInfo = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed.name, "test-vol");
    }

    #[test]
    fn test_format_disk_list_empty_table() {
        let output = format_disk_list(&[], OutputFormat::Table).unwrap();
        assert_eq!(output, "No disks found.");
    }

    #[test]
    fn test_format_disk_list_empty_json() {
        let output = format_disk_list(&[], OutputFormat::Json).unwrap();
        assert_eq!(output, "[]");
    }

    #[test]
    fn test_format_disk_list_table() {
        let disks = vec![sample_disk()];
        let output = format_disk_list(&disks, OutputFormat::Table).unwrap();
        assert!(output.contains("/dev/sda"));
        assert!(output.contains("healthy"));
    }

    #[test]
    fn test_format_disk_list_json() {
        let disks = vec![sample_disk()];
        let output = format_disk_list(&disks, OutputFormat::Json).unwrap();
        let parsed: Vec<DiskInfo> = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed.len(), 1);
    }

    #[test]
    fn test_format_disk_detail_table() {
        let disk = sample_disk();
        let output = format_disk_detail(&disk, OutputFormat::Table).unwrap();
        assert!(output.contains("Disk:"));
        assert!(output.contains("/dev/sda"));
        assert!(output.contains("Errors:"));
    }

    #[test]
    fn test_format_disk_detail_json() {
        let disk = sample_disk();
        let output = format_disk_detail(&disk, OutputFormat::Json).unwrap();
        let parsed: DiskInfo = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed.path, "/dev/sda");
    }

    #[test]
    fn test_format_disk_detail_zero_total() {
        let mut disk = sample_disk();
        disk.total_bytes = 0;
        disk.used_bytes = 0;
        let output = format_disk_detail(&disk, OutputFormat::Table).unwrap();
        assert!(output.contains("0.0%"));
    }

    #[test]
    fn test_format_node_list_empty_table() {
        let output = format_node_list(&[], OutputFormat::Table).unwrap();
        assert_eq!(output, "No nodes found.");
    }

    #[test]
    fn test_format_node_list_empty_json() {
        let output = format_node_list(&[], OutputFormat::Json).unwrap();
        assert_eq!(output, "[]");
    }

    #[test]
    fn test_format_node_list_table() {
        let nodes = vec![sample_node()];
        let output = format_node_list(&nodes, OutputFormat::Table).unwrap();
        assert!(output.contains("10.0.0.1:9800"));
        assert!(output.contains("online"));
    }

    #[test]
    fn test_format_node_list_json() {
        let nodes = vec![sample_node()];
        let output = format_node_list(&nodes, OutputFormat::Json).unwrap();
        let parsed: Vec<NodeInfo> = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed.len(), 1);
    }

    #[test]
    fn test_format_node_detail_table() {
        let node = sample_node();
        let output = format_node_detail(&node, OutputFormat::Table).unwrap();
        assert!(output.contains("Node:"));
        assert!(output.contains("10.0.0.1:9800"));
        assert!(output.contains("Uptime:"));
    }

    #[test]
    fn test_format_node_detail_json() {
        let node = sample_node();
        let output = format_node_detail(&node, OutputFormat::Json).unwrap();
        let parsed: NodeInfo = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed.address, "10.0.0.1:9800");
    }

    #[test]
    fn test_format_cluster_status_table() {
        let status = sample_cluster_status();
        let output = format_cluster_status(&status, OutputFormat::Table).unwrap();
        assert!(output.contains("Cluster Status"));
        assert!(output.contains("3/3 online"));
        assert!(output.contains("healthy"));
        assert!(output.contains("Epoch:"));
    }

    #[test]
    fn test_format_cluster_status_json() {
        let status = sample_cluster_status();
        let output = format_cluster_status(&status, OutputFormat::Json).unwrap();
        let parsed: ClusterStatus = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed.node_count, 3);
    }

    #[test]
    fn test_format_cluster_status_zero_capacity() {
        let mut status = sample_cluster_status();
        status.total_capacity_bytes = 0;
        status.used_capacity_bytes = 0;
        let output = format_cluster_status(&status, OutputFormat::Table).unwrap();
        assert!(output.contains("0.0%"));
    }

    #[test]
    fn test_format_mount_info_table() {
        let info = MountInfo {
            volume_id: VolumeId::generate(),
            device_path: "/dev/ublk0".into(),
            mount_point: Some("/mnt/data".into()),
        };
        let output = format_mount_info(&info, OutputFormat::Table).unwrap();
        assert!(output.contains("Volume:"));
        assert!(output.contains("/dev/ublk0"));
        assert!(output.contains("/mnt/data"));
    }

    #[test]
    fn test_format_mount_info_no_mount_point() {
        let info = MountInfo {
            volume_id: VolumeId::generate(),
            device_path: "/dev/ublk0".into(),
            mount_point: None,
        };
        let output = format_mount_info(&info, OutputFormat::Table).unwrap();
        assert!(output.contains("/dev/ublk0"));
        assert!(!output.contains("Mount:"));
    }

    #[test]
    fn test_format_mount_info_json() {
        let info = MountInfo {
            volume_id: VolumeId::generate(),
            device_path: "/dev/ublk0".into(),
            mount_point: None,
        };
        let output = format_mount_info(&info, OutputFormat::Json).unwrap();
        let parsed: MountInfo = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed.device_path, "/dev/ublk0");
    }

    #[test]
    fn test_output_format_equality() {
        assert_eq!(OutputFormat::Table, OutputFormat::Table);
        assert_eq!(OutputFormat::Json, OutputFormat::Json);
        assert_ne!(OutputFormat::Table, OutputFormat::Json);
    }

    #[test]
    fn test_output_format_debug() {
        let debug = format!("{:?}", OutputFormat::Table);
        assert_eq!(debug, "Table");
    }

    #[test]
    fn test_output_format_clone() {
        let fmt = OutputFormat::Json;
        let cloned = fmt;
        assert_eq!(fmt, cloned);
    }

    fn sample_volume() -> VolumeInfo {
        VolumeInfo {
            id: VolumeId::generate(),
            name: "test-vol".into(),
            size_bytes: 10 * 1024 * 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            state: VolumeState::Healthy,
            replica_nodes: vec![NodeId::generate(), NodeId::generate()],
            created_at: Utc::now(),
        }
    }

    fn sample_disk() -> DiskInfo {
        DiskInfo {
            id: DiskId::generate(),
            node_id: NodeId::generate(),
            path: "/dev/sda".into(),
            state: DiskState::Healthy,
            total_bytes: 1_000_000_000_000,
            used_bytes: 400_000_000_000,
            extent_count: 150,
            error_count: 0,
        }
    }

    fn sample_node() -> NodeInfo {
        NodeInfo {
            id: NodeId::generate(),
            address: "10.0.0.1:9800".into(),
            state: NodeState::Online,
            disk_count: 4,
            volume_count: 10,
            uptime_seconds: 86400,
        }
    }

    fn sample_cluster_status() -> ClusterStatus {
        ClusterStatus {
            node_count: 3,
            nodes_online: 3,
            volume_count: 10,
            disk_count: 12,
            placement_epoch: EpochId::new(42),
            quorum_health: QuorumHealth::Healthy,
            total_capacity_bytes: 5_000_000_000_000,
            used_capacity_bytes: 2_000_000_000_000,
        }
    }
}
