//! Phase 9D — Rebalancing integration test scenarios.
//!
//! Rebalancing is not yet implemented (Phase 7 in the ROADMAP). These tests
//! are placeholders that exercise the test harness's rebalance simulation API.
//! They will be replaced with real rebalancing logic tests once Phase 7 lands.
//!
//! The harness infrastructure (ProcessCluster, WorkloadGenerator, ConsistencyChecker)
//! is preserved and validated here to ensure it remains functional for when
//! real rebalancing is implemented.

use std::path::PathBuf;
use std::time::Duration;

use blockyard_common::VolumeId;
use blockyard_test_harness::TestNodeId as NodeId;
use blockyard_test_harness::{
    AckStatus, Cluster, ClusterConfig, ConsistencyChecker, NetworkConfig, OperationLog,
    ProcessCluster, WorkloadConfig, WorkloadGenerator,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn cluster_config(node_count: u32, base_port: u16) -> ClusterConfig {
    ClusterConfig {
        node_count,
        binary_path: PathBuf::from("/usr/bin/false"),
        base_data_dir: PathBuf::from("/tmp/blockyard-rebal-test"),
        network: NetworkConfig {
            base_listen_port: base_port,
            base_gossip_port: base_port + 1000,
            host: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        },
    }
}

fn workload_for_volumes(volume_ids: &[VolumeId], write_ratio: f64) -> WorkloadGenerator {
    let config = WorkloadConfig {
        volume_ids: volume_ids.to_vec(),
        write_ratio,
        block_size: 4096,
        max_offset: 4096 * 64,
        operations_per_second: 1000,
        duration: Duration::from_secs(5),
        concurrent_clients: 4,
        ..Default::default()
    };
    WorkloadGenerator::new(config)
}

fn simulate_workload(workload: &WorkloadGenerator, op_count: usize, ack_all: bool) {
    for _ in 0..op_count {
        let mut op = workload.generate_operation();
        if ack_all {
            op.complete(AckStatus::Acked);
        } else {
            op.complete(AckStatus::Nacked);
        }
        workload.log().record(op);
    }
}

fn verify_consistency(
    log: &OperationLog,
    min_operations: u64,
) -> Vec<blockyard_test_harness::CheckReport> {
    let acked = log.acked_writes();
    let mut checker = ConsistencyChecker::new(clone_log(log)).with_min_operations(min_operations);
    for op in &acked {
        if let Some(checksum) = &op.data_checksum {
            checker.record_read_back(op.volume_id, op.offset, checksum.clone());
        }
    }
    checker.check_all()
}

fn clone_log(log: &OperationLog) -> OperationLog {
    let new_log = OperationLog::new();
    for op in log.all() {
        new_log.record(op);
    }
    new_log
}

// Pending Phase 7: simulates rebalance by returning synthetic counts.
fn simulate_rebalance(
    source_nodes: &[NodeId],
    target_nodes: &[NodeId],
    volumes: &[VolumeId],
) -> (u64, Vec<VolumeId>) {
    let extents_per_volume = 8u64;
    let total_extents = volumes.len() as u64 * extents_per_volume;
    let moved = total_extents / (source_nodes.len() + target_nodes.len()) as u64;
    (moved.max(1), volumes.to_vec())
}

// Pending Phase 7: simulates draining a node.
fn simulate_drain(drain_node: NodeId, remaining: &[NodeId], volumes: &[VolumeId]) -> Vec<VolumeId> {
    assert!(
        !remaining.is_empty(),
        "need at least one remaining node to drain to"
    );
    assert!(
        !remaining.contains(&drain_node),
        "drain target must not be in remaining set"
    );
    volumes.to_vec()
}

// ---------------------------------------------------------------------------
// P9D.1 — Add node → rebalance → data integrity verified
// (Placeholder: pending Phase 7 real rebalancing implementation)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_add_node_rebalance_data_integrity() {
    let mut cluster = ProcessCluster::new(cluster_config(3, 44000));
    assert_eq!(cluster.node_count(), 3);

    let vol1 = VolumeId::generate();
    let vol2 = VolumeId::generate();
    let volumes = vec![vol1, vol2];

    let workload = workload_for_volumes(&volumes, 1.0);
    simulate_workload(&workload, 60, true);
    assert_eq!(workload.log().acked_write_count(), 60);

    let new_node_id = cluster.add_node();
    assert_eq!(new_node_id, NodeId(3));
    assert_eq!(cluster.node_count(), 4);

    let original_nodes: Vec<NodeId> = (0..3).map(NodeId).collect();
    let (extents_moved, rebalanced_vols) =
        simulate_rebalance(&original_nodes, &[new_node_id], &volumes);
    assert!(extents_moved > 0);
    assert_eq!(rebalanced_vols.len(), volumes.len());

    simulate_workload(&workload, 40, true);
    assert_eq!(workload.log().acked_write_count(), 100);

    let reports = verify_consistency(workload.log(), 50);
    assert!(
        reports.iter().all(|r| r.result.is_pass()),
        "post-rebalance consistency failed"
    );
}

// ---------------------------------------------------------------------------
// P9D.2 — Remove node (drain) → all volumes migrated → no data loss
// (Placeholder: pending Phase 7 real rebalancing implementation)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_remove_node_drain_no_data_loss() {
    let mut cluster = ProcessCluster::new(cluster_config(5, 45000));
    assert_eq!(cluster.node_count(), 5);

    let volumes: Vec<VolumeId> = (0..3).map(|_| VolumeId::generate()).collect();
    let workload = workload_for_volumes(&volumes, 0.8);
    simulate_workload(&workload, 80, true);

    let drain_target = NodeId(4);
    let remaining: Vec<NodeId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != drain_target)
        .collect();

    let migrated = simulate_drain(drain_target, &remaining, &volumes);
    assert_eq!(migrated.len(), volumes.len());

    cluster.remove_node(drain_target).unwrap();
    assert_eq!(cluster.node_count(), 4);

    simulate_workload(&workload, 40, true);

    let reports = verify_consistency(workload.log(), 50);
    assert!(
        reports.iter().all(|r| r.result.is_pass()),
        "post-drain consistency failed"
    );
}

// ---------------------------------------------------------------------------
// P9D.3 — Kill node during rebalance → rebalance resumes after recovery
// (Placeholder: pending Phase 7 real rebalancing implementation)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_kill_during_rebalance_resumes() {
    let mut cluster = ProcessCluster::new(cluster_config(4, 46000));
    let vol = VolumeId::generate();
    let workload = workload_for_volumes(&[vol], 1.0);
    simulate_workload(&workload, 40, true);

    let new_node_id = cluster.add_node();
    assert_eq!(cluster.node_count(), 5);

    let original_nodes: Vec<NodeId> = (0..4).map(NodeId).collect();
    let (moved, _) = simulate_rebalance(&original_nodes, &[new_node_id], &[vol]);
    assert!(moved > 0);

    simulate_workload(&workload, 30, true);
    assert_eq!(workload.log().acked_write_count(), 70);

    let reports = verify_consistency(workload.log(), 40);
    assert!(
        reports.iter().all(|r| r.result.is_pass()),
        "consistency failed after simulated kill-during-rebalance"
    );
}

// ---------------------------------------------------------------------------
// P9D.4 — Concurrent client IO during rebalance: no errors
// (Placeholder: pending Phase 7 real rebalancing implementation)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_concurrent_io_during_rebalance() {
    let mut cluster = ProcessCluster::new(cluster_config(3, 47000));
    let volumes: Vec<VolumeId> = (0..2).map(|_| VolumeId::generate()).collect();
    let workload = workload_for_volumes(&volumes, 0.7);

    simulate_workload(&workload, 50, true);

    let new_node = cluster.add_node();
    let original_nodes: Vec<NodeId> = (0..3).map(NodeId).collect();

    let batch_size = 10;
    for batch in 0..10 {
        simulate_workload(&workload, batch_size, true);
        if batch == 5 {
            let (moved, _) = simulate_rebalance(&original_nodes, &[new_node], &volumes);
            assert!(moved > 0);
        }
    }

    assert_eq!(
        workload.log().failed_operations().len(),
        0,
        "concurrent IO during rebalance must have zero errors"
    );

    let reports = verify_consistency(workload.log(), 50);
    assert!(
        reports.iter().all(|r| r.result.is_pass()),
        "consistency failed during concurrent IO + rebalance"
    );
}
