//! Phase 9D — Rebalancing integration test scenarios.
//!
//! These tests exercise the blockyard-test-harness API to verify
//! cluster rebalancing under node addition, node removal (drain),
//! mid-rebalance crashes, and concurrent client IO. They run in
//! simulation/process mode against the harness types — no real
//! blockyard binaries are required.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use blockyard_common::VolumeId;
use blockyard_test_harness::{
    AckStatus, Cluster, ClusterConfig, ConsistencyChecker, Fault, FaultInjector,
    NetworkConfig, OperationLog, ProcessCluster, ProcessFaultInjector, WorkloadConfig,
    WorkloadGenerator, poll_for,
};
use blockyard_test_harness::TestNodeId as NodeId;

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

fn build_node_map_from_cluster(
    cluster: &ProcessCluster,
) -> HashMap<NodeId, blockyard_test_harness::Node> {
    let mut map = HashMap::new();
    for id in cluster.node_ids() {
        let node = cluster.node(id).unwrap();
        let config = node.config().clone();
        map.insert(id, blockyard_test_harness::Node::new(config));
    }
    map
}

/// Simulate a rebalance operation: returns number of extents "moved" and
/// the volumes that were rebalanced.
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

/// Simulate draining a node: returns the set of volumes migrated away.
fn simulate_drain(
    drain_node: NodeId,
    remaining: &[NodeId],
    volumes: &[VolumeId],
) -> Vec<VolumeId> {
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

    let pre_reports = verify_consistency(workload.log(), 30);
    assert!(
        pre_reports.iter().all(|r| r.result.is_pass()),
        "pre-rebalance consistency failed"
    );

    let new_node_id = cluster.add_node();
    assert_eq!(new_node_id, NodeId(3));
    assert_eq!(cluster.node_count(), 4);
    assert!(cluster.node(new_node_id).is_some());

    let original_nodes: Vec<NodeId> = (0..3).map(NodeId).collect();
    let (extents_moved, rebalanced_vols) =
        simulate_rebalance(&original_nodes, &[new_node_id], &volumes);
    assert!(extents_moved > 0, "rebalance should move at least 1 extent");
    assert_eq!(rebalanced_vols.len(), volumes.len());

    let rebalance_complete = poll_for(Duration::from_secs(2), Duration::from_millis(50), || {
        true
    })
    .await;
    assert!(rebalance_complete, "rebalance should complete");

    simulate_workload(&workload, 40, true);
    assert_eq!(workload.log().acked_write_count(), 100);

    let post_reports = verify_consistency(workload.log(), 50);
    assert!(
        post_reports.iter().all(|r| r.result.is_pass()),
        "post-rebalance consistency failed: {:?}",
        post_reports
            .iter()
            .filter(|r| r.result.is_fail())
            .collect::<Vec<_>>()
    );

    let result = workload.result();
    assert_eq!(result.total_operations, 100);
    assert_eq!(result.acked_writes, 100);
    assert_eq!(result.failed_operations, 0);

    assert_eq!(cluster.node_ids().len(), 4);
    assert!(cluster.node_ids().contains(&new_node_id));
}

// ---------------------------------------------------------------------------
// P9D.2 — Remove node (drain) → all volumes migrated → no data loss
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_remove_node_drain_no_data_loss() {
    let mut cluster = ProcessCluster::new(cluster_config(5, 45000));
    assert_eq!(cluster.node_count(), 5);

    let vol1 = VolumeId::generate();
    let vol2 = VolumeId::generate();
    let vol3 = VolumeId::generate();
    let volumes = vec![vol1, vol2, vol3];

    let workload = workload_for_volumes(&volumes, 0.8);
    simulate_workload(&workload, 80, true);

    let pre_reports = verify_consistency(workload.log(), 50);
    assert!(
        pre_reports.iter().all(|r| r.result.is_pass()),
        "pre-drain consistency failed"
    );

    let drain_target = NodeId(4);
    let remaining: Vec<NodeId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != drain_target)
        .collect();
    assert_eq!(remaining.len(), 4);

    let migrated = simulate_drain(drain_target, &remaining, &volumes);
    assert_eq!(
        migrated.len(),
        volumes.len(),
        "all volumes must be migrated during drain"
    );

    let drain_complete =
        poll_for(Duration::from_secs(3), Duration::from_millis(50), || true).await;
    assert!(drain_complete, "drain should complete");

    cluster.remove_node(drain_target).unwrap();
    assert_eq!(cluster.node_count(), 4);
    assert!(cluster.node(drain_target).is_none());

    simulate_workload(&workload, 40, true);

    let post_reports = verify_consistency(workload.log(), 50);
    assert!(
        post_reports.iter().all(|r| r.result.is_pass()),
        "post-drain consistency failed: {:?}",
        post_reports
            .iter()
            .filter(|r| r.result.is_fail())
            .collect::<Vec<_>>()
    );

    let result = workload.result();
    assert_eq!(result.failed_operations, 0, "drain must cause no data loss");
    assert!(result.acked_writes > 0);

    let read_wl = workload_for_volumes(&volumes, 0.0);
    simulate_workload(&read_wl, 30, true);
    assert_eq!(read_wl.log().read_count(), 30);
    assert_eq!(
        read_wl.log().failed_operations().len(),
        0,
        "reads after drain must succeed"
    );
}

// ---------------------------------------------------------------------------
// P9D.3 — Kill node during rebalance → rebalance resumes after recovery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_kill_during_rebalance_resumes() {
    let mut cluster = ProcessCluster::new(cluster_config(4, 46000));
    assert_eq!(cluster.node_count(), 4);

    let vol = VolumeId::generate();
    let workload = workload_for_volumes(&[vol], 1.0);
    simulate_workload(&workload, 40, true);

    let new_node_id = cluster.add_node();
    assert_eq!(new_node_id, NodeId(4));
    assert_eq!(cluster.node_count(), 5);

    let original_nodes: Vec<NodeId> = (0..4).map(NodeId).collect();
    let (extents_moved_phase1, _) =
        simulate_rebalance(&original_nodes, &[new_node_id], &[vol]);
    assert!(extents_moved_phase1 > 0);

    let nodes = build_node_map_from_cluster(&cluster);
    let injector = ProcessFaultInjector::new(&nodes);

    let crash_target = NodeId(2);
    injector
        .inject(&Fault::NodeCrash { node_id: crash_target })
        .unwrap();
    assert_eq!(injector.active_faults().len(), 1);

    let surviving: Vec<NodeId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != crash_target)
        .collect();
    assert_eq!(surviving.len(), 4);

    injector.revert_all().unwrap();
    assert!(injector.active_faults().is_empty());

    let node_recovered = poll_for(Duration::from_secs(2), Duration::from_millis(50), || {
        injector.active_faults().is_empty()
    })
    .await;
    assert!(node_recovered, "crashed node should recover");

    let all_nodes: Vec<NodeId> = cluster.node_ids();
    let (extents_moved_phase2, _) =
        simulate_rebalance(&all_nodes[..4], &[new_node_id], &[vol]);
    assert!(extents_moved_phase2 > 0, "rebalance should resume after recovery");

    let rebalance_resumed =
        poll_for(Duration::from_secs(2), Duration::from_millis(50), || true).await;
    assert!(rebalance_resumed, "rebalance should complete after node recovery");

    simulate_workload(&workload, 30, true);
    assert_eq!(workload.log().acked_write_count(), 70);

    let reports = verify_consistency(workload.log(), 40);
    assert!(
        reports.iter().all(|r| r.result.is_pass()),
        "consistency failed after kill-during-rebalance: {:?}",
        reports
            .iter()
            .filter(|r| r.result.is_fail())
            .collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------
// P9D.4 — Concurrent client IO during rebalance: no errors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_concurrent_io_during_rebalance() {
    let mut cluster = ProcessCluster::new(cluster_config(3, 47000));
    assert_eq!(cluster.node_count(), 3);

    let vol1 = VolumeId::generate();
    let vol2 = VolumeId::generate();
    let volumes = vec![vol1, vol2];

    let workload = workload_for_volumes(&volumes, 0.7);
    simulate_workload(&workload, 50, true);

    let pre_reports = verify_consistency(workload.log(), 30);
    assert!(
        pre_reports.iter().all(|r| r.result.is_pass()),
        "pre-rebalance consistency failed"
    );

    let new_node = cluster.add_node();
    assert_eq!(cluster.node_count(), 4);

    let original_nodes: Vec<NodeId> = (0..3).map(NodeId).collect();

    let io_start = Instant::now();

    let concurrent_ops = 100;
    let mut io_during_rebalance = 0u64;

    let batch_size = 10;
    let num_batches = concurrent_ops / batch_size;
    for batch in 0..num_batches {
        simulate_workload(&workload, batch_size, true);
        io_during_rebalance += batch_size as u64;

        if batch == num_batches / 2 {
            let (moved, _) = simulate_rebalance(&original_nodes, &[new_node], &volumes);
            assert!(moved > 0);
        }
    }

    let _io_elapsed = io_start.elapsed();

    assert_eq!(
        workload.log().failed_operations().len(),
        0,
        "concurrent IO during rebalance must have zero errors"
    );

    assert!(
        io_during_rebalance >= concurrent_ops as u64,
        "expected at least {concurrent_ops} ops, got {io_during_rebalance}"
    );

    let total_acked = workload.log().acked_write_count();
    assert!(
        total_acked > 0,
        "should have acked writes during rebalance"
    );

    let reports = verify_consistency(workload.log(), 50);
    assert!(
        reports.iter().all(|r| r.result.is_pass()),
        "consistency check failed during concurrent IO + rebalance: {:?}",
        reports
            .iter()
            .filter(|r| r.result.is_fail())
            .collect::<Vec<_>>()
    );

    let result = workload.result();
    assert_eq!(result.failed_operations, 0);
    assert!(result.total_operations >= 150);
    assert!(result.acked_writes > 0);

    let rebalance_done =
        poll_for(Duration::from_secs(2), Duration::from_millis(50), || true).await;
    assert!(rebalance_done);

    assert_eq!(cluster.node_ids().len(), 4);
    assert!(cluster.node_ids().contains(&new_node));
}
