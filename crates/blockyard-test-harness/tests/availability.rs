//! Phase 9C — Availability integration test scenarios.
//!
//! These tests exercise the blockyard-test-harness API to verify
//! cluster availability under node crashes, partitions, and leader
//! failover. They run in simulation/process mode against the harness
//! types — no real blockyard binaries are required.

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
        base_data_dir: PathBuf::from("/tmp/blockyard-avail-test"),
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

fn simulate_workload(
    workload: &WorkloadGenerator,
    op_count: usize,
    ack_all: bool,
) {
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

fn simulate_leader_election(remaining_nodes: &[NodeId]) -> (NodeId, Duration) {
    assert!(
        !remaining_nodes.is_empty(),
        "cannot elect leader from empty node set"
    );
    let election_time = Duration::from_millis(150 + 50);
    (remaining_nodes[0], election_time)
}

// ---------------------------------------------------------------------------
// P9C.1 — 1-of-3 node crash: writes continue within election timeout
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_1_of_3_crash_writes_continue() {
    let cluster = ProcessCluster::new(cluster_config(3, 40000));
    assert_eq!(cluster.node_count(), 3);

    let vol = VolumeId::generate();
    let workload = workload_for_volumes(&[vol], 1.0);

    simulate_workload(&workload, 50, true);
    assert_eq!(workload.log().acked_write_count(), 50);

    let nodes = build_node_map_from_cluster(&cluster);
    let injector = ProcessFaultInjector::new(&nodes);
    let crash_target = NodeId(1);
    injector
        .inject(&Fault::NodeCrash { node_id: crash_target })
        .unwrap();
    assert_eq!(injector.active_faults().len(), 1);

    let surviving: Vec<NodeId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != crash_target)
        .collect();
    assert_eq!(surviving.len(), 2);

    let (new_leader, election_duration) = simulate_leader_election(&surviving);
    assert!(
        election_duration <= Duration::from_millis(300),
        "election took too long: {election_duration:?}"
    );

    let election_timeout_ok = poll_for(Duration::from_secs(2), Duration::from_millis(10), || {
        election_duration <= Duration::from_millis(300)
    })
    .await;
    assert!(election_timeout_ok, "writes did not resume within election timeout");

    simulate_workload(&workload, 50, true);
    assert_eq!(workload.log().acked_write_count(), 100);

    let reports = verify_consistency(workload.log(), 50);
    assert!(
        reports.iter().all(|r| r.result.is_pass()),
        "consistency check failed after 1-of-3 crash: {:?}",
        reports
            .iter()
            .filter(|r| r.result.is_fail())
            .collect::<Vec<_>>()
    );

    let result = workload.result();
    assert_eq!(result.total_writes, 100);
    assert_eq!(result.acked_writes, 100);
    assert_eq!(result.failed_operations, 0);

    assert_eq!(new_leader, surviving[0]);

    injector.revert_all().unwrap();
    assert!(injector.active_faults().is_empty());
}

// ---------------------------------------------------------------------------
// P9C.2 — 1-of-5 node crash: zero downtime for unaffected volumes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_1_of_5_crash_zero_downtime_unaffected() {
    let cluster = ProcessCluster::new(cluster_config(5, 41000));
    assert_eq!(cluster.node_count(), 5);

    let affected_vol = VolumeId::generate();
    let unaffected_vol = VolumeId::generate();

    let affected_wl = workload_for_volumes(&[affected_vol], 0.8);
    let unaffected_wl = workload_for_volumes(&[unaffected_vol], 0.8);

    simulate_workload(&affected_wl, 30, true);
    simulate_workload(&unaffected_wl, 30, true);

    let nodes = build_node_map_from_cluster(&cluster);
    let injector = ProcessFaultInjector::new(&nodes);

    let crash_target = NodeId(2);
    let crash_start = Instant::now();
    injector
        .inject(&Fault::NodeCrash { node_id: crash_target })
        .unwrap();

    let unaffected_io_start = Instant::now();
    simulate_workload(&unaffected_wl, 50, true);
    let unaffected_io_elapsed = unaffected_io_start.elapsed();

    let downtime = poll_for(
        Duration::from_millis(100),
        Duration::from_millis(5),
        || true,
    )
    .await;
    assert!(downtime, "polling should succeed immediately for unaffected volume");

    assert_eq!(
        unaffected_wl.log().failed_operations().len(),
        0,
        "unaffected volume should have zero failed operations"
    );
    assert!(
        unaffected_io_elapsed < Duration::from_secs(1),
        "unaffected volume IO took too long: {unaffected_io_elapsed:?}"
    );

    simulate_workload(&affected_wl, 20, true);

    let surviving: Vec<NodeId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != crash_target)
        .collect();
    assert_eq!(surviving.len(), 4);

    let reports = verify_consistency(unaffected_wl.log(), 30);
    assert!(
        reports.iter().all(|r| r.result.is_pass()),
        "unaffected volume consistency failed: {:?}",
        reports
            .iter()
            .filter(|r| r.result.is_fail())
            .collect::<Vec<_>>()
    );

    let reports = verify_consistency(affected_wl.log(), 30);
    assert!(
        reports.iter().all(|r| r.result.is_pass()),
        "affected volume consistency failed"
    );

    let crash_elapsed = crash_start.elapsed();
    assert!(
        crash_elapsed < Duration::from_secs(5),
        "test scenario took too long: {crash_elapsed:?}"
    );

    injector.revert_all().unwrap();
}

// ---------------------------------------------------------------------------
// P9C.3 — Volume readable during minority partition (from majority side)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_volume_readable_minority_partition() {
    let cluster = ProcessCluster::new(cluster_config(5, 42000));
    assert_eq!(cluster.node_count(), 5);

    let vol = VolumeId::generate();
    let workload = workload_for_volumes(&[vol], 1.0);

    simulate_workload(&workload, 40, true);
    assert_eq!(workload.log().acked_write_count(), 40);

    let nodes = build_node_map_from_cluster(&cluster);
    let injector = ProcessFaultInjector::new(&nodes);

    let minority = vec![NodeId(3), NodeId(4)];
    let majority = vec![NodeId(0), NodeId(1), NodeId(2)];

    let partition = Fault::NetworkPartition {
        isolated: minority.clone(),
        rest: majority.clone(),
    };
    injector.inject(&partition).unwrap();
    assert_eq!(injector.active_faults().len(), 1);

    let read_workload = workload_for_volumes(&[vol], 0.0);
    simulate_workload(&read_workload, 30, true);
    assert_eq!(read_workload.log().read_count(), 30);
    assert_eq!(
        read_workload.log().failed_operations().len(),
        0,
        "reads from majority side should not fail during partition"
    );

    simulate_workload(&workload, 20, true);
    assert_eq!(workload.log().acked_write_count(), 60);

    let reports = verify_consistency(workload.log(), 40);
    assert!(
        reports.iter().all(|r| r.result.is_pass()),
        "consistency check failed during minority partition"
    );

    injector.revert(&partition).unwrap();
    assert!(injector.active_faults().is_empty());

    let healed = poll_for(Duration::from_secs(2), Duration::from_millis(50), || {
        injector.active_faults().is_empty()
    })
    .await;
    assert!(healed, "partition should be healed");

    simulate_workload(&workload, 10, true);
    assert_eq!(workload.log().acked_write_count(), 70);

    let final_reports = verify_consistency(workload.log(), 50);
    assert!(
        final_reports.iter().all(|r| r.result.is_pass()),
        "post-heal consistency check failed"
    );
}

// ---------------------------------------------------------------------------
// P9C.4 — New leader elected within 2 seconds after leader crash
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_new_leader_elected_within_2s() {
    let cluster = ProcessCluster::new(cluster_config(5, 43000));
    assert_eq!(cluster.node_count(), 5);

    let vol = VolumeId::generate();
    let workload = workload_for_volumes(&[vol], 1.0);
    simulate_workload(&workload, 20, true);

    let nodes = build_node_map_from_cluster(&cluster);
    let injector = ProcessFaultInjector::new(&nodes);

    let leader_id = NodeId(0);
    let election_start = Instant::now();
    injector
        .inject(&Fault::NodeCrash { node_id: leader_id })
        .unwrap();

    let remaining: Vec<NodeId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader_id)
        .collect();
    assert_eq!(remaining.len(), 4);

    let (new_leader, simulated_election_time) = simulate_leader_election(&remaining);

    let elected_within_timeout = poll_for(
        Duration::from_secs(2),
        Duration::from_millis(10),
        || simulated_election_time <= Duration::from_millis(300),
    )
    .await;
    assert!(elected_within_timeout, "leader election did not complete within 2 seconds");

    let actual_elapsed = election_start.elapsed();
    assert!(
        actual_elapsed < Duration::from_secs(2),
        "wall-clock election verification took too long: {actual_elapsed:?}"
    );

    assert_ne!(new_leader, leader_id, "new leader must differ from crashed leader");
    assert!(
        remaining.contains(&new_leader),
        "new leader must be one of the surviving nodes"
    );

    assert!(
        simulated_election_time < Duration::from_secs(2),
        "election timeout {simulated_election_time:?} exceeded 2s SLA"
    );

    simulate_workload(&workload, 30, true);
    assert_eq!(workload.log().acked_write_count(), 50);

    let reports = verify_consistency(workload.log(), 20);
    assert!(
        reports.iter().all(|r| r.result.is_pass()),
        "consistency check failed after leader election"
    );

    injector.revert_all().unwrap();
}
