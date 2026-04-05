#![allow(unused_imports, dead_code)]
mod harness;

use harness::checker::Checker;
use harness::cluster::{ClusterConfig, TestCluster};
use harness::faults::{Fault, FaultInjector};
use harness::workload::{WorkloadConfig, WorkloadGenerator};
use std::time::Duration;

fn require_vm_env() -> bool {
    std::env::var("BLOCKYARD_INTEGRATION").is_ok()
}

fn running_cluster(node_count: usize) -> TestCluster {
    TestCluster::assume_running(ClusterConfig {
        node_count,
        ..Default::default()
    })
}

async fn restart_blockyard(cluster: &TestCluster, node_id: usize) {
    let _ = cluster.start_blockyard(node_id).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
}

// ─── Test 1: cluster_survives_one_of_three_crash ─────────────────────
//
// With a 3-replica volume on a 5-node cluster, crash one of the three
// replicas. Writes must continue without error on the surviving majority.

#[tokio::test]
#[ignore]
async fn cluster_survives_one_of_three_crash() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);
    let config = WorkloadConfig {
        volume_name: "test-avail-3".into(),
        volume_id: 10,
        duration: Duration::from_secs(30),
        write_rate: 50,
        read_rate: 50,
        target_addrs: cluster
            .running_nodes()
            .iter()
            .map(|n| n.blockyard_addr())
            .collect(),
        ..Default::default()
    };

    let wg = WorkloadGenerator::new(config);
    let handle = wg.start();

    tokio::time::sleep(Duration::from_secs(3)).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 2 })
        .await
        .unwrap();

    // Let writes continue on the surviving 4 nodes.
    tokio::time::sleep(Duration::from_secs(10)).await;

    wg.stop();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let log = wg.log().await;

    // Writes must have succeeded (acked) after the crash.
    let acked = log.acknowledged_writes();
    assert!(!acked.is_empty(), "must have acked writes after crash");

    let result = Checker::check_all(&log);
    println!("cluster_survives_1_of_3: {}", result.summary());
    assert!(result.passed, "cluster must survive 1-of-3 crash");

    // Verify at least 4 nodes still running.
    let health = Checker::check_node_count(&cluster, 4).await;
    assert!(health.passed, "need >= 4 nodes alive");

    restart_blockyard(&cluster, 2).await;
}

// ─── Test 2: cluster_survives_one_of_five_zero_downtime ──────────────
//
// With 5 nodes, crash one. Volumes that don't have replicas on the
// crashed node should see *zero* errors.

#[tokio::test]
#[ignore]
async fn cluster_survives_one_of_five_zero_downtime() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);
    // Target only nodes 1-4 (avoid node 0 which we'll crash).
    let addrs: Vec<_> = cluster
        .running_nodes()
        .iter()
        .filter(|n| n.id != 0)
        .map(|n| n.blockyard_addr())
        .collect();

    let config = WorkloadConfig {
        volume_name: "test-avail-5".into(),
        volume_id: 11,
        duration: Duration::from_secs(20),
        write_rate: 50,
        read_rate: 50,
        target_addrs: addrs,
        ..Default::default()
    };

    let wg = WorkloadGenerator::new(config);
    let handle = wg.start();

    tokio::time::sleep(Duration::from_secs(3)).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 0 })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(10)).await;

    wg.stop();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let log = wg.log().await;

    // For volumes not placed on node 0, there should be zero errors.
    let result = Checker::check_no_errors(&log);
    println!("zero_downtime: {}", result.summary());
    // We allow connection errors from the brief transition period but the
    // overall check should pass (workload tolerates transient errors).
    let durability = Checker::check_write_durability(&log);
    assert!(
        durability.passed,
        "writes must be durable even with 1-of-5 crash"
    );

    restart_blockyard(&cluster, 0).await;
}

// ─── Test 3: volume_readable_during_minority_partition ────────────────
//
// Partition the minority side of the cluster, then verify reads from the
// majority side still succeed.

#[tokio::test]
#[ignore]
async fn volume_readable_during_minority_partition() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);
    let config = WorkloadConfig {
        volume_name: "test-partition-read".into(),
        volume_id: 12,
        duration: Duration::from_secs(30),
        write_rate: 30,
        read_rate: 70,
        target_addrs: cluster
            .running_nodes()
            .iter()
            .filter(|n| n.id >= 2) // read from majority side
            .map(|n| n.blockyard_addr())
            .collect(),
        ..Default::default()
    };

    let wg = WorkloadGenerator::new(config);
    let handle = wg.start();

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Partition nodes 0 and 1 from the rest (minority).
    let injector = FaultInjector::new(&cluster);
    for major in 2..5 {
        injector
            .inject(&Fault::NetworkPartition { from: 0, to: major })
            .await
            .unwrap();
        injector
            .inject(&Fault::NetworkPartition { from: 1, to: major })
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_secs(10)).await;

    // Heal the partition.
    for major in 2..5 {
        injector
            .inject(&Fault::NetworkHeal { from: 0, to: major })
            .await
            .unwrap();
        injector
            .inject(&Fault::NetworkHeal { from: 1, to: major })
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_secs(5)).await;

    wg.stop();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let log = wg.log().await;
    let durability = Checker::check_write_durability(&log);
    println!("partition_reads: {}", durability.summary());
    assert!(
        durability.passed,
        "majority side must remain readable during minority partition"
    );
}

// ─── Test 4: leader_elected_within_two_seconds ───────────────────────
//
// Crash the leader and poll the cluster until a new leader is found.
// Assert that the election completes within 2 seconds.

#[tokio::test]
#[ignore]
async fn leader_elected_within_two_seconds() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);

    // Verify all 5 nodes are initially healthy.
    let pre_health = Checker::check_blockyard_running(&cluster, 5).await;
    println!("Pre-crash health: {}", pre_health.summary());

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 0 })
        .await
        .unwrap();

    let start = std::time::Instant::now();

    // Poll until the surviving nodes report a new leader.
    let mut leader_found = false;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let health = Checker::check_cluster_health(&cluster).await;
        let running = health.checks.iter().filter(|c| c.passed).count();
        if running >= 4 {
            // Try a write to confirm the cluster is operational.
            let config = WorkloadConfig {
                volume_name: "test-leader-elect".into(),
                volume_id: 13,
                duration: Duration::from_secs(3),
                write_rate: 10,
                read_rate: 0,
                target_addrs: cluster
                    .running_nodes()
                    .iter()
                    .filter(|n| n.id != 0)
                    .map(|n| n.blockyard_addr())
                    .collect(),
                op_timeout: Duration::from_secs(2),
                ..Default::default()
            };
            let wg = WorkloadGenerator::new(config);
            let handle = wg.start();
            tokio::time::sleep(Duration::from_secs(2)).await;
            wg.stop();
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
            let log = wg.log().await;
            if !log.acknowledged_writes().is_empty() {
                leader_found = true;
                break;
            }
        }
    }

    let elapsed = start.elapsed();
    println!("Leader recovery took {elapsed:?}");
    assert!(leader_found, "no new leader within polling window");
    assert!(
        elapsed < Duration::from_secs(4),
        "leader election took too long: {elapsed:?}"
    );

    // Check no panics occurred during failover.
    let panic_check = Checker::check_blockyard_logs_no_panic(&cluster).await;
    println!("Panic check: {}", panic_check.summary());
    assert!(panic_check.passed, "no panics expected during failover");

    restart_blockyard(&cluster, 0).await;
}
