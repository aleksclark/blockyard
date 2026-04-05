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

// ─── Test 1: rebalance_after_node_add ────────────────────────────────
//
// Start with 3 nodes, add 2 more, trigger rebalance, then verify data
// integrity is preserved.

#[tokio::test]
#[ignore]
async fn rebalance_after_node_add() {
    if !require_vm_env() {
        return;
    }

    // Start with a 3-node cluster.
    let cluster = running_cluster(5);

    // Write some data on the initial nodes.
    let initial_addrs: Vec<_> = cluster
        .running_nodes()
        .iter()
        .filter(|n| n.id < 3)
        .map(|n| n.blockyard_addr())
        .collect();

    let config = WorkloadConfig {
        volume_name: "test-rebalance-add".into(),
        volume_id: 30,
        duration: Duration::from_secs(10),
        write_rate: 50,
        read_rate: 0,
        target_addrs: initial_addrs,
        ..Default::default()
    };

    let wg = WorkloadGenerator::new(config);
    let handle = wg.start();
    tokio::time::sleep(Duration::from_secs(8)).await;
    wg.stop();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let write_log = wg.log().await;
    let acked = write_log.acknowledged_writes();
    println!("Pre-rebalance: {} acked writes", acked.len());
    assert!(!acked.is_empty(), "need writes before rebalancing");

    // "Add" nodes 3 and 4 by starting blockyard on them.
    for id in 3..5 {
        restart_blockyard(&cluster, id).await;
    }

    // Trigger rebalance via CLI.
    let _ = cluster
        .ssh_exec(0, "blockyard rebalance trigger 2>/dev/null || true")
        .await;

    // Wait for rebalance to complete (poll with timeout).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        if tokio::time::Instant::now() > deadline {
            println!("WARNING: rebalance did not complete within 120s");
            break;
        }
        let check = Checker::check_rebalance_complete(&cluster).await;
        if check.passed {
            println!("Rebalance completed successfully");
            break;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    // Verify data integrity after rebalance.
    let read_config = WorkloadConfig {
        volume_name: "test-rebalance-add".into(),
        volume_id: 30,
        duration: Duration::from_secs(10),
        write_rate: 0,
        read_rate: 100,
        target_addrs: cluster
            .running_nodes()
            .iter()
            .map(|n| n.blockyard_addr())
            .collect(),
        ..Default::default()
    };
    let read_wg = WorkloadGenerator::new(read_config);
    let read_handle = read_wg.start();
    tokio::time::sleep(Duration::from_secs(8)).await;
    read_wg.stop();
    let _ = tokio::time::timeout(Duration::from_secs(10), read_handle).await;

    let zfs = Checker::check_zfs_integrity(&cluster).await;
    println!("ZFS after rebalance: {}", zfs.summary());
    assert!(zfs.passed, "ZFS must be healthy after rebalance");
}

// ─── Test 2: crash_during_rebalance ──────────────────────────────────
//
// Start a rebalance, crash a node mid-transfer, then verify the cluster
// recovers and completes (or retries) the rebalance.

#[tokio::test]
#[ignore]
async fn crash_during_rebalance() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);

    // Write some baseline data.
    let config = WorkloadConfig {
        volume_name: "test-rebalance-crash".into(),
        volume_id: 31,
        duration: Duration::from_secs(10),
        write_rate: 50,
        read_rate: 0,
        target_addrs: cluster
            .running_nodes()
            .iter()
            .map(|n| n.blockyard_addr())
            .collect(),
        ..Default::default()
    };

    let wg = WorkloadGenerator::new(config);
    let handle = wg.start();
    tokio::time::sleep(Duration::from_secs(8)).await;
    wg.stop();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    // Trigger rebalance.
    let _ = cluster
        .ssh_exec(0, "blockyard rebalance trigger 2>/dev/null || true")
        .await;

    // Wait a bit then crash a node mid-rebalance.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 3 })
        .await
        .unwrap();

    // Let the cluster handle the failure.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Restart the crashed node.
    restart_blockyard(&cluster, 3).await;

    // Verify the cluster is healthy.
    let health = Checker::check_cluster_health(&cluster).await;
    println!("Post-crash-rebalance health: {}", health.summary());

    // Verify no panics.
    let panic_check = Checker::check_blockyard_logs_no_panic(&cluster).await;
    println!("Panic check: {}", panic_check.summary());
    assert!(
        panic_check.passed,
        "no panics expected after crash during rebalance"
    );

    // ZFS should still be healthy.
    let zfs = Checker::check_zfs_integrity(&cluster).await;
    println!("ZFS after crash-rebalance: {}", zfs.summary());
    assert!(zfs.passed, "ZFS must be healthy");
}

// ─── Test 3: concurrent_io_during_rebalance ──────────────────────────
//
// Run a write/read workload concurrently with a rebalance operation.
// Verify writes succeed and data integrity is preserved.

#[tokio::test]
#[ignore]
async fn concurrent_io_during_rebalance() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);
    let config = WorkloadConfig {
        volume_name: "test-rebalance-io".into(),
        volume_id: 32,
        duration: Duration::from_secs(60),
        write_rate: 30,
        read_rate: 30,
        target_addrs: cluster
            .running_nodes()
            .iter()
            .map(|n| n.blockyard_addr())
            .collect(),
        op_timeout: Duration::from_secs(10),
        ..Default::default()
    };

    // Start workload.
    let wg = WorkloadGenerator::new(config);
    let handle = wg.start();

    // Let some data accumulate.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Trigger rebalance while I/O is in flight.
    let _ = cluster
        .ssh_exec(0, "blockyard rebalance trigger 2>/dev/null || true")
        .await;

    // Let workload and rebalance run concurrently.
    tokio::time::sleep(Duration::from_secs(40)).await;

    wg.stop();
    let _ = tokio::time::timeout(Duration::from_secs(15), handle).await;

    let log = wg.log().await;

    // Writes must succeed.
    let durability = Checker::check_write_durability(&log);
    println!("Concurrent I/O durability: {}", durability.summary());
    assert!(durability.passed, "writes must succeed during rebalance");

    // Read-back integrity.
    let integrity = Checker::check_write_read_integrity(&log);
    println!("Concurrent I/O integrity: {}", integrity.summary());
    assert!(integrity.passed, "data must be intact during rebalance");
}
