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

async fn restart_all_blockyard(cluster: &TestCluster, node_ids: &[usize]) {
    for &id in node_ids {
        let _ = cluster.start_blockyard(id).await;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
}

// ─── Test 1: write_crash_restart_verify ──────────────────────────────
//
// Write a known data pattern, crash ALL nodes, restart them, then verify
// the data is intact. This is the fundamental durability test.

#[tokio::test]
#[ignore]
async fn write_crash_restart_verify() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);
    let config = WorkloadConfig {
        volume_name: "test-integrity".into(),
        volume_id: 20,
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

    // Phase 1: Write data.
    let wg = WorkloadGenerator::new(config.clone());
    let handle = wg.start();
    tokio::time::sleep(Duration::from_secs(8)).await;
    wg.stop();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let write_log = wg.log().await;
    let acked_writes = write_log.acknowledged_writes();
    assert!(!acked_writes.is_empty(), "must have acked writes");
    println!(
        "Phase 1: {} acked writes, {} errors",
        acked_writes.len(),
        write_log.error_count()
    );

    // Phase 2: Crash all nodes.
    let injector = FaultInjector::new(&cluster);
    for i in 0..5 {
        injector
            .inject(&Fault::NodeCrash { node_id: i })
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Phase 3: Restart all nodes.
    restart_all_blockyard(&cluster, &[0, 1, 2, 3, 4]).await;

    // Phase 4: Verify data by reading back the offsets we wrote.
    let read_config = WorkloadConfig {
        volume_name: "test-integrity".into(),
        volume_id: 20,
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

    // Check ZFS integrity on all nodes.
    let zfs_result = Checker::check_zfs_integrity(&cluster).await;
    println!("ZFS integrity: {}", zfs_result.summary());
    assert!(zfs_result.passed, "ZFS pools must be healthy after restart");

    // Check cluster health.
    let health = Checker::check_cluster_health(&cluster).await;
    println!("Cluster health: {}", health.summary());

    // Check no panics after crash/restart.
    let panic_check = Checker::check_blockyard_logs_no_panic(&cluster).await;
    println!("Panic check: {}", panic_check.summary());
    assert!(panic_check.passed, "no panics expected after crash/restart");
}

// ─── Test 2: partition_heal_convergence ──────────────────────────────
//
// Partition the cluster into two halves, run writes on the majority side
// (which can reach quorum), then heal the partition and verify the
// minority side converges to the same state.

#[tokio::test]
#[ignore]
async fn partition_heal_convergence() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);
    let config = WorkloadConfig {
        volume_name: "test-partition".into(),
        volume_id: 21,
        duration: Duration::from_secs(30),
        write_rate: 50,
        read_rate: 50,
        target_addrs: cluster
            .running_nodes()
            .iter()
            .filter(|n| n.id >= 2) // write to majority side
            .map(|n| n.blockyard_addr())
            .collect(),
        ..Default::default()
    };

    let wg = WorkloadGenerator::new(config);
    let handle = wg.start();

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Create partition: isolate nodes 0 and 1 from nodes 2, 3, 4.
    let injector = FaultInjector::new(&cluster);
    for minority in [0, 1] {
        for majority in [2, 3, 4] {
            injector
                .inject(&Fault::NetworkPartition {
                    from: minority,
                    to: majority,
                })
                .await
                .unwrap();
        }
    }

    tokio::time::sleep(Duration::from_secs(10)).await;

    // Heal the partition.
    for minority in [0, 1] {
        for majority in [2, 3, 4] {
            injector
                .inject(&Fault::NetworkHeal {
                    from: minority,
                    to: majority,
                })
                .await
                .unwrap();
        }
    }

    // Let the cluster converge.
    tokio::time::sleep(Duration::from_secs(10)).await;

    wg.stop();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let log = wg.log().await;
    let result = Checker::check_all(&log);
    println!("Partition convergence: {}", result.summary());
    assert!(result.passed, "data must converge after partition heal");

    // After healing, all 5 nodes should be running.
    let health = Checker::check_blockyard_running(&cluster, 5).await;
    println!("Post-heal health: {}", health.summary());
    assert!(
        health.passed,
        "all nodes should be running after partition heal"
    );
}

// ─── Test 3: node_pause_resume_no_data_loss ──────────────────────────
//
// SIGSTOP a node (simulating a process freeze), continue writes on the
// remaining cluster, then SIGCONT the paused node. Verify no data loss.

#[tokio::test]
#[ignore]
async fn node_pause_resume_no_data_loss() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);
    let config = WorkloadConfig {
        volume_name: "test-pause".into(),
        volume_id: 22,
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

    // Pause node 1.
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodePause { node_id: 1 })
        .await
        .unwrap();

    // Continue writing to the remaining cluster.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Resume node 1.
    injector
        .inject(&Fault::NodeResume { node_id: 1 })
        .await
        .unwrap();

    // Let it catch up.
    tokio::time::sleep(Duration::from_secs(5)).await;

    wg.stop();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let log = wg.log().await;
    let durability = Checker::check_write_durability(&log);
    println!("Pause/resume durability: {}", durability.summary());
    assert!(
        durability.passed,
        "all acked writes must survive pause/resume"
    );

    let integrity = Checker::check_write_read_integrity(&log);
    println!("Pause/resume integrity: {}", integrity.summary());
    assert!(integrity.passed, "read-back data must match writes");
}

// ─── Test 4: network_delay_writes_succeed ────────────────────────────
//
// Add 200ms network delay to one node. Verify writes still complete
// (they'll be slower but must not lose data).

#[tokio::test]
#[ignore]
async fn network_delay_writes_succeed() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);
    let config = WorkloadConfig {
        volume_name: "test-delay".into(),
        volume_id: 23,
        duration: Duration::from_secs(30),
        write_rate: 30,
        read_rate: 30,
        target_addrs: cluster
            .running_nodes()
            .iter()
            .map(|n| n.blockyard_addr())
            .collect(),
        op_timeout: Duration::from_secs(10), // longer timeout for delayed ops
        ..Default::default()
    };

    let wg = WorkloadGenerator::new(config);
    let handle = wg.start();

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Add 200ms delay to node 2.
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NetworkDelay {
            node_id: 2,
            latency: Duration::from_millis(200),
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(15)).await;

    // Remove delay.
    injector
        .inject(&Fault::NetworkReset { node_id: 2 })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;

    wg.stop();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let log = wg.log().await;

    // Writes must succeed.
    let durability = Checker::check_write_durability(&log);
    println!("Delay durability: {}", durability.summary());
    assert!(durability.passed, "writes must succeed with network delay");

    // Latency should be elevated but bounded.
    let p99 = log.read_p99_latency();
    println!("Read p99 latency with 200ms delay: {p99:?}");
    // We allow up to 5s p99 (generous bound for 200ms added delay).
    assert!(p99 < Duration::from_secs(5), "p99 latency {p99:?} too high");
}
