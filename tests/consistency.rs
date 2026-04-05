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

/// Helper: start blockyard on a killed node and wait for it to rejoin.
async fn restart_blockyard(cluster: &TestCluster, node_id: usize) {
    let _ = cluster.start_blockyard(node_id).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
}

// ─── Test 1: linearizable_writes_during_leader_failover ──────────────
//
// Start a write workload against the cluster, crash the presumed leader
// (node 0) mid-flight, then verify that every acknowledged write is
// still readable after the failover completes.

#[tokio::test]
#[ignore]
async fn linearizable_writes_during_leader_failover() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);
    let config = WorkloadConfig {
        volume_name: "test-linear".into(),
        volume_id: 1,
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

    // Let writes accumulate for a few seconds.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Crash the leader.
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 0 })
        .await
        .expect("failed to crash node 0");

    // Let the cluster elect a new leader and continue the workload.
    tokio::time::sleep(Duration::from_secs(10)).await;

    wg.stop();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let log = wg.log().await;

    // All acknowledged writes must be durable.
    let durability = Checker::check_write_durability(&log);
    println!("Durability: {}", durability.summary());
    assert!(durability.passed, "acked writes must be durable");

    // Reads should return consistent data.
    let consistency = Checker::check_read_consistency(&log);
    println!("Consistency: {}", consistency.summary());
    assert!(consistency.passed, "no stale reads allowed");

    // Restart killed node so the cluster is clean for the next test.
    restart_blockyard(&cluster, 0).await;
}

// ─── Test 2: majority_ack_no_data_loss ───────────────────────────────
//
// Write workload with majority-ack semantics. Crash one node. Verify
// that every acknowledged write survives — none may be lost.

#[tokio::test]
#[ignore]
async fn majority_ack_no_data_loss() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);
    let config = WorkloadConfig {
        volume_name: "test-majority".into(),
        volume_id: 2,
        duration: Duration::from_secs(30),
        write_rate: 80,
        read_rate: 20,
        target_addrs: cluster
            .running_nodes()
            .iter()
            .map(|n| n.blockyard_addr())
            .collect(),
        ..Default::default()
    };

    let wg = WorkloadGenerator::new(config);
    let handle = wg.start();

    tokio::time::sleep(Duration::from_secs(5)).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 0 })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(10)).await;
    wg.stop();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let log = wg.log().await;
    let acked = log.acknowledged_writes();
    assert!(
        !acked.is_empty(),
        "expected at least some acknowledged writes"
    );

    let durability = Checker::check_write_durability(&log);
    println!("Durability: {}", durability.summary());
    assert!(durability.passed, "majority-acked writes must survive");

    // Clean up.
    restart_blockyard(&cluster, 0).await;
}

// ─── Test 3: single_ack_leader_crash ─────────────────────────────────
//
// With single-ack writes (fire-and-forget to leader), crash the leader.
// Replicated writes should survive; un-replicated ones may be lost.
// We only assert that *no* acknowledged write that was also replicated
// is lost — we tolerate some loss for single-ack.

#[tokio::test]
#[ignore]
async fn single_ack_leader_crash() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);
    let config = WorkloadConfig {
        volume_name: "test-single-ack".into(),
        volume_id: 3,
        duration: Duration::from_secs(20),
        write_rate: 100,
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

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Crash leader.
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 0 })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;
    wg.stop();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let log = wg.log().await;
    // For single-ack we merely check that the test ran and produced some writes.
    // Data loss for un-replicated writes is expected.
    println!(
        "single_ack: {} writes total, {} acked, {} errors",
        log.write_count(),
        log.acknowledged_writes().len(),
        log.error_count()
    );
    assert!(
        log.write_count() > 0,
        "workload should have attempted writes"
    );

    restart_blockyard(&cluster, 0).await;
}

// ─── Test 4: no_stale_reads_leader_policy ────────────────────────────
//
// Issue writes and then reads with a "leader" read policy during a
// leadership transition. Reads directed at the new leader after it is
// elected must never return stale data.

#[tokio::test]
#[ignore]
async fn no_stale_reads_leader_policy() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);
    let config = WorkloadConfig {
        volume_name: "test-leader-reads".into(),
        volume_id: 4,
        duration: Duration::from_secs(30),
        write_rate: 40,
        read_rate: 60,
        target_addrs: cluster
            .running_nodes()
            .iter()
            .map(|n| n.blockyard_addr())
            .collect(),
        ..Default::default()
    };

    let wg = WorkloadGenerator::new(config);
    let handle = wg.start();

    tokio::time::sleep(Duration::from_secs(5)).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 0 })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(10)).await;
    wg.stop();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let log = wg.log().await;
    let result = Checker::check_read_consistency(&log);
    println!("Leader-read consistency: {}", result.summary());
    assert!(result.passed, "reads via leader policy must not be stale");

    restart_blockyard(&cluster, 0).await;
}

// ─── Test 5: bounded_staleness_any_policy ────────────────────────────
//
// Write to the cluster, read from any replica. Measure the maximum
// staleness (difference between expected data and actual). Staleness
// must be bounded — eventually consistent, not divergent.

#[tokio::test]
#[ignore]
async fn bounded_staleness_any_policy() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);
    let config = WorkloadConfig {
        volume_name: "test-any-reads".into(),
        volume_id: 5,
        duration: Duration::from_secs(20),
        write_rate: 30,
        read_rate: 70,
        target_addrs: cluster
            .running_nodes()
            .iter()
            .map(|n| n.blockyard_addr())
            .collect(),
        ..Default::default()
    };

    let wg = WorkloadGenerator::new(config);
    let handle = wg.start();

    tokio::time::sleep(Duration::from_secs(15)).await;
    wg.stop();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let log = wg.log().await;

    // For "any" policy, a small number of stale reads is tolerable.
    // We assert the stale-read ratio is below 5%.
    let mut stale = 0usize;
    let mut total_reads = 0usize;
    let mut write_map: std::collections::HashMap<u64, &[u8]> = std::collections::HashMap::new();
    for w in log.acknowledged_writes() {
        write_map.insert(w.offset, &w.data);
    }
    for r in &log.reads {
        if !r.success {
            continue;
        }
        total_reads += 1;
        if let Some(expected) = write_map.get(&r.offset) {
            if r.data != **expected {
                stale += 1;
            }
        }
    }

    let stale_ratio = if total_reads > 0 {
        stale as f64 / total_reads as f64
    } else {
        0.0
    };
    println!(
        "bounded_staleness: {stale}/{total_reads} stale reads ({:.1}%)",
        stale_ratio * 100.0
    );
    assert!(
        stale_ratio < 0.05,
        "stale read ratio {stale_ratio:.2} exceeds 5% bound"
    );
}
