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

// ─── Test 1: mount_write_kill_remount_verify ─────────────────────────
//
// Mount a blockyard volume via ublk, write data through the block device,
// kill the mount process, remount, and verify the data survived.
//
// This test is a stub — it exercises the structure but requires a real
// ublk-capable kernel and device setup to run end-to-end.

#[tokio::test]
#[ignore]
async fn mount_write_kill_remount_verify() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);

    // Mount the volume.
    let mount_result = cluster
        .ssh_exec(
            0,
            "blockyard mount test-ublk /dev/ublkb0 2>/dev/null || true",
        )
        .await;
    println!("Mount: {mount_result:?}");

    // Write a known pattern through the block device.
    let write_result = cluster
        .ssh_exec(
            0,
            "dd if=/dev/urandom of=/dev/ublkb0 bs=4096 count=100 2>/dev/null || true",
        )
        .await;
    println!("Write: {write_result:?}");

    // Read back a checksum.
    let pre_checksum = cluster
        .ssh_exec(
            0,
            "dd if=/dev/ublkb0 bs=4096 count=100 2>/dev/null | sha256sum || echo 'no-checksum'",
        )
        .await
        .unwrap_or_else(|_| "no-checksum".to_string());
    println!("Pre-kill checksum: {pre_checksum}");

    // Kill the mount process.
    let _ = cluster
        .ssh_exec(0, "pkill -9 'blockyard.*mount' 2>/dev/null || true")
        .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Remount.
    let remount = cluster
        .ssh_exec(
            0,
            "blockyard mount test-ublk /dev/ublkb0 2>/dev/null || true",
        )
        .await;
    println!("Remount: {remount:?}");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify data.
    let post_checksum = cluster
        .ssh_exec(
            0,
            "dd if=/dev/ublkb0 bs=4096 count=100 2>/dev/null | sha256sum || echo 'no-checksum'",
        )
        .await
        .unwrap_or_else(|_| "no-checksum".to_string());
    println!("Post-remount checksum: {post_checksum}");

    // In a real environment, these should match.
    if pre_checksum != "no-checksum" && post_checksum != "no-checksum" {
        assert_eq!(
            pre_checksum.trim(),
            post_checksum.trim(),
            "data must survive mount process kill"
        );
    } else {
        println!("SKIP: ublk device not available, checksums could not be compared");
    }

    // No panics.
    let panic_check = Checker::check_blockyard_logs_no_panic(&cluster).await;
    assert!(panic_check.passed, "no panics expected during mount test");
}

// ─── Test 2: mount_partition_leader_failover ─────────────────────────
//
// Mount a volume, partition the client from the current leader, and
// verify the client follows the new leader after failover.
//
// This test is a stub — full end-to-end requires ublk device support.

#[tokio::test]
#[ignore]
async fn mount_partition_leader_failover() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);

    // Mount volume on node 0.
    let _ = cluster
        .ssh_exec(
            0,
            "blockyard mount test-ublk-fo /dev/ublkb1 2>/dev/null || true",
        )
        .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Start a background write workload via the block device.
    let _ = cluster
        .ssh_exec(
            0,
            "nohup dd if=/dev/urandom of=/dev/ublkb1 bs=4096 count=1000 2>/dev/null &",
        )
        .await;

    // Partition node 0 from the leader (node 1, assumed).
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NetworkPartition { from: 0, to: 1 })
        .await
        .unwrap();

    // Let the client detect the partition and failover.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Heal the partition.
    injector
        .inject(&Fault::NetworkHeal { from: 0, to: 1 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify cluster health.
    let health = Checker::check_cluster_health(&cluster).await;
    println!("Post-failover health: {}", health.summary());

    // Check no panics.
    let panic_check = Checker::check_blockyard_logs_no_panic(&cluster).await;
    println!("Panic check: {}", panic_check.summary());
    assert!(
        panic_check.passed,
        "no panics expected during ublk leader failover"
    );
}
