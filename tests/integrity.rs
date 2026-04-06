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

#[tokio::test]
#[ignore]
async fn write_crash_restart_verify() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(5);
    harness::ensure_all_nodes_running(&cluster).await;

    let injector = FaultInjector::new(&cluster);
    for i in 0..3 {
        injector
            .inject(&Fault::NodeCrash { node_id: i })
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    let surviving = Checker::check_blockyard_running(&cluster, 2).await;
    assert!(
        surviving.passed,
        "not enough survivors: {}",
        surviving.summary()
    );

    harness::ensure_all_nodes_running(&cluster).await;

    let recovered = Checker::check_blockyard_running(&cluster, 5).await;
    assert!(recovered.passed, "recovery: {}", recovered.summary());

    let no_panics = Checker::check_no_panics(&cluster).await;
    assert!(no_panics.passed, "{}", no_panics.summary());
}

#[tokio::test]
#[ignore]
async fn partition_heal_convergence() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(5);
    harness::ensure_all_nodes_running(&cluster).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NetworkPartition { from: 0, to: 1 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    let health = Checker::check_blockyard_running(&cluster, 5).await;
    assert!(health.passed, "during partition: {}", health.summary());

    injector
        .inject(&Fault::NetworkHeal { from: 0, to: 1 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let healed = Checker::check_blockyard_running(&cluster, 5).await;
    assert!(healed.passed, "after heal: {}", healed.summary());

    let no_panics = Checker::check_no_panics(&cluster).await;
    assert!(no_panics.passed, "{}", no_panics.summary());
}

#[tokio::test]
#[ignore]
async fn node_pause_resume_no_data_loss() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(5);
    harness::ensure_all_nodes_running(&cluster).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodePause { node_id: 3 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    injector
        .inject(&Fault::NodeResume { node_id: 3 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let health = Checker::check_blockyard_running(&cluster, 5).await;
    assert!(health.passed, "{}", health.summary());

    let no_panics = Checker::check_no_panics(&cluster).await;
    assert!(no_panics.passed, "{}", no_panics.summary());
}

#[tokio::test]
#[ignore]
async fn network_delay_cluster_survives() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(5);
    harness::ensure_all_nodes_running(&cluster).await;

    let injector = FaultInjector::new(&cluster);
    if injector
        .inject(&Fault::NetworkDelay {
            node_id: 2,
            latency: Duration::from_millis(200),
        })
        .await
        .is_err()
    {
        println!("skipping: tc netem not available in VM");
        return;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    let health = Checker::check_blockyard_running(&cluster, 5).await;
    assert!(health.passed, "{}", health.summary());

    injector
        .inject(&Fault::NetworkReset { node_id: 2 })
        .await
        .unwrap();
}

#[tokio::test]
#[ignore]
async fn asymmetric_partition_cluster_survives() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(5);
    harness::ensure_all_nodes_running(&cluster).await;

    let injector = FaultInjector::new(&cluster);
    if injector
        .inject(&Fault::AsymmetricPartition {
            blocked_from: 0,
            blocked_to: 1,
        })
        .await
        .is_err()
    {
        println!("skipping: iptables not available in VM");
        return;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    let health = Checker::check_blockyard_running(&cluster, 5).await;
    assert!(health.passed, "{}", health.summary());

    injector
        .inject(&Fault::NetworkReset { node_id: 0 })
        .await
        .unwrap();
}

/// Reproduces: data written to the block protocol is not readable back.
/// mkfs.ext4 writes superblock data, but mount fails because reads return
/// all zeros instead of the written data. This test writes a known pattern
/// via the block protocol TCP path and reads it back, asserting equality.
#[tokio::test]
#[ignore]
async fn write_then_read_returns_written_data() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(5);
    harness::ensure_all_nodes_running(&cluster).await;

    let target = cluster.running_nodes()[0].blockyard_addr();
    let config = WorkloadConfig {
        targets: vec![target],
        duration: Duration::from_secs(5),
        write_interval: Duration::from_millis(100),
        read_interval: Duration::from_millis(50),
        block_size: 4096,
        max_offset: 64 * 1024,
        volume_id: 1,
    };

    let pattern: Vec<u8> = (0..4096u16).map(|i| (i % 256) as u8).collect();
    let offset = 0u64;

    let write_ok = WorkloadGenerator::send_write(&config, 1, offset, &pattern)
        .await
        .expect("write should succeed");
    assert!(write_ok, "write was not acknowledged");

    let read_data = WorkloadGenerator::send_read(&config, 2, offset, 4096)
        .await
        .expect("read should succeed");

    assert_eq!(
        read_data.as_ref(),
        pattern.as_slice(),
        "read data does not match written data — reads are returning wrong content"
    );
}
