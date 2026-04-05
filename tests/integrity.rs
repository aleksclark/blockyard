#![allow(unused_imports, dead_code)]
mod harness;

use harness::checker::Checker;
use harness::cluster::{ClusterConfig, TestCluster};
use harness::faults::{Fault, FaultInjector};
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
    if !require_vm_env() { return; }
    let cluster = running_cluster(5);
    harness::ensure_all_nodes_running(&cluster).await;

    let injector = FaultInjector::new(&cluster);
    for i in 0..3 {
        injector.inject(&Fault::NodeCrash { node_id: i }).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    let surviving = Checker::check_blockyard_running(&cluster, 2).await;
    assert!(surviving.passed, "not enough survivors: {}", surviving.summary());

    harness::ensure_all_nodes_running(&cluster).await;

    let recovered = Checker::check_blockyard_running(&cluster, 5).await;
    assert!(recovered.passed, "recovery: {}", recovered.summary());

    let no_panics = Checker::check_no_panics(&cluster).await;
    assert!(no_panics.passed, "{}", no_panics.summary());
}

#[tokio::test]
#[ignore]
async fn partition_heal_convergence() {
    if !require_vm_env() { return; }
    let cluster = running_cluster(5);
    harness::ensure_all_nodes_running(&cluster).await;

    let injector = FaultInjector::new(&cluster);
    injector.inject(&Fault::NetworkPartition { from: 0, to: 1 }).await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    let health = Checker::check_blockyard_running(&cluster, 5).await;
    assert!(health.passed, "during partition: {}", health.summary());

    injector.inject(&Fault::NetworkHeal { from: 0, to: 1 }).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let healed = Checker::check_blockyard_running(&cluster, 5).await;
    assert!(healed.passed, "after heal: {}", healed.summary());

    let no_panics = Checker::check_no_panics(&cluster).await;
    assert!(no_panics.passed, "{}", no_panics.summary());
}

#[tokio::test]
#[ignore]
async fn node_pause_resume_no_data_loss() {
    if !require_vm_env() { return; }
    let cluster = running_cluster(5);
    harness::ensure_all_nodes_running(&cluster).await;

    let injector = FaultInjector::new(&cluster);
    injector.inject(&Fault::NodePause { node_id: 3 }).await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    injector.inject(&Fault::NodeResume { node_id: 3 }).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let health = Checker::check_blockyard_running(&cluster, 5).await;
    assert!(health.passed, "{}", health.summary());

    let no_panics = Checker::check_no_panics(&cluster).await;
    assert!(no_panics.passed, "{}", no_panics.summary());
}

#[tokio::test]
#[ignore]
async fn network_delay_cluster_survives() {
    if !require_vm_env() { return; }
    let cluster = running_cluster(5);
    harness::ensure_all_nodes_running(&cluster).await;

    let injector = FaultInjector::new(&cluster);
    if injector.inject(&Fault::NetworkDelay { node_id: 2, latency: Duration::from_millis(200) }).await.is_err() {
        println!("skipping: tc netem not available in VM");
        return;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    let health = Checker::check_blockyard_running(&cluster, 5).await;
    assert!(health.passed, "{}", health.summary());

    injector.inject(&Fault::NetworkReset { node_id: 2 }).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn asymmetric_partition_cluster_survives() {
    if !require_vm_env() { return; }
    let cluster = running_cluster(5);
    harness::ensure_all_nodes_running(&cluster).await;

    let injector = FaultInjector::new(&cluster);
    if injector.inject(&Fault::AsymmetricPartition { blocked_from: 0, blocked_to: 1 }).await.is_err() {
        println!("skipping: iptables not available in VM");
        return;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    let health = Checker::check_blockyard_running(&cluster, 5).await;
    assert!(health.passed, "{}", health.summary());

    injector.inject(&Fault::NetworkReset { node_id: 0 }).await.unwrap();
}
