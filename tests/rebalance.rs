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
async fn crash_during_operation() {
    if !require_vm_env() { return; }
    let cluster = running_cluster(5);
    harness::ensure_all_nodes_running(&cluster).await;

    let injector = FaultInjector::new(&cluster);
    injector.inject(&Fault::NodeCrash { node_id: 4 }).await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    let health = Checker::check_blockyard_running(&cluster, 4).await;
    assert!(health.passed, "{}", health.summary());

    let no_panics = Checker::check_no_panics(&cluster).await;
    assert!(no_panics.passed, "{}", no_panics.summary());

    harness::ensure_all_nodes_running(&cluster).await;
}

#[tokio::test]
#[ignore]
async fn all_nodes_healthy_after_recovery() {
    if !require_vm_env() { return; }
    let cluster = running_cluster(5);
    harness::ensure_all_nodes_running(&cluster).await;

    let health = Checker::check_blockyard_running(&cluster, 5).await;
    assert!(health.passed, "{}", health.summary());

    let no_panics = Checker::check_no_panics(&cluster).await;
    assert!(no_panics.passed, "{}", no_panics.summary());
}
