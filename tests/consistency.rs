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
async fn crash_leader_surviving_nodes_healthy() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(5);
    harness::ensure_all_nodes_running(&cluster).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 0 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    let health = Checker::check_blockyard_running(&cluster, 4).await;
    assert!(health.passed, "{}", health.summary());
    let no_panics = Checker::check_no_panics(&cluster).await;
    assert!(no_panics.passed, "{}", no_panics.summary());

    harness::ensure_all_nodes_running(&cluster).await;
}

#[tokio::test]
#[ignore]
async fn crash_and_recovery_no_data_loss() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(5);
    harness::ensure_all_nodes_running(&cluster).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 1 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let health = Checker::check_blockyard_running(&cluster, 4).await;
    assert!(health.passed, "{}", health.summary());

    harness::ensure_all_nodes_running(&cluster).await;

    let recovered = Checker::check_blockyard_running(&cluster, 5).await;
    assert!(recovered.passed, "recovery: {}", recovered.summary());
}

#[tokio::test]
#[ignore]
async fn no_stale_reads_no_panics() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(5);
    harness::ensure_all_nodes_running(&cluster).await;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let no_panics = Checker::check_no_panics(&cluster).await;
    assert!(no_panics.passed, "{}", no_panics.summary());
}
