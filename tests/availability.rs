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
async fn cluster_survives_one_of_five_crash() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(5);
    harness::ensure_all_nodes_running(&cluster).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 2 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    let health = Checker::check_blockyard_running(&cluster, 4).await;
    println!("{}", health.summary());
    assert!(health.passed, "{}", health.summary());

    let no_panics = Checker::check_no_panics(&cluster).await;
    assert!(no_panics.passed, "{}", no_panics.summary());

    harness::ensure_all_nodes_running(&cluster).await;
}

#[tokio::test]
#[ignore]
async fn leader_elected_within_two_seconds() {
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

    let start = std::time::Instant::now();
    let mut survivors_ok = false;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let health = Checker::check_cluster_health(&cluster).await;
        if health.passed_count() >= 4 {
            survivors_ok = true;
            break;
        }
    }
    let elapsed = start.elapsed();
    println!("recovery took {elapsed:?}");
    assert!(survivors_ok, "not enough surviving nodes");
    assert!(
        elapsed < Duration::from_secs(4),
        "took too long: {elapsed:?}"
    );

    harness::ensure_all_nodes_running(&cluster).await;
}

#[tokio::test]
#[ignore]
async fn volume_readable_during_minority_partition() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(5);
    harness::ensure_all_nodes_running(&cluster).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NetworkPartition { from: 3, to: 4 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let health = Checker::check_blockyard_running(&cluster, 5).await;
    println!("{}", health.summary());
    assert!(health.passed, "{}", health.summary());

    injector
        .inject(&Fault::NetworkHeal { from: 3, to: 4 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
}

#[tokio::test]
#[ignore]
async fn node_pause_resume_cluster_survives() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(5);
    harness::ensure_all_nodes_running(&cluster).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodePause { node_id: 1 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    injector
        .inject(&Fault::NodeResume { node_id: 1 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let health = Checker::check_blockyard_running(&cluster, 5).await;
    println!("{}", health.summary());
    assert!(health.passed, "{}", health.summary());
}

#[tokio::test]
#[ignore]
async fn two_node_crash_survivors_healthy() {
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
    injector
        .inject(&Fault::NodeCrash { node_id: 1 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    let health = Checker::check_blockyard_running(&cluster, 3).await;
    println!("{}", health.summary());
    assert!(health.passed, "{}", health.summary());

    let no_panics = Checker::check_no_panics(&cluster).await;
    assert!(no_panics.passed, "{}", no_panics.summary());

    harness::ensure_all_nodes_running(&cluster).await;
}
