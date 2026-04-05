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

    let wg = WorkloadGenerator::new(WorkloadConfig {
        volume_name: "test-integrity".into(),
        duration: Duration::from_secs(10),
        ..Default::default()
    });
    wg.start();
    tokio::time::sleep(Duration::from_secs(5)).await;
    wg.stop();

    let injector = FaultInjector::new(&cluster);
    for i in 0..3 {
        injector
            .inject(&Fault::NodeCrash { node_id: i })
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    let zfs_result = Checker::check_zfs_integrity(&cluster).await;
    println!("ZFS: {}", zfs_result.summary());

    let health = Checker::check_cluster_health(&cluster).await;
    println!("Health: {}", health.summary());
}

#[tokio::test]
#[ignore]
async fn partition_heal_convergence() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);

    let wg = WorkloadGenerator::new(WorkloadConfig {
        volume_name: "test-partition".into(),
        duration: Duration::from_secs(30),
        ..Default::default()
    });
    wg.start();

    tokio::time::sleep(Duration::from_secs(3)).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NetworkPartition { from: 0, to: 1 })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;

    injector
        .inject(&Fault::NetworkHeal { from: 0, to: 1 })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;

    wg.stop();

    let log = wg.log().await;
    let result = Checker::check_all(&log);
    println!("{}", result.summary());
    assert!(result.passed);
}
