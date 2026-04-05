#![allow(dead_code, unused_imports)]
mod harness;

use harness::checker::Checker;
use harness::cluster::{ClusterConfig, TestCluster};
use harness::faults::{Fault, FaultInjector};
use harness::workload::{WorkloadConfig, WorkloadGenerator};
use std::time::Duration;

fn require_vm_env() {
    if std::env::var("BLOCKYARD_INTEGRATION").is_err() {
        eprintln!("skipping: set BLOCKYARD_INTEGRATION=1 to run VM-based tests");
        return;
    }
}

#[tokio::test]
#[ignore]
async fn write_crash_restart_verify() {
    require_vm_env();

    let mut cluster = TestCluster::new(ClusterConfig {
        node_count: 3,
        ..Default::default()
    });

    cluster.provision().await.unwrap();
    cluster.start_all().await.unwrap();

    for i in 0..3 {
        cluster
            .wait_for_ssh(i, Duration::from_secs(120))
            .await
            .unwrap();
        cluster.deploy_blockyard(i).await.unwrap();
        cluster.start_blockyard(i).await.unwrap();
    }

    let wg = WorkloadGenerator::new(WorkloadConfig {
        volume_name: "test-integrity".into(),
        duration: Duration::from_secs(10),
        ..Default::default()
    });
    wg.start();
    tokio::time::sleep(Duration::from_secs(10)).await;
    wg.stop();

    let injector = FaultInjector::new(&cluster);
    for i in 0..3 {
        injector
            .inject(&Fault::NodeCrash { node_id: i })
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    for i in 0..3 {
        cluster.start_blockyard(i).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(5)).await;

    let zfs_result = Checker::check_zfs_integrity(&cluster).await;
    println!("{}", zfs_result.summary());

    let health = Checker::check_cluster_health(&cluster).await;
    println!("{}", health.summary());

    cluster.stop_all().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn partition_heal_convergence() {
    require_vm_env();

    let mut cluster = TestCluster::new(ClusterConfig {
        node_count: 5,
        ..Default::default()
    });

    cluster.provision().await.unwrap();
    cluster.start_all().await.unwrap();

    for i in 0..5 {
        cluster
            .wait_for_ssh(i, Duration::from_secs(120))
            .await
            .unwrap();
        cluster.deploy_blockyard(i).await.unwrap();
        cluster.start_blockyard(i).await.unwrap();
    }

    tokio::time::sleep(Duration::from_secs(5)).await;

    let wg = WorkloadGenerator::new(WorkloadConfig {
        volume_name: "test-partition".into(),
        duration: Duration::from_secs(30),
        ..Default::default()
    });
    wg.start();

    tokio::time::sleep(Duration::from_secs(5)).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NetworkPartition { from: 0, to: 1 })
        .await
        .unwrap();
    injector
        .inject(&Fault::NetworkPartition { from: 0, to: 2 })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(10)).await;

    injector
        .inject(&Fault::NetworkHeal { from: 0, to: 1 })
        .await
        .unwrap();
    injector
        .inject(&Fault::NetworkHeal { from: 0, to: 2 })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(10)).await;

    wg.stop();

    let log = wg.log().await;
    let result = Checker::check_all(&log);
    println!("{}", result.summary());
    assert!(result.passed);

    cluster.stop_all().await.unwrap();
}
