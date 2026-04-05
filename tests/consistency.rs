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
async fn linearizable_writes_during_leader_failover() {
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
        volume_name: "test-linear".into(),
        duration: Duration::from_secs(30),
        ..Default::default()
    });
    wg.start();

    tokio::time::sleep(Duration::from_secs(10)).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 0 })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;

    wg.stop();

    let log = wg.log().await;
    let result = Checker::check_all(&log);
    println!("{}", result.summary());
    assert!(result.passed);

    cluster.stop_all().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn majority_ack_no_data_loss() {
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

    let wg = WorkloadGenerator::new(WorkloadConfig {
        volume_name: "test-majority".into(),
        duration: Duration::from_secs(30),
        ..Default::default()
    });
    wg.start();

    tokio::time::sleep(Duration::from_secs(10)).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 0 })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(20)).await;
    wg.stop();

    let log = wg.log().await;
    let durability = Checker::check_write_durability(&log);
    println!("{}", durability.summary());
    assert!(durability.passed);

    cluster.stop_all().await.unwrap();
}
