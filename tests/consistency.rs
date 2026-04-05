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
async fn linearizable_writes_during_leader_failover() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);

    let wg = WorkloadGenerator::new(WorkloadConfig {
        volume_name: "test-linear".into(),
        duration: Duration::from_secs(30),
        ..Default::default()
    });
    wg.start();

    tokio::time::sleep(Duration::from_secs(5)).await;

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
}

#[tokio::test]
#[ignore]
async fn majority_ack_no_data_loss() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);

    let wg = WorkloadGenerator::new(WorkloadConfig {
        volume_name: "test-majority".into(),
        duration: Duration::from_secs(30),
        ..Default::default()
    });
    wg.start();

    tokio::time::sleep(Duration::from_secs(5)).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 0 })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(10)).await;
    wg.stop();

    let log = wg.log().await;
    let durability = Checker::check_write_durability(&log);
    println!("{}", durability.summary());
    assert!(durability.passed);
}
