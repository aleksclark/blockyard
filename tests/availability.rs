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
async fn cluster_survives_one_of_three_crash() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);

    let wg = WorkloadGenerator::new(WorkloadConfig {
        volume_name: "test-avail".into(),
        duration: Duration::from_secs(30),
        ..Default::default()
    });
    wg.start();

    tokio::time::sleep(Duration::from_secs(3)).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 2 })
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
async fn leader_elected_within_two_seconds() {
    if !require_vm_env() {
        return;
    }

    let cluster = running_cluster(5);

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
        let running = health.checks.iter().filter(|c| c.passed).count();
        if running >= 4 {
            survivors_ok = true;
            break;
        }
    }

    let elapsed = start.elapsed();
    println!("leader recovery check took {elapsed:?}");
    assert!(survivors_ok, "not enough surviving nodes after crash");
    assert!(
        elapsed < Duration::from_secs(4),
        "took too long: {elapsed:?}"
    );
}
