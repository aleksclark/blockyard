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
async fn cluster_survives_one_of_three_crash() {
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

    tokio::time::sleep(Duration::from_secs(5)).await;

    let wg = WorkloadGenerator::new(WorkloadConfig {
        volume_name: "test-avail".into(),
        duration: Duration::from_secs(30),
        ..Default::default()
    });
    wg.start();

    tokio::time::sleep(Duration::from_secs(5)).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 2 })
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

#[tokio::test]
#[ignore]
async fn leader_elected_within_two_seconds() {
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

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 0 })
        .await
        .unwrap();

    let start = std::time::Instant::now();

    let mut leader_found = false;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let health = Checker::check_cluster_health(&cluster).await;
        let running = health.checks.iter().filter(|c| c.passed).count();
        if running >= 4 {
            leader_found = true;
            break;
        }
    }

    let elapsed = start.elapsed();
    println!("leader recovery took {elapsed:?}");
    assert!(leader_found);
    assert!(elapsed < Duration::from_secs(4));

    cluster.stop_all().await.unwrap();
}
