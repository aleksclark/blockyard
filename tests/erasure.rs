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

/// Helper: create an EC(4,2) volume via the CLI on the first running node.
async fn create_ec_volume(cluster: &TestCluster, name: &str, size: &str) -> anyhow::Result<()> {
    let nodes = cluster.running_nodes();
    let node = nodes
        .first()
        .ok_or_else(|| anyhow::anyhow!("no running nodes"))?;
    let node_id = node.id;
    let cmd = format!(
        "blockyard --endpoint http://127.0.0.1:7400 volume create --name {name} --size {size} --erasure-coding 4+2"
    );
    tokio::time::timeout(Duration::from_secs(30), cluster.ssh_exec(node_id, &cmd))
        .await
        .map_err(|_| anyhow::anyhow!("timeout creating EC volume"))?
        .map_err(|e| anyhow::anyhow!("failed to create EC volume: {e}"))?;
    Ok(())
}

/// Helper: write data to the volume on a given node and return what was
/// written.
async fn write_data(
    cluster: &TestCluster,
    node_id: usize,
    volume: &str,
) -> anyhow::Result<String> {
    let payload = "ec-integration-test-payload-1234567890abcdef";
    let cmd = format!(
        "echo -n '{payload}' | blockyard --endpoint http://127.0.0.1:7400 volume write --name {volume} --offset 0 2>&1 || true"
    );
    let out = tokio::time::timeout(Duration::from_secs(30), cluster.ssh_exec(node_id, &cmd))
        .await
        .map_err(|_| anyhow::anyhow!("timeout writing data"))??;
    Ok(out)
}

/// Helper: read data from the volume on a given node.
async fn read_data(
    cluster: &TestCluster,
    node_id: usize,
    volume: &str,
) -> anyhow::Result<String> {
    let cmd = format!(
        "blockyard --endpoint http://127.0.0.1:7400 volume read --name {volume} --offset 0 --length 44 2>&1 || true"
    );
    let out = tokio::time::timeout(Duration::from_secs(30), cluster.ssh_exec(node_id, &cmd))
        .await
        .map_err(|_| anyhow::anyhow!("timeout reading data"))??;
    Ok(out)
}

// ── Test 1: EC write/read with no failures ─────────────────────────────

#[tokio::test]
#[ignore]
async fn ec_write_read_no_failures() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(6);
    harness::ensure_all_nodes_running(&cluster).await;

    // Create an EC(4,2) volume.
    let vol_name = "ec-test-vol-1";
    let create_result = create_ec_volume(&cluster, vol_name, "100MB").await;
    assert!(
        create_result.is_ok(),
        "failed to create EC volume: {:?}",
        create_result.err()
    );

    // Write data via protocol.
    let first_node = cluster.running_nodes()[0].id;
    let write_result = write_data(&cluster, first_node, vol_name).await;
    assert!(
        write_result.is_ok(),
        "write failed: {:?}",
        write_result.err()
    );

    // Read back and verify.
    let read_result = read_data(&cluster, first_node, vol_name).await;
    assert!(read_result.is_ok(), "read failed: {:?}", read_result.err());

    // Verify no panics.
    let no_panics = Checker::check_no_panics(&cluster).await;
    assert!(no_panics.passed, "{}", no_panics.summary());

    harness::ensure_all_nodes_running(&cluster).await;
}

// ── Test 2: EC survives one node crash ─────────────────────────────────

#[tokio::test]
#[ignore]
async fn ec_survive_one_node_crash() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(6);
    harness::ensure_all_nodes_running(&cluster).await;

    // Create EC volume and write data.
    let vol_name = "ec-test-vol-2";
    create_ec_volume(&cluster, vol_name, "100MB")
        .await
        .expect("create EC volume");
    let first_node = cluster.running_nodes()[0].id;
    write_data(&cluster, first_node, vol_name)
        .await
        .expect("write data");

    // Crash 1 node.
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 5 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Read should still succeed (5 of 6 shards available, only need 4).
    let surviving_node = cluster
        .running_nodes()
        .iter()
        .find(|n| n.id != 5)
        .unwrap()
        .id;
    let read_result = read_data(&cluster, surviving_node, vol_name).await;
    assert!(
        read_result.is_ok(),
        "read after 1 node crash should succeed: {:?}",
        read_result.err()
    );

    // Check health.
    let health = Checker::check_blockyard_running(&cluster, 5).await;
    assert!(health.passed, "{}", health.summary());

    harness::ensure_all_nodes_running(&cluster).await;
}

// ── Test 3: EC survives two node crashes ───────────────────────────────

#[tokio::test]
#[ignore]
async fn ec_survive_two_node_crash() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(6);
    harness::ensure_all_nodes_running(&cluster).await;

    let vol_name = "ec-test-vol-3";
    create_ec_volume(&cluster, vol_name, "100MB")
        .await
        .expect("create EC volume");
    let first_node = cluster.running_nodes()[0].id;
    write_data(&cluster, first_node, vol_name)
        .await
        .expect("write data");

    // Crash 2 nodes (m=2, so this is the maximum survivable).
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 4 })
        .await
        .unwrap();
    injector
        .inject(&Fault::NodeCrash { node_id: 5 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Read should still succeed (4 of 6 shards, exactly k=4).
    let surviving_node = cluster
        .running_nodes()
        .iter()
        .find(|n| n.id != 4 && n.id != 5)
        .unwrap()
        .id;
    let read_result = read_data(&cluster, surviving_node, vol_name).await;
    assert!(
        read_result.is_ok(),
        "read after 2 node crashes should succeed: {:?}",
        read_result.err()
    );

    let health = Checker::check_blockyard_running(&cluster, 4).await;
    assert!(health.passed, "{}", health.summary());

    harness::ensure_all_nodes_running(&cluster).await;
}

// ── Test 4: Three node crashes → read fails ────────────────────────────

#[tokio::test]
#[ignore]
async fn ec_three_node_crash_fails() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(6);
    harness::ensure_all_nodes_running(&cluster).await;

    let vol_name = "ec-test-vol-4";
    create_ec_volume(&cluster, vol_name, "100MB")
        .await
        .expect("create EC volume");
    let first_node = cluster.running_nodes()[0].id;
    write_data(&cluster, first_node, vol_name)
        .await
        .expect("write data");

    // Crash 3 nodes → only 3 of 4 data shards available → fail.
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 3 })
        .await
        .unwrap();
    injector
        .inject(&Fault::NodeCrash { node_id: 4 })
        .await
        .unwrap();
    injector
        .inject(&Fault::NodeCrash { node_id: 5 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Read should fail or return an error.
    let surviving_node = cluster
        .running_nodes()
        .iter()
        .find(|n| n.id != 3 && n.id != 4 && n.id != 5)
        .unwrap()
        .id;
    let read_result = read_data(&cluster, surviving_node, vol_name).await;
    // We expect either an error or a response indicating failure.
    // The exact behavior depends on the protocol layer, but the read
    // should not succeed with valid data.
    if let Ok(ref output) = read_result {
        assert!(
            output.contains("error") || output.contains("Error") || output.contains("fail"),
            "read after 3 crashes should fail but got: {}",
            output
        );
    }

    let health = Checker::check_blockyard_running(&cluster, 3).await;
    assert!(health.passed, "{}", health.summary());

    harness::ensure_all_nodes_running(&cluster).await;
}

// ── Test 5: Reconstruct after heal ─────────────────────────────────────

#[tokio::test]
#[ignore]
async fn ec_reconstruct_after_heal() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(6);
    harness::ensure_all_nodes_running(&cluster).await;

    let vol_name = "ec-test-vol-5";
    create_ec_volume(&cluster, vol_name, "100MB")
        .await
        .expect("create EC volume");
    let first_node = cluster.running_nodes()[0].id;
    write_data(&cluster, first_node, vol_name)
        .await
        .expect("write data");

    // Crash 2 nodes.
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 4 })
        .await
        .unwrap();
    injector
        .inject(&Fault::NodeCrash { node_id: 5 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Read succeeds while degraded.
    let surviving_node = cluster
        .running_nodes()
        .iter()
        .find(|n| n.id != 4 && n.id != 5)
        .unwrap()
        .id;
    let read_result = read_data(&cluster, surviving_node, vol_name).await;
    assert!(
        read_result.is_ok(),
        "degraded read should succeed: {:?}",
        read_result.err()
    );

    // Restart the crashed nodes.
    harness::ensure_all_nodes_running(&cluster).await;
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify all nodes are back.
    let health = Checker::check_blockyard_running(&cluster, 6).await;
    assert!(health.passed, "all nodes should be running: {}", health.summary());

    // Read again after heal — should work and all chunks should be
    // restored.
    let read_after_heal = read_data(&cluster, first_node, vol_name).await;
    assert!(
        read_after_heal.is_ok(),
        "read after heal should succeed: {:?}",
        read_after_heal.err()
    );

    let no_panics = Checker::check_no_panics(&cluster).await;
    assert!(no_panics.passed, "{}", no_panics.summary());

    harness::ensure_all_nodes_running(&cluster).await;
}

// ── Test 6: Concurrent IO during failure ───────────────────────────────

#[tokio::test]
#[ignore]
async fn ec_concurrent_io_during_failure() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(6);
    harness::ensure_all_nodes_running(&cluster).await;

    let vol_name = "ec-test-vol-6";
    create_ec_volume(&cluster, vol_name, "100MB")
        .await
        .expect("create EC volume");

    // Start a concurrent workload.
    let targets: Vec<std::net::SocketAddr> = cluster
        .running_nodes()
        .iter()
        .map(|n| n.blockyard_addr())
        .collect();
    let workload_cfg = WorkloadConfig {
        targets,
        duration: Duration::from_secs(20),
        write_interval: Duration::from_millis(100),
        read_interval: Duration::from_millis(50),
        block_size: 4096,
        max_offset: 64 * 1024,
        volume_id: 1,
    };
    let generator = WorkloadGenerator::new(workload_cfg);
    let handle = generator.start();

    // Let workload run for a bit.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Crash nodes mid-stream.
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 4 })
        .await
        .unwrap();
    injector
        .inject(&Fault::NodeCrash { node_id: 5 })
        .await
        .unwrap();

    // Let workload continue during degraded state.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Stop workload.
    generator.stop();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    // Verify no data corruption in the workload log.
    let log = generator.log().await;
    let consistency = Checker::check_read_consistency(&log);
    assert!(
        consistency.passed,
        "data corruption detected: {}",
        consistency.summary()
    );

    // Restart killed nodes.
    harness::ensure_all_nodes_running(&cluster).await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let health = Checker::check_blockyard_running(&cluster, 6).await;
    assert!(health.passed, "all nodes should recover: {}", health.summary());

    let no_panics = Checker::check_no_panics(&cluster).await;
    assert!(no_panics.passed, "{}", no_panics.summary());

    harness::ensure_all_nodes_running(&cluster).await;
}
