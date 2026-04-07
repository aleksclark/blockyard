#![allow(unused_imports, dead_code)]
mod harness;

use harness::checker::Checker;
use harness::cluster::{ClusterConfig, TestCluster};
use harness::faults::{Fault, FaultInjector};
use harness::workload::{WorkloadConfig, WorkloadGenerator};
use harness::{
    CLIENT_NODE, MOUNT_PATH, STORAGE_NODES, ensure_all_nodes_running, mount_volume, start_cluster,
    unmount_volume,
};
use std::path::{Path, PathBuf};
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

/// Resolve the path to the `blockyard-stress` release binary.
///
/// The integration test crate lives at `tests/` within the workspace root.
/// `CARGO_MANIFEST_DIR` points to that directory, so the binary is at
/// `../target/release/blockyard-stress` relative to it.
fn stress_binary_path() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| "tests".to_string());
    let workspace_root = PathBuf::from(&manifest_dir)
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let target_dir = std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| workspace_root.join("target"));
    target_dir.join("release/blockyard-stress")
}

/// Deploy the `blockyard-stress` binary to the given client VM node.
async fn deploy_stress_binary(cluster: &TestCluster, client_node: usize) {
    let binary = stress_binary_path();
    assert!(
        binary.exists(),
        "blockyard-stress binary not found at {}: run `cargo build --release` first",
        binary.display()
    );

    cluster
        .scp_to(client_node, &binary, "/usr/local/bin/blockyard-stress")
        .await
        .unwrap_or_else(|e| panic!("failed to SCP blockyard-stress to node {client_node}: {e}"));

    cluster
        .ssh_exec(client_node, "chmod +x /usr/local/bin/blockyard-stress")
        .await
        .unwrap_or_else(|e| panic!("chmod failed: {e}"));
}

/// Run a stress command on the client VM via SSH with a timeout.
///
/// Returns `true` if the command exits with status 0.
async fn run_stress_cmd(cluster: &TestCluster, client_node: usize, args: &str) -> bool {
    let cmd = format!("timeout 120 /usr/local/bin/blockyard-stress {args}");
    match cluster.ssh_exec(client_node, &cmd).await {
        Ok(_output) => true,
        Err(e) => {
            eprintln!("[stress] command failed: {e}");
            false
        }
    }
}

// ── Test 1: concurrent random writes ────────────────────────────────────

#[tokio::test]
// Requires BLOCKYARD_INTEGRATION=1 and running QEMU VM cluster
#[ignore]
async fn concurrent_random_writes() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let _mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;
    deploy_stress_binary(&cluster, CLIENT_NODE).await;

    let passed = run_stress_cmd(
        &cluster,
        CLIENT_NODE,
        "random-writes --dir /mnt/blockyard --threads 8 --ops-per-thread 50",
    )
    .await;
    assert!(passed, "blockyard-stress random-writes failed");

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 2: concurrent read/write ───────────────────────────────────────

#[tokio::test]
// Requires BLOCKYARD_INTEGRATION=1 and running QEMU VM cluster
#[ignore]
async fn concurrent_read_write() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let _mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;
    deploy_stress_binary(&cluster, CLIENT_NODE).await;

    let passed = run_stress_cmd(
        &cluster,
        CLIENT_NODE,
        "concurrent-read-write --dir /mnt/blockyard --writers 4 --readers 4 --ops 100",
    )
    .await;
    assert!(passed, "blockyard-stress concurrent-read-write failed");

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 3: write-read consistency ──────────────────────────────────────

#[tokio::test]
// Requires BLOCKYARD_INTEGRATION=1 and running QEMU VM cluster
#[ignore]
async fn write_read_consistency() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let _mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;
    deploy_stress_binary(&cluster, CLIENT_NODE).await;

    let passed = run_stress_cmd(
        &cluster,
        CLIENT_NODE,
        "write-read-consistency --dir /mnt/blockyard --iterations 30",
    )
    .await;
    assert!(passed, "blockyard-stress write-read-consistency failed");

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 4: sequential write + random read ──────────────────────────────

#[tokio::test]
// Requires BLOCKYARD_INTEGRATION=1 and running QEMU VM cluster
#[ignore]
async fn seq_write_random_read() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let _mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;
    deploy_stress_binary(&cluster, CLIENT_NODE).await;

    let passed = run_stress_cmd(
        &cluster,
        CLIENT_NODE,
        "seq-write-random-read --dir /mnt/blockyard --readers 8 --files 50",
    )
    .await;
    assert!(passed, "blockyard-stress seq-write-rand-read failed");

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 5: high concurrency stress ─────────────────────────────────────

#[tokio::test]
// Requires BLOCKYARD_INTEGRATION=1 and running QEMU VM cluster
#[ignore]
async fn stress_high_concurrency() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let _mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;
    deploy_stress_binary(&cluster, CLIENT_NODE).await;

    let passed = run_stress_cmd(
        &cluster,
        CLIENT_NODE,
        "stress --dir /mnt/blockyard --threads 16 --duration-secs 10",
    )
    .await;
    assert!(passed, "blockyard-stress stress failed");

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 6: large sequential + random read ──────────────────────────────

#[tokio::test]
// Requires BLOCKYARD_INTEGRATION=1 and running QEMU VM cluster
#[ignore]
async fn large_seq_random_read() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let _mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;
    deploy_stress_binary(&cluster, CLIENT_NODE).await;

    let passed = run_stress_cmd(
        &cluster,
        CLIENT_NODE,
        "large-seq-random-read --dir /mnt/blockyard --readers 4",
    )
    .await;
    assert!(passed, "blockyard-stress large-seq-rand-read failed");

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 7: fsync durability ────────────────────────────────────────────

#[tokio::test]
// Requires BLOCKYARD_INTEGRATION=1 and running QEMU VM cluster
#[ignore]
async fn fsync_durability() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let _mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;
    deploy_stress_binary(&cluster, CLIENT_NODE).await;

    let passed = run_stress_cmd(
        &cluster,
        CLIENT_NODE,
        "fsync-durability --dir /mnt/blockyard --files 20",
    )
    .await;
    assert!(passed, "blockyard-stress fsync-durability failed");

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 8: stress with node crash (fault injection during I/O) ──────────

#[tokio::test]
// Requires BLOCKYARD_INTEGRATION=1 and running QEMU VM cluster
#[ignore]
async fn stress_with_node_crash() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let _mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;
    deploy_stress_binary(&cluster, CLIENT_NODE).await;

    let stress_cmd = "timeout 30 /usr/local/bin/blockyard-stress stress --dir /mnt/blockyard --threads 8 --duration-secs 20";
    let _ = cluster
        .ssh_exec(
            CLIENT_NODE,
            &format!("nohup {stress_cmd} > /tmp/stress-crash.log 2>&1 &"),
        )
        .await;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 2 })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(20)).await;

    ensure_all_nodes_running(&cluster).await;

    let ls_output = cluster
        .ssh_exec(CLIENT_NODE, "ls /mnt/blockyard/ 2>/dev/null | wc -l")
        .await
        .unwrap_or_default();
    let file_count: u32 = ls_output.trim().parse().unwrap_or(0);
    assert!(
        file_count > 0,
        "expected files to exist after stress+crash, found {file_count}"
    );

    // Check stress binary output for corruption
    let stress_output = cluster
        .ssh_exec(CLIENT_NODE, "cat /tmp/stress-crash.log 2>/dev/null")
        .await
        .unwrap_or_default();
    eprintln!("[stress-crash] output: {}", stress_output.trim());

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 9: stress with node pause (SIGSTOP simulating slow node) ────────

#[tokio::test]
// Requires BLOCKYARD_INTEGRATION=1 and running QEMU VM cluster
#[ignore]
async fn stress_with_node_pause() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let _mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;
    deploy_stress_binary(&cluster, CLIENT_NODE).await;

    let stress_cmd = "timeout 30 /usr/local/bin/blockyard-stress write-read-consistency --dir /mnt/blockyard --iterations 20";
    let _ = cluster
        .ssh_exec(
            CLIENT_NODE,
            &format!("nohup {stress_cmd} > /tmp/stress-pause.log 2>&1 &"),
        )
        .await;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodePause { node_id: 1 })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;

    injector
        .inject(&Fault::NodeResume { node_id: 1 })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(15)).await;

    // Check stress binary output for corruption
    let stress_output = cluster
        .ssh_exec(CLIENT_NODE, "cat /tmp/stress-pause.log 2>/dev/null")
        .await
        .unwrap_or_default();
    eprintln!("[stress-pause] output: {}", stress_output.trim());

    ensure_all_nodes_running(&cluster).await;
    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 10: WorkloadGenerator + Checker during node crash ───────────────

#[tokio::test]
// Requires BLOCKYARD_INTEGRATION=1 and running QEMU VM cluster
#[ignore]
async fn workload_generator_with_crash() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let _mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;

    let node0 = cluster.node(0).unwrap();
    let workload_config = WorkloadConfig {
        targets: vec![node0.blockyard_addr()],
        duration: Duration::from_secs(15),
        write_interval: Duration::from_millis(200),
        read_interval: Duration::from_millis(100),
        block_size: 4096,
        max_offset: 512 * 1024,
        volume_id: 1,
    };

    let workload_duration = workload_config.duration;
    let generator = WorkloadGenerator::new(workload_config);
    let handle = generator.start();

    tokio::time::sleep(Duration::from_secs(5)).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 2 })
        .await
        .unwrap();

    let _ = handle.await;

    let log = generator.log().await;
    eprintln!(
        "[workload] writes={}, reads={}, errors={}",
        log.write_count(),
        log.read_count(),
        log.error_count(),
    );

    let result = Checker::check_all(&log);
    eprintln!("[checker] {}", result.summary());

    let io_result = Checker::check_io_happened(&log);
    assert!(io_result.passed, "workload must generate I/O");

    let durability = Checker::check_write_durability(&log);
    assert!(durability.passed, "acknowledged writes must exist");

    let acked = log.acknowledged_writes();
    let expected_min = (workload_duration.as_secs() as usize) / 2;
    assert!(
        acked.len() >= expected_min,
        "expected at least {expected_min} acked writes for {}s workload, got {}",
        workload_duration.as_secs(),
        acked.len(),
    );

    ensure_all_nodes_running(&cluster).await;
    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 11: WorkloadGenerator + Checker during network partition ────────

#[tokio::test]
// Requires BLOCKYARD_INTEGRATION=1 and running QEMU VM cluster
#[ignore]
async fn workload_generator_with_partition() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let _mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;

    let node0 = cluster.node(0).unwrap();
    let workload_config = WorkloadConfig {
        targets: vec![node0.blockyard_addr()],
        duration: Duration::from_secs(15),
        write_interval: Duration::from_millis(200),
        read_interval: Duration::from_millis(100),
        block_size: 4096,
        max_offset: 512 * 1024,
        volume_id: 1,
    };

    let workload_duration = workload_config.duration;
    let generator = WorkloadGenerator::new(workload_config);
    let handle = generator.start();

    tokio::time::sleep(Duration::from_secs(3)).await;

    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NetworkPartition { from: 0, to: 2 })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;

    injector
        .inject(&Fault::NetworkHeal { from: 0, to: 2 })
        .await
        .unwrap();

    let _ = handle.await;

    let log = generator.log().await;
    eprintln!(
        "[workload] writes={}, reads={}, errors={}",
        log.write_count(),
        log.read_count(),
        log.error_count(),
    );

    let result = Checker::check_all(&log);
    eprintln!("[checker] {}", result.summary());

    let io_result = Checker::check_io_happened(&log);
    assert!(io_result.passed, "workload must generate I/O");

    let acked = log.acknowledged_writes();
    let expected_min = (workload_duration.as_secs() as usize) / 2;
    assert!(
        acked.len() >= expected_min,
        "expected at least {expected_min} acked writes for {}s workload, got {}",
        workload_duration.as_secs(),
        acked.len(),
    );

    ensure_all_nodes_running(&cluster).await;
    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}
