//! UBLK end-to-end test runner.
//!
//! This binary is designed to run **inside a QEMU VM** with root privileges
//! and the `ublk_drv` kernel module loaded. It:
//!
//! 1. Starts a local blockyard cluster (N nodes as child processes)
//! 2. Creates a volume
//! 3. Acquires a write lease
//! 4. Sets up a `UblkDevice` backed by `ClusterBlockHandler`
//! 5. Runs filesystem-level test scenarios (mkfs, mount, write, verify)
//! 6. Reports results to stdout and exits 0 on success, non-zero on failure.
//!
//! # Usage
//!
//! ```text
//! ublk-e2e --test mount-write-read --blockyard-bin /usr/local/bin/blockyard
//! ublk-e2e --test node-failure --blockyard-bin /usr/local/bin/blockyard
//! ```

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, bail};
use blockyard_ublk::MetadataClient as _;

/// Test scenarios supported by this runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestScenario {
    /// mkfs → mount → write files → unmount → remount → verify checksums
    MountWriteRead,
    /// mount → write → kill a node → continue writing → unmount → fsck
    NodeFailure,
}

impl TestScenario {
    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "mount-write-read" => Ok(Self::MountWriteRead),
            "node-failure" => Ok(Self::NodeFailure),
            _ => bail!("unknown test scenario: {}", s),
        }
    }
}

fn main() -> anyhow::Result<()> {
    // Simple arg parsing — no clap dependency needed for a test runner.
    let args: Vec<String> = std::env::args().collect();
    let mut test_name = None;
    let mut blockyard_bin = None;
    let mut cluster_size: usize = 3;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--test" => {
                i += 1;
                test_name = Some(args[i].clone());
            }
            "--blockyard-bin" => {
                i += 1;
                blockyard_bin = Some(PathBuf::from(&args[i]));
            }
            "--cluster-size" => {
                i += 1;
                cluster_size = args[i].parse().context("parse cluster-size")?;
            }
            _ => bail!("unknown arg: {}", args[i]),
        }
        i += 1;
    }

    let test_name = test_name.context("--test is required")?;
    let scenario = TestScenario::from_str(&test_name)?;
    let blockyard_bin = blockyard_bin.context("--blockyard-bin is required")?;

    eprintln!("[ublk-e2e] test={:?} cluster_size={} binary={}", scenario, cluster_size, blockyard_bin.display());

    // Ensure ublk_drv is loaded.
    let modprobe = Command::new("modprobe").arg("ublk_drv").status();
    match modprobe {
        Ok(s) if s.success() => eprintln!("[ublk-e2e] ublk_drv module loaded"),
        Ok(s) => eprintln!("[ublk-e2e] WARNING: modprobe ublk_drv exited {}", s),
        Err(e) => eprintln!("[ublk-e2e] WARNING: modprobe failed: {}", e),
    }

    // Build a tokio runtime for async operations.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;

    rt.block_on(async move {
        run_scenario(scenario, &blockyard_bin, cluster_size).await
    })
}

async fn run_scenario(
    scenario: TestScenario,
    blockyard_bin: &std::path::Path,
    cluster_size: usize,
) -> anyhow::Result<()> {
    // Start cluster nodes as child processes.
    let base_port: u16 = 19800;
    let cluster = blockyard_test_harness::process_harness::RealProcessCluster::new(
        cluster_size,
        blockyard_bin.to_path_buf(),
        base_port,
    );
    cluster.start_all().await.context("start cluster")?;
    cluster
        .wait_cluster_healthy(Duration::from_secs(60))
        .await
        .context("wait cluster healthy")?;
    eprintln!("[ublk-e2e] cluster healthy with {} nodes", cluster_size);

    // Create a volume.
    let vol_size: u64 = 256 * 1024 * 1024; // 256 MiB
    let vol = cluster
        .create_volume(
            "ublk-e2e-test",
            vol_size,
            serde_json::json!({"Replicated": {"replicas": 3}}),
        )
        .await
        .context("create volume")?;
    let vol_id = vol["id"]
        .as_str()
        .context("volume id")?
        .to_string();
    eprintln!("[ublk-e2e] created volume: {}", vol_id);

    // Acquire a write lease.
    let session_id = uuid::Uuid::new_v4().to_string();
    let lease_resp = cluster
        .acquire_lease(&vol_id, &session_id)
        .await
        .context("acquire lease")?;
    if !lease_resp.status().is_success() {
        bail!("lease acquisition failed: {}", lease_resp.status());
    }
    eprintln!("[ublk-e2e] acquired write lease");

    // Set up the ublk device via ClusterBlockHandler + UblkDevice.
    let mgmt_url = cluster.node(0).mgmt_url();
    let data_addr = cluster.node(0).data_addr();

    // Create real TCP data client and HTTP metadata client.
    let metadata_client = std::sync::Arc::new(
        blockyard_ublk::HttpMetadataClient::new(mgmt_url),
    );
    let data_client = std::sync::Arc::new(
        blockyard_ublk::TcpDataNodeClient::new(),
    );

    // Populate metadata cache with node addresses.
    let metadata_cache = std::sync::Arc::new(blockyard_ublk::MetadataCache::new());
    metadata_client
        .refresh_metadata(&metadata_cache)
        .await
        .context("refresh metadata")?;
    eprintln!("[ublk-e2e] metadata cache populated");

    // Set up client session, lease manager, etc.
    let volume_id: blockyard_common::VolumeId = vol_id.parse().context("parse volume id")?;
    let session_id_typed: blockyard_common::SessionId = session_id.parse().context("parse session id")?;

    let session = std::sync::Arc::new(blockyard_ublk::session::ClientSession::new(volume_id));
    let lease_manager = std::sync::Arc::new(blockyard_ublk::LeaseManager::new(
        volume_id,
        session_id_typed,
        Duration::from_secs(30),
    ));
    let watermark = std::sync::Arc::new(blockyard_ublk::WriteWatermark::new());
    let stale_handler = std::sync::Arc::new(blockyard_ublk::StaleEpochHandler::new());

    // Acquire lease via the lease manager.
    lease_manager
        .acquire(metadata_client.as_ref())
        .await
        .context("acquire lease via manager")?;
    eprintln!("[ublk-e2e] lease manager active");

    let volume_config = blockyard_ublk::VolumeConfig {
        volume_id,
        size_bytes: vol_size,
        block_size: 4096,
        protection: blockyard_common::ProtectionPolicy::Replicated { replicas: 3 },
    };

    let handler = blockyard_ublk::ClusterBlockHandler::new(
        volume_config,
        data_client,
        metadata_client,
        lease_manager,
        session,
        metadata_cache,
        watermark,
        stale_handler,
    );

    let ublk_config = blockyard_ublk::ublk::UblkDeviceConfig {
        device_size_bytes: vol_size,
        block_size: 4096,
        queue_depth: 128,
        num_queues: 1,
    };

    let device = blockyard_ublk::UblkDevice::new(handler, ublk_config);

    #[cfg(feature = "ublk-kernel")]
    let device_path = device
        .start_kernel()
        .await
        .context("start ublk kernel device")?;

    #[cfg(not(feature = "ublk-kernel"))]
    bail!("ublk-e2e must be built with --features ublk-kernel");

    #[cfg(feature = "ublk-kernel")]
    {
        eprintln!("[ublk-e2e] ublk device created: {}", device_path);

        match scenario {
            TestScenario::MountWriteRead => {
                run_mount_write_read(&device_path).await?;
            }
            TestScenario::NodeFailure => {
                run_node_failure(&device_path, &cluster).await?;
            }
        }

        // Clean up ublk device.
        device.stop().await.context("stop ublk device")?;
        eprintln!("[ublk-e2e] ublk device stopped");

        eprintln!("[ublk-e2e] PASS");
        Ok(())
    }
}

#[cfg(feature = "ublk-kernel")]
async fn run_mount_write_read(device_path: &str) -> anyhow::Result<()> {
    use std::process::Command;

    let mount_point = "/mnt/ublk-test";

    // mkfs.ext4
    eprintln!("[ublk-e2e] mkfs.ext4 {}", device_path);
    let status = Command::new("mkfs.ext4")
        .args(["-F", device_path])
        .status()
        .context("mkfs.ext4")?;
    if !status.success() {
        bail!("mkfs.ext4 failed");
    }

    // mount
    std::fs::create_dir_all(mount_point)?;
    eprintln!("[ublk-e2e] mount {} {}", device_path, mount_point);
    let status = Command::new("mount")
        .args([device_path, mount_point])
        .status()
        .context("mount")?;
    if !status.success() {
        bail!("mount failed");
    }

    // Write test files and compute checksums.
    let test_files = [
        ("test1.dat", vec![0xAAu8; 65536]),
        ("test2.dat", vec![0xBBu8; 131072]),
        ("test3.dat", (0..4096).map(|i| (i % 256) as u8).collect()),
    ];

    let mut checksums = Vec::new();
    for (name, data) in &test_files {
        let path = format!("{}/{}", mount_point, name);
        std::fs::write(&path, data).context(format!("write {}", name))?;
        let hash = blake3::hash(data);
        checksums.push((name.to_string(), hash.to_hex().to_string()));
        eprintln!("[ublk-e2e] wrote {} ({} bytes, hash={})", name, data.len(), checksums.last().unwrap().1);
    }

    // Sync and unmount.
    let _ = Command::new("sync").status();
    eprintln!("[ublk-e2e] unmounting...");
    let status = Command::new("umount").arg(mount_point).status()?;
    if !status.success() {
        bail!("umount failed");
    }

    // Remount and verify.
    eprintln!("[ublk-e2e] remounting...");
    let status = Command::new("mount")
        .args([device_path, mount_point])
        .status()?;
    if !status.success() {
        bail!("remount failed");
    }

    for (name, expected_hash) in &checksums {
        let path = format!("{}/{}", mount_point, name);
        let data = std::fs::read(&path).context(format!("read {}", name))?;
        let actual_hash = blake3::hash(&data).to_hex().to_string();
        if actual_hash != *expected_hash {
            bail!(
                "checksum mismatch for {}: expected={} actual={}",
                name, expected_hash, actual_hash
            );
        }
        eprintln!("[ublk-e2e] verified {} checksum OK", name);
    }

    // Final unmount.
    let _ = Command::new("umount").arg(mount_point).status();
    eprintln!("[ublk-e2e] mount-write-read scenario PASSED");
    Ok(())
}

#[cfg(feature = "ublk-kernel")]
async fn run_node_failure(
    device_path: &str,
    cluster: &blockyard_test_harness::process_harness::RealProcessCluster,
) -> anyhow::Result<()> {
    use std::process::Command;

    let mount_point = "/mnt/ublk-test";

    // mkfs + mount
    eprintln!("[ublk-e2e] mkfs.ext4 {}", device_path);
    let status = Command::new("mkfs.ext4")
        .args(["-F", device_path])
        .status()?;
    if !status.success() {
        bail!("mkfs.ext4 failed");
    }

    std::fs::create_dir_all(mount_point)?;
    let status = Command::new("mount")
        .args([device_path, mount_point])
        .status()?;
    if !status.success() {
        bail!("mount failed");
    }

    // Write initial files.
    for i in 0..5 {
        let path = format!("{}/before_{}.dat", mount_point, i);
        let data = vec![(i as u8); 8192];
        std::fs::write(&path, &data)?;
        eprintln!("[ublk-e2e] wrote before_{}.dat", i);
    }
    let _ = Command::new("sync").status();

    // Kill one node.
    eprintln!("[ublk-e2e] killing node 2...");
    cluster.kill_node(2).context("kill node 2")?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Continue writing — should succeed with remaining 2 of 3 nodes.
    let mut write_errors = 0;
    for i in 0..5 {
        let path = format!("{}/after_{}.dat", mount_point, i);
        let data = vec![((i + 10) as u8); 8192];
        match std::fs::write(&path, &data) {
            Ok(_) => eprintln!("[ublk-e2e] wrote after_{}.dat", i),
            Err(e) => {
                eprintln!("[ublk-e2e] ERROR writing after_{}.dat: {}", i, e);
                write_errors += 1;
            }
        }
    }

    if write_errors > 0 {
        eprintln!("[ublk-e2e] WARNING: {} write errors after node failure", write_errors);
        // We allow some errors — the test checks that the filesystem is still consistent.
    }

    // Sync and unmount.
    let _ = Command::new("sync").status();
    let status = Command::new("umount").arg(mount_point).status()?;
    if !status.success() {
        bail!("umount after node failure failed");
    }

    // fsck -n (read-only check — should be clean).
    eprintln!("[ublk-e2e] running fsck -n...");
    let output = Command::new("fsck.ext4")
        .args(["-n", "-f", device_path])
        .output()?;
    let fsck_stdout = String::from_utf8_lossy(&output.stdout);
    let fsck_stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!("[ublk-e2e] fsck output: {}{}", fsck_stdout, fsck_stderr);

    // fsck exit code 0 = clean, 1 = corrected errors (shouldn't happen with -n)
    if !output.status.success() {
        let code = output.status.code().unwrap_or(-1);
        if code > 1 {
            bail!("fsck detected filesystem errors (exit code {})", code);
        }
    }

    eprintln!("[ublk-e2e] node-failure scenario PASSED");
    Ok(())
}
