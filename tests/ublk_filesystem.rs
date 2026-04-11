//! Full-stack UBLK filesystem tests with real blockyard processes in a QEMU VM.
//!
//! These tests boot a QEMU VM with KVM, SCP the blockyard and ublk-e2e
//! binaries into it, and execute end-to-end filesystem scenarios that
//! exercise the real ublk kernel driver.
//!
//! Requirements (checked at runtime):
//! - QEMU (`qemu-system-x86_64`) with KVM (`/dev/kvm`)
//! - `genisoimage` for cloud-init seed ISO creation
//! - Internet access for first-run cloud image download (~500 MB, cached)
//! - `ssh`, `scp`, `ssh-keygen` in PATH

use std::time::Duration;

use blockyard_test_harness::qemu_harness::{QemuVm, QemuVmConfig, build_ublk_test_binaries};

/// Check if the QEMU VM test prerequisites are available.
fn check_prerequisites() -> bool {
    // Check /dev/kvm exists.
    if !std::path::Path::new("/dev/kvm").exists() {
        eprintln!("SKIP: /dev/kvm not available");
        return false;
    }

    // Check qemu-system-x86_64 is in PATH.
    if std::process::Command::new("qemu-system-x86_64")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_err()
    {
        eprintln!("SKIP: qemu-system-x86_64 not found");
        return false;
    }

    // Check genisoimage is in PATH.
    if std::process::Command::new("genisoimage")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_err()
    {
        eprintln!("SKIP: genisoimage not found");
        return false;
    }

    true
}

/// Boot a VM, SCP binaries in, run a test scenario, check the result.
async fn run_vm_test(scenario: &str) {
    if !check_prerequisites() {
        eprintln!("Skipping QEMU VM test — prerequisites not met");
        return;
    }

    // Build the binaries.
    let (blockyard_bin, ublk_e2e_bin) = build_ublk_test_binaries().expect("build test binaries");

    // Boot the VM.
    let config = QemuVmConfig {
        memory_mb: 1024,
        cpus: 2,
        extra_packages: vec!["e2fsprogs".to_string()],
        extra_runcmd: vec!["modprobe ublk_drv".to_string()],
        ..Default::default()
    };
    let mut vm = QemuVm::boot(config).expect("boot QEMU VM");
    vm.wait_ready(Duration::from_secs(180))
        .expect("VM SSH ready");

    // SCP binaries into the VM.
    vm.scp_to(&blockyard_bin, "/usr/local/bin/blockyard")
        .expect("scp blockyard binary");
    vm.ssh_exec_checked("chmod +x /usr/local/bin/blockyard")
        .expect("chmod blockyard");

    vm.scp_to(&ublk_e2e_bin, "/usr/local/bin/ublk-e2e")
        .expect("scp ublk-e2e binary");
    vm.ssh_exec_checked("chmod +x /usr/local/bin/ublk-e2e")
        .expect("chmod ublk-e2e");

    // Ensure ublk_drv module is loaded.
    vm.ssh_exec_checked("modprobe ublk_drv")
        .expect("modprobe ublk_drv");

    // Run the test scenario inside the VM.
    let cmd = format!(
        "/usr/local/bin/ublk-e2e --test {} --blockyard-bin /usr/local/bin/blockyard --cluster-size 3",
        scenario
    );
    let output = vm.ssh_exec_checked(&cmd).expect("ublk-e2e test runner");

    eprintln!("=== ublk-e2e output ===\n{}\n=== end ===", output);

    // Verify PASS in output.
    assert!(
        output.contains("PASS"),
        "ublk-e2e did not report PASS. Output:\n{}",
        output
    );

    // Shutdown VM (also happens on drop).
    vm.shutdown();
}

#[tokio::test]
async fn test_mount_format_write_read() {
    run_vm_test("mount-write-read").await;
}

#[tokio::test]
async fn test_mount_node_failure_fs_survives() {
    run_vm_test("node-failure").await;
}
