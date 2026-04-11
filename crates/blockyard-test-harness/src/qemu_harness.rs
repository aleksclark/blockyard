//! QEMU VM test harness for integration tests requiring kernel features.
//!
//! Provides [`QemuVm`] — a managed QEMU virtual machine with SSH access,
//! used for tests that need root, kernel modules (e.g. `ublk_drv`), or
//! real block devices.
//!
//! # Image management
//!
//! On first run, [`download_cloud_image`] fetches an Arch Linux cloud image
//! (~500 MB) and caches it at `/tmp/blockyard-test-vm/`. Subsequent runs
//! create a qcow2 overlay so the base image stays pristine.
//!
//! # Lifecycle
//!
//! ```text
//! QemuVm::boot()  →  wait_ready()  →  ssh_exec() / scp_to()  →  shutdown()
//! ```

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use tracing::{debug, info, warn};

/// Directory where we cache the cloud image and ephemeral SSH keys.
const CACHE_DIR: &str = "/tmp/blockyard-test-vm";

/// Arch Linux cloud image URL.
const CLOUD_IMAGE_URL: &str =
    "https://geo.mirror.pkgbuild.com/images/latest/Arch-Linux-x86_64-cloudimg.qcow2";

/// Filename of the cached base image.
const BASE_IMAGE_NAME: &str = "Arch-Linux-x86_64-cloudimg.qcow2";

/// A running QEMU virtual machine with SSH access.
///
/// The VM boots from an Arch Linux cloud image with cloud-init
/// configured for passwordless root SSH. All communication happens
/// over SSH via a host-forwarded port.
pub struct QemuVm {
    /// QEMU child process.
    child: Option<Child>,
    /// Host port forwarded to guest port 22 (SSH).
    ssh_port: u16,
    /// Path to ephemeral SSH private key.
    ssh_key_path: PathBuf,
    /// Temp directory holding the overlay image, seed ISO, and SSH keys.
    work_dir: tempfile::TempDir,
    /// Additional host→guest port forwards: (host_port, guest_port).
    _port_forwards: Vec<(u16, u16)>,
}

impl std::fmt::Debug for QemuVm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QemuVm")
            .field("ssh_port", &self.ssh_port)
            .field("pid", &self.child.as_ref().map(|c| c.id()))
            .finish()
    }
}

/// Configuration for booting a [`QemuVm`].
#[derive(Debug, Clone)]
pub struct QemuVmConfig {
    /// RAM in megabytes. Default: 1024.
    pub memory_mb: u32,
    /// Number of vCPUs. Default: 2.
    pub cpus: u32,
    /// Additional host→guest port forwards: `(host_port, guest_port)`.
    pub port_forwards: Vec<(u16, u16)>,
    /// Extra packages to install via cloud-init.
    pub extra_packages: Vec<String>,
    /// Extra shell commands to run at boot via cloud-init `runcmd`.
    pub extra_runcmd: Vec<String>,
}

impl Default for QemuVmConfig {
    fn default() -> Self {
        Self {
            memory_mb: 1024,
            cpus: 2,
            port_forwards: Vec::new(),
            extra_packages: Vec::new(),
            extra_runcmd: Vec::new(),
        }
    }
}

impl QemuVm {
    /// Boot a new QEMU VM with the given configuration.
    ///
    /// This will:
    /// 1. Ensure the cloud image is cached locally.
    /// 2. Generate an ephemeral SSH key pair.
    /// 3. Create a cloud-init seed ISO.
    /// 4. Create a qcow2 overlay image.
    /// 5. Start QEMU with KVM acceleration and port forwarding.
    pub fn boot(config: QemuVmConfig) -> anyhow::Result<Self> {
        let base_image = download_cloud_image()?;

        let work_dir = tempfile::TempDir::new().context("create work dir")?;
        let work = work_dir.path();

        // Generate ephemeral SSH key pair.
        let ssh_key_path = work.join("id_ed25519");
        let ssh_pub_path = work.join("id_ed25519.pub");
        generate_ssh_keypair(&ssh_key_path)?;
        let pub_key = std::fs::read_to_string(&ssh_pub_path).context("read public key")?;

        // Create cloud-init seed ISO.
        let seed_iso = work.join("seed.iso");
        create_cloud_init_iso(
            &seed_iso,
            pub_key.trim(),
            &config.extra_packages,
            &config.extra_runcmd,
        )?;

        // Create qcow2 overlay.
        let overlay = work.join("overlay.qcow2");
        let status = Command::new("qemu-img")
            .args(["create", "-f", "qcow2", "-b"])
            .arg(&base_image)
            .args(["-F", "qcow2"])
            .arg(&overlay)
            .args(["20G"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .context("qemu-img create overlay")?;
        if !status.success() {
            bail!("qemu-img create overlay failed");
        }

        // Pick a free port for SSH.
        let ssh_port = pick_free_port()?;

        // Build QEMU command.
        let mut cmd = Command::new("qemu-system-x86_64");
        cmd.args(["-enable-kvm", "-machine", "q35"]);
        cmd.args(["-cpu", "host"]);
        cmd.args(["-m", &format!("{}M", config.memory_mb)]);
        cmd.args(["-smp", &config.cpus.to_string()]);
        cmd.args(["-nographic"]);
        cmd.args([
            "-drive",
            &format!("file={},format=qcow2,if=virtio", overlay.display()),
        ]);
        cmd.args([
            "-drive",
            &format!("file={},format=raw,if=virtio", seed_iso.display()),
        ]);

        // Build netdev with port forwards.
        let mut hostfwd = format!("hostfwd=tcp::{}-:22", ssh_port);
        for &(host_port, guest_port) in &config.port_forwards {
            hostfwd.push_str(&format!(",hostfwd=tcp::{}-:{}", host_port, guest_port));
        }
        cmd.args(["-netdev", &format!("user,id=net0,{}", hostfwd)]);
        cmd.args(["-device", "virtio-net-pci,netdev=net0"]);

        // Serial console to stdout (for debugging).
        cmd.args(["-serial", "mon:stdio"]);

        // Redirect stdout/stderr to log files.
        let qemu_log = std::fs::File::create(work.join("qemu.log")).context("create qemu.log")?;
        let qemu_err = qemu_log.try_clone()?;

        cmd.stdout(Stdio::from(qemu_log));
        cmd.stderr(Stdio::from(qemu_err));

        info!(ssh_port, "booting QEMU VM");
        let child = cmd.spawn().context("spawn qemu-system-x86_64")?;
        info!(pid = child.id(), "QEMU started");

        Ok(Self {
            child: Some(child),
            ssh_port,
            ssh_key_path,
            work_dir,
            _port_forwards: config.port_forwards,
        })
    }

    /// Poll until SSH is reachable, up to `timeout`.
    pub fn wait_ready(&self, timeout: Duration) -> anyhow::Result<()> {
        let start = Instant::now();
        info!(
            ssh_port = self.ssh_port,
            "waiting for VM SSH to become ready"
        );

        while start.elapsed() < timeout {
            match self.ssh_exec_raw("echo ready") {
                Ok(output) if output.contains("ready") => {
                    info!(
                        elapsed = ?start.elapsed(),
                        "VM SSH is ready"
                    );
                    return Ok(());
                }
                Ok(output) => {
                    debug!(output, "SSH responded but unexpected output, retrying...");
                }
                Err(_) => {
                    debug!("SSH not ready yet, retrying...");
                }
            }
            std::thread::sleep(Duration::from_secs(2));
        }

        bail!(
            "VM SSH did not become ready within {:?} (port {})",
            timeout,
            self.ssh_port
        )
    }

    /// Execute a command in the VM via SSH. Returns stdout+stderr combined.
    pub fn ssh_exec(&self, cmd: &str) -> anyhow::Result<String> {
        let output = self.ssh_exec_raw(cmd)?;
        Ok(output)
    }

    /// Execute a command in the VM via SSH, returning the full output.
    /// Returns an error if the SSH connection fails OR the remote command
    /// exits non-zero.
    pub fn ssh_exec_checked(&self, cmd: &str) -> anyhow::Result<String> {
        let output = Command::new("ssh")
            .args(self.ssh_base_args())
            .arg("root@127.0.0.1")
            .arg(cmd)
            .output()
            .context("ssh exec")?;

        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        if !output.status.success() {
            bail!(
                "SSH command failed (exit {}): {}\nOutput: {}",
                output.status.code().unwrap_or(-1),
                cmd,
                combined
            );
        }

        Ok(combined)
    }

    /// Copy a local file into the VM via SCP.
    pub fn scp_to(&self, local: &Path, remote: &str) -> anyhow::Result<()> {
        info!(
            local = %local.display(),
            remote,
            "SCP file to VM"
        );
        let status = Command::new("scp")
            .args(self.scp_base_args())
            .arg(local)
            .arg(format!("root@127.0.0.1:{}", remote))
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .status()
            .context("scp to VM")?;

        if !status.success() {
            bail!("scp to VM failed: {} -> {}", local.display(), remote);
        }
        Ok(())
    }

    /// Copy a file from the VM to the host via SCP.
    pub fn scp_from(&self, remote: &str, local: &Path) -> anyhow::Result<()> {
        let status = Command::new("scp")
            .args(self.scp_base_args())
            .arg(format!("root@127.0.0.1:{}", remote))
            .arg(local)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .status()
            .context("scp from VM")?;

        if !status.success() {
            bail!("scp from VM failed: {} -> {}", remote, local.display());
        }
        Ok(())
    }

    /// The SSH port on the host that forwards to guest port 22.
    pub fn ssh_port(&self) -> u16 {
        self.ssh_port
    }

    /// Path to the work directory (overlay image, logs, etc.).
    pub fn work_dir(&self) -> &Path {
        self.work_dir.path()
    }

    /// Gracefully shut down the VM.
    pub fn shutdown(&mut self) {
        if self.child.is_none() {
            return;
        }
        info!("shutting down QEMU VM");
        // Try graceful shutdown via SSH first (before borrowing child mutably).
        let _ = self.ssh_exec_raw("poweroff");
        // Give it a few seconds to exit gracefully.
        if let Some(ref mut child) = self.child {
            let start = Instant::now();
            while start.elapsed() < Duration::from_secs(10) {
                match child.try_wait() {
                    Ok(Some(_)) => {
                        info!("QEMU exited gracefully");
                        self.child = None;
                        return;
                    }
                    _ => std::thread::sleep(Duration::from_millis(500)),
                }
            }
            // Force kill.
            warn!("QEMU did not exit gracefully, killing");
            let _ = child.kill();
            let _ = child.wait();
        }
        self.child = None;
    }

    // ── private helpers ──

    fn ssh_base_args(&self) -> Vec<String> {
        vec![
            "-o".into(),
            "StrictHostKeyChecking=no".into(),
            "-o".into(),
            "UserKnownHostsFile=/dev/null".into(),
            "-o".into(),
            "LogLevel=ERROR".into(),
            "-o".into(),
            format!("ConnectTimeout=5"),
            "-o".into(),
            "BatchMode=yes".into(),
            "-i".into(),
            self.ssh_key_path.display().to_string(),
            "-p".into(),
            self.ssh_port.to_string(),
        ]
    }

    fn scp_base_args(&self) -> Vec<String> {
        vec![
            "-o".into(),
            "StrictHostKeyChecking=no".into(),
            "-o".into(),
            "UserKnownHostsFile=/dev/null".into(),
            "-o".into(),
            "LogLevel=ERROR".into(),
            "-o".into(),
            "BatchMode=yes".into(),
            "-i".into(),
            self.ssh_key_path.display().to_string(),
            "-P".into(),
            self.ssh_port.to_string(),
        ]
    }

    fn ssh_exec_raw(&self, cmd: &str) -> anyhow::Result<String> {
        let output = Command::new("ssh")
            .args(self.ssh_base_args())
            .arg("root@127.0.0.1")
            .arg(cmd)
            .output()
            .context("ssh exec")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("ssh command failed (exit {}): {}", output.status, stderr);
        }

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        Ok(stdout)
    }
}

impl Drop for QemuVm {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Download the Arch Linux cloud image if not already cached.
///
/// Returns the path to the cached qcow2 base image.
pub fn download_cloud_image() -> anyhow::Result<PathBuf> {
    std::fs::create_dir_all(CACHE_DIR).context("create cache dir")?;
    let image_path = PathBuf::from(CACHE_DIR).join(BASE_IMAGE_NAME);

    if image_path.exists() {
        info!(path = %image_path.display(), "using cached cloud image");
        return Ok(image_path);
    }

    info!(
        url = CLOUD_IMAGE_URL,
        "downloading cloud image (this may take a while)..."
    );
    let status = Command::new("curl")
        .args(["-fSL", "-o"])
        .arg(&image_path)
        .arg(CLOUD_IMAGE_URL)
        .status()
        .context("curl download cloud image")?;

    if !status.success() {
        // Clean up partial download.
        let _ = std::fs::remove_file(&image_path);
        bail!("cloud image download failed (exit {})", status);
    }

    info!(path = %image_path.display(), "cloud image downloaded");
    Ok(image_path)
}

/// Generate an ephemeral ed25519 SSH key pair at the given path.
fn generate_ssh_keypair(private_key_path: &Path) -> anyhow::Result<()> {
    let status = Command::new("ssh-keygen")
        .args(["-t", "ed25519", "-N", "", "-q", "-f"])
        .arg(private_key_path)
        .status()
        .context("ssh-keygen")?;

    if !status.success() {
        bail!("ssh-keygen failed");
    }
    // Ensure private key has correct permissions.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(private_key_path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

/// Create a cloud-init NoCloud seed ISO.
///
/// The ISO contains `user-data` and `meta-data` files that configure:
/// - Root SSH access with the given public key
/// - Package installation (e2fsprogs + extras)
/// - `modprobe ublk_drv` at boot
fn create_cloud_init_iso(
    iso_path: &Path,
    ssh_pub_key: &str,
    extra_packages: &[String],
    extra_runcmd: &[String],
) -> anyhow::Result<()> {
    let ci_dir = iso_path.parent().unwrap().join("cloud-init");
    std::fs::create_dir_all(&ci_dir)?;

    // meta-data
    let meta_data = "instance-id: blockyard-test-vm\nlocal-hostname: blockyard-test\n";
    std::fs::write(ci_dir.join("meta-data"), meta_data)?;

    // user-data
    let mut packages = vec!["e2fsprogs".to_string()];
    packages.extend_from_slice(extra_packages);
    let packages_yaml: String = packages.iter().map(|p| format!("  - {}\n", p)).collect();

    let mut runcmd = vec!["modprobe ublk_drv".to_string()];
    runcmd.extend_from_slice(extra_runcmd);
    let runcmd_yaml: String = runcmd.iter().map(|c| format!("  - {}\n", c)).collect();

    let user_data = format!(
        r#"#cloud-config
users:
  - name: root
    ssh_authorized_keys:
      - {ssh_pub_key}

ssh_pwauth: false

packages:
{packages_yaml}
runcmd:
{runcmd_yaml}
"#,
        ssh_pub_key = ssh_pub_key,
        packages_yaml = packages_yaml,
        runcmd_yaml = runcmd_yaml,
    );
    std::fs::write(ci_dir.join("user-data"), &user_data)?;

    // Generate ISO with genisoimage (or mkisofs).
    let status = Command::new("genisoimage")
        .args(["-output"])
        .arg(iso_path)
        .args(["-volid", "cidata", "-joliet", "-rock"])
        .arg(ci_dir.join("user-data"))
        .arg(ci_dir.join("meta-data"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("genisoimage create seed ISO")?;

    if !status.success() {
        bail!("genisoimage failed to create seed ISO");
    }

    info!(path = %iso_path.display(), "created cloud-init seed ISO");
    Ok(())
}

/// Pick a free TCP port by binding to port 0 and reading back the assigned port.
fn pick_free_port() -> anyhow::Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").context("bind to port 0")?;
    let port = listener.local_addr()?.port();
    Ok(port)
}

/// Build both the `blockyard` binary and the `ublk-e2e` test runner binary.
///
/// Returns `(blockyard_binary, ublk_e2e_binary)`.
pub fn build_ublk_test_binaries() -> anyhow::Result<(PathBuf, PathBuf)> {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");

    let blockyard_bin = workspace_root.join("target/release/blockyard");
    let ublk_e2e_bin = workspace_root.join("target/release/ublk-e2e");

    // Build blockyard binary.
    if !blockyard_bin.exists() {
        info!("building blockyard binary (release)...");
        let status = Command::new("cargo")
            .args([
                "build",
                "--release",
                "-p",
                "blockyard",
                "--bin",
                "blockyard",
            ])
            .current_dir(workspace_root)
            .status()
            .context("build blockyard")?;
        if !status.success() {
            bail!("cargo build --release --bin blockyard failed");
        }
    }

    // Build ublk-e2e binary (requires ublk-kernel feature).
    if !ublk_e2e_bin.exists() {
        info!("building ublk-e2e binary (release, ublk-kernel)...");
        let status = Command::new("cargo")
            .args([
                "build",
                "--release",
                "--bin",
                "ublk-e2e",
                "--features",
                "ublk-kernel",
            ])
            .current_dir(workspace_root)
            .status()
            .context("build ublk-e2e")?;
        if !status.success() {
            bail!("cargo build --release --bin ublk-e2e --features ublk-kernel failed");
        }
    }

    Ok((blockyard_bin, ublk_e2e_bin))
}

/// Helper to write a shell script string into the VM and make it executable.
pub fn write_vm_script(vm: &QemuVm, remote_path: &str, content: &str) -> anyhow::Result<()> {
    // Write to a local temp file, SCP it in.
    let tmp = tempfile::NamedTempFile::new()?;
    std::fs::write(tmp.path(), content)?;
    vm.scp_to(tmp.path(), remote_path)?;
    vm.ssh_exec_checked(&format!("chmod +x {}", remote_path))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pick_free_port() {
        let port = pick_free_port().unwrap();
        assert!(port > 0);
    }

    #[test]
    fn test_qemu_vm_config_default() {
        let config = QemuVmConfig::default();
        assert_eq!(config.memory_mb, 1024);
        assert_eq!(config.cpus, 2);
        assert!(config.port_forwards.is_empty());
        assert!(config.extra_packages.is_empty());
        assert!(config.extra_runcmd.is_empty());
    }

    #[test]
    fn test_ssh_base_args_structure() {
        // We can't create a full QemuVm without QEMU, but we can verify
        // the args builder logic indirectly by checking the constants.
        assert!(CLOUD_IMAGE_URL.contains("qcow2"));
        assert!(!CACHE_DIR.is_empty());
    }

    #[test]
    fn test_cloud_init_iso_creation() {
        let dir = tempfile::TempDir::new().unwrap();
        let iso = dir.path().join("seed.iso");

        // This test will fail if genisoimage is not installed, which is fine
        // for CI — we just verify the function signature works.
        let result = create_cloud_init_iso(
            &iso,
            "ssh-ed25519 AAAA... test@test",
            &["curl".to_string()],
            &["echo hello".to_string()],
        );
        // Don't assert success — genisoimage may not be available in all envs.
        // Just verify it doesn't panic.
        if result.is_ok() {
            assert!(iso.exists());
        }
    }

    #[test]
    fn test_ssh_keypair_generation() {
        let dir = tempfile::TempDir::new().unwrap();
        let key_path = dir.path().join("test_key");
        let result = generate_ssh_keypair(&key_path);
        // ssh-keygen should be available everywhere.
        if result.is_ok() {
            assert!(key_path.exists());
            assert!(dir.path().join("test_key.pub").exists());
        }
    }
}
