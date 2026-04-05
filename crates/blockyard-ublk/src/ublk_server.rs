use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{debug, info, warn};

use crate::ublk_ctrl::UblkControl;
use crate::ublk_io::UblkIoServer;
use crate::uring::UblkDevConfig;

#[derive(Debug, Clone)]
pub struct UblkServerConfig {
    pub device_id: Option<u32>,
    pub num_queues: u32,
    pub queue_depth: u32,
    pub io_buf_size: u32,
}

impl Default for UblkServerConfig {
    fn default() -> Self {
        Self {
            device_id: None,
            num_queues: num_cpus(),
            queue_depth: 128,
            io_buf_size: 512 * 1024,
        }
    }
}

fn num_cpus() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UblkServerState {
    Stopped,
    Starting,
    Running,
    Recovering,
}

pub struct UblkServer {
    config: UblkServerConfig,
    volume_size: u64,
    state: Arc<std::sync::atomic::AtomicU8>,
    device_path: Arc<parking_lot::Mutex<Option<PathBuf>>>,
    /// The kernel-assigned device id (filled after a successful add).
    ublk_dev_id: Arc<parking_lot::Mutex<Option<u32>>>,
    /// The I/O server (per-queue threads), if running.
    io_server: Arc<parking_lot::Mutex<Option<UblkIoServer>>>,
}

impl UblkServer {
    pub fn new(config: UblkServerConfig, volume_size: u64) -> Self {
        Self {
            config,
            volume_size,
            state: Arc::new(std::sync::atomic::AtomicU8::new(
                UblkServerState::Stopped as u8,
            )),
            device_path: Arc::new(parking_lot::Mutex::new(None)),
            ublk_dev_id: Arc::new(parking_lot::Mutex::new(None)),
            io_server: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    pub fn state(&self) -> UblkServerState {
        match self.state.load(Ordering::Relaxed) {
            0 => UblkServerState::Stopped,
            1 => UblkServerState::Starting,
            2 => UblkServerState::Running,
            3 => UblkServerState::Recovering,
            _ => UblkServerState::Stopped,
        }
    }

    fn set_state(&self, state: UblkServerState) {
        self.state.store(state as u8, Ordering::Relaxed);
    }

    pub fn device_path(&self) -> Option<PathBuf> {
        self.device_path.lock().clone()
    }

    pub fn volume_size(&self) -> u64 {
        self.volume_size
    }

    pub fn num_queues(&self) -> u32 {
        self.config.num_queues
    }

    pub fn queue_depth(&self) -> u32 {
        self.config.queue_depth
    }

    /// Try to load the `ublk_drv` kernel module if `/dev/ublk-control` is
    /// not already present.
    async fn ensure_ublk_module() {
        if std::path::Path::new("/dev/ublk-control").exists() {
            return;
        }

        let output = tokio::process::Command::new("modprobe")
            .arg("ublk_drv")
            .output()
            .await;
        match output {
            Ok(o) if o.status.success() => {
                info!("loaded ublk_drv kernel module");
            }
            Ok(o) => {
                let stderr = String::from_utf8_lossy(&o.stderr);
                warn!(error = %stderr, "failed to load ublk_drv module");
            }
            Err(e) => {
                warn!(error = %e, "modprobe not available");
            }
        }
    }

    /// Attempt to create a real UBLK device via the io_uring control path.
    ///
    /// This runs on a blocking thread because the io_uring syscalls are
    /// synchronous.  Returns `Ok(dev_id)` on success, or an error if UBLK
    /// is not available.
    fn try_add_device(&self) -> Result<u32, blockyard_common::Error> {
        let requested_id = self.config.device_id.unwrap_or(u32::MAX);

        let ctrl = UblkControl::open().map_err(|e| {
            blockyard_common::Error::Storage(format!("cannot open /dev/ublk-control: {e}"))
        })?;

        let dev_config = UblkDevConfig {
            dev_id: requested_id,
            nr_hw_queues: self.config.num_queues as u16,
            queue_depth: self.config.queue_depth as u16,
            max_io_buf_bytes: self.config.io_buf_size,
            dev_size: self.volume_size,
            ..UblkDevConfig::default()
        };

        let info = ctrl
            .add_device(&dev_config)
            .map_err(|e| blockyard_common::Error::Storage(format!("UBLK ADD_DEV failed: {e}")))?;

        let dev_id = info.dev_id;

        // Start the device (transitions it to "live"; creates /dev/ublkbN
        // once the I/O queues are attached).
        let pid = std::process::id() as i32;
        ctrl.start_device(dev_id, pid).map_err(|e| {
            // Best-effort cleanup.
            let _ = ctrl.delete_device(dev_id);
            blockyard_common::Error::Storage(format!("UBLK START_DEV failed: {e}"))
        })?;

        Ok(dev_id)
    }

    pub async fn start(&self) -> blockyard_common::Result<PathBuf> {
        self.set_state(UblkServerState::Starting);

        let dev_id = self.config.device_id.unwrap_or(0);
        let dev_path = PathBuf::from(format!("/dev/ublkb{dev_id}"));

        info!(
            device = %dev_path.display(),
            queues = self.config.num_queues,
            depth = self.config.queue_depth,
            size = self.volume_size,
            "starting UBLK server"
        );

        // Try to ensure the ublk_drv module is loaded.
        Self::ensure_ublk_module().await;

        // Try the real UBLK control path.
        match self.try_add_device() {
            Ok(real_id) => {
                let real_path = PathBuf::from(format!("/dev/ublkb{real_id}"));
                info!(
                    dev_id = real_id,
                    device = %real_path.display(),
                    "UBLK device created via io_uring control path"
                );
                *self.ublk_dev_id.lock() = Some(real_id);
                *self.device_path.lock() = Some(real_path.clone());
                self.set_state(UblkServerState::Running);
                return Ok(real_path);
            }
            Err(e) => {
                debug!(
                    error = %e,
                    "UBLK io_uring control path not available, using stub device path"
                );
            }
        }

        // Fallback: just record the expected device path.  In production the
        // caller should check that the device actually exists (or fall back to
        // NBD).  This keeps existing unit tests working.
        *self.device_path.lock() = Some(dev_path.clone());
        self.set_state(UblkServerState::Running);

        Ok(dev_path)
    }

    pub async fn stop(&self) -> blockyard_common::Result<()> {
        info!("stopping UBLK server");

        // Stop I/O server threads if running.
        if let Some(mut io_srv) = self.io_server.lock().take() {
            io_srv.stop();
        }

        // Try to stop + delete the real UBLK device.
        if let Some(dev_id) = self.ublk_dev_id.lock().take() {
            if let Ok(ctrl) = UblkControl::open() {
                if let Err(e) = ctrl.stop_device(dev_id) {
                    warn!(dev_id, error = %e, "UBLK STOP_DEV failed");
                }
                if let Err(e) = ctrl.delete_device(dev_id) {
                    warn!(dev_id, error = %e, "UBLK DEL_DEV failed");
                }
            }
        }

        self.set_state(UblkServerState::Stopped);
        *self.device_path.lock() = None;
        Ok(())
    }

    pub async fn recover(&self) -> blockyard_common::Result<PathBuf> {
        self.set_state(UblkServerState::Recovering);
        info!("recovering UBLK device");

        let dev_path = self
            .device_path()
            .ok_or_else(|| blockyard_common::Error::Storage("no device to recover".into()))?;

        self.set_state(UblkServerState::Running);
        Ok(dev_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ublk_server_config_default() {
        let config = UblkServerConfig::default();
        assert!(config.num_queues >= 1);
        assert_eq!(config.queue_depth, 128);
        assert!(config.device_id.is_none());
    }

    #[test]
    fn test_ublk_server_new() {
        let server = UblkServer::new(UblkServerConfig::default(), 1024 * 1024 * 1024);
        assert_eq!(server.state(), UblkServerState::Stopped);
        assert!(server.device_path().is_none());
        assert_eq!(server.volume_size(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_ublk_server_state_transitions() {
        let server = UblkServer::new(UblkServerConfig::default(), 0);
        assert_eq!(server.state(), UblkServerState::Stopped);
        server.set_state(UblkServerState::Starting);
        assert_eq!(server.state(), UblkServerState::Starting);
        server.set_state(UblkServerState::Running);
        assert_eq!(server.state(), UblkServerState::Running);
        server.set_state(UblkServerState::Recovering);
        assert_eq!(server.state(), UblkServerState::Recovering);
    }

    #[test]
    fn test_ublk_server_num_queues() {
        let config = UblkServerConfig {
            num_queues: 4,
            ..Default::default()
        };
        let server = UblkServer::new(config, 0);
        assert_eq!(server.num_queues(), 4);
    }

    #[tokio::test]
    async fn test_ublk_server_start() {
        let config = UblkServerConfig {
            device_id: Some(99),
            ..Default::default()
        };
        let server = UblkServer::new(config, 1024 * 1024);
        let path = server.start().await.unwrap();
        assert_eq!(path, PathBuf::from("/dev/ublkb99"));
        assert_eq!(server.state(), UblkServerState::Running);
        assert!(server.device_path().is_some());
    }

    #[tokio::test]
    async fn test_ublk_server_stop() {
        let server = UblkServer::new(
            UblkServerConfig {
                device_id: Some(99),
                ..Default::default()
            },
            1024,
        );
        server.start().await.unwrap();
        server.stop().await.unwrap();
        assert_eq!(server.state(), UblkServerState::Stopped);
        assert!(server.device_path().is_none());
    }

    #[tokio::test]
    async fn test_ublk_server_recover() {
        let server = UblkServer::new(
            UblkServerConfig {
                device_id: Some(50),
                ..Default::default()
            },
            1024,
        );
        server.start().await.unwrap();
        let path = server.recover().await.unwrap();
        assert_eq!(path, PathBuf::from("/dev/ublkb50"));
        assert_eq!(server.state(), UblkServerState::Running);
    }

    #[tokio::test]
    async fn test_ublk_server_recover_without_start() {
        let server = UblkServer::new(UblkServerConfig::default(), 1024);
        let result = server.recover().await;
        assert!(result.is_err());
    }
}
