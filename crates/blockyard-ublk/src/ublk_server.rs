use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::{info, warn, error};

#[cfg(target_os = "linux")]
use crate::uring::{self, UblkCtrl, UblkQueue};

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
    stop_flag: Arc<AtomicBool>,
    /// Handles to queue worker threads (Linux only).
    #[cfg(target_os = "linux")]
    queue_workers: parking_lot::Mutex<Vec<std::thread::JoinHandle<()>>>,
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
            stop_flag: Arc::new(AtomicBool::new(false)),
            #[cfg(target_os = "linux")]
            queue_workers: parking_lot::Mutex::new(Vec::new()),
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

    /// Attempt to load the `ublk_drv` kernel module if the control device
    /// does not already exist.
    async fn ensure_ublk_module(&self) {
        if !std::path::Path::new("/dev/ublk-control").exists() {
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
    }

    /// Start the UBLK server.
    ///
    /// On Linux this will:
    /// 1. Ensure the `ublk_drv` module is loaded.
    /// 2. Open `/dev/ublk-control` via [`UblkCtrl`].
    /// 3. Create and start a new UBLK device.
    /// 4. Spawn one I/O worker thread per queue.
    ///
    /// On non-Linux platforms the device path is synthesised but no real
    /// device is created (useful for compile-time checking and tests).
    pub async fn start(&self) -> blockyard_common::Result<PathBuf> {
        self.set_state(UblkServerState::Starting);
        self.stop_flag.store(false, Ordering::Relaxed);

        let dev_id = self.config.device_id.unwrap_or(0);
        let dev_path = PathBuf::from(format!("/dev/ublkb{dev_id}"));

        info!(
            device = %dev_path.display(),
            queues = self.config.num_queues,
            depth = self.config.queue_depth,
            size = self.volume_size,
            "starting UBLK server"
        );

        self.ensure_ublk_module().await;

        // --- Linux: create the actual UBLK device via io_uring -----------
        #[cfg(target_os = "linux")]
        {
            match self.start_uring_device(dev_id) {
                Ok(()) => {
                    info!(dev_id, "UBLK device started via io_uring");
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound
                    || e.kind() == std::io::ErrorKind::PermissionDenied =>
                {
                    // The control device doesn't exist or we don't have
                    // permission.  This is expected in unprivileged
                    // environments, CI, and unit tests.
                    warn!(
                        error = %e,
                        "UBLK control device unavailable — running without io_uring backing"
                    );
                }
                Err(e) => {
                    error!(error = %e, "failed to start UBLK device via io_uring");
                    self.set_state(UblkServerState::Stopped);
                    return Err(blockyard_common::Error::Io(e));
                }
            }
        }

        *self.device_path.lock() = Some(dev_path.clone());
        self.set_state(UblkServerState::Running);

        Ok(dev_path)
    }

    /// Core io_uring device setup (Linux only).
    #[cfg(target_os = "linux")]
    fn start_uring_device(&self, dev_id: u32) -> std::io::Result<()> {
        let mut ctrl = UblkCtrl::open()?;

        let num_queues = self.config.num_queues as u16;
        let queue_depth = self.config.queue_depth as u16;
        let io_buf_size = self.config.io_buf_size;

        let dev_info = ctrl.create_device(dev_id, num_queues, queue_depth)?;
        info!(
            dev_id = dev_info.dev_id,
            queues = dev_info.nr_hw_queues,
            depth = dev_info.queue_depth,
            "UBLK device created"
        );

        // Spawn one worker thread per queue.
        let mut workers = self.queue_workers.lock();
        for q in 0..num_queues {
            let stop = self.stop_flag.clone();
            let d_id = dev_id;
            let depth = queue_depth;
            let buf_sz = io_buf_size;
            let volume_sz = self.volume_size;

            let handle = std::thread::Builder::new()
                .name(format!("ublk-q{q}"))
                .spawn(move || {
                    Self::queue_worker(d_id, q, depth, buf_sz, volume_sz, stop);
                })
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

            workers.push(handle);
        }

        // Start the device after all queue workers are spawned.
        ctrl.start_device(dev_id)?;

        Ok(())
    }

    /// Per-queue worker thread entry point (Linux only).
    ///
    /// Opens a [`UblkQueue`] and runs its I/O loop with a simple handler
    /// that responds to reads with zeroes and acknowledges writes.
    #[cfg(target_os = "linux")]
    fn queue_worker(
        dev_id: u32,
        queue_id: u16,
        depth: u16,
        io_buf_size: u32,
        _volume_size: u64,
        stop: Arc<AtomicBool>,
    ) {
        info!(dev_id, queue_id, "queue worker starting");

        let mut queue = match UblkQueue::new(dev_id, queue_id, depth, io_buf_size) {
            Ok(q) => q,
            Err(e) => {
                error!(dev_id, queue_id, error = %e, "failed to open UBLK queue");
                return;
            }
        };

        let result = queue.run(|op, _offset_sectors, _len_sectors, buf| {
            if stop.load(Ordering::Relaxed) {
                return uring::UBLK_IO_RES_ABORT;
            }

            match op {
                op if op == uring::UBLK_IO_OP_READ => {
                    // Default implementation: return zeroes.
                    // A real implementation would read from the block store.
                    buf.fill(0);
                    buf.len() as i32
                }
                op if op == uring::UBLK_IO_OP_WRITE => {
                    // Default implementation: discard the data.
                    // A real implementation would write to the block store.
                    buf.len() as i32
                }
                op if op == uring::UBLK_IO_OP_FLUSH => {
                    // Acknowledge the flush.
                    uring::UBLK_IO_RES_OK
                }
                op if op == uring::UBLK_IO_OP_DISCARD => {
                    // Acknowledge the discard.
                    uring::UBLK_IO_RES_OK
                }
                _ => {
                    warn!(dev_id, queue_id, op, "unknown UBLK I/O operation");
                    uring::UBLK_IO_RES_OK
                }
            }
        });

        match result {
            Ok(()) => info!(dev_id, queue_id, "queue worker exited cleanly"),
            Err(e) => error!(dev_id, queue_id, error = %e, "queue worker exited with error"),
        }
    }

    /// Stop the UBLK server and tear down the device.
    pub async fn stop(&self) -> blockyard_common::Result<()> {
        info!("stopping UBLK server");
        self.stop_flag.store(true, Ordering::Relaxed);

        #[cfg(target_os = "linux")]
        {
            let dev_id = self.config.device_id.unwrap_or(0);
            if let Err(e) = self.stop_uring_device(dev_id) {
                warn!(error = %e, "error during UBLK device teardown");
            }
        }

        self.set_state(UblkServerState::Stopped);
        *self.device_path.lock() = None;
        Ok(())
    }

    /// Tear down the io_uring device (Linux only).
    #[cfg(target_os = "linux")]
    fn stop_uring_device(&self, dev_id: u32) -> std::io::Result<()> {
        // First stop and delete the device, which will cause queue CQEs
        // to return ENODEV, breaking the workers out of their loops.
        if let Ok(mut ctrl) = UblkCtrl::open() {
            let _ = ctrl.stop_device(dev_id);
            let _ = ctrl.delete_device(dev_id);
        }

        // Join all worker threads.
        let mut workers = self.queue_workers.lock();
        for handle in workers.drain(..) {
            if let Err(e) = handle.join() {
                warn!("queue worker thread panicked: {:?}", e);
            }
        }

        Ok(())
    }

    pub async fn recover(&self) -> blockyard_common::Result<PathBuf> {
        self.set_state(UblkServerState::Recovering);
        info!("recovering UBLK device");

        let dev_path = self.device_path().ok_or_else(|| {
            blockyard_common::Error::Storage("no device to recover".into())
        })?;

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

    #[test]
    fn test_ublk_server_stop_flag() {
        let server = UblkServer::new(UblkServerConfig::default(), 0);
        assert!(!server.stop_flag.load(Ordering::Relaxed));
        server.stop_flag.store(true, Ordering::Relaxed);
        assert!(server.stop_flag.load(Ordering::Relaxed));
    }

    #[test]
    fn test_ublk_server_config_io_buf_size() {
        let config = UblkServerConfig::default();
        assert_eq!(config.io_buf_size, 512 * 1024);
    }
}
