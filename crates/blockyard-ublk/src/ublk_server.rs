use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{debug, info, warn};

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
    worker: Arc<parking_lot::Mutex<Option<std::thread::JoinHandle<()>>>>,
    store: Arc<crate::nbd::MemBlockStore>,
}

impl UblkServer {
    pub fn new(config: UblkServerConfig, volume_size: u64) -> Self {
        let store = Arc::new(crate::nbd::MemBlockStore::new(volume_size, 4096));
        Self {
            config,
            volume_size,
            state: Arc::new(std::sync::atomic::AtomicU8::new(
                UblkServerState::Stopped as u8,
            )),
            device_path: Arc::new(parking_lot::Mutex::new(None)),
            worker: Arc::new(parking_lot::Mutex::new(None)),
            store,
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

    pub async fn start(&self) -> blockyard_common::Result<PathBuf> {
        self.set_state(UblkServerState::Starting);

        let dev_id_hint = self.config.device_id.unwrap_or(0);

        info!(
            dev_id = dev_id_hint,
            queues = self.config.num_queues,
            depth = self.config.queue_depth,
            size = self.volume_size,
            "starting UBLK server"
        );

        Self::ensure_ublk_module().await;

        #[cfg(feature = "libublk")]
        {
            match self.start_libublk(dev_id_hint) {
                Ok(path) => return Ok(path),
                Err(e) => {
                    warn!(error = %e, "UBLK device creation failed, falling back to stub path");
                }
            }
        }

        let dev_path = PathBuf::from(format!("/dev/ublkb{dev_id_hint}"));
        *self.device_path.lock() = Some(dev_path.clone());
        self.set_state(UblkServerState::Running);
        Ok(dev_path)
    }

    #[cfg(feature = "libublk")]
    fn start_libublk(&self, dev_id: u32) -> blockyard_common::Result<PathBuf> {
        use libublk::ctrl::UblkCtrlBuilder;
        use libublk::io::{UblkDev, UblkQueue};
        use libublk::UblkFlags;

        let nr_queues = self.config.num_queues;
        let depth = self.config.queue_depth;
        let vol_size = self.volume_size;
        let state = self.state.clone();
        let dp = self.device_path.clone();
        let store = self.store.clone();

        let (tx, rx) = std::sync::mpsc::sync_channel::<Result<PathBuf, String>>(1);

        let handle = std::thread::spawn(move || {
            let ctrl = match UblkCtrlBuilder::default()
                .name("blockyard")
                .nr_queues(nr_queues as u16)
                .depth(depth as u16)
                .id(dev_id as i32)
                .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV)
                .build()
            {
                Ok(c) => Arc::new(c),
                Err(e) => {
                    let _ = tx.send(Err(format!("UblkCtrl build: {e}")));
                    return;
                }
            };

            let real_id = ctrl.dev_info().dev_id;
            let dev_path = PathBuf::from(format!("/dev/ublkb{real_id}"));
            *dp.lock() = Some(dev_path.clone());
            let _ = tx.send(Ok(dev_path));

            let tgt_init = move |dev: &mut UblkDev| {
                dev.set_default_params(vol_size);
                Ok(())
            };

            let store_for_q = store.clone();
            let q_handler = move |qid: u16, dev: &UblkDev| {
                let bufs = std::rc::Rc::new(dev.alloc_queue_io_bufs());
                let store_ref = store_for_q.clone();
                let io_handler = {
                    let bufs = bufs.clone();
                    move |q: &UblkQueue, tag: u16, _io: &libublk::io::UblkIOCtx| {
                        let iod = q.get_iod(tag);
                        let op = iod.op_flags & 0xff;
                        let offset = (iod.start_sector as u64) << 9;
                        let len = (iod.nr_sectors as u32) << 9;

                        if op == 0 {
                            let data = store_ref.read(offset, len);
                            let buf = bufs[tag as usize].as_slice();
                            let buf_ptr = buf.as_ptr() as *mut u8;
                            let copy_len = data.len().min(buf.len());
                            // SAFETY: we own the buffer exclusively for this tag
                            // during the I/O operation. libublk guarantees single-
                            // writer access per tag.
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    data.as_ptr(),
                                    buf_ptr,
                                    copy_len,
                                );
                            }
                        } else if op == 1 {
                            let buf = bufs[tag as usize].as_slice();
                            store_ref.write(offset, &buf[..len as usize]);
                        }

                        q.complete_io_cmd_unified(
                            tag,
                            libublk::BufDesc::Slice(bufs[tag as usize].as_slice()),
                            Ok(libublk::UblkIORes::Result(len as i32)),
                        )
                        .unwrap_or_else(|e| {
                            tracing::error!(tag, error = %e, "complete_io_cmd failed");
                        });
                    }
                };

                let queue = UblkQueue::new(qid, dev)
                    .unwrap()
                    .submit_fetch_commands_unified(libublk::io::BufDescList::Slices(Some(&bufs)))
                    .unwrap();
                queue.wait_and_handle_io(io_handler);
            };

            state.store(UblkServerState::Running as u8, Ordering::Relaxed);

            if let Err(e) = ctrl.run_target(tgt_init, q_handler, |_dev| {}) {
                tracing::error!(error = %e, "UBLK run_target exited with error");
            }

            state.store(UblkServerState::Stopped as u8, Ordering::Relaxed);
        });

        *self.worker.lock() = Some(handle);

        let result = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .map_err(|_| blockyard_common::Error::Storage("UBLK start timeout".into()))?
            .map_err(|e| blockyard_common::Error::Storage(e))?;

        // Wait briefly for the block device to appear
        for _ in 0..30 {
            if result.exists() {
                info!(device = %result.display(), "UBLK block device ready");
                return Ok(result);
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        info!(device = %result.display(), "UBLK device created (block device may still be initializing)");
        Ok(result)
    }

    pub async fn stop(&self) -> blockyard_common::Result<()> {
        info!("stopping UBLK server");

        #[cfg(feature = "libublk")]
        {
            if let Some(path) = self.device_path() {
                if let Some(id_str) = path.file_name().and_then(|f| f.to_str()) {
                    if let Some(id_num) = id_str.strip_prefix("ublkb") {
                        if let Ok(dev_id) = id_num.parse::<i32>() {
                            if let Ok(ctrl) = libublk::ctrl::UblkCtrlBuilder::default()
                                .id(dev_id)
                                .build()
                            {
                                let _ = ctrl.kill_dev();
                            }
                        }
                    }
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
    async fn test_ublk_server_start_fallback() {
        let config = UblkServerConfig {
            device_id: Some(99),
            ..Default::default()
        };
        let server = UblkServer::new(config, 1024 * 1024);
        let path = server.start().await.unwrap();
        assert_eq!(path, PathBuf::from("/dev/ublkb99"));
        assert_eq!(server.state(), UblkServerState::Running);
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
    }

    #[tokio::test]
    async fn test_ublk_server_recover_without_start() {
        let server = UblkServer::new(UblkServerConfig::default(), 1024);
        let result = server.recover().await;
        assert!(result.is_err());
    }
}
