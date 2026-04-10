//! UBLK device driver integration (P4A.1).
//!
//! Wraps `libublk` to register a block device and dispatch kernel IO requests
//! to a [`BlockHandler`] trait implementor.
//!
//! # Feature-gated kernel integration
//!
//! When the `ublk-kernel` feature is enabled, [`UblkDevice::start_kernel`]
//! creates a real `/dev/ublkbN` device via the `libublk` crate. The ublk
//! IO loop runs on a dedicated OS thread (io_uring requires its own
//! executor), bridging into the tokio runtime via channels.
//!
//! Without the feature, the device operates in **mock mode** — no kernel
//! device is created and IO is submitted directly via [`UblkDevice::submit_io`].

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;

use blockyard_common::error::Error;

/// The type of IO operation from the kernel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IoOperation {
    Read,
    Write,
    Flush,
    Discard,
}

/// A single IO request from the kernel via ublk.
#[derive(Debug, Clone)]
pub struct IoRequest {
    pub operation: IoOperation,
    pub offset_bytes: u64,
    pub length_bytes: u32,
    pub data: Option<Bytes>,
    pub tag: u64,
}

impl IoRequest {
    pub fn block_range(&self, block_size: u64) -> Range<u64> {
        let start = self.offset_bytes / block_size;
        let end = (self.offset_bytes + self.length_bytes as u64).div_ceil(block_size);
        start..end
    }
}

/// Trait for handling block IO dispatched from the ublk device.
///
/// Implementors translate kernel block requests into Blockyard protocol
/// operations (write pipeline, read pipeline, etc.).
#[allow(async_fn_in_trait)]
pub trait BlockHandler: Send + Sync + 'static {
    async fn handle_io(&self, request: IoRequest) -> Result<Option<Bytes>, Error>;
}

/// Configuration for a ublk device.
#[derive(Debug, Clone)]
pub struct UblkDeviceConfig {
    pub device_size_bytes: u64,
    pub block_size: u32,
    pub queue_depth: u16,
    pub num_queues: u16,
}

impl Default for UblkDeviceConfig {
    fn default() -> Self {
        Self {
            device_size_bytes: 0,
            block_size: 4096,
            queue_depth: 128,
            num_queues: 1,
        }
    }
}

/// Tracks how the device was started.
#[derive(Debug, Clone, PartialEq, Eq)]
enum DeviceMode {
    /// Device not started yet.
    Idle,
    /// Mock mode — no kernel device, IO submitted via `submit_io`.
    Mock,
    /// Kernel mode — real `/dev/ublkbN` device via libublk.
    #[cfg(feature = "ublk-kernel")]
    Kernel { device_path: String, device_id: i32 },
}

/// A ublk block device that dispatches IO to a [`BlockHandler`].
///
/// In production this wraps the `libublk` crate. The struct is designed
/// to be testable without a running kernel ublk driver: the handler
/// trait can be mocked and IO can be injected directly via [`submit_io`].
pub struct UblkDevice<H: BlockHandler> {
    handler: Arc<H>,
    config: UblkDeviceConfig,
    running: std::sync::atomic::AtomicBool,
    mode: Mutex<DeviceMode>,
    #[cfg(feature = "ublk-kernel")]
    kernel_shutdown: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    #[cfg(feature = "ublk-kernel")]
    kernel_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl<H: BlockHandler> UblkDevice<H> {
    pub fn new(handler: H, config: UblkDeviceConfig) -> Self {
        Self {
            handler: Arc::new(handler),
            config,
            running: std::sync::atomic::AtomicBool::new(false),
            mode: Mutex::new(DeviceMode::Idle),
            #[cfg(feature = "ublk-kernel")]
            kernel_shutdown: Mutex::new(None),
            #[cfg(feature = "ublk-kernel")]
            kernel_thread: Mutex::new(None),
        }
    }

    pub fn config(&self) -> &UblkDeviceConfig {
        &self.config
    }

    pub fn handler(&self) -> &H {
        &self.handler
    }

    pub fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Returns the device path (e.g. `/dev/ublkb0`) if running in kernel mode.
    pub fn device_path(&self) -> Option<String> {
        let mode = self.mode.lock();
        match &*mode {
            #[cfg(feature = "ublk-kernel")]
            DeviceMode::Kernel { device_path, .. } => Some(device_path.clone()),
            _ => None,
        }
    }

    /// Start the device in mock mode. IO is submitted via [`submit_io`].
    /// This is the default for testing and when `ublk-kernel` is not enabled.
    pub async fn start(&self) -> Result<(), Error> {
        if self.config.device_size_bytes == 0 {
            return Err(Error::Config("device size must be > 0".into()));
        }
        *self.mode.lock() = DeviceMode::Mock;
        self.running
            .store(true, std::sync::atomic::Ordering::Release);
        tracing::info!(
            size = self.config.device_size_bytes,
            block_size = self.config.block_size,
            "ublk device started (mock mode)"
        );
        Ok(())
    }

    /// Start the device with a real kernel ublk device via libublk.
    ///
    /// Requires the `ublk-kernel` feature, Linux kernel 6.0+ with `ublk_drv`
    /// module loaded, and appropriate capabilities (root or CAP_SYS_ADMIN).
    ///
    /// The IO loop runs on a dedicated OS thread; IO commands are bridged
    /// into the tokio runtime via channels.
    #[cfg(feature = "ublk-kernel")]
    pub async fn start_kernel(&self) -> Result<String, Error> {
        use libublk::ctrl::{UblkCtrl, UblkCtrlBuilder};
        use libublk::io::{UblkDev, UblkIOCtx, UblkQueue};
        use libublk::{UblkError as LibUblkError, UblkFlags, UblkIORes};
        use std::rc::Rc;

        if self.config.device_size_bytes == 0 {
            return Err(Error::Config("device size must be > 0".into()));
        }

        let nr_queues = self.config.num_queues as u32;
        let queue_depth = self.config.queue_depth as u32;
        let dev_size = self.config.device_size_bytes;
        let block_size = self.config.block_size;
        let handler = Arc::clone(&self.handler);

        let (path_tx, path_rx) = tokio::sync::oneshot::channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let tokio_handle = tokio::runtime::Handle::current();

        let thread = std::thread::spawn(move || {
            let path_tx = std::sync::Mutex::new(Some(path_tx));
            let path_tx_arc = std::sync::Arc::new(path_tx);
            let path_tx_err = std::sync::Arc::clone(&path_tx_arc);

            let result = (|| -> Result<(), Error> {
                let ctrl = UblkCtrlBuilder::default()
                    .name("blockyard")
                    .nr_queues(nr_queues as u16)
                    .depth(queue_depth as u16)
                    .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV)
                    .build()
                    .map_err(|e| Error::Storage(format!("failed to create ublk ctrl: {e}")))?;

                let dev_id = ctrl.dev_info().dev_id as i32;
                let device_path = format!("/dev/ublkb{}", dev_id);

                let tgt_init = |dev: &mut UblkDev| {
                    dev.set_default_params(dev_size);
                    Ok(())
                };

                let handler_clone = handler;
                let tokio_handle_clone = tokio_handle.clone();

                let q_handler = move |qid: u16, dev: &UblkDev| {
                    let bufs = Rc::new(dev.alloc_queue_io_bufs());
                    let handler = handler_clone.clone();
                    let tokio_handle = tokio_handle_clone.clone();

                    let io_handler = {
                        let bufs = bufs.clone();
                        move |q: &UblkQueue, tag: u16, _io: &UblkIOCtx| {
                            let iod = q.get_iod(tag);
                            let op = iod.op_flags & 0xFF;
                            let offset = iod.start_sector * 512;
                            let nr_sectors = iod.nr_sectors;
                            let length = nr_sectors as u32 * 512;

                            let io_op = match op {
                                0 => IoOperation::Read,    // REQ_OP_READ
                                1 => IoOperation::Write,   // REQ_OP_WRITE
                                2 => IoOperation::Flush,   // REQ_OP_FLUSH
                                3 => IoOperation::Discard, // REQ_OP_DISCARD
                                _ => {
                                    let buf_addr = bufs[tag as usize].as_mut_ptr();
                                    q.complete_io_cmd(
                                        tag,
                                        buf_addr,
                                        Err(LibUblkError::OtherError(-95)),
                                    );
                                    return;
                                }
                            };

                            let data = if io_op == IoOperation::Write {
                                let buf = &bufs[tag as usize];
                                Some(Bytes::copy_from_slice(buf.as_slice()))
                            } else {
                                None
                            };

                            let request = IoRequest {
                                operation: io_op.clone(),
                                offset_bytes: offset,
                                length_bytes: length,
                                data,
                                tag: tag as u64,
                            };

                            let h = handler.clone();
                            let result =
                                tokio_handle.block_on(async move { h.handle_io(request).await });

                            match result {
                                Ok(read_data) => {
                                    if io_op == IoOperation::Read {
                                        if let Some(data) = read_data {
                                            let buf = &bufs[tag as usize];
                                            let len =
                                                std::cmp::min(data.len(), buf.as_slice().len());
                                            unsafe {
                                                std::ptr::copy_nonoverlapping(
                                                    data.as_ptr(),
                                                    buf.as_mut_ptr(),
                                                    len,
                                                );
                                            }
                                        }
                                    }
                                    let buf_addr = bufs[tag as usize].as_mut_ptr();
                                    q.complete_io_cmd(tag, buf_addr, Ok(UblkIORes::Result(length as i32)));
                                }
                                Err(_e) => {
                                    let buf_addr = bufs[tag as usize].as_mut_ptr();
                                    q.complete_io_cmd(tag, buf_addr, Err(LibUblkError::OtherError(-5)));
                                }
                            }
                        }
                    };

                    let queue = UblkQueue::new(qid as u16, dev)
                        .unwrap()
                        .submit_fetch_commands(Some(&bufs));
                    queue.wait_and_handle_io(io_handler);
                };

                let device_path_clone = device_path.clone();
                let path_tx_ready = std::sync::Arc::clone(&path_tx_arc);

                let dev_ready = move |ctrl_ref: &UblkCtrl| {
                    if let Some(tx) = path_tx_ready.lock().unwrap().take() {
                        let _ = tx.send(device_path_clone.clone());
                    }
                    ctrl_ref.dump();
                };

                ctrl.run_target(tgt_init, q_handler, dev_ready)
                    .map_err(|e| Error::Storage(format!("ublk run_target failed: {e}")))?;

                Ok(())
            })();

            if let Err(e) = result {
                tracing::error!(error = %e, "ublk kernel thread failed");
                if let Some(tx) = path_tx_err.lock().unwrap().take() {
                    let _ = tx.send(String::new());
                }
            }
        });

        let device_path = path_rx
            .await
            .map_err(|_| Error::Storage("ublk device creation failed".into()))?;

        if device_path.is_empty() {
            return Err(Error::Storage("ublk device creation failed".into()));
        }

        let dev_id: i32 = device_path
            .strip_prefix("/dev/ublkb")
            .and_then(|s| s.parse().ok())
            .unwrap_or(-1);

        *self.mode.lock() = DeviceMode::Kernel {
            device_path: device_path.clone(),
            device_id: dev_id,
        };
        *self.kernel_shutdown.lock() = Some(shutdown_tx);
        *self.kernel_thread.lock() = Some(thread);

        self.running
            .store(true, std::sync::atomic::Ordering::Release);

        tracing::info!(
            device_path = %device_path,
            size = self.config.device_size_bytes,
            block_size = self.config.block_size,
            "ublk device started (kernel mode)"
        );

        Ok(device_path)
    }

    /// Stop the device.
    pub async fn stop(&self) -> Result<(), Error> {
        let mode = self.mode.lock().clone();

        #[cfg(feature = "ublk-kernel")]
        if let DeviceMode::Kernel { device_id, .. } = mode {
            if let Some(tx) = self.kernel_shutdown.lock().take() {
                let _ = tx.send(());
            }

            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                use libublk::ctrl::UblkCtrl;
                if let Ok(ctrl) = UblkCtrl::new_simple(device_id) {
                    let _ = ctrl.kill_dev();
                    let _ = ctrl.del_dev();
                }
            }));
            if let Err(e) = result {
                tracing::warn!("ublk device cleanup panicked: {:?}", e);
            }

            if let Some(thread) = self.kernel_thread.lock().take() {
                let _ = thread.join();
            }
        }

        self.running
            .store(false, std::sync::atomic::Ordering::Release);
        *self.mode.lock() = DeviceMode::Idle;
        tracing::info!("ublk device stopped");
        Ok(())
    }

    /// Submit an IO request directly (used for testing and by the ublk event loop).
    pub async fn submit_io(&self, request: IoRequest) -> Result<Option<Bytes>, Error> {
        if !self.is_running() {
            return Err(Error::Storage("device not running".into()));
        }
        self.handler.handle_io(request).await
    }
}

impl<H: BlockHandler> std::fmt::Debug for UblkDevice<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UblkDevice")
            .field("config", &self.config)
            .field("running", &self.is_running())
            .field("mode", &*self.mode.lock())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EchoHandler;

    impl BlockHandler for EchoHandler {
        async fn handle_io(&self, request: IoRequest) -> Result<Option<Bytes>, Error> {
            match request.operation {
                IoOperation::Write => Ok(None),
                IoOperation::Read => {
                    let data = vec![0xAB; request.length_bytes as usize];
                    Ok(Some(Bytes::from(data)))
                }
                IoOperation::Flush => Ok(None),
                IoOperation::Discard => Ok(None),
            }
        }
    }

    struct FailHandler;

    impl BlockHandler for FailHandler {
        async fn handle_io(&self, _request: IoRequest) -> Result<Option<Bytes>, Error> {
            Err(Error::Storage("injected failure".into()))
        }
    }

    #[tokio::test]
    async fn test_ublk_device_start_stop() {
        let dev = UblkDevice::new(
            EchoHandler,
            UblkDeviceConfig {
                device_size_bytes: 1024 * 1024,
                ..Default::default()
            },
        );
        assert!(!dev.is_running());
        dev.start().await.unwrap();
        assert!(dev.is_running());
        dev.stop().await.unwrap();
        assert!(!dev.is_running());
    }

    #[tokio::test]
    async fn test_ublk_device_start_zero_size() {
        let dev = UblkDevice::new(EchoHandler, UblkDeviceConfig::default());
        let result = dev.start().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("device size"));
    }

    #[tokio::test]
    async fn test_ublk_device_submit_read() {
        let dev = UblkDevice::new(
            EchoHandler,
            UblkDeviceConfig {
                device_size_bytes: 1024 * 1024,
                ..Default::default()
            },
        );
        dev.start().await.unwrap();

        let req = IoRequest {
            operation: IoOperation::Read,
            offset_bytes: 0,
            length_bytes: 512,
            data: None,
            tag: 1,
        };
        let result = dev.submit_io(req).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 512);
    }

    #[tokio::test]
    async fn test_ublk_device_submit_write() {
        let dev = UblkDevice::new(
            EchoHandler,
            UblkDeviceConfig {
                device_size_bytes: 1024 * 1024,
                ..Default::default()
            },
        );
        dev.start().await.unwrap();

        let req = IoRequest {
            operation: IoOperation::Write,
            offset_bytes: 0,
            length_bytes: 512,
            data: Some(Bytes::from(vec![0u8; 512])),
            tag: 2,
        };
        let result = dev.submit_io(req).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_ublk_device_submit_flush() {
        let dev = UblkDevice::new(
            EchoHandler,
            UblkDeviceConfig {
                device_size_bytes: 1024 * 1024,
                ..Default::default()
            },
        );
        dev.start().await.unwrap();

        let req = IoRequest {
            operation: IoOperation::Flush,
            offset_bytes: 0,
            length_bytes: 0,
            data: None,
            tag: 3,
        };
        let result = dev.submit_io(req).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_ublk_device_submit_discard() {
        let dev = UblkDevice::new(
            EchoHandler,
            UblkDeviceConfig {
                device_size_bytes: 1024 * 1024,
                ..Default::default()
            },
        );
        dev.start().await.unwrap();

        let req = IoRequest {
            operation: IoOperation::Discard,
            offset_bytes: 4096,
            length_bytes: 4096,
            data: None,
            tag: 4,
        };
        let result = dev.submit_io(req).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_ublk_device_submit_not_running() {
        let dev = UblkDevice::new(
            EchoHandler,
            UblkDeviceConfig {
                device_size_bytes: 1024 * 1024,
                ..Default::default()
            },
        );
        let req = IoRequest {
            operation: IoOperation::Read,
            offset_bytes: 0,
            length_bytes: 512,
            data: None,
            tag: 1,
        };
        let result = dev.submit_io(req).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not running"));
    }

    #[tokio::test]
    async fn test_ublk_device_handler_error() {
        let dev = UblkDevice::new(
            FailHandler,
            UblkDeviceConfig {
                device_size_bytes: 1024 * 1024,
                ..Default::default()
            },
        );
        dev.start().await.unwrap();

        let req = IoRequest {
            operation: IoOperation::Read,
            offset_bytes: 0,
            length_bytes: 512,
            data: None,
            tag: 1,
        };
        let result = dev.submit_io(req).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("injected failure"));
    }

    #[test]
    fn test_io_request_block_range() {
        let req = IoRequest {
            operation: IoOperation::Read,
            offset_bytes: 4096,
            length_bytes: 8192,
            data: None,
            tag: 0,
        };
        let range = req.block_range(4096);
        assert_eq!(range, 1..3);
    }

    #[test]
    fn test_io_request_block_range_unaligned() {
        let req = IoRequest {
            operation: IoOperation::Read,
            offset_bytes: 100,
            length_bytes: 4096,
            data: None,
            tag: 0,
        };
        let range = req.block_range(4096);
        assert_eq!(range, 0..2);
    }

    #[test]
    fn test_io_request_block_range_zero_offset() {
        let req = IoRequest {
            operation: IoOperation::Read,
            offset_bytes: 0,
            length_bytes: 4096,
            data: None,
            tag: 0,
        };
        let range = req.block_range(4096);
        assert_eq!(range, 0..1);
    }

    #[test]
    fn test_ublk_device_config_default() {
        let config = UblkDeviceConfig::default();
        assert_eq!(config.device_size_bytes, 0);
        assert_eq!(config.block_size, 4096);
        assert_eq!(config.queue_depth, 128);
        assert_eq!(config.num_queues, 1);
    }

    #[test]
    fn test_ublk_device_config_debug() {
        let config = UblkDeviceConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("UblkDeviceConfig"));
    }

    #[test]
    fn test_ublk_device_debug() {
        let dev = UblkDevice::new(
            EchoHandler,
            UblkDeviceConfig {
                device_size_bytes: 1024,
                ..Default::default()
            },
        );
        let debug = format!("{:?}", dev);
        assert!(debug.contains("UblkDevice"));
        assert!(debug.contains("running"));
    }

    #[test]
    fn test_io_operation_eq() {
        assert_eq!(IoOperation::Read, IoOperation::Read);
        assert_ne!(IoOperation::Read, IoOperation::Write);
        assert_ne!(IoOperation::Flush, IoOperation::Discard);
    }

    #[test]
    fn test_io_operation_debug() {
        let debug = format!("{:?}", IoOperation::Read);
        assert_eq!(debug, "Read");
    }

    #[test]
    fn test_io_operation_clone() {
        let op = IoOperation::Write;
        let cloned = op.clone();
        assert_eq!(op, cloned);
    }

    #[test]
    fn test_io_request_clone() {
        let req = IoRequest {
            operation: IoOperation::Read,
            offset_bytes: 0,
            length_bytes: 512,
            data: Some(Bytes::from(vec![1, 2, 3])),
            tag: 42,
        };
        let cloned = req.clone();
        assert_eq!(cloned.tag, 42);
        assert_eq!(cloned.length_bytes, 512);
    }

    #[tokio::test]
    async fn test_ublk_device_config_accessor() {
        let config = UblkDeviceConfig {
            device_size_bytes: 2048,
            block_size: 512,
            queue_depth: 64,
            num_queues: 2,
        };
        let dev = UblkDevice::new(EchoHandler, config);
        assert_eq!(dev.config().device_size_bytes, 2048);
        assert_eq!(dev.config().block_size, 512);
        assert_eq!(dev.config().queue_depth, 64);
        assert_eq!(dev.config().num_queues, 2);
    }

    #[tokio::test]
    async fn test_ublk_device_handler_accessor() {
        let dev = UblkDevice::new(
            EchoHandler,
            UblkDeviceConfig {
                device_size_bytes: 1024,
                ..Default::default()
            },
        );
        let _handler = dev.handler();
    }

    #[tokio::test]
    async fn test_ublk_device_restart() {
        let dev = UblkDevice::new(
            EchoHandler,
            UblkDeviceConfig {
                device_size_bytes: 1024 * 1024,
                ..Default::default()
            },
        );
        dev.start().await.unwrap();
        assert!(dev.is_running());
        dev.stop().await.unwrap();
        assert!(!dev.is_running());
        dev.start().await.unwrap();
        assert!(dev.is_running());
    }

    #[tokio::test]
    async fn test_device_path_none_in_mock_mode() {
        let dev = UblkDevice::new(
            EchoHandler,
            UblkDeviceConfig {
                device_size_bytes: 1024 * 1024,
                ..Default::default()
            },
        );
        assert!(dev.device_path().is_none());
        dev.start().await.unwrap();
        assert!(dev.device_path().is_none());
    }

    #[tokio::test]
    async fn test_device_path_none_before_start() {
        let dev = UblkDevice::new(
            EchoHandler,
            UblkDeviceConfig {
                device_size_bytes: 1024,
                ..Default::default()
            },
        );
        assert!(dev.device_path().is_none());
    }

    #[cfg(feature = "ublk-kernel")]
    #[tokio::test]
    async fn test_start_kernel_zero_size() {
        let dev = UblkDevice::new(EchoHandler, UblkDeviceConfig::default());
        let result = dev.start_kernel().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("device size"));
    }
}
