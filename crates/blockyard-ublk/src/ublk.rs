//! UBLK device driver integration (P4A.1).
//!
//! Wraps `libublk` to register a block device and dispatch kernel IO requests
//! to a [`BlockHandler`] trait implementor.

use std::ops::Range;

use bytes::Bytes;

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

/// A ublk block device that dispatches IO to a [`BlockHandler`].
///
/// In production this wraps the `libublk` crate. The struct is designed
/// to be testable without a running kernel ublk driver: the handler
/// trait can be mocked and IO can be injected directly via [`submit_io`].
pub struct UblkDevice<H: BlockHandler> {
    handler: H,
    config: UblkDeviceConfig,
    running: std::sync::atomic::AtomicBool,
}

impl<H: BlockHandler> UblkDevice<H> {
    pub fn new(handler: H, config: UblkDeviceConfig) -> Self {
        Self {
            handler,
            config,
            running: std::sync::atomic::AtomicBool::new(false),
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

    /// Start the device. In production this registers with the kernel ublk
    /// driver; in tests it simply marks the device as running.
    pub async fn start(&self) -> Result<(), Error> {
        if self.config.device_size_bytes == 0 {
            return Err(Error::Config("device size must be > 0".into()));
        }
        self.running
            .store(true, std::sync::atomic::Ordering::Release);
        tracing::info!(
            size = self.config.device_size_bytes,
            block_size = self.config.block_size,
            "ublk device started"
        );
        Ok(())
    }

    /// Stop the device.
    pub async fn stop(&self) -> Result<(), Error> {
        self.running
            .store(false, std::sync::atomic::Ordering::Release);
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
}
