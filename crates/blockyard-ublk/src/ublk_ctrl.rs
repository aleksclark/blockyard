//! UBLK control-plane implementation.
//!
//! When the `libublk` feature is enabled (Linux-only), this module delegates to
//! the `libublk` crate which correctly handles the io_uring SQE128 encoding for
//! UBLK commands.
//!
//! When the feature is disabled (or on non-Linux), a stub implementation is
//! provided that always returns an error, allowing the rest of the codebase to
//! compile and fall back to NBD.

use std::io;
#[cfg(not(feature = "libublk"))]
use tracing::info;
#[cfg(feature = "libublk")]
use tracing::{debug, info, warn};

use crate::uring::UblkDevConfig;

// ---------------------------------------------------------------------------
// libublk-backed implementation (Linux only, feature-gated)
// ---------------------------------------------------------------------------

/// Handle for managing a UBLK device through the control path.
///
/// On Linux with the `libublk` feature, this delegates to
/// `libublk::ctrl::UblkCtrl` for each operation.
/// Otherwise it is a stub that always returns errors.
pub struct UblkControl {
    _private: (),
}

impl UblkControl {
    /// Open the UBLK control device.
    ///
    /// With `libublk`, this is a lightweight operation — the actual device
    /// creation happens in [`add_device`].
    pub fn open() -> io::Result<Self> {
        #[cfg(feature = "libublk")]
        {
            info!("opened UBLK control device (libublk backend)");
        }

        #[cfg(not(feature = "libublk"))]
        {
            info!("UBLK control device not available (libublk feature disabled)");
        }

        Ok(Self { _private: () })
    }

    /// Add a new UBLK device.
    ///
    /// Returns a `UblkDeviceHandle` that can be used to start, stop, and
    /// delete the device.
    pub fn add_device(&self, config: &UblkDevConfig) -> io::Result<UblkDeviceHandle> {
        #[cfg(feature = "libublk")]
        {
            use libublk::UblkFlags;

            let dev_id = if config.dev_id == u32::MAX {
                -1i32
            } else {
                config.dev_id as i32
            };

            debug!(
                dev_id,
                nr_queues = config.nr_hw_queues,
                depth = config.queue_depth,
                io_buf = config.max_io_buf_bytes,
                "ADD_DEV via libublk"
            );

            let ctrl = libublk::ctrl::UblkCtrl::new(
                Some("blockyard".to_string()),
                dev_id,
                config.nr_hw_queues as u32,
                config.queue_depth as u32,
                config.max_io_buf_bytes,
                0, // flags (kernel ublk flags)
                0, // tgt_flags
                UblkFlags::UBLK_DEV_F_ADD_DEV,
            )
            .map_err(|e| io::Error::other(format!("libublk ADD_DEV: {e}")))?;

            let assigned_id = ctrl.dev_info().dev_id;
            info!(dev_id = assigned_id, "UBLK device added via libublk");

            Ok(UblkDeviceHandle {
                dev_id: assigned_id,
                dev_size: config.dev_size,
                nr_queues: config.nr_hw_queues as u32,
                queue_depth: config.queue_depth as u32,
                io_buf_bytes: config.max_io_buf_bytes,
                #[cfg(feature = "libublk")]
                ctrl: Some(ctrl),
            })
        }

        #[cfg(not(feature = "libublk"))]
        {
            let _ = config;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "UBLK not available: libublk feature not enabled",
            ))
        }
    }

    /// Start a UBLK device (transitions it to the live state).
    pub fn start_device(&self, dev_id: u32, _pid: i32) -> io::Result<()> {
        #[cfg(feature = "libublk")]
        {
            let _ = (dev_id, _pid);
            // Starting is handled through UblkDeviceHandle::start()
            warn!(
                "start_device called on UblkControl directly — use UblkDeviceHandle::start() instead"
            );
            Ok(())
        }

        #[cfg(not(feature = "libublk"))]
        {
            let _ = (dev_id, _pid);
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "UBLK not available: libublk feature not enabled",
            ))
        }
    }

    /// Stop a UBLK device.
    pub fn stop_device(&self, dev_id: u32) -> io::Result<()> {
        #[cfg(feature = "libublk")]
        {
            debug!(dev_id, "STOP_DEV via libublk");
            let ctrl = libublk::ctrl::UblkCtrl::new_simple(dev_id as i32)
                .map_err(|e| io::Error::other(format!("libublk open: {e}")))?;
            ctrl.stop_dev()
                .map_err(|e| io::Error::other(format!("libublk STOP_DEV: {e}")))?;
            info!(dev_id, "UBLK device stopped");
            Ok(())
        }

        #[cfg(not(feature = "libublk"))]
        {
            let _ = dev_id;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "UBLK not available: libublk feature not enabled",
            ))
        }
    }

    /// Delete a UBLK device entirely.
    pub fn delete_device(&self, dev_id: u32) -> io::Result<()> {
        #[cfg(feature = "libublk")]
        {
            debug!(dev_id, "DEL_DEV via libublk");
            let ctrl = libublk::ctrl::UblkCtrl::new_simple(dev_id as i32)
                .map_err(|e| io::Error::other(format!("libublk open: {e}")))?;
            ctrl.kill_dev()
                .map_err(|e| io::Error::other(format!("libublk DEL_DEV: {e}")))?;
            info!(dev_id, "UBLK device deleted");
            Ok(())
        }

        #[cfg(not(feature = "libublk"))]
        {
            let _ = dev_id;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "UBLK not available: libublk feature not enabled",
            ))
        }
    }

    /// Query device info for an existing UBLK device.
    pub fn get_device_info(&self, dev_id: u32) -> io::Result<crate::uring::UblkDevInfo> {
        #[cfg(feature = "libublk")]
        {
            debug!(dev_id, "GET_DEV_INFO via libublk");
            let ctrl = libublk::ctrl::UblkCtrl::new_simple(dev_id as i32)
                .map_err(|e| io::Error::other(format!("libublk open: {e}")))?;
            ctrl.read_dev_info()
                .map_err(|e| io::Error::other(format!("libublk GET_DEV_INFO: {e}")))?;

            let kinfo = ctrl.dev_info();
            let info = crate::uring::UblkDevInfo {
                nr_hw_queues: kinfo.nr_hw_queues,
                queue_depth: kinfo.queue_depth,
                state: kinfo.state,
                pad0: kinfo.pad0,
                max_io_buf_bytes: kinfo.max_io_buf_bytes,
                dev_id: kinfo.dev_id,
                ublksrv_pid: kinfo.ublksrv_pid,
                pad1: kinfo.pad1,
                flags: kinfo.flags,
                ublksrv_flags: kinfo.ublksrv_flags,
                // libublk's binding uses owner_uid/owner_gid where our
                // local struct has reserved0; map what we can.
                reserved0: 0,
                reserved1: kinfo.reserved1,
                reserved2: kinfo.reserved2,
            };
            Ok(info)
        }

        #[cfg(not(feature = "libublk"))]
        {
            let _ = dev_id;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "UBLK not available: libublk feature not enabled",
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// UblkDeviceHandle — tracks a device through its lifecycle
// ---------------------------------------------------------------------------

/// A handle to a UBLK device that has been added but may not yet be started.
///
/// This bundles the control device with the device configuration so that
/// `start()` can set up target parameters and begin I/O.
pub struct UblkDeviceHandle {
    /// The kernel-assigned device id.
    pub dev_id: u32,
    /// Configured device size in bytes.
    pub dev_size: u64,
    /// Number of hardware queues.
    pub nr_queues: u32,
    /// Per-queue depth.
    pub queue_depth: u32,
    /// Per-tag I/O buffer size.
    pub io_buf_bytes: u32,

    #[cfg(feature = "libublk")]
    ctrl: Option<libublk::ctrl::UblkCtrl>,
}

impl UblkDeviceHandle {
    /// Start the device, configuring target parameters and beginning I/O
    /// service.
    ///
    /// With `libublk`, this uses `UblkCtrl::run_target()` which handles:
    /// 1. Target initialization (setting device size, params)
    /// 2. Starting queue threads
    /// 3. Starting the device in the kernel
    ///
    /// **Note**: `run_target` is blocking and takes ownership, so the caller
    /// should run this on a blocking thread if needed.
    #[cfg(feature = "libublk")]
    pub fn take_ctrl(&mut self) -> Option<libublk::ctrl::UblkCtrl> {
        self.ctrl.take()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Public API tests (work with or without libublk) ---

    #[test]
    fn test_ublk_control_open() {
        // Should always succeed — it's a lightweight operation.
        let ctrl = UblkControl::open();
        assert!(ctrl.is_ok());
    }

    #[cfg(not(feature = "libublk"))]
    #[test]
    fn test_ublk_control_add_device_stub() {
        let ctrl = UblkControl::open().unwrap();
        let config = UblkDevConfig::default();
        let result = ctrl.add_device(&config);
        assert!(result.is_err());
    }

    #[cfg(not(feature = "libublk"))]
    #[test]
    fn test_ublk_control_stop_device_stub() {
        let ctrl = UblkControl::open().unwrap();
        assert!(ctrl.stop_device(0).is_err());
    }

    #[cfg(not(feature = "libublk"))]
    #[test]
    fn test_ublk_control_delete_device_stub() {
        let ctrl = UblkControl::open().unwrap();
        assert!(ctrl.delete_device(0).is_err());
    }

    #[cfg(not(feature = "libublk"))]
    #[test]
    fn test_ublk_control_get_device_info_stub() {
        let ctrl = UblkControl::open().unwrap();
        assert!(ctrl.get_device_info(0).is_err());
    }

    // --- UblkDeviceHandle tests ---

    #[test]
    fn test_device_handle_fields() {
        // Verify we can construct a handle (without libublk the ctrl field
        // doesn't exist so we just check the plain fields).
        let handle = UblkDeviceHandle {
            dev_id: 42,
            dev_size: 1024 * 1024,
            nr_queues: 2,
            queue_depth: 64,
            io_buf_bytes: 512 * 1024,
            #[cfg(feature = "libublk")]
            ctrl: None,
        };
        assert_eq!(handle.dev_id, 42);
        assert_eq!(handle.dev_size, 1024 * 1024);
        assert_eq!(handle.nr_queues, 2);
    }
}
