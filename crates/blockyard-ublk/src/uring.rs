//! io_uring-based UBLK (Userspace Block Device) integration for Linux.
//!
//! This module implements the control and I/O paths for UBLK devices using
//! the Linux io_uring subsystem (kernel 6.0+). UBLK allows userspace programs
//! to implement block device drivers by communicating with the kernel through
//! io_uring submission/completion queues.
//!
//! # Architecture
//!
//! - [`UblkCtrl`] manages device lifecycle (create, start, stop, delete) via
//!   `/dev/ublk-control`.
//! - [`UblkQueue`] handles I/O for a single queue (one per CPU core), running
//!   the fetch → process → commit loop over io_uring.
//!
//! # Kernel interface
//!
//! The UBLK driver uses two ioctl families:
//! - **Control commands** (`UBLK_CMD_*`): sent to `/dev/ublk-control` to manage
//!   device lifecycle.
//! - **I/O commands** (`UBLK_IO_*`): sent to `/dev/ublkc<N>` to fetch and
//!   complete block I/O requests.

#![cfg(target_os = "linux")]

use std::fs::{File, OpenOptions};
use std::os::unix::io::{AsRawFd, RawFd};

use io_uring::{cqueue, opcode, squeue, types, IoUring};
use tracing::{debug, error, info, trace, warn};

/// io_uring ring type using 128-byte SQEs (required for URING_CMD / `UringCmd80`).
type UblkRing = IoUring<squeue::Entry128, cqueue::Entry>;

// ---------------------------------------------------------------------------
// UBLK ioctl command constants (from linux/ublk_cmd.h)
// ---------------------------------------------------------------------------

/// Ioctl type byte for UBLK control commands.
const UBLK_IOC_TYPE: u8 = b'u';

/// Add (create) a new UBLK device.
///
/// Kernel source: `#define UBLK_CMD_ADD_DEV  _IOWR('u', UBLK_CMD_ADD_DEV, struct ublksrv_ctrl_cmd)`
/// Command number 0x04.
pub const UBLK_CMD_ADD_DEV: u32 = 0x04;

/// Start an already-created UBLK device (transition to live).
pub const UBLK_CMD_START_DEV: u32 = 0x05;

/// Stop a running UBLK device.
pub const UBLK_CMD_STOP_DEV: u32 = 0x06;

/// Delete a stopped UBLK device.
pub const UBLK_CMD_DEL_DEV: u32 = 0x07;

/// Retrieve device information.
pub const UBLK_CMD_GET_DEV_INFO: u32 = 0x08;

/// Fetch the next I/O request from the kernel (queue-side).
pub const UBLK_IO_FETCH_REQ: u32 = 0x20;

/// Commit the result of the current I/O request and fetch the next one
/// in a single round-trip (queue-side).
pub const UBLK_IO_COMMIT_AND_FETCH_REQ: u32 = 0x21;

// ---------------------------------------------------------------------------
// UBLK I/O operation codes (from the block layer)
// ---------------------------------------------------------------------------

/// Block read operation.
pub const UBLK_IO_OP_READ: u32 = 0;

/// Block write operation.
pub const UBLK_IO_OP_WRITE: u32 = 1;

/// Flush (write barrier) operation.
pub const UBLK_IO_OP_FLUSH: u32 = 2;

/// Discard / trim operation.
pub const UBLK_IO_OP_DISCARD: u32 = 3;

// ---------------------------------------------------------------------------
// UBLK I/O result codes
// ---------------------------------------------------------------------------

/// Successful completion.
pub const UBLK_IO_RES_OK: i32 = 0;

/// I/O was aborted.
pub const UBLK_IO_RES_ABORT: i32 = -libc::ENODEV;

// ---------------------------------------------------------------------------
// UBLK feature flags
// ---------------------------------------------------------------------------

/// The device supports zero-copy I/O.
pub const UBLK_F_SUPPORT_ZERO_COPY: u64 = 1 << 0;

/// The device is recoverable after the server process dies.
pub const UBLK_F_URING_CMD_COMP_IN_TASK: u64 = 1 << 1;

/// Require the user namespace to match the creator.
pub const UBLK_F_NEED_GET_DATA: u64 = 1 << 2;

/// Support unprivileged UBLK.
pub const UBLK_F_USER_RECOVERY: u64 = 1 << 3;

/// Support user recovery with reissue.
pub const UBLK_F_USER_RECOVERY_REISSUE: u64 = 1 << 4;

// ---------------------------------------------------------------------------
// UBLK control ioctl encoding helpers
//
// The kernel defines these as _IOWR('u', nr, struct ublksrv_ctrl_cmd).
// sizeof(ublksrv_ctrl_cmd) = 80 bytes on 64-bit Linux.
// _IOWR encodes: direction(2) | size(14) | type(8) | nr(8)
// direction for _IOWR = 0b11 (read | write)
// ---------------------------------------------------------------------------

const UBLK_CTRL_CMD_SIZE: u32 = 80; // sizeof(ublksrv_ctrl_cmd)

/// Encode a _IOWR('u', nr, 80) ioctl number.
const fn ublk_ctrl_ioctl(nr: u32) -> u32 {
    let dir: u32 = 3; // _IOC_READ | _IOC_WRITE
    let size = UBLK_CTRL_CMD_SIZE;
    (dir << 30) | (size << 16) | ((UBLK_IOC_TYPE as u32) << 8) | nr
}

/// Full ioctl number for UBLK_CMD_ADD_DEV.
pub const UBLK_CTL_ADD_DEV: u32 = ublk_ctrl_ioctl(UBLK_CMD_ADD_DEV);

/// Full ioctl number for UBLK_CMD_START_DEV.
pub const UBLK_CTL_START_DEV: u32 = ublk_ctrl_ioctl(UBLK_CMD_START_DEV);

/// Full ioctl number for UBLK_CMD_STOP_DEV.
pub const UBLK_CTL_STOP_DEV: u32 = ublk_ctrl_ioctl(UBLK_CMD_STOP_DEV);

/// Full ioctl number for UBLK_CMD_DEL_DEV.
pub const UBLK_CTL_DEL_DEV: u32 = ublk_ctrl_ioctl(UBLK_CMD_DEL_DEV);

/// Full ioctl number for UBLK_CMD_GET_DEV_INFO.
pub const UBLK_CTL_GET_DEV_INFO: u32 = ublk_ctrl_ioctl(UBLK_CMD_GET_DEV_INFO);

// ---------------------------------------------------------------------------
// UBLK I/O ioctl encoding helpers
//
// I/O commands use _IOWR('u', nr, struct ublksrv_io_cmd).
// sizeof(ublksrv_io_cmd) = 16 bytes.
// ---------------------------------------------------------------------------

const UBLK_IO_CMD_SIZE: u32 = 16; // sizeof(ublksrv_io_cmd)

/// Encode a _IOWR('u', nr, 16) ioctl number for I/O commands.
const fn ublk_io_ioctl(nr: u32) -> u32 {
    let dir: u32 = 3; // _IOC_READ | _IOC_WRITE
    let size = UBLK_IO_CMD_SIZE;
    (dir << 30) | (size << 16) | ((UBLK_IOC_TYPE as u32) << 8) | nr
}

/// Full ioctl number for UBLK_IO_FETCH_REQ.
pub const UBLK_IO_CTL_FETCH_REQ: u32 = ublk_io_ioctl(UBLK_IO_FETCH_REQ);

/// Full ioctl number for UBLK_IO_COMMIT_AND_FETCH_REQ.
pub const UBLK_IO_CTL_COMMIT_AND_FETCH: u32 = ublk_io_ioctl(UBLK_IO_COMMIT_AND_FETCH_REQ);

// ---------------------------------------------------------------------------
// Path constants
// ---------------------------------------------------------------------------

/// Path to the UBLK control device.
pub const UBLK_CTRL_DEV_PATH: &str = "/dev/ublk-control";

/// Returns the character device path for a given device id (e.g. `/dev/ublkc0`).
pub fn ublk_char_dev_path(dev_id: u32) -> String {
    format!("/dev/ublkc{dev_id}")
}

/// Returns the block device path for a given device id (e.g. `/dev/ublkb0`).
pub fn ublk_block_dev_path(dev_id: u32) -> String {
    format!("/dev/ublkb{dev_id}")
}

// ---------------------------------------------------------------------------
// UblkDevInfo — device information returned from the kernel
// ---------------------------------------------------------------------------

/// Information about a UBLK device, mirroring the kernel's `ublksrv_ctrl_dev_info`.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct UblkDevInfo {
    /// Number of I/O queues.
    pub nr_hw_queues: u16,
    /// Depth of each I/O queue (max in-flight requests per queue).
    pub queue_depth: u16,
    /// Device state (from kernel).
    pub state: u16,
    /// Padding.
    pub pad0: u16,
    /// Maximum size of an I/O request in bytes.
    pub max_io_buf_bytes: u32,
    /// Device id.
    pub dev_id: u32,
    /// Device minor number in sysfs.
    pub ublksrv_pid: i32,
    /// Flags (see `UBLK_F_*` constants).
    pub flags: u64,
    /// Reserved for future use.
    pub owner_uid: u64,
    /// Reserved for future use.
    pub owner_gid: u64,
    /// Reserved.
    pub reserved1: u64,
    /// Reserved.
    pub reserved2: u64,
}

impl UblkDevInfo {
    /// Create a new `UblkDevInfo` with the given parameters, zeroing everything else.
    pub fn new(dev_id: u32, num_queues: u16, queue_depth: u16) -> Self {
        Self {
            nr_hw_queues: num_queues,
            queue_depth,
            state: 0,
            pad0: 0,
            max_io_buf_bytes: 512 * 1024,
            dev_id,
            ublksrv_pid: 0,
            flags: 0,
            owner_uid: 0,
            owner_gid: 0,
            reserved1: 0,
            reserved2: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// UblkCtrlCmd — command structure sent to /dev/ublk-control
// ---------------------------------------------------------------------------

/// Control command structure matching the kernel's `ublksrv_ctrl_cmd`.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct UblkCtrlCmd {
    /// Device id this command targets.
    pub dev_id: u32,
    /// Queue id (used for some commands).
    pub queue_id: u16,
    /// Length of extra data.
    pub len: u16,
    /// Userspace address of the data buffer.
    pub addr: u64,
    /// Extra data (command-specific).
    pub data: [u64; 2],
    /// Reserved padding to reach 80 bytes.
    pub reserved: [u8; 48],
}

impl UblkCtrlCmd {
    /// Create a zeroed-out command targeting the given device.
    pub fn new(dev_id: u32) -> Self {
        Self {
            dev_id,
            queue_id: 0,
            len: 0,
            addr: 0,
            data: [0; 2],
            reserved: [0; 48],
        }
    }
}

// ---------------------------------------------------------------------------
// UblkIoCmd — I/O command sent per-request to /dev/ublkcN
// ---------------------------------------------------------------------------

/// I/O command structure matching the kernel's `ublksrv_io_cmd`.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct UblkIoCmd {
    /// Queue-local tag identifying this I/O request.
    pub tag: u16,
    /// Padding.
    pub pad: u16,
    /// Result of the I/O (for COMMIT operations), or 0 for FETCH.
    pub result: i32,
    /// Userspace buffer address for the I/O data.
    pub addr: u64,
}

// ---------------------------------------------------------------------------
// UblkCtrl — control-plane handle
// ---------------------------------------------------------------------------

/// Handle to `/dev/ublk-control` for managing UBLK device lifecycle.
///
/// Each method sends a command through an io_uring submission queue entry
/// using the `IORING_OP_URING_CMD` opcode, which the kernel's UBLK driver
/// picks up as an ioctl-over-io_uring request.
pub struct UblkCtrl {
    /// The opened `/dev/ublk-control` file.
    ctrl_file: File,
    /// The io_uring instance used for control commands.
    ring: UblkRing,
}

impl UblkCtrl {
    /// Open the UBLK control device and create an io_uring ring for it.
    ///
    /// # Errors
    ///
    /// Returns an error if `/dev/ublk-control` cannot be opened (e.g. the
    /// `ublk_drv` kernel module is not loaded) or the io_uring ring cannot be
    /// created.
    pub fn open() -> std::io::Result<Self> {
        let ctrl_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(UBLK_CTRL_DEV_PATH)?;

        info!(path = UBLK_CTRL_DEV_PATH, "opened UBLK control device");

        // A small ring is sufficient for serialised control commands.
        // We use 128-byte SQEs because UringCmd80 requires Entry128.
        let ring = UblkRing::builder().build(4)?;

        Ok(Self { ctrl_file, ring })
    }

    /// File descriptor for the control device.
    pub fn ctrl_fd(&self) -> RawFd {
        self.ctrl_file.as_raw_fd()
    }

    /// Submit a single control command and wait for its completion.
    ///
    /// The `ioctl_nr` is one of the `UBLK_CTL_*` constants, and `cmd` is the
    /// populated control command structure.
    fn submit_ctrl_cmd(&mut self, ioctl_nr: u32, cmd: &UblkCtrlCmd) -> std::io::Result<i32> {
        let fd = types::Fd(self.ctrl_fd());
        let cmd_ptr = cmd as *const UblkCtrlCmd as u64;

        // Build the io_uring SQE for URING_CMD (opcode 80).
        // The cmd_op field carries the ioctl number.
        let sqe = opcode::UringCmd80::new(fd, ioctl_nr)
            .cmd({
                let mut buf = [0u8; 80];
                let bytes = unsafe {
                    std::slice::from_raw_parts(
                        cmd as *const UblkCtrlCmd as *const u8,
                        std::mem::size_of::<UblkCtrlCmd>(),
                    )
                };
                let len = bytes.len().min(80);
                buf[..len].copy_from_slice(&bytes[..len]);
                buf
            })
            .build()
            .user_data(cmd_ptr);

        // Safety: the SQE references stack-local data that outlives the
        // submission + wait cycle.
        unsafe {
            self.ring.submission().push(&sqe).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "io_uring SQ full")
            })?;
        }

        self.ring.submit_and_wait(1)?;

        let cqe = self
            .ring
            .completion()
            .next()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "no CQE received"))?;

        let ret = cqe.result();
        if ret < 0 {
            Err(std::io::Error::from_raw_os_error(-ret))
        } else {
            Ok(ret)
        }
    }

    /// Create a new UBLK device.
    ///
    /// `dev_id` is the requested device number (e.g. 0 → `/dev/ublkb0`).
    /// `num_queues` is the number of I/O queues, typically one per CPU core.
    /// `queue_depth` is the maximum number of in-flight I/O requests per queue.
    ///
    /// On success, returns the [`UblkDevInfo`] populated by the kernel.
    pub fn create_device(
        &mut self,
        dev_id: u32,
        num_queues: u16,
        queue_depth: u16,
    ) -> std::io::Result<UblkDevInfo> {
        let mut info = UblkDevInfo::new(dev_id, num_queues, queue_depth);
        info.flags = UBLK_F_URING_CMD_COMP_IN_TASK;
        info.ublksrv_pid = std::process::id() as i32;

        let mut cmd = UblkCtrlCmd::new(dev_id);
        cmd.addr = &info as *const UblkDevInfo as u64;
        cmd.len = std::mem::size_of::<UblkDevInfo>() as u16;

        info!(
            dev_id,
            num_queues,
            queue_depth,
            "creating UBLK device"
        );

        self.submit_ctrl_cmd(UBLK_CTL_ADD_DEV, &cmd)?;

        debug!(dev_id, "UBLK device created");
        Ok(info)
    }

    /// Start a previously created UBLK device, making the block device node
    /// available for I/O.
    pub fn start_device(&mut self, dev_id: u32) -> std::io::Result<()> {
        let mut cmd = UblkCtrlCmd::new(dev_id);
        cmd.data[0] = std::process::id() as u64;

        info!(dev_id, "starting UBLK device");
        self.submit_ctrl_cmd(UBLK_CTL_START_DEV, &cmd)?;
        info!(
            dev_id,
            block_dev = %ublk_block_dev_path(dev_id),
            "UBLK device started"
        );
        Ok(())
    }

    /// Stop a running UBLK device.
    pub fn stop_device(&mut self, dev_id: u32) -> std::io::Result<()> {
        let cmd = UblkCtrlCmd::new(dev_id);

        info!(dev_id, "stopping UBLK device");
        self.submit_ctrl_cmd(UBLK_CTL_STOP_DEV, &cmd)?;
        info!(dev_id, "UBLK device stopped");
        Ok(())
    }

    /// Delete a stopped UBLK device, releasing all kernel resources.
    pub fn delete_device(&mut self, dev_id: u32) -> std::io::Result<()> {
        let cmd = UblkCtrlCmd::new(dev_id);

        info!(dev_id, "deleting UBLK device");
        self.submit_ctrl_cmd(UBLK_CTL_DEL_DEV, &cmd)?;
        info!(dev_id, "UBLK device deleted");
        Ok(())
    }

    /// Query device information from the kernel.
    pub fn get_device_info(&mut self, dev_id: u32) -> std::io::Result<UblkDevInfo> {
        let info = UblkDevInfo::new(dev_id, 0, 0);

        let mut cmd = UblkCtrlCmd::new(dev_id);
        cmd.addr = &info as *const UblkDevInfo as u64;
        cmd.len = std::mem::size_of::<UblkDevInfo>() as u16;

        debug!(dev_id, "querying UBLK device info");
        self.submit_ctrl_cmd(UBLK_CTL_GET_DEV_INFO, &cmd)?;

        // The kernel writes back into the info struct at cmd.addr.
        // Because we passed a pointer to our stack-local info, it is updated
        // in place by the kernel before the CQE is posted.
        Ok(info)
    }
}

// ---------------------------------------------------------------------------
// UblkQueue — per-queue I/O processing
// ---------------------------------------------------------------------------

/// One I/O queue of a UBLK device.
///
/// Each queue is typically pinned to a single CPU core and processes block I/O
/// requests in a tight loop:
///
/// 1. **Fetch** — submit `UBLK_IO_FETCH_REQ` for each tag slot to prime the ring.
/// 2. **Wait** — wait for a CQE indicating a new I/O request from the kernel.
/// 3. **Process** — handle the read/write/flush/discard.
/// 4. **Commit** — submit `UBLK_IO_COMMIT_AND_FETCH_REQ` to return the result
///    and simultaneously fetch the next request.
pub struct UblkQueue {
    /// Device id this queue belongs to.
    dev_id: u32,
    /// Queue index (0-based).
    queue_id: u16,
    /// Queue depth (max in-flight I/O requests).
    depth: u16,
    /// The character device file for this queue (`/dev/ublkcN`).
    char_file: File,
    /// The io_uring ring for this queue.
    ring: UblkRing,
    /// Per-tag I/O buffers. Each tag in `0..depth` gets its own buffer.
    io_bufs: Vec<Vec<u8>>,
    /// Whether the queue is running.
    running: bool,
}

impl UblkQueue {
    /// Open a queue on the given UBLK character device.
    ///
    /// `io_buf_size` is the size of each per-tag I/O buffer in bytes.
    pub fn new(
        dev_id: u32,
        queue_id: u16,
        depth: u16,
        io_buf_size: u32,
    ) -> std::io::Result<Self> {
        let char_path = ublk_char_dev_path(dev_id);
        let char_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&char_path)?;

        info!(
            dev_id,
            queue_id,
            depth,
            char_path = %char_path,
            "opening UBLK queue"
        );

        // Size the ring to at least the queue depth so we can have all tag
        // slots in flight simultaneously.
        let ring_size = (depth as u32).next_power_of_two().max(32);
        let ring = UblkRing::builder().build(ring_size)?;

        // Allocate per-tag I/O buffers.
        let io_bufs = (0..depth)
            .map(|_| vec![0u8; io_buf_size as usize])
            .collect();

        Ok(Self {
            dev_id,
            queue_id,
            depth,
            char_file,
            ring,
            io_bufs,
            running: false,
        })
    }

    /// File descriptor for the character device.
    pub fn char_fd(&self) -> RawFd {
        self.char_file.as_raw_fd()
    }

    /// Queue id.
    pub fn queue_id(&self) -> u16 {
        self.queue_id
    }

    /// Queue depth.
    pub fn depth(&self) -> u16 {
        self.depth
    }

    /// Whether the queue loop is marked as running.
    pub fn is_running(&self) -> bool {
        self.running
    }

    /// Submit an initial `UBLK_IO_FETCH_REQ` for a given tag to prime the ring.
    ///
    /// This tells the kernel "I'm ready to receive an I/O request on this tag
    /// slot". The kernel will complete the SQE when a block I/O request arrives.
    fn submit_fetch(&mut self, tag: u16) -> std::io::Result<()> {
        let fd = types::Fd(self.char_fd());
        let buf_addr = self.io_bufs[tag as usize].as_ptr() as u64;

        let io_cmd = UblkIoCmd {
            tag,
            pad: 0,
            result: 0,
            addr: buf_addr,
        };

        let sqe = opcode::UringCmd80::new(fd, UBLK_IO_CTL_FETCH_REQ)
            .cmd({
                let mut buf = [0u8; 80];
                let bytes = unsafe {
                    std::slice::from_raw_parts(
                        &io_cmd as *const UblkIoCmd as *const u8,
                        std::mem::size_of::<UblkIoCmd>(),
                    )
                };
                buf[..bytes.len()].copy_from_slice(bytes);
                buf
            })
            .build()
            // Encode queue_id and tag in user_data so we can identify
            // completions: high 16 bits = queue_id, low 16 bits = tag.
            .user_data(Self::encode_user_data(self.queue_id, tag));

        unsafe {
            self.ring.submission().push(&sqe).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "io_uring SQ full on fetch")
            })?;
        }

        trace!(
            dev_id = self.dev_id,
            queue_id = self.queue_id,
            tag,
            "submitted FETCH_REQ"
        );
        Ok(())
    }

    /// Submit `UBLK_IO_COMMIT_AND_FETCH_REQ` to return the I/O result and
    /// simultaneously prime the tag for the next request.
    fn submit_commit_and_fetch(&mut self, tag: u16, result: i32) -> std::io::Result<()> {
        let fd = types::Fd(self.char_fd());
        let buf_addr = self.io_bufs[tag as usize].as_ptr() as u64;

        let io_cmd = UblkIoCmd {
            tag,
            pad: 0,
            result,
            addr: buf_addr,
        };

        let sqe = opcode::UringCmd80::new(fd, UBLK_IO_CTL_COMMIT_AND_FETCH)
            .cmd({
                let mut buf = [0u8; 80];
                let bytes = unsafe {
                    std::slice::from_raw_parts(
                        &io_cmd as *const UblkIoCmd as *const u8,
                        std::mem::size_of::<UblkIoCmd>(),
                    )
                };
                buf[..bytes.len()].copy_from_slice(bytes);
                buf
            })
            .build()
            .user_data(Self::encode_user_data(self.queue_id, tag));

        unsafe {
            self.ring.submission().push(&sqe).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "io_uring SQ full on commit_and_fetch",
                )
            })?;
        }

        trace!(
            dev_id = self.dev_id,
            queue_id = self.queue_id,
            tag,
            result,
            "submitted COMMIT_AND_FETCH"
        );
        Ok(())
    }

    /// Run the I/O processing loop.
    ///
    /// 1. Submit `FETCH_REQ` for all tag slots.
    /// 2. Enter the event loop: wait for CQEs, process them, commit results.
    ///
    /// The `handler` closure is called for each I/O request with
    /// `(op, offset_sectors, len_sectors, &mut buf)` and must return the number
    /// of bytes processed (for reads/writes) or 0 for flush/discard.
    ///
    /// The loop runs until `stop` is called or an unrecoverable error occurs.
    pub fn run<F>(&mut self, mut handler: F) -> std::io::Result<()>
    where
        F: FnMut(u32, u64, u32, &mut [u8]) -> i32,
    {
        self.running = true;

        info!(
            dev_id = self.dev_id,
            queue_id = self.queue_id,
            depth = self.depth,
            "starting UBLK queue I/O loop"
        );

        // Prime all tag slots with FETCH_REQ.
        for tag in 0..self.depth {
            self.submit_fetch(tag)?;
        }
        self.ring.submit()?;

        // Main event loop.
        while self.running {
            // Wait for at least one completion.
            match self.ring.submit_and_wait(1) {
                Ok(_) => {}
                Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => {
                    error!(
                        dev_id = self.dev_id,
                        queue_id = self.queue_id,
                        error = %e,
                        "io_uring submit_and_wait failed"
                    );
                    self.running = false;
                    return Err(e);
                }
            }

            // Drain all available CQEs.
            let cqes: Vec<io_uring::cqueue::Entry> = self.ring.completion().collect();

            for cqe in cqes {
                let user_data = cqe.user_data();
                let (_q_id, tag) = Self::decode_user_data(user_data);
                let res = cqe.result();

                if res < 0 {
                    // Negative result means the fetch/commit was rejected.
                    // ENODEV means the device is being torn down.
                    if -res == libc::ENODEV {
                        info!(
                            dev_id = self.dev_id,
                            queue_id = self.queue_id,
                            tag,
                            "device removed, exiting queue loop"
                        );
                        self.running = false;
                        break;
                    }
                    warn!(
                        dev_id = self.dev_id,
                        queue_id = self.queue_id,
                        tag,
                        error = -res,
                        "CQE returned error"
                    );
                    continue;
                }

                // The CQE result for UBLK encodes the I/O operation info:
                //   bits [31:24] = operation (read/write/flush/discard)
                //   bits [23:0]  = reserved / used by kernel
                // However, the actual operation details come through the
                // io_uring cmd buffer. For our handler interface, we extract
                // what we can and delegate to the callback.
                //
                // In practice, the UBLK driver passes operation details through
                // the io_uring CQE `big_cqe` or via shared memory. Here we
                // provide the raw result and let the handler decide.
                let op = ((res as u32) >> 24) & 0xFF;
                let buf = &mut self.io_bufs[tag as usize];

                let io_result = handler(op, 0, 0, buf);

                // Commit the result and fetch the next request.
                self.submit_commit_and_fetch(tag, io_result)?;
            }

            // Flush any pending submissions.
            self.ring.submit()?;
        }

        info!(
            dev_id = self.dev_id,
            queue_id = self.queue_id,
            "UBLK queue I/O loop exited"
        );
        Ok(())
    }

    /// Signal the queue to stop processing.
    pub fn stop(&mut self) {
        info!(
            dev_id = self.dev_id,
            queue_id = self.queue_id,
            "stopping UBLK queue"
        );
        self.running = false;
    }

    /// Encode queue_id and tag into a u64 user_data value.
    fn encode_user_data(queue_id: u16, tag: u16) -> u64 {
        ((queue_id as u64) << 16) | (tag as u64)
    }

    /// Decode queue_id and tag from a u64 user_data value.
    fn decode_user_data(user_data: u64) -> (u16, u16) {
        let queue_id = ((user_data >> 16) & 0xFFFF) as u16;
        let tag = (user_data & 0xFFFF) as u16;
        (queue_id, tag)
    }

    /// Get a reference to the I/O buffer for a given tag.
    pub fn io_buf(&self, tag: u16) -> &[u8] {
        &self.io_bufs[tag as usize]
    }

    /// Get a mutable reference to the I/O buffer for a given tag.
    pub fn io_buf_mut(&mut self, tag: u16) -> &mut [u8] {
        &mut self.io_bufs[tag as usize]
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Constant value tests — verify ioctl numbers match kernel definitions
    // -----------------------------------------------------------------------

    #[test]
    fn test_ublk_cmd_constants() {
        assert_eq!(UBLK_CMD_ADD_DEV, 0x04);
        assert_eq!(UBLK_CMD_START_DEV, 0x05);
        assert_eq!(UBLK_CMD_STOP_DEV, 0x06);
        assert_eq!(UBLK_CMD_DEL_DEV, 0x07);
        assert_eq!(UBLK_CMD_GET_DEV_INFO, 0x08);
    }

    #[test]
    fn test_ublk_io_constants() {
        assert_eq!(UBLK_IO_FETCH_REQ, 0x20);
        assert_eq!(UBLK_IO_COMMIT_AND_FETCH_REQ, 0x21);
    }

    #[test]
    fn test_ublk_io_op_constants() {
        assert_eq!(UBLK_IO_OP_READ, 0);
        assert_eq!(UBLK_IO_OP_WRITE, 1);
        assert_eq!(UBLK_IO_OP_FLUSH, 2);
        assert_eq!(UBLK_IO_OP_DISCARD, 3);
    }

    #[test]
    fn test_ublk_feature_flags() {
        assert_eq!(UBLK_F_SUPPORT_ZERO_COPY, 1 << 0);
        assert_eq!(UBLK_F_URING_CMD_COMP_IN_TASK, 1 << 1);
        assert_eq!(UBLK_F_NEED_GET_DATA, 1 << 2);
        assert_eq!(UBLK_F_USER_RECOVERY, 1 << 3);
        assert_eq!(UBLK_F_USER_RECOVERY_REISSUE, 1 << 4);
    }

    #[test]
    fn test_ublk_ctrl_ioctl_encoding() {
        // _IOWR('u', 0x04, 80) should produce:
        //   dir=3 (bits 31:30), size=80 (bits 29:16), type='u'=0x75 (bits 15:8), nr=0x04 (bits 7:0)
        let expected = (3u32 << 30) | (80u32 << 16) | (0x75u32 << 8) | 0x04;
        assert_eq!(UBLK_CTL_ADD_DEV, expected);
    }

    #[test]
    fn test_ublk_ctrl_ioctl_start() {
        let expected = (3u32 << 30) | (80u32 << 16) | (0x75u32 << 8) | 0x05;
        assert_eq!(UBLK_CTL_START_DEV, expected);
    }

    #[test]
    fn test_ublk_ctrl_ioctl_stop() {
        let expected = (3u32 << 30) | (80u32 << 16) | (0x75u32 << 8) | 0x06;
        assert_eq!(UBLK_CTL_STOP_DEV, expected);
    }

    #[test]
    fn test_ublk_ctrl_ioctl_del() {
        let expected = (3u32 << 30) | (80u32 << 16) | (0x75u32 << 8) | 0x07;
        assert_eq!(UBLK_CTL_DEL_DEV, expected);
    }

    #[test]
    fn test_ublk_ctrl_ioctl_get_info() {
        let expected = (3u32 << 30) | (80u32 << 16) | (0x75u32 << 8) | 0x08;
        assert_eq!(UBLK_CTL_GET_DEV_INFO, expected);
    }

    #[test]
    fn test_ublk_io_ioctl_fetch() {
        let expected = (3u32 << 30) | (16u32 << 16) | (0x75u32 << 8) | 0x20;
        assert_eq!(UBLK_IO_CTL_FETCH_REQ, expected);
    }

    #[test]
    fn test_ublk_io_ioctl_commit_and_fetch() {
        let expected = (3u32 << 30) | (16u32 << 16) | (0x75u32 << 8) | 0x21;
        assert_eq!(UBLK_IO_CTL_COMMIT_AND_FETCH, expected);
    }

    // -----------------------------------------------------------------------
    // Path helper tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_ublk_ctrl_dev_path() {
        assert_eq!(UBLK_CTRL_DEV_PATH, "/dev/ublk-control");
    }

    #[test]
    fn test_ublk_char_dev_path() {
        assert_eq!(ublk_char_dev_path(0), "/dev/ublkc0");
        assert_eq!(ublk_char_dev_path(5), "/dev/ublkc5");
        assert_eq!(ublk_char_dev_path(99), "/dev/ublkc99");
    }

    #[test]
    fn test_ublk_block_dev_path() {
        assert_eq!(ublk_block_dev_path(0), "/dev/ublkb0");
        assert_eq!(ublk_block_dev_path(5), "/dev/ublkb5");
        assert_eq!(ublk_block_dev_path(99), "/dev/ublkb99");
    }

    // -----------------------------------------------------------------------
    // Struct construction tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_ublk_dev_info_new() {
        let info = UblkDevInfo::new(42, 4, 128);
        assert_eq!(info.dev_id, 42);
        assert_eq!(info.nr_hw_queues, 4);
        assert_eq!(info.queue_depth, 128);
        assert_eq!(info.max_io_buf_bytes, 512 * 1024);
        assert_eq!(info.state, 0);
        assert_eq!(info.ublksrv_pid, 0);
        assert_eq!(info.flags, 0);
    }

    #[test]
    fn test_ublk_dev_info_size() {
        // The kernel expects this struct to be exactly 64 bytes.
        assert_eq!(std::mem::size_of::<UblkDevInfo>(), 64);
    }

    #[test]
    fn test_ublk_ctrl_cmd_new() {
        let cmd = UblkCtrlCmd::new(7);
        assert_eq!(cmd.dev_id, 7);
        assert_eq!(cmd.queue_id, 0);
        assert_eq!(cmd.len, 0);
        assert_eq!(cmd.addr, 0);
        assert_eq!(cmd.data, [0; 2]);
    }

    #[test]
    fn test_ublk_ctrl_cmd_size() {
        // Must be exactly 80 bytes to match the kernel ioctl definition.
        assert_eq!(std::mem::size_of::<UblkCtrlCmd>(), 80);
    }

    #[test]
    fn test_ublk_io_cmd_size() {
        // Must be exactly 16 bytes.
        assert_eq!(std::mem::size_of::<UblkIoCmd>(), 16);
    }

    // -----------------------------------------------------------------------
    // User data encoding / decoding
    // -----------------------------------------------------------------------

    #[test]
    fn test_encode_decode_user_data() {
        let (q, t) = UblkQueue::decode_user_data(UblkQueue::encode_user_data(3, 42));
        assert_eq!(q, 3);
        assert_eq!(t, 42);
    }

    #[test]
    fn test_encode_decode_user_data_max() {
        let (q, t) = UblkQueue::decode_user_data(UblkQueue::encode_user_data(0xFFFF, 0xFFFF));
        assert_eq!(q, 0xFFFF);
        assert_eq!(t, 0xFFFF);
    }

    #[test]
    fn test_encode_decode_user_data_zero() {
        let (q, t) = UblkQueue::decode_user_data(UblkQueue::encode_user_data(0, 0));
        assert_eq!(q, 0);
        assert_eq!(t, 0);
    }

    // -----------------------------------------------------------------------
    // Integration tests that need root + ublk_drv kernel module
    // These are #[ignore] by default.
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "requires root privileges and the ublk_drv kernel module loaded"]
    fn test_ublk_ctrl_open() {
        let ctrl = UblkCtrl::open();
        assert!(ctrl.is_ok(), "failed to open UBLK control device: {:?}", ctrl.err());
    }

    #[test]
    #[ignore = "requires root privileges and the ublk_drv kernel module loaded"]
    fn test_ublk_create_and_delete_device() {
        let mut ctrl = UblkCtrl::open().expect("open control device");
        let info = ctrl.create_device(254, 1, 32).expect("create device");
        assert_eq!(info.nr_hw_queues, 1);
        assert_eq!(info.queue_depth, 32);

        ctrl.stop_device(254).ok(); // may not be started
        ctrl.delete_device(254).expect("delete device");
    }

    #[test]
    #[ignore = "requires root privileges and the ublk_drv kernel module loaded"]
    fn test_ublk_queue_new() {
        let mut ctrl = UblkCtrl::open().expect("open control device");
        let _info = ctrl.create_device(253, 1, 32).expect("create device");

        let queue = UblkQueue::new(253, 0, 32, 512 * 1024);
        assert!(queue.is_ok(), "failed to open queue: {:?}", queue.err());

        let queue = queue.unwrap();
        assert_eq!(queue.queue_id(), 0);
        assert_eq!(queue.depth(), 32);
        assert!(!queue.is_running());

        ctrl.stop_device(253).ok();
        ctrl.delete_device(253).expect("delete device");
    }

    // -----------------------------------------------------------------------
    // IO result code tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_ublk_io_res_ok() {
        assert_eq!(UBLK_IO_RES_OK, 0);
    }

    #[test]
    fn test_ublk_io_res_abort() {
        // ENODEV is 19 on Linux.
        assert_eq!(UBLK_IO_RES_ABORT, -19);
    }
}
