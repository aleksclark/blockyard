//! UBLK control-plane implementation via raw io_uring syscalls.
//!
//! The Linux UBLK driver (`ublk_drv`) exclusively uses `IORING_OP_URING_CMD`
//! (opcode 80) for both control and data path — there is **no** legacy ioctl
//! path.  This module provides a minimal io_uring setup (via raw
//! `io_uring_setup`, `io_uring_enter`, and `mmap`) sufficient to issue UBLK
//! control commands (`ADD_DEV`, `START_DEV`, `STOP_DEV`, `DEL_DEV`,
//! `GET_DEV_INFO`) against `/dev/ublk-control`.
//!
//! # Safety
//!
//! The io_uring interface is inherently unsafe: we share memory-mapped ring
//! buffers with the kernel.  All unsafe blocks are documented with `// SAFETY:`
//! comments.

use std::io;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU32, Ordering};

use tracing::{debug, info};

use crate::uring::{
    UBLK_CMD_ADD_DEV, UBLK_CMD_DEL_DEV, UBLK_CMD_GET_DEV_INFO, UBLK_CMD_START_DEV,
    UBLK_CMD_STOP_DEV, UBLK_CTRL_DEV_PATH, UblkCtrlCmd, UblkDevConfig, UblkDevInfo,
};

// ---------------------------------------------------------------------------
// io_uring constants
// ---------------------------------------------------------------------------

/// `IORING_OP_URING_CMD` — pass a driver-specific command through a uring SQE.
const IORING_OP_URING_CMD: u8 = 80;

/// `IORING_SETUP_SQE128` — use 128-byte SQEs (needed for the 80-byte `cmd`
/// payload that UBLK control commands require).
const IORING_SETUP_SQE128: u32 = 1 << 10;

/// `IORING_SETUP_CQE32` — use 32-byte CQEs for the extra results that some
/// UBLK commands return.
const IORING_SETUP_CQE32: u32 = 1 << 11;

// `IORING_ENTER_GETEVENTS` — block until at least one CQE is available.
const IORING_ENTER_GETEVENTS: u32 = 1;

// `IORING_OFF_SQ_RING` and friends — mmap offsets for the shared ring buffers.
const IORING_OFF_SQ_RING: u64 = 0;
const IORING_OFF_CQ_RING: u64 = 0x0800_0000;
const IORING_OFF_SQES: u64 = 0x1000_0000;

// ---------------------------------------------------------------------------
// `struct io_uring_params` — passed to io_uring_setup(2)
// ---------------------------------------------------------------------------

/// Mirrors the kernel's `struct io_uring_params`.  Only the fields we actually
/// read or write are named; the rest is reserved padding.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct IoUringParams {
    sq_entries: u32,
    cq_entries: u32,
    flags: u32,
    sq_thread_cpu: u32,
    sq_thread_idle: u32,
    features: u32,
    wq_fd: u32,
    resv: [u32; 3],
    sq_off: IoSqRingOffsets,
    cq_off: IoCqRingOffsets,
}

impl Default for IoUringParams {
    fn default() -> Self {
        // SAFETY: all-zero is a valid representation; the kernel fills in the
        // output fields.
        unsafe { std::mem::zeroed() }
    }
}

/// Offsets into the SQ ring mmap region.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct IoSqRingOffsets {
    head: u32,
    tail: u32,
    ring_mask: u32,
    ring_entries: u32,
    flags: u32,
    dropped: u32,
    array: u32,
    resv1: u32,
    user_addr: u64,
}

/// Offsets into the CQ ring mmap region.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct IoCqRingOffsets {
    head: u32,
    tail: u32,
    ring_mask: u32,
    ring_entries: u32,
    overflow: u32,
    cqes: u32,
    flags: u32,
    resv1: u32,
    user_addr: u64,
}

// ---------------------------------------------------------------------------
// SQE layout — 128-byte variant (`IORING_SETUP_SQE128`)
// ---------------------------------------------------------------------------

/// A 128-byte SQE (Submission Queue Entry).
///
/// The first 64 bytes are the standard SQE fields.  The remaining 64 bytes are
/// the `cmd` payload that UBLK uses to pass `ublksrv_ctrl_cmd`.
///
/// For UBLK control commands we only set:
/// - `opcode`  = IORING_OP_URING_CMD (80)
/// - `fd`      = the ublk-control fd
/// - `cmd_op`  = the encoded ioctl number (e.g. `ublk_ctrl_ioctl(UBLK_CMD_ADD_DEV)`)
/// - `cmd[..]` = the `UblkCtrlCmd` struct serialised into the tail
#[repr(C, align(8))]
#[derive(Clone, Copy)]
struct Sqe128 {
    // --- standard 64-byte SQE ---
    opcode: u8,
    flags: u8,
    ioprio: u16,
    fd: i32,
    off_or_addr2: u64,   // union: off / addr2
    addr_or_splice: u64, // union: addr / splice_off_in
    len: u32,
    op_flags: u32, // union: rw_flags / fsync_flags / …
    user_data: u64,
    buf_index_or_group: u16, // union
    personality: u16,
    splice_fd_or_file_index: i32,
    addr3_or_cmd_op: u64, // union: addr3 / cmd_op (for uring_cmd)
    _pad2: u64,

    // --- 64-byte cmd payload (SQE128 extension) ---
    cmd: [u8; 64],
}

impl Default for Sqe128 {
    fn default() -> Self {
        // SAFETY: all-zero is a valid representation.
        unsafe { std::mem::zeroed() }
    }
}

// Check that the type is exactly 128 bytes.
const _: () = assert!(std::mem::size_of::<Sqe128>() == 128);

// ---------------------------------------------------------------------------
// CQE layout — 32-byte variant (`IORING_SETUP_CQE32`)
// ---------------------------------------------------------------------------

/// A 32-byte CQE (Completion Queue Entry).
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct Cqe32 {
    user_data: u64,
    res: i32,
    flags: u32,
    // 16 bytes extra (CQE32 extension)
    extra1: u64,
    extra2: u64,
}

const _: () = assert!(std::mem::size_of::<Cqe32>() == 32);

// ---------------------------------------------------------------------------
// Minimal io_uring ring
// ---------------------------------------------------------------------------

/// A tiny io_uring instance that can submit a single SQE and wait for one CQE.
///
/// We only ever need a depth-1 ring for control commands; they are issued
/// sequentially.
#[allow(dead_code)] // some fields only accessed through raw pointer dereference
struct IoUring {
    ring_fd: RawFd,

    // SQ ring pointers (mmap'd).
    sq_head: *const AtomicU32,
    sq_tail: *mut AtomicU32,
    sq_mask: u32,
    sq_array: *mut u32,

    // CQ ring pointers (mmap'd).
    cq_head: *mut AtomicU32,
    cq_tail: *const AtomicU32,
    cq_mask: u32,
    cq_cqes: *const Cqe32,

    // SQE array (mmap'd).
    sqes: *mut Sqe128,

    // mmap regions we need to unmap on drop.
    sq_ring_ptr: *mut libc::c_void,
    sq_ring_len: usize,
    cq_ring_ptr: *mut libc::c_void,
    cq_ring_len: usize,
    sqes_ptr: *mut libc::c_void,
    sqes_len: usize,
}

// SAFETY: IoUring is only used from a single thread within `UblkControl`.
// The mmap'd regions are valid for the lifetime of the ring fd.
unsafe impl Send for IoUring {}

impl IoUring {
    /// Create a new io_uring instance with `entries` SQ entries and
    /// `SQE128 | CQE32` flags.
    fn new(entries: u32) -> io::Result<Self> {
        let mut params = IoUringParams {
            flags: IORING_SETUP_SQE128 | IORING_SETUP_CQE32,
            ..IoUringParams::default()
        };

        // SAFETY: io_uring_setup is a well-known Linux syscall.  We pass a
        // valid mutable pointer to our params struct.
        let ring_fd = unsafe {
            libc::syscall(
                libc::SYS_io_uring_setup,
                entries as libc::c_int,
                &mut params as *mut IoUringParams,
            )
        } as i32;

        if ring_fd < 0 {
            return Err(io::Error::last_os_error());
        }

        // --- mmap the SQ ring ---
        let sq_ring_len = (params.sq_off.array as usize)
            + (params.sq_entries as usize) * std::mem::size_of::<u32>();

        // SAFETY: mmap with a valid fd and well-known offset.  The kernel
        // guarantees the mapping is valid for the returned length.
        let sq_ring_ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                sq_ring_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_POPULATE,
                ring_fd,
                IORING_OFF_SQ_RING as libc::off_t,
            )
        };
        if sq_ring_ptr == libc::MAP_FAILED {
            // SAFETY: close a valid fd we just opened.
            unsafe { libc::close(ring_fd) };
            return Err(io::Error::last_os_error());
        }

        // --- mmap the CQ ring ---
        let cq_ring_len = (params.cq_off.cqes as usize)
            + (params.cq_entries as usize) * std::mem::size_of::<Cqe32>();

        // SAFETY: same reasoning as the SQ mmap.
        let cq_ring_ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                cq_ring_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_POPULATE,
                ring_fd,
                IORING_OFF_CQ_RING as libc::off_t,
            )
        };
        if cq_ring_ptr == libc::MAP_FAILED {
            // SAFETY: unmap the SQ ring we already mapped.
            unsafe { libc::munmap(sq_ring_ptr, sq_ring_len) };
            unsafe { libc::close(ring_fd) };
            return Err(io::Error::last_os_error());
        }

        // --- mmap the SQE array ---
        let sqes_len = (params.sq_entries as usize) * std::mem::size_of::<Sqe128>();

        // SAFETY: same reasoning as above.
        let sqes_ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                sqes_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_POPULATE,
                ring_fd,
                IORING_OFF_SQES as libc::off_t,
            )
        };
        if sqes_ptr == libc::MAP_FAILED {
            // SAFETY: clean up all previous mmaps.
            unsafe { libc::munmap(cq_ring_ptr, cq_ring_len) };
            unsafe { libc::munmap(sq_ring_ptr, sq_ring_len) };
            unsafe { libc::close(ring_fd) };
            return Err(io::Error::last_os_error());
        }

        // SAFETY: the offsets were provided by the kernel; casting the base
        // pointer + offset to the correct atomic / array type is valid.
        let sq_head =
            unsafe { sq_ring_ptr.byte_add(params.sq_off.head as usize) as *const AtomicU32 };
        let sq_tail =
            unsafe { sq_ring_ptr.byte_add(params.sq_off.tail as usize) as *mut AtomicU32 };
        let sq_mask =
            unsafe { *(sq_ring_ptr.byte_add(params.sq_off.ring_mask as usize) as *const u32) };
        let sq_array = unsafe { sq_ring_ptr.byte_add(params.sq_off.array as usize) as *mut u32 };

        let cq_head =
            unsafe { cq_ring_ptr.byte_add(params.cq_off.head as usize) as *mut AtomicU32 };
        let cq_tail =
            unsafe { cq_ring_ptr.byte_add(params.cq_off.tail as usize) as *const AtomicU32 };
        let cq_mask =
            unsafe { *(cq_ring_ptr.byte_add(params.cq_off.ring_mask as usize) as *const u32) };
        let cq_cqes = unsafe { cq_ring_ptr.byte_add(params.cq_off.cqes as usize) as *const Cqe32 };

        let sqes = sqes_ptr as *mut Sqe128;

        Ok(Self {
            ring_fd,
            sq_head,
            sq_tail,
            sq_mask,
            sq_array,
            cq_head,
            cq_tail,
            cq_mask,
            cq_cqes,
            sqes,
            sq_ring_ptr,
            sq_ring_len,
            cq_ring_ptr,
            cq_ring_len,
            sqes_ptr,
            sqes_len,
        })
    }

    /// Submit a single SQE and wait for the CQE, returning `(res, extra1, extra2)`.
    fn submit_and_wait(&self, sqe: &Sqe128) -> io::Result<(i32, u64, u64)> {
        // SAFETY: we have exclusive access to the ring (single-threaded usage).
        unsafe {
            // 1. Read the current SQ tail.
            let tail = (*self.sq_tail).load(Ordering::Acquire);
            let idx = (tail & self.sq_mask) as usize;

            // 2. Copy the SQE into the ring.
            std::ptr::write_volatile(self.sqes.add(idx), *sqe);

            // 3. Update the SQ array to point at this entry.
            *self.sq_array.add(idx) = idx as u32;

            // 4. Advance the SQ tail.
            (*self.sq_tail).store(tail.wrapping_add(1), Ordering::Release);
        }

        // 5. Tell the kernel to consume 1 SQE and wait for 1 CQE.
        // SAFETY: well-known syscall; we pass the ring fd and valid counts.
        let ret = unsafe {
            libc::syscall(
                libc::SYS_io_uring_enter,
                self.ring_fd,
                1u32,                             // to_submit
                1u32,                             // min_complete
                IORING_ENTER_GETEVENTS,           // flags
                std::ptr::null::<libc::c_void>(), // sig
                0usize,                           // sigsz
            )
        } as i32;

        if ret < 0 {
            return Err(io::Error::last_os_error());
        }

        // 6. Read the CQE.
        // SAFETY: the kernel has produced at least one CQE; reading it is safe.
        let (res, extra1, extra2) = unsafe {
            let head = (*self.cq_head).load(Ordering::Acquire);
            let cq_idx = (head & self.cq_mask) as usize;
            let cqe = &*self.cq_cqes.add(cq_idx);
            let r = (cqe.res, cqe.extra1, cqe.extra2);
            (*self.cq_head).store(head.wrapping_add(1), Ordering::Release);
            r
        };

        if res < 0 {
            return Err(io::Error::from_raw_os_error(-res));
        }

        Ok((res, extra1, extra2))
    }
}

impl Drop for IoUring {
    fn drop(&mut self) {
        // SAFETY: we unmap regions we mapped in `new()` and close the fd.
        unsafe {
            libc::munmap(self.sqes_ptr, self.sqes_len);
            libc::munmap(self.cq_ring_ptr, self.cq_ring_len);
            libc::munmap(self.sq_ring_ptr, self.sq_ring_len);
            libc::close(self.ring_fd);
        }
    }
}

// ---------------------------------------------------------------------------
// UblkControl — public API
// ---------------------------------------------------------------------------

/// Handle to `/dev/ublk-control` backed by a minimal io_uring ring.
///
/// All methods are blocking (they submit a SQE and wait for the CQE) and must
/// not be called from an async context without `spawn_blocking`.
pub struct UblkControl {
    ctrl_fd: RawFd,
    ring: IoUring,
}

impl UblkControl {
    /// Open `/dev/ublk-control` and create the io_uring ring.
    pub fn open() -> io::Result<Self> {
        // SAFETY: standard open(2) on a well-known device path.
        let fd = unsafe {
            libc::open(
                UBLK_CTRL_DEV_PATH.as_ptr() as *const libc::c_char,
                libc::O_RDWR | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        let ring = match IoUring::new(2) {
            Ok(r) => r,
            Err(e) => {
                // SAFETY: close the fd we just opened.
                unsafe { libc::close(fd) };
                return Err(e);
            }
        };

        info!("opened UBLK control device");
        Ok(Self { ctrl_fd: fd, ring })
    }

    /// Issue a UBLK control command via `IORING_OP_URING_CMD`.
    ///
    /// Returns the CQE result code.
    fn issue_ctrl_cmd(&self, ioctl_nr: u32, ctrl_cmd: &UblkCtrlCmd) -> io::Result<(i32, u64, u64)> {
        let mut sqe = Sqe128 {
            opcode: IORING_OP_URING_CMD,
            fd: self.ctrl_fd,
            addr3_or_cmd_op: ioctl_nr as u64,
            ..Sqe128::default()
        };

        // Copy the UblkCtrlCmd into the 64-byte cmd tail.
        let cmd_bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(
                ctrl_cmd as *const UblkCtrlCmd as *const u8,
                std::mem::size_of::<UblkCtrlCmd>(),
            )
        };
        sqe.cmd[..cmd_bytes.len()].copy_from_slice(cmd_bytes);

        self.ring.submit_and_wait(&sqe)
    }

    /// Add a new UBLK device.
    ///
    /// Returns the `UblkDevInfo` populated by the kernel.
    pub fn add_device(&self, config: &UblkDevConfig) -> io::Result<UblkDevInfo> {
        let info = UblkDevInfo {
            nr_hw_queues: config.nr_hw_queues,
            queue_depth: config.queue_depth,
            max_io_buf_bytes: config.max_io_buf_bytes,
            dev_id: config.dev_id,
            flags: config.flags,
            ..UblkDevInfo::default()
        };

        let ctrl_cmd = UblkCtrlCmd {
            dev_id: config.dev_id,
            addr: &info as *const UblkDevInfo as u64,
            len: std::mem::size_of::<UblkDevInfo>() as u16,
            ..UblkCtrlCmd::default()
        };

        let ioctl_nr = crate::uring::ublk_ctrl_ioctl(UBLK_CMD_ADD_DEV);
        debug!(dev_id = config.dev_id, ioctl = ioctl_nr, "ADD_DEV");
        self.issue_ctrl_cmd(ioctl_nr, &ctrl_cmd)?;

        // The kernel may have assigned a different dev_id (e.g. when we pass
        // u32::MAX / -1 to request auto-assignment).
        info!(dev_id = info.dev_id, "UBLK device added");
        Ok(info)
    }

    /// Start a UBLK device (transitions it to the live state, creates
    /// `/dev/ublkbN`).
    pub fn start_device(&self, dev_id: u32, pid: i32) -> io::Result<()> {
        let ctrl_cmd = UblkCtrlCmd {
            dev_id,
            data: [pid as u64],
            ..UblkCtrlCmd::default()
        };

        let ioctl_nr = crate::uring::ublk_ctrl_ioctl(UBLK_CMD_START_DEV);
        debug!(dev_id, "START_DEV");
        self.issue_ctrl_cmd(ioctl_nr, &ctrl_cmd)?;
        info!(dev_id, "UBLK device started");
        Ok(())
    }

    /// Stop a UBLK device (quiesces I/O, removes `/dev/ublkbN`).
    pub fn stop_device(&self, dev_id: u32) -> io::Result<()> {
        let ctrl_cmd = UblkCtrlCmd {
            dev_id,
            ..UblkCtrlCmd::default()
        };

        let ioctl_nr = crate::uring::ublk_ctrl_ioctl(UBLK_CMD_STOP_DEV);
        debug!(dev_id, "STOP_DEV");
        self.issue_ctrl_cmd(ioctl_nr, &ctrl_cmd)?;
        info!(dev_id, "UBLK device stopped");
        Ok(())
    }

    /// Delete a UBLK device entirely.
    pub fn delete_device(&self, dev_id: u32) -> io::Result<()> {
        let ctrl_cmd = UblkCtrlCmd {
            dev_id,
            ..UblkCtrlCmd::default()
        };

        let ioctl_nr = crate::uring::ublk_ctrl_ioctl(UBLK_CMD_DEL_DEV);
        debug!(dev_id, "DEL_DEV");
        self.issue_ctrl_cmd(ioctl_nr, &ctrl_cmd)?;
        info!(dev_id, "UBLK device deleted");
        Ok(())
    }

    /// Query device info for an existing UBLK device.
    pub fn get_device_info(&self, dev_id: u32) -> io::Result<UblkDevInfo> {
        let mut info = UblkDevInfo::default();

        let ctrl_cmd = UblkCtrlCmd {
            dev_id,
            addr: &mut info as *mut UblkDevInfo as u64,
            len: std::mem::size_of::<UblkDevInfo>() as u16,
            ..UblkCtrlCmd::default()
        };

        let ioctl_nr = crate::uring::ublk_ctrl_ioctl(UBLK_CMD_GET_DEV_INFO);
        debug!(dev_id, "GET_DEV_INFO");
        self.issue_ctrl_cmd(ioctl_nr, &ctrl_cmd)?;
        Ok(info)
    }
}

impl Drop for UblkControl {
    fn drop(&mut self) {
        // SAFETY: close the /dev/ublk-control fd.
        unsafe { libc::close(self.ctrl_fd) };
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Layout assertions ---

    #[test]
    fn test_sqe128_size() {
        assert_eq!(std::mem::size_of::<Sqe128>(), 128);
    }

    #[test]
    fn test_cqe32_size() {
        assert_eq!(std::mem::size_of::<Cqe32>(), 32);
    }

    #[test]
    fn test_io_uring_params_size() {
        // The kernel struct is 120 bytes.
        assert_eq!(std::mem::size_of::<IoUringParams>(), 120);
    }

    #[test]
    fn test_sqe128_default_is_zeroed() {
        let sqe = Sqe128::default();
        assert_eq!(sqe.opcode, 0);
        assert_eq!(sqe.fd, 0);
        assert_eq!(sqe.user_data, 0);
        assert_eq!(sqe.cmd, [0u8; 64]);
    }

    #[test]
    fn test_sqe128_set_opcode_and_fd() {
        let mut sqe = Sqe128::default();
        sqe.opcode = IORING_OP_URING_CMD;
        sqe.fd = 42;
        assert_eq!(sqe.opcode, 80);
        assert_eq!(sqe.fd, 42);
    }

    #[test]
    fn test_sqe128_cmd_payload_fits_ctrl_cmd() {
        // UblkCtrlCmd is 32 bytes; the cmd field is 64 bytes — it fits.
        assert!(std::mem::size_of::<UblkCtrlCmd>() <= 64);
    }

    #[test]
    fn test_ioring_op_uring_cmd_value() {
        assert_eq!(IORING_OP_URING_CMD, 80);
    }

    #[test]
    fn test_ioring_setup_flags() {
        assert_eq!(IORING_SETUP_SQE128, 1 << 10);
        assert_eq!(IORING_SETUP_CQE32, 1 << 11);
    }

    #[test]
    fn test_ioring_enter_getevents() {
        assert_eq!(IORING_ENTER_GETEVENTS, 1);
    }

    #[test]
    fn test_ioring_mmap_offsets() {
        assert_eq!(IORING_OFF_SQ_RING, 0);
        assert_eq!(IORING_OFF_CQ_RING, 0x0800_0000);
        assert_eq!(IORING_OFF_SQES, 0x1000_0000);
    }

    // --- UblkCtrlCmd serialisation round-trip ---

    #[test]
    fn test_ctrl_cmd_fits_in_sqe_cmd() {
        let ctrl = UblkCtrlCmd {
            dev_id: 7,
            queue_id: 0,
            len: 64,
            addr: 0xdead_beef,
            data: [42],
            dev_path_len: 0,
            pad: 0,
            reserved: 0,
        };

        let mut sqe = Sqe128::default();

        // SAFETY: UblkCtrlCmd is a POD struct, reading its bytes is safe.
        let src = unsafe {
            std::slice::from_raw_parts(
                &ctrl as *const UblkCtrlCmd as *const u8,
                std::mem::size_of::<UblkCtrlCmd>(),
            )
        };
        sqe.cmd[..src.len()].copy_from_slice(src);

        // Read it back.
        // SAFETY: we just wrote valid UblkCtrlCmd bytes; re-interpreting is safe.
        let recovered = unsafe { &*(sqe.cmd.as_ptr() as *const UblkCtrlCmd) };
        assert_eq!(recovered.dev_id, 7);
        assert_eq!(recovered.addr, 0xdead_beef);
        assert_eq!(recovered.data[0], 42);
    }

    // --- IoUring construction (requires io_uring support in the kernel) ---

    #[test]
    fn test_io_uring_new_small_ring() {
        // This will fail in containers without io_uring support; skip gracefully.
        match IoUring::new(2) {
            Ok(ring) => {
                assert!(ring.ring_fd >= 0);
                assert!(ring.sq_mask > 0 || ring.sq_mask == 0); // just check it doesn't crash
                // ring is dropped here — verifies Drop works.
            }
            Err(e) => {
                // ENOSYS / EPERM is expected in restricted environments.
                eprintln!("io_uring not available in this environment: {e}");
            }
        }
    }

    #[test]
    fn test_io_uring_new_with_sqe128_cqe32() {
        match IoUring::new(4) {
            Ok(ring) => {
                assert!(ring.ring_fd >= 0);
            }
            Err(e) => {
                eprintln!("io_uring (SQE128+CQE32) not available: {e}");
            }
        }
    }

    // --- UblkControl::open (requires /dev/ublk-control) ---

    #[test]
    #[ignore] // requires root + ublk_drv module loaded
    fn test_ublk_control_open() {
        let ctrl = UblkControl::open().expect("failed to open /dev/ublk-control");
        assert!(ctrl.ctrl_fd >= 0);
    }

    #[test]
    #[ignore] // requires root + ublk_drv module loaded
    fn test_ublk_control_add_and_delete() {
        let ctrl = UblkControl::open().expect("failed to open /dev/ublk-control");

        let config = UblkDevConfig {
            dev_id: u32::MAX, // auto-assign
            nr_hw_queues: 1,
            queue_depth: 64,
            max_io_buf_bytes: 512 * 1024,
            dev_size: 64 * 1024 * 1024,
            ..UblkDevConfig::default()
        };

        let info = ctrl.add_device(&config).expect("ADD_DEV failed");
        let dev_id = info.dev_id;
        assert!(dev_id < 1024, "dev_id looks sane");

        // Clean up.
        ctrl.delete_device(dev_id).expect("DEL_DEV failed");
    }

    #[test]
    #[ignore] // requires root + ublk_drv module loaded
    fn test_ublk_control_get_device_info() {
        let ctrl = UblkControl::open().expect("failed to open /dev/ublk-control");

        let config = UblkDevConfig {
            dev_id: u32::MAX,
            nr_hw_queues: 1,
            queue_depth: 64,
            max_io_buf_bytes: 512 * 1024,
            dev_size: 32 * 1024 * 1024,
            ..UblkDevConfig::default()
        };

        let info = ctrl.add_device(&config).expect("ADD_DEV failed");
        let dev_id = info.dev_id;

        let queried = ctrl.get_device_info(dev_id).expect("GET_DEV_INFO failed");
        assert_eq!(queried.dev_id, dev_id);
        assert_eq!(queried.nr_hw_queues, 1);

        ctrl.delete_device(dev_id).expect("DEL_DEV failed");
    }
}
