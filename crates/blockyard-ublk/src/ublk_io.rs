//! UBLK I/O data-plane — per-queue io_uring loop that serves block I/O.
//!
//! After a UBLK device has been added via [`super::ublk_ctrl::UblkControl`],
//! each hardware queue needs a dedicated thread that:
//!
//! 1. Opens `/dev/ublkcN` (the per-device character device)
//! 2. Sets up an io_uring ring
//! 3. Pre-submits `UBLK_U_IO_FETCH_REQ` SQEs for every tag (0..queue_depth)
//! 4. Waits for CQEs from the kernel — each CQE carries a block I/O request
//!    (read / write / flush / discard)
//! 5. Processes the request against backing storage
//! 6. Submits `UBLK_U_IO_COMMIT_AND_FETCH_REQ` SQEs with the result
//!
//! This module provides [`UblkQueue`] which encapsulates one such queue loop,
//! and [`UblkIoServer`] which manages the full set of queue threads for a
//! single UBLK device.

use std::io;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::thread::JoinHandle;

use tracing::{debug, error, info, warn};

use crate::nbd::MemBlockStore;
use crate::uring::{
    UBLK_IO_COMMIT_AND_FETCH_REQ, UBLK_IO_FETCH_REQ, UBLK_IO_OP_DISCARD, UBLK_IO_OP_FLUSH,
    UBLK_IO_OP_READ, UBLK_IO_OP_WRITE, UBLK_IO_RES_ABORT, UBLK_IO_RES_OK, UblkIoCmd,
};

// ---------------------------------------------------------------------------
// io_uring constants (same as ublk_ctrl but we keep them local for clarity)
// ---------------------------------------------------------------------------

const IORING_OP_URING_CMD: u8 = 80;
const IORING_SETUP_SQE128: u32 = 1 << 10;
const IORING_SETUP_CQE32: u32 = 1 << 11;
const IORING_ENTER_GETEVENTS: u32 = 1;
const IORING_OFF_SQ_RING: u64 = 0;
const IORING_OFF_CQ_RING: u64 = 0x0800_0000;
const IORING_OFF_SQES: u64 = 0x1000_0000;

// ---------------------------------------------------------------------------
// Shared io_uring structs (identical to ublk_ctrl.rs)
// ---------------------------------------------------------------------------

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
        // SAFETY: all-zero is valid for this POD struct.
        unsafe { std::mem::zeroed() }
    }
}

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

#[repr(C, align(8))]
#[derive(Clone, Copy)]
struct Sqe128 {
    opcode: u8,
    flags: u8,
    ioprio: u16,
    fd: i32,
    off_or_addr2: u64,
    addr_or_splice: u64,
    len: u32,
    op_flags: u32,
    user_data: u64,
    buf_index_or_group: u16,
    personality: u16,
    splice_fd_or_file_index: i32,
    addr3_or_cmd_op: u64,
    _pad2: u64,
    cmd: [u8; 64],
}

impl Default for Sqe128 {
    fn default() -> Self {
        // SAFETY: all-zero is valid.
        unsafe { std::mem::zeroed() }
    }
}

const _: () = assert!(std::mem::size_of::<Sqe128>() == 128);

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct Cqe32 {
    user_data: u64,
    res: i32,
    flags: u32,
    extra1: u64,
    extra2: u64,
}

const _: () = assert!(std::mem::size_of::<Cqe32>() == 32);

// ---------------------------------------------------------------------------
// Minimal io_uring ring (I/O variant — larger depth)
// ---------------------------------------------------------------------------

struct IoUring {
    ring_fd: RawFd,
    sq_head: *const AtomicU32,
    sq_tail: *mut AtomicU32,
    sq_mask: u32,
    sq_array: *mut u32,
    cq_head: *mut AtomicU32,
    cq_tail: *const AtomicU32,
    cq_mask: u32,
    cq_cqes: *const Cqe32,
    sqes: *mut Sqe128,
    sq_ring_ptr: *mut libc::c_void,
    sq_ring_len: usize,
    cq_ring_ptr: *mut libc::c_void,
    cq_ring_len: usize,
    sqes_ptr: *mut libc::c_void,
    sqes_len: usize,
}

// SAFETY: used only within a single queue thread.
unsafe impl Send for IoUring {}

impl IoUring {
    fn new(entries: u32) -> io::Result<Self> {
        let mut params = IoUringParams {
            flags: IORING_SETUP_SQE128 | IORING_SETUP_CQE32,
            ..IoUringParams::default()
        };

        // SAFETY: standard io_uring_setup syscall.
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

        let sq_ring_len = (params.sq_off.array as usize)
            + (params.sq_entries as usize) * std::mem::size_of::<u32>();

        // SAFETY: mmap with valid fd and kernel-provided offsets.
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
            unsafe { libc::close(ring_fd) };
            return Err(io::Error::last_os_error());
        }

        let cq_ring_len = (params.cq_off.cqes as usize)
            + (params.cq_entries as usize) * std::mem::size_of::<Cqe32>();

        // SAFETY: same as above.
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
            unsafe { libc::munmap(sq_ring_ptr, sq_ring_len) };
            unsafe { libc::close(ring_fd) };
            return Err(io::Error::last_os_error());
        }

        let sqes_len = (params.sq_entries as usize) * std::mem::size_of::<Sqe128>();

        // SAFETY: same as above.
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
            unsafe { libc::munmap(cq_ring_ptr, cq_ring_len) };
            unsafe { libc::munmap(sq_ring_ptr, sq_ring_len) };
            unsafe { libc::close(ring_fd) };
            return Err(io::Error::last_os_error());
        }

        // SAFETY: pointer arithmetic on kernel-provided offsets.
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

    /// Queue a single SQE into the submission ring (does NOT submit to kernel).
    fn push_sqe(&self, sqe: &Sqe128) -> io::Result<()> {
        // SAFETY: single-threaded per-queue access.
        unsafe {
            let tail = (*self.sq_tail).load(Ordering::Acquire);
            let head = (*self.sq_head).load(Ordering::Acquire);
            if tail.wrapping_sub(head) > self.sq_mask {
                return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
            }
            let idx = (tail & self.sq_mask) as usize;
            std::ptr::write_volatile(self.sqes.add(idx), *sqe);
            *self.sq_array.add(idx) = idx as u32;
            (*self.sq_tail).store(tail.wrapping_add(1), Ordering::Release);
        }
        Ok(())
    }

    /// Submit all queued SQEs and wait for at least `min_complete` CQEs.
    fn submit_and_wait(&self, to_submit: u32, min_complete: u32) -> io::Result<()> {
        // SAFETY: well-known syscall with valid fd.
        let ret = unsafe {
            libc::syscall(
                libc::SYS_io_uring_enter,
                self.ring_fd,
                to_submit,
                min_complete,
                IORING_ENTER_GETEVENTS,
                std::ptr::null::<libc::c_void>(),
                0usize,
            )
        } as i32;
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Pop one CQE from the completion ring.  Returns `None` if the CQ is
    /// empty.
    fn pop_cqe(&self) -> Option<Cqe32> {
        // SAFETY: single-threaded.
        unsafe {
            let head = (*self.cq_head).load(Ordering::Acquire);
            let tail = (*self.cq_tail).load(Ordering::Acquire);
            if head == tail {
                return None;
            }
            let idx = (head & self.cq_mask) as usize;
            let cqe = *self.cq_cqes.add(idx);
            (*self.cq_head).store(head.wrapping_add(1), Ordering::Release);
            Some(cqe)
        }
    }
}

impl Drop for IoUring {
    fn drop(&mut self) {
        // SAFETY: unmap regions and close the fd.
        unsafe {
            libc::munmap(self.sqes_ptr, self.sqes_len);
            libc::munmap(self.cq_ring_ptr, self.cq_ring_len);
            libc::munmap(self.sq_ring_ptr, self.sq_ring_len);
            libc::close(self.ring_fd);
        }
    }
}

// ---------------------------------------------------------------------------
// UBLK I/O request descriptor
// ---------------------------------------------------------------------------

/// Decoded UBLK I/O request from a CQE.
#[derive(Debug, Clone, Copy)]
pub struct UblkIoRequest {
    /// The I/O tag (0..queue_depth).
    pub tag: u16,
    /// Operation type (READ / WRITE / FLUSH / DISCARD).
    pub op: u32,
    /// Byte offset on the block device.
    pub offset: u64,
    /// Number of bytes.
    pub length: u32,
    /// Pointer to the kernel-provided I/O buffer (for WRITE: data to read from;
    /// for READ: buffer to write data into).
    pub buf_addr: u64,
}

// ---------------------------------------------------------------------------
// Per-queue I/O loop
// ---------------------------------------------------------------------------

/// Runs the I/O loop for a single UBLK queue.
///
/// This is the core of the UBLK data plane: it opens `/dev/ublkcN`, creates
/// an io_uring ring, and services block I/O requests in a tight loop until
/// `stop` is signalled.
pub struct UblkQueue {
    dev_id: u32,
    queue_id: u16,
    queue_depth: u16,
    char_fd: RawFd,
    ring: IoUring,
    /// Per-tag I/O buffers (allocated by userspace for the kernel to DMA into).
    io_bufs: Vec<Vec<u8>>,
}

impl UblkQueue {
    /// Open the character device and set up the ring.
    pub fn open(
        dev_id: u32,
        queue_id: u16,
        queue_depth: u16,
        io_buf_size: u32,
    ) -> io::Result<Self> {
        let path = crate::uring::ublk_char_path(dev_id);
        let c_path = std::ffi::CString::new(path.as_str())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        // SAFETY: standard open(2).
        let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        let ring = match IoUring::new(queue_depth as u32 * 2) {
            Ok(r) => r,
            Err(e) => {
                unsafe { libc::close(fd) };
                return Err(e);
            }
        };

        // Allocate per-tag I/O buffers.
        let io_bufs: Vec<Vec<u8>> = (0..queue_depth)
            .map(|_| vec![0u8; io_buf_size as usize])
            .collect();

        info!(
            dev_id,
            queue_id, queue_depth, io_buf_size, "UBLK queue opened"
        );

        Ok(Self {
            dev_id,
            queue_id,
            queue_depth,
            char_fd: fd,
            ring,
            io_bufs,
        })
    }

    /// Build a `FETCH_REQ` or `COMMIT_AND_FETCH` SQE for the given tag.
    fn build_io_sqe(&self, cmd_op: u32, tag: u16, result: i32) -> Sqe128 {
        let ioctl_nr = crate::uring::ublk_io_ioctl(cmd_op);
        let mut sqe = Sqe128 {
            opcode: IORING_OP_URING_CMD,
            fd: self.char_fd,
            user_data: tag as u64,
            addr3_or_cmd_op: ioctl_nr as u64,
            ..Sqe128::default()
        };

        let io_cmd = UblkIoCmd {
            q_id: self.queue_id,
            tag,
            result,
            addr: self.io_bufs[tag as usize].as_ptr() as u64,
        };

        // SAFETY: UblkIoCmd is a POD struct; reading its bytes is safe.
        let src = unsafe {
            std::slice::from_raw_parts(
                &io_cmd as *const UblkIoCmd as *const u8,
                std::mem::size_of::<UblkIoCmd>(),
            )
        };
        sqe.cmd[..src.len()].copy_from_slice(src);

        sqe
    }

    /// Run the I/O loop, servicing requests against `store` until `stop` is
    /// signalled.
    pub fn run(&mut self, store: &MemBlockStore, stop: &AtomicBool) -> io::Result<()> {
        // Pre-submit FETCH_REQ for every tag.
        let mut pending_submits = 0u32;
        for tag in 0..self.queue_depth {
            let sqe = self.build_io_sqe(UBLK_IO_FETCH_REQ, tag, 0);
            self.ring.push_sqe(&sqe)?;
            pending_submits += 1;
        }

        // Initial submit.
        self.ring.submit_and_wait(pending_submits, 0)?;

        info!(
            dev_id = self.dev_id,
            queue_id = self.queue_id,
            "UBLK I/O loop started"
        );

        // Main loop.
        while !stop.load(Ordering::Relaxed) {
            // Wait for at least one CQE.
            self.ring.submit_and_wait(0, 1)?;

            // Drain all available CQEs.
            while let Some(cqe) = self.ring.pop_cqe() {
                let tag = cqe.user_data as u16;

                if cqe.res < 0 {
                    if cqe.res == UBLK_IO_RES_ABORT {
                        debug!(
                            dev_id = self.dev_id,
                            queue_id = self.queue_id,
                            tag,
                            "UBLK I/O aborted (device stopping)"
                        );
                        return Ok(());
                    }
                    warn!(
                        dev_id = self.dev_id,
                        queue_id = self.queue_id,
                        tag,
                        res = cqe.res,
                        "UBLK I/O error"
                    );
                    continue;
                }

                // Decode the I/O request from the CQE extra data.
                //
                // The UBLK driver packs: op (8 bits) | unused (24 bits) in
                // the lower 32 bits of extra1, and the sector count / length
                // information in the remaining fields.  The exact layout
                // depends on the kernel version; we use the CQE-based
                // approach where:
                //   extra1 = (op << 0) | (nr_sectors << 16)
                //   extra2 = start_sector
                let op = (cqe.extra1 & 0xFF) as u32;
                let nr_sectors = ((cqe.extra1 >> 16) & 0xFFFF) as u32;
                let start_sector = cqe.extra2;
                let offset = start_sector * 512;
                let length = nr_sectors * 512;

                let result = match op {
                    UBLK_IO_OP_READ => {
                        let data = store.read(offset, length);
                        let buf = &mut self.io_bufs[tag as usize];
                        buf[..data.len()].copy_from_slice(&data);
                        length as i32
                    }
                    UBLK_IO_OP_WRITE => {
                        let buf = &self.io_bufs[tag as usize];
                        store.write(offset, &buf[..length as usize]);
                        length as i32
                    }
                    UBLK_IO_OP_FLUSH => {
                        store.flush();
                        UBLK_IO_RES_OK
                    }
                    UBLK_IO_OP_DISCARD => {
                        store.trim(offset, length);
                        UBLK_IO_RES_OK
                    }
                    _ => {
                        warn!(op, "unknown UBLK I/O op");
                        -libc::EIO
                    }
                };

                // Submit COMMIT_AND_FETCH for this tag.
                let sqe = self.build_io_sqe(UBLK_IO_COMMIT_AND_FETCH_REQ, tag, result);
                self.ring.push_sqe(&sqe)?;
            }

            // Submit any queued SQEs (COMMIT_AND_FETCH).
            self.ring.submit_and_wait(self.queue_depth as u32, 0)?;
        }

        info!(
            dev_id = self.dev_id,
            queue_id = self.queue_id,
            "UBLK I/O loop stopped"
        );
        Ok(())
    }
}

impl Drop for UblkQueue {
    fn drop(&mut self) {
        // SAFETY: close the char device fd.
        unsafe { libc::close(self.char_fd) };
    }
}

// ---------------------------------------------------------------------------
// UblkIoServer — manages all queue threads for one UBLK device
// ---------------------------------------------------------------------------

/// Manages the per-queue I/O threads for a single UBLK device.
pub struct UblkIoServer {
    dev_id: u32,
    nr_queues: u16,
    queue_depth: u16,
    io_buf_size: u32,
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
}

impl UblkIoServer {
    /// Create a new I/O server (does not start threads yet).
    pub fn new(dev_id: u32, nr_queues: u16, queue_depth: u16, io_buf_size: u32) -> Self {
        Self {
            dev_id,
            nr_queues,
            queue_depth,
            io_buf_size,
            stop: Arc::new(AtomicBool::new(false)),
            threads: Vec::new(),
        }
    }

    /// Spawn one thread per queue and start serving I/O.
    pub fn start(&mut self, store: MemBlockStore) -> io::Result<()> {
        info!(
            dev_id = self.dev_id,
            nr_queues = self.nr_queues,
            queue_depth = self.queue_depth,
            "starting UBLK I/O server"
        );

        for qid in 0..self.nr_queues {
            let dev_id = self.dev_id;
            let queue_depth = self.queue_depth;
            let io_buf_size = self.io_buf_size;
            let stop = self.stop.clone();
            let store = store.clone();

            let handle = std::thread::Builder::new()
                .name(format!("ublk-q{qid}"))
                .spawn(
                    move || match UblkQueue::open(dev_id, qid, queue_depth, io_buf_size) {
                        Ok(mut queue) => {
                            if let Err(e) = queue.run(&store, &stop) {
                                error!(dev_id, queue_id = qid, error = %e, "UBLK queue error");
                            }
                        }
                        Err(e) => {
                            error!(dev_id, queue_id = qid, error = %e, "failed to open UBLK queue");
                        }
                    },
                )?;
            self.threads.push(handle);
        }

        Ok(())
    }

    /// Signal all queue threads to stop and join them.
    pub fn stop(&mut self) {
        info!(dev_id = self.dev_id, "stopping UBLK I/O server");
        self.stop.store(true, Ordering::Release);

        for handle in self.threads.drain(..) {
            if let Err(e) = handle.join() {
                error!(dev_id = self.dev_id, "queue thread panicked: {e:?}");
            }
        }
    }

    /// Whether the server has been told to stop.
    pub fn is_stopped(&self) -> bool {
        self.stop.load(Ordering::Acquire)
    }

    /// The device id this server is managing.
    pub fn dev_id(&self) -> u32 {
        self.dev_id
    }
}

impl Drop for UblkIoServer {
    fn drop(&mut self) {
        if !self.threads.is_empty() {
            self.stop();
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqe128_size() {
        assert_eq!(std::mem::size_of::<Sqe128>(), 128);
    }

    #[test]
    fn test_cqe32_size() {
        assert_eq!(std::mem::size_of::<Cqe32>(), 32);
    }

    #[test]
    fn test_ublk_io_request_debug() {
        let req = UblkIoRequest {
            tag: 0,
            op: UBLK_IO_OP_READ,
            offset: 4096,
            length: 512,
            buf_addr: 0,
        };
        let s = format!("{req:?}");
        assert!(s.contains("tag: 0"));
        assert!(s.contains("offset: 4096"));
    }

    #[test]
    fn test_ublk_io_server_new() {
        let server = UblkIoServer::new(0, 1, 64, 512 * 1024);
        assert_eq!(server.dev_id(), 0);
        assert!(!server.is_stopped());
    }

    #[test]
    fn test_ublk_io_server_stop_without_start() {
        let mut server = UblkIoServer::new(0, 1, 64, 512 * 1024);
        server.stop(); // should not panic
        assert!(server.is_stopped());
    }

    #[test]
    fn test_ublk_io_server_drop_without_start() {
        let server = UblkIoServer::new(0, 1, 64, 512 * 1024);
        drop(server); // should not panic
    }

    #[test]
    fn test_io_uring_constants() {
        assert_eq!(IORING_OP_URING_CMD, 80);
        assert_eq!(IORING_SETUP_SQE128, 1 << 10);
        assert_eq!(IORING_SETUP_CQE32, 1 << 11);
    }

    #[test]
    fn test_io_uring_params_default() {
        let p = IoUringParams::default();
        assert_eq!(p.sq_entries, 0);
        assert_eq!(p.flags, 0);
    }

    #[test]
    fn test_sqe128_build_io_fields() {
        let mut sqe = Sqe128::default();
        sqe.opcode = IORING_OP_URING_CMD;
        sqe.fd = 5;
        sqe.user_data = 42;
        assert_eq!(sqe.opcode, 80);
        assert_eq!(sqe.fd, 5);
        assert_eq!(sqe.user_data, 42);
    }

    #[test]
    fn test_io_cmd_fits_in_sqe_cmd() {
        let io_cmd = UblkIoCmd {
            q_id: 0,
            tag: 7,
            result: 0,
            addr: 0xAAAA_BBBB,
        };
        let mut sqe = Sqe128::default();
        // SAFETY: UblkIoCmd is a POD struct.
        let src = unsafe {
            std::slice::from_raw_parts(
                &io_cmd as *const UblkIoCmd as *const u8,
                std::mem::size_of::<UblkIoCmd>(),
            )
        };
        sqe.cmd[..src.len()].copy_from_slice(src);
        // SAFETY: valid UblkIoCmd bytes.
        let recovered = unsafe { &*(sqe.cmd.as_ptr() as *const UblkIoCmd) };
        assert_eq!(recovered.tag, 7);
        assert_eq!(recovered.addr, 0xAAAA_BBBB);
    }

    #[test]
    fn test_io_uring_params_size() {
        assert_eq!(std::mem::size_of::<IoUringParams>(), 120);
    }

    #[test]
    fn test_io_uring_new_for_io() {
        // Best-effort: may not work in containers.
        match IoUring::new(4) {
            Ok(ring) => {
                assert!(ring.ring_fd >= 0);
            }
            Err(e) => {
                eprintln!("io_uring not available: {e}");
            }
        }
    }

    // --- Integration tests (require root + ublk_drv) ---

    #[test]
    #[ignore] // requires root + ublk_drv module loaded + a live UBLK device
    fn test_ublk_queue_open() {
        // This assumes device 0 exists and has been added.
        let queue = UblkQueue::open(0, 0, 64, 512 * 1024);
        match queue {
            Ok(_) => info!("opened UBLK queue 0 on device 0"),
            Err(e) => eprintln!("could not open UBLK queue: {e}"),
        }
    }
}
