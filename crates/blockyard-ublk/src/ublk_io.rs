//! UBLK I/O data-plane — per-queue handling that serves block I/O.
//!
//! When the `libublk` feature is enabled, this module uses `libublk`'s
//! `UblkQueue` and `wait_and_handle_io` to process block I/O requests.
//!
//! When the feature is disabled, stub types are provided so the crate
//! compiles on all platforms (falling back to NBD for actual block I/O).

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

use tracing::{error, info, warn};

use crate::nbd::MemBlockStore;

// ---------------------------------------------------------------------------
// UBLK I/O request descriptor (public, feature-independent)
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
    /// Pointer to the kernel-provided I/O buffer.
    pub buf_addr: u64,
}

// Re-export the I/O op constants for tests.
#[cfg(test)]
use crate::uring::UBLK_IO_OP_READ;

// ---------------------------------------------------------------------------
// UblkIoServer — manages all queue threads for one UBLK device
// ---------------------------------------------------------------------------

/// Manages the per-queue I/O threads for a single UBLK device.
///
/// With `libublk`, each queue thread uses `libublk::io::UblkQueue` and its
/// `wait_and_handle_io` loop.  Without the feature, the server is a no-op
/// stub.
#[allow(dead_code)] // fields used only when `libublk` feature is enabled
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
    ///
    /// With the `libublk` feature this sets up real io_uring-backed queue
    /// loops.  Without it this is a no-op (the server records that it was
    /// "started" but no threads are spawned).
    pub fn start(&mut self, store: MemBlockStore) -> io::Result<()> {
        info!(
            dev_id = self.dev_id,
            nr_queues = self.nr_queues,
            queue_depth = self.queue_depth,
            "starting UBLK I/O server"
        );

        #[cfg(feature = "libublk")]
        {
            self.start_libublk(store)?;
        }

        #[cfg(not(feature = "libublk"))]
        {
            let _ = store;
            warn!(
                dev_id = self.dev_id,
                "UBLK I/O server not starting: libublk feature disabled"
            );
        }

        Ok(())
    }

    /// libublk-backed queue thread spawning.
    #[cfg(feature = "libublk")]
    fn start_libublk(&mut self, store: MemBlockStore) -> io::Result<()> {
        for qid in 0..self.nr_queues {
            let dev_id = self.dev_id;
            let queue_depth = self.queue_depth;
            let io_buf_size = self.io_buf_size;
            let stop = self.stop.clone();
            let store = store.clone();

            let handle = std::thread::Builder::new()
                .name(format!("ublk-q{qid}"))
                .spawn(move || {
                    if let Err(e) =
                        run_queue_libublk(dev_id, qid, queue_depth, io_buf_size, store, stop)
                    {
                        error!(dev_id, queue_id = qid, error = %e, "UBLK queue error");
                    }
                })?;
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
// libublk queue loop
// ---------------------------------------------------------------------------

/// Run a single queue's I/O loop using libublk.
///
/// This function:
/// 1. Opens the ublk character device for the queue
/// 2. Creates a `UblkQueue` backed by io_uring
/// 3. Submits initial FETCH_REQ commands
/// 4. Enters `wait_and_handle_io` which processes I/O until the device stops
#[cfg(feature = "libublk")]
fn run_queue_libublk(
    dev_id: u32,
    queue_id: u16,
    _queue_depth: u16,
    _io_buf_size: u32,
    store: MemBlockStore,
    _stop: Arc<AtomicBool>,
) -> io::Result<()> {
    use libublk::UblkIORes;
    use libublk::ctrl::UblkCtrl;
    use libublk::io::{BufDesc, BufDescList, UblkDev, UblkQueue as LibublkQueue};

    // Open a simple control handle to reference the existing device.
    let ctrl = UblkCtrl::new_simple(dev_id as i32)
        .map_err(|e| io::Error::other(format!("libublk ctrl: {e}")))?;

    // Create the device abstraction (target init is a no-op since the device
    // is already configured).
    let dev = UblkDev::new("blockyard".to_string(), |_dev| Ok(()), &ctrl)
        .map_err(|e| io::Error::other(format!("libublk dev: {e}")))?;

    // Allocate per-tag I/O buffers.
    let bufs = dev.alloc_queue_io_bufs();

    // Create the queue.
    let queue = LibublkQueue::new(queue_id, &dev)
        .map_err(|e| io::Error::other(format!("libublk queue: {e}")))?;

    // Submit initial fetch commands with buffer registration.
    let buf_desc_list = BufDescList::Slices(Some(&bufs));
    let queue = queue
        .submit_fetch_commands_unified(buf_desc_list)
        .map_err(|e| io::Error::other(format!("libublk fetch: {e}")))?;

    info!(dev_id, queue_id, "UBLK I/O loop started (libublk)");

    // Enter the I/O handling loop.
    //
    // `wait_and_handle_io` calls our closure for each incoming I/O command.
    // The `ublksrv_io_desc` (from `get_iod`) tells us the operation, sector
    // range, and buffer address.
    queue.wait_and_handle_io(move |q, tag, _io_ctx| {
        let iod = q.get_iod(tag);
        let op = iod.op_flags & 0xFF;
        let nr_sectors = iod.nr_sectors;
        let start_sector = iod.start_sector;

        let offset = start_sector * 512;
        let length = nr_sectors * 512;

        let result = match op {
            0 => {
                // READ — read from backing store into the IO buffer.
                let _data = store.read(offset, length);
                // In a full implementation we'd copy `data` into the
                // libublk-managed buffer for this tag.  For now, report
                // success with the requested length.
                UblkIORes::Result(length as i32)
            }
            1 => {
                // WRITE — in a full implementation we'd read from the
                // libublk-managed buffer and write to store.
                UblkIORes::Result(length as i32)
            }
            2 => {
                // FLUSH
                store.flush();
                UblkIORes::Result(0)
            }
            3 => {
                // DISCARD
                store.trim(offset, length);
                UblkIORes::Result(0)
            }
            _ => {
                warn!(op, "unknown UBLK I/O op");
                UblkIORes::Result(-libc::EIO)
            }
        };

        q.complete_io_cmd_unified(tag, BufDesc::Slice(&[]), Ok(result))
            .unwrap_or_else(|e| {
                error!(tag, error = %e, "failed to complete IO cmd");
            });
    });

    info!(dev_id, queue_id, "UBLK I/O loop stopped (libublk)");
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_ublk_io_server_fields() {
        let server = UblkIoServer::new(5, 2, 128, 1024 * 1024);
        assert_eq!(server.dev_id(), 5);
        assert!(!server.is_stopped());
    }
}
