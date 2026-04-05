use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

// ---------------------------------------------------------------------------
// NBD protocol constants
// ---------------------------------------------------------------------------

/// Newstyle negotiation magic: `NBDMAGIC` (sent first in the handshake).
pub const NBD_INIT_MAGIC: u64 = 0x4e42444d41474943;

/// Option‑haggling magic: `IHAVEOPT` (follows NBDMAGIC).
pub const NBD_OPTS_MAGIC: u64 = 0x49484156454F5054;

/// Transmission‑phase request magic.
pub const NBD_REQUEST_MAGIC: u32 = 0x25609513;

/// Transmission‑phase reply magic.
pub const NBD_REPLY_MAGIC: u32 = 0x67446698;

/// Option‑reply magic used during negotiation.
pub const NBD_OPT_REPLY_MAGIC: u64 = 0x3e889045565a9;

// Negotiation flags (handshake flags the server sends).
pub const NBD_FLAG_FIXED_NEWSTYLE: u16 = 1 << 0;
pub const NBD_FLAG_NO_ZEROES: u16 = 1 << 1;

// Client flags sent back during negotiation.
pub const NBD_FLAG_C_FIXED_NEWSTYLE: u32 = 1 << 0;
pub const NBD_FLAG_C_NO_ZEROES: u32 = 1 << 1;

// Transmission flags (per‑export, sent with the export info).
pub const NBD_FLAG_HAS_FLAGS: u16 = 1 << 0;
pub const NBD_FLAG_SEND_FLUSH: u16 = 1 << 2;
pub const NBD_FLAG_SEND_TRIM: u16 = 1 << 5;

// Option codes used during option haggling.
pub const NBD_OPT_EXPORT_NAME: u32 = 1;
pub const NBD_OPT_ABORT: u32 = 2;
pub const NBD_OPT_LIST: u32 = 3;

// Option reply types.
pub const NBD_REP_ACK: u32 = 1;
pub const NBD_REP_SERVER: u32 = 2;
pub const NBD_REP_ERR_UNSUP: u32 = (1 << 31) | 1;

// NBD command types (in the type field of a request header).
pub const NBD_CMD_READ: u16 = 0;
pub const NBD_CMD_WRITE: u16 = 1;
pub const NBD_CMD_DISC: u16 = 2;
pub const NBD_CMD_FLUSH: u16 = 3;
pub const NBD_CMD_TRIM: u16 = 4;

// NBD error codes returned in the reply header.
pub const NBD_OK: u32 = 0;
pub const NBD_EIO: u32 = 5; // EIO
pub const NBD_EINVAL: u32 = 22; // EINVAL

/// NBD request header size (28 bytes).
pub const NBD_REQUEST_HEADER_SIZE: usize = 28;

/// NBD reply header size (16 bytes).
pub const NBD_REPLY_HEADER_SIZE: usize = 16;

/// Default block size for the in‑memory store.
pub const DEFAULT_BLOCK_SIZE: u64 = 4096;

/// The export name we serve.
pub const EXPORT_NAME: &str = "blockyard";

// ---------------------------------------------------------------------------
// In-memory block storage
// ---------------------------------------------------------------------------

/// Simple block store backed by a `HashMap<u64, Vec<u8>>` keyed by
/// block‑aligned offset.  Reads of unwritten regions return zeroes.
#[derive(Debug, Clone)]
pub struct MemBlockStore {
    blocks: Arc<Mutex<HashMap<u64, Vec<u8>>>>,
    block_size: u64,
    volume_size: u64,
}

impl MemBlockStore {
    pub fn new(volume_size: u64, block_size: u64) -> Self {
        Self {
            blocks: Arc::new(Mutex::new(HashMap::new())),
            block_size,
            volume_size,
        }
    }

    /// Read `length` bytes starting at `offset`.  Handles cross‑block reads.
    pub fn read(&self, offset: u64, length: u32) -> Vec<u8> {
        let mut result = vec![0u8; length as usize];
        let mut remaining = length as u64;
        let mut pos = offset;
        let mut buf_off: usize = 0;

        let blocks = self.blocks.lock();
        while remaining > 0 {
            let block_start = (pos / self.block_size) * self.block_size;
            let offset_in_block = (pos - block_start) as usize;
            let can_read =
                std::cmp::min(remaining, self.block_size - offset_in_block as u64) as usize;

            if let Some(block) = blocks.get(&block_start) {
                let end = std::cmp::min(offset_in_block + can_read, block.len());
                let src = &block[offset_in_block..end];
                result[buf_off..buf_off + src.len()].copy_from_slice(src);
            }
            // else: stays zero

            buf_off += can_read;
            pos += can_read as u64;
            remaining -= can_read as u64;
        }
        result
    }

    /// Write `data` starting at `offset`.  Handles cross‑block writes.
    pub fn write(&self, offset: u64, data: &[u8]) {
        let mut remaining = data.len();
        let mut pos = offset;
        let mut data_off: usize = 0;

        let mut blocks = self.blocks.lock();
        while remaining > 0 {
            let block_start = (pos / self.block_size) * self.block_size;
            let offset_in_block = (pos - block_start) as usize;
            let can_write =
                std::cmp::min(remaining as u64, self.block_size - offset_in_block as u64) as usize;

            let block = blocks
                .entry(block_start)
                .or_insert_with(|| vec![0u8; self.block_size as usize]);

            block[offset_in_block..offset_in_block + can_write]
                .copy_from_slice(&data[data_off..data_off + can_write]);

            data_off += can_write;
            pos += can_write as u64;
            remaining -= can_write;
        }
    }

    /// Trim (discard) blocks that are fully covered by the range.
    pub fn trim(&self, offset: u64, length: u32) {
        let mut blocks = self.blocks.lock();
        let end = offset + length as u64;
        let mut pos = offset;
        while pos < end {
            let block_start = (pos / self.block_size) * self.block_size;
            if block_start >= offset && block_start + self.block_size <= end {
                blocks.remove(&block_start);
            }
            pos = block_start + self.block_size;
        }
    }

    pub fn flush(&self) {
        // No-op for in-memory store.
    }

    pub fn volume_size(&self) -> u64 {
        self.volume_size
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }
}

// ---------------------------------------------------------------------------
// NbdServer
// ---------------------------------------------------------------------------

/// A real NBD server that:
/// 1. Listens on a TCP port on localhost
/// 2. Speaks the NBD newstyle negotiation protocol
/// 3. Handles READ / WRITE / DISC / FLUSH / TRIM in the transmission phase
/// 4. Backs data with an in-memory `MemBlockStore`
///
/// After `start()` returns, run `nbd-client` to attach `/dev/nbdN`.
pub struct NbdServer {
    listen_port: u16,
    device_path: String,
    volume_size: u64,
    store: MemBlockStore,
    server_handle: Mutex<Option<JoinHandle<()>>>,
    /// The actual port the server bound to (filled after start).
    actual_port: Mutex<Option<u16>>,
}

impl NbdServer {
    /// Create a new `NbdServer`.
    ///
    /// - `device_id` — which `/dev/nbdN` device to use
    /// - `volume_size` — size of the virtual block device in bytes
    pub fn new(device_id: u32, volume_size: u64) -> Self {
        Self {
            listen_port: 0, // auto-assign
            device_path: format!("/dev/nbd{device_id}"),
            volume_size,
            store: MemBlockStore::new(volume_size, DEFAULT_BLOCK_SIZE),
            server_handle: Mutex::new(None),
            actual_port: Mutex::new(None),
        }
    }

    pub fn device_path(&self) -> &str {
        &self.device_path
    }

    pub fn volume_size(&self) -> u64 {
        self.volume_size
    }

    pub fn listen_port(&self) -> Option<u16> {
        *self.actual_port.lock()
    }

    pub fn store(&self) -> &MemBlockStore {
        &self.store
    }

    /// Start the TCP listener and spawn the accept loop.
    /// Returns the device path (e.g. `/dev/nbd0`).
    ///
    /// **Does NOT run `nbd-client`** — the caller is responsible for
    /// attaching the kernel NBD device to us (see `UblkClient::mount`).
    pub async fn start(&self) -> blockyard_common::Result<String> {
        let addr = format!("127.0.0.1:{}", self.listen_port);
        let listener = TcpListener::bind(&addr).await?;
        let bound = listener.local_addr()?;
        info!(addr = %bound, device = %self.device_path, "NBD server listening");

        *self.actual_port.lock() = Some(bound.port());

        let store = self.store.clone();
        let volume_size = self.volume_size;
        let device_path = self.device_path.clone();

        let handle = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        info!(peer = %peer, device = %device_path, "NBD client connected");
                        let store = store.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_nbd_connection(stream, store, volume_size).await
                            {
                                warn!(peer = %peer, error = %e, "NBD connection ended");
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "NBD accept failed");
                    }
                }
            }
        });

        *self.server_handle.lock() = Some(handle);
        Ok(self.device_path.clone())
    }

    /// Disconnect the NBD device by running `nbd-client -d /dev/nbdN`,
    /// then abort the server task.
    pub async fn stop(&self) -> blockyard_common::Result<()> {
        info!(device = %self.device_path, "stopping NBD server");

        let output = tokio::process::Command::new("nbd-client")
            .arg("-d")
            .arg(&self.device_path)
            .output()
            .await;

        match output {
            Ok(o) if o.status.success() => {
                info!(device = %self.device_path, "nbd-client disconnected");
            }
            Ok(o) => {
                let stderr = String::from_utf8_lossy(&o.stderr);
                warn!(device = %self.device_path, error = %stderr, "nbd-client disconnect failed");
            }
            Err(e) => {
                warn!(device = %self.device_path, error = %e, "nbd-client not available");
            }
        }

        if let Some(handle) = self.server_handle.lock().take() {
            handle.abort();
        }

        *self.actual_port.lock() = None;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// NBD protocol handling
// ---------------------------------------------------------------------------

/// Handle a single NBD connection: newstyle negotiation → transmission loop.
async fn handle_nbd_connection(
    mut stream: tokio::net::TcpStream,
    store: MemBlockStore,
    volume_size: u64,
) -> blockyard_common::Result<()> {
    stream.set_nodelay(true)?;

    // ── Phase 1: Newstyle negotiation ────────────────────────────────
    negotiate_newstyle(&mut stream, volume_size).await?;

    // ── Phase 2: Transmission ────────────────────────────────────────
    transmission_loop(&mut stream, &store).await?;

    Ok(())
}

/// Perform the fixed newstyle negotiation.
///
/// Server sends:
///   8B  NBDMAGIC
///   8B  IHAVEOPT
///   2B  handshake flags
///
/// Client replies:
///   4B  client flags
///
/// Then we enter option haggling until the client sends `NBD_OPT_EXPORT_NAME`.
async fn negotiate_newstyle(
    stream: &mut tokio::net::TcpStream,
    volume_size: u64,
) -> blockyard_common::Result<()> {
    // Send server hello.
    stream.write_u64(NBD_INIT_MAGIC).await?;
    stream.write_u64(NBD_OPTS_MAGIC).await?;
    let server_flags = NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES;
    stream.write_u16(server_flags).await?;
    stream.flush().await?;
    debug!("sent server hello");

    // Read client flags.
    let client_flags = stream.read_u32().await?;
    debug!(client_flags, "received client flags");

    let no_zeroes = (client_flags & NBD_FLAG_C_NO_ZEROES) != 0;

    // Option haggling loop.
    loop {
        let opt_magic = stream.read_u64().await?;
        if opt_magic != NBD_OPTS_MAGIC {
            return Err(blockyard_common::Error::Protocol(format!(
                "bad option magic: {opt_magic:#x}"
            )));
        }

        let opt_id = stream.read_u32().await?;
        let opt_len = stream.read_u32().await?;

        // Read option data (if any).
        let mut opt_data = vec![0u8; opt_len as usize];
        if opt_len > 0 {
            stream.read_exact(&mut opt_data).await?;
        }

        match opt_id {
            NBD_OPT_EXPORT_NAME => {
                debug!(export = %String::from_utf8_lossy(&opt_data), "OPT_EXPORT_NAME");

                // Reply: export size (8B) + transmission flags (2B) + 124 zero bytes (unless
                // the client negotiated NO_ZEROES).
                stream.write_u64(volume_size).await?;
                let trans_flags = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_TRIM;
                stream.write_u16(trans_flags).await?;

                if !no_zeroes {
                    let zeroes = [0u8; 124];
                    stream.write_all(&zeroes).await?;
                }
                stream.flush().await?;
                debug!("negotiation complete, entering transmission phase");
                return Ok(());
            }
            NBD_OPT_ABORT => {
                debug!("client sent OPT_ABORT");
                // Send ACK then close.
                send_opt_reply(stream, opt_id, NBD_REP_ACK, &[]).await?;
                return Err(blockyard_common::Error::Protocol(
                    "client aborted negotiation".to_string(),
                ));
            }
            NBD_OPT_LIST => {
                debug!("OPT_LIST: sending export name");
                // Send one NBD_REP_SERVER entry, then NBD_REP_ACK.
                let name_bytes = EXPORT_NAME.as_bytes();
                let mut payload = Vec::with_capacity(4 + name_bytes.len());
                payload.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
                payload.extend_from_slice(name_bytes);
                send_opt_reply(stream, opt_id, NBD_REP_SERVER, &payload).await?;
                send_opt_reply(stream, opt_id, NBD_REP_ACK, &[]).await?;
            }
            _ => {
                debug!(opt_id, "unsupported option, sending ERR_UNSUP");
                send_opt_reply(stream, opt_id, NBD_REP_ERR_UNSUP, &[]).await?;
            }
        }
    }
}

/// Send an option reply during negotiation.
async fn send_opt_reply(
    stream: &mut tokio::net::TcpStream,
    opt_id: u32,
    reply_type: u32,
    data: &[u8],
) -> blockyard_common::Result<()> {
    stream.write_u64(NBD_OPT_REPLY_MAGIC).await?;
    stream.write_u32(opt_id).await?;
    stream.write_u32(reply_type).await?;
    stream.write_u32(data.len() as u32).await?;
    if !data.is_empty() {
        stream.write_all(data).await?;
    }
    stream.flush().await?;
    Ok(())
}

/// Read NBD requests and send replies until disconnect.
async fn transmission_loop(
    stream: &mut tokio::net::TcpStream,
    store: &MemBlockStore,
) -> blockyard_common::Result<()> {
    loop {
        // Read 28-byte request header.
        let magic = stream.read_u32().await?;
        if magic != NBD_REQUEST_MAGIC {
            return Err(blockyard_common::Error::Protocol(format!(
                "bad request magic: {magic:#x}"
            )));
        }

        let _cmd_flags = stream.read_u16().await?;
        let cmd_type = stream.read_u16().await?;
        let handle = stream.read_u64().await?;
        let offset = stream.read_u64().await?;
        let length = stream.read_u32().await?;

        match cmd_type {
            NBD_CMD_READ => {
                debug!(handle, offset, length, "NBD READ");
                let data = store.read(offset, length);
                send_reply(stream, handle, NBD_OK, &data).await?;
            }
            NBD_CMD_WRITE => {
                debug!(handle, offset, length, "NBD WRITE");
                let mut data = vec![0u8; length as usize];
                stream.read_exact(&mut data).await?;
                store.write(offset, &data);
                send_reply(stream, handle, NBD_OK, &[]).await?;
            }
            NBD_CMD_DISC => {
                debug!("NBD DISC — disconnecting");
                // No reply for DISC per the spec.
                return Ok(());
            }
            NBD_CMD_FLUSH => {
                debug!(handle, "NBD FLUSH");
                store.flush();
                send_reply(stream, handle, NBD_OK, &[]).await?;
            }
            NBD_CMD_TRIM => {
                debug!(handle, offset, length, "NBD TRIM");
                store.trim(offset, length);
                send_reply(stream, handle, NBD_OK, &[]).await?;
            }
            _ => {
                warn!(cmd_type, "unknown NBD command");
                send_reply(stream, handle, NBD_EINVAL, &[]).await?;
            }
        }
    }
}

/// Send a 16-byte reply header + optional data payload.
async fn send_reply(
    stream: &mut tokio::net::TcpStream,
    handle: u64,
    error: u32,
    data: &[u8],
) -> blockyard_common::Result<()> {
    stream.write_u32(NBD_REPLY_MAGIC).await?;
    stream.write_u32(error).await?;
    stream.write_u64(handle).await?;
    if !data.is_empty() {
        stream.write_all(data).await?;
    }
    stream.flush().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Legacy compat — keep the old NbdFallback so existing code still compiles.
// ---------------------------------------------------------------------------

pub struct NbdFallback;

impl NbdFallback {
    pub fn new() -> Self {
        Self
    }
}

impl Default for NbdFallback {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── Keep existing tests ──────────────────────────────────────────

    #[test]
    fn test_nbd_fallback_new() {
        let _nbd = NbdFallback::new();
    }

    #[test]
    fn test_nbd_fallback_default() {
        let _nbd = NbdFallback::default();
    }

    // ── Protocol constants ───────────────────────────────────────────

    #[test]
    fn test_nbd_magic_constants() {
        assert_eq!(NBD_INIT_MAGIC, 0x4e42444d41474943);
        assert_eq!(NBD_OPTS_MAGIC, 0x49484156454F5054);
        assert_eq!(NBD_REQUEST_MAGIC, 0x25609513);
        assert_eq!(NBD_REPLY_MAGIC, 0x67446698);
    }

    #[test]
    fn test_nbd_command_constants() {
        assert_eq!(NBD_CMD_READ, 0);
        assert_eq!(NBD_CMD_WRITE, 1);
        assert_eq!(NBD_CMD_DISC, 2);
        assert_eq!(NBD_CMD_FLUSH, 3);
        assert_eq!(NBD_CMD_TRIM, 4);
    }

    #[test]
    fn test_nbd_flag_constants() {
        assert_eq!(NBD_FLAG_FIXED_NEWSTYLE, 1);
        assert_eq!(NBD_FLAG_NO_ZEROES, 2);
        assert_eq!(NBD_FLAG_HAS_FLAGS, 1);
        assert_eq!(NBD_FLAG_SEND_FLUSH, 4);
        assert_eq!(NBD_FLAG_SEND_TRIM, 32);
    }

    #[test]
    fn test_nbd_header_sizes() {
        assert_eq!(NBD_REQUEST_HEADER_SIZE, 28);
        assert_eq!(NBD_REPLY_HEADER_SIZE, 16);
    }

    #[test]
    fn test_nbd_error_constants() {
        assert_eq!(NBD_OK, 0);
        assert_eq!(NBD_EIO, 5);
        assert_eq!(NBD_EINVAL, 22);
    }

    #[test]
    fn test_nbd_option_constants() {
        assert_eq!(NBD_OPT_EXPORT_NAME, 1);
        assert_eq!(NBD_OPT_ABORT, 2);
        assert_eq!(NBD_OPT_LIST, 3);
    }

    #[test]
    fn test_nbd_reply_constants() {
        assert_eq!(NBD_REP_ACK, 1);
        assert_eq!(NBD_REP_SERVER, 2);
        // ERR_UNSUP has the error bit set
        assert!(NBD_REP_ERR_UNSUP & (1 << 31) != 0);
    }

    #[test]
    fn test_default_block_size() {
        assert_eq!(DEFAULT_BLOCK_SIZE, 4096);
    }

    // ── MemBlockStore ────────────────────────────────────────────────

    #[test]
    fn test_mem_block_store_new() {
        let store = MemBlockStore::new(1024 * 1024, 4096);
        assert_eq!(store.volume_size(), 1024 * 1024);
        assert_eq!(store.block_size(), 4096);
    }

    #[test]
    fn test_mem_block_store_read_zeroes() {
        let store = MemBlockStore::new(1024 * 1024, 4096);
        let data = store.read(0, 512);
        assert_eq!(data.len(), 512);
        assert!(data.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_mem_block_store_write_read() {
        let store = MemBlockStore::new(1024 * 1024, 4096);
        let payload = vec![0xAB; 256];
        store.write(0, &payload);
        let data = store.read(0, 256);
        assert_eq!(data, payload);
    }

    #[test]
    fn test_mem_block_store_write_read_with_offset() {
        let store = MemBlockStore::new(1024 * 1024, 4096);
        let payload = vec![0xCD; 128];
        store.write(512, &payload);

        // Before the written region should be zero.
        let before = store.read(0, 512);
        assert!(before.iter().all(|&b| b == 0));

        // The written region should match.
        let data = store.read(512, 128);
        assert_eq!(data, payload);
    }

    #[test]
    fn test_mem_block_store_cross_block_write() {
        let store = MemBlockStore::new(1024 * 1024, 4096);
        // Write across a block boundary.
        let payload = vec![0xEF; 8192];
        store.write(2048, &payload);

        let data = store.read(2048, 8192);
        assert_eq!(data, payload);
    }

    #[test]
    fn test_mem_block_store_trim() {
        let store = MemBlockStore::new(1024 * 1024, 4096);
        store.write(0, &vec![0xFF; 4096]);
        store.trim(0, 4096);
        let data = store.read(0, 4096);
        assert!(data.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_mem_block_store_flush_is_noop() {
        let store = MemBlockStore::new(1024 * 1024, 4096);
        store.flush(); // Should not panic.
    }

    // ── NbdServer construction ───────────────────────────────────────

    #[test]
    fn test_nbd_server_new() {
        let server = NbdServer::new(0, 1024 * 1024 * 1024);
        assert_eq!(server.device_path(), "/dev/nbd0");
        assert_eq!(server.volume_size(), 1024 * 1024 * 1024);
        assert!(server.listen_port().is_none());
    }

    #[test]
    fn test_nbd_server_new_custom_device() {
        let server = NbdServer::new(5, 512 * 1024);
        assert_eq!(server.device_path(), "/dev/nbd5");
        assert_eq!(server.volume_size(), 512 * 1024);
    }

    // ── NbdServer start (TCP listener) ───────────────────────────────

    #[tokio::test]
    async fn test_nbd_server_start_binds_port() {
        let server = NbdServer::new(99, 1024 * 1024);
        let dev = server.start().await.unwrap();
        assert_eq!(dev, "/dev/nbd99");

        let port = server.listen_port().unwrap();
        assert!(port > 0, "should have bound to a real port");

        // Clean up — abort the background task.
        if let Some(handle) = server.server_handle.lock().take() {
            handle.abort();
        }
    }

    // ── NBD protocol negotiation + transmission (pure TCP, no kernel) ─

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_nbd_protocol_negotiation_and_readwrite() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let server = NbdServer::new(99, 64 * 1024);
            server.start().await.unwrap();
            let port = server.listen_port().unwrap();

            let mut client = TcpStream::connect(format!("127.0.0.1:{port}"))
                .await
                .unwrap();
            client.set_nodelay(true).unwrap();

            // ── Read server hello ────────────────────────────────────
            let init_magic = client.read_u64().await.unwrap();
            assert_eq!(init_magic, NBD_INIT_MAGIC);

            let opts_magic = client.read_u64().await.unwrap();
            assert_eq!(opts_magic, NBD_OPTS_MAGIC);

            let server_flags = client.read_u16().await.unwrap();
            assert!(server_flags & NBD_FLAG_FIXED_NEWSTYLE != 0);

            // ── Send client flags (with NO_ZEROES) ───────────────────
            let cflags = NBD_FLAG_C_FIXED_NEWSTYLE | NBD_FLAG_C_NO_ZEROES;
            client.write_u32(cflags).await.unwrap();

            // ── Send OPT_EXPORT_NAME ─────────────────────────────────
            let export = EXPORT_NAME.as_bytes();
            client.write_u64(NBD_OPTS_MAGIC).await.unwrap();
            client.write_u32(NBD_OPT_EXPORT_NAME).await.unwrap();
            client.write_u32(export.len() as u32).await.unwrap();
            client.write_all(export).await.unwrap();
            client.flush().await.unwrap();

            // ── Read export info ─────────────────────────────────────
            let export_size = client.read_u64().await.unwrap();
            assert_eq!(export_size, 64 * 1024);

            let trans_flags = client.read_u16().await.unwrap();
            assert!(trans_flags & NBD_FLAG_HAS_FLAGS != 0);
            // No 124 zero bytes because we sent NO_ZEROES.

            // ── WRITE 256 bytes at offset 0 ──────────────────────────
            let handle: u64 = 1;
            let write_data = vec![0xAB; 256];
            client.write_u32(NBD_REQUEST_MAGIC).await.unwrap();
            client.write_u16(0).await.unwrap(); // command flags
            client.write_u16(NBD_CMD_WRITE).await.unwrap();
            client.write_u64(handle).await.unwrap();
            client.write_u64(0).await.unwrap(); // offset
            client.write_u32(256).await.unwrap(); // length
            client.write_all(&write_data).await.unwrap();
            client.flush().await.unwrap();

            // Read reply.
            let reply_magic = client.read_u32().await.unwrap();
            assert_eq!(reply_magic, NBD_REPLY_MAGIC);
            let error = client.read_u32().await.unwrap();
            assert_eq!(error, NBD_OK);
            let reply_handle = client.read_u64().await.unwrap();
            assert_eq!(reply_handle, handle);

            // ── READ 256 bytes at offset 0 ───────────────────────────
            let handle: u64 = 2;
            client.write_u32(NBD_REQUEST_MAGIC).await.unwrap();
            client.write_u16(0).await.unwrap(); // command flags
            client.write_u16(NBD_CMD_READ).await.unwrap();
            client.write_u64(handle).await.unwrap();
            client.write_u64(0).await.unwrap();
            client.write_u32(256).await.unwrap();
            client.flush().await.unwrap();

            let reply_magic = client.read_u32().await.unwrap();
            assert_eq!(reply_magic, NBD_REPLY_MAGIC);
            let error = client.read_u32().await.unwrap();
            assert_eq!(error, NBD_OK);
            let reply_handle = client.read_u64().await.unwrap();
            assert_eq!(reply_handle, handle);

            let mut read_back = vec![0u8; 256];
            client.read_exact(&mut read_back).await.unwrap();
            assert_eq!(read_back, write_data);

            // ── FLUSH ────────────────────────────────────────────────
            let handle: u64 = 3;
            client.write_u32(NBD_REQUEST_MAGIC).await.unwrap();
            client.write_u16(0).await.unwrap(); // command flags
            client.write_u16(NBD_CMD_FLUSH).await.unwrap();
            client.write_u64(handle).await.unwrap();
            client.write_u64(0).await.unwrap();
            client.write_u32(0).await.unwrap();
            client.flush().await.unwrap();

            let reply_magic = client.read_u32().await.unwrap();
            assert_eq!(reply_magic, NBD_REPLY_MAGIC);
            let error = client.read_u32().await.unwrap();
            assert_eq!(error, NBD_OK);
            let reply_handle = client.read_u64().await.unwrap();
            assert_eq!(reply_handle, handle);

            // ── TRIM ─────────────────────────────────────────────────
            let handle: u64 = 4;
            client.write_u32(NBD_REQUEST_MAGIC).await.unwrap();
            client.write_u16(0).await.unwrap(); // command flags
            client.write_u16(NBD_CMD_TRIM).await.unwrap();
            client.write_u64(handle).await.unwrap();
            client.write_u64(0).await.unwrap();
            client.write_u32(4096).await.unwrap();
            client.flush().await.unwrap();

            let reply_magic = client.read_u32().await.unwrap();
            assert_eq!(reply_magic, NBD_REPLY_MAGIC);
            let error = client.read_u32().await.unwrap();
            assert_eq!(error, NBD_OK);
            let reply_handle = client.read_u64().await.unwrap();
            assert_eq!(reply_handle, handle);

            // ── DISC (disconnect) ────────────────────────────────────
            client.write_u32(NBD_REQUEST_MAGIC).await.unwrap();
            client.write_u16(0).await.unwrap(); // command flags
            client.write_u16(NBD_CMD_DISC).await.unwrap();
            client.write_u64(5).await.unwrap();
            client.write_u64(0).await.unwrap();
            client.write_u32(0).await.unwrap();
            client.flush().await.unwrap();

            // Server should close the connection; a subsequent read should EOF.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            // Clean up.
            if let Some(h) = server.server_handle.lock().take() {
                h.abort();
            }
        })
        .await
        .expect("test timed out");
    }

    // ── Integration test: full NBD attach via nbd-client ─────────────
    // Requires root, the `nbd` kernel module loaded, and `nbd-client` installed.

    #[tokio::test]
    #[ignore]
    async fn test_nbd_server_integration_with_nbd_client() {
        let server = NbdServer::new(15, 16 * 1024 * 1024);
        let _dev = server.start().await.unwrap();
        let port = server.listen_port().unwrap();

        // Attach the NBD device.
        let status = tokio::process::Command::new("nbd-client")
            .args([
                "-N",
                EXPORT_NAME,
                "localhost",
                &port.to_string(),
                "/dev/nbd15",
            ])
            .status()
            .await
            .expect("failed to run nbd-client");
        assert!(status.success(), "nbd-client failed to connect");

        // Write via /dev/nbd15.
        let payload = vec![0x42u8; 4096];
        tokio::fs::write("/dev/nbd15", &payload).await.unwrap();

        // Read back.
        use tokio::fs::File;
        use tokio::io::AsyncReadExt as _;
        let mut f = File::open("/dev/nbd15").await.unwrap();
        let mut buf = vec![0u8; 4096];
        f.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, payload);

        // Disconnect.
        server.stop().await.unwrap();
    }
}
