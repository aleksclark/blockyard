use crate::harness::cluster::TestCluster;
use blockyard_protocol::wire::{OpType, RESPONSE_HEADER_SIZE, Request, Response};
use bytes::{Bytes, BytesMut};
use rand::Rng;
use rand::SeedableRng;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
pub struct WriteRecord {
    pub request_id: u64,
    pub volume_name: String,
    pub offset: u64,
    pub data: Vec<u8>,
    pub acknowledged: bool,
    pub timestamp: Duration,
}

#[derive(Debug, Clone)]
pub struct ReadRecord {
    pub request_id: u64,
    pub volume_name: String,
    pub offset: u64,
    pub data: Vec<u8>,
    pub success: bool,
    pub timestamp: Duration,
    pub latency: Duration,
}

#[derive(Debug)]
pub struct WorkloadLog {
    pub writes: Vec<WriteRecord>,
    pub reads: Vec<ReadRecord>,
    pub errors: Vec<String>,
    pub start_time: Instant,
}

impl WorkloadLog {
    pub fn new() -> Self {
        Self {
            writes: Vec::new(),
            reads: Vec::new(),
            errors: Vec::new(),
            start_time: Instant::now(),
        }
    }

    pub fn acknowledged_writes(&self) -> Vec<&WriteRecord> {
        self.writes.iter().filter(|w| w.acknowledged).collect()
    }

    pub fn failed_reads(&self) -> Vec<&ReadRecord> {
        self.reads.iter().filter(|r| !r.success).collect()
    }

    pub fn write_count(&self) -> usize {
        self.writes.len()
    }

    pub fn read_count(&self) -> usize {
        self.reads.len()
    }

    pub fn error_count(&self) -> usize {
        self.errors.len()
    }

    pub fn read_p99_latency(&self) -> Duration {
        if self.reads.is_empty() {
            return Duration::ZERO;
        }
        let mut latencies: Vec<Duration> = self.reads.iter().map(|r| r.latency).collect();
        latencies.sort();
        let idx = (latencies.len() as f64 * 0.99) as usize;
        latencies[idx.min(latencies.len() - 1)]
    }

    pub fn write_p99_latency(&self) -> Duration {
        let acked: Vec<Duration> = self
            .writes
            .iter()
            .filter(|w| w.acknowledged)
            .map(|w| w.timestamp)
            .collect();
        if acked.is_empty() {
            return Duration::ZERO;
        }
        let mut sorted = acked;
        sorted.sort();
        let idx = (sorted.len() as f64 * 0.99) as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    /// Returns the latest data written to a given offset (by timestamp), if any.
    pub fn latest_write_at(&self, offset: u64) -> Option<&WriteRecord> {
        self.writes
            .iter()
            .filter(|w| w.offset == offset && w.acknowledged)
            .max_by_key(|w| w.timestamp)
    }

    /// Returns all offsets that have at least one acknowledged write.
    pub fn written_offsets(&self) -> Vec<u64> {
        let mut offsets: Vec<u64> = self
            .writes
            .iter()
            .filter(|w| w.acknowledged)
            .map(|w| w.offset)
            .collect();
        offsets.sort();
        offsets.dedup();
        offsets
    }
}

impl Default for WorkloadLog {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct WorkloadConfig {
    pub volume_name: String,
    pub volume_id: u64,
    pub duration: Duration,
    pub write_rate: u64,
    pub read_rate: u64,
    pub block_size: usize,
    pub max_offset: u64,
    /// Target addresses to connect to. The generator will try each in order and
    /// fall back to the next on connection failure.
    pub target_addrs: Vec<SocketAddr>,
    /// Per-operation timeout for sends and receives.
    pub op_timeout: Duration,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            volume_name: "test-vol".into(),
            volume_id: 1,
            duration: Duration::from_secs(30),
            write_rate: 100,
            read_rate: 200,
            block_size: 4096,
            max_offset: 1024 * 1024 * 1024,
            target_addrs: Vec::new(),
            op_timeout: Duration::from_secs(5),
        }
    }
}

pub struct WorkloadGenerator {
    config: WorkloadConfig,
    log: Arc<Mutex<WorkloadLog>>,
    running: Arc<AtomicBool>,
    next_id: Arc<AtomicU64>,
}

impl WorkloadGenerator {
    pub fn new(config: WorkloadConfig) -> Self {
        Self {
            config,
            log: Arc::new(Mutex::new(WorkloadLog::new())),
            running: Arc::new(AtomicBool::new(false)),
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    pub async fn log(&self) -> WorkloadLog {
        let guard = self.log.lock().await;
        WorkloadLog {
            writes: guard.writes.clone(),
            reads: guard.reads.clone(),
            errors: guard.errors.clone(),
            start_time: guard.start_time,
        }
    }

    pub async fn record_write(&self, offset: u64, data: Vec<u8>, acknowledged: bool) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut log = self.log.lock().await;
        let ts = log.start_time.elapsed();
        log.writes.push(WriteRecord {
            request_id: id,
            volume_name: self.config.volume_name.clone(),
            offset,
            data,
            acknowledged,
            timestamp: ts,
        });
        id
    }

    pub async fn record_read(
        &self,
        offset: u64,
        data: Vec<u8>,
        success: bool,
        latency: Duration,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut log = self.log.lock().await;
        let ts = log.start_time.elapsed();
        log.reads.push(ReadRecord {
            request_id: id,
            volume_name: self.config.volume_name.clone(),
            offset,
            data,
            success,
            timestamp: ts,
            latency,
        });
        id
    }

    pub async fn record_error(&self, msg: String) {
        self.log.lock().await.errors.push(msg);
    }

    /// Start the I/O workload loop as a background tokio task.
    ///
    /// The loop connects to a cluster node via TCP, sends `Request` messages
    /// using the binary wire protocol, reads `Response` messages back, and
    /// records every operation in the log. The task runs until [`stop`] is
    /// called or the configured duration elapses.
    ///
    /// Returns a `JoinHandle` that resolves when the loop finishes.
    pub fn start(&self) -> JoinHandle<()> {
        self.running.store(true, Ordering::SeqCst);

        let config = self.config.clone();
        let log = self.log.clone();
        let running = self.running.clone();
        let next_id = self.next_id.clone();

        tokio::spawn(async move {
            let start_time = {
                let g = log.lock().await;
                g.start_time
            };

            // Track offsets we've written to so we can read them back.
            let mut written_offsets: Vec<(u64, Vec<u8>)> = Vec::new();

            // Compute inter-operation delay from combined rate.
            let combined_rate = config.write_rate + config.read_rate;
            let op_delay = if combined_rate > 0 {
                Duration::from_secs_f64(1.0 / combined_rate as f64)
            } else {
                Duration::from_millis(10)
            };

            let write_ratio = if combined_rate > 0 {
                config.write_rate as f64 / combined_rate as f64
            } else {
                0.5
            };

            // Use a Send-able rng (not ThreadRng which is !Send).
            let mut rng = rand::rngs::StdRng::from_os_rng();

            // Connection state — we attempt to maintain a single TCP stream.
            let mut stream: Option<TcpStream> = None;
            let mut recv_buf = BytesMut::with_capacity(64 * 1024);
            let mut addr_idx: usize = 0;

            loop {
                // Check termination conditions.
                if !running.load(Ordering::SeqCst) {
                    break;
                }
                if start_time.elapsed() >= config.duration {
                    running.store(false, Ordering::SeqCst);
                    break;
                }

                // Ensure we have a connection.
                if stream.is_none() && !config.target_addrs.is_empty() {
                    let attempts = config.target_addrs.len();
                    for _ in 0..attempts {
                        let addr = config.target_addrs[addr_idx % config.target_addrs.len()];
                        addr_idx += 1;
                        match tokio::time::timeout(config.op_timeout, TcpStream::connect(addr))
                            .await
                        {
                            Ok(Ok(s)) => {
                                let _ = s.set_nodelay(true);
                                stream = Some(s);
                                recv_buf.clear();
                                break;
                            }
                            Ok(Err(e)) => {
                                let msg = format!("connect to {addr} failed: {e}");
                                log.lock().await.errors.push(msg);
                            }
                            Err(_) => {
                                let msg = format!("connect to {addr} timed out");
                                log.lock().await.errors.push(msg);
                            }
                        }
                    }
                    // If still no connection after trying all addrs, sleep and retry.
                    if stream.is_none() {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }

                // If no target addrs configured, we can't do real I/O. Just
                // sleep and let the caller stop us.
                if config.target_addrs.is_empty() {
                    tokio::time::sleep(op_delay).await;
                    continue;
                }

                let request_id = next_id.fetch_add(1, Ordering::Relaxed);

                // Decide write vs read.
                let do_write = written_offsets.is_empty() || rng.random_bool(write_ratio);

                let op_start = Instant::now();

                if do_write {
                    // Generate random block-aligned offset and random data.
                    let max_blocks = config.max_offset / config.block_size as u64;
                    let block_num = if max_blocks > 0 {
                        rng.random_range(0..max_blocks)
                    } else {
                        0
                    };
                    let offset = block_num * config.block_size as u64;
                    let data: Vec<u8> = (0..config.block_size).map(|_| rng.random()).collect();

                    let req = Request {
                        request_id,
                        op: OpType::Write,
                        volume_id: config.volume_id,
                        offset,
                        length: config.block_size as u32,
                        data: Bytes::from(data.clone()),
                    };

                    match send_and_recv(
                        stream.as_mut().unwrap(),
                        &req,
                        &mut recv_buf,
                        config.op_timeout,
                    )
                    .await
                    {
                        Ok(resp) => {
                            let acked = resp.status == blockyard_protocol::wire::Status::Ok;
                            let ts = start_time.elapsed();
                            let mut g = log.lock().await;
                            g.writes.push(WriteRecord {
                                request_id,
                                volume_name: config.volume_name.clone(),
                                offset,
                                data: data.clone(),
                                acknowledged: acked,
                                timestamp: ts,
                            });
                            drop(g);
                            if acked {
                                written_offsets.push((offset, data));
                            }
                        }
                        Err(e) => {
                            log.lock()
                                .await
                                .errors
                                .push(format!("write req {request_id} offset {offset}: {e}"));
                            // Drop broken connection so we reconnect.
                            stream = None;
                        }
                    }
                } else {
                    // Read a previously-written offset.
                    let idx = rng.random_range(0..written_offsets.len());
                    let (offset, _expected_data) = &written_offsets[idx];
                    let offset = *offset;

                    let req = Request {
                        request_id,
                        op: OpType::Read,
                        volume_id: config.volume_id,
                        offset,
                        length: config.block_size as u32,
                        data: Bytes::new(),
                    };

                    match send_and_recv(
                        stream.as_mut().unwrap(),
                        &req,
                        &mut recv_buf,
                        config.op_timeout,
                    )
                    .await
                    {
                        Ok(resp) => {
                            let success = resp.status == blockyard_protocol::wire::Status::Ok;
                            let latency = op_start.elapsed();
                            let ts = start_time.elapsed();
                            let mut g = log.lock().await;
                            g.reads.push(ReadRecord {
                                request_id,
                                volume_name: config.volume_name.clone(),
                                offset,
                                data: resp.data.to_vec(),
                                success,
                                timestamp: ts,
                                latency,
                            });
                        }
                        Err(e) => {
                            log.lock()
                                .await
                                .errors
                                .push(format!("read req {request_id} offset {offset}: {e}"));
                            stream = None;
                        }
                    }
                }

                tokio::time::sleep(op_delay).await;
            }
        })
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Create a `WorkloadConfig` populated with the data port addresses from a
    /// `TestCluster`, targeting all running nodes.
    pub fn config_from_cluster(
        cluster: &TestCluster,
        volume_name: &str,
        volume_id: u64,
    ) -> WorkloadConfig {
        let addrs: Vec<SocketAddr> = cluster
            .running_nodes()
            .iter()
            .map(|n| format!("127.0.0.1:{}", n.data_port).parse().unwrap())
            .collect();
        WorkloadConfig {
            volume_name: volume_name.to_string(),
            volume_id,
            target_addrs: addrs,
            ..Default::default()
        }
    }
}

/// Send a request over a TCP stream and read the response, with a timeout.
async fn send_and_recv(
    stream: &mut TcpStream,
    req: &Request,
    recv_buf: &mut BytesMut,
    timeout: Duration,
) -> anyhow::Result<Response> {
    let mut send_buf =
        BytesMut::with_capacity(blockyard_protocol::wire::REQUEST_HEADER_SIZE + req.data.len());
    req.encode(&mut send_buf);

    tokio::time::timeout(timeout, async {
        stream.write_all(&send_buf).await?;
        stream.flush().await?;

        // Read until we can decode a full response.
        loop {
            if let Some(resp) = Response::decode(recv_buf) {
                return Ok(resp);
            }
            let n = stream.read_buf(recv_buf).await?;
            if n == 0 {
                anyhow::bail!("connection closed by server");
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("operation timed out"))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workload_config_default() {
        let config = WorkloadConfig::default();
        assert_eq!(config.volume_name, "test-vol");
        assert_eq!(config.block_size, 4096);
        assert!(config.target_addrs.is_empty());
    }

    #[test]
    fn test_workload_log_new() {
        let log = WorkloadLog::new();
        assert_eq!(log.write_count(), 0);
        assert_eq!(log.read_count(), 0);
        assert_eq!(log.error_count(), 0);
        assert!(log.acknowledged_writes().is_empty());
        assert!(log.failed_reads().is_empty());
    }

    #[test]
    fn test_workload_log_p99_empty() {
        let log = WorkloadLog::new();
        assert_eq!(log.read_p99_latency(), Duration::ZERO);
        assert_eq!(log.write_p99_latency(), Duration::ZERO);
    }

    #[test]
    fn test_workload_log_acknowledged_writes() {
        let mut log = WorkloadLog::new();
        log.writes.push(WriteRecord {
            request_id: 1,
            volume_name: "v".into(),
            offset: 0,
            data: vec![1],
            acknowledged: true,
            timestamp: Duration::from_millis(1),
        });
        log.writes.push(WriteRecord {
            request_id: 2,
            volume_name: "v".into(),
            offset: 4096,
            data: vec![2],
            acknowledged: false,
            timestamp: Duration::from_millis(2),
        });
        assert_eq!(log.acknowledged_writes().len(), 1);
        assert_eq!(log.write_count(), 2);
    }

    #[test]
    fn test_workload_log_failed_reads() {
        let mut log = WorkloadLog::new();
        log.reads.push(ReadRecord {
            request_id: 1,
            volume_name: "v".into(),
            offset: 0,
            data: vec![],
            success: true,
            timestamp: Duration::from_millis(1),
            latency: Duration::from_millis(5),
        });
        log.reads.push(ReadRecord {
            request_id: 2,
            volume_name: "v".into(),
            offset: 4096,
            data: vec![],
            success: false,
            timestamp: Duration::from_millis(2),
            latency: Duration::from_millis(100),
        });
        assert_eq!(log.failed_reads().len(), 1);
        assert_eq!(log.read_count(), 2);
    }

    #[test]
    fn test_workload_log_p99_latency() {
        let mut log = WorkloadLog::new();
        for i in 0..100 {
            log.reads.push(ReadRecord {
                request_id: i,
                volume_name: "v".into(),
                offset: 0,
                data: vec![],
                success: true,
                timestamp: Duration::from_millis(i),
                latency: Duration::from_millis(i + 1),
            });
        }
        let p99 = log.read_p99_latency();
        assert!(p99 >= Duration::from_millis(99));
    }

    #[tokio::test]
    async fn test_workload_generator_record_write() {
        let wg = WorkloadGenerator::new(WorkloadConfig::default());
        let id = wg.record_write(0, vec![1, 2, 3], true).await;
        assert_eq!(id, 1);
        let log = wg.log().await;
        assert_eq!(log.write_count(), 1);
        assert!(log.writes[0].acknowledged);
    }

    #[tokio::test]
    async fn test_workload_generator_record_read() {
        let wg = WorkloadGenerator::new(WorkloadConfig::default());
        let id = wg
            .record_read(0, vec![1, 2], true, Duration::from_millis(5))
            .await;
        assert_eq!(id, 1);
        let log = wg.log().await;
        assert_eq!(log.read_count(), 1);
    }

    #[tokio::test]
    async fn test_workload_generator_record_error() {
        let wg = WorkloadGenerator::new(WorkloadConfig::default());
        wg.record_error("timeout".into()).await;
        let log = wg.log().await;
        assert_eq!(log.error_count(), 1);
    }

    #[tokio::test]
    async fn test_workload_generator_start_stop_no_addrs() {
        // With no target_addrs, the loop just spins on sleep and exits on stop.
        let wg = WorkloadGenerator::new(WorkloadConfig {
            duration: Duration::from_secs(60),
            ..Default::default()
        });
        let handle = wg.start();
        assert!(wg.is_running());
        tokio::time::sleep(Duration::from_millis(50)).await;
        wg.stop();
        // The task should finish promptly.
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("task should finish after stop")
            .expect("task should not panic");
        assert!(!wg.is_running());
    }

    #[tokio::test]
    async fn test_workload_generator_increments_ids() {
        let wg = WorkloadGenerator::new(WorkloadConfig::default());
        let id1 = wg.record_write(0, vec![], true).await;
        let id2 = wg.record_write(0, vec![], true).await;
        let id3 = wg.record_read(0, vec![], true, Duration::ZERO).await;
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
    }

    #[test]
    fn test_workload_log_written_offsets() {
        let mut log = WorkloadLog::new();
        log.writes.push(WriteRecord {
            request_id: 1,
            volume_name: "v".into(),
            offset: 0,
            data: vec![1],
            acknowledged: true,
            timestamp: Duration::from_millis(1),
        });
        log.writes.push(WriteRecord {
            request_id: 2,
            volume_name: "v".into(),
            offset: 4096,
            data: vec![2],
            acknowledged: true,
            timestamp: Duration::from_millis(2),
        });
        log.writes.push(WriteRecord {
            request_id: 3,
            volume_name: "v".into(),
            offset: 0,
            data: vec![3],
            acknowledged: false,
            timestamp: Duration::from_millis(3),
        });
        let offsets = log.written_offsets();
        assert_eq!(offsets, vec![0, 4096]);
    }

    #[test]
    fn test_workload_log_latest_write_at() {
        let mut log = WorkloadLog::new();
        log.writes.push(WriteRecord {
            request_id: 1,
            volume_name: "v".into(),
            offset: 0,
            data: vec![1],
            acknowledged: true,
            timestamp: Duration::from_millis(1),
        });
        log.writes.push(WriteRecord {
            request_id: 2,
            volume_name: "v".into(),
            offset: 0,
            data: vec![2],
            acknowledged: true,
            timestamp: Duration::from_millis(5),
        });
        let latest = log.latest_write_at(0).unwrap();
        assert_eq!(latest.request_id, 2);
        assert_eq!(latest.data, vec![2]);
        assert!(log.latest_write_at(4096).is_none());
    }
}
