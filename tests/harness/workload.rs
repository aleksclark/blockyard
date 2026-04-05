use bytes::{Bytes, BytesMut};
use rand::Rng;
use rand::SeedableRng;
use rand::prelude::IndexedRandom;
use rand::rngs::StdRng;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use blockyard_protocol::wire::{OpType, Request, Response};

#[derive(Debug, Clone)]
pub struct WriteRecord {
    pub request_id: u64,
    pub offset: u64,
    pub data: Vec<u8>,
    pub acknowledged: bool,
    pub timestamp: Duration,
}

#[derive(Debug, Clone)]
pub struct ReadRecord {
    pub request_id: u64,
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

    pub fn written_offsets(&self) -> Vec<u64> {
        self.acknowledged_writes().iter().map(|w| w.offset).collect()
    }
}

impl Default for WorkloadLog {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct WorkloadConfig {
    pub targets: Vec<SocketAddr>,
    pub duration: Duration,
    pub write_interval: Duration,
    pub read_interval: Duration,
    pub block_size: usize,
    pub max_offset: u64,
    pub volume_id: u64,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            targets: vec!["127.0.0.1:7400".parse().unwrap()],
            duration: Duration::from_secs(30),
            write_interval: Duration::from_millis(100),
            read_interval: Duration::from_millis(50),
            block_size: 4096,
            max_offset: 1024 * 1024,
            volume_id: 1,
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

    pub fn start(&self) -> JoinHandle<()> {
        self.running.store(true, Ordering::Relaxed);

        let running = self.running.clone();
        let log = self.log.clone();
        let next_id = self.next_id.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            let mut written: HashMap<u64, Vec<u8>> = HashMap::new();
            let mut rng = StdRng::from_os_rng();
            let start = Instant::now();

            while running.load(Ordering::Relaxed) && start.elapsed() < config.duration {
                let id = next_id.fetch_add(1, Ordering::Relaxed);
                let max_blocks = config.max_offset / config.block_size as u64;
                let offset = rng.random_range(0..max_blocks) * config.block_size as u64;
                let mut data = vec![0u8; config.block_size];
                rng.fill(&mut data[..]);

                let write_start = Instant::now();
                match Self::send_write(&config, id, offset, &data).await {
                    Ok(true) => {
                        let elapsed = start.elapsed();
                        let mut wl = log.lock().await;
                        wl.writes.push(WriteRecord {
                            request_id: id,
                            offset,
                            data: data.clone(),
                            acknowledged: true,
                            timestamp: elapsed,
                        });
                        drop(wl);
                        written.insert(offset, data);
                    }
                    Ok(false) => {
                        let elapsed = start.elapsed();
                        let mut wl = log.lock().await;
                        wl.writes.push(WriteRecord {
                            request_id: id,
                            offset,
                            data,
                            acknowledged: false,
                            timestamp: elapsed,
                        });
                    }
                    Err(e) => {
                        log.lock().await.errors.push(format!("write error: {e}"));
                    }
                }

                if !written.is_empty() && rng.random_range(0u32..2) == 0 {
                    let read_id = next_id.fetch_add(1, Ordering::Relaxed);
                    let offsets: Vec<u64> = written.keys().copied().collect();
                    let read_offset = *offsets.choose(&mut rng).unwrap();

                    let read_start = Instant::now();
                    match Self::send_read(&config, read_id, read_offset, config.block_size as u32)
                        .await
                    {
                        Ok(read_data) => {
                            let latency = read_start.elapsed();
                            let expected = written.get(&read_offset);
                            let success =
                                expected.map_or(true, |exp| exp.as_slice() == read_data.as_ref());
                            let mut wl = log.lock().await;
                            wl.reads.push(ReadRecord {
                                request_id: read_id,
                                offset: read_offset,
                                data: read_data.to_vec(),
                                success,
                                timestamp: start.elapsed(),
                                latency,
                            });
                        }
                        Err(e) => {
                            log.lock().await.errors.push(format!("read error: {e}"));
                        }
                    }
                }

                tokio::time::sleep(config.write_interval).await;
            }

            running.store(false, Ordering::Relaxed);
        })
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    async fn connect(config: &WorkloadConfig) -> anyhow::Result<TcpStream> {
        for target in &config.targets {
            match tokio::time::timeout(Duration::from_millis(200), TcpStream::connect(target)).await {
                Ok(Ok(stream)) => {
                    stream.set_nodelay(true)?;
                    return Ok(stream);
                }
                _ => continue,
            }
        }
        anyhow::bail!("could not connect to any target")
    }

    async fn send_write(
        config: &WorkloadConfig,
        request_id: u64,
        offset: u64,
        data: &[u8],
    ) -> anyhow::Result<bool> {
        tokio::time::timeout(Duration::from_secs(1), async {
            let mut stream = Self::connect(config).await?;

            let req = Request {
                request_id,
                op: OpType::Write,
                volume_id: config.volume_id,
                offset,
                length: data.len() as u32,
                data: Bytes::copy_from_slice(data),
            };
            let mut buf = BytesMut::new();
            req.encode(&mut buf);
            stream.write_all(&buf).await?;
            stream.flush().await?;

        let mut resp_buf = BytesMut::with_capacity(256);
        loop {
            let n = stream.read_buf(&mut resp_buf).await?;
            if n == 0 {
                anyhow::bail!("connection closed");
            }
            if let Some(resp) = Response::decode(&mut resp_buf) {
                return Ok(resp.status == blockyard_protocol::wire::Status::Ok);
            }
        }
        }).await.map_err(|_| anyhow::anyhow!("write timeout"))?
    }

    async fn send_read(
        config: &WorkloadConfig,
        request_id: u64,
        offset: u64,
        length: u32,
    ) -> anyhow::Result<Bytes> {
        tokio::time::timeout(Duration::from_secs(1), async {
            let mut stream = Self::connect(config).await?;

            let req = Request {
                request_id,
                op: OpType::Read,
                volume_id: config.volume_id,
                offset,
                length,
                data: Bytes::new(),
            };
            let mut buf = BytesMut::new();
            req.encode(&mut buf);
            stream.write_all(&buf).await?;
            stream.flush().await?;

            let mut resp_buf = BytesMut::with_capacity(8192);
            loop {
                let n = stream.read_buf(&mut resp_buf).await?;
                if n == 0 {
                    anyhow::bail!("connection closed");
                }
                if let Some(resp) = Response::decode(&mut resp_buf) {
                    return Ok(resp.data);
                }
            }
        }).await.map_err(|_| anyhow::anyhow!("read timeout"))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workload_config_default() {
        let config = WorkloadConfig::default();
        assert_eq!(config.block_size, 4096);
        assert!(!config.targets.is_empty());
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
            offset: 0,
            data: vec![1],
            acknowledged: true,
            timestamp: Duration::from_millis(1),
        });
        log.writes.push(WriteRecord {
            request_id: 2,
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
            offset: 0,
            data: vec![],
            success: true,
            timestamp: Duration::from_millis(1),
            latency: Duration::from_millis(5),
        });
        log.reads.push(ReadRecord {
            request_id: 2,
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

    #[test]
    fn test_workload_log_written_offsets() {
        let mut log = WorkloadLog::new();
        log.writes.push(WriteRecord {
            request_id: 1,
            offset: 0,
            data: vec![1],
            acknowledged: true,
            timestamp: Duration::from_millis(1),
        });
        log.writes.push(WriteRecord {
            request_id: 2,
            offset: 4096,
            data: vec![2],
            acknowledged: false,
            timestamp: Duration::from_millis(2),
        });
        let offsets = log.written_offsets();
        assert_eq!(offsets, vec![0]);
    }

    #[test]
    fn test_workload_generator_stop() {
        let wg = WorkloadGenerator::new(WorkloadConfig::default());
        assert!(!wg.is_running());
        wg.stop();
        assert!(!wg.is_running());
    }
}
