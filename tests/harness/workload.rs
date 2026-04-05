use crate::harness::cluster::TestCluster;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

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
}

impl Default for WorkloadLog {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct WorkloadConfig {
    pub volume_name: String,
    pub duration: Duration,
    pub write_rate: u64,
    pub read_rate: u64,
    pub block_size: usize,
    pub max_offset: u64,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            volume_name: "test-vol".into(),
            duration: Duration::from_secs(30),
            write_rate: 100,
            read_rate: 200,
            block_size: 4096,
            max_offset: 1024 * 1024 * 1024,
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

    pub async fn record_write(
        &self,
        offset: u64,
        data: Vec<u8>,
        acknowledged: bool,
    ) -> u64 {
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

    pub fn start(&self) {
        self.running.store(true, Ordering::Relaxed);
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workload_config_default() {
        let config = WorkloadConfig::default();
        assert_eq!(config.volume_name, "test-vol");
        assert_eq!(config.block_size, 4096);
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

    #[test]
    fn test_workload_generator_start_stop() {
        let wg = WorkloadGenerator::new(WorkloadConfig::default());
        assert!(!wg.is_running());
        wg.start();
        assert!(wg.is_running());
        wg.stop();
        assert!(!wg.is_running());
    }

    #[tokio::test]
    async fn test_workload_generator_increments_ids() {
        let wg = WorkloadGenerator::new(WorkloadConfig::default());
        let id1 = wg.record_write(0, vec![], true).await;
        let id2 = wg.record_write(0, vec![], true).await;
        let id3 = wg
            .record_read(0, vec![], true, Duration::ZERO)
            .await;
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
    }
}
