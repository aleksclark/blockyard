use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::RwLock;
use rand::Rng;

use blockyard_common::{OperationId, VolumeId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationKind {
    Write,
    Read,
}

impl fmt::Display for OperationKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OperationKind::Write => write!(f, "write"),
            OperationKind::Read => write!(f, "read"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckStatus {
    Pending,
    Acked,
    Nacked,
    Timeout,
}

impl fmt::Display for AckStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AckStatus::Pending => write!(f, "pending"),
            AckStatus::Acked => write!(f, "acked"),
            AckStatus::Nacked => write!(f, "nacked"),
            AckStatus::Timeout => write!(f, "timeout"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Operation {
    pub id: OperationId,
    pub kind: OperationKind,
    pub volume_id: VolumeId,
    pub offset: u64,
    pub length: u32,
    pub data_checksum: Option<String>,
    pub status: AckStatus,
    pub started_at: Instant,
    pub completed_at: Option<Instant>,
    pub error: Option<String>,
}

impl Operation {
    pub fn new_write(volume_id: VolumeId, offset: u64, length: u32, checksum: String) -> Self {
        Self {
            id: OperationId::generate(),
            kind: OperationKind::Write,
            volume_id,
            offset,
            length,
            data_checksum: Some(checksum),
            status: AckStatus::Pending,
            started_at: Instant::now(),
            completed_at: None,
            error: None,
        }
    }

    pub fn new_read(volume_id: VolumeId, offset: u64, length: u32) -> Self {
        Self {
            id: OperationId::generate(),
            kind: OperationKind::Read,
            volume_id,
            offset,
            length,
            data_checksum: None,
            status: AckStatus::Pending,
            started_at: Instant::now(),
            completed_at: None,
            error: None,
        }
    }

    pub fn complete(&mut self, status: AckStatus) {
        self.status = status;
        self.completed_at = Some(Instant::now());
    }

    pub fn complete_with_error(&mut self, error: String) {
        self.status = AckStatus::Nacked;
        self.completed_at = Some(Instant::now());
        self.error = Some(error);
    }

    pub fn latency(&self) -> Option<Duration> {
        self.completed_at.map(|t| t.duration_since(self.started_at))
    }

    pub fn is_complete(&self) -> bool {
        self.completed_at.is_some()
    }
}

#[derive(Debug)]
pub struct OperationLog {
    operations: RwLock<Vec<Operation>>,
}

impl OperationLog {
    pub fn new() -> Self {
        Self {
            operations: RwLock::new(Vec::new()),
        }
    }

    pub fn record(&self, op: Operation) {
        self.operations.write().push(op);
    }

    pub fn all(&self) -> Vec<Operation> {
        self.operations.read().clone()
    }

    pub fn writes(&self) -> Vec<Operation> {
        self.operations
            .read()
            .iter()
            .filter(|op| op.kind == OperationKind::Write)
            .cloned()
            .collect()
    }

    pub fn reads(&self) -> Vec<Operation> {
        self.operations
            .read()
            .iter()
            .filter(|op| op.kind == OperationKind::Read)
            .cloned()
            .collect()
    }

    pub fn acked_writes(&self) -> Vec<Operation> {
        self.operations
            .read()
            .iter()
            .filter(|op| op.kind == OperationKind::Write && op.status == AckStatus::Acked)
            .cloned()
            .collect()
    }

    pub fn failed_operations(&self) -> Vec<Operation> {
        self.operations
            .read()
            .iter()
            .filter(|op| op.status == AckStatus::Nacked || op.status == AckStatus::Timeout)
            .cloned()
            .collect()
    }

    pub fn count(&self) -> usize {
        self.operations.read().len()
    }

    pub fn write_count(&self) -> usize {
        self.operations
            .read()
            .iter()
            .filter(|op| op.kind == OperationKind::Write)
            .count()
    }

    pub fn read_count(&self) -> usize {
        self.operations
            .read()
            .iter()
            .filter(|op| op.kind == OperationKind::Read)
            .count()
    }

    pub fn acked_write_count(&self) -> usize {
        self.operations
            .read()
            .iter()
            .filter(|op| op.kind == OperationKind::Write && op.status == AckStatus::Acked)
            .count()
    }

    pub fn average_latency(&self) -> Option<Duration> {
        let ops = self.operations.read();
        let latencies: Vec<Duration> = ops.iter().filter_map(|op| op.latency()).collect();
        if latencies.is_empty() {
            return None;
        }
        let total: Duration = latencies.iter().sum();
        Some(total / latencies.len() as u32)
    }
}

impl Default for OperationLog {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyDistribution {
    Sequential,
    Uniform,
    Zipfian,
    Hotspot {
        hot_fraction: u32,
        hot_ops_percent: u32,
    },
}

#[derive(Debug, Clone)]
pub struct WorkloadConfig {
    pub volume_ids: Vec<VolumeId>,
    pub write_ratio: f64,
    pub block_size: u32,
    pub max_offset: u64,
    pub operations_per_second: u32,
    pub duration: Duration,
    pub concurrent_clients: u32,
    pub key_distribution: KeyDistribution,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            volume_ids: vec![VolumeId::generate()],
            write_ratio: 0.5,
            block_size: 4096,
            max_offset: 1024 * 1024,
            operations_per_second: 100,
            duration: Duration::from_secs(10),
            concurrent_clients: 1,
            key_distribution: KeyDistribution::Uniform,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkloadResult {
    pub total_operations: u64,
    pub total_writes: u64,
    pub total_reads: u64,
    pub acked_writes: u64,
    pub failed_operations: u64,
    pub average_latency: Option<Duration>,
    pub elapsed: Duration,
    pub ops_per_second: f64,
}

pub struct WorkloadGenerator {
    config: WorkloadConfig,
    log: OperationLog,
    ops_generated: AtomicU64,
    sequential_offset: AtomicU64,
}

impl fmt::Debug for WorkloadGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkloadGenerator")
            .field("config", &self.config)
            .field("ops_generated", &self.ops_generated.load(Ordering::Relaxed))
            .finish()
    }
}

impl WorkloadGenerator {
    pub fn new(config: WorkloadConfig) -> Self {
        Self {
            config,
            log: OperationLog::new(),
            ops_generated: AtomicU64::new(0),
            sequential_offset: AtomicU64::new(0),
        }
    }

    pub fn config(&self) -> &WorkloadConfig {
        &self.config
    }

    pub fn log(&self) -> &OperationLog {
        &self.log
    }

    pub fn generate_operation(&self) -> Operation {
        let mut rng = rand::thread_rng(); // noqa: gen is reserved in 2024

        let volume_id = if self.config.volume_ids.is_empty() {
            VolumeId::generate()
        } else {
            let idx = rng.r#gen_range(0..self.config.volume_ids.len());
            self.config.volume_ids[idx]
        };

        let is_write: bool = rng.r#gen::<f64>() < self.config.write_ratio;
        let offset = self.next_offset(&mut rng);

        let op = if is_write {
            let data = self.generate_data(&mut rng);
            let checksum = blake3::hash(&data).to_hex().to_string();
            Operation::new_write(volume_id, offset, self.config.block_size, checksum)
        } else {
            Operation::new_read(volume_id, offset, self.config.block_size)
        };

        self.ops_generated.fetch_add(1, Ordering::Relaxed);
        op
    }

    pub fn generate_and_record(&self) -> Operation {
        let op = self.generate_operation();
        self.log.record(op.clone());
        op
    }

    pub fn generate_write(&self, volume_id: VolumeId, offset: u64) -> (Operation, Bytes) {
        let mut rng = rand::thread_rng(); // rng via thread_rng
        let data = self.generate_data(&mut rng);
        let checksum = blake3::hash(&data).to_hex().to_string();
        let op = Operation::new_write(volume_id, offset, self.config.block_size, checksum);
        self.ops_generated.fetch_add(1, Ordering::Relaxed);
        (op, Bytes::from(data))
    }

    pub fn ops_generated(&self) -> u64 {
        self.ops_generated.load(Ordering::Relaxed)
    }

    pub fn result(&self) -> WorkloadResult {
        let log = &self.log;
        let total_operations = log.count() as u64;
        let total_writes = log.write_count() as u64;
        let total_reads = log.read_count() as u64;
        let acked_writes = log.acked_write_count() as u64;
        let failed_operations = log.failed_operations().len() as u64;
        let average_latency = log.average_latency();

        let elapsed = if total_operations > 0 {
            let ops = log.all();
            let first = ops.iter().map(|o| o.started_at).min();
            let last = ops.iter().filter_map(|o| o.completed_at).max();
            match (first, last) {
                (Some(f), Some(l)) => l.duration_since(f),
                _ => Duration::ZERO,
            }
        } else {
            Duration::ZERO
        };

        let ops_per_second = if elapsed.as_secs_f64() > 0.0 {
            total_operations as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        WorkloadResult {
            total_operations,
            total_writes,
            total_reads,
            acked_writes,
            failed_operations,
            average_latency,
            elapsed,
            ops_per_second,
        }
    }

    fn next_offset(&self, source: &mut impl Rng) -> u64 {
        let max_blocks = self.config.max_offset / self.config.block_size as u64;
        if max_blocks == 0 {
            return 0;
        }

        let block = match self.config.key_distribution {
            KeyDistribution::Sequential => {
                self.sequential_offset.fetch_add(1, Ordering::Relaxed) % max_blocks
            }
            KeyDistribution::Uniform => source.r#gen_range(0..max_blocks),
            KeyDistribution::Zipfian => {
                let u: f64 = source.r#gen();
                let rank = ((max_blocks as f64).powf(1.0 - u)).floor() as u64;
                rank.min(max_blocks - 1)
            }
            KeyDistribution::Hotspot {
                hot_fraction,
                hot_ops_percent,
            } => {
                let hot_range = max_blocks * hot_fraction as u64 / 100;
                let hot_range = hot_range.max(1);
                if source.r#gen_range(0..100) < hot_ops_percent {
                    source.r#gen_range(0..hot_range)
                } else {
                    source.r#gen_range(0..max_blocks)
                }
            }
        };

        block * self.config.block_size as u64
    }

    fn generate_data(&self, source: &mut impl Rng) -> Vec<u8> {
        let mut data = vec![0u8; self.config.block_size as usize];
        source.fill(&mut data[..]);
        data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_kind_display() {
        assert_eq!(format!("{}", OperationKind::Write), "write");
        assert_eq!(format!("{}", OperationKind::Read), "read");
    }

    #[test]
    fn test_ack_status_display() {
        assert_eq!(format!("{}", AckStatus::Pending), "pending");
        assert_eq!(format!("{}", AckStatus::Acked), "acked");
        assert_eq!(format!("{}", AckStatus::Nacked), "nacked");
        assert_eq!(format!("{}", AckStatus::Timeout), "timeout");
    }

    #[test]
    fn test_operation_new_write() {
        let vol = VolumeId::generate();
        let op = Operation::new_write(vol, 0, 4096, "abc123".to_string());
        assert_eq!(op.kind, OperationKind::Write);
        assert_eq!(op.volume_id, vol);
        assert_eq!(op.offset, 0);
        assert_eq!(op.length, 4096);
        assert_eq!(op.data_checksum.as_deref(), Some("abc123"));
        assert_eq!(op.status, AckStatus::Pending);
        assert!(!op.is_complete());
        assert!(op.latency().is_none());
    }

    #[test]
    fn test_operation_new_read() {
        let vol = VolumeId::generate();
        let op = Operation::new_read(vol, 8192, 4096);
        assert_eq!(op.kind, OperationKind::Read);
        assert!(op.data_checksum.is_none());
        assert_eq!(op.status, AckStatus::Pending);
    }

    #[test]
    fn test_operation_complete() {
        let vol = VolumeId::generate();
        let mut op = Operation::new_write(vol, 0, 4096, "hash".to_string());
        assert!(!op.is_complete());

        op.complete(AckStatus::Acked);
        assert!(op.is_complete());
        assert_eq!(op.status, AckStatus::Acked);
        assert!(op.latency().is_some());
    }

    #[test]
    fn test_operation_complete_with_error() {
        let vol = VolumeId::generate();
        let mut op = Operation::new_write(vol, 0, 4096, "hash".to_string());

        op.complete_with_error("disk full".to_string());
        assert_eq!(op.status, AckStatus::Nacked);
        assert_eq!(op.error.as_deref(), Some("disk full"));
        assert!(op.is_complete());
    }

    #[test]
    fn test_operation_log_basic() {
        let log = OperationLog::new();
        assert_eq!(log.count(), 0);

        let vol = VolumeId::generate();
        let mut write_op = Operation::new_write(vol, 0, 4096, "hash".to_string());
        write_op.complete(AckStatus::Acked);
        log.record(write_op);

        let read_op = Operation::new_read(vol, 0, 4096);
        log.record(read_op);

        assert_eq!(log.count(), 2);
        assert_eq!(log.write_count(), 1);
        assert_eq!(log.read_count(), 1);
        assert_eq!(log.acked_write_count(), 1);
    }

    #[test]
    fn test_operation_log_filters() {
        let log = OperationLog::new();
        let vol = VolumeId::generate();

        let mut op1 = Operation::new_write(vol, 0, 4096, "h1".to_string());
        op1.complete(AckStatus::Acked);
        log.record(op1);

        let mut op2 = Operation::new_write(vol, 4096, 4096, "h2".to_string());
        op2.complete(AckStatus::Nacked);
        log.record(op2);

        let mut op3 = Operation::new_read(vol, 0, 4096);
        op3.complete(AckStatus::Timeout);
        log.record(op3);

        assert_eq!(log.writes().len(), 2);
        assert_eq!(log.reads().len(), 1);
        assert_eq!(log.acked_writes().len(), 1);
        assert_eq!(log.failed_operations().len(), 2);
    }

    #[test]
    fn test_operation_log_average_latency() {
        let log = OperationLog::new();
        assert!(log.average_latency().is_none());

        let vol = VolumeId::generate();
        let mut op = Operation::new_write(vol, 0, 4096, "hash".to_string());
        std::thread::sleep(Duration::from_millis(5));
        op.complete(AckStatus::Acked);
        log.record(op);

        let avg = log.average_latency();
        assert!(avg.is_some());
        assert!(avg.unwrap() >= Duration::from_millis(1));
    }

    #[test]
    fn test_operation_log_default() {
        let log = OperationLog::default();
        assert_eq!(log.count(), 0);
    }

    #[test]
    fn test_workload_config_default() {
        let config = WorkloadConfig::default();
        assert_eq!(config.write_ratio, 0.5);
        assert_eq!(config.block_size, 4096);
        assert_eq!(config.operations_per_second, 100);
        assert_eq!(config.concurrent_clients, 1);
        assert_eq!(config.key_distribution, KeyDistribution::Uniform);
    }

    #[test]
    fn test_workload_generator_generates_operations() {
        let config = WorkloadConfig::default();
        let wg = WorkloadGenerator::new(config);

        let op = wg.generate_operation();
        assert!(op.kind == OperationKind::Write || op.kind == OperationKind::Read);
        assert_eq!(wg.ops_generated(), 1);
    }

    #[test]
    fn test_workload_generator_generate_and_record() {
        let config = WorkloadConfig::default();
        let wg = WorkloadGenerator::new(config);

        wg.generate_and_record();
        wg.generate_and_record();
        wg.generate_and_record();

        assert_eq!(wg.log().count(), 3);
        assert_eq!(wg.ops_generated(), 3);
    }

    #[test]
    fn test_workload_generator_write_heavy() {
        let config = WorkloadConfig {
            write_ratio: 1.0,
            ..Default::default()
        };
        let wg = WorkloadGenerator::new(config);

        for _ in 0..20 {
            let op = wg.generate_operation();
            assert_eq!(op.kind, OperationKind::Write);
        }
    }

    #[test]
    fn test_workload_generator_read_heavy() {
        let config = WorkloadConfig {
            write_ratio: 0.0,
            ..Default::default()
        };
        let wg = WorkloadGenerator::new(config);

        for _ in 0..20 {
            let op = wg.generate_operation();
            assert_eq!(op.kind, OperationKind::Read);
        }
    }

    #[test]
    fn test_workload_generator_sequential_offset() {
        let config = WorkloadConfig {
            key_distribution: KeyDistribution::Sequential,
            block_size: 4096,
            max_offset: 4096 * 10,
            write_ratio: 1.0,
            ..Default::default()
        };
        let wg = WorkloadGenerator::new(config);

        let op1 = wg.generate_operation();
        let op2 = wg.generate_operation();
        let op3 = wg.generate_operation();

        assert_eq!(op1.offset, 0);
        assert_eq!(op2.offset, 4096);
        assert_eq!(op3.offset, 8192);
    }

    #[test]
    fn test_workload_generator_generate_write() {
        let vol = VolumeId::generate();
        let config = WorkloadConfig {
            block_size: 512,
            ..Default::default()
        };
        let wg = WorkloadGenerator::new(config);

        let (op, data) = wg.generate_write(vol, 1024);
        assert_eq!(op.kind, OperationKind::Write);
        assert_eq!(op.volume_id, vol);
        assert_eq!(op.offset, 1024);
        assert_eq!(op.length, 512);
        assert_eq!(data.len(), 512);
        assert!(op.data_checksum.is_some());

        let expected_checksum = blake3::hash(&data).to_hex().to_string();
        assert_eq!(op.data_checksum.unwrap(), expected_checksum);
    }

    #[test]
    fn test_workload_generator_result_empty() {
        let config = WorkloadConfig::default();
        let wg = WorkloadGenerator::new(config);
        let result = wg.result();
        assert_eq!(result.total_operations, 0);
        assert_eq!(result.total_writes, 0);
        assert_eq!(result.total_reads, 0);
        assert_eq!(result.acked_writes, 0);
        assert_eq!(result.ops_per_second, 0.0);
    }

    #[test]
    fn test_workload_generator_result_with_ops() {
        let config = WorkloadConfig {
            write_ratio: 1.0,
            ..Default::default()
        };
        let wg = WorkloadGenerator::new(config);

        for _ in 0..5 {
            let mut op = wg.generate_operation();
            op.complete(AckStatus::Acked);
            wg.log().record(op);
        }

        let result = wg.result();
        assert_eq!(result.total_operations, 5);
        assert_eq!(result.total_writes, 5);
        assert_eq!(result.acked_writes, 5);
    }

    #[test]
    fn test_workload_generator_hotspot_distribution() {
        let config = WorkloadConfig {
            key_distribution: KeyDistribution::Hotspot {
                hot_fraction: 10,
                hot_ops_percent: 90,
            },
            block_size: 4096,
            max_offset: 4096 * 1000,
            write_ratio: 1.0,
            ..Default::default()
        };
        let wg = WorkloadGenerator::new(config);

        let hot_range = 4096u64 * 100;
        let mut hot_count = 0u32;
        let total = 1000;
        for _ in 0..total {
            let op = wg.generate_operation();
            if op.offset < hot_range {
                hot_count += 1;
            }
        }

        let hot_ratio = hot_count as f64 / total as f64;
        assert!(
            hot_ratio > 0.5,
            "hotspot should concentrate ops in hot range, got {:.1}%",
            hot_ratio * 100.0
        );
    }

    #[test]
    fn test_workload_generator_zipfian_distribution() {
        let config = WorkloadConfig {
            key_distribution: KeyDistribution::Zipfian,
            block_size: 4096,
            max_offset: 4096 * 1000,
            write_ratio: 1.0,
            ..Default::default()
        };
        let wg = WorkloadGenerator::new(config);

        let mut low_offset_count = 0u32;
        let total = 1000;
        let threshold = 4096u64 * 100;
        for _ in 0..total {
            let op = wg.generate_operation();
            if op.offset < threshold {
                low_offset_count += 1;
            }
        }

        let low_ratio = low_offset_count as f64 / total as f64;
        assert!(
            low_ratio > 0.3,
            "zipfian should skew toward low offsets, got {:.1}%",
            low_ratio * 100.0
        );
    }

    #[test]
    fn test_workload_generator_zero_max_offset() {
        let config = WorkloadConfig {
            max_offset: 0,
            write_ratio: 1.0,
            ..Default::default()
        };
        let wg = WorkloadGenerator::new(config);
        let op = wg.generate_operation();
        assert_eq!(op.offset, 0);
    }

    #[test]
    fn test_key_distribution_eq() {
        assert_eq!(KeyDistribution::Sequential, KeyDistribution::Sequential);
        assert_eq!(KeyDistribution::Uniform, KeyDistribution::Uniform);
        assert_ne!(KeyDistribution::Sequential, KeyDistribution::Uniform);
        assert_eq!(
            KeyDistribution::Hotspot {
                hot_fraction: 10,
                hot_ops_percent: 90
            },
            KeyDistribution::Hotspot {
                hot_fraction: 10,
                hot_ops_percent: 90
            }
        );
    }
}
