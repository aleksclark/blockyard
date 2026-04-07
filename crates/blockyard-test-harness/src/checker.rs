use std::collections::HashMap;
use std::fmt;

use tracing::{error, info, warn};

use crate::workload::{Operation, OperationLog};
use blockyard_common::VolumeId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckResult {
    Pass,
    Fail(String),
}

impl fmt::Display for CheckResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CheckResult::Pass => write!(f, "PASS"),
            CheckResult::Fail(msg) => write!(f, "FAIL: {}", msg),
        }
    }
}

impl CheckResult {
    pub fn is_pass(&self) -> bool {
        matches!(self, CheckResult::Pass)
    }

    pub fn is_fail(&self) -> bool {
        matches!(self, CheckResult::Fail(_))
    }
}

#[derive(Debug, Clone)]
pub struct CheckReport {
    pub name: String,
    pub result: CheckResult,
    pub details: Vec<String>,
}

impl fmt::Display for CheckReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.result, self.name)?;
        for detail in &self.details {
            write!(f, "\n  {}", detail)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ConsistencyChecker {
    operation_log: OperationLog,
    read_back_data: HashMap<(VolumeId, u64), String>,
    min_operations: u64,
}

impl ConsistencyChecker {
    pub fn new(operation_log: OperationLog) -> Self {
        Self {
            operation_log,
            read_back_data: HashMap::new(),
            min_operations: 1,
        }
    }

    pub fn with_min_operations(mut self, min: u64) -> Self {
        self.min_operations = min;
        self
    }

    pub fn record_read_back(&mut self, volume_id: VolumeId, offset: u64, checksum: String) {
        self.read_back_data.insert((volume_id, offset), checksum);
    }

    pub fn check_all(&self) -> Vec<CheckReport> {
        let reports = vec![
            self.check_io_happened(),
            self.check_acked_writes_readable(),
            self.check_no_lost_acks(),
            self.check_data_integrity(),
        ];

        for report in &reports {
            match &report.result {
                CheckResult::Pass => info!("{}", report),
                CheckResult::Fail(_) => error!("{}", report),
            }
        }

        reports
    }

    pub fn check_io_happened(&self) -> CheckReport {
        let total = self.operation_log.count() as u64;
        let acked_writes = self.operation_log.acked_write_count() as u64;

        if total < self.min_operations {
            return CheckReport {
                name: "io_happened".to_string(),
                result: CheckResult::Fail(format!(
                    "workload generated only {} operations, minimum is {}",
                    total, self.min_operations
                )),
                details: vec![
                    format!("total_operations: {}", total),
                    format!("min_required: {}", self.min_operations),
                ],
            };
        }

        if acked_writes == 0 {
            return CheckReport {
                name: "io_happened".to_string(),
                result: CheckResult::Fail(
                    "workload had zero acked writes — no data was durably committed".to_string(),
                ),
                details: vec![
                    format!("total_operations: {}", total),
                    format!("acked_writes: 0"),
                ],
            };
        }

        CheckReport {
            name: "io_happened".to_string(),
            result: CheckResult::Pass,
            details: vec![
                format!("total_operations: {}", total),
                format!("acked_writes: {}", acked_writes),
            ],
        }
    }

    pub fn check_acked_writes_readable(&self) -> CheckReport {
        let acked = self.operation_log.acked_writes();
        if acked.is_empty() {
            return CheckReport {
                name: "acked_writes_readable".to_string(),
                result: CheckResult::Pass,
                details: vec!["no acked writes to verify".to_string()],
            };
        }

        let mut missing = Vec::new();
        for op in &acked {
            let key = (op.volume_id, op.offset);
            if !self.read_back_data.contains_key(&key) {
                missing.push(format!(
                    "op={} vol={} offset={}: no read-back data recorded",
                    op.id, op.volume_id, op.offset
                ));
            }
        }

        if missing.is_empty() {
            CheckReport {
                name: "acked_writes_readable".to_string(),
                result: CheckResult::Pass,
                details: vec![format!(
                    "all {} acked writes have read-back data",
                    acked.len()
                )],
            }
        } else {
            let fail_count = missing.len();
            CheckReport {
                name: "acked_writes_readable".to_string(),
                result: CheckResult::Fail(format!(
                    "{} acked writes missing read-back verification",
                    fail_count
                )),
                details: missing.into_iter().take(10).collect(),
            }
        }
    }

    pub fn check_no_lost_acks(&self) -> CheckReport {
        let acked = self.operation_log.acked_writes();
        if acked.is_empty() {
            return CheckReport {
                name: "no_lost_acks".to_string(),
                result: CheckResult::Pass,
                details: vec!["no acked writes to verify".to_string()],
            };
        }

        let mut latest_writes: HashMap<(VolumeId, u64), &Operation> = HashMap::new();
        for op in &acked {
            let key = (op.volume_id, op.offset);
            match latest_writes.get(&key) {
                Some(existing) if existing.started_at > op.started_at => {}
                _ => {
                    latest_writes.insert(key, op);
                }
            }
        }

        let mut mismatches = Vec::new();
        for (key, write_op) in &latest_writes {
            if let Some(read_checksum) = self.read_back_data.get(key) {
                if let Some(write_checksum) = &write_op.data_checksum {
                    if write_checksum != read_checksum {
                        mismatches.push(format!(
                            "vol={} offset={}: write_checksum={} != read_checksum={}",
                            key.0, key.1, write_checksum, read_checksum
                        ));
                    }
                }
            }
        }

        if mismatches.is_empty() {
            CheckReport {
                name: "no_lost_acks".to_string(),
                result: CheckResult::Pass,
                details: vec![format!(
                    "all {} latest acked writes have matching checksums",
                    latest_writes.len()
                )],
            }
        } else {
            let fail_count = mismatches.len();
            CheckReport {
                name: "no_lost_acks".to_string(),
                result: CheckResult::Fail(format!(
                    "{} acked writes have mismatched checksums after read-back",
                    fail_count
                )),
                details: mismatches.into_iter().take(10).collect(),
            }
        }
    }

    pub fn check_data_integrity(&self) -> CheckReport {
        let all_ops = self.operation_log.all();
        let completed = all_ops.iter().filter(|op| op.is_complete()).count();
        let pending = all_ops.iter().filter(|op| !op.is_complete()).count();
        let failed = self.operation_log.failed_operations().len();

        let mut details = vec![
            format!("total_operations: {}", all_ops.len()),
            format!("completed: {}", completed),
            format!("pending: {}", pending),
            format!("failed: {}", failed),
        ];

        if pending > 0 {
            warn!("{} operations still pending at check time", pending);
        }

        let duplicate_offsets = self.find_conflicting_writes();
        if !duplicate_offsets.is_empty() {
            details.push(format!(
                "conflicting_write_offsets: {} (last-writer-wins used for verification)",
                duplicate_offsets.len()
            ));
        }

        CheckReport {
            name: "data_integrity".to_string(),
            result: CheckResult::Pass,
            details,
        }
    }

    fn find_conflicting_writes(&self) -> Vec<(VolumeId, u64)> {
        let acked = self.operation_log.acked_writes();
        let mut offset_counts: HashMap<(VolumeId, u64), u32> = HashMap::new();

        for op in &acked {
            *offset_counts.entry((op.volume_id, op.offset)).or_insert(0) += 1;
        }

        offset_counts
            .into_iter()
            .filter(|(_, count)| *count > 1)
            .map(|(key, _)| key)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::AckStatus;
    use std::time::Duration;

    #[test]
    fn test_check_result_display() {
        assert_eq!(format!("{}", CheckResult::Pass), "PASS");
        assert_eq!(
            format!("{}", CheckResult::Fail("bad data".to_string())),
            "FAIL: bad data"
        );
    }

    #[test]
    fn test_check_result_is_pass_fail() {
        assert!(CheckResult::Pass.is_pass());
        assert!(!CheckResult::Pass.is_fail());
        assert!(!CheckResult::Fail("x".to_string()).is_pass());
        assert!(CheckResult::Fail("x".to_string()).is_fail());
    }

    #[test]
    fn test_check_report_display() {
        let report = CheckReport {
            name: "test_check".to_string(),
            result: CheckResult::Pass,
            details: vec!["detail1".to_string(), "detail2".to_string()],
        };
        let s = format!("{}", report);
        assert!(s.contains("[PASS]"));
        assert!(s.contains("test_check"));
        assert!(s.contains("detail1"));
    }

    #[test]
    fn test_check_io_happened_empty() {
        let log = OperationLog::new();
        let checker = ConsistencyChecker::new(log);
        let report = checker.check_io_happened();
        assert!(report.result.is_fail());
        assert!(report.result.to_string().contains("only 0 operations"));
    }

    #[test]
    fn test_check_io_happened_no_acked_writes() {
        let log = OperationLog::new();
        let vol = VolumeId::generate();
        let mut op = Operation::new_write(vol, 0, 4096, "hash".to_string());
        op.complete(AckStatus::Nacked);
        log.record(op);

        let checker = ConsistencyChecker::new(log);
        let report = checker.check_io_happened();
        assert!(report.result.is_fail());
        assert!(report.result.to_string().contains("zero acked writes"));
    }

    #[test]
    fn test_check_io_happened_success() {
        let log = OperationLog::new();
        let vol = VolumeId::generate();
        let mut op = Operation::new_write(vol, 0, 4096, "hash".to_string());
        op.complete(AckStatus::Acked);
        log.record(op);

        let checker = ConsistencyChecker::new(log);
        let report = checker.check_io_happened();
        assert!(report.result.is_pass());
    }

    #[test]
    fn test_check_io_happened_min_operations() {
        let log = OperationLog::new();
        let vol = VolumeId::generate();
        let mut op = Operation::new_write(vol, 0, 4096, "hash".to_string());
        op.complete(AckStatus::Acked);
        log.record(op);

        let checker = ConsistencyChecker::new(log).with_min_operations(10);
        let report = checker.check_io_happened();
        assert!(report.result.is_fail());
    }

    #[test]
    fn test_check_acked_writes_readable_no_readback() {
        let log = OperationLog::new();
        let vol = VolumeId::generate();
        let mut op = Operation::new_write(vol, 0, 4096, "hash".to_string());
        op.complete(AckStatus::Acked);
        log.record(op);

        let checker = ConsistencyChecker::new(log);
        let report = checker.check_acked_writes_readable();
        assert!(report.result.is_fail());
    }

    #[test]
    fn test_check_acked_writes_readable_with_readback() {
        let log = OperationLog::new();
        let vol = VolumeId::generate();
        let mut op = Operation::new_write(vol, 0, 4096, "hash".to_string());
        op.complete(AckStatus::Acked);
        log.record(op);

        let mut checker = ConsistencyChecker::new(log);
        checker.record_read_back(vol, 0, "hash".to_string());
        let report = checker.check_acked_writes_readable();
        assert!(report.result.is_pass());
    }

    #[test]
    fn test_check_no_lost_acks_matching() {
        let log = OperationLog::new();
        let vol = VolumeId::generate();
        let mut op = Operation::new_write(vol, 0, 4096, "checksum_a".to_string());
        op.complete(AckStatus::Acked);
        log.record(op);

        let mut checker = ConsistencyChecker::new(log);
        checker.record_read_back(vol, 0, "checksum_a".to_string());
        let report = checker.check_no_lost_acks();
        assert!(report.result.is_pass());
    }

    #[test]
    fn test_check_no_lost_acks_mismatch() {
        let log = OperationLog::new();
        let vol = VolumeId::generate();
        let mut op = Operation::new_write(vol, 0, 4096, "checksum_a".to_string());
        op.complete(AckStatus::Acked);
        log.record(op);

        let mut checker = ConsistencyChecker::new(log);
        checker.record_read_back(vol, 0, "checksum_b".to_string());
        let report = checker.check_no_lost_acks();
        assert!(report.result.is_fail());
        assert!(report.result.to_string().contains("mismatched checksums"));
    }

    #[test]
    fn test_check_no_lost_acks_last_writer_wins() {
        let log = OperationLog::new();
        let vol = VolumeId::generate();

        let mut op1 = Operation::new_write(vol, 0, 4096, "old_checksum".to_string());
        op1.complete(AckStatus::Acked);
        log.record(op1);

        std::thread::sleep(Duration::from_millis(5));

        let mut op2 = Operation::new_write(vol, 0, 4096, "new_checksum".to_string());
        op2.complete(AckStatus::Acked);
        log.record(op2);

        let mut checker = ConsistencyChecker::new(log);
        checker.record_read_back(vol, 0, "new_checksum".to_string());
        let report = checker.check_no_lost_acks();
        assert!(report.result.is_pass());
    }

    #[test]
    fn test_check_data_integrity() {
        let log = OperationLog::new();
        let vol = VolumeId::generate();

        let mut op1 = Operation::new_write(vol, 0, 4096, "h1".to_string());
        op1.complete(AckStatus::Acked);
        log.record(op1);

        let mut op2 = Operation::new_read(vol, 0, 4096);
        op2.complete(AckStatus::Acked);
        log.record(op2);

        let checker = ConsistencyChecker::new(log);
        let report = checker.check_data_integrity();
        assert!(report.result.is_pass());
        assert!(report.details.iter().any(|d| d.contains("completed: 2")));
    }

    #[test]
    fn test_check_all_passes() {
        let log = OperationLog::new();
        let vol = VolumeId::generate();

        let mut op = Operation::new_write(vol, 0, 4096, "test_hash".to_string());
        op.complete(AckStatus::Acked);
        log.record(op);

        let mut checker = ConsistencyChecker::new(log);
        checker.record_read_back(vol, 0, "test_hash".to_string());

        let reports = checker.check_all();
        assert_eq!(reports.len(), 4);
        assert!(reports.iter().all(|r| r.result.is_pass()));
    }

    #[test]
    fn test_check_all_with_failures() {
        let log = OperationLog::new();
        let checker = ConsistencyChecker::new(log);
        let reports = checker.check_all();

        let failures: Vec<_> = reports.iter().filter(|r| r.result.is_fail()).collect();
        assert!(!failures.is_empty());
    }

    #[test]
    fn test_find_conflicting_writes() {
        let log = OperationLog::new();
        let vol = VolumeId::generate();

        let mut op1 = Operation::new_write(vol, 0, 4096, "h1".to_string());
        op1.complete(AckStatus::Acked);
        log.record(op1);

        let mut op2 = Operation::new_write(vol, 0, 4096, "h2".to_string());
        op2.complete(AckStatus::Acked);
        log.record(op2);

        let checker = ConsistencyChecker::new(log);
        let conflicts = checker.find_conflicting_writes();
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0], (vol, 0));
    }

    #[test]
    fn test_check_acked_writes_readable_empty_log() {
        let log = OperationLog::new();
        let checker = ConsistencyChecker::new(log);
        let report = checker.check_acked_writes_readable();
        assert!(report.result.is_pass());
    }

    #[test]
    fn test_check_no_lost_acks_empty_log() {
        let log = OperationLog::new();
        let checker = ConsistencyChecker::new(log);
        let report = checker.check_no_lost_acks();
        assert!(report.result.is_pass());
    }

    #[test]
    fn test_consistency_checker_multiple_volumes() {
        let log = OperationLog::new();
        let vol1 = VolumeId::generate();
        let vol2 = VolumeId::generate();

        let mut op1 = Operation::new_write(vol1, 0, 4096, "hash_v1".to_string());
        op1.complete(AckStatus::Acked);
        log.record(op1);

        let mut op2 = Operation::new_write(vol2, 0, 4096, "hash_v2".to_string());
        op2.complete(AckStatus::Acked);
        log.record(op2);

        let mut checker = ConsistencyChecker::new(log);
        checker.record_read_back(vol1, 0, "hash_v1".to_string());
        checker.record_read_back(vol2, 0, "hash_v2".to_string());

        let reports = checker.check_all();
        assert!(reports.iter().all(|r| r.result.is_pass()));
    }
}
