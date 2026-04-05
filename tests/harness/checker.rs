use crate::harness::cluster::TestCluster;
use crate::harness::workload::WorkloadLog;
use std::collections::HashMap;

#[derive(Debug)]
pub struct CheckResult {
    pub passed: bool,
    pub checks: Vec<CheckItem>,
}

#[derive(Debug)]
pub struct CheckItem {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

impl CheckResult {
    pub fn new() -> Self {
        Self {
            passed: true,
            checks: Vec::new(),
        }
    }

    pub fn add(&mut self, name: &str, passed: bool, detail: &str) {
        if !passed {
            self.passed = false;
        }
        self.checks.push(CheckItem {
            name: name.to_string(),
            passed,
            detail: detail.to_string(),
        });
    }

    pub fn summary(&self) -> String {
        let total = self.checks.len();
        let passed = self.checks.iter().filter(|c| c.passed).count();
        let failed = total - passed;
        let mut out = format!("{passed}/{total} checks passed");
        if failed > 0 {
            out.push_str("\nFailed:\n");
            for c in &self.checks {
                if !c.passed {
                    out.push_str(&format!("  - {}: {}\n", c.name, c.detail));
                }
            }
        }
        out
    }
}

impl Default for CheckResult {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Checker;

impl Checker {
    pub fn check_write_durability(log: &WorkloadLog) -> CheckResult {
        let mut result = CheckResult::new();

        let acked = log.acknowledged_writes();
        result.add(
            "acknowledged_writes_exist",
            !acked.is_empty() || log.write_count() == 0,
            &format!(
                "{} acknowledged out of {} total",
                acked.len(),
                log.write_count()
            ),
        );

        result
    }

    pub fn check_read_consistency(log: &WorkloadLog) -> CheckResult {
        let mut result = CheckResult::new();

        let mut write_map: HashMap<u64, &[u8]> = HashMap::new();
        for w in log.acknowledged_writes() {
            write_map.insert(w.offset, &w.data);
        }

        let mut stale_reads = 0;
        for r in &log.reads {
            if !r.success {
                continue;
            }
            if let Some(expected) = write_map.get(&r.offset) {
                if &r.data != *expected {
                    stale_reads += 1;
                }
            }
        }

        result.add(
            "no_stale_reads",
            stale_reads == 0,
            &format!("{stale_reads} stale reads detected"),
        );

        result
    }

    pub fn check_no_errors(log: &WorkloadLog) -> CheckResult {
        let mut result = CheckResult::new();
        result.add(
            "no_workload_errors",
            log.error_count() == 0,
            &format!("{} errors", log.error_count()),
        );
        result
    }

    pub async fn check_zfs_integrity(cluster: &TestCluster) -> CheckResult {
        let mut result = CheckResult::new();

        for node in cluster.running_nodes() {
            match cluster
                .ssh_exec(
                    node.id,
                    "zpool scrub blockyard && sleep 2 && zpool status blockyard",
                )
                .await
            {
                Ok(output) => {
                    let has_errors = output.contains("DEGRADED")
                        || output.contains("FAULTED")
                        || output.contains("UNAVAIL");
                    result.add(
                        &format!("zfs_integrity_node_{}", node.id),
                        !has_errors,
                        if has_errors {
                            "pool has errors"
                        } else {
                            "pool healthy"
                        },
                    );
                }
                Err(e) => {
                    result.add(
                        &format!("zfs_integrity_node_{}", node.id),
                        false,
                        &format!("ssh failed: {e}"),
                    );
                }
            }
        }

        result
    }

    pub async fn check_cluster_health(cluster: &TestCluster) -> CheckResult {
        let mut result = CheckResult::new();

        for node in cluster.running_nodes() {
            match cluster.ssh_exec(node.id, "pgrep -x blockyard").await {
                Ok(_) => {
                    result.add(
                        &format!("blockyard_running_node_{}", node.id),
                        true,
                        "process running",
                    );
                }
                Err(_) => {
                    result.add(
                        &format!("blockyard_running_node_{}", node.id),
                        false,
                        "process not running",
                    );
                }
            }
        }

        result
    }

    pub fn check_all(log: &WorkloadLog) -> CheckResult {
        let mut result = CheckResult::new();

        let durability = Self::check_write_durability(log);
        for c in durability.checks {
            result.add(&c.name, c.passed, &c.detail);
        }

        let consistency = Self::check_read_consistency(log);
        for c in consistency.checks {
            result.add(&c.name, c.passed, &c.detail);
        }

        let errors = Self::check_no_errors(log);
        for c in errors.checks {
            result.add(&c.name, c.passed, &c.detail);
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::workload::{ReadRecord, WorkloadLog, WriteRecord};
    use std::time::Duration;

    #[test]
    fn test_check_result_new() {
        let result = CheckResult::new();
        assert!(result.passed);
        assert!(result.checks.is_empty());
    }

    #[test]
    fn test_check_result_add_passing() {
        let mut result = CheckResult::new();
        result.add("test", true, "ok");
        assert!(result.passed);
        assert_eq!(result.checks.len(), 1);
    }

    #[test]
    fn test_check_result_add_failing() {
        let mut result = CheckResult::new();
        result.add("test", false, "failed");
        assert!(!result.passed);
    }

    #[test]
    fn test_check_result_summary_all_pass() {
        let mut result = CheckResult::new();
        result.add("a", true, "ok");
        result.add("b", true, "ok");
        let summary = result.summary();
        assert!(summary.starts_with("2/2"));
    }

    #[test]
    fn test_check_result_summary_with_failures() {
        let mut result = CheckResult::new();
        result.add("a", true, "ok");
        result.add("b", false, "broken");
        let summary = result.summary();
        assert!(summary.contains("1/2"));
        assert!(summary.contains("broken"));
    }

    #[test]
    fn test_check_write_durability_empty() {
        let log = WorkloadLog::new();
        let result = Checker::check_write_durability(&log);
        assert!(result.passed);
    }

    #[test]
    fn test_check_write_durability_with_writes() {
        let mut log = WorkloadLog::new();
        log.writes.push(WriteRecord {
            request_id: 1,
            volume_name: "v".into(),
            offset: 0,
            data: vec![1],
            acknowledged: true,
            timestamp: Duration::from_millis(1),
        });
        let result = Checker::check_write_durability(&log);
        assert!(result.passed);
    }

    #[test]
    fn test_check_read_consistency_no_stale() {
        let mut log = WorkloadLog::new();
        log.writes.push(WriteRecord {
            request_id: 1,
            volume_name: "v".into(),
            offset: 0,
            data: vec![42],
            acknowledged: true,
            timestamp: Duration::from_millis(1),
        });
        log.reads.push(ReadRecord {
            request_id: 2,
            volume_name: "v".into(),
            offset: 0,
            data: vec![42],
            success: true,
            timestamp: Duration::from_millis(5),
            latency: Duration::from_millis(2),
        });
        let result = Checker::check_read_consistency(&log);
        assert!(result.passed);
    }

    #[test]
    fn test_check_read_consistency_stale_read() {
        let mut log = WorkloadLog::new();
        log.writes.push(WriteRecord {
            request_id: 1,
            volume_name: "v".into(),
            offset: 0,
            data: vec![42],
            acknowledged: true,
            timestamp: Duration::from_millis(1),
        });
        log.reads.push(ReadRecord {
            request_id: 2,
            volume_name: "v".into(),
            offset: 0,
            data: vec![99],
            success: true,
            timestamp: Duration::from_millis(5),
            latency: Duration::from_millis(2),
        });
        let result = Checker::check_read_consistency(&log);
        assert!(!result.passed);
    }

    #[test]
    fn test_check_no_errors_clean() {
        let log = WorkloadLog::new();
        let result = Checker::check_no_errors(&log);
        assert!(result.passed);
    }

    #[test]
    fn test_check_no_errors_with_errors() {
        let mut log = WorkloadLog::new();
        log.errors.push("timeout".into());
        let result = Checker::check_no_errors(&log);
        assert!(!result.passed);
    }

    #[test]
    fn test_check_all_clean() {
        let log = WorkloadLog::new();
        let result = Checker::check_all(&log);
        assert!(result.passed);
        assert_eq!(result.checks.len(), 3);
    }
}
