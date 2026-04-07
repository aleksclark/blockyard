//! Background scrubbing: verify extent file readability, checksum integrity,
//! and local metadata recoverability (P7.1, P7.2, §5.12).
//!
//! The [`ScrubWorker`] periodically scans extent files on each disk,
//! verifies checksums, and reports corruption for repair.

use std::time::Instant;

use blockyard_common::{DiskId, ExtentId};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::background::rate_limit::TokenBucket;

/// Configuration for the scrub worker.
#[derive(Debug, Clone)]
pub struct ScrubConfig {
    /// Interval between full scrub passes in seconds.
    pub interval_secs: u64,
    /// Number of rate-limit tokens per extent read.
    pub tokens_per_extent: u64,
}

impl Default for ScrubConfig {
    fn default() -> Self {
        Self {
            interval_secs: 86400,
            tokens_per_extent: 1,
        }
    }
}

/// Result of scrubbing a single disk.
#[derive(Debug, Clone)]
pub struct ScrubResult {
    /// Disk that was scrubbed.
    pub disk_id: DiskId,
    /// Total extents checked.
    pub extents_checked: u64,
    /// Number of extents with checksum errors.
    pub checksum_errors: u64,
    /// Number of extents that could not be read.
    pub read_errors: u64,
    /// Number of missing metadata files.
    pub metadata_errors: u64,
    /// Duration of the scrub pass.
    pub duration: std::time::Duration,
}

/// An extent entry for scrubbing.
#[derive(Debug, Clone)]
pub struct ScrubExtentEntry {
    pub extent_id: ExtentId,
    pub disk_id: DiskId,
    pub expected_checksum: String,
    pub version: u64,
}

/// Trait for reading extent data during scrub (testability).
#[allow(dead_code)]
pub trait ExtentReader: Send + Sync {
    /// Read extent data and return (data, computed_checksum).
    fn read_extent(
        &self,
        disk_id: DiskId,
        extent_id: ExtentId,
        version: u64,
    ) -> Result<(Vec<u8>, String), String>;

    /// List all extents on a disk.
    fn list_extents(&self, disk_id: DiskId) -> Vec<ScrubExtentEntry>;

    /// List all disk IDs.
    fn list_disks(&self) -> Vec<DiskId>;
}

/// Notification sent when scrub detects corruption.
#[derive(Debug, Clone)]
pub struct CorruptionNotification {
    pub disk_id: DiskId,
    pub extent_id: ExtentId,
    pub reason: CorruptionReason,
}

/// Reason for corruption detection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CorruptionReason {
    ChecksumMismatch { expected: String, actual: String },
    ReadError(String),
    MissingMetadata,
}

/// Background scrub worker (P7.1).
///
/// Periodically scans all extent files, verifies checksum integrity,
/// and reports corruption through a channel for repair.
#[derive(Debug)]
pub struct ScrubWorker {
    config: ScrubConfig,
    results: parking_lot::Mutex<Vec<ScrubResult>>,
}

impl ScrubWorker {
    /// Create a new scrub worker.
    pub fn new(config: ScrubConfig) -> Self {
        Self {
            config,
            results: parking_lot::Mutex::new(Vec::new()),
        }
    }

    /// Get configuration.
    pub fn config(&self) -> &ScrubConfig {
        &self.config
    }

    /// Perform a single scrub pass over all disks.
    ///
    /// Reads each extent, verifies checksum, and sends corruption notifications.
    pub async fn scrub_pass(
        &self,
        reader: &dyn ExtentReader,
        rate_limiter: &TokenBucket,
        corruption_tx: &mpsc::Sender<CorruptionNotification>,
    ) -> Vec<ScrubResult> {
        let disks = reader.list_disks();
        let mut results = Vec::new();

        for disk_id in disks {
            let result = self
                .scrub_disk(disk_id, reader, rate_limiter, corruption_tx)
                .await;
            results.push(result);
        }

        let mut stored = self.results.lock();
        *stored = results.clone();

        info!(
            disk_count = results.len(),
            total_checked = results.iter().map(|r| r.extents_checked).sum::<u64>(),
            total_errors = results
                .iter()
                .map(|r| r.checksum_errors + r.read_errors + r.metadata_errors)
                .sum::<u64>(),
            "scrub pass complete"
        );

        results
    }

    /// Scrub a single disk.
    async fn scrub_disk(
        &self,
        disk_id: DiskId,
        reader: &dyn ExtentReader,
        rate_limiter: &TokenBucket,
        corruption_tx: &mpsc::Sender<CorruptionNotification>,
    ) -> ScrubResult {
        let start = Instant::now();
        let extents = reader.list_extents(disk_id);
        let mut result = ScrubResult {
            disk_id,
            extents_checked: 0,
            checksum_errors: 0,
            read_errors: 0,
            metadata_errors: 0,
            duration: std::time::Duration::ZERO,
        };

        for entry in &extents {
            rate_limiter.acquire(self.config.tokens_per_extent).await;
            result.extents_checked += 1;

            match reader.read_extent(disk_id, entry.extent_id, entry.version) {
                Ok((_data, computed_checksum)) => {
                    if computed_checksum != entry.expected_checksum {
                        result.checksum_errors += 1;
                        let notification = CorruptionNotification {
                            disk_id,
                            extent_id: entry.extent_id,
                            reason: CorruptionReason::ChecksumMismatch {
                                expected: entry.expected_checksum.clone(),
                                actual: computed_checksum,
                            },
                        };
                        if corruption_tx.send(notification).await.is_err() {
                            warn!(
                                %disk_id,
                                extent_id = %entry.extent_id,
                                "failed to send corruption notification — channel closed"
                            );
                        }
                    }
                }
                Err(e) => {
                    if e.contains("metadata") {
                        result.metadata_errors += 1;
                        let notification = CorruptionNotification {
                            disk_id,
                            extent_id: entry.extent_id,
                            reason: CorruptionReason::MissingMetadata,
                        };
                        let _ = corruption_tx.send(notification).await;
                    } else {
                        result.read_errors += 1;
                        let notification = CorruptionNotification {
                            disk_id,
                            extent_id: entry.extent_id,
                            reason: CorruptionReason::ReadError(e),
                        };
                        let _ = corruption_tx.send(notification).await;
                    }
                }
            }

            debug!(
                %disk_id,
                extent_id = %entry.extent_id,
                checked = result.extents_checked,
                "scrub progress"
            );
        }

        result.duration = start.elapsed();

        info!(
            %disk_id,
            extents_checked = result.extents_checked,
            checksum_errors = result.checksum_errors,
            read_errors = result.read_errors,
            metadata_errors = result.metadata_errors,
            duration_ms = result.duration.as_millis() as u64,
            "disk scrub complete"
        );

        result
    }

    /// Get the most recent scrub results.
    pub fn last_results(&self) -> Vec<ScrubResult> {
        self.results.lock().clone()
    }

    /// Run the scrub worker in a loop until the cancellation token fires.
    pub async fn run(
        &self,
        reader: &dyn ExtentReader,
        rate_limiter: &TokenBucket,
        corruption_tx: &mpsc::Sender<CorruptionNotification>,
        mut cancel: tokio::sync::watch::Receiver<bool>,
    ) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(self.config.interval_secs)) => {
                    self.scrub_pass(reader, rate_limiter, corruption_tx).await;
                }
                _ = cancel.changed() => {
                    if *cancel.borrow() {
                        info!("scrub worker shutting down");
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeExtentReader {
        disks: Vec<DiskId>,
        extents: Vec<ScrubExtentEntry>,
        read_results: parking_lot::Mutex<
            std::collections::HashMap<ExtentId, Result<(Vec<u8>, String), String>>,
        >,
    }

    impl FakeExtentReader {
        fn new() -> Self {
            Self {
                disks: Vec::new(),
                extents: Vec::new(),
                read_results: parking_lot::Mutex::new(std::collections::HashMap::new()),
            }
        }

        fn with_disk(mut self, disk_id: DiskId) -> Self {
            self.disks.push(disk_id);
            self
        }

        fn with_extent(
            mut self,
            entry: ScrubExtentEntry,
            result: Result<(Vec<u8>, String), String>,
        ) -> Self {
            let eid = entry.extent_id;
            self.extents.push(entry);
            self.read_results.lock().insert(eid, result);
            self
        }
    }

    impl ExtentReader for FakeExtentReader {
        fn read_extent(
            &self,
            _disk_id: DiskId,
            extent_id: ExtentId,
            _version: u64,
        ) -> Result<(Vec<u8>, String), String> {
            let results = self.read_results.lock();
            results
                .get(&extent_id)
                .cloned()
                .unwrap_or(Err("extent not found".into()))
        }

        fn list_extents(&self, disk_id: DiskId) -> Vec<ScrubExtentEntry> {
            self.extents
                .iter()
                .filter(|e| e.disk_id == disk_id)
                .cloned()
                .collect()
        }

        fn list_disks(&self) -> Vec<DiskId> {
            self.disks.clone()
        }
    }

    #[test]
    fn test_scrub_config_default() {
        let config = ScrubConfig::default();
        assert_eq!(config.interval_secs, 86400);
        assert_eq!(config.tokens_per_extent, 1);
    }

    #[test]
    fn test_scrub_config_custom() {
        let config = ScrubConfig {
            interval_secs: 3600,
            tokens_per_extent: 2,
        };
        assert_eq!(config.interval_secs, 3600);
        assert_eq!(config.tokens_per_extent, 2);
    }

    #[test]
    fn test_scrub_result_debug() {
        let result = ScrubResult {
            disk_id: DiskId::generate(),
            extents_checked: 100,
            checksum_errors: 2,
            read_errors: 1,
            metadata_errors: 0,
            duration: std::time::Duration::from_secs(10),
        };
        let debug = format!("{:?}", result);
        assert!(debug.contains("ScrubResult"));
    }

    #[test]
    fn test_corruption_reason_checksum_mismatch() {
        let reason = CorruptionReason::ChecksumMismatch {
            expected: "abc".into(),
            actual: "def".into(),
        };
        assert_eq!(
            reason,
            CorruptionReason::ChecksumMismatch {
                expected: "abc".into(),
                actual: "def".into()
            }
        );
    }

    #[test]
    fn test_corruption_reason_read_error() {
        let reason = CorruptionReason::ReadError("io fail".into());
        assert_eq!(reason, CorruptionReason::ReadError("io fail".into()));
    }

    #[test]
    fn test_corruption_reason_missing_metadata() {
        let reason = CorruptionReason::MissingMetadata;
        assert_eq!(reason, CorruptionReason::MissingMetadata);
    }

    #[test]
    fn test_corruption_notification_debug() {
        let note = CorruptionNotification {
            disk_id: DiskId::generate(),
            extent_id: ExtentId::generate(),
            reason: CorruptionReason::MissingMetadata,
        };
        let debug = format!("{:?}", note);
        assert!(debug.contains("CorruptionNotification"));
    }

    #[test]
    fn test_scrub_worker_new() {
        let worker = ScrubWorker::new(ScrubConfig::default());
        assert_eq!(worker.config().interval_secs, 86400);
        assert!(worker.last_results().is_empty());
    }

    #[tokio::test]
    async fn test_scrub_pass_no_disks() {
        let worker = ScrubWorker::new(ScrubConfig::default());
        let reader = FakeExtentReader::new();
        let rate_limiter = TokenBucket::new(1000, 1000);
        let (tx, _rx) = mpsc::channel(100);

        let results = worker.scrub_pass(&reader, &rate_limiter, &tx).await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_scrub_pass_healthy_extents() {
        let disk_id = DiskId::generate();
        let eid = ExtentId::generate();
        let checksum = "abc123".to_string();

        let reader = FakeExtentReader::new().with_disk(disk_id).with_extent(
            ScrubExtentEntry {
                extent_id: eid,
                disk_id,
                expected_checksum: checksum.clone(),
                version: 1,
            },
            Ok((vec![1, 2, 3], checksum)),
        );

        let worker = ScrubWorker::new(ScrubConfig::default());
        let rate_limiter = TokenBucket::new(1000, 1000);
        let (tx, _rx) = mpsc::channel(100);

        let results = worker.scrub_pass(&reader, &rate_limiter, &tx).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].extents_checked, 1);
        assert_eq!(results[0].checksum_errors, 0);
        assert_eq!(results[0].read_errors, 0);
    }

    #[tokio::test]
    async fn test_scrub_pass_checksum_mismatch() {
        let disk_id = DiskId::generate();
        let eid = ExtentId::generate();

        let reader = FakeExtentReader::new().with_disk(disk_id).with_extent(
            ScrubExtentEntry {
                extent_id: eid,
                disk_id,
                expected_checksum: "expected".to_string(),
                version: 1,
            },
            Ok((vec![1, 2, 3], "actual_different".to_string())),
        );

        let worker = ScrubWorker::new(ScrubConfig::default());
        let rate_limiter = TokenBucket::new(1000, 1000);
        let (tx, mut rx) = mpsc::channel(100);

        let results = worker.scrub_pass(&reader, &rate_limiter, &tx).await;
        assert_eq!(results[0].checksum_errors, 1);

        let notification = rx
            .try_recv()
            .expect("should receive corruption notification");
        assert_eq!(notification.extent_id, eid);
        match notification.reason {
            CorruptionReason::ChecksumMismatch { expected, actual } => {
                assert_eq!(expected, "expected");
                assert_eq!(actual, "actual_different");
            }
            _ => panic!("expected ChecksumMismatch"),
        }
    }

    #[tokio::test]
    async fn test_scrub_pass_read_error() {
        let disk_id = DiskId::generate();
        let eid = ExtentId::generate();

        let reader = FakeExtentReader::new().with_disk(disk_id).with_extent(
            ScrubExtentEntry {
                extent_id: eid,
                disk_id,
                expected_checksum: "abc".to_string(),
                version: 1,
            },
            Err("io error".to_string()),
        );

        let worker = ScrubWorker::new(ScrubConfig::default());
        let rate_limiter = TokenBucket::new(1000, 1000);
        let (tx, mut rx) = mpsc::channel(100);

        let results = worker.scrub_pass(&reader, &rate_limiter, &tx).await;
        assert_eq!(results[0].read_errors, 1);

        let notification = rx.try_recv().expect("should receive notification");
        match notification.reason {
            CorruptionReason::ReadError(msg) => assert!(msg.contains("io error")),
            _ => panic!("expected ReadError"),
        }
    }

    #[tokio::test]
    async fn test_scrub_pass_metadata_error() {
        let disk_id = DiskId::generate();
        let eid = ExtentId::generate();

        let reader = FakeExtentReader::new().with_disk(disk_id).with_extent(
            ScrubExtentEntry {
                extent_id: eid,
                disk_id,
                expected_checksum: "abc".to_string(),
                version: 1,
            },
            Err("metadata not found".to_string()),
        );

        let worker = ScrubWorker::new(ScrubConfig::default());
        let rate_limiter = TokenBucket::new(1000, 1000);
        let (tx, mut rx) = mpsc::channel(100);

        let results = worker.scrub_pass(&reader, &rate_limiter, &tx).await;
        assert_eq!(results[0].metadata_errors, 1);

        let notification = rx.try_recv().expect("should receive notification");
        assert_eq!(notification.reason, CorruptionReason::MissingMetadata);
    }

    #[tokio::test]
    async fn test_scrub_pass_multiple_disks() {
        let disk1 = DiskId::generate();
        let disk2 = DiskId::generate();
        let eid1 = ExtentId::generate();
        let eid2 = ExtentId::generate();

        let reader = FakeExtentReader::new()
            .with_disk(disk1)
            .with_disk(disk2)
            .with_extent(
                ScrubExtentEntry {
                    extent_id: eid1,
                    disk_id: disk1,
                    expected_checksum: "c1".to_string(),
                    version: 1,
                },
                Ok((vec![1], "c1".to_string())),
            )
            .with_extent(
                ScrubExtentEntry {
                    extent_id: eid2,
                    disk_id: disk2,
                    expected_checksum: "c2".to_string(),
                    version: 1,
                },
                Ok((vec![2], "c2".to_string())),
            );

        let worker = ScrubWorker::new(ScrubConfig::default());
        let rate_limiter = TokenBucket::new(1000, 1000);
        let (tx, _rx) = mpsc::channel(100);

        let results = worker.scrub_pass(&reader, &rate_limiter, &tx).await;
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_scrub_last_results_stored() {
        let disk_id = DiskId::generate();

        let reader = FakeExtentReader::new().with_disk(disk_id);

        let worker = ScrubWorker::new(ScrubConfig::default());
        let rate_limiter = TokenBucket::new(1000, 1000);
        let (tx, _rx) = mpsc::channel(100);

        assert!(worker.last_results().is_empty());
        worker.scrub_pass(&reader, &rate_limiter, &tx).await;
        assert_eq!(worker.last_results().len(), 1);
    }

    #[tokio::test]
    async fn test_scrub_worker_run_cancellation() {
        let worker = ScrubWorker::new(ScrubConfig {
            interval_secs: 3600,
            tokens_per_extent: 1,
        });
        let reader = FakeExtentReader::new();
        let rate_limiter = TokenBucket::new(1000, 1000);
        let (tx, _rx) = mpsc::channel(100);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        let handle = tokio::spawn(async move {
            worker.run(&reader, &rate_limiter, &tx, cancel_rx).await;
        });

        // Signal cancellation
        cancel_tx.send(true).unwrap_or(());
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        // The worker should have exited
        let result = tokio::time::timeout(tokio::time::Duration::from_millis(500), handle).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_scrub_extent_entry_clone() {
        let entry = ScrubExtentEntry {
            extent_id: ExtentId::generate(),
            disk_id: DiskId::generate(),
            expected_checksum: "abc".to_string(),
            version: 1,
        };
        let cloned = entry.clone();
        assert_eq!(cloned.extent_id, entry.extent_id);
        assert_eq!(cloned.expected_checksum, entry.expected_checksum);
    }

    #[test]
    fn test_scrub_config_clone() {
        let config = ScrubConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.interval_secs, config.interval_secs);
    }

    #[test]
    fn test_scrub_result_clone() {
        let result = ScrubResult {
            disk_id: DiskId::generate(),
            extents_checked: 10,
            checksum_errors: 1,
            read_errors: 0,
            metadata_errors: 0,
            duration: std::time::Duration::from_secs(5),
        };
        let cloned = result.clone();
        assert_eq!(cloned.extents_checked, result.extents_checked);
    }

    #[test]
    fn test_scrub_worker_debug() {
        let worker = ScrubWorker::new(ScrubConfig::default());
        let debug = format!("{:?}", worker);
        assert!(debug.contains("ScrubWorker"));
    }

    #[tokio::test]
    async fn test_scrub_pass_mixed_results() {
        let disk_id = DiskId::generate();
        let eid1 = ExtentId::generate();
        let eid2 = ExtentId::generate();
        let eid3 = ExtentId::generate();

        let reader = FakeExtentReader::new()
            .with_disk(disk_id)
            .with_extent(
                ScrubExtentEntry {
                    extent_id: eid1,
                    disk_id,
                    expected_checksum: "good".to_string(),
                    version: 1,
                },
                Ok((vec![1], "good".to_string())),
            )
            .with_extent(
                ScrubExtentEntry {
                    extent_id: eid2,
                    disk_id,
                    expected_checksum: "expected".to_string(),
                    version: 1,
                },
                Ok((vec![2], "mismatch".to_string())),
            )
            .with_extent(
                ScrubExtentEntry {
                    extent_id: eid3,
                    disk_id,
                    expected_checksum: "x".to_string(),
                    version: 1,
                },
                Err("read failure".to_string()),
            );

        let worker = ScrubWorker::new(ScrubConfig::default());
        let rate_limiter = TokenBucket::new(1000, 1000);
        let (tx, _rx) = mpsc::channel(100);

        let results = worker.scrub_pass(&reader, &rate_limiter, &tx).await;
        assert_eq!(results[0].extents_checked, 3);
        assert_eq!(results[0].checksum_errors, 1);
        assert_eq!(results[0].read_errors, 1);
    }
}
