//! Repair workers for re-replication and erasure-code rebuild (P7.2–P7.4, §5.13).
//!
//! Handles both replication repair (read healthy copy, write replacement) and
//! EC rebuild (read K fragments, reconstruct missing fragment). Queue-based
//! processing with configurable concurrency.

use std::collections::VecDeque;

use blockyard_common::{DiskId, ExtentId};
use bytes::Bytes;
use parking_lot::Mutex;
use tracing::{debug, info, warn};

use crate::background::rate_limit::TokenBucket;

/// Configuration for the repair worker.
#[derive(Debug, Clone)]
pub struct RepairConfig {
    /// Maximum concurrent repair operations.
    pub max_concurrent: usize,
    /// Tokens consumed per repair operation.
    pub tokens_per_repair: u64,
}

impl Default for RepairConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 4,
            tokens_per_repair: 10,
        }
    }
}

/// Type of repair to perform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepairType {
    /// Replicated extent: read from healthy replica, write to new disk.
    Replication {
        /// Disk IDs that have healthy copies.
        healthy_sources: Vec<DiskId>,
    },
    /// Erasure-coded extent: read K fragments, reconstruct missing.
    ErasureCode {
        /// K (data fragments count).
        data_count: usize,
        /// M (parity fragments count).
        parity_count: usize,
        /// Available fragment indices and their source disks.
        available_fragments: Vec<(usize, DiskId)>,
        /// Original data length for decoding.
        original_data_len: usize,
    },
}

/// A request to repair an extent.
#[derive(Debug, Clone)]
pub struct RepairRequest {
    /// Extent to repair.
    pub extent_id: ExtentId,
    /// Version of the extent.
    pub version: u64,
    /// Disk where the extent should be written.
    pub target_disk_id: DiskId,
    /// Type of repair.
    pub repair_type: RepairType,
    /// Priority (lower = higher priority).
    pub priority: u32,
}

/// Result of a single repair operation.
#[derive(Debug, Clone)]
pub struct RepairOutcome {
    /// Extent that was repaired.
    pub extent_id: ExtentId,
    /// Target disk where the repair was written.
    pub target_disk_id: DiskId,
    /// Whether the repair succeeded.
    pub success: bool,
    /// Error message if failed.
    pub error: Option<String>,
}

/// Trait for reading extent data from a healthy source (testability).
pub trait RepairExtentReader: Send + Sync {
    /// Read a full extent from a source disk.
    fn read_extent(
        &self,
        source_disk: DiskId,
        extent_id: ExtentId,
        version: u64,
    ) -> Result<Bytes, String>;
}

/// Trait for reading EC fragments from source disks (testability).
pub trait FragmentReader: Send + Sync {
    /// Read a single fragment from a source disk.
    fn read_fragment(
        &self,
        source_disk: DiskId,
        extent_id: ExtentId,
        fragment_index: usize,
    ) -> Result<Bytes, String>;
}

/// Trait for writing repaired extent data to a target disk (testability).
pub trait RepairExtentWriter: Send + Sync {
    /// Write repaired extent data to the target disk.
    fn write_extent(
        &self,
        target_disk: DiskId,
        extent_id: ExtentId,
        version: u64,
        data: &[u8],
    ) -> Result<(), String>;
}

/// Trait for EC reconstruction (testability).
pub trait EcReconstructor: Send + Sync {
    /// Reconstruct data from available fragments.
    fn reconstruct(
        &self,
        data_count: usize,
        parity_count: usize,
        fragments: Vec<Option<Bytes>>,
        original_len: usize,
    ) -> Result<Bytes, String>;
}

/// Background repair worker (P7.3, P7.4).
///
/// Processes a queue of repair requests, reading from healthy sources
/// and writing repaired data to target disks.
#[derive(Debug)]
pub struct RepairWorker {
    config: RepairConfig,
    queue: Mutex<VecDeque<RepairRequest>>,
    completed: Mutex<Vec<RepairOutcome>>,
}

impl RepairWorker {
    /// Create a new repair worker.
    pub fn new(config: RepairConfig) -> Self {
        Self {
            config,
            queue: Mutex::new(VecDeque::new()),
            completed: Mutex::new(Vec::new()),
        }
    }

    /// Get configuration.
    pub fn config(&self) -> &RepairConfig {
        &self.config
    }

    /// Enqueue a repair request.
    pub fn enqueue(&self, request: RepairRequest) {
        let mut queue = self.queue.lock();
        // Insert in priority order (lower priority number = higher priority)
        let pos = queue
            .iter()
            .position(|r| r.priority > request.priority)
            .unwrap_or(queue.len());
        queue.insert(pos, request);
        debug!(queue_len = queue.len(), "repair request enqueued");
    }

    /// Get the current queue length.
    pub fn queue_len(&self) -> usize {
        self.queue.lock().len()
    }

    /// Get completed repair outcomes.
    pub fn completed(&self) -> Vec<RepairOutcome> {
        self.completed.lock().clone()
    }

    /// Process the next repair request from the queue.
    pub async fn process_next(
        &self,
        extent_reader: &dyn RepairExtentReader,
        fragment_reader: &dyn FragmentReader,
        extent_writer: &dyn RepairExtentWriter,
        ec_reconstructor: &dyn EcReconstructor,
        rate_limiter: &TokenBucket,
    ) -> Option<RepairOutcome> {
        let request = {
            let mut queue = self.queue.lock();
            queue.pop_front()
        }?;

        rate_limiter.acquire(self.config.tokens_per_repair).await;

        let outcome = match &request.repair_type {
            RepairType::Replication { healthy_sources } => {
                self.repair_replication(
                    &request,
                    healthy_sources,
                    extent_reader,
                    extent_writer,
                )
            }
            RepairType::ErasureCode {
                data_count,
                parity_count,
                available_fragments,
                original_data_len,
            } => self.repair_ec(
                &request,
                *data_count,
                *parity_count,
                available_fragments,
                *original_data_len,
                fragment_reader,
                extent_writer,
                ec_reconstructor,
            ),
        };

        self.completed.lock().push(outcome.clone());

        Some(outcome)
    }

    /// Process all queued requests.
    pub async fn process_all(
        &self,
        extent_reader: &dyn RepairExtentReader,
        fragment_reader: &dyn FragmentReader,
        extent_writer: &dyn RepairExtentWriter,
        ec_reconstructor: &dyn EcReconstructor,
        rate_limiter: &TokenBucket,
    ) -> Vec<RepairOutcome> {
        let mut outcomes = Vec::new();
        while self.queue_len() > 0 {
            if let Some(outcome) = self
                .process_next(
                    extent_reader,
                    fragment_reader,
                    extent_writer,
                    ec_reconstructor,
                    rate_limiter,
                )
                .await
            {
                outcomes.push(outcome);
            }
        }
        outcomes
    }

    /// Repair via replication: read from healthy source, write to target.
    fn repair_replication(
        &self,
        request: &RepairRequest,
        healthy_sources: &[DiskId],
        extent_reader: &dyn RepairExtentReader,
        extent_writer: &dyn RepairExtentWriter,
    ) -> RepairOutcome {
        for source in healthy_sources {
            match extent_reader.read_extent(*source, request.extent_id, request.version) {
                Ok(data) => {
                    match extent_writer.write_extent(
                        request.target_disk_id,
                        request.extent_id,
                        request.version,
                        &data,
                    ) {
                        Ok(()) => {
                            info!(
                                extent_id = %request.extent_id,
                                source = %source,
                                target = %request.target_disk_id,
                                "replication repair successful"
                            );
                            return RepairOutcome {
                                extent_id: request.extent_id,
                                target_disk_id: request.target_disk_id,
                                success: true,
                                error: None,
                            };
                        }
                        Err(e) => {
                            warn!(
                                extent_id = %request.extent_id,
                                target = %request.target_disk_id,
                                error = %e,
                                "write failed during replication repair"
                            );
                        }
                    }
                }
                Err(e) => {
                    debug!(
                        source = %source,
                        extent_id = %request.extent_id,
                        error = %e,
                        "read from source failed, trying next"
                    );
                }
            }
        }

        RepairOutcome {
            extent_id: request.extent_id,
            target_disk_id: request.target_disk_id,
            success: false,
            error: Some("all healthy sources failed".into()),
        }
    }

    /// Repair via erasure coding: read K fragments, reconstruct, write.
    #[allow(clippy::too_many_arguments)]
    fn repair_ec(
        &self,
        request: &RepairRequest,
        data_count: usize,
        parity_count: usize,
        available_fragments: &[(usize, DiskId)],
        original_data_len: usize,
        fragment_reader: &dyn FragmentReader,
        extent_writer: &dyn RepairExtentWriter,
        ec_reconstructor: &dyn EcReconstructor,
    ) -> RepairOutcome {
        let total = data_count + parity_count;
        let mut fragments: Vec<Option<Bytes>> = vec![None; total];

        for (idx, disk_id) in available_fragments {
            match fragment_reader.read_fragment(*disk_id, request.extent_id, *idx) {
                Ok(data) => {
                    if *idx < total {
                        fragments[*idx] = Some(data);
                    }
                }
                Err(e) => {
                    debug!(
                        fragment_idx = idx,
                        disk = %disk_id,
                        error = %e,
                        "failed to read EC fragment"
                    );
                }
            }
        }

        let available_count = fragments.iter().filter(|f| f.is_some()).count();
        if available_count < data_count {
            return RepairOutcome {
                extent_id: request.extent_id,
                target_disk_id: request.target_disk_id,
                success: false,
                error: Some(format!(
                    "insufficient fragments: have {available_count}, need {data_count}"
                )),
            };
        }

        match ec_reconstructor.reconstruct(data_count, parity_count, fragments, original_data_len) {
            Ok(reconstructed) => {
                match extent_writer.write_extent(
                    request.target_disk_id,
                    request.extent_id,
                    request.version,
                    &reconstructed,
                ) {
                    Ok(()) => {
                        info!(
                            extent_id = %request.extent_id,
                            target = %request.target_disk_id,
                            "EC repair successful"
                        );
                        RepairOutcome {
                            extent_id: request.extent_id,
                            target_disk_id: request.target_disk_id,
                            success: true,
                            error: None,
                        }
                    }
                    Err(e) => RepairOutcome {
                        extent_id: request.extent_id,
                        target_disk_id: request.target_disk_id,
                        success: false,
                        error: Some(format!("EC repair write failed: {e}")),
                    },
                }
            }
            Err(e) => RepairOutcome {
                extent_id: request.extent_id,
                target_disk_id: request.target_disk_id,
                success: false,
                error: Some(format!("EC reconstruction failed: {e}")),
            },
        }
    }

    /// Run the repair worker in a loop until cancellation.
    pub async fn run(
        &self,
        extent_reader: &dyn RepairExtentReader,
        fragment_reader: &dyn FragmentReader,
        extent_writer: &dyn RepairExtentWriter,
        ec_reconstructor: &dyn EcReconstructor,
        rate_limiter: &TokenBucket,
        mut cancel: tokio::sync::watch::Receiver<bool>,
    ) {
        loop {
            if self.queue_len() > 0 {
                self.process_next(
                    extent_reader,
                    fragment_reader,
                    extent_writer,
                    ec_reconstructor,
                    rate_limiter,
                )
                .await;
            } else {
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {}
                    _ = cancel.changed() => {
                        if *cancel.borrow() {
                            info!("repair worker shutting down");
                            return;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct FakeRepairReader {
        data: parking_lot::Mutex<HashMap<(DiskId, ExtentId), Bytes>>,
    }

    impl FakeRepairReader {
        fn new() -> Self {
            Self {
                data: parking_lot::Mutex::new(HashMap::new()),
            }
        }

        fn add(&self, disk: DiskId, extent: ExtentId, data: Vec<u8>) {
            self.data.lock().insert((disk, extent), Bytes::from(data));
        }
    }

    impl RepairExtentReader for FakeRepairReader {
        fn read_extent(
            &self,
            source_disk: DiskId,
            extent_id: ExtentId,
            _version: u64,
        ) -> Result<Bytes, String> {
            self.data
                .lock()
                .get(&(source_disk, extent_id))
                .cloned()
                .ok_or_else(|| "not found".into())
        }
    }

    struct FakeFragmentReader {
        fragments: parking_lot::Mutex<HashMap<(DiskId, ExtentId, usize), Bytes>>,
    }

    impl FakeFragmentReader {
        fn new() -> Self {
            Self {
                fragments: parking_lot::Mutex::new(HashMap::new()),
            }
        }

        fn add(&self, disk: DiskId, extent: ExtentId, idx: usize, data: Vec<u8>) {
            self.fragments
                .lock()
                .insert((disk, extent, idx), Bytes::from(data));
        }
    }

    impl FragmentReader for FakeFragmentReader {
        fn read_fragment(
            &self,
            source_disk: DiskId,
            extent_id: ExtentId,
            fragment_index: usize,
        ) -> Result<Bytes, String> {
            self.fragments
                .lock()
                .get(&(source_disk, extent_id, fragment_index))
                .cloned()
                .ok_or_else(|| "fragment not found".into())
        }
    }

    struct FakeRepairWriter {
        written: parking_lot::Mutex<Vec<(DiskId, ExtentId, Vec<u8>)>>,
        fail: parking_lot::Mutex<bool>,
    }

    impl FakeRepairWriter {
        fn new() -> Self {
            Self {
                written: parking_lot::Mutex::new(Vec::new()),
                fail: parking_lot::Mutex::new(false),
            }
        }

        fn set_fail(&self, fail: bool) {
            *self.fail.lock() = fail;
        }

        fn written(&self) -> Vec<(DiskId, ExtentId, Vec<u8>)> {
            self.written.lock().clone()
        }
    }

    impl RepairExtentWriter for FakeRepairWriter {
        fn write_extent(
            &self,
            target_disk: DiskId,
            extent_id: ExtentId,
            _version: u64,
            data: &[u8],
        ) -> Result<(), String> {
            if *self.fail.lock() {
                return Err("write failed".into());
            }
            self.written
                .lock()
                .push((target_disk, extent_id, data.to_vec()));
            Ok(())
        }
    }

    struct FakeEcReconstructor {
        result: parking_lot::Mutex<Option<Result<Bytes, String>>>,
    }

    impl FakeEcReconstructor {
        fn new(result: Result<Bytes, String>) -> Self {
            Self {
                result: parking_lot::Mutex::new(Some(result)),
            }
        }
    }

    impl EcReconstructor for FakeEcReconstructor {
        fn reconstruct(
            &self,
            _data_count: usize,
            _parity_count: usize,
            _fragments: Vec<Option<Bytes>>,
            _original_len: usize,
        ) -> Result<Bytes, String> {
            self.result
                .lock()
                .take()
                .unwrap_or(Ok(Bytes::from_static(b"reconstructed")))
        }
    }

    #[test]
    fn test_repair_config_default() {
        let config = RepairConfig::default();
        assert_eq!(config.max_concurrent, 4);
        assert_eq!(config.tokens_per_repair, 10);
    }

    #[test]
    fn test_repair_type_replication_eq() {
        let a = RepairType::Replication {
            healthy_sources: vec![DiskId::generate()],
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_repair_type_ec_eq() {
        let a = RepairType::ErasureCode {
            data_count: 4,
            parity_count: 2,
            available_fragments: vec![],
            original_data_len: 100,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_repair_request_debug() {
        let req = RepairRequest {
            extent_id: ExtentId::generate(),
            version: 1,
            target_disk_id: DiskId::generate(),
            repair_type: RepairType::Replication {
                healthy_sources: vec![],
            },
            priority: 0,
        };
        let debug = format!("{:?}", req);
        assert!(debug.contains("RepairRequest"));
    }

    #[test]
    fn test_repair_outcome_debug() {
        let outcome = RepairOutcome {
            extent_id: ExtentId::generate(),
            target_disk_id: DiskId::generate(),
            success: true,
            error: None,
        };
        let debug = format!("{:?}", outcome);
        assert!(debug.contains("RepairOutcome"));
    }

    #[test]
    fn test_enqueue_and_queue_len() {
        let worker = RepairWorker::new(RepairConfig::default());
        assert_eq!(worker.queue_len(), 0);

        worker.enqueue(RepairRequest {
            extent_id: ExtentId::generate(),
            version: 1,
            target_disk_id: DiskId::generate(),
            repair_type: RepairType::Replication {
                healthy_sources: vec![],
            },
            priority: 5,
        });
        assert_eq!(worker.queue_len(), 1);
    }

    #[test]
    fn test_enqueue_priority_ordering() {
        let worker = RepairWorker::new(RepairConfig::default());

        let e1 = ExtentId::generate();
        let e2 = ExtentId::generate();
        let e3 = ExtentId::generate();

        worker.enqueue(RepairRequest {
            extent_id: e1,
            version: 1,
            target_disk_id: DiskId::generate(),
            repair_type: RepairType::Replication {
                healthy_sources: vec![],
            },
            priority: 10,
        });
        worker.enqueue(RepairRequest {
            extent_id: e2,
            version: 1,
            target_disk_id: DiskId::generate(),
            repair_type: RepairType::Replication {
                healthy_sources: vec![],
            },
            priority: 1,
        });
        worker.enqueue(RepairRequest {
            extent_id: e3,
            version: 1,
            target_disk_id: DiskId::generate(),
            repair_type: RepairType::Replication {
                healthy_sources: vec![],
            },
            priority: 5,
        });

        let queue = worker.queue.lock();
        assert_eq!(queue[0].extent_id, e2); // priority 1 first
        assert_eq!(queue[1].extent_id, e3); // priority 5 next
        assert_eq!(queue[2].extent_id, e1); // priority 10 last
    }

    #[tokio::test]
    async fn test_process_next_empty_queue() {
        let worker = RepairWorker::new(RepairConfig::default());
        let reader = FakeRepairReader::new();
        let frag_reader = FakeFragmentReader::new();
        let writer = FakeRepairWriter::new();
        let ec = FakeEcReconstructor::new(Ok(Bytes::new()));
        let limiter = TokenBucket::new(1000, 1000);

        let result = worker
            .process_next(&reader, &frag_reader, &writer, &ec, &limiter)
            .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_replication_repair_success() {
        let worker = RepairWorker::new(RepairConfig::default());
        let reader = FakeRepairReader::new();
        let writer = FakeRepairWriter::new();
        let frag_reader = FakeFragmentReader::new();
        let ec = FakeEcReconstructor::new(Ok(Bytes::new()));
        let limiter = TokenBucket::new(1000, 1000);

        let source_disk = DiskId::generate();
        let target_disk = DiskId::generate();
        let extent_id = ExtentId::generate();
        let data = vec![1, 2, 3, 4];
        reader.add(source_disk, extent_id, data.clone());

        worker.enqueue(RepairRequest {
            extent_id,
            version: 1,
            target_disk_id: target_disk,
            repair_type: RepairType::Replication {
                healthy_sources: vec![source_disk],
            },
            priority: 0,
        });

        let outcome = worker
            .process_next(&reader, &frag_reader, &writer, &ec, &limiter)
            .await
            .unwrap();
        assert!(outcome.success);
        assert_eq!(outcome.extent_id, extent_id);

        let written = writer.written();
        assert_eq!(written.len(), 1);
        assert_eq!(written[0].2, data);
    }

    #[tokio::test]
    async fn test_replication_repair_source_failover() {
        let worker = RepairWorker::new(RepairConfig::default());
        let reader = FakeRepairReader::new();
        let writer = FakeRepairWriter::new();
        let frag_reader = FakeFragmentReader::new();
        let ec = FakeEcReconstructor::new(Ok(Bytes::new()));
        let limiter = TokenBucket::new(1000, 1000);

        let bad_source = DiskId::generate();
        let good_source = DiskId::generate();
        let target_disk = DiskId::generate();
        let extent_id = ExtentId::generate();
        let data = vec![5, 6, 7, 8];
        // Only add data for the second source
        reader.add(good_source, extent_id, data.clone());

        worker.enqueue(RepairRequest {
            extent_id,
            version: 1,
            target_disk_id: target_disk,
            repair_type: RepairType::Replication {
                healthy_sources: vec![bad_source, good_source],
            },
            priority: 0,
        });

        let outcome = worker
            .process_next(&reader, &frag_reader, &writer, &ec, &limiter)
            .await
            .unwrap();
        assert!(outcome.success);
    }

    #[tokio::test]
    async fn test_replication_repair_all_sources_fail() {
        let worker = RepairWorker::new(RepairConfig::default());
        let reader = FakeRepairReader::new();
        let writer = FakeRepairWriter::new();
        let frag_reader = FakeFragmentReader::new();
        let ec = FakeEcReconstructor::new(Ok(Bytes::new()));
        let limiter = TokenBucket::new(1000, 1000);

        let extent_id = ExtentId::generate();
        worker.enqueue(RepairRequest {
            extent_id,
            version: 1,
            target_disk_id: DiskId::generate(),
            repair_type: RepairType::Replication {
                healthy_sources: vec![DiskId::generate()],
            },
            priority: 0,
        });

        let outcome = worker
            .process_next(&reader, &frag_reader, &writer, &ec, &limiter)
            .await
            .unwrap();
        assert!(!outcome.success);
        assert!(outcome.error.is_some());
    }

    #[tokio::test]
    async fn test_replication_repair_write_failure() {
        let worker = RepairWorker::new(RepairConfig::default());
        let reader = FakeRepairReader::new();
        let writer = FakeRepairWriter::new();
        let frag_reader = FakeFragmentReader::new();
        let ec = FakeEcReconstructor::new(Ok(Bytes::new()));
        let limiter = TokenBucket::new(1000, 1000);

        let source_disk = DiskId::generate();
        let extent_id = ExtentId::generate();
        reader.add(source_disk, extent_id, vec![1, 2, 3]);
        writer.set_fail(true);

        worker.enqueue(RepairRequest {
            extent_id,
            version: 1,
            target_disk_id: DiskId::generate(),
            repair_type: RepairType::Replication {
                healthy_sources: vec![source_disk],
            },
            priority: 0,
        });

        let outcome = worker
            .process_next(&reader, &frag_reader, &writer, &ec, &limiter)
            .await
            .unwrap();
        assert!(!outcome.success);
    }

    #[tokio::test]
    async fn test_ec_repair_success() {
        let worker = RepairWorker::new(RepairConfig::default());
        let reader = FakeRepairReader::new();
        let frag_reader = FakeFragmentReader::new();
        let writer = FakeRepairWriter::new();
        let limiter = TokenBucket::new(1000, 1000);

        let extent_id = ExtentId::generate();
        let disk0 = DiskId::generate();
        let disk1 = DiskId::generate();
        let target = DiskId::generate();

        frag_reader.add(disk0, extent_id, 0, vec![10, 20]);
        frag_reader.add(disk1, extent_id, 1, vec![30, 40]);

        let reconstructed = Bytes::from_static(b"reconstructed data");
        let ec = FakeEcReconstructor::new(Ok(reconstructed.clone()));

        worker.enqueue(RepairRequest {
            extent_id,
            version: 1,
            target_disk_id: target,
            repair_type: RepairType::ErasureCode {
                data_count: 2,
                parity_count: 1,
                available_fragments: vec![(0, disk0), (1, disk1)],
                original_data_len: 4,
            },
            priority: 0,
        });

        let outcome = worker
            .process_next(&reader, &frag_reader, &writer, &ec, &limiter)
            .await
            .unwrap();
        assert!(outcome.success);

        let written = writer.written();
        assert_eq!(written.len(), 1);
        assert_eq!(written[0].2, reconstructed.to_vec());
    }

    #[tokio::test]
    async fn test_ec_repair_insufficient_fragments() {
        let worker = RepairWorker::new(RepairConfig::default());
        let reader = FakeRepairReader::new();
        let frag_reader = FakeFragmentReader::new();
        let writer = FakeRepairWriter::new();
        let ec = FakeEcReconstructor::new(Ok(Bytes::new()));
        let limiter = TokenBucket::new(1000, 1000);

        let extent_id = ExtentId::generate();
        // data_count=2 but only 1 fragment available and readable
        let disk0 = DiskId::generate();
        frag_reader.add(disk0, extent_id, 0, vec![10, 20]);

        worker.enqueue(RepairRequest {
            extent_id,
            version: 1,
            target_disk_id: DiskId::generate(),
            repair_type: RepairType::ErasureCode {
                data_count: 2,
                parity_count: 1,
                available_fragments: vec![(0, disk0)],
                original_data_len: 4,
            },
            priority: 0,
        });

        let outcome = worker
            .process_next(&reader, &frag_reader, &writer, &ec, &limiter)
            .await
            .unwrap();
        assert!(!outcome.success);
        assert!(outcome.error.as_ref().is_some_and(|e| e.contains("insufficient")));
    }

    #[tokio::test]
    async fn test_ec_repair_reconstruction_failure() {
        let worker = RepairWorker::new(RepairConfig::default());
        let reader = FakeRepairReader::new();
        let frag_reader = FakeFragmentReader::new();
        let writer = FakeRepairWriter::new();
        let limiter = TokenBucket::new(1000, 1000);

        let extent_id = ExtentId::generate();
        let disk0 = DiskId::generate();
        let disk1 = DiskId::generate();
        frag_reader.add(disk0, extent_id, 0, vec![10]);
        frag_reader.add(disk1, extent_id, 1, vec![20]);

        let ec = FakeEcReconstructor::new(Err("codec error".into()));

        worker.enqueue(RepairRequest {
            extent_id,
            version: 1,
            target_disk_id: DiskId::generate(),
            repair_type: RepairType::ErasureCode {
                data_count: 2,
                parity_count: 1,
                available_fragments: vec![(0, disk0), (1, disk1)],
                original_data_len: 2,
            },
            priority: 0,
        });

        let outcome = worker
            .process_next(&reader, &frag_reader, &writer, &ec, &limiter)
            .await
            .unwrap();
        assert!(!outcome.success);
        assert!(outcome.error.as_ref().is_some_and(|e| e.contains("reconstruction")));
    }

    #[tokio::test]
    async fn test_process_all() {
        let worker = RepairWorker::new(RepairConfig::default());
        let reader = FakeRepairReader::new();
        let frag_reader = FakeFragmentReader::new();
        let writer = FakeRepairWriter::new();
        let ec = FakeEcReconstructor::new(Ok(Bytes::new()));
        let limiter = TokenBucket::new(1000, 1000);

        let source = DiskId::generate();
        let target = DiskId::generate();

        for _ in 0..3 {
            let eid = ExtentId::generate();
            reader.add(source, eid, vec![1, 2, 3]);
            worker.enqueue(RepairRequest {
                extent_id: eid,
                version: 1,
                target_disk_id: target,
                repair_type: RepairType::Replication {
                    healthy_sources: vec![source],
                },
                priority: 0,
            });
        }

        let outcomes = worker
            .process_all(&reader, &frag_reader, &writer, &ec, &limiter)
            .await;
        assert_eq!(outcomes.len(), 3);
        assert!(outcomes.iter().all(|o| o.success));
    }

    #[tokio::test]
    async fn test_completed_tracking() {
        let worker = RepairWorker::new(RepairConfig::default());
        let reader = FakeRepairReader::new();
        let frag_reader = FakeFragmentReader::new();
        let writer = FakeRepairWriter::new();
        let ec = FakeEcReconstructor::new(Ok(Bytes::new()));
        let limiter = TokenBucket::new(1000, 1000);

        let source = DiskId::generate();
        let extent_id = ExtentId::generate();
        reader.add(source, extent_id, vec![1]);

        worker.enqueue(RepairRequest {
            extent_id,
            version: 1,
            target_disk_id: DiskId::generate(),
            repair_type: RepairType::Replication {
                healthy_sources: vec![source],
            },
            priority: 0,
        });

        assert!(worker.completed().is_empty());
        worker
            .process_next(&reader, &frag_reader, &writer, &ec, &limiter)
            .await;
        assert_eq!(worker.completed().len(), 1);
    }

    #[test]
    fn test_repair_worker_debug() {
        let worker = RepairWorker::new(RepairConfig::default());
        let debug = format!("{:?}", worker);
        assert!(debug.contains("RepairWorker"));
    }

    #[test]
    fn test_repair_config_clone() {
        let config = RepairConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.max_concurrent, config.max_concurrent);
    }

    #[test]
    fn test_repair_outcome_clone() {
        let outcome = RepairOutcome {
            extent_id: ExtentId::generate(),
            target_disk_id: DiskId::generate(),
            success: true,
            error: None,
        };
        let cloned = outcome.clone();
        assert_eq!(cloned.success, outcome.success);
    }

    #[tokio::test]
    async fn test_ec_repair_write_failure() {
        let worker = RepairWorker::new(RepairConfig::default());
        let reader = FakeRepairReader::new();
        let frag_reader = FakeFragmentReader::new();
        let writer = FakeRepairWriter::new();
        let limiter = TokenBucket::new(1000, 1000);

        let extent_id = ExtentId::generate();
        let disk0 = DiskId::generate();
        let disk1 = DiskId::generate();
        frag_reader.add(disk0, extent_id, 0, vec![10]);
        frag_reader.add(disk1, extent_id, 1, vec![20]);

        writer.set_fail(true);
        let ec = FakeEcReconstructor::new(Ok(Bytes::from_static(b"data")));

        worker.enqueue(RepairRequest {
            extent_id,
            version: 1,
            target_disk_id: DiskId::generate(),
            repair_type: RepairType::ErasureCode {
                data_count: 2,
                parity_count: 1,
                available_fragments: vec![(0, disk0), (1, disk1)],
                original_data_len: 2,
            },
            priority: 0,
        });

        let outcome = worker
            .process_next(&reader, &frag_reader, &writer, &ec, &limiter)
            .await
            .unwrap();
        assert!(!outcome.success);
        assert!(outcome.error.as_ref().is_some_and(|e| e.contains("write failed")));
    }

    #[tokio::test]
    async fn test_repair_worker_run_cancellation() {
        let worker = RepairWorker::new(RepairConfig::default());
        let reader = FakeRepairReader::new();
        let frag_reader = FakeFragmentReader::new();
        let writer = FakeRepairWriter::new();
        let ec = FakeEcReconstructor::new(Ok(Bytes::new()));
        let limiter = TokenBucket::new(1000, 1000);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        let handle = tokio::spawn(async move {
            worker
                .run(&reader, &frag_reader, &writer, &ec, &limiter, cancel_rx)
                .await;
        });

        cancel_tx.send(true).unwrap_or(());
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        let result = tokio::time::timeout(tokio::time::Duration::from_secs(1), handle).await;
        assert!(result.is_ok());
    }
}
