//! Extent file layout, staging, commit, and immutability (§5.2, §5.3, §5.4).
//!
//! Manages the full lifecycle of extent files on XFS:
//! - Path derivation from `(DiskId, ExtentId, ExtentVersion)`
//! - Temporary file staging with integrity metadata
//! - Crash-consistent local durability (`fsync`)
//! - Atomic promotion from staged to committed (rename after fsync)
//! - Immutability enforcement for committed extents

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use blockyard_common::{DiskId, ExtentId};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::error::StorageError;

/// Version identifier for an extent.
pub type ExtentVersion = u64;

/// Storage class for an extent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StorageClass {
    #[default]
    Default,
    HighPerformance,
    Archive,
}

/// Metadata for a committed local extent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtentMeta {
    pub extent_id: ExtentId,
    pub disk_id: DiskId,
    pub version: ExtentVersion,
    pub checksum: String,
    pub size: u64,
    pub storage_class: StorageClass,
    pub committed_at: u64,
}

/// Local extent index entry: `ExtentId → (DiskId, path, version, checksum, storage_class)`.
#[derive(Debug, Clone)]
pub struct LocalExtentEntry {
    pub extent_id: ExtentId,
    pub disk_id: DiskId,
    pub path: PathBuf,
    pub version: ExtentVersion,
    pub checksum: String,
    pub size: u64,
    pub storage_class: StorageClass,
}

/// Central extent index tracking all committed local extents.
#[derive(Debug)]
pub struct ExtentIndex {
    entries: RwLock<HashMap<ExtentId, LocalExtentEntry>>,
}

impl ExtentIndex {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    pub fn insert(&self, entry: LocalExtentEntry) -> Result<(), StorageError> {
        let mut entries = self.entries.write();
        if entries.contains_key(&entry.extent_id) {
            return Err(StorageError::ExtentExists(format!(
                "extent {} already indexed",
                entry.extent_id
            )));
        }
        entries.insert(entry.extent_id, entry);
        Ok(())
    }

    pub fn get(&self, extent_id: ExtentId) -> Option<LocalExtentEntry> {
        let entries = self.entries.read();
        entries.get(&extent_id).cloned()
    }

    pub fn remove(&self, extent_id: ExtentId) -> Option<LocalExtentEntry> {
        let mut entries = self.entries.write();
        entries.remove(&extent_id)
    }

    pub fn contains(&self, extent_id: ExtentId) -> bool {
        let entries = self.entries.read();
        entries.contains_key(&extent_id)
    }

    pub fn count(&self) -> usize {
        let entries = self.entries.read();
        entries.len()
    }

    pub fn list_for_disk(&self, disk_id: DiskId) -> Vec<LocalExtentEntry> {
        let entries = self.entries.read();
        entries
            .values()
            .filter(|e| e.disk_id == disk_id)
            .cloned()
            .collect()
    }

    pub fn list_all(&self) -> Vec<LocalExtentEntry> {
        let entries = self.entries.read();
        entries.values().cloned().collect()
    }
}

impl Default for ExtentIndex {
    fn default() -> Self {
        Self::new()
    }
}

const STAGED_DIR: &str = "staged";
const COMMITTED_DIR: &str = "committed";
const META_SUFFIX: &str = ".meta";

/// Compute the directory path for an extent on a disk.
///
/// Layout: `<mount_path>/committed/<extent_id_prefix>/<extent_id>_v<version>`
pub fn committed_extent_path(
    mount_path: &Path,
    extent_id: ExtentId,
    version: ExtentVersion,
) -> PathBuf {
    let id_str = extent_id.to_string();
    let prefix = &id_str[..8.min(id_str.len())];
    mount_path
        .join(COMMITTED_DIR)
        .join(prefix)
        .join(format!("{id_str}_v{version}"))
}

/// Compute the path for a staged (temporary) extent file.
pub fn staged_extent_path(
    mount_path: &Path,
    extent_id: ExtentId,
    version: ExtentVersion,
) -> PathBuf {
    let id_str = extent_id.to_string();
    mount_path
        .join(STAGED_DIR)
        .join(format!("{id_str}_v{version}.staging"))
}

/// Compute the metadata path for a committed extent.
pub fn extent_meta_path(mount_path: &Path, extent_id: ExtentId, version: ExtentVersion) -> PathBuf {
    let mut p = committed_extent_path(mount_path, extent_id, version);
    let name = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{name}{META_SUFFIX}"));
    p
}

/// Compute blake3 checksum of data (canonical implementation from blockyard-common).
pub fn compute_checksum(data: &[u8]) -> String {
    blockyard_common::checksum::compute_checksum(data)
}

/// Verify data against a previously computed checksum.
pub fn verify_checksum(data: &[u8], expected: &str) -> bool {
    compute_checksum(data) == expected
}

/// Manages extent file lifecycle operations for a single disk.
#[derive(Debug)]
pub struct ExtentStore {
    mount_path: PathBuf,
    disk_id: DiskId,
}

impl ExtentStore {
    pub fn new(mount_path: PathBuf, disk_id: DiskId) -> Self {
        Self {
            mount_path,
            disk_id,
        }
    }

    /// Ensure directory structure exists for staging and committed extents.
    pub fn init_directories(&self) -> Result<(), StorageError> {
        fs::create_dir_all(self.mount_path.join(STAGED_DIR))?;
        fs::create_dir_all(self.mount_path.join(COMMITTED_DIR))?;
        Ok(())
    }

    /// Stage an extent: write data to a temporary file with integrity metadata (§5.3).
    ///
    /// Returns the path to the staged file and the computed checksum.
    pub fn stage_extent(
        &self,
        extent_id: ExtentId,
        version: ExtentVersion,
        data: &[u8],
    ) -> Result<(PathBuf, String), StorageError> {
        let staged_path = staged_extent_path(&self.mount_path, extent_id, version);

        if let Some(parent) = staged_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let checksum = compute_checksum(data);

        let mut file = File::create(&staged_path).map_err(|e| {
            StorageError::StagingError(format!(
                "failed to create staging file {}: {e}",
                staged_path.display()
            ))
        })?;

        file.write_all(data).map_err(|e| {
            StorageError::StagingError(format!(
                "failed to write staging file {}: {e}",
                staged_path.display()
            ))
        })?;

        file.sync_all().map_err(|e| {
            StorageError::StagingError(format!(
                "fsync failed for staging file {}: {e}",
                staged_path.display()
            ))
        })?;

        debug!(
            %extent_id,
            %version,
            checksum = %checksum,
            path = %staged_path.display(),
            "extent staged"
        );

        Ok((staged_path, checksum))
    }

    /// Promote a staged extent to committed via atomic rename (§5.3, §5.4).
    ///
    /// This is the crash-consistent commit point: after rename + fsync of the
    /// parent directory, the extent is committed.
    pub fn commit_extent(
        &self,
        extent_id: ExtentId,
        version: ExtentVersion,
        checksum: &str,
        size: u64,
        storage_class: StorageClass,
    ) -> Result<LocalExtentEntry, StorageError> {
        let staged_path = staged_extent_path(&self.mount_path, extent_id, version);
        let committed_path = committed_extent_path(&self.mount_path, extent_id, version);

        if !staged_path.exists() {
            return Err(StorageError::StagingError(format!(
                "staged file not found: {}",
                staged_path.display()
            )));
        }

        if committed_path.exists() {
            return Err(StorageError::ImmutabilityViolation(format!(
                "committed extent already exists: {}",
                committed_path.display()
            )));
        }

        if let Some(parent) = committed_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let meta = ExtentMeta {
            extent_id,
            disk_id: self.disk_id,
            version,
            checksum: checksum.to_string(),
            size,
            storage_class,
            committed_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        let meta_path = extent_meta_path(&self.mount_path, extent_id, version);
        let meta_json = serde_json::to_string_pretty(&meta).map_err(|e| {
            StorageError::StagingError(format!("failed to serialize extent metadata: {e}"))
        })?;

        let mut meta_file = File::create(&meta_path).map_err(|e| {
            StorageError::StagingError(format!(
                "failed to create metadata file {}: {e}",
                meta_path.display()
            ))
        })?;
        meta_file.write_all(meta_json.as_bytes()).map_err(|e| {
            StorageError::StagingError(format!(
                "failed to write metadata file {}: {e}",
                meta_path.display()
            ))
        })?;
        meta_file.sync_all().map_err(|e| {
            StorageError::StagingError(format!(
                "fsync failed for metadata file {}: {e}",
                meta_path.display()
            ))
        })?;
        drop(meta_file);

        fs::rename(&staged_path, &committed_path).map_err(|e| {
            StorageError::StagingError(format!(
                "atomic rename failed {} → {}: {e}",
                staged_path.display(),
                committed_path.display()
            ))
        })?;

        sync_parent_dir(&committed_path)?;

        info!(
            %extent_id,
            %version,
            path = %committed_path.display(),
            "extent committed"
        );

        Ok(LocalExtentEntry {
            extent_id,
            disk_id: self.disk_id,
            path: committed_path,
            version,
            checksum: checksum.to_string(),
            size,
            storage_class,
        })
    }

    /// Read a committed extent and verify its checksum (§5.6).
    pub fn read_extent(
        &self,
        extent_id: ExtentId,
        version: ExtentVersion,
    ) -> Result<(Vec<u8>, String), StorageError> {
        let path = committed_extent_path(&self.mount_path, extent_id, version);

        if !path.exists() {
            return Err(StorageError::ExtentNotFound(format!(
                "committed extent not found: {}",
                path.display()
            )));
        }

        let data = fs::read(&path)?;
        let checksum = compute_checksum(&data);

        let meta_path = extent_meta_path(&self.mount_path, extent_id, version);
        if meta_path.exists() {
            let meta_json = fs::read_to_string(&meta_path)?;
            if let Ok(meta) = serde_json::from_str::<ExtentMeta>(&meta_json) {
                if meta.checksum != checksum {
                    return Err(StorageError::ChecksumMismatch(format!(
                        "extent {extent_id} v{version}: expected {}, got {checksum}",
                        meta.checksum
                    )));
                }
            }
        }

        Ok((data, checksum))
    }

    /// Check whether a committed extent file exists.
    pub fn extent_exists(&self, extent_id: ExtentId, version: ExtentVersion) -> bool {
        committed_extent_path(&self.mount_path, extent_id, version).exists()
    }

    /// Clean up orphaned staged files that are older than the retention interval (§6.9).
    pub fn cleanup_orphaned_staged(
        &self,
        retention_secs: u64,
    ) -> Result<Vec<PathBuf>, StorageError> {
        let staged_dir = self.mount_path.join(STAGED_DIR);
        if !staged_dir.exists() {
            return Ok(Vec::new());
        }

        let now = std::time::SystemTime::now();
        let mut cleaned = Vec::new();

        for entry in fs::read_dir(&staged_dir)? {
            let entry = entry?;
            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            if let Ok(metadata) = fs::metadata(&path) {
                if let Ok(modified) = metadata.modified() {
                    if let Ok(age) = now.duration_since(modified) {
                        if age.as_secs() >= retention_secs && fs::remove_file(&path).is_ok() {
                            debug!(path = %path.display(), "cleaned up orphaned staged file");
                            cleaned.push(path);
                        }
                    }
                }
            }
        }

        Ok(cleaned)
    }

    /// Recover state on startup: rebuild index from committed extents, discard staged (§6.10).
    pub fn recover(&self, index: &ExtentIndex) -> Result<RecoveryReport, StorageError> {
        let mut report = RecoveryReport::default();

        self.init_directories()?;

        let committed_dir = self.mount_path.join(COMMITTED_DIR);
        if committed_dir.exists() {
            self.scan_committed_dir(&committed_dir, index, &mut report)?;
        }

        let staged_dir = self.mount_path.join(STAGED_DIR);
        if staged_dir.exists() {
            for entry in fs::read_dir(&staged_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() && fs::remove_file(&path).is_ok() {
                    debug!(path = %path.display(), "discarded incomplete staged file");
                    report.staged_discarded += 1;
                }
            }
        }

        info!(
            disk_id = %self.disk_id,
            committed = report.committed_recovered,
            staged_discarded = report.staged_discarded,
            errors = report.errors,
            "disk recovery complete"
        );

        Ok(report)
    }

    fn scan_committed_dir(
        &self,
        dir: &Path,
        index: &ExtentIndex,
        report: &mut RecoveryReport,
    ) -> Result<(), StorageError> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                self.scan_committed_dir(&path, index, report)?;
                continue;
            }

            if path.extension().and_then(|e| e.to_str()) == Some("meta") {
                continue;
            }

            if !path.is_file() {
                continue;
            }

            match self.recover_extent_from_file(&path) {
                Ok(entry_data) => {
                    if index.insert(entry_data).is_ok() {
                        report.committed_recovered += 1;
                    }
                }
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "failed to recover extent");
                    report.errors += 1;
                }
            }
        }
        Ok(())
    }

    fn recover_extent_from_file(&self, path: &Path) -> Result<LocalExtentEntry, StorageError> {
        let meta_path = PathBuf::from(format!("{}{}", path.to_string_lossy(), META_SUFFIX));

        if !meta_path.exists() {
            return Err(StorageError::ExtentNotFound(format!(
                "metadata file missing for {}",
                path.display()
            )));
        }

        let meta_json = fs::read_to_string(&meta_path)?;
        let meta: ExtentMeta = serde_json::from_str(&meta_json).map_err(|e| {
            StorageError::ExtentNotFound(format!(
                "corrupt metadata at {}: {e}",
                meta_path.display()
            ))
        })?;

        Ok(LocalExtentEntry {
            extent_id: meta.extent_id,
            disk_id: meta.disk_id,
            path: path.to_path_buf(),
            version: meta.version,
            checksum: meta.checksum,
            size: meta.size,
            storage_class: meta.storage_class,
        })
    }

    pub fn mount_path(&self) -> &Path {
        &self.mount_path
    }

    pub fn disk_id(&self) -> DiskId {
        self.disk_id
    }
}

/// Report from startup recovery.
#[derive(Debug, Default)]
pub struct RecoveryReport {
    pub committed_recovered: usize,
    pub staged_discarded: usize,
    pub errors: usize,
}

/// Sync the parent directory of a path for crash consistency.
fn sync_parent_dir(path: &Path) -> Result<(), StorageError> {
    if let Some(parent) = path.parent() {
        let dir = OpenOptions::new().read(true).open(parent).map_err(|e| {
            StorageError::StagingError(format!(
                "failed to open parent dir {} for sync: {e}",
                parent.display()
            ))
        })?;
        dir.sync_all().map_err(|e| {
            StorageError::StagingError(format!(
                "failed to sync parent dir {}: {e}",
                parent.display()
            ))
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_disk() -> (tempfile::TempDir, ExtentStore) {
        let dir = tempfile::tempdir().unwrap();
        let disk_id = DiskId::generate();
        let store = ExtentStore::new(dir.path().to_path_buf(), disk_id);
        store.init_directories().unwrap();
        (dir, store)
    }

    #[test]
    fn test_committed_extent_path_format() {
        let eid = ExtentId::generate();
        let path = committed_extent_path(Path::new("/mnt/disk0"), eid, 1);
        let id_str = eid.to_string();
        let prefix = &id_str[..8];
        assert!(path.to_string_lossy().contains(prefix));
        assert!(path.to_string_lossy().contains("_v1"));
        assert!(path.to_string_lossy().contains("committed"));
    }

    #[test]
    fn test_staged_extent_path_format() {
        let eid = ExtentId::generate();
        let path = staged_extent_path(Path::new("/mnt/disk0"), eid, 1);
        assert!(path.to_string_lossy().contains("staged"));
        assert!(path.to_string_lossy().contains(".staging"));
    }

    #[test]
    fn test_extent_meta_path_format() {
        let eid = ExtentId::generate();
        let path = extent_meta_path(Path::new("/mnt/disk0"), eid, 1);
        assert!(path.to_string_lossy().contains(".meta"));
    }

    #[test]
    fn test_compute_checksum_deterministic() {
        let data = b"hello world";
        let c1 = compute_checksum(data);
        let c2 = compute_checksum(data);
        assert_eq!(c1, c2);
    }

    #[test]
    fn test_compute_checksum_different_data() {
        assert_ne!(compute_checksum(b"hello"), compute_checksum(b"world"));
    }

    #[test]
    fn test_verify_checksum_valid() {
        let data = b"test data";
        let checksum = compute_checksum(data);
        assert!(verify_checksum(data, &checksum));
    }

    #[test]
    fn test_verify_checksum_invalid() {
        assert!(!verify_checksum(b"test", "badchecksum"));
    }

    #[test]
    fn test_stage_extent() {
        let (_dir, store) = setup_disk();
        let eid = ExtentId::generate();
        let data = b"extent payload data";
        let (path, checksum) = store.stage_extent(eid, 1, data).unwrap();
        assert!(path.exists());
        assert!(!checksum.is_empty());
        let contents = fs::read(&path).unwrap();
        assert_eq!(contents, data);
    }

    #[test]
    fn test_commit_extent() {
        let (_dir, store) = setup_disk();
        let eid = ExtentId::generate();
        let data = b"commit me";

        let (_staged_path, checksum) = store.stage_extent(eid, 1, data).unwrap();
        let entry = store
            .commit_extent(eid, 1, &checksum, data.len() as u64, StorageClass::Default)
            .unwrap();

        assert_eq!(entry.extent_id, eid);
        assert_eq!(entry.version, 1);
        assert!(entry.path.exists());
        assert!(!staged_extent_path(store.mount_path(), eid, 1).exists());
    }

    #[test]
    fn test_commit_without_staging_fails() {
        let (_dir, store) = setup_disk();
        let eid = ExtentId::generate();
        let result = store.commit_extent(eid, 1, "fake", 100, StorageClass::Default);
        assert!(result.is_err());
    }

    #[test]
    fn test_immutability_double_commit() {
        let (_dir, store) = setup_disk();
        let eid = ExtentId::generate();
        let data = b"payload";

        let (_, checksum) = store.stage_extent(eid, 1, data).unwrap();
        store
            .commit_extent(eid, 1, &checksum, data.len() as u64, StorageClass::Default)
            .unwrap();

        let (_, checksum2) = store.stage_extent(eid, 1, data).unwrap();
        let result =
            store.commit_extent(eid, 1, &checksum2, data.len() as u64, StorageClass::Default);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_committed_extent() {
        let (_dir, store) = setup_disk();
        let eid = ExtentId::generate();
        let data = b"readable data";

        let (_, checksum) = store.stage_extent(eid, 1, data).unwrap();
        store
            .commit_extent(eid, 1, &checksum, data.len() as u64, StorageClass::Default)
            .unwrap();

        let (read_data, read_checksum) = store.read_extent(eid, 1).unwrap();
        assert_eq!(read_data, data.to_vec());
        assert_eq!(read_checksum, checksum);
    }

    #[test]
    fn test_read_nonexistent_extent() {
        let (_dir, store) = setup_disk();
        let result = store.read_extent(ExtentId::generate(), 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_detects_corruption() {
        let (_dir, store) = setup_disk();
        let eid = ExtentId::generate();
        let data = b"original";

        let (_, checksum) = store.stage_extent(eid, 1, data).unwrap();
        store
            .commit_extent(eid, 1, &checksum, data.len() as u64, StorageClass::Default)
            .unwrap();

        let committed_path = committed_extent_path(store.mount_path(), eid, 1);
        fs::write(&committed_path, b"corrupted").unwrap();

        let result = store.read_extent(eid, 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_extent_exists() {
        let (_dir, store) = setup_disk();
        let eid = ExtentId::generate();
        assert!(!store.extent_exists(eid, 1));

        let data = b"data";
        let (_, checksum) = store.stage_extent(eid, 1, data).unwrap();
        store
            .commit_extent(eid, 1, &checksum, data.len() as u64, StorageClass::Default)
            .unwrap();
        assert!(store.extent_exists(eid, 1));
    }

    #[test]
    fn test_cleanup_orphaned_staged() {
        let (_dir, store) = setup_disk();
        let eid = ExtentId::generate();

        store.stage_extent(eid, 1, b"orphan").unwrap();

        let cleaned = store.cleanup_orphaned_staged(0).unwrap();
        assert_eq!(cleaned.len(), 1);

        assert!(!staged_extent_path(store.mount_path(), eid, 1).exists());
    }

    #[test]
    fn test_cleanup_respects_retention() {
        let (_dir, store) = setup_disk();
        let eid = ExtentId::generate();

        store.stage_extent(eid, 1, b"recent").unwrap();

        let cleaned = store.cleanup_orphaned_staged(3600).unwrap();
        assert!(cleaned.is_empty());
    }

    #[test]
    fn test_recover_committed_extents() {
        let (_dir, store) = setup_disk();
        let _index = ExtentIndex::new();

        let eid1 = ExtentId::generate();
        let eid2 = ExtentId::generate();
        let data = b"recovery test";

        let (_, c1) = store.stage_extent(eid1, 1, data).unwrap();
        store
            .commit_extent(eid1, 1, &c1, data.len() as u64, StorageClass::Default)
            .unwrap();

        let (_, c2) = store.stage_extent(eid2, 1, data).unwrap();
        store
            .commit_extent(eid2, 1, &c2, data.len() as u64, StorageClass::Default)
            .unwrap();

        store
            .stage_extent(ExtentId::generate(), 1, b"orphan")
            .unwrap();

        let new_index = ExtentIndex::new();
        let report = store.recover(&new_index).unwrap();

        assert_eq!(report.committed_recovered, 2);
        assert_eq!(report.staged_discarded, 1);
        assert_eq!(new_index.count(), 2);
    }

    #[test]
    fn test_recover_empty_disk() {
        let (_dir, store) = setup_disk();
        let index = ExtentIndex::new();
        let report = store.recover(&index).unwrap();
        assert_eq!(report.committed_recovered, 0);
        assert_eq!(report.staged_discarded, 0);
    }

    #[test]
    fn test_extent_index_insert_get() {
        let index = ExtentIndex::new();
        let eid = ExtentId::generate();
        let entry = LocalExtentEntry {
            extent_id: eid,
            disk_id: DiskId::generate(),
            path: PathBuf::from("/mnt/disk0/committed/test"),
            version: 1,
            checksum: "abc123".into(),
            size: 100,
            storage_class: StorageClass::Default,
        };

        index.insert(entry).unwrap();
        assert!(index.contains(eid));

        let got = index.get(eid).unwrap();
        assert_eq!(got.version, 1);
    }

    #[test]
    fn test_extent_index_duplicate_insert() {
        let index = ExtentIndex::new();
        let eid = ExtentId::generate();
        let entry = LocalExtentEntry {
            extent_id: eid,
            disk_id: DiskId::generate(),
            path: PathBuf::from("/test"),
            version: 1,
            checksum: "abc".into(),
            size: 0,
            storage_class: StorageClass::Default,
        };

        index.insert(entry.clone()).unwrap();
        assert!(index.insert(entry).is_err());
    }

    #[test]
    fn test_extent_index_remove() {
        let index = ExtentIndex::new();
        let eid = ExtentId::generate();
        let entry = LocalExtentEntry {
            extent_id: eid,
            disk_id: DiskId::generate(),
            path: PathBuf::from("/test"),
            version: 1,
            checksum: "abc".into(),
            size: 0,
            storage_class: StorageClass::Default,
        };

        index.insert(entry).unwrap();
        let removed = index.remove(eid);
        assert!(removed.is_some());
        assert!(!index.contains(eid));
    }

    #[test]
    fn test_extent_index_get_nonexistent() {
        let index = ExtentIndex::new();
        assert!(index.get(ExtentId::generate()).is_none());
    }

    #[test]
    fn test_extent_index_count() {
        let index = ExtentIndex::new();
        assert_eq!(index.count(), 0);

        let entry = LocalExtentEntry {
            extent_id: ExtentId::generate(),
            disk_id: DiskId::generate(),
            path: PathBuf::from("/test"),
            version: 1,
            checksum: "abc".into(),
            size: 0,
            storage_class: StorageClass::Default,
        };
        index.insert(entry).unwrap();
        assert_eq!(index.count(), 1);
    }

    #[test]
    fn test_extent_index_list_for_disk() {
        let index = ExtentIndex::new();
        let disk1 = DiskId::generate();
        let disk2 = DiskId::generate();

        for _ in 0..3 {
            index
                .insert(LocalExtentEntry {
                    extent_id: ExtentId::generate(),
                    disk_id: disk1,
                    path: PathBuf::from("/test"),
                    version: 1,
                    checksum: "abc".into(),
                    size: 0,
                    storage_class: StorageClass::Default,
                })
                .unwrap();
        }
        index
            .insert(LocalExtentEntry {
                extent_id: ExtentId::generate(),
                disk_id: disk2,
                path: PathBuf::from("/test2"),
                version: 1,
                checksum: "def".into(),
                size: 0,
                storage_class: StorageClass::Default,
            })
            .unwrap();

        assert_eq!(index.list_for_disk(disk1).len(), 3);
        assert_eq!(index.list_for_disk(disk2).len(), 1);
    }

    #[test]
    fn test_extent_index_list_all() {
        let index = ExtentIndex::new();
        for _ in 0..5 {
            index
                .insert(LocalExtentEntry {
                    extent_id: ExtentId::generate(),
                    disk_id: DiskId::generate(),
                    path: PathBuf::from("/test"),
                    version: 1,
                    checksum: "abc".into(),
                    size: 0,
                    storage_class: StorageClass::Default,
                })
                .unwrap();
        }
        assert_eq!(index.list_all().len(), 5);
    }

    #[test]
    fn test_extent_index_default() {
        let index = ExtentIndex::default();
        assert_eq!(index.count(), 0);
    }

    #[test]
    fn test_storage_class_default() {
        assert_eq!(StorageClass::default(), StorageClass::Default);
    }

    #[test]
    fn test_storage_class_serde() {
        for class in [
            StorageClass::Default,
            StorageClass::HighPerformance,
            StorageClass::Archive,
        ] {
            let json = serde_json::to_string(&class).unwrap();
            let parsed: StorageClass = serde_json::from_str(&json).unwrap();
            assert_eq!(class, parsed);
        }
    }

    #[test]
    fn test_extent_meta_serde() {
        let meta = ExtentMeta {
            extent_id: ExtentId::generate(),
            disk_id: DiskId::generate(),
            version: 1,
            checksum: "abc123".into(),
            size: 4096,
            storage_class: StorageClass::Default,
            committed_at: 1000,
        };
        let json = serde_json::to_string(&meta).unwrap();
        let parsed: ExtentMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(meta.extent_id, parsed.extent_id);
        assert_eq!(meta.version, parsed.version);
    }

    #[test]
    fn test_different_versions_different_paths() {
        let eid = ExtentId::generate();
        let p1 = committed_extent_path(Path::new("/mnt"), eid, 1);
        let p2 = committed_extent_path(Path::new("/mnt"), eid, 2);
        assert_ne!(p1, p2);
    }

    #[test]
    fn test_init_directories() {
        let dir = tempfile::tempdir().unwrap();
        let store = ExtentStore::new(dir.path().to_path_buf(), DiskId::generate());
        store.init_directories().unwrap();
        assert!(dir.path().join("staged").exists());
        assert!(dir.path().join("committed").exists());
    }

    #[test]
    fn test_disk_id_accessor() {
        let disk_id = DiskId::generate();
        let store = ExtentStore::new(PathBuf::from("/tmp"), disk_id);
        assert_eq!(store.disk_id(), disk_id);
    }

    #[test]
    fn test_mount_path_accessor() {
        let store = ExtentStore::new(PathBuf::from("/mnt/disk0"), DiskId::generate());
        assert_eq!(store.mount_path(), Path::new("/mnt/disk0"));
    }

    #[test]
    fn test_recovery_report_default() {
        let r = RecoveryReport::default();
        assert_eq!(r.committed_recovered, 0);
        assert_eq!(r.staged_discarded, 0);
        assert_eq!(r.errors, 0);
    }

    #[test]
    fn test_cleanup_orphaned_no_staged_dir() {
        let dir = tempfile::tempdir().unwrap();
        let store = ExtentStore::new(dir.path().to_path_buf(), DiskId::generate());
        let cleaned = store.cleanup_orphaned_staged(0).unwrap();
        assert!(cleaned.is_empty());
    }

    #[test]
    fn test_commit_extent_meta_durable_before_rename() {
        let (_dir, store) = setup_disk();
        let eid = ExtentId::generate();
        let data = b"meta durability test";

        let (_, checksum) = store.stage_extent(eid, 1, data).unwrap();
        let entry = store
            .commit_extent(eid, 1, &checksum, data.len() as u64, StorageClass::Default)
            .unwrap();

        let meta_path = extent_meta_path(store.mount_path(), eid, 1);
        assert!(meta_path.exists(), "metadata sidecar must exist after commit");

        let meta_json = fs::read_to_string(&meta_path).unwrap();
        let meta: ExtentMeta = serde_json::from_str(&meta_json).unwrap();
        assert_eq!(meta.extent_id, eid);
        assert_eq!(meta.version, 1);
        assert_eq!(meta.checksum, checksum);
        assert_eq!(meta.size, data.len() as u64);

        assert!(entry.path.exists(), "committed data file must exist");
    }
}
