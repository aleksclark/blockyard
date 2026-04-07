//! Storage-specific error types.
//!
//! These complement the shared [`blockyard_common::Error`] with storage-layer detail.

/// Errors specific to the storage engine.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("disk not found: {0}")]
    DiskNotFound(String),

    #[error("duplicate disk: {0}")]
    DuplicateDisk(String),

    #[error("XFS validation failed: {0}")]
    XfsValidation(String),

    #[error("disk identity error: {0}")]
    DiskIdentity(String),

    #[error("invalid state transition: {0}")]
    InvalidTransition(String),

    #[error("allocation denied: {0}")]
    AllocationDenied(String),

    #[error("extent not found: {0}")]
    ExtentNotFound(String),

    #[error("extent already exists: {0}")]
    ExtentExists(String),

    #[error("immutability violation: {0}")]
    ImmutabilityViolation(String),

    #[error("checksum mismatch: {0}")]
    ChecksumMismatch(String),

    #[error("staging error: {0}")]
    StagingError(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("stale epoch: {0}")]
    StaleEpoch(String),

    #[error("duplicate operation: {0}")]
    DuplicateOperation(String),
}

impl From<StorageError> for blockyard_common::Error {
    fn from(e: StorageError) -> Self {
        blockyard_common::Error::Storage(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disk_not_found_display() {
        let err = StorageError::DiskNotFound("disk-1".into());
        assert_eq!(err.to_string(), "disk not found: disk-1");
    }

    #[test]
    fn test_duplicate_disk_display() {
        let err = StorageError::DuplicateDisk("disk-1".into());
        assert_eq!(err.to_string(), "duplicate disk: disk-1");
    }

    #[test]
    fn test_xfs_validation_display() {
        let err = StorageError::XfsValidation("not xfs".into());
        assert_eq!(err.to_string(), "XFS validation failed: not xfs");
    }

    #[test]
    fn test_disk_identity_display() {
        let err = StorageError::DiskIdentity("corrupt".into());
        assert_eq!(err.to_string(), "disk identity error: corrupt");
    }

    #[test]
    fn test_invalid_transition_display() {
        let err = StorageError::InvalidTransition("bad".into());
        assert_eq!(err.to_string(), "invalid state transition: bad");
    }

    #[test]
    fn test_allocation_denied_display() {
        let err = StorageError::AllocationDenied("degraded".into());
        assert_eq!(err.to_string(), "allocation denied: degraded");
    }

    #[test]
    fn test_extent_not_found_display() {
        let err = StorageError::ExtentNotFound("e-1".into());
        assert_eq!(err.to_string(), "extent not found: e-1");
    }

    #[test]
    fn test_extent_exists_display() {
        let err = StorageError::ExtentExists("e-1".into());
        assert_eq!(err.to_string(), "extent already exists: e-1");
    }

    #[test]
    fn test_immutability_violation_display() {
        let err = StorageError::ImmutabilityViolation("overwrite".into());
        assert_eq!(err.to_string(), "immutability violation: overwrite");
    }

    #[test]
    fn test_checksum_mismatch_display() {
        let err = StorageError::ChecksumMismatch("bad hash".into());
        assert_eq!(err.to_string(), "checksum mismatch: bad hash");
    }

    #[test]
    fn test_staging_error_display() {
        let err = StorageError::StagingError("temp failed".into());
        assert_eq!(err.to_string(), "staging error: temp failed");
    }

    #[test]
    fn test_io_error_from() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        let err: StorageError = io_err.into();
        assert!(err.to_string().contains("gone"));
    }

    #[test]
    fn test_stale_epoch_display() {
        let err = StorageError::StaleEpoch("epoch 5 < 7".into());
        assert_eq!(err.to_string(), "stale epoch: epoch 5 < 7");
    }

    #[test]
    fn test_duplicate_operation_display() {
        let err = StorageError::DuplicateOperation("op-1".into());
        assert_eq!(err.to_string(), "duplicate operation: op-1");
    }

    #[test]
    fn test_into_common_error() {
        let err = StorageError::DiskNotFound("x".into());
        let common: blockyard_common::Error = err.into();
        assert!(common.to_string().contains("disk not found"));
    }

    #[test]
    fn test_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StorageError>();
    }

    #[test]
    fn test_error_debug() {
        let err = StorageError::AllocationDenied("test".into());
        let debug = format!("{:?}", err);
        assert!(debug.contains("AllocationDenied"));
    }
}
