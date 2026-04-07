//! Shared strong checksum function (§5.3).
//!
//! All extent data checksums in Blockyard use blake3 for integrity verification.
//! This module provides a single canonical implementation used by all pipelines.

/// Compute a blake3 checksum of `data`, returned as a hex string.
pub fn compute_checksum(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}

/// Verify that `data` matches the expected checksum.
pub fn verify_checksum(data: &[u8], expected: &str) -> bool {
    compute_checksum(data) == expected
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_compute_checksum_empty() {
        let c = compute_checksum(b"");
        assert!(!c.is_empty());
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
    fn test_checksum_is_hex() {
        let c = compute_checksum(b"some data");
        // blake3 produces 64 hex characters (32 bytes)
        assert_eq!(c.len(), 64);
        assert!(c.chars().all(|ch| ch.is_ascii_hexdigit()));
    }
}
