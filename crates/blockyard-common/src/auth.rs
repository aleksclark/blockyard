//! Token-based authentication and volume-level authorization.
//!
//! Provides a simple in-memory [`TokenStore`] that maps bearer tokens to
//! client metadata and per-volume permissions.

use std::collections::HashMap;

/// Permission level for a volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    /// The client may only read from the volume.
    ReadOnly,
    /// The client may read from and write to the volume.
    ReadWrite,
}

/// Metadata associated with a single authentication token.
#[derive(Debug, Clone)]
pub struct TokenInfo {
    /// The raw token string (echoed back for convenience).
    pub token: String,
    /// A human-readable name for the client that owns this token.
    pub client_name: String,
    /// Per-volume permissions granted to this token.
    /// Key = volume name, Value = permission level.
    pub volumes: HashMap<String, Permission>,
}

/// An in-memory store of authentication tokens and their associated
/// permissions.
///
/// Thread-safety note: [`TokenStore`] is `Send + Sync` because it is
/// immutable after construction; a new store should be created when the
/// configuration changes.
#[derive(Debug, Clone)]
pub struct TokenStore {
    tokens: HashMap<String, TokenInfo>,
}

impl TokenStore {
    /// Create a new, empty token store.
    pub fn new() -> Self {
        Self {
            tokens: HashMap::new(),
        }
    }

    /// Insert a token and its associated info into the store.
    pub fn insert(&mut self, info: TokenInfo) {
        self.tokens.insert(info.token.clone(), info);
    }

    /// Look up token metadata.
    ///
    /// Returns `None` if the token is unknown.
    pub fn validate_token(&self, token: &str) -> Option<&TokenInfo> {
        self.tokens.get(token)
    }

    /// Check whether `token` is allowed to access `volume` with the requested
    /// access mode.
    ///
    /// * If `write` is `true`, the token must have [`Permission::ReadWrite`] on
    ///   the volume.
    /// * If `write` is `false`, either [`Permission::ReadOnly`] or
    ///   [`Permission::ReadWrite`] suffices.
    ///
    /// Returns `false` for unknown tokens or volumes not listed in the token's
    /// permissions.
    pub fn check_volume_access(&self, token: &str, volume: &str, write: bool) -> bool {
        let Some(info) = self.tokens.get(token) else {
            return false;
        };
        let Some(perm) = info.volumes.get(volume) else {
            return false;
        };
        match perm {
            Permission::ReadWrite => true,
            Permission::ReadOnly => !write,
        }
    }

    /// Return the number of tokens currently stored.
    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    /// Returns `true` when the store contains no tokens.
    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }
}

impl Default for TokenStore {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_store() -> TokenStore {
        let mut store = TokenStore::new();

        store.insert(TokenInfo {
            token: "tok-admin".into(),
            client_name: "admin-client".into(),
            volumes: HashMap::from([
                ("vol-a".into(), Permission::ReadWrite),
                ("vol-b".into(), Permission::ReadWrite),
            ]),
        });

        store.insert(TokenInfo {
            token: "tok-reader".into(),
            client_name: "read-only-client".into(),
            volumes: HashMap::from([("vol-a".into(), Permission::ReadOnly)]),
        });

        store
    }

    #[test]
    fn test_validate_known_token() {
        let store = sample_store();
        let info = store
            .validate_token("tok-admin")
            .expect("token should exist");
        assert_eq!(info.client_name, "admin-client");
    }

    #[test]
    fn test_validate_unknown_token() {
        let store = sample_store();
        assert!(store.validate_token("tok-unknown").is_none());
    }

    #[test]
    fn test_read_access_read_write_token() {
        let store = sample_store();
        assert!(store.check_volume_access("tok-admin", "vol-a", false));
    }

    #[test]
    fn test_write_access_read_write_token() {
        let store = sample_store();
        assert!(store.check_volume_access("tok-admin", "vol-a", true));
    }

    #[test]
    fn test_read_access_read_only_token() {
        let store = sample_store();
        assert!(store.check_volume_access("tok-reader", "vol-a", false));
    }

    #[test]
    fn test_write_access_read_only_token_denied() {
        let store = sample_store();
        assert!(!store.check_volume_access("tok-reader", "vol-a", true));
    }

    #[test]
    fn test_access_unknown_volume() {
        let store = sample_store();
        assert!(!store.check_volume_access("tok-admin", "vol-unknown", false));
    }

    #[test]
    fn test_access_unknown_token() {
        let store = sample_store();
        assert!(!store.check_volume_access("tok-nope", "vol-a", false));
    }

    #[test]
    fn test_empty_store() {
        let store = TokenStore::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
        assert!(store.validate_token("any").is_none());
        assert!(!store.check_volume_access("any", "vol", false));
    }

    #[test]
    fn test_store_len() {
        let store = sample_store();
        assert_eq!(store.len(), 2);
        assert!(!store.is_empty());
    }

    #[test]
    fn test_default_store_is_empty() {
        let store = TokenStore::default();
        assert!(store.is_empty());
    }

    #[test]
    fn test_permission_debug() {
        // Ensure Debug derive works.
        let dbg = format!("{:?}", Permission::ReadOnly);
        assert!(dbg.contains("ReadOnly"));
    }
}
