//! Protection policy definitions for volumes and extents (§2.10).
//!
//! A protection policy specifies either replication factor N or erasure coding K+M.

use serde::{Deserialize, Serialize};

use crate::error::Error;

/// Durability policy for a volume or extent.
///
/// Determines how data is protected against disk and node failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProtectionPolicy {
    /// N-way replication: each extent is stored as N identical copies.
    Replicated {
        /// Number of replicas. Must be >= 1.
        replicas: u8,
    },
    /// Erasure coding with K data chunks and M parity chunks.
    ErasureCoded {
        /// Number of data chunks. Must be >= 1.
        data_chunks: u8,
        /// Number of parity chunks. Must be >= 1.
        parity_chunks: u8,
    },
}

impl ProtectionPolicy {
    /// Validate that the policy parameters are within acceptable bounds.
    pub fn validate(&self) -> Result<(), Error> {
        match self {
            ProtectionPolicy::Replicated { replicas } => {
                if *replicas < 1 {
                    return Err(Error::Config(
                        "replicated policy requires at least 1 replica".into(),
                    ));
                }
                Ok(())
            }
            ProtectionPolicy::ErasureCoded {
                data_chunks,
                parity_chunks,
            } => {
                if *data_chunks < 1 {
                    return Err(Error::Config(
                        "erasure coding requires at least 1 data chunk".into(),
                    ));
                }
                if *parity_chunks < 1 {
                    return Err(Error::Config(
                        "erasure coding requires at least 1 parity chunk".into(),
                    ));
                }
                Ok(())
            }
        }
    }

    /// Total number of nodes required to satisfy this policy.
    pub fn required_nodes(&self) -> u8 {
        match self {
            ProtectionPolicy::Replicated { replicas } => *replicas,
            ProtectionPolicy::ErasureCoded {
                data_chunks,
                parity_chunks,
            } => data_chunks.saturating_add(*parity_chunks),
        }
    }

    /// Number of simultaneous failures the policy can tolerate.
    pub fn fault_tolerance(&self) -> u8 {
        match self {
            ProtectionPolicy::Replicated { replicas } => replicas.saturating_sub(1),
            ProtectionPolicy::ErasureCoded { parity_chunks, .. } => *parity_chunks,
        }
    }
}

impl std::fmt::Display for ProtectionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtectionPolicy::Replicated { replicas } => write!(f, "replicated({})", replicas),
            ProtectionPolicy::ErasureCoded {
                data_chunks,
                parity_chunks,
            } => write!(f, "ec({}+{})", data_chunks, parity_chunks),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replicated_valid() {
        let p = ProtectionPolicy::Replicated { replicas: 3 };
        assert!(p.validate().is_ok());
    }

    #[test]
    fn test_replicated_single_replica() {
        let p = ProtectionPolicy::Replicated { replicas: 1 };
        assert!(p.validate().is_ok());
    }

    #[test]
    fn test_replicated_zero_replicas() {
        let p = ProtectionPolicy::Replicated { replicas: 0 };
        assert!(p.validate().is_err());
    }

    #[test]
    fn test_erasure_coded_valid() {
        let p = ProtectionPolicy::ErasureCoded {
            data_chunks: 4,
            parity_chunks: 2,
        };
        assert!(p.validate().is_ok());
    }

    #[test]
    fn test_erasure_coded_zero_data() {
        let p = ProtectionPolicy::ErasureCoded {
            data_chunks: 0,
            parity_chunks: 2,
        };
        let err = p.validate().unwrap_err();
        assert!(err.to_string().contains("data chunk"));
    }

    #[test]
    fn test_erasure_coded_zero_parity() {
        let p = ProtectionPolicy::ErasureCoded {
            data_chunks: 4,
            parity_chunks: 0,
        };
        let err = p.validate().unwrap_err();
        assert!(err.to_string().contains("parity chunk"));
    }

    #[test]
    fn test_required_nodes_replicated() {
        let p = ProtectionPolicy::Replicated { replicas: 3 };
        assert_eq!(p.required_nodes(), 3);
    }

    #[test]
    fn test_required_nodes_erasure_coded() {
        let p = ProtectionPolicy::ErasureCoded {
            data_chunks: 4,
            parity_chunks: 2,
        };
        assert_eq!(p.required_nodes(), 6);
    }

    #[test]
    fn test_fault_tolerance_replicated() {
        let p = ProtectionPolicy::Replicated { replicas: 3 };
        assert_eq!(p.fault_tolerance(), 2);
    }

    #[test]
    fn test_fault_tolerance_single_replica() {
        let p = ProtectionPolicy::Replicated { replicas: 1 };
        assert_eq!(p.fault_tolerance(), 0);
    }

    #[test]
    fn test_fault_tolerance_erasure_coded() {
        let p = ProtectionPolicy::ErasureCoded {
            data_chunks: 4,
            parity_chunks: 2,
        };
        assert_eq!(p.fault_tolerance(), 2);
    }

    #[test]
    fn test_display_replicated() {
        let p = ProtectionPolicy::Replicated { replicas: 3 };
        assert_eq!(p.to_string(), "replicated(3)");
    }

    #[test]
    fn test_display_erasure_coded() {
        let p = ProtectionPolicy::ErasureCoded {
            data_chunks: 4,
            parity_chunks: 2,
        };
        assert_eq!(p.to_string(), "ec(4+2)");
    }

    #[test]
    fn test_serde_roundtrip_replicated() {
        let p = ProtectionPolicy::Replicated { replicas: 3 };
        let json = serde_json::to_string(&p).unwrap();
        let parsed: ProtectionPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(p, parsed);
    }

    #[test]
    fn test_serde_roundtrip_erasure_coded() {
        let p = ProtectionPolicy::ErasureCoded {
            data_chunks: 4,
            parity_chunks: 2,
        };
        let json = serde_json::to_string(&p).unwrap();
        let parsed: ProtectionPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(p, parsed);
    }

    #[test]
    fn test_protection_policy_clone() {
        let p = ProtectionPolicy::Replicated { replicas: 3 };
        let cloned = p.clone();
        assert_eq!(p, cloned);
    }

    #[test]
    fn test_protection_policy_debug() {
        let p = ProtectionPolicy::Replicated { replicas: 3 };
        let debug = format!("{:?}", p);
        assert!(debug.contains("Replicated"));
    }

    #[test]
    fn test_protection_policy_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(ProtectionPolicy::Replicated { replicas: 3 });
        set.insert(ProtectionPolicy::Replicated { replicas: 3 });
        assert_eq!(set.len(), 1);
    }
}
