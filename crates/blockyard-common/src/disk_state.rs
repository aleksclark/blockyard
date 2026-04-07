//! Disk health state management (§2.9, §5.8.1).
//!
//! Models the per-disk state machine with validated transitions.

use serde::{Deserialize, Serialize};

use crate::error::Error;

/// Per-disk health state (§2.9).
///
/// State effects from §5.8.1:
/// - `Healthy`: new allocations and reads permitted.
/// - `Suspect`: reads permitted; new allocations deprioritized.
/// - `Degraded`: reads may continue; new allocations prohibited; evacuation should begin.
/// - `Draining`: reads may continue; new allocations prohibited; data movement must proceed.
/// - `Failed`: no reads or allocations except diagnostics.
/// - `Removed`: disk shall not be referenced for user data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DiskState {
    Healthy,
    Suspect,
    Degraded,
    Draining,
    Failed,
    Removed,
}

impl DiskState {
    /// Whether new extent allocations are permitted on a disk in this state.
    pub fn allows_allocation(self) -> bool {
        matches!(self, DiskState::Healthy | DiskState::Suspect)
    }

    /// Whether reads are permitted from a disk in this state.
    pub fn allows_reads(self) -> bool {
        matches!(
            self,
            DiskState::Healthy | DiskState::Suspect | DiskState::Degraded | DiskState::Draining
        )
    }

    /// Check whether a state transition is legal, returning an error if not.
    ///
    /// Legal transitions:
    /// - Healthy  → Suspect, Degraded, Draining, Failed
    /// - Suspect  → Healthy, Degraded, Draining, Failed
    /// - Degraded → Draining, Failed, Removed
    /// - Draining → Failed, Removed
    /// - Failed   → Removed
    /// - Removed  → (terminal, no transitions)
    pub fn validate_transition(self, to: DiskState) -> Result<(), Error> {
        if self == to {
            return Ok(());
        }

        let valid = match self {
            DiskState::Healthy => matches!(
                to,
                DiskState::Suspect | DiskState::Degraded | DiskState::Draining | DiskState::Failed
            ),
            DiskState::Suspect => matches!(
                to,
                DiskState::Healthy | DiskState::Degraded | DiskState::Draining | DiskState::Failed
            ),
            DiskState::Degraded => {
                matches!(
                    to,
                    DiskState::Draining | DiskState::Failed | DiskState::Removed
                )
            }
            DiskState::Draining => matches!(to, DiskState::Failed | DiskState::Removed),
            DiskState::Failed => matches!(to, DiskState::Removed),
            DiskState::Removed => false,
        };

        if valid {
            Ok(())
        } else {
            Err(Error::Storage(format!(
                "illegal disk state transition: {} → {}",
                self, to
            )))
        }
    }
}

impl std::fmt::Display for DiskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiskState::Healthy => write!(f, "healthy"),
            DiskState::Suspect => write!(f, "suspect"),
            DiskState::Degraded => write!(f, "degraded"),
            DiskState::Draining => write!(f, "draining"),
            DiskState::Failed => write!(f, "failed"),
            DiskState::Removed => write!(f, "removed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_healthy_allows_allocation() {
        assert!(DiskState::Healthy.allows_allocation());
    }

    #[test]
    fn test_suspect_allows_allocation() {
        assert!(DiskState::Suspect.allows_allocation());
    }

    #[test]
    fn test_degraded_denies_allocation() {
        assert!(!DiskState::Degraded.allows_allocation());
    }

    #[test]
    fn test_draining_denies_allocation() {
        assert!(!DiskState::Draining.allows_allocation());
    }

    #[test]
    fn test_failed_denies_allocation() {
        assert!(!DiskState::Failed.allows_allocation());
    }

    #[test]
    fn test_removed_denies_allocation() {
        assert!(!DiskState::Removed.allows_allocation());
    }

    #[test]
    fn test_healthy_allows_reads() {
        assert!(DiskState::Healthy.allows_reads());
    }

    #[test]
    fn test_suspect_allows_reads() {
        assert!(DiskState::Suspect.allows_reads());
    }

    #[test]
    fn test_degraded_allows_reads() {
        assert!(DiskState::Degraded.allows_reads());
    }

    #[test]
    fn test_draining_allows_reads() {
        assert!(DiskState::Draining.allows_reads());
    }

    #[test]
    fn test_failed_denies_reads() {
        assert!(!DiskState::Failed.allows_reads());
    }

    #[test]
    fn test_removed_denies_reads() {
        assert!(!DiskState::Removed.allows_reads());
    }

    #[test]
    fn test_self_transition_allowed() {
        assert!(
            DiskState::Healthy
                .validate_transition(DiskState::Healthy)
                .is_ok()
        );
        assert!(
            DiskState::Failed
                .validate_transition(DiskState::Failed)
                .is_ok()
        );
    }

    #[test]
    fn test_healthy_to_suspect() {
        assert!(
            DiskState::Healthy
                .validate_transition(DiskState::Suspect)
                .is_ok()
        );
    }

    #[test]
    fn test_healthy_to_degraded() {
        assert!(
            DiskState::Healthy
                .validate_transition(DiskState::Degraded)
                .is_ok()
        );
    }

    #[test]
    fn test_healthy_to_draining() {
        assert!(
            DiskState::Healthy
                .validate_transition(DiskState::Draining)
                .is_ok()
        );
    }

    #[test]
    fn test_healthy_to_failed() {
        assert!(
            DiskState::Healthy
                .validate_transition(DiskState::Failed)
                .is_ok()
        );
    }

    #[test]
    fn test_healthy_to_removed_illegal() {
        assert!(
            DiskState::Healthy
                .validate_transition(DiskState::Removed)
                .is_err()
        );
    }

    #[test]
    fn test_suspect_to_healthy() {
        assert!(
            DiskState::Suspect
                .validate_transition(DiskState::Healthy)
                .is_ok()
        );
    }

    #[test]
    fn test_suspect_to_degraded() {
        assert!(
            DiskState::Suspect
                .validate_transition(DiskState::Degraded)
                .is_ok()
        );
    }

    #[test]
    fn test_suspect_to_draining() {
        assert!(
            DiskState::Suspect
                .validate_transition(DiskState::Draining)
                .is_ok()
        );
    }

    #[test]
    fn test_suspect_to_failed() {
        assert!(
            DiskState::Suspect
                .validate_transition(DiskState::Failed)
                .is_ok()
        );
    }

    #[test]
    fn test_suspect_to_removed_illegal() {
        assert!(
            DiskState::Suspect
                .validate_transition(DiskState::Removed)
                .is_err()
        );
    }

    #[test]
    fn test_degraded_to_draining() {
        assert!(
            DiskState::Degraded
                .validate_transition(DiskState::Draining)
                .is_ok()
        );
    }

    #[test]
    fn test_degraded_to_failed() {
        assert!(
            DiskState::Degraded
                .validate_transition(DiskState::Failed)
                .is_ok()
        );
    }

    #[test]
    fn test_degraded_to_removed() {
        assert!(
            DiskState::Degraded
                .validate_transition(DiskState::Removed)
                .is_ok()
        );
    }

    #[test]
    fn test_degraded_to_healthy_illegal() {
        assert!(
            DiskState::Degraded
                .validate_transition(DiskState::Healthy)
                .is_err()
        );
    }

    #[test]
    fn test_degraded_to_suspect_illegal() {
        assert!(
            DiskState::Degraded
                .validate_transition(DiskState::Suspect)
                .is_err()
        );
    }

    #[test]
    fn test_draining_to_failed() {
        assert!(
            DiskState::Draining
                .validate_transition(DiskState::Failed)
                .is_ok()
        );
    }

    #[test]
    fn test_draining_to_removed() {
        assert!(
            DiskState::Draining
                .validate_transition(DiskState::Removed)
                .is_ok()
        );
    }

    #[test]
    fn test_draining_to_healthy_illegal() {
        assert!(
            DiskState::Draining
                .validate_transition(DiskState::Healthy)
                .is_err()
        );
    }

    #[test]
    fn test_draining_to_suspect_illegal() {
        assert!(
            DiskState::Draining
                .validate_transition(DiskState::Suspect)
                .is_err()
        );
    }

    #[test]
    fn test_draining_to_degraded_illegal() {
        assert!(
            DiskState::Draining
                .validate_transition(DiskState::Degraded)
                .is_err()
        );
    }

    #[test]
    fn test_failed_to_removed() {
        assert!(
            DiskState::Failed
                .validate_transition(DiskState::Removed)
                .is_ok()
        );
    }

    #[test]
    fn test_failed_to_healthy_illegal() {
        assert!(
            DiskState::Failed
                .validate_transition(DiskState::Healthy)
                .is_err()
        );
    }

    #[test]
    fn test_failed_to_suspect_illegal() {
        assert!(
            DiskState::Failed
                .validate_transition(DiskState::Suspect)
                .is_err()
        );
    }

    #[test]
    fn test_failed_to_degraded_illegal() {
        assert!(
            DiskState::Failed
                .validate_transition(DiskState::Degraded)
                .is_err()
        );
    }

    #[test]
    fn test_failed_to_draining_illegal() {
        assert!(
            DiskState::Failed
                .validate_transition(DiskState::Draining)
                .is_err()
        );
    }

    #[test]
    fn test_removed_terminal() {
        assert!(
            DiskState::Removed
                .validate_transition(DiskState::Healthy)
                .is_err()
        );
        assert!(
            DiskState::Removed
                .validate_transition(DiskState::Suspect)
                .is_err()
        );
        assert!(
            DiskState::Removed
                .validate_transition(DiskState::Degraded)
                .is_err()
        );
        assert!(
            DiskState::Removed
                .validate_transition(DiskState::Draining)
                .is_err()
        );
        assert!(
            DiskState::Removed
                .validate_transition(DiskState::Failed)
                .is_err()
        );
    }

    #[test]
    fn test_display_all_states() {
        assert_eq!(DiskState::Healthy.to_string(), "healthy");
        assert_eq!(DiskState::Suspect.to_string(), "suspect");
        assert_eq!(DiskState::Degraded.to_string(), "degraded");
        assert_eq!(DiskState::Draining.to_string(), "draining");
        assert_eq!(DiskState::Failed.to_string(), "failed");
        assert_eq!(DiskState::Removed.to_string(), "removed");
    }

    #[test]
    fn test_serde_roundtrip() {
        for state in [
            DiskState::Healthy,
            DiskState::Suspect,
            DiskState::Degraded,
            DiskState::Draining,
            DiskState::Failed,
            DiskState::Removed,
        ] {
            let json = serde_json::to_string(&state).unwrap();
            let parsed: DiskState = serde_json::from_str(&json).unwrap();
            assert_eq!(state, parsed);
        }
    }

    #[test]
    fn test_transition_error_message() {
        let err = DiskState::Removed
            .validate_transition(DiskState::Healthy)
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("removed"));
        assert!(msg.contains("healthy"));
    }

    #[test]
    fn test_debug() {
        let debug = format!("{:?}", DiskState::Healthy);
        assert_eq!(debug, "Healthy");
    }
}
