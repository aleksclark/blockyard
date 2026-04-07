//! Per-disk telemetry collection and health-derived state (§5.8).
//!
//! Collects read/write errors, checksum mismatches, media errors,
//! timeouts, temperature, and latency outliers. Derives a [`DiskState`]
//! from observed telemetry and configurable policy thresholds.

use blockyard_common::{DiskId, DiskState};

/// Telemetry snapshot for a single disk.
#[derive(Debug, Clone, Default)]
pub struct DiskTelemetry {
    pub read_errors: u64,
    pub write_errors: u64,
    pub checksum_mismatches: u64,
    pub media_errors: u64,
    pub timeouts: u64,
    pub temperature_celsius: Option<u32>,
    pub latency_p99_us: Option<u64>,
}

/// Policy thresholds for deriving disk state from telemetry.
#[derive(Debug, Clone)]
pub struct HealthPolicy {
    pub suspect_error_threshold: u64,
    pub degraded_error_threshold: u64,
    pub failed_error_threshold: u64,
    pub temperature_warning_celsius: u32,
    pub temperature_critical_celsius: u32,
}

impl Default for HealthPolicy {
    fn default() -> Self {
        Self {
            suspect_error_threshold: 5,
            degraded_error_threshold: 20,
            failed_error_threshold: 100,
            temperature_warning_celsius: 60,
            temperature_critical_celsius: 75,
        }
    }
}

/// Tracks cumulative telemetry for a disk and derives state from it.
#[derive(Debug)]
pub struct DiskHealthTracker {
    pub disk_id: DiskId,
    pub cumulative: DiskTelemetry,
    pub policy: HealthPolicy,
}

impl DiskHealthTracker {
    pub fn new(disk_id: DiskId) -> Self {
        Self {
            disk_id,
            cumulative: DiskTelemetry::default(),
            policy: HealthPolicy::default(),
        }
    }

    pub fn with_policy(disk_id: DiskId, policy: HealthPolicy) -> Self {
        Self {
            disk_id,
            cumulative: DiskTelemetry::default(),
            policy,
        }
    }

    /// Record a telemetry snapshot, accumulating errors.
    pub fn record(&mut self, telemetry: &DiskTelemetry) {
        self.cumulative.read_errors += telemetry.read_errors;
        self.cumulative.write_errors += telemetry.write_errors;
        self.cumulative.checksum_mismatches += telemetry.checksum_mismatches;
        self.cumulative.media_errors += telemetry.media_errors;
        self.cumulative.timeouts += telemetry.timeouts;

        if telemetry.temperature_celsius.is_some() {
            self.cumulative.temperature_celsius = telemetry.temperature_celsius;
        }
        if telemetry.latency_p99_us.is_some() {
            self.cumulative.latency_p99_us = telemetry.latency_p99_us;
        }
    }

    /// Derive the disk state from accumulated telemetry.
    pub fn derive_state(&self) -> Option<DiskState> {
        let total_errors = self.cumulative.read_errors
            + self.cumulative.write_errors
            + self.cumulative.checksum_mismatches
            + self.cumulative.media_errors;

        if total_errors >= self.policy.failed_error_threshold {
            return Some(DiskState::Failed);
        }

        if let Some(temp) = self.cumulative.temperature_celsius {
            if temp >= self.policy.temperature_critical_celsius {
                return Some(DiskState::Failed);
            }
        }

        if total_errors >= self.policy.degraded_error_threshold {
            return Some(DiskState::Degraded);
        }

        if let Some(temp) = self.cumulative.temperature_celsius {
            if temp >= self.policy.temperature_warning_celsius {
                return Some(DiskState::Suspect);
            }
        }

        if total_errors >= self.policy.suspect_error_threshold {
            return Some(DiskState::Suspect);
        }

        None
    }

    /// Total accumulated error count.
    pub fn total_errors(&self) -> u64 {
        self.cumulative.read_errors
            + self.cumulative.write_errors
            + self.cumulative.checksum_mismatches
            + self.cumulative.media_errors
            + self.cumulative.timeouts
    }

    /// Reset accumulated telemetry.
    pub fn reset(&mut self) {
        self.cumulative = DiskTelemetry::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_tracker_no_derived_state() {
        let tracker = DiskHealthTracker::new(DiskId::generate());
        assert!(tracker.derive_state().is_none());
    }

    #[test]
    fn test_record_accumulates() {
        let mut tracker = DiskHealthTracker::new(DiskId::generate());

        let t1 = DiskTelemetry {
            read_errors: 2,
            write_errors: 1,
            ..Default::default()
        };
        tracker.record(&t1);
        assert_eq!(tracker.cumulative.read_errors, 2);
        assert_eq!(tracker.cumulative.write_errors, 1);

        let t2 = DiskTelemetry {
            read_errors: 3,
            write_errors: 0,
            ..Default::default()
        };
        tracker.record(&t2);
        assert_eq!(tracker.cumulative.read_errors, 5);
        assert_eq!(tracker.cumulative.write_errors, 1);
    }

    #[test]
    fn test_derive_suspect_from_errors() {
        let mut tracker = DiskHealthTracker::new(DiskId::generate());
        tracker.record(&DiskTelemetry {
            read_errors: 5,
            ..Default::default()
        });
        assert_eq!(tracker.derive_state(), Some(DiskState::Suspect));
    }

    #[test]
    fn test_derive_degraded_from_errors() {
        let mut tracker = DiskHealthTracker::new(DiskId::generate());
        tracker.record(&DiskTelemetry {
            read_errors: 10,
            write_errors: 10,
            ..Default::default()
        });
        assert_eq!(tracker.derive_state(), Some(DiskState::Degraded));
    }

    #[test]
    fn test_derive_failed_from_errors() {
        let mut tracker = DiskHealthTracker::new(DiskId::generate());
        tracker.record(&DiskTelemetry {
            media_errors: 100,
            ..Default::default()
        });
        assert_eq!(tracker.derive_state(), Some(DiskState::Failed));
    }

    #[test]
    fn test_derive_suspect_from_temperature() {
        let mut tracker = DiskHealthTracker::new(DiskId::generate());
        tracker.record(&DiskTelemetry {
            temperature_celsius: Some(65),
            ..Default::default()
        });
        assert_eq!(tracker.derive_state(), Some(DiskState::Suspect));
    }

    #[test]
    fn test_derive_failed_from_temperature() {
        let mut tracker = DiskHealthTracker::new(DiskId::generate());
        tracker.record(&DiskTelemetry {
            temperature_celsius: Some(80),
            ..Default::default()
        });
        assert_eq!(tracker.derive_state(), Some(DiskState::Failed));
    }

    #[test]
    fn test_total_errors() {
        let mut tracker = DiskHealthTracker::new(DiskId::generate());
        tracker.record(&DiskTelemetry {
            read_errors: 1,
            write_errors: 2,
            checksum_mismatches: 3,
            media_errors: 4,
            timeouts: 5,
            ..Default::default()
        });
        assert_eq!(tracker.total_errors(), 15);
    }

    #[test]
    fn test_reset() {
        let mut tracker = DiskHealthTracker::new(DiskId::generate());
        tracker.record(&DiskTelemetry {
            read_errors: 10,
            ..Default::default()
        });
        assert!(tracker.derive_state().is_some());
        tracker.reset();
        assert!(tracker.derive_state().is_none());
        assert_eq!(tracker.total_errors(), 0);
    }

    #[test]
    fn test_custom_policy() {
        let policy = HealthPolicy {
            suspect_error_threshold: 1,
            degraded_error_threshold: 2,
            failed_error_threshold: 3,
            temperature_warning_celsius: 40,
            temperature_critical_celsius: 50,
        };

        let mut tracker = DiskHealthTracker::with_policy(DiskId::generate(), policy);
        tracker.record(&DiskTelemetry {
            read_errors: 1,
            ..Default::default()
        });
        assert_eq!(tracker.derive_state(), Some(DiskState::Suspect));

        tracker.record(&DiskTelemetry {
            read_errors: 1,
            ..Default::default()
        });
        assert_eq!(tracker.derive_state(), Some(DiskState::Degraded));

        tracker.record(&DiskTelemetry {
            read_errors: 1,
            ..Default::default()
        });
        assert_eq!(tracker.derive_state(), Some(DiskState::Failed));
    }

    #[test]
    fn test_latency_recorded() {
        let mut tracker = DiskHealthTracker::new(DiskId::generate());
        tracker.record(&DiskTelemetry {
            latency_p99_us: Some(1000),
            ..Default::default()
        });
        assert_eq!(tracker.cumulative.latency_p99_us, Some(1000));

        tracker.record(&DiskTelemetry {
            latency_p99_us: Some(2000),
            ..Default::default()
        });
        assert_eq!(tracker.cumulative.latency_p99_us, Some(2000));
    }

    #[test]
    fn test_temperature_latest_wins() {
        let mut tracker = DiskHealthTracker::new(DiskId::generate());
        tracker.record(&DiskTelemetry {
            temperature_celsius: Some(30),
            ..Default::default()
        });
        assert_eq!(tracker.cumulative.temperature_celsius, Some(30));

        tracker.record(&DiskTelemetry {
            temperature_celsius: Some(45),
            ..Default::default()
        });
        assert_eq!(tracker.cumulative.temperature_celsius, Some(45));
    }

    #[test]
    fn test_default_telemetry() {
        let t = DiskTelemetry::default();
        assert_eq!(t.read_errors, 0);
        assert_eq!(t.write_errors, 0);
        assert!(t.temperature_celsius.is_none());
    }

    #[test]
    fn test_default_policy_values() {
        let p = HealthPolicy::default();
        assert_eq!(p.suspect_error_threshold, 5);
        assert_eq!(p.degraded_error_threshold, 20);
        assert_eq!(p.failed_error_threshold, 100);
        assert_eq!(p.temperature_warning_celsius, 60);
        assert_eq!(p.temperature_critical_celsius, 75);
    }

    #[test]
    fn test_below_suspect_threshold_no_state() {
        let mut tracker = DiskHealthTracker::new(DiskId::generate());
        tracker.record(&DiskTelemetry {
            read_errors: 4,
            ..Default::default()
        });
        assert!(tracker.derive_state().is_none());
    }
}
