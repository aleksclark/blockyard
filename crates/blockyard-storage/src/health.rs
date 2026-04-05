use crate::backend::StorageBackend;
use blockyard_common::types::{ZfsHealthState, ZfsPoolHealth};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{info, warn};

pub struct HealthMonitor {
    tx: watch::Sender<ZfsPoolHealth>,
    rx: watch::Receiver<ZfsPoolHealth>,
    interval: Duration,
}

impl HealthMonitor {
    pub fn new(interval: Duration) -> Self {
        let (tx, rx) = watch::channel(ZfsPoolHealth::default());
        Self { tx, rx, interval }
    }

    pub fn subscribe(&self) -> watch::Receiver<ZfsPoolHealth> {
        self.rx.clone()
    }

    pub fn current(&self) -> ZfsPoolHealth {
        self.rx.borrow().clone()
    }

    pub fn current_state(&self) -> ZfsHealthState {
        self.rx.borrow().state
    }

    pub async fn check_once<B: StorageBackend>(&self, backend: &B) -> ZfsPoolHealth {
        match backend.pool_health().await {
            Ok(health) => {
                let _ = self.tx.send(health.clone());
                health
            }
            Err(e) => {
                warn!(error = %e, "health check failed");
                let health = ZfsPoolHealth {
                    pool_name: backend.pool_name().to_string(),
                    state: ZfsHealthState::Unknown,
                    ..Default::default()
                };
                let _ = self.tx.send(health.clone());
                health
            }
        }
    }

    pub async fn run<B: StorageBackend + 'static>(self: Arc<Self>, backend: Arc<B>) {
        info!(interval = ?self.interval, "starting ZFS health monitor");
        loop {
            let health = self.check_once(backend.as_ref()).await;
            match health.state {
                ZfsHealthState::Online => {}
                ZfsHealthState::Degraded => {
                    warn!(pool = %health.pool_name, "ZFS pool DEGRADED");
                }
                ZfsHealthState::Faulted => {
                    warn!(pool = %health.pool_name, "ZFS pool FAULTED");
                }
                ZfsHealthState::Unknown => {
                    warn!(pool = %health.pool_name, "ZFS pool health unknown");
                }
            }
            tokio::time::sleep(self.interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemoryBackend;

    #[test]
    fn test_health_monitor_new() {
        let monitor = HealthMonitor::new(Duration::from_secs(10));
        assert_eq!(monitor.current_state(), ZfsHealthState::Unknown);
    }

    #[tokio::test]
    async fn test_health_monitor_check_once() {
        let monitor = HealthMonitor::new(Duration::from_secs(10));
        let backend = MemoryBackend::new("test".into(), 1024 * 1024 * 1024);
        let health = monitor.check_once(&backend).await;
        assert_eq!(health.state, ZfsHealthState::Online);
        assert_eq!(monitor.current_state(), ZfsHealthState::Online);
    }

    #[tokio::test]
    async fn test_health_monitor_subscribe() {
        let monitor = HealthMonitor::new(Duration::from_secs(10));
        let mut rx = monitor.subscribe();
        let backend = MemoryBackend::new("test".into(), 1024 * 1024 * 1024);

        monitor.check_once(&backend).await;

        rx.changed().await.unwrap();
        let health = rx.borrow();
        assert_eq!(health.state, ZfsHealthState::Online);
    }

    #[test]
    fn test_health_monitor_current_default() {
        let monitor = HealthMonitor::new(Duration::from_secs(5));
        let health = monitor.current();
        assert_eq!(health.state, ZfsHealthState::Unknown);
        assert!(health.pool_name.is_empty());
    }
}
