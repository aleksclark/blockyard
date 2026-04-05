//! Prometheus HTTP metrics endpoint.
//!
//! Wraps `metrics-exporter-prometheus` to expose a `/metrics` HTTP scrape
//! endpoint that Prometheus can poll.

use std::net::SocketAddr;

use tracing::info;

use crate::error::{Error, Result};

/// A thin wrapper around the Prometheus exporter that serves metrics over HTTP.
#[derive(Debug)]
pub struct MetricsServer {
    addr: SocketAddr,
}

impl MetricsServer {
    /// Create a new [`MetricsServer`] bound to the given address.
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    /// Install the Prometheus recorder globally and spawn an HTTP listener.
    ///
    /// The server responds to **any** `GET` request with the current set of
    /// metrics in the Prometheus text exposition format.
    ///
    /// This method must be called **once** per process; calling it a second
    /// time will return an error because a global recorder is already installed.
    pub fn start(&self) -> Result<()> {
        metrics_exporter_prometheus::PrometheusBuilder::new()
            .with_http_listener(self.addr)
            .install()
            .map_err(|e| Error::Config(format!("failed to start metrics server: {e}")))?;

        info!(addr = %self.addr, "prometheus metrics server started");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_server_new() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = MetricsServer::new(addr);
        assert_eq!(format!("{:?}", server), format!("MetricsServer {{ addr: {addr} }}"));
    }

    // NOTE: We intentionally do **not** call `start()` in unit tests because
    // it installs a global recorder which is a one-time operation and would
    // conflict across tests run in the same process.  Integration tests
    // should cover the full HTTP round-trip.
}
