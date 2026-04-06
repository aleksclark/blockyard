use crate::cluster_client::ClusterClient;
use crate::nbd::NbdServer;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

pub struct UblkClient {
    volume_name: String,
    device_path: Option<String>,
    cluster: Option<Arc<ClusterClient>>,
    backend: MountBackend,
    nbd_server: Option<Arc<NbdServer>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountBackend {
    Ublk,
    Nbd,
}

impl std::str::FromStr for MountBackend {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ublk" => Ok(Self::Ublk),
            "nbd" => Ok(Self::Nbd),
            _ => Err(format!("unknown mount backend: {s}")),
        }
    }
}

impl std::fmt::Display for MountBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ublk => write!(f, "ublk"),
            Self::Nbd => write!(f, "nbd"),
        }
    }
}

/// Default volume size when the cluster hasn't told us the real value yet.
/// 1 GiB — the NBD device needs a concrete size to negotiate.
const DEFAULT_NBD_VOLUME_SIZE: u64 = 1024 * 1024 * 1024;

impl UblkClient {
    pub fn new(volume_name: String) -> Self {
        Self {
            volume_name,
            device_path: None,
            cluster: None,
            backend: MountBackend::Ublk,
            nbd_server: None,
        }
    }

    pub fn with_backend(mut self, backend: MountBackend) -> Self {
        self.backend = backend;
        self
    }

    pub fn with_cluster(mut self, cluster_addrs: Vec<SocketAddr>) -> Self {
        self.cluster = Some(Arc::new(ClusterClient::new(
            self.volume_name.clone(),
            cluster_addrs,
        )));
        self
    }

    pub fn volume_name(&self) -> &str {
        &self.volume_name
    }

    pub fn device_path(&self) -> Option<&str> {
        self.device_path.as_deref()
    }

    pub fn backend(&self) -> MountBackend {
        self.backend
    }

    pub async fn mount(&mut self, device: Option<&str>) -> blockyard_common::Result<String> {
        match self.backend {
            MountBackend::Ublk => {
                let dev_id = device
                    .and_then(|d| d.strip_prefix("/dev/ublkb"))
                    .and_then(|s| s.parse::<u32>().ok())
                    .unwrap_or(0);

                info!(
                    volume = %self.volume_name,
                    dev_id,
                    backend = %self.backend,
                    "mounting volume via UBLK"
                );

                let config = crate::ublk_server::UblkServerConfig {
                    device_id: Some(dev_id),
                    num_queues: 1,
                    queue_depth: 128,
                    io_buf_size: 512 * 1024,
                };
                let server = crate::ublk_server::UblkServer::new(config, 10 * 1024 * 1024 * 1024);
                let dev_path = server.start().await?;
                let dev = dev_path.to_string_lossy().to_string();
                self.device_path = Some(dev.clone());
                Ok(dev)
            }
            MountBackend::Nbd => {
                // Parse device id from the provided path or default to 0.
                let (dev_id, dev_path) = match device {
                    Some(p) => {
                        let id = parse_nbd_device_id(p).unwrap_or(0);
                        (id, p.to_string())
                    }
                    None => (0, "/dev/nbd0".to_string()),
                };

                info!(
                    volume = %self.volume_name,
                    device = %dev_path,
                    backend = %self.backend,
                    "mounting volume via NBD"
                );

                // Start the NBD TCP server.
                let nbd = Arc::new(NbdServer::new(dev_id, DEFAULT_NBD_VOLUME_SIZE));
                nbd.start().await?;
                let port = nbd.listen_port().ok_or_else(|| {
                    blockyard_common::Error::Protocol("NBD server did not bind".to_string())
                })?;

                // Run nbd-client to attach the kernel device.
                let output = tokio::process::Command::new("nbd-client")
                    .args([
                        "-N",
                        crate::nbd::EXPORT_NAME,
                        "localhost",
                        &port.to_string(),
                        &dev_path,
                    ])
                    .output()
                    .await;

                match output {
                    Ok(o) if o.status.success() => {
                        info!(device = %dev_path, port, "nbd-client connected");
                    }
                    Ok(o) => {
                        let stderr = String::from_utf8_lossy(&o.stderr);
                        tracing::warn!(
                            device = %dev_path,
                            error = %stderr,
                            "nbd-client failed to connect (device may not be available)"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            device = %dev_path,
                            error = %e,
                            "nbd-client binary not found — NBD server is running but device not attached"
                        );
                    }
                }

                self.nbd_server = Some(nbd);
                self.device_path = Some(dev_path.clone());
                Ok(dev_path)
            }
        }
    }

    pub async fn unmount(&mut self) -> blockyard_common::Result<()> {
        if let Some(dev) = &self.device_path {
            info!(device = %dev, "unmounting device");
        }

        if let Some(nbd) = self.nbd_server.take() {
            nbd.stop().await?;
        }

        self.device_path = None;
        Ok(())
    }

    pub fn is_mounted(&self) -> bool {
        self.device_path.is_some()
    }
}

/// Extract the numeric device id from a path like `/dev/nbd7`.
fn parse_nbd_device_id(path: &str) -> Option<u32> {
    let name = path.rsplit('/').next()?;
    let digits = name.trim_start_matches("nbd");
    digits.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ublk_client_new() {
        let client = UblkClient::new("vol-1".into());
        assert_eq!(client.volume_name(), "vol-1");
        assert!(client.device_path().is_none());
        assert!(!client.is_mounted());
        assert_eq!(client.backend(), MountBackend::Ublk);
    }

    #[test]
    fn test_ublk_client_with_backend() {
        let client = UblkClient::new("vol-1".into()).with_backend(MountBackend::Nbd);
        assert_eq!(client.backend(), MountBackend::Nbd);
    }

    #[test]
    fn test_ublk_client_with_cluster() {
        let addrs: Vec<SocketAddr> = vec!["10.0.0.1:7400".parse().unwrap()];
        let client = UblkClient::new("vol-1".into()).with_cluster(addrs);
        assert!(client.cluster.is_some());
    }

    #[tokio::test]
    async fn test_ublk_client_mount_default() {
        let mut client = UblkClient::new("vol-1".into());
        let dev = client.mount(None).await.unwrap();
        assert_eq!(dev, "/dev/ublkb0");
        assert!(client.is_mounted());
        assert_eq!(client.device_path(), Some("/dev/ublkb0"));
    }

    #[tokio::test]
    async fn test_ublk_client_mount_custom_device() {
        let mut client = UblkClient::new("vol-1".into());
        let dev = client.mount(Some("/dev/ublkb5")).await.unwrap();
        assert_eq!(dev, "/dev/ublkb5");
    }

    #[tokio::test]
    async fn test_nbd_client_mount_default() {
        let mut client = UblkClient::new("vol-1".into()).with_backend(MountBackend::Nbd);
        let dev = client.mount(None).await.unwrap();
        assert_eq!(dev, "/dev/nbd0");
        // The NBD server should be running even though nbd-client may not be available.
        assert!(client.nbd_server.is_some());
        // Clean up.
        client.unmount().await.unwrap();
    }

    #[tokio::test]
    async fn test_ublk_client_unmount() {
        let mut client = UblkClient::new("vol-1".into());
        client.mount(None).await.unwrap();
        assert!(client.is_mounted());
        client.unmount().await.unwrap();
        assert!(!client.is_mounted());
    }

    #[tokio::test]
    async fn test_ublk_client_unmount_not_mounted() {
        let mut client = UblkClient::new("vol-1".into());
        client.unmount().await.unwrap();
    }

    #[test]
    fn test_mount_backend_from_str() {
        assert_eq!("ublk".parse::<MountBackend>().unwrap(), MountBackend::Ublk);
        assert_eq!("nbd".parse::<MountBackend>().unwrap(), MountBackend::Nbd);
        assert_eq!("UBLK".parse::<MountBackend>().unwrap(), MountBackend::Ublk);
        assert!("bad".parse::<MountBackend>().is_err());
    }

    #[test]
    fn test_mount_backend_display() {
        assert_eq!(MountBackend::Ublk.to_string(), "ublk");
        assert_eq!(MountBackend::Nbd.to_string(), "nbd");
    }

    #[test]
    fn test_parse_nbd_device_id() {
        assert_eq!(parse_nbd_device_id("/dev/nbd0"), Some(0));
        assert_eq!(parse_nbd_device_id("/dev/nbd15"), Some(15));
        assert_eq!(parse_nbd_device_id("/dev/nbd"), None);
        assert_eq!(parse_nbd_device_id("/dev/sda1"), None);
    }
}
