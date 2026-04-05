use crate::cluster_client::ClusterClient;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

pub struct UblkClient {
    volume_name: String,
    device_path: Option<String>,
    cluster: Option<Arc<ClusterClient>>,
    backend: MountBackend,
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

impl UblkClient {
    pub fn new(volume_name: String) -> Self {
        Self {
            volume_name,
            device_path: None,
            cluster: None,
            backend: MountBackend::Ublk,
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
        let dev = match self.backend {
            MountBackend::Ublk => device.unwrap_or("/dev/ublkb0").to_string(),
            MountBackend::Nbd => device.unwrap_or("/dev/nbd0").to_string(),
        };

        info!(
            volume = %self.volume_name,
            device = %dev,
            backend = %self.backend,
            "mounting volume"
        );

        self.device_path = Some(dev.clone());
        Ok(dev)
    }

    pub async fn unmount(&mut self) -> blockyard_common::Result<()> {
        if let Some(dev) = &self.device_path {
            info!(device = %dev, "unmounting device");
        }
        self.device_path = None;
        Ok(())
    }

    pub fn is_mounted(&self) -> bool {
        self.device_path.is_some()
    }
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
}
