//! HTTP-based BlockyardClient implementation using reqwest.

use anyhow::Result;

use blockyard_common::{DiskId, NodeId, VolumeId};

use crate::client::BlockyardClient;
use crate::types::{ClusterStatus, DiskInfo, MountInfo, NodeInfo, VolumeCreateParams, VolumeInfo};

#[derive(Debug)]
pub struct HttpClient {
    base_url: String,
    client: reqwest::Client,
}

impl HttpClient {
    pub fn new(endpoint: &str) -> Self {
        let base_url = endpoint.trim_end_matches('/').to_string();
        Self {
            base_url,
            client: reqwest::Client::new(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
}

impl BlockyardClient for HttpClient {
    async fn volume_create(&self, params: VolumeCreateParams) -> Result<VolumeInfo> {
        let resp = self
            .client
            .post(self.url("/api/v1/volumes"))
            .json(&params)
            .send()
            .await?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("create volume failed: {}", text);
        }
        Ok(resp.json().await?)
    }

    async fn volume_delete(&self, id: VolumeId) -> Result<()> {
        let resp = self
            .client
            .delete(self.url(&format!("/api/v1/volumes/{}", id)))
            .send()
            .await?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("delete volume failed: {}", text);
        }
        Ok(())
    }

    async fn volume_list(&self) -> Result<Vec<VolumeInfo>> {
        let resp = self.client.get(self.url("/api/v1/volumes")).send().await?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("list volumes failed: {}", text);
        }
        Ok(resp.json().await?)
    }

    async fn volume_inspect(&self, id: VolumeId) -> Result<VolumeInfo> {
        let resp = self
            .client
            .get(self.url(&format!("/api/v1/volumes/{}", id)))
            .send()
            .await?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("inspect volume failed: {}", text);
        }
        Ok(resp.json().await?)
    }

    async fn disk_list(&self) -> Result<Vec<DiskInfo>> {
        let resp = self.client.get(self.url("/api/v1/disks")).send().await?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("list disks failed: {}", text);
        }
        Ok(resp.json().await?)
    }

    async fn disk_inspect(&self, _id: DiskId) -> Result<DiskInfo> {
        anyhow::bail!("disk inspect not yet supported via HTTP")
    }

    async fn disk_drain(&self, _id: DiskId) -> Result<()> {
        anyhow::bail!("disk drain not yet supported via HTTP")
    }

    async fn disk_remove(&self, _id: DiskId) -> Result<()> {
        anyhow::bail!("disk remove not yet supported via HTTP")
    }

    async fn node_list(&self) -> Result<Vec<NodeInfo>> {
        let resp = self.client.get(self.url("/api/v1/nodes")).send().await?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("list nodes failed: {}", text);
        }
        Ok(resp.json().await?)
    }

    async fn node_inspect(&self, id: NodeId) -> Result<NodeInfo> {
        let resp = self
            .client
            .get(self.url(&format!("/api/v1/nodes/{}", id)))
            .send()
            .await?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("inspect node failed: {}", text);
        }
        Ok(resp.json().await?)
    }

    async fn node_decommission(&self, _id: NodeId) -> Result<()> {
        anyhow::bail!("node decommission not yet supported via HTTP")
    }

    async fn cluster_status(&self) -> Result<ClusterStatus> {
        let resp = self
            .client
            .get(self.url("/api/v1/cluster/status"))
            .send()
            .await?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("cluster status failed: {}", text);
        }
        Ok(resp.json().await?)
    }

    async fn mount(&self, _volume_id: VolumeId, _device_path: Option<String>) -> Result<MountInfo> {
        anyhow::bail!("mount not yet supported via HTTP")
    }

    async fn unmount(&self, _volume_id: VolumeId) -> Result<()> {
        anyhow::bail!("unmount not yet supported via HTTP")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_client_new() {
        let client = HttpClient::new("http://127.0.0.1:9801");
        assert_eq!(client.base_url, "http://127.0.0.1:9801");
    }

    #[test]
    fn test_http_client_trailing_slash() {
        let client = HttpClient::new("http://127.0.0.1:9801/");
        assert_eq!(client.base_url, "http://127.0.0.1:9801");
    }

    #[test]
    fn test_http_client_url() {
        let client = HttpClient::new("http://127.0.0.1:9801");
        assert_eq!(
            client.url("/api/v1/volumes"),
            "http://127.0.0.1:9801/api/v1/volumes"
        );
    }

    #[test]
    fn test_http_client_debug() {
        let client = HttpClient::new("http://127.0.0.1:9801");
        let debug = format!("{:?}", client);
        assert!(debug.contains("HttpClient"));
    }
}
