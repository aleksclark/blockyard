use tracing::info;

pub struct UblkClient {
    volume_name: String,
    device_path: Option<String>,
}

impl UblkClient {
    pub fn new(volume_name: String) -> Self {
        Self {
            volume_name,
            device_path: None,
        }
    }

    pub fn volume_name(&self) -> &str {
        &self.volume_name
    }

    pub fn device_path(&self) -> Option<&str> {
        self.device_path.as_deref()
    }

    pub async fn mount(&mut self, device: Option<&str>) -> blockyard_common::Result<String> {
        let dev = device.unwrap_or("/dev/ublkb0").to_string();
        info!(volume = %self.volume_name, device = %dev, "mounting volume via UBLK");
        self.device_path = Some(dev.clone());
        Ok(dev)
    }

    pub async fn unmount(&mut self) -> blockyard_common::Result<()> {
        if let Some(dev) = &self.device_path {
            info!(device = %dev, "unmounting UBLK device");
        }
        self.device_path = None;
        Ok(())
    }
}
