use blockyard_common::types::RaftGroupId;

pub struct VolumeGroup {
    group_id: RaftGroupId,
    volume_name: String,
}

impl VolumeGroup {
    pub fn new(group_id: RaftGroupId, volume_name: String) -> Self {
        Self {
            group_id,
            volume_name,
        }
    }

    pub fn group_id(&self) -> RaftGroupId {
        self.group_id
    }

    pub fn volume_name(&self) -> &str {
        &self.volume_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_volume_group() {
        let vg = VolumeGroup::new(42, "web-db".into());
        assert_eq!(vg.group_id(), 42);
        assert_eq!(vg.volume_name(), "web-db");
    }
}
