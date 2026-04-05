pub const META_GROUP_ID: u64 = 0;

pub struct MetaGroup;

impl MetaGroup {
    pub fn group_id() -> u64 {
        META_GROUP_ID
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_meta_group_id() {
        assert_eq!(MetaGroup::group_id(), 0);
    }
}
