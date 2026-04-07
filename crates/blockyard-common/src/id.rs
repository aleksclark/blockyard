//! Core identifier types for Blockyard entities.
//!
//! All IDs are newtypes providing type safety, Display/FromStr, and serde support.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! define_u64_id {
    ($(#[doc = $doc:expr])* $name:ident) => {
        $(#[doc = $doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        pub struct $name(u64);

        impl $name {
            pub fn new(val: u64) -> Self {
                Self(val)
            }

            pub fn as_u64(self) -> u64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl FromStr for $name {
            type Err = std::num::ParseIntError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                s.parse::<u64>().map(Self)
            }
        }

        impl From<u64> for $name {
            fn from(val: u64) -> Self {
                Self(val)
            }
        }
    };
}

macro_rules! define_uuid_id {
    ($(#[doc = $doc:expr])* $name:ident) => {
        $(#[doc = $doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct $name(Uuid);

        impl $name {
            pub fn new(val: Uuid) -> Self {
                Self(val)
            }

            pub fn generate() -> Self {
                Self(Uuid::new_v4())
            }

            pub fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                s.parse::<Uuid>().map(Self)
            }
        }

        impl From<Uuid> for $name {
            fn from(val: Uuid) -> Self {
                Self(val)
            }
        }
    };
}

define_uuid_id! {
    /// Unique identifier for a cluster node.
    NodeId
}

define_uuid_id! {
    /// Unique identifier for a logical volume.
    VolumeId
}

define_uuid_id! {
    /// Unique identifier for an extent (contiguous block range).
    ExtentId
}

define_uuid_id! {
    /// Unique identifier for a physical disk.
    DiskId
}

define_uuid_id! {
    /// Unique identifier for a client session.
    SessionId
}

define_uuid_id! {
    /// Unique identifier for a single write operation within a session.
    OperationId
}

define_u64_id! {
    /// Monotonically increasing Raft group identifier.
    RaftGroupId
}

define_u64_id! {
    /// Monotonically increasing placement epoch (§2.4).
    EpochId
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_id_display_roundtrip() {
        let id = NodeId::generate();
        let s = id.to_string();
        let parsed: NodeId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_node_id_from_uuid() {
        let uuid = Uuid::new_v4();
        let id = NodeId::new(uuid);
        assert_eq!(id.as_uuid(), uuid);
    }

    #[test]
    fn test_node_id_from_trait() {
        let uuid = Uuid::new_v4();
        let id: NodeId = uuid.into();
        assert_eq!(id.as_uuid(), uuid);
    }

    #[test]
    fn test_volume_id_display_roundtrip() {
        let id = VolumeId::generate();
        let s = id.to_string();
        let parsed: VolumeId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_extent_id_display_roundtrip() {
        let id = ExtentId::generate();
        let s = id.to_string();
        let parsed: ExtentId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_disk_id_display_roundtrip() {
        let id = DiskId::generate();
        let s = id.to_string();
        let parsed: DiskId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_session_id_display_roundtrip() {
        let id = SessionId::generate();
        let s = id.to_string();
        let parsed: SessionId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_operation_id_display_roundtrip() {
        let id = OperationId::generate();
        let s = id.to_string();
        let parsed: OperationId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_raft_group_id_display_roundtrip() {
        let id = RaftGroupId::new(42);
        let s = id.to_string();
        let parsed: RaftGroupId = s.parse().unwrap();
        assert_eq!(id, parsed);
        assert_eq!(id.as_u64(), 42);
    }

    #[test]
    fn test_raft_group_id_from_u64() {
        let id: RaftGroupId = 7u64.into();
        assert_eq!(id.as_u64(), 7);
    }

    #[test]
    fn test_epoch_id_display_roundtrip() {
        let id = EpochId::new(100);
        let s = id.to_string();
        let parsed: EpochId = s.parse().unwrap();
        assert_eq!(id, parsed);
        assert_eq!(id.as_u64(), 100);
    }

    #[test]
    fn test_epoch_id_ordering() {
        let a = EpochId::new(1);
        let b = EpochId::new(2);
        assert!(a < b);
    }

    #[test]
    fn test_uuid_id_parse_invalid() {
        let result = "not-a-uuid".parse::<NodeId>();
        assert!(result.is_err());
    }

    #[test]
    fn test_u64_id_parse_invalid() {
        let result = "not-a-number".parse::<RaftGroupId>();
        assert!(result.is_err());
    }

    #[test]
    fn test_uuid_id_equality() {
        let uuid = Uuid::new_v4();
        let a = NodeId::new(uuid);
        let b = NodeId::new(uuid);
        assert_eq!(a, b);
    }

    #[test]
    fn test_uuid_id_inequality() {
        let a = NodeId::generate();
        let b = NodeId::generate();
        assert_ne!(a, b);
    }

    #[test]
    fn test_u64_id_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(RaftGroupId::new(1));
        set.insert(RaftGroupId::new(2));
        set.insert(RaftGroupId::new(1));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_uuid_id_hash() {
        use std::collections::HashSet;
        let id = VolumeId::generate();
        let mut set = HashSet::new();
        set.insert(id);
        set.insert(id);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_uuid_id_serde_roundtrip() {
        let id = NodeId::generate();
        let json = serde_json::to_string(&id).unwrap();
        let parsed: NodeId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_u64_id_serde_roundtrip() {
        let id = EpochId::new(999);
        let json = serde_json::to_string(&id).unwrap();
        let parsed: EpochId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_uuid_id_debug() {
        let id = NodeId::generate();
        let debug = format!("{:?}", id);
        assert!(debug.starts_with("NodeId("));
    }

    #[test]
    fn test_u64_id_debug() {
        let id = RaftGroupId::new(42);
        let debug = format!("{:?}", id);
        assert_eq!(debug, "RaftGroupId(42)");
    }

    #[test]
    fn test_u64_id_clone_copy() {
        let id = EpochId::new(5);
        let cloned = id.clone();
        let copied = id;
        assert_eq!(id, cloned);
        assert_eq!(id, copied);
    }

    #[test]
    fn test_uuid_id_clone_copy() {
        let id = DiskId::generate();
        let cloned = id.clone();
        let copied = id;
        assert_eq!(id, cloned);
        assert_eq!(id, copied);
    }
}
