//! Blockyard storage engine — extent files, disk management, and placement.
//!
//! Manages per-disk XFS filesystems, extent file lifecycle (staging, commit,
//! immutability), local extent index, and background scrub/repair.
