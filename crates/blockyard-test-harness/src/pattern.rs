use std::collections::HashMap;
use std::fmt;

use blockyard_common::VolumeId;

#[derive(Debug, Clone)]
pub struct PatternBlock {
    pub volume_id: VolumeId,
    pub offset: u64,
    pub data: Vec<u8>,
    pub checksum: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternKind {
    Deterministic,
    Alternating,
    Ascending,
    Checkerboard,
}

impl fmt::Display for PatternKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PatternKind::Deterministic => write!(f, "deterministic"),
            PatternKind::Alternating => write!(f, "alternating"),
            PatternKind::Ascending => write!(f, "ascending"),
            PatternKind::Checkerboard => write!(f, "checkerboard"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PatternConfig {
    pub seed: u64,
    pub block_size: u32,
    pub block_count: u64,
    pub kind: PatternKind,
}

impl Default for PatternConfig {
    fn default() -> Self {
        Self {
            seed: 0xDEAD_BEEF_CAFE_BABE,
            block_size: 4096,
            block_count: 64,
            kind: PatternKind::Deterministic,
        }
    }
}

pub struct PatternGenerator {
    config: PatternConfig,
    volume_id: VolumeId,
}

impl fmt::Debug for PatternGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PatternGenerator")
            .field("config", &self.config)
            .field("volume_id", &self.volume_id)
            .finish()
    }
}

impl PatternGenerator {
    pub fn new(config: PatternConfig, volume_id: VolumeId) -> Self {
        Self { config, volume_id }
    }

    pub fn config(&self) -> &PatternConfig {
        &self.config
    }

    pub fn volume_id(&self) -> VolumeId {
        self.volume_id
    }

    pub fn generate_block(&self, block_index: u64) -> PatternBlock {
        let offset = block_index * self.config.block_size as u64;
        let data = self.generate_data(block_index);
        let checksum = blake3::hash(&data).to_hex().to_string();

        PatternBlock {
            volume_id: self.volume_id,
            offset,
            data,
            checksum,
        }
    }

    pub fn generate_all(&self) -> Vec<PatternBlock> {
        (0..self.config.block_count)
            .map(|i| self.generate_block(i))
            .collect()
    }

    pub fn verify_block(&self, block_index: u64, data: &[u8]) -> PatternVerifyResult {
        let expected = self.generate_block(block_index);

        if data.len() != expected.data.len() {
            return PatternVerifyResult::LengthMismatch {
                block_index,
                expected: expected.data.len(),
                actual: data.len(),
            };
        }

        let actual_checksum = blake3::hash(data).to_hex().to_string();
        if actual_checksum != expected.checksum {
            let first_diff = data
                .iter()
                .zip(expected.data.iter())
                .position(|(a, b)| a != b);

            return PatternVerifyResult::DataMismatch {
                block_index,
                expected_checksum: expected.checksum,
                actual_checksum,
                first_diff_offset: first_diff,
            };
        }

        PatternVerifyResult::Ok { block_index }
    }

    pub fn verify_all(&self, blocks: &HashMap<u64, Vec<u8>>) -> Vec<PatternVerifyResult> {
        let mut results = Vec::new();

        for block_index in 0..self.config.block_count {
            let offset = block_index * self.config.block_size as u64;
            match blocks.get(&offset) {
                Some(data) => results.push(self.verify_block(block_index, data)),
                None => results.push(PatternVerifyResult::Missing { block_index }),
            }
        }

        results
    }

    fn generate_data(&self, block_index: u64) -> Vec<u8> {
        let size = self.config.block_size as usize;
        let mut data = vec![0u8; size];

        match self.config.kind {
            PatternKind::Deterministic => {
                self.fill_deterministic(&mut data, block_index);
            }
            PatternKind::Alternating => {
                self.fill_alternating(&mut data, block_index);
            }
            PatternKind::Ascending => {
                self.fill_ascending(&mut data, block_index);
            }
            PatternKind::Checkerboard => {
                self.fill_checkerboard(&mut data, block_index);
            }
        }

        data
    }

    fn fill_deterministic(&self, data: &mut [u8], block_index: u64) {
        let mut state = self.config.seed ^ block_index;
        for byte in data.iter_mut() {
            state = xorshift64(state);
            *byte = state as u8;
        }
    }

    fn fill_alternating(&self, data: &mut [u8], block_index: u64) {
        let pattern_a = (self.config.seed & 0xFF) as u8;
        let pattern_b = ((self.config.seed >> 8) & 0xFF) as u8;
        let base = (block_index & 1) as u8;

        for (i, byte) in data.iter_mut().enumerate() {
            *byte = if (i as u8).wrapping_add(base) % 2 == 0 {
                pattern_a
            } else {
                pattern_b
            };
        }
    }

    fn fill_ascending(&self, data: &mut [u8], block_index: u64) {
        let base = (self.config.seed ^ block_index) as u8;
        for (i, byte) in data.iter_mut().enumerate() {
            *byte = base.wrapping_add(i as u8);
        }
    }

    fn fill_checkerboard(&self, data: &mut [u8], block_index: u64) {
        let cell_size = 64usize;
        let seed_byte = ((self.config.seed ^ block_index) & 0xFF) as u8;
        let inv_byte = !seed_byte;

        for (i, byte) in data.iter_mut().enumerate() {
            let cell = i / cell_size;
            *byte = if cell % 2 == 0 { seed_byte } else { inv_byte };
        }
    }
}

fn xorshift64(mut state: u64) -> u64 {
    if state == 0 {
        state = 1;
    }
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    state
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PatternVerifyResult {
    Ok {
        block_index: u64,
    },
    Missing {
        block_index: u64,
    },
    LengthMismatch {
        block_index: u64,
        expected: usize,
        actual: usize,
    },
    DataMismatch {
        block_index: u64,
        expected_checksum: String,
        actual_checksum: String,
        first_diff_offset: Option<usize>,
    },
}

impl PatternVerifyResult {
    pub fn is_ok(&self) -> bool {
        matches!(self, PatternVerifyResult::Ok { .. })
    }
}

impl fmt::Display for PatternVerifyResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PatternVerifyResult::Ok { block_index } => {
                write!(f, "block {}: OK", block_index)
            }
            PatternVerifyResult::Missing { block_index } => {
                write!(f, "block {}: MISSING", block_index)
            }
            PatternVerifyResult::LengthMismatch {
                block_index,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "block {}: LENGTH MISMATCH (expected={}, actual={})",
                    block_index, expected, actual
                )
            }
            PatternVerifyResult::DataMismatch {
                block_index,
                expected_checksum,
                actual_checksum,
                first_diff_offset,
            } => {
                write!(
                    f,
                    "block {}: DATA MISMATCH (expected={}, actual={}, first_diff={:?})",
                    block_index, expected_checksum, actual_checksum, first_diff_offset
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_volume_id() -> VolumeId {
        VolumeId::generate()
    }

    #[test]
    fn test_pattern_kind_display() {
        assert_eq!(format!("{}", PatternKind::Deterministic), "deterministic");
        assert_eq!(format!("{}", PatternKind::Alternating), "alternating");
        assert_eq!(format!("{}", PatternKind::Ascending), "ascending");
        assert_eq!(format!("{}", PatternKind::Checkerboard), "checkerboard");
    }

    #[test]
    fn test_pattern_config_default() {
        let config = PatternConfig::default();
        assert_eq!(config.seed, 0xDEAD_BEEF_CAFE_BABE);
        assert_eq!(config.block_size, 4096);
        assert_eq!(config.block_count, 64);
        assert_eq!(config.kind, PatternKind::Deterministic);
    }

    #[test]
    fn test_generate_block_deterministic() {
        let vol = test_volume_id();
        let pgen = PatternGenerator::new(PatternConfig::default(), vol);

        let block = pgen.generate_block(0);
        assert_eq!(block.volume_id, vol);
        assert_eq!(block.offset, 0);
        assert_eq!(block.data.len(), 4096);
        assert!(!block.checksum.is_empty());
        assert!(
            block.data.iter().any(|&b| b != 0),
            "data should not be all zeros"
        );
    }

    #[test]
    fn test_generate_block_reproducible() {
        let vol = test_volume_id();
        let pgen = PatternGenerator::new(PatternConfig::default(), vol);

        let block1 = pgen.generate_block(5);
        let block2 = pgen.generate_block(5);

        assert_eq!(block1.data, block2.data);
        assert_eq!(block1.checksum, block2.checksum);
    }

    #[test]
    fn test_generate_different_blocks_differ() {
        let vol = test_volume_id();
        let pgen = PatternGenerator::new(PatternConfig::default(), vol);

        let block0 = pgen.generate_block(0);
        let block1 = pgen.generate_block(1);

        assert_ne!(block0.data, block1.data);
        assert_ne!(block0.checksum, block1.checksum);
    }

    #[test]
    fn test_generate_all() {
        let vol = test_volume_id();
        let config = PatternConfig {
            block_count: 8,
            ..Default::default()
        };
        let pgen = PatternGenerator::new(config, vol);

        let blocks = pgen.generate_all();
        assert_eq!(blocks.len(), 8);

        for (i, block) in blocks.iter().enumerate() {
            assert_eq!(block.offset, i as u64 * 4096);
        }
    }

    #[test]
    fn test_verify_block_ok() {
        let vol = test_volume_id();
        let pgen = PatternGenerator::new(PatternConfig::default(), vol);

        let block = pgen.generate_block(3);
        let result = pgen.verify_block(3, &block.data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_block_data_mismatch() {
        let vol = test_volume_id();
        let pgen = PatternGenerator::new(PatternConfig::default(), vol);

        let mut block = pgen.generate_block(3);
        block.data[42] ^= 0xFF;
        let result = pgen.verify_block(3, &block.data);
        assert!(!result.is_ok());
        assert!(matches!(result, PatternVerifyResult::DataMismatch { .. }));
    }

    #[test]
    fn test_verify_block_length_mismatch() {
        let vol = test_volume_id();
        let pgen = PatternGenerator::new(PatternConfig::default(), vol);

        let result = pgen.verify_block(0, &[0u8; 100]);
        assert!(matches!(result, PatternVerifyResult::LengthMismatch { .. }));
    }

    #[test]
    fn test_verify_all_complete() {
        let vol = test_volume_id();
        let config = PatternConfig {
            block_count: 4,
            ..Default::default()
        };
        let pgen = PatternGenerator::new(config, vol);

        let blocks_vec = pgen.generate_all();
        let mut blocks_map = HashMap::new();
        for block in &blocks_vec {
            blocks_map.insert(block.offset, block.data.clone());
        }

        let results = pgen.verify_all(&blocks_map);
        assert_eq!(results.len(), 4);
        assert!(results.iter().all(|r| r.is_ok()));
    }

    #[test]
    fn test_verify_all_missing_block() {
        let vol = test_volume_id();
        let config = PatternConfig {
            block_count: 4,
            ..Default::default()
        };
        let pgen = PatternGenerator::new(config, vol);

        let blocks_map = HashMap::new();
        let results = pgen.verify_all(&blocks_map);
        assert_eq!(results.len(), 4);
        assert!(
            results
                .iter()
                .all(|r| matches!(r, PatternVerifyResult::Missing { .. }))
        );
    }

    #[test]
    fn test_alternating_pattern() {
        let vol = test_volume_id();
        let config = PatternConfig {
            kind: PatternKind::Alternating,
            block_count: 2,
            ..Default::default()
        };
        let pgen = PatternGenerator::new(config, vol);

        let block = pgen.generate_block(0);
        assert_eq!(block.data.len(), 4096);

        let result = pgen.verify_block(0, &block.data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_ascending_pattern() {
        let vol = test_volume_id();
        let config = PatternConfig {
            kind: PatternKind::Ascending,
            block_count: 2,
            ..Default::default()
        };
        let pgen = PatternGenerator::new(config, vol);

        let block = pgen.generate_block(0);
        assert_eq!(block.data.len(), 4096);

        let result = pgen.verify_block(0, &block.data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_checkerboard_pattern() {
        let vol = test_volume_id();
        let config = PatternConfig {
            kind: PatternKind::Checkerboard,
            block_count: 2,
            ..Default::default()
        };
        let pgen = PatternGenerator::new(config, vol);

        let block = pgen.generate_block(0);
        assert_eq!(block.data.len(), 4096);

        let result = pgen.verify_block(0, &block.data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_different_seeds_produce_different_data() {
        let vol = test_volume_id();
        let pgen1 = PatternGenerator::new(
            PatternConfig {
                seed: 1234,
                ..Default::default()
            },
            vol,
        );
        let pgen2 = PatternGenerator::new(
            PatternConfig {
                seed: 5678,
                ..Default::default()
            },
            vol,
        );

        let block1 = pgen1.generate_block(0);
        let block2 = pgen2.generate_block(0);
        assert_ne!(block1.data, block2.data);
    }

    #[test]
    fn test_xorshift64_nonzero_input() {
        let result = xorshift64(42);
        assert_ne!(result, 0);
        assert_ne!(result, 42);
    }

    #[test]
    fn test_xorshift64_zero_input() {
        let result = xorshift64(0);
        assert_ne!(result, 0);
    }

    #[test]
    fn test_pattern_verify_result_display() {
        assert!(format!("{}", PatternVerifyResult::Ok { block_index: 0 }).contains("OK"));
        assert!(format!("{}", PatternVerifyResult::Missing { block_index: 1 }).contains("MISSING"));
        assert!(
            format!(
                "{}",
                PatternVerifyResult::LengthMismatch {
                    block_index: 2,
                    expected: 4096,
                    actual: 100
                }
            )
            .contains("LENGTH MISMATCH")
        );
        assert!(
            format!(
                "{}",
                PatternVerifyResult::DataMismatch {
                    block_index: 3,
                    expected_checksum: "abc".to_string(),
                    actual_checksum: "def".to_string(),
                    first_diff_offset: Some(42),
                }
            )
            .contains("DATA MISMATCH")
        );
    }

    #[test]
    fn test_pattern_generator_accessors() {
        let vol = test_volume_id();
        let config = PatternConfig::default();
        let pgen = PatternGenerator::new(config.clone(), vol);
        assert_eq!(pgen.volume_id(), vol);
        assert_eq!(pgen.config().seed, config.seed);
    }
}
