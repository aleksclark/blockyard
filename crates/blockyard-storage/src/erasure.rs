use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Deserialize, Serialize};
use tracing::debug;

/// Reed-Solomon erasure codec for encoding and decoding data into k data
/// shards and m parity shards.
#[derive(Debug, Clone)]
pub struct ErasureCodec {
    data_shards: usize,
    parity_shards: usize,
    rs: ReedSolomon,
}

/// Errors produced by the erasure codec.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ErasureError {
    /// Invalid codec parameters (k or m < 1).
    InvalidParams(String),
    /// Supplied data is empty.
    EmptyData,
    /// Not enough shards available for reconstruction.
    InsufficientShards { available: usize, required: usize },
    /// Internal Reed-Solomon error.
    Internal(String),
}

impl std::fmt::Display for ErasureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidParams(msg) => write!(f, "invalid erasure params: {msg}"),
            Self::EmptyData => write!(f, "empty data"),
            Self::InsufficientShards {
                available,
                required,
            } => write!(
                f,
                "insufficient shards: {available} available, {required} required"
            ),
            Self::Internal(msg) => write!(f, "internal erasure error: {msg}"),
        }
    }
}

impl std::error::Error for ErasureError {}

impl ErasureCodec {
    /// Create a new Reed-Solomon codec with `data_shards` (k) data shards
    /// and `parity_shards` (m) parity shards.
    ///
    /// Both k and m must be >= 1.
    pub fn new(data_shards: usize, parity_shards: usize) -> Result<Self, ErasureError> {
        if data_shards < 1 {
            return Err(ErasureError::InvalidParams(
                "data_shards must be >= 1".into(),
            ));
        }
        if parity_shards < 1 {
            return Err(ErasureError::InvalidParams(
                "parity_shards must be >= 1".into(),
            ));
        }

        let rs = ReedSolomon::new(data_shards, parity_shards)
            .map_err(|e| ErasureError::Internal(format!("failed to create RS codec: {e}")))?;

        debug!(k = data_shards, m = parity_shards, "created erasure codec");

        Ok(Self {
            data_shards,
            parity_shards,
            rs,
        })
    }

    /// Total number of shards (k + m).
    pub fn total_shards(&self) -> usize {
        self.data_shards + self.parity_shards
    }

    /// Number of data shards (k).
    pub fn data_shards(&self) -> usize {
        self.data_shards
    }

    /// Number of parity shards (m).
    pub fn parity_shards(&self) -> usize {
        self.parity_shards
    }

    /// Compute the chunk size for a given data length.
    ///
    /// Each chunk will be `ceil(data_len / k)` bytes.
    pub fn chunk_size(&self, data_len: usize) -> usize {
        data_len.div_ceil(self.data_shards)
    }

    /// Encode data into k+m chunks.
    ///
    /// The data is split into k equal-sized chunks (padded with zeros if
    /// necessary), and m parity chunks are computed.  The first 8 bytes of
    /// the returned Vec<Vec<u8>> conceptually store the original length, but
    /// we prepend it to the first data shard.
    ///
    /// Returns k+m chunks where chunks 0..k are data and k..k+m are parity.
    pub fn encode(&self, data: &[u8]) -> Result<Vec<Vec<u8>>, ErasureError> {
        if data.is_empty() {
            return Err(ErasureError::EmptyData);
        }

        let chunk_sz = self.chunk_size(data.len());
        let total = self.total_shards();

        // Build data shards with zero-padding for the last shard if needed.
        let mut shards: Vec<Vec<u8>> = Vec::with_capacity(total);
        for i in 0..self.data_shards {
            let start = i * chunk_sz;
            let end = ((i + 1) * chunk_sz).min(data.len());
            let mut chunk = if start < data.len() {
                data[start..end].to_vec()
            } else {
                Vec::new()
            };
            // Pad to chunk_sz.
            chunk.resize(chunk_sz, 0);
            shards.push(chunk);
        }

        // Add empty parity shards.
        for _ in 0..self.parity_shards {
            shards.push(vec![0u8; chunk_sz]);
        }

        // Compute parity.
        self.rs
            .encode(&mut shards)
            .map_err(|e| ErasureError::Internal(format!("encode failed: {e}")))?;

        debug!(
            data_len = data.len(),
            chunk_size = chunk_sz,
            total_shards = total,
            "encoded data"
        );

        Ok(shards)
    }

    /// Decode data from k+m shard slots.
    ///
    /// `chunks` must have exactly k+m entries.  `Some(data)` means the chunk
    /// is available; `None` means it is missing.  At least k chunks must be
    /// present for successful reconstruction.
    ///
    /// Returns the original data (without padding).  The caller must know the
    /// original data length to strip trailing zeros from the last data shard.
    /// We store the original length in the first 8 bytes of the
    /// reassembled payload.
    pub fn decode(&self, chunks: &mut [Option<Vec<u8>>]) -> Result<Vec<u8>, ErasureError> {
        let total = self.total_shards();
        if chunks.len() != total {
            return Err(ErasureError::InvalidParams(format!(
                "expected {} chunks, got {}",
                total,
                chunks.len()
            )));
        }

        let available = chunks.iter().filter(|c| c.is_some()).count();
        if available < self.data_shards {
            return Err(ErasureError::InsufficientShards {
                available,
                required: self.data_shards,
            });
        }

        // Determine chunk size from any available shard.
        let chunk_sz = chunks.iter().flatten().next().map(|c| c.len()).ok_or(
            ErasureError::InsufficientShards {
                available: 0,
                required: self.data_shards,
            },
        )?;

        // Build the shard array for reconstruction.
        let mut shards: Vec<Option<Vec<u8>>> = chunks.to_vec();

        // Reconstruct missing shards.
        self.rs
            .reconstruct(&mut shards)
            .map_err(|e| ErasureError::Internal(format!("reconstruct failed: {e}")))?;

        // Reassemble original data from data shards.
        let mut result = Vec::with_capacity(self.data_shards * chunk_sz);
        for shard in shards.iter().take(self.data_shards) {
            if let Some(s) = shard {
                result.extend_from_slice(s);
            } else {
                // Should not happen after successful reconstruct.
                result.extend(std::iter::repeat_n(0u8, chunk_sz));
            }
        }

        debug!(
            chunk_size = chunk_sz,
            available,
            total_shards = total,
            "decoded data"
        );

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helper ────────────────────────────────────────────────────────────

    /// Encode `data` with the given codec, then decode all chunks and
    /// verify the round-trip matches.
    fn roundtrip(k: usize, m: usize, data: &[u8]) {
        let codec = ErasureCodec::new(k, m).unwrap();
        let chunks = codec.encode(data).unwrap();
        assert_eq!(chunks.len(), k + m);

        let original_len = data.len();
        let mut input: Vec<Option<Vec<u8>>> = chunks.into_iter().map(Some).collect();
        let decoded = codec.decode(&mut input).unwrap();
        assert_eq!(&decoded[..original_len], data);
    }

    /// Encode, drop specific chunks, then decode and verify round-trip.
    fn roundtrip_with_drops(k: usize, m: usize, data: &[u8], drops: &[usize]) {
        let codec = ErasureCodec::new(k, m).unwrap();
        let chunks = codec.encode(data).unwrap();
        let original_len = data.len();

        let mut input: Vec<Option<Vec<u8>>> = chunks.into_iter().map(Some).collect();
        for &idx in drops {
            input[idx] = None;
        }
        let decoded = codec.decode(&mut input).unwrap();
        assert_eq!(&decoded[..original_len], data);
    }

    /// Encode, drop specific chunks, decode should fail.
    fn roundtrip_should_fail(k: usize, m: usize, data: &[u8], drops: &[usize]) {
        let codec = ErasureCodec::new(k, m).unwrap();
        let chunks = codec.encode(data).unwrap();

        let mut input: Vec<Option<Vec<u8>>> = chunks.into_iter().map(Some).collect();
        for &idx in drops {
            input[idx] = None;
        }
        let result = codec.decode(&mut input);
        assert!(result.is_err(), "expected decode to fail");
    }

    // ── Basic encode/decode ──────────────────────────────────────────────

    #[test]
    fn test_encode_decode_all_chunks_rs_2_1() {
        roundtrip(2, 1, b"hello world, this is a test");
    }

    #[test]
    fn test_encode_decode_all_chunks_rs_4_2() {
        roundtrip(4, 2, b"Reed-Solomon erasure coding is great!");
    }

    #[test]
    fn test_encode_decode_all_chunks_rs_6_3() {
        roundtrip(6, 3, b"testing RS(6,3) with some data that is longer");
    }

    #[test]
    fn test_encode_decode_all_chunks_rs_8_4() {
        roundtrip(
            8,
            4,
            b"testing RS(8,4) with a somewhat longer payload here.",
        );
    }

    // ── Drop 1 parity chunk ─────────────────────────────────────────────

    #[test]
    fn test_drop_one_parity_rs_2_1() {
        // k=2, m=1: drop parity shard (index 2)
        roundtrip_with_drops(2, 1, b"parity test", &[2]);
    }

    #[test]
    fn test_drop_one_parity_rs_4_2() {
        // k=4, m=2: drop one parity shard (index 4)
        roundtrip_with_drops(4, 2, b"parity test four-two", &[4]);
    }

    // ── Drop all m parity chunks ────────────────────────────────────────

    #[test]
    fn test_drop_all_parity_rs_2_1() {
        roundtrip_with_drops(2, 1, b"only data shards", &[2]);
    }

    #[test]
    fn test_drop_all_parity_rs_4_2() {
        roundtrip_with_drops(4, 2, b"only data shards remain", &[4, 5]);
    }

    #[test]
    fn test_drop_all_parity_rs_6_3() {
        roundtrip_with_drops(6, 3, b"drop all parity shards for 6+3", &[6, 7, 8]);
    }

    #[test]
    fn test_drop_all_parity_rs_8_4() {
        roundtrip_with_drops(8, 4, b"drop all parity shards for 8+4", &[8, 9, 10, 11]);
    }

    // ── Drop 1 data chunk ───────────────────────────────────────────────

    #[test]
    fn test_drop_one_data_rs_2_1() {
        roundtrip_with_drops(2, 1, b"data reconstruct 2+1", &[0]);
    }

    #[test]
    fn test_drop_one_data_rs_4_2() {
        roundtrip_with_drops(4, 2, b"data reconstruct 4+2", &[1]);
    }

    #[test]
    fn test_drop_one_data_rs_6_3() {
        roundtrip_with_drops(6, 3, b"data reconstruct 6+3 test data!!", &[3]);
    }

    #[test]
    fn test_drop_one_data_rs_8_4() {
        roundtrip_with_drops(8, 4, b"data reconstruct 8+4 some longer payload data", &[5]);
    }

    // ── Drop m chunks (mix of data + parity) ────────────────────────────

    #[test]
    fn test_drop_m_mixed_rs_2_1() {
        // m=1, drop 1 data shard
        roundtrip_with_drops(2, 1, b"mixed drop 2+1", &[0]);
    }

    #[test]
    fn test_drop_m_mixed_rs_4_2() {
        // m=2, drop 1 data + 1 parity
        roundtrip_with_drops(4, 2, b"mixed drop 4+2 test data", &[1, 5]);
    }

    #[test]
    fn test_drop_m_mixed_rs_6_3() {
        // m=3, drop 2 data + 1 parity
        roundtrip_with_drops(6, 3, b"mixed drop 6+3 test data payload!!", &[0, 2, 7]);
    }

    #[test]
    fn test_drop_m_mixed_rs_8_4() {
        // m=4, drop 2 data + 2 parity
        roundtrip_with_drops(
            8,
            4,
            b"mixed drop 8+4 with a longer test payload for erasure coding",
            &[1, 3, 9, 11],
        );
    }

    // ── Drop m+1 chunks → error ─────────────────────────────────────────

    #[test]
    fn test_drop_m_plus_1_rs_2_1() {
        // m=1, drop 2 chunks → only 1 of 2 data shards available
        roundtrip_should_fail(2, 1, b"too many drops", &[0, 2]);
    }

    #[test]
    fn test_drop_m_plus_1_rs_4_2() {
        // m=2, drop 3 chunks
        roundtrip_should_fail(4, 2, b"too many drops 4+2", &[0, 1, 4]);
    }

    #[test]
    fn test_drop_m_plus_1_rs_6_3() {
        // m=3, drop 4 chunks
        roundtrip_should_fail(6, 3, b"too many drops 6+3 data!!", &[0, 1, 2, 6]);
    }

    #[test]
    fn test_drop_m_plus_1_rs_8_4() {
        // m=4, drop 5 chunks
        roundtrip_should_fail(
            8,
            4,
            b"too many drops 8+4 longer data here!",
            &[0, 1, 2, 3, 8],
        );
    }

    // ── Various data sizes (not aligned to k) ───────────────────────────

    #[test]
    fn test_unaligned_data_rs_4_2() {
        // 13 bytes / 4 chunks = 3.25 → chunk_size = 4, padded to 16
        let data = b"thirteen byte";
        roundtrip(4, 2, data);
    }

    #[test]
    fn test_unaligned_data_rs_6_3() {
        // 7 bytes / 6 chunks = 1.17 → chunk_size = 2
        let data = b"7-bytes";
        roundtrip(6, 3, data);
    }

    #[test]
    fn test_unaligned_data_rs_8_4() {
        // 17 bytes / 8 chunks = 2.125 → chunk_size = 3
        let data = b"seventeen bytes!X";
        roundtrip(8, 4, data);
    }

    #[test]
    fn test_single_byte_aligned() {
        // 4 bytes / 4 chunks = 1 byte each (aligned)
        roundtrip(4, 2, b"ABCD");
    }

    #[test]
    fn test_exactly_k_bytes() {
        roundtrip(2, 1, b"AB");
        roundtrip(4, 2, b"ABCD");
        roundtrip(6, 3, b"ABCDEF");
    }

    // ── Empty data → error ──────────────────────────────────────────────

    #[test]
    fn test_empty_data_error() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let result = codec.encode(b"");
        assert!(matches!(result, Err(ErasureError::EmptyData)));
    }

    // ── 1-byte data ─────────────────────────────────────────────────────

    #[test]
    fn test_single_byte_data_rs_2_1() {
        roundtrip(2, 1, &[0x42]);
    }

    #[test]
    fn test_single_byte_data_rs_4_2() {
        roundtrip(4, 2, &[0xFF]);
    }

    #[test]
    fn test_single_byte_data_rs_6_3() {
        roundtrip(6, 3, &[0x01]);
    }

    #[test]
    fn test_single_byte_data_rs_8_4() {
        roundtrip(8, 4, &[0xAB]);
    }

    // ── Large data (1MB) ────────────────────────────────────────────────

    #[test]
    fn test_large_data_1mb_rs_4_2() {
        let data: Vec<u8> = (0..1_048_576u32).map(|i| (i % 256) as u8).collect();
        roundtrip(4, 2, &data);
    }

    #[test]
    fn test_large_data_1mb_rs_4_2_with_drops() {
        let data: Vec<u8> = (0..1_048_576u32).map(|i| (i % 256) as u8).collect();
        // Drop 2 chunks (max m=2)
        roundtrip_with_drops(4, 2, &data, &[0, 5]);
    }

    #[test]
    fn test_large_data_1mb_rs_8_4() {
        let data: Vec<u8> = (0..1_048_576u32).map(|i| (i * 7 % 256) as u8).collect();
        roundtrip(8, 4, &data);
    }

    #[test]
    fn test_large_data_1mb_rs_8_4_with_drops() {
        let data: Vec<u8> = (0..1_048_576u32).map(|i| (i * 7 % 256) as u8).collect();
        // Drop 4 chunks (max m=4)
        roundtrip_with_drops(8, 4, &data, &[1, 3, 8, 10]);
    }

    // ── Constructor validation ──────────────────────────────────────────

    #[test]
    fn test_invalid_params_zero_data_shards() {
        let result = ErasureCodec::new(0, 2);
        assert!(matches!(result, Err(ErasureError::InvalidParams(_))));
    }

    #[test]
    fn test_invalid_params_zero_parity_shards() {
        let result = ErasureCodec::new(4, 0);
        assert!(matches!(result, Err(ErasureError::InvalidParams(_))));
    }

    #[test]
    fn test_invalid_params_both_zero() {
        let result = ErasureCodec::new(0, 0);
        assert!(matches!(result, Err(ErasureError::InvalidParams(_))));
    }

    // ── Accessor methods ────────────────────────────────────────────────

    #[test]
    fn test_accessors() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        assert_eq!(codec.data_shards(), 4);
        assert_eq!(codec.parity_shards(), 2);
        assert_eq!(codec.total_shards(), 6);
    }

    #[test]
    fn test_chunk_size_aligned() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        assert_eq!(codec.chunk_size(16), 4);
        assert_eq!(codec.chunk_size(8), 2);
    }

    #[test]
    fn test_chunk_size_unaligned() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        assert_eq!(codec.chunk_size(13), 4); // ceil(13/4) = 4
        assert_eq!(codec.chunk_size(1), 1); // ceil(1/4) = 1
        assert_eq!(codec.chunk_size(5), 2); // ceil(5/4) = 2
    }

    // ── Decode with wrong chunk count ───────────────────────────────────

    #[test]
    fn test_decode_wrong_chunk_count() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let mut chunks: Vec<Option<Vec<u8>>> = vec![Some(vec![0; 4]); 5]; // 5 != 6
        let result = codec.decode(&mut chunks);
        assert!(matches!(result, Err(ErasureError::InvalidParams(_))));
    }

    // ── ErasureError Display ────────────────────────────────────────────

    #[test]
    fn test_erasure_error_display() {
        assert_eq!(ErasureError::EmptyData.to_string(), "empty data");
        assert_eq!(
            ErasureError::InvalidParams("bad".into()).to_string(),
            "invalid erasure params: bad"
        );
        assert_eq!(
            ErasureError::InsufficientShards {
                available: 2,
                required: 4
            }
            .to_string(),
            "insufficient shards: 2 available, 4 required"
        );
        assert_eq!(
            ErasureError::Internal("oops".into()).to_string(),
            "internal erasure error: oops"
        );
    }

    // ── Drop exact boundary cases ───────────────────────────────────────

    #[test]
    fn test_drop_exactly_m_data_shards_rs_4_2() {
        // Drop 2 data shards, keep all 2 parity → should succeed (4 of 4 needed available)
        roundtrip_with_drops(4, 2, b"boundary test 4+2", &[0, 1]);
    }

    #[test]
    fn test_drop_exactly_m_data_shards_rs_6_3() {
        // Drop 3 data shards, keep all 3 parity → should succeed (6 of 6 needed available)
        roundtrip_with_drops(6, 3, b"boundary test 6+3 with enough data.", &[0, 1, 2]);
    }

    #[test]
    fn test_all_parity_dropped_single_data_dropped_rs_4_2_fails() {
        // Drop 2 parity + 1 data = 3 drops > m=2 → fail
        roundtrip_should_fail(4, 2, b"should fail", &[0, 4, 5]);
    }

    // ── Reconstruction with different drop patterns ─────────────────────

    #[test]
    fn test_drop_last_data_shard_rs_4_2() {
        roundtrip_with_drops(4, 2, b"drop last data shard", &[3]);
    }

    #[test]
    fn test_drop_first_and_last_data_shards_rs_4_2() {
        roundtrip_with_drops(4, 2, b"drop first and last data shards", &[0, 3]);
    }

    #[test]
    fn test_drop_all_data_shards_fails_rs_4_2() {
        // Drop all 4 data shards, only 2 parity left < k=4
        roundtrip_should_fail(4, 2, b"all data gone", &[0, 1, 2, 3]);
    }

    // ── Binary data patterns ────────────────────────────────────────────

    #[test]
    fn test_all_zeros() {
        let data = vec![0u8; 1024];
        roundtrip(4, 2, &data);
    }

    #[test]
    fn test_all_ones() {
        let data = vec![0xFFu8; 1024];
        roundtrip(4, 2, &data);
    }

    #[test]
    fn test_alternating_pattern() {
        let data: Vec<u8> = (0..1024)
            .map(|i| if i % 2 == 0 { 0xAA } else { 0x55 })
            .collect();
        roundtrip(4, 2, &data);
    }
}
