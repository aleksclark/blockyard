//! Reed-Solomon erasure coding (P4D.1, §4.5.3).
//!
//! Wraps `reed_solomon_erasure` to encode user data into K data + M parity
//! fragments and reconstruct the original data from any K available fragments.

use bytes::Bytes;
use reed_solomon_erasure::galois_8::ReedSolomon;
use thiserror::Error;
use tracing::debug;

/// Errors from erasure coding operations.
#[derive(Debug, Error)]
pub enum EcError {
    #[error("invalid parameters: K={data_count}, M={parity_count}")]
    InvalidParameters { data_count: usize, parity_count: usize },

    #[error("encoding failed: {0}")]
    EncodeFailed(String),

    #[error("decoding failed: {0}")]
    DecodeFailed(String),

    #[error("insufficient fragments: have {available}, need {required}")]
    InsufficientFragments { available: usize, required: usize },
}

/// An individual fragment produced by erasure encoding.
#[derive(Debug, Clone)]
pub struct Fragment {
    pub index: usize,
    pub is_data: bool,
    pub data: Bytes,
}

/// Reed-Solomon erasure codec for K data + M parity fragments.
///
/// Encoding splits input data into K equal-sized chunks (padding if needed),
/// then computes M parity chunks. Decoding reconstructs the original data
/// from any K of the K+M fragments.
#[derive(Debug)]
pub struct ErasureCodec {
    data_count: usize,
    parity_count: usize,
    rs: ReedSolomon,
}

impl ErasureCodec {
    /// Create a new codec with K data fragments and M parity fragments.
    pub fn new(data_count: usize, parity_count: usize) -> Result<Self, EcError> {
        if data_count == 0 || parity_count == 0 {
            return Err(EcError::InvalidParameters {
                data_count,
                parity_count,
            });
        }

        let rs = ReedSolomon::new(data_count, parity_count).map_err(|_| {
            EcError::InvalidParameters {
                data_count,
                parity_count,
            }
        })?;

        Ok(Self {
            data_count,
            parity_count,
            rs,
        })
    }

    /// Number of data fragments (K).
    pub fn data_count(&self) -> usize {
        self.data_count
    }

    /// Number of parity fragments (M).
    pub fn parity_count(&self) -> usize {
        self.parity_count
    }

    /// Total fragments (K + M).
    pub fn total_count(&self) -> usize {
        self.data_count + self.parity_count
    }

    /// Compute the per-fragment size for the given data length.
    /// Rounds up so that `fragment_size * K >= data_len`.
    pub fn fragment_size(&self, data_len: usize) -> usize {
        data_len.div_ceil(self.data_count)
    }

    /// Encode data into K data fragments + M parity fragments.
    ///
    /// The input data is padded to be evenly divisible by K.
    /// Returns K+M [`Fragment`]s where the first K are data and the
    /// remaining M are parity.
    pub fn encode(&self, data: &[u8]) -> Result<Vec<Fragment>, EcError> {
        if data.is_empty() {
            return Err(EcError::EncodeFailed("empty input data".into()));
        }

        let frag_size = self.fragment_size(data.len());

        let mut shards: Vec<Vec<u8>> = Vec::with_capacity(self.total_count());
        for i in 0..self.data_count {
            let start = i * frag_size;
            let end = std::cmp::min(start + frag_size, data.len());
            let mut shard = vec![0u8; frag_size];
            if start < data.len() {
                let copy_len = end - start;
                shard[..copy_len].copy_from_slice(&data[start..end]);
            }
            shards.push(shard);
        }

        for _ in 0..self.parity_count {
            shards.push(vec![0u8; frag_size]);
        }

        self.rs
            .encode(&mut shards)
            .map_err(|e| EcError::EncodeFailed(e.to_string()))?;

        let fragments: Vec<Fragment> = shards
            .into_iter()
            .enumerate()
            .map(|(i, shard)| Fragment {
                index: i,
                is_data: i < self.data_count,
                data: Bytes::from(shard),
            })
            .collect();

        debug!(
            data_len = data.len(),
            frag_size = frag_size,
            k = self.data_count,
            m = self.parity_count,
            "encoded data into fragments"
        );

        Ok(fragments)
    }

    /// Decode (reconstruct) original data from a set of available fragments.
    ///
    /// `fragments` is a Vec of `Option<Fragment>` of length K+M, where
    /// `None` indicates a missing/failed fragment. At least K fragments
    /// must be present.
    ///
    /// `original_len` is the original data length before padding.
    pub fn decode(
        &self,
        fragments: Vec<Option<Fragment>>,
        original_len: usize,
    ) -> Result<Bytes, EcError> {
        if fragments.len() != self.total_count() {
            return Err(EcError::DecodeFailed(format!(
                "expected {} fragments, got {}",
                self.total_count(),
                fragments.len()
            )));
        }

        let available = fragments.iter().filter(|f| f.is_some()).count();
        if available < self.data_count {
            return Err(EcError::InsufficientFragments {
                available,
                required: self.data_count,
            });
        }

        let frag_size = self.fragment_size(original_len);

        let mut shards: Vec<Option<Vec<u8>>> = fragments
            .into_iter()
            .map(|opt| opt.map(|f| f.data.to_vec()))
            .collect();

        self.rs
            .reconstruct(&mut shards)
            .map_err(|e| EcError::DecodeFailed(e.to_string()))?;

        let mut result = Vec::with_capacity(self.data_count * frag_size);
        for shard in shards.iter().take(self.data_count) {
            if let Some(data) = shard {
                result.extend_from_slice(data);
            } else {
                return Err(EcError::DecodeFailed(
                    "reconstruction did not restore all data shards".into(),
                ));
            }
        }

        result.truncate(original_len);

        debug!(
            original_len = original_len,
            recovered_len = result.len(),
            k = self.data_count,
            "decoded fragments to original data"
        );

        Ok(Bytes::from(result))
    }

    /// Verify that a set of fragments is consistent with the encoding.
    pub fn verify(&self, fragments: &[Fragment]) -> Result<bool, EcError> {
        if fragments.len() != self.total_count() {
            return Err(EcError::DecodeFailed(format!(
                "expected {} fragments for verify, got {}",
                self.total_count(),
                fragments.len()
            )));
        }

        let shards: Vec<Vec<u8>> = fragments.iter().map(|f| f.data.to_vec()).collect();

        self.rs
            .verify(&shards)
            .map_err(|e| EcError::DecodeFailed(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_valid() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        assert_eq!(codec.data_count(), 4);
        assert_eq!(codec.parity_count(), 2);
        assert_eq!(codec.total_count(), 6);
    }

    #[test]
    fn test_new_zero_data() {
        let err = ErasureCodec::new(0, 2).unwrap_err();
        assert!(matches!(err, EcError::InvalidParameters { .. }));
    }

    #[test]
    fn test_new_zero_parity() {
        let err = ErasureCodec::new(4, 0).unwrap_err();
        assert!(matches!(err, EcError::InvalidParameters { .. }));
    }

    #[test]
    fn test_new_both_zero() {
        let err = ErasureCodec::new(0, 0).unwrap_err();
        assert!(matches!(err, EcError::InvalidParameters { .. }));
    }

    #[test]
    fn test_fragment_size_exact() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        assert_eq!(codec.fragment_size(400), 100);
    }

    #[test]
    fn test_fragment_size_rounds_up() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        assert_eq!(codec.fragment_size(401), 101);
        assert_eq!(codec.fragment_size(403), 101);
    }

    #[test]
    fn test_fragment_size_small() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        assert_eq!(codec.fragment_size(1), 1);
        assert_eq!(codec.fragment_size(3), 1);
        assert_eq!(codec.fragment_size(4), 1);
        assert_eq!(codec.fragment_size(5), 2);
    }

    #[test]
    fn test_encode_basic() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let data = vec![0xAA; 400];
        let fragments = codec.encode(&data).unwrap();
        assert_eq!(fragments.len(), 6);
        assert!(fragments[0].is_data);
        assert!(fragments[3].is_data);
        assert!(!fragments[4].is_data);
        assert!(!fragments[5].is_data);
        assert_eq!(fragments[0].data.len(), 100);
    }

    #[test]
    fn test_encode_empty_fails() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let err = codec.encode(&[]).unwrap_err();
        assert!(matches!(err, EcError::EncodeFailed(_)));
    }

    #[test]
    fn test_encode_non_divisible() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let data = vec![0xBB; 401];
        let fragments = codec.encode(&data).unwrap();
        assert_eq!(fragments.len(), 6);
        assert_eq!(fragments[0].data.len(), 101);
    }

    #[test]
    fn test_encode_decode_roundtrip_exact() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let data: Vec<u8> = (0..400).map(|i| (i % 256) as u8).collect();
        let fragments = codec.encode(&data).unwrap();

        let opt_frags: Vec<Option<Fragment>> = fragments.into_iter().map(Some).collect();
        let decoded = codec.decode(opt_frags, data.len()).unwrap();
        assert_eq!(decoded.as_ref(), &data[..]);
    }

    #[test]
    fn test_encode_decode_roundtrip_padded() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let data: Vec<u8> = (0..401).map(|i| (i % 256) as u8).collect();
        let fragments = codec.encode(&data).unwrap();

        let opt_frags: Vec<Option<Fragment>> = fragments.into_iter().map(Some).collect();
        let decoded = codec.decode(opt_frags, data.len()).unwrap();
        assert_eq!(decoded.as_ref(), &data[..]);
    }

    #[test]
    fn test_decode_with_one_missing_data_fragment() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let data: Vec<u8> = (0..400).map(|i| (i % 256) as u8).collect();
        let fragments = codec.encode(&data).unwrap();

        let mut opt_frags: Vec<Option<Fragment>> = fragments.into_iter().map(Some).collect();
        opt_frags[0] = None;

        let decoded = codec.decode(opt_frags, data.len()).unwrap();
        assert_eq!(decoded.as_ref(), &data[..]);
    }

    #[test]
    fn test_decode_with_two_missing_data_fragments() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let data: Vec<u8> = (0..400).map(|i| (i % 256) as u8).collect();
        let fragments = codec.encode(&data).unwrap();

        let mut opt_frags: Vec<Option<Fragment>> = fragments.into_iter().map(Some).collect();
        opt_frags[0] = None;
        opt_frags[2] = None;

        let decoded = codec.decode(opt_frags, data.len()).unwrap();
        assert_eq!(decoded.as_ref(), &data[..]);
    }

    #[test]
    fn test_decode_with_missing_parity_fragments() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let data: Vec<u8> = (0..400).map(|i| (i % 256) as u8).collect();
        let fragments = codec.encode(&data).unwrap();

        let mut opt_frags: Vec<Option<Fragment>> = fragments.into_iter().map(Some).collect();
        opt_frags[4] = None;
        opt_frags[5] = None;

        let decoded = codec.decode(opt_frags, data.len()).unwrap();
        assert_eq!(decoded.as_ref(), &data[..]);
    }

    #[test]
    fn test_decode_with_mixed_missing() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let data: Vec<u8> = (0..400).map(|i| (i % 256) as u8).collect();
        let fragments = codec.encode(&data).unwrap();

        let mut opt_frags: Vec<Option<Fragment>> = fragments.into_iter().map(Some).collect();
        opt_frags[1] = None;
        opt_frags[4] = None;

        let decoded = codec.decode(opt_frags, data.len()).unwrap();
        assert_eq!(decoded.as_ref(), &data[..]);
    }

    #[test]
    fn test_decode_insufficient_fragments() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let data: Vec<u8> = (0..400).map(|i| (i % 256) as u8).collect();
        let fragments = codec.encode(&data).unwrap();

        let mut opt_frags: Vec<Option<Fragment>> = fragments.into_iter().map(Some).collect();
        opt_frags[0] = None;
        opt_frags[1] = None;
        opt_frags[2] = None;

        let err = codec.decode(opt_frags, data.len()).unwrap_err();
        assert!(matches!(err, EcError::InsufficientFragments { available: 3, required: 4 }));
    }

    #[test]
    fn test_decode_wrong_fragment_count() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let frags: Vec<Option<Fragment>> = vec![None; 3];
        let err = codec.decode(frags, 100).unwrap_err();
        assert!(matches!(err, EcError::DecodeFailed(_)));
    }

    #[test]
    fn test_verify_valid() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let data: Vec<u8> = (0..400).map(|i| (i % 256) as u8).collect();
        let fragments = codec.encode(&data).unwrap();
        assert!(codec.verify(&fragments).unwrap());
    }

    #[test]
    fn test_verify_corrupt() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let data: Vec<u8> = (0..400).map(|i| (i % 256) as u8).collect();
        let mut fragments = codec.encode(&data).unwrap();
        fragments[0] = Fragment {
            index: 0,
            is_data: true,
            data: Bytes::from(vec![0xFF; 100]),
        };
        assert!(!codec.verify(&fragments).unwrap());
    }

    #[test]
    fn test_verify_wrong_count() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let err = codec.verify(&[]).unwrap_err();
        assert!(matches!(err, EcError::DecodeFailed(_)));
    }

    #[test]
    fn test_small_data() {
        let codec = ErasureCodec::new(2, 1).unwrap();
        let data = vec![42u8; 10];
        let fragments = codec.encode(&data).unwrap();
        assert_eq!(fragments.len(), 3);

        let opt_frags: Vec<Option<Fragment>> = fragments.into_iter().map(Some).collect();
        let decoded = codec.decode(opt_frags, data.len()).unwrap();
        assert_eq!(decoded.as_ref(), &data[..]);
    }

    #[test]
    fn test_single_byte_data() {
        let codec = ErasureCodec::new(2, 1).unwrap();
        let data = vec![99u8; 1];
        let fragments = codec.encode(&data).unwrap();

        let opt_frags: Vec<Option<Fragment>> = fragments.into_iter().map(Some).collect();
        let decoded = codec.decode(opt_frags, data.len()).unwrap();
        assert_eq!(decoded.as_ref(), &data[..]);
    }

    #[test]
    fn test_large_data() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let data: Vec<u8> = (0..10_000).map(|i| (i % 256) as u8).collect();
        let fragments = codec.encode(&data).unwrap();

        let mut opt_frags: Vec<Option<Fragment>> = fragments.into_iter().map(Some).collect();
        opt_frags[0] = None;
        opt_frags[5] = None;

        let decoded = codec.decode(opt_frags, data.len()).unwrap();
        assert_eq!(decoded.as_ref(), &data[..]);
    }

    #[test]
    fn test_fragment_index_and_type() {
        let codec = ErasureCodec::new(3, 2).unwrap();
        let data = vec![0xAA; 300];
        let fragments = codec.encode(&data).unwrap();
        for (i, frag) in fragments.iter().enumerate() {
            assert_eq!(frag.index, i);
            assert_eq!(frag.is_data, i < 3);
        }
    }

    #[test]
    fn test_reconstruct_from_only_parity_and_some_data() {
        let codec = ErasureCodec::new(3, 3).unwrap();
        let data: Vec<u8> = (0..300).map(|i| (i % 256) as u8).collect();
        let fragments = codec.encode(&data).unwrap();

        let mut opt_frags: Vec<Option<Fragment>> = fragments.into_iter().map(Some).collect();
        opt_frags[0] = None;
        opt_frags[1] = None;
        opt_frags[2] = None;

        let decoded = codec.decode(opt_frags, data.len()).unwrap();
        assert_eq!(decoded.as_ref(), &data[..]);
    }

    #[test]
    fn test_ec_error_display() {
        let e = EcError::InvalidParameters {
            data_count: 0,
            parity_count: 2,
        };
        assert!(e.to_string().contains("K=0"));

        let e = EcError::InsufficientFragments {
            available: 2,
            required: 4,
        };
        assert!(e.to_string().contains("have 2"));
        assert!(e.to_string().contains("need 4"));

        let e = EcError::EncodeFailed("test".into());
        assert!(e.to_string().contains("test"));

        let e = EcError::DecodeFailed("test".into());
        assert!(e.to_string().contains("test"));
    }

    #[test]
    fn test_ec_error_debug() {
        let e = EcError::InvalidParameters {
            data_count: 4,
            parity_count: 2,
        };
        let debug = format!("{:?}", e);
        assert!(debug.contains("InvalidParameters"));
    }

    #[test]
    fn test_fragment_clone() {
        let frag = Fragment {
            index: 0,
            is_data: true,
            data: Bytes::from(vec![1, 2, 3]),
        };
        let cloned = frag.clone();
        assert_eq!(cloned.index, 0);
        assert_eq!(cloned.is_data, true);
        assert_eq!(cloned.data, frag.data);
    }

    #[test]
    fn test_fragment_debug() {
        let frag = Fragment {
            index: 3,
            is_data: false,
            data: Bytes::from(vec![0xFF; 10]),
        };
        let debug = format!("{:?}", frag);
        assert!(debug.contains("Fragment"));
        assert!(debug.contains("3"));
    }

    #[test]
    fn test_erasure_codec_debug() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let debug = format!("{:?}", codec);
        assert!(debug.contains("ErasureCodec"));
    }

    #[test]
    fn test_k1_m1() {
        let codec = ErasureCodec::new(1, 1).unwrap();
        let data = vec![42u8; 100];
        let fragments = codec.encode(&data).unwrap();
        assert_eq!(fragments.len(), 2);

        let mut opt_frags: Vec<Option<Fragment>> = fragments.into_iter().map(Some).collect();
        opt_frags[0] = None;

        let decoded = codec.decode(opt_frags, data.len()).unwrap();
        assert_eq!(decoded.as_ref(), &data[..]);
    }

    #[test]
    fn test_all_fragments_missing_fails() {
        let codec = ErasureCodec::new(2, 1).unwrap();
        let frags: Vec<Option<Fragment>> = vec![None; 3];
        let err = codec.decode(frags, 100).unwrap_err();
        assert!(matches!(err, EcError::InsufficientFragments { .. }));
    }

    #[test]
    fn test_encode_preserves_data_content() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let data: Vec<u8> = (0..400).map(|i| (i % 256) as u8).collect();
        let fragments = codec.encode(&data).unwrap();

        for i in 0..4 {
            let start = i * 100;
            let end = start + 100;
            assert_eq!(fragments[i].data.as_ref(), &data[start..end]);
        }
    }
}
