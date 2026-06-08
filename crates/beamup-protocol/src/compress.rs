use lz4_flex::{compress_prepend_size, decompress_size_prepended};

pub fn compress(data: &[u8]) -> Vec<u8> {
    compress_prepend_size(data)
}

pub fn decompress(data: &[u8]) -> anyhow::Result<Vec<u8>> {
    decompress_size_prepended(data).map_err(|e| anyhow::anyhow!("lz4 decompress failed: {e}"))
}

/// Default chunk size for parallel transfer (64MB)
pub const CHUNK_SIZE: usize = 64 * 1024 * 1024;

/// Minimum file size to trigger chunked transfer (anything above inline threshold
/// that's also larger than one chunk gets split)
pub const CHUNKED_THRESHOLD: u64 = CHUNK_SIZE as u64;

/// Split data into chunks of CHUNK_SIZE
pub fn split_chunks(data: &[u8]) -> Vec<Vec<u8>> {
    data.chunks(CHUNK_SIZE).map(|c| c.to_vec()).collect()
}

/// Reassemble chunks into a single buffer
pub fn reassemble_chunks(mut chunks: Vec<Vec<u8>>) -> Vec<u8> {
    let total: usize = chunks.iter().map(|c| c.len()).sum();
    let mut result = Vec::with_capacity(total);
    for chunk in chunks.drain(..) {
        result.extend_from_slice(&chunk);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_small() {
        let data = b"hello world, this is a test of lz4 compression";
        let compressed = compress(data);
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn round_trip_empty() {
        let data = b"";
        let compressed = compress(data);
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn round_trip_large_repetitive() {
        // 10MB of repetitive data (compresses well)
        let data: Vec<u8> = (0..10 * 1024 * 1024).map(|i| (i % 256) as u8).collect();
        let compressed = compress(&data);
        assert!(compressed.len() < data.len()); // should actually compress
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn per_chunk_round_trip() {
        // Simulate the streaming compression pattern:
        // split into chunks, compress each independently, decompress each, concatenate
        let data: Vec<u8> = (0..20 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        let chunks = split_chunks(&data);

        let compressed_chunks: Vec<Vec<u8>> = chunks.iter().map(|c| compress(c)).collect();

        let mut reconstructed = Vec::new();
        for cc in &compressed_chunks {
            let decompressed = decompress(cc).unwrap();
            reconstructed.extend_from_slice(&decompressed);
        }

        assert_eq!(data, reconstructed);
    }

    #[test]
    fn split_chunks_correct_count() {
        // Exactly 2 chunks
        let data = vec![0u8; CHUNK_SIZE * 2];
        assert_eq!(split_chunks(&data).len(), 2);

        // 2 chunks + 1 byte remainder
        let data = vec![0u8; CHUNK_SIZE * 2 + 1];
        assert_eq!(split_chunks(&data).len(), 3);

        // Less than 1 chunk
        let data = vec![0u8; 100];
        assert_eq!(split_chunks(&data).len(), 1);
    }

    #[test]
    fn split_reassemble_identity() {
        let data: Vec<u8> = (0..CHUNK_SIZE * 3 + 12345).map(|i| (i % 256) as u8).collect();
        let chunks = split_chunks(&data);
        let reassembled = reassemble_chunks(chunks);
        assert_eq!(data, reassembled);
    }

    #[test]
    fn decompress_invalid_data_returns_error() {
        let garbage = vec![0xFF, 0xFE, 0xFD, 0xFC, 0x00, 0x01];
        assert!(decompress(&garbage).is_err());
    }
}
