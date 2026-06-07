use lz4_flex::{compress_prepend_size, decompress_size_prepended};

pub fn compress(data: &[u8]) -> Vec<u8> {
    compress_prepend_size(data)
}

pub fn decompress(data: &[u8]) -> anyhow::Result<Vec<u8>> {
    decompress_size_prepended(data).map_err(|e| anyhow::anyhow!("lz4 decompress failed: {e}"))
}

/// Chunk size for parallel transfer (8MB)
pub const CHUNK_SIZE: usize = 8 * 1024 * 1024;

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
