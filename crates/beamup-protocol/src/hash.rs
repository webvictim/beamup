use xxhash_rust::xxh3::xxh3_64;

pub fn hash_content(data: &[u8]) -> u64 {
    xxh3_64(data)
}

pub fn hash_file(path: &std::path::Path) -> anyhow::Result<u64> {
    let data = std::fs::read(path)?;
    Ok(hash_content(&data))
}
