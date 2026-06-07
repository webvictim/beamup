use xxhash_rust::xxh3::xxh3_64;

pub fn hash_content(data: &[u8]) -> u64 {
    xxh3_64(data)
}

pub fn hash_file(path: &std::path::Path) -> anyhow::Result<u64> {
    let data = std::fs::read(path)?;
    Ok(hash_content(&data))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_content_same_hash() {
        let data = b"hello world";
        assert_eq!(hash_content(data), hash_content(data));
    }

    #[test]
    fn different_content_different_hash() {
        assert_ne!(hash_content(b"hello"), hash_content(b"world"));
    }

    #[test]
    fn empty_content_deterministic() {
        let h1 = hash_content(b"");
        let h2 = hash_content(b"");
        assert_eq!(h1, h2);
        assert_ne!(h1, 0); // xxh3 of empty is not zero
    }

    #[test]
    fn hash_file_matches_content() {
        let dir = std::env::temp_dir().join("beamup-test-hash");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.txt");
        std::fs::write(&path, b"test content").unwrap();

        let file_hash = hash_file(&path).unwrap();
        let content_hash = hash_content(b"test content");
        assert_eq!(file_hash, content_hash);

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
