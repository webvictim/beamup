use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u32 = 2;

/// Threshold below which files are sent inline via the pipe.
/// Above this, scp is used for transfer.
pub const INLINE_THRESHOLD: u64 = 64 * 1024; // 64 KB

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Message {
    Hello {
        version: u32,
        session_id: String,
    },
    HelloAck {
        version: u32,
    },

    FileManifest {
        entries: Vec<ManifestEntry>,
    },
    /// Agent's response to FileManifest — tells CLI what to push/pull via scp
    SyncPlan {
        to_push: Vec<SyncEntry>,
        to_pull: Vec<SyncEntry>,
    },
    ManifestAck,

    /// A file changed (metadata only — used to trigger scp or inline transfer)
    FileChanged {
        path: String,
        hash: u64,
        mtime: u64,
        size: u64,
    },
    FileDeleted {
        path: String,
    },
    DirCreated {
        path: String,
    },
    DirDeleted {
        path: String,
    },

    /// Small file sent inline (< INLINE_THRESHOLD)
    FileContent {
        path: String,
        hash: u64,
        data: Vec<u8>,
    },

    /// CLI finished scp'ing a file to the beam — agent should update state
    FileReady {
        path: String,
        hash: u64,
        size: u64,
    },

    /// CLI finished pulling a file from the beam — agent can update bookkeeping
    FileReceived {
        path: String,
        hash: u64,
    },

    ConflictDetected {
        path: String,
        local_hash: u64,
        remote_hash: u64,
    },

    Ping,
    Pong,

    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub path: String,
    pub hash: u64,
    pub mtime: u64,
    pub size: u64,
    pub is_dir: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncEntry {
    pub path: String,
    pub hash: u64,
    pub size: u64,
}

impl Message {
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        Ok(rmp_serde::to_vec(self)?)
    }

    pub fn decode(data: &[u8]) -> anyhow::Result<Self> {
        Ok(rmp_serde::from_slice(data)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(msg: &Message) -> Message {
        let encoded = msg.encode().unwrap();
        Message::decode(&encoded).unwrap()
    }

    #[test]
    fn all_variants_serialize() {
        let messages = vec![
            Message::Hello { version: 2, session_id: "test".into() },
            Message::HelloAck { version: 2 },
            Message::FileManifest { entries: vec![
                ManifestEntry { path: "a/b.rs".into(), hash: 42, mtime: 1000, size: 500, is_dir: false },
            ]},
            Message::SyncPlan {
                to_push: vec![SyncEntry { path: "x".into(), hash: 1, size: 10 }],
                to_pull: vec![],
            },
            Message::ManifestAck,
            Message::FileChanged { path: "foo.rs".into(), hash: 123, mtime: 456, size: 789 },
            Message::FileDeleted { path: "old.txt".into() },
            Message::DirCreated { path: "new_dir".into() },
            Message::DirDeleted { path: "old_dir".into() },
            Message::FileContent { path: "small.txt".into(), hash: 11, data: vec![1, 2, 3] },
            Message::FileReady { path: "big.bin".into(), hash: 22, size: 1000000 },
            Message::FileReceived { path: "big.bin".into(), hash: 22 },
            Message::ConflictDetected { path: "conflict.txt".into(), local_hash: 1, remote_hash: 2 },
            Message::Ping,
            Message::Pong,
            Message::Shutdown,
        ];

        for msg in &messages {
            let decoded = round_trip(msg);
            let re_encoded = decoded.encode().unwrap();
            let original_encoded = msg.encode().unwrap();
            assert_eq!(original_encoded, re_encoded, "round-trip failed for {msg:?}");
        }
    }

    #[test]
    fn file_content_preserves_binary_data() {
        let data: Vec<u8> = (0..256).map(|i| i as u8).collect();
        let msg = Message::FileContent {
            path: "bin".into(),
            hash: 0,
            data: data.clone(),
        };
        match round_trip(&msg) {
            Message::FileContent { data: d, .. } => assert_eq!(d, data),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn manifest_entry_fields_preserved() {
        let entry = ManifestEntry {
            path: "deeply/nested/path/file.txt".into(),
            hash: u64::MAX,
            mtime: 1717718400,
            size: 999999999,
            is_dir: false,
        };
        let msg = Message::FileManifest { entries: vec![entry.clone()] };
        match round_trip(&msg) {
            Message::FileManifest { entries } => {
                assert_eq!(entries[0].path, entry.path);
                assert_eq!(entries[0].hash, entry.hash);
                assert_eq!(entries[0].mtime, entry.mtime);
                assert_eq!(entries[0].size, entry.size);
                assert_eq!(entries[0].is_dir, entry.is_dir);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn decode_garbage_returns_error() {
        let garbage = vec![0xFF, 0xFE, 0xFD];
        assert!(Message::decode(&garbage).is_err());
    }
}
