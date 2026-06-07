use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u32 = 2;

/// Threshold below which files are sent inline via the pipe.
/// Above this, scp is used for transfer.
pub const INLINE_THRESHOLD: u64 = 64 * 1024; // 64 KB

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub path: String,
    pub hash: u64,
    pub mtime: u64,
    pub size: u64,
    pub is_dir: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
