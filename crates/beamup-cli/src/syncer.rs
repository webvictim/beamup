use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use beamup_protocol::hash::{hash_content, hash_file};
use beamup_protocol::ignore::{relative_path, IgnoreRules};
use beamup_protocol::messages::{ManifestEntry, Message, PROTOCOL_VERSION};
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::beam::Beam;
use crate::transport::Transport;
use crate::watcher::{FsWatcher, WatchEvent};

const CHUNK_SIZE: usize = 256 * 1024;
const SUPPRESS_DURATION: Duration = Duration::from_millis(200);
const PING_INTERVAL: Duration = Duration::from_secs(5);
const PING_TIMEOUT: Duration = Duration::from_secs(15);

#[allow(dead_code)]
struct FileState {
    hash: u64,
    mtime: u64,
    size: u64,
}

#[allow(dead_code)]
pub struct SyncEngine {
    beam_id: String,
    local_dir: PathBuf,
    remote_dir: String,
    transport: Transport,
    ignore_rules: IgnoreRules,
    file_states: HashMap<String, FileState>,
    suppress_set: HashMap<String, Instant>,
    pending_events: HashMap<String, (WatchEvent, Instant)>,
}

impl SyncEngine {
    pub async fn new(beam_id: String, local_dir: PathBuf, remote_dir: String) -> Result<Self> {
        let mut child = Beam::spawn_agent(&beam_id, &remote_dir)?;

        let stdin = child.stdin.take().expect("agent stdin not captured");
        let stdout = child.stdout.take().expect("agent stdout not captured");

        let transport = Transport::new(stdout, stdin);
        let ignore_rules = IgnoreRules::load(&local_dir);

        Ok(Self {
            beam_id,
            local_dir,
            remote_dir,
            transport,
            ignore_rules,
            file_states: HashMap::new(),
            suppress_set: HashMap::new(),
            pending_events: HashMap::new(),
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        // Handshake
        let session_id = uuid_simple();
        self.transport
            .send(Message::Hello {
                version: PROTOCOL_VERSION,
                session_id: session_id.clone(),
            })
            .await?;

        match self.transport.recv().await? {
            Some(Message::HelloAck { version }) => {
                if version != PROTOCOL_VERSION {
                    anyhow::bail!("agent protocol version mismatch: {version}");
                }
                info!("handshake complete");
            }
            other => anyhow::bail!("expected HelloAck, got: {other:?}"),
        }

        // Initial sync — send our manifest
        eprintln!("Performing initial sync...");
        let manifest = self.build_local_manifest()?;
        let entry_count = manifest.len();
        self.transport
            .send(Message::FileManifest { entries: manifest })
            .await?;

        // Wait for ManifestAck (processing requests in between)
        let mut manifest_done = false;
        while !manifest_done {
            match self.transport.recv().await? {
                Some(Message::ManifestAck) => {
                    manifest_done = true;
                }
                Some(msg) => {
                    self.handle_remote_message(msg).await?;
                }
                None => anyhow::bail!("transport closed during initial sync"),
            }
        }
        eprintln!("Initial sync complete ({entry_count} entries).");

        // Start local watcher
        let mut watcher = FsWatcher::new(&self.local_dir, &self.ignore_rules)?;
        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        let mut last_pong = Instant::now();

        // Main loop
        loop {
            // Clean expired suppression entries
            self.suppress_set
                .retain(|_, expiry| expiry.elapsed() < SUPPRESS_DURATION);

            tokio::select! {
                msg = self.transport.recv() => {
                    match msg? {
                        Some(msg) => {
                            if matches!(msg, Message::Pong) {
                                last_pong = Instant::now();
                            } else {
                                self.handle_remote_message(msg).await?;
                            }
                        }
                        None => {
                            eprintln!("Connection to beam lost.");
                            break;
                        }
                    }
                }
                event = watcher.rx.recv() => {
                    if let Some(event) = event {
                        self.handle_local_event(event).await?;
                    }
                }
                _ = ping_interval.tick() => {
                    self.transport.send(Message::Ping).await?;
                    if last_pong.elapsed() > PING_TIMEOUT {
                        eprintln!("Beam not responding (no pong in {:?}), connection may be dead.", PING_TIMEOUT);
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_remote_message(&mut self, msg: Message) -> Result<()> {
        match msg {
            Message::Ping => {
                self.transport.send(Message::Pong).await?;
            }
            Message::RequestContent { path } => {
                self.send_file_content(&path).await?;
            }
            Message::FileContent { path, hash, data } => {
                self.write_local_file(&path, &data, hash).await?;
            }
            Message::FileContentChunk {
                path,
                offset,
                data,
                final_chunk,
            } => {
                self.append_local_chunk(&path, offset, &data, final_chunk)
                    .await?;
            }
            Message::FileChanged {
                path,
                hash,
                mtime: _,
                size: _,
            } => {
                let needs_content = match self.file_states.get(&path) {
                    Some(state) => state.hash != hash,
                    None => true,
                };
                if needs_content {
                    self.transport
                        .send(Message::RequestContent { path })
                        .await?;
                }
            }
            Message::FileDeleted { path } => {
                let full_path = self.local_dir.join(&path);
                if full_path.exists() {
                    self.suppress_set.insert(path.clone(), Instant::now());
                    std::fs::remove_file(&full_path)?;
                    self.file_states.remove(&path);
                    debug!("remote deleted: {path}");
                }
            }
            Message::DirCreated { path } => {
                let full_path = self.local_dir.join(&path);
                std::fs::create_dir_all(&full_path)?;
            }
            Message::DirDeleted { path } => {
                let full_path = self.local_dir.join(&path);
                if full_path.is_dir() {
                    self.suppress_set.insert(path.clone(), Instant::now());
                    let _ = std::fs::remove_dir_all(&full_path);
                    self.file_states.retain(|k, _| !k.starts_with(&path));
                }
            }
            Message::ConflictDetected {
                path,
                local_hash: _,
                remote_hash: _,
            } => {
                eprintln!(
                    "CONFLICT: {path} — local and remote both changed. Local saved as {path}.local.conflict"
                );
            }
            _ => {
                debug!("unhandled remote message: {msg:?}");
            }
        }
        Ok(())
    }

    async fn handle_local_event(&mut self, event: WatchEvent) -> Result<()> {
        match event {
            WatchEvent::Modified(path) => {
                let rel = relative_path(&self.local_dir, &path);
                let rel_str = rel.to_string_lossy().to_string();

                if self.ignore_rules.filter_path(&self.local_dir, &path, false) {
                    return Ok(());
                }
                if self.suppress_set.contains_key(&rel_str) {
                    return Ok(());
                }

                let data = match std::fs::read(&path) {
                    Ok(d) => d,
                    Err(_) => return Ok(()),
                };
                let hash = hash_content(&data);

                // Check if actually changed
                if let Some(state) = self.file_states.get(&rel_str) {
                    if state.hash == hash {
                        return Ok(());
                    }
                }

                let metadata = path.metadata()?;
                let mtime = metadata
                    .modified()
                    .unwrap_or(SystemTime::now())
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let size = metadata.len();

                self.file_states.insert(
                    rel_str.clone(),
                    FileState { hash, mtime, size },
                );

                self.transport
                    .send(Message::FileChanged {
                        path: rel_str,
                        hash,
                        mtime,
                        size,
                    })
                    .await?;
            }
            WatchEvent::Deleted(path) => {
                let rel = relative_path(&self.local_dir, &path);
                let rel_str = rel.to_string_lossy().to_string();

                if self.suppress_set.contains_key(&rel_str) {
                    return Ok(());
                }

                self.file_states.remove(&rel_str);
                self.transport
                    .send(Message::FileDeleted { path: rel_str })
                    .await?;
            }
            WatchEvent::DirCreated(path) => {
                let rel = relative_path(&self.local_dir, &path);
                let rel_str = rel.to_string_lossy().to_string();

                if self.ignore_rules.filter_path(&self.local_dir, &path, true) {
                    return Ok(());
                }

                self.transport
                    .send(Message::DirCreated { path: rel_str })
                    .await?;
            }
            WatchEvent::DirDeleted(path) => {
                let rel = relative_path(&self.local_dir, &path);
                let rel_str = rel.to_string_lossy().to_string();

                if self.suppress_set.contains_key(&rel_str) {
                    return Ok(());
                }

                self.file_states.retain(|k, _| !k.starts_with(&rel_str));
                self.transport
                    .send(Message::DirDeleted { path: rel_str })
                    .await?;
            }
        }
        Ok(())
    }

    async fn send_file_content(&mut self, path: &str) -> Result<()> {
        let full_path = self.local_dir.join(path);
        let data = match std::fs::read(&full_path) {
            Ok(d) => d,
            Err(e) => {
                warn!("cannot read {path}: {e}");
                return Ok(());
            }
        };

        let hash = hash_content(&data);
        let len = data.len();

        if len <= 1024 * 1024 {
            self.transport
                .send(Message::FileContent {
                    path: path.to_string(),
                    hash,
                    data,
                })
                .await?;
        } else {
            for (i, chunk) in data.chunks(CHUNK_SIZE).enumerate() {
                let offset = (i * CHUNK_SIZE) as u64;
                let final_chunk = offset as usize + chunk.len() >= len;
                self.transport
                    .send(Message::FileContentChunk {
                        path: path.to_string(),
                        offset,
                        data: chunk.to_vec(),
                        final_chunk,
                    })
                    .await?;
                if i % 16 == 15 {
                    tokio::task::yield_now().await;
                }
            }
        }

        debug!("sent to remote: {path} ({len} bytes)");
        Ok(())
    }

    async fn write_local_file(&mut self, path: &str, data: &[u8], hash: u64) -> Result<()> {
        let full_path = self.local_dir.join(path);

        // Conflict detection
        if let Some(state) = self.file_states.get(path) {
            if full_path.exists() {
                let current_hash = hash_file(&full_path).unwrap_or(0);
                if current_hash != state.hash && current_hash != hash {
                    // Both sides changed — conflict!
                    let conflict_path = format!("{}.local.conflict", full_path.display());
                    std::fs::copy(&full_path, &conflict_path)?;
                    eprintln!(
                        "CONFLICT: {path} — local version saved as {path}.local.conflict"
                    );
                }
            }
        }

        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let tmp_path = full_path.with_extension("beamup-tmp");
        std::fs::write(&tmp_path, data)?;
        std::fs::rename(&tmp_path, &full_path)?;

        self.suppress_set.insert(path.to_string(), Instant::now());

        let mtime = full_path
            .metadata()
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::now())
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.file_states.insert(
            path.to_string(),
            FileState {
                hash,
                mtime,
                size: data.len() as u64,
            },
        );

        debug!("wrote locally: {path} ({} bytes)", data.len());
        Ok(())
    }

    async fn append_local_chunk(
        &mut self,
        path: &str,
        offset: u64,
        data: &[u8],
        final_chunk: bool,
    ) -> Result<()> {
        let full_path = self.local_dir.join(path);
        let tmp_path = full_path.with_extension("beamup-chunk-tmp");

        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        use std::io::{Seek, Write};
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&tmp_path)?;
        file.seek(std::io::SeekFrom::Start(offset))?;
        file.write_all(data)?;

        if final_chunk {
            drop(file);
            std::fs::rename(&tmp_path, &full_path)?;
            self.suppress_set.insert(path.to_string(), Instant::now());

            let content = std::fs::read(&full_path)?;
            let hash = hash_content(&content);
            let size = content.len() as u64;
            let mtime = full_path
                .metadata()
                .and_then(|m| m.modified())
                .unwrap_or(SystemTime::now())
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            self.file_states
                .insert(path.to_string(), FileState { hash, mtime, size });
            debug!("wrote chunked locally: {path} ({size} bytes)");
        }

        Ok(())
    }

    fn build_local_manifest(&mut self) -> Result<Vec<ManifestEntry>> {
        let mut entries = Vec::new();
        self.walk_local_dir(&self.local_dir.clone(), &mut entries)?;
        Ok(entries)
    }

    fn walk_local_dir(&mut self, dir: &Path, entries: &mut Vec<ManifestEntry>) -> Result<()> {
        let read_dir = match std::fs::read_dir(dir) {
            Ok(rd) => rd,
            Err(_) => return Ok(()),
        };

        for entry in read_dir.flatten() {
            let path = entry.path();
            let is_dir = path.is_dir();

            if self.ignore_rules.filter_path(&self.local_dir, &path, is_dir) {
                continue;
            }

            let relative = path
                .strip_prefix(&self.local_dir)
                .unwrap_or(&path)
                .to_string_lossy()
                .to_string();

            if is_dir {
                entries.push(ManifestEntry {
                    path: relative.clone(),
                    hash: 0,
                    mtime: 0,
                    size: 0,
                    is_dir: true,
                });
                self.walk_local_dir(&path, entries)?;
            } else {
                let metadata = path.metadata()?;
                let mtime = metadata
                    .modified()
                    .unwrap_or(SystemTime::now())
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let size = metadata.len();
                let hash = hash_file(&path).unwrap_or(0);

                self.file_states.insert(
                    relative.clone(),
                    FileState { hash, mtime, size },
                );

                entries.push(ManifestEntry {
                    path: relative,
                    hash,
                    mtime,
                    size,
                    is_dir: false,
                });
            }
        }

        Ok(())
    }
}

fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:x}", ts)
}
