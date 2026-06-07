use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use beamup_protocol::hash::hash_file;
use beamup_protocol::ignore::{relative_path, IgnoreRules};
use beamup_protocol::messages::{ManifestEntry, Message, INLINE_THRESHOLD, PROTOCOL_VERSION};
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::beam::Beam;
use crate::transfer::TransferPool;
use crate::transport::Transport;
use crate::watcher::{FsWatcher, WatchEvent};

const SUPPRESS_DURATION: Duration = Duration::from_millis(500);
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
    transfer_pool: TransferPool,
    ignore_rules: IgnoreRules,
    file_states: HashMap<String, FileState>,
    suppress_set: HashMap<String, Instant>,
    in_flight: HashSet<String>,
}

impl SyncEngine {
    pub async fn new(
        beam_id: String,
        local_dir: PathBuf,
        remote_dir: String,
        concurrency: usize,
    ) -> Result<Self> {
        let mut child = Beam::spawn_agent(&beam_id, &remote_dir)?;

        let stdin = child.stdin.take().expect("agent stdin not captured");
        let stdout = child.stdout.take().expect("agent stdout not captured");

        let transport = Transport::new(stdout, stdin);
        let ignore_rules = IgnoreRules::load(&local_dir);
        let transfer_pool = TransferPool::new(
            beam_id.clone(),
            local_dir.clone(),
            remote_dir.clone(),
            concurrency,
        );

        Ok(Self {
            beam_id,
            local_dir,
            remote_dir,
            transport,
            transfer_pool,
            ignore_rules,
            file_states: HashMap::new(),
            suppress_set: HashMap::new(),
            in_flight: HashSet::new(),
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

        // Initial sync
        eprintln!("Performing initial sync...");
        self.initial_sync().await?;
        eprintln!("Initial sync complete.");

        // Start local watcher
        let mut watcher = FsWatcher::new(&self.local_dir, &self.ignore_rules)?;
        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        let mut last_pong = Instant::now();

        // Main loop
        loop {
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
                result = self.transfer_pool.results_rx.recv() => {
                    if let Some(result) = result {
                        self.handle_transfer_result(result).await?;
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

    async fn initial_sync(&mut self) -> Result<()> {
        // Build and send our manifest
        let manifest = self.build_local_manifest()?;
        let entry_count = manifest.len();
        eprintln!("  Local: {entry_count} entries");
        self.transport
            .send(Message::FileManifest { entries: manifest })
            .await?;

        // Wait for SyncPlan from agent
        let (to_push, to_pull) = loop {
            match self.transport.recv().await? {
                Some(Message::SyncPlan { to_push, to_pull }) => {
                    break (to_push, to_pull);
                }
                Some(Message::FileContent { path, hash, data }) => {
                    // Agent sends small files inline during initial sync
                    self.write_local_file_inline(&path, &data, hash)?;
                }
                Some(other) => {
                    debug!("during initial sync, got: {other:?}");
                }
                None => anyhow::bail!("transport closed during initial sync"),
            }
        };

        let push_count = to_push.len();
        let pull_count = to_pull.len();
        eprintln!("  Plan: push {push_count} files, pull {pull_count} files");

        // Split pushes into inline (small) and scp (large)
        if !to_push.is_empty() {
            let (small, large): (Vec<_>, Vec<_>) = to_push
                .iter()
                .partition(|e| e.size <= INLINE_THRESHOLD);

            // Send small files inline
            for entry in &small {
                let full_path = self.local_dir.join(&entry.path);
                if let Ok(data) = std::fs::read(&full_path) {
                    let hash = beamup_protocol::hash::hash_content(&data);
                    self.transport
                        .send(Message::FileContent {
                            path: entry.path.clone(),
                            hash,
                            data,
                        })
                        .await?;
                }
            }

            // Push large files via scp
            if !large.is_empty() {
                let paths: Vec<String> = large.iter().map(|e| e.path.clone()).collect();
                let results = self.transfer_pool.push_batch(paths).await;
                let failures: Vec<_> = results.iter().filter(|r| !r.success).collect();
                if !failures.is_empty() {
                    for f in &failures {
                        warn!("push failed: {} — {:?}", f.path, f.error);
                    }
                    eprintln!("  {} push failures (see log)", failures.len());
                }

                for entry in &large {
                    self.transport
                        .send(Message::FileReady {
                            path: entry.path.clone(),
                            hash: entry.hash,
                            size: entry.size,
                        })
                        .await?;
                }
            }
        }

        // Execute pulls in parallel
        if !to_pull.is_empty() {
            let paths: Vec<String> = to_pull.iter().map(|e| e.path.clone()).collect();
            let results = self.transfer_pool.pull_batch(paths).await;
            let failures: Vec<_> = results.iter().filter(|r| !r.success).collect();
            if !failures.is_empty() {
                for f in &failures {
                    warn!("pull failed: {} — {:?}", f.path, f.error);
                }
                eprintln!("  {} pull failures (see log)", failures.len());
            }

            // Update local state for pulled files
            for entry in &to_pull {
                let full_path = self.local_dir.join(&entry.path);
                if full_path.exists() {
                    let hash = hash_file(&full_path).unwrap_or(0);
                    let metadata = full_path.metadata()?;
                    let mtime = metadata
                        .modified()
                        .unwrap_or(SystemTime::now())
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    self.file_states.insert(
                        entry.path.clone(),
                        FileState {
                            hash,
                            mtime,
                            size: metadata.len(),
                        },
                    );
                    self.suppress_set.insert(entry.path.clone(), Instant::now());
                }
            }

            // Notify agent we received the files
            for entry in &to_pull {
                self.transport
                    .send(Message::FileReceived {
                        path: entry.path.clone(),
                        hash: entry.hash,
                    })
                    .await?;
            }
        }

        // Exchange ManifestAck
        self.transport.send(Message::ManifestAck).await?;

        // Wait for agent's ManifestAck
        loop {
            match self.transport.recv().await? {
                Some(Message::ManifestAck) => break,
                Some(Message::FileContent { path, hash, data }) => {
                    self.write_local_file_inline(&path, &data, hash)?;
                }
                Some(_) => {}
                None => anyhow::bail!("transport closed waiting for ManifestAck"),
            }
        }

        Ok(())
    }

    async fn handle_remote_message(&mut self, msg: Message) -> Result<()> {
        match msg {
            Message::Ping => {
                self.transport.send(Message::Pong).await?;
            }
            Message::FileContent { path, hash, data } => {
                self.write_local_file_inline(&path, &data, hash)?;
            }
            Message::FileChanged {
                path,
                hash,
                mtime: _,
                size,
            } => {
                let needs_content = match self.file_states.get(&path) {
                    Some(state) => state.hash != hash,
                    None => true,
                };
                if needs_content {
                    if size <= INLINE_THRESHOLD {
                        // Agent will send inline — nothing to do, it'll arrive as FileContent
                    } else if !self.in_flight.contains(&path) {
                        // Pull via scp
                        self.in_flight.insert(path.clone());
                        self.transfer_pool.pull(path);
                    }
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

                let metadata = match path.metadata() {
                    Ok(m) => m,
                    Err(_) => return Ok(()),
                };
                let size = metadata.len();
                let mtime = metadata
                    .modified()
                    .unwrap_or(SystemTime::now())
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                let hash = hash_file(&path).unwrap_or(0);

                // Check if actually changed
                if let Some(state) = self.file_states.get(&rel_str) {
                    if state.hash == hash {
                        return Ok(());
                    }
                }

                self.file_states.insert(
                    rel_str.clone(),
                    FileState { hash, mtime, size },
                );

                if size <= INLINE_THRESHOLD {
                    // Send small file inline
                    let data = match std::fs::read(&path) {
                        Ok(d) => d,
                        Err(_) => return Ok(()),
                    };
                    self.transport
                        .send(Message::FileContent {
                            path: rel_str,
                            hash,
                            data,
                        })
                        .await?;
                } else if !self.in_flight.contains(&rel_str) {
                    // Notify agent of change, then push via scp
                    self.in_flight.insert(rel_str.clone());
                    self.transport
                        .send(Message::FileChanged {
                            path: rel_str.clone(),
                            hash,
                            mtime,
                            size,
                        })
                        .await?;
                    self.transfer_pool.push(rel_str);
                }
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

    async fn handle_transfer_result(
        &mut self,
        result: crate::transfer::TransferResult,
    ) -> Result<()> {
        self.in_flight.remove(&result.path);

        if !result.success {
            warn!("transfer failed: {} — {:?}", result.path, result.error);
            return Ok(());
        }

        match result.direction {
            crate::transfer::Direction::Push => {
                // We pushed a file to the beam — notify agent it's ready
                if let Some(state) = self.file_states.get(&result.path) {
                    self.transport
                        .send(Message::FileReady {
                            path: result.path,
                            hash: state.hash,
                            size: state.size,
                        })
                        .await?;
                }
            }
            crate::transfer::Direction::Pull => {
                // We pulled a file from the beam — update local state
                let full_path = self.local_dir.join(&result.path);
                if full_path.exists() {
                    let hash = hash_file(&full_path).unwrap_or(0);
                    let metadata = full_path.metadata()?;
                    let mtime = metadata
                        .modified()
                        .unwrap_or(SystemTime::now())
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    self.file_states.insert(
                        result.path.clone(),
                        FileState {
                            hash,
                            mtime,
                            size: metadata.len(),
                        },
                    );
                    self.suppress_set
                        .insert(result.path.clone(), Instant::now());

                    self.transport
                        .send(Message::FileReceived {
                            path: result.path,
                            hash,
                        })
                        .await?;
                }
            }
        }
        Ok(())
    }

    fn write_local_file_inline(&mut self, path: &str, data: &[u8], hash: u64) -> Result<()> {
        let full_path = self.local_dir.join(path);

        // Conflict detection
        if let Some(state) = self.file_states.get(path) {
            if full_path.exists() {
                let current_hash = hash_file(&full_path).unwrap_or(0);
                if current_hash != state.hash && current_hash != hash {
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

        debug!("wrote locally (inline): {path} ({} bytes)", data.len());
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
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:x}", ts)
}
