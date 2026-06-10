use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use beamup_protocol::hash::hash_file;
use beamup_protocol::ignore::{relative_path, IgnoreRules};
use beamup_protocol::messages::{ManifestEntry, Message, SyncDirection, INLINE_THRESHOLD, PROTOCOL_VERSION};
use indicatif::{ProgressBar, ProgressStyle};
use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::beam::Beam;
use crate::progress;
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
    chunk_size: usize,
    initial_direction: SyncDirection,
    ongoing_direction: SyncDirection,
}

impl SyncEngine {
    pub async fn new(
        beam_id: String,
        local_dir: PathBuf,
        remote_dir: String,
        concurrency: usize,
        chunk_size: usize,
        initial_direction: SyncDirection,
        ongoing_direction: SyncDirection,
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
            chunk_size,
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
            chunk_size,
            initial_direction,
            ongoing_direction,
        })
    }

    pub async fn run(&mut self, on_sync_complete: Option<oneshot::Sender<()>>) -> Result<()> {
        // Handshake
        let session_id = uuid_simple();
        self.transport
            .send(Message::Hello {
                version: PROTOCOL_VERSION,
                session_id: session_id.clone(),
                initial_direction: self.initial_direction,
                ongoing_direction: self.ongoing_direction,
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
        info!("performing initial sync...");
        let sync_start = Instant::now();
        let sync_bytes = self.initial_sync().await?;
        let sync_duration = sync_start.elapsed();
        let sync_secs = sync_duration.as_secs_f64();
        let bw = if sync_secs > 0.0 {
            sync_bytes as f64 / sync_secs / 1024.0 / 1024.0
        } else {
            0.0
        };
        info!(
            "initial sync complete: {} in {:.1}s ({:.1} MB/s)",
            format_size(sync_bytes),
            sync_secs,
            bw
        );

        if on_sync_complete.is_none() {
            eprintln!("Connect with: tsh beams console {}", self.beam_id);
        }

        if let Some(tx) = on_sync_complete {
            let _ = tx.send(());
        }

        // Start local watcher (only if ongoing sync pushes local changes)
        let mut watcher = if self.ongoing_direction.should_push() {
            Some(FsWatcher::new(&self.local_dir, &self.ignore_rules)?)
        } else {
            None
        };
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
                            warn!("connection to beam lost");
                            break;
                        }
                    }
                }
                event = async {
                    match watcher.as_mut() {
                        Some(w) => w.rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
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
                        warn!("beam not responding (no pong in {:?}), connection may be dead", PING_TIMEOUT);
                    }
                }
            }
        }

        Ok(())
    }

    async fn initial_sync(&mut self) -> Result<u64> {
        let mut total_bytes: u64 = 0;

        // Build and send our manifest
        let manifest = self.build_local_manifest()?;
        let entry_count = manifest.len();
        info!("local: {entry_count} entries");
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
                    total_bytes += data.len() as u64;
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
        info!("sync plan: push {push_count} files, pull {pull_count} files");

        // Set up progress bar (only counting bytes for the active direction)
        let total_push_bytes: u64 = if self.initial_direction.should_push() {
            to_push.iter().map(|e| e.size).sum()
        } else {
            0
        };
        let total_pull_bytes: u64 = if self.initial_direction.should_pull() {
            to_pull.iter().map(|e| e.size).sum()
        } else {
            0
        };
        let total_bytes_expected = total_push_bytes + total_pull_bytes;
        let total_files = if self.initial_direction.should_push() { push_count } else { 0 }
            + if self.initial_direction.should_pull() { pull_count } else { 0 };
        let mut files_done: usize = 0;

        let pb = if total_bytes_expected > 0 {
            let pb = ProgressBar::new(total_bytes_expected);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} [{bar:30.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}) {msg} [{eta}]")
                    .unwrap()
                    .progress_chars("=>-"),
            );
            pb.set_message(format!("0/{total_files} files"));
            pb.enable_steady_tick(std::time::Duration::from_millis(100));
            progress::set_progress_bar(pb.clone());
            Some(pb)
        } else {
            None
        };

        // Push files: tar batch for all files <= chunk_size, chunked scp for huge files
        if !to_push.is_empty() && self.initial_direction.should_push() {
            let mut tar_files = Vec::new();
            let mut huge = Vec::new();
            for entry in &to_push {
                if entry.size <= self.chunk_size as u64 {
                    tar_files.push(entry);
                } else {
                    huge.push(entry);
                }
            }

            // Push all normal files via tar batches (parallel pipes)
            if !tar_files.is_empty() {
                let paths: Vec<String> = tar_files.iter().map(|e| e.path.clone()).collect();
                let sizes: Vec<u64> = tar_files.iter().map(|e| e.size).collect();
                let tar_total: u64 = sizes.iter().sum();
                let results = self.transfer_pool.push_batch_tar(paths, sizes).await;
                total_bytes += tar_total;
                let failures: Vec<_> = results.iter().filter(|r| !r.success).collect();
                if !failures.is_empty() {
                    warn!("{} tar push failures", failures.len());
                }

                files_done += tar_files.len();
                if let Some(ref pb) = pb {
                    pb.inc(tar_total);
                    pb.set_message(format!("{files_done}/{total_files} files"));
                }

                for entry in &tar_files {
                    self.transport
                        .send(Message::FileReady {
                            path: entry.path.clone(),
                            hash: entry.hash,
                            size: entry.size,
                        })
                        .await?;
                }
            }

            // Push huge files via chunked scp
            if !huge.is_empty() {
                let num_huge = huge.len();
                let paths: Vec<String> = huge.iter().map(|e| e.path.clone()).collect();
                for path in paths {
                    self.transfer_pool.push(path);
                }
                let mut remaining = num_huge;
                while remaining > 0 {
                    tokio::select! {
                        Some(result) = self.transfer_pool.results_rx.recv() => {
                            remaining -= 1;
                            if result.success {
                                files_done += 1;
                                if let Some(ref pb) = pb {
                                    pb.set_message(format!("{files_done}/{total_files} files"));
                                }
                            } else {
                                warn!("push failed: {} — {:?}", result.path, result.error);
                            }
                        }
                        Some(bytes) = self.transfer_pool.progress_rx.recv() => {
                            total_bytes += bytes;
                            if let Some(ref pb) = pb {
                                pb.inc(bytes);
                            }
                        }
                    }
                }
                // Drain any remaining progress messages
                while let Ok(bytes) = self.transfer_pool.progress_rx.try_recv() {
                    total_bytes += bytes;
                    if let Some(ref pb) = pb {
                        pb.inc(bytes);
                    }
                }

                for entry in &huge {
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

        // Pull files: tar batch for all files <= chunk_size, chunked scp for huge files
        if !to_pull.is_empty() && self.initial_direction.should_pull() {
            let mut tar_files = Vec::new();
            let mut huge_pull = Vec::new();
            for entry in &to_pull {
                if entry.size <= self.chunk_size as u64 {
                    tar_files.push(entry);
                } else {
                    huge_pull.push(entry);
                }
            }

            // Pull normal files via tar batch
            if !tar_files.is_empty() {
                let paths: Vec<String> = tar_files.iter().map(|e| e.path.clone()).collect();
                let sizes: Vec<u64> = tar_files.iter().map(|e| e.size).collect();
                let pull_total: u64 = sizes.iter().sum();
                let results = self.transfer_pool.pull_batch_tar(paths, sizes).await;
                total_bytes += pull_total;
                let failures: Vec<_> = results.iter().filter(|r| !r.success).collect();
                if !failures.is_empty() {
                    warn!("{} tar pull failures", failures.len());
                }

                files_done += tar_files.len();
                if let Some(ref pb) = pb {
                    pb.inc(pull_total);
                    pb.set_message(format!("{files_done}/{total_files} files"));
                }
            }

            // Pull huge files via chunked scp
            if !huge_pull.is_empty() {
                let num_huge = huge_pull.len();
                let paths: Vec<String> = huge_pull.iter().map(|e| e.path.clone()).collect();
                for path in paths {
                    self.transfer_pool.pull(path);
                }
                let mut remaining = num_huge;
                while remaining > 0 {
                    tokio::select! {
                        Some(result) = self.transfer_pool.results_rx.recv() => {
                            remaining -= 1;
                            if result.success {
                                files_done += 1;
                                if let Some(ref pb) = pb {
                                    pb.set_message(format!("{files_done}/{total_files} files"));
                                }
                            } else {
                                warn!("pull failed: {} — {:?}", result.path, result.error);
                            }
                        }
                        Some(bytes) = self.transfer_pool.progress_rx.recv() => {
                            total_bytes += bytes;
                            if let Some(ref pb) = pb {
                                pb.inc(bytes);
                            }
                        }
                    }
                }
                // Drain any remaining progress messages
                while let Ok(bytes) = self.transfer_pool.progress_rx.try_recv() {
                    total_bytes += bytes;
                    if let Some(ref pb) = pb {
                        pb.inc(bytes);
                    }
                }
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

        if let Some(pb) = pb {
            pb.finish_and_clear();
            progress::clear_progress_bar();
        }

        // Exchange ManifestAck
        info!("sending ManifestAck, waiting for agent...");
        self.transport.send(Message::ManifestAck).await?;

        // Wait for agent's ManifestAck
        let mut ack_inline_count = 0u64;
        loop {
            match self.transport.recv().await? {
                Some(Message::ManifestAck) => break,
                Some(Message::FileContent { path, hash, data }) => {
                    ack_inline_count += 1;
                    total_bytes += data.len() as u64;
                    self.write_local_file_inline(&path, &data, hash)?;
                }
                Some(_) => {}
                None => anyhow::bail!("transport closed waiting for ManifestAck"),
            }
        }
        if ack_inline_count > 0 {
            info!("received {ack_inline_count} inline files from agent during ack");
        }

        Ok(total_bytes)
    }

    async fn handle_remote_message(&mut self, msg: Message) -> Result<()> {
        match &msg {
            Message::Ping => {
                self.transport.send(Message::Pong).await?;
                return Ok(());
            }
            _ if !self.ongoing_direction.should_pull() => {
                return Ok(());
            }
            _ => {}
        }
        match msg {
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
                warn!("CONFLICT: {path} — local and remote both changed. Local saved as {path}.local.conflict");
            }
            _ => {
                debug!("unhandled remote message: {msg:?}");
            }
        }
        Ok(())
    }

    async fn handle_local_event(&mut self, event: WatchEvent) -> Result<()> {
        if !self.ongoing_direction.should_push() {
            return Ok(());
        }
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
                    warn!(
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

fn format_size(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1} GB", bytes as f64 / 1024.0 / 1024.0 / 1024.0)
    } else if bytes >= 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / 1024.0 / 1024.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}
