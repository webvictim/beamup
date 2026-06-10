use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use beamup_protocol::hash::{hash_content, hash_file};
use beamup_protocol::ignore::IgnoreRules;
use beamup_protocol::messages::{ManifestEntry, Message, SyncDirection, SyncEntry, INLINE_THRESHOLD, PROTOCOL_VERSION};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{debug, info, warn};

#[cfg(target_os = "linux")]
use tracing::error;

use crate::transport::Transport;

const SUPPRESS_DURATION: Duration = Duration::from_millis(500);

#[allow(dead_code)]
struct FileState {
    hash: u64,
    mtime: u64,
    size: u64,
}

pub async fn run(watch_dir: PathBuf) -> Result<()> {
    let mut transport = Transport::new();
    let ignore_rules = IgnoreRules::load(&watch_dir);
    let mut file_states: HashMap<String, FileState> = HashMap::new();
    let mut suppress_set: HashMap<String, Instant> = HashMap::new();

    // Wait for Hello
    let (initial_direction, ongoing_direction) = match transport.recv().await? {
        Some(Message::Hello {
            version,
            session_id,
            initial_direction,
            ongoing_direction,
        }) => {
            if version != PROTOCOL_VERSION {
                anyhow::bail!(
                    "protocol version mismatch: got {version}, expected {PROTOCOL_VERSION}"
                );
            }
            transport
                .send(Message::HelloAck {
                    version: PROTOCOL_VERSION,
                })
                .await?;
            info!("handshake complete, session: {session_id}, initial: {initial_direction:?}, ongoing: {ongoing_direction:?}");
            (initial_direction, ongoing_direction)
        }
        other => anyhow::bail!("expected Hello, got: {other:?}"),
    };

    // Set up watcher channel
    #[allow(unused_variables)]
    let (watch_tx, mut watch_rx) = mpsc::channel::<WatchEvent>(1024);

    // Start filesystem watcher (only if ongoing sync pulls from beam)
    #[cfg(target_os = "linux")]
    if ongoing_direction.should_pull() {
        let dir = watch_dir.clone();
        let rules = IgnoreRules::load(&dir);
        tokio::task::spawn_blocking(move || {
            if let Err(e) = crate::watcher::watch(dir, rules, watch_tx) {
                error!("watcher error: {e}");
            }
        });
    }

    loop {
        tokio::select! {
            msg = transport.recv() => {
                match msg? {
                    Some(msg) => {
                        handle_message(
                            msg,
                            &transport,
                            &watch_dir,
                            &ignore_rules,
                            &mut file_states,
                            &mut suppress_set,
                            initial_direction,
                        ).await?;
                    }
                    None => {
                        info!("transport closed, shutting down");
                        break;
                    }
                }
            }
            event = watch_rx.recv() => {
                if let Some(event) = event {
                    if ongoing_direction.should_pull() {
                        handle_watch_event(
                            event,
                            &transport,
                            &watch_dir,
                            &mut file_states,
                            &mut suppress_set,
                        ).await?;
                    }
                }
            }
        }

        suppress_set.retain(|_, expiry| expiry.elapsed() < SUPPRESS_DURATION);
    }

    Ok(())
}

async fn handle_message(
    msg: Message,
    transport: &Transport,
    watch_dir: &Path,
    ignore_rules: &IgnoreRules,
    file_states: &mut HashMap<String, FileState>,
    suppress_set: &mut HashMap<String, Instant>,
    initial_direction: SyncDirection,
) -> Result<()> {
    match msg {
        Message::Ping => {
            transport.send(Message::Pong).await?;
        }
        Message::Pong => {}
        Message::FileManifest { entries } => {
            handle_manifest(entries, transport, watch_dir, ignore_rules, file_states, initial_direction).await?;
        }
        Message::FileContent { path, hash, data } => {
            write_file(&path, &data, hash, watch_dir, file_states, suppress_set)?;
        }
        Message::FileReady {
            path,
            hash,
            size,
        } => {
            // CLI has pushed a file via scp — update our state
            suppress_set.insert(path.clone(), Instant::now());
            let full_path = watch_dir.join(&path);
            let actual_hash = if full_path.exists() {
                hash_file(&full_path).unwrap_or(0)
            } else {
                0
            };
            let mtime = full_path
                .metadata()
                .and_then(|m| m.modified())
                .unwrap_or(SystemTime::now())
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            file_states.insert(
                path.clone(),
                FileState {
                    hash: actual_hash,
                    mtime,
                    size,
                },
            );
            debug!("file ready (scp'd): {path} (expected hash {hash}, actual {actual_hash})");
        }
        Message::FileReceived { path, hash } => {
            // CLI pulled a file via scp — just bookkeeping
            debug!("file received by cli: {path} (hash {hash})");
        }
        Message::FileChanged {
            path,
            hash: _,
            mtime: _,
            size,
        } => {
            // CLI is about to push this file via scp — add to suppress set
            suppress_set.insert(path.clone(), Instant::now());
            debug!("file incoming via scp: {path} (size {size})");
        }
        Message::FileDeleted { path } => {
            let full_path = watch_dir.join(&path);
            if full_path.exists() {
                suppress_set.insert(path.clone(), Instant::now());
                std::fs::remove_file(&full_path)?;
                file_states.remove(&path);
                debug!("deleted: {path}");
            }
        }
        Message::DirCreated { path } => {
            let full_path = watch_dir.join(&path);
            std::fs::create_dir_all(&full_path)?;
        }
        Message::DirDeleted { path } => {
            let full_path = watch_dir.join(&path);
            if full_path.is_dir() {
                suppress_set.insert(path.clone(), Instant::now());
                let _ = std::fs::remove_dir_all(&full_path);
                file_states.retain(|k, _| !k.starts_with(&path));
            }
        }
        Message::ManifestAck => {
            // CLI finished initial sync — send our ack back
            transport.send(Message::ManifestAck).await?;
            info!("initial sync complete");
        }
        Message::Shutdown => {
            info!("received shutdown, exiting");
            std::process::exit(0);
        }
        _ => {
            warn!("unhandled message: {msg:?}");
        }
    }
    Ok(())
}

async fn handle_manifest(
    remote_entries: Vec<ManifestEntry>,
    transport: &Transport,
    watch_dir: &Path,
    ignore_rules: &IgnoreRules,
    file_states: &mut HashMap<String, FileState>,
    initial_direction: SyncDirection,
) -> Result<()> {
    let remote_map: HashMap<&str, &ManifestEntry> =
        remote_entries.iter().map(|e| (e.path.as_str(), e)).collect();

    // Build local manifest
    let local_entries = build_manifest(watch_dir, ignore_rules)?;
    let local_map: HashMap<&str, &ManifestEntry> =
        local_entries.iter().map(|e| (e.path.as_str(), e)).collect();

    let mut to_push: Vec<SyncEntry> = Vec::new(); // CLI should push these to us
    let mut to_pull: Vec<SyncEntry> = Vec::new(); // CLI should pull these from us

    // Files remote (CLI) has that we don't — CLI should push them
    if initial_direction.should_push() {
        for entry in &remote_entries {
            if entry.is_dir {
                let dir_path = watch_dir.join(&entry.path);
                std::fs::create_dir_all(&dir_path)?;
                continue;
            }
            if !local_map.contains_key(entry.path.as_str()) {
                to_push.push(SyncEntry {
                    path: entry.path.clone(),
                    hash: entry.hash,
                    size: entry.size,
                });
            } else {
                let local = local_map[entry.path.as_str()];
                if local.hash != entry.hash {
                    to_push.push(SyncEntry {
                        path: entry.path.clone(),
                        hash: entry.hash,
                        size: entry.size,
                    });
                }
            }
        }
    }

    // Files we have that remote doesn't — CLI should pull them
    if initial_direction.should_pull() {
        for entry in &local_entries {
            if entry.is_dir {
                continue;
            }
            if !remote_map.contains_key(entry.path.as_str()) {
                to_pull.push(SyncEntry {
                    path: entry.path.clone(),
                    hash: entry.hash,
                    size: entry.size,
                });
            }
        }
    }

    // Update file states for local files
    for entry in &local_entries {
        if !entry.is_dir {
            file_states.insert(
                entry.path.clone(),
                FileState {
                    hash: entry.hash,
                    mtime: entry.mtime,
                    size: entry.size,
                },
            );
        }
    }

    // Send the sync plan
    transport
        .send(Message::SyncPlan { to_push, to_pull })
        .await?;

    info!(
        "manifest processed: {} local, {} remote entries",
        local_entries.len(),
        remote_entries.len()
    );
    Ok(())
}

#[allow(dead_code)]
async fn send_file_inline(path: &str, transport: &Transport, watch_dir: &Path) -> Result<()> {
    let full_path = watch_dir.join(path);
    let data = match std::fs::read(&full_path) {
        Ok(d) => d,
        Err(e) => {
            warn!("cannot read {path}: {e}");
            return Ok(());
        }
    };
    let hash = hash_content(&data);

    transport
        .send(Message::FileContent {
            path: path.to_string(),
            hash,
            data,
        })
        .await?;

    debug!("sent inline: {path}");
    Ok(())
}

fn write_file(
    path: &str,
    data: &[u8],
    hash: u64,
    watch_dir: &Path,
    file_states: &mut HashMap<String, FileState>,
    suppress_set: &mut HashMap<String, Instant>,
) -> Result<()> {
    let full_path = watch_dir.join(path);

    if let Some(parent) = full_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let tmp_path = full_path.with_extension("beamup-tmp");
    std::fs::write(&tmp_path, data)?;
    std::fs::rename(&tmp_path, &full_path)?;

    suppress_set.insert(path.to_string(), Instant::now());

    let mtime = full_path
        .metadata()
        .and_then(|m| m.modified())
        .unwrap_or(SystemTime::now())
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    file_states.insert(
        path.to_string(),
        FileState {
            hash,
            mtime,
            size: data.len() as u64,
        },
    );

    debug!("wrote: {path} ({} bytes)", data.len());
    Ok(())
}

pub fn build_manifest(root: &Path, ignore_rules: &IgnoreRules) -> Result<Vec<ManifestEntry>> {
    let mut entries = Vec::new();
    walk_dir(root, root, ignore_rules, &mut entries)?;
    Ok(entries)
}

fn walk_dir(
    base: &Path,
    dir: &Path,
    ignore_rules: &IgnoreRules,
    entries: &mut Vec<ManifestEntry>,
) -> Result<()> {
    let read_dir = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(_) => return Ok(()),
    };

    for entry in read_dir {
        let entry = entry?;
        let path = entry.path();
        let is_dir = path.is_dir();

        if ignore_rules.filter_path(base, &path, is_dir) {
            continue;
        }

        let relative = path
            .strip_prefix(base)
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
            walk_dir(base, &path, ignore_rules, entries)?;
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

#[derive(Debug)]
#[allow(dead_code)]
pub enum WatchEvent {
    Modified(String),
    Deleted(String),
    DirCreated(String),
    DirDeleted(String),
}

async fn handle_watch_event(
    event: WatchEvent,
    transport: &Transport,
    watch_dir: &Path,
    file_states: &mut HashMap<String, FileState>,
    suppress_set: &mut HashMap<String, Instant>,
) -> Result<()> {
    match event {
        WatchEvent::Modified(path) => {
            if suppress_set.contains_key(&path) {
                return Ok(());
            }
            let full_path = watch_dir.join(&path);
            let metadata = match full_path.metadata() {
                Ok(m) => m,
                Err(_) => return Ok(()),
            };
            let size = metadata.len();
            let hash = hash_file(&full_path).unwrap_or(0);

            if let Some(state) = file_states.get(&path) {
                if state.hash == hash {
                    return Ok(());
                }
            }

            let mtime = metadata
                .modified()
                .unwrap_or(SystemTime::now())
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            file_states.insert(path.clone(), FileState { hash, mtime, size });

            if size <= INLINE_THRESHOLD {
                // Send small file inline
                let data = match std::fs::read(&full_path) {
                    Ok(d) => d,
                    Err(_) => return Ok(()),
                };
                transport
                    .send(Message::FileContent {
                        path,
                        hash,
                        data,
                    })
                    .await?;
            } else {
                // Notify CLI — it will pull via scp
                transport
                    .send(Message::FileChanged {
                        path,
                        hash,
                        mtime,
                        size,
                    })
                    .await?;
            }
        }
        WatchEvent::Deleted(path) => {
            if suppress_set.contains_key(&path) {
                return Ok(());
            }
            file_states.remove(&path);
            transport.send(Message::FileDeleted { path }).await?;
        }
        WatchEvent::DirCreated(path) => {
            transport.send(Message::DirCreated { path }).await?;
        }
        WatchEvent::DirDeleted(path) => {
            if suppress_set.contains_key(&path) {
                return Ok(());
            }
            file_states.retain(|k, _| !k.starts_with(&path));
            transport.send(Message::DirDeleted { path }).await?;
        }
    }
    Ok(())
}
