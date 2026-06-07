use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use beamup_protocol::hash::{hash_content, hash_file};
use beamup_protocol::ignore::IgnoreRules;
use beamup_protocol::messages::{ManifestEntry, Message, PROTOCOL_VERSION};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{debug, info, warn};

#[cfg(target_os = "linux")]
use tracing::error;

use crate::transport::Transport;

const CHUNK_SIZE: usize = 256 * 1024; // 256 KB
const SUPPRESS_DURATION: Duration = Duration::from_millis(200);

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
    let _session_id = match transport.recv().await? {
        Some(Message::Hello { version, session_id }) => {
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
            info!("handshake complete, session: {session_id}");
            session_id
        }
        other => anyhow::bail!("expected Hello, got: {other:?}"),
    };

    // Set up watcher channel
    #[allow(unused_variables)]
    let (watch_tx, mut watch_rx) = mpsc::channel::<WatchEvent>(1024);

    // Start filesystem watcher
    #[cfg(target_os = "linux")]
    {
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

        // Clean expired suppression entries
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
) -> Result<()> {
    match msg {
        Message::Ping => {
            transport.send(Message::Pong).await?;
        }
        Message::Pong => {}
        Message::FileManifest { entries } => {
            handle_manifest(entries, transport, watch_dir, ignore_rules, file_states).await?;
        }
        Message::RequestContent { path } => {
            send_file_content(&path, transport, watch_dir).await?;
        }
        Message::FileContent { path, hash, data } => {
            write_file(&path, &data, hash, watch_dir, file_states, suppress_set).await?;
        }
        Message::FileContentChunk {
            path,
            offset,
            data,
            final_chunk,
        } => {
            append_chunk(&path, offset, &data, final_chunk, watch_dir, file_states, suppress_set)
                .await?;
        }
        Message::FileChanged {
            path,
            hash,
            mtime: _,
            size: _,
        } => {
            let local_state = file_states.get(&path);
            let needs_content = match local_state {
                Some(state) => state.hash != hash,
                None => true,
            };
            if needs_content {
                transport.send(Message::RequestContent { path }).await?;
            }
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
) -> Result<()> {
    let remote_map: HashMap<&str, &ManifestEntry> =
        remote_entries.iter().map(|e| (e.path.as_str(), e)).collect();

    // Build local manifest
    let local_entries = build_manifest(watch_dir, ignore_rules)?;
    let local_map: HashMap<&str, &ManifestEntry> =
        local_entries.iter().map(|e| (e.path.as_str(), e)).collect();

    // Files we have that remote doesn't — send them
    for entry in &local_entries {
        if entry.is_dir {
            continue;
        }
        if !remote_map.contains_key(entry.path.as_str()) {
            send_file_content(&entry.path, transport, watch_dir).await?;
            file_states.insert(
                entry.path.clone(),
                FileState {
                    hash: entry.hash,
                    mtime: entry.mtime,
                    size: entry.size,
                },
            );
        } else {
            let remote = remote_map[entry.path.as_str()];
            if remote.hash != entry.hash {
                send_file_content(&entry.path, transport, watch_dir).await?;
            }
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

    // Files remote has that we don't — request them
    for entry in &remote_entries {
        if entry.is_dir {
            let dir_path = watch_dir.join(&entry.path);
            std::fs::create_dir_all(&dir_path)?;
            continue;
        }
        if !local_map.contains_key(entry.path.as_str()) {
            transport
                .send(Message::RequestContent {
                    path: entry.path.clone(),
                })
                .await?;
        }
    }

    transport.send(Message::ManifestAck).await?;
    info!(
        "manifest sync complete: {} local, {} remote",
        local_entries.len(),
        remote_entries.len()
    );
    Ok(())
}

async fn send_file_content(path: &str, transport: &Transport, watch_dir: &Path) -> Result<()> {
    let full_path = watch_dir.join(path);
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
        transport
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
            transport
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

    debug!("sent: {path} ({len} bytes)");
    Ok(())
}

async fn write_file(
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

    // Atomic write: temp + rename
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

async fn append_chunk(
    path: &str,
    offset: u64,
    data: &[u8],
    final_chunk: bool,
    watch_dir: &Path,
    file_states: &mut HashMap<String, FileState>,
    suppress_set: &mut HashMap<String, Instant>,
) -> Result<()> {
    let full_path = watch_dir.join(path);
    let tmp_path = full_path.with_extension("beamup-chunk-tmp");

    if let Some(parent) = full_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&tmp_path)?;
    use std::io::Seek;
    file.seek(std::io::SeekFrom::Start(offset))?;
    file.write_all(data)?;

    if final_chunk {
        drop(file);
        std::fs::rename(&tmp_path, &full_path)?;
        suppress_set.insert(path.to_string(), Instant::now());

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

        file_states.insert(path.to_string(), FileState { hash, mtime, size });
        debug!("wrote chunked: {path} ({size} bytes)");
    }

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
            let data = match std::fs::read(&full_path) {
                Ok(d) => d,
                Err(_) => return Ok(()),
            };
            let hash = hash_content(&data);

            if let Some(state) = file_states.get(&path) {
                if state.hash == hash {
                    return Ok(());
                }
            }

            let metadata = full_path.metadata()?;
            let mtime = metadata
                .modified()
                .unwrap_or(SystemTime::now())
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let size = metadata.len();

            file_states.insert(path.clone(), FileState { hash, mtime, size });

            transport
                .send(Message::FileChanged {
                    path,
                    hash,
                    mtime,
                    size,
                })
                .await?;
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
