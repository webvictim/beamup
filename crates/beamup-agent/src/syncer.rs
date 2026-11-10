use std::collections::{HashMap, HashSet};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use beamup_protocol::hash::{hash_content, hash_file};
use beamup_protocol::ignore::IgnoreRules;
use beamup_protocol::messages::{ManifestEntry, Message, SyncDirection, SyncEntry, INLINE_THRESHOLD, PROTOCOL_VERSION};
use tokio::net::{UnixListener, UnixStream};
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

/// A file's hash, valid for as long as its inode, ctime, mtime and size are unchanged.
/// ctime can't be set from userspace, so any write since we hashed it shows up here.
struct CachedHash {
    ino: u64,
    ctime_ns: i128,
    mtime_ns: i128,
    size: u64,
    hash: u64,
}

impl CachedHash {
    fn stamp(metadata: &std::fs::Metadata) -> (u64, i128, i128, u64) {
        let ns = |secs: i64, nsec: i64| secs as i128 * 1_000_000_000 + nsec as i128;
        (
            metadata.ino(),
            ns(metadata.ctime(), metadata.ctime_nsec()),
            ns(metadata.mtime(), metadata.mtime_nsec()),
            metadata.size(),
        )
    }
}

/// File hashes from the previous manifest, so a long-lived agent only re-reads files
/// that changed since its last client connected. Empty for a one-shot stdio agent.
#[derive(Default)]
pub struct HashCache(HashMap<String, CachedHash>);

/// How a session over one client connection ended.
enum SessionEnd {
    /// The client went away.
    Closed,
    /// A newer client connected; it takes over from the current one.
    Superseded(UnixStream),
}

/// Serve a single client over stdin/stdout, as spawned directly by `tsh beams exec`.
pub async fn run(watch_dir: PathBuf) -> Result<()> {
    let mut cache = HashCache::default();
    let mut watch_rx = None;
    run_session(Transport::stdio(), &watch_dir, &mut cache, &mut watch_rx, None).await?;
    Ok(())
}

/// Serve clients on a Unix socket, one at a time, for as long as the process lives.
/// Clients reach it through `--connect`; the newest connection always wins, so a client
/// reconnecting after a dead link isn't stuck behind its own stale session.
pub async fn listen(watch_dir: PathBuf, socket: PathBuf) -> Result<()> {
    let _ = std::fs::remove_file(&socket);
    let listener = UnixListener::bind(&socket)
        .with_context(|| format!("failed to listen on {}", socket.display()))?;
    info!("listening on {}", socket.display());

    let mut cache = HashCache::default();
    let mut watch_rx = start_watcher(&watch_dir);
    let mut next: Option<UnixStream> = None;

    loop {
        let stream = match next.take() {
            Some(stream) => stream,
            None => loop {
                tokio::select! {
                    conn = listener.accept() => match conn {
                        Ok((stream, _)) => break stream,
                        Err(e) => warn!("accept failed: {e}"),
                    },
                    // No client to forward to; the next client's manifest catches it up.
                    _ = recv_watch(&mut watch_rx) => {}
                }
            },
        };

        let (reader, writer) = stream.into_split();
        let transport = Transport::new(reader, writer);
        match run_session(transport, &watch_dir, &mut cache, &mut watch_rx, Some(&listener)).await {
            Ok(SessionEnd::Closed) => info!("client disconnected, waiting for the next one"),
            Ok(SessionEnd::Superseded(stream)) => {
                info!("new client connected, ending the current session");
                next = Some(stream);
            }
            Err(e) => warn!("session ended with error: {e:#}"),
        }
    }
}

async fn run_session(
    mut transport: Transport,
    watch_dir: &Path,
    cache: &mut HashCache,
    watch_rx: &mut Option<mpsc::Receiver<WatchEvent>>,
    listener: Option<&UnixListener>,
) -> Result<SessionEnd> {
    let mut file_states: HashMap<String, FileState> = HashMap::new();
    let mut suppress_set: HashMap<String, Instant> = HashMap::new();
    // Paths the CLI is pushing to us, from the SyncPlan or a FileChanged until its
    // FileReady. Whatever the watcher sees for them meanwhile is our own transfer
    // half-written, and must never be echoed back as the file's content.
    let mut incoming: HashSet<String> = HashSet::new();

    // Wait for Hello
    let (initial_direction, ongoing_direction, ignore_rules) = match transport.recv().await? {
        Some(Message::Hello {
            version,
            session_id,
            initial_direction,
            ongoing_direction,
            excludes,
        }) => {
            if version != PROTOCOL_VERSION {
                anyhow::bail!(
                    "protocol version mismatch: got {version}, expected {PROTOCOL_VERSION}"
                );
            }
            // The CLI checks these before it gets this far, so a failure here
            // means the two sides disagree about the syntax.
            let ignore_rules = IgnoreRules::with_excludes(watch_dir, &excludes)
                .context("client sent unusable exclude patterns")?;
            transport
                .send(Message::HelloAck {
                    version: PROTOCOL_VERSION,
                })
                .await?;
            info!("handshake complete, session: {session_id}, initial: {initial_direction:?}, ongoing: {ongoing_direction:?}, excludes: {excludes:?}");
            (initial_direction, ongoing_direction, ignore_rules)
        }
        other => anyhow::bail!("expected Hello, got: {other:?}"),
    };

    // Start filesystem watcher (only if ongoing sync pulls from beam). A listening
    // agent already has one running from startup.
    if watch_rx.is_none() && ongoing_direction.should_pull() {
        *watch_rx = start_watcher(watch_dir);
    }
    // Anything queued before this client arrived is covered by its manifest exchange.
    if let Some(rx) = watch_rx.as_mut() {
        while rx.try_recv().is_ok() {}
    }

    loop {
        tokio::select! {
            msg = transport.recv() => {
                match msg? {
                    Some(msg) => {
                        handle_message(
                            msg,
                            &transport,
                            watch_dir,
                            &ignore_rules,
                            cache,
                            &mut file_states,
                            &mut suppress_set,
                            &mut incoming,
                            initial_direction,
                        ).await?;
                    }
                    None => {
                        info!("transport closed, ending session");
                        return Ok(SessionEnd::Closed);
                    }
                }
            }
            event = recv_watch(watch_rx) => {
                if let Some(event) = event {
                    if ongoing_direction.should_pull() {
                        handle_watch_event(
                            event,
                            &transport,
                            watch_dir,
                            &ignore_rules,
                            &mut file_states,
                            &suppress_set,
                            &incoming,
                        ).await?;
                    }
                }
            }
            conn = accept(listener) => {
                match conn {
                    Ok(stream) => return Ok(SessionEnd::Superseded(stream)),
                    Err(e) => warn!("accept failed: {e}"),
                }
            }
        }

        suppress_set.retain(|_, expiry| expiry.elapsed() < SUPPRESS_DURATION);
    }
}

fn start_watcher(watch_dir: &Path) -> Option<mpsc::Receiver<WatchEvent>> {
    #[cfg(target_os = "linux")]
    {
        let (watch_tx, watch_rx) = mpsc::channel::<WatchEvent>(1024);
        let dir = watch_dir.to_path_buf();
        let rules = IgnoreRules::load(&dir);
        // A plain thread, not spawn_blocking: runtime shutdown waits for blocking tasks,
        // and this one sits in a blocking inotify read, so the agent could never exit.
        let spawned = std::thread::Builder::new()
            .name("watcher".into())
            .spawn(move || {
                if let Err(e) = crate::watcher::watch(dir, rules, watch_tx) {
                    error!("watcher error: {e}");
                }
            });
        if let Err(e) = spawned {
            error!("failed to start watcher: {e}");
            return None;
        }
        Some(watch_rx)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = watch_dir;
        None
    }
}

/// Next watcher event, or never if there is no watcher. A watcher that died is
/// dropped rather than polled in a loop.
async fn recv_watch(watch_rx: &mut Option<mpsc::Receiver<WatchEvent>>) -> Option<WatchEvent> {
    let Some(rx) = watch_rx.as_mut() else {
        return std::future::pending().await;
    };
    let event = rx.recv().await;
    if event.is_none() {
        *watch_rx = None;
    }
    event
}

async fn accept(listener: Option<&UnixListener>) -> std::io::Result<UnixStream> {
    match listener {
        Some(listener) => listener.accept().await.map(|(stream, _)| stream),
        None => std::future::pending().await,
    }
}

async fn handle_message(
    msg: Message,
    transport: &Transport,
    watch_dir: &Path,
    ignore_rules: &IgnoreRules,
    cache: &mut HashCache,
    file_states: &mut HashMap<String, FileState>,
    suppress_set: &mut HashMap<String, Instant>,
    incoming: &mut HashSet<String>,
    initial_direction: SyncDirection,
) -> Result<()> {
    match msg {
        Message::Ping => {
            transport.send(Message::Pong).await?;
        }
        Message::Pong => {}
        Message::FileManifest { entries } => {
            handle_manifest(entries, transport, watch_dir, ignore_rules, cache, file_states, incoming, initial_direction).await?;
        }
        Message::FileContent { path, hash, data } => {
            write_file(&path, &data, hash, watch_dir, file_states, suppress_set)?;
        }
        Message::FileReady {
            path,
            hash,
            size,
        } => {
            // CLI has finished pushing a file (it also says so when the push failed) —
            // record what actually landed, so later watcher events for it dedupe by hash
            incoming.remove(&path);
            suppress_set.insert(path.clone(), Instant::now());
            let full_path = watch_dir.join(&path);
            let actual_hash = if full_path.exists() {
                hash_file(&full_path).unwrap_or(0)
            } else {
                0
            };
            let metadata = full_path.metadata().ok();
            let mtime = metadata
                .as_ref()
                .and_then(|m| m.modified().ok())
                .unwrap_or(SystemTime::now())
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            file_states.insert(
                path.clone(),
                FileState {
                    hash: actual_hash,
                    mtime,
                    size: metadata.map_or(size, |m| m.len()),
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
            // CLI is about to push this file via scp — hold off reporting it until
            // FileReady. A timed suppression alone expires mid-transfer on big files.
            incoming.insert(path.clone());
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
    cache: &mut HashCache,
    file_states: &mut HashMap<String, FileState>,
    incoming: &mut HashSet<String>,
    initial_direction: SyncDirection,
) -> Result<()> {
    let remote_map: HashMap<&str, &ManifestEntry> =
        remote_entries.iter().map(|e| (e.path.as_str(), e)).collect();

    // Build local manifest
    let local_entries = build_manifest(watch_dir, ignore_rules, cache)?;
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

    // The CLI is about to tar/scp these in; they're ours until it sends FileReady.
    incoming.extend(to_push.iter().map(|e| e.path.clone()));

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

pub fn build_manifest(
    root: &Path,
    ignore_rules: &IgnoreRules,
    cache: &mut HashCache,
) -> Result<Vec<ManifestEntry>> {
    let mut entries = Vec::new();
    // Rebuilt from scratch each time so deleted files drop out of the cache.
    let mut fresh = HashCache::default();
    walk_dir(root, root, ignore_rules, cache, &mut fresh, &mut entries)?;
    *cache = fresh;
    Ok(entries)
}

fn walk_dir(
    base: &Path,
    dir: &Path,
    ignore_rules: &IgnoreRules,
    cache: &mut HashCache,
    fresh: &mut HashCache,
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
            walk_dir(base, &path, ignore_rules, cache, fresh, entries)?;
        } else {
            let metadata = path.metadata()?;
            let mtime = metadata
                .modified()
                .unwrap_or(SystemTime::now())
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let size = metadata.len();

            let stamp = CachedHash::stamp(&metadata);
            let hash = match cache.0.remove(&relative) {
                Some(c) if (c.ino, c.ctime_ns, c.mtime_ns, c.size) == stamp => Some(c.hash),
                _ => hash_file(&path).ok(),
            };
            // An unreadable file reports hash 0, as before, but isn't cached.
            if let Some(hash) = hash {
                let (ino, ctime_ns, mtime_ns, size) = stamp;
                fresh.0.insert(
                    relative.clone(),
                    CachedHash { ino, ctime_ns, mtime_ns, size, hash },
                );
            }
            let hash = hash.unwrap_or(0);

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
    ignore_rules: &IgnoreRules,
    file_states: &mut HashMap<String, FileState>,
    suppress_set: &HashMap<String, Instant>,
    incoming: &HashSet<String>,
) -> Result<()> {
    // The watcher thread only knows the base rules: a persistent agent starts it
    // before any client, and so before any client's --exclude patterns.
    let (path, is_dir) = match &event {
        WatchEvent::Modified(p) | WatchEvent::Deleted(p) => (p, false),
        WatchEvent::DirCreated(p) | WatchEvent::DirDeleted(p) => (p, true),
    };
    if ignore_rules.is_ignored(Path::new(path), is_dir) {
        return Ok(());
    }
    match event {
        WatchEvent::Modified(path) => {
            if suppress_set.contains_key(&path) || incoming.contains(&path) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use beamup_protocol::codec::MessageCodec;
    use futures_util::{SinkExt, StreamExt};
    use tokio_util::codec::Framed;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("beamup-agent-test-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn manifest_hash(entries: &[ManifestEntry], path: &str) -> u64 {
        entries.iter().find(|e| e.path == path).unwrap().hash
    }

    #[test]
    fn manifest_reuses_cached_hash_for_untouched_file() {
        let dir = temp_dir("cache-hit");
        std::fs::write(dir.join("a.txt"), b"hello").unwrap();
        let rules = IgnoreRules::load(&dir);
        let mut cache = HashCache::default();

        let first = build_manifest(&dir, &rules, &mut cache).unwrap();
        assert_eq!(manifest_hash(&first, "a.txt"), hash_content(b"hello"));

        // Plant a sentinel: if the file is re-read, the real hash comes back instead.
        cache.0.get_mut("a.txt").unwrap().hash = 42;
        let second = build_manifest(&dir, &rules, &mut cache).unwrap();
        assert_eq!(manifest_hash(&second, "a.txt"), 42);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn manifest_rehashes_rewrite_with_same_size_and_mtime() {
        let dir = temp_dir("cache-miss");
        let path = dir.join("a.txt");
        std::fs::write(&path, b"aaaa").unwrap();
        let mtime = std::fs::metadata(&path).unwrap().modified().unwrap();
        let rules = IgnoreRules::load(&dir);
        let mut cache = HashCache::default();
        build_manifest(&dir, &rules, &mut cache).unwrap();

        // Same size, mtime put back: only ctime gives the change away.
        std::thread::sleep(Duration::from_millis(10));
        std::fs::write(&path, b"bbbb").unwrap();
        std::fs::File::options().write(true).open(&path).unwrap().set_modified(mtime).unwrap();

        let entries = build_manifest(&dir, &rules, &mut cache).unwrap();
        assert_eq!(manifest_hash(&entries, "a.txt"), hash_content(b"bbbb"));

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn manifest_drops_deleted_files_from_cache() {
        let dir = temp_dir("cache-delete");
        std::fs::write(dir.join("a.txt"), b"x").unwrap();
        let rules = IgnoreRules::load(&dir);
        let mut cache = HashCache::default();
        build_manifest(&dir, &rules, &mut cache).unwrap();
        assert!(cache.0.contains_key("a.txt"));

        std::fs::remove_file(dir.join("a.txt")).unwrap();
        build_manifest(&dir, &rules, &mut cache).unwrap();
        assert!(cache.0.is_empty());

        std::fs::remove_dir_all(&dir).unwrap();
    }

    async fn hello(conn: &mut Framed<UnixStream, MessageCodec>, session_id: &str) {
        hello_with(conn, session_id, vec![]).await
    }

    async fn hello_with(conn: &mut Framed<UnixStream, MessageCodec>, session_id: &str, excludes: Vec<String>) {
        conn.send(Message::Hello {
            version: PROTOCOL_VERSION,
            session_id: session_id.into(),
            initial_direction: SyncDirection::Bidirectional,
            ongoing_direction: SyncDirection::LocalToBeam,
            excludes,
        })
        .await
        .unwrap();
        match conn.next().await {
            Some(Ok(Message::HelloAck { version })) => assert_eq!(version, PROTOCOL_VERSION),
            other => panic!("expected HelloAck, got {other:?}"),
        }
    }

    /// Everything the agent sends within `window`.
    async fn drain(conn: &mut Framed<UnixStream, MessageCodec>, window: Duration) -> Vec<Message> {
        let mut msgs = Vec::new();
        let deadline = tokio::time::Instant::now() + window;
        while let Ok(Some(Ok(msg))) = tokio::time::timeout_at(deadline, conn.next()).await {
            msgs.push(msg);
        }
        msgs
    }

    fn mentions(msg: &Message, path: &str) -> bool {
        matches!(msg, Message::FileContent { path: p, .. } | Message::FileChanged { path: p, .. } if p == path)
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn pushed_file_is_not_echoed_back_while_it_is_being_written() {
        use std::io::Write;

        let dir = temp_dir("no-echo");
        let watch_dir = dir.join("sync");
        std::fs::create_dir_all(&watch_dir).unwrap();
        let socket = dir.join("agent.sock");
        tokio::spawn(listen(watch_dir.clone(), socket.clone()));

        let mut conn = loop {
            if let Ok(s) = UnixStream::connect(&socket).await {
                break Framed::new(s, MessageCodec);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        conn.send(Message::Hello {
            version: PROTOCOL_VERSION,
            session_id: "push".into(),
            initial_direction: SyncDirection::Bidirectional,
            ongoing_direction: SyncDirection::Bidirectional,
            excludes: vec![],
        })
        .await
        .unwrap();
        assert!(matches!(conn.next().await, Some(Ok(Message::HelloAck { .. }))));

        // The CLI has a 2 MB file the beam lacks, so the plan says push it.
        let full = vec![7u8; 2_000_000];
        conn.send(Message::FileManifest {
            entries: vec![ManifestEntry {
                path: "big.bin".into(),
                hash: hash_content(&full),
                mtime: 0,
                size: full.len() as u64,
                is_dir: false,
            }],
        })
        .await
        .unwrap();
        match conn.next().await {
            Some(Ok(Message::SyncPlan { to_push, .. })) => assert_eq!(to_push.len(), 1),
            other => panic!("expected SyncPlan, got {other:?}"),
        }

        // The push lands in pieces, as tar or scp would write it. While it's still
        // under the inline threshold the old agent sent these partials back as
        // FileContent, and the CLI wrote them over the full local file.
        let mut f = std::fs::File::create(watch_dir.join("big.bin")).unwrap();
        for chunk in full.chunks(16 * 1024).take(4) {
            f.write_all(chunk).unwrap();
            f.flush().unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        for chunk in full.chunks(16 * 1024).skip(4) {
            f.write_all(chunk).unwrap();
        }
        drop(f);
        let echoed: Vec<_> = drain(&mut conn, Duration::from_millis(800))
            .await
            .into_iter()
            .filter(|m| mentions(m, "big.bin"))
            .collect();
        assert!(echoed.is_empty(), "agent echoed our own push back: {echoed:?}");

        // Once the CLI says the push is done, a real edit on the beam is reported.
        conn.send(Message::FileReady {
            path: "big.bin".into(),
            hash: hash_content(&full),
            size: full.len() as u64,
        })
        .await
        .unwrap();
        tokio::time::sleep(SUPPRESS_DURATION + Duration::from_millis(100)).await;
        std::fs::write(watch_dir.join("big.bin"), b"edited on the beam").unwrap();
        let reported = drain(&mut conn, Duration::from_secs(2)).await;
        assert!(
            reported.iter().any(|m| matches!(
                m,
                Message::FileContent { path, data, .. } if path == "big.bin" && data == b"edited on the beam"
            )),
            "beam-side edit after FileReady was not reported: {reported:?}"
        );

        std::fs::remove_dir_all(&dir).unwrap();
    }

    fn manifest_paths(entries: &[ManifestEntry]) -> Vec<&str> {
        let mut paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
        paths.sort();
        paths
    }

    /// A beam's home directory: a project, its tsh profile and kubeconfig, a cache,
    /// and some build output.
    fn home_like(name: &str) -> PathBuf {
        let dir = temp_dir(name);
        std::fs::create_dir_all(dir.join(".tsh/keys/odd-firefly.beams.sh")).unwrap();
        std::fs::write(dir.join(".tsh/keys/odd-firefly.beams.sh/bot-beam-x-x509.pem"), b"cert").unwrap();
        std::fs::write(dir.join(".tsh/odd-firefly.beams.sh.yaml"), b"profile").unwrap();
        std::fs::create_dir_all(dir.join(".kube")).unwrap();
        std::fs::write(dir.join(".kube/config"), b"kubeconfig").unwrap();
        std::fs::create_dir_all(dir.join(".cache/pip")).unwrap();
        std::fs::write(dir.join(".cache/pip/wheel"), b"wheel").unwrap();
        std::fs::create_dir_all(dir.join("project/build")).unwrap();
        std::fs::write(dir.join("project/main.rs"), b"fn main() {}").unwrap();
        std::fs::write(dir.join("project/build/out.o"), b"obj").unwrap();
        std::fs::write(dir.join("notes.log"), b"log").unwrap();
        dir
    }

    #[test]
    fn manifest_leaves_out_tsh_kube_and_cache() {
        let dir = home_like("manifest-tsh");
        let rules = IgnoreRules::load(&dir);
        let entries = build_manifest(&dir, &rules, &mut HashCache::default()).unwrap();

        assert_eq!(
            manifest_paths(&entries),
            vec!["notes.log", "project", "project/build", "project/build/out.o", "project/main.rs"]
        );

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn manifest_leaves_out_excludes() {
        let dir = home_like("manifest-excludes");
        let excludes = vec!["build/".to_string(), "*.log".to_string()];
        let rules = IgnoreRules::with_excludes(&dir, &excludes).unwrap();
        let entries = build_manifest(&dir, &rules, &mut HashCache::default()).unwrap();

        assert_eq!(manifest_paths(&entries), vec!["project", "project/main.rs"]);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Excludes arrive in Hello: the agent must not offer excluded files for pulling,
    /// and must still leave out ~/.tsh, ~/.kube and ~/.cache with none given.
    #[tokio::test]
    async fn session_applies_excludes_from_hello() {
        let dir = temp_dir("session-excludes");
        let watch_dir = home_like("session-excludes-home");
        let socket = dir.join("agent.sock");
        tokio::spawn(listen(watch_dir.clone(), socket.clone()));

        let connect = || async {
            for _ in 0..50 {
                if let Ok(s) = UnixStream::connect(&socket).await {
                    return Framed::new(s, MessageCodec);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            panic!("agent never started listening");
        };
        async fn plan_pulls(conn: &mut Framed<UnixStream, MessageCodec>) -> Vec<String> {
            conn.send(Message::FileManifest { entries: vec![] }).await.unwrap();
            match conn.next().await {
                Some(Ok(Message::SyncPlan { to_pull, .. })) => {
                    let mut paths: Vec<String> = to_pull.into_iter().map(|e| e.path).collect();
                    paths.sort();
                    paths
                }
                other => panic!("expected SyncPlan, got {other:?}"),
            }
        }

        let mut conn = connect().await;
        hello_with(&mut conn, "excludes", vec!["build/".into(), "*.log".into()]).await;
        assert_eq!(plan_pulls(&mut conn).await, vec!["project/main.rs"]);

        // Excludes belong to the session: the next client on the same agent sends none.
        let mut conn = connect().await;
        hello(&mut conn, "none").await;
        assert_eq!(
            plan_pulls(&mut conn).await,
            vec!["notes.log", "project/build/out.o", "project/main.rs"]
        );

        std::fs::remove_dir_all(&dir).unwrap();
        std::fs::remove_dir_all(&watch_dir).unwrap();
    }

    #[tokio::test]
    async fn session_rejects_unusable_excludes() {
        let dir = temp_dir("session-bad-excludes");
        let watch_dir = dir.join("sync");
        std::fs::create_dir_all(&watch_dir).unwrap();
        let (client, server) = UnixStream::pair().unwrap();
        let (reader, writer) = server.into_split();
        let session = tokio::spawn({
            let watch_dir = watch_dir.clone();
            async move {
                let mut cache = HashCache::default();
                let mut watch_rx = None;
                run_session(Transport::new(reader, writer), &watch_dir, &mut cache, &mut watch_rx, None).await
            }
        });

        let mut conn = Framed::new(client, MessageCodec);
        conn.send(Message::Hello {
            version: PROTOCOL_VERSION,
            session_id: "bad".into(),
            initial_direction: SyncDirection::Bidirectional,
            ongoing_direction: SyncDirection::LocalToBeam,
            excludes: vec!["a[z-a]".into()],
        })
        .await
        .unwrap();

        let err = match session.await.unwrap() {
            Ok(_) => panic!("session accepted an invalid exclude pattern"),
            Err(e) => e,
        };
        assert!(format!("{err:#}").contains("exclude"), "unexpected error: {err:#}");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The watcher thread runs with base rules only, so --exclude has to be applied
    /// to its events. Built-ins and excludes alike must never reach the CLI.
    #[tokio::test]
    async fn watch_events_for_ignored_paths_are_dropped() {
        let watch_dir = home_like("watch-excludes");
        let rules = IgnoreRules::with_excludes(&watch_dir, &["build/".to_string()]).unwrap();
        let (client, server) = UnixStream::pair().unwrap();
        let (reader, writer) = server.into_split();
        let transport = Transport::new(reader, writer);
        let mut conn = Framed::new(client, MessageCodec);
        let mut file_states = HashMap::new();
        let suppress = HashMap::new();
        let incoming = HashSet::new();

        for event in [
            WatchEvent::Modified("project/build/out.o".into()),
            WatchEvent::Deleted("project/build/out.o".into()),
            WatchEvent::DirCreated("project/build".into()),
            WatchEvent::DirDeleted("project/build".into()),
            WatchEvent::Modified(".tsh/odd-firefly.beams.sh.yaml".into()),
            WatchEvent::DirCreated(".tsh/keys".into()),
            // Not ignored: this one, and only this one, must come through.
            WatchEvent::Modified("project/main.rs".into()),
        ] {
            handle_watch_event(event, &transport, &watch_dir, &rules, &mut file_states, &suppress, &incoming)
                .await
                .unwrap();
        }

        let sent = drain(&mut conn, Duration::from_millis(300)).await;
        assert_eq!(sent.len(), 1, "expected only project/main.rs, got {sent:?}");
        assert!(mentions(&sent[0], "project/main.rs"), "unexpected message: {:?}", sent[0]);

        std::fs::remove_dir_all(&watch_dir).unwrap();
    }

    #[tokio::test]
    async fn listen_serves_reconnects_and_newest_client_wins() {
        let dir = temp_dir("listen");
        let watch_dir = dir.join("sync");
        std::fs::create_dir_all(&watch_dir).unwrap();
        let socket = dir.join("agent.sock");
        tokio::spawn(listen(watch_dir, socket.clone()));

        let connect = || async {
            for _ in 0..50 {
                if let Ok(s) = UnixStream::connect(&socket).await {
                    return Framed::new(s, MessageCodec);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            panic!("agent never started listening");
        };

        // A client can connect, finish, and a later one is served by the same agent.
        let mut first = connect().await;
        hello(&mut first, "one").await;
        drop(first);
        let mut second = connect().await;
        hello(&mut second, "two").await;

        // A third client takes over; the second sees its connection close.
        let mut third = connect().await;
        hello(&mut third, "three").await;
        let closed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match second.next().await {
                    None | Some(Err(_)) => break,
                    Some(Ok(_)) => continue,
                }
            }
        })
        .await;
        assert!(closed.is_ok(), "superseded client was not disconnected");

        third.send(Message::Ping).await.unwrap();
        assert!(matches!(third.next().await, Some(Ok(Message::Pong))));

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
