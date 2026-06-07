use std::path::{Path, PathBuf};

use anyhow::Result;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;

use beamup_protocol::ignore::IgnoreRules;

#[derive(Debug)]
pub enum WatchEvent {
    Modified(PathBuf),
    Deleted(PathBuf),
    DirCreated(PathBuf),
    DirDeleted(PathBuf),
}

pub struct FsWatcher {
    _watcher: RecommendedWatcher,
    pub rx: mpsc::Receiver<WatchEvent>,
}

impl FsWatcher {
    pub fn new(root: &Path, _ignore_rules: &IgnoreRules) -> Result<Self> {
        let (tx, rx) = mpsc::channel(4096);
        let _root_clone = root.to_path_buf();

        // We need to clone the root for the closure but can't move ignore_rules
        // so we'll filter in the recv loop instead
        let watcher_tx = tx.clone();
        let mut watcher = notify::recommended_watcher(move |res: notify::Result<Event>| {
            let Ok(event) = res else { return };

            for path in &event.paths {
                let ev = match event.kind {
                    EventKind::Create(notify::event::CreateKind::File) => {
                        Some(WatchEvent::Modified(path.clone()))
                    }
                    EventKind::Create(notify::event::CreateKind::Folder) => {
                        Some(WatchEvent::DirCreated(path.clone()))
                    }
                    EventKind::Modify(_) => {
                        if path.is_dir() {
                            None
                        } else {
                            Some(WatchEvent::Modified(path.clone()))
                        }
                    }
                    EventKind::Remove(notify::event::RemoveKind::File) => {
                        Some(WatchEvent::Deleted(path.clone()))
                    }
                    EventKind::Remove(notify::event::RemoveKind::Folder) => {
                        Some(WatchEvent::DirDeleted(path.clone()))
                    }
                    _ => None,
                };

                if let Some(ev) = ev {
                    let _ = watcher_tx.blocking_send(ev);
                }
            }
        })?;

        watcher.watch(root, RecursiveMode::Recursive)?;

        Ok(Self {
            _watcher: watcher,
            rx,
        })
    }
}
