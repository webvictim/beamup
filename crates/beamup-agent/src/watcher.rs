#[cfg(target_os = "linux")]
use std::collections::HashMap;
#[cfg(target_os = "linux")]
use std::path::{Path, PathBuf};

#[cfg(target_os = "linux")]
use anyhow::Result;
#[cfg(target_os = "linux")]
use beamup_protocol::ignore::IgnoreRules;
#[cfg(target_os = "linux")]
use inotify::{EventMask, Inotify, WatchDescriptor, WatchMask};
#[cfg(target_os = "linux")]
use tokio::sync::mpsc;
#[cfg(target_os = "linux")]
use tracing::{debug, warn};

#[cfg(target_os = "linux")]
use crate::syncer::WatchEvent;

#[cfg(target_os = "linux")]
pub fn watch(
    root: PathBuf,
    ignore_rules: IgnoreRules,
    tx: mpsc::Sender<WatchEvent>,
) -> Result<()> {
    let mut inotify = Inotify::init()?;
    let mut watches: HashMap<WatchDescriptor, PathBuf> = HashMap::new();

    let mask = WatchMask::CREATE
        | WatchMask::MODIFY
        | WatchMask::DELETE
        | WatchMask::MOVED_FROM
        | WatchMask::MOVED_TO
        | WatchMask::CLOSE_WRITE;

    // Recursively add watches
    add_watches_recursive(&root, &root, &ignore_rules, &mut inotify, &mut watches, mask)?;

    let mut buffer = vec![0u8; 4096];

    loop {
        let events = inotify.read_events_blocking(&mut buffer)?;

        for event in events {
            let Some(dir_path) = watches.get(&event.wd) else {
                continue;
            };

            let Some(name) = event.name else {
                continue;
            };

            let full_path = dir_path.join(name);
            let relative = full_path
                .strip_prefix(&root)
                .unwrap_or(&full_path)
                .to_string_lossy()
                .to_string();

            let is_dir = event.mask.contains(EventMask::ISDIR);

            if ignore_rules.filter_path(&root, &full_path, is_dir) {
                continue;
            }

            if event.mask.contains(EventMask::CREATE) && is_dir {
                // New directory — add watch and notify
                if let Ok(wd) = inotify.watches().add(&full_path, mask) {
                    watches.insert(wd, full_path.clone());
                }
                let _ = tx.blocking_send(WatchEvent::DirCreated(relative));
            } else if event.mask.contains(EventMask::CLOSE_WRITE)
                || event.mask.contains(EventMask::MODIFY)
            {
                if !is_dir {
                    let _ = tx.blocking_send(WatchEvent::Modified(relative));
                }
            } else if event.mask.contains(EventMask::DELETE)
                || event.mask.contains(EventMask::MOVED_FROM)
            {
                if is_dir {
                    let _ = tx.blocking_send(WatchEvent::DirDeleted(relative));
                    // Remove watch for this directory
                    watches.retain(|_, p| !p.starts_with(&full_path));
                } else {
                    let _ = tx.blocking_send(WatchEvent::Deleted(relative));
                }
            } else if event.mask.contains(EventMask::MOVED_TO) {
                if is_dir {
                    if let Ok(wd) = inotify.watches().add(&full_path, mask) {
                        watches.insert(wd, full_path.clone());
                    }
                    let _ = tx.blocking_send(WatchEvent::DirCreated(relative));
                } else {
                    let _ = tx.blocking_send(WatchEvent::Modified(relative));
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn add_watches_recursive(
    base: &Path,
    dir: &Path,
    ignore_rules: &IgnoreRules,
    inotify: &mut Inotify,
    watches: &mut HashMap<WatchDescriptor, PathBuf>,
    mask: WatchMask,
) -> Result<()> {
    if ignore_rules.filter_path(base, dir, true) {
        return Ok(());
    }

    let wd = inotify.watches().add(dir, mask)?;
    watches.insert(wd, dir.to_path_buf());

    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return Ok(()),
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            add_watches_recursive(base, &path, ignore_rules, inotify, watches, mask)?;
        }
    }

    Ok(())
}
