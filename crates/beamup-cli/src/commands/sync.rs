use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use beamup_protocol::ignore::IgnoreRules;
use clap::Args;
use tracing::info;

use crate::beam::Beam;
use crate::commands::CliSyncDirection;
use crate::config::Session;
use crate::paths;
use crate::syncer::SyncEngine;

#[derive(Args)]
pub struct SyncArgs {
    /// Beam to sync with
    #[arg(short, long)]
    pub beam: Option<String>,

    /// Local directory to sync
    #[arg(long)]
    pub local_path: Option<PathBuf>,

    /// Remote directory to sync into (`~` and relative paths resolve against
    /// the beam's home directory)
    #[arg(long, default_value = "/home/beams/sync")]
    pub remote_path: String,

    /// Max concurrent scp transfers
    #[arg(short, long, default_value = "8")]
    pub concurrency: usize,

    /// Leave matching paths out of the sync, in both directions (gitignore syntax,
    /// relative to the synced directory; repeatable). `.tsh/`, `.kube/` and `.cache/` are always excluded.
    /// Omit to reuse the active session's patterns when syncing the same
    /// directories with the same beam.
    #[arg(long, value_name = "PATTERN")]
    pub exclude: Vec<String>,

    /// Chunk size in MB for large file transfers (default: 64)
    #[arg(long, default_value = "64")]
    pub chunk_size: usize,

    /// Direction for initial sync
    #[arg(long, value_enum, default_value = "bidirectional")]
    pub initial_sync: CliSyncDirection,

    /// Direction for ongoing sync
    #[arg(long, value_enum, default_value = "bidirectional", conflicts_with = "oneshot")]
    pub ongoing_sync: CliSyncDirection,

    /// Exit after initial sync, printing the beam name to stdout
    #[arg(long)]
    pub oneshot: bool,
}

pub async fn run(args: SyncArgs) -> Result<()> {
    let saved = Session::load()?;
    let beam_id = match (args.beam, &saved) {
        (Some(id), _) => id,
        (None, Some(s)) => s.beam_id.clone(),
        (None, None) => anyhow::bail!("no active session — specify --beam"),
    };

    let local_dir = args
        .local_path
        .unwrap_or_else(|| std::env::current_dir().expect("cannot get current directory"));
    let local_dir = paths::expand_local(&local_dir)?;
    let local_dir = local_dir
        .canonicalize()
        .with_context(|| format!("cannot resolve --local-path {}", local_dir.display()))?;
    let remote_dir = paths::expand_remote(&args.remote_path)?;

    let excludes = resolve_excludes(args.exclude, saved.as_ref(), &beam_id, &local_dir, &remote_dir);
    IgnoreRules::with_excludes(&local_dir, &excludes)?;
    if !excludes.is_empty() {
        info!("excluding: {}", excludes.join(", "));
    }

    // Verify beam exists
    let beams = Beam::list().await?;
    if !beams.iter().any(|b| b.id == beam_id) {
        anyhow::bail!("beam not found: {beam_id}");
    }

    // Deploy agent (in case it's not running)
    info!("deploying agent...");
    let agent_mode = Beam::deploy_agent(&beam_id, &remote_dir).await?;

    let session = Session {
        beam_id: beam_id.clone(),
        local_dir: local_dir.clone(),
        remote_dir: remote_dir.clone(),
        agent_service: agent_mode.service_name().map(str::to_string),
        excludes: excludes.clone(),
    };
    session.save()?;

    info!("starting sync: {} ↔ {}:{}", local_dir.display(), beam_id, remote_dir);

    let chunk_size_bytes = args.chunk_size * 1024 * 1024;
    let mut engine = SyncEngine::new(
        beam_id.clone(),
        local_dir,
        remote_dir,
        args.concurrency,
        chunk_size_bytes,
        args.initial_sync.into(),
        args.ongoing_sync.into(),
        excludes,
        &agent_mode,
    )
    .await?;

    if args.oneshot {
        engine.run_initial_sync_only().await?;
        println!("{beam_id}");
        return Ok(());
    }

    engine.run(None).await
}

/// `--exclude` patterns given on the command line win. Without any, reuse the saved
/// session's, but only for the same beam and directories: they're relative to the
/// synced directory, so they mean something else anywhere else.
fn resolve_excludes(
    given: Vec<String>,
    saved: Option<&Session>,
    beam_id: &str,
    local_dir: &Path,
    remote_dir: &str,
) -> Vec<String> {
    if !given.is_empty() {
        return given;
    }
    match saved {
        Some(s) if s.beam_id == beam_id && s.local_dir == local_dir && s.remote_dir == remote_dir => {
            s.excludes.clone()
        }
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(excludes: &[&str]) -> Session {
        Session {
            beam_id: "beam-1".into(),
            local_dir: PathBuf::from("/Users/me/project"),
            remote_dir: "/home/beams/sync".into(),
            agent_service: None,
            excludes: excludes.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn resolve(given: &[&str], saved: Option<&Session>, beam: &str, local: &str, remote: &str) -> Vec<String> {
        let given = given.iter().map(|s| s.to_string()).collect();
        resolve_excludes(given, saved, beam, Path::new(local), remote)
    }

    #[test]
    fn reuses_saved_excludes_for_the_same_session() {
        let saved = session(&["*.sqlite", "build/"]);
        assert_eq!(
            resolve(&[], Some(&saved), "beam-1", "/Users/me/project", "/home/beams/sync"),
            vec!["*.sqlite", "build/"]
        );
    }

    #[test]
    fn given_excludes_replace_saved_ones() {
        let saved = session(&["*.sqlite"]);
        assert_eq!(
            resolve(&["*.log"], Some(&saved), "beam-1", "/Users/me/project", "/home/beams/sync"),
            vec!["*.log"]
        );
    }

    #[test]
    fn saved_excludes_are_not_carried_to_another_session() {
        let saved = session(&["*.sqlite"]);
        let none: Vec<String> = Vec::new();
        assert_eq!(resolve(&[], Some(&saved), "beam-2", "/Users/me/project", "/home/beams/sync"), none);
        assert_eq!(resolve(&[], Some(&saved), "beam-1", "/Users/me/other", "/home/beams/sync"), none);
        assert_eq!(resolve(&[], Some(&saved), "beam-1", "/Users/me/project", "/home/beams"), none);
        assert_eq!(resolve(&[], None, "beam-1", "/Users/me/project", "/home/beams/sync"), none);
    }

    #[test]
    fn sessions_saved_before_excludes_existed_still_load() {
        let json = r#"{"beam_id":"b","local_dir":"/x","remote_dir":"/y","agent_service":null}"#;
        let s: Session = serde_json::from_str(json).unwrap();
        assert!(s.excludes.is_empty());
    }
}
