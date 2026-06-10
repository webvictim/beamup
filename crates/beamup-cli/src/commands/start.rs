use std::path::PathBuf;

use anyhow::Result;
use clap::Args;
use tokio::sync::oneshot;
use tracing::info;

use crate::beam::Beam;
use crate::commands::CliSyncDirection;
use crate::config::Session;
use crate::syncer::SyncEngine;

#[derive(Args)]
pub struct StartArgs {
    /// Local directory to sync (default: current directory)
    #[arg(long)]
    pub local_path: Option<PathBuf>,

    /// Use an existing beam instead of creating one
    #[arg(short, long)]
    pub beam: Option<String>,

    /// Remote directory to sync into
    #[arg(long, default_value = "/home/beams/sync")]
    pub remote_path: String,

    /// Skip initial sync
    #[arg(long)]
    pub no_initial_sync: bool,

    /// Max concurrent scp transfers
    #[arg(short, long, default_value = "8")]
    pub concurrency: usize,

    /// Additional exclude patterns
    #[arg(long)]
    pub exclude: Vec<String>,

    /// Chunk size in MB for large file transfers (default: 64)
    #[arg(long, default_value = "64")]
    pub chunk_size: usize,

    /// Drop into a console on the beam after initial sync
    #[arg(long)]
    pub console: bool,

    /// Direction for initial sync
    #[arg(long, value_enum, default_value = "bidirectional")]
    pub initial_sync: CliSyncDirection,

    /// Direction for ongoing sync
    #[arg(long, value_enum, default_value = "bidirectional")]
    pub ongoing_sync: CliSyncDirection,
}

pub async fn run(args: StartArgs, log_file: Option<PathBuf>) -> Result<()> {
    let local_dir = args
        .local_path
        .unwrap_or_else(|| std::env::current_dir().expect("cannot get current directory"));
    let local_dir = local_dir.canonicalize()?;

    if !local_dir.is_dir() {
        anyhow::bail!("path is not a directory: {}", local_dir.display());
    }

    // Create or use existing beam
    let beam_id = if let Some(id) = args.beam {
        info!("using existing beam: {id}");
        let beams = Beam::list().await?;
        if !beams.iter().any(|b| b.id == id) {
            anyhow::bail!("beam not found: {id}");
        }
        id
    } else {
        info!("creating beam...");
        let beam = Beam::create().await?;
        info!("beam created: {}", beam.id);
        beam.id
    };

    // Save session
    let session = Session {
        beam_id: beam_id.clone(),
        local_dir: local_dir.clone(),
        remote_dir: args.remote_path.clone(),
    };
    session.save()?;

    info!("deploying agent to beam...");
    Beam::deploy_agent(&beam_id, args.concurrency).await?;

    let chunk_size_bytes = args.chunk_size * 1024 * 1024;

    info!("starting sync: {} ↔ {}:{}", local_dir.display(), beam_id, args.remote_path);

    let mut engine = SyncEngine::new(
        beam_id.clone(),
        local_dir,
        args.remote_path,
        args.concurrency,
        chunk_size_bytes,
        args.initial_sync.into(),
        args.ongoing_sync.into(),
    )
    .await?;

    if !args.console {
        return engine.run(None).await;
    }

    // Console mode: run sync in background, launch console after initial sync completes
    let (sync_done_tx, sync_done_rx) = oneshot::channel::<()>();

    let sync_handle = tokio::spawn(async move {
        engine.run(Some(sync_done_tx)).await
    });

    let _ = sync_done_rx.await;

    if let Some(ref path) = log_file {
        eprintln!("Sync running in background. Logs: {}", path.display());
    }

    let status = Beam::console(&beam_id).await?;

    sync_handle.abort();
    std::process::exit(status.code().unwrap_or(0));
}
