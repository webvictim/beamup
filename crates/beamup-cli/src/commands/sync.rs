use std::path::PathBuf;

use anyhow::Result;
use clap::Args;
use tracing::info;

use crate::beam::Beam;
use crate::config::Session;
use crate::syncer::SyncEngine;

#[derive(Args)]
pub struct SyncArgs {
    /// Beam to sync with
    #[arg(short, long)]
    pub beam: Option<String>,

    /// Local directory to sync
    #[arg(short, long)]
    pub path: Option<PathBuf>,

    /// Remote directory to sync into
    #[arg(short, long, default_value = "/home/beams/sync")]
    pub remote_dir: String,

    /// Max concurrent scp transfers
    #[arg(short, long, default_value = "8")]
    pub concurrency: usize,

    /// Chunk size in MB for large file transfers (default: 64)
    #[arg(long, default_value = "64")]
    pub chunk_size: usize,
}

pub async fn run(args: SyncArgs) -> Result<()> {
    let beam_id = if let Some(id) = args.beam {
        id
    } else {
        let session = Session::load()?;
        match session {
            Some(s) => s.beam_id,
            None => anyhow::bail!("no active session — specify --beam"),
        }
    };

    let local_dir = args
        .path
        .unwrap_or_else(|| std::env::current_dir().expect("cannot get current directory"));
    let local_dir = local_dir.canonicalize()?;

    // Verify beam exists
    let beams = Beam::list().await?;
    if !beams.iter().any(|b| b.id == beam_id) {
        anyhow::bail!("beam not found: {beam_id}");
    }

    // Deploy agent (in case it's not running)
    info!("deploying agent...");
    Beam::deploy_agent(&beam_id, args.concurrency).await?;

    let session = Session {
        beam_id: beam_id.clone(),
        local_dir: local_dir.clone(),
        remote_dir: args.remote_dir.clone(),
    };
    session.save()?;

    info!("starting sync: {} ↔ {}:{}", local_dir.display(), beam_id, args.remote_dir);

    let chunk_size_bytes = args.chunk_size * 1024 * 1024;
    let mut engine =
        SyncEngine::new(beam_id, local_dir, args.remote_dir, args.concurrency, chunk_size_bytes).await?;
    engine.run().await
}
