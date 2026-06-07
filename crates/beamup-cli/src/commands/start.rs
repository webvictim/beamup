use std::path::PathBuf;

use anyhow::Result;
use clap::Args;
use tracing::info;

use crate::beam::Beam;
use crate::config::Session;
use crate::syncer::SyncEngine;

#[derive(Args)]
pub struct StartArgs {
    /// Local directory to sync (default: current directory)
    #[arg(short, long)]
    pub path: Option<PathBuf>,

    /// Use an existing beam instead of creating one
    #[arg(short, long)]
    pub beam: Option<String>,

    /// Remote directory to sync into
    #[arg(short, long, default_value = "/home/beams/sync")]
    pub remote_dir: String,

    /// Skip initial sync
    #[arg(long)]
    pub no_initial_sync: bool,

    /// Max concurrent scp transfers
    #[arg(short, long, default_value = "8")]
    pub concurrency: usize,

    /// Additional exclude patterns
    #[arg(long)]
    pub exclude: Vec<String>,
}

pub async fn run(args: StartArgs) -> Result<()> {
    let local_dir = args
        .path
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
        remote_dir: args.remote_dir.clone(),
    };
    session.save()?;

    info!("deploying agent to beam...");
    Beam::deploy_agent(&beam_id, args.concurrency).await?;

    info!("starting sync: {} ↔ {}:{}", local_dir.display(), beam_id, args.remote_dir);

    let mut engine =
        SyncEngine::new(beam_id, local_dir, args.remote_dir, args.concurrency).await?;
    engine.run().await
}
