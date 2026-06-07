mod transport;
mod syncer;

#[cfg(target_os = "linux")]
mod watcher;

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "beamup-agent", about = "Beamup remote sync agent")]
struct Args {
    #[arg(long)]
    serve: bool,

    #[arg(long, default_value = "/home/beams/sync")]
    watch_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();

    if !args.serve {
        anyhow::bail!("agent must be run with --serve flag");
    }

    std::fs::create_dir_all(&args.watch_dir)?;
    syncer::run(args.watch_dir).await
}
