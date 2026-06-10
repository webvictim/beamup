mod beam;
mod commands;
mod config;
mod progress;
mod syncer;
mod transfer;
mod transport;
mod watcher;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use crate::progress::ProgressMakeWriter;

#[derive(Parser)]
#[command(name = "beamup", about = "Bidirectional real-time file sync with Teleport Beams")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(short, long, global = true)]
    verbose: bool,

    #[arg(short, long, global = true)]
    quiet: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a beam and start syncing
    Start(commands::start::StartArgs),
    /// Stop syncing and optionally destroy the beam
    Down(commands::down::DownArgs),
    /// Start syncing with an existing beam
    Sync(commands::sync::SyncArgs),
    /// Show current sync status
    Status(commands::status::StatusArgs),
    /// Run a command in the synced beam
    Exec(commands::exec::ExecArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let filter = if cli.verbose {
        "beamup=debug,beamup_protocol=debug"
    } else if cli.quiet {
        "error"
    } else {
        "beamup=info"
    };

    let log_file_path = match &cli.command {
        Commands::Start(args) if args.console => {
            let path = std::env::temp_dir()
                .join(format!("beamup-sync-{}.log", std::process::id()));
            Some(path)
        }
        _ => None,
    };

    if let Some(ref path) = log_file_path {
        let file = std::fs::File::create(path)?;
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::new(filter))
            .with_writer(file)
            .with_ansi(false)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::new(filter))
            .with_writer(ProgressMakeWriter)
            .init();
    }

    match cli.command {
        Commands::Start(args) => commands::start::run(args, log_file_path).await,
        Commands::Down(args) => commands::down::run(args).await,
        Commands::Sync(args) => commands::sync::run(args).await,
        Commands::Status(args) => commands::status::run(args).await,
        Commands::Exec(args) => commands::exec::run(args).await,
    }
}
