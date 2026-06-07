mod beam;
mod commands;
mod config;
mod syncer;
mod transport;
mod watcher;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

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

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(filter))
        .with_writer(std::io::stderr)
        .init();

    match cli.command {
        Commands::Start(args) => commands::start::run(args).await,
        Commands::Down(args) => commands::down::run(args).await,
        Commands::Sync(args) => commands::sync::run(args).await,
        Commands::Status(args) => commands::status::run(args).await,
        Commands::Exec(args) => commands::exec::run(args).await,
    }
}
