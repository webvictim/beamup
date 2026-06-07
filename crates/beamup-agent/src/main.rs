mod transport;
mod syncer;

#[cfg(target_os = "linux")]
mod watcher;

use std::path::PathBuf;

use anyhow::Result;
use beamup_protocol::compress;
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "beamup-agent", about = "Beamup remote sync agent")]
struct Args {
    /// Run in sync server mode
    #[arg(long)]
    serve: bool,

    /// Directory to watch/sync
    #[arg(long, default_value = "/home/beams/sync")]
    watch_dir: PathBuf,

    /// Decompress an lz4 file: --decompress <input.lz4> <output>
    #[arg(long)]
    decompress: Option<PathBuf>,

    /// Compress a file for chunked transfer: --compress <file>
    /// Outputs the number of chunks to stdout. Creates <file>.beamup-lz4-chunk-NNNN files.
    #[arg(long)]
    compress: Option<PathBuf>,

    /// Output path for decompress mode (positional after --decompress)
    #[arg(index = 1)]
    output: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();

    if let Some(input) = args.decompress {
        // Decompress mode: read lz4 file, decompress, write to output
        let output = args
            .output
            .ok_or_else(|| anyhow::anyhow!("--decompress requires an output path argument"))?;
        let compressed = std::fs::read(&input)?;
        let data = compress::decompress(&compressed)?;
        std::fs::write(&output, &data)?;
        let _ = std::fs::remove_file(&input);
        return Ok(());
    }

    if let Some(input) = args.compress {
        // Compress mode: read file, compress, split into chunks, output count
        let data = std::fs::read(&input)?;

        if (data.len() as u64) <= compress::CHUNKED_THRESHOLD {
            // Not worth chunking — tell caller 0 chunks (use single scp)
            println!("0");
            return Ok(());
        }

        let compressed = compress::compress(&data);
        let chunks = compress::split_chunks(&compressed);
        let num_chunks = chunks.len();

        let base = input.to_string_lossy();
        for (i, chunk) in chunks.into_iter().enumerate() {
            let chunk_path = format!("{base}.beamup-lz4-chunk-{i:04}");
            std::fs::write(&chunk_path, &chunk)?;
        }

        println!("{num_chunks}");
        return Ok(());
    }

    if !args.serve {
        anyhow::bail!("agent must be run with --serve, --decompress, or --compress");
    }

    std::fs::create_dir_all(&args.watch_dir)?;
    syncer::run(args.watch_dir).await
}
