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

    /// Compress a file for chunked transfer: --compress <file> <file_id>
    /// Writes chunks to /tmp/beamup-xfer/<file_id>/chunk-NNNN
    /// Outputs the number of chunks to stdout.
    #[arg(long)]
    compress: Option<PathBuf>,

    /// Decompress chunked files: --decompress-chunks <output-path> <num-chunks> <file_id>
    /// Reads from /tmp/beamup-xfer/<file_id>/chunk-NNNN, writes concatenated output.
    #[arg(long)]
    decompress_chunks: Option<PathBuf>,

    /// Chunk size in bytes for compression
    #[arg(long)]
    chunk_size: Option<usize>,

    /// Positional arguments (varies by mode)
    #[arg(trailing_var_arg = true)]
    positional: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();

    if let Some(input) = args.decompress {
        let output = args
            .positional
            .first()
            .ok_or_else(|| anyhow::anyhow!("--decompress requires: <input> <output>"))?;
        let compressed = std::fs::read(&input)?;
        let data = compress::decompress(&compressed)?;
        std::fs::write(PathBuf::from(output), &data)?;
        let _ = std::fs::remove_file(&input);
        return Ok(());
    }

    if let Some(output_path) = args.decompress_chunks {
        use std::io::Write;

        if args.positional.len() < 2 {
            anyhow::bail!("--decompress-chunks requires: <output> <num_chunks> <file_id>");
        }
        let num_chunks: usize = args.positional[0].parse()?;
        let file_id = &args.positional[1];
        let tmp_dir = format!("/tmp/beamup-xfer/{file_id}");

        let mut out = std::fs::File::create(&output_path)?;
        for i in 0..num_chunks {
            let chunk_path = format!("{tmp_dir}/chunk-{i:04}");
            let compressed = std::fs::read(&chunk_path)?;
            let data = compress::decompress(&compressed)?;
            out.write_all(&data)?;
            let _ = std::fs::remove_file(&chunk_path);
        }
        let _ = std::fs::remove_dir_all(&tmp_dir);

        return Ok(());
    }

    if let Some(input) = args.compress {
        use std::io::Read;

        let file_id = args
            .positional
            .first()
            .ok_or_else(|| anyhow::anyhow!("--compress requires: <file> <file_id>"))?;

        let chunk_size = args.chunk_size.unwrap_or(compress::CHUNK_SIZE);

        let metadata = std::fs::metadata(&input)?;
        let file_size = metadata.len();

        if file_size <= chunk_size as u64 {
            println!("0");
            return Ok(());
        }

        let tmp_dir = format!("/tmp/beamup-xfer/{file_id}");
        std::fs::create_dir_all(&tmp_dir)?;

        let mut file = std::fs::File::open(&input)?;
        let mut chunk_idx = 0;
        let mut buf = vec![0u8; chunk_size];

        loop {
            let bytes_read = file.read(&mut buf)?;
            if bytes_read == 0 {
                break;
            }

            let compressed = compress::compress(&buf[..bytes_read]);
            let chunk_path = format!("{tmp_dir}/chunk-{chunk_idx:04}");
            std::fs::write(&chunk_path, &compressed)?;
            chunk_idx += 1;
        }

        println!("{chunk_idx}");
        return Ok(());
    }

    if !args.serve {
        anyhow::bail!("agent must be run with --serve, --decompress, --compress, or --decompress-chunks");
    }

    std::fs::create_dir_all(&args.watch_dir)?;
    syncer::run(args.watch_dir).await
}
