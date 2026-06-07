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

    /// Decompress chunked files: --decompress-chunks <output-path> <num-chunks>
    /// Reads <output-path>.beamup-chunk-NNNN files, decompresses each, writes concatenated output.
    #[arg(long)]
    decompress_chunks: Option<PathBuf>,

    /// Output path for decompress mode, or num_chunks for decompress-chunks mode
    #[arg(index = 1)]
    output: Option<String>,
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
        std::fs::write(PathBuf::from(&output), &data)?;
        let _ = std::fs::remove_file(&input);
        return Ok(());
    }

    if let Some(output_path) = args.decompress_chunks {
        // Decompress-chunks mode: read N chunk files, decompress each, concatenate into output
        use std::io::Write;

        let num_chunks: usize = args
            .output
            .ok_or_else(|| anyhow::anyhow!("--decompress-chunks requires num_chunks argument"))?
            .parse()?;

        let base = output_path.to_string_lossy().to_string();
        let mut out = std::fs::File::create(&output_path)?;

        for i in 0..num_chunks {
            let chunk_path = format!("{base}.beamup-chunk-{i:04}");
            let compressed = std::fs::read(&chunk_path)?;
            let data = compress::decompress(&compressed)?;
            out.write_all(&data)?;
            let _ = std::fs::remove_file(&chunk_path);
        }

        return Ok(());
    }

    if let Some(input) = args.compress {
        // Compress mode: stream file in 8MB chunks, compress each individually,
        // write as separate chunk files. This avoids loading the whole file into memory.
        use std::io::Read;

        let metadata = std::fs::metadata(&input)?;
        let file_size = metadata.len();

        if file_size <= compress::CHUNKED_THRESHOLD {
            println!("0");
            return Ok(());
        }

        let mut file = std::fs::File::open(&input)?;
        let base = input.to_string_lossy();
        let mut chunk_idx = 0;
        let mut buf = vec![0u8; compress::CHUNK_SIZE];

        loop {
            let bytes_read = file.read(&mut buf)?;
            if bytes_read == 0 {
                break;
            }

            let compressed = compress::compress(&buf[..bytes_read]);
            let chunk_path = format!("{base}.beamup-lz4-chunk-{chunk_idx:04}");
            std::fs::write(&chunk_path, &compressed)?;
            chunk_idx += 1;
        }

        println!("{chunk_idx}");
        return Ok(());
    }

    if !args.serve {
        anyhow::bail!("agent must be run with --serve, --decompress, or --compress");
    }

    std::fs::create_dir_all(&args.watch_dir)?;
    syncer::run(args.watch_dir).await
}
