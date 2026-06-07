use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use beamup_protocol::compress::{self, CHUNKED_THRESHOLD};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;
use tracing::{debug, info};

use crate::beam::Beam;

#[derive(Debug, Clone)]
pub enum Direction {
    Push,
    Pull,
}

#[derive(Debug)]
pub struct TransferResult {
    pub path: String,
    pub direction: Direction,
    pub success: bool,
    pub error: Option<String>,
}

pub struct TransferPool {
    beam_id: String,
    remote_dir: String,
    local_dir: PathBuf,
    semaphore: Arc<Semaphore>,
    results_tx: mpsc::Sender<TransferResult>,
    pub results_rx: mpsc::Receiver<TransferResult>,
}

impl TransferPool {
    pub fn new(
        beam_id: String,
        local_dir: PathBuf,
        remote_dir: String,
        concurrency: usize,
    ) -> Self {
        let (results_tx, results_rx) = mpsc::channel(256);
        Self {
            beam_id,
            remote_dir,
            local_dir,
            semaphore: Arc::new(Semaphore::new(concurrency)),
            results_tx,
            results_rx,
        }
    }

    /// Push a local file to the beam. Large files are compressed, chunked, and transferred in parallel.
    pub fn push(&self, relative_path: String) -> JoinHandle<()> {
        let sem = self.semaphore.clone();
        let tx = self.results_tx.clone();
        let beam_id = self.beam_id.clone();
        let local_path = self.local_dir.join(&relative_path);
        let remote_path = format!("{}/{}", self.remote_dir, relative_path);

        tokio::spawn(async move {
            let result = push_file(&beam_id, &local_path, &remote_path, &sem).await;
            let _ = tx
                .send(TransferResult {
                    path: relative_path,
                    direction: Direction::Push,
                    success: result.is_ok(),
                    error: result.err().map(|e| e.to_string()),
                })
                .await;
        })
    }

    /// Pull a file from the beam. Large files are pulled as compressed chunks then reassembled locally.
    pub fn pull(&self, relative_path: String) -> JoinHandle<()> {
        let sem = self.semaphore.clone();
        let tx = self.results_tx.clone();
        let beam_id = self.beam_id.clone();
        let local_path = self.local_dir.join(&relative_path);
        let remote_path = format!("{}/{}", self.remote_dir, relative_path);

        tokio::spawn(async move {
            let result = pull_file(&beam_id, &remote_path, &local_path, &sem).await;
            let _ = tx
                .send(TransferResult {
                    path: relative_path,
                    direction: Direction::Pull,
                    success: result.is_ok(),
                    error: result.err().map(|e| e.to_string()),
                })
                .await;
        })
    }

    /// Push multiple files in parallel. Waits for all to complete.
    pub async fn push_batch(&mut self, paths: Vec<String>) -> Vec<TransferResult> {
        let handles: Vec<_> = paths.into_iter().map(|p| self.push(p)).collect();

        for handle in handles {
            let _ = handle.await;
        }

        let mut results = Vec::new();
        while let Ok(result) = self.results_rx.try_recv() {
            results.push(result);
        }
        results
    }

    /// Pull multiple files in parallel. Waits for all to complete.
    pub async fn pull_batch(&mut self, paths: Vec<String>) -> Vec<TransferResult> {
        let handles: Vec<_> = paths.into_iter().map(|p| self.pull(p)).collect();

        for handle in handles {
            let _ = handle.await;
        }

        let mut results = Vec::new();
        while let Ok(result) = self.results_rx.try_recv() {
            results.push(result);
        }
        results
    }
}

/// Push a file to the beam. If it's large, compress per-chunk + parallel scp + reassemble.
async fn push_file(
    beam_id: &str,
    local_path: &Path,
    remote_path: &str,
    sem: &Arc<Semaphore>,
) -> Result<()> {
    let metadata = std::fs::metadata(local_path)?;
    let size = metadata.len();

    if size <= CHUNKED_THRESHOLD {
        // Small-ish file: single scp
        let _permit = sem.acquire().await.unwrap();
        Beam::scp_to_beam(beam_id, local_path, remote_path).await?;
        debug!("pushed (single): {} ({size} bytes)", local_path.display());
    } else {
        // Large file: read in 8MB blocks, compress each independently, parallel scp
        use std::io::Read;

        let file_id = format!("{:x}", beamup_protocol::hash::hash_content(remote_path.as_bytes()));
        let tmp_dir = std::env::temp_dir().join(format!("beamup-push-{file_id}"));
        std::fs::create_dir_all(&tmp_dir)?;

        let mut file = std::fs::File::open(local_path)?;
        let mut buf = vec![0u8; compress::CHUNK_SIZE];
        let mut chunk_idx = 0;

        // Read and compress chunks
        loop {
            let bytes_read = file.read(&mut buf)?;
            if bytes_read == 0 {
                break;
            }
            let compressed = compress::compress(&buf[..bytes_read]);
            let chunk_local = tmp_dir.join(format!("chunk_{chunk_idx:04}"));
            std::fs::write(&chunk_local, &compressed)?;
            chunk_idx += 1;
        }
        let num_chunks = chunk_idx;

        info!(
            "pushing chunked: {} ({size} bytes, {num_chunks} chunks)",
            local_path.display()
        );

        // Parallel scp each chunk
        let mut handles = Vec::with_capacity(num_chunks);
        for i in 0..num_chunks {
            let chunk_local = tmp_dir.join(format!("chunk_{i:04}"));
            let chunk_remote = format!("{remote_path}.beamup-chunk-{i:04}");
            let beam_id = beam_id.to_string();
            let sem = sem.clone();

            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire_owned().await.unwrap();
                let result = Beam::scp_to_beam(&beam_id, &chunk_local, &chunk_remote).await;
                let _ = std::fs::remove_file(&chunk_local);
                result
            }));
        }

        for handle in handles {
            handle.await??;
        }

        // Reassemble and decompress on the beam using the agent
        // The agent's --decompress-chunks mode handles: read N chunk files,
        // decompress each (lz4), concatenate into output file
        Beam::exec_cmd(
            beam_id,
            &[
                "/tmp/beamup-agent",
                "--decompress-chunks",
                remote_path,
                &num_chunks.to_string(),
            ],
        )
        .await?;

        let _ = std::fs::remove_dir_all(&tmp_dir);
        debug!("pushed (chunked): {} ({num_chunks} chunks)", local_path.display());
    }

    Ok(())
}

/// Pull a file from the beam. If it's large, have the agent compress + chunk, then parallel scp pull + reassemble locally.
async fn pull_file(
    beam_id: &str,
    remote_path: &str,
    local_path: &Path,
    sem: &Arc<Semaphore>,
) -> Result<()> {
    if let Some(parent) = local_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Ask the agent to prepare chunked transfer by compressing + splitting
    // Use exec to invoke the agent in compress mode
    let output = Beam::exec_cmd_output(
        beam_id,
        &["/tmp/beamup-agent", "--compress", remote_path],
    )
    .await?;

    // Agent outputs the number of chunks as a single line
    let num_chunks: usize = output.trim().parse().unwrap_or(0);

    if num_chunks == 0 {
        // File is small enough for single scp or doesn't exist
        let _permit = sem.acquire().await.unwrap();
        Beam::scp_from_beam(beam_id, remote_path, local_path).await?;
        debug!("pulled (single): {remote_path}");
        return Ok(());
    }

    info!("pulling chunked: {remote_path} ({num_chunks} chunks)");

    let file_id = format!("{:x}", beamup_protocol::hash::hash_content(remote_path.as_bytes()));
    let tmp_dir = std::env::temp_dir().join(format!("beamup-pull-{file_id}"));
    std::fs::create_dir_all(&tmp_dir)?;

    // Pull chunks in parallel
    let mut handles = Vec::with_capacity(num_chunks);
    for i in 0..num_chunks {
        let chunk_remote = format!("{remote_path}.beamup-lz4-chunk-{i:04}");
        let chunk_local = tmp_dir.join(format!("chunk_{i:04}"));
        let beam_id = beam_id.to_string();
        let sem = sem.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.unwrap();
            Beam::scp_from_beam(&beam_id, &chunk_remote, &chunk_local).await
        }));
    }

    for handle in handles {
        handle.await??;
    }

    // Reassemble locally: each chunk is independently lz4-compressed
    let tmp_path = local_path.with_extension("beamup-tmp");
    {
        use std::io::Write;
        let mut out = std::fs::File::create(&tmp_path)?;
        for i in 0..num_chunks {
            let chunk_local = tmp_dir.join(format!("chunk_{i:04}"));
            let chunk_data = std::fs::read(&chunk_local)?;
            let decompressed = compress::decompress(&chunk_data)?;
            out.write_all(&decompressed)?;
            let _ = std::fs::remove_file(&chunk_local);
        }
    }
    std::fs::rename(&tmp_path, local_path)?;

    // Clean up local temp dir and remote chunks
    let _ = std::fs::remove_dir_all(&tmp_dir);
    let mut rm_cmd = String::from("rm -f");
    for i in 0..num_chunks {
        rm_cmd.push_str(&format!(" '{remote_path}.beamup-lz4-chunk-{i:04}'"));
    }
    let _ = Beam::exec_shell(beam_id, &rm_cmd).await;

    debug!("pulled (chunked): {remote_path} ({num_chunks} chunks)");
    Ok(())
}

/// Deploy the agent binary using chunked parallel transfer (no compression —
/// the agent isn't available yet to decompress, and raw chunked parallel scp
/// is already much faster than a single serial transfer).
pub async fn deploy_agent_chunked(
    beam_id: &str,
    agent_path: &Path,
    concurrency: usize,
) -> Result<()> {
    let data = std::fs::read(agent_path)?;
    let size = data.len();
    let chunks = compress::split_chunks(&data);
    let num_chunks = chunks.len();

    info!("deploying agent: {size} bytes, {num_chunks} chunks");

    if num_chunks <= 1 {
        Beam::scp_to_beam(beam_id, agent_path, "/tmp/beamup-agent").await?;
        Beam::exec_cmd(beam_id, &["chmod", "+x", "/tmp/beamup-agent"]).await?;
        return Ok(());
    }

    let tmp_dir = std::env::temp_dir().join("beamup-agent-deploy");
    std::fs::create_dir_all(&tmp_dir)?;

    let sem = Arc::new(Semaphore::new(concurrency));

    let mut handles = Vec::with_capacity(num_chunks);
    for (i, chunk) in chunks.into_iter().enumerate() {
        let chunk_local = tmp_dir.join(format!("agent_chunk_{i:04}"));
        std::fs::write(&chunk_local, &chunk)?;

        let chunk_remote = format!("/tmp/beamup-agent.chunk-{i:04}");
        let beam_id = beam_id.to_string();
        let sem = sem.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.unwrap();
            let result = Beam::scp_to_beam(&beam_id, &chunk_local, &chunk_remote).await;
            let _ = std::fs::remove_file(&chunk_local);
            result
        }));
    }

    for handle in handles {
        handle.await??;
    }

    // Reassemble on beam with cat
    let mut cat_cmd = String::from("cat");
    for i in 0..num_chunks {
        cat_cmd.push_str(&format!(" /tmp/beamup-agent.chunk-{i:04}"));
    }
    cat_cmd.push_str(" > /tmp/beamup-agent && rm -f");
    for i in 0..num_chunks {
        cat_cmd.push_str(&format!(" /tmp/beamup-agent.chunk-{i:04}"));
    }

    Beam::exec_shell(beam_id, &cat_cmd).await?;
    Beam::exec_cmd(beam_id, &["chmod", "+x", "/tmp/beamup-agent"]).await?;

    info!("agent deployed ({num_chunks} chunks)");
    Ok(())
}
