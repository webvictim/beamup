use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

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
#[allow(dead_code)]
pub struct TransferResult {
    pub path: String,
    pub direction: Direction,
    pub success: bool,
    pub error: Option<String>,
    pub bytes: u64,
    pub duration_ms: u64,
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

    pub fn push(&self, relative_path: String) -> JoinHandle<()> {
        let sem = self.semaphore.clone();
        let tx = self.results_tx.clone();
        let beam_id = self.beam_id.clone();
        let local_path = self.local_dir.join(&relative_path);
        let remote_path = format!("{}/{}", self.remote_dir, relative_path);

        tokio::spawn(async move {
            let start = Instant::now();
            let size = std::fs::metadata(&local_path).map(|m| m.len()).unwrap_or(0);
            let result = push_file(&beam_id, &local_path, &remote_path, &sem).await;
            let duration = start.elapsed();

            if result.is_ok() {
                let bw = if duration.as_secs_f64() > 0.0 {
                    size as f64 / duration.as_secs_f64() / 1024.0 / 1024.0
                } else {
                    0.0
                };
                info!(
                    "pushed: {} ({} bytes in {:.1}s, {:.1} MB/s)",
                    relative_path,
                    size,
                    duration.as_secs_f64(),
                    bw
                );
            }

            let _ = tx
                .send(TransferResult {
                    path: relative_path,
                    direction: Direction::Push,
                    success: result.is_ok(),
                    error: result.err().map(|e| e.to_string()),
                    bytes: size,
                    duration_ms: duration.as_millis() as u64,
                })
                .await;
        })
    }

    pub fn pull(&self, relative_path: String) -> JoinHandle<()> {
        let sem = self.semaphore.clone();
        let tx = self.results_tx.clone();
        let beam_id = self.beam_id.clone();
        let local_path = self.local_dir.join(&relative_path);
        let remote_path = format!("{}/{}", self.remote_dir, relative_path);

        tokio::spawn(async move {
            let start = Instant::now();
            let result = pull_file(&beam_id, &remote_path, &local_path, &sem).await;
            let duration = start.elapsed();
            let size = std::fs::metadata(&local_path).map(|m| m.len()).unwrap_or(0);

            if result.is_ok() {
                let bw = if duration.as_secs_f64() > 0.0 {
                    size as f64 / duration.as_secs_f64() / 1024.0 / 1024.0
                } else {
                    0.0
                };
                info!(
                    "pulled: {} ({} bytes in {:.1}s, {:.1} MB/s)",
                    relative_path,
                    size,
                    duration.as_secs_f64(),
                    bw
                );
            }

            let _ = tx
                .send(TransferResult {
                    path: relative_path,
                    direction: Direction::Pull,
                    success: result.is_ok(),
                    error: result.err().map(|e| e.to_string()),
                    bytes: size,
                    duration_ms: duration.as_millis() as u64,
                })
                .await;
        })
    }

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

fn file_id(path: &str) -> String {
    format!("{:x}", beamup_protocol::hash::hash_content(path.as_bytes()))
}

async fn push_file(
    beam_id: &str,
    local_path: &Path,
    remote_path: &str,
    sem: &Arc<Semaphore>,
) -> Result<()> {
    let metadata = std::fs::metadata(local_path)?;
    let size = metadata.len();

    if size <= CHUNKED_THRESHOLD {
        let _permit = sem.acquire().await.unwrap();
        Beam::scp_to_beam(beam_id, local_path, remote_path).await?;
        debug!("pushed (single): {} ({size} bytes)", local_path.display());
    } else {
        use std::io::Read;

        let fid = file_id(remote_path);
        let local_tmp = std::env::temp_dir().join(format!("beamup-push-{fid}"));
        std::fs::create_dir_all(&local_tmp)?;
        let remote_tmp = format!("/tmp/beamup-xfer/{fid}");

        // Ensure remote temp dir exists
        Beam::exec_shell(beam_id, &format!("mkdir -p '{remote_tmp}'")).await?;

        let mut file = std::fs::File::open(local_path)?;
        let mut buf = vec![0u8; compress::CHUNK_SIZE];
        let mut chunk_idx = 0;

        loop {
            let bytes_read = file.read(&mut buf)?;
            if bytes_read == 0 {
                break;
            }
            let compressed = compress::compress(&buf[..bytes_read]);
            let chunk_local = local_tmp.join(format!("chunk-{chunk_idx:04}"));
            std::fs::write(&chunk_local, &compressed)?;
            chunk_idx += 1;
        }
        let num_chunks = chunk_idx;

        info!(
            "pushing chunked: {} ({size} bytes, {num_chunks} chunks)",
            local_path.display()
        );

        let mut handles = Vec::with_capacity(num_chunks);
        for i in 0..num_chunks {
            let chunk_local = local_tmp.join(format!("chunk-{i:04}"));
            let chunk_remote = format!("{remote_tmp}/chunk-{i:04}");
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

        // Agent reassembles: reads chunks from /tmp/beamup-xfer/<fid>/, decompresses, writes output
        Beam::exec_cmd(
            beam_id,
            &[
                "/tmp/beamup-agent",
                "--decompress-chunks",
                remote_path,
                &num_chunks.to_string(),
                &fid,
            ],
        )
        .await?;

        let _ = std::fs::remove_dir_all(&local_tmp);
    }

    Ok(())
}

async fn pull_file(
    beam_id: &str,
    remote_path: &str,
    local_path: &Path,
    sem: &Arc<Semaphore>,
) -> Result<()> {
    if let Some(parent) = local_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let fid = file_id(remote_path);

    // Agent compresses + splits into /tmp/beamup-xfer/<fid>/
    let output = Beam::exec_cmd_output(
        beam_id,
        &["/tmp/beamup-agent", "--compress", remote_path, &fid],
    )
    .await?;

    let num_chunks: usize = output.trim().parse().unwrap_or(0);

    if num_chunks == 0 {
        // Atomic pull: scp to temp file, then rename
        let tmp_path = local_path.with_extension("beamup-pull-tmp");
        let _permit = sem.acquire().await.unwrap();
        Beam::scp_from_beam(beam_id, remote_path, &tmp_path).await?;
        std::fs::rename(&tmp_path, local_path)?;
        debug!("pulled (single): {remote_path}");
        return Ok(());
    }

    info!("pulling chunked: {remote_path} ({num_chunks} chunks)");

    let local_tmp = std::env::temp_dir().join(format!("beamup-pull-{fid}"));
    std::fs::create_dir_all(&local_tmp)?;
    let remote_tmp = format!("/tmp/beamup-xfer/{fid}");

    let mut handles = Vec::with_capacity(num_chunks);
    for i in 0..num_chunks {
        let chunk_remote = format!("{remote_tmp}/chunk-{i:04}");
        let chunk_local = local_tmp.join(format!("chunk-{i:04}"));
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
            let chunk_local = local_tmp.join(format!("chunk-{i:04}"));
            let chunk_data = std::fs::read(&chunk_local)?;
            let decompressed = compress::decompress(&chunk_data)?;
            out.write_all(&decompressed)?;
            let _ = std::fs::remove_file(&chunk_local);
        }
    }
    std::fs::rename(&tmp_path, local_path)?;

    // Clean up
    let _ = std::fs::remove_dir_all(&local_tmp);
    let _ = Beam::exec_shell(beam_id, &format!("rm -rf '{remote_tmp}'")).await;

    Ok(())
}

/// Deploy the agent binary using chunked parallel transfer
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

    let local_tmp = std::env::temp_dir().join("beamup-agent-deploy");
    std::fs::create_dir_all(&local_tmp)?;

    // Ensure remote temp dir exists
    Beam::exec_shell(beam_id, "mkdir -p /tmp/beamup-deploy").await?;

    let sem = Arc::new(Semaphore::new(concurrency));

    let mut handles = Vec::with_capacity(num_chunks);
    for (i, chunk) in chunks.into_iter().enumerate() {
        let chunk_local = local_tmp.join(format!("chunk-{i:04}"));
        std::fs::write(&chunk_local, &chunk)?;

        let chunk_remote = format!("/tmp/beamup-deploy/chunk-{i:04}");
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
        cat_cmd.push_str(&format!(" /tmp/beamup-deploy/chunk-{i:04}"));
    }
    cat_cmd.push_str(" > /tmp/beamup-agent && rm -rf /tmp/beamup-deploy");

    Beam::exec_shell(beam_id, &cat_cmd).await?;
    Beam::exec_cmd(beam_id, &["chmod", "+x", "/tmp/beamup-agent"]).await?;

    let _ = std::fs::remove_dir_all(&local_tmp);
    info!("agent deployed ({num_chunks} chunks)");
    Ok(())
}
