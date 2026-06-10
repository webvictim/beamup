use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use beamup_protocol::compress;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

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
    chunk_size: usize,
    results_tx: mpsc::Sender<TransferResult>,
    pub results_rx: mpsc::Receiver<TransferResult>,
    progress_tx: mpsc::Sender<u64>,
    pub progress_rx: mpsc::Receiver<u64>,
}

impl TransferPool {
    pub fn new(
        beam_id: String,
        local_dir: PathBuf,
        remote_dir: String,
        concurrency: usize,
        chunk_size: usize,
    ) -> Self {
        let (results_tx, results_rx) = mpsc::channel(256);
        let (progress_tx, progress_rx) = mpsc::channel(1024);
        Self {
            beam_id,
            remote_dir,
            local_dir,
            semaphore: Arc::new(Semaphore::new(concurrency)),
            chunk_size,
            results_tx,
            results_rx,
            progress_tx,
            progress_rx,
        }
    }

    pub fn push(&self, relative_path: String) -> JoinHandle<()> {
        let sem = self.semaphore.clone();
        let tx = self.results_tx.clone();
        let progress_tx = self.progress_tx.clone();
        let beam_id = self.beam_id.clone();
        let local_path = self.local_dir.join(&relative_path);
        let remote_path = format!("{}/{}", self.remote_dir, relative_path);
        let chunk_size = self.chunk_size;

        tokio::spawn(async move {
            let start = Instant::now();
            let size = std::fs::metadata(&local_path).map(|m| m.len()).unwrap_or(0);
            debug!("push start: {} ({size} bytes)", relative_path);
            let result = push_file(&beam_id, &local_path, &remote_path, &sem, chunk_size, &progress_tx).await;
            let duration = start.elapsed();

            if result.is_ok() {
                let bw = if duration.as_secs_f64() > 0.0 {
                    size as f64 / duration.as_secs_f64() / 1024.0 / 1024.0
                } else {
                    0.0
                };
                debug!(
                    "push done: {} ({} bytes in {:.1}s, {:.1} MB/s)",
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
        let progress_tx = self.progress_tx.clone();
        let beam_id = self.beam_id.clone();
        let local_path = self.local_dir.join(&relative_path);
        let remote_path = format!("{}/{}", self.remote_dir, relative_path);
        let chunk_size = self.chunk_size;

        tokio::spawn(async move {
            let start = Instant::now();
            debug!("pull start: {}", relative_path);
            let result = pull_file(&beam_id, &remote_path, &local_path, &sem, chunk_size, &progress_tx).await;
            let duration = start.elapsed();
            let size = std::fs::metadata(&local_path).map(|m| m.len()).unwrap_or(0);

            if result.is_ok() {
                let bw = if duration.as_secs_f64() > 0.0 {
                    size as f64 / duration.as_secs_f64() / 1024.0 / 1024.0
                } else {
                    0.0
                };
                debug!(
                    "pull done: {} ({} bytes in {:.1}s, {:.1} MB/s)",
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

    /// Push files via tar streaming through tsh beams exec pipes.
    /// Files are grouped into batches (~256MB each) and transferred in parallel.
    pub async fn push_batch_tar(&self, paths: Vec<String>, sizes: Vec<u64>) -> Vec<TransferResult> {
        let batches = make_batches(paths, sizes);
        let num_batches = batches.len();
        debug!("tar push: {num_batches} batches");

        let mut handles = Vec::with_capacity(num_batches);
        for (batch_idx, batch) in batches.into_iter().enumerate() {
            let sem = self.semaphore.clone();
            let beam_id = self.beam_id.clone();
            let local_dir = self.local_dir.clone();
            let remote_dir = self.remote_dir.clone();

            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                let start = Instant::now();
                let batch_paths = batch.paths.clone();
                let batch_bytes = batch.total_bytes;
                info!("tar push batch {batch_idx}: {} files, {} bytes", batch_paths.len(), batch_bytes);

                let result = tar_push_batch(&beam_id, &local_dir, &remote_dir, &batch.paths).await;
                let duration = start.elapsed();

                match result {
                    Ok(bytes) => {
                        let bw = if duration.as_secs_f64() > 0.0 {
                            bytes as f64 / duration.as_secs_f64() / 1024.0 / 1024.0
                        } else {
                            0.0
                        };
                        info!(
                            "tar push batch {batch_idx} done: {} files, {} bytes in {:.1}s ({:.1} MB/s)",
                            batch_paths.len(), bytes, duration.as_secs_f64(), bw
                        );
                        batch_paths.into_iter().map(|p| TransferResult {
                            path: p,
                            direction: Direction::Push,
                            success: true,
                            error: None,
                            bytes: 0,
                            duration_ms: duration.as_millis() as u64,
                        }).collect::<Vec<_>>()
                    }
                    Err(e) => {
                        warn!("tar push batch {batch_idx} failed: {e}");
                        batch_paths.into_iter().map(|p| TransferResult {
                            path: p,
                            direction: Direction::Push,
                            success: false,
                            error: Some(e.to_string()),
                            bytes: 0,
                            duration_ms: duration.as_millis() as u64,
                        }).collect::<Vec<_>>()
                    }
                }
            }));
        }

        let mut results = Vec::new();
        for handle in handles {
            if let Ok(batch_results) = handle.await {
                results.extend(batch_results);
            }
        }
        results
    }

    /// Pull files via tar streaming through tsh beams exec pipes.
    /// Files are grouped into batches (~256MB each) and transferred in parallel.
    pub async fn pull_batch_tar(&self, paths: Vec<String>, sizes: Vec<u64>) -> Vec<TransferResult> {
        let batches = make_batches(paths, sizes);
        let num_batches = batches.len();
        debug!("tar pull: {num_batches} batches");

        let mut handles = Vec::with_capacity(num_batches);
        for (batch_idx, batch) in batches.into_iter().enumerate() {
            let sem = self.semaphore.clone();
            let beam_id = self.beam_id.clone();
            let local_dir = self.local_dir.clone();
            let remote_dir = self.remote_dir.clone();

            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                let start = Instant::now();
                let batch_paths = batch.paths.clone();
                let batch_bytes = batch.total_bytes;
                debug!("tar pull batch {batch_idx}: {} files, {} bytes", batch_paths.len(), batch_bytes);

                let result = tar_pull_batch(&beam_id, &local_dir, &remote_dir, &batch.paths).await;
                let duration = start.elapsed();

                match result {
                    Ok(bytes) => {
                        let bw = if duration.as_secs_f64() > 0.0 {
                            bytes as f64 / duration.as_secs_f64() / 1024.0 / 1024.0
                        } else {
                            0.0
                        };
                        debug!(
                            "tar pull batch {batch_idx} done: {} files, {} bytes in {:.1}s ({:.1} MB/s)",
                            batch_paths.len(), bytes, duration.as_secs_f64(), bw
                        );
                        batch_paths.into_iter().map(|p| TransferResult {
                            path: p,
                            direction: Direction::Pull,
                            success: true,
                            error: None,
                            bytes: 0,
                            duration_ms: duration.as_millis() as u64,
                        }).collect::<Vec<_>>()
                    }
                    Err(e) => {
                        warn!("tar pull batch {batch_idx} failed: {e}");
                        batch_paths.into_iter().map(|p| TransferResult {
                            path: p,
                            direction: Direction::Pull,
                            success: false,
                            error: Some(e.to_string()),
                            bytes: 0,
                            duration_ms: duration.as_millis() as u64,
                        }).collect::<Vec<_>>()
                    }
                }
            }));
        }

        let mut results = Vec::new();
        for handle in handles {
            if let Ok(batch_results) = handle.await {
                results.extend(batch_results);
            }
        }
        results
    }
}

fn file_id(path: &str) -> String {
    format!("{:x}", beamup_protocol::hash::hash_content(path.as_bytes()))
}

const TAR_BATCH_SIZE: u64 = 256 * 1024 * 1024; // 256MB per batch

struct Batch {
    paths: Vec<String>,
    total_bytes: u64,
}

fn make_batches(paths: Vec<String>, sizes: Vec<u64>) -> Vec<Batch> {
    let mut batches: Vec<Batch> = Vec::new();
    let mut current = Batch { paths: Vec::new(), total_bytes: 0 };

    for (path, size) in paths.into_iter().zip(sizes.into_iter()) {
        if !current.paths.is_empty() && current.total_bytes + size > TAR_BATCH_SIZE {
            batches.push(current);
            current = Batch { paths: Vec::new(), total_bytes: 0 };
        }
        current.total_bytes += size;
        current.paths.push(path);
    }
    if !current.paths.is_empty() {
        batches.push(current);
    }
    batches
}

/// Push a batch of files to the beam by streaming a tar archive through tsh beams exec.
async fn tar_push_batch(
    beam_id: &str,
    local_dir: &Path,
    remote_dir: &str,
    paths: &[String],
) -> Result<u64> {
    use std::process::{Command, Stdio};

    let beam_id = beam_id.to_string();
    let remote_dir = remote_dir.to_string();
    let local_dir = local_dir.to_path_buf();
    let paths = paths.to_vec();

    tokio::task::spawn_blocking(move || -> Result<u64> {
        let mut child = Command::new("tsh")
            .args(["beams", "exec", &beam_id, "--", "tar", "xf", "-", "-C", &remote_dir])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let stdin = child.stdin.take().expect("stdin not captured");
        let mut bytes_written: u64 = 0;
        let mut skipped: usize = 0;
        let mut builder = tar::Builder::new(stdin);
        for rel_path in &paths {
            let full_path = local_dir.join(rel_path);
            if !full_path.exists() {
                skipped += 1;
                continue;
            }
            if let Ok(metadata) = full_path.metadata() {
                bytes_written += metadata.len();
            }
            let mut file = match std::fs::File::open(&full_path) {
                Ok(f) => f,
                Err(_) => { skipped += 1; continue; }
            };
            if let Err(e) = builder.append_file(rel_path, &mut file) {
                // If the pipe broke (remote tar died), bail out
                if e.kind() == std::io::ErrorKind::BrokenPipe {
                    drop(builder);
                    let output = child.wait_with_output()?;
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    anyhow::bail!("tar pipe broke ({skipped} skipped): {stderr}");
                }
                skipped += 1;
            }
        }
        if skipped > 0 {
            debug!("tar push: skipped {skipped} missing/unreadable files");
        }
        builder.finish()?;
        drop(builder);

        let output = child.wait_with_output()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("tar push exec failed: {stderr}");
        }

        Ok(bytes_written)
    }).await?
}

/// Pull a batch of files from the beam by reading a tar stream from tsh beams exec.
async fn tar_pull_batch(
    beam_id: &str,
    local_dir: &Path,
    remote_dir: &str,
    paths: &[String],
) -> Result<u64> {
    use std::io::{Read, Write};
    use std::process::{Command, Stdio};

    let beam_id = beam_id.to_string();
    let remote_dir = remote_dir.to_string();
    let local_dir = local_dir.to_path_buf();
    let paths = paths.to_vec();

    tokio::task::spawn_blocking(move || -> Result<u64> {
        // Use -T - to pass file list via stdin (avoids arg length limits)
        let mut child = Command::new("tsh")
            .args(["beams", "exec", &beam_id, "--", "tar", "cf", "-", "-C", &remote_dir, "-T", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        // Write file list to stdin then close it
        let mut stdin = child.stdin.take().expect("stdin not captured");
        let file_list = paths.join("\n") + "\n";
        stdin.write_all(file_list.as_bytes())?;
        drop(stdin);

        // Read tar from stdout
        let mut stdout = child.stdout.take().expect("stdout not captured");
        let mut tar_data = Vec::new();
        stdout.read_to_end(&mut tar_data)?;

        let status = child.wait()?;
        if !status.success() {
            let stderr_data = child.stderr.take()
                .map(|mut e| { let mut s = String::new(); let _ = e.read_to_string(&mut s); s })
                .unwrap_or_default();
            if tar_data.is_empty() {
                anyhow::bail!("tar pull exec failed: {stderr_data}");
            }
            debug!("tar pull exec returned non-zero (some files may be missing)");
        }

        let total_bytes = tar_data.len() as u64;

        // Extract tar archive locally
        let mut archive = tar::Archive::new(tar_data.as_slice());
        archive.unpack(&local_dir)?;

        Ok(total_bytes)
    }).await?
}

async fn push_file(
    beam_id: &str,
    local_path: &Path,
    remote_path: &str,
    sem: &Arc<Semaphore>,
    chunk_size: usize,
    progress_tx: &mpsc::Sender<u64>,
) -> Result<()> {
    let metadata = std::fs::metadata(local_path)?;
    let size = metadata.len();

    if size <= chunk_size as u64 {
        let _permit = sem.acquire().await.unwrap();
        Beam::scp_to_beam(beam_id, local_path, remote_path).await?;
        let _ = progress_tx.send(size).await;
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
        let mut buf = vec![0u8; chunk_size];
        let mut chunk_idx = 0;
        let mut chunk_sizes: Vec<u64> = Vec::new();

        loop {
            let bytes_read = file.read(&mut buf)?;
            if bytes_read == 0 {
                break;
            }
            let compressed = compress::compress(&buf[..bytes_read]);
            let chunk_local = local_tmp.join(format!("chunk-{chunk_idx:04}"));
            std::fs::write(&chunk_local, &compressed)?;
            chunk_sizes.push(bytes_read as u64);
            chunk_idx += 1;
        }
        let num_chunks = chunk_idx;

        debug!(
            "pushing chunked: {} ({size} bytes, {num_chunks} chunks)",
            local_path.display()
        );

        let mut handles = Vec::with_capacity(num_chunks);
        for i in 0..num_chunks {
            let chunk_local = local_tmp.join(format!("chunk-{i:04}"));
            let chunk_remote = format!("{remote_tmp}/chunk-{i:04}");
            let beam_id = beam_id.to_string();
            let sem = sem.clone();
            let progress_tx = progress_tx.clone();
            let chunk_bytes = chunk_sizes[i];

            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire_owned().await.unwrap();
                let result = Beam::scp_to_beam(&beam_id, &chunk_local, &chunk_remote).await;
                let _ = std::fs::remove_file(&chunk_local);
                if result.is_ok() {
                    let _ = progress_tx.send(chunk_bytes).await;
                }
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
    chunk_size: usize,
    progress_tx: &mpsc::Sender<u64>,
) -> Result<()> {
    if let Some(parent) = local_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let fid = file_id(remote_path);

    // Agent compresses + splits into /tmp/beamup-xfer/<fid>/
    let output = Beam::exec_cmd_output(
        beam_id,
        &["/tmp/beamup-agent", "--compress", remote_path, &fid, "--chunk-size", &chunk_size.to_string()],
    )
    .await?;

    let num_chunks: usize = output.trim().parse().unwrap_or(0);

    if num_chunks == 0 {
        // Atomic pull: scp to temp file, then rename
        let tmp_path = local_path.with_extension("beamup-pull-tmp");
        let _permit = sem.acquire().await.unwrap();
        Beam::scp_from_beam(beam_id, remote_path, &tmp_path).await?;
        let size = std::fs::metadata(&tmp_path).map(|m| m.len()).unwrap_or(0);
        std::fs::rename(&tmp_path, local_path)?;
        let _ = progress_tx.send(size).await;
        debug!("pulled (single): {remote_path}");
        return Ok(());
    }

    debug!("pulling chunked: {remote_path} ({num_chunks} chunks)");

    let local_tmp = std::env::temp_dir().join(format!("beamup-pull-{fid}"));
    std::fs::create_dir_all(&local_tmp)?;
    let remote_tmp = format!("/tmp/beamup-xfer/{fid}");

    let mut handles = Vec::with_capacity(num_chunks);
    for i in 0..num_chunks {
        let chunk_remote = format!("{remote_tmp}/chunk-{i:04}");
        let chunk_local = local_tmp.join(format!("chunk-{i:04}"));
        let beam_id = beam_id.to_string();
        let sem = sem.clone();
        let progress_tx = progress_tx.clone();
        let chunk_bytes = chunk_size as u64;

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.unwrap();
            let result = Beam::scp_from_beam(&beam_id, &chunk_remote, &chunk_local).await;
            if result.is_ok() {
                let _ = progress_tx.send(chunk_bytes).await;
            }
            result
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

/// Deploy the agent binary to the beam
pub async fn deploy_agent_chunked(
    beam_id: &str,
    agent_path: &Path,
    _concurrency: usize,
) -> Result<()> {
    // Remove existing agent binary to avoid SFTP failures when overwriting
    let _ = Beam::exec_shell(beam_id, "rm -rf /tmp/beamup-agent /tmp/beamup-xfer && mkdir -p /tmp/beamup-xfer").await;

    let size = agent_path.metadata()?.len();

    if size <= 50 * 1024 * 1024 {
        info!("deploying agent: {} bytes", size);
        Beam::scp_to_beam(beam_id, agent_path, "/tmp/beamup-agent").await?;
        Beam::exec_shell(beam_id, "chmod +x /tmp/beamup-agent").await?;
    } else {
        use std::io::Write;
        use flate2::write::GzEncoder;
        use flate2::Compression;

        let data = std::fs::read(agent_path)?;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&data)?;
        let compressed = encoder.finish()?;
        let compressed_size = compressed.len();
        let ratio = size as f64 / compressed_size as f64;
        info!("deploying agent: {size} bytes -> {compressed_size} bytes compressed ({ratio:.1}x)");

        let local_tmp = std::env::temp_dir().join("beamup-agent-deploy.gz");
        std::fs::write(&local_tmp, &compressed)?;
        Beam::scp_to_beam(beam_id, &local_tmp, "/tmp/beamup-agent.gz").await?;
        let _ = std::fs::remove_file(&local_tmp);
        Beam::exec_shell(beam_id, "gunzip -f /tmp/beamup-agent.gz && chmod +x /tmp/beamup-agent").await?;
    }

    Ok(())
}
