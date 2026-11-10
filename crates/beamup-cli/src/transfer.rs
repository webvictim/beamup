use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use beamup_protocol::compress;
use indicatif::ProgressBar;
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
    /// `pb`, if given, advances as each batch's bytes move through its pipe.
    pub async fn push_batch_tar(
        &self,
        paths: Vec<String>,
        sizes: Vec<u64>,
        pb: Option<ProgressBar>,
    ) -> Vec<TransferResult> {
        let batches = make_batches(paths, sizes);
        let num_batches = batches.len();
        debug!("tar push: {num_batches} batches");

        let mut handles = Vec::with_capacity(num_batches);
        for (batch_idx, batch) in batches.into_iter().enumerate() {
            let sem = self.semaphore.clone();
            let beam_id = self.beam_id.clone();
            let local_dir = self.local_dir.clone();
            let remote_dir = self.remote_dir.clone();
            let progress = BatchProgress::new(pb.clone(), batch.total_bytes);

            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                let start = Instant::now();
                let batch_paths = batch.paths.clone();
                let batch_bytes = batch.total_bytes;
                info!("tar push batch {batch_idx}: {} files, {} bytes", batch_paths.len(), batch_bytes);

                let mut result = tar_push_batch(&beam_id, &local_dir, &remote_dir, &batch.paths, progress.clone()).await;
                for retry in 1..=3u32 {
                    if result.is_ok() {
                        break;
                    }
                    progress.rewind();
                    let backoff = std::time::Duration::from_secs(1) * 2u32.pow(retry - 1);
                    warn!("tar push batch {batch_idx} retry {retry}/3 (backoff {backoff:?})");
                    tokio::time::sleep(backoff).await;
                    result = tar_push_batch(&beam_id, &local_dir, &remote_dir, &batch.paths, progress.clone()).await;
                }
                if result.is_ok() {
                    progress.complete();
                } else {
                    progress.rewind();
                }
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
    /// `pb`, if given, advances as each batch's bytes move through its pipe.
    pub async fn pull_batch_tar(
        &self,
        paths: Vec<String>,
        sizes: Vec<u64>,
        pb: Option<ProgressBar>,
    ) -> Vec<TransferResult> {
        let batches = make_batches(paths, sizes);
        let num_batches = batches.len();
        debug!("tar pull: {num_batches} batches");

        let mut handles = Vec::with_capacity(num_batches);
        for (batch_idx, batch) in batches.into_iter().enumerate() {
            let sem = self.semaphore.clone();
            let beam_id = self.beam_id.clone();
            let local_dir = self.local_dir.clone();
            let remote_dir = self.remote_dir.clone();
            let progress = BatchProgress::new(pb.clone(), batch.total_bytes);

            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                let start = Instant::now();
                let batch_paths = batch.paths.clone();
                let batch_bytes = batch.total_bytes;
                debug!("tar pull batch {batch_idx}: {} files, {} bytes", batch_paths.len(), batch_bytes);

                let mut result = tar_pull_batch(&beam_id, &local_dir, &remote_dir, &batch.paths, progress.clone()).await;
                for retry in 1..=3u32 {
                    if result.is_ok() {
                        break;
                    }
                    progress.rewind();
                    let backoff = std::time::Duration::from_secs(1) * 2u32.pow(retry - 1);
                    warn!("tar pull batch {batch_idx} retry {retry}/3 (backoff {backoff:?})");
                    tokio::time::sleep(backoff).await;
                    result = tar_pull_batch(&beam_id, &local_dir, &remote_dir, &batch.paths, progress.clone()).await;
                }
                if result.is_ok() {
                    progress.complete();
                } else {
                    progress.rewind();
                }
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

/// Moves the progress bar as a tar batch's bytes go through its pipe, so a
/// slow link shows steady progress rather than a jump when the batch ends.
/// Credit is capped at the batch's file bytes (a pulled tar stream also
/// carries headers), and `rewind` takes back a failed attempt's credit before
/// a retry so the same bytes are never counted twice.
#[derive(Clone)]
struct BatchProgress {
    pb: Option<ProgressBar>,
    total: u64,
    credited: Arc<AtomicU64>,
}

impl BatchProgress {
    fn new(pb: Option<ProgressBar>, total: u64) -> Self {
        Self { pb, total, credited: Arc::new(AtomicU64::new(0)) }
    }

    fn add(&self, n: u64) {
        let Some(pb) = &self.pb else { return };
        // A compare-exchange loop rather than fetch_update, which newer
        // toolchains deprecate in favour of a try_update older ones lack.
        let mut prev = self.credited.load(Ordering::Relaxed);
        loop {
            let next = prev.saturating_add(n).min(self.total);
            if next == prev {
                return;
            }
            match self.credited.compare_exchange_weak(prev, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => {
                    pb.inc(next - prev);
                    return;
                }
                Err(actual) => prev = actual,
            }
        }
    }

    /// Credit whatever the stream didn't (e.g. files skipped as missing).
    fn complete(&self) {
        self.add(self.total);
    }

    fn rewind(&self) {
        let Some(pb) = &self.pb else { return };
        let prev = self.credited.swap(0, Ordering::Relaxed);
        // dec, not set_position(position() - prev): batches share the bar,
        // and another batch's inc between the read and the write would be
        // lost. The bar only ever gains through inc, so its position is at
        // least this batch's credit and dec can't wrap.
        pb.dec(prev);
    }
}

/// Reports every byte read through it to a batch's progress.
struct CountingReader<R> {
    inner: R,
    progress: BatchProgress,
}

impl<R: std::io::Read> std::io::Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.progress.add(n as u64);
        Ok(n)
    }
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

/// A tar entry for `file` whose data is read through a counter, so the bar
/// moves as the file streams; `Builder::append_data` adds it. This is what
/// `append_file` does — the same GNU header, from the same metadata, with
/// long paths handled by `append_data` — except that a sparse file is sent
/// in full rather than as sparse entries, which unpacks to the same file.
/// `take()` keeps a file that grows mid-read from overrunning the size in
/// its header.
fn counted_entry(
    file: std::fs::File,
    progress: &BatchProgress,
) -> std::io::Result<(tar::Header, CountingReader<std::io::Take<std::fs::File>>, u64)> {
    use std::io::Read;

    let metadata = file.metadata()?;
    let mut header = tar::Header::new_gnu();
    header.set_metadata(&metadata);
    let len = metadata.len();
    let reader = CountingReader { inner: file.take(len), progress: progress.clone() };
    Ok((header, reader, len))
}

/// Push a batch of files to the beam by streaming a tar archive through tsh beams exec.
async fn tar_push_batch(
    beam_id: &str,
    local_dir: &Path,
    remote_dir: &str,
    paths: &[String],
    progress: BatchProgress,
) -> Result<u64> {
    use std::process::Stdio;

    let beam_id = beam_id.to_string();
    let remote_dir = remote_dir.to_string();
    let local_dir = local_dir.to_path_buf();
    let paths = paths.to_vec();

    tokio::task::spawn_blocking(move || -> Result<u64> {
        let mut child = crate::beam::tsh_command_sync()
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
            let Ok(file) = std::fs::File::open(&full_path) else {
                skipped += 1;
                continue;
            };
            let Ok((mut header, reader, len)) = counted_entry(file, &progress) else {
                skipped += 1;
                continue;
            };
            bytes_written += len;
            if let Err(e) = builder.append_data(&mut header, rel_path, reader) {
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
    progress: BatchProgress,
) -> Result<u64> {
    use std::io::{Read, Write};
    use std::process::Stdio;

    let beam_id = beam_id.to_string();
    let remote_dir = remote_dir.to_string();
    let local_dir = local_dir.to_path_buf();
    let paths = paths.to_vec();

    tokio::task::spawn_blocking(move || -> Result<u64> {
        // Use -T - to pass file list via stdin (avoids arg length limits)
        let mut child = crate::beam::tsh_command_sync()
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
        let stdout = child.stdout.take().expect("stdout not captured");
        let mut tar_data = Vec::new();
        CountingReader { inner: stdout, progress }.read_to_end(&mut tar_data)?;

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
                crate::beam::agent_remote_path(),
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
        &[crate::beam::agent_remote_path(), "--compress", remote_path, &fid, "--chunk-size", &chunk_size.to_string()],
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

/// Upload the agent binary to `remote_path` on the beam. It's staged under a unique
/// name and moved into place, so a concurrent session never sees a partial binary and
/// scp never has to overwrite an existing file.
pub async fn upload_agent(beam_id: &str, agent_path: &Path, remote_path: &str) -> Result<()> {
    let staging = format!("{remote_path}.{}.beamup-tmp", std::process::id());
    let size = agent_path.metadata()?.len();

    if size <= 50 * 1024 * 1024 {
        info!("deploying agent: {} bytes", size);
        Beam::scp_to_beam(beam_id, agent_path, &staging).await?;
        Beam::exec_shell(beam_id, &format!("chmod +x '{staging}' && mv -f '{staging}' '{remote_path}'")).await?;
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
        Beam::scp_to_beam(beam_id, &local_tmp, &format!("{staging}.gz")).await?;
        let _ = std::fs::remove_file(&local_tmp);
        Beam::exec_shell(
            beam_id,
            &format!("gunzip -f '{staging}.gz' && chmod +x '{staging}' && mv -f '{staging}' '{remote_path}'"),
        )
        .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn bar(len: u64) -> ProgressBar {
        let pb = ProgressBar::hidden();
        pb.set_length(len);
        pb
    }

    #[test]
    fn credit_is_capped_at_the_batch_total() {
        let pb = bar(100);
        let p = BatchProgress::new(Some(pb.clone()), 100);
        p.add(60);
        p.add(60); // a pulled stream's tar headers push past the file bytes
        assert_eq!(pb.position(), 100);
        p.add(1);
        assert_eq!(pb.position(), 100);
    }

    #[test]
    fn rewind_takes_back_only_this_batchs_credit() {
        let pb = bar(200);
        let a = BatchProgress::new(Some(pb.clone()), 100);
        let b = BatchProgress::new(Some(pb.clone()), 100);
        a.add(30);
        b.add(50);
        a.rewind();
        assert_eq!(pb.position(), 50, "b's bytes stay counted");
        a.add(10); // the retry starts from nothing
        assert_eq!(pb.position(), 60);
        a.rewind();
        a.rewind(); // nothing left to take back
        assert_eq!(pb.position(), 50);
    }

    #[test]
    fn complete_tops_up_the_remainder_once() {
        let pb = bar(100);
        let p = BatchProgress::new(Some(pb.clone()), 100);
        p.add(40);
        p.complete();
        assert_eq!(pb.position(), 100);
        p.complete();
        assert_eq!(pb.position(), 100);
    }

    #[test]
    fn concurrent_credit_never_exceeds_the_total() {
        let pb = bar(10_000);
        let p = BatchProgress::new(Some(pb.clone()), 10_000);
        std::thread::scope(|s| {
            for _ in 0..8 {
                let p = p.clone();
                s.spawn(move || (0..5_000).for_each(|_| p.add(1)));
            }
        });
        assert_eq!(pb.position(), 10_000);
        assert_eq!(p.credited.load(Ordering::Relaxed), 10_000);
    }

    #[test]
    fn without_a_bar_it_does_nothing() {
        let p = BatchProgress::new(None, 100);
        p.add(50);
        p.complete();
        p.rewind();
        assert_eq!(p.credited.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn counting_reader_reports_every_byte() {
        let pb = bar(1 << 20);
        let p = BatchProgress::new(Some(pb.clone()), 1 << 20);
        let data = vec![7u8; 300_000];
        let mut out = Vec::new();
        CountingReader { inner: &data[..], progress: p }.read_to_end(&mut out).unwrap();
        assert_eq!(out, data);
        assert_eq!(pb.position(), 300_000);
    }

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("beamup-test-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn archive(src: &Path, paths: &[&str], counted: Option<&BatchProgress>) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for rel in paths {
            let file = std::fs::File::open(src.join(rel)).unwrap();
            match counted {
                Some(progress) => {
                    let (mut header, reader, _) = counted_entry(file, progress).unwrap();
                    builder.append_data(&mut header, rel, reader).unwrap();
                }
                None => builder.append_file(rel, &mut { file }).unwrap(),
            }
        }
        builder.into_inner().unwrap()
    }

    /// The counted push path has to build the archive append_file did:
    /// same entries, same headers, same contents, long paths included.
    #[test]
    fn counted_entries_match_append_file() {
        let src = scratch("tar-src");
        let long = format!("{}/{}.bin", "deep-directory-name".repeat(6), "f".repeat(120));
        let files: Vec<(String, Vec<u8>)> = vec![
            ("small.txt".into(), b"hello\n".to_vec()),
            ("empty".into(), Vec::new()),
            ("nested/dir/blocky.bin".into(), (0..2048u32).map(|i| (i * 31 % 251) as u8).collect()),
            ("odd-size.bin".into(), vec![0xAB; 513]),
            (long.clone(), b"long path".to_vec()),
        ];
        for (rel, data) in &files {
            let path = src.join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, data).unwrap();
        }
        let rels: Vec<&str> = files.iter().map(|(r, _)| r.as_str()).collect();
        let total: u64 = files.iter().map(|(_, d)| d.len() as u64).sum();

        let pb = bar(total);
        let progress = BatchProgress::new(Some(pb.clone()), total);
        let counted = archive(&src, &rels, Some(&progress));
        let plain = archive(&src, &rels, None);
        assert_eq!(pb.position(), total, "every file byte was counted as it streamed");

        // Entry by entry: path, header fields and data agree.
        let entries = |bytes: &[u8]| -> Vec<(PathBuf, u64, u32, u64, u8, Vec<u8>)> {
            let mut ar = tar::Archive::new(bytes);
            ar.entries()
                .unwrap()
                .map(|e| {
                    let mut e = e.unwrap();
                    let h = e.header().clone();
                    let mut data = Vec::new();
                    e.read_to_end(&mut data).unwrap();
                    let path = e.path().unwrap().into_owned();
                    (path, h.size().unwrap(), h.mode().unwrap(), h.mtime().unwrap(), h.entry_type().as_byte(), data)
                })
                .collect()
        };
        let got = entries(&counted);
        assert_eq!(got, entries(&plain));
        assert_eq!(got.len(), files.len());
        assert!(got.iter().any(|e| e.0 == Path::new(&long)), "long path survived");

        // And it unpacks to identical files.
        let dst = scratch("tar-dst");
        tar::Archive::new(&counted[..]).unpack(&dst).unwrap();
        for (rel, data) in &files {
            assert_eq!(&std::fs::read(dst.join(rel)).unwrap(), data, "{rel}");
        }
        let _ = std::fs::remove_dir_all(&src);
        let _ = std::fs::remove_dir_all(&dst);
    }
}
