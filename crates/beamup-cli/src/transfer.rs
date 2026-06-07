use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;
use tracing::debug;

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
    pub fn new(beam_id: String, local_dir: PathBuf, remote_dir: String, concurrency: usize) -> Self {
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

    /// Push a local file to the beam via scp (non-blocking, returns immediately)
    pub fn push(&self, relative_path: String) -> JoinHandle<()> {
        let sem = self.semaphore.clone();
        let tx = self.results_tx.clone();
        let beam_id = self.beam_id.clone();
        let local_path = self.local_dir.join(&relative_path);
        let remote_path = format!("{}/{}", self.remote_dir, relative_path);

        tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            debug!("scp push: {relative_path}");

            let result = Beam::scp_to_beam(&beam_id, &local_path, &remote_path).await;

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

    /// Pull a file from the beam to local via scp (non-blocking, returns immediately)
    pub fn pull(&self, relative_path: String) -> JoinHandle<()> {
        let sem = self.semaphore.clone();
        let tx = self.results_tx.clone();
        let beam_id = self.beam_id.clone();
        let local_path = self.local_dir.join(&relative_path);
        let remote_path = format!("{}/{}", self.remote_dir, relative_path);

        tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            debug!("scp pull: {relative_path}");

            let result = Beam::scp_from_beam(&beam_id, &remote_path, &local_path).await;

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

    /// Push multiple files in parallel (bounded by semaphore). Waits for all to complete.
    pub async fn push_batch(&mut self, paths: Vec<String>) -> Vec<TransferResult> {
        let count = paths.len();
        let handles: Vec<_> = paths.into_iter().map(|p| self.push(p)).collect();

        let mut results = Vec::with_capacity(count);
        for handle in handles {
            let _ = handle.await;
        }

        // Drain results channel
        while let Ok(result) = self.results_rx.try_recv() {
            results.push(result);
        }

        results
    }

    /// Pull multiple files in parallel (bounded by semaphore). Waits for all to complete.
    pub async fn pull_batch(&mut self, paths: Vec<String>) -> Vec<TransferResult> {
        let count = paths.len();
        let handles: Vec<_> = paths.into_iter().map(|p| self.pull(p)).collect();

        let mut results = Vec::with_capacity(count);
        for handle in handles {
            let _ = handle.await;
        }

        // Drain results channel
        while let Ok(result) = self.results_rx.try_recv() {
            results.push(result);
        }

        results
    }
}
