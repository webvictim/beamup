use anyhow::Result;
use beamup_protocol::codec::MessageCodec;
use beamup_protocol::messages::Message;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{stdin, stdout, AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::error;

pub struct Transport {
    pub rx: mpsc::Receiver<Message>,
    pub tx: mpsc::Sender<Message>,
    tasks: [JoinHandle<()>; 2],
}

impl Transport {
    /// Transport over our own stdin/stdout, as spawned directly by `tsh beams exec`.
    pub fn stdio() -> Self {
        Self::new(stdin(), stdout())
    }

    pub fn new<R, W>(reader: R, writer: W) -> Self
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (incoming_tx, incoming_rx) = mpsc::channel::<Message>(256);
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<Message>(256);

        let writer_tx = outgoing_tx.clone();

        // Reader task: reads frames, auto-replies to Pings, forwards everything else
        let read_task = tokio::spawn(async move {
            let mut reader = FramedRead::new(reader, MessageCodec);
            while let Some(result) = reader.next().await {
                match result {
                    Ok(Message::Ping) => {
                        if writer_tx.send(Message::Pong).await.is_err() {
                            break;
                        }
                    }
                    Ok(msg) => {
                        if incoming_tx.send(msg).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        error!("transport read error: {e}");
                        break;
                    }
                }
            }
        });

        // Writer task: drains outgoing channel into the writer
        let write_task = tokio::spawn(async move {
            let mut writer = FramedWrite::new(writer, MessageCodec);
            while let Some(msg) = outgoing_rx.recv().await {
                if let Err(e) = writer.send(msg).await {
                    error!("transport write error: {e}");
                    break;
                }
            }
        });

        Self {
            rx: incoming_rx,
            tx: outgoing_tx,
            tasks: [read_task, write_task],
        }
    }

    pub async fn recv(&mut self) -> Result<Option<Message>> {
        Ok(self.rx.recv().await)
    }

    pub async fn send(&self, msg: Message) -> Result<()> {
        self.tx
            .send(msg)
            .await
            .map_err(|_| anyhow::anyhow!("transport closed"))?;
        Ok(())
    }
}

impl Drop for Transport {
    // The reader answers Pings on its own, so a dropped session would otherwise keep
    // looking alive to its client. Aborting both tasks drops the stream halves, which
    // closes the connection and lets a superseded client see EOF.
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
