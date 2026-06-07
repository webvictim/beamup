use anyhow::Result;
use beamup_protocol::codec::MessageCodec;
use beamup_protocol::messages::Message;
use futures_util::{SinkExt, StreamExt};
use tokio::process::{ChildStdin, ChildStdout};
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::error;

pub struct Transport {
    pub rx: mpsc::Receiver<Message>,
    pub tx: mpsc::Sender<Message>,
}

impl Transport {
    pub fn new(child_stdout: ChildStdout, child_stdin: ChildStdin) -> Self {
        let (incoming_tx, incoming_rx) = mpsc::channel::<Message>(256);
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<Message>(256);

        let writer_tx = outgoing_tx.clone();

        // Reader task: reads from child stdout, auto-replies to Pings, forwards everything else
        tokio::spawn(async move {
            let mut reader = FramedRead::new(child_stdout, MessageCodec);
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

        // Writer task: drains outgoing channel into child stdin
        tokio::spawn(async move {
            let mut writer = FramedWrite::new(child_stdin, MessageCodec);
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
