use anyhow::Result;
use beamup_protocol::codec::MessageCodec;
use beamup_protocol::messages::Message;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{stdin, stdout};
use tokio_util::codec::{FramedRead, FramedWrite};

pub struct Transport {
    reader: FramedRead<tokio::io::Stdin, MessageCodec>,
    writer: FramedWrite<tokio::io::Stdout, MessageCodec>,
}

impl Transport {
    pub fn new() -> Self {
        let reader = FramedRead::new(stdin(), MessageCodec);
        let writer = FramedWrite::new(stdout(), MessageCodec);
        Self { reader, writer }
    }

    pub async fn recv(&mut self) -> Result<Option<Message>> {
        match self.reader.next().await {
            Some(Ok(msg)) => Ok(Some(msg)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    pub async fn send(&mut self, msg: Message) -> Result<()> {
        self.writer.send(msg).await?;
        Ok(())
    }
}
