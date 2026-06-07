use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::messages::Message;

const MAX_FRAME_SIZE: u32 = 128 * 1024 * 1024; // 128 MB

pub struct MessageCodec;

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let length = u32::from_be_bytes([src[0], src[1], src[2], src[3]]);

        if length > MAX_FRAME_SIZE {
            anyhow::bail!("frame too large: {length} bytes (max {MAX_FRAME_SIZE})");
        }

        let total = 4 + length as usize;
        if src.len() < total {
            src.reserve(total - src.len());
            return Ok(None);
        }

        src.advance(4);
        let payload = src.split_to(length as usize);
        let msg = Message::decode(&payload)?;
        Ok(Some(msg))
    }
}

impl Encoder<Message> for MessageCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let payload = item.encode()?;
        let length = payload.len() as u32;

        if length > MAX_FRAME_SIZE {
            anyhow::bail!("message too large to encode: {length} bytes");
        }

        dst.reserve(4 + payload.len());
        dst.put_u32(length);
        dst.extend_from_slice(&payload);
        Ok(())
    }
}
