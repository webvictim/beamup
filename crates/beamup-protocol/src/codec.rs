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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::{ManifestEntry, SyncEntry};

    fn round_trip(msg: Message) -> Message {
        let mut codec = MessageCodec;
        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).unwrap();
        codec.decode(&mut buf).unwrap().unwrap()
    }

    #[test]
    fn round_trip_ping_pong() {
        match round_trip(Message::Ping) {
            Message::Ping => {}
            other => panic!("expected Ping, got {other:?}"),
        }
        match round_trip(Message::Pong) {
            Message::Pong => {}
            other => panic!("expected Pong, got {other:?}"),
        }
    }

    #[test]
    fn round_trip_hello() {
        let msg = Message::Hello {
            version: 2,
            session_id: "abc123".to_string(),
        };
        match round_trip(msg) {
            Message::Hello { version, session_id } => {
                assert_eq!(version, 2);
                assert_eq!(session_id, "abc123");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn round_trip_file_content() {
        let data = vec![1, 2, 3, 4, 5];
        let msg = Message::FileContent {
            path: "src/main.rs".to_string(),
            hash: 12345,
            data: data.clone(),
        };
        match round_trip(msg) {
            Message::FileContent { path, hash, data: d } => {
                assert_eq!(path, "src/main.rs");
                assert_eq!(hash, 12345);
                assert_eq!(d, data);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn round_trip_sync_plan() {
        let msg = Message::SyncPlan {
            to_push: vec![SyncEntry { path: "a.txt".into(), hash: 1, size: 100 }],
            to_pull: vec![SyncEntry { path: "b.txt".into(), hash: 2, size: 200 }],
        };
        match round_trip(msg) {
            Message::SyncPlan { to_push, to_pull } => {
                assert_eq!(to_push.len(), 1);
                assert_eq!(to_pull.len(), 1);
                assert_eq!(to_push[0].path, "a.txt");
                assert_eq!(to_pull[0].size, 200);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn round_trip_manifest() {
        let msg = Message::FileManifest {
            entries: vec![
                ManifestEntry { path: "src".into(), hash: 0, mtime: 0, size: 0, is_dir: true },
                ManifestEntry { path: "src/lib.rs".into(), hash: 999, mtime: 1000, size: 500, is_dir: false },
            ],
        };
        match round_trip(msg) {
            Message::FileManifest { entries } => {
                assert_eq!(entries.len(), 2);
                assert!(entries[0].is_dir);
                assert!(!entries[1].is_dir);
                assert_eq!(entries[1].hash, 999);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn partial_buffer_returns_none() {
        let mut codec = MessageCodec;
        let mut buf = BytesMut::new();

        // Encode a message
        codec.encode(Message::Ping, &mut buf).unwrap();

        // Only give the decoder part of the buffer
        let full_len = buf.len();
        let mut partial = buf.split_to(full_len - 1);
        assert_eq!(codec.decode(&mut partial).unwrap(), None);
    }

    #[test]
    fn empty_buffer_returns_none() {
        let mut codec = MessageCodec;
        let mut buf = BytesMut::new();
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
    }

    #[test]
    fn too_short_for_length_returns_none() {
        let mut codec = MessageCodec;
        let mut buf = BytesMut::from(&[0u8, 0, 0][..]);
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
    }

    #[test]
    fn multiple_messages_in_buffer() {
        let mut codec = MessageCodec;
        let mut buf = BytesMut::new();

        codec.encode(Message::Ping, &mut buf).unwrap();
        codec.encode(Message::Pong, &mut buf).unwrap();
        codec.encode(Message::Shutdown, &mut buf).unwrap();

        match codec.decode(&mut buf).unwrap().unwrap() {
            Message::Ping => {}
            other => panic!("expected Ping, got {other:?}"),
        }
        match codec.decode(&mut buf).unwrap().unwrap() {
            Message::Pong => {}
            other => panic!("expected Pong, got {other:?}"),
        }
        match codec.decode(&mut buf).unwrap().unwrap() {
            Message::Shutdown => {}
            other => panic!("expected Shutdown, got {other:?}"),
        }
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
    }

    #[test]
    fn oversized_frame_rejected() {
        let mut codec = MessageCodec;
        let mut buf = BytesMut::new();
        // Write a length header claiming 200MB
        buf.put_u32(200 * 1024 * 1024);
        buf.extend_from_slice(&[0u8; 100]);

        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }
}
