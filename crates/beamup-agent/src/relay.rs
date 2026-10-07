use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::time::Instant;

/// How long to wait for a just-started `--listen` agent to create its socket.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Splice stdin/stdout onto a `--listen` agent's socket. The CLI's `tsh beams exec`
/// pipe ends with the connection; the agent behind the socket does not.
pub async fn run(socket: &Path) -> Result<()> {
    let stream = connect(socket).await?;
    let (sock_reader, sock_writer) = stream.into_split();

    // Either direction closing ends the relay: stdin EOF means the client went away,
    // socket EOF means the agent ended (or handed off) this session.
    tokio::select! {
        r = pump(tokio::io::stdin(), sock_writer) => r.context("relay stdin -> agent")?,
        r = pump(sock_reader, tokio::io::stdout()) => r.context("relay agent -> stdout")?,
    }
    Ok(())
}

async fn connect(socket: &Path) -> Result<UnixStream> {
    let deadline = Instant::now() + CONNECT_TIMEOUT;
    loop {
        match UnixStream::connect(socket).await {
            Ok(stream) => return Ok(stream),
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => {
                return Err(e).with_context(|| format!("failed to connect to agent at {}", socket.display()))
            }
        }
    }
}

/// Copy until EOF, flushing every write so frames aren't held back in a buffer.
async fn pump<R, W>(mut reader: R, mut writer: W) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            return writer.shutdown().await;
        }
        writer.write_all(&buf[..n]).await?;
        writer.flush().await?;
    }
}
