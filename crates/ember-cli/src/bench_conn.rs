//! Lightweight pipelining TCP connection for benchmarks.
//!
//! Optimized for throughput: pre-serializes a command once, then writes
//! it N times per pipeline batch and reads N responses. Uses a larger
//! read buffer than the regular connection to avoid syscall overhead.

use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use ember_protocol::parse::parse_frame;
use ember_protocol::types::Frame;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Read buffer size for benchmark connections (256 KiB).
const READ_BUF_SIZE: usize = 256 * 1024;

/// A TCP connection tuned for pipelined benchmark workloads.
pub struct BenchConnection {
    stream: TcpStream,
    read_buf: BytesMut,
    /// Pre-serialized command bytes, ready to write N times.
    command_bytes: Bytes,
}

impl BenchConnection {
    /// Connects to the server.
    pub async fn connect(host: &str, port: u16) -> std::io::Result<Self> {
        let stream = TcpStream::connect((host, port)).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            read_buf: BytesMut::with_capacity(READ_BUF_SIZE),
            command_bytes: Bytes::new(),
        })
    }

    /// Authenticates with the server using AUTH.
    pub async fn authenticate(&mut self, password: &str) -> Result<(), String> {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"AUTH")),
            Frame::Bulk(Bytes::from(password.to_string())),
        ]);
        let mut buf = BytesMut::new();
        frame.serialize(&mut buf);

        self.stream
            .write_all(&buf)
            .await
            .map_err(|e| format!("write: {e}"))?;
        self.stream
            .flush()
            .await
            .map_err(|e| format!("flush: {e}"))?;

        self.read_one_response()
            .await
            .map_err(|e| format!("read: {e}"))?;
        Ok(())
    }

    /// Sets the pre-serialized command to pipeline.
    ///
    /// Call this once before `send_pipeline`. The command bytes are cloned
    /// from a `Frame` — the serialization cost is paid once, not per batch.
    pub fn set_command(&mut self, frame: &Frame) {
        let mut buf = BytesMut::new();
        frame.serialize(&mut buf);
        self.command_bytes = buf.freeze();
    }

    /// Writes the pre-serialized command `count` times, flushes, then
    /// reads `count` responses. Returns the elapsed wall-clock time for
    /// the entire batch.
    pub async fn send_pipeline(&mut self, count: usize) -> Result<Duration, std::io::Error> {
        let start = Instant::now();

        // write the command `count` times
        for _ in 0..count {
            self.stream.write_all(&self.command_bytes).await?;
        }
        self.stream.flush().await?;

        // read `count` responses
        for _ in 0..count {
            self.read_one_response().await?;
        }

        Ok(start.elapsed())
    }

    /// Reads and discards a single RESP3 response.
    async fn read_one_response(&mut self) -> Result<(), std::io::Error> {
        loop {
            if !self.read_buf.is_empty() {
                match parse_frame(&self.read_buf) {
                    Ok(Some((_frame, consumed))) => {
                        let _ = self.read_buf.split_to(consumed);
                        return Ok(());
                    }
                    Ok(None) => {
                        // incomplete, need more data
                    }
                    Err(e) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            e.to_string(),
                        ));
                    }
                }
            }

            let n = self.stream.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "server disconnected",
                ));
            }
        }
    }
}

/// Builds a RESP3 array frame from string tokens.
///
/// Used to construct benchmark commands like `["SET", "key:123", "value"]`.
#[cfg(test)]
fn build_command(tokens: &[&str]) -> Frame {
    Frame::Array(
        tokens
            .iter()
            .map(|t| Frame::Bulk(Bytes::from(t.to_string())))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_command_ping() {
        let frame = build_command(&["PING"]);
        let mut buf = BytesMut::new();
        frame.serialize(&mut buf);
        assert_eq!(&buf[..], b"*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn build_command_set() {
        let frame = build_command(&["SET", "key", "value"]);
        let mut buf = BytesMut::new();
        frame.serialize(&mut buf);
        assert_eq!(&buf[..], b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
    }

    #[test]
    fn build_command_get() {
        let frame = build_command(&["GET", "mykey"]);
        let mut buf = BytesMut::new();
        frame.serialize(&mut buf);
        assert_eq!(&buf[..], b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n");
    }
}
