//! Async TCP connection to an ember server.
//!
//! Handles connecting, sending commands as RESP3 arrays,
//! and reading back parsed frames.

use bytes::BytesMut;
use ember_protocol::parse::parse_frame;
use ember_protocol::types::Frame;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Errors that can occur during connection operations.
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("connection failed: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("server disconnected")]
    Disconnected,

    #[error("authentication failed: {0}")]
    AuthFailed(String),
}

/// A TCP connection to an ember server with read/write buffering.
pub struct Connection {
    stream: TcpStream,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

impl Connection {
    /// Connects to an ember server at the given host and port.
    pub async fn connect(host: &str, port: u16) -> Result<Self, ConnectionError> {
        let stream = TcpStream::connect((host, port)).await?;
        Ok(Self {
            stream,
            read_buf: BytesMut::with_capacity(4096),
            write_buf: BytesMut::with_capacity(4096),
        })
    }

    /// Sends a command (as a list of string tokens) and reads the response.
    ///
    /// Tokens are serialized as a RESP3 array of bulk strings, which is
    /// the standard client → server wire format.
    pub async fn send_command(&mut self, tokens: &[String]) -> Result<Frame, ConnectionError> {
        // build a RESP3 array of bulk strings
        let parts: Vec<Frame> = tokens
            .iter()
            .map(|t| Frame::Bulk(bytes::Bytes::from(t.clone())))
            .collect();
        let frame = Frame::Array(parts);

        // serialize into the write buffer and flush
        self.write_buf.clear();
        frame.serialize(&mut self.write_buf);
        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;

        // read the response
        self.read_response().await
    }

    /// Authenticates with the server using the AUTH command.
    pub async fn authenticate(&mut self, password: &str) -> Result<(), ConnectionError> {
        let tokens = vec!["AUTH".to_string(), password.to_string()];
        let response = self.send_command(&tokens).await?;

        match &response {
            Frame::Simple(s) if s == "OK" => Ok(()),
            Frame::Error(e) => Err(ConnectionError::AuthFailed(e.clone())),
            _ => Err(ConnectionError::AuthFailed(
                "unexpected response to AUTH".into(),
            )),
        }
    }

    /// Reads a complete RESP3 frame from the server.
    async fn read_response(&mut self) -> Result<Frame, ConnectionError> {
        loop {
            // try to parse a frame from what we have
            if !self.read_buf.is_empty() {
                match parse_frame(&self.read_buf) {
                    Ok(Some((frame, consumed))) => {
                        // remove the consumed bytes from the buffer
                        let _ = self.read_buf.split_to(consumed);
                        return Ok(frame);
                    }
                    Ok(None) => {
                        // incomplete — need more data
                    }
                    Err(e) => {
                        return Err(ConnectionError::Protocol(e.to_string()));
                    }
                }
            }

            // read more data from the socket
            let n = self.stream.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                return Err(ConnectionError::Disconnected);
            }
        }
    }
}
