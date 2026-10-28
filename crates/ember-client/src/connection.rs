//! Async client connection to an ember server.
//!
//! Handles connecting, sending commands as RESP3 arrays, and reading back
//! parsed frames. Works transparently over plain TCP or TLS.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::BytesMut;
use ember_protocol::parse::parse_frame;
use ember_protocol::types::Frame;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;

#[cfg(feature = "tls")]
use crate::tls::TlsClientConfig;

/// Largest reply the client buffers (1 GiB), above the server's 512 MB
/// limit on a value. Stops unbounded memory growth if the server sends a
/// reply that never completes.
const MAX_READ_BUF: usize = 1024 * 1024 * 1024;

/// Default timeout for establishing the TCP connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Default timeout for reading a response from the server.
const READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Errors that can occur during client operations.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("connection failed: {0}")]
    Io(#[from] io::Error),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("server error: {0}")]
    Server(String),

    #[error("server disconnected")]
    Disconnected,

    #[error("authentication failed: {0}")]
    AuthFailed(String),

    #[error("connection timed out")]
    Timeout,

    #[error("response too large (exceeded {MAX_READ_BUF} bytes)")]
    ResponseTooLarge,
}

/// Underlying transport — plain TCP or (optionally) TLS.
///
/// Centralising the dispatch here keeps the `Client` logic clean regardless
/// of which features are compiled in.
pub(crate) enum Transport {
    Tcp(TcpStream),
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for Transport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Transport::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            Transport::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Transport {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Transport::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            Transport::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Transport::Tcp(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            Transport::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Transport::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            Transport::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// An async client connected to a single ember server.
///
/// Buffers reads and writes internally. Not thread-safe — use one `Client`
/// per task, or wrap in an `Arc<Mutex<_>>` if sharing is needed.
pub struct Client {
    transport: Transport,
    read_buf: BytesMut,
    write_buf: BytesMut,
    /// A read or write failed partway, so the stream may hold the rest of
    /// a reply. Every later call fails with `Disconnected` instead of
    /// returning that reply for the wrong command.
    broken: bool,
}

impl Client {
    /// Connects to an ember server over plain TCP.
    ///
    /// Times out after 5 seconds if the server is unreachable.
    pub async fn connect(host: &str, port: u16) -> Result<Self, ClientError> {
        let tcp = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect((host, port)))
            .await
            .map_err(|_| ClientError::Timeout)?
            .map_err(ClientError::Io)?;

        Ok(Self::from_transport(Transport::Tcp(tcp)))
    }

    /// Connects to an ember server with TLS.
    ///
    /// Performs the TCP connection and TLS handshake within the 5-second
    /// connect timeout.
    #[cfg(feature = "tls")]
    pub async fn connect_tls(
        host: &str,
        port: u16,
        tls: &TlsClientConfig,
    ) -> Result<Self, ClientError> {
        let stream = tokio::time::timeout(CONNECT_TIMEOUT, crate::tls::connect(host, port, tls))
            .await
            .map_err(|_| ClientError::Timeout)?
            .map_err(ClientError::Io)?;

        Ok(Self::from_transport(stream))
    }

    fn from_transport(transport: Transport) -> Self {
        Self {
            transport,
            read_buf: BytesMut::with_capacity(4096),
            write_buf: BytesMut::with_capacity(4096),
            broken: false,
        }
    }

    /// Sends a command and returns the server's RESP3 response.
    ///
    /// Arguments are serialized as a RESP3 array of bulk strings, which is
    /// the standard client-to-server wire format.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use ember_client::Client;
    /// # async fn example() -> Result<(), ember_client::ClientError> {
    /// let mut client = Client::connect("127.0.0.1", 6379).await?;
    /// let pong = client.send(&["PING"]).await?;
    /// let value = client.send(&["GET", "mykey"]).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send(&mut self, args: &[&str]) -> Result<Frame, ClientError> {
        let parts = args
            .iter()
            .map(|t| Frame::Bulk(bytes::Bytes::copy_from_slice(t.as_bytes())))
            .collect();
        self.send_frame(Frame::Array(parts)).await
    }

    /// Sends a single pre-built frame and returns the response.
    ///
    /// Used internally by typed command methods to avoid re-encoding
    /// arguments through `&[&str]`.
    pub(crate) async fn send_frame(&mut self, frame: Frame) -> Result<Frame, ClientError> {
        self.write_frame(frame).await?;
        self.read_response().await
    }

    /// Writes all frames in a single flush, then reads one response per frame.
    ///
    /// This is the core of pipelining: batching multiple commands into one
    /// syscall and reading responses sequentially afterwards.
    pub(crate) async fn send_batch(&mut self, frames: &[Frame]) -> Result<Vec<Frame>, ClientError> {
        self.write_buf.clear();
        for frame in frames {
            frame.serialize(&mut self.write_buf);
        }
        self.write_out().await?;

        let mut results = Vec::with_capacity(frames.len());
        for _ in 0..frames.len() {
            results.push(self.read_response().await?);
        }
        Ok(results)
    }

    /// Authenticates with the server using the `AUTH` command.
    ///
    /// Returns `Ok(())` on success, or `ClientError::AuthFailed` if the server
    /// rejects the password.
    pub async fn auth(&mut self, password: &str) -> Result<(), ClientError> {
        let frame = Frame::Array(vec![
            Frame::Bulk(bytes::Bytes::from_static(b"AUTH")),
            Frame::Bulk(bytes::Bytes::copy_from_slice(password.as_bytes())),
        ]);

        match self.send_frame(frame).await? {
            Frame::Simple(s) if s == "OK" => Ok(()),
            Frame::Error(e) => Err(ClientError::AuthFailed(e)),
            _ => Err(ClientError::AuthFailed(
                "unexpected response to AUTH".into(),
            )),
        }
    }

    /// Gracefully disconnects from the server.
    ///
    /// Sends `QUIT` so the server can clean up, then shuts down the transport.
    /// Errors are ignored — this is best-effort cleanup.
    pub async fn disconnect(&mut self) {
        let quit = Frame::Array(vec![Frame::Bulk(bytes::Bytes::from_static(b"QUIT"))]);
        self.write_buf.clear();
        quit.serialize(&mut self.write_buf);
        let _ = self.transport.write_all(&self.write_buf).await;
        let _ = self.transport.flush().await;
        let _ = self.transport.shutdown().await;
    }

    /// Serializes and writes a single frame without waiting for a response.
    ///
    /// Used by [`Subscriber`] to send SUBSCRIBE/UNSUBSCRIBE frames before
    /// draining the confirmation responses separately.
    pub(crate) async fn write_frame(&mut self, frame: Frame) -> Result<(), ClientError> {
        self.write_buf.clear();
        frame.serialize(&mut self.write_buf);
        self.write_out().await
    }

    /// Writes and flushes `write_buf`. A failure marks the connection broken.
    async fn write_out(&mut self) -> Result<(), ClientError> {
        if self.broken {
            return Err(ClientError::Disconnected);
        }
        let result = async {
            self.transport.write_all(&self.write_buf).await?;
            self.transport.flush().await
        }
        .await;
        if result.is_err() {
            self.broken = true;
        }
        Ok(result?)
    }

    /// Whether an earlier failure left this connection unusable. Reconnect
    /// to keep going.
    pub fn is_broken(&self) -> bool {
        self.broken
    }

    /// Reads a complete RESP3 frame from the server.
    pub(crate) async fn read_response(&mut self) -> Result<Frame, ClientError> {
        self.read_frame(Some(READ_TIMEOUT)).await
    }

    /// Reads the next frame with no time limit, for a subscriber waiting for
    /// messages on a quiet channel.
    pub(crate) async fn read_message(&mut self) -> Result<Frame, ClientError> {
        self.read_frame(None).await
    }

    /// Reads one complete frame. Any failure marks the connection broken,
    /// since part of the reply may still be on its way.
    async fn read_frame(&mut self, timeout: Option<Duration>) -> Result<Frame, ClientError> {
        if self.broken {
            return Err(ClientError::Disconnected);
        }
        let result = self.try_read_frame(timeout).await;
        if result.is_err() {
            self.broken = true;
        }
        result
    }

    async fn try_read_frame(&mut self, timeout: Option<Duration>) -> Result<Frame, ClientError> {
        loop {
            if !self.read_buf.is_empty() {
                match parse_frame(&self.read_buf) {
                    Ok(Some((frame, consumed))) => {
                        let _ = self.read_buf.split_to(consumed);
                        return Ok(frame);
                    }
                    Ok(None) => {
                        // incomplete frame — need more data
                    }
                    Err(e) => {
                        return Err(ClientError::Protocol(e.to_string()));
                    }
                }
            }

            if self.read_buf.len() >= MAX_READ_BUF {
                return Err(ClientError::ResponseTooLarge);
            }

            let read = self.transport.read_buf(&mut self.read_buf);
            let read_result = match timeout {
                Some(limit) => tokio::time::timeout(limit, read)
                    .await
                    .map_err(|_| ClientError::Timeout)?,
                None => read.await,
            };

            match read_result {
                Ok(0) => return Err(ClientError::Disconnected),
                Ok(_) => {} // got data, loop back to try parsing
                Err(e) => return Err(ClientError::Io(e)),
            }
        }
    }
}
