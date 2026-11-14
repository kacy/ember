//! The gRPC listener: accepts connections within `maxclients`, and runs
//! the TLS handshake when TLS is configured.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::server::{Connected, TcpConnectInfo};
use tracing::warn;

/// An accepted gRPC connection. It holds one of the `maxclients` places
/// while open, and reports its TCP addresses so protected mode can check
/// the peer.
pub(crate) struct Conn {
    io: Io,
    info: TcpConnectInfo,
    _permit: OwnedSemaphorePermit,
}

enum Io {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl Connected for Conn {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> TcpConnectInfo {
        self.info.clone()
    }
}

impl AsyncRead for Conn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut self.get_mut().io {
            Io::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Io::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Conn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().io {
            Io::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Io::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().io {
            Io::Plain(s) => Pin::new(s).poll_flush(cx),
            Io::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().io {
            Io::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Io::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Accepts connections on `addr` for `Server::serve_with_incoming`.
///
/// Each connection takes a place from `clients`, the semaphore RESP
/// connections share, and is closed when none is left. With `tls`, each
/// handshake runs in its own task, so a slow client doesn't hold up the
/// others.
pub(crate) async fn incoming(
    addr: SocketAddr,
    tls: Option<TlsAcceptor>,
    clients: Arc<Semaphore>,
) -> io::Result<ReceiverStream<io::Result<Conn>>> {
    let listener = TcpListener::bind(addr).await?;
    let (tx, rx) = mpsc::channel(64);
    tokio::spawn(async move {
        while !tx.is_closed() {
            let (stream, peer) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    warn!("gRPC accept error: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let Ok(permit) = Arc::clone(&clients).try_acquire_owned() else {
                warn!("connection limit reached, dropping gRPC connection from {peer}");
                continue;
            };
            let _ = stream.set_nodelay(true);
            let keepalive = socket2::TcpKeepalive::new().with_time(Duration::from_secs(60));
            let _ = socket2::SockRef::from(&stream).set_tcp_keepalive(&keepalive);
            let info = stream.connect_info();
            let Some(acceptor) = tls.clone() else {
                let conn = Conn {
                    io: Io::Plain(stream),
                    info,
                    _permit: permit,
                };
                let _ = tx.send(Ok(conn)).await;
                continue;
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                if let Some(stream) = crate::server::tls_handshake(acceptor, stream, peer).await {
                    let conn = Conn {
                        io: Io::Tls(Box::new(stream)),
                        info,
                        _permit: permit,
                    };
                    let _ = tx.send(Ok(conn)).await;
                }
            });
        }
    });
    Ok(ReceiverStream::new(rx))
}
