//! Serving gRPC over TLS with the server's own certificate.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::server::{Connected, TcpConnectInfo};
use tracing::warn;

/// A gRPC connection after its TLS handshake. It reports the TCP
/// addresses, so protected mode can check the peer as it does without TLS.
pub(crate) struct TlsConn {
    stream: TlsStream<TcpStream>,
    info: TcpConnectInfo,
}

impl Connected for TlsConn {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> TcpConnectInfo {
        self.info.clone()
    }
}

impl AsyncRead for TlsConn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsConn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

/// Accepts connections on `addr` and completes their TLS handshakes, for
/// `Server::serve_with_incoming`. Each handshake runs in its own task, so
/// a slow client doesn't hold up the others.
pub(crate) async fn incoming(
    addr: SocketAddr,
    acceptor: TlsAcceptor,
) -> io::Result<ReceiverStream<io::Result<TlsConn>>> {
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
            let info = stream.connect_info();
            let (acceptor, tx) = (acceptor.clone(), tx.clone());
            tokio::spawn(async move {
                if let Some(stream) = crate::server::tls_handshake(acceptor, stream, peer).await {
                    let _ = tx.send(Ok(TlsConn { stream, info })).await;
                }
            });
        }
    });
    Ok(ReceiverStream::new(rx))
}
