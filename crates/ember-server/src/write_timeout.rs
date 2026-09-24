//! A stream wrapper that gives up on clients that stop reading.
//!
//! Once a client's socket buffer is full, each write waits until the
//! client reads again. A client that never does would hold its task and
//! its pending replies forever. [`WriteTimeout`] fails a write with
//! `TimedOut` when it makes no progress for the timeout, which closes the
//! connection.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Sleep;

pub struct WriteTimeout<S> {
    inner: S,
    timeout: Duration,
    /// Runs while a write is blocked. Cleared as soon as one goes through.
    deadline: Option<Pin<Box<Sleep>>>,
}

impl<S> WriteTimeout<S> {
    pub fn new(inner: S, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            deadline: None,
        }
    }

    /// Passes through a write result, and while the write is blocked,
    /// turns it into a `TimedOut` error once the deadline passes.
    fn check<T>(&mut self, cx: &mut Context<'_>, poll: Poll<io::Result<T>>) -> Poll<io::Result<T>> {
        if poll.is_ready() {
            self.deadline = None;
            return poll;
        }
        let timeout = self.timeout;
        let deadline = self
            .deadline
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)));
        match deadline.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "client stopped reading replies",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for WriteTimeout<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for WriteTimeout<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let poll = Pin::new(&mut self.inner).poll_write(cx, buf);
        self.check(cx, poll)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let poll = Pin::new(&mut self.inner).poll_flush(cx);
        self.check(cx, poll)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let poll = Pin::new(&mut self.inner).poll_shutdown(cx);
        self.check(cx, poll)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test(start_paused = true)]
    async fn write_fails_when_peer_stops_reading() {
        // the peer never reads, so the 64-byte pipe fills up
        let (client, _peer) = tokio::io::duplex(64);
        let mut stream = WriteTimeout::new(client, Duration::from_secs(5));

        let err = stream.write_all(&[0; 1024]).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test(start_paused = true)]
    async fn slow_reader_that_keeps_up_is_fine() {
        let (client, mut peer) = tokio::io::duplex(64);
        let mut stream = WriteTimeout::new(client, Duration::from_secs(5));

        let reader = tokio::spawn(async move {
            let mut buf = [0; 64];
            let mut total = 0;
            while total < 1024 {
                tokio::time::sleep(Duration::from_secs(1)).await;
                total += tokio::io::AsyncReadExt::read(&mut peer, &mut buf)
                    .await
                    .unwrap();
            }
        });

        // takes 16 seconds in total, but no single write waits 5
        stream.write_all(&[0; 1024]).await.unwrap();
        reader.await.unwrap();
    }
}
