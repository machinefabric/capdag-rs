#[cfg(unix)]
pub use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
#[cfg(unix)]
pub use tokio::net::UnixStream;

/// A cross-platform AF_UNIX listener.
///
/// The macOS XPC service listens on a Unix socket the engine connects to; on
/// Linux/Windows the cartridge host daemon plays that role. `accept` yields the
/// same [`UnixStream`] the engine connects with (`UnixStream::connect`), so the
/// listener side and the connector side share one stream type on both platforms.
#[cfg(unix)]
pub struct LocalListener {
    inner: tokio::net::UnixListener,
}

#[cfg(unix)]
impl LocalListener {
    /// Bind a listening AF_UNIX socket at `path`. The path must not already
    /// exist — callers own relay-socket hygiene (removing a stale file) so a
    /// genuine bind conflict surfaces rather than being papered over here.
    pub fn bind(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        Ok(Self {
            inner: tokio::net::UnixListener::bind(path)?,
        })
    }

    /// Await the next inbound connection.
    pub async fn accept(&self) -> std::io::Result<UnixStream> {
        let (stream, _addr) = self.inner.accept().await?;
        Ok(stream)
    }
}

#[cfg(windows)]
mod windows {
    use socket2::{Domain, SockAddr, Socket, Type};
    use std::collections::VecDeque;
    use std::future::Future;
    use std::io::{self, Read, Write};
    use std::path::{Path, PathBuf};
    use std::pin::Pin;
    use std::sync::mpsc;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tokio::sync::oneshot;

    pub struct UnixStream {
        read: OwnedReadHalf,
        write: OwnedWriteHalf,
    }

    pub struct OwnedReadHalf {
        rx: tokio::sync::mpsc::UnboundedReceiver<io::Result<Vec<u8>>>,
        pending: VecDeque<u8>,
    }

    pub struct OwnedWriteHalf {
        tx: mpsc::Sender<WriteCommand>,
        pending_write: Option<oneshot::Receiver<io::Result<usize>>>,
        pending_flush: Option<oneshot::Receiver<io::Result<()>>>,
        pending_shutdown: Option<oneshot::Receiver<io::Result<()>>>,
    }

    enum WriteCommand {
        Write(Vec<u8>, oneshot::Sender<io::Result<usize>>),
        Flush(oneshot::Sender<io::Result<()>>),
        Shutdown(oneshot::Sender<io::Result<()>>),
    }

    impl UnixStream {
        pub async fn connect(path: impl AsRef<Path>) -> io::Result<Self> {
            let path = path.as_ref().to_owned();
            tokio::task::spawn_blocking(move || connect_blocking(&path))
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        }

        pub fn pair() -> io::Result<(Self, Self)> {
            let path = unique_socket_path()?;
            let addr = SockAddr::unix(&path)?;
            let listener = Socket::new(Domain::UNIX, Type::STREAM, None)?;

            if path.exists() {
                std::fs::remove_file(&path)?;
            }
            listener.bind(&addr)?;
            listener.listen(1)?;

            let client = Socket::new(Domain::UNIX, Type::STREAM, None)?;
            client.connect(&addr)?;
            let (server, _) = listener.accept()?;

            std::fs::remove_file(&path)?;

            Ok((Self::from_socket(client), Self::from_socket(server)))
        }

        pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
            (self.read, self.write)
        }

        fn from_socket(socket: Socket) -> Self {
            let read_socket = socket
                .try_clone()
                .expect("AF_UNIX stream clone failed during split");
            let write_socket = socket;
            let (read_tx, read_rx) = tokio::sync::mpsc::unbounded_channel();
            std::thread::spawn(move || read_loop(read_socket, read_tx));

            let (write_tx, write_rx) = mpsc::channel();
            std::thread::spawn(move || write_loop(write_socket, write_rx));

            Self {
                read: OwnedReadHalf {
                    rx: read_rx,
                    pending: VecDeque::new(),
                },
                write: OwnedWriteHalf {
                    tx: write_tx,
                    pending_write: None,
                    pending_flush: None,
                    pending_shutdown: None,
                },
            }
        }
    }

    fn connect_blocking(path: &Path) -> io::Result<UnixStream> {
        let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
        let addr = SockAddr::unix(path)?;
        socket.connect(&addr)?;
        Ok(UnixStream::from_socket(socket))
    }

    /// AF_UNIX listener (Windows 10+ supports AF_UNIX). Mirrors the Unix
    /// `LocalListener`: the engine connects to this socket via `UnixStream`.
    /// `accept` blocks, so it runs on a blocking task.
    pub struct LocalListener {
        listener: std::sync::Arc<Socket>,
    }

    impl LocalListener {
        pub fn bind(path: impl AsRef<Path>) -> io::Result<Self> {
            let addr = SockAddr::unix(path.as_ref())?;
            let listener = Socket::new(Domain::UNIX, Type::STREAM, None)?;
            listener.bind(&addr)?;
            listener.listen(128)?;
            Ok(Self {
                listener: std::sync::Arc::new(listener),
            })
        }

        pub async fn accept(&self) -> io::Result<UnixStream> {
            let listener = self.listener.clone();
            let (socket, _addr) = tokio::task::spawn_blocking(move || listener.accept())
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;
            Ok(UnixStream::from_socket(socket))
        }
    }

    fn unique_socket_path() -> io::Result<PathBuf> {
        let mut path = std::env::temp_dir();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            .as_nanos();
        path.push(format!(
            "machinefabric-{}-{}.sock",
            std::process::id(),
            nonce
        ));
        Ok(path)
    }

    fn read_loop(mut socket: Socket, tx: tokio::sync::mpsc::UnboundedSender<io::Result<Vec<u8>>>) {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            match socket.read(&mut buf) {
                Ok(0) => {
                    let _ = tx.send(Ok(Vec::new()));
                    break;
                }
                Ok(n) => {
                    if tx.send(Ok(buf[..n].to_vec())).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e));
                    break;
                }
            }
        }
    }

    /// The write direction's thread, and the one place it ends.
    ///
    /// The peer sees end-of-file when the write direction is SHUT DOWN, as on
    /// Unix, where tokio's `OwnedWriteHalf` does it on drop and on
    /// `poll_shutdown`. Here the read direction runs on a thread of its own
    /// holding a clone of the socket, blocked in `read`, so dropping the write
    /// half closed nothing: the socket stayed open, the peer never read EOF,
    /// and anything waiting for the other side to go away waited for ever. On
    /// Windows a cartridge that exited was never noticed —
    /// `test417_route_req_to_correct_cartridge` hung until the suite was killed
    /// and `test1255_app_exit_suppresses_err_frames` failed.
    ///
    /// So the thread shuts the direction down when asked and when its channel
    /// closes — that is, when the half is dropped — after every write queued
    /// before it has gone out.
    fn write_loop(mut socket: Socket, rx: mpsc::Receiver<WriteCommand>) {
        for command in rx {
            match command {
                WriteCommand::Write(bytes, ack) => {
                    let result = socket.write(&bytes);
                    let _ = ack.send(result);
                }
                WriteCommand::Flush(ack) => {
                    let result = socket.flush();
                    let _ = ack.send(result);
                }
                WriteCommand::Shutdown(ack) => {
                    let result = socket.shutdown(std::net::Shutdown::Write);
                    let _ = ack.send(result);
                    return;
                }
            }
        }
        // Dropped without a shutdown. As tokio's drop does, and for the same
        // reason: nobody is left to hear that a peer which already went away
        // cannot be told.
        let _ = socket.shutdown(std::net::Shutdown::Write);
    }

    impl AsyncRead for OwnedReadHalf {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            while buf.remaining() > 0 {
                if let Some(byte) = self.pending.pop_front() {
                    buf.put_slice(&[byte]);
                } else {
                    break;
                }
            }

            if buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }

            match Pin::new(&mut self.rx).poll_recv(cx) {
                Poll::Pending => {
                    if buf.filled().is_empty() {
                        Poll::Pending
                    } else {
                        Poll::Ready(Ok(()))
                    }
                }
                Poll::Ready(None) => Poll::Ready(Ok(())),
                Poll::Ready(Some(Ok(bytes))) => {
                    if bytes.is_empty() {
                        return Poll::Ready(Ok(()));
                    }
                    let take = bytes.len().min(buf.remaining());
                    buf.put_slice(&bytes[..take]);
                    self.pending.extend(bytes[take..].iter().copied());
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Some(Err(e))) => Poll::Ready(Err(e)),
            }
        }
    }

    impl AsyncRead for UnixStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.read).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for OwnedWriteHalf {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.pending_write.is_none() {
                let (tx, rx) = oneshot::channel();
                self.tx
                    .send(WriteCommand::Write(buf.to_vec(), tx))
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::BrokenPipe, "AF_UNIX writer closed")
                    })?;
                self.pending_write = Some(rx);
            }

            let rx = self.pending_write.as_mut().expect("pending write set");
            match Pin::new(rx).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    self.pending_write = None;
                    Poll::Ready(result.unwrap_or_else(|_| {
                        Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "AF_UNIX writer thread stopped",
                        ))
                    }))
                }
            }
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            if self.pending_flush.is_none() {
                let (tx, rx) = oneshot::channel();
                self.tx.send(WriteCommand::Flush(tx)).map_err(|_| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "AF_UNIX writer closed")
                })?;
                self.pending_flush = Some(rx);
            }

            let rx = self.pending_flush.as_mut().expect("pending flush set");
            match Pin::new(rx).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    self.pending_flush = None;
                    Poll::Ready(result.unwrap_or_else(|_| {
                        Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "AF_UNIX writer thread stopped",
                        ))
                    }))
                }
            }
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            if self.pending_shutdown.is_none() {
                let (tx, rx) = oneshot::channel();
                self.tx.send(WriteCommand::Shutdown(tx)).map_err(|_| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "AF_UNIX writer closed")
                })?;
                self.pending_shutdown = Some(rx);
            }

            let rx = self.pending_shutdown.as_mut().expect("pending shutdown set");
            match Pin::new(rx).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    self.pending_shutdown = None;
                    Poll::Ready(result.unwrap_or_else(|_| {
                        Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "AF_UNIX writer thread stopped",
                        ))
                    }))
                }
            }
        }
    }

    impl AsyncWrite for UnixStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.write).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.write).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.write).poll_shutdown(cx)
        }
    }
}

#[cfg(windows)]
pub use windows::{LocalListener, OwnedReadHalf, OwnedWriteHalf, UnixStream};

#[cfg(test)]
mod tests {
    use super::UnixStream;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // TEST6747: Local socket pair round trips in both directions
    #[tokio::test]
    async fn test6747_local_socket_pair_round_trips_in_both_directions() {
        let (left, right) = UnixStream::pair().expect("create AF_UNIX stream pair");
        let (mut left_read, mut left_write) = left.into_split();
        let (mut right_read, mut right_write) = right.into_split();

        left_write
            .write_all(b"left-to-right")
            .await
            .expect("write left-to-right bytes");
        left_write.flush().await.expect("flush left-to-right bytes");

        let mut right_buf = [0u8; 13];
        right_read
            .read_exact(&mut right_buf)
            .await
            .expect("read left-to-right bytes");
        assert_eq!(&right_buf, b"left-to-right");

        right_write
            .write_all(b"right-to-left")
            .await
            .expect("write right-to-left bytes");
        right_write
            .flush()
            .await
            .expect("flush right-to-left bytes");

        let mut left_buf = [0u8; 13];
        left_read
            .read_exact(&mut left_buf)
            .await
            .expect("read right-to-left bytes");
        assert_eq!(&left_buf, b"right-to-left");
    }

    /// TEST12147: the peer reads end-of-file when a write half is dropped or
    /// shut down, with its read half still alive.
    ///
    /// Unix gets this from tokio. The Windows stream split the socket across
    /// two threads holding two handles, so dropping the write half closed
    /// nothing and the peer's read never ended: a cartridge that exited was
    /// never noticed, `test417_route_req_to_correct_cartridge` hung until the
    /// suite was killed, and `test1255_app_exit_suppresses_err_frames` failed.
    /// Bounded, so a regression fails here rather than hanging the suite.
    #[tokio::test]
    async fn test12147_the_peer_reads_eof_when_a_write_half_goes() {
        let bound = std::time::Duration::from_secs(10);

        // Dropped, with the read half of the same stream still held.
        let (left, right) = UnixStream::pair().expect("create AF_UNIX stream pair");
        let (_left_read, mut left_write) = left.into_split();
        let (mut right_read, _right_write) = right.into_split();
        left_write.write_all(b"last").await.expect("write");
        drop(left_write);
        let mut got = Vec::new();
        tokio::time::timeout(bound, right_read.read_to_end(&mut got))
            .await
            .expect("the peer never read end-of-file after the write half was dropped")
            .expect("read to end");
        assert_eq!(got, b"last", "what was written before the drop must arrive first");

        // Shut down, with the write half still held.
        let (left, right) = UnixStream::pair().expect("create AF_UNIX stream pair");
        let (_left_read, mut left_write) = left.into_split();
        let (mut right_read, _right_write) = right.into_split();
        left_write.shutdown().await.expect("shutdown");
        let mut got = Vec::new();
        tokio::time::timeout(bound, right_read.read_to_end(&mut got))
            .await
            .expect("the peer never read end-of-file after the write half was shut down")
            .expect("read to end");
        assert!(got.is_empty());
    }
}
