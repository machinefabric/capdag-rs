//! A fabric registry on a local socket, for tests: it answers each request by
//! its path with a status, a body, or nothing at all, and counts what it was
//! asked. A real server, so the whole fetch path — connect, send, read,
//! retry — is exercised as it is against the published fabric.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// What the stub answers one request with.
#[derive(Debug, Clone)]
pub(crate) enum Answer {
    /// This status, with a short plain-text body.
    Status(u16),
    /// 200 with this body.
    Body(Vec<u8>),
    /// Read the request and never answer: the client's timeout decides.
    Silence,
}

/// A running stub registry.
pub(crate) struct StubRegistry {
    /// `http://127.0.0.1:<port>`: what a registry is anchored at.
    pub(crate) anchor: String,
    requests: Arc<Mutex<HashMap<String, usize>>>,
    total: Arc<AtomicUsize>,
}

impl StubRegistry {
    /// Start a stub that answers every path with `answer(path)`.
    pub(crate) async fn start(answer: impl Fn(&str) -> Answer + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a local port for the stub registry");
        let addr = listener.local_addr().expect("the stub's local address");
        let answer = Arc::new(answer);
        let requests = Arc::new(Mutex::new(HashMap::<String, usize>::new()));
        let total = Arc::new(AtomicUsize::new(0));
        let (requests_task, total_task) = (Arc::clone(&requests), Arc::clone(&total));
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let answer = Arc::clone(&answer);
                let requests = Arc::clone(&requests_task);
                let total = Arc::clone(&total_task);
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut buf = [0u8; 1024];
                    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                        match socket.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => head.extend_from_slice(&buf[..n]),
                        }
                    }
                    let path = String::from_utf8_lossy(&head)
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("")
                        .to_string();
                    *requests.lock().unwrap().entry(path.clone()).or_default() += 1;
                    total.fetch_add(1, Ordering::SeqCst);
                    let (status, body) = match answer(&path) {
                        Answer::Status(code) => (code, format!("status {code}").into_bytes()),
                        Answer::Body(body) => (200, body),
                        Answer::Silence => {
                            // Hold the connection open, answering nothing,
                            // until the client gives up and closes it.
                            let _ = socket.read(&mut buf).await;
                            return;
                        }
                    };
                    let head = format!(
                        "HTTP/1.1 {status} STUB\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(head.as_bytes()).await;
                    let _ = socket.write_all(&body).await;
                    let _ = socket.flush().await;
                });
            }
        });
        Self {
            anchor: format!("http://{addr}"),
            requests,
            total,
        }
    }

    /// How many requests were made for `path`.
    pub(crate) fn requests_for(&self, path: &str) -> usize {
        self.requests.lock().unwrap().get(path).copied().unwrap_or(0)
    }

    /// How many requests were made in all.
    pub(crate) fn total_requests(&self) -> usize {
        self.total.load(Ordering::SeqCst)
    }

    /// The path of `url` on this stub.
    pub(crate) fn path_of<'a>(&self, url: &'a str) -> &'a str {
        url.strip_prefix(self.anchor.as_str())
            .unwrap_or_else(|| panic!("{url} is not on the stub at {}", self.anchor))
    }
}
