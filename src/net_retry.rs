//! Shared network-retry primitive for cartridges.
//!
//! Every cartridge that makes HTTP calls (modelcartridge → HuggingFace,
//! fetchcartridge → fabric registry, …) needs the SAME answer to one question:
//! *which failures are worth retrying, and how?* Duplicating that answer per
//! call site guarantees drift — one site retries DNS blips, another doesn't,
//! a third retries a 404 and masks a real "model does not exist". This module
//! is the single definition.
//!
//! ## What is transient (retried) vs permanent (surfaced immediately)
//!
//! - **Transport errors** — a [`reqwest::Error`] that `is_timeout()`,
//!   `is_connect()`, or `is_request()` (which covers DNS resolution failure,
//!   refused/reset connections, and request-send I/O). These are the blips a
//!   retry is meant to ride out.
//! - **Transient HTTP statuses** — `408 Request Timeout`, `425 Too Early`,
//!   `429 Too Many Requests`, and `500/502/503/504`. A `Retry-After` header on
//!   the response is honored exactly.
//!
//! Everything else is **permanent and returned to the caller unchanged** — most
//! importantly `404`, `401`, `403`, every other `4xx`, and any successful
//! response. The caller's own status handling (e.g. 404 → ModelNotFound) runs
//! exactly as before; this layer never reclassifies a response, it only decides
//! whether to *try again*.
//!
//! ## Failure is exposed, never hidden
//!
//! When attempts are exhausted, the **last** outcome is returned verbatim — the
//! real timeout error, or the real `503` response. There is no fallback value,
//! no swallowed error, no "best effort empty result". A persistent outage fails
//! hard with the genuine cause so it can be seen and fixed.
//!
//! ## Usage
//!
//! ```no_run
//! # use capdag::net_retry::{retry_request, RetryPolicy};
//! # async fn example(client: &reqwest::Client, url: &str) -> Result<(), Box<dyn std::error::Error>> {
//! let response = retry_request(&RetryPolicy::default(), "resolve file size", || {
//!     // A FRESH request every attempt — a RequestBuilder is consumed by send().
//!     client.get(url).header("Range", "bytes=0-0").send()
//! })
//! .await?;
//! // `response` is a normal reqwest::Response: success, or a permanent status
//! // (404/401/…) that the caller classifies itself.
//! # let _ = response;
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::time::Duration;

use reqwest::{Response, StatusCode};

/// Backoff and attempt-count policy for [`retry_request`].
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Total number of attempts, including the first. `1` disables retrying.
    /// Must be `>= 1`; a `0` would mean "never even try", which is a caller
    /// bug, so [`retry_request`] treats `0` as `1` rather than silently doing
    /// nothing.
    pub max_attempts: u32,
    /// Delay before the *first* retry. Each subsequent retry doubles the base
    /// (capped at `max_delay`) before jitter is applied.
    pub base_delay: Duration,
    /// Upper bound on the pre-jitter backoff delay.
    pub max_delay: Duration,
    /// Cap on how long a `Retry-After` header is honored. A hostile or
    /// misconfigured server can send `Retry-After: 86400`; without a cap a
    /// single such response would wedge the operation for a day. The wait is
    /// `min(server_retry_after, retry_after_cap)`.
    pub retry_after_cap: Duration,
}

impl Default for RetryPolicy {
    /// A general default tuned for interactive cartridge calls: 4 attempts
    /// (1 initial + 3 retries), 250ms base growing to a 10s ceiling, honoring
    /// `Retry-After` up to 30s. Generous enough to ride out a transient DNS or
    /// connection blip, bounded enough that a real outage fails in seconds.
    fn default() -> Self {
        Self {
            max_attempts: 4,
            base_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(10),
            retry_after_cap: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    /// A policy tuned for large-file download attempts, where each attempt is
    /// expensive (re-establishing a transfer) so retries are fewer but the
    /// backoff ceiling is higher to let a congested CDN recover.
    pub fn download() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
            retry_after_cap: Duration::from_secs(60),
        }
    }

    /// Pre-jitter backoff for a given zero-based retry index (0 = first retry):
    /// `base * 2^retry_index`, saturating, capped at `max_delay`.
    ///
    /// The doubling is done in nanoseconds with saturating arithmetic so a large
    /// `retry_index` simply saturates to `max_delay` instead of wrapping. (An
    /// earlier version cast `2^retry_index` to `u32` for `Duration::checked_mul`,
    /// which truncated to 0 once the shift exceeded 32 bits — yielding a 0ns
    /// backoff for high retry indices. Computing in `u128` nanos avoids that.)
    fn backoff_for(&self, retry_index: u32) -> Duration {
        let cap_nanos = self.max_delay.as_nanos();
        // Once the shift would exceed the cap's bit-width there is no point
        // computing the exact value — it is already past the cap.
        if retry_index >= 127 {
            return self.max_delay;
        }
        let scaled_nanos = self
            .base_delay
            .as_nanos()
            .checked_shl(retry_index)
            .unwrap_or(u128::MAX)
            .min(cap_nanos);
        // scaled_nanos <= cap_nanos <= max_delay.as_nanos(), which fits in the
        // Duration range, so this conversion cannot itself overflow.
        Duration::from_nanos(scaled_nanos.min(u64::MAX as u128) as u64).min(self.max_delay)
    }
}

/// Classification of a single attempt's result. The original
/// `reqwest::Result<Response>` is carried through unchanged so that, when an
/// attempt is the last one, the *genuine* result is returned to the caller
/// without re-issuing the request or fabricating a value.
enum Attempt {
    /// Terminal: hand this back to the caller (success, or a permanent status).
    Done(reqwest::Result<Response>),
    /// Transient: worth retrying. `result` is the real outcome to surface if
    /// this turns out to be the final attempt.
    Retry {
        reason: String,
        retry_after: Option<Duration>,
        result: reqwest::Result<Response>,
    },
}

/// True for HTTP statuses that represent a transient server-side condition.
fn is_transient_status(status: StatusCode) -> bool {
    // 425 Too Early has no named constant in this `http` version, so match it
    // by its numeric code alongside the named transient statuses.
    if status.as_u16() == 425 {
        return true;
    }
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT            // 408
            | StatusCode::TOO_MANY_REQUESTS    // 429
            | StatusCode::INTERNAL_SERVER_ERROR // 500
            | StatusCode::BAD_GATEWAY          // 502
            | StatusCode::SERVICE_UNAVAILABLE  // 503
            | StatusCode::GATEWAY_TIMEOUT // 504
    )
}

/// True for transport-level errors (no HTTP response) worth retrying: timeouts,
/// connect failures (which include DNS resolution failure), and request-send
/// I/O errors. A `reqwest` body/decode error after a response is NOT here — that
/// surfaces through the response path and is handled by the caller.
fn is_transient_transport(error: &reqwest::Error) -> bool {
    error.is_timeout() || error.is_connect() || error.is_request()
}

/// Parse a `Retry-After` header value: either delta-seconds (`"120"`) or an
/// HTTP-date. We only parse the numeric form here — HTTP-date `Retry-After` is
/// rare for these APIs and the exponential backoff is a correct (if less
/// precise) substitute when the date form is present, so an unparseable value
/// yields `None` (fall back to computed backoff) rather than a hard error.
fn parse_retry_after(response: &Response) -> Option<Duration> {
    let raw = response.headers().get(reqwest::header::RETRY_AFTER)?;
    let text = raw.to_str().ok()?.trim();
    let secs: u64 = text.parse().ok()?;
    Some(Duration::from_secs(secs))
}

fn classify(result: reqwest::Result<Response>) -> Attempt {
    match result {
        Ok(response) => {
            let status = response.status();
            if is_transient_status(status) {
                let retry_after = parse_retry_after(&response);
                Attempt::Retry {
                    reason: format!("HTTP {status}"),
                    retry_after,
                    result: Ok(response),
                }
            } else {
                // Success or a permanent status (404/401/403/other) — the
                // caller owns the interpretation. We do NOT touch it.
                Attempt::Done(Ok(response))
            }
        }
        Err(error) => {
            if is_transient_transport(&error) {
                Attempt::Retry {
                    reason: format!("transport error: {error}"),
                    retry_after: None,
                    result: Err(error),
                }
            } else {
                Attempt::Done(Err(error))
            }
        }
    }
}

/// Apply full jitter to a backoff duration: a uniformly random value in
/// `[0, delay]`. Full jitter (rather than `delay ± x`) is the AWS-recommended
/// scheme — it maximally de-correlates retries from many concurrent callers so
/// they don't all wake and re-stampede a recovering server in lockstep.
fn jitter(delay: Duration) -> Duration {
    let millis = delay.as_millis();
    if millis == 0 {
        return Duration::ZERO;
    }
    let capped = millis.min(u64::MAX as u128) as u64;
    Duration::from_millis(fastrand::u64(0..=capped))
}

/// Execute `make_request` under the retry `policy`, returning the first
/// terminal [`Response`] (success or permanent status) or the last error after
/// exhausting transient retries.
///
/// `make_request` is invoked once per attempt and MUST build and send a fresh
/// request each time (a `reqwest::RequestBuilder` is consumed by `send()`), so
/// the closure typically captures the client + URL and calls
/// `client.get(url)…send()` inside.
///
/// `label` is a short human description of the call (e.g. "resolve file size")
/// used only in the diagnostic logged before each retry; it never changes
/// control flow.
pub async fn retry_request<F, Fut>(
    policy: &RetryPolicy,
    label: &str,
    mut make_request: F,
) -> reqwest::Result<Response>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = reqwest::Result<Response>>,
{
    let attempts = policy.max_attempts.max(1);
    let last_index = attempts - 1;

    for attempt in 0..attempts {
        match classify(make_request().await) {
            // Terminal outcome (success, permanent status, or non-retryable
            // transport error) — return the genuine result unchanged.
            Attempt::Done(result) => return result,
            Attempt::Retry {
                reason,
                retry_after,
                result,
            } => {
                if attempt == last_index {
                    // Retry budget exhausted. Surface the REAL last result —
                    // the genuine 503 response or transport error — never a
                    // fabricated value. The failure is exposed, not hidden.
                    return result;
                }
                // Honor server Retry-After (capped), else exponential backoff,
                // then apply full jitter.
                let base = match retry_after {
                    Some(server) => server.min(policy.retry_after_cap),
                    None => policy.backoff_for(attempt),
                };
                let wait = jitter(base);
                eprintln!(
                    "[net_retry] {label}: attempt {}/{} failed ({reason}); retrying in {} ms",
                    attempt + 1,
                    attempts,
                    wait.as_millis()
                );
                tokio::time::sleep(wait).await;
            }
        }
    }

    // The loop always returns on its final iteration (attempt == last_index),
    // so this is structurally unreachable. We keep an honest final request
    // rather than `unreachable!()` so a future change to the loop bound can
    // never turn a logic slip into a panic in production.
    make_request().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    fn fast_policy() -> RetryPolicy {
        // Near-zero delays so the tests exercise the control flow, not the clock.
        RetryPolicy {
            max_attempts: 4,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(2),
            retry_after_cap: Duration::from_millis(5),
        }
    }

    // A local HTTP server that returns a scripted sequence of statuses, one per
    // request, so a test can assert exactly how many attempts happened and what
    // the retry layer did with each status. Using a real socket (not a reqwest
    // mock) means the transport path — connect, send, read — is genuinely
    // exercised.
    async fn scripted_server(statuses: Vec<u16>) -> (String, Arc<AtomicU32>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicU32::new(0));
        let hits_for_task = Arc::clone(&hits);

        tokio::spawn(async move {
            for status in statuses {
                let (mut sock, _) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_) => return,
                };
                // Drain the request headers (read until we see the blank line).
                let mut buf = [0u8; 1024];
                let _ = sock.read(&mut buf).await;
                hits_for_task.fetch_add(1, Ordering::SeqCst);
                let body = "ok";
                let response = format!(
                    "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(response.as_bytes()).await;
                let _ = sock.flush().await;
            }
        });

        (format!("http://{addr}/"), hits)
    }

    // TEST8200: a transient 503 followed by a 200 retries exactly once and the
    // caller receives the 200 — the core "ride out a blip" contract.
    #[tokio::test]
    async fn test8200_transient_status_then_success_retries_and_succeeds() {
        let (url, hits) = scripted_server(vec![503, 200]).await;
        let client = reqwest::Client::new();
        let resp = retry_request(&fast_policy(), "test", || client.get(&url).send())
            .await
            .expect("a 200 must come back after the 503 is retried");
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(hits.load(Ordering::SeqCst), 2, "exactly one retry expected");
    }

    // TEST8201: a permanent 404 is returned to the caller on the FIRST attempt
    // with NO retry. Retrying a 404 would mask a genuine "does not exist" and
    // waste time; this pins that 404 is terminal.
    #[tokio::test]
    async fn test8201_permanent_status_is_not_retried() {
        let (url, hits) = scripted_server(vec![404, 200]).await;
        let client = reqwest::Client::new();
        let resp = retry_request(&fast_policy(), "test", || client.get(&url).send())
            .await
            .expect("the 404 response itself is returned, not an error");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "a 404 must not trigger any retry"
        );
    }

    // TEST8202: when every attempt is a transient status, the retry budget is
    // spent and the LAST response (the real 503) is returned verbatim — the
    // failure is exposed, not swallowed into a fabricated success/empty.
    #[tokio::test]
    async fn test8202_exhausted_retries_surface_the_last_transient_response() {
        let (url, hits) = scripted_server(vec![503, 503, 503, 503]).await;
        let client = reqwest::Client::new();
        let resp = retry_request(&fast_policy(), "test", || client.get(&url).send())
            .await
            .expect("a transient STATUS still yields a Response, just an error one");
        assert_eq!(
            resp.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "the genuine terminal 503 must be surfaced"
        );
        assert_eq!(
            hits.load(Ordering::SeqCst),
            4,
            "all 4 attempts must be spent before giving up"
        );
    }

    // TEST8203: a transport-level failure (connect refused — no server) is
    // transient, so it is retried up to the budget, and the final transport
    // error is returned (not a panic, not a swallowed Ok).
    #[tokio::test]
    async fn test8203_transport_failure_is_retried_then_surfaced() {
        // Port 1 on loopback: nothing listens, so connect is refused — a
        // transient transport error on every attempt.
        let url = "http://127.0.0.1:1/";
        let client = reqwest::Client::new();
        let err = retry_request(&fast_policy(), "test", || client.get(url).send())
            .await
            .expect_err("a persistently refused connection must surface as an error");
        assert!(
            err.is_connect() || err.is_request(),
            "the surfaced error must be the genuine transport failure, got: {err}"
        );
    }

    // TEST8204: max_attempts == 1 disables retrying — a transient 503 is
    // returned immediately after a single attempt. Pins that the policy knob
    // actually gates the loop.
    #[tokio::test]
    async fn test8204_single_attempt_policy_does_not_retry() {
        let (url, hits) = scripted_server(vec![503, 200]).await;
        let client = reqwest::Client::new();
        let policy = RetryPolicy {
            max_attempts: 1,
            ..fast_policy()
        };
        let resp = retry_request(&policy, "test", || client.get(&url).send())
            .await
            .expect("single attempt returns the 503 response");
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(hits.load(Ordering::SeqCst), 1, "no retry when max_attempts=1");
    }

    // TEST8205: backoff grows exponentially and is capped at max_delay. Pure
    // arithmetic on the policy — no clock — so it is deterministic.
    #[tokio::test]
    async fn test8205_backoff_is_exponential_and_capped() {
        let policy = RetryPolicy {
            max_attempts: 10,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(800),
            retry_after_cap: Duration::from_secs(1),
        };
        assert_eq!(policy.backoff_for(0), Duration::from_millis(100));
        assert_eq!(policy.backoff_for(1), Duration::from_millis(200));
        assert_eq!(policy.backoff_for(2), Duration::from_millis(400));
        assert_eq!(policy.backoff_for(3), Duration::from_millis(800));
        // Capped from here on.
        assert_eq!(policy.backoff_for(4), Duration::from_millis(800));
        assert_eq!(policy.backoff_for(50), Duration::from_millis(800));
    }

    // TEST8206: jitter stays within [0, delay] and 0 maps to 0. Guards the
    // de-correlation invariant — a jittered wait must never exceed the base.
    #[tokio::test]
    async fn test8206_jitter_is_bounded() {
        assert_eq!(jitter(Duration::ZERO), Duration::ZERO);
        let base = Duration::from_millis(500);
        for _ in 0..1000 {
            assert!(jitter(base) <= base, "jitter must not exceed the base delay");
        }
    }
}
