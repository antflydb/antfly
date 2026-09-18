//! Explicit query-only retries. Generated arbitrary operations never retry.

use crate::{AdmissionPool, Admitted, RunError};
use reqwest::{Method, Request, Response, header::HeaderMap};
use std::{fmt, time::Duration};
use tokio::time::{Instant, sleep, timeout_at};

#[derive(Clone, Copy, Debug)]
pub struct ReadRetryPolicy {
    /// Includes the original attempt; must be in 2..=5.
    pub max_attempts: u8,
    /// One budget for admission, attempts and backoff (at most 60 seconds).
    /// A shorter query-body timeout_ms or caller deadline wins for all attempts.
    pub max_elapsed: Duration,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl ReadRetryPolicy {
    fn valid(self) -> bool {
        (2..=5).contains(&self.max_attempts)
            && !self.initial_backoff.is_zero()
            && self.initial_backoff <= self.max_backoff
            && self.max_backoff <= self.max_elapsed
            && self.max_elapsed <= Duration::from_secs(60)
    }
}

#[derive(Debug)]
pub enum QueryRetryError {
    InvalidPolicy,
    /// Only known query POST routes with in-memory replayable bodies qualify.
    UnsupportedRequest,
    Request(RunError<reqwest::Error>),
    /// Retains bounded raw error details, including overload/retry metadata.
    Http {
        status: u16,
        headers: HeaderMap,
        body: Vec<u8>,
        truncated: bool,
    },
}
impl fmt::Display for QueryRetryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Antfly query retry: {self:?}")
    }
}
impl std::error::Error for QueryRetryError {}

/// Reuses the caller's reqwest connections and admission pool. Requests must
/// already include authentication headers. Cancellation drops the operation;
/// successful response streams retain their pool slot and are never retried.
/// The original operation deadline also bounds successful response consumption.
/// Supply stricter limits as a request timeout or `execute` deadline: the retry
/// budget sets reqwest's per-request timeout, overriding its opaque client default.
#[derive(Clone)]
pub struct QueryRetryClient {
    client: reqwest::Client,
    pool: AdmissionPool,
    policy: ReadRetryPolicy,
}

fn query_request(request: &Request) -> bool {
    if request.method() != Method::POST
        || request.body().is_some_and(|body| body.as_bytes().is_none())
    {
        return false;
    }
    let parts: Vec<_> = request.url().path().split('/').collect();
    match parts.as_slice() {
        ["", "db", "v1", "query"] => true,
        ["", "db", "v1", "tables", table, "query"] => !table.is_empty(),
        [
            "",
            "db",
            "v1",
            "databases",
            db,
            "namespaces",
            ns,
            "tables",
            table,
            "query",
        ] => !db.is_empty() && !ns.is_empty() && !table.is_empty(),
        _ => false,
    }
}

fn query_budget(request: &Request, maximum: Duration) -> Option<Duration> {
    let body = request.body()?.as_bytes()?;
    if body.len() > 1 << 20 {
        return None;
    }
    let ndjson = request
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(';')
                .next()
                .unwrap_or("")
                .trim()
                .eq_ignore_ascii_case("application/x-ndjson")
        });
    let lines: Vec<&[u8]> = if ndjson {
        body.split(|byte| *byte == b'\n')
            .filter(|line| line.iter().any(|byte| !byte.is_ascii_whitespace()))
            .collect()
    } else {
        vec![body]
    };
    if lines.is_empty() {
        return None;
    }
    let mut budget = maximum;
    for line in lines {
        let value: serde_json::Value = serde_json::from_slice(line).ok()?;
        let object = value.as_object()?;
        if let Some(timeout) = object.get("timeout_ms").filter(|value| !value.is_null()) {
            let milliseconds = timeout.as_u64()?;
            budget = budget.min(Duration::from_millis(milliseconds));
        }
    }
    Some(budget)
}

fn retry_delay(
    policy: ReadRetryPolicy,
    attempt: u8,
    status: u16,
    headers: &HeaderMap,
    body: &[u8],
) -> Option<Duration> {
    if status != 429 {
        return None;
    }
    let detail: serde_json::Value = serde_json::from_slice(body).ok()?;
    if detail["reason"] != "instance_busy"
        || detail["stage"] != "admission"
        || detail["execution_started"] != false
    {
        return None;
    }
    let mut delay = (policy.initial_backoff * (1 << attempt)).min(policy.max_backoff);
    if let Some(after) = headers.get("retry-after") {
        let text = after.to_str().ok()?;
        if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let seconds: u64 = text.parse().ok()?;
        let minimum = Duration::from_secs(seconds);
        if minimum > policy.max_backoff {
            return None;
        }
        delay = delay.max(minimum);
    }
    Some(delay)
}

impl QueryRetryClient {
    pub fn new(
        client: reqwest::Client,
        pool: AdmissionPool,
        policy: ReadRetryPolicy,
    ) -> Result<Self, QueryRetryError> {
        if !policy.valid() {
            return Err(QueryRetryError::InvalidPolicy);
        }
        Ok(Self {
            client,
            pool,
            policy,
        })
    }

    /// Dispatch a query request. Newer data may be visible on eventual admission.
    /// HTTP errors are bounded and returned explicitly; transport errors and
    /// unknown/executed outcomes never trigger another attempt.
    pub async fn execute(
        &self,
        request: Request,
        deadline: Option<Instant>,
    ) -> Result<Admitted<Response>, QueryRetryError> {
        if !query_request(&request) {
            return Err(QueryRetryError::UnsupportedRequest);
        }
        let started = Instant::now();
        let body_budget = query_budget(&request, self.policy.max_elapsed);
        let budget = request
            .timeout()
            .map_or(body_budget.unwrap_or(self.policy.max_elapsed), |timeout| {
                (*timeout).min(body_budget.unwrap_or(self.policy.max_elapsed))
            });
        let end = deadline.map_or(started + budget, |value| value.min(started + budget));
        timeout_at(end, self.execute_until(request, end, body_budget.is_some()))
            .await
            .map_err(|_| QueryRetryError::Request(RunError::RequestDeadlineExceeded))?
    }

    async fn execute_until(
        &self,
        request: Request,
        end: Instant,
        allow_retry: bool,
    ) -> Result<Admitted<Response>, QueryRetryError> {
        for attempt in 0..self.policy.max_attempts {
            let mut next = request
                .try_clone()
                .ok_or(QueryRetryError::UnsupportedRequest)?;
            let mut response = self
                .pool
                .run(Some(end), || {
                    // reqwest transfers this total timeout into the response body.
                    // Compute after local admission, so waiting and prior attempts
                    // consume the same original budget even after headers arrive.
                    *next.timeout_mut() = Some(end.saturating_duration_since(Instant::now()));
                    self.client.execute(next)
                })
                .await
                .map_err(QueryRetryError::Request)?;
            if response.status().is_success() {
                return Ok(response);
            }
            let status = response.status().as_u16();
            let headers = response.headers().clone();
            let mut body = Vec::new();
            let mut truncated = false;
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|error| QueryRetryError::Request(RunError::Request(error)))?
            {
                let remaining = 16384 - body.len();
                body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
                if chunk.len() > remaining {
                    truncated = true;
                    break;
                }
            }
            // Retire response/permit before backoff, including truncated errors.
            drop(response);
            let delay = if allow_retry && !truncated && attempt + 1 < self.policy.max_attempts {
                retry_delay(self.policy, attempt, status, &headers, &body)
            } else {
                None
            };
            if let Some(delay) = delay.filter(|delay| Instant::now() + *delay < end) {
                sleep(delay).await;
            } else {
                return Err(QueryRetryError::Http {
                    status,
                    headers,
                    body,
                    truncated,
                });
            }
        }
        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn successful_body_keeps_original_budget_after_retry_backoff() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let end = std::time::Instant::now() + Duration::from_secs(5);
            for attempt in 0..2 {
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            assert!(std::time::Instant::now() < end);
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        Err(error) => panic!("{error}"),
                    }
                };
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = Vec::new();
                while !request.ends_with(br#"{"timeout_ms":300}"#) {
                    let mut part = [0; 1024];
                    let count = socket.read(&mut part).unwrap();
                    assert!(count > 0);
                    request.extend_from_slice(&part[..count]);
                }
                if attempt == 0 {
                    let body = r#"{"reason":"instance_busy","stage":"admission","execution_started":false}"#;
                    write!(socket, "HTTP/1.1 429 Too Many Requests\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
                } else {
                    // Headers and one byte succeed; the remaining body arrives
                    // after both the original and a mistakenly restarted budget.
                    socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\na",
                        )
                        .unwrap();
                    socket.flush().unwrap();
                    std::thread::sleep(Duration::from_millis(500));
                    let _ = socket.write_all(b"b");
                }
            }
        });
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let pool = AdmissionPool::new(crate::AdmissionConfig {
                    max_in_flight: 1,
                    max_queued: 0,
                    max_wait: Duration::ZERO,
                })
                .unwrap();
                let raw = reqwest::Client::new();
                let client = QueryRetryClient::new(
                    raw.clone(),
                    pool.clone(),
                    ReadRetryPolicy {
                        max_attempts: 2,
                        max_elapsed: Duration::from_secs(2),
                        initial_backoff: Duration::from_millis(180),
                        max_backoff: Duration::from_millis(180),
                    },
                )
                .unwrap();
                let started = Instant::now();
                let mut response = client
                    .execute(
                        raw.post(format!("http://{address}/db/v1/query"))
                            .body(r#"{"timeout_ms":300}"#)
                            .build()
                            .unwrap(),
                        None,
                    )
                    .await
                    .unwrap();
                assert_eq!(response.chunk().await.unwrap().unwrap(), "a");
                let error = response.chunk().await.unwrap_err();
                assert!(error.is_timeout(), "{error:?}");
                assert!(
                    started.elapsed() < Duration::from_millis(440),
                    "body timeout restarted after backoff"
                );
                drop(response);
                assert!(pool.run(None, || async { Ok::<_, ()>(()) }).await.is_ok());
            });
        server.join().unwrap();
    }

    #[test]
    fn query_retries_release_each_attempt_and_keep_final_response_admitted() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let end = std::time::Instant::now() + Duration::from_secs(5);
            for attempt in 0..2 {
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            assert!(
                                std::time::Instant::now() < end,
                                "client did not dispatch expected attempt"
                            );
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        Err(error) => panic!("{error}"),
                    }
                };
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = Vec::new();
                loop {
                    let mut part = [0; 1024];
                    let count = socket.read(&mut part).unwrap();
                    assert!(count > 0);
                    request.extend_from_slice(&part[..count]);
                    if request.windows(6).any(|value| value == b"\r\n\r\n{}") {
                        break;
                    }
                }
                assert!(request.starts_with(b"POST /db/v1/query "));
                let body = if attempt == 0 {
                    r#"{"reason":"instance_busy","stage":"admission","execution_started":false}"#
                } else {
                    "result"
                };
                let status = if attempt == 0 {
                    "429 Too Many Requests"
                } else {
                    "200 OK"
                };
                write!(
                    socket,
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let pool = AdmissionPool::new(crate::AdmissionConfig {
                    max_in_flight: 1,
                    max_queued: 0,
                    max_wait: Duration::ZERO,
                })
                .unwrap();
                let raw = reqwest::Client::new();
                let client = QueryRetryClient::new(
                    raw.clone(),
                    pool.clone(),
                    ReadRetryPolicy {
                        max_attempts: 3,
                        max_elapsed: Duration::from_secs(2),
                        initial_backoff: Duration::from_millis(1),
                        max_backoff: Duration::from_millis(10),
                    },
                )
                .unwrap();
                let mut response = client
                    .execute(
                        raw.post(format!("http://{address}/db/v1/query"))
                            .body("{}")
                            .build()
                            .unwrap(),
                        None,
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), 200);
                assert!(matches!(
                    pool.run(None, || async { Ok::<_, ()>(()) }).await,
                    Err(RunError::Admission(crate::AdmissionError::Busy))
                ));
                assert_eq!(response.chunk().await.unwrap().unwrap(), "result");
                drop(response);
                assert!(pool.run(None, || async { Ok::<_, ()>(()) }).await.is_ok());
            });
        server.join().unwrap();
    }

    #[test]
    fn retries_require_explicit_rejection_and_bounded_guidance() {
        let policy = ReadRetryPolicy {
            max_attempts: 3,
            max_elapsed: Duration::from_secs(5),
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(1),
        };
        let body = br#"{"reason":"instance_busy","stage":"admission","execution_started":false}"#;
        assert_eq!(
            retry_delay(policy, 1, 429, &HeaderMap::new(), body),
            Some(Duration::from_millis(20))
        );
        for unknown in [
            br#"{"reason":"instance_busy"}"#.as_slice(),
            br#"{"reason":"instance_busy","stage":"admission","execution_started":true}"#,
        ] {
            assert_eq!(
                retry_delay(policy, 0, 429, &HeaderMap::new(), unknown),
                None
            );
        }
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", "2".parse().unwrap());
        assert_eq!(retry_delay(policy, 0, 429, &headers, body), None);
        let client = reqwest::Client::new();
        let body_request = client
            .post("http://localhost/db/v1/query")
            .header("content-type", "application/x-ndjson")
            .body("{\"timeout_ms\":800}\n{\"timeout_ms\":80}\n")
            .build()
            .unwrap();
        let budget = query_budget(&body_request, policy.max_elapsed).unwrap();
        assert_eq!(budget, Duration::from_millis(80));
        let delay = retry_delay(
            ReadRetryPolicy {
                initial_backoff: Duration::from_millis(100),
                ..policy
            },
            0,
            429,
            &HeaderMap::new(),
            body,
        )
        .unwrap();
        assert!(
            delay > budget,
            "backoff cannot restart the body's original deadline"
        );
        assert!(query_request(
            &client
                .post("http://localhost/db/v1/tables/docs/query")
                .body("{}")
                .build()
                .unwrap()
        ));
        assert!(!query_request(
            &client
                .post("http://localhost/db/v1/tables/docs/batch")
                .body("{}")
                .build()
                .unwrap()
        ));
        assert!(!query_request(
            &client.get("http://localhost/db/v1/query").build().unwrap()
        ));
    }
}
