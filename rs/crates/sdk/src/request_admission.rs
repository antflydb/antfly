// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use http_body::Body as _;
use reqwest::ResponseBuilderExt;
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// A finite request envelope. Queue time is separate from reqwest's transport
/// timeout; an external timeout/cancellation of the operation includes both.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestAdmissionConfig {
    pub max_active: usize,
    pub max_queued: usize,
    pub max_wait: Duration,
}
impl Default for RequestAdmissionConfig {
    fn default() -> Self {
        Self {
            max_active: 100,
            max_queued: 100,
            max_wait: Duration::from_secs(1),
        }
    }
}

/// A local refusal before request construction or transmission. These errors
/// never establish a server outcome and do not automatically retry writes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientCapacityError {
    InvalidConfig,
    Closed,
    QueueFull,
    WaitExpired,
}
impl std::fmt::Display for ClientCapacityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidConfig => "antfly_client_capacity:invalid_config:unsent",
            Self::Closed => "antfly_client_capacity:closed:unsent",
            Self::QueueFull => "antfly_client_capacity:queue_full:unsent",
            Self::WaitExpired => "antfly_client_capacity:wait_expired:unsent",
        })
    }
}
impl std::error::Error for ClientCapacityError {}

/// Recover a typed local refusal from the generated client's error envelope.
pub fn client_capacity_error<E>(
    error: &progenitor_client::Error<E>,
) -> Option<ClientCapacityError> {
    let progenitor_client::Error::Custom(message) = error else {
        return None;
    };
    [
        ClientCapacityError::InvalidConfig,
        ClientCapacityError::Closed,
        ClientCapacityError::QueueFull,
        ClientCapacityError::WaitExpired,
    ]
    .into_iter()
    .find(|value| value.to_string() == *message)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestAdmissionStats {
    pub active: usize,
    pub queued: usize,
    pub closed: bool,
}
#[derive(Debug)]
struct Inner {
    config: RequestAdmissionConfig,
    permits: Arc<Semaphore>,
    active: AtomicUsize,
    queued: AtomicUsize,
}
/// Clones share one FIFO owner, including its close state. Responses keep the
/// owner alive after the client handle is dropped.
#[derive(Clone, Debug)]
pub struct RequestAdmission(Arc<Inner>);
impl Default for RequestAdmission {
    fn default() -> Self {
        Self::new(RequestAdmissionConfig::default()).unwrap()
    }
}
impl RequestAdmission {
    pub fn new(config: RequestAdmissionConfig) -> Result<Self, ClientCapacityError> {
        if config.max_active == 0
            || config.max_active > Semaphore::MAX_PERMITS
            || std::time::Instant::now()
                .checked_add(config.max_wait)
                .is_none()
        {
            return Err(ClientCapacityError::InvalidConfig);
        }
        Ok(Self(Arc::new(Inner {
            config,
            permits: Arc::new(Semaphore::new(config.max_active)),
            active: AtomicUsize::new(0),
            queued: AtomicUsize::new(0),
        })))
    }
    pub fn config(&self) -> RequestAdmissionConfig {
        self.0.config
    }
    /// Fields are independent observations during concurrent transitions.
    pub fn stats(&self) -> RequestAdmissionStats {
        RequestAdmissionStats {
            active: self.0.active.load(Ordering::Acquire),
            queued: self.0.queued.load(Ordering::Acquire),
            closed: self.0.permits.is_closed(),
        }
    }
    pub fn close(&self) {
        self.0.permits.close();
    }

    pub(crate) async fn acquire(&self) -> Result<Permit, ClientCapacityError> {
        match self.0.permits.clone().try_acquire_owned() {
            Ok(slot) => return Ok(self.grant(slot)),
            Err(TryAcquireError::Closed) => return Err(ClientCapacityError::Closed),
            Err(TryAcquireError::NoPermits) => {}
        }
        if self.0.config.max_wait.is_zero() {
            return Err(ClientCapacityError::QueueFull);
        }
        let deadline = tokio::time::Instant::now()
            .checked_add(self.0.config.max_wait)
            .ok_or(ClientCapacityError::InvalidConfig)?;
        if self
            .0
            .queued
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < self.0.config.max_queued).then(|| count + 1)
            })
            .is_err()
        {
            return Err(ClientCapacityError::QueueFull);
        }
        // Dropping the caller's future cancels its semaphore waiter and removes
        // the queue charge, including when it was already selected for a grant.
        let queued = Queued(&self.0);
        let slot = tokio::time::timeout_at(deadline, self.0.permits.clone().acquire_owned())
            .await
            .map_err(|_| ClientCapacityError::WaitExpired)?
            .map_err(|_| ClientCapacityError::Closed)?;
        drop(queued);
        if self.0.permits.is_closed() {
            return Err(ClientCapacityError::Closed);
        }
        // A delayed poll can find both the semaphore and timer ready. Tokio
        // polls the semaphore first, so recheck the original deadline before
        // recording an active grant. Refusal drops the owned slot for its peer.
        if tokio::time::Instant::now() >= deadline {
            return Err(ClientCapacityError::WaitExpired);
        }
        Ok(self.grant(slot))
    }
    fn grant(&self, slot: OwnedSemaphorePermit) -> Permit {
        self.0.active.fetch_add(1, Ordering::AcqRel);
        Permit {
            slot,
            owner: self.0.clone(),
        }
    }
}
struct Queued<'a>(&'a Inner);
impl Drop for Queued<'_> {
    fn drop(&mut self) {
        self.0.queued.fetch_sub(1, Ordering::AcqRel);
    }
}
#[derive(Debug)]
pub(crate) struct Permit {
    slot: OwnedSemaphorePermit,
    owner: Arc<Inner>,
}
impl Drop for Permit {
    fn drop(&mut self) {
        self.owner.active.fetch_sub(1, Ordering::AcqRel);
        // `slot` releases after accounting, waking the oldest live waiter.
        let _ = &self.slot;
    }
}

struct RetainedBody {
    // Drop the transport body before releasing capacity, including early drop.
    body: Option<reqwest::Body>,
    permit: Option<Permit>,
}
impl RetainedBody {
    fn retire(&mut self) {
        self.body = None;
        self.permit = None;
    }
}
impl http_body::Body for RetainedBody {
    type Data = bytes::Bytes;
    type Error = reqwest::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let Some(body) = &mut self.body else {
            return Poll::Ready(None);
        };
        let result = Pin::new(&mut *body).poll_frame(context);
        let terminal = matches!(&result, Poll::Ready(None) | Poll::Ready(Some(Err(_))))
            || body.is_end_stream();
        if terminal {
            self.retire();
        }
        result
    }
    fn is_end_stream(&self) -> bool {
        self.body.as_ref().is_none_or(|body| body.is_end_stream())
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.body.as_ref().map_or_else(
            || http_body::SizeHint::with_exact(0),
            |body| body.size_hint(),
        )
    }
}

pub(crate) fn retain_response(
    result: reqwest::Result<reqwest::Response>,
    permit: Permit,
) -> reqwest::Result<reqwest::Response> {
    let response = result?;
    let url = response.url().clone();
    let response: http::Response<reqwest::Body> = response.into();
    let (parts, body) = response.into_parts();
    let mut retained = RetainedBody {
        body: Some(body),
        permit: Some(permit),
    };
    if retained.is_end_stream() {
        retained.retire();
    }
    let mut response = http::Response::builder()
        .url(url)
        .body(reqwest::Body::wrap(retained))
        .unwrap();
    *response.status_mut() = parts.status;
    *response.headers_mut() = parts.headers;
    *response.version_mut() = parts.version;
    response.extensions_mut().extend(parts.extensions);
    Ok(response.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(future: impl std::future::Future<Output = ()>) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(future);
    }
    fn owner(queued: usize, wait: Duration) -> RequestAdmission {
        RequestAdmission::new(RequestAdmissionConfig {
            max_active: 1,
            max_queued: queued,
            max_wait: wait,
        })
        .unwrap()
    }
    async fn queued(owner: &RequestAdmission, count: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while owner.stats().queued != count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    #[test]
    fn defaults_and_invalid_envelopes() {
        assert_eq!(
            RequestAdmission::default().config(),
            RequestAdmissionConfig {
                max_active: 100,
                max_queued: 100,
                max_wait: Duration::from_secs(1)
            }
        );
        assert_eq!(
            RequestAdmission::new(RequestAdmissionConfig {
                max_active: 0,
                ..Default::default()
            })
            .unwrap_err(),
            ClientCapacityError::InvalidConfig
        );
        assert_eq!(
            RequestAdmission::new(RequestAdmissionConfig {
                max_wait: Duration::MAX,
                ..Default::default()
            })
            .unwrap_err(),
            ClientCapacityError::InvalidConfig
        );
    }
    #[test]
    fn fifo_survives_middle_waiter_cancellation() {
        run(async {
            let owner = owner(3, Duration::from_secs(1));
            let held = owner.acquire().await.unwrap();
            let a = owner.clone();
            let first = tokio::spawn(async move { a.acquire().await });
            queued(&owner, 1).await;
            let a = owner.clone();
            let middle = tokio::spawn(async move { a.acquire().await });
            queued(&owner, 2).await;
            let a = owner.clone();
            let last = tokio::spawn(async move { a.acquire().await });
            queued(&owner, 3).await;
            assert_eq!(
                owner.acquire().await.unwrap_err(),
                ClientCapacityError::QueueFull
            );
            middle.abort();
            assert!(middle.await.unwrap_err().is_cancelled());
            queued(&owner, 2).await;
            drop(held);
            let first = tokio::time::timeout(Duration::from_secs(1), first)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(owner.stats().active, 1);
            assert_eq!(owner.stats().queued, 1);
            drop(first);
            drop(last.await.unwrap().unwrap());
            assert_eq!(
                owner.stats(),
                RequestAdmissionStats {
                    active: 0,
                    queued: 0,
                    closed: false
                }
            );
        });
    }
    #[test]
    fn bounded_wait_expires_without_releasing_peer() {
        run(async {
            let owner = owner(1, Duration::from_millis(10));
            let held = owner.acquire().await.unwrap();
            assert_eq!(
                owner.acquire().await.unwrap_err(),
                ClientCapacityError::WaitExpired
            );
            assert_eq!(owner.stats().active, 1);
            assert_eq!(owner.stats().queued, 0);
            drop(held);
            drop(owner.acquire().await.unwrap());
            assert_eq!(owner.stats().active, 0);
        });
    }
    #[test]
    fn delayed_poll_cannot_grant_an_expired_waiter_or_lose_its_successor() {
        use std::future::Future;

        run(async {
            tokio::time::pause();
            for overshoot in [Duration::ZERO, Duration::from_millis(1)] {
                let owner = owner(2, Duration::from_secs(1));
                let held = owner.acquire().await.unwrap();
                let mut expired = Box::pin(owner.acquire());
                let mut context = Context::from_waker(std::task::Waker::noop());
                assert!(expired.as_mut().poll(&mut context).is_pending());
                tokio::time::advance(Duration::from_millis(500)).await;
                let mut successor = Box::pin(owner.acquire());
                assert!(successor.as_mut().poll(&mut context).is_pending());
                assert_eq!(owner.stats().queued, 2);
                tokio::time::advance(Duration::from_millis(500) + overshoot).await;
                // Neither waiter has been polled since the first deadline.
                // Tokio's timeout can now see both a ready permit and timer.
                drop(held);
                assert!(matches!(
                    expired.as_mut().poll(&mut context),
                    Poll::Ready(Err(ClientCapacityError::WaitExpired))
                ));
                assert_eq!(owner.stats().active, 0);
                assert_eq!(owner.stats().queued, 1);
                let Poll::Ready(Ok(next)) = successor.as_mut().poll(&mut context) else {
                    panic!("live successor did not receive the returned permit");
                };
                assert_eq!(owner.stats().active, 1);
                assert_eq!(owner.stats().queued, 0);
                drop(next);
                drop(owner.acquire().await.unwrap());
                assert_eq!(owner.stats().active, 0);
            }
        });
    }
    #[test]
    fn close_wakes_queue_and_keeps_active_capacity_owned() {
        run(async {
            let owner = owner(1, Duration::from_secs(1));
            let held = owner.acquire().await.unwrap();
            let a = owner.clone();
            let waiting = tokio::spawn(async move { a.acquire().await });
            queued(&owner, 1).await;
            owner.clone().close();
            assert_eq!(
                waiting.await.unwrap().unwrap_err(),
                ClientCapacityError::Closed
            );
            assert_eq!(
                owner.acquire().await.unwrap_err(),
                ClientCapacityError::Closed
            );
            assert_eq!(
                owner.stats(),
                RequestAdmissionStats {
                    active: 1,
                    queued: 0,
                    closed: true
                }
            );
            drop(held);
            assert_eq!(owner.stats().active, 0);
        });
    }
    #[test]
    fn cancellation_after_selection_returns_the_reserved_permit() {
        run(async {
            use std::future::Future;
            let owner = owner(1, Duration::from_secs(1));
            let held = owner.acquire().await.unwrap();
            let mut waiting = Box::pin(owner.acquire());
            let mut context = Context::from_waker(std::task::Waker::noop());
            assert!(waiting.as_mut().poll(&mut context).is_pending());
            assert_eq!(owner.stats().queued, 1);
            drop(held); // The semaphore selects the waiter before it is polled again.
            drop(waiting);
            assert_eq!(owner.stats().queued, 0);
            drop(owner.acquire().await.unwrap());
            assert_eq!(owner.stats().active, 0);
        });
    }
    #[test]
    fn disabled_queue_is_fail_fast_and_refusal_is_typed_unsent() {
        run(async {
            let owner = owner(0, Duration::from_secs(1));
            let held = owner.acquire().await.unwrap();
            let refusal = owner.acquire().await.unwrap_err();
            assert_eq!(refusal, ClientCapacityError::QueueFull);
            let error: progenitor_client::Error<()> =
                progenitor_client::Error::Custom(refusal.to_string());
            assert_eq!(client_capacity_error(&error), Some(refusal));
            assert!(!error.is_retryable());
            drop(held);
        });
    }
    #[test]
    fn response_eof_preserves_metadata_and_releases_capacity() {
        run(async {
            let owner = owner(0, Duration::ZERO);
            let response: reqwest::Response = http::Response::builder()
                .url("http://localhost/example".parse().unwrap())
                .status(202)
                .header("etag", "original")
                .body(reqwest::Body::from("[]"))
                .unwrap()
                .into();
            let response = retain_response(Ok(response), owner.acquire().await.unwrap()).unwrap();
            assert_eq!(owner.stats().active, 1);
            assert_eq!(response.url().as_str(), "http://localhost/example");
            assert_eq!(response.status(), 202);
            assert_eq!(response.headers()["etag"], "original");
            assert_eq!(response.content_length(), Some(2));
            assert_eq!(response.bytes().await.unwrap(), "[]");
            assert_eq!(owner.stats().active, 0);
        });
    }
    struct ProbeBody {
        owner: RequestAdmission,
        failed: bool,
        dropped: Arc<AtomicUsize>,
    }
    impl Drop for ProbeBody {
        fn drop(&mut self) {
            assert_eq!(
                self.owner.stats().active,
                1,
                "transport must retire before its permit"
            );
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
    impl http_body::Body for ProbeBody {
        type Data = bytes::Bytes;
        type Error = std::io::Error;
        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            if self.failed {
                Poll::Ready(Some(Err(std::io::Error::other("injected body failure"))))
            } else {
                Poll::Pending
            }
        }
    }
    #[test]
    fn unread_and_failed_bodies_retire_transport_before_capacity() {
        run(async {
            for failed in [false, true] {
                let owner = owner(0, Duration::ZERO);
                let dropped = Arc::new(AtomicUsize::new(0));
                let response: reqwest::Response =
                    http::Response::new(reqwest::Body::wrap(ProbeBody {
                        owner: owner.clone(),
                        failed,
                        dropped: dropped.clone(),
                    }))
                    .into();
                let response =
                    retain_response(Ok(response), owner.acquire().await.unwrap()).unwrap();
                assert_eq!(owner.stats().active, 1);
                if failed {
                    assert!(response.bytes().await.is_err());
                } else {
                    drop(response);
                }
                assert_eq!(dropped.load(Ordering::Relaxed), 1);
                assert_eq!(owner.stats().active, 0);
            }
        });
    }
    #[test]
    fn generated_clients_share_owner_and_all_families_refuse_before_transport() {
        run(async {
            let owner = owner(0, Duration::ZERO);
            let client =
                crate::Client::new("http://invalid.test").with_request_admission(owner.clone());
            let cloned = client.clone();
            let held = owner.acquire().await.unwrap();
            let error = cloned
                .list_tables(None, None, None, None)
                .await
                .unwrap_err();
            assert_eq!(
                client_capacity_error(&error),
                Some(ClientCapacityError::QueueFull)
            );
            let error = client.list_models().await.unwrap_err();
            assert_eq!(
                client_capacity_error(&error),
                Some(ClientCapacityError::QueueFull)
            );
            drop(held);
            cloned.close();
            assert!(client.request_admission().stats().closed);
            assert_eq!(
                client_capacity_error(&client.list_models().await.unwrap_err()),
                Some(ClientCapacityError::Closed)
            );
        });
    }
    #[test]
    fn close_after_selection_refuses_unpublished_queued_work() {
        run(async {
            use std::future::Future;
            let owner = owner(1, Duration::from_secs(1));
            let held = owner.acquire().await.unwrap();
            let mut waiting = Box::pin(owner.acquire());
            let mut context = Context::from_waker(std::task::Waker::noop());
            assert!(waiting.as_mut().poll(&mut context).is_pending());
            drop(held);
            owner.close();
            assert_eq!(waiting.await.unwrap_err(), ClientCapacityError::Closed);
            assert_eq!(
                owner.stats(),
                RequestAdmissionStats {
                    active: 0,
                    queued: 0,
                    closed: true
                }
            );
        });
    }

    #[test]
    fn caller_timeout_and_request_construction_failure_unwind_admission() {
        run(async {
            let owner = owner(1, Duration::from_secs(1));
            let held = owner.acquire().await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(5), owner.acquire())
                    .await
                    .is_err()
            );
            assert_eq!(owner.stats().queued, 0);
            assert_eq!(owner.stats().active, 1);
            drop(held);
            let client =
                crate::Client::new("invalid base URL").with_request_admission(owner.clone());
            let error = client
                .list_tables(None, None, None, None)
                .await
                .unwrap_err();
            assert!(client_capacity_error(&error).is_none());
            assert_eq!(owner.stats().active, 0);
        });
    }

    struct HttpFixture {
        url: String,
        requests: Arc<AtomicUsize>,
        stop: std::sync::mpsc::Sender<()>,
        thread: Option<std::thread::JoinHandle<()>>,
    }
    impl HttpFixture {
        fn start() -> Self {
            Self::with_redirect(None)
        }
        fn with_redirect(redirect: Option<u16>) -> Self {
            use std::io::{Read, Write};
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            listener.set_nonblocking(true).unwrap();
            let requests = Arc::new(AtomicUsize::new(0));
            let count = requests.clone();
            let (stop, stopped) = std::sync::mpsc::channel();
            let thread = std::thread::spawn(move || {
                let mut retained = Vec::new();
                while stopped.try_recv().is_err() {
                    let mut stream = match listener.accept() {
                        Ok((stream, _)) => stream,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(1));
                            continue;
                        }
                        Err(error) => panic!("fixture accept: {error}"),
                    };
                    stream
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .unwrap();
                    stream
                        .set_write_timeout(Some(Duration::from_secs(2)))
                        .unwrap();
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        let mut byte = [0];
                        stream.read_exact(&mut byte).unwrap();
                        request.push(byte[0]);
                        assert!(request.len() < 65536);
                    }
                    let headers = std::str::from_utf8(&request).unwrap();
                    let length: usize = headers
                        .lines()
                        .filter_map(|line| line.split_once(':'))
                        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                        .map_or(0, |(_, value)| value.trim().parse().unwrap());
                    assert!(length < 65536);
                    let mut body = vec![0; length];
                    stream.read_exact(&mut body).unwrap();
                    let number = count.fetch_add(1, Ordering::AcqRel);
                    if number == 0 {
                        if redirect.is_some() {
                            assert!(request.starts_with(b"POST /db/v1/sql"));
                            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
                            assert_eq!(body["statement"], "DELETE FROM items");
                        } else {
                            assert!(request.starts_with(b"GET /db/v1/tables"));
                        }
                    }
                    if let Some(status) = redirect {
                        write!(stream, "HTTP/1.1 {status} Redirect\r\nContent-Length: 0\r\nConnection: close\r\nLocation: /replayed\r\n\r\n").unwrap();
                    } else if number == 0 {
                        // Headers return an unrecognized status immediately;
                        // its body remains incomplete and owned by the caller.
                        stream.write_all(b"HTTP/1.1 418 Teapot\r\nContent-Length: 2\r\nConnection: close\r\nETag: retained\r\n\r\nx").unwrap();
                        retained.push(stream);
                    } else {
                        stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\nContent-Type: application/json\r\n\r\n[]").unwrap();
                    }
                }
            });
            Self {
                url,
                requests,
                stop,
                thread: Some(thread),
            }
        }
    }
    impl Drop for HttpFixture {
        fn drop(&mut self) {
            let _ = self.stop.send(());
            if let Err(panic) = self.thread.take().unwrap().join() {
                if !std::thread::panicking() {
                    std::panic::resume_unwind(panic);
                }
            }
        }
    }
    #[test]
    fn real_http_unread_error_body_blocks_clones_until_physical_drop() {
        run(async {
            let server = HttpFixture::start();
            let owner = owner(1, Duration::from_millis(20));
            let client = crate::Client::new(&server.url).with_request_admission(owner.clone());
            let cloned = client.clone();
            let response = match client.list_tables(None, None, None, None).await {
                Err(progenitor_client::Error::UnexpectedResponse(response)) => response,
                result => panic!("expected unread response: {result:?}"),
            };
            assert_eq!(response.headers()["etag"], "retained");
            assert_eq!(
                response.url().as_str(),
                format!("{}/db/v1/tables", server.url)
            );
            assert_eq!(owner.stats().active, 1);
            let error = cloned.list_models().await.unwrap_err();
            assert_eq!(
                client_capacity_error(&error),
                Some(ClientCapacityError::WaitExpired)
            );
            assert_eq!(server.requests.load(Ordering::Acquire), 1);
            let canceled_client = cloned.clone();
            let waiting =
                tokio::spawn(
                    async move { canceled_client.list_tables(None, None, None, None).await },
                );
            queued(&owner, 1).await;
            waiting.abort();
            assert!(waiting.await.unwrap_err().is_cancelled());
            assert_eq!(owner.stats().queued, 0);
            assert_eq!(server.requests.load(Ordering::Acquire), 1);
            drop(client);
            drop(cloned);
            assert_eq!(owner.stats().active, 1);
            drop(response);
            assert_eq!(owner.stats().active, 0);
            let fresh = crate::Client::new(&server.url).with_request_admission(owner.clone());
            assert!(
                fresh
                    .list_tables(None, None, None, None)
                    .await
                    .unwrap()
                    .into_inner()
                    .is_empty()
            );
            assert_eq!(server.requests.load(Ordering::Acquire), 2);
            assert_eq!(owner.stats().active, 0);
        });
    }
    #[test]
    fn owned_http_clients_never_redirect_mutating_sql() {
        run(async {
            for status in [301, 302, 303, 307, 308] {
                for token in [false, true] {
                    let server = HttpFixture::with_redirect(Some(status));
                    let client = if token {
                        crate::new_client_with_token(&server.url, "local-test-token").unwrap()
                    } else {
                        crate::Client::new(&server.url)
                    };
                    let body = serde_json::from_value(
                        serde_json::json!({"statement": "DELETE FROM items"}),
                    )
                    .unwrap();
                    match client.execute_sql(&body).await {
                        Err(progenitor_client::Error::UnexpectedResponse(response)) => {
                            assert_eq!(response.status().as_u16(), status)
                        }
                        result => panic!("redirect was followed or lost: {result:?}"),
                    }
                    assert_eq!(server.requests.load(Ordering::Acquire), 1);
                    assert_eq!(client.request_admission().stats().active, 0);
                }
            }
        });
    }
}
