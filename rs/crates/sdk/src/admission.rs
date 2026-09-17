//! Optional client admission. A response (including a stream) owns its slot
//! until dropped. The pool never retries a request with an unknown outcome.

use std::{
    fmt,
    future::Future,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, timeout_at},
};

#[derive(Clone, Copy, Debug)]
pub struct AdmissionConfig {
    pub max_in_flight: usize,
    pub max_queued: usize,
    pub max_wait: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionError {
    InvalidConfig,
    Busy,
    DeadlineExceeded,
}

impl fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Antfly client admission: {self:?} (request not dispatched)"
        )
    }
}
impl std::error::Error for AdmissionError {}

#[derive(Debug)]
pub enum RunError<E> {
    Admission(AdmissionError),
    /// The request was dispatched. A write may have committed.
    RequestDeadlineExceeded,
    Request(E),
}
impl<E: fmt::Display> fmt::Display for RunError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(error) => error.fmt(f),
            Self::RequestDeadlineExceeded => write!(
                f,
                "Antfly request deadline exceeded; outcome may be unknown"
            ),
            Self::Request(error) => error.fmt(f),
        }
    }
}
impl<E: std::error::Error + 'static> std::error::Error for RunError<E> {}

#[derive(Clone)]
pub struct AdmissionPool {
    active: Arc<Semaphore>,
    outstanding: Arc<Semaphore>,
    max_wait: Duration,
}

/// The value drops before its permits, so a retained stream cannot silently
/// leave admission when the response headers arrive. There is intentionally no
/// `into_inner` that could strip a live stream of its reservation.
pub struct Admitted<T> {
    value: T,
    active: OwnedSemaphorePermit,
    outstanding: OwnedSemaphorePermit,
}
impl<T> Deref for Admitted<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}
impl<T> DerefMut for Admitted<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}
impl<T> Admitted<T> {
    /// Preserve the permit while converting a response to its streaming body.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Admitted<U> {
        Admitted {
            value: f(self.value),
            active: self.active,
            outstanding: self.outstanding,
        }
    }
}

impl AdmissionPool {
    pub fn new(config: AdmissionConfig) -> Result<Self, AdmissionError> {
        let total = config
            .max_in_flight
            .checked_add(config.max_queued)
            .ok_or(AdmissionError::InvalidConfig)?;
        if config.max_in_flight == 0
            || total > Semaphore::MAX_PERMITS
            || (config.max_queued > 0 && config.max_wait.is_zero())
            || Instant::now().checked_add(config.max_wait).is_none()
        {
            return Err(AdmissionError::InvalidConfig);
        }
        Ok(Self {
            active: Arc::new(Semaphore::new(config.max_in_flight)),
            outstanding: Arc::new(Semaphore::new(total)),
            max_wait: config.max_wait,
        })
    }

    async fn acquire(&self, deadline: Option<Instant>) -> Result<Admitted<()>, AdmissionError> {
        if deadline.is_some_and(|end| Instant::now() >= end) {
            return Err(AdmissionError::DeadlineExceeded);
        }
        let outstanding = self
            .outstanding
            .clone()
            .try_acquire_owned()
            .map_err(|_| AdmissionError::Busy)?;
        let active = match self.active.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) if self.max_wait.is_zero() => return Err(AdmissionError::Busy),
            Err(_) => {
                let wait_end = Instant::now() + self.max_wait;
                let end = deadline.map_or(wait_end, |value| value.min(wait_end));
                let timed_out = || {
                    if deadline.is_some_and(|value| Instant::now() >= value) {
                        AdmissionError::DeadlineExceeded
                    } else {
                        AdmissionError::Busy
                    }
                };
                let permit = timeout_at(end, self.active.clone().acquire_owned())
                    .await
                    .map_err(|_| timed_out())?
                    .map_err(|_| AdmissionError::Busy)?;
                // A simultaneous timer/grant cannot dispatch an expired waiter.
                if Instant::now() >= end {
                    return Err(timed_out());
                }
                permit
            }
        };
        if deadline.is_some_and(|end| Instant::now() >= end) {
            return Err(AdmissionError::DeadlineExceeded);
        }
        Ok(Admitted {
            value: (),
            active,
            outstanding,
        })
    }

    /// Construct the operation only after grant. A single deadline covers local
    /// waiting and the operation future. Streaming values retain the permit;
    /// callers must also bound their subsequent stream reads by this deadline.
    pub async fn run<F, Fut, T, E>(
        &self,
        deadline: Option<Instant>,
        operation: F,
    ) -> Result<Admitted<T>, RunError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let lease = self.acquire(deadline).await.map_err(RunError::Admission)?;
        let value = if let Some(end) = deadline {
            timeout_at(end, operation())
                .await
                .map_err(|_| RunError::RequestDeadlineExceeded)?
        } else {
            operation().await
        }
        .map_err(RunError::Request)?;
        Ok(lease.map(|()| value))
    }
}

/// Generated API calls remain typed and use the original reusable reqwest
/// client. Clones share the same pool; separate pools do not provide a node cap.
#[derive(Clone)]
pub struct PooledClient {
    client: crate::Client,
    pool: AdmissionPool,
}
impl PooledClient {
    pub fn new(client: crate::Client, pool: AdmissionPool) -> Self {
        Self { client, pool }
    }

    pub async fn run<'a, F, Fut, T, E>(
        &'a self,
        deadline: Option<Instant>,
        operation: F,
    ) -> Result<Admitted<T>, RunError<E>>
    where
        F: FnOnce(&'a crate::Client) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        self.pool.run(deadline, || operation(&self.client)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_admission_owns_responses_and_cancelled_waiters() {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                let pool = AdmissionPool::new(AdmissionConfig {
                    max_in_flight: 1,
                    max_queued: 1,
                    max_wait: Duration::from_secs(1),
                })
                .unwrap();
                let response = pool
                    .run(None, || async { Ok::<_, ()>("body") })
                    .await
                    .unwrap();
                let mut waiting = Box::pin(pool.run(None, || async { Ok::<_, ()>("next") }));
                assert!(
                    std::future::poll_fn(|cx| std::task::Poll::Ready(
                        waiting.as_mut().poll(cx).is_pending()
                    ))
                    .await
                );
                assert!(matches!(
                    pool.run(None, || async { Ok::<_, ()>(()) }).await,
                    Err(RunError::Admission(AdmissionError::Busy))
                ));
                drop(waiting);
                assert_eq!(pool.outstanding.available_permits(), 1);
                let response = response.map(|body| body.len());
                assert_eq!(*response, 4);
                assert_eq!(pool.active.available_permits(), 0);
                drop(response);
                assert_eq!(pool.active.available_permits(), 1);
                assert_eq!(pool.outstanding.available_permits(), 2);
            });
    }

    #[test]
    fn client_admission_deadlines_and_unknown_write_failures_do_not_retry() {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                let pool = AdmissionPool::new(AdmissionConfig {
                    max_in_flight: 1,
                    max_queued: 1,
                    max_wait: Duration::from_millis(5),
                })
                .unwrap();
                let mut dispatched = 0;
                assert!(matches!(
                    pool.run(Some(Instant::now()), || {
                        dispatched += 1;
                        async { Ok::<_, ()>(()) }
                    })
                    .await,
                    Err(RunError::Admission(AdmissionError::DeadlineExceeded))
                ));
                assert_eq!(dispatched, 0);
                assert!(matches!(
                    pool.run(None, || {
                        dispatched += 1;
                        async { Err::<(), _>("unknown outcome") }
                    })
                    .await,
                    Err(RunError::Request("unknown outcome"))
                ));
                assert_eq!(dispatched, 1);
                let blocker = pool.acquire(None).await.unwrap();
                assert!(matches!(
                    pool.run(None, || async { Ok::<_, ()>(()) }).await,
                    Err(RunError::Admission(AdmissionError::Busy))
                ));
                drop(blocker);
                assert_eq!(pool.active.available_permits(), 1);
                assert_eq!(pool.outstanding.available_permits(), 2);
            });
    }
}
